#!/usr/bin/env python3
"""CBM_IN_PROCESS writes no shared state (#2072).

Driver for tests/test_in_process_mcp.sh, which owns runtime isolation
(scripts/test-runtime.sh) and cleanup; run that, not this.

The maintainer's condition for accepting the mode: an opted-in client never
writes shared cache state -- no daemon coordination files, no cohort or
lifetime locks, and no writes to another process's index -- so it cannot
disturb a daemon running alongside it. Three legs prove it at the process level:

  1. Fresh state, no daemon. Every mutating tool is attempted; afterwards the
     runtime, cache and HOME directories must still be EMPTY. Everything CBM
     coordinates through -- the rendezvous, cohort and lifetime locks, project
     leases, _config.db, an index -- would have to be created there first, so
     an empty tree proves no code path reached any of it. This is the leg that
     caught a refused index_repository creating _config.db: the handler loads
     the index policy before its executor can refuse.

  2. A live daemon that owns two freshly published (rollback-journal) indexes.
     A quiescent baseline of every entry under those roots (type, mode, size,
     mtime, inode, sha256) must be identical after an in-process session that
     reads the indexes with every tool it serves and attempts every mutation.
     A pre-existing lock file carries no trace of being locked, so while the
     session is live its open descriptors are inspected too: no socket, and
     nothing under CBM_RUNTIME_DIR -- a process cannot hold a lock it has no
     descriptor for.

  3. The same after a daemon-side write has left the index in WAL mode, as
     real use does. Reading a WAL database is SQLite's shared reader protocol:
     any reader, the daemon's own included, creates <project>.db-shm and an
     empty <project>.db-wal if they are absent and records read marks in the
     -shm. Exactly that is allowed; the database itself must stay
     byte-identical and the -wal must gain no frames.

A positive control runs the descriptor check against a session started with
CBM_IN_PROCESS=false and requires it to FIND the socket and the runtime lock,
so the checks above are known to be able to fail -- and "false" is known to
mean off.

Every wait is a bounded poll on an asserted state; exhausting one is a failure
with its reason, never a sleep that lets an assertion run on an unobserved
state.

Usage: test_in_process_mcp.py BINARY SCRATCH_ROOT
  SCRATCH_ROOT holds runtime/ and cache/ (CBM_RUNTIME_DIR / CBM_CACHE_DIR, as
  created and exported by cbm_test_runtime_init).
"""

from __future__ import annotations

import hashlib
import json
import os
import queue
import re
import shutil
import stat
import subprocess
import sys
import threading
import time

WAIT_S = 30.0
POLL_S = 0.1

# Everything the full profile serves beyond the analysis allowlist.
MUTATING_TOOLS = ("index_repository", "delete_project", "manage_adr", "ingest_traces")


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def deadline() -> float:
    return time.monotonic() + WAIT_S


# --- filesystem state --------------------------------------------------------


def sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 16), b""):
            digest.update(block)
    return digest.hexdigest()


def entry_state(path: str) -> tuple:
    st = os.lstat(path)
    mode = stat.S_IMODE(st.st_mode)
    if stat.S_ISDIR(st.st_mode):
        # Directory timestamps are not state anyone coordinates through, and
        # they move without a lasting write: startup re-hardens the cache root
        # to 0700 as every CBM role does, and search_code / detect_changes
        # stage command output in logs/.mcp-command-* and unlink it. A
        # leftover file still shows up as a new entry.
        return ("dir", mode)
    if stat.S_ISREG(st.st_mode):
        return ("file", mode, st.st_size, st.st_mtime_ns, st.st_ino, sha256(path))
    return ("other", stat.S_IFMT(st.st_mode), mode, st.st_ino)


def snapshot(roots: dict[str, str]) -> dict[str, tuple]:
    entries = {}
    for label, root in roots.items():
        for dirpath, dirnames, filenames in os.walk(root):
            for name in dirnames + filenames:
                path = os.path.join(dirpath, name)
                entries[f"{label}/{os.path.relpath(path, root)}"] = entry_state(path)
    return entries


def diff(before: dict[str, tuple], after: dict[str, tuple]) -> dict[str, str]:
    changes = {}
    for key in sorted(set(before) | set(after)):
        if key not in before:
            changes[key] = "new"
        elif key not in after:
            changes[key] = "removed"
        elif before[key] != after[key]:
            changes[key] = f"changed {before[key][:5]} -> {after[key][:5]}"
    return changes


def quiescent_snapshot(roots: dict[str, str]) -> dict[str, tuple]:
    """Two consecutive identical snapshots: the daemon has finished reacting to
    whatever ran last, so any later change belongs to what runs next."""
    until = deadline()
    previous = snapshot(roots)
    while time.monotonic() < until:
        time.sleep(0.5)
        current = snapshot(roots)
        if current == previous:
            return current
        previous = current
    fail(f"daemon state did not settle within {WAIT_S:.0f}s")
    return {}


def journal_is_wal(db_path: str) -> bool:
    with open(db_path, "rb") as fh:
        header = fh.read(20)
    return len(header) == 20 and header[18] == 2 and header[19] == 2


# --- open descriptors --------------------------------------------------------


def open_descriptors(pid: int) -> list[tuple[str, str]]:
    """(kind, name) for each descriptor, kind in {"socket", "path", "other"}."""
    if os.path.isdir(f"/proc/{pid}/fd"):
        found = []
        for fd in os.listdir(f"/proc/{pid}/fd"):
            try:
                target = os.readlink(f"/proc/{pid}/fd/{fd}")
            except OSError:
                continue
            if target.startswith("socket:"):
                found.append(("socket", target))
            elif target.startswith("/"):
                found.append(("path", target))
            else:
                found.append(("other", target))
        return found
    lsof = shutil.which("lsof") or "/usr/sbin/lsof"
    if not os.access(lsof, os.X_OK):
        print("neither /proc nor lsof is available to inspect descriptors", file=sys.stderr)
        sys.exit(2)
    out = subprocess.run(
        [lsof, "-n", "-P", "-a", "-p", str(pid), "-F", "ftn"],
        capture_output=True, text=True, timeout=WAIT_S,
    ).stdout
    found, kind = [], "other"
    for line in out.splitlines():
        field, value = line[:1], line[1:]
        if field == "f":
            kind = "other"
        elif field == "t":
            kind = ("socket" if value in ("unix", "IPv4", "IPv6", "sock")
                    else "path" if value in ("REG", "DIR", "CHR") else "other")
        elif field == "n":
            found.append((kind, value))
    return found


def coordination_descriptors(pid: int, runtime_dir: str) -> list[str]:
    runtime = os.path.realpath(runtime_dir) + os.sep
    return [
        f"{kind} {name}"
        for kind, name in open_descriptors(pid)
        if kind == "socket" or (kind == "path" and os.path.realpath(name).startswith(runtime))
    ]


# --- MCP stdio session -------------------------------------------------------


class Session:
    def __init__(self, binary: str, env: dict[str, str], cwd: str, stderr_path: str):
        self._stderr = open(stderr_path, "wb")
        self.proc = subprocess.Popen(
            [binary], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=self._stderr, env=env, cwd=cwd,
        )
        self._lines: queue.Queue = queue.Queue()
        threading.Thread(target=self._pump, daemon=True).start()
        self._next_id = 1

    def _pump(self) -> None:
        for raw in self.proc.stdout:
            self._lines.put(raw)
        self._lines.put(None)

    def _send(self, message: dict) -> None:
        self.proc.stdin.write((json.dumps(message) + "\n").encode())
        self.proc.stdin.flush()

    def request(self, method: str, params: dict) -> dict:
        request_id, self._next_id = self._next_id, self._next_id + 1
        self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        until = deadline()
        while True:
            try:
                raw = self._lines.get(timeout=max(0.0, until - time.monotonic()))
            except queue.Empty:
                fail(f"no response to {method} (id {request_id}) within {WAIT_S:.0f}s")
            if raw is None:
                fail(f"server exited before answering {method} (id {request_id})")
            try:
                message = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if message.get("id") == request_id:
                return message

    def open(self) -> list[str]:
        init = self.request("initialize", {"protocolVersion": "2025-11-25", "capabilities": {}})
        if "result" not in init:
            fail(f"initialize failed: {init}")
        self._send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        listed = self.request("tools/list", {})
        return [tool["name"] for tool in listed.get("result", {}).get("tools", [])]

    def call(self, name: str, arguments: dict) -> tuple[bool, str]:
        """(is_error, text) for one tools/call."""
        message = self.request("tools/call", {"name": name, "arguments": arguments})
        if "error" in message:
            return True, json.dumps(message["error"])
        result = message.get("result") or {}
        text = " ".join(part.get("text", "") for part in result.get("content") or [])
        return bool(result.get("isError")), text

    def close(self) -> None:
        self.proc.stdin.close()
        try:
            self.proc.wait(timeout=WAIT_S)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
            fail(f"server did not exit within {WAIT_S:.0f}s of stdin EOF")
        finally:
            self._stderr.close()


# --- the in-process session under test --------------------------------------


def mutations(repo: str, project: str) -> list[tuple[str, dict]]:
    return [
        ("index_repository", {"repo_path": repo}),
        ("index_repository", {"repo_path": repo, "mode": "cross-repo-intelligence"}),
        ("delete_project", {"project": project}),
        ("manage_adr", {"project": project, "mode": "update", "content": "# ADR\n"}),
        ("ingest_traces", {"project": project, "traces": []}),
    ]


def reads(project: str, other: str) -> dict[str, dict]:
    """Arguments for every tool the in-process session may serve. Each must
    succeed, so the session demonstrably read the daemon's indexes; a tool it
    lists but this table lacks fails the test, so a tool added to the
    allowlist is exercised here before it ships."""
    p = {"project": project}
    return {
        "list_projects": {},
        "search_graph": {**p, "name_pattern": "add"},
        "query_graph": {**p, "query": "MATCH (f:Function) RETURN f.name LIMIT 5"},
        "trace_path": {**p, "function_name": "add", "direction": "both"},
        "get_code_snippet": {**p, "qualified_name": "add"},
        "get_file_outline": {**p, "file_path": "sample.c"},
        "get_graph_schema": p,
        "compare_graphs": {"base_project": project, "target_project": other},
        "get_architecture": p,
        "search_code": {**p, "pattern": "add"},
        "index_status": p,
        "check_index_coverage": {**p, "paths": ["sample.c"]},
        "detect_changes": {**p, "base_branch": "HEAD"},
    }


class Round:
    """One in-process session: every call is recorded first and judged after,
    so a shared-state write is reported before the checks it would also trip."""

    def __init__(self, binary: str, env: dict[str, str], repo: str, stderr_path: str,
                 runtime: str, project: str, other: str | None):
        session = Session(binary, env, repo, stderr_path)
        self.tools = session.open()
        self.read_errors = []
        if other is not None:
            table = reads(project, other)
            for name in self.tools:
                if name not in table:
                    self.read_errors.append(f"{name}: served, but not exercised by this test")
                    continue
                is_error, text = session.call(name, table[name])
                if is_error:
                    self.read_errors.append(f"{name}: {text[:200]}")
        else:
            session.call("list_projects", {})
        self.accepted = []
        for name, arguments in mutations(repo, project):
            is_error, text = session.call(name, arguments)
            if not is_error:
                self.accepted.append(f"{name} {arguments}: {text[:200]}")
        self.held = coordination_descriptors(session.proc.pid, runtime)
        session.close()

    def check_session(self) -> None:
        if self.held:
            fail(f"CBM_IN_PROCESS held coordination descriptors: {self.held}")
        exposed = [name for name in MUTATING_TOOLS if name in self.tools]
        if exposed:
            fail(f"CBM_IN_PROCESS lists mutating tools: {exposed}")
        if self.accepted:
            fail(f"CBM_IN_PROCESS did not refuse: {self.accepted}")
        if self.read_errors:
            fail(f"CBM_IN_PROCESS could not read the daemon's indexes: {self.read_errors}")


# --- fixture and daemon ------------------------------------------------------


def run(argv: list[str], env: dict[str, str], label: str) -> str:
    done = subprocess.run(argv, env=env, capture_output=True, text=True, timeout=120)
    if done.returncode != 0:
        fail(f"{label} exited {done.returncode}: {done.stdout}{done.stderr}")
    return done.stdout


def make_repo(path: str, env: dict[str, str]) -> str:
    os.mkdir(path, 0o700)
    with open(os.path.join(path, "sample.c"), "w") as fh:
        fh.write("int add(int a, int b) { return a + b; }\n"
                 "int main(void) { return add(1, 2); }\n")
    for step in (["init", "-q"], ["add", "-A"],
                 ["-c", "user.email=t@example.com", "-c", "user.name=t",
                  "commit", "-q", "-m", "init"]):
        subprocess.run(["git", "-C", path] + step, env=env, check=True,
                       capture_output=True, timeout=60)
    return os.path.realpath(path)


def index(binary: str, repo: str, env: dict[str, str], cache: str) -> str:
    out = run([binary, "cli", "index_repository", "--repo-path", repo], env,
              "cli index_repository")
    try:
        project = json.loads(out)["project"]
    except (ValueError, KeyError):
        fail(f"cli index_repository did not report a project: {out[:300]}")
    if not os.path.isfile(os.path.join(cache, f"{project}.db")):
        fail(f"the daemon did not publish {project}.db")
    return project


def daemon_pid(binary: str, env: dict[str, str]) -> int | None:
    done = subprocess.run([binary, "daemon", "status"], env=env,
                          capture_output=True, text=True, timeout=120)
    match = re.search(r"^\s*pid: (\d+)", done.stdout, re.MULTILINE)
    return int(match.group(1)) if done.returncode == 0 and match else None


def main() -> None:
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    binary, scratch = sys.argv[1], sys.argv[2]
    runtime, cache = os.path.join(scratch, "runtime"), os.path.join(scratch, "cache")
    home = os.path.join(scratch, "home")
    os.mkdir(home, 0o700)

    # Hermetic git for the fixture and for the git that CBM itself runs: no
    # developer or runner config (default branch, signing) leaks in.
    env = {k: v for k, v in os.environ.items()
           if k not in ("CBM_IN_PROCESS", "CBM_ALLOWED_ROOT", "CBM_DIAGNOSTICS")}
    env.update(CBM_RUNTIME_DIR=runtime, CBM_CACHE_DIR=cache, HOME=home,
               GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    in_process = dict(env, CBM_IN_PROCESS="1")
    roots = {"runtime": runtime, "cache": cache, "home": home}
    repo = make_repo(os.path.join(scratch, "repo"), env)
    repo2 = make_repo(os.path.join(scratch, "repo2"), env)

    # Leg 1: fresh state, no daemon -- nothing may appear anywhere.
    fresh = Round(binary, in_process, repo, os.path.join(scratch, "leg1.err"),
                  runtime, "never-indexed", None)
    created = sorted(snapshot(roots))
    if created:
        fail(f"CBM_IN_PROCESS created shared state from a fresh start: {created}")
    fresh.check_session()
    print("ok: fresh start -- every mutation refused, runtime/cache/HOME still empty")

    # Leg 2: a live daemon owns two freshly published indexes.
    started = run([binary, "daemon", "start"], env, "daemon start")
    match = re.search(r"pid (\d+)", started)
    if not match:
        fail(f"daemon start did not report a pid: {started}")
    pid = int(match.group(1))
    project, other = index(binary, repo, env, cache), index(binary, repo2, env, cache)
    if not os.listdir(runtime):
        fail("a live daemon left CBM_RUNTIME_DIR empty; the runtime check would be vacuous")

    baseline = quiescent_snapshot(roots)
    alongside = Round(binary, in_process, repo, os.path.join(scratch, "leg2.err"),
                      runtime, project, other)
    changes = diff(baseline, snapshot(roots))
    if changes:
        fail("CBM_IN_PROCESS changed shared state next to a live daemon:\n  "
             + "\n  ".join(f"{k}: {v}" for k, v in changes.items()))
    alongside.check_session()
    if daemon_pid(binary, env) != pid:
        fail(f"the daemon (pid {pid}) is no longer the active daemon")
    print(f"ok: alongside daemon pid {pid} -- served {len(alongside.tools)} read tools from its "
          "indexes, refused every mutation, held no socket or runtime descriptor, changed nothing")

    # Positive control, and "false" means off: this session goes through the
    # daemon, so the descriptor check must see its coordination. Its ADR write
    # also leaves the index in WAL mode for leg 3, as real use does.
    control = Session(binary, dict(env, CBM_IN_PROCESS="false"), repo,
                      os.path.join(scratch, "control.err"))
    control.open()
    is_error, text = control.call("manage_adr", {"project": project, "mode": "update",
                                                 "content": "# ADR\n\nFixture.\n"})
    if is_error:
        fail(f"positive control: a daemon-backed manage_adr update failed: {text[:300]}")
    held = coordination_descriptors(control.proc.pid, runtime)
    control.close()
    if not any(entry.startswith("socket") for entry in held) or \
            not any(entry.startswith("path") for entry in held):
        fail("positive control: a CBM_IN_PROCESS=false session showed no socket and runtime "
             f"lock: {held}")
    print("ok: positive control -- a CBM_IN_PROCESS=false session holds a socket and a runtime lock")

    # Leg 3: the same next to a WAL-mode index.
    db = os.path.join(cache, f"{project}.db")
    if not journal_is_wal(db):
        fail(f"{project}.db is not in WAL mode after a daemon-side write; leg 3 would be vacuous")
    baseline = quiescent_snapshot(roots)
    wal = Round(binary, in_process, repo, os.path.join(scratch, "leg3.err"),
                runtime, project, other)
    after = snapshot(roots)
    shm, wal_file = f"cache/{project}.db-shm", f"cache/{project}.db-wal"
    changes = diff(baseline, after)
    changes.pop(shm, None)
    if changes.get(wal_file) == "new" and after[wal_file][2] == 0:
        changes.pop(wal_file)
    if changes:
        fail("CBM_IN_PROCESS changed shared state next to a WAL-mode index (only SQLite's "
             "reader -shm and an empty -wal are allowed):\n  "
             + "\n  ".join(f"{k}: {v}" for k, v in changes.items()))
    wal.check_session()
    print("ok: WAL-mode index -- database byte-identical, no WAL frames, only SQLite's reader -shm")

    run([binary, "daemon", "stop"], env, "daemon stop")
    until = deadline()
    while daemon_pid(binary, env) is not None:
        if time.monotonic() >= until:
            fail(f"daemon pid {pid} did not stop within {WAIT_S:.0f}s")
        time.sleep(POLL_S)
    print("PASS: CBM_IN_PROCESS writes no shared state (#2072)")


if __name__ == "__main__":
    main()
