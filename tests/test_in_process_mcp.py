#!/usr/bin/env python3
"""CBM_IN_PROCESS writes no shared state, and serves a read-only cache (#2072).

Driver for tests/test_in_process_mcp.sh, which owns runtime isolation
(scripts/test-runtime.sh) and cleanup; run that, not this.

The maintainer's conditions for the mode: an opted-in client never writes
shared cache state -- no daemon coordination files, no cohort or lifetime
locks, and no writes to another process's index -- so it cannot disturb a
daemon running alongside it; and it works on a read-only cache, never
chmodding one, since read-only mounts and sandboxes are why it exists.

  1. Fresh state, no daemon. Every mutating tool is attempted; afterwards the
     runtime, cache and HOME directories must still be EMPTY, and the cache
     root's own mode, ctime and ACL unchanged. Everything CBM coordinates
     through -- the rendezvous, cohort and lifetime locks, project leases,
     _config.db, an index -- would have to be created there first, so an
     empty tree proves no code path reached any of it; and a chmod, even to
     the mode the root already has, moves its ctime. Then a cache that is not
     owner-only (group bits; on macOS an allow-ACL) and a missing one must be
     refused before initialize, and left exactly as they were.

  2. A live daemon that owns two freshly published (rollback-journal) indexes,
     with its watcher off. Every entry under the roots (files: type, mode,
     size, mtime, inode, sha256; directories, the roots included: mode and
     mtime, so even a file created and removed again shows) must be identical
     after an in-process session that
     reads the indexes with every tool it serves and attempts every mutation.
     A pre-existing lock file carries no trace of being locked, so while the
     session is live its open descriptors are inspected too: no socket, and
     nothing under CBM_RUNTIME_DIR -- a process cannot hold a lock it has no
     descriptor for.

  3. The same after a daemon-side write has left the index in WAL mode, as
     real use does. Reading a WAL database is SQLite's shared reader protocol:
     any reader, the daemon's own included, creates <db>-shm and an empty
     <db>-wal if they are absent and records read marks in the -shm. Exactly
     that is allowed, for any database whose header says WAL; every database
     itself must stay byte-identical and an existing -wal must gain no frames.

  4. A read-only copy of that cache: directories 0500, files 0400. Every read
     tool must succeed against it, and nothing may change -- the root stays
     0500 with the same ctime.

  5. The same cache on a genuinely read-only mount (EROFS) with its ordinary
     0700/0600 modes, where any chmod or create fails: an unprivileged
     read-only disk image on macOS, a read-only bind mount in an unprivileged
     user+mount namespace on Linux. Where neither can be set up the leg prints
     SKIP and the reason.

A positive control runs the descriptor check against a session started with
CBM_IN_PROCESS=false and requires it to FIND the socket and the runtime lock,
so the checks above are known to be able to fail -- and "false" is known to
mean off.

No baseline waits for state to settle. The daemon is idle by construction:
its watcher is off (config watcher_enabled=false, read once at start and
confirmed from its log), so no watch outlives a session to poll and log later;
`cli index_repository` returns only after the worker has exited and the index
is sealed and renamed into place; and leg 3 waits for the one thing it depends
on, the daemon's connections to the WAL database closing (SQLite then deletes
its -wal and -shm). An idle daemon still writes two things, both metadata on
its own files, and both are named where they are skipped: once a second it
re-hardens the cache root (fchmod 0700 and an ACL clear, from its UI-config
poll), which moves only that root's ctime -- so legs 2 and 3 compare the
roots' mtime, not their ctime -- and once a minute it restamps the mtime of the
coordination files it holds (entry_state()). Every wait is a bounded poll on an
asserted state; exhausting one is a failure with its reason, never a sleep that
lets an assertion run on an unobserved state.

Usage: test_in_process_mcp.py BINARY SCRATCH_ROOT
  SCRATCH_ROOT holds runtime/ and cache/ (CBM_RUNTIME_DIR / CBM_CACHE_DIR, as
  created and exported by cbm_test_runtime_init).
"""

from __future__ import annotations

import errno
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
UNAVAILABLE = 77  # --ro-bind-exec: the read-only mount could not be set up

# Everything the full profile serves beyond the analysis allowlist.
MUTATING_TOOLS = ("index_repository", "delete_project", "manage_adr", "ingest_traces")

INITIALIZE = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                         "params": {"protocolVersion": "2025-11-25", "capabilities": {}}})


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    sys.exit(1)


def deadline() -> float:
    return time.monotonic() + WAIT_S


def wait_until(predicate, label: str):
    """Poll until predicate() returns something truthy, and return it."""
    until = deadline()
    while True:
        value = predicate()
        if value:
            return value
        if time.monotonic() >= until:
            fail(f"timed out after {WAIT_S:.0f}s waiting for {label}")
        time.sleep(POLL_S)


# --- filesystem state --------------------------------------------------------


def sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 16), b""):
            digest.update(block)
    return digest.hexdigest()


def entry_state(label: str, path: str) -> tuple:
    st = os.lstat(path)
    mode = stat.S_IMODE(st.st_mode)
    if stat.S_ISDIR(st.st_mode):
        # mtime, not ctime: a directory's mtime moves when an entry in it is
        # created or removed, so a write that cleaned up after itself still
        # shows. Its ctime also moves on a chmod, which the daemon does to the
        # cache root once a second; the roots' ctime is checked where no
        # daemon runs (root_state).
        return ("dir", mode, st.st_mtime_ns)
    if stat.S_ISREG(st.st_mode):
        # Under runtime/ the daemon restamps the mtime of every coordination
        # file it holds once a minute (futimens, HOST_COORDINATION_TOUCH_MS in
        # daemon/host.c), with or without clients: there mtime is its
        # heartbeat. Content, size, inode and mode still count, and holding a
        # lock changes none of them anyway -- the descriptor check is what
        # proves no lock was taken.
        mtime = None if label == "runtime" else st.st_mtime_ns
        return ("file", mode, st.st_size, mtime, st.st_ino, sha256(path))
    return ("other", stat.S_IFMT(st.st_mode), mode, st.st_ino)


def snapshot(roots: dict[str, str]) -> dict[str, tuple]:
    entries = {}
    for label, root in roots.items():
        entries[f"{label}/."] = entry_state(label, root)
        for dirpath, dirnames, filenames in os.walk(root):
            for name in dirnames + filenames:
                path = os.path.join(dirpath, name)
                entries[f"{label}/{os.path.relpath(path, root)}"] = entry_state(label, path)
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


def report(what: str, changes: dict[str, str]) -> None:
    if changes:
        fail(f"{what}:\n  " + "\n  ".join(f"{k}: {v}" for k, v in changes.items()))


def acl_text(path: str) -> str:
    """macOS extended ACL entries ("" elsewhere, or when there are none)."""
    if sys.platform != "darwin":
        return ""
    out = subprocess.run(["ls", "-lde", path], capture_output=True, text=True,
                         timeout=WAIT_S).stdout
    return "\n".join(out.splitlines()[1:])


def root_state(path: str) -> tuple:
    """A root directory's own mode, ctime and ACL. A chmod moves ctime even
    when it sets the mode the directory already has."""
    st = os.stat(path)
    return (oct(stat.S_IMODE(st.st_mode)), st.st_ctime_ns, acl_text(path))


def roots_state(roots: dict[str, str]) -> dict[str, tuple]:
    return {label: root_state(path) for label, path in roots.items()}


def journal_is_wal(db_path: str) -> bool:
    try:
        with open(db_path, "rb") as fh:
            header = fh.read(20)
    except OSError:
        return False
    return len(header) == 20 and header[18] == 2 and header[19] == 2


def parent_key(key: str) -> str:
    label, _, rel = key.partition("/")
    parent = os.path.dirname(rel)
    return f"{label}/{parent}" if parent else f"{label}/."


def reader_protocol_only(changes: dict[str, str], before: dict[str, tuple],
                         after: dict[str, tuple], roots: dict[str, str]) -> dict[str, str]:
    """changes, minus what SQLite's WAL reader protocol does to a WAL database:
    a -shm that is new or changed, a -wal that is new and empty, and the mtime
    of the directory those new files appeared in."""
    left, gained = {}, set()
    for key, change in changes.items():
        label, _, rel = key.partition("/")
        stem, suffix = key[:-4], key[-4:]
        wal_db = suffix in ("-shm", "-wal") and journal_is_wal(
            os.path.join(roots[label], rel[:-4]))
        if wal_db and stem not in changes and (
                suffix == "-shm" or (change == "new" and after[key][2] == 0)):
            if change == "new":
                gained.add(parent_key(key))
            continue
        left[key] = change
    for key in list(left):
        if key in gained and key in before and key in after and \
                before[key][:-1] == after[key][:-1]:
            del left[key]  # a directory whose only change is its mtime
    return left


def make_read_only(top: str) -> None:
    for dirpath, dirnames, filenames in os.walk(top, topdown=False):
        for name in filenames:
            os.chmod(os.path.join(dirpath, name), 0o400)
        os.chmod(dirpath, 0o500)


def make_writable(top: str) -> None:
    if not os.path.isdir(top):
        return
    os.chmod(top, 0o700)
    for dirpath, dirnames, filenames in os.walk(top):
        for name in dirnames:
            os.chmod(os.path.join(dirpath, name), 0o700)
        for name in filenames:
            os.chmod(os.path.join(dirpath, name), 0o600)


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
    def __init__(self, argv: list[str], env: dict[str, str], cwd: str, stderr_path: str):
        self._stderr = open(stderr_path, "wb")
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
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


def refused(argv: list[str], env: dict[str, str], cwd: str, reason: str) -> None:
    """Start a session that must refuse to start: initialize unanswered, a
    nonzero exit, and the reason both on stderr and in a JSON-RPC error -- an
    MCP host shows the client only the latter."""
    done = subprocess.run(argv, input=INITIALIZE + "\n", capture_output=True, text=True,
                          env=env, cwd=cwd, timeout=WAIT_S)
    if '"result"' in done.stdout:
        fail(f"a session that must be refused answered initialize: {done.stdout[:300]}")
    if done.returncode == 0:
        fail(f"a session that must be refused exited 0; stderr: {done.stderr[:300]}")
    try:
        error = json.loads(done.stdout.splitlines()[0])["error"]
    except (IndexError, ValueError, KeyError, TypeError):
        fail(f"a refused session sent no JSON-RPC error: {done.stdout[:300]!r}")
    if reason not in error.get("message", "") or reason not in done.stderr:
        fail(f"a refused session did not say why ({reason!r}): error={error} "
             f"stderr={done.stderr[:300]!r}")


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

    def __init__(self, argv: list[str], env: dict[str, str], repo: str, stderr_path: str,
                 runtime: str, project: str, other: str | None):
        session = Session(argv, env, repo, stderr_path)
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
            # scope "tests" stages a test-impact work tree of its own.
            if "detect_changes" in self.tools:
                is_error, text = session.call("detect_changes", {**table["detect_changes"],
                                                                 "scope": "tests"})
                if is_error:
                    self.read_errors.append(f"detect_changes scope=tests: {text[:200]}")
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
            fail(f"CBM_IN_PROCESS could not read the indexes: {self.read_errors}")


def read_only_round(label: str, argv: list[str], env: dict[str, str], cache: str, repo: str,
                    scratch: str, runtime: str, home: str, project: str, other: str,
                    reader_protocol: bool = False) -> int:
    """A Round against a read-only cache: every read succeeds and nothing under
    it, its root included, or under runtime/ or HOME changes at all.
    reader_protocol admits SQLite's WAL reader files, for a cache whose
    permission bits do not bind (euid 0)."""
    roots = {"cache": cache, "runtime": runtime, "home": home}
    before, before_root = snapshot(roots), root_state(cache)
    round_ = Round(argv, dict(env, CBM_CACHE_DIR=cache), repo,
                   os.path.join(scratch, f"{label}.err"), runtime, project, other)
    after = snapshot(roots)
    changes = diff(before, after)
    if reader_protocol:
        changes = reader_protocol_only(changes, before, after, roots)
    report(f"CBM_IN_PROCESS changed a read-only cache ({label})", changes)
    if root_state(cache) != before_root:
        fail(f"CBM_IN_PROCESS changed the read-only cache root ({label}): "
             f"{before_root} -> {root_state(cache)}")
    round_.check_session()
    return len(round_.tools)


# --- read-only mounts ---------------------------------------------------------


def ro_bind_exec(directory: str, argv: list[str]) -> None:
    """--ro-bind-exec DIR -- ARGV: exec ARGV with DIR bind-mounted read-only, in
    an unprivileged user+mount namespace mapping this uid 1:1 (as
    tests/test_daemon_ipc.c's #1830 smoke does). ARGV starts with no
    capabilities: exec as a non-zero uid drops the namespace's. Exits
    UNAVAILABLE, naming the step, if the namespace or mount cannot be made."""
    import ctypes
    import ctypes.util

    libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.so.6", use_errno=True)
    libc.unshare.argtypes = [ctypes.c_int]
    libc.mount.argtypes = [ctypes.c_char_p, ctypes.c_char_p, ctypes.c_char_p,
                           ctypes.c_ulong, ctypes.c_void_p]
    clone_newns, clone_newuser = 0x00020000, 0x10000000
    ms_rdonly, ms_nosuid, ms_nodev, ms_noexec, ms_remount = 1, 2, 4, 8, 32
    ms_noatime, ms_nodiratime, ms_bind, ms_rec = 1024, 2048, 4096, 16384
    ms_private, ms_relatime = 1 << 18, 1 << 21

    def unavailable(step: str, err: int | None = None) -> None:
        err = ctypes.get_errno() if err is None else err
        print(f"{step}: {os.strerror(err) if err else 'failed'}", file=sys.stderr)
        sys.exit(UNAVAILABLE)

    uid, gid = os.geteuid(), os.getegid()
    if libc.unshare(clone_newuser | clone_newns) != 0:
        unavailable("unshare(CLONE_NEWUSER|CLONE_NEWNS)")
    for path, line in (("/proc/self/setgroups", "deny"), ("/proc/self/uid_map", f"{uid} {uid} 1"),
                       ("/proc/self/gid_map", f"{gid} {gid} 1")):
        try:
            with open(path, "w") as fh:
                fh.write(line)
        except OSError as exc:
            unavailable(f"write {path}", exc.errno)
    target = os.fsencode(directory)
    if libc.mount(None, b"/", None, ms_rec | ms_private, None) != 0:
        unavailable("make / private")
    if libc.mount(target, target, None, ms_bind, None) != 0:
        unavailable("bind mount")
    # A remount inside an unprivileged namespace must keep the flags the
    # mount was locked with, or it fails EPERM.
    locked, flags = 0, os.statvfs(directory).f_flag
    for st_flag, ms_flag in ((os.ST_NOSUID, ms_nosuid), (os.ST_NODEV, ms_nodev),
                             (os.ST_NOEXEC, ms_noexec), (os.ST_NOATIME, ms_noatime),
                             (os.ST_NODIRATIME, ms_nodiratime), (os.ST_RELATIME, ms_relatime)):
        if flags & st_flag:
            locked |= ms_flag
    if libc.mount(None, target, None, ms_remount | ms_bind | ms_rdonly | locked, None) != 0:
        unavailable("read-only remount")
    if not os.statvfs(directory).f_flag & os.ST_RDONLY:
        unavailable("read-only remount", errno.EINVAL)
    os.execv(argv[0], argv)


WRITE_PROBE = ("import errno, os, sys\n"
               "try:\n"
               "    os.close(os.open(os.path.join(sys.argv[1], '.write-probe'),"
               " os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600))\n"
               "except OSError as exc:\n"
               "    print(errno.errorcode.get(exc.errno, exc.errno))\n"
               "else:\n"
               "    print('WRITABLE')\n")


def linux_read_only_mount(cache: str) -> tuple[list[str] | None, str]:
    """(argv prefix that runs a command with cache read-only, "") or (None, why
    the mount is unavailable). The prefix is proven first: a write through it
    must fail with EROFS."""
    prefix = [sys.executable, "-I", os.path.abspath(__file__), "--ro-bind-exec", cache, "--"]
    probe = subprocess.run(prefix + [sys.executable, "-I", "-c", WRITE_PROBE, cache],
                           capture_output=True, text=True, timeout=WAIT_S)
    if probe.returncode == UNAVAILABLE:
        return None, probe.stderr.strip()
    if probe.returncode != 0 or probe.stdout.strip() != "EROFS":
        fail(f"read-only bind mount preflight: expected EROFS, got rc={probe.returncode} "
             f"stdout={probe.stdout.strip()!r} stderr={probe.stderr.strip()[:300]!r}")
    return prefix, ""


def macos_read_only_mount(source: str, image: str, mountpoint: str) -> str:
    """Attach a read-only disk image of source at mountpoint; "" on success,
    otherwise why it is unavailable. The caller detaches it."""
    def hdiutil(args: list[str]) -> tuple[int, str]:
        try:
            done = subprocess.run(["hdiutil"] + args, capture_output=True, text=True,
                                  timeout=120)
        except subprocess.TimeoutExpired:
            return -1, "timed out after 120s"
        return done.returncode, done.stderr.strip()[:200]

    for attempt in range(3):
        rc, why = hdiutil(["create", "-quiet", "-ov", "-srcfolder", source, "-format", "UDRO",
                           "-fs", "HFS+", "-volname", "cbmro", image])
        if rc == 0:
            break
        time.sleep(1.0)  # "Resource busy" retries; the outcome is still checked below
    else:
        return f"hdiutil create: {why}"
    os.mkdir(mountpoint, 0o700)
    rc, why = hdiutil(["attach", "-quiet", "-noverify", "-nobrowse", "-readonly",
                       "-mountpoint", mountpoint, image])
    if rc != 0:
        return f"hdiutil attach: {why}"
    probe = subprocess.run([sys.executable, "-I", "-c", WRITE_PROBE,
                            os.path.join(mountpoint, "cache")],
                           capture_output=True, text=True, timeout=WAIT_S)
    if probe.stdout.strip() != "EROFS":
        fail(f"read-only disk image preflight: expected EROFS, got {probe.stdout.strip()!r}")
    return ""


def macos_detach(mountpoint: str) -> None:
    """Never raises: it runs in a finally, and the .sh trap force-detaches too."""
    for args in (["detach", "-quiet", mountpoint], ["detach", "-quiet", "-force", mountpoint]):
        if not os.path.ismount(mountpoint):
            return
        try:
            subprocess.run(["hdiutil"] + args, capture_output=True, timeout=120)
        except subprocess.TimeoutExpired:
            pass


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
    """Index through the daemon. It answers only once the index worker has
    exited and the database is sealed and renamed into place, so the index is
    fully published when this returns."""
    out = run([binary, "cli", "index_repository", "--repo-path", repo], env,
              "cli index_repository")
    try:
        project = json.loads(out)["project"]
    except (ValueError, KeyError):
        fail(f"cli index_repository did not report a project: {out[:300]}")
    if not os.path.isfile(os.path.join(cache, f"{project}.db")):
        fail(f"the daemon did not publish {project}.db")
    return project


def log_match(log: str, pattern: str):
    try:
        with open(log, encoding="utf-8", errors="replace") as fh:
            return re.search(pattern, fh.read(), re.MULTILINE)
    except FileNotFoundError:
        return None


def pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def main() -> None:
    if len(sys.argv) > 4 and sys.argv[1] == "--ro-bind-exec" and sys.argv[3] == "--":
        ro_bind_exec(sys.argv[2], sys.argv[4:])
    if len(sys.argv) != 3:
        print(__doc__, file=sys.stderr)
        sys.exit(2)
    binary, scratch = sys.argv[1], sys.argv[2]
    runtime, cache = os.path.join(scratch, "runtime"), os.path.join(scratch, "cache")
    home = os.path.join(scratch, "home")
    os.mkdir(home, 0o700)

    # Hermetic git for the fixture and for the git that CBM itself runs: no
    # developer or runner config (default branch, signing) leaks in. The
    # daemon log is read below, so it keeps its default level and format.
    env = {k: v for k, v in os.environ.items()
           if k not in ("CBM_IN_PROCESS", "CBM_ALLOWED_ROOT", "CBM_DIAGNOSTICS",
                        "CBM_LOG_LEVEL", "CBM_LOG_FORMAT")}
    env.update(CBM_RUNTIME_DIR=runtime, CBM_CACHE_DIR=cache, HOME=home,
               GIT_CONFIG_NOSYSTEM="1", GIT_CONFIG_GLOBAL=os.devnull)
    in_process = dict(env, CBM_IN_PROCESS="1")
    roots = {"runtime": runtime, "cache": cache, "home": home}
    repo = make_repo(os.path.join(scratch, "repo"), env)
    repo2 = make_repo(os.path.join(scratch, "repo2"), env)
    daemon_log = os.path.join(cache, "logs", "cbm-daemon.log")

    # Leg 1: fresh state, no daemon -- nothing may appear anywhere, and the
    # roots themselves are left exactly as they were.
    roots_before = roots_state(roots)
    fresh = Round([binary], in_process, repo, os.path.join(scratch, "leg1.err"),
                  runtime, "never-indexed", None)
    created = sorted(key for key in snapshot(roots) if not key.endswith("/."))
    if created:
        fail(f"CBM_IN_PROCESS created shared state from a fresh start: {created}")
    if roots_state(roots) != roots_before:
        fail(f"CBM_IN_PROCESS changed a root directory: {roots_before} -> {roots_state(roots)}")
    fresh.check_session()
    print("ok: fresh start -- every mutation refused, runtime/cache/HOME still empty, "
          "cache root mode/ctime/ACL unchanged")

    # A cache that is not already owner-only is refused, never repaired; a
    # missing one is refused, never created.
    shared = os.path.join(scratch, "shared-cache")
    os.mkdir(shared)
    os.chmod(shared, 0o750)
    refusals = [("group-accessible", shared, "grants group or other access")]
    if sys.platform == "darwin":
        acl = os.path.join(scratch, "acl-cache")
        os.mkdir(acl, 0o700)
        subprocess.run(["chmod", "+a", "group:everyone allow list,search", acl], check=True,
                       timeout=WAIT_S)
        refusals.append(("allow-ACL", acl, "extended ACL"))
    for label, path, reason in refusals:
        before = root_state(path)
        refused([binary], dict(in_process, CBM_CACHE_DIR=path), repo, reason)
        if root_state(path) != before:
            fail(f"CBM_IN_PROCESS changed a {label} cache it refused: "
                 f"{before} -> {root_state(path)}")
    missing = os.path.join(scratch, "missing", "cache")
    refused([binary], dict(in_process, CBM_CACHE_DIR=missing), repo, "does not exist")
    if os.path.lexists(os.path.dirname(missing)):
        fail("CBM_IN_PROCESS created a missing cache directory it refused")
    print(f"ok: refused {', '.join(label for label, _, _ in refusals)} and missing caches "
          "before initialize, saying why over JSON-RPC, each left exactly as it was")

    # Leg 2: a live daemon owns two freshly published indexes. Its watcher is
    # off, so nothing in it reacts to the repositories or to sessions: the
    # setting is read once at daemon start, which is why it is set first --
    # after leg 1, since `config` itself coordinates through runtime/ and
    # writes _config.db.
    run([binary, "config", "set", "watcher_enabled", "false"], env, "config set watcher_enabled")
    run([binary, "config", "set", "auto_index", "false"], env, "config set auto_index")
    run([binary, "daemon", "start"], env, "daemon start")
    started = wait_until(lambda: log_match(daemon_log, r"msg=daemon\.start\b.*\bpid=(\d+)"),
                         f"msg=daemon.start in {daemon_log}")
    pid = int(started.group(1))
    if not log_match(daemon_log, r"msg=watcher\.disabled\b.*\breason=config\b") or \
            log_match(daemon_log, r"msg=watcher\.start\b"):
        fail("the daemon did not start with its watcher disabled by config")
    project, other = index(binary, repo, env, cache), index(binary, repo2, env, cache)
    if not os.listdir(runtime):
        fail("a live daemon left CBM_RUNTIME_DIR empty; the runtime check would be vacuous")

    baseline = snapshot(roots)
    alongside = Round([binary], in_process, repo, os.path.join(scratch, "leg2.err"),
                      runtime, project, other)
    report("CBM_IN_PROCESS changed shared state next to a live daemon",
           diff(baseline, snapshot(roots)))
    alongside.check_session()
    if not pid_alive(pid):
        fail(f"the daemon (pid {pid}) exited during the in-process session")
    print(f"ok: alongside daemon pid {pid} -- served {len(alongside.tools)} read tools from its "
          "indexes, refused every mutation, held no socket or runtime descriptor, changed nothing")

    # Positive control, and "false" means off: this session goes through the
    # daemon, so the descriptor check must see its coordination. Its ADR write
    # also leaves the index in WAL mode for leg 3, as real use does.
    control = Session([binary], dict(env, CBM_IN_PROCESS="false"), repo,
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

    # Leg 3: the same next to a WAL-mode index. The baseline waits for the one
    # state leg 3 depends on: the daemon's last connections to the database
    # and to its ADR sidecar have closed, which deletes their -wal and -shm.
    db = os.path.join(cache, f"{project}.db")
    if not journal_is_wal(db):
        fail(f"{project}.db is not in WAL mode after a daemon-side write; leg 3 would be vacuous")
    sidecars = [f"{path}{suffix}" for path in (db, f"{db}.adr.db") for suffix in ("-wal", "-shm")]
    wait_until(lambda: not any(os.path.exists(path) for path in sidecars),
               f"the daemon to close {project}.db and its ADR sidecar (-wal and -shm removed)")
    baseline = snapshot(roots)
    wal = Round([binary], in_process, repo, os.path.join(scratch, "leg3.err"),
                runtime, project, other)
    after = snapshot(roots)
    report("CBM_IN_PROCESS changed shared state next to a WAL-mode index (only SQLite's "
           "reader -shm and an empty -wal are allowed)",
           reader_protocol_only(diff(baseline, after), baseline, after, roots))
    wal.check_session()
    print("ok: WAL-mode index -- every database byte-identical, no WAL frames, only SQLite's "
          "reader -shm")

    # `daemon stop` returns once the daemon accepts the request; it logs
    # daemon.stop only after shutting down, and exits after that.
    run([binary, "daemon", "stop"], env, "daemon stop")
    wait_until(lambda: log_match(daemon_log, r"msg=daemon\.stop\b"),
               f"msg=daemon.stop in {daemon_log}")
    wait_until(lambda: not pid_alive(pid), f"daemon pid {pid} to exit")

    # Leg 4: a read-only copy of that cache (directories 0500, files 0400).
    snap = os.path.join(scratch, "snap")
    os.mkdir(snap, 0o700)
    shutil.copytree(cache, os.path.join(snap, "cache"), symlinks=True)
    dac = os.path.join(scratch, "dac")
    os.mkdir(dac, 0o700)
    dac_cache = os.path.join(dac, "cache")
    shutil.copytree(cache, dac_cache, symlinks=True)
    make_read_only(dac_cache)
    try:
        # As euid 0 the permission bits do not bind: SQLite may open the
        # copied 0400 -shm read-write, as any reader of a writable WAL
        # database does, so that much is allowed there.
        served = read_only_round("leg4", [binary], in_process, dac_cache, repo, scratch,
                                 runtime, home, project, other,
                                 reader_protocol=os.geteuid() == 0)
    finally:
        make_writable(dac_cache)
    note = (" -- PARTIAL: euid 0 bypasses permission bits, so this leg proves only no chmod "
            "and no write beyond SQLite's reader protocol") if os.geteuid() == 0 else ""
    print(f"ok: read-only (0500/0400) cache -- served {served} read tools, root still 0500, "
          f"nothing changed{note}")

    # Leg 5: the same cache, with its ordinary 0700/0600 modes, on a genuinely
    # read-only mount.
    if sys.platform == "darwin":
        mountpoint = os.path.join(scratch, "romnt")
        why = macos_read_only_mount(snap, os.path.join(scratch, "ro.dmg"), mountpoint)
        try:
            if not why:
                served = read_only_round("leg5", [binary], in_process,
                                         os.path.join(mountpoint, "cache"), repo, scratch,
                                         runtime, home, project, other)
        finally:
            macos_detach(mountpoint)
        kind = "read-only disk image"
    elif sys.platform.startswith("linux"):
        prefix, why = linux_read_only_mount(os.path.join(snap, "cache"))
        if prefix:
            served = read_only_round("leg5", prefix + [binary], in_process,
                                     os.path.join(snap, "cache"), repo, scratch,
                                     runtime, home, project, other)
        kind = "read-only bind mount in a user+mount namespace"
    else:
        why, kind = f"no read-only mount method on {sys.platform}", "read-only mount"
    if why:
        print(f"SKIP: leg 5 ({kind}) unavailable: {why}")
    else:
        print(f"ok: {kind} (EROFS) -- served {served} read tools, nothing changed")
    print("PASS: CBM_IN_PROCESS writes no shared state and serves a read-only cache (#2072)")


if __name__ == "__main__":
    main()
