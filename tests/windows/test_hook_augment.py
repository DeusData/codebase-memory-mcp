r"""GREEN regression guard — the PreToolUse hook augmenter fires on Windows.

Guards the fix for issue #618 (landed on main via #619) at the product surface.

`codebase-memory-mcp hook-augment` is the non-blocking Claude Code PreToolUse
Grep/Glob augmenter: given a hook payload it should emit a `hookSpecificOutput`
with `additionalContext` listing graph symbols that match the searched token.

Before #619 it emitted nothing for every payload on Windows: `src/cli/hook_augment.c`
gated on POSIX-style absolute paths (`cwd[0] == '/'` and a walk-up loop over
`dir[0] == '/'`). A Windows `cwd` is a drive-letter path (`C:\...` / `C:/...`),
so `cwd[0]` was never `'/'` and the augmenter bailed before querying the graph.
#619 added `cbm_is_walkable_abs_path` (accepts `X:/` drive-letter roots), so the
augmenter now fires for a drive-letter cwd.

This test indexes a repo with a known symbol, confirms `search_graph` finds it
(control — proves the index and project name are fine), then invokes
`hook-augment` exactly as Claude's shell-free hook registration does: the
executable is a literal argv element, including a forward-slash Windows path,
and the hook payload is forwarded directly on stdin. It asserts a
`hookSpecificOutput` payload is produced and fails (red) if either product
contract regresses. Also passes on Linux/macOS (`cwd` starts with `/`).

Exit code: 0 == augmenter fired (green), 1 == no-op (regression), 2 == setup error.

Usage:
    python test_hook_augment.py <path-to-codebase-memory-mcp[.exe]>
"""
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time

SYMBOL = "someIndexedSymbol"
SRC = "export function %s(a: number): number { return a + 1; }\n" % SYMBOL


def run_cli(binary, cache, args, stdin=None, timeout=120):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    env["CBM_RUNTIME_DIR"] = os.path.join(cache, "runtime")
    return subprocess.run([binary] + args, capture_output=True, timeout=timeout,
                          env=env, input=stdin)


def main():
    if len(sys.argv) < 2:
        print("usage: python test_hook_augment.py <binary>")
        return 2
    # Claude's exec-form hook preserves this string exactly. On Windows, keep
    # the forward-slash spelling that previously crossed Git Bash incorrectly.
    binary = os.path.abspath(sys.argv[1]).replace("\\", "/")
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    # macOS resolves /tmp through a symlink; the daemon's secure path walk
    # accepts the canonical root-owned sticky parent at /private/tmp.
    temp_parent = "/private/tmp" if sys.platform == "darwin" else None
    work = tempfile.mkdtemp(prefix="cbm_win_hook_", dir=temp_parent)
    cache = None
    daemon_pid = 0
    daemon_started = False
    try:
        repo = os.path.join(work, "repo")
        os.makedirs(os.path.join(repo, "src"), exist_ok=True)
        with open(os.path.join(repo, "src", "m.ts"), "wb") as f:
            f.write(SRC.encode("utf-8"))
        cache = os.path.join(work, "cache")
        os.makedirs(cache, exist_ok=True)
        os.makedirs(os.path.join(cache, "runtime"), mode=0o700, exist_ok=True)

        # The CLI and hooks share the mandatory daemon. Start the supported
        # permanent mode before indexing so every product-surface command uses
        # the same isolated cache and daemon cohort.
        # Retry once: a cold runner's daemon startup latency (#1952) is setup
        # noise, not the surface under test.
        start_out = ""
        rc = 1
        for attempt in (1, 2):
            start = run_cli(binary, cache, ["daemon", "start"], timeout=60)
            start_out = (start.stdout or b"").decode("utf-8", "replace")
            start_err = (start.stderr or b"").decode("utf-8", "replace")
            rc = start.returncode
            print("daemon start attempt %d rc=%d stdout=%r stderr=%r" %
                  (attempt, rc, start_out[:120], start_err[:500]))
            if rc == 0:
                break
            time.sleep(2)
        if rc != 0:
            print("SETUP FAIL: permanent daemon did not start")
            return 2
        daemon_started = True
        pid_match = re.search(r"pid (\d+)", start_out)
        daemon_pid = int(pid_match.group(1)) if pid_match else 0

        # repo_path / cwd in the forward-slash drive form Claude Code passes.
        repo_fwd = repo.replace("\\", "/")
        # Setup, not the surface under test: retry once so a cold runner's
        # coordination-daemon startup latency (#1952) is not misread as a
        # broken CLI index. Reindexing the same cache is idempotent.
        idx_out = ""
        idx_err = ""
        for attempt in (1, 2):
            idx = run_cli(binary, cache, ["cli", "index_repository",
                                          json.dumps({"repo_path": repo_fwd})])
            idx_out = (idx.stdout or b"").decode("utf-8", "replace")
            idx_err = (idx.stderr or b"").decode("utf-8", "replace")
            if '"nodes"' in idx_out:
                break
            # Full stdout AND stderr: the reason lives on stderr (daemon
            # spawn/handshake diagnostics), and a head slice of stdout only
            # ever showed the allocator preamble.
            print("SETUP: index attempt %d did not run (rc=%s):\nstdout:\n%s\nstderr:\n%s"
                  % (attempt, idx.returncode, idx_out, idx_err))
            time.sleep(2)
        if '"nodes"' not in idx_out:
            print("SETUP FAIL: index did not run:\nstdout:\n%s\nstderr:\n%s"
                  % (idx_out, idx_err))
            return 2

        # Control: prove the symbol is indexed and queryable.
        lp = run_cli(binary, cache, ["cli", "list_projects", "{}"])
        projects = json.loads((lp.stdout or b"").decode("utf-8", "replace"))["projects"]
        name = projects[0]["name"]
        sg = run_cli(binary, cache, ["cli", "search_graph",
                     json.dumps({"label": "Function",
                                 "name_pattern": ".*%s.*" % SYMBOL,
                                 "project": name})])
        if SYMBOL not in (sg.stdout or b"").decode("utf-8", "replace"):
            print("SETUP FAIL: control search_graph did not find %s" % SYMBOL)
            return 2
        print("control: search_graph finds %s in project %s" % (SYMBOL, name))

        # Invoke hook-augment exactly as the installed PreToolUse hook does.
        payload = json.dumps({
            "hook_event_name": "PreToolUse",
            "tool_name": "Grep",
            "cwd": repo_fwd,
            "tool_input": {"pattern": SYMBOL},
        }).encode("utf-8")
        ha = run_cli(binary, cache, ["hook-augment"], stdin=payload, timeout=60)
        out = (ha.stdout or b"").decode("utf-8", "replace").strip()
        print("hook-augment rc=%d stdout=%r" % (ha.returncode, out[:200]))

        fired = ("hookSpecificOutput" in out) and ("additionalContext" in out)
        if fired:
            print("\nGREEN: PreToolUse augmenter emitted additionalContext.")
            return 0
        print("\nREGRESSION (red): hook-augment produced no hookSpecificOutput on "
              "Windows (drive-letter cwd rejected — has the #619 "
              "cbm_is_walkable_abs_path handling in hook_augment.c regressed?).")
        return 1
    finally:
        if daemon_started and cache:
            stopped = False
            try:
                stop = run_cli(binary, cache, ["daemon", "stop"], timeout=30)
                stopped = stop.returncode == 0
            except subprocess.TimeoutExpired:
                pass
            if not stopped and daemon_pid:
                subprocess.run(["taskkill" if os.name == "nt" else "kill",
                                "/F" if os.name == "nt" else "-9",
                               "/PID" if os.name == "nt" else str(daemon_pid)] +
                               ([str(daemon_pid)] if os.name == "nt" else []),
                               capture_output=True, timeout=30)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
