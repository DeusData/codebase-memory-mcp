r"""RED integration test — the PreToolUse hook augmenter is a no-op on Windows.

Reproduces issue #618 at the product surface.

`codebase-memory-mcp hook-augment` is the non-blocking Claude Code PreToolUse
Grep/Glob augmenter: given a hook payload it should emit a `hookSpecificOutput`
with `additionalContext` listing graph symbols that match the searched token.

On Windows it emits nothing for every payload. `src/cli/hook_augment.c` gates on
POSIX-style absolute paths in two places:

    cbm_cmd_hook_augment (_WIN32 branch):  if (!cwd || cwd[0] != '/') return 0;
    ha_resolve_and_query walk-up loop:     for (... ; dir[0] == '/'; ...)

A Windows `cwd` is a drive-letter path (`C:\...` / `C:/...`), so `cwd[0]` is
never `'/'`; the augmenter bails before it ever queries the graph.

This test indexes a repo with a known symbol, confirms `search_graph` finds it
(control — proves the index and project name are fine), then invokes
`hook-augment` exactly as the installed PreToolUse hook does and asserts a
`hookSpecificOutput` payload is produced.

Passes on Linux/macOS (`cwd` starts with `/`). Fails on native Windows until the
path guards accept drive-letter absolute paths (and the walk-up loop climbs them).

Exit code: 0 == augmenter fired (green), 1 == no-op (red), 2 == setup error.

Usage:
    python test_hook_augment.py <path-to-codebase-memory-mcp[.exe]>
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

SYMBOL = "someIndexedSymbol"
SRC = "export function %s(a: number): number { return a + 1; }\n" % SYMBOL


def run_cli(binary, cache, args, stdin=None, timeout=120):
    env = dict(os.environ)
    env["CBM_CACHE_DIR"] = cache
    return subprocess.run([binary] + args, capture_output=True, timeout=timeout,
                          env=env, input=stdin)


def main():
    if len(sys.argv) < 2:
        print("usage: python test_hook_augment.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    work = tempfile.mkdtemp(prefix="cbm_win_hook_")
    try:
        repo = os.path.join(work, "repo")
        os.makedirs(os.path.join(repo, "src"), exist_ok=True)
        with open(os.path.join(repo, "src", "m.ts"), "wb") as f:
            f.write(SRC.encode("utf-8"))
        cache = os.path.join(work, "cache")
        os.makedirs(cache, exist_ok=True)

        # repo_path / cwd in the forward-slash drive form Claude Code passes.
        repo_fwd = repo.replace("\\", "/")
        idx = run_cli(binary, cache, ["cli", "index_repository",
                                      json.dumps({"repo_path": repo_fwd})])
        idx_out = (idx.stdout or b"").decode("utf-8", "replace")
        if '"nodes"' not in idx_out:
            print("SETUP FAIL: index did not run:\n%s" % idx_out[:300])
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
        print("\nRED: hook-augment produced no hookSpecificOutput on Windows "
              "(drive-letter cwd fails the cwd[0]=='/' guards in hook_augment.c).")
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
