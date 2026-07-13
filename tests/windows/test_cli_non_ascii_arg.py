"""GREEN regression guard — `cli index_repository` honors a non-ASCII repo_path.

Guards the CLI-argv fix for issue #636 / #423 / #20 on native Windows.

The documented entrypoint `codebase-memory-mcp cli index_repository '<json>'`
receives its JSON argument through argv. main() used to take only the narrow
`int main(int argc, char **argv)` (src/main.c), so on Windows the C runtime handed
it argv in the active ANSI code page: a repo_path containing non-ASCII characters
was mangled (or, when yyjson rejected the now-invalid UTF-8, the whole argument was
discarded), and the command failed with "repo_path is required" / "Pipeline failed"
instead of indexing the real directory.

Fixed: on Windows main() now rebuilds argv from the wide command line
(GetCommandLineW + CommandLineToArgvW) and converts each element to UTF-8, so a
non-ASCII repo path survives. This test asserts that fix stays in place — it was RED
before it and is GREEN after. (It is inherently green on Linux/macOS, where argv is
already UTF-8 bytes.)

The directory itself is created with the Windows wide API (Python uses
CreateFileW/_wmkdir under the hood), so it genuinely exists on disk; only the
argv path delivery was lossy.

Exit code: 0 == honored (green), 1 == rejected/mangled (red), 2 == setup error.

Usage:
    python test_cli_non_ascii_arg.py <path-to-codebase-memory-mcp[.exe]>
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

MATH_TS = (
    "export function add(a: number, b: number): number { return a + b; }\n"
    "export class Calc { total = 0; push(x: number): void { this.total = "
    "add(this.total, x); } }\n"
)


def make_fixture(root):
    src = os.path.join(root, "src")
    os.makedirs(src, exist_ok=True)
    with open(os.path.join(src, "math.ts"), "wb") as f:
        f.write(MATH_TS.encode("utf-8"))


def main():
    if len(sys.argv) < 2:
        print("usage: python test_cli_non_ascii_arg.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    work = tempfile.mkdtemp(prefix="cbm_win_cliarg_")
    try:
        # Non-ASCII repo directory (created via the OS wide API → really exists).
        repo = os.path.join(work, "café_日本語_repo")
        make_fixture(repo)
        cache = os.path.join(work, "cache")
        os.makedirs(cache, exist_ok=True)

        # Sanity: an ASCII control path must index through the CLI, proving the
        # CLI path itself works and isolating the failure to argv encoding.
        ascii_repo = os.path.join(work, "ascii_repo")
        make_fixture(ascii_repo)
        env = dict(os.environ)
        env["CBM_CACHE_DIR"] = os.path.join(work, "cache_ascii")
        ctrl = subprocess.run(
            [binary, "cli", "index_repository",
             json.dumps({"repo_path": ascii_repo})],
            capture_output=True, timeout=120, env=env)
        ctrl_out = (ctrl.stdout or b"").decode("utf-8", "replace")
        if '"nodes"' not in ctrl_out:
            print("SETUP FAIL: ASCII control did not index via CLI:\n%s" %
                  ctrl_out[:300])
            return 2

        env2 = dict(os.environ)
        env2["CBM_CACHE_DIR"] = cache
        # Exercise the DEFAULT (supervisor-enabled) path, not in-process. The non-ASCII
        # repo path must survive BOTH the argv read (main() wide command line) AND the
        # supervisor -> worker spawn (CreateProcessW). The suite runner sets
        # CBM_INDEX_SUPERVISOR=0 for determinism across the other guards; forcing it OFF
        # here would run in-process and mask the spawn-boundary half of #423/#20, so we
        # drop the override and let the supervisor wrap the worker as it does for users.
        env2.pop("CBM_INDEX_SUPERVISOR", None)
        arg = json.dumps({"repo_path": repo}, ensure_ascii=False)
        p = subprocess.run([binary, "cli", "index_repository", arg],
                           capture_output=True, timeout=120, env=env2)
        out = (p.stdout or b"").decode("utf-8", "replace")
        err = (p.stderr or b"").decode("utf-8", "replace")
        honored = '"nodes"' in out and '"nodes":0' not in out.replace(" ", "")
        print("ASCII control: indexed OK")
        print("non-ASCII argv (supervised): rc=%d" % p.returncode)
        print("  stdout: %s" % out[:200].replace("\n", " "))
        print("  stderr: %s" % err[-200:].replace("\n", " "))
        # #973 variant: the reporter's exact shape — Traditional-Chinese dir,
        # FLAG-form --repo-path + --mode fast, supervised. This adds coverage
        # of the flag->JSON converter and the repo-path canonicalization,
        # which used ANSI _access/_fullpath (locale-dependent — corrupted CJK
        # paths on CJK-codepage systems) before the cbm_canonical_path fix.
        cjk_repo = os.path.join(work, "\u96f7\u9054\u6e2c\u8a66")
        make_fixture(cjk_repo)
        env3 = dict(os.environ)
        env3["CBM_CACHE_DIR"] = os.path.join(work, "cache_cjk")
        env3.pop("CBM_INDEX_SUPERVISOR", None)
        p2 = subprocess.run([binary, "cli", "index_repository",
                             "--repo-path", cjk_repo, "--mode", "fast"],
                            capture_output=True, timeout=120, env=env3)
        out2 = (p2.stdout or b"").decode("utf-8", "replace")
        cjk_ok = '"nodes"' in out2 and '"nodes":0' not in out2.replace(" ", "")
        print("CJK flag-form (--repo-path, supervised, fast): rc=%d" % p2.returncode)
        print("  stdout: %s" % out2[:200].replace("\n", " "))
        if not cjk_ok:
            print("\nRED: CJK flag-form repo_path was not indexed (#973 — "
                  "canonicalization or spawn boundary mangled the path).")
            return 1

        if honored:
            print("\nGREEN: CLI honored the non-ASCII repo_path (argv + worker spawn).")
            return 0
        print("\nRED: CLI did not index the non-ASCII repo_path — the path was mangled "
              "either in the argv read (narrow main()) or re-mangled at the "
              "supervisor->worker CreateProcess boundary (ANSI code page).")
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
