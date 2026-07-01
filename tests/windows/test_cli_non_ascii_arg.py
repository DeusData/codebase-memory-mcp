"""RED integration test — `cli index_repository` rejects a non-ASCII repo_path.

Reproduces the CLI-argv half of issue #636 / #423 / #20 on native Windows.

The documented entrypoint `codebase-memory-mcp cli index_repository '<json>'`
receives its JSON argument through argv. main() is declared as
`int main(int argc, char **argv)` (src/main.c) — it does not use wmain /
GetCommandLineW — so on Windows the C runtime hands it argv in the active ANSI
code page. A repo_path containing non-ASCII characters is therefore mangled (or,
when yyjson rejects the now-invalid UTF-8, the whole argument is discarded), and
the command fails with "repo_path is required" / "Pipeline failed" instead of
indexing the real directory.

The directory itself is created with the Windows wide API (Python uses
CreateFileW/_wmkdir under the hood), so it genuinely exists on disk; only the
argv path delivery is lossy.

Passes on Linux/macOS (argv is UTF-8 bytes). Fails on native Windows until the
CLI reads the wide command line (GetCommandLineW + CommandLineToArgvW, or a
wmain entrypoint) and converts to UTF-8.

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
        arg = json.dumps({"repo_path": repo}, ensure_ascii=False)
        p = subprocess.run([binary, "cli", "index_repository", arg],
                           capture_output=True, timeout=120, env=env2)
        out = (p.stdout or b"").decode("utf-8", "replace")
        err = (p.stderr or b"").decode("utf-8", "replace")
        honored = '"nodes"' in out and '"nodes":0' not in out.replace(" ", "")
        print("ASCII control: indexed OK")
        print("non-ASCII argv: rc=%d" % p.returncode)
        print("  stdout: %s" % out[:200].replace("\n", " "))
        print("  stderr: %s" % err[-200:].replace("\n", " "))
        if honored:
            print("\nGREEN: CLI honored the non-ASCII repo_path.")
            return 0
        print("\nRED: CLI did not index the non-ASCII repo_path (argv delivered "
              "in the ANSI code page; main() does not read the wide command line).")
        return 1
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
