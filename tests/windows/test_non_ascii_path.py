"""GREEN regression guard — non-ASCII runtime and repo paths work on Windows.

Guards the fix for issue #636 / #357 (landed on main via #700) at the product
surface (real codebase-memory-mcp process, real SQLite DB, real stdio). Two
byte-identical polyglot fixtures (TypeScript + Go, #1959) are indexed: one under
an ASCII parent path, one under a non-ASCII parent path. The invariant under test:

    A byte-identical fixture must produce equivalent graph counts regardless of
    whether its absolute path contains non-ASCII characters.

Before #700 native Windows extracted only File/Folder nodes for every non-ASCII
copy (Latin-1 accents, Cyrillic, CJK, Greek) — zero definitions — while the ASCII
copy extracted functions/classes/methods. Root cause: each pipeline pass read
source bytes with plain fopen(path, "rb") (src/pipeline/pass_definitions.c,
pass_calls.c, …); on Windows fopen() interprets the UTF-8 path in the active ANSI
code page, so a non-ASCII path could not be opened and the parser received
nothing. #700 routed the per-pass reads through cbm_fopen (→ _wfopen with a wide
path, src/foundation/compat_fs.c), so non-ASCII paths now parse identically.

This Windows guard also exercises native runtime publication beyond legacy
MAX_PATH; it fails (red) if either path contract regresses.

Exit code: 0 == invariant holds (green), 1 == invariant violated (regression),
2 == environment/setup error.

Usage:
    python test_non_ascii_path.py <path-to-codebase-memory-mcp[.exe]>
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from mcp_stdio import McpServer, wait_projects_with_stats  # noqa: E402

MATH_TS = (
    "export function add(a: number, b: number): number { return a + b; }\n"
    "export function mul(a: number, b: number): number { return add(a, a); }\n"
    "export class Calc {\n"
    "  total: number = 0;\n"
    "  push(x: number): void { this.total = add(this.total, x); }\n"
    "}\n"
)
MAIN_TS = (
    'import { add, mul, Calc } from "./math";\n'
    "function run(): number {\n"
    "  const c = new Calc();\n"
    "  c.push(add(1, 2));\n"
    "  return mul(3, 4);\n"
    "}\n"
    "run();\n"
)

# Dummy Go package covering the shapes the Go/cgo extraction work (#1932
# family) showed were previously invisible to CI: struct fields and a method,
# a build-tag twin pair sharing one function name, a cgo file (C preamble,
# `C.` call, `//export` directive), and a channel producer/consumer pair
# (#1959). Go qualified names embed the containing directory, so these also
# put non-ASCII repo paths through the Go pipeline passes.
GO_MOD = "module example.com/fixture\n\ngo 1.22\n"

GO_STORE = """package gopkg

type Store struct {
	Total int
	Name  string
}

func (s *Store) Push(x int) {
	s.Total = s.Total + x
}

func NewStore(name string) *Store {
	return &Store{Name: name}
}
"""

GO_FLUSH_LINUX = """//go:build linux

package gopkg

func FlushDisk() int { return 1 }
"""

GO_FLUSH_WINDOWS = """//go:build windows

package gopkg

func FlushDisk() int { return 2 }
"""

GO_BRIDGE = """package gopkg

/*
#include <stdio.h>
static void announce(void) { puts("hi"); }
*/
import "C"

//export FixtureReady
func FixtureReady() int { return 3 }

func Announce() {
	C.announce()
}
"""

GO_EVENTS = """package gopkg

func Produce(events chan string) {
	events <- "tick"
}

func Drain(events chan string) string {
	v := <-events
	return v
}
"""

GO_FILES = (
    ("go.mod", GO_MOD),
    ("gopkg/store.go", GO_STORE),
    ("gopkg/flush_linux.go", GO_FLUSH_LINUX),
    ("gopkg/flush_windows.go", GO_FLUSH_WINDOWS),
    ("gopkg/bridge.go", GO_BRIDGE),
    ("gopkg/events.go", GO_EVENTS),
)

# Distinct non-ASCII scripts — each must behave like the ASCII baseline.
NON_ASCII_SEGMENTS = {
    "latin1_accents": "café_repo",
    "cyrillic": "проект_repo",
    "cjk": "日本語_repo",
    "greek": "Ωμέγα_repo",
}


def windows_extended_path(path):
    """Return a Python/Win32 path that is independent of LongPathsEnabled."""
    absolute = os.path.abspath(path).replace("/", "\\")
    if absolute.startswith("\\\\?\\"):
        return absolute
    if absolute.startswith("\\\\"):
        return "\\\\?\\UNC\\" + absolute[2:]
    return "\\\\?\\" + absolute


def run_product(argv, cwd, env, timeout=120):
    options = {
        "cwd": cwd,
        "env": env,
        "stdout": subprocess.PIPE,
        "stderr": subprocess.STDOUT,
        "text": True,
        "timeout": timeout,
    }
    # With lpApplicationName=NULL, CreateProcess applies the legacy MAX_PATH
    # limit while deriving the module name from the command line. Python's
    # explicit `executable` argument fills lpApplicationName, allowing the
    # extended path to reach the product exactly as a native launcher would.
    if os.name == "nt" and argv and argv[0].startswith("\\\\?\\"):
        options["executable"] = argv[0]
    return subprocess.run(argv, **options)


def verify_relocated_runtime(binary, work):
    """Probe relocation, then install/probe/uninstall beyond legacy MAX_PATH.

    This binds Windows self-discovery to GetModuleFileNameW: a process launched
    from an extended-length, non-ASCII path must resolve its own location
    without an ANSI-code-page fallback. The binary is self-contained, so the
    whole runtime is one file. The real install sequence also binds target
    existence checks: after a secure first install, the target is rewritten in
    place and a non-force `--no` install must preserve those exact bytes.
    """
    source_dir = os.path.dirname(binary)
    runtime_dir = os.path.join(work, "café_日本語_runtime")
    os.makedirs(runtime_dir, exist_ok=True)
    relocated = os.path.join(runtime_dir, os.path.basename(binary))
    shutil.copy2(binary, relocated)

    env = os.environ.copy()
    env.pop("CBM_ASSETS_DIR", None)
    env.pop("CBM_UI_ASSETS_DIR", None)
    env["HOME"] = os.path.join(work, "runtime_home")
    env["USERPROFILE"] = env["HOME"]
    env["APPDATA"] = os.path.join(work, "runtime_appdata")
    env["LOCALAPPDATA"] = os.path.join(work, "runtime_localappdata")
    env["XDG_CONFIG_HOME"] = os.path.join(work, "runtime_xdg_config")
    env["XDG_CACHE_HOME"] = os.path.join(work, "runtime_xdg_cache")
    env["XDG_DATA_HOME"] = os.path.join(work, "runtime_xdg_data")
    env["CBM_CACHE_DIR"] = os.path.join(work, "runtime_cache")
    for inherited_config in (
            "CLAUDE_CONFIG_DIR", "CODEX_HOME", "COPILOT_HOME",
            "CRUSH_GLOBAL_CONFIG", "OPENCLAW_CONFIG_PATH", "OPENCLAW_HOME",
            "OPENCLAW_PROFILE", "OPENCLAW_STATE_DIR", "OPENCLAW_WORKSPACE_DIR",
            "OPENCODE_CONFIG", "OPENCODE_CONFIG_DIR", "VIBE_HOME"):
        env.pop(inherited_config, None)
    for directory in (
            env["HOME"], env["APPDATA"], env["LOCALAPPDATA"],
            env["XDG_CONFIG_HOME"], env["XDG_CACHE_HOME"],
            env["XDG_DATA_HOME"], env["CBM_CACHE_DIR"]):
        os.makedirs(directory, exist_ok=True)

    def stop_runtime_daemon():
        try:
            first = run_product(
                [relocated, "daemon", "stop"], runtime_dir, env, timeout=30)
            second = run_product(
                [relocated, "daemon", "stop"], runtime_dir, env, timeout=30)
            return first.returncode == 0 and second.returncode == 0
        except (OSError, subprocess.TimeoutExpired):
            return False

    def fail(message):
        stop_runtime_daemon()
        return message

    probe = run_product([relocated, "--version"], runtime_dir, env)
    if probe.returncode != 0:
        return fail("runtime probe rc=%d output=%r" % (
            probe.returncode, probe.stdout[-800:]))

    # Keep every component below the per-component limit while making the
    # complete UTF-16 path decisively longer than legacy MAX_PATH. The product,
    # not Python, creates this tree during the first real install.
    target = os.path.join(
        work,
        "install_Ωμέγα",
        *("segment_%02d_%s" % (i, "x" * 32) for i in range(7)),
        "bin",
    )
    installed = os.path.join(target, "codebase-memory-mcp.exe")
    if len(os.path.abspath(installed)) <= 260:
        return fail("setup: intended long install path is only %d characters" % len(
            os.path.abspath(installed)))

    first_install = run_product(
        [relocated, "install", "--force", "--skip-config", "--yes",
         "--dir=" + target],
        runtime_dir,
        env,
    )
    if first_install.returncode != 0:
        return fail("long-path install rc=%d output=%r path_len=%d" % (
            first_install.returncode,
            first_install.stdout[-1200:],
            len(os.path.abspath(installed)),
        ))

    installed_extended = windows_extended_path(installed)
    if not os.path.isfile(installed_extended):
        return fail("long-path install did not publish %r" % installed)

    installed_probe = run_product(
        [installed_extended, "--version"], runtime_dir, env)
    if installed_probe.returncode != 0:
        return fail("installed long-path probe rc=%d output=%r" % (
            installed_probe.returncode, installed_probe.stdout[-1200:]))

    # Preserve the file identity/owner/DACL created by the native transaction,
    # but make the bytes visibly foreign. A UTF-8-aware existence check sees
    # this target and honors --no; the legacy narrow stat() treated it as absent
    # and silently replaced it.
    sentinel = b"cbm-long-path-existing-target-must-be-preserved\n"
    with open(installed_extended, "wb") as stream:
        stream.write(sentinel)
    keep = run_product(
        [relocated, "install", "--skip-config", "--no", "--dir=" + target],
        runtime_dir,
        env,
    )
    if keep.returncode != 0:
        return fail("long-path non-force install rc=%d output=%r" % (
            keep.returncode, keep.stdout[-1200:]))
    with open(installed_extended, "rb") as stream:
        retained = stream.read()
    if retained != sentinel:
        return fail("long-path existing target was overwritten despite --no "
                    "(target existence probe is not extended-path safe)")

    restore = run_product(
        [relocated, "install", "--force", "--skip-config", "--yes",
         "--dir=" + target],
        runtime_dir,
        env,
    )
    if restore.returncode != 0:
        return fail("long-path restore install rc=%d output=%r" % (
            restore.returncode, restore.stdout[-1200:]))
    restored_probe = run_product(
        [installed_extended, "--version"], runtime_dir, env)
    if restored_probe.returncode != 0:
        return fail("restored long-path probe rc=%d output=%r" % (
            restored_probe.returncode, restored_probe.stdout[-1200:]))
    if not stop_runtime_daemon():
        return fail("could not retire the isolated daemon before uninstall")

    uninstall = run_product(
        [installed_extended, "uninstall", "--yes", "--dir=" + target],
        runtime_dir,
        env,
    )
    if uninstall.returncode != 0:
        return fail("long-path uninstall rc=%d output=%r" % (
            uninstall.returncode, uninstall.stdout[-1200:]))
    if os.path.exists(installed_extended):
        return fail("long-path uninstall retained the installed executable")
    stop_runtime_daemon()
    return None


def make_fixture(root):
    src = os.path.join(root, "src")
    os.makedirs(src, exist_ok=True)
    for name, text in (("math.ts", MATH_TS), ("main.ts", MAIN_TS)):
        with open(os.path.join(src, name), "wb") as f:
            f.write(text.encode("utf-8"))  # exact bytes, identical across copies
    for rel, text in GO_FILES:
        path = os.path.join(root, rel.replace("/", os.sep))
        os.makedirs(os.path.dirname(path) or root, exist_ok=True)
        with open(path, "wb") as f:
            f.write(text.encode("utf-8"))


def no_project_error(index_txt, repo, cache):
    """Assemble the venue diagnostics for an index that left no project row.

    The count summary cannot explain a venue-specific empty listing; carry the
    index response, the cache contents and the supervisor's worker logs (the
    only record of the pipeline's own error) into the CI log.
    """
    try:
        cache_entries = sorted(os.listdir(cache))
    except OSError as exc:
        cache_entries = ["<listdir failed: %s>" % exc]
    log_tails = []
    logs_dir = os.path.join(cache, "logs")
    if os.path.isdir(logs_dir):
        for log_name in sorted(os.listdir(logs_dir)):
            try:
                with open(os.path.join(logs_dir, log_name), "rb") as lf:
                    tail = lf.read()[-800:].decode("utf-8", "replace")
                log_tails.append("%s: %s" % (log_name, tail))
            except OSError as exc:
                log_tails.append("%s: <unreadable: %s>" % (log_name, exc))
    try:
        repo_entries = sorted(os.listdir(repo))
    except OSError as exc:
        repo_entries = ["<listdir failed: %s>" % exc]
    return {"error": "no project listed after index; index said %r; "
                     "cache holds %r; repo holds %r; worker logs: %s"
                     % (index_txt[:400], cache_entries, repo_entries,
                        " | ".join(log_tails) or "<none>")}


def index_and_count(binary, repo, cache):
    """Index `repo` into an isolated cache and return label-resolved counts."""
    os.makedirs(cache, exist_ok=True)
    with McpServer(binary, cache_dir=cache) as s:
        s.initialize()
        resp = s.call_tool("index_repository", {"repo_path": repo}, timeout=180)
        index_txt, err = s.tool_text(resp)
        if err:
            return {"error": "index tools/call error: %r" % err}
        # The index response itself carries the synchronous, authoritative
        # counts ("nodes"/"edges"). list_projects publishes its stats columns
        # asynchronously — on some venues never within a one-shot session — so
        # gating on it misreads a healthy index as a setup failure (#1952).
        try:
            summary = json.loads(index_txt)
        except ValueError:
            summary = {}
        out = {"name": summary.get("project"), "nodes": summary.get("nodes"),
               "edges": summary.get("edges")}
        if out["nodes"] is None:
            # Payload without counts: fall back to list_projects, polled
            # because its stats row can trail the index on slow runners.
            projects, _ = wait_projects_with_stats(s)
            if not projects:
                return no_project_error(index_txt, repo, cache)
            p = projects[0]
            out = {"name": p.get("name"), "nodes": p.get("nodes"),
                   "edges": p.get("edges")}
        # Definition-level counts prove the parser ran (not just discovery).
        # query_graph defaults to TOON text; this scripted consumer requests
        # format="json" ({"columns":[...],"rows":[["<n>"]],...}) explicitly.
        name = out["name"]
        defs = 0
        for label in ("Function", "Class", "Method"):
            q = "MATCH (n:%s) RETURN count(n)" % label
            r = s.call_tool("query_graph",
                            {"query": q, "project": name, "format": "json"},
                            timeout=60)
            t, _ = s.tool_text(r)
            try:
                rows = json.loads(t).get("rows") or []
                if rows and rows[0]:
                    defs += int(rows[0][0])
            except Exception:
                pass
        out["definition_nodes"] = defs
        # Go-file definitions separately: the Go passes derive qualified names
        # from the containing directory, so they meet non-ASCII paths on a
        # different route than the TS passes; a Go-specific count catches a
        # regression that total counts could mask (#1959).
        go_defs = 0
        for label in ("Function", "Method"):
            q = ("MATCH (n:%s) WHERE n.file_path CONTAINS '.go' "
                 "RETURN count(n)" % label)
            r = s.call_tool("query_graph",
                            {"query": q, "project": name, "format": "json"},
                            timeout=60)
            t, _ = s.tool_text(r)
            try:
                rows = json.loads(t).get("rows") or []
                if rows and rows[0]:
                    go_defs += int(rows[0][0])
            except Exception:
                pass
        out["go_definition_nodes"] = go_defs
        return out


def main():
    if len(sys.argv) < 2:
        print("usage: python test_non_ascii_path.py <binary>")
        return 2
    binary = os.path.abspath(sys.argv[1])
    if not os.path.exists(binary):
        print("FAIL: binary not found: %s" % binary)
        return 2

    work = tempfile.mkdtemp(prefix="cbm_win_nonascii_")
    failures = []
    runtime_error = None
    try:
        runtime_error = verify_relocated_runtime(binary, work)
        if runtime_error:
            print("[FAIL] non-ASCII runtime relocation: %s" % runtime_error)
            print("\nREGRESSION (red): Unicode/extended-length runtime publication "
                  "or exact adjacent-asset resolution failed.")
            return 1
        print("[PASS] Unicode relocation + extended-length install/probe/uninstall")

        ascii_repo = os.path.join(work, "ascii_repo")
        make_fixture(ascii_repo)
        # The baseline is setup, not the surface under test: a cold runner can
        # lose the first index to daemon startup latency (#1952). One retry
        # against a fresh cache separates that environmental window from a
        # real indexing failure before the guard declares a precondition skip.
        base = {}
        for attempt in ("c_ascii", "c_ascii_retry"):
            base = index_and_count(binary, ascii_repo, os.path.join(work, attempt))
            if not base.get("error") and base.get("nodes"):
                break
            print("SETUP: ASCII baseline attempt %r did not index: %r"
                  % (attempt, base))
        if base.get("error") or not base.get("nodes"):
            print("SETUP FAIL: ASCII baseline did not index: %r" % base)
            return 2
        print("baseline (ASCII): nodes=%s edges=%s definitions=%s go=%s" %
              (base["nodes"], base["edges"], base["definition_nodes"],
               base["go_definition_nodes"]))
        if base["definition_nodes"] < 1:
            print("SETUP FAIL: ASCII baseline produced no definitions: %r" % base)
            return 2
        if base["go_definition_nodes"] < 1:
            print("SETUP FAIL: ASCII baseline extracted no Go definitions: %r"
                  % base)
            return 2

        for key, seg in NON_ASCII_SEGMENTS.items():
            repo = os.path.join(work, seg)
            make_fixture(repo)
            got = index_and_count(binary, repo, os.path.join(work, "c_" + key))
            ok = (not got.get("error")
                  and got.get("nodes") == base["nodes"]
                  and got.get("edges") == base["edges"]
                  and got.get("definition_nodes") == base["definition_nodes"]
                  and got.get("go_definition_nodes") == base["go_definition_nodes"])
            status = "PASS" if ok else "FAIL"
            print("[%s] non-ascii/%-14s nodes=%s edges=%s definitions=%s go=%s "
                  "(baseline %s/%s/%s/%s) name=%r" %
                  (status, key, got.get("nodes"), got.get("edges"),
                   got.get("definition_nodes"), got.get("go_definition_nodes"),
                   base["nodes"], base["edges"], base["definition_nodes"],
                   base["go_definition_nodes"], got.get("name")))
            if not ok:
                # The counts alone cannot explain a venue-specific failure;
                # surface the captured error verbatim so CI logs carry the
                # diagnosis instead of swallowing it.
                if got.get("error"):
                    print("       %s error: %s" % (key, got["error"]))
                # Discriminate order-dependence from path-dependence: the same
                # repo, fresh cache, immediately again. A passing retry means
                # the previous case's daemon interfered; a failing retry means
                # the path itself is the trigger.
                retry = index_and_count(binary, repo,
                                        os.path.join(work, "c2_" + key))
                print("       %s retry: nodes=%s error=%s" % (
                    key, retry.get("nodes"), retry.get("error", "")[:200] or None))
                if (not retry.get("error")
                        and retry.get("nodes") == base["nodes"]
                        and retry.get("edges") == base["edges"]
                        and retry.get("definition_nodes") == base["definition_nodes"]
                        and retry.get("go_definition_nodes")
                        == base["go_definition_nodes"]):
                    print("       %s retry matched baseline -- order-dependent, "
                          "not path-dependent" % key)
                else:
                    # The MCP result hides the pipeline's own diagnostics; the
                    # CLI entrypoint prints them. Same repo, third fresh cache.
                    cli_cache = os.path.join(work, "c3_" + key)
                    cli_env = os.environ.copy()
                    cli_env["CBM_CACHE_DIR"] = cli_cache
                    # CBM_PROFILE keeps the supervisor from unlinking the worker
                    # log on a clean exit (index.supervisor.profile_log), which
                    # is the only record of the pipeline's own diagnostics.
                    cli_env["CBM_PROFILE"] = "1"
                    cli = subprocess.run(
                        [binary, "cli", "index_repository",
                         json.dumps({"repo_path": repo})],
                        capture_output=True, timeout=180, env=cli_env)
                    cli_out = (cli.stdout or b"").decode("utf-8", "replace")
                    cli_err = (cli.stderr or b"").decode("utf-8", "replace")
                    print("       %s cli probe rc=%s\n       stdout: %s\n"
                          "       stderr: %s" % (key, cli.returncode,
                                                 cli_out[-900:], cli_err[-900:]))
                    probe_logs = os.path.join(cli_cache, "logs")
                    if os.path.isdir(probe_logs):
                        for log_name in sorted(os.listdir(probe_logs)):
                            try:
                                with open(os.path.join(probe_logs, log_name), "rb") as lf:
                                    tail = lf.read()[-1500:].decode("utf-8", "replace")
                            except OSError as exc:
                                tail = "<unreadable: %s>" % exc
                            print("       %s worker log %s:\n%s"
                                  % (key, log_name, tail))
                failures.append(key)
    finally:
        shutil.rmtree(work, ignore_errors=True)

    if failures:
        print("\nREGRESSION (red): %d/%d non-ASCII repo path variants lost "
              "definitions: %s" %
              (len(failures), len(NON_ASCII_SEGMENTS), ", ".join(failures)))
        print("Invariant violated: byte-identical fixtures under non-ASCII paths "
              "must extract the same definitions as the ASCII baseline (fixed by "
              "#700 — has the cbm_fopen routing in the pass readers regressed?).")
        return 1
    print("\nGREEN: Unicode/extended-length runtime operations passed and all "
          "non-ASCII repo variants matched the ASCII baseline.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
