# Windows Red-Test Analysis

Deterministic, Windows-only red tests found during a native-Windows red-test
campaign. They reproduce platform-specific failures at the product surface and
are intended as regression guards while the underlying issues are fixed in
separate maintainer PRs. **This PR contains no production fixes.**

## Environment

- OS: Microsoft Windows 11 Pro, build 10.0.26200
- Source build: MinGW-w64 GCC 15.2.0 (MSYS2), `make -f Makefile.cbm cbm`
- Filesystem: NTFS, code page 65001 (UTF-8 console)
- Shells/launchers exercised: PowerShell 5.1 (5.1.26100), `cmd.exe`,
  Git Bash (MSYS2), direct Win32 process launch, Python `subprocess.Popen`,
  Python stdio (line-delimited JSON-RPC) transport
- CBM source commit under test: `b075f05`
- Binary: `build/c/codebase-memory-mcp.exe` (production build)

### Sanitizer note

The MinGW/LLVM toolchain available on this machine ships **no** `libasan` /
`libubsan`, so an AddressSanitizer/UBSan build is not possible natively (the plan
anticipates this). The C unit/invariant suite (`build/c/test-runner`) was built
with `SANITIZE=` and runs; the two red tests below are product-level integration
tests that drive a real `codebase-memory-mcp.exe` over stdio. On a host where the
toolchain provides sanitizers (Linux container, WSL), the same fixtures should be
run through an ASan/UBSan binary via `scripts/test.sh`.

## How to run

```powershell
# Builds build/c/codebase-memory-mcp.exe if missing, then runs the red suite.
pwsh -File scripts/test-windows.ps1
# or, against an installed/relocated binary:
pwsh -File scripts/test-windows.ps1 -Binary "C:\path\to\codebase-memory-mcp.exe"
```

Each test exits `0` (green / invariant holds), `1` (red / Windows failure
reproduced), or `2` (environment/setup error). Standard-library Python 3 only.

---

## windows_non_ascii_repo_path_preserves_definitions

- Class: integration
- Test: `tests/windows/test_non_ascii_path.py`
- Related issues: #636, #357, #571 (naming), #530
- Environment: Windows 11 26200, PowerShell 5.1 / Python stdio, NTFS, CP 65001
- Fixture: byte-identical 2-file TypeScript repo (`src/math.ts`, `src/main.ts`),
  copied to an ASCII parent path and to four non-ASCII parent paths
  (Latin-1 accents `café`, Cyrillic `проект`, CJK `日本語`, Greek `Ωμέγα`)
- Expected: each non-ASCII copy produces the same graph counts as the ASCII
  baseline (12 nodes / 20 edges / 5 definition nodes)
- Actual: every non-ASCII copy produces **5 nodes / 4 edges / 0 definition
  nodes** — only `File`/`Folder` nodes; zero `Function`/`Class`/`Method`
- Command: `python tests/windows/test_non_ascii_path.py build\c\codebase-memory-mcp.exe`
- Minimal failure output:

  ```
  baseline (ASCII): nodes=12 edges=20 definitions=5
  [FAIL] non-ascii/latin1_accents nodes=5 edges=4 definitions=0 (baseline 12/20/5)
  [FAIL] non-ascii/cyrillic       nodes=5 edges=4 definitions=0 (baseline 12/20/5)
  [FAIL] non-ascii/cjk            nodes=5 edges=4 definitions=0 (baseline 12/20/5)
  [FAIL] non-ascii/greek          nodes=5 edges=4 definitions=0 (baseline 12/20/5)
  ```

- Suspected implementation area: the per-pass source readers
  `read_file()` in `src/pipeline/pass_definitions.c`, `pass_calls.c`,
  `pass_parallel.c`, `pass_semantic.c` (and the `k8s`/`lsp_cross`/`pkgmap`
  variants) open files with plain `fopen(path, "rb")`. On Windows `fopen`
  interprets the UTF-8 path in the active **ANSI code page**, so a path with
  non-ASCII bytes cannot be opened and the tree-sitter parser receives no bytes.
  Directory discovery already uses the wide API
  (`cbm_utf8_to_wide` + `FindFirstFileW` in `src/foundation/compat_fs.c`,
  `src/foundation/platform.c`), which is why `File`/`Folder` nodes still appear
  while all definitions vanish. Fix direction: route the pass-level reads through
  the wide layer (`cbm_utf8_to_wide` + `_wfopen`), or add a shared
  UTF-8-aware file reader and use it from every pass.

Verified with `_wfopen` vs `fopen` on a non-ASCII path: `fopen(utf8, "rb")`
returns `NULL`, `_wfopen(cbm_utf8_to_wide(utf8), L"rb")` opens the same file.

This invariant holds on Linux/macOS (byte-transparent UTF-8 filesystem); the test
turns green once the pass readers convert to wide.

---

## windows_cli_non_ascii_repo_path_is_honored

- Class: integration
- Test: `tests/windows/test_cli_non_ascii_arg.py`
- Related issues: #636, #423, #20
- Environment: Windows 11 26200, `cli` argv path, NTFS, CP 65001
- Fixture: a TypeScript repo under a non-ASCII directory (`café_日本語_repo`),
  created with the OS wide API so it genuinely exists; an ASCII control repo
- Expected: `codebase-memory-mcp cli index_repository '{"repo_path":"<non-ascii>"}'`
  indexes the directory (ASCII control proves the CLI path works)
- Actual: the ASCII control indexes; the non-ASCII invocation fails with
  `repo_path is required` (the mangled, now-invalid-UTF-8 JSON argument is
  rejected) and exits non-zero
- Command: `python tests/windows/test_cli_non_ascii_arg.py build\c\codebase-memory-mcp.exe`
- Minimal failure output:

  ```
  ASCII control: indexed OK
  non-ASCII argv: rc=1
    stderr: ... repo_path is required
  ```

- Suspected implementation area: `int main(int argc, char **argv)` in
  `src/main.c` does not use `wmain` / `GetCommandLineW`, so on Windows the C
  runtime delivers `argv` in the ANSI code page. The non-ASCII bytes in the JSON
  argument are corrupted before `yyjson` parses them. Fix direction: read the
  wide command line on Windows (`GetCommandLineW` + `CommandLineToArgvW`, or a
  `wmain` entrypoint) and convert each argument to UTF-8.

Real MCP clients pass `repo_path` inside a JSON-RPC message over stdio (which is
byte-clean), so this affects the documented `cli` entrypoint and the hook/install
flows that shell out to it, not the stdio server path. Holds on Linux/macOS
(argv is UTF-8 bytes).

---

## windows_hook_augment_emits_context

- Class: integration
- Test: `tests/windows/test_hook_augment.py`
- Related issues: #618
- Environment: Windows 11 26200, `hook-augment` CLI subcommand
- Fixture: a repo with a known function `someIndexedSymbol`, indexed; a realistic
  Claude Code PreToolUse Grep payload with a Windows drive-letter `cwd`
- Expected: `codebase-memory-mcp hook-augment` emits a `hookSpecificOutput` with
  `additionalContext` listing the matching graph symbol (the control
  `search_graph` finds the symbol, so the index and project name are fine)
- Actual: `hook-augment` emits **empty stdout** for every payload
- Command: `python tests/windows/test_hook_augment.py build\c\codebase-memory-mcp.exe`
- Minimal failure output:

  ```
  control: search_graph finds someIndexedSymbol in project C-...-repo
  hook-augment rc=0 stdout=''
  ```

- Suspected implementation area: `src/cli/hook_augment.c` has two POSIX-only path
  guards. `cbm_cmd_hook_augment` (`_WIN32` branch, ~L330):
  `if (!cwd || cwd[0] != '/') { ...; return 0; }` and the `ha_resolve_and_query`
  walk-up loop (~L254): `for (...; dir[0] == '/'; ...)`. A Windows `cwd` is a
  drive-letter path (`C:\...` / `C:/...`), so `cwd[0]` is never `'/'`; the
  augmenter bails before it queries the graph. The PreToolUse Grep/Glob graph
  augmentation therefore never fires on Windows. Fix direction: accept
  drive-letter absolute paths (and climb them in the walk-up loop).

Holds on Linux/macOS (`cwd` starts with `/`).

---

## windows_ui_picker_reaches_all_drives

- Class: integration
- Test: `tests/windows/test_ui_drive_listing.py`
- Related issues: #548
- Environment: Windows 11 26200 with drives `C:\`, `D:\`, `E:\`; UI build
  (`make -f Makefile.cbm cbm-with-ui`); embedded HTTP server on a local port
- Fixture: none — exercises the live `GET /api/browse` endpoint
- Expected: browsing the filesystem root (`/api/browse?path=/`) lets the user
  reach every fixed drive (`D:\`, `E:\`), so a project on a non-system drive can
  be selected
- Actual: the control browse of an explicit directory returns entries (endpoint
  works), but `browse('/')` returns **0 entries** and no drive letters — `D:\`
  and `E:\` are unreachable from the picker root
- Command: `python tests/windows/test_ui_drive_listing.py build\c\codebase-memory-mcp.exe`
- Minimal failure output:

  ```
  control browse('C:/Users/jacob') -> dirs(23)
  browse('/') -> path='/' dirs(0)=[]
  RED: drives ['D:\\', 'E:\\'] are not reachable from the UI root picker
  ```

- Suspected implementation area: `handle_browse` in `src/ui/http_server.c` does
  `opendir(path)` for the requested path. For the root it lists only the current
  drive's contents and never enumerates the logical drives
  (`GetLogicalDriveStrings`). Fix direction: when the path is the filesystem root
  on Windows, return the available drive letters as the directory list so the
  picker can descend into any drive.

This test requires a UI build because the HTTP server only starts when the
frontend is embedded (`CBM_EMBEDDED_FILE_COUNT > 0`); against a non-UI binary it
reports a precondition (exit 2), and on a single-drive machine it is not
meaningful (exit 2). Holds on Linux/macOS (a single `/` root with no drive
letters).

---

## Seed areas revisited and ruled out (green on native Windows)

Each was reproduced as a concrete attempt against the production binary and
behaved correctly — recorded as green and **not** included as a red test:

| Area | Seed | Result on Windows |
|---|---|---|
| stdio `initialize` returns before stdin EOF; stdout flushes before EOF | #513, #530.1, #635 | green |
| `tools/list` non-empty; all 14 tools return valid JSON-RPC | #530 | green |
| `get_code_snippet` on a CP949 file emits valid UTF-8 (invalid bytes → U+FFFD) | #530.3 | green |
| Indexing a mapped (subst) drive `W:\` — no `bad_root_path`/`store.corrupt`, DB kept | #227, #367 | green (subst; real SMB not testable here) |
| Client exit terminates the server process (no residual `.exe`) | #185, #406 | green |
| `--help` / `--version` exit 0 in PowerShell, cmd, Git Bash | — | green |
| `search_code` works without bash/GNU grep (PowerShell `Select-String`) | #422, #348 | green |
| `.gitignore` and `.cbmignore` honored | #274 | green |
| `detect_changes` reports real changed files across commits | #371, #137 | green |
| `query_graph` shapes (counts, paths, labels) — no crash/disconnect | #627 | green |
| Paths with spaces, `&`, `()`, `[]`, `#`, `%`, `!`, apostrophe | #272 | green |
| Mixed slash/backslash and lower-case drive letters | #133 | green |
| Non-UTF-8 (CP949) source file emits valid UTF-8 JSON; no crash | #511 | green |
| Re-index is idempotent (counts stable, single project) | #140 | green |
| Index never escapes the selected root | #331 | green |
| Every JSON-RPC response decodes as strict UTF-8 | invariant | green |

## Observed but intentionally out of scope for this PR

- **Project-name collision for non-ASCII paths (#571/#20).** Two distinct repos
  (`проект`, `日本語`) under the same parent derive the *same* project name,
  because `cbm_project_name_from_path` (`src/pipeline/fqn.c`) maps every
  non-`[A-Za-z0-9._-]` byte to `-` and then trims. This is a real bug but it is
  **not Windows-specific** — `cbm_project_name_from_path` is platform-independent
  and collides identically on Linux. Per the campaign rules it is recorded here
  and left for a cross-platform PR.
- **Paths longer than 260 characters.** This machine has
  `HKLM\SYSTEM\CurrentControlSet\Control\FileSystem\LongPathsEnabled = 0`, so
  paths over `MAX_PATH` are unreachable by every application, not just CBM.
  CBM could opt in via the `\\?\` prefix + wide APIs, but the failure is gated by
  a machine-wide policy rather than a clean CBM-only defect, so it is excluded.
- **Cascading nested `.gitignore` (#530.2) and `.git/info/exclude` (#530.5).**
  `try_load_nested_gitignore` in `src/discover/discover.c` skips nested
  `.gitignore` files once a parent ignore is loaded, and discovery never reads
  `.git/info/exclude`. Both are real, but the discovery logic is
  platform-independent and reproduces identically on Linux, so they are out of
  scope for a Windows-only PR.
- **libgit2 1.8+ build break (#530.4).** `git_allocator` moved to
  `<git2/sys/alloc.h>`; cross-platform compile issue, not a Windows runtime bug.
- **Memory growth over hours (#581).** Requires a multi-hour soak to surface and
  is not deterministic in a unit/integration test; the existing
  `scripts/soak-test.sh` RSS-trend harness is the right vehicle and is not
  reproduced as a red test here.
- **C `test-runner` failures on Windows.** The in-process C suite reports many
  extraction-count failures concentrated in `test_grammar_probe_*`,
  `test_node_creation_probe`, `test_edge_*`, `test_matrix_*`, and
  `test_integration.c` (e.g. `integ_index_has_files` finds 0 files even for an
  **ASCII** fixture). The production binary indexes those same ASCII/CRLF cases
  correctly (CRLF vs LF source files were verified to extract identically), so
  these look like in-process test-harness issues rather than user-facing product
  regressions. Distinguishing genuine Windows-only product regressions from
  fixture/harness sensitivity requires a Linux baseline of the same commit and is
  left as a follow-up; they are deliberately **not** converted into red tests
  here to avoid shipping undiagnosed assertions.

## Stop-condition coverage

- Shells/launchers covered: PowerShell 5.1, `cmd.exe`, Git Bash, direct Win32,
  Python `subprocess`, Python stdio JSON-RPC (>= 3 required).
- Classes covered in the green streak: smoke, integration, unit (the passing
  `build/c/test-runner` cases), invariant.
- Seed areas (Unicode paths, mapped-drive/UNC, stdio, `search_code`,
  install/update, watcher/ignore, query, memory/process lifecycle) were each
  revisited or explicitly ruled out above.
