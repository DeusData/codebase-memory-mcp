# Windows Troubleshooting

This page collects known Windows-specific symptoms, with the version range each affects, confirmed workarounds, and links to the canonical tracking issues.

## Non-ASCII paths in CBM_CACHE_DIR or CBM_ALLOWED_ROOT

**Symptom:** Configuration appears not to persist, the config database cannot be opened (`error: cannot open config database`), or repositories inside `CBM_ALLOWED_ROOT` are rejected during indexing.

**Affected versions:** Affects v0.9.x and earlier. Fixed in v0.10.0 (environment variables are read as wide strings via `GetEnvironmentVariableW`).

**Workaround:** On an older build, use ASCII-only paths for these two variables or upgrade with `install.ps1`.

**Tracking:** #1165.

## Upgrade completes but binary version does not change

**Symptom:** `codebase-memory-mcp update` reports success and downloads files, but `codebase-memory-mcp --version` still reports the previous version (with `.exe.old` remaining alongside).

**Affected versions:** Affects v0.9.x and earlier (in-process self-update on Windows). From v0.10.0, `update` prints the `install.ps1` command instead of replacing itself.

**Workaround:** On an older build, run `install.ps1` once; it replaces the binary out-of-process and verifies the installed version.

**Tracking:** #1168 (closed; fixed in v0.10.0).

## Index worker hangs, crashes, or leaves an empty worker log

**Symptom:** `index_repository` reports worker hangs or crashes. (An *empty* worker log is a separate, fixed bug: v0.10.4 and earlier.)

**Affected versions:** Empty worker logs affect v0.10.4 and earlier (fixed in v0.10.5). Misleading "crashed on a file" reports for returned errors fixed in v0.10.0. From v0.11.0, the Windows worker is memory-capped. From v0.12.0, a worker that exits without returning a result is no longer counted as a success, and its log is kept. The underlying hang (#1130) and startup exit (#1145) remain open for investigation.

**Workaround:** For repositories containing large binary, media, or model directories, exclude those paths with `.cbmignore` before indexing (see [`.cbmignore` documentation](cbmignore.md)). From v0.10.5, the worker log names the in-flight file; if indexing still fails on v0.11.0 or newer, attach that log to #1130 or #1145.

**Tracking:** #1130, #1132, #1145.

## Antigravity 2.0 configuration not picked up

**Symptom:** Installation completes but Antigravity 2.0 does not see the MCP server or hooks, or the environment is detected as Gemini-CLI; duplicate large binaries may also be left on disk.

**Affected versions:** All releases through v0.12.1 when neither detection signal below is present (v0.9.x checks only for `~/.gemini/antigravity-cli`; from v0.10.0, `antigravity` on PATH also counts). The duplicate binary left behind by `install.ps1` affects v0.9.x and earlier (fixed in v0.10.0).

**Workaround:** Antigravity is configured only when `~/.gemini/antigravity-cli` exists or `antigravity` is on PATH; the target is `~/.gemini/config/mcp_config.json`, and no hooks are installed for it. If neither signal is present, the machine is treated as Gemini-CLI. From v0.10.0, `install.ps1` keeps a single binary; running `codebase-memory-mcp install` manually without `--dir` can still create a second copy in `~/.local/bin`.

**Tracking:** #1161.

## OpenCode installed but not detected

**Symptom:** `codebase-memory-mcp install` detects other clients but skips OpenCode on Windows.

**Affected versions:** v0.9.x and earlier detect OpenCode only through `opencode` on PATH. v0.10.0 also detects its configuration (the config file, `%USERPROFILE%\.config\opencode\`, or `OPENCODE_CONFIG_DIR`), and v0.12.0 also detects its data folder `%USERPROFILE%\.local\share\opencode\`, which the OpenCode CLI and the Desktop app both create, so detection no longer depends on PATH. In v0.12.1 and earlier, a PATH longer than 4 KB hides every CLI from detection, and npm, mise and scoop shim folders are not searched; both are fixed after v0.12.1 (#2381).

**Workaround:** On v0.12.0 or newer, start OpenCode once (CLI or Desktop app) so that its data folder exists, then re-run `codebase-memory-mcp install`. On v0.10.x or v0.11.0, make sure `%USERPROFILE%\.config\opencode\` exists (or point `OPENCODE_CONFIG` at an existing config file, or `OPENCODE_CONFIG_DIR` at an existing folder), then re-run `codebase-memory-mcp install`. On v0.9.x, `opencode` must be on PATH; upgrading with `install.ps1` is the simpler fix.

**Tracking:** #1167.

---

When a release resolves one of these issues, update or remove the corresponding entry rather than letting this page become a second issue tracker.
