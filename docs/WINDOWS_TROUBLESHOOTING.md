# Windows Troubleshooting

This page collects known Windows-specific symptoms, with the version range each affects, confirmed workarounds, and links to the canonical tracking issues.

## Non-ASCII paths in CBM_CACHE_DIR or CBM_ALLOWED_ROOT

**Symptom:** Configuration appears not to persist, the config database cannot be opened (`error: cannot open config database`), or repositories inside `CBM_ALLOWED_ROOT` are rejected during indexing.

**Affected versions:** Affects v0.9.x and earlier. Fixed in v0.10.0 (environment variables are read as wide strings via `GetEnvironmentVariableW`).

**Workaround:** On an older build, use ASCII-only paths for these two variables (e.g. via an NTFS junction) or upgrade with `install.ps1`.

**Tracking:** #1165.

## Upgrade completes but binary version does not change

**Symptom:** `codebase-memory-mcp upgrade` reports success and downloads files, but `codebase-memory-mcp --version` still reports the previous version (with `.exe.old` remaining alongside).

**Affected versions:** Affects v0.9.x and earlier (in-process self-update on Windows). From v0.10.0, `update` prints the `install.ps1` command instead of replacing itself.

**Workaround:** On an older build, run `install.ps1` once; it replaces the binary out-of-process and verifies the installed version.

**Tracking:** #1168.

## Index worker hangs, crashes, or leaves an empty worker log

**Symptom:** `index_repository` reports worker hangs or crashes. (An *empty* worker log is a separate, fixed bug: v0.10.4 and earlier.)

**Affected versions:** Empty worker logs affect v0.10.4 and earlier (fixed in v0.10.5). Misleading "crashed on a file" reports for returned errors fixed in v0.10.0. From v0.11.0, the Windows worker is memory-capped. The underlying hang (#1130) and startup exit (#1145) remain open for investigation.

**Workaround:** For repositories containing large binary, media, or model directories, exclude those paths with `.cbmignore` before indexing (see [`.cbmignore` documentation](cbmignore.md)). From v0.10.5, the worker log names the in-flight file; if indexing fails on v0.11.0, attach that log to #1130 or #1145.

**Tracking:** #1130, #1132, #1145.

## Antigravity 2.0 configuration not picked up

**Symptom:** Installation completes but Antigravity 2.0 does not see the MCP server or hooks, or the environment is detected as Gemini-CLI; duplicate large binaries may also be left on disk.

**Affected versions:** Affects detection when neither `~/.gemini/antigravity-cli` exists nor `antigravity` is found on PATH. Duplicate binary accumulation during `install.ps1` fixed since v0.10.0.

**Workaround:** Antigravity is configured only when `~/.gemini/antigravity-cli` exists or `antigravity` is on PATH; the target is `~/.gemini/config/mcp_config.json`, and no hooks are installed for it. If neither signal is present, the machine is treated as Gemini-CLI. From v0.10.0, `install.ps1` keeps a single binary; running `codebase-memory-mcp install` manually without `--dir` can still create a second copy in `~/.local/bin`.

**Tracking:** #1161.

## OpenCode installed but not detected

**Symptom:** `codebase-memory-mcp install` detects other clients but skips OpenCode on Windows.

**Affected versions:** Affects v0.9.x and earlier with PATH-only detection. From v0.10.0, OpenCode is also detected via its configuration directory. PATH-only detection can still fail on Windows (long PATH strings, non-ASCII user profiles, or npm shim directory paths).

**Workaround:** Ensure `%USERPROFILE%\.config\opencode\` exists (or set `OPENCODE_CONFIG` / `OPENCODE_CONFIG_DIR`), then re-run `codebase-memory-mcp install`.

**Tracking:** #1167.

---

When a release resolves one of these issues, update or remove the corresponding entry rather than letting this page become a second issue tracker.
