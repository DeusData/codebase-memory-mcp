# Changelog

## v0.5.5 — Hotfix

### Windows Defender False Positive Fix

Removed the DLL resolve tracking feature that was introduced in v0.5.4. The feature contained literal strings (`GetProcAddress`, `dlsym`, `LoadLibrary`) used for code analysis regex patterns, which triggered Windows Defender's `Trojan:Script/Wacatac.B!ml` machine-learning heuristic. The ML model couldn't distinguish our code analysis tool (which *detects* these patterns) from malware (which *uses* them).

The removed feature served a niche use case (tracking dynamic DLL loading in C/C++ projects). All other v0.5.4 features remain intact.

Fixes #89.

## v0.5.4

### Security Hardening — 8-Layer Security Test Suite

New automated security audit suite that runs in CI on every build:

- **Layer 1**: Static allow-list audit for dangerous calls (system/popen/fork) + hardcoded URLs
- **Layer 2**: Binary string audit — scans compiled binary for unauthorized URLs, credentials, dangerous commands
- **Layer 3**: Network egress monitoring via strace (Linux)
- **Layer 4**: Install output path + content validation
- **Layer 5**: Smoke test hardening — clean shutdown, residual process detection, version integrity
- **Layer 6**: Graph UI audit — external domains, CORS, server binding, eval/iframe detection
- **Layer 7**: MCP robustness — 23 adversarial JSON-RPC payloads (malformed JSON, shell injection, SQL injection, path traversal)
- **Layer 8**: Vendored dependency integrity — SHA-256 checksums for 72 vendored files + dangerous call scan across all vendored libraries and 354 tree-sitter grammar files

**Code-level defenses:**
- Shell injection prevention: `cbm_validate_shell_arg()` rejects metacharacters before all `popen`/`system` calls
- SQLite authorizer: blocks `ATTACH`/`DETACH` at engine level (prevents file creation via SQL injection)
- CORS locked to localhost origins only (was wildcard `*`)
- Path containment: `realpath()` check prevents `get_code_snippet` from reading files outside project root
- `/api/process-kill` restricted to server-spawned PIDs only
- SHA-256 checksum verification in update command

### Editor Compatibility

- **OpenCode support** — Added Content-Length framed transport (LSP-style). The server now auto-detects the transport: Content-Length framing for clients that use it (OpenCode, some VS Code extensions), bare JSONL for everyone else. Fully backwards compatible. (Fixes #78)
- **VS Code support** — Fixed schema validation failure (`ingest_traces` array missing `items`), added VS Code to install/uninstall detection, implemented proper MCP protocol version negotiation supporting versions 2024-11-05 through 2025-11-25. (PR #79 by @bingh0)
- **OpenClaw support** — Auto-detects `~/.openclaw/` and writes MCP config to `openclaw.json`.
- **Dual MCP config location** — `install` now writes to both `~/.claude/.mcp.json` and `~/.claude.json` for compatibility with Claude Code >=2.1.80 which changed the config path. (Fixes #69)

### Bug Fixes

- **WAL crash safety** — Bulk writes no longer switch away from WAL journal mode. Previously, a crash during indexing could corrupt the database. Now WAL is preserved throughout, making the database recoverable after any crash. (PR #72 by @halindrome)
- **Laravel route false positives** — Route extractors now scoped by file extension (.go->Go, .php->Laravel, .js/.ts->Express, .kt->Ktor). Paths containing `$` or `:` (cache keys, interpolated expressions) are filtered. (PR #65 by @mariomeyer)
- **Swift call extraction** — Fixed 0 CALLS edges for Swift files.

### New Features

- **FastAPI Depends() tracking** — Scans Python function signatures for `Depends(func_ref)` patterns and creates CALLS edges. Auth/DI functions no longer appear as dead code. (PR #66 by @kingchenc)
- **10 agents supported** — Added VS Code and OpenClaw detection. Install now auto-detects and configures 10 coding agents.

### CI & Infrastructure

- Security audit runs as a parallel job (~14 seconds, no build needed) — doesn't block the test->build->smoke pipeline
- Binary security layers run per-platform in smoke jobs (Linux, macOS, Windows)
- Frontend integrity scan on post-build `dist/` output
- VirusTotal scanning of all release binaries
- Updated `CONTRIBUTING.md` for pure C project
- 2044 tests passing

### Contributors

Thanks to @halindrome, @bingh0, @mariomeyer, @kingchenc, @aaabramov, and @heraque for their contributions, bug reports, and testing!

## v0.5.3

See [GitHub release](https://github.com/DeusData/codebase-memory-mcp/releases/tag/v0.5.3).
