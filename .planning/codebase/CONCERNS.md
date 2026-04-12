# Codebase Concerns

**Analysis Date:** 2026-04-11

## Tech Debt

**Monolithic core modules:**
- Issue: Several core modules carry multiple responsibilities and large surface areas, which raises regression risk and slows focused changes.
- Files: `src/store/store.c` (~4527 lines), `src/mcp/mcp.c` (~3585 lines), `src/cli/cli.c` (~3496 lines), `src/cypher/cypher.c` (~3421 lines), `internal/cbm/lsp/c_lsp.c` (~4767 lines)
- Impact: Small feature changes require navigation across parsing, I/O, persistence, and response formatting in the same file; reviews and debugging stay expensive.
- Fix approach: Split by responsibility first: move path/db resolution out of `src/mcp/mcp.c`, isolate HTTP response builders from `src/ui/http_server.c`, break `src/store/store.c` into schema/query/write modules, and separate parser/planner/executor stages inside `src/cypher/cypher.c`.

**Large integration test files as behavior maps:**
- Issue: Test suites concentrate many scenarios into a few oversized files instead of smaller feature-focused suites.
- Files: `tests/test_c_lsp.c` (~15816 lines), `tests/test_pipeline.c` (~6368 lines), `tests/test_incremental.c` (~3097 lines), `tests/test_cli.c` (~2478 lines)
- Impact: Failures are harder to localize, setup code gets reused by copy/paste, and adding one scenario increases compile time and maintenance load for unrelated test areas.
- Fix approach: Split by subsystem behavior (`gdscript`, `indexing`, `cli install/update`, `incremental`) and extract shared fixtures into `tests/test_helpers.h` plus small helper `.c` files.

**Git history analysis depends on an optional shell fallback:**
- Issue: When `libgit2` is unavailable, git history parsing falls back to a shell command built around `git log`.
- Files: `Makefile.cbm`, `src/pipeline/pass_githistory.c`
- Impact: Behavior differs by environment, large repositories pay for a full `git log --since='1 year ago' --max-count=10000`, and shell-based execution remains more brittle than the `libgit2` path.
- Fix approach: Prefer `libgit2` in supported builds, reduce fallback scope, and move fallback execution to `cbm_exec_no_shell()` style argv handling instead of a `cd 'repo' && git log ...` command string.

## Known Bugs

**Graph UI detail mode is not implemented end-to-end:**
- Symptoms: Selecting a center node does not request a filtered/detail graph; the UI always reloads the full overview layout.
- Files: `graph-ui/src/hooks/useGraphData.ts`, `src/ui/http_server.c`, `src/ui/layout3d.h`
- Trigger: `fetchDetail()` ignores `centerNode`, and `/api/layout` always calls `cbm_layout_compute(..., CBM_LAYOUT_OVERVIEW, NULL, 0, max_nodes)`.
- Workaround: None in the current UI; use MCP graph tools instead of the detail panel when a focused subgraph is needed.

**UI route responses can become invalid JSON for unescaped file or process names:**
- Symptoms: `/api/browse`, `/api/processes`, and `/api/index-status` build JSON with raw `snprintf()` string interpolation instead of escaping values.
- Files: `src/ui/http_server.c`, `graph-ui/src/components/StatsTab.tsx`, `graph-ui/src/components/ControlTab.tsx`
- Trigger: Directory names, project paths, or process command names containing quotes, backslashes, or control characters.
- Workaround: Use repositories and directories without JSON-breaking characters, or call MCP tools directly instead of the UI endpoints.

## Security Considerations

**Project name path traversal in DB path construction:**
- Risk: Project identifiers are interpolated directly into `<cache_dir>/<project>.db` paths without path-segment validation.
- Files: `src/ui/http_server.c`, `src/mcp/mcp.c`, `src/store/store.c`
- Current mitigation: File existence checks and some route-level validation exist, but the project name itself is not normalized or containment-checked before path assembly.
- Recommendations: Reject `/`, `..`, `\\`, and absolute-path patterns in project names; canonicalize the final DB path; verify it remains inside the cache directory before open/delete operations.

**HTTP server manually serializes JSON for user-influenced strings:**
- Risk: Manual JSON assembly can corrupt responses and can expose UI consumers to malformed payload handling.
- Files: `src/ui/http_server.c`
- Current mitigation: Some endpoints use `yyjson`, and CORS is restricted to localhost in `src/ui/http_server.c`.
- Recommendations: Convert string-producing UI endpoints to `yyjson` builders and add regression tests for quotes, backslashes, and Unicode paths.

## Performance Bottlenecks

**Layout endpoint always computes overview graphs up to 50k nodes:**
- Problem: The UI fetch path requests large graph payloads and the backend recomputes overview layouts instead of narrowing to a center node.
- Files: `graph-ui/src/hooks/useGraphData.ts`, `src/ui/http_server.c`, `src/ui/layout3d.c`
- Cause: `fetchOverview()` and `fetchDetail()` both call the same 50,000-node request, and `handle_layout()` ignores detail parameters.
- Improvement path: Add `center_node` and `radius` query params, route detail requests to `CBM_LAYOUT_DETAIL`, and lower default node caps for overview requests.

**Git history fallback can become expensive on large repos:**
- Problem: The shell fallback scans up to 10,000 commits from the last year.
- Files: `src/pipeline/pass_githistory.c`
- Cause: Fallback mode shells out to `git log --name-only --pretty=format:COMMIT:%H --since='1 year ago' --max-count=10000`.
- Improvement path: Cache commit windows, limit fallback scans by project size, or require `libgit2` in production builds that depend on change-coupling results.

## Fragile Areas

**UI HTTP layer:**
- Files: `src/ui/http_server.c`, `tests/test_ui.c`
- Why fragile: One file owns routing, JSON serialization, subprocess management, process monitoring, directory browsing, indexing job state, and layout serving.
- Safe modification: Change one endpoint at a time, add route-specific tests before refactoring, and avoid expanding the manual `snprintf()` JSON pattern.
- Test coverage: `tests/test_ui.c` covers config, embedded assets, and layout helpers, but not `/api/browse`, `/api/processes`, `/api/process-kill`, `/api/index`, or `/api/layout` request handling.

**Project deletion and reopen lifecycle:**
- Files: `src/mcp/mcp.c`, `src/ui/http_server.c`, `tests/test_integration.c`
- Why fragile: Current behavior depends on WAL files and OS-specific unlink semantics; `tests/test_integration.c` documents Linux behavior where deleted DB files can remain reachable through open handles.
- Safe modification: Close cached store handles before delete/reopen paths, keep WAL cleanup explicit, and preserve the guard in `src/mcp/mcp.c` that verifies a project still exists after opening the DB.
- Test coverage: Integration tests cover happy-path delete behavior, but no traversal or repeated delete/reopen stress tests were found.

## Scaling Limits

**UI indexing concurrency:**
- Current capacity: 4 concurrent indexing jobs.
- Limit: `src/ui/http_server.c` uses `MAX_INDEX_JOBS 4` and returns HTTP 429 when all slots are busy.
- Scaling path: Replace the fixed array with a bounded job queue plus per-job metadata persisted outside process memory.

## Dependencies at Risk

**Optional `libgit2` dependency path split:**
- Risk: Feature behavior depends on whether `pkg-config` resolves `libgit2` during build.
- Impact: Change-coupling and git-history behavior vary by machine and CI image.
- Migration plan: Standardize build environments with `libgit2`, or narrow the supported fallback surface and document degraded-mode behavior explicitly.

## Missing Critical Features

**Focused graph inspection in the UI:**
- Problem: The frontend exposes a detail interaction path, but the request/response path stays overview-only.
- Blocks: Practical inspection of dense projects from `graph-ui/src/components/GraphTab.tsx` without switching back to MCP queries.

## Test Coverage Gaps

**Graph UI frontend is configured for Vitest but has no test files:**
- What's not tested: React hooks and components under `graph-ui/src/`, including `graph-ui/src/hooks/useGraphData.ts`, `graph-ui/src/components/StatsTab.tsx`, and `graph-ui/src/components/ControlTab.tsx`.
- Files: `graph-ui/package.json`, `graph-ui/vite.config.ts`, `graph-ui/src/`
- Risk: UI regressions in fetch flows, error handling, and route integration can ship without automated detection.
- Priority: High

**HTTP route encoding and project-name sanitization:**
- What's not tested: JSON escaping for UI endpoints and traversal-safe DB path resolution for project names.
- Files: `src/ui/http_server.c`, `src/mcp/mcp.c`, `tests/test_ui.c`, `tests/test_mcp.c`
- Risk: Malformed responses or cache-directory escape bugs can persist undetected.
- Priority: High

---

*Concerns audit: 2026-04-11*
