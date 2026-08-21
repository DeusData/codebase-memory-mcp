## 1. Drift guard in get_code_snippet

- [x] 1.1 Change `resolve_snippet_source` (src/mcp/mcp.c:8506) to accept a `bool read_allowed` parameter and skip `read_file_lines` when false, keeping the path/containment logic and `out_abs_path` intact
- [x] 1.2 In `build_snippet_response` (src/mcp/mcp.c:8639), compute `coverage_path_freshness(srv->store, node->project, root_path, node->file_path, &outside)` before resolving the source; when the state is `metadata_changed` or `missing`, call `resolve_snippet_source` with `read_allowed=false` and add `source_drift: true`, `freshness: <state>`, and a `source` string stating the file changed after indexing and re-indexing is required
- [ ] 1.3 Verify: `make -f Makefile.cbm test-focused TEST_SUITES=mcp` passes (existing snippet tests green; no drift test yet)

## 2. Drift guard in search_code

- [x] 2.1 Thread `cbm_store_t *store` and `const char *project` into `assemble_search_output` (src/mcp/mcp.c:9287) and its call site (mcp.c:10208), then into `attach_result_source` (mcp.c:9071)
- [x] 2.2 In `attach_result_source`, when `coverage_path_freshness` reports `metadata_changed` or `missing` for `r->file`, skip `source`/`context` attachment and add `source_drift: true` + `freshness: <state>` to the item object
- [ ] 2.3 Verify: `make -f Makefile.cbm test-focused TEST_SUITES=mcp` passes

## 3. Regression tests

- [x] 3.1 Add `TEST(tool_get_code_snippet_reports_sourceless_drift_after_file_change)` in tests/test_mcp.c: index a small fixture via the test store helpers, rewrite the file on disk (changing mtime/size), call the snippet handler, and assert `source_drift: true`, `freshness: "metadata_changed"`, and no stale source text; then restore mtime/size and assert the response carries the full source again (metadata_match path unchanged)
- [x] 3.2 Add `TEST(tool_search_code_marks_drifted_file_results)` in tests/test_mcp.c: index a fixture, edit the file on disk, run a search matching that file (mode full), and assert the result item carries `source_drift: true` and no `source` text; a second non-drifted file in the same index still gets `source` attached
- [x] 3.3 Verify: `make -f Makefile.cbm test-focused TEST_SUITES=mcp` passes with the new tests

## 4. Final validation

- [ ] 4.1 Run `make -f Makefile.cbm test` (full suite) and `make -f Makefile.cbm lint-ci`; both pass
- [ ] 4.2 Run `OPENSPEC_NO_UPDATE_CHECK=1 openspec validate oss-codebase-memory-mcp-issue-1750 --json`; clean
- [ ] 4.3 Archive the change: `OPENSPEC_NO_UPDATE_CHECK=1 openspec archive oss-codebase-memory-mcp-issue-1750 --yes`
