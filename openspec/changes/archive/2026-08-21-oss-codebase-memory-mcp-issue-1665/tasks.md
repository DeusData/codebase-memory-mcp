## 1. Pipeline snapshot of the artifact export error

- [ ] 1.1 Add `char export_error[CBM_SZ_1K]` to `cbm_pipeline_t` (pipeline_internal.h), zero it at the start of `cbm_pipeline_run`, and in `export_after_publish` copy `cbm_artifact_export_last_error()` (fallback "unknown") into it on failure
- [ ] 1.2 Add `const char *cbm_pipeline_export_error(const cbm_pipeline_t *p);` to pipeline.h and implement it (NULL-safe) in pipeline.c
- [ ] 1.3 Verify: `make -f Makefile.cbm test-focused TEST_SUITES=pipeline` compiles and passes

## 2. Truthful MCP error hint

- [ ] 2.1 In `handle_index_repository`'s error branch (src/mcp/mcp.c), when `rc != 0` and `cbm_pipeline_export_error(p)` is non-empty, emit a hint naming the artifact export failure (`.codebase-memory/` directory, the export error detail when available), state the index database was published, and name the remedies (writable checkout or `--persistence false`); keep the generic hint verbatim in all other failure cases

## 3. Regression test

- [ ] 3.1 Add a test in tests/test_pipeline.c: build a pipeline with `persistence` enabled over a fixture repo, replace the artifact directory with a chmod 0555 directory (skip when `geteuid() == 0`), run `cbm_pipeline_run`, assert `rc != 0`, `cbm_pipeline_export_error(p)` non-empty, and the exported error mentions the artifact path
- [ ] 3.2 Verify: `make -f Makefile.cbm test-focused TEST_SUITES=pipeline` passes with the new test

## 4. Final validation

- [ ] 4.1 Run `make -f Makefile.cbm test` (full suite) and `make -f Makefile.cbm lint-ci` — pass
- [ ] 4.2 `OPENSPEC_NO_UPDATE_CHECK=1 openspec validate oss-codebase-memory-mcp-issue-1665 --json` — clean
- [ ] 4.3 Archive: `OPENSPEC_NO_UPDATE_CHECK=1 openspec archive oss-codebase-memory-mcp-issue-1665 --yes`
