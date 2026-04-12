# Testing Patterns

**Analysis Date:** 2026-04-11

## Test Framework

**Runner:**
- Native C test runner built from `tests/test_main.c` and `tests/test_framework.h` via `scripts/test.sh` and the `test` target in `Makefile.cbm`.
- Frontend test framework is configured in `graph-ui/package.json` as Vitest 3, but no `graph-ui/src/**/*.test.ts?(x)` files are present.
- Config: `Makefile.cbm`, `scripts/test.sh`, and `graph-ui/package.json`

**Assertion Library:**
- C tests use the in-repo macro framework in `tests/test_framework.h` (`ASSERT_EQ`, `ASSERT_STR_EQ`, `ASSERT_NOT_NULL`, `SKIP`, `RUN_TEST`, `RUN_SUITE`).
- Frontend dependencies include `vitest`, `@testing-library/react`, `@testing-library/jest-dom`, and `jsdom` in `graph-ui/package.json`, but there is no checked-in Vitest config file or test suite.

**Run Commands:**
```bash
scripts/test.sh                       # Canonical full native test run
make -f Makefile.cbm test-foundation # Fast foundation-only native tests
cd graph-ui && npm run test          # Vitest run target, if UI tests are added
```

## Test File Organization

**Location:**
- Native tests live in the top-level `tests/` directory, separate from production code.
- Test fixtures live under `tests/fixtures/`, including GDScript fixture repos in `tests/fixtures/gdscript/min_project/`.
- Supplemental smoke/proof scripts live in `tests/` and `scripts/`, for example `tests/smoke_guard.sh` and `scripts/test_gdscript_proof_same_script_calls.py`.

**Naming:**
- Native tests follow `tests/test_<area>.c`, for example `tests/test_pipeline.c`, `tests/test_ui.c`, and `tests/test_store_search.c`.
- Shared test support uses `tests/test_framework.h` and `tests/test_helpers.h`.
- Grammar packages under `tools/` use Tree-sitter corpus tests in `tools/tree-sitter-form/test/corpus/` and `tools/tree-sitter-magma/test/corpus/`.

**Structure:**
```
tests/
├── test_main.c
├── test_framework.h
├── test_helpers.h
├── test_*.c
└── fixtures/
    └── gdscript/
```

## Test Structure

**Suite Organization:**
```typescript
// Pattern from `tests/test_framework.h` and `tests/test_pipeline.c`
TEST(pipeline_create_free) {
    cbm_pipeline_t *p = cbm_pipeline_new("/some/path", NULL, CBM_MODE_FULL);
    ASSERT_NOT_NULL(p);
    ASSERT_STR_EQ(cbm_pipeline_project_name(p), "some-path");
    cbm_pipeline_free(p);
    PASS();
}

SUITE(pipeline) {
    RUN_TEST(pipeline_create_free);
}
```

**Patterns:**
- Central registration happens in `tests/test_main.c`; every suite is declared `extern` and run with `RUN_SUITE(...)`.
- Use inline setup/teardown helpers inside each test file for temporary repositories and temporary databases, as shown by `setup_test_repo()` / `teardown_test_repo()` in `tests/test_pipeline.c`.
- Use `SKIP("reason")` for environment-dependent failures such as temp-dir setup issues rather than partial assertions.

## Mocking

**Framework:** None; prefer fakes, stubs, temp repos, and in-memory stores.

**Patterns:**
```typescript
// Fake persistence/store pattern from `tests/test_ui.c`
cbm_store_t *store = cbm_store_open_memory();
ASSERT_NOT_NULL(store);
cbm_store_upsert_project(store, "test", "/tmp/test");

// Environment override pattern from `tests/test_ui.c`
char *old_home = getenv("HOME") ? strdup(getenv("HOME")) : NULL;
cbm_setenv("HOME", td, 1);
cbm_ui_config_load(&cfg);
```

**What to Mock:**
- Use `cbm_store_open_memory()` from `src/store/store.h` for store-backed tests instead of creating real on-disk state when persistence is not the subject.
- Redirect environment-sensitive code by overriding `HOME` or similar variables, as in `tests/test_ui.c`.
- Swap in fixture repositories and generated files instead of mocking parser output, as in `tests/test_pipeline.c` and `scripts/test_gdscript_proof_same_script_calls.py`.

**What NOT to Mock:**
- Do not mock the core pipeline when verifying extraction/index behavior; create a real temp repo and run the real pipeline, as done in `tests/test_pipeline.c`.
- Do not bypass the CLI/binary for smoke checks that validate file-system side effects; `tests/smoke_guard.sh` executes the built binary directly.

## Fixtures and Factories

**Test Data:**
```typescript
// Pattern from `tests/test_helpers.h`
th_write_file(TH_PATH(base, "src/main.go"), "package main\n");
th_mkdir_p(TH_PATH(base, "pkg/util"));
th_cleanup(base);
```

**Location:**
- Reusable file-system helpers are in `tests/test_helpers.h`.
- Static fixtures are in `tests/fixtures/`.
- Script-generated repos are created in temp directories inside tests such as `tests/test_pipeline.c` and `scripts/test_gdscript_proof_same_script_calls.py`.

## Coverage

**Requirements:** None enforced.
- `scripts/test.sh`, `Makefile.cbm`, and `.github/workflows/_test.yml` enforce pass/fail across platforms, but no coverage report target or threshold is defined.
- `graph-ui/package.json` exposes `npm run test:coverage`, but there are no frontend tests to produce meaningful coverage yet.

**View Coverage:**
```bash
cd graph-ui && npm run test:coverage
```

## Test Types

**Unit Tests:**
- Broad native unit coverage exists for foundation, store, discovery, CLI, UI helpers, YAML, security helpers, and more through files like `tests/test_log.c`, `tests/test_store_nodes.c`, `tests/test_language.c`, and `tests/test_ui.c`.

**Integration Tests:**
- The main runner includes integration-style tests that build temporary repos and run the full indexing flow, especially `tests/test_pipeline.c`, `tests/test_integration.c`, and `tests/test_incremental.c`.
- `scripts/test_gdscript_proof_same_script_calls.py` is a regression/integration test around `scripts/gdscript-proof.sh`.

**E2E Tests:**
- Shell smoke coverage exists through `tests/smoke_guard.sh`, which builds `build/c/codebase-memory-mcp` and exercises CLI handlers.
- UI/browser E2E tooling is not detected.

## Common Patterns

**Async Testing:**
```typescript
// Process-based async integration pattern from `scripts/test_gdscript_proof_same_script_calls.py`
result = subprocess.run(cmd, cwd=repo_root, capture_output=True, text=True)
if result.returncode != 0:
    fail(f"proof command exited {result.returncode}")
```

**Error Testing:**
```typescript
// Corrupt-input regression pattern from `tests/test_ui.c`
FILE *f = fopen(path, "w");
ASSERT_NOT_NULL(f);
fprintf(f, "this is not json!!!");
fclose(f);

cbm_ui_config_t cfg;
cbm_ui_config_load(&cfg);
ASSERT_FALSE(cfg.ui_enabled);
ASSERT_EQ(cfg.ui_port, 9749);
```

---

*Testing analysis: 2026-04-11*
