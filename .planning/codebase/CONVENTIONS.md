# Coding Conventions

**Analysis Date:** 2026-04-11

## Naming Patterns

**Files:**
- C source and headers use lowercase snake_case filenames in feature folders, for example `src/foundation/log.c`, `src/pipeline/pass_tests.c`, and `src/ui/config.h`.
- C tests use `test_*.c` plus shared headers `test_*.h` in `tests/`, for example `tests/test_pipeline.c` and `tests/test_framework.h`.
- React and hook files use PascalCase for components in `graph-ui/src/components/` (`graph-ui/src/components/StatsTab.tsx`) and camelCase for hooks/utilities in `graph-ui/src/hooks/` and `graph-ui/src/lib/` (`graph-ui/src/hooks/useProjects.ts`, `graph-ui/src/lib/utils.ts`).

**Functions:**
- Public C APIs are prefixed with `cbm_` and keep snake_case names, for example `cbm_pipeline_run()` in `src/pipeline/pipeline.h` and `cbm_store_open_memory()` in `src/store/store.h`.
- File-local C helpers are `static` and snake_case, for example `signal_handler()` and `watcher_thread()` in `src/main.c`.
- Test cases use the `TEST(name)` macro, which expands to `test_<name>`; keep test names descriptive snake_case, as in `TEST(store_file_persistence)` in `tests/test_pipeline.c`.
- React hooks use `useX` camelCase names, for example `useProjects()` in `graph-ui/src/hooks/useProjects.ts` and `useGraphData()` in `graph-ui/src/hooks/useGraphData.ts`.

**Variables:**
- C globals use a `g_` prefix for file-static state, for example `g_watcher`, `g_server`, and `g_http_server` in `src/main.c`.
- Macro-style constants are uppercase with underscores, for example `MAIN_MAX_PORT` in `src/main.c` and `CBM_UI_DEFAULT_PORT` in `src/ui/config.h`.
- TypeScript state and locals use camelCase, for example `activeTab`, `selectedProject`, and `fetchProjects` in `graph-ui/src/App.tsx` and `graph-ui/src/hooks/useProjects.ts`.

**Types:**
- C opaque and data structs use `_t` suffixes, for example `cbm_store_t`, `cbm_node_t`, and `cbm_ui_config_t` in `src/store/store.h` and `src/ui/config.h`.
- TypeScript interfaces and component prop types use PascalCase, for example `GraphNode` in `graph-ui/src/lib/types.ts`, `UseProjectsResult` in `graph-ui/src/hooks/useProjects.ts`, and `StatsTabProps` in `graph-ui/src/components/StatsTab.tsx`.

## Code Style

**Formatting:**
- C formatting is enforced by `clang-format` through `scripts/lint.sh` and `Makefile.cbm`.
- Use 4-space indentation, no tabs, K&R braces, 100-column line limit, right-aligned pointers, preserved include order, and no include sorting per `.clang-format`.
- TypeScript has no repo-level ESLint or Prettier config. Keep the existing per-file style instead of reformatting unrelated UI files. Handwritten app files such as `graph-ui/src/App.tsx` use semicolons; generated-style UI primitives such as `graph-ui/src/components/ui/button.tsx` omit semicolons.

**Linting:**
- Run `scripts/lint.sh` locally; it is the single entry point for linting in `scripts/lint.sh`.
- C static analysis is blocking: `.clang-tidy` enables broad check sets with `WarningsAsErrors: '*'`, and `Makefile.cbm` runs `clang-tidy`, `cppcheck`, `clang-format`, and `lint-no-suppress`.
- Do not add arbitrary `NOLINT` comments. `Makefile.cbm` only permits `NOLINT(misc-no-recursion)` for whitelisted functions documented in `src/foundation/recursion_whitelist.h`.

## Import Organization

**Order:**
1. In C, include the module header first, then project headers, then standard library headers, as shown in `src/foundation/log.c` and `src/main.c`.
2. In React files, import React APIs first, then aliased UI imports, then relative project imports, as shown in `graph-ui/src/components/StatsTab.tsx`.
3. Put `import type` statements adjacent to the runtime import they support, as in `graph-ui/src/App.tsx`, `graph-ui/src/hooks/useProjects.ts`, and `graph-ui/src/components/ErrorBoundary.tsx`.

**Path Aliases:**
- `graph-ui/tsconfig.json` defines `@/* -> ./src/*`.
- Use `@/` for cross-cutting UI utilities/components (`graph-ui/src/components/ui/button.tsx` imports `@/lib/utils`) and relative imports for nearby feature modules (`graph-ui/src/hooks/useProjects.ts`).

## Error Handling

**Patterns:**
- In C, validate inputs early and return sentinel values immediately, for example the repeated `if (!s || !s->db) return ...` guard pattern in `src/store/store.c` and `if (!p) return ...` patterns throughout `src/main.c`.
- Use `NULL`-safe destructors and cleanup helpers; `cbm_pipeline_free(NULL)` is treated as valid and tests assert this in `tests/test_pipeline.c`.
- Emit machine-readable logs for operational failures with `cbm_log_*` helpers from `src/foundation/log.c`.
- In TypeScript, throw typed or plain `Error` objects at the fetch boundary (`graph-ui/src/api/rpc.ts`, `graph-ui/src/hooks/useGraphData.ts`) and convert them to user-facing string state in hooks/components (`graph-ui/src/hooks/useProjects.ts`, `graph-ui/src/components/StatsTab.tsx`).

## Logging

**Framework:** Structured C logger plus limited browser `console.error`

**Patterns:**
- Backend/native code logs structured key-value pairs through `cbm_log()`, `cbm_log_info()`, and related helpers in `src/foundation/log.c`.
- Prefer event-style messages with explicit keys, for example `cbm_log_info("watcher.reindex", "project", project_name, "path", root_path)` in `src/main.c`.
- UI code logs only boundary-level failures; `graph-ui/src/components/ErrorBoundary.tsx` uses `console.error` inside `componentDidCatch` and otherwise surfaces errors in component state.

## Comments

**When to Comment:**
- Start C files with a short responsibility block comment, as in `src/main.c`, `src/foundation/log.c`, and `tests/test_ui.c`.
- Use section-divider comments to group related logic, especially in larger C and test files, for example `/* ── Lifecycle tests ─────────────────────────────────────────────── */` in `tests/test_pipeline.c`.
- Add comments for invariants, portability notes, or non-obvious safety constraints, such as the sink behavior note in `src/foundation/log.c` and the temp-HOME isolation comments in `tests/test_ui.c`.

**JSDoc/TSDoc:**
- Not used in the React UI. Favor inline comments or descriptive type names in `graph-ui/src/lib/types.ts` and `graph-ui/src/api/rpc.ts`.

## Function Design

**Size:**
- Small C helpers stay `static` within the file; larger orchestration functions are broken into named helpers plus section comments, as in `src/main.c`.
- `.clang-tidy` sets review thresholds of 200 statements and 400 lines; treat those as upper bounds, not targets.

**Parameters:**
- C APIs pass explicit buffers, sizes, and out-parameters rather than hidden allocation, as shown by `cbm_ui_config_path(char *buf, int bufsz)` in `src/ui/config.h` and many store APIs in `src/store/store.h`.
- TypeScript components pass explicit prop objects and return typed hook results, as in `StatsTabProps` in `graph-ui/src/components/StatsTab.tsx` and `UseProjectsResult` in `graph-ui/src/hooks/useProjects.ts`.

**Return Values:**
- C functions commonly return `0`/negative status codes or nullable pointers; callers check immediately, as in `run_cli()` in `src/main.c` and store APIs in `src/store/store.h`.
- TypeScript async helpers return parsed data and throw on failure; hooks normalize failures into `{ error, loading }` state in `graph-ui/src/hooks/useProjects.ts` and `graph-ui/src/hooks/useGraphData.ts`.

## Module Design

**Exports:**
- C modules expose narrow headers and keep implementation details opaque, as documented by the opaque `cbm_store_t` handle in `src/store/store.h`.
- React modules export named symbols instead of defaults, for example `export function App()` in `graph-ui/src/App.tsx`, `export function useProjects()` in `graph-ui/src/hooks/useProjects.ts`, and `export { Button, buttonVariants }` in `graph-ui/src/components/ui/button.tsx`.

**Barrel Files:**
- Not used. Import directly from the defining file in both `src/` and `graph-ui/src/`.

---

*Convention analysis: 2026-04-11*
