---
phase: 03-real-repo-semantic-verification
plan: 04
subsystem: testing
tags: [asan, parallel, tree-sitter, incremental, regression]
requires:
  - phase: 03-03
    provides: forced-mode proof coverage and the Phase 03 verification baseline
provides:
  - forced-parallel incremental regression coverage for the new-file-added path
  - worker-lifetime parser cleanup that no longer frees Tree-sitter slab state mid-worker
affects: [phase-03-verification, forced-parallel, incremental-indexing]
tech-stack:
  added: []
  patterns: [reset parser state between files, destroy parser before thread slab teardown]
key-files:
  created: []
  modified: [tests/test_pipeline.c, src/pipeline/pass_parallel.c]
key-decisions:
  - "Parallel workers now reset parser state between files and destroy the parser only at worker exit."
  - "The forced-parallel UAT regression is covered by the incremental new-file-added native test flow."
patterns-established:
  - "Worker lifetime cleanup: keep thread-local parser/slab allocations alive across files, then tear them down in parser-before-slab order on thread exit."
  - "Forced-parallel regressions should use existing incremental fixtures and assert queryable output after reruns."
requirements-completed: [SEM-06]
duration: 30 min
completed: 2026-04-12
---

# Phase 03 Plan 04: Forced Parallel Lifetime Fix Summary

**Forced-parallel incremental coverage plus a parser-before-slab worker cleanup fix for the Tree-sitter ASan crash path**

## Performance

- **Duration:** 30 min
- **Started:** 2026-04-12T13:38:49-07:00
- **Completed:** 2026-04-12T21:09:03Z
- **Tasks:** 2
- **Files modified:** 2

## Accomplishments
- Added a native regression that exercises the incremental new-file-added path with `CBM_FORCE_PIPELINE_MODE=parallel`.
- Removed per-file parser/slab teardown from parallel extraction workers.
- Kept the worker cleanup contract explicit by resetting parser state between files and destroying parser state only at worker exit.

## task Commits

Each task was committed atomically:

1. **task 1: add a forced-parallel incremental regression for the ASan crash path** - `1c87ea4` (test)
2. **task 2: keep Tree-sitter parser and slab state alive for the full worker lifetime** - `9095763` (fix)

## Files Created/Modified
- `tests/test_pipeline.c` - Adds the forced-parallel incremental regression and worker-capacity/env handling.
- `src/pipeline/pass_parallel.c` - Removes per-file parser/slab destruction and performs final worker cleanup in parser-before-slab order.

## Decisions Made
- Reset the thread-local parser between files instead of destroying it so parser-owned buffers stay valid for the full worker lifetime.
- Keep the regression anchored to `setup_incremental_repo()` and the `incremental_new_file_added` flow so the UAT crash path stays reproducible.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- `CBM_FORCE_PIPELINE_MODE=parallel make -f Makefile.cbm test` still reports an unrelated `pipeline_fastapi_depends_edges` failure at `tests/test_pipeline.c:4361`. This is outside the worker-lifetime crash path fixed here and was logged to `.planning/phases/03-real-repo-semantic-verification/deferred-items.md`.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- The Phase 03 UAT blocker tied to the forced-parallel Tree-sitter heap-use-after-free is covered by an automated regression and no longer reproduces in the new crash-path test.
- A separate forced-parallel FastAPI edge-resolution failure remains deferred outside this plan's scope.

## Self-Check: PASSED
