---
phase: 03-real-repo-semantic-verification
plan: 01
subsystem: pipeline
tags: [c, pipeline, gdscript, parity, testing]

# Dependency graph
requires:
  - phase: 02-isolated-proof-harness
    provides: isolated proof execution and wrapper-first evidence artifacts that later parity plans will extend
provides:
  - deterministic sequential/parallel override contract via CBM_FORCE_PIPELINE_MODE
  - explicit pipeline.mode_selection logging for requested and selected execution modes
  - native regressions proving forced-mode behavior for later proof-harness parity work
affects: [03-02, SEM-06, proof-harness]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - shared native mode-selection helper used by product code and tests
    - forced parallel fails deterministically when worker count cannot support it

key-files:
  created: []
  modified:
    - src/pipeline/pipeline.c
    - src/pipeline/pipeline_internal.h
    - tests/test_parallel.c

key-decisions:
  - "Expose cbm_pipeline_select_mode in pipeline_internal.h so runtime selection and native regressions share one contract."
  - "Treat forced parallel without worker capacity as an explicit error instead of silently falling back to sequential."

patterns-established:
  - "Mode forcing uses CBM_FORCE_PIPELINE_MODE=auto|sequential|parallel while preserving the original auto threshold rule by default."
  - "Forced-mode parity regressions assert semantic outcomes on known fixture edges rather than byte-for-byte graph equality."

requirements-completed: [SEM-06]

# Metrics
duration: 17 min
completed: 2026-04-12
---

# Phase 03 Plan 01: Deterministic Pipeline Mode Selection Summary

**Deterministic native pipeline mode selection with explicit forced-mode logging and parity regressions for later proof-harness dual-run validation**

## Performance

- **Duration:** 17 min
- **Started:** 2026-04-12T09:39:45-07:00
- **Completed:** 2026-04-12T09:57:36-07:00
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added a shared `cbm_pipeline_select_mode` contract that preserves auto selection while supporting explicit sequential and parallel overrides.
- Logged one machine-readable `pipeline.mode_selection` event per run with requested mode, selected mode, worker count, file count, and forced-state details.
- Added native regressions for helper-level mode forcing and GDScript fixture parity so the proof harness can rely on deterministic mode selection in later plans.

## task Commits

Each task was committed atomically:

1. **task 1: add deterministic pipeline mode override selection (RED)** - `b46dde8` (test)
2. **task 1: add deterministic pipeline mode override selection (GREEN)** - `52b972c` (feat)
3. **task 2: cover forced-mode selection with native parity regressions** - `3ca3d4d` (test)

**Plan metadata:** pending

_Note: task 1 used TDD, so it produced separate RED and GREEN commits._

## Files Created/Modified
- `src/pipeline/pipeline.c` - Adds the mode-selection helper implementation, environment override handling, and explicit selection/failure logging.
- `src/pipeline/pipeline_internal.h` - Exports the shared `cbm_pipeline_mode` contract and helper declarations used by tests.
- `tests/test_parallel.c` - Adds helper-level and forced-mode regression coverage, including GDScript fixture validation for forced parallel selection.

## Decisions Made
- Used a shared native helper in `pipeline_internal.h` so runtime code and regressions evaluate the exact same selection rules.
- Logged forced parallel failures as explicit machine-readable errors rather than allowing hidden sequential fallback, matching the SEM-06 parity contract.
- Kept forced-mode parity checks focused on semantic outcomes from the GDScript fixture instead of byte-for-byte graph identity.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

- `make -f Makefile.cbm test` initially hit stale AddressSanitizer objects in `build/c`; rerunning after `make -f Makefile.cbm clean-c` restored a clean verification build.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Ready for `03-02-PLAN.md`: the proof harness can now drive `CBM_FORCE_PIPELINE_MODE` deterministically and consume explicit mode-selection logs.
- Native regressions now protect the product-layer contract SEM-06 depends on before dual-mode artifact work begins.

## Self-Check: PASSED

- Summary file exists at `.planning/phases/03-real-repo-semantic-verification/03-real-repo-semantic-verification-01-SUMMARY.md`.
- Verified task commits: `b46dde8`, `52b972c`, and `3ca3d4d`.
