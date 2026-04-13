---
phase: 06-parallel-native-suite-repair
plan: 01
subsystem: pipeline
tags: [c, pipeline, fastapi, parallel, testing]
requires:
  - phase: 03-real-repo-semantic-verification
    provides: forced parallel mode selection and prior incremental worker-lifetime repair
provides:
  - forced-parallel FastAPI Depends regression coverage in the native suite
  - shared FastAPI Depends edge extraction on full and incremental parallel pipeline paths
affects: [phase-07-validation, native-suite, fastapi-depends]
tech-stack:
  added: []
  patterns: [reuse cache-backed semantic passes across sequential and parallel orchestration]
key-files:
  created: []
  modified: [tests/test_pipeline.c, src/pipeline/pipeline.c, src/pipeline/pipeline_incremental.c]
key-decisions:
  - Reused cbm_pipeline_pass_fastapi_depends() by temporarily wiring ctx->result_cache around the parallel cache instead of duplicating edge logic.
  - Added a dedicated forced-parallel regression beside the existing sequential FastAPI test so the deferred suite gap is directly reproducible.
patterns-established:
  - Parallel post-passes that depend on CBMFileResult cache should run before cache teardown and restore ctx->result_cache afterward.
requirements-completed: []
duration: 15 min
completed: 2026-04-13
---

# Phase 6 Plan 1: Parallel Native Suite Repair Summary

**Forced-parallel FastAPI Depends CALLS edges now survive both full and incremental native pipeline paths.**

## Performance

- **Duration:** 15 min
- **Started:** 2026-04-13T03:35:09Z
- **Completed:** 2026-04-13T03:50:53Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added a dedicated forced-parallel FastAPI `Depends()` regression in the native suite.
- Restored the shared FastAPI Depends pass on the full parallel pipeline before cache teardown.
- Matched the same cache-backed FastAPI pass contract in the incremental parallel branch.

## task Commits

Each task was committed atomically:

1. **task 1: make the forced-parallel FastAPI Depends gap directly reproducible in native tests** - `73ce536` (test)
2. **task 2: run the shared FastAPI Depends pass on both parallel pipeline branches before cache teardown** - `dd31ca2` (fix)

**Plan metadata:** pending

_Note: TDD tasks may have multiple commits (test → feat → refactor)_

## Files Created/Modified
- `tests/test_pipeline.c` - Adds shared FastAPI regression scaffolding plus a dedicated forced-parallel regression and suite registration.
- `src/pipeline/pipeline.c` - Runs the shared FastAPI Depends pass against the live parallel cache before cleanup.
- `src/pipeline/pipeline_incremental.c` - Applies the same live-cache FastAPI Depends pass contract to incremental parallel indexing.

## Decisions Made
- Reused `cbm_pipeline_pass_fastapi_depends()` as the single semantic implementation by temporarily exposing the parallel cache through `ctx->result_cache`.
- Kept the original sequential regression and added a second forced-parallel regression so sequential coverage stayed intact while the deferred gap became explicit.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered

None.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness

- Forced and default native suite verification completed successfully for this repair.
- Ready for the orchestrator to record state updates and move to the next planned phase.

## Self-Check: PASSED

---
*Phase: 06-parallel-native-suite-repair*
*Completed: 2026-04-13*
