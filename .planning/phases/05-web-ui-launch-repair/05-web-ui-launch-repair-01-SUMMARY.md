---
phase: 05-web-ui-launch-repair
plan: 01
subsystem: ui
tags: [c, python, regression, embedded-ui]
requires:
  - phase: 04-verdicts-and-acceptance-summaries
    provides: explicit acceptance tracking carried into the UI launch repair
provides:
  - persisted UI launch regression coverage for enable, relaunch, disable, and help output
  - single-source runtime UI config flow in the native entrypoint
affects: [web-ui-launch, Makefile.cbm, persisted-config]
tech-stack:
  added: []
  patterns:
    - single-source-of-truth runtime UI config loading in src/main.c
    - subprocess localhost regression with isolated HOME-backed UI config
key-files:
  created:
    - scripts/test_ui_launch_persisted_flag.py
  modified:
    - src/main.c
    - Makefile.cbm
key-decisions:
  - "Wrapped UI config load/parse/save in load_runtime_ui_config() so startup gating uses the same updated ui_cfg instance."
  - "Seed the regression with a persisted disabled state so --ui=true is proven as a real state transition instead of relying on embedded-asset defaults."
patterns-established:
  - "Persisted UI launch regressions should exercise the built binary with isolated HOME and localhost reachability checks."
requirements-completed: [UI-01]
duration: 12 min
completed: 2026-04-13
---

# Phase 5 Plan 1: Web UI Launch Repair Summary

**Persisted `--ui=true` / `--ui=false` launch coverage for the embedded localhost UI with a single runtime UI config path in `src/main.c`.**

## Performance

- **Duration:** 12 min
- **Started:** 2026-04-13T00:19:00Z
- **Completed:** 2026-04-13T00:31:23Z
- **Tasks:** 2
- **Files modified:** 3

## Accomplishments
- Added an end-to-end Python regression that builds the UI-enabled binary, isolates HOME, and verifies enable, persisted relaunch, disable, and help output behavior.
- Updated the default `make -f Makefile.cbm test` path to run the new persisted UI launch regression.
- Refactored `src/main.c` so load, CLI flag parsing, optional save, and startup gating all flow through the same in-memory `ui_cfg` value.

## task Commits

Each task was committed atomically:

1. **task 1: add a failing persisted UI launch regression** - `3bcd815` (test)
2. **task 2: repair the native entrypoint so persisted UI enablement actually launches the server** - `2021f97` (fix)

**Plan metadata:** included in the final docs commit for summary/state tracking

## Files Created/Modified
- `scripts/test_ui_launch_persisted_flag.py` - Builds the UI binary and verifies persisted enable/disable launch behavior against `127.0.0.1`.
- `Makefile.cbm` - Runs the new persisted UI regression from the default `test` target.
- `src/main.c` - Centralizes runtime UI config loading/parsing/saving before HTTP server startup gating.

## Decisions Made
- Wrapped UI config load/parse/save in `load_runtime_ui_config()` so the startup branch uses the same updated `ui_cfg` value that may be persisted.
- Kept the exact `--ui=true` and `--ui=false` help contract unchanged while adding regression coverage around that persisted contract.

## Deviations from Plan

None - plan executed exactly as written.

## Issues Encountered
- The first regression draft could read a stale persisted config value between successive launches; the final script waits for the expected config payload so each launch assertion matches the process under test.

## User Setup Required

None - no external service configuration required.

## Next Phase Readiness
- Phase 5 now has direct regression protection for the persisted embedded UI launch contract.
- No blockers remain for milestone verification from this plan's scope.

## Self-Check: PASSED

---
*Phase: 05-web-ui-launch-repair*
*Completed: 2026-04-13*
