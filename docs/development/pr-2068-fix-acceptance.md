# Product repair after browser audit

Repair verification completed on 9 September 2026 at 21:47 UTC. The checked items below are tested software flows, not a first-time developer usability study or a claim of complete semantic architecture inference. See `pr-2068-final-delivery.md` for current evidence and remaining limits. The execution notes below preserve intermediate findings chronologically, including failures that were subsequently repaired. The original frontend at port 9751 shares the current daemon/index; it is a frontend comparison, not a historical backend deployment.

Required manual flows before delivery:

- [x] Show repository purpose and source documentation for component responsibilities, with source links (author descriptions; source areas remain inferred).
- [x] Distinguish product entry points from test/tool entry candidates and follow a real static path across components (role classification is heuristic).
- [x] Open a relationship at its valid call site while keeping the map and selection in place; mark invalid sites unverified.
- [x] Find an exact file path and load its actual functions/classes, including compact RPC responses; reject stale and inexact provisional results.
- [x] Use one shared change-analysis workspace from map and Explore; analyze a selection and real working-tree changes.
- [x] Follow structural paths, test candidates and Git evidence; inspect bounds, revision and uncertainty.
- [x] Read the same persisted project errors in System and Galaxy; filter before limiting rows.
- [x] Produce explicitly TEST-labelled local hook activity on the daemon port; verify persistence after an actual restart and browser offline/online recovery without duplicates. No real LLM session was launched.
- [x] Diagnose index gaps deliberately, with no upload, report generation or issue creation without a separate explicit action.
- [x] Verify keyboard navigation, 1024×800 and 1440×1000 viewports, honest missing-data states and no browser errors in these flows.

Implementation ownership: repository/source navigation and shared layout (root); RPC/search compatibility (research agent); unified impact/change-set analysis (impact agent); persistent logs, System filters and agent producer setup (daemon agent). No new framework or package is planned. Existing dirty work is preserved; nothing is pushed.

Final reports: `repair-map-e2e.json`, `explore-context-e2e.json`, `change-analysis-e2e.json`, `repair-errors-e2e.json`, `final-agent-diagnosis-restart.json`, `final-comparison.json` in `graph-ui/verification/pr-2068/`. The final frontend unit run passes 175 files / 2473 tests. Earlier green suites did not establish these product flows; the final reports do.

## Executed repair checks (still in progress)

- Production RPC check reads all 144 functions and 8 classes in `src/daemon/application.c`; exact file search returns that file first. Evidence: `provider-search-fix.json`.
- Whole UI unit suite on 9 September at 23:15 local time: 175 files / 2469 tests pass. Subsequent changes require a final rerun.
- Two initial repaired-browser runs exposed `net::ERR_CONTENT_LENGTH_MISMATCH` on the 11,070,807-byte repository map response. The failed browser request was inspected directly before the transport repair (later runs replace the rolling `repair-map-e2e.json` artifact). The server's one-second absolute write deadline was too short for the large response under browser load; the map response now has a bounded five-second deadline. Ordinary responses retain their previous deadline. Browser confirmation is pending in the current run.
- Source evidence previously failed in a fresh Explore/impact session without its file in the bounded map. A targeted project/path Module lookup now precedes source reading when necessary. The real API fallback was verified at `tests/test_daemon_application.c:4185`; see `source-evidence-fallback.json`.
- The first 1024×800 impact check exposed only 89px available for findings. Duplicate selection/header content was removed; the next browser run checks the resulting space.
- SQLite activity schema 4 is running on the same port, with a local pre-migration backup at `/tmp/cbm-2068-activity-before-v4-1788987970.db`. Existing history was preserved. No new external agent inference was started.

Current live build: port 9749. Original frontend comparison: port 9751. Map, impact and System/Galaxy error flows are being verified concurrently, using the real repository plus the explicitly marked local error fixture.


## Latest integration evidence

- HTTP/journal native suite: 83 pass, 1 existing Windows-only skip under ASan/UBSan; native UI/map/impact suite: 40 pass. See `native-log-transport-repair.log` and `native-map-resolution-repair.log`.
- Whole frontend unit suite: 175 files / 2470 tests pass; the separate operational-client gate tests: 2 pass. Acceptance scaffold: 203 pass. Style and promises gates pass.
- System/Galaxy live-error workflow passes with real, explicitly marked local EACCES failures. System severity, project and text filters match SQLite exactly (57/57, 11/11 and 3/3 in the recorded run). No page errors or external requests; `repair-errors-e2e.json`.
- Working-tree/since-ref/selection/test-source/Git-copy flow passes after the actual repository reindex (`ub2c1b42d30fb1da2g2`). At 1024×800 it keeps 190px of findings visible and has no horizontal page overflow; `change-analysis-e2e.json`.
- Real local hook process and retries pass, with an explicitly TEST-labelled event, no LLM invocation and no personal client configuration changes; `agent-setup-integration.json`.
- Remaining map blocker found by the browser: an EOF newline was always stripped from snippet text even when the daemon's reported source range includes that final empty line. The strict reader then withheld valid line numbers. Exact API reproduction exists; deterministic range-aware normalization is being verified before the final map rerun.

The map also exposed a pre-existing index coordinate limitation: a `unique_name` CALLS edge from `main` reports a line beyond its declaration. Out-of-range sites are now labelled unverified and have no site navigation button. Declaration navigation remains available. Resolver method and confidence are shown as index metadata, not as calibrated probabilities. This repairs the UI claim; it does not repair the underlying C preprocessor coordinate mapping.
