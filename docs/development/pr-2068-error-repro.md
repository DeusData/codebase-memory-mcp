# PR 2068: real index-error and local-diagnosis verification

This exercise uses an explicitly synthetic repository at
`/tmp/cbm-2068-error-fixture`, project `pr2068-error-fixture`, against the actual
compiled application on `http://127.0.0.1:9749`. It does not modify the real
`cbm-pr2068` repository or its index. The daemon uses the isolated cache
`/tmp/cbm-2068-cache`; the browser, MCP requests, index requests and event polling
all use that one daemon origin.

## Reproduce

With the isolated daemon already running, from `graph-ui`:

```sh
node tools/pr-2068-errors-e2e.mjs
```

The script refuses a pre-existing fixture directory unless its explicit test
marker matches. It creates these test inputs:

- `readable.c`: valid C function.
- `partial.c`: malformed C declaration on line 2, producing the real indexer's
  `parse_partial` coverage record, with source range 2-2.
- `excluded.c`: excluded by an actual `.cbmignore` rule, producing
  `not_indexed_file`, reason `cbmignore`.
- `unreadable.c`: initially readable so a real index can be established; then
  permission mode `000`, with an independent Node file-read check for `EACCES`.

After the first successful index, the script opens the real application and
closes the first-visit welcome with its normal button. While System, and then
Galaxy, is already visible, it requests a new index with the inaccessible input.
It requires the resulting job to report `error`, matches a new persisted
`ui.index.done` error by its monotonic event ID, and waits for that same event to
be visible in the shared priority feed without reloading the page. It then
opens the local diagnosis panel, verifies that no report exists before the
explicit button click, runs the existing coverage RPCs, edits the report and
verifies that the downloaded file contains exactly that edited text.

All browser requests are recorded. The browser route rejects any origin other
than the local daemon (local Blob/data URLs are allowed). SQLite evidence is
read with `mode=ro` from the real `activity.db` and the fixture's project DB.
The script restores only its owned test file's permissions to `0600` in a
`finally` block. It deliberately leaves the clearly named test project and
fixture available for inspection.

## Observed failure and correction

The first two harness attempts exposed harness setup omissions: `index_status`
defaults to a human-readable format, so the script now explicitly requests
`format: "json"`, as the production client does; and a fresh browser must close
the welcome dialog before clicking the workspace tabs. Those attempts are
preserved separately and are not claimed as successful UI tests.

The third attempt exposed a real application bug. With `unreadable.c` at mode
`000`, SQLite event 747 recorded an `index.worker` error containing
`semantic_manifest.err` and the inaccessible file path. The following supervisor
result reported `clean`, and event 749 incorrectly recorded `ui.index.done
rc=ok`; `/api/index-status` returned `done`.

The coordinator had treated a non-null response from a cleanly exited worker
as success, ignoring the valid MCP result's `isError: true`. The pipeline itself
had correctly reported an aborted refresh that preserved the previous database.
The coordinator now reads the MCP success/error result, and a regression
test covers clean worker exits carrying aborted, failed and malformed results. The strict end-to-end job assertion remains in the script.
The original contradictory log rows are retained in
`graph-ui/verification/pr-2068/errors-e2e-attempt3.json`.

## Final integrated run

The coordinated C/UI rerun completed on 2026-09-09 at 19:52:04 UTC. Both real
permission-failure reindexes returned `error`. Persisted completion events 2265
and 2285 appeared in System and Galaxy respectively while each view was already
open, without a page reload. System also displayed the failed index task. The
compact priority feed measured 128.5 px at a 1560 × 1100 viewport.

The browser recorded 32 same-daemon-origin requests, no external attempts and no
JavaScript page errors. Merely opening the diagnosis produced no report and no
requests. The explicit diagnosis action used local coverage RPCs; the callback
refreshed the shared project reading. Editing and downloading preserved exactly
the edited text. Read-only SQLite queries matched the displayed event IDs and
the two actual coverage rows. Four real screenshots and their SHA-256 hashes
are recorded in `graph-ui/verification/pr-2068/errors-e2e.json`.

Visual inspection found one remaining evidence loss: the existing shared
coverage parser discarded structured `ranges` from `check_index_coverage`,
causing the partial parse to show no recorded reason despite source line 2 being
known. The shared parser now preserves validated range endpoints as `Source
lines: 2-2`, without replacing an existing detail. The focused shared-parser and
diagnosis suites passed 54 tests. The final embedded-UI rerun passed the explicit source-range assertion. The
diagnosis view and the local report both show `Source lines: 2-2`. All four
current screenshot hashes were independently verified after capture.

The preserved fixture index generation is `2026-09-09T19:29:28Z`; this is the
previous successful index, not the time of either failed refresh. The diagnosis
was checked at `2026-09-09T19:52:03.390Z`. The original graph still contains
10 nodes and 9 relationships.

| Actual browser capture | Verified behavior |
| --- | --- |
| [System](../../graph-ui/verification/pr-2068/errors-after-system.png) | Live red error with event ID and a failed index job |
| [Galaxy](../../graph-ui/verification/pr-2068/errors-after-galaxy.png) | Same daemon error feed while the graph remains navigable |
| [Local diagnosis](../../graph-ui/verification/pr-2068/errors-local-diagnosis.png) | Actual parser source range and explicit exclusion rule |
| [Editable local report](../../graph-ui/verification/pr-2068/errors-local-report.png) | Reviewable local draft; copy and download controls |

[Machine-readable evidence](../../graph-ui/verification/pr-2068/errors-e2e.json)
contains the requests, API results, exact SQLite rows, screenshot hashes and
post-run row comparisons. No further source changes followed this final run.

## Interpretation boundaries

An unreadable input is rejected while building the semantic manifest, before
new coverage is published. The old graph and its old coverage are intentionally
preserved. Therefore the permission error is evidenced by the worker/job logs;
the `parse_partial` and `.cbmignore` rows describe the preceding successful
index. They must not be relabeled as a new `read failed` coverage row.

The existing path freshness check compares size and modification time. A
permission-only change may still report `metadata_match`; that is not a
content-hash or readability guarantee. The diagnosis UI states this limit and
keeps generation/recording metadata visible. Coverage counts describe recorded
paths, including directories, and do not establish a complete file denominator.

Local report generation, editing and download are tested here. No issue,
external report, telemetry submission or account is created. A local report is
not authorization to publish its contents.
