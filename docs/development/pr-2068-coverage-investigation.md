# Coverage and repeated worker warnings: observed repository data

Read-only checks against the actual daemon on 127.0.0.1:9749 and its SQLite
project `cbm-pr2068`, generation `2026-09-09T19:51:51Z`, agree on 281 recorded
coverage rows. This is a count of diagnostic records, not a repository-wide
coverage denominator.

| Recorded category | Count | Meaning supported by the data |
| --- | ---: | --- |
| `parse_partial` | 85 | The index parser reported source ranges it could not reliably interpret. These files are not intentional exclusions. |
| `not_indexed_dir` | 9 | Explicitly excluded subtrees: `.git`, build output, dependencies/vendor directories and `private`. The stored reason is `excluded subtree`. |
| `not_indexed_file` | 187 | 179 files excluded by `ignored-suffix` and 8 by `gitignore`. Examples include PNG screenshots and generated TypeScript build metadata. |
| Other skipped/read/extract failure rows | 0 | None recorded for this project in this generation. This does not prove every undiscovered file was considered. |

The 85 parser records group into 46 under `src`, 16 under `tools`, 12 under
`tests`, 5 under `scripts`, 3 under `internal`, 2 under `graph-ui` and 1 under
`pkg`. These are source-location groups, not assigned causes.

A parser gap is not itself proof of a compilation or runtime error. Concrete
examples distinguish the evidence:

- `tests/fixtures/plsql/create_type_as_object_limitation.tps:1` explicitly
  documents a known upstream grammar limitation for `CREATE TYPE ... AS
  OBJECT`. `tests/test_extraction.c:1345` positively tests that limitation and
  the missing Class definition. This is a deliberate test fixture of a real
  parser limitation, not an exclusion.
- `src/pipeline/pipeline.c:247` uses `CBM_TLS` in declarations; the macro expands
  to standard C11 `_Thread_local` in `src/foundation/compat.h:25`. Preprocessor
  token handling is a plausible explanation for this parser finding. No
  subtree investigation was performed to establish the exact cause.
- `graph-ui/src/galaxy/coverage-shadow-scene.test.tsx:10` contains a generic
  asynchronous import expression accepted by the TypeScript/Vitest checks.
  The coverage range alone does not establish invalid TypeScript.
- `graph-ui/src/galaxy/hierarchy-layout.ts:249` includes a literal NUL byte in
  template text. The exact reason the index parser flags it remains unknown.
- `src/cli/cli.c` has a broad reported range, lines 1-13337. It remains visible
  as a substantial evidence limitation; this investigation does not dismiss
  that range or claim the graph extracted every symbol in the file.

## Why the warning count was larger than the file count

The captured activity database held 177 `index.worker` records: 172 warnings
and 5 errors. Of those warnings, 170 were two occurrences each of the same 85
project parser diagnostics. For example, `src/cli/cli.c` was logged at
19:32:04 UTC and again at 19:51:43 UTC during separate indexing activity. The
remaining two warnings were one explicit test-fixture partial parse and one
unattributed pipeline-route fallback. The five errors were the explicit
permission-denied test-fixture attempts.

These durable event rows are not 177 current frontend failures or 172 distinct
unparsed files. The indexer emits a warning for each recorded partial parse on
each relevant run. Retained event counts therefore depend on history and the
response limit. The current coverage table is the source for the current
unique path records; errors and warning history remain separate evidence.

## Confirmed frontend defects and implementation

`index_status` intentionally returned summary counts with zero detail rows and
`truncated: true`. `check_index_coverage` returned all 281 rows with a complete
root-scope reading. The join nevertheless carried the summary's truncation
warnings into the UI after the omitted rows had already been retrieved.

A second defect affected genuinely larger responses: the production daemon
puts `has_more` and `next_offset` on the response envelope. The client only
read those fields inside each scope, so it would stop after one page. Existing
mock tests had modeled the wrong wire location. It also retained every earlier
page's `has_more` as an unresolved warning after later pages arrived.

The shared reader now accepts the real envelope continuation for its single
root scope. The join verifies the final page, consistent totals and distinct
path/kind rows before declaring a recorded listing complete. Only summary
omissions that have actually been covered are cleared. Duplicate pages,
missing rows, unavailable coverage, partial subtree queries and the page cap
retain uncertainty. An index-generation change between the summary and pages
fails the reading instead of combining snapshots. The 85 parser records and
196 explicit exclusions remain visible.

The diagnosis panel explains the distinction between parser limitations,
malformed input and program failures. A complete recorded listing explicitly
retains the existing parser limitations and exclusions.

## Executed verification

- 64 focused shared coverage/parser/diagnosis tests passed, including real
  envelope pagination, duplicate/missing rows, completed abbreviation
  recovery, unavailable coverage and generation changes.
- The updated production TypeScript reader was bundled and run against the
  real read-only RPC. A verification-only scope limit of 100 forced three
  pages: offsets 0, 100 and 200 returned 100, 100 and 81 entries. Result:
  281 rows, `listingComplete: true`, no unresolved truncations, 85 partial
  parses still present, generation unchanged.
- The coordinated frontend build passed, including 2387 frontend tests.
  Full TypeScript checking (`npx tsc -b --pretty false`) and
  `git diff --check` also passed.

[Captured RPC, SQLite and updated-client evidence](../../graph-ui/verification/pr-2068/coverage-investigation.json)
contains the original data, classified counts and real pagination result. No
indexer code or repository index was mutated for this investigation. The newly embedded UI was checked at 20:15:31 UTC on the same local daemon.
It displayed 85 partial parses and 196 intentional exclusions, explicitly
confirmed that all recorded entries were loaded, and showed no false
`Incomplete list` warning. There were 100 same-origin browser requests, no
external attempts or JavaScript page errors, and no index mutation.

[Actual diagnosis screenshot](../../graph-ui/verification/pr-2068/coverage-after-diagnosis.png)
and [browser verification](../../graph-ui/verification/pr-2068/coverage-ui-check.json)
record the final rendered result and its screenshot hash.
