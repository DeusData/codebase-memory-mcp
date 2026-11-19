# 06 Searching: search_code

Case file: `cases/common/20_search_code.json` (both backends).

`search_code` greps the indexed files (ripgrep/grep, scoped to the project's root
and to the indexed file list) and classifies every hit under the symbol that
contains it, using the graph (Neo4j for a Neo4j project: batched node reads by
file window). The JSON reply has `cols/rows` (classified results), `raw_matches`
(hits outside any symbol), `directories` (histogram), `total_grep_matches`,
`total_results`, `total_relation`, and three independent pagers
(`result_*`, `raw_*`, `directory_*`).

## Evals

| Id | Pattern / args | Pass rule | Tier |
|---|---|---|---|
| SC-01 | literal `hub()` | ≥ 12 grep matches, `hub/` directory, classified rows include `caller_*`; under 20 s | 1 |
| SC-02 | `round_cents` | ≥ 3 matches across `app/money.py` and `app/models.py` | 1 |
| SC-03 | regex `caller_[0-9]+` | 12 definitions + the docstring line: 12 ≤ n ≤ 14 (freeze exact at the pilot) | 1 |
| SC-04 | `mode files` | both files listed, nothing from `vendor/` | 1 |
| SC-05 | `mode full`, `source_max_lines 5` | source of `slugify` present | 2 |
| SC-06 | `file_pattern *.java` | no `.py` anywhere in the answer | 1 |
| SC-07 | `path_filter java/src/test` | only the test file | 1 |
| SC-08 | unicode pattern | found in `ünïcødé.py` | 1 |
| SC-09 | docs-only phrase | exactly 1 match, attributed to `docs/ARCHITECTURE.md` | 1 |
| SC-10/11 | `vendored_fn`, `generated_fn` | 0 matches: ignored files are never scanned | 1 |
| SC-12/13 | `(` as regex → error; `hub(` as literal → fine | | 1 |
| SC-14 | nonsense | 0 matches, 0 results, no error | 1 |
| SC-15/16 | `result_limit 2` then `result_offset 2` | `results_returned 2`, `has_more`, `next_offset 2`; same `total_results` | 1 |
| SC-17/18 | `raw_limit 1` / `0` | raw rows bounded / suppressed | 2 |
| SC-19 | `context 2`, full | neighbouring line present | 2 |
| SC-20 | `debug true` | `scan_ms` present | 2 |
| SC-21 | `String.format("%02d"` literal | ≥ 1 | 2 |
| SC-22 | `directory_limit 1` | `directories` present | 1 |
| SC-23 | `path_filter ../..` | no `../` path in any answer (scan confined to the root) | 1 |
| SC-24 | tree format | renders | 1 |
| SC-25 | Neo4j project | `graph_backend.kind neo4j` | 1 |
| SC-26 | `total_relation` is `eq` or `gte` | | 1 |

## Failure semantics (from the 2026-11 functional run)

When the batched node read that classifies hits fails, is truncated or hangs, the
reply must not silently demote every hit to raw with `total_relation eq` and no
warning (finding M4/M7/M10). This is not encoded as a case (it needs a fault
proxy); the procedure is in spec 10. Until then, SC-01's `caller_*` classification
is the smoke check that classification is alive.

## What to record

`scan_ms`/`enrich_ms` from SC-20 per backend; on `eval-medium` the N09-17/18
numbers (5,000 hits).
