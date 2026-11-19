# 11 Scale and performance

Case file: `cases/neo4j/N09_scale.json` (Neo4j; the SQLite profile can run the
same file by pointing `project_medium`/`repo_medium` at a SQLite index of
`eval-medium`, the `graph_db` steps then simply index locally). Fixture:
`fixtures/gen_medium.py` via `prepare.py medium` (20,002 Java files; 5,000-caller
hub; 200 dead methods; chain of 20,000 `run()` calls; docstring tokens every
1,000th class). For estate-sized runs (838k and 3.3M nodes) use `test-env/scale`.

## Budgets

Budgets are per call, wall time measured by the harness (`ms`), on a developer
machine with Neo4j in Docker (heap 4 GB, page cache 2 GB). They come from the
2026-11 measurements (hub callers 11–13 s on 820k nodes; search_code on 197k hits
49 s; the largest store read 6.2 s) with headroom; tighten after the pilot.

| Id | Call | Budget | Tier |
|---|---|---|---|
| N09-02 | `index_status` after the sync | 60 s | 1 |
| N09-05 | `trace_path` inbound on the 5,000-caller hub, depth 1, limit 100 | 30 s | 1 |
| N09-07 | `query_graph` hub caller count | 60 s | 1 |
| N09-09 | `get_code_snippet` hub with neighbours | 30 s | 1 |
| N09-10 | one-caller chain query (`ENDS WITH 'C10000.run'`) | 60 s | 1 |
| N09-12/13 | `search_graph label Method` page 1 and offset 50,000 | 30 s each | 1 |
| N09-14/15 | `query_graph` 5,000-row cursor pages | 120 s each | 1 |
| N09-17 | `search_code` with 5,000 hits | 120 s | 1 |
| N09-19 | BM25 docstring token | 30 s | 1 |
| N09-20 | `get_graph_schema` | 60 s | 1 |
| N09-21 | `get_architecture` default | 300 s | 1 |
| N09-26 | 8 concurrent sessions × 2 rounds × 6 calls | no errors/hangs; max 120 s | 1 |

## Correctness at scale

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| N09-03 | indexes after a large sync | all six `ONLINE`, 100% | 1 |
| N09-04 | ≥ 100,000 rows | | 1 |
| N09-06/08 | hub callers | exactly 5,000 (trace_path and query_graph agree) | 2 (resolver) |
| N09-11 | dead methods via `NOT EXISTS` | 200 (engine label scan over ~60k methods; time recorded) | 2 |
| N09-13 | deep offset page | 500 unique qns | 1 |
| N09-15 | second cursor page | unique rows | 1 |
| N09-16 | `paging_check.py` over all Method pages (page 500) | complete and disjoint, union = total | 1 |
| N09-18 | `total_grep_matches` | 5,000 | 2 |
| N09-22..25 | one-line edit → re-index | `delta: nodes +0 ~k -0` with k < 100; time recorded (the 12k-class run of 2026-11-12 measured ~8 s for a one-file delta vs ~90 s full) | 1 |
| N09-27 | daemon RSS (Linux) | recorded; the 2026-11 run saw 303–347 MB peak on the feature branch vs 1,207 MB on main | 2 |

## What to record in the scorecard

- Cold index + sync time (N09-01), delta re-index time (N09-23), full-sync node and
  edge counts (`index_status.graph_backend.last_sync`).
- Every budgeted call's `ms` and `graph_backend.remote_queries`.
- `concurrent.py` summary (p50/p95/max, slowest calls).
- `SHOW INDEXES … readCount` deltas before/after the battery (spec 03).

## Estate-sized runs

`test-env/scale/scripts/battery.py`, `conc.py`, `sustain.py` and `cfg/*.json` drive
the 838k-node and 3.3M-node datasets built by `test-env/gen/gen_repo.sh`; their
budgets and the measured numbers are in `test-env/scale/RESULTS.md`. Promote a
battery to `cases/neo4j/` only once the dataset generator is committed under
`evals/fixtures/` with a fixed seed.
