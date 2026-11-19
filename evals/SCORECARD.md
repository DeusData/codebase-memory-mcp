# Eval scorecard — template

Copy this file into the run's results folder (outside the repo) and fill it in.
`harness/report.py` produces the per-suite table; the rest is by hand.

## Run

| Item | Value |
|---|---|
| Date | |
| Branch / commit under test | |
| Binary (path, sha256) | |
| Baseline binary (if comparing) | |
| Host (OS, CPU, RAM, disk) | |
| Neo4j (image, version, heap, page cache, container) | |
| Index mode (`index_mode`) | full |
| Fixture commit (`prepare.py` output) | |

## Per-suite results (paste from `results/SCORECARD.md`)

| suite | backend | pass | fail | tier-1 fail | skip |
|---|---|---|---|---|---|
| | | | | | |

## Tier-1 failures (each is a defect)

| step | suite | what failed | evidence (results file, line) | ticket |
|---|---|---|---|---|
| | | | | |

## Tier-2 review

| step | observed | decision (frozen value / defect / accepted) |
|---|---|---|
| | | |

## Coverage recall (`coverage_check.py`, spec 04)

| metric | sqlite | neo4j |
|---|---|---|
| files_tier1 / tier2 | | |
| symbols_tier1 / tier2 | | |
| calls_tier1 / tier2 | | |
| hub_inbound (12) | | |
| dead_code clean | | |

## Neo4j indexes (spec 03)

| index | state | population | plan check | readCount before → after |
|---|---|---|---|---|
| cbm_node_db | | | | |
| cbm_node_id | | | | |
| cbm_node_qn | | | | |
| cbm_node_name | | | | |
| cbm_node_file | | | | |
| cbm_node_label | | | | |
| cbm_project_name (constraint) | | | | |

## Timings (ms, from the results files)

| call | sqlite | neo4j | budget |
|---|---|---|---|
| S01-01 / N01-01 cold index (+ sync) | | | record |
| S01-06 / N01-15 no-op re-index | | | record |
| N03-08 delta re-index (docstring edit) | | | record |
| SG-01 name lookup | | | 5,000 |
| QG-02 hub callers | | | 5,000 |
| TP-01 trace hub | | | 5,000 |
| SC-01 search_code | | | 20,000 |
| N09-05 hub 5,000 callers | | | 30,000 |
| N09-26 concurrency p50 / p95 / max | | | 120,000 max |

## Parity (spec 09)

| common file | differing steps | notes |
|---|---|---|
| | | |

## Sync (spec 02)

| event | last_sync text | nodes / edges |
|---|---|---|
| first sync | | |
| no-op | | |
| docstring edit | | |
| add file | | |
| delete file | | |
| forced full | | |
