# evals/ — index and search evals for both backends

Black-box evals of `codebase-memory-mcp` through MCP: does indexing cover
what is on disk, are the Neo4j schema indexes created and used, and do the
search tools answer correctly and identically on the **SQLite** and **Neo4j**
backends. Written 2026-11-12; first run 2026-11-12, second full run 2026-11-18 (see PILOT_ADJUSTMENTS.md).

```
evals/
  specs/        the evals, in detail (start with specs/00_overview.md)
  cases/        executable encodings: cases/common (both backends), cases/sqlite, cases/neo4j
  fixtures/     eval-small (committed, hand-verified ground truth in expected/), gen_medium.py, prepare.py
  harness/      run_cases.py (runner), assertions.py (check DSL), neo4j_http.py, paging_check.py,
                coverage_check.py, concurrent.py, compare_backends.py, report.py, run_all.py, selftest.py
  profiles/     sqlite.json, neo4j.json (copy to *.local.json; never commit a password)
  results/      run outputs (git-ignored)
  SCORECARD.md  template for recording a run
```

## Prerequisites

- A build of the fork on PATH as `codebase-memory-mcp`, or pass `--binary`.
  The eval binary and any other CBM process must be the same build (the daemon
  admission barrier refuses mixed versions).
- Python 3.9+ (standard library only), git.
- For the Neo4j profile: a disposable Neo4j 4.4/5.x reachable over HTTP. The repo's
  stack works: `docker compose -f docker/graph/compose.yaml up -d neo4j`
  (container `cbm-neo4j`, port 7474, password from `docker/graph/.env`). Export the
  password as `NEO4J_PASSWORD` for the harness. **Use an instance that holds
  nothing you care about**: N02/N04 drop an index and move a generation pointer by
  hand, N05 stops the container, N99 deletes projects.
- Windows: the harness is cross-platform (Python one-liners instead of shell
  tools); `docker stop/start` need Docker Desktop on PATH. The 2026-11-12 rig notes
  (isolated compose project with named volumes, `git archive` with autocrlf off
  for Linux builds) are in `test-env/DEV_TEST_REPORT_2026-11-12.md`.

## Run

```sh
# 1. working copies (fresh git repos; the evals edit files)
python3 evals/fixtures/prepare.py small  --work D:/cbm-evals/work
python3 evals/fixtures/prepare.py medium --work D:/cbm-evals/work     # optional, 20k Java files

# 2. profiles
cp evals/profiles/sqlite.json evals/profiles/sqlite.local.json      # set vars.repo
cp evals/profiles/neo4j.json  evals/profiles/neo4j.local.json       # set vars.repo, graph_db, container
export NEO4J_PASSWORD=...                                           # never in the file

# 3. SQLite, then Neo4j (the Neo4j parity suite diffs against the SQLite results)
python3 evals/harness/run_all.py --profile evals/profiles/sqlite.local.json
python3 evals/harness/run_all.py --profile evals/profiles/neo4j.local.json [--with-scale]

# one suite, a few steps, or a dry run that only validates templates
python3 evals/harness/run_cases.py --profile evals/profiles/neo4j.local.json \
    --cases evals/cases/neo4j/N02_neo4j_indexes.json --out evals/results/neo4j/N02_neo4j_indexes.jsonl [--only N02-07,N02-11] [--dry-run]

# scorecard from whatever has run
python3 evals/harness/report.py "evals/results/**/*.jsonl" --out evals/results/SCORECARD.md
```

Suite order and what each needs: `specs/00_overview.md`. Both profiles use the
same binary and therefore the same daemon; a `session` step that must change the
daemon's environment runs `daemon stop` first.

## Pass rules

- Every step has a tier. **Tier 1** failures are defects and fail the run (exit 1).
  **Tier 2** failures are reported for review (extractor/resolver/model-dependent
  details, environment-specific timings).
- The check vocabulary (`ok`, `json` path ops, `rows` on any tool table shape,
  `deep_contains`, `served_by`, `ms_under`, Cypher `rows_eq`/`row_eq`/`cell_contains`,
  `plan_has_operator`/`plan_uses_index`, SQLite rows, `same_as` across steps,
  `use_prev` to chain cursors, `any_of`) is documented at the top of
  `harness/assertions.py` and `harness/run_cases.py`. `harness/selftest.py`
  exercises it without a server.

## Pilot (first run) checklist

1. `python3 evals/harness/selftest.py` → `selftest_ok`.
2. `--dry-run` every case file with your local profile (no unknown `{{vars}}`).
3. Run the SQLite profile. Expect some tier-2 steps to need adjustment where the
   extractor's exact output differs (which tier-2 CALLS resolve, the exact
   `check_index_coverage` vocabulary, `get_architecture`/`manage_adr`/`ingest_traces`
   argument names flagged "adjust at pilot"). Fix the case file, not the product,
   when the eval was wrong; record the frozen values in `SCORECARD.md`.
4. Run the Neo4j profile. `N08_parity` must report `0 differing step(s)` for every
   common file; a difference is a Neo4j-path defect.
5. Keep the reviewed `results/` tree as the golden run (outside the repo), and
   compare future runs with `compare_backends.py <golden> <new>`.

## Coverage of the request

| Asked for | Where |
|---|---|
| Neo4j index (schema indexes/constraint: created, shape, ONLINE, used by the planner, recreated, not duplicated, scoped) | `specs/03_neo4j_indexes.md`, `cases/neo4j/N02` |
| Index creation and its coverage (files, symbols, edges, docs, infra, ignore rules, freshness, limits, recall script) | `specs/01`, `02`, `04`; `cases/sqlite/S01-S02`, `cases/neo4j/N01/N03/N04`, `cases/common/00`, `60` |
| Searching (structural, BM25, semantic, text, Cypher, traces, snippets, paging) | `specs/05`–`08`; `cases/common/10`–`80`, `sqlite/S03`, `neo4j/N06` |
| Both backends | every `cases/common` file runs on both profiles; `specs/09` + `cases/neo4j/N08` prove parity |
| Failure handling, secrets, isolation, scale | `specs/10`, `11`; `cases/neo4j/N05`, `N07`, `N09` |
