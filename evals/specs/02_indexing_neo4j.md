# 02 Index creation: Neo4j backend

Case files: `cases/neo4j/N01_index_create.json`, `N03_delta_sync.json`,
`N04_generations.json`, `N99_cleanup.json`. Profile: `profiles/neo4j.json` with
`NEO4J_PASSWORD` in the environment.

## Model under test (from `src/neo4j/neo4j_sync.c`, `neo4j_store.c`, README)

- Indexing always builds the local SQLite index first; after every successful
  publish the graph is mirrored into Neo4j as `(:CbmNode:<Label> {db, gen, project,
  id, label, name, qualified_name, file_path, start_line, end_line, properties_json,
  prop_keys, …flattened scalar props})` and `-[:<TYPE> {id, type, …}]->` edges
  (`CBM_EDGE` when the type is not an identifier).
- `(:CbmProject {name, gen, generation, sync_format, nodes, edges})` points at the
  live generation. A **full** sync writes a new `gen`, flips the pointer, deletes
  the old generation. A **delta** sync edits the live generation in one
  transaction, guided by `<db>.neo4j-state.db` (id + row hash per node/edge), only
  when `CbmProject` still matches that record; else full. `CBM_NEO4J_DELTA_MAX_ROWS`
  (default 50,000; `0` = off) and a 32 MB cap force full.
- `<db>.graph.json` (marker): uri, user, database, synced_generation, last_sync,
  last_sync_error. Never the password.
- There is no fallback: if Neo4j cannot serve the project's current generation,
  graph reads fail with `graph_backend.status = unavailable` and a reason.

## N-IDX: first index and sync (N01)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| N01-01 | `index_repository` with `graph_db/graph_user/graph_password/graph_database` | Succeeds; the password never appears in the reply | 1 |
| N01-02/03 | `index_status.graph_backend` | `kind neo4j`, `status serving`, `in_sync true`, `uri`, `database`, `synced_generation = local_generation`, `last_sync` starts with `full`, no `last_sync_error` | 1 |
| N01-04 | marker file | has `uri`, `synced_generation`; no key containing "pass"; masked password absent | 1 |
| N01-05..07 | `CbmProject` | one node; `sync_format = 2`; `generation` equals the synced generation string | 1 |
| N01-08..12 | counts | Neo4j node count (db, live gen, project) = engine `count(n)`; same for edges; `CbmProject.nodes/edges` recorded | 1 |
| N01-13/14 | source naming | `served_by: neo4j` in `structuredContent.graph_backend`, "Neo4j" in the text, `served_by=neo4j` lines in the daemon log | 1 |
| N01-15/16 | flagless re-index | the marker keeps the target; still serving and in sync | 1 |
| N01-17/18 | second project from the same repo | two disjoint `db` keys | 1 |
| N01-19 | `bolt://` | refused with a hint naming HTTP / 7474 | 1 |
| N01-20/21 | unreachable host | fails before indexing, under 30 s; nothing registered | 1 |
| N01-22..26 | `graph_db: sqlite` | returns the project to local reads, Neo4j data left in place; re-attach works | 2 |

Record: time of N01-01 (index + full sync) versus S01-01 (index only) for the
same fixture; the difference is the sync cost.

## N-DELTA: delta sync (N03)

Preconditions: N01 done; the working copy is clean (`git status` empty).

| Id | Change | Expected `last_sync` | Other checks | Tier |
|---|---|---|---|---|
| N03-04..06 | none | in sync, no error (text recorded: skipped / zero delta) | node count unchanged | 1 |
| N03-07..15 | docstring edit in `app/money.py` | `delta: …` | nodes `+0 ~k -0` with small k (tier 2); node and edge counts unchanged; `properties_json` of `round_cents` carries the new docstring; `get_code_snippet` serves it; hub still has 12 callers | 1 (count/edge/content), 2 (k) |
| N03-16..21 | add `app/extra.py` (two functions, one call) | `delta` or `full` (adding a file may renumber ids; record which) | both functions and the `extra_two -> extra_one` CALLS edge exist in Neo4j; search_graph sees them | 1 |
| N03-22..26 | delete `app/extra.py` | `delta` or `full` | rows gone; counts back to baseline; no relationship joins a dead generation | 1 |
| N03-27..29 | restore `money.py` | any | `DELTA-EDIT` gone | 1 |
| N03-30..35 | delete `<db>.neo4j-state.db` | `full …` (reason given) | state file rewritten; counts baseline; only one generation remains | 1 |
| N03-36..39 | daemon restarted with `CBM_NEO4J_DELTA_MAX_ROWS=0`, docstring edit | `full` | | 2 |
| N03-40..44 | variable unset, restore edit | `delta` | `git status` clean | 2 |

Pass rule: every tier-1 row. The delta must never lose or duplicate an untouched
row (N03-11/12/15/25/26 are the guards).

## N-GEN: generations and consistency (N04)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| N04-01..07 | forced full sync | `CbmProject.gen` increases; the old generation's rows are deleted; exactly one `gen` value under the db; reads use the new one | 1 |
| N04-08..14 | `CbmProject.gen` moved by hand | search_graph and query_graph fail with `status unavailable` (no local fallback); `index_status` says unavailable with a reason; `list_projects` works; undoing the tamper restores serving without a re-index | 1 (tamper step itself 2) |
| N04-15..19 | graph deleted by hand in Neo4j | a read is an error or empty, never a stale local answer; re-index repairs with a `full` sync; hub has 12 callers again | 2 / 1 |
| N04-20/21 | generation string | `index_status.synced_generation = CbmProject.generation` | 1 |

Not encoded: the "readers see old or new, never half" property during a sync.
Suggested manual eval: run `harness/concurrent.py` with a `count(n)` call while a
forced full sync of `eval-medium` runs; every answer must equal either the old or
the new count. The functional run of 2026-11-02 (R2-01..11) covered generation
flips within one session.

## N-CLEAN (N99)

`delete_project` removes the project from Neo4j too: zero `CbmNode` rows with the
db prefix, zero `CbmProject`, the six indexes untouched, local `.db`, marker and
state file gone, reads fail with "not found".
