# 01 Index creation: SQLite backend

Case files: `cases/sqlite/S01_index_create.json`, `cases/sqlite/S02_sqlite_schema.json`,
`cases/sqlite/S99_cleanup.json`. Profile: `profiles/sqlite.json`.

## Preconditions

- `prepare.py small` has produced `{{repo}}` (fresh git repo of eval-small).
- No project named `{{project}}` exists (`list_projects`), or S99 was run.
- The binary under test is on PATH or given with `--binary`; its daemon starts on
  demand. `{{cache_dir}}` is `CBM_NEO4J_CACHE_DIR` or `~/.cache/codebase-memory-mcp-neo4j`.

## S-IDX: creating and maintaining the index (S01)

| Id | Eval | Expectation | Tier |
|---|---|---|---|
| S01-01 | Fresh `index_repository` in the profile's mode (`full` by default) | Succeeds, reply carries a `status`, no error text | 1 |
| S01-02 | `.cbmignore` subtrees are reported | Reply's `excluded` names `vendor` (documented in `docs/cbmignore.md`) | 2 |
| S01-03 | `index_status` | `graph_backend.kind = sqlite`, a `status` | 1 |
| S01-04 | `list_projects detail=stats` | The project is listed with counts | 1 |
| S01-05 | File nodes | `MATCH (f:File) RETURN count(f) >= 14` (the tier-1 source files) | 1 |
| S01-06/07 | No-op re-index | Succeeds; node count unchanged (incremental path, nothing rewritten) | 1 |
| S01-08/09/10 | `persistence:true` | Writes `<repo>/.codebase-memory-neo4j/graph.db.zst`; the eval deletes it afterwards | 2 |
| S01-11 | Nonexistent path | Error | 1 |
| S01-12..17 | `async:true` then `status:true` | Returns at once with `state` queued/running; polling reaches `succeeded`; `async`+`status` together is refused | 2 |
| S01-18..20 | `mode: fast` into `<project>-fast` | Succeeds; schema has no `SIMILAR_TO`/`SEMANTICALLY_RELATED`; same File count as the full index | 2 |
| S01-21 | `semantic_query` on the fast index | Rejected or empty, never a crash | 2 |
| S01-22/23 | `name` with unsafe characters | Normalized and listed | 2 |

What to record: wall time of S01-01 (cold full index) and S01-06 (no-op
re-index), from the `ms` field.

## S-SCHEMA: the SQLite store (S02)

Read directly from `{{db_path}}` with Python's `sqlite3` in read-only mode while the
daemon may hold the file (WAL allows this).

### Tables and indexes

| Id | Check | Pass rule | Tier |
|---|---|---|---|
| S02-01 | `sqlite_master` tables | `projects`, `file_hashes`, `nodes`, `edges`, `project_summaries`, `index_coverage`, `nodes_fts` exist (`src/store/store.c` DDL) | 1 |
| S02-02 | user indexes | the nine `idx_*` from `create_user_indexes`: `idx_nodes_label/name/file`, `idx_edges_source/target/type/target_type/source_type/url_path` | 1 |
| S02-03 | index columns | `nodes(project, label)`, `nodes(project, name)`, `nodes(project, file_path)`, `edges(project, target_id, type)` | 1 |
| S02-04 | FTS5 | `nodes_fts` is `fts5`, external content (`content=''`), tokenizer `unicode61`, has the `body` column (docstrings) | 1 |
| S02-05/06 | FTS backfill | `count(nodes_fts) = count(nodes)` | 1 |

### Query plans (EXPLAIN QUERY PLAN)

The store's own searches must be index-backed. Each plan must name the index and
must not be a full `SCAN nodes`.

| Id | Query shape | Index expected | Tier |
|---|---|---|---|
| S02-07 | `project = ? AND label = ?` | `idx_nodes_label` | 1 |
| S02-08 | `project = ? AND name = ?` | `idx_nodes_name` | 1 |
| S02-09 | `project = ? AND file_path = ?` | `idx_nodes_file` | 1 |
| S02-10 | `source_id = ? AND type = ?` | an `idx_edges_source*` | 1 |
| S02-11 | `target_id = ? AND type = ?` | an `idx_edges_target*` | 1 |
| S02-12 | `project = ? AND type = ?` on edges | `idx_edges_type` | 1 |
| S02-13 | project + label | no `SCAN nodes` | 1 |

### Content and integrity

| Id | Check | Tier |
|---|---|---|
| S02-14 | no `vendor/` or `generated/` rows in `nodes` | 1 |
| S02-15/16/17 | `projects` has the project; `file_hashes` has the 13 ASCII tier-1 paths and the unicode path | 1 / 1 / 2 |
| S02-18 | every edge endpoint exists (no dangling `source_id`/`target_id`) | 1 |
| S02-19 | every `properties` JSON is valid (the FTS backfill guards against invalid rows; there must be none in a fresh index) | 1 |
| S02-20 | `PRAGMA integrity_check = ok` | 1 |
| S02-21 | `PRAGMA journal_mode` recorded (WAL expected) | 2 |
| S02-22/23 | FTS `body:hyphenate` finds exactly one row; `name:checkout` finds rows | 2 |

## S-CLEAN (S99)

`delete_project` removes the `-fast` and main projects; `list_projects` no longer
lists them; a read on the deleted project is a clean "not found"; the `.db` file is
gone. Run last.

## Not encoded (manual or future)

- Opening a database written by an older build (legacy FTS without `body`) still
  searches without prose: covered by unit tests in `tests/test_store_search.c`;
  needs an archived old database to eval.
- `index_max_source_mb` behaves like `index_max_files` (COV-45..48 covers files).
- Watcher auto-refresh (`auto_watch`, `watcher_enabled`): needs a long-lived
  session and a timed edit; see `docs/CONFIGURATION.md`. Suggested eval: edit a
  file, wait `poll_interval_ms` + margin, check `index_status.watch.last_scan_at`
  advanced and the symbol changed.
