# 10 Failure handling and security

Case files: `cases/neo4j/N05_unavailable.json`, `cases/neo4j/N07_security.json`.
Partly covered elsewhere: N01-19/20 (bad URL, dead host), N04-08..19 (tampered
generation, deleted graph), N06-07..22 (write guard, scoping), S99/N99 (delete).

## Neo4j unavailable (N05)

The container is stopped with `docker stop {{neo4j_container}}` (skipped when the
profile has no container name; stop it by hand then).

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| N05-02..09 | search_graph, query_graph, trace_path, get_code_snippet, search_code, get_graph_schema, detect_changes, native query | each fails **under 20 s** with `graph_backend.status unavailable` and a reason naming Neo4j; never a local answer | 1 |
| N05-10/11/12 | index_status, list_projects, check_index_coverage | keep working; index_status says unavailable + reason | 1 / 1 / 2 |
| N05-13/14 | re-index while down | fails and says so; `last_sync_error` recorded | 1 / 2 |
| N05-15 | a second read | still bounded (no retry storm, no growing backoff) | 1 |
| N05-16..21 | start, wait for HTTP, re-index, status, read | serving, in sync, no lingering error, `served_by neo4j` | 1 |
| N05-22..24 | wrong password on a new project (one attempt) | clear error under 30 s, nothing registered, the password not echoed, the existing project still serves | 2 |

Why one attempt: Neo4j locks an account after repeated failures within a short
window; the product itself refuses to retry rejected credentials for 60 s.

The 2026-11 run measured the dead-server path at 120–262 s before the fix and 15 s
after; 20 s is the budget here.

## Secrets (N07-01..06)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| N07-01/02 | `index_status` (verbose, full diagnostics), `list_projects stats` | the masked password (`***`) never appears in text or structured output | 1 |
| N07-03 | marker file | does not contain the password | 1 |
| N07-04 | every file under `<cache>/logs` | zero files contain the password (the daemon log records Cypher at debug level, never the Authorization header) | 1 |
| N07-05 | every file under `evals/results` | zero (the harness masks it; this proves the masking) | 1 |
| N07-06 | failed login | the attempted password is not echoed in the error | 1 |

## Project isolation (N07-07..18)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| N07-07 | rows under this project's `db` | none belong to another `project` (except `::missed`) | 1 |
| N07-08/09/10 | File count on A, on B (same repo), in Neo4j for both | A = B; Neo4j holds 2×, so A's count was not "all of Neo4j" | 1 |
| N07-11/12 | trace_path and search_graph on A | never a qualified name of B | 1 |
| N07-13..17 | delete B | B's rows and `CbmProject` gone; A unchanged; the six indexes untouched | 1 |

Native Cypher (`native: true`) can read any db in the same Neo4j: N06-23 records
it; the policy decision is open (spec 07).

## SQLite-side failures (not encoded; procedures)

- **Locked/busy database**: start a long `index_repository` of `eval-medium` and
  run `common/30` concurrently; every read must answer from the previous
  generation (no `database is locked` error). The daemon serializes writes per
  project; reads use a separate query store.
- **Corrupt database**: copy `<db>` aside, truncate the last 4 KB of the live file
  with the daemon stopped, start a session: `index_status` must report an error
  for that project; other projects must still work; `delete_project` must remove
  it. Restore the copy afterwards.
- **Cache dir not writable**: run with `CBM_NEO4J_CACHE_DIR` pointing at a
  read-only directory; `initialize` must still succeed and `index_repository` must
  fail with a clear message.

## Fault injection on the Neo4j path (procedure)

`test-env/functional/proxy.py` is an HTTP fault proxy (close, 401, hang, truncate,
error on the N-th statement). Re-run `cases/common/20_search_code.json` and
`cases/common/70_detect_changes.json` through it with the "batched node read
fails" fault: the replies must be errors (or carry a warning with
`total_relation gte`), never a silent `0 results`/`impacted_total 0`. Findings
M4/M7/M10 and M5b from 2026-11 are the reference; promote to a case file once the
proxy is moved under `evals/harness/`.
