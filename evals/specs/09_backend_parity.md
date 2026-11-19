# 09 Backend parity: Neo4j answers equal SQLite answers

Case file: `cases/neo4j/N08_parity.json`. Tool: `harness/compare_backends.py`.

## Claim under test

For a project indexed from the same working copy in the same mode, every graph
tool returns the same answer whether the project is SQLite-backed or
Neo4j-backed. The Neo4j copy is a mirror of the SQLite generation; search_graph
narrows candidates in Neo4j and applies the same regex/glob code locally; the
engine evaluates the same plan over the remote store. Any difference is a defect in
the Neo4j path (sync, routing, paging, conversion) by definition.

## Method

1. Run every `cases/common/*.json` and `cases/sqlite/S03_fts_bm25.json` on the
   SQLite profile (project `eval-small`) and on the Neo4j profile (project
   `eval-small-n4`, same `{{repo}}`, same `index_mode`).
2. `compare_backends.py A B` pairs steps by id and compares `structured` after:
   - dropping `graph_backend`, timing fields (`ms`, `scan_ms`, …), cursors
     (opaque per backend), generation strings, `root_path`/size fields, and
     warnings that only mention timing;
   - replacing each run's project name with `<project>` (qualified names differ
     only by the project prefix);
   - sorting rows for ranked/unordered suites (`--unordered`: BM25, semantic).
3. Pass rule: `0 differing step(s)` for N08-01..10 and N08-12 (tier 1). N08-11
   (architecture) is tier 2 because bounded jobs may be observed at different
   stages; its diff is reviewed.

## Known, accepted differences

| Where | Difference | Why |
|---|---|---|
| text replies | the Neo4j reply appends `graph_backend: neo4j -- …` | source naming; only `structured` is compared |
| `query_graph` | a missing property renders `""` in the engine and `"null"` through the native fallback (B03/A31 in 2026-11) | documented; the fallback only triggers when the engine cannot finish, which the small fixture never causes |
| `query_graph` | cursor tokens | opaque, stripped |
| `index_status` | `graph_backend` block | stripped |

Anything else that differs is logged in `results/parity/<file>.md` with the first
differing paths and is a finding.

## Also compare across builds

The same tool compares two runs of the same backend (golden vs new build):

```
python3 evals/harness/compare_backends.py results-golden/neo4j/30_query_graph.jsonl results/neo4j/30_query_graph.jsonl
```

That is the regression gate for a PR touching `src/neo4j/`, `src/store/` or
`src/cypher/`.
