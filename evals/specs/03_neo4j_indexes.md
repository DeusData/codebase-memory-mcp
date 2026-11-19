# 03 The Neo4j indexes

Case file: `cases/neo4j/N02_neo4j_indexes.json`. Runs after N01 against the same
Neo4j. Checks are Cypher statements sent through the HTTP API by the harness
(`harness/neo4j_http.py`), plus a few tool calls.

## What the product creates (`neo4j_sync.c`, `ensure_schema`)

Every sync runs these, idempotently (`IF NOT EXISTS`):

| Name | Kind | On | Properties | Which product query needs it |
|---|---|---|---|---|
| `cbm_project_name` | uniqueness constraint | `:CbmProject` | `name` | MERGE/lookup of the project pointer; the delta's consistency lock |
| `cbm_node_db` | RANGE index | `:CbmNode` | `db` | delete of an old generation (`{db} WHERE gen <> $gen`), project delete |
| `cbm_node_id` | RANGE index | `:CbmNode` | `db, gen, id` | batched node reads by id (`UNWIND $ids … {db,gen,id}`), edge creation (`MATCH a, b by id`), delta updates/deletes |
| `cbm_node_qn` | RANGE index | `:CbmNode` | `db, gen, qualified_name` | `get_code_snippet`, trace seeds, qualified-name lookups |
| `cbm_node_name` | RANGE index | `:CbmNode` | `db, gen, name` | short-name lookups (trace_path, snippet by name) |
| `cbm_node_file` | RANGE index | `:CbmNode` | `db, gen, file_path` | the paged full node scan (`USING INDEX … ORDER BY db, gen, file_path, id`), file outlines, file filters |
| `cbm_node_label` | RANGE index | `:CbmNode` | `db, gen, label` | label scans (`USING INDEX … label`), `list_files` (`label:'File'`), label counts, search_graph label filter |

Neo4j also keeps its own token-lookup indexes (`index_343aff4e`, …); they are not
counted.

## N02 evals

### Existence, shape, state

| Id | Check | Pass rule | Tier |
|---|---|---|---|
| N02-01 | `SHOW INDEXES … WHERE name STARTS WITH 'cbm_'` | exactly 6 rows, all `ONLINE`, type `RANGE`, label `CbmNode`; none `POPULATING`/`FAILED` | 1 |
| N02-02 | property lists | exactly the table above, in name order | 1 |
| N02-03 | `populationPercent` | 100 for all six | 1 |
| N02-04 | `SHOW CONSTRAINTS` | `cbm_project_name` is `UNIQUENESS` on `CbmProject(name)` | 1 |
| N02-05 | constraint enforced | creating a second `CbmProject` with the same name fails with "already exists" (nothing written) | 2 |

### The planner uses them (EXPLAIN)

Each statement is the shape the product sends (`neo4j_store.c`). The plan must
contain a `NodeIndexSeek` (or `NodeIndexSeekByRange`) whose details name the
index's property signature, and must not contain `NodeByLabelScan`/`AllNodesScan`.
The HTTP API returns operator names as `<Operator>@neo4j`; `plan_uses_index` is a
substring match on the plan's `Details` text (for example
`RANGE INDEX n:CbmNode(db, gen, label) WHERE …`). Verify the exact detail text at
the pilot and tighten the strings if your Neo4j version prints them differently.

| Id | Statement shape | Index | Tier |
|---|---|---|---|
| N02-07 | `{db, gen, label:'File'} WHERE n.project = $project` | `CbmNode(db, gen, label)` | 1 |
| N02-08 | `{db, gen, qualified_name}` | `CbmNode(db, gen, qualified_name)` | 1 |
| N02-09 | `UNWIND $ids … {db, gen, id}` | `CbmNode(db, gen, id)` | 1 |
| N02-10 | `{db, gen, name}` | `CbmNode(db, gen, name)` | 1 |
| N02-11 | hinted full-scan page (`USING INDEX … file_path`, keyed continuation, `ORDER BY n.db, n.gen, n.file_path, n.id LIMIT $max`) | `CbmNode(db, gen, file_path)`, no label scan | 1 |
| N02-12 | same statement | no plain `Sort`/`Top` (the index order is used: PartialTop/PartialSort at most) | 2 |
| N02-13 | hinted label page (`USING INDEX … label`, `n.id > $after_id`, `ORDER BY n.id`) | `CbmNode(db, gen, label)` | 1 |
| N02-14 | `{db} WHERE n.gen <> $gen` (old-generation delete) | `CbmNode(db` | 1 |
| N02-15 | `list_files` query (`label:'File'`, `DISTINCT path ORDER BY path`) | `CbmNode(db, gen, label)` | 1 |
| N02-16 | `PROFILE` of the label count | seek present; db hits recorded for the scorecard | 2 |

### Data shape the indexes rely on

| Id | Check | Pass rule | Tier |
|---|---|---|---|
| N02-17 | one generation | zero rows with `gen <> CbmProject.gen` | 1 |
| N02-18 | label as property **and** as Neo4j label | `count(:CbmNode:Function) = count({label:'Function'})` | 1 |
| N02-19 | `sync_format 2` | no node without `prop_keys` | 1 |
| N02-20 | flattening | `hub` row has `file_path = hub/hub.py`, `qualified_name = <project>.hub.hub.hub`, `start_line`, `properties_json` | 1 |
| N02-21 | edge properties | every `CALLS` relationship has `type = 'CALLS'` and an `id` | 1 |
| N02-22 | `CBM_EDGE` | count and the distinct real types recorded | 2 |
| N02-23 | hub | 12 inbound `CALLS` in Neo4j | 1 |
| N02-24 | no cross-db / cross-gen relationships | 0 | 1 |
| N02-25 | `::missed` graph | lives under the same `db` with its own `project` key; no other foreign rows | 1 |

### Resilience and maintenance

| Id | Check | Pass rule | Tier |
|---|---|---|---|
| N02-26..28 | `DROP INDEX cbm_node_file` by hand | a file-filtered search_graph and a File count still answer (the hinted page errors, the product retries without the hint) | 2 |
| N02-29/30 | next sync | recreates `cbm_node_file`, `ONLINE` | 2 |
| N02-31/32 | repeated syncs | still exactly six indexes and one constraint (no duplicates) | 1 |
| N02-33..35 | latency on the small graph | name lookup, qn snippet, label count each under 3 s | 1 |

### Scale (see spec 11, N09-03)

After the `eval-medium` sync every index must be `ONLINE` at 100%. While an index
is still populating (right after creation on a large first sync) reads must not
fail: the product retries a hinted page without the hint. The 2026-11 runs
measured the largest store read at 6.2 s on an 817k-node project; a read that is
still running after `N4S_READ_MAX_EXEC_MS` (60 s) is an error, not a hang.

## Manual checks worth doing at the pilot

```cypher
SHOW INDEXES YIELD name, state, populationPercent, lastRead, readCount
WHERE name STARTS WITH 'cbm_' RETURN *;
```

`readCount` must grow for `cbm_node_label`, `cbm_node_id`, `cbm_node_qn` after a
`common/` run; an index that is never read is either unused or shadowed by another
and should be questioned.
