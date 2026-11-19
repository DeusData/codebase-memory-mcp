# 07 Searching: query_graph (engine) and native Neo4j Cypher

Case files: `cases/common/30_query_graph.json` (both backends),
`cases/neo4j/N06_native_query.json` (Neo4j).

## The engine (openCypher read subset)

The in-process engine parses the query and evaluates it over the store (SQLite,
or Neo4j through the same store interface). Anything outside the subset fails with
an `unsupported …` error instead of returning empty results.

### Correctness on known truth

| Id | Query | Pass rule | Tier |
|---|---|---|---|
| QG-01 | `count(n)` | ≥ 60, `total_relation eq`, under 10 s | 1 |
| QG-02/03 | hub callers | 12; `caller_01 … caller_12` in order | 1 |
| QG-04 | dead code (`NOT EXISTS` both directions) | includes `unused_helper`; excludes hub, callers, `format_cents` | 1 |
| QG-05/06 | `checkout` callees; 2 hops | validate, notify; + `format_cents` (tier 2) | 1 / 2 |
| QG-07/08 | Java `issue→audit`, `format→roundCents` | | 1 |
| QG-09 | `(n:Class\|Interface)` | ≥ 8 | 1 |
| QG-10/11 | no File under `vendor/`/`generated/`; 14 ≤ File ≤ 30 | | 1 |
| QG-12/13 | `DEFINES_METHOD`, `DEFINES` | | 1 / 2 |
| QG-14 | `IMPORTS` exist | | 2 |
| QG-15 | aggregation by `n.label` | | 1 |
| QG-16 | `=~ 'caller_0[1-9]'` | 9 | 1 |
| QG-17 | `toLower … STARTS WITH` | Invoice, InvoiceService, InvoiceServiceTest | 1 |
| QG-18 | `OPTIONAL MATCH` + `count` 0 | | 1 |
| QG-19..22 | `UNION`, `ORDER BY/SKIP/LIMIT`, `WITH … WHERE`, `CASE` | | 1 |
| QG-23 | virtual `in_degree` | hub | 2 |
| QG-24/25 | duplicate names → two files; unicode round-trip | | 1 |
| QG-26 | test caller resolved | | 2 |

### Unsupported syntax and writes

| Id | Query | Pass rule | Tier |
|---|---|---|---|
| QG-27 | list literal in RETURN | clear `unsupported`/parse error | 1 |
| QG-28/29/30 | `CREATE`, `SET`, `CALL` | refused by the parser | 1 |
| QG-31 | syntax error | clear error | 1 |
| QG-32 | `(:Probe)` count | 0: nothing was written | 1 |

### Paging

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| QG-33/34 | `max_rows 5`, cursor | 5 rows, `has_more`, `next_cursor`; the cursor continues with the same total | 1 |
| QG-35 | `paging_check.py` over every node by cursor (page 7) | complete and disjoint | 1 |
| QG-36/37 | `offset 5`; offset + cursor | works; refused together | 1 |
| QG-38 | `max_rows 0` | legacy maximum, exact total = QG-01's count | 1 |
| QG-39 | `graph: missed` | runs | 1 |
| QG-40/41 | tree format; served_by neo4j | | 1 |
| QG-42 | 3-hop var-length from every node | finishes under 60 s on the small graph | 2 |

## Native Cypher on Neo4j (N06, `native: true`)

The query is sent to Neo4j as is, read-only, with `$project`, `$db`, `$gen` bound
and a 60 s server-side execution limit.

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| N06-01/02 | native count scoped by the three parameters | equals the engine count | 1 |
| N06-03/04 | `$project`, `$db`, `$gen` | project name, project name, positive integer | 1 |
| N06-05 | hub callers | 12 | 1 |
| N06-06 | `served_by neo4j` | | 1 |
| N06-07..15 | `CREATE`, `MERGE`, `SET`, `DETACH DELETE`, `REMOVE`, `CALL`, `LOAD CSV`, comment-smuggled `CREATE` after `EXPLAIN`, `DROP INDEX` | all refused | 1 |
| N06-16/17/18 | probes | no `EvalProbe` node, no `evalprobe` property, `cbm_node_name` still exists | 1 |
| N06-19 | engine query with `--(COUNT { … other db … })` | refused (the fallback rewrite must not be desynced by a `--` comment) | 1 |
| N06-20 | engine query with an inline `{db: '<other>'}` | error or 0 rows | 1 |
| N06-21/22 | engine `/* ' */ DETACH DELETE` | parse error; graph intact | 1 |
| N06-23 | native read of another project's db | **recorded, not judged**: native Cypher is raw by design. Decide the policy (scope rewrite for native too?) and promote to tier 1 when decided | 2 |
| N06-24 | `max_rows 10` of 1,000 | 10 returned | 1 |
| N06-25 | syntax error | clear Neo4j error | 1 |
| N06-26 | native on a SQLite project | refused, names Neo4j | 1 |
| N06-27 | missing property | `''`/null, no crash | 1 |
| N06-28 | four-way cross product | stopped by the server limit or finished; the call returns within 130 s either way | 2 |

## Engine on Neo4j: when the engine cannot finish

For a Neo4j project the engine may hand a query it cannot finish in time to Neo4j
through a scope-rewritten native query (PR #10). The 2026-11 run found and fixed:
cross-project reads through a `--` comment desync (K01/K09), writes through the
fallback (K02), `EXPLAIN` guard bypass (C30), `::missed` included in the fallback
scope (X03), and regex dialect change (X04). N06-19..22 and N02-25 keep the first
four closed; the regex dialect difference (POSIX classes such as `[[:digit:]]`)
remains a documented difference to re-check on `eval-medium`:
`MATCH (m:Method) WHERE m.name =~ 'dead[[:digit:]]+' RETURN count(m)` must give 200
on both the engine and whatever path answers it, or warn.
