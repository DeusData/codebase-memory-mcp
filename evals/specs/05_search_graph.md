# 05 Searching: search_graph

Case files: `cases/common/10_search_graph_structural.json`,
`cases/common/11_search_graph_bm25_semantic.json`,
`cases/common/12_search_graph_paging.json`, `cases/sqlite/S03_fts_bm25.json`
(runs on both backends). All answers in `format: json`; rows are the grouped
table (`cols`, `groups[{qn_prefix, file, rows}]`) that the harness flattens to
`name, label, …, qn, file`.

## Three modes

| Mode | Argument | Where it runs | Neo4j project |
|---|---|---|---|
| Structural | `label`, `name_pattern`, `qn_pattern`, `file_pattern`, `kind`, `namespace`, `relationship`, `min/max_degree`, `exclude_entry_points`, `include_connected` | SQLite search, or Neo4j candidate narrowing + the same local regex/glob filter | `served_by: neo4j` |
| BM25 | `query` | FTS5 `nodes_fts` (name, qualified_name, label, file_path, body=docstring) | local (reply still names the backend) |
| Semantic | `semantic_query: [..]` (full/moderate index) | local embeddings | local |

`query` and `semantic_query` together are refused (SEM-07, S03-07).

## Structural (SG)

| Id | Filter | Pass rule | Tier |
|---|---|---|---|
| SG-01/02 | `^hub$` | one row, file `hub/hub.py`, under 5 s | 1 |
| SG-03 | `label Function` + `file_pattern hub/*` | exactly 13 rows (hub + 12 callers) | 1 |
| SG-04/05/06 | `label Class` / `label Method` | the 8 classes; Python and Java methods; every row has that label | 1 |
| SG-07 | `^caller_0[1-9]$` | 9 rows | 1 |
| SG-08 | `^orderservice$` | case-insensitive: finds `OrderService` | 2 |
| SG-09 | `qn_pattern app\.service\.` | every qn contains it; includes `<project>.app.service.OrderService.checkout` | 1 |
| SG-10 | `**/*.java` + Method | Java methods only | 1 |
| SG-11 | `dup/*.py` + `^shared_name$` | 2 rows, both files | 1 |
| SG-12 | `*service*` glob substring semantics (issue 200) | finds `app/service.py` symbols | 1 |
| SG-13/14/15 | `min_degree 12` keeps hub, `13` drops it, hub row shows `in = 12` | degree counts CALLS/USAGE/CALL_REFERENCE/INHERITS/IMPLEMENTS over both directions | 1 |
| SG-16 | `max_degree 0` + Function | includes `unused_helper`, excludes hub/callers/`format_cents` | 1 |
| SG-17 | `relationship CALLS` | keeps hub, drops `unused_helper` | 1 |
| SG-18 | `exclude_entry_points` | drops `caller_01` (out-CALLS, no in-CALLS), keeps hub | 1 |
| SG-19 | `include_connected` | adds neighbours | 2 |
| SG-20/21 | unicode name + file; 200-char name | | 1 / 2 |
| SG-22/23/24 | ignored files absent; test symbols present; `label File` lists indexed files and no ignored ones | | 1 |
| SG-25 | `(` | clear regex error | 1 |
| SG-26/27 | `.*` equals omitting the pattern (same total) | | 1 |
| SG-28 | unknown label | empty or clear error | 1 |
| SG-29/30/31 | `fields`, `detail=ids`, tree format | run | 2 / 2 / 1 |
| SG-32/33 | `label Resource` lists compose services; `kind Deployment` empty | | 2 |
| SG-34/35 | `served_by` neo4j with `remote_queries ≥ 1` / sqlite | | 1 |

Neo4j note: the Cypher the product sends (`k_search_where` in `neo4j_store.c`)
narrows by project, label, excluded labels, case-insensitive `CONTAINS` hints
extracted from the regex/glob, kind, namespace, relationship, entry-point rule and
degree, then the local code applies the exact regex and glob, so answers are
identical to SQLite (spec 09). SG-26/27 guards the "anchored `.*` equals omitted"
equivalence on both.

## BM25 (S03, both backends)

| Id | Query | Pass rule | Tier |
|---|---|---|---|
| S03-01 | `hyphenate` (docstring-only word) | finds `slugify`; `search_mode = bm25` | 1 |
| S03-02 | `checkout` | `checkout` and `test_checkout` | 1 |
| S03-03 | `receipt` | `notify`, `checkout` (docstrings) | 2 |
| S03-04 | nonsense | `total 0`, no error | 1 |
| S03-05/06 | + `label Function`, + `file_pattern java/**` | rows filtered | 1 |
| S03-08 | `ünïcode_fn` | unicode61 tokenizer finds it | 2 |
| S03-09 | `billing` | the Markdown section | 2 |
| S03-10/11/12 | `limit 1`, `offset 5`, total stable | | 1 |
| S03-14 | under 5 s | | 1 |
| S03-15 | on a Neo4j project the reply still says kind neo4j | | 2 |

## Semantic (SEM, needs a full/moderate index)

| Id | Query | Pass rule | Tier |
|---|---|---|---|
| SEM-01 | "turn an amount of cents into a printable dollar string" | a `format*` symbol in the top 10 | 2 |
| SEM-02 | two phrases | validate/notify/checkout among rows | 2 |
| SEM-03/04 | `semantic_limit 2`, `semantic_offset 2` | bounded; `semantic_next_offset` present | 2 |
| SEM-05/06 | + label / file filters | rows filtered | 2 |
| SEM-08/09 | `SEMANTICALLY_RELATED` edges exist; the Python/Java money helpers linked (record) | | 2 |
| SEM-10/11 | served locally on Neo4j projects; under 15 s | | 2 |

Semantic ranks are model-dependent: the pass rules only ask for the expected
symbol anywhere in the top `semantic_limit`.

## Paging (PG)

| Id | Eval | Pass rule | Tier |
|---|---|---|---|
| PG-01/02 | `limit 5` pages of Functions | 5 rows, `has_more`, exact `total ≥ 20`; page 2 has 5 distinct qns and the same total | 1 |
| PG-03 | offset past the end | empty, `has_more false` | 1 |
| PG-04/05/06 | `limit 0`, `limit 501`, `offset -1` | rejected (501: rejected or clamped) | 1 / 2 / 1 |
| PG-07/08 | `paging_check.py` over Functions (page 5) and over all nodes (page 17) | union = total, no duplicates, every page but the last full | 1 |
| PG-09 | `max_output_tokens 128` | truncation flagged or fewer rows | 2 |
| PG-10 | the same page twice | identical groups (stable `ORDER BY name, id`) | 1 |
| PG-11 | the whole small graph in one 500-row page | `has_more false`, unique qns | 1 |

## What to record

- Timing of SG-01, S03-14, SEM-11 on each backend.
- For Neo4j: `graph_backend.remote_queries` of SG-34 (expected 1 for a plain
  structural search: one Cypher round trip).
