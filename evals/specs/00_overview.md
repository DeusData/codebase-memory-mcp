# Evals: overview

These evals measure whether `codebase-memory-mcp-neo4j` builds a complete, correct
index of a repository and answers graph, text and semantic searches correctly, on
**both** storage backends:

| Backend | Where graph reads come from | Profile |
|---|---|---|
| SQLite | the local `<cache>/<project>.db` (nodes, edges, FTS5) | `profiles/sqlite.json` |
| Neo4j | the project's mirrored generation in Neo4j, via the HTTP API; BM25, semantic, ADR, coverage and architecture stay local | `profiles/neo4j.json` |

Every spec in this folder is a list of numbered evals. Each eval names the case
file and step ids that encode it (`cases/<backend>/<file>.json`, run by
`harness/run_cases.py`), the preconditions, the exact expectation and the
pass rule. A spec is the source of truth; a case file is its executable form.

## Scope map

| Area the user asked for | Spec | Case files |
|---|---|---|
| Index creation, SQLite | [01_indexing_sqlite.md](01_indexing_sqlite.md) | `sqlite/S01`, `sqlite/S02`, `sqlite/S99` |
| Index creation, Neo4j (sync, delta, generations) | [02_indexing_neo4j.md](02_indexing_neo4j.md) | `neo4j/N01`, `neo4j/N03`, `neo4j/N04`, `neo4j/N99` |
| **The Neo4j indexes** (schema indexes and constraint: existence, shape, state, plans, resilience) | [03_neo4j_indexes.md](03_neo4j_indexes.md) | `neo4j/N02` |
| **Index-creation coverage** (files, symbols, edges, docs, infra, ignore rules, freshness, limits) | [04_index_coverage.md](04_index_coverage.md) | `common/00`, `common/60` |
| **Searching**: search_graph structural, BM25, semantic, paging | [05_search_graph.md](05_search_graph.md) | `common/10`, `common/11`, `common/12`, `sqlite/S03` (runs on both) |
| Searching: search_code | [06_search_code.md](06_search_code.md) | `common/20` |
| Searching: query_graph engine and native Neo4j Cypher | [07_query_graph.md](07_query_graph.md) | `common/30`, `neo4j/N06` |
| trace_path, snippets, outlines, detect_changes, architecture | [08_trace_snippets_architecture.md](08_trace_snippets_architecture.md) | `common/40`, `common/50`, `common/70`, `common/80` |
| Neo4j vs SQLite parity | [09_backend_parity.md](09_backend_parity.md) | `neo4j/N08` |
| Failure handling and security | [10_failure_security.md](10_failure_security.md) | `neo4j/N05`, `neo4j/N07` |
| Scale and performance | [11_scale_performance.md](11_scale_performance.md) | `neo4j/N09` |

## Fixtures and ground truth

- **eval-small** (`fixtures/eval-small`, 28 files, Python + Java + Markdown + Docker
  Compose). Hand-verified ground truth in `fixtures/expected/eval-small.json`:
  every tier-1 file, symbol and call edge, a 12-caller hub, a dead function, a
  duplicate name in two files, a unicode file and identifier, a 200-character
  identifier, a test caller in each language, a `.cbmignore`d vendor file and a
  git-ignored generated file, and two doc-only tokens that must not become symbols.
- **eval-medium** (`fixtures/gen_medium.py`, default 20,002 Java files): a known
  call chain, a 5,000-caller hub, 200 dead methods, docstring tokens every 1,000th
  class. `EXPECTED.json` is written next to it.
- Working copies are made by `fixtures/prepare.py` (fresh git repo, one commit),
  because the evals edit files and need a diff.

## Tiers and pass rules

- **Tier 1**: must hold on every build. A tier-1 failure is a defect and fails the
  run (`run_cases.py` exit code 1).
- **Tier 2**: expected; parser-, resolver- or environment-dependent. A failure is
  reported and reviewed, not an automatic defect. Values marked "record" or
  "freeze at the pilot" are copied into the case file after the first clean run.

A step passes when every check in its `expect` block holds (`harness/assertions.py`
documents the checks). A suite passes when it has no tier-1 failures.

## Pilot and golden process

1. Run the SQLite profile first on `eval-small`. Fix the harness or the case file
   where the eval is wrong (not the product).
2. Freeze tier-2 numbers that depend on the extractor (for example which tier-2
   CALLS edges resolve) by editing the case file; note them in the scorecard.
3. Run the Neo4j profile. Every `common/` file must give the same answers
   (spec 09): a parity difference is a Neo4j-backend defect by definition.
4. Keep the result JSONL of a reviewed run as the golden run; later runs are
   compared with `harness/compare_backends.py <golden> <new>` (same backend).

## Execution order

`harness/run_all.py --profile <p>` runs the suites in this order and keeps going on
failures (tier-1 failures are counted at the end):

| SQLite profile | Neo4j profile |
|---|---|
| S01 index create | N01 index create (needs `NEO4J_PASSWORD`) |
| common/00 … common/80 | N02 Neo4j indexes |
| S03 BM25 | common/00 … common/80, S03 BM25 |
| S99 cleanup | N03 delta, N04 generations, N06 native, N07 security |
| | N05 unavailable (stops/starts the container; run last among reads) |
| | N08 parity (needs the SQLite results) |
| | N09 scale (needs `eval-medium`; optional) |
| | N99 cleanup |

## What a result looks like

`results/<backend>/<case-file>.jsonl`: one line per step with the request (password
masked), reply text, structured JSON, timing, the failed checks and `pass`.
`harness/report.py` turns a set of them into `results/SCORECARD.md` (template:
[../SCORECARD.md](../SCORECARD.md)). Nothing under `results/` is committed.

## Relation to the existing test material

- `tests/test_neo4j.c`, `tests/test_store_search.c` and the rest of the native
  suite are unit and contract tests; the evals are black-box, through MCP.
- `test-env/` holds the ad-hoc runs of November 2026 (functional, scale,
  regression). The evals reuse their harness ideas (one MCP stdio session per case
  file, fault injection by proxy, daemon RSS sampling) in a committed, repeatable
  form with a fixed fixture and ground truth.
- `docs/EVALUATION_PLAN.md` is the 159-language answer-quality plan (graph vs
  grep, LLM judge). These evals are narrower and deterministic: index and search
  correctness on a known repository.
