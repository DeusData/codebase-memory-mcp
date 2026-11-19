# Eval scorecard

| suite | backend | binary | pass | fail | tier-1 fail | skip | file |
|---|---|---|---|---|---|---|---|
| C-STATUS | neo4j | codebase-memory-mcp-neo4j | 12 | 0 | 0 | 1 | evals/results/neo4j/00_schema_and_status.jsonl |
| C-SG | neo4j | codebase-memory-mcp-neo4j | 39 | 0 | 0 | 1 | evals/results/neo4j/10_search_graph_structural.jsonl |
| C-SG-SEM | neo4j | codebase-memory-mcp-neo4j | 11 | 0 | 0 | 0 | evals/results/neo4j/11_search_graph_bm25_semantic.jsonl |
| C-SG-PAGE | neo4j | codebase-memory-mcp-neo4j | 11 | 0 | 0 | 0 | evals/results/neo4j/12_search_graph_paging.jsonl |
| C-SC | neo4j | codebase-memory-mcp-neo4j | 26 | 0 | 0 | 0 | evals/results/neo4j/20_search_code.jsonl |
| C-QG | neo4j | codebase-memory-mcp-neo4j | 43 | 0 | 0 | 0 | evals/results/neo4j/30_query_graph.jsonl |
| C-TP | neo4j | codebase-memory-mcp-neo4j | 25 | 0 | 0 | 0 | evals/results/neo4j/40_trace_path.jsonl |
| C-SNIP | neo4j | codebase-memory-mcp-neo4j | 25 | 0 | 0 | 0 | evals/results/neo4j/50_snippets_outline.jsonl |
| C-COV | neo4j | codebase-memory-mcp-neo4j | 37 | 0 | 0 | 0 | evals/results/neo4j/60_coverage.jsonl |
| C-DC | neo4j | codebase-memory-mcp-neo4j | 20 | 0 | 0 | 0 | evals/results/neo4j/70_detect_changes.jsonl |
| C-ARCH | neo4j | codebase-memory-mcp-neo4j | 19 | 0 | 0 | 0 | evals/results/neo4j/80_architecture.jsonl |
| N-IDX | neo4j | codebase-memory-mcp-neo4j | 26 | 0 | 0 | 0 | evals/results/neo4j/N01_index_create.jsonl |
| N-SCHEMA-IDX | neo4j | codebase-memory-mcp-neo4j | 40 | 0 | 0 | 0 | evals/results/neo4j/N02_neo4j_indexes.jsonl |
| N-DELTA | neo4j | codebase-memory-mcp-neo4j | 47 | 0 | 0 | 0 | evals/results/neo4j/N03_delta_sync.jsonl |
| N-GEN | neo4j | codebase-memory-mcp-neo4j | 30 | 0 | 0 | 0 | evals/results/neo4j/N04_generations.jsonl |
| N-DOWN | neo4j | codebase-memory-mcp-neo4j | 26 | 0 | 0 | 0 | evals/results/neo4j/N05_unavailable.jsonl |
| N-NATIVE | neo4j | codebase-memory-mcp-neo4j | 30 | 0 | 0 | 0 | evals/results/neo4j/N06_native_query.jsonl |
| N-SEC | neo4j | codebase-memory-mcp-neo4j | 20 | 0 | 0 | 0 | evals/results/neo4j/N07_security.jsonl |
| N-PARITY | neo4j | codebase-memory-mcp-neo4j | 12 | 1 | 0 | 0 | evals/results/neo4j/N08_parity.jsonl |
| N-SCALE | neo4j | codebase-memory-mcp-neo4j | 37 | 0 | 0 | 0 | evals/results/neo4j/N09_scale.jsonl |
| N-CLEAN | neo4j | codebase-memory-mcp-neo4j | 8 | 0 | 0 | 0 | evals/results/neo4j/N99_cleanup.jsonl |
| S-FTS | neo4j | codebase-memory-mcp-neo4j | 15 | 0 | 0 | 0 | evals/results/neo4j/S03_fts_bm25.jsonl |
| C-STATUS | sqlite | codebase-memory-mcp-neo4j | 12 | 0 | 0 | 1 | evals/results/sqlite/00_schema_and_status.jsonl |
| C-SG | sqlite | codebase-memory-mcp-neo4j | 39 | 0 | 0 | 1 | evals/results/sqlite/10_search_graph_structural.jsonl |
| C-SG-SEM | sqlite | codebase-memory-mcp-neo4j | 10 | 0 | 0 | 1 | evals/results/sqlite/11_search_graph_bm25_semantic.jsonl |
| C-SG-PAGE | sqlite | codebase-memory-mcp-neo4j | 11 | 0 | 0 | 0 | evals/results/sqlite/12_search_graph_paging.jsonl |
| C-SC | sqlite | codebase-memory-mcp-neo4j | 25 | 0 | 0 | 1 | evals/results/sqlite/20_search_code.jsonl |
| C-QG | sqlite | codebase-memory-mcp-neo4j | 42 | 0 | 0 | 1 | evals/results/sqlite/30_query_graph.jsonl |
| C-TP | sqlite | codebase-memory-mcp-neo4j | 24 | 0 | 0 | 1 | evals/results/sqlite/40_trace_path.jsonl |
| C-SNIP | sqlite | codebase-memory-mcp-neo4j | 24 | 0 | 0 | 1 | evals/results/sqlite/50_snippets_outline.jsonl |
| C-COV | sqlite | codebase-memory-mcp-neo4j | 37 | 0 | 0 | 0 | evals/results/sqlite/60_coverage.jsonl |
| C-DC | sqlite | codebase-memory-mcp-neo4j | 19 | 0 | 0 | 1 | evals/results/sqlite/70_detect_changes.jsonl |
| C-ARCH | sqlite | codebase-memory-mcp-neo4j | 18 | 0 | 0 | 1 | evals/results/sqlite/80_architecture.jsonl |
| S-IDX | sqlite | codebase-memory-mcp-neo4j | 23 | 0 | 0 | 0 | evals/results/sqlite/S01_index_create.jsonl |
| S-SCHEMA | sqlite | codebase-memory-mcp-neo4j | 24 | 0 | 0 | 0 | evals/results/sqlite/S02_sqlite_schema.jsonl |
| S-FTS | sqlite | codebase-memory-mcp-neo4j | 14 | 0 | 0 | 1 | evals/results/sqlite/S03_fts_bm25.jsonl |
| S-CLEAN | sqlite | codebase-memory-mcp-neo4j | 6 | 0 | 0 | 0 | evals/results/sqlite/S99_cleanup.jsonl |

**Total: 887 pass, 1 fail (0 tier-1), 12 skipped**

## C-STATUS on neo4j (evals/results/neo4j/00_schema_and_status.jsonl)


Slowest tool calls: C00-02 122 ms, C00-08 111 ms, C00-06 77 ms, C00-01 45 ms, C00-05 44 ms

## C-SG on neo4j (evals/results/neo4j/10_search_graph_structural.jsonl)


Slowest tool calls: SG-04 246 ms, SG-26 224 ms, SG-33 209 ms, SG-19 205 ms, SG-28 203 ms

## C-SG-SEM on neo4j (evals/results/neo4j/11_search_graph_bm25_semantic.jsonl)


Slowest tool calls: SEM-09 215 ms, SEM-06 124 ms, SEM-05 27 ms, SEM-01 26 ms, SEM-08 24 ms

## C-SG-PAGE on neo4j (evals/results/neo4j/12_search_graph_paging.jsonl)


Slowest tool calls: PG-03 35 ms, PG-11 30 ms, PG-09 28 ms, PG-01 26 ms, PG-02 25 ms

## C-SC on neo4j (evals/results/neo4j/20_search_code.jsonl)


Slowest tool calls: SC-01 119 ms, SC-02 87 ms, SC-20 37 ms, SC-04 29 ms, SC-18 28 ms

## C-QG on neo4j (evals/results/neo4j/30_query_graph.jsonl)


Slowest tool calls: QG-42 569 ms, QG-04 188 ms, QG-06 143 ms, QG-14 108 ms, QG-05 76 ms

## C-TP on neo4j (evals/results/neo4j/40_trace_path.jsonl)


Slowest tool calls: TP-17 107 ms, TP-18 85 ms, TP-01 53 ms, TP-16 52 ms, TP-05 51 ms

## C-SNIP on neo4j (evals/results/neo4j/50_snippets_outline.jsonl)


Slowest tool calls: CS-10 102 ms, CS-12 99 ms, CS-07 71 ms, CS-02 65 ms, CS-05 44 ms

## C-COV on neo4j (evals/results/neo4j/60_coverage.jsonl)


Slowest tool calls: COV-38 595 ms, COV-41 382 ms, COV-46 244 ms, COV-16 84 ms, COV-13 74 ms

## C-DC on neo4j (evals/results/neo4j/70_detect_changes.jsonl)


Slowest tool calls: DC-06 455 ms, DC-14 451 ms, DC-11 266 ms, DC-05v 108 ms, DC-04 102 ms

## C-ARCH on neo4j (evals/results/neo4j/80_architecture.jsonl)


Slowest tool calls: AR-04 26 ms, AR-01 25 ms, AR-03 24 ms, AR-02 22 ms, AR-05 20 ms

## N-IDX on neo4j (evals/results/neo4j/N01_index_create.jsonl)


Slowest tool calls: N01-01 4356 ms, N01-17 758 ms, N01-25 567 ms, N01-15 353 ms, N01-22 334 ms

## N-SCHEMA-IDX on neo4j (evals/results/neo4j/N02_neo4j_indexes.jsonl)


Slowest tool calls: N02-29 2123 ms, N02-30b 908 ms, N02-30e 507 ms, N02-27 206 ms, N02-33 172 ms

## N-DELTA on neo4j (evals/results/neo4j/N03_delta_sync.jsonl)


Slowest tool calls: N03-31 1104 ms, N03-38 506 ms, N03-42 434 ms, N03-17 410 ms, N03-23 402 ms

## N-GEN on neo4j (evals/results/neo4j/N04_generations.jsonl)


Slowest tool calls: N04-19c 538 ms, N04-03 507 ms, N04-19f 390 ms, N04-17 374 ms, N04-07b 369 ms

## N-DOWN on neo4j (evals/results/neo4j/N05_unavailable.jsonl)


Slowest tool calls: N05-19 2084 ms, N05-21 564 ms, N05-13 274 ms, N05-26 257 ms, N05-22 223 ms

## N-NATIVE on neo4j (evals/results/neo4j/N06_native_query.jsonl)


Slowest tool calls: N06-28 61255 ms, N06-25b 356 ms, N06-05 45 ms, N06-27 42 ms, N06-25 38 ms

## N-SEC on neo4j (evals/results/neo4j/N07_security.jsonl)


Slowest tool calls: N07-18 511 ms, N07-06 212 ms, N07-02 112 ms, N07-13 61 ms, N07-01 42 ms

## N-PARITY on neo4j (evals/results/neo4j/N08_parity.jsonl)

| id | tier | title | why | ms |
|---|---|---|---|---|
| N08-03 | 2 | 11 search_graph bm25 + semantic (ranked rows compared as sets) (tier 2: semantic vectors include the project name, which differs between the twins) | rc 1 != 0: ; stdout lacks '0 differing step(s)' | 78 |

## N-SCALE on neo4j (evals/results/neo4j/N09_scale.jsonl)


Slowest tool calls: N09-01 22525 ms, N09-23 8710 ms, N09-17 2143 ms, N09-18 1190 ms, N09-12 887 ms

## N-CLEAN on neo4j (evals/results/neo4j/N99_cleanup.jsonl)


Slowest tool calls: N99-02 9629 ms, N99-01 308 ms, N99-03 33 ms, N99-08 16 ms

## S-FTS on neo4j (evals/results/neo4j/S03_fts_bm25.jsonl)


Slowest tool calls: S03-01 19 ms, S03-12 19 ms, S03-14 18 ms, S03-04 15 ms, S03-06 15 ms

## C-STATUS on sqlite (evals/results/sqlite/00_schema_and_status.jsonl)


Slowest tool calls: C00-02 120 ms, C00-01 41 ms, C00-05 24 ms, C00-09 18 ms, C00-08 13 ms

## C-SG on sqlite (evals/results/sqlite/10_search_graph_structural.jsonl)


Slowest tool calls: SG-07b 19 ms, SG-21 17 ms, SG-07e 15 ms, SG-01 14 ms, SG-02 13 ms

## C-SG-SEM on sqlite (evals/results/sqlite/11_search_graph_bm25_semantic.jsonl)


Slowest tool calls: SEM-01 15 ms, SEM-04 12 ms, SEM-06 12 ms, SEM-09 11 ms, SEM-02 10 ms

## C-SG-PAGE on sqlite (evals/results/sqlite/12_search_graph_paging.jsonl)


Slowest tool calls: PG-01 14 ms, PG-06 13 ms, PG-11 13 ms, PG-10 12 ms, PG-02 11 ms

## C-SC on sqlite (evals/results/sqlite/20_search_code.jsonl)


Slowest tool calls: SC-16 26 ms, SC-01 15 ms, SC-22 14 ms, SC-03 13 ms, SC-05 12 ms

## C-QG on sqlite (evals/results/sqlite/30_query_graph.jsonl)


Slowest tool calls: QG-42 19 ms, QG-25 18 ms, QG-08 15 ms, QG-38 15 ms, QG-01 14 ms

## C-TP on sqlite (evals/results/sqlite/40_trace_path.jsonl)


Slowest tool calls: TP-20 16 ms, TP-01 13 ms, TP-12 13 ms, TP-19 13 ms, TP-23 13 ms

## C-SNIP on sqlite (evals/results/sqlite/50_snippets_outline.jsonl)


Slowest tool calls: CS-01 15 ms, CS-06 12 ms, CS-11 12 ms, CS-21 12 ms, CS-27 12 ms

## C-COV on sqlite (evals/results/sqlite/60_coverage.jsonl)


Slowest tool calls: COV-38 356 ms, COV-41 341 ms, COV-46 218 ms, COV-01 14 ms, COV-10 12 ms

## C-DC on sqlite (evals/results/sqlite/70_detect_changes.jsonl)


Slowest tool calls: DC-06 366 ms, DC-14 362 ms, DC-11 264 ms, DC-04 87 ms, DC-05v 87 ms

## C-ARCH on sqlite (evals/results/sqlite/80_architecture.jsonl)


Slowest tool calls: AR-01 15 ms, AR-16 15 ms, AR-18 15 ms, AR-06 13 ms, AR-13 13 ms

## S-IDX on sqlite (evals/results/sqlite/S01_index_create.jsonl)


Slowest tool calls: S01-01 391 ms, S01-22 315 ms, S01-08 306 ms, S01-18 305 ms, S01-06 245 ms

## S-FTS on sqlite (evals/results/sqlite/S03_fts_bm25.jsonl)


Slowest tool calls: S03-01 14 ms, S03-03 12 ms, S03-05 12 ms, S03-10 12 ms, S03-12 12 ms

## S-CLEAN on sqlite (evals/results/sqlite/S99_cleanup.jsonl)


Slowest tool calls: S99-04 21 ms, S99-05 14 ms, S99-01 13 ms, S99-03 11 ms, S99-02 10 ms
