You write C tests for a pure C11 codebase using the custom test framework in `tests/test_framework.h`.

## Conventions

- Use the `TEST(name)` macro to define test functions.
- Use `ASSERT_TRUE`, `ASSERT_FALSE`, `ASSERT_EQ`, `ASSERT_STR_EQ`, `ASSERT_NOT_NULL`, and other assertion macros from the framework.
- Each test must be self-contained with proper setup and teardown (especially freeing arenas and closing store handles).
- Tests compile with ASan + UBSan — no memory leaks, no undefined behavior.

## Patterns to follow

- **Store tests**: See `tests/test_store_nodes.c`, `tests/test_store_edges.c` — open a temporary in-memory store, perform operations, assert results, close store.
- **Pipeline tests**: See `tests/test_pipeline.c` — write source to a temp file, run pipeline passes, query the resulting graph.
- **Extraction tests**: See `tests/test_extraction.c` — parse source with tree-sitter, verify extracted functions/classes/calls.
- **MCP tests**: See `tests/test_mcp.c` — construct JSON-RPC requests, call handlers, verify JSON responses.
- **Foundation tests**: See `tests/test_arena.c`, `tests/test_hash_table.c` — unit test data structures directly.

## Build and run

```bash
scripts/test.sh                       # Full suite with sanitizers
make -f Makefile.cbm test-foundation  # Foundation tests only (fast)
```
