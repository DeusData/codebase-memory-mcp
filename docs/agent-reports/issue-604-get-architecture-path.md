# Issue #604 — `get_architecture` path scoping

## Problem

`get_architecture` accepted a `path` argument in some clients but the MCP tool schema, handler, and store layer ignored it. Responses used whole-project node/edge totals and global package lists even when requesting a subdirectory (e.g. `apps/hoa`).

## Root cause

1. **MCP** (`src/mcp/mcp.c`): `inputSchema` had only `project` and `aspects`; handler never parsed `path`.
2. **Store** (`src/store/store.c`): `cbm_store_get_architecture` had no path parameter; all `arch_*` SQL filtered by `project` only.
3. **Tests**: No regression for path-scoped architecture.

## Solution

### Store (`store.h` / `store.c`)

- Added `cbm_arch_path_scope_t` with normalization (strip `./`, trailing slashes, `cbm_normalize_path_sep`).
- Scoped queries with parameterized `file_path = ?2 OR file_path LIKE ?3` (`prefix` and `prefix/%`).
- Extended `cbm_store_get_architecture(..., const char *path, ...)`.
- Added `cbm_store_count_nodes_under_path` and `cbm_store_count_edges_under_path` for scoped totals.

### MCP (`mcp.c`)

- Documented optional `path` in `get_architecture` `inputSchema`.
- Handler passes `path` to the store and includes `path` in JSON when set.
- Response fields:
  - `root_total_nodes` / `root_total_edges` — full project
  - `scoped_total_nodes` / `scoped_total_edges` — under `path` when provided, else same as root
  - `total_nodes` / `total_edges` — aliases for scoped counts (backward-friendly primary totals)

### Tests

- `tests/test_store_arch.c`: `arch_path_scope_issue604`
- `tests/test_mcp.c`: `tool_get_architecture_path_scope_issue604`

## Verification

```bash
make -f Makefile.cbm test
```

- New regression tests: **PASS**
- Full suite: **5695 passed**, **1 failed** — `tests/test_incremental.c` RSS limit (pre-existing, unrelated to this change)

## Usage

```bash
codebase-memory-mcp cli get_architecture '{"project":"<name>","path":"apps/hoa"}'
```

Expect `scoped_total_nodes` &lt; `root_total_nodes` when the subtree is smaller than the repo, and aspect data (packages, entry points, etc.) limited to files under that prefix.