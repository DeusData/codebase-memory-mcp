---
name: codebase-memory-reference
description: >
  This skill should be used when the user asks about "codebase-memory-mcp tools",
  "graph query syntax", "Cypher query examples", "edge types",
  "how to use search_graph", "query_graph examples", or needs reference
  documentation for the codebase knowledge graph tools.
---

# Codebase Memory MCP — Tool Reference

## Tools (12 total)

| Tool | Purpose |
|------|---------|
| `index_repository` | Parse and ingest repo into graph (only once — auto-sync keeps it fresh) |
| `index_status` | Check indexing status (ready/indexing/not found) |
| `list_projects` | List all indexed projects with timestamps and counts |
| `delete_project` | Remove a project from the graph |
| `search_graph` | Structured search with filters (name, label, degree, file pattern) |
| `search_code` | Grep-like text search within indexed project files |
| `trace_call_path` | BFS call chain traversal (exact name match required) |
| `query_graph` | Cypher-like graph queries (200-row cap) |
| `get_graph_schema` | Node/edge counts, relationship patterns |
| `get_code_snippet` | Read source code by qualified name |
| `read_file` | Read any file from indexed project |
| `list_directory` | List files/directories with glob filter |

## Edge Types

| Type | Meaning |
|------|---------|
| `CALLS` | Direct function call within same service |
| `HTTP_CALLS` | Synchronous cross-service HTTP request |
| `ASYNC_CALLS` | Async dispatch (Cloud Tasks, Pub/Sub, SQS, Kafka) |
| `IMPORTS` | Module/package import |
| `DEFINES` / `DEFINES_METHOD` | Module/class defines a function/method |
| `HANDLES` | Route node handled by a function |
| `IMPLEMENTS` | Type implements an interface |
| `OVERRIDE` | Struct method overrides an interface method |
| `USAGE` | Read reference (callback, variable assignment) |
| `FILE_CHANGES_WITH` | Git history change coupling |
| `CONTAINS_FILE` / `CONTAINS_FOLDER` / `CONTAINS_PACKAGE` | Structural containment |

## Node Labels

`Project`, `Package`, `Folder`, `File`, `Module`, `Class`, `Function`, `Method`, `Interface`, `Enum`, `Type`, `Route`

## Qualified Name Format

`<project>.<path_parts>.<name>` — file path with `/` replaced by `.`, extension removed.

Examples:
- `myproject.cmd.server.main.HandleRequest` (Go)
- `myproject.services.orders.ProcessOrder` (Python)
- `myproject.src.components.App.App` (TypeScript)

Use `search_graph` to discover qualified names, then pass them to `get_code_snippet`.

## Cypher Subset (for query_graph)

**Supported:**
- `MATCH` with node labels and relationship types
- Variable-length paths: `-[:CALLS*1..3]->`
- `WHERE` with `=`, `<>`, `>`, `<`, `>=`, `<=`, `=~` (regex), `CONTAINS`, `STARTS WITH`
- `WHERE` with `AND`, `OR`, `NOT`
- `RETURN` with property access, `COUNT(x)`, `DISTINCT`
- `ORDER BY` with `ASC`/`DESC`
- `LIMIT`
- Edge property access: `r.confidence`, `r.url_path`, `r.coupling_score`

**Not supported:** `WITH`, `COLLECT`, `SUM`, `CREATE/DELETE/SET`, `OPTIONAL MATCH`, `UNION`

## Common Cypher Patterns

```
# Cross-service HTTP calls with confidence
MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path, r.confidence LIMIT 20

# Filter by URL path
MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.url_path CONTAINS '/orders' RETURN a.name, b.name

# Interface implementations
MATCH (s)-[r:OVERRIDE]->(i) RETURN s.name, i.name LIMIT 20

# Change coupling
MATCH (a)-[r:FILE_CHANGES_WITH]->(b) WHERE r.coupling_score >= 0.5 RETURN a.name, b.name, r.coupling_score

# Functions calling a specific function
MATCH (f:Function)-[:CALLS]->(g:Function) WHERE g.name = 'ProcessOrder' RETURN f.name LIMIT 20
```

## Critical Pitfalls

1. **`search_graph(relationship="HTTP_CALLS")` does NOT return edges** — it filters nodes by degree. Use `query_graph` with Cypher to see actual edges.
2. **`query_graph` has a 200-row cap** before aggregation — COUNT queries silently undercount on large codebases. Use `search_graph` with `min_degree`/`max_degree` for counting.
3. **`trace_call_path` needs exact names** — use `search_graph(name_pattern=".*Partial.*")` first to discover names.
4. **`direction="outbound"` misses cross-service callers** — use `direction="both"` for full context.

## Decision Matrix

| Question | Use |
|----------|-----|
| Who calls X? | `trace_call_path(direction="inbound")` |
| What does X call? | `trace_call_path(direction="outbound")` |
| Full call context | `trace_call_path(direction="both")` |
| Find by name pattern | `search_graph(name_pattern="...")` |
| Dead code | `search_graph(max_degree=0, exclude_entry_points=true)` |
| Cross-service edges | `query_graph` with Cypher |
| Text search | `search_code` or Grep |
