---
name: codebase-memory-reference
description: >
  This skill should be used when the user asks about "codebase-memory-mcp tools",
  "graph query syntax", "Cypher query examples", "edge types",
  "how to use search_graph", "query_graph examples", or needs reference
  documentation for the codebase knowledge graph tools.
---

# Codebase Memory MCP — Tool Reference

## Tools (15 total)

| Tool | Purpose |
|------|---------|
| `index_repository` | Parse and ingest repo into graph (only once — auto-sync keeps it fresh) |
| `index_status` | Check indexing status (ready/indexing/not found) |
| `list_projects` | List all indexed projects with timestamps and counts |
| `delete_project` | Remove a project from the graph |
| `search_graph` | Structured search with filters (name, label, degree, file pattern). Supports `mode=summary` for aggregate counts, `compact=true` to reduce tokens. |
| `search_code` | Grep-like text search within indexed project files |
| `trace_call_path` | BFS call chain traversal (exact name match required). Supports `risk_labels=true`, `compact=true`, `max_results`. |
| `detect_changes` | Map git diff to affected symbols + blast radius with risk scoring |
| `query_graph` | Cypher-like graph queries. Output capped at `max_output_bytes` (default 32KB). |
| `get_graph_schema` | Node/edge counts, relationship patterns |
| `get_code_snippet` | Read source code by qualified name. Supports `mode=signature` (API only) and `mode=head_tail` (preserve start+end). |
| `index_dependencies` | Index dependency/library source into separate `_deps.db`. Use `include_dependencies=true` on query tools to include. |
| `ingest_traces` | Ingest OpenTelemetry traces to validate HTTP_CALLS edges |

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

## Regex-Powered Search (No Full-Text Index Needed)

`search_graph` and `search_code` support full Go regex, making full-text search indexes unnecessary. Regex patterns provide precise, composable queries that cover all common discovery scenarios:

### search_graph — name_pattern / qn_pattern

| Pattern | Matches | Use case |
|---------|---------|----------|
| `.*Handler$` | names ending in Handler | Find all handlers |
| `(?i)auth` | case-insensitive "auth" | Find auth-related symbols |
| `get\|fetch\|load` | any of three words | Find data-loading functions |
| `^on[A-Z]` | names starting with on + uppercase | Find event handlers |
| `.*Service.*Impl` | Service...Impl pattern | Find service implementations |
| `^(Get\|Set\|Delete)` | CRUD prefixes | Find CRUD operations |
| `.*_test$` | names ending in _test | Find test functions |
| `.*\\.controllers\\..*` | qn_pattern for directory scoping | Scope to controllers dir |

### search_code — regex=true

| Pattern | Matches | Use case |
|---------|---------|----------|
| `TODO\|FIXME\|HACK` | multi-pattern scan | Find tech debt markers |
| `(?i)password\|secret\|token` | case-insensitive secrets | Security scan |
| `func\\s+Test` | Go test functions | Find test entry points |
| `api[._/]v[0-9]` | API version references | Find versioned API usage |
| `import.*from ['"]@` | scoped npm imports | Find package imports |

### Combining Filters for Surgical Queries

```
# Find unused auth handlers
search_graph(name_pattern="(?i).*auth.*handler.*", max_degree=0, exclude_entry_points=true)

# Find high fan-out functions in the services directory
search_graph(qn_pattern=".*\\.services\\..*", min_degree=10, relationship="CALLS", direction="outbound")

# Find all route handlers matching a URL pattern
search_code(pattern="(?i)(POST|PUT).*\\/api\\/v[0-9]\\/orders", regex=true)
```

## Token Reduction Parameters

These parameters reduce response size (tokens) without affecting indexed data:

| Parameter | Tool | Effect |
|-----------|------|--------|
| `mode="summary"` | `search_graph` | Return aggregate counts by label/file instead of individual results (~99% reduction) |
| `mode="signature"` | `get_code_snippet` | Return only function signature, params, return type (~99% reduction) |
| `mode="head_tail"` | `get_code_snippet` | Return first 60% + last 40% of lines, preserving signature and return/cleanup |
| `compact=true` | `search_graph`, `trace_call_path` | Omit `name` field when redundant with `qualified_name` (~15-25% reduction) |
| `max_lines=N` | `get_code_snippet` | Cap source lines (default 200, set 0 for unlimited) |
| `max_output_bytes=N` | `query_graph` | Cap response bytes (default 32KB, set 0 for unlimited) |
| `max_results=N` | `trace_call_path` | Cap BFS results per direction (default 25) |
| `include_dependencies=true` | `search_graph` | Include dependency symbols (marked with `source:dependency`) |

All defaults are configurable via `codebase-memory-mcp config set <key> <value>`:
`search_limit`, `snippet_max_lines`, `trace_max_results`, `query_max_output_bytes`.

## Critical Pitfalls

1. **`search_graph(relationship="HTTP_CALLS")` does NOT return edges** — it filters nodes by degree. Use `query_graph` with Cypher to see actual edges.
2. **`query_graph` output is capped at 32KB by default** — add LIMIT to your Cypher query or set `max_output_bytes=0` for unlimited.
3. **`trace_call_path` needs exact names** — use `search_graph(name_pattern=".*Partial.*")` first to discover names.
4. **`direction="outbound"` misses cross-service callers** — use `direction="both"` for full context.
5. **`search_graph` defaults to 50 results** — use `limit` parameter for more, or `mode=summary` to see total counts first.

## Decision Matrix

| Question | Use |
|----------|-----|
| Who calls X? | `trace_call_path(direction="inbound")` |
| What does X call? | `trace_call_path(direction="outbound")` |
| Full call context | `trace_call_path(direction="both")` |
| Find by name pattern | `search_graph(name_pattern="...")` |
| Dead code | `search_graph(max_degree=0, exclude_entry_points=true)` |
| Cross-service edges | `query_graph` with Cypher |
| Impact of local changes | `detect_changes()` |
| Risk-classified trace | `trace_call_path(risk_labels=true)` |
| Text search | `search_code` or Grep |
| Quick codebase overview | `search_graph(mode="summary")` |
| Function API only | `get_code_snippet(mode="signature")` |
| Large function safely | `get_code_snippet(mode="head_tail")` |
| Search library APIs | `search_graph(include_dependencies=true)` |
| Index library source | `index_dependencies(project=..., package_manager=...)` |
