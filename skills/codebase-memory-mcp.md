# codebase-memory-mcp — Claude Code Skill File

> **Copy this file** to `~/.claude/skills/codebase-memory-mcp/SKILL.md` to teach Claude Code how to use the graph tools effectively. Or add the contents to your project's `CLAUDE.md`.

## When to Use Graph Tools vs Direct Search

| Task | Tool | Why |
|------|------|-----|
| Find a specific file by name | `Glob` | Exact match, instant |
| Find a specific string/pattern | `Grep` | Text search, instant |
| Understand call chains, dependencies, structure | **graph tools** | Graph traversal, relationship-aware |
| Plan a refactor or migration | **graph tools** | Full structural context |
| "What calls this function?" | `trace_call_path(direction="inbound")` | Graph CALLS relationships |
| "What does X call?" | `trace_call_path(direction="outbound")` | BFS call chain traversal |
| "What implements this interface?" | `search_graph(relationship="IMPLEMENTS")` | Graph type relationships |
| Cross-service HTTP calls (list all edges) | `query_graph("MATCH (a)-[r:HTTP_CALLS]->(b) RETURN ...")` | Cypher returns edges with URL+confidence |
| Cross-service HTTP calls (for one function) | `trace_call_path(direction="both")` | Finds inbound HTTP_CALLS edges too |
| Which structs implement an interface method? | `query_graph("MATCH (s)-[r:OVERRIDE]->(i) RETURN ...")` | OVERRIDE edges link struct→interface methods |
| Read references (callbacks, stored in vars) | `query_graph("MATCH (a)-[r:USAGE]->(b) RETURN ...")` | USAGE edges (not calls) |
| Files that change together (hidden coupling) | `query_graph("MATCH (a)-[r:FILE_CHANGES_WITH]->(b) RETURN ...")` | Git history change coupling |
| Validate HTTP edges with runtime traces | `ingest_traces(project, file_path)` | Enriches edges with latency/frequency |
| Async dispatch (Cloud Tasks, Pub/Sub, SQS) | `search_graph(name_pattern=".*CreateTask.*")` then `trace_call_path` | Find dispatch functions, trace CALLS chains |
| Async dispatch (if ASYNC_CALLS edges exist) | `query_graph("MATCH (a)-[r:ASYNC_CALLS]->(b) RETURN ...")` | Only works when URLs are resolvable literals |
| Dead code detection | `search_graph(max_degree=0, exclude_entry_points=true)` | Degree-based filtering |
| REST route inventory | `search_graph(label="Route")` | Route nodes are first-class |
| Explore an unfamiliar codebase | `get_graph_schema` → `search_graph` | Structured overview |
| Quick one-off lookup in 1-2 files | `Read` / `Grep` | Faster for simple tasks |
| Text search (string literals, errors) | `search_code` or `Grep` | Graph doesn't index text content |

**Rule of thumb**: If the task requires understanding **relationships, structure, or flow** across multiple files/packages — use graph tools. If it's a simple text lookup — use Glob/Grep/Read.

## Auto-Sync: Graph Stays Fresh Automatically

After the initial `index_repository` call, a **background watcher** polls for file changes (mtime + size) and triggers incremental re-indexing automatically. You do NOT need to manually reindex after editing files.

### When to Call `index_repository`

| Situation | Action |
|-----------|--------|
| Project never indexed before | `index_repository` (required once) |
| Exploring an unfamiliar codebase | Check `list_projects` — index if missing |
| After editing files | **Nothing** — auto-sync handles it within 1–60s |
| After a large `git pull` or branch switch | Optional: `index_repository` for immediate freshness (auto-sync will catch up anyway) |
| Simple Glob/Grep lookup | Don't index — use Glob/Grep directly |

**Auto-sync details**: Polling interval adapts to repo size (1s for small repos, up to 60s for 50K+ file repos). Non-blocking — never interferes with tool queries.

## Decision Matrix

| Question | Use | NOT |
|---|---|---|
| Who calls X? | `trace_call_path(direction="inbound")` | `query_graph` |
| What does X call? | `trace_call_path(direction="outbound")` | `query_graph` |
| Full call context (callers + callees) | `trace_call_path(direction="both")` | `direction="outbound"` alone |
| Find functions by pattern | `search_graph(name_pattern="...")` | `trace_call_path` |
| Find functions, exclude noise labels | `search_graph(name_pattern="...", exclude_labels=["Route"])` | unfiltered search |
| Count callers/callees | `search_graph` with `min_degree`/`max_degree` | `query_graph COUNT` (200-row cap) |
| Dead code | `search_graph(max_degree=0, exclude_entry_points=true)` | `query_graph` |
| Cross-service HTTP calls (list edges) | `query_graph("MATCH (a)-[r:HTTP_CALLS]->(b) RETURN ...")` | `search_graph(relationship="HTTP_CALLS")` |
| Filter HTTP calls by URL path | `query_graph("... WHERE r.url_path CONTAINS 'orders' ...")` | unfiltered Cypher |
| Filter HTTP calls by confidence | `query_graph("... WHERE r.confidence >= 0.6 ...")` | unfiltered Cypher |
| Filter by confidence band | `query_graph("... WHERE r.confidence_band = 'high' ...")` | numeric threshold |
| Interface method overrides | `query_graph("MATCH (s)-[:OVERRIDE]->(i) RETURN s.name, i.name")` | manual AST inspection |
| Read refs (callbacks, stored funcs) | `query_graph("MATCH (a)-[:USAGE]->(b) RETURN a.name, b.name")` | grepping for function names |
| Files that change together | `query_graph("MATCH (a)-[r:FILE_CHANGES_WITH]->(b) WHERE r.coupling_score >= 0.5 RETURN ...")` | manual git log analysis |
| Validate HTTP edges with traces | `ingest_traces(project="myproj", file_path="/path/to/traces.json")` | manual trace inspection |
| Async dispatch functions | `search_graph(name_pattern=".*CreateTask.*")` then `trace_call_path` | `search_graph(relationship="ASYNC_CALLS")` |
| Text search | `search_code` or grep | graph tools |
| Complex multi-hop patterns | `query_graph` with Cypher + LIMIT | `search_graph` |

## Tool Reference (12 Tools)

| Tool | Purpose | When to Use |
|------|---------|-------------|
| `index_repository` | Parse and ingest repo into graph | Only needed once per project — auto-sync keeps it fresh |
| `trace_call_path` | BFS from/to a function (exact name match) | Primary tool for call chains — requires EXACT name |
| `search_graph` | Structured search with filters | Find functions, dead code, fan-out, cross-service links |
| `query_graph` | Cypher-like graph queries (200-row cap) | Complex multi-hop patterns, filtered joins, edge property filtering |
| `get_graph_schema` | Node/edge counts, relationship patterns | Understand what's indexed before querying |
| `get_code_snippet` | Read source by qualified name | After search finds a target function |
| `search_code` | Grep-like text search within project | String literals, error messages, TODO comments |
| `read_file` | Read any file from indexed project | Config files, source code |
| `list_directory` | List files/directories with glob filter | Explore project structure |
| `list_projects` | See all indexed projects | Check if reindex is needed (shows `indexed_at`) |
| `delete_project` | Remove a project from graph | Cleanup stale projects |
| `ingest_traces` | Ingest OTLP JSON traces to validate HTTP_CALLS edges | After deploying — enrich edges with p99_latency, call_count, validated_by_trace |

## Edge Types

| Type | Meaning |
|------|---------|
| `CALLS` | Direct function call within same service |
| `HTTP_CALLS` | Synchronous cross-service HTTP request |
| `ASYNC_CALLS` | Async dispatch (Cloud Tasks, Pub/Sub, SQS, SNS, Kafka, RabbitMQ) |
| `IMPORTS` | Module/package import |
| `DEFINES` | Module/class defines a function |
| `DEFINES_METHOD` | Class defines a method |
| `HANDLES` | Route node handled by a function |
| `IMPLEMENTS` | Type implements an interface |
| `OVERRIDE` | Struct method overrides an interface method (e.g., FileReader.Read → Reader.Read) |
| `USAGE` | Read reference — function passed as callback, stored in variable, not invoked |
| `FILE_CHANGES_WITH` | Git history change coupling — files that frequently change together (properties: coupling_score, co_change_count) |
| `CONTAINS_FILE` / `CONTAINS_FOLDER` / `CONTAINS_PACKAGE` | Structural containment |

## Critical Query Pitfalls

**`search_graph(relationship="HTTP_CALLS")` does NOT return edges.** It filters *nodes* by their HTTP_CALLS degree count. To see the actual cross-service links with URLs and confidence scores, use Cypher:
```
query_graph("MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path, r.confidence ORDER BY r.confidence DESC LIMIT 30")
```

**Edge property filtering works in WHERE clauses.** You can filter on any edge property:
```
# Filter by URL path substring
query_graph("MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.url_path CONTAINS 'bestellung' RETURN a.name, b.name, r.url_path")

# Filter by confidence threshold
query_graph("MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.confidence >= 0.6 RETURN a.name, b.name, r.confidence LIMIT 20")

# Filter by confidence band (high/medium/speculative)
query_graph("MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.confidence_band = 'high' RETURN a.name, b.name, r.url_path")

# Change coupling above threshold
query_graph("MATCH (a)-[r:FILE_CHANGES_WITH]->(b) WHERE r.coupling_score >= 0.5 RETURN a.name, b.name, r.coupling_score, r.co_change_count LIMIT 20")
```

**Confidence bands** on HTTP_CALLS/ASYNC_CALLS edges:
- `high` (>= 0.7): Strong structural path match
- `medium` (0.45-0.7): Good signal, current default range
- `speculative` (0.25-0.45): Fuzzy match, needs human review

**`exclude_labels` filters out noise.** When searching by `name_pattern`, Route nodes can dominate results. Use `exclude_labels` to remove them:
```
search_graph(name_pattern=".*[Ff]irestore.*", exclude_labels=["Route"])
```

**`search_graph(relationship="ASYNC_CALLS")` has the same limitation.** ASYNC_CALLS edges only exist when the URL linker can resolve dispatch URLs to route handlers. When URLs are in config variables or env vars (common for Cloud Tasks, Pub/Sub), the linker can't match them. Instead, find dispatch functions by name pattern and trace via CALLS edges:
```
search_graph(name_pattern=".*CreateTask.*|.*create_task.*")
trace_call_path(function_name="CreateMultidataTask", direction="both")
```

**`direction="outbound"` misses cross-service callers.** HTTP_CALLS edges from JS/Go frontends to Python backends appear as *inbound* edges on the backend function. Always use `direction="both"` when investigating a function's full context:
```
trace_call_path(function_name="validate_order", direction="both", depth=3)
# Finds: JS frontend → HTTP_CALLS → validate_order → logger (outbound CALLS)
```

## Key Workflow Patterns

### Discover Then Trace

`trace_call_path` requires an **exact** function name. If unsure, discover first:

```
search_graph(name_pattern=".*Order.*")
# → finds "ProcessOrder", "ValidateOrder", etc.

trace_call_path(function_name="ProcessOrder", direction="inbound", depth=3)
```

### Pagination & Token Efficiency

- **Start small**: Search tools return 10 results by default. Check `has_more` before paginating with `offset`.
- **Avoid `include_connected`**: Only enable when you need neighbor names (adds ~30% tokens).
- **`query_graph` has a 200-row cap** that applies BEFORE aggregation — COUNT queries silently undercount.
- **Route handler names**: Route nodes have `properties.handler` with the actual handler function QN.

### Scope to One Project

When multiple repositories are indexed, always scope queries:

```
search_graph(label="Function", name_pattern=".*Handler", project="my-api")
```

## Keeping the Graph Fresh

Auto-sync handles most freshness concerns automatically. Manual intervention is only needed for initial indexing:

| Event | Action |
|-------|--------|
| First time using a project | `index_repository` (required once) |
| After editing/creating/deleting files | **Nothing** — auto-sync detects changes within 1–60s |
| After `git pull` or branch switch | Optional: `index_repository` for immediate freshness |
| Switching to a different project | Check `list_projects`, index if missing |
| Small edits within existing functions | No action needed — graph tracks structure, not line content |

## Performance

- **Indexing**: ~10-30s for medium repos (incremental — only changed files)
- **Queries**: <1s (direct SQLite, no LLM translation)
- **Resource usage**: Single binary, ~50MB RAM, SQLite WAL mode
- **Database**: `~/.cache/codebase-memory-mcp/codebase-memory.db`
- **Reset**: `rm -rf ~/.cache/codebase-memory-mcp/`
