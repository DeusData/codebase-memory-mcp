---
name: codebase-memory-tracing
description: >
  This skill should be used when the user asks "who calls this function",
  "what does X call", "trace the call chain", "find callers of",
  "show dependencies", "what depends on", "trace call path",
  "find all references to", "impact analysis", or needs to understand
  function call relationships and dependency chains.
---

# Call Chain Tracing via Knowledge Graph

Use graph tools to trace function call relationships. One `trace_call_path` call replaces dozens of grep searches across files.

## Workflow

### Step 1: Discover the exact function name

`trace_call_path` requires an **exact** name match. If you don't know the exact name, discover it first:

```
search_graph(name_pattern=".*Order.*", label="Function")
```

This returns matching functions with their qualified names and file locations.

### Step 2: Trace callers (who calls this function?)

```
trace_call_path(function_name="ProcessOrder", direction="inbound", depth=3)
```

Returns a hop-by-hop list of all functions that call `ProcessOrder`, up to 3 levels deep.

### Step 3: Trace callees (what does this function call?)

```
trace_call_path(function_name="ProcessOrder", direction="outbound", depth=3)
```

### Step 4: Full context (both callers and callees)

```
trace_call_path(function_name="ProcessOrder", direction="both", depth=3)
```

**Always use `direction="both"` for complete context.** Cross-service HTTP_CALLS edges from other services appear as inbound edges — `direction="outbound"` alone misses them.

### Step 5: Read suspicious code

After finding interesting callers/callees, read their source:

```
get_code_snippet(qualified_name="project.path.module.FunctionName")
```

## Cross-Service HTTP Calls

To see all HTTP links between services with URLs and confidence scores:

```
query_graph(query="MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path, r.confidence ORDER BY r.confidence DESC LIMIT 20")
```

Filter by URL path:
```
query_graph(query="MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.url_path CONTAINS '/orders' RETURN a.name, b.name, r.url_path")
```

## Async Dispatch (Cloud Tasks, Pub/Sub, etc.)

Find dispatch functions by name pattern, then trace:
```
search_graph(name_pattern=".*CreateTask.*|.*send_to_pubsub.*")
trace_call_path(function_name="CreateMultidataTask", direction="both")
```

## Interface Implementations

Find which structs implement an interface method:
```
query_graph(query="MATCH (s)-[r:OVERRIDE]->(i) WHERE i.name = 'Read' RETURN s.name, i.name LIMIT 20")
```

## Read References (callbacks, variable assignments)

```
query_graph(query="MATCH (a)-[r:USAGE]->(b) WHERE b.name = 'ProcessOrder' RETURN a.name, a.file_path LIMIT 20")
```

## Key Tips

- Start with `depth=1` for quick answers, increase only if needed (max 5).
- Edge types in trace results: `CALLS` (direct), `HTTP_CALLS` (cross-service), `ASYNC_CALLS` (async dispatch), `USAGE` (read reference), `OVERRIDE` (interface implementation).
- `search_graph(relationship="HTTP_CALLS")` filters nodes by degree — it does NOT return edges. Use `query_graph` with Cypher to see actual edges with properties.
- Results are capped at 200 nodes per trace.
