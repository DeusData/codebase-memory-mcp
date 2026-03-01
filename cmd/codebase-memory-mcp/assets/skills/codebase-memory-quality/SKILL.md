---
name: codebase-memory-quality
description: >
  This skill should be used when the user asks about "dead code",
  "find dead code", "detect dead code", "show dead code", "dead code analysis",
  "unused functions", "find unused functions", "unreachable code",
  "identify high fan-out functions", "find complex functions",
  "code quality audit", "find functions nobody calls",
  "reduce codebase size", "refactor candidates", "cleanup candidates",
  or needs code quality analysis.
---

# Code Quality Analysis via Knowledge Graph

Use graph degree filtering to find dead code, high-complexity functions, and refactor candidates — all in single tool calls.

## Workflow

### Dead Code Detection

Find functions with zero inbound CALLS edges, excluding entry points:

```
search_graph(
  label="Function",
  relationship="CALLS",
  direction="inbound",
  max_degree=0,
  exclude_entry_points=true
)
```

`exclude_entry_points=true` removes route handlers, `main()`, and framework-registered functions that have zero callers by design.

### Verify Dead Code Candidates

Before deleting, verify each candidate truly has no callers:

```
trace_call_path(function_name="SuspectFunction", direction="inbound", depth=1)
```

Also check for read references (callbacks, stored in variables):

```
query_graph(query="MATCH (a)-[r:USAGE]->(b) WHERE b.name = 'SuspectFunction' RETURN a.name, a.file_path LIMIT 10")
```

### High Fan-Out Functions (calling 10+ others)

These are often doing too much and are refactor candidates:

```
search_graph(
  label="Function",
  relationship="CALLS",
  direction="outbound",
  min_degree=10
)
```

### High Fan-In Functions (called by 10+ others)

These are critical functions — changes have wide impact:

```
search_graph(
  label="Function",
  relationship="CALLS",
  direction="inbound",
  min_degree=10
)
```

### Files That Change Together (Hidden Coupling)

Find files with high git change coupling:

```
query_graph(query="MATCH (a)-[r:FILE_CHANGES_WITH]->(b) WHERE r.coupling_score >= 0.5 RETURN a.name, b.name, r.coupling_score, r.co_change_count ORDER BY r.coupling_score DESC LIMIT 20")
```

High coupling between unrelated files suggests hidden dependencies.

### Unused Imports

```
search_graph(
  relationship="IMPORTS",
  direction="outbound",
  max_degree=0,
  label="Module"
)
```

## Key Tips

- `search_graph` with degree filters has no row cap (unlike `query_graph` which caps at 200).
- Use `file_pattern` to scope analysis to specific directories: `file_pattern="**/services/**"`.
- Dead code detection works best after a full index — run `index_repository` if the project was recently set up.
- Paginate results with `limit` and `offset` — check `has_more` in the response.
