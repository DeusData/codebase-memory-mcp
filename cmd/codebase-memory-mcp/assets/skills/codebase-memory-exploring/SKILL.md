---
name: codebase-memory-exploring
description: >
  This skill should be used when the user asks to "explore the codebase",
  "understand the architecture", "what functions exist", "show me the structure",
  "how is the code organized", "find functions matching", "search for classes",
  "list all routes", "show API endpoints", or needs codebase orientation.
---

# Codebase Exploration via Knowledge Graph

Use graph tools for structural code questions. They return precise results in ~500 tokens vs ~80K for grep-based exploration.

## Workflow

### Step 1: Check if project is indexed

```
list_projects
```

If the project is missing from the list:

```
index_repository(repo_path="/path/to/project")
```

If already indexed, skip — auto-sync keeps the graph fresh.

### Step 2: Get a structural overview

```
get_graph_schema
```

This returns node label counts (functions, classes, routes, etc.), edge type counts, and relationship patterns. Use it to understand what's in the graph before querying.

### Step 3: Find specific code elements

Find functions by name pattern:
```
search_graph(label="Function", name_pattern=".*Handler.*")
```

Find classes:
```
search_graph(label="Class", name_pattern=".*Service.*")
```

Find all REST routes:
```
search_graph(label="Route")
```

Find modules/packages:
```
search_graph(label="Module")
```

Scope to a specific directory:
```
search_graph(label="Function", qn_pattern=".*services\\.order\\..*")
```

### Step 4: Read source code

After finding a function via search, read its source:
```
get_code_snippet(qualified_name="project.path.to.FunctionName")
```

### Step 5: Understand structure

For file/directory exploration within the indexed project:
```
list_directory(path="src/services")
```

## When to Use Grep Instead

- Searching for **string literals** or error messages → `search_code` or Grep
- Finding a file by exact name → Glob
- The graph doesn't index text content, only structural elements

## Key Tips

- Results default to 10 per page. Check `has_more` and use `offset` to paginate.
- Use `project` parameter when multiple repos are indexed.
- Route nodes have a `properties.handler` field with the actual handler function name.
- `exclude_labels` removes noise (e.g., `exclude_labels=["Route"]` when searching by name pattern).
