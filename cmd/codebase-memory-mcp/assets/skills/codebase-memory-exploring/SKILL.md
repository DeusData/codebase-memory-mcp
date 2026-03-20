---
name: codebase-memory-exploring
description: >
  This skill should be used when the user asks to "explore the codebase",
  "understand the architecture", "what functions exist", "show me the structure",
  "how is the code organized", "find functions matching", "search for classes",
  "list all routes", "show API endpoints", "list services", "what services exist",
  "how do services connect", "show service dependencies", "scan repos",
  or needs codebase orientation.
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

## Cross-Repo Service Exploration (Service Graph Tools)

For understanding how services connect across repos — Pub/Sub, GraphQL, database ownership.

### Step 1: Build the service dependency graph

```
scan_repos()
```

Scans all repos in REPOS_DIR. Only needed once — re-run after adding new repos.

### Step 2: List all services

```
list_services
```

Shows each service with counts of Pub/Sub topics, GraphQL calls, and DB tables.

### Step 3: Explore a specific service's dependencies

```
find_dependencies(service="ddi-service")
```

Returns everything a service publishes, subscribes to, queries via GraphQL, and which DB tables it owns/reads. Accepts partial names.

### Step 4: Understand Pub/Sub topology

```
list_topics
```

Shows all Pub/Sub topics with publishers and subscribers (with file:line locations).

### Step 5: Trace a message flow

```
trace_message(topic="user-created")
```

Returns an ASCII flowchart: publisher → topic → subscribers, with file locations.

### Step 6: Get the full graph as JSON

```
get_graph(filter="pubsub")
```

Filters: `all`, `pubsub`, `graphql`, `database`.

## When to Use Grep Instead

- Searching for **string literals** or error messages → `search_code` or Grep
- Finding a file by exact name → Glob
- The graph doesn't index text content, only structural elements

## Key Tips

- Results default to 10 per page. Check `has_more` and use `offset` to paginate.
- Use `project` parameter when multiple repos are indexed.
- Route nodes have a `properties.handler` field with the actual handler function name.
- `exclude_labels` removes noise (e.g., `exclude_labels=["Route"]` when searching by name pattern).
- For cross-repo exploration, always run `scan_repos` first — other service graph tools depend on it.
- `find_dependencies` and `trace_message` accept partial/substring matches.
