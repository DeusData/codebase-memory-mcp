# Codebase Knowledge Graph

This project has a code knowledge graph available via MCP tools. Use it for
structural questions instead of grep — one graph query returns what would take
dozens of file-by-file searches.

## Finding Code

- "Who calls X?" → use `trace_call_path` with `direction=inbound`
- "What does X call?" → use `trace_call_path` with `direction=outbound`
- "Find functions matching pattern" → use `search_graph` with `name_pattern`
- "Find all routes" → use `search_graph` with `label=Route`
- "Show codebase structure" → use `get_graph_schema` for overview

## Tracing Dependencies

- Always discover exact names first: `search_graph(name_pattern=".*Partial.*")`
- Then trace: `trace_call_path(function_name="ExactName", direction="both")`
- Cross-service HTTP calls: `query_graph("MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path")`
- Read source: `get_code_snippet(qualified_name="project.path.FunctionName")`

## Quality Analysis

- Dead code: `search_graph` with `max_degree=0`, `exclude_entry_points=true`, `relationship=CALLS`, `direction=inbound`
- High fan-out: `search_graph` with `min_degree=10`, `relationship=CALLS`, `direction=outbound`
- Change coupling: `query_graph("MATCH (a)-[r:FILE_CHANGES_WITH]->(b) WHERE r.coupling_score >= 0.5 RETURN ...")`

## Important

- Always check `list_projects` first — run `index_repository` if the project is missing
- Use `search_graph` to discover exact names before `trace_call_path` (it requires exact match)
- The graph doesn't index text content — use grep for string literals, error messages, config values
- Results default to 10 per page — check `has_more` and use `offset` to paginate
- Use `project` parameter when multiple repos are indexed
