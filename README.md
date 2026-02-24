# codebase-memory-mcp

An MCP server that remembers your codebase structure. Indexes source code into a queryable knowledge graph — functions, classes, call chains, cross-service HTTP links — all stored in embedded SQLite. Single Go binary, no Docker, no external databases.

Parses source code with [tree-sitter](https://tree-sitter.github.io/tree-sitter/), extracts functions, classes, modules, call relationships, and cross-service HTTP links. Exposes the graph through 11 MCP tools for use with Claude Code or any MCP-compatible client.

## Features

- **12 languages**: Python, Go, JavaScript, TypeScript, TSX, Rust, Java, C++, C#, PHP, Lua, Scala
- **Call graph**: Resolves function calls across files and packages (import-aware, type-inferred)
- **Cross-service HTTP linking**: Discovers REST routes (FastAPI, Gin, Express) and matches them to HTTP call sites with confidence scoring
- **Incremental reindex**: Content-hash based — only re-parses changed files
- **Cypher-like queries**: `MATCH (f:Function)-[:CALLS]->(g) WHERE f.name = 'main' RETURN g.name`
- **Dead code detection**: Finds functions with zero callers, excluding entry points (route handlers, `main()`, framework-decorated functions)
- **Route nodes**: REST endpoints are first-class graph entities, queryable by path/method
- **JSON config scanning**: Extracts URLs from config/payload JSON files for cross-service linking
- **Single binary, zero infrastructure**: SQLite WAL mode, persists to `~/.cache/codebase-memory-mcp/`

## How It Works

codebase-memory-mcp is a **structural analysis backend** — it builds and queries the knowledge graph. It does **not** include an LLM. Instead, it relies on the MCP client (Claude Code, or any MCP-compatible AI assistant) to be the intelligence layer.

When you ask Claude Code a question like *"what calls ProcessOrder?"*, this is what happens:

1. **Claude Code** understands your natural language question
2. **Claude Code** decides which MCP tool to call — in this case `trace_call_path(function_name="ProcessOrder", direction="inbound")`
3. **codebase-memory-mcp** executes the graph query against SQLite and returns structured results
4. **Claude Code** interprets the results and presents them in plain English

For complex graph patterns, Claude Code writes Cypher queries on the fly:

```
You: "Show me all cross-service HTTP calls with confidence above 0.5"

Claude Code generates and sends:
  query_graph(query="MATCH (a)-[r:HTTP_CALLS]->(b) WHERE r.confidence > 0.5
                     RETURN a.name, b.name, r.url_path, r.confidence
                     ORDER BY r.confidence DESC LIMIT 20")

codebase-memory-mcp returns the matching edges.
Claude Code formats and explains the results.
```

**Why no built-in LLM?** Other code graph tools embed an LLM to translate natural language into graph queries. This means extra API keys, extra cost per query, and another model to configure. With MCP, the AI assistant you're already talking to *is* the query translator — no duplication needed.

**Token efficiency**: Compared to having an AI agent grep through your codebase file by file, graph queries return precise results in a single tool call. In benchmarks on a multi-service project (2,348 nodes, 3,853 edges), five structural queries consumed ~3,400 tokens via codebase-memory-mcp versus ~412,000 tokens via file-by-file exploration — a **99.2% reduction**.

## Installation

### Quick Install via Claude Code

The fastest way: paste the repo URL directly into Claude Code and ask it to install:

```
You: "Install this MCP server: https://github.com/DeusData/codebase-memory-mcp"
```

Claude Code will clone, build, and configure the MCP server automatically.

### Prerequisites

| Requirement | Version | Check | Install |
|-------------|---------|-------|---------|
| **Go** | 1.23+ | `go version` | [go.dev/dl](https://go.dev/dl/) |
| **C compiler** | gcc or clang | `gcc --version` or `clang --version` | See below |
| **Git** | any | `git --version` | Pre-installed on most systems |

**C compiler** is needed because tree-sitter uses CGO (C bindings for AST parsing):

- **macOS**: Install Xcode command line tools — `xcode-select --install`. This provides `clang` and is likely already installed.
- **Linux (Debian/Ubuntu)**: `sudo apt install build-essential`
- **Linux (Fedora/RHEL)**: `sudo dnf install gcc`
- **Windows**: Not currently supported (CGO cross-compilation is complex). Use WSL2 with the Linux instructions above.

### Build from Source

```bash
# Clone the repository
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp

# Build the binary (CGO_ENABLED=1 is the default, but be explicit)
CGO_ENABLED=1 go build -o codebase-memory-mcp ./cmd/codebase-memory-mcp/

# Option A: Move to a directory on your PATH
sudo mv codebase-memory-mcp /usr/local/bin/

# Option B: Or keep it in place and use the absolute path in MCP config
```

### Verify

```bash
# Should print nothing and wait for stdio input (Ctrl+C to exit)
codebase-memory-mcp
```

### Configure Claude Code

Add the MCP server to your project's `.mcp.json` (per-project) or `~/.claude/settings.json` (global):

**Per-project** (`.mcp.json` in project root — recommended):

```json
{
  "mcpServers": {
    "codebase-memory-mcp": {
      "type": "stdio",
      "command": "/usr/local/bin/codebase-memory-mcp"
    }
  }
}
```

**Global** (`~/.claude/settings.json`):

```json
{
  "mcpServers": {
    "codebase-memory-mcp": {
      "type": "stdio",
      "command": "/usr/local/bin/codebase-memory-mcp"
    }
  }
}
```

If you kept the binary in the cloned directory, use the full path instead:

```json
{
  "command": "/path/to/codebase-memory-mcp/codebase-memory-mcp"
}
```

Restart Claude Code after adding the config. Verify with `/mcp` — you should see `codebase-memory-mcp` listed with 11 tools.

### First Use

```
You: "Index this project"
```

Claude Code will call `index_repository` and build the knowledge graph. After indexing, you can ask structural questions like *"what calls main?"*, *"find dead code"*, or *"show cross-service HTTP calls"*.

## MCP Tools

### Indexing

| Tool | Description |
|------|-------------|
| `index_repository` | Index a repository into the graph. Supports incremental reindex via content hashing. |
| `list_projects` | List all indexed projects with timestamps and node/edge counts. |
| `delete_project` | Remove a project and all its graph data. |

### Querying

| Tool | Description |
|------|-------------|
| `search_graph` | Structured search with filters: label, name pattern (regex), file pattern (glob), relationship type, degree (fan-in/fan-out), entry point exclusion. |
| `trace_call_path` | BFS traversal from/to a function. Returns call chains with signatures, constants, and edge types. |
| `query_graph` | Execute Cypher-like graph queries (read-only). |
| `get_graph_schema` | Node/edge counts, relationship patterns, sample names. |
| `get_code_snippet` | Read source code for a function by qualified name (reads from disk). |

### File Access

| Tool | Description |
|------|-------------|
| `search_code` | Grep-like text search within indexed project files. |
| `read_file` | Read any file from an indexed project (with optional line range). |
| `list_directory` | List files/directories with glob filtering. |

## Usage Examples

### Index a project

```
index_repository(repo_path="/path/to/your/project")
```

### Find all functions matching a pattern

```
search_graph(label="Function", name_pattern=".*Handler")
```

### Trace what a function calls

```
trace_call_path(function_name="ProcessOrder", depth=3, direction="outbound")
```

### Find what calls a function

```
trace_call_path(function_name="ProcessOrder", depth=2, direction="inbound")
```

### Dead code detection

```
search_graph(
  label="Function",
  relationship="CALLS",
  direction="inbound",
  max_degree=0,
  exclude_entry_points=true
)
```

### Cross-service HTTP calls

```
search_graph(label="Function", relationship="HTTP_CALLS", direction="outbound")
```

### Query all REST routes

```
search_graph(label="Route")
```

### Cypher queries

```
query_graph(query="MATCH (f:Function)-[:CALLS]->(g:Function) WHERE f.name = 'main' RETURN g.name, g.qualified_name LIMIT 20")
```

```
query_graph(query="MATCH (a)-[r:HTTP_CALLS]->(b) RETURN a.name, b.name, r.url_path, r.confidence LIMIT 10")
```

### High fan-out functions (calling 10+ others)

```
search_graph(label="Function", relationship="CALLS", direction="outbound", min_degree=10)
```

## Graph Data Model

### Node Labels

`Project`, `Package`, `Folder`, `File`, `Module`, `Class`, `Function`, `Method`, `Interface`, `Enum`, `Type`, `Route`

### Edge Types

`CONTAINS_PACKAGE`, `CONTAINS_FOLDER`, `CONTAINS_FILE`, `CONTAINS_MODULE`, `DEFINES`, `DEFINES_METHOD`, `IMPORTS`, `CALLS`, `HTTP_CALLS`, `INHERITS`, `IMPLEMENTS`, `DEPENDS_ON_EXTERNAL`, `HANDLES`

### Node Properties

- **Function/Method**: `signature`, `return_type`, `receiver`, `decorators`, `is_exported`, `is_entry_point`
- **Module**: `constants` (list of module-level constants)
- **Route**: `method`, `path`, `handler`
- **All nodes**: `name`, `qualified_name`, `file_path`, `start_line`, `end_line`

## Teaching Claude Code to Use the Graph

Claude Code can use the tools without any configuration — the MCP tool descriptions are self-documenting. However, without a hint, Claude Code will default to its built-in Grep/Glob/Read tools for code questions instead of the faster graph queries.

Add one of the following to tell Claude Code to **prefer graph tools for structural questions**.

### Option A: Global CLAUDE.md (recommended — works across all projects)

Add to `~/.claude/CLAUDE.md`:

```markdown
## Codebase Memory (codebase-memory-mcp)

When this MCP server is available, **prefer graph tools over grep/Explore for structural code questions**.
Graph queries return precise results in a single tool call (~500 tokens) vs file-by-file exploration (~80K tokens).

- **Before exploration/planning**: Run `index_repository` to ensure the graph is current
- **"Who calls X?"**: `trace_call_path(function_name="X", direction="inbound")`
- **"What does X call?"**: `trace_call_path(function_name="X", direction="outbound")`
- **Find functions by pattern**: `search_graph(label="Function", name_pattern=".*Pattern.*")`
- **Dead code**: `search_graph(label="Function", relationship="CALLS", direction="inbound", max_degree=0, exclude_entry_points=true)`
- **Cross-service calls**: `search_graph(relationship="HTTP_CALLS")` or `query_graph` with Cypher
- **REST routes**: `search_graph(label="Route")`
- **Understand structure first**: `get_graph_schema` before writing complex queries
- **Read source**: `get_code_snippet(qualified_name="...")` after finding functions via search
- **Complex patterns**: `query_graph` with Cypher for multi-hop graph traversals

Use grep/Glob for text search (string literals, error messages, config values) — the graph doesn't index text content.
```

### Option B: Per-project CLAUDE.md

Add the same snippet to a specific project's `CLAUDE.md` if you only want it active for that project.

### Option C: Claude Code skill file

Create `~/.claude/skills/codebase-memory.md` for automatic activation when relevant:

```markdown
# codebase-memory-mcp Skill

## When to use
- Structural code questions: "who calls X?", "what does X depend on?", "show me the call chain"
- Dead code analysis: functions with zero callers
- Cross-service tracing: HTTP call paths between microservices
- Architecture overview: understanding module boundaries and dependencies
- Pre-planning: index before designing changes to understand blast radius

## When NOT to use
- Text search (use grep/Glob instead)
- Single file reads (use Read tool instead)
- Syntax/formatting questions (not a graph concern)

## Workflow
1. **Ensure freshness**: `list_projects` to check `indexed_at`. If stale, `index_repository`.
2. **Understand schema**: `get_graph_schema` to see what's indexed (node counts, edge types).
3. **Search**: `search_graph` for filtered queries, `trace_call_path` for call chains.
4. **Deep dive**: `get_code_snippet` to read source of interesting functions.
5. **Complex queries**: `query_graph` with Cypher for multi-hop patterns.

## Tips
- `trace_call_path` with `direction="both"` shows full context (callers + callees)
- `search_graph` with `file_pattern` scopes results to a service/directory
- Route nodes (`label="Route"`) let you query REST endpoints as graph entities
- Edge properties on HTTP_CALLS include `confidence` and `url_path`
- Reindex after significant code changes (new files, moved functions)
```

## Persistence

The SQLite database is stored at `~/.cache/codebase-memory-mcp/codebase-memory.db`. It persists across restarts automatically (WAL mode, ACID-safe).

To reset everything:

```bash
rm -rf ~/.cache/codebase-memory-mcp/
```

## Development

```bash
make build    # Build binary to bin/
make test     # Run all tests
make lint     # Run golangci-lint
make install  # go install
```

## Architecture

```
cmd/codebase-memory-mcp/  Entry point (MCP stdio server)
internal/
  store/                  SQLite graph storage (nodes, edges, traversal, search)
  lang/                   Language specs (12 languages, tree-sitter node types)
  parser/                 Tree-sitter grammar loading and AST parsing
  pipeline/               4-pass indexing (structure -> definitions -> calls -> HTTP links)
  httplink/               Cross-service HTTP route/call-site matching
  cypher/                 Cypher query lexer, parser, planner, executor
  tools/                  MCP tool handlers (11 tools)
  discover/               File discovery with .cgrignore support
  fqn/                    Qualified name computation
```

## License

MIT
