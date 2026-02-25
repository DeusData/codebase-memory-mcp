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

### Just Want to Get Started?

Grab a pre-built binary from the [latest release](https://github.com/DeusData/codebase-memory-mcp/releases/latest) — no Go, no compiler, no git needed.

| Platform | Binary |
|----------|--------|
| macOS (Apple Silicon) | `codebase-memory-mcp-darwin-arm64.tar.gz` |
| macOS (Intel) | `codebase-memory-mcp-darwin-amd64.tar.gz` |
| Linux (x86_64) | `codebase-memory-mcp-linux-amd64.tar.gz` |
| Linux (ARM64 / Graviton) | `codebase-memory-mcp-linux-arm64.tar.gz` |
| Windows (x86_64) | `codebase-memory-mcp-windows-amd64.zip` |

Every release includes a `checksums.txt` with SHA-256 hashes for verification.

**3 steps:**

1. **Download and extract** the binary for your platform
2. **Put it somewhere on your PATH** (e.g. `~/.local/bin/` on macOS/Linux, or any folder on Windows)
3. **Tell Claude Code about it** — add to `.mcp.json` in your project root:
   ```json
   {
     "mcpServers": {
       "codebase-memory-mcp": {
         "type": "stdio",
         "command": "/path/to/codebase-memory-mcp"
       }
     }
   }
   ```

Restart Claude Code, verify with `/mcp`, then say **"Index this project"** — done.

> **Windows note**: Windows SmartScreen may show "Windows protected your PC" when you first run the binary. This is normal for unsigned open-source software. Click **"More info"** then **"Run anyway"**. You can verify the binary integrity using the `checksums.txt` file included in each release.

### Setup Scripts (Automated)

The setup scripts automate download, placement, and Claude Code configuration.

**macOS / Linux:**

```bash
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup-windows.ps1 | iex
```

The scripts download the correct binary for your platform, install it, and offer to auto-configure Claude Code's MCP settings.

### Install via Claude Code

Or just paste the repo URL into Claude Code:

```
You: "Install this MCP server: https://github.com/DeusData/codebase-memory-mcp"
```

Claude Code will clone, build, and configure it automatically.

### Want to Hack on It? Build from Source

<details>
<summary>Prerequisites</summary>

| Requirement | Version | Check | Install |
|-------------|---------|-------|---------|
| **Go** | 1.23+ | `go version` | [go.dev/dl](https://go.dev/dl/) |
| **C compiler** | gcc or clang | `gcc --version` or `clang --version` | See below |
| **Git** | any | `git --version` | Pre-installed on most systems |

A **C compiler** is needed because tree-sitter uses CGO (C bindings for AST parsing):

- **macOS**: `xcode-select --install` (provides `clang`)
- **Linux (Debian/Ubuntu)**: `sudo apt install build-essential`
- **Linux (Fedora/RHEL)**: `sudo dnf install gcc`
- **Windows**: Install [MSYS2](https://www.msys2.org/), then: `pacman -S mingw-w64-ucrt-x86_64-gcc`. Build from an MSYS2 UCRT64 shell.

</details>

**Via setup script:**

```bash
# macOS / Linux
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup.sh | bash -s -- --from-source

# Windows (PowerShell) — builds inside WSL
irm https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup-windows.ps1 -OutFile setup.ps1; .\setup.ps1 -FromSource
```

**Or manually:**

```bash
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp
CGO_ENABLED=1 go build -o codebase-memory-mcp ./cmd/codebase-memory-mcp/
# Move the binary to somewhere on your PATH
```

On **Windows with MSYS2** (from a UCRT64 shell):

```bash
CGO_ENABLED=1 CC=gcc go build -o codebase-memory-mcp.exe ./cmd/codebase-memory-mcp/
```

On **Windows with WSL** (credit: [@Flipper1994](https://github.com/Flipper1994)):

```bash
# Inside WSL (Ubuntu)
sudo apt update && sudo apt install build-essential
# Install Go 1.23+ from https://go.dev/dl/
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp
CGO_ENABLED=1 go build -buildvcs=false -o ~/.local/bin/codebase-memory-mcp ./cmd/codebase-memory-mcp/
```

When using a WSL-built binary, configure Claude Code to invoke it via `wsl.exe`:

```json
{
  "mcpServers": {
    "codebase-memory-mcp": {
      "type": "stdio",
      "command": "wsl.exe",
      "args": ["-d", "Ubuntu", "--", "/home/YOUR_USER/.local/bin/codebase-memory-mcp"]
    }
  }
}
```

### Configure Claude Code

Add the MCP server to your project's `.mcp.json` (per-project, recommended) or `~/.claude/settings.json` (global):

```json
{
  "mcpServers": {
    "codebase-memory-mcp": {
      "type": "stdio",
      "command": "/path/to/codebase-memory-mcp"
    }
  }
}
```

Restart Claude Code after adding the config. Verify with `/mcp` — you should see `codebase-memory-mcp` listed with 11 tools.

### First Use

```
You: "Index this project"
```

Claude Code will call `index_repository` with your project's root path and build the knowledge graph. After indexing, you can ask structural questions like *"what calls main?"*, *"find dead code"*, or *"show cross-service HTTP calls"*.

> **Note**: `index_repository` requires a `repo_path` parameter. When you say "Index this project", Claude Code infers the path from its working directory. If indexing fails, pass the path explicitly: `index_repository(repo_path="/absolute/path/to/project")`.

**When to reindex**: Run `index_repository` again after adding new files, moving functions, or making significant code changes. Incremental reindex is fast — only changed files are re-parsed (content-hash based). Check staleness with `list_projects` which shows `indexed_at` timestamps.

## MCP Tools

### Indexing

| Tool | Key Parameters | Description |
|------|---------------|-------------|
| `index_repository` | `repo_path` (required) | Index a repository into the graph. Supports incremental reindex via content hashing — only changed files are re-parsed. |
| `list_projects` | — | List all indexed projects with `indexed_at` timestamps and node/edge counts. Use to check if a reindex is needed. |
| `delete_project` | `project_name` (required) | Remove a project and all its graph data. Irreversible. |

### Querying

| Tool | Key Parameters | Description |
|------|---------------|-------------|
| `search_graph` | `label`, `name_pattern`, `project`, `file_pattern`, `relationship`, `direction`, `min_degree`, `max_degree`, `exclude_entry_points`, `limit` (default 100), `offset` | Structured search with filters. Use `project` to scope to a single repo when multiple are indexed. Supports pagination via `limit`/`offset` — response includes `has_more` and `total`. |
| `trace_call_path` | `function_name` (required), `direction` (inbound/outbound/both), `depth` (1-5, default 3) | BFS traversal from/to a function (exact name match). Returns call chains with signatures, constants, and edge types. Capped at 200 nodes. |
| `query_graph` | `query` (required) | Execute Cypher-like graph queries (read-only). See [Supported Cypher Subset](#supported-cypher-subset) for what's supported. |
| `get_graph_schema` | — | Node/edge counts, relationship patterns, sample names. Run this first to understand what's in the graph. |
| `get_code_snippet` | `qualified_name` (required) | Read source code for a function by its qualified name (reads from disk). See [Qualified Names](#qualified-names) for the format. |

### File Access

> **Note**: These tools require at least one indexed project. They resolve relative paths against indexed project roots. Index a project first with `index_repository`.

| Tool | Key Parameters | Description |
|------|---------------|-------------|
| `search_code` | `pattern` (required), `file_pattern`, `regex`, `max_results` (default 100), `offset` | Grep-like text search within indexed project files. Supports pagination via `max_results`/`offset`. |
| `read_file` | `path`, `start_line`, `end_line` | Read any file from an indexed project. Path can be absolute or relative to project root. |
| `list_directory` | `path`, `pattern` | List files/directories with optional glob filtering (e.g. `*.go`, `*.py`). |

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

### Scope queries to a single project

When multiple repositories are indexed, use `project` to avoid cross-project contamination:

```
search_graph(label="Function", name_pattern=".*Handler", project="my-api")
```

### Discover then trace (when you don't know the exact name)

`trace_call_path` requires an exact function name match. Use `search_graph` first to discover the correct name:

```
search_graph(label="Function", name_pattern=".*Order.*")
# → finds "ProcessOrder", "ValidateOrder", etc.

trace_call_path(function_name="ProcessOrder", direction="inbound", depth=3)
```

### Paginate large result sets

All search tools support pagination. The response includes `total`, `has_more`, `limit`, and `offset`:

```
search_graph(label="Function", limit=50, offset=0)
# → {total: 449, has_more: true, limit: 50, offset: 0, results: [...]}

search_graph(label="Function", limit=50, offset=50)
# → next page
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

### Edge Properties

- **HTTP_CALLS**: `confidence` (0.0–1.0), `url_path`, `http_method`
- **CALLS**: `via` (e.g. `"route_registration"` for handler wiring)

Edge properties are accessible in Cypher queries: `MATCH (a)-[r:HTTP_CALLS]->(b) RETURN r.confidence, r.url_path`

### Qualified Names

`get_code_snippet` and graph results use **qualified names** in the format `<project>.<path_parts>.<name>`:

| Language | Source | Qualified Name |
|----------|--------|---------------|
| Go | `cmd/server/main.go` → `HandleRequest` | `myproject.cmd.server.main.HandleRequest` |
| Python | `services/orders.py` → `ProcessOrder` | `myproject.services.orders.ProcessOrder` |
| Python | `services/__init__.py` → `setup` | `myproject.services.setup` |
| TypeScript | `src/components/App.tsx` → `App` | `myproject.src.components.App.App` |
| Method | `UserService.GetUser` | `myproject.pkg.service.UserService.GetUser` |

The format is: project name, file path with `/` replaced by `.` and extension removed, then the symbol name. Use `search_graph` to discover qualified names before passing them to `get_code_snippet`.

### Supported Cypher Subset

`query_graph` supports a subset of the Cypher query language. Results are capped at 200 rows.

**Supported:**
- `MATCH` with node labels: `(f:Function)`
- `MATCH` with relationship types: `-[:CALLS]->`
- `MATCH` with variable-length paths: `-[:CALLS*1..3]->`
- `WHERE` with `=`, `<>`, `>`, `<`, `>=`, `<=`
- `WHERE` with `=~` (regex), `CONTAINS`, `STARTS WITH`
- `WHERE` with `AND`, `OR`, `NOT`
- `RETURN` with property access: `f.name`, `r.confidence`
- `RETURN` with `COUNT(x)`, `DISTINCT`
- `ORDER BY` with `ASC`/`DESC`
- `LIMIT`
- Edge property access: `r.confidence`, `r.url_path`

**Not supported:**
- `WITH` clauses
- `COLLECT`, `SUM`, or other aggregation functions (except `COUNT`)
- `CREATE`, `DELETE`, `SET`, `MERGE` (read-only)
- `OPTIONAL MATCH`
- `UNION`
- Variable-length path edge property binding (can't access individual edges in a path like `*1..3`)

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
- **Scope to one project**: `search_graph(..., project="project-name")` when multiple repos are indexed
- **Understand structure first**: `get_graph_schema` before writing complex queries
- **Read source**: `get_code_snippet(qualified_name="...")` after finding functions via search
- **Discover then trace**: Use `search_graph(name_pattern=".*Partial.*")` to find exact names, then `trace_call_path`
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

## Ignoring Files (`.cgrignore`)

Place a `.cgrignore` file in your project root to exclude directories or files from indexing. The syntax is one glob pattern per line (comments with `#`):

```
# .cgrignore
generated
vendor
__pycache__
*.pb.go
testdata
fixtures
```

Patterns are matched against both directory names and relative paths using Go's `filepath.Match` syntax. Directories matching a pattern are skipped entirely (including all contents).

The following directories are **always ignored** regardless of `.cgrignore`: `.git`, `node_modules`, `vendor`, `__pycache__`, `.mypy_cache`, `.venv`, `dist`, `build`, `.cache`, `.idea`, `.vscode`, and others.

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

## Troubleshooting

| Problem | Cause | Fix |
|---------|-------|-----|
| `/mcp` doesn't show the server | Config not loaded or binary not found | Check `.mcp.json` path is absolute and correct. Restart Claude Code. Verify binary runs: `/path/to/codebase-memory-mcp` should output JSON. |
| `index_repository` fails | Missing `repo_path` or path doesn't exist | Pass an absolute path: `index_repository(repo_path="/absolute/path")` |
| `read_file` / `list_directory` returns error | No project indexed yet | Run `index_repository` first. These tools resolve paths against indexed project roots. |
| `get_code_snippet` returns "node not found" | Wrong qualified name format | Use `search_graph` first to find the exact `qualified_name`, then pass it to `get_code_snippet`. See [Qualified Names](#qualified-names). |
| `trace_call_path` returns 0 results | Exact name match — no fuzzy matching | Use `search_graph(name_pattern=".*PartialName.*")` to discover the exact function name first. |
| Queries return results from wrong project | Multiple projects indexed, no filter | Add `project="your-project-name"` to `search_graph`. Use `list_projects` to see indexed project names. |
| Graph is missing recently added files | Index is stale | Re-run `index_repository`. Incremental reindex only re-parses changed files. |
| Binary not found after install | `~/.local/bin` not on PATH | Add to your shell profile: `export PATH="$HOME/.local/bin:$PATH"` |
| Cypher query fails with parse error | Unsupported Cypher feature | See [Supported Cypher Subset](#supported-cypher-subset). `WITH`, `COLLECT`, `OPTIONAL MATCH` are not supported. |

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
