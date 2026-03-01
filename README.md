# codebase-memory-mcp

**Stop paying 100x more tokens for code exploration.** This MCP server indexes your codebase into a persistent knowledge graph that survives session restarts and context compaction. One graph query returns what would take dozens of grep/Glob calls — precise structural results in ~500 tokens vs ~80K tokens for file-by-file exploration.

Single Go binary. No Docker, no external databases, no API keys. One command to install, say *"Index this project"* — done.

Parses source code with [tree-sitter](https://tree-sitter.github.io/tree-sitter/), extracts functions, classes, modules, call relationships, and cross-service HTTP links. Exposes the graph through 12 MCP tools for use with Claude Code, Codex CLI, or any MCP-compatible client. Also includes a **CLI mode** for direct tool invocation from the shell — no MCP client needed.

## Features

- **25 languages**: Python, Go, JavaScript, TypeScript, TSX, Rust, Java, C++, C#, C, PHP, Lua, Scala, Kotlin, Ruby, Bash, Zig, Elixir, Haskell, OCaml, HTML, CSS, YAML, TOML, HCL
- **One-command install**: `codebase-memory-mcp install` auto-detects Claude Code and Codex CLI, registers the MCP server, and installs task-specific skills
- **Self-update**: `codebase-memory-mcp update` downloads the latest release, verifies checksums, and atomically swaps the binary
- **Task-specific skills**: 4 skills (exploring, tracing, quality, reference) that prescribe exact tool sequences — Claude Code automatically uses graph tools instead of defaulting to grep
- **Fast**: Sub-millisecond graph queries, incremental reindex 4x faster than full scan, optimized SQLite with LIKE pre-filtering for regex searches
- **Call graph**: Resolves function calls across files and packages (import-aware, type-inferred)
- **Cross-service HTTP linking**: Discovers REST routes (FastAPI, Gin, Express) and matches them to HTTP call sites with confidence scoring
- **Auto-sync**: Background polling detects file changes and triggers incremental re-indexing automatically — no manual reindex needed after the initial index
- **Incremental reindex**: Content-hash based — only re-parses changed files
- **Cypher-like queries**: `MATCH (f:Function)-[:CALLS]->(g) WHERE f.name = 'main' RETURN g.name`
- **Dead code detection**: Finds functions with zero callers, excluding entry points (route handlers, `main()`, framework-decorated functions)
- **Route nodes**: REST endpoints are first-class graph entities, queryable by path/method
- **JSON config scanning**: Extracts URLs from config/payload JSON files for cross-service linking
- **CLI mode**: Invoke any tool directly from the shell — `codebase-memory-mcp cli search_graph '{"name_pattern": ".*Handler.*"}'`
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

## Performance

Benchmarked on a multi-service project (~37K nodes, ~35K edges) with Go 1.26 on Apple Silicon:

| Operation | Time | Notes |
|-----------|------|-------|
| Fresh index (full codebase) | ~6.3s | 37K nodes, 35K edges |
| Incremental reindex | ~1.2s | Content-hash skip for unchanged files |
| Cypher query (relationship traversal) | <1ms | Up to 600x faster than v0.1.3 for pattern queries |
| Name search (regex) | <10ms | SQL LIKE pre-filtering narrows before Go regex |
| Dead code detection | ~150ms | Full graph scan with degree filtering |
| Trace call path (3 hops) | <10ms | BFS traversal with batch degree counting |

**Token efficiency**: Five structural queries consumed ~3,400 tokens via codebase-memory-mcp versus ~412,000 tokens via file-by-file grep exploration — a **99.2% reduction**.

## Quick Start

1. **Download** the binary for your platform from the [latest release](https://github.com/DeusData/codebase-memory-mcp/releases/latest)
2. **Install**:
   ```bash
   codebase-memory-mcp install
   ```
3. **Restart** Claude Code / Codex CLI
4. Say **"Index this project"** — done.

The `install` command auto-detects Claude Code and Codex CLI, registers the MCP server, installs 4 task-specific skills, and ensures the binary is on your PATH. Use `--dry-run` to preview without making changes.

### Keeping Up to Date

```bash
codebase-memory-mcp update
```

Downloads the latest release, verifies SHA-256 checksums, atomically swaps the binary, and re-applies skills. Restart Claude Code / Codex to activate.

### Uninstall

```bash
codebase-memory-mcp uninstall
```

Removes skills, MCP registration, and Codex instructions. Does not remove the binary or SQLite databases.

## Installation

### Pre-built Binaries

| Platform | Binary |
|----------|--------|
| macOS (Apple Silicon) | `codebase-memory-mcp-darwin-arm64.tar.gz` |
| macOS (Intel) | `codebase-memory-mcp-darwin-amd64.tar.gz` |
| Linux (x86_64) | `codebase-memory-mcp-linux-amd64.tar.gz` |
| Linux (ARM64 / Graviton) | `codebase-memory-mcp-linux-arm64.tar.gz` |
| Windows (x86_64) | `codebase-memory-mcp-windows-amd64.zip` |

Every release includes a `checksums.txt` with SHA-256 hashes for verification.

> **Windows note**: Windows SmartScreen may show "Windows protected your PC" when you first run the binary. This is normal for unsigned open-source software. Click **"More info"** then **"Run anyway"**. You can verify the binary integrity using the `checksums.txt` file included in each release.

### Setup Scripts

<details>
<summary>Alternative: automated download + install</summary>

**macOS / Linux:**

```bash
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup-windows.ps1 | iex
```

The scripts download the correct binary for your platform, install it, and run `codebase-memory-mcp install` to configure Claude Code/Codex.

</details>

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
| **Go** | 1.26+ | `go version` | [go.dev/dl](https://go.dev/dl/) |
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
# Install Go 1.26+ from https://go.dev/dl/
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

### Manual Configuration

<details>
<summary>If you prefer not to use `codebase-memory-mcp install`</summary>

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

Restart Claude Code after adding the config. Verify with `/mcp` — you should see `codebase-memory-mcp` listed with 12 tools.

</details>

### First Use

```
You: "Index this project"
```

Claude Code will call `index_repository` with your project's root path and build the knowledge graph. After indexing, you can ask structural questions like *"what calls main?"*, *"find dead code"*, or *"show cross-service HTTP calls"*.

> **Note**: `index_repository` requires a `repo_path` parameter. When you say "Index this project", Claude Code infers the path from its working directory. If indexing fails, pass the path explicitly: `index_repository(repo_path="/absolute/path/to/project")`.

### Auto-Sync

After the initial `index_repository` call, the graph **stays fresh automatically**. A background watcher polls indexed projects for file changes (mtime + size) and triggers incremental re-indexing when changes are detected. You don't need to manually call `index_repository` again — just edit your code and the graph updates within seconds.

- **Adaptive polling**: The interval scales with repo size (1s for small repos, up to 60s for very large ones)
- **Non-blocking**: Auto-sync never blocks tool queries — if a manual `index_repository` is in progress, the watcher skips that cycle
- **Incremental**: Only changed files are re-parsed (content-hash based), so even triggered re-indexes are fast

You can still call `index_repository` manually at any time to force an immediate reindex (e.g. after a large `git pull`).

## CLI Mode

Every MCP tool can be invoked directly from the command line — no MCP client needed. Useful for testing, scripting, CI pipelines, and quick one-off queries.

```bash
codebase-memory-mcp cli <tool_name> [json_args]
```

By default, the CLI prints a **human-friendly summary**. Use `--raw` for full JSON output (same format the MCP server returns).

### Examples

```bash
# Index a repository
codebase-memory-mcp cli index_repository '{"repo_path": "/path/to/repo"}'
# → Indexed "repo": 1017 nodes, 2574 edges
# →   db: ~/.cache/codebase-memory-mcp/codebase-memory.db

# List indexed projects
codebase-memory-mcp cli list_projects
# → 2 project(s) indexed:
# →   my-api       1017 nodes, 2574 edges  (indexed 2026-02-26T18:10:24Z)
# →   my-frontend   450 nodes,  312 edges  (indexed 2026-02-26T17:34:06Z)

# Search for functions
codebase-memory-mcp cli search_graph '{"name_pattern": ".*Handler.*", "label": "Function"}'
# → 5 result(s) found
# →   [Function] HandleRequest  cmd/server/main.go:42

# Trace call paths
codebase-memory-mcp cli trace_call_path '{"function_name": "Search", "direction": "both"}'
# → Trace from "Search": 8 node(s), 8 edge(s), 2 hop(s)

# Run Cypher queries
codebase-memory-mcp cli query_graph '{"query": "MATCH (f:Function) RETURN f.name LIMIT 5"}'
# → 5 row(s) returned  [f.name]
# →   main
# →   HandleRequest

# View graph schema
codebase-memory-mcp cli get_graph_schema

# No args needed for tools without required parameters
codebase-memory-mcp cli list_projects

# Full JSON output for scripting
codebase-memory-mcp cli --raw search_graph '{"label": "Function", "limit": 100}' | jq '.results[].name'

# List available tools
codebase-memory-mcp cli --help
```

The CLI uses the same SQLite database as the MCP server (`~/.cache/codebase-memory-mcp/codebase-memory.db`). No watcher is started in CLI mode — each invocation is a single-shot operation.

## MCP Tools

### Indexing

| Tool | Key Parameters | Description |
|------|---------------|-------------|
| `index_repository` | `repo_path` (required) | Index a repository into the graph. Only needed once — auto-sync keeps it fresh after that. Supports incremental reindex via content hashing. |
| `list_projects` | — | List all indexed projects with `indexed_at` timestamps and node/edge counts. |
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

## How Claude Code Uses the Graph

After `codebase-memory-mcp install`, Claude Code automatically has 4 task-specific skills that prescribe when and how to use graph tools:

- **Exploring**: Codebase orientation, structure overview, finding functions/classes/routes
- **Tracing**: Call chains, dependency analysis, cross-service HTTP calls, impact analysis
- **Quality**: Dead code detection, fan-out analysis, refactor candidates
- **Reference**: Tool syntax, Cypher query examples, edge types, pitfalls

No CLAUDE.md changes needed — skills auto-trigger based on conversation context.

<details>
<summary>Manual skill installation (if not using `install` command)</summary>

The skill files are embedded in the binary. If you prefer to install them manually, copy from this repo:

- `cmd/codebase-memory-mcp/assets/skills/codebase-memory-exploring/SKILL.md` → `~/.claude/skills/codebase-memory-exploring/SKILL.md`
- `cmd/codebase-memory-mcp/assets/skills/codebase-memory-tracing/SKILL.md` → `~/.claude/skills/codebase-memory-tracing/SKILL.md`
- `cmd/codebase-memory-mcp/assets/skills/codebase-memory-quality/SKILL.md` → `~/.claude/skills/codebase-memory-quality/SKILL.md`
- `cmd/codebase-memory-mcp/assets/skills/codebase-memory-reference/SKILL.md` → `~/.claude/skills/codebase-memory-reference/SKILL.md`


</details>

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
| Graph is missing recently added files | Auto-sync hasn't caught up yet, or project was never indexed | Wait a few seconds for auto-sync, or run `index_repository` manually. Auto-sync polls at 1–60s intervals depending on repo size. |
| Binary not found after install | `~/.local/bin` not on PATH | Add to your shell profile: `export PATH="$HOME/.local/bin:$PATH"` |
| Cypher query fails with parse error | Unsupported Cypher feature | See [Supported Cypher Subset](#supported-cypher-subset). `WITH`, `COLLECT`, `OPTIONAL MATCH` are not supported. |

## Language Benchmark

Benchmarked against 35 real open-source repositories (78 to 49K nodes). 12 standardized questions per language, up to 5 retry attempts each. Grading: PASS (1.0) / PARTIAL (0.5) / FAIL (0.0). Overall: **91.8%** weighted score.

| Tier | Score | Languages |
|------|-------|-----------|
| **Tier 1 — Excellent** | >= 90% | Lua, Kotlin, C++, Perl, Objective-C, Groovy, C, Bash, Zig, Swift, CSS, YAML, TOML, HTML, SCSS, HCL, Dockerfile |
| **Tier 2 — Good** | 75–89% | Python, TypeScript, TSX, Go, Rust, Java, R, Dart, JavaScript, Erlang, Elixir, Scala, Ruby, PHP, C#, SQL |
| **Tier 3 — Functional** | < 75% | OCaml (72%), Haskell (62%) |

**Stress test**: Linux kernel `drivers/net/ethernet/intel/` — 20K nodes, 67K edges, 129K-char deep traces, zero timeouts.

See [`BENCHMARK.md`](BENCHMARK.md) for the full 35-language benchmark with per-question scoring and methodology.

## Architecture

```
cmd/codebase-memory-mcp/  Entry point (MCP stdio server + CLI mode + install/update commands)
internal/
  store/                  SQLite graph storage (nodes, edges, traversal, search)
  lang/                   Language specs (25 languages, tree-sitter node types)
  parser/                 Tree-sitter grammar loading and AST parsing
  pipeline/               4-pass indexing (structure -> definitions -> calls -> HTTP links)
  httplink/               Cross-service HTTP route/call-site matching
  cypher/                 Cypher query lexer, parser, planner, executor
  selfupdate/             GitHub release checking, version comparison, asset download
  tools/                  MCP tool handlers (12 tools) + CLI dispatch
  watcher/                Background auto-sync (mtime+size polling, adaptive intervals)
  discover/               File discovery with .cgrignore support
  fqn/                    Qualified name computation
```

## License

MIT
