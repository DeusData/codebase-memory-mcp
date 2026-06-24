# codebase-memory-mcp

[![GitHub Release](https://img.shields.io/github/v/release/DeusData/codebase-memory-mcp?style=flat&color=blue)](https://github.com/DeusData/codebase-memory-mcp/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)
[![CI](https://img.shields.io/github/actions/workflow/status/DeusData/codebase-memory-mcp/dry-run.yml?label=CI)](https://github.com/DeusData/codebase-memory-mcp/actions/workflows/dry-run.yml)
[![Tests](https://img.shields.io/badge/tests-5604_passing-brightgreen)](https://github.com/DeusData/codebase-memory-mcp)
[![Languages](https://img.shields.io/badge/languages-158-orange)](https://github.com/DeusData/codebase-memory-mcp)
[![Hybrid LSP](https://img.shields.io/badge/Hybrid_LSP-9_languages-blue)](#hybrid-lsp)
[![Agents](https://img.shields.io/badge/agents-11-purple)](https://github.com/DeusData/codebase-memory-mcp)
[![Pure C](https://img.shields.io/badge/pure_C-zero_dependencies-blue)](https://github.com/DeusData/codebase-memory-mcp)
[![Platform](https://img.shields.io/badge/macOS_%7C_Linux_%7C_Windows-supported-lightgrey)](https://github.com/DeusData/codebase-memory-mcp/releases/latest)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/DeusData/codebase-memory-mcp/badge)](https://scorecard.dev/viewer/?uri=github.com/DeusData/codebase-memory-mcp)
[![SLSA 3](https://slsa.dev/images/gh-badge-level3.svg)](https://slsa.dev)
[![VirusTotal](https://img.shields.io/badge/VirusTotal-scanned_every_release-brightgreen?logo=virustotal)](https://github.com/DeusData/codebase-memory-mcp/releases/latest)
[![arXiv](https://img.shields.io/badge/arXiv-2603.27277-b31b1b?logo=arxiv)](https://arxiv.org/abs/2603.27277)

**The fastest and most efficient code intelligence engine for AI coding agents.** Full-indexes an average repository in milliseconds, the Linux kernel (28M LOC, 75K files) in 3 minutes. Answers structural queries in under 1ms. Ships as a single static binary for macOS, Linux, and Windows — download, run `install`, done.

High-quality parsing through [tree-sitter](https://tree-sitter.github.io/tree-sitter/) AST analysis across all 158 languages, enhanced with [**Hybrid LSP** semantic type resolution](#hybrid-lsp) for Python, TypeScript / JavaScript / JSX / TSX, PHP, C#, Go, C, C++, Java, Kotlin, and Rust — producing a persistent knowledge graph of functions, classes, call chains, HTTP routes, and cross-service links. 14 MCP tools. Zero dependencies. Plug and play across 11 coding agents.

> **Research** — The design and benchmarks behind this project are described in the preprint [*Codebase-Memory: Tree-Sitter-Based Knowledge Graphs for LLM Code Exploration via MCP*](https://arxiv.org/abs/2603.27277) (arXiv:2603.27277). Evaluated across 31 real-world repositories: 83% answer quality, 10× fewer tokens, 2.1× fewer tool calls vs. file-by-file exploration.

> **Security & Trust** — This tool reads your codebase and writes to your agent configuration files. That is what it is designed to do. If you prefer to audit before running, the [full source is here](https://github.com/DeusData/codebase-memory-mcp) — every release binary is signed, checksummed, and scanned by 70+ antivirus engines. All processing happens 100% locally; your code never leaves your machine. Found a security issue? We want to know — see [SECURITY.md](SECURITY.md). Security is Priority #1 for us.

<p align="center">
  <img src="docs/graph-ui-screenshot.png" alt="Graph visualization UI showing the codebase-memory-mcp knowledge graph" width="800">
  <br>
  <em>Built-in 3D graph visualization (UI variant) — explore your knowledge graph at localhost:9749</em>
</p>

## Why codebase-memory-mcp

- **Extreme indexing speed** — Linux kernel (28M LOC, 75K files) in 3 minutes. RAM-first pipeline: LZ4 compression, in-memory SQLite, fused Aho-Corasick pattern matching. Memory released after indexing.
- **Plug and play** — single static binary for macOS (arm64/amd64), Linux (arm64/amd64), and Windows (amd64). No Docker, no runtime dependencies, no API keys. Download → `install` → restart agent → done.
- **158 languages** — vendored tree-sitter grammars compiled into the binary. Nothing to install, nothing that breaks.
- **120x fewer tokens** — 5 structural queries: ~3,400 tokens vs ~412,000 via file-by-file search. One graph query replaces dozens of grep/read cycles.
- **11 agents, one command** — `install` auto-detects Claude Code, Codex CLI, Gemini CLI, Zed, OpenCode, Antigravity, Aider, KiloCode, VS Code, OpenClaw, and Kiro — configures MCP entries, instruction files, and pre-tool hooks for each.
- **Built-in graph visualization** — 3D interactive UI at `localhost:9749` (optional UI binary variant).
- **Infrastructure-as-code indexing** — Dockerfiles, Kubernetes manifests, and Kustomize overlays indexed as graph nodes with cross-references. `Resource` nodes for K8s kinds, `Module` nodes for Kustomize overlays with `IMPORTS` edges to referenced resources.
- **14 MCP tools** — search, trace, architecture, impact analysis, Cypher queries, dead code detection, cross-service HTTP linking, ADR management, and more.

## Quick Start

**One-line install** (macOS / Linux):
```bash
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.sh | bash
```

With graph visualization UI:
```bash
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.sh | bash -s -- --ui
```

**Windows** (PowerShell):
```powershell
# 1. Download the installer
Invoke-WebRequest -Uri https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/install.ps1 -OutFile install.ps1

# 2. (Optional but recommended) Inspect the script
notepad install.ps1

# 3. Run it
.\install.ps1

```

Options: `--ui` (graph visualization), `--skip-config` (binary only, no agent setup), `--dir=<path>` (custom location).

Restart your coding agent. Say **"Index this project"** — done.

<details>
<summary>Manual install</summary>

1. **Download** the archive for your platform from the [latest release](https://github.com/DeusData/codebase-memory-mcp/releases/latest):
   - `codebase-memory-mcp-<os>-<arch>.tar.gz` (macOS/Linux) or `.zip` (Windows) — standard
   - `codebase-memory-mcp-ui-<os>-<arch>.tar.gz` / `.zip` — with graph visualization

2. **Extract and install** (each archive includes `install.sh` or `install.ps1`):

   macOS / Linux:
   ```bash
   tar xzf codebase-memory-mcp-*.tar.gz
   ./install.sh
   ```

   Windows (PowerShell):
   ```powershell
   Expand-Archive codebase-memory-mcp-windows-amd64.zip -DestinationPath .
   .\install.ps1
   ```

3. **Restart** your coding agent.

The `install` command automatically strips macOS quarantine attributes and ad-hoc signs the binary — no manual `xattr`/`codesign` needed.
</details>

The `install` command auto-detects all installed coding agents and configures MCP server entries, instruction files, skills, and pre-tool hooks for each.

### Graph Visualization UI

If you downloaded the `ui` variant:

```bash
codebase-memory-mcp --ui=true --port=9749
```

Open `http://localhost:9749` in your browser. The UI runs as a background thread alongside the MCP server — it's available whenever your agent is connected.

### Auto-Index

Enable automatic indexing on MCP session start:

```bash
codebase-memory-mcp config set auto_index true
```

When enabled, new projects are indexed automatically on first connection. Previously-indexed projects are registered with the background watcher for ongoing git-based change detection. Configurable file limit: `config set auto_index_limit 50000`.

### Keeping Up to Date

```bash
codebase-memory-mcp update
```

The MCP server also checks for updates on startup and notifies on the first tool call if a newer release is available.

### Uninstall

```bash
codebase-memory-mcp uninstall
```

Removes all agent configs, skills, hooks, and instructions. Does not remove the binary or SQLite databases.

## Features

### Graph & analysis
- **Architecture overview**: `get_architecture` returns languages, packages, entry points, routes, hotspots, boundaries, layers, and clusters in a single call
- **Architecture Decision Records**: `manage_adr` persists architectural decisions across sessions
- **Louvain community detection**: Discovers functional modules by clustering call edges
- **Git diff impact mapping**: `detect_changes` maps uncommitted changes to affected symbols with risk classification
- **Call graph**: Resolves function calls across files and packages (import-aware, type-inferred)
- **Dead code detection**: Finds functions with zero callers, excluding entry points
- **Cypher-like queries**: `MATCH (f:Function)-[:CALLS]->(g) WHERE f.name = 'main' RETURN g.name`

### Search
- **Semantic search** (`semantic_query`): vector search across the entire graph, powered by bundled Nomic `nomic-embed-code` embeddings (40K tokens, 768d int8) compiled into the binary — no API key, no Ollama, no Docker. 11-signal combined scoring (TF-IDF, RRI, API/Type/Decorator signatures, AST profiles, data flow, Halstead-lite, MinHash, module proximity, graph diffusion).
- **BM25 full-text search** via SQLite FTS5 with `cbm_camel_split` tokenizer (camelCase / snake_case aware)
- **Structural search** (`search_graph`): regex name patterns, label filters, min/max degree, file scoping
- **Code search** (`search_code`): graph-augmented grep over indexed files only

### Cross-service linking
- **HTTP** route ↔ call-site matching with confidence scoring
- **gRPC, GraphQL, tRPC** service detection with protobuf Route extraction
- **Channel detection** (`EMITS` / `LISTENS_ON`) for Socket.IO, EventEmitter, and generic pub-sub patterns across 8 languages with constant resolution

### Cross-repo intelligence
- **`CROSS_*` edges** link nodes across multiple repos indexed under the same store
- **Multi-galaxy 3D UI layout** for cross-repo architecture visualization
- **Cross-repo architecture summary** combining services, routes, and dependencies across the indexed fleet

### Edge types (selected)
- `CALLS`, `IMPORTS`, `DEFINES`, `IMPLEMENTS`, `INHERITS`
- `HTTP_CALLS`, `ASYNC_CALLS` (cross-service)
- `EMITS`, `LISTENS_ON` (channels)
- `DATA_FLOWS` with arg-to-param mapping + field access chains
- `SIMILAR_TO` (MinHash + LSH near-clone detection, Jaccard scored)
- `SEMANTICALLY_RELATED` (vocabulary-mismatch, same-language, score ≥ 0.80)

### Indexing pipeline
- **158 vendored tree-sitter grammars** compiled into the binary
- **Generic package / module resolution** — bare specifiers like `@myorg/pkg`, `github.com/foo/bar`, `use my_crate::foo` resolved via manifest scanning (`package.json`, `go.mod`, `Cargo.toml`, `pyproject.toml`, `composer.json`, `pubspec.yaml`, `pom.xml`, `build.gradle`, `mix.exs`, `*.gemspec`)
- **Infrastructure-as-code indexing** — Dockerfiles, Kubernetes manifests, Kustomize overlays as graph nodes
- **[Hybrid LSP semantic type resolution](#hybrid-lsp)** for Python, TypeScript / JavaScript / JSX / TSX, PHP, C#, Go, C, C++, Java, Kotlin, and Rust — a lightweight C implementation of language type-resolution algorithms, structurally inspired by and compatible with major language servers including tsserver / typescript-go, pyright, gopls, Roslyn, Eclipse JDT, and rust-analyzer (parameter binding, return-type inference, generic substitution, JSX component dispatch, JSDoc inference for plain JS files, namespace + trait + late-static-binding resolution for PHP, file-scoped namespaces + records + LINQ method syntax for C#, class-hierarchy + overload + lambda resolution for Java, extension-function + scope-function resolution for Kotlin, trait-method + UFCS resolution for Rust)
- **RAM-first pipeline**: LZ4 compression, in-memory SQLite, single dump at end. Memory released after.

### Distribution & operation
- **Single static binary, zero infrastructure**: SQLite-backed, persists to `~/.cache/codebase-memory-mcp/`
- **Auto-sync**: Background watcher detects file changes and re-indexes automatically
- **Route nodes**: REST endpoints are first-class graph entities
- **CLI mode**: `codebase-memory-mcp cli search_graph '{"name_pattern": ".*Handler.*"}'`
- **Available on**: npm, PyPI, Homebrew, Scoop, Winget, Chocolatey, AUR, `go install`

## Team-Shared Graph Artifact

Commit a single compressed file to your repo and your teammates skip the reindex.

`.codebase-memory/graph.db.zst` is a zstd-compressed snapshot of the knowledge graph that lives next to your source. When you index, the artifact is written or refreshed; when a teammate clones the repo and runs `codebase-memory-mcp` for the first time, the artifact is decompressed and incremental indexing fills in their local diff.

- **Format**: SQLite database, indexes stripped, `VACUUM INTO` compacted, then zstd 1.5.7 compressed (8–13:1 ratio typical)
- **Two tiers**:
  - **Best** (`zstd -9` + index strip + `VACUUM INTO`) — written on explicit `index_repository`
  - **Fast** (`zstd -3`) — written by the watcher for low-latency incremental updates
- **Bootstrap**: when no local DB exists but the artifact is present, `index_repository` imports the artifact first, then runs incremental indexing — avoiding the full reindex cost
- **No merge pain**: a `.gitattributes` line with `merge=ours` is auto-created on first export, so concurrent edits don't produce conflicts on the binary artifact
- **Optional**: never committed unless you want it. Add `.codebase-memory/` to `.gitignore` if you prefer everyone to reindex from scratch.

The result is similar in spirit to graphify's `graphify-out/` directory, but as a single compressed file with explicit two-tier export, integrity-checked import, and zero merge friction.

## How It Works

codebase-memory-mcp is a **structural analysis backend** — it builds and queries the knowledge graph. It does **not** include an LLM. Instead, it relies on your MCP client (Claude Code, or any MCP-compatible agent) to be the intelligence layer.

```
You: "what calls ProcessOrder?"

Agent calls: trace_path(function_name="ProcessOrder", direction="inbound")

codebase-memory-mcp: executes graph query, returns structured results

Agent: presents the call chain in plain English
```

**Why no built-in LLM?** Other code graph tools embed an LLM for natural language → graph query translation. This means extra API keys, extra cost, and another model to configure. With MCP, the agent you're already talking to *is* the query translator.

## Performance

Benchmarked on Apple M3 Pro:

| Operation | Time | Notes |
|-----------|------|-------|
| **Linux kernel full index** | **3 min** | 28M LOC, 75K files → 4.81M nodes, 7.72M edges |
| Linux kernel fast index | 1m 12s | 1.88M nodes |
| Django full index | ~6s | 49K nodes, 196K edges |
| Cypher query | <1ms | Relationship traversal |
| Name search (regex) | <10ms | SQL LIKE pre-filtering |
| Dead code detection | ~150ms | Full graph scan with degree filtering |
| Trace call path (depth=5) | <10ms | BFS traversal |

**RAM-first pipeline**: All indexing runs in memory (LZ4 HC compressed read, in-memory SQLite, single dump at end). Memory is released back to the OS after indexing completes.

**Token efficiency**: Five structural queries consumed ~3,400 tokens via codebase-memory-mcp versus ~412,000 tokens via file-by-file grep exploration — a **99.2% reduction**.

## Installation

### Pre-built Binaries

| Platform | Standard | With Graph UI |
|----------|----------|---------------|
| macOS (Apple Silicon) | `codebase-memory-mcp-darwin-arm64.tar.gz` | `codebase-memory-mcp-ui-darwin-arm64.tar.gz` |
| macOS (Intel) | `codebase-memory-mcp-darwin-amd64.tar.gz` | `codebase-memory-mcp-ui-darwin-amd64.tar.gz` |
| Linux (x86_64) | `codebase-memory-mcp-linux-amd64.tar.gz` | `codebase-memory-mcp-ui-linux-amd64.tar.gz` |
| Linux (ARM64) | `codebase-memory-mcp-linux-arm64.tar.gz` | `codebase-memory-mcp-ui-linux-arm64.tar.gz` |
| Windows (x86_64) | `codebase-memory-mcp-windows-amd64.zip` | `codebase-memory-mcp-ui-windows-amd64.zip` |

Every release includes `checksums.txt` with SHA-256 hashes. All binaries are statically linked — no shared library dependencies.

> **Windows note**: SmartScreen may show a warning for unsigned software. Click **"More info"** → **"Run anyway"**. Verify integrity with `checksums.txt`.

### Setup Scripts

<details>
<summary>Automated download + install</summary>

**macOS / Linux:**

```bash
curl -fsSL https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup.sh | bash
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/DeusData/codebase-memory-mcp/main/scripts/setup-windows.ps1 | iex
```

</details>

### AUR (Arch Linux)

```bash
yay -S codebase-memory-mcp-bin
```

```bash
paru -S codebase-memory-mcp-bin
```

The `codebase-memory-mcp-bin` package is available at: https://aur.archlinux.org/packages/codebase-memory-mcp-bin

### Install via Claude Code

```
You: "Install this MCP server: https://github.com/DeusData/codebase-memory-mcp"
```

### Build from Source

<details>
<summary>Prerequisites: C compiler + zlib</summary>

| Requirement | Check | Install |
|-------------|-------|---------|
| **C compiler** (gcc or clang) | `gcc --version` or `clang --version` | macOS: `xcode-select --install`, Linux: `apt install build-essential` |
| **C++ compiler** | `g++ --version` or `clang++ --version` | Same as above |
| **zlib** | — | macOS: included, Linux: `apt install zlib1g-dev` |
| **Git** | `git --version` | Pre-installed on most systems |

</details>

```bash
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp
scripts/build.sh                    # standard binary
scripts/build.sh --with-ui          # with graph visualization
# Binary at: build/c/codebase-memory-mcp
```

### Manual MCP Configuration

<details>
<summary>If you prefer not to use the install command</summary>

Add to `~/.claude/.mcp.json` (global) or project `.mcp.json`:

```json
{
  "mcpServers": {
    "codebase-memory-mcp": {
      "command": "/path/to/codebase-memory-mcp",
      "args": []
    }
  }
}
```

Restart your agent. Verify with `/mcp` — you should see `codebase-memory-mcp` with 14 tools.

</details>

## Multi-Agent Support

`install` auto-detects and configures all installed agents:

| Agent | MCP Config | Instructions | Hooks |
|-------|-----------|-------------|-------|
| Claude Code | `.claude/.mcp.json` | 4 Skills | PreToolUse (Grep/Glob graph augment, non-blocking) |
| Codex CLI | `.codex/config.toml` | `.codex/AGENTS.md` | SessionStart reminder |
| Gemini CLI | `.gemini/settings.json` | `.gemini/GEMINI.md` | BeforeTool (grep reminder) + SessionStart reminder |
| Zed | `settings.json` (JSONC) | — | — |
| OpenCode | `opencode.json` | `AGENTS.md` | — |
| Antigravity | `.gemini/config/mcp_config.json` (shared) | `antigravity-cli/AGENTS.md` | SessionStart reminder |
| Aider | — | `CONVENTIONS.md` | — |
| KiloCode | `mcp_settings.json` | `~/.kilocode/rules/` | — |
| VS Code | `Code/User/mcp.json` | — | — |
| OpenClaw | `openclaw.json` | — | — |
| Kiro | `.kiro/settings/mcp.json` | — | — |

**Hooks are structurally non-blocking** (exit code 0, every failure path).
For Claude Code, the `PreToolUse` hook intercepts `Grep`/`Glob` (never `Read` —
gating `Read` breaks the read-before-edit invariant) and, when the search
token matches indexed symbols, injects them as `additionalContext` via
`search_graph` so the agent gets structured context alongside its normal
search results. For Codex, Gemini CLI, and Antigravity, a `SessionStart` hook
injects a one-line code-discovery reminder as session context (Gemini CLI also
keeps its `BeforeTool` reminder).
The installed Claude shim file is named `cbm-code-discovery-gate` for
backward compatibility with existing installs; despite the legacy name it
never gates and never blocks.

## CLI Mode

Every MCP tool can be invoked from the command line:

```bash
codebase-memory-mcp cli index_repository '{"repo_path": "/path/to/repo"}'
codebase-memory-mcp cli search_graph '{"name_pattern": ".*Handler.*", "label": "Function"}'
codebase-memory-mcp cli trace_path '{"function_name": "Search", "direction": "both"}'
codebase-memory-mcp cli query_graph '{"query": "MATCH (f:Function) RETURN f.name LIMIT 5"}'
codebase-memory-mcp cli list_projects
codebase-memory-mcp cli --raw search_graph '{"label": "Function"}' | jq '.results[].name'
```

## MCP Tools

### Indexing

| Tool | Description |
|------|-------------|
| `index_repository` | Index a repository into the graph. Auto-sync keeps it fresh after that. |
| `list_projects` | List all indexed projects with node/edge counts. |
| `delete_project` | Remove a project and all its graph data. |
| `index_status` | Check indexing status of a project. |

### Querying

| Tool | Description |
|------|-------------|
| `search_graph` | Structured search by label, name pattern, file pattern, degree filters. Pagination via limit/offset. |
| `trace_path` | BFS traversal — who calls a function and what it calls (alias: `trace_call_path`). Depth 1-5. |
| `detect_changes` | Map git diff to affected symbols + blast radius with risk classification. |
| `query_graph` | Execute Cypher-like graph queries (read-only). |
| `get_graph_schema` | Node/edge counts, relationship patterns, property definitions per label. Run this first. |
| `get_code_snippet` | Read source code for a function by qualified name. |
| `get_architecture` | Codebase overview: languages, packages, routes, hotspots, clusters, ADR. |
| `search_code` | Grep-like text search within indexed project files. |
| `manage_adr` | CRUD for Architecture Decision Records. |
| `ingest_traces` | Ingest runtime traces to validate HTTP_CALLS edges. |

## Graph Data Model

### Node Labels

`Project`, `Package`, `Folder`, `File`, `Module`, `Class`, `Function`, `Method`, `Interface`, `Enum`, `Type`, `Route`, `Resource`

### Edge Types

`CONTAINS_PACKAGE`, `CONTAINS_FOLDER`, `CONTAINS_FILE`, `DEFINES`, `DEFINES_METHOD`, `IMPORTS`, `CALLS`, `HTTP_CALLS`, `ASYNC_CALLS`, `IMPLEMENTS`, `HANDLES`, `USAGE`, `CONFIGURES`, `WRITES`, `MEMBER_OF`, `TESTS`, `USES_TYPE`, `FILE_CHANGES_WITH`

### Qualified Names

`get_code_snippet` uses qualified names: `<project>.<path_parts>.<name>`. Use `search_graph` to discover them first.

### Supported Cypher (openCypher read subset)

`query_graph` is a read-only openCypher subset:

- **Clauses**: `MATCH`, `OPTIONAL MATCH`, multiple `MATCH`, `WHERE`, `WITH` (+ `WITH … WHERE`), `RETURN`, `ORDER BY`, `SKIP`, `LIMIT`, `DISTINCT`, `UNWIND`, `UNION` / `UNION ALL`, `CASE`.
- **Patterns**: labelled nodes, label alternation `(n:A|B)`, relationship types/direction, variable-length paths `[*1..3]`, inline property maps.
- **WHERE**: `= <> < <= > >=`, `AND/OR/XOR/NOT`, `IN`, `CONTAINS`, `STARTS WITH`, `ENDS WITH`, `IS [NOT] NULL`, regex `=~`, label test `n:Label`, and `EXISTS { (n)-[:TYPE]->() }` (single-hop existence — great for dead-code, e.g. `WHERE NOT EXISTS { (f)<-[:CALLS]-() }`).
- **Aggregates**: `count` (+`DISTINCT`), `sum`, `avg`, `min`, `max`, `collect`.
- **Functions**: `labels`, `type`, `id`, `keys`, `properties`; `toLower/toUpper/toString/toInteger/toFloat/toBoolean`; `size`, `length`, `trim/ltrim/rtrim`, `reverse`; `coalesce`, `substring`, `replace`, `left`, `right`.

Anything outside this subset (write/`MERGE`/`CALL` clauses, unsupported functions, list/map literals, comprehensions, path functions, parameters) **fails with a clear `unsupported …` error** rather than returning empty results.

## Ignoring Files

Layered: hardcoded patterns (`.git`, `node_modules`, etc.) → `.gitignore` hierarchy → `.cbmignore` (project-specific, gitignore syntax). Symlinks are always skipped.

## Configuration

```bash
codebase-memory-mcp config list                          # show all settings
codebase-memory-mcp config set auto_index true           # auto-index on session start
codebase-memory-mcp config set auto_index_limit 50000    # max files for auto-index
codebase-memory-mcp config reset auto_index              # reset to default
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CBM_CACHE_DIR` | `~/.cache/codebase-memory-mcp` | Override the database storage directory. All project indexes and config are stored here. |
| `CBM_DIAGNOSTICS` | `false` | Set to `1` or `true` to enable periodic diagnostics output to `/tmp/cbm-diagnostics-<pid>.json`. |
| `CBM_DOWNLOAD_URL` | *(GitHub releases)* | Override the download URL for updates. Used for testing or self-hosted deployments. |
| `CBM_LOG_LEVEL` | `info` | Set the minimum log level. Accepted values (case-insensitive): `debug`, `info`, `warn`, `error`, `none` — or their numeric equivalents `0`–`4` matching the internal enum. Logs go to stderr; stdout is reserved for MCP JSON-RPC. |
| `CBM_WORKERS` | *(detected)* | Override the parallel-indexing worker count returned by `cbm_default_worker_count`. Useful inside containers where `sysconf(_SC_NPROCESSORS_ONLN)` reports host CPUs rather than the cgroup's effective quota. Range 1–256; invalid values are ignored with a warning. |
| `CBM_DUMP_VERIFY_MIN_RATIO` | `0.5` | After indexing, compare persisted SQLite node count to the in-memory dump count. When persisted nodes fall below this fraction of committed nodes (and committed > 50), `index_repository` returns `status:"degraded"` instead of silent `indexed`. Range 0–1; set `0` to disable. Invalid values are ignored with a warning. |

```bash
# Store indexes in a custom directory
export CBM_CACHE_DIR=~/my-projects/cbm-data
```

## Custom File Extensions

Map additional file extensions to supported languages via JSON config files. Useful for framework-specific extensions like `.blade.php` (Laravel) or `.mjs` (ES modules).

**Per-project** (in your repo root):
```json
// .codebase-memory.json
{"extra_extensions": {".blade.php": "php", ".mjs": "javascript"}}
```

**Global** (applies to all projects):
```json
// ~/.config/codebase-memory-mcp/config.json  (or $XDG_CONFIG_HOME/...)
{"extra_extensions": {".twig": "html", ".phtml": "php"}}
```

Project config overrides global for conflicting extensions. Unknown language values are silently skipped. Missing config files are ignored.

## Persistence

SQLite databases stored at `~/.cache/codebase-memory-mcp/`. Persists across restarts (WAL mode, ACID-safe). To reset: `rm -rf ~/.cache/codebase-memory-mcp/`.

## Troubleshooting

| Problem | Fix |
|---------|-----|
| `/mcp` doesn't show the server | Check `.mcp.json` path is absolute. Restart agent. Test: `echo '{}' \| /path/to/binary` should output JSON. |
| `index_repository` fails | Pass absolute path: `index_repository(repo_path="/absolute/path")` |
| `trace_path` returns 0 results | Use `search_graph(name_pattern=".*PartialName.*")` first to find the exact name. |
| Queries return wrong project results | Add `project="name"` parameter. Use `list_projects` to see names. |
| Binary not found after install | Add to PATH: `export PATH="$HOME/.local/bin:$PATH"` |
| UI not loading | Ensure you downloaded the `ui` variant and ran `--ui=true`. Check `http://localhost:9749`. |

## Hybrid LSP

**Semantic type resolution beyond tree-sitter.**

Tree-sitter alone gives a syntactic AST. That handles naming, structure, and call sites well, but it can't tell you that `user.profile.display_name()` resolves to `Profile.display_name` declared three modules away — tree-sitter doesn't track imports, generics, inheritance, or stdlib types.

codebase-memory-mcp ships a **lightweight C implementation of language type-resolution algorithms, structurally inspired by and compatible with major language servers** (tsserver / typescript-go, pyright, gopls, Roslyn, Eclipse JDT, rust-analyzer), embedded directly into the static binary. No language server process, no per-project setup, no API key. We call this layer **Hybrid LSP**: it runs alongside tree-sitter on every parse and refines `CALLS`, `USAGE`, and `RESOLVED_CALLS` edges with type information, so the resulting graph mirrors what an IDE "Go to Definition" would resolve.

**Languages with full Hybrid LSP:**

| Language | What it handles |
|----------|-----------------|
| **Python** *(new in v0.7.0)* | imports + dotted submodule walks, dataclasses, `Self` return types, generics, `@property`, `match/case` class patterns, SQLAlchemy 2.0 `Mapped[T]`, Pydantic `BaseModel`, `typing.Annotated` / `ClassVar` / `Final` / `InitVar`, async/await, classmethod/staticmethod, narrowing (`isinstance` / `is not None` / walrus), `typing.cast` / `assert_type`, common stdlib (logging, pathlib, json, functools). Target ~95% resolution on idiomatic code. |
| **TypeScript / JavaScript / JSX / TSX** | generics, JSX component dispatch, JSDoc inference for plain JS, `.d.ts` declarations, module re-exports, method chaining via return-type propagation, per-file overlay chained to a shared cross-file registry |
| **PHP** *(new in v0.7.0)* | namespaces, traits, late-static-binding, PHPDoc inference, parameter binding, return-type inference |
| **C#** *(new in v0.7.0)* | global usings, file-scoped namespaces, records (incl. C# 12 primary constructors), LINQ method syntax, `async Task<T>` / `ValueTask<T>` unwrap, generic methods, `this` / `base` dispatch, `var` inference, common BCL stdlib |
| **Go** *(sharpened in v0.7.0)* | pre-built per-package cross-file registry, generics, embedded structs, interface satisfaction, package-aware import resolution |
| **C / C++** *(sharpened in v0.7.0)* | pre-built per-language cross-file registry shared across C and C++; C side handles macros + `typedef` chains + header-vs-source linking; C++ side handles templates, namespaces, `auto` inference, and method resolution via class hierarchy |
| **Java** *(new in v0.8.0)* | imports (single-type, on-demand, static), class hierarchies with `this` / `super` dispatch, generics, annotations, overload matching by arity and parameter types, lambdas / method references bound to functional interfaces, field-type inference, common JDK stdlib |
| **Kotlin** *(new in v0.8.0)* | imports + same-package resolution, classes / objects / companion objects, extension functions, data classes, nullable-type unwrapping, scope functions (`let` / `apply` / `run` / `also` / `with`), infix calls, common stdlib |
| **Rust** *(new in v0.8.0)* | `use` declarations + module paths, `impl` blocks and trait methods, struct fields, generics with trait bounds, operator-trait desugaring, derive-macro method synthesis, UFCS static paths, common std prelude |

**Two-layer architecture:**

1. **Tree-sitter pass** — fast, syntactic, runs for every one of the 158 languages. Extracts definitions, calls, imports.
2. **Hybrid LSP pass** — type-aware, runs above the tree-sitter pass per-language. Refines call edges using the import graph plus a per-file or pre-built cross-file definition registry. Languages without a Hybrid LSP pass yet fall back to textual resolution, so you always get *some* answer.

The result is a knowledge graph accurate enough to drive `trace_path` across packages, inheritance hierarchies, and stdlib calls — without paying for a language server process per project.

## Language Support

158 languages, all parsed via vendored tree-sitter grammars compiled into the binary. Benchmarked against 64 real open-source repositories (78 to 49K nodes):

| Tier | Score | Languages |
|------|-------|-----------|
| **Excellent** (>= 90%) | | Lua, Kotlin, C++, Perl, Objective-C, Groovy, C, Bash, Zig, Swift, CSS, YAML, TOML, HTML, SCSS, HCL, Dockerfile |
| **Good** (75-89%) | | Python, TypeScript, TSX, Go, Rust, Java, R, Dart, JavaScript, Erlang, Elixir, Scala, Ruby, PHP, C#, SQL |
| **Functional** (< 75%) | | OCaml, Haskell |

Also supported (not yet benchmarked): Ada, Agda, Apex, Assembly (NASM), Astro, AWK, Beancount, BibTeX, Bicep, Bitbake, Blade, Cairo, Cap'n Proto, Clojure, CMake, COBOL, Common Lisp, Crystal, CSV, CUDA, D, Devicetree, Diff, .env, Elm, Emacs Lisp, F#, Fennel, Fish, FORM, Fortran, FunC, GDScript, .gitattributes, .gitignore, Gleam, GLSL, GN, Go module, Go template, GraphQL, Hare, HLSL, Hyprlang, INI, ISPC, Janet, Jinja2, JSDoc, JSON, JSON5, Jsonnet, Julia, Just, Kconfig, KDL, Lean 4, Linker Script, Liquid, LLVM IR, Luau, Magma, Makefile, Markdown, MATLAB, Mermaid, Meson, Move, Nickel, Nim, Nix, Odin, Pascal, Pkl, PO (gettext), Pony, PowerShell, Prisma, .properties, Protobuf, Puppet, PureScript, Racket, Regex, requirements.txt, ReScript, RON, reStructuredText, Scheme, Slang, Smali, Smithy, Solidity, SOQL, SOSL, Squirrel, SSH config, Starlark, Svelte, Sway, SystemVerilog, TableGen, Tcl, Teal, Templ, Thrift, TLA+, Typst, Verilog, VHDL, Vim script, Vue, WGSL, WIT, Wolfram, XML, Zsh.

## Architecture

```
src/
  main.c              Entry point (MCP stdio server + CLI + install/update/config)
  mcp/                MCP server (14 tools, JSON-RPC 2.0, session detection, auto-index)
  cli/                Install/uninstall/update/config (10 agents, hooks, instructions)
  store/              SQLite graph storage (nodes, edges, traversal, search, Louvain)
  pipeline/           Multi-pass indexing (structure → definitions → calls → HTTP links → config → tests)
  cypher/             Cypher query lexer, parser, planner, executor
  discover/           File discovery (.gitignore, .cbmignore, symlink handling)
  watcher/            Background auto-sync (git polling, adaptive intervals)
  traces/             Runtime trace ingestion
  ui/                 Embedded HTTP server + 3D graph visualization
  foundation/         Platform abstractions (threads, filesystem, logging, memory)
internal/cbm/         Vendored tree-sitter grammars (158 languages) + AST extraction engine
```

## Security

Every release binary is verified through a multi-layer pipeline before publication:

- **VirusTotal** — all binaries scanned by 70+ antivirus engines (zero detections required to publish)
- **SLSA Level 3** — cryptographic build provenance generated by GitHub Actions; verify with `gh attestation verify <file> --repo DeusData/codebase-memory-mcp`
- **Sigstore cosign** — keyless signatures on all artifacts; bundles included in every release
- **SHA-256 checksums** — `checksums.txt` published with every release; verified by both install scripts before extraction
- **CodeQL SAST** — blocks release pipeline if any open alerts remain
- **Zero runtime dependencies** — no transitive supply chain; all libraries vendored at compile time

### v0.7.0 VirusTotal scans

| Binary | SHA-256 | VirusTotal |
|--------|---------|-----------|
| `linux-amd64` | `8e12bb2d6ead7f20a6d3...` | [0/72 ✅](https://www.virustotal.com/gui/file/8e12bb2d6ead7f20a6d3bf2be1e51f978c38acce810f0734f510d134b039d152/detection) |
| `linux-arm64` | `10f7136bfbf3950c6b2a...` | [0/72 ✅](https://www.virustotal.com/gui/file/10f7136bfbf3950c6b2a1a950bbf85e88b97ee55ab00b4dfbc2a5e9c2ede8672/detection) |
| `darwin-arm64` | `7062a7408906344bf4f8...` | [0/72 ✅](https://www.virustotal.com/gui/file/7062a7408906344bf4f835e9580048af85d12dd2b7cec0edf869df93ad9a0592/detection) |
| `darwin-amd64` | `28c6d640e1a0ac7bfcab...` | [0/72 ✅](https://www.virustotal.com/gui/file/28c6d640e1a0ac7bfcab5094c2186eced5264a20dcdffcb4455a1b28c5df2171/detection) |
| `windows-amd64` | `9c3ddcf78368fd4fa891...` | [0/72 ✅](https://www.virustotal.com/gui/file/9c3ddcf78368fd4fa89156a553641bf1e03640b4fb6dd29a12c84aa5bc98cd86/detection) |

Scan links for every release are also included in the GitHub Release notes automatically.

## License

MIT
