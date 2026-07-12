# codebase-memory-mcp

[![npm](https://img.shields.io/npm/v/codebase-memory-mcp?style=flat&color=blue)](https://www.npmjs.com/package/codebase-memory-mcp)
[![GitHub Release](https://img.shields.io/github/v/release/DeusData/codebase-memory-mcp?style=flat&color=blue)](https://github.com/DeusData/codebase-memory-mcp/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/DeusData/codebase-memory-mcp/blob/main/LICENSE)
[![Platform](https://img.shields.io/badge/macOS_%7C_Linux_%7C_Windows-supported-lightgrey)](https://github.com/DeusData/codebase-memory-mcp/releases/latest)

**The fastest and most efficient code intelligence engine for AI coding agents.** Full-indexes an average repository in milliseconds, the Linux kernel (28M LOC, 75K files) in 3 minutes. Answers structural queries in under 1ms. Ships as a single static binary — this package downloads and runs it automatically.

High-quality parsing through [tree-sitter](https://tree-sitter.github.io/tree-sitter/) AST analysis across 159 languages — producing a persistent knowledge graph of functions, classes, call chains, HTTP routes, and cross-service links. 14 MCP tools. Zero dependencies. Plug and play across 43 automatic/conditional client surfaces.

## Installation

```bash
npm install -g codebase-memory-mcp
```

The binary for your platform is downloaded automatically at install time. Then configure your coding agents:

```bash
codebase-memory-mcp install
```

Restart your agent. Say **"Index this project"** — done.

## Why codebase-memory-mcp

- **Extreme indexing speed** — Linux kernel (28M LOC, 75K files) in 3 minutes. RAM-first pipeline with LZ4 compression and in-memory SQLite.
- **Plug and play** — single static binary for macOS (arm64/amd64), Linux (arm64/amd64), and Windows (amd64). No Docker, no runtime dependencies, no API keys.
- **159 languages** — vendored tree-sitter grammars compiled into the binary. Nothing to install, nothing that breaks.
- **120x fewer tokens** — 5 structural queries: ~3,400 tokens vs ~412,000 via file-by-file search.
- **43 supported automatic/conditional client surfaces** — `install` configures the appropriate MCP, durable-context, and documented hook surfaces without widening client permissions.
- **Detected automatically (37)** — Claude Code, Codex CLI, Gemini CLI, Zed, OpenCode, Antigravity, Aider, KiloCode, VS Code, Cursor, Windsurf, Augment / Auggie, OpenClaw, Kiro, Junie, Hermes, OpenHands, Cline, Warp, Qwen Code, GitHub Copilot CLI, Factory Droid, Crush, Goose, Mistral Vibe, Qoder CLI, Kimi Code CLI, GitLab Duo CLI, Rovo Dev CLI, Amp, Devin CLI / Local, Tabnine, Amazon Q Developer IDE, CodeBuddy Code CLI, IBM Bob Shell, Pochi, and Pi.
- **Conditional or explicit (6)** — Continue / cn, Visual Studio, TRAE, Roo Code, IBM Bob IDE, and Sourcegraph Cody. Bob IDE is touched only when `~/.bob/mcp.json` already exists.
- **New documented adapters** — CodeBuddy uses `~/.codebuddy/.mcp.json` while preserving active older files; Bob Shell uses `~/.bob/mcp_settings.json`; Pochi uses the `mcp` section in `~/.pochi/config.jsonc`; Amazon Q Developer IDE defaults to `~/.aws/amazonq/default.json` while preserving either documented alternative.
- **Lifecycle hooks stay conservative** — Kimi uses `UserPromptSubmit`; on macOS/Linux, GitLab Duo gets a fail-open user `SessionStart`, while Devin gets `UserPromptSubmit`, `PostCompaction`, and a deduplicated `SessionStart` when Claude does not already provide it. Qoder, GitLab Duo, Devin, and Factory hooks are withheld on Windows without a documented shell/executor contract. Cline's auto-activating file hooks are withheld because their context output is not reliably consumed, CodeBuddy beta hooks are not auto-installed, and Cursor context hooks remain withheld.
- **Subagent access is explicit** — Claude, Gemini, Kiro, Qwen, CodeBuddy, KiloCode, Mistral Vibe, Qoder, Junie, and Factory get documented graph profiles with the narrowest tool/server filters their schemas support. KiloCode and Vibe enumerate read-only query tools rather than using server wildcards. Cursor, Rovo, Pochi, and Cline use explicit parent handoff where child MCP is unavailable or unsafe; IBM Bob receives no invented hook or agent.
- **Manual, UI, cloud, or repository-managed (not counted)** — Qodo, Warp MCP, JetBrains AI/ACP, GitHub Copilot coding agent, Jules, CodeRabbit, Replit, BLACKBOX AI, Plandex, and SWE-agent. Warp is counted above for its detected skill installation; its MCP connection remains manual.
- **14 MCP tools** — search, trace, architecture, impact analysis, Cypher queries, dead code detection, cross-service HTTP linking, ADR management, and more.

## Supported Platforms

| OS      | Architecture |
|---------|-------------|
| macOS   | arm64, amd64 |
| Linux   | arm64, amd64 |
| Windows | amd64 |

## Usage

```bash
codebase-memory-mcp install          # configure all detected coding agents
codebase-memory-mcp --version
codebase-memory-mcp --help
codebase-memory-mcp update           # update to latest release
codebase-memory-mcp uninstall        # remove agent configs
```

### CLI Mode

Every MCP tool is also available directly from the command line:

```bash
codebase-memory-mcp cli index_repository '{"repo_path": "/path/to/repo"}'
codebase-memory-mcp cli search_graph '{"name_pattern": ".*Handler.*", "label": "Function"}'
codebase-memory-mcp cli trace_call_path '{"function_name": "main", "direction": "both"}'
codebase-memory-mcp cli get_architecture '{}'
```

## MCP Tools

| Category | Tools |
|----------|-------|
| **Indexing** | `index_repository`, `list_projects`, `delete_project`, `index_status` |
| **Querying** | `search_graph`, `trace_call_path`, `detect_changes`, `query_graph` |
| **Analysis** | `get_architecture`, `get_graph_schema`, `get_code_snippet`, `search_code` |
| **Advanced** | `manage_adr`, `ingest_traces` |

## Performance

Benchmarked on Apple M3 Pro:

| Operation | Time |
|-----------|------|
| Linux kernel full index (28M LOC, 75K files) | 3 min |
| Django full index | ~6s |
| Cypher query | <1ms |
| Trace call path (depth=5) | <10ms |

## Full Documentation

See [github.com/DeusData/codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp) for the full README including all MCP tools, configuration options, graph data model, and language support details.

## License

MIT
