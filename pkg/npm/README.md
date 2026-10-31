# codebase-memory-mcp

[![npm](https://img.shields.io/npm/v/codebase-memory-mcp?style=flat&color=blue)](https://www.npmjs.com/package/codebase-memory-mcp)
[![GitHub Release](https://img.shields.io/github/v/release/DeusData/codebase-memory-mcp?style=flat&color=blue)](https://github.com/DeusData/codebase-memory-mcp/releases/latest)
[![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/DeusData/codebase-memory-mcp/blob/main/LICENSE)
[![Platform](https://img.shields.io/badge/macOS_%7C_Linux_%7C_Windows-supported-lightgrey)](https://github.com/DeusData/codebase-memory-mcp/releases/latest)

**Local code intelligence for developers and AI agents.** CBM connects source code, documentation and infrastructure in a persistent knowledge graph. Explore through MCP, the CLI or CBM Lens to find code, trace dependencies and deployments, and understand what a change affects.

This npm wrapper downloads, verifies and caches the native executable, its authenticated integration asset and the content-addressed Lens frontend pack. Node.js manages installation and launch; the graph is processed locally without a hosted service or API key. The installer offers 45 client integration targets, with capabilities that vary by client.

## Installation

```bash
npm install -g codebase-memory-mcp
```

The runtime set for your platform is downloaded automatically at install time. There is one composition per platform and the graph UI is always included — the former `CBM_VARIANT=ui` opt-in is obsolete.

For testing, `CBM_DOWNLOAD_URL` overrides the download base URL (default `https://github.com/DeusData/codebase-memory-mcp/releases/download/v<version>`): the archive and `checksums.txt` are fetched from `<CBM_DOWNLOAD_URL>/<file>`. It must be `https://`, or plain `http://` to exactly `localhost`, `127.0.0.1`, or `[::1]` (optional port; no proxy, no redirects).

Then configure your coding agents:

```bash
codebase-memory-mcp install
```

Restart your agent and ask it to **"Index this project"**. Use `codebase-memory-mcp daemon start --open` to open CBM Lens. Supported integrations also install graph instructions, skills, agent profiles and context hooks; consult the full client matrix for differences.

## Why codebase-memory-mcp

- **Connected repository knowledge** — source symbols and calls, documentation sections and references, configuration, infrastructure and cross-service links.
- **Plug and play** — verified native runtime sets for macOS (arm64/amd64), Linux (arm64/amd64), and Windows (arm64/amd64). No Docker or API keys; Node.js owns package download, cache repair, and launch.
- **Broad language and format support** — bundled parsers, with deeper semantic resolution for supported language families. See the [language reference](https://github.com/DeusData/codebase-memory-mcp#language-support) for coverage.
- **Focused context** — retrieve symbols, relationships and bounded source excerpts instead of repeatedly reading whole files. Savings depend on the task and workflow.
- **CBM Lens** — explore source, architecture, call paths, decisions and index coverage in a local browser workspace.
- **Smart CI** — experimental change-aware test selection for supported C/C++ test runners, using graph relationships and admitted per-test coverage. Missing evidence broadens execution. See the [Smart CI guide](https://github.com/DeusData/codebase-memory-mcp/blob/main/docs/SMART_CI.md).
- **45 supported automatic/conditional client surfaces** — `install` configures the appropriate MCP, durable-context, and documented hook surfaces without widening client permissions.
- **Detected automatically (39)** — Claude Code, Codex CLI, Gemini CLI, Zed, OpenCode, Antigravity, Aider, KiloCode, VS Code, Cursor, Windsurf, Augment / Auggie, OpenClaw, Kiro, Junie, Hermes, OpenHands, Cline, Warp, Qwen Code, GitHub Copilot CLI, Factory Droid, Crush, Goose, Mistral Vibe, Grok Build, Qoder CLI, Kimi Code CLI, GitLab Duo CLI, Rovo Dev CLI, Amp, Devin CLI / Local, Tabnine, Amazon Q Developer IDE, CodeBuddy Code CLI, IBM Bob Shell, Pochi, Pi, and Oh My Pi (omp).
- **Conditional or explicit (6)** — Continue / cn, Visual Studio, TRAE, Roo Code, IBM Bob IDE, and Sourcegraph Cody. Bob IDE is touched only when `~/.bob/mcp.json` already exists.
- **New documented adapters** — CodeBuddy uses `~/.codebuddy/.mcp.json` while preserving active older files; Bob Shell uses `~/.bob/mcp_settings.json`; Pochi uses the `mcp` section in `~/.pochi/config.jsonc`; Amazon Q Developer IDE defaults to `~/.aws/amazonq/default.json` while preserving either documented alternative.
- **Lifecycle hooks stay conservative** — Kimi uses `UserPromptSubmit`; on macOS/Linux, GitLab Duo gets a fail-open user `SessionStart`, while Devin gets `UserPromptSubmit`, `PostCompaction`, and a deduplicated `SessionStart` when Claude does not already provide it. Qoder, GitLab Duo, Devin, and Factory hooks are withheld on Windows without a documented shell/executor contract. Cline's auto-activating file hooks are withheld because their context output is not reliably consumed, CodeBuddy beta hooks are not auto-installed, and Cursor context hooks remain withheld.
- **Subagent access is explicit** — Claude, Gemini, Kiro, Qwen, CodeBuddy, KiloCode, Mistral Vibe, Grok Build, Qoder, Junie, and Factory get documented graph profiles with the narrowest tool/server filters their schemas support. KiloCode and Vibe enumerate read-only query tools rather than using server wildcards. Cursor, Rovo, Pochi, and Cline use explicit parent handoff where child MCP is unavailable or unsafe; IBM Bob receives no invented hook or agent.
- **Manual, UI, cloud, or repository-managed (not counted)** — Qodo, Warp MCP, JetBrains AI/ACP, GitHub Copilot coding agent, Jules, CodeRabbit, Replit, BLACKBOX AI, Plandex, and SWE-agent. Warp is counted above for its detected skill installation; its MCP connection remains manual.
- **17 MCP tools** — search, trace, architecture, impact analysis, Cypher queries, dead code detection, cross-service HTTP linking, ADR management, and more.

## Supported Platforms

| OS      | Architecture |
|---------|-------------|
| macOS   | arm64, amd64 |
| Linux   | arm64, amd64 |
| Windows | arm64, amd64 |

## Usage

```bash
codebase-memory-mcp install          # configure all detected coding agents
codebase-memory-mcp --version
codebase-memory-mcp --help
codebase-memory-mcp update           # update to latest release
codebase-memory-mcp uninstall        # remove agent configs; keep indexes by default
```

Uninstall keeps project indexes with `--yes`, `--no`, or noninteractive input unless you explicitly add `--delete-indexes`. An interactive terminal without the flag asks separately and defaults to keeping them. `--delete-indexes` overrides `--no`; add `--dry-run` to preview without changing files. Kept indexes remain in the cache directory printed by uninstall.

### CLI Mode

Every MCP tool is also available directly from the command line:

```bash
codebase-memory-mcp cli index_repository '{"repo_path": "/path/to/repo"}'
codebase-memory-mcp cli list_projects '{}'
# Replace PROJECT with the indexed project name returned above.
codebase-memory-mcp cli search_graph '{"project":"PROJECT","name_pattern":".*Handler.*","label":"Function"}'
codebase-memory-mcp cli trace_path '{"project":"PROJECT","function_name":"main","direction":"both"}'
codebase-memory-mcp cli get_architecture '{"project":"PROJECT"}'
```

## MCP Tools

| Category | Tools |
|----------|-------|
| **Indexing and coverage** | `index_repository`, `list_projects`, `delete_project`, `index_status`, `check_index_coverage` |
| **Search and source** | `search_graph`, `search_code`, `get_code_snippet`, `get_file_outline` |
| **Relationships and changes** | `trace_path`, `query_graph`, `detect_changes`, `compare_graphs` |
| **Architecture and evidence** | `get_architecture`, `get_graph_schema`, `manage_adr`, `ingest_traces` |

## Performance

Performance and context savings depend on the repository, query, machine and agent workflow. See the [scoped benchmark results](https://github.com/DeusData/codebase-memory-mcp#performance) and [methodology](https://github.com/DeusData/codebase-memory-mcp/blob/main/docs/BENCHMARK.md) for recorded measurements and limitations.

## Full Documentation

See [github.com/DeusData/codebase-memory-mcp](https://github.com/DeusData/codebase-memory-mcp) for the full README including all MCP tools, configuration options, graph data model, and language support details.

## License

MIT
