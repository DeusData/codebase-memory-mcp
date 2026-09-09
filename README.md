# codebase-memory-mcp

A local MCP server that builds a structural knowledge graph of a repository. It helps coding agents find symbols, callers, dependencies and architecture without repeatedly reading large files.

The engine runs locally and does not call language models. It can reduce context usage by returning focused graph results instead of whole files.

## Build

```bash
./scripts/build.sh
```

Build with the local graph viewer:

```bash
./scripts/build.sh --with-ui
```

## Install for Codex

```bash
./build/c/codebase-memory-mcp install --client codex
```

The installer only registers the MCP server in the selected client. Existing
indexes are preserved. Hooks and instruction files are opt-in.

For registration, a first structural index, environment verification, and
watcher configuration in one command:

```bash
./build/c/codebase-memory-mcp setup . --client codex
```

Use `--no-watch`, `--with-hooks`, or `--with-instructions` only when wanted.

## Index a repository

```bash
./build/c/codebase-memory-mcp cli index_repository '{"repo_path":"/absolute/path/to/repository","mode":"structural"}'
```

Useful MCP tools:

- `get_context` — deterministic evidence pack within a token budget
- `get_architecture` — repository overview
- `search_graph` — symbols and definitions
- `search_code` — graph-augmented text search
- `trace_path` — callers, callees and data flow
- `get_code_snippet` — focused source
- `detect_changes` — structural change detection
- `index_status` / `index_repository` — freshness and indexing

Advanced and administrative toolsets are disabled by default.

## Diagnostic dashboard

The optional embedded dashboard reports index freshness, coverage and errors.
It does not embed a terminal or run a separate desktop process.

```bash
./scripts/build.sh --with-ui
./build/c/codebase-memory-mcp --ui=true
```

## Verify

```bash
bash install-combined.sh --check
make -f Makefile.cbm test
cd graph-ui && npm test
```

## License

MIT. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
