# codebase-memory-mcp

[![npm](https://img.shields.io/npm/v/codebase-memory-mcp?style=flat&color=blue)](https://www.npmjs.com/package/codebase-memory-mcp)
[![License](https://img.shields.io/badge/license-MIT-green)](https://github.com/DeusData/codebase-memory-mcp/blob/main/LICENSE)

A preview local code-intelligence layer for coding agents. It indexes the
current worktree into a structural graph and returns focused, reproducible
evidence within a caller-supplied token budget.

The binary and all indexing run locally. Language support is deliberately
tiered: Go, Python, JavaScript/TypeScript, Java, C/C++, and Rust are the core
quality targets; C#, PHP, and Kotlin static resolvers are preview features;
other bundled Tree-sitter grammars provide best-effort structural results.

## Installation

```bash
npm install -g codebase-memory-mcp
codebase-memory-mcp install --client codex
```

`install` changes only the explicitly selected MCP client. It does not delete
indexes or enable hooks and instruction files.

To register the client, build the first structural index, verify the
environment, and start the watcher:

```bash
codebase-memory-mcp setup . --client codex
```

Add `--no-watch`, `--with-hooks`, or `--with-instructions` only when those
behaviors are wanted.

## Core MCP tools

- `get_context`
- `search_graph`
- `search_code`
- `trace_path`
- `get_code_snippet`
- `get_architecture`
- `detect_changes`
- `index_status`
- `index_repository`

Advanced graph, ADR, cross-repository, and administrative tools must be enabled
explicitly. Tool responses include structured content plus a text JSON copy.

## Evidence policy

The project does not publish blanket speed, token, or language-accuracy claims
without a versioned benchmark artifact. See
[CAPABILITIES.md](https://github.com/DeusData/codebase-memory-mcp/blob/main/docs/CAPABILITIES.md)
for the current product contract and
[BENCHMARK.md](https://github.com/DeusData/codebase-memory-mcp/blob/main/docs/BENCHMARK.md)
for the reproducibility requirements.

## License

MIT
