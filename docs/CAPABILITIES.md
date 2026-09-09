# Product capabilities

This file is generated from `product-manifest.json`.

`codebase-memory-mcp` is a preview local code-intelligence layer for coding
agents. Its product goals are fresh indexes, reproducible evidence and
token-efficient retrieval.

## Language quality

- Core: Go, Python, JavaScript, TypeScript, Java, C, C++, Rust.
- Preview static resolvers: C#, PHP, Kotlin.
- Structural: other vendored Tree-sitter grammars, best effort only.
- Removed: Lean and the dedicated SystemVerilog parser. `.sv` files use the
  experimental Verilog structural fallback.

## MCP toolsets

- Core: `get_context`, `search_graph`, `search_code`, `trace_path`, `get_code_snippet`, `get_architecture`, `detect_changes`, `index_status`, `index_repository`.
- Advanced: `query_graph`, `get_graph_schema`, `manage_adr`.
- Admin: `list_projects`, `delete_project`.

Advanced and administrative tools are not advertised by default.

## Evidence policy

Performance and quality claims must identify a reproducible benchmark artifact.
The historical 66-language results belong to the
[project preprint](https://arxiv.org/abs/2603.27277); they are not a blanket
guarantee for the current worktree or every language.
