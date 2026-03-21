# Contributing to codebase-memory-mcp

Contributions are welcome. This guide covers setup, testing, and PR guidelines.

> **Important**: This project is a **pure C binary** (rewritten from Go in v0.5.0). Please submit C code, not Go. Go PRs may be ported but cannot be merged directly.

## Build from Source

**Prerequisites**: C compiler (gcc or clang), make, zlib, Git. Optional: Node.js 22+ (for graph UI).

```bash
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp
git config core.hooksPath scripts/hooks  # activates pre-commit security checks
scripts/build.sh
```

macOS: `xcode-select --install` provides clang.
Linux: `sudo apt install build-essential zlib1g-dev` (Debian/Ubuntu) or `sudo dnf install gcc zlib-devel` (Fedora).

The binary is output to `build/c/codebase-memory-mcp`.

## Run Tests

```bash
scripts/test.sh
```

This builds with ASan + UBSan and runs all tests (~2040 cases). Key test files:
- `tests/test_pipeline.c` — pipeline integration tests
- `tests/test_httplink.c` — HTTP route extraction and linking
- `tests/test_mcp.c` — MCP protocol and tool handler tests
- `tests/test_store_*.c` — SQLite graph store tests

## Run Linter

```bash
scripts/lint.sh
```

Runs clang-tidy, cppcheck, and clang-format. All must pass before committing (also enforced by pre-commit hook).

## Run Security Audit

```bash
make -f Makefile.cbm security
```

Runs 8 security layers: static allow-list audit, binary string scan, UI audit, install audit, network egress test, MCP robustness (fuzz), vendored dependency integrity, and frontend integrity.

## Project Structure

```
src/
  foundation/       Arena allocator, hash table, string utils, platform compat
  store/            SQLite graph storage (WAL mode, FTS5)
  cypher/           Cypher query → SQL translation
  mcp/              MCP server (JSON-RPC 2.0 over stdio, 14 tools)
  pipeline/         Multi-pass indexing pipeline
    pass_*.c        Individual pipeline passes (definitions, calls, usages, etc.)
    httplink.c      HTTP route extraction (Go/Express/Laravel/Ktor/Python)
  discover/         File discovery with gitignore support
  watcher/          Git-based background auto-sync
  cli/              CLI subcommands (install, update, uninstall, config)
  ui/               Graph visualization HTTP server (mongoose)
internal/cbm/       Tree-sitter AST extraction (64 languages, vendored C grammars)
vendored/           sqlite3, yyjson, mongoose, mimalloc, xxhash, tre
graph-ui/           React/Three.js frontend for graph visualization
scripts/            Build, test, lint, security audit scripts
tests/              All C test files
```

## Adding or Fixing Language Support

Language support is split between two layers:

1. **Tree-sitter extraction** (`internal/cbm/`): Grammar loading, AST node type configuration in `lang_specs.c`, function/call/import extraction in `extract_*.c`
2. **Pipeline passes** (`src/pipeline/`): Call resolution, usage tracking, HTTP route linking

**Workflow for language fixes:**

1. Check the language spec in `internal/cbm/lang_specs.c`
2. Use regression tests to verify extraction: `tests/test_extraction.c`
3. Check parity tests: `internal/cbm/regression_test.go` (legacy, being migrated)
4. Add a test case in `tests/test_pipeline.c` for integration-level fixes
5. Verify with a real open-source repo

## Pull Request Guidelines

- **C code only** — this project was rewritten from Go to pure C in v0.5.0. Go PRs will be acknowledged and potentially ported, but cannot be merged directly.
- One logical change per PR — don't bundle unrelated features
- Include tests for new functionality
- Run `scripts/test.sh` and `scripts/lint.sh` before submitting
- Keep PRs focused — avoid unrelated reformatting or refactoring
- Reference the issue number in your PR description

## Security

We take security seriously. All PRs go through:
- Manual security review (dangerous calls, network access, file writes, prompt injection)
- Automated 8-layer security audit in CI
- Vendored dependency integrity checks

If you add a new `system()`, `popen()`, `fork()`, or network call, it must be justified and added to `scripts/security-allowlist.txt`.

## Good First Issues

Check [issues labeled `good first issue`](https://github.com/DeusData/codebase-memory-mcp/labels/good%20first%20issue) for beginner-friendly tasks with clear scope and guidance.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
