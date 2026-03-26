# Contributing to codebase-memory-mcp

Contributions are welcome. This guide covers setup, testing, and PR guidelines.

## Build from Source

**Prerequisites**: Go 1.26+, a C compiler (gcc or clang — needed for tree-sitter CGO bindings), Git.

```bash
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp
CGO_ENABLED=1 go build -o codebase-memory-mcp ./cmd/codebase-memory-mcp/
```

macOS: `xcode-select --install` provides clang.
Linux: `sudo apt install build-essential` (Debian/Ubuntu) or `sudo dnf install gcc` (Fedora).

## Run Tests

```bash
go test ./... -count=1
```

Key test files:
- `internal/pipeline/langparity_test.go` — 125+ language parity cases
- `internal/pipeline/astdump_test.go` — 90+ AST structure cases
- `internal/pipeline/pipeline_test.go` — integration tests

## Run C Server Tests

The MCP server core is written in C and has its own test suite under `tests/`:

```bash
make -f Makefile.cbm test          # full suite with ASan + UBSan
make -f Makefile.cbm test-leak     # heap leak check (see below)
make -f Makefile.cbm test-analyze  # Clang static analyzer (requires clang, not gcc)
```

**Memory leak detection:**

On **macOS**, `test-leak` builds a sanitizer-free binary (`test-runner-nosan`) and runs Apple's
`leaks --atExit` on it. ASan replaces malloc, so the standard `test-runner` cannot be inspected
by `leaks` — the separate nosan build is required.

On **Linux**, `test-leak` runs the regular `test-runner` with `ASAN_OPTIONS=detect_leaks=1` to
activate LSan.

In both cases the full report is written to `build/c/leak-report.txt`. A clean run ends with:
```
Process NNNNN: 0 leaks for 0 total leaked bytes.
```

## Run Linter

```bash
golangci-lint run ./...
```

## Project Structure

```
cmd/codebase-memory-mcp/  Entry point (MCP server + CLI + install/update)
internal/
  lang/                   Language specs (63 languages, tree-sitter node types)
  parser/                 Tree-sitter grammar loading
  pipeline/               Multi-pass indexing pipeline
  httplink/               Cross-service HTTP route matching
  cypher/                 Cypher query engine
  store/                  SQLite graph storage
  tools/                  MCP tool handlers (12 tools)
  watcher/                Background auto-sync
  discover/               File discovery with .cgrignore
  fqn/                    Qualified name computation
```

## Adding or Fixing Language Support

Most language issues are in `internal/lang/<name>.go` (node type configuration) or `internal/pipeline/` (extraction logic).

**Workflow for language fixes:**

1. Find the relevant language spec in `internal/lang/`
2. Use AST dump tests to see actual tree-sitter node types:
   ```bash
   go test ./internal/pipeline/ -run TestASTDump -v
   ```
3. Compare configured node types vs actual AST output
4. Update the language spec and add/fix parity test cases
5. Verify with a real open-source repo (see `BENCHMARK_REPORT.md` for test repos per language)

## Pull Request Guidelines

- One logical change per PR
- Include tests for new functionality
- Run `go test ./... -count=1` and `golangci-lint run` before submitting
- Keep PRs focused — avoid unrelated reformatting or refactoring
- Reference the issue number in your PR description

## Good First Issues

Check [issues labeled `good first issue`](https://github.com/DeusData/codebase-memory-mcp/labels/good%20first%20issue) for beginner-friendly tasks with clear scope and guidance.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
