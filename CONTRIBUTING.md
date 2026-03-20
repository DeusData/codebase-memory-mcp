# Contributing to codebase-memory-mcp

Contributions are welcome. This guide covers setup, testing, and PR guidelines.

> **Important**: This project is a **pure C binary** (rewritten from Go in v0.5.0). Please submit C code, not Go. Go PRs may be ported but cannot be merged directly.

## Build from Source

**Prerequisites**: C compiler (gcc or clang), make, zlib, Git. Optional: Node.js 22+ (for graph UI).

```bash
git clone https://github.com/DeusData/codebase-memory-mcp.git
cd codebase-memory-mcp
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

## Commit Format

Use conventional commits: `type(scope): description`

| Type | When to use |
|------|-------------|
| `feat` | New feature or capability |
| `fix` | Bug fix |
| `test` | Adding or updating tests |
| `refactor` | Code change that neither fixes a bug nor adds a feature |
| `perf` | Performance improvement |
| `docs` | Documentation only |
| `style` | Formatting, whitespace (no logic change) |
| `chore` | Build scripts, CI, dependency updates |

QA round fix commits must use the format `fix(scope): address QA round N` (e.g. `fix(pipeline): address QA round 2`).

## Pull Request Guidelines

- **C code only** — this project was rewritten from Go to pure C in v0.5.0. Go PRs will be acknowledged and potentially ported, but cannot be merged directly.
- One logical change per PR — don't bundle unrelated features
- Include tests for new functionality
- Run `scripts/test.sh` and `scripts/lint.sh` before submitting
- Keep PRs focused — avoid unrelated reformatting or refactoring

## Pull Request Process

1. **Open an issue first.** Every PR must reference a tracking issue. Accepted formats:
   - Closing keywords: `Fixes #N`, `Closes #N`, `Resolves #N`
   - Full GitHub issue URLs: `https://github.com/owner/repo/issues/N`
   - Bare issue references: `#N` (any `#` followed by a valid issue number)
   - Sidebar-linked issues (via the GitHub "Development" section on the PR)
2. Describe what changed and why. Include before/after if relevant.
3. Test your changes against at least one real project (not the repo itself).
4. **Run QA review before marking ready.** Repeat this cycle at least 3 times (or until the latest report contains no confirmed critical/major issues):

   > **Docs-only or trivial PRs:** The QA round requirement only applies when the PR touches logic paths. PRs that only change docs, CI config, or repo metadata skip the check automatically.

   **Step A — Run the QA prompt.** Open a **new** Claude Code (or other AI) session using a top-tier model — **Claude Opus 4.6**, **GPT-5.3 Codex high/xhigh**, or **Gemini 3.1 Pro**. Smaller models (Haiku, Sonnet, etc.) don't produce thorough enough reviews. Paste the prompt below (fill in the placeholders). *(Prompt approach inspired by [@dpearson2699](https://github.com/dpearson2699)'s work in the [VBW project](https://github.com/yidakee/vibe-better-with-claude-code-vbw).)*

   ````text
   You are a read-only QA reviewer. Do NOT modify files, make commits, or push fixes — report only.

   PR: #<number>
   Branch: <branch-name>

   1. Review the commits in the PR to understand the change narrative.
   2. Read all files changed in the PR for full context.
   3. Act as a devil's advocate — find edge cases, missed regressions, and untested
      paths the implementer didn't consider.

   Do NOT prescribe what to test upfront. Discover what matters by reading the code.

   Report format (use a markdown code block):
   - Model used: (e.g., Claude Opus 4.6, GPT-5.3 Codex (high or xhigh), Gemini 3.1 Pro)
   - What was tested
   - Expected vs actual
   - Severity (critical / major / minor)
   - Confirmed vs hypothetical
   ````

   **Step B — Fix the findings.** Copy the QA report and paste it into your original working session (or a new session on the same branch). Tell it to fix the issues found. Each QA round's fixes must be a **separate commit** — do not amend previous commits. Use the format `fix(scope): address QA round N`.

   **Step C — Repeat.** Go back to Step A with a fresh session. The new QA round will see the fix commits from Step B and look for anything still missed. Continue until a round comes back clean or only has hypothetical/minor findings.

   **Proving your work:** Paste each round's QA report as a separate comment on the PR. Reviewers will cross-reference the reports against the fix commits in the PR history.


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
