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

This is the local maintainer gate used by the project scripts. It cleans the C build, runs the
full ASan + UBSan C test suite, builds the production binary, and then runs the parent-watchdog
and security-string regression scripts. The exact test count changes as suites are added; use the
runner summary as the source of truth. Key test files:
- `tests/test_pipeline.c` — pipeline integration tests
- `tests/test_httplink.c` — HTTP route extraction and linking
- `tests/test_mcp.c` — MCP protocol and tool handler tests
- `tests/test_store_*.c` — SQLite graph store tests

Useful script options and environment:

```bash
scripts/test.sh --arch arm64             # macOS: force target architecture
scripts/test.sh CC=clang CXX=clang++     # override compiler
CBM_RUN_HANG_TEST=1 scripts/test.sh      # include the slower C++ index-hang guard
```

## Run C Server Tests

The MCP server core is written in C and has its own test suite under `tests/`:

```bash
make -f Makefile.cbm test          # full suite with ASan + UBSan
make -f Makefile.cbm test-tsan     # thread-sensitive suites with ThreadSanitizer
make -f Makefile.cbm test-leak     # heap leak check (see below)
make -f Makefile.cbm test-memory   # macOS MallocScribble/PreScribble nosan run
make -f Makefile.cbm test-gmalloc  # macOS Guard Malloc nosan run
make -f Makefile.cbm test-analyze  # Clang static analyzer (requires clang, not gcc)
```

Focused runs are opt-in. Leave these unset for the complete suite:

```bash
CBM_ONLY_SUITE=pipeline make -f Makefile.cbm test
CBM_ONLY_SUITE=pipeline CBM_ONLY_TEST=exact build/c/test-runner
```

By default, tests isolate `CBM_CACHE_DIR` in a temporary directory so local indexes are not
polluted. Set `CBM_TEST_NO_ISOLATE=1` only when intentionally testing the user's configured cache.

### Build profiles

Use `scripts/build.sh` for a clean local release build. It is the same entry point used by the
release workflow and produces the default installable binary with `-O2`. For a faster incremental
release rebuild, use `make -f Makefile.cbm cbm`; `make -f Makefile.cbm install` installs that same
optimized artifact.

Use `scripts/test.sh` for normal development validation. Its C test binary uses `-g -O1` with
ASan and UBSan, then it builds the production binary for the parent/worker watchdog checks. Use
the dedicated `test-tsan`, `test-leak`, `test-memory`, and `test-gmalloc` targets above when
diagnosing concurrency or allocator lifetime behavior. These diagnostic binaries are not valid
performance-benchmark inputs.

For performance changes, use the canonical fact tables and comparison rules in
[`docs/BENCHMARK_EXPERIMENTS.md`](docs/BENCHMARK_EXPERIMENTS.md). Field, step,
concurrency, and lifecycle meanings come from the generated
[`docs/BENCHMARK_TERMINOLOGY.md`](docs/BENCHMARK_TERMINOLOGY.md), whose source of
truth is `docs/benchmark-terminology.json`.

Additional build flags follow the Makefile conventions:

```bash
make -f Makefile.cbm test SANITIZE=      # disable ASan/UBSan, mainly for unsupported toolchains
make -f Makefile.cbm cbm CFLAGS_EXTRA=-DCBM_VERSION=dev
make -f Makefile.cbm cbm STATIC=1        # static link where supported
```

**Memory leak detection:**

On **macOS**, `test-leak` builds a sanitizer-free binary (`test-runner-nosan`) and runs Apple's
`leaks --atExit` on allocation-owning store, pipeline, ranker, and parallel-worker
suites. ASan replaces malloc, so the standard `test-runner` cannot be inspected by `leaks` — the
separate nosan build is required. Deliberate crash tests are excluded because Apple's debugger
stops their child processes before the parent can reap them; this includes the subprocess,
stack-overflow, MCP crash-quarantine, and HTTP socket-inheritance suites.

On **Linux**, `test-leak` runs the regular `test-runner` with `ASAN_OPTIONS=detect_leaks=1` to
activate LSan.

In both cases the full report is written to `build/c/leak-report.txt`. A clean run ends with:
```
Process NNNNN: 0 leaks for 0 total leaked bytes.
```

## Run Linter

```bash
scripts/lint.sh
make -f Makefile.cbm lint-source-safety
```

Runs clang-tidy, cppcheck, and clang-format. `lint-source-safety` runs the source guard and its
self-tests; it blocks new MCP stdout writes, insecure string APIs, raw env/filesystem calls in
reviewed paths, and other regressions that should use existing CBM helpers instead. All must pass
before committing (also enforced by pre-commit hook).

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
  ui/               Graph visualization HTTP server (first-party httpd)
internal/cbm/       Tree-sitter AST extraction (64 languages, vendored C grammars)
vendored/           sqlite3, yyjson, mimalloc, xxhash, tre, nomic
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

### Infrastructure Languages (Infra-Pass Pattern)

Languages like **Dockerfile**, **docker-compose**, **Kubernetes manifests**, and **Kustomize** do not require a new tree-sitter grammar. Instead they follow an *infra-pass* pattern, reusing the existing tree-sitter YAML grammar where applicable:

1. **Detection helpers** in `src/pipeline/pass_infrascan.c` — functions like `cbm_is_dockerfile()`, `cbm_is_k8s_manifest()`, `cbm_is_kustomize_file()` identify files by name and/or content heuristics (e.g., presence of `apiVersion:`).
2. **Custom extractors** in `internal/cbm/extract_k8s.c` — tree-sitter-based parsers that walk the YAML AST (using the tree-sitter YAML grammar) and populate `CBMFileResult` with imports and definitions.
3. **Pipeline pass** (`pass_k8s.c`, `pass_infrascan.c`) — calls the extractor and emits graph nodes/edges. K8s manifests emit `Resource` nodes; Kustomize files emit `Module` nodes with `IMPORTS` edges to referenced resource files.

**When adding a new infrastructure language:**
- Add a detection helper (`cbm_is_<lang>_file()`) in `pass_infrascan.c` or a new `pass_<lang>.c`.
- Add the `CBM_LANG_<LANG>` enum value in `internal/cbm/cbm.h` and a row in the language table in `lang_specs.c`.
- Write a custom extractor that returns `CBMFileResult*` — do not add a tree-sitter grammar.
- Register the pass in `pipeline.c`.
- Add tests in `tests/test_pipeline.c` following the `TEST(infra_is_dockerfile)` and `TEST(k8s_extract_manifest)` patterns.

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
| `chore` | Build scripts, CI, dependency updates |

Examples: `fix(store): set busy_timeout before WAL`, `feat(cli): add --progress flag`

## Pull Request Guidelines

### Before You Write Code

- **Open an issue first — always.** Every PR must reference a tracking issue (`Fixes #N` or `Closes #N`). Describe what you want to change and why. Wait for maintainer feedback before implementing. PRs without a prior issue discussion will be closed.
- **Bug fixes and test additions** are the exception — these are welcome without prior discussion, as long as they're focused.

### What Requires Explicit Maintainer Approval

The following changes will not be merged without prior design discussion in an issue:

- **API surface changes** — adding, removing, renaming, or changing defaults of MCP tools
- **New pipeline passes or indexing algorithms** — anything that changes what gets extracted or how
- **Build system / Makefile changes** — beyond trivial fixes
- **Project configuration** — CLAUDE.md, skill files, .mcp.json, CI workflows
- **New dependencies** — vendored or otherwise
- **Breaking changes** of any kind

If in doubt, open an issue and ask.

### PR Scope and Size

- **One issue per PR.** Each PR must address exactly one bug, one feature, or one refactor. Do not bundle multiple fixes or feature additions into a single PR. Kitchen-sink PRs will be closed with a request to split.
- **Keep PRs small.** A good PR is under 500 lines. If your change is larger, split it into reviewable increments that each stand on their own.
- **Don't mix features with fixes.** If you find a bug while implementing a feature, submit the bug fix as a separate PR.

### Code Requirements

- **C code only** — this project was rewritten from Go to pure C in v0.5.0. Go PRs will be acknowledged and potentially ported, but cannot be merged directly.
- Include tests for new functionality
- Run `scripts/test.sh` and `scripts/lint.sh` before submitting
- Keep PRs focused — avoid unrelated reformatting or refactoring

## Security

We take security seriously. All PRs go through:
- Manual security review (dangerous calls, network access, file writes, prompt injection)
- Automated 8-layer security audit in CI
- Vendored dependency integrity checks

If you add a new `system()`, `popen()`, `fork()`, or network call, it must be justified and added to `scripts/security-allowlist.txt`.

## Good First Issues

Check [issues labeled `good first issue`](https://github.com/DeusData/codebase-memory-mcp/labels/good%20first%20issue) for beginner-friendly tasks with clear scope and guidance.

## License and sign-off (DCO) — required on every commit

All contributions are licensed under the project's MIT License
(inbound = outbound). To make that explicit and permanent, this project
uses the [Developer Certificate of Origin 1.1](DCO) — the same mechanism
as the Linux kernel: **every commit must carry a `Signed-off-by` trailer
matching the commit author.**

```bash
git commit -s             # adds: Signed-off-by: Your Name <you@example.com>
```

**Adding a `Signed-off-by` line to a commit constitutes your certification
of the [Developer Certificate of Origin 1.1](DCO) — in full, all four
clauses — for that contribution.** The sign-off must match the commit's
author name and email (enforced by CI). In short: you certify that you
wrote the change or otherwise have the right to submit it under the MIT
license, and that you understand the contribution and your sign-off are
public and permanent.

(Independently of the DCO, submitting a contribution to this repository is
also subject to GitHub's Terms of Service §D.6, under which contributions
are licensed inbound = outbound — i.e., under this repository's MIT
license.)

Enforcement is strict and automated:

- CI rejects every push and pull request containing an unsigned commit
  (`scripts/check-dco.sh`).
- Install the local hook so unsigned commits are rejected at commit time:

```bash
scripts/install-git-hooks.sh
```

Forgot to sign? `git commit --amend -s` fixes the last commit;
`git rebase --signoff <base>` fixes a whole branch.
