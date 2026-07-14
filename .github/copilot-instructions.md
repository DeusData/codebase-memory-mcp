# codebase-memory-mcp Copilot instructions

## Build, test, and lint commands

Use the scripts in `scripts/` as the canonical entrypoints (they are used by CI and wrap the Makefile correctly).

```bash
# Build (default production binary)
scripts/build.sh

# Build with embedded graph UI assets
scripts/build.sh --with-ui

# Run full C test suite (ASan + UBSan build)
scripts/test.sh

# Run one/few suites only (suite names come from tests/test_main.c)
make -f Makefile.cbm test-focused TEST_SUITES="pipeline"
make -f Makefile.cbm test-focused TEST_SUITES="mcp store_nodes"

# Fast foundation-only tests
make -f Makefile.cbm test-foundation

# ThreadSanitizer run (default suites: mem slab_alloc parallel)
make -f Makefile.cbm test-tsan CC=clang CXX=clang++

# Lint (full local lint: clang-tidy + cppcheck + clang-format)
scripts/lint.sh

# CI-style lint (no clang-tidy)
scripts/lint.sh --ci

# Security audit suite
make -f Makefile.cbm security
```

UI frontend-only build path used by `cbm-with-ui`:

```bash
cd graph-ui && npm ci && npm run build
```

## High-level architecture

- **Single C binary, two operating modes**:
  - `codebase-memory-mcp` (no subcommand): MCP JSON-RPC server over stdio (`src/main.c` + `src/mcp/mcp.c`)
  - `codebase-memory-mcp cli <tool> ...`: one-shot tool execution path (`run_cli` in `src/main.c`)
- **Pipeline orchestrator (`src/pipeline/pipeline.c`)**:
  1. discover files
  2. build structural nodes
  3. run extraction/resolution (sequential or parallel depending on file count/worker count)
  4. run post passes (tests/history/predump)
  5. dump graph to SQLite and persist hashes/artifacts
- **Graph persistence layer (`src/store/store.c`)**: SQLite-backed graph store with prepared-statement caching, schema/init, node/edge/project/file-hash operations, and search/traversal queries.
- **Language extraction stack**:
  - `internal/cbm/`: tree-sitter extraction, language specs, LSP-style resolvers, vendored grammar/runtime code
  - `src/pipeline/pass_*.c`: pass-level graph construction and enrichment
- **CLI installation/config surface (`src/cli/*.c`)**: install/update/uninstall/config plus agent config/instruction/profile wiring across many client surfaces.

## Key conventions

- **Pure C codebase**: this project was rewritten from Go; implementation changes are expected in C (see `CONTRIBUTING.md`).
- **Use script entrypoints, not ad-hoc make recipes**: `scripts/build.sh`, `scripts/test.sh`, and `scripts/lint.sh` are the maintained source of truth and handle env/arch/compiler wiring consistently.
- **When adding/changing pipeline passes, keep sequential and parallel paths aligned**:
  - sequential pass list is in `run_sequential_pipeline()` (`src/pipeline/pipeline.c`)
  - parallel flow is in `run_parallel_pipeline()` and related parallel helpers
  - if logic exists in only one path, behavior diverges by repo size/worker config.
- **Focused test runs use suite names from `tests/test_main.c`** via `TEST_SUITES="..."`; this runner supports multiple suite names in one invocation.
- **Strict lint suppression policy**: only `NOLINT(misc-no-recursion)` is allowed, and only for whitelisted functions (`Makefile.cbm` + `src/foundation/recursion_whitelist.h`).
- **No-skips policy for tests**: lint checks enforce that tests should pass/fail rather than generic skips (`scripts/check-no-test-skips.sh`).
- **Security-sensitive calls are allowlisted**: additions like `system()`, `popen()`, `fork()`, or new network calls must be justified and reflected in `scripts/security-allowlist.txt`.
- **Contribution workflow conventions from repo docs**: enable repo hooks (`git config core.hooksPath scripts/hooks`) and use DCO sign-off commits (`git commit -s`).
