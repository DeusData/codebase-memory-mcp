# Codebase Structure

**Analysis Date:** 2026-04-11

## Directory Layout

```text
[project-root]/
├── `src/`                  # Native application modules for process bootstrap, indexing, storage, MCP, watcher, and UI server
├── `internal/cbm/`         # Tree-sitter extraction engine, grammar shims, and parser/runtime internals
├── `graph-ui/`             # React/Vite frontend embedded into the native UI server
├── `tests/`                # Native test suite organized by subsystem
├── `scripts/`              # Canonical build, test, setup, lint, and proof automation
├── `tools/`                # Standalone Tree-sitter grammar workspaces
├── `vendored/`             # Bundled third-party C dependencies such as SQLite, yyjson, mongoose, and mimalloc
├── `docs/`                 # Human-readable project documentation
├── `test-infrastructure/`  # Support assets for environment and test scenarios
├── `Makefile.cbm`          # Primary native build graph
├── `codemap.md`            # Repository-level architecture summary
└── `.planning/codebase/`   # Generated planning reference docs
```

## Directory Purposes

**`src/`:**
- Purpose: Hold the production native application code.
- Contains: Composition root, pipeline passes, store/query code, watcher, CLI, UI server, and shared foundation utilities.
- Key files: `src/main.c`, `src/pipeline/pipeline.c`, `src/mcp/mcp.c`, `src/store/store.c`, `src/ui/http_server.c`

**`src/foundation/`:**
- Purpose: Put cross-cutting runtime primitives in one place.
- Contains: `*.c`/`*.h` pairs for logging, memory, compatibility, threading, hash tables, diagnostics, YAML, and string utilities.
- Key files: `src/foundation/log.c`, `src/foundation/mem.c`, `src/foundation/hash_table.c`, `src/foundation/platform.c`

**`src/discover/`:**
- Purpose: Keep repository walking, ignore handling, and language detection together.
- Contains: Discovery implementation, gitignore parsing, language routing, user config support.
- Key files: `src/discover/discover.c`, `src/discover/gitignore.c`, `src/discover/language.c`, `src/discover/userconfig.c`

**`src/pipeline/`:**
- Purpose: Separate indexing orchestration from extraction internals and storage.
- Contains: Core pipeline lifecycle, incremental mode, worker pool, registry, FQN helpers, and feature-specific pass files.
- Key files: `src/pipeline/pipeline.c`, `src/pipeline/pipeline_incremental.c`, `src/pipeline/registry.c`, `src/pipeline/pass_definitions.c`, `src/pipeline/pass_calls.c`

**`src/mcp/`:**
- Purpose: Isolate the stdio MCP protocol implementation.
- Contains: One main implementation file and header.
- Key files: `src/mcp/mcp.c`, `src/mcp/mcp.h`

**`src/store/`:**
- Purpose: Centralize graph persistence and query logic.
- Contains: SQLite-backed store implementation and public API header.
- Key files: `src/store/store.c`, `src/store/store.h`

**`src/ui/`:**
- Purpose: Host the native side of the browser UI.
- Contains: UI config persistence, embedded asset hooks, HTTP server, and graph layout generation.
- Key files: `src/ui/http_server.c`, `src/ui/layout3d.c`, `src/ui/config.c`, `src/ui/embedded_assets.h`

**`src/graph_buffer/`:**
- Purpose: Provide the in-memory write model between extraction and SQLite.
- Contains: Buffer implementation and indexes for staged graph mutations.
- Key files: `src/graph_buffer/graph_buffer.c`

**`src/cli/`:**
- Purpose: Keep installer/update/config subcommands separate from `src/main.c`.
- Contains: CLI handlers and progress sink support.
- Key files: `src/cli/cli.c`, `src/cli/progress_sink.c`

**`src/watcher/`:**
- Purpose: Track repository changes and schedule reindexing.
- Contains: Git-based watcher implementation.
- Key files: `src/watcher/watcher.c`

**`src/cypher/`, `src/simhash/`, `src/traces/`:**
- Purpose: Group specialized capabilities beside the main pipeline and store.
- Contains: Cypher query support, similarity logic, and trace ingestion.
- Key files: `src/cypher/cypher.c`, `src/simhash/minhash.c`, `src/traces/traces.c`

**`internal/cbm/`:**
- Purpose: Keep extraction-engine internals out of the top-level application layer.
- Contains: `cbm.c`, extraction passes, helper code, LSP helpers, generated grammar shims, SQLite writer, vendored Tree-sitter runtime.
- Key files: `internal/cbm/cbm.c`, `internal/cbm/cbm.h`, `internal/cbm/extract_defs.c`, `internal/cbm/extract_calls.c`, `internal/cbm/grammar_gdscript.c`

**`graph-ui/src/`:**
- Purpose: Hold the browser client source.
- Contains: React bootstrap, app shell, hooks, API client code, components, and styles.
- Key files: `graph-ui/src/main.tsx`, `graph-ui/src/App.tsx`, `graph-ui/src/hooks/useProjects.ts`, `graph-ui/src/hooks/useGraphData.ts`, `graph-ui/src/components/GraphTab.tsx`

**`tests/`:**
- Purpose: Keep native tests flat and easy to target by subsystem.
- Contains: `test_*.c` files for foundation, store, pipeline, UI, security, watcher, and integration coverage plus `fixtures/`.
- Key files: `tests/test_main.c`, `tests/test_pipeline.c`, `tests/test_mcp.c`, `tests/test_ui.c`, `tests/fixtures/`

**`scripts/`:**
- Purpose: Expose the supported developer and CI workflows.
- Contains: Build/test wrappers, environment setup, cleanup, lint, security checks, UI embedding, and CI helpers.
- Key files: `scripts/build.sh`, `scripts/test.sh`, `scripts/env.sh`, `scripts/embed-frontend.sh`, `scripts/ci/`

**`tools/`:**
- Purpose: Store self-contained grammar packages rather than application modules.
- Contains: Tree-sitter grammar workspaces such as `tools/tree-sitter-form/` and `tools/tree-sitter-magma/`.
- Key files: `tools/tree-sitter-form/grammar.js`, `tools/tree-sitter-magma/grammar.js`

## Key File Locations

**Entry Points:**
- `src/main.c`: Native binary bootstrap and top-level mode routing.
- `graph-ui/src/main.tsx`: Browser bootstrap for the React UI.
- `scripts/build.sh`: Canonical release build entry point.
- `scripts/test.sh`: Canonical full test entry point.

**Configuration:**
- `Makefile.cbm`: Source lists, compiler flags, build targets, and test targets.
- `src/ui/config.c`: Persisted UI enablement and port settings.
- `src/discover/userconfig.c`: User-defined language override support.
- `server.json`: Runtime server metadata for integrations.

**Core Logic:**
- `src/pipeline/pipeline.c`: Full indexing orchestration.
- `internal/cbm/cbm.c`: File extraction entry point.
- `src/store/store.c`: Durable graph storage and query logic.
- `src/mcp/mcp.c`: Tool definitions and MCP dispatch.
- `src/ui/http_server.c`: Local browser-facing API and asset server.

**Testing:**
- `tests/`: Flat subsystem-oriented C test files.
- `tests/fixtures/`: Sample repositories and source inputs for tests.
- `test-infrastructure/`: Additional environment/test support assets.

## Naming Conventions

**Files:**
- Native source uses snake_case filenames with paired headers when public APIs are exposed: `src/store/store.c` + `src/store/store.h`, `src/ui/http_server.c` + `src/ui/http_server.h`.
- Pipeline feature files are grouped by prefix and concern: `src/pipeline/pass_*.c`, `src/pipeline/pipeline_*.c`.
- Tests use flat `test_<area>.c` filenames under `tests/`: `tests/test_store_search.c`, `tests/test_worker_pool.c`.
- Frontend components use PascalCase React filenames: `graph-ui/src/components/GraphTab.tsx`, `graph-ui/src/components/StatsTab.tsx`.
- Frontend hooks use `use*.ts` names: `graph-ui/src/hooks/useGraphData.ts`, `graph-ui/src/hooks/useProjects.ts`.

**Directories:**
- Native directories are lowercase and concern-oriented: `src/store/`, `src/pipeline/`, `src/watcher/`.
- Frontend directories also stay lowercase by role: `graph-ui/src/components/`, `graph-ui/src/hooks/`, `graph-ui/src/lib/`, `graph-ui/src/styles/`.
- Extraction internals live below `internal/cbm/` instead of `src/`, which signals “subsystem internals, not top-level app modules”.

## Where to Add New Code

**New Native Feature:**
- Primary code: Add the feature to the closest concern directory under `src/` rather than expanding `src/main.c`. For indexing behavior, use `src/pipeline/`; for persistence/query behavior, use `src/store/`; for transport behavior, use `src/mcp/` or `src/ui/`.
- Tests: Add a matching flat test file in `tests/`, or extend the nearest existing subsystem file such as `tests/test_pipeline.c`, `tests/test_store_search.c`, or `tests/test_ui.c`.

**New Indexing Pass or Extraction Rule:**
- Pipeline orchestration: `src/pipeline/pass_<concern>.c` and register it from `src/pipeline/pipeline.c` or related pipeline wiring files.
- Parser/extractor behavior: `internal/cbm/extract_<concern>.c`, `internal/cbm/helpers.c`, or grammar-specific code such as `internal/cbm/grammar_gdscript.c` when the concern is language-specific.

**New MCP Tool or Query Surface:**
- Implementation: `src/mcp/mcp.c` for tool schema and dispatch; `src/store/store.c` or `src/cypher/cypher.c` for the backing query logic.
- UI exposure: If the browser also needs it, add a route to `src/ui/http_server.c` and a client/hook in `graph-ui/src/api/` or `graph-ui/src/hooks/`.

**New Frontend View or UI Module:**
- Implementation: `graph-ui/src/components/` for components, `graph-ui/src/hooks/` for data loading, and `graph-ui/src/lib/` for shared types/helpers.
- Server contract: Keep the backend endpoint in `src/ui/http_server.c` aligned with the consuming hook/component.

**Utilities:**
- Shared native helpers: `src/foundation/` if the utility is broadly reusable and not domain-specific.
- Shared frontend helpers: `graph-ui/src/lib/`.
- Build/test workflow helpers: `scripts/`.

## Special Directories

**`vendored/`:**
- Purpose: Bundled third-party native dependencies used by the build.
- Generated: No
- Committed: Yes

**`build/`:**
- Purpose: Local build outputs including the compiled binary under `build/c/`.
- Generated: Yes
- Committed: No

**`.artifacts/`:**
- Purpose: Generated proof, benchmark, or workflow artifacts.
- Generated: Yes
- Committed: Typically no

**`.planning/codebase/`:**
- Purpose: Generated planning reference docs consumed by GSD workflows.
- Generated: Yes
- Committed: Yes, when planners/mappers update documentation intentionally.

**`.worktrees/`:**
- Purpose: Local git worktree storage for branch-based development.
- Generated: Yes
- Committed: No

---

*Structure analysis: 2026-04-11*
