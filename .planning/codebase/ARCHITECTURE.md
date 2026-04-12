# Architecture

**Analysis Date:** 2026-04-11

## Pattern Overview

**Overall:** Layered native application with a persistent graph store, an indexing pipeline, and two delivery surfaces: stdio MCP and localhost UI.

**Key Characteristics:**
- `src/main.c` is the composition root for CLI mode, MCP server mode, watcher startup, and optional HTTP UI startup.
- `src/pipeline/` owns end-to-end indexing orchestration, while `internal/cbm/` is a lower-level extraction subsystem that stays behind the pipeline boundary.
- `src/store/store.c` is the durable system boundary; both `src/mcp/mcp.c` and `src/ui/http_server.c` read indexed data back through the SQLite-backed store.

## Layers

**Application Composition Layer:**
- Purpose: Bootstrap the process, choose runtime mode, and wire long-lived services together.
- Location: `src/main.c`, `src/cli/`
- Contains: Argument parsing, signal handling, CLI subcommands, watcher thread startup, UI thread startup.
- Depends on: `src/mcp/`, `src/pipeline/`, `src/watcher/`, `src/ui/`, `src/store/`, `src/foundation/`
- Used by: The compiled binary built through `Makefile.cbm` and `scripts/build.sh`.

**Protocol and Query Surface Layer:**
- Purpose: Expose indexed data and management operations to clients.
- Location: `src/mcp/mcp.c`, `src/cypher/cypher.c`, `src/ui/http_server.c`
- Contains: JSON-RPC parsing, MCP tool dispatch, Cypher execution, HTTP routes, UI-only APIs, embedded asset serving.
- Depends on: `src/store/store.c`, `src/pipeline/`, `src/watcher/`, `src/ui/layout3d.c`, `src/foundation/`
- Used by: Agent clients over stdio and the React app in `graph-ui/src/`.

**Indexing Orchestration Layer:**
- Purpose: Turn a repository path into graph nodes, edges, and enrichment data.
- Location: `src/pipeline/`
- Contains: `pipeline.c`, `pipeline_incremental.c`, `registry.c`, `worker_pool.c`, and pass files such as `pass_definitions.c`, `pass_calls.c`, `pass_usages.c`, `pass_semantic.c`, `pass_tests.c`, `pass_similarity.c`.
- Depends on: `src/discover/`, `src/graph_buffer/`, `src/store/`, `internal/cbm/`, `src/simhash/`, `src/foundation/`
- Used by: `src/main.c`, watcher callbacks in `src/main.c`, and management flows exposed from `src/mcp/mcp.c` and `src/ui/http_server.c`.

**Discovery and Extraction Layer:**
- Purpose: Decide which files to index and extract language-aware symbols and relations from each file.
- Location: `src/discover/`, `internal/cbm/`
- Contains: Recursive discovery in `src/discover/discover.c`, ignore handling in `src/discover/gitignore.c`, language mapping in `src/discover/language.c`, user overrides in `src/discover/userconfig.c`, and Tree-sitter extraction in `internal/cbm/cbm.c` plus `internal/cbm/extract_*.c`.
- Depends on: `src/foundation/`, Tree-sitter runtime under `internal/cbm/vendored/`, generated grammar shims like `internal/cbm/grammar_gdscript.c`.
- Used by: `src/pipeline/pipeline.c` and `src/pipeline/pass_parallel.c`.

**Graph Staging and Persistence Layer:**
- Purpose: Buffer graph mutations in memory, then persist and query them efficiently.
- Location: `src/graph_buffer/graph_buffer.c`, `src/store/store.c`, `internal/cbm/sqlite_writer.c`
- Contains: In-memory node/edge staging, dedup indexes, SQLite schema/bootstrap, prepared statement caching, search and traversal queries.
- Depends on: `src/foundation/`, vendored `sqlite3` in `vendored/sqlite3/`.
- Used by: `src/pipeline/`, `src/mcp/mcp.c`, `src/ui/http_server.c`, `src/watcher/watcher.c`.

**Runtime Primitives Layer:**
- Purpose: Provide reusable low-level services used everywhere else.
- Location: `src/foundation/`
- Contains: Logging, memory setup, platform compatibility, threads, hash tables, dynamic arrays, string utilities, diagnostics, YAML support.
- Depends on: Platform/system libraries only.
- Used by: Every native subsystem under `src/` and `internal/cbm/`.

**Frontend Presentation Layer:**
- Purpose: Render indexed projects and graph data in a browser.
- Location: `graph-ui/src/`
- Contains: App bootstrap in `graph-ui/src/main.tsx`, tab shell in `graph-ui/src/App.tsx`, data hooks in `graph-ui/src/hooks/`, and tab/scene components in `graph-ui/src/components/`.
- Depends on: The localhost HTTP server implemented in `src/ui/http_server.c`.
- Used by: Developers opening the local graph UI served from embedded assets.

## Data Flow

**Full Indexing Flow:**

1. `src/main.c` or an MCP/UI management path creates a pipeline instance from a repository root.
2. `src/pipeline/pipeline.c` loads overrides from `src/discover/userconfig.c` and calls discovery in `src/discover/discover.c`.
3. `src/pipeline/pipeline.c` creates structural graph nodes first, then sends source files through extraction backed by `internal/cbm/cbm.c` and the extractor files in `internal/cbm/extract_*.c`.
4. Passes in `src/pipeline/` resolve definitions, calls, usages, semantic edges, tests, git metadata, infra/config links, and similarity data into `src/graph_buffer/graph_buffer.c`.
5. The completed graph is flushed into SQLite through `src/store/store.c` and `internal/cbm/sqlite_writer.c`.

**Query and UI Flow:**

1. Clients call MCP tools through stdio handled by `src/mcp/mcp.c`, or the browser calls `/rpc` and `/api/*` routes in `src/ui/http_server.c`.
2. The server path opens project data through `src/store/store.c` and optional Cypher helpers in `src/cypher/cypher.c`.
3. For visualization, `src/ui/layout3d.c` computes graph layout data and returns it to hooks in `graph-ui/src/hooks/useGraphData.ts` and `graph-ui/src/hooks/useProjects.ts`.
4. Components in `graph-ui/src/components/` render stats, controls, and the 3D graph scene.

**Change Detection Flow:**

1. `src/main.c` starts the background watcher when the process is running normally.
2. `src/watcher/watcher.c` polls git state, tracks adaptive intervals, and detects dirty working trees or HEAD changes.
3. The watcher callback in `src/main.c` acquires the global pipeline lock in `src/pipeline/pipeline.c` and triggers a new index run for the affected project.

**State Management:**
- Persistent project state lives in SQLite via `src/store/store.c`.
- In-flight indexing state lives in the opaque pipeline struct in `src/pipeline/pipeline.c` and the graph buffer in `src/graph_buffer/graph_buffer.c`.
- Runtime service state lives in process globals and thread-owned structs in `src/main.c`, `src/watcher/watcher.c`, and `src/ui/http_server.c`.
- Frontend view state is local React state in `graph-ui/src/App.tsx` plus per-hook fetch state in `graph-ui/src/hooks/`.

## Key Abstractions

**Pipeline Handle (`cbm_pipeline_t`):**
- Purpose: Represent one full or incremental indexing run.
- Examples: `src/pipeline/pipeline.c`, `src/pipeline/pipeline_incremental.c`
- Pattern: Opaque lifecycle-managed C handle with shared run context and pass-oriented execution.

**Graph Buffer (`cbm_gbuf_t`):**
- Purpose: Stage nodes and edges before persistence.
- Examples: `src/graph_buffer/graph_buffer.c`, `src/pipeline/pipeline.c`
- Pattern: In-memory append-plus-index buffer with dedup maps and stable heap-owned node/edge records.

**Store Handle (`cbm_store_t`):**
- Purpose: Encapsulate all SQLite persistence and query logic.
- Examples: `src/store/store.c`, `src/mcp/mcp.c`, `src/ui/http_server.c`
- Pattern: Opaque handle with schema bootstrap and cached prepared statements.

**Extraction Result (`CBMFileResult` and related extraction types):**
- Purpose: Carry per-file parse results from Tree-sitter extraction into the pipeline.
- Examples: `internal/cbm/cbm.h`, `internal/cbm/cbm.c`, `internal/cbm/extract_defs.c`
- Pattern: Shared data-model header plus specialized extractor passes.

**Server Pair (`cbm_mcp_server_t` and `cbm_http_server_t`):**
- Purpose: Expose the indexed graph to agent and browser clients.
- Examples: `src/mcp/mcp.c`, `src/ui/http_server.c`
- Pattern: Separate transport implementations over the same store/query core.

## Entry Points

**Native Binary Entrypoint:**
- Location: `src/main.c`
- Triggers: Running `build/c/codebase-memory-mcp` directly, via agent integration, or via scripts such as `scripts/build.sh` and `scripts/test.sh`.
- Responsibilities: Select CLI vs MCP mode, persist UI config changes, install signal handlers, start watcher and HTTP UI threads, and own process shutdown.

**MCP Tool Surface:**
- Location: `src/mcp/mcp.c`
- Triggers: JSON-RPC requests over stdio in server mode, or the single-tool CLI path in `src/main.c`.
- Responsibilities: Parse requests, define tool schemas, dispatch graph/query/index operations, and serialize responses.

**Indexing Orchestrator:**
- Location: `src/pipeline/pipeline.c`
- Triggers: Repository indexing commands from MCP/UI flows and watcher-driven reindexing.
- Responsibilities: Discover files, build structure, run extraction/resolution passes, enrich the graph, and persist results.

**HTTP UI Server:**
- Location: `src/ui/http_server.c`
- Triggers: `--ui=true` configuration or embedded UI startup from `src/main.c`.
- Responsibilities: Serve embedded frontend assets, proxy `/rpc`, expose `/api/*` endpoints, and provide layout/log/project control APIs.

**Frontend Bootstrap:**
- Location: `graph-ui/src/main.tsx`
- Triggers: Browser load of the embedded or Vite-served UI.
- Responsibilities: Mount `graph-ui/src/App.tsx` and hand control to the React application shell.

## Error Handling

**Strategy:** Return explicit status codes in native modules, keep long-lived services alive where possible, and centralize user-facing serialization at the transport layer.

**Patterns:**
- Indexing and runtime services use integer return codes and NULL checks throughout files such as `src/main.c`, `src/pipeline/pipeline.c`, and `src/watcher/watcher.c`.
- The MCP layer in `src/mcp/mcp.c` converts parse and dispatch failures into JSON-RPC error payloads.
- The HTTP layer in `src/ui/http_server.c` replies with HTTP status codes and bounded JSON/text responses instead of crashing the server thread.

## Cross-Cutting Concerns

**Logging:** Structured logging primitives come from `src/foundation/log.c`; UI log capture is mirrored into a ring buffer in `src/ui/http_server.c`.

**Validation:** Input validation is distributed at subsystem boundaries: CLI flag parsing in `src/main.c`, JSON validation in `src/mcp/mcp.c`, repository/file filtering in `src/discover/discover.c`, and SQL/schema constraints in `src/store/store.c`.

**Authentication:** Not applicable for a multi-user service. Access is constrained by local process and localhost-only serving in `src/ui/http_server.c`, which binds to `127.0.0.1` and only reflects localhost origins for CORS.

---

*Architecture analysis: 2026-04-11*
