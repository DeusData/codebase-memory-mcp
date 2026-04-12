# Technology Stack

**Analysis Date:** 2026-04-11

## Languages

**Primary:**
- C11 - Core MCP server, indexing pipeline, storage layer, watcher, CLI, and localhost UI server in `src/main.c`, `src/mcp/mcp.c`, `src/pipeline/pipeline.c`, `src/store/store.c`, and `src/ui/http_server.c`.

**Secondary:**
- TypeScript + TSX - Graph UI and browser-side RPC client in `graph-ui/src/main.tsx`, `graph-ui/src/App.tsx`, and `graph-ui/src/api/rpc.ts`.
- Shell - Build, test, install, security, and packaging automation in `scripts/build.sh`, `scripts/test.sh`, `scripts/setup.sh`, and `scripts/embed-frontend.sh`.
- PowerShell - Windows installation flow in `install.ps1` and `scripts/setup-windows.ps1`.
- Python - Targeted proof and runtime verification scripts in `scripts/test_gdscript_proof_same_script_calls.py` and `scripts/test_mcp_rapid_init.py`.

## Runtime

**Environment:**
- Native compiled binary with no required managed runtime for the main product; the executable entry point is `src/main.c`.
- Browser runtime for the optional UI served by the native HTTP server in `src/ui/http_server.c`.
- Node.js 22 is the documented CI/build runtime for the frontend in `.github/workflows/_build.yml`.

**Package Manager:**
- Native build orchestration uses GNU Make via `Makefile.cbm` and wrapper scripts in `scripts/build.sh` and `scripts/test.sh`.
- Frontend packages use npm in `graph-ui/package.json`.
- Lockfile: present at `graph-ui/package-lock.json` and `.opencode/package-lock.json`.

## Frameworks

**Core:**
- Tree-sitter 0.24.4 - Multi-language parsing and grammar runtime vendored into the C engine; version is declared in `.github/workflows/release.yml` and used from `internal/cbm/` via `internal/cbm/cbm.c` and `internal/cbm/ts_runtime.c`.
- SQLite 3.49.1 - Persistent graph storage and query backend; version is declared in `.github/workflows/release.yml` and the integration lives in `src/store/store.c`.
- yyjson 0.10.0 - JSON parsing and config serialization in `src/mcp/mcp.c`, `src/discover/userconfig.c`, and `src/ui/config.c`.
- Mongoose 7.16 - Embedded HTTP server for the local graph UI in `src/ui/http_server.c`.
- mimalloc 2.1.7 - Native allocator integration defined in `Makefile.cbm` and vendored under `vendored/mimalloc/`.

**Testing:**
- Native C test suite driven by `make -f Makefile.cbm test`; test source groups are declared in `Makefile.cbm` and live under `tests/`.
- Vitest 3.0.0 for the frontend in `graph-ui/package.json`.

**Build/Dev:**
- Vite 6.0.0 - Frontend dev server and production bundler in `graph-ui/package.json` and `graph-ui/vite.config.ts`.
- React 19 - UI framework in `graph-ui/package.json` with app bootstrap in `graph-ui/src/main.tsx`.
- TypeScript 5.7.0 - Frontend type-check/build step in `graph-ui/package.json` and `graph-ui/tsconfig.json`.
- Tailwind CSS 4.x - UI styling via `graph-ui/package.json` and Vite plugin setup in `graph-ui/vite.config.ts`.
- GitHub Actions - CI, build, smoke, soak, and release automation in `.github/workflows/dry-run.yml`, `.github/workflows/_build.yml`, `.github/workflows/_test.yml`, and `.github/workflows/release.yml`.

## Key Dependencies

**Critical:**
- `vendored/sqlite3` / SQLite 3.49.1 - Core knowledge-graph persistence; `src/store/store.c` opens databases and registers query helpers.
- `internal/cbm/vendored/ts_runtime` / Tree-sitter 0.24.4 - Parsing backbone for repository indexing, wired through `internal/cbm/cbm.c` and `Makefile.cbm`.
- `vendored/yyjson` / yyjson 0.10.0 - Required for MCP payloads and config file mutation in `src/mcp/mcp.c`, `src/cli/cli.c`, and `src/ui/config.c`.
- `vendored/mongoose` / Mongoose 7.16 - Required when shipping the UI-enabled binary served by `src/ui/http_server.c`.

**Infrastructure:**
- Optional `libgit2` - Auto-detected in `Makefile.cbm`; accelerates git history parsing when available.
- `zlib` - Required system dependency for native builds, documented in `README.md` and installed in `.github/workflows/_build.yml`.
- `@react-three/fiber`, `@react-three/drei`, `three`, and `postprocessing` - 3D graph rendering stack in `graph-ui/package.json` and scene components under `graph-ui/src/components/`.
- `@vitejs/plugin-react` and `@tailwindcss/vite` - Frontend build plugins configured in `graph-ui/vite.config.ts`.
- `nan` and `tree-sitter-cli` - Grammar-package tooling in `tools/tree-sitter-form/package.json` and `tools/tree-sitter-magma/package.json`.

## Configuration

**Environment:**
- No required runtime `.env` file is detected in the workspace root.
- User language-extension overrides are loaded from `$XDG_CONFIG_HOME/codebase-memory-mcp/config.json` or `~/.config/codebase-memory-mcp/config.json`, plus per-project `.codebase-memory.json`, as implemented in `src/discover/userconfig.c`.
- UI settings persist to `~/.cache/codebase-memory-mcp/config.json` with keys `ui_enabled` and `ui_port`, as implemented in `src/ui/config.c`.
- Build-time overrides are passed through shell environment variables such as `CBM_ARCH`, `CC`, and `CXX` in `scripts/env.sh`, `scripts/build.sh`, and `scripts/test.sh`.
- Windows installer supports optional `CBM_DOWNLOAD_URL` override in `install.ps1`.

**Build:**
- Native compilation is defined in `Makefile.cbm`.
- Canonical build entrypoint is `scripts/build.sh`.
- Canonical test entrypoint is `scripts/test.sh`.
- Frontend bundling is configured in `graph-ui/vite.config.ts` and embedded into the C binary by `scripts/embed-frontend.sh`.
- Cross-platform CI build matrices live in `.github/workflows/_build.yml` and `.github/workflows/_test.yml`.

## Platform Requirements

**Development:**
- C compiler plus C++ compiler are required for source builds, documented in `README.md` and enforced by `scripts/env.sh`.
- zlib development headers are required on Linux, installed in `.github/workflows/_build.yml` and `.github/workflows/_test.yml`.
- Node.js is required to build `graph-ui/`, with CI pinned to Node 22 in `.github/workflows/_build.yml`.
- Git is required by installer and source checkout flow in `scripts/setup.sh`.

**Production:**
- Deployment target is a single native binary for macOS, Linux, and Windows, packaged by `.github/workflows/_build.yml` and released through `.github/workflows/release.yml`.
- Optional UI variant embeds the built frontend into the same binary and serves it locally from `src/ui/http_server.c`.

---

*Stack analysis: 2026-04-11*
