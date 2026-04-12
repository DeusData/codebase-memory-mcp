# External Integrations

**Analysis Date:** 2026-04-11

## APIs & External Services

**Distribution & Updates:**
- GitHub Releases - Binary download source for installers and self-update flows.
  - SDK/Client: shell `curl`/`wget` in `scripts/setup.sh`, PowerShell `Invoke-WebRequest` in `install.ps1`, and native CLI download logic in `src/cli/cli.c`.
  - Auth: none for public release downloads.
- GitHub REST API - Release metadata lookup for latest-version detection.
  - SDK/Client: `https://api.github.com/repos/DeusData/codebase-memory-mcp/releases/latest` in `src/mcp/mcp.c`, `scripts/setup.sh`, and `scripts/setup-windows.ps1`.
  - Auth: none for anonymous metadata checks in code paths read here.

**Local UI / Browser Contract:**
- Localhost HTTP API - Optional graph viewer API served by `src/ui/http_server.c` and consumed by `graph-ui/src/api/rpc.ts`, `graph-ui/src/hooks/useGraphData.ts`, `graph-ui/src/components/ControlTab.tsx`, and `graph-ui/src/components/StatsTab.tsx`.
  - SDK/Client: browser `fetch` plus Mongoose server implementation.
  - Auth: no user auth; access is constrained to localhost binding and localhost-only CORS in `src/ui/http_server.c`.

**Agent Tooling Integration:**
- Local coding-agent config files - The installer writes MCP server entries for supported agents from CLI code in `src/cli/cli.c` and installer flows in `scripts/setup.sh` and `install.ps1`.
  - SDK/Client: direct JSON/TOML file mutation using `yyjson` and shell/PowerShell utilities.
  - Auth: not applicable.

**Release Security Services:**
- Sigstore Cosign - Artifact signing in `.github/workflows/release.yml`.
  - SDK/Client: `cosign sign-blob` CLI.
  - Auth: GitHub OIDC/id-token in `.github/workflows/release.yml`.
- VirusTotal - Release verification scan in `.github/workflows/release.yml`.
  - SDK/Client: `crazy-max/ghaction-virustotal` GitHub Action.
  - Auth: `VIRUS_TOTAL_SCANNER_API_KEY` GitHub Actions secret.

## Data Storage

**Databases:**
- SQLite (local embedded) - Knowledge-graph persistence handled in `src/store/store.c` with schema/query logic behind `src/store/store.h`.
  - Connection: no external DSN; databases are local files resolved under the cache directory referenced by `README.md`, `src/ui/config.c`, and `src/ui/http_server.c`.
  - Client: vendored SQLite C API in `vendored/sqlite3/`.

**File Storage:**
- Local filesystem only - Persistent config and cache files are written under `~/.cache/codebase-memory-mcp/` and user config paths handled by `src/ui/config.c` and `src/discover/userconfig.c`.

**Caching:**
- Local filesystem cache only - Cache directory is resolved by native platform helpers and used for DB/config storage in `src/ui/config.c` and `src/ui/http_server.c`.

## Authentication & Identity

**Auth Provider:**
- None for product runtime.
  - Implementation: MCP stdio mode in `src/main.c` and localhost UI mode in `src/ui/http_server.c` run without end-user login; browser access is limited by `127.0.0.1` binding and strict localhost CORS checks.

## Monitoring & Observability

**Error Tracking:**
- None detected for external SaaS error tracking.

**Logs:**
- Native in-process logging with a ring buffer exposed to the UI by `src/ui/http_server.c` (`GET /api/logs`) and produced through the foundation logging layer used across `src/`.

## CI/CD & Deployment

**Hosting:**
- GitHub Releases hosts downloadable binaries and installers, as referenced in `README.md`, `scripts/setup.sh`, `install.ps1`, and `src/cli/cli.c`.
- Localhost HTTP hosting for the optional UI is provided by the same native binary in `src/ui/http_server.c`.

**CI Pipeline:**
- GitHub Actions - Main automation platform in `.github/workflows/dry-run.yml`, `.github/workflows/_build.yml`, `.github/workflows/_test.yml`, and `.github/workflows/release.yml`.

## Environment Configuration

**Required env vars:**
- None required for normal runtime use of the MCP server or localhost UI.
- Optional build/install overrides:
  - `CBM_ARCH` in `scripts/env.sh`
  - `CC` / `CXX` in `scripts/build.sh` and `scripts/test.sh`
  - `CBM_DOWNLOAD_URL` in `install.ps1`
- CI/release secrets and tokens:
  - `GITHUB_TOKEN` / `GH_TOKEN` in `.github/workflows/release.yml`
  - `VIRUS_TOTAL_SCANNER_API_KEY` in `.github/workflows/release.yml`
  - `CBM_SKIP_PERF` in `.github/workflows/_test.yml`

**Secrets location:**
- GitHub Actions secrets for release automation in `.github/workflows/release.yml`.
- No workspace `.env` file is detected at the repository root.

## Webhooks & Callbacks

**Incoming:**
- None detected. The product exposes MCP stdio in `src/main.c` and a localhost HTTP API in `src/ui/http_server.c`, but no external webhook receiver is configured.

**Outgoing:**
- GitHub API requests for release/update checks from `src/mcp/mcp.c`, `scripts/setup.sh`, and `scripts/setup-windows.ps1`.
- GitHub release downloads from `scripts/setup.sh`, `install.ps1`, and `src/cli/cli.c`.
- VirusTotal submission during release verification in `.github/workflows/release.yml`.

---

*Integration audit: 2026-04-11*
