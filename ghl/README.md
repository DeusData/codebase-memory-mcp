# GHL Fleet Server

GHL-specific wrapper around [codebase-memory-mcp](../README.md) that indexes the GoHighLevel GitHub org as a fleet and exposes the knowledge graph over HTTP.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   GHL Fleet Server                       │
│                  (single Go process)                     │
│                                                         │
│  ┌──────────┐  ┌──────────────┐  ┌───────────────────┐  │
│  │  HTTP     │  │  Fleet       │  │  GitHub Webhook   │  │
│  │  Bridge   │  │  Indexer     │  │  Handler          │  │
│  │          │  │              │  │                   │  │
│  │ /mcp     │  │ Cron-based   │  │ POST /webhooks/   │  │
│  │ /health  │  │ batch index  │  │      github       │  │
│  │ /status  │  │ of all repos │  │                   │  │
│  └────┬─────┘  └──────┬───────┘  └────────┬──────────┘  │
│       │               │                   │             │
│  ┌────▼───────────────▼───────────────────▼──────────┐  │
│  │           MCP Binary Process Pool                  │  │
│  │                                                    │  │
│  │  Bridge Pool (2)     Indexer Pool (3)   Discovery  │  │
│  │  ── query serving    ── repo indexing   Pool (3)   │  │
│  │                                                    │  │
│  │  Each pool member is a codebase-memory-mcp binary  │  │
│  │  subprocess communicating over stdio JSON-RPC.     │  │
│  └────────────────────┬──────────────────────────────┘  │
│                       │                                  │
│  ┌────────────────────▼──────────────────────────────┐  │
│  │                   SQLite                           │  │
│  │  Per-repo .db files in CBM_CACHE_DIR              │  │
│  │  Knowledge graph: nodes, edges, FTS5 index        │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
          │                              │
          ▼                              ▼
   ┌─────────────┐              ┌──────────────┐
   │  GitHub API  │              │  GCS / PVC   │
   │  (clone,     │              │  (artifact   │
   │   activity   │              │   persist)   │
   │   check)     │              │              │
   └─────────────┘              └──────────────┘
```

## How It Works End-to-End

### 1. Startup

The server loads `REPOS.yaml` (the curated fleet manifest) and starts three process pools:

| Pool | Size | Purpose |
|------|------|---------|
| **Bridge** | 2 | Serve live MCP queries from agents/users |
| **Indexer** | 3 | Run `index_repository` for fleet indexing |
| **Discovery** | 3 | Power `discover_projects` cross-repo search |

Each pool member is a `codebase-memory-mcp` C binary subprocess. If a subprocess crashes, the pool auto-replaces it.

If artifact persistence is enabled, the server hydrates from GCS or local filesystem — restoring previously-built SQLite indexes so queries work immediately without waiting for a full index run.

### 2. Indexing Pipeline

Indexing happens through three triggers:

| Trigger | When | Mode | What |
|---------|------|------|------|
| **Cron incremental** | Daily at 3 AM | `fast` | Index all repos in manifest, skip similarity/semantic passes |
| **Cron full** | Weekly Sunday 2 AM | `full` | Force re-index everything including similarity/semantic edges |
| **GitHub webhook** | On push to default branch | `fast` | Re-index the single pushed repo |

#### Per-repo index flow

```
1. Activity check (GitHub API)
   └─ Skip repos with no commits in 7 days (incremental only)

2. Git update
   └─ First run: git clone --depth=1
   └─ Subsequent: git fetch --depth=1 + git checkout -f
       └─ checkout preserves mtimes on unchanged files
          (critical for incremental indexing)

3. C binary: index_repository
   └─ Check existing SQLite DB for stored file hashes
   └─ Compare mtime+size for each file
   ┌─────────────────────────────────────────────┐
   │ If DB exists and hashes match most files:   │
   │   → INCREMENTAL: only re-parse changed files│
   │   → Skip unchanged files entirely           │
   │   → No-op if nothing changed                │
   │                                             │
   │ If no DB or mode changed:                   │
   │   → FULL: parse all files from scratch      │
   └─────────────────────────────────────────────┘
   └─ Pipeline passes:
      1. Discover files (respecting .gitignore)
      2. Extract definitions (tree-sitter AST)
      3. Resolve imports
      4. Resolve calls (registry + LSP for Go/C/C++)
      5. Resolve usages / type refs
      6. Semantic edges (inherits, decorates)
      7. [full/moderate only] Similarity (MinHash/LSH)
      8. [full/moderate only] Semantic edges (TF-IDF)
      9. Post-passes: tests, HTTP links, config, git history

4. Persist artifact
   └─ Sync .db file to GCS bucket or local PVC

5. Invalidate discovery cache
   └─ Next discover_projects call rebuilds candidates
```

### 3. Query Serving

Agents connect to the HTTP bridge at `/mcp` using JSON-RPC 2.0. The bridge:

1. Authenticates via GitHub org membership (bearer token → GitHub API → verify org)
2. Routes to the correct handler:
   - `tools/list` → returns 14 core tools + `discover_projects` + `customer_surface`
   - `tools/call` → dispatches to bridge pool or Go-native handlers
3. Caches results for `search_code`, `search_graph`, `get_code_snippet` (1000 entries, 60s TTL)

#### Go-native fast paths

Two tools bypass the C binary entirely for performance:

- **`search_code`** — Go regexp over SQLite file index (64 parallel goroutines). The C binary uses `grep -rn` which is catastrophically slow on GCS Fuse mounts.
- **`customer_surface`** — Go-only composite enricher. Fuses product area + Vue component metadata + frontend fetch calls. No C binary equivalent.

### 4. Discovery (Cross-Repo Search)

The `discover_projects` tool finds relevant repos for a task description:

1. BM25 text search across all indexed repos
2. Graph-based confidence scoring
3. Returns ranked candidates with reasons

### 5. Enrichment Layer

GHL-specific enrichers add org knowledge on top of the core graph:

| Enricher | What it does |
|----------|-------------|
| **Product map** | Maps repos/paths → product areas (CRM, Calendars, etc.) |
| **MFA registry** | Maps micro-frontend app slugs → repos |
| **Topic registry** | Maps Pub/Sub topics → producer/consumer repos |
| **Route callers** | Maps internal HTTP routes → calling services |
| **NestJS extractor** | Extracts controllers, injectables, internal-request calls |
| **Semantic classifier** | Classifies files into a taxonomy (API, UI, config, etc.) |
| **DTO contract tracer** | Tracks shared DTOs across service boundaries |

## Configuration

All configuration is via environment variables (overridable in Helm values):

### Core

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP server port |
| `REPOS_MANIFEST` | `/app/REPOS.local.yaml` | Path to fleet manifest YAML |
| `CBM_CACHE_DIR` | `/tmp/codebase-memory-mcp` | SQLite index storage |
| `FLEET_CACHE_DIR` | `/tmp/fleet-repos` | Git clone storage |

### Process Pools

| Variable | Default | Description |
|----------|---------|-------------|
| `BRIDGE_CLIENTS` | `2` | Concurrent query-serving C binaries |
| `FLEET_CONCURRENCY` | `3` | Parallel repo indexing goroutines |
| `INDEXER_CLIENTS` | `3` | C binary subprocesses for indexing |
| `INDEXER_CLIENT_MAX_USES` | `20` | Recycle subprocess after N uses (prevents memory leaks) |
| `DISCOVERY_CLIENTS` | derived | C binaries for discovery queries |

### Scheduling

| Variable | Default | Description |
|----------|---------|-------------|
| `SCHEDULED_INDEXING_ENABLED` | `true` | Enable/disable cron-based indexing |
| `CRON_INCREMENTAL` | `0 3 * * *` | Incremental index schedule (daily 3 AM) |
| `CRON_FULL` | `0 2 * * 0` | Full index schedule (Sunday 2 AM) |

### Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `GITHUB_TOKEN` | — | Token for cloning private repos + activity checks |
| `GITHUB_ALLOWED_ORGS` | — | Comma-separated GitHub orgs for bearer auth |
| `GITHUB_AUTH_CACHE_TTL_MS` | `300000` | Cache auth results for 5 minutes |

### Artifacts

| Variable | Default | Description |
|----------|---------|-------------|
| `ARTIFACTS_ENABLED` | `true` | Persist/hydrate SQLite indexes |
| `ARTIFACTS_BACKEND` | `filesystem` | `filesystem` or `gcs` |
| `ARTIFACTS_BUCKET` | — | GCS bucket name (when backend=gcs) |

## Deployment

Deployed on GKE via Helm:

```bash
helm upgrade --install codebase-memory-mcp \
  deployments/ghl/helm \
  -f deployments/ghl/helm/values.yaml \
  -f deployments/ghl/helm/values-staging.yaml
```

The deployment uses:
- **1 replica** with Recreate strategy (SQLite requires single-writer)
- **PVC** (20Gi) for clone cache and index persistence
- **Istio VirtualService** for internal mesh routing
- **Secrets** from GCP Secret Manager (GitHub token, bearer token, webhook secret)

## API Endpoints

| Endpoint | Method | Auth | Description |
|----------|--------|------|-------------|
| `/mcp` | POST | Bearer | MCP JSON-RPC 2.0 bridge |
| `/health` | GET | None | Liveness/readiness probe |
| `/status` | GET | Bearer | Fleet status (repos, versions, pool sizes) |
| `/webhooks/github` | POST | HMAC | GitHub push webhook |
| `/index/{repoSlug}` | POST | Bearer | Manually trigger single-repo re-index |
| `/index-all` | POST | Bearer | Trigger full fleet re-index |

## Cost Optimization

The fleet is tuned for minimal compute:

- **43 curated repos** (not the full 480-repo org)
- **Daily incremental** cron (not every 6 hours)
- **`fast` mode** for incremental runs (skips similarity/semantic passes)
- **Incremental indexing** — only re-parses files whose mtime+size changed
- **`git checkout -f`** instead of `git reset --hard` to preserve file mtimes
- **Subprocess reuse** — each C binary handles 20 repos before recycling
- **2 bridge clients** (not 8) — idle memory savings
- **Search cache** — 1000 entries, 60s TTL, prevents duplicate work

## Local Development

```bash
# Generate local manifest from repos on disk
cd ghl && go run ./cmd/genlocalmanifest

# Run the fleet server locally
go run ./cmd/server

# Run tests
go test ./...
```

## Directory Structure

```
ghl/
├── cmd/
│   ├── server/          # Fleet server entry point
│   └── genlocalmanifest/ # Generate REPOS.local.yaml from disk
├── internal/
│   ├── auth/            # GitHub org membership auth
│   ├── bridge/          # HTTP → MCP stdio bridge
│   ├── cachepersist/    # GCS / filesystem artifact sync
│   ├── discovery/       # Cross-repo project discovery
│   ├── enricher/        # GHL-specific metadata enrichers
│   ├── indexer/         # Fleet clone + index orchestrator
│   ├── manifest/        # REPOS.yaml loader
│   ├── mcp/             # MCP JSON-RPC client (stdio)
│   ├── searchtools/     # Go-native search_code + customer_surface
│   └── webhook/         # GitHub push webhook handler
├── go.mod
└── go.sum
```
