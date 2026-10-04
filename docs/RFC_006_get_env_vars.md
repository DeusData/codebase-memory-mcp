# RFC 006: `get_env_vars` — Environment Variable & Configuration Topology Inspector

- **RFC Number:** 006
- **Title:** `get_env_vars` — Environment Variable & Configuration Topology Inspector
- **Status:** Proposed
- **Author:** Codebase Memory Architecture Team
- **Target Subsystem:** `src/mcp/`, `src/store/`, `src/cli/`, `graph-ui/`
- **Target Version:** v0.12.0
- **Created:** 2026-10-04

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Setting up, running, or deploying an existing codebase is frequently hindered by environment configuration ambiguity:
1. **Missing Variables:** Developers and AI agents run into runtime crashes (`process.env.DB_PASSWORD is undefined`, `KeyError: 'REDIS_URL'`) because required environment variables are undocumented or scattered across dozens of files.
2. **Scattered Consumption:** Environment variables are read inconsistently via `getenv()`, `process.env.VAR`, `os.environ.get()`, `std::env::var()`, or configuration parser structs.
3. **Hidden Defaults & Fallbacks:** Some variables have hardcoded fallback defaults, while others cause fatal startup crashes if missing.

Currently:
- The parser passes in `codebase-memory-mcp` extract environment variable accesses and populate **67 `EnvVar` nodes** and **394 `CONFIGURES` edges** in the SQLite database.
- However, **there is no MCP tool to query this environment configuration map**.
- To understand environment requirements, an AI agent must fall back to grepping for variable strings across the codebase, missing AST-resolved references and configuration linkages.

### 1.2 The Solution
Introduce `get_env_vars`.
This tool inspects the `EnvVar` nodes and `CONFIGURES` edges to:
- Generate a comprehensive manifest of all environment variables used by the codebase.
- Map each environment variable to the exact files, functions, and modules that read it.
- Extract detected default fallback values, required status, and configuration file references (`.env.example`, `docker-compose.yml`, `config.yaml`).

---

## 2. Existing SQLite Database Assets (Zero Reindexing Required)

```
        ┌────────────────────────────────────────────────────────┐
        │ Node: label = 'EnvVar'                                 │
        │ name: "CBM_DB_PATH"                                    │
        │ properties: {                                          │
        │   "default_value": "~/.codebase-memory/data.db",       │
        │   "is_required": false                                 │
        │ }                                                      │
        └───────────────────────────┬────────────────────────────┘
                                    │
                                    │ Edge: CONFIGURES
                                    │ properties: {
                                    │   "read_line": 842,
                                    │   "access_method": "getenv"
                                    │ }
                                    ▼
        ┌────────────────────────────────────────────────────────┐
        │ Node: label = 'Function'                               │
        │ qualified_name: "src.store.store.cbm_store_open"       │
        │ file_path: "src/store/store.c"                         │
        └────────────────────────────────────────────────────────┘
```

- **`EnvVar` Nodes (67 currently in DB)**: Extracted during AST parsing.
- **`CONFIGURES` Edges (394 currently in DB)**: Linking `EnvVar` nodes to the functions that read them.

---

## 3. Tool Specification (MCP JSON-RPC)

### 3.1 Tool Registration
- **Name:** `get_env_vars`
- **Description:** Returns the environment variables and configuration parameters used by the project, including where they are read in code, detected fallback defaults, and consuming modules.

### 3.2 Input Parameters (`inputSchema`)

```json
{
  "type": "object",
  "properties": {
    "project": {
      "type": "string",
      "description": "Target project identifier registered in Codebase Memory."
    },
    "name_pattern": {
      "type": "string",
      "description": "Optional substring or regex to filter variable names (e.g. 'DB_*' or 'PORT')."
    },
    "include_consumers": {
      "type": "boolean",
      "default": true,
      "description": "Include the list of functions and files that consume each environment variable."
    },
    "generate_env_example": {
      "type": "boolean",
      "default": false,
      "description": "If true, generates a ready-to-use .env.example template string."
    }
  },
  "required": ["project"]
}
```

### 3.3 Output Response Structure

```json
{
  "project": "codebase-memory-mcp-ui",
  "total_env_vars": 67,
  "env_vars": [
    {
      "name": "CBM_DB_PATH",
      "detected_default": "~/.codebase-memory/data.db",
      "is_required": false,
      "access_methods": ["getenv"],
      "consumers_count": 3,
      "consumers": [
        {
          "symbol": "src.store.store.cbm_store_open",
          "file_path": "src/store/store.c",
          "line": 842
        },
        {
          "symbol": "src.cli.cli.cbm_cli_main",
          "file_path": "src/cli/cli.c",
          "line": 128
        }
      ],
      "description": "Path to the primary SQLite graph storage file."
    },
    {
      "name": "CBM_LOG_LEVEL",
      "detected_default": "INFO",
      "is_required": false,
      "access_methods": ["getenv"],
      "consumers_count": 5,
      "consumers": [
        {
          "symbol": "src.foundation.log.cbm_log_init",
          "file_path": "src/foundation/log.c",
          "line": 54
        }
      ],
      "description": "Log verbosity level (DEBUG, INFO, WARN, ERROR)."
    }
  ],
  "env_example_template": "# Generated by Codebase Memory\nCBM_DB_PATH=~/.codebase-memory/data.db\nCBM_LOG_LEVEL=INFO\n"
}
```

---

## 4. Query Architecture & C Implementation

### 4.1 SQL Query (`src/store/store.c`)

```sql
SELECT 
    v.id AS env_id,
    v.name AS var_name,
    json_extract(v.properties, '$.default_value') AS default_val,
    coalesce(cast(json_extract(v.properties, '$.is_required') AS INTEGER), 0) AS is_required,
    f.qualified_name AS consumer_qn,
    f.file_path AS consumer_file,
    f.start_line AS consumer_line,
    json_extract(e.properties, '$.access_method') AS access_method
FROM nodes v
LEFT JOIN edges e ON (e.source_id = v.id AND e.type = 'CONFIGURES')
LEFT JOIN nodes f ON e.target_id = f.id
WHERE v.project = ?1
  AND v.label = 'EnvVar'
  AND (?2 IS NULL OR v.name LIKE ?2)
ORDER BY v.name ASC, f.file_path ASC;
```

---

## 5. CLI & UI Integration

### 5.1 CLI Command
```bash
# List all environment variables
cbm cli get_env_vars --project codebase-memory-mcp-ui

# Generate .env.example
cbm cli get_env_vars --project codebase-memory-mcp-ui --generate-env-example
```

### 5.2 UI Integration (`graph-ui`)
- **ControlTab**:
  - Add an **"Environment & Configuration"** card listing all active environment variables, showing which ones are currently set in the running process vs which ones are falling back to defaults.
- **ToolsTab**:
  - Direct parameter inspection and `.env` template export.

---

## 6. Acceptance Criteria & Test Plan

1. **Unit Tests (`tests/test_mcp.c`)**:
   - `test_get_env_vars_catalog`: Verifies all `EnvVar` nodes are returned.
   - `test_get_env_vars_consumers`: Verifies `CONFIGURES` edges link variables to their consumer functions.
   - `test_get_env_vars_filtering`: Verifies pattern filter matches wildcard queries.
