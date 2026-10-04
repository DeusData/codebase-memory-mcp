# RFC 003: `audit_test_coverage` — Test-to-Production Code Mapping & Untested Entry Point Discovery

- **RFC Number:** 003
- **Title:** `audit_test_coverage` — Test-to-Production Code Mapping & Untested Entry Point Discovery
- **Status:** Proposed
- **Author:** Codebase Memory Architecture Team
- **Target Subsystem:** `src/mcp/`, `src/store/`, `src/cli/`, `graph-ui/`
- **Target Version:** v0.12.0
- **Created:** 2026-10-04

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
When building new features, reviewing PRs, or refactoring in unfamiliar codebases, engineers and AI assistants face a major blindspot:
1. **Does this function or module have any automated tests covering it?**
2. **If so, where are those tests located, and how do they exercise the code?**
3. **Across the entire repository, what are the highest-priority, high-importance entry points and core algorithms that have ZERO automated test coverage?**

Currently:
- The existing MCP tool `check_index_coverage` only inspects **AST parser syntax completeness** (e.g. whether Tree-sitter failed on a syntax error or skipped an oversized file). It provides **zero information about test suite coverage**.
- The indexing pipeline in `src/pipeline/pass_tests.c` already analyzes test directories (`tests/`, `__tests__/`, `spec/`), identifies test functions (`is_test=true`), and constructs `TESTS` and `TESTS_FILE` edges.
- However, **there is no MCP tool enabling agents to query this test graph**. As a result, agents frequently generate redundant tests, modify code without running relevant test suites, or fail to write tests for high-risk, untested core functions.

### 1.2 The Solution
Introduce `audit_test_coverage`.
This tool leverages the pre-computed `TESTS` and `TESTS_FILE` edges together with the calculated symbol `importance` scores to:
1. Locate all test functions and test files covering a specific symbol or file.
2. Generate an **Untested High-Priority Audit**: ranking production functions that have zero test callers by their importance score and public ingress exposure.
3. Compute project-wide test-to-code ratio and architectural test distribution.

---

## 2. Existing SQLite Database Assets (Zero Reindexing Required)

| Entity | Type | Properties / Details |
| :--- | :--- | :--- |
| `edges` | `type = 'TESTS'` | Direct edge from test function node to production function node |
| `edges` | `type = 'TESTS_FILE'` | Edge from test `File` node to production `File` node |
| `nodes` | `properties.is_test` | Boolean flag marking test suites, fixtures, and assertions |
| `nodes` | `properties.is_entry_point` | Flag marking public API controllers, CLI commands, and exports |
| `nodes` | `properties.importance` | Weighted PageRank/fan-in metric computed during indexing ([`pass_importance.c`](file:///c:/AI/Source/codebase-memory-mcp-ui/src/pipeline/pass_importance.c)) |

---

## 3. Tool Specification (MCP JSON-RPC)

### 3.1 Tool Registration
- **Name:** `audit_test_coverage`
- **Description:** Audits test coverage mapping across a project: finds tests covering a specific symbol/file, or identifies critical untested entry points ranked by architectural importance score.

### 3.2 Input Parameters (`inputSchema`)

```json
{
  "type": "object",
  "properties": {
    "project": {
      "type": "string",
      "description": "Target project identifier registered in Codebase Memory."
    },
    "mode": {
      "type": "string",
      "enum": ["gaps", "symbol_tests", "summary"],
      "default": "gaps",
      "description": "Operation mode: 'gaps' (untested critical symbols), 'symbol_tests' (tests covering a target), 'summary' (aggregate project test stats)."
    },
    "target": {
      "type": "string",
      "description": "Qualified symbol name or file path (required when mode is 'symbol_tests')."
    },
    "min_importance": {
      "type": "number",
      "default": 1.0,
      "description": "Minimum importance score threshold for reporting untested symbols in 'gaps' mode."
    },
    "limit": {
      "type": "integer",
      "default": 25,
      "minimum": 1,
      "maximum": 100,
      "description": "Maximum number of untested symbols or tests to return."
    }
  },
  "required": ["project"]
}
```

### 3.3 Output Response Structure

#### Scenario A: Untested Gaps (`mode: "gaps"`)

```json
{
  "project": "codebase-memory-mcp-ui",
  "mode": "gaps",
  "summary": {
    "total_production_functions": 18450,
    "tested_production_functions": 1210,
    "untested_entry_points": 18,
    "untested_critical_functions": 42
  },
  "critical_untested_symbols": [
    {
      "qualified_name": "src.pipeline.pipeline_incremental.apply_graph_patch",
      "file_path": "src/pipeline/pipeline_incremental.c",
      "line": 1420,
      "importance": 14.82,
      "is_entry_point": true,
      "inbound_callers_count": 9,
      "public_route_exposure": false,
      "recommendation": "High architectural centrality (importance 14.82). Add dedicated test suite in tests/test_incremental.c."
    },
    {
      "qualified_name": "src.ui.http_server.handle_delete_project",
      "file_path": "src/ui/http_server.c",
      "line": 560,
      "importance": 9.35,
      "is_entry_point": true,
      "inbound_callers_count": 2,
      "public_route_exposure": true,
      "route": "DELETE /api/v1/projects/:name",
      "recommendation": "Public mutating route without automated tests. Add integration test in tests/test_http.c."
    }
  ]
}
```

#### Scenario B: Target Symbol Tests (`mode: "symbol_tests"`)

```json
{
  "target": "cbm_store_upsert_project",
  "file_path": "src/store/store.c",
  "direct_tests": [
    {
      "test_symbol": "tests.test_mcp.test_project_lifecycle",
      "file_path": "tests/test_mcp.c",
      "line": 11538,
      "edge_type": "TESTS"
    }
  ],
  "indirect_tests": [
    {
      "test_symbol": "tests.test_cli.test_cli_index_command",
      "file_path": "tests/test_cli.c",
      "line": 482,
      "distance": 2,
      "call_path": "test_cli_index_command -> cbm_cli_index -> cbm_store_upsert_project"
    }
  ]
}
```

---

## 4. Query Architecture & SQL Implementation

### 4.1 Untested Critical Symbols Query (`mode: "gaps"`)

```sql
SELECT 
    n.id,
    n.name,
    n.qualified_name,
    n.file_path,
    n.start_line,
    coalesce(json_extract(n.properties, '$.importance'), 0.0) AS importance,
    coalesce(json_extract(n.properties, '$.is_entry_point'), 0) AS is_entry_point,
    (SELECT COUNT(*) FROM edges e_in WHERE e_in.target_id = n.id AND e_in.type = 'CALLS') AS caller_count,
    (SELECT COUNT(*) FROM edges e_r JOIN nodes r ON e_r.source_id = r.id AND r.label = 'Route' WHERE e_r.target_id = n.id) AS route_count
FROM nodes n
WHERE n.project = ?1
  AND n.label IN ('Function', 'Method')
  AND (json_extract(n.properties, '$.is_test') IS NULL OR json_extract(n.properties, '$.is_test') != 1)
  -- Must have ZERO incoming TESTS edges
  AND NOT EXISTS (
      SELECT 1 FROM edges e_test
      WHERE e_test.project = ?1
        AND e_test.target_id = n.id
        AND e_test.type = 'TESTS'
  )
  -- Filter by minimum architectural importance or entry-point status
  AND (
      coalesce(json_extract(n.properties, '$.importance'), 0.0) >= ?2
      OR json_extract(n.properties, '$.is_entry_point') = 1
  )
ORDER BY is_entry_point DESC, importance DESC
LIMIT ?3;
```

### 4.2 Target Symbol Tests Query (`mode: "symbol_tests"`)

```sql
-- Direct tests
SELECT 
    t.name, t.qualified_name, t.file_path, t.start_line, e.type
FROM edges e
JOIN nodes t ON e.source_id = t.id
WHERE e.project = ?1
  AND e.target_id = ?2
  AND (e.type = 'TESTS' OR json_extract(t.properties, '$.is_test') = 1);
```

---

## 5. CLI & UI Integration

### 5.1 CLI Command
```bash
# Find untested high-priority functions
cbm cli audit_test_coverage --project codebase-memory-mcp-ui --mode gaps

# Check what tests exercise cbm_store_close
cbm cli audit_test_coverage --project codebase-memory-mcp-ui --mode symbol_tests --target cbm_store_close
```

### 5.2 UI Integration (`graph-ui`)
- **ControlTab / ProjectsTab**:
  - Add a **"Test Safety Index"** widget displaying total test coverage percentage.
  - One-click list of top 10 untested critical entry points.
- **NodeDetailPanel**:
  - Add a **"Covering Tests"** badge. If empty, render a warning badge: `⚠️ Untested (Importance: 14.8)`.

---

## 6. Acceptance Criteria & Test Plan

1. **Unit Tests (`tests/test_mcp.c`)**:
   - `test_audit_test_gaps`: Verifies functions with no incoming `TESTS` edges are listed in descending importance order.
   - `test_audit_symbol_tests`: Verifies direct and indirect test callers are resolved for a known symbol.
   - `test_audit_test_excludes_test_nodes`: Verifies test functions themselves are not reported as untested code.
