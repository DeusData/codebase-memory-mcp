# RFC 007: `find_dead_code` — Programmatic Unreferenced Symbol & Dead Code Auditor

- **RFC Number:** 007
- **Title:** `find_dead_code` — Programmatic Unreferenced Symbol & Dead Code Auditor
- **Status:** Proposed
- **Author:** Codebase Memory Architecture Team
- **Target Subsystem:** `src/mcp/`, `src/store/`, `src/cli/`, `graph-ui/`
- **Target Version:** v0.12.0
- **Created:** 2026-10-04

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
As software projects evolve, functions, classes, and variables become obsolete when features are deprecated, refactored, or replaced. This dead code creates significant friction:
1. **Cognitive Overhead:** Developers and AI assistants waste time reading, analyzing, and maintaining functions that are never executed.
2. **Refactoring Pitfalls:** Engineers hesitate to delete old code because they cannot be sure whether it is truly unreferenced or called dynamically.
3. **Build Bloat:** Dead symbols inflate binary sizes, AST index generation times, and test execution suites.

Currently:
- `graph-ui` has a visual "Dead Code" toggle in its 3D graph view (highlighting nodes with in-degree = 0).
- However, **there is NO programmatic MCP tool for agents to audit dead code**.
- An AI assistant performing refactoring, code cleanup, or PR review cannot programmatically fetch candidate dead symbols without writing complex Cypher queries that risk false positives (e.g. accidentally flagging public API exports or test fixtures as dead).

### 1.2 The Solution
Introduce `find_dead_code`.
This tool provides a principled, cross-language dead code auditor that:
- Identifies internal symbols with zero inbound `CALLS`, `USAGE`, or `HTTP_CALLS` edges.
- Cross-references the `lsp_surface` table to exclude public exports intended for downstream consumers.
- Filters out entry points (`is_entry_point: 1`), test helpers (`is_test: true`), route handlers (`Route` targets), and main routines.
- Assigns a **Dead Code Confidence Score** (High / Medium / Low) based on dynamic dispatch risk, reflection, and language semantics.

---

## 2. Existing SQLite Database Assets (Zero Reindexing Required)

| Table | Column / Filter | Usage in Dead Code Detection |
| :--- | :--- | :--- |
| `nodes` | `label IN ('Function', 'Class', 'Variable')` | Candidate symbol set |
| `nodes` | `json_extract(properties, '$.is_entry_point')` | Exclude `main()`, CLI entry points, and top-level scripts |
| `nodes` | `json_extract(properties, '$.is_test')` | Exclude test suites, fixtures, and assertions |
| `edges` | `type IN ('CALLS', 'USAGE', 'HTTP_CALLS')` | In-degree calculation (must be exactly 0) |
| `lsp_surface` | `defs_json` | Exclude symbols registered as exported cross-file public API surfaces |

---

## 3. Tool Specification (MCP JSON-RPC)

### 3.1 Tool Registration
- **Name:** `find_dead_code`
- **Description:** Audits the codebase for unreferenced functions, classes, and variables with zero inbound callers or usages, filtering out public API exports, entry points, and test suites.

### 3.2 Input Parameters (`inputSchema`)

```json
{
  "type": "object",
  "properties": {
    "project": {
      "type": "string",
      "description": "Target project identifier registered in Codebase Memory."
    },
    "file_path": {
      "type": "string",
      "description": "Optional file path or directory prefix to restrict dead code analysis."
    },
    "label": {
      "type": "string",
      "enum": ["ALL", "Function", "Class", "Variable", "Method"],
      "default": "ALL",
      "description": "Symbol type filter: Function, Class, Variable, Method, or ALL."
    },
    "exclude_exported": {
      "type": "boolean",
      "default": true,
      "description": "Exclude symbols exported across module or library boundaries via lsp_surface."
    },
    "min_confidence": {
      "type": "string",
      "enum": ["HIGH", "MEDIUM", "LOW"],
      "default": "HIGH",
      "description": "Minimum confidence threshold: HIGH (private/internal with 0 calls), MEDIUM (module-scoped), LOW (all 0-in-degree symbols)."
    },
    "limit": {
      "type": "integer",
      "default": 30,
      "minimum": 1,
      "maximum": 100,
      "description": "Maximum number of dead symbols to return."
    }
  },
  "required": ["project"]
}
```

### 3.3 Output Response Structure

```json
{
  "project": "codebase-memory-mcp-ui",
  "total_dead_candidates_found": 12,
  "confidence_breakdown": {
    "HIGH": 8,
    "MEDIUM": 4,
    "LOW": 0
  },
  "dead_symbols": [
    {
      "name": "legacy_parse_token_stream",
      "qualified_name": "src.pipeline.legacy_parser.legacy_parse_token_stream",
      "label": "Function",
      "file_path": "src/pipeline/legacy_parser.c",
      "line_range": [210, 245],
      "inbound_callers": 0,
      "inbound_usages": 0,
      "is_exported": false,
      "confidence": "HIGH",
      "rationale": "Static C function with 0 callers and 0 references in the repository.",
      "estimated_lines_saved": 35
    },
    {
      "name": "UNUSED_BUFFER_CAPACITY",
      "qualified_name": "src.foundation.compat.UNUSED_BUFFER_CAPACITY",
      "label": "Variable",
      "file_path": "src/foundation/compat.c",
      "line_range": [34, 34],
      "inbound_callers": 0,
      "inbound_usages": 0,
      "is_exported": false,
      "confidence": "HIGH",
      "rationale": "Module-internal constant with no usage in any active source file.",
      "estimated_lines_saved": 1
    }
  ],
  "summary": {
    "total_lines_recoverable": 284,
    "action_prompt": "Run 'git rm' or delete the verified HIGH-confidence symbols during refactoring."
  }
}
```

---

## 4. Query Architecture & C Implementation

### 4.1 SQL Dead Code Auditor Query (`src/store/store.c`)

```sql
SELECT 
    n.id,
    n.name,
    n.qualified_name,
    n.label,
    n.file_path,
    n.start_line,
    n.end_line,
    (n.end_line - n.start_line + 1) AS line_count,
    CASE 
        WHEN n.file_path LIKE '%.c' AND n.properties LIKE '%"is_static":true%' THEN 'HIGH'
        WHEN n.file_path LIKE '%.ts' AND n.properties NOT LIKE '%"is_exported":true%' THEN 'HIGH'
        ELSE 'MEDIUM'
    END AS confidence
FROM nodes n
WHERE n.project = ?1
  AND (?2 IS NULL OR n.file_path LIKE ?2 || '%')
  AND (?3 = 'ALL' OR n.label = ?3)
  AND n.label IN ('Function', 'Class', 'Variable', 'Method')
  -- Exclude tests
  AND (json_extract(n.properties, '$.is_test') IS NULL OR json_extract(n.properties, '$.is_test') != 1)
  -- Exclude entry points
  AND (json_extract(n.properties, '$.is_entry_point') IS NULL OR json_extract(n.properties, '$.is_entry_point') != 1)
  -- Must have ZERO incoming CALLS, USAGE, HTTP_CALLS
  AND NOT EXISTS (
      SELECT 1 FROM edges e
      WHERE e.project = ?1
        AND e.target_id = n.id
        AND e.type IN ('CALLS', 'USAGE', 'HTTP_CALLS', 'HANDLES')
  )
  -- Exclude public LSP export surface if requested
  AND NOT EXISTS (
      SELECT 1 FROM lsp_surface l
      WHERE l.project = ?1
        AND l.rel_path = n.file_path
        AND instr(l.defs_json, '"' || n.name || '"') > 0
  )
ORDER BY line_count DESC
LIMIT ?4;
```

---

## 5. CLI & UI Integration

### 5.1 CLI Command
```bash
# Find dead functions across the project
cbm cli find_dead_code --project codebase-memory-mcp-ui --label Function --min-confidence HIGH

# Check a specific directory
cbm cli find_dead_code --project codebase-memory-mcp-ui --file-path src/pipeline/
```

### 5.2 UI Integration (`graph-ui`)
- **GraphTab**:
  - Connect the existing "Dead Code" toggle directly to the `find_dead_code` API.
  - Clicking any dead code candidate in the list centers the 3D camera on the orphan node with a glowing amber halo.
- **NodeDetailPanel**:
  - Show a banner: `⚠️ Dead Code Candidate: Zero inbound callers detected across repository.`

---

## 6. Acceptance Criteria & Test Plan

1. **Unit Tests (`tests/test_mcp.c`)**:
   - `test_find_dead_code_orphan`: Verifies an unreferenced internal function is returned with HIGH confidence.
   - `test_find_dead_code_preserves_entrypoints`: Verifies entry points like `main()` or CLI dispatchers are never flagged as dead code.
   - `test_find_dead_code_preserves_routes`: Verifies controller functions linked to `Route` nodes are preserved.
