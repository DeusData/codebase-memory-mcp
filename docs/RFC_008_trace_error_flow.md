# RFC 008: `trace_error_flow` — Exception, Error Type & Panic Propagation Flow Analyzer

- **RFC Number:** 008
- **Title:** `trace_error_flow` — Exception, Error Type, and Panic Propagation Flow Analyzer
- **Status:** Proposed
- **Author:** Codebase Memory Architecture Team
- **Target Subsystem:** `src/mcp/`, `src/store/`, `src/cli/`, `graph-ui/`
- **Target Version:** v0.12.0
- **Created:** 2026-10-04

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Understanding how exceptions, errors, and panics propagate through a call hierarchy is one of the hardest aspects of software comprehension:
1. **Unhandled Crashes:** An API controller calls a service, which calls a repository, which throws `RecordNotFoundException` or `DatabaseTimeoutError`. If neither layer catches it, the request crashes with an unhandled HTTP 500.
2. **Invisible Throw Contracts:** In dynamically typed languages (Python, TypeScript, JavaScript) and languages with unchecked exceptions (Java, C#), functions do not declare what exceptions they or their downstream callees can throw.
3. **Audit Fatigue:** During refactoring or security reviews, verifying that all database, network, and validation errors are safely caught requires laboriously inspecting every nested callee in the call tree.

Currently:
- The parser passes in `codebase-memory-mcp` extract `throw`, `raise`, and panic statements and record **`THROWS` and `RAISES` edges** in SQLite.
- However, **there is no MCP tool to query or trace error propagation**.
- When an AI agent needs to know *"What errors can this endpoint produce?"*, it must either read source code for dozens of files or guess based on variable names.

### 1.2 The Solution
Introduce `trace_error_flow`.
This tool traverses the downstream `CALLS` tree from a starting function or route, collects all `THROWS` and `RAISES` edges, and reports:
- Every error/exception type that can bubble up to the target function.
- The exact originating function, file, and line where each error is instantiated or thrown.
- Whether intermediate callers catch or re-wrap the exception.
- Potential unhandled error risks for public API routes and background worker jobs.

---

## 2. Existing SQLite Database Assets (Zero Reindexing Required)

```
       ┌────────────────────────────────────────────────────────┐
       │ Ingress Route: POST /api/checkout                      │
       └───────────────────────────┬────────────────────────────┘
                                   │ CALLS (depth 1)
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │ Controller: process_payment()                          │
       └───────────────────────────┬────────────────────────────┘
                                   │ CALLS (depth 2)
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │ Service: charge_card()                                 │
       └───────────────────────────┬────────────────────────────┘
                                   │ THROWS / RAISES
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │ Error Node: "CardDeclinedException"                    │
       │ properties: {                                          │
       │   "error_code": 402,                                   │
       │   "is_recoverable": true                               │
       │ }                                                      │
       └────────────────────────────────────────────────────────┘
```

- **`THROWS` & `RAISES` Edges (22 currently in DB)**: Extracted during AST parsing from `throw new Error(...)`, `raise ValueError(...)`, etc.
- **`CALLS` Edges**: Providing the transitive call chain down to error origin sites.

---

## 3. Tool Specification (MCP JSON-RPC)

### 3.1 Tool Registration
- **Name:** `trace_error_flow`
- **Description:** Traces error and exception propagation across a call tree: discovers all exception types that can bubble up to a target function or route from downstream callees.

### 3.2 Input Parameters (`inputSchema`)

```json
{
  "type": "object",
  "properties": {
    "project": {
      "type": "string",
      "description": "Target project identifier registered in Codebase Memory."
    },
    "target": {
      "type": "string",
      "description": "Starting symbol (e.g. 'handle_rpc_post') or API route path (e.g. '/rpc')."
    },
    "max_depth": {
      "type": "integer",
      "default": 4,
      "minimum": 1,
      "maximum": 8,
      "description": "Maximum call depth to traverse looking for downstream errors."
    },
    "unhandled_only": {
      "type": "boolean",
      "default": false,
      "description": "If true, only returns exceptions that lack an enclosing catch/recovery block."
    }
  },
  "required": ["project", "target"]
}
```

### 3.3 Output Response Structure

```json
{
  "project": "codebase-memory-mcp-ui",
  "target": "src.ui.http_server.handle_rpc_post",
  "file_path": "src/ui/http_server.c",
  "line": 420,
  "max_depth_searched": 4,
  "total_exceptions_detected": 3,
  "propagating_errors": [
    {
      "error_type": "JSONRPC_PARSE_ERROR",
      "origin_symbol": "src.mcp.mcp.yy_parse_doc",
      "origin_file": "src/mcp/mcp.c",
      "origin_line": 312,
      "call_distance": 2,
      "call_path": "handle_rpc_post -> dispatch_rpc_call -> yy_parse_doc",
      "status": "HANDLED",
      "handling_block": "src/ui/http_server.c:438 (returns HTTP 400 JSON-RPC error)"
    },
    {
      "error_type": "CBM_STORE_NOT_FOUND",
      "origin_symbol": "src.store.store.cbm_store_open",
      "origin_file": "src/store/store.c",
      "origin_line": 845,
      "call_distance": 3,
      "call_path": "handle_rpc_post -> get_project_store -> cbm_store_open",
      "status": "HANDLED",
      "handling_block": "src/ui/http_server.c:450 (returns error code -32001)"
    },
    {
      "error_type": "SQLITE_CORRUPT",
      "origin_symbol": "src.store.store.exec_sql",
      "origin_file": "src/store/store.c",
      "origin_line": 195,
      "call_distance": 4,
      "call_path": "handle_rpc_post -> get_project_store -> cbm_store_open -> exec_sql",
      "status": "UNHANDLED",
      "handling_block": null,
      "risk": "HIGH",
      "recommendation": "Add recovery block in get_project_store to handle corrupted SQLite files gracefully."
    }
  ]
}
```

---

## 4. Query Architecture & C Implementation

### 4.1 Downstream Recursive Error Collector (`src/store/store.c`)

```sql
WITH RECURSIVE downstream(node_id, depth, path) AS (
    SELECT ?1, 0, CAST(?1 AS TEXT)
    UNION
    SELECT e.target_id, d.depth + 1, d.path || '->' || CAST(e.target_id AS TEXT)
    FROM edges e
    JOIN downstream d ON e.source_id = d.node_id
    WHERE e.project = ?2
      AND e.type = 'CALLS'
      AND d.depth < ?3
      AND instr(d.path, CAST(e.target_id AS TEXT)) = 0 -- Cycle prevention
)
SELECT 
    d.depth,
    d.path,
    origin.qualified_name AS origin_symbol,
    origin.file_path AS origin_file,
    origin.start_line AS origin_line,
    e_err.type AS edge_type,
    err_node.name AS error_type,
    err_node.properties AS error_props
FROM downstream d
JOIN edges e_err ON (e_err.source_id = d.node_id AND e_err.type IN ('THROWS', 'RAISES'))
JOIN nodes err_node ON e_err.target_id = err_node.id
JOIN nodes origin ON d.node_id = origin.id
WHERE e_err.project = ?2
ORDER BY d.depth ASC;
```

---

## 5. CLI & UI Integration

### 5.1 CLI Command
```bash
# Trace what errors can bubble up to handle_rpc_post
cbm cli trace_error_flow --project codebase-memory-mcp-ui --target handle_rpc_post --max-depth 4
```

### 5.2 UI Integration (`graph-ui`)
- **NodeDetailPanel**:
  - Add an **"Exceptions & Error Flow"** tab.
  - Lists all downstream exceptions with red warning badges for unhandled exceptions.
- **DiagramsTab**:
  - Add error flow edges to sequence diagrams rendering error return branches in dotted red lines.

---

## 6. Acceptance Criteria & Test Plan

1. **Unit Tests (`tests/test_mcp.c`)**:
   - `test_trace_error_flow_direct`: Verifies a function directly throwing an error returns depth 0.
   - `test_trace_error_flow_transitive`: Verifies downstream errors 3 hops away are returned with complete call paths.
   - `test_trace_error_flow_cycle`: Verifies recursive call graphs terminate cleanly.
