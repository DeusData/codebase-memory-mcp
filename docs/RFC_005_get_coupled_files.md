# RFC 005: `get_coupled_files` — Temporal Commit Co-Change & Git History Companion Finder

- **RFC Number:** 005
- **Title:** `get_coupled_files` — Temporal Commit Co-Change & Git History Companion Finder
- **Status:** Proposed
- **Author:** Codebase Memory Architecture Team
- **Target Subsystem:** `src/mcp/`, `src/store/`, `src/cli/`, `graph-ui/`
- **Target Version:** v0.12.0
- **Created:** 2026-10-04

---

## 1. Executive Summary & Problem Statement

### 1.1 The Problem
Static code analysis (call graphs, imports, AST hierarchies) is blind to **implicit, non-syntactic dependencies**:
1. When modifying an entity model (e.g. `schema.prisma` or `User.java`), an engineer or AI must also update the database migration script (`V4__user.sql`), the frontend TypeScript contract (`user.ts`), and test fixtures (`fixtures/users.json`).
2. These files rarely import or call each other directly, so traditional static analyzers report zero relationship.
3. As a result, AI agents frequently propose changes that fix the primary code file but leave companion files out of sync, causing broken builds, failed CI pipelines, or schema desynchronization.

### 1.2 The Solution
Introduce `get_coupled_files`.
During repository indexing, [`src/pipeline/pass_githistory.c`](file:///c:/AI/Source/codebase-memory-mcp-ui/src/pipeline/pass_githistory.c) parses the repository's git commit log, calculates co-occurrence matrices, and records **`FILE_CHANGES_WITH` edges** between files that historically change together.

This tool exposes those pre-computed temporal coupling edges to:
- Prompt developers and AI agents before commits: *"You modified File A; historically, File B changes alongside File A in 88% of commits."*
- Uncover architectural coupling anomalies (e.g. two seemingly independent modules that are actually tightly coupled in practice).

---

## 2. Existing SQLite Database Assets (Zero Reindexing Required)

```
        ┌────────────────────────────────────────────────────────┐
        │ File Node A: "src/store/store.c"                       │
        └───────────────────────────┬────────────────────────────┘
                                    │
                                    │ Edge: FILE_CHANGES_WITH
                                    │ properties: {
                                    │   "co_commits": 48,
                                    │   "confidence": 0.887,
                                    │   "first_seen": "2024-01-12",
                                    │   "last_seen": "2026-09-28"
                                    │ }
                                    ▼
        ┌────────────────────────────────────────────────────────┐
        │ File Node B: "src/store/store.h"                       │
        └────────────────────────────────────────────────────────┘
```

- **`FILE_CHANGES_WITH` Edges (465 currently in DB)**: Created by `pass_githistory.c`.
- **Edge Properties JSON**:
  - `co_commits`: Total number of distinct git commits modifying both files.
  - `confidence`: Ratio of joint commits to total commits touching the source file ($\frac{|\text{commits}(A \cap B)|}{|\text{commits}(A)|}$).

---

## 3. Tool Specification (MCP JSON-RPC)

### 3.1 Tool Registration
- **Name:** `get_coupled_files`
- **Description:** Returns the "hidden companion files" that historically commit together with a target file based on mined git history, preventing forgotten edits and out-of-sync migrations.

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
      "description": "Relative path of the target file being inspected or edited (e.g. 'src/store/store.c')."
    },
    "min_confidence": {
      "type": "number",
      "default": 0.30,
      "minimum": 0.10,
      "maximum": 1.00,
      "description": "Minimum co-change confidence threshold (0.10 to 1.00)."
    },
    "limit": {
      "type": "integer",
      "default": 10,
      "minimum": 1,
      "maximum": 50,
      "description": "Maximum number of coupled companion files to return."
    }
  },
  "required": ["project", "file_path"]
}
```

### 3.3 Output Response Structure

```json
{
  "project": "codebase-memory-mcp-ui",
  "source_file": "src/store/store.c",
  "total_commits_recorded": 54,
  "companion_files_count": 3,
  "coupled_files": [
    {
      "file_path": "src/store/store.h",
      "co_commit_count": 48,
      "confidence": 0.889,
      "coupling_strength": "VERY HIGH",
      "relationship_type": "header_implementation",
      "recommendation": "Header definition file. Ensure any new store function signatures are declared here."
    },
    {
      "file_path": "tests/test_store.c",
      "co_commit_count": 38,
      "confidence": 0.704,
      "coupling_strength": "HIGH",
      "relationship_type": "unit_test",
      "recommendation": "Test suite file. Ensure unit tests are updated or added for changes in store.c."
    },
    {
      "file_path": "src/pipeline/pipeline_delta.c",
      "co_commit_count": 18,
      "confidence": 0.333,
      "coupling_strength": "MODERATE",
      "relationship_type": "pipeline_consumer",
      "recommendation": "Consumes store schema during incremental indexing."
    }
  ],
  "pre_commit_warning": "Warning: Edits to 'src/store/store.c' usually require corresponding changes in 'src/store/store.h' (89% historical frequency)."
}
```

---

## 4. Query Architecture & C Implementation

### 4.1 SQL Traversal Query (`src/store/store.c`)

```sql
SELECT 
    target_f.file_path AS companion_file,
    coalesce(cast(json_extract(e.properties, '$.co_commits') AS INTEGER), 1) AS co_commits,
    coalesce(cast(json_extract(e.properties, '$.confidence') AS REAL), 0.5) AS confidence,
    json_extract(e.properties, '$.last_seen') AS last_seen
FROM nodes src_f
JOIN edges e ON (e.source_id = src_f.id AND e.type = 'FILE_CHANGES_WITH')
JOIN nodes target_f ON e.target_id = target_f.id
WHERE src_f.project = ?1
  AND src_f.label = 'File'
  AND src_f.file_path = ?2
  AND coalesce(cast(json_extract(e.properties, '$.confidence') AS REAL), 0.5) >= ?3
ORDER BY confidence DESC, co_commits DESC
LIMIT ?4;
```

---

## 5. CLI & UI Integration

### 5.1 CLI Command
```bash
cbm cli get_coupled_files --project codebase-memory-mcp-ui --file-path src/store/store.c
```

### 5.2 UI Integration (`graph-ui`)
- **File Explorer / File Tree**:
  - Hovering over any file shows a tooltip: *"Coupled with: store.h (89%), test_store.c (70%)"*.
- **DiagramsTab**:
  - Add a **Temporal Coupling Matrix** view showing clusters of files that frequently commit together, exposing architectural modularity boundaries.

---

## 6. Acceptance Criteria & Test Plan

1. **Unit Tests (`tests/test_mcp.c`)**:
   - `test_coupled_files_bidirectional`: Verifies coupling relationships are queryable from either file.
   - `test_coupled_files_confidence_filtering`: Verifies `min_confidence` filters low-frequency accidental co-commits.
2. **Integration Verification**:
   - Verify tool returns expected companion files for core modules.
