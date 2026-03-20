# Merged Branch Changes (`token-reduction-and-reference-indexing`)

## Overview

This branch combines both feature branches into a single branch with all capabilities:
- **Token reduction** (from `reduce-token-usage`) -- 8 RTK-inspired strategies reducing output tokens by 72-99%
- **Reference API indexing** (from `reference-api-indexing`) -- dependency source indexing with AI grounding infrastructure

## Branch Lineage

```mermaid
gitGraph
    commit id: "main"
    branch reduce-token-usage
    commit id: "bb23ea4 token reduction"
    commit id: "3518cef summary + pagination"
    commit id: "701d8a7 remove depindex refs"
    commit id: "83b70ed config-backed defaults"
    commit id: "4873697 fix 6 review issues"
    commit id: "e9d92ed remove include_deps schema"
    commit id: "5448324 clarify comments"
    checkout main
    branch reference-api-indexing
    commit id: "3ee66a3 dep tool + grounding"
    checkout main
    branch token-reduction-and-reference-indexing
    merge reduce-token-usage id: "merge token reduction"
    merge reference-api-indexing id: "merge dep indexing"
    commit id: "9619252 restore depindex tests"
    commit id: "7e9774e fix review issues"
    commit id: "7b76742 clarify comments"
```

## Changed Files (vs main)

| File | Insertions | Deletions |
|------|-----------|-----------|
| `src/mcp/mcp.c` | 446 | 54 (net) |
| `tests/test_token_reduction.c` | 826 | 0 (new) |
| `tests/test_depindex.c` | 486 | 0 (new) |
| `tests/test_main.c` | 8 | 0 |
| `Makefile.cbm` | 6 | 1 |
| `src/cypher/cypher.c` | 1 | 1 |
| `src/store/store.c` | 3 | 2 |
| **Total** | **1,725** | **54** |

## Commits (9)

```
7b76742 mcp.c: clarify code comments for token metadata, pagination, head_tail
7e9774e mcp.c: fix 6 issues found in code review
9619252 Makefile.cbm, test_main.c: restore depindex test suite on merged branch
83b70ed mcp: config-backed defaults + magic-number-free tool descriptions
701d8a7 Makefile.cbm, test_main.c: remove depindex refs from token-reduction branch
3518cef mcp: fix summary mode aggregation limit + add pagination hint
a6cfc88 mcp: fix summary mode aggregation limit + add pagination hint
3ee66a3 mcp: add index_dependencies tool + AI grounding infrastructure
bb23ea4 mcp: reduce token consumption via RTK-inspired filtering strategies
```

## Combined Capabilities

### Token Reduction Features

| Feature | Parameter | Default | Savings |
|---------|-----------|---------|---------|
| Default limits | `limit` | 50 | 99.6% |
| Signature mode | `mode="signature"` | -- | 99.4% |
| Head/tail mode | `mode="head_tail"` | -- | 50-70% |
| Summary mode | `mode="summary"` | -- | 99.8% |
| Compact mode | `compact=true` | false | 72.7% |
| Output cap | `max_output_bytes` | 32KB | Caps worst case |
| Token metadata | `_result_bytes`, `_est_tokens` | Always | Awareness |

### Dependency Indexing Features

| Feature | Parameter | Default | Status |
|---------|-----------|---------|--------|
| Index deps | `index_dependencies` tool | -- | Interface only |
| Query deps | `include_dependencies` | false | Ready for deps |
| Source field | `"source":"project/dependency"` | project | Ready |
| QN prefix | `dep.{mgr}.{pkg}.{sym}` | -- | Designed |

## Combined Architecture

```mermaid
graph TB
    subgraph Indexing["Full Indexing (unchanged)"]
        SRC[Source Files] -->|tree-sitter| AST[AST]
        AST -->|multi-pass pipeline| DB[(project.db)]
    end

    subgraph DepIndex["Dependency Indexing (interface ready)"]
        PKG[Package Sources] -->|"subset pipeline (deferred)"| DEPDB[(project_deps.db)]
    end

    subgraph Query["Query with Token Reduction"]
        DB -->|SQL query| RAW[Full Result Set]
        DEPDB -.->|"include_dependencies=true"| RAW
        RAW -->|"1. limit (default 50)"| S1[Bounded Results]
        S1 -->|"2. compact (omit redundant name)"| S2[Deduplicated]
        S2 -->|"3. summary/full mode"| S3[Mode-Filtered]
        S3 -->|"4. max_output_bytes cap"| S4[Size-Capped]
        S4 -->|"5. + _meta tokens"| RESP[MCP Response]
    end

    style Indexing fill:#e8f5e9
    style DepIndex fill:#e3f2fd
    style Query fill:#fff3e0
```

## Snippet Mode Decision Flow

```mermaid
flowchart TD
    A[get_code_snippet called] --> B{mode parameter?}
    B -->|"signature"| C[Return signature only<br/>No file read needed<br/>~99% savings]
    B -->|"head_tail"| D{total_lines > max_lines?}
    B -->|"full" or default| E{total_lines > max_lines?}

    D -->|Yes| F[Read first 60% + last 40%<br/>Insert omission marker<br/>~50-70% savings]
    D -->|No| G[Return all lines<br/>No truncation needed]

    E -->|Yes| H[Truncate at max_lines<br/>Add truncated=true<br/>Variable savings]
    E -->|No| I[Return all lines<br/>No truncation]

    F --> J[Add metadata:<br/>truncated, total_lines, signature]
    H --> J
    C --> K[Response with _result_bytes, _est_tokens]
    G --> K
    I --> K
    J --> K
```

## Token Reduction Pipeline (per query tool)

```mermaid
flowchart LR
    subgraph search_graph
        SG1[SQL Query] --> SG2{mode=summary?}
        SG2 -->|Yes| SG3[Aggregate counts<br/>by_label, by_file_top20]
        SG2 -->|No| SG4[Apply limit<br/>default 50]
        SG4 --> SG5{compact=true?}
        SG5 -->|Yes| SG6[Omit redundant name<br/>when name = QN suffix]
        SG5 -->|No| SG7[Full result objects]
    end

    subgraph trace_call_path
        TR1[BFS Traversal] --> TR2[Dedup by node ID]
        TR2 --> TR3[Cap at max_results<br/>default 25]
        TR3 --> TR4{compact=true?}
        TR4 -->|Yes| TR5[Omit redundant names]
        TR4 -->|No| TR6[Full nodes]
    end

    subgraph query_graph
        QG1[Cypher Execute] --> QG2[Serialize Result]
        QG2 --> QG3{> max_output_bytes?}
        QG3 -->|Yes| QG4[Replace with metadata<br/>truncated=true, total_bytes]
        QG3 -->|No| QG5[Return as-is]
    end
```

## Test Coverage

| Suite | Tests | Lines | Branch |
|-------|-------|-------|--------|
| `suite_token_reduction` | 22 | 826 | reduce-token-usage |
| `suite_depindex` | 12 | 486 | reference-api-indexing |
| **Both** | **34** | **1,312** | merged |

Plus all existing upstream tests (~2,030).

## Merge Conflicts Resolved

- `src/mcp/mcp.c` TOOLS[] array -- both branches added entries; combined in merged branch
- `src/mcp/mcp.c` tool dispatch -- both branches added `strcmp()` entries; combined
- `tests/test_main.c` -- both branches added `extern` + `RUN_SUITE`; combined
- `Makefile.cbm` -- both branches added test source vars; combined

## Known Issues

- `index_dependencies` handler returns `not_yet_implemented` (pipeline deferred)
- `include_dependencies` accepted but no-op until deps are indexed
- Summary mode aggregation capped at 10,000 results
- `limit=0` maps to 500,000 in store.c (upstream behavior)
- CONTRIBUTING.md still references Go build system (upstream responsibility)
