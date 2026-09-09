/*
 * mcp.c — MCP server: JSON-RPC 2.0 over stdio with focused toolsets.
 *
 * Uses yyjson for fast JSON parsing/building.
 * Single-threaded event loop: read line → parse → dispatch → respond.
 */

// operations

#include "foundation/constants.h"

enum {
    MCP_FIELD_SIZE = 1040,
    MCP_TIMEOUT_MS = 1000,
    MCP_HALF_SEC_US = 500000,
    MCP_MAX_ROWS = 100,
    MCP_MAX_DEPTH = 15,
    MCP_COL_2 = 2,
    MCP_COL_3 = 3,
    MCP_COL_4 = 4,
    MCP_COL_7 = 7,
    MCP_COL_10 = 10,
    MCP_COL_16 = 16,
    MCP_DB_EXT = 3,      /* strlen(".db") */
    MCP_MIN_DB_NAME = 4, /* min length for "x.db" */
    MCP_SEPARATOR = 2,   /* space for separator chars */
    MCP_DEFAULT_DEPTH = 3,
    MCP_DEFAULT_BFS_DEPTH = 2,
    MCP_DEFAULT_LIMIT = 10,
    MCP_BFS_LIMIT = 100,
    MCP_N_DEFAULTS_2 = 2,
    MCP_N_DEFAULTS_4 = 4,
    MCP_URI_PREFIX = 7,      /* strlen("file://") */
    MCP_CONTENT_PREFIX = 15, /* strlen("Content-Length:") */
    MCP_RETURN_2 = 2,
};
#define MCP_MS_TO_US 1000LL
#define MCP_S_TO_US 1000000LL

#define SLEN(s) (sizeof(s) - 1)
#include "mcp/mcp.h"
#include "store/store.h"
#include <sqlite3.h>
#include "cypher/cypher.h"
#include "pipeline/pipeline.h"
#include "pipeline/pass_cross_repo.h"
#include "cli/cli.h"
#include "watcher/watcher.h"
#include "foundation/mem.h"
#include "foundation/diagnostics.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/str_util.h"
#include "foundation/compat_regex.h"
#include "pipeline/artifact.h"

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>
#endif
#include <yyjson/yyjson.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>

/* ── Constants ────────────────────────────────────────────────── */

/* Default snippet fallback line count */
#define SNIPPET_DEFAULT_LINES 50

/* Idle store eviction: close cached project store after this many seconds
 * of inactivity to free SQLite memory during idle periods. */
#define STORE_IDLE_TIMEOUT_S 60

/* Directory permissions: rwxr-xr-x */
#define ADR_DIR_PERMS 0755

/* JSON-RPC 2.0 standard error codes */
#define JSONRPC_PARSE_ERROR (-32700)
#define JSONRPC_METHOD_NOT_FOUND (-32601)

/* ── Helpers ────────────────────────────────────────────────────── */

static char *heap_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *d = malloc(len + SKIP_ONE);
    if (d) {
        memcpy(d, s, len + SKIP_ONE);
    }
    return d;
}

/* Write yyjson_mut_doc to heap-allocated JSON string.
 * ALLOW_INVALID_UNICODE: some database strings may contain non-UTF-8 bytes
 * from older indexing runs — don't fail serialization over it. */
static char *yy_doc_to_str(yyjson_mut_doc *doc) {
    size_t len = 0;
    char *s = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, &len);
    return s;
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC PARSING
 * ══════════════════════════════════════════════════════════════════ */

int cbm_jsonrpc_parse(const char *line, cbm_jsonrpc_request_t *out) {
    memset(out, 0, sizeof(*out));
    out->id = CBM_NOT_FOUND;

    yyjson_doc *doc = yyjson_read(line, strlen(line), 0);
    if (!doc) {
        return CBM_NOT_FOUND;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return CBM_NOT_FOUND;
    }

    yyjson_val *v_jsonrpc = yyjson_obj_get(root, "jsonrpc");
    yyjson_val *v_method = yyjson_obj_get(root, "method");
    yyjson_val *v_id = yyjson_obj_get(root, "id");
    yyjson_val *v_params = yyjson_obj_get(root, "params");

    if (!v_method || !yyjson_is_str(v_method)) {
        yyjson_doc_free(doc);
        return CBM_NOT_FOUND;
    }

    out->jsonrpc =
        heap_strdup(v_jsonrpc && yyjson_is_str(v_jsonrpc) ? yyjson_get_str(v_jsonrpc) : "2.0");
    out->method = heap_strdup(yyjson_get_str(v_method));

    if (v_id) {
        out->has_id = true;
        if (yyjson_is_int(v_id)) {
            out->id = yyjson_get_int(v_id);
        } else if (yyjson_is_str(v_id)) {
            /* JSON-RPC 2.0 §4 permits string ids (Claude Desktop uses them).
             * Preserve verbatim instead of coercing via strtol (issue #253). */
            out->id_str = heap_strdup(yyjson_get_str(v_id));
        }
    }

    if (v_params) {
        out->params_raw = yyjson_val_write(v_params, 0, NULL);
    }

    yyjson_doc_free(doc);
    return 0;
}

void cbm_jsonrpc_request_free(cbm_jsonrpc_request_t *r) {
    if (!r) {
        return;
    }
    safe_str_free(&r->jsonrpc);
    safe_str_free(&r->method);
    safe_str_free(&r->id_str);
    safe_str_free(&r->params_raw);
    memset(r, 0, sizeof(*r));
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC FORMATTING
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_jsonrpc_format_response(const cbm_jsonrpc_response_t *resp) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    if (resp->id_str) {
        yyjson_mut_obj_add_str(doc, root, "id", resp->id_str);
    } else {
        yyjson_mut_obj_add_int(doc, root, "id", resp->id);
    }

    if (resp->error_json) {
        /* Parse the error JSON and embed */
        yyjson_doc *err_doc = yyjson_read(resp->error_json, strlen(resp->error_json), 0);
        if (err_doc) {
            yyjson_mut_val *err_val = yyjson_val_mut_copy(doc, yyjson_doc_get_root(err_doc));
            yyjson_mut_obj_add_val(doc, root, "error", err_val);
            yyjson_doc_free(err_doc);
        }
    } else if (resp->result_json) {
        /* Parse the result JSON and embed */
        yyjson_doc *res_doc = yyjson_read(resp->result_json, strlen(resp->result_json), 0);
        if (res_doc) {
            yyjson_mut_val *res_val = yyjson_val_mut_copy(doc, yyjson_doc_get_root(res_doc));
            yyjson_mut_obj_add_val(doc, root, "result", res_val);
            yyjson_doc_free(res_doc);
        }
    } else {
        /* JSON-RPC 2.0 spec: response MUST contain "result" or "error" */
        yyjson_mut_obj_add_null(doc, root, "result");
    }

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_jsonrpc_format_error(int64_t id, int code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    yyjson_mut_obj_add_int(doc, root, "id", id);

    yyjson_mut_val *err = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, err, "code", code);
    yyjson_mut_obj_add_str(doc, err, "message", message);
    yyjson_mut_obj_add_val(doc, root, "error", err);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP PROTOCOL HELPERS
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_mcp_text_result(const char *text, bool is_error) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    const char *message = text ? text : "";
    char *owned_compat_text = NULL;
    yyjson_doc *structured_doc = NULL;
    yyjson_mut_val *structured = NULL;

    /* Tool outputSchema is an object. Keep already-structured JSON objects
     * byte-for-byte compatible; turn legacy plain text (and JSON scalars or
     * arrays) into a typed object so every tool result is machine-readable. */
    if (text) {
        structured_doc = yyjson_read(text, strlen(text), 0);
    }
    if (structured_doc && yyjson_is_obj(yyjson_doc_get_root(structured_doc))) {
        structured = yyjson_val_mut_copy(doc, yyjson_doc_get_root(structured_doc));
    } else {
        yyjson_mut_val *typed = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, typed, "code", is_error ? "tool_error" : "ok");
        yyjson_mut_obj_add_str(doc, typed, "message", message);
        yyjson_mut_obj_add_bool(doc, typed, "is_error", is_error);
        owned_compat_text = yyjson_mut_val_write(typed, 0, NULL);
        structured = typed;
    }

    yyjson_mut_val *content = yyjson_mut_arr(doc);
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "type", "text");
    yyjson_mut_obj_add_str(doc, item, "text",
                           owned_compat_text ? owned_compat_text : message);
    yyjson_mut_arr_add_val(content, item);
    yyjson_mut_obj_add_val(doc, root, "content", content);

    if (structured) {
        yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
    }

    if (is_error) {
        yyjson_mut_obj_add_bool(doc, root, "isError", true);
    }

    char *out = yy_doc_to_str(doc);
    yyjson_doc_free(structured_doc);
    yyjson_mut_doc_free(doc);
    free(owned_compat_text);
    return out;
}

/* ── Tool definitions ─────────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *description;
    const char *input_schema; /* JSON string */
} tool_def_t;

static const tool_def_t TOOLS[] = {
    {"get_context",
     "Build a deterministic, token-budgeted evidence pack for understanding, debugging, "
     "changing, reviewing, or securing code.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\"},"
     "\"intent\":{\"type\":\"string\",\"enum\":[\"understand\",\"debug\",\"change\","
     "\"review\",\"security\"],\"default\":\"understand\"},"
     "\"project\":{\"type\":\"string\"},\"focus_symbols\":{\"type\":\"array\","
     "\"items\":{\"type\":\"string\"}},\"depth\":{\"type\":\"integer\",\"minimum\":0,"
     "\"maximum\":4,\"default\":2},\"budget_tokens\":{\"type\":\"integer\",\"minimum\":500,"
     "\"maximum\":12000,\"default\":3000},\"include_tests\":{\"type\":\"boolean\","
     "\"default\":false}},\"required\":[\"query\"]}"},

    {"index_repository",
     "Index a repository into the local knowledge graph. structural is the fast default; "
     "enriched additionally computes semantic and similarity evidence.",
     "{\"type\":\"object\",\"properties\":{\"repo_path\":{\"type\":\"string\",\"description\":"
     "\"Path to the repository\"},"
     "\"mode\":{\"type\":\"string\","
     "\"enum\":[\"structural\",\"enriched\",\"fast\",\"moderate\",\"full\"],"
     "\"default\":\"structural\",\"description\":\"structural: Tree-sitter plus static "
     "type-aware resolution. enriched: structural plus semantic/similarity evidence. "
     "fast, moderate, and full are deprecated compatibility aliases.\"},"
     "\"persistence\":{\"type\":\"boolean\",\"default\":false,\"description\":"
     "\"Write compressed artifact to .codebase-memory/graph.db.zst for team sharing. "
     "Teammates can bootstrap from the artifact instead of full re-indexing.\"}"
     "},\"required\":[\"repo_path\"]}"},

    {"search_graph",
     "Search the code knowledge graph for functions, classes, routes, and variables. Use INSTEAD "
     "OF grep/glob when finding code definitions, implementations, or relationships. Three search "
     "modes: (1) query='update settings' for BM25 ranked full-text search with camelCase "
     "splitting and structural label boosting — recommended for natural-language discovery; "
     "(2) name_pattern='.*regex.*' for exact pattern matching; (3) semantic_query=[...] for "
     "vector cosine search that bridges vocabulary (finds 'publish' when you search 'send'). "
     "The three modes are independent and can be combined in a single call. "
     "PAGINATION: results are capped at limit (default 200) — broader queries are silently "
     "truncated. The response always includes 'total' (full match count before limit) and "
     "'has_more' (true when total > offset+returned). Detect truncation with has_more, then "
     "page by re-calling with offset=offset+limit until has_more is false. Narrow first via "
     "label/file_pattern/min_degree before paginating large result sets.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"query\":{\"type\":\"string\",\"description\":\"Natural-language or keyword full-text "
     "search using BM25 ranking. Tokens are split on whitespace; camelCase identifiers are "
     "indexed as individual words (updateCloudClient → update, cloud, client). Results are "
     "ranked with structural boosting: Functions/Methods +10, Routes +8, Classes/Interfaces +5. "
     "Noise labels (File/Folder/Module/Variable) are filtered out. When provided, name_pattern "
     "is ignored.\"},"
     "\"label\":{\"type\":\"string\"},\"name_pattern\":{\"type\":\"string\"},\"qn_pattern\":{"
     "\"type\":\"string\"},\"file_pattern\":{\"type\":\"string\"},"
     "\"relationship\":{\"type\":\"string\"},\"min_degree\":{\"type\":\"integer\"},"
     "\"max_degree\":{\"type\":\"integer\"},\"exclude_entry_points\":{\"type\":\"boolean\"},"
     "\"include_connected\":{\"type\":\"boolean\"},\"semantic_query\":{"
     "\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"MUST be an ARRAY of "
     "keyword strings (e.g. [\\\"send\\\",\\\"pubsub\\\",\\\"publish\\\"]) — NOT a single string. "
     "Each keyword is scored independently via per-keyword min-cosine; results reflect functions "
     "that score well on ALL keywords. Requires moderate/full index mode. Results appear in the "
     "'semantic_results' field (separate from 'results').\"},\"limit\":{\"type\":"
     "\"integer\",\"description\":\"Max results per call. Default 200. Response carries "
     "'total' (full match count) and 'has_more' (true if truncated) so callers can "
     "detect the limit and paginate.\"},\"offset\":{\"type\":\"integer\",\"default\":0,"
     "\"description\":\"Skip the first N matching nodes. Combine with 'limit' to page: "
     "increment offset by limit and re-call while has_more is true.\"}}}"},

    {"query_graph",
     "Execute a Cypher query against the knowledge graph for complex multi-hop patterns, "
     "aggregations, and cross-service analysis. The response includes 'total' (returned "
     "row count). There is a hard 100k row ceiling — for broad queries add LIMIT in the "
     "Cypher itself or use search_graph + offset/limit pagination instead. "
     "COMPLEXITY / BOTTLENECKS: every Function and Method node carries queryable complexity "
     "properties — cyclomatic (complexity), cognitive, loop_count, loop_depth (max nested-loop "
     "depth, a polynomial-degree proxy), plus interprocedural transitive_loop_depth (worst-case "
     "nested-loop degree propagated along CALLS edges) and a recursive flag. Additional "
     "hot-path signals: linear_scan_in_loop (count of find/contains/indexOf-style scans inside a "
     "loop — the hidden O(n^2) that loop_depth misses), alloc_in_loop (allocations/appends inside "
     "a loop), recursion_in_loop (a self-call inside a loop), unguarded_recursion (recursion with "
     "no conditionally-guarded base case), param_count and max_access_depth (structure smells). "
     "Find all hot-path candidates in one query, e.g. MATCH (f:Function) WHERE "
     "f.transitive_loop_depth >= 3 OR f.linear_scan_in_loop >= 1 RETURN f.qualified_name, "
     "f.transitive_loop_depth, f.linear_scan_in_loop ORDER BY f.transitive_loop_depth DESC.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\"},\"max_rows\":{\"type\":\"integer\","
     "\"description\":"
     "\"Optional row limit. Default: unlimited up to a 100k row "
     "ceiling. No offset support — use search_graph for paginated browsing.\"}},"
     "\"required\":[\"query\"]}"},

    {"trace_path",
     "Trace paths through the code graph. Modes: calls (callers/callees), data_flow (value "
     "propagation with args at each hop), cross_service (through HTTP/async Route nodes). "
     "Use INSTEAD OF grep for callers, dependencies, impact analysis, or data flow tracing.",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\"},\"project\":{"
     "\"type\":\"string\"},\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\","
     "\"both\"],\"default\":\"both\"},\"depth\":{\"type\":\"integer\",\"default\":3},\"mode\":{"
     "\"type\":\"string\",\"enum\":[\"calls\",\"data_flow\",\"cross_service\"],\"default\":"
     "\"calls\",\"description\":\"calls: follow CALLS edges. data_flow: follow CALLS+DATA_FLOWS "
     "with arg expressions. cross_service: follow HTTP_CALLS+ASYNC_CALLS+DATA_FLOWS through "
     "Routes.\"},\"parameter_name\":{\"type\":\"string\",\"description\":\"For data_flow mode: "
     "scope trace to a specific parameter name\"},\"edge_types\":{\"type\":\"array\",\"items\":{"
     "\"type\":\"string\"}},\"risk_labels\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Add risk classification (CRITICAL/HIGH/MEDIUM/LOW) based on hop distance"
     "\"},\"include_tests\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Include test files in results. When false (default), test files are "
     "filtered out. When true, test nodes are included with is_test=true marker."
     "\"}},\"required\":[\"function_name\"]}"},

    {"get_code_snippet",
     "Read source code for a function/class/symbol. IMPORTANT: First call search_graph to find the "
     "exact qualified_name, then pass it here. This is a read tool, not a search tool. Accepts "
     "full qualified_name (exact match) or short function name (returns suggestions if ambiguous).",
     "{\"type\":\"object\",\"properties\":{\"qualified_name\":{\"type\":\"string\",\"description\":"
     "\"Full qualified_name from search_graph, or short function name\"},\"project\":{"
     "\"type\":\"string\"},\"include_neighbors\":{"
     "\"type\":\"boolean\",\"default\":false}},\"required\":[\"qualified_name\"]}"},

    {"get_graph_schema", "Get the schema of the knowledge graph (node labels, edge types)",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}}}"},

    {"get_architecture",
     "Get high-level architecture overview — packages, services, dependencies, and project "
     "structure at a glance. Includes 'clusters': Leiden community detection over the call/import "
     "graph, surfacing the de-facto modules (each with a label, member count, cohesion score, "
     "representative top_nodes, and the packages/edge_types that bind it) — use these to grasp "
     "the real architectural seams, which often cut across the folder layout.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"aspects\":{\"type\":"
     "\"array\",\"items\":{\"type\":\"string\"}}}}"},

    {"search_code",
     "Graph-augmented code search. Finds text patterns via grep, then enriches results with "
     "the knowledge graph: deduplicates matches into containing functions, ranks by structural "
     "importance (definitions first, popular functions next, tests last). "
     "Modes: compact (default, signatures only — token efficient), full (with source), "
     "files (just file paths). Use path_filter regex to scope results. "
     "TRUNCATION: enriched results are capped at limit (default 10). Response carries "
     "'total_grep_matches' (raw grep hit count) and 'total_results' (deduplicated function "
     "count) — compare to limit to detect truncation. There is no offset parameter; to see "
     "more, raise limit or narrow the query with file_pattern / path_filter.",
     "{\"type\":\"object\",\"properties\":{\"pattern\":{\"type\":\"string\"},\"project\":{\"type\":"
     "\"string\"},\"file_pattern\":{\"type\":\"string\",\"description\":\"Glob for grep "
     "--include (e.g. *.go)\"},\"path_filter\":{\"type\":\"string\",\"description\":\"Regex "
     "filter on result file paths (e.g. ^src/ or \\\\.(go|ts)$)\"},\"mode\":{\"type\":\"string\","
     "\"enum\":[\"compact\",\"full\",\"files\"],\"default\":\"compact\",\"description\":\"compact: "
     "signatures+metadata (default). full: with source. files: just file list.\"},"
     "\"context\":{\"type\":\"integer\",\"description\":\"Lines of context around each match "
     "(like grep -C). Only used in compact mode.\"},"
     "\"regex\":{\"type\":\"boolean\",\"default\":false},\"limit\":{\"type\":\"integer\","
     "\"description\":\"Max enriched results per call. Default 10. Response includes "
     "'total_grep_matches' and 'total_results' so callers can detect truncation. No "
     "offset parameter — raise limit or narrow with file_pattern / path_filter to see more."
     "\",\"default\":10}},\"required\":[\"pattern\"]}"},

    {"list_projects", "List all indexed projects", "{\"type\":\"object\",\"properties\":{}}"},

    {"delete_project", "Delete a project from the index",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"index_status", "Get the indexing status of a project",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}}}"},

    {"detect_changes", "Detect code changes and their impact",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"scope\":{\"type\":"
     "\"string\"},\"depth\":{\"type\":\"integer\",\"default\":2},\"base_branch\":{\"type\":"
     "\"string\",\"default\":\"main\"},\"since\":{\"type\":\"string\",\"description\":"
     "\"Git ref or date to compare from (e.g. HEAD~5, v0.5.0, 2026-01-01)\"}}}"},

    {"manage_adr", "Create or update Architecture Decision Records",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"mode\":{\"type\":"
     "\"string\",\"enum\":[\"get\",\"update\",\"sections\"]},\"content\":{\"type\":\"string\"},"
     "\"sections\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"project\"]"
     "}"},
};

static const int TOOL_COUNT = sizeof(TOOLS) / sizeof(TOOLS[0]);

static unsigned toolset_for_name(const char *name) {
    if (strcmp(name, "get_context") == 0 || strcmp(name, "index_repository") == 0 ||
        strcmp(name, "search_graph") == 0 || strcmp(name, "trace_path") == 0 ||
        strcmp(name, "get_code_snippet") == 0 || strcmp(name, "get_architecture") == 0 ||
        strcmp(name, "search_code") == 0 || strcmp(name, "index_status") == 0 ||
        strcmp(name, "detect_changes") == 0) {
        return CBM_MCP_TOOLSET_CORE;
    }
    if (strcmp(name, "query_graph") == 0 || strcmp(name, "get_graph_schema") == 0 ||
        strcmp(name, "manage_adr") == 0) {
        return CBM_MCP_TOOLSET_ADVANCED;
    }
    if (strcmp(name, "list_projects") == 0 || strcmp(name, "delete_project") == 0) {
        return CBM_MCP_TOOLSET_ADMIN;
    }
    return 0;
}

static bool tool_is_read_only(const char *name) {
    return strcmp(name, "index_repository") != 0 && strcmp(name, "manage_adr") != 0 &&
           strcmp(name, "delete_project") != 0;
}

static char *toolset_disabled_result(const char *tool_name, unsigned required);

char *cbm_mcp_tools_list_for_toolsets(unsigned toolsets) {
    const unsigned known_toolsets =
        CBM_MCP_TOOLSET_CORE | CBM_MCP_TOOLSET_ADVANCED | CBM_MCP_TOOLSET_ADMIN;
    toolsets &= known_toolsets;
    if (toolsets == 0) {
        toolsets = CBM_MCP_TOOLSET_CORE;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *tools = yyjson_mut_arr(doc);

    for (int i = 0; i < TOOL_COUNT; i++) {
        unsigned toolset = toolset_for_name(TOOLS[i].name);
        if (toolset == 0 || (toolsets & toolset) == 0) {
            continue;
        }

        yyjson_mut_val *tool = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, tool, "name", TOOLS[i].name);
        yyjson_mut_obj_add_str(doc, tool, "description", TOOLS[i].description);

        /* Parse input schema JSON and embed */
        yyjson_doc *schema_doc =
            yyjson_read(TOOLS[i].input_schema, strlen(TOOLS[i].input_schema), 0);
        if (schema_doc) {
            yyjson_mut_val *schema = yyjson_val_mut_copy(doc, yyjson_doc_get_root(schema_doc));
            yyjson_mut_obj_add_val(doc, tool, "inputSchema", schema);
            yyjson_doc_free(schema_doc);
        }

        /* All current tool payloads are JSON objects. More specific schemas
         * can be introduced without breaking clients that accept this base
         * contract. */
        yyjson_mut_val *output_schema = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, output_schema, "type", "object");
        yyjson_mut_obj_add_bool(doc, output_schema, "additionalProperties", true);
        yyjson_mut_obj_add_val(doc, tool, "outputSchema", output_schema);

        bool read_only = tool_is_read_only(TOOLS[i].name);
        yyjson_mut_val *annotations = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_bool(doc, annotations, "readOnlyHint", read_only);
        yyjson_mut_obj_add_bool(doc, annotations, "destructiveHint",
                                strcmp(TOOLS[i].name, "delete_project") == 0);
        yyjson_mut_obj_add_bool(doc, annotations, "idempotentHint", read_only);
        yyjson_mut_obj_add_bool(doc, annotations, "openWorldHint", false);
        yyjson_mut_obj_add_val(doc, tool, "annotations", annotations);

        yyjson_mut_arr_add_val(tools, tool);
    }

    yyjson_mut_obj_add_val(doc, root, "tools", tools);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_mcp_tools_list(void) {
    return cbm_mcp_tools_list_for_toolsets(CBM_MCP_TOOLSET_CORE);
}

/* Supported protocol versions, newest first. The server picks the newest
 * version that it shares with the client (per MCP spec version negotiation). */
static const char *SUPPORTED_PROTOCOL_VERSIONS[] = {
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
};
static const int SUPPORTED_VERSION_COUNT =
    (int)(sizeof(SUPPORTED_PROTOCOL_VERSIONS) / sizeof(SUPPORTED_PROTOCOL_VERSIONS[0]));

char *cbm_mcp_initialize_response(const char *params_json) {
    /* Determine protocol version: if client requests a version we support,
     * echo it back; otherwise respond with our latest. */
    const char *version = SUPPORTED_PROTOCOL_VERSIONS[0]; /* default: latest */
    if (params_json) {
        yyjson_doc *pdoc = yyjson_read(params_json, strlen(params_json), 0);
        if (pdoc) {
            yyjson_val *pv = yyjson_obj_get(yyjson_doc_get_root(pdoc), "protocolVersion");
            if (pv && yyjson_is_str(pv)) {
                const char *requested = yyjson_get_str(pv);
                for (int i = 0; i < SUPPORTED_VERSION_COUNT; i++) {
                    if (strcmp(requested, SUPPORTED_PROTOCOL_VERSIONS[i]) == 0) {
                        version = SUPPORTED_PROTOCOL_VERSIONS[i];
                        break;
                    }
                }
            }
            yyjson_doc_free(pdoc);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "protocolVersion", version);

    yyjson_mut_val *impl = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, impl, "name", "codebase-memory-mcp");
    yyjson_mut_obj_add_str(doc, impl, "version", CBM_VERSION);
    yyjson_mut_obj_add_val(doc, root, "serverInfo", impl);

    yyjson_mut_val *caps = yyjson_mut_obj(doc);
    yyjson_mut_val *tools_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, caps, "tools", tools_cap);
    yyjson_mut_obj_add_val(doc, root, "capabilities", caps);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* ══════════════════════════════════════════════════════════════════
 *  ARGUMENT EXTRACTION
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_mcp_get_tool_name(const char *params_json) {
    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *name = yyjson_obj_get(root, "name");
    char *result = NULL;
    if (name && yyjson_is_str(name)) {
        result = heap_strdup(yyjson_get_str(name));
    }
    yyjson_doc_free(doc);
    return result;
}

char *cbm_mcp_get_arguments(const char *params_json) {
    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *args = yyjson_obj_get(root, "arguments");
    char *result = NULL;
    if (args) {
        result = yyjson_val_write(args, 0, NULL);
    }
    yyjson_doc_free(doc);
    return result ? result : heap_strdup("{}");
}

char *cbm_mcp_get_string_arg(const char *args_json, const char *key) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    char *result = NULL;
    if (val && yyjson_is_str(val)) {
        result = heap_strdup(yyjson_get_str(val));
    }
    yyjson_doc_free(doc);
    return result;
}

int cbm_mcp_get_int_arg(const char *args_json, const char *key, int default_val) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return default_val;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    int result = default_val;
    if (val && yyjson_is_int(val)) {
        result = yyjson_get_int(val);
    }
    yyjson_doc_free(doc);
    return result;
}

bool cbm_mcp_get_bool_arg(const char *args_json, const char *key) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    bool result = false;
    if (val && yyjson_is_bool(val)) {
        result = yyjson_get_bool(val);
    }
    yyjson_doc_free(doc);
    return result;
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP SERVER
 * ══════════════════════════════════════════════════════════════════ */

struct cbm_mcp_server {
    cbm_store_t *store;             /* currently open project store (or NULL) */
    bool owns_store;                /* true if we opened the store */
    char *current_project;          /* which project store is open for (heap) */
    time_t store_last_used;         /* last time resolve_store was called for a named project */
    unsigned toolsets;              /* advertised and callable tool groups */

    /* Session + auto-index state */
    char session_root[CBM_SZ_1K];     /* detected project root path */
    char session_project[CBM_SZ_256]; /* derived project name */
    bool session_detected;            /* true after first detection attempt */
    struct cbm_watcher *watcher;      /* external watcher ref (not owned) */
    struct cbm_config *config;        /* external config ref (not owned) */
    cbm_thread_t autoindex_tid;
    bool autoindex_active; /* true if auto-index thread was started */

    /* Active pipeline tracking for cancellation support */
    cbm_pipeline_t *active_pipeline; /* non-NULL while index_repository runs */
    int64_t active_request_id;       /* JSON-RPC id of the in-progress tool call */
};

static bool env_toolset_contains(const char *value, const char *wanted) {
    if (!value || !wanted) {
        return false;
    }
    size_t wanted_len = strlen(wanted);
    const char *p = value;
    while (*p) {
        while (*p == ',' || *p == ' ' || *p == '\t') {
            p++;
        }
        const char *start = p;
        while (*p && *p != ',' && *p != ' ' && *p != '\t') {
            p++;
        }
        if ((size_t)(p - start) == wanted_len && strncmp(start, wanted, wanted_len) == 0) {
            return true;
        }
    }
    return false;
}

static unsigned toolsets_from_env(void) {
    unsigned toolsets = CBM_MCP_TOOLSET_CORE;
    const char *value = getenv("CBM_MCP_TOOLSETS");
    if (!value) {
        return toolsets;
    }
    if (env_toolset_contains(value, "all") || env_toolset_contains(value, "advanced")) {
        toolsets |= CBM_MCP_TOOLSET_ADVANCED;
    }
    if (env_toolset_contains(value, "all") || env_toolset_contains(value, "admin")) {
        toolsets |= CBM_MCP_TOOLSET_ADMIN;
    }
    return toolsets;
}

cbm_mcp_server_t *cbm_mcp_server_new(const char *store_path) {
    cbm_mcp_server_t *srv = calloc(CBM_ALLOC_ONE, sizeof(*srv));
    if (!srv) {
        return NULL;
    }

    /* If a store_path is given, open that project directly.
     * Otherwise, create an in-memory store for test/embedded use. */
    if (store_path) {
        srv->store = cbm_store_open(store_path);
        srv->current_project = heap_strdup(store_path);
    } else {
        srv->store = cbm_store_open_memory();
    }
    srv->owns_store = true;
    srv->toolsets = toolsets_from_env();

    return srv;
}

cbm_store_t *cbm_mcp_server_store(cbm_mcp_server_t *srv) {
    return srv ? srv->store : NULL;
}

void cbm_mcp_server_set_project(cbm_mcp_server_t *srv, const char *project) {
    if (!srv) {
        return;
    }
    free(srv->current_project);
    srv->current_project = project ? heap_strdup(project) : NULL;
}

void cbm_mcp_server_set_toolsets(cbm_mcp_server_t *srv, unsigned toolsets) {
    if (!srv) {
        return;
    }
    const unsigned known_toolsets =
        CBM_MCP_TOOLSET_CORE | CBM_MCP_TOOLSET_ADVANCED | CBM_MCP_TOOLSET_ADMIN;
    srv->toolsets = toolsets & known_toolsets;
    if (srv->toolsets == 0) {
        srv->toolsets = CBM_MCP_TOOLSET_CORE;
    }
}

unsigned cbm_mcp_server_get_toolsets(const cbm_mcp_server_t *srv) {
    return srv ? srv->toolsets : 0;
}

void cbm_mcp_server_set_watcher(cbm_mcp_server_t *srv, struct cbm_watcher *w) {
    if (srv) {
        srv->watcher = w;
    }
}

void cbm_mcp_server_set_config(cbm_mcp_server_t *srv, struct cbm_config *cfg) {
    if (srv) {
        srv->config = cfg;
    }
}

void cbm_mcp_server_free(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    if (srv->autoindex_active) {
        cbm_thread_join(&srv->autoindex_tid);
    }
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
    }
    free(srv->current_project);
    free(srv);
}

/* ── Idle store eviction ──────────────────────────────────────── */

void cbm_mcp_server_evict_idle(cbm_mcp_server_t *srv, int timeout_s) {
    if (!srv || !srv->store) {
        return;
    }
    /* Protect initial in-memory stores that were never accessed via a named project.
     * store_last_used stays 0 until resolve_store is called with a non-NULL project. */
    if (srv->store_last_used == 0) {
        return;
    }

    time_t now = time(NULL);
    if ((now - srv->store_last_used) < timeout_s) {
        return;
    }

    if (srv->owns_store) {
        cbm_store_close(srv->store);
    }
    srv->store = NULL;
    free(srv->current_project);
    srv->current_project = NULL;
    srv->store_last_used = 0;
}

bool cbm_mcp_server_has_cached_store(cbm_mcp_server_t *srv) {
    return (srv && srv->store != NULL) != 0;
}

cbm_pipeline_t *cbm_mcp_server_active_pipeline(cbm_mcp_server_t *srv) {
    return srv ? srv->active_pipeline : NULL;
}

/* ── Cache dir + project DB path helpers ───────────────────────── */

/* Returns the cache directory. Writes to buf, returns buf for convenience. */
static const char *cache_dir(char *buf, size_t bufsz) {
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) {
        dir = cbm_tmpdir();
    }
    snprintf(buf, bufsz, "%s", dir);
    return buf;
}

/* Returns full .db path for a project: <cache_dir>/<project>.db */
static const char *project_db_path(const char *project, char *buf, size_t bufsz) {
    if (!cbm_validate_project_name(project)) {
        buf[0] = '\0';
        return buf;
    }
    char dir[CBM_SZ_1K];
    cache_dir(dir, sizeof(dir));
    snprintf(buf, bufsz, "%s/%s.db", dir, project);
    return buf;
}

/* ── Store resolution ──────────────────────────────────────────── */

/* Open the right project's .db file for query tools.
 * Caches the connection — reopens only when project changes.
 * Tracks last-access time so the event loop can evict idle stores. */
static cbm_store_t *resolve_store(cbm_mcp_server_t *srv, const char *project) {
    if (!project) {
        return NULL; /* project is required — no implicit fallback */
    }

    srv->store_last_used = time(NULL);

    /* Already open for this project? */
    if (srv->current_project && strcmp(srv->current_project, project) == 0 && srv->store) {
        return srv->store;
    }

    /* Close old store */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }

    /* Open project's .db file — query-only open (no SQLITE_OPEN_CREATE) to
     * prevent ghost .db file creation for unknown/unindexed projects. */
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    srv->store = cbm_store_open_path_query(path);
    if (srv->store) {
        /* Check DB integrity — auto-clean corrupt databases */
        if (!cbm_store_check_integrity(srv->store)) {
            cbm_log_error("store.auto_clean", "project", project, "path", path, "action",
                          "deleting corrupt db — re-index required");
            cbm_store_close(srv->store);
            srv->store = NULL;
            /* Delete the corrupt DB + WAL/SHM files */
            cbm_unlink(path);
            char wal_path[MCP_FIELD_SIZE];
            char shm_path[MCP_FIELD_SIZE];
            snprintf(wal_path, sizeof(wal_path), "%s-wal", path);
            snprintf(shm_path, sizeof(shm_path), "%s-shm", path);
            cbm_unlink(wal_path);
            cbm_unlink(shm_path);
            return NULL;
        }

        /* Verify the project actually exists in this database.
         * A .db file may exist but be empty (e.g., after delete_project on
         * Linux where unlink defers actual removal). Opening an empty/deleted
         * store without closing it leaks the SQLite connection. */
        cbm_project_t proj_verify = {0};
        if (cbm_store_get_project(srv->store, project, &proj_verify) != CBM_STORE_OK) {
            cbm_store_close(srv->store);
            srv->store = NULL;
            return NULL;
        }
        cbm_project_free_fields(&proj_verify);
        srv->owns_store = true;
        free(srv->current_project);
        srv->current_project = heap_strdup(project);
    }

    return srv->store;
}

/* Forward decl — definition lives below alongside list_projects. */
static bool is_project_db_file(const char *name, size_t len);

/* Forward decl — definition lives below in handle_trace_call_path's helpers. */
static void free_node_contents(cbm_node_t *n);

/* Scan cache dir for .db files, writing comma-separated quoted names into out.
 * Returns the number of projects found. */
static int collect_db_project_names(const char *dir_path, char *out, size_t out_sz) {
    int count = 0;
    int offset = 0;
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) {
        return 0;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        if ((size_t)offset >= out_sz)
            break; /* bounds check before write */
        if (count > 0 && offset < (int)out_sz - MCP_SEPARATOR) {
            out[offset++] = ',';
        }
        int wrote = snprintf(out + offset, out_sz - (size_t)offset, "\"%.*s\"", (int)(len - 3), n);
        if (wrote > 0) {
            offset += wrote;
            if ((size_t)offset >= out_sz) {
                offset = (int)out_sz - 1; /* clamp on truncation */
            }
        }
        count++;
    }
    cbm_closedir(d);
    return count;
}

/* Build a helpful error listing available projects. Caller must free() result. */
static char *build_project_list_error(const char *reason) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));

    char projects[CBM_SZ_4K] = "";
    int count = collect_db_project_names(dir_path, projects, sizeof(projects));

    enum { ERR_BUF_SZ = 5120 };
    char buf[ERR_BUF_SZ];
    if (count > 0) {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"Use list_projects to see all indexed projects, "
                 "then pass the project name.\",\"available_projects\":[%s],\"count\":%d}",
                 reason, projects, count);
    } else {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"No projects indexed yet. "
                 "Call index_repository first.\"}",
                 reason);
    }
    return heap_strdup(buf);
}

/* Bail with project list when no store is available. */
#define REQUIRE_STORE(store, project)                                                  \
    do {                                                                               \
        if (!(store)) {                                                                \
            char *_err = build_project_list_error("project not found or not indexed"); \
            char *_res = cbm_mcp_text_result(_err, true);                              \
            free(_err);                                                                \
            free(project);                                                             \
            return _res;                                                               \
        }                                                                              \
    } while (0)

/* ── Tool handler implementations ─────────────────────────────── */

/* Return true if filename is a valid project .db file (not temp/internal).
 *
 * Project names derived from /tmp/... source roots legitimately begin with
 * "tmp-" (cbm_project_name_from_path: "/tmp/bench/..." → "tmp-bench-...";
 * see tests/test_pipeline.c fixtures), so the prefix must NOT be excluded.
 * The "_" prefix is reserved for internal/hidden DBs, and ":memory:" is the
 * SQLite in-memory marker (defensive — never appears as a real file). */
static bool is_project_db_file(const char *name, size_t len) {
    if (len < MCP_MIN_DB_NAME || strcmp(name + len - MCP_DB_EXT, ".db") != 0) {
        return false;
    }
    if (strncmp(name, "_", SLEN("_")) == 0 || strncmp(name, ":memory:", SLEN(":memory:")) == 0) {
        return false;
    }
    return true;
}

/* Open a .db file briefly, collect node/edge counts and root_path,
 * then append a JSON entry to arr. */
static void build_project_json_entry(yyjson_mut_doc *doc, yyjson_mut_val *arr, const char *dir_path,
                                     const char *name, size_t name_len, const struct stat *st) {
    char project_name[CBM_SZ_1K];
    snprintf(project_name, sizeof(project_name), "%.*s", (int)(name_len - 3), name);

    char full_path[CBM_SZ_2K];
    snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);

    cbm_store_t *pstore = cbm_store_open_path(full_path);
    int nodes = 0;
    int edges = 0;
    char root_path_buf[CBM_SZ_1K] = "";
    if (pstore) {
        nodes = cbm_store_count_nodes(pstore, project_name);
        edges = cbm_store_count_edges(pstore, project_name);
        cbm_project_t proj = {0};
        if (cbm_store_get_project(pstore, project_name, &proj) == CBM_STORE_OK) {
            if (proj.root_path) {
                snprintf(root_path_buf, sizeof(root_path_buf), "%s", proj.root_path);
            }
            safe_str_free(&proj.name);
            safe_str_free(&proj.indexed_at);
            safe_str_free(&proj.root_path);
        }
        cbm_store_close(pstore);
    }

    yyjson_mut_val *p = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, p, "name", project_name);
    yyjson_mut_obj_add_strcpy(doc, p, "root_path", root_path_buf);
    yyjson_mut_obj_add_int(doc, p, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, p, "edges", edges);
    yyjson_mut_obj_add_int(doc, p, "size_bytes", (int64_t)st->st_size);
    yyjson_mut_arr_add_val(arr, p);
}

/* list_projects: scan cache directory for .db files.
 * Each project is a single .db file — no central registry needed. */
static char *handle_list_projects(cbm_mcp_server_t *srv, const char *args) {
    (void)srv;
    (void)args;

    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));

    cbm_dir_t *d = cbm_opendir(dir_path);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);

    if (!d) {
        char msg[CBM_SZ_1K];
        snprintf(msg, sizeof(msg),
                 "{\"error\":\"cannot read cache directory: %s\",\"hint\":"
                 "\"Check directory permissions or run index_repository first.\"}",
                 dir_path);
        yyjson_mut_doc_free(doc);
        return cbm_mcp_text_result(msg, true);
    }

    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *name = entry->name;
        size_t len = strlen(name);
        if (!is_project_db_file(name, len)) {
            continue;
        }
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);
        struct stat st;
        if (stat(full_path, &st) != 0) {
            continue;
        }
        build_project_json_entry(doc, arr, dir_path, name, len, &st);
    }
    cbm_closedir(d);

    yyjson_mut_obj_add_val(doc, root, "projects", arr);

    /* Guide user when no projects are indexed */
    if (yyjson_mut_arr_size(arr) == 0) {
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "No projects indexed. Call index_repository(repo_path=...) first.");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* verify_project_indexed — returns a heap-allocated error JSON string when the
 * named project has not been indexed yet, or NULL when the project exists.
 * resolve_store uses cbm_store_open_path_query (no SQLITE_OPEN_CREATE), so
 * store is NULL for missing .db files (REQUIRE_STORE fires first). This
 * function catches the remaining case: a .db file exists but has no indexed
 * nodes (e.g., an empty or half-initialised project).
 * Callers that receive a non-NULL return value must free(project) themselves
 * before returning the error string. */
static char *verify_project_indexed(cbm_store_t *store, const char *project) {
    cbm_project_t proj_check = {0};
    if (cbm_store_get_project(store, project, &proj_check) != CBM_STORE_OK) {
        char *err = build_project_list_error("project not indexed — run index_repository first");
        char *res = cbm_mcp_text_result(err, true);
        free(err);
        return res;
    }
    cbm_project_free_fields(&proj_check);
    return NULL;
}

static char *handle_get_graph_schema(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        return not_indexed;
    }

    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, project, &schema);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *labels = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; i++) {
        yyjson_mut_val *lbl = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, lbl, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, lbl, "count", schema.node_labels[i].count);
        yyjson_mut_val *props = yyjson_mut_arr(doc);
        for (int j = 0; j < schema.node_labels[i].property_count; j++) {
            yyjson_mut_arr_add_str(doc, props, schema.node_labels[i].properties[j]);
        }
        yyjson_mut_obj_add_val(doc, lbl, "properties", props);
        yyjson_mut_arr_add_val(labels, lbl);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", labels);

    yyjson_mut_val *types = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *typ = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, typ, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, typ, "count", schema.edge_types[i].count);
        yyjson_mut_val *eprops = yyjson_mut_arr(doc);
        for (int j = 0; j < schema.edge_types[i].property_count; j++) {
            yyjson_mut_arr_add_str(doc, eprops, schema.edge_types[i].properties[j]);
        }
        yyjson_mut_obj_add_val(doc, typ, "properties", eprops);
        yyjson_mut_arr_add_val(types, typ);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", types);

    /* Check ADR presence */
    cbm_project_t proj_info = {0};
    if (cbm_store_get_project(store, project, &proj_info) == 0 && proj_info.root_path) {
        char adr_path[CBM_SZ_4K];
        snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", proj_info.root_path);
        struct stat adr_st;
        bool adr_exists = (stat(adr_path, &adr_st) == 0);
        yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
        if (!adr_exists) {
            yyjson_mut_obj_add_str(
                doc, root, "adr_hint",
                "No ADR found. Use manage_adr(mode='update') to persist architectural "
                "decisions across sessions. Run get_architecture(aspects=['all']) first.");
        }
        cbm_project_free_fields(&proj_info);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_schema_free(&schema);
    free(project);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Validate edge type: uppercase letters + underscore only, max 64 chars. */
static bool validate_edge_type(const char *s) {
    if (!s || strlen(s) > CBM_SZ_64) {
        return false;
    }
    for (const char *c = s; *c; c++) {
        if (!(*c >= 'A' && *c <= 'Z') && *c != '_') {
            return false;
        }
    }
    return true;
}

/* Enrich search result with 1-hop connected node names. */
/* Add BFS results to a yyjson array (deduped by name). */
static void enrich_add_bfs(yyjson_mut_doc *doc, yyjson_mut_val *arr, cbm_traverse_result_t *tr) {
    for (int j = 0; j < tr->visited_count; j++) {
        if (tr->visited[j].node.name) {
            yyjson_mut_arr_add_strcpy(doc, arr, tr->visited[j].node.name);
        }
    }
}

/* Enrich search result with 1-hop connected node names (inbound + outbound). */
static void enrich_connected(yyjson_mut_doc *doc, yyjson_mut_val *item, cbm_store_t *store,
                             int64_t node_id, const char *relationship) {
    const char *et[] = {relationship ? relationship : "CALLS"};
    yyjson_mut_val *conn = yyjson_mut_arr(doc);

    /* BFS doesn't support "both" — run inbound + outbound separately. */
    cbm_traverse_result_t tr_in = {0};
    cbm_store_bfs(store, node_id, "inbound", et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr_in);
    enrich_add_bfs(doc, conn, &tr_in);
    cbm_store_traverse_free(&tr_in);

    cbm_traverse_result_t tr_out = {0};
    cbm_store_bfs(store, node_id, "outbound", et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr_out);
    enrich_add_bfs(doc, conn, &tr_out);
    cbm_store_traverse_free(&tr_out);

    if (yyjson_mut_arr_size(conn) > 0) {
        yyjson_mut_obj_add_val(doc, item, "connected_names", conn);
    }
}

/* Build an FTS5 MATCH expression from a free-form query string by splitting
 * on whitespace and joining the terms with OR.  Each token is also sanitized:
 * anything that isn't alnum or underscore is dropped, so the caller can't
 * inject FTS5 operators or double-quoted phrases.  Returns the number of
 * tokens emitted (0 if the query contained no usable terms). */
enum {
    BM25_MIN_BUF = 2, /* minimum buffer size: at least NUL + one char */
    BM25_SEP_RESERVE = 1,
    BM25_QUERY_BUF = 1024,
    BM25_DEFAULT_LIMIT = 100,
    BM25_COL_ID = 0,
    BM25_COL_LABEL = 1,
    BM25_COL_NAME = 2,
    BM25_COL_QN = 3,
    BM25_COL_FILE = 4,
    BM25_COL_START = 5,
    BM25_COL_END = 6,
    BM25_COL_RANK = 7,
    BM25_BIND_QUERY = 1,
    BM25_BIND_PROJECT = 2,
    BM25_BIND_LIMIT = 3,
    BM25_BIND_OFFSET = 4,
    BM25_BIND_INNER = 5,
    BM25_SQL_AUTO_LEN = -1,
    /* Inner FTS5 candidate cap.  SQLite can early-terminate a plain FTS5 query
     * (no JOIN/WHERE on outer table) of the form:
     *   SELECT rowid, bm25() FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * By fetching only the top BM25_INNER_LIMIT candidates from the FTS5 index
     * and then joining/filtering/re-ranking those, we bound all work to O(N) where
     * N = BM25_INNER_LIMIT rather than the full match set size. */
    BM25_INNER_LIMIT = 2000,
};

/* Module-local SQLITE_TRANSIENT wrapper to dodge performance-no-int-to-ptr.
 * See the matching helper in src/store/store.c for the same pattern. */
static sqlite3_destructor_type mcp_sqlite_transient(void) {
    static const volatile intptr_t raw = -1;
    sqlite3_destructor_type dtor = NULL;
    memcpy(&dtor, (const void *)&raw, sizeof(dtor));
    return dtor;
}
#define MCP_SQLITE_TRANSIENT (mcp_sqlite_transient())

static int bm25_build_match(const char *query, char *out, size_t out_size) {
    if (!query || !out || out_size < BM25_MIN_BUF) {
        return 0;
    }
    size_t pos = 0;
    int tokens = 0;
    const char *p = query;
    while (*p) {
        while (*p && !((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                       (*p >= '0' && *p <= '9') || *p == '_')) {
            p++;
        }
        if (!*p) {
            break;
        }
        const char *tok_start = p;
        while (*p && ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                      (*p >= '0' && *p <= '9') || *p == '_')) {
            p++;
        }
        size_t tok_len = (size_t)(p - tok_start);
        if (tok_len == 0) {
            continue;
        }
        const char *sep = (tokens > 0) ? " OR " : "";
        size_t sep_len = strlen(sep);
        if (pos + sep_len + tok_len + BM25_SEP_RESERVE >= out_size) {
            break; /* out of room — stop cleanly, keep what we have */
        }
        memcpy(out + pos, sep, sep_len);
        pos += sep_len;
        memcpy(out + pos, tok_start, tok_len);
        pos += tok_len;
        tokens++;
    }
    out[pos] = '\0';
    return tokens;
}

/* Run the BM25 full-text search path and return the JSON result string.
 * Returns NULL if FTS5 is unavailable or the query produced no usable tokens,
 * in which case the caller falls back to the regex-based search path. */
static char *bm25_search(cbm_store_t *store, const char *project, const char *query, int limit,
                         int offset) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return NULL;
    }
    char fts_query[BM25_QUERY_BUF];
    int tok_count = bm25_build_match(query, fts_query, sizeof(fts_query));
    if (tok_count == 0) {
        return NULL;
    }

    /* BM25 ranked query using a two-step approach to enable FTS5 early termination.
     *
     * Flat queries of the form:
     *   SELECT ... FROM nodes_fts JOIN nodes WHERE MATCH ? AND n.project=? ORDER BY rank LIMIT N
     * block FTS5's WAND/MaxScore early-exit because the outer JOIN+WHERE conditions
     * are invisible to the FTS5 planner — it must score every matching document before
     * the project/label filter can discard any of them.  On a large codebase with 100K+
     * matches, this causes multi-minute queries.
     *
     * The fix: let FTS5 drive the inner subquery alone.  SQLite CAN early-terminate
     *   SELECT rowid, bm25(nodes_fts) FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * because no outer predicate blocks it.  We fetch BM25_INNER_LIMIT top candidates
     * from the FTS5 index, then join/filter/boost only those rows.  bm25() returns a
     * NEGATIVE score (lower = more relevant). */
    const char *sql =
        "SELECT n.id, n.label, n.name, n.qualified_name, n.file_path, n.start_line, n.end_line, "
        "       (fts.base_rank "
        "        - CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        "               WHEN n.label = 'Route' THEN 8.0 "
        "               WHEN n.label IN ('Class','Interface','Type','Enum') THEN 5.0 "
        "               ELSE 0.0 END) AS rank "
        "FROM ("
        "    SELECT rowid, bm25(nodes_fts) AS base_rank"
        "    FROM nodes_fts WHERE nodes_fts MATCH ?1"
        "    ORDER BY base_rank, rowid LIMIT ?5"
        ") fts "
        "JOIN nodes n ON n.id = fts.rowid "
        "WHERE n.project = ?2 "
        "  AND n.label NOT IN ('File','Folder','Module','Section','Variable','Project') "
        "ORDER BY rank, n.qualified_name, n.file_path, n.start_line, n.id "
        "LIMIT ?3 OFFSET ?4";

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, BM25_SQL_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        return NULL;
    }
    sqlite3_bind_text(stmt, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, BM25_BIND_LIMIT, limit > 0 ? limit : BM25_DEFAULT_LIMIT);
    sqlite3_bind_int(stmt, BM25_BIND_OFFSET, offset > 0 ? offset : 0);
    sqlite3_bind_int(stmt, BM25_BIND_INNER, BM25_INNER_LIMIT);

    /* Count hits within the same inner-limit window — capped at BM25_INNER_LIMIT.
     * Uses the identical subquery structure so the FTS5 early-exit applies here too. */
    int total = 0;
    {
        const char *count_sql =
            "SELECT COUNT(*) FROM ("
            "    SELECT fts.rowid FROM ("
            "        SELECT rowid FROM nodes_fts WHERE nodes_fts MATCH ?1"
            "        ORDER BY bm25(nodes_fts), rowid LIMIT ?3"
            "    ) fts "
            "    JOIN nodes n ON n.id = fts.rowid "
            "    WHERE n.project = ?2 "
            "      AND n.label NOT IN ('File','Folder','Module','Section','Variable','Project')"
            ")";
        sqlite3_stmt *cs = NULL;
        if (sqlite3_prepare_v2(db, count_sql, BM25_SQL_AUTO_LEN, &cs, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cs, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_text(cs, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_int(cs, BM25_BIND_LIMIT, BM25_INNER_LIMIT);
            if (sqlite3_step(cs) == SQLITE_ROW) {
                total = sqlite3_column_int(cs, 0);
            }
            sqlite3_finalize(cs);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_str(doc, root, "search_mode", "bm25");

    yyjson_mut_val *results = yyjson_mut_arr(doc);
    int emitted = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "name",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_NAME));
        yyjson_mut_obj_add_strcpy(doc, item, "qualified_name",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_QN));
        yyjson_mut_obj_add_strcpy(doc, item, "label",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_LABEL));
        yyjson_mut_obj_add_strcpy(doc, item, "file_path",
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_FILE));
        yyjson_mut_obj_add_int(doc, item, "start_line", sqlite3_column_int(stmt, BM25_COL_START));
        yyjson_mut_obj_add_int(doc, item, "end_line", sqlite3_column_int(stmt, BM25_COL_END));
        yyjson_mut_obj_add_real(doc, item, "rank", sqlite3_column_double(stmt, BM25_COL_RANK));
        yyjson_mut_arr_add_val(results, item);
        emitted++;
    }
    sqlite3_finalize(stmt);

    yyjson_mut_obj_add_val(doc, root, "results", results);
    yyjson_mut_obj_add_bool(doc, root, "has_more", total > offset + emitted);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Forward declaration — defined later. enrich_node_properties parses the
 * node's properties_json and grafts the parsed values onto the result item.
 * It returns the parsed yyjson_doc which must outlive the serialization
 * because yyjson_mut_obj_add_val uses zero-copy strings into that doc. */
static yyjson_doc *enrich_node_properties(yyjson_mut_doc *doc, yyjson_mut_val *obj,
                                          const char *properties_json);

/* Emit the cbm_store_search results as a JSON "results" array on the doc.
 * Property docs created via enrich_node_properties are collected in
 * *out_pdocs (count in *out_pdoc_count) and must be freed by the caller
 * AFTER serializing doc, since yyjson_mut strings are zero-copy pointers
 * into those parsed docs. The caller also frees out_pdocs itself. */
static void emit_search_results(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                const cbm_search_output_t *out, cbm_store_t *store,
                                const char *relationship, bool include_connected, int offset,
                                yyjson_doc ***out_pdocs, int *out_pdoc_count) {
    yyjson_doc **pdocs = out->count > 0 ? malloc((size_t)out->count * sizeof(yyjson_doc *)) : NULL;
    int pdoc_count = 0;
    yyjson_mut_obj_add_int(doc, root, "total", out->total);
    yyjson_mut_val *results = yyjson_mut_arr(doc);
    for (int i = 0; i < out->count; i++) {
        cbm_search_result_t *sr = &out->results[i];
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "name", sr->node.name ? sr->node.name : "");
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               sr->node.qualified_name ? sr->node.qualified_name : "");
        yyjson_mut_obj_add_str(doc, item, "label", sr->node.label ? sr->node.label : "");
        yyjson_mut_obj_add_str(doc, item, "file_path",
                               sr->node.file_path ? sr->node.file_path : "");
        yyjson_mut_obj_add_int(doc, item, "in_degree", sr->in_degree);
        yyjson_mut_obj_add_int(doc, item, "out_degree", sr->out_degree);
        if (include_connected && sr->node.id > 0) {
            enrich_connected(doc, item, store, sr->node.id, relationship);
        }
        yyjson_doc *pdoc = enrich_node_properties(doc, item, sr->node.properties_json);
        if (pdoc && pdocs) {
            pdocs[pdoc_count++] = pdoc;
        }
        yyjson_mut_arr_add_val(results, item);
    }
    yyjson_mut_obj_add_val(doc, root, "results", results);
    yyjson_mut_obj_add_bool(doc, root, "has_more", out->total > offset + out->count);
    *out_pdocs = pdocs;
    *out_pdoc_count = pdoc_count;
}

/* Extract keyword strings from a yyjson array into `keywords`.  Returns the
 * number of strings copied (capped at `max_out`). */
static int extract_semantic_keywords(yyjson_val *sq_val, const char **keywords, int max_out) {
    int kw_count = (int)yyjson_arr_size(sq_val);
    if (kw_count > max_out) {
        kw_count = max_out;
    }
    size_t kw_idx = 0;
    size_t kw_max = 0;
    yyjson_val *kw_val;
    int ki = 0;
    yyjson_arr_foreach(sq_val, kw_idx, kw_max, kw_val) {
        if (ki < kw_count && yyjson_is_str(kw_val)) {
            keywords[ki++] = yyjson_get_str(kw_val);
        }
    }
    return ki;
}

/* Emit cbm_vector_result_t entries as a "semantic_results" array on the doc. */
static void emit_semantic_results(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  cbm_vector_result_t *vresults, int vcount) {
    yyjson_mut_val *sem_results = yyjson_mut_arr(doc);
    for (int v = 0; v < vcount; v++) {
        yyjson_mut_val *vitem = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, vitem, "name", vresults[v].name);
        yyjson_mut_obj_add_strcpy(doc, vitem, "qualified_name", vresults[v].qualified_name);
        yyjson_mut_obj_add_strcpy(doc, vitem, "label", vresults[v].label);
        yyjson_mut_obj_add_strcpy(doc, vitem, "file_path", vresults[v].file_path);
        yyjson_mut_obj_add_real(doc, vitem, "score", vresults[v].score);
        yyjson_mut_arr_add_val(sem_results, vitem);
    }
    yyjson_mut_obj_add_val(doc, root, "semantic_results", sem_results);
}

/* Append the semantic_query vector-search results onto the doc.  Returns
 * true if semantic_query was provided as a non-array (type error — caller
 * should surface to the user). */
static bool run_semantic_query(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *args,
                               cbm_store_t *store, const char *project, int limit) {
    enum { MAX_KW_SEARCH = 32 };
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *sq_val = args_root ? yyjson_obj_get(args_root, "semantic_query") : NULL;
    bool type_error = false;
    if (sq_val && !yyjson_is_arr(sq_val)) {
        type_error = true;
    } else if (sq_val && yyjson_arr_size(sq_val) > 0) {
        const char *keywords[MAX_KW_SEARCH];
        int ki = extract_semantic_keywords(sq_val, keywords, MAX_KW_SEARCH);
        cbm_vector_result_t *vresults = NULL;
        int vcount = 0;
        int sem_limit = limit > 0 ? limit : CBM_SZ_16;
        if (cbm_store_vector_search(store, project, keywords, ki, sem_limit, &vresults, &vcount) ==
                CBM_STORE_OK &&
            vcount > 0) {
            emit_semantic_results(doc, root, vresults, vcount);
            cbm_store_free_vector_results(vresults, vcount);
        }
    }
    if (args_doc) {
        yyjson_doc_free(args_doc);
    }
    return type_error;
}

static char *handle_search_graph(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        return not_indexed;
    }

    /* BM25 path: if `query` is set, run FTS5 full-text search with ranking
     * and return early.  The regex/vector path below is untouched for all
     * other callers.  If FTS5 is unavailable or the query is empty after
     * tokenization, fall through to the regex path. */
    char *query = cbm_mcp_get_string_arg(args, "query");
    if (query && query[0]) {
        int q_limit = cbm_mcp_get_int_arg(args, "limit", BM25_DEFAULT_LIMIT);
        int q_offset = cbm_mcp_get_int_arg(args, "offset", 0);
        char *bm25_json = bm25_search(store, project, query, q_limit, q_offset);
        if (bm25_json) {
            free(query);
            free(project);
            char *result = cbm_mcp_text_result(bm25_json, false);
            free(bm25_json);
            return result;
        }
    }
    free(query);

    char *label = cbm_mcp_get_string_arg(args, "label");
    char *name_pattern = cbm_mcp_get_string_arg(args, "name_pattern");
    char *qn_pattern = cbm_mcp_get_string_arg(args, "qn_pattern");
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *relationship = cbm_mcp_get_string_arg(args, "relationship");
    bool exclude_entry_points = cbm_mcp_get_bool_arg(args, "exclude_entry_points");
    bool include_connected = cbm_mcp_get_bool_arg(args, "include_connected");
    int limit = cbm_mcp_get_int_arg(args, "limit", CBM_DEFAULT_SEARCH_LIMIT);
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    int min_degree = cbm_mcp_get_int_arg(args, "min_degree", CBM_NOT_FOUND);
    int max_degree = cbm_mcp_get_int_arg(args, "max_degree", CBM_NOT_FOUND);

    if (relationship && !validate_edge_type(relationship)) {
        free(project);
        free(label);
        free(name_pattern);
        free(qn_pattern);
        free(file_pattern);
        free(relationship);
        return cbm_mcp_text_result("relationship must be uppercase letters and underscores", true);
    }

    cbm_search_params_t params = {
        .project = project,
        .label = label,
        .name_pattern = name_pattern,
        .qn_pattern = qn_pattern,
        .file_pattern = file_pattern,
        .relationship = relationship,
        .exclude_entry_points = exclude_entry_points,
        .include_connected = include_connected,
        .limit = limit,
        .offset = offset,
        .min_degree = min_degree,
        .max_degree = max_degree,
    };

    cbm_search_output_t out = {0};
    cbm_store_search(store, &params, &out);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_doc **props_docs = NULL;
    int props_doc_count = 0;
    emit_search_results(doc, root, &out, store, relationship, include_connected, offset,
                        &props_docs, &props_doc_count);

    /* Add diagnostic hint when zero results */
    if (out.total == 0) {
        if (name_pattern && label) {
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "No results. Try removing the label filter or broadening the name_pattern regex.");
        } else if (name_pattern) {
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "No nodes match this pattern. Check spelling or try a broader regex.");
        } else if (label) {
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "No nodes with this label. Available labels: Function, Method, Class, "
                "Interface, Route, Variable, Module, Package, File, Folder.");
        }
    }

    bool sq_type_error = run_semantic_query(doc, root, args, store, project, limit);

    if (sq_type_error) {
        for (int pi = 0; pi < props_doc_count; pi++) {
            yyjson_doc_free(props_docs[pi]);
        }
        free(props_docs);
        yyjson_mut_doc_free(doc);
        cbm_store_search_free(&out);
        free(project);
        free(label);
        free(name_pattern);
        free(qn_pattern);
        free(file_pattern);
        free(relationship);
        return cbm_mcp_text_result(
            "semantic_query must be an array of keyword strings, e.g. "
            "[\"send\",\"pubsub\",\"publish\"] — not a single string. Split your query "
            "into individual keywords; each is scored independently via per-keyword "
            "min-cosine.",
            true);
    }

    char *json = yy_doc_to_str(doc);
    /* Property docs are zero-copy referenced by the mut doc — they must
     * outlive yy_doc_to_str. Free them once serialization is complete. */
    for (int pi = 0; pi < props_doc_count; pi++) {
        yyjson_doc_free(props_docs[pi]);
    }
    free(props_docs);
    yyjson_mut_doc_free(doc);
    cbm_store_search_free(&out);

    free(project);
    free(label);
    free(name_pattern);
    free(qn_pattern);
    free(file_pattern);
    free(relationship);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_query_graph(cbm_mcp_server_t *srv, const char *args) {
    char *query = cbm_mcp_get_string_arg(args, "query");
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    int max_rows = cbm_mcp_get_int_arg(args, "max_rows", 0);

    if (!query) {
        free(project);
        return cbm_mcp_text_result("query is required", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(query);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(query);
        return not_indexed;
    }

    cbm_cypher_result_t result = {0};
    int rc = cbm_cypher_execute(store, query, project, max_rows, &result);

    if (rc < 0) {
        char *err_msg = result.error ? result.error : "query execution failed";
        char *resp = cbm_mcp_text_result(err_msg, true);
        cbm_cypher_result_free(&result);
        free(query);
        free(project);
        return resp;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    /* columns */
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    for (int i = 0; i < result.col_count; i++) {
        yyjson_mut_arr_add_str(doc, cols, result.columns[i]);
    }
    yyjson_mut_obj_add_val(doc, root, "columns", cols);

    /* rows */
    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    for (int r = 0; r < result.row_count; r++) {
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        for (int c = 0; c < result.col_count; c++) {
            yyjson_mut_arr_add_str(doc, row, result.rows[r][c]);
        }
        yyjson_mut_arr_add_val(rows, row);
    }
    yyjson_mut_obj_add_val(doc, root, "rows", rows);
    yyjson_mut_obj_add_int(doc, root, "total", result.row_count);

    if (result.row_count == 0) {
        yyjson_mut_obj_add_str(
            doc, root, "hint",
            "Query returned no results. Use get_graph_schema() to see available labels and "
            "edge types.");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_cypher_result_free(&result);
    free(query);
    free(project);

    char *res = cbm_mcp_text_result(json, false);
    free(json);
    return res;
}

static char *handle_index_status(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (project) {
        int nodes = cbm_store_count_nodes(store, project);
        int edges = cbm_store_count_edges(store, project);
        yyjson_mut_obj_add_str(doc, root, "project", project);
        yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
        yyjson_mut_obj_add_int(doc, root, "edges", edges);
        yyjson_mut_obj_add_str(doc, root, "status", nodes > 0 ? "ready" : "empty");
        if (nodes == 0) {
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "Project is empty. Re-run index_repository(repo_path=...) to populate.");
        }
    } else {
        yyjson_mut_obj_add_str(doc, root, "status", "no_project");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(project);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* delete_project: just erase the .db file (and WAL/SHM). */
static char *handle_delete_project(cbm_mcp_server_t *srv, const char *args) {
    char *name = cbm_mcp_get_string_arg(args, "project");
    if (!name) {
        return cbm_mcp_text_result("project is required", true);
    }

    /* Close store if it's the project being deleted */
    if (srv->current_project && strcmp(srv->current_project, name) == 0) {
        if (srv->owns_store && srv->store) {
            cbm_store_close(srv->store);
            srv->store = NULL;
        }
        free(srv->current_project);
        srv->current_project = NULL;
    }

    /* Wait for any in-progress pipeline to finish before deleting */
    cbm_pipeline_lock_project(name);

    /* Delete the .db file + WAL/SHM */
    char path[CBM_SZ_1K];
    project_db_path(name, path, sizeof(path));

    char wal[CBM_SZ_1K];
    char shm[CBM_SZ_1K];
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);

    bool exists = (access(path, F_OK) == 0);
    const char *status = "not_found";
    const char *error_detail = NULL;
    bool is_error = false;

    if (exists) {
        int rc = cbm_unlink(path);
        (void)cbm_unlink(wal);
        (void)cbm_unlink(shm);
        if (rc == 0) {
            status = "deleted";
        } else {
            status = "delete_failed";
            error_detail = strerror(errno);
            is_error = true;
        }
    } else {
        is_error = true;
    }

    cbm_pipeline_unlock_project(name);
    cbm_mem_collect(); /* return freed pages to OS after closing database */

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "project", name);
    yyjson_mut_obj_add_str(doc, root, "status", status);
    if (error_detail) {
        yyjson_mut_obj_add_str(doc, root, "error", error_detail);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(name);

    char *result = cbm_mcp_text_result(json, is_error);
    free(json);
    return result;
}

/* Check if an aspect is requested (NULL aspects = all, or array contains "all" or the name). */
static bool aspect_wanted(yyjson_doc *aspects_doc, yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return true; /* no filter = all */
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (s && (strcmp(s, "all") == 0 || strcmp(s, name) == 0)) {
            return true;
        }
    }
    (void)aspects_doc;
    return false;
}

/* Append cross_repo_links summary to architecture JSON if CROSS_* edges exist. */
static void append_cross_repo_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                      const cbm_schema_info_t *schema) {
    /* Scan edge types for any CROSS_* edges and sum them */
    int cross_total = 0;
    yyjson_mut_val *cr = yyjson_mut_obj(doc);
    static const char *cross_types[] = {"CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS",
                                        "CROSS_CHANNEL",       "CROSS_GRPC_CALLS",
                                        "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
    for (int t = 0; t < (int)(sizeof(cross_types) / sizeof(cross_types[0])); t++) {
        for (int i = 0; i < schema->edge_type_count; i++) {
            if (strcmp(schema->edge_types[i].type, cross_types[t]) == 0) {
                yyjson_mut_obj_add_int(doc, cr, cross_types[t], schema->edge_types[i].count);
                cross_total += schema->edge_types[i].count;
                break;
            }
        }
    }
    if (cross_total > 0) {
        yyjson_mut_obj_add_int(doc, cr, "total", cross_total);
        yyjson_mut_obj_add_val(doc, root, "cross_repo_links", cr);
    }
}

static char *handle_get_architecture(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        return not_indexed;
    }

    /* Parse aspects array from args */
    yyjson_doc *aspects_doc = NULL;
    yyjson_val *aspects_arr = NULL;
    {
        yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
        if (args_doc) {
            yyjson_val *aval = yyjson_obj_get(yyjson_doc_get_root(args_doc), "aspects");
            if (yyjson_is_arr(aval)) {
                aspects_doc = args_doc; /* keep alive */
                aspects_arr = aval;
            } else {
                yyjson_doc_free(args_doc);
            }
        }
    }

    /* Build a C string array from aspects for cbm_store_get_architecture.
     * Strings point into aspects_doc memory so aspects_doc must outlive this array. */
    const char *aspects_strs[MCP_COL_16];
    int aspects_strs_count = 0;
    if (aspects_arr) {
        size_t aspect_idx;
        size_t aspect_max;
        yyjson_val *aspect_val;
        yyjson_arr_foreach(aspects_arr, aspect_idx, aspect_max, aspect_val) {
            const char *s = yyjson_get_str(aspect_val);
            if (s && aspects_strs_count < MCP_COL_16) {
                aspects_strs[aspects_strs_count++] = s;
            }
        }
    }

    cbm_schema_info_t schema = {0};
    /* Counts-only: this handler renders label/type counts but never property
     * keys, and full key discovery json_each-scans every row (seconds-to-
     * minutes on multi-million-node graphs). */
    cbm_store_get_schema_counts(store, project, &schema);

    cbm_architecture_info_t arch = {0};
    cbm_store_get_architecture(store, project, aspects_strs_count > 0 ? aspects_strs : NULL,
                               aspects_strs_count, &arch);

    int node_count = cbm_store_count_nodes(store, project);
    int edge_count = cbm_store_count_edges(store, project);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (project) {
        yyjson_mut_obj_add_str(doc, root, "project", project);
    }
    yyjson_mut_obj_add_int(doc, root, "total_nodes", node_count);
    yyjson_mut_obj_add_int(doc, root, "total_edges", edge_count);

    /* Node label summary */
    if (aspect_wanted(aspects_doc, aspects_arr, "structure")) {
        yyjson_mut_val *labels = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.node_label_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "label", schema.node_labels[i].label);
            yyjson_mut_obj_add_int(doc, item, "count", schema.node_labels[i].count);
            yyjson_mut_arr_add_val(labels, item);
        }
        yyjson_mut_obj_add_val(doc, root, "node_labels", labels);
    }

    /* Edge type summary */
    if (aspect_wanted(aspects_doc, aspects_arr, "dependencies")) {
        yyjson_mut_val *types = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.edge_type_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "type", schema.edge_types[i].type);
            yyjson_mut_obj_add_int(doc, item, "count", schema.edge_types[i].count);
            yyjson_mut_arr_add_val(types, item);
        }
        yyjson_mut_obj_add_val(doc, root, "edge_types", types);
    }

    /* Relationship patterns */
    if (aspect_wanted(aspects_doc, aspects_arr, "routes") && schema.rel_pattern_count > 0) {
        yyjson_mut_val *pats = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            yyjson_mut_arr_add_str(doc, pats, schema.rel_patterns[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "relationship_patterns", pats);
    }

    /* Languages */
    if (arch.language_count > 0) {
        yyjson_mut_val *langs = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.language_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "language",
                                   arch.languages[i].language ? arch.languages[i].language : "");
            yyjson_mut_obj_add_int(doc, item, "file_count", arch.languages[i].file_count);
            yyjson_mut_arr_add_val(langs, item);
        }
        yyjson_mut_obj_add_val(doc, root, "languages", langs);
    }

    /* Packages */
    if (arch.package_count > 0) {
        yyjson_mut_val *pkgs = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.package_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   arch.packages[i].name ? arch.packages[i].name : "");
            yyjson_mut_obj_add_int(doc, item, "node_count", arch.packages[i].node_count);
            yyjson_mut_obj_add_int(doc, item, "fan_in", arch.packages[i].fan_in);
            yyjson_mut_obj_add_int(doc, item, "fan_out", arch.packages[i].fan_out);
            yyjson_mut_arr_add_val(pkgs, item);
        }
        yyjson_mut_obj_add_val(doc, root, "packages", pkgs);
    }

    /* Entry points */
    if (arch.entry_point_count > 0) {
        yyjson_mut_val *eps = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.entry_point_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   arch.entry_points[i].name ? arch.entry_points[i].name : "");
            yyjson_mut_obj_add_str(
                doc, item, "qualified_name",
                arch.entry_points[i].qualified_name ? arch.entry_points[i].qualified_name : "");
            yyjson_mut_obj_add_str(doc, item, "file",
                                   arch.entry_points[i].file ? arch.entry_points[i].file : "");
            yyjson_mut_arr_add_val(eps, item);
        }
        yyjson_mut_obj_add_val(doc, root, "entry_points", eps);
    }

    /* HTTP routes */
    if (arch.route_count > 0) {
        yyjson_mut_val *routes = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.route_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "method",
                                   arch.routes[i].method ? arch.routes[i].method : "");
            yyjson_mut_obj_add_str(doc, item, "path",
                                   arch.routes[i].path ? arch.routes[i].path : "");
            yyjson_mut_obj_add_str(doc, item, "handler",
                                   arch.routes[i].handler ? arch.routes[i].handler : "");
            yyjson_mut_arr_add_val(routes, item);
        }
        yyjson_mut_obj_add_val(doc, root, "routes", routes);
    }

    /* Hotspots */
    if (arch.hotspot_count > 0) {
        yyjson_mut_val *hotspots = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.hotspot_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   arch.hotspots[i].name ? arch.hotspots[i].name : "");
            yyjson_mut_obj_add_str(doc, item, "qualified_name",
                                   arch.hotspots[i].qualified_name ? arch.hotspots[i].qualified_name
                                                                   : "");
            yyjson_mut_obj_add_int(doc, item, "fan_in", arch.hotspots[i].fan_in);
            yyjson_mut_arr_add_val(hotspots, item);
        }
        yyjson_mut_obj_add_val(doc, root, "hotspots", hotspots);
    }

    /* Cross-package boundaries */
    if (arch.boundary_count > 0) {
        yyjson_mut_val *boundaries = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.boundary_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "from",
                                   arch.boundaries[i].from ? arch.boundaries[i].from : "");
            yyjson_mut_obj_add_str(doc, item, "to",
                                   arch.boundaries[i].to ? arch.boundaries[i].to : "");
            yyjson_mut_obj_add_int(doc, item, "call_count", arch.boundaries[i].call_count);
            yyjson_mut_arr_add_val(boundaries, item);
        }
        yyjson_mut_obj_add_val(doc, root, "boundaries", boundaries);
    }

    /* Cross-service links (HTTP/async between services) */
    if (arch.service_count > 0) {
        yyjson_mut_val *services = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.service_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "from",
                                   arch.services[i].from ? arch.services[i].from : "");
            yyjson_mut_obj_add_str(doc, item, "to", arch.services[i].to ? arch.services[i].to : "");
            yyjson_mut_obj_add_str(doc, item, "type",
                                   arch.services[i].type ? arch.services[i].type : "");
            yyjson_mut_obj_add_int(doc, item, "count", arch.services[i].count);
            yyjson_mut_arr_add_val(services, item);
        }
        yyjson_mut_obj_add_val(doc, root, "services", services);
    }

    /* Package layers */
    if (arch.layer_count > 0) {
        yyjson_mut_val *layers = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.layer_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   arch.layers[i].name ? arch.layers[i].name : "");
            yyjson_mut_obj_add_str(doc, item, "layer",
                                   arch.layers[i].layer ? arch.layers[i].layer : "");
            yyjson_mut_obj_add_str(doc, item, "reason",
                                   arch.layers[i].reason ? arch.layers[i].reason : "");
            yyjson_mut_arr_add_val(layers, item);
        }
        yyjson_mut_obj_add_val(doc, root, "layers", layers);
    }

    /* Clusters (community detection) */
    if (arch.cluster_count > 0) {
        yyjson_mut_val *clusters = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.cluster_count; i++) {
            const cbm_cluster_info_t *c = &arch.clusters[i];
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, item, "id", c->id);
            yyjson_mut_obj_add_str(doc, item, "label", c->label ? c->label : "");
            yyjson_mut_obj_add_int(doc, item, "members", c->members);
            yyjson_mut_obj_add_real(doc, item, "cohesion", c->cohesion);
            yyjson_mut_val *top = yyjson_mut_arr(doc);
            for (int j = 0; j < c->top_node_count; j++) {
                yyjson_mut_arr_add_str(doc, top, c->top_nodes[j] ? c->top_nodes[j] : "");
            }
            yyjson_mut_obj_add_val(doc, item, "top_nodes", top);
            yyjson_mut_val *pkgs = yyjson_mut_arr(doc);
            for (int j = 0; j < c->package_count; j++) {
                yyjson_mut_arr_add_str(doc, pkgs, c->packages[j] ? c->packages[j] : "");
            }
            yyjson_mut_obj_add_val(doc, item, "packages", pkgs);
            yyjson_mut_val *etypes = yyjson_mut_arr(doc);
            for (int j = 0; j < c->edge_type_count; j++) {
                yyjson_mut_arr_add_str(doc, etypes, c->edge_types[j] ? c->edge_types[j] : "");
            }
            yyjson_mut_obj_add_val(doc, item, "edge_types", etypes);
            yyjson_mut_arr_add_val(clusters, item);
        }
        yyjson_mut_obj_add_val(doc, root, "clusters", clusters);
    }

    /* File tree */
    if (arch.file_tree_count > 0) {
        yyjson_mut_val *file_tree = yyjson_mut_arr(doc);
        for (int i = 0; i < arch.file_tree_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "path",
                                   arch.file_tree[i].path ? arch.file_tree[i].path : "");
            yyjson_mut_obj_add_str(doc, item, "type",
                                   arch.file_tree[i].type ? arch.file_tree[i].type : "");
            yyjson_mut_obj_add_int(doc, item, "children", arch.file_tree[i].children);
            yyjson_mut_arr_add_val(file_tree, item);
        }
        yyjson_mut_obj_add_val(doc, root, "file_tree", file_tree);
    }

    append_cross_repo_summary(doc, root, &schema);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_architecture_free(&arch);
    cbm_store_schema_free(&schema);
    if (aspects_doc) {
        yyjson_doc_free(aspects_doc);
    }
    free(project);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Resolve edge types from args: explicit array > mode-based > default ("CALLS").
 * Writes types into out_types (max 16). Returns the parsed yyjson_doc if explicit
 * edge_types were found (caller must keep alive until types are consumed), or NULL. */
static yyjson_doc *resolve_trace_edge_types(const char *args, const char *mode,
                                            const char **out_types, int *out_count) {
    static const char *mode_calls[] = {"CALLS"};
    static const char *mode_data_flow[] = {"CALLS", "DATA_FLOWS"};
    static const char *mode_cross_svc[] = {"HTTP_CALLS", "ASYNC_CALLS", "DATA_FLOWS", "CALLS"};

    *out_count = 0;

    yyjson_doc *et_doc = yyjson_read(args, strlen(args), 0);
    if (et_doc) {
        yyjson_val *et_arr = yyjson_obj_get(yyjson_doc_get_root(et_doc), "edge_types");
        if (et_arr && yyjson_is_arr(et_arr)) {
            size_t idx2;
            size_t max2;
            yyjson_val *val2;
            yyjson_arr_foreach(et_arr, idx2, max2, val2) {
                if (yyjson_is_str(val2) && *out_count < MCP_COL_16) {
                    out_types[(*out_count)++] = yyjson_get_str(val2);
                }
            }
        }
    }

    if (*out_count > 0) {
        return et_doc; /* caller must keep alive — pointers reference doc memory */
    }

    yyjson_doc_free(et_doc); /* no explicit types found, free */

    const char **defaults = mode_calls;
    int n_defaults = SKIP_ONE;
    if (mode && strcmp(mode, "data_flow") == 0) {
        defaults = mode_data_flow;
        n_defaults = MCP_N_DEFAULTS_2;
    } else if (mode && strcmp(mode, "cross_service") == 0) {
        defaults = mode_cross_svc;
        n_defaults = MCP_N_DEFAULTS_4;
    }
    for (int i = 0; i < n_defaults; i++) {
        out_types[i] = defaults[i];
    }
    *out_count = n_defaults;
    return NULL;
}

/* Check if a file path looks like a test file. */
static bool is_test_file(const char *path) {
    if (!path) {
        return false;
    }
    return strstr(path, "/test") != NULL || strstr(path, "test_") != NULL ||
           strstr(path, "_test.") != NULL || strstr(path, "/tests/") != NULL ||
           strstr(path, "/spec/") != NULL || strstr(path, ".test.") != NULL;
}

/* Convert BFS traversal results into a yyjson_mut array. */
static yyjson_mut_val *bfs_to_json_array(yyjson_mut_doc *doc, cbm_traverse_result_t *tr,
                                         bool risk_labels, bool include_tests) {
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < tr->visited_count; i++) {
        const char *fp = tr->visited[i].node.file_path;
        bool test = is_test_file(fp);
        if (!include_tests && test) {
            continue;
        }
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "name",
                               tr->visited[i].node.name ? tr->visited[i].node.name : "");
        yyjson_mut_obj_add_str(
            doc, item, "qualified_name",
            tr->visited[i].node.qualified_name ? tr->visited[i].node.qualified_name : "");
        yyjson_mut_obj_add_int(doc, item, "hop", tr->visited[i].hop);
        if (risk_labels) {
            yyjson_mut_obj_add_str(doc, item, "risk",
                                   cbm_risk_label(cbm_hop_to_risk(tr->visited[i].hop)));
        }
        if (test) {
            yyjson_mut_obj_add_bool(doc, item, "is_test", true);
        }
        yyjson_mut_arr_add_val(arr, item);
    }
    return arr;
}

static char *handle_trace_call_path(cbm_mcp_server_t *srv, const char *args) {
    char *func_name = cbm_mcp_get_string_arg(args, "function_name");
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    char *mode = cbm_mcp_get_string_arg(args, "mode");
    char *param_name = cbm_mcp_get_string_arg(args, "parameter_name");
    int depth = cbm_mcp_get_int_arg(args, "depth", MCP_DEFAULT_DEPTH);
    bool risk_labels = cbm_mcp_get_bool_arg(args, "risk_labels");
    bool include_tests = cbm_mcp_get_bool_arg(args, "include_tests");

    if (!func_name) {
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        return cbm_mcp_text_result("function_name is required", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        return not_indexed;
    }

    if (!direction) {
        direction = heap_strdup("both");
    }

    /* Find the node by name. If the bare-name lookup misses, fall back to
     * qualified_name so callers passing a fully-qualified identifier (which
     * the not-found hint actually recommends) hit the same path. The QN
     * lookup uses the same scan_node helper as the bare lookup, so the
     * shallow struct copy below transfers ownership of the strdup'd string
     * fields cleanly and cbm_store_free_nodes will free them. */
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    cbm_store_find_nodes_by_name(store, project, func_name, &nodes, &node_count);

    if (node_count == 0) {
        cbm_node_t qn_node = {0};
        if (cbm_store_find_node_by_qn(store, project, func_name, &qn_node) == CBM_STORE_OK) {
            nodes = malloc(sizeof(cbm_node_t));
            if (nodes) {
                nodes[0] = qn_node;
                node_count = 1;
            } else {
                free_node_contents(&qn_node);
            }
        }
    }

    if (node_count == 0) {
        enum { HINT_BUF_SZ = 512 };
        char hint[HINT_BUF_SZ];
        snprintf(hint, sizeof(hint),
                 "{\"error\":\"function not found\",\"function_name\":\"%s\","
                 "\"hint\":\"Use search_graph(name_pattern=\\\".*%s.*\\\") to find the exact "
                 "name, then pass it to trace_path.\"}",
                 func_name, func_name);
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        cbm_store_free_nodes(nodes, 0);
        return cbm_mcp_text_result(hint, true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "function", func_name);
    yyjson_mut_obj_add_str(doc, root, "direction", direction);
    if (mode) {
        yyjson_mut_obj_add_str(doc, root, "mode", mode);
    }

    /* Edge types: explicit > mode-based > default */
    const char *edge_types[MCP_COL_16];
    int edge_type_count = 0;
    yyjson_doc *et_doc_keep = resolve_trace_edge_types(args, mode, edge_types, &edge_type_count);

    /* Run BFS for each requested direction.
     * IMPORTANT: yyjson_mut_obj_add_str borrows pointers — we must keep
     * traversal results alive until after yy_doc_to_str serialization. */
    bool do_outbound = strcmp(direction, "outbound") == 0 || strcmp(direction, "both") == 0;
    bool do_inbound = strcmp(direction, "inbound") == 0 || strcmp(direction, "both") == 0;

    cbm_traverse_result_t tr_out = {0};
    cbm_traverse_result_t tr_in = {0};

    if (do_outbound) {
        cbm_store_bfs(store, nodes[0].id, "outbound", edge_types, edge_type_count, depth,
                      MCP_BFS_LIMIT, &tr_out);
        yyjson_mut_obj_add_val(doc, root, "callees",
                               bfs_to_json_array(doc, &tr_out, risk_labels, include_tests));
    }

    if (do_inbound) {
        cbm_store_bfs(store, nodes[0].id, "inbound", edge_types, edge_type_count, depth,
                      MCP_BFS_LIMIT, &tr_in);
        yyjson_mut_obj_add_val(doc, root, "callers",
                               bfs_to_json_array(doc, &tr_in, risk_labels, include_tests));
    }

    /* Serialize BEFORE freeing traversal results (yyjson borrows strings) */
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    /* Now safe to free traversal data */
    if (do_outbound) {
        cbm_store_traverse_free(&tr_out);
    }
    if (do_inbound) {
        cbm_store_traverse_free(&tr_in);
    }

    cbm_store_free_nodes(nodes, node_count);
    free(func_name);
    free(project);
    free(direction);
    free(mode);
    free(param_name);
    if (et_doc_keep) {
        yyjson_doc_free(et_doc_keep);
    }

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── Helper: free heap fields of a stack-allocated node ────────── */

static void free_node_contents(cbm_node_t *n) {
    safe_str_free(&n->project);
    safe_str_free(&n->label);
    safe_str_free(&n->name);
    safe_str_free(&n->qualified_name);
    safe_str_free(&n->file_path);
    safe_str_free(&n->properties_json);
    memset(n, 0, sizeof(*n));
}

/* ── Helper: read lines [start, end] from a file ─────────────── */

static char *read_file_lines(const char *path, int start, int end) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        return NULL;
    }

    size_t cap = CBM_SZ_4K;
    char *buf = malloc(cap);
    size_t len = 0;
    buf[0] = '\0';

    char line[CBM_SZ_2K];
    int lineno = 0;
    while (fgets(line, sizeof(line), fp)) {
        lineno++;
        if (lineno < start) {
            continue;
        }
        if (lineno > end) {
            break;
        }
        size_t ll = strlen(line);
        while (len + ll + SKIP_ONE > cap) {
            cap *= PAIR_LEN;
            buf = safe_realloc(buf, cap);
        }
        memcpy(buf + len, line, ll);
        len += ll;
        buf[len] = '\0';
    }

    (void)fclose(fp);
    if (len == 0) {
        free(buf);
        return NULL;
    }
    return buf;
}

/* ── Helper: get project root_path from store ─────────────────── */

static char *get_project_root(cbm_mcp_server_t *srv, const char *project) {
    if (!project) {
        return NULL;
    }
    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        return NULL;
    }
    cbm_project_t proj = {0};
    if (cbm_store_get_project(store, project, &proj) != CBM_STORE_OK) {
        return NULL;
    }
    char *root = heap_strdup(proj.root_path);
    safe_str_free(&proj.name);
    safe_str_free(&proj.indexed_at);
    safe_str_free(&proj.root_path);
    return root;
}

/* ── index_repository ─────────────────────────────────────────── */

/* Handle mode="cross-repo-intelligence" — extract to reduce complexity. */
static char *handle_cross_repo_mode(const char *repo_path, const char *args) {
    char *project = heap_strdup(cbm_project_name_from_path(repo_path));
    if (!project) {
        return cbm_mcp_text_result("cannot derive project name", true);
    }

    yyjson_doc *jdoc = yyjson_read(args, strlen(args), 0);
    yyjson_val *jroot = jdoc ? yyjson_doc_get_root(jdoc) : NULL;
    yyjson_val *tp_arr = jroot ? yyjson_obj_get(jroot, "target_projects") : NULL;

    if (!tp_arr || !yyjson_is_arr(tp_arr) || yyjson_arr_size(tp_arr) == 0) {
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result(
            "{\"error\":\"target_projects is required for cross-repo-intelligence mode. "
            "Use [\\\"*\\\"] for all projects. Run list_projects to see available.\"}",
            true);
    }

    int tp_count = (int)yyjson_arr_size(tp_arr);
    const char **targets = malloc((size_t)tp_count * sizeof(char *));
    size_t idx;
    size_t max;
    yyjson_val *val;
    int ti = 0;
    yyjson_arr_foreach(tp_arr, idx, max, val) {
        targets[ti++] = yyjson_get_str(val);
    }

    cbm_cross_repo_result_t result = cbm_cross_repo_match(project, targets, tp_count);
    free(targets);
    yyjson_doc_free(jdoc);

    int total = result.http_edges + result.async_edges + result.channel_edges + result.grpc_edges +
                result.graphql_edges + result.trpc_edges;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "success");
    yyjson_mut_obj_add_str(doc, root, "mode", "cross-repo-intelligence");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_int(doc, root, "projects_scanned", result.projects_scanned);
    yyjson_mut_obj_add_int(doc, root, "cross_http_calls", result.http_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_async_calls", result.async_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_channel", result.channel_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_grpc_calls", result.grpc_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_graphql_calls", result.graphql_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_trpc_calls", result.trpc_edges);
    yyjson_mut_obj_add_int(doc, root, "total_cross_edges", total);
    yyjson_mut_obj_add_real(doc, root, "elapsed_ms", result.elapsed_ms);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(project);
    char *out = cbm_mcp_text_result(json, false);
    free(json);
    return out;
}

/* Bootstrap from artifact if no local DB exists for this project. */
static void try_artifact_bootstrap(const char *project_name, const char *repo_path) {
    char db_buf[CBM_SZ_1K];
    project_db_path(project_name, db_buf, sizeof(db_buf));
    struct stat db_st;
    if (stat(db_buf, &db_st) != 0 && cbm_artifact_exists(repo_path)) {
        cbm_log_info("index.artifact_bootstrap", "project", project_name);
        cbm_artifact_import(repo_path, db_buf);
    }
}

/* Cap on excluded dir paths listed in the response — keep it compact on large
 * repos (node_modules / vendor / etc. can produce many skip points). The full
 * count is still reported via "count" + "truncated". */
enum { INDEX_EXCLUDED_DIR_CAP = 25 };

/* Attach a compact summary of directory subtrees skipped during discovery (#411).
 * Shape: "excluded": {"dirs": [up to 25 rel-paths], "count": <total>, "truncated": <bool>}.
 * No-op when nothing was excluded. excluded_dirs[] is borrowed (copied into doc). */
static void add_excluded_summary(yyjson_mut_doc *doc, yyjson_mut_val *root, char **excluded_dirs,
                                 int excluded_count) {
    if (!excluded_dirs || excluded_count <= 0) {
        return;
    }
    yyjson_mut_val *excluded = yyjson_mut_obj(doc);
    yyjson_mut_val *dirs = yyjson_mut_arr(doc);
    int shown = excluded_count < INDEX_EXCLUDED_DIR_CAP ? excluded_count : INDEX_EXCLUDED_DIR_CAP;
    for (int i = 0; i < shown; i++) {
        if (excluded_dirs[i]) {
            yyjson_mut_arr_add_strcpy(doc, dirs, excluded_dirs[i]);
        }
    }
    yyjson_mut_obj_add_val(doc, excluded, "dirs", dirs);
    yyjson_mut_obj_add_int(doc, excluded, "count", excluded_count);
    yyjson_mut_obj_add_bool(doc, excluded, "truncated", excluded_count > INDEX_EXCLUDED_DIR_CAP);
    yyjson_mut_obj_add_val(doc, root, "excluded", excluded);
}

/* Build the success portion of the index_repository response. */
static void build_index_success_response(cbm_mcp_server_t *srv, yyjson_mut_doc *doc,
                                         yyjson_mut_val *root, const char *project_name,
                                         const char *repo_path, bool persistence,
                                         char **excluded_dirs, int excluded_count) {
    add_excluded_summary(doc, root, excluded_dirs, excluded_count);

    cbm_store_t *store = resolve_store(srv, project_name);
    if (!store) {
        return;
    }
    int nodes = cbm_store_count_nodes(store, project_name);
    int edges = cbm_store_count_edges(store, project_name);
    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);

    char adr_path[CBM_SZ_4K];
    snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", repo_path);
    struct stat adr_st;
    bool adr_exists = (stat(adr_path, &adr_st) == 0);
    yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
    if (!adr_exists) {
        yyjson_mut_obj_add_str(
            doc, root, "adr_hint",
            "Project indexed. Consider creating an Architecture Decision Record: "
            "explore the codebase with get_architecture(aspects=['all']), then use "
            "manage_adr(mode='store') to persist architectural insights across sessions.");
    }

    bool has_artifact = cbm_artifact_exists(repo_path);
    yyjson_mut_obj_add_bool(doc, root, "artifact_present", has_artifact);
    if (persistence && has_artifact) {
        yyjson_mut_obj_add_str(doc, root, "artifact_hint",
                               "Persistent artifact written to .codebase-memory/graph.db.zst. "
                               "Commit this file to share the index with teammates.");
    }
}

static char *handle_index_repository(cbm_mcp_server_t *srv, const char *args) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    cbm_normalize_path_sep(repo_path);

    if (!repo_path) {
        free(mode_str);
        return cbm_mcp_text_result("repo_path is required", true);
    }

    if (mode_str && strcmp(mode_str, "cross-repo-intelligence") == 0) {
        if ((srv->toolsets & CBM_MCP_TOOLSET_ADVANCED) == 0) {
            free(mode_str);
            free(repo_path);
            return toolset_disabled_result("cross-repo-intelligence",
                                           CBM_MCP_TOOLSET_ADVANCED);
        }
        free(mode_str);
        char *result = handle_cross_repo_mode(repo_path, args);
        free(repo_path);
        return result;
    }

    cbm_index_mode_t mode = CBM_MODE_FAST;
    if (mode_str &&
        (strcmp(mode_str, "structural") == 0 || strcmp(mode_str, "fast") == 0)) {
        mode = CBM_MODE_FAST;
    } else if (mode_str && strcmp(mode_str, "moderate") == 0) {
        mode = CBM_MODE_MODERATE;
    } else if (mode_str &&
               (strcmp(mode_str, "enriched") == 0 || strcmp(mode_str, "full") == 0)) {
        mode = CBM_MODE_FULL;
    } else if (mode_str) {
        free(mode_str);
        free(repo_path);
        return cbm_mcp_text_result(
            "{\"code\":\"invalid_arguments\","
            "\"message\":\"mode must be structural or enriched\"}",
            true);
    }
    free(mode_str);

    bool persistence = cbm_mcp_get_bool_arg(args, "persistence");

    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, NULL, mode);
    if (!p) {
        free(repo_path);
        return cbm_mcp_text_result("failed to create pipeline", true);
    }
    cbm_pipeline_set_persistence(p, persistence);

    char *project_name = heap_strdup(cbm_pipeline_project_name(p));

    /* Bootstrap from artifact if no local DB exists */
    try_artifact_bootstrap(project_name, repo_path);

    /* Close cached store — pipeline will delete + recreate the .db file */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;

    /* Serialize pipeline runs to prevent concurrent writes.
     * Track active pipeline so signal handler and notifications/cancelled
     * can cancel it mid-run. */
    cbm_pipeline_lock_project(project_name);
    srv->active_pipeline = p;
    int rc = cbm_pipeline_run(p);
    srv->active_pipeline = NULL;
    cbm_pipeline_unlock_project(project_name);

    /* Capture the excluded-subtree list (#411) while the pipeline (which owns
     * the strings) is still alive — the response builder copies them into the
     * JSON doc, so they need only outlive that call, not cbm_pipeline_free. */
    char **excluded_dirs = NULL;
    int excluded_count = 0;
    cbm_pipeline_get_excluded(p, &excluded_dirs, &excluded_count);

    cbm_mem_collect(); /* return mimalloc pages to OS after large indexing */

    /* Invalidate cached store so next query reopens the fresh database */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "project", project_name);
    yyjson_mut_obj_add_str(doc, root, "status", rc == 0 ? "indexed" : "error");

    if (rc != 0) {
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "Pipeline failed. Check repo_path exists and contains source files. "
                               "Try mode='fast' for a quicker diagnostic run.");
    }

    if (rc == 0) {
        build_index_success_response(srv, doc, root, project_name, repo_path, persistence,
                                     excluded_dirs, excluded_count);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    /* Free the pipeline only after the response doc copied the excluded list. */
    cbm_pipeline_free(p);
    free(project_name);
    free(repo_path);

    char *result = cbm_mcp_text_result(json, rc != 0);
    free(json);
    return result;
}

/* ── get_code_snippet ─────────────────────────────────────────── */

/* Copy a node from an array into a heap-allocated standalone node. */
static void copy_node(const cbm_node_t *src, cbm_node_t *dst) {
    dst->id = src->id;
    dst->project = heap_strdup(src->project);
    dst->label = heap_strdup(src->label);
    dst->name = heap_strdup(src->name);
    dst->qualified_name = heap_strdup(src->qualified_name);
    dst->file_path = heap_strdup(src->file_path);
    dst->start_line = src->start_line;
    dst->end_line = src->end_line;
    dst->properties_json = src->properties_json ? heap_strdup(src->properties_json) : NULL;
}

/* Build a JSON suggestions response for ambiguous or fuzzy results. */
static char *snippet_suggestions(const char *input, cbm_node_t *nodes, int count) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "status", "ambiguous");

    char msg[CBM_SZ_512];
    snprintf(msg, sizeof(msg),
             "%d matches for \"%s\". Pick a qualified_name from suggestions below, "
             "or use search_graph(name_pattern=\"...\") to narrow results.",
             count, input);
    yyjson_mut_obj_add_str(doc, root, "message", msg);

    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_val *s = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, s, "qualified_name",
                               nodes[i].qualified_name ? nodes[i].qualified_name : "");
        yyjson_mut_obj_add_str(doc, s, "name", nodes[i].name ? nodes[i].name : "");
        yyjson_mut_obj_add_str(doc, s, "label", nodes[i].label ? nodes[i].label : "");
        yyjson_mut_obj_add_str(doc, s, "file_path", nodes[i].file_path ? nodes[i].file_path : "");
        yyjson_mut_arr_append(arr, s);
    }
    yyjson_mut_obj_add_val(doc, root, "suggestions", arr);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Enrich a mutable JSON object with key-value pairs from a node's properties_json.
 * Returns the parsed yyjson_doc (caller frees AFTER serialization — zero-copy). */
static yyjson_doc *enrich_node_properties(yyjson_mut_doc *doc, yyjson_mut_val *obj,
                                          const char *properties_json) {
    if (!properties_json || properties_json[0] == '\0') {
        return NULL;
    }
    yyjson_doc *props_doc = yyjson_read(properties_json, strlen(properties_json), 0);
    if (!props_doc) {
        return NULL;
    }
    yyjson_val *props_root = yyjson_doc_get_root(props_doc);
    if (!props_root || !yyjson_is_obj(props_root)) {
        yyjson_doc_free(props_doc);
        return NULL;
    }
    yyjson_obj_iter iter;
    yyjson_obj_iter_init(props_root, &iter);
    yyjson_val *key;
    while ((key = yyjson_obj_iter_next(&iter))) {
        yyjson_val *val = yyjson_obj_iter_get_val(key);
        const char *k = yyjson_get_str(key);
        if (!k) {
            continue;
        }
        if (yyjson_is_str(val)) {
            yyjson_mut_obj_add_str(doc, obj, k, yyjson_get_str(val));
        } else if (yyjson_is_bool(val)) {
            yyjson_mut_obj_add_bool(doc, obj, k, yyjson_get_bool(val));
        } else if (yyjson_is_int(val)) {
            yyjson_mut_obj_add_int(doc, obj, k, yyjson_get_int(val));
        } else if (yyjson_is_real(val)) {
            yyjson_mut_obj_add_real(doc, obj, k, yyjson_get_real(val));
        }
    }
    return props_doc; /* caller frees after serialization */
}

/* Resolve an absolute path from root_path + file_path, verify containment,
 * and read source lines. Sets *out_abs_path (caller frees). Returns source
 * string (caller frees) or NULL if path is invalid/unreadable. */
static char *resolve_snippet_source(const char *root_path, const char *file_path, int start,
                                    int end, char **out_abs_path) {
    *out_abs_path = NULL;
    if (!root_path || !file_path) {
        return NULL;
    }
    size_t apsz = strlen(root_path) + strlen(file_path) + MCP_SEPARATOR;
    char *abs_path = malloc(apsz);
    snprintf(abs_path, apsz, "%s/%s", root_path, file_path);

    char real_root[CBM_SZ_4K];
    char real_file[CBM_SZ_4K];
    bool path_ok = false;
#ifdef _WIN32
    if (_fullpath(real_root, root_path, sizeof(real_root)) &&
        _fullpath(real_file, abs_path, sizeof(real_file))) {
        cbm_normalize_path_sep(real_root);
        cbm_normalize_path_sep(real_file);
#else
    if (realpath(root_path, real_root) && realpath(abs_path, real_file)) {
#endif
        size_t root_len = strlen(real_root);
        if (strncmp(real_file, real_root, root_len) == 0 &&
            (real_file[root_len] == '/' || real_file[root_len] == '\0')) {
            path_ok = true;
        }
    }
    *out_abs_path = abs_path;
    if (path_ok) {
        return read_file_lines(abs_path, start, end);
    }
    return NULL;
}

/* Build an enriched snippet response for a resolved node. */
/* Add a string array to a JSON object (no-op if count == 0). */
static void add_string_array(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                             char **strings, int count) {
    if (count <= 0) {
        return;
    }
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_arr_add_str(doc, arr, strings[i]);
    }
    yyjson_mut_obj_add_val(doc, obj, key, arr);
}

static char *build_snippet_response(cbm_mcp_server_t *srv, cbm_node_t *node,
                                    const char *match_method, bool include_neighbors,
                                    cbm_node_t *alternatives, int alt_count) {
    char *root_path = get_project_root(srv, node->project);

    int start = node->start_line > 0 ? node->start_line : SKIP_ONE;
    int end = node->end_line > start ? node->end_line : start + SNIPPET_DEFAULT_LINES;
    char *abs_path = NULL;
    char *source = resolve_snippet_source(root_path, node->file_path, start, end, &abs_path);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    yyjson_mut_obj_add_str(doc, root_obj, "name", node->name ? node->name : "");
    yyjson_mut_obj_add_str(doc, root_obj, "qualified_name",
                           node->qualified_name ? node->qualified_name : "");
    yyjson_mut_obj_add_str(doc, root_obj, "label", node->label ? node->label : "");

    const char *display_path = "";
    if (abs_path) {
        display_path = abs_path;
    } else if (node->file_path) {
        display_path = node->file_path;
    }
    yyjson_mut_obj_add_str(doc, root_obj, "file_path", display_path);
    yyjson_mut_obj_add_int(doc, root_obj, "start_line", start);
    yyjson_mut_obj_add_int(doc, root_obj, "end_line", end);

    if (source) {
        yyjson_mut_obj_add_str(doc, root_obj, "source", source);
    } else {
        yyjson_mut_obj_add_str(doc, root_obj, "source", "(source not available)");
    }

    /* match_method — omitted for exact matches */
    if (match_method) {
        yyjson_mut_obj_add_str(doc, root_obj, "match_method", match_method);
    }

    /* Enrich with node properties (freed AFTER serialization — zero-copy). */
    yyjson_doc *props_doc = enrich_node_properties(doc, root_obj, node->properties_json);

    /* Caller/callee counts — store already resolved by calling handler */
    cbm_store_t *store = srv->store;
    int in_deg = 0;
    int out_deg = 0;
    cbm_store_node_degree(store, node->id, &in_deg, &out_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callers", in_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callees", out_deg);

    char **nb_callers = NULL;
    int nb_caller_count = 0;
    char **nb_callees = NULL;
    int nb_callee_count = 0;
    if (include_neighbors) {
        cbm_store_node_neighbor_names(store, node->id, MCP_DEFAULT_LIMIT, &nb_callers,
                                      &nb_caller_count, &nb_callees, &nb_callee_count);
        add_string_array(doc, root_obj, "caller_names", nb_callers, nb_caller_count);
        add_string_array(doc, root_obj, "callee_names", nb_callees, nb_callee_count);
    }

    /* Alternatives (when auto-resolved from ambiguous) */
    if (alternatives && alt_count > 0) {
        yyjson_mut_val *arr = yyjson_mut_arr(doc);
        for (int i = 0; i < alt_count; i++) {
            yyjson_mut_val *a = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, a, "qualified_name",
                                   alternatives[i].qualified_name ? alternatives[i].qualified_name
                                                                  : "");
            yyjson_mut_obj_add_str(doc, a, "file_path",
                                   alternatives[i].file_path ? alternatives[i].file_path : "");
            yyjson_mut_arr_append(arr, a);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "alternatives", arr);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(props_doc); /* safe if NULL */
    for (int i = 0; i < nb_caller_count; i++) {
        free(nb_callers[i]);
    }
    for (int i = 0; i < nb_callee_count; i++) {
        free(nb_callees[i]);
    }
    free(nb_callers);
    free(nb_callees);
    free(root_path);
    free(abs_path);
    free(source);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_get_code_snippet(cbm_mcp_server_t *srv, const char *args) {
    char *qn = cbm_mcp_get_string_arg(args, "qualified_name");
    char *project = cbm_mcp_get_string_arg(args, "project");
    bool include_neighbors = cbm_mcp_get_bool_arg(args, "include_neighbors");

    if (!qn) {
        free(project);
        return cbm_mcp_text_result("qualified_name is required", true);
    }

    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(qn);
        free(project);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(qn);
        free(project);
        return not_indexed;
    }

    /* Default to current project (same as all other tools) */
    const char *effective_project = project ? project : srv->current_project;

    /* Tier 1: Exact QN match */
    cbm_node_t node = {0};
    int rc = cbm_store_find_node_by_qn(store, effective_project, qn, &node);
    if (rc == CBM_STORE_OK) {
        char *result = build_snippet_response(srv, &node, NULL, include_neighbors, NULL, 0);
        free_node_contents(&node);
        free(qn);
        free(project);
        return result;
    }

    /* Tier 2: Suffix match — handles partial QNs ("main.HandleRequest")
     * and short names ("ProcessOrder") via LIKE '%.X'. */
    cbm_node_t *suffix_nodes = NULL;
    int suffix_count = 0;
    cbm_store_find_nodes_by_qn_suffix(store, effective_project, qn, &suffix_nodes, &suffix_count);

    if (suffix_count == SKIP_ONE) {
        copy_node(&suffix_nodes[0], &node);
        cbm_store_free_nodes(suffix_nodes, suffix_count);
        char *result = build_snippet_response(srv, &node, "suffix", include_neighbors, NULL, 0);
        free_node_contents(&node);
        free(qn);
        free(project);
        return result;
    }

    if (suffix_count > SKIP_ONE) {
        char *result = snippet_suggestions(qn, suffix_nodes, suffix_count);
        cbm_store_free_nodes(suffix_nodes, suffix_count);
        free(qn);
        free(project);
        return result;
    }

    cbm_store_free_nodes(suffix_nodes, suffix_count);
    free(qn);
    free(project);

    /* Nothing found — guide the caller toward search_graph */
    return cbm_mcp_text_result(
        "symbol not found. Use search_graph(name_pattern=\"...\") first to discover "
        "the exact qualified_name, then pass it to get_code_snippet.",
        true);
}

/* ── search_code v2: graph-augmented code search ─────────────── */

/* Preserve valid UTF-8 and replace only malformed bytes. Replacing every
 * non-ASCII byte corrupted legitimate source code and search results. */
static void sanitize_utf8(char *s) {
    unsigned char *p = (unsigned char *)s;
    while (*p) {
        if (*p <= 0x7F) {
            p++;
            continue;
        }

        int continuation_count = 0;
        uint32_t codepoint = 0;
        uint32_t min_codepoint = 0;
        if ((*p & 0xE0) == 0xC0) {
            continuation_count = 1;
            codepoint = *p & 0x1F;
            min_codepoint = 0x80;
        } else if ((*p & 0xF0) == 0xE0) {
            continuation_count = 2;
            codepoint = *p & 0x0F;
            min_codepoint = 0x800;
        } else if ((*p & 0xF8) == 0xF0) {
            continuation_count = 3;
            codepoint = *p & 0x07;
            min_codepoint = 0x10000;
        } else {
            *p++ = '?';
            continue;
        }

        bool valid = true;
        for (int i = 1; i <= continuation_count; i++) {
            if (p[i] == '\0' || (p[i] & 0xC0) != 0x80) {
                valid = false;
                break;
            }
            codepoint = (codepoint << 6) | (p[i] & 0x3F);
        }
        if (!valid || codepoint < min_codepoint || codepoint > 0x10FFFF ||
            (codepoint >= 0xD800 && codepoint <= 0xDFFF)) {
            *p++ = '?';
            continue;
        }
        p += continuation_count + 1;
    }
}

/* Intermediate grep match */
typedef struct {
    char file[CBM_SZ_512];
    int line;
    char content[CBM_SZ_1K];
} grep_match_t;

/* Deduped result: one per containing graph node */
typedef struct {
    int64_t node_id; /* 0 = raw match (no containing node) */
    char node_name[CBM_SZ_256];
    char qualified_name[CBM_SZ_512];
    char label[CBM_SZ_64];
    char file[CBM_SZ_512];
    int start_line;
    int end_line;
    int in_degree;
    int out_degree;
    int score;
    int match_lines[CBM_SZ_64];
    int match_count;
} search_result_t;

/* Score a result for ranking: project source first, vendored last, tests lowest */
enum { SCORE_FUNC = 10, SCORE_ROUTE = 15, SCORE_VENDORED = -50, SCORE_TEST = -5 };
enum { MAX_LINE_SPAN = 999999 };

static int compute_search_score(const search_result_t *r) {
    int score = r->in_degree;
    if (strcmp(r->label, "Function") == 0 || strcmp(r->label, "Method") == 0) {
        score += SCORE_FUNC;
    }
    if (strcmp(r->label, "Route") == 0) {
        score += SCORE_ROUTE;
    }
    if (strstr(r->file, "vendored/") || strstr(r->file, "vendor/") ||
        strstr(r->file, "node_modules/")) {
        score += SCORE_VENDORED;
    }
    /* Penalize test files */
    if (strstr(r->file, "test") || strstr(r->file, "spec") || strstr(r->file, "_test.")) {
        score += SCORE_TEST;
    }
    return score;
}

static int search_result_cmp(const void *a, const void *b) {
    const search_result_t *ra = (const search_result_t *)a;
    const search_result_t *rb = (const search_result_t *)b;
    return rb->score - ra->score; /* descending */
}

static int grep_match_cmp(const void *a, const void *b) {
    const grep_match_t *ma = (const grep_match_t *)a;
    const grep_match_t *mb = (const grep_match_t *)b;
    int by_file = strcmp(ma->file, mb->file);
    if (by_file != 0) {
        return by_file;
    }
    return (ma->line > mb->line) - (ma->line < mb->line);
}

/* Build the grep/search command string based on scoped vs recursive mode.
 * On Windows, uses PowerShell Select-String with tab-delimited output.
 * On POSIX, uses grep with colon-delimited output. */
static void build_grep_cmd(char *cmd, size_t cmd_sz, bool use_regex, bool scoped,
                           const char *file_pattern, const char *tmpfile, const char *filelist,
                           const char *root_path) {
#ifdef _WIN32
    const char *sm = use_regex ? "" : " -SimpleMatch";
    if (scoped) {
        if (file_pattern) {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"$pat = Get-Content '%s'; "
                "Get-Content '%s' | ForEach-Object { Select-String -LiteralPath $_ -Pattern $pat%s "
                "-ErrorAction SilentlyContinue }"
                " | Where-Object { $_.Path -like '*%s' }"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                tmpfile, filelist, sm, file_pattern);
        } else {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"$pat = Get-Content '%s'; "
                "Get-Content '%s' | ForEach-Object { Select-String -LiteralPath $_ -Pattern $pat%s "
                "-ErrorAction SilentlyContinue }"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                tmpfile, filelist, sm);
        }
    } else {
        if (file_pattern) {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"Get-ChildItem -Recurse -Path '%s\\*' -Include '%s' -File "
                "-ErrorAction SilentlyContinue"
                " | Select-String -Pattern (Get-Content '%s')%s -ErrorAction SilentlyContinue"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                root_path, file_pattern, tmpfile, sm);
        } else {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"Get-ChildItem -Recurse -Path '%s\\*' -File -ErrorAction "
                "SilentlyContinue"
                " | Select-String -Pattern (Get-Content '%s')%s -ErrorAction SilentlyContinue"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                root_path, tmpfile, sm);
        }
    }
#else
    const char *flag = use_regex ? "-E" : "-F";
    if (scoped) {
        if (file_pattern) {
            snprintf(cmd, cmd_sz,
                     "xargs -0 grep -Hn %s --include='%s' -f '%s' < '%s' 2>/dev/null", flag,
                     file_pattern, tmpfile, filelist);
        } else {
            snprintf(cmd, cmd_sz, "xargs -0 grep -Hn %s -f '%s' < '%s' 2>/dev/null", flag,
                     tmpfile, filelist);
        }
    } else {
        if (file_pattern) {
            snprintf(cmd, cmd_sz, "grep -rn %s --include='%s' -f '%s' '%s' 2>/dev/null", flag,
                     file_pattern, tmpfile, root_path);
        } else {
            snprintf(cmd, cmd_sz, "grep -rn %s -f '%s' '%s' 2>/dev/null", flag, tmpfile, root_path);
        }
    }
#endif
}

/* Build deduplicated file list from search results + raw matches. */
static yyjson_mut_val *build_dedup_files_array(yyjson_mut_doc *doc, search_result_t *sr,
                                               int output_count, grep_match_t *raw, int raw_count) {
    yyjson_mut_val *files_arr = yyjson_mut_arr(doc);
    char *seen_files[CBM_SZ_512];
    int seen_count = 0;
    for (int fi = 0; fi < output_count; fi++) {
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], sr[fi].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup && seen_count < CBM_SZ_512) {
            seen_files[seen_count++] = sr[fi].file;
            yyjson_mut_arr_add_str(doc, files_arr, sr[fi].file);
        }
    }
    for (int fi = 0; fi < raw_count && seen_count < CBM_SZ_512; fi++) {
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], raw[fi].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            seen_files[seen_count++] = raw[fi].file;
            yyjson_mut_arr_add_str(doc, files_arr, raw[fi].file);
        }
    }
    return files_arr;
}

/* Attach source or context lines to a search result JSON item. */
static void attach_result_source(yyjson_mut_doc *doc, yyjson_mut_val *item, search_result_t *r,
                                 int mode, int context_lines, const char *root_path) {
    enum { MODE_FULL = 1 };
    if (r->start_line <= 0 || r->end_line <= 0) {
        return;
    }
    char abs_path[CBM_SZ_1K];
    snprintf(abs_path, sizeof(abs_path), "%s/%s", root_path, r->file);

    if (mode == MODE_FULL) {
        char *source = read_file_lines(abs_path, r->start_line, r->end_line);
        if (source) {
            sanitize_utf8(source);
            yyjson_mut_obj_add_strcpy(doc, item, "source", source);
            free(source);
        }
    } else if (context_lines > 0 && r->match_count > 0) {
        int ctx_start = r->match_lines[0] - context_lines;
        int ctx_end = r->match_lines[r->match_count - SKIP_ONE] + context_lines;
        if (ctx_start < SKIP_ONE) {
            ctx_start = SKIP_ONE;
        }
        char *ctx = read_file_lines(abs_path, ctx_start, ctx_end);
        if (ctx) {
            sanitize_utf8(ctx);
            yyjson_mut_obj_add_strcpy(doc, item, "context", ctx);
            yyjson_mut_obj_add_int(doc, item, "context_start", ctx_start);
            free(ctx);
        }
    }
}

/* Build directory distribution object from search results (top-level dir → count). */
static yyjson_mut_val *build_dir_distribution(yyjson_mut_doc *doc, search_result_t *sr,
                                              int sr_count) {
    yyjson_mut_val *dirs = yyjson_mut_obj(doc);
    char dir_names[CBM_SZ_64][CBM_SZ_128];
    int dir_counts[CBM_SZ_64];
    int dir_n = 0;
    for (int di = 0; di < sr_count; di++) {
        char top[CBM_SZ_128] = "";
        const char *slash = strchr(sr[di].file, '/');
        if (slash) {
            size_t dlen = (size_t)(slash - sr[di].file + SKIP_ONE);
            if (dlen >= sizeof(top)) {
                dlen = sizeof(top) - SKIP_ONE;
            }
            memcpy(top, sr[di].file, dlen);
            top[dlen] = '\0';
        } else {
            snprintf(top, sizeof(top), "%s", sr[di].file);
        }
        int found = CBM_NOT_FOUND;
        for (int d = 0; d < dir_n; d++) {
            if (strcmp(dir_names[d], top) == 0) {
                found = d;
                break;
            }
        }
        if (found >= 0) {
            dir_counts[found]++;
        } else if (dir_n < CBM_SZ_64) {
            snprintf(dir_names[dir_n], sizeof(dir_names[0]), "%s", top);
            dir_counts[dir_n] = SKIP_ONE;
            dir_n++;
        }
    }
    for (int d = 0; d < dir_n; d++) {
        yyjson_mut_val *key = yyjson_mut_strcpy(doc, dir_names[d]);
        yyjson_mut_val *val = yyjson_mut_int(doc, dir_counts[d]);
        yyjson_mut_obj_add(dirs, key, val);
    }
    return dirs;
}

/* Phase 4: assemble JSON output from search results */
static char *assemble_search_output(search_result_t *sr, int sr_count, grep_match_t *raw,
                                    int raw_count, int gm_count, int limit, int mode,
                                    int context_lines, const char *root_path,
                                    bool warn_literal_pipe, uint64_t elapsed_ms) {
    enum { MODE_COMPACT = 0, MODE_FULL = 1, MODE_FILES = 2, SEARCH_SLOW_MS = 5000 };

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    int output_count = sr_count < limit ? sr_count : limit;

    if (mode == MODE_FILES) {
        yyjson_mut_obj_add_val(doc, root_obj, "files",
                               build_dedup_files_array(doc, sr, output_count, raw, raw_count));
    } else {
        yyjson_mut_val *results_arr = yyjson_mut_arr(doc);
        for (int ri = 0; ri < output_count; ri++) {
            search_result_t *r = &sr[ri];
            yyjson_mut_val *item = yyjson_mut_obj(doc);

            yyjson_mut_obj_add_str(doc, item, "node", r->node_name);
            yyjson_mut_obj_add_str(doc, item, "qualified_name", r->qualified_name);
            yyjson_mut_obj_add_str(doc, item, "label", r->label);
            yyjson_mut_obj_add_str(doc, item, "file", r->file);
            yyjson_mut_obj_add_int(doc, item, "start_line", r->start_line);
            yyjson_mut_obj_add_int(doc, item, "end_line", r->end_line);
            yyjson_mut_obj_add_int(doc, item, "in_degree", r->in_degree);
            yyjson_mut_obj_add_int(doc, item, "out_degree", r->out_degree);

            yyjson_mut_val *ml = yyjson_mut_arr(doc);
            for (int j = 0; j < r->match_count; j++) {
                yyjson_mut_arr_add_int(doc, ml, r->match_lines[j]);
            }
            yyjson_mut_obj_add_val(doc, item, "match_lines", ml);
            attach_result_source(doc, item, r, mode, context_lines, root_path);
            yyjson_mut_arr_add_val(results_arr, item);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "results", results_arr);

        enum { MAX_RAW = 20 };
        yyjson_mut_val *raw_arr = yyjson_mut_arr(doc);
        int raw_output = raw_count < MAX_RAW ? raw_count : MAX_RAW;
        for (int ri = 0; ri < raw_output; ri++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "file", raw[ri].file);
            yyjson_mut_obj_add_int(doc, item, "line", raw[ri].line);
            yyjson_mut_obj_add_str(doc, item, "content", raw[ri].content);
            yyjson_mut_arr_add_val(raw_arr, item);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "raw_matches", raw_arr);
    }

    yyjson_mut_obj_add_val(doc, root_obj, "directories", build_dir_distribution(doc, sr, sr_count));

    /* Summary stats */
    yyjson_mut_obj_add_int(doc, root_obj, "total_grep_matches", gm_count);
    yyjson_mut_obj_add_int(doc, root_obj, "total_results", sr_count);
    yyjson_mut_obj_add_int(doc, root_obj, "raw_match_count", raw_count);
    yyjson_mut_obj_add_int(doc, root_obj, "elapsed_ms", (int)elapsed_ms);
    if (sr_count > 0 && gm_count > 0) {
        char ratio[CBM_SZ_32];
        snprintf(ratio, sizeof(ratio), "%.1fx", (double)gm_count / (double)(sr_count + raw_count));
        yyjson_mut_obj_add_strcpy(doc, root_obj, "dedup_ratio", ratio);
    }

    /* Warnings: surface common foot-guns instead of leaving them silent. */
    yyjson_mut_val *warnings = yyjson_mut_arr(doc);
    if (warn_literal_pipe) {
        yyjson_mut_arr_add_strcpy(
            doc, warnings,
            "pattern contains '|' but regex=false, so it is matched literally (not as "
            "alternation). Pass regex=true for 'foo|bar' to mean 'foo OR bar'.");
    }
    if (elapsed_ms >= SEARCH_SLOW_MS) {
        char slow[CBM_SZ_128];
        snprintf(slow, sizeof(slow),
                 "search took %dms (>%ds); narrow file_pattern/path_filter or use a more "
                 "specific pattern",
                 (int)elapsed_ms, SEARCH_SLOW_MS / 1000);
        yyjson_mut_arr_add_strcpy(doc, warnings, slow);
        char ems[CBM_SZ_32];
        snprintf(ems, sizeof(ems), "%d", (int)elapsed_ms);
        cbm_log_warn("search.slow", "elapsed_ms", ems); /* visibility in logs */
    }
    if (yyjson_mut_arr_size(warnings) > 0) {
        yyjson_mut_obj_add_val(doc, root_obj, "warnings", warnings);
    }

    char *json = yy_doc_to_str(doc);
    if (json) {
        sanitize_utf8(json);
    }
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Read grep output from fp, parse file:line:content format, apply path filter,
 * and return a dynamically-allocated grep_match_t array. */
/* Strip root path prefix from a file path. */
static const char *strip_root_prefix(const char *path, const char *root, size_t root_len) {
    if (strncmp(path, root, root_len) != 0) {
        return path;
    }
    const char *p = path + root_len;
    if (*p == '/') {
        p++;
    }
    return p;
}

static grep_match_t *collect_grep_matches(FILE *fp, const char *root_path, size_t root_len,
                                          bool has_path_filter, cbm_regex_t *path_regex,
                                          int grep_limit, int *out_count) {
    int gm_cap = CBM_SZ_64;
    int gm_count = 0;
    grep_match_t *gm = malloc(gm_cap * sizeof(grep_match_t));
    char line[CBM_SZ_2K];

    while (fgets(line, sizeof(line), fp) && gm_count < grep_limit) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        /* PowerShell output uses tab as delimiter (paths may contain colons
         * on Windows, e.g. C:\dir\file). Unix grep uses colon. */
#ifdef _WIN32
        char sep = '\t';
#else
        char sep = ':';
#endif
        char *sep1 = strchr(line, (unsigned char)sep);
        if (!sep1) {
            continue;
        }
        char *sep2 = strchr(sep1 + SKIP_ONE, (unsigned char)sep);
        if (!sep2) {
            continue;
        }
        *sep1 = '\0';
        *sep2 = '\0';

#ifdef _WIN32
        cbm_normalize_path_sep(line);
#endif
        const char *path = line;
        const char *file = strip_root_prefix(path, root_path, root_len);

        if (has_path_filter && cbm_regexec(path_regex, file, 0, NULL, 0) != CBM_REG_OK) {
            continue;
        }

        safe_grow(gm, gm_count, gm_cap, PAIR_LEN);
        snprintf(gm[gm_count].file, sizeof(gm[0].file), "%s", file);
        gm[gm_count].line = (int)strtol(sep1 + SKIP_ONE, NULL, CBM_DECIMAL_BASE);
        snprintf(gm[gm_count].content, sizeof(gm[0].content), "%s", sep2 + SKIP_ONE);
        sanitize_utf8(gm[gm_count].content);
        gm_count++;
    }

    *out_count = gm_count;
    return gm;
}

/* Find the tightest node containing a line in a file. Returns index or -1. */
static int find_tightest_node(cbm_node_t *nodes, int count, int line) {
    int best = CBM_NOT_FOUND;
    int best_span = MAX_LINE_SPAN;
    for (int j = 0; j < count; j++) {
        if (nodes[j].start_line <= line && nodes[j].end_line >= line) {
            int span = nodes[j].end_line - nodes[j].start_line;
            if (span < best_span) {
                best = j;
                best_span = span;
            }
        }
    }
    return best;
}

/* Add a grep hit to the search result set (merge into existing or create new). */
static void add_to_search_results(search_result_t **sr, int *sr_count, int *sr_cap, cbm_node_t *n,
                                  int line) {
    for (int j = 0; j < *sr_count; j++) {
        if ((*sr)[j].node_id == n->id) {
            if ((*sr)[j].match_count < CBM_SZ_64) {
                (*sr)[j].match_lines[(*sr)[j].match_count++] = line;
            }
            return;
        }
    }
    if (*sr_count >= *sr_cap) {
        *sr_cap *= PAIR_LEN;
        *sr = safe_realloc(*sr, *sr_cap * sizeof(search_result_t));
        memset(&(*sr)[*sr_count], 0, (*sr_cap - *sr_count) * sizeof(search_result_t));
    }
    search_result_t *r = &(*sr)[*sr_count];
    r->node_id = n->id;
    snprintf(r->node_name, sizeof(r->node_name), "%s", n->name ? n->name : "");
    snprintf(r->qualified_name, sizeof(r->qualified_name), "%s",
             n->qualified_name ? n->qualified_name : "");
    snprintf(r->label, sizeof(r->label), "%s", n->label ? n->label : "");
    snprintf(r->file, sizeof(r->file), "%s", n->file_path ? n->file_path : "");
    r->start_line = n->start_line;
    r->end_line = n->end_line;
    r->match_lines[0] = line;
    r->match_count = SKIP_ONE;
    (*sr_count)++;
}

/* Match a single grep hit to the tightest containing node, then add to sr or raw. */
static void classify_grep_hit(grep_match_t *hit, cbm_node_t *file_nodes, int file_node_count,
                              search_result_t **sr, int *sr_count, int *sr_cap, grep_match_t **raw,
                              int *raw_count, int *raw_cap) {
    int best = find_tightest_node(file_nodes, file_node_count, hit->line);
    if (best >= 0) {
        add_to_search_results(sr, sr_count, sr_cap, &file_nodes[best], hit->line);
    } else {
        if (*raw_count >= *raw_cap) {
            *raw_cap = (*raw_cap == 0) ? CBM_SZ_32 : *raw_cap * PAIR_LEN;
            *raw = safe_realloc(*raw, *raw_cap * sizeof(grep_match_t));
        }
        if (*raw) {
            (*raw)[(*raw_count)++] = *hit;
        }
    }
}

/* Free a file_nodes array returned from cbm_store_find_nodes_by_file. */
static void free_file_nodes(cbm_node_t *nodes, int count) {
    for (int j = 0; j < count; j++) {
        safe_str_free(&nodes[j].project);
        safe_str_free(&nodes[j].label);
        safe_str_free(&nodes[j].name);
        safe_str_free(&nodes[j].qualified_name);
        safe_str_free(&nodes[j].file_path);
        safe_str_free(&nodes[j].properties_json);
    }
    free(nodes);
}

/* Classify all grep matches file-by-file into search results and raw hits. */
static void classify_all_grep_hits(grep_match_t *gm, int gm_count, cbm_store_t *store,
                                   const char *project, search_result_t **sr, int *sr_count,
                                   int *sr_cap, grep_match_t **raw, int *raw_count, int *raw_cap) {
    qsort(gm, gm_count, sizeof(grep_match_t), grep_match_cmp);
    int i = 0;
    while (i < gm_count) {
        const char *cur_file = gm[i].file;
        int file_start = i;
        while (i < gm_count && strcmp(gm[i].file, cur_file) == 0) {
            i++;
        }
        cbm_node_t *file_nodes = NULL;
        int file_node_count = 0;
        if (store) {
            cbm_store_find_nodes_by_file(store, project, cur_file, &file_nodes, &file_node_count);
        }
        for (int mi = file_start; mi < i; mi++) {
            classify_grep_hit(&gm[mi], file_nodes, file_node_count, sr, sr_count, sr_cap, raw,
                              raw_count, raw_cap);
        }
        free_file_nodes(file_nodes, file_node_count);
    }
}

/* Write indexed file list for scoped grep. Returns true if scoped. */
static bool write_scoped_filelist(cbm_mcp_server_t *srv, const char *project, const char *root_path,
                                  char *filelist, size_t filelist_sz) {
    cbm_store_t *pre_store = resolve_store(srv, project);
    if (!pre_store) {
        return false;
    }
    char **indexed_files = NULL;
    int indexed_count = 0;
    if (cbm_store_list_files(pre_store, project, &indexed_files, &indexed_count) != CBM_STORE_OK ||
        indexed_count == 0) {
        return false;
    }
    snprintf(filelist, filelist_sz, "%s/cbm_search_files_XXXXXX", cbm_tmpdir());
    int fd = cbm_mkstemp(filelist);
    if (fd < 0) {
        for (int fi = 0; fi < indexed_count; fi++) {
            free(indexed_files[fi]);
        }
        free(indexed_files);
        return false;
    }
    FILE *fl = fdopen(fd, "wb");
    bool ok = false;
    if (fl) {
        for (int fi = 0; fi < indexed_count; fi++) {
            /* Use forward slashes for PowerShell on Windows. POSIX uses NUL
             * delimiters so whitespace and backslashes remain part of paths. */
            (void)fprintf(fl, "%s/%s", root_path, indexed_files[fi]);
#ifdef _WIN32
            (void)fputc('\n', fl);
#else
            (void)fputc('\0', fl);
#endif
        }
        (void)fclose(fl);
        ok = true;
    } else {
#ifdef _WIN32
        (void)_close(fd);
#else
        (void)close(fd);
#endif
        (void)cbm_unlink(filelist);
    }
    for (int fi = 0; fi < indexed_count; fi++) {
        free(indexed_files[fi]);
    }
    free(indexed_files);
    return ok;
}

/* Parse search mode string (0=compact, 1=full, 2=files). */
static int parse_search_mode(const char *mode_str) {
    if (!mode_str) {
        return 0;
    }
    if (strcmp(mode_str, "full") == 0) {
        return SKIP_ONE;
    }
    if (strcmp(mode_str, "files") == 0) {
        return MCP_RETURN_2;
    }
    return 0;
}

/* Validate shell-safe arguments for search. */
/* Search/grep paths and globs are ALWAYS single-quoted (POSIX sh) or
 * double-/single-quoted (Windows cmd/PowerShell) on the command line, which
 * neutralises '&' — a very common character in real paths (R&D, "Foo & Bar",
 * OneDrive). Accept '&' here while still rejecting every metacharacter that
 * could break out of the quoting (#272). */
static bool validate_search_path_arg(const char *s) {
    if (!s) {
        return false;
    }
    for (const char *p = s; *p; p++) {
        switch (*p) {
        case '\'':
        case '"':
        case ';':
        case '|':
        case '$':
        case '`':
        case '<':
        case '>':
        case '\n':
        case '\r':
#ifndef _WIN32
        case '\\':
#endif
            return false;
        default:
            break;
        }
    }
    return true;
}

static bool validate_search_args(const char *root_path, const char *file_pattern) {
    if (!validate_search_path_arg(root_path)) {
        return false;
    }
    if (file_pattern && !validate_search_path_arg(file_pattern)) {
        return false;
    }
    return true;
}

/* Write pattern to a temp file for grep -f. Returns true on success. */
static bool write_pattern_file(char *tmpfile, int tmpfile_sz, const char *pattern) {
    snprintf(tmpfile, tmpfile_sz, "%s/cbm_search_XXXXXX", cbm_tmpdir());
    int fd = cbm_mkstemp(tmpfile);
    if (fd < 0) {
        return false;
    }
    FILE *tf = fdopen(fd, "w");
    if (!tf) {
#ifdef _WIN32
        (void)_close(fd);
#else
        (void)close(fd);
#endif
        (void)cbm_unlink(tmpfile);
        return false;
    }
    (void)fprintf(tf, "%s\n", pattern);
    (void)fclose(tf);
    return true;
}

/* Compile a path filter regex. Returns true if compiled successfully. */
static bool compile_path_filter(const char *filter, cbm_regex_t *re) {
    if (!filter || !filter[0]) {
        return false;
    }
    return cbm_regcomp(re, filter, CBM_REG_EXTENDED | CBM_REG_NOSUB) == CBM_REG_OK;
}

static char *handle_search_code(cbm_mcp_server_t *srv, const char *args) {
    char *pattern = cbm_mcp_get_string_arg(args, "pattern");
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *path_filter = cbm_mcp_get_string_arg(args, "path_filter");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    int limit = cbm_mcp_get_int_arg(args, "limit", MCP_DEFAULT_LIMIT);
    int context_lines = cbm_mcp_get_int_arg(args, "context", 0);
    bool use_regex = cbm_mcp_get_bool_arg(args, "regex");
    uint64_t search_t0 = cbm_now_ms();
    /* In literal (non-regex) mode a '|' is matched as a byte, not alternation —
     * a common silent 0-match trap; flagged in the result warnings (#282). */
    bool pat_has_pipe = pattern && strchr(pattern, '|') != NULL;

    int mode = parse_search_mode(mode_str);
    free(mode_str);

    cbm_regex_t path_regex;
    bool has_path_filter = compile_path_filter(path_filter, &path_regex);
    free(path_filter);
    path_filter = NULL;

    if (!pattern) {
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result("pattern is required", true);
    }

    /* Project is required */
    if (!project) {
        free(pattern);
        free(file_pattern);
        char *_err = build_project_list_error("project is required");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        return _res;
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(pattern);
        free(project);
        free(file_pattern);
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        return _res;
    }

    if (!validate_search_args(root_path, file_pattern)) {
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result("path or file_pattern contains invalid characters", true);
    }

    /* issue #283: when regex=true, a syntactically invalid pattern (e.g. an
     * unclosed group) makes the underlying grep fail, which the handler would
     * otherwise report as an empty result set — indistinguishable from a
     * legitimate no-match. Validate the user's regex up front and return an
     * explicit error so callers can tell "broken pattern" from "no matches". */
    if (use_regex) {
        cbm_regex_t probe;
        if (cbm_regcomp(&probe, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != CBM_REG_OK) {
            if (has_path_filter) {
                cbm_regfree(&path_regex);
            }
            free(root_path);
            free(pattern);
            free(project);
            free(file_pattern);
            return cbm_mcp_text_result(
                "invalid regex pattern (regex=true): check for unbalanced (), [], or {}", true);
        }
        cbm_regfree(&probe);
    }

    /* ── Phase 0.5: Multi-word → regex conversion ───────────── */
    /* If pattern contains whitespace and is not already a regex, convert to a
     * regex that matches all words in order: "foo bar baz" → "foo.*bar.*baz".
     * This avoids requiring the exact phrase as a contiguous substring. */
    if (!use_regex && strchr(pattern, ' ')) {
        size_t plen = strlen(pattern);
        /* Worst case: every char is a space → ".*" between each char */
        char *regex_pat = malloc(plen * 3 + 1);
        if (regex_pat) {
            char *dst = regex_pat;
            const char *src = pattern;
            bool in_space = false;
            while (*src) {
                if (*src == ' ' || *src == '\t') {
                    if (!in_space) {
                        *dst++ = '.';
                        *dst++ = '*';
                        in_space = true;
                    }
                } else {
                    /* Escape regex metacharacters from user input */
                    if (strchr("\\^$.|?*+()[]{}", *src)) {
                        *dst++ = '\\';
                    }
                    *dst++ = *src;
                    in_space = false;
                }
                src++;
            }
            *dst = '\0';
            free(pattern);
            pattern = regex_pat;
            use_regex = true;
        }
    }

    /* ── Phase 1: Grep scan ──────────────────────────────────── */
    char tmpfile[CBM_SZ_256];
    if (!write_pattern_file(tmpfile, sizeof(tmpfile), pattern)) {
        char errmsg[CBM_SZ_256];
        snprintf(errmsg, sizeof(errmsg), "search failed: cannot create temp file (%s)",
                 strerror(errno));
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(errmsg, true);
    }

    /* No grep-level match limit — let grep find all matches, then dedup and
     * cap in our code. The -m flag caused results from large vendored files
     * to exhaust the quota before reaching project source files. */
    enum { GREP_MAX_MATCHES = 500 };
    int grep_limit = GREP_MAX_MATCHES;

    /* Scope grep to indexed files only — avoids scanning vendored/generated code.
     * Query the graph for distinct file paths, write them to a temp file,
     * then use xargs to pass them to grep. Falls back to recursive grep if
     * no indexed files found (project not fully indexed). */
    char filelist[CBM_SZ_256] = {0};
    bool scoped = false;

    scoped = write_scoped_filelist(srv, project, root_path, filelist, sizeof(filelist));

    char cmd[CBM_SZ_4K];
    build_grep_cmd(cmd, sizeof(cmd), use_regex, scoped, file_pattern, tmpfile, filelist, root_path);

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        cbm_unlink(tmpfile);
        if (scoped) {
            cbm_unlink(filelist);
        }
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result("search failed", true);
    }

    /* Collect grep matches into array */
    int gm_count = 0;
    grep_match_t *gm = collect_grep_matches(fp, root_path, strlen(root_path), has_path_filter,
                                            &path_regex, grep_limit, &gm_count);
    cbm_pclose(fp);
    cbm_unlink(tmpfile);
    if (scoped) {
        cbm_unlink(filelist);
    }

    /* ── Phase 2+3: Block expansion + graph ranking ──────────── */
    /* Sort grep matches by file for contiguous processing.
     * Then: one SQL query per unique file for nodes, one batch query for all degrees. */

    cbm_store_t *store = resolve_store(srv, project);

    int sr_cap = CBM_SZ_32;
    int sr_count = 0;
    search_result_t *sr = calloc(sr_cap, sizeof(search_result_t));

    int raw_cap = CBM_SZ_32;
    int raw_count = 0;
    grep_match_t *raw = malloc(raw_cap * sizeof(grep_match_t));

    classify_all_grep_hits(gm, gm_count, store, project, &sr, &sr_count, &sr_cap, &raw, &raw_count,
                           &raw_cap);

    /* Phase 3: batch degree query — ONE query for all results instead of 2×N */
    if (store && sr_count > 0) {
        int64_t *ids = malloc(sr_count * sizeof(int64_t));
        int *in_degs = malloc(sr_count * sizeof(int));
        int *out_degs = malloc(sr_count * sizeof(int));
        for (int j = 0; j < sr_count; j++) {
            ids[j] = sr[j].node_id;
        }
        if (cbm_store_batch_count_degrees(store, ids, sr_count, "CALLS", in_degs, out_degs) ==
            CBM_STORE_OK) {
            for (int j = 0; j < sr_count; j++) {
                sr[j].in_degree = in_degs[j];
                sr[j].out_degree = out_degs[j];
            }
        }
        free(ids);
        free(in_degs);
        free(out_degs);
    }

    /* Compute scores and sort */
    for (int j = 0; j < sr_count; j++) {
        sr[j].score = compute_search_score(&sr[j]);
    }
    if (sr_count > SKIP_ONE) {
        qsort(sr, sr_count, sizeof(search_result_t), search_result_cmp);
    }

    /* ── Phase 4: Context assembly (extracted helper) ─────────── */

    char *result =
        assemble_search_output(sr, sr_count, raw, raw_count, gm_count, limit, mode, context_lines,
                               root_path, pat_has_pipe && !use_regex, cbm_now_ms() - search_t0);
    free(gm);
    free(sr);
    free(raw);
    free(root_path);
    free(pattern);
    free(project);
    free(file_pattern);
    if (has_path_filter) {
        cbm_regfree(&path_regex);
    }
    return result;
}

/* ── detect_changes ───────────────────────────────────────────── */

static const char *context_sql_text(sqlite3_stmt *stmt, int column);

typedef struct {
    char status;
    char *type;
    char *path;
    char *old_path;
} detect_change_t;

typedef struct {
    int64_t id;
    char *name;
    char *qualified_name;
    char *label;
    char *file_path;
    int depth;
    char *path_explanation;
} detect_impact_t;

static void detect_changes_free(detect_change_t *changes, int count) {
    for (int i = 0; i < count; i++) {
        free(changes[i].type);
        free(changes[i].path);
        free(changes[i].old_path);
    }
    free(changes);
}

static void detect_impacts_free(detect_impact_t *impacts, int count) {
    for (int i = 0; i < count; i++) {
        free(impacts[i].name);
        free(impacts[i].qualified_name);
        free(impacts[i].label);
        free(impacts[i].file_path);
        free(impacts[i].path_explanation);
    }
    free(impacts);
}

static bool detect_is_iso_date(const char *value) {
    if (!value || strlen(value) < 10 || value[4] != '-' || value[7] != '-') {
        return false;
    }
    for (int i = 0; i < 10; i++) {
        if (i == 4 || i == 7) {
            continue;
        }
        if (value[i] < '0' || value[i] > '9') {
            return false;
        }
    }
    return true;
}

/* Resolve a ref/date before building the diff command. Besides producing one
 * stable commit, this prevents a failed git command from being hidden by the
 * following sort in a shell pipeline. */
static char *detect_resolve_baseline(const char *root_path, const char *baseline) {
    if (!root_path || !baseline || !cbm_validate_shell_arg(baseline)) {
        return NULL;
    }
    char command[CBM_SZ_4K];
#ifdef _WIN32
    if (detect_is_iso_date(baseline)) {
        snprintf(command, sizeof(command),
                 "git -C \"%s\" rev-list -1 --before=\"%s 23:59:59\" HEAD 2>NUL", root_path,
                 baseline);
    } else {
        snprintf(command, sizeof(command), "git -C \"%s\" rev-parse --verify \"%s^{commit}\" 2>NUL",
                 root_path, baseline);
    }
#else
    if (detect_is_iso_date(baseline)) {
        snprintf(command, sizeof(command),
                 "git -C '%s' rev-list -1 --before='%s 23:59:59' HEAD 2>/dev/null", root_path,
                 baseline);
    } else {
        snprintf(command, sizeof(command),
                 "git -C '%s' rev-parse --verify '%s^{commit}' 2>/dev/null", root_path, baseline);
    }
#endif
    FILE *fp = cbm_popen(command, "r");
    if (!fp) {
        return NULL;
    }
    char commit[96] = {0};
    bool read_commit = fgets(commit, sizeof(commit), fp) != NULL;
    int status = cbm_pclose(fp);
    size_t len = strlen(commit);
    while (len > 0 && (commit[len - 1] == '\n' || commit[len - 1] == '\r')) {
        commit[--len] = '\0';
    }
    if (!read_commit || status != 0 || len < 7 || len > 64) {
        return NULL;
    }
    for (size_t i = 0; i < len; i++) {
        bool hex = (commit[i] >= '0' && commit[i] <= '9') ||
                   (commit[i] >= 'a' && commit[i] <= 'f') ||
                   (commit[i] >= 'A' && commit[i] <= 'F');
        if (!hex) {
            return NULL;
        }
    }
    return heap_strdup(commit);
}

static const char *detect_status_type(char status) {
    switch (status) {
    case 'A':
        return "added";
    case 'D':
        return "deleted";
    case 'R':
        return "renamed";
    case 'C':
        return "copied";
    case 'T':
        return "type_changed";
    case 'U':
        return "unmerged";
    default:
        return "modified";
    }
}

static int detect_change_cmp(const void *left, const void *right) {
    const detect_change_t *a = left;
    const detect_change_t *b = right;
    int cmp = strcmp(a->path ? a->path : "", b->path ? b->path : "");
    if (cmp != 0) {
        return cmp;
    }
    cmp = strcmp(a->old_path ? a->old_path : "", b->old_path ? b->old_path : "");
    if (cmp != 0) {
        return cmp;
    }
    return (int)a->status - (int)b->status;
}

static bool detect_change_exists(detect_change_t *changes, int count, char status,
                                 const char *path, const char *old_path) {
    for (int i = 0; i < count; i++) {
        if (changes[i].status == status && strcmp(changes[i].path, path) == 0 &&
            strcmp(changes[i].old_path ? changes[i].old_path : "",
                   old_path ? old_path : "") == 0) {
            return true;
        }
    }
    return false;
}

static void detect_change_add(detect_change_t **changes, int *count, int *capacity, char status,
                              const char *path, const char *old_path) {
    if (!path || !path[0] || detect_change_exists(*changes, *count, status, path, old_path)) {
        return;
    }
    if (*count == *capacity) {
        int next_capacity = *capacity == 0 ? 16 : *capacity * 2;
        detect_change_t *next =
            realloc(*changes, (size_t)next_capacity * sizeof(**changes));
        if (!next) {
            return;
        }
        *changes = next;
        *capacity = next_capacity;
    }
    detect_change_t *change = &(*changes)[(*count)++];
    memset(change, 0, sizeof(*change));
    change->status = status;
    change->type = heap_strdup(detect_status_type(status));
    change->path = heap_strdup(path);
    change->old_path = old_path ? heap_strdup(old_path) : NULL;
}

static int detect_impact_find(detect_impact_t *impacts, int count, int64_t id) {
    for (int i = 0; i < count; i++) {
        if (impacts[i].id == id) {
            return i;
        }
    }
    return -1;
}

static int detect_impact_add(detect_impact_t **impacts, int *count, int *capacity,
                             const cbm_node_t *node, int depth, const char *explanation) {
    int existing = detect_impact_find(*impacts, *count, node->id);
    if (existing >= 0) {
        detect_impact_t *impact = &(*impacts)[existing];
        if (depth < impact->depth ||
            (depth == impact->depth &&
             strcmp(explanation, impact->path_explanation ? impact->path_explanation : "") < 0)) {
            impact->depth = depth;
            free(impact->path_explanation);
            impact->path_explanation = heap_strdup(explanation);
        }
        return existing;
    }
    if (*count >= 512) {
        return -1;
    }
    if (*count == *capacity) {
        int next_capacity = *capacity == 0 ? 32 : *capacity * 2;
        if (next_capacity > 512) {
            next_capacity = 512;
        }
        detect_impact_t *next =
            realloc(*impacts, (size_t)next_capacity * sizeof(**impacts));
        if (!next) {
            return -1;
        }
        *impacts = next;
        *capacity = next_capacity;
    }
    detect_impact_t *impact = &(*impacts)[*count];
    memset(impact, 0, sizeof(*impact));
    impact->id = node->id;
    impact->name = heap_strdup(node->name ? node->name : "");
    impact->qualified_name = heap_strdup(node->qualified_name ? node->qualified_name : "");
    impact->label = heap_strdup(node->label ? node->label : "");
    impact->file_path = heap_strdup(node->file_path ? node->file_path : "");
    impact->depth = depth;
    impact->path_explanation = heap_strdup(explanation ? explanation : "");
    return (*count)++;
}

static bool detect_dependency_edge(const char *type) {
    return type &&
           (strcmp(type, "CALLS") == 0 || strcmp(type, "IMPORTS") == 0 ||
            strcmp(type, "IMPLEMENTS") == 0 || strcmp(type, "EXTENDS") == 0 ||
            strcmp(type, "CONTAINS") == 0 || strcmp(type, "USES") == 0 ||
            strcmp(type, "TESTS") == 0);
}

static void detect_seed_file(cbm_store_t *store, const char *project, const char *file,
                             detect_impact_t **impacts, int *count, int *capacity) {
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    if (cbm_store_find_nodes_by_file(store, project, file, &nodes, &node_count) != CBM_STORE_OK) {
        return;
    }
    char explanation[CBM_SZ_2K];
    snprintf(explanation, sizeof(explanation), "changed file: %s", file);
    for (int i = 0; i < node_count; i++) {
        if (!nodes[i].label || strcmp(nodes[i].label, "File") == 0 ||
            strcmp(nodes[i].label, "Folder") == 0 || strcmp(nodes[i].label, "Project") == 0) {
            continue;
        }
        detect_impact_add(impacts, count, capacity, &nodes[i], 0, explanation);
    }
    cbm_store_free_nodes(nodes, node_count);
}

static void detect_expand_impacts(cbm_store_t *store, const char *project,
                                  detect_impact_t **impacts, int *count, int *capacity,
                                  int max_depth) {
    sqlite3 *db = cbm_store_get_db(store);
    const char *sql =
        "SELECT e.type,e.source_id,e.target_id,s.qualified_name,t.qualified_name "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.project=?1 AND (e.source_id=?2 OR e.target_id=?2) "
        "ORDER BY e.type,s.qualified_name,t.qualified_name,e.source_id,e.target_id,e.id";
    int cursor = 0;
    while (cursor < *count) {
        detect_impact_t current = (*impacts)[cursor++];
        if (current.depth >= max_depth) {
            continue;
        }
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            return;
        }
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, current.id);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *edge_type = context_sql_text(stmt, 0);
            if (!detect_dependency_edge(edge_type)) {
                continue;
            }
            int64_t source_id = sqlite3_column_int64(stmt, 1);
            int64_t target_id = sqlite3_column_int64(stmt, 2);
            const char *source_qn = context_sql_text(stmt, 3);
            const char *target_qn = context_sql_text(stmt, 4);
            int64_t neighbor_id = source_id == current.id ? target_id : source_id;
            cbm_node_t neighbor = {0};
            if (cbm_store_find_node_by_id(store, neighbor_id, &neighbor) != CBM_STORE_OK) {
                continue;
            }
            char explanation[CBM_SZ_4K];
            snprintf(explanation, sizeof(explanation), "%s; %s --%s--> %s",
                     current.path_explanation ? current.path_explanation : "", source_qn,
                     edge_type, target_qn);
            detect_impact_add(impacts, count, capacity, &neighbor, current.depth + 1,
                              explanation);
            free_node_contents(&neighbor);
        }
        sqlite3_finalize(stmt);
    }
}

static int detect_impact_cmp(const void *left, const void *right) {
    const detect_impact_t *a = left;
    const detect_impact_t *b = right;
    if (a->depth != b->depth) {
        return a->depth < b->depth ? -1 : 1;
    }
    int cmp = strcmp(a->qualified_name ? a->qualified_name : "",
                     b->qualified_name ? b->qualified_name : "");
    if (cmp != 0) {
        return cmp;
    }
    cmp = strcmp(a->file_path ? a->file_path : "", b->file_path ? b->file_path : "");
    if (cmp != 0) {
        return cmp;
    }
    return a->id < b->id ? -1 : (a->id > b->id ? 1 : 0);
}

static char *handle_detect_changes(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *base_branch = cbm_mcp_get_string_arg(args, "base_branch");
    char *since = cbm_mcp_get_string_arg(args, "since");
    char *scope = cbm_mcp_get_string_arg(args, "scope");
    int depth = cbm_mcp_get_int_arg(args, "depth", MCP_DEFAULT_BFS_DEPTH);

    /* scope: "files" = just changed files, "symbols" = files + symbols (default) */
    bool want_symbols = !scope || strcmp(scope, "symbols") == 0 || strcmp(scope, "impact") == 0;

    if (!base_branch) {
        base_branch = heap_strdup("main");
    }

    if (depth < 0 || depth > 4 || !cbm_validate_shell_arg(base_branch) ||
        (since && !cbm_validate_shell_arg(since))) {
        free(project);
        free(base_branch);
        free(since);
        free(scope);
        return cbm_mcp_text_result(
            "{\"code\":\"invalid_arguments\",\"message\":\"depth must be 0..4 and git "
            "baseline must not contain shell metacharacters\"}",
            true);
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(project);
        free(base_branch);
        free(since);
        free(scope);
        return cbm_mcp_text_result("project not found", true);
    }

    if (!validate_search_path_arg(root_path)) {
        free(root_path);
        free(project);
        free(base_branch);
        free(since);
        free(scope);
        return cbm_mcp_text_result("project path contains invalid characters", true);
    }

    const char *requested_baseline = since && since[0] ? since : base_branch;
    char *baseline_commit = detect_resolve_baseline(root_path, requested_baseline);
    if (!baseline_commit) {
        char message[CBM_SZ_1K];
        snprintf(message, sizeof(message),
                 "{\"code\":\"invalid_baseline\",\"message\":\"Cannot resolve git ref or date: "
                 "%s\"}",
                 requested_baseline);
        free(root_path);
        free(project);
        free(base_branch);
        free(since);
        free(scope);
        return cbm_mcp_text_result(message, true);
    }

    /* Name-status retains add/delete/rename semantics. Include committed,
     * staged, unstaged, and untracked changes in one deterministically sorted
     * stream. */
    char cmd[CBM_SZ_4K];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd),
             "(git -C \"%s\" diff --name-status -M \"%s\"...HEAD 2>NUL & "
             "git -C \"%s\" diff --cached --name-status -M 2>NUL & "
             "git -C \"%s\" diff --name-status -M 2>NUL)",
             root_path, baseline_commit, root_path, root_path);
#else
    snprintf(cmd, sizeof(cmd),
             "{ git -C '%s' diff --name-status -M '%s'...HEAD 2>/dev/null; "
             "git -C '%s' diff --cached --name-status -M 2>/dev/null; "
             "git -C '%s' diff --name-status -M 2>/dev/null; "
             "git -C '%s' ls-files --others --exclude-standard 2>/dev/null | "
             "sed 's/^/A\\t/'; } | LC_ALL=C sort -u",
             root_path, baseline_commit, root_path, root_path, root_path);
#endif

    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        char errmsg[CBM_SZ_256];
        snprintf(errmsg, sizeof(errmsg),
                 "git diff failed: cannot execute command (%s). Check that git is installed.",
                 strerror(errno));
        free(root_path);
        free(baseline_commit);
        free(project);
        free(base_branch);
        free(since);
        free(scope);
        return cbm_mcp_text_result(errmsg, true);
    }

    cbm_store_t *store = srv->store;
    detect_change_t *changes = NULL;
    int change_count = 0;
    int change_capacity = 0;
    char line[CBM_SZ_1K];
    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }
        char *save = NULL;
        char *status_text = strtok_r(line, "\t", &save);
        char *first_path = strtok_r(NULL, "\t", &save);
        char *second_path = strtok_r(NULL, "\t", &save);
        if (!status_text || !first_path) {
            continue;
        }
        char status = status_text[0];
        if ((status == 'R' || status == 'C') && second_path) {
            detect_change_add(&changes, &change_count, &change_capacity, status, second_path,
                              first_path);
        } else {
            detect_change_add(&changes, &change_count, &change_capacity, status, first_path,
                              NULL);
        }
    }
    int git_status = cbm_pclose(fp);

    if (change_count > 1) {
        qsort(changes, (size_t)change_count, sizeof(*changes), detect_change_cmp);
    }

    detect_impact_t *impacts = NULL;
    int impact_count = 0;
    int impact_capacity = 0;
    if (want_symbols) {
        const char *effective_project = project ? project : srv->current_project;
        for (int i = 0; i < change_count; i++) {
            detect_seed_file(store, effective_project, changes[i].path, &impacts, &impact_count,
                             &impact_capacity);
            if (changes[i].old_path) {
                detect_seed_file(store, effective_project, changes[i].old_path, &impacts,
                                 &impact_count, &impact_capacity);
            }
        }
        if (impact_count > 1) {
            qsort(impacts, (size_t)impact_count, sizeof(*impacts), detect_impact_cmp);
        }
        detect_expand_impacts(store, effective_project, &impacts, &impact_count, &impact_capacity,
                              depth);
        if (impact_count > 1) {
            qsort(impacts, (size_t)impact_count, sizeof(*impacts), detect_impact_cmp);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);
    yyjson_mut_obj_add_str(doc, root_obj, "project",
                           project ? project : (srv->current_project ? srv->current_project : ""));
    yyjson_mut_obj_add_str(doc, root_obj, "baseline_commit", baseline_commit);
    if (since && since[0]) {
        yyjson_mut_obj_add_str(doc, root_obj, "since", since);
    } else {
        yyjson_mut_obj_add_str(doc, root_obj, "base_branch", base_branch);
    }

    yyjson_mut_val *changed = yyjson_mut_arr(doc);
    yyjson_mut_val *change_details = yyjson_mut_arr(doc);
    for (int i = 0; i < change_count; i++) {
        yyjson_mut_arr_add_str(doc, changed, changes[i].path);
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "type", changes[i].type);
        yyjson_mut_obj_add_str(doc, item, "path", changes[i].path);
        if (changes[i].old_path) {
            yyjson_mut_obj_add_str(doc, item, "old_path", changes[i].old_path);
        }
        yyjson_mut_obj_add_strn(doc, item, "git_status", &changes[i].status, 1);
        yyjson_mut_arr_add_val(change_details, item);
    }

    yyjson_mut_val *impacted_symbols = yyjson_mut_arr(doc);
    yyjson_mut_val *impacted_tests = yyjson_mut_arr(doc);
    yyjson_mut_val *impact_paths = yyjson_mut_arr(doc);
    for (int i = 0; i < impact_count; i++) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "name", impacts[i].name);
        yyjson_mut_obj_add_str(doc, item, "qualified_name", impacts[i].qualified_name);
        yyjson_mut_obj_add_str(doc, item, "label", impacts[i].label);
        yyjson_mut_obj_add_str(doc, item, "file_path", impacts[i].file_path);
        yyjson_mut_obj_add_int(doc, item, "depth", impacts[i].depth);
        yyjson_mut_obj_add_str(doc, item, "impact_path", impacts[i].path_explanation);
        if (is_test_file(impacts[i].file_path)) {
            yyjson_mut_arr_add_val(impacted_tests, item);
        } else {
            yyjson_mut_arr_add_val(impacted_symbols, item);
        }

        yyjson_mut_val *path_item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, path_item, "symbol", impacts[i].qualified_name);
        yyjson_mut_obj_add_int(doc, path_item, "depth", impacts[i].depth);
        yyjson_mut_obj_add_str(doc, path_item, "path", impacts[i].path_explanation);
        yyjson_mut_arr_add_val(impact_paths, path_item);
    }

    yyjson_mut_obj_add_val(doc, root_obj, "changed_files", changed);
    yyjson_mut_obj_add_val(doc, root_obj, "changes", change_details);
    yyjson_mut_obj_add_int(doc, root_obj, "changed_count", change_count);
    yyjson_mut_obj_add_val(doc, root_obj, "impacted_symbols", impacted_symbols);
    yyjson_mut_obj_add_val(doc, root_obj, "impacted_tests", impacted_tests);
    yyjson_mut_obj_add_val(doc, root_obj, "impact_paths", impact_paths);
    yyjson_mut_obj_add_int(doc, root_obj, "depth", depth);
    if (git_status != 0) {
        yyjson_mut_obj_add_int(doc, root_obj, "git_status", git_status);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    detect_changes_free(changes, change_count);
    detect_impacts_free(impacts, impact_count);
    free(root_path);
    free(baseline_commit);
    free(project);
    free(base_branch);
    free(since);
    free(scope);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── manage_adr ───────────────────────────────────────────────── */

/* ADR "sections" mode: list markdown headers ('#'-prefixed lines) from the
 * ADR content string. */
static void adr_list_sections_from_content(yyjson_mut_doc *doc, yyjson_mut_val *root_obj,
                                           const char *content) {
    yyjson_mut_val *sections = yyjson_mut_arr(doc);
    const char *p = content;
    while (p && *p) {
        const char *eol = strchr(p, '\n');
        size_t linelen = eol ? (size_t)(eol - p) : strlen(p);
        while (linelen > 0 && p[linelen - SKIP_ONE] == '\r') {
            linelen--;
        }
        if (linelen > 0 && p[0] == '#') {
            char hdr[CBM_SZ_1K];
            if (linelen >= sizeof(hdr)) {
                linelen = sizeof(hdr) - SKIP_ONE;
            }
            memcpy(hdr, p, linelen);
            hdr[linelen] = '\0';
            yyjson_mut_arr_add_strcpy(doc, sections, hdr);
        }
        if (!eol) {
            break;
        }
        p = eol + SKIP_ONE;
    }
    yyjson_mut_obj_add_val(doc, root_obj, "sections", sections);
}

/* Read the legacy file-based ADR (<root>/.codebase-memory/adr.md), used by
 * older versions. Returns a heap buffer (caller frees) or NULL if missing/
 * empty. Kept only to migrate old ADRs into the store (#256). */
static char *adr_read_legacy_file(const char *root_path) {
    if (!root_path) {
        return NULL;
    }
    char adr_path[CBM_SZ_4K];
    snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", root_path);
    FILE *fp = fopen(adr_path, "r");
    if (!fp) {
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    if (sz <= 0) {
        (void)fclose(fp);
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_SET);
    char *buf = malloc((size_t)sz + SKIP_ONE);
    if (!buf) {
        (void)fclose(fp);
        return NULL;
    }
    size_t n = fread(buf, SKIP_ONE, (size_t)sz, fp);
    buf[n] = '\0';
    (void)fclose(fp);
    if (buf[0] == '\0') {
        free(buf);
        return NULL;
    }
    return buf;
}

#define ADR_EMPTY_HINT                                                             \
    "No ADR yet. Create one with manage_adr(mode='update', "                       \
    "content='## PURPOSE\\n...\\n\\n## STACK\\n...\\n\\n## ARCHITECTURE\\n..."     \
    "\\n\\n## PATTERNS\\n...\\n\\n## TRADEOFFS\\n...\\n\\n## PHILOSOPHY\\n...'). " \
    "For guided creation: explore the codebase with get_architecture, "            \
    "then draft and store. Sections: PURPOSE, STACK, ARCHITECTURE, "               \
    "PATTERNS, TRADEOFFS, PHILOSOPHY."

static char *handle_manage_adr(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *content = cbm_mcp_get_string_arg(args, "content");

    if (!mode_str) {
        mode_str = heap_strdup("get");
    }

    /* ADRs are stored in the SQLite store (project_summaries), the SAME
     * backend the UI /api/adr endpoints use — so writes via the MCP tool and
     * the UI are visible to each other (#256). */
    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        free(project);
        free(mode_str);
        free(content);
        return cbm_mcp_text_result("project not found", true);
    }

    /* One-time migration: older versions wrote ADRs to a file at
     * <root>/.codebase-memory/adr.md. If the store has no ADR yet but that
     * legacy file exists, import it so nothing is lost on upgrade. */
    cbm_adr_t adr;
    memset(&adr, 0, sizeof(adr));
    bool have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
    if (!have_adr) {
        char *root_path = get_project_root(srv, project);
        char *legacy = adr_read_legacy_file(root_path);
        free(root_path);
        if (legacy) {
            if (cbm_store_adr_store(store, project, legacy) == CBM_STORE_OK) {
                have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
            }
            free(legacy);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    bool is_error = false;
    if ((strcmp(mode_str, "update") == 0 || strcmp(mode_str, "store") == 0) && content) {
        if (cbm_store_adr_store(store, project, content) == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "updated");
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "write_error");
            is_error = true;
        }
    } else if (strcmp(mode_str, "sections") == 0) {
        adr_list_sections_from_content(doc, root_obj, have_adr ? adr.content : NULL);
    } else { /* get */
        if (have_adr && adr.content) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "content", adr.content);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "content", "");
            yyjson_mut_obj_add_str(doc, root_obj, "status", "no_adr");
            yyjson_mut_obj_add_str(doc, root_obj, "adr_hint", ADR_EMPTY_HINT);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (have_adr) {
        cbm_store_adr_free(&adr);
    }
    free(project);
    free(mode_str);
    free(content);

    char *result = cbm_mcp_text_result(json, is_error);
    free(json);
    return result;
}

/* ── get_context ──────────────────────────────────────────────── */

static bool context_intent_valid(const char *intent) {
    return strcmp(intent, "understand") == 0 || strcmp(intent, "debug") == 0 ||
           strcmp(intent, "change") == 0 || strcmp(intent, "review") == 0 ||
           strcmp(intent, "security") == 0;
}

static bool context_args_well_typed(const char *args) {
    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    bool valid = root && yyjson_is_obj(root);
    if (valid) {
        yyjson_val *query = yyjson_obj_get(root, "query");
        yyjson_val *intent = yyjson_obj_get(root, "intent");
        yyjson_val *depth = yyjson_obj_get(root, "depth");
        yyjson_val *budget = yyjson_obj_get(root, "budget_tokens");
        yyjson_val *include_tests = yyjson_obj_get(root, "include_tests");
        yyjson_val *focus = yyjson_obj_get(root, "focus_symbols");
        valid = (!query || yyjson_is_str(query)) && (!intent || yyjson_is_str(intent)) &&
                (!depth || yyjson_is_int(depth)) && (!budget || yyjson_is_int(budget)) &&
                (!include_tests || yyjson_is_bool(include_tests)) &&
                (!focus || yyjson_is_arr(focus));
        if (valid && focus) {
            size_t idx, max;
            yyjson_val *item;
            yyjson_arr_foreach(focus, idx, max, item) {
                if (!yyjson_is_str(item)) {
                    valid = false;
                    break;
                }
            }
        }
    }
    yyjson_doc_free(doc);
    return valid;
}

enum {
    CONTEXT_SOURCE_EXACT = 1u,
    CONTEXT_SOURCE_SYMBOL = 2u,
    CONTEXT_SOURCE_BM25 = 4u,
    CONTEXT_MAX_CANDIDATES = 128,
    CONTEXT_RRF_K = 60,
};

typedef struct {
    int64_t id;
    char *name;
    char *qualified_name;
    char *label;
    char *file_path;
    int start_line;
    int end_line;
    int exact_rank;
    int symbol_rank;
    int bm25_rank;
    unsigned sources;
    double score;
} context_candidate_t;

static const char *context_sql_text(sqlite3_stmt *stmt, int column) {
    const unsigned char *value = sqlite3_column_text(stmt, column);
    return value ? (const char *)value : "";
}

static void context_candidates_free(context_candidate_t *candidates, int count) {
    for (int i = 0; i < count; i++) {
        free(candidates[i].name);
        free(candidates[i].qualified_name);
        free(candidates[i].label);
        free(candidates[i].file_path);
    }
    free(candidates);
}

static context_candidate_t *context_candidate_find(context_candidate_t *candidates, int count,
                                                   int64_t id) {
    for (int i = 0; i < count; i++) {
        if (candidates[i].id == id) {
            return &candidates[i];
        }
    }
    return NULL;
}

static void context_candidate_add(context_candidate_t **candidates, int *count, int *capacity,
                                  sqlite3_stmt *stmt, unsigned source, int rank) {
    int64_t id = sqlite3_column_int64(stmt, 0);
    context_candidate_t *candidate = context_candidate_find(*candidates, *count, id);
    if (!candidate) {
        if (*count >= CONTEXT_MAX_CANDIDATES) {
            return;
        }
        if (*count == *capacity) {
            int next_capacity = *capacity == 0 ? 16 : *capacity * 2;
            if (next_capacity > CONTEXT_MAX_CANDIDATES) {
                next_capacity = CONTEXT_MAX_CANDIDATES;
            }
            context_candidate_t *next =
                realloc(*candidates, (size_t)next_capacity * sizeof(**candidates));
            if (!next) {
                return;
            }
            *candidates = next;
            *capacity = next_capacity;
        }
        candidate = &(*candidates)[(*count)++];
        memset(candidate, 0, sizeof(*candidate));
        candidate->id = id;
        candidate->name = heap_strdup(context_sql_text(stmt, 1));
        candidate->qualified_name = heap_strdup(context_sql_text(stmt, 2));
        candidate->label = heap_strdup(context_sql_text(stmt, 3));
        candidate->file_path = heap_strdup(context_sql_text(stmt, 4));
        candidate->start_line = sqlite3_column_int(stmt, 5);
        candidate->end_line = sqlite3_column_int(stmt, 6);
    }

    candidate->sources |= source;
    int *rank_slot = source == CONTEXT_SOURCE_EXACT
                         ? &candidate->exact_rank
                         : (source == CONTEXT_SOURCE_SYMBOL ? &candidate->symbol_rank
                                                             : &candidate->bm25_rank);
    if (*rank_slot == 0 || rank < *rank_slot) {
        *rank_slot = rank;
    }
}

static void context_collect_symbol(sqlite3 *db, const char *project, const char *query,
                                   bool exact_only, int limit,
                                   context_candidate_t **candidates, int *count, int *capacity) {
    const char *sql_exact =
        "SELECT id,name,qualified_name,label,file_path,start_line,end_line FROM nodes "
        "WHERE project=?1 "
        "AND label NOT IN ('File','Folder','Module','Section','Variable','Project') "
        "AND (lower(name)=lower(?2) OR lower(qualified_name)=lower(?2)) "
        "ORDER BY CASE WHEN lower(qualified_name)=lower(?2) THEN 0 ELSE 1 END,"
        "qualified_name,file_path,start_line,end_line,id LIMIT ?3";
    const char *sql_symbol =
        "SELECT id,name,qualified_name,label,file_path,start_line,end_line FROM nodes "
        "WHERE project=?1 "
        "AND label NOT IN ('File','Folder','Module','Section','Variable','Project') "
        "AND (instr(lower(name),lower(?2))>0 OR instr(lower(qualified_name),lower(?2))>0) "
        "ORDER BY CASE WHEN lower(name)=lower(?2) THEN 0 "
        "              WHEN lower(qualified_name)=lower(?2) THEN 1 "
        "              WHEN instr(lower(name),lower(?2))=1 THEN 2 ELSE 3 END,"
        "length(name),qualified_name,file_path,start_line,end_line,id LIMIT ?3";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, exact_only ? sql_exact : sql_symbol, -1, &stmt, NULL) !=
        SQLITE_OK) {
        return;
    }
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, query, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, limit);
    int rank = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        context_candidate_add(candidates, count, capacity, stmt,
                              exact_only ? CONTEXT_SOURCE_EXACT : CONTEXT_SOURCE_SYMBOL, ++rank);
    }
    sqlite3_finalize(stmt);
}

static void context_collect_bm25(sqlite3 *db, const char *project, const char *query, int limit,
                                 context_candidate_t **candidates, int *count, int *capacity) {
    char fts_query[BM25_QUERY_BUF];
    if (bm25_build_match(query, fts_query, sizeof(fts_query)) == 0) {
        return;
    }
    const char *sql =
        "SELECT n.id,n.name,n.qualified_name,n.label,n.file_path,n.start_line,n.end_line "
        "FROM (SELECT rowid,bm25(nodes_fts) AS base_rank FROM nodes_fts "
        "      WHERE nodes_fts MATCH ?1 ORDER BY base_rank,rowid LIMIT ?4) fts "
        "JOIN nodes n ON n.id=fts.rowid "
        "WHERE n.project=?2 "
        "AND n.label NOT IN ('File','Folder','Module','Section','Variable','Project') "
        "ORDER BY (fts.base_rank-CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        " ELSE 0.0 END),n.qualified_name,n.file_path,n.start_line,n.end_line,n.id LIMIT ?3";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return;
    }
    sqlite3_bind_text(stmt, 1, fts_query, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, project, -1, SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, 3, limit);
    sqlite3_bind_int(stmt, 4, BM25_INNER_LIMIT);
    int rank = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        context_candidate_add(candidates, count, capacity, stmt, CONTEXT_SOURCE_BM25, ++rank);
    }
    sqlite3_finalize(stmt);
}

static int context_source_count(unsigned sources) {
    int count = 0;
    for (unsigned bit = 1; bit <= CONTEXT_SOURCE_BM25; bit <<= 1) {
        if (sources & bit) {
            count++;
        }
    }
    return count;
}

static int context_candidate_cmp(const void *left, const void *right) {
    const context_candidate_t *a = left;
    const context_candidate_t *b = right;
    if (a->score > b->score) {
        return -1;
    }
    if (a->score < b->score) {
        return 1;
    }
    int source_delta = context_source_count(b->sources) - context_source_count(a->sources);
    if (source_delta != 0) {
        return source_delta;
    }
    int cmp = strcmp(a->qualified_name ? a->qualified_name : "",
                     b->qualified_name ? b->qualified_name : "");
    if (cmp != 0) {
        return cmp;
    }
    cmp = strcmp(a->file_path ? a->file_path : "", b->file_path ? b->file_path : "");
    if (cmp != 0) {
        return cmp;
    }
    if (a->start_line != b->start_line) {
        return a->start_line < b->start_line ? -1 : 1;
    }
    if (a->end_line != b->end_line) {
        return a->end_line < b->end_line ? -1 : 1;
    }
    return a->id < b->id ? -1 : (a->id > b->id ? 1 : 0);
}

static void context_score_candidates(context_candidate_t *candidates, int count) {
    for (int i = 0; i < count; i++) {
        double score = 0.0;
        if (candidates[i].exact_rank > 0) {
            score += 1.0 / (CONTEXT_RRF_K + candidates[i].exact_rank);
        }
        if (candidates[i].symbol_rank > 0) {
            score += 1.0 / (CONTEXT_RRF_K + candidates[i].symbol_rank);
        }
        if (candidates[i].bm25_rank > 0) {
            score += 1.0 / (CONTEXT_RRF_K + candidates[i].bm25_rank);
        }
        candidates[i].score = score;
    }
    if (count > 1) {
        qsort(candidates, (size_t)count, sizeof(*candidates), context_candidate_cmp);
    }
}

static const char *context_why_matched(const context_candidate_t *candidate) {
    if ((candidate->sources & (CONTEXT_SOURCE_EXACT | CONTEXT_SOURCE_SYMBOL |
                               CONTEXT_SOURCE_BM25)) ==
        (CONTEXT_SOURCE_EXACT | CONTEXT_SOURCE_SYMBOL | CONTEXT_SOURCE_BM25)) {
        return "exact symbol, symbol substring, and BM25 agreement";
    }
    if (context_source_count(candidate->sources) > 1) {
        return "multiple deterministic retrieval methods agreed";
    }
    if (candidate->sources & CONTEXT_SOURCE_EXACT) {
        return "exact symbol match";
    }
    if (candidate->sources & CONTEXT_SOURCE_SYMBOL) {
        return "symbol substring match";
    }
    return "BM25 code-index match";
}

static const char *context_source_name(const context_candidate_t *candidate) {
    if (context_source_count(candidate->sources) > 1) {
        return "reciprocal_rank_fusion";
    }
    if (candidate->sources & CONTEXT_SOURCE_EXACT) {
        return "exact_symbol";
    }
    if (candidate->sources & CONTEXT_SOURCE_SYMBOL) {
        return "symbol_search";
    }
    return "bm25";
}

static double context_confidence(const context_candidate_t *candidate) {
    if (candidate->sources & CONTEXT_SOURCE_EXACT) {
        return context_source_count(candidate->sources) > 1 ? 0.99 : 0.96;
    }
    return context_source_count(candidate->sources) > 1 ? 0.90 : 0.75;
}

static yyjson_mut_val *context_symbol(yyjson_mut_doc *doc,
                                      const context_candidate_t *candidate) {
    yyjson_mut_val *symbol = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, symbol, "name", candidate->name ? candidate->name : "");
    yyjson_mut_obj_add_str(doc, symbol, "qualified_name",
                           candidate->qualified_name ? candidate->qualified_name : "");
    yyjson_mut_obj_add_str(doc, symbol, "label", candidate->label ? candidate->label : "");
    yyjson_mut_obj_add_str(doc, symbol, "file_path",
                           candidate->file_path ? candidate->file_path : "");
    yyjson_mut_obj_add_int(doc, symbol, "start_line", candidate->start_line);
    yyjson_mut_obj_add_int(doc, symbol, "end_line", candidate->end_line);
    return symbol;
}

static bool context_path_seen(const char **paths, int count, const char *path) {
    for (int i = 0; i < count; i++) {
        if (strcmp(paths[i], path) == 0) {
            return true;
        }
    }
    return false;
}

typedef struct {
    int64_t id;
    int depth;
} context_trace_queue_item_t;

static bool context_trace_visited(const context_trace_queue_item_t *queue, int count,
                                  int64_t id) {
    for (int i = 0; i < count; i++) {
        if (queue[i].id == id) {
            return true;
        }
    }
    return false;
}

static void context_add_traces(yyjson_mut_doc *doc, yyjson_mut_val *traces, sqlite3 *db,
                               const char *project, const context_candidate_t *candidates,
                               int candidate_count, const bool *selected, int max_depth,
                               size_t char_budget, size_t *used_chars, bool *truncated) {
    if (max_depth <= 0 || !db) {
        return;
    }
    context_trace_queue_item_t queue[64] = {0};
    int queue_count = 0;
    for (int i = 0; i < candidate_count && queue_count < 8; i++) {
        if (selected[i] && !context_trace_visited(queue, queue_count, candidates[i].id)) {
            queue[queue_count++] =
                (context_trace_queue_item_t){.id = candidates[i].id, .depth = 0};
        }
    }

    const char *sql =
        "SELECT e.id,e.type,e.source_id,e.target_id,s.qualified_name,t.qualified_name "
        "FROM edges e JOIN nodes s ON s.id=e.source_id JOIN nodes t ON t.id=e.target_id "
        "WHERE e.project=?1 AND (e.source_id=?2 OR e.target_id=?2) "
        "ORDER BY e.type,s.qualified_name,t.qualified_name,e.source_id,e.target_id,e.id";
    int cursor = 0;
    while (cursor < queue_count) {
        context_trace_queue_item_t current = queue[cursor++];
        if (current.depth >= max_depth) {
            continue;
        }
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
            return;
        }
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, current.id);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *edge_type = context_sql_text(stmt, 1);
            if (!detect_dependency_edge(edge_type)) {
                continue;
            }
            int64_t source_id = sqlite3_column_int64(stmt, 2);
            int64_t target_id = sqlite3_column_int64(stmt, 3);
            const char *source_qn = context_sql_text(stmt, 4);
            const char *target_qn = context_sql_text(stmt, 5);
            int64_t neighbor_id = source_id == current.id ? target_id : source_id;

            yyjson_mut_val *trace = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, trace, "from", source_qn);
            yyjson_mut_obj_add_strcpy(doc, trace, "to", target_qn);
            yyjson_mut_obj_add_strcpy(doc, trace, "edge_type", edge_type);
            yyjson_mut_obj_add_str(
                doc, trace, "direction",
                source_id == current.id ? "outbound" : "inbound");
            yyjson_mut_obj_add_int(doc, trace, "depth", current.depth + 1);
            yyjson_mut_obj_add_str(doc, trace, "source", "graph_edge");
            yyjson_mut_obj_add_str(doc, trace, "origin", "graph_index");
            yyjson_mut_obj_add_real(doc, trace, "confidence", 0.80);
            size_t trace_len = 0;
            char *trace_json = yyjson_mut_val_write(trace, 0, &trace_len);
            free(trace_json);
            if (*used_chars + trace_len + 160 <= char_budget) {
                yyjson_mut_arr_add_val(traces, trace);
                *used_chars += trace_len + 1;
            } else {
                *truncated = true;
            }

            if (queue_count < (int)(sizeof(queue) / sizeof(queue[0])) &&
                !context_trace_visited(queue, queue_count, neighbor_id)) {
                queue[queue_count++] = (context_trace_queue_item_t){
                    .id = neighbor_id, .depth = current.depth + 1};
            }
        }
        sqlite3_finalize(stmt);
    }
}

static char *handle_get_context(cbm_mcp_server_t *srv, const char *args) {
    if (!context_args_well_typed(args)) {
        return cbm_mcp_text_result(
            "{\"code\":\"invalid_arguments\",\"message\":\"get_context argument types do not "
            "match its input schema\"}",
            true);
    }
    char *query = cbm_mcp_get_string_arg(args, "query");
    if (!query || query[0] == '\0') {
        free(query);
        return cbm_mcp_text_result(
            "{\"code\":\"invalid_arguments\",\"message\":\"query is required\"}", true);
    }

    char *project = cbm_mcp_get_string_arg(args, "project");

    char *intent = cbm_mcp_get_string_arg(args, "intent");
    if (!intent) {
        intent = heap_strdup("understand");
    }
    if (!intent || !context_intent_valid(intent)) {
        free(query);
        free(project);
        free(intent);
        return cbm_mcp_text_result(
            "{\"code\":\"invalid_arguments\","
            "\"message\":\"intent must be understand, debug, change, review, or security\"}",
            true);
    }

    int budget_tokens = cbm_mcp_get_int_arg(args, "budget_tokens", 3000);
    int depth = cbm_mcp_get_int_arg(args, "depth", 2);
    bool include_tests = cbm_mcp_get_bool_arg(args, "include_tests");
    if (budget_tokens < 500 || budget_tokens > 12000 || depth < 0 || depth > 4) {
        free(query);
        free(project);
        free(intent);
        return cbm_mcp_text_result(
            "{\"code\":\"invalid_arguments\","
            "\"message\":\"budget_tokens must be 500..12000 and depth must be 0..4\"}",
            true);
    }

    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        free(query);
        free(project);
        free(intent);
        return cbm_mcp_text_result(
            "{\"code\":\"project_not_found\",\"message\":\"project not found or not indexed\"}",
            true);
    }
    const char *effective_project = project ? project : srv->current_project;
    if (!effective_project || effective_project[0] == '\0') {
        free(query);
        free(project);
        free(intent);
        return cbm_mcp_text_result(
            "{\"code\":\"project_required\",\"message\":\"project is required\"}", true);
    }
    char *not_indexed = verify_project_indexed(store, effective_project);
    if (not_indexed) {
        free(query);
        free(project);
        free(intent);
        return not_indexed;
    }

    int retrieval_limit = budget_tokens / 100;
    if (retrieval_limit < 8) {
        retrieval_limit = 8;
    } else if (retrieval_limit > 50) {
        retrieval_limit = 50;
    }
    context_candidate_t *candidates = NULL;
    int candidate_count = 0;
    int candidate_capacity = 0;
    sqlite3 *db = cbm_store_get_db(store);
    context_collect_symbol(db, effective_project, query, true, retrieval_limit, &candidates,
                           &candidate_count, &candidate_capacity);
    context_collect_symbol(db, effective_project, query, false, retrieval_limit, &candidates,
                           &candidate_count, &candidate_capacity);
    context_collect_bm25(db, effective_project, query, retrieval_limit, &candidates,
                         &candidate_count, &candidate_capacity);

    /* Focus symbols are additional exact/symbol seeds, never prompt text and
     * never inputs to a generated summary. */
    yyjson_doc *input_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *focus =
        input_doc ? yyjson_obj_get(yyjson_doc_get_root(input_doc), "focus_symbols") : NULL;
    if (focus && yyjson_is_arr(focus)) {
        size_t focus_idx, focus_max;
        yyjson_val *focus_value;
        yyjson_arr_foreach(focus, focus_idx, focus_max, focus_value) {
            if (focus_idx >= 16 || !yyjson_is_str(focus_value)) {
                continue;
            }
            const char *focus_query = yyjson_get_str(focus_value);
            context_collect_symbol(db, effective_project, focus_query, true, retrieval_limit,
                                   &candidates, &candidate_count, &candidate_capacity);
            context_collect_symbol(db, effective_project, focus_query, false, retrieval_limit,
                                   &candidates, &candidate_count, &candidate_capacity);
        }
    }
    context_score_candidates(candidates, candidate_count);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "project", effective_project);
    yyjson_mut_obj_add_str(doc, root, "query", query);
    yyjson_mut_obj_add_str(doc, root, "intent", intent);
    yyjson_mut_obj_add_int(doc, root, "budget_tokens", budget_tokens);
    yyjson_mut_obj_add_int(doc, root, "depth", depth);
    yyjson_mut_obj_add_bool(doc, root, "include_tests", include_tests);

    yyjson_mut_val *freshness = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, freshness, "status", "index_available");
    yyjson_mut_obj_add_bool(doc, freshness, "worktree_verified", false);
    cbm_project_t project_meta = {0};
    if (cbm_store_get_project(store, effective_project, &project_meta) == CBM_STORE_OK) {
        yyjson_mut_obj_add_int(doc, freshness, "generation", project_meta.generation);
        if (project_meta.commit_hash && project_meta.commit_hash[0]) {
            yyjson_mut_obj_add_str(doc, freshness, "commit", project_meta.commit_hash);
        }
        if (project_meta.dirty_fingerprint && project_meta.dirty_fingerprint[0]) {
            yyjson_mut_obj_add_str(doc, freshness, "dirty_fingerprint",
                                   project_meta.dirty_fingerprint);
        }
        if (project_meta.structural_indexed_at && project_meta.structural_indexed_at[0]) {
            yyjson_mut_obj_add_str(doc, freshness, "structural_indexed_at",
                                   project_meta.structural_indexed_at);
        }
        if (project_meta.derived_indexed_at && project_meta.derived_indexed_at[0]) {
            yyjson_mut_obj_add_str(doc, freshness, "derived_indexed_at",
                                   project_meta.derived_indexed_at);
        }
        if (project_meta.structural_digest && project_meta.structural_digest[0]) {
            yyjson_mut_obj_add_str(doc, freshness, "structural_digest",
                                   project_meta.structural_digest);
        }
        if (project_meta.parser_version && project_meta.parser_version[0]) {
            yyjson_mut_obj_add_str(doc, freshness, "parser_version",
                                   project_meta.parser_version);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "freshness", freshness);

    yyjson_mut_val *seed_symbols = yyjson_mut_arr(doc);
    yyjson_mut_val *ranked_evidence = yyjson_mut_arr(doc);
    yyjson_mut_val *snippets = yyjson_mut_arr(doc);
    yyjson_mut_val *important_paths = yyjson_mut_arr(doc);
    yyjson_mut_val *traces = yyjson_mut_arr(doc);
    yyjson_mut_val *change_risks = yyjson_mut_arr(doc);
    size_t used_chars = 700;
    size_t char_budget = (size_t)budget_tokens * 4;
    bool truncated = false;
    const char *seen_paths[CONTEXT_MAX_CANDIDATES] = {0};
    int seen_path_count = 0;
    bool selected[CONTEXT_MAX_CANDIDATES] = {false};
    int selected_count = 0;
    char *root_path = get_project_root(srv, effective_project);

    /* First pass favors one result per file. The second pass fills remaining
     * budget in fused rank order. Both passes have total tie-breakers. */
    for (int pass = 0; pass < 2; pass++) {
        for (int i = 0; i < candidate_count; i++) {
            context_candidate_t *candidate = &candidates[i];
            if (selected[i] || (!include_tests && is_test_file(candidate->file_path))) {
                continue;
            }
            bool path_seen =
                context_path_seen(seen_paths, seen_path_count, candidate->file_path);
            if ((pass == 0 && path_seen) || (pass == 1 && !path_seen)) {
                continue;
            }

            int start = candidate->start_line > 0 ? candidate->start_line : 1;
            int end = candidate->end_line >= start ? candidate->end_line : start;
            if (end - start > 40) {
                end = start + 40;
            }
            char *abs_path = NULL;
            char *source =
                resolve_snippet_source(root_path, candidate->file_path, start, end, &abs_path);
            free(abs_path);
            if (!source) {
                source = heap_strdup("(source not available)");
            }
            size_t max_source_chars = char_budget / 3;
            if (max_source_chars > 1200) {
                max_source_chars = 1200;
            }
            if (source && strlen(source) > max_source_chars) {
                source[max_source_chars] = '\0';
            }

            yyjson_mut_val *seed = context_symbol(doc, candidate);
            yyjson_mut_val *evidence = context_symbol(doc, candidate);
            yyjson_mut_obj_add_str(doc, evidence, "why_matched",
                                   context_why_matched(candidate));
            yyjson_mut_obj_add_str(doc, evidence, "source", context_source_name(candidate));
            yyjson_mut_obj_add_str(doc, evidence, "origin", "graph_index");
            yyjson_mut_obj_add_real(doc, evidence, "confidence",
                                    context_confidence(candidate));
            yyjson_mut_obj_add_real(doc, evidence, "rrf_score", candidate->score);

            yyjson_mut_val *snippet = context_symbol(doc, candidate);
            yyjson_mut_obj_add_str(doc, snippet, "why_matched",
                                   context_why_matched(candidate));
            yyjson_mut_obj_add_str(doc, snippet, "source", "source_file");
            yyjson_mut_obj_add_str(doc, snippet, "origin", "worktree");
            yyjson_mut_obj_add_real(doc, snippet, "confidence",
                                    source && strcmp(source, "(source not available)") != 0
                                        ? context_confidence(candidate)
                                        : 0.25);
            yyjson_mut_obj_add_strcpy(doc, snippet, "text", source ? source : "");

            size_t seed_len = 0, evidence_len = 0, snippet_len = 0;
            char *seed_json = yyjson_mut_val_write(seed, 0, &seed_len);
            char *evidence_json = yyjson_mut_val_write(evidence, 0, &evidence_len);
            char *snippet_json = yyjson_mut_val_write(snippet, 0, &snippet_len);
            free(seed_json);
            free(evidence_json);
            free(snippet_json);
            free(source);
            size_t projected = used_chars + seed_len + evidence_len + snippet_len + 12;
            if (projected + 160 > char_budget) {
                truncated = true;
                continue;
            }

            yyjson_mut_arr_add_val(seed_symbols, seed);
            yyjson_mut_arr_add_val(ranked_evidence, evidence);
            yyjson_mut_arr_add_val(snippets, snippet);
            used_chars = projected;
            selected[i] = true;
            selected_count++;
            if (!path_seen && seen_path_count < CONTEXT_MAX_CANDIDATES) {
                seen_paths[seen_path_count++] = candidate->file_path;
                yyjson_mut_arr_add_str(doc, important_paths, candidate->file_path);
            }

            int in_degree = 0;
            int out_degree = 0;
            cbm_store_node_degree(store, candidate->id, &in_degree, &out_degree);
            if ((strcmp(intent, "change") == 0 || strcmp(intent, "review") == 0 ||
                 strcmp(intent, "security") == 0) &&
                in_degree > 0) {
                yyjson_mut_val *risk = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_str(doc, risk, "symbol",
                                       candidate->qualified_name ? candidate->qualified_name : "");
                yyjson_mut_obj_add_str(doc, risk, "risk",
                                       in_degree >= 5 ? "high" : "medium");
                yyjson_mut_obj_add_int(doc, risk, "inbound_dependencies", in_degree);
                yyjson_mut_obj_add_str(
                    doc, risk, "reason",
                    "Changing this symbol can affect indexed inbound dependencies");
                yyjson_mut_arr_add_val(change_risks, risk);
            }
        }
    }
    if (selected_count < candidate_count) {
        truncated = true;
    }

    context_add_traces(doc, traces, db, effective_project, candidates, candidate_count, selected,
                       depth, char_budget, &used_chars, &truncated);

    yyjson_mut_obj_add_val(doc, root, "seed_symbols", seed_symbols);
    yyjson_mut_obj_add_val(doc, root, "ranked_evidence", ranked_evidence);
    yyjson_mut_obj_add_val(doc, root, "important_paths", important_paths);
    yyjson_mut_obj_add_val(doc, root, "snippets", snippets);
    yyjson_mut_obj_add_val(doc, root, "traces", traces);
    yyjson_mut_obj_add_val(doc, root, "change_risks", change_risks);

    yyjson_mut_val *provenance = yyjson_mut_arr(doc);
    yyjson_mut_val *source = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, source, "source", "deterministic_retrieval_pipeline");
    yyjson_mut_obj_add_str(doc, source, "origin", "graph_index");
    yyjson_mut_obj_add_real(doc, source, "confidence", 0.85);
    yyjson_mut_obj_add_bool(doc, source, "worktree_verified", false);
    yyjson_mut_obj_add_bool(doc, source, "llm_generated", false);
    yyjson_mut_val *methods = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_str(doc, methods, "exact_symbol");
    yyjson_mut_arr_add_str(doc, methods, "symbol_search");
    yyjson_mut_arr_add_str(doc, methods, "bm25");
    yyjson_mut_arr_add_str(doc, methods, "reciprocal_rank_fusion");
    yyjson_mut_obj_add_val(doc, source, "methods", methods);
    yyjson_mut_arr_add_val(provenance, source);
    yyjson_mut_obj_add_val(doc, root, "provenance", provenance);

    int estimated_tokens = (int)((used_chars + 3) / 4);
    if (estimated_tokens > budget_tokens) {
        estimated_tokens = budget_tokens;
    }
    yyjson_mut_obj_add_int(doc, root, "estimated_tokens", estimated_tokens);
    yyjson_mut_obj_add_bool(doc, root, "truncated", truncated);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_project_free_fields(&project_meta);
    yyjson_doc_free(input_doc);
    context_candidates_free(candidates, candidate_count);
    free(root_path);
    free(query);
    free(project);
    free(intent);
    if (!json) {
        return cbm_mcp_text_result("out of memory", true);
    }
    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── ingest_traces ────────────────────────────────────────────── */

static char *handle_ingest_traces(cbm_mcp_server_t *srv, const char *args) {
    (void)srv;
    (void)args;
    return cbm_mcp_text_result(
        "{\"code\":\"unsupported\",\"tool\":\"ingest_traces\","
        "\"message\":\"Runtime trace ingestion is not implemented\"}",
        true);
}

/* ── Tool dispatch ────────────────────────────────────────────── */

static bool tool_uses_session_project(const char *tool_name) {
    return strcmp(tool_name, "get_context") == 0 || strcmp(tool_name, "search_graph") == 0 ||
           strcmp(tool_name, "search_code") == 0 || strcmp(tool_name, "trace_path") == 0 ||
           strcmp(tool_name, "trace_call_path") == 0 ||
           strcmp(tool_name, "get_code_snippet") == 0 ||
           strcmp(tool_name, "get_architecture") == 0 ||
           strcmp(tool_name, "detect_changes") == 0 || strcmp(tool_name, "index_status") == 0 ||
           strcmp(tool_name, "query_graph") == 0 || strcmp(tool_name, "get_graph_schema") == 0;
}

/* Add the active session project only to non-destructive read tools. An
 * explicit project key always wins, including an invalid/empty one, so callers
 * never accidentally operate on a different project than requested. */
static char *args_with_session_project(cbm_mcp_server_t *srv, const char *tool_name,
                                       const char *args_json) {
    const char *args = args_json ? args_json : "{}";
    if (!srv || !tool_uses_session_project(tool_name)) {
        return heap_strdup(args);
    }

    yyjson_doc *source_doc = yyjson_read(args, strlen(args), 0);
    if (!source_doc) {
        return heap_strdup(args);
    }
    yyjson_val *source_root = yyjson_doc_get_root(source_doc);
    if (!yyjson_is_obj(source_root) || yyjson_obj_get(source_root, "project")) {
        yyjson_doc_free(source_doc);
        return heap_strdup(args);
    }

    const char *project = srv->session_project[0] ? srv->session_project : srv->current_project;
    if (!project || project[0] == '\0') {
        yyjson_doc_free(source_doc);
        return heap_strdup(args);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(doc, source_root);
    yyjson_doc_free(source_doc);
    if (!root) {
        yyjson_mut_doc_free(doc);
        return heap_strdup(args);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "project", project);
    char *resolved = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return resolved ? resolved : heap_strdup(args);
}

static char *toolset_disabled_result(const char *tool_name, unsigned required) {
    const char *required_name =
        required == CBM_MCP_TOOLSET_ADVANCED ? "advanced" : "admin";
    char message[CBM_SZ_512];
    snprintf(message, sizeof(message),
             "{\"code\":\"toolset_disabled\",\"tool\":\"%s\","
             "\"required_toolset\":\"%s\","
             "\"message\":\"Enable the %s MCP toolset explicitly\"}",
             tool_name, required_name, required_name);
    return cbm_mcp_text_result(message, true);
}

char *cbm_mcp_handle_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    if (!tool_name) {
        return cbm_mcp_text_result("missing tool name", true);
    }

    if (strcmp(tool_name, "ingest_traces") == 0) {
        return handle_ingest_traces(srv, args_json);
    }

    unsigned required_toolset = toolset_for_name(tool_name);
    if (strcmp(tool_name, "trace_call_path") == 0) {
        required_toolset = CBM_MCP_TOOLSET_CORE; /* deprecated alias */
    }
    unsigned enabled_toolsets = srv ? srv->toolsets : CBM_MCP_TOOLSET_CORE;
    if (required_toolset != 0 && (enabled_toolsets & required_toolset) == 0) {
        return toolset_disabled_result(tool_name, required_toolset);
    }

    char *effective_args = args_with_session_project(srv, tool_name, args_json);
    if (!effective_args) {
        return cbm_mcp_text_result("out of memory", true);
    }

#define RETURN_TOOL(handler)                  \
    do {                                      \
        char *_result = handler(srv, effective_args); \
        free(effective_args);                 \
        return _result;                       \
    } while (0)

    if (strcmp(tool_name, "list_projects") == 0) {
        RETURN_TOOL(handle_list_projects);
    }
    if (strcmp(tool_name, "get_graph_schema") == 0) {
        RETURN_TOOL(handle_get_graph_schema);
    }
    if (strcmp(tool_name, "get_context") == 0) {
        RETURN_TOOL(handle_get_context);
    }
    if (strcmp(tool_name, "search_graph") == 0) {
        RETURN_TOOL(handle_search_graph);
    }
    if (strcmp(tool_name, "query_graph") == 0) {
        RETURN_TOOL(handle_query_graph);
    }
    if (strcmp(tool_name, "index_status") == 0) {
        RETURN_TOOL(handle_index_status);
    }
    if (strcmp(tool_name, "delete_project") == 0) {
        RETURN_TOOL(handle_delete_project);
    }
    if (strcmp(tool_name, "trace_path") == 0 || strcmp(tool_name, "trace_call_path") == 0) {
        RETURN_TOOL(handle_trace_call_path);
    }
    if (strcmp(tool_name, "get_architecture") == 0) {
        RETURN_TOOL(handle_get_architecture);
    }

    /* Pipeline-dependent tools */
    if (strcmp(tool_name, "index_repository") == 0) {
        RETURN_TOOL(handle_index_repository);
    }
    if (strcmp(tool_name, "get_code_snippet") == 0) {
        RETURN_TOOL(handle_get_code_snippet);
    }
    if (strcmp(tool_name, "search_code") == 0) {
        RETURN_TOOL(handle_search_code);
    }
    if (strcmp(tool_name, "detect_changes") == 0) {
        RETURN_TOOL(handle_detect_changes);
    }
    if (strcmp(tool_name, "manage_adr") == 0) {
        RETURN_TOOL(handle_manage_adr);
    }
    char msg[CBM_SZ_256];
    snprintf(msg, sizeof(msg), "unknown tool: %s", tool_name);
    free(effective_args);
#undef RETURN_TOOL
    return cbm_mcp_text_result(msg, true);
}

/* ── Session detection + auto-index ────────────────────────────── */

/* Detect session root from CWD (fallback: single indexed project from DB). */
static void detect_session(cbm_mcp_server_t *srv) {
    if (srv->session_detected) {
        return;
    }
    srv->session_detected = true;

    /* 1. Try CWD */
    char cwd[CBM_SZ_1K];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        const char *home = cbm_get_home_dir();
        /* Skip useless roots: / and $HOME */
        if (strcmp(cwd, "/") != 0 && (home == NULL || strcmp(cwd, home) != 0)) {
            snprintf(srv->session_root, sizeof(srv->session_root), "%s", cwd);
            cbm_log_info("session.root.cwd", "path", cwd);
        }
    }

    /* Derive project name from path — must match cbm_project_name_from_path
     * used by the pipeline, otherwise session queries look for a .db file
     * that doesn't match the indexed project name. */
    if (srv->session_root[0]) {
        char *pname = cbm_project_name_from_path(srv->session_root);
        if (pname) {
            snprintf(srv->session_project, sizeof(srv->session_project), "%s", pname);
            free(pname);
        }
    }
}

/* Background auto-index thread function */
static void *autoindex_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;

    cbm_log_info("autoindex.start", "project", srv->session_project, "path", srv->session_root);

    cbm_pipeline_t *p = cbm_pipeline_new(srv->session_root, NULL, CBM_MODE_FULL);
    if (!p) {
        cbm_log_warn("autoindex.err", "msg", "pipeline_create_failed");
        return NULL;
    }

    /* Block until any concurrent pipeline finishes */
    cbm_pipeline_lock_project(srv->session_project);
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_unlock_project(srv->session_project);

    cbm_pipeline_free(p);
    cbm_mem_collect(); /* return mimalloc pages to OS after indexing */

    if (rc == 0) {
        cbm_log_info("autoindex.done", "project", srv->session_project);
        /* Register with watcher for ongoing change detection */
        if (srv->watcher) {
            cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
        }
    } else {
        cbm_log_warn("autoindex.err", "msg", "pipeline_run_failed");
    }
    return NULL;
}

/* Start auto-indexing if configured and project not yet indexed. */
static void maybe_auto_index(cbm_mcp_server_t *srv) {
    if (srv->session_root[0] == '\0') {
        return; /* no session root detected */
    }

    /* Check if project already has a DB */
    const char *home = cbm_get_home_dir();
    if (home) {
        char db_check[CBM_SZ_1K];
        snprintf(db_check, sizeof(db_check), "%s/%s.db", cbm_resolve_cache_dir(),
                 srv->session_project);
        struct stat st;
        if (stat(db_check, &st) == 0) {
            /* Already indexed → register watcher for change detection */
            cbm_log_info("autoindex.skip", "reason", "already_indexed", "project",
                         srv->session_project);
            if (srv->watcher) {
                cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
            }
            return;
        }
    }

/* Default file limit for auto-indexing new projects */
#define DEFAULT_AUTO_INDEX_LIMIT 50000

    /* Check auto_index config */
    bool auto_index = false;
    int file_limit = DEFAULT_AUTO_INDEX_LIMIT;
    if (srv->config) {
        auto_index = cbm_config_get_bool(srv->config, CBM_CONFIG_AUTO_INDEX, false);
        file_limit =
            cbm_config_get_int(srv->config, CBM_CONFIG_AUTO_INDEX_LIMIT, DEFAULT_AUTO_INDEX_LIMIT);
    }

    if (!auto_index) {
        cbm_log_info("autoindex.skip", "reason", "disabled", "hint",
                     "run: codebase-memory-mcp config set auto_index true");
        return;
    }

    /* Quick file count check to avoid OOM on massive repos */
    if (!cbm_validate_shell_arg(srv->session_root)) {
        cbm_log_warn("autoindex.skip", "reason", "path contains shell metacharacters");
        return;
    }
    char cmd[CBM_SZ_1K];
    snprintf(cmd, sizeof(cmd), "git -C '%s' ls-files 2>/dev/null | wc -l", srv->session_root);
    FILE *fp = cbm_popen(cmd, "r");
    if (fp) {
        char line[CBM_SZ_64];
        if (fgets(line, sizeof(line), fp)) {
            int count = (int)strtol(line, NULL, CBM_DECIMAL_BASE);
            if (count > file_limit) {
                cbm_log_warn("autoindex.skip", "reason", "too_many_files", "files", line, "limit",
                             CBM_CONFIG_AUTO_INDEX_LIMIT);
                cbm_pclose(fp);
                return;
            }
        }
        cbm_pclose(fp);
    }

    /* Launch auto-index in background */
    if (cbm_thread_create(&srv->autoindex_tid, 0, autoindex_thread, srv) == 0) {
        srv->autoindex_active = true;
    }
}

/* ── Server request handler ───────────────────────────────────── */

char *cbm_mcp_server_handle(cbm_mcp_server_t *srv, const char *line) {
    cbm_jsonrpc_request_t req = {0};
    if (cbm_jsonrpc_parse(line, &req) < 0) {
        return cbm_jsonrpc_format_error(0, JSONRPC_PARSE_ERROR, "Parse error");
    }

    /* Notifications (no id) → handle cancellation, then no response */
    if (!req.has_id) {
        if (req.method && strcmp(req.method, "notifications/cancelled") == 0) {
            /* MCP cancellation: cancel the active pipeline if request ID matches */
            if (srv->active_pipeline) {
                cbm_pipeline_cancel(srv->active_pipeline);
                cbm_log_info("mcp.cancelled", "request_id_active",
                             srv->active_request_id > 0 ? "yes" : "none");
            }
        }
        cbm_jsonrpc_request_free(&req);
        return NULL;
    }

    char *result_json = NULL;

    if (strcmp(req.method, "initialize") == 0) {
        result_json = cbm_mcp_initialize_response(req.params_raw);
        detect_session(srv);
        maybe_auto_index(srv);
    } else if (strcmp(req.method, "ping") == 0) {
        result_json = heap_strdup("{}");
    } else if (strcmp(req.method, "tools/list") == 0) {
        result_json = cbm_mcp_tools_list_for_toolsets(srv->toolsets);
    } else if (strcmp(req.method, "tools/call") == 0) {
        char *tool_name = req.params_raw ? cbm_mcp_get_tool_name(req.params_raw) : NULL;
        char *tool_args =
            req.params_raw ? cbm_mcp_get_arguments(req.params_raw) : heap_strdup("{}");

        struct timespec t0;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
        result_json = cbm_mcp_handle_tool(srv, tool_name, tool_args);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - t0.tv_nsec) / MCP_MS_TO_US);
        bool is_err = (result_json != NULL) && (strstr(result_json, "\"isError\":true") != NULL);
        cbm_diag_record_query(dur_us, is_err);

        free(tool_name);
        free(tool_args);
    } else {
        /* Echo the original id (string or numeric, issue #253) on the error. */
        char err_obj[160];
        snprintf(err_obj, sizeof(err_obj), "{\"code\":%d,\"message\":\"Method not found\"}",
                 JSONRPC_METHOD_NOT_FOUND);
        cbm_jsonrpc_response_t err_resp = {
            .id = req.id,
            .id_str = req.id_str,
            .error_json = err_obj,
        };
        char *err = cbm_jsonrpc_format_response(&err_resp);
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    cbm_jsonrpc_response_t resp = {
        .id = req.id,
        .id_str = req.id_str,
        .result_json = result_json,
    };
    char *out = cbm_jsonrpc_format_response(&resp);
    free(result_json);
    cbm_jsonrpc_request_free(&req);
    return out;
}

/* Handle a Content-Length-framed message (LSP-style transport).
 * Reads headers, body, processes request, writes framed response. */
static void handle_content_length_frame(cbm_mcp_server_t *srv, FILE *in, FILE *out, char **line,
                                        size_t *cap, int content_len) {
    /* Skip blank line(s) between header and body */
    while (cbm_getline(line, cap, in) > 0) {
        size_t hlen = strlen(*line);
        while (hlen > 0 && ((*line)[hlen - SKIP_ONE] == '\n' || (*line)[hlen - SKIP_ONE] == '\r')) {
            (*line)[--hlen] = '\0';
        }
        if (hlen == 0) {
            break;
        }
    }

    char *body = malloc((size_t)content_len + SKIP_ONE);
    if (!body) {
        return;
    }
    size_t nread = fread(body, SKIP_ONE, (size_t)content_len, in);
    body[nread] = '\0';

    char *resp = cbm_mcp_server_handle(srv, body);
    free(body);

    if (resp) {
        size_t rlen = strlen(resp);
        (void)fprintf(out, "Content-Length: %zu\r\n\r\n%s", rlen, resp);
        (void)fflush(out);
        free(resp);
    }
}

#ifndef _WIN32
/* Unix 3-phase poll: non-blocking fd check, FILE* buffer peek, blocking poll.
 * Returns: 1 = data ready, 0 = timeout (evicted idle stores), -1 = error/EOF. */
static int poll_for_input_unix(cbm_mcp_server_t *srv, int fd, FILE *in) {
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    int pr = poll(&pfd, SKIP_ONE, 0); /* Phase 1: non-blocking */

    if (pr < 0) {
        return CBM_NOT_FOUND;
    }
    if (pr > 0) {
        return SKIP_ONE;
    }

    /* Phase 2: peek FILE* buffer */
    int saved_flags = fcntl(fd, F_GETFL);
    if (saved_flags < 0) {
        /* fcntl failed — fall through to blocking poll */
        pr = poll(&pfd, SKIP_ONE, STORE_IDLE_TIMEOUT_S * MCP_TIMEOUT_MS);
        if (pr < 0) {
            return CBM_NOT_FOUND;
        }
        if (pr == 0) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            return 0;
        }
        return SKIP_ONE;
    }

    (void)fcntl(fd, F_SETFL, saved_flags | O_NONBLOCK);
    int c = fgetc(in);
    (void)fcntl(fd, F_SETFL, saved_flags);

    if (c == EOF) {
        if (feof(in)) {
            return CBM_NOT_FOUND; /* true EOF */
        }
        clearerr(in);
        /* Phase 3: blocking poll */
        pr = poll(&pfd, SKIP_ONE, STORE_IDLE_TIMEOUT_S * MCP_TIMEOUT_MS);
        if (pr < 0) {
            return CBM_NOT_FOUND;
        }
        if (pr == 0) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            return 0;
        }
        return SKIP_ONE;
    }

    (void)ungetc(c, in);
    return SKIP_ONE;
}
#endif

/* ── Event loop ───────────────────────────────────────────────── */

int cbm_mcp_server_run(cbm_mcp_server_t *srv, FILE *in, FILE *out) {
    char *line = NULL;
    size_t cap = 0;
    int fd = cbm_fileno(in);

    for (;;) {
        /* Poll with idle timeout so we can evict unused stores between requests.
         *
         * IMPORTANT: poll() operates on the raw fd, but getline() reads from a
         * buffered FILE*. When a client sends multiple messages in rapid
         * succession, the first getline() call may drain ALL kernel data into
         * libc's internal FILE* buffer. Subsequent poll() calls then see an
         * empty kernel fd and block for STORE_IDLE_TIMEOUT_S seconds even
         * though the next messages are already in the FILE* buffer.
         *
         * Fix (Unix): use a three-phase approach —
         *   Phase 1: non-blocking poll (timeout=0) to check the kernel fd.
         *   Phase 2: if Phase 1 returns 0, peek the FILE* buffer via fgetc/
         *            ungetc to detect data buffered by a prior getline() call.
         *            The fd is temporarily set O_NONBLOCK so fgetc() returns
         *            immediately (EAGAIN → EOF + ferror) instead of blocking
         *            when the FILE* buffer is empty, which would otherwise
         *            bypass the Phase 3 idle eviction timeout.
         *   Phase 3: only if both phases confirm no data, do blocking poll. */
#ifdef _WIN32
        /* Windows: WaitForSingleObject on stdin handle */
        HANDLE hStdin = (HANDLE)_get_osfhandle(fd);
        DWORD wr = WaitForSingleObject(hStdin, STORE_IDLE_TIMEOUT_S * MCP_TIMEOUT_MS);
        if (wr == WAIT_FAILED) {
            break;
        }
        if (wr == WAIT_TIMEOUT) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            continue;
        }
#else
        int pr = poll_for_input_unix(srv, fd, in);
        if (pr < 0) {
            break;
        }
        if (pr == 0) {
            continue; /* timeout — idle stores evicted */
        }
#endif

        if (cbm_getline(&line, &cap, in) <= 0) {
            break;
        }

        /* Trim trailing newline/CR */
        size_t len = strlen(line);
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        /* Content-Length framing (LSP-style transport) */
        if (strncmp(line, "Content-Length:", SLEN("Content-Length:")) == 0) {
            int content_len = (int)strtol(line + MCP_CONTENT_PREFIX, NULL, CBM_DECIMAL_BASE);
            if (content_len > 0 && content_len <= MCP_DEFAULT_LIMIT * CBM_SZ_1K * CBM_SZ_1K) {
                handle_content_length_frame(srv, in, out, &line, &cap, content_len);
            }
            continue;
        }

        char *resp = cbm_mcp_server_handle(srv, line);
        if (resp) {
            (void)fprintf(out, "%s\n", resp);
            (void)fflush(out);
            free(resp);
        }
    }

    free(line);
    return 0;
}

/* ── cbm_parse_file_uri ──────────────────────────────────────── */

bool cbm_parse_file_uri(const char *uri, char *out_path, int out_size) {
    if (!uri || !out_path || out_size <= 0) {
        if (out_path && out_size > 0) {
            out_path[0] = '\0';
        }
        return false;
    }

    /* Must start with file:// */
    if (strncmp(uri, "file://", SLEN("file://")) != 0) {
        out_path[0] = '\0';
        return false;
    }

    const char *path = uri + MCP_URI_PREFIX;

    /* On Windows, file:///C:/path → /C:/path. Strip leading / before drive letter. */
    if (path[0] == '/' && path[SKIP_ONE] &&
        ((path[SKIP_ONE] >= 'A' && path[SKIP_ONE] <= 'Z') ||
         (path[SKIP_ONE] >= 'a' && path[SKIP_ONE] <= 'z')) &&
        path[PAIR_LEN] == ':') {
        path++; /* skip the leading / */
    }

    snprintf(out_path, out_size, "%s", path);
    return true;
}
