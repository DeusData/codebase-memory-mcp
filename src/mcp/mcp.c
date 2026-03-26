/*
 * mcp.c — MCP server: JSON-RPC 2.0 over stdio with 14 graph tools.
 *
 * Uses yyjson for fast JSON parsing/building.
 * Single-threaded event loop: read line → parse → dispatch → respond.
 */

// operations

#include "mcp/mcp.h"
#include "store/store.h"
#include "cypher/cypher.h"
#include "pipeline/pipeline.h"
#include "depindex/depindex.h"
#include "pagerank/pagerank.h"
#include "cli/cli.h"
#include "watcher/watcher.h"
#include "foundation/mem.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/compat_regex.h"
#include <sqlite3.h>

#ifdef _WIN32
#include <process.h> /* _getpid */
#else
#include <unistd.h>
#include <sys/unistd.h>
#include <sys/poll.h>
#include <poll.h>
#endif
#include <yyjson/yyjson.h>
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* ── Constants ────────────────────────────────────────────────── */

/* Add a "pagerank" key with value formatted to 4 significant figures.
 * Writes directly as a raw JSON number (e.g. 4.755e-05) — no double round-trip.
 * 4 sig figs preserves ranking distinguishability while saving ~12 chars/value.
 * This is the single place pagerank values are serialized to JSON. */
static void add_pagerank_val(yyjson_mut_doc *doc, yyjson_mut_val *obj, double v) {
    char buf[32];
    snprintf(buf, sizeof(buf), "%.4g", v);
    yyjson_mut_obj_add_val(doc, obj, "pagerank", yyjson_mut_rawcpy(doc, buf));
}

/* Default snippet fallback line count (when end_line unknown) */
#define SNIPPET_DEFAULT_LINES 50

/* Default result limit for search_graph and search_code.
 * Prevents unbounded 500K-result responses. Callers can override.
 * Configurable via config key "search_limit". */
#define CBM_DEFAULT_SEARCH_LIMIT 50
#define CBM_CONFIG_SEARCH_LIMIT "search_limit"

/* Default max source lines returned by get_code_snippet.
 * Set to 0 for unlimited. Prevents huge functions from consuming tokens.
 * Configurable via config key "snippet_max_lines". */
#define CBM_DEFAULT_SNIPPET_MAX_LINES 200
#define CBM_CONFIG_SNIPPET_MAX_LINES "snippet_max_lines"

/* Default max BFS results for trace_call_path per direction.
 * Configurable via config key "trace_max_results". */
#define CBM_DEFAULT_TRACE_MAX_RESULTS 25
#define CBM_CONFIG_TRACE_MAX_RESULTS "trace_max_results"

/* Default max output bytes for query_graph responses.
 * Caps worst-case at ~8000 tokens. Set to 0 for unlimited.
 * Configurable via config key "query_max_output_bytes". */
#define CBM_DEFAULT_QUERY_MAX_OUTPUT_BYTES 32768
#define CBM_CONFIG_QUERY_MAX_OUTPUT_BYTES "query_max_output_bytes"

/* Idle store eviction: close cached project store after this many seconds
 * of inactivity to free SQLite memory during idle periods. */
#define STORE_IDLE_TIMEOUT_S 60

/* Config key: comma-separated glob patterns to exclude from key_functions.
 * Set via: config set key_functions_exclude "scripts/,tools/,tests/" */
#define CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE "key_functions_exclude"
#define CBM_CONFIG_KEY_FUNCTIONS_COUNT   "key_functions_count"
#define CBM_CONFIG_ARCH_HOTSPOT_LIMIT    "arch_hotspot_limit"

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
    char *d = malloc(len + 1);
    if (d) {
        memcpy(d, s, len + 1);
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
    out->id = -1;

    yyjson_doc *doc = yyjson_read(line, strlen(line), 0);
    if (!doc) {
        return -1;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return -1;
    }

    yyjson_val *v_jsonrpc = yyjson_obj_get(root, "jsonrpc");
    yyjson_val *v_method = yyjson_obj_get(root, "method");
    yyjson_val *v_id = yyjson_obj_get(root, "id");
    yyjson_val *v_params = yyjson_obj_get(root, "params");

    if (!v_method || !yyjson_is_str(v_method)) {
        yyjson_doc_free(doc);
        return -1;
    }

    out->jsonrpc =
        heap_strdup(v_jsonrpc && yyjson_is_str(v_jsonrpc) ? yyjson_get_str(v_jsonrpc) : "2.0");
    out->method = heap_strdup(yyjson_get_str(v_method));

    if (v_id) {
        out->has_id = true;
        if (yyjson_is_int(v_id)) {
            out->id = yyjson_get_int(v_id);
        } else if (yyjson_is_str(v_id)) {
            out->id = strtol(yyjson_get_str(v_id), NULL, 10);
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
    free((void *)r->jsonrpc);
    free((void *)r->method);
    free((void *)r->params_raw);
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
    yyjson_mut_obj_add_int(doc, root, "id", resp->id);

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

    yyjson_mut_val *content = yyjson_mut_arr(doc);
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "type", "text");
    yyjson_mut_obj_add_str(doc, item, "text", text);
    yyjson_mut_arr_add_val(content, item);
    yyjson_mut_obj_add_val(doc, root, "content", content);

    if (is_error) {
        yyjson_mut_obj_add_bool(doc, root, "isError", true);
    }

    /* Token metadata: helps LLMs gauge context cost before requesting more data.
     * _result_bytes = byte length of the inner JSON text payload.
     * _est_tokens = bytes / 4 (same heuristic as RTK's estimate_tokens). */
    size_t text_len = text ? strlen(text) : 0;
    yyjson_mut_obj_add_int(doc, root, "_result_bytes", (int64_t)text_len);
    yyjson_mut_obj_add_int(doc, root, "_est_tokens", (int64_t)((text_len + 3) / 4));

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* ── Tool definitions ─────────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *description;
    const char *input_schema; /* JSON string */
} tool_def_t;

static const tool_def_t TOOLS[] = {
    {"index_repository", "Index a repository into the knowledge graph",
     "{\"type\":\"object\",\"properties\":{\"repo_path\":{\"type\":\"string\",\"description\":"
     "\"Path to the "
     "repository\"},\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"fast\"],\"default\":"
     "\"full\"}},\"required\":[\"repo_path\"]}"},

    {"search_graph",
     "Search the code knowledge graph for functions, classes, routes, and variables. Use INSTEAD "
     "OF grep/glob when finding code definitions, implementations, or relationships. Returns "
     "precise results in one call. When has_more=true, use offset+limit to paginate. "
     "Use mode=summary for quick codebase overview without individual results.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"label\":{\"type\":"
     "\"string\"},\"name_pattern\":{\"type\":\"string\"},\"qn_pattern\":{\"type\":\"string\"},"
     "\"file_pattern\":{\"type\":\"string\"},\"relationship\":{\"type\":\"string\"},\"min_degree\":"
     "{\"type\":\"integer\"},\"max_degree\":{\"type\":\"integer\"},\"exclude_entry_points\":{"
     "\"type\":\"boolean\"},\"include_connected\":{\"type\":\"boolean\"},\"limit\":{\"type\":"
     "\"integer\",\"description\":\"Max results per page (configurable via search_limit config key). "
     "Response includes has_more and pagination_hint when more pages exist."
     "\"},\"offset\":{\"type\":\"integer\",\"default\":0,\"description\":\"Skip N results "
     "for pagination. Check pagination_hint in response for next page offset.\"},"
     "\"sort_by\":{\"type\":\"string\",\"enum\":[\"relevance\",\"name\",\"degree\",\"calls\",\"linkrank\"],"
     "\"description\":\"Sort order: relevance (PageRank structural importance, default), "
     "name (alphabetical), degree (most connected by edge weight), "
     "calls (most direct function calls in+out), linkrank (link-based rank score).\"},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"summary\"],\"default\":\"full\","
     "\"description\":\"full=individual results (default), summary=aggregate counts by label and "
     "file. Use summary first to understand scope, then full with filters to drill down."
     "\"},\"compact\":{\"type\":\"boolean\",\"default\":true,\"description\":\"Omit fields at their "
     "default: name when it equals qualified_name's last segment (e.g. \\\"main\\\" in "
     "\\\"pkg.main\\\"), empty label/file_path, and zero degrees. Absent fields assume defaults: "
     "label/file_path='', degree=0. Saves tokens.\"},"
     "\"include_dependencies\":{\"type\":\"boolean\",\"default\":true,\"description\":\"Include "
     "symbols from dependency sub-projects (marked source=dependency in results). Set false to "
     "scope to project code only.\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Glob "
     "patterns for file paths to exclude from results (e.g. [\\\"tests/**\\\",\\\"scripts/**\\\"])."
     "\"}}}"},

    {"query_graph",
     "Execute a Cypher query against the knowledge graph for complex multi-hop patterns, "
     "aggregations, and cross-service analysis. Output is capped by default (configurable via "
     "query_max_output_bytes config key) — set max_output_bytes=0 for unlimited or add LIMIT.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\"},\"max_rows\":{\"type\":\"integer\","
     "\"description\":\"Scan-level row limit (default: unlimited). Note: limits nodes scanned, "
     "not rows returned. For output size, use max_output_bytes or add LIMIT to your Cypher query.\"},\"max_output_bytes\":{\"type\":"
     "\"integer\",\"description\":\"Max response size in bytes (configurable via "
     "query_max_output_bytes config key). Set to 0 for unlimited. When exceeded, returns "
     "truncated=true with total_bytes and hint to add LIMIT.\"}},\"required\":[\"query\"]}"},

    {"trace_call_path",
     "Trace function call paths — who calls a function and what it calls. Use INSTEAD OF grep when "
     "finding callers, dependencies, or impact analysis. Shows candidates array when function name "
     "is ambiguous. Results are deduplicated (cycles don't inflate counts).",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\"},\"project\":{"
     "\"type\":\"string\"},\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\","
     "\"both\"],\"default\":\"both\"},\"depth\":{\"type\":\"integer\",\"default\":3},\"max_results"
     "\":{\"type\":\"integer\",\"description\":\"Max nodes per direction (configurable via "
     "trace_max_results config key). Set higher for exhaustive traces. Response includes "
     "callees_total/callers_total for truncation awareness.\"},\"compact\":{\"type\":\"boolean\","
     "\"default\":true,\"description\":"
     "\"Omit name when it equals qualified_name's last segment (e.g. \\\"main\\\" in \\\"pkg.main\\\"). Reduces token count.\"},\"edge_types\":{\"type\":\"array\",\"items\":{"
     "\"type\":\"string\"}},\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Glob patterns for file paths to exclude from trace results."
     "\"}},\"required\":[\"function_name\"]}"},

    {"get_code_snippet",
     "Get source code for a specific function, class, or symbol by qualified name. Use INSTEAD OF "
     "reading entire files when you need one function's implementation. Use mode=signature for "
     "quick API lookup (99%% token savings). Use mode=head_tail for large functions to see both "
     "the signature and return/cleanup code. When truncated=true, set max_lines=0 for full source.",
     "{\"type\":\"object\",\"properties\":{\"qualified_name\":{\"type\":\"string\"},\"project\":{"
     "\"type\":\"string\"},\"auto_resolve\":{\"type\":\"boolean\",\"default\":false,\"description\":"
     "\"Auto-pick best match when name is ambiguous (by degree). Shows alternatives in response."
     "\"},\"include_neighbors\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Include "
     "caller/callee names (up to 10 each). Adds context but increases response size.\"},"
     "\"max_lines\":{\"type\":\"integer\",\"description\":\"Max source lines "
     "(configurable via snippet_max_lines config key). Set to 0 for unlimited. When truncated, "
     "response includes total_lines and signature for context.\"},\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"signature\","
     "\"head_tail\"],\"default\":\"full\",\"description\":\"full=source up to max_lines, "
     "signature=API signature+params+return type only (no source body, ~99%% savings), "
     "head_tail=first 60%% + last 40%% of max_lines with omission marker (preserves return/"
     "cleanup code)\"}},\"required\":[\"qualified_name\"]}"},

    {"get_graph_schema", "Get the schema of the knowledge graph (node labels, edge types)",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}}}"},

    {"get_architecture",
     "Get high-level architecture overview — packages, services, dependencies, and project "
     "structure at a glance.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"aspects\":{\"type\":"
     "\"array\",\"items\":{\"type\":\"string\"}}}}"},

    {"search_code",
     "Search source code content with text or regex patterns. Case-insensitive by default. "
     "Use for string literals, error messages, and config values not in the knowledge graph.",
     "{\"type\":\"object\",\"properties\":{\"pattern\":{\"type\":\"string\"},\"project\":{\"type\":"
     "\"string\"},\"file_pattern\":{\"type\":\"string\"},\"regex\":{\"type\":\"boolean\","
     "\"default\":false},\"case_sensitive\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Match case-sensitively (default: case-insensitive).\"},"
     "\"limit\":{\"type\":\"integer\",\"description\":\"Max "
     "results (configurable via search_limit config key). Set higher for exhaustive text search."
     "\"}},\"required\":["
     "\"pattern\"]}"},

    {"list_projects", "List all indexed projects", "{\"type\":\"object\",\"properties\":{}}"},

    {"delete_project", "Delete a project from the index",
     "{\"type\":\"object\",\"properties\":{\"project_name\":{\"type\":\"string\"}},\"required\":["
     "\"project_name\"]}"},

    {"index_status", "Get the indexing status of a project",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}}}"},

    {"detect_changes", "Detect code changes and their impact",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"scope\":{\"type\":"
     "\"string\"},\"depth\":{\"type\":\"integer\",\"default\":2},\"base_branch\":{\"type\":"
     "\"string\",\"default\":\"main\"}}}"},

    {"manage_adr", "Create or update Architecture Decision Records",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"mode\":{\"type\":"
     "\"string\",\"enum\":[\"get\",\"update\",\"sections\"]},\"content\":{\"type\":\"string\"},"
     "\"sections\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}}}"},

    {"ingest_traces", "Ingest runtime traces to enhance the knowledge graph",
     "{\"type\":\"object\",\"properties\":{\"traces\":{\"type\":\"array\"},\"project\":{\"type\":"
     "\"string\"}},\"required\":[\"traces\"]}"},

    {"index_dependencies",
     "Index dependency/library source for API reference. Works with ANY language (78 supported). "
     "Deps stored with {project}.dep.{name} project names, tagged source:dependency in results. "
     "PRIMARY: Use source_paths (works for all languages). "
     "SHORTCUT: package_manager auto-resolves paths for uv/cargo/npm/bun.",
     "{\"type\":\"object\",\"properties\":{"
     "\"project\":{\"type\":\"string\",\"description\":\"Existing indexed project to add deps to\"},"
     "\"source_paths\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Dep source directories, paired 1:1 with packages[]. Any language.\"},"
     "\"packages\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Dep names, paired 1:1 with source_paths[]. "
     "Creates {project}.dep.{name} in the graph.\"},"
     "\"package_manager\":{\"type\":\"string\","
     "\"description\":\"Auto-resolve source_paths for installed packages. "
     "Supported: uv/pip/cargo/npm/bun. Errors include source_path hints.\"},"
     "\"public_only\":{\"type\":\"boolean\",\"default\":true,"
     "\"description\":\"Index only exported/public symbols\"}"
     "},\"required\":[\"project\",\"packages\"]}"},
};

static const int TOOL_COUNT = sizeof(TOOLS) / sizeof(TOOLS[0]);

/* ── Streamlined tool definitions (Phase 9: 3 visible tools) ─── */

static const tool_def_t STREAMLINED_TOOLS[] = {
    {"search_code_graph",
     "Search the code knowledge graph for functions, classes, routes, variables, "
     "and relationships. Use INSTEAD OF grep/glob for code definitions and structure. "
     "Projects are auto-indexed on first query — no manual setup needed. "
     "Supports Cypher queries via 'cypher' param for complex multi-hop patterns "
     "(when cypher is set, label/name_pattern/sort_by filters are ignored — use WHERE instead). "
     "Results sorted by PageRank (structural importance) by default. "
     "mode=summary returns aggregate counts (results_suppressed=true). "
     "Read codebase://schema for node labels, edge types, and Cypher examples. "
     "Read codebase://architecture for key functions and graph overview.",
     "{\"type\":\"object\",\"properties\":{"
     "\"project\":{\"type\":\"string\",\"description\":\"Project name, path, or filter. "
     "Accepts: project name, directory path (/path/to/repo), 'self' (project only), "
     "'dep'/'deps' (dependencies only), 'dep.pandas' (specific dep), glob patterns.\"},"
     "\"cypher\":{\"type\":\"string\",\"description\":\"Cypher query for complex multi-hop "
     "patterns. When provided, other filter params are ignored. Add LIMIT.\"},"
     "\"label\":{\"type\":\"string\"},\"name_pattern\":{\"type\":\"string\"},"
     "\"qn_pattern\":{\"type\":\"string\"},\"file_pattern\":{\"type\":\"string\"},"
     "\"sort_by\":{\"type\":\"string\",\"enum\":[\"relevance\",\"name\",\"degree\",\"calls\",\"linkrank\"]},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"summary\"]},"
     "\"compact\":{\"type\":\"boolean\"},\"include_dependencies\":{\"type\":\"boolean\"},"
     "\"limit\":{\"type\":\"integer\"},\"offset\":{\"type\":\"integer\"},"
     "\"min_degree\":{\"type\":\"integer\"},\"max_degree\":{\"type\":\"integer\"},"
     "\"max_output_bytes\":{\"type\":\"integer\",\"description\":\"Max response bytes (cypher mode). 0=unlimited.\"},"
     "\"relationship\":{\"type\":\"string\"},"
     "\"exclude_entry_points\":{\"type\":\"boolean\"},"
     "\"include_connected\":{\"type\":\"boolean\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Glob patterns for file paths to exclude (e.g. [\\\"tests/**\\\",\\\"scripts/**\\\"])\"}"
     "}}"},

    {"trace_call_path",
     "Trace function call paths — who calls a function and what it calls. "
     "Use for impact analysis, understanding callers, and finding dependencies. "
     "Auto-indexes the project on first use if not already indexed. "
     "Results sorted by PageRank within each hop level. depth < 1 clamped to 1. "
     "direction must be inbound, outbound, or both (invalid values return error). "
     "Read codebase://architecture for key functions to start tracing from.",
     "{\"type\":\"object\",\"properties\":{"
     "\"function_name\":{\"type\":\"string\",\"description\":\"Function name to trace\"},"
     "\"project\":{\"type\":\"string\"},"
     "\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\",\"both\"]},"
     "\"depth\":{\"type\":\"integer\",\"default\":3},"
     "\"max_results\":{\"type\":\"integer\"},"
     "\"compact\":{\"type\":\"boolean\"},"
     "\"edge_types\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Glob patterns for file paths to exclude from trace results\"}"
     "},\"required\":[\"function_name\"]}"},

    {"get_code",
     "Get source code for a function, class, or symbol by qualified name. "
     "Use INSTEAD OF reading entire files. Use mode=signature for API lookup (99%% savings). "
     "Use mode=head_tail for large functions (preserves return code). "
     "Module nodes return metadata only — use auto_resolve=true for file source. "
     "Get qualified_name values from search_code_graph results.",
     "{\"type\":\"object\",\"properties\":{"
     "\"qualified_name\":{\"type\":\"string\",\"description\":\"Qualified name from search results\"},"
     "\"project\":{\"type\":\"string\"},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"signature\",\"head_tail\"]},"
     "\"max_lines\":{\"type\":\"integer\"},"
     "\"auto_resolve\":{\"type\":\"boolean\"},"
     "\"include_neighbors\":{\"type\":\"boolean\"}"
     "},\"required\":[\"qualified_name\"]}"},
};
static const int STREAMLINED_TOOL_COUNT = sizeof(STREAMLINED_TOOLS) / sizeof(STREAMLINED_TOOLS[0]);

/* Config key for tool visibility mode */
#define CBM_CONFIG_TOOL_MODE "tool_mode"

static void emit_tool(yyjson_mut_doc *doc, yyjson_mut_val *tools, const tool_def_t *t) {
    yyjson_mut_val *tool = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, tool, "name", t->name);
    yyjson_mut_obj_add_str(doc, tool, "description", t->description);
    yyjson_doc *schema_doc = yyjson_read(t->input_schema, strlen(t->input_schema), 0);
    if (schema_doc) {
        yyjson_mut_val *schema = yyjson_val_mut_copy(doc, yyjson_doc_get_root(schema_doc));
        yyjson_mut_obj_add_val(doc, tool, "inputSchema", schema);
        yyjson_doc_free(schema_doc);
    }
    yyjson_mut_arr_add_val(tools, tool);
}

/* cbm_mcp_tools_list() defined after struct cbm_mcp_server (needs full type) */

char *cbm_mcp_initialize_response(void) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "protocolVersion", "2024-11-05");

    yyjson_mut_val *impl = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, impl, "name", "codebase-memory-mcp");
    yyjson_mut_obj_add_str(doc, impl, "version", "0.10.0");
    yyjson_mut_obj_add_val(doc, root, "serverInfo", impl);

    yyjson_mut_val *caps = yyjson_mut_obj(doc);
    yyjson_mut_val *tools_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, caps, "tools", tools_cap);
    /* Advertise MCP resources capability — clients can read codebase://schema etc. */
    yyjson_mut_val *res_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, res_cap, "subscribe", false);
    yyjson_mut_obj_add_bool(doc, res_cap, "listChanged", true);
    yyjson_mut_obj_add_val(doc, caps, "resources", res_cap);
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

/* Check if name is the last dot/colon/slash-separated segment of qualified_name.
 * E.g. ends_with_segment("app.utils.process", "process") → true
 *      ends_with_segment("app.subprocess", "process") → false */
static bool ends_with_segment(const char *qn, const char *name) {
    if (!qn || !name) return false;
    size_t qn_len = strlen(qn);
    size_t name_len = strlen(name);
    if (name_len > qn_len) return false;
    if (name_len == qn_len) return strcmp(qn, name) == 0;
    char sep = qn[qn_len - name_len - 1];
    return (sep == '.' || sep == ':' || sep == '/') &&
           strcmp(qn + qn_len - name_len, name) == 0;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
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

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
bool cbm_mcp_get_bool_arg(const char *args_json, const char *key) {
    return cbm_mcp_get_bool_arg_default(args_json, key, false);
}

bool cbm_mcp_get_bool_arg_default(const char *args_json, const char *key, bool default_val) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return default_val;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    bool result = default_val;
    if (val && yyjson_is_bool(val)) {
        result = yyjson_get_bool(val);
    }
    yyjson_doc_free(doc);
    return result;
}

/* Extract a JSON array of strings from args. Returns heap-allocated
 * NULL-terminated array of heap-allocated strings. Caller must free each
 * string and the array itself. Returns NULL if key absent or not array. */
static char **cbm_mcp_get_string_array_arg(const char *args_json, const char *key, int *out_count) {
    if (out_count) *out_count = 0;
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) return NULL;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *arr = yyjson_obj_get(root, key);
    if (!arr || !yyjson_is_arr(arr)) {
        yyjson_doc_free(doc);
        return NULL;
    }
    int n = (int)yyjson_arr_size(arr);
    if (n == 0) {
        yyjson_doc_free(doc);
        return NULL;
    }
    char **result = calloc((size_t)(n + 1), sizeof(char *));
    int count = 0;
    yyjson_val *item;
    yyjson_arr_iter iter = yyjson_arr_iter_with(arr);
    while ((item = yyjson_arr_iter_next(&iter))) {
        if (yyjson_is_str(item)) {
            result[count++] = heap_strdup(yyjson_get_str(item));
        }
    }
    result[count] = NULL;
    if (out_count) *out_count = count;
    yyjson_doc_free(doc);
    return result;
}

static void free_string_array(char **arr) {
    if (!arr) return;
    for (int i = 0; arr[i]; i++) free(arr[i]);
    free(arr);
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP SERVER
 * ══════════════════════════════════════════════════════════════════ */

/* Forward declarations for functions defined after first use */
static void notify_resources_updated(cbm_mcp_server_t *srv);
static char *build_key_functions_sql(const char *exclude_csv, const char **exclude_arr, int limit);
char *cbm_glob_to_like(const char *pattern); /* store.c */

struct cbm_mcp_server {
    cbm_store_t *store;        /* currently open project store (or NULL) */
    bool owns_store;           /* true if we opened the store */
    char *current_project;     /* which project store is open for (heap) */
    time_t store_last_used;    /* last time resolve_store was called for a named project */
    char update_notice[256];   /* one-shot update notice, cleared after first injection */
    bool update_checked;       /* true after background check has been launched */
    cbm_thread_t update_tid;   /* background update check thread */
    bool update_thread_active; /* true if update thread was started and needs joining */

    /* Session + auto-index state */
    char session_root[1024];     /* detected project root path */
    char session_project[256];   /* derived project name */
    bool session_detected;       /* true after first detection attempt */
    struct cbm_watcher *watcher; /* external watcher ref (not owned) */
    struct cbm_config *config;   /* external config ref (not owned) */
    cbm_thread_t autoindex_tid;
    bool autoindex_active; /* true if auto-index thread was started */
    bool autoindex_failed; /* IX-1: true if last auto-index attempt failed */
    bool just_autoindexed; /* IX-3: true after auto-index completes, reset on next search */
    bool context_injected; /* true after first _context header sent (Phase 9) */
    bool client_has_resources; /* true if client advertised resources capability */
    FILE *out_stream;          /* stdout for sending notifications (set in server_run) */
};

/* ── Tool list (needs full struct definition above) ──────────── */

char *cbm_mcp_tools_list(cbm_mcp_server_t *srv) {
    /* Env var CBM_TOOL_MODE overrides config (for backwards compat without config store) */
    const char *tool_mode = getenv("CBM_TOOL_MODE");
    if (!tool_mode || tool_mode[0] == '\0') {
        tool_mode = (srv && srv->config)
            ? cbm_config_get(srv->config, CBM_CONFIG_TOOL_MODE, "streamlined")
            : "streamlined";
    }
    bool classic = (strcmp(tool_mode, "classic") == 0);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *tools = yyjson_mut_arr(doc);

    if (!classic) {
        /* Streamlined mode: emit 3 consolidated tools */
        for (int i = 0; i < STREAMLINED_TOOL_COUNT; i++) {
            emit_tool(doc, tools, &STREAMLINED_TOOLS[i]);
        }
        /* Also emit individually-enabled tools */
        for (int i = 0; i < TOOL_COUNT; i++) {
            char key[64];
            snprintf(key, sizeof(key), "tool_%s", TOOLS[i].name);
            if (srv && srv->config && cbm_config_get_bool(srv->config, key, false)) {
                emit_tool(doc, tools, &TOOLS[i]);
            }
        }

        /* Progressive disclosure: list hidden tools so AI knows they exist.
         * Added as a special tool entry with description explaining how to enable. */
        yyjson_mut_val *hint_tool = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, hint_tool, "name", "_hidden_tools");
        yyjson_mut_obj_add_str(doc, hint_tool, "description",
            "14 additional tools available but hidden in streamlined mode. "
            "Hidden: index_repository, search_graph, query_graph, get_code_snippet, "
            "get_graph_schema, get_architecture, search_code, list_projects, "
            "delete_project, index_status, detect_changes, manage_adr, "
            "ingest_traces, index_dependencies. "
            "Projects auto-index on first query (no manual setup needed). "
            "Enable all: set env CBM_TOOL_MODE=classic or config set tool_mode classic. "
            "Enable one: config set tool_<name> true (e.g. tool_index_repository true). "
            "Resources: codebase://schema (labels, edge types, Cypher examples), "
            "codebase://architecture (key functions, graph overview), "
            "codebase://status (index state: ready/indexing/not_indexed/empty).");
        /* inputSchema MUST be a JSON object, not a string — Claude Code rejects
         * the entire tools/list if any tool has a string inputSchema. */
        yyjson_mut_val *hint_schema = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, hint_schema, "type", "object");
        yyjson_mut_val *hint_props = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_val(doc, hint_schema, "properties", hint_props);
        yyjson_mut_obj_add_val(doc, hint_tool, "inputSchema", hint_schema);
        yyjson_mut_arr_add_val(tools, hint_tool);
    } else {
        /* Classic mode: all 15 original tools */
        for (int i = 0; i < TOOL_COUNT; i++) {
            emit_tool(doc, tools, &TOOLS[i]);
        }
    }

    yyjson_mut_obj_add_val(doc, root, "tools", tools);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

cbm_mcp_server_t *cbm_mcp_server_new(const char *store_path) {
    cbm_mcp_server_t *srv = calloc(1, sizeof(*srv));
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

void cbm_mcp_server_set_session_project(cbm_mcp_server_t *srv, const char *name) {
    if (!srv || !name) return;
    snprintf(srv->session_project, sizeof(srv->session_project), "%s", name);
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
    if (srv->update_thread_active) {
        cbm_thread_join(&srv->update_tid);
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

/* ── Cache dir + project DB path helpers ───────────────────────── */

/* Returns the platform cache directory: ~/.cache/codebase-memory-mcp
 * Writes to buf, returns buf for convenience. */
static const char *cache_dir(char *buf, size_t bufsz) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *home = getenv("HOME");
    if (!home) {
        home = "/tmp";
    }
    snprintf(buf, bufsz, "%s/.cache/codebase-memory-mcp", home);
    return buf;
}

/* Returns full .db path for a project: <cache_dir>/<project>.db */
static const char *project_db_path(const char *project, char *buf, size_t bufsz) {
    char dir[1024];
    cache_dir(dir, sizeof(dir));
    snprintf(buf, bufsz, "%s/%s.db", dir, project);
    return buf;
}

/* ── QN project extraction ─────────────────────────────────────── */

/* Try to identify the project prefix of a qualified name by scanning each
 * dot-separated prefix and checking if a matching DB file exists.
 * Returns a heap-allocated project name (caller must free), or NULL if no
 * matching DB is found.  Cost: one access() call per dot in the QN (~5-10). */
static char *extract_project_from_qn(const char *qn) {
    if (!qn) return NULL;
    const char *home = getenv("HOME");
    if (!home) return NULL;

    /* Scan each dot-separated prefix of the QN and test if a matching DB file
     * exists.  Walk left-to-right so the last hit is the longest (most
     * specific) match.  Record only the winning offset to do a single strdup
     * at the end — avoids repeated alloc/free on multi-dot project names. */
    size_t qn_len = strlen(qn);
    char *candidate = malloc(qn_len + 1);
    if (!candidate) return NULL;
    memcpy(candidate, qn, qn_len + 1);

    size_t best_end = 0; /* length of the longest matching prefix found */
    char db_path[1024];
    const char *home_val = home;

    for (size_t i = 0; i < qn_len; i++) {
        if (candidate[i] == '.') {
            candidate[i] = '\0';
            snprintf(db_path, sizeof(db_path),
                     "%s/.cache/codebase-memory-mcp/%s.db", home_val, candidate);
            if (access(db_path, F_OK) == 0) {
                best_end = i; /* length of this prefix */
            }
            candidate[i] = '.';
        }
    }

    char *result = NULL;
    if (best_end > 0) {
        result = malloc(best_end + 1);
        if (result) {
            memcpy(result, qn, best_end);
            result[best_end] = '\0';
        }
    }
    free(candidate);
    return result; /* NULL if no matching DB found; caller frees */
}

/* ── Store resolution ──────────────────────────────────────────── */

/* Open the right project's .db file for query tools.
 * Caches the connection — reopens only when project changes.
 * Tracks last-access time so the event loop can evict idle stores. */
/* Extract the parent project name from a dep project name.
 * "myapp.dep.pandas" → "myapp", "myapp.dep" → "myapp", "myapp" → "myapp".
 * Returns a stack buffer pointer (caller must NOT free). */
static const char *parent_project_for_db(const char *project, char *buf, size_t bufsz) {
    const char *dep = strstr(project, ".dep");
    if (dep && (dep[4] == '.' || dep[4] == '\0')) {
        size_t len = (size_t)(dep - project);
        if (len >= bufsz) len = bufsz - 1;
        memcpy(buf, project, len);
        buf[len] = '\0';
        return buf;
    }
    return project; /* no .dep → use as-is */
}

static cbm_store_t *resolve_store(cbm_mcp_server_t *srv, const char *project) {
    if (!project) {
        return srv->store; /* no project specified → use whatever's open */
    }

    srv->store_last_used = time(NULL);

    /* Dep projects (e.g., "myapp.dep.pandas") live in the parent project's DB
     * ("myapp.db"), not in a separate "myapp.dep.pandas.db". Extract parent. */
    char parent_buf[1024];
    const char *db_project = parent_project_for_db(project, parent_buf, sizeof(parent_buf));

    /* Already open for this project's DB? */
    if (srv->current_project && strcmp(srv->current_project, db_project) == 0 && srv->store) {
        return srv->store;
    }

    /* Close old store */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }

    /* Open project's .db file */
    char path[1024];
    project_db_path(db_project, path, sizeof(path));
    srv->store = cbm_store_open_path(path);
    srv->owns_store = true;
    free(srv->current_project);
    srv->current_project = heap_strdup(db_project);
    /* Register newly-accessed project with watcher (root_path from DB) */
    if (srv->watcher && srv->store) {
        cbm_project_t proj = {0};
        if (cbm_store_get_project(srv->store, db_project, &proj) == CBM_STORE_OK) {
            if (proj.root_path && proj.root_path[0])
                cbm_watcher_watch(srv->watcher, db_project, proj.root_path);
            /* Always free fields — cbm_store_get_project heap-allocates even empty strings */
            cbm_project_free_fields(&proj);
        }
    }

    return srv->store;
}

/* Bail with JSON error + hint when no store is available. */
/* Auto-index on first use: when store is NULL, session_root is set, and
 * auto_index_on_first_use is enabled, run the pipeline synchronously.
 * This eliminates the need for an explicit index_repository call.
 * MCP is strict request-response — synchronous blocking is safe here
 * (same pattern used by handle_index_repository at line ~1959). */
#define REQUIRE_STORE(store, project)                                                             \
    do {                                                                                          \
        if (!(store) && srv->session_root[0] && access(srv->session_root, F_OK) == 0) {              \
            /* Try auto-index on first use (only if session_root is a real directory) */           \
            if (srv->autoindex_active) {                                                          \
                /* Background thread running — wait for it to complete */                         \
                cbm_thread_join(&srv->autoindex_tid);                                              \
                srv->autoindex_active = false;                                                    \
                /* Re-resolve store after background index finished */                            \
                store = resolve_store(srv, project);                                              \
            }                                                                                     \
            if (!(store)) {                                                                       \
                /* No background thread or it failed — try sync index */                          \
                cbm_pipeline_t *_p = cbm_pipeline_new(                                            \
                    srv->session_root, NULL, CBM_MODE_FULL);                                      \
                if (_p) {                                                                         \
                    cbm_log_info("autoindex.sync", "project", srv->session_project);               \
                    int _rc = cbm_pipeline_run(_p);                                               \
                    cbm_pipeline_free(_p);                                                        \
                    if (_rc != 0) {                                                               \
                        /* IX-1: Auto-index FAILED */                                             \
                        srv->autoindex_failed = true;                                             \
                        cbm_log_error("autoindex.failed", "project",                              \
                                      srv->session_project);                                      \
                    } else {                                                                      \
                        srv->autoindex_failed = false;                                            \
                        srv->just_autoindexed = true;                                             \
                        /* Invalidate + reopen store */                                           \
                        if (srv->owns_store && srv->store) {                                      \
                            cbm_store_close(srv->store);                                          \
                            srv->store = NULL;                                                    \
                        }                                                                         \
                        free(srv->current_project);                                               \
                        srv->current_project = NULL;                                              \
                        store = resolve_store(srv, srv->session_project);                          \
                        if (store) {                                                              \
                            cbm_dep_auto_index(srv->session_project, srv->session_root,           \
                                               store, CBM_DEFAULT_AUTO_DEP_LIMIT);                \
                            cbm_pagerank_compute_with_config(store, srv->session_project,          \
                                                            srv->config);                         \
                        }                                                                         \
                    }                                                                             \
                    cbm_mem_collect();                                                             \
                } else {                                                                          \
                    srv->autoindex_failed = true;                                                  \
                    cbm_log_error("autoindex.create_failed", "root",                              \
                                  srv->session_root);                                              \
                }                                                                                 \
            }                                                                                     \
        }                                                                                         \
        if (!(store)) {                                                                           \
            if (srv->autoindex_failed) {                                                          \
                free(project);                                                                    \
                return cbm_mcp_text_result(                                                       \
                    "{\"error\":\"auto-indexing failed for this project\","                        \
                    "\"detail\":\"The pipeline failed. Check file permissions and project size.\"," \
                    "\"fix\":\"Run index_repository explicitly with repo_path for detailed errors.\"}", \
                    true);                                                                        \
            }                                                                                     \
            free(project);                                                                        \
            return cbm_mcp_text_result(                                                           \
                "{\"error\":\"no project loaded\","                                               \
                "\"hint\":\"Run index_repository with repo_path to index the project first.\"}", \
                true);                                                                            \
        }                                                                                         \
    } while (0)

/* ── Auto-context injection (Phase 9) ─────────────────────────── */

/* Inject _context header into the FIRST tool response after session starts.
 * Contains architecture, schema, status — eliminates the need for separate
 * get_architecture / get_graph_schema / index_status / list_projects calls.
 * Subsequent responses include only session_project (lightweight). */
static void inject_context_once(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                cbm_mcp_server_t *srv, cbm_store_t *store) {
    /* Always include session_project */
    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    /* If client supports MCP resources, skip _context injection — client reads
     * codebase://schema, codebase://architecture, codebase://status instead. */
    if (srv->client_has_resources) return;

    if (srv->context_injected) return;
    srv->context_injected = true;

    yyjson_mut_val *ctx = yyjson_mut_obj(doc);

    if (!store) {
        yyjson_mut_obj_add_str(doc, ctx, "status", "not_indexed");
        yyjson_mut_obj_add_str(doc, ctx, "hint",
            "Project not yet indexed. Use index_repository or set auto_index=true.");
        yyjson_mut_obj_add_val(doc, root, "_context", ctx);
        return;
    }

    yyjson_mut_obj_add_str(doc, ctx, "status", "ready");

    /* Node/edge counts */
    const char *proj = srv->session_project[0] ? srv->session_project : NULL;
    int nodes = cbm_store_count_nodes(store, proj);
    int edges = cbm_store_count_edges(store, proj);
    yyjson_mut_obj_add_int(doc, ctx, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, ctx, "edges", edges);

    /* Schema: node labels + edge types */
    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, proj, &schema);
    yyjson_mut_val *label_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; i++) {
        yyjson_mut_val *lbl = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, lbl, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, lbl, "count", schema.node_labels[i].count);
        yyjson_mut_arr_add_val(label_arr, lbl);
    }
    yyjson_mut_obj_add_val(doc, ctx, "node_labels", label_arr);

    yyjson_mut_val *type_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *et = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, et, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, et, "count", schema.edge_types[i].count);
        yyjson_mut_arr_add_val(type_arr, et);
    }
    yyjson_mut_obj_add_val(doc, ctx, "edge_types", type_arr);
    cbm_store_schema_free(&schema);

    /* PageRank stats */
    sqlite3 *db = cbm_store_get_db(store);
    if (db && proj) {
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT COUNT(*), MAX(computed_at) FROM pagerank WHERE project = ?1",
                -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, proj, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int ranked = sqlite3_column_int(stmt, 0);
                if (ranked > 0) {
                    yyjson_mut_obj_add_int(doc, ctx, "ranked_nodes", ranked);
                    const char *ts = (const char *)sqlite3_column_text(stmt, 1);
                    if (ts) yyjson_mut_obj_add_strcpy(doc, ctx, "pagerank_computed_at", ts);
                }
            }
            sqlite3_finalize(stmt);
        }
    }

    /* Detected ecosystem */
    if (srv->session_root[0]) {
        cbm_pkg_manager_t eco = cbm_detect_ecosystem(srv->session_root);
        if (eco != CBM_PKG_COUNT) {
            yyjson_mut_obj_add_str(doc, ctx, "detected_ecosystem",
                                   cbm_pkg_manager_str(eco));
        }
    }

    yyjson_mut_obj_add_val(doc, root, "_context", ctx);
}

/* ── Smart project param expansion ─────────────────────────────── */

typedef enum { MATCH_NONE, MATCH_EXACT, MATCH_PREFIX, MATCH_GLOB } match_mode_t;

typedef struct {
    char *value;       /* expanded project string (heap) or NULL. Caller must free. */
    match_mode_t mode; /* how to match in SQL */
} project_expand_t;

/* Forward declaration — defined below, needed by handle_get_graph_schema */
static cbm_store_t *resolve_project_store(cbm_mcp_server_t *srv,
                                           char *raw_project,
                                           project_expand_t *out_pe);

/* Expand project param shorthands (self/dep/glob/prefix).
 * Takes ownership of raw — caller must NOT free raw after this call.
 * Returns expanded result. Caller must free(result.value).
 * Runtime: O(1) — fixed number of string comparisons + one snprintf + strdup.
 * Memory: one heap allocation for result.value. */
static project_expand_t expand_project_param(cbm_mcp_server_t *srv, char *raw) {
    project_expand_t r = {.value = NULL, .mode = MATCH_NONE};
    if (!raw) return r;

    /* Rule 0: Path detection — convert paths to project names.
     * Enables: search_code_graph(project="/path/to/repo") */
    if (raw[0] == '/' || raw[0] == '~' || (raw[0] == '.' && raw[1] == '/') ||
        (strchr(raw, '/') != NULL && raw[0] != '*')) {
        char *resolved = realpath(raw, NULL);
        const char *path = resolved ? resolved : raw;
        char *name = cbm_project_name_from_path(path);
        if (resolved && srv->session_root[0] == '\0') {
            snprintf(srv->session_root, sizeof(srv->session_root), "%s", resolved);
            snprintf(srv->session_project, sizeof(srv->session_project), "%s", name);
        }
        free(raw);
        free(resolved);
        r.value = name;
        r.mode = MATCH_PREFIX;
        return r;
    }

    /* Guard: if session_project is empty, skip all expansion rules */
    if (!srv->session_project[0]) {
        r.value = raw;
        r.mode = strchr(raw, '*') ? MATCH_GLOB : MATCH_PREFIX;
        return r;
    }

    size_t sp_len = strlen(srv->session_project);
    char buf[4096];

    /* Rule 1: "self" prefix → replace with session project name */
    if (strncmp(raw, "self", 4) == 0 && (raw[4] == '\0' || raw[4] == '.')) {
        bool is_self_only = (raw[4] == '\0');
        snprintf(buf, sizeof(buf), "%s%s", srv->session_project, raw + 4);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = is_self_only ? MATCH_EXACT : MATCH_PREFIX;
        if (r.mode == MATCH_PREFIX && strchr(r.value, '*')) r.mode = MATCH_GLOB;
        return r;
    }

    /* Rule 2: "dep" / "deps" exactly → "{session}.dep" */
    if (strcmp(raw, "dep") == 0 || strcmp(raw, "deps") == 0) {
        snprintf(buf, sizeof(buf), "%s.dep", srv->session_project);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = MATCH_PREFIX;
        return r;
    }

    /* Rule 3: starts with "dep." → prepend session */
    if (strncmp(raw, "dep.", 4) == 0) {
        snprintf(buf, sizeof(buf), "%s.%s", srv->session_project, raw);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = strchr(r.value, '*') ? MATCH_GLOB : MATCH_PREFIX;
        return r;
    }

    /* Rule 4: starts with "{session}." but next segment isn't "dep" → insert .dep. */
    if (strncmp(raw, srv->session_project, sp_len) == 0 && raw[sp_len] == '.' &&
        !(strncmp(raw + sp_len + 1, "dep", 3) == 0 &&
          (raw[sp_len + 4] == '.' || raw[sp_len + 4] == '\0'))) {
        snprintf(buf, sizeof(buf), "%s.dep.%s", srv->session_project, raw + sp_len + 1);
        free(raw);
        r.value = heap_strdup(buf);
        r.mode = strchr(r.value, '*') ? MATCH_GLOB : MATCH_PREFIX;
        return r;
    }

    /* Rule 5: everything else — as-is (bare words are project names) */
    r.value = raw;
    r.mode = strchr(raw, '*') ? MATCH_GLOB : MATCH_PREFIX;
    return r;
}

/* Fill cbm_search_params_t project fields from an expand result.
 * Also translates * → % for SQL LIKE in glob mode. */
static void fill_project_params(const project_expand_t *pe, cbm_search_params_t *params) {
    switch (pe->mode) {
    case MATCH_GLOB:
        params->project_pattern = pe->value;
        break;
    case MATCH_EXACT:
        params->project = pe->value;
        params->project_exact = true;
        break;
    case MATCH_PREFIX:
        params->project = pe->value;
        break;
    case MATCH_NONE:
        break;
    }
}

/* ── Tool handler implementations ─────────────────────────────── */

/* Validate that a file is a codebase-memory-mcp SQLite database.
 * Returns true if file has SQLite magic bytes AND contains the expected
 * 'nodes' table (core schema indicator).
 * On ANY error: returns false, logs actionable warning to stderr,
 * does NOT crash, does NOT hang, does NOT modify the file.
 * Opens read-only with busy_timeout to avoid hanging on locked files. */
static bool validate_cbm_db(const char *path) {
    if (!path) return false;

    struct stat vst;
    if (stat(path, &vst) != 0) return false;
    if (vst.st_size == 0) {
        cbm_log_warn("db.skip", "path", path, "reason", "empty_file");
        return false;
    }

    /* Check SQLite magic bytes (first 16 bytes = "SQLite format 3\0") */
    FILE *f = fopen(path, "rb");
    if (!f) {
        cbm_log_warn("db.skip", "path", path, "reason", "cannot_open");
        return false;
    }
    char magic[16];
    size_t n = fread(magic, 1, 16, f);
    fclose(f);
    if (n < 16 || memcmp(magic, "SQLite format 3", 15) != 0) {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        cbm_log_warn("db.skip", "file", base, "reason", "not_sqlite");
        return false;
    }

    /* Open READ-ONLY — never modify foreign databases.
     * Check for 'nodes' table which is the core cbm schema indicator. */
    sqlite3 *db = NULL;
    int rc = sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY, NULL);
    if (rc != SQLITE_OK) {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        cbm_log_warn("db.skip", "file", base, "reason", "sqlite_open_failed");
        if (db) sqlite3_close(db);
        return false;
    }
    sqlite3_busy_timeout(db, 1000); /* 1s max — don't hang on locked files */

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(db,
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='nodes' LIMIT 1;",
        -1, &stmt, NULL);
    bool valid = false;
    if (rc == SQLITE_OK && sqlite3_step(stmt) == SQLITE_ROW) {
        valid = true;
    } else {
        const char *base = strrchr(path, '/');
        base = base ? base + 1 : path;
        cbm_log_warn("db.skip", "file", base,
                     "reason", "not_cbm_database",
                     "hint", "File in cache dir lacks codebase-memory-mcp schema. "
                             "Move it aside if not needed.");
    }
    if (stmt) sqlite3_finalize(stmt);
    sqlite3_close(db);
    return valid;
}

/* list_projects: scan cache directory for .db files.
 * Each project is a single .db file — no central registry needed. */
static char *handle_list_projects(cbm_mcp_server_t *srv, const char *args) {
    (void)srv;
    (void)args;

    char dir_path[1024];
    cache_dir(dir_path, sizeof(dir_path));

    cbm_dir_t *d = cbm_opendir(dir_path);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);

    if (d) {
        cbm_dirent_t *entry;
        while ((entry = cbm_readdir(d)) != NULL) {
            const char *name = entry->name;
            size_t len = strlen(name);

            /* Must end with .db and be at least 4 chars (x.db) */
            if (len < 4 || strcmp(name + len - 3, ".db") != 0) {
                continue;
            }

            /* Skip temp/internal files and corrupt project names */
            if (strncmp(name, "tmp-", 4) == 0 || strncmp(name, "_", 1) == 0 ||
                strncmp(name, ":memory:", 8) == 0 ||
                strcmp(name, "..db") == 0 || strcmp(name, ".db") == 0) {
                continue;
            }

            /* Extract project name = filename without .db suffix */
            char project_name[1024];
            snprintf(project_name, sizeof(project_name), "%.*s", (int)(len - 3), name);

            /* Skip invalid project names (corrupt entries like ..db) */
            if (project_name[0] == '\0' || strcmp(project_name, ".") == 0 ||
                strcmp(project_name, "..") == 0) {
                continue;
            }

            /* Get file metadata */
            char full_path[2048];
            snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);
            struct stat st;
            if (stat(full_path, &st) != 0) {
                continue;
            }

            /* Validate db structure before opening — skip corrupt/non-cbm files */
            if (!validate_cbm_db(full_path)) {
                continue;
            }

            /* Open briefly to get node/edge count + root_path */
            cbm_store_t *pstore = cbm_store_open_path(full_path);
            int nodes = 0;
            int edges = 0;
            char root_path_buf[1024] = "";
            if (pstore) {
                nodes = cbm_store_count_nodes(pstore, project_name);
                edges = cbm_store_count_edges(pstore, project_name);
                cbm_project_t proj = {0};
                if (cbm_store_get_project(pstore, project_name, &proj) == CBM_STORE_OK) {
                    if (proj.root_path) {
                        snprintf(root_path_buf, sizeof(root_path_buf), "%s", proj.root_path);
                    }
                    free((void *)proj.name);
                    free((void *)proj.indexed_at);
                    free((void *)proj.root_path);
                }
                cbm_store_close(pstore);
            }

            yyjson_mut_val *p = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, p, "name", project_name);
            yyjson_mut_obj_add_strcpy(doc, p, "root_path", root_path_buf);
            yyjson_mut_obj_add_int(doc, p, "nodes", nodes);
            yyjson_mut_obj_add_int(doc, p, "edges", edges);
            yyjson_mut_obj_add_int(doc, p, "size_bytes", (int64_t)st.st_size);
            yyjson_mut_arr_add_val(arr, p);
        }
        cbm_closedir(d);
    }

    yyjson_mut_obj_add_val(doc, root, "projects", arr);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_get_graph_schema(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

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
        yyjson_mut_arr_add_val(labels, lbl);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", labels);

    yyjson_mut_val *types = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *typ = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, typ, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, typ, "count", schema.edge_types[i].count);
        yyjson_mut_arr_add_val(types, typ);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", types);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_schema_free(&schema);
    free(project);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Expand a raw project param, resolve the correct store, and auto-index if needed.
 * Returns the resolved store (or NULL). Sets *out_pe to the expand result
 * (caller must free out_pe->value). Handles:
 *   - expand_project_param (Rule 0: /path → project name)
 *   - DB selection with prefix collision avoidance
 *   - Auto-index on first use (join background thread or sync index) */
static cbm_store_t *resolve_project_store(cbm_mcp_server_t *srv,
                                           char *raw_project,
                                           project_expand_t *out_pe) {
    project_expand_t pe = expand_project_param(srv, raw_project);

    /* DB selection: if expanded value IS the session project or a dep of it
     * (session.dep.X), use session store. The check requires the char after
     * session_project to be '.' or '\0' to avoid prefix collisions. */
    const char *db_project = pe.value;
    if (pe.value && srv->session_project[0]) {
        size_t sp_len = strlen(srv->session_project);
        if (strncmp(pe.value, srv->session_project, sp_len) == 0 &&
            (pe.value[sp_len] == '.' || pe.value[sp_len] == '\0')) {
            db_project = srv->session_project; /* deps are in session db */
        }
    }
    cbm_store_t *store = resolve_store(srv, db_project);

    /* Auto-index on first use (same logic as REQUIRE_STORE macro). */
    if (!store && srv->session_root[0] && access(srv->session_root, F_OK) == 0) {
        if (srv->autoindex_active) {
            cbm_thread_join(&srv->autoindex_tid);
            srv->autoindex_active = false;
            store = resolve_store(srv, db_project);
        }
        if (!store) {
            cbm_pipeline_t *_p = cbm_pipeline_new(srv->session_root, NULL, CBM_MODE_FULL);
            if (_p) {
                cbm_log_info("autoindex.sync", "project", srv->session_project);
                cbm_pipeline_run(_p);
                cbm_pipeline_free(_p);
                if (srv->owns_store && srv->store) {
                    cbm_store_close(srv->store); srv->store = NULL;
                }
                free(srv->current_project); srv->current_project = NULL;
                store = resolve_store(srv, srv->session_project);
                if (store) {
                    cbm_dep_auto_index(srv->session_project, srv->session_root,
                                       store, CBM_DEFAULT_AUTO_DEP_LIMIT);
                    cbm_pagerank_compute_with_config(store, srv->session_project, srv->config);
                }
                cbm_mem_collect();
            }
        }
    }

    *out_pe = pe; /* caller takes ownership of pe.value */
    return store;
}

static char *handle_search_graph(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    if (!store) {
        free(pe.value);
        return cbm_mcp_text_result(
            "{\"error\":\"no project loaded\","
            "\"hint\":\"Run index_repository with repo_path to index the project first.\"}", true);
    }

    char *label = cbm_mcp_get_string_arg(args, "label");
    /* F1: treat empty string as "no filter" */
    if (label && label[0] == '\0') { free(label); label = NULL; }
    char *name_pattern = cbm_mcp_get_string_arg(args, "name_pattern");
    char *qn_pattern = cbm_mcp_get_string_arg(args, "qn_pattern");
    /* F9: pre-validate regex patterns — O(1) per pattern via cbm_regcomp */
    if (name_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, name_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char errbuf[512];
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"invalid regex in name_pattern: '%s'\","
                "\"hint\":\"Escape special chars with \\\\\\\\ or use plain text\"}", name_pattern);
            free(label); free(name_pattern); free(pe.value);
            return cbm_mcp_text_result(errbuf, true);
        }
        cbm_regfree(&re);
    }
    if (qn_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, qn_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char errbuf[512];
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"invalid regex in qn_pattern: '%s'\","
                "\"hint\":\"Escape special chars with \\\\\\\\ or use plain text\"}", qn_pattern);
            free(label); free(name_pattern); free(qn_pattern); free(pe.value);
            return cbm_mcp_text_result(errbuf, true);
        }
        cbm_regfree(&re);
    }
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *relationship = cbm_mcp_get_string_arg(args, "relationship");
    char *sort_by = cbm_mcp_get_string_arg(args, "sort_by");
    /* F6: validate sort_by enum — O(1) string comparisons */
    if (sort_by && strcmp(sort_by, "relevance") != 0 && strcmp(sort_by, "name") != 0 &&
        strcmp(sort_by, "degree") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid sort_by '%s'\","
            "\"hint\":\"Valid values: relevance, name, degree, calls, linkrank\"}", sort_by);
        free(label); free(name_pattern); free(qn_pattern); free(file_pattern);
        free(relationship); free(sort_by); free(pe.value);
        return cbm_mcp_text_result(errbuf, true);
    }
    int cfg_search_limit = cbm_config_get_int(srv->config, CBM_CONFIG_SEARCH_LIMIT,
                                               CBM_DEFAULT_SEARCH_LIMIT);
    int limit = cbm_mcp_get_int_arg(args, "limit", cfg_search_limit);
    /* F4: treat limit<=0 as default */
    if (limit <= 0) limit = cfg_search_limit;
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", true);
    char *search_mode = cbm_mcp_get_string_arg(args, "mode");
    /* F7: validate mode enum — O(1) */
    if (search_mode && strcmp(search_mode, "full") != 0 && strcmp(search_mode, "summary") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid mode '%s'\","
            "\"hint\":\"Valid values: full, summary\"}", search_mode);
        free(label); free(name_pattern); free(qn_pattern); free(file_pattern);
        free(relationship); free(sort_by); free(search_mode); free(pe.value);
        return cbm_mcp_text_result(errbuf, true);
    }
    int min_degree = cbm_mcp_get_int_arg(args, "min_degree", -1);
    int max_degree = cbm_mcp_get_int_arg(args, "max_degree", -1);
    bool exclude_entry_points = cbm_mcp_get_bool_arg_default(args, "exclude_entry_points", false);
    bool include_connected = cbm_mcp_get_bool_arg_default(args, "include_connected", false);
    /* Default true: prefix match includes myproject.dep.* sub-projects.
     * false: forces exact match (only effective when project set + not glob mode). */
    bool include_dependencies = cbm_mcp_get_bool_arg_default(args, "include_dependencies", true);

    /* Summary mode needs all results for accurate aggregation */
    bool is_summary = search_mode && strcmp(search_mode, "summary") == 0;
    int effective_limit = is_summary ? 10000 : limit;

    cbm_search_params_t params = {0};
    fill_project_params(&pe, &params);
    /* include_dependencies=false: force exact match to exclude dep sub-projects.
     * Guard: only effective for MATCH_PREFIX (project set, no glob pattern).
     * MATCH_GLOB (project_pattern set) and MATCH_NONE (no project) are unaffected. */
    if (!include_dependencies && params.project && !params.project_pattern) {
        params.project_exact = true;
    }
    params.label = label;
    params.name_pattern = name_pattern;
    params.qn_pattern = qn_pattern;
    params.file_pattern = file_pattern;
    params.relationship = relationship;
    params.sort_by = sort_by;
    params.degree_mode = srv->config
        ? cbm_config_get(srv->config, "degree_mode", NULL)
        : NULL;
    params.limit = effective_limit;
    params.offset = offset;
    params.min_degree = min_degree;
    params.max_degree = max_degree;
    params.exclude_entry_points = exclude_entry_points;
    params.include_connected = include_connected;
    int exclude_count = 0;
    char **exclude = cbm_mcp_get_string_array_arg(args, "exclude", &exclude_count);
    params.exclude_paths = (const char **)exclude;

    cbm_search_output_t out = {0};
    cbm_store_search(store, &params, &out);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_int(doc, root, "total", out.total);

    /* Auto-context: first response gets full architecture/schema/_context header.
     * Subsequent responses just get session_project. */
    inject_context_once(doc, root, srv, store);

    if (is_summary) {
        /* Summary mode: aggregate counts by label and file (top 20) */
        yyjson_mut_val *by_label = yyjson_mut_obj(doc);
        yyjson_mut_val *by_file = yyjson_mut_obj(doc);

        /* Simple aggregation — 64 slots for labels (CBM defines ~12 label types),
         * 20 slots for top files. Excess entries are silently capped. */
        const char *labels[64] = {0};
        int label_counts[64] = {0};
        int label_n = 0;
        const char *files[20] = {0};
        int file_counts[20] = {0};
        int file_n = 0;

        for (int i = 0; i < out.count; i++) {
            cbm_search_result_t *sr = &out.results[i];
            /* Count by label */
            const char *lbl = sr->node.label ? sr->node.label : "(unknown)";
            int found = -1;
            for (int j = 0; j < label_n; j++) {
                if (strcmp(labels[j], lbl) == 0) { found = j; break; }
            }
            if (found >= 0) {
                label_counts[found]++;
            } else if (label_n < 64) {
                labels[label_n] = lbl;
                label_counts[label_n] = 1;
                label_n++;
            }
            /* Count by file (top 20 only) */
            const char *fp = sr->node.file_path ? sr->node.file_path : "(unknown)";
            found = -1;
            for (int j = 0; j < file_n; j++) {
                if (strcmp(files[j], fp) == 0) { found = j; break; }
            }
            if (found >= 0) {
                file_counts[found]++;
            } else if (file_n < 20) {
                files[file_n] = fp;
                file_counts[file_n] = 1;
                file_n++;
            }
        }
        for (int i = 0; i < label_n; i++) {
            yyjson_mut_obj_add_int(doc, by_label, labels[i], label_counts[i]);
        }
        for (int i = 0; i < file_n; i++) {
            yyjson_mut_obj_add_int(doc, by_file, files[i], file_counts[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "by_label", by_label);
        yyjson_mut_obj_add_val(doc, root, "by_file_top20", by_file);
        /* G1: make suppression explicit so callers know results exist */
        yyjson_mut_val *empty_arr = yyjson_mut_arr(doc);
        yyjson_mut_obj_add_val(doc, root, "results", empty_arr);
        yyjson_mut_obj_add_bool(doc, root, "results_suppressed", true);
        yyjson_mut_obj_add_str(doc, root, "hint",
            "mode='summary' returns counts only. Use mode='full' with compact=true for node records.");
    } else {
        /* Full mode: individual results */
        yyjson_mut_val *results = yyjson_mut_arr(doc);
        for (int i = 0; i < out.count; i++) {
            cbm_search_result_t *sr = &out.results[i];
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            if ((!compact || !ends_with_segment(sr->node.qualified_name, sr->node.name)) &&
                sr->node.name && sr->node.name[0]) {
                yyjson_mut_obj_add_str(doc, item, "name", sr->node.name);
            }
            yyjson_mut_obj_add_str(doc, item, "qualified_name",
                                   sr->node.qualified_name ? sr->node.qualified_name : "");
            if (sr->node.label && sr->node.label[0]) {
                yyjson_mut_obj_add_str(doc, item, "label", sr->node.label);
            }
            if (sr->node.file_path && sr->node.file_path[0]) {
                yyjson_mut_obj_add_str(doc, item, "file_path", sr->node.file_path);
            }
            if (sr->pagerank_score > 0.0) {
                add_pagerank_val(doc, item, sr->pagerank_score);
            } else {
                /* Degree fields only when PageRank not available — PR subsumes degree info.
                 * Zero degrees add no information; omit to save tokens. */
                if (sr->in_degree > 0)
                    yyjson_mut_obj_add_int(doc, item, "in_degree", sr->in_degree);
                if (sr->out_degree > 0)
                    yyjson_mut_obj_add_int(doc, item, "out_degree", sr->out_degree);
            }

            /* Unconditional source tagging — critical for AI grounding.
             * Every result tagged source:"project" or source:"dependency".
             * Dep results also get package name and read_only:true. */
            bool is_dep = cbm_is_dep_project(sr->node.project, srv->session_project);
            yyjson_mut_obj_add_str(doc, item, "source", is_dep ? "dependency" : "project");
            if (is_dep && sr->node.project) {
                /* Extract package name: find ".dep." and take everything after it.
                 * "myapp.dep.pandas" → "pandas", "myapp.dep.uv.pandas" → "uv.pandas" */
                const char *dep_sep = strstr(sr->node.project, CBM_DEP_SEPARATOR);
                if (dep_sep) {
                    const char *pkg = dep_sep + CBM_DEP_SEPARATOR_LEN;
                    yyjson_mut_obj_add_strcpy(doc, item, "package", pkg);
                }
                yyjson_mut_obj_add_bool(doc, item, "read_only", true);
            }

            yyjson_mut_arr_add_val(results, item);
        }
        yyjson_mut_obj_add_val(doc, root, "results", results);
        /* Pagination: tell the caller how to get the next page */
        bool more = out.total > offset + out.count;
        yyjson_mut_obj_add_bool(doc, root, "has_more", more);
        if (more) {
            char hint[128];
            snprintf(hint, sizeof(hint),
                     "Use offset:%d and limit:%d for next page (%d total)",
                     offset + out.count, limit, (int)out.total);
            yyjson_mut_obj_add_strcpy(doc, root, "pagination_hint", hint);
        }
    }

    /* When searching for dep projects returns nothing, explain why.
     * Heuristic: dep search if expanded value ends with ".dep" (from "dep"/"deps" shorthand)
     * or project_pattern contains ".dep." — both indicate a dependency project query. */
    if (out.total == 0) {
        bool is_dep_search = false;
        if (pe.mode == MATCH_PREFIX && pe.value) {
            size_t n = strlen(pe.value);
            is_dep_search = (n >= 4 && strcmp(pe.value + n - 4, ".dep") == 0);
        } else if (pe.mode == MATCH_GLOB && pe.value) {
            is_dep_search = (strstr(pe.value, ".dep.") != NULL ||
                             strstr(pe.value, ".dep%") != NULL);
        }
        if (is_dep_search) {
            /* Detect what build system is in use to give an actionable hint */
            cbm_pkg_manager_t eco = CBM_PKG_COUNT;
            if (srv->session_root[0])
                eco = cbm_detect_ecosystem(srv->session_root);
            char hint[1024];
            if (eco == CBM_PKG_COUNT) {
                snprintf(hint, sizeof(hint),
                    "No dependency sub-projects indexed, and no recognized build system "
                    "detected in '%s'. Supported: Python/uv (pyproject.toml, requirements.txt), "
                    "Rust/cargo, npm/bun (package.json), Go (go.mod), JVM/Maven/Gradle, "
                    ".NET/NuGet (*.csproj), Ruby/Bundler (Gemfile), PHP/Composer, "
                    "Swift/SPM, Dart/pub, Elixir/Mix, C-Make (Makefile), C-CMake, "
                    "C-Meson, C-Conan, or generic vendor/ directory. "
                    "Re-index after adding a manifest file.",
                    srv->session_root[0] ? srv->session_root : "(unknown project root)");
            } else {
                snprintf(hint, sizeof(hint),
                    "No dependency sub-projects indexed yet for %s build system '%s'. "
                    "Dep scanning runs automatically on index_repository. "
                    "If deps are vendored in vendor/ vendored/ third_party/ etc., "
                    "re-run index_repository(repo_path=\"%s\") to trigger dep discovery.",
                    cbm_pkg_manager_str(eco), cbm_pkg_manager_str(eco),
                    srv->session_root[0] ? srv->session_root : "<repo_path>");
            }
            yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_search_free(&out);

    free(pe.value);
    free(label);
    free(name_pattern);
    free(qn_pattern);
    free(file_pattern);
    free(relationship);
    free(search_mode);
    free(sort_by);
    free_string_array(exclude);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_query_graph(cbm_mcp_server_t *srv, const char *args) {
    /* B7: schema says "cypher" but handler read "query" — fix to read "cypher" first */
    char *query = cbm_mcp_get_string_arg(args, "cypher");
    if (!query) query = cbm_mcp_get_string_arg(args, "query"); /* backward compat */
    /* CQ-2: use resolve_project_store for "self"/"dep"/path expansion */
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    int max_rows = cbm_mcp_get_int_arg(args, "max_rows", 0);
    int cfg_max_output = cbm_config_get_int(srv->config, CBM_CONFIG_QUERY_MAX_OUTPUT_BYTES,
                                            CBM_DEFAULT_QUERY_MAX_OUTPUT_BYTES);
    int max_output_bytes = cbm_mcp_get_int_arg(args, "max_output_bytes", cfg_max_output);

    if (!query) {
        free(project);
        return cbm_mcp_text_result(
            "{\"error\":\"query is required\","
            "\"hint\":\"Pass a Cypher query string, e.g. MATCH (n:Function) RETURN n.name LIMIT 10\"}", true);
    }
    if (!store) {
        free(project);
        free(query);
        return cbm_mcp_text_result(
            "{\"error\":\"no project loaded\","
            "\"hint\":\"Run index_repository with repo_path to index the project first.\"}", true);
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

    /* CQ-3: Warn when filter params combined with cypher — they're silently ignored */
    {
        char *ignored_label = cbm_mcp_get_string_arg(args, "label");
        if (ignored_label) {
            yyjson_mut_obj_add_str(doc, root, "warning",
                "cypher param present — label, name_pattern, file_pattern, sort_by, and other "
                "filter params are ignored in Cypher mode. Use WHERE clause instead.");
            free(ignored_label);
        }
    }

    char *json = yy_doc_to_str(doc);
    int total_rows = result.row_count;
    yyjson_mut_doc_free(doc);
    cbm_cypher_result_free(&result);
    free(query);
    free(project);

    /* Output truncation: cap response at max_output_bytes */
    if (max_output_bytes > 0 && json) {
        size_t json_len = strlen(json);
        if (json_len > (size_t)max_output_bytes) {
            /* Build a truncated response with metadata */
            char trunc_json[256];
            snprintf(trunc_json, sizeof(trunc_json),
                     "{\"truncated\":true,\"total_bytes\":%lu,\"rows_returned\":%d,"
                     "\"hint\":\"Add LIMIT to your Cypher query\"}",
                     (unsigned long)json_len, total_rows);
            char *res = cbm_mcp_text_result(trunc_json, false);
            free(json);
            return res;
        }
    }

    char *res = cbm_mcp_text_result(json, false);
    free(json);
    return res;
}

static char *handle_index_status(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    if (project) {
        int nodes = cbm_store_count_nodes(store, project);
        int edges = cbm_store_count_edges(store, project);
        yyjson_mut_obj_add_str(doc, root, "project", project);
        yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
        yyjson_mut_obj_add_int(doc, root, "edges", edges);
        yyjson_mut_obj_add_str(doc, root, "status", nodes > 0 ? "ready" : "empty");

        /* Report indexed dependencies by searching for {project}.dep.% nodes.
         * Uses project_pattern for LIKE query to find all dep projects. */
        char dep_like[4096];
        snprintf(dep_like, sizeof(dep_like), "%s.dep.%%", project);
        cbm_search_params_t dep_params = {0};
        dep_params.project_pattern = dep_like;
        dep_params.limit = 100;
        cbm_search_output_t dep_out = {0};
        if (cbm_store_search(store, &dep_params, &dep_out) == 0) {
            /* Collect unique dep project names */
            if (dep_out.count > 0) {
                yyjson_mut_val *dep_arr = yyjson_mut_arr(doc);
                const char *last_dep_proj = "";
                int dep_count = 0;
                for (int i = 0; i < dep_out.count; i++) {
                    const char *proj = dep_out.results[i].node.project;
                    if (!proj || strcmp(proj, last_dep_proj) == 0) continue;
                    last_dep_proj = proj;
                    /* Extract package name from "myproj.dep.pandas" */
                    const char *dep_sep = strstr(proj, CBM_DEP_SEPARATOR);
                    if (!dep_sep) continue;
                    const char *pkg = dep_sep + CBM_DEP_SEPARATOR_LEN;
                    yyjson_mut_val *d = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, d, "package", pkg);
                    int dn = cbm_store_count_nodes(store, proj);
                    yyjson_mut_obj_add_int(doc, d, "nodes", dn);
                    yyjson_mut_arr_add_val(dep_arr, d);
                    dep_count++;
                }
                if (dep_count > 0) {
                    yyjson_mut_obj_add_val(doc, root, "dependencies", dep_arr);
                    yyjson_mut_obj_add_int(doc, root, "dependency_count", dep_count);
                }
            }
            /* Always free search results — cbm_store_search allocates even when count==0 */
            cbm_store_search_free(&dep_out);
        }

        /* Report detected ecosystem */
        cbm_project_t proj_info;
        if (cbm_store_get_project(store, project, &proj_info) == 0) {
            if (proj_info.root_path) {
                cbm_pkg_manager_t eco = cbm_detect_ecosystem(proj_info.root_path);
                if (eco != CBM_PKG_COUNT) {
                    yyjson_mut_obj_add_str(doc, root, "detected_ecosystem",
                                           cbm_pkg_manager_str(eco));
                }
            }
            /* Always free project fields — cbm_store_get_project heap-allocates strings */
            cbm_project_free_fields(&proj_info);
        }
        /* Report PageRank stats */
        {
            sqlite3 *db = cbm_store_get_db(store);
            if (db) {
                sqlite3_stmt *pr_stmt = NULL;
                const char *pr_sql = "SELECT COUNT(*), MAX(computed_at) "
                                     "FROM pagerank WHERE project = ?1";
                if (sqlite3_prepare_v2(db, pr_sql, -1, &pr_stmt, NULL) == SQLITE_OK) {
                    sqlite3_bind_text(pr_stmt, 1, project, -1, SQLITE_TRANSIENT);
                    if (sqlite3_step(pr_stmt) == SQLITE_ROW) {
                        int ranked = sqlite3_column_int(pr_stmt, 0);
                        if (ranked > 0) {
                            yyjson_mut_val *pr_obj = yyjson_mut_obj(doc);
                            yyjson_mut_obj_add_int(doc, pr_obj, "ranked_nodes", ranked);
                            const char *ts = (const char *)sqlite3_column_text(pr_stmt, 1);
                            if (ts)
                                yyjson_mut_obj_add_strcpy(doc, pr_obj, "computed_at", ts);
                            yyjson_mut_obj_add_val(doc, root, "pagerank", pr_obj);
                        }
                    }
                    sqlite3_finalize(pr_stmt);
                }
            }
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
    char *name = cbm_mcp_get_string_arg(args, "project_name");
    if (!name) {
        return cbm_mcp_text_result(
            "{\"error\":\"project_name is required\","
            "\"hint\":\"Pass the project name to delete. Use list_projects to see available projects.\"}", true);
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

    /* Delete the .db file + WAL/SHM */
    char path[1024];
    project_db_path(name, path, sizeof(path));

    char wal[1024];
    char shm[1024];
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);

    bool exists = (access(path, F_OK) == 0);
    const char *status = "not_found";
    if (exists) {
        (void)cbm_unlink(path);
        (void)cbm_unlink(wal);
        (void)cbm_unlink(shm);
        status = "deleted";
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "project", name);
    yyjson_mut_obj_add_str(doc, root, "status", status);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(name);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_get_architecture(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, project, &schema);

    int node_count = cbm_store_count_nodes(store, project);
    int edge_count = cbm_store_count_edges(store, project);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    if (project) {
        yyjson_mut_obj_add_str(doc, root, "project", project);
    }
    yyjson_mut_obj_add_int(doc, root, "total_nodes", node_count);
    yyjson_mut_obj_add_int(doc, root, "total_edges", edge_count);

    /* Node label summary */
    yyjson_mut_val *labels = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; i++) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, item, "count", schema.node_labels[i].count);
        yyjson_mut_arr_add_val(labels, item);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", labels);

    /* Edge type summary */
    yyjson_mut_val *types = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, item, "count", schema.edge_types[i].count);
        yyjson_mut_arr_add_val(types, item);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", types);

    /* Relationship patterns */
    if (schema.rel_pattern_count > 0) {
        yyjson_mut_val *pats = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            yyjson_mut_arr_add_str(doc, pats, schema.rel_patterns[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "relationship_patterns", pats);
    }

    /* Key functions: top 10 by PageRank with config + param exclude patterns */
    {
        sqlite3 *db = cbm_store_get_db(store);
        if (db) {
            int excl_count = 0;
            char **excl_arr = cbm_mcp_get_string_array_arg(args, "exclude", &excl_count);
            const char *excl_csv = srv->config
                ? cbm_config_get(srv->config, CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE, "")
                : "";
            int kf_limit = srv->config
                ? cbm_config_get_int(srv->config, CBM_CONFIG_KEY_FUNCTIONS_COUNT, 25)
                : 25;
            char *kf_sql_heap = build_key_functions_sql(excl_csv, (const char **)excl_arr, kf_limit);
            free_string_array(excl_arr);
            const char *kf_sql = kf_sql_heap;
            sqlite3_stmt *kf_stmt = NULL;
            if (sqlite3_prepare_v2(db, kf_sql, -1, &kf_stmt, NULL) == SQLITE_OK) {
                if (project) sqlite3_bind_text(kf_stmt, 1, project, -1, SQLITE_TRANSIENT);
                yyjson_mut_val *kf_arr = yyjson_mut_arr(doc);
                while (sqlite3_step(kf_stmt) == SQLITE_ROW) {
                    yyjson_mut_val *kf = yyjson_mut_obj(doc);
                    const char *n = (const char *)sqlite3_column_text(kf_stmt, 0);
                    const char *qn = (const char *)sqlite3_column_text(kf_stmt, 1);
                    const char *lbl = (const char *)sqlite3_column_text(kf_stmt, 2);
                    const char *fp = (const char *)sqlite3_column_text(kf_stmt, 3);
                    double rank = sqlite3_column_double(kf_stmt, 4);
                    if (n && !ends_with_segment(qn, n))
                        yyjson_mut_obj_add_strcpy(doc, kf, "name", n);
                    if (qn)  yyjson_mut_obj_add_strcpy(doc, kf, "qualified_name", qn);
                    if (lbl && lbl[0]) yyjson_mut_obj_add_strcpy(doc, kf, "label", lbl);
                    if (fp  && fp[0])  yyjson_mut_obj_add_strcpy(doc, kf, "file_path", fp);
                    add_pagerank_val(doc, kf, rank);
                    yyjson_mut_arr_add_val(kf_arr, kf);
                }
                sqlite3_finalize(kf_stmt);
                yyjson_mut_obj_add_val(doc, root, "key_functions", kf_arr);
            }
            free(kf_sql_heap);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_schema_free(&schema);
    free(project);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_trace_call_path(cbm_mcp_server_t *srv, const char *args) {
    char *func_name = cbm_mcp_get_string_arg(args, "function_name");
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value; /* take ownership for free() below */
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    int depth = cbm_mcp_get_int_arg(args, "depth", 3);
    /* F10: clamp depth to minimum 1 — O(1) */
    if (depth < 1) depth = 1;
    int cfg_trace_max = cbm_config_get_int(srv->config, CBM_CONFIG_TRACE_MAX_RESULTS,
                                            CBM_DEFAULT_TRACE_MAX_RESULTS);
    int max_results = cbm_mcp_get_int_arg(args, "max_results", cfg_trace_max);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", true);

    if (!func_name) {
        free(project);
        free(direction);
        return cbm_mcp_text_result(
            "{\"error\":\"function_name is required\","
            "\"hint\":\"Pass the name of a function to trace, e.g. {\\\"function_name\\\":\\\"main\\\"}\"}", true);
    }
    if (!store) {
        free(func_name);
        free(project);
        free(direction);
        return cbm_mcp_text_result(
            "{\"error\":\"no project loaded\","
            "\"hint\":\"Run index_repository with repo_path to index the project first.\"}", true);
    }
    /* F15: validate direction enum — O(1) */
    if (direction && strcmp(direction, "inbound") != 0 &&
        strcmp(direction, "outbound") != 0 && strcmp(direction, "both") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid direction '%s'\","
            "\"hint\":\"Valid values: inbound, outbound, both\"}", direction);
        free(func_name);
        free(project);
        free(direction);
        return cbm_mcp_text_result(errbuf, true);
    }
    if (!direction) {
        direction = heap_strdup("both");
    }

    /* Find the node by name */
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    cbm_store_find_nodes_by_name(store, project, func_name, &nodes, &node_count);

    if (node_count == 0) {
        char errbuf[512];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"function not found: '%s'\","
            "\"hint\":\"Use search_code_graph with name_pattern to find similar symbols.\"}", func_name);
        free(func_name);
        free(project);
        free(direction);
        cbm_store_free_nodes(nodes, 0);
        return cbm_mcp_text_result(errbuf, true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "function", func_name);
    yyjson_mut_obj_add_str(doc, root, "direction", direction);

    /* Report ambiguity when multiple nodes match the function name */
    if (node_count > 1) {
        yyjson_mut_val *candidates = yyjson_mut_arr(doc);
        for (int i = 0; i < node_count && i < 5; i++) {
            yyjson_mut_val *c = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, c, "qualified_name",
                                   nodes[i].qualified_name ? nodes[i].qualified_name : "");
            if (nodes[i].file_path && nodes[i].file_path[0])
                yyjson_mut_obj_add_str(doc, c, "file_path", nodes[i].file_path);
            yyjson_mut_arr_append(candidates, c);
        }
        yyjson_mut_obj_add_val(doc, root, "candidates", candidates);
        yyjson_mut_obj_add_str(doc, root, "resolved",
                               nodes[0].qualified_name ? nodes[0].qualified_name : "");
    }

    /* Extract edge_types here — after all early returns — to avoid memory leaks.
     * free_string_array(NULL) is NULL-safe (mcp.c:663). */
    int edge_type_count_user = 0;
    char **edge_types_user = cbm_mcp_get_string_array_arg(args, "edge_types",
                                                           &edge_type_count_user);
    /* Use user-supplied edge_types if provided, else default to CALLS only.
     * default_edge_types is stack-local; no ownership transfer needed. */
    const char *default_edge_types[] = {"CALLS"};
    const char **edge_types = (edge_type_count_user > 0)
        ? (const char **)edge_types_user
        : default_edge_types;
    int edge_type_count = (edge_type_count_user > 0) ? edge_type_count_user : 1;

    /* Run BFS for each requested direction.
     * IMPORTANT: yyjson_mut_obj_add_str borrows pointers — we must keep
     * traversal results alive until after yy_doc_to_str serialization. */
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool do_outbound = strcmp(direction, "outbound") == 0 || strcmp(direction, "both") == 0;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool do_inbound = strcmp(direction, "inbound") == 0 || strcmp(direction, "both") == 0;

    cbm_traverse_result_t tr_out = {0};
    cbm_traverse_result_t tr_in = {0};

    if (do_outbound) {
        cbm_store_bfs(store, nodes[0].id, "outbound", edge_types, edge_type_count, depth,
                      max_results, &tr_out);

        yyjson_mut_val *callees = yyjson_mut_arr(doc);
        /* Deduplicate by node ID to prevent cycle inflation */
        int64_t *seen_out = calloc((size_t)tr_out.visited_count + 1, sizeof(int64_t));
        int seen_out_n = 0;
        for (int i = 0; i < tr_out.visited_count; i++) {
            if (seen_out) { /* OOM-safe: skip dedup if calloc failed */
                bool dup = false;
                for (int j = 0; j < seen_out_n; j++) {
                    if (seen_out[j] == tr_out.visited[i].node.id) { dup = true; break; }
                }
                if (dup) continue;
                seen_out[seen_out_n++] = tr_out.visited[i].node.id;
            }
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            if ((!compact || !ends_with_segment(tr_out.visited[i].node.qualified_name,
                                                tr_out.visited[i].node.name)) &&
                tr_out.visited[i].node.name && tr_out.visited[i].node.name[0]) {
                yyjson_mut_obj_add_str(doc, item, "name", tr_out.visited[i].node.name);
            }
            yyjson_mut_obj_add_str(
                doc, item, "qualified_name",
                tr_out.visited[i].node.qualified_name ? tr_out.visited[i].node.qualified_name : "");
            yyjson_mut_obj_add_int(doc, item, "hop", tr_out.visited[i].hop);
            {
                double pr = cbm_pagerank_get(store, tr_out.visited[i].node.id);
                if (pr > 0.0)
                    add_pagerank_val(doc, item, pr);
            }
            /* Boundary tagging: mark if callee is in a dependency */
            bool callee_dep = cbm_is_dep_project(tr_out.visited[i].node.project,
                                                  srv->session_project);
            yyjson_mut_obj_add_str(doc, item, "source",
                                   callee_dep ? "dependency" : "project");
            if (callee_dep) {
                yyjson_mut_obj_add_bool(doc, item, "read_only", true);
            }
            yyjson_mut_arr_add_val(callees, item);
        }
        free(seen_out);
        yyjson_mut_obj_add_val(doc, root, "callees", callees);
        yyjson_mut_obj_add_int(doc, root, "callees_total", tr_out.visited_count);
    }

    if (do_inbound) {
        cbm_store_bfs(store, nodes[0].id, "inbound", edge_types, edge_type_count, depth,
                      max_results, &tr_in);

        yyjson_mut_val *callers = yyjson_mut_arr(doc);
        /* Deduplicate by node ID */
        int64_t *seen_in = calloc((size_t)tr_in.visited_count + 1, sizeof(int64_t));
        int seen_in_n = 0;
        for (int i = 0; i < tr_in.visited_count; i++) {
            if (seen_in) { /* OOM-safe: skip dedup if calloc failed */
                bool dup = false;
                for (int j = 0; j < seen_in_n; j++) {
                    if (seen_in[j] == tr_in.visited[i].node.id) { dup = true; break; }
                }
                if (dup) continue;
                seen_in[seen_in_n++] = tr_in.visited[i].node.id;
            }
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            if ((!compact || !ends_with_segment(tr_in.visited[i].node.qualified_name,
                                                tr_in.visited[i].node.name)) &&
                tr_in.visited[i].node.name && tr_in.visited[i].node.name[0]) {
                yyjson_mut_obj_add_str(doc, item, "name", tr_in.visited[i].node.name);
            }
            yyjson_mut_obj_add_str(
                doc, item, "qualified_name",
                tr_in.visited[i].node.qualified_name ? tr_in.visited[i].node.qualified_name : "");
            yyjson_mut_obj_add_int(doc, item, "hop", tr_in.visited[i].hop);
            {
                double pr = cbm_pagerank_get(store, tr_in.visited[i].node.id);
                if (pr > 0.0)
                    add_pagerank_val(doc, item, pr);
            }
            /* Boundary tagging: mark if caller is in a dependency */
            bool caller_dep = cbm_is_dep_project(tr_in.visited[i].node.project,
                                                  srv->session_project);
            yyjson_mut_obj_add_str(doc, item, "source",
                                   caller_dep ? "dependency" : "project");
            if (caller_dep) {
                yyjson_mut_obj_add_bool(doc, item, "read_only", true);
            }
            yyjson_mut_arr_add_val(callers, item);
        }
        free(seen_in);
        yyjson_mut_obj_add_val(doc, root, "callers", callers);
        yyjson_mut_obj_add_int(doc, root, "callers_total", tr_in.visited_count);
    }

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

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
    free_string_array(edge_types_user); /* NULL-safe; reuses existing helper (mcp.c:663) */

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── Helper: free heap fields of a stack-allocated node ────────── */

static void free_node_contents(cbm_node_t *n) {
    free((void *)n->project);
    free((void *)n->label);
    free((void *)n->name);
    free((void *)n->qualified_name);
    free((void *)n->file_path);
    free((void *)n->properties_json);
    memset(n, 0, sizeof(*n));
}

/* ── Helper: read lines [start, end] from a file ─────────────── */

static char *read_file_lines(const char *path, int start, int end) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        return NULL;
    }

    size_t cap = 4096;
    char *buf = malloc(cap);
    size_t len = 0;
    buf[0] = '\0';

    char line[2048];
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
        while (len + ll + 1 > cap) {
            cap *= 2;
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
    free((void *)proj.name);
    free((void *)proj.indexed_at);
    free((void *)proj.root_path);
    return root;
}

/* ── index_repository ─────────────────────────────────────────── */

static char *handle_index_repository(cbm_mcp_server_t *srv, const char *args) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");

    if (!repo_path) {
        free(mode_str);
        return cbm_mcp_text_result(
            "{\"error\":\"repo_path is required\","
            "\"hint\":\"Pass the absolute path to the project root directory.\"}", true);
    }

    cbm_index_mode_t mode = CBM_MODE_FULL;
    if (mode_str && strcmp(mode_str, "fast") == 0) {
        mode = CBM_MODE_FAST;
    }
    free(mode_str);

    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, NULL, mode);
    if (!p) {
        free(repo_path);
        return cbm_mcp_text_result(
            "{\"error\":\"failed to create indexing pipeline\","
            "\"hint\":\"Check that repo_path exists and is readable. The directory may be empty or inaccessible.\"}", true);
    }

    char *project_name = heap_strdup(cbm_pipeline_project_name(p));

    /* Pipeline builds everything in-memory, then dumps to file atomically.
     * No need to close srv->store — pipeline doesn't touch the open store. */
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);
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

    if (rc == 0) {
        cbm_store_t *store = resolve_store(srv, project_name);
        if (store) {
            /* Auto-detect ecosystem and index installed deps from fresh graph.
             * Queries manifest files already indexed by pipeline step 1. */
            int deps_reindexed = cbm_dep_auto_index(
                project_name, repo_path, store, CBM_DEFAULT_AUTO_DEP_LIMIT);

            /* Compute PageRank + LinkRank on full graph (project + deps).
             * Uses config-backed edge weights when config is available. */
            cbm_pagerank_compute_with_config(store, project_name, srv->config);
            /* Register project with watcher so future file changes trigger auto-reindex */
            if (srv->watcher)
                cbm_watcher_watch(srv->watcher, project_name, repo_path);

            int nodes = cbm_store_count_nodes(store, project_name);
            int edges = cbm_store_count_edges(store, project_name);
            yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
            yyjson_mut_obj_add_int(doc, root, "edges", edges);
            if (deps_reindexed > 0)
                yyjson_mut_obj_add_int(doc, root, "dependencies_indexed", deps_reindexed);

            cbm_pkg_manager_t eco = cbm_detect_ecosystem(repo_path);
            if (eco != CBM_PKG_COUNT)
                yyjson_mut_obj_add_str(doc, root, "detected_ecosystem",
                                       cbm_pkg_manager_str(eco));
        }
    }

    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    /* Notify resource-capable clients that graph data changed */
    if (rc == 0) notify_resources_updated(srv);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
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

    char msg[512];
    snprintf(msg, sizeof(msg),
             "%d matches found for \"%s\" — use a qualified_name "
             "from the suggestions to disambiguate",
             count, input);
    yyjson_mut_obj_add_str(doc, root, "message", msg);

    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_val *s = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, s, "qualified_name",
                               nodes[i].qualified_name ? nodes[i].qualified_name : "");
        if (nodes[i].name && nodes[i].name[0])
            yyjson_mut_obj_add_str(doc, s, "name", nodes[i].name);
        if (nodes[i].label && nodes[i].label[0])
            yyjson_mut_obj_add_str(doc, s, "label", nodes[i].label);
        if (nodes[i].file_path && nodes[i].file_path[0])
            yyjson_mut_obj_add_str(doc, s, "file_path", nodes[i].file_path);
        yyjson_mut_arr_append(arr, s);
    }
    yyjson_mut_obj_add_val(doc, root, "suggestions", arr);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Build an enriched snippet response for a resolved node. */
static char *build_snippet_response(cbm_mcp_server_t *srv, cbm_node_t *node,
                                    const char *match_method, bool include_neighbors,
                                    cbm_node_t *alternatives, int alt_count,
                                    int max_lines, const char *mode, bool compact) {
    char *root_path = get_project_root(srv, node->project);

    int start = node->start_line > 0 ? node->start_line : 1;
    int end = node->end_line > start ? node->end_line : start + SNIPPET_DEFAULT_LINES;
    int total_lines = end - start + 1;
    bool truncated = false;
    char *source = NULL;
    char *source_tail = NULL;

    /* Build absolute path (persists until free) */
    char *abs_path = NULL;
    if (root_path && node->file_path) {
        size_t apsz = strlen(root_path) + strlen(node->file_path) + 2;
        abs_path = malloc(apsz);
        snprintf(abs_path, apsz, "%s/%s", root_path, node->file_path);

        if (mode && strcmp(mode, "signature") == 0) {
            /* Signature mode: no source read — use properties only */
            truncated = true;
        } else if (mode && strcmp(mode, "head_tail") == 0 && max_lines > 0 &&
                   total_lines > max_lines) {
            /* Head+tail mode: read first 60% (signature/setup) and last 40%
             * (return/cleanup). Middle implementation detail is omitted. */
            int head_count = (max_lines * 60) / 100;
            int tail_count = max_lines - head_count;
            if (head_count < 1) head_count = 1;
            if (tail_count < 1) tail_count = 1;
            source = read_file_lines(abs_path, start, start + head_count - 1);
            source_tail = read_file_lines(abs_path, end - tail_count + 1, end);
            truncated = true;
        } else if (max_lines > 0 && total_lines > max_lines) {
            /* Full mode with truncation */
            end = start + max_lines - 1;
            source = read_file_lines(abs_path, start, end);
            truncated = true;
        } else {
            /* Full mode, no truncation needed */
            source = read_file_lines(abs_path, start, end);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    if (node->name && node->name[0] &&
        (!compact || !ends_with_segment(node->qualified_name, node->name)))
        yyjson_mut_obj_add_str(doc, root_obj, "name", node->name);
    yyjson_mut_obj_add_str(doc, root_obj, "qualified_name",
                           node->qualified_name ? node->qualified_name : "");
    if (node->label && node->label[0])
        yyjson_mut_obj_add_str(doc, root_obj, "label", node->label);

    const char *display_path = "";
    if (abs_path) {
        display_path = abs_path;
    } else if (node->file_path) {
        display_path = node->file_path;
    }
    if (display_path[0])
        yyjson_mut_obj_add_str(doc, root_obj, "file_path", display_path);
    yyjson_mut_obj_add_int(doc, root_obj, "start_line", start);
    yyjson_mut_obj_add_int(doc, root_obj, "end_line", end);

    if (mode && strcmp(mode, "signature") == 0) {
        /* Signature mode: source omitted; signature comes from properties below */
    } else if (mode && strcmp(mode, "head_tail") == 0 && source && source_tail) {
        /* Combine head + marker + tail */
        int omitted = total_lines - max_lines;
        char marker[128];
        snprintf(marker, sizeof(marker), "\n[... %d lines omitted ...]\n", omitted);
        size_t combined_sz = strlen(source) + strlen(marker) + strlen(source_tail) + 1;
        char *combined = malloc(combined_sz);
        if (combined) {
            snprintf(combined, combined_sz, "%s%s%s", source, marker, source_tail);
            yyjson_mut_obj_add_strcpy(doc, root_obj, "source", combined);
            free(combined);
        } else {
            /* OOM fallback: output head only */
            yyjson_mut_obj_add_str(doc, root_obj, "source", source);
        }
    } else if (source) {
        yyjson_mut_obj_add_str(doc, root_obj, "source", source);
    } else {
        yyjson_mut_obj_add_str(doc, root_obj, "source", "(source not available)");
    }

    /* Truncation metadata */
    if (truncated) {
        yyjson_mut_obj_add_bool(doc, root_obj, "truncated", true);
        yyjson_mut_obj_add_int(doc, root_obj, "total_lines", total_lines);
    }

    /* match_method — omitted for exact matches */
    if (match_method) {
        yyjson_mut_obj_add_str(doc, root_obj, "match_method", match_method);
    }

    /* Enrich with node properties.
     * props_doc is freed AFTER serialization since yyjson_mut_obj_add_str
     * stores pointers into it (zero-copy). */
    yyjson_doc *props_doc = NULL;
    if (node->properties_json && node->properties_json[0] != '\0') {
        props_doc = yyjson_read(node->properties_json, strlen(node->properties_json), 0);
        if (props_doc) {
            yyjson_val *props_root = yyjson_doc_get_root(props_doc);
            if (props_root && yyjson_is_obj(props_root)) {
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
                        const char *sv = yyjson_get_str(val);
                        if (sv && sv[0])
                            yyjson_mut_obj_add_str(doc, root_obj, k, sv);
                    } else if (yyjson_is_bool(val)) {
                        bool bv = yyjson_get_bool(val);
                        /* compact: omit false booleans (false = absent/default) */
                        if (!compact || bv)
                            yyjson_mut_obj_add_bool(doc, root_obj, k, bv);
                    } else if (yyjson_is_int(val)) {
                        int64_t iv = yyjson_get_int(val);
                        /* compact: omit zero integers (0 = absent/default) */
                        if (!compact || iv != 0)
                            yyjson_mut_obj_add_int(doc, root_obj, k, iv);
                    } else if (yyjson_is_real(val)) {
                        double rv = yyjson_get_real(val);
                        if (!compact || rv != 0.0)
                            yyjson_mut_obj_add_real(doc, root_obj, k, rv);
                    }
                }
            }
        }
    }

    /* Caller/callee counts — store already resolved by calling handler */
    cbm_store_t *store = srv->store;
    int in_deg = 0;
    int out_deg = 0;
    cbm_store_node_degree(store, node->id, &in_deg, &out_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callers", in_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callees", out_deg);

    /* Include neighbor names (opt-in).
     * Strings stored by yyjson reference — freed after serialization. */
    char **nb_callers = NULL;
    int nb_caller_count = 0;
    char **nb_callees = NULL;
    int nb_callee_count = 0;
    if (include_neighbors) {
        cbm_store_node_neighbor_names(store, node->id, 10, &nb_callers, &nb_caller_count,
                                      &nb_callees, &nb_callee_count);
        if (nb_caller_count > 0) {
            yyjson_mut_val *arr = yyjson_mut_arr(doc);
            for (int i = 0; i < nb_caller_count; i++) {
                yyjson_mut_arr_add_str(doc, arr, nb_callers[i]);
            }
            yyjson_mut_obj_add_val(doc, root_obj, "caller_names", arr);
        }
        if (nb_callee_count > 0) {
            yyjson_mut_val *arr = yyjson_mut_arr(doc);
            for (int i = 0; i < nb_callee_count; i++) {
                yyjson_mut_arr_add_str(doc, arr, nb_callees[i]);
            }
            yyjson_mut_obj_add_val(doc, root_obj, "callee_names", arr);
        }
    }

    /* Alternatives (when auto-resolved from ambiguous) */
    if (alternatives && alt_count > 0) {
        yyjson_mut_val *arr = yyjson_mut_arr(doc);
        for (int i = 0; i < alt_count; i++) {
            yyjson_mut_val *a = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, a, "qualified_name",
                                   alternatives[i].qualified_name ? alternatives[i].qualified_name
                                                                  : "");
            if (alternatives[i].file_path && alternatives[i].file_path[0])
                yyjson_mut_obj_add_str(doc, a, "file_path", alternatives[i].file_path);
            yyjson_mut_arr_append(arr, a);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "alternatives", arr);
    }

    /* Provenance tagging: mark if snippet is from a dependency */
    bool snippet_dep = cbm_is_dep_project(node->project, srv->session_project);
    yyjson_mut_obj_add_str(doc, root_obj, "source",
                           snippet_dep ? "dependency" : "project");
    if (snippet_dep) {
        yyjson_mut_obj_add_bool(doc, root_obj, "read_only", true);
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
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(nb_callers);
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(nb_callees);
    free(root_path);
    free(abs_path);
    free(source);
    free(source_tail);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_get_code_snippet(cbm_mcp_server_t *srv, const char *args) {
    char *qn = cbm_mcp_get_string_arg(args, "qualified_name");
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    /* When no project param given, try to parse the project prefix from the
     * qualified name by checking for a matching .db file.  This is Option C:
     * the QN is self-describing, so we can always open the right store even on
     * a cold start (no prior search_code_graph call).
     * Falls back to the currently-open store's project as a secondary option. */
    const char *eff_project = project;
    if (!eff_project && qn) {
        /* Option C: QN is self-describing — try to find the project prefix by
         * checking for a matching .db file.  assign into project so the
         * existing free(project) calls at every exit path own the memory. */
        project = extract_project_from_qn(qn);
        if (project) {
            eff_project = project;
            store = resolve_store(srv, project); /* open the correct DB */
        } else if (srv->current_project && srv->current_project[0]) {
            eff_project = srv->current_project; /* fallback: last-used project */
        }
    }
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", true);
    bool auto_resolve = cbm_mcp_get_bool_arg(args, "auto_resolve");
    bool include_neighbors = cbm_mcp_get_bool_arg(args, "include_neighbors");
    int cfg_max_lines = cbm_config_get_int(srv->config, CBM_CONFIG_SNIPPET_MAX_LINES,
                                           CBM_DEFAULT_SNIPPET_MAX_LINES);
    int max_lines = cbm_mcp_get_int_arg(args, "max_lines", cfg_max_lines);
    char *snippet_mode = cbm_mcp_get_string_arg(args, "mode");

    if (!qn) {
        free(project);
        free(snippet_mode);
        return cbm_mcp_text_result(
            "{\"error\":\"qualified_name is required\","
            "\"hint\":\"Pass a symbol qualified name, e.g. {\\\"qualified_name\\\":\\\"myapp.src.main.handle_request\\\"}. "
            "Use search_code_graph to find qualified names.\"}", true);
    }
    if (!store) {
        free(qn);
        free(project);
        free(snippet_mode);
        return cbm_mcp_text_result(
            "{\"error\":\"no project loaded\","
            "\"hint\":\"Run index_repository with repo_path to index the project first.\"}", true);
    }

    /* Tier 1: Exact QN match */
    cbm_node_t node = {0};
    int rc = cbm_store_find_node_by_qn(store, eff_project, qn, &node);
    if (rc == CBM_STORE_OK) {
        char *result =
            build_snippet_response(srv, &node, NULL /*exact*/, include_neighbors, NULL, 0,
                                      max_lines, snippet_mode, compact);
        free_node_contents(&node);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    /* Tier 2: QN suffix match */
    cbm_node_t *suffix_nodes = NULL;
    int suffix_count = 0;
    cbm_store_find_nodes_by_qn_suffix(store, eff_project, qn, &suffix_nodes, &suffix_count);
    if (suffix_count == 1) {
        copy_node(&suffix_nodes[0], &node);
        cbm_store_free_nodes(suffix_nodes, suffix_count);
        char *result = build_snippet_response(srv, &node, "suffix", include_neighbors, NULL, 0,
                                                     max_lines, snippet_mode, compact);
        free_node_contents(&node);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    /* Tier 3: Short name match */
    cbm_node_t *name_nodes = NULL;
    int name_count = 0;
    cbm_store_find_nodes_by_name(store, eff_project, qn, &name_nodes, &name_count);
    if (name_count == 1) {
        copy_node(&name_nodes[0], &node);
        cbm_store_free_nodes(name_nodes, name_count);
        cbm_store_free_nodes(suffix_nodes, suffix_count);
        char *result = build_snippet_response(srv, &node, "name", include_neighbors, NULL, 0,
                                                     max_lines, snippet_mode, compact);
        free_node_contents(&node);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    /* Ambiguous: collect candidates from suffix + name tiers (dedup by id) */
    int total_cand = suffix_count + name_count;
    if (total_cand > 0) {
        /* Dedup by node ID */
        cbm_node_t *candidates = calloc((size_t)total_cand, sizeof(cbm_node_t));
        int cand_count = 0;

        for (int i = 0; i < suffix_count; i++) {
            copy_node(&suffix_nodes[i], &candidates[cand_count++]);
        }
        for (int i = 0; i < name_count; i++) {
            bool dup = false;
            for (int j = 0; j < cand_count; j++) {
                if (candidates[j].id == name_nodes[i].id) {
                    dup = true;
                    break;
                }
            }
            if (!dup) {
                copy_node(&name_nodes[i], &candidates[cand_count++]);
            }
        }

        cbm_store_free_nodes(suffix_nodes, suffix_count);
        cbm_store_free_nodes(name_nodes, name_count);

        /* Single candidate after dedup — resolve immediately, not ambiguous */
        if (cand_count == 1) {
            copy_node(&candidates[0], &node);
            free_node_contents(&candidates[0]);
            free(candidates);
            char *result = build_snippet_response(srv, &node, "name", include_neighbors, NULL, 0,
                                                         max_lines, snippet_mode, compact);
            free_node_contents(&node);
            free(qn);
            free(project);
            free(snippet_mode);
            return result;
        }

        /* Auto-resolve: pick best candidate by degree when 2+ candidates */
        if (auto_resolve && cand_count >= 2) {
            /* Find best: highest total degree, prefer non-test files */
            int best_idx = 0;
            int best_deg = -1;
            bool best_is_test = false;
            for (int i = 0; i < cand_count; i++) {
                int in_d = 0;
                int out_d = 0;
                cbm_store_node_degree(store, candidates[i].id, &in_d, &out_d);
                int deg = in_d + out_d;
                bool is_test =
                    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
                    candidates[i].file_path && strstr(candidates[i].file_path, "_test") != NULL;
                if (i == 0 || (best_is_test && !is_test) ||
                    (!best_is_test == !is_test && deg > best_deg) ||
                    (!best_is_test == !is_test && deg == best_deg && candidates[i].qualified_name &&
                     best_idx >= 0 && candidates[best_idx].qualified_name &&
                     strcmp(candidates[i].qualified_name, candidates[best_idx].qualified_name) <
                         0)) {
                    best_idx = i;
                    best_deg = deg;
                    best_is_test = is_test;
                }
            }

            copy_node(&candidates[best_idx], &node);

            /* Build alternatives list (skip the picked one) */
            cbm_node_t *alts = calloc((size_t)(cand_count - 1), sizeof(cbm_node_t));
            int alt_count = 0;
            for (int i = 0; i < cand_count; i++) {
                if (i != best_idx) {
                    copy_node(&candidates[i], &alts[alt_count++]);
                }
            }

            for (int i = 0; i < cand_count; i++) {
                free_node_contents(&candidates[i]);
            }
            free(candidates);

            char *result =
                build_snippet_response(srv, &node, "auto_best", include_neighbors, alts, alt_count,
                                          max_lines, snippet_mode, compact);
            free_node_contents(&node);
            for (int i = 0; i < alt_count; i++) {
                free_node_contents(&alts[i]);
            }
            free(alts);
            free(qn);
            free(project);
            free(snippet_mode);
            return result;
        }

        /* Return suggestions */
        char *result = snippet_suggestions(qn, candidates, cand_count);
        for (int i = 0; i < cand_count; i++) {
            free_node_contents(&candidates[i]);
        }
        free(candidates);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }

    cbm_store_free_nodes(suffix_nodes, suffix_count);
    cbm_store_free_nodes(name_nodes, name_count);

    /* Tier 4: Fuzzy — try last segment for name-based search */
    const char *dot = strrchr(qn, '.');
    const char *search_name = dot ? dot + 1 : qn;

    /* Use search with name pattern for fuzzy matching */
    cbm_search_params_t params = {0};
    params.project = eff_project;
    params.name_pattern = search_name;
    params.limit = 5;
    params.min_degree = -1;
    params.max_degree = -1;
    const char *excl[] = {"Community", NULL};
    params.exclude_labels = excl;

    cbm_search_output_t search_out = {0};
    if (cbm_store_search(store, &params, &search_out) == CBM_STORE_OK && search_out.count > 0) {
        /* Build suggestions from search results */
        cbm_node_t *fuzzy = calloc((size_t)search_out.count, sizeof(cbm_node_t));
        for (int i = 0; i < search_out.count; i++) {
            copy_node(&search_out.results[i].node, &fuzzy[i]);
        }
        int fuzzy_count = search_out.count;
        cbm_store_search_free(&search_out);

        /* Single fuzzy result — resolve immediately rather than reporting ambiguous */
        if (fuzzy_count == 1) {
            copy_node(&fuzzy[0], &node);
            free_node_contents(&fuzzy[0]);
            free(fuzzy);
            char *result = build_snippet_response(srv, &node, "fuzzy", include_neighbors, NULL, 0,
                                                         max_lines, snippet_mode, compact);
            free_node_contents(&node);
            free(qn);
            free(project);
            free(snippet_mode);
            return result;
        }

        char *result = snippet_suggestions(qn, fuzzy, fuzzy_count);
        for (int i = 0; i < fuzzy_count; i++) {
            free_node_contents(&fuzzy[i]);
        }
        free(fuzzy);
        free(qn);
        free(project);
        free(snippet_mode);
        return result;
    }
    cbm_store_search_free(&search_out);

    /* Nothing found */
    {
        char errbuf[512];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"symbol not found: '%s'\","
            "\"hint\":\"Use search_code_graph with name_pattern to find the correct qualified_name.\"}", qn);
        free(qn);
        free(project);
        free(snippet_mode);
        return cbm_mcp_text_result(errbuf, true);
    }
}

/* ── search_code ──────────────────────────────────────────────── */

static char *handle_search_code(cbm_mcp_server_t *srv, const char *args) {
    char *pattern = cbm_mcp_get_string_arg(args, "pattern");
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    int cfg_search_limit_sc = cbm_config_get_int(srv->config, CBM_CONFIG_SEARCH_LIMIT,
                                                   CBM_DEFAULT_SEARCH_LIMIT);
    int limit = cbm_mcp_get_int_arg(args, "limit", cfg_search_limit_sc);
    bool use_regex = cbm_mcp_get_bool_arg(args, "regex");

    if (!pattern) {
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"pattern is required\","
            "\"hint\":\"Pass a text pattern or regex (with regex:true) to search source code.\"}", true);
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"project not found or not indexed\","
            "\"hint\":\"Run index_repository with repo_path to index the project first, "
            "or use list_projects to see available projects.\"}", true);
    }

    /* Write pattern to temp file to avoid shell injection */
    char tmpfile[256];
#ifdef _WIN32
    snprintf(tmpfile, sizeof(tmpfile), "/tmp/cbm_search_%d.pat", (int)_getpid());
#else
    snprintf(tmpfile, sizeof(tmpfile), "/tmp/cbm_search_%d.pat", (int)getpid());
#endif
    FILE *tf = fopen(tmpfile, "w");
    if (!tf) {
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"search failed: could not create temp file\","
            "\"hint\":\"Check that /tmp is writable and has disk space.\"}", true);
    }
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    (void)fprintf(tf, "%s\n", pattern);
    (void)fclose(tf);

    char cmd[4096];
    /* Case-sensitivity: default case-insensitive, opt-in sensitive. */
    bool case_sensitive = cbm_mcp_get_bool_arg(args, "case_sensitive");
    const char *flag;
    if (use_regex) {
        flag = case_sensitive ? "-E" : "-Ei";
    } else {
        flag = case_sensitive ? "-F" : "-Fi";
    }
    /* Use a generous -m limit to avoid early termination on repos with
     * many files. The actual result limit is enforced in post-processing.
     * Old limit*3 was too small — grep stops after N total matches across
     * ALL files, so alphabetically early directories exhaust the limit. */
    int grep_limit = limit * 50;
    if (grep_limit < 500) grep_limit = 500;
    if (file_pattern) {
        snprintf(cmd, sizeof(cmd), "grep -rn %s --include='%s' -m %d -f '%s' '%s' 2>/dev/null",
                 flag, file_pattern, grep_limit, tmpfile, root_path);
    } else {
        snprintf(cmd, sizeof(cmd), "grep -rn %s -m %d -f '%s' '%s' 2>/dev/null", flag, grep_limit,
                 tmpfile, root_path);
    }

    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        cbm_unlink(tmpfile);
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"search failed: grep command could not execute\","
            "\"hint\":\"Check that grep is installed and the project root directory exists.\"}", true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    yyjson_mut_val *matches = yyjson_mut_arr(doc);
    char line[2048];
    int count = 0;
    size_t root_len = strlen(root_path);

    while (fgets(line, sizeof(line), fp) && count < limit) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        /* grep output: /abs/path/file:lineno:content */
        char *colon1 = strchr(line, ':');
        if (!colon1) {
            continue;
        }
        char *colon2 = strchr(colon1 + 1, ':');
        if (!colon2) {
            continue;
        }

        *colon1 = '\0';
        *colon2 = '\0';

        /* Strip root_path prefix to get relative path */
        const char *file = line;
        if (strncmp(file, root_path, root_len) == 0) {
            file += root_len;
            if (*file == '/') {
                file++;
            }
        }
        int lineno = (int)strtol(colon1 + 1, NULL, 10);
        const char *content = colon2 + 1;

        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "file", file);
        yyjson_mut_obj_add_int(doc, item, "line", lineno);
        yyjson_mut_obj_add_str(doc, item, "content", content);
        yyjson_mut_arr_add_val(matches, item);
        count++;
    }
    cbm_pclose(fp);
    cbm_unlink(tmpfile); /* Clean up pattern file after grep is done */

    yyjson_mut_obj_add_val(doc, root_obj, "matches", matches);
    yyjson_mut_obj_add_int(doc, root_obj, "count", count);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(root_path);
    free(pattern);
    free(project);
    free(file_pattern);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── detect_changes ───────────────────────────────────────────── */

static char *handle_detect_changes(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *base_branch = cbm_mcp_get_string_arg(args, "base_branch");
    int depth = cbm_mcp_get_int_arg(args, "depth", 2);

    if (!base_branch) {
        base_branch = heap_strdup("main");
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(project);
        free(base_branch);
        return cbm_mcp_text_result(
            "{\"error\":\"project not found\","
            "\"hint\":\"Run index_repository with repo_path to index the project first, "
            "or use list_projects to see available projects.\"}", true);
    }

    /* Get changed files via git */
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && { git diff --name-only '%s'...HEAD 2>/dev/null; "
             "git diff --name-only 2>/dev/null; } | sort -u",
             root_path, base_branch);

    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        free(root_path);
        free(project);
        free(base_branch);
        return cbm_mcp_text_result(
            "{\"error\":\"git diff failed\","
            "\"hint\":\"Check that git is installed and the project is a git repository.\"}", true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    yyjson_mut_val *changed = yyjson_mut_arr(doc);
    yyjson_mut_val *impacted = yyjson_mut_arr(doc);

    /* resolve_store already called via get_project_root above */
    cbm_store_t *store = srv->store;

    char line[1024];
    int file_count = 0;

    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        yyjson_mut_arr_add_str(doc, changed, line);
        file_count++;

        /* Find symbols defined in this file */
        cbm_node_t *nodes = NULL;
        int ncount = 0;
        cbm_store_find_nodes_by_file(store, project, line, &nodes, &ncount);

        for (int i = 0; i < ncount; i++) {
            if (nodes[i].label && strcmp(nodes[i].label, "File") != 0 &&
                strcmp(nodes[i].label, "Folder") != 0 && strcmp(nodes[i].label, "Project") != 0) {
                yyjson_mut_val *item = yyjson_mut_obj(doc);
                if (nodes[i].name && nodes[i].name[0])
                    yyjson_mut_obj_add_str(doc, item, "name", nodes[i].name);
                yyjson_mut_obj_add_str(doc, item, "label", nodes[i].label);
                yyjson_mut_obj_add_str(doc, item, "file", line);
                yyjson_mut_arr_add_val(impacted, item);
            }
        }
        cbm_store_free_nodes(nodes, ncount);
    }
    cbm_pclose(fp);

    yyjson_mut_obj_add_val(doc, root_obj, "changed_files", changed);
    yyjson_mut_obj_add_int(doc, root_obj, "changed_count", file_count);
    yyjson_mut_obj_add_val(doc, root_obj, "impacted_symbols", impacted);
    yyjson_mut_obj_add_int(doc, root_obj, "depth", depth);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(root_path);
    free(project);
    free(base_branch);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── manage_adr ───────────────────────────────────────────────── */

static char *handle_manage_adr(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *content = cbm_mcp_get_string_arg(args, "content");

    if (!mode_str) {
        mode_str = heap_strdup("get");
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(project);
        free(mode_str);
        free(content);
        return cbm_mcp_text_result(
            "{\"error\":\"project not found\","
            "\"hint\":\"Run index_repository with repo_path to index the project first, "
            "or use list_projects to see available projects.\"}", true);
    }

    char adr_dir[4096];
    snprintf(adr_dir, sizeof(adr_dir), "%s/.codebase-memory", root_path);
    char adr_path[4096];
    snprintf(adr_path, sizeof(adr_path), "%s/adr.md", adr_dir);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    if (strcmp(mode_str, "update") == 0 && content) {
        /* Create dir if needed */
        cbm_mkdir(adr_dir);
        FILE *fp = fopen(adr_path, "w");
        if (fp) {
            (void)fputs(content, fp);
            (void)fclose(fp);
            yyjson_mut_obj_add_str(doc, root_obj, "status", "updated");
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "write_error");
        }
    } else if (strcmp(mode_str, "sections") == 0) {
        /* List section headers from ADR */
        FILE *fp = fopen(adr_path, "r");
        yyjson_mut_val *sections = yyjson_mut_arr(doc);
        if (fp) {
            char line[1024];
            while (fgets(line, sizeof(line), fp)) {
                if (line[0] == '#') {
                    size_t len = strlen(line);
                    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
                        line[--len] = '\0';
                    }
                    yyjson_mut_arr_add_str(doc, sections, line);
                }
            }
            (void)fclose(fp);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "sections", sections);
    } else {
        /* get: read ADR content */
        FILE *fp = fopen(adr_path, "r");
        if (fp) {
            (void)fseek(fp, 0, SEEK_END);
            long sz = ftell(fp);
            (void)fseek(fp, 0, SEEK_SET);
            char *buf = malloc(sz + 1);
            size_t n = fread(buf, 1, sz, fp);
            buf[n] = '\0';
            (void)fclose(fp);
            yyjson_mut_obj_add_str(doc, root_obj, "content", buf);
            free(buf);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "content", "");
            yyjson_mut_obj_add_str(doc, root_obj, "status", "no_adr");
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(root_path);
    free(project);
    free(mode_str);
    free(content);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── ingest_traces ────────────────────────────────────────────── */

static char *handle_ingest_traces(cbm_mcp_server_t *srv, const char *args) {
    (void)srv;
    /* Parse traces array from JSON args */
    yyjson_doc *adoc = yyjson_read(args, strlen(args), 0);
    int trace_count = 0;

    if (adoc) {
        yyjson_val *aroot = yyjson_doc_get_root(adoc);
        yyjson_val *traces = yyjson_obj_get(aroot, "traces");
        if (traces && yyjson_is_arr(traces)) {
            trace_count = (int)yyjson_arr_size(traces);
        }
        yyjson_doc_free(adoc);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "status", "accepted");
    yyjson_mut_obj_add_int(doc, root, "traces_received", trace_count);
    yyjson_mut_obj_add_str(doc, root, "note",
                           "Runtime edge creation from traces not yet implemented");

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── index_dependencies ───────────────────────────────────────── */

static char *handle_index_dependencies(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    char *pkg_mgr_str = cbm_mcp_get_string_arg(args, "package_manager");

    if (!raw_project) {
        free(pkg_mgr_str);
        return cbm_mcp_text_result("{\"error\":\"project is required\"}", true);
    }

    /* Parse packages[] array */
    yyjson_doc *doc_args = yyjson_read(args, strlen(args), 0);
    yyjson_val *root_args = yyjson_doc_get_root(doc_args);
    yyjson_val *packages_val = yyjson_obj_get(root_args, "packages");
    yyjson_val *source_paths_val = yyjson_obj_get(root_args, "source_paths");

    if (!packages_val || !yyjson_is_arr(packages_val) || yyjson_arr_size(packages_val) == 0) {
        yyjson_doc_free(doc_args);
        free(raw_project);
        free(pkg_mgr_str);
        return cbm_mcp_text_result(
            "{\"error\":\"packages[] is required\"}", true);
    }

    bool has_paths = source_paths_val && yyjson_is_arr(source_paths_val);
    bool has_mgr = pkg_mgr_str != NULL;
    if (!has_paths && !has_mgr) {
        yyjson_doc_free(doc_args);
        free(raw_project);
        free(pkg_mgr_str);
        return cbm_mcp_text_result(
            "{\"error\":\"Either source_paths[] or package_manager is required\"}", true);
    }

    /* DRY: expand "self"/"dep"/path shortcuts */
    project_expand_t pe = {0};
    (void)resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value ? pe.value : raw_project;
    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        yyjson_doc_free(doc_args);
        free(project);
        free(pkg_mgr_str);
        return cbm_mcp_text_result(
            "{\"error\":\"no project loaded\","
            "\"hint\":\"Run index_repository with repo_path first.\"}", true);
    }

    cbm_pkg_manager_t mgr = has_mgr ? cbm_parse_pkg_manager(pkg_mgr_str) : CBM_PKG_CUSTOM;

    /* Get project root for package_manager resolution */
    char *root_path = NULL;
    if (has_mgr) {
        cbm_project_t proj_info;
        if (cbm_store_get_project(store, project, &proj_info) == 0) {
            root_path = heap_strdup(proj_info.root_path);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "ok");
    yyjson_mut_val *pkg_results = yyjson_mut_arr(doc);

    size_t pkg_count = yyjson_arr_size(packages_val);
    for (size_t i = 0; i < pkg_count; i++) {
        yyjson_val *pkg_val = yyjson_arr_get(packages_val, i);
        const char *pkg_name = yyjson_get_str(pkg_val);
        if (!pkg_name) continue;

        yyjson_mut_val *pr = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, pr, "name", pkg_name);

        /* Resolve source directory */
        const char *source_dir = NULL;
        char *resolved_path = NULL;

        if (has_paths && i < yyjson_arr_size(source_paths_val)) {
            yyjson_val *sp = yyjson_arr_get(source_paths_val, i);
            source_dir = yyjson_get_str(sp);
        } else if (has_mgr && root_path) {
            cbm_dep_resolved_t resolved = {0};
            if (cbm_resolve_pkg_source(mgr, pkg_name, root_path, &resolved) == 0) {
                resolved_path = heap_strdup(resolved.path);
                source_dir = resolved_path;
                if (resolved.version)
                    yyjson_mut_obj_add_str(doc, pr, "version", resolved.version);
                cbm_dep_resolved_free(&resolved);
            }
        }

        if (!source_dir || access(source_dir, F_OK) != 0) {
            yyjson_mut_obj_add_str(doc, pr, "status", "not_found");
            yyjson_mut_obj_add_str(doc, pr, "hint",
                "Use source_paths[] with the directory containing dep source.");
            yyjson_mut_arr_append(pkg_results, pr);
            free(resolved_path);
            continue;
        }

        /* Run pipeline: flush dep into project db */
        char *dep_proj = cbm_dep_project_name(project, pkg_name);
        cbm_pipeline_t *dp = cbm_pipeline_new(source_dir, NULL, CBM_MODE_DEP);
        if (dp) {
            cbm_pipeline_set_project_name(dp, dep_proj);
            cbm_pipeline_set_flush_store(dp, store);
            int rc = cbm_pipeline_run(dp);
            cbm_pipeline_free(dp);

            if (rc == 0) {
                int nodes = cbm_store_count_nodes(store, dep_proj);
                int edges = cbm_store_count_edges(store, dep_proj);
                yyjson_mut_obj_add_str(doc, pr, "status", "indexed");
                yyjson_mut_obj_add_int(doc, pr, "nodes", nodes);
                yyjson_mut_obj_add_int(doc, pr, "edges", edges);
            } else {
                yyjson_mut_obj_add_str(doc, pr, "status", "index_failed");
            }
        } else {
            yyjson_mut_obj_add_str(doc, pr, "status", "pipeline_failed");
            yyjson_mut_obj_add_str(doc, pr, "hint", "Out of memory or invalid source path.");
        }
        free(dep_proj);
        free(resolved_path);
        yyjson_mut_arr_append(pkg_results, pr);
    }

    yyjson_mut_obj_add_val(doc, root, "packages", pkg_results);
    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    /* Recompute PageRank after adding dep nodes so relevance sort includes them */
    cbm_pagerank_compute_default(store, project);

    /* Notify resource-capable clients that graph data changed */
    notify_resources_updated(srv);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(doc_args);
    free(project);
    free(pkg_mgr_str);
    free(root_path);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── Tool dispatch ────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_mcp_handle_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    if (!tool_name) {
        return cbm_mcp_text_result(
            "{\"error\":\"missing tool name\","
            "\"hint\":\"Available tools: search_code_graph, trace_call_path, get_code. "
            "Use tools/list to see all available tools.\"}", true);
    }

    /* Phase 9: consolidated tool names (streamlined mode) */
    if (strcmp(tool_name, "search_code_graph") == 0) {
        /* Check if cypher param is present → route to query_graph handler */
        char *cypher = cbm_mcp_get_string_arg(args_json, "cypher");
        if (cypher) {
            free(cypher);
            return handle_query_graph(srv, args_json);
        }
        return handle_search_graph(srv, args_json);
    }
    if (strcmp(tool_name, "get_code") == 0) {
        return handle_get_code_snippet(srv, args_json);
    }

    /* Original tool names (classic mode or individually enabled) */
    if (strcmp(tool_name, "list_projects") == 0) {
        return handle_list_projects(srv, args_json);
    }
    if (strcmp(tool_name, "get_graph_schema") == 0) {
        return handle_get_graph_schema(srv, args_json);
    }
    if (strcmp(tool_name, "search_graph") == 0) {
        return handle_search_graph(srv, args_json);
    }
    if (strcmp(tool_name, "query_graph") == 0) {
        return handle_query_graph(srv, args_json);
    }
    if (strcmp(tool_name, "index_status") == 0) {
        return handle_index_status(srv, args_json);
    }
    if (strcmp(tool_name, "delete_project") == 0) {
        return handle_delete_project(srv, args_json);
    }
    if (strcmp(tool_name, "trace_call_path") == 0) {
        return handle_trace_call_path(srv, args_json);
    }
    if (strcmp(tool_name, "get_architecture") == 0) {
        return handle_get_architecture(srv, args_json);
    }

    /* Pipeline-dependent tools */
    if (strcmp(tool_name, "index_repository") == 0) {
        return handle_index_repository(srv, args_json);
    }
    if (strcmp(tool_name, "get_code_snippet") == 0) {
        return handle_get_code_snippet(srv, args_json);
    }
    if (strcmp(tool_name, "search_code") == 0) {
        return handle_search_code(srv, args_json);
    }
    if (strcmp(tool_name, "detect_changes") == 0) {
        return handle_detect_changes(srv, args_json);
    }
    if (strcmp(tool_name, "manage_adr") == 0) {
        return handle_manage_adr(srv, args_json);
    }
    if (strcmp(tool_name, "ingest_traces") == 0) {
        return handle_ingest_traces(srv, args_json);
    }
    if (strcmp(tool_name, "index_dependencies") == 0) {
        return handle_index_dependencies(srv, args_json);
    }

    /* _hidden_tools: informational pseudo-tool for progressive disclosure */
    if (strcmp(tool_name, "_hidden_tools") == 0) {
        return cbm_mcp_text_result(
            "{\"hidden_tools\":[\"index_repository\",\"search_graph\",\"query_graph\","
            "\"get_code_snippet\",\"get_graph_schema\",\"get_architecture\",\"search_code\","
            "\"list_projects\",\"delete_project\",\"index_status\",\"detect_changes\","
            "\"manage_adr\",\"ingest_traces\",\"index_dependencies\"],"
            "\"enable_all\":\"set env CBM_TOOL_MODE=classic or config set tool_mode classic\","
            "\"enable_one\":\"config set tool_<name> true (e.g. tool_index_repository true)\","
            "\"resources\":[\"codebase://schema\",\"codebase://architecture\",\"codebase://status\"]}", false);
    }

    char msg[512];
    snprintf(msg, sizeof(msg),
        "{\"error\":\"unknown tool: '%s'\","
        "\"hint\":\"Available tools: search_code_graph, trace_call_path, get_code. "
        "Use tools/list to see all available tools.\"}", tool_name);
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
    char cwd[1024];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        // NOLINTNEXTLINE(concurrency-mt-unsafe)
        const char *home = getenv("HOME");
        /* Skip useless roots: / and $HOME */
        if (strcmp(cwd, "/") != 0 && (home == NULL || strcmp(cwd, home) != 0)) {
            snprintf(srv->session_root, sizeof(srv->session_root), "%s", cwd);
            cbm_log_info("session.root.cwd", "path", cwd);
        }
    }

    /* Derive project name from path — MUST match cbm_project_name_from_path()
     * (fqn.c:168) which the pipeline uses for db file naming and node project column.
     * Previous code used "last 2 segments" convention which produced different names,
     * breaking expand_project_param() and maybe_auto_index db file checks. */
    if (srv->session_root[0]) {
        char *name = cbm_project_name_from_path(srv->session_root);
        snprintf(srv->session_project, sizeof(srv->session_project), "%s", name);
        free(name);
    }

    /* Validate derived project name — don't create dbs for empty/dot names */
    if (srv->session_project[0] == '\0' ||
        strcmp(srv->session_project, ".") == 0 ||
        strcmp(srv->session_project, "..") == 0) {
        cbm_log_warn("session.invalid_name", "derived", srv->session_project,
                     "cwd", srv->session_root,
                     "hint", "Cannot derive valid project name from CWD");
        srv->session_project[0] = '\0';
        srv->session_root[0] = '\0';
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

    int rc = cbm_pipeline_run(p);
    cbm_pipeline_free(p);

    if (rc == 0) {
        /* Re-index dependencies after fresh dump */
        cbm_store_t *store = resolve_store(srv, srv->session_project);
        if (store) {
            cbm_dep_auto_index(srv->session_project, srv->session_root,
                               store, CBM_DEFAULT_AUTO_DEP_LIMIT);
            cbm_pagerank_compute_with_config(store, srv->session_project, srv->config);
        }

        cbm_log_info("autoindex.done", "project", srv->session_project);
        notify_resources_updated(srv);
        if (srv->watcher) {
            cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
        }
    } else {
        cbm_log_warn("autoindex.err", "msg", "pipeline_run_failed");
    }
    cbm_mem_collect();
    return NULL;
}

/* Check if a DB file has actual content (at least 1 node).
 * Returns true if DB exists AND has nodes. Lightweight raw SQLite check. */
static bool db_has_content(const char *db_path) {
    struct stat st;
    if (stat(db_path, &st) != 0) return false; /* file doesn't exist */

    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    sqlite3_stmt *stmt = NULL;
    bool has = false;
    if (sqlite3_prepare_v2(db, "SELECT 1 FROM nodes LIMIT 1", -1, &stmt, NULL) == SQLITE_OK) {
        has = (sqlite3_step(stmt) == SQLITE_ROW);
        sqlite3_finalize(stmt);
    }
    sqlite3_close(db);
    return has;
}

/* Check if a DB's index is stale by comparing DB file mtime against latest
 * git commit time. If the repo has commits newer than the DB, it's stale.
 * Also stale if DB is older than max_age_seconds (0 = disabled).
 * Returns false on any error (conservative: don't trigger unnecessary reindex). */
static bool db_is_stale(const char *db_path, const char *repo_path, int max_age_seconds) {
    struct stat db_st;
    if (stat(db_path, &db_st) != 0) return false;
    time_t db_mtime = db_st.st_mtime;

    /* Check age-based staleness (configurable, 0 = disabled).
     * Guard against clock skew: only consider stale if now > db_mtime. */
    if (max_age_seconds > 0) {
        time_t now = time(NULL);
        if (now > db_mtime && (now - db_mtime) > max_age_seconds) return true;
    }

    /* Check git HEAD commit time vs DB mtime */
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
        "git -C '%s' log -1 --format=%%ct HEAD 2>/dev/null", repo_path);
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) return false;
    char line[64] = {0};
    if (fgets(line, sizeof(line), fp)) {
        long commit_time = strtol(line, NULL, 10);
        cbm_pclose(fp);
        /* Stale if latest commit is newer than DB */
        return commit_time > (long)db_mtime;
    }
    cbm_pclose(fp);
    return false;
}

/* Config keys for reindex behavior */
#define CBM_CONFIG_REINDEX_ON_STARTUP "reindex_on_startup"
#define CBM_CONFIG_REINDEX_STALE_SECONDS "reindex_stale_seconds"

/* Start auto-indexing if configured and project not yet indexed. */
static void maybe_auto_index(cbm_mcp_server_t *srv) {
    if (srv->session_root[0] == '\0') {
        return; /* no session root detected */
    }

    /* Check if project already has a populated DB */
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *home = getenv("HOME");
    bool needs_index = true;
    char db_check[1024] = {0};
    if (home) {
        snprintf(db_check, sizeof(db_check), "%s/.cache/codebase-memory-mcp/%s.db", home,
                 srv->session_project);

        if (db_has_content(db_check)) {
            /* DB exists and has nodes — check if stale */
            bool reindex_on_startup = srv->config
                ? cbm_config_get_bool(srv->config, CBM_CONFIG_REINDEX_ON_STARTUP, false)
                : false;
            int stale_seconds = srv->config
                ? cbm_config_get_int(srv->config, CBM_CONFIG_REINDEX_STALE_SECONDS, 0)
                : 0;
            bool stale = db_is_stale(db_check, srv->session_root, stale_seconds);

            if (stale && reindex_on_startup) {
                cbm_log_info("autoindex.stale", "reason", "commits_newer_than_index", "project",
                             srv->session_project);
                needs_index = true;
            } else {
                if (stale) {
                    cbm_log_info("autoindex.stale_skipped", "reason", "reindex_on_startup=false",
                                 "hint", "set reindex_on_startup true to auto-update on restart",
                                 "project", srv->session_project);
                } else {
                    cbm_log_info("autoindex.skip", "reason", "already_indexed", "project",
                                 srv->session_project);
                }
                /* Register watcher for live change detection */
                if (srv->watcher) {
                    cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
                }
                needs_index = false;
            }
        } else {
            struct stat st;
            if (stat(db_check, &st) == 0) {
                /* DB file exists but has 0 nodes — treat as not indexed */
                cbm_log_info("autoindex.empty_db", "reason", "db_exists_but_empty", "project",
                             srv->session_project);
            }
            needs_index = true;
        }
    }

    if (!needs_index) return;

/* Default file limit for auto-indexing new projects */
#define DEFAULT_AUTO_INDEX_LIMIT 50000

    /* Check auto_index: env var CBM_AUTO_INDEX > config DB > default (true).
     * Defaults to true so resources have data at startup. */
    bool auto_index = true;
    int file_limit = DEFAULT_AUTO_INDEX_LIMIT;
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char *auto_env = getenv("CBM_AUTO_INDEX");
    if (auto_env && auto_env[0]) {
        auto_index = (strcmp(auto_env, "true") == 0 || strcmp(auto_env, "1") == 0);
    } else if (srv->config) {
        auto_index = cbm_config_get_bool(srv->config, CBM_CONFIG_AUTO_INDEX, true);
    }
    if (srv->config) {
        file_limit =
            cbm_config_get_int(srv->config, CBM_CONFIG_AUTO_INDEX_LIMIT, DEFAULT_AUTO_INDEX_LIMIT);
    }

    if (!auto_index) {
        cbm_log_info("autoindex.skip", "reason", "disabled", "hint",
                     "export CBM_AUTO_INDEX=true  OR  codebase-memory-mcp config set auto_index true");
        return;
    }

    /* Quick file count check to avoid OOM on massive repos */
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "git -C '%s' ls-files 2>/dev/null | wc -l", srv->session_root);
    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
    FILE *fp = cbm_popen(cmd, "r");
    if (fp) {
        char line[64];
        if (fgets(line, sizeof(line), fp)) {
            int count = (int)strtol(line, NULL, 10);
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

/* ── Background update check ──────────────────────────────────── */

#define UPDATE_CHECK_URL "https://api.github.com/repos/DeusData/codebase-memory-mcp/releases/latest"

static void *update_check_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;

    /* Use curl with 5s timeout to fetch latest release tag */
    FILE *fp = cbm_popen("curl -sf --max-time 5 -H 'Accept: application/vnd.github+json' "
                         "'" UPDATE_CHECK_URL "' 2>/dev/null",
                         "r");
    if (!fp) {
        srv->update_checked = true;
        return NULL;
    }

    char buf[4096];
    size_t total = 0;
    while (total < sizeof(buf) - 1) {
        size_t n = fread(buf + total, 1, sizeof(buf) - 1 - total, fp);
        if (n == 0) {
            break;
        }
        total += n;
    }
    buf[total] = '\0';
    cbm_pclose(fp);

    /* Parse tag_name from JSON response */
    yyjson_doc *doc = yyjson_read(buf, total, 0);
    if (!doc) {
        srv->update_checked = true;
        return NULL;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *tag = yyjson_obj_get(root, "tag_name");
    const char *tag_str = yyjson_get_str(tag);

    if (tag_str) {
        const char *current = cbm_cli_get_version();
        if (cbm_compare_versions(tag_str, current) > 0) {
            snprintf(srv->update_notice, sizeof(srv->update_notice),
                     "Update available: %s -> %s -- run: codebase-memory-mcp update", current,
                     tag_str);
            cbm_log_info("update.available", "current", current, "latest", tag_str);
        }
    }

    yyjson_doc_free(doc);
    srv->update_checked = true;
    return NULL;
}

static void start_update_check(cbm_mcp_server_t *srv) {
    if (srv->update_checked) {
        return;
    }
    srv->update_checked = true; /* prevent double-launch */
    if (cbm_thread_create(&srv->update_tid, 0, update_check_thread, srv) == 0) {
        srv->update_thread_active = true;
    }
}

/* Prepend update notice to a tool result, then clear it (one-shot). */
static char *inject_update_notice(cbm_mcp_server_t *srv, char *result_json) {
    if (srv->update_notice[0] == '\0') {
        return result_json;
    }

    /* Parse existing result, prepend notice text, rebuild */
    yyjson_doc *doc = yyjson_read(result_json, strlen(result_json), 0);
    if (!doc) {
        return result_json;
    }

    yyjson_mut_doc *mdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_val_mut_copy(mdoc, yyjson_doc_get_root(doc));
    yyjson_doc_free(doc);
    if (!root) {
        yyjson_mut_doc_free(mdoc);
        return result_json;
    }
    yyjson_mut_doc_set_root(mdoc, root);

    /* Find the "content" array */
    yyjson_mut_val *content = yyjson_mut_obj_get(root, "content");
    if (content && yyjson_mut_is_arr(content)) {
        /* Prepend a text content item with the update notice */
        yyjson_mut_val *notice_item = yyjson_mut_obj(mdoc);
        yyjson_mut_obj_add_str(mdoc, notice_item, "type", "text");
        yyjson_mut_obj_add_str(mdoc, notice_item, "text", srv->update_notice);
        yyjson_mut_arr_prepend(content, notice_item);
    }

    size_t len;
    char *new_json = yyjson_mut_write(mdoc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, &len);
    yyjson_mut_doc_free(mdoc);

    if (new_json) {
        free(result_json);
        srv->update_notice[0] = '\0'; /* clear — one-shot */
        return new_json;
    }
    return result_json;
}

/* ── MCP Resources (Phase 10) ─────────────────────────────────── */

/* Send a JSON-RPC notification (no id) to the client's output stream.
 * Used for notifications/resources/updated after index operations. */
static void send_notification(cbm_mcp_server_t *srv, const char *method) {
    if (!srv || !srv->out_stream) return;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    yyjson_mut_obj_add_str(doc, root, "method", method);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (json) {
        (void)fprintf(srv->out_stream, "%s\n", json);
        (void)fflush(srv->out_stream);
        free(json);
    }
}

/* Send notifications/resources/list_changed after index operations.
 * Per MCP spec: list_changed is for when the server's resource data changes
 * (we declared listChanged:true in capabilities). notifications/resources/updated
 * is only for per-resource subscriptions (we don't support subscribe). */
static void notify_resources_updated(cbm_mcp_server_t *srv) {
    if (srv->client_has_resources)
        send_notification(srv, "notifications/resources/list_changed");
}

/* Handle resources/list — return 3 resource URIs. */
static char *handle_resources_list(cbm_mcp_server_t *srv) {
    (void)srv;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *arr = yyjson_mut_arr(doc);

    /* Resource 1: schema */
    yyjson_mut_val *r1 = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, r1, "uri", "codebase://schema");
    yyjson_mut_obj_add_str(doc, r1, "name", "Code Graph Schema");
    yyjson_mut_obj_add_str(doc, r1, "description",
        "Node labels (Function, Class, Module, etc.) and edge types (CALLS, IMPORTS, "
        "DEFINES_METHOD, etc.) with counts. Read this before writing Cypher queries "
        "to know valid labels and relationship types.");
    yyjson_mut_obj_add_str(doc, r1, "mimeType", "application/json");
    yyjson_mut_arr_add_val(arr, r1);

    /* Resource 2: architecture */
    yyjson_mut_val *r2 = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, r2, "uri", "codebase://architecture");
    yyjson_mut_obj_add_str(doc, r2, "name", "Architecture Overview");
    yyjson_mut_obj_add_str(doc, r2, "description",
        "Total nodes/edges, top 10 key functions ranked by PageRank (structural "
        "importance), and relationship patterns. Read this first to understand "
        "codebase structure and find important entry points.");
    yyjson_mut_obj_add_str(doc, r2, "mimeType", "application/json");
    yyjson_mut_arr_add_val(arr, r2);

    /* Resource 3: status */
    yyjson_mut_val *r3 = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, r3, "uri", "codebase://status");
    yyjson_mut_obj_add_str(doc, r3, "name", "Index Status");
    yyjson_mut_obj_add_str(doc, r3, "description",
        "Project name, indexing status (ready/empty/not_indexed/indexing), "
        "node/edge counts, PageRank stats, detected ecosystem, dependency list. "
        "Status 'indexing' = in progress, 'not_indexed' includes action_required hint. "
        "Auto-index failure reports detail and fix suggestion.");
    yyjson_mut_obj_add_str(doc, r3, "mimeType", "application/json");
    yyjson_mut_arr_add_val(arr, r3);

    yyjson_mut_obj_add_val(doc, root, "resources", arr);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* Get the active project name: current_project (from last tool call) or session_project. */
static const char *active_project_name(cbm_mcp_server_t *srv) {
    if (srv->current_project) return srv->current_project;
    return srv->session_project[0] ? srv->session_project : NULL;
}

/* Resolve store for resource handlers. Prefers the currently-open project
 * (set by the most recent tool call) over the session project, so resources
 * reflect data the user is actually querying — not the empty CWD project. */
static cbm_store_t *resolve_resource_store(cbm_mcp_server_t *srv) {
    /* 1. Use currently-open project (set by last resolve_store call) */
    if (srv->current_project && srv->store)
        return srv->store;
    /* 2. Fall back to session project */
    const char *proj = srv->session_project[0] ? srv->session_project : NULL;
    if (proj) return resolve_store(srv, proj);
    return srv->store;
}

/* Build schema resource content (reuses inject_context_once logic). */
static void build_resource_schema(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  cbm_mcp_server_t *srv) {
    cbm_store_t *store = resolve_resource_store(srv);
    const char *proj = active_project_name(srv);

    if (!store) {
        yyjson_mut_obj_add_str(doc, root, "status", "not_indexed");
        return;
    }

    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, proj, &schema);

    yyjson_mut_val *label_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; i++) {
        yyjson_mut_val *lbl = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, lbl, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, lbl, "count", schema.node_labels[i].count);
        yyjson_mut_arr_add_val(label_arr, lbl);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", label_arr);

    yyjson_mut_val *type_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; i++) {
        yyjson_mut_val *et = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, et, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, et, "count", schema.edge_types[i].count);
        yyjson_mut_arr_add_val(type_arr, et);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", type_arr);
    cbm_store_schema_free(&schema);
}

/* CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE defined in constants section at top of file */

/* Build a key_functions SQL query with optional exclude patterns.
 * exclude_csv: comma-separated globs from config, or NULL.
 * exclude_arr: NULL-terminated array from tool param, or NULL.
 * Returns a heap-allocated SQL string. Caller must free. */
static char *build_key_functions_sql(const char *exclude_csv,
                                     const char **exclude_arr, int limit) {
    char sql[4096];
    int pos = 0;
    pos += snprintf(sql + pos, sizeof(sql) - pos,
        "SELECT n.name, n.qualified_name, n.label, n.file_path, pr.rank "
        "FROM pagerank pr JOIN nodes n ON n.id = pr.node_id "
        "WHERE pr.project = ?1 "
        "AND n.label IN ('Function','Class','Method','Interface') ");

    /* Apply config-based excludes (comma-separated globs) */
    if (exclude_csv && exclude_csv[0]) {
        char *csv_copy = heap_strdup(exclude_csv);
        char *tok = strtok(csv_copy, ",");
        while (tok && pos < (int)sizeof(sql) - 128) {
            while (*tok == ' ') tok++; /* trim leading space */
            char *like = cbm_glob_to_like(tok);
            if (like) {
                pos += snprintf(sql + pos, sizeof(sql) - pos,
                    "AND n.file_path NOT LIKE '%s' ", like);
                free(like);
            }
            tok = strtok(NULL, ",");
        }
        free(csv_copy);
    }

    /* Apply param-based excludes (array of globs) */
    if (exclude_arr) {
        for (int i = 0; exclude_arr[i] && pos < (int)sizeof(sql) - 128; i++) {
            char *like = cbm_glob_to_like(exclude_arr[i]);
            if (like) {
                pos += snprintf(sql + pos, sizeof(sql) - pos,
                    "AND n.file_path NOT LIKE '%s' ", like);
                free(like);
            }
        }
    }

    snprintf(sql + pos, sizeof(sql) - pos, "ORDER BY pr.rank DESC LIMIT %d",
             limit > 0 ? limit : 25);
    return heap_strdup(sql);
}

/* Build architecture resource content. */
static void build_resource_architecture(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                        cbm_mcp_server_t *srv) {
    cbm_store_t *store = resolve_resource_store(srv);
    const char *proj = active_project_name(srv);

    if (!store) {
        yyjson_mut_obj_add_str(doc, root, "status", "not_indexed");
        return;
    }

    int nodes = cbm_store_count_nodes(store, proj);
    int edges = cbm_store_count_edges(store, proj);
    yyjson_mut_obj_add_int(doc, root, "total_nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "total_edges", edges);

    /* Key functions by PageRank (top 10), with config-driven exclude patterns */
    struct sqlite3 *db = cbm_store_get_db(store);
    if (db && proj) {
        const char *excl_csv = srv->config
            ? cbm_config_get(srv->config, CBM_CONFIG_KEY_FUNCTIONS_EXCLUDE, "")
            : "";
        int kf_limit = srv->config
            ? cbm_config_get_int(srv->config, CBM_CONFIG_KEY_FUNCTIONS_COUNT, 25)
            : 25;
        char *sql = build_key_functions_sql(excl_csv, NULL, kf_limit);
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, proj, -1, SQLITE_TRANSIENT);
            yyjson_mut_val *kf_arr = yyjson_mut_arr(doc);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                yyjson_mut_val *kf = yyjson_mut_obj(doc);
                const char *name = (const char *)sqlite3_column_text(stmt, 0);
                const char *qn = (const char *)sqlite3_column_text(stmt, 1);
                const char *label = (const char *)sqlite3_column_text(stmt, 2);
                const char *fp = (const char *)sqlite3_column_text(stmt, 3);
                double rank = sqlite3_column_double(stmt, 4);
                if (name && !ends_with_segment(qn, name))
                    yyjson_mut_obj_add_strcpy(doc, kf, "name", name);
                if (qn)              yyjson_mut_obj_add_strcpy(doc, kf, "qualified_name", qn);
                if (label && label[0]) yyjson_mut_obj_add_strcpy(doc, kf, "label", label);
                if (fp    && fp[0])    yyjson_mut_obj_add_strcpy(doc, kf, "file_path", fp);
                add_pagerank_val(doc, kf, rank);
                yyjson_mut_arr_add_val(kf_arr, kf);
            }
            yyjson_mut_obj_add_val(doc, root, "key_functions", kf_arr);
            sqlite3_finalize(stmt);
        }
        free(sql);
    }

    /* Relationship patterns from schema */
    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, proj, &schema);
    if (schema.rel_pattern_count > 0) {
        yyjson_mut_val *rp_arr = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            yyjson_mut_arr_add_strcpy(doc, rp_arr, schema.rel_patterns[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "relationship_patterns", rp_arr);
    }
    cbm_store_schema_free(&schema);
}

/* Build status resource content. */
static void build_resource_status(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  cbm_mcp_server_t *srv) {
    cbm_store_t *store = resolve_resource_store(srv);
    const char *proj = active_project_name(srv);

    if (proj) yyjson_mut_obj_add_str(doc, root, "project", proj);

    /* IX-2: Check for indexing-in-progress BEFORE checking store contents */
    if (srv->autoindex_active) {
        yyjson_mut_obj_add_str(doc, root, "status", "indexing");
        yyjson_mut_obj_add_str(doc, root, "hint",
            "Indexing is in progress. Results will be available when status changes to 'ready'. "
            "This typically takes 5-30 seconds depending on project size.");
        return;
    }

    if (!store) {
        yyjson_mut_obj_add_str(doc, root, "status", "not_indexed");
        /* IX-1: Report if auto-index was attempted and failed */
        if (srv->autoindex_failed) {
            yyjson_mut_obj_add_str(doc, root, "detail",
                "Auto-indexing was attempted but failed. Run index_repository explicitly for detailed errors.");
        } else {
            yyjson_mut_obj_add_str(doc, root, "action_required",
                "Call index_repository with repo_path to index this project.");
        }
        return;
    }

    int nodes = cbm_store_count_nodes(store, proj);
    int edges = cbm_store_count_edges(store, proj);
    yyjson_mut_obj_add_str(doc, root, "status", nodes > 0 ? "ready" : "empty");
    if (nodes == 0 && !srv->autoindex_failed) {
        yyjson_mut_obj_add_str(doc, root, "hint",
            "Project store exists but is empty. This may happen if the project has no recognized source files, "
            "or if indexing hasn't completed yet. Try index_repository for explicit indexing.");
    }
    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);

    /* PageRank stats */
    struct sqlite3 *db = cbm_store_get_db(store);
    if (db && proj) {
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT COUNT(*), MAX(computed_at) FROM pagerank WHERE project = ?1",
                -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, proj, -1, SQLITE_TRANSIENT);
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                int ranked = sqlite3_column_int(stmt, 0);
                if (ranked > 0) {
                    yyjson_mut_obj_add_int(doc, root, "ranked_nodes", ranked);
                    const char *ts = (const char *)sqlite3_column_text(stmt, 1);
                    if (ts) yyjson_mut_obj_add_strcpy(doc, root, "pagerank_computed_at", ts);
                }
            }
            sqlite3_finalize(stmt);
        }
    }

    /* Detected ecosystem */
    if (srv->session_root[0]) {
        cbm_pkg_manager_t eco = cbm_detect_ecosystem(srv->session_root);
        if (eco != CBM_PKG_COUNT)
            yyjson_mut_obj_add_str(doc, root, "detected_ecosystem",
                                   cbm_pkg_manager_str(eco));
    }

    /* Dependencies — query projects table for dep entries */
    if (db && proj) {
        sqlite3_stmt *stmt = NULL;
        char pattern[512];
        snprintf(pattern, sizeof(pattern), "%s.dep.%%", proj);
        if (sqlite3_prepare_v2(db,
                "SELECT name FROM projects WHERE name LIKE ?1 ORDER BY name",
                -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, pattern, -1, SQLITE_TRANSIENT);
            yyjson_mut_val *dep_arr = yyjson_mut_arr(doc);
            int dep_count = 0;
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *dname = (const char *)sqlite3_column_text(stmt, 0);
                if (dname) {
                    yyjson_mut_val *d = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, d, "name", dname);
                    int dn = cbm_store_count_nodes(store, dname);
                    yyjson_mut_obj_add_int(doc, d, "nodes", dn);
                    yyjson_mut_arr_add_val(dep_arr, d);
                    dep_count++;
                }
            }
            sqlite3_finalize(stmt);
            if (dep_count > 0)
                yyjson_mut_obj_add_val(doc, root, "dependencies", dep_arr);
        }
    }
}

/* Handle resources/read — dispatch by URI.
 * Returns result JSON on success (caller wraps in JSON-RPC response).
 * On error, sets *err_out to a pre-formatted JSON-RPC error and returns NULL. */
static char *handle_resources_read(cbm_mcp_server_t *srv, const char *params_raw,
                                   int64_t req_id, char **err_out) {
    *err_out = NULL;
    /* Extract URI from params */
    char *uri = NULL;
    if (params_raw) {
        yyjson_doc *pdoc = yyjson_read(params_raw, strlen(params_raw), 0);
        if (pdoc) {
            yyjson_val *u = yyjson_obj_get(yyjson_doc_get_root(pdoc), "uri");
            if (u && yyjson_is_str(u))
                uri = heap_strdup(yyjson_get_str(u));
            yyjson_doc_free(pdoc);
        }
    }
    if (!uri) {
        *err_out = cbm_jsonrpc_format_error(req_id, -32602, "Missing uri parameter");
        return NULL;
    }

    /* Build resource content — root IS the content object */
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (strcmp(uri, "codebase://schema") == 0) {
        build_resource_schema(doc, root, srv);
    } else if (strcmp(uri, "codebase://architecture") == 0) {
        build_resource_architecture(doc, root, srv);
    } else if (strcmp(uri, "codebase://status") == 0) {
        build_resource_status(doc, root, srv);
    } else {
        yyjson_mut_doc_free(doc);
        char msg[512];
        snprintf(msg, sizeof(msg),
            "Resource not found: '%s'. "
            "Available resources: codebase://schema, codebase://architecture, codebase://status. "
            "Use resources/list to discover all resources.",
            uri);
        free(uri);
        *err_out = cbm_jsonrpc_format_error(req_id, -32002, msg);
        return NULL;
    }

    /* Format as resources/read response: {contents: [{uri, mimeType, text}]} */
    char *content_json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    yyjson_mut_doc *rdoc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *rroot = yyjson_mut_obj(rdoc);
    yyjson_mut_doc_set_root(rdoc, rroot);

    yyjson_mut_val *contents = yyjson_mut_arr(rdoc);
    yyjson_mut_val *item = yyjson_mut_obj(rdoc);
    yyjson_mut_obj_add_strcpy(rdoc, item, "uri", uri);
    yyjson_mut_obj_add_str(rdoc, item, "mimeType", "application/json");
    if (content_json)
        yyjson_mut_obj_add_strcpy(rdoc, item, "text", content_json);
    yyjson_mut_arr_add_val(contents, item);
    yyjson_mut_obj_add_val(rdoc, rroot, "contents", contents);

    char *out = yy_doc_to_str(rdoc);
    yyjson_mut_doc_free(rdoc);
    free(content_json);
    free(uri);
    return out;
}

/* ── Server request handler ───────────────────────────────────── */

char *cbm_mcp_server_handle(cbm_mcp_server_t *srv, const char *line) {
    cbm_jsonrpc_request_t req = {0};
    if (cbm_jsonrpc_parse(line, &req) < 0) {
        return cbm_jsonrpc_format_error(0, JSONRPC_PARSE_ERROR, "Parse error");
    }

    /* Notifications (no id) → no response */
    if (!req.has_id) {
        cbm_jsonrpc_request_free(&req);
        return NULL;
    }

    char *result_json = NULL;

    if (strcmp(req.method, "initialize") == 0) {
        result_json = cbm_mcp_initialize_response();
        /* Parse client capabilities to detect resources support */
        if (req.params_raw) {
            yyjson_doc *pdoc = yyjson_read(req.params_raw, strlen(req.params_raw), 0);
            if (pdoc) {
                yyjson_val *proot = yyjson_doc_get_root(pdoc);
                yyjson_val *ccaps = yyjson_obj_get(proot, "capabilities");
                if (ccaps && yyjson_obj_get(ccaps, "resources"))
                    srv->client_has_resources = true;
                yyjson_doc_free(pdoc);
            }
        }
        start_update_check(srv);
        detect_session(srv);
        maybe_auto_index(srv);
    } else if (strcmp(req.method, "resources/list") == 0) {
        result_json = handle_resources_list(srv);
    } else if (strcmp(req.method, "resources/read") == 0) {
        /* handle_resources_read may return a pre-formatted JSON-RPC error (id=0).
         * Detect by checking for NULL result_json — errors are returned via err_out. */
        char *err_out = NULL;
        result_json = handle_resources_read(srv, req.params_raw, req.id, &err_out);
        if (err_out) {
            /* Error already formatted as JSON-RPC with correct id — return directly */
            cbm_jsonrpc_request_free(&req);
            return err_out;
        }
    } else if (strcmp(req.method, "tools/list") == 0) {
        result_json = cbm_mcp_tools_list(srv);
    } else if (strcmp(req.method, "tools/call") == 0) {
        char *tool_name = req.params_raw ? cbm_mcp_get_tool_name(req.params_raw) : NULL;
        char *tool_args =
            req.params_raw ? cbm_mcp_get_arguments(req.params_raw) : heap_strdup("{}");

        result_json = cbm_mcp_handle_tool(srv, tool_name, tool_args);
        result_json = inject_update_notice(srv, result_json);
        free(tool_name);
        free(tool_args);
    } else {
        char *err = cbm_jsonrpc_format_error(req.id, JSONRPC_METHOD_NOT_FOUND, "Method not found");
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    cbm_jsonrpc_response_t resp = {
        .id = req.id,
        .result_json = result_json,
    };
    char *out = cbm_jsonrpc_format_response(&resp);
    free(result_json);
    cbm_jsonrpc_request_free(&req);
    return out;
}

/* ── Event loop ───────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_mcp_server_run(cbm_mcp_server_t *srv, FILE *in, FILE *out) {
    srv->out_stream = out; /* store for sending notifications */
    char *line = NULL;
    size_t cap = 0;
    int fd = cbm_fileno(in);

    for (;;) {
        /* Poll with idle timeout so we can evict unused stores between requests.
         * MCP is request-response (one line at a time), so mixing poll() on the
         * raw fd with getline() on the buffered FILE* is safe in practice. */
#ifdef _WIN32
        /* Windows: WaitForSingleObject on stdin handle */
        HANDLE hStdin = (HANDLE)_get_osfhandle(fd);
        DWORD wr = WaitForSingleObject(hStdin, STORE_IDLE_TIMEOUT_S * 1000);
        if (wr == WAIT_FAILED) {
            break;
        }
        if (wr == WAIT_TIMEOUT) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            continue;
        }
#else
        struct pollfd pfd = {.fd = fd, .events = POLLIN};
        int pr = poll(&pfd, 1, STORE_IDLE_TIMEOUT_S * 1000);

        if (pr < 0) {
            break; /* error or signal */
        }
        if (pr == 0) {
            /* Timeout — evict idle store to free resources */
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            continue;
        }
#endif

        if (cbm_getline(&line, &cap, in) <= 0) {
            break;
        }

        /* Trim trailing newline */
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        char *resp = cbm_mcp_server_handle(srv, line);
        if (resp) {
            // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
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
    if (strncmp(uri, "file://", 7) != 0) {
        out_path[0] = '\0';
        return false;
    }

    const char *path = uri + 7;

    /* On Windows, file:///C:/path → /C:/path. Strip leading / before drive letter. */
    if (path[0] == '/' && path[1] &&
        ((path[1] >= 'A' && path[1] <= 'Z') || (path[1] >= 'a' && path[1] <= 'z')) &&
        path[2] == ':') {
        path++; /* skip the leading / */
    }

    snprintf(out_path, out_size, "%s", path);
    return true;
}
