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
#include "foundation/diagnostics.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/str_util.h"
#include "foundation/compat_regex.h"
#include <sqlite3.h>

#ifdef _WIN32
#include <process.h> /* _getpid */
#else
#include <unistd.h>
#include <sys/unistd.h>
#include <sys/poll.h>
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
     "\"string\"},\"name_pattern\":{\"type\":\"string\",\"description\":\"Regex pattern on symbol "
     "name. Glob wildcards (*tool*, foo?) auto-convert to regex.\"},"
     "\"qn_pattern\":{\"type\":\"string\",\"description\":\"Regex pattern on qualified name. "
     "Glob wildcards auto-convert to regex.\"},"
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
     "finding callers, dependencies, or impact analysis. Matches exact name first, then falls back "
     "to case-insensitive search if not found. Shows candidates array when function name "
     "is ambiguous. Results are deduplicated (cycles don't inflate counts).",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\","
     "\"description\":\"Function name to trace. Case-insensitive fallback if exact match not found."
     "\"},\"project\":{"
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
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"get_architecture",
     "Get high-level architecture overview — packages, services, dependencies, and project "
     "structure at a glance.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"aspects\":{\"type\":"
     "\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"project\"]}"},

    {"search_code",
     "Search source code with text or regex patterns. Case-insensitive by default. "
     "Use for string literals, error messages, and config values not in the knowledge graph. "
     "Use path_filter regex to scope results to specific paths.",
     "{\"type\":\"object\",\"properties\":{\"pattern\":{\"type\":\"string\"},\"project\":{\"type\":"
     "\"string\"},\"file_pattern\":{\"type\":\"string\",\"description\":\"Glob for grep "
     "--include (e.g. *.go)\"},\"path_filter\":{\"type\":\"string\",\"description\":\"Regex "
     "filter on result file paths (e.g. ^src/ or \\\\.(go|ts)$)\"},"
     "\"regex\":{\"type\":\"boolean\",\"default\":false},"
     "\"case_sensitive\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Match case-sensitively (default: case-insensitive).\"},"
     "\"limit\":{\"type\":\"integer\",\"description\":\"Max "
     "results (configurable via search_limit config key). Set higher for exhaustive text search."
     "\"}},\"required\":["
     "\"pattern\"]}"},

    {"list_projects", "List all indexed projects", "{\"type\":\"object\",\"properties\":{}}"},

    {"delete_project", "Delete a project from the index",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"index_status", "Get the indexing status of a project",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"detect_changes", "Detect code changes and their impact",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"scope\":{\"type\":"
     "\"string\"},\"depth\":{\"type\":\"integer\",\"default\":2},\"base_branch\":{\"type\":"
     "\"string\",\"default\":\"main\"}},\"required\":[\"project\"]}"},

    {"manage_adr", "Create or update Architecture Decision Records",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"mode\":{\"type\":"
     "\"string\",\"enum\":[\"get\",\"update\",\"sections\"]},\"content\":{\"type\":\"string\"},"
     "\"sections\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"project\"]"
     "}"},

    {"ingest_traces", "Ingest runtime traces to enhance the knowledge graph",
     "{\"type\":\"object\",\"properties\":{\"traces\":{\"type\":\"array\",\"items\":{\"type\":"
     "\"object\"}},\"project\":{\"type\":"
     "\"string\"}},\"required\":[\"traces\",\"project\"]}"},

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
     "3 modes via dispatch params: "
     "(1) cypher=<query>: Cypher multi-hop query. "
     "(2) search_in='source': grep source files for text patterns. "
     "(3) default: graph attribute search by label/name_pattern/pattern/sort_by. "
     "pattern= searches name OR qualified_name (OR-match). "
     "Results sorted by PageRank by default. "
     "mode=summary returns aggregate counts (results_suppressed=true). "
     "Read codebase://schema for node labels, edge types, and Cypher examples. "
     "Read codebase://architecture for key functions and graph overview.",
     "{\"type\":\"object\",\"properties\":{"
     "\"project\":{\"type\":\"string\",\"description\":\"Project name, path, or filter. "
     "Accepts: project name, directory path (/path/to/repo), tilde path (~/path/to/repo), "
     "'self' (project only), 'dep'/'deps' (dependencies only), 'dep.pandas' (specific dep), "
     "glob patterns. Omit to use the auto-detected session project.\"},"
     "\"cypher\":{\"type\":\"string\",\"description\":\"Cypher query for complex multi-hop "
     "patterns. When provided, other filter params are ignored. Add LIMIT.\"},"
     "\"label\":{\"type\":\"string\"},"
     "\"name_pattern\":{\"type\":\"string\",\"description\":\"Regex pattern on symbol name. "
     "Glob wildcards (*tool*, foo?) auto-convert to regex.\"},"
     "\"qn_pattern\":{\"type\":\"string\",\"description\":\"Regex pattern on qualified name. "
     "Glob wildcards auto-convert to regex.\"},"
     "\"file_pattern\":{\"type\":\"string\"},"
     "\"sort_by\":{\"type\":\"string\",\"enum\":[\"relevance\",\"name\",\"degree\",\"calls\",\"linkrank\"],"
     "\"description\":\"Sort order: relevance (PageRank, default), name, degree (edge weight), "
     "calls (function calls in+out), linkrank (link-based rank).\"},"
     "\"mode\":{\"type\":\"string\",\"enum\":[\"full\",\"summary\"]},"
     "\"compact\":{\"type\":\"boolean\"},\"include_dependencies\":{\"type\":\"boolean\"},"
     "\"limit\":{\"type\":\"integer\"},\"offset\":{\"type\":\"integer\"},"
     "\"min_degree\":{\"type\":\"integer\"},\"max_degree\":{\"type\":\"integer\"},"
     "\"max_output_bytes\":{\"type\":\"integer\",\"description\":\"Max response bytes (cypher mode). 0=unlimited.\"},"
     "\"relationship\":{\"type\":\"string\"},"
     "\"exclude_entry_points\":{\"type\":\"boolean\"},"
     "\"include_connected\":{\"type\":\"boolean\"},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Glob patterns for file paths to exclude (e.g. [\\\"tests/**\\\",\\\"scripts/**\\\"])\"},"
     "\"search_in\":{\"type\":\"string\",\"enum\":[\"graph\",\"source\"],\"default\":\"graph\","
     "\"description\":\"'graph' (default): search indexed symbols — returns {total,results:[{qualified_name,label,...}]}. "
     "'source': grep raw source files — returns {matches:[{file,line,content}],count}. "
     "Use 'source' for string literals, error messages, and text not in the symbol graph.\"},"
     "\"pattern\":{\"type\":\"string\",\"description\":\"OR-search: matches symbol name OR qualified name. "
     "Also used as the grep pattern when search_in='source'. Glob wildcards auto-convert to regex.\"},"
     "\"case_sensitive\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Case-sensitive name_pattern/qn_pattern/pattern matching (default: insensitive).\"},"
     "\"regex\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"When search_in='source': treat pattern as regex (default: literal text).\"},"
     "\"summary\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Return aggregate counts by label and file only. Alias for mode='summary'.\"},"
     "\"max_rows\":{\"type\":\"integer\","
     "\"description\":\"Max row scan for Cypher queries (cypher mode only).\"}"
     "}}"},

    {"trace_call_path",
     "Trace function call paths — who calls a function and what it calls. "
     "Use for impact analysis, understanding callers, and finding dependencies. "
     "Auto-indexes the project on first use if not already indexed. "
     "Matches exact name first, then falls back to case-insensitive search if not found. "
     "Results sorted by PageRank within each hop level. depth < 1 clamped to 1. "
     "direction must be inbound, outbound, or both (invalid values return error). "
     "Read codebase://architecture for key functions to start tracing from.",
     "{\"type\":\"object\",\"properties\":{"
     "\"function_name\":{\"type\":\"string\",\"description\":\"Function name to trace. "
     "Case-insensitive fallback if exact match not found.\"},"
     "\"qualified_name\":{\"type\":\"string\",\"description\":\"Exact qualified name from search results "
     "(e.g. 'proj.src.module.func'). Pass instead of function_name for cross-tool chaining.\"},"
     "\"project\":{\"type\":\"string\"},"
     "\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\",\"both\"]},"
     "\"depth\":{\"type\":\"integer\",\"default\":3},"
     "\"max_results\":{\"type\":\"integer\"},"
     "\"compact\":{\"type\":\"boolean\"},"
     "\"edge_types\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
     "\"exclude\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Glob patterns for file paths to exclude from trace results\"}"
     "},\"description\":\"Pass function_name OR qualified_name (at least one required).\"}"},

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
     "\"include_neighbors\":{\"type\":\"boolean\"},"
     "\"compact\":{\"type\":\"boolean\",\"default\":true,"
     "\"description\":\"Omit name when it equals last segment of qualified_name (default: compact config).\"}"
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
    const char *home = cbm_get_home_dir();
    if (!home) {
        home = cbm_tmpdir();
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
        return NULL; /* project is required — no implicit fallback */
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

    /* Open project's .db file — query-only open (no SQLITE_OPEN_CREATE) to
     * prevent ghost .db file creation for unknown/unindexed projects. */
    char path[1024];
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
            char wal_path[1040];
            char shm_path[1040];
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
        /* Register newly-accessed project with watcher (root_path from DB) */
        if (srv->watcher && proj_verify.root_path && proj_verify.root_path[0]) {
            cbm_watcher_watch(srv->watcher, project, proj_verify.root_path);
        }
        cbm_project_free_fields(&proj_verify);
        srv->owns_store = true;
        free(srv->current_project);
        srv->current_project = heap_strdup(project);
    }

    return srv->store;
}

/* Build a helpful error listing available projects. Caller must free() result. */
static char *build_project_list_error(const char *reason) {
    char dir_path[1024];
    cache_dir(dir_path, sizeof(dir_path));

    /* Collect project names from .db files */
    char projects[4096] = "";
    int count = 0;
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (d) {
        int offset = 0;
        cbm_dirent_t *entry;
        while ((entry = cbm_readdir(d)) != NULL) {
            const char *n = entry->name;
            size_t len = strlen(n);
            if (len < 4 || strcmp(n + len - 3, ".db") != 0) {
                continue;
            }
            if (strncmp(n, "tmp-", 4) == 0 || strncmp(n, "_", 1) == 0) {
                continue;
            }
            if (count > 0 && offset < (int)sizeof(projects) - 2) {
                projects[offset++] = ',';
            }
            int wrote = snprintf(projects + offset, sizeof(projects) - (size_t)offset, "\"%.*s\"",
                                 (int)(len - 3), n);
            if (wrote > 0) {
                offset += wrote;
            }
            count++;
        }
        cbm_closedir(d);
    }

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
                    "\"fix\":\"Enable classic tools: set env CBM_TOOL_MODE=classic then call index_repository. " \
                    "Or retry by passing project=\\\"/path/to/repo\\\" or project=\\\"~/path\\\" explicitly.\"}", \
                    true);                                                                        \
            }                                                                                     \
            free(project);                                                                        \
            {                                                                                     \
                char *_err = build_project_list_error("no project loaded");                        \
                char *_res = cbm_mcp_text_result(_err, true);                                     \
                free(_err);                                                                       \
                return _res;                                                                      \
            }                                                                                     \
        }                                                                                         \
    } while (0)

/* ── Auto-context injection (Phase 9) ─────────────────────────── */

/* Inject _context header into the FIRST tool response after session starts.
 * Contains architecture, schema, status — eliminates the need for separate
 * get_architecture / get_graph_schema / index_status / list_projects calls.
 * Subsequent responses include only session_project (lightweight).
 *
 * WHY we inject for ALL clients, including those that declare MCP resources:
 *
 * MCP resources are "application-controlled" (pull-only). When a client
 * declares capabilities.resources:{}, it means the client CAN fetch resources
 * via resources/read — it does NOT mean the client will automatically read
 * codebase://schema or codebase://architecture. The spec defines no push path:
 *   - notifications/resources/updated signals that a resource changed but
 *     sends NO content; the client must explicitly call resources/read to get it
 *   - resources/read requires an explicit model action (or user @-mention)
 *   - In Claude Code, resources are only fetched on user @-mention or explicit
 *     system-prompt instruction — never spontaneously
 * References:
 *   https://modelcontextprotocol.io/specification/2025-06-18/server/resources
 *   https://modelcontextprotocol.io/docs/concepts/resources
 *   https://workos.com/blog/mcp-features-guide  ("resources = application-controlled")
 *
 * Embedding _context in the first tool response is therefore the ONLY reliable
 * delivery channel that reaches the model without requiring explicit user action.
 * The context_injected flag already prevents duplicate injection on subsequent
 * calls, so the one-shot delivery is both sufficient and non-repetitive.
 *
 * Resources remain available for explicit access (e.g. codebase://schema via
 * @-mention) — the two mechanisms are complementary, not mutually exclusive. */
static void inject_context_once(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                cbm_mcp_server_t *srv, cbm_store_t *store) {
    /* Always include session_project */
    if (srv->session_project[0])
        yyjson_mut_obj_add_str(doc, root, "session_project", srv->session_project);

    if (srv->context_injected) return;

    /* Configurable via config key "context_injection" (default true) or env
     * CBM_CONTEXT_INJECTION=false.  Disable to suppress the _context header
     * when token cost matters more than automatic situational awareness:
     *   - scripted / programmatic tool use (output parsed, extra JSON is noise)
     *   - CI pipelines (token cost is metered, model doesn't need schema context)
     *   - model given explicit system-prompt codebase instructions instead
     *   - benchmarking (removes schema-query overhead from latency measurements)
     * Checked before setting context_injected so toggling mid-session works. */
    bool inject_enabled = cbm_config_get_bool(srv->config, "context_injection", true);
    if (!inject_enabled) return;

    srv->context_injected = true;

    yyjson_mut_val *ctx = yyjson_mut_obj(doc);

    if (!store) {
        if (srv->session_root[0]) {
            yyjson_mut_obj_add_str(doc, ctx, "status", "auto_indexing");
            yyjson_mut_obj_add_str(doc, ctx, "hint",
                "Auto-indexing your project — retry this query in a moment. "
                "Pass project='/path/to/repo' or project='~/path/to/repo' explicitly to trigger immediately.");
        } else {
            yyjson_mut_obj_add_str(doc, ctx, "status", "not_indexed");
            yyjson_mut_obj_add_str(doc, ctx, "hint",
                "No project path detected. Pass project='/path/to/repo' or project='~/path/to/repo' to index and search.");
        }
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
/* ── Project path helpers ─────────────────────────────────────────
 * Shared by expand_project_param (Rule 0) and get_project_root so the
 * detection condition and slug computation are defined exactly once. */

/* Returns true if s looks like a filesystem path rather than a project slug.
 * Project slugs are dot-separated identifiers (e.g. "Users-foo-bar"); they
 * never contain / (except globs starting with *) and don't start with / ~ . */
static bool project_is_path(const char *s) {
    if (!s || !s[0]) return false;
    if (s[0] == '.') {
        return s[1] == '\0' || s[1] == '/'; /* "." or "./" — relative paths */
    }
    return s[0] == '/' || s[0] == '~' ||
           (strchr(s, '/') != NULL && s[0] != '*');
}

/* Expand a leading ~ to $HOME (~/... or ~ alone).
 * ~user/... is left unexpanded (requires getpwnam — not worth the complexity).
 * Returns a heap-allocated expanded string, or NULL when no expansion is needed
 * or $HOME is unset. Caller must free the result. */
static char *expand_tilde(const char *s) {
    if (s[0] != '~') return NULL;
    if (s[1] != '\0' && s[1] != '/') return NULL; /* "~user/..." — leave as-is */
    const char *home = getenv("HOME");
    if (!home || !home[0]) return NULL;
    /* Build: home + rest  ("~" → home, "~/rest" → home + "/rest") */
    size_t hlen = strlen(home);
    const char *rest = s + 1; /* "" or "/rest" */
    char *result = malloc(hlen + strlen(rest) + 1);
    if (!result) return NULL;
    memcpy(result, home, hlen);
    strcpy(result + hlen, rest); /* copies rest incl. NUL */
    return result;
}

/* Convert a filesystem path to a heap-allocated project slug.
 * Handles ~/ tilde expansion, resolves symlinks and relative components
 * via realpath(3), then derives the slug from the canonical absolute path.
 * Returns NULL if s is not a path. Caller must free the result. */
static char *project_slug_from_path(const char *s) {
    if (!project_is_path(s)) return NULL;
    char *expanded = expand_tilde(s);          /* non-NULL only for ~ paths */
    const char *to_resolve = expanded ? expanded : s;
    char *resolved = realpath(to_resolve, NULL); /* NULL if path doesn't exist */
    const char *canonical = resolved ? resolved : to_resolve;
    char *slug = cbm_project_name_from_path(canonical);
    free(resolved);
    free(expanded);
    return slug;
}

static project_expand_t expand_project_param(cbm_mcp_server_t *srv, char *raw) {
    project_expand_t r = {.value = NULL, .mode = MATCH_NONE};
    if (!raw) return r;

    /* Rule 0: Path detection — convert paths to project names.
     * Enables: search_code_graph(project="/path/to/repo") */
    if (project_is_path(raw)) {
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
        return cbm_mcp_text_result(
            "{\"error\":\"project not indexed — run index_repository first\"}", true);
    }
    cbm_project_free_fields(&proj_check);
    return NULL;
}

static char *handle_get_graph_schema(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
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

    /* Check ADR presence */
    cbm_project_t proj_info = {0};
    if (cbm_store_get_project(store, project, &proj_info) == 0 && proj_info.root_path) {
        char adr_path[4096];
        snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", proj_info.root_path);
        struct stat adr_st;
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
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

/* Expand a raw project param, resolve the correct store, and auto-index if needed.
 * Returns the resolved store (or NULL). Sets *out_pe to the expand result
 * (caller must free out_pe->value). Handles:
 *   - expand_project_param (Rule 0: /path → project name)
 *   - DB selection with prefix collision avoidance
 *   - Auto-index on first use (join background thread or sync index) */
static cbm_store_t *resolve_project_store(cbm_mcp_server_t *srv,
                                           char *raw_project,
                                           project_expand_t *out_pe) {
    /* Cold-start: when no project param given, use session_project so
     * auto-index fires on the correct DB instead of returning NULL. */
    project_expand_t pe;
    if (!raw_project && srv->session_project[0]) {
        pe.value = heap_strdup(srv->session_project);
        pe.mode = MATCH_PREFIX;
    } else {
        pe = expand_project_param(srv, raw_project);
    }

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

/* Convert shell-glob wildcards to POSIX ERE: bare '*' → '.*', bare '?' → '.'
 * "Bare" means not already preceded by '.' or '\'. This lets users pass
 * glob-style patterns like "*tool*" and have them work as ".*tool.*". */
static char *glob_to_regex(const char *glob) {
    size_t len = strlen(glob);
    /* Worst case: every char expands to 2 chars plus NUL */
    char *out = malloc(len * 2 + 1);
    if (!out) return NULL;
    size_t o = 0;
    for (size_t i = 0; i < len; i++) {
        char prev = i > 0 ? glob[i - 1] : 0;
        if (glob[i] == '*' && prev != '.' && prev != '\\') {
            out[o++] = '.';
            out[o++] = '*';
        } else if (glob[i] == '?' && prev != '.' && prev != '\\') {
            out[o++] = '.';
        } else {
            out[o++] = glob[i];
        }
    }
    out[o] = '\0';
    return out;
}

static char *handle_search_graph(cbm_mcp_server_t *srv, const char *args) {
    char *raw_project = cbm_mcp_get_string_arg(args, "project");
    project_expand_t pe = {0};
    cbm_store_t *store = resolve_project_store(srv, raw_project, &pe);
    char *project = pe.value;
    REQUIRE_STORE(store, project);

    char *label = cbm_mcp_get_string_arg(args, "label");
    /* F1: treat empty string as "no filter" */
    if (label && label[0] == '\0') { free(label); label = NULL; }
    char *name_pattern = cbm_mcp_get_string_arg(args, "name_pattern");
    char *qn_pattern = cbm_mcp_get_string_arg(args, "qn_pattern");
    /* F9: pre-validate regex patterns — auto-convert glob wildcards to regex.
     * Users/agents frequently pass *tool* (glob) instead of .*tool.* (regex).
     * On regex compilation failure, try glob_to_regex() conversion before erroring. */
    if (name_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, name_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char *converted = glob_to_regex(name_pattern);
            if (converted && cbm_regcomp(&re, converted, CBM_REG_EXTENDED | CBM_REG_NOSUB) == 0) {
                cbm_regfree(&re);
                free(name_pattern);
                name_pattern = converted;
            } else {
                free(converted);
                char errbuf[512];
                snprintf(errbuf, sizeof(errbuf),
                    "{\"error\":\"invalid regex in name_pattern: '%s'\","
                    "\"hint\":\"Use regex syntax: '.*tool.*' instead of '*tool*'\"}", name_pattern);
                free(label); free(name_pattern); free(pe.value);
                return cbm_mcp_text_result(errbuf, true);
            }
        } else {
            cbm_regfree(&re);
        }
    }
    if (qn_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, qn_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char *converted = glob_to_regex(qn_pattern);
            if (converted && cbm_regcomp(&re, converted, CBM_REG_EXTENDED | CBM_REG_NOSUB) == 0) {
                cbm_regfree(&re);
                free(qn_pattern);
                qn_pattern = converted;
            } else {
                free(converted);
                char errbuf[512];
                snprintf(errbuf, sizeof(errbuf),
                    "{\"error\":\"invalid regex in qn_pattern: '%s'\","
                    "\"hint\":\"Use regex syntax: '.*tool.*' instead of '*tool*'\"}", qn_pattern);
                free(label); free(name_pattern); free(qn_pattern); free(pe.value);
                return cbm_mcp_text_result(errbuf, true);
            }
        } else {
            cbm_regfree(&re);
        }
    }
    /* NEW: unified pattern — OR search across name AND qualified_name */
    char *unified_pattern = cbm_mcp_get_string_arg(args, "pattern");
    if (unified_pattern) {
        cbm_regex_t re;
        if (cbm_regcomp(&re, unified_pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            char *converted = glob_to_regex(unified_pattern);
            if (converted && cbm_regcomp(&re, converted, CBM_REG_EXTENDED | CBM_REG_NOSUB) == 0) {
                cbm_regfree(&re);
                free(unified_pattern);
                unified_pattern = converted;
            } else {
                free(converted);
                char errbuf[512];
                snprintf(errbuf, sizeof(errbuf),
                    "{\"error\":\"invalid regex in pattern: '%s'\","
                    "\"hint\":\"Use regex syntax: '.*tool.*' instead of '*tool*'\"}", unified_pattern);
                free(label); free(name_pattern); free(qn_pattern);
                free(unified_pattern); free(pe.value);
                return cbm_mcp_text_result(errbuf, true);
            }
        } else {
            cbm_regfree(&re);
        }
    }
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *relationship = cbm_mcp_get_string_arg(args, "relationship");
    char *sort_by = cbm_mcp_get_string_arg(args, "sort_by");
    /* Config default: heap_strdup REQUIRED — cbm_config_get returns cfg->get_buf (internal buffer),
     * NOT a heap pointer. free(sort_by) at all exits would corrupt config's buffer without strdup. */
    if (!sort_by && srv && srv->config) {
        const char *cfg_sort = cbm_config_get(srv->config, "default_sort_by", NULL);
        if (cfg_sort && cfg_sort[0]) sort_by = heap_strdup(cfg_sort);
    }
    /* F6: validate sort_by enum — O(1) string comparisons */
    if (sort_by && strcmp(sort_by, "relevance") != 0 && strcmp(sort_by, "name") != 0 &&
        strcmp(sort_by, "degree") != 0 && strcmp(sort_by, "calls") != 0 &&
        strcmp(sort_by, "linkrank") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid sort_by '%s'\","
            "\"hint\":\"Valid values: relevance, name, degree, calls, linkrank\"}", sort_by);
        free(label); free(name_pattern); free(qn_pattern); free(unified_pattern);
        free(file_pattern); free(relationship); free(sort_by); free(pe.value);
        return cbm_mcp_text_result(errbuf, true);
    }
    int cfg_search_limit = cbm_config_get_int(srv->config, CBM_CONFIG_SEARCH_LIMIT,
                                               CBM_DEFAULT_SEARCH_LIMIT);
    int limit = cbm_mcp_get_int_arg(args, "limit", cfg_search_limit);
    /* F4: treat limit<=0 as default */
    if (limit <= 0) limit = cfg_search_limit;
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    bool cfg_compact = cbm_config_get_bool(srv->config, "compact", true);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", cfg_compact);
    char *search_mode = cbm_mcp_get_string_arg(args, "mode");
    /* summary=true alias: avoids mode enum collision with get_code (full|sig|head_tail) */
    if (!search_mode && cbm_mcp_get_bool_arg(args, "summary")) {
        search_mode = heap_strdup("summary"); /* heap_strdup: freed at mode error and normal exit */
    }
    /* F7: validate mode enum — O(1) */
    if (search_mode && strcmp(search_mode, "full") != 0 && strcmp(search_mode, "summary") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid mode '%s'\","
            "\"hint\":\"Valid values: full, summary\"}", search_mode);
        free(label); free(name_pattern); free(qn_pattern); free(unified_pattern);
        free(file_pattern); free(relationship); free(sort_by); free(search_mode); free(pe.value);
        return cbm_mcp_text_result(errbuf, true);
    }
    bool case_sensitive = cbm_mcp_get_bool_arg(args, "case_sensitive");
    int min_degree = cbm_mcp_get_int_arg(args, "min_degree", -1);
    int max_degree = cbm_mcp_get_int_arg(args, "max_degree", -1);
    bool exclude_entry_points = cbm_mcp_get_bool_arg_default(args, "exclude_entry_points", false);
    bool include_connected = cbm_mcp_get_bool_arg_default(args, "include_connected", false);
    /* Default true: prefix match includes myproject.dep.* sub-projects.
     * false: forces exact match (only effective when project set + not glob mode). */
    bool cfg_inc_deps = cbm_config_get_bool(srv->config, "default_include_dependencies", true);
    bool include_dependencies = cbm_mcp_get_bool_arg_default(args, "include_dependencies", cfg_inc_deps);

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
    params.pattern = unified_pattern;
    params.case_sensitive = case_sensitive;
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
    free(unified_pattern);
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
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(query);
        return _res;
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
    char *name = cbm_mcp_get_string_arg(args, "project");
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

    /* Wait for any in-progress pipeline to finish before deleting */
    cbm_pipeline_lock();

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

    cbm_pipeline_unlock();

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

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        return not_indexed;
    }

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

/* Forward declaration: defined after handle_trace_call_path */
static void free_node_contents(cbm_node_t *n);

static char *handle_trace_call_path(cbm_mcp_server_t *srv, const char *args) {
    char *func_name = cbm_mcp_get_string_arg(args, "function_name");
    char *qn_input = cbm_mcp_get_string_arg(args, "qualified_name"); /* cross-tool chaining */
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
    bool cfg_compact_t = cbm_config_get_bool(srv->config, "compact", true);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", cfg_compact_t);



    if (!func_name && !qn_input) {
        free(project);
        free(direction);
        free(qn_input);
        return cbm_mcp_text_result(
            "{\"error\":\"function_name or qualified_name is required\","
            "\"hint\":\"Pass the name of a function to trace, e.g. {\\\"function_name\\\":\\\"main\\\"}\"}", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        return _res;
    }
    /* Validate direction enum */
    if (direction && strcmp(direction, "inbound") != 0 &&
        strcmp(direction, "outbound") != 0 && strcmp(direction, "both") != 0) {
        char errbuf[256];
        snprintf(errbuf, sizeof(errbuf),
            "{\"error\":\"invalid direction '%s'\","
            "\"hint\":\"Valid values: inbound, outbound, both\"}", direction);
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        return cbm_mcp_text_result(errbuf, true);
    }

    if (!direction) {
        direction = heap_strdup("both");
    }

    /* QN-first lookup: if qualified_name provided, resolve to node directly */
    cbm_node_t *qn_node = NULL;
    if (qn_input && store) {
        cbm_node_t qn_tmp = {0};
        if (cbm_store_find_node_by_qn(store, project, qn_input, &qn_tmp) == 0 && qn_tmp.id > 0) {
            qn_node = calloc(1, sizeof(cbm_node_t));
            if (qn_node) {
                *qn_node = qn_tmp;  /* shallow copy; ownership of heap fields transferred */
            } else {
                free_node_contents(&qn_tmp);  /* OOM: free fields to avoid leak */
            }
        }
    }

    /* Find the node by name */
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    if (qn_node) {
        /* Use QN-resolved node directly */
        nodes = qn_node;
        node_count = 1;
    } else {
        cbm_store_find_nodes_by_name(store, project,
            func_name ? func_name : (qn_input ? qn_input : ""), &nodes, &node_count);
    }

    if (node_count == 0 && func_name) {
        /* Fallback: case-insensitive substring search via cbm_store_search.
         * Only fires when exact match misses — zero overhead on hit.
         * Skipped when func_name is NULL (only qualified_name given): QN lookup already ran
         * and returned nothing; falling back with name_pattern=NULL would return random nodes. */
        cbm_search_params_t sp = {0};
        fill_project_params(&pe, &sp);
        sp.name_pattern = func_name;
        sp.case_sensitive = false;
        sp.limit = 5;
        sp.min_degree = -1;
        sp.max_degree = -1;
        cbm_search_output_t sout = {0};
        if (cbm_store_search(store, &sp, &sout) == 0 && sout.count > 0) {
            const char *found_project = sout.results[0].node.project;
            cbm_store_find_nodes_by_name(store,
                                         found_project ? found_project : project,
                                         sout.results[0].node.name, &nodes, &node_count);
        }
        cbm_store_search_free(&sout);
    }
    if (node_count == 0) {
        char errbuf[512];
        if (qn_input && !func_name) {
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"function not found for qualified_name: '%s'\","
                "\"hint\":\"Use search_code_graph with pattern= to find the correct qualified_name, "
                "then pass it here.\"}",
                qn_input);
        } else {
            snprintf(errbuf, sizeof(errbuf),
                "{\"error\":\"function not found: '%s'\","
                "\"hint\":\"Use search_code_graph with name_pattern to find similar symbols.\"}",
                func_name ? func_name : "");
        }
        free(func_name);
        free(qn_input);
        free(project);
        free(direction);
        cbm_store_free_nodes(nodes, node_count);
        return cbm_mcp_text_result(errbuf, true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    /* func_name may be NULL when only qualified_name was passed — use qn_input as fallback */
    yyjson_mut_obj_add_str(doc, root, "function",
        func_name ? func_name : (qn_input ? qn_input : ""));
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
    free(qn_input);
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
    /* Resolve the project slug: accept either a slug or a filesystem path.
     * Also fall back to session_project when project is NULL. */
    const char *slug = NULL;
    char *slug_owned = NULL; /* heap-allocated slug, must free before return */

    if (!project || project[0] == '\0') {
        /* No project arg — use session_project (set by cold-start detect_session) */
        if (srv->session_project[0])
            slug = srv->session_project;
        else
            return NULL;
    } else if (project_is_path(project)) {
        /* Path-based arg: convert to slug (shared helper, same logic as expand_project_param Rule 0) */
        slug_owned = project_slug_from_path(project);
        if (!slug_owned) return NULL;
        slug = slug_owned;
    } else {
        slug = project;
    }

    cbm_store_t *store = resolve_store(srv, slug);
    if (!store) {
        free(slug_owned);
        return NULL;
    }
    cbm_project_t proj = {0};
    if (cbm_store_get_project(store, slug, &proj) != CBM_STORE_OK) {
        free(slug_owned);
        return NULL;
    }
    char *root = heap_strdup(proj.root_path);
    free((void *)proj.name);
    free((void *)proj.indexed_at);
    free((void *)proj.root_path);
    free(slug_owned);
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

    /* Close cached store — pipeline will delete + recreate the .db file */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;

    /* Serialize pipeline runs to prevent concurrent writes */
    cbm_pipeline_lock();
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_unlock();

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


            /* Check ADR presence and suggest creation if missing */
            char adr_path[4096];
            snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", repo_path);
            struct stat adr_st;
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            bool adr_exists = (stat(adr_path, &adr_st) == 0);
            yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
            if (!adr_exists) {
                yyjson_mut_obj_add_str(
                    doc, root, "adr_hint",
                    "Project indexed. Consider creating an Architecture Decision Record: "
                    "explore the codebase with get_architecture(aspects=['all']), then use "
                    "manage_adr(mode='store') to persist architectural insights across sessions.");
            }
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
             "%d matches for \"%s\". Pick a qualified_name from suggestions below, "
             "or use search_graph(name_pattern=\"...\") to narrow results.",
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

    /* Build absolute path and verify it's within the project root.
     * Prevents path traversal via crafted file_path (e.g., "../../.ssh/id_rsa"). */
    char *abs_path = NULL;
    if (root_path && node->file_path) {
        size_t apsz = strlen(root_path) + strlen(node->file_path) + 2;
        abs_path = malloc(apsz);
        snprintf(abs_path, apsz, "%s/%s", root_path, node->file_path);

        /* Path containment: resolve symlinks/../ and verify file stays within root */
        char real_root[4096];
        char real_file[4096];
        bool path_ok = false;
#ifdef _WIN32
        if (_fullpath(real_root, root_path, sizeof(real_root)) &&
            _fullpath(real_file, abs_path, sizeof(real_file))) {
#else
        if (realpath(root_path, real_root) && realpath(abs_path, real_file)) {
#endif
            size_t root_len = strlen(real_root);
            if (strncmp(real_file, real_root, root_len) == 0 &&
                (real_file[root_len] == '/' || real_file[root_len] == '\\' ||
                 real_file[root_len] == '\0')) {
                path_ok = true;
            }
        }
        if (path_ok) {

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
    }   /* end if (path_ok) */
    }   /* end if (root_path && node->file_path) */

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
        project = extract_project_from_qn(qn);
        if (project) {
            eff_project = project;
            store = resolve_store(srv, project);
        } else if (srv->current_project && srv->current_project[0]) {
            eff_project = srv->current_project;
        }
    }
    bool cfg_compact_g = cbm_config_get_bool(srv->config, "compact", true);
    bool compact = cbm_mcp_get_bool_arg_default(args, "compact", cfg_compact_g);
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

    REQUIRE_STORE(store, project);

    /* eff_project already set via resolve_project_store + QN extraction fallback */

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

    /* Tier 2: Suffix match — handles partial QNs ("main.HandleRequest")
     * and short names ("ProcessOrder") via LIKE '%.X'. */
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

/* ── search_code v2: graph-augmented code search ─────────────── */

/* Strip non-ASCII bytes to guarantee valid UTF-8 JSON output */
enum { ASCII_MAX = 127 };
static void sanitize_ascii(char *s) {
    for (unsigned char *p = (unsigned char *)s; *p; p++) {
        if (*p > ASCII_MAX) {
            *p = '?';
        }
    }
}

/* Intermediate grep match */
typedef struct {
    char file[512];
    int line;
    char content[1024];
} grep_match_t;

/* Deduped result: one per containing graph node */
typedef struct {
    int64_t node_id; /* 0 = raw match (no containing node) */
    char node_name[256];
    char qualified_name[512];
    char label[64];
    char file[512];
    int start_line;
    int end_line;
    int in_degree;
    int out_degree;
    int score;
    int match_lines[64];
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

/* Build the grep command string based on scoped vs recursive mode */
static void build_grep_cmd(char *cmd, size_t cmd_sz, bool use_regex, bool scoped,
                           const char *file_pattern, const char *tmpfile, const char *filelist,
                           const char *root_path) {
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    const char *flag = use_regex ? "-E" : "-F";
    if (scoped) {
        if (file_pattern) {
            snprintf(cmd, cmd_sz, "xargs grep -n %s --include='%s' -f '%s' < '%s' 2>/dev/null",
                     flag, file_pattern, tmpfile, filelist);
        } else {
            snprintf(cmd, cmd_sz, "xargs grep -n %s -f '%s' < '%s' 2>/dev/null", flag, tmpfile,
                     filelist);
        }
    } else {
        if (file_pattern) {
            snprintf(cmd, cmd_sz, "grep -rn %s --include='%s' -f '%s' '%s' 2>/dev/null", flag,
                     file_pattern, tmpfile, root_path);
        } else {
            snprintf(cmd, cmd_sz, "grep -rn %s -f '%s' '%s' 2>/dev/null", flag, tmpfile, root_path);
        }
    }
}

/* Phase 4: assemble JSON output from search results */
static char *assemble_search_output(search_result_t *sr, int sr_count, grep_match_t *raw,
                                    int raw_count, int gm_count, int limit, int mode,
                                    int context_lines, const char *root_path) {
    enum { MODE_COMPACT = 0, MODE_FULL = 1, MODE_FILES = 2 };

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    int output_count = sr_count < limit ? sr_count : limit;

    if (mode == MODE_FILES) {
        yyjson_mut_val *files_arr = yyjson_mut_arr(doc);
        char *seen_files[512];
        int seen_count = 0;
        for (int fi = 0; fi < output_count; fi++) {
            bool dup = false;
            for (int j = 0; j < seen_count; j++) {
                if (strcmp(seen_files[j], sr[fi].file) == 0) {
                    dup = true;
                    break;
                }
            }
            if (!dup && seen_count < 512) {
                seen_files[seen_count++] = sr[fi].file;
                yyjson_mut_arr_add_str(doc, files_arr, sr[fi].file);
            }
        }
        for (int fi = 0; fi < raw_count && seen_count < 512; fi++) {
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
        yyjson_mut_obj_add_val(doc, root_obj, "files", files_arr);
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

            if (r->start_line > 0 && r->end_line > 0) {
                char abs_path[1024];
                snprintf(abs_path, sizeof(abs_path), "%s/%s", root_path, r->file);

                if (mode == MODE_FULL) {
                    char *source = read_file_lines(abs_path, r->start_line, r->end_line);
                    if (source) {
                        sanitize_ascii(source);
                        yyjson_mut_obj_add_strcpy(doc, item, "source", source);
                        free(source);
                    }
                } else if (context_lines > 0 && r->match_count > 0) {
                    int first_match = r->match_lines[0];
                    int last_match = r->match_lines[r->match_count - 1];
                    int ctx_start = first_match - context_lines;
                    int ctx_end = last_match + context_lines;
                    if (ctx_start < 1) {
                        ctx_start = 1;
                    }
                    char *ctx = read_file_lines(abs_path, ctx_start, ctx_end);
                    if (ctx) {
                        sanitize_ascii(ctx);
                        yyjson_mut_obj_add_strcpy(doc, item, "context", ctx);
                        yyjson_mut_obj_add_int(doc, item, "context_start", ctx_start);
                        free(ctx);
                    }
                }
            }

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

    /* Directory distribution */
    {
        yyjson_mut_val *dirs = yyjson_mut_obj(doc);
        char dir_names[64][128];
        int dir_counts[64];
        int dir_n = 0;
        for (int di = 0; di < sr_count; di++) {
            char top[128] = "";
            const char *slash = strchr(sr[di].file, '/');
            if (slash) {
                size_t dlen = (size_t)(slash - sr[di].file + 1);
                if (dlen >= sizeof(top)) {
                    dlen = sizeof(top) - 1;
                }
                memcpy(top, sr[di].file, dlen);
                top[dlen] = '\0';
            } else {
                snprintf(top, sizeof(top), "%s", sr[di].file);
            }
            int found = -1;
            for (int d = 0; d < dir_n; d++) {
                if (strcmp(dir_names[d], top) == 0) {
                    found = d;
                    break;
                }
            }
            if (found >= 0) {
                dir_counts[found]++;
            } else if (dir_n < 64) {
                snprintf(dir_names[dir_n], sizeof(dir_names[0]), "%s", top);
                dir_counts[dir_n] = 1;
                dir_n++;
            }
        }
        for (int d = 0; d < dir_n; d++) {
            yyjson_mut_val *key = yyjson_mut_strcpy(doc, dir_names[d]);
            yyjson_mut_val *val = yyjson_mut_int(doc, dir_counts[d]);
            yyjson_mut_obj_add(dirs, key, val);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "directories", dirs);
    }

    /* Summary stats */
    yyjson_mut_obj_add_int(doc, root_obj, "total_grep_matches", gm_count);
    yyjson_mut_obj_add_int(doc, root_obj, "total_results", sr_count);
    yyjson_mut_obj_add_int(doc, root_obj, "raw_match_count", raw_count);
    if (sr_count > 0 && gm_count > 0) {
        char ratio[32];
        snprintf(ratio, sizeof(ratio), "%.1fx", (double)gm_count / (double)(sr_count + raw_count));
        yyjson_mut_obj_add_strcpy(doc, root_obj, "dedup_ratio", ratio);
    }

    char *json = yy_doc_to_str(doc);
    if (json) {
        sanitize_ascii(json);
    }
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static char *handle_search_code(cbm_mcp_server_t *srv, const char *args) {
    char *pattern = cbm_mcp_get_string_arg(args, "pattern");
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *path_filter = cbm_mcp_get_string_arg(args, "path_filter");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    int context_lines = cbm_mcp_get_int_arg(args, "context", 0);
    int cfg_search_limit_sc = cbm_config_get_int(srv->config, CBM_CONFIG_SEARCH_LIMIT,
                                                CBM_DEFAULT_SEARCH_LIMIT);
    int limit = cbm_mcp_get_int_arg(args, "limit", cfg_search_limit_sc);
    bool use_regex = cbm_mcp_get_bool_arg(args, "regex");

    /* Parse mode: compact (default), full, files */
    enum { MODE_COMPACT, MODE_FULL, MODE_FILES };
    int mode = MODE_COMPACT;
    if (mode_str) {
        if (strcmp(mode_str, "full") == 0) {
            mode = MODE_FULL;
        } else if (strcmp(mode_str, "files") == 0) {
            mode = MODE_FILES;
        }
        free(mode_str);
    }

    /* Compile path_filter regex if provided */
    cbm_regex_t path_regex;
    bool has_path_filter = false;
    if (path_filter && path_filter[0]) {
        if (cbm_regcomp(&path_regex, path_filter, CBM_REG_EXTENDED | CBM_REG_NOSUB) == CBM_REG_OK) {
            has_path_filter = true;
        }
        free(path_filter);
        path_filter = NULL;
    } else {
        free(path_filter);
        path_filter = NULL;
    }

    if (!pattern) {
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result(
            "{\"error\":\"pattern is required\","
            "\"hint\":\"Pass a text pattern or regex (with regex:true) to search source code.\"}", true);
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

    /* Reject shell metacharacters in user-supplied arguments */
    if (!cbm_validate_shell_arg(root_path) ||
        (file_pattern && !cbm_validate_shell_arg(file_pattern))) {
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result("path or file_pattern contains invalid characters", true);
    }

    /* ── Phase 1: Grep scan ──────────────────────────────────── */

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

    /* Case-sensitivity: default case-insensitive, opt-in sensitive. */
    bool case_sensitive = cbm_mcp_get_bool_arg(args, "case_sensitive");
    /* Use case_sensitive with scoped grep via build_grep_cmd */
    (void)case_sensitive; /* TODO: pass to build_grep_cmd */

    /* No grep-level match limit — let grep find all matches, then dedup and
     * cap in our code. The -m flag caused results from large vendored files
     * to exhaust the quota before reaching project source files. */
    enum { GREP_MAX_MATCHES = 500 };
    int grep_limit = GREP_MAX_MATCHES;

    /* Scope grep to indexed files only — avoids scanning vendored/generated code.
     * Query the graph for distinct file paths, write them to a temp file,
     * then use xargs to pass them to grep. Falls back to recursive grep if
     * no indexed files found (project not fully indexed). */
    char filelist[256];
    snprintf(filelist, sizeof(filelist), "%s.files", tmpfile);
    bool scoped = false;

    cbm_store_t *pre_store = resolve_store(srv, project);
    if (pre_store) {
        char **indexed_files = NULL;
        int indexed_count = 0;
        if (cbm_store_list_files(pre_store, project, &indexed_files, &indexed_count) ==
                CBM_STORE_OK &&
            indexed_count > 0) {
            FILE *fl = fopen(filelist, "w");
            if (fl) {
                for (int fi = 0; fi < indexed_count; fi++) {
                    fprintf(fl, "%s/%s\n", root_path, indexed_files[fi]);
                }
                fclose(fl);
                scoped = true;
            }
            for (int fi = 0; fi < indexed_count; fi++) {
                free(indexed_files[fi]);
            }
            free(indexed_files);
        }
    }

    char cmd[4096];
    build_grep_cmd(cmd, sizeof(cmd), use_regex, scoped, file_pattern, tmpfile, filelist, root_path);

    // NOLINTNEXTLINE(bugprone-command-processor,cert-env33-c)
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
        return cbm_mcp_text_result(
            "{\"error\":\"search failed: grep command could not execute\","
            "\"hint\":\"Check that grep is installed and the project root directory exists.\"}", true);
    }

    /* Collect grep matches into array */
    int gm_cap = 64;
    int gm_count = 0;
    grep_match_t *gm = malloc(gm_cap * sizeof(grep_match_t));
    char line[2048];
    size_t root_len = strlen(root_path);

    while (fgets(line, sizeof(line), fp) && gm_count < grep_limit) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

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

        const char *file = line;
        if (strncmp(file, root_path, root_len) == 0) {
            file += root_len;
            if (*file == '/') {
                file++;
            }
        }

        /* Apply path_filter regex — skip files that don't match */
        if (has_path_filter && cbm_regexec(&path_regex, file, 0, NULL, 0) != CBM_REG_OK) {
            continue;
        }

        if (gm_count >= gm_cap) {
            gm_cap *= 2;
            gm = safe_realloc(gm, gm_cap * sizeof(grep_match_t));
        }
        snprintf(gm[gm_count].file, sizeof(gm[0].file), "%s", file);
        gm[gm_count].line = (int)strtol(colon1 + 1, NULL, 10);
        snprintf(gm[gm_count].content, sizeof(gm[0].content), "%s", colon2 + 1);
        sanitize_ascii(gm[gm_count].content);
        gm_count++;
    }
    cbm_pclose(fp);
    cbm_unlink(tmpfile);
    if (scoped) {
        cbm_unlink(filelist);
    }

    /* ── Phase 2+3: Block expansion + graph ranking ──────────── */
    /* Sort grep matches by file for contiguous processing.
     * Then: one SQL query per unique file for nodes, one batch query for all degrees. */

    cbm_store_t *store = resolve_store(srv, project);

    int sr_cap = 32;
    int sr_count = 0;
    search_result_t *sr = calloc(sr_cap, sizeof(search_result_t));

    int raw_cap = 32;
    int raw_count = 0;
    grep_match_t *raw = malloc(raw_cap * sizeof(grep_match_t));

    /* Sort matches by file path for contiguous per-file processing */
    qsort(gm, gm_count, sizeof(grep_match_t), (int (*)(const void *, const void *))strcmp);

    /* Process matches file-by-file (contiguous runs after sort) */
    int i = 0;
    while (i < gm_count) {
        const char *cur_file = gm[i].file;
        int file_start = i;

        /* Find end of this file's run */
        while (i < gm_count && strcmp(gm[i].file, cur_file) == 0) {
            i++;
        }
        int file_end = i; /* [file_start, file_end) */

        /* One SQL query: load all nodes in this file */
        cbm_node_t *file_nodes = NULL;
        int file_node_count = 0;
        if (store) {
            cbm_store_find_nodes_by_file(store, project, cur_file, &file_nodes, &file_node_count);
        }

        /* Match each grep hit to tightest containing node (in-memory) */
        for (int mi = file_start; mi < file_end; mi++) {
            int best = -1;
            int best_span = MAX_LINE_SPAN;
            for (int j = 0; j < file_node_count; j++) {
                if (file_nodes[j].start_line <= gm[mi].line &&
                    file_nodes[j].end_line >= gm[mi].line) {
                    int span = file_nodes[j].end_line - file_nodes[j].start_line;
                    if (span < best_span) {
                        best = j;
                        best_span = span;
                    }
                }
            }

            if (best >= 0) {
                cbm_node_t *n = &file_nodes[best];

                /* Dedup: check if node already in results */
                int existing = -1;
                for (int j = 0; j < sr_count; j++) {
                    if (sr[j].node_id == n->id) {
                        existing = j;
                        break;
                    }
                }

                if (existing >= 0) {
                    if (sr[existing].match_count < 64) {
                        sr[existing].match_lines[sr[existing].match_count++] = gm[mi].line;
                    }
                } else {
                    if (sr_count >= sr_cap) {
                        sr_cap *= 2;
                        sr = safe_realloc(sr, sr_cap * sizeof(search_result_t));
                        memset(&sr[sr_count], 0, (sr_cap - sr_count) * sizeof(search_result_t));
                    }
                    search_result_t *r = &sr[sr_count];
                    r->node_id = n->id;
                    snprintf(r->node_name, sizeof(r->node_name), "%s", n->name ? n->name : "");
                    snprintf(r->qualified_name, sizeof(r->qualified_name), "%s",
                             n->qualified_name ? n->qualified_name : "");
                    snprintf(r->label, sizeof(r->label), "%s", n->label ? n->label : "");
                    snprintf(r->file, sizeof(r->file), "%s", n->file_path ? n->file_path : "");
                    r->start_line = n->start_line;
                    r->end_line = n->end_line;
                    r->match_lines[0] = gm[mi].line;
                    r->match_count = 1;
                    sr_count++;
                }
            } else {
                if (raw_count >= raw_cap) {
                    raw_cap *= 2;
                    raw = safe_realloc(raw, raw_cap * sizeof(grep_match_t));
                }
                raw[raw_count++] = gm[mi];
            }
        }

        /* Free file nodes */
        for (int j = 0; j < file_node_count; j++) {
            free((void *)file_nodes[j].project);
            free((void *)file_nodes[j].label);
            free((void *)file_nodes[j].name);
            free((void *)file_nodes[j].qualified_name);
            free((void *)file_nodes[j].file_path);
            free((void *)file_nodes[j].properties_json);
        }
        free(file_nodes);
    }

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
    if (sr_count > 1) {
        qsort(sr, sr_count, sizeof(search_result_t), search_result_cmp);
    }

    /* ── Phase 4: Context assembly (extracted helper) ─────────── */

    char *result = assemble_search_output(sr, sr_count, raw, raw_count, gm_count, limit, mode,
                                          context_lines, root_path);
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

static char *handle_detect_changes(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *base_branch = cbm_mcp_get_string_arg(args, "base_branch");
    int depth = cbm_mcp_get_int_arg(args, "depth", 2);

    if (!base_branch) {
        base_branch = heap_strdup("main");
    }

    /* Reject shell metacharacters in user-supplied branch name */
    if (!cbm_validate_shell_arg(base_branch)) {
        free(project);
        free(base_branch);
        return cbm_mcp_text_result("base_branch contains invalid characters", true);
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(project);
        free(base_branch);
        return cbm_mcp_text_result(
            "{\"error\":\"project not found\","
            "\"hint\":\"Pass project='/path/to/repo' or project='~/path/to/repo' to specify the project. "
            "Run index_repository with repo_path to index it first, "
            "or use list_projects to see available projects.\"}", true);
    }

    if (!cbm_validate_shell_arg(root_path)) {
        free(root_path);
        free(project);
        free(base_branch);
        return cbm_mcp_text_result("project path contains invalid characters", true);
    }

    /* Get changed files via git (-C avoids cd + quoting issues on Windows) */
    char cmd[2048];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" diff --name-only \"%s\"...HEAD 2>NUL & "
             "git -C \"%s\" diff --name-only 2>NUL",
             root_path, base_branch, root_path);
#else
    snprintf(cmd, sizeof(cmd),
             "{ git -C '%s' diff --name-only '%s'...HEAD 2>/dev/null; "
             "git -C '%s' diff --name-only 2>/dev/null; } | sort -u",
             root_path, base_branch, root_path);
#endif

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
    char *adr_buf = NULL; /* freed after yy_doc_to_str — yyjson holds pointer, not copy */

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
            "\"hint\":\"Pass project='/path/to/repo' or project='~/path/to/repo' to specify the project. "
            "Run index_repository with repo_path to index it first, "
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
            adr_buf = malloc(sz + 1);
            size_t n = fread(adr_buf, 1, sz, fp);
            adr_buf[n] = '\0';
            (void)fclose(fp);
            yyjson_mut_obj_add_str(doc, root_obj, "content", adr_buf);
            /* do NOT free adr_buf here: yyjson stores the pointer, not a copy */
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "content", "");
            yyjson_mut_obj_add_str(doc, root_obj, "status", "no_adr");
            yyjson_mut_obj_add_str(
                doc, root_obj, "adr_hint",
                "No ADR yet. Create one with manage_adr(mode='update', "
                "content='## PURPOSE\\n...\\n\\n## STACK\\n...\\n\\n## ARCHITECTURE\\n..."
                "\\n\\n## PATTERNS\\n...\\n\\n## TRADEOFFS\\n...\\n\\n## PHILOSOPHY\\n...'). "
                "For guided creation: explore the codebase with get_architecture, "
                "then draft and store. Sections: PURPOSE, STACK, ARCHITECTURE, "
                "PATTERNS, TRADEOFFS, PHILOSOPHY.");
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(adr_buf); /* safe to free now — doc has been serialized */
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
        /* Check if search_in="source" → route to search_code handler */
        char *si = cbm_mcp_get_string_arg(args_json, "search_in");
        bool src = si && strcmp(si, "source") == 0;
        free(si);
        if (src) return handle_search_code(srv, args_json);
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
        const char *home = cbm_get_home_dir();
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

    /* Block until any concurrent pipeline finishes */
    cbm_pipeline_lock();
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_unlock();

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
    bool needs_index = true;
    char db_check[1024] = {0};
    const char *home = cbm_get_home_dir();
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
    if (!cbm_validate_shell_arg(srv->session_root)) {
        cbm_log_warn("autoindex.skip", "reason", "path contains shell metacharacters");
        return;
    }
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
        result_json = cbm_mcp_initialize_response(req.params_raw);
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

        struct timespec t0;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
        result_json = cbm_mcp_handle_tool(srv, tool_name, tool_args);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - t0.tv_sec) * 1000000LL) +
                           ((long long)(t1.tv_nsec - t0.tv_nsec) / 1000LL);
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        bool is_err = (result_json != NULL) && (strstr(result_json, "\"isError\":true") != NULL);
        cbm_diag_record_query(dur_us, is_err);

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
        DWORD wr = WaitForSingleObject(hStdin, STORE_IDLE_TIMEOUT_S * 1000);
        if (wr == WAIT_FAILED) {
            break;
        }
        if (wr == WAIT_TIMEOUT) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            continue;
        }
#else
        /* Phase 1: non-blocking poll — catches data already in the kernel fd
         * AND handles the case where a prior getline() drained the kernel fd
         * into libc's FILE* buffer (raw fd appears empty but data is buffered).
         * We always try a zero-timeout poll first; if it misses buffered data,
         * phase 2 below catches it via an explicit FILE* peek. */
        struct pollfd pfd = {.fd = fd, .events = POLLIN};
        int pr = poll(&pfd, 1, 0); /* non-blocking */

        if (pr < 0) {
            break; /* error or signal */
        }
        if (pr == 0) {
            /* Raw fd appears empty. Check whether libc has already buffered
             * data from a previous over-read by peeking one byte via fgetc.
             * IMPORTANT: temporarily set O_NONBLOCK so fgetc() returns
             * immediately when the FILE* buffer AND kernel fd are both empty
             * (EAGAIN → EOF + ferror). Without this, fgetc() on a blocking fd
             * would block indefinitely, preventing the Phase 3 idle eviction
             * timeout from ever firing. */
            int saved_flags = fcntl(fd, F_GETFL);
            if (saved_flags < 0) {
                /* fcntl failed (should not happen on a valid fd) — skip the
                 * FILE* peek and fall straight through to the blocking poll so
                 * idle eviction still fires on timeout. */
                pr = poll(&pfd, 1, STORE_IDLE_TIMEOUT_S * 1000);
                if (pr < 0) {
                    break;
                }
                if (pr == 0) {
                    cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
                    continue;
                }
            } else {
                (void)fcntl(fd, F_SETFL, saved_flags | O_NONBLOCK);
                int c = fgetc(in);
                (void)fcntl(fd, F_SETFL, saved_flags); /* restore blocking */
                if (c == EOF) {
                    if (feof(in)) {
                        break; /* true EOF */
                    }
                    /* No buffered data (EAGAIN from non-blocking read) — clear
                     * the ferror indicator set by EAGAIN, then blocking poll. */
                    clearerr(in);
                    pr = poll(&pfd, 1, STORE_IDLE_TIMEOUT_S * 1000);
                    if (pr < 0) {
                        break;
                    }
                    if (pr == 0) {
                        cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
                        continue;
                    }
                } else {
                    /* Buffered data found — push back and fall through to getline */
                    (void)ungetc(c, in);
                }
            }
        }
#endif

        if (cbm_getline(&line, &cap, in) <= 0) {
            break;
        }

        /* Trim trailing newline/CR */
        size_t len = strlen(line);
        while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        /* Content-Length framing support (LSP-style transport).
         * Some MCP clients (OpenCode, VS Code extensions) send:
         *   Content-Length: <n>\r\n\r\n<json>
         * instead of bare JSONL. Detect the header, read the payload,
         * and respond with the same framing. */
        if (strncmp(line, "Content-Length:", 15) == 0) {
            int content_len = (int)strtol(line + 15, NULL, 10);
            if (content_len <= 0 || content_len > 10 * 1024 * 1024) {
                continue; /* invalid or too large */
            }

            /* Skip blank line(s) between header and body */
            while (cbm_getline(&line, &cap, in) > 0) {
                size_t hlen = strlen(line);
                while (hlen > 0 && (line[hlen - 1] == '\n' || line[hlen - 1] == '\r')) {
                    line[--hlen] = '\0';
                }
                if (hlen == 0) {
                    break; /* found the blank separator */
                }
                /* Skip other headers (e.g. Content-Type) */
            }

            /* Read exact content_len bytes */
            char *body = malloc((size_t)content_len + 1);
            if (!body) {
                continue;
            }
            size_t nread = fread(body, 1, (size_t)content_len, in);
            body[nread] = '\0';

            char *resp = cbm_mcp_server_handle(srv, body);
            free(body);

            if (resp) {
                size_t rlen = strlen(resp);
                (void)fprintf(out, "Content-Length: %zu\r\n\r\n%s", rlen, resp);
                (void)fflush(out);
                free(resp);
            }
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
