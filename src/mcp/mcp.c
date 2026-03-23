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
#include "cli/cli.h"
#include "watcher/watcher.h"
#include "foundation/mem.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/str_util.h"
#include "foundation/compat_regex.h"

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
     "precise results in one call.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"label\":{\"type\":"
     "\"string\"},\"name_pattern\":{\"type\":\"string\"},\"qn_pattern\":{\"type\":\"string\"},"
     "\"file_pattern\":{\"type\":\"string\"},\"relationship\":{\"type\":\"string\"},\"min_degree\":"
     "{\"type\":\"integer\"},\"max_degree\":{\"type\":\"integer\"},\"exclude_entry_points\":{"
     "\"type\":\"boolean\"},\"include_connected\":{\"type\":\"boolean\"},\"limit\":{\"type\":"
     "\"integer\",\"description\":\"Max results. Default: "
     "unlimited\"},\"offset\":{\"type\":\"integer\",\"default\":0}}}"},

    {"query_graph",
     "Execute a Cypher query against the knowledge graph for complex multi-hop patterns, "
     "aggregations, and cross-service analysis.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\"},\"max_rows\":{\"type\":\"integer\","
     "\"description\":"
     "\"Optional row limit. Default: unlimited (100k ceiling)\"}},\"required\":[\"query\"]}"},

    {"trace_call_path",
     "Trace function call paths — who calls a function and what it calls. Use INSTEAD OF grep when "
     "finding callers, dependencies, or impact analysis.",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\"},\"project\":{"
     "\"type\":\"string\"},\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\","
     "\"both\"],\"default\":\"both\"},\"depth\":{\"type\":\"integer\",\"default\":3},\"edge_"
     "types\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"function_"
     "name\"]}"},

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
     "structure at a glance.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"aspects\":{\"type\":"
     "\"array\",\"items\":{\"type\":\"string\"}}}}"},

    {"search_code",
     "Graph-augmented code search. Finds text patterns via grep, then enriches results with "
     "the knowledge graph: deduplicates matches into containing functions, ranks by structural "
     "importance (definitions first, popular functions next, tests last). "
     "Modes: compact (default, signatures only — token efficient), full (with source), "
     "files (just file paths). Use path_filter regex to scope results.",
     "{\"type\":\"object\",\"properties\":{\"pattern\":{\"type\":\"string\"},\"project\":{\"type\":"
     "\"string\"},\"file_pattern\":{\"type\":\"string\",\"description\":\"Glob for grep "
     "--include (e.g. *.go)\"},\"path_filter\":{\"type\":\"string\",\"description\":\"Regex "
     "filter on result file paths (e.g. ^src/ or \\\\.(go|ts)$)\"},\"mode\":{\"type\":\"string\","
     "\"enum\":[\"compact\",\"full\",\"files\"],\"default\":\"compact\",\"description\":\"compact: "
     "signatures+metadata (default). full: with source. files: just file list.\"},"
     "\"context\":{\"type\":\"integer\",\"description\":\"Lines of context around each match "
     "(like grep -C). Only used in compact mode.\"},"
     "\"regex\":{\"type\":\"boolean\",\"default\":false},\"limit\":{\"type\":\"integer\","
     "\"description\":\"Max results (default 10)\",\"default\":10}},\"required\":["
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
     "{\"type\":\"object\",\"properties\":{\"traces\":{\"type\":\"array\",\"items\":{\"type\":"
     "\"object\"}},\"project\":{\"type\":"
     "\"string\"}},\"required\":[\"traces\"]}"},
};

static const int TOOL_COUNT = sizeof(TOOLS) / sizeof(TOOLS[0]);

char *cbm_mcp_tools_list(void) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *tools = yyjson_mut_arr(doc);

    for (int i = 0; i < TOOL_COUNT; i++) {
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

        yyjson_mut_arr_add_val(tools, tool);
    }

    yyjson_mut_obj_add_val(doc, root, "tools", tools);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
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
    yyjson_mut_obj_add_str(doc, impl, "version", "0.10.0");
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
};

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

/* ── Store resolution ──────────────────────────────────────────── */

/* Open the right project's .db file for query tools.
 * Caches the connection — reopens only when project changes.
 * Tracks last-access time so the event loop can evict idle stores. */
static cbm_store_t *resolve_store(cbm_mcp_server_t *srv, const char *project) {
    if (!project) {
        return srv->store; /* no project specified → use whatever's open */
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

    /* Open project's .db file */
    char path[1024];
    project_db_path(project, path, sizeof(path));
    srv->store = cbm_store_open_path(path);
    srv->owns_store = true;
    free(srv->current_project);
    srv->current_project = heap_strdup(project);

    return srv->store;
}

/* Bail with empty JSON result when no store is available. */
#define REQUIRE_STORE(store, project)                                              \
    do {                                                                           \
        if (!(store)) {                                                            \
            free(project);                                                         \
            return cbm_mcp_text_result("{\"error\":\"no project loaded\"}", true); \
        }                                                                          \
    } while (0)

/* ── Tool handler implementations ─────────────────────────────── */

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

            /* Skip temp/internal files */
            if (strncmp(name, "tmp-", 4) == 0 || strncmp(name, "_", 1) == 0 ||
                strncmp(name, ":memory:", 8) == 0) {
                continue;
            }

            /* Extract project name = filename without .db suffix */
            char project_name[1024];
            snprintf(project_name, sizeof(project_name), "%.*s", (int)(len - 3), name);

            /* Get file metadata */
            char full_path[2048];
            snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);
            struct stat st;
            if (stat(full_path, &st) != 0) {
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
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
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

static char *handle_search_graph(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);
    char *label = cbm_mcp_get_string_arg(args, "label");
    char *name_pattern = cbm_mcp_get_string_arg(args, "name_pattern");
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    int limit = cbm_mcp_get_int_arg(args, "limit", 500000);
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    int min_degree = cbm_mcp_get_int_arg(args, "min_degree", -1);
    int max_degree = cbm_mcp_get_int_arg(args, "max_degree", -1);

    cbm_search_params_t params = {
        .project = project,
        .label = label,
        .name_pattern = name_pattern,
        .file_pattern = file_pattern,
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

    yyjson_mut_obj_add_int(doc, root, "total", out.total);

    yyjson_mut_val *results = yyjson_mut_arr(doc);
    for (int i = 0; i < out.count; i++) {
        cbm_search_result_t *sr = &out.results[i];
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "name", sr->node.name ? sr->node.name : "");
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               sr->node.qualified_name ? sr->node.qualified_name : "");
        yyjson_mut_obj_add_str(doc, item, "label", sr->node.label ? sr->node.label : "");
        yyjson_mut_obj_add_str(doc, item, "file_path",
                               sr->node.file_path ? sr->node.file_path : "");
        yyjson_mut_obj_add_int(doc, item, "in_degree", sr->in_degree);
        yyjson_mut_obj_add_int(doc, item, "out_degree", sr->out_degree);
        yyjson_mut_arr_add_val(results, item);
    }
    yyjson_mut_obj_add_val(doc, root, "results", results);
    yyjson_mut_obj_add_bool(doc, root, "has_more", out.total > offset + out.count);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_search_free(&out);

    free(project);
    free(label);
    free(name_pattern);
    free(file_pattern);

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
        free(project);
        free(query);
        return cbm_mcp_text_result("{\"error\":\"no project loaded\"}", true);
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
        return cbm_mcp_text_result("project_name is required", true);
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
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, project, &schema);

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
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    int depth = cbm_mcp_get_int_arg(args, "depth", 3);

    if (!func_name) {
        free(project);
        free(direction);
        return cbm_mcp_text_result("function_name is required", true);
    }
    if (!store) {
        free(func_name);
        free(project);
        free(direction);
        return cbm_mcp_text_result("{\"error\":\"no project loaded\"}", true);
    }
    if (!direction) {
        direction = heap_strdup("both");
    }

    /* Find the node by name */
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    cbm_store_find_nodes_by_name(store, project, func_name, &nodes, &node_count);

    if (node_count == 0) {
        free(func_name);
        free(project);
        free(direction);
        cbm_store_free_nodes(nodes, 0);
        return cbm_mcp_text_result("{\"error\":\"function not found\"}", true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "function", func_name);
    yyjson_mut_obj_add_str(doc, root, "direction", direction);

    const char *edge_types[] = {"CALLS"};
    int edge_type_count = 1;

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
        cbm_store_bfs(store, nodes[0].id, "outbound", edge_types, edge_type_count, depth, 100,
                      &tr_out);

        yyjson_mut_val *callees = yyjson_mut_arr(doc);
        for (int i = 0; i < tr_out.visited_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   tr_out.visited[i].node.name ? tr_out.visited[i].node.name : "");
            yyjson_mut_obj_add_str(
                doc, item, "qualified_name",
                tr_out.visited[i].node.qualified_name ? tr_out.visited[i].node.qualified_name : "");
            yyjson_mut_obj_add_int(doc, item, "hop", tr_out.visited[i].hop);
            yyjson_mut_arr_add_val(callees, item);
        }
        yyjson_mut_obj_add_val(doc, root, "callees", callees);
    }

    if (do_inbound) {
        cbm_store_bfs(store, nodes[0].id, "inbound", edge_types, edge_type_count, depth, 100,
                      &tr_in);

        yyjson_mut_val *callers = yyjson_mut_arr(doc);
        for (int i = 0; i < tr_in.visited_count; i++) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, item, "name",
                                   tr_in.visited[i].node.name ? tr_in.visited[i].node.name : "");
            yyjson_mut_obj_add_str(
                doc, item, "qualified_name",
                tr_in.visited[i].node.qualified_name ? tr_in.visited[i].node.qualified_name : "");
            yyjson_mut_obj_add_int(doc, item, "hop", tr_in.visited[i].hop);
            yyjson_mut_arr_add_val(callers, item);
        }
        yyjson_mut_obj_add_val(doc, root, "callers", callers);
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
        return cbm_mcp_text_result("repo_path is required", true);
    }

    cbm_index_mode_t mode = CBM_MODE_FULL;
    if (mode_str && strcmp(mode_str, "fast") == 0) {
        mode = CBM_MODE_FAST;
    }
    free(mode_str);

    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, NULL, mode);
    if (!p) {
        free(repo_path);
        return cbm_mcp_text_result("failed to create pipeline", true);
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
            int nodes = cbm_store_count_nodes(store, project_name);
            int edges = cbm_store_count_edges(store, project_name);
            yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
            yyjson_mut_obj_add_int(doc, root, "edges", edges);

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

/* Build an enriched snippet response for a resolved node. */
static char *build_snippet_response(cbm_mcp_server_t *srv, cbm_node_t *node,
                                    const char *match_method, bool include_neighbors,
                                    cbm_node_t *alternatives, int alt_count) {
    char *root_path = get_project_root(srv, node->project);

    int start = node->start_line > 0 ? node->start_line : 1;
    int end = node->end_line > start ? node->end_line : start + SNIPPET_DEFAULT_LINES;
    char *source = NULL;

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
            source = read_file_lines(abs_path, start, end);
        }
    }

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
                        yyjson_mut_obj_add_str(doc, root_obj, k, yyjson_get_str(val));
                    } else if (yyjson_is_bool(val)) {
                        yyjson_mut_obj_add_bool(doc, root_obj, k, yyjson_get_bool(val));
                    } else if (yyjson_is_int(val)) {
                        yyjson_mut_obj_add_int(doc, root_obj, k, yyjson_get_int(val));
                    } else if (yyjson_is_real(val)) {
                        yyjson_mut_obj_add_real(doc, root_obj, k, yyjson_get_real(val));
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
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(nb_callers);
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
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
        free(qn);
        free(project);
        return cbm_mcp_text_result("no project loaded — run index_repository first", true);
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

    if (suffix_count == 1) {
        copy_node(&suffix_nodes[0], &node);
        cbm_store_free_nodes(suffix_nodes, suffix_count);
        char *result = build_snippet_response(srv, &node, "suffix", include_neighbors, NULL, 0);
        free_node_contents(&node);
        free(qn);
        free(project);
        return result;
    }

    if (suffix_count > 1) {
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
    int limit = cbm_mcp_get_int_arg(args, "limit", 10);
    int context_lines = cbm_mcp_get_int_arg(args, "context", 0);
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
        return cbm_mcp_text_result("pattern is required", true);
    }

    /* Resolve project */
    if (!project && srv->session_project[0]) {
        project = heap_strdup(srv->session_project);
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        free(pattern);
        free(project);
        free(file_pattern);
        return cbm_mcp_text_result("project not found or not indexed", true);
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
        return cbm_mcp_text_result("search failed: temp file", true);
    }
    // NOLINTNEXTLINE(clang-analyzer-security.insecureAPI.DeprecatedOrUnsafeBufferHandling)
    (void)fprintf(tf, "%s\n", pattern);
    (void)fclose(tf);

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
        const char *pn = project ? project : srv->session_project;
        char **indexed_files = NULL;
        int indexed_count = 0;
        if (pn &&
            cbm_store_list_files(pre_store, pn, &indexed_files, &indexed_count) == CBM_STORE_OK &&
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
        return cbm_mcp_text_result("search failed", true);
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
    const char *proj_name = project ? project : srv->session_project;

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
        if (store && proj_name) {
            cbm_store_find_nodes_by_file(store, proj_name, cur_file, &file_nodes, &file_node_count);
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
        return cbm_mcp_text_result("project not found", true);
    }

    if (!cbm_validate_shell_arg(root_path)) {
        free(root_path);
        free(project);
        free(base_branch);
        return cbm_mcp_text_result("project path contains invalid characters", true);
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
        return cbm_mcp_text_result("git diff failed", true);
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
                yyjson_mut_obj_add_str(doc, item, "name", nodes[i].name ? nodes[i].name : "");
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
        return cbm_mcp_text_result("project not found", true);
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

/* ── Tool dispatch ────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
char *cbm_mcp_handle_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    if (!tool_name) {
        return cbm_mcp_text_result("missing tool name", true);
    }

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

    char msg[256];
    snprintf(msg, sizeof(msg), "unknown tool: %s", tool_name);
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

    /* Derive project name from path */
    if (srv->session_root[0]) {
        /* Use last two path components joined by dash, matching Go's ProjectNameFromPath */
        const char *p = srv->session_root;
        const char *last_slash = strrchr(p, '/');
        if (last_slash && last_slash > p) {
            const char *prev = last_slash - 1;
            while (prev > p && *prev != '/') {
                prev--;
            }
            if (*prev == '/') {
                prev++;
            }
            snprintf(srv->session_project, sizeof(srv->session_project), "%.*s",
                     (int)(strlen(p) - (size_t)(prev - p)), prev);
            /* Replace / with - */
            for (char *c = srv->session_project; *c; c++) {
                if (*c == '/') {
                    *c = '-';
                }
            }
        } else {
            snprintf(srv->session_project, sizeof(srv->session_project), "%s",
                     last_slash ? last_slash + 1 : p);
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

    int rc = cbm_pipeline_run(p);
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
        char db_check[1024];
        snprintf(db_check, sizeof(db_check), "%s/.cache/codebase-memory-mcp/%s.db", home,
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
    } else if (strcmp(req.method, "tools/list") == 0) {
        result_json = cbm_mcp_tools_list();
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
