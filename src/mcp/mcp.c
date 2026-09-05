/*
 * mcp.c — MCP server: JSON-RPC 2.0 over stdio with graph tools.
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
    MCP_BFS_LIMIT = 100,            /* default per-direction trace budget (limit param raises) */
    MCP_BFS_LIMIT_MAX = 5000,       /* hard ceiling for the limit param (context-bomb guard) */
    MCP_DEFAULT_IMPACT_LIMIT = 200, /* detect_changes per-symbol rows; rollup stays complete */
    MCP_SNIPPET_MAX_LINES = 500,    /* get_code_snippet line cap (whole-file Module guard) */
    MCP_N_DEFAULTS_2 = 2,
    MCP_URI_PREFIX = 7,      /* strlen("file://") */
    MCP_CONTENT_PREFIX = 15, /* strlen("Content-Length:") */
    MCP_RETURN_2 = 2,
    MCP_TOOLS_PAGE_SIZE = 8,
    MCP_HELP_TOOLS_WRAP_COL = 74, /* --help tool list stays readable on 80-col terminals */
    MCP_MAX_CROSS_REPO_TARGETS = 4096,
    MCP_COMPARE_DEFAULT_LIMIT = 200,
    MCP_COMPARE_MAX_LIMIT = 1000,
    MCP_COMPARE_DEFAULT_SCAN_LIMIT = 2000000,
    MCP_COMPARE_MAX_SCAN_LIMIT = 10000000,
    MCP_COMPARE_SET_BYTE_BUDGET = 512 * 1024,
    MCP_QUERY_MAX_VISIBLE_ROWS = 99998,
    /* max_output_tokens is model-neutral sizing guidance, not a tokenizer
     * promise. The actual cross-platform contract is this deterministic UTF-8
     * byte ceiling, applied only at whole semantic-unit boundaries. */
    MCP_OUTPUT_BYTES_PER_TOKEN_ESTIMATE = 4,
};
#define MCP_MS_TO_US 1000LL
#define MCP_S_TO_US 1000000LL

#define SLEN(s) (sizeof(s) - 1)
#include "mcp/mcp.h"
#include "mcp/mcp_internal.h"
#include "store/store.h"
#include <sqlite3.h>
#include "cypher/cypher.h"
#include "discover/discover.h"
#include "pipeline/pipeline.h"
#include "pipeline/pass_cross_repo.h"
#include "git/git_context.h"
#include "cli/cli.h"
#include "watcher/watcher.h"
#include "foundation/mem.h"
#include "foundation/diagnostics.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/limits.h"
#include "foundation/subprocess.h"
#include "foundation/sha256.h"
#include "mcp/index_supervisor.h"
#include "mcp/compact_out.h"
#include "foundation/str_util.h"
#include "foundation/workspace.h"
#include "foundation/dump_verify.h"
#include "foundation/compat_regex.h"
#include "pipeline/artifact.h"

#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <direct.h>
#include <io.h>
#include <process.h>
#include <windows.h>
#define getpid _getpid
/* Write through the descriptor cbm_mkstemp returned rather than reopening its
 * path — see search_scratch_open. Mirrors config_toml_edit.c's toml_fdopen. */
#define mcp_fdopen _fdopen
#define mcp_close _close
#else
#include <fnmatch.h>
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>
#include <sys/wait.h>
#define mcp_fdopen fdopen
#define mcp_close close
#endif
#include <yyjson/yyjson.h>
#include <ctype.h>
#include <limits.h>
#include <stdarg.h> // va_list, for the bounded help-list appender
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <stdatomic.h>

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
#define JSONRPC_INVALID_PARAMS (-32602)
#define JSONRPC_INTERNAL_ERROR (-32603)

/* MCP stdio framing limits. The body limit is also the upper bound used by the
 * daemon IPC transport; headers stay deliberately small to prevent a peer from
 * growing getline buffers without bound through ignored extension headers. */
#define MCP_MAX_MESSAGE_SIZE ((size_t)10U * 1024U * 1024U)
#define MCP_MAX_HEADER_SIZE ((size_t)8U * 1024U)
#define MCP_SEARCH_OUTPUT_MAX ((size_t)64U * 1024U * 1024U)
#define MCP_SEARCH_SCAN_TIMEOUT_MS ((uint64_t)30000U)
#define MCP_FILE_OUTLINE_OUTPUT_MAX ((size_t)2U * 1024U * 1024U)

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

/* Replace non-UTF-8 strings inside a mutable document before standard JSON
 * serialization. Payload documents also escape literal reserved prefixes;
 * final JSON-RPC envelopes preserve already-normalized payload strings so
 * structuredContent does not acquire a second @utf8: layer. */
static bool yy_mut_normalize_output_text(yyjson_mut_doc *doc, yyjson_mut_val *value,
                                         bool escape_reserved) {
    if (yyjson_mut_is_str(value)) {
        char *encoded = NULL;
        size_t encoded_len = 0;
        if (!cbm_output_encode_text(yyjson_mut_get_str(value), yyjson_mut_get_len(value), &encoded,
                                    &encoded_len)) {
            return false;
        }
        if (!encoded) {
            return true;
        }
        if (!escape_reserved && strncmp(encoded, "@utf8:", 6) == 0) {
            free(encoded);
            return true;
        }
        yyjson_mut_val *owned = yyjson_mut_strncpy(doc, encoded, encoded_len);
        free(encoded);
        return owned && yyjson_mut_set_strn(value, yyjson_mut_get_str(owned), encoded_len);
    }
    if (yyjson_mut_is_arr(value)) {
        size_t index;
        size_t maximum;
        yyjson_mut_val *item;
        yyjson_mut_arr_foreach(value, index, maximum, item) {
            if (!yy_mut_normalize_output_text(doc, item, escape_reserved)) {
                return false;
            }
        }
    } else if (yyjson_mut_is_obj(value)) {
        size_t index;
        size_t maximum;
        yyjson_mut_val *key;
        yyjson_mut_val *item;
        yyjson_mut_obj_foreach(value, index, maximum, key, item) {
            if (!yy_mut_normalize_output_text(doc, key, escape_reserved) ||
                !yy_mut_normalize_output_text(doc, item, escape_reserved)) {
                return false;
            }
        }
    }
    return true;
}

static char *yy_doc_write_output(yyjson_mut_doc *doc, bool escape_reserved) {
    yyjson_mut_val *root = doc ? yyjson_mut_doc_get_root(doc) : NULL;
    if (!root || !yy_mut_normalize_output_text(doc, root, escape_reserved)) {
        return NULL;
    }
    return yyjson_mut_write(doc, 0, NULL);
}

/* Tool payloads are the first encoding boundary. */
static char *yy_doc_to_str(yyjson_mut_doc *doc) {
    return yy_doc_write_output(doc, true);
}

/* Payloads embedded in a JSON-RPC envelope have already crossed that boundary. */
static char *yy_final_doc_to_str(yyjson_mut_doc *doc) {
    return yy_doc_write_output(doc, false);
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

    char *out = yy_final_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_jsonrpc_format_error(int64_t id, int code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    yyjson_mut_obj_add_int(doc, root, "id", id);

    char *encoded_message = NULL;
    size_t encoded_message_len = 0;
    const char *raw_message = message ? message : "";
    if (!cbm_output_encode_text(raw_message, strlen(raw_message), &encoded_message,
                                &encoded_message_len)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }

    yyjson_mut_val *err = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, err, "code", code);
    yyjson_mut_obj_add_str(doc, err, "message", encoded_message ? encoded_message : raw_message);
    yyjson_mut_obj_add_val(doc, root, "error", err);

    char *out = yy_final_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(encoded_message);
    return out;
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP PROTOCOL HELPERS
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_mcp_text_result(const char *text, bool is_error) {
    yyjson_doc *structured_doc = text ? yyjson_read(text, strlen(text), 0) : NULL;
    yyjson_val *structured_root = structured_doc ? yyjson_doc_get_root(structured_doc) : NULL;
    bool structured_object = structured_root && yyjson_is_obj(structured_root);
    char *encoded_text = NULL;
    size_t encoded_text_len = 0;
    const char *raw_text = text ? text : "";
    if (!structured_object &&
        !cbm_output_encode_text(raw_text, strlen(raw_text), &encoded_text, &encoded_text_len)) {
        yyjson_doc_free(structured_doc);
        return NULL;
    }
    const char *wire_text = encoded_text ? encoded_text : raw_text;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *content = yyjson_mut_arr(doc);
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "type", "text");
    yyjson_mut_obj_add_str(doc, item, "text", wire_text);
    yyjson_mut_arr_add_val(content, item);
    yyjson_mut_obj_add_val(doc, root, "content", content);

    bool has_structured_content = false;
    if (structured_object) {
        yyjson_mut_val *structured = yyjson_val_mut_copy(doc, structured_root);
        if (structured) {
            yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
            has_structured_content = true;
        }
    }
    if (!has_structured_content && is_error) {
        /* structuredContent has now been wrong in both directions, so the rule
         * is spelled out here in full:
         *
         *   - JSON-object payload  -> structuredContent = the PARSED object
         *     (the branch above; the spec's structured+serialized pattern).
         *   - error                -> structuredContent = {"error": <text>} —
         *     bounded, small, and the only machine-readable failure form.
         *   - anything else       -> NO structuredContent key at all.
         *
         * Pre-#1488 the "anything else" case duplicated the payload verbatim
         * ({"text": <payload>} beside an identical content[0].text — 2.05x the
         * bytes on a 20k-node query_graph, #1375). #1488 replaced that with an
         * EMPTY object on the theory that it "still satisfies outputSchema" —
         * but clients that honor a declared outputSchema treat structuredContent
         * as THE authoritative result, so every tree-format reply rendered as
         * literally "{}" in Claude Code and friends (#1522). An empty object
         * beside a non-empty payload is not conservative; it is a wrong answer.
         *
         * The key is therefore OMITTED for text-shaped payloads, and no tool
         * declares an outputSchema anymore (see mcp_add_tool_def): output is
         * format-parameter-polymorphic, so a static schema was never truthful.
         * tests/test_mcp.c binds all three branches; scripts/smoke-test.sh
         * asserts the same contract on the shipped binary. */
        yyjson_mut_val *structured = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, structured, "error", wire_text);
        yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
    }
    yyjson_mut_obj_add_bool(doc, root, "isError", is_error);

    char *out = yy_final_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(structured_doc);
    free(encoded_text);
    return out;
}

bool cbm_mcp_cancel_request_matches(const char *params_json, int64_t active_id,
                                    const char *active_id_str) {
    if (!params_json) {
        return false;
    }

    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return false;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *request_id = yyjson_obj_get(root, "requestId");
    bool matches = false;
    if (request_id) {
        if (active_id_str) {
            matches =
                yyjson_is_str(request_id) && strcmp(yyjson_get_str(request_id), active_id_str) == 0;
        } else {
            matches = yyjson_is_int(request_id) && yyjson_get_int(request_id) == active_id;
        }
    }

    yyjson_doc_free(doc);
    return matches;
}

/* ── Tool definitions ─────────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *description;
    const char *input_schema; /* JSON string */
} tool_def_t;

static const tool_def_t TOOLS[] = {
    {"index_repository",
     "Index a repository. full/moderate add semantics; fast omits them; cross-repo-intelligence "
     "links services. Reports coverage gaps.",
     "{\"type\":\"object\",\"properties\":{\"repo_path\":{\"type\":\"string\",\"description\":"
     "\"Repository path\"},"
     "\"mode\":{\"type\":\"string\","
     "\"enum\":[\"full\",\"moderate\",\"fast\",\"cross-repo-intelligence\"],"
     "\"default\":\"full\",\"description\":\"full: all+semantic; moderate: "
     "filtered+semantic; fast: filtered only; cross-repo-intelligence: link services.\"},"
     "\"target_projects\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Cross-repo targets; [\\\"*\\\"] means all.\"},"
     "\"name\":{\"type\":\"string\",\"description\":"
     "\"Name override; Non-ASCII bytes are encoded; unsafe characters normalized.\"},"
     "\"persistence\":{\"type\":\"boolean\",\"default\":false,\"description\":"
     "\"Write .codebase-memory/graph.db.zst.\"}"
     "},\"required\":[\"repo_path\"]}"},

    {"search_graph",
     "Find symbols via BM25 query, regex name/qn filters, or semantic_query. Rows keep "
     "qn/file/lines and in/out over CALLS/USAGE/CALL_REFERENCE/INHERITS/IMPLEMENTS.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"query\":{\"type\":\"string\"},"
     "\"label\":{\"type\":\"string\"},\"name_pattern\":{\"type\":\"string\"},\"qn_pattern\":{"
     "\"type\":\"string\"},\"file_pattern\":{\"type\":\"string\"},"
     "\"relationship\":{\"type\":\"string\"},\"min_degree\":{\"type\":\"integer\"},"
     "\"max_degree\":{\"type\":\"integer\"},\"exclude_entry_points\":{\"type\":\"boolean\"},"
     "\"include_connected\":{\"type\":\"boolean\"},\"semantic_query\":{"
     "\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"Not with query.\"},"
     "\"semantic_limit\":{\"type\":\"integer\",\"default\":50,\"minimum\":0,"
     "\"maximum\":500},"
     "\"semantic_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0,"
     "\"maximum\":99998},"
     "\"limit\":{\"type\":"
     "\"integer\",\"default\":50,\"minimum\":1,\"maximum\":500},"
     "\"offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"max_output_tokens\":{\"type\":\"integer\",\"default\":3200,\"minimum\":128,"
     "\"maximum\":1000000},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"},"
     "\"fields\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},"
     "\"detail\":{\"type\":\"string\",\"enum\":[\"ids\",\"default\"],\"default\":\"default\"}},"
     "\"required\":[\"project\"]}"},

    {"query_graph",
     "Read-only Cypher for multi-hop, aggregation, complexity, or cross-service analysis. "
     "Default: 200 visible rows with "
     "exact/lower-bound totals and truncation; continue safely with next_cursor. "
     "graph=missed is a file tree of flagged coverage gaps; absence is not proof of completeness. "
     "Use get_graph_schema(diagnostics=full) for properties.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\"},"
     "\"graph\":{\"type\":\"string\",\"enum\":[\"code\",\"missed\"],\"default\":\"code\","
     "\"description\":\"code graph (default) or missed coverage-gap file tree.\"},"
     "\"max_rows\":{\"type\":\"integer\","
     "\"description\":"
     "\"Visible rows (default 200; max 99998); 0 uses the legacy maximum. Evaluation is "
     "unchanged.\",\"minimum\":0,\"default\":200},"
     "\"offset\":{\"type\":\"integer\",\"minimum\":0,\"default\":0,"
     "\"description\":\"Live compatibility paging; cannot be combined with cursor.\"},"
     "\"cursor\":{\"type\":\"string\",\"description\":\"Snapshot continuation; keep "
     "query/project/graph. Format, budget, and max_rows may change.\"},"
     "\"max_output_tokens\":{\"type\":\"integer\",\"minimum\":128,\"maximum\":1000000,"
     "\"description\":\"Sizing hint; hard ceiling is 4 UTF-8 bytes/token. Rows stay whole.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\","
     "\"description\":\"tree may use local @N+suffix prefix refs; json uses direct strings.\"}},"
     "\"required\":[\"query\",\"project\"]}"},

    {"trace_path",
     "Trace callers/callees, data flow, or cross-service paths. Defaults exclude tests and "
     "resolver evidence. Rows keep qn/hop with explicit totals, relations, and continuations.",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\"},\"project\":{"
     "\"type\":\"string\"},\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\","
     "\"both\"],\"default\":\"both\"},\"depth\":{\"type\":\"integer\",\"default\":3,"
     "\"minimum\":1,\"maximum\":15},"
     "\"limit\":{\"type\":\"integer\",\"default\":100,\"minimum\":1,\"maximum\":5000,"
     "\"description\":\"Rows/page; gte flags the 5000-node engine ceiling.\"},"
     "\"max_output_tokens\":{\"type\":\"integer\",\"default\":3200,\"minimum\":128,"
     "\"maximum\":1000000,\"description\":\"Sizing hint; hard ceiling is 4 UTF-8 bytes/token. "
     "Evidence/args yield before nearest graph rows.\"},"
     "\"cursor\":{\"type\":\"string\",\"description\":\"Pass next/next_cursor with traversal "
     "args unchanged; budget may increase.\"},"
     "\"mode\":{"
     "\"type\":\"string\",\"enum\":[\"calls\",\"data_flow\",\"cross_service\"],\"default\":"
     "\"calls\",\"description\":\"calls, argument-aware data_flow, or service edges.\"},"
     "\"parameter_name\":{\"type\":\"string\",\"description\":\"data_flow parameter filter.\"},"
     "\"edge_types\":{\"type\":\"array\",\"items\":{"
     "\"type\":\"string\"}},\"risk_labels\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Add hop-risk labels.\"},\"include_tests\":{\"type\":\"boolean\","
     "\"default\":false},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\","
     "\"description\":\"tree chooses smaller complete direct/grouped output; json uses stable "
     "grouped tables.\"},"
     "\"include_evidence\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Add resolver class and confidence.\"}},"
     "\"required\":[\"function_name\",\"project\"]}"},

    {"get_code_snippet",
     "Read a search_graph symbol. auto bounds source and outlines large containers; full "
     "restores up to 500 lines. Source/outline pages continue; coverage_note marks gaps.",
     "{\"type\":\"object\",\"properties\":{\"qualified_name\":{\"type\":\"string\",\"description\":"
     "\"search_graph qn, or short name.\"},\"project\":{"
     "\"type\":\"string\"},\"include_neighbors\":{"
     "\"type\":\"boolean\",\"default\":false},"
     "\"source_mode\":{\"type\":\"string\",\"enum\":[\"auto\",\"full\",\"outline\"],"
     "\"default\":\"auto\",\"description\":\"auto outlines 200+ line containers; full returns "
     "source; "
     "outline lists members.\"},"
     "\"member_limit\":{\"type\":\"integer\",\"default\":50,\"minimum\":1,\"maximum\":500},"
     "\"member_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"start_line\":{\"type\":\"integer\",\"minimum\":1},"
     "\"max_lines\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":500},"
     "\"max_output_tokens\":{\"type\":\"integer\",\"minimum\":128,"
     "\"maximum\":1000000,\"description\":\"auto/outline sizing hint (default 2500), hard "
     "ceiling 4 UTF-8 bytes/token. Lines stay whole; explicit full is uncapped unless set.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"required\":[\"qualified_name\",\"project\"]}"},

    {"get_file_outline",
     "Declaration outline of one exact repository-relative file: optional exact label filter, "
     "source order, exact total/offset/limit paging; file/folder/container nodes excluded.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"file_path\":{\"type\":\"string\",\"description\":\"Exact repository-relative "
     "file path\"},\"labels\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"maxItems\":16,\"description\":\"Optional exact node-label filter\"},"
     "\"limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":200,\"default\":100},"
     "\"offset\":{\"type\":\"integer\",\"minimum\":0,\"default\":0},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],"
     "\"default\":\"tree\"}},\"additionalProperties\":false,"
     "\"required\":[\"project\",\"file_path\"]}"},

    {"get_graph_schema",
     "Get node-label and edge-type counts. diagnostics=full also lists queryable properties.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"},"
     "\"diagnostics\":{\"type\":\"string\",\"enum\":[\"none\",\"full\"],\"default\":\"none\"},"
     "\"limit\":{\"type\":\"integer\",\"default\":50,\"minimum\":1,\"maximum\":500},"
     "\"offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0}},\"required\":["
     "\"project\"]}"},

    {"compare_graphs",
     "Compare two indexed snapshots: deterministic target-only additions and base-only "
     "removals of stable node/edge identities; each set capped by limit and a 512 KiB budget "
     "with exact totals and truncation reasons.",
     "{\"type\":\"object\",\"properties\":{"
     "\"base_project\":{\"type\":\"string\",\"minLength\":1},"
     "\"target_project\":{\"type\":\"string\",\"minLength\":1},"
     "\"limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":1000,\"default\":200},"
     "\"scan_limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":10000000,"
     "\"default\":2000000}},\"required\":[\"base_project\",\"target_project\"],"
     "\"additionalProperties\":false}"},

    {"get_architecture",
     "Compact counts, languages, packages, entry points. Request structure, dependencies, "
     "routes, hotspots, boundaries, layers, clusters, cycles, or file_tree; path scopes a "
     "directory.",
     /* The aspects enum mirrors VALID_ASPECTS (see aspect_is_valid) — update both together. */
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"path\":{\"type\":"
     "\"string\",\"description\":\"Directory prefix (for example apps/hoa).\"},"
     "\"aspects\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"enum\":[\"all\","
     "\"overview\",\"structure\",\"dependencies\",\"routes\",\"languages\",\"packages\","
     "\"entry_points\",\"hotspots\",\"boundaries\",\"layers\",\"file_tree\",\"clusters\","
     "\"cycles\"]},"
     "\"description\":\"all=everything; overview=compact except file_tree; omitted=languages/"
     "packages/entry_points; cycles is opt-in.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"required\":[\"project\"]}"},

    {"search_code",
     "Graph-ranked text search: compact symbols, full bounded source, or file paths.",
     "{\"type\":\"object\",\"properties\":{\"pattern\":{\"type\":\"string\"},\"project\":{\"type\":"
     "\"string\"},\"file_pattern\":{\"type\":\"string\"},\"path_filter\":{\"type\":\"string\"},"
     "\"mode\":{\"type\":\"string\","
     "\"enum\":[\"compact\",\"full\",\"files\"],\"default\":\"compact\"},"
     "\"context\":{\"type\":\"integer\"},"
     "\"regex\":{\"type\":\"boolean\",\"default\":false},"
     "\"debug\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Add scope_ms/scan_ms/enrich_ms phase timings.\"},"
     "\"limit\":{\"type\":\"integer\","
     "\"default\":10,\"minimum\":1,\"maximum\":500},"
     "\"result_limit\":{\"type\":\"integer\",\"default\":10,\"minimum\":1,\"maximum\":500},"
     "\"result_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"raw_limit\":{\"type\":\"integer\",\"default\":5,\"minimum\":0,\"maximum\":100},"
     "\"raw_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"raw_content_offset\":{\"type\":\"integer\",\"minimum\":0,"
     "\"description\":\"Raw-line byte offset; omit for match-centered preview.\"},"
     "\"directory_limit\":{\"type\":\"integer\",\"default\":20,\"minimum\":0,"
     "\"maximum\":64},"
     "\"directory_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"match_limit\":{\"type\":\"integer\",\"default\":8,\"minimum\":1,"
     "\"maximum\":500},"
     "\"source_max_lines\":{\"type\":\"integer\",\"default\":20,\"minimum\":1,"
     "\"maximum\":200},"
     "\"max_output_tokens\":{\"type\":\"integer\",\"minimum\":128,\"maximum\":1000000},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"required\":[\"pattern\",\"project\"]}"},

    {"list_projects", "List projects with stable paging. Identity is lean; stats adds graph sizes.",
     "{\"type\":\"object\",\"properties\":{"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"},"
     "\"detail\":{\"type\":\"string\",\"enum\":[\"identity\",\"stats\"],"
     "\"default\":\"identity\",\"description\":\"stats adds node/edge/database-size counts.\"},"
     "\"include_details\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Alias for detail=stats: include branch, node/edge counts and database "
     "size. Slower.\"},"
     "\"limit\":{\"type\":\"integer\",\"default\":50,\"minimum\":1,\"maximum\":500},"
     "\"offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"metadata_only\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Compatibility: omit counts, size, and branch.\"}}}"},
    {"delete_project", "Delete a project from the index",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"index_status",
     "Project readiness, counts, root, and coverage gaps. diagnostics adds coverage rows; verbose "
     "adds Git paths. Best-effort only; verify cited paths with check_index_coverage.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"verbose\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Add worktree/"
     "shadow Git paths for index-location debugging.\"},"
     "\"diagnostics\":{\"type\":\"string\",\"enum\":[\"none\",\"summary\",\"full\"],"
     "\"default\":\"none\",\"description\":\"Coverage rows: counts, five samples, or up to "
     "500.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"required\":["
     "\"project\"]}"},

    {"check_index_coverage",
     "Best-effort exact-path/scope coverage and freshness, paged separately. full diagnostics "
     "adds raw detail. Clean is not proof of completeness.",
     "{\"type\":\"object\",\"properties\":{"
     "\"project\":{\"type\":\"string\"},"
     "\"paths\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"maxItems\":128},"
     "\"path_limit\":{\"type\":\"integer\",\"default\":20,\"minimum\":1,\"maximum\":128},"
     "\"path_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"scopes\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"maxItems\":32},"
     "\"scope_limit\":{\"type\":\"integer\",\"default\":20,\"minimum\":1,\"maximum\":1000},"
     "\"scope_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"diagnostics\":{\"type\":\"string\",\"enum\":[\"none\",\"full\"],\"default\":\"none\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"required\":[\"project\"]"
     "}"},

    {"detect_changes", "Map a Git diff to files and impact. Page with snapshot cursors.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"scope\":{\"type\":"
     "\"string\",\"enum\":[\"files\",\"impact\"],\"default\":\"impact\"},"
     "\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\",\"both\"],\"default\":"
     "\"inbound\",\"description\":\"inbound=callers; outbound=dependencies; both=union.\"},"
     "\"depth\":{\"type\":\"integer\",\"default\":2},"
     "\"limit\":{\"type\":\"integer\",\"default\":200,\"maximum\":5000},"
     "\"impact_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"impact_cursor\":{\"type\":\"string\"},"
     "\"changed_limit\":{\"type\":\"integer\",\"default\":20,\"minimum\":0,\"maximum\":5000},"
     "\"changed_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"changed_cursor\":{\"type\":\"string\"},"
     "\"module_limit\":{\"type\":\"integer\",\"default\":20,\"minimum\":0,\"maximum\":256},"
     "\"module_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"module_cursor\":{\"type\":\"string\"},"
     "\"max_output_tokens\":{\"type\":\"integer\",\"default\":3200,\"minimum\":128,"
     "\"maximum\":1000000,\"description\":\"Sizing hint; hard ceiling is 4 UTF-8 bytes/token.\"},"
     "\"base_branch\":{\"type\":"
     "\"string\",\"default\":\"main\"},\"since\":{\"type\":\"string\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"required\":"
     "[\"project\"]}"},

    {"manage_adr", "Outline an ADR by default; get reads it; update replaces it",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"mode\":{\"type\":"
     "\"string\",\"enum\":[\"outline\",\"get\",\"update\",\"set_sections\",\"sections\"],"
     "\"default\":\"outline\",\"description\":\"outline pages headings; get reads; update replaces "
     "the whole document; set_sections rewrites only the named sections and leaves every other "
     "byte untouched (an identical repeated write is byte-identical, so retrying is safe); "
     "sections is legacy.\"},"
     "\"content\":{\"type\":\"string\",\"description\":\"Whole document for update\"},"
     "\"section_updates\":{\"type\":\"object\",\"description\":\"set_sections: section name -> "
     "new body. Any heading name works; names match exactly, including case.\","
     "\"additionalProperties\":{\"type\":\"string\"}},"
     "\"section_limit\":{\"type\":\"integer\",\"default\":50,\"minimum\":1,\"maximum\":500},"
     "\"section_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"additionalProperties\":false,"
     "\"required\":[\"project\"]}"},

    {"ingest_traces", "Validate and count traces; graph edge creation is not implemented",
     "{\"type\":\"object\",\"properties\":{\"traces\":{\"type\":\"array\",\"items\":{\"type\":"
     "\"object\",\"properties\":{\"caller\":{\"type\":\"string\"},\"callee\":{\"type\":\"string\"},"
     "\"count\":{\"type\":\"integer\"}},\"additionalProperties\":false}},\"project\":{\"type\":"
     "\"string\"}},\"required\":[\"traces\",\"project\"]}"},
};

static const int TOOL_COUNT = sizeof(TOOLS) / sizeof(TOOLS[0]);

typedef struct {
    const char *name;
    bool read_only;
    bool destructive;
    bool idempotent;
    bool open_world;
} tool_annotation_def_t;

/* Tool annotations are deliberately explicit. All tools operate on the local
 * repository/index domain, so none cross an open-world trust boundary.
 *
 * The ten pure query tools resolve their store through resolve_store(), whose
 * query-only path is strictly non-mutating: a corrupt database is reported and
 * left in place, never quarantined or rebuilt — quarantine/rebuild is reserved
 * for write-side opens (index_repository, manage_adr writes). That is what
 * makes readOnlyHint=true honest for them and lets plan-mode clients expose
 * them (the "read-only analysis tools" surface described in #1100). */
static const tool_annotation_def_t TOOL_ANNOTATIONS[] = {
    {"index_repository", false, false, true, false},
    {"search_graph", true, false, true, false},
    {"query_graph", true, false, true, false},
    {"trace_path", true, false, true, false},
    {"get_code_snippet", true, false, true, false},
    {"get_file_outline", false, true, true, false},
    {"get_graph_schema", true, false, true, false},
    {"compare_graphs", true, false, true, false},
    {"get_architecture", true, false, true, false},
    {"search_code", true, false, true, false},
    {"list_projects", true, false, true, false},
    {"delete_project", false, true, true, false},
    {"index_status", true, false, true, false},
    {"check_index_coverage", true, false, true, false},
    {"detect_changes", true, false, true, false},
    {"manage_adr", false, true, false, false},
    {"ingest_traces", false, false, false, false},
};

static const tool_annotation_def_t *mcp_tool_annotations(const char *name) {
    size_t count = sizeof(TOOL_ANNOTATIONS) / sizeof(TOOL_ANNOTATIONS[0]);
    for (size_t i = 0; i < count; i++) {
        if (strcmp(TOOL_ANNOTATIONS[i].name, name) == 0) {
            return &TOOL_ANNOTATIONS[i];
        }
    }
    return NULL;
}

static void mcp_add_json_schema(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                const char *schema_json) {
    yyjson_doc *schema_doc = yyjson_read(schema_json, strlen(schema_json), 0);
    if (schema_doc) {
        yyjson_mut_val *schema = yyjson_val_mut_copy(doc, yyjson_doc_get_root(schema_doc));
        if (schema) {
            yyjson_mut_obj_add_val(doc, obj, key, schema);
        }
        yyjson_doc_free(schema_doc);
    }
}

static void mcp_add_tool_def(yyjson_mut_doc *doc, yyjson_mut_val *tools, int i) {
    yyjson_mut_val *tool = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, tool, "name", TOOLS[i].name);
    /* MCP title is optional and clients fall back to name. These titles only
     * repeated the snake_case name with spaces, so omit their recurring
     * discovery cost and retain the behavioral description instead. */
    yyjson_mut_obj_add_str(doc, tool, "description", TOOLS[i].description);

    mcp_add_json_schema(doc, tool, "inputSchema", TOOLS[i].input_schema);
    /* Deliberately NO outputSchema. Tool output is format-parameter-polymorphic
     * (tree text by default, a JSON object under format:"json"), so no static
     * schema is truthful — and a declared schema makes spec-honoring clients
     * read structuredContent as the authoritative result, which is exactly how
     * the empty-object regression rendered every tree reply as "{}" (#1522).
     * The blanket {"type":"object","additionalProperties":true} it replaced
     * validated anything and informed nobody. */

    const tool_annotation_def_t *def = mcp_tool_annotations(TOOLS[i].name);
    yyjson_mut_val *annotations = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, annotations, "readOnlyHint", def ? def->read_only : false);
    yyjson_mut_obj_add_bool(doc, annotations, "destructiveHint", def ? def->destructive : true);
    yyjson_mut_obj_add_bool(doc, annotations, "idempotentHint", def ? def->idempotent : false);
    yyjson_mut_obj_add_bool(doc, annotations, "openWorldHint", def ? def->open_world : true);
    yyjson_mut_obj_add_val(doc, tool, "annotations", annotations);

    yyjson_mut_arr_add_val(tools, tool);
}

static bool mcp_tool_allowed(cbm_mcp_tool_profile_t profile, const char *name) {
    static const char *const analysis_tools[] = {
        "search_graph",     "query_graph",      "trace_path",     "get_code_snippet",
        "get_file_outline", "get_graph_schema", "compare_graphs", "get_architecture",
        "search_code",      "list_projects",    "index_status",   "check_index_coverage",
        "detect_changes",
    };
    static const char *const scout_tools[] = {
        "search_graph",     "trace_path",    "get_code_snippet", "get_file_outline",
        "get_architecture", "list_projects", "index_status",     "check_index_coverage",
    };
    if (!name) {
        return false;
    }
    if (profile == CBM_MCP_TOOL_PROFILE_ALL) {
        return true;
    }
    const char *const *allowed = NULL;
    size_t allowed_count = 0U;
    if (profile == CBM_MCP_TOOL_PROFILE_ANALYSIS) {
        allowed = analysis_tools;
        allowed_count = sizeof(analysis_tools) / sizeof(analysis_tools[0]);
    } else if (profile == CBM_MCP_TOOL_PROFILE_SCOUT) {
        allowed = scout_tools;
        allowed_count = sizeof(scout_tools) / sizeof(scout_tools[0]);
    }
    for (size_t i = 0U; i < allowed_count; i++) {
        if (strcmp(name, allowed[i]) == 0) {
            return true;
        }
    }
    return false;
}

static const char *mcp_tool_profile_name(cbm_mcp_tool_profile_t profile) {
    return profile == CBM_MCP_TOOL_PROFILE_SCOUT ? "scout" : "analysis";
}

int cbm_mcp_parse_tool_profile_args(int argc, const char *const argv[const],
                                    cbm_mcp_tool_profile_t *profile_out) {
    if (argc < 0 || !argv || !profile_out) {
        return -1;
    }
    *profile_out = CBM_MCP_TOOL_PROFILE_ALL;
    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        if (!arg) {
            return -1;
        }
        if (strcmp(arg, "--tool-profile=analysis") == 0) {
            *profile_out = CBM_MCP_TOOL_PROFILE_ANALYSIS;
            continue;
        }
        if (strcmp(arg, "--tool-profile=scout") == 0) {
            *profile_out = CBM_MCP_TOOL_PROFILE_SCOUT;
            continue;
        }
        if (strcmp(arg, "--tool-profile") == 0) {
            if (i + 1 >= argc || !argv[i + 1]) {
                return -1;
            }
            if (strcmp(argv[i + 1], "analysis") == 0) {
                *profile_out = CBM_MCP_TOOL_PROFILE_ANALYSIS;
            } else if (strcmp(argv[i + 1], "scout") == 0) {
                *profile_out = CBM_MCP_TOOL_PROFILE_SCOUT;
            } else {
                return -1;
            }
            i++;
            continue;
        }
        if (strncmp(arg, "--tool-profile=", strlen("--tool-profile=")) == 0) {
            return -1;
        }
    }
    return 0;
}

bool cbm_mcp_tool_profile_allows_http(cbm_mcp_tool_profile_t profile) {
    return profile == CBM_MCP_TOOL_PROFILE_ALL;
}

static int mcp_allowed_tool_count(cbm_mcp_tool_profile_t profile) {
    int count = 0;
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (mcp_tool_allowed(profile, TOOLS[i].name)) {
            count++;
        }
    }
    return count;
}

static char *cbm_mcp_tools_list_range(cbm_mcp_tool_profile_t profile, int offset, int limit,
                                      bool include_next_cursor) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *tools = yyjson_mut_arr(doc);

    if (offset < 0) {
        offset = 0;
    }
    int allowed_count = mcp_allowed_tool_count(profile);
    if (offset > allowed_count) {
        offset = allowed_count;
    }
    if (limit < 0 || limit > allowed_count) {
        limit = allowed_count;
    }

    int end = offset + limit;
    if (end > allowed_count) {
        end = allowed_count;
    }

    int visible = 0;
    for (int i = 0; i < TOOL_COUNT && visible < end; i++) {
        if (!mcp_tool_allowed(profile, TOOLS[i].name)) {
            continue;
        }
        if (visible >= offset) {
            mcp_add_tool_def(doc, tools, i);
        }
        visible++;
    }

    yyjson_mut_obj_add_val(doc, root, "tools", tools);
    if (include_next_cursor && end < allowed_count) {
        char cursor[32];
        snprintf(cursor, sizeof(cursor), "%d", end);
        yyjson_mut_obj_add_strcpy(doc, root, "nextCursor", cursor);
    }

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_mcp_tools_list(void) {
    return cbm_mcp_tools_list_range(CBM_MCP_TOOL_PROFILE_ALL, 0, TOOL_COUNT, false);
}

/* Return the JSON input_schema string for a tool by name, or NULL if unknown.
 * Used by the CLI to build --flag arguments and per-tool --help from the same
 * source of truth the MCP tools/list advertises. Static lifetime; do not free. */
const char *cbm_mcp_tool_input_schema(const char *tool_name) {
    if (!tool_name) {
        return NULL;
    }
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (strcmp(TOOLS[i].name, tool_name) == 0) {
            return TOOLS[i].input_schema;
        }
    }
    return NULL;
}

int cbm_mcp_tool_count(void) {
    return TOOL_COUNT;
}

const char *cbm_mcp_tool_name(int index) {
    if (index < 0 || index >= TOOL_COUNT) {
        return NULL;
    }
    return TOOLS[index].name;
}

const char *cbm_mcp_tool_description(const char *tool_name) {
    if (!tool_name) {
        return NULL;
    }
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (strcmp(TOOLS[i].name, tool_name) == 0) {
            return TOOLS[i].description;
        }
    }
    return NULL;
}

/* Render the top-level --help "Tools:" block from the registry tools/list
 * serves. The list used to be hand-maintained in the help text and drifted
 * when check_index_coverage was added (#1361); deriving it here makes that
 * divergence impossible. Heap-allocated; caller frees. */
/* Append at out[len] and return the bytes ACTUALLY written.
 *
 * snprintf returns the length it WOULD have written, so accumulating that value
 * lets len run past cap; the next `cap - len` then underflows to a huge size_t
 * and the following write lands outside the buffer. CodeQL flagged exactly that
 * shape here, and it is the same class #1173 just fixed in the Cypher list
 * builder. The capacity computed below does happen to be sufficient today —
 * which makes this the more dangerous version, not the safer one: the code is
 * correct only by an argument made ten lines away, so renaming a tool or
 * changing the wrap rule would turn it into an overflow with nothing to notice.
 * Clamping makes `len <= cap - 1` a local invariant no later edit can void. */
static size_t help_append(char *out, size_t cap, size_t len, const char *fmt, ...) {
    if (len + 1 >= cap) {
        return 0;
    }
    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(out + len, cap - len, fmt, args);
    va_end(args);
    if (written <= 0) {
        return 0;
    }
    size_t room = cap - len - 1;
    return (size_t)written > room ? room : (size_t)written;
}

char *cbm_mcp_tools_help_list(void) {
    size_t cap = SLEN("Tools:") + 2; /* trailing newline + NUL */
    for (int i = 0; i < TOOL_COUNT; i++) {
        cap += strlen(TOOLS[i].name) + SLEN(" ,\n "); /* per-tool worst case incl. a wrap */
    }
    char *out = malloc(cap);
    if (!out) {
        return NULL;
    }
    size_t len = help_append(out, cap, 0, "Tools:");
    size_t col = len;
    for (int i = 0; i < TOOL_COUNT; i++) {
        const char *sep = (i + 1 < TOOL_COUNT) ? "," : "";
        size_t item = SLEN(" ") + strlen(TOOLS[i].name) + strlen(sep);
        if (i > 0 && col + item > MCP_HELP_TOOLS_WRAP_COL) {
            len += help_append(out, cap, len, "\n ");
            col = 1;
        }
        size_t wrote = help_append(out, cap, len, " %s%s", TOOLS[i].name, sep);
        len += wrote;
        col += wrote;
    }
    len += help_append(out, cap, len, "\n");
    (void)len; /* final length is not needed; the buffer is NUL-terminated */
    return out;
}

static int mcp_tools_cursor_offset(const char *params_json, bool *has_cursor_out) {
    if (has_cursor_out) {
        *has_cursor_out = false;
    }
    if (!params_json) {
        return 0;
    }

    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return 0;
    }

    int offset = 0;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *cursor = root ? yyjson_obj_get(root, "cursor") : NULL;
    if (cursor) {
        if (has_cursor_out) {
            *has_cursor_out = true;
        }
        offset = TOOL_COUNT;
        if (yyjson_is_str(cursor)) {
            const char *cursor_str = yyjson_get_str(cursor);
            if (cursor_str && *cursor_str != '\0') {
                char *endptr = NULL;
                errno = 0;
                long parsed = strtol(cursor_str, &endptr, 10);
                if (endptr && *endptr == '\0' && errno == 0 && parsed >= 0) {
                    offset = parsed > TOOL_COUNT ? TOOL_COUNT : (int)parsed;
                }
            }
        }
    }

    yyjson_doc_free(doc);
    return offset;
}

static char *cbm_mcp_tools_list_page(cbm_mcp_tool_profile_t profile, const char *params_json) {
    bool has_cursor = false;
    int offset = mcp_tools_cursor_offset(params_json, &has_cursor);
    if (!has_cursor) {
        return cbm_mcp_tools_list_range(profile, 0, TOOL_COUNT, false);
    }
    return cbm_mcp_tools_list_range(profile, offset, MCP_TOOLS_PAGE_SIZE, true);
}

/* ── Prompt definitions ───────────────────────────────────────── */

static void mcp_add_prompt_argument(yyjson_mut_doc *doc, yyjson_mut_val *arguments,
                                    const char *name, const char *title, const char *description,
                                    bool required) {
    yyjson_mut_val *argument = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, argument, "name", name);
    yyjson_mut_obj_add_str(doc, argument, "title", title);
    yyjson_mut_obj_add_str(doc, argument, "description", description);
    yyjson_mut_obj_add_bool(doc, argument, "required", required);
    yyjson_mut_arr_add_val(arguments, argument);
}

static char *cbm_mcp_prompts_list(void) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *prompts = yyjson_mut_arr(doc);

    yyjson_mut_val *explore = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, explore, "name", "explore_codebase");
    yyjson_mut_obj_add_str(doc, explore, "title", "Explore codebase");
    yyjson_mut_obj_add_str(doc, explore, "description",
                           "Explore a codebase with graph-first structural discovery.");
    yyjson_mut_val *explore_arguments = yyjson_mut_arr(doc);
    mcp_add_prompt_argument(doc, explore_arguments, "project", "Project",
                            "Indexed project name from list_projects.", true);
    mcp_add_prompt_argument(doc, explore_arguments, "question", "Question",
                            "Architecture or implementation question to investigate.", true);
    yyjson_mut_obj_add_val(doc, explore, "arguments", explore_arguments);
    yyjson_mut_arr_add_val(prompts, explore);

    yyjson_mut_val *review = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, review, "name", "review_change_impact");
    yyjson_mut_obj_add_str(doc, review, "title", "Review change impact");
    yyjson_mut_obj_add_str(doc, review, "description",
                           "Review affected callers, tests, boundaries, and risks.");
    yyjson_mut_val *review_arguments = yyjson_mut_arr(doc);
    mcp_add_prompt_argument(doc, review_arguments, "project", "Project",
                            "Indexed project name from list_projects.", true);
    mcp_add_prompt_argument(doc, review_arguments, "change", "Change",
                            "Change, symbol, or area whose impact should be reviewed.", true);
    mcp_add_prompt_argument(doc, review_arguments, "base_branch", "Base branch",
                            "Git branch or ref for detect_changes; defaults to main.", false);
    yyjson_mut_obj_add_val(doc, review, "arguments", review_arguments);
    yyjson_mut_arr_add_val(prompts, review);

    yyjson_mut_obj_add_val(doc, root, "prompts", prompts);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static const char *mcp_prompt_string_argument(yyjson_val *arguments, const char *name) {
    if (!arguments || !yyjson_is_obj(arguments)) {
        return NULL;
    }
    yyjson_val *value = yyjson_obj_get(arguments, name);
    if (!value || !yyjson_is_str(value)) {
        return NULL;
    }
    const char *text = yyjson_get_str(value);
    return text && text[0] ? text : NULL;
}

static char *mcp_prompt_result(const char *description, const char *text) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "description", description);

    yyjson_mut_val *messages = yyjson_mut_arr(doc);
    yyjson_mut_val *message = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, message, "role", "user");
    yyjson_mut_val *content = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, content, "type", "text");
    yyjson_mut_obj_add_str(doc, content, "text", text);
    yyjson_mut_obj_add_val(doc, message, "content", content);
    yyjson_mut_arr_add_val(messages, message);
    yyjson_mut_obj_add_val(doc, root, "messages", messages);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static char *mcp_prompt_error_json(int code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "code", code);
    yyjson_mut_obj_add_str(doc, root, "message", message);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static char *cbm_mcp_prompt_get(const char *params_json, char **error_json) {
    *error_json = NULL;
    yyjson_doc *params_doc = params_json ? yyjson_read(params_json, strlen(params_json), 0) : NULL;
    yyjson_val *params = params_doc ? yyjson_doc_get_root(params_doc) : NULL;
    yyjson_val *name_value =
        params && yyjson_is_obj(params) ? yyjson_obj_get(params, "name") : NULL;
    if (!name_value || !yyjson_is_str(name_value)) {
        *error_json = mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Invalid prompt name");
        if (params_doc) {
            yyjson_doc_free(params_doc);
        }
        return NULL;
    }

    const char *name = yyjson_get_str(name_value);
    bool is_explore = strcmp(name, "explore_codebase") == 0;
    bool is_review = strcmp(name, "review_change_impact") == 0;
    if (!is_explore && !is_review) {
        *error_json = mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Invalid prompt name");
        yyjson_doc_free(params_doc);
        return NULL;
    }

    yyjson_val *arguments = yyjson_obj_get(params, "arguments");
    const char *project = mcp_prompt_string_argument(arguments, "project");
    const char *request = mcp_prompt_string_argument(arguments, is_explore ? "question" : "change");
    if (!project || !request) {
        *error_json =
            mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Missing required prompt arguments");
        yyjson_doc_free(params_doc);
        return NULL;
    }

    const char *base_branch = "main";
    yyjson_val *base_branch_value = is_review && arguments && yyjson_is_obj(arguments)
                                        ? yyjson_obj_get(arguments, "base_branch")
                                        : NULL;
    if (base_branch_value) {
        if (!yyjson_is_str(base_branch_value) || !yyjson_get_str(base_branch_value)[0]) {
            *error_json = mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Invalid prompt arguments");
            yyjson_doc_free(params_doc);
            return NULL;
        }
        base_branch = yyjson_get_str(base_branch_value);
    }

    static const char EXPLORE_TEMPLATE[] =
        "Explore project \"%s\" to answer: %s\n\n"
        "Use graph tools first: search_graph to find relevant symbols, get_code_snippet for "
        "exact source, and trace_path(direction=\"both\") for callers and callees. Use "
        "get_architecture for broad orientation and query_graph only for multi-hop patterns. "
        "Check has_more and paginate. Fall back to search_code or grep only for literal or "
        "non-code text, or where graph coverage is incomplete.";
    static const char REVIEW_TEMPLATE[] =
        "Review change impact in project \"%s\" for: %s\n\n"
        "Use detect_changes with base_branch \"%s\", then trace_path(direction=\"both\", "
        "include_tests=true) for affected callers, callees, and tests. Read exact definitions "
        "with get_code_snippet and use query_graph for cross-boundary patterns. Report affected "
        "callers, tests, boundaries, and risks; do not modify files.";

    size_t text_size = strlen(project) + strlen(request) + strlen(base_branch) +
                       (is_explore ? sizeof(EXPLORE_TEMPLATE) : sizeof(REVIEW_TEMPLATE));
    char *text = malloc(text_size);
    if (!text) {
        *error_json = mcp_prompt_error_json(JSONRPC_INTERNAL_ERROR, "Internal error");
        yyjson_doc_free(params_doc);
        return NULL;
    }
    if (is_explore) {
        snprintf(text, text_size, EXPLORE_TEMPLATE, project, request);
    } else {
        snprintf(text, text_size, REVIEW_TEMPLATE, project, request, base_branch);
    }

    char *result = mcp_prompt_result(
        is_explore ? "Graph-first codebase exploration" : "Graph-first change-impact review", text);
    free(text);
    yyjson_doc_free(params_doc);
    return result;
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

static const char MCP_SERVER_INSTRUCTIONS[] =
    "Graph first: search_graph for symbols, trace_path for relationships, get_code_snippet for "
    "source, query_graph for multi-hop, and get_architecture for overview. Use search_code/grep "
    "for literals or coverage gaps. Indexes auto-refresh. Check cited-path coverage; paginate.";

static const char MCP_ANALYSIS_SERVER_INSTRUCTIONS[] =
    "analysis tool profile: read-only graph work via search_graph, trace_path, "
    "get_code_snippet, query_graph, get_architecture, or search_code. Use check_index_coverage "
    "for cited paths; paginate.";

static const char MCP_SCOUT_SERVER_INSTRUCTIONS[] =
    "scout tool profile: fast positive discovery via search_graph, trace_path, "
    "get_code_snippet, or get_architecture with narrow limits. Use check_index_coverage for "
    "cited paths. Findings are provisional.";

static char *cbm_mcp_initialize_response_for_profile(const char *params_json,
                                                     cbm_mcp_tool_profile_t profile) {
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
    yyjson_mut_obj_add_str(doc, impl, "version", cbm_cli_get_version());
    yyjson_mut_obj_add_val(doc, root, "serverInfo", impl);

    yyjson_mut_val *caps = yyjson_mut_obj(doc);
    yyjson_mut_val *tools_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, tools_cap, "listChanged", false);
    yyjson_mut_obj_add_val(doc, caps, "tools", tools_cap);
    yyjson_mut_val *prompts_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, prompts_cap, "listChanged", false);
    yyjson_mut_obj_add_val(doc, caps, "prompts", prompts_cap);
    yyjson_mut_obj_add_val(doc, root, "capabilities", caps);
    const char *instructions = MCP_SERVER_INSTRUCTIONS;
    if (profile == CBM_MCP_TOOL_PROFILE_ANALYSIS) {
        instructions = MCP_ANALYSIS_SERVER_INSTRUCTIONS;
    } else if (profile == CBM_MCP_TOOL_PROFILE_SCOUT) {
        instructions = MCP_SCOUT_SERVER_INSTRUCTIONS;
    }
    yyjson_mut_obj_add_str(doc, root, "instructions", instructions);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_mcp_initialize_response(const char *params_json) {
    return cbm_mcp_initialize_response_for_profile(params_json, CBM_MCP_TOOL_PROFILE_ALL);
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

static char *canonicalize_repo_path_if_exists(char *repo_path) {
    if (!repo_path) {
        return NULL;
    }
    bool root_syntax = true;
    for (const char *p = repo_path; *p; p++) {
        if (*p != '/' && *p != '\\' && *p != ':') {
            root_syntax = false;
            break;
        }
    }
    if (root_syntax) {
        return repo_path;
    }

    char real[CBM_SZ_4K];
    /* Wide-path canonicalization: the old _access/_fullpath pair decoded the
     * UTF-8 repo_path through the ANSI codepage and corrupted CJK paths on
     * CJK-locale systems (#973). */
    if (cbm_canonical_path(repo_path, real, sizeof(real))) {
        cbm_normalize_path_sep(real);
        char *canonical = heap_strdup(real);
        if (canonical) {
            free(repo_path);
            return canonical;
        }
    }

    return repo_path;
}

static bool repo_path_is_absolute(const char *path) {
    if (!path || path[0] == '\0') {
        return false;
    }
#ifdef _WIN32
    /* Path separators are normalized before this helper is called. A drive-
     * relative path such as "C:repo" is deliberately not considered absolute. */
    return (path[0] == '/' && path[1] == '/') ||
           (isalpha((unsigned char)path[0]) && path[1] == ':' && path[2] == '/');
#else
    return path[0] == '/';
#endif
}

static char *normalize_project_arg(char *project) {
    if (!project || (!strchr(project, '/') && !strchr(project, '\\'))) {
        return project;
    }

    project = canonicalize_repo_path_if_exists(project);
    char *normalized = cbm_project_name_from_path(project);
    if (normalized) {
        free(project);
        return normalized;
    }
    return project;
}

/* Forward decls — defined below alongside store resolution. */
static const char *cache_dir(char *buf, size_t bufsz);
static bool is_project_db_file(const char *name, size_t len);
bool cbm_validate_project_name(const char *project);

/* #1025: agents naturally pass the repo FOLDER name ("codebase-memory-mcp"),
 * but indexed project names derive from the full path
 * (E:\project\graph\x -> "E-project-graph-x"), so the exact lookup fails
 * while list_projects clearly shows the project. When no <project>.db exists,
 * scan cache-dir FILENAMES for a segment-aligned tail match ("-<project>.db"):
 * exactly one match adopts the full name; zero or several keep the original so
 * the existing not-found error (which lists all candidates) fires. Filename-
 * level only — internal-name drift stays #704's fallback in resolve_store. */
static char *resolve_project_tail(char *project) {
    if (!project || !cbm_validate_project_name(project)) {
        return project;
    }
    char dir[CBM_SZ_1K];
    cache_dir(dir, sizeof(dir));
    char exact[CBM_SZ_2K];
    snprintf(exact, sizeof(exact), "%s/%s.db", dir, project);
    if (cbm_file_exists(exact)) {
        return project; /* exact name — untouched fast path */
    }
    size_t plen = strlen(project);
    char match[CBM_SZ_1K] = "";
    int matches = 0;
    cbm_dir_t *d = cbm_opendir(dir);
    if (!d) {
        return project;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        size_t stem_len = len - MCP_DB_EXT; /* strip ".db" */
        if (stem_len <= plen + 1 || stem_len >= sizeof(match)) {
            continue;
        }
        if (n[stem_len - plen - 1] != '-' || strncmp(n + stem_len - plen, project, plen) != 0) {
            continue;
        }
        matches++;
        if (matches > 1) {
            break; /* ambiguous — keep the original name */
        }
        memcpy(match, n, stem_len);
        match[stem_len] = '\0';
    }
    cbm_closedir(d);
    if (matches == 1) {
        cbm_log_info("mcp.project_tail_resolved", "passed", project, "resolved", match);
        free(project);
        return heap_strdup(match);
    }
    return project;
}

/* Resolve the project argument, accepting the canonical "project" key plus the
 * aliases a caller naturally reaches for (#640): list_projects surfaces the
 * field as "name" and the not-found hint says "pass the project name", so
 * "project_name" is the usual guess; "project_id" / "projectName" are accepted
 * too. NOT bare "name" — index_repository uses "name" for an explicit
 * project-name override. Caller must free() the result. */
static char *get_project_arg(const char *args_json) {
    char *p = cbm_mcp_get_string_arg(args_json, "project");
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "project_name");
    }
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "project_id");
    }
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "projectName");
    }
    return resolve_project_tail(normalize_project_arg(p));
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

/* Read tools use a compact tree for the model-facing default. Machine callers
 * can request format:"json", which preserves the structuredContent fallback
 * in cbm_mcp_text_result. Keeping this conversion at the final transport edge
 * avoids maintaining a second semantic response model in each handler. */
static bool mcp_wants_json(const char *args_json) {
    char *format = cbm_mcp_get_string_arg(args_json ? args_json : "{}", "format");
    bool wants_json = format && strcmp(format, "json") == 0;
    free(format);
    return wants_json;
}

static char *mcp_result_from_json(const char *args_json, const char *json) {
    if (!json) {
        return cbm_mcp_text_result("out of memory", true);
    }
    if (mcp_wants_json(args_json)) {
        return cbm_mcp_text_result(json, false);
    }
    char *tree = cbm_json_to_tree(json);
    if (!tree) {
        return cbm_mcp_text_result("failed to render compact output", true);
    }
    char *result = cbm_mcp_text_result(tree, false);
    free(tree);
    return result;
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP SERVER
 * ══════════════════════════════════════════════════════════════════ */

struct cbm_mcp_server {
    cbm_store_t *store;     /* currently open project store (or NULL) */
    bool owns_store;        /* true if we opened the store */
    char *current_project;  /* which project store is open for (heap) */
    time_t store_last_used; /* last time resolve_store was called for a named project */
    /* Set by a query-only resolve that hit a confirmed-corrupt database and
     * left it in place (no quarantine, no rebuild). Error builders read this
     * so the tool reply names the corruption instead of a misleading
     * "project not found". */
    bool readonly_resolve_hit_corrupt;
    char readonly_corrupt_project[CBM_SZ_1K];

    /* Session + auto-index state */
    char session_root[CBM_SZ_1K];     /* detected project root path */
    char session_project[CBM_SZ_256]; /* derived project name */
    bool session_detected;            /* true after first detection attempt */
    char *allowed_root;               /* explicit per-session boundary (heap, nullable) */
    bool allowed_root_policy_set;     /* true even when explicit policy is unrestricted */
    bool background_tasks;            /* per-server update/auto-index work enabled */
    struct cbm_watcher *watcher;      /* external watcher ref (not owned) */
    struct cbm_config *config;        /* external config ref (not owned) */
    cbm_mcp_index_executor_fn index_executor;
    void *index_executor_context;
    cbm_proc_log_cb index_log_callback;
    void *index_log_context;
    cbm_mcp_project_mutation_begin_fn mutation_begin;
    cbm_mcp_project_mutation_try_begin_fn mutation_try_begin;
    cbm_mcp_project_mutation_end_fn mutation_end;
    void *mutation_context;
    cbm_mcp_quarantine_test_hook_fn quarantine_test_hook;
    void *quarantine_test_context;
    cbm_mcp_command_test_hook_fn command_test_hook;
    void *command_test_context;
#ifdef CBM_ENABLE_TEST_SEAMS
    cbm_mcp_auto_index_count_test_hook_fn auto_index_count_test_hook;
    void *auto_index_count_test_context;
    cbm_mcp_snapshot_read_test_hook_fn snapshot_read_test_hook;
    void *snapshot_read_test_context;
#endif
    size_t search_output_limit_override;
    const char *search_scan_command_override;
    uint64_t search_scan_timeout_override_ms;
    bool search_scan_timeout_override_set;
    cbm_thread_t autoindex_tid;
    bool autoindex_active; /* true if auto-index thread was started */

    /* Request-scoped cancellation. The flag is shared by every cancellable
     * operation reached during one tool dispatch; active_pipeline remains a
     * diagnostic pointer for index_repository only. */
    cbm_mutex_t request_scope_mutex;
    unsigned int request_scope_depth;
    atomic_int pipeline_cancel_requested;
    cbm_pipeline_t *active_pipeline; /* non-NULL while index_repository runs */
    int64_t active_request_id;       /* JSON-RPC id of the in-progress tool call */
    char *active_request_id_str;     /* string JSON-RPC id of the in-progress tool call */
    cbm_mcp_tool_profile_t tool_profile;
};

cbm_mcp_server_t *cbm_mcp_server_new(const char *store_path) {
    cbm_mcp_server_t *srv = calloc(CBM_ALLOC_ONE, sizeof(*srv));
    if (!srv) {
        return NULL;
    }
    cbm_mutex_init(&srv->request_scope_mutex);
    atomic_init(&srv->pipeline_cancel_requested, 0);

    /* If a store_path is given, open that project directly.
     * Otherwise, create an in-memory store for test/embedded use. */
    if (store_path) {
        srv->store = cbm_store_open(store_path);
        srv->current_project = heap_strdup(store_path);
    } else {
        srv->store = cbm_store_open_memory();
    }
    srv->owns_store = true;
    srv->tool_profile = CBM_MCP_TOOL_PROFILE_ALL;
    srv->background_tasks = true;

    return srv;
}

void cbm_mcp_server_set_tool_profile(cbm_mcp_server_t *srv, cbm_mcp_tool_profile_t profile) {
    if (srv) {
        srv->tool_profile = profile;
    }
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

#ifdef CBM_ENABLE_TEST_SEAMS
void cbm_mcp_server_set_auto_index_count_test_hook(cbm_mcp_server_t *srv,
                                                   cbm_mcp_auto_index_count_test_hook_fn hook,
                                                   void *context) {
    if (srv) {
        srv->auto_index_count_test_hook = hook;
        srv->auto_index_count_test_context = context;
    }
}
#endif

bool cbm_mcp_server_set_session_context(cbm_mcp_server_t *srv, const char *session_root,
                                        const char *allowed_root) {
    if (!srv || !session_root || session_root[0] == '\0' ||
        strlen(session_root) >= sizeof(srv->session_root)) {
        return false;
    }

    char *project = cbm_project_name_from_path(session_root);
    if (!project || project[0] == '\0' || strlen(project) >= sizeof(srv->session_project)) {
        free(project);
        return false;
    }

    char *allowed_copy = allowed_root ? heap_strdup(allowed_root) : NULL;
    if (allowed_root && !allowed_copy) {
        free(project);
        return false;
    }

    snprintf(srv->session_root, sizeof(srv->session_root), "%s", session_root);
    snprintf(srv->session_project, sizeof(srv->session_project), "%s", project);
    free(project);

    free(srv->allowed_root);
    srv->allowed_root = allowed_copy;
    srv->allowed_root_policy_set = true;
    srv->session_detected = true;
    return true;
}

const char *cbm_mcp_server_session_root(const cbm_mcp_server_t *srv) {
    return srv ? srv->session_root : NULL;
}

const char *cbm_mcp_server_session_project(const cbm_mcp_server_t *srv) {
    return srv ? srv->session_project : NULL;
}

const char *cbm_mcp_server_allowed_root(const cbm_mcp_server_t *srv) {
    return srv ? srv->allowed_root : NULL;
}

void cbm_mcp_server_set_background_tasks(cbm_mcp_server_t *srv, bool enabled) {
    if (srv) {
        srv->background_tasks = enabled;
    }
}

void cbm_mcp_server_set_index_executor(cbm_mcp_server_t *srv, cbm_mcp_index_executor_fn executor,
                                       void *context) {
    if (srv) {
        srv->index_executor = executor;
        srv->index_executor_context = context;
    }
}

void cbm_mcp_server_set_index_log_callback(cbm_mcp_server_t *srv, cbm_proc_log_cb callback,
                                           void *context) {
    if (srv) {
        srv->index_log_callback = callback;
        srv->index_log_context = callback ? context : NULL;
    }
}

void cbm_mcp_server_set_project_mutation_guard(cbm_mcp_server_t *srv,
                                               cbm_mcp_project_mutation_begin_fn begin,
                                               cbm_mcp_project_mutation_end_fn end, void *context) {
    if (!srv) {
        return;
    }
    /* A half-configured guard could acquire without releasing (or mutate
     * without acquiring), so accept only complete callback pairs. */
    if ((begin == NULL) != (end == NULL)) {
        return;
    }
    srv->mutation_begin = begin;
    srv->mutation_try_begin = NULL;
    srv->mutation_end = end;
    srv->mutation_context = begin ? context : NULL;
}

void cbm_mcp_server_set_project_mutation_try_guard(
    cbm_mcp_server_t *srv, cbm_mcp_project_mutation_try_begin_fn try_begin) {
    if (srv && srv->mutation_begin) {
        srv->mutation_try_begin = try_begin;
    }
}

static bool mcp_project_mutation_begin(cbm_mcp_server_t *srv, const char *project) {
    return !srv->mutation_begin || srv->mutation_begin(srv->mutation_context, project);
}

static bool mcp_project_mutation_try_begin(cbm_mcp_server_t *srv, const char *project) {
    return !srv->mutation_begin ||
           (srv->mutation_try_begin && srv->mutation_try_begin(srv->mutation_context, project));
}

static void mcp_project_mutation_end(cbm_mcp_server_t *srv, const char *project) {
    if (srv->mutation_end) {
        srv->mutation_end(srv->mutation_context, project);
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
    free(srv->allowed_root);
    free(srv->active_request_id_str);
    cbm_mutex_destroy(&srv->request_scope_mutex);
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

bool cbm_mcp_server_release_pristine_memory_store(cbm_mcp_server_t *srv) {
    const char *db_path = srv && srv->store ? cbm_store_db_path(srv->store) : NULL;
    if (!srv || !srv->owns_store || !srv->store || srv->current_project ||
        srv->store_last_used != 0 || db_path != NULL) {
        return false;
    }
    cbm_store_close(srv->store);
    srv->store = NULL;
    return true;
}

cbm_pipeline_t *cbm_mcp_server_active_pipeline(cbm_mcp_server_t *srv) {
    return srv ? srv->active_pipeline : NULL;
}

bool cbm_mcp_server_cancel_active(cbm_mcp_server_t *srv) {
    if (!srv) {
        return false;
    }
    cbm_mutex_lock(&srv->request_scope_mutex);
    bool active = srv->request_scope_depth != 0;
    if (active) {
        atomic_store_explicit(&srv->pipeline_cancel_requested, 1, memory_order_release);
    }
    cbm_mutex_unlock(&srv->request_scope_mutex);
    return active;
}

bool cbm_mcp_server_request_scope_begin(cbm_mcp_server_t *srv) {
    if (!srv) {
        return false;
    }
    cbm_mutex_lock(&srv->request_scope_mutex);
    bool available = srv->request_scope_depth < UINT_MAX;
    if (available) {
        if (srv->request_scope_depth == 0) {
            atomic_store_explicit(&srv->pipeline_cancel_requested, 0, memory_order_release);
        }
        srv->request_scope_depth++;
    }
    cbm_mutex_unlock(&srv->request_scope_mutex);
    return available;
}

void cbm_mcp_server_request_scope_end(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    cbm_mutex_lock(&srv->request_scope_mutex);
    if (srv->request_scope_depth > 0) {
        srv->request_scope_depth--;
        if (srv->request_scope_depth == 0) {
            atomic_store_explicit(&srv->pipeline_cancel_requested, 0, memory_order_release);
        }
    }
    cbm_mutex_unlock(&srv->request_scope_mutex);
}

static bool mcp_request_cancelled(const cbm_mcp_server_t *srv) {
    return srv && atomic_load_explicit(&srv->pipeline_cancel_requested, memory_order_acquire) != 0;
}

void cbm_mcp_server_set_quarantine_test_hook(cbm_mcp_server_t *srv,
                                             cbm_mcp_quarantine_test_hook_fn hook, void *context) {
    if (!srv) {
        return;
    }
    srv->quarantine_test_hook = hook;
    srv->quarantine_test_context = context;
}

void cbm_mcp_server_set_command_test_hook(cbm_mcp_server_t *srv, cbm_mcp_command_test_hook_fn hook,
                                          void *context) {
    if (!srv) {
        return;
    }
    srv->command_test_hook = hook;
    srv->command_test_context = context;
}

void cbm_mcp_server_set_search_output_limit_for_test(cbm_mcp_server_t *srv, size_t limit) {
    if (srv) {
        srv->search_output_limit_override = limit;
    }
}

void cbm_mcp_server_set_search_scan_command_for_test(cbm_mcp_server_t *srv, const char *command) {
    if (srv) {
        srv->search_scan_command_override = command;
    }
}

void cbm_mcp_server_set_search_scan_timeout_for_test(cbm_mcp_server_t *srv, uint64_t timeout_ms,
                                                     bool override_set) {
    if (srv) {
        srv->search_scan_timeout_override_ms = timeout_ms;
        srv->search_scan_timeout_override_set = override_set;
    }
}
#ifdef CBM_ENABLE_TEST_SEAMS
void cbm_mcp_server_set_snapshot_read_test_hook(cbm_mcp_server_t *srv,
                                                cbm_mcp_snapshot_read_test_hook_fn hook,
                                                void *context) {
    if (!srv) {
        return;
    }
    srv->snapshot_read_test_hook = hook;
    srv->snapshot_read_test_context = context;
}
#endif

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

/* Read the sole INTERNAL project name from a .db file at full_path.
 * Opens the file query-mode (no create) and succeeds ONLY when the db holds
 * exactly one project row with a non-empty name — this filters ghost/empty
 * /corrupt dbs (0-byte file, missing `projects` table, or >1 row). On success
 * the internal name is copied into name_out; if out_store is non-NULL the open
 * handle is transferred to the caller (who must cbm_store_close it). On failure
 * the store is always closed. Defined after is_project_db_file below. */
static bool db_internal_project_name(const char *full_path, char *name_out, size_t name_sz,
                                     cbm_store_t **out_store);

/* #704 fallback: scan the cache dir for the db whose sole internal project name
 * equals `project`, returning an open store handle (caller owns it) or NULL.
 * Used only when <project>.db is absent or its internal name differs from the
 * passed name (drifted filename). Defined after is_project_db_file below. */
static cbm_store_t *resolve_store_fallback_scan(const char *project);

static bool reserve_unique_corrupt_pending(const char *path, char *pending, size_t pending_size,
                                           char *backup, size_t backup_size) {
    static atomic_uint_fast64_t sequence = 0;
    for (unsigned int attempt = 0; attempt < 128; attempt++) {
        uint64_t token = cbm_now_ns() ^ ((uint64_t)(unsigned int)getpid() << 32) ^
                         atomic_fetch_add_explicit(&sequence, 1, memory_order_relaxed);
        int backup_written =
            snprintf(backup, backup_size, "%s.corrupt.%016llx", path, (unsigned long long)token);
        int pending_written = snprintf(pending, pending_size, "%s.corrupt.pending.%016llx", path,
                                       (unsigned long long)token);
        if (backup_written <= 0 || (size_t)backup_written >= backup_size || pending_written <= 0 ||
            (size_t)pending_written >= pending_size) {
            return false;
        }
        if (cbm_file_exists(backup)) {
            continue;
        }
#ifdef _WIN32
        wchar_t *wide = cbm_path_to_wide(pending);
        HANDLE file = wide ? CreateFileW(wide, GENERIC_READ | GENERIC_WRITE, 0, NULL, CREATE_NEW,
                                         FILE_ATTRIBUTE_NORMAL, NULL)
                           : INVALID_HANDLE_VALUE;
        DWORD create_error = file == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
        free(wide);
        if (file != INVALID_HANDLE_VALUE) {
            CloseHandle(file);
            return true;
        }
        if (create_error != ERROR_FILE_EXISTS && create_error != ERROR_ALREADY_EXISTS) {
            return false;
        }
#else
        int fd = open(pending, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd >= 0) {
            (void)close(fd);
            return true;
        }
        if (errno != EEXIST) {
            return false;
        }
#endif
    }
    return false;
}

static void discard_corrupt_pending(const char *pending) {
    if (!pending) {
        return;
    }
    (void)cbm_remove_db_sidecars(pending);
    (void)cbm_unlink(pending);
}

#ifndef _WIN32
static bool sync_parent_directory(const char *path) {
    char directory[CBM_SZ_2K];
    int written = snprintf(directory, sizeof(directory), "%s", path ? path : "");
    if (written <= 0 || (size_t)written >= sizeof(directory)) {
        return false;
    }
    char *slash = strrchr(directory, '/');
    if (!slash) {
        snprintf(directory, sizeof(directory), ".");
    } else if (slash == directory) {
        slash[1] = '\0';
    } else {
        *slash = '\0';
    }
    int fd = open(directory, O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return false;
    }
    int rc;
    do {
        rc = fsync(fd);
    } while (rc != 0 && errno == EINTR);
    (void)close(fd);
    return rc == 0;
}
#endif

/* Publish only a fully closed SQLite snapshot, without ever replacing a prior
 * recovery file. POSIX link() and Windows MoveFileExW without REPLACE are
 * atomic no-clobber operations within the cache directory. */
static bool publish_corrupt_backup(const char *pending, const char *backup) {
#ifdef _WIN32
    wchar_t *wide_pending = cbm_path_to_wide(pending);
    wchar_t *wide_backup = cbm_path_to_wide(backup);
    bool published = wide_pending && wide_backup &&
                     MoveFileExW(wide_pending, wide_backup, MOVEFILE_WRITE_THROUGH) != 0;
    free(wide_pending);
    free(wide_backup);
    return published;
#else
    if (link(pending, backup) != 0) {
        return false;
    }
    if (!sync_parent_directory(backup)) {
        (void)cbm_unlink(backup);
        return false;
    }
    /* A crash before this cleanup merely leaves a second link to the same
     * complete snapshot; the published recovery generation is already safe. */
    (void)cbm_unlink(pending);
    (void)sync_parent_directory(backup);
    return true;
#endif
}

static bool quarantine_step_allowed(cbm_mcp_server_t *srv, const char *step) {
    return !srv || !srv->quarantine_test_hook ||
           srv->quarantine_test_hook(srv->quarantine_test_context, step);
}

/* Create one transactionally consistent, self-contained recovery snapshot
 * (SQLite backup incorporates committed WAL frames), publish it atomically,
 * and only then remove the corrupt live generation. A crash can therefore
 * leave the live DB, the completed backup, or both, but never destroys the
 * only recoverable generation. */
static bool quarantine_corrupt_store(cbm_mcp_server_t *srv, const char *project, const char *path,
                                     char *backup_out, size_t backup_out_size) {
    char backup[CBM_SZ_2K];
    char pending[CBM_SZ_2K];
    /* #1425 belt-and-braces: an empty store path would render the backup as a
     * bare relative ".corrupt.<hex>" in the process cwd. There is nothing at
     * such a path worth quarantining. */
    if (!path || !path[0]) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", "", "reason",
                      "empty store path");
        return false;
    }
    if (!reserve_unique_corrupt_pending(path, pending, sizeof(pending), backup, sizeof(backup))) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot reserve unique backup");
        return false;
    }

    if (cbm_store_backup_path(path, pending) != CBM_STORE_OK ||
        cbm_store_prepare_path_for_replace(pending) != CBM_STORE_OK) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot create self-contained recovery snapshot");
        return false;
    }

    cbm_store_t *snapshot = cbm_store_open_path_query(pending);
    if (!snapshot) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "recovery snapshot cannot be reopened");
        return false;
    }
    cbm_store_close(snapshot);

    if (!quarantine_step_allowed(srv, "before_snapshot_publish") ||
        !publish_corrupt_backup(pending, backup)) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot atomically publish recovery snapshot");
        return false;
    }
    discard_corrupt_pending(pending);

    if (!quarantine_step_allowed(srv, "after_snapshot_publish")) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "backup complete; live generation retained", "backup", backup);
        return false;
    }

    if (cbm_unlink(path) != 0 && errno != ENOENT) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "backup complete; live database removal failed", "backup", backup);
        return false;
    }
    if (cbm_remove_db_sidecars(path) != 0) {
        cbm_log_error("store.auto_clean_sidecars", "project", project, "path", path, "reason",
                      "backup complete; stale sidecar cleanup deferred");
    }

    if (backup_out && backup_out_size > 0) {
        snprintf(backup_out, backup_out_size, "%s", backup);
    }
    return true;
}

/* Open the right project's .db file for query tools.
 * Caches the connection — reopens only when project changes.
 * Tracks last-access time so the event loop can evict idle stores. */
typedef enum {
    STORE_RECOVERY_NONE,
    STORE_RECOVERY_BUSY,
    STORE_RECOVERY_TRY_GUARD_UNAVAILABLE,
    /* Query-only resolve: a confirmed-corrupt database was reported and left
     * in place. Not an error state of the recovery machinery — the caller is
     * expected to answer with the corruption, not retry. */
    STORE_RECOVERY_CORRUPT,
} store_recovery_status_t;

static cbm_store_t *resolve_store_internal(cbm_mcp_server_t *srv, const char *project,
                                           bool mutation_already_held, bool nonblocking_recovery,
                                           store_recovery_status_t *recovery_status,
                                           bool allow_autorecovery) {
    if (recovery_status) {
        *recovery_status = STORE_RECOVERY_NONE;
    }
    srv->readonly_resolve_hit_corrupt = false;
    srv->readonly_corrupt_project[0] = '\0';
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
     * prevent ghost .db file creation for unknown/unindexed projects.
     * #1425: an invalid project name yields an empty path. SQLite opens ""
     * as an anonymous temp db, which then fails the integrity check and
     * quarantines a db that never existed — as a RELATIVE .corrupt.<hex>
     * file in the daemon's cwd. Skip the direct open entirely; the fallback
     * scan below still resolves legacy dbs whose internal name predates
     * validation. */
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    srv->store = path[0] ? cbm_store_open_path_query(path) : NULL;
    if (srv->store) {
        /* Query-only resolve: classify a failed integrity check without any
         * mutation — no lease, no quarantine. A corrupt database is reported
         * and left in place for a write-side open (index_repository) to
         * quarantine and rebuild; that is what keeps the ten read-only
         * tools' readOnlyHint=true honest. A transient failure (lock, IO, or
         * the shallow check's own prepare-error-as-corrupt blind spot) is
         * answered as busy and retried by the next resolve. */
        if (!allow_autorecovery && !cbm_store_check_integrity(srv->store)) {
            cbm_store_close(srv->store);
            srv->store = NULL;
            srv->store = cbm_store_open_path_query(path);
            cbm_integrity_verdict_t verdict = srv->store
                                                  ? cbm_store_check_integrity_verdict(srv->store)
                                                  : CBM_INTEGRITY_TRANSIENT;
            if (srv->store) {
                cbm_store_close(srv->store);
                srv->store = NULL;
            }
            if (verdict == CBM_INTEGRITY_CORRUPT) {
                srv->readonly_resolve_hit_corrupt = true;
                snprintf(srv->readonly_corrupt_project, sizeof(srv->readonly_corrupt_project), "%s",
                         project);
                cbm_log_warn("store.corrupt_readonly", "project", project, "path", path, "action",
                             "left in place for a write-side rebuild");
                if (recovery_status) {
                    *recovery_status = STORE_RECOVERY_CORRUPT;
                }
            } else if (recovery_status) {
                *recovery_status = STORE_RECOVERY_BUSY;
            }
            return NULL;
        }
        /* Check DB integrity — back up (never silently delete) a corrupt DB */
        if (!cbm_store_check_integrity(srv->store)) {
            cbm_store_close(srv->store);
            srv->store = NULL;
            bool mutation_acquired = mutation_already_held;
            if (!mutation_acquired) {
                mutation_acquired = nonblocking_recovery
                                        ? mcp_project_mutation_try_begin(srv, project)
                                        : mcp_project_mutation_begin(srv, project);
            }
            if (!mutation_acquired) {
                if (nonblocking_recovery && recovery_status) {
                    *recovery_status = srv->mutation_try_begin
                                           ? STORE_RECOVERY_BUSY
                                           : STORE_RECOVERY_TRY_GUARD_UNAVAILABLE;
                }
                return NULL;
            }

            /* The lease may have waited behind a publisher. Re-open and trust
             * only the current generation, never the stale pre-wait verdict.
             * Use the verdict API here — this is the point that decides whether
             * a healthy DB gets quarantined. The plain bool check cannot tell
             * corruption from a transient SQLITE_BUSY race (#1206: concurrent
             * instances quarantining each other's DBs) and does not run
             * quick_check, so page-torn DBs with an intact projects table sail
             * through (#1037). Only a confirmed CORRUPT verdict is quarantined;
             * TRANSIENT (lock/IO) falls through and retries on next access. */
            srv->store = cbm_store_open_path_query(path);
            cbm_integrity_verdict_t verdict = srv->store
                                                  ? cbm_store_check_integrity_verdict(srv->store)
                                                  : CBM_INTEGRITY_TRANSIENT;
            bool current_valid = (verdict == CBM_INTEGRITY_OK);
            if (verdict == CBM_INTEGRITY_TRANSIENT) {
                /* The DB could not be conclusively evaluated (lock contention,
                 * busy writer, IO hiccup). Do NOT quarantine — close and let
                 * the next resolve retry. A spurious quarantine here is exactly
                 * what destroys healthy DBs under concurrent access. */
                cbm_store_close(srv->store);
                srv->store = NULL;
                if (recovery_status) {
                    *recovery_status = STORE_RECOVERY_BUSY;
                }
                if (!mutation_already_held) {
                    mcp_project_mutation_end(srv, project);
                }
                return NULL;
            }
            if (!current_valid) {
                cbm_store_close(srv->store);
                srv->store = NULL;
                char backup[CBM_SZ_2K] = {0};
                bool quarantined =
                    quarantine_corrupt_store(srv, project, path, backup, sizeof(backup));
                cbm_log_error("store.auto_clean", "project", project, "path", path, "action",
                              quarantined ? "corrupt generation quarantined"
                                          : "corrupt generation preserved",
                              "backup", quarantined ? backup : "none");
            }
            if (!mutation_already_held) {
                mcp_project_mutation_end(srv, project);
            }
            if (!srv->store) {
                return NULL;
            }
        }

        /* Verify the project actually exists in this database.
         * A .db file may exist but be empty (e.g., after delete_project on
         * Linux where unlink defers actual removal). Opening an empty/deleted
         * store without closing it leaks the SQLite connection. */
        cbm_project_t proj_verify = {0};
        if (cbm_store_get_project(srv->store, project, &proj_verify) == CBM_STORE_OK) {
            cbm_project_free_fields(&proj_verify);
            srv->owns_store = true;
            free(srv->current_project);
            srv->current_project = heap_strdup(project);
            return srv->store; /* fast path: filename == internal name */
        }
        /* #704: <project>.db exists but its INTERNAL project name differs from
         * the passed name (a copied/renamed db, or a legacy '.'-vs-'-' username
         * twin). Close it and fall through to the cache-dir scan below. */
        cbm_store_close(srv->store);
        srv->store = NULL;
    }

    /* #704 fallback: either <project>.db is absent or its internal name drifted
     * from its filename. Node rows are keyed on the INTERNAL name (== the passed
     * name, since list_projects now advertises internal names), so scan the
     * cache dir for the db whose sole internal project name equals `project` and
     * adopt it. Runs ONLY on the fallback — the common fast path is unchanged.
     * No match → NULL (a genuine typo stays not-found). */
    cbm_store_t *scanned = resolve_store_fallback_scan(project);
    if (scanned) {
        srv->store = scanned;
        srv->owns_store = true;
        free(srv->current_project);
        srv->current_project = heap_strdup(project);
    }

    return srv->store;
}

static cbm_store_t *resolve_store(cbm_mcp_server_t *srv, const char *project) {
    /* Query-only callers (every read tool): strictly non-mutating resolve. */
    return resolve_store_internal(srv, project, false, false, NULL, false);
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
        /* #704: advertise the db's INTERNAL project name, not its filename, and
         * skip ghost/empty/corrupt dbs — so the hint lists names the user can
         * actually pass to resolve a store. */
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        char iname[CBM_SZ_1K];
        if (!db_internal_project_name(full_path, iname, sizeof(iname), NULL)) {
            continue;
        }
        /* Element-boundary write: only emit this name if the WHOLE element —
         * optional leading comma + "iname" — plus the NUL fits in what remains.
         * Never truncate mid-token; a partial name would corrupt the JSON array
         * (issue #235). Stop cleanly at the last name that fits: the array then
         * always holds complete names and `count` == its length. */
        size_t off = (size_t)offset;
        size_t need = strlen(iname) + 2 /* quotes */ + (count > 0 ? 1u : 0u) /* comma */;
        if (off + need + 1 > out_sz) {
            break; /* would not fit entirely — stop at this element boundary */
        }
        if (count > 0) {
            out[offset++] = ',';
        }
        int wrote = snprintf(out + offset, out_sz - (size_t)offset, "\"%s\"", iname);
        if (wrote > 0) {
            offset += wrote; /* guaranteed to fit (checked above) — no truncation */
        }
        count++;
    }
    cbm_closedir(d);
    return count;
}

static void add_git_context_string(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                   const char *value) {
    if (value) {
        yyjson_mut_obj_add_strcpy(doc, obj, key, value);
    } else {
        yyjson_mut_obj_add_null(doc, obj, key);
    }
}

static void add_git_context_json(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *root_path) {
    cbm_git_context_t ctx = {0};
    (void)cbm_git_context_resolve(root_path, &ctx);

    yyjson_mut_val *git = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, git, "is_git", ctx.is_git);
    yyjson_mut_obj_add_bool(doc, git, "is_worktree", ctx.is_worktree);
    yyjson_mut_obj_add_bool(doc, git, "is_detached", ctx.is_detached);
    yyjson_mut_obj_add_bool(doc, git, "root_exists", ctx.root_exists);
    add_git_context_string(doc, git, "worktree_root", ctx.worktree_root);
    add_git_context_string(doc, git, "git_dir", ctx.git_dir);
    add_git_context_string(doc, git, "git_common_dir", ctx.git_common_dir);
    add_git_context_string(doc, git, "canonical_root", ctx.canonical_root);
    add_git_context_string(doc, git, "branch", ctx.branch);
    add_git_context_string(doc, git, "branch_slug", ctx.branch_slug);
    add_git_context_string(doc, git, "head_sha", ctx.head_sha);
    add_git_context_string(doc, git, "base_sha", ctx.base_sha);
    yyjson_mut_obj_add_val(doc, obj, "git", git);

    cbm_git_context_free(&ctx);
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
                 "then pass it as the \\\"project\\\" "
                 "argument.\",\"available_projects\":[%s],\"count\":%d}",
                 reason, projects, count);
    } else {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"No projects indexed yet. "
                 "Call index_repository first.\"}",
                 reason);
    }
    return heap_strdup(buf);
}

/* Distinct from "unknown project": the caller omitted the project argument
 * entirely (no recognized key). Name the literal "project" key so the fix is
 * obvious (#640). Caller must free() result. */
static char *build_missing_project_error(void) {
    return heap_strdup("{\"error\":\"missing required argument: project\",\"hint\":\"Pass "
                       "the project as the \\\"project\\\" argument, e.g. "
                       "{\\\"project\\\":\\\"<name from list_projects>\\\"}. Run "
                       "list_projects to see indexed projects.\"}");
}

/* Pick the right no-store error: a NULL project means the argument was missing
 * (clearer message); a non-NULL project that didn't resolve means it's
 * unknown/unindexed (list the available ones). */
static char *build_no_store_error(const char *project) {
    return project ? build_project_list_error("project not found or not indexed")
                   : build_missing_project_error();
}

/* Same contract as build_no_store_error, but when the query-only resolve
 * confirmed a corrupt database and left it in place, name that instead of a
 * misleading "project not found". */
static char *build_no_store_error_checked(cbm_mcp_server_t *srv, const char *project) {
    if (srv->readonly_resolve_hit_corrupt && project &&
        strcmp(project, srv->readonly_corrupt_project) == 0) {
        return build_project_list_error(
            "project store is corrupt (left untouched); run index_repository to rebuild it");
    }
    return build_no_store_error(project);
}

/* Bail with the right error when no store is available. */
#define REQUIRE_STORE(store, project)                                \
    do {                                                             \
        if (!(store)) {                                              \
            char *_err = build_no_store_error_checked(srv, project); \
            char *_res = cbm_mcp_text_result(_err, true);            \
            free(_err);                                              \
            free(project);                                           \
            return _res;                                             \
        }                                                            \
    } while (0)

static bool project_has_adr(cbm_store_t *store, const char *project, const char *root_path) {
    if (store && project) {
        cbm_adr_t adr;
        memset(&adr, 0, sizeof(adr));
        if (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK) {
            cbm_store_adr_free(&adr);
            return true;
        }
    }

    if (!root_path) {
        return false;
    }

    char adr_path[CBM_SZ_4K];
    snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", root_path);
    struct stat adr_st;
    return stat(adr_path, &adr_st) == 0;
}

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

/* db_internal_project_name — see forward declaration above resolve_store. */
static bool db_internal_project_name(const char *full_path, char *name_out, size_t name_sz,
                                     cbm_store_t **out_store) {
    if (out_store) {
        *out_store = NULL;
    }
    cbm_store_t *st = cbm_store_open_path_query(full_path);
    if (!st) {
        return false; /* nonexistent / unreadable */
    }
    cbm_project_t *projs = NULL;
    int n = 0;
    bool ok = false;
    if (cbm_store_list_projects(st, &projs, &n) == CBM_STORE_OK) {
        /* Ignore internal shadow projects ("<name>::missed" miss-graph rows):
         * they share the db with the primary project and must not make it
         * unresolvable — requiring n == 1 over ALL rows made every project
         * with a miss graph vanish from list_projects and the UI (#1044). */
        int primary = -1;
        int primary_count = 0;
        for (int i = 0; i < n; i++) {
            if (projs[i].name && projs[i].name[0] && !strstr(projs[i].name, "::")) {
                primary = i;
                primary_count++;
            }
        }
        if (primary_count == 1) {
            snprintf(name_out, name_sz, "%s", projs[primary].name);
            ok = true;
        }
    }
    cbm_store_free_projects(projs, n);
    if (ok && out_store) {
        *out_store = st; /* transfer ownership to caller */
    } else {
        cbm_store_close(st);
    }
    return ok;
}

/* resolve_store_fallback_scan — see forward declaration above resolve_store. */
static cbm_store_t *resolve_store_fallback_scan(const char *project) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) {
        return NULL;
    }
    cbm_store_t *found = NULL;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        char iname[CBM_SZ_1K];
        cbm_store_t *st = NULL;
        if (db_internal_project_name(full_path, iname, sizeof(iname), &st)) {
            if (strcmp(iname, project) == 0) {
                found = st; /* adopt — caller takes ownership */
                break;
            }
            cbm_store_close(st);
        }
    }
    cbm_closedir(d);
    return found;
}

typedef struct {
    char *name;
    char *root_path;
    char *db_file;
    char *branch;
    int nodes;
    int edges;
    int64_t size_bytes;
    bool populated;
} mcp_project_record_t;

static void project_record_clear(mcp_project_record_t *record) {
    if (!record) {
        return;
    }
    free(record->name);
    free(record->root_path);
    free(record->db_file);
    free(record->branch);
    memset(record, 0, sizeof(*record));
}

static int project_record_compare(const void *left, const void *right) {
    const mcp_project_record_t *a = left;
    const mcp_project_record_t *b = right;
    int by_name = strcmp(a->name ? a->name : "", b->name ? b->name : "");
    if (by_name != 0) {
        return by_name;
    }
    int by_root = strcmp(a->root_path ? a->root_path : "", b->root_path ? b->root_path : "");
    if (by_root != 0) {
        return by_root;
    }
    return strcmp(a->db_file ? a->db_file : "", b->db_file ? b->db_file : "");
}

typedef enum {
    PROJECT_RECORD_SKIP = 0,
    PROJECT_RECORD_OK = 1,
    PROJECT_RECORD_OOM = -1,
} project_record_status_t;

/* Open a .db file briefly and collect sortable identity only. Counts and Git
 * context are populated after paging so a 50-row response does not repeat
 * heavyweight work for every database in the cache. */
static project_record_status_t read_project_record_identity(const char *dir_path, const char *name,
                                                            int64_t size_bytes,
                                                            mcp_project_record_t *record) {
    memset(record, 0, sizeof(*record));

    char full_path[CBM_SZ_2K];
    snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);

    /* #704: key on the db's INTERNAL project name, not its filename. Node/edge
     * rows are tagged with the internal name, so a drifted filename (copied or
     * renamed db, legacy '.'-vs-'-' username twin) would otherwise report 0
     * nodes/edges and be unresolvable. Skip ghost/empty/corrupt dbs entirely so
     * they don't appear as resolvable projects. */
    char project_name[CBM_SZ_1K];
    cbm_store_t *pstore = NULL;
    if (!db_internal_project_name(full_path, project_name, sizeof(project_name), &pstore)) {
        return PROJECT_RECORD_SKIP; /* ghost / unreadable — not resolvable */
    }

    record->name = heap_strdup(project_name);
    record->db_file = heap_strdup(name);
    record->size_bytes = size_bytes;
    cbm_project_t proj = {0};
    if (cbm_store_get_project(pstore, project_name, &proj) == CBM_STORE_OK) {
        /* root_path is NOT NULL in the store schema, so a missing copy here is
         * allocation failure rather than an empty identity. */
        if (!proj.root_path) {
            cbm_project_free_fields(&proj);
            cbm_store_close(pstore);
            project_record_clear(record);
            return PROJECT_RECORD_OOM;
        }
        /* Store project fields are heap-owned by the caller. Transfer the root
         * identity intact: it is the value used for sorting, output, and branch
         * lookup, so routing it through a fixed buffer would silently change
         * all three for long roots. */
        record->root_path = (char *)proj.root_path;
        proj.root_path = NULL;
        cbm_project_free_fields(&proj);
    }
    cbm_store_close(pstore);
    if (!record->root_path) {
        record->root_path = heap_strdup("");
    }
    if (!record->name || !record->root_path || !record->db_file) {
        project_record_clear(record);
        return PROJECT_RECORD_OOM;
    }
    return PROJECT_RECORD_OK;
}

static bool populate_project_record(const char *dir_path, bool include_stats,
                                    mcp_project_record_t *record) {
    if (record->populated) {
        record->populated = true;
        return true;
    }
    if (include_stats) {
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, record->db_file);
        cbm_store_t *pstore = cbm_store_open_path_query(full_path);
        if (!pstore) {
            return false;
        }
        record->nodes = cbm_store_count_nodes(pstore, record->name);
        record->edges = cbm_store_count_edges(pstore, record->name);
        cbm_store_close(pstore);
    }
    /* Listing stays lean: only the branch (the one git fact that
     * disambiguates same-repo projects). The 12-field git block — mostly
     * null for non-git roots — cost ~10KB across a full cache and is one
     * index_status call away for the project you actually care about. */
    if (record->root_path[0]) {
        cbm_git_context_t gctx = {0};
        (void)cbm_git_context_resolve(record->root_path, &gctx);
        if (gctx.is_git && gctx.branch) {
            record->branch = heap_strdup(gctx.branch);
            if (!record->branch) {
                cbm_git_context_free(&gctx);
                return false;
            }
        }
        cbm_git_context_free(&gctx);
    }
    record->populated = true;
    return true;
}

static void add_project_record_json(yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                    const mcp_project_record_t *record, bool include_stats) {
    yyjson_mut_val *p = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, p, "name", record->name);
    yyjson_mut_obj_add_strcpy(doc, p, "root_path", record->root_path);
    if (record->branch) {
        yyjson_mut_obj_add_strcpy(doc, p, "branch", record->branch);
    }
    if (include_stats) {
        yyjson_mut_obj_add_int(doc, p, "nodes", record->nodes);
        yyjson_mut_obj_add_int(doc, p, "edges", record->edges);
        yyjson_mut_obj_add_int(doc, p, "size_bytes", record->size_bytes);
    }
    yyjson_mut_arr_add_val(arr, p);
}

/* list_projects: scan cache directory for .db files.
 * Each project is a single .db file — no central registry needed. */
static char *handle_list_projects(cbm_mcp_server_t *srv, const char *args) {
    (void)srv;
    bool metadata_only = false;
    yyjson_doc *args_doc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *metadata_value =
        args_root && yyjson_is_obj(args_root) ? yyjson_obj_get(args_root, "metadata_only") : NULL;
    metadata_only = metadata_value && yyjson_is_true(metadata_value);
    char *detail = cbm_mcp_get_string_arg(args, "detail");
    /* `include_details` is the boolean spelling main shipped for the same
     * request; both select the stats projection. */
    bool include_stats = !metadata_only && ((detail && strcmp(detail, "stats") == 0) ||
                                            cbm_mcp_get_bool_arg(args, "include_details"));
    free(detail);
    if (args_doc) {
        yyjson_doc_free(args_doc);
    }

    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));

    cbm_dir_t *d = cbm_opendir(dir_path);

    if (!d) {
        char msg[CBM_SZ_1K];
        snprintf(msg, sizeof(msg),
                 "{\"error\":\"cannot read cache directory: %s\",\"hint\":"
                 "\"Check directory permissions or run index_repository first.\"}",
                 dir_path);
        return cbm_mcp_text_result(msg, true);
    }

    mcp_project_record_t *records = NULL;
    int record_count = 0;
    int record_cap = 0;
    bool oom = false;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *name = entry->name;
        size_t len = strlen(name);
        if (!is_project_db_file(name, len)) {
            continue;
        }
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);
        int64_t size_bytes = cbm_file_size(full_path);
        if (size_bytes < 0) {
            continue;
        }
        mcp_project_record_t record = {0};
        project_record_status_t read_status =
            read_project_record_identity(dir_path, name, size_bytes, &record);
        if (read_status == PROJECT_RECORD_OOM) {
            oom = true;
            break;
        }
        if (read_status != PROJECT_RECORD_OK) {
            continue;
        }
        if (record_count == record_cap) {
            int new_cap = record_cap ? record_cap * 2 : 16;
            void *grown = realloc(records, (size_t)new_cap * sizeof(*records));
            if (!grown) {
                project_record_clear(&record);
                oom = true;
                break;
            }
            records = grown;
            record_cap = new_cap;
        }
        records[record_count++] = record;
    }
    cbm_closedir(d);

    if (oom) {
        for (int i = 0; i < record_count; i++) {
            project_record_clear(&records[i]);
        }
        free(records);
        return cbm_mcp_text_result("out of memory while listing projects", true);
    }
    qsort(records, (size_t)record_count, sizeof(*records), project_record_compare);

    int limit = cbm_mcp_get_int_arg(args, "limit", 50);
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    if (limit < 1) {
        limit = 1;
    } else if (limit > 500) {
        limit = 500;
    }
    if (offset < 0) {
        offset = 0;
    }
    int start = offset < record_count ? offset : record_count;
    int end = start + limit < record_count ? start + limit : record_count;
    if (!metadata_only) {
        for (int i = start; i < end; i++) {
            if (!populate_project_record(dir_path, include_stats, &records[i])) {
                oom = true;
                break;
            }
        }
    }
    if (oom) {
        for (int i = 0; i < record_count; i++) {
            project_record_clear(&records[i]);
        }
        free(records);
        return cbm_mcp_text_result("failed to populate project page", true);
    }

    bool wants_json = mcp_wants_json(args);
    char *payload = NULL;
    if (wants_json) {
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);
        yyjson_mut_val *arr = yyjson_mut_arr(doc);
        for (int i = start; i < end; i++) {
            add_project_record_json(doc, arr, &records[i], include_stats);
        }

        yyjson_mut_obj_add_val(doc, root, "projects", arr);
        yyjson_mut_obj_add_int(doc, root, "total", record_count);
        yyjson_mut_obj_add_int(doc, root, "offset", offset);
        yyjson_mut_obj_add_int(doc, root, "limit", limit);
        yyjson_mut_obj_add_int(doc, root, "returned", end - start);
        yyjson_mut_obj_add_bool(doc, root, "has_more", end < record_count);
        if (end < record_count) {
            yyjson_mut_obj_add_int(doc, root, "next_offset", end);
        }
        if (record_count == 0) {
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "No projects indexed. Call index_repository(repo_path=...) first.");
        }
        payload = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    } else {
        /* Stable union schema: branch is an explicit empty marker instead of
         * changing each object's shape and forcing the generic tree emitter
         * to repeat every key. Values and row order remain direct/copyable. */
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        const char *metadata_cols[] = {"name", "root_path"};
        const char *identity_cols[] = {"name", "root_path", "branch"};
        const char *stats_cols[] = {"name", "root_path", "branch", "nodes", "edges", "size_bytes"};
        const char *const *cols = metadata_only   ? metadata_cols
                                  : include_stats ? stats_cols
                                                  : identity_cols;
        int column_count = metadata_only ? 2 : include_stats ? 6 : 3;
        int page_count = end - start;
        size_t cell_count = (size_t)page_count * (size_t)column_count;
        const char **cells = cell_count > 0 && cell_count <= SIZE_MAX / sizeof(*cells)
                                 ? malloc(cell_count * sizeof(*cells))
                                 : NULL;
        char (*numbers)[32] = include_stats && page_count > 0
                                  ? malloc((size_t)page_count * 3U * sizeof(*numbers))
                                  : NULL;
        if (cells && (!include_stats || numbers)) {
            for (int row = 0; row < page_count; row++) {
                const mcp_project_record_t *record = &records[start + row];
                size_t base = (size_t)row * (size_t)column_count;
                cells[base] = record->name;
                cells[base + 1U] = record->root_path;
                if (!metadata_only) {
                    cells[base + 2U] = record->branch ? record->branch : "";
                }
                if (include_stats) {
                    size_t number_base = (size_t)row * 3U;
                    snprintf(numbers[number_base], sizeof(numbers[number_base]), "%d",
                             record->nodes);
                    snprintf(numbers[number_base + 1U], sizeof(numbers[number_base + 1U]), "%d",
                             record->edges);
                    snprintf(numbers[number_base + 2U], sizeof(numbers[number_base + 2U]), "%lld",
                             (long long)record->size_bytes);
                    cells[base + 3U] = numbers[number_base];
                    cells[base + 4U] = numbers[number_base + 1U];
                    cells[base + 5U] = numbers[number_base + 2U];
                }
            }
            const bool string_cols[] = {true, true, true, false, false, false};
            const bool prefix_cols[] = {false, true, false, false, false, false};
            cbm_tree_table_rows_profiled(&sb, "projects", page_count, cols, column_count, cells,
                                         string_cols, prefix_cols);
        } else {
            cbm_tree_table_header(&sb, "projects", page_count, cols, column_count);
            for (int i = start; i < end; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, records[i].name, true);
                cbm_tree_cell_str(&sb, records[i].root_path, false);
                if (!metadata_only) {
                    cbm_tree_cell_str(&sb, records[i].branch ? records[i].branch : "", false);
                }
                if (include_stats) {
                    cbm_tree_cell_int(&sb, records[i].nodes, false);
                    cbm_tree_cell_int(&sb, records[i].edges, false);
                    cbm_tree_cell_int(&sb, (long long)records[i].size_bytes, false);
                }
                cbm_tree_row_end(&sb);
            }
        }
        free(numbers);
        free(cells);
        cbm_tree_scalar_int(&sb, "total", record_count);
        cbm_tree_scalar_int(&sb, "returned", end - start);
        cbm_tree_scalar_bool(&sb, "has_more", end < record_count);
        if (end < record_count) {
            cbm_tree_scalar_int(&sb, "next_offset", end);
        }
        if (record_count == 0) {
            cbm_tree_scalar_str(&sb, "hint",
                                "No projects indexed. Call index_repository(repo_path=...) first.");
        }
        payload = cbm_sb_finish(&sb);
    }

    for (int i = 0; i < record_count; i++) {
        project_record_clear(&records[i]);
    }
    free(records);

    char *result = cbm_mcp_text_result(payload ? payload : "out of memory", payload == NULL);
    free(payload);
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

/* compare_graphs deliberately bypasses resolve_store(): it needs two
 * independently-owned request-scoped read handles, while resolve_store caches
 * one handle on the server. Direct-name lookup stays the fast path; the
 * existing internal-name fallback preserves legacy renamed databases. */
static cbm_store_t *compare_open_project_store(const char *project) {
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    cbm_store_t *store = path[0] ? cbm_store_open_path_query(path) : NULL;
    if (store) {
        cbm_project_t row = {0};
        if (cbm_store_get_project(store, project, &row) == CBM_STORE_OK) {
            cbm_project_free_fields(&row);
            return store;
        }
        cbm_store_close(store);
    }
    return resolve_store_fallback_scan(project);
}

typedef struct {
    yyjson_mut_val *items;
    size_t returned;
    size_t encoded_bytes;
    bool budget_exhausted;
} compare_result_set_t;

typedef struct {
    cbm_mcp_server_t *server;
    yyjson_mut_doc *doc;
    size_t limit;
    compare_result_set_t nodes_added;
    compare_result_set_t nodes_removed;
    compare_result_set_t edges_added;
    compare_result_set_t edges_removed;
} compare_response_t;

static char *compare_graphs_error(const char *code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        yyjson_mut_doc_free(doc);
        return cbm_mcp_text_result("compare_graphs failed: out of memory", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    if (!yyjson_mut_obj_add_strcpy(doc, root, "error", message) ||
        !yyjson_mut_obj_add_strcpy(doc, root, "code", code)) {
        yyjson_mut_doc_free(doc);
        return cbm_mcp_text_result("compare_graphs failed: out of memory", true);
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return cbm_mcp_text_result("compare_graphs failed: out of memory", true);
    }
    char *result = cbm_mcp_text_result(json, true);
    free(json);
    return result;
}

static bool compare_arg_name_allowed(const char *name) {
    return strcmp(name, "base_project") == 0 || strcmp(name, "target_project") == 0 ||
           strcmp(name, "limit") == 0 || strcmp(name, "scan_limit") == 0;
}

static bool compare_parse_bounded_integer(yyjson_val *root, const char *key, int64_t default_value,
                                          int64_t maximum, uint64_t *out,
                                          const char **error_message) {
    yyjson_val *value = yyjson_obj_get(root, key);
    int64_t parsed = default_value;
    if (value) {
        if (!yyjson_is_int(value)) {
            *error_message = "limit values must be integers";
            return false;
        }
        parsed = yyjson_get_int(value);
    }
    if (parsed < 1 || parsed > maximum) {
        *error_message = strcmp(key, "limit") == 0 ? "limit must be between 1 and 1000"
                                                   : "scan_limit must be between 1 and 10000000";
        return false;
    }
    *out = (uint64_t)parsed;
    return true;
}

static bool compare_parse_arguments(const char *args, char **base_project, char **target_project,
                                    uint64_t *limit, uint64_t *scan_limit,
                                    const char **error_message) {
    *base_project = NULL;
    *target_project = NULL;
    yyjson_doc *doc = yyjson_read(args ? args : "{}", args ? strlen(args) : SLEN("{}"), 0);
    if (!doc) {
        *error_message = "arguments must be valid JSON";
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        *error_message = "arguments must be an object";
        yyjson_doc_free(doc);
        return false;
    }

    yyjson_obj_iter iterator = yyjson_obj_iter_with(root);
    yyjson_val *key = NULL;
    while ((key = yyjson_obj_iter_next(&iterator)) != NULL) {
        const char *name = yyjson_get_str(key);
        if (!name || !compare_arg_name_allowed(name)) {
            *error_message = "unknown argument";
            yyjson_doc_free(doc);
            return false;
        }
    }

    yyjson_val *base = yyjson_obj_get(root, "base_project");
    yyjson_val *target = yyjson_obj_get(root, "target_project");
    if (!base || !yyjson_is_str(base) || yyjson_get_len(base) == 0 || !target ||
        !yyjson_is_str(target) || yyjson_get_len(target) == 0) {
        *error_message = "base_project and target_project are required non-empty strings";
        yyjson_doc_free(doc);
        return false;
    }
    if (strcmp(yyjson_get_str(base), yyjson_get_str(target)) == 0) {
        *error_message = "base_project and target_project must be distinct";
        yyjson_doc_free(doc);
        return false;
    }
    if (!compare_parse_bounded_integer(root, "limit", MCP_COMPARE_DEFAULT_LIMIT,
                                       MCP_COMPARE_MAX_LIMIT, limit, error_message) ||
        !compare_parse_bounded_integer(root, "scan_limit", MCP_COMPARE_DEFAULT_SCAN_LIMIT,
                                       MCP_COMPARE_MAX_SCAN_LIMIT, scan_limit, error_message)) {
        yyjson_doc_free(doc);
        return false;
    }

    *base_project = heap_strdup(yyjson_get_str(base));
    *target_project = heap_strdup(yyjson_get_str(target));
    yyjson_doc_free(doc);
    if (!*base_project || !*target_project) {
        free(*base_project);
        free(*target_project);
        *base_project = NULL;
        *target_project = NULL;
        *error_message = "out of memory while validating arguments";
        return false;
    }
    return true;
}

static char *sanitize_utf8_lossy(const char *s);

static bool compare_add_identity_string(yyjson_mut_doc *doc, yyjson_mut_val *object,
                                        const char *key, const char *value) {
    char *sanitized = sanitize_utf8_lossy(value);
    if (!sanitized) {
        return false;
    }
    bool ok = yyjson_mut_obj_add_strcpy(doc, object, key, sanitized);
    free(sanitized);
    return ok;
}

static yyjson_mut_val *compare_node_json(yyjson_mut_doc *doc,
                                         const cbm_graph_node_identity_t *node) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    if (!object ||
        !compare_add_identity_string(doc, object, "qualified_name", node->qualified_name) ||
        !compare_add_identity_string(doc, object, "label", node->label) ||
        !compare_add_identity_string(doc, object, "file_path", node->file_path)) {
        return NULL;
    }
    return object;
}

static bool compare_append_item(compare_response_t *response, compare_result_set_t *set,
                                yyjson_mut_doc *item_doc, yyjson_mut_val *item) {
    if (!item_doc || !item) {
        yyjson_mut_doc_free(item_doc);
        return false;
    }
    char *encoded = yy_doc_to_str(item_doc);
    if (!encoded) {
        yyjson_mut_doc_free(item_doc);
        return false;
    }
    size_t encoded_len = strlen(encoded);
    size_t separator = set->returned > 0 ? 1U : 0U;
    free(encoded);

    if (set->encoded_bytes > MCP_COMPARE_SET_BYTE_BUDGET ||
        separator > MCP_COMPARE_SET_BYTE_BUDGET - set->encoded_bytes ||
        encoded_len > MCP_COMPARE_SET_BYTE_BUDGET - set->encoded_bytes - separator) {
        set->budget_exhausted = true;
        yyjson_mut_doc_free(item_doc);
        return true;
    }

    yyjson_mut_val *copy = yyjson_mut_val_mut_copy(response->doc, item);
    bool ok = copy && yyjson_mut_arr_add_val(set->items, copy);
    yyjson_mut_doc_free(item_doc);
    if (!ok) {
        return false;
    }
    set->encoded_bytes += separator + encoded_len;
    set->returned++;
    return true;
}

static bool compare_node_callback(void *context, bool added,
                                  const cbm_graph_node_identity_t *node) {
    compare_response_t *response = (compare_response_t *)context;
    compare_result_set_t *set = added ? &response->nodes_added : &response->nodes_removed;
    if (set->returned >= response->limit || set->budget_exhausted) {
        return true;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *item = doc ? compare_node_json(doc, node) : NULL;
    if (doc && item) {
        yyjson_mut_doc_set_root(doc, item);
    }
    return compare_append_item(response, set, doc, item);
}

static bool compare_edge_callback(void *context, bool added,
                                  const cbm_graph_edge_identity_t *edge) {
    compare_response_t *response = (compare_response_t *)context;
    compare_result_set_t *set = added ? &response->edges_added : &response->edges_removed;
    if (set->returned >= response->limit || set->budget_exhausted) {
        return true;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *item = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *source = doc ? compare_node_json(doc, &edge->source) : NULL;
    yyjson_mut_val *target = doc ? compare_node_json(doc, &edge->target) : NULL;
    bool ok = item && source && target && yyjson_mut_obj_add_val(doc, item, "source", source) &&
              yyjson_mut_obj_add_val(doc, item, "target", target) &&
              compare_add_identity_string(doc, item, "type", edge->type) &&
              compare_add_identity_string(doc, item, "local_name_gen", edge->local_name_gen);
    if (ok) {
        yyjson_mut_doc_set_root(doc, item);
    }
    return compare_append_item(response, set, doc, ok ? item : NULL);
}

static bool compare_cancel_callback(void *context) {
    compare_response_t *response = (compare_response_t *)context;
    return mcp_request_cancelled(response->server);
}

static yyjson_mut_val *compare_project_json(yyjson_mut_doc *doc, const char *project,
                                            const cbm_graph_compare_project_t *metadata) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    if (!object || !yyjson_mut_obj_add_strcpy(doc, object, "project", project) ||
        !compare_add_identity_string(doc, object, "generation", metadata->generation) ||
        !compare_add_identity_string(doc, object, "index_mode", metadata->index_mode) ||
        !yyjson_mut_obj_add_sint(doc, object, "node_count", metadata->node_count) ||
        !yyjson_mut_obj_add_sint(doc, object, "edge_count", metadata->edge_count)) {
        return NULL;
    }
    return object;
}

static yyjson_mut_val *compare_set_json(yyjson_mut_doc *doc, compare_result_set_t *set,
                                        uint64_t total, size_t limit) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    yyjson_mut_val *reasons = yyjson_mut_arr(doc);
    bool truncated = total > (uint64_t)set->returned;
    if (!object || !reasons || !yyjson_mut_obj_add_val(doc, object, "items", set->items) ||
        !yyjson_mut_obj_add_uint(doc, object, "returned", set->returned) ||
        !yyjson_mut_obj_add_uint(doc, object, "total", total) ||
        !yyjson_mut_obj_add_bool(doc, object, "truncated", truncated)) {
        return NULL;
    }
    if (truncated && set->returned >= limit && !yyjson_mut_arr_add_strcpy(doc, reasons, "limit")) {
        return NULL;
    }
    if (truncated && set->budget_exhausted &&
        !yyjson_mut_arr_add_strcpy(doc, reasons, "encoded_byte_budget")) {
        return NULL;
    }
    if (!yyjson_mut_obj_add_val(doc, object, "truncation_reasons", reasons)) {
        return NULL;
    }
    return object;
}

static char *handle_compare_graphs(cbm_mcp_server_t *server, const char *args) {
    char *base_project = NULL;
    char *target_project = NULL;
    uint64_t limit = 0;
    uint64_t scan_limit = 0;
    const char *argument_error = NULL;
    if (!compare_parse_arguments(args, &base_project, &target_project, &limit, &scan_limit,
                                 &argument_error)) {
        return compare_graphs_error("invalid_arguments", argument_error);
    }
    if (mcp_request_cancelled(server)) {
        free(base_project);
        free(target_project);
        return compare_graphs_error("cancelled", "compare_graphs cancelled for this request");
    }

    cbm_store_t *base_store = compare_open_project_store(base_project);
    if (!base_store) {
        free(base_project);
        free(target_project);
        return compare_graphs_error("project_not_indexed", "base project is not indexed");
    }
    cbm_store_t *target_store = compare_open_project_store(target_project);
    if (!target_store) {
        cbm_store_close(base_store);
        free(base_project);
        free(target_project);
        return compare_graphs_error("project_not_indexed", "target project is not indexed");
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    compare_response_t response = {
        .server = server,
        .doc = doc,
        .limit = (size_t)limit,
        .nodes_added = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .nodes_removed = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .edges_added = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .edges_removed = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
    };
    if (!doc || !root || !response.nodes_added.items || !response.nodes_removed.items ||
        !response.edges_added.items || !response.edges_removed.items) {
        cbm_store_close(target_store);
        cbm_store_close(base_store);
        yyjson_mut_doc_free(doc);
        free(base_project);
        free(target_project);
        return compare_graphs_error("allocation_failed", "could not allocate comparison result");
    }
    yyjson_mut_doc_set_root(doc, root);

    cbm_graph_compare_result_t comparison = {0};
    int compare_rc = cbm_store_compare_graphs(
        base_store, base_project, target_store, target_project, scan_limit, compare_cancel_callback,
        compare_node_callback, compare_edge_callback, &response, &comparison);
    cbm_store_close(target_store);
    cbm_store_close(base_store);

    if (compare_rc != CBM_STORE_OK) {
        yyjson_mut_doc_free(doc);
        free(base_project);
        free(target_project);
        if (compare_rc == CBM_STORE_CANCELLED) {
            return compare_graphs_error("cancelled", "compare_graphs cancelled for this request");
        }
        if (compare_rc == CBM_STORE_NOT_FOUND) {
            return compare_graphs_error("project_not_indexed", "project is not indexed");
        }
        if (compare_rc == CBM_STORE_SCAN_LIMIT) {
            return compare_graphs_error("scan_limit_exceeded",
                                        "combined graph rows exceed scan_limit");
        }
        if (compare_rc == CBM_STORE_CALLBACK_ERR) {
            return compare_graphs_error("allocation_failed",
                                        "could not allocate comparison result");
        }
        return compare_graphs_error("query_failed", "graph comparison query failed");
    }

    yyjson_mut_val *base = compare_project_json(doc, base_project, &comparison.base);
    yyjson_mut_val *target = compare_project_json(doc, target_project, &comparison.target);
    yyjson_mut_val *nodes = yyjson_mut_obj(doc);
    yyjson_mut_val *edges = yyjson_mut_obj(doc);
    yyjson_mut_val *limits = yyjson_mut_obj(doc);
    yyjson_mut_val *nodes_added =
        compare_set_json(doc, &response.nodes_added, comparison.nodes_added_total, response.limit);
    yyjson_mut_val *nodes_removed = compare_set_json(
        doc, &response.nodes_removed, comparison.nodes_removed_total, response.limit);
    yyjson_mut_val *edges_added =
        compare_set_json(doc, &response.edges_added, comparison.edges_added_total, response.limit);
    yyjson_mut_val *edges_removed = compare_set_json(
        doc, &response.edges_removed, comparison.edges_removed_total, response.limit);
    bool built =
        base && target && nodes && edges && limits && nodes_added && nodes_removed && edges_added &&
        edges_removed && yyjson_mut_obj_add_int(doc, root, "schema_version", 1) &&
        yyjson_mut_obj_add_val(doc, root, "base", base) &&
        yyjson_mut_obj_add_val(doc, root, "target", target) &&
        yyjson_mut_obj_add_val(doc, nodes, "added", nodes_added) &&
        yyjson_mut_obj_add_val(doc, nodes, "removed", nodes_removed) &&
        yyjson_mut_obj_add_val(doc, root, "nodes", nodes) &&
        yyjson_mut_obj_add_val(doc, edges, "added", edges_added) &&
        yyjson_mut_obj_add_val(doc, edges, "removed", edges_removed) &&
        yyjson_mut_obj_add_val(doc, root, "edges", edges) &&
        yyjson_mut_obj_add_uint(doc, limits, "limit", limit) &&
        yyjson_mut_obj_add_uint(doc, limits, "scan_limit", scan_limit) &&
        yyjson_mut_obj_add_uint(doc, limits, "encoded_byte_budget", MCP_COMPARE_SET_BYTE_BUDGET) &&
        yyjson_mut_obj_add_val(doc, root, "limits", limits);
    free(base_project);
    free(target_project);
    if (!built) {
        yyjson_mut_doc_free(doc);
        return compare_graphs_error("allocation_failed", "could not allocate comparison result");
    }
    if (mcp_request_cancelled(server)) {
        yyjson_mut_doc_free(doc);
        return compare_graphs_error("cancelled", "compare_graphs cancelled for this request");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return compare_graphs_error("allocation_failed", "could not serialize comparison result");
    }
    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static bool sg_field_blocked(const char *f); /* internal-only fields, defined with search_graph */

typedef struct {
    bool node;
    int index;
    int count;
    const char *name;
} mcp_schema_record_t;

static int schema_record_compare(const void *left, const void *right) {
    const mcp_schema_record_t *a = left;
    const mcp_schema_record_t *b = right;
    if (a->node != b->node) {
        return a->node ? -1 : 1;
    }
    if (a->count != b->count) {
        return a->count > b->count ? -1 : 1;
    }
    return strcmp(a->name ? a->name : "", b->name ? b->name : "");
}

static char *handle_get_graph_schema(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        return not_indexed;
    }

    char *diagnostics = cbm_mcp_get_string_arg(args, "diagnostics");
    bool include_properties = diagnostics && strcmp(diagnostics, "full") == 0;
    free(diagnostics);

    cbm_schema_info_t schema = {0};
    if (include_properties) {
        cbm_store_get_schema(store, project, &schema);
    } else {
        cbm_store_get_schema_counts(store, project, &schema);
    }

    int limit = cbm_mcp_get_int_arg(args, "limit", 50);
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    if (limit < 1) {
        limit = 1;
    } else if (limit > 500) {
        limit = 500;
    }
    if (offset < 0) {
        offset = 0;
    }
    int total = schema.node_label_count + schema.edge_type_count;
    size_t record_capacity = total > 0 ? (size_t)total : 1U;
    mcp_schema_record_t *records = calloc(record_capacity, sizeof(*records));
    if (!records) {
        cbm_store_schema_free(&schema);
        free(project);
        return cbm_mcp_text_result("out of memory while rendering schema", true);
    }
    int record_count = 0;
    for (int i = 0; i < schema.node_label_count; i++) {
        records[record_count++] = (mcp_schema_record_t){true, i, schema.node_labels[i].count,
                                                        schema.node_labels[i].label};
    }
    for (int i = 0; i < schema.edge_type_count; i++) {
        records[record_count++] =
            (mcp_schema_record_t){false, i, schema.edge_types[i].count, schema.edge_types[i].type};
    }
    qsort(records, (size_t)record_count, sizeof(*records), schema_record_compare);
    int start = offset < total ? offset : total;
    int end = start + limit < total ? start + limit : total;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *labels = yyjson_mut_arr(doc);
    for (int page = start; page < end; page++) {
        if (!records[page].node) {
            continue;
        }
        int i = records[page].index;
        yyjson_mut_val *lbl = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, lbl, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, lbl, "count", schema.node_labels[i].count);
        if (include_properties) {
            yyjson_mut_val *props = yyjson_mut_arr(doc);
            for (int j = 0; j < schema.node_labels[i].property_count; j++) {
                /* Internal similarity intermediates are never public. */
                if (sg_field_blocked(schema.node_labels[i].properties[j])) {
                    continue;
                }
                yyjson_mut_arr_add_str(doc, props, schema.node_labels[i].properties[j]);
            }
            yyjson_mut_obj_add_val(doc, lbl, "properties", props);
        }
        yyjson_mut_arr_add_val(labels, lbl);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", labels);

    yyjson_mut_val *types = yyjson_mut_arr(doc);
    for (int page = start; page < end; page++) {
        if (records[page].node) {
            continue;
        }
        int i = records[page].index;
        yyjson_mut_val *typ = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, typ, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, typ, "count", schema.edge_types[i].count);
        if (include_properties) {
            yyjson_mut_val *eprops = yyjson_mut_arr(doc);
            for (int j = 0; j < schema.edge_types[i].property_count; j++) {
                yyjson_mut_arr_add_str(doc, eprops, schema.edge_types[i].properties[j]);
            }
            yyjson_mut_obj_add_val(doc, typ, "properties", eprops);
        }
        yyjson_mut_arr_add_val(types, typ);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", types);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_int(doc, root, "returned", end - start);
    yyjson_mut_obj_add_bool(doc, root, "has_more", end < total);
    if (end < total) {
        yyjson_mut_obj_add_int(doc, root, "next_offset", end);
    }

    /* Check ADR presence */
    cbm_project_t proj_info = {0};
    if (cbm_store_get_project(store, project, &proj_info) == 0 && proj_info.root_path) {
        bool adr_exists = project_has_adr(store, project, proj_info.root_path);
        yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
        if (!adr_exists && include_properties) {
            yyjson_mut_obj_add_str(
                doc, root, "adr_hint",
                "No ADR found. Use manage_adr(mode='update') to persist architectural "
                "decisions across sessions. Run get_architecture(aspects=['all']) first.");
        }
        cbm_project_free_fields(&proj_info);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(records);
    cbm_store_schema_free(&schema);
    free(project);

    char *result = mcp_result_from_json(args, json);
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
/* Build the connected-names array (1-hop callers + callees) for a node.
 * Returns a (possibly empty) yyjson array owned by doc. */
static yyjson_mut_val *enrich_connected(yyjson_mut_doc *doc, cbm_store_t *store, int64_t node_id,
                                        const char *relationship) {
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

    return conn;
}

/* Text-tree variant: the same 1-hop neighbor names joined without a fixed
 * staging buffer. Identifiers are semantic atoms: emit each in full or omit
 * the entire connected column at the response-budget layer. */
static char *enrich_connected_joined(cbm_store_t *store, int64_t node_id,
                                     const char *relationship) {
    const char *et[] = {relationship ? relationship : "CALLS"};
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    bool any = false;
    const char *dirs[2] = {"inbound", "outbound"};
    for (int d = 0; d < 2; d++) {
        cbm_traverse_result_t tr = {0};
        cbm_store_bfs(store, node_id, dirs[d], et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr);
        for (int j = 0; j < tr.visited_count; j++) {
            if (!tr.visited[j].node.name) {
                continue;
            }
            if (any) {
                cbm_sb_append(&sb, ";");
            }
            cbm_sb_append(&sb, tr.visited[j].node.name);
            any = true;
        }
        cbm_store_traverse_free(&tr);
    }
    return cbm_sb_finish(&sb);
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
    BM25_DEFAULT_LIMIT = 50,
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
    BM25_BIND_FILE = 6,
    BM25_SQL_AUTO_LEN = -1,
    /* Inner FTS5 candidate cap.  SQLite can early-terminate a plain FTS5 query
     * (no JOIN/WHERE on outer table) of the form:
     *   SELECT rowid, bm25() FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * By fetching only the top BM25_INNER_LIMIT candidates from the FTS5 index
     * and then joining/filtering/re-ranking those, we bound all work to O(N) where
     * N = BM25_INNER_LIMIT rather than the full match set size. */
    BM25_INNER_LIMIT = 2000,
};

/* Column weights for nodes_fts (name, qualified_name, label, file_path, body).
 * The four identifier columns stay at parity; prose sits well below them.
 * FTS5 applies these to per-column term frequency BEFORE the tf-saturation
 * term, which is what makes the weighting BM25F-correct rather than a post-hoc
 * rescale. 0.3 is the findability-favouring end of the field weighting the IR
 * literature settles on for body text (typical title:body ratios run 3:1 to
 * 10:1): a prose-only hit still surfaces, but never outranks a node whose
 * IDENTIFIER matches.
 *
 * Defined once and used by BOTH the ranked query and the count query — they
 * share an inner candidate window, so different weights would silently
 * desynchronise the reported total from the rows returned.
 *
 * Safe against a legacy four-column nodes_fts: FTS5's bm25() reads a weight
 * only when an instance actually lands in that column (`nVal > ic`), so the
 * fifth weight is simply never consulted on a table that has no fifth
 * column. */
#define BM25_WEIGHTS "bm25(nodes_fts, 1.0, 1.0, 1.0, 1.0, 0.3)"

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

static char *bm25_file_pattern_like(const char *file_pattern) {
    if (!file_pattern) {
        return NULL;
    }
    char *like = cbm_glob_to_like(file_pattern);
    if (like && !strchr(file_pattern, '*') && !strchr(file_pattern, '?')) {
        size_t len = strlen(like);
        char *contains = malloc(len + MCP_SEPARATOR + SKIP_ONE);
        if (contains) {
            contains[0] = '%';
            memcpy(contains + SKIP_ONE, like, len);
            contains[len + SKIP_ONE] = '%';
            contains[len + MCP_SEPARATOR] = '\0';
            free(like);
            like = contains;
        }
    }
    return like;
}

typedef struct {
    char *qualified_name;
    char *label;
    char *file_path;
    int start_line;
    int end_line;
    double rank;
} bm25_output_row_t;

static void bm25_output_rows_free(bm25_output_row_t *rows, int count) {
    if (!rows) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(rows[i].qualified_name);
        free(rows[i].label);
        free(rows[i].file_path);
    }
    free(rows);
}

static void bm25_lines_str(char *out, size_t size, int start, int end) {
    if (start > 0) {
        snprintf(out, size, "%d-%d", start, end > start ? end : start);
    } else {
        out[0] = '\0';
    }
}

static void bm25_emit_metadata_tree(cbm_sb_t *sb, int total, int offset, int returned,
                                    bool candidate_window_saturated, bool budget_hit,
                                    bool floor_exceeded, size_t max_output_bytes) {
    bool has_more = total > offset + returned;
    cbm_tree_scalar_int(sb, "total", total);
    cbm_tree_scalar_str(sb, "total_relation", candidate_window_saturated ? "gte" : "eq");
    if (candidate_window_saturated) {
        cbm_tree_scalar_bool(sb, "candidate_window_saturated", true);
    }
    cbm_tree_scalar_str(sb, "search_mode", "bm25");
    cbm_tree_scalar_int(sb, "returned", returned);
    cbm_tree_scalar_bool(sb, "has_more", has_more);
    if (has_more && returned > 0) {
        cbm_tree_scalar_int(sb, "next_offset", offset + returned);
    } else if (has_more) {
        cbm_tree_scalar_bool(sb, "continuation_requires_higher_budget", true);
    }
    bool truncated = has_more || candidate_window_saturated || budget_hit;
    cbm_tree_scalar_bool(sb, "truncated", truncated);
    if (truncated) {
        const char *reason = budget_hit ? "output_budget"
                             : has_more ? "page_limit"
                                        : "candidate_window";
        cbm_tree_scalar_str(sb, "truncation_reason", reason);
    }
    if (budget_hit) {
        cbm_tree_scalar_int(sb, "max_output_bytes", (long long)max_output_bytes);
    }
    if (floor_exceeded) {
        cbm_tree_scalar_bool(sb, "output_budget_floor_exceeded", true);
        cbm_tree_scalar_str(sb, "hint",
                            "first identity row exceeds max_output_bytes; raise "
                            "max_output_tokens");
    }
}

static void bm25_emit_metadata_json(yyjson_mut_doc *doc, yyjson_mut_val *root, int total,
                                    int offset, int returned, bool candidate_window_saturated,
                                    bool budget_hit, bool floor_exceeded, size_t max_output_bytes) {
    bool has_more = total > offset + returned;
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_str(doc, root, "total_relation", candidate_window_saturated ? "gte" : "eq");
    if (candidate_window_saturated) {
        yyjson_mut_obj_add_bool(doc, root, "candidate_window_saturated", true);
    }
    yyjson_mut_obj_add_str(doc, root, "search_mode", "bm25");
    yyjson_mut_obj_add_int(doc, root, "returned", returned);
    yyjson_mut_obj_add_int(doc, root, "count", returned);
    yyjson_mut_obj_add_bool(doc, root, "has_more", has_more);
    if (has_more && returned > 0) {
        yyjson_mut_obj_add_int(doc, root, "next_offset", offset + returned);
    } else if (has_more) {
        yyjson_mut_obj_add_bool(doc, root, "continuation_requires_higher_budget", true);
    }
    bool truncated = has_more || candidate_window_saturated || budget_hit;
    yyjson_mut_obj_add_bool(doc, root, "truncated", truncated);
    if (truncated) {
        const char *reason = budget_hit ? "output_budget"
                             : has_more ? "page_limit"
                                        : "candidate_window";
        yyjson_mut_obj_add_str(doc, root, "truncation_reason", reason);
    }
    if (budget_hit) {
        yyjson_mut_obj_add_uint(doc, root, "max_output_bytes", max_output_bytes);
    }
    if (floor_exceeded) {
        yyjson_mut_obj_add_bool(doc, root, "output_budget_floor_exceeded", true);
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "first identity row exceeds max_output_bytes; raise "
                               "max_output_tokens");
    }
}

static char *bm25_render(const bm25_output_row_t *rows, int returned, int total, int offset,
                         bool candidate_window_saturated, bool tree_format, bool budget_hit,
                         bool floor_exceeded, size_t max_output_bytes) {
    if (tree_format) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        static const char *const cols[] = {"qn", "label", "file", "lines", "rank"};
        cbm_tree_table_header(&sb, "results", returned, cols, 5);
        for (int i = 0; i < returned; i++) {
            char lines[CBM_SZ_32];
            bm25_lines_str(lines, sizeof(lines), rows[i].start_line, rows[i].end_line);
            cbm_tree_row_begin(&sb);
            cbm_tree_cell_str(&sb, rows[i].qualified_name, true);
            cbm_tree_cell_str(&sb, rows[i].label, false);
            cbm_tree_cell_str(&sb, rows[i].file_path, false);
            cbm_tree_cell_str(&sb, lines, false);
            cbm_tree_cell_real(&sb, rows[i].rank, false);
            cbm_tree_row_end(&sb);
        }
        bm25_emit_metadata_tree(&sb, total, offset, returned, candidate_window_saturated,
                                budget_hit, floor_exceeded, max_output_bytes);
        return cbm_sb_finish(&sb);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    static const char *const col_names[] = {"qn", "label", "file", "lines", "rank"};
    for (size_t i = 0; i < sizeof(col_names) / sizeof(col_names[0]); i++) {
        yyjson_mut_arr_add_str(doc, cols, col_names[i]);
    }
    yyjson_mut_obj_add_val(doc, root, "cols", cols);
    yyjson_mut_val *result_rows = yyjson_mut_arr(doc);
    for (int i = 0; i < returned; i++) {
        char lines[CBM_SZ_32];
        bm25_lines_str(lines, sizeof(lines), rows[i].start_line, rows[i].end_line);
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, rows[i].qualified_name);
        yyjson_mut_arr_add_strcpy(doc, row, rows[i].label);
        yyjson_mut_arr_add_strcpy(doc, row, rows[i].file_path);
        yyjson_mut_arr_add_strcpy(doc, row, lines);
        yyjson_mut_arr_add_real(doc, row, rows[i].rank);
        yyjson_mut_arr_add_val(result_rows, row);
    }
    yyjson_mut_obj_add_val(doc, root, "rows", result_rows);
    bm25_emit_metadata_json(doc, root, total, offset, returned, candidate_window_saturated,
                            budget_hit, floor_exceeded, max_output_bytes);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Run the BM25 full-text search path and return the JSON result string.
 * Returns NULL if FTS5 is unavailable or the query produced no usable tokens,
 * in which case the caller falls back to the regex-based search path. */
static char *bm25_search(cbm_store_t *store, const char *project, const char *query,
                         const char *file_pattern, int limit, int offset, bool tree_format,
                         size_t max_output_bytes) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return NULL;
    }
    char fts_query[BM25_QUERY_BUF];
    int tok_count = bm25_build_match(query, fts_query, sizeof(fts_query));
    if (tok_count == 0) {
        return NULL;
    }
    char *file_like = bm25_file_pattern_like(file_pattern);

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
     *   SELECT rowid, bm25(nodes_fts,...) FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * because no outer predicate blocks it.  We fetch BM25_INNER_LIMIT top candidates
     * from the FTS5 index, then join/filter/boost only those rows.  bm25() returns a
     * NEGATIVE score (lower = more relevant). */
    const char *sql =
        "SELECT n.id, n.label, n.name, n.qualified_name, n.file_path, n.start_line, n.end_line, "
        "       (fts.base_rank "
        "        - CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        "               WHEN n.label = 'Route' THEN 8.0 "
        "               WHEN n.label IN (" CBM_SQL_TYPE_LIKE_LABELS ") THEN 5.0 "
        /* Relations rank with the type tier: a table IS the schema container
         * a data question is looking for (findability-first). */
        "               WHEN n.label IN (" CBM_SQL_RELATION_LABELS ") THEN 5.0 "
        "               ELSE 0.0 END) AS rank "
        "FROM ("
        "    SELECT rowid, " BM25_WEIGHTS " AS base_rank"
        "    FROM nodes_fts WHERE nodes_fts MATCH ?1"
        "    ORDER BY base_rank, rowid LIMIT ?5"
        ") fts "
        "JOIN nodes n ON n.id = fts.rowid "
        "WHERE n.project = ?2 "
        /* Section and Module are NO LONGER excluded (#518/#519): they are the
         * labels that carry prose — a Markdown section's body, a config file's
         * description — so excluding them made the body column unreachable.
         * This exclusion list is MIRRORED in the count query below; the two
         * must be changed together or results desynchronise from counts. */
        "  AND n.label NOT IN ('File','Folder','Variable','Project') "
        "  AND (?6 IS NULL OR n.file_path LIKE ?6) "
        /* rank ties are common (boosted floats) — the id tie-break makes
         * offset pages contractually stable across calls. */
        "ORDER BY rank, n.id "
        "LIMIT ?3 OFFSET ?4";

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, BM25_SQL_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        free(file_like);
        return NULL;
    }
    sqlite3_bind_text(stmt, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, BM25_BIND_LIMIT, limit > 0 ? limit : BM25_DEFAULT_LIMIT);
    sqlite3_bind_int(stmt, BM25_BIND_OFFSET, offset > 0 ? offset : 0);
    sqlite3_bind_int(stmt, BM25_BIND_INNER, BM25_INNER_LIMIT);
    if (file_like) {
        sqlite3_bind_text(stmt, BM25_BIND_FILE, file_like, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, BM25_BIND_FILE);
    }

    /* Count hits within the same inner-limit window — capped at BM25_INNER_LIMIT.
     * Uses the identical subquery structure so the FTS5 early-exit applies here too. */
    int total = 0;
    {
        const char *count_sql = "SELECT COUNT(*) FROM ("
                                "    SELECT fts.rowid FROM ("
                                "        SELECT rowid FROM nodes_fts WHERE nodes_fts MATCH ?1"
                                "        ORDER BY " BM25_WEIGHTS " LIMIT ?3"
                                "    ) fts "
                                "    JOIN nodes n ON n.id = fts.rowid "
                                "    WHERE n.project = ?2 "
                                /* MIRRORS the ranked query's filter verbatim — same weights, same
                                 * label exclusions. Changing one alone reports a total that does
                                 * not describe the rows returned. */
                                "      AND n.label NOT IN ('File','Folder','Variable','Project')"
                                "      AND (?6 IS NULL OR n.file_path LIKE ?6)"
                                ")";
        sqlite3_stmt *cs = NULL;
        if (sqlite3_prepare_v2(db, count_sql, BM25_SQL_AUTO_LEN, &cs, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cs, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_text(cs, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_int(cs, BM25_BIND_LIMIT, BM25_INNER_LIMIT);
            if (file_like) {
                sqlite3_bind_text(cs, BM25_BIND_FILE, file_like, BM25_SQL_AUTO_LEN,
                                  MCP_SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(cs, BM25_BIND_FILE);
            }
            if (sqlite3_step(cs) == SQLITE_ROW) {
                total = sqlite3_column_int(cs, 0);
            }
            sqlite3_finalize(cs);
        }
    }

    /* The top-candidate window is a performance ceiling, not an exact-total
     * boundary. Probe one candidate beyond it so a broad query never presents
     * a window-local count as the complete match count. This is global to the
     * FTS table; saturation is therefore conservatively reported even when
     * later project/path filters might discard the hidden candidates. */
    bool candidate_window_saturated = true;
    {
        const char *probe_sql = "SELECT rowid FROM nodes_fts WHERE nodes_fts MATCH ?1 "
                                "ORDER BY bm25(nodes_fts), rowid LIMIT 1 OFFSET ?2";
        sqlite3_stmt *probe = NULL;
        if (sqlite3_prepare_v2(db, probe_sql, BM25_SQL_AUTO_LEN, &probe, NULL) == SQLITE_OK) {
            sqlite3_bind_text(probe, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_int(probe, BM25_BIND_PROJECT, BM25_INNER_LIMIT);
            int probe_step = sqlite3_step(probe);
            candidate_window_saturated = probe_step != SQLITE_DONE;
            sqlite3_finalize(probe);
        }
    }

    int row_cap = limit > 0 ? limit : BM25_DEFAULT_LIMIT;
    bm25_output_row_t *rows = calloc((size_t)row_cap, sizeof(*rows));
    int row_count = 0;
    while (rows && row_count < row_cap && sqlite3_step(stmt) == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(stmt, BM25_COL_QN);
        const char *label = (const char *)sqlite3_column_text(stmt, BM25_COL_LABEL);
        const char *file = (const char *)sqlite3_column_text(stmt, BM25_COL_FILE);
        rows[row_count].qualified_name = heap_strdup(qn ? qn : "");
        rows[row_count].label = heap_strdup(label ? label : "");
        rows[row_count].file_path = heap_strdup(file ? file : "");
        rows[row_count].start_line = sqlite3_column_int(stmt, BM25_COL_START);
        rows[row_count].end_line = sqlite3_column_int(stmt, BM25_COL_END);
        rows[row_count].rank = sqlite3_column_double(stmt, BM25_COL_RANK);
        row_count++;
    }
    sqlite3_finalize(stmt);
    free(file_like);

    if (!rows) {
        return heap_strdup("out of memory");
    }

    int returned = row_count;
    bool budget_hit = false;
    char *payload = NULL;
    for (;;) {
        payload = bm25_render(rows, returned, total, offset, candidate_window_saturated,
                              tree_format, budget_hit, false, max_output_bytes);
        if (!payload || strlen(payload) <= max_output_bytes || returned == 0) {
            break;
        }
        free(payload);
        payload = NULL;
        budget_hit = true;
        returned--;
    }

    if (!payload || strlen(payload) > max_output_bytes || (row_count > 0 && returned == 0)) {
        free(payload);
        payload = bm25_render(rows, 0, total, offset, candidate_window_saturated, tree_format, true,
                              row_count > 0, max_output_bytes);
    }
    bm25_output_rows_free(rows, row_count);
    return payload;
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

/* Emit vector-search hits in the json-tree model: "semantic": {cols, rows}
 * — score order preserved (ranked output is never regrouped). */
static void emit_semantic_results(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  const cbm_vector_result_t *vresults, int returned) {
    yyjson_mut_val *sem = yyjson_mut_obj(doc);
    yyjson_mut_val *scols = yyjson_mut_arr(doc);
    static const char *const sem_cols[] = {"qn", "label", "file", "score"};
    for (size_t ci = 0; ci < sizeof(sem_cols) / sizeof(sem_cols[0]); ci++) {
        yyjson_mut_arr_add_str(doc, scols, sem_cols[ci]);
    }
    yyjson_mut_obj_add_val(doc, sem, "cols", scols);
    yyjson_mut_val *sem_results = yyjson_mut_arr(doc);
    for (int v = 0; v < returned; v++) {
        yyjson_mut_val *vitem = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, vitem, vresults[v].qualified_name);
        yyjson_mut_arr_add_strcpy(doc, vitem, vresults[v].label);
        yyjson_mut_arr_add_strcpy(doc, vitem, vresults[v].file_path);
        yyjson_mut_arr_add_real(doc, vitem, vresults[v].score);
        yyjson_mut_arr_add_val(sem_results, vitem);
    }
    yyjson_mut_obj_add_val(doc, sem, "rows", sem_results);
    yyjson_mut_obj_add_val(doc, root, "semantic", sem);
}

/* Run the semantic_query vector search from raw args. Sets *out_vresults /
 * *out_vcount (caller frees via cbm_store_free_vector_results when vcount>0).
 * Returns true if semantic_query was provided as a non-array (type error —
 * caller should surface to the user). */
static bool run_semantic_query_core(const char *args, cbm_store_t *store, const char *project,
                                    int materialize_limit, cbm_vector_result_t **out_vresults,
                                    int *out_vcount, bool *out_present) {
    enum { MAX_KW_SEARCH = 32 };
    *out_vresults = NULL;
    *out_vcount = 0;
    if (out_present) {
        *out_present = false;
    }
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *sq_val = args_root ? yyjson_obj_get(args_root, "semantic_query") : NULL;
    if (out_present && sq_val) {
        *out_present = true;
    }
    bool type_error = false;
    if (sq_val && !yyjson_is_arr(sq_val)) {
        type_error = true;
    } else if (sq_val && yyjson_arr_size(sq_val) > 0) {
        const char *keywords[MAX_KW_SEARCH];
        int ki = extract_semantic_keywords(sq_val, keywords, MAX_KW_SEARCH);
        cbm_vector_result_t *vresults = NULL;
        int vcount = 0;
        if (cbm_store_vector_search(store, project, keywords, ki, materialize_limit, &vresults,
                                    &vcount) == CBM_STORE_OK &&
            vcount > 0) {
            *out_vresults = vresults;
            *out_vcount = vcount;
        }
    }
    if (args_doc) {
        yyjson_doc_free(args_doc);
    }
    return type_error;
}

static bool search_graph_arg_present(const char *args, const char *name) {
    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    bool present = root && yyjson_is_obj(root) && yyjson_obj_get(root, name) != NULL;
    if (doc) {
        yyjson_doc_free(doc);
    }
    return present;
}

/* ── Tree output for search_graph ───────────────────────────────────
 * Default response encoding chooses the smaller complete direct/grouped tree
 * (compact_out.h). format:"json" keeps a stable grouped model;
 * include_connected adds a `connected` column in both encodings. */

enum { SG_MAX_EXTRA_FIELDS = 12 };

/* Internal-only node properties never emitted to agents: similarity /
 * semantic pipeline intermediates (minhash fingerprint, structural profile,
 * body-token bag). They dominate payload size and carry zero agent value. */
static bool sg_field_blocked(const char *f) {
    return strcmp(f, "fp") == 0 || strcmp(f, "sp") == 0 || strcmp(f, "bt") == 0;
}

/* Core row columns every search result already carries. Requesting one as an
 * extra `fields` entry used to emit a silent empty column (core values are
 * node columns, never in the properties JSON) — field-eval agents burned a
 * round-trip on exactly that. Drop them and teach instead. */
static bool sg_field_is_core(const char *f) {
    return strcmp(f, "qn") == 0 || strcmp(f, "qualified_name") == 0 || strcmp(f, "name") == 0 ||
           strcmp(f, "label") == 0 || strcmp(f, "file") == 0 || strcmp(f, "file_path") == 0 ||
           strcmp(f, "path") == 0 || strcmp(f, "lines") == 0 || strcmp(f, "in") == 0 ||
           strcmp(f, "out") == 0;
}

/* Parse the `fields` argument (array of property names) into out[] as
 * pointers owned by the returned doc (caller frees the doc after emission).
 * Blocked internal fields are silently dropped; core-column requests are
 * dropped too and reported via *core_requested so the emitter can hint. */
static int sg_parse_fields(const char *args, const char *out[], int max_out, yyjson_doc **out_owner,
                           bool *core_requested) {
    *out_owner = NULL;
    if (core_requested) {
        *core_requested = false;
    }
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *fv = args_root ? yyjson_obj_get(args_root, "fields") : NULL;
    if (!fv || !yyjson_is_arr(fv)) {
        if (args_doc) {
            yyjson_doc_free(args_doc);
        }
        return 0;
    }
    int n = 0;
    size_t idx = 0;
    size_t max = 0;
    yyjson_val *item;
    yyjson_arr_foreach(fv, idx, max, item) {
        const char *s = yyjson_get_str(item);
        if (!s || !s[0] || sg_field_blocked(s)) {
            continue;
        }
        if (sg_field_is_core(s)) {
            if (core_requested) {
                *core_requested = true;
            }
            continue;
        }
        if (n < max_out) {
            out[n++] = s;
        }
    }
    if (n == 0) {
        yyjson_doc_free(args_doc);
        return 0;
    }
    *out_owner = args_doc;
    return n;
}

/* Append a property as one compact-output cell. Compound values stay one
 * column by using their compact JSON representation; the cell emitter quotes
 * and escapes that representation as needed. */
static void sg_toon_property_cell(cbm_sb_t *sb, yyjson_val *v) {
    if (v && yyjson_is_str(v)) {
        cbm_tree_cell_str(sb, yyjson_get_str(v), false);
    } else if (v && yyjson_is_bool(v)) {
        cbm_tree_cell_bool(sb, yyjson_get_bool(v), false);
    } else if (v && yyjson_is_int(v)) {
        cbm_tree_cell_int(sb, yyjson_get_int(v), false);
    } else if (v && yyjson_is_real(v)) {
        cbm_tree_cell_real(sb, yyjson_get_real(v), false);
    } else if (v && !yyjson_is_null(v)) {
        char *json = yyjson_val_write(v, 0, NULL);
        cbm_tree_cell_str(sb, json ? json : "", false);
        free(json);
    } else {
        cbm_tree_cell_str(sb, "", false);
    }
}

/* "start-end" line range, or empty when the node carries no line info. */
static void sg_lines_str(char *out, size_t sz, int start, int end) {
    if (start > 0) {
        snprintf(out, sz, "%d-%d", start, end > start ? end : start);
    } else {
        out[0] = '\0';
    }
}

/* ── Tree format (Phase-2 A/B candidate) ────────────────────────────
 * Prefix-factored, file-grouped output: the shared (qn-prefix, file) pair is
 * printed ONCE per group, rows beneath carry only the short name + data
 * cells. The reconstruction rule accounts for dotless qualified names and is
 * stated once in the header so agents can copy exact join keys into follow-up
 * calls. Research basis: HDT front-coding (prefix factoring), LocAgent tree
 * ablation (tree > flat/DOT for LLM comprehension), Lost-in-Distance
 * (related rows adjacent — grouping by module does exactly that). */

/* qn-prefix = qualified_name minus its last '.'-segment. Returns length. */
static size_t sg_qn_prefix_len(const char *qn) {
    const char *last = qn ? strrchr(qn, '.') : NULL;
    return last ? (size_t)(last - qn) : 0;
}

typedef struct {
    int returned;
    int fields_returned;
    int semantic_returned;
    bool connected_returned;
    bool core_hint_returned;
    bool budget_hit;
    bool floor_exceeded;
} sg_render_plan_t;

typedef struct {
    const cbm_vector_result_t *results;
    int total;
    int offset;
    int limit;
    bool total_exact;
    bool present;
} sg_semantic_page_t;

static bool sg_page_has_more(int total, int offset, int returned) {
    return (long long)total > (long long)offset + returned;
}

static void sg_emit_paging_tree(cbm_sb_t *sb, int total, int offset, const sg_render_plan_t *plan,
                                int requested_fields, bool connected_requested,
                                size_t max_output_bytes, bool continuation_supported,
                                bool continuation_requires_positive_limit) {
    bool rows_remain = sg_page_has_more(total, offset, plan->returned);
    bool has_more = continuation_supported && rows_remain;
    bool engine_saturated = !continuation_supported && rows_remain;
    cbm_tree_scalar_int(sb, "total", total);
    cbm_tree_scalar_int(sb, "returned", plan->returned);
    cbm_tree_scalar_bool(sb, "has_more", has_more);
    if (has_more && plan->returned > 0) {
        cbm_tree_scalar_int(sb, "next_offset", offset + plan->returned);
    } else if (has_more) {
        cbm_tree_scalar_bool(sb,
                             continuation_requires_positive_limit
                                 ? "continuation_requires_positive_limit"
                                 : "continuation_requires_higher_budget",
                             true);
    }
    bool truncated = rows_remain || plan->budget_hit;
    cbm_tree_scalar_bool(sb, "truncated", truncated);
    if (truncated) {
        cbm_tree_scalar_str(sb, "truncation_reason",
                            plan->budget_hit   ? "output_budget"
                            : engine_saturated ? "engine_limit"
                                               : "page_limit");
    }
    if (plan->budget_hit) {
        cbm_tree_scalar_int(sb, "max_output_bytes", (long long)max_output_bytes);
    }
    if (requested_fields > plan->fields_returned) {
        cbm_tree_scalar_int(sb, "fields_omitted", requested_fields - plan->fields_returned);
    }
    if (connected_requested && !plan->connected_returned) {
        cbm_tree_scalar_bool(sb, "connected_omitted", true);
    }
    if (plan->floor_exceeded) {
        cbm_tree_scalar_bool(sb, "output_budget_floor_exceeded", true);
        cbm_tree_scalar_str(sb, "hint",
                            "first identity row exceeds max_output_bytes; raise "
                            "max_output_tokens");
    }
}

static void sg_emit_paging_json(yyjson_mut_doc *doc, yyjson_mut_val *root, int total, int offset,
                                const sg_render_plan_t *plan, int requested_fields,
                                bool connected_requested, size_t max_output_bytes,
                                bool continuation_supported,
                                bool continuation_requires_positive_limit) {
    bool rows_remain = sg_page_has_more(total, offset, plan->returned);
    bool has_more = continuation_supported && rows_remain;
    bool engine_saturated = !continuation_supported && rows_remain;
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_int(doc, root, "returned", plan->returned);
    yyjson_mut_obj_add_int(doc, root, "count", plan->returned);
    yyjson_mut_obj_add_bool(doc, root, "has_more", has_more);
    if (has_more && plan->returned > 0) {
        yyjson_mut_obj_add_int(doc, root, "next_offset", offset + plan->returned);
    } else if (has_more) {
        yyjson_mut_obj_add_bool(doc, root,
                                continuation_requires_positive_limit
                                    ? "continuation_requires_positive_limit"
                                    : "continuation_requires_higher_budget",
                                true);
    }
    bool truncated = rows_remain || plan->budget_hit;
    yyjson_mut_obj_add_bool(doc, root, "truncated", truncated);
    if (truncated) {
        yyjson_mut_obj_add_str(doc, root, "truncation_reason",
                               plan->budget_hit   ? "output_budget"
                               : engine_saturated ? "engine_limit"
                                                  : "page_limit");
    }
    if (plan->budget_hit) {
        yyjson_mut_obj_add_uint(doc, root, "max_output_bytes", max_output_bytes);
    }
    if (requested_fields > plan->fields_returned) {
        yyjson_mut_obj_add_int(doc, root, "fields_omitted",
                               requested_fields - plan->fields_returned);
    }
    if (connected_requested && !plan->connected_returned) {
        yyjson_mut_obj_add_bool(doc, root, "connected_omitted", true);
    }
    if (plan->floor_exceeded) {
        yyjson_mut_obj_add_bool(doc, root, "output_budget_floor_exceeded", true);
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "first identity row exceeds max_output_bytes; raise "
                               "max_output_tokens");
    }
}

static bool sg_semantic_has_remaining_rows(const sg_semantic_page_t *semantic, int returned) {
    return semantic && sg_page_has_more(semantic->total, semantic->offset, returned);
}

/* semantic_offset is deliberately bounded: vector ranking certifies an
 * ordered prefix, so an unbounded offset would turn one tiny MCP request into
 * unbounded materialization. A continuation is usable only when the next
 * offset is accepted by the public schema/runtime. */
static bool sg_semantic_engine_saturated(const sg_semantic_page_t *semantic, int returned) {
    return sg_semantic_has_remaining_rows(semantic, returned) && returned > 0 &&
           (long long)semantic->offset + returned > MCP_QUERY_MAX_VISIBLE_ROWS;
}

static bool sg_semantic_has_more(const sg_semantic_page_t *semantic, int returned) {
    return sg_semantic_has_remaining_rows(semantic, returned) &&
           !sg_semantic_engine_saturated(semantic, returned);
}

static void sg_emit_semantic_paging_tree(cbm_sb_t *sb, const sg_semantic_page_t *semantic,
                                         int returned) {
    bool saturated = sg_semantic_engine_saturated(semantic, returned);
    bool has_more = sg_semantic_has_more(semantic, returned);
    cbm_tree_scalar_int(sb, "semantic_total", semantic->total);
    cbm_tree_scalar_str(sb, "semantic_total_relation",
                        semantic->total_exact && !saturated ? "eq" : "gte");
    cbm_tree_scalar_int(sb, "semantic_returned", returned);
    cbm_tree_scalar_bool(sb, "semantic_has_more", has_more);
    if (has_more && returned > 0) {
        cbm_tree_scalar_int(sb, "semantic_next_offset", semantic->offset + returned);
    } else if (has_more) {
        cbm_tree_scalar_bool(sb,
                             semantic->limit == 0 ? "semantic_continuation_requires_positive_limit"
                                                  : "semantic_continuation_requires_higher_budget",
                             true);
    }
    if (saturated) {
        cbm_tree_scalar_bool(sb, "semantic_engine_saturated", true);
        cbm_tree_scalar_bool(sb, "semantic_continuation_unavailable", true);
    }
}

static void sg_emit_semantic_paging_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                         const sg_semantic_page_t *semantic, int returned) {
    bool saturated = sg_semantic_engine_saturated(semantic, returned);
    bool has_more = sg_semantic_has_more(semantic, returned);
    yyjson_mut_obj_add_int(doc, root, "semantic_total", semantic->total);
    yyjson_mut_obj_add_str(doc, root, "semantic_total_relation",
                           semantic->total_exact && !saturated ? "eq" : "gte");
    yyjson_mut_obj_add_int(doc, root, "semantic_returned", returned);
    yyjson_mut_obj_add_bool(doc, root, "semantic_has_more", has_more);
    if (has_more && returned > 0) {
        yyjson_mut_obj_add_int(doc, root, "semantic_next_offset", semantic->offset + returned);
    } else if (has_more) {
        yyjson_mut_obj_add_bool(doc, root,
                                semantic->limit == 0
                                    ? "semantic_continuation_requires_positive_limit"
                                    : "semantic_continuation_requires_higher_budget",
                                true);
    }
    if (saturated) {
        yyjson_mut_obj_add_bool(doc, root, "semantic_engine_saturated", true);
        yyjson_mut_obj_add_bool(doc, root, "semantic_continuation_unavailable", true);
    }
}

#ifdef CBM_ENABLE_TEST_SEAMS
char *cbm_mcp_render_semantic_paging_for_testing(int total, int offset, int returned, int limit,
                                                 bool total_exact, bool json_format) {
    sg_semantic_page_t semantic = {
        .total = total,
        .offset = offset,
        .limit = limit,
        .total_exact = total_exact,
        .present = true,
    };
    if (!json_format) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        sg_emit_semantic_paging_tree(&sb, &semantic, returned);
        sg_render_plan_t plan = {.returned = returned, .semantic_returned = returned};
        sg_emit_paging_tree(&sb, total, offset, &plan, 0, false, SIZE_MAX,
                            !sg_semantic_engine_saturated(&semantic, returned), false);
        return cbm_sb_finish(&sb);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    sg_emit_semantic_paging_json(doc, root, &semantic, returned);
    sg_render_plan_t plan = {.returned = returned, .semantic_returned = returned};
    sg_emit_paging_json(doc, root, total, offset, &plan, 0, false, SIZE_MAX,
                        !sg_semantic_engine_saturated(&semantic, returned), false);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}
#endif

static void emit_search_results_ids_tree(cbm_sb_t *sb, const cbm_search_output_t *out,
                                         int returned) {
    static const char *const id_cols[] = {"qn"};
    cbm_tree_table_header(sb, "results", returned, id_cols, 1);
    for (int i = 0; i < returned; i++) {
        cbm_tree_row_begin(sb);
        cbm_tree_cell_str(sb, out->results[i].node.qualified_name, true);
        cbm_tree_row_end(sb);
    }
}

static void emit_search_results_ids_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                         const cbm_search_output_t *out, int returned) {
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_str(doc, cols, "qn");
    yyjson_mut_obj_add_val(doc, root, "cols", cols);
    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    for (int i = 0; i < returned; i++) {
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(
            doc, row,
            out->results[i].node.qualified_name ? out->results[i].node.qualified_name : "");
        yyjson_mut_arr_add_val(rows, row);
    }
    yyjson_mut_obj_add_val(doc, root, "rows", rows);
}

static void emit_search_results_grouped_tree(cbm_sb_t *sb, const cbm_search_output_t *out,
                                             const char *const *fields, int nfields,
                                             cbm_store_t *store, const char *relationship,
                                             bool include_connected, int returned) {
    cbm_sb_append(sb, "results: ");
    char count_buf[CBM_SZ_32];
    snprintf(count_buf, sizeof(count_buf), "%d", returned);
    cbm_sb_append(sb, count_buf);
    cbm_sb_append(sb, "  (rows: name label lines in out");
    for (int f = 0; f < nfields; f++) {
        cbm_sb_append(sb, " ");
        cbm_sb_append(sb, fields[f]);
    }
    if (include_connected) {
        cbm_sb_append(sb, " connected");
    }
    cbm_sb_append(sb, "; group prefix \"-\" means empty; "
                      "qn = prefix empty ? name : prefix + \".\" + name)\n");

    char *previous_prefix = NULL;
    char *previous_file = NULL;
    for (int i = 0; i < returned; i++) {
        const cbm_search_result_t *sr = &out->results[i];
        const char *qn = sr->node.qualified_name ? sr->node.qualified_name : "";
        const char *file = sr->node.file_path ? sr->node.file_path : "";
        size_t plen = sg_qn_prefix_len(qn);
        bool same_group = previous_prefix && strlen(previous_prefix) == plen &&
                          memcmp(previous_prefix, qn, plen) == 0 && previous_file &&
                          strcmp(previous_file, file) == 0;
        if (!same_group) {
            free(previous_prefix);
            free(previous_file);
            previous_prefix = cbm_strndup(qn, plen);
            previous_file = heap_strdup(file);
            cbm_tree_cell_str(sb, previous_prefix ? previous_prefix : "", true);
            cbm_sb_append(sb, " (");
            cbm_tree_cell_str(sb, previous_file ? previous_file : "", true);
            cbm_sb_append(sb, "):\n");
        }
        const char *shortname = plen ? qn + plen + 1 : qn;
        char lines[CBM_SZ_32];
        sg_lines_str(lines, sizeof(lines), sr->node.start_line, sr->node.end_line);
        cbm_sb_append(sb, "  ");
        cbm_tree_cell_str(sb, shortname, true);
        cbm_tree_cell_str(sb, sr->node.label ? sr->node.label : "", false);
        cbm_tree_cell_str(sb, lines, false);
        cbm_tree_cell_int(sb, sr->in_degree, false);
        cbm_tree_cell_int(sb, sr->out_degree, false);
        /* Extra property columns (fields param). Routed through the shared
         * cell emitters so values with spaces (signatures, docstrings) are
         * QUOTED — a raw append would shift every following column. Missing
         * values emit as "-" (the emitter's empty-cell placeholder). */
        if (nfields > 0) {
            yyjson_doc *pd =
                (sr->node.properties_json && sr->node.properties_json[0])
                    ? yyjson_read(sr->node.properties_json, strlen(sr->node.properties_json), 0)
                    : NULL;
            yyjson_val *pr = pd ? yyjson_doc_get_root(pd) : NULL;
            for (int f = 0; f < nfields; f++) {
                yyjson_val *v = (pr && yyjson_is_obj(pr)) ? yyjson_obj_get(pr, fields[f]) : NULL;
                sg_toon_property_cell(sb, v);
            }
            if (pd) {
                yyjson_doc_free(pd);
            }
        }
        if (include_connected && sr->node.id > 0) {
            char *joined = enrich_connected_joined(store, sr->node.id, relationship);
            cbm_tree_cell_str(sb, joined ? joined : "", false); /* empty emits "-" */
            free(joined);
        }
        cbm_sb_append(sb, "\n");
    }
    free(previous_prefix);
    free(previous_file);
}

/* The grouped search shape is excellent when rows share a module/file, but its
 * reconstruction rule and group headers are pure overhead for a singleton or
 * a scatter of unrelated rows. The common, no-extra-fields path can be
 * represented as a regular table, so render both byte-for-byte and keep the
 * smaller lossless form. Extra property/connected columns retain the grouped
 * emitter because their cells are dynamically typed. */
static char *render_search_results_flat_tree(const cbm_search_output_t *out, int returned) {
    static const char *const cols[] = {"qn", "label", "file", "lines", "in", "out"};
    static const bool string_cols[] = {true, true, true, true, false, false};
    static const bool prefix_cols[] = {true, false, true, false, false, false};
    enum { COLS = 6, CELL_TEXTS = 3 };
    if (returned == 0) {
        cbm_sb_t empty;
        cbm_sb_init(&empty);
        cbm_tree_table_header(&empty, "results", 0, cols, COLS);
        return cbm_sb_finish(&empty);
    }
    if (returned < 0 || (size_t)returned > (size_t)-1 / (COLS * sizeof(char *)) ||
        (size_t)returned > (size_t)-1 / (CELL_TEXTS * CBM_SZ_32)) {
        return NULL;
    }
    const char **cells = calloc((size_t)returned * COLS, sizeof(*cells));
    char *text = calloc((size_t)returned * CELL_TEXTS, CBM_SZ_32);
    if (!cells || !text) {
        free(cells);
        free(text);
        return NULL;
    }
    for (int i = 0; i < returned; i++) {
        const cbm_search_result_t *sr = &out->results[i];
        char *lines = text + (size_t)(i * CELL_TEXTS) * CBM_SZ_32;
        char *in_degree = lines + CBM_SZ_32;
        char *out_degree = in_degree + CBM_SZ_32;
        sg_lines_str(lines, CBM_SZ_32, sr->node.start_line, sr->node.end_line);
        snprintf(in_degree, CBM_SZ_32, "%d", sr->in_degree);
        snprintf(out_degree, CBM_SZ_32, "%d", sr->out_degree);
        cells[(size_t)i * COLS] = sr->node.qualified_name ? sr->node.qualified_name : "";
        cells[(size_t)i * COLS + 1U] = sr->node.label ? sr->node.label : "";
        cells[(size_t)i * COLS + 2U] = sr->node.file_path ? sr->node.file_path : "";
        cells[(size_t)i * COLS + 3U] = lines;
        cells[(size_t)i * COLS + 4U] = in_degree;
        cells[(size_t)i * COLS + 5U] = out_degree;
    }
    cbm_sb_t flat;
    cbm_sb_init(&flat);
    cbm_tree_table_rows_profiled(&flat, "results", returned, cols, COLS, cells, string_cols,
                                 prefix_cols);
    char *rendered = cbm_sb_finish(&flat);
    free(cells);
    free(text);
    return rendered;
}

static void emit_search_results_tree(cbm_sb_t *sb, const cbm_search_output_t *out,
                                     const char *const *fields, int nfields, cbm_store_t *store,
                                     const char *relationship, bool include_connected,
                                     int returned) {
    cbm_sb_t grouped;
    cbm_sb_init(&grouped);
    emit_search_results_grouped_tree(&grouped, out, fields, nfields, store, relationship,
                                     include_connected, returned);
    char *grouped_text = cbm_sb_finish(&grouped);
    char *flat_text =
        nfields == 0 && !include_connected ? render_search_results_flat_tree(out, returned) : NULL;
    if (flat_text && (!grouped_text || strlen(flat_text) < strlen(grouped_text))) {
        cbm_sb_append(sb, flat_text);
    } else if (grouped_text) {
        cbm_sb_append(sb, grouped_text);
    }
    free(flat_text);
    free(grouped_text);
}

/* json-stringified tree: the SAME grouped model as the text tree, serialized
 * as JSON for agents that need structured parsing — groups with a shared
 * (qn_prefix, file) and column-ordered row ARRAYS (never per-row key
 * envelopes; that legacy shape was 84% key overhead). */
static void emit_search_results_tree_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                          const cbm_search_output_t *out, const char *const *fields,
                                          int nfields, cbm_store_t *store, const char *relationship,
                                          bool include_connected, int returned) {
    yyjson_mut_obj_add_str(doc, root, "qn_rule",
                           "qn = qn_prefix == \"\" ? name : qn_prefix + \".\" + name");
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    static const char *const col_names[] = {"name", "label", "lines", "in", "out"};
    for (size_t i = 0; i < sizeof(col_names) / sizeof(col_names[0]); i++) {
        yyjson_mut_arr_add_str(doc, cols, col_names[i]);
    }
    for (int f = 0; f < nfields; f++) {
        yyjson_mut_arr_add_strcpy(doc, cols, fields[f]);
    }
    if (include_connected) {
        yyjson_mut_arr_add_str(doc, cols, "connected");
    }
    yyjson_mut_obj_add_val(doc, root, "cols", cols);
    yyjson_mut_val *groups = yyjson_mut_arr(doc);
    yyjson_mut_val *cur = NULL;
    yyjson_mut_val *cur_rows = NULL;
    char *previous_prefix = NULL;
    char *previous_file = NULL;
    for (int i = 0; i < returned; i++) {
        const cbm_search_result_t *sr = &out->results[i];
        const char *qn = sr->node.qualified_name ? sr->node.qualified_name : "";
        const char *file = sr->node.file_path ? sr->node.file_path : "";
        size_t plen = sg_qn_prefix_len(qn);
        bool same_group = previous_prefix && strlen(previous_prefix) == plen &&
                          memcmp(previous_prefix, qn, plen) == 0 && previous_file &&
                          strcmp(previous_file, file) == 0;
        if (!same_group) {
            free(previous_prefix);
            free(previous_file);
            previous_prefix = cbm_strndup(qn, plen);
            previous_file = heap_strdup(file);
            cur = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strncpy(doc, cur, "qn_prefix", qn, plen);
            yyjson_mut_obj_add_strcpy(doc, cur, "file", file);
            cur_rows = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, cur, "rows", cur_rows);
            yyjson_mut_arr_add_val(groups, cur);
        }
        char lines[CBM_SZ_32];
        sg_lines_str(lines, sizeof(lines), sr->node.start_line, sr->node.end_line);
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, plen ? qn + plen + 1 : qn);
        yyjson_mut_arr_add_strcpy(doc, row, sr->node.label ? sr->node.label : "");
        yyjson_mut_arr_add_strcpy(doc, row, lines);
        yyjson_mut_arr_add_int(doc, row, sr->in_degree);
        yyjson_mut_arr_add_int(doc, row, sr->out_degree);
        if (nfields > 0) {
            yyjson_doc *pd =
                (sr->node.properties_json && sr->node.properties_json[0])
                    ? yyjson_read(sr->node.properties_json, strlen(sr->node.properties_json), 0)
                    : NULL;
            yyjson_val *pr = pd ? yyjson_doc_get_root(pd) : NULL;
            for (int f = 0; f < nfields; f++) {
                yyjson_val *v = (pr && yyjson_is_obj(pr)) ? yyjson_obj_get(pr, fields[f]) : NULL;
                yyjson_mut_val *copy =
                    (v && !yyjson_is_null(v)) ? yyjson_val_mut_copy(doc, v) : NULL;
                if (copy) {
                    yyjson_mut_arr_add_val(row, copy);
                } else {
                    yyjson_mut_arr_add_null(doc, row);
                }
            }
            if (pd) {
                yyjson_doc_free(pd);
            }
        }
        if (include_connected && sr->node.id > 0) {
            yyjson_mut_arr_add_val(row, enrich_connected(doc, store, sr->node.id, relationship));
        }
        yyjson_mut_arr_add_val(cur_rows, row);
    }
    free(previous_prefix);
    free(previous_file);
    yyjson_mut_obj_add_val(doc, root, "groups", groups);
}

/* Emit semantic vector-search results as a TOON table. */
static void emit_semantic_results_toon(cbm_sb_t *sb, const cbm_vector_result_t *vresults,
                                       int returned) {
    static const char *const cols[] = {"qn", "label", "file", "score"};
    cbm_tree_table_header(sb, "semantic", returned, cols, 4);
    for (int v = 0; v < returned; v++) {
        cbm_tree_row_begin(sb);
        cbm_tree_cell_str(sb, vresults[v].qualified_name, true);
        cbm_tree_cell_str(sb, vresults[v].label, false);
        cbm_tree_cell_str(sb, vresults[v].file_path, false);
        cbm_tree_cell_real(sb, vresults[v].score, false);
        cbm_tree_row_end(sb);
    }
}

static char *sg_render_payload(bool json_format, const cbm_search_output_t *out, int offset,
                               const char *const *fields, int requested_fields, cbm_store_t *store,
                               const char *relationship, bool connected_requested, bool detail_ids,
                               const sg_semantic_page_t *semantic, bool semantic_only,
                               const char *diagnostic_hint, const sg_render_plan_t *plan,
                               size_t max_output_bytes) {
    sg_render_plan_t meta_plan = *plan;
    int page_total = semantic_only ? semantic->total : out->total;
    int page_offset = semantic_only ? semantic->offset : offset;
    if (semantic_only) {
        meta_plan.returned = plan->semantic_returned;
    }
    bool continuation_supported =
        !semantic_only || !sg_semantic_engine_saturated(semantic, plan->semantic_returned);

    if (!json_format) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        if (!semantic_only) {
            if (detail_ids) {
                emit_search_results_ids_tree(&sb, out, plan->returned);
            } else {
                emit_search_results_tree(&sb, out, fields, plan->fields_returned, store,
                                         relationship, plan->connected_returned, plan->returned);
            }
        }
        if (semantic->present) {
            sg_emit_semantic_paging_tree(&sb, semantic, plan->semantic_returned);
            emit_semantic_results_toon(&sb, semantic->results, plan->semantic_returned);
        }
        if (plan->core_hint_returned && diagnostic_hint) {
            cbm_tree_scalar_str(&sb, "hint", diagnostic_hint);
        }
        sg_emit_paging_tree(&sb, page_total, page_offset, &meta_plan, requested_fields,
                            connected_requested, max_output_bytes, continuation_supported,
                            semantic_only && semantic->limit == 0);
        return cbm_sb_finish(&sb);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    if (!semantic_only) {
        if (detail_ids) {
            emit_search_results_ids_json(doc, root, out, plan->returned);
        } else {
            emit_search_results_tree_json(doc, root, out, fields, plan->fields_returned, store,
                                          relationship, plan->connected_returned, plan->returned);
        }
    } else {
        /* The structured shape stays stable for semantic-only calls: an empty
         * groups array rather than an absent structural section. */
        yyjson_mut_obj_add_val(doc, root, "groups", yyjson_mut_arr(doc));
    }
    if (semantic->present) {
        sg_emit_semantic_paging_json(doc, root, semantic, plan->semantic_returned);
        emit_semantic_results(doc, root, semantic->results, plan->semantic_returned);
    }
    if (plan->core_hint_returned && diagnostic_hint) {
        yyjson_mut_obj_add_strcpy(doc, root, "hint", diagnostic_hint);
    }
    sg_emit_paging_json(doc, root, page_total, page_offset, &meta_plan, requested_fields,
                        connected_requested, max_output_bytes, continuation_supported,
                        semantic_only && semantic->limit == 0);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

static char *sg_budget_floor(bool json_format, int total, int offset,
                             const sg_semantic_page_t *semantic, size_t max_output_bytes,
                             bool semantic_only) {
    sg_render_plan_t floor = {
        .returned = 0,
        .fields_returned = 0,
        .semantic_returned = 0,
        .connected_returned = false,
        .core_hint_returned = false,
        .budget_hit = true,
        .floor_exceeded = true,
    };
    int page_total = semantic_only ? semantic->total : total;
    int page_offset = semantic_only ? semantic->offset : offset;
    if (!json_format) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        if (semantic->present) {
            sg_emit_semantic_paging_tree(&sb, semantic, 0);
        }
        sg_emit_paging_tree(&sb, page_total, page_offset, &floor, 0, false, max_output_bytes, true,
                            semantic_only && semantic->limit == 0);
        return cbm_sb_finish(&sb);
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    if (semantic->present) {
        sg_emit_semantic_paging_json(doc, root, semantic, 0);
    }
    sg_emit_paging_json(doc, root, page_total, page_offset, &floor, 0, false, max_output_bytes,
                        true, semantic_only && semantic->limit == 0);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

static char *handle_search_graph(cbm_mcp_server_t *srv, const char *args) {
    /* Inner phase split: every tool leaks the same ~4 MB per request, so the
     * retainer is in what the handlers share -- store resolution or the query
     * itself. These marks separate the two. */
    cbm_mem_phase_mark("handler.args");
    char *project = get_project_arg(args);
    cbm_mem_phase_mark("handler.resolve_store");
    cbm_store_t *store = resolve_store(srv, project);
    cbm_mem_phase_mark("handler.body");
    REQUIRE_STORE(store, project);

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        return not_indexed;
    }

    /* Response encoding: the default tree chooses direct/grouped by serialized
     * size; format:"json" keeps the stable grouped model. The token argument
     * becomes a deterministic byte ceiling: tokenizers differ, bytes do not. */
    char *format_arg = cbm_mcp_get_string_arg(args, "format");
    bool json_format = format_arg && strcmp(format_arg, "json") == 0;
    free(format_arg);
    int limit = cbm_mcp_get_int_arg(args, "limit", CBM_DEFAULT_SEARCH_LIMIT);
    if (limit < 1) {
        limit = 1;
    } else if (limit > 500) {
        limit = 500;
    }
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    if (offset < 0) {
        offset = 0;
    }
    int semantic_limit = cbm_mcp_get_int_arg(args, "semantic_limit", CBM_DEFAULT_SEARCH_LIMIT);
    if (semantic_limit < 0) {
        semantic_limit = 0;
    } else if (semantic_limit > 500) {
        semantic_limit = 500;
    }
    int semantic_offset = cbm_mcp_get_int_arg(args, "semantic_offset", 0);
    if (semantic_offset < 0) {
        semantic_offset = 0;
    } else if (semantic_offset > MCP_QUERY_MAX_VISIBLE_ROWS) {
        free(project);
        return cbm_mcp_text_result(
            "semantic_offset maximum is 99998 — semantic ranking is resource-bounded; "
            "continue only with a semantic_next_offset emitted by search_graph.",
            true);
    }
    /* One ranked hit beyond the requested semantic page makes has_more
     * deterministic. Keeping the prefix through that lookahead also lets a
     * budget-trimmed page resume at offset + rows actually emitted. */
    int semantic_materialize_limit = semantic_offset + semantic_limit + 1;
    int max_output_tokens = cbm_mcp_get_int_arg(args, "max_output_tokens", 3200);
    if (max_output_tokens < 128) {
        max_output_tokens = 128;
    } else if (max_output_tokens > 1000000) {
        max_output_tokens = 1000000;
    }
    size_t max_output_bytes =
        (size_t)max_output_tokens * (size_t)MCP_OUTPUT_BYTES_PER_TOKEN_ESTIMATE;

    /* BM25 path: if `query` is set, run FTS5 full-text search with ranking
     * and return early. If FTS5 is unavailable or the query is empty after
     * tokenization, fall through to the regex path. */
    char *query = cbm_mcp_get_string_arg(args, "query");
    if (query && search_graph_arg_present(args, "semantic_query")) {
        free(query);
        free(project);
        return cbm_mcp_text_result(
            "query and semantic_query are mutually exclusive — use query for BM25 full-text "
            "ranking or semantic_query for vector ranking, then issue a separate request for "
            "the other mode.",
            true);
    }
    if (query && query[0]) {
        char *q_file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
        char *bm25_json = bm25_search(store, project, query, q_file_pattern, limit, offset,
                                      !json_format, max_output_bytes);
        free(q_file_pattern);
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
    const char *fields[SG_MAX_EXTRA_FIELDS];
    yyjson_doc *fields_owner = NULL;
    bool core_fields_requested = false;
    int nfields =
        sg_parse_fields(args, fields, SG_MAX_EXTRA_FIELDS, &fields_owner, &core_fields_requested);
    char *detail_arg = cbm_mcp_get_string_arg(args, "detail");
    bool detail_ids = detail_arg && strcmp(detail_arg, "ids") == 0;
    free(detail_arg);

    cbm_vector_result_t *vresults = NULL;
    int vcount = 0;
    bool sq_present = false;
    bool sq_type_error = run_semantic_query_core(args, store, project, semantic_materialize_limit,
                                                 &vresults, &vcount, &sq_present);
    if (sq_type_error) {
        if (fields_owner) {
            yyjson_doc_free(fields_owner);
        }
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

    int semantic_available = vcount > semantic_offset ? vcount - semantic_offset : 0;
    int semantic_page_count =
        semantic_available < semantic_limit ? semantic_available : semantic_limit;
    sg_semantic_page_t semantic = {
        .results = semantic_page_count > 0 ? vresults + semantic_offset : NULL,
        .total = vcount,
        .offset = semantic_offset,
        .limit = semantic_limit,
        .total_exact = vcount < semantic_materialize_limit,
        .present = sq_present,
    };

    /* Semantic-only calls must not prepend an unrelated, unfiltered symbol
     * page. Combined calls retain both independently ranked result sets. */
    bool has_filters = label || name_pattern || qn_pattern || file_pattern || relationship ||
                       exclude_entry_points || min_degree != CBM_NOT_FOUND ||
                       max_degree != CBM_NOT_FOUND;
    bool semantic_only = sq_present && !has_filters;
    cbm_search_output_t out = {0};
    if (!semantic_only) {
        (void)cbm_store_search(store, &params, &out);
    }

    const char *diagnostic_hint = NULL;
    if (semantic_only && vcount == 0) {
        diagnostic_hint = "No semantic matches; use a moderate/full index or broader keywords.";
    } else if (!semantic_only && out.total == 0) {
        if (name_pattern && label) {
            diagnostic_hint = "No results; remove label or broaden name_pattern.";
        } else if (name_pattern) {
            diagnostic_hint = "No nodes match; check spelling or broaden the regex.";
        } else if (label) {
            diagnostic_hint = "No nodes have this label; inspect get_graph_schema.";
        }
    } else if (core_fields_requested) {
        diagnostic_hint = "Core qn/name/label/file/lines fields are already present.";
    }

    int requested_fields = detail_ids ? 0 : nfields;
    sg_render_plan_t plan = {
        .returned = out.count,
        .fields_returned = requested_fields,
        .semantic_returned = semantic_page_count,
        .connected_returned = !detail_ids && include_connected,
        .core_hint_returned = diagnostic_hint != NULL,
        .budget_hit = false,
        .floor_exceeded = false,
    };

    char *payload = NULL;
    for (;;) {
        payload =
            sg_render_payload(json_format, &out, offset, fields, requested_fields, store,
                              relationship, !detail_ids && include_connected, detail_ids, &semantic,
                              semantic_only, diagnostic_hint, &plan, max_output_bytes);
        if (!payload || strlen(payload) <= max_output_bytes) {
            break;
        }
        free(payload);
        payload = NULL;
        plan.budget_hit = true;

        /* Preserve core graph answers in their stable ranked/name order.
         * Diagnostics and opt-in enrichment yield first, then whole lower-
         * ranked rows. No identifier, path, or field value is byte-sliced. */
        if (plan.core_hint_returned) {
            plan.core_hint_returned = false;
        } else if (plan.connected_returned) {
            plan.connected_returned = false;
        } else if (plan.fields_returned > 0) {
            plan.fields_returned--;
        } else if (plan.semantic_returned > 0) {
            plan.semantic_returned--;
        } else if (plan.returned > 0) {
            plan.returned--;
        } else {
            break;
        }
    }

    bool identity_available = out.count > 0 || semantic_page_count > 0;
    bool identity_returned = plan.returned > 0 || plan.semantic_returned > 0;
    if (!payload || strlen(payload) > max_output_bytes ||
        (identity_available && plan.budget_hit && !identity_returned)) {
        free(payload);
        payload = sg_budget_floor(json_format, out.total, offset, &semantic, max_output_bytes,
                                  semantic_only);
    }

    if (vcount > 0) {
        cbm_store_free_vector_results(vresults, vcount);
    }
    if (fields_owner) {
        yyjson_doc_free(fields_owner);
    }
    cbm_store_search_free(&out);
    free(project);
    free(label);
    free(name_pattern);
    free(qn_pattern);
    free(file_pattern);
    free(relationship);

    char *result = cbm_mcp_text_result(payload ? payload : "out of memory", payload == NULL);
    free(payload);
    return result;
}

/* query_graph response cursors bind one fully materialized result, not merely
 * an integer offset. Re-executing an unordered query may otherwise produce a
 * different order even while the store's coarse generation is unchanged.
 *
 * Token: q1.<generation>.<params-hash>.<result-digest>.<offset>.<seal>
 *
 * The SHA-256 materialization digest covers ordered columns and rows, explicit
 * NULL markers, engine truncation, and the warning. The short seal makes an
 * edited offset or token field fail closed instead of silently skipping rows.
 * It is an integrity checksum for an opaque token, not an authentication MAC.
 */
enum {
    QUERY_GRAPH_CURSOR_TOKEN_CAP = 224,
    QUERY_GRAPH_GENERATION_CAP = 96,
    QUERY_GRAPH_CURSOR_DIGEST_HEX_LEN = 32
};

typedef struct {
    char generation[QUERY_GRAPH_GENERATION_CAP];
    uint64_t params_hash;
    char result_digest[QUERY_GRAPH_CURSOR_DIGEST_HEX_LEN + 1];
    int offset;
} query_graph_cursor_t;

typedef struct {
    const char *generation;
    uint64_t params_hash;
    const char *result_digest;
    bool can_mint;
} query_graph_cursor_context_t;

static void query_graph_sha_u64(cbm_sha256_ctx *sha, uint64_t value) {
    uint8_t bytes[8];
    for (int index = 7; index >= 0; index--) {
        bytes[index] = (uint8_t)(value & 0xffU);
        value >>= 8U;
    }
    cbm_sha256_update(sha, bytes, sizeof(bytes));
}

static void query_graph_sha_field(cbm_sha256_ctx *sha, const char *value) {
    const uint8_t present = value ? 1U : 0U;
    cbm_sha256_update(sha, &present, sizeof(present));
    if (!value) {
        return;
    }
    size_t length = strlen(value);
    query_graph_sha_u64(sha, (uint64_t)length);
    cbm_sha256_update(sha, value, length);
}

static void query_graph_digest_hex(const uint8_t digest[CBM_SHA256_DIGEST_LEN],
                                   char out[CBM_SHA256_HEX_LEN + 1]) {
    static const char hex[] = "0123456789abcdef";
    for (int index = 0; index < CBM_SHA256_DIGEST_LEN; index++) {
        out[index * 2] = hex[digest[index] >> 4U];
        out[index * 2 + 1] = hex[digest[index] & 0x0fU];
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
}

static uint64_t query_graph_params_hash(const char *project, const char *query, const char *graph) {
    static const char domain[] = "cbm.query_graph.params.v1";
    cbm_sha256_ctx sha;
    cbm_sha256_init(&sha);
    cbm_sha256_update(&sha, domain, sizeof(domain));
    query_graph_sha_field(&sha, project);
    query_graph_sha_field(&sha, query);
    query_graph_sha_field(&sha, graph);
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&sha, digest);
    uint64_t hash = 0;
    for (int index = 0; index < 8; index++) {
        hash = (hash << 8U) | digest[index];
    }
    return hash;
}

static void query_graph_result_digest(const cbm_cypher_result_t *result,
                                      char out[CBM_SHA256_HEX_LEN + 1]) {
    static const char domain[] = "cbm.query_graph.materialization.v1";
    cbm_sha256_ctx sha;
    cbm_sha256_init(&sha);
    cbm_sha256_update(&sha, domain, sizeof(domain));
    query_graph_sha_u64(&sha, (uint64_t)result->col_count);
    query_graph_sha_u64(&sha, (uint64_t)result->row_count);
    const uint8_t truncated = result->truncated ? 1U : 0U;
    cbm_sha256_update(&sha, &truncated, sizeof(truncated));
    query_graph_sha_field(&sha, result->warning);
    for (int column = 0; column < result->col_count; column++) {
        query_graph_sha_field(&sha, result->columns ? result->columns[column] : NULL);
    }
    for (int row = 0; row < result->row_count; row++) {
        const char **cells = result->rows ? result->rows[row] : NULL;
        for (int column = 0; column < result->col_count; column++) {
            query_graph_sha_field(&sha, cells ? cells[column] : NULL);
        }
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&sha, digest);
    query_graph_digest_hex(digest, out);
}

static void query_graph_cursor_seal(const query_graph_cursor_t *cursor, char out[17]) {
    static const char domain[] = "cbm.query_graph.cursor.v1";
    cbm_sha256_ctx sha;
    cbm_sha256_init(&sha);
    cbm_sha256_update(&sha, domain, sizeof(domain));
    query_graph_sha_field(&sha, cursor->generation);
    query_graph_sha_u64(&sha, cursor->params_hash);
    query_graph_sha_field(&sha, cursor->result_digest);
    query_graph_sha_u64(&sha, (uint64_t)cursor->offset);
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&sha, digest);
    char full_hex[CBM_SHA256_HEX_LEN + 1];
    query_graph_digest_hex(digest, full_hex);
    memcpy(out, full_hex, 16);
    out[16] = '\0';
}

static bool query_graph_cursor_encode(const query_graph_cursor_context_t *context, int offset,
                                      char out[QUERY_GRAPH_CURSOR_TOKEN_CAP]) {
    if (!context || !context->can_mint || !context->generation || !context->result_digest ||
        offset <= 0) {
        return false;
    }
    query_graph_cursor_t cursor = {.params_hash = context->params_hash, .offset = offset};
    snprintf(cursor.generation, sizeof(cursor.generation), "%s", context->generation);
    snprintf(cursor.result_digest, sizeof(cursor.result_digest), "%.*s",
             QUERY_GRAPH_CURSOR_DIGEST_HEX_LEN, context->result_digest);
    char seal[17];
    query_graph_cursor_seal(&cursor, seal);
    int written =
        snprintf(out, QUERY_GRAPH_CURSOR_TOKEN_CAP, "q1.%s.%016llx.%s.%d.%s", cursor.generation,
                 (unsigned long long)cursor.params_hash, cursor.result_digest, cursor.offset, seal);
    return written > 0 && written < QUERY_GRAPH_CURSOR_TOKEN_CAP;
}

static bool query_graph_hex_span(const char *begin, const char *end, size_t expected) {
    if (!begin || !end || end < begin || (size_t)(end - begin) != expected) {
        return false;
    }
    for (const char *digit = begin; digit < end; digit++) {
        if (!isxdigit((unsigned char)*digit)) {
            return false;
        }
    }
    return true;
}

/* Decode and validate every token field that does not require executing the
 * query. The materialization digest itself is checked immediately after the
 * full query has been re-executed. */
static const char *query_graph_cursor_decode(const char *token, const char *current_generation,
                                             uint64_t expected_params_hash,
                                             query_graph_cursor_t *out) {
    static const char invalid[] =
        "invalid_cursor: unrecognized or modified token — re-run the original query without "
        "'cursor'";
    memset(out, 0, sizeof(*out));
    if (!token || strncmp(token, "q1.", 3) != 0) {
        return invalid;
    }
    const char *generation_start = token + 3;
    const char *generation_end = strchr(generation_start, '.');
    if (!generation_end || generation_end == generation_start ||
        (size_t)(generation_end - generation_start) >= sizeof(out->generation)) {
        return invalid;
    }
    memcpy(out->generation, generation_start, (size_t)(generation_end - generation_start));
    out->generation[generation_end - generation_start] = '\0';

    const char *hash_start = generation_end + 1;
    const char *hash_end = strchr(hash_start, '.');
    if (!query_graph_hex_span(hash_start, hash_end, 16)) {
        return invalid;
    }
    errno = 0;
    char *parsed_end = NULL;
    unsigned long long parsed_hash = strtoull(hash_start, &parsed_end, 16);
    if (errno == ERANGE || parsed_end != hash_end) {
        return invalid;
    }
    out->params_hash = (uint64_t)parsed_hash;

    const char *digest_start = hash_end + 1;
    const char *digest_end = strchr(digest_start, '.');
    if (!query_graph_hex_span(digest_start, digest_end, QUERY_GRAPH_CURSOR_DIGEST_HEX_LEN)) {
        return invalid;
    }
    memcpy(out->result_digest, digest_start, QUERY_GRAPH_CURSOR_DIGEST_HEX_LEN);
    out->result_digest[QUERY_GRAPH_CURSOR_DIGEST_HEX_LEN] = '\0';

    const char *offset_start = digest_end + 1;
    const char *offset_end = strchr(offset_start, '.');
    errno = 0;
    long parsed_offset = offset_end ? strtol(offset_start, &parsed_end, 10) : -1;
    if (!offset_end || offset_end == offset_start || errno == ERANGE || parsed_end != offset_end ||
        parsed_offset <= 0 || parsed_offset > INT_MAX) {
        return invalid;
    }
    out->offset = (int)parsed_offset;

    const char *seal_start = offset_end + 1;
    const char *seal_end = token + strlen(token);
    if (!query_graph_hex_span(seal_start, seal_end, 16)) {
        return invalid;
    }
    char expected_seal[17];
    query_graph_cursor_seal(out, expected_seal);
    if (memcmp(seal_start, expected_seal, 16) != 0) {
        return invalid;
    }
    if (out->params_hash != expected_params_hash) {
        return "cursor_params_mismatch: this cursor was issued for a different "
               "query/project/graph — pass those arguments unchanged";
    }
    if (strcmp(out->generation, current_generation) != 0) {
        return "stale_cursor: the project was reindexed since this cursor was issued — re-run "
               "the original query without 'cursor'";
    }
    return NULL;
}

static bool query_graph_semantic_prefix_column(const char *column) {
    if (!column) {
        return false;
    }
    const char *leaf = strrchr(column, '.');
    leaf = leaf ? leaf + 1 : column;
    return strcmp(leaf, "qualified_name") == 0 || strcmp(leaf, "qn") == 0 ||
           strcmp(leaf, "file") == 0 || strcmp(leaf, "file_path") == 0 ||
           strcmp(leaf, "path") == 0 || strcmp(leaf, "root_path") == 0 ||
           strcmp(leaf, "source_file") == 0;
}

static char *query_graph_tree_rows_text(const cbm_cypher_result_t *result, int row_offset,
                                        int row_count, bool allow_directory) {
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    bool cell_count_safe = row_count > 0 && result->col_count > 0 &&
                           (size_t)row_count <= SIZE_MAX / (size_t)result->col_count;
    size_t cell_count = cell_count_safe ? (size_t)row_count * (size_t)result->col_count : 0;
    cell_count_safe = cell_count_safe && cell_count <= SIZE_MAX / sizeof(const char *);
    const char **cells =
        cell_count_safe ? (const char **)malloc(cell_count * sizeof(*cells)) : NULL;
    if (cells) {
        for (int row = 0; row < row_count; row++) {
            for (int col = 0; col < result->col_count; col++) {
                cells[(size_t)row * (size_t)result->col_count + (size_t)col] =
                    result->rows[row_offset + row][col];
            }
        }
        if (allow_directory) {
            bool *prefix_cols = calloc((size_t)result->col_count, sizeof(*prefix_cols));
            if (prefix_cols) {
                for (int col = 0; col < result->col_count; col++) {
                    prefix_cols[col] = query_graph_semantic_prefix_column(result->columns[col]);
                }
                cbm_tree_table_rows_profiled(&sb, "rows", row_count,
                                             (const char *const *)result->columns,
                                             result->col_count, cells, NULL, prefix_cols);
                free(prefix_cols);
            } else {
                cbm_tree_table_rows(&sb, "rows", row_count, (const char *const *)result->columns,
                                    result->col_count, cells);
            }
        } else {
            cbm_tree_table_header(&sb, "rows", row_count, (const char *const *)result->columns,
                                  result->col_count);
            for (int row = 0; row < row_count; row++) {
                cbm_tree_row_begin(&sb);
                for (int col = 0; col < result->col_count; col++) {
                    cbm_tree_cell_str(&sb,
                                      cells[(size_t)row * (size_t)result->col_count + (size_t)col],
                                      col == 0);
                }
                cbm_tree_row_end(&sb);
            }
        }
        free(cells);
    } else {
        cbm_tree_table_header(&sb, "rows", row_count, (const char *const *)result->columns,
                              result->col_count);
        for (int row = 0; row < row_count; row++) {
            cbm_tree_row_begin(&sb);
            for (int col = 0; col < result->col_count; col++) {
                cbm_tree_cell_str(&sb, result->rows[row_offset + row][col], col == 0);
            }
            cbm_tree_row_end(&sb);
        }
    }
    return cbm_sb_finish(&sb);
}

static const char *query_graph_truncation_reason(const cbm_cypher_result_t *result, bool budget_hit,
                                                 bool page_limit_hit) {
    if (budget_hit) {
        return "output_budget";
    }
    if (page_limit_hit) {
        return "page_limit";
    }
    if (result->truncated) {
        return "engine_limit";
    }
    return NULL;
}

static char *query_graph_tree_response_text(const cbm_cypher_result_t *result, int row_offset,
                                            int row_count, int visible_count, bool exact_total,
                                            bool allow_directory,
                                            const query_graph_cursor_context_t *cursor_context) {
    bool budget_hit = row_count < visible_count;
    bool materialized_more = row_offset + row_count < result->row_count;
    bool page_limit_hit = !budget_hit && materialized_more;
    bool truncated = budget_hit || materialized_more || result->truncated;

    char *rows_text = query_graph_tree_rows_text(result, row_offset, row_count, allow_directory);
    if (!rows_text) {
        return NULL;
    }
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    cbm_sb_append(&sb, rows_text);
    free(rows_text);
    cbm_tree_scalar_int(&sb, "returned", row_count);
    cbm_tree_scalar_int(&sb, "total", result->row_count);
    cbm_tree_scalar_str(&sb, "total_relation", exact_total ? "eq" : "gte");
    if (row_offset > 0) {
        cbm_tree_scalar_int(&sb, "offset", row_offset);
    }
    cbm_tree_scalar_bool(&sb, "has_more", truncated);
    cbm_tree_scalar_bool(&sb, "truncated", truncated);
    const char *reason = query_graph_truncation_reason(result, budget_hit, page_limit_hit);
    if (reason) {
        cbm_tree_scalar_str(&sb, "truncation_reason", reason);
    }
    if (row_count > 0 && materialized_more) {
        char next_cursor[QUERY_GRAPH_CURSOR_TOKEN_CAP];
        if (query_graph_cursor_encode(cursor_context, row_offset + row_count, next_cursor)) {
            cbm_tree_scalar_str(&sb, "next_cursor", next_cursor);
        }
        cbm_tree_scalar_int(&sb, "next_offset", row_offset + row_count);
    } else if (budget_hit && row_count == 0) {
        cbm_tree_scalar_str(&sb, "hint",
                            "First row exceeds output budget; raise max_output_tokens.");
    }
    if (result->warning) {
        cbm_tree_scalar_str(&sb, "warning", result->warning);
    }
    if (result->row_count == 0 && !result->truncated) {
        cbm_tree_scalar_str(&sb, "hint",
                            "Query returned no results. Use get_graph_schema() to see "
                            "available labels and edge types.");
    }
    return cbm_sb_finish(&sb);
}

static char *query_graph_json_response_text(const cbm_cypher_result_t *result, int row_offset,
                                            int row_count, int visible_count, bool exact_total,
                                            const query_graph_cursor_context_t *cursor_context) {
    bool budget_hit = row_count < visible_count;
    bool materialized_more = row_offset + row_count < result->row_count;
    bool page_limit_hit = !budget_hit && materialized_more;
    bool truncated = budget_hit || materialized_more || result->truncated;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    for (int i = 0; i < result->col_count; i++) {
        yyjson_mut_arr_add_str(doc, cols, result->columns[i]);
    }
    yyjson_mut_obj_add_val(doc, root, "columns", cols);
    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    for (int row_index = 0; row_index < row_count; row_index++) {
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        for (int col = 0; col < result->col_count; col++) {
            yyjson_mut_arr_add_str(doc, row, result->rows[row_offset + row_index][col]);
        }
        yyjson_mut_arr_add_val(rows, row);
    }
    yyjson_mut_obj_add_val(doc, root, "rows", rows);
    yyjson_mut_obj_add_int(doc, root, "returned", row_count);
    yyjson_mut_obj_add_int(doc, root, "total", result->row_count);
    yyjson_mut_obj_add_str(doc, root, "total_relation", exact_total ? "eq" : "gte");
    if (row_offset > 0) {
        yyjson_mut_obj_add_int(doc, root, "offset", row_offset);
    }
    yyjson_mut_obj_add_bool(doc, root, "has_more", truncated);
    yyjson_mut_obj_add_bool(doc, root, "truncated", truncated);
    const char *reason = query_graph_truncation_reason(result, budget_hit, page_limit_hit);
    if (reason) {
        yyjson_mut_obj_add_str(doc, root, "truncation_reason", reason);
    }
    if (row_count > 0 && materialized_more) {
        char next_cursor[QUERY_GRAPH_CURSOR_TOKEN_CAP];
        if (query_graph_cursor_encode(cursor_context, row_offset + row_count, next_cursor)) {
            yyjson_mut_obj_add_strcpy(doc, root, "next_cursor", next_cursor);
        }
        yyjson_mut_obj_add_int(doc, root, "next_offset", row_offset + row_count);
    } else if (budget_hit && row_count == 0) {
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "First row exceeds output budget; raise max_output_tokens.");
    }
    if (result->warning) {
        yyjson_mut_obj_add_str(doc, root, "warning", result->warning);
    }
    if (result->row_count == 0 && !result->truncated) {
        yyjson_mut_obj_add_str(
            doc, root, "hint",
            "Query returned no results. Use get_graph_schema() to see available labels and "
            "edge types.");
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

/* A pathological query can make the column expression itself larger than the
 * requested response budget. Do not byte-slice that semantic key. Return a
 * small, explicit recovery envelope and let the caller raise the budget to
 * retrieve the exact columns and first row. */
static char *query_graph_budget_floor_text(const cbm_cypher_result_t *result, int row_offset,
                                           bool exact_total, bool json_format) {
    if (json_format) {
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);
        yyjson_mut_obj_add_val(doc, root, "columns", yyjson_mut_arr(doc));
        yyjson_mut_obj_add_val(doc, root, "rows", yyjson_mut_arr(doc));
        yyjson_mut_obj_add_bool(doc, root, "columns_omitted", true);
        yyjson_mut_obj_add_int(doc, root, "returned", 0);
        yyjson_mut_obj_add_int(doc, root, "total", result->row_count);
        yyjson_mut_obj_add_str(doc, root, "total_relation", exact_total ? "eq" : "gte");
        if (row_offset > 0) {
            yyjson_mut_obj_add_int(doc, root, "offset", row_offset);
        }
        yyjson_mut_obj_add_bool(doc, root, "has_more", true);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        yyjson_mut_obj_add_str(doc, root, "truncation_reason", "output_budget");
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "Column metadata or first row exceeds output budget; raise "
                               "max_output_tokens.");
        char *json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
        return json;
    }

    cbm_sb_t sb;
    cbm_sb_init(&sb);
    cbm_tree_scalar_int(&sb, "rows", 0);
    cbm_tree_scalar_bool(&sb, "columns_omitted", true);
    cbm_tree_scalar_int(&sb, "returned", 0);
    cbm_tree_scalar_int(&sb, "total", result->row_count);
    cbm_tree_scalar_str(&sb, "total_relation", exact_total ? "eq" : "gte");
    if (row_offset > 0) {
        cbm_tree_scalar_int(&sb, "offset", row_offset);
    }
    cbm_tree_scalar_bool(&sb, "has_more", true);
    cbm_tree_scalar_bool(&sb, "truncated", true);
    cbm_tree_scalar_str(&sb, "truncation_reason", "output_budget");
    cbm_tree_scalar_str(&sb, "hint",
                        "Column metadata or first row exceeds output budget; raise "
                        "max_output_tokens.");
    return cbm_sb_finish(&sb);
}

static char *handle_query_graph(cbm_mcp_server_t *srv, const char *args) {
    char *query = cbm_mcp_get_string_arg(args, "query");
    char *project = get_project_arg(args);
    cbm_store_t *store = resolve_store(srv, project);
    int visible_limit = cbm_mcp_get_int_arg(args, "max_rows", 200);
    if (visible_limit == 0 || visible_limit > MCP_QUERY_MAX_VISIBLE_ROWS) {
        /* compatibility: max_rows=0 used to mean engine default */
        visible_limit = MCP_QUERY_MAX_VISIBLE_ROWS;
    } else if (visible_limit < 0) {
        visible_limit = 1;
    }
    int requested_offset = cbm_mcp_get_int_arg(args, "offset", 0);
    int row_offset = requested_offset;
    if (row_offset < 0) {
        row_offset = 0;
    }
    char *cursor_arg = cbm_mcp_get_string_arg(args, "cursor");

    /* graph="missed" (#963): run the SAME cypher against the derived
     * miss-graph view (shadow project "<project>::missed") instead of the
     * code graph — file structure of not-fully-indexed files only. */
    char *graph_arg = cbm_mcp_get_string_arg(args, "graph");
    bool missed_graph = graph_arg && strcmp(graph_arg, "missed") == 0;
    free(graph_arg);

    if (!query) {
        free(cursor_arg);
        free(project);
        return cbm_mcp_text_result("query is required", true);
    }
    if (missed_graph && !project) {
        free(cursor_arg);
        free(query);
        return cbm_mcp_text_result("project is required when graph=\"missed\"", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(cursor_arg);
        free(project);
        free(query);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(cursor_arg);
        free(project);
        free(query);
        return not_indexed;
    }

    char covproj[CBM_SZ_512];
    const char *cypher_project = project;
    if (missed_graph) {
        cbm_store_coverage_shadow_project(covproj, sizeof(covproj), project);
        cypher_project = covproj;
    }

    char generation[QUERY_GRAPH_GENERATION_CAP] = "";
    int generation_rc = cbm_store_generation(store, generation, sizeof(generation));
    if (generation_rc != CBM_STORE_OK) {
        free(cursor_arg);
        free(project);
        free(query);
        return cbm_mcp_text_result(
            "index_metadata_error: generation metadata is unreadable; reindex before querying",
            true);
    }
    bool generation_available = generation[0] != '\0';
    const char *effective_project = project ? project : srv->current_project;
    uint64_t params_hash =
        query_graph_params_hash(effective_project, query, missed_graph ? "missed" : "code");
    query_graph_cursor_t cursor = {0};
    if (cursor_arg) {
        if (requested_offset != 0) {
            free(cursor_arg);
            free(project);
            free(query);
            return cbm_mcp_text_result(
                "invalid_params: cursor cannot be combined with a nonzero offset", true);
        }
        if (!generation_available || strcmp(generation, "legacy") == 0) {
            free(cursor_arg);
            free(project);
            free(query);
            return cbm_mcp_text_result(
                "cursor_unsupported: this index predates generation tracking; reindex before "
                "using snapshot pagination",
                true);
        }
        const char *cursor_error =
            query_graph_cursor_decode(cursor_arg, generation, params_hash, &cursor);
        if (cursor_error) {
            free(cursor_arg);
            free(project);
            free(query);
            return cbm_mcp_text_result(cursor_error, true);
        }
        row_offset = cursor.offset;
    }

    cbm_cypher_result_t result = {0};
    /* `max_rows` is a presentation budget, never an evaluation budget.
     * Executing at visible_limit+1 corrupts ORDER BY, DISTINCT, aggregation,
     * and SKIP semantics before serialization gets a chance to trim rows.
     * Zero preserves the engine's established 100k safety ceiling. */
    int rc = cbm_cypher_execute(store, query, cypher_project, 0, &result);

    if (rc < 0) {
        char *err_msg = result.error ? result.error : "query execution failed";
        char *resp = cbm_mcp_text_result(err_msg, true);
        cbm_cypher_result_free(&result);
        free(cursor_arg);
        free(query);
        free(project);
        return resp;
    }

    char result_digest[CBM_SHA256_HEX_LEN + 1];
    query_graph_result_digest(&result, result_digest);
    if (cursor_arg &&
        strncmp(cursor.result_digest, result_digest, QUERY_GRAPH_CURSOR_DIGEST_HEX_LEN) != 0) {
        char *resp = cbm_mcp_text_result(
            "stale_cursor: the complete query materialization changed since this cursor was "
            "issued; re-run the original query without 'cursor'",
            true);
        cbm_cypher_result_free(&result);
        free(cursor_arg);
        free(query);
        free(project);
        return resp;
    }
    query_graph_cursor_context_t cursor_context = {
        .generation = generation,
        .params_hash = params_hash,
        .result_digest = result_digest,
        .can_mint = generation_available && strcmp(generation, "legacy") != 0,
    };

    /* Response encoding: TOON table by default (the columns double as the
     * table header); format:"json" restores the legacy columns/rows arrays. */
    char *qg_format = cbm_mcp_get_string_arg(args, "format");
    bool qg_legacy_json = qg_format && strcmp(qg_format, "json") == 0;
    free(qg_format);

    int materialized_rows = row_offset < result.row_count ? result.row_count - row_offset : 0;
    int available_rows = materialized_rows < visible_limit ? materialized_rows : visible_limit;
    int max_output_tokens = cbm_mcp_get_int_arg(args, "max_output_tokens", 3200);
    if (max_output_tokens < 128) {
        max_output_tokens = 128;
    } else if (max_output_tokens > 1000000) {
        max_output_tokens = 1000000;
    }
    size_t byte_budget = (size_t)max_output_tokens * (size_t)MCP_OUTPUT_BYTES_PER_TOKEN_ESTIMATE;
    size_t estimated = 256U;
    for (int c = 0; c < result.col_count; c++) {
        estimated += strlen(result.columns[c]) + 1U;
    }
    int raw_fit_rows = 0;
    for (int r = 0; r < available_rows; r++) {
        size_t row_bytes = 4U;
        for (int c = 0; c < result.col_count; c++) {
            size_t cell_bytes =
                strlen(result.rows[row_offset + r][c] ? result.rows[row_offset + r][c] : "") + 1U;
            row_bytes = row_bytes > SIZE_MAX - cell_bytes ? SIZE_MAX : row_bytes + cell_bytes;
        }
        if (estimated <= byte_budget && row_bytes <= byte_budget - estimated) {
            raw_fit_rows = r + 1;
        }
        estimated = estimated > SIZE_MAX - row_bytes ? SIZE_MAX : estimated + row_bytes;
    }

    /* Exact sizing is bounded near the conservative raw estimate. This avoids
     * constructing a 100k-row candidate merely to learn that the default
     * 3,200-token response can hold a few hundred rows. */
    int exact_upper = raw_fit_rows;
    if (exact_upper < available_rows) {
        int headroom = available_rows - exact_upper < 32 ? available_rows - exact_upper : 32;
        exact_upper += headroom;
    }
    bool exact_total = !result.truncated;

    int output_rows = 0;
    char *json = NULL;
    if (qg_legacy_json) {
        /* JSON size is monotone in the row prefix, so exact serialized probes
         * safely account for escaping and syntax overhead. */
        int low = 0;
        int high = exact_upper;
        while (low <= high) {
            int middle = low + (high - low) / 2;
            char *candidate = query_graph_json_response_text(
                &result, row_offset, middle, available_rows, exact_total, &cursor_context);
            bool fits = candidate && strlen(candidate) <= byte_budget;
            free(candidate);
            if (fits) {
                output_rows = middle;
                low = middle + 1;
            } else {
                high = middle - 1;
            }
        }
        json = query_graph_json_response_text(&result, row_offset, output_rows, available_rows,
                                              exact_total, &cursor_context);
    } else {
        /* A repeated multi-KiB path/QN prefix can make the complete compact
         * page tiny even when the raw estimate is enormous. Probe that common
         * directory-shaped case once, under a hard construction guard, before
         * using the conservative raw estimate. */
        enum { QUERY_COMPACT_FULL_PROBE_MAX_BYTES = 16 * 1024 * 1024 };
        if (available_rows <= 512 && estimated <= QUERY_COMPACT_FULL_PROBE_MAX_BYTES) {
            char *candidate =
                query_graph_tree_response_text(&result, row_offset, available_rows, available_rows,
                                               exact_total, true, &cursor_context);
            if (candidate && strlen(candidate) <= byte_budget) {
                output_rows = available_rows;
                json = candidate;
            } else {
                free(candidate);
            }
        }

        if (!json) {
            /* Establish a monotone direct-table baseline first. */
            int low = 0;
            int high = exact_upper;
            while (low <= high) {
                int middle = low + (high - low) / 2;
                char *candidate =
                    query_graph_tree_response_text(&result, row_offset, middle, available_rows,
                                                   exact_total, false, &cursor_context);
                bool fits = candidate && strlen(candidate) <= byte_budget;
                free(candidate);
                if (fits) {
                    output_rows = middle;
                    low = middle + 1;
                } else {
                    high = middle - 1;
                }
            }

            /* Directory activation can make N+1 rows smaller than N. For the lean
             * default-sized answer, inspect every bounded candidate in descending
             * order instead of making an invalid binary-search assumption. Skip
             * this expansion for enormous raw payloads; the direct fit remains a
             * truthful, bounded answer and can still be encoded more compactly. */
            int compact_upper = exact_upper;
            size_t expanded_budget = byte_budget <= SIZE_MAX / 4U ? byte_budget * 4U : SIZE_MAX;
            if (available_rows <= 512 && estimated <= expanded_budget) {
                compact_upper = available_rows;
            }
            for (int candidate_rows = compact_upper; candidate_rows > output_rows;
                 candidate_rows--) {
                char *candidate = query_graph_tree_response_text(
                    &result, row_offset, candidate_rows, available_rows, exact_total, true,
                    &cursor_context);
                bool fits = candidate && strlen(candidate) <= byte_budget;
                if (fits) {
                    output_rows = candidate_rows;
                    json = candidate;
                    break;
                }
                free(candidate);
            }
            if (!json) {
                json =
                    query_graph_tree_response_text(&result, row_offset, output_rows, available_rows,
                                                   exact_total, true, &cursor_context);
            }
        }
    }
    if (json && output_rows == 0 && strlen(json) > byte_budget) {
        free(json);
        json = query_graph_budget_floor_text(&result, row_offset, exact_total, qg_legacy_json);
    }
    cbm_cypher_result_free(&result);
    free(cursor_arg);
    free(query);
    free(project);

    char *res = cbm_mcp_text_result(json ? json : "out of memory", json == NULL);
    free(json);
    return res;
}

/* Indexing-coverage report (#963), attached to index_status: the best-effort
 * signal from the separate index_coverage table (coverage is metadata ABOUT
 * the graph, stored outside it). Full per-project list, capped generously. */
enum { COVERAGE_FILE_CAP = 500 };

static void add_coverage_report(yyjson_mut_doc *doc, yyjson_mut_val *root, cbm_store_t *store,
                                const char *project, const char *indexed_at, int sample_limit) {
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    (void)cbm_store_coverage_get(store, project, &rows, &count);
    cbm_coverage_meta_t meta = {0};
    bool have_meta = cbm_store_coverage_meta_get(store, project, &meta) == CBM_STORE_OK;

    yyjson_mut_val *pp_files = yyjson_mut_arr(doc);
    yyjson_mut_val *sk_files = yyjson_mut_arr(doc);
    yyjson_mut_val *ni_dirs = yyjson_mut_arr(doc);
    yyjson_mut_val *ni_files = yyjson_mut_arr(doc);
    int pp_n = 0;
    int sk_n = 0;
    int ni_dir_n = 0;
    int ni_file_n = 0;
    for (int i = 0; i < count; i++) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "parse_partial") == 0) {
            if (pp_n < sample_limit) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "error_ranges",
                                          rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(pp_files, fe);
            }
            pp_n++;
        } else if (strcmp(kind, "not_indexed_dir") == 0) {
            if (ni_dir_n < sample_limit) {
                yyjson_mut_arr_add_strcpy(doc, ni_dirs, rows[i].rel_path);
            }
            ni_dir_n++;
        } else if (strcmp(kind, "not_indexed_file") == 0) {
            if (ni_file_n < sample_limit) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(ni_files, fe);
            }
            ni_file_n++;
        } else {
            if (sk_n < sample_limit) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_obj_add_strcpy(doc, fe, "phase", rows[i].kind ? rows[i].kind : "");
                yyjson_mut_arr_add_val(sk_files, fe);
            }
            sk_n++;
        }
    }
    cbm_store_free_coverage(rows, count);

    /* Discovery retains a bounded sample of ignored files but records the
     * exact uncapped total atomically beside it. Counts-only status must use
     * that authoritative total: reporting only the stored rows makes a lean
     * response look complete precisely when the discovery cap was hit. */
    bool generation_matches =
        have_meta && indexed_at && meta.generation && strcmp(indexed_at, meta.generation) == 0;
    bool ignored_total_authoritative = generation_matches && meta.ignored_files_total >= ni_file_n;
    int ni_file_total = ni_file_n;
    if (ignored_total_authoritative) {
        ni_file_total = meta.ignored_files_total;
    }
    cbm_store_coverage_meta_clear(&meta);
    int ni_files_shown = ni_file_n < sample_limit ? ni_file_n : sample_limit;

    yyjson_mut_val *pp = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, pp, "files", pp_files);
    yyjson_mut_obj_add_int(doc, pp, "count", pp_n);
    yyjson_mut_obj_add_bool(doc, pp, "truncated", pp_n > sample_limit);
    if (pp_n > sample_limit) {
        yyjson_mut_obj_add_int(doc, pp, "omitted", pp_n - sample_limit);
    }
    yyjson_mut_obj_add_val(doc, root, "parse_partial", pp);

    yyjson_mut_val *sk = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, sk, "files", sk_files);
    yyjson_mut_obj_add_int(doc, sk, "count", sk_n);
    yyjson_mut_obj_add_bool(doc, sk, "truncated", sk_n > sample_limit);
    if (sk_n > sample_limit) {
        yyjson_mut_obj_add_int(doc, sk, "omitted", sk_n - sample_limit);
    }
    yyjson_mut_obj_add_val(doc, root, "skipped", sk);

    /* By-design exclusions (#963 "purposely not indexed"): a deliberate,
     * deterministic class — NOT a failure and NOT best-effort. Dirs are
     * exhaustive; per-file entries are capped in discovery (2000) with the
     * truncation explicit. */
    yyjson_mut_val *ni = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, ni, "dirs", ni_dirs);
    yyjson_mut_obj_add_int(doc, ni, "dirs_count", ni_dir_n);
    yyjson_mut_obj_add_val(doc, ni, "files", ni_files);
    yyjson_mut_obj_add_int(doc, ni, "files_count", ni_file_total);
    if (!ignored_total_authoritative) {
        yyjson_mut_obj_add_str(doc, ni, "files_count_relation", "gte");
    }
    yyjson_mut_obj_add_bool(doc, ni, "truncated",
                            ni_dir_n > sample_limit || ni_file_total > ni_files_shown);
    if (ni_dir_n > sample_limit) {
        yyjson_mut_obj_add_int(doc, ni, "dirs_omitted", ni_dir_n - sample_limit);
    }
    if (ni_file_total > ni_files_shown) {
        yyjson_mut_obj_add_int(doc, ni, "files_omitted", ni_file_total - ni_files_shown);
    }
    if (sample_limit > 0 && (ni_dir_n > 0 || ni_file_total > 0)) {
        yyjson_mut_obj_add_str(doc, ni, "note",
                               "Purposely not indexed — excluded BY DESIGN via "
                               "gitignore/.cbmignore/skip-lists (see each file's reason). Not an "
                               "error: change the ignore rules and re-index to include them.");
    }
    yyjson_mut_obj_add_val(doc, root, "not_indexed", ni);

    if (sample_limit > 0 && (pp_n > 0 || sk_n > 0)) {
        yyjson_mut_obj_add_str(
            doc, root, "coverage_note",
            "Best-effort signal, not a completeness guarantee: parse_partial files WERE indexed, "
            "but constructs inside the listed line ranges (1-based) MAY be missing from the graph "
            "(tree-sitter error recovery still salvages some). skipped files were not indexed at "
            "all. Prefer text search (grep) for flagged files/ranges. Files absent from this list "
            "are NOT guaranteed to be fully indexed. (not_indexed entries are a separate, "
            "BY-DESIGN class — deliberate ignore rules, not failures.)");
    }
}

enum {
    COVERAGE_PATH_MAX = 128,
    COVERAGE_SCOPE_MAX = 32,
    COVERAGE_SCOPE_DEFAULT_LIMIT = 200,
    COVERAGE_SCOPE_MAX_LIMIT = 1000,
    COVERAGE_RANGE_MAX = 128,
};

bool cbm_path_within_root(const char *root_path, const char *abs_path); /* defined below */

typedef enum {
    COVERAGE_PATH_OK = 0,
    COVERAGE_PATH_OUTSIDE,
    COVERAGE_PATH_INVALID,
} coverage_path_result_t;

/* Normalize an untrusted repository-relative path without touching the
 * filesystem. Absolute paths, drive/UNC paths, control bytes, and any `..`
 * component are rejected. A root scope (`.`) normalizes to the empty prefix. */
static coverage_path_result_t coverage_normalize_rel(const char *input, bool allow_root, char *out,
                                                     size_t out_size) {
    if (!input || !out || out_size == 0U) {
        return COVERAGE_PATH_INVALID;
    }
    out[0] = '\0';
    size_t len = strlen(input);
    if (len == 0U || len >= out_size || input[0] == '/' || input[0] == '\\' ||
        (len >= 2U && isalpha((unsigned char)input[0]) && input[1] == ':')) {
        return COVERAGE_PATH_OUTSIDE;
    }

    size_t in = 0U;
    size_t written = 0U;
    while (in < len) {
        while (in < len && (input[in] == '/' || input[in] == '\\')) {
            in++;
        }
        if (in >= len) {
            break;
        }
        size_t start = in;
        while (in < len && input[in] != '/' && input[in] != '\\') {
            unsigned char c = (unsigned char)input[in];
            if (c < 0x20U) {
                return COVERAGE_PATH_INVALID;
            }
            in++;
        }
        size_t part_len = in - start;
        if (part_len == 1U && input[start] == '.') {
            continue;
        }
        if (part_len == 2U && input[start] == '.' && input[start + 1U] == '.') {
            return COVERAGE_PATH_OUTSIDE;
        }
        if (written > 0U) {
            if (written + 1U >= out_size) {
                return COVERAGE_PATH_INVALID;
            }
            out[written++] = '/';
        }
        if (written + part_len >= out_size) {
            return COVERAGE_PATH_INVALID;
        }
        memcpy(out + written, input + start, part_len);
        written += part_len;
    }
    out[written] = '\0';
    return written > 0U || allow_root ? COVERAGE_PATH_OK : COVERAGE_PATH_INVALID;
}

static int64_t coverage_stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * (int64_t)CBM_NSEC_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * (int64_t)CBM_NSEC_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

static const char *coverage_path_freshness(cbm_store_t *store, const char *project,
                                           const char *root_path, const char *rel_path,
                                           bool *outside) {
    *outside = false;
    if (!root_path || !root_path[0]) {
        return "unavailable";
    }
    char abs_path[CBM_SZ_4K];
    int n = snprintf(abs_path, sizeof(abs_path), "%s%s%s", root_path,
                     root_path[strlen(root_path) - 1U] == '/' ? "" : "/", rel_path);
    if (n < 0 || (size_t)n >= sizeof(abs_path)) {
        return "unavailable";
    }
    struct stat st;
    if (stat(abs_path, &st) != 0) {
        return "missing";
    }
    if (!cbm_path_within_root(root_path, abs_path)) {
        *outside = true;
        return "outside_project";
    }

    cbm_file_hash_t hash = {0};
    int rc = cbm_store_get_file_hash(store, project, rel_path, &hash);
    if (rc == CBM_STORE_NOT_FOUND) {
        return "not_tracked";
    }
    if (rc != CBM_STORE_OK) {
        return "unavailable";
    }
    bool matches = hash.mtime_ns == coverage_stat_mtime_ns(&st) && hash.size == st.st_size;
    cbm_store_clear_file_hash(&hash);
    return matches ? "metadata_match" : "metadata_changed";
}

static bool coverage_add_ranges(yyjson_mut_doc *doc, yyjson_mut_val *row, const char *detail) {
    if (!detail || !detail[0]) {
        return false;
    }
    yyjson_mut_val *ranges = yyjson_mut_arr(doc);
    const char *p = detail;
    int emitted = 0;
    while (*p && emitted < COVERAGE_RANGE_MAX) {
        while (*p == ' ' || *p == ',') {
            p++;
        }
        if (!isdigit((unsigned char)*p)) {
            break;
        }
        char *endptr = NULL;
        long start = strtol(p, &endptr, 10);
        if (endptr == p || start <= 0 || start > INT32_MAX) {
            break;
        }
        p = endptr;
        long end = start;
        if (*p == '-') {
            p++;
            long parsed = strtol(p, &endptr, 10);
            if (endptr == p || parsed < start || parsed > INT32_MAX) {
                break;
            }
            end = parsed;
            p = endptr;
        }
        yyjson_mut_val *range = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, range, "start", start);
        yyjson_mut_obj_add_int(doc, range, "end", end);
        yyjson_mut_arr_add_val(ranges, range);
        emitted++;
        while (*p == ' ') {
            p++;
        }
        if (*p && *p != ',') {
            break;
        }
    }
    if (emitted > 0) {
        yyjson_mut_obj_add_val(doc, row, "ranges", ranges);
        if (emitted == COVERAGE_RANGE_MAX && *p) {
            yyjson_mut_obj_add_bool(doc, row, "ranges_truncated", true);
        }
    }
    return emitted > 0;
}

static void coverage_add_row_json(yyjson_mut_doc *doc, yyjson_mut_val *array,
                                  const cbm_coverage_row_t *row, const char *requested_path,
                                  bool diagnostics_full) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "path", row->rel_path ? row->rel_path : "");
    yyjson_mut_obj_add_strcpy(doc, item, "kind", row->kind ? row->kind : "");
    if (requested_path) {
        yyjson_mut_obj_add_str(
            doc, item, "match",
            row->rel_path && strcmp(row->rel_path, requested_path) == 0 ? "exact" : "ancestor");
    }
    bool ranges_added = false;
    if (row->kind && strcmp(row->kind, "parse_partial") == 0) {
        ranges_added = coverage_add_ranges(doc, item, row->detail);
    }
    /* A parse_partial detail such as "3-4,9" is byte-for-byte redundant once
     * represented as typed ranges. Preserve raw storage diagnostics on request,
     * and preserve every non-range detail by default because it may explain why
     * a file was skipped or excluded. */
    if (diagnostics_full || !ranges_added) {
        yyjson_mut_obj_add_strcpy(doc, item, "detail", row->detail ? row->detail : "");
    }
    yyjson_mut_arr_add_val(array, item);
}

static const char *coverage_status(const cbm_coverage_row_t *rows, int count,
                                   const char *requested_path, const char *recording_status,
                                   bool generation_matches, bool lookup_ok,
                                   bool exact_path_verified) {
    if (!lookup_ok) {
        return "coverage_unavailable";
    }
    bool exact = false;
    for (int i = 0; i < count; i++) {
        if (rows[i].rel_path && strcmp(rows[i].rel_path, requested_path) == 0) {
            exact = true;
            break;
        }
    }
    for (int pass = 0; pass < 3; pass++) {
        for (int i = 0; i < count; i++) {
            if (exact && (!rows[i].rel_path || strcmp(rows[i].rel_path, requested_path) != 0)) {
                continue;
            }
            const char *kind = rows[i].kind ? rows[i].kind : "";
            if (pass == 0 && strcmp(kind, "parse_partial") == 0) {
                return "partial";
            }
            if (pass == 1 && strncmp(kind, "not_indexed", 11) == 0) {
                return "excluded";
            }
            if (pass == 2 && kind[0]) {
                return "skipped";
            }
        }
    }
    bool recording_complete = recording_status && strcmp(recording_status, "complete") == 0;
    bool truncated_exact_path_verified =
        exact_path_verified && recording_status && strcmp(recording_status, "truncated") == 0;
    if (!generation_matches || (!recording_complete && !truncated_exact_path_verified)) {
        return "coverage_unavailable";
    }
    return "no_recorded_issue";
}

static const char *coverage_recommended_action(const char *status, const char *freshness) {
    if (!freshness || strcmp(freshness, "metadata_match") != 0) {
        return "read_source_and_reindex";
    }
    if (strcmp(status, "partial") == 0) {
        return "read_ranges_and_verify_scope";
    }
    if (strcmp(status, "skipped") == 0) {
        return "read_source_directly";
    }
    if (strcmp(status, "excluded") == 0) {
        return "read_source_or_change_ignore_rules";
    }
    if (strcmp(status, "no_recorded_issue") == 0) {
        return "use_graph_with_best_effort_caveat";
    }
    return "read_source_and_reindex";
}

static char *handle_check_index_coverage(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);

    yyjson_doc *adoc = yyjson_read(args, strlen(args), 0);
    yyjson_val *aroot = adoc ? yyjson_doc_get_root(adoc) : NULL;
    yyjson_val *paths = aroot ? yyjson_obj_get(aroot, "paths") : NULL;
    yyjson_val *scopes = aroot ? yyjson_obj_get(aroot, "scopes") : NULL;
    size_t path_count = paths && yyjson_is_arr(paths) ? yyjson_arr_size(paths) : 0U;
    size_t scope_count = scopes && yyjson_is_arr(scopes) ? yyjson_arr_size(scopes) : 0U;
    if (!aroot || (paths && !yyjson_is_arr(paths)) || (scopes && !yyjson_is_arr(scopes)) ||
        (path_count == 0U && scope_count == 0U) || path_count > COVERAGE_PATH_MAX ||
        scope_count > COVERAGE_SCOPE_MAX) {
        if (adoc) {
            yyjson_doc_free(adoc);
        }
        free(project);
        return cbm_mcp_text_result(
            "paths or scopes is required (arrays; max 128 paths and 32 scopes)", true);
    }

    int path_limit = cbm_mcp_get_int_arg(args, "path_limit", 20);
    int path_offset = cbm_mcp_get_int_arg(args, "path_offset", 0);
    if (path_limit < 1) {
        path_limit = 1;
    } else if (path_limit > COVERAGE_PATH_MAX) {
        path_limit = COVERAGE_PATH_MAX;
    }
    if (path_offset < 0) {
        path_offset = 0;
    }
    char *diagnostics = cbm_mcp_get_string_arg(args, "diagnostics");
    bool diagnostics_full = diagnostics && strcmp(diagnostics, "full") == 0;

    cbm_project_t proj = {0};
    bool have_project = cbm_store_get_project(store, project, &proj) == CBM_STORE_OK;
    cbm_coverage_meta_t meta = {0};
    bool have_meta = cbm_store_coverage_meta_get(store, project, &meta) == CBM_STORE_OK;
    bool generation_matches = have_project && have_meta && proj.indexed_at && meta.generation &&
                              strcmp(proj.indexed_at, meta.generation) == 0;
    const char *recording_status =
        have_meta && meta.recording_status ? meta.recording_status : "unknown";

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_str(doc, root, "signal", "best_effort");
    yyjson_mut_obj_add_strcpy(doc, root, "indexed_at",
                              have_project && proj.indexed_at ? proj.indexed_at : "");

    yyjson_mut_val *meta_obj = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "generation",
                              have_meta && meta.generation ? meta.generation : "");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "index_mode",
                              have_meta && meta.index_mode ? meta.index_mode : "unknown");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "recorded_at",
                              have_meta && meta.recorded_at ? meta.recorded_at : "");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "recording_status", recording_status);
    yyjson_mut_obj_add_int(doc, meta_obj, "ignored_files_stored",
                           have_meta ? meta.ignored_files_stored : 0);
    yyjson_mut_obj_add_int(doc, meta_obj, "ignored_files_total",
                           have_meta ? meta.ignored_files_total : 0);
    yyjson_mut_obj_add_bool(doc, meta_obj, "hash_records_complete",
                            have_meta && meta.hash_records_complete);
    yyjson_mut_obj_add_int(doc, meta_obj, "coverage_version",
                           have_meta ? meta.coverage_version : 0);
    yyjson_mut_obj_add_bool(doc, meta_obj, "generation_matches", generation_matches);
    yyjson_mut_obj_add_val(doc, root, "metadata", meta_obj);

    yyjson_mut_val *path_results = yyjson_mut_arr(doc);
    int path_returned = 0;
    size_t idx;
    size_t max;
    yyjson_val *value;
    if (paths) {
        yyjson_arr_foreach(paths, idx, max, value) {
            if (idx < (size_t)path_offset || idx >= (size_t)path_offset + (size_t)path_limit) {
                continue;
            }
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            const char *input = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
            yyjson_mut_obj_add_strcpy(doc, item, "requested_path", input ? input : "");
            char rel[CBM_SZ_4K];
            coverage_path_result_t normalized =
                coverage_normalize_rel(input, false, rel, sizeof(rel));
            if (normalized != COVERAGE_PATH_OK) {
                yyjson_mut_obj_add_str(doc, item, "status",
                                       normalized == COVERAGE_PATH_OUTSIDE ? "outside_project"
                                                                           : "invalid_path");
                yyjson_mut_obj_add_str(doc, item, "freshness", "unavailable");
                yyjson_mut_obj_add_str(doc, item, "recommended_action",
                                       "use_project_relative_path");
                yyjson_mut_arr_add_val(path_results, item);
                path_returned++;
                continue;
            }
            yyjson_mut_obj_add_strcpy(doc, item, "path", rel);
            cbm_coverage_row_t *rows = NULL;
            int row_count = 0;
            int cov_rc = cbm_store_coverage_get_path(store, project, rel, &rows, &row_count);
            bool lookup_ok = cov_rc == CBM_STORE_OK || cov_rc == CBM_STORE_NOT_FOUND;
            if (!lookup_ok) {
                row_count = 0;
                yyjson_mut_obj_add_str(doc, item, "coverage_lookup", "error");
            }
            bool outside = false;
            const char *freshness = coverage_path_freshness(
                store, project, have_project ? proj.root_path : NULL, rel, &outside);
            bool exact_path_verified =
                have_meta && meta.hash_records_complete && strcmp(freshness, "metadata_match") == 0;
            const char *status =
                outside ? "outside_project"
                        : coverage_status(rows, row_count, rel, recording_status,
                                          generation_matches, lookup_ok, exact_path_verified);
            yyjson_mut_obj_add_strcpy(doc, item, "status", status);
            yyjson_mut_obj_add_strcpy(doc, item, "freshness", freshness);
            yyjson_mut_obj_add_strcpy(doc, item, "recommended_action",
                                      coverage_recommended_action(status, freshness));
            yyjson_mut_val *coverage = yyjson_mut_arr(doc);
            for (int i = 0; i < row_count; i++) {
                coverage_add_row_json(doc, coverage, &rows[i], rel, diagnostics_full);
            }
            yyjson_mut_obj_add_val(doc, item, "coverage", coverage);
            cbm_store_free_coverage(rows, row_count);
            yyjson_mut_arr_add_val(path_results, item);
            path_returned++;
        }
    }
    yyjson_mut_obj_add_val(doc, root, "paths", path_results);
    yyjson_mut_obj_add_int(doc, root, "path_total", (int64_t)path_count);
    yyjson_mut_obj_add_int(doc, root, "path_returned", path_returned);
    bool path_has_more = (size_t)path_offset + (size_t)path_returned < path_count;
    yyjson_mut_obj_add_bool(doc, root, "path_has_more", path_has_more);
    if (path_has_more) {
        yyjson_mut_obj_add_int(doc, root, "path_next_offset", path_offset + path_returned);
    }

    int scope_limit = cbm_mcp_get_int_arg(args, "scope_limit", 20);
    int scope_offset = cbm_mcp_get_int_arg(args, "scope_offset", 0);
    if (scope_limit < 1) {
        scope_limit = 1;
    } else if (scope_limit > COVERAGE_SCOPE_MAX_LIMIT) {
        scope_limit = COVERAGE_SCOPE_MAX_LIMIT;
    }
    if (scope_offset < 0) {
        scope_offset = 0;
    }
    yyjson_mut_val *scope_results = yyjson_mut_arr(doc);
    int scope_total = 0;
    int scope_returned = 0;
    if (scopes) {
        yyjson_arr_foreach(scopes, idx, max, value) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            const char *input = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
            yyjson_mut_obj_add_strcpy(doc, item, "requested_scope", input ? input : "");
            char scope[CBM_SZ_4K];
            coverage_path_result_t normalized =
                coverage_normalize_rel(input, true, scope, sizeof(scope));
            if (normalized != COVERAGE_PATH_OK) {
                yyjson_mut_obj_add_str(doc, item, "status",
                                       normalized == COVERAGE_PATH_OUTSIDE ? "outside_project"
                                                                           : "invalid_path");
                yyjson_mut_arr_add_val(scope_results, item);
                continue;
            }
            yyjson_mut_obj_add_strcpy(doc, item, "scope", scope[0] ? scope : ".");
            cbm_coverage_row_t *rows = NULL;
            int row_count = 0;
            int cov_rc = cbm_store_coverage_get_scope(store, project, scope, &rows, &row_count);
            bool lookup_ok = cov_rc == CBM_STORE_OK || cov_rc == CBM_STORE_NOT_FOUND;
            if (!lookup_ok) {
                row_count = 0;
                yyjson_mut_obj_add_str(doc, item, "coverage_lookup", "error");
            }
            yyjson_mut_obj_add_int(doc, item, "total", row_count);
            int flat_start = scope_total;
            int flat_end = flat_start + row_count;
            int page_start = scope_offset;
            int page_end = scope_offset + scope_limit;
            int start = page_start > flat_start ? page_start - flat_start : 0;
            int end = page_end < flat_end ? page_end - flat_start : row_count;
            if (start < 0) {
                start = 0;
            } else if (start > row_count) {
                start = row_count;
            }
            if (end < start) {
                end = start;
            } else if (end > row_count) {
                end = row_count;
            }
            int returned = end - start;
            yyjson_mut_obj_add_int(doc, item, "returned", returned);
            yyjson_mut_obj_add_bool(doc, item, "truncated", returned < row_count);
            yyjson_mut_val *entries = yyjson_mut_arr(doc);
            for (int i = start; i < end; i++) {
                coverage_add_row_json(doc, entries, &rows[i], NULL, diagnostics_full);
            }
            yyjson_mut_obj_add_val(doc, item, "entries", entries);
            const char *scope_status = !lookup_ok || !generation_matches ? "coverage_unavailable"
                                       : row_count > 0                   ? "known_gaps"
                                       : strcmp(recording_status, "complete") == 0
                                           ? "no_recorded_issue"
                                           : "coverage_unavailable";
            yyjson_mut_obj_add_str(doc, item, "status", scope_status);
            cbm_store_free_coverage(rows, row_count);
            yyjson_mut_arr_add_val(scope_results, item);
            scope_total += row_count;
            scope_returned += returned;
        }
    }
    yyjson_mut_obj_add_val(doc, root, "scopes", scope_results);
    yyjson_mut_obj_add_int(doc, root, "scope_total", scope_total);
    yyjson_mut_obj_add_int(doc, root, "scope_returned", scope_returned);
    yyjson_mut_obj_add_bool(doc, root, "scope_truncated", scope_returned < scope_total);
    yyjson_mut_obj_add_bool(doc, root, "has_more", scope_offset + scope_returned < scope_total);
    if (scope_offset + scope_returned < scope_total) {
        yyjson_mut_obj_add_int(doc, root, "next_offset", scope_offset + scope_returned);
    }
    yyjson_mut_obj_add_str(
        doc, root, "caveat",
        "Best-effort signal only. No recorded issue does not prove graph or source completeness; "
        "read flagged source and qualify claims when metadata is changed or unavailable.");

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(adoc);
    free(diagnostics);
    if (have_meta) {
        cbm_store_coverage_meta_clear(&meta);
    }
    if (have_project) {
        safe_str_free(&proj.name);
        safe_str_free(&proj.indexed_at);
        safe_str_free(&proj.root_path);
    }
    free(project);
    char *result = mcp_result_from_json(args, json);
    free(json);
    return result;
}

static char *handle_index_status(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    cbm_store_t *store = resolve_store(srv, project);
    REQUIRE_STORE(store, project);
    /* The git context block (worktree/shadow path variants) only matters when
     * debugging index-location issues — gate it so the common status call
     * stays lean. */
    bool verbose = cbm_mcp_get_bool_arg(args, "verbose");
    char *diagnostics = cbm_mcp_get_string_arg(args, "diagnostics");
    int coverage_samples = 0;
    if (diagnostics && strcmp(diagnostics, "summary") == 0) {
        coverage_samples = 5;
    } else if (diagnostics && strcmp(diagnostics, "full") == 0) {
        coverage_samples = COVERAGE_FILE_CAP;
    }
    free(diagnostics);

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
        cbm_project_t proj_info = {0};
        bool have_proj_info = cbm_store_get_project(store, project, &proj_info) == CBM_STORE_OK;
        if (have_proj_info) {
            yyjson_mut_obj_add_strcpy(doc, root, "root_path",
                                      proj_info.root_path ? proj_info.root_path : "");
            yyjson_mut_obj_add_strcpy(doc, root, "indexed_at",
                                      proj_info.indexed_at ? proj_info.indexed_at : "");
            if (verbose) {
                add_git_context_json(doc, root, proj_info.root_path);
            }
        }
        add_coverage_report(doc, root, store, project, have_proj_info ? proj_info.indexed_at : NULL,
                            coverage_samples);
        safe_str_free(&proj_info.name);
        safe_str_free(&proj_info.indexed_at);
        safe_str_free(&proj_info.root_path);
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

    char *result = mcp_result_from_json(args, json);
    free(json);
    return result;
}

/* delete_project: just erase the .db file (and WAL/SHM). */
static char *handle_delete_project(cbm_mcp_server_t *srv, const char *args) {
    char *name = get_project_arg(args);
    if (!name) {
        return cbm_mcp_text_result("project is required", true);
    }
    if (!mcp_project_mutation_begin(srv, name)) {
        free(name);
        return cbm_mcp_text_result("project operation cancelled or blocked by an active index",
                                   true);
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

    cbm_pipeline_unlock();

    if (srv->watcher) {
        cbm_watcher_unwatch(srv->watcher, name);
    }

    cbm_mem_collect(); /* return freed pages to OS after closing database */
    mcp_project_mutation_end(srv, name);

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

/* Canonical list of valid aspect tokens for get_architecture. Single source
 * of truth for the server-side validation (authoritative); the JSON-Schema
 * enum in the TOOLS entry above is the advisory client-side mirror — update
 * both together when the aspect set changes. */
static const char *VALID_ASPECTS[] = {"all",      "overview",   "structure", "dependencies",
                                      "routes",   "languages",  "packages",  "entry_points",
                                      "hotspots", "boundaries", "layers",    "file_tree",
                                      "clusters", "cycles",     NULL};

/* ── SCC / cycle condensation (get_architecture "cycles") ─────────
 * Iterative Tarjan over the CALLS call graph. Recursion would overflow on a
 * large graph, so the DFS state lives on explicit heap stacks. Reports the
 * strongly-connected components of size > 1 — the circular call dependencies,
 * the non-trivial content of the condensation quotient. */
typedef struct {
    int64_t *ids;  /* sorted unique node ids; index = position */
    int nverts;    /* |V| */
    int *adj_head; /* CSR row starts, length nverts+1 */
    int *adj;      /* CSR column (target vertex indices), length nedges */
    int nedges;    /* |E| within the vertex set */
} scc_graph_t;

static int scc_id_index(const int64_t *ids, int n, int64_t id) {
    int lo = 0;
    int hi = n - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (ids[mid] < id) {
            lo = mid + 1;
        } else if (ids[mid] > id) {
            hi = mid - 1;
        } else {
            return mid;
        }
    }
    return -1;
}

static int cmp_int64(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

/* Build the CSR call graph from parallel (src,tgt) edge arrays. Returns false
 * on OOM/empty. */
static bool scc_build(const int64_t *src, const int64_t *tgt, int ecount, scc_graph_t *g) {
    memset(g, 0, sizeof(*g));
    if (ecount <= 0) {
        return false;
    }
    int64_t *all = malloc((size_t)ecount * 2 * sizeof(int64_t));
    if (!all) {
        return false;
    }
    for (int i = 0; i < ecount; i++) {
        all[2 * i] = src[i];
        all[2 * i + 1] = tgt[i];
    }
    qsort(all, (size_t)ecount * 2, sizeof(int64_t), cmp_int64);
    int nv = 0;
    for (int i = 0; i < ecount * 2; i++) {
        if (i == 0 || all[i] != all[i - 1]) {
            all[nv++] = all[i];
        }
    }
    g->ids = all;
    g->nverts = nv;
    g->adj_head = calloc((size_t)nv + 1, sizeof(int));
    g->adj = malloc((size_t)ecount * sizeof(int));
    if (!g->adj_head || !g->adj) {
        free(g->ids);
        free(g->adj_head);
        free(g->adj);
        memset(g, 0, sizeof(*g));
        return false;
    }
    /* two-pass CSR fill. Every endpoint is in ids[] by construction (ids is
     * built from these same edge arrays), so a negative lookup is unreachable
     * -- but that invariant is invisible to path-sensitive analysis, and a
     * guard keeps a future collection bug from becoming an OOB write. Both
     * passes must skip identically or the cursors desync. */
    for (int i = 0; i < ecount; i++) {
        int u = scc_id_index(g->ids, nv, src[i]);
        int v = scc_id_index(g->ids, nv, tgt[i]);
        if (u < 0 || v < 0) {
            continue;
        }
        g->adj_head[u + 1]++;
    }
    for (int i = 0; i < nv; i++) {
        g->adj_head[i + 1] += g->adj_head[i];
    }
    int *cursor = malloc((size_t)nv * sizeof(int));
    if (!cursor) {
        free(g->ids);
        free(g->adj_head);
        free(g->adj);
        memset(g, 0, sizeof(*g));
        return false;
    }
    for (int i = 0; i < nv; i++) {
        cursor[i] = g->adj_head[i];
    }
    int filled = 0;
    for (int i = 0; i < ecount; i++) {
        int u = scc_id_index(g->ids, nv, src[i]);
        int v = scc_id_index(g->ids, nv, tgt[i]);
        if (u < 0 || v < 0) {
            continue;
        }
        g->adj[cursor[u]++] = v;
        filled++;
    }
    free(cursor);
    g->nedges = filled;
    return true;
}

static void scc_free(scc_graph_t *g) {
    free(g->ids);
    free(g->adj_head);
    free(g->adj);
    memset(g, 0, sizeof(*g));
}

/* Iterative Tarjan. Fills comp[v] with a component id; components are numbered
 * in discovery order. Returns the component count, or -1 on OOM. */
static int scc_tarjan(const scc_graph_t *g, int *comp) {
    enum { SCC_UNVISITED = -1 };
    int nv = g->nverts;
    int *index = malloc((size_t)nv * sizeof(int));
    int *low = malloc((size_t)nv * sizeof(int));
    bool *on_stack = calloc((size_t)nv, sizeof(bool));
    int *tstack = malloc((size_t)nv * sizeof(int)); /* Tarjan's node stack */
    int *dfs_v = malloc((size_t)nv * sizeof(int));  /* explicit DFS: vertex */
    int *dfs_i = malloc((size_t)nv * sizeof(int));  /* explicit DFS: adj cursor */
    if (!index || !low || !on_stack || !tstack || !dfs_v || !dfs_i) {
        free(index);
        free(low);
        free(on_stack);
        free(tstack);
        free(dfs_v);
        free(dfs_i);
        return -1;
    }
    for (int i = 0; i < nv; i++) {
        index[i] = SCC_UNVISITED;
        comp[i] = SCC_UNVISITED;
    }
    int counter = 0;
    int tsp = 0;   /* Tarjan stack pointer */
    int ncomp = 0; /* component id allocator */
    for (int s = 0; s < nv; s++) {
        if (index[s] != SCC_UNVISITED) {
            continue;
        }
        int dsp = 0; /* DFS stack pointer */
        dfs_v[dsp] = s;
        dfs_i[dsp] = g->adj_head[s];
        index[s] = low[s] = counter++;
        tstack[tsp++] = s;
        on_stack[s] = true;
        while (dsp >= 0) {
            int v = dfs_v[dsp];
            if (dfs_i[dsp] < g->adj_head[v + 1]) {
                int w = g->adj[dfs_i[dsp]++];
                if (index[w] == SCC_UNVISITED) {
                    index[w] = low[w] = counter++;
                    tstack[tsp++] = w;
                    on_stack[w] = true;
                    dsp++;
                    dfs_v[dsp] = w;
                    dfs_i[dsp] = g->adj_head[w];
                } else if (on_stack[w] && index[w] < low[v]) {
                    low[v] = index[w];
                }
            } else {
                /* v fully explored: it is a root iff low==index -> pop an SCC */
                if (low[v] == index[v]) {
                    int w;
                    do {
                        w = tstack[--tsp];
                        on_stack[w] = false;
                        comp[w] = ncomp;
                    } while (w != v);
                    ncomp++;
                }
                dsp--;
                if (dsp >= 0 && low[v] < low[dfs_v[dsp]]) {
                    low[dfs_v[dsp]] = low[v];
                }
            }
        }
    }
    free(index);
    free(low);
    free(on_stack);
    free(tstack);
    free(dfs_v);
    free(dfs_i);
    return ncomp;
}

static bool aspect_is_valid(const char *name) {
    if (!name) {
        return false;
    }
    for (int i = 0; VALID_ASPECTS[i]; i++) {
        if (strcmp(name, VALID_ASPECTS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Check if an aspect is requested. NULL aspects = all. The array can contain
 * "all" (everything), "overview" (everything except file_tree — see
 * cbm_store_arch_aspect_in_overview in store.c), or the aspect name itself. */
/* True ONLY when `name` is explicitly present in the aspects array — never via
 * the no-filter default, "all", or "overview". For expensive opt-in aspects
 * (cycles scans the whole call graph) that must not run on a bare call. */
static bool aspect_explicitly_named(yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return false;
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (s && strcmp(s, name) == 0) {
            return true;
        }
    }
    return false;
}

static bool aspect_wanted(yyjson_doc *aspects_doc, yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return true; /* no filter = all */
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (!s) {
            continue;
        }
        if (strcmp(s, "all") == 0) {
            return true;
        }
        if (strcmp(s, "overview") == 0 && cbm_store_arch_aspect_in_overview(name)) {
            return true;
        }
        if (strcmp(s, name) == 0) {
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

/* Join a complete string list with ';' separators. The caller owns the result;
 * allocation failure is explicit rather than a partial graph identifier. */
static char *arch_join_list(const char **items, int n) {
    cbm_sb_t joined;
    cbm_sb_init(&joined);
    for (int i = 0; i < n; i++) {
        const char *s = items[i] ? items[i] : "";
        if (i > 0) {
            cbm_sb_append(&joined, ";");
        }
        cbm_sb_append(&joined, s);
    }
    return cbm_sb_finish(&joined);
}

/* Compute the circular-dependency SCCs (size > 1) of the CALLS graph. Returns
 * a malloc'd array of components, each a malloc'd int64 array of member node
 * ids, with sizes in *out_sizes and count in *out_ncycles; sets *scanned_edges
 * and *edges_truncated. Caller frees each component + the arrays. Returns
 * CBM_STORE_OK, or CBM_STORE_ERR on failure (all outs zeroed). */
enum { ARCH_SCC_MAX_EDGES = 400000, ARCH_SCC_MAX_CYCLES = 100, ARCH_SCC_MEMBERS_SHOWN = 20 };

static int arch_compute_cycles(cbm_store_t *store, const char *project, int64_t ***out_members,
                               int **out_sizes, int *out_ncycles, int *out_total_cycles,
                               int *scanned_edges, bool *edges_truncated) {
    *out_members = NULL;
    *out_sizes = NULL;
    *out_ncycles = 0;
    *out_total_cycles = 0;
    *scanned_edges = 0;
    *edges_truncated = false;

    int64_t *src = NULL;
    int64_t *tgt = NULL;
    int ecount = 0;
    if (cbm_store_fetch_call_edges(store, project, ARCH_SCC_MAX_EDGES, &src, &tgt, &ecount,
                                   edges_truncated) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    *scanned_edges = ecount;
    scc_graph_t g;
    if (!scc_build(src, tgt, ecount, &g)) {
        free(src);
        free(tgt);
        return CBM_STORE_OK; /* no edges = no cycles, not an error */
    }
    free(src);
    free(tgt);

    int *comp = malloc((size_t)g.nverts * sizeof(int));
    if (!comp) {
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    int ncomp = scc_tarjan(&g, comp);
    if (ncomp < 0) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    /* Tarjan assigns every vertex a component, so ncomp >= 1 whenever
     * nverts >= 1 -- state it, so the zero-size-allocation path below is
     * provably confined to the empty graph. */
    if (g.nverts > 0 && ncomp <= 0) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    /* size per component */
    int *csize = calloc((size_t)ncomp, sizeof(int));
    if (!csize) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    for (int v = 0; v < g.nverts; v++) {
        csize[comp[v]]++;
    }
    /* collect components with size > 1 (the cycles) */
    int ncyc = 0;
    for (int c = 0; c < ncomp; c++) {
        if (csize[c] > 1) {
            ncyc++;
        }
    }
    *out_total_cycles = ncyc; /* the TRUE count, before the display clamp */
    if (ncyc > ARCH_SCC_MAX_CYCLES) {
        ncyc = ARCH_SCC_MAX_CYCLES;
    }
    int64_t **members = ncyc > 0 ? calloc((size_t)ncyc, sizeof(int64_t *)) : NULL;
    int *sizes = ncyc > 0 ? calloc((size_t)ncyc, sizeof(int)) : NULL;
    /* map component id -> output slot (only for the first ARCH_SCC_MAX_CYCLES
     * size>1 comps, in component-id order) */
    int *slot = malloc((size_t)ncomp * sizeof(int));
    if ((ncyc > 0 && (!members || !sizes)) || !slot) {
        free(members);
        free(sizes);
        free(slot);
        free(csize);
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    int next_slot = 0;
    for (int c = 0; c < ncomp; c++) {
        if (csize[c] > 1 && next_slot < ncyc) {
            slot[c] = next_slot;
            sizes[next_slot] = csize[c];
            members[next_slot] = malloc((size_t)csize[c] * sizeof(int64_t));
            next_slot++;
        } else {
            slot[c] = -1;
        }
    }
    int *fill = calloc((size_t)ncyc, sizeof(int));
    if (ncyc > 0 && !fill) {
        for (int i = 0; i < ncyc; i++) {
            free(members[i]);
        }
        free(members);
        free(sizes);
        free(slot);
        free(csize);
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    /* With ncyc == 0 no slot is ever >= 0 (the slot loop can't assign one),
     * so members -- NULL in that case -- is never indexed; make that
     * invariant local instead of cross-loop so it is checkable. */
    if (ncyc > 0) {
        for (int v = 0; v < g.nverts; v++) {
            int sl = slot[comp[v]];
            if (sl >= 0) {
                members[sl][fill[sl]++] = g.ids[v];
            }
        }
    }
    free(fill);
    free(slot);
    free(csize);
    free(comp);
    scc_free(&g);
    *out_members = members;
    *out_sizes = sizes;
    *out_ncycles = ncyc;
    return CBM_STORE_OK;
}

/* Begin a json-tree {cols, rows} section under `key` on root; returns the
 * rows array for the caller to fill with column-ordered row arrays. */
static yyjson_mut_val *arch_json_section(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *key,
                                         const char *const *cols, int ncols) {
    yyjson_mut_val *sec = yyjson_mut_obj(doc);
    yyjson_mut_val *c = yyjson_mut_arr(doc);
    for (int i = 0; i < ncols; i++) {
        yyjson_mut_arr_add_str(doc, c, cols[i]);
    }
    yyjson_mut_obj_add_val(doc, sec, "cols", c);
    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, sec, "rows", rows);
    yyjson_mut_obj_add_val(doc, root, key, sec);
    return rows;
}

/* Fetch the complete qualified_name for a node id, or a "#<id>" fallback. */
static char *arch_node_qn(cbm_store_t *store, int64_t id) {
    cbm_node_t n = {0};
    char *out = NULL;
    if (cbm_store_find_node_by_id(store, id, &n) == CBM_STORE_OK && n.qualified_name) {
        out = heap_strdup(n.qualified_name);
    } else {
        char fallback[48];
        snprintf(fallback, sizeof(fallback), "#%lld", (long long)id);
        out = heap_strdup(fallback);
    }
    free_node_contents(&n);
    return out;
}

static char *handle_get_architecture(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    cbm_store_t *store = resolve_store(srv, project);
    /* REQUIRE_STORE returns without freeing anything but `project`, so every
     * other allocation must come after it (scope_path leaked here before —
     * caught by the clang-analyzer unix.Malloc lane). */
    REQUIRE_STORE(store, project);
    char *scope_path = cbm_mcp_get_string_arg(args, "path");

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(scope_path);
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

    /* Server-side validation: reject unknown aspect tokens with an isError
     * result listing the valid values. The JSON-Schema enum is advisory —
     * many MCP clients do not validate arguments against tool schemas — so
     * without this check a typo degraded to a silent near-empty payload. */
    for (int i = 0; i < aspects_strs_count; i++) {
        if (!aspect_is_valid(aspects_strs[i])) {
            char valid_list[CBM_SZ_256];
            size_t off = 0;
            for (int j = 0; VALID_ASPECTS[j] && off < sizeof(valid_list); j++) {
                int n = snprintf(valid_list + off, sizeof(valid_list) - off, "%s%s",
                                 j > 0 ? ", " : "", VALID_ASPECTS[j]);
                if (n < 0) {
                    break;
                }
                off += (size_t)n;
            }
            char msg[CBM_SZ_512];
            snprintf(msg, sizeof(msg), "Unknown aspect '%s'. Valid: %s.", aspects_strs[i],
                     valid_list);
            char *err = cbm_mcp_text_result(msg, true);
            free(project);
            free(scope_path);
            if (aspects_doc) {
                yyjson_doc_free(aspects_doc);
            }
            return err;
        }
    }

    /* Default (no aspects) = compact summary. The old default rendered ALL
     * aspects including the full file_tree — ~94KB (~23K tokens) on a
     * mid-size repo, a context bomb for the LLM consumers. Explicit
     * aspects (or ["all"]) keep full access to every section. */
    bool default_summary = false;
    if (aspects_strs_count == 0) {
        /* NOT "overview" — that means everything-except-file_tree. Totals and
         * node_labels/edge_types counts are always emitted alongside. */
        aspects_strs[aspects_strs_count++] = "languages";
        aspects_strs[aspects_strs_count++] = "packages";
        aspects_strs[aspects_strs_count++] = "entry_points";
        default_summary = true;
    }

    cbm_schema_info_t schema = {0};
    /* Counts-only: this handler renders label/type counts but never property
     * keys, and full key discovery json_each-scans every row (seconds-to-
     * minutes on multi-million-node graphs). */
    cbm_store_get_schema_counts_scoped(store, project, scope_path, &schema);

    cbm_architecture_info_t arch = {0};
    cbm_store_get_architecture(store, project, scope_path, aspects_strs, aspects_strs_count, &arch);

    int node_count = cbm_store_count_nodes_scoped(store, project, scope_path);
    int edge_count = cbm_store_count_edges_scoped(store, project, scope_path);
    size_t norm_path_cap = scope_path ? strlen(scope_path) + 2U : 2U;
    char *norm_path = malloc(norm_path_cap);
    bool path_scoped =
        norm_path && cbm_store_normalize_arch_path(scope_path, norm_path, norm_path_cap);

    /* Response encoding: tree tables by default; format:"json" emits the
     * same model as structured JSON ({cols, rows} per section). */
    char *arch_format = cbm_mcp_get_string_arg(args, "format");
    bool arch_legacy_json = arch_format && strcmp(arch_format, "json") == 0;
    free(arch_format);

    if (!arch_legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        if (project) {
            cbm_tree_scalar_str(&sb, "project", project);
        }
        if (default_summary) {
            cbm_tree_scalar_str(&sb, "aspects_hint",
                                "Summary view (default). More on request via aspects:[...] — "
                                "structure, dependencies, routes, hotspots, boundaries, layers, "
                                "clusters, file_tree — or [\"all\"] for everything.");
        }
        if (path_scoped) {
            cbm_tree_scalar_str(&sb, "path", norm_path);
            cbm_tree_scalar_int(&sb, "root_total_nodes", cbm_store_count_nodes(store, project));
            cbm_tree_scalar_int(&sb, "root_total_edges", cbm_store_count_edges(store, project));
            cbm_tree_scalar_int(&sb, "scoped_total_nodes", node_count);
            cbm_tree_scalar_int(&sb, "scoped_total_edges", edge_count);
        }
        cbm_tree_scalar_int(&sb, "total_nodes", node_count);
        cbm_tree_scalar_int(&sb, "total_edges", edge_count);

        if (aspect_wanted(aspects_doc, aspects_arr, "structure") && schema.node_label_count > 0) {
            static const char *const lcols[] = {"label", "count"};
            cbm_tree_table_header(&sb, "node_labels", schema.node_label_count, lcols, 2);
            for (int i = 0; i < schema.node_label_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, schema.node_labels[i].label, true);
                cbm_tree_cell_int(&sb, schema.node_labels[i].count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (aspect_wanted(aspects_doc, aspects_arr, "dependencies") && schema.edge_type_count > 0) {
            static const char *const tcols[] = {"type", "count"};
            cbm_tree_table_header(&sb, "edge_types", schema.edge_type_count, tcols, 2);
            for (int i = 0; i < schema.edge_type_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, schema.edge_types[i].type, true);
                cbm_tree_cell_int(&sb, schema.edge_types[i].count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (aspect_wanted(aspects_doc, aspects_arr, "routes") && schema.rel_pattern_count > 0) {
            static const char *const pcols[] = {"pattern"};
            cbm_tree_table_header(&sb, "relationship_patterns", schema.rel_pattern_count, pcols, 1);
            for (int i = 0; i < schema.rel_pattern_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, schema.rel_patterns[i], true);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.language_count > 0) {
            static const char *const gcols[] = {"language", "files"};
            cbm_tree_table_header(&sb, "languages", arch.language_count, gcols, 2);
            for (int i = 0; i < arch.language_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.languages[i].language, true);
                cbm_tree_cell_int(&sb, arch.languages[i].file_count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.package_count > 0) {
            static const char *const kcols[] = {"name", "nodes", "fan_in", "fan_out"};
            cbm_tree_table_header(&sb, "packages", arch.package_count, kcols, 4);
            for (int i = 0; i < arch.package_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.packages[i].name, true);
                cbm_tree_cell_int(&sb, arch.packages[i].node_count, false);
                cbm_tree_cell_int(&sb, arch.packages[i].fan_in, false);
                cbm_tree_cell_int(&sb, arch.packages[i].fan_out, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.entry_point_count > 0) {
            /* qn only — `name` is its last segment. */
            static const char *const ecols[] = {"qn", "file"};
            cbm_tree_table_header(&sb, "entry_points", arch.entry_point_count, ecols, 2);
            for (int i = 0; i < arch.entry_point_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.entry_points[i].qualified_name, true);
                cbm_tree_cell_str(&sb, arch.entry_points[i].file, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.route_count > 0) {
            static const char *const rcols[] = {"method", "path", "handler"};
            cbm_tree_table_header(&sb, "routes", arch.route_count, rcols, 3);
            for (int i = 0; i < arch.route_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.routes[i].method, true);
                cbm_tree_cell_str(&sb, arch.routes[i].path, false);
                cbm_tree_cell_str(&sb, arch.routes[i].handler, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.hotspot_count > 0) {
            static const char *const hcols[] = {"qn", "fan_in"};
            cbm_tree_table_header(&sb, "hotspots", arch.hotspot_count, hcols, 2);
            for (int i = 0; i < arch.hotspot_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.hotspots[i].qualified_name, true);
                cbm_tree_cell_int(&sb, arch.hotspots[i].fan_in, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.boundary_count > 0) {
            static const char *const bcols[] = {"from", "to", "calls"};
            cbm_tree_table_header(&sb, "boundaries", arch.boundary_count, bcols, 3);
            for (int i = 0; i < arch.boundary_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.boundaries[i].from, true);
                cbm_tree_cell_str(&sb, arch.boundaries[i].to, false);
                cbm_tree_cell_int(&sb, arch.boundaries[i].call_count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.service_count > 0) {
            static const char *const scols[] = {"from", "to", "type", "count"};
            cbm_tree_table_header(&sb, "services", arch.service_count, scols, 4);
            for (int i = 0; i < arch.service_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.services[i].from, true);
                cbm_tree_cell_str(&sb, arch.services[i].to, false);
                cbm_tree_cell_str(&sb, arch.services[i].type, false);
                cbm_tree_cell_int(&sb, arch.services[i].count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.layer_count > 0) {
            static const char *const ycols[] = {"name", "layer", "reason"};
            cbm_tree_table_header(&sb, "layers", arch.layer_count, ycols, 3);
            for (int i = 0; i < arch.layer_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.layers[i].name, true);
                cbm_tree_cell_str(&sb, arch.layers[i].layer, false);
                cbm_tree_cell_str(&sb, arch.layers[i].reason, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.cluster_count > 0) {
            /* Nested lists become ';'-joined cells. */
            static const char *const ccols[] = {"id",        "label",    "members",   "cohesion",
                                                "top_nodes", "packages", "edge_types"};
            cbm_tree_table_header(&sb, "clusters", arch.cluster_count, ccols, 7);
            for (int i = 0; i < arch.cluster_count; i++) {
                const cbm_cluster_info_t *c = &arch.clusters[i];
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_int(&sb, c->id, true);
                cbm_tree_cell_str(&sb, c->label, false);
                cbm_tree_cell_int(&sb, c->members, false);
                cbm_tree_cell_real(&sb, c->cohesion, false);
                char *top_nodes = arch_join_list(c->top_nodes, c->top_node_count);
                char *packages = arch_join_list(c->packages, c->package_count);
                char *edge_types = arch_join_list(c->edge_types, c->edge_type_count);
                cbm_tree_cell_str(&sb, top_nodes ? top_nodes : "(out-of-memory)", false);
                cbm_tree_cell_str(&sb, packages ? packages : "(out-of-memory)", false);
                cbm_tree_cell_str(&sb, edge_types ? edge_types : "(out-of-memory)", false);
                cbm_tree_row_end(&sb);
                free(top_nodes);
                free(packages);
                free(edge_types);
            }
        }
        if (arch.file_tree_count > 0) {
            static const char *const fcols[] = {"path", "type", "children"};
            const bool string_cols[] = {true, true, false};
            size_t count = (size_t)arch.file_tree_count;
            const char **cells = count <= SIZE_MAX / (3U * sizeof(*cells))
                                     ? malloc(count * 3U * sizeof(*cells))
                                     : NULL;
            char (*child_values)[32] = count <= SIZE_MAX / sizeof(*child_values)
                                           ? malloc(count * sizeof(*child_values))
                                           : NULL;
            if (cells && child_values) {
                for (int i = 0; i < arch.file_tree_count; i++) {
                    snprintf(child_values[i], sizeof(child_values[i]), "%d",
                             arch.file_tree[i].children);
                    cells[(size_t)i * 3U] = arch.file_tree[i].path;
                    cells[(size_t)i * 3U + 1U] = arch.file_tree[i].type;
                    cells[(size_t)i * 3U + 2U] = child_values[i];
                }
                static const bool prefix_cols[] = {true, false, false};
                cbm_tree_table_rows_profiled(&sb, "file_tree", arch.file_tree_count, fcols, 3,
                                             cells, string_cols, prefix_cols);
            } else {
                cbm_tree_table_header(&sb, "file_tree", arch.file_tree_count, fcols, 3);
                for (int i = 0; i < arch.file_tree_count; i++) {
                    cbm_tree_row_begin(&sb);
                    cbm_tree_cell_str(&sb, arch.file_tree[i].path, true);
                    cbm_tree_cell_str(&sb, arch.file_tree[i].type, false);
                    cbm_tree_cell_int(&sb, arch.file_tree[i].children, false);
                    cbm_tree_row_end(&sb);
                }
            }
            free(child_values);
            free(cells);
        }
        /* Cross-repo edge summary (mirrors append_cross_repo_summary). */
        {
            static const char *const cross_types[] = {"CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS",
                                                      "CROSS_CHANNEL",       "CROSS_GRPC_CALLS",
                                                      "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
            int cross_total = 0;
            for (int t = 0; t < (int)(sizeof(cross_types) / sizeof(cross_types[0])); t++) {
                for (int i = 0; i < schema.edge_type_count; i++) {
                    if (strcmp(schema.edge_types[i].type, cross_types[t]) == 0) {
                        cross_total += schema.edge_types[i].count;
                        break;
                    }
                }
            }
            if (cross_total > 0) {
                cbm_tree_scalar_int(&sb, "cross_repo_links_total", cross_total);
            }
        }

        /* cycles: circular CALLS dependencies (SCCs of size > 1) — opt-in, it
         * scans the whole call graph. A quotient/condensation view. */
        if (aspect_explicitly_named(aspects_arr, "cycles")) {
            int64_t **members = NULL;
            int *sizes = NULL;
            int ncyc = 0;
            int total_cyc = 0;
            int scanned = 0;
            bool etrunc = false;
            if (arch_compute_cycles(store, project, &members, &sizes, &ncyc, &total_cyc, &scanned,
                                    &etrunc) == CBM_STORE_OK) {
                cbm_tree_scalar_int(&sb, "call_edges_scanned", scanned);
                cbm_tree_scalar_int(&sb, "cycles_total", total_cyc);
                if (etrunc) {
                    cbm_tree_scalar_bool(&sb, "cycles_partial", true);
                    cbm_tree_scalar_str(&sb, "cycles_hint",
                                        "call graph exceeded the scan budget; cycle list may be "
                                        "incomplete");
                }
                if (total_cyc > ncyc) {
                    char omit[CBM_SZ_128];
                    snprintf(omit, sizeof(omit), "cycles_omitted: %d  (showing the first %d)\n",
                             total_cyc - ncyc, ncyc);
                    cbm_sb_append(&sb, omit);
                }
                char hdr[CBM_SZ_128];
                snprintf(hdr, sizeof(hdr),
                         "cycles: %d  (rows: size members; circular CALLS dependencies)\n", ncyc);
                cbm_sb_append(&sb, hdr);
                for (int c = 0; c < ncyc; c++) {
                    bool clipped = sizes[c] > ARCH_SCC_MEMBERS_SHOWN;
                    int show = clipped ? ARCH_SCC_MEMBERS_SHOWN : sizes[c];
                    cbm_sb_t member_list;
                    cbm_sb_init(&member_list);
                    for (int m = 0; m < show; m++) {
                        char *qn = arch_node_qn(store, members[c][m]);
                        if (m > 0) {
                            cbm_sb_append(&member_list, ";");
                        }
                        cbm_sb_append(&member_list, qn ? qn : "(out-of-memory)");
                        free(qn);
                    }
                    if (clipped) {
                        char omitted[48];
                        snprintf(omitted, sizeof(omitted), ";+%d", sizes[c] - show);
                        cbm_sb_append(&member_list, omitted);
                    }
                    char *member_text = cbm_sb_finish(&member_list);
                    cbm_tree_row_begin(&sb);
                    cbm_tree_cell_int(&sb, sizes[c], true);
                    cbm_tree_cell_str(&sb, member_text ? member_text : "(out-of-memory)", false);
                    cbm_tree_row_end(&sb);
                    free(member_text);
                    free(members[c]);
                }
                free(members);
                free(sizes);
            }
        }

        cbm_store_architecture_free(&arch);
        cbm_store_schema_free(&schema);
        if (aspects_doc) {
            yyjson_doc_free(aspects_doc);
        }
        free(project);
        free(scope_path);
        free(norm_path);
        char *text = cbm_sb_finish(&sb);
        char *result = cbm_mcp_text_result(text ? text : "out of memory", text == NULL);
        free(text);
        return result;
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (project) {
        yyjson_mut_obj_add_str(doc, root, "project", project);
    }
    if (default_summary) {
        yyjson_mut_obj_add_str(doc, root, "aspects_hint",
                               "Summary view (default). More on request via aspects:[...] — "
                               "structure, dependencies, routes, hotspots, boundaries, layers, "
                               "clusters, file_tree — or [\"all\"] for everything.");
    }
    if (path_scoped) {
        yyjson_mut_obj_add_str(doc, root, "path", norm_path);
        int root_nodes = cbm_store_count_nodes(store, project);
        int root_edges = cbm_store_count_edges(store, project);
        yyjson_mut_obj_add_int(doc, root, "root_total_nodes", root_nodes);
        yyjson_mut_obj_add_int(doc, root, "root_total_edges", root_edges);
        yyjson_mut_obj_add_int(doc, root, "scoped_total_nodes", node_count);
        yyjson_mut_obj_add_int(doc, root, "scoped_total_edges", edge_count);
    }
    yyjson_mut_obj_add_int(doc, root, "total_nodes", node_count);
    yyjson_mut_obj_add_int(doc, root, "total_edges", edge_count);

    /* Every section below is the json-tree model: {cols, rows[[...]]} —
     * mirrors the text tree tables one-for-one. */
    if (aspect_wanted(aspects_doc, aspects_arr, "structure")) {
        static const char *const lcols[] = {"label", "count"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "node_labels", lcols, 2);
        for (int i = 0; i < schema.node_label_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, schema.node_labels[i].label);
            yyjson_mut_arr_add_int(doc, row, schema.node_labels[i].count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (aspect_wanted(aspects_doc, aspects_arr, "dependencies")) {
        static const char *const tcols[] = {"type", "count"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "edge_types", tcols, 2);
        for (int i = 0; i < schema.edge_type_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, schema.edge_types[i].type);
            yyjson_mut_arr_add_int(doc, row, schema.edge_types[i].count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    /* Relationship patterns (a plain list — no columns to factor) */
    if (aspect_wanted(aspects_doc, aspects_arr, "routes") && schema.rel_pattern_count > 0) {
        yyjson_mut_val *pats = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            yyjson_mut_arr_add_str(doc, pats, schema.rel_patterns[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "relationship_patterns", pats);
    }

    if (arch.language_count > 0) {
        static const char *const gcols[] = {"language", "files"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "languages", gcols, 2);
        for (int i = 0; i < arch.language_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.languages[i].language ? arch.languages[i].language : "");
            yyjson_mut_arr_add_int(doc, row, arch.languages[i].file_count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.package_count > 0) {
        static const char *const kcols[] = {"name", "nodes", "fan_in", "fan_out"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "packages", kcols, 4);
        for (int i = 0; i < arch.package_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.packages[i].name ? arch.packages[i].name : "");
            yyjson_mut_arr_add_int(doc, row, arch.packages[i].node_count);
            yyjson_mut_arr_add_int(doc, row, arch.packages[i].fan_in);
            yyjson_mut_arr_add_int(doc, row, arch.packages[i].fan_out);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.entry_point_count > 0) {
        static const char *const ecols[] = {"qn", "file"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "entry_points", ecols, 2);
        for (int i = 0; i < arch.entry_point_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(
                doc, row,
                arch.entry_points[i].qualified_name ? arch.entry_points[i].qualified_name : "");
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.entry_points[i].file ? arch.entry_points[i].file : "");
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.route_count > 0) {
        static const char *const rcols[] = {"method", "path", "handler"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "routes", rcols, 3);
        for (int i = 0; i < arch.route_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.routes[i].method ? arch.routes[i].method : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.routes[i].path ? arch.routes[i].path : "");
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.routes[i].handler ? arch.routes[i].handler : "");
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.hotspot_count > 0) {
        static const char *const hcols[] = {"qn", "fan_in"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "hotspots", hcols, 2);
        for (int i = 0; i < arch.hotspot_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(
                doc, row, arch.hotspots[i].qualified_name ? arch.hotspots[i].qualified_name : "");
            yyjson_mut_arr_add_int(doc, row, arch.hotspots[i].fan_in);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.boundary_count > 0) {
        static const char *const bcols[] = {"from", "to", "calls"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "boundaries", bcols, 3);
        for (int i = 0; i < arch.boundary_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.boundaries[i].from ? arch.boundaries[i].from : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.boundaries[i].to ? arch.boundaries[i].to : "");
            yyjson_mut_arr_add_int(doc, row, arch.boundaries[i].call_count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.service_count > 0) {
        static const char *const scols[] = {"from", "to", "type", "count"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "services", scols, 4);
        for (int i = 0; i < arch.service_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.services[i].from ? arch.services[i].from : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.services[i].to ? arch.services[i].to : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.services[i].type ? arch.services[i].type : "");
            yyjson_mut_arr_add_int(doc, row, arch.services[i].count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.layer_count > 0) {
        static const char *const ycols[] = {"name", "layer", "reason"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "layers", ycols, 3);
        for (int i = 0; i < arch.layer_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.layers[i].name ? arch.layers[i].name : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.layers[i].layer ? arch.layers[i].layer : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.layers[i].reason ? arch.layers[i].reason : "");
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.cluster_count > 0) {
        /* Nested lists stay nested arrays within the row (tree-json). */
        static const char *const ccols[] = {"id",        "label",    "members",   "cohesion",
                                            "top_nodes", "packages", "edge_types"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "clusters", ccols, 7);
        for (int i = 0; i < arch.cluster_count; i++) {
            const cbm_cluster_info_t *c = &arch.clusters[i];
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_int(doc, row, c->id);
            yyjson_mut_arr_add_strcpy(doc, row, c->label ? c->label : "");
            yyjson_mut_arr_add_int(doc, row, c->members);
            yyjson_mut_arr_add_real(doc, row, c->cohesion);
            yyjson_mut_val *top = yyjson_mut_arr(doc);
            for (int j = 0; j < c->top_node_count; j++) {
                yyjson_mut_arr_add_str(doc, top, c->top_nodes[j] ? c->top_nodes[j] : "");
            }
            yyjson_mut_arr_add_val(row, top);
            yyjson_mut_val *pkgs = yyjson_mut_arr(doc);
            for (int j = 0; j < c->package_count; j++) {
                yyjson_mut_arr_add_str(doc, pkgs, c->packages[j] ? c->packages[j] : "");
            }
            yyjson_mut_arr_add_val(row, pkgs);
            yyjson_mut_val *etypes = yyjson_mut_arr(doc);
            for (int j = 0; j < c->edge_type_count; j++) {
                yyjson_mut_arr_add_str(doc, etypes, c->edge_types[j] ? c->edge_types[j] : "");
            }
            yyjson_mut_arr_add_val(row, etypes);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.file_tree_count > 0) {
        static const char *const fcols[] = {"path", "type", "children"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "file_tree", fcols, 3);
        for (int i = 0; i < arch.file_tree_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.file_tree[i].path ? arch.file_tree[i].path : "");
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.file_tree[i].type ? arch.file_tree[i].type : "");
            yyjson_mut_arr_add_int(doc, row, arch.file_tree[i].children);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    append_cross_repo_summary(doc, root, &schema);

    /* cycles: SCCs of size > 1 in the CALLS graph (same model as tree). */
    if (aspect_explicitly_named(aspects_arr, "cycles")) {
        int64_t **members = NULL;
        int *sizes = NULL;
        int ncyc = 0;
        int total_cyc = 0;
        int scanned = 0;
        bool etrunc = false;
        if (arch_compute_cycles(store, project, &members, &sizes, &ncyc, &total_cyc, &scanned,
                                &etrunc) == CBM_STORE_OK) {
            yyjson_mut_obj_add_int(doc, root, "call_edges_scanned", scanned);
            yyjson_mut_obj_add_int(doc, root, "cycles_total", total_cyc);
            yyjson_mut_obj_add_bool(doc, root, "cycles_partial", etrunc);
            yyjson_mut_val *cyc = yyjson_mut_arr(doc);
            for (int c = 0; c < ncyc; c++) {
                yyjson_mut_val *o = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_int(doc, o, "size", sizes[c]);
                yyjson_mut_val *mem = yyjson_mut_arr(doc);
                int show = sizes[c] < ARCH_SCC_MEMBERS_SHOWN ? sizes[c] : ARCH_SCC_MEMBERS_SHOWN;
                for (int m = 0; m < show; m++) {
                    char *qn = arch_node_qn(store, members[c][m]);
                    yyjson_mut_arr_add_strcpy(doc, mem, qn ? qn : "(out-of-memory)");
                    free(qn);
                }
                yyjson_mut_obj_add_val(doc, o, "members", mem);
                yyjson_mut_arr_add_val(cyc, o);
                free(members[c]);
            }
            yyjson_mut_obj_add_val(doc, root, "cycles", cyc);
            free(members);
            free(sizes);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_architecture_free(&arch);
    cbm_store_schema_free(&schema);
    if (aspects_doc) {
        yyjson_doc_free(aspects_doc);
    }
    free(project);
    free(scope_path);
    free(norm_path);

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
    static const char *mode_cross_svc[] = {
        "HTTP_CALLS",          "ASYNC_CALLS",       "DATA_FLOWS",    "CALLS",
        "CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS", "CROSS_CHANNEL", "CROSS_GRPC_CALLS",
        "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};

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
        n_defaults = (int)(sizeof(mode_cross_svc) / sizeof(mode_cross_svc[0]));
    }
    for (int i = 0; i < n_defaults; i++) {
        out_types[i] = defaults[i];
    }
    *out_count = n_defaults;
    return NULL;
}

/* Check if a file path looks like a test file. The substring checks below
 * only catch a tests/ directory nested under another path component
 * (".../tests/foo"); a project-root-relative path like "tests/repro/foo.c"
 * has no leading slash before "tests" and fell through undetected, leaking
 * whole test subtrees into query_graph/trace_path results with the default
 * include_tests=false (#1294). */
static bool is_test_file(const char *path) {
    if (!path) {
        return false;
    }
    return strstr(path, "/test") != NULL || strstr(path, "test_") != NULL ||
           strstr(path, "_test.") != NULL || strstr(path, "/tests/") != NULL ||
           strstr(path, "/spec/") != NULL || strstr(path, ".test.") != NULL ||
           strncmp(path, "tests/", SLEN("tests/")) == 0 ||
           strncmp(path, "test/", SLEN("test/")) == 0 ||
           strncmp(path, "spec/", SLEN("spec/")) == 0 ||
           strncmp(path, "__tests__/", SLEN("__tests__/")) == 0;
}

/* Filtering belongs before page-window calculation: hidden test rows must not
 * consume the caller's visible limit or become cursor watermarks. Compact the
 * owned traversal array in place while preserving canonical (hop,id) order. */
static void trace_filter_test_rows(cbm_traverse_result_t *tr) {
    int write_index = 0;
    for (int read_index = 0; read_index < tr->visited_count; read_index++) {
        if (is_test_file(tr->visited[read_index].node.file_path)) {
            cbm_node_free_fields(&tr->visited[read_index].node);
            continue;
        }
        if (write_index != read_index) {
            tr->visited[write_index] = tr->visited[read_index];
            memset(&tr->visited[read_index], 0, sizeof(tr->visited[read_index]));
        }
        write_index++;
    }
    tr->visited_count = write_index;
}

typedef struct {
    const cbm_traverse_result_t *full;
    const cbm_node_t *roots;
    int root_count;
    bool inbound;
} trace_edge_context_t;

static bool trace_root_contains(const trace_edge_context_t *ctx, int64_t node_id) {
    for (int i = 0; ctx && i < ctx->root_count; i++) {
        if (ctx->roots[i].id == node_id) {
            return true;
        }
    }
    return false;
}

static int trace_hop_for_node(const trace_edge_context_t *ctx, int64_t node_id) {
    if (!ctx || !ctx->full) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < ctx->full->visited_count; i++) {
        if (ctx->full->visited[i].node.id == node_id) {
            return ctx->full->visited[i].hop;
        }
    }
    return CBM_NOT_FOUND;
}

/* Edge collection returns the whole induced subgraph, not a predecessor tree.
 * An arbitrary incident edge can therefore point sideways or away from the
 * root. Select a deterministic edge that can actually precede this shortest-
 * path row: it must have the traversal direction and connect hop h to h-1 (or
 * one of the resolved roots for hop 1). */
static const cbm_edge_info_t *trace_predecessor_edge(const trace_edge_context_t *ctx,
                                                     const cbm_node_hop_t *hop_node) {
    if (!ctx || !ctx->full || !hop_node || hop_node->hop <= 0) {
        return NULL;
    }
    const cbm_edge_info_t *best = NULL;
    int64_t best_predecessor = INT64_MAX;
    for (int e = 0; e < ctx->full->edge_count; e++) {
        const cbm_edge_info_t *edge = &ctx->full->edges[e];
        if ((ctx->inbound && edge->source_id != hop_node->node.id) ||
            (!ctx->inbound && edge->target_id != hop_node->node.id)) {
            continue;
        }
        int64_t predecessor = ctx->inbound ? edge->target_id : edge->source_id;
        bool previous_hop = hop_node->hop == 1
                                ? trace_root_contains(ctx, predecessor)
                                : trace_hop_for_node(ctx, predecessor) == hop_node->hop - 1;
        if (!previous_hop) {
            continue;
        }
        const char *edge_type = edge->type ? edge->type : "";
        const char *best_type = best && best->type ? best->type : "";
        if (!best || predecessor < best_predecessor ||
            (predecessor == best_predecessor && strcmp(edge_type, best_type) < 0)) {
            best = edge;
            best_predecessor = predecessor;
        }
    }
    return best;
}

/* Return the serialized argument-expression array from the selected canonical
 * predecessor edge. The returned slice is borrowed from properties_json. */
static const char *trace_edge_args(const cbm_edge_info_t *edge, size_t *out_len) {
    const char *pj = edge ? edge->properties_json : NULL;
    const char *args = pj ? strstr(pj, "\"args\"") : NULL;
    const char *open = args ? strchr(args, '[') : NULL;
    if (!open) {
        return NULL;
    }
    int depth = 0;
    const char *p = open;
    for (; *p; p++) {
        if (*p == '[') {
            depth++;
        } else if (*p == ']') {
            depth--;
            if (depth == 0) {
                p++;
                break;
            }
        }
    }
    *out_len = (size_t)(p - open);
    return open;
}

/* Classify a resolver strategy into the CLOSED public vocabulary.
 *
 * The indexer records ~20 internal strategy names on CALLS edges
 * (lsp_trait_dispatch, php_self_static, callee_suffix, ...) and the set grows
 * with every language. Publishing those verbatim would make each internal
 * resolver name public API by accident, so a rename would silently change a
 * user-visible field. We publish the CLASS instead: adding lsp_foo_dispatch
 * maps automatically, while a genuinely new KIND of resolution fails the
 * pinning test in tests/test_mcp.c and forces a deliberate decision.
 *
 * Returns NULL only for a NULL/empty strategy — every non-empty value lands in
 * a class, so an unmapped strategy can never silently vanish from output. */
const char *cbm_mcp_edge_strategy_class(const char *strategy) {
    if (!strategy || !strategy[0]) {
        return NULL;
    }
    /* Order matters: lsp_unresolved is an LSP strategy that failed, and the
     * caller cares that it did NOT resolve — so it classifies as unresolved. */
    if (strcmp(strategy, "lsp_unresolved") == 0 || strcmp(strategy, "unknown") == 0) {
        return "unresolved";
    }
    if (strncmp(strategy, "lsp_", 4) == 0) {
        return "lsp";
    }
    if (strncmp(strategy, "php_", 4) == 0 || strncmp(strategy, "perl_", 5) == 0) {
        return "language_rule";
    }
    /* Everything else is a name/shape heuristic: callee_suffix,
     * field_type_hint, service_pattern, fastapi_depends, unique_name, ... */
    return "heuristic";
}

/* Read resolution evidence from the already-selected predecessor edge. Returns
 * false when that edge carries no strategy (non-CALLS edges generally do not). */
static bool trace_edge_evidence(const cbm_edge_info_t *edge, const char **class_out,
                                double *confidence_out) {
    const char *pj = edge ? edge->properties_json : NULL;
    if (pj) {
        const char *key = strstr(pj, "\"strategy\"");
        if (!key) {
            return false;
        }
        const char *open = strchr(key + 10, '"');
        if (!open) {
            return false;
        }
        open++;
        const char *close = strchr(open, '"');
        if (!close || close == open) {
            return false;
        }
        char raw[CBM_SZ_64];
        size_t len = (size_t)(close - open);
        if (len >= sizeof(raw)) {
            len = sizeof(raw) - 1;
        }
        memcpy(raw, open, len);
        raw[len] = '\0';
        const char *cls = cbm_mcp_edge_strategy_class(raw);
        if (!cls) {
            return false;
        }
        *class_out = cls;
        /* Confidence rides on the same edge; absent is reported as -1 so a
         * genuine 0.0 stays distinguishable from "not recorded". */
        *confidence_out = -1.0;
        const char *conf = strstr(pj, "\"confidence\"");
        if (conf) {
            const char *colon = strchr(conf, ':');
            if (colon) {
                /* strtod answers 0.0 for text it cannot read, and the caller
                 * publishes any value >= 0 as a recorded confidence. So a
                 * malformed value used to print as 0.00 — the one number the
                 * surrounding code works to keep meaningful. Keep the -1 when
                 * the end pointer never moved: nothing was read. */
                char *end = NULL;
                double parsed = strtod(colon + 1, &end);
                if (end != colon + 1) {
                    *confidence_out = parsed;
                }
            }
        }
        return true;
    }
    return false;
}

/* TOON table for one trace direction: callees[N]{qn,hop,...} with optional
 * risk / test / args / evidence columns. `name` is omitted (it is the qn's last
 * segment); the per-item JSON key envelope was 84% of the legacy payload. */
static void bfs_to_toon_table(cbm_sb_t *sb, const char *key, cbm_traverse_result_t *tr,
                              bool risk_labels, bool include_tests, bool data_flow,
                              bool include_evidence, const trace_edge_context_t *edge_ctx) {
    int visible = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        visible++;
    }
    const char *cols[7] = {"qn", "hop"};
    int ncols = 2;
    if (risk_labels) {
        cols[ncols++] = "risk";
    }
    if (include_tests) {
        cols[ncols++] = "test";
    }
    /* Header order is the row order: strategy, confidence, then args (#1542). */
    if (include_evidence) {
        cols[ncols++] = "strategy";
        cols[ncols++] = "confidence";
    }
    if (data_flow) {
        cols[ncols++] = "args";
    }
    cbm_tree_table_header(sb, key, visible, cols, ncols);
    for (int i = 0; i < tr->visited_count; i++) {
        const char *fp = tr->visited[i].node.file_path;
        bool test = is_test_file(fp);
        if (!include_tests && test) {
            continue;
        }
        cbm_tree_row_begin(sb);
        cbm_tree_cell_str(sb, tr->visited[i].node.qualified_name, true);
        cbm_tree_cell_int(sb, tr->visited[i].hop, false);
        if (risk_labels) {
            cbm_tree_cell_str(sb, cbm_risk_label(cbm_hop_to_risk(tr->visited[i].hop)), false);
        }
        if (include_tests) {
            cbm_tree_cell_bool(sb, test, false);
        }
        const cbm_edge_info_t *predecessor = (data_flow || include_evidence)
                                                 ? trace_predecessor_edge(edge_ctx, &tr->visited[i])
                                                 : NULL;
        if (include_evidence) {
            const char *ev_class = NULL;
            double ev_conf = -1.0;
            if (trace_edge_evidence(predecessor, &ev_class, &ev_conf)) {
                cbm_tree_cell_str(sb, ev_class, false);
                if (ev_conf >= 0.0) {
                    cbm_tree_cell_real(sb, ev_conf, false);
                } else {
                    cbm_tree_cell_str(sb, "-", false);
                }
            } else {
                cbm_tree_cell_str(sb, "-", false);
                cbm_tree_cell_str(sb, "-", false);
            }
        }
        if (data_flow) {
            size_t alen = 0;
            const char *ea = trace_edge_args(predecessor, &alen);
            if (ea && alen > 0) {
                char *args = cbm_strndup(ea, alen);
                cbm_tree_cell_str(sb, args ? args : "", false);
                free(args);
            } else {
                cbm_tree_cell_str(sb, "", false);
            }
        }
        cbm_tree_row_end(sb);
    }
}

static char *snippet_suggestions(const char *input, cbm_node_t *nodes, int count);

/* Rank a candidate for name resolution. The label tier (callable > class-like >
 * module/file) is the primary key; WITHIN a tier the larger definition by line
 * span wins. In practice the .c-over-.h and C-main-over-shell-main preferences
 * come primarily from span (the real definition has the larger body), since the
 * competing matches usually share a tier — no file extension is hardcoded.
 * Consequence: two same-tier candidates with equal span tie and are reported
 * ambiguous (see pick_resolved_node) rather than guessed. */
enum {
    RES_RANK_CALLABLE = 2,     /* Function / Method */
    RES_RANK_OTHER = 1,        /* Class / Struct / etc. */
    RES_RANK_MODULE = 0,       /* Module / File */
    RES_LABEL_WEIGHT = 1000000 /* label tier dominates span */
};
static long node_resolution_score(const cbm_node_t *n) {
    long label_rank = RES_RANK_MODULE;
    if (n->label) {
        if (strcmp(n->label, "Function") == 0 || strcmp(n->label, "Method") == 0) {
            label_rank = RES_RANK_CALLABLE;
        } else if (strcmp(n->label, "Module") != 0 && strcmp(n->label, "File") != 0) {
            label_rank = RES_RANK_OTHER;
        }
    }
    long span = (long)n->end_line - (long)n->start_line;
    if (span < 0) {
        span = 0;
    }
    return label_rank * (long)RES_LABEL_WEIGHT + span;
}

/* A "real" callable definition: a Function/Method node with a non-empty body
 * span (end_line > start_line). A body-less node (start_line == end_line) is an
 * ambient declaration / signature stub — e.g. a TypeScript `.d.ts` declaration
 * — which is a *fragment* of one logical symbol, not a distinct definition. The
 * distinction lets pick_resolved_node union a stub with its real implementation
 * (#546) while still treating two genuinely-different same-named functions as
 * ambiguous rather than conflating their caller sets. */
static bool node_is_real_callable_def(const cbm_node_t *n) {
    if (!n->label) {
        return false;
    }
    if (strcmp(n->label, "Function") != 0 && strcmp(n->label, "Method") != 0) {
        return false;
    }
    return (long)n->end_line - (long)n->start_line > 0;
}

/* Pick the best-resolving node among name matches. Sets *ambiguous when the
 * matches can't be reduced to one logical symbol, so resolution never silently
 * traces (or conflates) the wrong same-named node:
 *   1. the top score is shared by >1 candidate (a genuine rank/span tie), or
 *   2. two or more *real* callable definitions share the name — distinct
 *      implementations, not a definition plus its body-less stub(s).
 * Rule 2 completes rule 1: without it, two same-named functions whose bodies
 * differ in length score differently, dodge the tie, and get their caller sets
 * unioned by bfs_union_same_name (#546) into one confidently-conflated answer.
 * Body-less .d.ts stubs still union with their implementation (#650). */
static int pick_resolved_node(const cbm_node_t *nodes, int count, bool *ambiguous) {
    *ambiguous = false;
    if (count <= 1) {
        return 0;
    }
    int best = 0;
    long best_score = node_resolution_score(&nodes[0]);
    for (int i = 1; i < count; i++) {
        long s = node_resolution_score(&nodes[i]);
        if (s > best_score) {
            best_score = s;
            best = i;
        }
    }
    int top_count = 0;
    int real_def_count = 0;
    for (int i = 0; i < count; i++) {
        if (node_resolution_score(&nodes[i]) == best_score) {
            top_count++;
        }
        if (node_is_real_callable_def(&nodes[i])) {
            real_def_count++;
        }
    }
    if (real_def_count > 1) {
        *ambiguous = true;
    }
    if (top_count > 1) {
        *ambiguous = true;
    }
    return best;
}

static int node_hop_cmp_hop_id(const void *pa, const void *pb) {
    const cbm_node_hop_t *a = (const cbm_node_hop_t *)pa;
    const cbm_node_hop_t *b = (const cbm_node_hop_t *)pb;
    if (a->hop != b->hop) {
        return a->hop < b->hop ? -1 : 1;
    }
    if (a->node.id != b->node.id) {
        return a->node.id < b->node.id ? -1 : 1;
    }
    return 0;
}

/* BFS from EVERY node sharing the resolved name and merge the results, so the
 * caller/callee set is complete even when one logical symbol is represented by
 * more than one graph node — e.g. a real .ts implementation plus an ambient
 * .d.ts stub, whose inbound CALLS edges are otherwise split across the two
 * nodes and silently truncated by tracing only one (#546). visited hops are
 * deduped by node id. Edge properties are skipped for lean traces or merged
 * under an explicit edge_limit for data-flow/evidence output. Ownership of all
 * heap fields transfers into *out, freed by cbm_store_traverse_free. */
static int bfs_union_same_name(cbm_store_t *store, const cbm_node_t *nodes, int node_count,
                               const char *direction, const char **edge_types, int edge_type_count,
                               int depth, int limit, int edge_limit, cbm_traverse_result_t *out) {
    memset(out, 0, sizeof(*out));
    int vcap = 0, ecap = 0;
    for (int k = 0; k < node_count; k++) {
        cbm_traverse_result_t tr = {0};
        int bfs_rc = cbm_store_bfs_with_edge_limit(store, nodes[k].id, direction, edge_types,
                                                   edge_type_count, depth, limit, edge_limit, &tr);
        if (bfs_rc != CBM_STORE_OK) {
            cbm_store_traverse_free(&tr);
            cbm_store_traverse_free(out);
            return bfs_rc;
        }
        out->truncated = out->truncated || tr.truncated;
        out->edges_truncated = out->edges_truncated || tr.edges_truncated;
        for (int i = 0; i < tr.visited_count; i++) {
            bool dup = false;
            for (int j = 0; j < out->visited_count; j++) {
                if (out->visited[j].node.id == tr.visited[i].node.id) {
                    /* Min-hop across seeds: keep-first recorded the EARLIER
                     * seed's (possibly longer) distance; hop feeds risk_labels
                     * and pagination watermarks, so it must match the
                     * single-BFS MIN(hop) semantics (#797). */
                    if (tr.visited[i].hop < out->visited[j].hop) {
                        out->visited[j].hop = tr.visited[i].hop;
                    }
                    dup = true;
                    break;
                }
            }
            if (dup) {
                continue;
            }
            if (out->visited_count >= limit) {
                out->truncated = true;
                continue;
            }
            if (out->visited_count >= vcap) {
                vcap = vcap ? vcap * 2 : 8;
                out->visited = safe_realloc(out->visited, vcap * sizeof(cbm_node_hop_t));
            }
            out->visited[out->visited_count++] = tr.visited[i];
            memset(&tr.visited[i], 0, sizeof(tr.visited[i])); /* ownership moved */
        }
        for (int i = 0; i < tr.edge_count; i++) {
            /* Overlapping seed neighborhoods yield the same edge from more
             * than one BFS — dedup by (source, target, type). */
            bool edup = false;
            for (int j = 0; j < out->edge_count; j++) {
                if (out->edges[j].source_id == tr.edges[i].source_id &&
                    out->edges[j].target_id == tr.edges[i].target_id && out->edges[j].type &&
                    tr.edges[i].type && strcmp(out->edges[j].type, tr.edges[i].type) == 0) {
                    edup = true;
                    break;
                }
            }
            if (edup) {
                continue;
            }
            if (edge_limit > 0 && out->edge_count >= edge_limit) {
                out->edges_truncated = true;
                continue;
            }
            if (out->edge_count >= ecap) {
                ecap = ecap ? ecap * 2 : 8;
                out->edges = safe_realloc(out->edges, ecap * sizeof(cbm_edge_info_t));
            }
            out->edges[out->edge_count++] = tr.edges[i];
            memset(&tr.edges[i], 0, sizeof(tr.edges[i])); /* ownership moved */
        }
        cbm_store_traverse_free(&tr); /* frees only the un-moved (root + dup) fields */
    }
    /* Canonical (hop, id) order — a pure function of the graph, independent of
     * seed iteration order; required for deterministic output and watermarks. */
    if (out->visited_count > 1) {
        qsort(out->visited, (size_t)out->visited_count, sizeof(cbm_node_hop_t),
              node_hop_cmp_hop_id);
    }
    return CBM_STORE_OK;
}

/* ── Pagination cursors (stateless, exactly-once) ────────────────────
 * Token: "c1.<leg>.<generation>.<qhash>.<hop>.<id>" — version, trace leg
 * (o=callees, i=callers), the store generation (per-DB uid + mutation
 * counter), an FNV-1a-64 hash of the canonical query params, and the
 * (hop, node_id) watermark of the last emitted row in canonical order.
 * Stateless by design: the server re-traverses (the recursive CTE pays the
 * full reachable-set cost regardless of LIMIT, so a page costs what one
 * call costs today) and skips to the watermark. The generation stamp turns
 * every post-reindex cursor into a loud, actionable error — node ids are
 * never reused across rebuilds, so silently resuming would be wrong. */

static uint64_t cursor_fnv1a64(const char *s, uint64_t h) {
    while (s && *s) {
        h ^= (uint64_t)(unsigned char)*s++;
        h *= 0x100000001b3ULL;
    }
    return h;
}

typedef struct {
    char leg;            /* 'o' callees, 'i' callers */
    char generation[96]; /* store generation at mint time */
    uint64_t qhash;      /* canonical-params hash */
    int hop;             /* watermark: last emitted row */
    int64_t node_id;
} trace_cursor_t;

/* Hash the params that define the traversal identity. A cursor replayed with
 * different params must fail loudly, never silently mis-skip. */
static uint64_t trace_params_hash(const char *project, const char *func_name, const char *direction,
                                  const char *mode, const char *param_name, int depth,
                                  bool include_tests, bool risk_labels, bool include_evidence,
                                  int limit, const char *args) {
    uint64_t h = 0xcbf29ce484222325ULL;
    h = cursor_fnv1a64(project ? project : "", h);
    h = cursor_fnv1a64("|", h);
    h = cursor_fnv1a64(func_name ? func_name : "", h);
    h = cursor_fnv1a64("|", h);
    h = cursor_fnv1a64(direction ? direction : "", h);
    h = cursor_fnv1a64("|", h);
    h = cursor_fnv1a64(mode ? mode : "", h);
    h = cursor_fnv1a64("|", h);
    h = cursor_fnv1a64(param_name ? param_name : "", h);
    char nums[64];
    /* Output budget changes only how many whole rows fit after the exact
     * watermark; it does not change graph identity. Excluding it lets a caller
     * follow a hard-floor instruction and raise max_output_tokens without
     * invalidating the cursor that reached that page. */
    snprintf(nums, sizeof(nums), "|%d|%d|%d|%d|%d", depth, include_tests ? 1 : 0,
             risk_labels ? 1 : 0, include_evidence ? 1 : 0, limit);
    h = cursor_fnv1a64(nums, h);

    /* Explicit edge types define the traversed graph. Omitting them from the
     * cursor identity allowed a token minted for CALLS to resume an IMPORTS
     * traversal at an unrelated watermark. Preserve caller order: cursors
     * intentionally require unchanged arguments. */
    yyjson_doc *doc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *types = root && yyjson_is_obj(root) ? yyjson_obj_get(root, "edge_types") : NULL;
    h = cursor_fnv1a64("|edge_types=", h);
    if (types && yyjson_is_arr(types)) {
        size_t index;
        size_t maximum;
        yyjson_val *value;
        yyjson_arr_foreach(types, index, maximum, value) {
            h = cursor_fnv1a64(yyjson_is_str(value) ? yyjson_get_str(value) : "<non-string>", h);
            h = cursor_fnv1a64(";", h);
        }
    }
    if (doc) {
        yyjson_doc_free(doc);
    }
    return h;
}

static void trace_cursor_encode(const trace_cursor_t *c, char *buf, size_t bufsz) {
    snprintf(buf, bufsz, "c1.%c.%s.%016llx.%d.%lld", c->leg, c->generation,
             (unsigned long long)c->qhash, c->hop, (long long)c->node_id);
}

/* Decode + validate. Returns NULL on success, else a static teaching error. */
static const char *trace_cursor_decode(const char *token, const char *current_generation,
                                       uint64_t expected_qhash, trace_cursor_t *out) {
    memset(out, 0, sizeof(*out));
    if (!token || strncmp(token, "c1.", 3) != 0) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    const char *p = token + 3;
    if ((*p != 'o' && *p != 'i') || p[1] != '.') {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    out->leg = *p;
    p += 2; /* leg + '.' */
    const char *gen_end = strchr(p, '.');
    if (!gen_end || gen_end == p || (size_t)(gen_end - p) >= sizeof(out->generation)) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    memcpy(out->generation, p, (size_t)(gen_end - p));
    out->generation[gen_end - p] = '\0';
    const char *hash_start = gen_end + 1;
    const char *hash_end = strchr(hash_start, '.');
    if (!hash_end || hash_end - hash_start != 16) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    for (const char *digit = hash_start; digit < hash_end; digit++) {
        if (!isxdigit((unsigned char)*digit)) {
            return "invalid_cursor: unrecognized token — re-run the original query without "
                   "'cursor'";
        }
    }
    errno = 0;
    char *parsed_end = NULL;
    unsigned long long qh = strtoull(hash_start, &parsed_end, 16);
    if (errno == ERANGE || parsed_end != hash_end) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    const char *hop_start = hash_end + 1;
    const char *hop_end = strchr(hop_start, '.');
    errno = 0;
    long parsed_hop = hop_end ? strtol(hop_start, &parsed_end, 10) : -1;
    if (!hop_end || hop_end == hop_start || errno == ERANGE || parsed_end != hop_end ||
        parsed_hop < 1 || parsed_hop > INT_MAX) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    const char *node_start = hop_end + 1;
    errno = 0;
    long long nid = strtoll(node_start, &parsed_end, 10);
    if (node_start == parsed_end || errno == ERANGE || *parsed_end != '\0' || nid <= 0) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    out->hop = (int)parsed_hop;
    out->qhash = qh;
    out->node_id = nid;
    if (out->qhash != expected_qhash) {
        return "cursor_params_mismatch: this cursor was issued for different arguments — "
               "pass the cursor back with ALL other arguments identical";
    }
    if (strcmp(out->generation, current_generation) != 0) {
        return "stale_cursor: the project was reindexed since this cursor was issued — "
               "re-run the original query without 'cursor' (node identities changed)";
    }
    return NULL;
}

/* Validate an exactly-issued watermark and return the first row after it.
 * Treating an arbitrary (hop,id) as an insertion point let syntactically valid
 * but edited cursors skip rows. The watermarked row must still exist in the
 * selected, test-filtered leg for this generation. */
static int trace_watermark_next_index(const cbm_traverse_result_t *tr, int hop, int64_t node_id) {
    for (int i = 0; i < tr->visited_count; i++) {
        if (tr->visited[i].hop == hop && tr->visited[i].node.id == node_id) {
            return i + SKIP_ONE;
        }
        if (tr->visited[i].hop > hop ||
            (tr->visited[i].hop == hop && tr->visited[i].node.id > node_id)) {
            break;
        }
    }
    return CBM_NOT_FOUND;
}

/* json-stringified tree for one trace leg: same grouped model as the text
 * output — {cols, groups:[{qn_prefix, rows:[[name,hop,...]]}]}. Optional
 * risk/args columns mirror the flags. */
static yyjson_mut_val *bfs_to_tree_json(yyjson_mut_doc *doc, cbm_traverse_result_t *tr,
                                        bool risk_labels, bool include_tests, bool data_flow,
                                        bool include_evidence,
                                        const trace_edge_context_t *edge_ctx) {
    yyjson_mut_val *leg = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, leg, "qn_rule",
                           "qn = qn_prefix == \"\" ? name : qn_prefix + \".\" + name");
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_str(doc, cols, "name");
    yyjson_mut_arr_add_str(doc, cols, "hop");
    if (risk_labels) {
        yyjson_mut_arr_add_str(doc, cols, "risk");
    }
    if (include_tests) {
        yyjson_mut_arr_add_str(doc, cols, "test");
    }
    /* Keep declaration order identical to row emission below: strategy,
     * confidence, then args (#1542) — enabling data_flow must never put args
     * under a strategy column. */
    if (include_evidence) {
        yyjson_mut_arr_add_str(doc, cols, "strategy");
        yyjson_mut_arr_add_str(doc, cols, "confidence");
    }
    if (data_flow) {
        yyjson_mut_arr_add_str(doc, cols, "args");
    }
    yyjson_mut_obj_add_val(doc, leg, "cols", cols);
    yyjson_mut_val *groups = yyjson_mut_arr(doc);
    yyjson_mut_val *cur_rows = NULL;
    char *cur_group = NULL;
    bool have_group = false;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        const char *qn =
            tr->visited[i].node.qualified_name ? tr->visited[i].node.qualified_name : "";
        size_t plen = sg_qn_prefix_len(qn);
        if (!have_group || strlen(cur_group) != plen || memcmp(cur_group, qn, plen) != 0) {
            char *next_group = cbm_strndup(qn, plen);
            if (!next_group) {
                continue;
            }
            free(cur_group);
            cur_group = next_group;
            have_group = true;
            yyjson_mut_val *g = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, g, "qn_prefix", cur_group);
            cur_rows = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, g, "rows", cur_rows);
            yyjson_mut_arr_add_val(groups, g);
        }
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, plen ? qn + plen + 1 : qn);
        yyjson_mut_arr_add_int(doc, row, tr->visited[i].hop);
        if (risk_labels) {
            yyjson_mut_arr_add_str(doc, row, cbm_risk_label(cbm_hop_to_risk(tr->visited[i].hop)));
        }
        if (include_tests) {
            yyjson_mut_arr_add_bool(doc, row, is_test_file(tr->visited[i].node.file_path));
        }
        const cbm_edge_info_t *predecessor = (data_flow || include_evidence)
                                                 ? trace_predecessor_edge(edge_ctx, &tr->visited[i])
                                                 : NULL;
        if (include_evidence) {
            const char *ev_class = NULL;
            double ev_conf = -1.0;
            if (trace_edge_evidence(predecessor, &ev_class, &ev_conf)) {
                yyjson_mut_arr_add_strcpy(doc, row, ev_class ? ev_class : "");
                if (ev_conf >= 0.0) {
                    yyjson_mut_arr_add_real(doc, row, ev_conf);
                } else {
                    yyjson_mut_arr_add_null(doc, row);
                }
            } else {
                /* The root hop has no inbound edge and non-CALLS edges record
                 * no strategy. The tree path emits "-" placeholders to keep the
                 * column count fixed; json says null, which is the same promise
                 * in a form a structured caller can test. */
                yyjson_mut_arr_add_null(doc, row);
                yyjson_mut_arr_add_null(doc, row);
            }
        }
        if (data_flow) {
            size_t alen = 0;
            const char *ea = trace_edge_args(predecessor, &alen);
            if (ea && alen > 0) {
                yyjson_mut_val *av = yyjson_mut_rawn(doc, ea, alen);
                if (av) {
                    yyjson_mut_arr_add_val(row, av);
                } else {
                    yyjson_mut_arr_add_str(doc, row, "");
                }
            } else {
                yyjson_mut_arr_add_str(doc, row, "");
            }
        }
        yyjson_mut_arr_add_val(cur_rows, row);
    }
    free(cur_group);
    yyjson_mut_obj_add_val(doc, leg, "groups", groups);
    return leg;
}

/* Tree-format trace leg: rows grouped by qn-prefix (printed once), each row
 * `name hop` — same data as the TOON table, prefix-factored. Test-file rows
 * honor include_tests exactly like bfs_to_toon_table. Rows arrive in
 * canonical (hop,id) order; grouping re-sorts by (prefix, hop, id) so
 * same-module rows are adjacent (Lost-in-Distance) while hop stays visible. */
static int tree_hop_cmp_qn(const void *pa, const void *pb) {
    const cbm_node_hop_t *a = (const cbm_node_hop_t *)pa;
    const cbm_node_hop_t *b = (const cbm_node_hop_t *)pb;
    const char *qa = a->node.qualified_name ? a->node.qualified_name : "";
    const char *qb = b->node.qualified_name ? b->node.qualified_name : "";
    int c = strcmp(qa, qb);
    if (c != 0) {
        return c;
    }
    return a->hop - b->hop;
}

static void bfs_to_tree_table(cbm_sb_t *sb, const char *key, cbm_traverse_result_t *tr,
                              bool include_tests, bool include_evidence,
                              const trace_edge_context_t *edge_ctx) {
    int visible = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        visible++;
    }
    char buf[CBM_SZ_256];
    snprintf(buf, sizeof(buf),
             "%s: %d  (rows: name hop%s%s; qn = group prefix empty ? "
             "name : prefix + \".\" + name; \"\" marks empty prefix)\n",
             key, visible, include_tests ? " test" : "",
             include_evidence ? " strategy confidence" : "");
    cbm_sb_append(sb, buf);
    cbm_node_hop_t *ordered = visible > 0 ? malloc((size_t)visible * sizeof(*ordered)) : NULL;
    bool ordered_owned = ordered != NULL;
    int ordered_count = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        if (ordered) {
            ordered[ordered_count++] = tr->visited[i];
        }
    }
    if (!ordered && tr->visited_count > 0) {
        /* The caller already applied the test filter; in the allocation
         * failure path retain lossless rows in canonical order. */
        ordered = tr->visited;
        ordered_count = tr->visited_count;
    }
    if (ordered_count > 1) {
        qsort(ordered, (size_t)ordered_count, sizeof(*ordered), tree_hop_cmp_qn);
    }
    char *cur_group = NULL;
    for (int i = 0; i < ordered_count; i++) {
        const char *qn = ordered[i].node.qualified_name ? ordered[i].node.qualified_name : "";
        size_t plen = sg_qn_prefix_len(qn);
        if (!cur_group || strlen(cur_group) != plen || memcmp(cur_group, qn, plen) != 0) {
            char *next_group = cbm_strndup(qn, plen);
            if (!next_group) {
                continue;
            }
            free(cur_group);
            cur_group = next_group;
            cbm_sb_append(sb, cur_group[0] ? cur_group : "\"\"");
            cbm_sb_append(sb, ":\n");
        }
        cbm_tree_row_begin(sb);
        cbm_tree_cell_str(sb, plen ? qn + plen + 1 : qn, true);
        cbm_tree_cell_int(sb, ordered[i].hop, false);
        if (include_tests) {
            cbm_tree_cell_bool(sb, is_test_file(ordered[i].node.file_path), false);
        }
        const char *ev_class = NULL;
        double ev_conf = -1.0;
        const cbm_edge_info_t *predecessor =
            include_evidence ? trace_predecessor_edge(edge_ctx, &ordered[i]) : NULL;
        if (include_evidence && trace_edge_evidence(predecessor, &ev_class, &ev_conf)) {
            cbm_tree_cell_str(sb, ev_class, false);
            if (ev_conf >= 0.0) {
                cbm_tree_cell_real(sb, ev_conf, false);
            } else {
                cbm_tree_cell_str(sb, "-", false);
            }
        } else if (include_evidence) {
            /* The root hop has no inbound edge, and non-CALLS edges record no
             * strategy. Emit placeholders so the column count stays fixed —
             * a ragged table is worse to parse than an explicit "-". */
            cbm_tree_cell_str(sb, "-", false);
            cbm_tree_cell_str(sb, "-", false);
        }
        cbm_tree_row_end(sb);
    }
    free(cur_group);
    if (ordered_owned) {
        free(ordered);
    }
}

/* Grouping removes repeated QN prefixes, but for singleton/scattered legs its
 * rule and group headers cost more than a direct qn column. Both encodings are
 * lossless, so choose the smaller serialized response instead of assuming a
 * directory always wins. */
static void bfs_to_adaptive_tree_table(cbm_sb_t *sb, const char *key, cbm_traverse_result_t *tr,
                                       bool include_tests, bool include_evidence,
                                       const trace_edge_context_t *edge_ctx) {
    cbm_sb_t grouped;
    cbm_sb_init(&grouped);
    bfs_to_tree_table(&grouped, key, tr, include_tests, include_evidence, edge_ctx);
    char *grouped_text = cbm_sb_finish(&grouped);

    cbm_sb_t flat;
    cbm_sb_init(&flat);
    bfs_to_toon_table(&flat, key, tr, false, include_tests, false, include_evidence, edge_ctx);
    char *flat_text = cbm_sb_finish(&flat);
    if (flat_text && (!grouped_text || strlen(flat_text) < strlen(grouped_text))) {
        cbm_sb_append(sb, flat_text);
    } else if (grouped_text) {
        cbm_sb_append(sb, grouped_text);
    }
    free(flat_text);
    free(grouped_text);
}

static void trace_emit_omitted_optional_fields_tree(cbm_sb_t *sb, bool risk_labels, bool data_flow,
                                                    bool include_evidence) {
    char names[CBM_SZ_64] = "";
    size_t used = 0;
    if (risk_labels) {
        used += (size_t)snprintf(names + used, sizeof(names) - used, "risk");
    }
    if (data_flow) {
        used += (size_t)snprintf(names + used, sizeof(names) - used, "%sargs", used ? "," : "");
    }
    if (include_evidence) {
        (void)snprintf(names + used, sizeof(names) - used, "%sstrategy,confidence",
                       used ? "," : "");
    }
    cbm_tree_scalar_str(sb, "omitted_optional_fields", names);
}

static void trace_emit_omitted_optional_fields_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                                    bool risk_labels, bool data_flow,
                                                    bool include_evidence) {
    yyjson_mut_val *names = yyjson_mut_arr(doc);
    if (risk_labels) {
        yyjson_mut_arr_add_str(doc, names, "risk");
    }
    if (data_flow) {
        yyjson_mut_arr_add_str(doc, names, "args");
    }
    if (include_evidence) {
        yyjson_mut_arr_add_str(doc, names, "strategy");
        yyjson_mut_arr_add_str(doc, names, "confidence");
    }
    yyjson_mut_obj_add_val(doc, root, "omitted_optional_fields", names);
}

/* Clamp a client-supplied traversal depth to the MCP ceiling (cbm_mcp_max_depth),
 * WARN-logging when it does so — never a silent truncation (#887). An unclamped
 * `depth` would drive the shared cbm_store_bfs to an arbitrary hop count. */
static int clamp_mcp_depth(int depth, const char *tool) {
    int cap = cbm_mcp_max_depth();
    if (depth > cap) {
        char req_buf[16];
        char cap_buf[16];
        snprintf(req_buf, sizeof(req_buf), "%d", depth);
        snprintf(cap_buf, sizeof(cap_buf), "%d", cap);
        cbm_log_warn("mcp.depth_capped", "tool", tool, "requested", req_buf, "cap", cap_buf);
        return cap;
    }
    return depth;
}

static char *handle_trace_call_path(cbm_mcp_server_t *srv, const char *args) {
    char *func_name = cbm_mcp_get_string_arg(args, "function_name");
    char *project = get_project_arg(args);
    cbm_store_t *store = resolve_store(srv, project);
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    char *mode = cbm_mcp_get_string_arg(args, "mode");
    char *param_name = cbm_mcp_get_string_arg(args, "parameter_name");
    int depth = cbm_mcp_get_int_arg(args, "depth", MCP_DEFAULT_DEPTH);
    depth = clamp_mcp_depth(depth, "trace_call_path");
    /* Per-direction node budget for the BFS working set. The old fixed
     * MCP_BFS_LIMIT silently truncated hub traces at 100 nodes with no
     * signal; now the limit is a documented parameter and hitting it emits
     * `truncated: true` (never a silent truncation — same policy as the
     * depth clamp, #887). */
    int trace_limit = cbm_mcp_get_int_arg(args, "limit", MCP_BFS_LIMIT);
    if (trace_limit < 1) {
        trace_limit = 1;
    }
    if (trace_limit > MCP_BFS_LIMIT_MAX) {
        trace_limit = MCP_BFS_LIMIT_MAX;
    }
    int max_output_tokens = cbm_mcp_get_int_arg(args, "max_output_tokens", 3200);
    if (max_output_tokens < 128) {
        max_output_tokens = 128;
    } else if (max_output_tokens > 1000000) {
        max_output_tokens = 1000000;
    }
    size_t output_budget_bytes =
        (size_t)max_output_tokens * (size_t)MCP_OUTPUT_BYTES_PER_TOKEN_ESTIMATE;
    bool risk_labels = cbm_mcp_get_bool_arg(args, "risk_labels");
    bool include_tests = cbm_mcp_get_bool_arg(args, "include_tests");
    /* Off by default: two extra columns on every row is exactly the kind of
     * inflation the tree format exists to avoid. Opt in when you need to judge
     * whether an edge is trustworthy. */
    bool include_evidence = cbm_mcp_get_bool_arg(args, "include_evidence");

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

    /* Pagination: decode + validate the resume cursor (if any) against the
     * current store generation and the canonical params. Errors teach the
     * recovery action instead of silently restarting from page 1. */
    char generation[96];
    (void)cbm_store_generation(store, generation, sizeof(generation));
    char *cursor_arg = cbm_mcp_get_string_arg(args, "cursor");
    trace_cursor_t cur = {0};
    bool have_cursor = false;
    /* A "legacy" generation (pre-migration DB, e.g. opened read-only before
     * any reindex) offers NO staleness detection: "legacy" == "legacy" across
     * rebuilds, so a stale watermark would silently resume on wrong node ids.
     * Cursors are therefore never minted nor accepted under it. */
    bool gen_legacy = strcmp(generation, "legacy") == 0;
    if (gen_legacy && cursor_arg && cursor_arg[0]) {
        free(cursor_arg);
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        return cbm_mcp_text_result(
            "cursor_unsupported: this project's index predates generation tracking, so cursor "
            "staleness cannot be detected. Re-call with a higher 'limit' instead, or re-index "
            "the project to enable pagination.",
            true);
    }
    if (cursor_arg && cursor_arg[0]) {
        uint64_t qh = trace_params_hash(project, func_name, direction ? direction : "both", mode,
                                        param_name, depth, include_tests, risk_labels,
                                        include_evidence, trace_limit, args);
        const char *cerr = trace_cursor_decode(cursor_arg, generation, qh, &cur);
        if (cerr) {
            free(cursor_arg);
            free(func_name);
            free(project);
            free(direction);
            free(mode);
            free(param_name);
            return cbm_mcp_text_result(cerr, true);
        }
        have_cursor = true;
    }
    free(cursor_arg);
    if (!direction) {
        direction = heap_strdup("both");
    }
    /* Teaching error: an unknown direction used to silently produce an empty
     * trace (both leg flags false) — a field-eval agent burned four calls on
     * "callers"/"callees" before falling back to Cypher. */
    if (strcmp(direction, "inbound") != 0 && strcmp(direction, "outbound") != 0 &&
        strcmp(direction, "both") != 0) {
        char errbuf[CBM_SZ_256];
        snprintf(errbuf, sizeof(errbuf),
                 "invalid direction \"%s\" — use \"inbound\" (callers), \"outbound\" (callees), "
                 "or \"both\"",
                 direction);
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        return cbm_mcp_text_result(errbuf, true);
    }
    if (have_cursor && ((cur.leg == 'o' && strcmp(direction, "inbound") == 0) ||
                        (cur.leg == 'i' && strcmp(direction, "outbound") == 0))) {
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        return cbm_mcp_text_result(
            "invalid_cursor: cursor leg is incompatible with the requested direction — "
            "re-run the original query without 'cursor'",
            true);
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
        yyjson_mut_doc *error_doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *error_obj = yyjson_mut_obj(error_doc);
        yyjson_mut_doc_set_root(error_doc, error_obj);
        yyjson_mut_obj_add_str(error_doc, error_obj, "error", "function not found");
        yyjson_mut_obj_add_strcpy(error_doc, error_obj, "function_name", func_name);
        yyjson_mut_obj_add_str(error_doc, error_obj, "hint",
                               "Use search_graph(name_pattern=...) to find the exact qualified "
                               "name, then pass it to trace_path.");
        char *error_json = yy_doc_to_str(error_doc);
        yyjson_mut_doc_free(error_doc);
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        cbm_store_free_nodes(nodes, 0);
        char *result = cbm_mcp_text_result(error_json ? error_json : "function not found", true);
        free(error_json);
        return result;
    }

    /* Disambiguate same-named matches: prefer the real definition, and report
     * ambiguity (rather than silently tracing nodes[0]) on a genuine tie — e.g.
     * a C main() vs a same-named shell-script main(). */
    bool trace_ambiguous = false;
    int sel = pick_resolved_node(nodes, node_count, &trace_ambiguous);
    if (trace_ambiguous) {
        char *result = snippet_suggestions(func_name, nodes, node_count);
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        cbm_store_free_nodes(nodes, node_count);
        return result;
    }

    /* Response encoding: tree tables by default; format:"json" emits the
     * same grouped model as structured JSON. */
    char *trace_format = cbm_mcp_get_string_arg(args, "format");
    bool trace_legacy_json = trace_format && strcmp(trace_format, "json") == 0;
    free(trace_format);

    /* Edge types: explicit > mode-based > default */
    const char *edge_types[MCP_COL_16];
    int edge_type_count = 0;
    yyjson_doc *et_doc_keep = resolve_trace_edge_types(args, mode, edge_types, &edge_type_count);

    /* Run BFS for each requested direction.
     * IMPORTANT: emitters borrow node-string pointers — traversal results
     * must stay alive until after serialization. */
    bool do_outbound = strcmp(direction, "outbound") == 0 || strcmp(direction, "both") == 0;
    bool do_inbound = strcmp(direction, "inbound") == 0 || strcmp(direction, "both") == 0;

    cbm_traverse_result_t tr_out = {0};
    cbm_traverse_result_t tr_in = {0};

    bool data_flow = mode && strcmp(mode, "data_flow") == 0;
    bool need_edge_data = data_flow || include_evidence;
    int edge_data_limit = need_edge_data ? MCP_BFS_LIMIT_MAX : 0;

    (void)sel; /* union across all same-name nodes — see bfs_union_same_name (#546) */

    /* Traverse with the SAFETY ceiling (not the page size): the recursive CTE
     * enumerates the full depth-bounded reachable set regardless of LIMIT, so
     * materializing up to MCP_BFS_LIMIT_MAX rows costs the same traversal —
     * and gives exact totals plus the rows every later page needs. The page
     * size (trace_limit) only bounds what THIS response emits. */
    int traversal_rc = CBM_STORE_OK;
    if (do_outbound) {
        traversal_rc =
            bfs_union_same_name(store, nodes, node_count, "outbound", edge_types, edge_type_count,
                                depth, MCP_BFS_LIMIT_MAX, edge_data_limit, &tr_out);
    }
    if (traversal_rc == CBM_STORE_OK && do_inbound) {
        traversal_rc =
            bfs_union_same_name(store, nodes, node_count, "inbound", edge_types, edge_type_count,
                                depth, MCP_BFS_LIMIT_MAX, edge_data_limit, &tr_in);
    }
    if (traversal_rc != CBM_STORE_OK) {
        cbm_store_traverse_free(&tr_out);
        cbm_store_traverse_free(&tr_in);
        cbm_store_free_nodes(nodes, node_count);
        free(func_name);
        free(project);
        free(direction);
        free(mode);
        free(param_name);
        if (et_doc_keep) {
            yyjson_doc_free(et_doc_keep);
        }
        return cbm_mcp_text_result("trace traversal failed; inspect index_status and retry", true);
    }
    if (!include_tests) {
        trace_filter_test_rows(&tr_out);
        trace_filter_test_rows(&tr_in);
    }

    /* Page windows in canonical (hop,id) order. Legs drain in a fixed order
     * (callees, then callers); a resume cursor starts its leg at the row
     * after the watermark, and a page that finishes one leg with budget to
     * spare continues into the next. */
    int out_start = 0;
    int in_start = 0;
    if (have_cursor) {
        int watermark_next = CBM_NOT_FOUND;
        if (cur.leg == 'o') {
            watermark_next = trace_watermark_next_index(&tr_out, cur.hop, cur.node_id);
            out_start = watermark_next;
        } else {
            out_start = tr_out.visited_count; /* callees leg already drained */
            watermark_next = trace_watermark_next_index(&tr_in, cur.hop, cur.node_id);
            in_start = watermark_next;
        }
        if (watermark_next == CBM_NOT_FOUND) {
            cbm_store_traverse_free(&tr_out);
            cbm_store_traverse_free(&tr_in);
            cbm_store_free_nodes(nodes, node_count);
            free(func_name);
            free(project);
            free(direction);
            free(mode);
            free(param_name);
            if (et_doc_keep) {
                yyjson_doc_free(et_doc_keep);
            }
            return cbm_mcp_text_result(
                "invalid_cursor: watermark is not present in the selected visible trace leg — "
                "re-run the original query without 'cursor'",
                true);
        }
    }
    int budget = trace_limit;
    int out_len = 0;
    int in_len = 0;
    if (do_outbound) {
        out_len = tr_out.visited_count - out_start;
        if (out_len > budget) {
            out_len = budget;
        }
        budget -= out_len;
    }
    if (do_inbound) {
        in_len = tr_in.visited_count - in_start;
        if (in_len > budget) {
            in_len = budget;
        }
    }
    const int requested_out_len = out_len;
    const int requested_in_len = in_len;
    const int requested_page_rows = requested_out_len + requested_in_len;
    int row_target = requested_page_rows;
    int budget_search_low = 0;
    int budget_search_high = requested_page_rows;
    int budget_search_best = 0;
    bool budget_search_active = false;
    bool output_budget_hit = false;
    bool emit_optional_fields = risk_labels || data_flow || include_evidence;
    bool optional_fields_omitted = false;
    char *json = NULL;

render_trace_output:;
    int rows_left = row_target;
    out_len = requested_out_len < rows_left ? requested_out_len : rows_left;
    rows_left -= out_len;
    in_len = requested_in_len < rows_left ? requested_in_len : rows_left;
    bool out_more = do_outbound && out_start + out_len < tr_out.visited_count;
    bool in_more = do_inbound && in_start + in_len < tr_in.visited_count;
    bool more_rows = out_more || in_more;
    bool engine_saturated = tr_out.truncated || tr_in.truncated;
    bool edge_data_saturated = tr_out.edges_truncated || tr_in.edges_truncated;
    bool trace_truncated =
        more_rows || engine_saturated || edge_data_saturated || output_budget_hit;
    char next_tok[192] = "";
    /* Never mint a cursor under a "legacy" generation (no staleness
     * detection); the truncated flag + raise-limit hint still fire below. */
    if (more_rows && !gen_legacy) {
        trace_cursor_t nc = {0};
        snprintf(nc.generation, sizeof(nc.generation), "%s", generation);
        nc.qhash =
            trace_params_hash(project, func_name, direction, mode, param_name, depth, include_tests,
                              risk_labels, include_evidence, trace_limit, args);
        /* The watermark is the last row ACTUALLY emitted, not the leg that
         * happens to have additional rows. At an exact outbound page boundary
         * inbound may be pending with zero emitted rows. */
        if (in_len > 0) {
            nc.leg = 'i';
            nc.hop = tr_in.visited[in_start + in_len - 1].hop;
            nc.node_id = tr_in.visited[in_start + in_len - 1].node.id;
        } else if (out_len > 0) {
            nc.leg = 'o';
            nc.hop = tr_out.visited[out_start + out_len - 1].hop;
            nc.node_id = tr_out.visited[out_start + out_len - 1].node.id;
        }
        if (in_len > 0 || out_len > 0) {
            trace_cursor_encode(&nc, next_tok, sizeof(next_tok));
        }
    }

    /* Window views: visited offset + count; the full edges array stays
     * attached so data_flow args resolve for boundary nodes whose incoming
     * edge originated on an earlier page. */
    cbm_traverse_result_t view_out = tr_out;
    view_out.visited = tr_out.visited ? tr_out.visited + out_start : NULL;
    view_out.visited_count = out_len;
    cbm_traverse_result_t view_in = tr_in;
    view_in.visited = tr_in.visited ? tr_in.visited + in_start : NULL;
    view_in.visited_count = in_len;
    trace_edge_context_t out_edge_ctx = {
        .full = &tr_out, .roots = nodes, .root_count = node_count, .inbound = false};
    trace_edge_context_t in_edge_ctx = {
        .full = &tr_in, .roots = nodes, .root_count = node_count, .inbound = true};

    /* Totals must count what the caller can actually enumerate: when
     * include_tests=false the tables hide test-file rows, so raw
     * visited_count overstated the reachable set (a field-eval agent read
     * callers_total=175 against a handful of visible rows and distrusted
     * the tool). Count with the same filter the emitters apply. */
    int out_total = 0;
    for (int i = 0; i < tr_out.visited_count; i++) {
        if (include_tests || !is_test_file(tr_out.visited[i].node.file_path)) {
            out_total++;
        }
    }
    int in_total = 0;
    for (int i = 0; i < tr_in.visited_count; i++) {
        if (include_tests || !is_test_file(tr_in.visited[i].node.file_path)) {
            in_total++;
        }
    }

    free(json);
    json = NULL;
    if (!trace_legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_str(&sb, "function", func_name);
        cbm_tree_scalar_str(&sb, "direction", direction);
        if (mode) {
            cbm_tree_scalar_str(&sb, "mode", mode);
        }
        /* Grouped tree is THE default; risk_labels/data_flow keep the flat
         * table (extra columns) in the same tree syntax. */
        bool render_risk = risk_labels && emit_optional_fields;
        bool render_data_flow = data_flow && emit_optional_fields;
        bool render_evidence = include_evidence && emit_optional_fields;
        bool flat_trace = render_risk || render_data_flow;
        if (do_outbound) {
            cbm_tree_scalar_int(&sb, "callees_total", out_total);
            cbm_tree_scalar_str(&sb, "callees_total_relation", tr_out.truncated ? "gte" : "eq");
            if (flat_trace) {
                bfs_to_toon_table(&sb, "callees", &view_out, render_risk, include_tests,
                                  render_data_flow, render_evidence, &out_edge_ctx);
            } else {
                bfs_to_adaptive_tree_table(&sb, "callees", &view_out, include_tests,
                                           render_evidence, &out_edge_ctx);
            }
        }
        if (do_inbound) {
            cbm_tree_scalar_int(&sb, "callers_total", in_total);
            cbm_tree_scalar_str(&sb, "callers_total_relation", tr_in.truncated ? "gte" : "eq");
            if (flat_trace) {
                bfs_to_toon_table(&sb, "callers", &view_in, render_risk, include_tests,
                                  render_data_flow, render_evidence, &in_edge_ctx);
            } else {
                bfs_to_adaptive_tree_table(&sb, "callers", &view_in, include_tests, render_evidence,
                                           &in_edge_ctx);
            }
        }
        if (trace_truncated) {
            cbm_tree_scalar_bool(&sb, "truncated", true);
            cbm_tree_scalar_bool(&sb, "has_more", more_rows);
            cbm_tree_scalar_str(
                &sb, "truncation_reason",
                output_budget_hit
                    ? "output_budget"
                    : (more_rows ? "page_limit"
                                 : (engine_saturated ? "engine_limit" : "edge_data_limit")));
            if (output_budget_hit) {
                cbm_tree_scalar_int(&sb, "max_output_bytes", (long long)output_budget_bytes);
            }
            if (optional_fields_omitted) {
                cbm_tree_scalar_bool(&sb, "optional_fields_omitted", true);
                trace_emit_omitted_optional_fields_tree(&sb, risk_labels, data_flow,
                                                        include_evidence);
            }
            if (engine_saturated) {
                cbm_tree_scalar_bool(&sb, "engine_saturated", true);
            }
            if (edge_data_saturated) {
                cbm_tree_scalar_bool(&sb, "edge_data_saturated", true);
            }
            if (next_tok[0]) {
                cbm_tree_scalar_str(&sb, "next", next_tok);
                cbm_tree_scalar_str(&sb, "hint",
                                    "more rows exist — re-call with cursor set to 'next' and ALL "
                                    "other arguments identical (no duplicates), or narrow with "
                                    "depth/edge_types");
            } else if (more_rows) {
                cbm_tree_scalar_str(&sb, "hint",
                                    "more rows exist — re-call with a higher 'limit' (cursors "
                                    "unavailable: index predates generation tracking)");
            } else if (engine_saturated) {
                cbm_tree_scalar_str(
                    &sb, "hint",
                    "Traversal reached the 5000-node safety ceiling; narrow depth/edge_types "
                    "or use query_graph for a differently bounded traversal.");
            } else {
                cbm_tree_scalar_str(&sb, "hint",
                                    "Optional edge evidence reached its 5000-edge ceiling; narrow "
                                    "depth/edge_types or disable data_flow/include_evidence.");
            }
        }
        json = cbm_sb_finish(&sb);
    } else {
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        yyjson_mut_obj_add_str(doc, root, "function", func_name);
        yyjson_mut_obj_add_str(doc, root, "direction", direction);
        if (mode) {
            yyjson_mut_obj_add_str(doc, root, "mode", mode);
        }
        if (do_outbound) {
            yyjson_mut_obj_add_int(doc, root, "callees_total", out_total);
            yyjson_mut_obj_add_str(doc, root, "callees_total_relation",
                                   tr_out.truncated ? "gte" : "eq");
            yyjson_mut_obj_add_val(
                doc, root, "callees",
                bfs_to_tree_json(doc, &view_out, risk_labels && emit_optional_fields, include_tests,
                                 data_flow && emit_optional_fields,
                                 include_evidence && emit_optional_fields, &out_edge_ctx));
        }
        if (do_inbound) {
            yyjson_mut_obj_add_int(doc, root, "callers_total", in_total);
            yyjson_mut_obj_add_str(doc, root, "callers_total_relation",
                                   tr_in.truncated ? "gte" : "eq");
            yyjson_mut_obj_add_val(
                doc, root, "callers",
                bfs_to_tree_json(doc, &view_in, risk_labels && emit_optional_fields, include_tests,
                                 data_flow && emit_optional_fields,
                                 include_evidence && emit_optional_fields, &in_edge_ctx));
        }
        if (trace_truncated) {
            yyjson_mut_obj_add_bool(doc, root, "truncated", true);
            yyjson_mut_obj_add_bool(doc, root, "has_more", more_rows);
            yyjson_mut_obj_add_str(
                doc, root, "truncation_reason",
                output_budget_hit
                    ? "output_budget"
                    : (more_rows ? "page_limit"
                                 : (engine_saturated ? "engine_limit" : "edge_data_limit")));
            if (output_budget_hit) {
                yyjson_mut_obj_add_uint(doc, root, "max_output_bytes", output_budget_bytes);
            }
            if (optional_fields_omitted) {
                yyjson_mut_obj_add_bool(doc, root, "optional_fields_omitted", true);
                trace_emit_omitted_optional_fields_json(doc, root, risk_labels, data_flow,
                                                        include_evidence);
            }
            if (engine_saturated) {
                yyjson_mut_obj_add_bool(doc, root, "engine_saturated", true);
            }
            if (edge_data_saturated) {
                yyjson_mut_obj_add_bool(doc, root, "edge_data_saturated", true);
            }
            if (next_tok[0]) {
                yyjson_mut_obj_add_strcpy(doc, root, "next_cursor", next_tok);
            }
        }
        /* Serialize BEFORE freeing traversal results (yyjson borrows strings) */
        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    /* Apply the deterministic byte ceiling after serialization. Optional
     * diagnostics yield first; if primary graph rows still do not fit, find
     * the largest whole-row prefix. Cursor watermarks are recomputed on every
     * render, so `next` always follows the last row actually emitted. */
    if (json && strlen(json) > output_budget_bytes) {
        output_budget_hit = true;
        if (emit_optional_fields) {
            emit_optional_fields = false;
            optional_fields_omitted = true;
            goto render_trace_output;
        }
        if (row_target <= 1 || requested_page_rows == 0) {
            free(json);
            bool floor_has_more = requested_page_rows > 0;
            if (!trace_legacy_json) {
                cbm_sb_t floor;
                cbm_sb_init(&floor);
                if (do_outbound) {
                    cbm_tree_scalar_int(&floor, "callees_total", out_total);
                    cbm_tree_scalar_str(&floor, "callees_total_relation",
                                        tr_out.truncated ? "gte" : "eq");
                }
                if (do_inbound) {
                    cbm_tree_scalar_int(&floor, "callers_total", in_total);
                    cbm_tree_scalar_str(&floor, "callers_total_relation",
                                        tr_in.truncated ? "gte" : "eq");
                }
                cbm_tree_scalar_bool(&floor, "has_more", floor_has_more);
                if (floor_has_more) {
                    cbm_tree_scalar_bool(&floor, "continuation_requires_higher_budget", true);
                }
                cbm_tree_scalar_bool(&floor, "truncated", true);
                cbm_tree_scalar_str(&floor, "truncation_reason", "output_budget");
                cbm_tree_scalar_bool(&floor, "output_budget_floor_exceeded", true);
                cbm_tree_scalar_int(&floor, "max_output_bytes", (long long)output_budget_bytes);
                if (optional_fields_omitted) {
                    cbm_tree_scalar_bool(&floor, "optional_fields_omitted", true);
                    trace_emit_omitted_optional_fields_tree(&floor, risk_labels, data_flow,
                                                            include_evidence);
                }
                cbm_tree_scalar_str(&floor, "hint",
                                    "raise max_output_tokens; no identifier was sliced");
                json = cbm_sb_finish(&floor);
            } else {
                yyjson_mut_doc *floor_doc = yyjson_mut_doc_new(NULL);
                yyjson_mut_val *floor = yyjson_mut_obj(floor_doc);
                yyjson_mut_doc_set_root(floor_doc, floor);
                if (do_outbound) {
                    yyjson_mut_obj_add_int(floor_doc, floor, "callees_total", out_total);
                    yyjson_mut_obj_add_str(floor_doc, floor, "callees_total_relation",
                                           tr_out.truncated ? "gte" : "eq");
                }
                if (do_inbound) {
                    yyjson_mut_obj_add_int(floor_doc, floor, "callers_total", in_total);
                    yyjson_mut_obj_add_str(floor_doc, floor, "callers_total_relation",
                                           tr_in.truncated ? "gte" : "eq");
                }
                yyjson_mut_obj_add_bool(floor_doc, floor, "has_more", floor_has_more);
                if (floor_has_more) {
                    yyjson_mut_obj_add_bool(floor_doc, floor, "continuation_requires_higher_budget",
                                            true);
                }
                yyjson_mut_obj_add_bool(floor_doc, floor, "truncated", true);
                yyjson_mut_obj_add_str(floor_doc, floor, "truncation_reason", "output_budget");
                yyjson_mut_obj_add_bool(floor_doc, floor, "output_budget_floor_exceeded", true);
                yyjson_mut_obj_add_uint(floor_doc, floor, "max_output_bytes", output_budget_bytes);
                if (optional_fields_omitted) {
                    yyjson_mut_obj_add_bool(floor_doc, floor, "optional_fields_omitted", true);
                    trace_emit_omitted_optional_fields_json(floor_doc, floor, risk_labels,
                                                            data_flow, include_evidence);
                }
                yyjson_mut_obj_add_str(floor_doc, floor, "hint",
                                       "raise max_output_tokens; no identifier was sliced");
                json = yy_doc_to_str(floor_doc);
                yyjson_mut_doc_free(floor_doc);
            }
            goto trace_output_ready;
        }
        if (!budget_search_active) {
            budget_search_active = true;
            budget_search_low = 1;
            budget_search_high = row_target - 1;
        } else {
            budget_search_high = row_target - 1;
        }
        if (budget_search_low > budget_search_high) {
            if (budget_search_best < 1) {
                row_target = 1;
            } else {
                row_target = budget_search_best;
            }
            budget_search_active = false;
            goto render_trace_output;
        }
        row_target = budget_search_low + (budget_search_high - budget_search_low) / 2;
        goto render_trace_output;
    }
    if (budget_search_active) {
        budget_search_best = row_target;
        budget_search_low = row_target + 1;
        if (budget_search_low <= budget_search_high) {
            row_target = budget_search_low + (budget_search_high - budget_search_low + 1) / 2;
            goto render_trace_output;
        }
    }

trace_output_ready:

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
    FILE *fp = cbm_fopen(path, "r");
    if (!fp) {
        return NULL;
    }

    cbm_sb_t selected;
    cbm_sb_init(&selected);
    int lineno = 1;
    int byte;
    while ((byte = fgetc(fp)) != EOF) {
        if (lineno >= start && lineno <= end) {
            char ch = (char)byte;
            cbm_sb_append_n(&selected, &ch, 1U);
        }
        if (byte == '\n') {
            if (lineno >= end) {
                break;
            }
            lineno++;
        }
    }

    (void)fclose(fp);
    char *result = cbm_sb_finish(&selected);
    if (result && result[0] == '\0') {
        free(result);
        return NULL;
    }
    return result;
}

/* ── Helper: get project root_path from store ─────────────────── */

static char *project_root_from_store(cbm_store_t *store, const char *project) {
    if (!store || !project) {
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

static char *get_project_root(cbm_mcp_server_t *srv, const char *project) {
    return project_root_from_store(resolve_store(srv, project), project);
}

/* ── index_repository ─────────────────────────────────────────── */

static int cross_repo_project_key_compare(const void *left, const void *right) {
    const char *const *left_key = left;
    const char *const *right_key = right;
    return strcmp(*left_key, *right_key);
}

static unsigned char cross_repo_project_lock_fold(unsigned char ch) {
    return ch >= 'A' && ch <= 'Z' ? (unsigned char)(ch + ('a' - 'A')) : ch;
}

/* Match daemon/project_lock.c's OS-key identity exactly: only ASCII A-Z folds.
 * The raw strcmp tie-break gives qsort a total, input-order-independent order
 * while keeping the caller's original project spelling as the lease value. */
static int cross_repo_project_lock_key_compare_values(const char *left, const char *right) {
    const unsigned char *left_cursor = (const unsigned char *)left;
    const unsigned char *right_cursor = (const unsigned char *)right;
    while (*left_cursor && *right_cursor) {
        unsigned char left_folded = cross_repo_project_lock_fold(*left_cursor);
        unsigned char right_folded = cross_repo_project_lock_fold(*right_cursor);
        if (left_folded != right_folded) {
            return left_folded < right_folded ? -1 : 1;
        }
        left_cursor++;
        right_cursor++;
    }
    if (*left_cursor != *right_cursor) {
        return *left_cursor ? 1 : -1;
    }
    return strcmp(left, right);
}

static int cross_repo_project_lock_key_compare(const void *left, const void *right) {
    const char *const *left_key = left;
    const char *const *right_key = right;
    return cross_repo_project_lock_key_compare_values(*left_key, *right_key);
}

static bool cross_repo_project_lock_keys_equivalent(const char *left, const char *right) {
    const unsigned char *left_cursor = (const unsigned char *)left;
    const unsigned char *right_cursor = (const unsigned char *)right;
    while (*left_cursor && *right_cursor) {
        if (cross_repo_project_lock_fold(*left_cursor) !=
            cross_repo_project_lock_fold(*right_cursor)) {
            return false;
        }
        left_cursor++;
        right_cursor++;
    }
    return *left_cursor == *right_cursor;
}

/* Handle mode="cross-repo-intelligence" — extract to reduce complexity. */
static char *handle_cross_repo_mode(cbm_mcp_server_t *srv, const char *repo_path,
                                    const char *name_override, const char *args) {
    if (name_override && name_override[0] && !cbm_validate_project_name(name_override)) {
        return cbm_mcp_text_result("invalid project name", true);
    }
    char *project = name_override && name_override[0] ? heap_strdup(name_override)
                                                      : cbm_project_name_from_path(repo_path);
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

    size_t target_count = yyjson_arr_size(tp_arr);
    if (target_count > MCP_MAX_CROSS_REPO_TARGETS) {
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("too many cross-repo target projects", true);
    }
    int tp_count = (int)target_count;
    const char **targets = malloc((size_t)tp_count * sizeof(*targets));
    const char **lease_keys = malloc(((size_t)tp_count + 1U) * sizeof(*lease_keys));
    if (!targets || !lease_keys) {
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("failed to allocate cross-repo project leases", true);
    }
    size_t idx;
    size_t max;
    yyjson_val *val;
    int ti = 0;
    bool all_projects = false;
    bool invalid_target = false;
    yyjson_arr_foreach(tp_arr, idx, max, val) {
        const char *target = yyjson_is_str(val) ? yyjson_get_str(val) : NULL;
        if (!target || !target[0] || strlen(target) >= CBM_SZ_256 ||
            (strcmp(target, "*") != 0 && !cbm_validate_project_name(target))) {
            invalid_target = true;
            break;
        }
        targets[ti++] = target;
        all_projects = all_projects || strcmp(target, "*") == 0;
    }
    if (invalid_target || ti != tp_count) {
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("target_projects must contain valid project names or '*'", true);
    }
    if (all_projects && tp_count != 1) {
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("target_projects wildcard '*' must be the only entry", true);
    }
    if (!all_projects) {
        qsort(targets, (size_t)tp_count, sizeof(*targets), cross_repo_project_key_compare);
        int unique_count = 0;
        for (int i = 0; i < tp_count; i++) {
            if (unique_count == 0 || strcmp(targets[i], targets[unique_count - 1]) != 0) {
                targets[unique_count++] = targets[i];
            }
        }
        tp_count = unique_count;
    }

    int lease_count = 0;
    if (all_projects) {
        lease_keys[lease_count++] = "*";
    } else {
        lease_keys[lease_count++] = project;
        for (int i = 0; i < tp_count; i++) {
            lease_keys[lease_count++] = targets[i];
        }
        qsort(lease_keys, (size_t)lease_count, sizeof(*lease_keys),
              cross_repo_project_lock_key_compare);
        int unique_count = 0;
        for (int i = 0; i < lease_count; i++) {
            if (unique_count == 0 || !cross_repo_project_lock_keys_equivalent(
                                         lease_keys[i], lease_keys[unique_count - 1])) {
                lease_keys[unique_count++] = lease_keys[i];
            }
        }
        lease_count = unique_count;
    }

    int held_count = 0;
    while (held_count < lease_count && mcp_project_mutation_begin(srv, lease_keys[held_count])) {
        held_count++;
    }
    bool cancelled =
        atomic_load_explicit(&srv->pipeline_cancel_requested, memory_order_acquire) != 0;
    if (held_count != lease_count || cancelled) {
        while (held_count > 0) {
            held_count--;
            mcp_project_mutation_end(srv, lease_keys[held_count]);
        }
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("cross-repo operation cancelled or blocked by active indexing",
                                   true);
    }

    cbm_cross_repo_result_t result = cbm_cross_repo_match_cancellable(
        project, targets, tp_count, &srv->pipeline_cancel_requested);
    while (held_count > 0) {
        held_count--;
        mcp_project_mutation_end(srv, lease_keys[held_count]);
    }
    free(targets);
    free(lease_keys);
    yyjson_doc_free(jdoc);

    if (result.failed) {
        free(project);
        return cbm_mcp_text_result(
            "cross-repo source or target project is missing, invalid, or not indexed", true);
    }

    int total = result.http_edges + result.async_edges + result.channel_edges + result.grpc_edges +
                result.graphql_edges + result.trpc_edges;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", result.cancelled ? "cancelled" : "success");
    yyjson_mut_obj_add_str(doc, root, "mode", "cross-repo-intelligence");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    if (result.cancelled) {
        yyjson_mut_obj_add_bool(doc, root, "partial_results", result.partial_results);
        yyjson_mut_obj_add_str(
            doc, root, "message",
            result.partial_results
                ? "cross-repo operation cancelled with partial results; completed database "
                  "writes were retained"
                : "cross-repo operation cancelled before database writes");
    }
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
    char *out = cbm_mcp_text_result(json, result.cancelled);
    free(json);
    return out;
}

/* Bootstrap from artifact if no local DB exists for this project. */
static void try_artifact_bootstrap(const char *project_name, const char *repo_path) {
    char db_buf[CBM_SZ_1K];
    project_db_path(project_name, db_buf, sizeof(db_buf));
    if (cbm_file_size(db_buf) < 0 && cbm_artifact_exists(repo_path)) {
        cbm_log_info("index.artifact_bootstrap", "project", project_name);
        /* An imported artifact is trusted for graph CONTENT as-is — nothing
         * verifies that its nodes/edges describe the code they claim to. What
         * has been limiting the blast radius is mechanical, not a check: every
         * imported row carries the EXPORTER's mtime, so the first incremental
         * run re-parses ~everything and auto-scrubs a poisoned artifact at a
         * clone time the producer cannot predict. That exposure is transient
         * and self-healing.
         *
         * cbm_artifact_reconcile_hashes deliberately trades part of that away
         * for the speed the artifact is supposed to deliver (#885): rows it
         * restamps are no longer re-parsed, so poisoned nodes for those files
         * persist until the file changes — a DURABLE exposure gated on a
         * producer-written marker. It stays acceptable only because the marker
         * alone never suffices: each restamped row must additionally be proven
         * unchanged by the LOCAL git against a commit present in this clone.
         * Read the trade-off note in artifact.h before widening it.
         *
         * Best-effort: a -1 return leaves every row foreign and falls back to
         * today's behavior, so a failure here can never fail the import. */
        if (cbm_artifact_import(repo_path, db_buf) == 0) {
            (void)cbm_artifact_reconcile_hashes(repo_path, db_buf, project_name);
        }
    }
}

/* Cap on excluded dir paths listed in the response — keep it compact on large
 * repos (node_modules / vendor / etc. can produce many skip points). The full
 * count is still reported via "count" + "truncated". */
enum { INDEX_EXCLUDED_DIR_CAP = 5 }; /* examples only — see INDEX_SKIPPED_FILE_CAP note */

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

/* Cap on per-file skips embedded in the JSON response — keep it compact on
 * large repos. The FULL, uncapped list always goes to the per-run logfile;
 * the JSON carries "count" + "truncated" so nothing is silently hidden. */
/* In-response coverage lists are EXAMPLES, not the record: the full uncapped
 * lists live in the per-run logfile (path in the same response) and are
 * queryable via index_status (scope_limit) / query_graph(graph="missed").
 * Five examples orient the agent; anything more duplicates the logfile into
 * every index response (53 KB observed on a large repo). */
enum { INDEX_SKIPPED_FILE_CAP = 5 };

/* Attach the by-design ignored-FILES summary (#963 "purposely not indexed").
 * Individual files dropped by ignore rules — deliberate, not failures; whole
 * excluded subtrees are reported separately via "excluded". Always emits
 * "not_indexed_files_count" (the uncapped total); the list itself is capped
 * like skipped[] and marked truncated when discovery hit its storage cap. */
static void add_not_indexed_files_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                          cbm_pipeline_t *p) {
    cbm_ignored_file_t *ignored = NULL;
    int stored = 0;
    int total = 0;
    cbm_pipeline_get_ignored(p, &ignored, &stored, &total);
    yyjson_mut_obj_add_int(doc, root, "not_indexed_files_count", total);
    if (!ignored || stored <= 0) {
        return;
    }
    yyjson_mut_val *ni = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = stored < INDEX_SKIPPED_FILE_CAP ? stored : INDEX_SKIPPED_FILE_CAP;
    for (int i = 0; i < shown; i++) {
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", ignored[i].rel_path ? ignored[i].rel_path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "reason", ignored[i].reason ? ignored[i].reason : "");
        yyjson_mut_arr_add_val(files, fe);
    }
    yyjson_mut_obj_add_val(doc, ni, "files", files);
    yyjson_mut_obj_add_int(doc, ni, "count", total);
    yyjson_mut_obj_add_bool(doc, ni, "truncated", total > shown);
    yyjson_mut_obj_add_str(doc, ni, "note",
                           "Excluded by design (gitignore/.cbmignore/skip-lists); examples only — "
                           "full list in 'logfile'.");
    yyjson_mut_obj_add_val(doc, root, "not_indexed_files", ni);
}

/* True when a recorded per-file entry is the parse-partial coverage signal
 * (#963) rather than a genuine skip. Kept out of skipped[]/skipped_count so
 * the "skipped" contract (file NOT indexed) stays exact. */
static bool is_parse_partial(const cbm_file_error_t *e) {
    return e->phase && strcmp(e->phase, "parse_partial") == 0;
}

/* Attach a summary of per-file skips (Stage 2 / Track B). Always emits a
 * top-level "skipped_count" (0 on clean runs) so consumers can rely on it.
 * When there are skips, also emits:
 *   "skipped": {"files":[{path,reason,phase}..(<=50)], "count":N, "truncated":bool}
 * and, if a per-run logfile was written, "logfile": "<path>".
 * The run status stays "indexed" — a skipped file is the expected handled
 * outcome, not a failure. errs[] is borrowed (copied into doc) and may contain
 * parse_partial entries, which are filtered out here (reported separately by
 * add_parse_partial_summary). */
static void add_skipped_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                const cbm_file_error_t *errs, int count, const char *logfile) {
    int skips = 0;
    for (int i = 0; i < count; i++) {
        if (!is_parse_partial(&errs[i])) {
            skips++;
        }
    }
    yyjson_mut_obj_add_int(doc, root, "skipped_count", skips);
    if (logfile && logfile[0]) {
        yyjson_mut_obj_add_strcpy(doc, root, "logfile", logfile);
    }
    if (!errs || skips <= 0) {
        return;
    }
    yyjson_mut_val *skipped = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = 0;
    for (int i = 0; i < count && shown < INDEX_SKIPPED_FILE_CAP; i++) {
        if (is_parse_partial(&errs[i])) {
            continue;
        }
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", errs[i].path ? errs[i].path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "reason", errs[i].reason ? errs[i].reason : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "phase", errs[i].phase ? errs[i].phase : "");
        yyjson_mut_arr_add_val(files, fe);
        shown++;
    }
    yyjson_mut_obj_add_val(doc, skipped, "files", files);
    yyjson_mut_obj_add_int(doc, skipped, "count", skips);
    yyjson_mut_obj_add_bool(doc, skipped, "truncated", skips > INDEX_SKIPPED_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "skipped", skipped);
}

/* Attach the best-effort parse-coverage summary (#963). Always emits a
 * top-level "parse_partial_count" (0 on clean runs). When files were flagged:
 *   "parse_partial": {"files":[{path,error_ranges}..(<=50)], "count":N,
 *                     "truncated":bool, "note":"..."}
 * These files WERE indexed — constructs inside the listed 1-based line ranges
 * are missing from the graph because tree-sitter could not parse them. The
 * note spells out the best-effort framing: absence from this list is NOT a
 * completeness guarantee. */
static void add_parse_partial_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                      const cbm_file_error_t *errs, int count) {
    int partials = 0;
    for (int i = 0; i < count; i++) {
        if (is_parse_partial(&errs[i])) {
            partials++;
        }
    }
    yyjson_mut_obj_add_int(doc, root, "parse_partial_count", partials);
    if (!errs || partials <= 0) {
        return;
    }
    yyjson_mut_val *pp = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = 0;
    for (int i = 0; i < count && shown < INDEX_SKIPPED_FILE_CAP; i++) {
        if (!is_parse_partial(&errs[i])) {
            continue;
        }
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", errs[i].path ? errs[i].path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "error_ranges", errs[i].reason ? errs[i].reason : "");
        yyjson_mut_arr_add_val(files, fe);
        shown++;
    }
    yyjson_mut_obj_add_val(doc, pp, "files", files);
    yyjson_mut_obj_add_int(doc, pp, "count", partials);
    yyjson_mut_obj_add_bool(doc, pp, "truncated", partials > INDEX_SKIPPED_FILE_CAP);
    yyjson_mut_obj_add_str(doc, pp, "note",
                           "Indexed, but constructs in these line ranges may be missing (best-"
                           "effort signal); examples only — full list via index_status or "
                           "'logfile'.");
    yyjson_mut_obj_add_val(doc, root, "parse_partial", pp);
}

/* The pipeline persists the complete current coverage set before this
 * response is built. Prefer that set over the per-run errors so incremental
 * runs that do not revisit a flagged file, and artifact bootstraps, do not
 * make existing gaps appear to have vanished. By-design exclusions have
 * their own response surface and are not failures. */
static bool add_persisted_failure_summaries(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                            cbm_store_t *store, const char *project,
                                            const char *logfile) {
    cbm_coverage_row_t *rows = NULL;
    int row_count = 0;
    if (cbm_store_coverage_get(store, project, &rows, &row_count) != CBM_STORE_OK) {
        return false;
    }

    int failure_count = 0;
    for (int i = 0; i < row_count; i++) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "not_indexed_dir") != 0 && strcmp(kind, "not_indexed_file") != 0) {
            failure_count++;
        }
    }

    cbm_file_error_t *failures =
        failure_count > 0 ? calloc((size_t)failure_count, sizeof(*failures)) : NULL;
    if (failure_count > 0 && !failures) {
        cbm_store_free_coverage(rows, row_count);
        return false;
    }

    int n = 0;
    for (int i = 0; i < row_count; i++) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "not_indexed_dir") == 0 || strcmp(kind, "not_indexed_file") == 0) {
            continue;
        }
        failures[n].path = (char *)rows[i].rel_path;
        failures[n].reason = (char *)rows[i].detail;
        failures[n].phase = (char *)rows[i].kind;
        n++;
    }

    add_skipped_summary(doc, root, failures, failure_count, logfile);
    add_parse_partial_summary(doc, root, failures, failure_count);
    free(failures);
    cbm_store_free_coverage(rows, row_count);
    return true;
}

/* Write the FULL (uncapped) skip list to a per-run logfile — ONLY when >=1 file
 * was skipped (no logfile on a clean run). Location:
 *   $CBM_INDEX_LOG (override) else <cache_dir>/logs/<project>-<epoch>.log
 * Returns true and fills out_path on success. */
static bool write_skip_logfile(const char *project, const cbm_file_error_t *errs, int count,
                               char *out_path, size_t out_sz) {
    if (!errs || count <= 0) {
        return false;
    }
    char path[CBM_SZ_1K];
    const char *override = getenv("CBM_INDEX_LOG");
    if (override && override[0]) {
        snprintf(path, sizeof(path), "%s", override);
    } else {
        const char *cdir = cbm_resolve_cache_dir();
        if (!cdir) {
            return false;
        }
        char logdir[CBM_SZ_1K];
        snprintf(logdir, sizeof(logdir), "%s/logs", cdir);
        cbm_mkdir_p(logdir, 0755);
        snprintf(path, sizeof(path), "%s/%s-%lld.log", logdir, project ? project : "index",
                 (long long)time(NULL));
    }
    FILE *f = cbm_fopen(path, "wb");
    if (!f) {
        cbm_log_warn("index.logfile_open_fail", "path", path);
        return false;
    }
    int partials = 0;
    for (int i = 0; i < count; i++) {
        if (is_parse_partial(&errs[i])) {
            partials++;
        }
    }
    (void)fprintf(f, "# codebase-memory-mcp index coverage report\n");
    (void)fprintf(f, "# project=%s skipped=%d parse_partial=%d\n", project ? project : "",
                  count - partials, partials);
    (void)fprintf(f, "# columns: phase\treason\tpath\n");
    for (int i = 0; i < count; i++) {
        (void)fprintf(f, "%s\t%s\t%s\n", errs[i].phase ? errs[i].phase : "",
                      errs[i].reason ? errs[i].reason : "", errs[i].path ? errs[i].path : "");
    }
    (void)fclose(f);
    if (out_path && out_sz) {
        snprintf(out_path, out_sz, "%s", path);
    }
    return true;
}

/* Build the success portion of the index_repository response.
 * Returns true when status should be "degraded" (#334 plausibility gate). */
static bool build_index_success_response(cbm_mcp_server_t *srv, yyjson_mut_doc *doc,
                                         yyjson_mut_val *root, const char *project_name,
                                         const char *repo_path, bool persistence, cbm_pipeline_t *p,
                                         char **excluded_dirs, int excluded_count,
                                         const cbm_file_error_t *file_errors, int file_error_count,
                                         const char *logfile) {
    add_excluded_summary(doc, root, excluded_dirs, excluded_count);
    add_not_indexed_files_summary(doc, root, p);

    int exp_nodes = -1;
    int exp_edges = -1;
    cbm_pipeline_get_committed_counts(p, &exp_nodes, &exp_edges);

    const double ratio = cbm_dump_verify_min_ratio();
    const int min_floor = CBM_DUMP_VERIFY_MIN_FLOOR;

    cbm_store_t *store = resolve_store(srv, project_name);
    if (!store || !add_persisted_failure_summaries(doc, root, store, project_name, logfile)) {
        add_skipped_summary(doc, root, file_errors, file_error_count, logfile);
        add_parse_partial_summary(doc, root, file_errors, file_error_count);
    }
    int nodes = 0;
    int edges = 0;
    bool degraded = false;

    if (!store) {
        degraded = true;
    } else {
        nodes = cbm_store_count_nodes(store, project_name);
        edges = cbm_store_count_edges(store, project_name);
        if (nodes < 0) {
            degraded = true;
            nodes = 0;
            edges = edges >= 0 ? edges : 0;
        } else if (cbm_dump_verify_is_degraded(exp_nodes, nodes, ratio, min_floor)) {
            (void)cbm_store_checkpoint(store);
            int nodes2 = cbm_store_count_nodes(store, project_name);
            int edges2 = cbm_store_count_edges(store, project_name);
            if (nodes2 >= 0) {
                nodes = nodes2;
            }
            if (edges2 >= 0) {
                edges = edges2;
            }
            degraded = cbm_dump_verify_is_degraded(exp_nodes, nodes, ratio, min_floor);
        }
    }

    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);
    if (exp_nodes >= 0) {
        yyjson_mut_obj_add_int(doc, root, "expected_nodes", exp_nodes);
        yyjson_mut_obj_add_int(doc, root, "expected_edges", exp_edges);
    }

    if (degraded) {
        if (!store) {
            yyjson_mut_obj_add_str(doc, root, "hint",
                                   "Index database failed integrity check and was removed. "
                                   "Re-run index_repository(repo_path=...) to rebuild.");
            cbm_log_warn("dump.verify", "reason", "store_missing", "expected_nodes",
                         exp_nodes >= 0 ? "set" : "unknown");
        } else {
            char exp_buf[MCP_FIELD_SIZE];
            char got_buf[MCP_FIELD_SIZE];
            snprintf(exp_buf, sizeof(exp_buf), "%d", exp_nodes);
            snprintf(got_buf, sizeof(got_buf), "%d", nodes);
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "Persisted far fewer nodes than indexed — likely durability loss from a "
                "hard-killed sibling process. Re-run index_repository(repo_path=...) to rebuild.");
            cbm_log_warn("dump.verify", "expected_nodes", exp_buf, "persisted_nodes", got_buf);
        }
    }

    bool adr_exists = project_has_adr(store, project_name, repo_path);
    yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
    if (!adr_exists && !degraded) {
        yyjson_mut_obj_add_str(
            doc, root, "adr_hint",
            "Project indexed. Consider creating an Architecture Decision Record: "
            "explore the codebase with get_architecture(aspects=['all']), then use "
            "manage_adr(mode='update') to persist architectural insights across sessions.");
    }

    bool has_artifact = cbm_artifact_exists(repo_path);
    yyjson_mut_obj_add_bool(doc, root, "artifact_present", has_artifact);
    if (persistence && has_artifact) {
        yyjson_mut_obj_add_str(doc, root, "artifact_hint",
                               "Persistent artifact written to .codebase-memory/graph.db.zst. "
                               "Commit this file to share the index with teammates.");
    }

    return degraded;
}

/* Build the response for a worker that crashed/hung/failed without producing a
 * result. The crash is already contained (this process survived); we report it
 * rather than dying. Precise skip-and-continue (quarantine the culprit, index the
 * rest) is layered on in the probe stage. */
static char *build_worker_failure_response(const char *args, cbm_proc_outcome_t outcome) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "error");
    yyjson_mut_obj_add_str(doc, root, "outcome", cbm_proc_outcome_str(outcome));
    const char *hint = NULL;
    if (outcome == CBM_PROC_SPAWN_FAILED) {
        hint = "Indexing worker could not be started. Supervision is mandatory in a CBM "
               "host, so no in-process fallback was attempted.";
    } else if (outcome == CBM_PROC_HANG) {
        hint = "Indexing worker timed out (a file made no progress). The worker was "
               "terminated and the server survived. Re-run to retry.";
    } else if (outcome == CBM_PROC_CRASH) {
        hint = "Indexing worker crashed on a file. The crash was contained (the server "
               "survived). Re-run to retry; a future release isolates the culprit file.";
    } else if (outcome == CBM_PROC_CLEAN) {
        hint = "Indexing worker exited without a valid response. No in-process fallback "
               "was attempted.";
    } else {
        hint = "Indexing worker did not complete successfully. The failure was contained "
               "and no in-process fallback was attempted.";
    }
    yyjson_mut_obj_add_str(doc, root, "hint", hint);
    if (repo_path) {
        yyjson_mut_obj_add_strcpy(doc, root, "repo_path", repo_path);
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(repo_path);
    char *result = cbm_mcp_text_result(json, true);
    free(json);
    return result;
}

static char *build_worker_unsafe_terminal_response(const char *args, cbm_proc_outcome_t outcome,
                                                   bool cancellation_requested) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "error");
    yyjson_mut_obj_add_str(doc, root, "outcome", cbm_proc_outcome_str(outcome));
    yyjson_mut_obj_add_str(
        doc, root, "hint",
        cancellation_requested
            ? "Indexing worker was cancelled. No in-process retry was started."
            : "Indexing worker process-tree containment failed. No in-process retry was "
              "started; inspect daemon logs.");
    if (repo_path) {
        yyjson_mut_obj_add_strcpy(doc, root, "repo_path", repo_path);
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(repo_path);
    char *response = cbm_mcp_text_result(json, true);
    free(json);
    return response;
}

/* Drop the cached store so the next query reopens whatever the worker wrote (each
 * worker is a fresh process that deletes + recreates the .db). NULL-safe: the
 * background watcher path (main.c) has no MCP server / cached store — the child
 * writes the DB and the parent only needs the return code, so there is nothing
 * to invalidate. */
static void invalidate_cached_store(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;
}

/* Resolve a per-supervisor-run temp path <cache_dir>/logs/.supervisor-<pid><suffix>
 * (falls back to the CWD if the cache dir is unresolvable). Used for the crash-
 * attribution marker and the quarantine list during the recovery re-run. */
static void supervisor_tmp_path(char *out, size_t out_sz, const char *suffix) {
    const char *cdir = cbm_resolve_cache_dir();
    if (cdir && cdir[0]) {
        char logdir[CBM_SZ_1K];
        snprintf(logdir, sizeof(logdir), "%s/logs", cdir);
        cbm_mkdir_p(logdir, 0755);
        snprintf(out, out_sz, "%s/.supervisor-%d%s", logdir, (int)getpid(), suffix);
    } else {
        snprintf(out, out_sz, ".supervisor-%d%s", (int)getpid(), suffix);
    }
}

/* Parse the worker's marker JOURNAL ("S <rel>" / "D <rel>" lines, one event
 * per line — see cbm_index_mark_start/done) into the crash/hang SUSPECT set:
 * files whose last event is an S with no closing D, i.e. the in-flight set
 * at kill time. Recovery runs are PARALLEL, so there are up to worker_count
 * suspects; a torn final line (no trailing newline) is discarded by design.
 * Returns a malloc'd array of malloc'd rel paths, OLDEST OPEN S FIRST (for a
 * hang, the oldest still-open file IS the stuck one). Caller frees via
 * supervisor_free_suspects. */
static char **supervisor_read_suspects(const char *path, int *out_n) {
    *out_n = 0;
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    char **open_paths = NULL; /* open (S-without-D) files in first-S order */
    int open_n = 0;
    int open_cap = 0;
    char line[CBM_SZ_1K];
    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        if (len == 0 || line[len - 1] != '\n') {
            break; /* torn final line — discard and stop */
        }
        line[--len] = '\0';
        if (len > 0 && line[len - 1] == '\r') {
            line[--len] = '\0';
        }
        if (len < 3 || (line[0] != 'S' && line[0] != 'D') || line[1] != ' ') {
            continue;
        }
        const char *rel = line + 2;
        if (line[0] == 'S') {
            bool already = false;
            for (int i = 0; i < open_n && !already; i++) {
                already = strcmp(open_paths[i], rel) == 0;
            }
            if (already) {
                continue;
            }
            if (open_n == open_cap) {
                int ncap = open_cap ? open_cap * 2 : 16;
                char **np = (char **)realloc(open_paths, (size_t)ncap * sizeof(char *));
                if (!np) {
                    break;
                }
                open_paths = np;
                open_cap = ncap;
            }
            open_paths[open_n++] = cbm_strdup(rel);
        } else {
            for (int i = 0; i < open_n; i++) {
                if (strcmp(open_paths[i], rel) == 0) {
                    free(open_paths[i]);
                    memmove(&open_paths[i], &open_paths[i + 1],
                            (size_t)(open_n - i - 1) * sizeof(char *));
                    open_n--;
                    break;
                }
            }
        }
    }
    (void)fclose(f);
    if (open_n == 0) {
        free(open_paths);
        return NULL;
    }
    *out_n = open_n;
    return open_paths;
}

static void supervisor_free_suspects(char **s, int n) {
    if (!s) {
        return;
    }
    for (int i = 0; i < n; i++) {
        free(s[i]);
    }
    free(s);
}

static bool supervisor_suspect_contains(char **s, int n, const char *rel) {
    for (int i = 0; i < n; i++) {
        if (s[i] && strcmp(s[i], rel) == 0) {
            return true;
        }
    }
    return false;
}

/* Append one quarantine entry "rel\tphase\n" (phase = "crash"|"hang"|"error") to the
 * quarantine list. The worker's loader parses this back and reports the skip's
 * phase in skipped[]; a bare "rel" line is still tolerated there (defaults crash). */
static bool supervisor_append_quarantine(const char *path, const char *rel, const char *phase) {
    FILE *f = cbm_fopen(path, "ab");
    if (!f) {
        return false;
    }
    (void)fprintf(f, "%s\t%s\n", rel, phase);
    (void)fclose(f);
    return true;
}

cbm_mcp_supervised_result_disposition_t cbm_mcp_supervised_result_disposition(
    int spawn_result, const cbm_index_worker_result_t *worker_result) {
    if (spawn_result != 0 || !worker_result || worker_result->outcome == CBM_PROC_SPAWN_FAILED) {
        return CBM_MCP_SUPERVISED_RESULT_FALLBACK;
    }
    if (worker_result->cancellation_requested || !worker_result->tree_quiesced ||
        worker_result->supervision_failed) {
        return CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL;
    }
    if (worker_result->outcome == CBM_PROC_CLEAN) {
        return worker_result->response ? CBM_MCP_SUPERVISED_RESULT_SUCCESS
                                       : CBM_MCP_SUPERVISED_RESULT_FALLBACK;
    }
    return CBM_MCP_SUPERVISED_RESULT_CONTAINED_FAILURE;
}

/* Run index_repository in a supervised worker subprocess with skip-and-continue
 * (Stage 3c). Returns the response string (caller frees):
 *   - the worker's own response on a clean first run (the common path);
 *   - after a crash/hang, the response from a clean single-threaded RECOVERY run
 *     that quarantines the culprit file(s) — status="indexed" with them listed in
 *     skipped[] as phase="crash"/"hang"/"error", and the good files indexed;
 *   - a best-effort PARTIAL index (one final quarantine-only run) if the recovery
 *     loop cannot converge but at least one file was quarantined;
 *   - a contained-failure response only if even that cannot produce a clean run.
 * A physical CBM host never falls back to its in-process pipeline: an initial
 * start/protocol failure is returned as an explicit error response. */
/* How many times a failed index worker may be re-run before the server gives
 * up, as CBM_INDEX_MAX_RESTARTS sets it. Default 100. */
static int index_restart_cap(void) {
    enum { INDEX_RESTART_CAP_DEFAULT = 100 };
    long v = 0;
    if (!cbm_env_long("CBM_INDEX_MAX_RESTARTS", &v)) {
        /* Unset is the ordinary case and says nothing. A value that is set but
         * unreadable is a person's intent being dropped, so name it. */
        char raw[CBM_SZ_64] = {0};
        if (cbm_safe_getenv("CBM_INDEX_MAX_RESTARTS", raw, sizeof(raw), NULL) && raw[0]) {
            cbm_log_warn("index.restart_cap.ignored", "value", raw, "action", "using_default");
        }
        return INDEX_RESTART_CAP_DEFAULT;
    }
    /* Zero is a real answer meaning no restarts. The old reader kept the
     * default unless the number was above zero, so the one value somebody sets
     * to leave the worker alone did the opposite. */
    if (v < 0 || v > INT_MAX) {
        cbm_log_warn("index.restart_cap.out_of_range", "action", "using_default");
        return INDEX_RESTART_CAP_DEFAULT;
    }
    return (int)v;
}

int cbm_index_restart_cap_for_testing(void) {
    return index_restart_cap();
}

static char *index_run_supervised(cbm_mcp_server_t *srv, const char *args) {
    invalidate_cached_store(srv);

    /* First attempt: normal parallel run. */
    cbm_index_worker_result_t wr;
    int rc = cbm_index_spawn_worker_with_log_cancel(
        args, false, NULL, NULL, srv ? srv->index_log_callback : NULL,
        srv ? srv->index_log_context : NULL, srv ? &srv->pipeline_cancel_requested : NULL, &wr);
    cbm_mcp_supervised_result_disposition_t disposition =
        cbm_mcp_supervised_result_disposition(rc, &wr);

    if (disposition == CBM_MCP_SUPERVISED_RESULT_FALLBACK) {
        cbm_proc_outcome_t outcome = wr.outcome;
        cbm_index_worker_result_free(&wr);
        invalidate_cached_store(srv);
        return build_worker_failure_response(args, outcome);
    }
    if (disposition == CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL) {
        char *failure =
            build_worker_unsafe_terminal_response(args, wr.outcome, wr.cancellation_requested);
        cbm_index_worker_result_free(&wr);
        invalidate_cached_store(srv);
        return failure;
    }
    if (disposition == CBM_MCP_SUPERVISED_RESULT_SUCCESS) {
        /* Clean exit → transfer the worker's response (the common path). */
        char *resp = wr.response; /* transfer ownership to caller (may be NULL) */
        wr.response = NULL;
        cbm_index_worker_result_free(&wr);
        invalidate_cached_store(srv);
        return resp;
    }

    /* Crash / hang / nonzero exit → skip-and-continue recovery. Re-run the
     * worker PARALLEL (there are no sequential production runs) with the
     * per-file marker JOURNAL armed; after each failed run the journal's
     * open-S set is the in-flight SUSPECT set. A file is quarantined only
     * when it appears in the suspect sets of TWO CONSECUTIVE failed runs
     * (intersection — a stale or merely unlucky in-flight file rotates out),
     * and only ONE file per round: the OLDEST open S in the intersection
     * (for a hang the oldest still-open file IS the stuck one; for a crash
     * it is the longest-running suspect — the best single deterministic
     * pick). A clean run then indexes the good files and reports the
     * quarantined ones as phase="crash"/"hang"/"error" skips via the ordinary
     * Stage-2 skip plumbing. The old design re-ran SINGLE-THREADED to keep
     * one exact marker; at scale that fell into the sequential crawl, went
     * quiet, was killed as a hang mid-pass, and the stale marker got FOUR
     * innocent ms-typescript fixtures quarantined one 15-minute retry at a
     * time. */
    cbm_proc_outcome_t last_outcome = wr.outcome;
    cbm_index_worker_result_free(&wr);

    char marker_path[CBM_SZ_1K];
    char quarantine_path[CBM_SZ_1K];
    supervisor_tmp_path(marker_path, sizeof(marker_path), ".marker");
    supervisor_tmp_path(quarantine_path, sizeof(quarantine_path), ".quarantine");
    (void)remove(marker_path);
    /* Start the quarantine list empty (truncate any stale file). */
    FILE *qinit = cbm_fopen(quarantine_path, "wb");
    if (qinit) {
        (void)fclose(qinit);
    }

    int cap = index_restart_cap();

    char *resp = NULL;
    int quarantined = 0;         /* files pinned + added to the quarantine list so far */
    char **prev_suspects = NULL; /* previous failed round's in-flight set */
    int prev_n = 0;
    bool unsafe_terminal = false;
    bool terminal_cancelled = false;
    for (int i = 0; i < cap; i++) {
        cbm_index_worker_result_t wr2;
        int rc2 = cbm_index_spawn_worker_with_log_cancel(
            args, /*single_thread=*/false, marker_path, quarantine_path,
            srv ? srv->index_log_callback : NULL, srv ? srv->index_log_context : NULL,
            srv ? &srv->pipeline_cancel_requested : NULL, &wr2);
        cbm_mcp_supervised_result_disposition_t recovery_disposition =
            cbm_mcp_supervised_result_disposition(rc2, &wr2);
        if (recovery_disposition == CBM_MCP_SUPERVISED_RESULT_FALLBACK) {
            last_outcome = wr2.outcome;
            cbm_index_worker_result_free(&wr2);
            break; /* spawn failed mid-recovery — give up */
        }
        if (recovery_disposition == CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL) {
            last_outcome = wr2.outcome;
            unsafe_terminal = true;
            terminal_cancelled = wr2.cancellation_requested;
            cbm_index_worker_result_free(&wr2);
            break;
        }
        if (recovery_disposition == CBM_MCP_SUPERVISED_RESULT_SUCCESS) {
            resp = wr2.response; /* transfer ownership to caller */
            wr2.response = NULL;
            cbm_index_worker_result_free(&wr2);
            break; /* good files indexed; quarantined files reported as crash/hang */
        }
        if (wr2.outcome == CBM_PROC_CRASH || wr2.outcome == CBM_PROC_HANG ||
            wr2.outcome == CBM_PROC_EXIT_NONZERO) {
            last_outcome = wr2.outcome;
            cbm_index_worker_result_free(&wr2);
            /* crash vs hang vs nonzero-exit: the phase this file is quarantined
             * under and reported as in skipped[]. A fault signal → "crash"; a
             * no-progress kill → "hang"; a graceful nonzero exit (e.g. an
             * internal parse-limit/abort on a pathological file) → "error".
             * EXIT_NONZERO is attributed via the SAME marker-journal suspect
             * mechanism as a crash: the two-consecutive-strikes intersection
             * below still guards against quarantining an innocent file, and a
             * SYSTEMIC nonzero exit (e.g. a bad arg) produces no recurring
             * suspect → the intersection is empty → give_up (correct). This
             * makes a single pathological file skip-and-continue instead of
             * aborting the whole chunk.
             *
             * Note: A deterministic first-file failure (e.g. worker crashing/exiting
             * on the very first file every run) will progressively quarantine in-flight
             * files until reaching the culprit. This is an existing property accepted
             * by the crash/hang path, accepted here as a considered tradeoff. */
            const char *phase;
            if (last_outcome == CBM_PROC_HANG) {
                phase = "hang";
            } else if (last_outcome == CBM_PROC_EXIT_NONZERO) {
                phase = "error";
            } else {
                phase = "crash";
            }
            int sus_n = 0;
            char **suspects = supervisor_read_suspects(marker_path, &sus_n);
            (void)remove(marker_path); /* fresh journal for the next re-run */
            if (!suspects || sus_n == 0) {
                supervisor_free_suspects(suspects, sus_n);
                cbm_log_warn("index.supervisor.unattributable", "action", "give_up");
                break;
            }
            if (prev_suspects) {
                /* Two-consecutive-strikes: quarantine the OLDEST open S that
                 * was also in flight in the previous failed round. */
                const char *pick = NULL;
                for (int k = 0; k < sus_n && !pick; k++) {
                    if (supervisor_suspect_contains(prev_suspects, prev_n, suspects[k])) {
                        pick = suspects[k];
                    }
                }
                if (!pick) {
                    /* Disjoint consecutive in-flight sets: the failure is not
                     * attributable to a recurring file (systemic) — stop
                     * rather than quarantine an innocent. */
                    supervisor_free_suspects(suspects, sus_n);
                    cbm_log_warn("index.supervisor.unattributable", "action", "give_up");
                    break;
                }
                if (!supervisor_append_quarantine(quarantine_path, pick, phase)) {
                    cbm_log_warn("index.supervisor.quarantine_write_fail", "path", pick);
                    supervisor_free_suspects(suspects, sus_n);
                    break;
                }
                quarantined++;
                char attempt_buf[MCP_FIELD_SIZE];
                snprintf(attempt_buf, sizeof(attempt_buf), "%d", i + 1);
                cbm_log_warn("index.file_quarantined", "path", pick, "outcome", phase, "attempt",
                             attempt_buf);
            }
            supervisor_free_suspects(prev_suspects, prev_n);
            prev_suspects = suspects;
            prev_n = sus_n;
            continue;
        }
        /* SPAWN_FAILED / nonzero exit / non-fault kill → not a crash we can
         * attribute; stop and report a contained failure. */
        last_outcome = wr2.outcome;
        cbm_index_worker_result_free(&wr2);
        break;
    }
    supervisor_free_suspects(prev_suspects, prev_n);

    (void)remove(marker_path); /* marker no longer needed */

    /* Terminal best-effort-partial: the loop exited WITHOUT a clean run (cap
     * exhausted, or an unattributable failure) but at least one file was already
     * quarantined. Try ONE final PARALLEL spawn with the accumulated quarantine
     * and NO marker — every known-bad file short-circuits, so a clean run yields
     * a PARTIAL index (all good files indexed, all known crashers/hangs reported
     * as skips) rather than a hard failure. Bounded by the same quiet-timeout,
     * so it cannot itself hang. Rare given monotonic progress. */
    if (!resp && !unsafe_terminal && quarantined > 0) {
        cbm_index_worker_result_t wrp;
        int rcp = cbm_index_spawn_worker_with_log_cancel(
            args, /*single_thread=*/false, NULL, quarantine_path,
            srv ? srv->index_log_callback : NULL, srv ? srv->index_log_context : NULL,
            srv ? &srv->pipeline_cancel_requested : NULL, &wrp);
        cbm_mcp_supervised_result_disposition_t partial_disposition =
            cbm_mcp_supervised_result_disposition(rcp, &wrp);
        if (partial_disposition == CBM_MCP_SUPERVISED_RESULT_SUCCESS) {
            resp = wrp.response; /* transfer ownership to caller */
            wrp.response = NULL;
            char qn[MCP_FIELD_SIZE];
            snprintf(qn, sizeof(qn), "%d", quarantined);
            cbm_log_error("index.supervisor.partial", "quarantined", qn, "outcome",
                          cbm_proc_outcome_str(last_outcome));
        } else if (partial_disposition == CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL) {
            last_outcome = wrp.outcome;
            unsafe_terminal = true;
            terminal_cancelled = wrp.cancellation_requested;
        }
        cbm_index_worker_result_free(&wrp);
    }

    (void)remove(quarantine_path);
    invalidate_cached_store(srv);

    if (resp) {
        return resp;
    }
    if (unsafe_terminal) {
        return build_worker_unsafe_terminal_response(args, last_outcome, terminal_cancelled);
    }
    return build_worker_failure_response(args, last_outcome);
}

/* Build a minimal {"repo_path": "<root>"} args object (path safely escaped) and
 * run it through index_run_supervised. Shared by the session auto-index (srv
 * present → its cached store is invalidated) and the watcher re-index (srv NULL).
 * Returns the worker's response string (caller frees) or NULL to degrade. */
static char *index_run_supervised_path(cbm_mcp_server_t *srv, const char *root_path) {
    if (!root_path || !root_path[0]) {
        return NULL;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "repo_path", root_path);
    char *args = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (!args) {
        return NULL;
    }
    char *resp = index_run_supervised(srv, args);
    free(args);
    return resp;
}

/* Public entry (see mcp.h): the watcher re-index in main.c has no MCP server, so
 * it reaches the supervised runner through this srv-less wrapper. */
char *cbm_mcp_index_run_supervised_path(const char *root_path) {
    return index_run_supervised_path(NULL, root_path);
}

bool cbm_path_within_root(const char *root_path, const char *abs_path); /* defined below */

/* Resolve relative index requests against an explicitly supplied MCP session
 * root, never against the long-lived daemon process cwd. */
static bool resolve_session_repo_path(cbm_mcp_server_t *srv, char **repo_path) {
    if (!srv || !repo_path || !*repo_path || !srv->allowed_root_policy_set ||
        srv->session_root[0] == '\0' || repo_path_is_absolute(*repo_path)) {
        return true;
    }

    size_t root_len = strlen(srv->session_root);
    size_t path_len = strlen(*repo_path);
    bool needs_separator = root_len > 0 && srv->session_root[root_len - 1] != '/';
    if (root_len > SIZE_MAX - path_len - (needs_separator ? 2U : 1U)) {
        return false;
    }

    size_t joined_size = root_len + (needs_separator ? 1U : 0U) + path_len + 1U;
    char *joined = malloc(joined_size);
    if (!joined) {
        return false;
    }
    (void)snprintf(joined, joined_size, "%s%s%s", srv->session_root, needs_separator ? "/" : "",
                   *repo_path);
    cbm_normalize_path_sep(joined);
    free(*repo_path);
    *repo_path = joined;
    return true;
}

/* Preserve every index option while replacing all caller-supplied repo_path
 * keys with the one canonical path that was actually authorized. */
static char *index_args_with_repo_path(const char *args, const char *canonical_repo_path) {
    if (!args || !canonical_repo_path) {
        return NULL;
    }
    yyjson_doc *source = yyjson_read(args, strlen(args), 0);
    yyjson_val *source_root = source ? yyjson_doc_get_root(source) : NULL;
    if (!source_root || !yyjson_is_obj(source_root)) {
        yyjson_doc_free(source);
        return NULL;
    }

    yyjson_mut_doc *copy = yyjson_doc_mut_copy(source, NULL);
    yyjson_doc_free(source);
    yyjson_mut_val *copy_root = copy ? yyjson_mut_doc_get_root(copy) : NULL;
    if (!copy_root || !yyjson_mut_is_obj(copy_root)) {
        yyjson_mut_doc_free(copy);
        return NULL;
    }
    (void)yyjson_mut_obj_remove_key(copy_root, "repo_path");
    if (!yyjson_mut_obj_add_strcpy(copy, copy_root, "repo_path", canonical_repo_path)) {
        yyjson_mut_doc_free(copy);
        return NULL;
    }
    char *rewritten = yy_doc_to_str(copy);
    yyjson_mut_doc_free(copy);
    return rewritten;
}

/* #1211: index_repository requires repo_path, but list_projects only ever
 * advertises the project NAME, and every read tool accepts that name back via
 * get_project_arg's "project"/"project_name"/"project_id"/"projectName"
 * aliases (mcp.c:1385). Re-indexing an already-indexed project by that same
 * name had no resolution path and fell straight to "repo_path is required".
 * Look up the project's own stored root_path (list_projects proves it's on
 * file) before giving up. Query-only open, always closed here: this never
 * creates a store or touches srv->store/srv->current_project, so it cannot
 * disturb whatever project the server has cached. */
static char *resolved_repo_path_from_project_arg(const char *args) {
    char *project = get_project_arg(args);
    if (!project) {
        return NULL;
    }
    char db_path[CBM_SZ_1K];
    project_db_path(project, db_path, sizeof(db_path));
    cbm_store_t *store = db_path[0] ? cbm_store_open_path_query(db_path) : NULL;
    char *root_path = NULL;
    if (store) {
        cbm_project_t proj = {0};
        if (cbm_store_get_project(store, project, &proj) == CBM_STORE_OK) {
            root_path = proj.root_path ? heap_strdup(proj.root_path) : NULL;
            cbm_project_free_fields(&proj);
        }
        cbm_store_close(store);
    }
    free(project);
    return root_path;
}

static char *handle_index_repository(cbm_mcp_server_t *srv, const char *args) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *name_override = cbm_mcp_get_string_arg(args, "name");
    cbm_normalize_path_sep(repo_path);

    if (!repo_path) {
        repo_path = resolved_repo_path_from_project_arg(args);
        cbm_normalize_path_sep(repo_path);
    }

    if (!repo_path) {
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("repo_path is required", true);
    }

    if (!resolve_session_repo_path(srv, &repo_path)) {
        free(mode_str);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result("failed to resolve repo_path", true);
    }

    repo_path = canonicalize_repo_path_if_exists(repo_path);

    /* Workspace boundary. Embedded/daemon sessions supply their explicit policy,
     * including an explicit NULL meaning unrestricted; a standalone server falls
     * back to the process-wide CBM_ALLOWED_ROOT. The decision itself lives in one
     * shared function so this handler and the HTTP UI indexing route cannot drift
     * apart — they had, and the divergence was the defect. */
    const char *allowed_root =
        srv->allowed_root_policy_set ? srv->allowed_root : getenv("CBM_ALLOWED_ROOT");
    /* repo_path is legitimately absent when the caller names an already-known
     * project instead; the root is resolved downstream. Only a path supplied here
     * is classified here — the previous check had the same tolerance. */
    char boundary_err[CBM_SZ_1K];
    if (repo_path && repo_path[0] &&
        !cbm_workspace_root_allowed(repo_path, cbm_workspace_home_dir(), cbm_workspace_cache_dir(),
                                    allowed_root, boundary_err, sizeof(boundary_err))) {
        free(mode_str);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result(boundary_err, true);
    }

    if (mode_str && strcmp(mode_str, "cross-repo-intelligence") == 0) {
        free(mode_str);
        char *result = handle_cross_repo_mode(srv, repo_path, name_override, args);
        free(name_override);
        free(repo_path);
        return result;
    }

    /* A daemon session delegates the one physical write to its shared job
     * registry only after path canonicalization and workspace authorization. */
    if (srv->index_executor) {
        char *worker_args = index_args_with_repo_path(args, repo_path);
        char *coordinated =
            worker_args ? srv->index_executor(srv->index_executor_context, repo_path, worker_args)
                        : NULL;
        free(worker_args);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return coordinated ? coordinated
                           : cbm_mcp_text_result(
                                 "daemon index coordinator could not start the operation", true);
    }

    /* Resolve the exact project key before choosing supervised or in-process
     * execution. A supervised worker owns the OS mutation lease itself: if the
     * CLI parent is killed, the worker must keep project exclusion until its
     * parent-death watchdog reaps the complete worker tree. */
    char *mutation_project =
        cbm_project_name_from_path(name_override && name_override[0] ? name_override : repo_path);
    if (!mutation_project) {
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("could not resolve index project name", true);
    }

    /* Supervisor gate: validate the canonical path and the host session's
     * workspace policy before handing work to a crash/hang-isolating worker.
     * The parent deliberately owns no project lease on this path; the worker
     * installs the same guard before running the in-process pipeline. A marked
     * host fails closed if preparation or worker startup cannot complete. */
    if (cbm_index_supervisor_should_wrap()) {
        char *worker_args = index_args_with_repo_path(args, repo_path);
        if (!worker_args) {
            free(mutation_project);
            free(repo_path);
            free(mode_str);
            free(name_override);
            return cbm_mcp_text_result("failed to prepare supervised index request", true);
        }
        char *supervised = index_run_supervised(srv, worker_args);
        free(worker_args);
        if (supervised) {
            free(mutation_project);
            free(repo_path);
            free(mode_str);
            free(name_override);
            return supervised;
        }
        free(mutation_project);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result(
            "index supervision failed before a contained worker could start; no "
            "in-process fallback was attempted",
            true);
    }

    if (!mcp_project_mutation_begin(srv, mutation_project)) {
        free(mutation_project);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("index operation blocked by another mutation for this project",
                                   true);
    }
    if (mcp_request_cancelled(srv)) {
        mcp_project_mutation_end(srv, mutation_project);
        free(mutation_project);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("index operation cancelled for this request", true);
    }

    cbm_index_mode_t mode = CBM_MODE_FULL;
    if (mode_str && strcmp(mode_str, "fast") == 0) {
        mode = CBM_MODE_FAST;
    } else if (mode_str && strcmp(mode_str, "moderate") == 0) {
        mode = CBM_MODE_MODERATE;
    }
    free(mode_str);

    bool persistence = cbm_mcp_get_bool_arg(args, "persistence");

    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, NULL, mode);
    if (!p) {
        mcp_project_mutation_end(srv, mutation_project);
        free(mutation_project);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result("failed to create pipeline", true);
    }
    if (name_override && name_override[0] && !cbm_pipeline_set_project_name(p, name_override)) {
        cbm_pipeline_free(p);
        mcp_project_mutation_end(srv, mutation_project);
        free(mutation_project);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result("invalid project name", true);
    }
    free(name_override);
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
    cbm_pipeline_lock();
    cbm_pipeline_bind_cancel_flag(p, &srv->pipeline_cancel_requested);
    srv->active_pipeline = p;
    int rc = cbm_pipeline_run(p);
    srv->active_pipeline = NULL;
    cbm_pipeline_unlock();

    /* Capture the excluded-subtree list (#411) while the pipeline (which owns
     * the strings) is still alive — the response builder copies them into the
     * JSON doc, so they need only outlive that call, not cbm_pipeline_free. */
    char **excluded_dirs = NULL;
    int excluded_count = 0;
    cbm_pipeline_get_excluded(p, &excluded_dirs, &excluded_count);

    /* Capture the per-file skip list (Stage 2 / Track B) while the pipeline
     * still owns the strings; the response builder copies them into the doc. */
    cbm_file_error_t *file_errors = NULL;
    int file_error_count = 0;
    cbm_pipeline_get_file_errors(p, &file_errors, &file_error_count);

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

    if (rc == 0) {
        /* Write the per-run logfile ONLY when there were skips (no logfile on a
         * clean run). The FULL list goes to the file; the JSON caps at 50. */
        char logfile_path[CBM_SZ_1K];
        logfile_path[0] = '\0';
        bool has_logfile = write_skip_logfile(project_name, file_errors, file_error_count,
                                              logfile_path, sizeof(logfile_path));
        bool degraded = build_index_success_response(
            srv, doc, root, project_name, repo_path, persistence, p, excluded_dirs, excluded_count,
            file_errors, file_error_count, has_logfile ? logfile_path : NULL);
        yyjson_mut_obj_add_str(doc, root, "status", degraded ? "degraded" : "indexed");
        if (cbm_pipeline_had_format_migration(p)) {
            yyjson_mut_obj_add_bool(doc, root, "format_migration", true);
        }
    } else {
        yyjson_mut_obj_add_str(doc, root, "status", "error");
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "Pipeline failed. Check repo_path exists and contains source files. "
                               "Try mode='fast' for a quicker diagnostic run.");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    /* Free the pipeline only after the response doc copied the excluded list.
     * Supervised worker: skip the deep free — the process exits right after
     * handing over the response (main.c fast-exits), and piecemeal-freeing a
     * multi-GB graph before process death costs minutes on kernel-scale repos;
     * the OS reclaims it wholesale at exit. In-process paths (tests, kill
     * switch, degrade) still free normally. */
    if (cbm_index_worker_active()) {
        cbm_log_info("index.worker.fast_exit", "skip", "pipeline_free");
    } else {
        cbm_pipeline_free(p);
    }
    free(project_name);
    free(repo_path);

    mcp_project_mutation_end(srv, mutation_project);
    free(mutation_project);

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

/* Resolve an absolute path from root_path + file_path, verify containment,
 * and read source lines. Sets *out_abs_path (caller frees). Returns source
 * string (caller frees) or NULL if path is invalid/unreadable. */
/* True only when abs_path, after realpath/_fullpath resolution (which collapses
 * `..` and resolves symlinks/junctions), stays within root_path. This is the
 * single containment guard every MCP file-read sink must pass before reading a
 * file into a tool response: both the snippet path (resolve_snippet_source) and
 * the search path (attach_result_source) route through it, so a result whose
 * indexed path escapes the project root — via a `..` segment, or a symlink /
 * Windows junction picked up during discovery — is never read back out. */
/* Canonicalize `path` (resolve symlinks/junctions and `..`) into `out`
 * (>= CBM_SZ_4K bytes); returns true on success. Isolating the per-OS resolver
 * keeps cbm_path_within_root's control flow unconditional: the previous `#ifdef`
 * opened the `if (...) {` brace in one branch and a different one in the other,
 * sharing a single close brace — legal C, but it splits the function's braces
 * across preprocessor branches, which defeats source-level tooling that parses
 * without the preprocessor (and left this function unindexed in the graph). */
static bool resolve_canonical_path(const char *path, char *out, size_t out_sz) {
    /* cbm_canonical_path: realpath on POSIX; an opened handle plus
     * GetFinalPathNameByHandleW on Windows.  The old bare _fullpath was ANSI
     * (CJK-locale corruption, #973), accepted nonexistent paths, and lexical
     * expansion alone did not resolve junctions for containment checks. */
    if (!cbm_canonical_path(path, out, out_sz)) {
        return false;
    }
#ifdef _WIN32
    cbm_normalize_path_sep(out);
#endif
    return true;
}

static bool canonical_path_has_root(const char *root_path, const char *candidate_path) {
#ifdef _WIN32
    wchar_t *wide_root = cbm_utf8_to_wide(root_path);
    wchar_t *wide_candidate = cbm_utf8_to_wide(candidate_path);
    bool contained = false;
    if (wide_root && wide_candidate) {
        size_t root_len = wcslen(wide_root);
        size_t candidate_len = wcslen(wide_candidate);
        bool prefix_equal = root_len <= candidate_len && root_len <= INT_MAX &&
                            CompareStringOrdinal(wide_candidate, (int)root_len, wide_root,
                                                 (int)root_len, TRUE) == CSTR_EQUAL;
        bool root_ends_separator =
            root_len > 0 && (wide_root[root_len - 1] == L'/' || wide_root[root_len - 1] == L'\\');
        bool boundary = root_ends_separator || root_len == candidate_len ||
                        (root_len < candidate_len &&
                         (wide_candidate[root_len] == L'/' || wide_candidate[root_len] == L'\\'));
        contained = prefix_equal && boundary;
    }
    free(wide_root);
    free(wide_candidate);
    return contained;
#else
    size_t root_len = strlen(root_path);
    size_t candidate_len = strlen(candidate_path);
    bool prefix_equal =
        root_len <= candidate_len && strncmp(candidate_path, root_path, root_len) == 0;
    bool root_ends_separator = root_len > 0 && root_path[root_len - 1] == '/';
    bool boundary = root_ends_separator || root_len == candidate_len ||
                    (root_len < candidate_len && candidate_path[root_len] == '/');
    return prefix_equal && boundary;
#endif
}

bool cbm_path_within_root(const char *root_path, const char *abs_path) {
    if (!root_path || !abs_path) {
        return false;
    }
    char real_root[CBM_SZ_4K];
    char real_file[CBM_SZ_4K];
    if (resolve_canonical_path(root_path, real_root, sizeof(real_root)) &&
        resolve_canonical_path(abs_path, real_file, sizeof(real_file))) {
        if (canonical_path_has_root(real_root, real_file)) {
            return true;
        }
    }
    return false;
}

static char *resolve_snippet_source(const char *root_path, const char *file_path, int start,
                                    int end, char **out_abs_path) {
    *out_abs_path = NULL;
    if (!root_path || !file_path) {
        return NULL;
    }
    size_t apsz = strlen(root_path) + strlen(file_path) + MCP_SEPARATOR;
    char *abs_path = malloc(apsz);
    snprintf(abs_path, apsz, "%s/%s", root_path, file_path);

    *out_abs_path = abs_path;
    if (cbm_path_within_root(root_path, abs_path)) {
        return read_file_lines(abs_path, start, end);
    }
    return NULL;
}

static bool utf8_is_cont(unsigned char c) {
    return (c & 0xC0) == 0x80;
}

static size_t utf8_sequence_len(const unsigned char *p, const unsigned char *end) {
    size_t remaining = (size_t)(end - p);
    unsigned char c = *p;
    if (c < 0x80) {
        return 1;
    }
    if (c >= 0xC2 && c <= 0xDF && remaining >= 2 && utf8_is_cont(p[1])) {
        return 2;
    }
    if (c == 0xE0 && remaining >= 3 && p[1] >= 0xA0 && p[1] <= 0xBF && utf8_is_cont(p[2])) {
        return 3;
    }
    if (c >= 0xE1 && c <= 0xEC && remaining >= 3 && utf8_is_cont(p[1]) && utf8_is_cont(p[2])) {
        return 3;
    }
    if (c == 0xED && remaining >= 3 && p[1] >= 0x80 && p[1] <= 0x9F && utf8_is_cont(p[2])) {
        return 3;
    }
    if (c >= 0xEE && c <= 0xEF && remaining >= 3 && utf8_is_cont(p[1]) && utf8_is_cont(p[2])) {
        return 3;
    }
    if (c == 0xF0 && remaining >= 4 && p[1] >= 0x90 && p[1] <= 0xBF && utf8_is_cont(p[2]) &&
        utf8_is_cont(p[3])) {
        return 4;
    }
    if (c >= 0xF1 && c <= 0xF3 && remaining >= 4 && utf8_is_cont(p[1]) && utf8_is_cont(p[2]) &&
        utf8_is_cont(p[3])) {
        return 4;
    }
    if (c == 0xF4 && remaining >= 4 && p[1] >= 0x80 && p[1] <= 0x8F && utf8_is_cont(p[2]) &&
        utf8_is_cont(p[3])) {
        return 4;
    }
    return 0;
}

static char *sanitize_utf8_lossy(const char *s) {
    enum { UTF8_REPLACEMENT_LEN = 3 };
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    if (len > (((size_t)-1) - SKIP_ONE) / UTF8_REPLACEMENT_LEN) {
        return NULL;
    }
    char *out = malloc(len * UTF8_REPLACEMENT_LEN + SKIP_ONE);
    if (!out) {
        return NULL;
    }

    const unsigned char *p = (const unsigned char *)s;
    const unsigned char *end = p + len;
    unsigned char *dst = (unsigned char *)out;
    while (p < end) {
        size_t n = utf8_sequence_len(p, end);

        if (n > 0) {
            memcpy(dst, p, n);
            dst += n;
            p += n;
        } else {
            *dst++ = 0xEF;
            *dst++ = 0xBF;
            *dst++ = 0xBD;
            p++;
        }
    }
    *dst = '\0';
    return out;
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

/* get_code_snippet coverage note (#963): if the resolved node's file is
 * flagged parse_partial, warn that the graph may under-report this file.
 * Correlated by construction — the result names its file. (An entirely-
 * skipped file cannot appear here: it has no nodes to resolve a snippet
 * from.) */
static void add_snippet_coverage_note(yyjson_mut_doc *doc, yyjson_mut_val *root_obj,
                                      cbm_store_t *store, const cbm_node_t *node) {
    if (!node->file_path || !node->file_path[0] || !node->project) {
        return;
    }
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    if (cbm_store_coverage_get_path(store, node->project, node->file_path, &rows, &count) !=
        CBM_STORE_OK) {
        return;
    }
    for (int i = 0; i < count; i++) {
        if (rows[i].rel_path && strcmp(rows[i].rel_path, node->file_path) == 0 && rows[i].kind &&
            strcmp(rows[i].kind, "parse_partial") == 0) {
            char note[CBM_SZ_1K];
            snprintf(note, sizeof(note),
                     "This file was only PARTIALLY indexed — line range(s) %s could not be "
                     "parsed, so constructs there may be missing from the graph (callers/callees "
                     "and search results can under-report this file). The source above is ground "
                     "truth. (best-effort signal)",
                     rows[i].detail && rows[i].detail[0] ? rows[i].detail : "?");
            yyjson_mut_obj_add_strcpy(doc, root_obj, "coverage_note", note);
            break;
        }
    }
    cbm_store_free_coverage(rows, count);
}

static int snippet_member_cmp(const void *left, const void *right) {
    const cbm_node_t *a = left;
    const cbm_node_t *b = right;
    if (a->start_line != b->start_line) {
        return a->start_line < b->start_line ? -1 : 1;
    }
    if (a->end_line != b->end_line) {
        return a->end_line < b->end_line ? -1 : 1;
    }
    int qn_cmp = strcmp(a->qualified_name ? a->qualified_name : "",
                        b->qualified_name ? b->qualified_name : "");
    if (qn_cmp != 0) {
        return qn_cmp;
    }
    return a->id < b->id ? -1 : a->id > b->id ? 1 : 0;
}

static void snippet_obj_set_int(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                int value) {
    (void)yyjson_mut_obj_remove_key(obj, key);
    yyjson_mut_obj_add_int(doc, obj, key, value);
}

static void snippet_obj_set_bool(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                 bool value) {
    (void)yyjson_mut_obj_remove_key(obj, key);
    yyjson_mut_obj_add_bool(doc, obj, key, value);
}

static char *snippet_render_payload(yyjson_mut_doc *doc, const char *args) {
    char *json = yy_doc_to_str(doc);
    if (!json || mcp_wants_json(args)) {
        return json;
    }
    char *tree = cbm_json_to_tree(json);
    free(json);
    return tree;
}

static int snippet_source_line_count(const char *source) {
    if (!source || !source[0]) {
        return 0;
    }
    int lines = 1;
    for (const char *p = source; *p; p++) {
        if (*p == '\n' && p[1] != '\0') {
            lines++;
        }
    }
    return lines;
}

/* Copy at most `line_limit` complete physical lines. A single over-budget
 * line therefore yields no source instead of a corrupt byte prefix. */
static char *snippet_source_line_prefix(const char *source, int line_limit) {
    if (!source || line_limit <= 0) {
        return NULL;
    }
    const char *end = source;
    int lines = 0;
    while (*end && lines < line_limit) {
        const char *newline = strchr(end, '\n');
        if (!newline) {
            end += strlen(end);
            break;
        }
        end = newline + 1;
        lines++;
    }
    size_t len = (size_t)(end - source);
    char *copy = malloc(len + 1U);
    if (!copy) {
        return NULL;
    }
    memcpy(copy, source, len);
    copy[len] = '\0';
    return copy;
}

static void snippet_set_source_prefix(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                      const char *whole_source, int line_count, int start,
                                      int original_end) {
    (void)yyjson_mut_obj_remove_key(root, "source");
    (void)yyjson_mut_obj_remove_key(root, "source_omitted");
    (void)yyjson_mut_obj_remove_key(root, "next_start_line");
    (void)yyjson_mut_obj_remove_key(root, "continuation_requires_higher_budget");
    (void)yyjson_mut_obj_remove_key(root, "continuation");
    if (line_count > 0) {
        char *prefix = snippet_source_line_prefix(whole_source, line_count);
        if (prefix) {
            yyjson_mut_obj_add_strcpy(doc, root, "source", prefix);
            free(prefix);
        } else {
            yyjson_mut_obj_add_bool(doc, root, "source_omitted", true);
            line_count = 0;
        }
    } else {
        yyjson_mut_obj_add_bool(doc, root, "source_omitted", true);
    }
    int returned_end = line_count > 0 ? start + line_count - 1 : start;
    snippet_obj_set_int(doc, root, "end_line", returned_end);
    snippet_obj_set_bool(doc, root, "source_truncated", true);
    snippet_obj_set_bool(doc, root, "source_clipped", true);
    (void)yyjson_mut_obj_remove_key(root, "truncation_reason");
    yyjson_mut_obj_add_str(doc, root, "truncation_reason", "output_budget");
    if (line_count > 0) {
        yyjson_mut_obj_add_int(doc, root, "next_start_line", returned_end + 1);
    } else {
        yyjson_mut_obj_add_bool(doc, root, "continuation_requires_higher_budget", true);
        yyjson_mut_obj_add_str(
            doc, root, "continuation",
            "raise max_output_tokens; no complete source line fits the current budget");
    }
    snippet_obj_set_int(doc, root, "original_end_line", original_end);
    snippet_obj_set_int(doc, root, "source_lines_returned", line_count);
}

static char *snippet_budget_floor(bool json_format, int max_output_tokens) {
    if (!json_format) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_str(&sb, "source_mode", "omitted");
        cbm_tree_scalar_bool(&sb, "truncated", true);
        cbm_tree_scalar_str(&sb, "truncation_reason", "output_budget");
        cbm_tree_scalar_int(&sb, "max_output_tokens", max_output_tokens);
        cbm_tree_scalar_bool(&sb, "continuation_requires_higher_budget", true);
        cbm_tree_scalar_str(&sb, "continuation",
                            "raise max_output_tokens; no partial line or identifier was emitted");
        return cbm_sb_finish(&sb);
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "source_mode", "omitted");
    yyjson_mut_obj_add_bool(doc, root, "truncated", true);
    yyjson_mut_obj_add_str(doc, root, "truncation_reason", "output_budget");
    yyjson_mut_obj_add_int(doc, root, "max_output_tokens", max_output_tokens);
    yyjson_mut_obj_add_bool(doc, root, "continuation_requires_higher_budget", true);
    yyjson_mut_obj_add_str(doc, root, "continuation",
                           "raise max_output_tokens; no partial line or identifier was emitted");
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

static char *build_snippet_response(cbm_mcp_server_t *srv, cbm_node_t *node,
                                    const char *match_method, bool include_neighbors,
                                    cbm_node_t *alternatives, int alt_count, const char *args) {
    char *root_path = get_project_root(srv, node->project);

    int original_start = node->start_line > 0 ? node->start_line : SKIP_ONE;
    /* A one-line symbol legitimately has end == start. Treat only missing or
     * inverted end metadata as unknown; expanding a valid one-line node by 50
     * lines both wastes output and lies about the continuation range. */
    int original_end =
        node->end_line >= original_start ? node->end_line : original_start + SNIPPET_DEFAULT_LINES;
    char *source_mode = cbm_mcp_get_string_arg(args, "source_mode");
    bool explicit_full = source_mode && strcmp(source_mode, "full") == 0;
    bool explicit_outline = source_mode && strcmp(source_mode, "outline") == 0;
    bool container =
        node->label && (strcmp(node->label, "File") == 0 || strcmp(node->label, "Module") == 0 ||
                        strcmp(node->label, "Class") == 0 || strcmp(node->label, "Interface") == 0);
    bool outline =
        explicit_outline || (!explicit_full && container && original_end - original_start >= 200);
    free(source_mode);

    int start = outline ? original_start : cbm_mcp_get_int_arg(args, "start_line", original_start);
    if (start < original_start || start > original_end) {
        start = original_start;
    }
    int end = original_end;
    int max_lines = cbm_mcp_get_int_arg(args, "max_lines", 0);
    int max_output_tokens =
        cbm_mcp_get_int_arg(args, "max_output_tokens", explicit_full ? 0 : 2500);
    if (max_output_tokens < 0) {
        max_output_tokens = 0;
    } else if (max_output_tokens > 0 && max_output_tokens < 128) {
        max_output_tokens = 128;
    } else if (max_output_tokens > 1000000) {
        max_output_tokens = 1000000;
    }
    if (max_lines > MCP_SNIPPET_MAX_LINES) {
        max_lines = MCP_SNIPPET_MAX_LINES;
    }
    if (!outline && max_lines > 0 && start + max_lines - 1 < end) {
        end = start + max_lines - 1;
    }
    /* Context-bomb guard: a structural node (Module/File) spans its whole file,
     * so an unclipped read returned the ENTIRE source — a field-eval agent that
     * fell back to a Module snippet pulled 400KB in one call. Cap the line span
     * (far above any real function) and flag it; the exact range is still in
     * start_line/end_line for a targeted re-read. */
    bool snippet_clipped = false;
    if (!outline && end - start + 1 > MCP_SNIPPET_MAX_LINES) {
        end = start + MCP_SNIPPET_MAX_LINES - 1;
        snippet_clipped = true;
    }
    if (!outline && end < original_end) {
        snippet_clipped = true;
    }
    char *abs_path = NULL;
    char *source =
        outline ? NULL : resolve_snippet_source(root_path, node->file_path, start, end, &abs_path);

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
    if (snippet_clipped) {
        yyjson_mut_obj_add_bool(doc, root_obj, "source_truncated", true);
        yyjson_mut_obj_add_bool(doc, root_obj, "source_clipped", true); /* compatibility */
        yyjson_mut_obj_add_int(doc, root_obj, "next_start_line", end + 1);
        yyjson_mut_obj_add_int(doc, root_obj, "original_end_line", original_end);
    }

    yyjson_mut_val *outline_members = NULL;
    int outline_member_offset = 0;
    int outline_members_total = 0;
    int outline_members_returned = 0;
    if (outline) {
        cbm_node_t *members = NULL;
        int member_count = 0;
        (void)cbm_store_find_nodes_by_file(srv->store, node->project, node->file_path, &members,
                                           &member_count);
        if (member_count > 1) {
            qsort(members, (size_t)member_count, sizeof(*members), snippet_member_cmp);
        }
        int member_limit = cbm_mcp_get_int_arg(args, "member_limit", 50);
        int member_offset = cbm_mcp_get_int_arg(args, "member_offset", 0);
        if (member_limit < 1) {
            member_limit = 1;
        } else if (member_limit > 500) {
            member_limit = 500;
        }
        if (member_offset < 0) {
            member_offset = 0;
        }
        int eligible = 0;
        for (int i = 0; i < member_count; i++) {
            if (members[i].id != node->id && members[i].start_line >= original_start &&
                members[i].end_line <= original_end) {
                eligible++;
            }
        }
        yyjson_mut_val *member_rows = yyjson_mut_arr(doc);
        int seen = 0;
        int emitted = 0;
        for (int i = 0; i < member_count && emitted < member_limit; i++) {
            if (members[i].id == node->id || members[i].start_line < original_start ||
                members[i].end_line > original_end) {
                continue;
            }
            if (seen++ < member_offset) {
                continue;
            }
            yyjson_mut_val *member = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, member, "qualified_name",
                                      members[i].qualified_name ? members[i].qualified_name : "");
            yyjson_mut_obj_add_strcpy(doc, member, "label",
                                      members[i].label ? members[i].label : "");
            yyjson_mut_obj_add_int(doc, member, "start_line", members[i].start_line);
            yyjson_mut_obj_add_int(doc, member, "end_line", members[i].end_line);
            yyjson_mut_arr_add_val(member_rows, member);
            emitted++;
        }
        yyjson_mut_obj_add_str(doc, root_obj, "source_mode", "outline");
        yyjson_mut_obj_add_val(doc, root_obj, "members", member_rows);
        yyjson_mut_obj_add_int(doc, root_obj, "members_total", eligible);
        yyjson_mut_obj_add_int(doc, root_obj, "members_returned", emitted);
        yyjson_mut_obj_add_bool(doc, root_obj, "members_has_more",
                                member_offset + emitted < eligible);
        if (member_offset + emitted < eligible) {
            yyjson_mut_obj_add_int(doc, root_obj, "next_member_offset", member_offset + emitted);
        }
        yyjson_mut_obj_add_bool(doc, root_obj, "full_source_available", true);
        outline_members = member_rows;
        outline_member_offset = member_offset;
        outline_members_total = eligible;
        outline_members_returned = emitted;
        cbm_store_free_nodes(members, member_count);
    } else if (source) {
        yyjson_mut_obj_add_str(doc, root_obj, "source_mode", "full");
        char *safe_source = sanitize_utf8_lossy(source);
        if (safe_source) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "source", safe_source);
            free(safe_source);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "source", "(source not available)");
        }
    } else {
        yyjson_mut_obj_add_str(doc, root_obj, "source_mode", "full");
        yyjson_mut_obj_add_str(doc, root_obj, "source", "(source not available)");
    }

    /* match_method — omitted for exact matches */
    if (match_method) {
        yyjson_mut_obj_add_str(doc, root_obj, "match_method", match_method);
    }

    /* No property-blob enrichment: the verbatim source IS the payload here —
     * signature/docstring are literally in it, and the similarity internals
     * (fp/sp/bt) plus metric fields were 41% of the response for zero agent
     * value. Metrics stay reachable via search_graph fields=[...]. */
    yyjson_doc *props_doc = NULL;

    /* Caller/callee counts — store already resolved by calling handler */
    cbm_store_t *store = srv->store;
    int in_deg = 0;
    int out_deg = 0;
    cbm_store_node_degree(store, node->id, &in_deg, &out_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callers", in_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callees", out_deg);

    add_snippet_coverage_note(doc, root_obj, store, node);

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

    bool bounded_default = max_output_tokens > 0;
    size_t snippet_byte_budget =
        bounded_default ? (size_t)max_output_tokens * (size_t)MCP_OUTPUT_BYTES_PER_TOKEN_ESTIMATE
                        : (size_t)-1;
    bool json_format = mcp_wants_json(args);
    char *payload = snippet_render_payload(doc, args);

    /* An outline is already the quality-preserving alternative to dumping a
     * large container. If its member page is still too large, page whole
     * member rows before considering the minimal floor; retain symbol identity
     * and an exact continuation offset throughout. */
    if (bounded_default && payload && strlen(payload) > snippet_byte_budget && outline &&
        outline_members) {
        (void)yyjson_mut_obj_remove_key(root_obj, "truncation_reason");
        yyjson_mut_obj_add_str(doc, root_obj, "truncation_reason", "output_budget");
        while (strlen(payload) > snippet_byte_budget && outline_members_returned > 0) {
            free(payload);
            outline_members_returned--;
            yyjson_mut_arr_remove(outline_members, (size_t)outline_members_returned);
            snippet_obj_set_int(doc, root_obj, "members_returned", outline_members_returned);
            snippet_obj_set_bool(doc, root_obj, "members_has_more",
                                 outline_member_offset + outline_members_returned <
                                     outline_members_total);
            (void)yyjson_mut_obj_remove_key(root_obj, "next_member_offset");
            if (outline_member_offset + outline_members_returned < outline_members_total) {
                yyjson_mut_obj_add_int(doc, root_obj, "next_member_offset",
                                       outline_member_offset + outline_members_returned);
            }
            payload = snippet_render_payload(doc, args);
            if (!payload) {
                break;
            }
        }
    }

    if (bounded_default && payload && strlen(payload) > snippet_byte_budget && !outline && source) {
        char *safe_whole_source = sanitize_utf8_lossy(source);
        int available_lines = snippet_source_line_count(safe_whole_source);
        int low = 0;
        int high = available_lines;
        int best_lines = -1;
        char *best_payload = NULL;
        while (low <= high) {
            int middle = low + (high - low) / 2;
            snippet_set_source_prefix(doc, root_obj, safe_whole_source, middle, start,
                                      original_end);
            char *candidate = snippet_render_payload(doc, args);
            if (candidate && strlen(candidate) <= snippet_byte_budget) {
                free(best_payload);
                best_payload = candidate;
                best_lines = middle;
                low = middle + 1;
            } else {
                free(candidate);
                high = middle - 1;
            }
        }
        free(payload);
        if (best_payload) {
            snippet_set_source_prefix(doc, root_obj, safe_whole_source, best_lines, start,
                                      original_end);
            payload = best_payload;
        } else {
            snippet_set_source_prefix(doc, root_obj, safe_whole_source, 0, start, original_end);
            payload = snippet_render_payload(doc, args);
        }
        free(safe_whole_source);
    }

    /* Large outlines/neighbor/alternative metadata still obey the default
     * ceiling. Do not slice identifiers: if the metadata floor itself cannot
     * fit, return a small continuation record. */
    if (bounded_default && (!payload || strlen(payload) > snippet_byte_budget)) {
        free(payload);
        payload = snippet_budget_floor(json_format, max_output_tokens);
    }
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

    char *result = cbm_mcp_text_result(payload ? payload : "out of memory", payload == NULL);
    free(payload);
    return result;
}

static bool file_outline_request_cancelled(void *context) {
    return mcp_request_cancelled((const cbm_mcp_server_t *)context);
}

static char *handle_get_file_outline(cbm_mcp_server_t *srv, const char *args) {
    const char *args_text = args ? args : "{}";
    yyjson_doc *args_doc = yyjson_read(args_text, strlen(args_text), 0);
    yyjson_val *root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    if (!root || !yyjson_is_obj(root)) {
        yyjson_doc_free(args_doc);
        return cbm_mcp_text_result("get_file_outline arguments must be a JSON object", true);
    }

    char *project = get_project_arg(args_text);
    yyjson_val *file_value = yyjson_obj_get(root, "file_path");
    const char *file_path =
        file_value && yyjson_is_str(file_value) ? yyjson_get_str(file_value) : NULL;
    if (!project) {
        yyjson_doc_free(args_doc);
        return cbm_mcp_text_result("project is required", true);
    }
    if (!file_path || !file_path[0]) {
        free(project);
        yyjson_doc_free(args_doc);
        return cbm_mcp_text_result("file_path is required", true);
    }

    char normalized_path[CBM_SZ_4K];
    if (coverage_normalize_rel(file_path, false, normalized_path, sizeof(normalized_path)) !=
        COVERAGE_PATH_OK) {
        free(project);
        yyjson_doc_free(args_doc);
        return cbm_mcp_text_result("file_path must be a repository-relative path without '..'",
                                   true);
    }

    int limit = 100;
    yyjson_val *limit_value = yyjson_obj_get(root, "limit");
    if (limit_value) {
        if (!yyjson_is_int(limit_value)) {
            free(project);
            yyjson_doc_free(args_doc);
            return cbm_mcp_text_result("limit must be an integer", true);
        }
        int64_t parsed = yyjson_get_sint(limit_value);
        if (parsed < 1 || parsed > CBM_STORE_FILE_OUTLINE_MAX_LIMIT) {
            free(project);
            yyjson_doc_free(args_doc);
            return cbm_mcp_text_result("limit must be between 1 and 200", true);
        }
        limit = (int)parsed;
    }

    int offset = 0;
    yyjson_val *offset_value = yyjson_obj_get(root, "offset");
    if (offset_value) {
        if (!yyjson_is_int(offset_value)) {
            free(project);
            yyjson_doc_free(args_doc);
            return cbm_mcp_text_result("offset must be a non-negative integer", true);
        }
        int64_t parsed = yyjson_get_sint(offset_value);
        if (parsed < 0 || parsed > INT_MAX) {
            free(project);
            yyjson_doc_free(args_doc);
            return cbm_mcp_text_result("offset must be a non-negative integer", true);
        }
        offset = (int)parsed;
    }

    bool json_format = false;
    yyjson_val *format_value = yyjson_obj_get(root, "format");
    if (format_value) {
        const char *format = yyjson_is_str(format_value) ? yyjson_get_str(format_value) : NULL;
        if (!format || (strcmp(format, "tree") != 0 && strcmp(format, "json") != 0)) {
            free(project);
            yyjson_doc_free(args_doc);
            return cbm_mcp_text_result("format must be either 'tree' or 'json'", true);
        }
        json_format = strcmp(format, "json") == 0;
    }

    const char *labels[CBM_STORE_FILE_OUTLINE_MAX_LABELS];
    int label_count = 0;
    yyjson_val *labels_value = yyjson_obj_get(root, "labels");
    if (labels_value) {
        if (!yyjson_is_arr(labels_value) ||
            yyjson_arr_size(labels_value) > CBM_STORE_FILE_OUTLINE_MAX_LABELS) {
            free(project);
            yyjson_doc_free(args_doc);
            return cbm_mcp_text_result("labels must be an array of at most 16 strings", true);
        }
        size_t index = 0;
        size_t max = 0;
        yyjson_val *item;
        yyjson_arr_foreach(labels_value, index, max, item) {
            const char *label = yyjson_is_str(item) ? yyjson_get_str(item) : NULL;
            if (!label || !label[0] || strlen(label) >= CBM_SZ_128) {
                free(project);
                yyjson_doc_free(args_doc);
                return cbm_mcp_text_result("labels must contain only non-empty bounded strings",
                                           true);
            }
            labels[label_count++] = label;
        }
    }

    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        char *error = build_project_list_error("project not found or not indexed");
        char *result = cbm_mcp_text_result(error, true);
        free(error);
        free(project);
        yyjson_doc_free(args_doc);
        return result;
    }
    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        yyjson_doc_free(args_doc);
        return not_indexed;
    }

    cbm_file_outline_row_t *rows = NULL;
    int row_count = 0;
    int total = 0;
    int rc = cbm_store_get_file_outline(store, project, normalized_path, labels, label_count, limit,
                                        offset, file_outline_request_cancelled, srv, &rows,
                                        &row_count, &total);
    if (rc != CBM_STORE_OK) {
        free(project);
        yyjson_doc_free(args_doc);
        if (rc == CBM_STORE_CANCELLED) {
            return cbm_mcp_text_result("get_file_outline cancelled for this request", true);
        }
        if (rc == CBM_STORE_SCAN_LIMIT) {
            return cbm_mcp_text_result("get_file_outline exceeded its fixed output safety limit",
                                       true);
        }
        return cbm_mcp_text_result("get_file_outline query failed", true);
    }

    char *payload = NULL;
    if (!json_format) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_str(&sb, "file_path", normalized_path);
        static const char *const columns[] = {"name", "label", "lines", "qn"};
        cbm_tree_table_header(&sb, "results", row_count, columns, 4);
        for (int i = 0; i < row_count; i++) {
            char lines[CBM_SZ_32];
            if (rows[i].start_line > 0) {
                snprintf(lines, sizeof(lines), "%d-%d", rows[i].start_line,
                         rows[i].end_line > rows[i].start_line ? rows[i].end_line
                                                               : rows[i].start_line);
            } else {
                lines[0] = '\0';
            }
            cbm_tree_row_begin(&sb);
            cbm_tree_cell_str(&sb, rows[i].name, true);
            cbm_tree_cell_str(&sb, rows[i].label, false);
            cbm_tree_cell_str(&sb, lines, false);
            cbm_tree_cell_str(&sb, rows[i].qualified_name, false);
            cbm_tree_row_end(&sb);
        }
        cbm_tree_scalar_int(&sb, "total", total);
        cbm_tree_scalar_int(&sb, "offset", offset);
        cbm_tree_scalar_int(&sb, "limit", limit);
        cbm_tree_scalar_int(&sb, "returned", row_count);
        cbm_tree_scalar_bool(&sb, "has_more", (int64_t)offset + row_count < total);
        payload = cbm_sb_finish(&sb);
    } else {
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *object = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, object);
        yyjson_mut_obj_add_str(doc, object, "file_path", normalized_path);
        yyjson_mut_val *columns = yyjson_mut_arr(doc);
        static const char *const column_names[] = {"name", "label", "lines", "qn"};
        for (size_t i = 0; i < sizeof(column_names) / sizeof(column_names[0]); i++) {
            yyjson_mut_arr_add_str(doc, columns, column_names[i]);
        }
        yyjson_mut_obj_add_val(doc, object, "cols", columns);
        yyjson_mut_val *json_rows = yyjson_mut_arr(doc);
        for (int i = 0; i < row_count; i++) {
            char lines[CBM_SZ_32];
            if (rows[i].start_line > 0) {
                snprintf(lines, sizeof(lines), "%d-%d", rows[i].start_line,
                         rows[i].end_line > rows[i].start_line ? rows[i].end_line
                                                               : rows[i].start_line);
            } else {
                lines[0] = '\0';
            }
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, rows[i].name);
            yyjson_mut_arr_add_strcpy(doc, row, rows[i].label);
            yyjson_mut_arr_add_strcpy(doc, row, lines);
            yyjson_mut_arr_add_strcpy(doc, row, rows[i].qualified_name);
            yyjson_mut_arr_add_val(json_rows, row);
        }
        yyjson_mut_obj_add_val(doc, object, "rows", json_rows);
        yyjson_mut_obj_add_int(doc, object, "total", total);
        yyjson_mut_obj_add_int(doc, object, "offset", offset);
        yyjson_mut_obj_add_int(doc, object, "limit", limit);
        yyjson_mut_obj_add_int(doc, object, "returned", row_count);
        yyjson_mut_obj_add_bool(doc, object, "has_more", (int64_t)offset + row_count < total);
        payload = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    cbm_store_free_file_outline(rows, row_count);
    free(project);
    yyjson_doc_free(args_doc);
    if (!payload) {
        return cbm_mcp_text_result("get_file_outline output allocation failed", true);
    }
    if (strlen(payload) > MCP_FILE_OUTLINE_OUTPUT_MAX) {
        free(payload);
        return cbm_mcp_text_result("get_file_outline exceeded its fixed output safety limit", true);
    }
    char *result = cbm_mcp_text_result(payload, false);
    free(payload);
    return result;
}

static char *handle_get_code_snippet(cbm_mcp_server_t *srv, const char *args) {
    char *qn = cbm_mcp_get_string_arg(args, "qualified_name");
    char *project = get_project_arg(args);
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
        char *result = build_snippet_response(srv, &node, NULL, include_neighbors, NULL, 0, args);
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
        char *result =
            build_snippet_response(srv, &node, "suffix", include_neighbors, NULL, 0, args);
        free_node_contents(&node);
        free(qn);
        free(project);
        return result;
    }

    if (suffix_count > SKIP_ONE) {
        /* Prefer the real definition (a .c body over a .h declaration, a Function
         * over a Module) so an unambiguous-by-preference match resolves directly
         * instead of forcing a disambiguation round trip; only a genuine tie still
         * returns suggestions. */
        bool snip_ambiguous = false;
        int ssel = pick_resolved_node(suffix_nodes, suffix_count, &snip_ambiguous);
        if (!snip_ambiguous) {
            copy_node(&suffix_nodes[ssel], &node);
            cbm_store_free_nodes(suffix_nodes, suffix_count);
            char *result =
                build_snippet_response(srv, &node, "suffix", include_neighbors, NULL, 0, args);
            free_node_contents(&node);
            free(qn);
            free(project);
            return result;
        }
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

/* Intermediate grep match */
typedef struct {
    char *file;
    int line;
    char content[CBM_SZ_1K];
    size_t content_start_byte;
    size_t content_returned_bytes;
    size_t content_total_bytes;
    size_t match_start_byte;
    size_t match_end_byte;
    bool match_known;
    bool content_truncated;
} grep_match_t;

/* Deduped result: one per containing graph node */
typedef struct {
    int64_t node_id; /* 0 = raw match (no containing node) */
    char node_name[CBM_SZ_256];
    char *qualified_name;
    char label[CBM_SZ_64];
    char *file;
    int start_line;
    int end_line;
    int in_degree;
    int out_degree;
    int score;
    int *match_lines;
    int match_count;        /* exact total across the complete grep stream */
    int match_stored_count; /* bounded evidence retained for rendering */
    int match_capacity;
} search_result_t;

typedef struct {
    uint64_t scope_ms;
    uint64_t scan_ms;
    uint64_t enrich_ms;
    uint64_t elapsed_ms;
    bool include_phase_timings;
} search_metrics_t;

#ifdef CBM_ENABLE_TEST_SEAMS
/* Test seams render synthetic rows without a live scan. */
static const search_metrics_t zero_metrics = {0};
#endif

typedef struct {
    char *name;
    int count;
} search_dir_count_t;

static bool set_search_result_identity(search_result_t *result, const cbm_node_t *node);
static bool append_search_match_line(search_result_t *result, int line);

static void free_grep_matches(grep_match_t *matches, int count) {
    if (!matches) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(matches[i].file);
    }
    free(matches);
}

static void free_search_results(search_result_t *results, int count) {
    if (!results) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(results[i].qualified_name);
        free(results[i].file);
        free(results[i].match_lines);
    }
    free(results);
}

static void free_search_dirs(search_dir_count_t *directories, int count) {
    if (!directories) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(directories[i].name);
    }
    free(directories);
}

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
    int score_order = rb->score - ra->score; /* descending */
    if (score_order != 0) {
        return score_order;
    }
    int qn_order = strcmp(ra->qualified_name ? ra->qualified_name : "",
                          rb->qualified_name ? rb->qualified_name : "");
    if (qn_order != 0) {
        return qn_order;
    }
    int file_order = strcmp(ra->file ? ra->file : "", rb->file ? rb->file : "");
    if (file_order != 0) {
        return file_order;
    }
    if (ra->start_line != rb->start_line) {
        return ra->start_line < rb->start_line ? -1 : 1;
    }
    if (ra->end_line != rb->end_line) {
        return ra->end_line < rb->end_line ? -1 : 1;
    }
    return 0;
}

/* Moving an arbitrary file_pattern ahead of Select-String is not generally results-preserving:
 * the current Windows path applies PowerShell -like to the full MatchInfo.Path, while POSIX
 * delegates glob semantics to grep --include. Restrict the Windows optimization to plain suffix
 * globs whose meaning cannot depend on path separator normalization or directory components. The
 * original post-scan filter remains in place as a second guard. */
bool cbm_search_code_file_pattern_can_prefilter(const char *file_pattern) {
    if (!file_pattern || file_pattern[0] != '*' || file_pattern[1] != '.' ||
        file_pattern[2] == '\0') {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)file_pattern + 2; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '.' || *p == '_' || *p == '-')) {
            return false;
        }
    }
    return true;
}

bool cbm_search_code_windows_path_matches_prefilter(const char *path, const char *file_pattern) {
    if (!path || !cbm_search_code_file_pattern_can_prefilter(file_pattern)) {
        return false;
    }

    const char *suffix = file_pattern + 1;
    size_t path_len = strlen(path);
    size_t suffix_len = strlen(suffix);
    if (path_len < suffix_len) {
        return false;
    }

    const unsigned char *candidate = (const unsigned char *)path + path_len - suffix_len;
    const unsigned char *expected = (const unsigned char *)suffix;
    for (size_t i = 0; i < suffix_len; i++) {
        unsigned char left = candidate[i];
        unsigned char right = expected[i];
        if (left >= 'A' && left <= 'Z') {
            left = (unsigned char)(left - 'A' + 'a');
        }
        if (right >= 'A' && right <= 'Z') {
            right = (unsigned char)(right - 'A' + 'a');
        }
        if (left != right) {
            return false;
        }
    }
    return true;
}

/* Build the grep/search command string based on scoped vs recursive mode.
 * On Windows, uses PowerShell Select-String with tab-delimited output.
 * On POSIX, uses grep with colon-delimited output. */
/* Windows PowerShell 5.1 encodes stdout for a native-process pipe in the
 * console OEM codepage, so any character the inherited CP cannot carry
 * (Cyrillic under CP437/850, etc.) degrades to '?' before it ever reaches
 * collect_grep_matches — and WHETHER it degrades depends on the console the
 * server happened to inherit. Pin the pipe to UTF-8 inside every generated
 * command so raw search content is codepage-independent. (The read side is
 * already safe: Select-String decodes BOM-less UTF-8 via .NET StreamReader
 * defaults.) */
#define CBM_PS_UTF8_PRELUDE "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; "

void cbm_search_code_build_grep_cmd(char *cmd, size_t cmd_sz, bool use_regex, bool scoped,
                                    const char *file_pattern, const char *tmpfile,
                                    const char *filelist, const char *root_path) {
#ifdef _WIN32
    const char *sm = use_regex ? "" : " -SimpleMatch";
    if (scoped) {
        if (file_pattern) {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"" CBM_PS_UTF8_PRELUDE
                "$pat = Get-Content -Encoding UTF8 -LiteralPath '%s'; "
                "Get-Content -Encoding UTF8 -LiteralPath '%s' | ForEach-Object { Select-String "
                "-LiteralPath $_ -Pattern $pat%s "
                "-ErrorAction Stop }"
                " | Where-Object { $_.Path -like '*%s' }"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                tmpfile, filelist, sm, file_pattern);
        } else {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"" CBM_PS_UTF8_PRELUDE
                "$pat = Get-Content -Encoding UTF8 -LiteralPath '%s'; "
                "Get-Content -Encoding UTF8 -LiteralPath '%s' | ForEach-Object { Select-String "
                "-LiteralPath $_ -Pattern $pat%s "
                "-ErrorAction Stop }"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                tmpfile, filelist, sm);
        }
    } else {
        if (file_pattern) {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"" CBM_PS_UTF8_PRELUDE
                "Get-ChildItem -Recurse -Path '%s\\*' -Include '%s' -File "
                "-ErrorAction SilentlyContinue"
                " | Select-String -Pattern (Get-Content -Encoding UTF8 -LiteralPath '%s')%s "
                "-ErrorAction Stop"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                root_path, file_pattern, tmpfile, sm);
        } else {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"" CBM_PS_UTF8_PRELUDE
                "Get-ChildItem -Recurse -Path '%s\\*' -File -ErrorAction "
                "SilentlyContinue"
                " | Select-String -Pattern (Get-Content -Encoding UTF8 -LiteralPath '%s')%s "
                "-ErrorAction Stop"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                root_path, tmpfile, sm);
        }
    }
#else
    const char *flag = use_regex ? "-E" : "-F";
    if (scoped) {
        /* file_pattern was already applied to the canonical file list in C.
         * Keep this scan compatible with BusyBox grep, which has no GNU
         * --include option (the shipped Alpine/static runtime). */
        snprintf(cmd, cmd_sz,
                 "xargs -0 sh -c 'pat=$1; shift; if [ \"$#\" -eq 0 ]; then exit 0; fi; "
                 "grep -Hn %s -f \"$pat\" -- \"$@\"; rc=$?; if [ \"$rc\" -eq 1 ]; then "
                 "exit 0; fi; if [ \"$rc\" -ne 0 ]; then exit 255; fi; exit 0' sh '%s' "
                 "< '%s' 2>/dev/null",
                 flag, tmpfile, filelist);
    } else {
        /* Do not pipe discovery directly into sort/xargs: POSIX sh reports only
         * the final pipeline command, which can hide a partial find or failed
         * sort behind a successful grep. Materialize the NUL list in the
         * request-private scratch file and check each producer before scanning.
         * The xargs wrapper's zero-argument guard also makes empty discovery
         * succeed on both GNU (runs once) and BSD (runs zero times) xargs. */
        if (file_pattern) {
            snprintf(cmd, cmd_sz,
                     "fl='%s'; find '%s' -type f -name '%s' -print0 > \"$fl\" 2>/dev/null; "
                     "rc=$?; if [ \"$rc\" -ne 0 ]; then exit \"$rc\"; fi; "
                     "LC_ALL=C sort -z -o \"$fl\" \"$fl\" 2>/dev/null; rc=$?; "
                     "if [ \"$rc\" -ne 0 ]; then exit \"$rc\"; fi; "
                     "xargs -0 sh -c 'pat=$1; shift; if [ \"$#\" -eq 0 ]; then exit 0; fi; "
                     "grep -Hn %s -f \"$pat\" -- \"$@\"; rc=$?; if [ \"$rc\" -eq 1 ]; then "
                     "exit 0; fi; if [ \"$rc\" -ne 0 ]; then exit 255; fi; exit 0' sh '%s' "
                     "< \"$fl\" 2>/dev/null",
                     filelist, root_path, file_pattern, flag, tmpfile);
        } else {
            snprintf(cmd, cmd_sz,
                     "fl='%s'; find '%s' -type f -print0 > \"$fl\" 2>/dev/null; rc=$?; "
                     "if [ \"$rc\" -ne 0 ]; then exit \"$rc\"; fi; "
                     "LC_ALL=C sort -z -o \"$fl\" \"$fl\" 2>/dev/null; rc=$?; "
                     "if [ \"$rc\" -ne 0 ]; then exit \"$rc\"; fi; "
                     "xargs -0 sh -c 'pat=$1; shift; if [ \"$#\" -eq 0 ]; then exit 0; fi; "
                     "grep -Hn %s -f \"$pat\" -- \"$@\"; rc=$?; if [ \"$rc\" -eq 1 ]; then "
                     "exit 0; fi; if [ \"$rc\" -ne 0 ]; then exit 255; fi; exit 0' sh '%s' "
                     "< \"$fl\" 2>/dev/null",
                     filelist, root_path, flag, tmpfile);
        }
    }
#endif
}

/* Build deduplicated file list from search results + raw matches. */
static yyjson_mut_val *build_dedup_files_array(yyjson_mut_doc *doc, search_result_t *sr,
                                               int result_start, int output_count,
                                               grep_match_t *raw, int raw_start, int raw_count) {
    (void)raw_start;
    yyjson_mut_val *files_arr = yyjson_mut_arr(doc);
    size_t seen_capacity = (size_t)output_count + (size_t)raw_count;
    const char **seen_files = seen_capacity > 0 ? calloc(seen_capacity, sizeof(*seen_files)) : NULL;
    if (!files_arr || (seen_capacity > 0 && !seen_files)) {
        free(seen_files);
        return NULL;
    }
    int seen_count = 0;
    for (int fi = 0; fi < output_count; fi++) {
        int result_index = result_start + fi;
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], sr[result_index].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            seen_files[seen_count++] = sr[result_index].file;
            yyjson_mut_arr_add_str(doc, files_arr, sr[result_index].file);
        }
    }
    for (int fi = 0; fi < raw_count; fi++) {
        /* `raw` contains only the requested page. raw_start is the global
         * offset used for continuation metadata, not an array index. */
        int raw_index = fi;
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], raw[raw_index].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            seen_files[seen_count++] = raw[raw_index].file;
            yyjson_mut_arr_add_str(doc, files_arr, raw[raw_index].file);
        }
    }
    free(seen_files);
    return files_arr;
}

/* Attach source or context lines to a search result JSON item. */
static void attach_result_source(yyjson_mut_doc *doc, yyjson_mut_val *item, search_result_t *r,
                                 int mode, int context_lines, int source_max_lines,
                                 const char *root_path) {
    enum { MODE_FULL = 1 };
    if (r->start_line <= 0 || r->end_line <= 0) {
        return;
    }
    size_t root_length = strlen(root_path);
    size_t file_length = strlen(r->file);
    if (root_length > SIZE_MAX - file_length - 2U) {
        return;
    }
    char *abs_path = malloc(root_length + file_length + 2U);
    if (!abs_path) {
        return;
    }
    memcpy(abs_path, root_path, root_length);
    abs_path[root_length] = '/';
    memcpy(abs_path + root_length + 1U, r->file, file_length + 1U);

    /* Containment: a search result whose indexed path resolves outside the
     * project root (a `..` segment, or a symlink/junction that discovery
     * followed) must not be read back into the response. Same guard the
     * snippet path already uses. */
    if (!cbm_path_within_root(root_path, abs_path)) {
        free(abs_path);
        return;
    }

    if (mode == MODE_FULL) {
        /* Cap each hit's source at a match-anchored window: uncapped
         * whole-symbol dumps ran to 5.7KB × N hits (142KB responses). The
         * complete symbol stays one get_code_snippet call away;
         * source_start/source_truncated make the cut explicit. */
        enum { SC_FULL_LEAD = 5 };
        if (source_max_lines <= 0) {
            yyjson_mut_obj_add_bool(doc, item, "source_omitted", true);
            free(abs_path);
            return;
        }
        int s = r->start_line;
        int e = r->end_line;
        bool truncated = false;
        if (e - s + 1 > source_max_lines) {
            if (r->match_count > 0 && r->match_lines[0] - SC_FULL_LEAD > s) {
                s = r->match_lines[0] - SC_FULL_LEAD;
            }
            e = s + source_max_lines - 1;
            if (e > r->end_line) {
                e = r->end_line;
            }
            truncated = true;
        }
        char *source = read_file_lines(abs_path, s, e);
        if (source) {
            char *safe_source = sanitize_utf8_lossy(source);
            if (safe_source) {
                yyjson_mut_obj_add_strcpy(doc, item, "source", safe_source);
                free(safe_source);
            }
            free(source);
            if (truncated) {
                yyjson_mut_obj_add_int(doc, item, "source_start", s);
                yyjson_mut_obj_add_bool(doc, item, "source_truncated", true);
            }
        }
    } else if (context_lines > 0 && r->match_count > 0) {
        int ctx_start = r->match_lines[0] - context_lines;
        int ctx_end = r->match_lines[r->match_count - SKIP_ONE] + context_lines;
        if (ctx_start < SKIP_ONE) {
            ctx_start = SKIP_ONE;
        }
        char *ctx = read_file_lines(abs_path, ctx_start, ctx_end);
        if (ctx) {
            char *safe_context = sanitize_utf8_lossy(ctx);
            if (safe_context) {
                yyjson_mut_obj_add_strcpy(doc, item, "context", safe_context);
                free(safe_context);
            }
            yyjson_mut_obj_add_int(doc, item, "context_start", ctx_start);
            free(ctx);
        }
    }
    free(abs_path);
}

/* Aggregate hits by top-level directory. Directory keys are allocation-backed:
 * truncating them used to merge distinct directories that shared their first
 * 127 bytes, making the diagnostic count look exact when it was not. */
static bool aggregate_search_dirs(search_result_t *sr, int sr_count,
                                  search_dir_count_t **directories_out, int *count_out) {
    *directories_out = NULL;
    *count_out = 0;
    if (sr_count <= 0) {
        return true;
    }
    search_dir_count_t *directories = calloc((size_t)sr_count, sizeof(*directories));
    if (!directories) {
        return false;
    }
    int dir_n = 0;
    for (int di = 0; di < sr_count; di++) {
        const char *file = sr[di].file ? sr[di].file : "";
        const char *slash = strchr(file, '/');
        size_t directory_length = slash ? (size_t)(slash - file + SKIP_ONE) : strlen(file);
        int found = CBM_NOT_FOUND;
        for (int d = 0; d < dir_n; d++) {
            if (strlen(directories[d].name) == directory_length &&
                memcmp(directories[d].name, file, directory_length) == 0) {
                found = d;
                break;
            }
        }
        if (found >= 0) {
            directories[found].count++;
        } else {
            directories[dir_n].name = cbm_strndup(file, directory_length);
            if (!directories[dir_n].name) {
                free_search_dirs(directories, dir_n);
                return false;
            }
            directories[dir_n].count = SKIP_ONE;
            dir_n++;
        }
    }
    *directories_out = directories;
    *count_out = dir_n;
    return true;
}

static bool raw_content_has_next(const grep_match_t *match) {
    return match->content_start_byte + match->content_returned_bytes < match->content_total_bytes;
}

static bool raw_match_fully_returned(const grep_match_t *match) {
    if (!match->match_known) {
        return false;
    }
    size_t content_end = match->content_start_byte + match->content_returned_bytes;
    return match->content_start_byte <= match->match_start_byte &&
           content_end >= match->match_end_byte;
}

static int search_match_lines_shown(const search_result_t *result, int match_limit) {
    return result->match_stored_count < match_limit ? result->match_stored_count : match_limit;
}

static char *search_match_lines_text(const search_result_t *result, int match_limit) {
    int shown = search_match_lines_shown(result, match_limit);
    size_t capacity = (size_t)shown * 16U + 1U;
    char *text = malloc(capacity);
    if (!text) {
        return NULL;
    }
    size_t used = 0;
    text[0] = '\0';
    for (int i = 0; i < shown; i++) {
        int written = snprintf(text + used, capacity - used, "%s%d", i > 0 ? ";" : "",
                               result->match_lines[i]);
        if (written < 0 || (size_t)written >= capacity - used) {
            free(text);
            return NULL;
        }
        used += (size_t)written;
    }
    return text;
}

/* TOON emission for compact-mode search results: one row per hit
 * (qn/label/file/lines/matches/degrees — `node` dropped, it duplicates the
 * qn's last segment), a raw[] table for uncorrelated matches, a dirs[]
 * distribution table, and the summary scalars. */
static char *assemble_search_output_toon(search_result_t *sr, int sr_count, grep_match_t *raw,
                                         int raw_count, int raw_content_truncated, int gm_count,
                                         int result_start, int result_limit, int output_count,
                                         int raw_start, int raw_output, int directory_start,
                                         int directory_output, int raw_limit, int directory_limit,
                                         int match_limit, bool scan_saturated,
                                         bool warn_literal_pipe, const search_metrics_t *metrics,
                                         bool budget_hit, int source_lines_returned) {
    enum { SEARCH_SLOW_MS = 5000 };
    cbm_sb_t sb;
    cbm_sb_init(&sb);

    static const char *const cols[] = {"qn",      "label",           "file", "lines",
                                       "matches", "matches_omitted", "in",   "out"};
    typedef struct {
        char lines[CBM_SZ_32];
        char *matches;
        char matches_omitted[CBM_SZ_32];
        char inbound[CBM_SZ_32];
        char outbound[CBM_SZ_32];
    } search_tree_row_t;
    search_tree_row_t *rendered =
        output_count > 0 ? calloc((size_t)output_count, sizeof(*rendered)) : NULL;
    const char **cells =
        output_count > 0 ? calloc((size_t)output_count * 8U, sizeof(*cells)) : NULL;
    if (output_count > 0 && (!rendered || !cells)) {
        free(cells);
        free(rendered);
        cbm_sb_free(&sb);
        return NULL;
    }
    for (int ri = 0; ri < output_count; ri++) {
        search_result_t *r = &sr[result_start + ri];
        if (r->start_line > 0) {
            snprintf(rendered[ri].lines, sizeof(rendered[ri].lines), "%d-%d", r->start_line,
                     r->end_line > r->start_line ? r->end_line : r->start_line);
        }
        rendered[ri].matches = search_match_lines_text(r, match_limit);
        if (!rendered[ri].matches) {
            for (int j = 0; j < ri; j++) {
                free(rendered[j].matches);
            }
            free(cells);
            free(rendered);
            cbm_sb_free(&sb);
            return NULL;
        }
        snprintf(rendered[ri].matches_omitted, sizeof(rendered[ri].matches_omitted), "%d",
                 r->match_count - search_match_lines_shown(r, match_limit));
        snprintf(rendered[ri].inbound, sizeof(rendered[ri].inbound), "%d", r->in_degree);
        snprintf(rendered[ri].outbound, sizeof(rendered[ri].outbound), "%d", r->out_degree);
        size_t base = (size_t)ri * 8U;
        cells[base] = r->qualified_name;
        cells[base + 1] = r->label;
        cells[base + 2] = r->file;
        cells[base + 3] = rendered[ri].lines;
        cells[base + 4] = rendered[ri].matches;
        cells[base + 5] = rendered[ri].matches_omitted;
        cells[base + 6] = rendered[ri].inbound;
        cells[base + 7] = rendered[ri].outbound;
    }
    if (output_count > 0 && rendered && cells) {
        static const bool string_cols[] = {true, true, true, true, true, false, false, false};
        static const bool prefix_cols[] = {true, false, true, false, false, false, false, false};
        cbm_tree_table_rows_profiled(&sb, "results", output_count, cols, 8, cells, string_cols,
                                     prefix_cols);
    } else {
        cbm_tree_table_header(&sb, "results", 0, cols, 8);
    }
    for (int ri = 0; ri < output_count; ri++) {
        free(rendered[ri].matches);
    }
    free(cells);
    free(rendered);

    if (raw_output > 0) {
        static const char *const rcols[] = {"file",
                                            "line",
                                            "content",
                                            "content_start_byte",
                                            "content_returned_bytes",
                                            "content_total_bytes",
                                            "match_start_byte",
                                            "match_end_byte",
                                            "content_has_more",
                                            "content_next_offset",
                                            "match_fully_returned"};
        enum { RAW_COLS = 11, RAW_TEXT_FIELDS = 9 };
        const char **raw_cells = calloc((size_t)raw_output * RAW_COLS, sizeof(*raw_cells));
        char (*raw_text)[RAW_TEXT_FIELDS][CBM_SZ_32] =
            calloc((size_t)raw_output, sizeof(*raw_text));
        if (!raw_cells || !raw_text) {
            free(raw_text);
            free(raw_cells);
            cbm_sb_free(&sb);
            return NULL;
        }
        for (int ri = 0; ri < raw_output; ri++) {
            int raw_index = ri;
            grep_match_t *r = &raw[raw_index];
            snprintf(raw_text[ri][0], sizeof(raw_text[ri][0]), "%d", r->line);
            snprintf(raw_text[ri][1], sizeof(raw_text[ri][1]), "%zu", r->content_start_byte);
            snprintf(raw_text[ri][2], sizeof(raw_text[ri][2]), "%zu", r->content_returned_bytes);
            snprintf(raw_text[ri][3], sizeof(raw_text[ri][3]), "%zu", r->content_total_bytes);
            if (r->match_known) {
                snprintf(raw_text[ri][4], sizeof(raw_text[ri][4]), "%zu", r->match_start_byte);
                snprintf(raw_text[ri][5], sizeof(raw_text[ri][5]), "%zu", r->match_end_byte);
            }
            snprintf(raw_text[ri][6], sizeof(raw_text[ri][6]), "%s",
                     raw_content_has_next(r) ? "true" : "false");
            if (raw_content_has_next(r)) {
                snprintf(raw_text[ri][7], sizeof(raw_text[ri][7]), "%zu",
                         r->content_start_byte + r->content_returned_bytes);
            }
            snprintf(raw_text[ri][8], sizeof(raw_text[ri][8]), "%s",
                     raw_match_fully_returned(r) ? "true" : "false");
            size_t base = (size_t)ri * RAW_COLS;
            raw_cells[base] = r->file;
            raw_cells[base + 1U] = raw_text[ri][0];
            raw_cells[base + 2U] = r->content;
            for (size_t field = 1; field < RAW_TEXT_FIELDS; field++) {
                raw_cells[base + field + 2U] = raw_text[ri][field];
            }
        }
        static const bool raw_string_cols[] = {true,  false, true,  false, false, false,
                                               false, false, false, false, false};
        static const bool raw_prefix_cols[] = {true,  false, false, false, false, false,
                                               false, false, false, false, false};
        cbm_tree_table_rows_profiled(&sb, "raw", raw_output, rcols, RAW_COLS, raw_cells,
                                     raw_string_cols, raw_prefix_cols);
        free(raw_text);
        free(raw_cells);
    }

    search_dir_count_t *directories = NULL;
    int dir_n = 0;
    if (!aggregate_search_dirs(sr, sr_count, &directories, &dir_n)) {
        cbm_sb_free(&sb);
        return NULL;
    }
    if (directory_start > dir_n) {
        directory_start = dir_n;
    }
    if (directory_output > dir_n - directory_start) {
        directory_output = dir_n - directory_start;
    }
    if (directory_output > 0) {
        static const char *const dcols[] = {"dir", "hits"};
        const char **dir_cells = calloc((size_t)directory_output * 2U, sizeof(*dir_cells));
        char (*count_text)[CBM_SZ_32] = calloc((size_t)directory_output, sizeof(*count_text));
        if (!dir_cells || !count_text) {
            free(count_text);
            free(dir_cells);
            free_search_dirs(directories, dir_n);
            cbm_sb_free(&sb);
            return NULL;
        }
        for (int d = 0; d < directory_output; d++) {
            int directory_index = directory_start + d;
            snprintf(count_text[d], sizeof(count_text[d]), "%d",
                     directories[directory_index].count);
            dir_cells[(size_t)d * 2U] = directories[directory_index].name;
            dir_cells[(size_t)d * 2U + 1U] = count_text[d];
        }
        static const bool dir_string_cols[] = {true, false};
        static const bool dir_prefix_cols[] = {true, false};
        cbm_tree_table_rows_profiled(&sb, "directories", directory_output, dcols, 2, dir_cells,
                                     dir_string_cols, dir_prefix_cols);
        free(count_text);
        free(dir_cells);
    }
    free_search_dirs(directories, dir_n);

    cbm_tree_scalar_int(&sb, "total_grep_matches", gm_count);
    cbm_tree_scalar_int(&sb, "total_results", sr_count);
    cbm_tree_scalar_int(&sb, "raw_match_count", raw_count);
    cbm_tree_scalar_str(&sb, "total_relation", scan_saturated ? "gte" : "eq");
    cbm_tree_scalar_int(&sb, "result_offset", result_start);
    cbm_tree_scalar_int(&sb, "results_returned", output_count);
    bool has_more = result_start + output_count < sr_count;
    cbm_tree_scalar_bool(&sb, "has_more", has_more);
    if (has_more && output_count > 0) {
        cbm_tree_scalar_int(&sb, "next_offset", result_start + output_count);
    } else if (has_more) {
        cbm_tree_scalar_bool(&sb,
                             result_limit == 0 ? "result_continuation_requires_positive_limit"
                                               : "result_continuation_requires_higher_budget",
                             true);
    }
    cbm_tree_scalar_int(&sb, "raw_returned", raw_output);
    bool raw_has_more = raw_start + raw_output < raw_count;
    cbm_tree_scalar_bool(&sb, "raw_has_more", raw_has_more);
    if (raw_has_more && raw_output > 0) {
        cbm_tree_scalar_int(&sb, "raw_next_offset", raw_start + raw_output);
    } else if (raw_has_more) {
        cbm_tree_scalar_bool(&sb,
                             raw_limit == 0 ? "raw_continuation_requires_positive_limit"
                                            : "raw_continuation_requires_higher_budget",
                             true);
    }
    if (raw_content_truncated > 0) {
        cbm_tree_scalar_int(&sb, "raw_content_truncated", raw_content_truncated);
    }
    cbm_tree_scalar_int(&sb, "directories_total", dir_n);
    cbm_tree_scalar_int(&sb, "directories_returned", directory_output);
    bool directories_has_more = directory_start + directory_output < dir_n;
    cbm_tree_scalar_bool(&sb, "directories_has_more", directories_has_more);
    if (directories_has_more && directory_output > 0) {
        cbm_tree_scalar_int(&sb, "directory_next_offset", directory_start + directory_output);
    } else if (directories_has_more) {
        cbm_tree_scalar_bool(&sb,
                             directory_limit == 0 ? "directory_continuation_requires_positive_limit"
                                                  : "directory_continuation_requires_higher_budget",
                             true);
    }
    if (scan_saturated) {
        cbm_tree_scalar_bool(&sb, "scan_saturated", true);
    }
    cbm_tree_scalar_bool(&sb, "truncated",
                         has_more || raw_has_more || directories_has_more || scan_saturated ||
                             budget_hit);
    if (budget_hit) {
        cbm_tree_scalar_str(&sb, "truncation_reason", "output_budget");
    }
    if (source_lines_returned >= 0) {
        cbm_tree_scalar_int(&sb, "source_max_lines_returned", source_lines_returned);
    }
    if (metrics->include_phase_timings) {
        cbm_tree_scalar_int(&sb, "scope_ms", (long long)metrics->scope_ms);
        cbm_tree_scalar_int(&sb, "scan_ms", (long long)metrics->scan_ms);
        cbm_tree_scalar_int(&sb, "enrich_ms", (long long)metrics->enrich_ms);
    }
    cbm_tree_scalar_int(&sb, "elapsed_ms", (long long)metrics->elapsed_ms);
    if (warn_literal_pipe) {
        cbm_tree_scalar_str(&sb, "warning",
                            "pattern contains '|' but regex=false, so it is matched literally "
                            "(not as alternation). Pass regex=true for 'foo|bar' to mean "
                            "'foo OR bar'.");
    }
    if (metrics->elapsed_ms >= SEARCH_SLOW_MS) {
        cbm_tree_scalar_str(&sb, "warning_slow",
                            "search was slow; narrow file_pattern/path_filter or use a more "
                            "specific pattern");
    }
    return cbm_sb_finish(&sb);
}

/* Phase 4: assemble JSON output from search results */
static char *assemble_search_output(
    search_result_t *sr, int sr_count, grep_match_t *raw, int raw_count, int raw_content_truncated,
    int gm_count, int result_start, int result_limit, int output_count, int raw_start,
    int raw_output, int directory_start, int directory_output, int raw_limit, int directory_limit,
    int match_limit, int mode, int context_lines, int source_max_lines, const char *root_path,
    bool scan_saturated, bool warn_literal_pipe, const search_metrics_t *metrics, bool budget_hit) {
    enum { MODE_COMPACT = 0, MODE_FULL = 1, MODE_FILES = 2, SEARCH_SLOW_MS = 5000 };

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    if (mode == MODE_FILES) {
        yyjson_mut_val *files = build_dedup_files_array(doc, sr, result_start, output_count, raw,
                                                        raw_start, raw_output);
        if (!files) {
            yyjson_mut_doc_free(doc);
            return NULL;
        }
        yyjson_mut_obj_add_val(doc, root_obj, "files", files);
    } else {
        /* json-stringified tree: cols + column-ordered row arrays. FULL mode
         * appends a per-row object cell with the (guarded, windowed) source;
         * context requests append the corresponding context object. */
        bool attach_context = context_lines > 0 && mode != MODE_FULL;
        yyjson_mut_val *jcols = yyjson_mut_arr(doc);
        static const char *const sc_cols[] = {"qn",      "label",           "file", "lines",
                                              "matches", "matches_omitted", "in",   "out"};
        for (size_t ci = 0; ci < sizeof(sc_cols) / sizeof(sc_cols[0]); ci++) {
            yyjson_mut_arr_add_str(doc, jcols, sc_cols[ci]);
        }
        if (mode == MODE_FULL || attach_context) {
            yyjson_mut_arr_add_str(doc, jcols, mode == MODE_FULL ? "source" : "context");
        }
        yyjson_mut_obj_add_val(doc, root_obj, "cols", jcols);

        yyjson_mut_val *results_arr = yyjson_mut_arr(doc);
        for (int ri = 0; ri < output_count; ri++) {
            search_result_t *r = &sr[result_start + ri];
            char lines[CBM_SZ_32];
            if (r->start_line > 0) {
                snprintf(lines, sizeof(lines), "%d-%d", r->start_line,
                         r->end_line > r->start_line ? r->end_line : r->start_line);
            } else {
                lines[0] = '\0';
            }
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, r->qualified_name);
            yyjson_mut_arr_add_strcpy(doc, row, r->label);
            yyjson_mut_arr_add_strcpy(doc, row, r->file);
            yyjson_mut_arr_add_strcpy(doc, row, lines);
            yyjson_mut_val *ml = yyjson_mut_arr(doc);
            int matches_shown = search_match_lines_shown(r, match_limit);
            for (int j = 0; j < matches_shown; j++) {
                yyjson_mut_arr_add_int(doc, ml, r->match_lines[j]);
            }
            yyjson_mut_arr_add_val(row, ml);
            yyjson_mut_arr_add_int(doc, row, r->match_count - matches_shown);
            yyjson_mut_arr_add_int(doc, row, r->in_degree);
            yyjson_mut_arr_add_int(doc, row, r->out_degree);
            if (mode == MODE_FULL || attach_context) {
                yyjson_mut_val *src = yyjson_mut_obj(doc);
                attach_result_source(doc, src, r, mode, context_lines, source_max_lines, root_path);
                yyjson_mut_arr_add_val(row, src);
            }
            yyjson_mut_arr_add_val(results_arr, row);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "rows", results_arr);

        yyjson_mut_val *raw_obj = yyjson_mut_obj(doc);
        yyjson_mut_val *rcols = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_str(doc, rcols, "file");
        yyjson_mut_arr_add_str(doc, rcols, "line");
        yyjson_mut_arr_add_str(doc, rcols, "content");
        yyjson_mut_arr_add_str(doc, rcols, "content_start_byte");
        yyjson_mut_arr_add_str(doc, rcols, "content_returned_bytes");
        yyjson_mut_arr_add_str(doc, rcols, "content_total_bytes");
        yyjson_mut_arr_add_str(doc, rcols, "match_start_byte");
        yyjson_mut_arr_add_str(doc, rcols, "match_end_byte");
        yyjson_mut_arr_add_str(doc, rcols, "content_has_more");
        yyjson_mut_arr_add_str(doc, rcols, "content_next_offset");
        yyjson_mut_arr_add_str(doc, rcols, "match_fully_returned");
        yyjson_mut_obj_add_val(doc, raw_obj, "cols", rcols);
        yyjson_mut_val *raw_arr = yyjson_mut_arr(doc);
        for (int ri = 0; ri < raw_output; ri++) {
            int raw_index = ri;
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_str(doc, row, raw[raw_index].file);
            yyjson_mut_arr_add_int(doc, row, raw[raw_index].line);
            yyjson_mut_arr_add_str(doc, row, raw[raw_index].content);
            yyjson_mut_arr_add_uint(doc, row, raw[raw_index].content_start_byte);
            yyjson_mut_arr_add_uint(doc, row, raw[raw_index].content_returned_bytes);
            yyjson_mut_arr_add_uint(doc, row, raw[raw_index].content_total_bytes);
            if (raw[raw_index].match_known) {
                yyjson_mut_arr_add_uint(doc, row, raw[raw_index].match_start_byte);
                yyjson_mut_arr_add_uint(doc, row, raw[raw_index].match_end_byte);
            } else {
                yyjson_mut_arr_add_null(doc, row);
                yyjson_mut_arr_add_null(doc, row);
            }
            yyjson_mut_arr_add_bool(doc, row, raw_content_has_next(&raw[raw_index]));
            if (raw_content_has_next(&raw[raw_index])) {
                yyjson_mut_arr_add_uint(doc, row,
                                        raw[raw_index].content_start_byte +
                                            raw[raw_index].content_returned_bytes);
            } else {
                yyjson_mut_arr_add_null(doc, row);
            }
            yyjson_mut_arr_add_bool(doc, row, raw_match_fully_returned(&raw[raw_index]));
            yyjson_mut_arr_add_val(raw_arr, row);
        }
        yyjson_mut_obj_add_val(doc, raw_obj, "rows", raw_arr);
        yyjson_mut_obj_add_val(doc, root_obj, "raw_matches", raw_obj);
    }

    search_dir_count_t *directories = NULL;
    int dir_total = 0;
    if (!aggregate_search_dirs(sr, sr_count, &directories, &dir_total)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    if (directory_start > dir_total) {
        directory_start = dir_total;
    }
    if (directory_output > dir_total - directory_start) {
        directory_output = dir_total - directory_start;
    }
    yyjson_mut_val *directory_object = yyjson_mut_obj(doc);
    for (int d = 0; d < directory_output; d++) {
        int directory_index = directory_start + d;
        yyjson_mut_val *key = yyjson_mut_strcpy(doc, directories[directory_index].name);
        yyjson_mut_val *value = yyjson_mut_int(doc, directories[directory_index].count);
        yyjson_mut_obj_add(directory_object, key, value);
    }
    yyjson_mut_obj_add_val(doc, root_obj, "directories", directory_object);
    free_search_dirs(directories, dir_total);

    /* Summary stats */
    yyjson_mut_obj_add_int(doc, root_obj, "total_grep_matches", gm_count);
    yyjson_mut_obj_add_int(doc, root_obj, "total_results", sr_count);
    yyjson_mut_obj_add_int(doc, root_obj, "raw_match_count", raw_count);
    yyjson_mut_obj_add_str(doc, root_obj, "total_relation", scan_saturated ? "gte" : "eq");
    yyjson_mut_obj_add_int(doc, root_obj, "result_offset", result_start);
    yyjson_mut_obj_add_int(doc, root_obj, "results_returned", output_count);
    bool has_more = result_start + output_count < sr_count;
    yyjson_mut_obj_add_bool(doc, root_obj, "has_more", has_more);
    if (has_more && output_count > 0) {
        yyjson_mut_obj_add_int(doc, root_obj, "next_offset", result_start + output_count);
    } else if (has_more) {
        yyjson_mut_obj_add_bool(doc, root_obj,
                                result_limit == 0 ? "result_continuation_requires_positive_limit"
                                                  : "result_continuation_requires_higher_budget",
                                true);
    }
    yyjson_mut_obj_add_int(doc, root_obj, "raw_returned", raw_output);
    bool raw_has_more = raw_start + raw_output < raw_count;
    yyjson_mut_obj_add_bool(doc, root_obj, "raw_has_more", raw_has_more);
    if (raw_has_more && raw_output > 0) {
        yyjson_mut_obj_add_int(doc, root_obj, "raw_next_offset", raw_start + raw_output);
    } else if (raw_has_more) {
        yyjson_mut_obj_add_bool(doc, root_obj,
                                raw_limit == 0 ? "raw_continuation_requires_positive_limit"
                                               : "raw_continuation_requires_higher_budget",
                                true);
    }
    if (raw_content_truncated > 0) {
        yyjson_mut_obj_add_int(doc, root_obj, "raw_content_truncated", raw_content_truncated);
    }
    yyjson_mut_obj_add_int(doc, root_obj, "directories_total", dir_total);
    yyjson_mut_obj_add_int(doc, root_obj, "directories_returned", directory_output);
    bool directories_has_more = directory_start + directory_output < dir_total;
    yyjson_mut_obj_add_bool(doc, root_obj, "directories_has_more", directories_has_more);
    if (directories_has_more && directory_output > 0) {
        yyjson_mut_obj_add_int(doc, root_obj, "directory_next_offset",
                               directory_start + directory_output);
    } else if (directories_has_more) {
        yyjson_mut_obj_add_bool(doc, root_obj,
                                directory_limit == 0
                                    ? "directory_continuation_requires_positive_limit"
                                    : "directory_continuation_requires_higher_budget",
                                true);
    }
    if (scan_saturated) {
        yyjson_mut_obj_add_bool(doc, root_obj, "scan_saturated", true);
    }
    yyjson_mut_obj_add_bool(doc, root_obj, "truncated",
                            has_more || raw_has_more || directories_has_more || scan_saturated ||
                                budget_hit);
    if (budget_hit) {
        yyjson_mut_obj_add_str(doc, root_obj, "truncation_reason", "output_budget");
    }
    if (mode == MODE_FULL) {
        yyjson_mut_obj_add_int(doc, root_obj, "source_max_lines_returned", source_max_lines);
    }
    if (metrics->include_phase_timings) {
        yyjson_mut_obj_add_uint(doc, root_obj, "scope_ms", metrics->scope_ms);
        yyjson_mut_obj_add_uint(doc, root_obj, "scan_ms", metrics->scan_ms);
        yyjson_mut_obj_add_uint(doc, root_obj, "enrich_ms", metrics->enrich_ms);
    }
    yyjson_mut_obj_add_uint(doc, root_obj, "elapsed_ms", metrics->elapsed_ms);
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
    if (metrics->elapsed_ms >= SEARCH_SLOW_MS) {
        char slow[CBM_SZ_128];
        snprintf(slow, sizeof(slow),
                 "search took %dms (>%ds); narrow file_pattern/path_filter or use a more "
                 "specific pattern",
                 (int)metrics->elapsed_ms, SEARCH_SLOW_MS / 1000);
        yyjson_mut_arr_add_strcpy(doc, warnings, slow);
        char ems[CBM_SZ_32];
        snprintf(ems, sizeof(ems), "%d", (int)metrics->elapsed_ms);
        cbm_log_warn("search.slow", "elapsed_ms", ems); /* visibility in logs */
    }
    if (yyjson_mut_arr_size(warnings) > 0) {
        yyjson_mut_obj_add_val(doc, root_obj, "warnings", warnings);
    }

    char *json = yy_doc_to_str(doc);
    if (json) {
        char *safe_json = sanitize_utf8_lossy(json);
        if (safe_json) {
            free(json);
            json = safe_json;
        }
    }
    yyjson_mut_doc_free(doc);

    return json;
}

/* Render the same search response model in either direct JSON or the lean tree
 * form. Compact search has a purpose-built table renderer so repeated paths
 * and qualified-name prefixes can use the response-local directory. Full and
 * files mode first build the canonical JSON model, then project it to tree. */
static char *render_search_payload(search_result_t *sr, int sr_count, grep_match_t *raw,
                                   int raw_count, int raw_content_truncated, int gm_count,
                                   int result_start, int result_limit, int output_count,
                                   int raw_start, int raw_output, int directory_start,
                                   int directory_output, int raw_limit, int directory_limit,
                                   int match_limit, int mode, int context_lines,
                                   int source_max_lines, const char *root_path, bool scan_saturated,
                                   bool warn_literal_pipe, const search_metrics_t *metrics,
                                   bool budget_hit, bool json_format) {
    if (mode == 0 && !json_format) {
        return assemble_search_output_toon(sr, sr_count, raw, raw_count, raw_content_truncated,
                                           gm_count, result_start, result_limit, output_count,
                                           raw_start, raw_output, directory_start, directory_output,
                                           raw_limit, directory_limit, match_limit, scan_saturated,
                                           warn_literal_pipe, metrics, budget_hit, -1);
    }

    char *json = assemble_search_output(
        sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start, result_limit,
        output_count, raw_start, raw_output, directory_start, directory_output, raw_limit,
        directory_limit, match_limit, mode, context_lines, source_max_lines, root_path,
        scan_saturated, warn_literal_pipe, metrics, budget_hit);
    if (!json || json_format) {
        return json;
    }
    char *tree = cbm_json_to_tree(json);
    free(json);
    return tree;
}

#ifdef CBM_ENABLE_TEST_SEAMS
char *cbm_mcp_render_search_rows_for_testing(const char *const *qualified_names,
                                             const char *const *file_paths, int row_count,
                                             bool json_format) {
    if (!qualified_names || !file_paths || row_count < 0) {
        return NULL;
    }
    search_result_t *results = row_count > 0 ? calloc((size_t)row_count, sizeof(*results)) : NULL;
    if (row_count > 0 && !results) {
        return NULL;
    }
    int initialized = 0;
    for (int i = 0; i < row_count; i++) {
        cbm_node_t node = {.id = i + SKIP_ONE,
                           .label = "Function",
                           .name = "synthetic",
                           .qualified_name = qualified_names[i],
                           .file_path = file_paths[i],
                           .start_line = SKIP_ONE,
                           .end_line = SKIP_ONE};
        if (!set_search_result_identity(&results[i], &node)) {
            free_search_results(results, initialized);
            return NULL;
        }
        if (!append_search_match_line(&results[i], SKIP_ONE)) {
            free_search_results(results, initialized + 1);
            return NULL;
        }
        initialized++;
    }
    char *payload =
        render_search_payload(results, row_count, NULL, 0, 0, row_count, 0, row_count, row_count, 0,
                              0, 0, row_count, 0, row_count > 0 ? 1 : 0, row_count > 0 ? 1 : 0, 0,
                              0, 0, "", false, false, &zero_metrics, false, json_format);
    free_search_results(results, initialized);
    return payload;
}
#endif

/* A pathological single qualified name, path, or source line can be larger
 * than the caller's entire budget. Never byte-slice it: return a small,
 * truthful floor that tells the caller how to request the omitted rows. */
static char *search_budget_floor(int sr_count, int raw_count, int gm_count, int result_start,
                                 int result_limit, int raw_start, int raw_limit,
                                 int directory_start, int directory_limit, int directory_total,
                                 bool scan_saturated, bool json_format, int max_output_tokens) {
    (void)gm_count;
    (void)max_output_tokens;
    bool result_has_more = result_start < sr_count;
    bool raw_has_more = raw_start < raw_count;
    bool directories_has_more = directory_start < directory_total;
    if (!json_format) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_int(&sb, "total_results", sr_count);
        cbm_tree_scalar_int(&sb, "raw_match_count", raw_count);
        cbm_tree_scalar_int(&sb, "result_offset", result_start);
        cbm_tree_scalar_int(&sb, "results_returned", 0);
        cbm_tree_scalar_bool(&sb, "has_more", result_has_more);
        if (result_has_more) {
            cbm_tree_scalar_bool(&sb,
                                 result_limit == 0 ? "result_continuation_requires_positive_limit"
                                                   : "result_continuation_requires_higher_budget",
                                 true);
        }
        cbm_tree_scalar_int(&sb, "raw_offset", raw_start);
        cbm_tree_scalar_int(&sb, "raw_returned", 0);
        cbm_tree_scalar_bool(&sb, "raw_has_more", raw_has_more);
        if (raw_has_more && raw_limit == 0) {
            cbm_tree_scalar_bool(&sb, "raw_continuation_requires_positive_limit", true);
        }
        cbm_tree_scalar_int(&sb, "directories_total", directory_total);
        cbm_tree_scalar_int(&sb, "directory_offset", directory_start);
        cbm_tree_scalar_int(&sb, "directories_returned", 0);
        cbm_tree_scalar_bool(&sb, "directories_has_more", directories_has_more);
        if (directories_has_more && directory_limit == 0) {
            cbm_tree_scalar_bool(&sb, "directory_continuation_requires_positive_limit", true);
        }
        if (scan_saturated) {
            cbm_tree_scalar_bool(&sb, "scan_saturated", true);
        }
        cbm_tree_scalar_bool(&sb, "truncated", true);
        cbm_tree_scalar_str(&sb, "truncation_reason", "output_budget");
        return cbm_sb_finish(&sb);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "total_results", sr_count);
    yyjson_mut_obj_add_int(doc, root, "raw_match_count", raw_count);
    yyjson_mut_obj_add_int(doc, root, "result_offset", result_start);
    yyjson_mut_obj_add_int(doc, root, "results_returned", 0);
    yyjson_mut_obj_add_bool(doc, root, "has_more", result_has_more);
    if (result_has_more) {
        yyjson_mut_obj_add_bool(doc, root,
                                result_limit == 0 ? "result_continuation_requires_positive_limit"
                                                  : "result_continuation_requires_higher_budget",
                                true);
    }
    yyjson_mut_obj_add_int(doc, root, "raw_offset", raw_start);
    yyjson_mut_obj_add_int(doc, root, "raw_returned", 0);
    yyjson_mut_obj_add_bool(doc, root, "raw_has_more", raw_has_more);
    if (raw_has_more && raw_limit == 0) {
        yyjson_mut_obj_add_bool(doc, root, "raw_continuation_requires_positive_limit", true);
    }
    yyjson_mut_obj_add_int(doc, root, "directories_total", directory_total);
    yyjson_mut_obj_add_int(doc, root, "directory_offset", directory_start);
    yyjson_mut_obj_add_int(doc, root, "directories_returned", 0);
    yyjson_mut_obj_add_bool(doc, root, "directories_has_more", directories_has_more);
    if (directories_has_more && directory_limit == 0) {
        yyjson_mut_obj_add_bool(doc, root, "directory_continuation_requires_positive_limit", true);
    }
    if (scan_saturated) {
        yyjson_mut_obj_add_bool(doc, root, "scan_saturated", true);
    }
    yyjson_mut_obj_add_bool(doc, root, "truncated", true);
    yyjson_mut_obj_add_str(doc, root, "truncation_reason", "output_budget");
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
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

static bool search_match_bounds(const char *content, const char *pattern, bool use_regex,
                                const cbm_regex_t *compiled_regex, size_t *start_out,
                                size_t *end_out) {
    if (!content || !pattern || !start_out || !end_out) {
        return false;
    }
    if (!use_regex) {
        const char *match = strstr(content, pattern);
        if (!match) {
            return false;
        }
        *start_out = (size_t)(match - content);
        *end_out = *start_out + strlen(pattern);
        return true;
    }
    if (!compiled_regex) {
        return false;
    }
    cbm_regmatch_t match = {.rm_so = -1, .rm_eo = -1};
    if (cbm_regexec(compiled_regex, content, 1, &match, 0) != CBM_REG_OK || match.rm_so < 0 ||
        match.rm_eo < match.rm_so) {
        return false;
    }
    *start_out = (size_t)match.rm_so;
    *end_out = (size_t)match.rm_eo;
    return true;
}

static bool search_utf8_continuation_byte(unsigned char byte) {
    return (byte & 0xC0U) == 0x80U;
}

/* A continuation-shaped byte is not necessarily part of valid UTF-8. Adjust
 * paging only when a complete sequence actually spans the requested boundary;
 * malformed bytes remain independently addressable original source bytes. */
static size_t search_utf8_page_start(const char *content, size_t total, size_t start) {
    if (start >= total || !search_utf8_continuation_byte((unsigned char)content[start])) {
        return start;
    }
    size_t earliest = start > 3U ? start - 3U : 0U;
    for (size_t candidate = start; candidate > earliest;) {
        candidate--;
        if (search_utf8_continuation_byte((unsigned char)content[candidate])) {
            continue;
        }
        size_t sequence = utf8_sequence_len((const unsigned char *)content + candidate,
                                            (const unsigned char *)content + total);
        if (sequence > 1U && candidate + sequence > start) {
            return candidate + sequence;
        }
        break;
    }
    return start;
}

static size_t search_utf8_page_end(const char *content, size_t total, size_t end) {
    if (end >= total || !search_utf8_continuation_byte((unsigned char)content[end])) {
        return end;
    }
    size_t earliest = end > 3U ? end - 3U : 0U;
    for (size_t candidate = end; candidate > earliest;) {
        candidate--;
        if (search_utf8_continuation_byte((unsigned char)content[candidate])) {
            continue;
        }
        size_t sequence = utf8_sequence_len((const unsigned char *)content + candidate,
                                            (const unsigned char *)content + total);
        if (sequence > 1U && candidate + sequence > end) {
            return candidate;
        }
        break;
    }
    return end;
}

/* Select one bounded raw-line preview. By default it contains the complete
 * match whenever the match itself fits. An explicit content offset pages the
 * original line independently of the raw-row cursor. Boundaries inside valid
 * UTF-8 code points are adjusted and reported; malformed bytes remain exactly
 * addressable and are reversibly encoded by the shared output boundary. */
static void search_raw_preview(grep_match_t *match, const char *content,
                               bool raw_content_offset_set, size_t raw_content_offset,
                               bool match_known, size_t match_start, size_t match_end) {
    const size_t preview_capacity = sizeof(match->content) - SKIP_ONE;
    size_t total = strlen(content);
    size_t start = 0;
    if (raw_content_offset_set) {
        start = raw_content_offset < total ? raw_content_offset : total;
    } else if (match_known && total > preview_capacity) {
        size_t match_length = match_end - match_start;
        if (match_length >= preview_capacity) {
            start = match_start;
        } else {
            size_t lead = (preview_capacity - match_length) / 2U;
            start = match_start > lead ? match_start - lead : 0;
            if (start + preview_capacity > total) {
                start = total - preview_capacity;
            }
        }
    }
    start = search_utf8_page_start(content, total, start);

    size_t remaining = total - start;
    size_t returned = remaining < preview_capacity ? remaining : preview_capacity;
    size_t end = start + returned;
    if (end < total) {
        end = search_utf8_page_end(content, total, end);
    }
    returned = end - start;
    memcpy(match->content, content + start, returned);
    match->content[returned] = '\0';
    match->content_start_byte = start;
    match->content_returned_bytes = returned;
    match->content_total_bytes = total;
    match->match_start_byte = match_start;
    match->match_end_byte = match_end;
    match->match_known = match_known;
    match->content_truncated = start > 0 || end < total;
}

#ifdef CBM_ENABLE_TEST_SEAMS
char *cbm_mcp_render_raw_preview_for_testing(const char *content, bool content_offset_set,
                                             size_t content_offset, bool json_format) {
    if (!content) {
        return NULL;
    }
    grep_match_t raw = {.file = "fixture.raw", .line = 7};
    search_raw_preview(&raw, content, content_offset_set, content_offset, false, 0, 0);
    return render_search_payload(NULL, 0, &raw, 1, raw.content_truncated ? 1 : 0, 1, 0, 0, 0, 0, 1,
                                 0, 0, 1, 0, 0, 0, 0, 0, "", false, false, &zero_metrics, false,
                                 json_format);
}
#endif

/* Find the tightest node containing a line in a file. Returns index or -1. */
static int find_tightest_node(cbm_node_t *nodes, int count, int line) {
    int best = CBM_NOT_FOUND;
    int best_span = MAX_LINE_SPAN;
    for (int j = 0; j < count; j++) {
        if (nodes[j].start_line <= line && nodes[j].end_line >= line) {
            int span = nodes[j].end_line - nodes[j].start_line;
            const char *candidate_qn = nodes[j].qualified_name ? nodes[j].qualified_name : "";
            const char *best_qn =
                best >= 0 && nodes[best].qualified_name ? nodes[best].qualified_name : "";
            bool stable_tie_winner =
                span == best_span &&
                (best < 0 || strcmp(candidate_qn, best_qn) < 0 ||
                 (strcmp(candidate_qn, best_qn) == 0 && nodes[j].id < nodes[best].id));
            if (span < best_span || stable_tie_winner) {
                best = j;
                best_span = span;
            }
        }
    }
    return best;
}

static bool set_search_result_identity(search_result_t *result, const cbm_node_t *node) {
    result->qualified_name = heap_strdup(node->qualified_name ? node->qualified_name : "");
    result->file = heap_strdup(node->file_path ? node->file_path : "");
    if (!result->qualified_name || !result->file) {
        free(result->qualified_name);
        free(result->file);
        result->qualified_name = NULL;
        result->file = NULL;
        return false;
    }
    result->node_id = node->id;
    snprintf(result->node_name, sizeof(result->node_name), "%s", node->name ? node->name : "");
    snprintf(result->label, sizeof(result->label), "%s", node->label ? node->label : "");
    result->start_line = node->start_line;
    result->end_line = node->end_line;
    return true;
}

static bool append_search_match_line(search_result_t *result, int line) {
    enum { SEARCH_MATCH_LINES_RETAINED = 500 };
    if (result->match_count == INT_MAX) {
        return false;
    }
    result->match_count++;
    if (result->match_stored_count >= SEARCH_MATCH_LINES_RETAINED) {
        return true;
    }
    if (result->match_stored_count >= result->match_capacity) {
        int next_capacity = result->match_capacity > 0 ? result->match_capacity * PAIR_LEN : 8;
        if (next_capacity > SEARCH_MATCH_LINES_RETAINED) {
            next_capacity = SEARCH_MATCH_LINES_RETAINED;
        }
        if (next_capacity < result->match_capacity ||
            (size_t)next_capacity > SIZE_MAX / sizeof(*result->match_lines)) {
            return false;
        }
        int *grown = realloc(result->match_lines, (size_t)next_capacity * sizeof(*grown));
        if (!grown) {
            return false;
        }
        result->match_lines = grown;
        result->match_capacity = next_capacity;
    }
    result->match_lines[result->match_stored_count++] = line;
    return true;
}

/* Add a grep hit to the search result set (merge into existing or create new). */
static bool add_to_search_results(search_result_t **sr, int *sr_count, int *sr_cap, cbm_node_t *n,
                                  int line) {
    for (int j = 0; j < *sr_count; j++) {
        if ((*sr)[j].node_id == n->id) {
            return append_search_match_line(&(*sr)[j], line);
        }
    }
    if (*sr_count >= *sr_cap) {
        int next_capacity = *sr_cap * PAIR_LEN;
        search_result_t *grown = realloc(*sr, (size_t)next_capacity * sizeof(**sr));
        if (!grown) {
            return false;
        }
        memset(grown + *sr_cap, 0, (size_t)(next_capacity - *sr_cap) * sizeof(*grown));
        *sr = grown;
        *sr_cap = next_capacity;
    }
    search_result_t *r = &(*sr)[*sr_count];
    if (!set_search_result_identity(r, n)) {
        return false;
    }
    if (!append_search_match_line(r, line)) {
        free(r->qualified_name);
        free(r->file);
        r->qualified_name = NULL;
        r->file = NULL;
        return false;
    }
    (*sr_count)++;
    return true;
}

/* Match a single grep hit to the tightest containing node, then add to sr or raw. */
static bool classify_grep_hit(grep_match_t *hit, cbm_node_t *file_nodes, int file_node_count,
                              search_result_t **sr, int *sr_count, int *sr_cap, grep_match_t **raw,
                              int raw_offset, int raw_limit, int *raw_count, int *raw_stored_count,
                              int *raw_cap, int *raw_content_truncated) {
    int best = find_tightest_node(file_nodes, file_node_count, hit->line);
    if (best >= 0) {
        return add_to_search_results(sr, sr_count, sr_cap, &file_nodes[best], hit->line);
    } else {
        if (*raw_count == INT_MAX) {
            return false;
        }
        int raw_index = (*raw_count)++;
        if (hit->content_truncated) {
            if (*raw_content_truncated == INT_MAX) {
                return false;
            }
            (*raw_content_truncated)++;
        }
        bool retain =
            raw_limit > 0 && raw_index >= raw_offset && raw_index - raw_offset < raw_limit;
        if (!retain) {
            return true;
        }
        if (*raw_stored_count >= *raw_cap) {
            int next_capacity = (*raw_cap == 0) ? 8 : *raw_cap * PAIR_LEN;
            if (next_capacity > raw_limit) {
                next_capacity = raw_limit;
            }
            grep_match_t *grown = realloc(*raw, (size_t)next_capacity * sizeof(**raw));
            if (!grown) {
                return false;
            }
            *raw = grown;
            *raw_cap = next_capacity;
        }
        (*raw)[*raw_stored_count] = *hit;
        hit->file = NULL;
        (*raw_stored_count)++;
    }
    return true;
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

/* Parse and classify the complete grep stream without retaining one object per
 * hit. Graph results retain one identity per distinct node plus at most 500
 * line numbers each; raw matches retain only the caller's requested page.
 * Exact totals are counted while the stream is consumed to EOF. */
static bool scan_and_classify_grep_matches(
    FILE *fp, const char *root_path, size_t root_len, bool has_path_filter, cbm_regex_t *path_regex,
    const char *pattern, bool use_regex, bool raw_content_offset_set, size_t raw_content_offset,
    cbm_store_t *store, const char *project, search_result_t **sr, int *sr_count, int *sr_cap,
    grep_match_t **raw, int raw_offset, int raw_limit, int *raw_count, int *raw_stored_count,
    int *raw_cap, int *raw_content_truncated, int *grep_count) {
    char *line = NULL;
    size_t line_capacity = 0;
    char *current_file = NULL;
    cbm_node_t *file_nodes = NULL;
    int file_node_count = 0;
    cbm_regex_t content_regex;
    bool content_regex_ready = use_regex && pattern &&
                               cbm_regcomp(&content_regex, pattern, CBM_REG_EXTENDED) == CBM_REG_OK;
    bool ok = true;

    for (;;) {
        ssize_t line_length = cbm_getline(&line, &line_capacity, fp);
        if (line_length < 0) {
            if (!feof(fp)) {
                ok = false;
            }
            break;
        }
        size_t len = (size_t)line_length;
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

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
        const char *file = strip_root_prefix(line, root_path, root_len);
        if (has_path_filter && cbm_regexec(path_regex, file, 0, NULL, 0) != CBM_REG_OK) {
            continue;
        }
        if (*grep_count == INT_MAX) {
            ok = false;
            break;
        }
        (*grep_count)++;

        if (!current_file || strcmp(current_file, file) != 0) {
            free_file_nodes(file_nodes, file_node_count);
            file_nodes = NULL;
            file_node_count = 0;
            free(current_file);
            current_file = heap_strdup(file);
            if (!current_file) {
                ok = false;
                break;
            }
            if (store) {
                (void)cbm_store_find_nodes_by_file(store, project, file, &file_nodes,
                                                   &file_node_count);
            }
        }

        grep_match_t hit = {0};
        hit.file = heap_strdup(file);
        if (!hit.file) {
            ok = false;
            break;
        }
        hit.line = (int)strtol(sep1 + SKIP_ONE, NULL, CBM_DECIMAL_BASE);
        const char *content = sep2 + SKIP_ONE;
        size_t match_start = 0;
        size_t match_end = 0;
        bool match_known = search_match_bounds(content, pattern, use_regex,
                                               content_regex_ready ? &content_regex : NULL,
                                               &match_start, &match_end);
        search_raw_preview(&hit, content, raw_content_offset_set, raw_content_offset, match_known,
                           match_start, match_end);
        if (!classify_grep_hit(&hit, file_nodes, file_node_count, sr, sr_count, sr_cap, raw,
                               raw_offset, raw_limit, raw_count, raw_stored_count, raw_cap,
                               raw_content_truncated)) {
            free(hit.file);
            ok = false;
            break;
        }
        free(hit.file);
    }

    if (content_regex_ready) {
        cbm_regfree(&content_regex);
    }
    free_file_nodes(file_nodes, file_node_count);
    free(current_file);
    free(line);
    return ok;
}

/* Write indexed file list for scoped grep. Returns true if scoped.
 * When a path_filter is provided, apply it here — before grep — so large
 * indexed projects do not scan files only for the stream classifier to discard
 * them later. The predicate is IDENTICAL to the post-grep filter: the same
 * compiled regex run against the same root-relative path (separators
 * normalized on Windows first), so prefiltering can only skip files whose
 * hits would be dropped anyway — results-preserving by construction.
 * *out_written receives the number of records written (0 = the filter
 * excluded every indexed file).
 *
 * `fl` is the caller's already-open binary stream on the descriptor cbm_mkstemp
 * created inside the private scratch directory; this function never opens or
 * closes it, so the list is never reachable through a predictable pathname. */
static bool write_scoped_filelist(cbm_mcp_server_t *srv, const char *project, const char *root_path,
                                  FILE *fl, const char *file_pattern, bool has_path_filter,
                                  cbm_regex_t *path_regex, int *out_written) {
    *out_written = 0;
    cbm_store_t *pre_store = resolve_store(srv, project);
    if (!pre_store) {
        return false;
    }
    char **indexed_files = NULL;
    int indexed_count = 0;
    int list_rc = cbm_store_list_files(pre_store, project, &indexed_files, &indexed_count);
    if (list_rc != CBM_STORE_OK) {
        for (int fi = 0; fi < indexed_count; fi++) {
            free(indexed_files[fi]);
        }
        free(indexed_files);
        return false;
    }
    if (indexed_count == 0) {
        /* cbm_store_list_files owns an allocation even for an empty successful
         * result; no element loop below can release that outer array. */
        free(indexed_files);
        return false;
    }
    bool ok = false;
    int written = 0;
    if (fl) {
        ok = true;
        for (int fi = 0; fi < indexed_count; fi++) {
            /* A source path never legitimately contains a newline or carriage
             * return. Those bytes are exactly the record separator on the
             * Windows filelist (and would split naive line readers elsewhere),
             * so a crafted indexed path with an embedded newline could inject
             * an extra entry into the scan set. Skip such paths entirely. */
            if (strpbrk(indexed_files[fi], "\r\n") != NULL) {
                continue;
            }
            if (has_path_filter && path_regex) {
#ifdef _WIN32
                cbm_normalize_path_sep(indexed_files[fi]);
#endif
                if (cbm_regexec(path_regex, indexed_files[fi], 0, NULL, 0) != CBM_REG_OK) {
                    continue;
                }
            }
#ifndef _WIN32
            /* GNU grep's --include is unavailable in BusyBox grep. Filter the
             * canonical list before xargs instead, preserving grep's basename
             * glob semantics without making the shipped static binary depend
             * on GNU userland. Windows keeps its PowerShell -like filter. */
            const char *basename = strrchr(indexed_files[fi], '/');
            basename = basename ? basename + SKIP_ONE : indexed_files[fi];
            if (file_pattern && fnmatch(file_pattern, basename, 0) != 0) {
                continue;
            }
#endif
#ifdef _WIN32
            if (cbm_search_code_file_pattern_can_prefilter(file_pattern) &&
                !cbm_search_code_windows_path_matches_prefilter(indexed_files[fi], file_pattern)) {
                continue;
            }
#endif
            size_t root_len = strlen(root_path);
            size_t file_len = strlen(indexed_files[fi]);
            if (root_len > SIZE_MAX - file_len - 2) {
                continue;
            }
            size_t scan_path_len = root_len + 1 + file_len;
            char *scan_path = malloc(scan_path_len + 1);
            if (!scan_path) {
                ok = false;
                break;
            }
            memcpy(scan_path, root_path, root_len);
            scan_path[root_len] = '/';
            memcpy(scan_path + root_len + 1, indexed_files[fi], file_len + 1);

            /* Incremental stores can retain structural directory nodes and
             * briefly stale deleted-file paths. Neither is a content-scan
             * operand. Filter them before spawning so an expected stale entry
             * cannot turn otherwise valid matches into grep status 2. This
             * deliberately does not follow symlinks/reparse points. */
            cbm_path_info_t path_info;
            if (cbm_path_info_utf8(scan_path, &path_info) != 0 || !path_info.is_regular) {
                free(scan_path);
                continue;
            }
            /* Write "<root>/<file>" piece-by-piece (no fixed-size buffer, so an
             * arbitrarily long absolute path cannot overflow). Forward slash join
             * so xargs doesn't treat Windows backslashes as escapes; binary mode
             * (wb) prevents CRLF translation. Record separator differs by platform:
             *   - Unix: NUL, consumed by `xargs -0` — handles spaces in paths (a
             *     newline separator would split plain xargs on the space).
             *   - Windows: newline, consumed by PowerShell `Get-Content |
             *     Select-String -LiteralPath` (NUL bytes break Get-Content). */
            (void)fwrite(scan_path, 1, scan_path_len, fl);
            free(scan_path);
#ifdef _WIN32
            (void)fputc('\n', fl);
#else
            (void)fputc('\0', fl);
#endif
            written++;
        }
        /* The stream stays open — the caller owns it and closes it (flushing
         * these records to disk) before the grep subprocess reads the list. */
    }
    for (int fi = 0; fi < indexed_count; fi++) {
        free(indexed_files[fi]);
    }
    free(indexed_files);
    *out_written = written;
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

/* These characters retain command-language meaning inside quoted cmd.exe
 * arguments: percent expands environment variables, exclamation can expand
 * delayed variables, and caret changes parsing. Never interpolate them from a
 * stored project root or request branch into the Windows detect_changes payload.
 * /V:OFF is defense in depth for exclamation; validation remains the boundary. */
static bool validate_windows_cmd_interpolation_arg(const char *s) {
#ifdef _WIN32
    return s && strpbrk(s, "%!^") == NULL;
#else
    return s != NULL;
#endif
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

/* Private scratch for one search_code scan: the grep -f pattern file and the
 * scoped file list.
 *
 * Both used to be fixed, guessable paths derived from the pid —
 * "<tmp>/cbm_search_<pid>.pat" and its ".files" companion — opened with a plain
 * fopen. Another local user could pre-plant a symlink at either name and
 * redirect the write; two searches in the same process could also collide on
 * them. Now both live inside a directory created by cbm_mkdtemp (0700 on POSIX,
 * an explicit owner-only DACL on Windows) under an unguessable XXXXXX suffix,
 * and each file is created by cbm_mkstemp — O_CREAT|O_EXCL at mode 0600, so the
 * create fails rather than following anything already at the name. Every write
 * goes through the descriptor cbm_mkstemp returned; neither path is ever
 * reopened by name.
 *
 * Sizing: cbm_mkdtemp copies its expanded result back into `dir`, and its own
 * internal buffer is CBM_SZ_512, so `dir` must be at least that big to receive
 * it. The two file paths are `dir` plus a short basename. */
typedef struct {
    char dir[CBM_SZ_512];
    char pattern_path[CBM_SZ_1K];
    char filelist_path[CBM_SZ_1K];
    FILE *filelist; /* held open for write_scoped_filelist; closed by the caller */
} search_scratch_t;

typedef enum {
    MCP_SCAN_SUCCESS = 0,
    MCP_SCAN_COMMAND_FAILURE,
    MCP_SCAN_CONTAINED_COMMAND_FAILURE,
    MCP_SCAN_OUTPUT_LIMIT,
    MCP_SCAN_DEADLINE,
    MCP_SCAN_CANCELLED,
    MCP_SCAN_SUPERVISION_FAILURE,
} mcp_scan_cause_t;

/* Create <scratch>/<basename>-XXXXXX exclusively and return a stream on the
 * descriptor. On failure `path_out` is emptied so cleanup skips it. */
static FILE *search_scratch_file(const char *dir, const char *basename, char *path_out,
                                 size_t path_sz) {
    path_out[0] = '\0';
    int written = snprintf(path_out, path_sz, "%s/%s-XXXXXX", dir, basename);
    if (written <= 0 || (size_t)written >= path_sz) {
        path_out[0] = '\0';
        return NULL;
    }
    int descriptor = cbm_mkstemp(path_out);
    if (descriptor < 0) {
        path_out[0] = '\0';
        return NULL;
    }
    /* Binary mode: the file list uses an explicit per-platform record separator
     * (NUL for xargs -0, newline for PowerShell) that CRLF translation would
     * corrupt — the same reason the previous code opened it "wb". */
    FILE *stream = mcp_fdopen(descriptor, "wb");
    if (!stream) {
        (void)mcp_close(descriptor);
        (void)cbm_unlink(path_out);
        path_out[0] = '\0';
    }
    return stream;
}

/* Anchored cleanup: removes both scratch files and the private directory. Safe
 * to call more than once and on any partially-initialised scratch, so every
 * exit from handle_search_code can call it unconditionally. rmdir succeeding is
 * itself the proof nothing was left inside. */
static void search_scratch_close(search_scratch_t *scratch) {
    if (scratch->filelist) {
        (void)fclose(scratch->filelist);
        scratch->filelist = NULL;
    }
    if (scratch->pattern_path[0] != '\0') {
        (void)cbm_unlink(scratch->pattern_path);
        scratch->pattern_path[0] = '\0';
    }
    if (scratch->filelist_path[0] != '\0') {
        (void)cbm_unlink(scratch->filelist_path);
        scratch->filelist_path[0] = '\0';
    }
    if (scratch->dir[0] != '\0') {
        (void)cbm_rmdir(scratch->dir);
        scratch->dir[0] = '\0';
    }
}

/* Open the scratch directory, write `pattern` to the grep -f file, and leave the
 * file list open for write_scoped_filelist. Returns true on success; on failure
 * everything already created is removed before returning. */
static bool search_scratch_open(search_scratch_t *scratch, const char *pattern) {
    scratch->dir[0] = '\0';
    scratch->pattern_path[0] = '\0';
    scratch->filelist_path[0] = '\0';
    scratch->filelist = NULL;

    int written =
        snprintf(scratch->dir, sizeof(scratch->dir), "%s/cbm-search-XXXXXX", cbm_tmpdir());
    if (written <= 0 || (size_t)written >= sizeof(scratch->dir) || !cbm_mkdtemp(scratch->dir)) {
        scratch->dir[0] = '\0';
        return false;
    }

    FILE *pattern_file = search_scratch_file(scratch->dir, "pat", scratch->pattern_path,
                                             sizeof(scratch->pattern_path));
    if (!pattern_file) {
        search_scratch_close(scratch);
        return false;
    }
    bool ok = fprintf(pattern_file, "%s\n", pattern) >= 0;
    ok = fclose(pattern_file) == 0 && ok;
    if (!ok) {
        search_scratch_close(scratch);
        return false;
    }

    scratch->filelist = search_scratch_file(scratch->dir, "files", scratch->filelist_path,
                                            sizeof(scratch->filelist_path));
    if (!scratch->filelist) {
        search_scratch_close(scratch);
        return false;
    }
    return true;
}

/* Compile a path filter regex. Returns true if compiled successfully. */
static bool compile_path_filter(const char *filter, cbm_regex_t *re) {
    if (!filter || !filter[0]) {
        return false;
    }
    return cbm_regcomp(re, filter, CBM_REG_EXTENDED | CBM_REG_NOSUB) == CBM_REG_OK;
}

static mcp_scan_cause_t mcp_run_shell_command_cancellable_bounded(
    cbm_mcp_server_t *srv, const char *command, char output_path[CBM_SZ_2K], size_t output_limit,
    uint64_t deadline_ms, bool deadline_enabled, bool deadline_latched, bool exit_one_is_no_match,
    cbm_proc_result_t *result_out);

static char *search_code_timeout_result(void) {
    static const char message[] = "search_code scan exceeded its execution deadline";
    static const char fallback[] =
        "{\"content\":[{\"type\":\"text\",\"text\":\"search_code scan exceeded its execution "
        "deadline\"}],\"structuredContent\":{\"code\":\"request_timeout\",\"message\":\"search_"
        "code "
        "scan exceeded its execution deadline\"},\"isError\":true}";
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return heap_strdup(fallback);
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *content = yyjson_mut_arr(doc);
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "type", "text");
    yyjson_mut_obj_add_str(doc, item, "text", message);
    yyjson_mut_arr_add_val(content, item);
    yyjson_mut_obj_add_val(doc, root, "content", content);
    yyjson_mut_val *structured = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, structured, "code", "request_timeout");
    yyjson_mut_obj_add_str(doc, structured, "message", message);
    yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
    yyjson_mut_obj_add_bool(doc, root, "isError", true);
    char *result = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return result ? result : heap_strdup(fallback);
}

static char *search_code_scan_error(search_scratch_t *scratch, const char *output_path,
                                    bool has_path_filter, cbm_regex_t *path_regex, char *root_path,
                                    char *pattern, char *project, char *file_pattern,
                                    mcp_scan_cause_t cause, const char *message) {
    if (output_path && output_path[0]) {
        (void)cbm_unlink(output_path);
    }
    search_scratch_close(scratch);
    if (has_path_filter) {
        cbm_regfree(path_regex);
    }
    free(root_path);
    free(pattern);
    free(project);
    free(file_pattern);
    if (cause == MCP_SCAN_DEADLINE) {
        return search_code_timeout_result();
    }
    return cbm_mcp_text_result(message, true);
}

static char *handle_search_code(cbm_mcp_server_t *srv, const char *args) {
    char *pattern = cbm_mcp_get_string_arg(args, "pattern");
    char *project = get_project_arg(args);
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    char *path_filter = cbm_mcp_get_string_arg(args, "path_filter");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    int legacy_limit = cbm_mcp_get_int_arg(args, "limit", MCP_DEFAULT_LIMIT);
    /* #1511: a negative limit flowed straight into the result cap and came back
     * as the reported count ("results: -5"), which reads to an agent as a real
     * answer rather than a rejected argument. The schema now declares
     * minimum:1, but a schema is a request to the client, never a guarantee to
     * the server — clamp here too. */
    if (legacy_limit < 1) {
        legacy_limit = MCP_DEFAULT_LIMIT;
    }
    int result_limit = cbm_mcp_get_int_arg(args, "result_limit", legacy_limit);
    int result_offset = cbm_mcp_get_int_arg(args, "result_offset", 0);
    int context_lines = cbm_mcp_get_int_arg(args, "context", 0);
    bool use_regex = cbm_mcp_get_bool_arg(args, "regex");
    uint64_t search_t0 = cbm_now_ms();
    search_metrics_t metrics = {0};
    metrics.include_phase_timings = cbm_mcp_get_bool_arg(args, "debug");
    /* In literal (non-regex) mode a '|' is matched as a byte, not alternation —
     * a common silent 0-match trap; flagged in the result warnings (#282). */
    bool pat_has_pipe = pattern && strchr(pattern, '|') != NULL;

    int mode = parse_search_mode(mode_str);
    free(mode_str);

    if (result_limit < 1) {
        result_limit = MCP_DEFAULT_LIMIT;
    } else if (result_limit > 500) {
        result_limit = 500;
    }
    if (result_offset < 0) {
        result_offset = 0;
    }
    int raw_limit = cbm_mcp_get_int_arg(args, "raw_limit", 5);
    if (raw_limit < 0) {
        raw_limit = 0;
    } else if (raw_limit > 100) {
        raw_limit = 100;
    }
    int raw_offset = cbm_mcp_get_int_arg(args, "raw_offset", 0);
    if (raw_offset < 0) {
        raw_offset = 0;
    }
    int raw_content_offset_arg = cbm_mcp_get_int_arg(args, "raw_content_offset", 0);
    if (raw_content_offset_arg < 0) {
        raw_content_offset_arg = 0;
    }
    bool raw_content_offset_set = false;
    yyjson_doc *search_args_doc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *search_args_root = search_args_doc ? yyjson_doc_get_root(search_args_doc) : NULL;
    yyjson_val *raw_content_offset_val =
        search_args_root && yyjson_is_obj(search_args_root)
            ? yyjson_obj_get(search_args_root, "raw_content_offset")
            : NULL;
    raw_content_offset_set = raw_content_offset_val && yyjson_is_int(raw_content_offset_val);
    yyjson_doc_free(search_args_doc);
    int directory_limit = cbm_mcp_get_int_arg(args, "directory_limit", 20);
    if (directory_limit < 0) {
        directory_limit = 0;
    } else if (directory_limit > 64) {
        directory_limit = 64;
    }
    int directory_offset = cbm_mcp_get_int_arg(args, "directory_offset", 0);
    if (directory_offset < 0) {
        directory_offset = 0;
    }
    int match_limit = cbm_mcp_get_int_arg(args, "match_limit", 8);
    if (match_limit < 1) {
        match_limit = 1;
    } else if (match_limit > 500) {
        match_limit = 500;
    }
    int source_max_lines = cbm_mcp_get_int_arg(args, "source_max_lines", 20);
    if (source_max_lines < 1) {
        source_max_lines = 1;
    } else if (source_max_lines > 200) {
        source_max_lines = 200;
    }
    /* Full source is an explicit detail request and gets a larger default;
     * compact/files stay lean. Every mode still has an exact serialized-byte
     * ceiling, and callers can raise it deliberately. */
    int max_output_tokens =
        cbm_mcp_get_int_arg(args, "max_output_tokens", mode == 1 ? 12000 : 3200);
    if (max_output_tokens < 128) {
        max_output_tokens = 128;
    } else if (max_output_tokens > 1000000) {
        max_output_tokens = 1000000;
    }
    size_t byte_budget = (size_t)max_output_tokens * (size_t)MCP_OUTPUT_BYTES_PER_TOKEN_ESTIMATE;

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
    uint64_t scan_budget_ms = srv->search_scan_timeout_override_set
                                  ? srv->search_scan_timeout_override_ms
                                  : MCP_SEARCH_SCAN_TIMEOUT_MS;
    uint64_t scan_started_ms = cbm_now_ms();
    uint64_t scan_deadline_ms = UINT64_MAX - scan_started_ms < scan_budget_ms
                                    ? UINT64_MAX
                                    : scan_started_ms + scan_budget_ms;
    bool scan_deadline_latched = false;
    search_scratch_t scratch;
    if (!search_scratch_open(&scratch, pattern)) {
        bool scan_cancelled = mcp_request_cancelled(srv);
        bool scan_timed_out = cbm_now_ms() >= scan_deadline_ms;
        char errmsg[CBM_SZ_256];
        snprintf(errmsg, sizeof(errmsg), "search failed: cannot create temp file (%s)",
                 strerror(errno));
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        if (scan_cancelled) {
            return cbm_mcp_text_result("search_code cancelled for this request", true);
        }
        if (scan_timed_out) {
            return search_code_timeout_result();
        }
        return cbm_mcp_text_result(errmsg, true);
    }
    scan_deadline_latched = cbm_now_ms() >= scan_deadline_ms;
    bool scan_cancellation_latched = mcp_request_cancelled(srv);
    const char *tmpfile = scratch.pattern_path;
    const char *filelist = scratch.filelist_path;

    /* Scope grep to indexed files only — avoids scanning vendored/generated code.
     * Query the graph for distinct file paths, write them to a temp file,
     * then use xargs to pass them to grep. Falls back to recursive grep if
     * no indexed files found (project not fully indexed). */
    bool scoped = false;
    int scoped_written = 0;

    uint64_t scope_t0 = metrics.include_phase_timings ? cbm_now_ms() : 0;
    if (!scan_cancellation_latched && !scan_deadline_latched) {
        scoped = write_scoped_filelist(srv, project, root_path, scratch.filelist, file_pattern,
                                       has_path_filter, has_path_filter ? &path_regex : NULL,
                                       &scoped_written);
    }
    /* Close before grep runs: this is what flushes the records the helper wrote
     * through the descriptor. Clearing the field hands ownership to
     * search_scratch_close, which still unlinks the file itself. */
    (void)fclose(scratch.filelist);
    scratch.filelist = NULL;
    scan_cancellation_latched = scan_cancellation_latched || mcp_request_cancelled(srv);
    scan_deadline_latched = scan_deadline_latched || cbm_now_ms() >= scan_deadline_ms;
    if (metrics.include_phase_timings) {
        metrics.scope_ms = cbm_now_ms() - scope_t0;
    }

    /* Consume and classify the complete stream. Only graph identities, bounded
     * per-result line evidence, and the requested raw page are retained. */
    cbm_store_t *store = resolve_store(srv, project);
    int sr_cap = CBM_SZ_32;
    int sr_count = 0;
    search_result_t *sr = calloc((size_t)sr_cap, sizeof(*sr));
    int raw_cap = 0;
    int raw_count = 0;
    int raw_stored_count = 0;
    int raw_content_truncated = 0;
    grep_match_t *raw = NULL;
    int gm_count = 0;
    bool scan_saturated = false;
    bool scan_ok = sr != NULL;
    uint64_t scan_t0 = metrics.include_phase_timings ? cbm_now_ms() : 0;
    if (scoped && scoped_written == 0 && !scan_cancellation_latched && !scan_deadline_latched) {
        /* The path_filter (or POSIX file_pattern) excluded every indexed file — nothing to scan.
         * Skip the grep subprocess: xargs on an empty filelist is
         * platform-dependent (GNU execs grep once with no operands, BSD
         * skips), and the post-grep filter would drop every hit anyway. */
        search_scratch_close(&scratch);
    } else if (scan_ok) {
        char cmd[CBM_SZ_4K];
        cbm_search_code_build_grep_cmd(cmd, sizeof(cmd), use_regex, scoped, file_pattern, tmpfile,
                                       filelist, root_path);

        /* The scan runs under the supervised, deadline- and output-bounded
         * runner. Every cause other than success fails closed here, so a page
         * is only ever classified from a COMPLETE stream. */
        char output_path[CBM_SZ_2K] = {0};
        cbm_proc_result_t scan_result = {0};
        size_t scan_output_limit = srv->search_output_limit_override
                                       ? srv->search_output_limit_override
                                       : MCP_SEARCH_OUTPUT_MAX;
        const char *scan_command =
            srv->search_scan_command_override ? srv->search_scan_command_override : cmd;
        mcp_scan_cause_t scan_cause = mcp_run_shell_command_cancellable_bounded(
            srv, scan_command, output_path, scan_output_limit, scan_deadline_ms, true,
            scan_deadline_latched, /*exit_one_is_no_match=*/false, &scan_result);
        /* Both POSIX commands wrap grep and map its no-match status to 0, so any
         * non-zero exit (a failed find/sort, an unreadable operand, a broken
         * grep) is an incomplete scan and fails closed. */
        FILE *fp = NULL;
        const char *scan_message = NULL; /* MCP_SCAN_DEADLINE renders its own text */
        char limit_message[CBM_SZ_128];
        if (scan_cause == MCP_SCAN_SUPERVISION_FAILURE) {
            scan_message = "search failed: process supervision could not quiesce";
        } else if (scan_cause == MCP_SCAN_CANCELLED) {
            scan_message = "search_code cancelled for this request";
        } else if (scan_cause == MCP_SCAN_OUTPUT_LIMIT) {
            snprintf(limit_message, sizeof(limit_message),
                     "search failed: output exceeded the %zu-byte safety limit", scan_output_limit);
            scan_message = limit_message;
        } else if (scan_cause == MCP_SCAN_COMMAND_FAILURE ||
                   scan_cause == MCP_SCAN_CONTAINED_COMMAND_FAILURE) {
            scan_message = "search failed before the complete result set was scanned: the "
                           "contained command could not complete";
        } else if (scan_cause == MCP_SCAN_SUCCESS) {
            fp = cbm_fopen(output_path, "rb");
            if (!fp) {
                scan_cause = MCP_SCAN_COMMAND_FAILURE;
                scan_message = "search failed: contained output could not be read";
            }
        }
        if (scan_cause != MCP_SCAN_SUCCESS) {
            free_search_results(sr, sr_count);
            return search_code_scan_error(&scratch, output_path, has_path_filter, &path_regex,
                                          root_path, pattern, project, file_pattern, scan_cause,
                                          scan_message);
        }
        scan_ok = scan_and_classify_grep_matches(
            fp, root_path, strlen(root_path), has_path_filter, &path_regex, pattern, use_regex,
            raw_content_offset_set, (size_t)raw_content_offset_arg, store, project, &sr, &sr_count,
            &sr_cap, &raw, raw_offset, raw_limit, &raw_count, &raw_stored_count, &raw_cap,
            &raw_content_truncated, &gm_count);
        (void)fclose(fp);
        (void)cbm_unlink(output_path);
        /* Both scratch files and the private directory go here — unlike the old
         * code, the file list is removed even when the scan was not scoped. */
        search_scratch_close(&scratch);
    }
    if (metrics.include_phase_timings) {
        metrics.scan_ms = cbm_now_ms() - scan_t0;
    }

    if (!scan_ok) {
        search_scratch_close(&scratch);
        free_search_results(sr, sr_count);
        free_grep_matches(raw, raw_stored_count);
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        return cbm_mcp_text_result("search failed before the complete result set was scanned",
                                   true);
    }

    /* ── Phase 2+3: degree expansion + global graph ranking ─── */
    uint64_t enrich_t0 = metrics.include_phase_timings ? cbm_now_ms() : 0;
    /* Phase 3: batch degree query — ONE query for all results instead of 2×N */
    if (store && sr_count > 0) {
        int64_t *ids = malloc(sr_count * sizeof(int64_t));
        int *in_degs = malloc(sr_count * sizeof(int));
        int *out_degs = malloc(sr_count * sizeof(int));
        if (!ids || !in_degs || !out_degs) {
            free(ids);
            free(in_degs);
            free(out_degs);
            free_search_results(sr, sr_count);
            free_grep_matches(raw, raw_stored_count);
            free(root_path);
            free(pattern);
            free(project);
            free(file_pattern);
            if (has_path_filter) {
                cbm_regfree(&path_regex);
            }
            return cbm_mcp_text_result("out of memory", true);
        }
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
    if (metrics.include_phase_timings) {
        metrics.enrich_ms = cbm_now_ms() - enrich_t0;
    }
    metrics.elapsed_ms = cbm_now_ms() - search_t0;

    /* ── Phase 4: Context assembly (extracted helper) ─────────── */

    /* Compact mode defaults to the lean tree. format:"json" preserves the
     * released machine shape ({cols, rows}; full adds a per-row source cell;
     * files is a plain list). */
    char *sc_format = cbm_mcp_get_string_arg(args, "format");
    bool sc_legacy_json = sc_format && strcmp(sc_format, "json") == 0;
    free(sc_format);

    int result_start = result_offset < sr_count ? result_offset : sr_count;
    int output_count = sr_count - result_start;
    if (output_count > result_limit) {
        output_count = result_limit;
    }
    int raw_start = raw_offset < raw_count ? raw_offset : raw_count;
    int raw_output = raw_stored_count;
    search_dir_count_t *directories = NULL;
    int dir_total = 0;
    if (!aggregate_search_dirs(sr, sr_count, &directories, &dir_total)) {
        free_search_results(sr, sr_count);
        free_grep_matches(raw, raw_stored_count);
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        return cbm_mcp_text_result("out of memory", true);
    }
    free_search_dirs(directories, dir_total);
    int directory_start = directory_offset < dir_total ? directory_offset : dir_total;
    int directory_output = dir_total - directory_start;
    if (directory_output > directory_limit) {
        directory_output = directory_limit;
    }
    bool warn_literal_pipe = pat_has_pipe && !use_regex;
    char *payload = render_search_payload(
        sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start, result_limit,
        output_count, raw_start, raw_output, directory_start, directory_output, raw_limit,
        directory_limit, match_limit, mode, context_lines, source_max_lines, root_path,
        scan_saturated, warn_literal_pipe, &metrics, false, sc_legacy_json);

    if (payload && strlen(payload) > byte_budget) {
        free(payload);
        payload = render_search_payload(
            sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start,
            result_limit, output_count, raw_start, raw_output, directory_start, directory_output,
            raw_limit, directory_limit, match_limit, mode, context_lines, source_max_lines,
            root_path, scan_saturated, warn_literal_pipe, &metrics, true, sc_legacy_json);
    }

    /* Preserve ranked graph answers. Unclassified raw grep rows and directory
     * diagnostics yield first, at whole-row boundaries. */
    while (payload && strlen(payload) > byte_budget && raw_output > 0) {
        raw_output--;
        free(payload);
        payload = render_search_payload(
            sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start,
            result_limit, output_count, raw_start, raw_output, directory_start, directory_output,
            raw_limit, directory_limit, match_limit, mode, context_lines, source_max_lines,
            root_path, scan_saturated, warn_literal_pipe, &metrics, true, sc_legacy_json);
    }
    while (payload && strlen(payload) > byte_budget && directory_output > 0) {
        directory_output--;
        free(payload);
        payload = render_search_payload(
            sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start,
            result_limit, output_count, raw_start, raw_output, directory_start, directory_output,
            raw_limit, directory_limit, match_limit, mode, context_lines, source_max_lines,
            root_path, scan_saturated, warn_literal_pipe, &metrics, true, sc_legacy_json);
    }

    /* Full source is the next detail tier. Find the largest whole-line window
     * that fits while retaining every ranked result identity. */
    if (payload && strlen(payload) > byte_budget && mode == 1 && source_max_lines > 0) {
        int low = 0;
        int high = source_max_lines - 1;
        int best_lines = -1;
        char *best_payload = NULL;
        while (low <= high) {
            int middle = low + (high - low) / 2;
            char *candidate = render_search_payload(
                sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start,
                result_limit, output_count, raw_start, raw_output, directory_start,
                directory_output, raw_limit, directory_limit, match_limit, mode, context_lines,
                middle, root_path, scan_saturated, warn_literal_pipe, &metrics, true,
                sc_legacy_json);
            if (candidate && strlen(candidate) <= byte_budget) {
                free(best_payload);
                best_payload = candidate;
                best_lines = middle;
                low = middle + 1;
            } else {
                free(candidate);
                high = middle - 1;
            }
        }
        free(payload);
        if (best_payload) {
            payload = best_payload;
            source_max_lines = best_lines;
        } else {
            source_max_lines = 0;
            payload = render_search_payload(
                sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start,
                result_limit, output_count, raw_start, raw_output, directory_start,
                directory_output, raw_limit, directory_limit, match_limit, mode, context_lines,
                source_max_lines, root_path, scan_saturated, warn_literal_pipe, &metrics, true,
                sc_legacy_json);
        }
    }

    /* If identities themselves exceed the ceiling, find the largest fitting
     * row prefix. Check every smaller prefix in descending order: response-local
     * prefix factoring can activate at a threshold, so encoded size is not
     * guaranteed to be monotonic in the row count. */
    if (payload && strlen(payload) > byte_budget && output_count > 0) {
        char *best_payload = NULL;
        for (int candidate_rows = output_count - 1; candidate_rows >= 0; candidate_rows--) {
            char *candidate = render_search_payload(
                sr, sr_count, raw, raw_count, raw_content_truncated, gm_count, result_start,
                result_limit, candidate_rows, raw_start, raw_output, directory_start,
                directory_output, raw_limit, directory_limit, match_limit, mode, context_lines,
                source_max_lines, root_path, scan_saturated, warn_literal_pipe, &metrics, true,
                sc_legacy_json);
            if (candidate && strlen(candidate) <= byte_budget) {
                best_payload = candidate;
                break;
            }
            free(candidate);
        }
        free(payload);
        payload = best_payload;
    }

    if (!payload || strlen(payload) > byte_budget) {
        free(payload);
        payload = search_budget_floor(sr_count, raw_count, gm_count, result_start, result_limit,
                                      raw_start, raw_limit, directory_start, directory_limit,
                                      dir_total, scan_saturated, sc_legacy_json, max_output_tokens);
    }
    char *result = cbm_mcp_text_result(payload ? payload : "out of memory", payload == NULL);
    free(payload);
    free_search_results(sr, sr_count);
    free_grep_matches(raw, raw_stored_count);
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

/* Run shell-backed query helpers inside the same process-tree containment used
 * by indexing. A plain popen owns only its shell stream: on disconnect there is
 * no safe handle with which to stop a blocked git child or its descendants. */
static bool mcp_command_output_path(char out[CBM_SZ_2K]) {
    char directory[CBM_SZ_1K];
    const char *cache = cbm_resolve_cache_dir();
    int written;
    if (cache && cache[0]) {
        written = snprintf(directory, sizeof(directory), "%s/logs", cache);
        if (written <= 0 || written >= (int)sizeof(directory) || !cbm_mkdir_p(directory, 0700)) {
            return false;
        }
    } else {
        written = snprintf(directory, sizeof(directory), "%s", cbm_tmpdir());
        if (written <= 0 || written >= (int)sizeof(directory)) {
            return false;
        }
    }
    written = snprintf(out, CBM_SZ_2K, "%s/.mcp-command-XXXXXX", directory);
    if (written <= 0 || written >= CBM_SZ_2K) {
        out[0] = '\0';
        return false;
    }
    int descriptor = cbm_mkstemp(out);
    if (descriptor < 0) {
        out[0] = '\0';
        return false;
    }
#ifdef _WIN32
    (void)_close(descriptor);
#else
    (void)close(descriptor);
#endif
    return true;
}

#ifdef _WIN32
/* Resolve the OS-owned command processor without consulting PATH or mutable
 * COMSPEC. cbm_subprocess receives this as lpApplicationName and validates the
 * same absolute cmd.exe path before using the dedicated payload serializer. */
static bool mcp_resolve_windows_cmd(char out[CBM_SZ_4K]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    wchar_t system_directory[MAX_PATH + 1];
    UINT directory_length = GetSystemDirectoryW(system_directory, MAX_PATH + 1);
    static const wchar_t suffix[] = L"\\cmd.exe";
    if (directory_length == 0 || directory_length > MAX_PATH ||
        (size_t)directory_length + (sizeof(suffix) / sizeof(suffix[0])) >
            sizeof(system_directory) / sizeof(system_directory[0])) {
        return false;
    }
    memcpy(system_directory + directory_length, suffix, sizeof(suffix));
    char *candidate = cbm_wide_to_utf8(system_directory);
    if (!candidate) {
        return false;
    }
    bool resolved = cbm_canonical_path(candidate, out, CBM_SZ_4K) != 0;
    free(candidate);
    return resolved;
}
#endif

static mcp_scan_cause_t mcp_scan_pre_spawn_cause(cbm_mcp_server_t *srv, const char *output_path,
                                                 size_t output_limit, uint64_t deadline_ms,
                                                 bool deadline_enabled, bool *cancellation_latched,
                                                 bool *deadline_latched,
                                                 bool *output_limit_latched) {
    *cancellation_latched = *cancellation_latched || mcp_request_cancelled(srv);
    *deadline_latched = *deadline_latched || (deadline_enabled && cbm_now_ms() >= deadline_ms);
    if (!*output_limit_latched && output_limit > 0 && output_path && output_path[0]) {
        int64_t output_size = cbm_file_size(output_path);
        *output_limit_latched = output_size > 0 && (uint64_t)output_size > output_limit;
    }
    if (*cancellation_latched) {
        return MCP_SCAN_CANCELLED;
    }
    if (*deadline_latched) {
        return MCP_SCAN_DEADLINE;
    }
    if (*output_limit_latched) {
        return MCP_SCAN_OUTPUT_LIMIT;
    }
    return MCP_SCAN_SUCCESS;
}

static mcp_scan_cause_t mcp_run_shell_command_cancellable_bounded(
    cbm_mcp_server_t *srv, const char *command, char output_path[CBM_SZ_2K], size_t output_limit,
    uint64_t deadline_ms, bool deadline_enabled, bool deadline_latched, bool exit_one_is_no_match,
    cbm_proc_result_t *result_out) {
    if (!srv || !command || !output_path || !result_out) {
        return MCP_SCAN_COMMAND_FAILURE;
    }
    memset(result_out, 0, sizeof(*result_out));
    bool cancellation_latched = false;
    bool output_limit_latched = false;
    mcp_scan_cause_t pre_spawn_cause =
        mcp_scan_pre_spawn_cause(srv, NULL, output_limit, deadline_ms, deadline_enabled,
                                 &cancellation_latched, &deadline_latched, &output_limit_latched);
    if (pre_spawn_cause != MCP_SCAN_SUCCESS) {
        result_out->tree_quiesced = true; /* no child was spawned */
        return pre_spawn_cause;
    }
    if (!mcp_command_output_path(output_path)) {
        pre_spawn_cause = mcp_scan_pre_spawn_cause(srv, NULL, output_limit, deadline_ms,
                                                   deadline_enabled, &cancellation_latched,
                                                   &deadline_latched, &output_limit_latched);
        result_out->tree_quiesced = true;
        return pre_spawn_cause != MCP_SCAN_SUCCESS ? pre_spawn_cause : MCP_SCAN_COMMAND_FAILURE;
    }
    /* Internal test seam: rejecting after output allocation exercises the same
     * cleanup contract as a contained process-tree failure. */
    bool command_rejected =
        srv->command_test_hook && !srv->command_test_hook(srv->command_test_context, command);
    pre_spawn_cause =
        mcp_scan_pre_spawn_cause(srv, output_path, output_limit, deadline_ms, deadline_enabled,
                                 &cancellation_latched, &deadline_latched, &output_limit_latched);
    if (pre_spawn_cause != MCP_SCAN_SUCCESS || command_rejected) {
        result_out->tree_quiesced = true; /* no child was spawned */
        return pre_spawn_cause != MCP_SCAN_SUCCESS ? pre_spawn_cause : MCP_SCAN_COMMAND_FAILURE;
    }
#ifdef _WIN32
    char shell[CBM_SZ_4K];
    if (!mcp_resolve_windows_cmd(shell)) {
        pre_spawn_cause = mcp_scan_pre_spawn_cause(srv, output_path, output_limit, deadline_ms,
                                                   deadline_enabled, &cancellation_latched,
                                                   &deadline_latched, &output_limit_latched);
        (void)cbm_unlink(output_path);
        output_path[0] = '\0';
        result_out->tree_quiesced = true;
        return pre_spawn_cause != MCP_SCAN_SUCCESS ? pre_spawn_cause : MCP_SCAN_COMMAND_FAILURE;
    }
#else
    const char *shell = "/bin/sh";
    const char *argv[] = {"sh", "-c", command, NULL};
#endif
    cbm_proc_opts_t options = {
        .bin = shell,
#ifdef _WIN32
        .windows_cmd_payload = command,
#else
        .argv = argv,
#endif
        .log_file = output_path,
        .quiet_timeout_ms = 0,
        .cancel_grace_ms = CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS,
        .delete_log_on_exit = false,
    };
    pre_spawn_cause =
        mcp_scan_pre_spawn_cause(srv, output_path, output_limit, deadline_ms, deadline_enabled,
                                 &cancellation_latched, &deadline_latched, &output_limit_latched);
    if (pre_spawn_cause != MCP_SCAN_SUCCESS) {
        result_out->tree_quiesced = true;
        return pre_spawn_cause;
    }
    cbm_subprocess_t *process = NULL;
    if (cbm_subprocess_spawn(&options, &process) != 0) {
        pre_spawn_cause = mcp_scan_pre_spawn_cause(srv, output_path, output_limit, deadline_ms,
                                                   deadline_enabled, &cancellation_latched,
                                                   &deadline_latched, &output_limit_latched);
        (void)cbm_unlink(output_path);
        output_path[0] = '\0';
        result_out->tree_quiesced = true;
        return pre_spawn_cause != MCP_SCAN_SUCCESS ? pre_spawn_cause : MCP_SCAN_COMMAND_FAILURE;
    }

    cbm_proc_poll_t state;
    for (;;) {
        if (mcp_request_cancelled(srv)) {
            cancellation_latched = true;
            (void)cbm_subprocess_request_cancel(process);
        }
        if (deadline_enabled && cbm_now_ms() >= deadline_ms) {
            deadline_latched = true;
            (void)cbm_subprocess_request_cancel(process);
        }
        if (!output_limit_latched && output_limit > 0) {
            int64_t output_size = cbm_file_size(output_path);
            if (output_size > 0 && (uint64_t)output_size > output_limit) {
                output_limit_latched = true;
                (void)cbm_subprocess_request_cancel(process);
            }
        }
        state = cbm_subprocess_poll(process, result_out);
        cancellation_latched = cancellation_latched || mcp_request_cancelled(srv);
        deadline_latched = deadline_latched || (deadline_enabled && cbm_now_ms() >= deadline_ms);
        if (!output_limit_latched && output_limit > 0) {
            int64_t output_size = cbm_file_size(output_path);
            output_limit_latched = output_size > 0 && (uint64_t)output_size > output_limit;
        }
        if (state != CBM_PROC_POLL_RUNNING) {
            break;
        }
        cbm_usleep(10000);
    }
    bool contained = state == CBM_PROC_POLL_TERMINAL && result_out->tree_quiesced &&
                     !result_out->supervision_failed;
    cbm_subprocess_destroy(process);
    if (!output_limit_latched && output_limit > 0) {
        int64_t final_size = cbm_file_size(output_path);
        output_limit_latched = final_size > 0 && (uint64_t)final_size > output_limit;
    }
    if (!contained) {
        return MCP_SCAN_SUPERVISION_FAILURE;
    }
    if (cancellation_latched) {
        return MCP_SCAN_CANCELLED;
    }
    if (deadline_latched) {
        return MCP_SCAN_DEADLINE;
    }
    if (output_limit_latched) {
        return MCP_SCAN_OUTPUT_LIMIT;
    }
#ifndef _WIN32
    if (exit_one_is_no_match && result_out->outcome == CBM_PROC_EXIT_NONZERO &&
        result_out->exit_code == 1) {
        return MCP_SCAN_SUCCESS; /* ordinary direct grep no-match */
    }
#endif
    return result_out->outcome == CBM_PROC_CLEAN ? MCP_SCAN_SUCCESS
                                                 : MCP_SCAN_CONTAINED_COMMAND_FAILURE;
}

static int mcp_run_shell_command_cancellable(cbm_mcp_server_t *srv, const char *command,
                                             char output_path[CBM_SZ_2K],
                                             cbm_proc_result_t *result_out) {
    mcp_scan_cause_t cause = mcp_run_shell_command_cancellable_bounded(
        srv, command, output_path, 0, 0, false, false, false, result_out);
    /* Legacy callers inspect result_out for cancellation/exit status. Preserve
     * their original contract: any contained terminal tree is transport
     * success; only spawn/rejection or failed supervision is a wrapper error. */
    return cause == MCP_SCAN_COMMAND_FAILURE || cause == MCP_SCAN_SUPERVISION_FAILURE ? -1 : 0;
}

/* Does `node`'s line range overlap any recorded hunk for `file`? Used to scope
 * seed detection to the actually-changed lines rather than the whole file.
 * Non-static (declared in mcp_internal.h) so tests can exercise the overlap
 * logic directly, matching this file's existing white-box test hooks. */
bool cbm_detect_node_in_hunks(const cbm_node_t *node, const cbm_changed_hunk_t *hunks,
                              int hunk_count, const char *file) {
    for (int h = 0; h < hunk_count; h++) {
        if (strcmp(hunks[h].path, file) == 0 && node->start_line <= hunks[h].end_line &&
            node->end_line >= hunks[h].start_line) {
            return true;
        }
    }
    return false;
}

/* Collect BFS seed ids for one changed file (everything but the structural
 * container labels — those have no CALLS edges). These anchor the
 * multi-source impact traversal.
 *
 * When `hunks` has at least one entry for `file`, only definitions whose line
 * range overlaps a hunk become seeds — a one-line edit inside a single
 * function no longer seeds every other definition in the file. When no hunk
 * is recorded for `file` (a brand-new/untracked file has no comparable
 * "before" state, or the hunk fetch failed/was skipped), every non-container
 * definition in the file is a seed — the previous, whole-file behavior — so
 * this is a precision improvement, not a new failure mode. */
/* Structural container labels carry no CALLS edges and span the whole file, so
 * they are never seeds. Shared by the seeding loop and the overlap probe. */
static bool detect_is_seedable_label(const char *lb) {
    return lb && strcmp(lb, "File") != 0 && strcmp(lb, "Folder") != 0 &&
           strcmp(lb, "Project") != 0 && strcmp(lb, "Module") != 0 && strcmp(lb, "Package") != 0 &&
           strcmp(lb, "Section") != 0;
}

static void detect_collect_seeds(cbm_store_t *store, const char *project, const char *file,
                                 const cbm_changed_hunk_t *hunks, int hunk_count, int64_t **seeds,
                                 int *n, int *cap) {
    cbm_node_t *nodes = NULL;
    int ncount = 0;
    cbm_store_find_nodes_by_file(store, project, file, &nodes, &ncount);
    bool scope_to_hunks = false;
    for (int h = 0; h < hunk_count; h++) {
        if (strcmp(hunks[h].path, file) == 0) {
            scope_to_hunks = true;
            break;
        }
    }
    /* A file can have hunks yet no SEEDABLE definition overlapping any of them:
     * an import-only edit, a module-level constant, or a change above the first
     * definition all land outside every definition's line range. Scoping would
     * then drop the file from the seed set entirely — strictly worse recall
     * than the whole-file behavior this replaces. Probe for an overlap first
     * and keep whole-file seeding for that file when there is none.
     *
     * The probe must apply the same label filter as the seeding loop below:
     * container nodes span the whole file (a Module node is lines 1..EOF), so
     * counting them would report an overlap for every hunk and defeat the
     * fallback entirely. */
    if (scope_to_hunks) {
        bool any_overlap = false;
        for (int i = 0; i < ncount && !any_overlap; i++) {
            any_overlap = detect_is_seedable_label(nodes[i].label) &&
                          cbm_detect_node_in_hunks(&nodes[i], hunks, hunk_count, file);
        }
        scope_to_hunks = any_overlap;
    }
    for (int i = 0; i < ncount; i++) {
        if (detect_is_seedable_label(nodes[i].label)) {
            if (scope_to_hunks && !cbm_detect_node_in_hunks(&nodes[i], hunks, hunk_count, file)) {
                continue;
            }
            if (*n >= *cap) {
                *cap = *cap ? *cap * 2 : 16;
                *seeds = safe_realloc(*seeds, (size_t)*cap * sizeof(int64_t));
            }
            (*seeds)[(*n)++] = nodes[i].id;
        }
    }
    cbm_store_free_nodes(nodes, ncount);
}

/* Module key for the impacted rollup = the first TWO path segments
 * ("src/mcp/mcp.c" -> "src/mcp"), a quotient of the blast radius coarse enough
 * to fit yet specific enough to localize (one segment collapses a whole tree
 * to "src"). Falls back to one segment, then the whole path. */
static char *detect_module_of(const char *file) {
    if (!file || !file[0]) {
        return heap_strdup("(root)");
    }
    const char *s1 = strchr(file, '/');
    if (!s1) {
        return heap_strdup(file);
    }
    const char *s2 = strchr(s1 + 1, '/');
    size_t len = s2 ? (size_t)(s2 - file) : strlen(file);
    return cbm_strndup(file, len);
}

/* Aggregate the impact set into the 2-segment module rollup. Fills up to
 * DETECT_MODCAP (module, count) pairs; symbols beyond the cap land in
 * *overflow (surfaced as "(other)", never silently dropped). Shared by the
 * tree and json emitters so both encodings carry the same model. */
enum { DETECT_MODCAP = 256 };

typedef struct {
    char *name;
    int count;
} detect_module_row_t;

static int detect_module_rollup(const cbm_traverse_result_t *impact, detect_module_row_t *modules,
                                int *overflow) {
    int nmods = 0;
    *overflow = 0;
    for (int i = 0; i < impact->visited_count; i++) {
        char *module = detect_module_of(impact->visited[i].node.file_path);
        if (!module) {
            (*overflow)++;
            continue;
        }
        int j = 0;
        for (; j < nmods; j++) {
            if (strcmp(modules[j].name, module) == 0) {
                modules[j].count++;
                break;
            }
        }
        if (j == nmods) {
            if (nmods < DETECT_MODCAP) {
                modules[nmods].name = module;
                modules[nmods].count = 1;
                nmods++;
                module = NULL;
            } else {
                (*overflow)++;
            }
        }
        free(module);
    }
    return nmods;
}

static void detect_module_rollup_free(detect_module_row_t *modules, int count) {
    for (int i = 0; i < count; i++) {
        free(modules[i].name);
    }
    free(modules);
}

/* Emit one losslessly pageable window of the impacted set. The visited array
 * is hop-ordered, so page zero preserves the closest, highest-signal rows.
 * Engine saturation is reported as a lower-bound total, never as an exact
 * count. */
static void detect_emit_impacted_tree(cbm_sb_t *sb, const cbm_traverse_result_t *tr, int start,
                                      int count, bool engine_saturated) {
    cbm_tree_scalar_int(sb, "impacted_total", tr->visited_count);
    cbm_tree_scalar_str(sb, "impacted_total_relation", engine_saturated ? "gte" : "eq");
    int shown = count;
    /* qn order for stable grouping, but keep hop-closeness: sort by (hop) is
     * lost under qn sort, so group AFTER selecting the nearest `shown` rows —
     * the visited array is already (hop,id)-ordered from the BFS. */
    cbm_tree_scalar_int(sb, "impacted_shown", shown);
    cbm_node_hop_t *selected = NULL;
    const cbm_node_hop_t *rows = tr->visited;
    if (shown > 0) {
        selected = malloc((size_t)shown * sizeof(*selected));
        if (selected) {
            memcpy(selected, tr->visited + start, (size_t)shown * sizeof(*selected));
            rows = selected;
        } else {
            rows = tr->visited + start;
        }
    }
    if (shown > 1 && selected) {
        qsort(selected, (size_t)shown, sizeof(*selected), tree_hop_cmp_qn);
    }
    static const char *const columns[] = {"qn", "label", "file", "hop"};
    const char **cells = shown > 0 ? calloc((size_t)shown * 4U, sizeof(*cells)) : NULL;
    char (*hop_text)[32] = shown > 0 ? calloc((size_t)shown, sizeof(*hop_text)) : NULL;
    if (shown == 0 || (cells && hop_text)) {
        for (int i = 0; i < shown; i++) {
            size_t base = (size_t)i * 4U;
            cells[base] = rows[i].node.qualified_name ? rows[i].node.qualified_name : "";
            cells[base + 1U] = rows[i].node.label ? rows[i].node.label : "";
            cells[base + 2U] = rows[i].node.file_path ? rows[i].node.file_path : "";
            snprintf(hop_text[i], sizeof(hop_text[i]), "%d", rows[i].hop);
            cells[base + 3U] = hop_text[i];
        }
        static const bool string_cols[] = {true, true, true, false};
        static const bool prefix_cols[] = {true, false, true, false};
        cbm_tree_table_rows_profiled(sb, "impacted", shown, columns, 4, cells, string_cols,
                                     prefix_cols);
    } else {
        cbm_tree_table_header(sb, "impacted", 0, columns, 4);
        cbm_tree_scalar_str(sb, "impacted_render_error", "out_of_memory");
    }
    free(hop_text);
    free(cells);
    free(selected);
    bool has_more = start + shown < tr->visited_count;
    cbm_tree_scalar_bool(sb, "impacted_has_more", has_more);
    if (has_more && shown > 0) {
        cbm_tree_scalar_int(sb, "impacted_next_offset", start + shown);
    } else if (has_more) {
        cbm_tree_scalar_bool(sb, "impacted_continuation_requires_higher_budget", true);
    }
}

/* Portable, allocation-backed record reader. Git's -z formats preserve UTF-8
 * and allow every path byte except NUL, including newlines. A fixed or
 * line-based buffer would either quote or split valid paths. */
static char *detect_read_record(FILE *stream, int delimiter, bool *oom, bool *terminated) {
    cbm_sb_t record;
    cbm_sb_init(&record);
    bool saw_input = false;
    if (terminated) {
        *terminated = false;
    }
    for (;;) {
        int ch = fgetc(stream);
        if (ch == EOF) {
            break;
        }
        saw_input = true;
        if (ch == delimiter) {
            if (terminated) {
                *terminated = true;
            }
            break;
        }
        char byte = (char)ch;
        cbm_sb_append_n(&record, &byte, 1);
        if (record.oom) {
            break;
        }
    }
    if (record.oom) {
        if (oom) {
            *oom = true;
        }
        cbm_sb_free(&record);
        return NULL;
    }
    if (!saw_input) {
        cbm_sb_free(&record);
        return NULL;
    }
    return cbm_sb_finish(&record);
}

static bool detect_add_changed_path(char ***files, int *file_count, int *file_cap,
                                    const char *path) {
    if (!path || !path[0]) {
        return true;
    }
    for (int i = 0; i < *file_count; i++) {
        if (strcmp((*files)[i], path) == 0) {
            return true;
        }
    }
    if (*file_count >= *file_cap) {
        int next_cap = *file_cap ? *file_cap * 2 : 16;
        char **grown = realloc(*files, (size_t)next_cap * sizeof(*grown));
        if (!grown) {
            return false;
        }
        *files = grown;
        *file_cap = next_cap;
    }
    char *copy = heap_strdup(path);
    if (!copy) {
        return false;
    }
    (*files)[(*file_count)++] = copy;
    return true;
}

static int detect_changed_path_compare(const void *left, const void *right) {
    const char *const *left_path = left;
    const char *const *right_path = right;
    return strcmp(*left_path, *right_path);
}

static bool detect_valid_object_id(const char *value) {
    size_t length = value ? strlen(value) : 0;
    if (length != 40 && length != 64) {
        return false;
    }
    for (size_t i = 0; i < length; i++) {
        if (!isxdigit((unsigned char)value[i])) {
            return false;
        }
    }
    return true;
}

typedef struct {
    char stream;       /* c=changed files, i=impacted symbols, m=module rollup */
    char snapshot[33]; /* first 128 bits of the SHA-256 live-state fingerprint */
    uint64_t qhash;    /* semantic query identity; page sizing is deliberately excluded */
    int offset;        /* next row in this independently pageable stream */
} detect_cursor_t;

static uint64_t detect_params_hash(const char *project, const char *base_branch, const char *scope,
                                   const char *direction, int depth) {
    uint64_t hash = 0xcbf29ce484222325ULL;
    hash = cursor_fnv1a64(project ? project : "", hash);
    hash = cursor_fnv1a64("|", hash);
    hash = cursor_fnv1a64(base_branch ? base_branch : "", hash);
    hash = cursor_fnv1a64("|", hash);
    hash = cursor_fnv1a64(scope ? scope : "impact", hash);
    hash = cursor_fnv1a64("|", hash);
    hash = cursor_fnv1a64(direction ? direction : "inbound", hash);
    char depth_text[32];
    snprintf(depth_text, sizeof(depth_text), "|%d", depth);
    return cursor_fnv1a64(depth_text, hash);
}

static void detect_cursor_encode(char stream, const char snapshot[33], uint64_t qhash, int offset,
                                 char out[80]) {
    snprintf(out, 80, "d1.%c.%s.%016llx.%d", stream, snapshot, (unsigned long long)qhash, offset);
}

static const char *detect_cursor_decode(const char *token, char expected_stream,
                                        const char current_snapshot[33], uint64_t expected_qhash,
                                        detect_cursor_t *out) {
    memset(out, 0, sizeof(*out));
    if (!token || strncmp(token, "d1.", 3) != 0 || token[3] != expected_stream || token[4] != '.') {
        return "invalid_cursor: unrecognized detect_changes cursor — rerun without the cursor";
    }
    out->stream = token[3];
    const char *snapshot_start = token + 5;
    const char *snapshot_end = strchr(snapshot_start, '.');
    if (!snapshot_end || snapshot_end - snapshot_start != 32) {
        return "invalid_cursor: unrecognized detect_changes cursor — rerun without the cursor";
    }
    for (const char *digit = snapshot_start; digit < snapshot_end; digit++) {
        if (!isxdigit((unsigned char)*digit)) {
            return "invalid_cursor: unrecognized detect_changes cursor — rerun without the cursor";
        }
    }
    memcpy(out->snapshot, snapshot_start, 32);
    out->snapshot[32] = '\0';

    const char *hash_start = snapshot_end + 1;
    const char *hash_end = strchr(hash_start, '.');
    if (!hash_end || hash_end - hash_start != 16) {
        return "invalid_cursor: unrecognized detect_changes cursor — rerun without the cursor";
    }
    for (const char *digit = hash_start; digit < hash_end; digit++) {
        if (!isxdigit((unsigned char)*digit)) {
            return "invalid_cursor: unrecognized detect_changes cursor — rerun without the cursor";
        }
    }
    errno = 0;
    char *parsed_end = NULL;
    unsigned long long parsed_hash = strtoull(hash_start, &parsed_end, 16);
    if (errno == ERANGE || parsed_end != hash_end) {
        return "invalid_cursor: unrecognized detect_changes cursor — rerun without the cursor";
    }
    errno = 0;
    long parsed_offset = strtol(hash_end + 1, &parsed_end, 10);
    if (errno == ERANGE || parsed_end == hash_end + 1 || *parsed_end != '\0' || parsed_offset < 1 ||
        parsed_offset > INT_MAX) {
        return "invalid_cursor: unrecognized detect_changes cursor — rerun without the cursor";
    }
    out->qhash = (uint64_t)parsed_hash;
    out->offset = (int)parsed_offset;
    if (out->qhash != expected_qhash) {
        return "cursor_params_mismatch: detect_changes cursor belongs to different semantic "
               "arguments — rerun without the cursor";
    }
    if (strcmp(out->snapshot, current_snapshot) != 0) {
        return "snapshot_changed: commits, worktree, or graph changed since this cursor was issued "
               "— rerun detect_changes without the cursor";
    }
    return NULL;
}

static void detect_snapshot_add_field(cbm_sha256_ctx *hash, const char *value) {
    size_t length = value ? strlen(value) : 0;
    char length_text[32];
    int count = snprintf(length_text, sizeof(length_text), "%zu:", length);
    cbm_sha256_update(hash, length_text, (size_t)count);
    if (length > 0) {
        cbm_sha256_update(hash, value, length);
    }
    cbm_sha256_update(hash, "|", 1);
}

static bool detect_snapshot_add_changed_file(cbm_mcp_server_t *srv, cbm_sha256_ctx *hash,
                                             const char *root_path, const char *relative_path) {
    detect_snapshot_add_field(hash, relative_path);
    size_t root_len = strlen(root_path);
    size_t relative_len = strlen(relative_path);
    if (root_len > (size_t)-1 - relative_len - 2U) {
        detect_snapshot_add_field(hash, "path_overflow");
        return false;
    }
    char *absolute = malloc(root_len + relative_len + 2U);
    if (!absolute) {
        detect_snapshot_add_field(hash, "path_oom");
        return false;
    }
    memcpy(absolute, root_path, root_len);
    absolute[root_len] = '/';
    memcpy(absolute + root_len + 1U, relative_path, relative_len + 1U);

    cbm_path_info_t info = {0};
    int info_status = cbm_path_info_utf8(absolute, &info);
    if (info_status == CBM_PATH_INFO_ABSENT) {
        detect_snapshot_add_field(hash, "absent");
        free(absolute);
        return true;
    }
    if (info_status != CBM_PATH_INFO_OK) {
        /* Hash the observed failure state for diagnostics, but do not issue a
         * cursor from incomplete evidence. Offset paging remains available. */
        detect_snapshot_add_field(hash, "metadata_unavailable");
        free(absolute);
        return false;
    }
    char metadata[160];
    snprintf(metadata, sizeof(metadata), "r%d:d%d:l%d:s%lld:m%lld", info.is_regular ? 1 : 0,
             info.is_directory ? 1 : 0, info.is_symlink ? 1 : 0, (long long)info.size,
             (long long)info.mtime_ns);
    detect_snapshot_add_field(hash, metadata);
    bool complete = true;
    if (info.is_regular) {
        FILE *file = NULL;
#ifdef CBM_ENABLE_TEST_SEAMS
        bool allow_open = !srv || !srv->snapshot_read_test_hook ||
                          srv->snapshot_read_test_hook(srv->snapshot_read_test_context, absolute);
        if (allow_open) {
            file = cbm_fopen(absolute, "rb");
        }
#else
        (void)srv;
        file = cbm_fopen(absolute, "rb");
#endif
        if (file) {
            unsigned char buffer[64 * 1024];
            size_t count;
            while ((count = fread(buffer, 1, sizeof(buffer), file)) > 0) {
                cbm_sha256_update(hash, buffer, count);
            }
            bool read_error = ferror(file) != 0;
            detect_snapshot_add_field(hash, read_error ? "read_error" : "read_complete");
            complete = !read_error;
            (void)fclose(file);
        } else {
            detect_snapshot_add_field(hash, "open_error");
            complete = false;
        }
    } else if (info.is_symlink) {
        /* lstat-style metadata identifies the link object but not its target
         * bytes. The cross-platform metadata API intentionally does not expose
         * a readlink/reparse payload, so fail closed instead of pretending the
         * cursor is bound to the changed link identity. */
        detect_snapshot_add_field(hash, "link_identity_unavailable");
        complete = false;
    }
    free(absolute);
    return complete;
}

static bool detect_snapshot_fingerprint(cbm_mcp_server_t *srv, const char *root_path,
                                        const char *head_oid, const char *base_oid,
                                        const char *merge_base, const char *generation,
                                        char **files, int file_count,
                                        const cbm_traverse_result_t *impact,
                                        const detect_module_row_t *modules, int module_count,
                                        int module_overflow, char out[33]) {
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    detect_snapshot_add_field(&hash, "detect_changes_snapshot_v1");
    detect_snapshot_add_field(&hash, head_oid);
    detect_snapshot_add_field(&hash, base_oid);
    detect_snapshot_add_field(&hash, merge_base);
    detect_snapshot_add_field(&hash, generation);
    bool complete = true;
    for (int i = 0; i < file_count; i++) {
        if (!detect_snapshot_add_changed_file(srv, &hash, root_path, files[i])) {
            complete = false;
        }
    }
    if (impact) {
        for (int i = 0; i < impact->visited_count; i++) {
            detect_snapshot_add_field(&hash, impact->visited[i].node.qualified_name);
            detect_snapshot_add_field(&hash, impact->visited[i].node.label);
            detect_snapshot_add_field(&hash, impact->visited[i].node.file_path);
            char row_metadata[64];
            snprintf(row_metadata, sizeof(row_metadata), "%d:%lld", impact->visited[i].hop,
                     (long long)impact->visited[i].node.id);
            detect_snapshot_add_field(&hash, row_metadata);
        }
    }
    for (int i = 0; i < module_count; i++) {
        detect_snapshot_add_field(&hash, modules[i].name);
        char count_text[32];
        snprintf(count_text, sizeof(count_text), "%d", modules[i].count);
        detect_snapshot_add_field(&hash, count_text);
    }
    char overflow_text[32];
    snprintf(overflow_text, sizeof(overflow_text), "%d", module_overflow);
    detect_snapshot_add_field(&hash, overflow_text);

    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    static const char hex[] = "0123456789abcdef";
    cbm_sha256_final(&hash, digest);
    for (int i = 0; i < 16; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    out[32] = '\0';
    return complete;
}

static char *handle_detect_changes(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    char *base_branch = cbm_mcp_get_string_arg(args, "base_branch");
    char *since = cbm_mcp_get_string_arg(args, "since");
    char *scope = cbm_mcp_get_string_arg(args, "scope");
    int depth = cbm_mcp_get_int_arg(args, "depth", MCP_DEFAULT_BFS_DEPTH);
    depth = clamp_mcp_depth(depth, "detect_changes");

    /* scope: "files" = changed files only; "impact" = files + symbols (default).
     * Keep accepting the legacy "symbols" spelling for compatibility. */
    bool want_symbols = !scope || strcmp(scope, "symbols") == 0 || strcmp(scope, "impact") == 0;

    /* `since` (e.g. "HEAD~10", "v0.5.0") is the documented diff base but was
     * previously parsed and never used: it takes precedence over base_branch.
     * Route it through base_branch so the shared shell-arg validation and the
     * existing `<base>...HEAD` (three-dot) diff apply unchanged — `since` thus
     * adopts the same merge-base semantics base_branch already uses. */
    if (since && since[0]) {
        free(base_branch);
        base_branch = since; /* transfer ownership */
        since = NULL;
    }
    free(since); /* no-op after the swap (since is NULL); frees it otherwise */

    if (!base_branch) {
        base_branch = heap_strdup("main");
    }

    /* Reject shell metacharacters, and a leading '-', in the user-supplied
     * branch name. base_branch is spliced into `git diff --name-only
     * "<base>"...HEAD`; a value starting with '-' would be read by git as an
     * option rather than a ref (e.g. `--output=<path>` writes the diff to an
     * arbitrary file). A real git ref never begins with '-'. */
    if (!cbm_validate_shell_arg(base_branch) || base_branch[0] == '-' ||
        !validate_windows_cmd_interpolation_arg(base_branch)) {
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result("base_branch contains invalid characters", true);
    }

    char *root_path = get_project_root(srv, project);
    if (!root_path) {
        char *err = build_no_store_error_checked(srv, project);
        char *res = cbm_mcp_text_result(err, true);
        free(err);
        free(project);
        free(base_branch);
        free(scope);
        return res;
    }

    if (!validate_search_path_arg(root_path) ||
        !validate_windows_cmd_interpolation_arg(root_path)) {
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result("project path contains invalid characters", true);
    }

    /* Every detect snapshot and cursor is generation-bound. Validate the
     * metadata immediately after store resolution so fresh requests and
     * cursor replays fail identically before Git work or cursor minting. */
    cbm_store_t *store = srv->store;
    char generation[96] = "";
    if (cbm_store_generation(store, generation, sizeof(generation)) != CBM_STORE_OK) {
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result(
            "index_metadata_error: generation metadata is unreadable; reindex before detecting "
            "changes",
            true);
    }

    /* Direction of impact. Default inbound = the BLAST RADIUS: the transitive
     * CALLERS of the changed symbols, which may need review. outbound = what
     * the changed code depends on; both = union. */
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    if (!direction) {
        direction = heap_strdup("inbound");
    }
    /* Teaching error, same contract as trace_path: never silently correct an
     * unknown direction — the caller would misread the result's semantics. */
    if (strcmp(direction, "inbound") != 0 && strcmp(direction, "outbound") != 0 &&
        strcmp(direction, "both") != 0) {
        char errbuf[CBM_SZ_256];
        snprintf(errbuf, sizeof(errbuf),
                 "invalid direction \"%s\" — use \"inbound\" (blast radius: transitive callers), "
                 "\"outbound\" (dependencies), or \"both\"",
                 direction);
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result(errbuf, true);
    }
    char *fmt = cbm_mcp_get_string_arg(args, "format");
    bool legacy_json = fmt && strcmp(fmt, "json") == 0;
    free(fmt);

    /* Freeze both endpoints before collecting paths. A failed or unknown base
     * is a request error, never an exact-looking empty diff, and a concurrent
     * HEAD advance cannot mix revisions within one answer. */
    char head_oid[65] = "";
    char base_oid[65] = "";
    char resolve_cmd[CBM_SZ_2K];
#ifdef _WIN32
    snprintf(resolve_cmd, sizeof(resolve_cmd),
             "git -C \"%s\" rev-parse \"HEAD^{commit}\" \"%s^{commit}\" 2>NUL", root_path,
             base_branch);
#else
    snprintf(resolve_cmd, sizeof(resolve_cmd),
             "git -C '%s' rev-parse 'HEAD^{commit}' '%s^{commit}' 2>/dev/null", root_path,
             base_branch);
#endif
    char resolve_output_path[CBM_SZ_2K] = {0};
    cbm_proc_result_t resolve_result = {0};
    int resolve_run =
        mcp_run_shell_command_cancellable(srv, resolve_cmd, resolve_output_path, &resolve_result);
    bool resolve_cancelled = resolve_result.cancellation_requested || mcp_request_cancelled(srv);
    bool resolve_oom = false;
    char *resolved_head = NULL;
    char *resolved_base = NULL;
    FILE *resolve_fp = resolve_run == 0 && resolve_result.exit_code == 0 && !resolve_cancelled
                           ? cbm_fopen(resolve_output_path, "rb")
                           : NULL;
    if (resolve_fp) {
        bool head_terminated = false;
        bool base_terminated = false;
        resolved_head = detect_read_record(resolve_fp, '\n', &resolve_oom, &head_terminated);
        resolved_base = detect_read_record(resolve_fp, '\n', &resolve_oom, &base_terminated);
        if (resolved_head) {
            size_t length = strlen(resolved_head);
            if (length > 0 && resolved_head[length - 1] == '\r') {
                resolved_head[--length] = '\0';
            }
            if (head_terminated && detect_valid_object_id(resolved_head)) {
                memcpy(head_oid, resolved_head, length + 1U);
            }
        }
        if (resolved_base) {
            size_t length = strlen(resolved_base);
            if (length > 0 && resolved_base[length - 1] == '\r') {
                resolved_base[--length] = '\0';
            }
            if (base_terminated && detect_valid_object_id(resolved_base)) {
                memcpy(base_oid, resolved_base, length + 1U);
            }
        }
        (void)fclose(resolve_fp);
    }
    free(resolved_head);
    free(resolved_base);
    if (resolve_output_path[0]) {
        (void)cbm_unlink(resolve_output_path);
    }
    if (resolve_cancelled || resolve_run != 0 || resolve_result.exit_code != 0 || resolve_oom ||
        !head_oid[0] || !base_oid[0]) {
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        if (resolve_cancelled) {
            return cbm_mcp_text_result("detect_changes cancelled for this request", true);
        }
        if (resolve_run != 0) {
            return cbm_mcp_text_result(
                "git revision resolution failed: the contained command could not complete", true);
        }
        return cbm_mcp_text_result(
            "git revision resolution failed: base_branch or HEAD is not a commit", true);
    }

    char merge_base[65] = "";
    char mbcmd[CBM_SZ_2K];
#ifdef _WIN32
    snprintf(mbcmd, sizeof(mbcmd), "git -C \"%s\" merge-base \"%s\" \"%s\" 2>NUL", root_path,
             base_oid, head_oid);
#else
    snprintf(mbcmd, sizeof(mbcmd), "git -C '%s' merge-base '%s' '%s' 2>/dev/null", root_path,
             base_oid, head_oid);
#endif
    char mb_output_path[CBM_SZ_2K] = {0};
    cbm_proc_result_t mb_result = {0};
    int mb_run = mcp_run_shell_command_cancellable(srv, mbcmd, mb_output_path, &mb_result);
    bool mb_cancelled = mb_result.cancellation_requested || mcp_request_cancelled(srv);
    bool mb_oom = false;
    bool mb_terminated = false;
    char *mb_record = NULL;
    FILE *mbfp = mb_run == 0 && mb_result.exit_code == 0 && !mb_cancelled
                     ? cbm_fopen(mb_output_path, "rb")
                     : NULL;
    if (mbfp) {
        mb_record = detect_read_record(mbfp, '\n', &mb_oom, &mb_terminated);
        (void)fclose(mbfp);
    }
    if (mb_record) {
        size_t length = strlen(mb_record);
        if (length > 0 && mb_record[length - 1] == '\r') {
            mb_record[--length] = '\0';
        }
        if (detect_valid_object_id(mb_record)) {
            memcpy(merge_base, mb_record, length + 1U);
        }
    }
    free(mb_record);
    if (mb_output_path[0]) {
        (void)cbm_unlink(mb_output_path);
    }
    if (mb_cancelled || mb_run != 0 || mb_result.exit_code != 0 || mb_oom || !merge_base[0]) {
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        if (mb_cancelled) {
            return cbm_mcp_text_result("detect_changes cancelled for this request", true);
        }
        if (mb_run != 0) {
            return cbm_mcp_text_result(
                "git merge-base failed: the contained command could not complete", true);
        }
        return cbm_mcp_text_result(
            "git merge-base failed: base_branch has no common ancestor with HEAD", true);
    }

    /* Collect exact Git path records. `-z` disables C-style path quoting and
     * keeps embedded newlines intact. Diff and porcelain status stay separate:
     * diff emits bare paths, while status emits typed `XY destination` records
     * and an extra source record for renames/copies. */
    char **files = NULL;
    int file_count = 0;
    int file_cap = 0;
    bool changed_path_oom = false;
    bool changed_path_malformed = false;

    char diff_cmd[CBM_SZ_2K];
#ifdef _WIN32
    snprintf(diff_cmd, sizeof(diff_cmd),
             "git -c core.quotePath=false -C \"%s\" diff --name-only -z \"%s\" \"%s\" -- 2>NUL "
             "&& git -c core.quotePath=false -C \"%s\" diff --name-only -z -- 2>NUL",
             root_path, merge_base, head_oid, root_path);
#else
    snprintf(diff_cmd, sizeof(diff_cmd),
             "git -c core.quotePath=false -C '%s' diff --name-only -z '%s' '%s' -- 2>/dev/null "
             "&& git -c core.quotePath=false -C '%s' diff --name-only -z -- 2>/dev/null",
             root_path, merge_base, head_oid, root_path);
#endif
    char diff_output_path[CBM_SZ_2K] = {0};
    cbm_proc_result_t diff_result = {0};
    int diff_run = mcp_run_shell_command_cancellable(srv, diff_cmd, diff_output_path, &diff_result);
    bool diff_cancelled = diff_result.cancellation_requested || mcp_request_cancelled(srv);
    FILE *diff_fp = diff_run == 0 && diff_result.exit_code == 0 && !diff_cancelled
                        ? cbm_fopen(diff_output_path, "rb")
                        : NULL;
    bool diff_output_opened = diff_fp != NULL;
    if (diff_fp) {
        for (;;) {
            bool terminated = false;
            char *record = detect_read_record(diff_fp, '\0', &changed_path_oom, &terminated);
            if (!record) {
                break;
            }
            if (!terminated) {
                changed_path_malformed = true;
                free(record);
                break;
            }
            if (!detect_add_changed_path(&files, &file_count, &file_cap, record)) {
                changed_path_oom = true;
                free(record);
                break;
            }
            free(record);
        }
        (void)fclose(diff_fp);
    }
    if (diff_output_path[0]) {
        (void)cbm_unlink(diff_output_path);
    }
    if (diff_cancelled || diff_run != 0 || diff_result.exit_code != 0 || !diff_output_opened ||
        changed_path_oom || changed_path_malformed) {
        for (int i = 0; i < file_count; i++) {
            free(files[i]);
        }
        free(files);
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        if (diff_cancelled) {
            return cbm_mcp_text_result("detect_changes cancelled for this request", true);
        }
        if (changed_path_oom) {
            return cbm_mcp_text_result("out of memory while reading changed paths", true);
        }
        if (changed_path_malformed) {
            return cbm_mcp_text_result("git diff returned a malformed path record", true);
        }
        return cbm_mcp_text_result(diff_run != 0
                                       ? "git diff failed: the contained command could not complete"
                                       : "git diff failed while collecting changed paths",
                                   true);
    }

    char status_cmd[CBM_SZ_2K];
#ifdef _WIN32
    snprintf(status_cmd, sizeof(status_cmd),
             "git --no-optional-locks -c core.quotePath=false -C \"%s\" status "
             "--porcelain=v1 -z --untracked-files=all -- 2>NUL",
             root_path);
#else
    snprintf(status_cmd, sizeof(status_cmd),
             "git --no-optional-locks -c core.quotePath=false -C '%s' status "
             "--porcelain=v1 -z --untracked-files=all -- 2>/dev/null",
             root_path);
#endif
    char status_output_path[CBM_SZ_2K] = {0};
    cbm_proc_result_t status_result = {0};
    int status_run =
        mcp_run_shell_command_cancellable(srv, status_cmd, status_output_path, &status_result);
    bool status_cancelled = status_result.cancellation_requested || mcp_request_cancelled(srv);
    FILE *status_fp = status_run == 0 && status_result.exit_code == 0 && !status_cancelled
                          ? cbm_fopen(status_output_path, "rb")
                          : NULL;
    bool status_output_opened = status_fp != NULL;
    if (status_fp) {
        for (;;) {
            bool terminated = false;
            char *record = detect_read_record(status_fp, '\0', &changed_path_oom, &terminated);
            if (!record) {
                break;
            }
            size_t length = strlen(record);
            bool typed = terminated && length > PAIR_LEN + 1U && record[PAIR_LEN] == ' ';
            bool rename_or_copy = typed && (record[0] == 'R' || record[0] == 'C' ||
                                            record[1] == 'R' || record[1] == 'C');
            if (!typed) {
                changed_path_malformed = true;
                free(record);
                break;
            }
            if (!detect_add_changed_path(&files, &file_count, &file_cap, record + PAIR_LEN + 1U)) {
                changed_path_oom = true;
                free(record);
                break;
            }
            free(record);
            if (rename_or_copy) {
                bool source_terminated = false;
                char *source =
                    detect_read_record(status_fp, '\0', &changed_path_oom, &source_terminated);
                if (!source || !source_terminated || !source[0]) {
                    free(source);
                    changed_path_malformed = !changed_path_oom;
                    break;
                }
                free(source);
            }
        }
        (void)fclose(status_fp);
    }
    if (status_output_path[0]) {
        (void)cbm_unlink(status_output_path);
    }
    if (status_cancelled || status_run != 0 || status_result.exit_code != 0 ||
        !status_output_opened || changed_path_oom || changed_path_malformed) {
        for (int i = 0; i < file_count; i++) {
            free(files[i]);
        }
        free(files);
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        if (status_cancelled) {
            return cbm_mcp_text_result("detect_changes cancelled for this request", true);
        }
        if (changed_path_oom) {
            return cbm_mcp_text_result("out of memory while reading changed paths", true);
        }
        if (changed_path_malformed) {
            return cbm_mcp_text_result("git status returned a malformed path record", true);
        }
        return cbm_mcp_text_result(
            status_run != 0 ? "git status failed: the contained command could not complete"
                            : "git status failed while collecting changed paths",
            true);
    }
    if (file_count > 1) {
        qsort(files, (size_t)file_count, sizeof(*files), detect_changed_path_compare);
    }

    /* Per-symbol impact page size. Engine saturation makes the reported total
     * an explicit lower bound, while impact_offset continues every materialized
     * row without identifier truncation. */
    int imp_limit = cbm_mcp_get_int_arg(args, "limit", MCP_DEFAULT_IMPACT_LIMIT);
    if (imp_limit < 1) {
        imp_limit = 1;
    }
    if (imp_limit > MCP_BFS_LIMIT_MAX) {
        imp_limit = MCP_BFS_LIMIT_MAX;
    }
    int impact_offset = cbm_mcp_get_int_arg(args, "impact_offset", 0);
    if (impact_offset < 0) {
        impact_offset = 0;
    }
    int changed_limit = cbm_mcp_get_int_arg(args, "changed_limit", 20);
    int changed_offset = cbm_mcp_get_int_arg(args, "changed_offset", 0);
    if (changed_limit < 0) {
        changed_limit = 0;
    } else if (changed_limit > MCP_BFS_LIMIT_MAX) {
        changed_limit = MCP_BFS_LIMIT_MAX;
    }
    if (changed_offset < 0) {
        changed_offset = 0;
    }
    int module_limit = cbm_mcp_get_int_arg(args, "module_limit", 20);
    int module_offset = cbm_mcp_get_int_arg(args, "module_offset", 0);
    if (module_limit < 0) {
        module_limit = 0;
    } else if (module_limit > DETECT_MODCAP) {
        module_limit = DETECT_MODCAP;
    }
    if (module_offset < 0) {
        module_offset = 0;
    }
    int max_output_tokens = cbm_mcp_get_int_arg(args, "max_output_tokens", 3200);
    if (max_output_tokens < 128) {
        max_output_tokens = 128;
    } else if (max_output_tokens > 1000000) {
        max_output_tokens = 1000000;
    }
    char *impact_cursor_arg = cbm_mcp_get_string_arg(args, "impact_cursor");
    char *changed_cursor_arg = cbm_mcp_get_string_arg(args, "changed_cursor");
    char *module_cursor_arg = cbm_mcp_get_string_arg(args, "module_cursor");
    uint64_t detect_qhash = detect_params_hash(project, base_branch,
                                               want_symbols ? "impact" : "files", direction, depth);

    /* Changed paths drive traversal seeds and both output encodings. */
    int64_t *seeds = NULL;
    int seed_count = 0;
    int seed_cap = 0;

    /* Hunk line ranges (unified=0 diff), used to scope seed detection to the
     * actually-changed lines instead of every definition in a changed file
     * (see detect_collect_seeds). Best-effort: any failure here just leaves
     * `hunks` empty and every file falls back to its previous whole-file
     * seeding — this is a precision improvement, not a correctness
     * dependency, so it is never treated as a request-level failure.
     *
     * Coordinate systems: `base...HEAD` hunks carry HEAD-side line numbers,
     * the worktree diff carries worktree-side ones, and node line ranges come
     * from the indexed snapshot. These agree while the index is fresh — the
     * watcher reindexes on HEAD movement and on a dirty tree — but a stale
     * index combined with insertions earlier in the file shifts the node lines
     * relative to the hunks and can mis-scope. The failure is bounded by
     * detect_collect_seeds' zero-overlap fallback: a file whose definitions all
     * miss reverts to whole-file seeding rather than dropping out. */
    cbm_changed_hunk_t *hunks = NULL;
    int hunk_count = 0;
    if (want_symbols) {
        char hunk_cmd[CBM_SZ_2K];
#ifdef _WIN32
        snprintf(hunk_cmd, sizeof(hunk_cmd),
                 "git -C \"%s\" diff --unified=0 \"%s\" \"%s\" -- 2>NUL && "
                 "git -C \"%s\" diff --unified=0 -- 2>NUL",
                 root_path, merge_base, head_oid, root_path);
#else
        snprintf(hunk_cmd, sizeof(hunk_cmd),
                 "git -C '%s' diff --unified=0 '%s' '%s' -- 2>/dev/null && "
                 "git -C '%s' diff --unified=0 -- 2>/dev/null",
                 root_path, merge_base, head_oid, root_path);
#endif
        char hunk_output_path[CBM_SZ_2K] = {0};
        cbm_proc_result_t hunk_result = {0};
        int hunk_run =
            mcp_run_shell_command_cancellable(srv, hunk_cmd, hunk_output_path, &hunk_result);
        bool hunk_cancelled = hunk_result.cancellation_requested || mcp_request_cancelled(srv);
        FILE *hfp = (!hunk_cancelled && hunk_run == 0 && hunk_result.exit_code == 0)
                        ? cbm_fopen(hunk_output_path, "rb")
                        : NULL;
        if (hfp) {
            (void)fseek(hfp, 0, SEEK_END);
            long hsz = ftell(hfp);
            if (hsz > 0) {
                (void)fseek(hfp, 0, SEEK_SET);
                char *hbuf = malloc((size_t)hsz + SKIP_ONE);
                if (hbuf) {
                    size_t hread = fread(hbuf, SKIP_ONE, (size_t)hsz, hfp);
                    hbuf[hread] = '\0';
                    enum { HUNK_CAP = 4096 };
                    hunks = safe_realloc(NULL, (size_t)HUNK_CAP * sizeof(cbm_changed_hunk_t));
                    hunk_count = cbm_parse_hunks(hbuf, hunks, HUNK_CAP);
                    /* A filled buffer means the diff was truncated: the hunks
                     * past the cap are gone, so files captured only partially
                     * would still look scoped and silently under-seed. Drop
                     * scoping for the whole request rather than under-report a
                     * large refactor — whole-file seeding is the safe side. */
                    if (hunk_count >= HUNK_CAP) {
                        cbm_log_info("detect_changes.hunks", "action", "scoping_disabled", "reason",
                                     "hunk_cap_reached");
                        free(hunks);
                        hunks = NULL;
                        hunk_count = 0;
                    }
                    free(hbuf);
                }
            }
            (void)fclose(hfp);
        }
        if (hunk_output_path[0]) {
            (void)cbm_unlink(hunk_output_path);
        }
        /* Hunk parsing is deliberately best-effort, but path collection is not.
         * Seed after both are complete so every exact path uses the same scoping
         * decision. */
        for (int i = 0; i < file_count; i++) {
            detect_collect_seeds(store, project, files[i], hunks, hunk_count, &seeds, &seed_count,
                                 &seed_cap);
        }
    }

    /* The impact traversal: ONE multi-source BFS over all seeds. */
    cbm_traverse_result_t impact = {0};
    bool engine_saturated = false;
    if (want_symbols && seed_count > 0) {
        (void)cbm_store_bfs_multi(store, seeds, seed_count, direction, NULL, 0, depth,
                                  MCP_BFS_LIMIT_MAX, &impact, &engine_saturated);
    }

    detect_module_row_t *modules = NULL;
    int nmods = 0;
    int module_overflow = 0;
    if (want_symbols && impact.visited_count > 0) {
        modules = calloc(DETECT_MODCAP, sizeof(*modules));
        if (modules) {
            nmods = detect_module_rollup(&impact, modules, &module_overflow);
        }
    }
    /* Overflow is represented as one explicit aggregate row. This is the exact
     * number of pageable rollup rows for the materialized impact set; its
     * relation becomes `gte` if traversal hit the engine ceiling. */
    int module_total = nmods + (module_overflow > 0 ? 1 : 0);
    const char *cursor_error = NULL;
    char detect_snapshot[33] = "";
    bool detect_snapshot_complete = detect_snapshot_fingerprint(
        srv, root_path, head_oid, base_oid, merge_base, generation, files, file_count, &impact,
        modules, nmods, module_overflow, detect_snapshot);

    detect_cursor_t decoded_cursor = {0};
    bool cursor_supplied = (changed_cursor_arg && changed_cursor_arg[0]) ||
                           (impact_cursor_arg && impact_cursor_arg[0]) ||
                           (module_cursor_arg && module_cursor_arg[0]);
    if (cursor_supplied && !detect_snapshot_complete) {
        cursor_error = "snapshot_unavailable: changed file bytes could not be fingerprinted — "
                       "rerun without the cursor after the files are readable";
    }
    if (!cursor_error && changed_cursor_arg && changed_cursor_arg[0]) {
        if (changed_offset != 0) {
            cursor_error = "cursor_params_mismatch: changed_cursor cannot be combined with a "
                           "nonzero changed_offset";
        } else {
            cursor_error = detect_cursor_decode(changed_cursor_arg, 'c', detect_snapshot,
                                                detect_qhash, &decoded_cursor);
            if (!cursor_error) {
                changed_offset = decoded_cursor.offset;
            }
        }
    }
    if (!cursor_error && impact_cursor_arg && impact_cursor_arg[0]) {
        if (impact_offset != 0) {
            cursor_error = "cursor_params_mismatch: impact_cursor cannot be combined with a "
                           "nonzero impact_offset";
        } else {
            cursor_error = detect_cursor_decode(impact_cursor_arg, 'i', detect_snapshot,
                                                detect_qhash, &decoded_cursor);
            if (!cursor_error) {
                impact_offset = decoded_cursor.offset;
            }
        }
    }
    if (!cursor_error && module_cursor_arg && module_cursor_arg[0]) {
        if (module_offset != 0) {
            cursor_error = "cursor_params_mismatch: module_cursor cannot be combined with a "
                           "nonzero module_offset";
        } else {
            cursor_error = detect_cursor_decode(module_cursor_arg, 'm', detect_snapshot,
                                                detect_qhash, &decoded_cursor);
            if (!cursor_error) {
                module_offset = decoded_cursor.offset;
            }
        }
    }
    if (cursor_error) {
        cbm_store_traverse_free(&impact);
        detect_module_rollup_free(modules, nmods);
        for (int i = 0; i < file_count; i++) {
            free(files[i]);
        }
        free(files);
        free(seeds);
        free(hunks);
        free(impact_cursor_arg);
        free(changed_cursor_arg);
        free(module_cursor_arg);
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return cbm_mcp_text_result(cursor_error, true);
    }

    int changed_start = changed_offset < file_count ? changed_offset : file_count;
    int changed_returned = file_count - changed_start;
    if (changed_returned > changed_limit) {
        changed_returned = changed_limit;
    }
    int module_start = module_offset < module_total ? module_offset : module_total;
    int module_returned = module_total - module_start;
    if (module_returned > module_limit) {
        module_returned = module_limit;
    }
    int imp_start = impact_offset < impact.visited_count ? impact_offset : impact.visited_count;
    int imp_returned = impact.visited_count - imp_start;
    if (imp_returned > imp_limit) {
        imp_returned = imp_limit;
    }
    size_t output_budget_bytes =
        (size_t)max_output_tokens * (size_t)MCP_OUTPUT_BYTES_PER_TOKEN_ESTIMATE;
    bool output_budget_hit = false;
    bool output_budget_floor_exceeded = false;
    char *out_str = NULL;

render_detect_output:;
    bool changed_has_more = changed_start + changed_returned < file_count;
    bool impacted_has_more = want_symbols && imp_start + imp_returned < impact.visited_count;
    bool module_has_more = want_symbols && module_start + module_returned < module_total;
    bool response_truncated = engine_saturated || output_budget_hit || changed_has_more ||
                              impacted_has_more || module_has_more;
    if (!legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_str(&sb, "base", base_branch);
        if (merge_base[0]) {
            cbm_tree_scalar_str(&sb, "merge_base", merge_base);
        }
        cbm_tree_scalar_str(&sb, "direction", direction);
        if (output_budget_hit) {
            cbm_tree_scalar_str(&sb, "truncation_reason", "output_budget");
            cbm_tree_scalar_int(&sb, "max_output_bytes", (long long)output_budget_bytes);
        }
        if (output_budget_floor_exceeded) {
            cbm_tree_scalar_bool(&sb, "output_budget_floor_exceeded", true);
        }
        if (engine_saturated) {
            cbm_tree_scalar_bool(&sb, "engine_saturated", true);
        }
        if (!detect_snapshot_complete) {
            cbm_tree_scalar_bool(&sb, "snapshot_cursor_unavailable", true);
        }
        /* Changed files are independently pageable: traversal still used every
         * file, so paging affects presentation only, never the graph answer. */
        cbm_tree_scalar_int(&sb, "changed_total", file_count);
        cbm_tree_scalar_int(&sb, "changed_returned", changed_returned);
        cbm_tree_scalar_bool(&sb, "changed_has_more", changed_has_more);
        if (changed_has_more && changed_returned > 0) {
            cbm_tree_scalar_int(&sb, "changed_next_offset", changed_start + changed_returned);
            if (detect_snapshot_complete) {
                char cursor[80];
                detect_cursor_encode('c', detect_snapshot, detect_qhash,
                                     changed_start + changed_returned, cursor);
                cbm_tree_scalar_str(&sb, "changed_next_cursor", cursor);
            }
        } else if (changed_has_more) {
            cbm_tree_scalar_bool(&sb,
                                 changed_limit == 0 ? "changed_continuation_requires_positive_limit"
                                                    : "changed_continuation_requires_higher_budget",
                                 true);
        }
        static const char *const changed_columns[] = {"path"};
        const char **changed_cells =
            changed_returned > 0 ? calloc((size_t)changed_returned, sizeof(*changed_cells)) : NULL;
        if (changed_returned == 0 || changed_cells) {
            for (int row = 0; row < changed_returned; row++) {
                changed_cells[row] = files[changed_start + row];
            }
            static const bool changed_string_columns[] = {true};
            static const bool changed_prefix_columns[] = {true};
            cbm_tree_table_rows_profiled(&sb, "changed_files", changed_returned, changed_columns, 1,
                                         changed_cells, changed_string_columns,
                                         changed_prefix_columns);
        } else {
            cbm_tree_table_header(&sb, "changed_files", 0, changed_columns, 1);
            cbm_tree_scalar_str(&sb, "changed_files_render_error", "out_of_memory");
        }
        free(changed_cells);
        cbm_tree_scalar_int(&sb, "seed_symbols", seed_count);
        if (want_symbols) {
            detect_emit_impacted_tree(&sb, &impact, imp_start, imp_returned, engine_saturated);
            if (detect_snapshot_complete && imp_start + imp_returned < impact.visited_count &&
                imp_returned > 0) {
                char cursor[80];
                detect_cursor_encode('i', detect_snapshot, detect_qhash, imp_start + imp_returned,
                                     cursor);
                cbm_tree_scalar_str(&sb, "impacted_next_cursor", cursor);
            }
            /* module rollup: independently pageable, while counts are still
             * computed from the complete impact set. */
            cbm_tree_scalar_int(&sb, "module_total", module_total);
            cbm_tree_scalar_str(&sb, "module_total_relation", engine_saturated ? "gte" : "eq");
            cbm_tree_scalar_int(&sb, "module_returned", module_returned);
            cbm_tree_scalar_bool(&sb, "module_has_more", module_has_more);
            if (module_has_more && module_returned > 0) {
                cbm_tree_scalar_int(&sb, "module_next_offset", module_start + module_returned);
                if (detect_snapshot_complete) {
                    char cursor[80];
                    detect_cursor_encode('m', detect_snapshot, detect_qhash,
                                         module_start + module_returned, cursor);
                    cbm_tree_scalar_str(&sb, "module_next_cursor", cursor);
                }
            } else if (module_has_more) {
                cbm_tree_scalar_bool(&sb,
                                     module_limit == 0
                                         ? "module_continuation_requires_positive_limit"
                                         : "module_continuation_requires_higher_budget",
                                     true);
            }
            static const char *const columns[] = {"module", "count"};
            const char **cells =
                module_returned > 0 ? calloc((size_t)module_returned * 2U, sizeof(*cells)) : NULL;
            char (*count_text)[32] =
                module_returned > 0 ? calloc((size_t)module_returned, sizeof(*count_text)) : NULL;
            if (module_returned == 0 || (cells && count_text)) {
                for (int row = 0; row < module_returned; row++) {
                    int j = module_start + row;
                    cells[(size_t)row * 2U] = j < nmods ? modules[j].name : "(other)";
                    snprintf(count_text[row], sizeof(count_text[row]), "%d",
                             j < nmods ? modules[j].count : module_overflow);
                    cells[(size_t)row * 2U + 1U] = count_text[row];
                }
                static const bool string_cols[] = {true, false};
                static const bool prefix_cols[] = {true, false};
                cbm_tree_table_rows_profiled(&sb, "impacted_modules", module_returned, columns, 2,
                                             cells, string_cols, prefix_cols);
            } else {
                cbm_tree_table_header(&sb, "impacted_modules", 0, columns, 2);
                cbm_tree_scalar_str(&sb, "module_render_error", "out_of_memory");
            }
            free(count_text);
            free(cells);
            if (engine_saturated) {
                cbm_tree_scalar_str(&sb, "hint",
                                    "impact hit the safety ceiling — narrow with a lower "
                                    "'depth' or a smaller diff");
            }
        }
        if (response_truncated) {
            cbm_tree_scalar_bool(&sb, "truncated", true);
        }
        out_str = cbm_sb_finish(&sb);
    } else {
        /* format:"json" = json-stringified tree: same model, structured. */
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root_obj);
        yyjson_mut_obj_add_strcpy(doc, root_obj, "base", base_branch);
        if (merge_base[0]) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "merge_base", merge_base);
        }
        yyjson_mut_obj_add_strcpy(doc, root_obj, "direction", direction);
        if (output_budget_hit) {
            yyjson_mut_obj_add_str(doc, root_obj, "truncation_reason", "output_budget");
            yyjson_mut_obj_add_uint(doc, root_obj, "max_output_bytes", output_budget_bytes);
        }
        if (output_budget_floor_exceeded) {
            yyjson_mut_obj_add_bool(doc, root_obj, "output_budget_floor_exceeded", true);
        }
        if (engine_saturated) {
            yyjson_mut_obj_add_bool(doc, root_obj, "engine_saturated", true);
        }
        if (!detect_snapshot_complete) {
            yyjson_mut_obj_add_bool(doc, root_obj, "snapshot_cursor_unavailable", true);
        }
        yyjson_mut_obj_add_int(doc, root_obj, "changed_total", file_count);
        yyjson_mut_obj_add_int(doc, root_obj, "changed_returned", changed_returned);
        yyjson_mut_obj_add_bool(doc, root_obj, "changed_has_more", changed_has_more);
        if (changed_has_more && changed_returned > 0) {
            yyjson_mut_obj_add_int(doc, root_obj, "changed_next_offset",
                                   changed_start + changed_returned);
            if (detect_snapshot_complete) {
                char cursor[80];
                detect_cursor_encode('c', detect_snapshot, detect_qhash,
                                     changed_start + changed_returned, cursor);
                yyjson_mut_obj_add_strcpy(doc, root_obj, "changed_next_cursor", cursor);
            }
        } else if (changed_has_more) {
            yyjson_mut_obj_add_bool(doc, root_obj,
                                    changed_limit == 0
                                        ? "changed_continuation_requires_positive_limit"
                                        : "changed_continuation_requires_higher_budget",
                                    true);
        }
        yyjson_mut_val *cf = yyjson_mut_arr(doc);
        for (int i = changed_start; i < changed_start + changed_returned; i++) {
            yyjson_mut_arr_add_strcpy(doc, cf, files[i]);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "changed_files", cf);
        yyjson_mut_obj_add_int(doc, root_obj, "seed_symbols", seed_count);
        yyjson_mut_obj_add_int(doc, root_obj, "impacted_total", impact.visited_count);
        yyjson_mut_obj_add_str(doc, root_obj, "impacted_total_relation",
                               engine_saturated ? "gte" : "eq");
        yyjson_mut_obj_add_int(doc, root_obj, "impacted_shown", imp_returned);
        yyjson_mut_val *imp = yyjson_mut_arr(doc);
        for (int i = imp_start; i < imp_start + imp_returned; i++) {
            yyjson_mut_val *o = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(
                doc, o, "qn",
                impact.visited[i].node.qualified_name ? impact.visited[i].node.qualified_name : "");
            yyjson_mut_obj_add_strcpy(
                doc, o, "label", impact.visited[i].node.label ? impact.visited[i].node.label : "");
            yyjson_mut_obj_add_strcpy(
                doc, o, "file",
                impact.visited[i].node.file_path ? impact.visited[i].node.file_path : "");
            yyjson_mut_obj_add_int(doc, o, "hop", impact.visited[i].hop);
            yyjson_mut_arr_add_val(imp, o);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "impacted", imp);
        yyjson_mut_obj_add_bool(doc, root_obj, "impacted_has_more", impacted_has_more);
        if (impacted_has_more && imp_returned > 0) {
            yyjson_mut_obj_add_int(doc, root_obj, "impacted_next_offset", imp_start + imp_returned);
            if (detect_snapshot_complete) {
                char cursor[80];
                detect_cursor_encode('i', detect_snapshot, detect_qhash, imp_start + imp_returned,
                                     cursor);
                yyjson_mut_obj_add_strcpy(doc, root_obj, "impacted_next_cursor", cursor);
            }
        } else if (impacted_has_more) {
            yyjson_mut_obj_add_bool(doc, root_obj, "impacted_continuation_requires_higher_budget",
                                    true);
        }
        /* Model parity with the tree encoding: complete totals, paged rows. */
        if (want_symbols) {
            yyjson_mut_obj_add_int(doc, root_obj, "module_total", module_total);
            yyjson_mut_obj_add_str(doc, root_obj, "module_total_relation",
                                   engine_saturated ? "gte" : "eq");
            yyjson_mut_obj_add_int(doc, root_obj, "module_returned", module_returned);
            yyjson_mut_obj_add_bool(doc, root_obj, "module_has_more", module_has_more);
            if (module_has_more && module_returned > 0) {
                yyjson_mut_obj_add_int(doc, root_obj, "module_next_offset",
                                       module_start + module_returned);
                if (detect_snapshot_complete) {
                    char cursor[80];
                    detect_cursor_encode('m', detect_snapshot, detect_qhash,
                                         module_start + module_returned, cursor);
                    yyjson_mut_obj_add_strcpy(doc, root_obj, "module_next_cursor", cursor);
                }
            } else if (module_has_more) {
                yyjson_mut_obj_add_bool(doc, root_obj,
                                        module_limit == 0
                                            ? "module_continuation_requires_positive_limit"
                                            : "module_continuation_requires_higher_budget",
                                        true);
            }
            yyjson_mut_val *rollup = yyjson_mut_arr(doc);
            for (int j = module_start; j < module_start + module_returned; j++) {
                yyjson_mut_val *o = yyjson_mut_obj(doc);
                if (j < nmods) {
                    yyjson_mut_obj_add_strcpy(doc, o, "module", modules[j].name);
                    yyjson_mut_obj_add_int(doc, o, "count", modules[j].count);
                } else {
                    yyjson_mut_obj_add_str(doc, o, "module", "(other)");
                    yyjson_mut_obj_add_int(doc, o, "count", module_overflow);
                }
                yyjson_mut_arr_add_val(rollup, o);
            }
            yyjson_mut_obj_add_val(doc, root_obj, "impacted_modules", rollup);
        }
        yyjson_mut_obj_add_bool(doc, root_obj, "truncated", response_truncated);
        out_str = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    /* Exact serialized-size check with semantic reductions only. Low-value
     * rollups and file names yield as whole sections before graph rows. The
     * continuations above remain valid even when a requested page is omitted
     * by this soft budget. */
    if (out_str && strlen(out_str) > output_budget_bytes) {
        output_budget_hit = true;
        if (module_returned > 0) {
            module_returned = 0;
        } else if (changed_returned > 0) {
            changed_returned = 0;
        } else if (imp_returned > 0) {
            /* Prefix-directory activation can make N compact rows smaller
             * than N-1 direct rows. Probe every smaller whole-row prefix so
             * the response retains the largest page that actually fits. */
            imp_returned--;
        } else if (!output_budget_floor_exceeded) {
            output_budget_floor_exceeded = true;
        } else {
            free(out_str);
            out_str = NULL;
            if (!legacy_json) {
                cbm_sb_t floor;
                cbm_sb_init(&floor);
                cbm_tree_scalar_str(&floor, "truncation_reason", "output_budget");
                cbm_tree_scalar_bool(&floor, "truncated", true);
                cbm_tree_scalar_bool(&floor, "output_budget_floor_exceeded", true);
                if (engine_saturated) {
                    cbm_tree_scalar_bool(&floor, "engine_saturated", true);
                }
                cbm_tree_scalar_int(&floor, "max_output_bytes", (long long)output_budget_bytes);
                cbm_tree_scalar_str(&floor, "hint",
                                    "mandatory detect_changes metadata exceeds the budget; raise "
                                    "max_output_tokens (no path or identifier was sliced)");
                out_str = cbm_sb_finish(&floor);
            } else {
                yyjson_mut_doc *floor_doc = yyjson_mut_doc_new(NULL);
                yyjson_mut_val *floor = yyjson_mut_obj(floor_doc);
                yyjson_mut_doc_set_root(floor_doc, floor);
                yyjson_mut_obj_add_str(floor_doc, floor, "truncation_reason", "output_budget");
                yyjson_mut_obj_add_bool(floor_doc, floor, "truncated", true);
                yyjson_mut_obj_add_bool(floor_doc, floor, "output_budget_floor_exceeded", true);
                if (engine_saturated) {
                    yyjson_mut_obj_add_bool(floor_doc, floor, "engine_saturated", true);
                }
                yyjson_mut_obj_add_uint(floor_doc, floor, "max_output_bytes", output_budget_bytes);
                yyjson_mut_obj_add_str(
                    floor_doc, floor, "hint",
                    "mandatory detect_changes metadata exceeds the budget; raise "
                    "max_output_tokens (no path or identifier was sliced)");
                out_str = yy_doc_to_str(floor_doc);
                yyjson_mut_doc_free(floor_doc);
            }
            goto detect_output_done;
        }
        free(out_str);
        out_str = NULL;
        goto render_detect_output;
    }

detect_output_done:

    cbm_store_traverse_free(&impact);
    detect_module_rollup_free(modules, nmods);
    for (int i = 0; i < file_count; i++) {
        free(files[i]);
    }
    free(files);
    free(seeds);
    free(hunks);
    free(impact_cursor_arg);
    free(changed_cursor_arg);
    free(module_cursor_arg);
    free(direction);
    free(root_path);
    free(project);
    free(base_branch);
    free(scope);

    char *result = cbm_mcp_text_result(out_str, false);
    free(out_str);
    return result;
}

/* ── manage_adr ───────────────────────────────────────────────── */

typedef struct {
    yyjson_mut_doc *doc;
    yyjson_mut_val *arr;
} adr_sections_ctx_t;

static void adr_sections_cb(void *ctx, const cbm_adr_heading_t *h) {
    adr_sections_ctx_t *c = (adr_sections_ctx_t *)ctx;
    char hdr[CBM_SZ_1K];
    snprintf(hdr, sizeof(hdr), "## %.*s", h->name_len, h->name);
    yyjson_mut_arr_add_strcpy(c->doc, c->arr, hdr);
}

/* ADR "sections" mode: list the section headings of the ADR.
 *
 * This uses cbm_adr_scan_headings(), the SAME classifier the section-write
 * path splices with. It used to list any '#'-prefixed line, so a '## Foo' in
 * prose — or inside a fenced code block — was reported as a section that no
 * write could target. Two components disagreeing about what a section is was
 * how a section write came to be able to destroy one. */
static void adr_list_sections_from_content(yyjson_mut_doc *doc, yyjson_mut_val *root_obj,
                                           const char *content) {
    yyjson_mut_val *sections = yyjson_mut_arr(doc);
    adr_sections_ctx_t ctx = {doc, sections};
    if (content && cbm_adr_scan_headings(content, adr_sections_cb, &ctx) != CBM_STORE_OK) {
        /* The ambiguity that refuses a section write is reported here too,
         * rather than answering with a heading list that is quietly partial. */
        yyjson_mut_obj_add_str(doc, root_obj, "sections_status", "unterminated_code_fence");
    }
    yyjson_mut_obj_add_val(doc, root_obj, "sections", sections);
}

static int adr_line_count(const char *content) {
    if (!content || !content[0]) {
        return 0;
    }
    int lines = 1;
    for (const char *p = content; *p; p++) {
        if (*p == '\n' && p[1]) {
            lines++;
        }
    }
    return lines;
}

typedef struct {
    const char *text;
    size_t text_len;
    int level;
    int start_line;
    int end_line;
} adr_heading_row_t;

static int adr_collect_headings(const char *content, adr_heading_row_t **out_rows,
                                int *out_total_lines) {
    *out_rows = NULL;
    *out_total_lines = adr_line_count(content);
    if (!content || !content[0]) {
        return 0;
    }
    int capacity = 16;
    int count = 0;
    adr_heading_row_t *rows = malloc((size_t)capacity * sizeof(*rows));
    if (!rows) {
        return 0;
    }
    const char *p = content;
    int line = 1;
    while (*p) {
        const char *eol = strchr(p, '\n');
        const char *line_end = eol ? eol : p + strlen(p);
        const char *trimmed_end = line_end;
        if (trimmed_end > p && trimmed_end[-1] == '\r') {
            trimmed_end--;
        }
        int level = 0;
        while (p + level < trimmed_end && p[level] == '#') {
            level++;
        }
        if (level > 0 && level <= 6 && p + level < trimmed_end && p[level] == ' ') {
            if (count == capacity) {
                capacity *= 2;
                rows = safe_realloc(rows, (size_t)capacity * sizeof(*rows));
            }
            if (count > 0) {
                rows[count - 1].end_line = line - 1;
            }
            rows[count++] = (adr_heading_row_t){.text = p,
                                                .text_len = (size_t)(trimmed_end - p),
                                                .level = level,
                                                .start_line = line,
                                                .end_line = *out_total_lines};
        }
        if (!eol) {
            break;
        }
        p = eol + 1;
        line++;
    }
    *out_rows = rows;
    return count;
}

static void adr_add_outline(yyjson_mut_doc *doc, yyjson_mut_val *root_obj, const char *content,
                            int offset, int limit) {
    int total_lines = 0;
    adr_heading_row_t *rows = NULL;
    int total = adr_collect_headings(content, &rows, &total_lines);
    if (offset < 0) {
        offset = 0;
    }
    if (offset > total) {
        offset = total;
    }
    int returned = total - offset;
    if (returned > limit) {
        returned = limit;
    }
    yyjson_mut_obj_add_str(doc, root_obj, "mode", "outline");
    yyjson_mut_val *headings = yyjson_mut_arr(doc);
    for (int i = 0; i < returned; i++) {
        const adr_heading_row_t *row = &rows[offset + i];
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, item, "index", offset + i);
        yyjson_mut_obj_add_strncpy(doc, item, "heading", row->text, row->text_len);
        yyjson_mut_obj_add_int(doc, item, "level", row->level);
        yyjson_mut_obj_add_int(doc, item, "start_line", row->start_line);
        yyjson_mut_obj_add_int(doc, item, "end_line", row->end_line);
        yyjson_mut_arr_add_val(headings, item);
    }
    yyjson_mut_obj_add_val(doc, root_obj, "headings", headings);
    yyjson_mut_obj_add_int(doc, root_obj, "total_lines", total_lines);
    yyjson_mut_obj_add_int(doc, root_obj, "sections_total", total);
    yyjson_mut_obj_add_int(doc, root_obj, "sections_returned", returned);
    bool has_more = offset + returned < total;
    yyjson_mut_obj_add_bool(doc, root_obj, "sections_has_more", has_more);
    if (has_more) {
        yyjson_mut_obj_add_int(doc, root_obj, "next_section_offset", offset + returned);
    }
    yyjson_mut_obj_add_bool(doc, root_obj, "full_content_available", content != NULL);
    free(rows);
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
    FILE *fp = cbm_fopen(adr_path, "r");
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

/* resolve_store opens file-backed projects query-only. A mutation must release
 * that reader before opening a dedicated writer because atomic publication uses
 * a self-contained DELETE-mode database that switches back to WAL on write. */
static cbm_store_t *open_adr_store_for_write(cbm_mcp_server_t *srv, cbm_store_t *resolved,
                                             cbm_store_t **owned_rw) {
    if (!srv || !resolved || !owned_rw) {
        return NULL;
    }
    const char *resolved_db_path = cbm_store_db_path(resolved);
    if (!resolved_db_path) {
        return resolved;
    }
    char *rw_path = heap_strdup(resolved_db_path);
    if (!rw_path) {
        return NULL;
    }
    invalidate_cached_store(srv);
    *owned_rw = cbm_store_open_path(rw_path);
    free(rw_path);
    return *owned_rw;
}

/* Parsed `section_updates` for mode='set_sections'.
 *
 * mode='update' replaces the whole document, so adding one entry costs a full
 * re-send and the stored ADR is only ever as good as that round-trip. Writing
 * named sections instead leaves the rest of the document as the authority for
 * itself — and, unlike a whole-document append, applying the same request
 * twice yields the same document, so a client that retries after a lost
 * response cannot silently duplicate content. */
typedef struct {
    char *keys[PROPS_MAX];
    char *values[PROPS_MAX];
    int count;
    /* Rejection reason, or NULL when the request parsed cleanly. Set means no
     * store was opened and nothing was written. */
    const char *status;
    const char *error;
} adr_section_updates_t;

static void adr_section_updates_free(adr_section_updates_t *u) {
    for (int i = 0; i < u->count; i++) {
        free(u->keys[i]);
        free(u->values[i]);
    }
    u->count = 0;
}

static bool adr_collect_section_update(adr_section_updates_t *u, yyjson_val *key, yyjson_val *val) {
    const char *name = yyjson_get_str(key);
    if (!name || !name[0]) {
        u->status = "invalid_section_updates";
        u->error = "'section_updates' keys must be non-empty section names. "
                   "No ADR write was performed.";
        return false;
    }
    if (!yyjson_is_str(val)) {
        u->status = "invalid_section_updates";
        u->error = "'section_updates' values must be strings (the new body for that section). "
                   "No ADR write was performed.";
        return false;
    }
    const char *body = yyjson_get_str(val);
    /* An empty body would render a heading with nothing under it — a silent
     * content deletion wearing the response shape of an update. Clearing a
     * section is whole-document surgery; that is what mode='update' is for. */
    if (!body || !body[0]) {
        u->status = "empty_section_content";
        u->error = "'section_updates' values must be non-empty; use mode='update' to remove a "
                   "section. No ADR write was performed.";
        return false;
    }
    if (u->count >= PROPS_MAX) {
        u->status = "too_many_sections";
        u->error = "'section_updates' carries more entries than an ADR can hold. "
                   "No ADR write was performed.";
        return false;
    }
    u->keys[u->count] = heap_strdup(name);
    u->values[u->count] = heap_strdup(body);
    if (!u->keys[u->count] || !u->values[u->count]) {
        free(u->keys[u->count]);
        free(u->values[u->count]);
        u->status = "write_error";
        u->error = "out of memory parsing 'section_updates'. No ADR write was performed.";
        return false;
    }
    u->count++;
    return true;
}

static adr_section_updates_t adr_parse_section_updates(const char *args) {
    adr_section_updates_t u;
    memset(&u, 0, sizeof(u));

    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *updates =
        (root && yyjson_is_obj(root)) ? yyjson_obj_get(root, "section_updates") : NULL;

    if (!updates) {
        /* Never fall through to 'get': a caller that meant to write must not
         * receive a success-shaped read. */
        u.status = "missing_section_updates";
        u.error = "mode='set_sections' requires 'section_updates', an object mapping section "
                  "name to its new body. No ADR write was performed.";
    } else if (!yyjson_is_obj(updates) || yyjson_obj_size(updates) == 0) {
        u.status = "invalid_section_updates";
        u.error = "'section_updates' must be a non-empty object mapping section name to its new "
                  "body. No ADR write was performed.";
    } else {
        size_t idx = 0;
        size_t max = 0;
        yyjson_val *key = NULL;
        yyjson_val *val = NULL;
        yyjson_obj_foreach(updates, idx, max, key, val) {
            if (!adr_collect_section_update(&u, key, val)) {
                adr_section_updates_free(&u);
                break;
            }
        }
    }
    yyjson_doc_free(doc);
    return u;
}

/* Build the rejection payload for a set_sections request that never reached a
 * store. Caller frees. */
static char *adr_section_updates_error(const adr_section_updates_t *u) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);
    yyjson_mut_obj_add_str(doc, root_obj, "status", u->status);
    yyjson_mut_obj_add_strcpy(doc, root_obj, "error", u->error);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

static char *handle_manage_adr(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *content = cbm_mcp_get_string_arg(args, "content");

    if (!mode_str) {
        mode_str = heap_strdup("outline");
    }

    bool valid_mode = strcmp(mode_str, "outline") == 0 || strcmp(mode_str, "get") == 0 ||
                      strcmp(mode_str, "sections") == 0 || strcmp(mode_str, "update") == 0 ||
                      strcmp(mode_str, "set_sections") == 0 || strcmp(mode_str, "store") == 0;
    if (!valid_mode) {
        free(project);
        free(mode_str);
        free(content);
        return cbm_mcp_text_result(
            "invalid mode: use outline, get, sections, set_sections, or update", true);
    }
    if ((strcmp(mode_str, "update") == 0 || strcmp(mode_str, "store") == 0) && !content) {
        free(project);
        free(mode_str);
        return cbm_mcp_text_result("content is required for update", true);
    }

    /* `sections` used to be advertised as an input argument, but it was never
     * consumed: mode=update still replaced the whole document. Reject the old
     * shape explicitly before opening a store so a stale client cannot mistake
     * a whole-document replacement for a section-scoped update. */
    bool has_sections_arg = false;
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    if (args_doc) {
        yyjson_val *args_root = yyjson_doc_get_root(args_doc);
        has_sections_arg =
            args_root && yyjson_is_obj(args_root) && yyjson_obj_get(args_root, "sections") != NULL;
        yyjson_doc_free(args_doc);
    }
    if (has_sections_arg) {
        free(project);
        free(mode_str);
        free(content);
        return cbm_mcp_text_result(
            "{\"status\":\"invalid_arguments\",\"error\":\"The sections argument is not an "
            "update primitive and has been removed. No ADR write was performed.\"}",
            true);
    }

    bool set_sections_mode = (strcmp(mode_str, "set_sections") == 0);
    adr_section_updates_t updates;
    memset(&updates, 0, sizeof(updates));
    char section_key_err[CBM_SZ_256] = "";
    if (set_sections_mode) {
        updates = adr_parse_section_updates(args);
        /* Any heading name is writable — the canonical six are a convention,
         * not a privilege. What is still refused is a name that could not
         * round-trip through a "## NAME" line (empty, '#'-leading, newline- or
         * edge-whitespace-bearing, over-long): such a name would scan back as
         * a different heading or as none, so a second identical write would
         * append a duplicate instead of being a no-op. */
        if (!updates.status && cbm_adr_validate_section_keys(
                                   (const char **)updates.keys, updates.count, section_key_err,
                                   (int)sizeof(section_key_err)) != CBM_STORE_OK) {
            adr_section_updates_free(&updates);
            updates.status = "invalid_section_name";
            updates.error = section_key_err;
        }
        if (updates.status) {
            /* Reject before taking the project lease or opening a store: a
             * malformed write must not block an index, and must not read. */
            char *err = adr_section_updates_error(&updates);
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            char *res = cbm_mcp_text_result(err, true);
            free(err);
            return res;
        }
    }

    /* This classification is load-bearing. A mode missing from it takes no
     * per-project mutation lease, resolves the store query-only, and never
     * reaches open_adr_store_for_write — so its write would be attempted
     * through a read-only handle, concurrently with an active index. */
    bool write_request =
        (content && (strcmp(mode_str, "update") == 0 || strcmp(mode_str, "store") == 0)) ||
        set_sections_mode;
    bool mutation_held = false;
    if (write_request && project) {
        mutation_held = mcp_project_mutation_begin(srv, project);
        if (!mutation_held) {
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            return cbm_mcp_text_result("project operation cancelled or blocked by an active index",
                                       true);
        }
        if (mcp_request_cancelled(srv)) {
            mcp_project_mutation_end(srv, project);
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            return cbm_mcp_text_result("project operation cancelled for this request", true);
        }
    }

    /* ADRs are stored in the SQLite store (project_summaries), the SAME
     * backend the UI /api/adr endpoints use — so writes via the MCP tool and
     * the UI are visible to each other (#256). */
    store_recovery_status_t recovery_status = STORE_RECOVERY_NONE;
    cbm_store_t *resolved =
        resolve_store_internal(srv, project, mutation_held, !write_request, &recovery_status, true);
    if (!resolved) {
        char *res = NULL;
        if (recovery_status == STORE_RECOVERY_BUSY) {
            res = cbm_mcp_text_result("project is busy; retry after indexing", true);
        } else if (recovery_status == STORE_RECOVERY_TRY_GUARD_UNAVAILABLE) {
            res =
                cbm_mcp_text_result("project recovery requires a nonblocking mutation guard", true);
        } else {
            char *err = build_no_store_error_checked(srv, project);
            res = cbm_mcp_text_result(err, true);
            free(err);
        }
        if (mutation_held) {
            mcp_project_mutation_end(srv, project);
        }
        adr_section_updates_free(&updates);
        free(project);
        free(mode_str);
        free(content);
        return res;
    }

    cbm_store_t *store = resolved;
    cbm_store_t *owned_rw = NULL;
    if (write_request) {
        store = open_adr_store_for_write(srv, resolved, &owned_rw);
        if (!store) {
            if (mutation_held) {
                mcp_project_mutation_end(srv, project);
            }
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            return cbm_mcp_text_result("failed to open writable ADR store", true);
        }
    }

    /* One-time migration: older versions wrote ADRs to a file at
     * <root>/.codebase-memory/adr.md. A read never waits for the project lease:
     * it returns the legacy content immediately and attempts migration only if
     * a nonblocking acquire succeeds. */
    cbm_adr_t adr;
    memset(&adr, 0, sizeof(adr));
    bool have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
    char *legacy = NULL;
    if (!have_adr && !write_request) {
        char *root_path = project_root_from_store(store, project);
        legacy = adr_read_legacy_file(root_path);
        free(root_path);
        if (legacy && mcp_project_mutation_try_begin(srv, project)) {
            if (!mcp_request_cancelled(srv)) {
                /* A publisher may have completed before the lease was granted.
                 * File-backed stores must reopen after acquisition and trust
                 * only that generation. Embedded stores have no publication
                 * boundary, so retain their live handle. */
                if (cbm_store_db_path(resolved)) {
                    invalidate_cached_store(srv);
                    resolved = NULL;
                    store = NULL;
                    resolved = resolve_store_internal(srv, project, true, false, NULL, true);
                }
                if (resolved) {
                    store = open_adr_store_for_write(srv, resolved, &owned_rw);
                    if (store) {
                        have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
                        if (!have_adr &&
                            cbm_store_adr_store(store, project, legacy) == CBM_STORE_OK) {
                            have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
                        }
                    }
                }
            }
            mcp_project_mutation_end(srv, project);
        }
    }

    /* A set_sections write must see a legacy file-backed ADR too. The
     * migration above deliberately runs on the read path only — it must never
     * block on the lease — so the write path reads the file here, where the
     * exclusive project lease and a writable store are already held. Merging
     * onto an empty document instead would silently discard an ADR the user
     * still has on disk. */
    char *legacy_seed = NULL;
    if (set_sections_mode && !have_adr) {
        char *root_path = project_root_from_store(store, project);
        legacy_seed = adr_read_legacy_file(root_path);
        free(root_path);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    bool is_error = false;
    const char *adr_content = have_adr ? adr.content : legacy;
    if (set_sections_mode) {
        /* cbm_store_adr_update_sections requires an existing row — its
         * contract, pinned by TEST(adr_update_no_existing). Seed one when the
         * project has none, so the mode degrades to a plain create: the legacy
         * document when there is one, an empty document otherwise. */
        bool base_present = have_adr;
        bool seeded_empty = false;
        if (!base_present) {
            const char *seed = legacy_seed ? legacy_seed : "";
            if (cbm_store_adr_store(store, project, seed) == CBM_STORE_OK) {
                base_present = true;
                seeded_empty = (legacy_seed == NULL);
            }
        }
        cbm_adr_t updated;
        memset(&updated, 0, sizeof(updated));
        int section_rc = base_present ? cbm_store_adr_update_sections(
                                            store, project, (const char **)updates.keys,
                                            (const char **)updates.values, updates.count, &updated)
                                      : CBM_STORE_ERR;
        if (section_rc == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "sections_updated");
            yyjson_mut_obj_add_str(doc, root_obj, "semantics",
                                   "named_sections_replaced_rest_preserved");
            yyjson_mut_obj_add_uint(doc, root_obj, "sections_written", (uint64_t)updates.count);
            /* Callers confirm the write landed without re-fetching the ADR. */
            yyjson_mut_obj_add_uint(doc, root_obj, "content_length",
                                    (uint64_t)strlen(updated.content));
            cbm_store_adr_free(&updated);
        } else {
            /* Undo an empty seed. A rejected write must not leave the project
             * holding a blank ADR where `get` used to answer no_adr. A legacy
             * seed is a real migration and is kept. */
            if (seeded_empty) {
                (void)cbm_store_adr_delete(store, project);
            }
            yyjson_mut_obj_add_str(doc, root_obj, "status", "write_error");
            const char *store_err = cbm_store_error(store);
            if (store_err && store_err[0]) {
                yyjson_mut_obj_add_strcpy(doc, root_obj, "error", store_err);
            }
            is_error = true;
        }
    } else if (write_request) {
        if (cbm_store_adr_store(store, project, content) == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "updated");
            yyjson_mut_obj_add_str(doc, root_obj, "semantics", "whole_document_replaced");
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "write_error");
            is_error = true;
        }
    } else if (strcmp(mode_str, "sections") == 0) {
        adr_list_sections_from_content(doc, root_obj, adr_content);
    } else if (strcmp(mode_str, "outline") == 0) {
        int section_limit = cbm_mcp_get_int_arg(args, "section_limit", 50);
        int section_offset = cbm_mcp_get_int_arg(args, "section_offset", 0);
        if (section_limit < 1) {
            section_limit = 1;
        } else if (section_limit > 500) {
            section_limit = 500;
        }
        adr_add_outline(doc, root_obj, adr_content, section_offset, section_limit);
    } else { /* get */
        if (adr_content) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "content", adr_content);
            yyjson_mut_obj_add_int(doc, root_obj, "total_lines", adr_line_count(adr_content));
            yyjson_mut_obj_add_bool(doc, root_obj, "content_complete", true);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "content", "");
            yyjson_mut_obj_add_str(doc, root_obj, "status", "no_adr");
            yyjson_mut_obj_add_str(doc, root_obj, "adr_hint", ADR_EMPTY_HINT);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    bool legacy_json_result = write_request || strcmp(mode_str, "sections") == 0;
    if (have_adr) {
        cbm_store_adr_free(&adr);
    }
    if (owned_rw) {
        cbm_store_close(owned_rw);
    }
    if (mutation_held) {
        mcp_project_mutation_end(srv, project);
    }
    adr_section_updates_free(&updates);
    free(legacy_seed);
    free(legacy);
    free(project);
    free(mode_str);
    free(content);

    char *result =
        legacy_json_result ? cbm_mcp_text_result(json, is_error) : mcp_result_from_json(args, json);
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

static char *dispatch_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    if (!tool_name) {
        return cbm_mcp_text_result("missing tool name", true);
    }
    if (srv && !mcp_tool_allowed(srv->tool_profile, tool_name)) {
        char message[CBM_SZ_256];
        snprintf(message, sizeof(message), "tool '%s' is not available in the %s tool profile",
                 tool_name, mcp_tool_profile_name(srv->tool_profile));
        return cbm_mcp_text_result(message, true);
    }

    if (strcmp(tool_name, "list_projects") == 0) {
        return handle_list_projects(srv, args_json);
    }
    if (strcmp(tool_name, "get_graph_schema") == 0) {
        return handle_get_graph_schema(srv, args_json);
    }
    if (strcmp(tool_name, "compare_graphs") == 0) {
        return handle_compare_graphs(srv, args_json);
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
    if (strcmp(tool_name, "check_index_coverage") == 0) {
        return handle_check_index_coverage(srv, args_json);
    }
    if (strcmp(tool_name, "delete_project") == 0) {
        return handle_delete_project(srv, args_json);
    }
    if (strcmp(tool_name, "trace_path") == 0 || strcmp(tool_name, "trace_call_path") == 0) {
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
    if (strcmp(tool_name, "get_file_outline") == 0) {
        return handle_get_file_outline(srv, args_json);
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
    char msg[CBM_SZ_256];
    snprintf(msg, sizeof(msg), "unknown tool: %s", tool_name);
    return cbm_mcp_text_result(msg, true);
}

/* File-backed query stores are request-scoped. Keeping one open between MCP
 * calls pins an old database generation after another process atomically
 * replaces the project DB. On Windows it can also prevent that replacement
 * entirely. Embedded/in-memory stores have no path and retain their existing
 * process lifetime. */
static void release_request_store(cbm_mcp_server_t *srv) {
    if (!srv || !srv->owns_store || !srv->store || !cbm_store_db_path(srv->store)) {
        return;
    }
    cbm_store_close(srv->store);
    srv->store = NULL;
    free(srv->current_project);
    srv->current_project = NULL;
    /* The close above frees a connection's worth of page cache. Ask the
     * allocator to hand those pages back now, which keeps a long-lived daemon
     * flat across thousands of request-scoped stores (#581). This only became
     * meaningful once the Windows interposer made the pages mimalloc's: an
     * earlier attempt aimed at the CRT heap instead and could not release
     * them. POSIX already purges on free, so this is a no-op there. */
    cbm_mem_collect();
}

char *cbm_mcp_handle_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    /* Phase marks bracket the WHOLE request with no unlabelled gap, so growth
     * cannot hide between them (CBM_MEM_PHASES=1; see foundation/mem.h). The
     * "idle" label owns everything outside a request, which is what makes a
     * request-path retainer distinguishable from background growth. */
    cbm_mem_phase_mark("request.scope_begin");
    bool request_scope = !srv || cbm_mcp_server_request_scope_begin(srv);
    if (!request_scope) {
        release_request_store(srv);
        cbm_mem_phase_mark("idle");
        return cbm_mcp_text_result("request cancellation scope unavailable", true);
    }
    cbm_mem_phase_mark("request.dispatch_tool");
    char *result = dispatch_tool(srv, tool_name, args_json);
    cbm_mem_phase_mark("request.scope_end");
    if (srv) {
        cbm_mcp_server_request_scope_end(srv);
    }
    cbm_mem_phase_mark("request.release_store");
    release_request_store(srv);
    cbm_mem_phase_mark("idle");
    /* One census per completed request, so growth can be attributed to a POOL
     * rather than inferred from a process total (#581). Emitted after the
     * request store is released, which is the point where a well-behaved
     * request has given everything back. */
    cbm_mem_census_log("mcp.request");
    return result;
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

/* auto_watch config: gates background watcher registration (default on).
 * Multi-project users can contain a session to its own project with
 * `config set auto_watch false`. */
static bool auto_watch_enabled(cbm_mcp_server_t *srv) {
    if (!srv->config) {
        return true; /* default on */
    }
    return cbm_config_get_bool(srv->config, CBM_CONFIG_AUTO_WATCH, true);
}

/* Register the session project with the background watcher for ongoing
 * change detection — unless auto_watch is disabled. */
static void register_watcher_if_enabled(cbm_mcp_server_t *srv) {
    if (!srv->watcher || srv->session_project[0] == '\0' || srv->session_root[0] == '\0') {
        return;
    }
    if (!auto_watch_enabled(srv)) {
        cbm_log_info("watcher.register.skipped", "reason", "auto_watch_off", "project",
                     srv->session_project);
        return;
    }
    cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
}

/* Background auto-index thread function */
static void *autoindex_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;

    cbm_log_info("autoindex.start", "project", srv->session_project, "path", srv->session_root);

    /* #832: use the supervised worker subprocess. Indexing the whole session in
     * this long-lived server thread ratchets RSS (mimalloc v3 does not reclaim the
     * pages worker threads abandon at exit); running it in a child that exits hands
     * 100% of that memory back to the OS every cycle. In a marked host this is a
     * safety boundary: preparation/start failure stops the operation. */
    if (cbm_index_supervisor_should_wrap()) {
        char *resp = index_run_supervised_path(srv, srv->session_root);
        if (resp) {
            free(resp);
            cbm_log_info("autoindex.done", "project", srv->session_project, "mode", "supervised");
            /* Register with watcher for ongoing change detection — gated on
             * auto_watch (#849), same as the in-process branch below. A bare
             * `if (srv->watcher)` would register even when the user set
             * `config set auto_watch false`, since srv->watcher is always set. */
            register_watcher_if_enabled(srv);
            return NULL;
        }
        cbm_log_error("autoindex.supervision_failed", "project", srv->session_project, "action",
                      "fail_closed");
        return NULL;
    }

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
    cbm_mem_collect(); /* return mimalloc pages to OS after indexing (in-process only) */

    if (rc == 0) {
        cbm_log_info("autoindex.done", "project", srv->session_project);
        register_watcher_if_enabled(srv);
    } else {
        cbm_log_warn("autoindex.err", "msg", "pipeline_run_failed");
    }
    return NULL;
}

bool cbm_mcp_auto_index_within_file_limit(const char *root_path, int file_limit,
                                          int *file_count_out) {
    if (file_count_out) {
        *file_count_out = -1;
    }
    if (!root_path || !root_path[0] || file_limit < 0) {
        return false;
    }
    enum { AUTO_INDEX_COUNT_TIMEOUT_MS = 5000 };
    cbm_discover_opts_t options = {
        .mode = CBM_MODE_FULL,
        .ignore_file = NULL,
        .max_file_size = 0,
    };
    int count = -1;
    cbm_discover_status_t status = cbm_discover_count_bounded(
        root_path, &options, file_limit, cbm_now_ms() + AUTO_INDEX_COUNT_TIMEOUT_MS, &count);
    if (file_count_out) {
        *file_count_out = status == CBM_DISCOVER_LIMIT_EXCEEDED
                              ? (file_limit < INT_MAX ? file_limit + 1 : INT_MAX)
                              : count;
    }
    return status == CBM_DISCOVER_OK;
}

/* Start auto-indexing if configured and project not yet indexed. */
static void maybe_auto_index(cbm_mcp_server_t *srv) {
    if (srv->session_root[0] == '\0') {
        return; /* no session root detected */
    }

    /* Automatic work must honor the same shared workspace boundary as the
     * explicit index_repository worker. Do this before the existing-DB watcher
     * branch and before bounded discovery, so a refused session root begins no
     * automatic observation or indexing. An exact sensitive-root grant remains
     * the shared policy's authenticated override. */
    const char *allowed_root =
        srv->allowed_root_policy_set ? srv->allowed_root : getenv("CBM_ALLOWED_ROOT");
    char boundary_err[CBM_SZ_1K];
    if (!cbm_workspace_root_allowed(srv->session_root, cbm_workspace_home_dir(),
                                    cbm_workspace_cache_dir(), allowed_root, boundary_err,
                                    sizeof(boundary_err))) {
        cbm_log_warn("autoindex.skip", "reason", "workspace_boundary", "detail", boundary_err);
        return;
    }

    /* Check if project already has a DB */
    const char *home = cbm_get_home_dir();
    if (home) {
        char db_check[CBM_SZ_1K];
        snprintf(db_check, sizeof(db_check), "%s/%s.db", cbm_resolve_cache_dir(),
                 srv->session_project);
        if (cbm_file_size(db_check) >= 0) {
            /* Already indexed → register watcher for change detection */
            cbm_log_info("autoindex.skip", "reason", "already_indexed", "project",
                         srv->session_project);
            register_watcher_if_enabled(srv);
            return;
        }
    }

    /* Check auto_index config */
    bool auto_index = false;
    int file_limit = CBM_MCP_DEFAULT_AUTO_INDEX_LIMIT;
    if (srv->config) {
        auto_index = cbm_config_get_bool(srv->config, CBM_CONFIG_AUTO_INDEX, false);
        file_limit = cbm_config_get_int(srv->config, CBM_CONFIG_AUTO_INDEX_LIMIT,
                                        CBM_MCP_DEFAULT_AUTO_INDEX_LIMIT);
    }

    if (!auto_index) {
        cbm_log_info("autoindex.skip", "reason", "disabled", "hint",
                     "run: codebase-memory-mcp config set auto_index true");
        return;
    }

    /* Quick tracked-file count check to avoid OOM on massive repos. */
    int file_count = -1;
#ifdef CBM_ENABLE_TEST_SEAMS
    if (srv->auto_index_count_test_hook) {
        srv->auto_index_count_test_hook(srv->auto_index_count_test_context);
    }
#endif
    if (!cbm_mcp_auto_index_within_file_limit(srv->session_root, file_limit, &file_count)) {
        char files[32];
        char limit[32];
        (void)snprintf(files, sizeof(files), "%d", file_count);
        (void)snprintf(limit, sizeof(limit), "%d", file_limit);
        cbm_log_warn("autoindex.skip", "reason",
                     file_count >= 0 ? "too_many_files" : "unsafe_or_unavailable_path", "files",
                     files, "limit", limit);
        return;
    }

    /* Launch auto-index in background */
    if (cbm_thread_create(&srv->autoindex_tid, 0, autoindex_thread, srv) == 0) {
        srv->autoindex_active = true;
    }
}

/* ── Server request handler ───────────────────────────────────── */

bool cbm_mcp_jsonrpc_response_prepend_notice(char **response_io, const char *notice) {
    if (!response_io || !*response_io || !notice || !notice[0]) {
        return false;
    }
    yyjson_doc *document = yyjson_read(*response_io, strlen(*response_io), 0);
    if (!document) {
        return false;
    }
    yyjson_mut_doc *mutable_document = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root =
        mutable_document ? yyjson_val_mut_copy(mutable_document, yyjson_doc_get_root(document))
                         : NULL;
    yyjson_doc_free(document);
    if (!mutable_document || !root) {
        yyjson_mut_doc_free(mutable_document);
        return false;
    }
    yyjson_mut_doc_set_root(mutable_document, root);
    yyjson_mut_val *result = yyjson_mut_is_obj(root) ? yyjson_mut_obj_get(root, "result") : NULL;
    yyjson_mut_val *content =
        result && yyjson_mut_is_obj(result) ? yyjson_mut_obj_get(result, "content") : NULL;
    yyjson_mut_val *item =
        content && yyjson_mut_is_arr(content) ? yyjson_mut_obj(mutable_document) : NULL;
    bool added = item && yyjson_mut_obj_add_str(mutable_document, item, "type", "text") &&
                 yyjson_mut_obj_add_str(mutable_document, item, "text", notice) &&
                 yyjson_mut_arr_prepend(content, item);
    if (!added) {
        yyjson_mut_doc_free(mutable_document);
        return false;
    }
    /* The response was already normalized at its JSON-RPC boundary; preserve
     * those strings while adding the valid UTF-8 notice. */
    char *replacement = yyjson_mut_write(mutable_document, 0, NULL);
    yyjson_mut_doc_free(mutable_document);
    if (!replacement) {
        return false;
    }
    free(*response_io);
    *response_io = replacement;
    return true;
}

char *cbm_mcp_server_handle(cbm_mcp_server_t *srv, const char *line) {
    cbm_jsonrpc_request_t req = {0};
    if (cbm_jsonrpc_parse(line, &req) < 0) {
        return cbm_jsonrpc_format_error(0, JSONRPC_PARSE_ERROR, "Parse error");
    }

    /* Notifications (no id) → handle cancellation, then no response */
    if (!req.has_id) {
        if (req.method && strcmp(req.method, "notifications/cancelled") == 0) {
            if (cbm_mcp_cancel_request_matches(req.params_raw, srv->active_request_id,
                                               srv->active_request_id_str) &&
                cbm_mcp_server_cancel_active(srv)) {
                cbm_log_info("mcp.cancelled", "match", "true");
            }
        }
        cbm_jsonrpc_request_free(&req);
        return NULL;
    }

    if (!cbm_mcp_server_request_scope_begin(srv)) {
        int64_t request_id = req.id;
        cbm_jsonrpc_request_free(&req);
        return cbm_jsonrpc_format_error(request_id, JSONRPC_INTERNAL_ERROR,
                                        "Request cancellation scope unavailable");
    }

    struct timespec req_t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &req_t0);
    char *result_json = NULL;
    char *request_error_json = NULL;
    bool request_logged = false;

    if (strcmp(req.method, "initialize") == 0) {
        result_json = cbm_mcp_initialize_response_for_profile(req.params_raw, srv->tool_profile);
        detect_session(srv);
        if (srv->background_tasks && srv->tool_profile == CBM_MCP_TOOL_PROFILE_ALL) {
            maybe_auto_index(srv);
        }
    } else if (strcmp(req.method, "ping") == 0) {
        result_json = heap_strdup("{}");
    } else if (strcmp(req.method, "resources/list") == 0) {
        /* This server exposes no resources, but clients probe these on
         * connect regardless of declared capabilities and surface -32601 as
         * a failed connection (#958). Empty lists are interoperable. */
        result_json = heap_strdup("{\"resources\":[]}");
    } else if (strcmp(req.method, "resources/templates/list") == 0) {
        result_json = heap_strdup("{\"resourceTemplates\":[]}");
    } else if (strcmp(req.method, "prompts/list") == 0) {
        result_json = cbm_mcp_prompts_list();
    } else if (strcmp(req.method, "prompts/get") == 0) {
        result_json = cbm_mcp_prompt_get(req.params_raw, &request_error_json);
    } else if (strcmp(req.method, "tools/list") == 0) {
        result_json = cbm_mcp_tools_list_page(srv->tool_profile, req.params_raw);
    } else if (strcmp(req.method, "tools/call") == 0) {
        char *tool_name = req.params_raw ? cbm_mcp_get_tool_name(req.params_raw) : NULL;
        char *tool_args =
            req.params_raw ? cbm_mcp_get_arguments(req.params_raw) : heap_strdup("{}");
        srv->active_request_id = req.id;
        free(srv->active_request_id_str);
        srv->active_request_id_str = req.id_str ? heap_strdup(req.id_str) : NULL;

        struct timespec t0;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
        result_json = cbm_mcp_handle_tool(srv, tool_name, tool_args);
        srv->active_request_id = CBM_NOT_FOUND;
        free(srv->active_request_id_str);
        srv->active_request_id_str = NULL;
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - t0.tv_nsec) / MCP_MS_TO_US);
        bool is_err = (result_json != NULL) && (strstr(result_json, "\"isError\":true") != NULL);
        cbm_diag_record_query(dur_us, is_err);
        long long request_dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                                   ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, tool_name, is_err, request_dur_us);
        request_logged = true;

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
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, true, dur_us);
        cbm_mcp_server_request_scope_end(srv);
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    if (request_error_json) {
        cbm_jsonrpc_response_t err_resp = {
            .id = req.id,
            .id_str = req.id_str,
            .error_json = request_error_json,
        };
        char *err = cbm_jsonrpc_format_response(&err_resp);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, true, dur_us);
        free(request_error_json);
        cbm_mcp_server_request_scope_end(srv);
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    if (!request_logged) {
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, false, dur_us);
    }

    cbm_jsonrpc_response_t resp = {
        .id = req.id,
        .id_str = req.id_str,
        .result_json = result_json,
    };
    char *out = cbm_jsonrpc_format_response(&resp);
    free(result_json);
    cbm_mcp_server_request_scope_end(srv);
    cbm_jsonrpc_request_free(&req);
    return out;
}

/* Read through one newline without ever growing the buffer beyond max_bytes.
 * Returns 1 for a line (including a final unterminated line), 0 for clean EOF,
 * and -1 for I/O, allocation, or size failure. */
static int read_bounded_line(FILE *in, char **line, size_t *cap, size_t max_bytes,
                             size_t *out_len) {
    if (!in || !line || !cap || !out_len || max_bytes == 0) {
        return CBM_NOT_FOUND;
    }

    size_t len = 0;
    for (;;) {
        int ch = fgetc(in);
        if (ch == EOF) {
            if (ferror(in)) {
                return CBM_NOT_FOUND;
            }
            if (len == 0) {
                return 0;
            }
            break;
        }
        if (len >= max_bytes) {
            return CBM_NOT_FOUND;
        }

        size_t needed = len + 2; /* byte plus trailing NUL */
        if (*cap < needed) {
            size_t limit_cap = max_bytes + 1;
            size_t new_cap = *cap ? *cap : (limit_cap < 256 ? limit_cap : 256);
            while (new_cap < needed && new_cap < max_bytes + 1) {
                size_t doubled = new_cap * 2;
                new_cap = doubled > limit_cap ? limit_cap : doubled;
            }
            if (new_cap < needed) {
                return CBM_NOT_FOUND;
            }
            char *grown = realloc(*line, new_cap);
            if (!grown) {
                return CBM_NOT_FOUND;
            }
            /* Zero the tail: every byte of the buffer is then defined in any
             * caller's model (parse_content_length reads up to one byte past
             * the matched prefix), and a future over-read degrades to reading
             * NULs instead of undefined memory. One memset per growth step. */
            memset(grown + len, 0, new_cap - len);
            *line = grown;
            *cap = new_cap;
        }

        (*line)[len++] = (char)ch;
        if (ch == '\n') {
            break;
        }
    }

    (*line)[len] = '\0';
    *out_len = len;
    return SKIP_ONE;
}

static bool parse_content_length(const char *line, size_t *out) {
    if (!line || !out || strncmp(line, "Content-Length:", SLEN("Content-Length:")) != 0) {
        return false;
    }

    const char *cursor = line + MCP_CONTENT_PREFIX;
    while (*cursor == ' ' || *cursor == '\t') {
        cursor++;
    }
    if (!isdigit((unsigned char)*cursor)) {
        return false;
    }

    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(cursor, &end, CBM_DECIMAL_BASE);
    if (errno == ERANGE || end == cursor) {
        return false;
    }
    while (*end == ' ' || *end == '\t') {
        end++;
    }
    if (*end != '\0' || parsed == 0 || parsed > MCP_MAX_MESSAGE_SIZE) {
        return false;
    }
    *out = (size_t)parsed;
    return true;
}

int cbm_mcp_read_message(FILE *in, char **message, bool *content_length_framed) {
    if (!in || !message || !content_length_framed) {
        return CBM_NOT_FOUND;
    }
    *message = NULL;
    *content_length_framed = false;

    char *line = NULL;
    size_t cap = 0;
    for (;;) {
        size_t line_read = 0;
        int line_status = read_bounded_line(in, &line, &cap, MCP_MAX_MESSAGE_SIZE, &line_read);
        if (line_status <= 0) {
            free(line);
            return line_status;
        }
        if (memchr(line, '\0', line_read) != NULL) {
            free(line);
            return CBM_NOT_FOUND;
        }

        size_t len = line_read;
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        if (strncmp(line, "Content-Length:", SLEN("Content-Length:")) != 0) {
            *message = line;
            return SKIP_ONE;
        }

        size_t content_len = 0;
        if (line_read > MCP_MAX_HEADER_SIZE || !parse_content_length(line, &content_len)) {
            free(line);
            return CBM_NOT_FOUND;
        }

        bool found_separator = false;
        size_t header_bytes = line_read;
        for (;;) {
            if (header_bytes >= MCP_MAX_HEADER_SIZE) {
                free(line);
                return CBM_NOT_FOUND;
            }
            size_t header_read = 0;
            int header_status = read_bounded_line(in, &line, &cap,
                                                  MCP_MAX_HEADER_SIZE - header_bytes, &header_read);
            if (header_status <= 0) {
                break;
            }
            if (memchr(line, '\0', header_read) != NULL) {
                free(line);
                return CBM_NOT_FOUND;
            }
            header_bytes += header_read;
            size_t header_len = header_read;
            while (header_len > 0 &&
                   (line[header_len - SKIP_ONE] == '\n' || line[header_len - SKIP_ONE] == '\r')) {
                line[--header_len] = '\0';
            }
            if (header_len == 0) {
                found_separator = true;
                break;
            }
        }
        if (!found_separator) {
            free(line);
            return CBM_NOT_FOUND;
        }
        free(line);

        char *body = malloc(content_len + SKIP_ONE);
        if (!body) {
            return CBM_NOT_FOUND;
        }
        size_t total = 0;
        while (total < content_len) {
            size_t nread = fread(body + total, SKIP_ONE, content_len - total, in);
            if (nread == 0) {
                free(body);
                return CBM_NOT_FOUND;
            }
            total += nread;
        }
        if (memchr(body, '\0', content_len) != NULL) {
            free(body);
            return CBM_NOT_FOUND;
        }
        body[content_len] = '\0';
        *message = body;
        *content_length_framed = true;
        return SKIP_ONE;
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
        /* fcntl failed — fall through to a short blocking poll (see the Phase-3
         * note below on why the interval is bounded, not the full idle timeout) */
        pr = poll(&pfd, SKIP_ONE, MCP_TIMEOUT_MS);
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
        /* Phase 3: blocking poll, bounded to a SHORT interval (not the full idle
         * timeout). macOS poll()/select() do NOT report POLLIN/POLLHUP when a
         * FIFO's last writer closes — only read() returns 0 there (verified). A
         * 60s poll would therefore leave the server blocked up to a full idle
         * timeout after stdin EOF (a client that closes the pipe would appear to
         * hang). Waking every MCP_TIMEOUT_MS lets the Phase-2 read() above detect
         * the EOF within ~1s. Idle-store eviction (threshold STORE_IDLE_TIMEOUT_S)
         * is idempotent, so checking it on each short tick is harmless. */
        pr = poll(&pfd, SKIP_ONE, MCP_TIMEOUT_MS);
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
    int fd = cbm_fileno(in);

#ifdef _WIN32
    /* Ensure stdio is in binary mode to prevent CRLF translation from corrupting
     * Content-Length byte counts and causing fread() to hang. */
    _setmode(cbm_fileno(in), _O_BINARY);
    _setmode(cbm_fileno(out), _O_BINARY);
#endif

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

        char *message = NULL;
        bool content_length_framed = false;
        if (cbm_mcp_read_message(in, &message, &content_length_framed) <= 0) {
            break;
        }

        char *resp = cbm_mcp_server_handle(srv, message);
        free(message);
        if (resp) {
            if (content_length_framed) {
                size_t response_len = strlen(resp);
                (void)fprintf(out, "Content-Length: %zu\r\n\r\n%s", response_len, resp);
            } else {
                (void)fprintf(out, "%s\n", resp);
            }
            (void)fflush(out);
            free(resp);
        }
    }

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
