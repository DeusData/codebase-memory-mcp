/*
 * mcp.c — MCP server: JSON-RPC 2.0 over stdio with graph tools.
 *
 * Uses yyjson for fast JSON parsing/building.
 * Single-threaded event loop: read line → parse → dispatch → respond.
 */

// operations

#include "mcp/mcp.h"
#include "mcp/session.h"
#include "store/store.h"
#include "foundation/hash_table.h"
#include "cypher/cypher.h"
#include "pipeline/pipeline.h"
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
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

/* ── Constants ────────────────────────────────────────────────── */

/* Default snippet fallback line count */
#define SNIPPET_DEFAULT_LINES 50

/* Approximate output budget: 1 token ~= 4 chars. */
#define DEFAULT_MAX_TOKENS 2000
#define MIN_JSON_CHAR_BUDGET 128
#define MAX_FULL_BUDGET_ITEMS 5
#define MAX_COMPACT_QUERY_CELL_CHARS 96

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

static size_t max_tokens_to_char_budget(int max_tokens) {
    if (max_tokens <= 0) {
        max_tokens = DEFAULT_MAX_TOKENS;
    }
    size_t budget = (size_t)max_tokens * 4U;
    if (budget < MIN_JSON_CHAR_BUDGET) {
        budget = MIN_JSON_CHAR_BUDGET;
    }
    return budget;
}

static char *json_string_field_dup(const char *json, const char *key) {
    if (!json || !key) {
        return NULL;
    }

    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return NULL;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value = root ? yyjson_obj_get(root, key) : NULL;
    const char *str = value ? yyjson_get_str(value) : NULL;
    char *dup = str ? heap_strdup(str) : NULL;
    yyjson_doc_free(doc);
    return dup;
}

static char *node_signature_dup(const cbm_node_t *node) {
    if (!node) {
        return NULL;
    }
    return json_string_field_dup(node->properties_json, "signature");
}

static size_t estimate_signature_field_chars(const cbm_node_t *node) {
    char *signature = node_signature_dup(node);
    if (!signature || !signature[0]) {
        free(signature);
        return 0;
    }

    size_t size = strlen(signature) + 24;
    free(signature);
    return size;
}

static char *truncate_text_copy(const char *text, size_t max_chars) {
    if (!text) {
        return heap_strdup("");
    }

    size_t len = strlen(text);
    if (len <= max_chars) {
        return heap_strdup(text);
    }
    if (max_chars <= 3) {
        char *out = malloc(max_chars + 1);
        if (!out) {
            return NULL;
        }
        for (size_t i = 0; i < max_chars; i++) {
            out[i] = '.';
        }
        out[max_chars] = '\0';
        return out;
    }

    char *out = malloc(max_chars + 1);
    if (!out) {
        return NULL;
    }
    size_t keep = max_chars - 3;
    memcpy(out, text, keep);
    memcpy(out + keep, "...", 4);
    return out;
}

static char *build_compact_hop_chain(const cbm_node_hop_t *hops, int count) {
    if (!hops || count <= 0) {
        return NULL;
    }

    const char *first = NULL;
    const char *last = NULL;
    int named_count = 0;
    for (int i = 0; i < count; i++) {
        if (hops[i].node.name && hops[i].node.name[0]) {
            if (!first) {
                first = hops[i].node.name;
            }
            last = hops[i].node.name;
            named_count++;
        }
    }
    if (!first) {
        return NULL;
    }
    if (named_count <= 1 || !last || strcmp(first, last) == 0) {
        return heap_strdup(first);
    }
    if (named_count == 2) {
        size_t len = strlen(first) + strlen(last) + strlen(" -> ") + 1;
        char *chain = malloc(len);
        if (!chain) {
            return NULL;
        }
        snprintf(chain, len, "%s -> %s", first, last);
        return chain;
    }

    int omitted = named_count - 2;
    char omitted_buf[32];
    snprintf(omitted_buf, sizeof(omitted_buf), "%d", omitted);

    size_t len =
        strlen(first) + strlen(last) + strlen(omitted_buf) + strlen(" -> ... ( more) -> ") + 1;
    char *chain = malloc(len);
    if (!chain) {
        return NULL;
    }
    snprintf(chain, len, "%s -> ... (%d more) -> %s", first, omitted, last);
    return chain;
}

static size_t estimate_search_result_chars(const cbm_search_result_t *sr, bool compact) {
    size_t size = 96;
    size += strlen(sr->node.name ? sr->node.name : "");
    size += strlen(sr->node.file_path ? sr->node.file_path : "");
    size += estimate_signature_field_chars(&sr->node);
    if (!compact) {
        size += strlen(sr->node.qualified_name ? sr->node.qualified_name : "");
        size += strlen(sr->node.label ? sr->node.label : "");
        size += 48;
    } else {
        size += 24;
    }
    return size;
}

static void add_search_result_item(yyjson_mut_doc *doc, yyjson_mut_val *results,
                                   const cbm_search_result_t *sr, bool compact) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "name", sr->node.name ? sr->node.name : "");
    yyjson_mut_obj_add_str(doc, item, "file_path", sr->node.file_path ? sr->node.file_path : "");
    yyjson_mut_obj_add_int(doc, item, "start_line", sr->node.start_line);

    char *signature = node_signature_dup(&sr->node);
    if (signature && signature[0]) {
        yyjson_mut_obj_add_strcpy(doc, item, "signature", signature);
    }
    free(signature);

    if (compact) {
        yyjson_mut_obj_add_bool(doc, item, "compact", true);
    } else {
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               sr->node.qualified_name ? sr->node.qualified_name : "");
        yyjson_mut_obj_add_str(doc, item, "label", sr->node.label ? sr->node.label : "");
        yyjson_mut_obj_add_int(doc, item, "end_line", sr->node.end_line);
        yyjson_mut_obj_add_int(doc, item, "in_degree", sr->in_degree);
        yyjson_mut_obj_add_int(doc, item, "out_degree", sr->out_degree);
        yyjson_mut_obj_add_real(doc, item, "pagerank", sr->pagerank);
    }

    yyjson_mut_arr_add_val(results, item);
}

static size_t estimate_node_hop_chars(const cbm_node_hop_t *hop, bool compact) {
    size_t size = 80;
    size += strlen(hop->node.name ? hop->node.name : "");
    size += strlen(hop->node.file_path ? hop->node.file_path : "");
    size += estimate_signature_field_chars(&hop->node);
    if (!compact) {
        size += strlen(hop->node.qualified_name ? hop->node.qualified_name : "");
        size += 40;
    } else {
        size += 20;
    }
    return size;
}

static void add_node_hop_item(yyjson_mut_doc *doc, yyjson_mut_val *items, const cbm_node_hop_t *hop,
                              bool compact) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "name", hop->node.name ? hop->node.name : "");
    yyjson_mut_obj_add_str(doc, item, "file_path", hop->node.file_path ? hop->node.file_path : "");
    yyjson_mut_obj_add_int(doc, item, "start_line", hop->node.start_line);
    yyjson_mut_obj_add_int(doc, item, "hop", hop->hop);

    char *signature = node_signature_dup(&hop->node);
    if (signature && signature[0]) {
        yyjson_mut_obj_add_strcpy(doc, item, "signature", signature);
    }
    free(signature);

    if (compact) {
        yyjson_mut_obj_add_bool(doc, item, "compact", true);
    } else {
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               hop->node.qualified_name ? hop->node.qualified_name : "");
        yyjson_mut_obj_add_real(doc, item, "pagerank", hop->pagerank);
    }

    yyjson_mut_arr_add_val(items, item);
}

static size_t estimate_query_row_chars(const char *const *row, int col_count, bool compact) {
    size_t size = 8;
    for (int c = 0; c < col_count; c++) {
        size += 4;
        if (!row[c]) {
            continue;
        }
        size += compact ? strnlen(row[c], MAX_COMPACT_QUERY_CELL_CHARS) : strlen(row[c]);
    }
    return size;
}

static void add_query_row(yyjson_mut_doc *doc, yyjson_mut_val *rows, const char *const *row,
                          int col_count,
                          bool compact) {
    yyjson_mut_val *out_row = yyjson_mut_arr(doc);
    for (int c = 0; c < col_count; c++) {
        const char *cell = row[c] ? row[c] : "";
        if (compact) {
            char *clipped = truncate_text_copy(cell, MAX_COMPACT_QUERY_CELL_CHARS);
            yyjson_mut_arr_add_strcpy(doc, out_row, clipped ? clipped : "");
            free(clipped);
        } else {
            yyjson_mut_arr_add_str(doc, out_row, cell);
        }
    }
    yyjson_mut_arr_add_val(rows, out_row);
}

static int impact_output_direct_caller_count(const cbm_impact_analysis_t *impact) {
    int direct_callers = 0;
    for (int i = 0; i < impact->direct_count; i++) {
        if (impact->direct[i].type && strcmp(impact->direct[i].type, "route") == 0) {
            continue;
        }
        direct_callers++;
    }
    return direct_callers;
}

static int impact_output_route_entry_count(const cbm_impact_analysis_t *impact) {
    int total = 0;
    const cbm_impact_item_t *groups[] = {impact->direct, impact->indirect, impact->transitive};
    const int counts[] = {impact->direct_count, impact->indirect_count, impact->transitive_count};
    for (int g = 0; g < 3; g++) {
        for (int i = 0; i < counts[g]; i++) {
            const char *type = groups[g][i].type;
            if (type && (strcmp(type, "route") == 0 || strcmp(type, "entry_point") == 0)) {
                total++;
            }
        }
    }
    return total;
}

static int impact_output_total_results(const cbm_impact_analysis_t *impact, bool include_tests) {
    int total = impact->direct_count + impact->indirect_count + impact->transitive_count;
    if (include_tests) {
        total += impact->affected_test_count;
    }
    return total;
}

static char *impact_output_summary_dup(const cbm_impact_analysis_t *impact, bool include_tests) {
    int direct_callers = impact_output_direct_caller_count(impact);
    int route_entries = impact_output_route_entry_count(impact);
    int tests = impact->affected_test_count;
    int transitive = impact->transitive_count;

    char buf[256];
    if (include_tests) {
        if (transitive > 0) {
            snprintf(buf, sizeof(buf),
                     "%d direct callers, %d route/entry points, %d affected tests, %d transitive impacts",
                     direct_callers, route_entries, tests, transitive);
        } else {
            snprintf(buf, sizeof(buf), "%d direct callers, %d route/entry points, %d affected tests",
                     direct_callers, route_entries, tests);
        }
    } else if (transitive > 0) {
        snprintf(buf, sizeof(buf), "%d direct callers, %d route/entry points, %d transitive impacts",
                 direct_callers, route_entries, transitive);
    } else {
        snprintf(buf, sizeof(buf), "%d direct callers, %d route/entry points",
                 direct_callers, route_entries);
    }
    return heap_strdup(buf);
}

static size_t estimate_impact_item_chars(const cbm_impact_item_t *item, bool compact) {
    size_t size = 72;
    size += strlen(item->name ? item->name : "");
    size += strlen(item->file ? item->file : "");
    size += strlen(item->type ? item->type : "");
    size += compact ? 16 : 32;
    return size;
}

static void add_impact_item_json(yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                 const cbm_impact_item_t *item, bool compact) {
    yyjson_mut_val *entry = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, entry, "name", item->name ? item->name : "");
    yyjson_mut_obj_add_str(doc, entry, "file", item->file ? item->file : "");
    yyjson_mut_obj_add_str(doc, entry, "type", item->type ? item->type : "");
    if (compact) {
        yyjson_mut_obj_add_bool(doc, entry, "compact", true);
    } else {
        yyjson_mut_obj_add_real(doc, entry, "pagerank", item->pagerank);
    }
    yyjson_mut_arr_add_val(arr, entry);
}

static size_t estimate_affected_test_chars(const cbm_affected_test_t *item) {
    size_t size = 48;
    size += strlen(item->name ? item->name : "");
    size += strlen(item->file ? item->file : "");
    return size;
}

static void add_affected_test_json(yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                   const cbm_affected_test_t *item) {
    yyjson_mut_val *entry = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, entry, "name", item->name ? item->name : "");
    yyjson_mut_obj_add_str(doc, entry, "file", item->file ? item->file : "");
    yyjson_mut_arr_add_val(arr, entry);
}

static void add_budgeted_impact_group(yyjson_mut_doc *doc, yyjson_mut_val *impact_obj,
                                      const char *group_name, const cbm_impact_item_t *items,
                                      int count, size_t char_budget, size_t *used, int *shown,
                                      int *full_items, bool *stop) {
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    if (!*stop) {
        for (int i = 0; i < count; i++) {
            bool compact = *full_items >= MAX_FULL_BUDGET_ITEMS;
            size_t estimate = estimate_impact_item_chars(&items[i], compact);
            if (*used + estimate > char_budget && !compact) {
                compact = true;
                estimate = estimate_impact_item_chars(&items[i], true);
            }
            if (*used + estimate > char_budget && *shown > 0) {
                *stop = true;
                break;
            }
            if (*used + estimate <= char_budget || *shown == 0) {
                add_impact_item_json(doc, arr, &items[i], compact);
                *used += estimate;
                (*shown)++;
                if (!compact) {
                    (*full_items)++;
                }
            }
        }
    }
    yyjson_mut_obj_add_val(doc, impact_obj, group_name, arr);
}

typedef struct {
    char *buf;
    size_t len;
    size_t cap;
    size_t limit;
    bool truncated;
} markdown_builder_t;

static void markdown_builder_init(markdown_builder_t *b, size_t limit) {
    b->cap = 512;
    b->buf = malloc(b->cap);
    b->len = 0;
    b->limit = limit;
    b->truncated = false;
    if (b->buf) {
        b->buf[0] = '\0';
    }
}

static bool markdown_builder_reserve(markdown_builder_t *b, size_t need) {
    if (!b->buf) {
        return false;
    }
    while (b->len + need + 1 > b->cap) {
        b->cap *= 2;
        b->buf = safe_realloc(b->buf, b->cap);
        if (!b->buf) {
            b->truncated = true;
            return false;
        }
    }
    return true;
}

static bool markdown_builder_append_raw(markdown_builder_t *b, const char *text) {
    if (!b || !b->buf || !text || b->truncated) {
        return false;
    }
    size_t add = strlen(text);
    if (b->len + add > b->limit) {
        b->truncated = true;
        return false;
    }
    if (!markdown_builder_reserve(b, add)) {
        return false;
    }
    memcpy(b->buf + b->len, text, add);
    b->len += add;
    b->buf[b->len] = '\0';
    return true;
}

static bool markdown_builder_appendf(markdown_builder_t *b, const char *fmt, ...) {
    if (!b || !b->buf || !fmt || b->truncated) {
        return false;
    }

    va_list ap;
    va_start(ap, fmt);
    va_list ap_copy;
    va_copy(ap_copy, ap);
    int needed = vsnprintf(NULL, 0, fmt, ap_copy);
    va_end(ap_copy);
    if (needed < 0) {
        va_end(ap);
        return false;
    }
    if (b->len + (size_t)needed > b->limit) {
        b->truncated = true;
        va_end(ap);
        return false;
    }
    if (!markdown_builder_reserve(b, (size_t)needed)) {
        va_end(ap);
        return false;
    }
    vsnprintf(b->buf + b->len, b->cap - b->len, fmt, ap);
    va_end(ap);
    b->len += (size_t)needed;
    return true;
}

static char *markdown_builder_finish(markdown_builder_t *b) {
    const char *note = "\n_Truncated at max_tokens._\n";
    if (!b || !b->buf) {
        return NULL;
    }
    if (b->truncated) {
        size_t note_len = strlen(note);
        if (note_len <= b->limit) {
            size_t keep_len = b->len;
            size_t final_len = 0;
            if (keep_len + note_len > b->limit) {
                keep_len = b->limit - note_len;
            }
            final_len = keep_len + note_len;
            if (final_len > b->len &&
                !markdown_builder_reserve(b, final_len - b->len)) {
                return b->buf;
            }
            b->len = keep_len;
            memcpy(b->buf + b->len, note, note_len);
            b->len = final_len;
            b->buf[b->len] = '\0';
        }
    }
    return b->buf;
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
    if (!text) text = "";
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *content = yyjson_mut_arr(doc);
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "type", "text");
    yyjson_mut_obj_add_str(doc, item, "text", text ? text : "");
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
     "unlimited\"},\"offset\":{\"type\":\"integer\",\"default\":0},\"ranked\":{\"type\":\"boolean\","
     "\"default\":true,\"description\":\"Sort results by PageRank importance when available.\"},"
     "\"max_tokens\":{\"type\":\"integer\",\"default\":2000,\"description\":\"Maximum output "
     "size. Truncates lower-ranked results when needed.\"}},"
     "\"required\":[\"project\"]}"},

    {"query_graph",
     "Execute a Cypher query against the knowledge graph for complex multi-hop patterns, "
     "aggregations, and cross-service analysis.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\"},\"max_rows\":{\"type\":\"integer\","
     "\"description\":"
     "\"Optional row limit. Default: unlimited (100k "
     "ceiling)\"},\"max_tokens\":{\"type\":\"integer\",\"default\":2000,\"description\":"
     "\"Maximum output size. Compacts lower-priority rows when needed.\"}},"
     "\"required\":[\"query\",\"project\"]}"},

    {"trace_call_path",
     "Trace function call paths — who calls a function and what it calls. Use INSTEAD OF grep when "
     "finding callers, dependencies, or impact analysis.",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\"},\"project\":{"
     "\"type\":\"string\"},\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\","
     "\"both\"],\"default\":\"both\"},\"depth\":{\"type\":\"integer\",\"default\":3},\"edge_"
     "types\":{\"type\":\"array\",\"items\":{\"type\":\"string\"}},\"ranked\":{\"type\":\"boolean\","
     "\"default\":true,\"description\":\"Sort callers/callees by PageRank importance.\"},"
     "\"max_tokens\":{\"type\":\"integer\",\"default\":2000,\"description\":\"Maximum output "
     "size. Truncates lower-ranked path results when needed.\"}},"
     "\"required\":[\"function_name\",\"project\"]}"},

    {"get_code_snippet",
     "Read source code for a function/class/symbol. IMPORTANT: First call search_graph to find the "
     "exact qualified_name, then pass it here. This is a read tool, not a search tool. Accepts "
     "full qualified_name (exact match) or short function name (returns suggestions if ambiguous).",
     "{\"type\":\"object\",\"properties\":{\"qualified_name\":{\"type\":\"string\",\"description\":"
     "\"Full qualified_name from search_graph, or short function name\"},\"project\":{"
     "\"type\":\"string\"},\"include_neighbors\":{"
     "\"type\":\"boolean\",\"default\":false}},\"required\":[\"qualified_name\",\"project\"]}"},

    {"get_graph_schema", "Get the schema of the knowledge graph (node labels, edge types)",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"get_architecture",
     "Get high-level architecture overview — packages, services, dependencies, and project "
     "structure at a glance.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"aspects\":{\"type\":"
     "\"array\",\"items\":{\"type\":\"string\"}}},\"required\":[\"project\"]}"},

    {"get_architecture_summary",
     "Generate a structured markdown architecture summary from the existing SQLite graph, with "
     "optional focus filtering and output size control.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\",\"description\":"
     "\"Indexed project name (from list_projects).\"},\"project_path\":{\"type\":\"string\","
     "\"description\":\"Deprecated alias: path to the indexed project.\"},\"max_tokens\":{"
     "\"type\":\"integer\",\"default\":2000,\"description\":\"Maximum output size. Controls "
     "detail level.\"},\"focus\":{\"type\":\"string\",\"description\":\"Optional domain keyword "
     "to zoom into (for example payment or inventory).\"}},\"anyOf\":[{\"required\":["
     "\"project\"]},{\"required\":[\"project_path\"]}]}"},

    {"get_key_symbols",
     "Human-readable ranked symbol list: top functions/classes by PageRank importance. Use this "
     "for fast first-session orientation and central entry-point discovery.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"limit\":{\"type\":"
     "\"integer\",\"default\":20},\"focus\":{\"type\":\"string\",\"description\":\"Optional "
     "keyword to narrow symbols by name, qualified name, or file path.\"}},\"required\":["
     "\"project\"]}"},

    {"get_impact_analysis",
     "Analyze the blast radius of changing a symbol: direct callers, indirect reach, routes, "
     "affected tests, and a low/medium/high risk score.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"symbol\":{\"type\":"
     "\"string\",\"description\":\"Exact function, method, or class name.\"},\"depth\":{"
     "\"type\":\"integer\",\"default\":3},\"include_tests\":{\"type\":\"boolean\",\"default\":true,"
     "\"description\":\"Include affected test files in the output array.\"},\"max_tokens\":{"
     "\"type\":\"integer\",\"default\":2000,\"description\":\"Maximum output size. Controls "
     "detail level.\"}},\"required\":["
     "\"project\",\"symbol\"]}"},

    {"explore",
     "Compound area exploration: matching symbols, small dependency summaries, hotspots, and "
     "entry points in one response.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"area\":{\"type\":"
     "\"string\",\"description\":\"Case-insensitive keyword for the area to explore.\"},"
     "\"max_tokens\":{\"type\":\"integer\",\"default\":2000,\"description\":\"Maximum output "
     "size. Truncates lower-priority sections when needed.\"}},\"required\":[\"project\","
     "\"area\"]}"},

    {"understand",
     "Compound symbol deep-dive: definition, source, callers, callees, and connected symbols in "
     "one response.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"symbol\":{\"type\":"
     "\"string\",\"description\":\"Short symbol name or full qualified_name.\"},\"max_tokens\":{"
     "\"type\":\"integer\",\"default\":2000,\"description\":\"Maximum output size. Truncates "
     "source and lower-priority arrays when needed.\"}},\"required\":[\"project\","
     "\"symbol\"]}"},

    {"prepare_change",
     "Compound pre-change analysis: blast radius, affected tests, risk score, and suggested "
     "review scope.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"symbol\":{\"type\":"
     "\"string\",\"description\":\"Exact function, method, or class name.\"},"
     "\"include_tests\":{\"type\":\"boolean\",\"default\":true,\"description\":\"Include "
     "affected test details in the output.\"},\"max_tokens\":{\"type\":\"integer\","
     "\"default\":2000,\"description\":\"Maximum output size. Truncates lower-priority impact "
     "details when needed.\"}},\"required\":[\"project\",\"symbol\"]}"},

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
     "\"pattern\",\"project\"]}"},

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

    {"get_session_context",
     "Get session memory: files read/edited, symbols queried, areas explored, "
     "and related symbols not yet examined, ranked by PageRank.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\","
     "\"description\":\"Project name (needed for related_untouched computation).\"},"
     "\"include_related\":{\"type\":\"boolean\",\"default\":true,"
     "\"description\":\"Include graph neighbors of touched symbols that have not been "
     "examined yet.\"},\"limit\":{\"type\":\"integer\",\"default\":10,"
     "\"description\":\"Max related_untouched items.\"}},\"required\":[]}"},

    {"get_session_summary",
     "Compact markdown session summary for context recovery after compaction. "
     "Shows files touched, symbols investigated with PageRank, areas explored, "
     "and suggested next steps.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\","
     "\"description\":\"Project name (needed for PageRank enrichment and next-step "
     "suggestions).\"},\"max_tokens\":{\"type\":\"integer\",\"default\":2000,"
     "\"description\":\"Maximum output size.\"}},\"required\":[]}"},
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

static bool cbm_mcp_get_bool_arg_default(const char *args_json, const char *key, bool default_val) {
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

    /* Ephemeral session memory (Phase 7A) — tracks files/symbols/areas touched */
    cbm_session_state_t *session;
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

struct cbm_session_state *cbm_mcp_server_session(cbm_mcp_server_t *srv) {
    return srv ? srv->session : NULL;
}

/* Lazily create session state on first use. */
static cbm_session_state_t *ensure_session(cbm_mcp_server_t *srv) {
    if (!srv->session) {
        srv->session = cbm_session_create();
    }
    return srv->session;
}

/* Forward declarations for Phase 7B session hint builders (defined later in file).
 * Note: build_understand_session_hint is declared later, after connected_symbol_t typedef. */
static char *build_search_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                       const char *project, const cbm_search_output_t *out);
static char *build_impact_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                       const char *project,
                                       const cbm_impact_analysis_t *impact);
static void maybe_add_session_hint(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *hint,
                                   size_t char_budget, size_t *used);

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
    cbm_session_free(srv->session);
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

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        return not_indexed;
    }

    char *label = cbm_mcp_get_string_arg(args, "label");
    char *name_pattern = cbm_mcp_get_string_arg(args, "name_pattern");
    char *qn_pattern = cbm_mcp_get_string_arg(args, "qn_pattern");
    char *file_pattern = cbm_mcp_get_string_arg(args, "file_pattern");
    int limit = cbm_mcp_get_int_arg(args, "limit", 500000);
    int offset = cbm_mcp_get_int_arg(args, "offset", 0);
    int min_degree = cbm_mcp_get_int_arg(args, "min_degree", -1);
    int max_degree = cbm_mcp_get_int_arg(args, "max_degree", -1);
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);
    bool ranked = cbm_mcp_get_bool_arg_default(args, "ranked", true);
    size_t char_budget = max_tokens_to_char_budget(max_tokens);

    cbm_search_params_t params = {
        .project = project,
        .label = label,
        .name_pattern = name_pattern,
        .qn_pattern = qn_pattern,
        .file_pattern = file_pattern,
        .limit = limit,
        .offset = offset,
        .min_degree = min_degree,
        .max_degree = max_degree,
        .sort_by = ranked ? "relevance" : "name",
    };

    cbm_search_output_t out = {0};
    cbm_store_search(store, &params, &out);

    /* Session: compute hint BEFORE tracking (so we can detect "already seen") */
    char *session_hint = build_search_session_hint(srv, store, project, &out);

    /* Session tracking: record top result names as queried symbols (cap 20) */
    {
        cbm_session_state_t *ss = ensure_session(srv);
        cbm_session_bump_query_count(ss);
        int track_limit = out.count < 20 ? out.count : 20;
        for (int i = 0; i < track_limit; i++) {
            if (out.results[i].node.name) {
                cbm_session_track_symbol(ss, out.results[i].node.name);
            }
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_int(doc, root, "total", out.total);

    yyjson_mut_val *results = yyjson_mut_arr(doc);
    for (int i = 0; i < out.count; i++) {
        add_search_result_item(doc, results, &out.results[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "results", results);
    yyjson_mut_obj_add_bool(doc, root, "has_more", out.total > offset + out.count);

    maybe_add_session_hint(doc, root, session_hint, 0, NULL);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    if (json && strlen(json) > char_budget) {
        free(json);

        doc = yyjson_mut_doc_new(NULL);
        root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        yyjson_mut_obj_add_int(doc, root, "total", out.total);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        yyjson_mut_obj_add_int(doc, root, "total_results", out.total);

        results = yyjson_mut_arr(doc);
        size_t used = 96;
        int shown = 0;
        int full_items = 0;
        for (int i = 0; i < out.count; i++) {
            bool compact = full_items >= MAX_FULL_BUDGET_ITEMS;
            size_t estimate = estimate_search_result_chars(&out.results[i], compact);
            if (used + estimate > char_budget && !compact) {
                compact = true;
                estimate = estimate_search_result_chars(&out.results[i], true);
            }
            if (used + estimate > char_budget && shown > 0) {
                break;
            }
            if (used + estimate <= char_budget || shown == 0) {
                add_search_result_item(doc, results, &out.results[i], compact);
                used += estimate;
                shown++;
                if (!compact) {
                    full_items++;
                }
            }
        }
        yyjson_mut_obj_add_val(doc, root, "results", results);
        yyjson_mut_obj_add_int(doc, root, "shown", shown);
        yyjson_mut_obj_add_bool(doc, root, "has_more", out.total > offset + shown);
        maybe_add_session_hint(doc, root, session_hint, char_budget, &used);
        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    cbm_store_search_free(&out);

    free(session_hint);
    free(project);
    free(label);
    free(name_pattern);
    free(qn_pattern);
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
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);
    size_t char_budget = max_tokens_to_char_budget(max_tokens);

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
        add_query_row(doc, rows, result.rows[r], result.col_count, false);
    }
    yyjson_mut_obj_add_val(doc, root, "rows", rows);
    yyjson_mut_obj_add_int(doc, root, "total", result.row_count);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    if (json && strlen(json) > char_budget) {
        free(json);

        doc = yyjson_mut_doc_new(NULL);
        root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        cols = yyjson_mut_arr(doc);
        for (int i = 0; i < result.col_count; i++) {
            yyjson_mut_arr_add_str(doc, cols, result.columns[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "columns", cols);

        rows = yyjson_mut_arr(doc);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        yyjson_mut_obj_add_int(doc, root, "total_results", result.row_count);

        size_t used = 96;
        for (int i = 0; i < result.col_count; i++) {
            used += strlen(result.columns[i] ? result.columns[i] : "") + 4;
        }
        int shown = 0;
        int full_rows = 0;
        for (int r = 0; r < result.row_count; r++) {
            bool compact = full_rows >= MAX_FULL_BUDGET_ITEMS;
            size_t estimate = estimate_query_row_chars(result.rows[r], result.col_count, compact);
            if (used + estimate > char_budget && !compact) {
                compact = true;
                estimate = estimate_query_row_chars(result.rows[r], result.col_count, true);
            }
            if (used + estimate > char_budget && shown > 0) {
                break;
            }
            if (used + estimate <= char_budget || shown == 0) {
                add_query_row(doc, rows, result.rows[r], result.col_count, compact);
                used += estimate;
                shown++;
                if (!compact) {
                    full_rows++;
                }
            }
        }
        yyjson_mut_obj_add_val(doc, root, "rows", rows);
        yyjson_mut_obj_add_int(doc, root, "total", result.row_count);
        yyjson_mut_obj_add_int(doc, root, "shown", shown);
        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

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

static char *handle_get_key_symbols(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *focus = cbm_mcp_get_string_arg(args, "focus");
    int limit = cbm_mcp_get_int_arg(args, "limit", 20);
    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(focus);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(focus);
        return not_indexed;
    }

    cbm_key_symbol_t *symbols = NULL;
    int count = 0;
    if (cbm_store_get_key_symbols(store, project, focus, limit, &symbols, &count) !=
        CBM_STORE_OK) {
        free(project);
        free(focus);
        return cbm_mcp_text_result("failed to load key symbols", true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "project", project ? project : "");
    yyjson_mut_obj_add_int(doc, root, "count", count);
    yyjson_mut_val *results = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "name", symbols[i].name ? symbols[i].name : "");
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               symbols[i].qualified_name ? symbols[i].qualified_name : "");
        yyjson_mut_obj_add_str(doc, item, "label", symbols[i].label ? symbols[i].label : "");
        yyjson_mut_obj_add_str(doc, item, "file_path",
                               symbols[i].file_path ? symbols[i].file_path : "");
        yyjson_mut_obj_add_int(doc, item, "in_degree", symbols[i].in_degree);
        yyjson_mut_obj_add_int(doc, item, "out_degree", symbols[i].out_degree);
        yyjson_mut_obj_add_real(doc, item, "pagerank", symbols[i].pagerank);
        yyjson_mut_arr_add_val(results, item);
    }
    yyjson_mut_obj_add_val(doc, root, "results", results);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    cbm_store_key_symbols_free(symbols, count);
    free(project);
    free(focus);

    {
        char *result = cbm_mcp_text_result(json, false);
        free(json);
        return result;
    }
}

static char *handle_get_impact_analysis(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *symbol = cbm_mcp_get_string_arg(args, "symbol");
    int depth = cbm_mcp_get_int_arg(args, "depth", 3);
    bool include_tests = cbm_mcp_get_bool_arg_default(args, "include_tests", true);
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);
    size_t char_budget = max_tokens_to_char_budget(max_tokens);
    cbm_store_t *store = resolve_store(srv, project);

    if (!symbol) {
        free(project);
        return cbm_mcp_text_result("symbol is required", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(symbol);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(symbol);
        return not_indexed;
    }

    /* Session tracking */
    {
        cbm_session_state_t *ss = ensure_session(srv);
        cbm_session_bump_query_count(ss);
        cbm_session_track_impact(ss, symbol);
    }

    cbm_impact_analysis_t impact = {0};
    int rc = cbm_store_get_impact_analysis(store, project, symbol, depth, &impact);
    if (rc == CBM_STORE_NOT_FOUND) {
        char err[512];
        snprintf(err, sizeof(err),
                 "symbol not found. Use search_graph(name_pattern=\".*%s.*\") first to discover "
                 "the exact symbol name.",
                 symbol);
        free(project);
        free(symbol);
        return cbm_mcp_text_result(err, true);
    }
    if (rc != CBM_STORE_OK) {
        free(project);
        free(symbol);
        cbm_store_impact_analysis_free(&impact);
        return cbm_mcp_text_result("failed to build impact analysis", true);
    }

    char *summary_text = impact_output_summary_dup(&impact, include_tests);
    if (!summary_text) {
        summary_text = heap_strdup(impact.summary ? impact.summary : "");
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "symbol", impact.symbol ? impact.symbol : "");
    yyjson_mut_obj_add_str(doc, root, "qualified_name",
                           impact.qualified_name ? impact.qualified_name : "");
    yyjson_mut_obj_add_str(doc, root, "file", impact.file ? impact.file : "");
    yyjson_mut_obj_add_real(doc, root, "pagerank", impact.pagerank);

    yyjson_mut_val *impact_obj = yyjson_mut_obj(doc);
    yyjson_mut_val *direct = yyjson_mut_arr(doc);
    for (int i = 0; i < impact.direct_count; i++) {
        add_impact_item_json(doc, direct, &impact.direct[i], false);
    }
    yyjson_mut_obj_add_val(doc, impact_obj, "direct", direct);

    yyjson_mut_val *indirect = yyjson_mut_arr(doc);
    for (int i = 0; i < impact.indirect_count; i++) {
        add_impact_item_json(doc, indirect, &impact.indirect[i], false);
    }
    yyjson_mut_obj_add_val(doc, impact_obj, "indirect", indirect);

    yyjson_mut_val *transitive = yyjson_mut_arr(doc);
    for (int i = 0; i < impact.transitive_count; i++) {
        add_impact_item_json(doc, transitive, &impact.transitive[i], false);
    }
    yyjson_mut_obj_add_val(doc, impact_obj, "transitive", transitive);
    yyjson_mut_obj_add_val(doc, root, "impact", impact_obj);

    yyjson_mut_val *tests = yyjson_mut_arr(doc);
    if (include_tests) {
        for (int i = 0; i < impact.affected_test_count; i++) {
            add_affected_test_json(doc, tests, &impact.affected_tests[i]);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "affected_tests", tests);

    yyjson_mut_obj_add_str(doc, root, "risk_score",
                           impact.risk_score ? impact.risk_score : "");
    yyjson_mut_obj_add_str(doc, root, "summary", summary_text ? summary_text : "");

    /* Session hint (Phase 7B) */
    char *impact_hint = build_impact_session_hint(srv, store, project, &impact);
    maybe_add_session_hint(doc, root, impact_hint, 0, NULL);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    if (json && strlen(json) > char_budget) {
        free(json);

        doc = yyjson_mut_doc_new(NULL);
        root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        yyjson_mut_obj_add_str(doc, root, "symbol", impact.symbol ? impact.symbol : "");
        yyjson_mut_obj_add_str(doc, root, "qualified_name",
                               impact.qualified_name ? impact.qualified_name : "");
        yyjson_mut_obj_add_str(doc, root, "file", impact.file ? impact.file : "");
        yyjson_mut_obj_add_real(doc, root, "pagerank", impact.pagerank);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        yyjson_mut_obj_add_int(doc, root, "total_results",
                               impact_output_total_results(&impact, include_tests));

        size_t used = 96;
        used += strlen(impact.symbol ? impact.symbol : "");
        used += strlen(impact.qualified_name ? impact.qualified_name : "");
        used += strlen(impact.file ? impact.file : "");
        used += strlen(impact.risk_score ? impact.risk_score : "");
        used += strlen(summary_text ? summary_text : "");

        yyjson_mut_val *impact_obj2 = yyjson_mut_obj(doc);
        int shown = 0;
        int full_items = 0;
        bool stop = false;

        add_budgeted_impact_group(doc, impact_obj2, "direct", impact.direct, impact.direct_count,
                                  char_budget, &used, &shown, &full_items, &stop);
        add_budgeted_impact_group(doc, impact_obj2, "indirect", impact.indirect,
                                  impact.indirect_count, char_budget, &used, &shown, &full_items,
                                  &stop);
        add_budgeted_impact_group(doc, impact_obj2, "transitive", impact.transitive,
                                  impact.transitive_count, char_budget, &used, &shown,
                                  &full_items, &stop);
        yyjson_mut_obj_add_val(doc, root, "impact", impact_obj2);

        yyjson_mut_val *tests2 = yyjson_mut_arr(doc);
        if (include_tests && !stop) {
            for (int i = 0; i < impact.affected_test_count; i++) {
                size_t estimate = estimate_affected_test_chars(&impact.affected_tests[i]);
                if (used + estimate > char_budget && shown > 0) {
                    break;
                }
                if (used + estimate <= char_budget || shown == 0) {
                    add_affected_test_json(doc, tests2, &impact.affected_tests[i]);
                    used += estimate;
                    shown++;
                }
            }
        }
        yyjson_mut_obj_add_val(doc, root, "affected_tests", tests2);
        yyjson_mut_obj_add_str(doc, root, "risk_score",
                               impact.risk_score ? impact.risk_score : "");
        yyjson_mut_obj_add_str(doc, root, "summary", summary_text ? summary_text : "");
        yyjson_mut_obj_add_int(doc, root, "shown", shown);
        maybe_add_session_hint(doc, root, impact_hint, char_budget, &used);

        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    free(impact_hint);
    free(summary_text);
    cbm_store_impact_analysis_free(&impact);
    free(project);
    free(symbol);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static int node_hop_rank_cmp(const void *lhs, const void *rhs) {
    const cbm_node_hop_t *a = lhs;
    const cbm_node_hop_t *b = rhs;
    if (a->pagerank < b->pagerank) {
        return 1;
    }
    if (a->pagerank > b->pagerank) {
        return -1;
    }
    if (a->hop != b->hop) {
        return a->hop - b->hop;
    }
    if (!a->node.name || !b->node.name) {
        return 0;
    }
    return strcmp(a->node.name, b->node.name);
}

static bool same_project_path(const char *lhs, const char *rhs) {
    if (!lhs || !rhs) {
        return false;
    }

    char lhs_real[4096];
    char rhs_real[4096];
#ifdef _WIN32
    if (_fullpath(lhs_real, lhs, sizeof(lhs_real)) && _fullpath(rhs_real, rhs, sizeof(rhs_real))) {
        return strcmp(lhs_real, rhs_real) == 0;
    }
#else
    if (realpath(lhs, lhs_real) && realpath(rhs, rhs_real)) {
        return strcmp(lhs_real, rhs_real) == 0;
    }
#endif
    return strcmp(lhs, rhs) == 0;
}

static char *handle_get_architecture_summary(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *project_path = cbm_mcp_get_string_arg(args, "project_path");
    char *focus = cbm_mcp_get_string_arg(args, "focus");
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", 2000);
    char *display_path = NULL;

    if (!project && !project_path) {
        free(focus);
        return cbm_mcp_text_result("project is required", true);
    }
    if (max_tokens <= 0) {
        max_tokens = 2000;
    }

    if (!project) {
        project = cbm_project_name_from_path(project_path);
        if (!project) {
            free(project_path);
            free(focus);
            return cbm_mcp_text_result("unable to derive project name from project_path", true);
        }
    }

    cbm_store_t *store = resolve_store(srv, project);
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(project_path);
        free(focus);
        return _res;
    }

    cbm_project_t proj_info = {0};
    if (cbm_store_get_project(store, project, &proj_info) != CBM_STORE_OK) {
        cbm_project_free_fields(&proj_info);
        free(project);
        free(project_path);
        free(focus);
        return cbm_mcp_text_result("project is not indexed", true);
    }
    if (project_path && proj_info.root_path && proj_info.root_path[0] &&
        !same_project_path(project_path, proj_info.root_path)) {
        cbm_project_free_fields(&proj_info);
        free(project);
        free(project_path);
        free(focus);
        return cbm_mcp_text_result("project_path does not match project", true);
    }
    if (proj_info.root_path && proj_info.root_path[0]) {
        display_path = heap_strdup(proj_info.root_path);
    } else if (project_path && project_path[0]) {
        display_path = heap_strdup(project_path);
    }
    cbm_project_free_fields(&proj_info);

    cbm_architecture_summary_t summary = {0};
    if (cbm_store_get_architecture_summary(store, project, focus, &summary) != CBM_STORE_OK) {
        free(project);
        free(project_path);
        free(display_path);
        free(focus);
        return cbm_mcp_text_result("failed to build architecture summary", true);
    }

    size_t char_budget = (size_t)max_tokens * 4U;
    if (char_budget < 512) {
        char_budget = 512;
    }
    markdown_builder_t md;
    markdown_builder_init(&md, char_budget);

    const char *display_name = display_path ? cbm_path_base(display_path) : project;
    (void)markdown_builder_appendf(&md, "## Project: %s\n", display_name ? display_name : project);
    if (focus && focus[0]) {
        (void)markdown_builder_appendf(&md, "Focus: %s\n", focus);
    }
    (void)markdown_builder_appendf(&md, "Files: %d | Functions: %d | Classes: %d | Routes: %d\n\n",
                                   summary.total_files, summary.total_functions,
                                   summary.total_classes, summary.total_routes);

    (void)markdown_builder_append_raw(&md, "## Key Files (by connectivity)\n");
    if (summary.file_count == 0) {
        (void)markdown_builder_append_raw(&md, "No matching files.\n\n");
    } else {
        for (int i = 0; i < summary.file_count; i++) {
            if (!markdown_builder_appendf(&md, "%d. %s - %d inbound calls, %d outbound\n", i + 1,
                                          summary.files[i].file ? summary.files[i].file : "",
                                          summary.files[i].inbound_calls,
                                          summary.files[i].outbound_calls)) {
                break;
            }
            if (summary.files[i].symbol_count > 0) {
                (void)markdown_builder_append_raw(&md, "   Key methods: ");
                for (int j = 0; j < summary.files[i].symbol_count; j++) {
                    if (j > 0 && !markdown_builder_append_raw(&md, ", ")) {
                        break;
                    }
                    if (summary.files[i].symbols[j].span_lines > 0) {
                        (void)markdown_builder_appendf(
                            &md, "%s (%d lines)",
                            summary.files[i].symbols[j].name ? summary.files[i].symbols[j].name : "",
                            summary.files[i].symbols[j].span_lines);
                    } else {
                        (void)markdown_builder_appendf(
                            &md, "%s",
                            summary.files[i].symbols[j].name ? summary.files[i].symbols[j].name : "");
                    }
                }
                (void)markdown_builder_append_raw(&md, "\n");
            }
        }
        (void)markdown_builder_append_raw(&md, "\n");
    }

    (void)markdown_builder_append_raw(&md, "## Route Map\n");
    if (summary.route_count == 0) {
        (void)markdown_builder_append_raw(&md, "No matching routes.\n\n");
    } else {
        for (int i = 0; i < summary.route_count; i++) {
            (void)markdown_builder_appendf(
                &md, "%s %s", summary.routes[i].method ? summary.routes[i].method : "",
                summary.routes[i].path ? summary.routes[i].path : "");
            if (summary.routes[i].handler && summary.routes[i].handler[0]) {
                (void)markdown_builder_appendf(&md, " -> %s", summary.routes[i].handler);
            }
            if (summary.routes[i].service && summary.routes[i].service[0]) {
                (void)markdown_builder_appendf(&md, " -> %s", summary.routes[i].service);
            }
            if (summary.routes[i].next && summary.routes[i].next[0]) {
                (void)markdown_builder_appendf(&md, " -> %s", summary.routes[i].next);
            }
            (void)markdown_builder_append_raw(&md, "\n");
        }
        (void)markdown_builder_append_raw(&md, "\n");
    }

    (void)markdown_builder_append_raw(&md, "## Module Clusters (Louvain communities)\n");
    if (summary.cluster_count == 0) {
        (void)markdown_builder_append_raw(&md, "No multi-file clusters found.\n\n");
    } else {
        for (int i = 0; i < summary.cluster_count; i++) {
            (void)markdown_builder_appendf(&md, "Cluster %d (%d files)\n", summary.clusters[i].id,
                                           summary.clusters[i].file_count);
            if (summary.clusters[i].core_file_count > 0) {
                (void)markdown_builder_append_raw(&md, "Core: ");
                for (int j = 0; j < summary.clusters[i].core_file_count; j++) {
                    if (j > 0) {
                        (void)markdown_builder_append_raw(&md, ", ");
                    }
                    (void)markdown_builder_appendf(
                        &md, "%s",
                        summary.clusters[i].core_files[j]
                            ? cbm_path_base(summary.clusters[i].core_files[j])
                            : "");
                }
                (void)markdown_builder_append_raw(&md, "\n");
            }
            if (summary.clusters[i].entry_point_count > 0) {
                (void)markdown_builder_append_raw(&md, "Entry: ");
                for (int j = 0; j < summary.clusters[i].entry_point_count; j++) {
                    if (j > 0) {
                        (void)markdown_builder_append_raw(&md, ", ");
                    }
                    (void)markdown_builder_appendf(
                        &md, "%s",
                        summary.clusters[i].entry_points[j]
                            ? summary.clusters[i].entry_points[j]
                            : "");
                }
                (void)markdown_builder_append_raw(&md, "\n");
            }
            (void)markdown_builder_append_raw(&md, "\n");
        }
    }

    (void)markdown_builder_append_raw(&md, "## High-Connectivity Functions (in_degree >= 5)\n");
    if (summary.function_count == 0) {
        (void)markdown_builder_append_raw(&md, "None above threshold.\n\n");
    } else {
        for (int i = 0; i < summary.function_count; i++) {
            (void)markdown_builder_appendf(
                &md, "%s - called by %d functions",
                summary.functions[i].name ? summary.functions[i].name : "",
                summary.functions[i].in_degree);
            if (summary.functions[i].file && summary.functions[i].file[0]) {
                (void)markdown_builder_appendf(&md, " [%s]", summary.functions[i].file);
            }
            (void)markdown_builder_append_raw(&md, "\n");
        }
        (void)markdown_builder_append_raw(&md, "\n");
    }

    (void)markdown_builder_append_raw(&md, "## Entry Points\n");
    if (summary.entry_point_count == 0) {
        (void)markdown_builder_append_raw(&md, "No matching entry points.\n");
    } else {
        for (int i = 0; i < summary.entry_point_count; i++) {
            (void)markdown_builder_appendf(&md, "%s: %d\n",
                                           summary.entry_points[i].kind
                                               ? summary.entry_points[i].kind
                                               : "Other",
                                           summary.entry_points[i].count);
        }
    }

    char *markdown = markdown_builder_finish(&md);
    char *result = cbm_mcp_text_result(markdown ? markdown : "", false);

    free(markdown);
    cbm_store_architecture_summary_free(&summary);
    free(project);
    free(project_path);
    free(display_path);
    free(focus);
    return result;
}

static char *handle_trace_call_path(cbm_mcp_server_t *srv, const char *args) {
    char *func_name = cbm_mcp_get_string_arg(args, "function_name");
    char *project = cbm_mcp_get_string_arg(args, "project");
    cbm_store_t *store = resolve_store(srv, project);
    char *direction = cbm_mcp_get_string_arg(args, "direction");
    int depth = cbm_mcp_get_int_arg(args, "depth", 3);
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);
    bool ranked = cbm_mcp_get_bool_arg_default(args, "ranked", true);
    size_t char_budget = max_tokens_to_char_budget(max_tokens);

    if (!func_name) {
        free(project);
        free(direction);
        return cbm_mcp_text_result("function_name is required", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(func_name);
        free(project);
        free(direction);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(func_name);
        free(project);
        free(direction);
        return not_indexed;
    }

    /* Session tracking */
    {
        cbm_session_state_t *ss = ensure_session(srv);
        cbm_session_bump_query_count(ss);
        cbm_session_track_symbol(ss, func_name);
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
        if (ranked && tr_out.visited_count > 1) {
            qsort(tr_out.visited, (size_t)tr_out.visited_count, sizeof(cbm_node_hop_t),
                  node_hop_rank_cmp);
        }

        yyjson_mut_val *callees = yyjson_mut_arr(doc);
        for (int i = 0; i < tr_out.visited_count; i++) {
            add_node_hop_item(doc, callees, &tr_out.visited[i], false);
        }
        yyjson_mut_obj_add_val(doc, root, "callees", callees);
    }

    if (do_inbound) {
        cbm_store_bfs(store, nodes[0].id, "inbound", edge_types, edge_type_count, depth, 100,
                      &tr_in);
        if (ranked && tr_in.visited_count > 1) {
            qsort(tr_in.visited, (size_t)tr_in.visited_count, sizeof(cbm_node_hop_t),
                  node_hop_rank_cmp);
        }

        yyjson_mut_val *callers = yyjson_mut_arr(doc);
        for (int i = 0; i < tr_in.visited_count; i++) {
            add_node_hop_item(doc, callers, &tr_in.visited[i], false);
        }
        yyjson_mut_obj_add_val(doc, root, "callers", callers);
    }

    /* Serialize BEFORE freeing traversal results (yyjson borrows strings) */
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    if (json && strlen(json) > char_budget) {
        free(json);

        doc = yyjson_mut_doc_new(NULL);
        root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        yyjson_mut_obj_add_str(doc, root, "function", func_name);
        yyjson_mut_obj_add_str(doc, root, "direction", direction);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);

        int total_results = 0;
        if (do_outbound) {
            total_results += tr_out.visited_count;
        }
        if (do_inbound) {
            total_results += tr_in.visited_count;
        }
        yyjson_mut_obj_add_int(doc, root, "total_results", total_results);

        size_t used = 96 + strlen(func_name) + strlen(direction);
        int shown = 0;

        if (do_outbound) {
            yyjson_mut_val *callees = yyjson_mut_arr(doc);
            int shown_callees = 0;
            int full_callees = 0;
            for (int i = 0; i < tr_out.visited_count; i++) {
                bool compact = full_callees >= MAX_FULL_BUDGET_ITEMS;
                size_t estimate = estimate_node_hop_chars(&tr_out.visited[i], compact);
                if (used + estimate > char_budget && !compact) {
                    compact = true;
                    estimate = estimate_node_hop_chars(&tr_out.visited[i], true);
                }
                if (used + estimate > char_budget && shown > 0) {
                    break;
                }
                if (used + estimate <= char_budget || shown == 0) {
                    add_node_hop_item(doc, callees, &tr_out.visited[i], compact);
                    used += estimate;
                    shown++;
                    shown_callees++;
                    if (!compact) {
                        full_callees++;
                    }
                }
            }
            yyjson_mut_obj_add_val(doc, root, "callees", callees);
            if (shown_callees < tr_out.visited_count) {
                char *chain = build_compact_hop_chain(tr_out.visited + shown_callees,
                                                      tr_out.visited_count - shown_callees);
                if (chain && chain[0]) {
                    yyjson_mut_obj_add_strcpy(doc, root, "callees_chain", chain);
                }
                free(chain);
            }
        }

        if (do_inbound) {
            yyjson_mut_val *callers = yyjson_mut_arr(doc);
            int shown_callers = 0;
            int full_callers = 0;
            for (int i = 0; i < tr_in.visited_count; i++) {
                bool compact = full_callers >= MAX_FULL_BUDGET_ITEMS;
                size_t estimate = estimate_node_hop_chars(&tr_in.visited[i], compact);
                if (used + estimate > char_budget && !compact) {
                    compact = true;
                    estimate = estimate_node_hop_chars(&tr_in.visited[i], true);
                }
                if (used + estimate > char_budget && shown > 0) {
                    break;
                }
                if (used + estimate <= char_budget || shown == 0) {
                    add_node_hop_item(doc, callers, &tr_in.visited[i], compact);
                    used += estimate;
                    shown++;
                    shown_callers++;
                    if (!compact) {
                        full_callers++;
                    }
                }
            }
            yyjson_mut_obj_add_val(doc, root, "callers", callers);
            if (shown_callers < tr_in.visited_count) {
                char *chain =
                    build_compact_hop_chain(tr_in.visited + shown_callers,
                                            tr_in.visited_count - shown_callers);
                if (chain && chain[0]) {
                    yyjson_mut_obj_add_strcpy(doc, root, "callers_chain", chain);
                }
                free(chain);
            }
        }

        yyjson_mut_obj_add_int(doc, root, "shown", shown);
        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

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
    cbm_normalize_path_sep(repo_path);

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

    /* Session tracking: bump count on every call */
    cbm_session_bump_query_count(ensure_session(srv));

    /* Tier 1: Exact QN match */
    cbm_node_t node = {0};
    int rc = cbm_store_find_node_by_qn(store, effective_project, qn, &node);
    if (rc == CBM_STORE_OK) {
        if (node.file_path) {
            cbm_session_track_file_read(ensure_session(srv), node.file_path);
        }
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
        if (node.file_path) {
            cbm_session_track_file_read(ensure_session(srv), node.file_path);
        }
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

/* ── compound queries ─────────────────────────────────────────── */

#define EXPLORE_SEARCH_LIMIT 64
#define EXPLORE_DEPENDENCY_LIMIT 5
#define UNDERSTAND_KEY_SYMBOL_LIMIT 20
#define UNDERSTAND_MAX_BFS_RESULTS 64
#define MAX_COMPACT_SOURCE_CHARS 256
#define CONNECTED_REL_CALLER 0x1U
#define CONNECTED_REL_CALLEE 0x2U

typedef struct {
    const cbm_search_result_t *match;
    char **callers;
    int caller_count;
    char **callees;
    int callee_count;
} explore_dependency_t;

typedef struct {
    cbm_node_t node;
    double pagerank;
    unsigned relation_mask;
} connected_symbol_t;

typedef struct {
    char **must_review;
    int must_review_count;
    char **should_review;
    int should_review_count;
    char **tests;
    int test_count;
} review_scope_t;

/* Forward declarations for Phase 7B hint builders that depend on above typedefs. */
static char *build_explore_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                        const char *project, const char *area,
                                        bool was_area_explored,
                                        const cbm_search_result_t *const *matches,
                                        int match_count);
static char *build_understand_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                           const char *project, const char *symbol,
                                           bool was_already_queried,
                                           const connected_symbol_t *connected,
                                           int connected_count);

static bool compound_is_symbol_label(const char *label) {
    return label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0 ||
                     strcmp(label, "Class") == 0);
}

static char *regex_escape_literal(const char *text) {
    static const char *meta = ".^$|()[]{}*+?\\";
    size_t extra = 0;
    for (const char *p = text; p && *p; p++) {
        if (strchr(meta, *p)) {
            extra++;
        }
    }

    size_t len = strlen(text ? text : "");
    char *out = malloc(len + extra + 1);
    if (!out) {
        return NULL;
    }

    char *dst = out;
    for (const char *p = text; p && *p; p++) {
        if (strchr(meta, *p)) {
            *dst++ = '\\';
        }
        *dst++ = *p;
    }
    *dst = '\0';
    return out;
}

static char *build_exact_regex_pattern(const char *text) {
    char *escaped = regex_escape_literal(text ? text : "");
    if (!escaped) {
        return NULL;
    }
    size_t len = strlen(escaped) + 3;
    char *pattern = malloc(len);
    if (!pattern) {
        free(escaped);
        return NULL;
    }
    snprintf(pattern, len, "^%s$", escaped);
    free(escaped);
    return pattern;
}

static char *build_contains_regex_pattern(const char *text) {
    char *escaped = regex_escape_literal(text ? text : "");
    if (!escaped) {
        return NULL;
    }
    size_t len = strlen(escaped) + 5;
    char *pattern = malloc(len);
    if (!pattern) {
        free(escaped);
        return NULL;
    }
    snprintf(pattern, len, ".*%s.*", escaped);
    free(escaped);
    return pattern;
}

static char *build_contains_glob_pattern(const char *text) {
    size_t len = strlen(text ? text : "") + 3;
    char *pattern = malloc(len);
    if (!pattern) {
        return NULL;
    }
    snprintf(pattern, len, "*%s*", text ? text : "");
    return pattern;
}

static bool search_result_matches_area(const cbm_search_result_t *sr, const char *area) {
    if (!sr || !area || !area[0]) {
        return false;
    }
    return (sr->node.name && cbm_strcasestr(sr->node.name, area)) ||
           (sr->node.qualified_name && cbm_strcasestr(sr->node.qualified_name, area)) ||
           (sr->node.file_path && cbm_strcasestr(sr->node.file_path, area));
}

static bool key_symbol_matches_area(const cbm_key_symbol_t *sym, const char *area) {
    if (!sym || !area || !area[0]) {
        return false;
    }
    return (sym->name && cbm_strcasestr(sym->name, area)) ||
           (sym->qualified_name && cbm_strcasestr(sym->qualified_name, area)) ||
           (sym->file_path && cbm_strcasestr(sym->file_path, area));
}

static bool entry_point_matches_area(const cbm_entry_point_t *entry, const char *area) {
    if (!entry || !area || !area[0]) {
        return false;
    }
    return (entry->name && cbm_strcasestr(entry->name, area)) ||
           (entry->qualified_name && cbm_strcasestr(entry->qualified_name, area)) ||
           (entry->file && cbm_strcasestr(entry->file, area));
}

static int search_result_ref_rank_cmp(const void *lhs, const void *rhs) {
    const cbm_search_result_t *a = *(const cbm_search_result_t *const *)lhs;
    const cbm_search_result_t *b = *(const cbm_search_result_t *const *)rhs;
    if (a->pagerank < b->pagerank) {
        return 1;
    }
    if (a->pagerank > b->pagerank) {
        return -1;
    }
    int a_degree = a->in_degree + a->out_degree;
    int b_degree = b->in_degree + b->out_degree;
    if (a_degree != b_degree) {
        return b_degree - a_degree;
    }
    const char *aqn = a->node.qualified_name ? a->node.qualified_name : "";
    const char *bqn = b->node.qualified_name ? b->node.qualified_name : "";
    return strcmp(aqn, bqn);
}

static int understand_exact_match_cmp(const void *lhs, const void *rhs) {
    const cbm_search_result_t *a = *(const cbm_search_result_t *const *)lhs;
    const cbm_search_result_t *b = *(const cbm_search_result_t *const *)rhs;
    bool a_test = cbm_is_test_file_path(a->node.file_path);
    bool b_test = cbm_is_test_file_path(b->node.file_path);
    if (a_test != b_test) {
        return a_test ? 1 : -1;
    }
    if (a->pagerank < b->pagerank) {
        return 1;
    }
    if (a->pagerank > b->pagerank) {
        return -1;
    }
    if (a->in_degree != b->in_degree) {
        return b->in_degree - a->in_degree;
    }
    const char *aqn = a->node.qualified_name ? a->node.qualified_name : "";
    const char *bqn = b->node.qualified_name ? b->node.qualified_name : "";
    return strcmp(aqn, bqn);
}

static int connected_symbol_cmp(const void *lhs, const void *rhs) {
    const connected_symbol_t *a = lhs;
    const connected_symbol_t *b = rhs;
    if (a->pagerank < b->pagerank) {
        return 1;
    }
    if (a->pagerank > b->pagerank) {
        return -1;
    }
    const char *an = a->node.name ? a->node.name : "";
    const char *bn = b->node.name ? b->node.name : "";
    return strcmp(an, bn);
}

static bool append_unique_search_ref(const cbm_search_result_t *sr,
                                     const cbm_search_result_t ***refs, int *count, int *cap) {
    if (!sr || !refs || !count || !cap) {
        return false;
    }
    for (int i = 0; i < *count; i++) {
        if ((*refs)[i]->node.id == sr->node.id) {
            return true;
        }
    }
    if (*count >= *cap) {
        *cap = *cap > 0 ? (*cap * 2) : 16;
        *refs = safe_realloc((void *)*refs, (size_t)*cap * sizeof(**refs));
    }
    (*refs)[(*count)++] = sr;
    return true;
}

static void append_matching_refs(const cbm_search_output_t *out, const char *area,
                                 bool symbol_only, const cbm_search_result_t ***refs,
                                 int *count, int *cap) {
    if (!out || !refs || !count || !cap) {
        return;
    }
    for (int i = 0; i < out->count; i++) {
        if (symbol_only && !compound_is_symbol_label(out->results[i].node.label)) {
            continue;
        }
        if (area && area[0] && !search_result_matches_area(&out->results[i], area)) {
            continue;
        }
        (void)append_unique_search_ref(&out->results[i], refs, count, cap);
    }
}

static char *load_node_source(cbm_mcp_server_t *srv, const cbm_node_t *node) {
    if (!srv || !node || !node->project || !node->file_path) {
        return NULL;
    }

    char *root_path = get_project_root(srv, node->project);
    if (!root_path) {
        return NULL;
    }

    int start = node->start_line > 0 ? node->start_line : 1;
    int end = node->end_line > start ? node->end_line : start + SNIPPET_DEFAULT_LINES;
    size_t path_len = strlen(root_path) + strlen(node->file_path) + 2;
    char *abs_path = malloc(path_len);
    char *source = NULL;
    if (!abs_path) {
        free(root_path);
        return NULL;
    }
    snprintf(abs_path, path_len, "%s/%s", root_path, node->file_path);

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

    free(abs_path);
    free(root_path);
    return source;
}

static void free_name_lists(char **items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i]);
    }
    free(items);
}

static void free_explore_dependencies(explore_dependency_t *deps, int count) {
    if (!deps) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free_name_lists(deps[i].callers, deps[i].caller_count);
        free_name_lists(deps[i].callees, deps[i].callee_count);
    }
    free(deps);
}

static int collect_explore_dependencies(cbm_store_t *store, const cbm_search_result_t **matches,
                                        int match_count, explore_dependency_t **out, int *out_count) {
    *out = NULL;
    *out_count = 0;
    if (!store || !matches || match_count <= 0) {
        return CBM_STORE_OK;
    }

    int dep_count = match_count < EXPLORE_DEPENDENCY_LIMIT ? match_count : EXPLORE_DEPENDENCY_LIMIT;
    explore_dependency_t *deps = calloc((size_t)dep_count, sizeof(*deps));
    if (!deps) {
        return CBM_STORE_ERR;
    }

    for (int i = 0; i < dep_count; i++) {
        deps[i].match = matches[i];
        if (cbm_store_node_neighbor_names(store, matches[i]->node.id, 5, &deps[i].callers,
                                          &deps[i].caller_count, &deps[i].callees,
                                          &deps[i].callee_count) != CBM_STORE_OK) {
            free_explore_dependencies(deps, dep_count);
            return CBM_STORE_ERR;
        }
    }

    *out = deps;
    *out_count = dep_count;
    return CBM_STORE_OK;
}

static int collect_connected_symbols(const cbm_traverse_result_t *callers,
                                     const cbm_traverse_result_t *callees,
                                     connected_symbol_t **out, int *out_count) {
    *out = NULL;
    *out_count = 0;

    int cap = callers->visited_count + callees->visited_count;
    if (cap <= 0) {
        return CBM_STORE_OK;
    }

    connected_symbol_t *items = calloc((size_t)cap, sizeof(*items));
    if (!items) {
        return CBM_STORE_ERR;
    }

    int count = 0;
    const cbm_traverse_result_t *groups[] = {callers, callees};
    const unsigned masks[] = {CONNECTED_REL_CALLER, CONNECTED_REL_CALLEE};
    for (int g = 0; g < 2; g++) {
        for (int i = 0; i < groups[g]->visited_count; i++) {
            const cbm_node_hop_t *hop = &groups[g]->visited[i];
            int found = -1;
            for (int j = 0; j < count; j++) {
                if (items[j].node.id == hop->node.id) {
                    found = j;
                    break;
                }
            }
            if (found >= 0) {
                items[found].relation_mask |= masks[g];
                if (hop->pagerank > items[found].pagerank) {
                    items[found].pagerank = hop->pagerank;
                }
                continue;
            }

            copy_node(&hop->node, &items[count].node);
            items[count].pagerank = hop->pagerank;
            items[count].relation_mask = masks[g];
            count++;
        }
    }

    qsort(items, (size_t)count, sizeof(*items), connected_symbol_cmp);
    *out = items;
    *out_count = count;
    return CBM_STORE_OK;
}

static void free_connected_symbols(connected_symbol_t *items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        cbm_node_free_fields(&items[i].node);
    }
    free(items);
}

static const char *connected_relation_label(unsigned relation_mask) {
    if ((relation_mask & CONNECTED_REL_CALLER) && (relation_mask & CONNECTED_REL_CALLEE)) {
        return "both";
    }
    if (relation_mask & CONNECTED_REL_CALLER) {
        return "caller";
    }
    return "callee";
}

static bool understand_is_key_symbol(cbm_store_t *store, const char *project,
                                     const cbm_search_result_t *selected) {
    cbm_key_symbol_t *symbols = NULL;
    int count = 0;
    bool found = false;
    if (cbm_store_get_key_symbols(store, project, NULL, UNDERSTAND_KEY_SYMBOL_LIMIT, &symbols,
                                  &count) != CBM_STORE_OK) {
        return false;
    }
    for (int i = 0; i < count; i++) {
        if (selected->node.qualified_name && symbols[i].qualified_name &&
            strcmp(selected->node.qualified_name, symbols[i].qualified_name) == 0) {
            found = true;
            break;
        }
    }
    cbm_store_key_symbols_free(symbols, count);
    return found;
}

static int append_unique_string(char ***items, int *count, const char *value) {
    if (!items || !count || !value || !value[0]) {
        return CBM_STORE_OK;
    }
    for (int i = 0; i < *count; i++) {
        if (strcmp((*items)[i], value) == 0) {
            return CBM_STORE_OK;
        }
    }
    char **resized = safe_realloc(*items, (size_t)(*count + 1) * sizeof(**items));
    *items = resized;
    (*items)[*count] = heap_strdup(value);
    if (!(*items)[*count]) {
        return CBM_STORE_ERR;
    }
    (*count)++;
    return CBM_STORE_OK;
}

static void free_string_array(char **items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i]);
    }
    free(items);
}

static void review_scope_free(review_scope_t *scope) {
    if (!scope) {
        return;
    }
    free_string_array(scope->must_review, scope->must_review_count);
    free_string_array(scope->should_review, scope->should_review_count);
    free_string_array(scope->tests, scope->test_count);
    memset(scope, 0, sizeof(*scope));
}

static int build_review_scope(const cbm_impact_analysis_t *impact, bool include_tests,
                              review_scope_t *scope) {
    memset(scope, 0, sizeof(*scope));
    if (append_unique_string(&scope->must_review, &scope->must_review_count, impact->file) !=
        CBM_STORE_OK) {
        review_scope_free(scope);
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < impact->direct_count; i++) {
        if (append_unique_string(&scope->must_review, &scope->must_review_count,
                                 impact->direct[i].file) != CBM_STORE_OK) {
            review_scope_free(scope);
            return CBM_STORE_ERR;
        }
    }
    for (int i = 0; i < impact->indirect_count; i++) {
        if (append_unique_string(&scope->should_review, &scope->should_review_count,
                                 impact->indirect[i].file) != CBM_STORE_OK) {
            review_scope_free(scope);
            return CBM_STORE_ERR;
        }
    }
    for (int i = 0; i < impact->transitive_count; i++) {
        if (append_unique_string(&scope->should_review, &scope->should_review_count,
                                 impact->transitive[i].file) != CBM_STORE_OK) {
            review_scope_free(scope);
            return CBM_STORE_ERR;
        }
    }
    if (include_tests) {
        for (int i = 0; i < impact->affected_test_count; i++) {
            if (append_unique_string(&scope->tests, &scope->test_count,
                                     impact->affected_tests[i].file) != CBM_STORE_OK) {
                review_scope_free(scope);
                return CBM_STORE_ERR;
            }
        }
    }
    return CBM_STORE_OK;
}

static size_t estimate_string_list_entry_chars(const char *value) {
    return strlen(value ? value : "") + 8;
}

static size_t estimate_key_symbol_chars(const cbm_key_symbol_t *sym, bool compact) {
    size_t size = 72;
    size += strlen(sym->name ? sym->name : "");
    size += strlen(sym->file_path ? sym->file_path : "");
    if (!compact) {
        size += strlen(sym->qualified_name ? sym->qualified_name : "");
        size += strlen(sym->label ? sym->label : "");
        size += 32;
    } else {
        size += 16;
    }
    return size;
}

static void add_key_symbol_json(yyjson_mut_doc *doc, yyjson_mut_val *arr, const cbm_key_symbol_t *sym,
                                bool compact) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "name", sym->name ? sym->name : "");
    yyjson_mut_obj_add_str(doc, item, "file_path", sym->file_path ? sym->file_path : "");
    if (compact) {
        yyjson_mut_obj_add_real(doc, item, "pagerank", sym->pagerank);
        yyjson_mut_obj_add_bool(doc, item, "compact", true);
    } else {
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               sym->qualified_name ? sym->qualified_name : "");
        yyjson_mut_obj_add_str(doc, item, "label", sym->label ? sym->label : "");
        yyjson_mut_obj_add_int(doc, item, "in_degree", sym->in_degree);
        yyjson_mut_obj_add_int(doc, item, "out_degree", sym->out_degree);
        yyjson_mut_obj_add_real(doc, item, "pagerank", sym->pagerank);
    }
    yyjson_mut_arr_add_val(arr, item);
}

static size_t estimate_entry_point_chars(const cbm_entry_point_t *entry, bool compact) {
    size_t size = 48;
    size += strlen(entry->name ? entry->name : "");
    size += strlen(entry->file ? entry->file : "");
    if (!compact) {
        size += strlen(entry->qualified_name ? entry->qualified_name : "");
    } else {
        size += 12;
    }
    return size;
}

static void add_entry_point_json(yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                 const cbm_entry_point_t *entry, bool compact) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "name", entry->name ? entry->name : "");
    yyjson_mut_obj_add_str(doc, item, "file_path", entry->file ? entry->file : "");
    if (compact) {
        yyjson_mut_obj_add_bool(doc, item, "compact", true);
    } else {
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               entry->qualified_name ? entry->qualified_name : "");
    }
    yyjson_mut_arr_add_val(arr, item);
}

static size_t estimate_explore_dependency_chars(const explore_dependency_t *dep, bool compact) {
    size_t size = 64;
    size += strlen(dep->match->node.name ? dep->match->node.name : "");
    size += strlen(dep->match->node.file_path ? dep->match->node.file_path : "");
    if (compact) {
        size += 24;
        return size;
    }
    size += strlen(dep->match->node.qualified_name ? dep->match->node.qualified_name : "");
    for (int i = 0; i < dep->caller_count; i++) {
        size += strlen(dep->callers[i]);
    }
    for (int i = 0; i < dep->callee_count; i++) {
        size += strlen(dep->callees[i]);
    }
    return size + 24;
}

static void add_string_array_json(yyjson_mut_doc *doc, yyjson_mut_val *arr, char **values,
                                  int count) {
    for (int i = 0; i < count; i++) {
        yyjson_mut_arr_add_str(doc, arr, values[i] ? values[i] : "");
    }
}

static void add_explore_dependency_json(yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                        const explore_dependency_t *dep, bool compact) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "name",
                           dep->match->node.name ? dep->match->node.name : "");
    yyjson_mut_obj_add_str(doc, item, "file_path",
                           dep->match->node.file_path ? dep->match->node.file_path : "");
    if (compact) {
        yyjson_mut_obj_add_int(doc, item, "caller_count", dep->caller_count);
        yyjson_mut_obj_add_int(doc, item, "callee_count", dep->callee_count);
        yyjson_mut_obj_add_bool(doc, item, "compact", true);
    } else {
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               dep->match->node.qualified_name ? dep->match->node.qualified_name
                                                               : "");
        yyjson_mut_val *callers = yyjson_mut_arr(doc);
        yyjson_mut_val *callees = yyjson_mut_arr(doc);
        add_string_array_json(doc, callers, dep->callers, dep->caller_count);
        add_string_array_json(doc, callees, dep->callees, dep->callee_count);
        yyjson_mut_obj_add_val(doc, item, "callers", callers);
        yyjson_mut_obj_add_val(doc, item, "callees", callees);
    }
    yyjson_mut_arr_add_val(arr, item);
}

static size_t estimate_alternative_chars(const cbm_search_result_t *sr, bool compact) {
    size_t size = 48;
    size += strlen(sr->node.qualified_name ? sr->node.qualified_name : "");
    size += strlen(sr->node.file_path ? sr->node.file_path : "");
    size += compact ? 12 : strlen(sr->node.label ? sr->node.label : "") + 16;
    return size;
}

static void add_alternative_json(yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                 const cbm_search_result_t *sr, bool compact) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "qualified_name",
                           sr->node.qualified_name ? sr->node.qualified_name : "");
    yyjson_mut_obj_add_str(doc, item, "file_path", sr->node.file_path ? sr->node.file_path : "");
    if (compact) {
        yyjson_mut_obj_add_bool(doc, item, "compact", true);
    } else {
        yyjson_mut_obj_add_str(doc, item, "label", sr->node.label ? sr->node.label : "");
        yyjson_mut_obj_add_real(doc, item, "pagerank", sr->pagerank);
    }
    yyjson_mut_arr_add_val(arr, item);
}

static size_t estimate_connected_symbol_chars(const connected_symbol_t *item, bool compact) {
    size_t size = 64;
    size += strlen(item->node.name ? item->node.name : "");
    size += strlen(item->node.file_path ? item->node.file_path : "");
    size += strlen(connected_relation_label(item->relation_mask));
    if (!compact) {
        size += strlen(item->node.qualified_name ? item->node.qualified_name : "");
        size += 16;
    } else {
        size += 8;
    }
    return size;
}

static void add_connected_symbol_json(yyjson_mut_doc *doc, yyjson_mut_val *arr,
                                      const connected_symbol_t *item, bool compact) {
    yyjson_mut_val *entry = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, entry, "name", item->node.name ? item->node.name : "");
    yyjson_mut_obj_add_str(doc, entry, "file_path", item->node.file_path ? item->node.file_path
                                                                         : "");
    yyjson_mut_obj_add_int(doc, entry, "start_line", item->node.start_line);
    yyjson_mut_obj_add_str(doc, entry, "relation", connected_relation_label(item->relation_mask));
    if (compact) {
        yyjson_mut_obj_add_bool(doc, entry, "compact", true);
    } else {
        yyjson_mut_obj_add_str(doc, entry, "qualified_name",
                               item->node.qualified_name ? item->node.qualified_name : "");
        yyjson_mut_obj_add_real(doc, entry, "pagerank", item->pagerank);
    }
    yyjson_mut_arr_add_val(arr, entry);
}

static size_t estimate_understand_definition_chars(const cbm_search_result_t *selected,
                                                   const char *source, bool include_source) {
    size_t size = 96;
    size += strlen(selected->node.file_path ? selected->node.file_path : "");
    size += estimate_signature_field_chars(&selected->node);
    if (include_source && source) {
        size += strlen(source) + 24;
    }
    return size;
}

static void add_understand_definition_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                           const cbm_search_result_t *selected,
                                           const char *source) {
    yyjson_mut_val *definition = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, definition, "file_path",
                           selected->node.file_path ? selected->node.file_path : "");
    yyjson_mut_obj_add_int(doc, definition, "start_line", selected->node.start_line);
    yyjson_mut_obj_add_int(doc, definition, "end_line", selected->node.end_line);

    char *signature = node_signature_dup(&selected->node);
    if (signature && signature[0]) {
        yyjson_mut_obj_add_strcpy(doc, definition, "signature", signature);
    }
    free(signature);

    if (source) {
        yyjson_mut_obj_add_str(doc, definition, "source", source);
    }

    yyjson_mut_obj_add_val(doc, root, "definition", definition);
}

static char *build_symbol_suggestions_response(const char *input, cbm_node_t *nodes, int count) {
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
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, item, "qualified_name",
                               nodes[i].qualified_name ? nodes[i].qualified_name : "");
        yyjson_mut_obj_add_str(doc, item, "name", nodes[i].name ? nodes[i].name : "");
        yyjson_mut_obj_add_str(doc, item, "label", nodes[i].label ? nodes[i].label : "");
        yyjson_mut_obj_add_str(doc, item, "file_path", nodes[i].file_path ? nodes[i].file_path
                                                                           : "");
        yyjson_mut_arr_add_val(arr, item);
    }
    yyjson_mut_obj_add_val(doc, root, "suggestions", arr);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

static int run_store_search(cbm_store_t *store, const cbm_search_params_t *params,
                            cbm_search_output_t *out) {
    memset(out, 0, sizeof(*out));
    return cbm_store_search(store, params, out);
}

static int search_exact_qn(cbm_store_t *store, const char *project, const char *qn,
                           cbm_search_output_t *out) {
    char *pattern = build_exact_regex_pattern(qn);
    if (!pattern) {
        return CBM_STORE_ERR;
    }
    cbm_search_params_t params = {
        .project = project,
        .qn_pattern = pattern,
        .limit = 16,
        .sort_by = "relevance",
        .min_degree = -1,
        .max_degree = -1,
    };
    int rc = run_store_search(store, &params, out);
    free(pattern);
    return rc;
}

static int search_exact_name(cbm_store_t *store, const char *project, const char *name,
                             cbm_search_output_t *out) {
    char *pattern = build_exact_regex_pattern(name);
    if (!pattern) {
        return CBM_STORE_ERR;
    }
    cbm_search_params_t params = {
        .project = project,
        .name_pattern = pattern,
        .limit = 64,
        .sort_by = "relevance",
        .min_degree = -1,
        .max_degree = -1,
    };
    int rc = run_store_search(store, &params, out);
    free(pattern);
    return rc;
}

static int search_contains(cbm_store_t *store, const char *project, const char *name_pattern,
                           const char *qn_pattern, const char *file_pattern,
                           cbm_search_output_t *out) {
    cbm_search_params_t params = {
        .project = project,
        .name_pattern = name_pattern,
        .qn_pattern = qn_pattern,
        .file_pattern = file_pattern,
        .limit = EXPLORE_SEARCH_LIMIT,
        .sort_by = "relevance",
        .min_degree = -1,
        .max_degree = -1,
    };
    return run_store_search(store, &params, out);
}

static char *handle_explore(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *area = cbm_mcp_get_string_arg(args, "area");
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);
    size_t char_budget = max_tokens_to_char_budget(max_tokens);
    cbm_store_t *store = resolve_store(srv, project);

    if (!area || !area[0]) {
        free(project);
        free(area);
        return cbm_mcp_text_result("area is required", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(area);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(area);
        return not_indexed;
    }

    /* Session tracking: check area overlap BEFORE tracking */
    bool was_area_explored = cbm_session_has_area(ensure_session(srv), area);
    {
        cbm_session_state_t *ss = ensure_session(srv);
        cbm_session_bump_query_count(ss);
        cbm_session_track_area(ss, area);
    }

    char *contains_regex = build_contains_regex_pattern(area);
    char *file_glob = build_contains_glob_pattern(area);
    cbm_search_output_t name_out = {0};
    cbm_search_output_t qn_out = {0};
    cbm_search_output_t file_out = {0};
    const cbm_search_result_t **matches = NULL;
    int match_count = 0;
    int match_cap = 0;
    char *explore_hint = NULL;
    explore_dependency_t *deps = NULL;
    int dep_count = 0;
    cbm_key_symbol_t *hotspots = NULL;
    int hotspot_count = 0;
    cbm_architecture_info_t arch = {0};
    cbm_entry_point_t **entry_refs = NULL;
    int entry_count = 0;
    int entry_cap = 0;
    char *result = NULL;

    if (!contains_regex || !file_glob) {
        result = cbm_mcp_text_result("failed to allocate explore filters", true);
        goto cleanup_explore;
    }
    if (search_contains(store, project, contains_regex, NULL, NULL, &name_out) != CBM_STORE_OK ||
        search_contains(store, project, NULL, contains_regex, NULL, &qn_out) != CBM_STORE_OK ||
        search_contains(store, project, NULL, NULL, file_glob, &file_out) != CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to load explore matches", true);
        goto cleanup_explore;
    }

    append_matching_refs(&name_out, area, false, &matches, &match_count, &match_cap);
    append_matching_refs(&qn_out, area, false, &matches, &match_count, &match_cap);
    append_matching_refs(&file_out, area, false, &matches, &match_count, &match_cap);
    if (match_count > 0) {
        qsort(matches, (size_t)match_count, sizeof(*matches), search_result_ref_rank_cmp);
    }

    if (collect_explore_dependencies(store, matches, match_count, &deps, &dep_count) !=
        CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to load explore dependencies", true);
        goto cleanup_explore;
    }

    if (cbm_store_get_key_symbols(store, project, area, 10, &hotspots, &hotspot_count) !=
        CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to load area hotspots", true);
        goto cleanup_explore;
    }

    const char *aspects[] = {"entry_points"};
    if (cbm_store_get_architecture(store, project, aspects, 1, &arch) != CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to load entry points", true);
        goto cleanup_explore;
    }
    for (int i = 0; i < arch.entry_point_count; i++) {
        if (!entry_point_matches_area(&arch.entry_points[i], area)) {
            continue;
        }
        if (entry_count >= entry_cap) {
            entry_cap = entry_cap > 0 ? entry_cap * 2 : 8;
            entry_refs = safe_realloc(entry_refs, (size_t)entry_cap * sizeof(*entry_refs));
        }
        entry_refs[entry_count++] = &arch.entry_points[i];
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "area", area);

    yyjson_mut_val *match_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < match_count; i++) {
        add_search_result_item(doc, match_arr, matches[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "matches", match_arr);

    yyjson_mut_val *dep_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < dep_count; i++) {
        add_explore_dependency_json(doc, dep_arr, &deps[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "dependencies", dep_arr);

    int filtered_hotspot_count = 0;
    for (int i = 0; i < hotspot_count; i++) {
        if (key_symbol_matches_area(&hotspots[i], area)) {
            filtered_hotspot_count++;
        }
    }

    yyjson_mut_val *hotspot_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < hotspot_count; i++) {
        if (!key_symbol_matches_area(&hotspots[i], area)) {
            continue;
        }
        add_key_symbol_json(doc, hotspot_arr, &hotspots[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "hotspots", hotspot_arr);

    yyjson_mut_val *entry_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < entry_count; i++) {
        add_entry_point_json(doc, entry_arr, entry_refs[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "entry_points", entry_arr);

    /* Compute session hint (Phase 7B) */
    explore_hint =
        build_explore_session_hint(srv, store, project, area, was_area_explored, matches, match_count);
    maybe_add_session_hint(doc, root, explore_hint, 0, NULL);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    if (json && strlen(json) > char_budget) {
        free(json);

        doc = yyjson_mut_doc_new(NULL);
        root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);
        yyjson_mut_obj_add_str(doc, root, "area", area);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        yyjson_mut_obj_add_int(doc, root, "total_results",
                               match_count + dep_count + filtered_hotspot_count + entry_count);

        size_t used = 64 + strlen(area);
        int shown = 0;
        int full_items = 0;
        bool stop = false;

        match_arr = yyjson_mut_arr(doc);
        for (int i = 0; i < match_count; i++) {
            bool compact = full_items >= MAX_FULL_BUDGET_ITEMS;
            size_t estimate = estimate_search_result_chars(matches[i], compact);
            if (used + estimate > char_budget && !compact) {
                compact = true;
                estimate = estimate_search_result_chars(matches[i], true);
            }
            if (used + estimate > char_budget && shown > 0) {
                stop = true;
                break;
            }
            add_search_result_item(doc, match_arr, matches[i], compact);
            used += estimate;
            shown++;
            if (!compact) {
                full_items++;
            }
        }
        yyjson_mut_obj_add_val(doc, root, "matches", match_arr);

        dep_arr = yyjson_mut_arr(doc);
        if (!stop) {
            for (int i = 0; i < dep_count; i++) {
                bool compact = true;
                size_t estimate = estimate_explore_dependency_chars(&deps[i], compact);
                if (used + estimate > char_budget && shown > 0) {
                    stop = true;
                    break;
                }
                add_explore_dependency_json(doc, dep_arr, &deps[i], compact);
                used += estimate;
                shown++;
            }
        }
        yyjson_mut_obj_add_val(doc, root, "dependencies", dep_arr);

        hotspot_arr = yyjson_mut_arr(doc);
        if (!stop) {
            for (int i = 0; i < hotspot_count; i++) {
                if (!key_symbol_matches_area(&hotspots[i], area)) {
                    continue;
                }
                bool compact = true;
                size_t estimate = estimate_key_symbol_chars(&hotspots[i], compact);
                if (used + estimate > char_budget && shown > 0) {
                    stop = true;
                    break;
                }
                add_key_symbol_json(doc, hotspot_arr, &hotspots[i], compact);
                used += estimate;
                shown++;
            }
        }
        yyjson_mut_obj_add_val(doc, root, "hotspots", hotspot_arr);

        entry_arr = yyjson_mut_arr(doc);
        if (!stop) {
            for (int i = 0; i < entry_count; i++) {
                bool compact = true;
                size_t estimate = estimate_entry_point_chars(entry_refs[i], compact);
                if (used + estimate > char_budget && shown > 0) {
                    break;
                }
                add_entry_point_json(doc, entry_arr, entry_refs[i], compact);
                used += estimate;
                shown++;
            }
        }
        yyjson_mut_obj_add_val(doc, root, "entry_points", entry_arr);
        yyjson_mut_obj_add_int(doc, root, "shown", shown);
        maybe_add_session_hint(doc, root, explore_hint, char_budget, &used);

        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    result = cbm_mcp_text_result(json, false);
    free(json);

cleanup_explore:
    free(explore_hint);
    free(contains_regex);
    free(file_glob);
    free(matches);
    free(entry_refs);
    free_explore_dependencies(deps, dep_count);
    cbm_store_search_free(&name_out);
    cbm_store_search_free(&qn_out);
    cbm_store_search_free(&file_out);
    cbm_store_key_symbols_free(hotspots, hotspot_count);
    cbm_store_architecture_free(&arch);
    free(project);
    free(area);
    return result;
}

static char *handle_understand(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *symbol = cbm_mcp_get_string_arg(args, "symbol");
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);
    size_t char_budget = max_tokens_to_char_budget(max_tokens);
    cbm_store_t *store = resolve_store(srv, project);

    if (!symbol || !symbol[0]) {
        free(project);
        free(symbol);
        return cbm_mcp_text_result("symbol is required", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(symbol);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(symbol);
        return not_indexed;
    }

    /* Session tracking: check BEFORE tracking for hint */
    bool was_already_queried = cbm_session_has_symbol(ensure_session(srv), symbol);
    {
        cbm_session_state_t *ss = ensure_session(srv);
        cbm_session_bump_query_count(ss);
        cbm_session_track_symbol(ss, symbol);
    }

    cbm_search_output_t qn_out = {0};
    cbm_search_output_t exact_name_out = {0};
    cbm_search_output_t selected_out = {0};
    cbm_node_t *suffix_nodes = NULL;
    int suffix_count = 0;
    const cbm_search_result_t **exact_refs = NULL;
    int exact_ref_count = 0;
    int exact_ref_cap = 0;
    const cbm_search_result_t *selected = NULL;
    bool auto_picked = false;
    cbm_traverse_result_t callers = {0};
    cbm_traverse_result_t callees = {0};
    connected_symbol_t *connected = NULL;
    char *understand_hint = NULL;
    int connected_count = 0;
    bool is_key_symbol = false;
    char *source = NULL;
    char *result = NULL;

    if (search_exact_qn(store, project, symbol, &qn_out) == CBM_STORE_OK && qn_out.count > 0) {
        selected = &qn_out.results[0];
    } else if (search_exact_name(store, project, symbol, &exact_name_out) == CBM_STORE_OK) {
        append_matching_refs(&exact_name_out, NULL, true, &exact_refs, &exact_ref_count,
                             &exact_ref_cap);
        if (exact_ref_count > 0) {
            qsort(exact_refs, (size_t)exact_ref_count, sizeof(*exact_refs),
                  understand_exact_match_cmp);
            selected = exact_refs[0];
            auto_picked = exact_ref_count > 1;
        }
    }

    if (!selected) {
        if (cbm_store_find_nodes_by_qn_suffix(store, project, symbol, &suffix_nodes,
                                              &suffix_count) != CBM_STORE_OK) {
            result = cbm_mcp_text_result("failed to resolve symbol", true);
            goto cleanup_understand;
        }

        int filtered = 0;
        for (int i = 0; i < suffix_count; i++) {
            if (compound_is_symbol_label(suffix_nodes[i].label)) {
                suffix_nodes[filtered++] = suffix_nodes[i];
            } else {
                cbm_node_free_fields(&suffix_nodes[i]);
            }
        }
        suffix_count = filtered;

        if (suffix_count == 1) {
            if (search_exact_qn(store, project, suffix_nodes[0].qualified_name, &selected_out) ==
                    CBM_STORE_OK &&
                selected_out.count > 0) {
                selected = &selected_out.results[0];
            }
        } else if (suffix_count > 1) {
            result = build_symbol_suggestions_response(symbol, suffix_nodes, suffix_count);
            goto cleanup_understand;
        }
    }

    if (!selected) {
        char err[512];
        snprintf(err, sizeof(err),
                 "symbol not found. Use search_graph(name_pattern=\".*%s.*\") first to discover "
                 "the exact symbol name.",
                 symbol);
        result = cbm_mcp_text_result(err, true);
        goto cleanup_understand;
    }

    source = load_node_source(srv, &selected->node);
    const char *edge_types[] = {"CALLS"};
    if (cbm_store_bfs(store, selected->node.id, "inbound", edge_types, 1, 1,
                      UNDERSTAND_MAX_BFS_RESULTS, &callers) != CBM_STORE_OK ||
        cbm_store_bfs(store, selected->node.id, "outbound", edge_types, 1, 1,
                      UNDERSTAND_MAX_BFS_RESULTS, &callees) != CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to load symbol dependencies", true);
        goto cleanup_understand;
    }

    if (callers.visited_count > 1) {
        qsort(callers.visited, (size_t)callers.visited_count, sizeof(cbm_node_hop_t),
              node_hop_rank_cmp);
    }
    if (callees.visited_count > 1) {
        qsort(callees.visited, (size_t)callees.visited_count, sizeof(cbm_node_hop_t),
              node_hop_rank_cmp);
    }
    if (collect_connected_symbols(&callers, &callees, &connected, &connected_count) !=
        CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to build connected symbol summary", true);
        goto cleanup_understand;
    }
    is_key_symbol = understand_is_key_symbol(store, project, selected);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "symbol", symbol);
    yyjson_mut_obj_add_str(doc, root, "qualified_name",
                           selected->node.qualified_name ? selected->node.qualified_name : "");
    yyjson_mut_obj_add_str(doc, root, "label", selected->node.label ? selected->node.label : "");
    yyjson_mut_obj_add_real(doc, root, "pagerank", selected->pagerank);
    yyjson_mut_obj_add_bool(doc, root, "is_key_symbol", is_key_symbol);
    add_understand_definition_json(doc, root, selected, source);

    if (auto_picked) {
        yyjson_mut_val *alts = yyjson_mut_arr(doc);
        for (int i = 1; i < exact_ref_count; i++) {
            add_alternative_json(doc, alts, exact_refs[i], false);
        }
        yyjson_mut_obj_add_val(doc, root, "alternatives", alts);
    }

    yyjson_mut_val *caller_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < callers.visited_count; i++) {
        add_node_hop_item(doc, caller_arr, &callers.visited[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "callers", caller_arr);

    yyjson_mut_val *callee_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < callees.visited_count; i++) {
        add_node_hop_item(doc, callee_arr, &callees.visited[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "callees", callee_arr);

    yyjson_mut_val *connected_arr = yyjson_mut_arr(doc);
    for (int i = 0; i < connected_count; i++) {
        add_connected_symbol_json(doc, connected_arr, &connected[i], false);
    }
    yyjson_mut_obj_add_val(doc, root, "connected_symbols", connected_arr);

    /* Compute session hint (Phase 7B) */
    understand_hint = build_understand_session_hint(srv, store, project, symbol,
                                                    was_already_queried, connected, connected_count);
    maybe_add_session_hint(doc, root, understand_hint, 0, NULL);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    if (json && strlen(json) > char_budget) {
        free(json);

        doc = yyjson_mut_doc_new(NULL);
        root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        yyjson_mut_obj_add_str(doc, root, "symbol", symbol);
        yyjson_mut_obj_add_str(doc, root, "qualified_name",
                               selected->node.qualified_name ? selected->node.qualified_name : "");
        yyjson_mut_obj_add_str(doc, root, "label",
                               selected->node.label ? selected->node.label : "");
        yyjson_mut_obj_add_real(doc, root, "pagerank", selected->pagerank);
        yyjson_mut_obj_add_bool(doc, root, "is_key_symbol", is_key_symbol);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        yyjson_mut_obj_add_int(doc, root, "total_results",
                               1 + callers.visited_count + callees.visited_count +
                                   connected_count + (auto_picked ? exact_ref_count - 1 : 0));

        size_t used = 96 + strlen(symbol) +
                      strlen(selected->node.qualified_name ? selected->node.qualified_name : "");
        int shown = 1;

        char *compact_source = NULL;
        if (source) {
            compact_source = truncate_text_copy(source, MAX_COMPACT_SOURCE_CHARS);
        }
        add_understand_definition_json(doc, root, selected, NULL);
        if (compact_source && compact_source[0]) {
            size_t estimate = estimate_understand_definition_chars(selected, compact_source, true) -
                              estimate_understand_definition_chars(selected, NULL, false);
            if (used + estimate <= char_budget) {
                yyjson_mut_val *definition = yyjson_mut_obj_get(root, "definition");
                yyjson_mut_obj_add_strcpy(doc, definition, "source", compact_source);
                yyjson_mut_obj_add_bool(doc, definition, "source_truncated", true);
                used += estimate;
            }
        }
        free(compact_source);

        if (auto_picked) {
            yyjson_mut_val *alts = yyjson_mut_arr(doc);
            for (int i = 1; i < exact_ref_count; i++) {
                size_t estimate = estimate_alternative_chars(exact_refs[i], true);
                if (used + estimate > char_budget && shown > 0) {
                    break;
                }
                add_alternative_json(doc, alts, exact_refs[i], true);
                used += estimate;
                shown++;
            }
            yyjson_mut_obj_add_val(doc, root, "alternatives", alts);
        }

        int full_items = 0;
        bool stop = false;
        caller_arr = yyjson_mut_arr(doc);
        for (int i = 0; i < callers.visited_count; i++) {
            bool compact = full_items >= MAX_FULL_BUDGET_ITEMS;
            size_t estimate = estimate_node_hop_chars(&callers.visited[i], compact);
            if (used + estimate > char_budget && !compact) {
                compact = true;
                estimate = estimate_node_hop_chars(&callers.visited[i], true);
            }
            if (used + estimate > char_budget && shown > 0) {
                stop = true;
                break;
            }
            add_node_hop_item(doc, caller_arr, &callers.visited[i], compact);
            used += estimate;
            shown++;
            if (!compact) {
                full_items++;
            }
        }
        yyjson_mut_obj_add_val(doc, root, "callers", caller_arr);

        callee_arr = yyjson_mut_arr(doc);
        if (!stop) {
            for (int i = 0; i < callees.visited_count; i++) {
                bool compact = full_items >= MAX_FULL_BUDGET_ITEMS;
                size_t estimate = estimate_node_hop_chars(&callees.visited[i], compact);
                if (used + estimate > char_budget && !compact) {
                    compact = true;
                    estimate = estimate_node_hop_chars(&callees.visited[i], true);
                }
                if (used + estimate > char_budget && shown > 0) {
                    stop = true;
                    break;
                }
                add_node_hop_item(doc, callee_arr, &callees.visited[i], compact);
                used += estimate;
                shown++;
                if (!compact) {
                    full_items++;
                }
            }
        }
        yyjson_mut_obj_add_val(doc, root, "callees", callee_arr);

        connected_arr = yyjson_mut_arr(doc);
        if (!stop) {
            for (int i = 0; i < connected_count; i++) {
                bool compact = true;
                size_t estimate = estimate_connected_symbol_chars(&connected[i], compact);
                if (used + estimate > char_budget && shown > 0) {
                    break;
                }
                add_connected_symbol_json(doc, connected_arr, &connected[i], compact);
                used += estimate;
                shown++;
            }
        }
        yyjson_mut_obj_add_val(doc, root, "connected_symbols", connected_arr);
        yyjson_mut_obj_add_int(doc, root, "shown", shown);
        maybe_add_session_hint(doc, root, understand_hint, char_budget, &used);

        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    result = cbm_mcp_text_result(json, false);
    free(json);

cleanup_understand:
    free(understand_hint);
    free(project);
    free(symbol);
    free(source);
    free(exact_refs);
    cbm_store_search_free(&qn_out);
    cbm_store_search_free(&exact_name_out);
    cbm_store_search_free(&selected_out);
    cbm_store_traverse_free(&callers);
    cbm_store_traverse_free(&callees);
    free_connected_symbols(connected, connected_count);
    cbm_store_free_nodes(suffix_nodes, suffix_count);
    return result;
}

static size_t estimate_review_scope_chars(const review_scope_t *scope, bool include_tests) {
    size_t size = 48;
    for (int i = 0; i < scope->must_review_count; i++) {
        size += estimate_string_list_entry_chars(scope->must_review[i]);
    }
    for (int i = 0; i < scope->should_review_count; i++) {
        size += estimate_string_list_entry_chars(scope->should_review[i]);
    }
    if (include_tests) {
        for (int i = 0; i < scope->test_count; i++) {
            size += estimate_string_list_entry_chars(scope->tests[i]);
        }
    }
    return size;
}

static void add_review_scope_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  const review_scope_t *scope, bool include_tests) {
    yyjson_mut_val *review = yyjson_mut_obj(doc);
    yyjson_mut_val *must = yyjson_mut_arr(doc);
    yyjson_mut_val *should = yyjson_mut_arr(doc);
    for (int i = 0; i < scope->must_review_count; i++) {
        yyjson_mut_arr_add_str(doc, must, scope->must_review[i] ? scope->must_review[i] : "");
    }
    for (int i = 0; i < scope->should_review_count; i++) {
        yyjson_mut_arr_add_str(doc, should, scope->should_review[i] ? scope->should_review[i] : "");
    }
    yyjson_mut_obj_add_val(doc, review, "must_review", must);
    yyjson_mut_obj_add_val(doc, review, "should_review", should);
    if (include_tests) {
        yyjson_mut_val *tests = yyjson_mut_arr(doc);
        for (int i = 0; i < scope->test_count; i++) {
            yyjson_mut_arr_add_str(doc, tests, scope->tests[i] ? scope->tests[i] : "");
        }
        yyjson_mut_obj_add_val(doc, review, "tests", tests);
    }
    yyjson_mut_obj_add_val(doc, root, "review_scope", review);
}

static int prepare_change_total_results(const cbm_impact_analysis_t *impact,
                                        const review_scope_t *scope, bool include_tests) {
    int total = impact_output_total_results(impact, include_tests);
    total += scope->must_review_count + scope->should_review_count;
    if (include_tests) {
        total += scope->test_count;
    }
    return total;
}

static char *handle_prepare_change(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    char *symbol = cbm_mcp_get_string_arg(args, "symbol");
    bool include_tests = cbm_mcp_get_bool_arg_default(args, "include_tests", true);
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);
    size_t char_budget = max_tokens_to_char_budget(max_tokens);
    cbm_store_t *store = resolve_store(srv, project);
    char *result = NULL;
    char *pc_hint = NULL;

    if (!symbol || !symbol[0]) {
        free(project);
        free(symbol);
        return cbm_mcp_text_result("symbol is required", true);
    }
    if (!store) {
        char *_err = build_project_list_error("project not found or not indexed");
        char *_res = cbm_mcp_text_result(_err, true);
        free(_err);
        free(project);
        free(symbol);
        return _res;
    }

    char *not_indexed = verify_project_indexed(store, project);
    if (not_indexed) {
        free(project);
        free(symbol);
        return not_indexed;
    }

    /* Session tracking */
    {
        cbm_session_state_t *ss = ensure_session(srv);
        cbm_session_bump_query_count(ss);
        cbm_session_track_impact(ss, symbol);
    }

    cbm_impact_analysis_t impact = {0};
    review_scope_t scope = {0};
    char *summary_text = NULL;

    int rc = cbm_store_get_impact_analysis(store, project, symbol, 4, &impact);
    if (rc == CBM_STORE_NOT_FOUND) {
        char err[512];
        snprintf(err, sizeof(err),
                 "symbol not found. Use search_graph(name_pattern=\".*%s.*\") first to discover "
                 "the exact symbol name.",
                 symbol);
        result = cbm_mcp_text_result(err, true);
        goto cleanup_prepare_change;
    }
    if (rc != CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to build pre-change analysis", true);
        goto cleanup_prepare_change;
    }

    if (build_review_scope(&impact, include_tests, &scope) != CBM_STORE_OK) {
        result = cbm_mcp_text_result("failed to build review scope", true);
        goto cleanup_prepare_change;
    }

    summary_text = impact_output_summary_dup(&impact, include_tests);
    if (!summary_text) {
        summary_text = heap_strdup(impact.summary ? impact.summary : "");
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "symbol", impact.symbol ? impact.symbol : "");
    yyjson_mut_obj_add_str(doc, root, "qualified_name",
                           impact.qualified_name ? impact.qualified_name : "");
    yyjson_mut_obj_add_str(doc, root, "file", impact.file ? impact.file : "");
    yyjson_mut_obj_add_real(doc, root, "pagerank", impact.pagerank);

    yyjson_mut_val *impact_obj = yyjson_mut_obj(doc);
    yyjson_mut_val *direct = yyjson_mut_arr(doc);
    for (int i = 0; i < impact.direct_count; i++) {
        add_impact_item_json(doc, direct, &impact.direct[i], false);
    }
    yyjson_mut_obj_add_val(doc, impact_obj, "direct", direct);

    yyjson_mut_val *indirect = yyjson_mut_arr(doc);
    for (int i = 0; i < impact.indirect_count; i++) {
        add_impact_item_json(doc, indirect, &impact.indirect[i], false);
    }
    yyjson_mut_obj_add_val(doc, impact_obj, "indirect", indirect);

    yyjson_mut_val *transitive = yyjson_mut_arr(doc);
    for (int i = 0; i < impact.transitive_count; i++) {
        add_impact_item_json(doc, transitive, &impact.transitive[i], false);
    }
    yyjson_mut_obj_add_val(doc, impact_obj, "transitive", transitive);
    yyjson_mut_obj_add_val(doc, root, "impact", impact_obj);

    yyjson_mut_val *tests = yyjson_mut_arr(doc);
    if (include_tests) {
        for (int i = 0; i < impact.affected_test_count; i++) {
            add_affected_test_json(doc, tests, &impact.affected_tests[i]);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "affected_tests", tests);
    add_review_scope_json(doc, root, &scope, include_tests);
    yyjson_mut_obj_add_str(doc, root, "risk_score", impact.risk_score ? impact.risk_score : "");
    yyjson_mut_obj_add_str(doc, root, "summary", summary_text ? summary_text : "");

    /* Session hint (Phase 7B) */
    pc_hint = build_impact_session_hint(srv, store, project, &impact);
    maybe_add_session_hint(doc, root, pc_hint, 0, NULL);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    if (json && strlen(json) > char_budget) {
        free(json);
        doc = yyjson_mut_doc_new(NULL);
        root = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root);

        yyjson_mut_obj_add_str(doc, root, "symbol", impact.symbol ? impact.symbol : "");
        yyjson_mut_obj_add_str(doc, root, "qualified_name",
                               impact.qualified_name ? impact.qualified_name : "");
        yyjson_mut_obj_add_str(doc, root, "file", impact.file ? impact.file : "");
        yyjson_mut_obj_add_real(doc, root, "pagerank", impact.pagerank);
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        yyjson_mut_obj_add_int(doc, root, "total_results",
                               prepare_change_total_results(&impact, &scope, include_tests));

        size_t used = 96;
        used += strlen(impact.symbol ? impact.symbol : "");
        used += strlen(impact.qualified_name ? impact.qualified_name : "");
        used += strlen(impact.file ? impact.file : "");
        used += strlen(summary_text ? summary_text : "");
        used += strlen(impact.risk_score ? impact.risk_score : "");

        int shown = 0;
        int full_items = 0;
        bool stop = false;

        impact_obj = yyjson_mut_obj(doc);
        add_budgeted_impact_group(doc, impact_obj, "direct", impact.direct, impact.direct_count,
                                  char_budget, &used, &shown, &full_items, &stop);
        yyjson_mut_obj_add_val(doc, root, "impact", impact_obj);

        if (!stop) {
            size_t estimate = estimate_review_scope_chars(&scope, include_tests);
            if (used + estimate <= char_budget || shown == 0) {
                add_review_scope_json(doc, root, &scope, include_tests);
                used += estimate;
                shown += scope.must_review_count + scope.should_review_count +
                         (include_tests ? scope.test_count : 0);
            } else {
                yyjson_mut_val *review = yyjson_mut_obj(doc);
                yyjson_mut_val *must = yyjson_mut_arr(doc);
                yyjson_mut_val *should = yyjson_mut_arr(doc);
                for (int i = 0; i < scope.must_review_count; i++) {
                    size_t item_estimate = estimate_string_list_entry_chars(scope.must_review[i]);
                    if (used + item_estimate > char_budget && shown > 0) {
                        stop = true;
                        break;
                    }
                    yyjson_mut_arr_add_str(doc, must, scope.must_review[i]);
                    used += item_estimate;
                    shown++;
                }
                if (!stop) {
                    for (int i = 0; i < scope.should_review_count; i++) {
                        size_t item_estimate = estimate_string_list_entry_chars(scope.should_review[i]);
                        if (used + item_estimate > char_budget && shown > 0) {
                            stop = true;
                            break;
                        }
                        yyjson_mut_arr_add_str(doc, should, scope.should_review[i]);
                        used += item_estimate;
                        shown++;
                    }
                }
                yyjson_mut_obj_add_val(doc, review, "must_review", must);
                yyjson_mut_obj_add_val(doc, review, "should_review", should);
                if (include_tests) {
                    yyjson_mut_val *review_tests = yyjson_mut_arr(doc);
                    if (!stop) {
                        for (int i = 0; i < scope.test_count; i++) {
                            size_t item_estimate = estimate_string_list_entry_chars(scope.tests[i]);
                            if (used + item_estimate > char_budget && shown > 0) {
                                stop = true;
                                break;
                            }
                            yyjson_mut_arr_add_str(doc, review_tests, scope.tests[i]);
                            used += item_estimate;
                            shown++;
                        }
                    }
                    yyjson_mut_obj_add_val(doc, review, "tests", review_tests);
                }
                yyjson_mut_obj_add_val(doc, root, "review_scope", review);
            }
        } else {
            yyjson_mut_val *review = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_val(doc, review, "must_review", yyjson_mut_arr(doc));
            yyjson_mut_obj_add_val(doc, review, "should_review", yyjson_mut_arr(doc));
            if (include_tests) {
                yyjson_mut_obj_add_val(doc, review, "tests", yyjson_mut_arr(doc));
            }
            yyjson_mut_obj_add_val(doc, root, "review_scope", review);
        }

        tests = yyjson_mut_arr(doc);
        if (include_tests && !stop) {
            for (int i = 0; i < impact.affected_test_count; i++) {
                size_t estimate = estimate_affected_test_chars(&impact.affected_tests[i]);
                if (used + estimate > char_budget && shown > 0) {
                    stop = true;
                    break;
                }
                add_affected_test_json(doc, tests, &impact.affected_tests[i]);
                used += estimate;
                shown++;
            }
        }
        yyjson_mut_obj_add_val(doc, root, "affected_tests", tests);

        if (!stop) {
            yyjson_mut_val *impact_existing = yyjson_mut_obj_get(root, "impact");
            add_budgeted_impact_group(doc, impact_existing, "indirect", impact.indirect,
                                      impact.indirect_count, char_budget, &used, &shown,
                                      &full_items, &stop);
            add_budgeted_impact_group(doc, impact_existing, "transitive", impact.transitive,
                                      impact.transitive_count, char_budget, &used, &shown,
                                      &full_items, &stop);
        } else {
            yyjson_mut_val *impact_existing = yyjson_mut_obj_get(root, "impact");
            yyjson_mut_obj_add_val(doc, impact_existing, "indirect", yyjson_mut_arr(doc));
            yyjson_mut_obj_add_val(doc, impact_existing, "transitive", yyjson_mut_arr(doc));
        }

        yyjson_mut_obj_add_str(doc, root, "risk_score", impact.risk_score ? impact.risk_score : "");
        yyjson_mut_obj_add_str(doc, root, "summary", summary_text ? summary_text : "");
        yyjson_mut_obj_add_int(doc, root, "shown", shown);
        maybe_add_session_hint(doc, root, pc_hint, char_budget, &used);

        json = yy_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    result = cbm_mcp_text_result(json, false);
    free(json);

cleanup_prepare_change:
    free(pc_hint);
    free(summary_text);
    review_scope_free(&scope);
    cbm_store_impact_analysis_free(&impact);
    free(project);
    free(symbol);
    return result;
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
        return cbm_mcp_text_result("project not found", true);
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
        return cbm_mcp_text_result("git diff failed", true);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    yyjson_mut_val *changed = yyjson_mut_arr(doc);
    yyjson_mut_val *impacted = yyjson_mut_arr(doc);

    /* resolve_store already called via get_project_root above */
    cbm_store_t *store = srv->store;

    /* Session tracking: bump count once for the detect_changes call */
    cbm_session_state_t *dc_session = ensure_session(srv);
    cbm_session_bump_query_count(dc_session);

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

        /* Session tracking: record each changed file */
        cbm_session_track_file_edited(dc_session, line);

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

    char *adr_buf = NULL; /* freed after yy_doc_to_str — yyjson holds pointer, not copy */
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

/* ── Session hints (Phase 7B) ──────────────────────────────────── */

#define SESSION_HINT_MAX_CHARS 800
#define SESSION_HINT_QUERY_THRESHOLD 10

/* Safe snprintf offset update: clamp to buffer size to prevent overflow. */
#define HINT_SNPRINTF(buf, bufsz, offvar, ...)                                        \
    do {                                                                              \
        if ((offvar) < (int)(bufsz)) {                                                \
            int _ret = snprintf((buf) + (offvar), (bufsz) - (size_t)(offvar), __VA_ARGS__); \
            if (_ret > 0) (offvar) += _ret;                                           \
            if ((offvar) >= (int)(bufsz)) (offvar) = (int)(bufsz) - 1;                \
        }                                                                             \
    } while (0)

/* Append a global "high-PageRank untouched" suggestion to buf.
 * Returns chars written (0 if nothing to say). */
static int append_global_pagerank_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                       const char *project, char *buf, int off, int bufsz) {
    cbm_session_state_t *ss = srv->session;
    if (!ss || !store || !project || off >= bufsz) {
        return 0;
    }
    int qc = cbm_session_query_count(ss);
    if (qc <= SESSION_HINT_QUERY_THRESHOLD) {
        return 0;
    }

    cbm_key_symbol_t *syms = NULL;
    int count = 0;
    cbm_store_get_key_symbols(store, project, NULL, 20, &syms, &count);

    int start_off = off;
    for (int i = 0; i < count; i++) {
        if (syms[i].name && !cbm_session_has_symbol(ss, syms[i].name) &&
            !cbm_session_has_file_read(ss, syms[i].file_path)) {
            HINT_SNPRINTF(buf, (size_t)bufsz, off,
                          "%sYou've made %d queries but haven't examined %s"
                          " (PageRank #%d, %d inbound calls).",
                          start_off > 0 ? " " : "", qc, syms[i].name, i + 1,
                          syms[i].in_degree);
            break;
        }
    }

    cbm_store_key_symbols_free(syms, count);
    return off - start_off;
}

/* Build hint for explore/search when an area was already explored.
 * Returns heap string or NULL. Caller frees. */
static char *build_explore_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                        const char *project, const char *area,
                                        bool was_area_explored,
                                        const cbm_search_result_t *const *matches,
                                        int match_count) {
    cbm_session_state_t *ss = srv->session;
    if (!ss) {
        return NULL;
    }

    char buf[SESSION_HINT_MAX_CHARS];
    buf[0] = '\0';
    int off = 0;

    if (was_area_explored) {
        /* Count new vs already-seen results */
        int new_count = 0;
        char new_names[400];
        new_names[0] = '\0';
        int noff = 0;
        for (int i = 0; i < match_count && new_count < 5; i++) {
            if (matches[i]->node.name && !cbm_session_has_symbol(ss, matches[i]->node.name)) {
                if (noff > 0) {
                    HINT_SNPRINTF(new_names, sizeof(new_names), noff, ", ");
                }
                HINT_SNPRINTF(new_names, sizeof(new_names), noff, "%s",
                              matches[i]->node.name);
                new_count++;
            }
        }
        if (new_count > 0) {
            HINT_SNPRINTF(buf, sizeof(buf), off,
                          "You previously explored '%s'. New results not seen before: %s.",
                          area, new_names);
        } else {
            HINT_SNPRINTF(buf, sizeof(buf), off,
                          "You previously explored '%s'. All results were already examined.",
                          area);
        }
    }

    off += append_global_pagerank_hint(srv, store, project, buf, off, (int)sizeof(buf));

    if (off == 0) {
        return NULL;
    }
    return strdup(buf);
}

/* Build hint for understand when symbol was already queried.
 * Returns heap string or NULL. Caller frees. */
static char *build_understand_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                           const char *project, const char *symbol,
                                           bool was_already_queried,
                                           const connected_symbol_t *connected,
                                           int connected_count) {
    cbm_session_state_t *ss = srv->session;
    if (!ss) {
        return NULL;
    }

    char buf[SESSION_HINT_MAX_CHARS];
    buf[0] = '\0';
    int off = 0;

    if (was_already_queried) {
        HINT_SNPRINTF(buf, sizeof(buf), off,
                      "You already queried '%s' earlier in this session.", symbol);
    }

    /* Find connected symbols not yet examined */
    int unseen_count = 0;
    char unseen_names[400];
    unseen_names[0] = '\0';
    int noff = 0;
    for (int i = 0; i < connected_count && unseen_count < 5; i++) {
        if (connected[i].node.name && !cbm_session_has_symbol(ss, connected[i].node.name)) {
            if (noff > 0) {
                HINT_SNPRINTF(unseen_names, sizeof(unseen_names), noff, ", ");
            }
            HINT_SNPRINTF(unseen_names, sizeof(unseen_names), noff, "%s",
                          connected[i].node.name);
            unseen_count++;
        }
    }
    if (unseen_count > 0) {
        HINT_SNPRINTF(buf, sizeof(buf), off,
                      "%sRelated symbols not yet examined: %s.", off > 0 ? " " : "",
                      unseen_names);
    }

    off += append_global_pagerank_hint(srv, store, project, buf, off, (int)sizeof(buf));

    if (off == 0) {
        return NULL;
    }
    return strdup(buf);
}

/* Build hint for prepare_change / get_impact_analysis when blast radius
 * overlaps with already-edited files.
 * Returns heap string or NULL. Caller frees. */
static char *build_impact_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                       const char *project,
                                       const cbm_impact_analysis_t *impact) {
    cbm_session_state_t *ss = srv->session;
    if (!ss || !impact) {
        return NULL;
    }

    char buf[SESSION_HINT_MAX_CHARS];
    buf[0] = '\0';
    int off = 0;

    /* Check if any files in the blast radius were already edited.
     * Track seen files by pointer to avoid strstr false positives. */
    const char *seen_files[5];
    int edited_count = 0;
    char edited_files[400];
    edited_files[0] = '\0';
    int eoff = 0;

    /* Check the target symbol's file */
    if (impact->file && cbm_session_has_file_edited(ss, impact->file)) {
        HINT_SNPRINTF(edited_files, sizeof(edited_files), eoff, "%s", impact->file);
        seen_files[edited_count++] = impact->file;
    }

    /* Check direct/indirect/transitive impact items */
    const struct {
        const cbm_impact_item_t *items;
        int count;
    } groups[] = {
        {impact->direct, impact->direct_count},
        {impact->indirect, impact->indirect_count},
        {impact->transitive, impact->transitive_count},
    };
    for (int g = 0; g < 3 && edited_count < 5; g++) {
        for (int i = 0; i < groups[g].count && edited_count < 5; i++) {
            const char *file = groups[g].items[i].file;
            if (file && cbm_session_has_file_edited(ss, file)) {
                /* Exact dedup against already-seen files */
                bool dup = false;
                for (int s = 0; s < edited_count; s++) {
                    if (strcmp(seen_files[s], file) == 0) {
                        dup = true;
                        break;
                    }
                }
                if (!dup) {
                    if (eoff > 0) {
                        HINT_SNPRINTF(edited_files, sizeof(edited_files), eoff, ", ");
                    }
                    HINT_SNPRINTF(edited_files, sizeof(edited_files), eoff, "%s", file);
                    seen_files[edited_count++] = file;
                }
            }
        }
    }

    if (edited_count > 0) {
        HINT_SNPRINTF(buf, sizeof(buf), off,
                      "Warning: %s %s already edited this session.",
                      edited_files, edited_count > 1 ? "were" : "was");
    }

    off += append_global_pagerank_hint(srv, store, project, buf, off, (int)sizeof(buf));

    if (off == 0) {
        return NULL;
    }
    return strdup(buf);
}

/* Build hint for search_graph: note how many results were already examined.
 * Returns heap string or NULL. Caller frees. */
static char *build_search_session_hint(cbm_mcp_server_t *srv, cbm_store_t *store,
                                       const char *project, const cbm_search_output_t *out) {
    cbm_session_state_t *ss = srv->session;
    if (!ss || !out || out->count == 0) {
        return NULL;
    }

    char buf[SESSION_HINT_MAX_CHARS];
    buf[0] = '\0';
    int off = 0;

    /* Count already-seen results */
    int seen = 0;
    for (int i = 0; i < out->count; i++) {
        if (out->results[i].node.name && cbm_session_has_symbol(ss, out->results[i].node.name)) {
            seen++;
        }
    }
    if (seen > 0 && seen < out->count) {
        HINT_SNPRINTF(buf, sizeof(buf), off,
                      "%d of %d results were already examined this session.", seen, out->count);
    } else if (seen > 0 && seen == out->count) {
        HINT_SNPRINTF(buf, sizeof(buf), off,
                      "All %d results were already examined this session.", out->count);
    }

    off += append_global_pagerank_hint(srv, store, project, buf, off, (int)sizeof(buf));

    if (off == 0) {
        return NULL;
    }
    return strdup(buf);
}

/* Inject session_hint into JSON doc if hint is non-NULL and fits the budget.
 * For full-build (no budget): pass char_budget=0 and used=NULL.
 * For truncated build: pass the tracked budget. */
static void maybe_add_session_hint(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *hint,
                                   size_t char_budget, size_t *used) {
    if (!hint || !hint[0]) {
        return;
    }
    size_t hint_len = strlen(hint);
    if (used && char_budget > 0 && (*used + hint_len + 30 > char_budget)) {
        return; /* skip — would blow the token budget */
    }
    yyjson_mut_obj_add_strcpy(doc, root, "session_hint", hint);
    if (used) {
        *used += hint_len + 30;
    }
}

/* ── Session helpers (shared by 7A + 7C) ──────────────────────── */

/* Callback: free a strdup'd hash table key (for temporary candidate sets). */
static void free_ht_key_cb(const char *key, void *value, void *userdata) {
    (void)value;
    (void)userdata;
    free((void *)key);
}

/* Callback: append key to a yyjson array (used by get_session_context). */
typedef struct {
    yyjson_mut_doc *doc;
    yyjson_mut_val *arr;
} json_arr_ctx_t;

static void append_key_to_json_arr(const char *key, void *userdata) {
    json_arr_ctx_t *ctx = (json_arr_ctx_t *)userdata;
    yyjson_mut_arr_add_strcpy(ctx->doc, ctx->arr, key);
}

/* Callback: collect symbol names into a list for neighbor lookup. */
typedef struct {
    const char **names;
    int count;
    int cap;
} name_collector_t;

static void collect_symbol_name(const char *key, void *userdata) {
    name_collector_t *nc = (name_collector_t *)userdata;
    if (nc->count < nc->cap) {
        nc->names[nc->count++] = key;
    }
}

/* ── Session summary (Phase 7C) ────────────────────────────────── */

/* Callback context for iterating session sets into markdown. */
typedef struct {
    markdown_builder_t *md;
    int count; /* items emitted so far */
} md_list_ctx_t;

static void append_key_comma_separated(const char *key, void *userdata) {
    md_list_ctx_t *ctx = (md_list_ctx_t *)userdata;
    if (ctx->count > 0) {
        (void)markdown_builder_append_raw(ctx->md, ", ");
    }
    (void)markdown_builder_append_raw(ctx->md, key);
    ctx->count++;
}

static void append_key_bullet(const char *key, void *userdata) {
    md_list_ctx_t *ctx = (md_list_ctx_t *)userdata;
    (void)markdown_builder_appendf(ctx->md, "- %s\n", key);
    ctx->count++;
}

static char *handle_get_session_summary(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    int max_tokens = cbm_mcp_get_int_arg(args, "max_tokens", DEFAULT_MAX_TOKENS);

    cbm_session_state_t *ss = ensure_session(srv);
    cbm_store_t *store = project ? resolve_store(srv, project) : NULL;

    size_t char_budget = max_tokens_to_char_budget(max_tokens);
    markdown_builder_t md;
    markdown_builder_init(&md, char_budget);

    /* ── Header ──────────────────────────────────────────────── */
    time_t start = cbm_session_start_time(ss);
    time_t now = time(NULL);
    int elapsed = (int)(now - start);
    if (elapsed < 0) elapsed = 0;
    int minutes = elapsed / 60;
    int seconds = elapsed % 60;
    int qc = cbm_session_query_count(ss);

    if (minutes > 0) {
        (void)markdown_builder_appendf(&md, "## Session Summary (%d queries, %dm%ds)\n\n",
                                       qc, minutes, seconds);
    } else {
        (void)markdown_builder_appendf(&md, "## Session Summary (%d queries, %ds)\n\n",
                                       qc, seconds);
    }

    /* ── Files touched ───────────────────────────────────────── */
    int read_count = cbm_session_files_read_count(ss);
    int edited_count = cbm_session_files_edited_count(ss);

    if (read_count > 0 || edited_count > 0) {
        (void)markdown_builder_append_raw(&md, "### Files touched\n");
        if (read_count > 0) {
            (void)markdown_builder_append_raw(&md, "- **Read:** ");
            md_list_ctx_t ctx = {.md = &md, .count = 0};
            cbm_session_foreach_file_read(ss, append_key_comma_separated, &ctx);
            (void)markdown_builder_append_raw(&md, "\n");
        }
        if (edited_count > 0) {
            (void)markdown_builder_append_raw(&md, "- **Edited:** ");
            md_list_ctx_t ctx = {.md = &md, .count = 0};
            cbm_session_foreach_file_edited(ss, append_key_comma_separated, &ctx);
            (void)markdown_builder_append_raw(&md, "\n");
        }
        (void)markdown_builder_append_raw(&md, "\n");
    }

    /* ── Symbols investigated ────────────────────────────────── */
    int sym_count = cbm_session_symbols_count(ss);
    int impact_count = cbm_session_impacts_count(ss);

    if (sym_count > 0 || impact_count > 0) {
        (void)markdown_builder_append_raw(&md, "### Symbols investigated\n");

        /* Collect queried symbol names */
        const char *sym_names[30];
        name_collector_t sc = {.names = sym_names, .count = 0, .cap = 30};
        cbm_session_foreach_symbol(ss, collect_symbol_name, &sc);

        for (int i = 0; i < sc.count; i++) {
            const char *name = sc.names[i];

            /* Look up PageRank if store available */
            if (store) {
                cbm_key_symbol_t *ks = NULL;
                int ks_count = 0;
                cbm_store_get_key_symbols(store, project, name, 1, &ks, &ks_count);
                if (ks_count > 0 && ks[0].name && strcmp(ks[0].name, name) == 0) {
                    (void)markdown_builder_appendf(&md, "- %s (%d callers, PageRank %.4f)",
                                                   name, ks[0].in_degree, ks[0].pagerank);
                } else {
                    (void)markdown_builder_appendf(&md, "- %s", name);
                }
                cbm_store_key_symbols_free(ks, ks_count);
            } else {
                (void)markdown_builder_appendf(&md, "- %s", name);
            }
            (void)markdown_builder_append_raw(&md, "\n");
        }
        (void)markdown_builder_append_raw(&md, "\n");
    }

    /* ── Impact analyses ─────────────────────────────────────── */
    if (impact_count > 0) {
        (void)markdown_builder_append_raw(&md, "### Impact analyses run\n");
        md_list_ctx_t ctx = {.md = &md, .count = 0};
        cbm_session_foreach_impact(ss, append_key_bullet, &ctx);
        (void)markdown_builder_append_raw(&md, "\n");
    }

    /* ── Areas explored ──────────────────────────────────────── */
    int area_count = cbm_session_areas_count(ss);
    if (area_count > 0) {
        (void)markdown_builder_append_raw(&md, "### Areas explored\n");
        md_list_ctx_t ctx = {.md = &md, .count = 0};
        cbm_session_foreach_area(ss, append_key_bullet, &ctx);
        (void)markdown_builder_append_raw(&md, "\n");
    }

    /* ── Suggested next steps ────────────────────────────────── */
    if (store && sym_count > 0) {
        {
            /* Collect symbols for neighbor lookup */
            const char *lookup_names[20];
            name_collector_t nc = {.names = lookup_names, .count = 0, .cap = 20};
            cbm_session_foreach_symbol(ss, collect_symbol_name, &nc);
            cbm_session_foreach_impact(ss, collect_symbol_name, &nc);

            /* Temporary dedup set for candidates */
            CBMHashTable *candidates = cbm_ht_create(64);
            for (int i = 0; i < nc.count; i++) {
                cbm_node_t *nodes = NULL;
                int ncount = 0;
                cbm_store_find_nodes_by_name(store, project, lookup_names[i], &nodes, &ncount);
                for (int j = 0; j < ncount; j++) {
                    char **callers = NULL;
                    char **callees = NULL;
                    int caller_count = 0, callee_count = 0;
                    cbm_store_node_neighbor_names(store, nodes[j].id, 10, &callers, &caller_count,
                                                  &callees, &callee_count);
                    for (int k = 0; k < caller_count; k++) {
                        if (callers[k] && !cbm_session_has_symbol(ss, callers[k]) &&
                            !cbm_ht_has(candidates, callers[k])) {
                            char *key = strdup(callers[k]);
                            if (key) cbm_ht_set(candidates, key, (void *)lookup_names[i]);
                        }
                    }
                    for (int k = 0; k < callee_count; k++) {
                        if (callees[k] && !cbm_session_has_symbol(ss, callees[k]) &&
                            !cbm_ht_has(candidates, callees[k])) {
                            char *key = strdup(callees[k]);
                            if (key) cbm_ht_set(candidates, key, (void *)lookup_names[i]);
                        }
                    }
                    for (int k = 0; k < caller_count; k++) free(callers[k]);
                    free(callers);
                    for (int k = 0; k < callee_count; k++) free(callees[k]);
                    free(callees);
                }
                cbm_store_free_nodes(nodes, ncount);
            }

            if (cbm_ht_count(candidates) > 0) {
                cbm_key_symbol_t *key_syms = NULL;
                int ks_count = 0;
                cbm_store_get_key_symbols(store, project, NULL, 200, &key_syms, &ks_count);

                bool header_emitted = false;
                int emitted = 0;
                for (int i = 0; i < ks_count && emitted < 5; i++) {
                    if (key_syms[i].name && cbm_ht_has(candidates, key_syms[i].name)) {
                        if (!header_emitted) {
                            (void)markdown_builder_append_raw(&md, "### Suggested next steps\n");
                            header_emitted = true;
                        }
                        const char *reason =
                            (const char *)cbm_ht_get(candidates, key_syms[i].name);
                        (void)markdown_builder_appendf(
                            &md, "- Examine %s%s%s (neighbor of %s, not yet examined)\n",
                            key_syms[i].name,
                            key_syms[i].file_path ? " in " : "",
                            key_syms[i].file_path ? key_syms[i].file_path : "",
                            reason ? reason : "queried symbol");
                        emitted++;
                    }
                }
                cbm_store_key_symbols_free(key_syms, ks_count);
            }

            cbm_ht_foreach(candidates, free_ht_key_cb, NULL);
            cbm_ht_free(candidates);
        }
    }

    char *markdown = markdown_builder_finish(&md);
    free(project);

    char *result = cbm_mcp_text_result(markdown ? markdown : "", false);
    free(markdown);
    return result;
}

/* ── Session context (Phase 7A) ────────────────────────────────── */

static char *handle_get_session_context(cbm_mcp_server_t *srv, const char *args) {
    char *project = cbm_mcp_get_string_arg(args, "project");
    bool include_related = cbm_mcp_get_bool_arg_default(args, "include_related", true);
    int limit = cbm_mcp_get_int_arg(args, "limit", 10);

    cbm_session_state_t *ss = ensure_session(srv);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    /* Basic session stats */
    time_t start = cbm_session_start_time(ss);
    time_t now = time(NULL);
    yyjson_mut_obj_add_int(doc, root, "session_duration_s", (int64_t)(now - start));
    yyjson_mut_obj_add_int(doc, root, "query_count", cbm_session_query_count(ss));

    /* Build set arrays using iteration */
    json_arr_ctx_t jctx;
    jctx.doc = doc;

#define EMIT_SET_ARRAY(field_name, foreach_fn)        \
    do {                                              \
        jctx.arr = yyjson_mut_arr(doc);               \
        foreach_fn(ss, append_key_to_json_arr, &jctx); \
        yyjson_mut_obj_add_val(doc, root, field_name, jctx.arr); \
    } while (0)

    EMIT_SET_ARRAY("files_read", cbm_session_foreach_file_read);
    EMIT_SET_ARRAY("files_edited", cbm_session_foreach_file_edited);
    EMIT_SET_ARRAY("symbols_queried", cbm_session_foreach_symbol);
    EMIT_SET_ARRAY("areas_explored", cbm_session_foreach_area);
    EMIT_SET_ARRAY("impact_analyses_run", cbm_session_foreach_impact);

#undef EMIT_SET_ARRAY

    /* related_untouched: graph neighbors of touched symbols not yet examined */
    yyjson_mut_val *related = yyjson_mut_arr(doc);
    if (include_related && project) {
        cbm_store_t *store = resolve_store(srv, project);
        if (store) {
            /* Collect queried symbols + impact symbols (cap at 20 total) */
            const int MAX_LOOKUP = 20;
            const char *lookup_names[20];
            name_collector_t nc = {.names = lookup_names, .count = 0, .cap = MAX_LOOKUP};
            cbm_session_foreach_symbol(ss, collect_symbol_name, &nc);
            cbm_session_foreach_impact(ss, collect_symbol_name, &nc);

            /* Temporary dedup set for candidates */
            CBMHashTable *candidates = cbm_ht_create(64);

            /* For each tracked symbol, find 1-hop neighbors */
            for (int i = 0; i < nc.count; i++) {
                cbm_node_t *nodes = NULL;
                int ncount = 0;
                cbm_store_find_nodes_by_name(store, project, lookup_names[i], &nodes, &ncount);

                for (int j = 0; j < ncount; j++) {
                    char **callers = NULL;
                    char **callees = NULL;
                    int caller_count = 0, callee_count = 0;
                    cbm_store_node_neighbor_names(store, nodes[j].id, 10, &callers, &caller_count,
                                                  &callees, &callee_count);

                    /* Add callers not already tracked */
                    for (int k = 0; k < caller_count; k++) {
                        if (callers[k] && !cbm_session_has_symbol(ss, callers[k]) &&
                            !cbm_ht_has(candidates, callers[k])) {
                            char *key = strdup(callers[k]);
                            if (key) {
                                /* Store the "reason" source symbol as the value */
                                cbm_ht_set(candidates, key, (void *)lookup_names[i]);
                            }
                        }
                    }
                    /* Add callees not already tracked */
                    for (int k = 0; k < callee_count; k++) {
                        if (callees[k] && !cbm_session_has_symbol(ss, callees[k]) &&
                            !cbm_ht_has(candidates, callees[k])) {
                            char *key = strdup(callees[k]);
                            if (key) {
                                cbm_ht_set(candidates, key, (void *)lookup_names[i]);
                            }
                        }
                    }

                    /* Free neighbor arrays */
                    for (int k = 0; k < caller_count; k++)
                        free(callers[k]);
                    free(callers);
                    for (int k = 0; k < callee_count; k++)
                        free(callees[k]);
                    free(callees);
                }
                cbm_store_free_nodes(nodes, ncount);
            }

            /* Rank candidates by PageRank: get key symbols and match against candidates */
            if (cbm_ht_count(candidates) > 0) {
                cbm_key_symbol_t *key_syms = NULL;
                int ks_count = 0;
                cbm_store_get_key_symbols(store, project, NULL, 200, &key_syms, &ks_count);

                int emitted = 0;
                for (int i = 0; i < ks_count && emitted < limit; i++) {
                    if (key_syms[i].name && cbm_ht_has(candidates, key_syms[i].name)) {
                        const char *reason_sym =
                            (const char *)cbm_ht_get(candidates, key_syms[i].name);
                        yyjson_mut_val *item = yyjson_mut_obj(doc);
                        /* Use strcpy variants: key_syms strings are freed below */
                        yyjson_mut_obj_add_strcpy(doc, item, "name", key_syms[i].name);
                        if (key_syms[i].file_path) {
                            yyjson_mut_obj_add_strcpy(doc, item, "file", key_syms[i].file_path);
                        }
                        if (key_syms[i].pagerank > 0) {
                            yyjson_mut_obj_add_real(doc, item, "pagerank", key_syms[i].pagerank);
                        }
                        /* Build reason string (stack buffer — must copy) */
                        char reason[256];
                        snprintf(reason, sizeof(reason), "neighbor of %s, not yet examined",
                                 reason_sym ? reason_sym : "queried symbol");
                        yyjson_mut_obj_add_strcpy(doc, item, "reason", reason);
                        yyjson_mut_arr_add_val(related, item);
                        emitted++;
                    }
                }

                /* Free key symbols */
                for (int i = 0; i < ks_count; i++) {
                    free((void *)key_syms[i].name);
                    free((void *)key_syms[i].qualified_name);
                    free((void *)key_syms[i].label);
                    free((void *)key_syms[i].file_path);
                }
                free(key_syms);
            }

            /* Free candidate keys */
            cbm_ht_foreach(candidates, free_ht_key_cb, NULL);
            cbm_ht_free(candidates);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "related_untouched", related);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(project);

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
    if (strcmp(tool_name, "get_key_symbols") == 0) {
        return handle_get_key_symbols(srv, args_json);
    }
    if (strcmp(tool_name, "get_impact_analysis") == 0) {
        return handle_get_impact_analysis(srv, args_json);
    }
    if (strcmp(tool_name, "get_architecture_summary") == 0) {
        return handle_get_architecture_summary(srv, args_json);
    }
    if (strcmp(tool_name, "explore") == 0) {
        return handle_explore(srv, args_json);
    }
    if (strcmp(tool_name, "understand") == 0) {
        return handle_understand(srv, args_json);
    }
    if (strcmp(tool_name, "prepare_change") == 0) {
        return handle_prepare_change(srv, args_json);
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
    if (strcmp(tool_name, "get_session_context") == 0) {
        return handle_get_session_context(srv, args_json);
    }
    if (strcmp(tool_name, "get_session_summary") == 0) {
        return handle_get_session_summary(srv, args_json);
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
    cbm_pipeline_lock();
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_unlock();

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
