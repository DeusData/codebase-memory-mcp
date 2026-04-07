/*
 * pass_definitions.c — Extract definitions from source files.
 *
 * For each discovered file:
 *   1. Read source content from disk
 *   2. Call cbm_extract_file() to get defs, calls, imports
 *   3. Create Function/Class/Method/Variable/Module nodes in graph buffer
 *   4. Register callables in the function registry
 *   5. Store import maps and call sites for later passes
 *
 * Depends on: extraction layer (cbm.h), graph_buffer, pipeline internals
 */
#include "foundation/constants.h"

enum { PD_RING = 4, PD_RING_MASK = 3, PD_JSON_MARGIN = 10, PD_ESC_MARGIN = 3, PD_ESC_SPACE = 2 };
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "cbm.h"
#include "simhash/minhash.h"
#include "semantic/ast_profile.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Read entire file into heap-allocated buffer. Returns NULL on error.
 * Caller must free(). Sets *out_len to byte count. */
static char *read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }

    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);

    if (size <= 0 ||
        size > (long)CBM_PERCENT * CBM_SZ_1K * CBM_SZ_1K) { /* CBM_PERCENT MB sanity limit */
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc(size + SKIP_ONE);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t nread = fread(buf, SKIP_ONE, size, f);
    (void)fclose(f);

    if (nread > (size_t)size) {
        nread = (size_t)size;
    }
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* Format int to string for logging. Thread-safe via TLS. */
static const char *itoa_log(int val) {
    static CBM_TLS char bufs[PD_RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & PD_RING_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Append a JSON-escaped string value to buf at position *pos.
 * Writes: ,"key":"escaped_value"
 * Handles: \, ", \n, \r, \t */
static int def_json_escape_char(char *buf, size_t avail, char ch) {
    char esc = 0;
    switch (ch) {
    case '"':
        esc = '"';
        break;
    case '\\':
        esc = '\\';
        break;
    case '\n':
        esc = 'n';
        break;
    case '\r':
        esc = 'r';
        break;
    case '\t':
        esc = 't';
        break;
    default:
        if (avail >= SKIP_ONE) {
            buf[0] = ch;
        }
        return SKIP_ONE;
    }
    if (avail >= PD_ESC_SPACE) {
        buf[0] = '\\';
        buf[SKIP_ONE] = esc;
    }
    return PD_ESC_SPACE;
}

static void append_json_string(char *buf, size_t bufsize, size_t *pos, const char *key,
                               const char *val) {
    if (!val || val[0] == '\0') {
        return;
    }
    if (*pos >= bufsize - PD_JSON_MARGIN) {
        return;
    }
    size_t p = *pos;
    int w = snprintf(buf + p, bufsize - p, ",\"%s\":\"", key);
    if (w <= 0 || (size_t)w >= bufsize - p) {
        return;
    }
    p += (size_t)w;
    for (const char *s = val; *s && p < bufsize - PD_ESC_MARGIN; s++) {
        p += (size_t)def_json_escape_char(buf + p, bufsize - p - PD_ESC_SPACE, *s);
    }
    if (p < bufsize - SKIP_ONE) {
        buf[p++] = '"';
    }
    buf[p] = '\0';
    *pos = p;
}

/* Append a JSON array of strings: ,"key":["a","b","c"] */
static void append_json_str_array(char *buf, size_t bufsize, size_t *pos, const char *key,
                                  const char **arr) {
    if (!arr || !arr[0] || *pos >= bufsize - PD_JSON_MARGIN) {
        return;
    }
    size_t p = *pos;
    int n = snprintf(buf + p, bufsize - p, ",\"%s\":[", key);
    if (n <= 0 || p + (size_t)n >= bufsize - PD_ESC_SPACE) {
        return;
    }
    p += (size_t)n;
    for (int i = 0; arr[i]; i++) {
        if (i > 0 && p < bufsize - SKIP_ONE) {
            buf[p++] = ',';
        }
        if (p < bufsize - SKIP_ONE) {
            buf[p++] = '"';
        }
        for (const char *s = arr[i]; *s && p < bufsize - PD_ESC_SPACE; s++) {
            if (*s == '"' || *s == '\\') {
                buf[p++] = '\\';
                if (p >= bufsize - PD_ESC_SPACE) {
                    break;
                }
            }
            buf[p++] = *s;
        }
        if (p < bufsize - SKIP_ONE) {
            buf[p++] = '"';
        }
    }
    if (p < bufsize - SKIP_ONE) {
        buf[p++] = ']';
    }
    buf[p] = '\0';
    *pos = p;
}

/* Build properties JSON for a definition node. */
static void build_def_props(char *buf, size_t bufsize, const CBMDefinition *def) {
    int n = snprintf(buf, bufsize,
                     "{\"complexity\":%d,\"lines\":%d,\"is_exported\":%s,"
                     "\"is_test\":%s,\"is_entry_point\":%s",
                     def->complexity, def->lines, def->is_exported ? "true" : "false",
                     def->is_test ? "true" : "false", def->is_entry_point ? "true" : "false");

    if (n <= 0 || (size_t)n >= bufsize) {
        buf[0] = '\0';
        return;
    }
    size_t pos = (size_t)n;
    append_json_string(buf, bufsize, &pos, "docstring", def->docstring);
    append_json_string(buf, bufsize, &pos, "signature", def->signature);
    append_json_string(buf, bufsize, &pos, "return_type", def->return_type);
    append_json_string(buf, bufsize, &pos, "parent_class", def->parent_class);
    append_json_str_array(buf, bufsize, &pos, "decorators", def->decorators);
    append_json_str_array(buf, bufsize, &pos, "base_classes", def->base_classes);
    append_json_str_array(buf, bufsize, &pos, "param_names", def->param_names);
    append_json_str_array(buf, bufsize, &pos, "param_types", def->param_types);
    append_json_string(buf, bufsize, &pos, "route_path", def->route_path);
    append_json_string(buf, bufsize, &pos, "route_method", def->route_method);

    /* MinHash fingerprint — append if present and buffer has room. */
    if (def->fingerprint && def->fingerprint_k > 0 &&
        pos + CBM_MINHASH_HEX_LEN + CBM_MINHASH_JSON_OVERHEAD < bufsize) {
        char fp_hex[CBM_MINHASH_HEX_BUF];
        cbm_minhash_to_hex((const cbm_minhash_t *)def->fingerprint, fp_hex, sizeof(fp_hex));
        append_json_string(buf, bufsize, &pos, "fp", fp_hex);
    }

    /* AST structural profile */
    if (def->structural_profile && pos + CBM_AST_PROFILE_BUF < bufsize) {
        append_json_string(buf, bufsize, &pos, "sp", def->structural_profile);
    }

    /* Body tokens */
    if (def->body_tokens && pos + CBM_SZ_512 < bufsize) {
        append_json_string(buf, bufsize, &pos, "bt", def->body_tokens);
    }

    if (pos < bufsize - SKIP_ONE) {
        buf[pos] = '}';
        buf[pos + SKIP_ONE] = '\0';
    }
}

/* Process one definition: create node, register, DEFINES + DEFINES_METHOD edges. */
static void process_def(cbm_pipeline_ctx_t *ctx, const CBMDefinition *def, const char *rel) {
    if (!def->qualified_name || !def->name) {
        return;
    }
    char props[CBM_SZ_2K];
    build_def_props(props, sizeof(props), def);
    int64_t node_id = cbm_gbuf_upsert_node(
        ctx->gbuf, def->label ? def->label : "Function", def->name, def->qualified_name,
        def->file_path ? def->file_path : rel, (int)def->start_line, (int)def->end_line, props);
    /* Register callable symbols + Interface.  Interface must be in the registry
     * so C#/Java `class Foo : IBar` / `class Foo implements IBar` can resolve
     * `IBar` to an INHERITS edge target during the enrichment phase. */
    if (node_id > 0 && def->label &&
        (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0 ||
         strcmp(def->label, "Class") == 0 || strcmp(def->label, "Interface") == 0)) {
        cbm_registry_add(ctx->registry, def->name, def->qualified_name, def->label);
    }
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    if (file_node && node_id > 0) {
        cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, node_id, "DEFINES", "{}");
    }
    free(file_qn);
    if (def->parent_class && def->label && strcmp(def->label, "Method") == 0) {
        const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(ctx->gbuf, def->parent_class);
        if (parent && node_id > 0) {
            cbm_gbuf_insert_edge(ctx->gbuf, parent->id, node_id, "DEFINES_METHOD", "{}");
        }
    }
}

/* Create Channel nodes + EMITS / LISTENS_ON edges for one file's channels.
 * Mirrors the parallel path in cbm_build_registry_from_cache — keep in sync. */
/* Find the source node for a channel edge: enclosing function or file node. */
static const cbm_gbuf_node_t *find_channel_source(cbm_pipeline_ctx_t *ctx, const CBMChannel *ch,
                                                  const char *rel) {
    const cbm_gbuf_node_t *node = NULL;
    if (ch->enclosing_func_qn && ch->enclosing_func_qn[0]) {
        node = cbm_gbuf_find_by_qn(ctx->gbuf, ch->enclosing_func_qn);
    }
    if (!node) {
        char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
        node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
        free(file_qn);
    }
    return node;
}

static void create_channel_edges_for_file(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                          const char *rel) {
    for (int j = 0; j < result->channels.count; j++) {
        const CBMChannel *ch = &result->channels.items[j];
        if (!ch->channel_name || !ch->channel_name[0]) {
            continue;
        }
        char channel_qn[CBM_SZ_512];
        snprintf(channel_qn, sizeof(channel_qn), "__channel__%s__%s",
                 ch->transport ? ch->transport : "unknown", ch->channel_name);
        char channel_props[CBM_SZ_512];
        snprintf(channel_props, sizeof(channel_props), "{\"transport\":\"%s\",\"name\":\"%s\"}",
                 ch->transport ? ch->transport : "unknown", ch->channel_name);
        int64_t channel_id = cbm_gbuf_upsert_node(ctx->gbuf, "Channel", ch->channel_name,
                                                  channel_qn, "", 0, 0, channel_props);

        const cbm_gbuf_node_t *src_node = find_channel_source(ctx, ch, rel);
        if (src_node && channel_id > 0) {
            const char *edge_type = ch->direction == CBM_CHANNEL_EMIT ? "EMITS" : "LISTENS_ON";
            char edge_props[CBM_SZ_128];
            snprintf(edge_props, sizeof(edge_props), "{\"transport\":\"%s\"}",
                     ch->transport ? ch->transport : "unknown");
            cbm_gbuf_insert_edge(ctx->gbuf, src_node->id, channel_id, edge_type, edge_props);
        }
    }
}

/* Create IMPORTS edges for one file's imports.  Mirrors the resolution
 * logic in pass_parallel.c register_and_link_def — keep the two in sync. */
static int create_import_edges_for_file(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                        const char *rel) {
    int count = 0;
    for (int j = 0; j < result->imports.count; j++) {
        const CBMImport *imp = &result->imports.items[j];
        if (!imp->module_path) {
            continue;
        }
        char *target_qn = NULL;
        char *resolved = cbm_pipeline_resolve_relative_import(rel, imp->module_path);
        if (resolved) {
            target_qn = cbm_pipeline_fqn_module(ctx->project_name, resolved);
            free(resolved);
        } else {
            target_qn = cbm_pipeline_fqn_module(ctx->project_name, imp->module_path);
        }
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
        char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
        const cbm_gbuf_node_t *source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
        if (source_node && target) {
            char imp_props[CBM_SZ_256];
            snprintf(imp_props, sizeof(imp_props), "{\"local_name\":\"%s\"}",
                     imp->local_name ? imp->local_name : "");
            cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, target->id, "IMPORTS", imp_props);
            count++;
        }
        free(target_qn);
        free(file_qn);
    }
    return count;
}

int cbm_pipeline_pass_definitions(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count) {
    cbm_log_info("pass.start", "pass", "definitions", "files", itoa_log(file_count));

    /* Ensure extraction library is initialized */
    cbm_init();

    int total_defs = 0;
    int total_calls = 0;
    int total_imports = 0;
    int errors = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return CBM_NOT_FOUND;
        }

        const char *path = files[i].path;
        const char *rel = files[i].rel_path;
        CBMLanguage lang = files[i].language;

        /* Read source file */
        int source_len = 0;
        char *source = read_file(path, &source_len);
        if (!source) {
            errors++;
            continue;
        }

        /* Extract */
        CBMFileResult *result =
            cbm_extract_file(source, source_len, lang, ctx->project_name, rel, CBM_EXTRACT_BUDGET,
                             NULL, NULL /* no extra defines or include paths */
            );
        free(source);

        if (!result) {
            errors++;
            continue;
        }

        /* Create nodes for each definition */
        for (int d = 0; d < result->defs.count; d++) {
            process_def(ctx, &result->defs.items[d], rel);
            total_defs++;
        }

        /* Store calls for pass_calls (we save them in the extraction results
         * for now — a future optimization would batch these) */
        total_calls += result->calls.count;
        total_imports += create_import_edges_for_file(ctx, result, rel);
        create_channel_edges_for_file(ctx, result, rel);

        /* Cache or free the extraction result */
        if (ctx->result_cache) {
            ctx->result_cache[i] = result;
        } else {
            cbm_free_result(result);
        }
    }

    cbm_log_info("pass.done", "pass", "definitions", "defs", itoa_log(total_defs), "calls",
                 itoa_log(total_calls), "imports", itoa_log(total_imports), "errors",
                 itoa_log(errors));
    return 0;
}
