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
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "cbm.h"
#include "simhash/minhash.h"

#include <stdbool.h>
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

static bool fp_ends_with(const char *fp, const char *suffix) {
    if (!fp || !suffix) {
        return false;
    }
    size_t fplen = strlen(fp);
    size_t sflen = strlen(suffix);
    return fplen >= sflen && strcmp(fp + fplen - sflen, suffix) == 0;
}

static const char *module_name_from_path(const char *path) {
    if (!path || !path[0]) {
        return "module";
    }
    const char *base = strrchr(path, '/');
    base = base ? base + 1 : path;
    static CBM_TLS char name[256];
    snprintf(name, sizeof(name), "%s", base);
    char *dot = strrchr(name, '.');
    if (dot) {
        *dot = '\0';
    }
    return name;
}

static bool repo_file_exists(const cbm_pipeline_ctx_t *ctx, const char *rel_path) {
    if (!ctx || !ctx->repo_path || !rel_path || !rel_path[0]) {
        return false;
    }

    char full_path[1024];
    if (snprintf(full_path, sizeof(full_path), "%s/%s", ctx->repo_path, rel_path) >=
        (int)sizeof(full_path)) {
        return false;
    }
    return cbm_file_exists(full_path);
}

static bool normalize_module_path(const char *module_path, char *out, size_t out_size) {
    if (!module_path || !out || out_size == 0) {
        return false;
    }

    const char *start = module_path;
    if (strncmp(start, "res://", 6) == 0) {
        start += 6;
    }
    if (strncmp(start, "./", 2) == 0) {
        start += 2;
    }

    if (snprintf(out, out_size, "%s", start) >= (int)out_size) {
        return false;
    }

    return out[0] != '\0';
}

static bool resolve_relative_module_path(const char *source_rel_path, const char *module_path,
                                        char *out, size_t out_size) {
    if (!source_rel_path || !module_path || !out || out_size == 0) {
        return false;
    }

    const char *slash = strrchr(source_rel_path, '/');
    if (!slash) {
        return false;
    }

    size_t dir_len = (size_t)(slash - source_rel_path);
    if (dir_len == 0) {
        return false;
    }

    if (snprintf(out, out_size, "%.*s/%s", (int)dir_len, source_rel_path, module_path) >=
        (int)out_size) {
        return false;
    }

    return out[0] != '\0';
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
    if (node_id > 0 && def->label &&
        (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0 ||
         strcmp(def->label, "Class") == 0)) {
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

/* Create IMPORTS edges for one file's imports. */
static int create_import_edges_for_file(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                        const char *rel) {
    int count = 0;
    for (int j = 0; j < result->imports.count; j++) {
        const CBMImport *imp = &result->imports.items[j];
        if (!imp->module_path) {
            continue;
        }

        char normalized_path[CBM_SZ_1K] = {0};
        if (!normalize_module_path(imp->module_path, normalized_path, sizeof(normalized_path))) {
            continue;
        }

        bool exists = repo_file_exists(ctx, normalized_path);
        char resolved_path[CBM_SZ_1K] = {0};

        if (!exists && fp_ends_with(normalized_path, ".gd")) {
            if (resolve_relative_module_path(rel, normalized_path, resolved_path,
                                            sizeof(resolved_path)) &&
                repo_file_exists(ctx, resolved_path)) {
                memcpy(normalized_path, resolved_path, sizeof(normalized_path));
                exists = true;
            }
        }

        char *target_qn = cbm_pipeline_fqn_module(ctx->project_name, normalized_path);
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);

        if (fp_ends_with(normalized_path, ".gd") && !exists) {
            free(target_qn);
            continue;
        }

        if (!target && fp_ends_with(normalized_path, ".gd")) {
            int64_t target_id = cbm_gbuf_upsert_node(
                ctx->gbuf, "Module", module_name_from_path(normalized_path), target_qn,
                normalized_path, 0, 0, "{}");
            if (target_id > 0) {
                target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
            }
        }

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
