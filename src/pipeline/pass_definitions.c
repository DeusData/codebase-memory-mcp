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
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "cbm.h"

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

    if (size <= 0 || size > (long)100 * 1024 * 1024) { /* 100 MB sanity limit */
        (void)fclose(f);
        return NULL;
    }

    char *buf = malloc(size + 1);
    if (!buf) {
        (void)fclose(f);
        return NULL;
    }

    size_t nread = fread(buf, 1, size, f);
    (void)fclose(f);

    // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
    buf[nread] = '\0';
    *out_len = (int)nread;
    return buf;
}

/* Format int to string for logging. Thread-safe via TLS. */
static const char *itoa_log(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Append a JSON-escaped string value to buf at position *pos.
 * Writes: ,"key":"escaped_value"
 * Handles: \, ", \n, \r, \t */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static void append_json_string(char *buf, size_t bufsize, size_t *pos, const char *key,
                               const char *val) {
    if (!val || !val[0]) {
        return;
    }
    if (*pos >= bufsize - 10) {
        return;
    }

    size_t p = *pos;
    /* ,\"key\":\" */
    int w = snprintf(buf + p, bufsize - p, ",\"%s\":\"", key);
    if (w <= 0 || (size_t)w >= bufsize - p) {
        return;
    }
    p += (size_t)w;

    for (const char *s = val; *s && p < bufsize - 3; s++) {
        switch (*s) {
        case '"':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = '"';
            }
            break;
        case '\\':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = '\\';
            }
            break;
        case '\n':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 'n';
            }
            break;
        case '\r':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 'r';
            }
            break;
        case '\t':
            buf[p++] = '\\';
            if (p < bufsize - 2) {
                buf[p++] = 't';
            }
            break;
        default:
            buf[p++] = *s;
            break;
        }
    }
    if (p < bufsize - 1) {
        buf[p++] = '"';
    }
    buf[p] = '\0';
    *pos = p;
}

/* Append a JSON array of strings: ,"key":["a","b","c"] */
static void append_json_str_array(char *buf, size_t bufsize, size_t *pos, const char *key,
                                  const char **arr) {
    if (!arr || !arr[0] || *pos >= bufsize - 10) {
        return;
    }
    size_t p = *pos;
    int n = snprintf(buf + p, bufsize - p, ",\"%s\":[", key);
    if (n <= 0 || p + (size_t)n >= bufsize - 2) {
        return;
    }
    p += (size_t)n;
    for (int i = 0; arr[i]; i++) {
        if (i > 0 && p < bufsize - 1) {
            buf[p++] = ',';
        }
        if (p < bufsize - 1) {
            buf[p++] = '"';
        }
        for (const char *s = arr[i]; *s && p < bufsize - 2; s++) {
            if (*s == '"' || *s == '\\') {
                buf[p++] = '\\';
                if (p >= bufsize - 2) {
                    break;
                }
            }
            buf[p++] = *s;
        }
        if (p < bufsize - 1) {
            buf[p++] = '"';
        }
    }
    if (p < bufsize - 1) {
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

    if (pos < bufsize - 1) {
        buf[pos] = '}';
        buf[pos + 1] = '\0';
    }
}

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
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
            return -1;
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
            CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name || !def->name) {
                continue;
            }

            char props[2048];
            build_def_props(props, sizeof(props), def);

            // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
            int64_t node_id =
                cbm_gbuf_upsert_node(ctx->gbuf, def->label ? def->label : "Function", def->name,
                                     def->qualified_name, def->file_path ? def->file_path : rel,
                                     (int)def->start_line, (int)def->end_line, props);

            /* Register callable symbols in the registry */
            if (node_id > 0 && def->label &&
                (strcmp(def->label, "Function") == 0 || strcmp(def->label, "Method") == 0 ||
                 strcmp(def->label, "Class") == 0)) {
                cbm_registry_add(ctx->registry, def->name, def->qualified_name, def->label);
            }

            /* DEFINES edge: File node → Definition node */
            const char *file_qn_name = "__file__";
            char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, file_qn_name);
            const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
            if (file_node && node_id > 0) {
                cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, node_id, "DEFINES", "{}");
            }
            free(file_qn);

            /* DEFINES_METHOD edge: Class → Method */
            if (def->parent_class && def->label && strcmp(def->label, "Method") == 0) {
                const cbm_gbuf_node_t *parent = cbm_gbuf_find_by_qn(ctx->gbuf, def->parent_class);
                if (parent && node_id > 0) {
                    cbm_gbuf_insert_edge(ctx->gbuf, parent->id, node_id, "DEFINES_METHOD", "{}");
                }
            }

            total_defs++;
        }

        /* Store calls for pass_calls (we save them in the extraction results
         * for now — a future optimization would batch these) */
        total_calls += result->calls.count;
        total_imports += result->imports.count;

        /* Store per-file import map for later use by pass_calls.
         * For each import, create an IMPORTS edge: File → imported module. */
        for (int j = 0; j < result->imports.count; j++) {
            CBMImport *imp = &result->imports.items[j];
            if (!imp->module_path) {
                continue;
            }

            /* Find or create the target module node */
            char *target_qn = cbm_pipeline_resolve_module(ctx, imp->module_path);
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);

            char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
            const cbm_gbuf_node_t *source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);

            if (source_node && target) {
                char imp_props[256];
                snprintf(imp_props, sizeof(imp_props), "{\"local_name\":\"%s\"}",
                         imp->local_name ? imp->local_name : "");
                cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, target->id, "IMPORTS", imp_props);
            }
            free(target_qn);
            free(file_qn);
        }

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
