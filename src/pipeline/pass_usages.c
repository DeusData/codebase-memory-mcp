/*
 * pass_usages.c — Resolve usages, throws, and read/write edges.
 *
 * For each file, re-extracts and resolves:
 *   - USAGE edges: identifier references (not calls) to registered symbols
 *   - THROWS/RAISES edges: exception types
 *   - READS/WRITES edges: variable read/write access patterns
 *
 * All three use the same registry lookup strategy. Combined into one pass
 * to avoid triple re-extraction.
 *
 * Depends on: pass_definitions having populated the registry and graph buffer
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

/* Read file into heap buffer. Caller must free(). */
static char *read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > (long)100 * 1024 * 1024) {
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

static const char *itoa_log(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Check if an exception name is a "checked" exception (Java-style).
 * Checked: Exception, IOException, etc. (extends Exception, not RuntimeException).
 * Simple heuristic: if name contains "Error" or "Panic", it's a runtime exception. */
static bool is_checked_exception(const char *name) {
    if (!name) {
        return false;
    }
    if (strstr(name, "Error") || strstr(name, "Panic") || strstr(name, "error") ||
        strstr(name, "panic")) {
        return false;
    }
    return true; /* Default: treat as checked */
}

/* Build per-file import map from cached extraction result or graph buffer edges. */
static int build_import_map(cbm_pipeline_ctx_t *ctx, const char *rel_path,
                            // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                            const CBMFileResult *result, const char ***out_keys,
                            const char ***out_vals, int *out_count) {
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;

    /* Fast path: build from cached extraction result (no JSON parsing) */
    if (result && result->imports.count > 0) {
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        const char **keys = calloc((size_t)result->imports.count, sizeof(const char *));
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        const char **vals = calloc((size_t)result->imports.count, sizeof(const char *));
        int count = 0;

        for (int i = 0; i < result->imports.count; i++) {
            const CBMImport *imp = &result->imports.items[i];
            if (!imp->local_name || !imp->local_name[0] || !imp->module_path) {
                continue;
            }
            char *target_qn = cbm_pipeline_resolve_module(ctx, imp->module_path);
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
            free(target_qn);
            if (!target) {
                continue;
            }
            // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
            keys[count] = strdup(imp->local_name);
            vals[count] = target->qualified_name;
            count++;
        }

        *out_keys = keys;
        *out_vals = vals;
        *out_count = count;
        return 0;
    }

    /* Slow path: scan graph buffer IMPORTS edges + parse JSON properties */
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
    free(file_qn);
    if (!file_node) {
        return 0;
    }

    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    int rc = cbm_gbuf_find_edges_by_source_type(ctx->gbuf, file_node->id, "IMPORTS", &edges,
                                                &edge_count);
    if (rc != 0 || edge_count == 0) {
        return 0;
    }

    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    const char **keys = calloc(edge_count, sizeof(const char *));
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    const char **vals = calloc(edge_count, sizeof(const char *));
    int count = 0;

    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *e = edges[i];
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(ctx->gbuf, e->target_id);
        if (!target) {
            continue;
        }

        if (e->properties_json) {
            const char *start = strstr(e->properties_json, "\"local_name\":\"");
            if (start) {
                start += strlen("\"local_name\":\"");
                const char *end = strchr(start, '"');
                if (end && end > start) {
                    // NOLINTNEXTLINE(misc-include-cleaner) — strndup provided by standard header
                    keys[count] = cbm_strndup(start, end - start);
                    vals[count] = target->qualified_name;
                    count++;
                }
            }
        }
    }

    *out_keys = keys;
    *out_vals = vals;
    *out_count = count;
    return 0;
}

static void free_import_map(const char **keys, const char **vals, int count) {
    if (keys) {
        for (int i = 0; i < count; i++) {
            free((void *)keys[i]);
        }
        free((void *)keys);
    }
    if (vals) {
        free((void *)vals);
    }
}

/* Find the graph buffer node for an enclosing function QN, falling back to file node. */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static const cbm_gbuf_node_t *find_enclosing_node(cbm_pipeline_ctx_t *ctx, const char *func_qn,
                                                  const char *rel_path) {
    const cbm_gbuf_node_t *node = NULL;
    if (func_qn && func_qn[0]) {
        node = cbm_gbuf_find_by_qn(ctx->gbuf, func_qn);
    }
    if (!node) {
        char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
        node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
        free(file_qn);
    }
    return node;
}

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
int cbm_pipeline_pass_usages(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                             int file_count) {
    cbm_log_info("pass.start", "pass", "usages", "files", itoa_log(file_count));

    int total_usages = 0;
    int usage_resolved = 0;
    int total_throws = 0;
    int throw_resolved = 0;
    int total_rw = 0;
    int rw_resolved = 0;
    int errors = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return -1;
        }

        const char *path = files[i].path;
        const char *rel = files[i].rel_path;

        /* Use cached extraction result or re-extract */
        CBMFileResult *result = NULL;
        bool result_owned = false;
        if (ctx->result_cache) {
            result = ctx->result_cache[i];
        }
        if (!result) {
            CBMLanguage lang = files[i].language;
            int source_len = 0;
            char *source = read_file(path, &source_len);
            if (!source) {
                errors++;
                continue;
            }
            result = cbm_extract_file(source, source_len, lang, ctx->project_name, rel,
                                      CBM_EXTRACT_BUDGET, NULL, NULL);
            free(source);
            if (!result) {
                errors++;
                continue;
            }
            result_owned = true;
        }

        /* Skip files with no usages/throws/rw */
        if (result->usages.count == 0 && result->throws.count == 0 && result->rw.count == 0) {
            if (result_owned) {
                cbm_free_result(result);
            }
            continue;
        }

        /* Build import map */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, rel, result, &imp_keys, &imp_vals, &imp_count);

        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, rel);

        /* ── USAGE edges ────────────────────────────────────────── */
        for (int u = 0; u < result->usages.count; u++) {
            CBMUsage *usage = &result->usages.items[u];
            if (!usage->ref_name) {
                continue;
            }
            total_usages++;

            const cbm_gbuf_node_t *src = find_enclosing_node(ctx, usage->enclosing_func_qn, rel);
            if (!src) {
                continue;
            }

            cbm_resolution_t res = cbm_registry_resolve(ctx->registry, usage->ref_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            {
                char uprops[256];
                snprintf(uprops, sizeof(uprops), "{\"callee\":\"%s\"}", usage->ref_name);
                cbm_gbuf_insert_edge(ctx->gbuf, src->id, tgt->id, "USAGE", uprops);
            }
            usage_resolved++;
        }

        /* ── THROWS / RAISES edges ──────────────────────────────── */
        for (int t = 0; t < result->throws.count; t++) {
            CBMThrow *thr = &result->throws.items[t];
            if (!thr->exception_name || !thr->enclosing_func_qn) {
                continue;
            }
            total_throws++;

            const cbm_gbuf_node_t *src = find_enclosing_node(ctx, thr->enclosing_func_qn, rel);
            if (!src) {
                continue;
            }

            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            const char *edge_type = is_checked_exception(thr->exception_name) ? "THROWS" : "RAISES";

            /* Try to resolve exception class */
            cbm_resolution_t res = cbm_registry_resolve(ctx->registry, thr->exception_name,
                                                        module_qn, imp_keys, imp_vals, imp_count);

            const cbm_gbuf_node_t *tgt = NULL;
            if (res.qualified_name && res.qualified_name[0]) {
                tgt = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
            }
            if (!tgt) {
                continue; /* Exception class not in graph — skip */
            }
            if (src->id == tgt->id) {
                continue;
            }

            cbm_gbuf_insert_edge(ctx->gbuf, src->id, tgt->id, edge_type, "{}");
            throw_resolved++;
        }

        /* ── READS / WRITES edges ──────────────────────────────── */
        for (int r = 0; r < result->rw.count; r++) {
            CBMReadWrite *rw = &result->rw.items[r];
            if (!rw->var_name) {
                continue;
            }
            total_rw++;

            const cbm_gbuf_node_t *src = find_enclosing_node(ctx, rw->enclosing_func_qn, rel);
            if (!src) {
                continue;
            }

            cbm_resolution_t res = cbm_registry_resolve(ctx->registry, rw->var_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);
            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                continue;
            }

            const cbm_gbuf_node_t *tgt = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
            if (!tgt || src->id == tgt->id) {
                continue;
            }

            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            const char *edge_type = rw->is_write ? "WRITES" : "READS";
            cbm_gbuf_insert_edge(ctx->gbuf, src->id, tgt->id, edge_type, "{}");
            rw_resolved++;
        }

        free(module_qn);
        free_import_map(imp_keys, imp_vals, imp_count);
        if (result_owned) {
            cbm_free_result(result);
        }
    }

    cbm_log_info("pass.done", "pass", "usages", "usage", itoa_log(usage_resolved), "throws",
                 itoa_log(throw_resolved), "rw", itoa_log(rw_resolved), "errors", itoa_log(errors));
    /* Suppress unused-but-set for totals — included for future debug logging */
    (void)total_usages;
    (void)total_throws;
    (void)total_rw;
    return 0;
}
