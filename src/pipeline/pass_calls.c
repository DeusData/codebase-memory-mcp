/*
 * pass_calls.c — Resolve function/method calls into CALLS edges.
 *
 * For each discovered file:
 *   1. Re-extract calls (cbm_extract_file)
 *   2. Build per-file import map from IMPORTS edges in graph buffer
 *   3. Resolve each call via registry (import_map → same_module → unique → suffix)
 *   4. Create CALLS edges in graph buffer with confidence/strategy properties
 *
 * Depends on: pass_definitions having populated the registry and graph buffer
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "cbm.h"

#include "foundation/compat_regex.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Read entire file into heap-allocated buffer. Caller must free(). */
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

/* Format int for logging. Thread-safe via TLS. */
static const char *itoa_log(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Build per-file import map from cached extraction result or graph buffer edges.
 * Returns parallel arrays of (local_name, module_qn) pairs. Caller frees. */
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
            char *target_qn = cbm_pipeline_fqn_module(ctx->project_name, imp->module_path);
            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
            free(target_qn);
            if (!target) {
                continue;
            }
            // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
            keys[count] = strdup(imp->local_name);
            vals[count] = target->qualified_name; /* borrowed from gbuf */
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
                    char *key = cbm_strndup(start, end - start);
                    keys[count] = key;
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

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
int cbm_pipeline_pass_calls(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "calls", "files", itoa_log(file_count));

    int total_calls = 0;
    int resolved = 0;
    int unresolved = 0;
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

        if (result->calls.count == 0) {
            if (result_owned) {
                cbm_free_result(result);
            }
            continue;
        }

        /* Build import map for this file */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, rel, result, &imp_keys, &imp_vals, &imp_count);

        /* Compute module QN for same-module resolution */
        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, rel);

        /* Resolve each call */
        for (int c = 0; c < result->calls.count; c++) {
            CBMCall *call = &result->calls.items[c];
            if (!call->callee_name) {
                continue;
            }

            total_calls++;

            /* Find enclosing function node (source of CALLS edge).
             * Resolution chain: exact QN → name+file filter → module fallback.
             * Each step uses O(1) hash table lookup. */
            const cbm_gbuf_node_t *source_node = NULL;
            if (call->enclosing_func_qn) {
                source_node = cbm_gbuf_find_by_qn(ctx->gbuf, call->enclosing_func_qn);
            }
            /* B2/B5: Name-based fallback when exact QN mismatches.
             * Uses DRY shared helper — O(1) hash + O(k) filter. */
            if (!source_node && call->enclosing_func_qn) {
                static const char *callable_labels[] = {"Function", "Method"};
                source_node = cbm_gbuf_resolve_by_name_in_file(
                    ctx->gbuf, call->enclosing_func_qn, rel, callable_labels, 2);
            }
            if (!source_node) {
                /* Module-level fallback: file node as source */
                char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel, "__file__");
                source_node = cbm_gbuf_find_by_qn(ctx->gbuf, file_qn);
                free(file_qn);
            }
            if (!source_node) {
                unresolved++;
                continue;
            }

            /* Resolve callee through registry */
            cbm_resolution_t res = cbm_registry_resolve(ctx->registry, call->callee_name, module_qn,
                                                        imp_keys, imp_vals, imp_count);

            if (!res.qualified_name || res.qualified_name[0] == '\0') {
                unresolved++;
                continue;
            }

            /* Find target node in graph buffer */
            const cbm_gbuf_node_t *target_node = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
            if (!target_node) {
                unresolved++;
                continue;
            }

            /* Skip self-calls */
            if (source_node->id == target_node->id) {
                continue;
            }

            /* Create CALLS edge with confidence + strategy properties */
            char props[256];
            snprintf(props, sizeof(props),
                     "{\"confidence\":%.2f,\"strategy\":\"%s\",\"candidates\":%d}", res.confidence,
                     res.strategy ? res.strategy : "unknown", res.candidate_count);

            cbm_gbuf_insert_edge(ctx->gbuf, source_node->id, target_node->id, "CALLS", props);
            resolved++;
        }

        free(module_qn);
        free_import_map(imp_keys, imp_vals, imp_count);
        if (result_owned) {
            cbm_free_result(result);
        }
    }

    cbm_log_info("pass.done", "pass", "calls", "total", itoa_log(total_calls), "resolved",
                 itoa_log(resolved), "unresolved", itoa_log(unresolved), "errors",
                 itoa_log(errors));

    /* Additional pattern-based edge passes run after normal call resolution */
    cbm_pipeline_pass_fastapi_depends(ctx, files, file_count);

    return 0;
}

/* ── FastAPI Depends() tracking ──────────────────────────────────── */
/* Scans Python function signatures for Depends(func_ref) patterns and
 * creates CALLS edges from the endpoint to the dependency function.
 * Without this, FastAPI auth/DI functions appear as dead code (in_degree=0). */

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
void cbm_pipeline_pass_fastapi_depends(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                       int file_count) {
    cbm_regex_t depends_re;
    if (cbm_regcomp(&depends_re, "Depends\\(([A-Za-z_][A-Za-z0-9_.]*)", CBM_REG_EXTENDED) != 0) {
        return;
    }

    int edge_count = 0;
    for (int i = 0; i < file_count; i++) {
        if (files[i].language != CBM_LANG_PYTHON) {
            continue;
        }
        if (cbm_pipeline_check_cancel(ctx)) {
            break;
        }

        /* Check if file has Depends call in cached extraction */
        CBMFileResult *result = ctx->result_cache ? ctx->result_cache[i] : NULL;
        if (!result) {
            continue;
        }
        bool has_depends = false;
        for (int c = 0; c < result->calls.count; c++) {
            if (result->calls.items[c].callee_name &&
                strcmp(result->calls.items[c].callee_name, "Depends") == 0) {
                has_depends = true;
                break;
            }
        }
        if (!has_depends) {
            continue;
        }

        /* Read source and scan for Depends(func_ref) in function signatures */
        int source_len = 0;
        char *source = read_file(files[i].path, &source_len);
        if (!source) {
            continue;
        }

        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, files[i].rel_path);

        /* Build import map for alias resolution */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, files[i].rel_path, result, &imp_keys, &imp_vals, &imp_count);

        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name || def->start_line == 0) {
                continue;
            }
            if (strcmp(def->label, "Function") != 0 && strcmp(def->label, "Method") != 0) {
                continue;
            }

            /* Extract function signature (def line through ~15 lines for multi-line sigs) */
            int sig_end_line = (int)def->start_line + 15;
            if (def->end_line > 0 && sig_end_line > (int)def->end_line) {
                sig_end_line = (int)def->end_line;
            }

            /* Find signature region in source */
            const char *p = source;
            int line = 1;
            while (*p && line < def->start_line) {
                if (*p == '\n') {
                    line++;
                }
                p++;
            }
            const char *sig_start = p;
            while (*p && line < sig_end_line) {
                if (*p == '\n') {
                    line++;
                }
                p++;
                /* Stop at closing paren + colon (end of Python signature) */
                if (p > sig_start + 1 && p[-1] == ':' && p[-2] == ')') {
                    break;
                }
            }
            size_t sig_len = (size_t)(p - sig_start);
            char *sig = malloc(sig_len + 1);
            if (!sig) {
                continue;
            }
            memcpy(sig, sig_start, sig_len);
            sig[sig_len] = '\0';

            /* Match Depends(func_ref) patterns */
            cbm_regmatch_t match[2];
            const char *scan = sig;
            while (cbm_regexec(&depends_re, scan, 2, match, 0) == 0) {
                int ref_len = match[1].rm_eo - match[1].rm_so;
                char func_ref[256];
                if (ref_len >= (int)sizeof(func_ref)) {
                    ref_len = (int)sizeof(func_ref) - 1;
                }
                memcpy(func_ref, scan + match[1].rm_so, (size_t)ref_len);
                func_ref[ref_len] = '\0';

                /* Resolve through registry */
                cbm_resolution_t res = cbm_registry_resolve(ctx->registry, func_ref, module_qn,
                                                            imp_keys, imp_vals, imp_count);
                if (res.qualified_name && res.qualified_name[0] != '\0') {
                    const cbm_gbuf_node_t *src_node =
                        cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
                    const cbm_gbuf_node_t *tgt_node =
                        cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
                    if (src_node && tgt_node && src_node->id != tgt_node->id) {
                        cbm_gbuf_insert_edge(ctx->gbuf, src_node->id, tgt_node->id, "CALLS",
                                             "{\"confidence\":0.95,\"strategy\":\"fastapi_depends\""
                                             "}");
                        edge_count++;
                    }
                }
                scan += match[0].rm_eo;
            }
            free(sig);
        }

        free(module_qn);
        free_import_map(imp_keys, imp_vals, imp_count);
        free(source);
    }

    cbm_regfree(&depends_re);
    if (edge_count > 0) {
        cbm_log_info("pass.fastapi_depends", "edges", itoa_log(edge_count));
    }
}

/* DLL resolve tracking removed — triggered Windows Defender false positive.
 * See issue #89. */
