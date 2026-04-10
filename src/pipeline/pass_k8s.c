/*
 * pass_k8s.c — Pipeline pass for Kubernetes manifest and Kustomize overlay processing.
 *
 * For each discovered YAML file:
 *   1. Check if it is a kustomize overlay (kustomization.yaml / kustomization.yml)
 *      → emit a Module node and IMPORTS edges for each resources/bases/patches entry
 *   2. Else if it is a generic k8s manifest (apiVersion: detected)
 *      → emit one Resource node per file (first document only — multi-document YAML is not yet
 * supported)
 *
 * Depends on: pass_infrascan.c (cbm_is_kustomize_file, cbm_is_k8s_manifest, cbm_infra_qn),
 *             extraction layer (cbm.h), graph_buffer, pipeline internals.
 */
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "cbm.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>

/* ── Internal helpers ────────────────────────────────────────────── */

/* Read entire file into heap-allocated buffer. Returns NULL on error.
 * Caller must free(). Sets *out_len to byte count. */
static char *k8s_read_file(const char *path, int *out_len) {
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

/* Format int to string for logging. Thread-safe via TLS. */
static const char *itoa_k8s(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Extract the basename of a path (pointer into the string; no allocation). */
static const char *k8s_basename(const char *path) {
    const char *p = strrchr(path, '/');
    return p ? p + 1 : path;
}

/* ── Kustomize handler ───────────────────────────────────────────── */

static void handle_kustomize(cbm_pipeline_ctx_t *ctx, const char *path, const char *rel_path,
                             CBMFileResult *result) {
    /* Emit Module node for this kustomize overlay file */
    char *mod_qn = cbm_infra_qn(ctx->project_name, rel_path, "kustomize", NULL);
    if (!mod_qn) {
        return;
    }

    // NOLINTNEXTLINE(misc-include-cleaner)
    int64_t mod_id = cbm_gbuf_upsert_node(ctx->gbuf, "Module", k8s_basename(rel_path), mod_qn,
                                          rel_path, 1, 0, "{\"source\":\"kustomize\"}");
    free(mod_qn);

    if (mod_id <= 0) {
        return;
    }

    /* If we have a cached extraction result, emit IMPORTS edges for
     * resources/bases/patches/components entries */
    int import_count = 0;
    CBMFileResult *res = result;
    bool allocated = false;

    if (!res) {
        /* Fall back to re-extraction */
        int src_len = 0;
        char *source = k8s_read_file(path, &src_len);
        if (source) {
            res = cbm_extract_file(source, src_len, CBM_LANG_KUSTOMIZE, ctx->project_name, rel_path,
                                   CBM_EXTRACT_BUDGET, NULL, NULL);
            free(source);
            allocated = true;
        }
    }

    if (res) {
        for (int j = 0; j < res->imports.count; j++) {
            CBMImport *imp = &res->imports.items[j];
            if (!imp->module_path) {
                continue;
            }

            /* Compute target file QN */
            char *target_qn =
                cbm_pipeline_fqn_compute(ctx->project_name, imp->module_path, "__file__");
            if (!target_qn) {
                continue;
            }

            const cbm_gbuf_node_t *target = cbm_gbuf_find_by_qn(ctx->gbuf, target_qn);
            free(target_qn);

            if (target) {
                cbm_gbuf_insert_edge(ctx->gbuf, mod_id, target->id, "IMPORTS",
                                     "{\"via\":\"kustomize\"}");
                import_count++;
            }
        }

        if (allocated) {
            cbm_free_result(res);
        }
    }

    cbm_log_info("pass.k8s.kustomize", "file", rel_path, "imports", itoa_k8s(import_count));
}

/* ── K8s manifest handler ────────────────────────────────────────── */

/* source/src_len are the already-read file bytes (caller retains ownership and
 * must free after this call returns). */
static void handle_k8s_manifest(cbm_pipeline_ctx_t *ctx, const char *path, const char *rel_path,
                                const char *source, int src_len) {
    (void)path; /* retained for symmetry; source is always provided now */
    int resource_count = 0;

    CBMFileResult *res = cbm_extract_file(source, src_len, CBM_LANG_K8S, ctx->project_name,
                                          rel_path, CBM_EXTRACT_BUDGET, NULL, NULL);
    if (!res) {
        return;
    }

    /* Compute file node QN for DEFINES edges */
    char *file_qn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
    const cbm_gbuf_node_t *file_node = file_qn ? cbm_gbuf_find_by_qn(ctx->gbuf, file_qn) : NULL;
    free(file_qn);

    for (int d = 0; d < res->defs.count; d++) {
        CBMDefinition *def = &res->defs.items[d];
        if (!def->label || strcmp(def->label, "Resource") != 0) {
            continue;
        }
        if (!def->name || !def->qualified_name) {
            continue;
        }

        // NOLINTNEXTLINE(misc-include-cleaner)
        int64_t node_id =
            cbm_gbuf_upsert_node(ctx->gbuf, "Resource", def->name, def->qualified_name, rel_path,
                                 (int)def->start_line, (int)def->end_line, "{\"source\":\"k8s\"}");

        /* DEFINES edge: File → Resource */
        if (file_node && node_id > 0) {
            cbm_gbuf_insert_edge(ctx->gbuf, file_node->id, node_id, "DEFINES", "{}");
        }

        resource_count++;
    }

    cbm_free_result(res);

    cbm_log_info("pass.k8s.manifest", "file", rel_path, "resources", itoa_k8s(resource_count));
}

/* ── Pass entry point ────────────────────────────────────────────── */

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
int cbm_pipeline_pass_k8s(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count) {
    cbm_log_info("pass.start", "pass", "k8s", "files", itoa_k8s(file_count));

    cbm_init();

    int kustomize_count = 0;
    int manifest_count = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return -1;
        }

        const char *path = files[i].path;
        const char *rel = files[i].rel_path;
        CBMLanguage lang = files[i].language;
        const char *base = k8s_basename(rel);

        CBMFileResult *cached =
            (ctx->result_cache && ctx->result_cache[i]) ? ctx->result_cache[i] : NULL;

        if (cbm_is_kustomize_file(base)) {
            handle_kustomize(ctx, path, rel, cached);
            kustomize_count++;
        } else if (lang == CBM_LANG_YAML || lang == CBM_LANG_K8S) {
            /* Read source once to classify (and reuse for uncached extraction). */
            int src_len = 0;
            char *source = k8s_read_file(path, &src_len);
            if (source) {
                if (cbm_is_k8s_manifest(base, source)) {
                    /* Always re-extract with CBM_LANG_K8S regardless of any cached
                     * result: cached results were produced during the parallel YAML
                     * pass and contain no "Resource" definitions.  Pass the already-
                     * read source buffer so handle_k8s_manifest does not re-read. */
                    (void)cached; /* cached YAML result intentionally discarded */
                    handle_k8s_manifest(ctx, path, rel, source, src_len);
                    manifest_count++;
                }
                free(source);
            }
        }
    }

    cbm_log_info("pass.done", "pass", "k8s", "kustomize", itoa_k8s(kustomize_count), "manifests",
                 itoa_k8s(manifest_count));
    return 0;
}
