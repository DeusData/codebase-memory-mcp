/*
 * pass_semantic.c — Semantic edge passes: INHERITS, DECORATES, IMPLEMENTS.
 *
 * Operates on already-extracted nodes in the graph buffer:
 *   - INHERITS: Class/Interface → base class (from base_classes in extraction)
 *   - DECORATES: Function/Method → decorator function (from decorators in extraction)
 *   - IMPLEMENTS: Struct/Class → Interface (Go implicit + explicit base_classes + Rust impl)
 *
 * These passes re-extract files to access base_classes, decorators, and impl_traits data
 * since that information is in CBMDefinition fields, not stored in node properties JSON.
 *
 * Depends on: pass_definitions having populated the registry and graph buffer
 */
#include "foundation/constants.h"
#include "pipeline/pipeline.h"
#include <stdint.h>
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "cbm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *read_file(const char *path, int *out_len) {
    FILE *f = fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    (void)fseek(f, 0, SEEK_END);
    long size = ftell(f);
    (void)fseek(f, 0, SEEK_SET);
    if (size <= 0 || size > (long)CBM_PERCENT * CBM_SZ_1K * CBM_SZ_1K) {
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

static const char *itoa_log(int val) {
    enum { RING_BUF_COUNT = 4, RING_BUF_MASK = 3 };
    static CBM_TLS char bufs[RING_BUF_COUNT][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + SKIP_ONE) & RING_BUF_MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Build per-file import map from cached extraction result or graph buffer edges. */
static int build_import_map(cbm_pipeline_ctx_t *ctx, const char *rel_path,
                            const CBMFileResult *result, const char ***out_keys,
                            const char ***out_vals, int *out_count) {
    *out_keys = NULL;
    *out_vals = NULL;
    *out_count = 0;

    /* Fast path: build from cached extraction result (no JSON parsing) */
    if (result && result->imports.count > 0) {
        const char **keys = calloc((size_t)result->imports.count, sizeof(const char *));
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

    const char **keys = calloc(edge_count, sizeof(const char *));
    const char **vals = calloc(edge_count, sizeof(const char *));
    int count = 0;

    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_edge_t *e = edges[i];
        const cbm_gbuf_node_t *target = cbm_gbuf_find_by_id(ctx->gbuf, e->target_id);
        if (!target || !e->properties_json) {
            continue;
        }

        const char *start = strstr(e->properties_json, "\"local_name\":\"");
        if (start) {
            start += strlen("\"local_name\":\"");
            const char *end = strchr(start, '"');
            if (end && end > start) {
                keys[count] = cbm_strndup(start, end - start);
                vals[count] = target->qualified_name;
                count++;
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

/* Resolve a class/type name through the registry. Returns borrowed QN or NULL. */
static const char *resolve_as_class(const cbm_registry_t *reg, const char *name,
                                    const char *module_qn, const char **imp_keys,
                                    const char **imp_vals, int imp_count) {
    cbm_resolution_t res =
        cbm_registry_resolve(reg, name, module_qn, imp_keys, imp_vals, imp_count);
    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        return NULL;
    }

    /* Verify it's a Class, Interface, or Type */
    const char *label = cbm_registry_label_of(reg, res.qualified_name);
    if (!label) {
        return NULL;
    }
    if (strcmp(label, "Class") != 0 && strcmp(label, "Interface") != 0 &&
        strcmp(label, "Type") != 0 && strcmp(label, "Enum") != 0) {
        return NULL;
    }
    return res.qualified_name;
}

/* Extract decorator function name: "@app.route('/api')" → "app.route" */
static void extract_decorator_func(const char *dec, char *out, size_t outsz) {
    out[0] = '\0';
    if (!dec) {
        return;
    }
    const char *start = dec;
    if (*start == '@') {
        start++;
    }
    /* Find opening paren */
    const char *paren = strchr(start, '(');
    size_t len = paren ? (size_t)(paren - start) : strlen(start);
    if (len == 0 || len >= outsz) {
        return;
    }
    memcpy(out, start, len);
    out[len] = '\0';
}

/* ── Go-style implicit interface satisfaction ─────────────────── */

/* Check if file_path ends with a suffix. */
static bool fp_ends_with(const char *fp, const char *suffix) {
    if (!fp || !suffix) {
        return false;
    }
    size_t fplen = strlen(fp);
    size_t sflen = strlen(suffix);
    return fplen >= sflen && strcmp(fp + fplen - sflen, suffix) == 0;
}

/* Info about one interface method (name + node ID). */
typedef struct {
    const char *name;
    int64_t id;
} go_imethod_t;

/* Check if class has all interface methods and create IMPLEMENTS + OVERRIDE edges. */
static int check_go_class_implements(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *cls,
                                     const cbm_gbuf_node_t *iface, const go_imethod_t *imethods,
                                     int im_count) {
    if (!cls->file_path || !cls->qualified_name) {
        return 0;
    }
    if (!fp_ends_with(cls->file_path, ".go")) {
        return 0;
    }
    char prefix[CBM_SZ_512];
    snprintf(prefix, sizeof(prefix), "%s.", cls->qualified_name);
    for (int m = 0; m < im_count; m++) {
        char method_qn[CBM_SZ_512];
        snprintf(method_qn, sizeof(method_qn), "%s%s", prefix, imethods[m].name);
        if (!cbm_gbuf_find_by_qn(ctx->gbuf, method_qn)) {
            return 0;
        }
    }
    cbm_gbuf_insert_edge(ctx->gbuf, cls->id, iface->id, "IMPLEMENTS", "{}");
    int edges = SKIP_ONE;
    for (int m = 0; m < im_count; m++) {
        char method_qn[CBM_SZ_512];
        snprintf(method_qn, sizeof(method_qn), "%s%s", prefix, imethods[m].name);
        const cbm_gbuf_node_t *cm = cbm_gbuf_find_by_qn(ctx->gbuf, method_qn);
        if (cm) {
            cbm_gbuf_insert_edge(ctx->gbuf, cm->id, imethods[m].id, "OVERRIDE", "{}");
            edges++;
        }
    }
    return edges;
}

int cbm_pipeline_implements_go(cbm_pipeline_ctx_t *ctx) {
    int edge_count = 0;

    /* Find all Interface nodes */
    const cbm_gbuf_node_t **ifaces = NULL;
    int iface_count = 0;
    if (cbm_gbuf_find_by_label(ctx->gbuf, "Interface", &ifaces, &iface_count) != 0) {
        return 0;
    }

    /* Find all Class nodes */
    const cbm_gbuf_node_t **classes = NULL;
    int class_count = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Class", &classes, &class_count);
    if (class_count == 0) {
        return 0;
    }

    for (int i = 0; i < iface_count; i++) {
        const cbm_gbuf_node_t *iface = ifaces[i];
        if (!iface->file_path || !fp_ends_with(iface->file_path, ".go")) {
            continue;
        }

        /* Get interface methods via DEFINES_METHOD edges */
        const cbm_gbuf_edge_t **dm_edges = NULL;
        int dm_count = 0;
        if (cbm_gbuf_find_edges_by_source_type(ctx->gbuf, iface->id, "DEFINES_METHOD", &dm_edges,
                                               &dm_count) != 0 ||
            dm_count == 0) {
            continue;
        }

        /* Collect interface method info */
        go_imethod_t imethods[CBM_SZ_128];
        int im_count = 0;
        for (int j = 0; j < dm_count && im_count < CBM_SZ_128; j++) {
            const cbm_gbuf_node_t *m = cbm_gbuf_find_by_id(ctx->gbuf, dm_edges[j]->target_id);
            if (m && m->name) {
                imethods[im_count++] = (go_imethod_t){m->name, m->id};
            }
        }
        if (im_count == 0) {
            continue;
        }

        /* Check each Class node for method-set satisfaction */
        for (int c = 0; c < class_count; c++) {
            edge_count += check_go_class_implements(ctx, classes[c], iface, imethods, im_count);
        }
    }
    return edge_count;
}

/* Process INHERITS + DECORATES edges for one definition. */
/* Resolve one decorator and create DECORATES edge. */
static void resolve_decorator(cbm_pipeline_ctx_t *ctx, const cbm_gbuf_node_t *node,
                              const char *decorator, const char *module_qn, const char **imp_keys,
                              const char **imp_vals, int imp_count, int *count) {
    char func_name[CBM_SZ_256];
    extract_decorator_func(decorator, func_name, sizeof(func_name));
    if (func_name[0] == '\0') {
        return;
    }
    cbm_resolution_t res =
        cbm_registry_resolve(ctx->registry, func_name, module_qn, imp_keys, imp_vals, imp_count);
    if (!res.qualified_name || res.qualified_name[0] == '\0') {
        return;
    }
    const cbm_gbuf_node_t *dec = cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
    if (dec && node->id != dec->id) {
        char props[CBM_SZ_256];
        snprintf(props, sizeof(props), "{\"decorator\":\"%s\"}", decorator);
        cbm_gbuf_insert_edge(ctx->gbuf, node->id, dec->id, "DECORATES", props);
        /* Ensure a reference edge exists so the decorator appears in usage queries
         * without being misclassified as a real call by downstream passes. */
        cbm_gbuf_insert_edge(ctx->gbuf, node->id, dec->id, "USAGE", "{}");
        (*count)++;
    }
}

static void sem_process_def_edges(cbm_pipeline_ctx_t *ctx, const CBMDefinition *def,
                                  const char *module_qn, const char **imp_keys,
                                  const char **imp_vals, int imp_count, int *inherits_count,
                                  int *decorates_count) {
    if (!def->qualified_name) {
        return;
    }
    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
    if (!node) {
        return;
    }
    if (def->base_classes) {
        for (int b = 0; def->base_classes[b]; b++) {
            const char *base_qn = resolve_as_class(ctx->registry, def->base_classes[b], module_qn,
                                                   imp_keys, imp_vals, imp_count);
            if (!base_qn) {
                continue;
            }
            const cbm_gbuf_node_t *base_node = cbm_gbuf_find_by_qn(ctx->gbuf, base_qn);
            if (base_node && node->id != base_node->id) {
                cbm_gbuf_insert_edge(ctx->gbuf, node->id, base_node->id, "INHERITS", "{}");
                (*inherits_count)++;
            }
        }
    }
    if (def->decorators) {
        for (int dc = 0; def->decorators[dc]; dc++) {
            resolve_decorator(ctx, node, def->decorators[dc], module_qn, imp_keys, imp_vals,
                              imp_count, decorates_count);
        }
    }
}

/* Get extraction result from cache or re-extract. Sets *owned=true if caller must free. */
static CBMFileResult *sem_get_or_extract(cbm_pipeline_ctx_t *ctx, int file_idx,
                                         const cbm_file_info_t *fi, bool *owned) {
    *owned = false;
    if (ctx->result_cache && ctx->result_cache[file_idx]) {
        return ctx->result_cache[file_idx];
    }
    int source_len = 0;
    char *source = read_file(fi->path, &source_len);
    if (!source) {
        return NULL;
    }
    CBMFileResult *r = cbm_extract_file(source, source_len, fi->language, ctx->project_name,
                                        fi->rel_path, CBM_EXTRACT_BUDGET, NULL, NULL);
    free(source);
    if (r) {
        *owned = true;
    }
    return r;
}

/* Resolve Rust impl traits for one file's extraction results. */
static int resolve_impl_traits(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                               const char *module_qn, const char **imp_keys, const char **imp_vals,
                               int imp_count) {
    int count = 0;
    for (int t = 0; t < result->impl_traits.count; t++) {
        CBMImplTrait *it = &result->impl_traits.items[t];
        if (!it->trait_name || !it->struct_name) {
            continue;
        }
        const char *trait_qn = resolve_as_class(ctx->registry, it->trait_name, module_qn, imp_keys,
                                                imp_vals, imp_count);
        if (!trait_qn) {
            continue;
        }
        const char *struct_qn = resolve_as_class(ctx->registry, it->struct_name, module_qn,
                                                 imp_keys, imp_vals, imp_count);
        if (!struct_qn) {
            continue;
        }
        const cbm_gbuf_node_t *tn = cbm_gbuf_find_by_qn(ctx->gbuf, trait_qn);
        const cbm_gbuf_node_t *sn = cbm_gbuf_find_by_qn(ctx->gbuf, struct_qn);
        if (tn && sn && tn->id != sn->id) {
            cbm_gbuf_insert_edge(ctx->gbuf, sn->id, tn->id, "IMPLEMENTS", "{}");
            count++;
        }
    }
    return count;
}

int cbm_pipeline_pass_semantic(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count) {
    cbm_log_info("pass.start", "pass", "semantic", "files", itoa_log(file_count));

    int inherits_count = 0;
    int decorates_count = 0;
    int implements_count = 0;
    int errors = 0;

    for (int i = 0; i < file_count; i++) {
        if (cbm_pipeline_check_cancel(ctx)) {
            return CBM_NOT_FOUND;
        }

        const char *rel = files[i].rel_path;

        bool result_owned = false;
        CBMFileResult *result = sem_get_or_extract(ctx, i, &files[i], &result_owned);
        if (!result) {
            errors++;
            continue;
        }

        /* Build import map for resolution */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, rel, result, &imp_keys, &imp_vals, &imp_count);

        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, rel);

        /* ── INHERITS + DECORATES from definitions ──────────────── */
        for (int d = 0; d < result->defs.count; d++) {
            sem_process_def_edges(ctx, &result->defs.items[d], module_qn, imp_keys, imp_vals,
                                  imp_count, &inherits_count, &decorates_count);
        }

        /* ── IMPLEMENTS from impl_traits (Rust) ─────────────────── */
        implements_count +=
            resolve_impl_traits(ctx, result, module_qn, imp_keys, imp_vals, imp_count);

        free(module_qn);
        free_import_map(imp_keys, imp_vals, imp_count);
        if (result_owned) {
            cbm_free_result(result);
        }
    }

    /* ── Go-style implicit interface satisfaction ──────────────── */
    int go_impl = cbm_pipeline_implements_go(ctx);
    implements_count += go_impl;

    cbm_log_info("pass.done", "pass", "semantic", "inherits", itoa_log(inherits_count), "decorates",
                 itoa_log(decorates_count), "implements", itoa_log(implements_count), "errors",
                 itoa_log(errors));
    return 0;
}
