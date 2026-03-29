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
#include "pipeline/pipeline.h"
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
        if (!target || !e->properties_json) {
            continue;
        }

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
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return fplen >= sflen && strcmp(fp + fplen - sflen, suffix) == 0;
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
        typedef struct {
            const char *name;
            // NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
            int64_t id;
        } imethod_t;
        imethod_t imethods[128];
        int im_count = 0;
        for (int j = 0; j < dm_count && im_count < 128; j++) {
            const cbm_gbuf_node_t *m = cbm_gbuf_find_by_id(ctx->gbuf, dm_edges[j]->target_id);
            if (m && m->name) {
                imethods[im_count++] = (imethod_t){m->name, m->id};
            }
        }
        if (im_count == 0) {
            continue;
        }

        /* Check each Class node for method-set satisfaction */
        for (int c = 0; c < class_count; c++) {
            const cbm_gbuf_node_t *cls = classes[c];
            if (!cls->file_path || !cls->qualified_name) {
                continue;
            }
            if (!fp_ends_with(cls->file_path, ".go")) {
                continue;
            }

            /* Build QN prefix: "pkg.FileReader." */
            char prefix[512];
            snprintf(prefix, sizeof(prefix), "%s.", cls->qualified_name);

            /* Check if ALL interface methods are present */
            bool all_match = true;
            for (int m = 0; m < im_count; m++) {
                char method_qn[512];
                snprintf(method_qn, sizeof(method_qn), "%s%s", prefix, imethods[m].name);
                if (!cbm_gbuf_find_by_qn(ctx->gbuf, method_qn)) {
                    all_match = false;
                    break;
                }
            }
            if (!all_match) {
                continue;
            }

            /* IMPLEMENTS edge: Class → Interface */
            cbm_gbuf_insert_edge(ctx->gbuf, cls->id, iface->id, "IMPLEMENTS", "{}");
            edge_count++;

            /* OVERRIDE edges: ClassMethod → InterfaceMethod */
            for (int m = 0; m < im_count; m++) {
                char method_qn[512];
                snprintf(method_qn, sizeof(method_qn), "%s%s", prefix, imethods[m].name);
                const cbm_gbuf_node_t *cm = cbm_gbuf_find_by_qn(ctx->gbuf, method_qn);
                if (cm) {
                    cbm_gbuf_insert_edge(ctx->gbuf, cm->id, imethods[m].id, "OVERRIDE", "{}");
                    edge_count++;
                }
            }
        }
    }
    return edge_count;
}

// NOLINTNEXTLINE(misc-include-cleaner) — cbm_file_info_t provided by standard header
int cbm_pipeline_pass_semantic(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count) {
    cbm_log_info("pass.start", "pass", "semantic", "files", itoa_log(file_count));

    int inherits_count = 0;
    int decorates_count = 0;
    int implements_count = 0;
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

        /* Build import map for resolution */
        const char **imp_keys = NULL;
        const char **imp_vals = NULL;
        int imp_count = 0;
        build_import_map(ctx, rel, result, &imp_keys, &imp_vals, &imp_count);

        char *module_qn = cbm_pipeline_fqn_module(ctx->project_name, rel);

        /* ── INHERITS + DECORATES from definitions ──────────────── */
        for (int d = 0; d < result->defs.count; d++) {
            CBMDefinition *def = &result->defs.items[d];
            if (!def->qualified_name) {
                continue;
            }

            const cbm_gbuf_node_t *node = cbm_gbuf_find_by_qn(ctx->gbuf, def->qualified_name);
            if (!node) {
                continue;
            }

            /* INHERITS: base_classes */
            if (def->base_classes) {
                for (int b = 0; def->base_classes[b]; b++) {
                    const char *base_qn =
                        resolve_as_class(ctx->registry, def->base_classes[b], module_qn, imp_keys,
                                         imp_vals, imp_count);
                    if (!base_qn) {
                        continue;
                    }

                    const cbm_gbuf_node_t *base_node = cbm_gbuf_find_by_qn(ctx->gbuf, base_qn);
                    if (!base_node || node->id == base_node->id) {
                        continue;
                    }

                    cbm_gbuf_insert_edge(ctx->gbuf, node->id, base_node->id, "INHERITS", "{}");
                    inherits_count++;
                }
            }

            /* DECORATES: decorator → the decorated function/class */
            if (def->decorators) {
                for (int dc = 0; def->decorators[dc]; dc++) {
                    char func_name[256];
                    extract_decorator_func(def->decorators[dc], func_name, sizeof(func_name));
                    if (func_name[0] == '\0') {
                        continue;
                    }

                    cbm_resolution_t res = cbm_registry_resolve(ctx->registry, func_name, module_qn,
                                                                imp_keys, imp_vals, imp_count);
                    if (!res.qualified_name || res.qualified_name[0] == '\0') {
                        continue;
                    }

                    const cbm_gbuf_node_t *dec_node =
                        cbm_gbuf_find_by_qn(ctx->gbuf, res.qualified_name);
                    if (!dec_node || node->id == dec_node->id) {
                        continue;
                    }

                    char props[256];
                    snprintf(props, sizeof(props), "{\"decorator\":\"%s\"}", def->decorators[dc]);
                    cbm_gbuf_insert_edge(ctx->gbuf, node->id, dec_node->id, "DECORATES", props);
                    decorates_count++;
                }
            }
        }

        /* ── IMPLEMENTS from impl_traits (Rust) ─────────────────── */
        for (int t = 0; t < result->impl_traits.count; t++) {
            CBMImplTrait *it = &result->impl_traits.items[t];
            if (!it->trait_name || !it->struct_name) {
                continue;
            }

            const char *trait_qn = resolve_as_class(ctx->registry, it->trait_name, module_qn,
                                                    imp_keys, imp_vals, imp_count);
            if (!trait_qn) {
                continue;
            }

            const char *struct_qn = resolve_as_class(ctx->registry, it->struct_name, module_qn,
                                                     imp_keys, imp_vals, imp_count);
            if (!struct_qn) {
                continue;
            }

            const cbm_gbuf_node_t *trait_node = cbm_gbuf_find_by_qn(ctx->gbuf, trait_qn);
            const cbm_gbuf_node_t *struct_node = cbm_gbuf_find_by_qn(ctx->gbuf, struct_qn);
            if (!trait_node || !struct_node) {
                continue;
            }
            if (trait_node->id == struct_node->id) {
                continue;
            }

            cbm_gbuf_insert_edge(ctx->gbuf, struct_node->id, trait_node->id, "IMPLEMENTS", "{}");
            implements_count++;
        }

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
