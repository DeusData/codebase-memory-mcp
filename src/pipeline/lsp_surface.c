/*
 * lsp_surface.c — per-file LSP-surface codec (closure-repair incremental).
 *
 * See lsp_surface.h for the contract. Two invariants carry the feature:
 *
 *  1. ROUND-TRIP FIDELITY: defs_from_json(build_json(defs)) must hand the
 *     per-language registrars the same values collect_all_defs would have
 *     built from a real parse — a lossy field here silently degrades
 *     cross-file resolution only on the incremental path, the exact class
 *     of divergence this feature exists to eliminate.
 *
 *  2. CANONICAL BYTES: every field is written, in fixed order, with an
 *     explicit JSON null for absent strings (NULL and "" are different
 *     values in the CBMLSPDef contract — receiver_type NULL means "not a
 *     method", and several registrars branch on that). Byte equality of the
 *     serialization therefore IS surface equality, and the sha over the
 *     bytes is the early-cutoff key: a body edit reserializes identically.
 */
#include "pipeline/lsp_surface.h"

#include <stdlib.h>
#include <string.h>

#include "cbm.h" /* cbm_label_is_relation — reg-only surface membership */
#include "foundation/log.h"
#include "foundation/sha256.h"
#include "yyjson/yyjson.h"

enum { SURFACE_CODEC_VERSION = 1 };

/* Labels the incremental name registry serves that pxc_map_label does NOT
 * carry into the CBMLSPDef set. Their (name, qn, label) triple must still
 * participate in the surface hash, or renaming one would slip past the
 * early cutoff while stale references to it survive in dependent files —
 * for Table/View that means a renamed table keeping stale FROM/JOIN lineage
 * edges from dependent SQL files. KEEP IN SYNC with pxc_map_label
 * (pass_lsp_cross.c) and incr_label_is_registry_symbol
 * (pipeline_incremental.c); the codec unit test cross-checks the three. */
static bool surface_reg_only_label(const char *label) {
    return label && (strcmp(label, "Field") == 0 || cbm_label_is_relation(label));
}

static void add_str_or_null(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                            const char *val) {
    if (val) {
        yyjson_mut_obj_add_str(doc, obj, key, val);
    } else {
        yyjson_mut_obj_add_null(doc, obj, key);
    }
}

static void add_str_array_or_null(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                  const char **items, int count_or_neg1_terminated) {
    if (!items) {
        yyjson_mut_obj_add_null(doc, obj, key);
        return;
    }
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    if (count_or_neg1_terminated >= 0) {
        for (int i = 0; i < count_or_neg1_terminated; i++) {
            yyjson_mut_arr_add_str(doc, arr, items[i] ? items[i] : "?");
        }
    } else {
        for (int i = 0; items[i]; i++) {
            yyjson_mut_arr_add_str(doc, arr, items[i]);
        }
    }
    yyjson_mut_obj_add_val(doc, obj, key, arr);
}

/* Serialize one file's surface: its slice of all_defs plus the registry-only
 * symbols from its raw extraction defs. Returns a malloc'd JSON string. */
static char *surface_file_to_json(const CBMFileResult *result, const CBMLSPDef *defs,
                                  int def_count) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "v", SURFACE_CODEC_VERSION);

    yyjson_mut_val *lsp = yyjson_mut_arr(doc);
    for (int i = 0; i < def_count; i++) {
        const CBMLSPDef *d = &defs[i];
        yyjson_mut_val *o = yyjson_mut_obj(doc);
        add_str_or_null(doc, o, "qn", d->qualified_name);
        add_str_or_null(doc, o, "sn", d->short_name);
        add_str_or_null(doc, o, "lb", d->label);
        add_str_or_null(doc, o, "rt", d->receiver_type);
        add_str_or_null(doc, o, "dm", d->def_module_qn);
        add_str_or_null(doc, o, "ret", d->return_types);
        add_str_or_null(doc, o, "emb", d->embedded_types);
        add_str_or_null(doc, o, "fd", d->field_defs);
        add_str_or_null(doc, o, "mn", d->method_names_str);
        add_str_array_or_null(doc, o, "spt", d->signature_param_types, d->signature_param_count);
        yyjson_mut_obj_add_bool(doc, o, "ii", d->is_interface);
        yyjson_mut_obj_add_int(doc, o, "lg", (int)d->lang);
        add_str_or_null(doc, o, "ns", d->namespace_name);
        add_str_or_null(doc, o, "tq", d->trait_qn);
        yyjson_mut_obj_add_bool(doc, o, "ir", d->is_rust_impl_relation);
        yyjson_mut_obj_add_bool(doc, o, "ab", d->is_abstract);
        add_str_array_or_null(doc, o, "dec", d->decorators, -1);
        yyjson_mut_arr_add_val(lsp, o);
    }
    yyjson_mut_obj_add_val(doc, root, "lsp", lsp);

    yyjson_mut_val *reg = yyjson_mut_arr(doc);
    if (result) {
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *d = &result->defs.items[i];
            if (!surface_reg_only_label(d->label) || !d->name || !d->qualified_name) {
                continue;
            }
            yyjson_mut_val *o = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, o, "n", d->name);
            yyjson_mut_obj_add_str(doc, o, "q", d->qualified_name);
            yyjson_mut_obj_add_str(doc, o, "k", d->label);
            yyjson_mut_arr_add_val(reg, o);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "reg", reg);

    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

int cbm_lsp_surface_build_rows(const char *project, CBMFileResult **cache,
                               const cbm_file_info_t *files, int file_count,
                               const CBMLSPDef *all_defs, const int *def_starts,
                               cbm_lsp_surface_row_t **out_rows, int *out_count) {
    *out_rows = NULL;
    *out_count = 0;
    if (file_count <= 0) {
        return 0;
    }
    cbm_lsp_surface_row_t *rows = calloc((size_t)file_count, sizeof(*rows));
    if (!rows) {
        return -1;
    }
    int n = 0;
    for (int i = 0; i < file_count; i++) {
        if (!cache[i]) {
            /* Never parsed this run (read/extract skip): no surface claim.
             * The routing layer treats a missing row as "must full-rebuild
             * before this file can be reasoned about", which is the correct
             * fail-closed default for an unreadable file. */
            continue;
        }
        int start = def_starts ? def_starts[i] : 0;
        int end = def_starts ? def_starts[i + 1] : 0;
        char *json =
            surface_file_to_json(cache[i], all_defs ? all_defs + start : NULL, end - start);
        if (!json) {
            cbm_store_free_lsp_surfaces(rows, n);
            return -1;
        }
        char sha[CBM_SHA256_HEX_LEN + 1];
        cbm_sha256_hex(json, strlen(json), sha);
        cbm_lsp_surface_row_t *r = &rows[n];
        r->project = strdup(project);
        r->rel_path = strdup(files[i].rel_path);
        r->surface_sha = strdup(sha);
        r->defs_json = json;
        r->ref_bloom = NULL;
        r->ref_bloom_len = 0;
        r->config_ctx = strdup("");
        if (!r->project || !r->rel_path || !r->surface_sha || !r->config_ctx) {
            n++;
            cbm_store_free_lsp_surfaces(rows, n);
            return -1;
        }
        n++;
    }
    *out_rows = rows;
    *out_count = n;
    return 0;
}

static const char *arena_str_or_null(CBMArena *arena, yyjson_val *v) {
    if (!v || yyjson_is_null(v)) {
        return NULL;
    }
    const char *s = yyjson_get_str(v);
    return s ? cbm_arena_strdup(arena, s) : NULL;
}

static const char **arena_str_array(CBMArena *arena, yyjson_val *arr, bool null_terminated,
                                    int *out_count) {
    *out_count = 0;
    if (!arr || yyjson_is_null(arr) || !yyjson_is_arr(arr)) {
        return NULL;
    }
    int count = (int)yyjson_arr_size(arr);
    const char **items =
        cbm_arena_alloc(arena, (size_t)(count + (null_terminated ? 1 : 0)) * sizeof(char *));
    if (!items) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        const char *s = yyjson_get_str(yyjson_arr_get(arr, (size_t)i));
        items[i] = s ? cbm_arena_strdup(arena, s) : "?";
    }
    if (null_terminated) {
        items[count] = NULL;
    }
    *out_count = count;
    return items;
}

int cbm_lsp_surface_defs_from_json(CBMArena *arena, const char *defs_json, CBMLSPDef **out_defs) {
    *out_defs = NULL;
    if (!defs_json) {
        return -1;
    }
    yyjson_doc *doc = yyjson_read(defs_json, strlen(defs_json), 0);
    if (!doc) {
        return -1;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *ver = root ? yyjson_obj_get(root, "v") : NULL;
    yyjson_val *lsp = root ? yyjson_obj_get(root, "lsp") : NULL;
    if (!ver || yyjson_get_int(ver) != SURFACE_CODEC_VERSION || !lsp || !yyjson_is_arr(lsp)) {
        yyjson_doc_free(doc);
        return -1;
    }
    int count = (int)yyjson_arr_size(lsp);
    if (count == 0) {
        yyjson_doc_free(doc);
        return 0;
    }
    CBMLSPDef *defs = cbm_arena_alloc(arena, (size_t)count * sizeof(CBMLSPDef));
    if (!defs) {
        yyjson_doc_free(doc);
        return -1;
    }
    memset(defs, 0, (size_t)count * sizeof(CBMLSPDef));
    for (int i = 0; i < count; i++) {
        yyjson_val *o = yyjson_arr_get(lsp, (size_t)i);
        CBMLSPDef *d = &defs[i];
        d->qualified_name = arena_str_or_null(arena, yyjson_obj_get(o, "qn"));
        d->short_name = arena_str_or_null(arena, yyjson_obj_get(o, "sn"));
        d->label = arena_str_or_null(arena, yyjson_obj_get(o, "lb"));
        d->receiver_type = arena_str_or_null(arena, yyjson_obj_get(o, "rt"));
        d->def_module_qn = arena_str_or_null(arena, yyjson_obj_get(o, "dm"));
        d->return_types = arena_str_or_null(arena, yyjson_obj_get(o, "ret"));
        d->embedded_types = arena_str_or_null(arena, yyjson_obj_get(o, "emb"));
        d->field_defs = arena_str_or_null(arena, yyjson_obj_get(o, "fd"));
        d->method_names_str = arena_str_or_null(arena, yyjson_obj_get(o, "mn"));
        d->signature_param_types =
            arena_str_array(arena, yyjson_obj_get(o, "spt"), false, &d->signature_param_count);
        d->is_interface = yyjson_get_bool(yyjson_obj_get(o, "ii"));
        d->lang = (CBMLanguage)yyjson_get_int(yyjson_obj_get(o, "lg"));
        d->namespace_name = arena_str_or_null(arena, yyjson_obj_get(o, "ns"));
        d->trait_qn = arena_str_or_null(arena, yyjson_obj_get(o, "tq"));
        d->is_rust_impl_relation = yyjson_get_bool(yyjson_obj_get(o, "ir"));
        d->is_abstract = yyjson_get_bool(yyjson_obj_get(o, "ab"));
        int dec_count = 0;
        d->decorators = arena_str_array(arena, yyjson_obj_get(o, "dec"), true, &dec_count);
        if (!d->qualified_name || !d->short_name || !d->label) {
            /* A def the writer could not have produced: qn/sn/label are
             * unconditionally present in build_lsp_def output. Corrupt row. */
            yyjson_doc_free(doc);
            return -1;
        }
    }
    yyjson_doc_free(doc);
    *out_defs = defs;
    return count;
}
