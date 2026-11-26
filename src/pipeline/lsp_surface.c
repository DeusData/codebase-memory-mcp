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
#include "foundation/constants.h"
#include "pipeline/pipeline_internal.h"

#include <limits.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "cbm.h"     /* cbm_label_is_relation — reg-only surface membership */
#include "doclink.h" /* cbm_doclink_portable_scope — the "dl" key */
#include "foundation/json_writer.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/sha256.h"
#include "pipeline/doc_links.h"  /* cbm_doclinks_storable_scope */
#include "pipeline/type_scope.h" /* the type labels identities may name */
#include "pipeline/worker_pool.h"
#include "yyjson/yyjson.h"

/* 3: Go defs carry their parsed result list `|`-joined in "ret" (F-G8 D1);
 * a version-2 surface holds the raw result text and must be re-extracted.
 * 4: Go identity aliases carry their target in "al" (#2033); a version-3
 * surface has no alias targets and must be re-extracted.
 * 5: defs carry their declared types' identities in "ty" (type_scope.h); a
 * version-4 surface has none, and a dependent re-resolved against it would
 * lose the calls a full index makes through them. The surface hash and the
 * Python namespace comparison leave "ty" out: they judge the identities'
 * inputs (the def's type texts, the file's imports in "dl" / "py"), which
 * the planner's probe computes exactly as a full index does; the outputs
 * also depend on other files' type definitions, which the planner gates
 * itself (closure_identity_decline, pipeline_incremental.c). */
enum { SURFACE_CODEC_VERSION = 5 };

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

/* The namespace payload is content-only. Its ordered records never depend on
 * the registry used to build ordinary LSP definitions. */
static bool surface_py_add_nullable(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                    const char *value) {
    return value ? yyjson_mut_obj_add_str(doc, obj, key, value)
                 : yyjson_mut_obj_add_null(doc, obj, key);
}

static bool surface_add_python_namespace(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                         const CBMFileResult *result, CBMLanguage language,
                                         const char *rel_path) {
    if (language != CBM_LANG_PYTHON) {
        return yyjson_mut_obj_add_null(doc, root, "py");
    }
    if (!result || !result->module_qn || !result->module_qn[0] || !rel_path || !rel_path[0] ||
        result->py_namespace.status != CBM_PY_NS_COMPLETE ||
        result->py_namespace.language != CBM_LANG_PYTHON ||
        !cbm_py_namespace_facts_valid(&result->py_namespace)) {
        return false;
    }
    const CBMPyNamespaceFacts *facts = &result->py_namespace;
    yyjson_mut_val *py = yyjson_mut_obj(doc);
    yyjson_mut_val *events = yyjson_mut_arr(doc);
    if (!py || !events || !yyjson_mut_obj_add_uint(doc, py, "v", facts->version) ||
        !yyjson_mut_obj_add_int(doc, py, "lang", (int)facts->language) ||
        !yyjson_mut_obj_add_str(doc, py, "module", result->module_qn) ||
        !yyjson_mut_obj_add_str(doc, py, "path", rel_path) ||
        !yyjson_mut_obj_add_int(doc, py, "status", (int)facts->status) ||
        !yyjson_mut_obj_add_int(doc, py, "failure", (int)facts->failure)) {
        return false;
    }
    for (int i = 0; i < facts->count; i++) {
        const CBMPyNamespaceFact *f = &facts->items[i];
        yyjson_mut_val *event = yyjson_mut_obj(doc);
        yyjson_mut_val *names = yyjson_mut_arr(doc);
        if (!event || !names || !yyjson_mut_obj_add_int(doc, event, "k", (int)f->kind) ||
            !yyjson_mut_obj_add_int(doc, event, "d", (int)f->def_kind) ||
            !yyjson_mut_obj_add_int(doc, event, "s", (int)f->sequence_kind) ||
            !yyjson_mut_obj_add_int(doc, event, "r", (int)f->reason) ||
            !yyjson_mut_obj_add_uint(doc, event, "f", f->flags) ||
            !yyjson_mut_obj_add_uint(doc, event, "l", f->relative_level) ||
            !surface_py_add_nullable(doc, event, "n", f->local_name) ||
            !surface_py_add_nullable(doc, event, "m", f->module_name) ||
            !surface_py_add_nullable(doc, event, "i", f->member_name)) {
            return false;
        }
        for (int j = 0; j < f->name_count; j++) {
            if (!yyjson_mut_arr_add_str(doc, names, f->names[j])) {
                return false;
            }
        }
        if (!yyjson_mut_obj_add_val(doc, event, "a", names) ||
            !yyjson_mut_arr_add_val(events, event)) {
            return false;
        }
    }
    return yyjson_mut_obj_add_val(doc, py, "events", events) &&
           yyjson_mut_obj_add_val(doc, root, "py", py);
}

/* Reject duplicate fields rather than letting object lookup choose one. */
static yyjson_val *surface_unique_field(yyjson_val *obj, const char *name) {
    if (!yyjson_is_obj(obj)) {
        return NULL;
    }
    yyjson_val *found = NULL;
    yyjson_obj_iter it = yyjson_obj_iter_with(obj);
    yyjson_val *key;
    while ((key = yyjson_obj_iter_next(&it))) {
        const char *text = yyjson_get_str(key);
        if (text && yyjson_get_len(key) == strlen(name) && strcmp(text, name) == 0) {
            if (found) {
                return NULL;
            }
            found = yyjson_obj_iter_get_val(key);
        }
    }
    return found;
}

static bool surface_py_uint(yyjson_val *value, uint32_t maximum, uint32_t *out) {
    if (!yyjson_is_int(value)) {
        return false;
    }
    int64_t n = yyjson_get_sint(value);
    if (n < 0 || (uint64_t)n > maximum) {
        return false;
    }
    *out = (uint32_t)n;
    return true;
}

static bool surface_py_string(yyjson_val *value, bool nullable, const char **out) {
    *out = NULL;
    if (!value) {
        return false;
    }
    if (yyjson_is_null(value)) {
        return nullable;
    }
    const char *text = yyjson_get_str(value);
    if (!text || strlen(text) != yyjson_get_len(value)) {
        return false;
    }
    *out = text;
    return true;
}

/* Arrays belong to scratch; strings borrow the immutable JSON document.
 * The caller keeps both alive until validation/copy/comparison is finished. */
static int surface_python_namespace_parse(CBMArena *scratch, yyjson_val *root,
                                          cbm_lsp_python_namespace_t *out) {
    memset(out, 0, sizeof(*out));
    uint32_t version = 0, file_language = 0;
    yyjson_val *py = surface_unique_field(root, "py");
    if (!surface_py_uint(surface_unique_field(root, "v"), UINT32_MAX, &version) ||
        version != SURFACE_CODEC_VERSION || !py ||
        !surface_py_uint(surface_unique_field(root, "lang"), CBM_LANG_COUNT - 1, &file_language) ||
        !yyjson_is_arr(surface_unique_field(root, "lsp"))) {
        return -1;
    }
    if (yyjson_is_null(py)) {
        return file_language == CBM_LANG_PYTHON ? -1 : 0;
    }
    if (file_language != CBM_LANG_PYTHON || !yyjson_is_obj(py) || yyjson_obj_size(py) != 7) {
        return -1;
    }
    CBMPyNamespaceFacts *facts = &out->facts;
    uint32_t language, status, failure;
    if (!surface_py_uint(surface_unique_field(py, "v"), UINT32_MAX, &facts->version) ||
        !surface_py_uint(surface_unique_field(py, "lang"), INT_MAX, &language) ||
        !surface_py_uint(surface_unique_field(py, "status"), INT_MAX, &status) ||
        !surface_py_uint(surface_unique_field(py, "failure"), INT_MAX, &failure) ||
        !surface_py_string(surface_unique_field(py, "module"), false, &out->module_qn) ||
        !surface_py_string(surface_unique_field(py, "path"), false, &out->rel_path) ||
        !out->module_qn[0] || !out->rel_path[0] || language != CBM_LANG_PYTHON ||
        status != CBM_PY_NS_COMPLETE || failure != CBM_PY_NS_FAILURE_NONE) {
        return -1;
    }
    facts->language = (CBMLanguage)language;
    facts->status = (CBMPyNamespaceStatus)status;
    facts->failure = (CBMPyNamespaceFailure)failure;
    yyjson_val *events = surface_unique_field(py, "events");
    if (!yyjson_is_arr(events)) {
        return -1;
    }
    size_t count = yyjson_arr_size(events);
    if (count > INT_MAX || count > SIZE_MAX / sizeof(*facts->items)) {
        return -1;
    }
    facts->count = (int)count;
    facts->cap = (int)count;
    if (count) {
        facts->items = cbm_arena_alloc(scratch, count * sizeof(*facts->items));
        if (!facts->items) {
            return -1;
        }
        memset(facts->items, 0, count * sizeof(*facts->items));
    }
    yyjson_arr_iter it = yyjson_arr_iter_with(events);
    yyjson_val *event;
    int i = 0;
    while ((event = yyjson_arr_iter_next(&it))) {
        if (!yyjson_is_obj(event) || yyjson_obj_size(event) != 10) {
            return -1;
        }
        CBMPyNamespaceFact *f = &facts->items[i++];
        uint32_t kind, def_kind, sequence_kind, reason;
        if (!surface_py_uint(surface_unique_field(event, "k"), INT_MAX, &kind) ||
            !surface_py_uint(surface_unique_field(event, "d"), INT_MAX, &def_kind) ||
            !surface_py_uint(surface_unique_field(event, "s"), INT_MAX, &sequence_kind) ||
            !surface_py_uint(surface_unique_field(event, "r"), INT_MAX, &reason) ||
            !surface_py_uint(surface_unique_field(event, "f"), UINT32_MAX, &f->flags) ||
            !surface_py_uint(surface_unique_field(event, "l"), UINT32_MAX, &f->relative_level) ||
            !surface_py_string(surface_unique_field(event, "n"), true, &f->local_name) ||
            !surface_py_string(surface_unique_field(event, "m"), true, &f->module_name) ||
            !surface_py_string(surface_unique_field(event, "i"), true, &f->member_name)) {
            return -1;
        }
        f->kind = (CBMPyNamespaceFactKind)kind;
        f->def_kind = (CBMPyNamespaceDefKind)def_kind;
        f->sequence_kind = (CBMPyNamespaceSequenceKind)sequence_kind;
        f->reason = (CBMPyNamespaceUnknownReason)reason;
        yyjson_val *names = surface_unique_field(event, "a");
        if (!yyjson_is_arr(names)) {
            return -1;
        }
        size_t n = yyjson_arr_size(names);
        if (n > INT_MAX || n > SIZE_MAX / sizeof(*f->names)) {
            return -1;
        }
        f->name_count = (int)n;
        if (n) {
            f->names = cbm_arena_alloc(scratch, n * sizeof(*f->names));
            if (!f->names) {
                return -1;
            }
        }
        yyjson_arr_iter ni = yyjson_arr_iter_with(names);
        yyjson_val *name;
        int j = 0;
        while ((name = yyjson_arr_iter_next(&ni))) {
            if (!surface_py_string(name, false, &f->names[j++])) {
                return -1;
            }
        }
    }
    return cbm_py_namespace_facts_valid(facts) ? 1 : -1;
}

int cbm_lsp_surface_python_namespace_from_json(CBMArena *arena, const char *json,
                                               cbm_lsp_python_namespace_t *out) {
    if (!out) {
        return -1;
    }
    memset(out, 0, sizeof(*out));
    if (!arena || !json) {
        return -1;
    }
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        return -1;
    }
    CBMArena scratch;
    cbm_arena_init(&scratch);
    cbm_lsp_python_namespace_t parsed = {0}, result = {0};
    int status = surface_python_namespace_parse(&scratch, yyjson_doc_get_root(doc), &parsed);
    if (status == 1) {
        if (!cbm_py_namespace_facts_copy(arena, &parsed.facts, &result.facts)) {
            status = -1;
        } else {
            result.module_qn = cbm_arena_strdup(arena, parsed.module_qn);
            result.rel_path = cbm_arena_strdup(arena, parsed.rel_path);
            if (!result.module_qn || !result.rel_path) {
                status = -1;
            }
        }
    }
    cbm_arena_destroy(&scratch);
    yyjson_doc_free(doc);
    if (status == 1) {
        *out = result;
    }
    return status;
}

/* Two objects equal member by member, in order, leaving out the members named
 * `skip` (NULL skips none). */
static bool surface_obj_equal_except(yyjson_val *a, yyjson_val *b, const char *skip) {
    if (!yyjson_is_obj(a) || !yyjson_is_obj(b)) {
        return yyjson_equals(a, b);
    }
    yyjson_obj_iter ia = yyjson_obj_iter_with(a);
    yyjson_obj_iter ib = yyjson_obj_iter_with(b);
    for (;;) {
        yyjson_val *ka = yyjson_obj_iter_next(&ia);
        while (ka && skip && yyjson_equals_str(ka, skip)) {
            ka = yyjson_obj_iter_next(&ia);
        }
        yyjson_val *kb = yyjson_obj_iter_next(&ib);
        while (kb && skip && yyjson_equals_str(kb, skip)) {
            kb = yyjson_obj_iter_next(&ib);
        }
        if (!ka || !kb) {
            return !ka && !kb;
        }
        if (!yyjson_equals(ka, kb) ||
            !yyjson_equals(yyjson_obj_iter_get_val(ka), yyjson_obj_iter_get_val(kb))) {
            return false;
        }
    }
}

/* Two surface rows equal but for their declared-type identities ("ty" in
 * each "lsp" def), which the comparisons leave out (SURFACE_CODEC_VERSION). */
static bool surface_rows_equal_without_identities(yyjson_val *a, yyjson_val *b) {
    if (!yyjson_is_obj(a) || !yyjson_is_obj(b)) {
        return yyjson_equals(a, b);
    }
    yyjson_obj_iter ia = yyjson_obj_iter_with(a);
    yyjson_obj_iter ib = yyjson_obj_iter_with(b);
    for (;;) {
        yyjson_val *ka = yyjson_obj_iter_next(&ia);
        yyjson_val *kb = yyjson_obj_iter_next(&ib);
        if (!ka || !kb) {
            return !ka && !kb;
        }
        if (!yyjson_equals(ka, kb)) {
            return false;
        }
        yyjson_val *va = yyjson_obj_iter_get_val(ka);
        yyjson_val *vb = yyjson_obj_iter_get_val(kb);
        if (!yyjson_equals_str(ka, "lsp") || !yyjson_is_arr(va) || !yyjson_is_arr(vb)) {
            if (!yyjson_equals(va, vb)) {
                return false;
            }
            continue;
        }
        if (yyjson_arr_size(va) != yyjson_arr_size(vb)) {
            return false;
        }
        yyjson_arr_iter da = yyjson_arr_iter_with(va);
        yyjson_arr_iter db = yyjson_arr_iter_with(vb);
        for (yyjson_val *x = yyjson_arr_iter_next(&da), *y = yyjson_arr_iter_next(&db); x && y;
             x = yyjson_arr_iter_next(&da), y = yyjson_arr_iter_next(&db)) {
            if (!surface_obj_equal_except(x, y, "ty")) {
                return false;
            }
        }
    }
}

int cbm_lsp_surface_python_change(const char *old_json, const char *new_json) {
    if (!old_json) {
        return -1;
    }
    yyjson_doc *old_doc = yyjson_read(old_json, strlen(old_json), 0);
    yyjson_doc *new_doc = new_json ? yyjson_read(new_json, strlen(new_json), 0) : NULL;
    if (!old_doc || (new_json && !new_doc)) {
        yyjson_doc_free(old_doc);
        yyjson_doc_free(new_doc);
        return -1;
    }
    CBMArena scratch;
    cbm_arena_init(&scratch);
    cbm_lsp_python_namespace_t old_facts, new_facts;
    int old_state =
        surface_python_namespace_parse(&scratch, yyjson_doc_get_root(old_doc), &old_facts);
    int new_state =
        new_doc ? surface_python_namespace_parse(&scratch, yyjson_doc_get_root(new_doc), &new_facts)
                : 0;
    int result = -1;
    if (old_state >= 0 && new_state >= 0) {
        result = (old_state == 0 && new_state == 0) ? 0
                 : (old_state != new_state || !new_doc ||
                    !surface_rows_equal_without_identities(yyjson_doc_get_root(old_doc),
                                                           yyjson_doc_get_root(new_doc)))
                     ? 1
                     : 0;
    }
    cbm_arena_destroy(&scratch);
    yyjson_doc_free(old_doc);
    yyjson_doc_free(new_doc);
    return result;
}

/* ── Declared-type identities across a repair ─────────────────────── */

static bool surface_type_label(int lang, const char *label) {
    const cbm_type_scope_lang_t *row =
        lang >= 0 && lang < (int)CBM_LANG_COUNT ? cbm_type_scope_lang((CBMLanguage)lang) : NULL;
    for (const char *const *l = row && label ? row->labels : NULL; l && *l; l++) {
        if (strcmp(*l, label) == 0) {
            return true;
        }
    }
    return false;
}

/* Hash one row's type definitions: QN -> its def object (both borrowed from
 * the returned doc, which the caller frees). A row that does not parse
 * returns NULL; the planner has validated every row it passes. */
static yyjson_doc *surface_row_types(const char *json, CBMHashTable *types, uint64_t *work) {
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    yyjson_val *lsp = doc ? yyjson_obj_get(yyjson_doc_get_root(doc), "lsp") : NULL;
    if (!yyjson_is_arr(lsp)) {
        return doc;
    }
    yyjson_arr_iter it = yyjson_arr_iter_with(lsp);
    for (yyjson_val *d = yyjson_arr_iter_next(&it); d; d = yyjson_arr_iter_next(&it)) {
        if (work) {
            (*work)++;
        }
        const char *qn = yyjson_get_str(yyjson_obj_get(d, "qn"));
        if (qn &&
            surface_type_label(yyjson_get_int(yyjson_obj_get(d, "lg")),
                               yyjson_get_str(yyjson_obj_get(d, "lb"))) &&
            !cbm_ht_get(types, qn)) {
            cbm_ht_set(types, qn, d);
        }
    }
    return doc;
}

typedef struct {
    const CBMHashTable *other; /* the row's types on the other side */
    CBMHashTable *gone;        /* QNs whose type definition changed or left (owned keys) */
    bool appeared;
} surface_type_cmp_t;

static void surface_type_appeared(const char *qn, void *val, void *ud) {
    (void)val;
    surface_type_cmp_t *c = (surface_type_cmp_t *)ud;
    if (!cbm_ht_get(c->other, qn)) {
        c->appeared = true;
    }
}

static void surface_type_changed(const char *qn, void *val, void *ud) {
    surface_type_cmp_t *c = (surface_type_cmp_t *)ud;
    yyjson_val *now = (yyjson_val *)cbm_ht_get(c->other, qn);
    if ((now && surface_obj_equal_except((yyjson_val *)val, now, "ty")) ||
        cbm_ht_get(c->gone, qn)) {
        return;
    }
    char *key = cbm_alloc(CBM_MEM_CLASS_OTHER, strlen(qn) + 1);
    if (key) {
        memcpy(key, qn, strlen(qn) + 1);
        cbm_ht_set(c->gone, key, key);
    }
}

static void surface_free_key(const char *key, void *val, void *ud) {
    (void)val;
    (void)ud;
    cbm_free(CBM_MEM_CLASS_OTHER, (void *)key);
}

/* True when one of the identities stored in `ty` ("spelling=QN|...") names a
 * QN of `gone`. */
static bool surface_names_gone(const char *ty, const CBMHashTable *gone, char **buf, size_t *cap,
                               uint64_t *work) {
    for (const char *p = ty; p && *p;) {
        const char *eq = strchr(p, '=');
        if (!eq) {
            return false;
        }
        const char *bar = strchr(eq, '|');
        size_t len = bar ? (size_t)(bar - eq - 1) : strlen(eq + 1);
        if (work) {
            *work += (uint64_t)(len + (size_t)(eq - p) + 1);
        }
        if (len + 1 > *cap) {
            size_t grown_cap = (len + 1) * 2;
            char *grown = cbm_realloc(CBM_MEM_CLASS_OTHER, *buf, grown_cap);
            if (!grown) {
                return true; /* cannot tell: decline */
            }
            *buf = grown;
            *cap = grown_cap;
        }
        memcpy(*buf, eq + 1, len);
        (*buf)[len] = '\0';
        if (cbm_ht_get(gone, *buf)) {
            return true;
        }
        p = bar ? bar + 1 : eq + 1 + len;
    }
    return false;
}

const char *cbm_lsp_surface_identity_decline(const char *const *old_rows,
                                             const char *const *new_rows, int changed_count,
                                             const cbm_lsp_surface_row_t *stored, int stored_count,
                                             uint64_t *work) {
    const char *decline = NULL;
    CBMHashTable *gone = cbm_ht_create(CBM_SZ_64);
    if (!gone) {
        return "alloc";
    }
    for (int i = 0; i < changed_count && !decline; i++) {
        CBMHashTable *before = cbm_ht_create(CBM_SZ_64);
        CBMHashTable *after = cbm_ht_create(CBM_SZ_64);
        yyjson_doc *old_doc = before ? surface_row_types(old_rows[i], before, work) : NULL;
        yyjson_doc *new_doc =
            after && new_rows[i] ? surface_row_types(new_rows[i], after, work) : NULL;
        if (!before || !after) {
            decline = "alloc";
        } else if (!old_doc || (new_rows[i] && !new_doc)) {
            decline = "surface_row_unreadable";
        } else {
            surface_type_cmp_t appear = {.other = before};
            cbm_ht_foreach(after, surface_type_appeared, &appear);
            surface_type_cmp_t change = {.other = after, .gone = gone};
            cbm_ht_foreach(before, surface_type_changed, &change);
            if (appear.appeared) {
                decline = "declared_types_changed";
            }
        }
        yyjson_doc_free(old_doc);
        yyjson_doc_free(new_doc);
        cbm_ht_free(before);
        cbm_ht_free(after);
    }
    /* A changed or departed type matters only when an identity names it:
     * read the stored identities (only rows that hold one are parsed). */
    char *buf = NULL;
    size_t cap = 0;
    for (int r = 0; r < stored_count && !decline && cbm_ht_count(gone) > 0; r++) {
        const char *json = stored[r].defs_json;
        if (work && json) {
            *work += (uint64_t)strlen(json);
        }
        if (!json || !strstr(json, "\"ty\":")) {
            continue;
        }
        yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
        yyjson_val *lsp = doc ? yyjson_obj_get(yyjson_doc_get_root(doc), "lsp") : NULL;
        if (!doc) {
            decline = "surface_row_unreadable";
        }
        yyjson_arr_iter it = yyjson_arr_iter_with(lsp);
        for (yyjson_val *d = lsp ? yyjson_arr_iter_next(&it) : NULL; d && !decline;
             d = yyjson_arr_iter_next(&it)) {
            const char *ty = yyjson_get_str(yyjson_obj_get(d, "ty"));
            if (ty && surface_names_gone(ty, gone, &buf, &cap, work)) {
                decline = "identity_target_changed";
            }
        }
        yyjson_doc_free(doc);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, buf);
    cbm_ht_foreach(gone, surface_free_key, NULL);
    cbm_ht_free(gone);
    return decline;
}

/* ── Scope facts: the "is" key ──────────────────────────────────────────
 *
 * A file's scope facts (cbm.h, CBMScopeEvent), stored as
 *   "is": {"v": SURFACE_SCOPE_V, "ev": [[kind, flags, rank, ...], ...], "im": [module path, ...]}
 * where rank orders the stored events by position (equal positions share a
 * rank): an edit that moves bytes but keeps the facts keeps the stored bytes,
 * and so the surface hash. Per kind: DEF [.., name, qualified name, qual],
 * INCLUDE [.., index into "im"], ASSIGN [.., name], OPAQUE [..]. QREF events
 * are not stored: a reference is read where its own file is re-extracted.
 * Written only for a language with a scope row, so every other file's bytes
 * stay as they were. A row of such a language written by an older binary has
 * no "is": a reader must take its facts as unknown, never as empty. */
enum { SURFACE_SCOPE_V = 1 };

static bool surface_scope_stored(const CBMFileResult *r, const CBMScopeEvent *e) {
    switch (e->kind) {
    case CBM_SCOPE_INCLUDE:
        return e->index >= 0 && e->index < r->imports.count &&
               r->imports.items[e->index].module_path;
    case CBM_SCOPE_DEF:
        return e->index >= 0 && e->index < r->defs.count && e->name &&
               r->defs.items[e->index].qualified_name;
    case CBM_SCOPE_ASSIGN:
        return e->name != NULL;
    case CBM_SCOPE_OPAQUE:
        return true;
    default:
        return false;
    }
}

static bool surface_arr_add_str_or_null(yyjson_mut_doc *doc, yyjson_mut_val *arr, const char *s) {
    return s ? yyjson_mut_arr_add_strcpy(doc, arr, s) : yyjson_mut_arr_add_null(doc, arr);
}

/* One stored event into `ev` (its module path into `im`). */
static bool surface_scope_event(yyjson_mut_doc *doc, yyjson_mut_val *ev, yyjson_mut_val *im,
                                int *nim, const CBMFileResult *r, const CBMScopeEvent *e,
                                int rank) {
    yyjson_mut_val *a = yyjson_mut_arr(doc);
    bool ok = a && yyjson_mut_arr_add_int(doc, a, e->kind) &&
              yyjson_mut_arr_add_int(doc, a, e->flags) && yyjson_mut_arr_add_int(doc, a, rank);
    switch (e->kind) {
    case CBM_SCOPE_INCLUDE:
        ok = ok && yyjson_mut_arr_add_strcpy(doc, im, r->imports.items[e->index].module_path) &&
             yyjson_mut_arr_add_int(doc, a, (*nim)++);
        break;
    case CBM_SCOPE_DEF:
        ok = ok && yyjson_mut_arr_add_strcpy(doc, a, e->name) &&
             yyjson_mut_arr_add_strcpy(doc, a, r->defs.items[e->index].qualified_name) &&
             surface_arr_add_str_or_null(doc, a, e->qual);
        break;
    case CBM_SCOPE_ASSIGN:
        ok = ok && yyjson_mut_arr_add_strcpy(doc, a, e->name);
        break;
    default:
        break;
    }
    return ok && yyjson_mut_arr_add_val(ev, a);
}

static bool surface_add_scope(yyjson_mut_doc *doc, yyjson_mut_val *root, const CBMFileResult *r,
                              CBMLanguage language) {
    if (!cbm_extract_scope_has_row(language)) {
        return true; /* no row: nothing to write */
    }
    yyjson_mut_val *is = yyjson_mut_obj(doc);
    yyjson_mut_val *ev = yyjson_mut_arr(doc);
    yyjson_mut_val *im = yyjson_mut_arr(doc);
    if (!is || !ev || !im) {
        return false;
    }
    int rank = -1;
    int64_t last = -1;
    int nim = 0;
    for (int i = 0; r && i < r->scope.count; i++) {
        const CBMScopeEvent *e = &r->scope.items[i];
        if (!surface_scope_stored(r, e)) {
            continue;
        }
        if ((int64_t)e->pos != last) {
            rank++;
            last = e->pos;
        }
        if (!surface_scope_event(doc, ev, im, &nim, r, e, rank)) {
            return false;
        }
    }
    return yyjson_mut_obj_add_int(doc, is, "v", SURFACE_SCOPE_V) &&
           yyjson_mut_obj_add_val(doc, is, "ev", ev) && yyjson_mut_obj_add_val(doc, is, "im", im) &&
           yyjson_mut_obj_add_val(doc, root, "is", is);
}

static const char *surface_json_strdup(CBMArena *a, yyjson_val *v) {
    const char *s = v ? yyjson_get_str(v) : NULL;
    return s ? cbm_arena_strdup(a, s) : NULL;
}

/* One stored event into e (its definition into defs[*ndefs] for a DEF);
 * false: malformed. */
static bool surface_scope_event_parse(CBMArena *a, yyjson_val *item, size_t nim, CBMScopeEvent *e,
                                      CBMDefinition *defs, int *ndefs) {
    if (!yyjson_is_arr(item) || yyjson_arr_size(item) < 3) {
        return false;
    }
    yyjson_val *kind = yyjson_arr_get(item, 0);
    yyjson_val *flags = yyjson_arr_get(item, 1);
    yyjson_val *rank = yyjson_arr_get(item, 2);
    if (!yyjson_is_int(kind) || !yyjson_is_int(flags) || !yyjson_is_int(rank) ||
        yyjson_get_int(rank) < 0) {
        return false;
    }
    memset(e, 0, sizeof(*e));
    e->kind = (uint8_t)yyjson_get_int(kind);
    e->flags = (uint8_t)yyjson_get_int(flags);
    e->pos = (uint32_t)yyjson_get_int(rank);
    e->index = -1;
    yyjson_val *x = yyjson_arr_get(item, 3);
    switch (e->kind) {
    case CBM_SCOPE_INCLUDE:
        e->index = (int32_t)yyjson_get_int(x);
        return yyjson_is_int(x) && e->index >= 0 && (size_t)e->index < nim;
    case CBM_SCOPE_DEF: {
        CBMDefinition *d = &defs[*ndefs];
        memset(d, 0, sizeof(*d));
        e->name = surface_json_strdup(a, x);
        e->qual = surface_json_strdup(a, yyjson_arr_get(item, 5));
        d->name = e->name;
        d->qualified_name = surface_json_strdup(a, yyjson_arr_get(item, 4));
        e->index = (*ndefs)++;
        return e->name && d->qualified_name;
    }
    case CBM_SCOPE_ASSIGN:
        e->name = surface_json_strdup(a, x);
        return e->name != NULL;
    case CBM_SCOPE_OPAQUE:
        return true;
    default:
        return false;
    }
}

int cbm_lsp_surface_scope_from_json(CBMArena *arena, const char *json, cbm_lsp_scope_facts_t *out) {
    if (!out) {
        return CBM_NOT_FOUND;
    }
    memset(out, 0, sizeof(*out));
    if (!arena || !json) {
        return CBM_NOT_FOUND;
    }
    yyjson_doc *jd = yyjson_read(json, strlen(json), 0);
    yyjson_val *root = jd ? yyjson_doc_get_root(jd) : NULL;
    yyjson_val *is = root ? yyjson_obj_get(root, "is") : NULL;
    if (!is) {
        yyjson_doc_free(jd);
        return jd ? 0 : CBM_NOT_FOUND;
    }
    yyjson_val *ver = yyjson_obj_get(is, "v");
    yyjson_val *ev = yyjson_obj_get(is, "ev");
    yyjson_val *im = yyjson_obj_get(is, "im");
    bool ok = yyjson_is_int(ver) && yyjson_get_int(ver) == SURFACE_SCOPE_V && yyjson_is_arr(ev) &&
              yyjson_is_arr(im);
    size_t nev = ok ? yyjson_arr_size(ev) : 0;
    size_t nim = ok ? yyjson_arr_size(im) : 0;
    CBMScopeEvent *events = nev ? cbm_arena_alloc(arena, nev * sizeof(*events)) : NULL;
    CBMDefinition *defs = nev ? cbm_arena_alloc(arena, nev * sizeof(*defs)) : NULL;
    const char **modules = nim ? cbm_arena_alloc(arena, nim * sizeof(*modules)) : NULL;
    ok = ok && (!nev || (events && defs)) && (!nim || modules);
    size_t idx;
    size_t max;
    yyjson_val *item;
    if (ok) {
        yyjson_arr_foreach(im, idx, max, item) {
            modules[idx] = surface_json_strdup(arena, item);
            ok = ok && modules[idx];
        }
    }
    int ndefs = 0;
    if (ok) {
        yyjson_arr_foreach(ev, idx, max, item) {
            ok = ok && surface_scope_event_parse(arena, item, nim, &events[idx], defs, &ndefs);
        }
    }
    yyjson_doc_free(jd);
    if (!ok) {
        return CBM_NOT_FOUND;
    }
    *out = (cbm_lsp_scope_facts_t){.events = events,
                                   .event_count = (int)nev,
                                   .defs = defs,
                                   .def_count = ndefs,
                                   .modules = modules,
                                   .module_count = (int)nim};
    return SKIP_ONE;
}

/* Serialize one file's surface: its slice of all_defs plus the registry-only
 * symbols from its raw extraction defs. Returns the JSON string (malloc'd, or
 * from `alc` when given) and its length. With identities, each def's
 * declared-type identities ("ty") are written as well: the stored row carries
 * them, the hashed bytes do not. */
static char *surface_file_to_json(const CBMFileResult *result, CBMLanguage language,
                                  const char *rel_path, const CBMLSPDef *defs, int def_count,
                                  bool identities, const yyjson_alc *alc, size_t *out_len) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    if ((unsigned)language >= CBM_LANG_COUNT ||
        !yyjson_mut_obj_add_int(doc, root, "v", SURFACE_CODEC_VERSION) ||
        !yyjson_mut_obj_add_int(doc, root, "lang", (int)language)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }

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
        add_str_or_null(doc, o, "al", d->alias_target);
        yyjson_mut_obj_add_bool(doc, o, "sg", d->is_singleton);
        add_str_or_null(doc, o, "mx", d->mixin_refs);
        /* Stored for the routes that resolve against this row without
         * re-extracting it; never hashed (see SURFACE_CODEC_VERSION). */
        if (identities && d->type_scope) {
            yyjson_mut_obj_add_str(doc, o, "ty", d->type_scope);
        }
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

    yyjson_mut_val *swift = NULL;
    for (int i = 0; result && i < result->defs.count; i++) {
        const CBMDefinition *d = &result->defs.items[i];
        if (!d->qn_sig_off || !d->qualified_name) {
            continue;
        }
        if (!swift) {
            swift = yyjson_mut_arr(doc);
        }
        yyjson_mut_val *o = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, o, "q", d->qualified_name);
        char defaults[CBM_SZ_16 + SKIP_ONE];
        (void)snprintf(defaults, sizeof(defaults), "%016llx",
                       (unsigned long long)d->param_default_mask);
        yyjson_mut_obj_add_strcpy(doc, o, "d", defaults);
        yyjson_mut_obj_add_int(doc, o, "p", d->selector_param_count);
        yyjson_mut_obj_add_str(doc, o, "s", d->call_selector ? d->call_selector : "");
        yyjson_mut_arr_add_val(swift, o);
    }
    if (swift) {
        yyjson_mut_obj_add_val(doc, root, "callables", swift);
    }

    /* #1916: an axios instance binding's baseURL is consumed by the files
     * that import it (their HTTP_CALLS compose base + path), so a changed
     * base must change the surface or those importers keep stale edges.
     * Written only when the file has such a binding: every other file's
     * surface bytes — and so its sha — stay exactly what they were. The
     * decoder ignores this key; it feeds the early-cutoff hash only. */
    yyjson_mut_val *http = NULL;
    for (int i = 0; result && i < result->defs.count; i++) {
        const CBMDefinition *d = &result->defs.items[i];
        if (!d->http_client || !d->qualified_name) {
            continue;
        }
        if (!http) {
            http = yyjson_mut_arr(doc);
        }
        yyjson_mut_val *o = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, o, "q", d->qualified_name);
        yyjson_mut_obj_add_str(doc, o, "c", d->http_client);
        add_str_or_null(doc, o, "b", d->http_base_url);
        yyjson_mut_arr_add_val(http, o);
    }
    if (http) {
        yyjson_mut_obj_add_val(doc, root, "http", http);
    }

    /* The doc-link scope (doclink.h): what other files' doc-comment
     * references resolve against -- namespaces, usings, type and member
     * declarations -- without line numbers, so a body edit keeps it. A
     * changed scope changes the hash, and the closure planner reads it to
     * decide whether a repair can stay local. Written only for files that
     * have one: every other file's surface bytes stay exactly as they were.
     * The defs decoder ignores this key; cbm_doclinks_scopes_from_surfaces
     * reads it back for the files an incremental run does not re-extract. */
    if (result && result->doc_scope) {
        /* a scope this run's reader refuses is stored as its language's
         * rejected marker: a later run then treats the file as this one does
         * (doc_links.h, scope_accepted) */
        char *marker = NULL;
        const char *kept = cbm_doclinks_storable_scope(result->doc_scope, &marker);
        char *portable = kept ? cbm_doclink_portable_scope(kept) : NULL;
        cbm_free(CBM_MEM_CLASS_OTHER, marker);
        if (!portable) {
            yyjson_mut_doc_free(doc);
            return NULL; /* a missing scope would diverge silently: fail the row */
        }
        yyjson_mut_obj_add_strcpy(doc, root, "dl", portable);
        cbm_free(CBM_MEM_CLASS_OTHER, portable);
    }

    if (!surface_add_python_namespace(doc, root, result, language, rel_path)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    if (!surface_add_scope(doc, root, result, language)) {
        yyjson_mut_doc_free(doc);
        return NULL; /* missing facts would diverge silently: fail the row */
    }
    /* Names, types and strings come from source text, which need not be UTF-8:
     * one such byte must not cost the repository every surface row. */
    char *json = cbm_json_write_text(doc, alc, out_len);
    yyjson_mut_doc_free(doc);
    return json;
}

/* One file's row, written in place: files are independent (their own result,
 * their own def slice), so the pass runs them in parallel. Sequentially it was
 * 1.15 s of the Go corpus's cross-LSP prepare (profile, 2026-09-17). */
typedef struct {
    const cbm_pipeline_ctx_t *ctx;
    const char *project;
    CBMFileResult **cache;
    const cbm_file_info_t *files;
    const CBMLSPDef *all_defs;
    const int *def_starts;
    cbm_lsp_surface_row_t *rows;
    int file_count;
    _Atomic int next_file;
    _Atomic bool failed;
} surface_job_t;

/* Row strings are released with cbm_store_free_lsp_surfaces (free), so they
 * come from the C heap: one place allocates them. */
static char *surface_row_alloc(size_t n) {
    return malloc(n);
}

static char *surface_row_strdup(const char *s) {
    size_t n = strlen(s);
    char *copy = surface_row_alloc(n + 1);
    if (copy) {
        memcpy(copy, s, n + 1);
    }
    return copy;
}

/* CBMFileResult.included_files, one file per line ("" when none). */
static char *surface_row_included_files(const CBMFileResult *fr) {
    size_t len = 0;
    for (const char *const *f = fr->included_files; f && *f; f++) {
        len += strlen(*f) + 1;
    }
    char *out = surface_row_alloc(len + 1);
    if (!out) {
        return NULL;
    }
    size_t at = 0;
    for (const char *const *f = fr->included_files; f && *f; f++) {
        size_t n = strlen(*f);
        memcpy(out + at, *f, n);
        at += n;
        out[at++] = '\n';
    }
    out[at] = '\0';
    return out;
}

static void surface_row_build(int i, void *arg) {
    surface_job_t *job = (surface_job_t *)arg;
    if (atomic_load_explicit(&job->failed, memory_order_relaxed)) {
        return;
    }
    bool loaded = false;
    CBMFileResult *fr = cbm_pipeline_result_acquire(job->ctx, job->cache, i, NULL, &loaded);
    if (!fr) {
        /* Never parsed this run (read/extract skip): no surface claim.
         * The routing layer treats a missing row as "must full-rebuild
         * before this file can be reasoned about", which is the correct
         * fail-closed default for an unreadable file. */
        return;
    }
    int start = job->def_starts ? job->def_starts[i] : 0;
    int end = job->def_starts ? job->def_starts[i + 1] : 0;
    const CBMLSPDef *slice = job->all_defs ? job->all_defs + start : NULL;
    bool identities = false;
    for (int d = 0; slice && d < end - start && !identities; d++) {
        identities = slice[d].type_scope != NULL;
    }
    size_t json_len = 0;
    /* The hash covers the row without identities; a row that has some is
     * written a second time with them, for storage (the hashed text is then
     * a memory-core buffer, released here). */
    char *json =
        surface_file_to_json(fr, job->files[i].language, job->files[i].rel_path, slice, end - start,
                             false, identities ? cbm_json_writer_allocator() : NULL, &json_len);
    char sha[CBM_SHA256_HEX_LEN + 1];
    if (json) {
        cbm_sha256_hex(json, json_len, sha);
    }
    if (json && identities) {
        size_t stored_len = 0;
        char *stored = surface_file_to_json(fr, job->files[i].language, job->files[i].rel_path,
                                            slice, end - start, true, NULL, &stored_len);
        cbm_free(CBM_MEM_CLASS_OTHER, json);
        json = stored;
    }
    char *included = surface_row_included_files(fr);
    cbm_pipeline_result_release(fr, loaded);
    if (!json || !included) {
        free(json);
        free(included);
        atomic_store_explicit(&job->failed, true, memory_order_relaxed);
        return;
    }
    cbm_lsp_surface_row_t *r = &job->rows[i];
    r->defs_json = json; /* set first: it marks the row present for the compaction */
    r->project = surface_row_strdup(job->project);
    r->rel_path = surface_row_strdup(job->files[i].rel_path);
    r->surface_sha = surface_row_strdup(sha);
    r->ref_bloom = NULL;
    r->ref_bloom_len = 0;
    r->config_ctx = surface_row_strdup("");
    r->included_files = included;
    if (!r->project || !r->rel_path || !r->surface_sha || !r->config_ctx) {
        atomic_store_explicit(&job->failed, true, memory_order_relaxed);
    }
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static cbm_lsp_surface_row_observer_fn g_row_observer;
static void *g_row_observer_ctx;

void cbm_lsp_surface_test_set_row_observer(cbm_lsp_surface_row_observer_fn observer, void *ctx) {
    g_row_observer = observer;
    g_row_observer_ctx = ctx;
}
#endif

static void surface_row_one(int i, void *arg) {
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (g_row_observer) {
        g_row_observer(i, true, g_row_observer_ctx);
    }
#endif
    surface_row_build(i, arg);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (g_row_observer) {
        g_row_observer(i, false, g_row_observer_ctx);
    }
#endif
}

/* The pool also runs work on its caller. Dispatch at most one coarse job per
 * allowed participant, each stealing files, so the total active row builders
 * cannot exceed the pipeline budget even when the caller takes a job. */
static void surface_rows_worker(int worker, void *arg) {
    (void)worker;
    surface_job_t *job = arg;
    for (;;) {
        int i = atomic_fetch_add_explicit(&job->next_file, 1, memory_order_relaxed);
        if (i >= job->file_count) {
            break;
        }
        surface_row_one(i, job);
    }
}

int cbm_lsp_surface_build_rows(const cbm_pipeline_ctx_t *ctx, const char *project,
                               CBMFileResult **cache, const cbm_file_info_t *files, int file_count,
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
    surface_job_t job = {
        .ctx = ctx,
        .project = project,
        .cache = cache,
        .files = files,
        .all_defs = all_defs,
        .def_starts = def_starts,
        .rows = rows,
        .file_count = file_count,
    };
    atomic_init(&job.next_file, 0);
    atomic_init(&job.failed, false);
    int workers = cbm_pipeline_worker_count(ctx ? ctx->pipeline : NULL);
    if (workers > file_count) {
        workers = file_count;
    }
    cbm_parallel_for(workers, surface_rows_worker, &job,
                     (cbm_parallel_for_opts_t){.max_workers = workers, .force_pthreads = false});
    if (atomic_load_explicit(&job.failed, memory_order_relaxed)) {
        cbm_store_free_lsp_surfaces(rows, file_count); /* untouched rows are all NULL */
        return -1;
    }
    /* Present rows to the front, in file order: the same rows, in the same
     * order, as the sequential loop produced. */
    int n = 0;
    for (int i = 0; i < file_count; i++) {
        if (!rows[i].defs_json) {
            continue;
        }
        if (n != i) {
            rows[n] = rows[i];
            memset(&rows[i], 0, sizeof(rows[i]));
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
    /* Iterate instead of yyjson_arr_get(i): that call is a linear scan for
     * non-flat arrays (arrays of objects are not flat), which turns this loop
     * into O(n^2) in the element count. */
    yyjson_arr_iter iter = yyjson_arr_iter_with(arr);
    int i = 0;
    yyjson_val *item;
    while ((item = yyjson_arr_iter_next(&iter))) {
        const char *s = yyjson_get_str(item);
        items[i++] = s ? cbm_arena_strdup(arena, s) : "?";
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
    CBMArena namespace_scratch;
    cbm_arena_init(&namespace_scratch);
    cbm_lsp_python_namespace_t namespace_payload = {0};
    int namespace_status =
        surface_python_namespace_parse(&namespace_scratch, root, &namespace_payload);
    cbm_arena_destroy(&namespace_scratch);
    yyjson_val *lsp = surface_unique_field(root, "lsp");
    if (namespace_status < 0) {
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
    /* Iterate instead of yyjson_arr_get(i). `lsp` is an array of objects, which
     * is not flat, so indexing rescans the preceding elements and the whole
     * decode becomes O(n^2). One generated data file with ~300k defs made this
     * step take minutes. */
    yyjson_arr_iter iter = yyjson_arr_iter_with(lsp);
    int i = 0;
    yyjson_val *o;
    while ((o = yyjson_arr_iter_next(&iter))) {
        CBMLSPDef *d = &defs[i++];
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
        d->alias_target = arena_str_or_null(arena, yyjson_obj_get(o, "al"));
        d->is_singleton = yyjson_get_bool(yyjson_obj_get(o, "sg"));
        d->mixin_refs = arena_str_or_null(arena, yyjson_obj_get(o, "mx"));
        d->type_scope = arena_str_or_null(arena, yyjson_obj_get(o, "ty"));
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
