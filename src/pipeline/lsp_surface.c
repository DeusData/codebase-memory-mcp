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
#include "pipeline/pipeline_internal.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "cbm.h" /* cbm_label_is_relation — reg-only surface membership */
#include "foundation/log.h"
#include "foundation/sha256.h"
#include "pipeline/worker_pool.h"
#include "yyjson/yyjson.h"

enum { SURFACE_CODEC_VERSION = 2 };

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
        result = (old_state == 0 && new_state == 0)                                         ? 0
                 : (old_state != new_state || !new_json || strcmp(old_json, new_json) != 0) ? 1
                                                                                            : 0;
    }
    cbm_arena_destroy(&scratch);
    yyjson_doc_free(old_doc);
    yyjson_doc_free(new_doc);
    return result;
}

/* Serialize one file's surface: its slice of all_defs plus the registry-only
 * symbols from its raw extraction defs. Returns a malloc'd JSON string and its
 * length. */
static char *surface_file_to_json(const CBMFileResult *result, CBMLanguage language,
                                  const char *rel_path, const CBMLSPDef *defs, int def_count,
                                  size_t *out_len) {
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

    if (!surface_add_python_namespace(doc, root, result, language, rel_path)) {
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    char *json = yyjson_mut_write(doc, 0, out_len);
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
    _Atomic bool failed;
} surface_job_t;

static void surface_row_one(int i, void *arg) {
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
    size_t json_len = 0;
    char *json =
        surface_file_to_json(fr, job->files[i].language, job->files[i].rel_path,
                             job->all_defs ? job->all_defs + start : NULL, end - start, &json_len);
    cbm_pipeline_result_release(fr, loaded);
    if (!json) {
        atomic_store_explicit(&job->failed, true, memory_order_relaxed);
        return;
    }
    char sha[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(json, json_len, sha);
    cbm_lsp_surface_row_t *r = &job->rows[i];
    r->defs_json = json; /* set first: it marks the row present for the compaction */
    r->project = strdup(job->project);
    r->rel_path = strdup(job->files[i].rel_path);
    r->surface_sha = strdup(sha);
    r->ref_bloom = NULL;
    r->ref_bloom_len = 0;
    r->config_ctx = strdup("");
    if (!r->project || !r->rel_path || !r->surface_sha || !r->config_ctx) {
        atomic_store_explicit(&job->failed, true, memory_order_relaxed);
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
    };
    atomic_init(&job.failed, false);
    cbm_parallel_for(file_count, surface_row_one, &job,
                     (cbm_parallel_for_opts_t){.max_workers = 0, .force_pthreads = false});
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
