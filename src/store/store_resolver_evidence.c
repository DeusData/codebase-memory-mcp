#include "store_resolver_evidence.h"
#include "foundation/json_writer.h"
#include "resolve/proof.h"
#include "resolve/snapshot.h"
#include <yyjson/yyjson.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const cbm_resolve_proof_t *node;
    size_t id;
    char digest[65];
} evidence_id_t;
typedef struct {
    sqlite3 *db;
    const char *project;
    sqlite3_stmt *fact, *support, *lookup;
    evidence_id_t *ids;
    size_t capacity, count;
    const cbm_resolve_file_t *const *files;
    size_t file_count;
} evidence_write_t;

static const char *text(const char *value) {
    return value ? value : "";
}
static bool bind_text(sqlite3_stmt *s, int col, const char *value) {
    return sqlite3_bind_text(s, col, text(value), -1, SQLITE_TRANSIENT) == SQLITE_OK;
}
static bool finish_row(sqlite3_stmt *s, bool ok) {
    ok = ok && sqlite3_step(s) == SQLITE_DONE;
    sqlite3_reset(s);
    sqlite3_clear_bindings(s);
    return ok;
}
static size_t pointer_hash(const cbm_resolve_proof_t *node) {
    uintptr_t value = (uintptr_t)node >> 3;
    return (size_t)(value ^ (value >> 17));
}
static evidence_id_t *id_slot(evidence_write_t *w, const cbm_resolve_proof_t *node) {
    size_t at = pointer_hash(node) & (w->capacity - 1);
    while (w->ids[at].node && w->ids[at].node != node)
        at = (at + 1) & (w->capacity - 1);
    return &w->ids[at];
}
static evidence_id_t *find_id(evidence_write_t *w, const cbm_resolve_proof_t *node) {
    if (!node || !w->capacity)
        return NULL;
    evidence_id_t *entry = id_slot(w, node);
    return entry->node ? entry : NULL;
}
static bool grow_ids(evidence_write_t *w) {
    if (w->capacity && w->count < w->capacity / 2)
        return true;
    size_t next = w->capacity ? w->capacity * 2 : 64;
    if (next < w->capacity || next > SIZE_MAX / sizeof(*w->ids))
        return false;
    evidence_id_t *ids = cbm_calloc(CBM_MEM_CLASS_STORE, next * sizeof(*ids));
    if (!ids)
        return false;
    evidence_id_t *old = w->ids;
    size_t old_capacity = w->capacity;
    w->ids = ids;
    w->capacity = next;
    for (size_t i = 0; i < old_capacity; i++)
        if (old[i].node)
            *id_slot(w, old[i].node) = old[i];
    cbm_free(CBM_MEM_CLASS_STORE, old);
    return true;
}
static int compare_files(const void *a, const void *b) {
    return strcmp(text((*(const cbm_resolve_file_t *const *)a)->file_path),
                  text((*(const cbm_resolve_file_t *const *)b)->file_path));
}
static const cbm_resolve_file_t *find_file(const evidence_write_t *w, const char *path) {
    size_t lo = 0, hi = w->file_count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (strcmp(text(w->files[mid]->file_path), text(path)) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < w->file_count && !strcmp(text(w->files[lo]->file_path), text(path)) ? w->files[lo]
                                                                                    : NULL;
}
static bool source_matches(const evidence_write_t *w, const cbm_resolve_proof_source_t *s) {
    /* Missing context remains explicitly unknown. Whenever a captured source
     * identity is claimed, it must identify this generation's exact snapshot. */
    if (!s->content_identity || !*s->content_identity)
        return true;
    const cbm_resolve_file_t *file = find_file(w, s->file);
    if (!file || s->source_origin > 1 ||
        (s->project && *s->project && strcmp(s->project, w->project)))
        return false;
    const char *content =
        s->source_origin ? file->preprocessed_content_identity : file->raw_content_identity;
    return !strcmp(text(content), s->content_identity) &&
           !strcmp(text(file->configuration_identity), text(s->configuration_identity));
}
static char *fact_json(const cbm_resolve_proof_view_t *v) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc)
        return NULL;
    yyjson_mut_val *obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, obj);
    bool ok = obj != NULL;
#define STR(name, value) ok = ok && yyjson_mut_obj_add_str(doc, obj, name, text(value))
#define NUM(name, value) ok = ok && yyjson_mut_obj_add_uint(doc, obj, name, (uint64_t)(value))
    NUM("kind", v->kind);
    STR("project", v->source.project);
    STR("file", v->source.file);
    STR("module", v->source.module);
    STR("scope", v->source.scope);
    STR("content_identity", v->source.content_identity);
    STR("configuration_identity", v->source.configuration_identity);
    NUM("source_origin", v->source.source_origin);
    STR("lookup_domain", v->key.lookup_domain);
    STR("lookup_key", v->key.lookup_key);
    STR("declaration_identity", v->key.declaration_identity);
    STR("binding_identity", v->key.binding_identity);
    STR("type_identity", v->key.type_identity);
    STR("detail", v->key.detail);
    NUM("start_byte", v->key.start_byte);
    NUM("end_byte", v->key.end_byte);
    NUM("origin", v->key.origin);
    NUM("flags", v->key.flags);
    NUM("lookup_closed", v->key.lookup_closed);
    NUM("binding", v->outcome.binding);
    NUM("domain", v->outcome.domain);
    NUM("analysis", v->outcome.analysis);
    NUM("rule", v->outcome.rule);
    NUM("facts", v->outcome.facts);
    STR("target", v->outcome.target);
    NUM("candidate_count", v->outcome.candidate_count);
    STR("reason", v->outcome.reason);
    NUM("predecessors_closed", v->predecessors_closed);
#undef STR
#undef NUM
    char *json = ok ? yyjson_mut_write_opts(doc, 0, cbm_json_writer_allocator(), NULL, NULL) : NULL;
    yyjson_mut_doc_free(doc);
    return json;
}
static const char *lookup_status(const cbm_resolve_proof_view_t *v) {
    if (v->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || !v->predecessors_closed ||
        !v->key.lookup_closed)
        return "incomplete";
    if (v->outcome.binding == CBM_RESOLVE_BINDING_AMBIGUOUS)
        return "ambiguous";
    if (v->outcome.binding == CBM_RESOLVE_BINDING_UNIQUE)
        return "positive";
    return "closed_negative";
}
static bool write_fact(const cbm_resolve_proof_t *node, void *context) {
    evidence_write_t *w = context;
    const cbm_resolve_proof_view_t *v = cbm_resolve_proof_view(node);
    if (!v || !source_matches(w, &v->source) || !grow_ids(w))
        return false;
    char *json = fact_json(v);
    if (!json)
        return false;
    evidence_id_t entry = {.node = node, .id = w->count + 1};
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    cbm_resolve_hash_text(&hash, json);
    bool ok = true;
    for (size_t i = 0; ok && i < v->premise_count; i++) {
        evidence_id_t *premise = find_id(w, v->premises[i]);
        ok = premise != NULL;
        if (ok) {
            cbm_resolve_hash_text(&hash, premise->digest);
            ok = finish_row(
                w->support,
                bind_text(w->support, 1, w->project) &&
                    sqlite3_bind_int64(w->support, 2, (sqlite3_int64)entry.id) == SQLITE_OK &&
                    sqlite3_bind_int64(w->support, 3, (sqlite3_int64)i) == SQLITE_OK &&
                    sqlite3_bind_int64(w->support, 4, (sqlite3_int64)premise->id) == SQLITE_OK);
        }
    }
    cbm_resolve_hash_finish(&hash, entry.digest);
    if (ok)
        ok = finish_row(w->fact,
                        bind_text(w->fact, 1, w->project) &&
                            sqlite3_bind_int64(w->fact, 2, (sqlite3_int64)entry.id) == SQLITE_OK &&
                            sqlite3_bind_int(w->fact, 3, (int)v->kind) == SQLITE_OK &&
                            bind_text(w->fact, 4, v->source.file) &&
                            bind_text(w->fact, 5, entry.digest) && bind_text(w->fact, 6, json));
    if (ok && v->key.lookup_domain && v->key.lookup_key)
        ok = finish_row(w->lookup, bind_text(w->lookup, 1, w->project) &&
                                       sqlite3_bind_int64(w->lookup, 2, (sqlite3_int64)entry.id) ==
                                           SQLITE_OK &&
                                       bind_text(w->lookup, 3, v->key.lookup_domain) &&
                                       bind_text(w->lookup, 4, v->key.lookup_key) &&
                                       bind_text(w->lookup, 5, lookup_status(v)));
    cbm_free(CBM_MEM_CLASS_OTHER, json);
    if (ok) {
        *id_slot(w, node) = entry;
        w->count++;
    }
    return ok;
}
static bool reset_tables(sqlite3 *db, const char *project) {
    const char *schema =
        "CREATE TABLE IF NOT EXISTS resolver_facts(project TEXT NOT NULL,id INTEGER NOT NULL,"
        "kind INTEGER NOT NULL,source_file TEXT NOT NULL,digest TEXT NOT NULL,payload TEXT NOT "
        "NULL,PRIMARY KEY(project,id));"
        "CREATE INDEX IF NOT EXISTS resolver_facts_file_kind ON "
        "resolver_facts(project,source_file,kind);"
        "CREATE TABLE IF NOT EXISTS resolver_supports(project TEXT NOT NULL,fact INTEGER NOT NULL,"
        "ordinal INTEGER NOT NULL,premise INTEGER NOT NULL,PRIMARY KEY(project,fact,ordinal));"
        "CREATE TABLE IF NOT EXISTS resolver_lookups(project TEXT NOT NULL,fact INTEGER NOT NULL,"
        "domain TEXT NOT NULL,key TEXT NOT NULL,status TEXT NOT NULL,PRIMARY KEY(project,fact));"
        "CREATE INDEX IF NOT EXISTS resolver_lookups_key ON resolver_lookups(project,domain,key);"
        "CREATE TABLE IF NOT EXISTS resolver_occurrence_evidence(project TEXT NOT NULL,"
        "ordinal INTEGER NOT NULL,content_identity TEXT NOT NULL,configuration_identity TEXT NOT "
        "NULL,"
        "proof INTEGER NOT NULL,source_binding_proof INTEGER NOT NULL,PRIMARY "
        "KEY(project,ordinal));"
        "CREATE TABLE IF NOT EXISTS resolver_file_summaries(project TEXT NOT NULL,file_path TEXT "
        "NOT NULL,"
        "module TEXT NOT NULL,language INTEGER NOT NULL,raw_content_identity TEXT NOT NULL,"
        "preprocessed_content_identity TEXT NOT NULL,configuration_identity TEXT NOT NULL,"
        "raw_length INTEGER NOT NULL,preprocessed_length INTEGER NOT NULL,extracted_summary TEXT "
        "NOT NULL,"
        "derived_summary TEXT NOT NULL,complete INTEGER NOT NULL,PRIMARY KEY(project,file_path));"
        "CREATE TABLE IF NOT EXISTS resolver_derived_bindings(project TEXT NOT NULL,file_path TEXT "
        "NOT NULL,"
        "key TEXT NOT NULL,kind INTEGER NOT NULL,module INTEGER NOT NULL,closed INTEGER NOT NULL,"
        "binding INTEGER NOT NULL,domain INTEGER NOT NULL,analysis INTEGER NOT NULL,"
        "target TEXT NOT NULL,proof INTEGER NOT NULL,PRIMARY KEY(project,file_path,key,kind));";
    if (sqlite3_exec(db, schema, NULL, NULL, NULL) != SQLITE_OK)
        return false;
    const char *deletes[] = {"DELETE FROM resolver_facts WHERE project=?",
                             "DELETE FROM resolver_supports WHERE project=?",
                             "DELETE FROM resolver_lookups WHERE project=?",
                             "DELETE FROM resolver_occurrence_evidence WHERE project=?",
                             "DELETE FROM resolver_file_summaries WHERE project=?",
                             "DELETE FROM resolver_derived_bindings WHERE project=?"};
    for (size_t i = 0; i < sizeof(deletes) / sizeof(deletes[0]); i++) {
        sqlite3_stmt *s = NULL;
        bool ok = sqlite3_prepare_v2(db, deletes[i], -1, &s, NULL) == SQLITE_OK &&
                  bind_text(s, 1, project) && sqlite3_step(s) == SQLITE_DONE;
        sqlite3_finalize(s);
        if (!ok)
            return false;
    }
    return true;
}
static bool write_occurrences(evidence_write_t *w, const cbm_resolve_record_t *rows, size_t count) {
    sqlite3_stmt *s = NULL;
    bool ok =
        sqlite3_prepare_v2(w->db, "INSERT INTO resolver_occurrence_evidence VALUES(?,?,?,?,?,?)",
                           -1, &s, NULL) == SQLITE_OK;
    for (size_t i = 0; ok && i < count; i++) {
        evidence_id_t *proof = find_id(w, rows[i].result.proof);
        evidence_id_t *binding = find_id(w, rows[i].binding_proof);
        cbm_resolve_proof_source_t source = {.project = w->project,
                                             .file = rows[i].file_path,
                                             .content_identity = rows[i].content_identity,
                                             .configuration_identity =
                                                 rows[i].configuration_identity,
                                             .source_origin = rows[i].source_origin};
        ok = !rows[i].proof_borrowed &&
             (!rows[i].emitted || (proof && source_matches(w, &source))) &&
             finish_row(s, bind_text(s, 1, w->project) &&
                               sqlite3_bind_int64(s, 2, (sqlite3_int64)i) == SQLITE_OK &&
                               bind_text(s, 3, rows[i].content_identity) &&
                               bind_text(s, 4, rows[i].configuration_identity) &&
                               sqlite3_bind_int64(s, 5, proof ? (sqlite3_int64)proof->id : 0) ==
                                   SQLITE_OK &&
                               sqlite3_bind_int64(s, 6, binding ? (sqlite3_int64)binding->id : 0) ==
                                   SQLITE_OK);
    }
    sqlite3_finalize(s);
    return ok;
}
static int compare_summaries(const void *a, const void *b) {
    const cbm_resolve_summary_t *left = *(const cbm_resolve_summary_t *const *)a;
    const cbm_resolve_summary_t *right = *(const cbm_resolve_summary_t *const *)b;
    int order = strcmp(text(left->file_path), text(right->file_path));
    if (!order)
        order = strcmp(text(left->key), text(right->key));
    if (!order && left->kind != right->kind)
        order = left->kind < right->kind ? -1 : 1;
    return order;
}
static bool write_summaries(evidence_write_t *w, const cbm_resolve_summary_t *const *rows,
                            size_t count) {
    sqlite3_stmt *s = NULL;
    bool ok = sqlite3_prepare_v2(
                  w->db, "INSERT INTO resolver_derived_bindings VALUES(?,?,?,?,?,?,?,?,?,?,?)", -1,
                  &s, NULL) == SQLITE_OK;
    for (size_t i = 0; ok && i < count; i++) {
        const cbm_resolve_summary_t *row = rows[i];
        evidence_id_t *proof = find_id(w, row->result.proof);
        ok = !row->proof_borrowed &&
             finish_row(s, bind_text(s, 1, w->project) && bind_text(s, 2, row->file_path) &&
                               bind_text(s, 3, row->key) &&
                               sqlite3_bind_int64(s, 4, row->kind) == SQLITE_OK &&
                               sqlite3_bind_int(s, 5, row->module) == SQLITE_OK &&
                               sqlite3_bind_int(s, 6, row->closed) == SQLITE_OK &&
                               sqlite3_bind_int(s, 7, row->result.binding) == SQLITE_OK &&
                               sqlite3_bind_int(s, 8, row->result.domain) == SQLITE_OK &&
                               sqlite3_bind_int(s, 9, row->result.analysis) == SQLITE_OK &&
                               bind_text(s, 10, row->result.target) &&
                               sqlite3_bind_int64(s, 11, proof ? (sqlite3_int64)proof->id : 0) ==
                                   SQLITE_OK);
    }
    sqlite3_finalize(s);
    return ok;
}
static bool write_files(evidence_write_t *w, const cbm_resolve_summary_t *const *rows,
                        size_t count) {
    sqlite3_stmt *s = NULL;
    bool ok = sqlite3_prepare_v2(
                  w->db, "INSERT INTO resolver_file_summaries VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", -1,
                  &s, NULL) == SQLITE_OK;
    size_t at = 0;
    for (size_t i = 0; ok && i < w->file_count; i++) {
        const cbm_resolve_file_t *file = w->files[i];
        cbm_sha256_ctx hash;
        cbm_sha256_init(&hash);
        cbm_resolve_hash_text(&hash, "derived-bindings-v1");
        while (at < count && strcmp(text(rows[at]->file_path), text(file->file_path)) < 0)
            at++;
        while (at < count && !strcmp(text(rows[at]->file_path), text(file->file_path))) {
            const cbm_resolve_summary_t *row = rows[at++];
            evidence_id_t *proof = find_id(w, row->result.proof);
            cbm_resolve_hash_text(&hash, row->key);
            cbm_resolve_hash_number(&hash, row->kind);
            cbm_resolve_hash_number(&hash, row->module);
            cbm_resolve_hash_number(&hash, row->closed);
            cbm_resolve_hash_number(&hash, row->result.binding);
            cbm_resolve_hash_number(&hash, row->result.domain);
            cbm_resolve_hash_number(&hash, row->result.analysis);
            cbm_resolve_hash_text(&hash, row->result.target);
            cbm_resolve_hash_text(&hash, proof ? proof->digest : "");
        }
        /* Observed type states are derived facts too. The indexed per-file
         * query avoids a file-by-corpus scan, and includes unknown/limited
         * states instead of hashing successful edges alone. */
        sqlite3_stmt *types = NULL;
        ok = sqlite3_prepare_v2(
                 w->db,
                 "SELECT digest FROM resolver_facts WHERE project=?1 AND source_file=?2 "
                 "AND kind IN (?3,?4,?5) ORDER BY digest,payload",
                 -1, &types, NULL) == SQLITE_OK &&
             bind_text(types, 1, w->project) && bind_text(types, 2, file->file_path) &&
             sqlite3_bind_int(types, 3, CBM_PROOF_TYPE_BIND) == SQLITE_OK &&
             sqlite3_bind_int(types, 4, CBM_PROOF_TYPE_COPY) == SQLITE_OK &&
             sqlite3_bind_int(types, 5, CBM_PROOF_TYPE_JOIN) == SQLITE_OK;
        int step = SQLITE_DONE;
        while (ok && (step = sqlite3_step(types)) == SQLITE_ROW)
            cbm_resolve_hash_text(&hash, (const char *)sqlite3_column_text(types, 0));
        ok = ok && step == SQLITE_DONE;
        sqlite3_finalize(types);
        if (!ok)
            break;
        char derived[65];
        cbm_resolve_hash_finish(&hash, derived);
        ok = finish_row(
            s,
            bind_text(s, 1, w->project) && bind_text(s, 2, file->file_path) &&
                bind_text(s, 3, file->module) &&
                sqlite3_bind_int64(s, 4, file->language) == SQLITE_OK &&
                bind_text(s, 5, file->raw_content_identity) &&
                bind_text(s, 6, file->preprocessed_content_identity) &&
                bind_text(s, 7, file->configuration_identity) &&
                sqlite3_bind_int64(s, 8, (sqlite3_int64)file->raw_length) == SQLITE_OK &&
                sqlite3_bind_int64(s, 9, (sqlite3_int64)file->preprocessed_length) == SQLITE_OK &&
                bind_text(s, 10, file->extracted_summary) && bind_text(s, 11, derived) &&
                sqlite3_bind_int(s, 12, file->complete) == SQLITE_OK);
    }
    sqlite3_finalize(s);
    return ok;
}
bool cbm_store_resolver_evidence(sqlite3 *db, const char *project, const cbm_resolve_record_t *rows,
                                 size_t count, const cbm_resolve_file_t *files, size_t file_count,
                                 const cbm_resolve_summary_t *summaries, size_t summary_count) {
    if (!db || !project || (!rows && count) || (!files && file_count) ||
        (!summaries && summary_count) || count > (SIZE_MAX - summary_count) / 2 ||
        file_count > SIZE_MAX / sizeof(void *) || summary_count > SIZE_MAX / sizeof(void *))
        return false;
    size_t root_capacity = count * 2 + summary_count;
    if (root_capacity > SIZE_MAX / sizeof(void *))
        return false;
    const cbm_resolve_proof_t **roots =
        root_capacity ? cbm_alloc(CBM_MEM_CLASS_STORE, root_capacity * sizeof(*roots)) : NULL;
    const cbm_resolve_file_t **ordered_files =
        file_count ? cbm_alloc(CBM_MEM_CLASS_STORE, file_count * sizeof(*ordered_files)) : NULL;
    const cbm_resolve_summary_t **ordered_summaries =
        summary_count ? cbm_alloc(CBM_MEM_CLASS_STORE, summary_count * sizeof(*ordered_summaries))
                      : NULL;
    bool ok = (!root_capacity || roots) && (!file_count || ordered_files) &&
              (!summary_count || ordered_summaries);
    size_t root_count = 0;
    for (size_t i = 0; ok && i < count; i++) {
        if (rows[i].result.proof)
            roots[root_count++] = rows[i].result.proof;
        if (rows[i].binding_proof)
            roots[root_count++] = rows[i].binding_proof;
    }
    for (size_t i = 0; ok && i < summary_count; i++) {
        ordered_summaries[i] = &summaries[i];
        if (summaries[i].result.proof)
            roots[root_count++] = summaries[i].result.proof;
    }
    for (size_t i = 0; ok && i < file_count; i++)
        ordered_files[i] = &files[i];
    if (ok && file_count > 1)
        qsort(ordered_files, file_count, sizeof(*ordered_files), compare_files);
    if (ok && summary_count > 1)
        qsort(ordered_summaries, summary_count, sizeof(*ordered_summaries), compare_summaries);
    evidence_write_t w = {
        .db = db, .project = project, .files = ordered_files, .file_count = file_count};
    ok = ok && reset_tables(db, project) &&
         sqlite3_prepare_v2(db, "INSERT INTO resolver_facts VALUES(?,?,?,?,?,?)", -1, &w.fact,
                            NULL) == SQLITE_OK &&
         sqlite3_prepare_v2(db, "INSERT INTO resolver_supports VALUES(?,?,?,?)", -1, &w.support,
                            NULL) == SQLITE_OK &&
         sqlite3_prepare_v2(db, "INSERT INTO resolver_lookups VALUES(?,?,?,?,?)", -1, &w.lookup,
                            NULL) == SQLITE_OK &&
         cbm_resolve_proof_visit_roots(roots, root_count, 16u * 1024u * 1024u, write_fact, &w) &&
         write_occurrences(&w, rows, count) &&
         write_summaries(&w, ordered_summaries, summary_count) &&
         write_files(&w, ordered_summaries, summary_count);
    sqlite3_finalize(w.fact);
    sqlite3_finalize(w.support);
    sqlite3_finalize(w.lookup);
    cbm_free(CBM_MEM_CLASS_STORE, w.ids);
    cbm_free(CBM_MEM_CLASS_STORE, roots);
    cbm_free(CBM_MEM_CLASS_STORE, ordered_files);
    cbm_free(CBM_MEM_CLASS_STORE, ordered_summaries);
    return ok;
}
