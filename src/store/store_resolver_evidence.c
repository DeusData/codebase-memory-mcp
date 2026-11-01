#include "resolve/stats.h"
#include "store_resolver_evidence.h"
#include "arena.h"
#include "foundation/json_writer.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h"
#include "resolve/policy.h"
#include "resolve/proof.h"
#include "resolve/snapshot.h"
#include <yyjson/yyjson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Storage layout. A generation holds one row per occurrence and per fact, and
 * nearly every column of those rows is a repeated string: project, file, scope,
 * reason, two 64-character content identities. Writing them out made the
 * database 10-30x the graph it describes, and the WAL of its one transaction
 * larger still. So the rows hold integers, every string lives once in
 * resolver_text, and the logical tables are views that read back exactly what
 * was written. Nothing is dropped: resolver_occurrences, resolver_facts,
 * resolver_supports, resolver_lookups and resolver_occurrence_evidence keep
 * their names, columns and values, including a fact's digest and payload.
 *
 * A NULL text id means the empty string. Rows are inserted in primary-key order
 * and no secondary index exists, so every b-tree grows at its right edge and a
 * page is written once; that is what keeps the WAL the size of the data. */

/* resolver_lookups derives a fact's lookup status from its own columns, and the
 * expression spells these enum values. */
_Static_assert(CBM_RESOLVE_ANALYSIS_COMPLETE == 1, "resolver_lookups view encodes this value");
_Static_assert(CBM_RESOLVE_BINDING_UNIQUE == 1, "resolver_lookups view encodes this value");
_Static_assert(CBM_RESOLVE_BINDING_AMBIGUOUS == 2, "resolver_lookups view encodes this value");

/* @o.file@ reads the string behind that id; schema_sql spells out the lookup. */
static const char resolver_schema[] =
    "CREATE TABLE IF NOT EXISTS resolver_projects(pid INTEGER PRIMARY KEY,"
    "project TEXT NOT NULL UNIQUE,rules_version INTEGER NOT NULL);"
    "CREATE TABLE IF NOT EXISTS resolver_text(pid INTEGER NOT NULL,id INTEGER NOT NULL,"
    "value TEXT NOT NULL,PRIMARY KEY(pid,id)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS resolver_occurrence_rows(pid INTEGER NOT NULL,"
    "ordinal INTEGER NOT NULL,file INTEGER,scope INTEGER,relation INTEGER,spelling INTEGER,"
    "start_byte INTEGER NOT NULL,span INTEGER NOT NULL,source_origin INTEGER NOT NULL,"
    "binding INTEGER NOT NULL,domain INTEGER NOT NULL,analysis INTEGER NOT NULL,"
    "rule INTEGER NOT NULL,facts INTEGER NOT NULL,target INTEGER,"
    "candidate_count INTEGER NOT NULL,reason INTEGER,emitted INTEGER NOT NULL,props INTEGER,"
    "line INTEGER NOT NULL,content INTEGER,configuration INTEGER,proof INTEGER NOT NULL,"
    "source_binding_proof INTEGER NOT NULL,PRIMARY KEY(pid,ordinal)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS resolver_fact_rows(pid INTEGER NOT NULL,id INTEGER NOT NULL,"
    "kind INTEGER NOT NULL,digest BLOB NOT NULL,source_project INTEGER,file INTEGER,"
    "module INTEGER,scope INTEGER,content INTEGER,configuration INTEGER,"
    "source_origin INTEGER NOT NULL,lookup_domain INTEGER,lookup_key INTEGER,"
    "declaration INTEGER,binding_identity INTEGER,type_identity INTEGER,detail INTEGER,"
    "start_byte INTEGER NOT NULL,end_byte INTEGER NOT NULL,origin INTEGER NOT NULL,"
    "flags INTEGER NOT NULL,lookup_closed INTEGER NOT NULL,binding INTEGER NOT NULL,"
    "domain INTEGER NOT NULL,analysis INTEGER NOT NULL,rule INTEGER NOT NULL,"
    "facts INTEGER NOT NULL,target INTEGER,candidate_count INTEGER NOT NULL,reason INTEGER,"
    "predecessors_closed INTEGER NOT NULL,PRIMARY KEY(pid,id)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS resolver_support_rows(pid INTEGER NOT NULL,"
    "fact INTEGER NOT NULL,ordinal INTEGER NOT NULL,premise INTEGER NOT NULL,"
    "PRIMARY KEY(pid,fact,ordinal)) WITHOUT ROWID;"
    "CREATE TABLE IF NOT EXISTS resolver_file_summaries(project TEXT NOT NULL,"
    "file_path TEXT NOT NULL,module TEXT NOT NULL,language INTEGER NOT NULL,"
    "raw_content_identity TEXT NOT NULL,preprocessed_content_identity TEXT NOT NULL,"
    "configuration_identity TEXT NOT NULL,raw_length INTEGER NOT NULL,"
    "preprocessed_length INTEGER NOT NULL,extracted_summary TEXT NOT NULL,"
    "derived_summary TEXT NOT NULL,complete INTEGER NOT NULL,"
    "PRIMARY KEY(project,file_path));"
    "CREATE TABLE IF NOT EXISTS resolver_derived_bindings(project TEXT NOT NULL,"
    "file_path TEXT NOT NULL,key TEXT NOT NULL,kind INTEGER NOT NULL,module INTEGER NOT NULL,"
    "closed INTEGER NOT NULL,binding INTEGER NOT NULL,domain INTEGER NOT NULL,"
    "analysis INTEGER NOT NULL,target TEXT NOT NULL,proof INTEGER NOT NULL,"
    "payload_json TEXT NOT NULL DEFAULT '{}',PRIMARY KEY(project,file_path,key,kind));"
    "CREATE VIEW IF NOT EXISTS resolver_occurrences(project,ordinal,file_path,scope,relation,"
    "spelling,start_byte,end_byte,source_origin,binding,domain,analysis,rule,facts,target,"
    "candidate_count,reason,emitted,rules_version,properties_json) AS "
    "SELECT p.project,o.ordinal,@o.file@,@o.scope@,@o.relation@,@o.spelling@,o.start_byte,"
    "o.start_byte+o.span,o.source_origin,o.binding,o.domain,o.analysis,o.rule,o.facts,"
    "@o.target@,o.candidate_count,@o.reason@,o.emitted,p.rules_version,"
    "CASE WHEN o.line<0 THEN @o.props@ ELSE "
    "'{\"strategy\":\"shared_resolver\",\"resolver_rule\":'||o.rule||"
    "',\"start_byte\":'||o.start_byte||',\"end_byte\":'||(o.start_byte+o.span)||"
    "',\"source_origin\":'||o.source_origin||"
    "CASE WHEN o.line>0 THEN replace(@o.props@,char(1),',\"line\":'||o.line) "
    "ELSE @o.props@ END END "
    "FROM resolver_projects p JOIN resolver_occurrence_rows o ON o.pid=p.pid;"
    "CREATE VIEW IF NOT EXISTS resolver_occurrence_evidence(project,ordinal,content_identity,"
    "configuration_identity,proof,source_binding_proof) AS "
    "SELECT p.project,o.ordinal,@o.content@,@o.configuration@,o.proof,o.source_binding_proof "
    "FROM resolver_projects p JOIN resolver_occurrence_rows o ON o.pid=p.pid;"
    "CREATE VIEW IF NOT EXISTS resolver_facts(project,id,kind,source_file,digest,payload) AS "
    "SELECT p.project,f.id,f.kind,@f.file@,lower(hex(f.digest)),json_object('kind',f.kind,"
    "'project',@f.source_project@,'file',@f.file@,'module',@f.module@,'scope',@f.scope@,"
    "'content_identity',@f.content@,'configuration_identity',@f.configuration@,"
    "'source_origin',f.source_origin,'lookup_domain',@f.lookup_domain@,"
    "'lookup_key',@f.lookup_key@,'declaration_identity',@f.declaration@,"
    "'binding_identity',@f.binding_identity@,'type_identity',@f.type_identity@,"
    "'detail',@f.detail@,'start_byte',f.start_byte,'end_byte',f.end_byte,'origin',f.origin,"
    "'flags',f.flags,'lookup_closed',f.lookup_closed,'binding',f.binding,'domain',f.domain,"
    "'analysis',f.analysis,'rule',f.rule,'facts',f.facts,'target',@f.target@,"
    "'candidate_count',f.candidate_count,'reason',@f.reason@,"
    "'predecessors_closed',f.predecessors_closed) "
    "FROM resolver_projects p JOIN resolver_fact_rows f ON f.pid=p.pid;"
    "CREATE VIEW IF NOT EXISTS resolver_supports(project,fact,ordinal,premise) AS "
    "SELECT p.project,s.fact,s.ordinal,s.premise "
    "FROM resolver_projects p JOIN resolver_support_rows s ON s.pid=p.pid;"
    "CREATE VIEW IF NOT EXISTS resolver_lookups(project,fact,domain,key,status) AS "
    "SELECT p.project,f.id,@f.lookup_domain@,@f.lookup_key@,CASE "
    "WHEN f.analysis<>1 OR NOT f.predecessors_closed OR NOT f.lookup_closed THEN 'incomplete' "
    "WHEN f.binding=2 THEN 'ambiguous' WHEN f.binding=1 THEN 'positive' "
    "ELSE 'closed_negative' END "
    "FROM resolver_projects p JOIN resolver_fact_rows f ON f.pid=p.pid "
    "WHERE f.lookup_domain IS NOT NULL AND f.lookup_key IS NOT NULL;";

/* A view reads 2-3 of a row's text columns far more often than all of them. A
 * scalar lookup costs only the columns a query names, which a JOIN would not. */
static char *schema_sql(void) {
    size_t markers = 0;
    for (const char *at = resolver_schema; *at; at++)
        markers += *at == '@';
    size_t capacity = sizeof(resolver_schema) + markers * 64;
    char *out = cbm_alloc(CBM_MEM_CLASS_STORE, capacity);
    if (!out)
        return NULL;
    char *to = out;
    for (const char *at = resolver_schema; *at;) {
        if (*at != '@') {
            *to++ = *at++;
            continue;
        }
        const char *name = at + 1;
        const char *end = strchr(name, '@');
        const char *dot = strchr(name, '.');
        to += snprintf(to, capacity - (size_t)(to - out),
                       "COALESCE((SELECT value FROM resolver_text t WHERE t.pid=%.*s.pid AND "
                       "t.id=%.*s),'')",
                       (int)(dot - name), name, (int)(end - name), name);
        at = end + 1;
    }
    *to = '\0';
    return out;
}

enum {
    DIGEST_BYTES = 32,
    DICT_CACHE_SLOTS = 1 << 16,
    /* One line per this many stored rows keeps a long write visibly alive: the
     * worker's log is the supervisor's no-progress heartbeat. */
    PROGRESS_ROWS = 1 << 20
};

typedef struct {
    const cbm_resolve_proof_t *node;
    size_t id;
} evidence_id_t;
typedef struct {
    const char *text;
    uint64_t hash;
    sqlite3_int64 id;
} dict_slot_t;
typedef struct {
    const char *pointer;
    sqlite3_int64 id;
} dict_cache_t;
typedef struct {
    sqlite3_int64 file;
    unsigned char digest[DIGEST_BYTES];
} type_digest_t;
typedef struct fact_text {
    char *bytes;
    size_t length, capacity;
    bool plain;
} fact_text_t;
typedef struct {
    sqlite3 *db;
    const char *project;
    sqlite3_int64 pid;
    sqlite3_stmt *text, *fact, *support, *occurrence;
    evidence_id_t *ids;
    size_t capacity, count;
    unsigned char *digests;
    size_t digest_capacity;
    dict_slot_t *dict;
    size_t dict_capacity, dict_count;
    dict_cache_t *cache;
    CBMArena owned;
    type_digest_t *types;
    size_t type_count, type_capacity;
    uint64_t rows;
    const cbm_resolve_file_t *const *files;
    size_t file_count;
    struct fact_text fact_text;
} evidence_write_t;

static const char *text(const char *value) {
    return value ? value : "";
}
static bool bind_text(sqlite3_stmt *s, int col, const char *value) {
    return sqlite3_bind_text(s, col, text(value), -1, SQLITE_STATIC) == SQLITE_OK;
}
static void note_rows(evidence_write_t *w, const char *stage) {
    if (++w->rows % PROGRESS_ROWS)
        return;
    char rows[32];
    (void)snprintf(rows, sizeof(rows), "%llu", (unsigned long long)w->rows);
    cbm_log_info("resolver.evidence.progress", "stage", stage, "rows", rows);
}
static bool finish_row(evidence_write_t *w, sqlite3_stmt *s, bool ok, const char *stage) {
    ok = ok && sqlite3_step(s) == SQLITE_DONE;
    if (ok) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_STORED_RECORDS, 1);
        note_rows(w, stage);
    }
    sqlite3_reset(s);
    return ok;
}

/* ── String dictionary ───────────────────────────────────────────── */

static uint64_t text_hash(const char *value) {
    uint64_t hash = UINT64_C(14695981039346656037);
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) {
        hash ^= *p;
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}
static dict_slot_t *dict_slot(evidence_write_t *w, const char *value, uint64_t hash) {
    size_t at = (size_t)(hash ^ (hash >> 29)) & (w->dict_capacity - 1);
    while (w->dict[at].text && (w->dict[at].hash != hash || strcmp(w->dict[at].text, value)))
        at = (at + 1) & (w->dict_capacity - 1);
    return &w->dict[at];
}
static bool dict_grow(evidence_write_t *w) {
    if (w->dict_capacity && w->dict_count < w->dict_capacity / 2)
        return true;
    size_t next = w->dict_capacity ? w->dict_capacity * 2 : 1024;
    if (next < w->dict_capacity || next > SIZE_MAX / sizeof(*w->dict))
        return false;
    dict_slot_t *slots = cbm_calloc(CBM_MEM_CLASS_STORE, next * sizeof(*slots));
    if (!slots)
        return false;
    dict_slot_t *old = w->dict;
    size_t old_capacity = w->dict_capacity;
    w->dict = slots;
    w->dict_capacity = next;
    for (size_t i = 0; i < old_capacity; i++)
        if (old[i].text)
            *dict_slot(w, old[i].text, old[i].hash) = old[i];
    cbm_free(CBM_MEM_CLASS_STORE, old);
    return true;
}
static size_t dict_cache_at(const char *pointer) {
    uintptr_t value = (uintptr_t)pointer;
    return (size_t)((value >> 3) ^ (value >> 19)) & (DICT_CACHE_SLOTS - 1);
}
/* 0 means the empty string; negative means the write failed. stable says value
 * outlives this write. A temporary is copied, and only a stable pointer may
 * answer from the pointer cache, because that cache trusts the address. */
static sqlite3_int64 dict_id(evidence_write_t *w, const char *value, bool stable) {
    if (!value || !*value)
        return 0;
    dict_cache_t *cached = stable ? &w->cache[dict_cache_at(value)] : NULL;
    if (cached && cached->pointer == value)
        return cached->id;
    uint64_t hash = text_hash(value);
    if (!dict_grow(w))
        return -1;
    dict_slot_t *slot = dict_slot(w, value, hash);
    if (!slot->text) {
        const char *owned = stable ? value : cbm_arena_strdup(&w->owned, value);
        if (!owned)
            return -1;
        sqlite3_int64 id = (sqlite3_int64)++w->dict_count;
        bool ok = sqlite3_bind_int64(w->text, 1, w->pid) == SQLITE_OK &&
                  sqlite3_bind_int64(w->text, 2, id) == SQLITE_OK && bind_text(w->text, 3, owned);
        if (!finish_row(w, w->text, ok, "text"))
            return -1;
        *slot = (dict_slot_t){.text = owned, .hash = hash, .id = id};
    }
    if (cached)
        *cached = (dict_cache_t){.pointer = value, .id = slot->id};
    return slot->id;
}
/* A file with no stored text has no type facts: no insert is needed to know. */
static sqlite3_int64 dict_find(evidence_write_t *w, const char *value) {
    if (!value || !*value)
        return 0;
    if (!w->dict_capacity)
        return -1;
    dict_slot_t *slot = dict_slot(w, value, text_hash(value));
    return slot->text ? slot->id : -1;
}
static bool bind_optional(sqlite3_stmt *s, int col, sqlite3_int64 id) {
    return id >= 0 &&
           (id ? sqlite3_bind_int64(s, col, id) : sqlite3_bind_null(s, col)) == SQLITE_OK;
}
static bool bind_id(evidence_write_t *w, sqlite3_stmt *s, int col, const char *value) {
    return bind_optional(s, col, dict_id(w, value, true));
}

/* ── Proof identity ──────────────────────────────────────────────── */

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
    if (w->count >= w->digest_capacity) {
        size_t next = w->digest_capacity ? w->digest_capacity * 2 : 64;
        if (next < w->digest_capacity || next > SIZE_MAX / DIGEST_BYTES)
            return false;
        unsigned char *digests = cbm_realloc(CBM_MEM_CLASS_STORE, w->digests, next * DIGEST_BYTES);
        if (!digests)
            return false;
        w->digests = digests;
        w->digest_capacity = next;
    }
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
static const unsigned char *id_digest(const evidence_write_t *w, const evidence_id_t *entry) {
    return w->digests + (entry->id - 1) * DIGEST_BYTES;
}
static void digest_hex(const unsigned char *digest, char output[65]) {
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < DIGEST_BYTES; i++) {
        output[i * 2] = hex[digest[i] >> 4];
        output[i * 2 + 1] = hex[digest[i] & 15];
    }
    output[64] = '\0';
}

/* ── Facts ───────────────────────────────────────────────────────── */

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
    char *json = ok ? cbm_json_write_text(doc, cbm_json_writer_allocator(), NULL) : NULL;
    yyjson_mut_doc_free(doc);
    return json;
}
/* A fact's digest covers the JSON above, and building a document per fact was the
 * largest cost of the write. Text with nothing to escape has only one spelling, so
 * those facts, nearly all of them, write the same bytes directly; anything else
 * goes through fact_json, which also handles text that is not UTF-8. */
static void fact_append(fact_text_t *t, const char *bytes, size_t length) {
    if (t->length + length + 1 > t->capacity) {
        size_t next = t->capacity ? t->capacity * 2 : 4096;
        while (next < t->length + length + 1)
            next *= 2;
        char *grown = cbm_realloc(CBM_MEM_CLASS_STORE, t->bytes, next);
        if (!grown) {
            t->plain = false;
            return;
        }
        t->bytes = grown;
        t->capacity = next;
    }
    memcpy(t->bytes + t->length, bytes, length);
    t->length += length;
}
static void fact_str(fact_text_t *t, const char *key, size_t key_length, const char *value) {
    size_t length = 0;
    for (const unsigned char *p = (const unsigned char *)text(value); *p; p++, length++) {
        if (*p < 0x20 || *p > 0x7e || *p == '"' || *p == '\\') {
            t->plain = false;
            return;
        }
    }
    fact_append(t, key, key_length);
    fact_append(t, text(value), length);
    fact_append(t, "\"", 1);
}
static void fact_num(fact_text_t *t, const char *key, size_t key_length, uint64_t value) {
    char digits[24];
    size_t at = sizeof(digits);
    do {
        digits[--at] = (char)('0' + value % 10);
        value /= 10;
    } while (value);
    fact_append(t, key, key_length);
    fact_append(t, digits + at, sizeof(digits) - at);
}
/* True with t->bytes holding exactly what fact_json would have written. */
static bool fact_plain_text(fact_text_t *t, const cbm_resolve_proof_view_t *v) {
    t->length = 0;
    t->plain = true;
#define STR(name, value) fact_str(t, name, sizeof(name) - 1, value)
#define NUM(name, value) fact_num(t, name, sizeof(name) - 1, (uint64_t)(value))
    NUM("{\"kind\":", v->kind);
    STR(",\"project\":\"", v->source.project);
    STR(",\"file\":\"", v->source.file);
    STR(",\"module\":\"", v->source.module);
    STR(",\"scope\":\"", v->source.scope);
    STR(",\"content_identity\":\"", v->source.content_identity);
    STR(",\"configuration_identity\":\"", v->source.configuration_identity);
    NUM(",\"source_origin\":", v->source.source_origin);
    STR(",\"lookup_domain\":\"", v->key.lookup_domain);
    STR(",\"lookup_key\":\"", v->key.lookup_key);
    STR(",\"declaration_identity\":\"", v->key.declaration_identity);
    STR(",\"binding_identity\":\"", v->key.binding_identity);
    STR(",\"type_identity\":\"", v->key.type_identity);
    STR(",\"detail\":\"", v->key.detail);
    NUM(",\"start_byte\":", v->key.start_byte);
    NUM(",\"end_byte\":", v->key.end_byte);
    NUM(",\"origin\":", v->key.origin);
    NUM(",\"flags\":", v->key.flags);
    NUM(",\"lookup_closed\":", v->key.lookup_closed);
    NUM(",\"binding\":", v->outcome.binding);
    NUM(",\"domain\":", v->outcome.domain);
    NUM(",\"analysis\":", v->outcome.analysis);
    NUM(",\"rule\":", v->outcome.rule);
    NUM(",\"facts\":", v->outcome.facts);
    STR(",\"target\":\"", v->outcome.target);
    NUM(",\"candidate_count\":", v->outcome.candidate_count);
    STR(",\"reason\":\"", v->outcome.reason);
    NUM(",\"predecessors_closed\":", v->predecessors_closed);
#undef STR
#undef NUM
    fact_append(t, "}", 2);
    return t->plain;
}
static bool note_type_digest(evidence_write_t *w, const cbm_resolve_proof_view_t *v,
                             const unsigned char *digest) {
    if (v->kind != CBM_PROOF_TYPE_BIND && v->kind != CBM_PROOF_TYPE_COPY &&
        v->kind != CBM_PROOF_TYPE_JOIN)
        return true;
    if (w->type_count == w->type_capacity) {
        size_t next = w->type_capacity ? w->type_capacity * 2 : 64;
        if (next > SIZE_MAX / sizeof(*w->types))
            return false;
        type_digest_t *types = cbm_realloc(CBM_MEM_CLASS_STORE, w->types, next * sizeof(*types));
        if (!types)
            return false;
        w->types = types;
        w->type_capacity = next;
    }
    sqlite3_int64 file = dict_id(w, v->source.file, true);
    if (file < 0)
        return false;
    type_digest_t *row = &w->types[w->type_count++];
    row->file = file;
    memcpy(row->digest, digest, DIGEST_BYTES);
    return true;
}
/* resolver_facts builds its payload from these strings, and a payload must stay
 * strict JSON, as fact_json's was. Source text that is not UTF-8 is stored as
 * fact_json would have written it. A plain fact has already been checked. */
static bool bind_fact_text(evidence_write_t *w, sqlite3_stmt *s, int col, const char *value) {
    if (w->fact_text.plain || !value || cbm_utf8_valid(value, strlen(value)))
        return bind_id(w, s, col, value);
    size_t length = 0;
    char *clean = cbm_utf8_lossy_dup(CBM_MEM_CLASS_STORE, value, strlen(value), &length);
    if (!clean)
        return false;
    bool ok = bind_optional(s, col, dict_id(w, clean, false));
    cbm_free(CBM_MEM_CLASS_STORE, clean);
    return ok;
}
static bool bind_fact(evidence_write_t *w, const cbm_resolve_proof_view_t *v, size_t id,
                      const unsigned char *digest) {
    sqlite3_stmt *s = w->fact;
    return sqlite3_bind_int64(s, 1, w->pid) == SQLITE_OK &&
           sqlite3_bind_int64(s, 2, (sqlite3_int64)id) == SQLITE_OK &&
           sqlite3_bind_int(s, 3, (int)v->kind) == SQLITE_OK &&
           sqlite3_bind_blob(s, 4, digest, DIGEST_BYTES, SQLITE_STATIC) == SQLITE_OK &&
           bind_fact_text(w, s, 5, v->source.project) && bind_fact_text(w, s, 6, v->source.file) &&
           bind_fact_text(w, s, 7, v->source.module) && bind_fact_text(w, s, 8, v->source.scope) &&
           bind_fact_text(w, s, 9, v->source.content_identity) &&
           bind_fact_text(w, s, 10, v->source.configuration_identity) &&
           sqlite3_bind_int64(s, 11, v->source.source_origin) == SQLITE_OK &&
           bind_fact_text(w, s, 12, v->key.lookup_domain) &&
           bind_fact_text(w, s, 13, v->key.lookup_key) &&
           bind_fact_text(w, s, 14, v->key.declaration_identity) &&
           bind_fact_text(w, s, 15, v->key.binding_identity) &&
           bind_fact_text(w, s, 16, v->key.type_identity) &&
           bind_fact_text(w, s, 17, v->key.detail) &&
           sqlite3_bind_int64(s, 18, v->key.start_byte) == SQLITE_OK &&
           sqlite3_bind_int64(s, 19, v->key.end_byte) == SQLITE_OK &&
           sqlite3_bind_int64(s, 20, v->key.origin) == SQLITE_OK &&
           sqlite3_bind_int64(s, 21, v->key.flags) == SQLITE_OK &&
           sqlite3_bind_int(s, 22, v->key.lookup_closed ? 1 : 0) == SQLITE_OK &&
           sqlite3_bind_int(s, 23, (int)v->outcome.binding) == SQLITE_OK &&
           sqlite3_bind_int(s, 24, (int)v->outcome.domain) == SQLITE_OK &&
           sqlite3_bind_int(s, 25, (int)v->outcome.analysis) == SQLITE_OK &&
           sqlite3_bind_int(s, 26, (int)v->outcome.rule) == SQLITE_OK &&
           sqlite3_bind_int64(s, 27, v->outcome.facts) == SQLITE_OK &&
           bind_fact_text(w, s, 28, v->outcome.target) &&
           sqlite3_bind_int64(s, 29, v->outcome.candidate_count) == SQLITE_OK &&
           bind_fact_text(w, s, 30, v->outcome.reason) &&
           sqlite3_bind_int(s, 31, v->predecessors_closed ? 1 : 0) == SQLITE_OK;
}
static bool write_fact(evidence_write_t *w, const cbm_resolve_proof_t *node) {
    const cbm_resolve_proof_view_t *v = cbm_resolve_proof_view(node);
    if (!v || !source_matches(w, &v->source) || !grow_ids(w))
        return false;
    bool plain = fact_plain_text(&w->fact_text, v);
    char *json = plain ? NULL : fact_json(v);
    if (!plain && !json)
        return false;
    evidence_id_t entry = {.node = node, .id = w->count + 1};
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    cbm_resolve_hash_text(&hash, plain ? w->fact_text.bytes : json);
    bool ok = true;
    for (size_t i = 0; ok && i < v->premise_count; i++) {
        evidence_id_t *premise = find_id(w, v->premises[i]);
        ok = premise != NULL;
        if (ok) {
            char hex[65];
            digest_hex(id_digest(w, premise), hex);
            cbm_resolve_hash_text(&hash, hex);
            ok = finish_row(
                w, w->support,
                sqlite3_bind_int64(w->support, 1, w->pid) == SQLITE_OK &&
                    sqlite3_bind_int64(w->support, 2, (sqlite3_int64)entry.id) == SQLITE_OK &&
                    sqlite3_bind_int64(w->support, 3, (sqlite3_int64)i) == SQLITE_OK &&
                    sqlite3_bind_int64(w->support, 4, (sqlite3_int64)premise->id) == SQLITE_OK,
                "supports");
        }
    }
    unsigned char *digest = w->digests + w->count * DIGEST_BYTES;
    cbm_sha256_final(&hash, digest);
    cbm_free(CBM_MEM_CLASS_OTHER, json);
    ok = ok && note_type_digest(w, v, digest) &&
         finish_row(w, w->fact, bind_fact(w, v, entry.id, digest), "facts");
    if (ok) {
        *id_slot(w, node) = entry;
        w->count++;
    }
    return ok;
}

/* Each distinct fact after its premises, so a fact id always follows the ids it
 * cites. ids doubles as the visited set: a second hash table over the same nodes
 * would cost a gigabyte on the largest repositories. Recursion is bounded because
 * a node's depth is checked when it is made, and a cycle cannot be built from
 * immutable nodes. */
static bool write_facts_from(evidence_write_t *w, const cbm_resolve_proof_t *node) {
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_PROPAGATED_FACTS, 1);
    if (find_id(w, node))
        return true;
    const cbm_resolve_proof_view_t *v = cbm_resolve_proof_view(node);
    if (!v || (v->premise_count && !v->premises))
        return false;
    for (size_t i = 0; i < v->premise_count; i++)
        if (!write_facts_from(w, v->premises[i]))
            return false;
    return write_fact(w, node);
}
static int compare_roots(const void *left, const void *right) {
    return cbm_resolve_proof_compare(*(const cbm_resolve_proof_t *const *)left,
                                     *(const cbm_resolve_proof_t *const *)right);
}
/* Roots are sorted structurally, so fact ids do not depend on occurrence order. */
static bool write_facts(evidence_write_t *w, const cbm_resolve_proof_t **roots, size_t count) {
    if (count > 1)
        qsort(roots, count, sizeof(*roots), compare_roots);
    for (size_t i = 0; i < count; i++)
        if (!write_facts_from(w, roots[i]))
            return false;
    return true;
}

/* ── Generation reset ────────────────────────────────────────────── */

static bool project_id(sqlite3 *db, const char *project, sqlite3_int64 *pid) {
    sqlite3_stmt *s = NULL;
    bool ok = sqlite3_prepare_v2(db,
                                 "INSERT INTO resolver_projects(project,rules_version) "
                                 "VALUES(?1,?2) ON CONFLICT(project) DO UPDATE SET "
                                 "rules_version=?2",
                                 -1, &s, NULL) == SQLITE_OK &&
              bind_text(s, 1, project) &&
              sqlite3_bind_int(s, 2, CBM_RESOLVER_RULES_VERSION) == SQLITE_OK &&
              sqlite3_step(s) == SQLITE_DONE;
    sqlite3_finalize(s);
    s = NULL;
    ok = ok &&
         sqlite3_prepare_v2(db, "SELECT pid FROM resolver_projects WHERE project=?", -1, &s,
                            NULL) == SQLITE_OK &&
         bind_text(s, 1, project) && sqlite3_step(s) == SQLITE_ROW;
    if (ok)
        *pid = sqlite3_column_int64(s, 0);
    sqlite3_finalize(s);
    return ok;
}
static bool reset_tables(sqlite3 *db, const char *project, sqlite3_int64 *pid) {
    char *schema = schema_sql();
    bool created = schema && sqlite3_exec(db, schema, NULL, NULL, NULL) == SQLITE_OK;
    cbm_free(CBM_MEM_CLASS_STORE, schema);
    if (!created || !project_id(db, project, pid))
        return false;
    const char *by_pid[] = {"DELETE FROM resolver_text WHERE pid=?",
                            "DELETE FROM resolver_occurrence_rows WHERE pid=?",
                            "DELETE FROM resolver_fact_rows WHERE pid=?",
                            "DELETE FROM resolver_support_rows WHERE pid=?"};
    for (size_t i = 0; i < sizeof(by_pid) / sizeof(by_pid[0]); i++) {
        sqlite3_stmt *s = NULL;
        bool ok = sqlite3_prepare_v2(db, by_pid[i], -1, &s, NULL) == SQLITE_OK &&
                  sqlite3_bind_int64(s, 1, *pid) == SQLITE_OK && sqlite3_step(s) == SQLITE_DONE;
        sqlite3_finalize(s);
        if (!ok)
            return false;
    }
    const char *by_name[] = {"DELETE FROM resolver_file_summaries WHERE project=?",
                             "DELETE FROM resolver_derived_bindings WHERE project=?"};
    for (size_t i = 0; i < sizeof(by_name) / sizeof(by_name[0]); i++) {
        sqlite3_stmt *s = NULL;
        bool ok = sqlite3_prepare_v2(db, by_name[i], -1, &s, NULL) == SQLITE_OK &&
                  bind_text(s, 1, project) && sqlite3_step(s) == SQLITE_DONE;
        sqlite3_finalize(s);
        if (!ok)
            return false;
    }
    return true;
}

/* ── Occurrences ─────────────────────────────────────────────────── */

/* A row that produced no edge already arrives split; any other is split here. */
static sqlite3_int64 properties_id(evidence_write_t *w, const cbm_resolve_record_t *row,
                                   int32_t *line) {
    if (row->properties_tail) {
        *line = row->properties_line;
        return dict_id(w, row->properties_tail, true);
    }
    char *tail = cbm_resolve_properties_tail(row, line);
    if (!tail)
        return -1;
    sqlite3_int64 id = dict_id(w, tail, false);
    cbm_free(CBM_MEM_CLASS_RESOLVE, tail);
    return id;
}
static bool bind_occurrence(evidence_write_t *w, const cbm_resolve_record_t *row, size_t ordinal,
                            const evidence_id_t *proof, const evidence_id_t *binding) {
    sqlite3_stmt *s = w->occurrence;
    const cbm_resolve_result_t *r = &row->result;
    int32_t line = -1;
    return sqlite3_bind_int64(s, 1, w->pid) == SQLITE_OK &&
           sqlite3_bind_int64(s, 2, (sqlite3_int64)ordinal) == SQLITE_OK &&
           bind_id(w, s, 3, row->file_path) && bind_id(w, s, 4, row->scope) &&
           bind_id(w, s, 5, row->relation) && bind_id(w, s, 6, row->spelling) &&
           sqlite3_bind_int64(s, 7, row->start_byte) == SQLITE_OK &&
           sqlite3_bind_int64(s, 8, (sqlite3_int64)row->end_byte - row->start_byte) == SQLITE_OK &&
           sqlite3_bind_int(s, 9, (int)row->source_origin) == SQLITE_OK &&
           sqlite3_bind_int(s, 10, (int)r->binding) == SQLITE_OK &&
           sqlite3_bind_int(s, 11, (int)r->domain) == SQLITE_OK &&
           sqlite3_bind_int(s, 12, (int)r->analysis) == SQLITE_OK &&
           sqlite3_bind_int(s, 13, (int)r->rule) == SQLITE_OK &&
           sqlite3_bind_int64(s, 14, r->facts) == SQLITE_OK && bind_id(w, s, 15, r->target) &&
           sqlite3_bind_int64(s, 16, r->candidate_count) == SQLITE_OK &&
           bind_id(w, s, 17, r->reason) &&
           sqlite3_bind_int(s, 18, row->emitted ? 1 : 0) == SQLITE_OK &&
           bind_optional(s, 19, properties_id(w, row, &line)) &&
           sqlite3_bind_int64(s, 20, line) == SQLITE_OK &&
           bind_id(w, s, 21, row->content_identity) &&
           bind_id(w, s, 22, row->configuration_identity) &&
           sqlite3_bind_int64(s, 23, proof ? (sqlite3_int64)proof->id : 0) == SQLITE_OK &&
           sqlite3_bind_int64(s, 24, binding ? (sqlite3_int64)binding->id : 0) == SQLITE_OK;
}
static bool write_occurrences(evidence_write_t *w, const cbm_resolve_record_t *rows, size_t count) {
    for (size_t i = 0; i < count; i++) {
        evidence_id_t *proof = find_id(w, rows[i].result.proof);
        evidence_id_t *binding = find_id(w, rows[i].binding_proof);
        cbm_resolve_proof_source_t source = {.project = w->project,
                                             .file = rows[i].file_path,
                                             .content_identity = rows[i].content_identity,
                                             .configuration_identity =
                                                 rows[i].configuration_identity,
                                             .source_origin = rows[i].source_origin};
        /* A claimed emitted row must satisfy the same admission as its edge.
         * A failed metadata write aborts publication, never fabricates zero. */
        if (rows[i].proof_borrowed ||
            (rows[i].emitted &&
             (!cbm_resolve_record_admit(&rows[i]) || !proof || !source_matches(w, &source))) ||
            !finish_row(w, w->occurrence, bind_occurrence(w, &rows[i], i, proof, binding),
                        "occurrences"))
            return false;
    }
    return true;
}

/* ── Summaries ───────────────────────────────────────────────────── */

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
                  w->db, "INSERT INTO resolver_derived_bindings VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                  -1, &s, NULL) == SQLITE_OK;
    for (size_t i = 0; ok && i < count; i++) {
        const cbm_resolve_summary_t *row = rows[i];
        evidence_id_t *proof = find_id(w, row->result.proof);
        ok = !row->proof_borrowed &&
             finish_row(w, s,
                        bind_text(s, 1, w->project) && bind_text(s, 2, row->file_path) &&
                            bind_text(s, 3, row->key) &&
                            sqlite3_bind_int64(s, 4, row->kind) == SQLITE_OK &&
                            sqlite3_bind_int(s, 5, row->module) == SQLITE_OK &&
                            sqlite3_bind_int(s, 6, row->closed) == SQLITE_OK &&
                            sqlite3_bind_int(s, 7, row->result.binding) == SQLITE_OK &&
                            sqlite3_bind_int(s, 8, row->result.domain) == SQLITE_OK &&
                            sqlite3_bind_int(s, 9, row->result.analysis) == SQLITE_OK &&
                            bind_text(s, 10, row->result.target) &&
                            sqlite3_bind_int64(s, 11, proof ? (sqlite3_int64)proof->id : 0) ==
                                SQLITE_OK &&
                            bind_text(s, 12, row->payload_json),
                        "derived_bindings");
    }
    sqlite3_finalize(s);
    return ok;
}
static int compare_type_digest(const void *a, const void *b) {
    const type_digest_t *left = a, *right = b;
    if (left->file != right->file)
        return left->file < right->file ? -1 : 1;
    return memcmp(left->digest, right->digest, DIGEST_BYTES);
}
/* Observed type states are derived facts too, including unknown/limited ones,
 * not just successful edges. They were noted while the facts were written, so a
 * file's states are a sorted slice: no per-file query, no index to maintain. */
static void hash_type_states(evidence_write_t *w, cbm_sha256_ctx *hash, const char *file) {
    sqlite3_int64 id = dict_find(w, file);
    if (id < 0)
        return;
    size_t lo = 0, hi = w->type_count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        if (w->types[mid].file < id)
            lo = mid + 1;
        else
            hi = mid;
    }
    for (; lo < w->type_count && w->types[lo].file == id; lo++) {
        char hex[65];
        digest_hex(w->types[lo].digest, hex);
        cbm_resolve_hash_text(hash, hex);
    }
}
static bool write_files(evidence_write_t *w, const cbm_resolve_summary_t *const *rows,
                        size_t count) {
    sqlite3_stmt *s = NULL;
    bool ok = sqlite3_prepare_v2(
                  w->db, "INSERT INTO resolver_file_summaries VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", -1,
                  &s, NULL) == SQLITE_OK;
    if (w->type_count > 1)
        qsort(w->types, w->type_count, sizeof(*w->types), compare_type_digest);
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
            char hex[65] = "";
            if (proof)
                digest_hex(id_digest(w, proof), hex);
            cbm_resolve_hash_text(&hash, row->key);
            cbm_resolve_hash_number(&hash, row->kind);
            cbm_resolve_hash_number(&hash, row->module);
            cbm_resolve_hash_number(&hash, row->closed);
            cbm_resolve_hash_number(&hash, row->result.binding);
            cbm_resolve_hash_number(&hash, row->result.domain);
            cbm_resolve_hash_number(&hash, row->result.analysis);
            cbm_resolve_hash_text(&hash, row->result.target);
            cbm_resolve_hash_text(&hash, row->payload_json);
            cbm_resolve_hash_text(&hash, hex);
        }
        hash_type_states(w, &hash, file->file_path);
        char derived[65];
        cbm_resolve_hash_finish(&hash, derived);
        ok = finish_row(
            w, s,
            bind_text(s, 1, w->project) && bind_text(s, 2, file->file_path) &&
                bind_text(s, 3, file->module) &&
                sqlite3_bind_int64(s, 4, file->language) == SQLITE_OK &&
                bind_text(s, 5, file->raw_content_identity) &&
                bind_text(s, 6, file->preprocessed_content_identity) &&
                bind_text(s, 7, file->configuration_identity) &&
                sqlite3_bind_int64(s, 8, (sqlite3_int64)file->raw_length) == SQLITE_OK &&
                sqlite3_bind_int64(s, 9, (sqlite3_int64)file->preprocessed_length) == SQLITE_OK &&
                bind_text(s, 10, file->extracted_summary) && bind_text(s, 11, derived) &&
                sqlite3_bind_int(s, 12, file->complete) == SQLITE_OK,
            "file_summaries");
    }
    sqlite3_finalize(s);
    return ok;
}
static bool prepare_writer(evidence_write_t *w) {
    return sqlite3_prepare_v2(w->db, "INSERT INTO resolver_text VALUES(?,?,?)", -1, &w->text,
                              NULL) == SQLITE_OK &&
           sqlite3_prepare_v2(w->db,
                              "INSERT INTO resolver_fact_rows VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,"
                              "?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                              -1, &w->fact, NULL) == SQLITE_OK &&
           sqlite3_prepare_v2(w->db, "INSERT INTO resolver_support_rows VALUES(?,?,?,?)", -1,
                              &w->support, NULL) == SQLITE_OK &&
           sqlite3_prepare_v2(w->db,
                              "INSERT INTO resolver_occurrence_rows VALUES(?,?,?,?,?,?,?,?,?,?,?,"
                              "?,?,?,?,?,?,?,?,?,?,?,?,?)",
                              -1, &w->occurrence, NULL) == SQLITE_OK;
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
    dict_cache_t *cache = cbm_calloc(CBM_MEM_CLASS_STORE, DICT_CACHE_SLOTS * sizeof(*cache));
    bool ok = (!root_capacity || roots) && (!file_count || ordered_files) &&
              (!summary_count || ordered_summaries) && cache;
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
    evidence_write_t w = {.db = db,
                          .project = project,
                          .files = ordered_files,
                          .file_count = file_count,
                          .cache = cache};
    cbm_arena_init(&w.owned);
    ok = ok && reset_tables(db, project, &w.pid) && prepare_writer(&w) &&
         write_facts(&w, roots, root_count);
    cbm_free(CBM_MEM_CLASS_STORE, roots);
    ok = ok && write_occurrences(&w, rows, count) &&
         write_summaries(&w, ordered_summaries, summary_count) &&
         write_files(&w, ordered_summaries, summary_count);
    sqlite3_finalize(w.text);
    sqlite3_finalize(w.fact);
    sqlite3_finalize(w.support);
    sqlite3_finalize(w.occurrence);
    cbm_free(CBM_MEM_CLASS_STORE, w.ids);
    cbm_free(CBM_MEM_CLASS_STORE, w.digests);
    cbm_free(CBM_MEM_CLASS_STORE, w.dict);
    cbm_free(CBM_MEM_CLASS_STORE, w.types);
    cbm_free(CBM_MEM_CLASS_STORE, w.fact_text.bytes);
    cbm_free(CBM_MEM_CLASS_STORE, cache);
    cbm_arena_destroy(&w.owned);
    cbm_free(CBM_MEM_CLASS_STORE, ordered_files);
    cbm_free(CBM_MEM_CLASS_STORE, ordered_summaries);
    return ok;
}
