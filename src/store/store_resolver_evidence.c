#include "resolve/stats.h"
#include "store_resolver_evidence.h"
#include "arena.h"
#include "foundation/compat.h"
#include "foundation/json_writer.h"
#include "foundation/log.h"
#include "foundation/mem_core.h"
#include "foundation/str_util.h"
#include "resolve/policy.h"
#include "resolve/proof.h"
#include "resolve/snapshot.h"
#include "pipeline/worker_pool.h"
#include "sqlite_writer.h"
#include <yyjson/yyjson.h>
#include <stdatomic.h>
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

bool cbm_store_resolver_view_definition_current(const char *sql) {
    /* SQLite stores "CREATE VIEW name..." without the IF NOT EXISTS it was given. */
    static const char prefix[] = "CREATE VIEW ";
    if (!sql || strncmp(sql, prefix, sizeof(prefix) - 1))
        return false;
    char *schema = schema_sql();
    if (!schema)
        return false;
    /* The definition runs to the statement's ';', which sqlite_schema drops. */
    size_t length = strlen(sql);
    const char *body = sql + sizeof(prefix) - 1;
    size_t body_length = length - (sizeof(prefix) - 1);
    bool found = false;
    static const char opening[] = "CREATE VIEW IF NOT EXISTS ";
    for (const char *at = strstr(schema, opening); at && !found; at = strstr(at + 1, opening)) {
        const char *definition = at + sizeof(opening) - 1;
        found = !strncmp(definition, body, body_length) && definition[body_length] == ';';
    }
    cbm_free(CBM_MEM_CLASS_STORE, schema);
    return found;
}

enum {
    DIGEST_BYTES = 32,
    DICT_CACHE_SLOTS = 1 << 16,
    VERIFIED_SOURCE_SLOTS = 1 << 12,
    /* One line per this many stored rows keeps a long write visibly alive: the
     * worker's log is the supervisor's no-progress heartbeat. */
    PROGRESS_ROWS = 1 << 20,
    /* Composed facts are digested this many at a time, in order, and forgotten. */
    FACT_BATCH = 1 << 16,
    /* Fewer jobs than this are not worth starting threads for. */
    FACT_INLINE_JOBS = 256,
    ROOT_CHUNK_ROWS = 1 << 12,
    FACT_TEXT_MEMO_SLOTS = 1 << 12,
    /* A new file's first project: see cbm_resolver_stage. */
    DIRECT_PID = 1
};

typedef struct {
    const cbm_resolve_proof_t *node;
    uint32_t id;
    uint32_t digest; /* Index into digests. */
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
typedef struct {
    const char *content, *file, *project, *configuration;
    uint32_t origin;
} verified_source_t;
/* A string's length and whether it needs escaping, by address: every string a fact
 * holds outlives the write, and a project repeats the same few in every fact. */
typedef struct {
    const char *pointer;
    size_t length;
    bool plain;
} fact_text_memo_t;
typedef struct fact_text {
    char *bytes;
    size_t length, capacity;
    bool plain;
    fact_text_memo_t *memo;
} fact_text_t;
/* What is left of a write once its rows are in pages: everything the two remaining
 * tables need, so the fact table and its digests can go before the graph is written. */
struct cbm_resolver_stage {
    char *table_sql[4];
    const cbm_resolve_summary_t **summaries;
    uint32_t *summary_proofs;
    size_t summary_count;
    const cbm_resolve_file_t **files;
    char (*derived)[65];
    size_t file_count;
};
typedef struct {
    sqlite3 *db;
    const char *project;
    sqlite3_int64 pid;
    sqlite3_stmt *text, *fact, *support, *occurrence;
    /* Set instead of the statements when rows go straight into a new file's pages. */
    cbm_writer_table_t *text_table, *fact_table, *support_table, *occurrence_table;
    evidence_id_t *ids;
    size_t capacity, id_count;
    size_t count;     /* Facts planned, which is also the last fact id. */
    uintptr_t *order; /* Fact id - 1 -> its node, or its row with the low bit set. */
    size_t order_capacity;
    const char **valid_text;
    int workers;
    unsigned char *digests;
    size_t digest_capacity, digest_count;
    dict_slot_t *dict;
    size_t dict_capacity, dict_count;
    dict_cache_t *cache;
    uint32_t *row_facts;
    verified_source_t verified[VERIFIED_SOURCE_SLOTS];
    CBMArena owned;
    type_digest_t *types;
    size_t type_count, type_capacity;
    uint64_t rows;
    bool commit_batches;
    const cbm_resolve_file_t *const *files;
    size_t file_count;
} evidence_write_t;

static const char *text(const char *value) {
    return value ? value : "";
}
static bool bind_text(sqlite3_stmt *s, int col, const char *value) {
    return sqlite3_bind_text(s, col, text(value), -1, SQLITE_STATIC) == SQLITE_OK;
}
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static uint64_t progress_rows = PROGRESS_ROWS;
static size_t held_digests;
void cbm_store_resolver_test_set_progress_rows(uint64_t rows) {
    progress_rows = rows ? rows : PROGRESS_ROWS;
}
size_t cbm_store_resolver_test_held_digests(void) {
    return held_digests;
}
#else
enum { progress_rows = PROGRESS_ROWS };
#endif
/* Every statement has been reset when this runs, so the commit can proceed. */
static bool note_rows(evidence_write_t *w, const char *stage) {
    if (++w->rows % progress_rows)
        return true;
    char rows[32];
    (void)snprintf(rows, sizeof(rows), "%llu", (unsigned long long)w->rows);
    cbm_log_info("resolver.evidence.progress", "stage", stage, "rows", rows);
    return !w->commit_batches ||
           sqlite3_exec(w->db, "RELEASE resolver_replace;SAVEPOINT resolver_replace", NULL, NULL,
                        NULL) == SQLITE_OK;
}
static bool finish_row(evidence_write_t *w, sqlite3_stmt *s, bool ok, const char *stage) {
    ok = ok && sqlite3_step(s) == SQLITE_DONE;
    sqlite3_reset(s);
    if (ok) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_STORED_RECORDS, 1);
        ok = note_rows(w, stage);
    }
    return ok;
}

static bool table_finish(evidence_write_t *w, cbm_writer_table_t *table, bool ok,
                         const char *stage) {
    ok = ok && cbm_writer_row_end(table);
    if (ok) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_STORED_RECORDS, 1);
        ok = note_rows(w, stage);
    }
    return ok;
}
/* A NULL text id means the empty string, as bind_optional writes it. */
static bool table_text_id(cbm_writer_table_t *table, sqlite3_int64 id) {
    if (id < 0)
        return false;
    if (id)
        cbm_writer_row_int(table, id);
    else
        cbm_writer_row_null(table);
    return true;
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
        if (w->text_table) {
            cbm_writer_row_begin(w->text_table);
            cbm_writer_row_int(w->text_table, w->pid);
            cbm_writer_row_int(w->text_table, id);
            cbm_writer_row_text(w->text_table, owned);
            if (!table_finish(w, w->text_table, true, "text"))
                return -1;
        } else {
            bool ok = sqlite3_bind_int64(w->text, 1, w->pid) == SQLITE_OK &&
                      sqlite3_bind_int64(w->text, 2, id) == SQLITE_OK &&
                      bind_text(w->text, 3, owned);
            if (!finish_row(w, w->text, ok, "text"))
                return -1;
        }
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
static evidence_id_t *id_slot(const evidence_write_t *w, const cbm_resolve_proof_t *node) {
    size_t at = pointer_hash(node) & (w->capacity - 1);
    while (w->ids[at].node && w->ids[at].node != node)
        at = (at + 1) & (w->capacity - 1);
    return &w->ids[at];
}
static evidence_id_t *find_id(const evidence_write_t *w, const cbm_resolve_proof_t *node) {
    if (!node || !w->capacity)
        return NULL;
    evidence_id_t *entry = id_slot(w, node);
    return entry->node ? entry : NULL;
}
/* Room for one more node. Only a node needs a slot and a digest: other facts cite a
 * fact through its node, and a composed fact is cited only by its own row. About half
 * the facts of a large repository are composed. */
static bool grow_ids(evidence_write_t *w) {
    if (w->digest_count >= UINT32_MAX)
        return false;
    if (w->digest_count >= w->digest_capacity) {
        size_t next = w->digest_capacity ? w->digest_capacity * 2 : 64;
        if (next < w->digest_capacity || next > SIZE_MAX / DIGEST_BYTES)
            return false;
        unsigned char *digests = cbm_realloc(CBM_MEM_CLASS_STORE, w->digests, next * DIGEST_BYTES);
        if (!digests)
            return false;
        w->digests = digests;
        w->digest_capacity = next;
    }
    if (w->capacity && w->id_count < w->capacity / 2)
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
    return w->digests + (size_t)entry->digest * DIGEST_BYTES;
}
/* Call grow_ids first. The digest slot is filled once every node has its id. */
static void hold_id(evidence_write_t *w, const cbm_resolve_proof_t *node, uint32_t id) {
    *id_slot(w, node) =
        (evidence_id_t){.node = node, .id = id, .digest = (uint32_t)w->digest_count};
    w->digest_count++;
    w->id_count++;
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
static bool source_matches_snapshot(const evidence_write_t *w,
                                    const cbm_resolve_proof_source_t *s) {
    const cbm_resolve_file_t *file = find_file(w, s->file);
    if (!file || s->source_origin > 1 ||
        (s->project && *s->project && strcmp(s->project, w->project)))
        return false;
    const char *content =
        s->source_origin ? file->preprocessed_content_identity : file->raw_content_identity;
    return !strcmp(text(content), s->content_identity) &&
           !strcmp(text(file->configuration_identity), text(s->configuration_identity));
}
/* Missing context remains explicitly unknown. Whenever a captured source identity
 * is claimed, it must identify this generation's exact snapshot. A pool stores each
 * string once, so facts of one file repeat the same pointers, and a source already
 * verified needs no binary search and no comparison of 64-character identities. */
static bool source_matches(evidence_write_t *w, const cbm_resolve_proof_source_t *s) {
    if (!s->content_identity || !*s->content_identity)
        return true;
    verified_source_t *seen =
        &w->verified[dict_cache_at(s->content_identity) & (VERIFIED_SOURCE_SLOTS - 1)];
    if (seen->content == s->content_identity && seen->file == s->file &&
        seen->project == s->project && seen->configuration == s->configuration_identity &&
        seen->origin == s->source_origin)
        return true;
    if (!source_matches_snapshot(w, s))
        return false;
    *seen = (verified_source_t){.content = s->content_identity,
                                .file = s->file,
                                .project = s->project,
                                .configuration = s->configuration_identity,
                                .origin = s->source_origin};
    return true;
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
static fact_text_memo_t *fact_memo(fact_text_t *t, const char *value) {
    uintptr_t address = (uintptr_t)value;
    fact_text_memo_t *slot = &t->memo[(address ^ (address >> 13)) & (FACT_TEXT_MEMO_SLOTS - 1)];
    if (slot->pointer != value) {
        size_t length = 0;
        bool plain = true;
        for (const unsigned char *p = (const unsigned char *)value; *p; p++, length++)
            plain = plain && *p >= 0x20 && *p <= 0x7e && *p != '"' && *p != '\\';
        *slot = (fact_text_memo_t){.pointer = value, .length = length, .plain = plain};
    }
    return slot;
}
static void fact_str(fact_text_t *t, const char *key, size_t key_length, const char *value) {
    const fact_text_memo_t *known = fact_memo(t, text(value));
    if (!known->plain) {
        t->plain = false;
        return;
    }
    fact_append(t, key, key_length);
    fact_append(t, known->pointer, known->length);
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
 * fact_json would have written it. Each address needs checking once. */
static sqlite3_int64 fact_text_id(evidence_write_t *w, const char *value) {
    if (!value || !*value)
        return dict_id(w, value, true);
    const char **checked = &w->valid_text[dict_cache_at(value)];
    if (*checked == value)
        return dict_id(w, value, true);
    if (cbm_utf8_valid(value, strlen(value))) {
        *checked = value;
        return dict_id(w, value, true);
    }
    size_t length = 0;
    char *clean = cbm_utf8_lossy_dup(CBM_MEM_CLASS_STORE, value, strlen(value), &length);
    if (!clean)
        return -1;
    sqlite3_int64 id = dict_id(w, clean, false);
    cbm_free(CBM_MEM_CLASS_STORE, clean);
    return id;
}
static bool bind_fact_text(evidence_write_t *w, sqlite3_stmt *s, int col, const char *value) {
    return bind_optional(s, col, fact_text_id(w, value));
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
static bool table_fact_text(evidence_write_t *w, const char *value) {
    return table_text_id(w->fact_table, fact_text_id(w, value));
}
/* The same columns as bind_fact, for a row going straight into a page. */
static bool table_fact(evidence_write_t *w, const cbm_resolve_proof_view_t *v, size_t id,
                       const unsigned char *digest) {
    cbm_writer_table_t *t = w->fact_table;
    cbm_writer_row_begin(t);
    cbm_writer_row_int(t, w->pid);
    cbm_writer_row_int(t, (sqlite3_int64)id);
    cbm_writer_row_int(t, (int)v->kind);
    cbm_writer_row_blob(t, digest, DIGEST_BYTES);
    bool ok = table_fact_text(w, v->source.project) && table_fact_text(w, v->source.file) &&
              table_fact_text(w, v->source.module) && table_fact_text(w, v->source.scope) &&
              table_fact_text(w, v->source.content_identity) &&
              table_fact_text(w, v->source.configuration_identity);
    cbm_writer_row_int(t, v->source.source_origin);
    ok = ok && table_fact_text(w, v->key.lookup_domain) && table_fact_text(w, v->key.lookup_key) &&
         table_fact_text(w, v->key.declaration_identity) &&
         table_fact_text(w, v->key.binding_identity) && table_fact_text(w, v->key.type_identity) &&
         table_fact_text(w, v->key.detail);
    cbm_writer_row_int(t, v->key.start_byte);
    cbm_writer_row_int(t, v->key.end_byte);
    cbm_writer_row_int(t, v->key.origin);
    cbm_writer_row_int(t, v->key.flags);
    cbm_writer_row_int(t, v->key.lookup_closed ? 1 : 0);
    cbm_writer_row_int(t, (int)v->outcome.binding);
    cbm_writer_row_int(t, (int)v->outcome.domain);
    cbm_writer_row_int(t, (int)v->outcome.analysis);
    cbm_writer_row_int(t, (int)v->outcome.rule);
    cbm_writer_row_int(t, v->outcome.facts);
    ok = ok && table_fact_text(w, v->outcome.target);
    cbm_writer_row_int(t, (sqlite3_int64)v->outcome.candidate_count);
    ok = ok && table_fact_text(w, v->outcome.reason);
    cbm_writer_row_int(t, v->predecessors_closed ? 1 : 0);
    return table_finish(w, t, ok, "facts");
}

/* What a fact is digested from: its text, then each premise's digest. Reads only what
 * planning has fixed, so any number of these run at once. */
static bool fact_digest(const evidence_write_t *w, const cbm_resolve_proof_view_t *v,
                        fact_text_t *fact_text, unsigned char digest[DIGEST_BYTES]) {
    bool plain = fact_plain_text(fact_text, v);
    char *json = plain ? NULL : fact_json(v);
    if (!plain && !json)
        return false;
    cbm_sha256_ctx hash;
    cbm_sha256_init(&hash);
    cbm_resolve_hash_text(&hash, plain ? fact_text->bytes : json);
    cbm_free(CBM_MEM_CLASS_OTHER, json);
    for (size_t i = 0; i < v->premise_count; i++) {
        const evidence_id_t *premise = find_id(w, v->premises[i]);
        if (!premise)
            return false;
        char hex[65];
        digest_hex(id_digest(w, premise), hex);
        cbm_resolve_hash_text(&hash, hex);
    }
    cbm_sha256_final(&hash, digest);
    return true;
}
static void fact_text_end(fact_text_t *fact_text) {
    cbm_free(CBM_MEM_CLASS_STORE, fact_text->bytes);
    cbm_free(CBM_MEM_CLASS_STORE, fact_text->memo);
}
static bool fact_text_begin(fact_text_t *fact_text) {
    *fact_text = (fact_text_t){0};
    fact_text->memo =
        cbm_calloc(CBM_MEM_CLASS_STORE, FACT_TEXT_MEMO_SLOTS * sizeof(*fact_text->memo));
    return fact_text->memo != NULL;
}

/* One fact's row and its support rows. The digest is already known. */
static bool write_fact_row(evidence_write_t *w, const cbm_resolve_proof_view_t *v, size_t id,
                           const unsigned char *digest) {
    bool ok = true;
    for (size_t i = 0; ok && i < v->premise_count; i++) {
        const evidence_id_t *premise = find_id(w, v->premises[i]);
        if (w->support_table) {
            ok = premise != NULL;
            if (ok) {
                cbm_writer_row_begin(w->support_table);
                cbm_writer_row_int(w->support_table, w->pid);
                cbm_writer_row_int(w->support_table, (sqlite3_int64)id);
                cbm_writer_row_int(w->support_table, (sqlite3_int64)i);
                cbm_writer_row_int(w->support_table, (sqlite3_int64)premise->id);
                ok = table_finish(w, w->support_table, true, "supports");
            }
            continue;
        }
        ok = premise &&
             finish_row(w, w->support,
                        sqlite3_bind_int64(w->support, 1, w->pid) == SQLITE_OK &&
                            sqlite3_bind_int64(w->support, 2, (sqlite3_int64)id) == SQLITE_OK &&
                            sqlite3_bind_int64(w->support, 3, (sqlite3_int64)i) == SQLITE_OK &&
                            sqlite3_bind_int64(w->support, 4, (sqlite3_int64)premise->id) ==
                                SQLITE_OK,
                        "supports");
    }
    return ok && note_type_digest(w, v, digest) &&
           (w->fact_table ? table_fact(w, v, id, digest)
                          : finish_row(w, w->fact, bind_fact(w, v, id, digest), "facts"));
}

/* Each distinct fact after its premises, so a fact id always follows the ids it
 * cites. ids doubles as the visited set: a second hash table over the same nodes
 * would cost a gigabyte on the largest repositories. Recursion is bounded because
 * a node's depth is checked when it is made, and a cycle cannot be built from
 * immutable nodes. */
static bool plan_fact(evidence_write_t *w, uintptr_t ref, size_t *id) {
    if (w->count >= UINT32_MAX)
        return false;
    if (w->count == w->order_capacity) {
        size_t next = w->order_capacity ? w->order_capacity * 2 : 1024;
        if (next > SIZE_MAX / sizeof(*w->order))
            return false;
        uintptr_t *order = cbm_realloc(CBM_MEM_CLASS_STORE, w->order, next * sizeof(*order));
        if (!order)
            return false;
        w->order = order;
        w->order_capacity = next;
    }
    w->order[w->count++] = ref;
    *id = w->count;
    return true;
}
static bool plan_facts_from(evidence_write_t *w, const cbm_resolve_proof_t *node) {
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_PROPAGATED_FACTS, 1);
    if (find_id(w, node))
        return true;
    const cbm_resolve_proof_view_t *v = cbm_resolve_proof_view(node);
    if (!v || (v->premise_count && !v->premises))
        return false;
    for (size_t i = 0; i < v->premise_count; i++)
        if (!plan_facts_from(w, v->premises[i]))
            return false;
    size_t id = 0;
    if (!source_matches(w, &v->source) || !grow_ids(w) || !plan_fact(w, (uintptr_t)node, &id))
        return false;
    hold_id(w, node, (uint32_t)id);
    return true;
}

/* Roots are sorted structurally, so fact ids do not depend on occurrence order. A
 * root is a node, or a row whose occurrence fact has not been composed yet: low bit
 * set. Equal fingerprints are rare, and only they compose a view to compare. */
typedef struct {
    uint64_t fingerprint;
    uintptr_t ref;
} fact_root_t;
typedef struct {
    cbm_resolve_proof_view_t view;
    const cbm_resolve_proof_t *premises[2];
} composed_fact_t;

static CBM_TLS const char *fact_root_project;

static bool root_is_row(const fact_root_t *root) {
    return root->ref & 1;
}
static const cbm_resolve_record_t *root_row(const fact_root_t *root) {
    return (const cbm_resolve_record_t *)(root->ref & ~(uintptr_t)1);
}
static bool compose_fact(composed_fact_t *fact, const cbm_resolve_record_t *row,
                         const char *project) {
    cbm_resolve_proof_source_t source;
    cbm_resolve_proof_key_t key;
    cbm_resolve_result_t outcome;
    size_t count = 0;
    cbm_resolve_record_envelope(row, project, &source, &key, &outcome, fact->premises, &count);
    return cbm_resolve_proof_compose(&fact->view, CBM_PROOF_OCCURRENCE_BINDING, &source, &key,
                                     &outcome, fact->premises, count, true);
}
/* The same fact when only its payload will be read: a root's fingerprint is already known. */
static bool compose_fact_payload(composed_fact_t *fact, const cbm_resolve_record_t *row,
                                 const char *project) {
    cbm_resolve_proof_source_t source;
    cbm_resolve_proof_key_t key;
    cbm_resolve_result_t outcome;
    size_t count = 0;
    cbm_resolve_record_envelope(row, project, &source, &key, &outcome, fact->premises, &count);
    return cbm_resolve_proof_compose_payload(&fact->view, CBM_PROOF_OCCURRENCE_BINDING, &source,
                                             &key, &outcome, fact->premises, count, true);
}
/* Composition of these rows already succeeded when their fingerprints were taken. */
static const cbm_resolve_proof_view_t *root_view(const fact_root_t *root, composed_fact_t *fact) {
    if (!root_is_row(root))
        return cbm_resolve_proof_view((const cbm_resolve_proof_t *)root->ref);
    return compose_fact(fact, root_row(root), fact_root_project) ? &fact->view : NULL;
}
static int compare_roots(const void *left, const void *right) {
    const fact_root_t *a = left, *b = right;
    if (a->fingerprint != b->fingerprint)
        return a->fingerprint < b->fingerprint ? -1 : 1;
    if (a->ref == b->ref)
        return 0;
    composed_fact_t first, second;
    const cbm_resolve_proof_view_t *x = root_view(a, &first), *y = root_view(b, &second);
    if (!x || !y)
        return x ? 1 : (y ? -1 : 0);
    return cbm_resolve_proof_view_compare(x, y);
}
/* A composed root that compares equal to its neighbour is one fact with it, as
 * interning would have made them: the first writes it and the others take its id.
 * Two nodes are the same fact only if they are the same node. */
static bool plan_root(evidence_write_t *w, const fact_root_t *root, const fact_root_t *previous,
                      const cbm_resolve_record_t *rows, size_t *last_id) {
    bool same =
        previous && (root_is_row(root) || root_is_row(previous)) && !compare_roots(previous, root);
    if (!root_is_row(root)) {
        const cbm_resolve_proof_t *node = (const cbm_resolve_proof_t *)root->ref;
        if (same && !find_id(w, node)) {
            if (!grow_ids(w))
                return false;
            hold_id(w, node, (uint32_t)*last_id);
            return true;
        }
        if (!plan_facts_from(w, node))
            return false;
        *last_id = find_id(w, node)->id;
        return true;
    }
    const cbm_resolve_record_t *row = root_row(root);
    if (!same) {
        composed_fact_t fact;
        if (!compose_fact_payload(&fact, row, w->project))
            return false;
        for (size_t i = 0; i < fact.view.premise_count; i++)
            if (!plan_facts_from(w, fact.view.premises[i]))
                return false;
        if (!source_matches(w, &fact.view.source) || !plan_fact(w, root->ref, last_id))
            return false;
    }
    w->row_facts[row - rows] = (uint32_t)*last_id;
    return true;
}

/* ── Digests ─────────────────────────────────────────────────────── */

enum { PROOF_DEPTH_LEVELS = CBM_RESOLVE_PROOF_MAX_DEPTH };

/* Workers take jobs from one counter and stop on the first failure. */
typedef struct {
    const evidence_write_t *w;
    const evidence_id_t **nodes; /* Node digests: one depth at a time. */
    size_t count;
    const uintptr_t *refs; /* Composed digests: one batch at a time. */
    composed_fact_t *facts;
    unsigned char *digests;
    _Atomic size_t next;
    _Atomic bool failed;
} digest_pass_t;

static bool digest_job(digest_pass_t *pass, size_t job, fact_text_t *fact_text) {
    const evidence_write_t *w = pass->w;
    if (pass->nodes) {
        const evidence_id_t *entry = pass->nodes[job];
        return fact_digest(w, cbm_resolve_proof_view(entry->node), fact_text,
                           w->digests + (size_t)entry->digest * DIGEST_BYTES);
    }
    if (!(pass->refs[job] & 1))
        return true;
    const cbm_resolve_record_t *row =
        (const cbm_resolve_record_t *)(pass->refs[job] & ~(uintptr_t)1);
    return compose_fact_payload(&pass->facts[job], row, w->project) &&
           fact_digest(w, &pass->facts[job].view, fact_text, pass->digests + job * DIGEST_BYTES);
}
static void digest_worker(int worker, void *context) {
    (void)worker;
    digest_pass_t *pass = context;
    fact_text_t fact_text;
    if (!fact_text_begin(&fact_text)) {
        atomic_store(&pass->failed, true);
        return;
    }
    for (;;) {
        size_t job = atomic_fetch_add_explicit(&pass->next, 1, memory_order_relaxed);
        if (job >= pass->count || atomic_load_explicit(&pass->failed, memory_order_relaxed))
            break;
        if (!digest_job(pass, job, &fact_text))
            atomic_store(&pass->failed, true);
    }
    fact_text_end(&fact_text);
}
static bool digest_run(const evidence_write_t *w, digest_pass_t *pass) {
    atomic_init(&pass->next, 0);
    atomic_init(&pass->failed, false);
    if (w->workers <= 1 || pass->count < FACT_INLINE_JOBS) {
        digest_worker(0, pass);
    } else {
        cbm_parallel_for_opts_t options = {.max_workers = w->workers, .force_pthreads = false};
        cbm_parallel_for(w->workers, digest_worker, pass, options);
    }
    return !atomic_load(&pass->failed);
}
/* Every node's digest, shallowest first: a premise is always shallower than the fact
 * citing it, so one depth at a time finds its premises done. */
static bool digest_nodes(evidence_write_t *w) {
    if (!w->id_count)
        return true;
    size_t counts[PROOF_DEPTH_LEVELS + 1] = {0};
    for (size_t i = 0; i < w->capacity; i++) {
        if (w->ids[i].node) {
            uint32_t depth = cbm_resolve_proof_view(w->ids[i].node)->depth;
            if (depth > PROOF_DEPTH_LEVELS)
                return false;
            counts[depth]++;
        }
    }
    const evidence_id_t **sorted = cbm_alloc(CBM_MEM_CLASS_STORE, w->id_count * sizeof(*sorted));
    if (!sorted)
        return false;
    size_t starts[PROOF_DEPTH_LEVELS + 2] = {0};
    for (size_t depth = 0; depth <= PROOF_DEPTH_LEVELS; depth++)
        starts[depth + 1] = starts[depth] + counts[depth];
    size_t placed[PROOF_DEPTH_LEVELS + 1];
    memcpy(placed, starts, sizeof(placed));
    for (size_t i = 0; i < w->capacity; i++)
        if (w->ids[i].node)
            sorted[placed[cbm_resolve_proof_view(w->ids[i].node)->depth]++] = &w->ids[i];
    bool ok = true;
    for (size_t depth = 0; ok && depth <= PROOF_DEPTH_LEVELS; depth++) {
        if (!counts[depth])
            continue;
        digest_pass_t pass = {.w = w, .nodes = sorted + starts[depth], .count = counts[depth]};
        ok = digest_run(w, &pass);
    }
    cbm_free(CBM_MEM_CLASS_STORE, sorted);
    return ok;
}
/* Rows in fact-id order. A composed fact's digest is needed once, for its own row, so a
 * batch is composed and digested together and dropped when its rows are written. */
static bool write_planned_facts(evidence_write_t *w) {
    composed_fact_t *facts = cbm_alloc(CBM_MEM_CLASS_STORE, FACT_BATCH * sizeof(*facts));
    unsigned char *digests = cbm_alloc(CBM_MEM_CLASS_STORE, (size_t)FACT_BATCH * DIGEST_BYTES);
    bool ok = facts && digests;
    for (size_t first = 0; ok && first < w->count; first += FACT_BATCH) {
        size_t count = w->count - first < FACT_BATCH ? w->count - first : FACT_BATCH;
        digest_pass_t pass = {
            .w = w, .count = count, .refs = w->order + first, .facts = facts, .digests = digests};
        ok = digest_run(w, &pass);
        for (size_t i = 0; ok && i < count; i++) {
            uintptr_t ref = w->order[first + i];
            if (ref & 1) {
                ok = write_fact_row(w, &facts[i].view, first + i + 1, digests + i * DIGEST_BYTES);
            } else {
                const cbm_resolve_proof_t *node = (const cbm_resolve_proof_t *)ref;
                ok = write_fact_row(w, cbm_resolve_proof_view(node), first + i + 1,
                                    id_digest(w, find_id(w, node)));
            }
        }
    }
    cbm_free(CBM_MEM_CLASS_STORE, facts);
    cbm_free(CBM_MEM_CLASS_STORE, digests);
    return ok;
}
static bool write_facts(evidence_write_t *w, fact_root_t *roots, size_t count,
                        const cbm_resolve_record_t *rows) {
    fact_root_project = w->project;
    char total[32];
    (void)snprintf(total, sizeof(total), "%zu", count);
    cbm_log_info("resolver.evidence.progress", "stage", "sort_roots", "roots", total);
    if (count > 1)
        qsort(roots, count, sizeof(*roots), compare_roots);
    size_t last_id = 0;
    for (size_t i = 0; i < count; i++)
        if (!plan_root(w, &roots[i], i ? &roots[i - 1] : NULL, rows, &last_id))
            return false;
    return digest_nodes(w) && write_planned_facts(w);
}
/* A row's occurrence fact, or the node it already has. */
static bool collect_row_roots(const evidence_write_t *w, const cbm_resolve_record_t *row,
                              fact_root_t *roots, size_t *count) {
    if (row->envelope_pending) {
        composed_fact_t fact;
        if (!row->result.proof || !compose_fact(&fact, row, w->project))
            return false;
        roots[(*count)++] = (fact_root_t){.fingerprint = fact.view.fingerprint,
                                          .ref = (uintptr_t)row | (uintptr_t)1};
    } else if (row->result.proof) {
        roots[(*count)++] = (fact_root_t){.fingerprint = row->result.proof->value.fingerprint,
                                          .ref = (uintptr_t)row->result.proof};
    }
    if (row->binding_proof)
        roots[(*count)++] = (fact_root_t){.fingerprint = row->binding_proof->value.fingerprint,
                                          .ref = (uintptr_t)row->binding_proof};
    return true;
}
/* Fingerprinting a composed fact hashes all its text. A chunk of rows owns a known stretch
 * of roots, so the chunks need no coordination and the roots keep their row order. */
typedef struct {
    const evidence_write_t *w;
    const cbm_resolve_record_t *rows;
    size_t row_count;
    fact_root_t *roots;
    const size_t *chunk_starts;
    _Atomic size_t next;
    _Atomic bool failed;
} root_pass_t;

static void root_worker(int worker, void *context) {
    (void)worker;
    root_pass_t *pass = context;
    size_t chunks = (pass->row_count + ROOT_CHUNK_ROWS - 1) / ROOT_CHUNK_ROWS;
    for (;;) {
        size_t chunk = atomic_fetch_add_explicit(&pass->next, 1, memory_order_relaxed);
        if (chunk >= chunks)
            break;
        size_t first = chunk * ROOT_CHUNK_ROWS;
        size_t last =
            first + ROOT_CHUNK_ROWS < pass->row_count ? first + ROOT_CHUNK_ROWS : pass->row_count;
        size_t at = pass->chunk_starts[chunk];
        for (size_t i = first; i < last; i++)
            if (!collect_row_roots(pass->w, &pass->rows[i], pass->roots, &at))
                atomic_store(&pass->failed, true);
    }
}
static bool collect_all_row_roots(const evidence_write_t *w, const cbm_resolve_record_t *rows,
                                  size_t count, fact_root_t *roots, size_t *root_count) {
    if (!count)
        return true;
    size_t chunks = (count + ROOT_CHUNK_ROWS - 1) / ROOT_CHUNK_ROWS;
    size_t *starts = cbm_alloc(CBM_MEM_CLASS_STORE, chunks * sizeof(*starts));
    if (!starts)
        return false;
    size_t at = *root_count;
    for (size_t i = 0; i < count; i++) {
        if (i % ROOT_CHUNK_ROWS == 0)
            starts[i / ROOT_CHUNK_ROWS] = at;
        at += (rows[i].envelope_pending || rows[i].result.proof) + (rows[i].binding_proof != NULL);
    }
    root_pass_t pass = {
        .w = w, .rows = rows, .row_count = count, .roots = roots, .chunk_starts = starts};
    atomic_init(&pass.next, 0);
    atomic_init(&pass.failed, false);
    if (w->workers <= 1 || chunks == 1) {
        root_worker(0, &pass);
    } else {
        cbm_parallel_for_opts_t options = {.max_workers = w->workers, .force_pthreads = false};
        cbm_parallel_for(w->workers, root_worker, &pass, options);
    }
    cbm_free(CBM_MEM_CLASS_STORE, starts);
    *root_count = at;
    return !atomic_load(&pass.failed);
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
static bool reset_tables(sqlite3 *db, const char *project, sqlite3_int64 *pid, bool rows_written) {
    char *schema = schema_sql();
    bool created = schema && sqlite3_exec(db, schema, NULL, NULL, NULL) == SQLITE_OK;
    cbm_free(CBM_MEM_CLASS_STORE, schema);
    if (!created || !project_id(db, project, pid))
        return false;
    /* Those rows were written for the first project of a new file. */
    if (rows_written && *pid != DIRECT_PID)
        return false;
    const char *by_pid[] = {"DELETE FROM resolver_text WHERE pid=?",
                            "DELETE FROM resolver_occurrence_rows WHERE pid=?",
                            "DELETE FROM resolver_fact_rows WHERE pid=?",
                            "DELETE FROM resolver_support_rows WHERE pid=?"};
    for (size_t i = 0; !rows_written && i < sizeof(by_pid) / sizeof(by_pid[0]); i++) {
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
                            sqlite3_int64 proof, const evidence_id_t *binding) {
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
           sqlite3_bind_int64(s, 23, proof) == SQLITE_OK &&
           sqlite3_bind_int64(s, 24, binding ? (sqlite3_int64)binding->id : 0) == SQLITE_OK;
}
static bool table_occurrence(evidence_write_t *w, const cbm_resolve_record_t *row, size_t ordinal,
                             sqlite3_int64 proof, const evidence_id_t *binding) {
    cbm_writer_table_t *t = w->occurrence_table;
    const cbm_resolve_result_t *r = &row->result;
    int32_t line = -1;
    cbm_writer_row_begin(t);
    cbm_writer_row_int(t, w->pid);
    cbm_writer_row_int(t, (sqlite3_int64)ordinal);
    bool ok = table_text_id(t, dict_id(w, row->file_path, true)) &&
              table_text_id(t, dict_id(w, row->scope, true)) &&
              table_text_id(t, dict_id(w, row->relation, true)) &&
              table_text_id(t, dict_id(w, row->spelling, true));
    cbm_writer_row_int(t, row->start_byte);
    cbm_writer_row_int(t, (sqlite3_int64)row->end_byte - row->start_byte);
    cbm_writer_row_int(t, (int)row->source_origin);
    cbm_writer_row_int(t, (int)r->binding);
    cbm_writer_row_int(t, (int)r->domain);
    cbm_writer_row_int(t, (int)r->analysis);
    cbm_writer_row_int(t, (int)r->rule);
    cbm_writer_row_int(t, r->facts);
    ok = ok && table_text_id(t, dict_id(w, r->target, true));
    cbm_writer_row_int(t, (sqlite3_int64)r->candidate_count);
    ok = ok && table_text_id(t, dict_id(w, r->reason, true));
    cbm_writer_row_int(t, row->emitted ? 1 : 0);
    ok = ok && table_text_id(t, properties_id(w, row, &line));
    cbm_writer_row_int(t, line);
    ok = ok && table_text_id(t, dict_id(w, row->content_identity, true)) &&
         table_text_id(t, dict_id(w, row->configuration_identity, true));
    cbm_writer_row_int(t, proof);
    cbm_writer_row_int(t, binding ? (sqlite3_int64)binding->id : 0);
    return table_finish(w, t, ok, "occurrences");
}
static bool write_occurrences(evidence_write_t *w, const cbm_resolve_record_t *rows, size_t count) {
    for (size_t i = 0; i < count; i++) {
        evidence_id_t *node = rows[i].envelope_pending ? NULL : find_id(w, rows[i].result.proof);
        sqlite3_int64 proof = rows[i].envelope_pending ? w->row_facts[i]
                              : node                   ? (sqlite3_int64)node->id
                                                       : 0;
        evidence_id_t *binding = find_id(w, rows[i].binding_proof);
        cbm_resolve_proof_source_t source = {.project = w->project,
                                             .file = rows[i].file_path,
                                             .content_identity = rows[i].content_identity,
                                             .configuration_identity =
                                                 rows[i].configuration_identity,
                                             .source_origin = rows[i].source_origin};
        /* A claimed emitted row must satisfy the same admission as its edge.
         * A failed metadata write aborts publication, never fabricates zero. */
        if (rows[i].proof_borrowed || (rows[i].emitted && (!cbm_resolve_record_admit(&rows[i]) ||
                                                           !proof || !source_matches(w, &source))))
            return false;
        if (w->occurrence_table
                ? !table_occurrence(w, &rows[i], i, proof, binding)
                : !finish_row(w, w->occurrence, bind_occurrence(w, &rows[i], i, proof, binding),
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
static bool write_summaries(evidence_write_t *w, const cbm_resolver_stage_t *stage) {
    sqlite3_stmt *s = NULL;
    bool ok = sqlite3_prepare_v2(
                  w->db, "INSERT INTO resolver_derived_bindings VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                  -1, &s, NULL) == SQLITE_OK;
    for (size_t i = 0; ok && i < stage->summary_count; i++) {
        const cbm_resolve_summary_t *row = stage->summaries[i];
        ok = finish_row(
            w, s,
            bind_text(s, 1, w->project) && bind_text(s, 2, row->file_path) &&
                bind_text(s, 3, row->key) && sqlite3_bind_int64(s, 4, row->kind) == SQLITE_OK &&
                sqlite3_bind_int(s, 5, row->module) == SQLITE_OK &&
                sqlite3_bind_int(s, 6, row->closed) == SQLITE_OK &&
                sqlite3_bind_int(s, 7, row->result.binding) == SQLITE_OK &&
                sqlite3_bind_int(s, 8, row->result.domain) == SQLITE_OK &&
                sqlite3_bind_int(s, 9, row->result.analysis) == SQLITE_OK &&
                bind_text(s, 10, row->result.target) &&
                sqlite3_bind_int64(s, 11, (sqlite3_int64)stage->summary_proofs[i]) == SQLITE_OK &&
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
/* Each file's digest of its derived facts and each summary's fact id: all that the two
 * remaining tables take from the facts. */
static bool decide_stage(evidence_write_t *w, cbm_resolver_stage_t *stage) {
    const cbm_resolve_summary_t *const *rows = stage->summaries;
    size_t count = stage->summary_count;
    stage->summary_proofs =
        count ? cbm_alloc(CBM_MEM_CLASS_STORE, count * sizeof(*stage->summary_proofs)) : NULL;
    stage->derived = stage->file_count ? cbm_alloc(CBM_MEM_CLASS_STORE,
                                                   stage->file_count * sizeof(*stage->derived))
                                       : NULL;
    if ((count && !stage->summary_proofs) || (stage->file_count && !stage->derived))
        return false;
    for (size_t i = 0; i < count; i++) {
        if (rows[i]->proof_borrowed)
            return false;
        const evidence_id_t *proof = find_id(w, rows[i]->result.proof);
        stage->summary_proofs[i] = proof ? proof->id : 0;
    }
    if (w->type_count > 1)
        qsort(w->types, w->type_count, sizeof(*w->types), compare_type_digest);
    size_t at = 0;
    for (size_t i = 0; i < w->file_count; i++) {
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
        cbm_resolve_hash_finish(&hash, stage->derived[i]);
    }
    return true;
}
static bool write_files(evidence_write_t *w, const cbm_resolver_stage_t *stage) {
    sqlite3_stmt *s = NULL;
    bool ok = sqlite3_prepare_v2(
                  w->db, "INSERT INTO resolver_file_summaries VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", -1,
                  &s, NULL) == SQLITE_OK;
    for (size_t i = 0; ok && i < stage->file_count; i++) {
        const cbm_resolve_file_t *file = stage->files[i];
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
                bind_text(s, 10, file->extracted_summary) && bind_text(s, 11, stage->derived[i]) &&
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
void cbm_store_resolver_stage_free(cbm_resolver_stage_t *stage) {
    if (!stage)
        return;
    for (size_t i = 0; i < sizeof(stage->table_sql) / sizeof(stage->table_sql[0]); i++)
        cbm_free(CBM_MEM_CLASS_STORE, stage->table_sql[i]);
    cbm_free(CBM_MEM_CLASS_STORE, stage->summaries);
    cbm_free(CBM_MEM_CLASS_STORE, stage->summary_proofs);
    cbm_free(CBM_MEM_CLASS_STORE, stage->files);
    cbm_free(CBM_MEM_CLASS_STORE, stage->derived);
    cbm_free(CBM_MEM_CLASS_STORE, stage);
}
/* A table's CREATE as sqlite_schema keeps it: without IF NOT EXISTS or the ';'. */
static char *table_create_sql(const char *table) {
    static const char opening[] = "CREATE TABLE IF NOT EXISTS ";
    size_t name_length = strlen(table);
    const char *at = resolver_schema;
    while ((at = strstr(at, opening)) != NULL) {
        const char *name = at + sizeof(opening) - 1;
        if (!strncmp(name, table, name_length) && name[name_length] == '(') {
            const char *end = strchr(name, ';');
            if (!end)
                return NULL;
            size_t length = (size_t)(end - name);
            char *out = cbm_alloc(CBM_MEM_CLASS_STORE, sizeof("CREATE TABLE ") + length);
            if (out) {
                memcpy(out, "CREATE TABLE ", sizeof("CREATE TABLE ") - 1);
                memcpy(out + sizeof("CREATE TABLE ") - 1, name, length);
                out[sizeof("CREATE TABLE ") - 1 + length] = '\0';
            }
            return out;
        }
        at = name;
    }
    return NULL;
}
static bool begin_tables(evidence_write_t *w, cbm_db_writer_t *direct,
                         cbm_resolver_stage_t *stage) {
    static const char *const names[] = {"resolver_text", "resolver_fact_rows",
                                        "resolver_support_rows", "resolver_occurrence_rows"};
    cbm_writer_table_t **tables[] = {&w->text_table, &w->fact_table, &w->support_table,
                                     &w->occurrence_table};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        stage->table_sql[i] = table_create_sql(names[i]);
        if (!stage->table_sql[i])
            return false;
        *tables[i] = cbm_writer_table_begin(direct, names[i], stage->table_sql[i]);
        if (!*tables[i])
            return false;
    }
    return true;
}
static bool end_tables(evidence_write_t *w) {
    cbm_writer_table_t **tables[] = {&w->text_table, &w->fact_table, &w->support_table,
                                     &w->occurrence_table};
    bool ok = true;
    for (size_t i = 0; i < sizeof(tables) / sizeof(tables[0]); i++) {
        if (*tables[i])
            ok = cbm_writer_table_end(*tables[i]) && ok;
        *tables[i] = NULL;
    }
    return ok;
}
/* One write, with its rows going to the statements on db or to direct's pages. In the
 * second case db is NULL, and *out takes what the store still has to write. */
static bool evidence_run(sqlite3 *db, cbm_db_writer_t *direct, const char *project,
                         const cbm_resolve_record_t *rows, size_t count,
                         const cbm_resolve_file_t *files, size_t file_count,
                         const cbm_resolve_summary_t *summaries, size_t summary_count,
                         bool commit_batches, int workers, cbm_resolver_stage_t **out) {
    if ((!db && !direct) || !project || (!rows && count) || (!files && file_count) ||
        (!summaries && summary_count) || count > (SIZE_MAX - summary_count) / 2 ||
        file_count > SIZE_MAX / sizeof(void *) || summary_count > SIZE_MAX / sizeof(void *))
        return false;
    size_t root_capacity = summary_count;
    for (size_t i = 0; i < count; i++)
        root_capacity +=
            (rows[i].envelope_pending || rows[i].result.proof) + (rows[i].binding_proof != NULL);
    if (root_capacity > SIZE_MAX / sizeof(fact_root_t))
        return false;
    fact_root_t *roots =
        root_capacity ? cbm_alloc(CBM_MEM_CLASS_STORE, root_capacity * sizeof(*roots)) : NULL;
    cbm_resolver_stage_t *stage = cbm_calloc(CBM_MEM_CLASS_STORE, sizeof(*stage));
    const cbm_resolve_file_t **ordered_files =
        file_count ? cbm_alloc(CBM_MEM_CLASS_STORE, file_count * sizeof(*ordered_files)) : NULL;
    const cbm_resolve_summary_t **ordered_summaries =
        summary_count ? cbm_alloc(CBM_MEM_CLASS_STORE, summary_count * sizeof(*ordered_summaries))
                      : NULL;
    dict_cache_t *cache = cbm_calloc(CBM_MEM_CLASS_STORE, DICT_CACHE_SLOTS * sizeof(*cache));
    bool ok = (!root_capacity || roots) && stage && (!file_count || ordered_files) &&
              (!summary_count || ordered_summaries) && cache;
    if (stage) {
        stage->summaries = ordered_summaries;
        stage->summary_count = summary_count;
        stage->files = ordered_files;
        stage->file_count = file_count;
    }
    size_t root_count = 0;
    for (size_t i = 0; ok && i < summary_count; i++) {
        ordered_summaries[i] = &summaries[i];
        if (summaries[i].result.proof)
            roots[root_count++] =
                (fact_root_t){.fingerprint = summaries[i].result.proof->value.fingerprint,
                              .ref = (uintptr_t)summaries[i].result.proof};
    }
    for (size_t i = 0; ok && i < file_count; i++)
        ordered_files[i] = &files[i];
    if (ok && file_count > 1)
        qsort(ordered_files, file_count, sizeof(*ordered_files), compare_files);
    if (ok && summary_count > 1)
        qsort(ordered_summaries, summary_count, sizeof(*ordered_summaries), compare_summaries);
    evidence_write_t *writer = cbm_calloc(CBM_MEM_CLASS_STORE, sizeof(*writer));
    if (!writer) {
        cbm_free(CBM_MEM_CLASS_STORE, roots);
        if (stage) {
            cbm_store_resolver_stage_free(stage);
        } else {
            cbm_free(CBM_MEM_CLASS_STORE, ordered_files);
            cbm_free(CBM_MEM_CLASS_STORE, ordered_summaries);
        }
        cbm_free(CBM_MEM_CLASS_STORE, cache);
        return false;
    }
    evidence_write_t *w = writer;
    w->db = db;
    w->project = project;
    w->files = ordered_files;
    w->file_count = file_count;
    w->cache = cache;
    w->valid_text = cbm_calloc(CBM_MEM_CLASS_STORE, DICT_CACHE_SLOTS * sizeof(*w->valid_text));
    ok = ok && w->valid_text;
    w->commit_batches = commit_batches;
    w->workers = workers;
    cbm_arena_init(&w->owned);
    w->row_facts = count ? cbm_calloc(CBM_MEM_CLASS_STORE, count * sizeof(*w->row_facts)) : NULL;
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    held_digests = 0;
#endif
    ok = ok && (!count || w->row_facts);
    ok = ok && collect_all_row_roots(w, rows, count, roots, &root_count);
    if (direct) {
        w->pid = DIRECT_PID;
        ok = ok && begin_tables(w, direct, stage);
    } else {
        ok = ok && reset_tables(db, project, &w->pid, false) && prepare_writer(w);
    }
    ok = ok && write_facts(w, roots, root_count, rows);
    cbm_free(CBM_MEM_CLASS_STORE, roots);
    ok = ok && write_occurrences(w, rows, count) && decide_stage(w, stage);
    ok = end_tables(w) && ok;
    if (!direct)
        ok = ok && write_summaries(w, stage) && write_files(w, stage);
    sqlite3_finalize(w->text);
    sqlite3_finalize(w->fact);
    sqlite3_finalize(w->support);
    sqlite3_finalize(w->occurrence);
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    held_digests = w->digest_count;
#endif
    cbm_free(CBM_MEM_CLASS_STORE, w->ids);
    cbm_free(CBM_MEM_CLASS_STORE, w->digests);
    cbm_free(CBM_MEM_CLASS_STORE, w->dict);
    cbm_free(CBM_MEM_CLASS_STORE, w->types);
    cbm_free(CBM_MEM_CLASS_STORE, w->order);
    cbm_free(CBM_MEM_CLASS_STORE, w->valid_text);
    cbm_free(CBM_MEM_CLASS_STORE, w->row_facts);
    cbm_free(CBM_MEM_CLASS_STORE, cache);
    cbm_arena_destroy(&w->owned);
    cbm_free(CBM_MEM_CLASS_STORE, writer);
    /* The writer's tables keep pointing at the stage's SQL until finalize, which runs whether
     * this worked or not, so a caller who asked for the stage frees it either way. */
    if (out)
        *out = stage;
    else
        cbm_store_resolver_stage_free(stage);
    return ok;
}
bool cbm_store_resolver_evidence(sqlite3 *db, const char *project, const cbm_resolve_record_t *rows,
                                 size_t count, const cbm_resolve_file_t *files, size_t file_count,
                                 const cbm_resolve_summary_t *summaries, size_t summary_count,
                                 bool commit_batches, int workers) {
    return db && evidence_run(db, NULL, project, rows, count, files, file_count, summaries,
                              summary_count, commit_batches, workers, NULL);
}
bool cbm_store_resolver_stage_write(cbm_db_writer_t *writer, const char *project,
                                    const cbm_resolve_record_t *rows, size_t count,
                                    const cbm_resolve_file_t *files, size_t file_count,
                                    const cbm_resolve_summary_t *summaries, size_t summary_count,
                                    int workers, cbm_resolver_stage_t **stage) {
    return writer && stage &&
           evidence_run(NULL, writer, project, rows, count, files, file_count, summaries,
                        summary_count, false, workers, stage);
}
bool cbm_store_resolver_stage_finish(sqlite3 *db, const char *project,
                                     const cbm_resolver_stage_t *stage) {
    if (!db || !project || !stage)
        return false;
    evidence_write_t *w = cbm_calloc(CBM_MEM_CLASS_STORE, sizeof(*w));
    if (!w)
        return false;
    w->db = db;
    w->project = project;
    bool ok = reset_tables(db, project, &w->pid, true) && write_summaries(w, stage) &&
              write_files(w, stage);
    cbm_free(CBM_MEM_CLASS_STORE, w);
    return ok;
}
