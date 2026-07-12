/*
 * memory.c — User-global, revisioned knowledge memory.
 *
 * Canonical domain rows live in memory_* tables.  Existing nodes/edges are a
 * rebuildable Cypher projection under project "global-memory".  Wiki files are
 * likewise rebuildable from committed memory_revisions through an outbox.
 */

#include "memory/memory.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "foundation/sha256.h"
#include "store/store.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <errno.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#ifdef _WIN32
#include <io.h>
#include <process.h>
#define getpid _getpid
#else
#include <fcntl.h>
#include <unistd.h>
#endif

enum {
    MEM_PATH_MAX = 4096,
    MEM_ID_MAX = 96,
    MEM_TIME_MAX = 32,
    MEM_DEFAULT_LIMIT = 10,
    MEM_MAX_LIMIT = 100,
    MEM_MAX_SOURCE_BYTES = 16 * 1024 * 1024,
    MEM_OUTBOX_LEASE_SECONDS = 30,
};

struct cbm_memory {
    cbm_store_t *graph;
    sqlite3 *db; /* borrowed from graph */
    char *home;
    char *db_path;
    char *raw_objects_dir;
    char *wiki_dir;
    char *export_dir;
    char *sync_dir;
    char lease_owner[MEM_ID_MAX];
    char error[CBM_SZ_512];
};

static atomic_uint_fast64_t memory_handle_sequence = ATOMIC_VAR_INIT(1);

static int memory_recover_outbox(cbm_memory_t *m);
static int memory_rebuild_projection_internal(cbm_memory_t *m, bool in_transaction);
static int memory_relation_add(cbm_memory_t *m, const char *source_kind, const char *source_id,
                               const char *target_kind, const char *target_id, const char *type,
                               const char *relation_key, const char *properties_json, int64_t epoch,
                               const char *now);
static int dirty_source_dependents(cbm_memory_t *m, const char *old_source_id,
                                   const char *new_source_id, int64_t epoch, const char *now);
static char *fts_expression(const char *query);

static char *mem_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t n = strlen(s) + 1;
    char *copy = malloc(n);
    if (copy) {
        memcpy(copy, s, n);
    }
    return copy;
}

static char *path_join2(const char *a, const char *b) {
    if (!a || !b) {
        return NULL;
    }
    size_t an = strlen(a);
    size_t bn = strlen(b);
    bool slash = an > 0 && a[an - 1] != '/';
    char *out = malloc(an + bn + (slash ? 2U : 1U));
    if (!out) {
        return NULL;
    }
    (void)snprintf(out, an + bn + (slash ? 2U : 1U), slash ? "%s/%s" : "%s%s", a, b);
    cbm_normalize_path_sep(out);
    return out;
}

static void iso_now(char out[MEM_TIME_MAX]) {
    time_t now = time(NULL);
    struct tm tmv;
    cbm_gmtime_r(&now, &tmv);
    (void)strftime(out, MEM_TIME_MAX, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

static void memory_set_sqlite_error(cbm_memory_t *m, const char *prefix) {
    if (!m) {
        return;
    }
    (void)snprintf(m->error, sizeof(m->error), "%s: %s", prefix,
                   m->db ? sqlite3_errmsg(m->db) : "database unavailable");
}

static int memory_exec(cbm_memory_t *m, const char *sql) {
    char *err = NULL;
    int rc = sqlite3_exec(m->db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        (void)snprintf(m->error, sizeof(m->error), "sql: %s", err ? err : sqlite3_errmsg(m->db));
        sqlite3_free(err);
        return -1;
    }
    return 0;
}

static int memory_begin(cbm_memory_t *m) {
    /* SQLITE_LOCKED is not covered by busy_timeout. It can be observed for a
     * very short window when another handle has just finalized an autocommit
     * writer, so retry it briefly before surfacing contention. SQLITE_BUSY is
     * already governed by the connection's 10-second busy_timeout. */
    for (int attempt = 0; attempt < 20; attempt++) {
        char *err = NULL;
        int rc = sqlite3_exec(m->db, "BEGIN IMMEDIATE;", NULL, NULL, &err);
        if (rc == SQLITE_OK) {
            sqlite3_free(err);
            return 0;
        }
        int primary = rc & 0xff;
        int extended = sqlite3_extended_errcode(m->db);
        bool retryable = primary == SQLITE_LOCKED;
#ifdef SQLITE_BUSY_SNAPSHOT
        retryable |= extended == SQLITE_BUSY_SNAPSHOT;
#endif
        if (!retryable) {
            (void)snprintf(m->error, sizeof(m->error), "sql: %s (sqlite %d/%d)",
                           err ? err : sqlite3_errmsg(m->db), rc, extended);
            sqlite3_free(err);
            return -1;
        }
        if (attempt == 19) {
            (void)snprintf(m->error, sizeof(m->error), "sql: %s (sqlite %d/%d)",
                           err ? err : sqlite3_errmsg(m->db), rc, extended);
            sqlite3_free(err);
            return -1;
        }
        sqlite3_free(err);
        sqlite3_sleep(5);
    }
    return -1;
}

static int memory_commit_tx(cbm_memory_t *m) {
    return memory_exec(m, "COMMIT;");
}

static void memory_rollback(cbm_memory_t *m) {
    (void)sqlite3_exec(m->db, "ROLLBACK;", NULL, NULL, NULL);
}

static int memory_prepare(cbm_memory_t *m, const char *sql, sqlite3_stmt **out) {
    if (sqlite3_prepare_v2(m->db, sql, -1, out, NULL) != SQLITE_OK) {
        memory_set_sqlite_error(m, "prepare");
        return -1;
    }
    return 0;
}

static int bind_text_or_null(sqlite3_stmt *stmt, int col, const char *value) {
    if (!value) {
        return sqlite3_bind_null(stmt, col);
    }
    return sqlite3_bind_text(stmt, col, value, -1, SQLITE_TRANSIENT);
}

static int resolve_memory_home(const char *override, char out[MEM_PATH_MAX]) {
    if (override && override[0]) {
        if (snprintf(out, MEM_PATH_MAX, "%s", override) >= MEM_PATH_MAX) {
            return -1;
        }
        cbm_normalize_path_sep(out);
        return 0;
    }

    char env[MEM_PATH_MAX] = "";
    if (cbm_safe_getenv("CBM_MEMORY_HOME", env, sizeof(env), NULL) && env[0]) {
        if (snprintf(out, MEM_PATH_MAX, "%s", env) >= MEM_PATH_MAX) {
            return -1;
        }
        cbm_normalize_path_sep(out);
        return 0;
    }

    const char *home = cbm_get_home_dir();
    int path_len = -1;
#ifdef _WIN32
    char local[MEM_PATH_MAX] = "";
    const char *base = cbm_safe_getenv("LOCALAPPDATA", local, sizeof(local), NULL);
    if (!base || !base[0]) {
        base = cbm_safe_getenv("APPDATA", local, sizeof(local), NULL);
    }
    if (base && base[0]) {
        path_len = snprintf(out, MEM_PATH_MAX, "%s/codebase-memory-mcp/memory", base);
    } else if (home) {
        path_len = snprintf(out, MEM_PATH_MAX, "%s/AppData/Local/codebase-memory-mcp/memory", home);
    } else {
        return -1;
    }
#elif defined(__APPLE__)
    if (!home) {
        return -1;
    }
    path_len = snprintf(out, MEM_PATH_MAX,
                        "%s/Library/Application Support/codebase-memory-mcp/memory", home);
#else
    char xdg[MEM_PATH_MAX] = "";
    const char *base = cbm_safe_getenv("XDG_DATA_HOME", xdg, sizeof(xdg), NULL);
    if (base && base[0]) {
        path_len = snprintf(out, MEM_PATH_MAX, "%s/codebase-memory-mcp/memory", base);
    } else if (home) {
        path_len = snprintf(out, MEM_PATH_MAX, "%s/.local/share/codebase-memory-mcp/memory", home);
    } else {
        return -1;
    }
#endif
    if (path_len < 0 || path_len >= MEM_PATH_MAX) {
        return -1;
    }
    cbm_normalize_path_sep(out);
    return 0;
}

static const char *const MEMORY_SCHEMA_V1 =
    "CREATE TABLE IF NOT EXISTS memory_state("
    " singleton INTEGER PRIMARY KEY CHECK(singleton=1),"
    " schema_version INTEGER NOT NULL,"
    " memory_epoch INTEGER NOT NULL DEFAULT 0 CHECK(memory_epoch>=0),"
    " instance_id TEXT NOT NULL,"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS memory_sources("
    " source_id TEXT PRIMARY KEY, content_hash TEXT NOT NULL UNIQUE,"
    " object_relpath TEXT NOT NULL UNIQUE, title TEXT NOT NULL DEFAULT '',"
    " origin TEXT NOT NULL DEFAULT '', media_type TEXT NOT NULL DEFAULT 'text/plain',"
    " publisher TEXT NOT NULL DEFAULT '', published_at TEXT, retrieved_at TEXT NOT NULL,"
    " metadata_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(metadata_json)),"
    " revision_of TEXT REFERENCES memory_sources(source_id),"
    " byte_size INTEGER NOT NULL CHECK(byte_size>=0), created_at TEXT NOT NULL);"
    "CREATE INDEX IF NOT EXISTS idx_memory_sources_origin ON memory_sources(origin,retrieved_at);"
    "CREATE TABLE IF NOT EXISTS memory_pages("
    " page_id TEXT PRIMARY KEY, slug TEXT NOT NULL UNIQUE COLLATE NOCASE,"
    " title TEXT NOT NULL, page_kind TEXT NOT NULL DEFAULT 'concept',"
    " status TEXT NOT NULL DEFAULT 'active', current_revision INTEGER NOT NULL DEFAULT 0,"
    " current_revision_id TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS memory_revisions("
    " revision_id TEXT PRIMARY KEY, page_id TEXT NOT NULL REFERENCES memory_pages(page_id),"
    " revision INTEGER NOT NULL CHECK(revision>0), base_revision INTEGER NOT NULL "
    "CHECK(base_revision>=0),"
    " body_hash TEXT NOT NULL, markdown TEXT NOT NULL,"
    " section_hashes_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(section_hashes_json)),"
    " author_agent TEXT NOT NULL DEFAULT '', author_session TEXT NOT NULL DEFAULT '',"
    " created_epoch INTEGER NOT NULL, created_at TEXT NOT NULL, UNIQUE(page_id,revision));"
    "CREATE INDEX IF NOT EXISTS idx_memory_revisions_page ON memory_revisions(page_id,revision "
    "DESC);"
    "CREATE TABLE IF NOT EXISTS memory_claims("
    " claim_id TEXT PRIMARY KEY,"
    " claim_kind TEXT NOT NULL CHECK(claim_kind IN "
    "('fact','inference','hypothesis','recommendation')),"
    " subject TEXT NOT NULL, predicate TEXT NOT NULL, object_text TEXT NOT NULL,"
    " scope_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(scope_json)),"
    " status TEXT NOT NULL DEFAULT 'unverified' CHECK(status IN "
    " ('unverified','active','stale','contested','superseded','retracted')),"
    " valid_from TEXT, valid_to TEXT, recorded_from TEXT NOT NULL, recorded_to TEXT,"
    " observed_at TEXT, review_after TEXT, volatility TEXT NOT NULL DEFAULT 'normal',"
    " revision INTEGER NOT NULL DEFAULT 1 CHECK(revision>0),"
    " created_epoch INTEGER NOT NULL, updated_epoch INTEGER NOT NULL,"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL,"
    " CHECK(valid_to IS NULL OR valid_from IS NULL OR valid_to>valid_from),"
    " CHECK(recorded_to IS NULL OR recorded_to>=recorded_from));"
    "CREATE INDEX IF NOT EXISTS idx_memory_claims_current ON memory_claims(status,review_after)"
    " WHERE recorded_to IS NULL;"
    "CREATE TABLE IF NOT EXISTS memory_claim_revisions("
    " claim_id TEXT NOT NULL, revision INTEGER NOT NULL, claim_kind TEXT NOT NULL,"
    " subject TEXT NOT NULL, predicate TEXT NOT NULL, object_text TEXT NOT NULL,"
    " scope_json TEXT NOT NULL, status TEXT NOT NULL, valid_from TEXT, valid_to TEXT,"
    " recorded_from TEXT NOT NULL, recorded_to TEXT NOT NULL, observed_at TEXT,"
    " review_after TEXT, volatility TEXT NOT NULL, closed_epoch INTEGER NOT NULL,"
    " created_at TEXT NOT NULL, PRIMARY KEY(claim_id,revision));"
    "CREATE TABLE IF NOT EXISTS memory_decisions("
    " decision_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', chosen_option TEXT NOT NULL,"
    " alternatives_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(alternatives_json)),"
    " rejected_because_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(rejected_because_json)),"
    " assumptions_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(assumptions_json)),"
    " constraints_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(constraints_json)),"
    " applicability_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(applicability_json)),"
    " invalidation_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(invalidation_json)),"
    " expected_outcome TEXT NOT NULL DEFAULT '', actual_outcome TEXT NOT NULL DEFAULT '',"
    " review_after TEXT, exit_criteria_json TEXT NOT NULL DEFAULT '[]' "
    "CHECK(json_valid(exit_criteria_json)),"
    " status TEXT NOT NULL DEFAULT 'active', revision INTEGER NOT NULL DEFAULT 1,"
    " created_epoch INTEGER NOT NULL, updated_epoch INTEGER NOT NULL,"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS memory_experiences("
    " experience_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', context_json TEXT NOT NULL "
    "DEFAULT '{}' CHECK(json_valid(context_json)),"
    " environment_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(environment_json)),"
    " action TEXT NOT NULL DEFAULT '', observation TEXT NOT NULL, outcome TEXT NOT NULL DEFAULT '',"
    " sample_size INTEGER, generalization_limits_json TEXT NOT NULL DEFAULT '[]' "
    "CHECK(json_valid(generalization_limits_json)),"
    " failure_signals_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(failure_signals_json)),"
    " status TEXT NOT NULL DEFAULT 'active', revision INTEGER NOT NULL DEFAULT 1,"
    " created_epoch INTEGER NOT NULL, updated_epoch INTEGER NOT NULL,"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS memory_preferences("
    " preference_id TEXT PRIMARY KEY, title TEXT NOT NULL DEFAULT '', value_text TEXT NOT NULL,"
    " scope_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(scope_json)),"
    " context_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(context_json)),"
    " rationale TEXT NOT NULL DEFAULT '', status TEXT NOT NULL DEFAULT 'active',"
    " revision INTEGER NOT NULL DEFAULT 1, created_epoch INTEGER NOT NULL, updated_epoch INTEGER "
    "NOT NULL,"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS memory_relations("
    " relation_id TEXT PRIMARY KEY, source_kind TEXT NOT NULL, source_id TEXT NOT NULL,"
    " target_kind TEXT NOT NULL, target_id TEXT NOT NULL, type TEXT NOT NULL,"
    " relation_key TEXT NOT NULL DEFAULT '', properties_json TEXT NOT NULL DEFAULT '{}' "
    "CHECK(json_valid(properties_json)),"
    " valid_from TEXT, valid_to TEXT, recorded_from TEXT NOT NULL, recorded_to TEXT,"
    " created_epoch INTEGER NOT NULL, closed_epoch INTEGER,"
    " CHECK(recorded_to IS NULL OR recorded_to>=recorded_from));"
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_relations_current ON memory_relations"
    " (source_kind,source_id,target_kind,target_id,type,relation_key) WHERE recorded_to IS NULL;"
    "CREATE INDEX IF NOT EXISTS idx_memory_relations_source ON "
    "memory_relations(source_kind,source_id,type);"
    "CREATE INDEX IF NOT EXISTS idx_memory_relations_target ON "
    "memory_relations(target_kind,target_id,type);"
    "CREATE TABLE IF NOT EXISTS memory_code_refs("
    " code_ref_id TEXT PRIMARY KEY, project TEXT NOT NULL, git_remote TEXT NOT NULL DEFAULT '',"
    " ref_kind TEXT NOT NULL DEFAULT 'symbol' CHECK(ref_kind IN "
    "('symbol','file','api_schema','dependency')),"
    " qualified_name TEXT NOT NULL DEFAULT '', file_path TEXT NOT NULL DEFAULT '',"
    " locator_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(locator_json)),"
    " properties_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(properties_json)),"
    " commit_or_tree_hash TEXT NOT NULL DEFAULT '', last_resolved_at TEXT,"
    " resolution_status TEXT NOT NULL DEFAULT 'unresolved', revision INTEGER NOT NULL DEFAULT 1,"
    " created_epoch INTEGER NOT NULL, updated_epoch INTEGER NOT NULL,"
    " created_at TEXT NOT NULL, updated_at TEXT NOT NULL,"
    " UNIQUE(project,ref_kind,qualified_name,file_path));"
    "CREATE INDEX IF NOT EXISTS idx_memory_code_refs_file ON memory_code_refs(project,file_path);"
    "CREATE TABLE IF NOT EXISTS memory_proposals("
    " proposal_id TEXT PRIMARY KEY, agent_id TEXT NOT NULL DEFAULT '', session_id TEXT NOT NULL "
    "DEFAULT '',"
    " base_epoch INTEGER NOT NULL, status TEXT NOT NULL CHECK(status IN "
    " ('pending','approved','committed','conflicted','abandoned')),"
    " operations_json TEXT NOT NULL CHECK(json_valid(operations_json)),"
    " expected_revisions_json TEXT NOT NULL DEFAULT '{}' "
    "CHECK(json_valid(expected_revisions_json)),"
    " reason TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL, resolved_at TEXT,"
    " committed_epoch INTEGER, conflict_json TEXT, result_json TEXT);"
    "CREATE INDEX IF NOT EXISTS idx_memory_proposals_status ON memory_proposals(status,created_at);"
    "CREATE TABLE IF NOT EXISTS memory_operations("
    " operation_id TEXT PRIMARY KEY, proposal_id TEXT NOT NULL, committed_epoch INTEGER NOT NULL,"
    " result_json TEXT NOT NULL CHECK(json_valid(result_json)), created_at TEXT NOT NULL);"
    "CREATE TABLE IF NOT EXISTS memory_activities("
    " activity_id TEXT PRIMARY KEY, operation_id TEXT NOT NULL UNIQUE, proposal_id TEXT,"
    " agent_id TEXT NOT NULL DEFAULT '', session_id TEXT NOT NULL DEFAULT '', action TEXT NOT NULL,"
    " base_epoch INTEGER NOT NULL, committed_epoch INTEGER NOT NULL, user_approved INTEGER NOT "
    "NULL DEFAULT 0,"
    " details_json TEXT NOT NULL DEFAULT '{}' CHECK(json_valid(details_json)), created_at TEXT NOT "
    "NULL);"
    "CREATE TABLE IF NOT EXISTS memory_outbox("
    " outbox_id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, aggregate_id TEXT NOT NULL,"
    " revision INTEGER NOT NULL, payload_json TEXT NOT NULL CHECK(json_valid(payload_json)),"
    " state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','leased','done','failed')),"
    " lease_owner TEXT, lease_until TEXT, attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,"
    " created_at TEXT NOT NULL, processed_at TEXT, UNIQUE(kind,aggregate_id,revision));"
    "CREATE INDEX IF NOT EXISTS idx_memory_outbox_pending ON memory_outbox(state,outbox_id);"
    "CREATE TABLE IF NOT EXISTS memory_dirty("
    " dirty_id INTEGER PRIMARY KEY AUTOINCREMENT, entity_kind TEXT NOT NULL, entity_id TEXT NOT "
    "NULL,"
    " reason TEXT NOT NULL, source_id TEXT NOT NULL DEFAULT '', detected_epoch INTEGER NOT NULL,"
    " status TEXT NOT NULL DEFAULT 'open', details_json TEXT NOT NULL DEFAULT '{}' "
    "CHECK(json_valid(details_json)),"
    " created_at TEXT NOT NULL, resolved_at TEXT);"
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_memory_dirty_open ON "
    "memory_dirty(entity_kind,entity_id,reason,source_id)"
    " WHERE status='open';"
    "CREATE TABLE IF NOT EXISTS memory_documents("
    " doc_id INTEGER PRIMARY KEY AUTOINCREMENT, entity_kind TEXT NOT NULL, entity_id TEXT NOT NULL,"
    " title TEXT NOT NULL DEFAULT '', summary TEXT NOT NULL DEFAULT '', body TEXT NOT NULL DEFAULT "
    "'',"
    " metadata TEXT NOT NULL DEFAULT '', UNIQUE(entity_kind,entity_id));"
    "CREATE VIRTUAL TABLE IF NOT EXISTS memory_fts USING fts5("
    " title,summary,body,entity_kind UNINDEXED,entity_id UNINDEXED,"
    " content='memory_documents',content_rowid='doc_id',tokenize='unicode61 remove_diacritics 2');"
    "CREATE TRIGGER IF NOT EXISTS memory_documents_ai AFTER INSERT ON memory_documents BEGIN"
    " INSERT INTO memory_fts(rowid,title,summary,body,entity_kind,entity_id)"
    " VALUES(new.doc_id,new.title,new.summary,new.body,new.entity_kind,new.entity_id); END;"
    "CREATE TRIGGER IF NOT EXISTS memory_documents_ad AFTER DELETE ON memory_documents BEGIN"
    " INSERT INTO memory_fts(memory_fts,rowid,title,summary,body,entity_kind,entity_id)"
    " VALUES('delete',old.doc_id,old.title,old.summary,old.body,old.entity_kind,old.entity_id); "
    "END;"
    "CREATE TRIGGER IF NOT EXISTS memory_documents_au AFTER UPDATE ON memory_documents BEGIN"
    " INSERT INTO memory_fts(memory_fts,rowid,title,summary,body,entity_kind,entity_id)"
    " VALUES('delete',old.doc_id,old.title,old.summary,old.body,old.entity_kind,old.entity_id);"
    " INSERT INTO memory_fts(rowid,title,summary,body,entity_kind,entity_id)"
    " VALUES(new.doc_id,new.title,new.summary,new.body,new.entity_kind,new.entity_id); END;";

static int memory_schema_version(cbm_memory_t *m) {
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, "PRAGMA user_version;", &stmt) != 0) {
        return -1;
    }
    int version = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        version = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return version;
}

static int memory_migrate(cbm_memory_t *m) {
    if (memory_begin(m) != 0) {
        return -1;
    }
    int version = memory_schema_version(m);
    if (version < 0 || version > CBM_MEMORY_SCHEMA_VERSION) {
        (void)snprintf(m->error, sizeof(m->error), "unsupported memory schema version %d", version);
        memory_rollback(m);
        return -1;
    }
    if (version == 0) {
        if (memory_exec(m, MEMORY_SCHEMA_V1) != 0 ||
            memory_exec(m, "PRAGMA user_version=1;") != 0) {
            memory_rollback(m);
            return -1;
        }
    }

    char now[MEM_TIME_MAX];
    iso_now(now);
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(
            m,
            "INSERT OR IGNORE INTO "
            "memory_state(singleton,schema_version,memory_epoch,instance_id,created_at,updated_at)"
            " VALUES(1,?,0,lower(hex(randomblob(16))),?,?);",
            &stmt) != 0) {
        memory_rollback(m);
        return -1;
    }
    sqlite3_bind_int(stmt, 1, CBM_MEMORY_SCHEMA_VERSION);
    bind_text_or_null(stmt, 2, now);
    bind_text_or_null(stmt, 3, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        memory_set_sqlite_error(m, "initialize memory state");
        memory_rollback(m);
        return -1;
    }
    if (memory_commit_tx(m) != 0) {
        memory_rollback(m);
        return -1;
    }
    return 0;
}

static void memory_free_paths(cbm_memory_t *m) {
    if (!m) {
        return;
    }
    free(m->home);
    free(m->db_path);
    free(m->raw_objects_dir);
    free(m->wiki_dir);
    free(m->export_dir);
    free(m->sync_dir);
}

static void memory_harden_sqlite_files(cbm_memory_t *m) {
#ifndef _WIN32
    if (!m || !m->db_path) {
        return;
    }
    (void)chmod(m->db_path, 0600);
    static const char *const suffixes[] = {"-wal", "-shm", "-journal"};
    for (size_t i = 0; i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
        char path[MEM_PATH_MAX];
        if (snprintf(path, sizeof(path), "%s%s", m->db_path, suffixes[i]) < (int)sizeof(path) &&
            cbm_file_exists(path)) {
            (void)chmod(path, 0600);
        }
    }
#else
    (void)m;
#endif
}

cbm_memory_t *cbm_memory_open(const char *home_override) {
    char home[MEM_PATH_MAX];
    if (resolve_memory_home(home_override, home) != 0) {
        return NULL;
    }
    if (!cbm_mkdir_p(home, 0700)) {
        return NULL;
    }

    cbm_memory_t *m = calloc(1, sizeof(*m));
    if (!m) {
        return NULL;
    }
    uint64_t handle_sequence =
        atomic_fetch_add_explicit(&memory_handle_sequence, 1, memory_order_relaxed);
    if (snprintf(m->lease_owner, sizeof(m->lease_owner), "materializer-%ld-%" PRIu64,
                 (long)getpid(), handle_sequence) >= (int)sizeof(m->lease_owner)) {
        free(m);
        return NULL;
    }
    m->home = mem_strdup(home);
    m->db_path = path_join2(home, CBM_MEMORY_DB_FILENAME);
    m->raw_objects_dir = path_join2(home, "raw/objects");
    m->wiki_dir = path_join2(home, "wiki");
    m->export_dir = path_join2(home, "export");
    m->sync_dir = path_join2(home, "sync");
    if (!m->home || !m->db_path || !m->raw_objects_dir || !m->wiki_dir || !m->export_dir ||
        !m->sync_dir || !cbm_mkdir_p(m->raw_objects_dir, 0700) || !cbm_mkdir_p(m->wiki_dir, 0700) ||
        !cbm_mkdir_p(m->export_dir, 0700) || !cbm_mkdir_p(m->sync_dir, 0700)) {
        memory_free_paths(m);
        free(m);
        return NULL;
    }
#ifndef _WIN32
    if (chmod(m->home, 0700) != 0 || chmod(m->raw_objects_dir, 0700) != 0 ||
        chmod(m->wiki_dir, 0700) != 0 || chmod(m->export_dir, 0700) != 0 ||
        chmod(m->sync_dir, 0700) != 0) {
        memory_free_paths(m);
        free(m);
        return NULL;
    }
#endif

    m->graph = cbm_store_open_path(m->db_path);
    if (!m->graph) {
        memory_free_paths(m);
        free(m);
        return NULL;
    }
    m->db = cbm_store_get_db(m->graph);
#ifndef _WIN32
    if (chmod(m->db_path, 0600) != 0) {
        cbm_store_close(m->graph);
        memory_free_paths(m);
        free(m);
        return NULL;
    }
#endif
    if (!m->db || memory_migrate(m) != 0 ||
        cbm_store_upsert_project(m->graph, CBM_MEMORY_PROJECT, m->home) != CBM_STORE_OK) {
        cbm_store_close(m->graph);
        memory_free_paths(m);
        free(m);
        return NULL;
    }
    (void)memory_recover_outbox(m);
    memory_harden_sqlite_files(m);
    return m;
}

void cbm_memory_close(cbm_memory_t *m) {
    if (!m) {
        return;
    }
    if (m->graph) {
        cbm_store_close(m->graph);
    }
    memory_free_paths(m);
    free(m);
}

const char *cbm_memory_home(const cbm_memory_t *m) {
    return m ? m->home : NULL;
}

const char *cbm_memory_db_path(const cbm_memory_t *m) {
    return m ? m->db_path : NULL;
}

cbm_store_t *cbm_memory_graph_store(cbm_memory_t *m) {
    return m ? m->graph : NULL;
}

sqlite3 *cbm_memory_db(cbm_memory_t *m) {
    return m ? m->db : NULL;
}

int64_t cbm_memory_snapshot_epoch(cbm_memory_t *m) {
    if (!m) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, "SELECT memory_epoch FROM memory_state WHERE singleton=1;", &stmt) != 0) {
        return -1;
    }
    int64_t epoch = -1;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        epoch = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return epoch;
}

/* ── JSON / identity helpers ──────────────────────────────────── */

static char *json_write_mut(yyjson_mut_doc *doc) {
    size_t len = 0;
    return yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, &len);
}

static char *json_error(const char *code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_bool(doc, root, "ok", false);
    yyjson_mut_obj_add_strcpy(doc, root, "error", code ? code : "memory_error");
    yyjson_mut_obj_add_strcpy(doc, root, "message", message ? message : "memory operation failed");
    char *out = json_write_mut(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static yyjson_doc *parse_object_args(const char *args_json, yyjson_val **root_out) {
    const char *input = args_json ? args_json : "{}";
    yyjson_doc *doc = yyjson_read(input, strlen(input), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return NULL;
    }
    *root_out = root;
    return doc;
}

static const char *json_str(yyjson_val *obj, const char *key) {
    yyjson_val *value = yyjson_obj_get(obj, key);
    return value && yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
}

static int64_t json_int(yyjson_val *obj, const char *key, int64_t fallback) {
    yyjson_val *value = yyjson_obj_get(obj, key);
    return value && yyjson_is_int(value) ? yyjson_get_sint(value) : fallback;
}

static bool json_bool(yyjson_val *obj, const char *key, bool fallback) {
    yyjson_val *value = yyjson_obj_get(obj, key);
    return value && yyjson_is_bool(value) ? yyjson_get_bool(value) : fallback;
}

static char *json_field_serialized(yyjson_val *obj, const char *key, const char *fallback) {
    yyjson_val *value = yyjson_obj_get(obj, key);
    if (!value) {
        return mem_strdup(fallback);
    }
    return yyjson_val_write(value, YYJSON_WRITE_ALLOW_INVALID_UNICODE, NULL);
}

static int memory_new_id(cbm_memory_t *m, const char *prefix, char out[MEM_ID_MAX]) {
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, "SELECT lower(hex(randomblob(16)));", &stmt) != 0) {
        return -1;
    }
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return -1;
    }
    const char *random_hex = (const char *)sqlite3_column_text(stmt, 0);
    int n = snprintf(out, MEM_ID_MAX, "%s_%s", prefix, random_hex ? random_hex : "");
    sqlite3_finalize(stmt);
    return (n > 0 && n < MEM_ID_MAX) ? 0 : -1;
}

static int64_t memory_bump_epoch(cbm_memory_t *m, const char *now) {
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m,
                       "UPDATE memory_state SET memory_epoch=memory_epoch+1,updated_at=?"
                       " WHERE singleton=1;",
                       &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE || sqlite3_changes(m->db) != 1) {
        memory_set_sqlite_error(m, "advance memory epoch");
        return -1;
    }
    return cbm_memory_snapshot_epoch(m);
}

static char *make_node_properties(const char *id_key, const char *id, const char *status,
                                  int revision, int64_t epoch) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, obj);
    yyjson_mut_obj_add_strcpy(doc, obj, id_key, id);
    if (status) {
        yyjson_mut_obj_add_strcpy(doc, obj, "status", status);
    }
    if (revision >= 0) {
        yyjson_mut_obj_add_int(doc, obj, "revision", revision);
    }
    if (epoch >= 0) {
        yyjson_mut_obj_add_int(doc, obj, "updated_epoch", epoch);
    }
    char *json = json_write_mut(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

static int64_t memory_graph_node(cbm_memory_t *m, const char *kind, const char *label,
                                 const char *id, const char *name, const char *file_path,
                                 const char *properties_json) {
    char qn[MEM_PATH_MAX];
    if (snprintf(qn, sizeof(qn), "memory:%s:%s", kind, id) >= (int)sizeof(qn)) {
        return -1;
    }
    cbm_node_t node = {.project = CBM_MEMORY_PROJECT,
                       .label = label,
                       .name = name ? name : id,
                       .qualified_name = qn,
                       .file_path = file_path ? file_path : "",
                       .properties_json = properties_json ? properties_json : "{}"};
    return cbm_store_upsert_node(m->graph, &node);
}

static int memory_document_upsert(cbm_memory_t *m, const char *kind, const char *id,
                                  const char *title, const char *summary, const char *body,
                                  const char *metadata) {
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO memory_documents(entity_kind,entity_id,title,summary,body,metadata)"
        " VALUES(?,?,?,?,?,?) ON CONFLICT(entity_kind,entity_id) DO UPDATE SET"
        " title=excluded.title,summary=excluded.summary,body=excluded.body,metadata=excluded."
        "metadata;";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, kind);
    bind_text_or_null(stmt, 2, id);
    bind_text_or_null(stmt, 3, title ? title : "");
    bind_text_or_null(stmt, 4, summary ? summary : "");
    bind_text_or_null(stmt, 5, body ? body : "");
    bind_text_or_null(stmt, 6, metadata ? metadata : "");
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

/* ── Immutable source objects ─────────────────────────────────── */

static int read_file_bytes(const char *path, unsigned char **out, size_t *out_len) {
    *out = NULL;
    *out_len = 0;
    FILE *fp = cbm_fopen(path, "rb");
    if (!fp) {
        return -1;
    }
    if (fseek(fp, 0, SEEK_END) != 0) {
        fclose(fp);
        return -1;
    }
    long size = ftell(fp);
    if (size < 0 || size > MEM_MAX_SOURCE_BYTES || fseek(fp, 0, SEEK_SET) != 0) {
        fclose(fp);
        return -1;
    }
    unsigned char *bytes = malloc((size_t)size + 1U);
    if (!bytes) {
        fclose(fp);
        return -1;
    }
    size_t got = fread(bytes, 1, (size_t)size, fp);
    int failed = ferror(fp);
    fclose(fp);
    if (failed || got != (size_t)size) {
        free(bytes);
        return -1;
    }
    bytes[got] = 0;
    *out = bytes;
    *out_len = got;
    return 0;
}

static bool raw_object_matches(const char *path, const unsigned char *bytes, size_t len,
                               const char *expected_hash) {
    if (!cbm_file_exists(path) || cbm_file_size(path) != (int64_t)len) {
        return false;
    }
    unsigned char *existing = NULL;
    size_t existing_len = 0;
    if (read_file_bytes(path, &existing, &existing_len) != 0) {
        return false;
    }
    char hash[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(existing, existing_len, hash);
    bool matches = existing_len == len && strcmp(hash, expected_hash) == 0 &&
                   (len == 0 || memcmp(existing, bytes, len) == 0);
    free(existing);
    return matches;
}

static int durable_flush(FILE *fp) {
    if (fflush(fp) != 0) {
        return -1;
    }
#ifdef _WIN32
    return _commit(_fileno(fp));
#else
    return fsync(fileno(fp));
#endif
}

static int sync_parent_directory(const char *path) {
#ifdef _WIN32
    (void)path;
    return 0; /* cbm_rename_replace uses MoveFileExW write-through semantics. */
#else
    char dir[MEM_PATH_MAX];
    int n = snprintf(dir, sizeof(dir), "%s", path);
    if (n < 0 || n >= (int)sizeof(dir)) {
        return -1;
    }
    char *slash = strrchr(dir, '/');
    if (!slash) {
        return -1;
    }
    *slash = '\0';
#ifdef O_DIRECTORY
    int fd = open(dir, O_RDONLY | O_DIRECTORY);
#else
    int fd = open(dir, O_RDONLY);
#endif
    if (fd < 0) {
        return -1;
    }
    int rc = fsync(fd);
    close(fd);
    return rc;
#endif
}

static int write_raw_object(cbm_memory_t *m, const char *final_path, const unsigned char *bytes,
                            size_t len, const char *hash) {
    (void)m;
    if (cbm_file_exists(final_path)) {
#ifndef _WIN32
        if (chmod(final_path, 0600) != 0) {
            return -1;
        }
#endif
        return raw_object_matches(final_path, bytes, len, hash) ? 0 : -1;
    }
    static atomic_uint_fast64_t sequence = 1;
    uint64_t seq = atomic_fetch_add_explicit(&sequence, 1, memory_order_relaxed);
    char temp[MEM_PATH_MAX];
    int n = snprintf(temp, sizeof(temp), "%s.tmp.%ld.%" PRIu64, final_path, (long)getpid(), seq);
    if (n < 0 || n >= (int)sizeof(temp)) {
        return -1;
    }
    FILE *fp = cbm_fopen(temp, "wb");
    if (!fp) {
        return -1;
    }
    size_t written = len ? fwrite(bytes, 1, len, fp) : 0;
    int flush_rc = written == len ? durable_flush(fp) : -1;
    int close_rc = fclose(fp);
    if (flush_rc != 0 || close_rc != 0) {
        (void)cbm_unlink(temp);
        return -1;
    }
#ifndef _WIN32
    if (chmod(temp, 0600) != 0) {
        (void)cbm_unlink(temp);
        return -1;
    }
#endif
    if (cbm_file_exists(final_path)) {
        (void)cbm_unlink(temp);
        return raw_object_matches(final_path, bytes, len, hash) ? 0 : -1;
    }
    if (cbm_rename_replace(temp, final_path) != 0) {
        (void)cbm_unlink(temp);
        return -1;
    }
    return raw_object_matches(final_path, bytes, len, hash) &&
                   sync_parent_directory(final_path) == 0
               ? 0
               : -1;
}

static bool safe_path_component(const char *value) {
    if (!value || !value[0] || strlen(value) > 255 || strcmp(value, ".") == 0 ||
        strcmp(value, "..") == 0) {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '-' || *p == '_')) {
            return false;
        }
    }
    return true;
}

static bool safe_entity_id(const char *value) {
    if (!value || !value[0] || strlen(value) >= MEM_ID_MAX) {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)value; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '_' || *p == '-' || *p == '.' || *p == ':')) {
            return false;
        }
    }
    return true;
}

static int write_file_replace(const char *path, const char *body, size_t len) {
    static atomic_uint_fast64_t sequence = 1;
    uint64_t seq = atomic_fetch_add_explicit(&sequence, 1, memory_order_relaxed);
    char temp[MEM_PATH_MAX];
    int n = snprintf(temp, sizeof(temp), "%s.tmp.%ld.%" PRIu64, path, (long)getpid(), seq);
    if (n < 0 || n >= (int)sizeof(temp)) {
        return -1;
    }
    FILE *fp = cbm_fopen(temp, "wb");
    if (!fp) {
        return -1;
    }
    size_t written = len ? fwrite(body, 1, len, fp) : 0;
    int flush_rc = written == len ? durable_flush(fp) : -1;
    int close_rc = fclose(fp);
    if (flush_rc != 0 || close_rc != 0) {
        (void)cbm_unlink(temp);
        return -1;
    }
#ifndef _WIN32
    if (chmod(temp, 0600) != 0) {
        (void)cbm_unlink(temp);
        return -1;
    }
#endif
    if (cbm_rename_replace(temp, path) != 0) {
        (void)cbm_unlink(temp);
        return -1;
    }
    return sync_parent_directory(path);
}

static int materialize_page(cbm_memory_t *m, const char *page_id, int revision) {
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT p.page_kind,p.slug,r.markdown FROM memory_pages p"
                      " JOIN memory_revisions r ON r.page_id=p.page_id AND r.revision=?"
                      " WHERE p.page_id=?;";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    sqlite3_bind_int(stmt, 1, revision);
    bind_text_or_null(stmt, 2, page_id);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return -1;
    }
    const char *kind_col = (const char *)sqlite3_column_text(stmt, 0);
    const char *slug_col = (const char *)sqlite3_column_text(stmt, 1);
    const char *markdown_col = (const char *)sqlite3_column_text(stmt, 2);
    char *kind = mem_strdup(kind_col ? kind_col : "");
    char *slug = mem_strdup(slug_col ? slug_col : "");
    char *markdown = mem_strdup(markdown_col ? markdown_col : "");
    sqlite3_finalize(stmt);
    if (!kind || !slug || !markdown || !safe_path_component(kind) || !safe_path_component(slug)) {
        free(kind);
        free(slug);
        free(markdown);
        return -1;
    }
    char dir[MEM_PATH_MAX];
    char path[MEM_PATH_MAX];
    int dn = snprintf(dir, sizeof(dir), "%s/%s", m->wiki_dir, kind);
    int pn = snprintf(path, sizeof(path), "%s/%s.md", dir, slug);
    bool dir_ready = dn >= 0 && dn < (int)sizeof(dir) && pn >= 0 && pn < (int)sizeof(path) &&
                     cbm_mkdir_p(dir, 0700);
#ifndef _WIN32
    if (dir_ready && chmod(dir, 0700) != 0) {
        dir_ready = false;
    }
#endif
    int rc = dir_ready ? write_file_replace(path, markdown, strlen(markdown)) : -1;
    free(kind);
    free(slug);
    free(markdown);
    return rc;
}

static void iso_after_seconds(int seconds, char out[MEM_TIME_MAX]) {
    time_t when = time(NULL) + seconds;
    struct tm tmv;
    cbm_gmtime_r(&when, &tmv);
    (void)strftime(out, MEM_TIME_MAX, "%Y-%m-%dT%H:%M:%SZ", &tmv);
}

static int memory_recover_outbox(cbm_memory_t *m) {
    if (!m) {
        return -1;
    }
    const char *worker = m->lease_owner;
    int processed = 0;
    for (;;) {
        char now[MEM_TIME_MAX];
        char lease_until[MEM_TIME_MAX];
        iso_now(now);
        iso_after_seconds(MEM_OUTBOX_LEASE_SECONDS, lease_until);
        if (memory_begin(m) != 0) {
            return processed > 0 ? processed : -1;
        }
        sqlite3_stmt *stmt = NULL;
        const char *select_sql = "SELECT outbox_id,aggregate_id,revision FROM memory_outbox"
                                 " WHERE state='pending' OR (state='leased' AND lease_until<=?)"
                                 " ORDER BY outbox_id LIMIT 1;";
        if (memory_prepare(m, select_sql, &stmt) != 0) {
            memory_rollback(m);
            return processed > 0 ? processed : -1;
        }
        bind_text_or_null(stmt, 1, now);
        if (sqlite3_step(stmt) != SQLITE_ROW) {
            sqlite3_finalize(stmt);
            (void)memory_commit_tx(m);
            break;
        }
        int64_t outbox_id = sqlite3_column_int64(stmt, 0);
        const char *aggregate_col = (const char *)sqlite3_column_text(stmt, 1);
        char *aggregate_id = mem_strdup(aggregate_col ? aggregate_col : "");
        int revision = sqlite3_column_int(stmt, 2);
        sqlite3_finalize(stmt);
        if (!aggregate_id) {
            memory_rollback(m);
            return processed > 0 ? processed : -1;
        }
        if (memory_prepare(m,
                           "UPDATE memory_outbox SET state='leased',lease_owner=?,lease_until=?,"
                           " attempts=attempts+1 WHERE outbox_id=? AND"
                           " (state='pending' OR (state='leased' AND lease_until<=?));",
                           &stmt) != 0) {
            free(aggregate_id);
            memory_rollback(m);
            return processed > 0 ? processed : -1;
        }
        bind_text_or_null(stmt, 1, worker);
        bind_text_or_null(stmt, 2, lease_until);
        sqlite3_bind_int64(stmt, 3, outbox_id);
        bind_text_or_null(stmt, 4, now);
        int claim_rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (claim_rc != SQLITE_DONE || sqlite3_changes(m->db) != 1 || memory_commit_tx(m) != 0) {
            free(aggregate_id);
            memory_rollback(m);
            continue;
        }

        int materialize_rc = materialize_page(m, aggregate_id, revision);
        iso_now(now);
        if (memory_begin(m) != 0) {
            free(aggregate_id);
            return processed > 0 ? processed : -1;
        }
        const char *finish_sql =
            materialize_rc == 0
                ? "UPDATE memory_outbox SET state='done',processed_at=?,lease_owner=NULL,"
                  " lease_until=NULL,last_error=NULL WHERE outbox_id=? AND lease_owner=?;"
                : "UPDATE memory_outbox SET state='failed',processed_at=?,lease_owner=NULL,"
                  " lease_until=NULL,last_error='materialization failed' WHERE outbox_id=?"
                  " AND lease_owner=?;";
        if (memory_prepare(m, finish_sql, &stmt) != 0) {
            free(aggregate_id);
            memory_rollback(m);
            return processed > 0 ? processed : -1;
        }
        bind_text_or_null(stmt, 1, now);
        sqlite3_bind_int64(stmt, 2, outbox_id);
        bind_text_or_null(stmt, 3, worker);
        int finish_rc = sqlite3_step(stmt);
        int finish_changes = sqlite3_changes(m->db);
        sqlite3_finalize(stmt);
        free(aggregate_id);
        if (finish_rc != SQLITE_DONE || finish_changes != 1 || memory_commit_tx(m) != 0) {
            memory_rollback(m);
            return processed > 0 ? processed : -1;
        }
        processed++;
    }
    return processed;
}

int cbm_memory_materialize_pending(cbm_memory_t *m) {
    return memory_recover_outbox(m);
}

static const char *media_extension(const char *media_type) {
    if (!media_type) {
        return "txt";
    }
    if (strstr(media_type, "markdown")) {
        return "md";
    }
    if (strstr(media_type, "json")) {
        return "json";
    }
    if (strstr(media_type, "yaml") || strstr(media_type, "yml")) {
        return "yaml";
    }
    if (strstr(media_type, "html")) {
        return "html";
    }
    return strncmp(media_type, "text/", 5) == 0 ? "txt" : "bin";
}

static char *ingest_result_json(cbm_memory_t *m, const char *source_id, const char *hash,
                                const char *object_relpath, const char *candidate_query,
                                bool deduplicated, int64_t epoch) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_bool(doc, root, "ok", true);
    yyjson_mut_obj_add_strcpy(doc, root, "source_id", source_id);
    yyjson_mut_obj_add_strcpy(doc, root, "content_hash", hash);
    yyjson_mut_obj_add_strcpy(doc, root, "object_path", object_relpath);
    yyjson_mut_obj_add_bool(doc, root, "deduplicated", deduplicated);
    yyjson_mut_obj_add_int(doc, root, "snapshot_epoch", epoch);
    yyjson_mut_val *pages = yyjson_mut_arr(doc);
    yyjson_mut_val *claims = yyjson_mut_arr(doc);
    char *fts = fts_expression(candidate_query);
    sqlite3_stmt *stmt = NULL;
    if (fts &&
        memory_prepare(m,
                       "SELECT d.entity_kind,d.entity_id,d.title,-bm25(memory_fts)"
                       " FROM memory_fts JOIN memory_documents d ON d.doc_id=memory_fts.rowid"
                       " WHERE memory_fts MATCH ? AND d.entity_kind IN ('page','claim')"
                       " ORDER BY bm25(memory_fts) LIMIT 5;",
                       &stmt) == 0) {
        bind_text_or_null(stmt, 1, fts);
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            const char *kind = (const char *)sqlite3_column_text(stmt, 0);
            yyjson_mut_obj_add_strcpy(doc, item, "id", (const char *)sqlite3_column_text(stmt, 1));
            yyjson_mut_obj_add_strcpy(doc, item, "title",
                                      (const char *)sqlite3_column_text(stmt, 2));
            yyjson_mut_obj_add_real(doc, item, "score", sqlite3_column_double(stmt, 3));
            yyjson_mut_arr_add_val(kind && strcmp(kind, "page") == 0 ? pages : claims, item);
        }
        sqlite3_finalize(stmt);
    }
    free(fts);
    yyjson_mut_obj_add_val(doc, root, "related_pages", pages);
    yyjson_mut_obj_add_val(doc, root, "related_claims", claims);
    char *out = json_write_mut(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static char *memory_source_existing(cbm_memory_t *m, const char *hash) {
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m,
                       "SELECT source_id,object_relpath,title,origin FROM memory_sources"
                       " WHERE content_hash=?;",
                       &stmt) != 0) {
        return NULL;
    }
    bind_text_or_null(stmt, 1, hash);
    char *out = NULL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *source_id = (const char *)sqlite3_column_text(stmt, 0);
        const char *relpath = (const char *)sqlite3_column_text(stmt, 1);
        const char *title = (const char *)sqlite3_column_text(stmt, 2);
        const char *origin = (const char *)sqlite3_column_text(stmt, 3);
        out = ingest_result_json(m, source_id, hash, relpath, title && title[0] ? title : origin,
                                 true, cbm_memory_snapshot_epoch(m));
    }
    sqlite3_finalize(stmt);
    return out;
}

char *cbm_memory_ingest_json(cbm_memory_t *m, const char *args_json) {
    if (!m) {
        return json_error("memory_unavailable", "memory handle is null");
    }
    yyjson_val *root = NULL;
    yyjson_doc *doc = parse_object_args(args_json, &root);
    if (!doc) {
        return json_error("invalid_arguments", "arguments must be a JSON object");
    }
    const char *content = json_str(root, "content");
    const char *path = json_str(root, "path");
    if ((!content && !path) || (content && path)) {
        yyjson_doc_free(doc);
        return json_error("invalid_source", "provide exactly one of content or path");
    }

    unsigned char *owned_bytes = NULL;
    const unsigned char *bytes = NULL;
    size_t len = 0;
    if (path) {
        if (read_file_bytes(path, &owned_bytes, &len) != 0) {
            yyjson_doc_free(doc);
            return json_error("source_read_failed",
                              "cannot read source path or source is too large");
        }
        bytes = owned_bytes;
    } else {
        bytes = (const unsigned char *)content;
        yyjson_val *content_val = yyjson_obj_get(root, "content");
        len = yyjson_get_len(content_val);
        if (len > MEM_MAX_SOURCE_BYTES) {
            yyjson_doc_free(doc);
            return json_error("source_too_large", "source exceeds 16 MiB");
        }
    }

    char hash[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(bytes, len, hash);
    char *existing = memory_source_existing(m, hash);
    if (existing) {
        free(owned_bytes);
        yyjson_doc_free(doc);
        return existing;
    }

    const char *media_type = json_str(root, "media_type");
    if (!media_type) {
        media_type = "text/plain";
    }
    const char *extension = media_extension(media_type);
    char relpath[MEM_PATH_MAX];
    int rn = snprintf(relpath, sizeof(relpath), "raw/objects/%.2s/%s.%s", hash, hash, extension);
    char prefix_dir[MEM_PATH_MAX];
    int pn = snprintf(prefix_dir, sizeof(prefix_dir), "%s/%.2s", m->raw_objects_dir, hash);
    char final_path[MEM_PATH_MAX];
    int fn = snprintf(final_path, sizeof(final_path), "%s/%s.%s", prefix_dir, hash, extension);
    if (rn < 0 || rn >= (int)sizeof(relpath) || pn < 0 || pn >= (int)sizeof(prefix_dir) || fn < 0 ||
        fn >= (int)sizeof(final_path) || !cbm_mkdir_p(prefix_dir, 0700) ||
        write_raw_object(m, final_path, bytes, len, hash) != 0) {
        free(owned_bytes);
        yyjson_doc_free(doc);
        return json_error("raw_store_failed", "failed to persist immutable source object");
    }

    char source_id[MEM_ID_MAX];
    (void)snprintf(source_id, sizeof(source_id), "src_%s", hash);
    const char *title = json_str(root, "title");
    const char *origin = json_str(root, "origin");
    const char *publisher = json_str(root, "publisher");
    const char *published_at = json_str(root, "published_at");
    const char *revision_of = json_str(root, "revision_of");
    if (revision_of && !safe_entity_id(revision_of)) {
        free(owned_bytes);
        yyjson_doc_free(doc);
        return json_error("invalid_source", "revision_of is not a safe memory source id");
    }
    char now[MEM_TIME_MAX];
    iso_now(now);
    const char *retrieved_at = json_str(root, "retrieved_at");
    if (!retrieved_at) {
        retrieved_at = now;
    }
    char *metadata = json_field_serialized(root, "metadata", "{}");
    if (!metadata) {
        free(owned_bytes);
        yyjson_doc_free(doc);
        return NULL;
    }

    if (memory_begin(m) != 0) {
        free(metadata);
        free(owned_bytes);
        yyjson_doc_free(doc);
        return json_error("database_busy", m->error);
    }
    int64_t epoch = memory_bump_epoch(m, now);
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT OR IGNORE INTO "
        "memory_sources(source_id,content_hash,object_relpath,title,origin,media_type,publisher,"
        "published_at,retrieved_at,metadata_json,revision_of,byte_size,created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?);";
    bool inserted = false;
    if (epoch >= 0 && memory_prepare(m, sql, &stmt) == 0) {
        bind_text_or_null(stmt, 1, source_id);
        bind_text_or_null(stmt, 2, hash);
        bind_text_or_null(stmt, 3, relpath);
        bind_text_or_null(stmt, 4, title ? title : (origin ? origin : source_id));
        bind_text_or_null(stmt, 5, origin ? origin : "");
        bind_text_or_null(stmt, 6, media_type);
        bind_text_or_null(stmt, 7, publisher ? publisher : "");
        bind_text_or_null(stmt, 8, published_at);
        bind_text_or_null(stmt, 9, retrieved_at);
        bind_text_or_null(stmt, 10, metadata);
        bind_text_or_null(stmt, 11, revision_of);
        sqlite3_bind_int64(stmt, 12, (sqlite3_int64)len);
        bind_text_or_null(stmt, 13, now);
        int step_rc = sqlite3_step(stmt);
        inserted = step_rc == SQLITE_DONE && sqlite3_changes(m->db) == 1;
        sqlite3_finalize(stmt);
        stmt = NULL;
    }
    char *props = make_node_properties("source_id", source_id, "immutable", 1, epoch);
    int64_t node_id =
        inserted && props
            ? memory_graph_node(m, "source", "MemorySource", source_id,
                                title ? title : (origin ? origin : source_id), relpath, props)
            : -1;
    bool index_text = memchr(bytes, 0, len) == NULL && strcmp(extension, "bin") != 0;
    int doc_rc = node_id > 0
                     ? memory_document_upsert(m, "source", source_id,
                                              title ? title : (origin ? origin : source_id), origin,
                                              index_text ? (const char *)bytes : "", metadata)
                     : -1;
    int lineage_rc = 0;
    if (revision_of && inserted && node_id > 0) {
        lineage_rc = memory_relation_add(m, "source", source_id, "source", revision_of,
                                         "REVISION_OF", "", "{}", epoch, now);
        if (lineage_rc == 0) {
            lineage_rc = dirty_source_dependents(m, revision_of, source_id, epoch, now);
        }
    }
    char activity_id[MEM_ID_MAX];
    int activity_rc = memory_new_id(m, "act", activity_id);
    if (activity_rc == 0) {
        const char *asql = "INSERT INTO "
                           "memory_activities(activity_id,operation_id,agent_id,session_id,action,"
                           "base_epoch,committed_epoch,user_approved,details_json,created_at)"
                           " VALUES(?,?, '', '', 'ingest', ?, ?, 1, '{}', ?);";
        if (memory_prepare(m, asql, &stmt) == 0) {
            char operation_id[MEM_ID_MAX + 16];
            (void)snprintf(operation_id, sizeof(operation_id), "ingest:%s", hash);
            bind_text_or_null(stmt, 1, activity_id);
            bind_text_or_null(stmt, 2, operation_id);
            sqlite3_bind_int64(stmt, 3, epoch > 0 ? epoch - 1 : 0);
            sqlite3_bind_int64(stmt, 4, epoch);
            bind_text_or_null(stmt, 5, now);
            activity_rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
            sqlite3_finalize(stmt);
        } else {
            activity_rc = -1;
        }
    }
    free(props);
    free(metadata);

    if (!inserted || epoch < 0 || node_id <= 0 || doc_rc != 0 || lineage_rc != 0 ||
        activity_rc != 0 || memory_rebuild_projection_internal(m, true) != 0 ||
        memory_commit_tx(m) != 0) {
        memory_rollback(m);
        char *race = memory_source_existing(m, hash);
        free(owned_bytes);
        yyjson_doc_free(doc);
        return race ? race : json_error("ingest_failed", m->error);
    }

    char *out = ingest_result_json(m, source_id, hash, relpath, title && title[0] ? title : origin,
                                   false, epoch);
    free(owned_bytes);
    yyjson_doc_free(doc);
    return out;
}

/* ── Proposals ────────────────────────────────────────────────── */

static bool operation_type_supported(const char *type) {
    return type && (strcmp(type, "upsert_page") == 0 || strcmp(type, "add_claim") == 0 ||
                    strcmp(type, "update_claim_status") == 0 || strcmp(type, "add_decision") == 0 ||
                    strcmp(type, "add_experience") == 0 || strcmp(type, "add_preference") == 0 ||
                    strcmp(type, "link") == 0 || strcmp(type, "add_code_ref") == 0);
}

static const char *validate_operations(yyjson_val *operations) {
    if (!operations || !yyjson_is_arr(operations) || yyjson_arr_size(operations) == 0) {
        return "operations must be a non-empty array";
    }
    size_t index;
    size_t max;
    yyjson_val *op;
    yyjson_arr_foreach(operations, index, max, op) {
        if (!yyjson_is_obj(op)) {
            return "every operation must be an object";
        }
        const char *type = json_str(op, "type");
        if (!operation_type_supported(type)) {
            return "unsupported operation type";
        }
        if (strcmp(type, "upsert_page") == 0) {
            const char *slug = json_str(op, "slug");
            if (!safe_path_component(slug) || !json_str(op, "markdown")) {
                return "upsert_page requires a safe slug and markdown";
            }
        } else if (strcmp(type, "add_claim") == 0) {
            if (!json_str(op, "subject") || !json_str(op, "predicate") || !json_str(op, "object")) {
                return "add_claim requires subject, predicate, and object";
            }
        } else if (strcmp(type, "update_claim_status") == 0) {
            if (!json_str(op, "claim_id") || !json_str(op, "status")) {
                return "update_claim_status requires claim_id and status";
            }
        } else if (strcmp(type, "add_decision") == 0) {
            if (!json_str(op, "chosen_option")) {
                return "add_decision requires chosen_option";
            }
        } else if (strcmp(type, "add_experience") == 0) {
            if (!json_str(op, "observation") || !yyjson_obj_get(op, "context")) {
                return "add_experience requires observation and explicit context";
            }
        } else if (strcmp(type, "add_preference") == 0) {
            if (!json_str(op, "value")) {
                return "add_preference requires value";
            }
        } else if (strcmp(type, "link") == 0) {
            if (!json_str(op, "source_kind") || !json_str(op, "source_id") ||
                !json_str(op, "target_kind") || !json_str(op, "target_id") ||
                !json_str(op, "relation_type")) {
                return "link requires source, target, and relation_type";
            }
        } else if (strcmp(type, "add_code_ref") == 0) {
            if (!json_str(op, "project")) {
                return "add_code_ref requires project";
            }
        }
    }
    return NULL;
}

char *cbm_memory_propose_json(cbm_memory_t *m, const char *args_json) {
    if (!m) {
        return json_error("memory_unavailable", "memory handle is null");
    }
    yyjson_val *root = NULL;
    yyjson_doc *doc = parse_object_args(args_json, &root);
    if (!doc) {
        return json_error("invalid_arguments", "arguments must be a JSON object");
    }
    yyjson_val *operations = yyjson_obj_get(root, "operations");
    const char *validation_error = validate_operations(operations);
    if (validation_error) {
        yyjson_doc_free(doc);
        return json_error("invalid_operations", validation_error);
    }

    char generated_id[MEM_ID_MAX];
    const char *proposal_id = json_str(root, "proposal_id");
    if (!proposal_id) {
        if (memory_new_id(m, "prop", generated_id) != 0) {
            yyjson_doc_free(doc);
            return NULL;
        }
        proposal_id = generated_id;
    } else if (!safe_entity_id(proposal_id)) {
        yyjson_doc_free(doc);
        return json_error("invalid_proposal_id", "proposal_id contains unsafe characters");
    }
    const char *agent_id = json_str(root, "agent_id");
    const char *session_id = json_str(root, "session_id");
    const char *reason = json_str(root, "reason");
    int64_t current_epoch = cbm_memory_snapshot_epoch(m);
    int64_t base_epoch = json_int(root, "base_epoch", current_epoch);
    if (base_epoch < 0 || base_epoch > current_epoch) {
        yyjson_doc_free(doc);
        return json_error("invalid_base_epoch", "base_epoch is outside the known memory history");
    }
    char *operations_json = yyjson_val_write(operations, YYJSON_WRITE_ALLOW_INVALID_UNICODE, NULL);
    char *expected_json = json_field_serialized(root, "expected_revisions", "{}");
    if (!operations_json || !expected_json) {
        free(operations_json);
        free(expected_json);
        yyjson_doc_free(doc);
        return NULL;
    }
    char now[MEM_TIME_MAX];
    iso_now(now);
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO memory_proposals(proposal_id,agent_id,session_id,base_epoch,status,"
        " operations_json,expected_revisions_json,reason,created_at)"
        " VALUES(?,?,?,?,'pending',?,?,?,?);";
    if (memory_prepare(m, sql, &stmt) != 0) {
        free(operations_json);
        free(expected_json);
        yyjson_doc_free(doc);
        return json_error("proposal_failed", m->error);
    }
    bind_text_or_null(stmt, 1, proposal_id);
    bind_text_or_null(stmt, 2, agent_id ? agent_id : "");
    bind_text_or_null(stmt, 3, session_id ? session_id : "");
    sqlite3_bind_int64(stmt, 4, base_epoch);
    bind_text_or_null(stmt, 5, operations_json);
    bind_text_or_null(stmt, 6, expected_json);
    bind_text_or_null(stmt, 7, reason ? reason : "");
    bind_text_or_null(stmt, 8, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    free(operations_json);
    free(expected_json);
    if (rc != SQLITE_DONE) {
        bool duplicate = sqlite3_extended_errcode(m->db) == SQLITE_CONSTRAINT_PRIMARYKEY ||
                         sqlite3_extended_errcode(m->db) == SQLITE_CONSTRAINT_UNIQUE;
        yyjson_doc_free(doc);
        return json_error(duplicate ? "proposal_exists" : "proposal_failed",
                          duplicate ? "proposal_id already exists" : sqlite3_errmsg(m->db));
    }

    yyjson_mut_doc *out_doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *out_root = yyjson_mut_obj(out_doc);
    yyjson_mut_doc_set_root(out_doc, out_root);
    yyjson_mut_obj_add_bool(out_doc, out_root, "ok", true);
    yyjson_mut_obj_add_strcpy(out_doc, out_root, "proposal_id", proposal_id);
    yyjson_mut_obj_add_strcpy(out_doc, out_root, "status", "pending");
    yyjson_mut_obj_add_int(out_doc, out_root, "base_epoch", base_epoch);
    yyjson_mut_obj_add_int(out_doc, out_root, "snapshot_epoch", current_epoch);
    char *out = json_write_mut(out_doc);
    yyjson_mut_doc_free(out_doc);
    yyjson_doc_free(doc);
    return out;
}

/* ── Commit operation helpers ─────────────────────────────────── */

static bool valid_status(const char *status) {
    return status && (strcmp(status, "unverified") == 0 || strcmp(status, "active") == 0 ||
                      strcmp(status, "stale") == 0 || strcmp(status, "contested") == 0 ||
                      strcmp(status, "superseded") == 0 || strcmp(status, "retracted") == 0);
}

static bool valid_claim_kind(const char *kind) {
    return kind && (strcmp(kind, "fact") == 0 || strcmp(kind, "inference") == 0 ||
                    strcmp(kind, "hypothesis") == 0 || strcmp(kind, "recommendation") == 0);
}

static bool valid_ref_kind(const char *kind) {
    return kind && (strcmp(kind, "symbol") == 0 || strcmp(kind, "file") == 0 ||
                    strcmp(kind, "api_schema") == 0 || strcmp(kind, "dependency") == 0);
}

static bool valid_relation_type(const char *type) {
    if (!type || !type[0] || strlen(type) > 64) {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)type; *p; p++) {
        if (!((*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') || *p == '_')) {
            return false;
        }
    }
    return true;
}

static const char *kind_label(const char *kind) {
    if (strcmp(kind, "source") == 0) {
        return "MemorySource";
    }
    if (strcmp(kind, "page") == 0) {
        return "WikiPage";
    }
    if (strcmp(kind, "revision") == 0) {
        return "WikiRevision";
    }
    if (strcmp(kind, "claim") == 0) {
        return "Claim";
    }
    if (strcmp(kind, "decision") == 0) {
        return "Decision";
    }
    if (strcmp(kind, "experience") == 0) {
        return "Experience";
    }
    if (strcmp(kind, "preference") == 0) {
        return "Preference";
    }
    if (strcmp(kind, "code_ref") == 0) {
        return "CodeRef";
    }
    if (strcmp(kind, "activity") == 0) {
        return "Activity";
    }
    return NULL;
}

static int64_t graph_node_id(cbm_memory_t *m, const char *kind, const char *id) {
    char qn[MEM_PATH_MAX];
    if (!kind_label(kind) ||
        snprintf(qn, sizeof(qn), "memory:%s:%s", kind, id) >= (int)sizeof(qn)) {
        return -1;
    }
    /* Do not use the store's cached find-by-QN statement here: successful
     * cached readers intentionally remain positioned on SQLITE_ROW until the
     * next lookup, which would pin a WAL snapshot across API calls and make a
     * later BEGIN IMMEDIATE fail with SQLITE_BUSY_SNAPSHOT after another agent
     * commits. This short-lived lookup is always finalized. */
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, "SELECT id FROM nodes WHERE project=? AND qualified_name=?;", &stmt) !=
        0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, CBM_MEMORY_PROJECT);
    bind_text_or_null(stmt, 2, qn);
    int64_t node_id = sqlite3_step(stmt) == SQLITE_ROW ? sqlite3_column_int64(stmt, 0) : -1;
    sqlite3_finalize(stmt);
    return node_id;
}

static int memory_graph_edge(cbm_memory_t *m, const char *source_kind, const char *source_id,
                             const char *target_kind, const char *target_id, const char *type,
                             const char *properties_json) {
    int64_t source_node = graph_node_id(m, source_kind, source_id);
    int64_t target_node = graph_node_id(m, target_kind, target_id);
    if (source_node <= 0 || target_node <= 0 || !valid_relation_type(type)) {
        return -1;
    }
    cbm_edge_t edge = {.project = CBM_MEMORY_PROJECT,
                       .source_id = source_node,
                       .target_id = target_node,
                       .type = type,
                       .properties_json = properties_json ? properties_json : "{}"};
    return cbm_store_insert_edge(m->graph, &edge) > 0 ? 0 : -1;
}

static int memory_relation_add(cbm_memory_t *m, const char *source_kind, const char *source_id,
                               const char *target_kind, const char *target_id, const char *type,
                               const char *relation_key, const char *properties_json, int64_t epoch,
                               const char *now) {
    if (!kind_label(source_kind) || !kind_label(target_kind) || !safe_entity_id(source_id) ||
        !safe_entity_id(target_id) || !valid_relation_type(type)) {
        return -1;
    }
    char relation_id[MEM_ID_MAX];
    if (memory_new_id(m, "rel", relation_id) != 0) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT OR IGNORE INTO memory_relations(relation_id,source_kind,source_id,target_kind,"
        " target_id,type,relation_key,properties_json,recorded_from,created_epoch)"
        " VALUES(?,?,?,?,?,?,?,?,?,?);";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, relation_id);
    bind_text_or_null(stmt, 2, source_kind);
    bind_text_or_null(stmt, 3, source_id);
    bind_text_or_null(stmt, 4, target_kind);
    bind_text_or_null(stmt, 5, target_id);
    bind_text_or_null(stmt, 6, type);
    bind_text_or_null(stmt, 7, relation_key ? relation_key : "");
    bind_text_or_null(stmt, 8, properties_json ? properties_json : "{}");
    bind_text_or_null(stmt, 9, now);
    sqlite3_bind_int64(stmt, 10, epoch);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return -1;
    }
    return memory_graph_edge(m, source_kind, source_id, target_kind, target_id, type,
                             properties_json);
}

static int64_t expected_revision(yyjson_val *op, yyjson_val *expected, const char *entity_id) {
    int64_t direct = json_int(op, "expected_revision", -1);
    if (direct >= 0) {
        return direct;
    }
    if (expected && yyjson_is_obj(expected) && entity_id) {
        yyjson_val *value = yyjson_obj_get(expected, entity_id);
        if (value && yyjson_is_int(value)) {
            return yyjson_get_sint(value);
        }
    }
    return -1;
}

static void result_add_entity(yyjson_mut_doc *doc, yyjson_mut_val *results, const char *type,
                              const char *id, int revision) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "type", type);
    yyjson_mut_obj_add_strcpy(doc, item, "id", id);
    if (revision >= 0) {
        yyjson_mut_obj_add_int(doc, item, "revision", revision);
    }
    yyjson_mut_arr_add_val(results, item);
}

static int apply_upsert_page(cbm_memory_t *m, yyjson_val *op, yyjson_val *expected,
                             const char *agent_id, const char *session_id, int64_t epoch,
                             const char *now, yyjson_mut_doc *result_doc, yyjson_mut_val *results,
                             char **conflict_out) {
    const char *slug = json_str(op, "slug");
    const char *title = json_str(op, "title");
    const char *page_kind = json_str(op, "page_kind");
    const char *status = json_str(op, "status");
    const char *markdown = json_str(op, "markdown");
    const char *requested_id = json_str(op, "page_id");
    bool page_kind_provided = page_kind != NULL;
    if (requested_id && !safe_entity_id(requested_id)) {
        return -1;
    }
    if (!safe_path_component(slug) || !markdown) {
        return -1;
    }
    if (page_kind && !safe_path_component(page_kind)) {
        return -1;
    }
    if ((title && strlen(title) >= MEM_PATH_MAX) || (status && strlen(status) >= 64)) {
        return -1;
    }

    sqlite3_stmt *stmt = NULL;
    const char *lookup_sql =
        requested_id
            ? "SELECT page_id,current_revision,slug,title,page_kind,status FROM memory_pages"
              " WHERE page_id=?;"
            : "SELECT page_id,current_revision,slug,title,page_kind,status FROM memory_pages"
              " WHERE slug=?;";
    if (memory_prepare(m, lookup_sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, requested_id ? requested_id : slug);
    bool exists = sqlite3_step(stmt) == SQLITE_ROW;
    char page_id[MEM_ID_MAX];
    char existing_slug[256] = "";
    char existing_title[MEM_PATH_MAX] = "";
    char existing_page_kind[256] = "";
    char existing_status[64] = "";
    int current_revision = 0;
    if (exists) {
        const char *id_col = (const char *)sqlite3_column_text(stmt, 0);
        (void)snprintf(page_id, sizeof(page_id), "%s", id_col ? id_col : "");
        current_revision = sqlite3_column_int(stmt, 1);
        if (snprintf(existing_slug, sizeof(existing_slug), "%s",
                     sqlite3_column_text(stmt, 2) ? (const char *)sqlite3_column_text(stmt, 2)
                                                  : "") >= (int)sizeof(existing_slug) ||
            snprintf(existing_title, sizeof(existing_title), "%s",
                     sqlite3_column_text(stmt, 3) ? (const char *)sqlite3_column_text(stmt, 3)
                                                  : "") >= (int)sizeof(existing_title) ||
            snprintf(existing_page_kind, sizeof(existing_page_kind), "%s",
                     sqlite3_column_text(stmt, 4) ? (const char *)sqlite3_column_text(stmt, 4)
                                                  : "") >= (int)sizeof(existing_page_kind) ||
            snprintf(existing_status, sizeof(existing_status), "%s",
                     sqlite3_column_text(stmt, 5) ? (const char *)sqlite3_column_text(stmt, 5)
                                                  : "") >= (int)sizeof(existing_status)) {
            sqlite3_finalize(stmt);
            return -1;
        }
    }
    sqlite3_finalize(stmt);
    if (exists) {
        if (strcmp(slug, existing_slug) != 0 ||
            (page_kind_provided && strcmp(page_kind, existing_page_kind) != 0)) {
            *conflict_out = mem_strdup("page slug and page_kind are immutable");
            return 1;
        }
        if (!title) {
            title = existing_title;
        }
        if (!page_kind) {
            page_kind = existing_page_kind;
        }
        if (!status) {
            status = existing_status;
        }
    } else {
        if (!title) {
            title = slug;
        }
        if (!page_kind) {
            page_kind = "concept";
        }
        if (!status) {
            status = "active";
        }
    }
    if (!exists) {
        if (requested_id) {
            (void)snprintf(page_id, sizeof(page_id), "%s", requested_id);
        } else if (memory_new_id(m, "page", page_id) != 0) {
            return -1;
        }
    }
    int64_t expected_value = expected_revision(op, expected, page_id);
    if (exists && expected_value < 0) {
        *conflict_out = mem_strdup("expected_revision is required when updating a page");
        return 1;
    }
    if ((exists && expected_value != current_revision) || (!exists && expected_value > 0)) {
        char msg[CBM_SZ_256];
        (void)snprintf(msg, sizeof(msg),
                       "page %s revision conflict: expected %" PRId64 ", current %d", page_id,
                       expected_value, current_revision);
        *conflict_out = mem_strdup(msg);
        return 1;
    }

    if (!exists) {
        if (memory_prepare(m,
                           "INSERT INTO memory_pages(page_id,slug,title,page_kind,status,"
                           " current_revision,created_at,updated_at) VALUES(?,?,?,?,?,0,?,?);",
                           &stmt) != 0) {
            return -1;
        }
        bind_text_or_null(stmt, 1, page_id);
        bind_text_or_null(stmt, 2, slug);
        bind_text_or_null(stmt, 3, title);
        bind_text_or_null(stmt, 4, page_kind);
        bind_text_or_null(stmt, 5, status);
        bind_text_or_null(stmt, 6, now);
        bind_text_or_null(stmt, 7, now);
        int rc = sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        if (rc != SQLITE_DONE) {
            return -1;
        }
    }

    int revision = current_revision + 1;
    char revision_id[MEM_ID_MAX];
    if (memory_new_id(m, "rev", revision_id) != 0) {
        return -1;
    }
    char body_hash[CBM_SHA256_HEX_LEN + 1];
    cbm_sha256_hex(markdown, strlen(markdown), body_hash);
    char *section_hashes = json_field_serialized(op, "section_hashes", "{}");
    if (!section_hashes) {
        return -1;
    }
    if (memory_prepare(m,
                       "INSERT INTO memory_revisions(revision_id,page_id,revision,base_revision,"
                       " body_hash,markdown,section_hashes_json,author_agent,author_session,"
                       " created_epoch,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?);",
                       &stmt) != 0) {
        free(section_hashes);
        return -1;
    }
    bind_text_or_null(stmt, 1, revision_id);
    bind_text_or_null(stmt, 2, page_id);
    sqlite3_bind_int(stmt, 3, revision);
    sqlite3_bind_int(stmt, 4, current_revision);
    bind_text_or_null(stmt, 5, body_hash);
    bind_text_or_null(stmt, 6, markdown);
    bind_text_or_null(stmt, 7, section_hashes);
    bind_text_or_null(stmt, 8, agent_id);
    bind_text_or_null(stmt, 9, session_id);
    sqlite3_bind_int64(stmt, 10, epoch);
    bind_text_or_null(stmt, 11, now);
    int revision_rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    free(section_hashes);
    if (revision_rc != SQLITE_DONE) {
        return -1;
    }
    if (memory_prepare(m,
                       "UPDATE memory_pages SET slug=?,title=?,page_kind=?,status=?,"
                       " current_revision=?,current_revision_id=?,updated_at=?"
                       " WHERE page_id=? AND current_revision=?;",
                       &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, slug);
    bind_text_or_null(stmt, 2, title);
    bind_text_or_null(stmt, 3, page_kind);
    bind_text_or_null(stmt, 4, status);
    sqlite3_bind_int(stmt, 5, revision);
    bind_text_or_null(stmt, 6, revision_id);
    bind_text_or_null(stmt, 7, now);
    bind_text_or_null(stmt, 8, page_id);
    sqlite3_bind_int(stmt, 9, current_revision);
    int update_rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (update_rc != SQLITE_DONE || sqlite3_changes(m->db) != 1) {
        *conflict_out = mem_strdup("page changed while committing");
        return 1;
    }
    char *page_props = make_node_properties("page_id", page_id, status, revision, epoch);
    char *rev_props =
        make_node_properties("revision_id", revision_id, "committed", revision, epoch);
    int64_t page_node =
        page_props ? memory_graph_node(m, "page", "WikiPage", page_id, title, "", page_props) : -1;
    int64_t rev_node = rev_props ? memory_graph_node(m, "revision", "WikiRevision", revision_id,
                                                     revision_id, "", rev_props)
                                 : -1;
    free(page_props);
    free(rev_props);
    if (page_node <= 0 || rev_node <= 0 ||
        memory_document_upsert(m, "page", page_id, title, "", markdown, page_kind) != 0) {
        return -1;
    }
    char relation_key[32];
    (void)snprintf(relation_key, sizeof(relation_key), "%d", revision);
    if (memory_relation_add(m, "page", page_id, "revision", revision_id, "HAS_REVISION",
                            relation_key, "{}", epoch, now) != 0) {
        return -1;
    }
    char payload[CBM_SZ_512];
    int payload_len = snprintf(payload, sizeof(payload), "{\"page_id\":\"%s\",\"revision\":%d}",
                               page_id, revision);
    if (payload_len < 0 || payload_len >= (int)sizeof(payload) ||
        memory_prepare(
            m,
            "INSERT INTO memory_outbox(kind,aggregate_id,revision,payload_json,created_at)"
            " VALUES('wiki_materialize',?,?,?,?);",
            &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, page_id);
    sqlite3_bind_int(stmt, 2, revision);
    bind_text_or_null(stmt, 3, payload);
    bind_text_or_null(stmt, 4, now);
    int outbox_rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (outbox_rc != SQLITE_DONE) {
        return -1;
    }
    result_add_entity(result_doc, results, "page", page_id, revision);
    return 0;
}

static int apply_add_claim(cbm_memory_t *m, yyjson_val *op, int64_t epoch, const char *now,
                           yyjson_mut_doc *result_doc, yyjson_mut_val *results) {
    const char *kind = json_str(op, "claim_kind");
    const char *status = json_str(op, "status");
    const char *subject = json_str(op, "subject");
    const char *predicate = json_str(op, "predicate");
    const char *object = json_str(op, "object");
    if (!kind) {
        kind = "fact";
    }
    if (!status) {
        status = "unverified";
    }
    if (!valid_claim_kind(kind) || !valid_status(status) || !subject || !predicate || !object) {
        return -1;
    }
    char claim_id[MEM_ID_MAX];
    const char *requested_id = json_str(op, "claim_id");
    if (requested_id && !safe_entity_id(requested_id)) {
        return -1;
    }
    if (requested_id) {
        (void)snprintf(claim_id, sizeof(claim_id), "%s", requested_id);
    } else if (memory_new_id(m, "claim", claim_id) != 0) {
        return -1;
    }
    char *scope = json_field_serialized(op, "scope", "{}");
    if (!scope) {
        return -1;
    }
    const char *recorded_from = json_str(op, "recorded_from");
    if (!recorded_from) {
        recorded_from = now;
    }
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO memory_claims(claim_id,claim_kind,subject,predicate,object_text,scope_json,"
        " status,valid_from,valid_to,recorded_from,recorded_to,observed_at,review_after,volatility,"
        " revision,created_epoch,updated_epoch,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?);";
    if (memory_prepare(m, sql, &stmt) != 0) {
        free(scope);
        return -1;
    }
    bind_text_or_null(stmt, 1, claim_id);
    bind_text_or_null(stmt, 2, kind);
    bind_text_or_null(stmt, 3, subject);
    bind_text_or_null(stmt, 4, predicate);
    bind_text_or_null(stmt, 5, object);
    bind_text_or_null(stmt, 6, scope);
    bind_text_or_null(stmt, 7, status);
    bind_text_or_null(stmt, 8, json_str(op, "valid_from"));
    bind_text_or_null(stmt, 9, json_str(op, "valid_to"));
    bind_text_or_null(stmt, 10, recorded_from);
    bind_text_or_null(stmt, 11, json_str(op, "recorded_to"));
    bind_text_or_null(stmt, 12, json_str(op, "observed_at"));
    bind_text_or_null(stmt, 13, json_str(op, "review_after"));
    bind_text_or_null(stmt, 14, json_str(op, "volatility") ? json_str(op, "volatility") : "normal");
    sqlite3_bind_int64(stmt, 15, epoch);
    sqlite3_bind_int64(stmt, 16, epoch);
    bind_text_or_null(stmt, 17, now);
    bind_text_or_null(stmt, 18, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        free(scope);
        return -1;
    }
    char *props = make_node_properties("claim_id", claim_id, status, 1, epoch);
    int64_t node_id =
        props ? memory_graph_node(m, "claim", "Claim", claim_id, subject, "", props) : -1;
    free(props);
    char body[CBM_SZ_4K];
    int body_len = snprintf(body, sizeof(body), "%s %s %s", subject, predicate, object);
    if (node_id <= 0 || body_len < 0 || body_len >= (int)sizeof(body) ||
        memory_document_upsert(m, "claim", claim_id, subject, predicate, body, scope) != 0) {
        free(scope);
        return -1;
    }
    free(scope);

    const char *page_id = json_str(op, "page_id");
    if (page_id && memory_relation_add(m, "page", page_id, "claim", claim_id, "ASSERTS", "", "{}",
                                       epoch, now) != 0) {
        return -1;
    }
    yyjson_val *sources = yyjson_obj_get(op, "source_ids");
    if (sources && yyjson_is_arr(sources)) {
        size_t index;
        size_t max;
        yyjson_val *source;
        yyjson_arr_foreach(sources, index, max, source) {
            if (!yyjson_is_str(source)) {
                return -1;
            }
            const char *source_id = yyjson_get_str(source);
            char relation_key[32];
            (void)snprintf(relation_key, sizeof(relation_key), "%zu", index);
            if (memory_relation_add(m, "claim", claim_id, "source", source_id, "SUPPORTED_BY",
                                    relation_key, "{}", epoch, now) != 0) {
                return -1;
            }
        }
    }
    result_add_entity(result_doc, results, "claim", claim_id, 1);
    return 0;
}

static int apply_update_claim_status(cbm_memory_t *m, yyjson_val *op, yyjson_val *expected,
                                     int64_t epoch, const char *now, yyjson_mut_doc *result_doc,
                                     yyjson_mut_val *results, char **conflict_out) {
    const char *claim_id = json_str(op, "claim_id");
    const char *status = json_str(op, "status");
    if (!claim_id || !valid_status(status)) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m,
                       "SELECT revision,subject,predicate,object_text,scope_json FROM memory_claims"
                       " WHERE claim_id=? AND recorded_to IS NULL;",
                       &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, claim_id);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        *conflict_out = mem_strdup("claim does not exist or has no current version");
        return 1;
    }
    int current_revision = sqlite3_column_int(stmt, 0);
    char *subject = mem_strdup((const char *)sqlite3_column_text(stmt, 1));
    char *predicate = mem_strdup((const char *)sqlite3_column_text(stmt, 2));
    char *object = mem_strdup((const char *)sqlite3_column_text(stmt, 3));
    char *scope = mem_strdup((const char *)sqlite3_column_text(stmt, 4));
    sqlite3_finalize(stmt);
    if (!subject || !predicate || !object || !scope) {
        free(subject);
        free(predicate);
        free(object);
        free(scope);
        return -1;
    }
    int64_t expected_value = expected_revision(op, expected, claim_id);
    if (expected_value < 0 || expected_value != current_revision) {
        char msg[CBM_SZ_256];
        (void)snprintf(msg, sizeof(msg),
                       "claim %s revision conflict: expected %" PRId64 ", current %d", claim_id,
                       expected_value, current_revision);
        *conflict_out = mem_strdup(msg);
        free(subject);
        free(predicate);
        free(object);
        free(scope);
        return 1;
    }
    const char *history_sql =
        "INSERT INTO memory_claim_revisions(claim_id,revision,claim_kind,subject,predicate,"
        " object_text,scope_json,status,valid_from,valid_to,recorded_from,recorded_to,observed_at,"
        " review_after,volatility,closed_epoch,created_at)"
        " SELECT claim_id,revision,claim_kind,subject,predicate,object_text,scope_json,status,"
        " valid_from,valid_to,recorded_from,?,observed_at,review_after,volatility,?,created_at"
        " FROM memory_claims WHERE claim_id=? AND revision=?;";
    if (memory_prepare(m, history_sql, &stmt) != 0) {
        free(subject);
        free(predicate);
        free(object);
        free(scope);
        return -1;
    }
    bind_text_or_null(stmt, 1, now);
    sqlite3_bind_int64(stmt, 2, epoch);
    bind_text_or_null(stmt, 3, claim_id);
    sqlite3_bind_int(stmt, 4, current_revision);
    int history_rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (history_rc != SQLITE_DONE || sqlite3_changes(m->db) != 1) {
        free(subject);
        free(predicate);
        free(object);
        free(scope);
        return -1;
    }
    if (memory_prepare(m,
                       "UPDATE memory_claims SET status=?,recorded_from=?,recorded_to=NULL,"
                       " revision=revision+1,updated_epoch=?,updated_at=?"
                       " WHERE claim_id=? AND revision=?;",
                       &stmt) != 0) {
        free(subject);
        free(predicate);
        free(object);
        free(scope);
        return -1;
    }
    bind_text_or_null(stmt, 1, status);
    bind_text_or_null(stmt, 2, now);
    sqlite3_bind_int64(stmt, 3, epoch);
    bind_text_or_null(stmt, 4, now);
    bind_text_or_null(stmt, 5, claim_id);
    sqlite3_bind_int(stmt, 6, current_revision);
    int update_rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (update_rc != SQLITE_DONE || sqlite3_changes(m->db) != 1) {
        *conflict_out = mem_strdup("claim changed while committing");
        free(subject);
        free(predicate);
        free(object);
        free(scope);
        return 1;
    }
    int new_revision = current_revision + 1;
    char *props = make_node_properties("claim_id", claim_id, status, new_revision, epoch);
    int64_t node_id =
        props ? memory_graph_node(m, "claim", "Claim", claim_id, subject, "", props) : -1;
    free(props);
    char body[CBM_SZ_4K];
    int body_len = snprintf(body, sizeof(body), "%s %s %s", subject, predicate, object);
    int doc_rc = body_len >= 0 && body_len < (int)sizeof(body)
                     ? memory_document_upsert(m, "claim", claim_id, subject, predicate, body, scope)
                     : -1;
    free(subject);
    free(predicate);
    free(object);
    free(scope);
    if (node_id <= 0 || doc_rc != 0) {
        return -1;
    }
    result_add_entity(result_doc, results, "claim", claim_id, new_revision);
    return 0;
}

static int apply_add_decision(cbm_memory_t *m, yyjson_val *op, int64_t epoch, const char *now,
                              yyjson_mut_doc *result_doc, yyjson_mut_val *results) {
    const char *chosen = json_str(op, "chosen_option");
    if (!chosen) {
        return -1;
    }
    char id[MEM_ID_MAX];
    const char *requested = json_str(op, "decision_id");
    if (requested && !safe_entity_id(requested)) {
        return -1;
    }
    if (requested) {
        (void)snprintf(id, sizeof(id), "%s", requested);
    } else if (memory_new_id(m, "decision", id) != 0) {
        return -1;
    }
    char *alternatives = json_field_serialized(op, "alternatives", "[]");
    char *rejected = json_field_serialized(op, "rejected_because", "[]");
    char *assumptions = json_field_serialized(op, "assumptions", "[]");
    char *constraints = json_field_serialized(op, "constraints", "[]");
    char *applicability = json_field_serialized(op, "applicability", "[]");
    char *invalidation = json_field_serialized(op, "invalidation", "[]");
    char *exit_criteria = json_field_serialized(op, "exit_criteria", "[]");
    if (!alternatives || !rejected || !assumptions || !constraints || !applicability ||
        !invalidation || !exit_criteria) {
        free(alternatives);
        free(rejected);
        free(assumptions);
        free(constraints);
        free(applicability);
        free(invalidation);
        free(exit_criteria);
        return -1;
    }
    const char *title = json_str(op, "title");
    const char *status = json_str(op, "status");
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO memory_decisions(decision_id,title,chosen_option,alternatives_json,"
        " rejected_because_json,assumptions_json,constraints_json,applicability_json,"
        " invalidation_json,expected_outcome,actual_outcome,review_after,exit_criteria_json,"
        " status,revision,created_epoch,updated_epoch,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?);";
    int rc = -1;
    if (memory_prepare(m, sql, &stmt) == 0) {
        bind_text_or_null(stmt, 1, id);
        bind_text_or_null(stmt, 2, title ? title : chosen);
        bind_text_or_null(stmt, 3, chosen);
        bind_text_or_null(stmt, 4, alternatives);
        bind_text_or_null(stmt, 5, rejected);
        bind_text_or_null(stmt, 6, assumptions);
        bind_text_or_null(stmt, 7, constraints);
        bind_text_or_null(stmt, 8, applicability);
        bind_text_or_null(stmt, 9, invalidation);
        bind_text_or_null(stmt, 10,
                          json_str(op, "expected_outcome") ? json_str(op, "expected_outcome") : "");
        bind_text_or_null(stmt, 11,
                          json_str(op, "actual_outcome") ? json_str(op, "actual_outcome") : "");
        bind_text_or_null(stmt, 12, json_str(op, "review_after"));
        bind_text_or_null(stmt, 13, exit_criteria);
        bind_text_or_null(stmt, 14, status ? status : "active");
        sqlite3_bind_int64(stmt, 15, epoch);
        sqlite3_bind_int64(stmt, 16, epoch);
        bind_text_or_null(stmt, 17, now);
        bind_text_or_null(stmt, 18, now);
        rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
        sqlite3_finalize(stmt);
    }
    if (rc == 0) {
        char *props = make_node_properties("decision_id", id, status ? status : "active", 1, epoch);
        int64_t node_id = props ? memory_graph_node(m, "decision", "Decision", id,
                                                    title ? title : chosen, "", props)
                                : -1;
        free(props);
        rc = node_id > 0
                 ? memory_document_upsert(m, "decision", id, title ? title : chosen,
                                          json_str(op, "expected_outcome"), chosen, applicability)
                 : -1;
    }
    free(alternatives);
    free(rejected);
    free(assumptions);
    free(constraints);
    free(applicability);
    free(invalidation);
    free(exit_criteria);
    if (rc != 0) {
        return -1;
    }
    result_add_entity(result_doc, results, "decision", id, 1);
    return 0;
}

static int apply_add_experience(cbm_memory_t *m, yyjson_val *op, int64_t epoch, const char *now,
                                yyjson_mut_doc *result_doc, yyjson_mut_val *results) {
    const char *observation = json_str(op, "observation");
    if (!observation || !yyjson_obj_get(op, "context")) {
        return -1;
    }
    char id[MEM_ID_MAX];
    const char *requested = json_str(op, "experience_id");
    if (requested && !safe_entity_id(requested)) {
        return -1;
    }
    if (requested) {
        (void)snprintf(id, sizeof(id), "%s", requested);
    } else if (memory_new_id(m, "experience", id) != 0) {
        return -1;
    }
    char *context = json_field_serialized(op, "context", "{}");
    char *environment = json_field_serialized(op, "environment", "{}");
    char *limits = json_field_serialized(op, "generalization_limits", "[]");
    char *signals = json_field_serialized(op, "failure_signals", "[]");
    if (!context || !environment || !limits || !signals) {
        free(context);
        free(environment);
        free(limits);
        free(signals);
        return -1;
    }
    const char *title = json_str(op, "title");
    const char *status = json_str(op, "status");
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO memory_experiences(experience_id,title,context_json,environment_json,action,"
        " observation,outcome,sample_size,generalization_limits_json,failure_signals_json,status,"
        " revision,created_epoch,updated_epoch,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?);";
    int rc = -1;
    if (memory_prepare(m, sql, &stmt) == 0) {
        bind_text_or_null(stmt, 1, id);
        bind_text_or_null(stmt, 2, title ? title : observation);
        bind_text_or_null(stmt, 3, context);
        bind_text_or_null(stmt, 4, environment);
        bind_text_or_null(stmt, 5, json_str(op, "action") ? json_str(op, "action") : "");
        bind_text_or_null(stmt, 6, observation);
        bind_text_or_null(stmt, 7, json_str(op, "outcome") ? json_str(op, "outcome") : "");
        int64_t sample_size = json_int(op, "sample_size", -1);
        if (sample_size < 0) {
            sqlite3_bind_null(stmt, 8);
        } else {
            sqlite3_bind_int64(stmt, 8, sample_size);
        }
        bind_text_or_null(stmt, 9, limits);
        bind_text_or_null(stmt, 10, signals);
        bind_text_or_null(stmt, 11, status ? status : "active");
        sqlite3_bind_int64(stmt, 12, epoch);
        sqlite3_bind_int64(stmt, 13, epoch);
        bind_text_or_null(stmt, 14, now);
        bind_text_or_null(stmt, 15, now);
        rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
        sqlite3_finalize(stmt);
    }
    if (rc == 0) {
        char *props =
            make_node_properties("experience_id", id, status ? status : "active", 1, epoch);
        int64_t node_id = props ? memory_graph_node(m, "experience", "Experience", id,
                                                    title ? title : observation, "", props)
                                : -1;
        free(props);
        rc = node_id > 0 ? memory_document_upsert(m, "experience", id, title ? title : observation,
                                                  json_str(op, "outcome"), observation, context)
                         : -1;
    }
    free(context);
    free(environment);
    free(limits);
    free(signals);
    if (rc != 0) {
        return -1;
    }
    result_add_entity(result_doc, results, "experience", id, 1);
    return 0;
}

static int apply_add_preference(cbm_memory_t *m, yyjson_val *op, int64_t epoch, const char *now,
                                yyjson_mut_doc *result_doc, yyjson_mut_val *results) {
    const char *value = json_str(op, "value");
    if (!value) {
        return -1;
    }
    char id[MEM_ID_MAX];
    const char *requested = json_str(op, "preference_id");
    if (requested && !safe_entity_id(requested)) {
        return -1;
    }
    if (requested) {
        (void)snprintf(id, sizeof(id), "%s", requested);
    } else if (memory_new_id(m, "preference", id) != 0) {
        return -1;
    }
    char *scope = json_field_serialized(op, "scope", "{}");
    char *context = json_field_serialized(op, "context", "{}");
    if (!scope || !context) {
        free(scope);
        free(context);
        return -1;
    }
    const char *title = json_str(op, "title");
    const char *status = json_str(op, "status");
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO memory_preferences(preference_id,title,value_text,scope_json,context_json,"
        " rationale,status,revision,created_epoch,updated_epoch,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,1,?,?,?,?);";
    int rc = -1;
    if (memory_prepare(m, sql, &stmt) == 0) {
        bind_text_or_null(stmt, 1, id);
        bind_text_or_null(stmt, 2, title ? title : value);
        bind_text_or_null(stmt, 3, value);
        bind_text_or_null(stmt, 4, scope);
        bind_text_or_null(stmt, 5, context);
        bind_text_or_null(stmt, 6, json_str(op, "rationale") ? json_str(op, "rationale") : "");
        bind_text_or_null(stmt, 7, status ? status : "active");
        sqlite3_bind_int64(stmt, 8, epoch);
        sqlite3_bind_int64(stmt, 9, epoch);
        bind_text_or_null(stmt, 10, now);
        bind_text_or_null(stmt, 11, now);
        rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
        sqlite3_finalize(stmt);
    }
    if (rc == 0) {
        char *props =
            make_node_properties("preference_id", id, status ? status : "active", 1, epoch);
        int64_t node_id = props ? memory_graph_node(m, "preference", "Preference", id,
                                                    title ? title : value, "", props)
                                : -1;
        free(props);
        rc = node_id > 0 ? memory_document_upsert(m, "preference", id, title ? title : value,
                                                  json_str(op, "rationale"), value, scope)
                         : -1;
    }
    free(scope);
    free(context);
    if (rc != 0) {
        return -1;
    }
    result_add_entity(result_doc, results, "preference", id, 1);
    return 0;
}

static int apply_link(cbm_memory_t *m, yyjson_val *op, int64_t epoch, const char *now,
                      yyjson_mut_doc *result_doc, yyjson_mut_val *results) {
    const char *source_kind = json_str(op, "source_kind");
    const char *source_id = json_str(op, "source_id");
    const char *target_kind = json_str(op, "target_kind");
    const char *target_id = json_str(op, "target_id");
    const char *type = json_str(op, "relation_type");
    const char *key = json_str(op, "relation_key");
    char *properties = json_field_serialized(op, "properties", "{}");
    if (!properties) {
        return -1;
    }
    int rc = memory_relation_add(m, source_kind, source_id, target_kind, target_id, type,
                                 key ? key : "", properties, epoch, now);
    free(properties);
    if (rc != 0) {
        return -1;
    }
    result_add_entity(result_doc, results, "relation", type, -1);
    return 0;
}

static int apply_add_code_ref(cbm_memory_t *m, yyjson_val *op, int64_t epoch, const char *now,
                              yyjson_mut_doc *result_doc, yyjson_mut_val *results) {
    const char *project = json_str(op, "project");
    const char *ref_kind = json_str(op, "ref_kind");
    const char *qn = json_str(op, "qualified_name");
    const char *file_path = json_str(op, "file_path");
    if (!project) {
        return -1;
    }
    if (!ref_kind) {
        ref_kind = qn && qn[0] ? "symbol" : "file";
    }
    if (!valid_ref_kind(ref_kind)) {
        return -1;
    }
    char id[MEM_ID_MAX];
    const char *requested = json_str(op, "code_ref_id");
    if (requested && !safe_entity_id(requested)) {
        return -1;
    }
    if (requested) {
        (void)snprintf(id, sizeof(id), "%s", requested);
    } else if (memory_new_id(m, "coderef", id) != 0) {
        return -1;
    }
    char *locator = json_field_serialized(op, "locator", "{}");
    char *properties = json_field_serialized(op, "properties", "{}");
    if (!locator || !properties) {
        free(locator);
        free(properties);
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT INTO memory_code_refs(code_ref_id,project,git_remote,ref_kind,qualified_name,"
        " file_path,locator_json,properties_json,commit_or_tree_hash,last_resolved_at,"
        " resolution_status,revision,created_epoch,updated_epoch,created_at,updated_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?,?,1,?,?,?,?);";
    int rc = -1;
    if (memory_prepare(m, sql, &stmt) == 0) {
        bind_text_or_null(stmt, 1, id);
        bind_text_or_null(stmt, 2, project);
        bind_text_or_null(stmt, 3, json_str(op, "git_remote") ? json_str(op, "git_remote") : "");
        bind_text_or_null(stmt, 4, ref_kind);
        bind_text_or_null(stmt, 5, qn ? qn : "");
        bind_text_or_null(stmt, 6, file_path ? file_path : "");
        bind_text_or_null(stmt, 7, locator);
        bind_text_or_null(stmt, 8, properties);
        bind_text_or_null(stmt, 9,
                          json_str(op, "commit_or_tree_hash") ? json_str(op, "commit_or_tree_hash")
                                                              : "");
        bind_text_or_null(stmt, 10, json_str(op, "last_resolved_at"));
        bind_text_or_null(stmt, 11,
                          json_str(op, "resolution_status") ? json_str(op, "resolution_status")
                                                            : "unresolved");
        sqlite3_bind_int64(stmt, 12, epoch);
        sqlite3_bind_int64(stmt, 13, epoch);
        bind_text_or_null(stmt, 14, now);
        bind_text_or_null(stmt, 15, now);
        rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
        sqlite3_finalize(stmt);
    }
    if (rc == 0) {
        char name[CBM_SZ_1K];
        int name_len = snprintf(name, sizeof(name), "%s:%s", project,
                                qn && qn[0] ? qn : (file_path ? file_path : ref_kind));
        char *props = make_node_properties("code_ref_id", id, "unresolved", 1, epoch);
        int64_t node_id = name_len >= 0 && name_len < (int)sizeof(name) && props
                              ? memory_graph_node(m, "code_ref", "CodeRef", id, name,
                                                  file_path ? file_path : "", props)
                              : -1;
        free(props);
        rc = node_id > 0 ? memory_document_upsert(m, "code_ref", id, name, ref_kind, qn ? qn : "",
                                                  properties)
                         : -1;
    }
    free(locator);
    free(properties);
    if (rc != 0) {
        return -1;
    }
    result_add_entity(result_doc, results, "code_ref", id, 1);
    return 0;
}

static char *operation_result_existing(cbm_memory_t *m, const char *operation_id,
                                       const char *proposal_id, bool *key_reused) {
    *key_reused = false;
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(
            m, "SELECT proposal_id,result_json FROM memory_operations WHERE operation_id=?;",
            &stmt) != 0) {
        return NULL;
    }
    bind_text_or_null(stmt, 1, operation_id);
    char *result = NULL;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *stored_proposal = (const char *)sqlite3_column_text(stmt, 0);
        if (stored_proposal && strcmp(stored_proposal, proposal_id) == 0) {
            result = mem_strdup((const char *)sqlite3_column_text(stmt, 1));
        } else {
            *key_reused = true;
        }
    }
    sqlite3_finalize(stmt);
    return result;
}

static char *conflict_result_json(cbm_memory_t *m, const char *proposal_id, const char *reason) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return NULL;
    }
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_bool(doc, root, "ok", false);
    yyjson_mut_obj_add_strcpy(doc, root, "error", "revision_conflict");
    yyjson_mut_obj_add_strcpy(doc, root, "proposal_id", proposal_id);
    yyjson_mut_obj_add_strcpy(doc, root, "message", reason ? reason : "entity changed");
    yyjson_mut_obj_add_int(doc, root, "snapshot_epoch", cbm_memory_snapshot_epoch(m));
    char *out = json_write_mut(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static void mark_proposal_conflicted(cbm_memory_t *m, const char *proposal_id, const char *reason) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    if (!doc) {
        return;
    }
    yyjson_mut_val *obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, obj);
    yyjson_mut_obj_add_strcpy(doc, obj, "reason", reason ? reason : "revision conflict");
    char *conflict_json = json_write_mut(doc);
    yyjson_mut_doc_free(doc);
    if (!conflict_json) {
        return;
    }
    char now[MEM_TIME_MAX];
    iso_now(now);
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m,
                       "UPDATE memory_proposals SET status='conflicted',resolved_at=?,"
                       " conflict_json=? WHERE proposal_id=? AND status IN ('pending','approved');",
                       &stmt) == 0) {
        bind_text_or_null(stmt, 1, now);
        bind_text_or_null(stmt, 2, conflict_json);
        bind_text_or_null(stmt, 3, proposal_id);
        (void)sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    free(conflict_json);
}

static bool safe_operation_id(const char *operation_id) {
    if (!operation_id || !operation_id[0] || strlen(operation_id) > 200) {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)operation_id; *p; p++) {
        if (*p < 0x20 || *p == 0x7f) {
            return false;
        }
    }
    return true;
}

char *cbm_memory_commit_json(cbm_memory_t *m, const char *args_json) {
    if (!m) {
        return json_error("memory_unavailable", "memory handle is null");
    }
    yyjson_val *root = NULL;
    yyjson_doc *args_doc = parse_object_args(args_json, &root);
    if (!args_doc) {
        return json_error("invalid_arguments", "arguments must be a JSON object");
    }
    const char *proposal_id_arg = json_str(root, "proposal_id");
    const char *operation_id_arg = json_str(root, "operation_id");
    bool approved = json_bool(root, "user_approved", false);
    if (!safe_entity_id(proposal_id_arg) || !safe_operation_id(operation_id_arg)) {
        yyjson_doc_free(args_doc);
        return json_error("invalid_commit", "proposal_id and a valid operation_id are required");
    }
    char *proposal_id = mem_strdup(proposal_id_arg);
    char *operation_id = mem_strdup(operation_id_arg);
    yyjson_doc_free(args_doc);
    if (!proposal_id || !operation_id) {
        free(proposal_id);
        free(operation_id);
        return NULL;
    }

    bool key_reused = false;
    char *prior = operation_result_existing(m, operation_id, proposal_id, &key_reused);
    if (key_reused) {
        free(proposal_id);
        free(operation_id);
        return json_error("idempotency_key_reused",
                          "operation_id is already bound to a different proposal");
    }
    if (prior) {
        free(proposal_id);
        free(operation_id);
        return prior;
    }
    if (memory_begin(m) != 0) {
        free(proposal_id);
        free(operation_id);
        return json_error("database_busy", m->error);
    }
    prior = operation_result_existing(m, operation_id, proposal_id, &key_reused);
    if (key_reused) {
        memory_rollback(m);
        free(proposal_id);
        free(operation_id);
        return json_error("idempotency_key_reused",
                          "operation_id is already bound to a different proposal");
    }
    if (prior) {
        memory_rollback(m);
        free(proposal_id);
        free(operation_id);
        return prior;
    }

    sqlite3_stmt *stmt = NULL;
    const char *load_sql =
        "SELECT agent_id,session_id,base_epoch,status,operations_json,expected_revisions_json,"
        " result_json FROM memory_proposals WHERE proposal_id=?;";
    if (memory_prepare(m, load_sql, &stmt) != 0) {
        memory_rollback(m);
        free(proposal_id);
        free(operation_id);
        return json_error("commit_failed", m->error);
    }
    bind_text_or_null(stmt, 1, proposal_id);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        memory_rollback(m);
        free(proposal_id);
        free(operation_id);
        return json_error("proposal_not_found", "proposal does not exist");
    }
    char *agent_id = mem_strdup((const char *)sqlite3_column_text(stmt, 0));
    char *session_id = mem_strdup((const char *)sqlite3_column_text(stmt, 1));
    int64_t base_epoch = sqlite3_column_int64(stmt, 2);
    char *status = mem_strdup((const char *)sqlite3_column_text(stmt, 3));
    char *operations_json = mem_strdup((const char *)sqlite3_column_text(stmt, 4));
    char *expected_json = mem_strdup((const char *)sqlite3_column_text(stmt, 5));
    char *committed_result = mem_strdup((const char *)sqlite3_column_text(stmt, 6));
    sqlite3_finalize(stmt);
    if (!agent_id || !session_id || !status || !operations_json || !expected_json) {
        memory_rollback(m);
        free(agent_id);
        free(session_id);
        free(status);
        free(operations_json);
        free(expected_json);
        free(committed_result);
        free(proposal_id);
        free(operation_id);
        return NULL;
    }
    if (strcmp(status, "committed") == 0 && committed_result) {
        memory_rollback(m);
        yyjson_mut_doc *already_doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *already_root = already_doc ? yyjson_mut_obj(already_doc) : NULL;
        char *already = NULL;
        if (already_doc && already_root) {
            yyjson_mut_doc_set_root(already_doc, already_root);
            yyjson_mut_obj_add_bool(already_doc, already_root, "ok", false);
            yyjson_mut_obj_add_strcpy(already_doc, already_root, "error",
                                      "proposal_already_committed");
            yyjson_mut_obj_add_strcpy(already_doc, already_root, "proposal_id", proposal_id);
            yyjson_mut_obj_add_strcpy(already_doc, already_root, "message",
                                      "proposal was committed with another operation_id");
            yyjson_doc *prior_doc = yyjson_read(committed_result, strlen(committed_result), 0);
            if (prior_doc) {
                yyjson_val *prior_root = yyjson_doc_get_root(prior_doc);
                const char *original_operation =
                    yyjson_is_obj(prior_root) ? json_str(prior_root, "operation_id") : NULL;
                if (original_operation) {
                    yyjson_mut_obj_add_strcpy(already_doc, already_root, "original_operation_id",
                                              original_operation);
                }
                yyjson_doc_free(prior_doc);
            }
            already = json_write_mut(already_doc);
        }
        yyjson_mut_doc_free(already_doc);
        free(committed_result);
        free(agent_id);
        free(session_id);
        free(status);
        free(operations_json);
        free(expected_json);
        free(proposal_id);
        free(operation_id);
        return already ? already
                       : json_error("proposal_already_committed",
                                    "proposal was committed with another operation_id");
    }
    free(committed_result);
    if (strcmp(status, "pending") != 0 && strcmp(status, "approved") != 0) {
        memory_rollback(m);
        char *error = json_error("proposal_not_committable", status);
        free(agent_id);
        free(session_id);
        free(status);
        free(operations_json);
        free(expected_json);
        free(proposal_id);
        free(operation_id);
        return error;
    }
    free(status);

    yyjson_doc *ops_doc = yyjson_read(operations_json, strlen(operations_json), 0);
    yyjson_doc *expected_doc = yyjson_read(expected_json, strlen(expected_json), 0);
    yyjson_val *operations = ops_doc ? yyjson_doc_get_root(ops_doc) : NULL;
    yyjson_val *expected = expected_doc ? yyjson_doc_get_root(expected_doc) : NULL;
    if (!operations || !yyjson_is_arr(operations) || !expected || !yyjson_is_obj(expected)) {
        memory_rollback(m);
        yyjson_doc_free(ops_doc);
        yyjson_doc_free(expected_doc);
        free(agent_id);
        free(session_id);
        free(operations_json);
        free(expected_json);
        free(proposal_id);
        free(operation_id);
        return json_error("proposal_corrupt", "stored proposal JSON is invalid");
    }

    char now[MEM_TIME_MAX];
    iso_now(now);
    int64_t epoch = memory_bump_epoch(m, now);
    yyjson_mut_doc *result_doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *result_root = result_doc ? yyjson_mut_obj(result_doc) : NULL;
    yyjson_mut_val *results = result_doc ? yyjson_mut_arr(result_doc) : NULL;
    if (epoch < 0 || !result_doc || !result_root || !results) {
        memory_rollback(m);
        yyjson_doc_free(ops_doc);
        yyjson_doc_free(expected_doc);
        yyjson_mut_doc_free(result_doc);
        free(agent_id);
        free(session_id);
        free(operations_json);
        free(expected_json);
        free(proposal_id);
        free(operation_id);
        return NULL;
    }
    yyjson_mut_doc_set_root(result_doc, result_root);
    yyjson_mut_obj_add_bool(result_doc, result_root, "ok", true);
    yyjson_mut_obj_add_strcpy(result_doc, result_root, "proposal_id", proposal_id);
    yyjson_mut_obj_add_strcpy(result_doc, result_root, "operation_id", operation_id);
    yyjson_mut_obj_add_int(result_doc, result_root, "committed_epoch", epoch);
    yyjson_mut_obj_add_int(result_doc, result_root, "snapshot_epoch", epoch);
    yyjson_mut_obj_add_val(result_doc, result_root, "results", results);

    char *conflict = NULL;
    int apply_rc = 0;
    size_t index;
    size_t max;
    yyjson_val *op;
    yyjson_arr_foreach(operations, index, max, op) {
        const char *type = json_str(op, "type");
        if (strcmp(type, "upsert_page") == 0) {
            apply_rc = apply_upsert_page(m, op, expected, agent_id, session_id, epoch, now,
                                         result_doc, results, &conflict);
        } else if (strcmp(type, "add_claim") == 0) {
            apply_rc = apply_add_claim(m, op, epoch, now, result_doc, results);
        } else if (strcmp(type, "update_claim_status") == 0) {
            apply_rc = apply_update_claim_status(m, op, expected, epoch, now, result_doc, results,
                                                 &conflict);
        } else if (strcmp(type, "add_decision") == 0) {
            apply_rc = apply_add_decision(m, op, epoch, now, result_doc, results);
        } else if (strcmp(type, "add_experience") == 0) {
            apply_rc = apply_add_experience(m, op, epoch, now, result_doc, results);
        } else if (strcmp(type, "add_preference") == 0) {
            apply_rc = apply_add_preference(m, op, epoch, now, result_doc, results);
        } else if (strcmp(type, "link") == 0) {
            apply_rc = apply_link(m, op, epoch, now, result_doc, results);
        } else if (strcmp(type, "add_code_ref") == 0) {
            apply_rc = apply_add_code_ref(m, op, epoch, now, result_doc, results);
        } else {
            apply_rc = -1;
        }
        if (apply_rc < 0 && !conflict) {
            int ext = sqlite3_extended_errcode(m->db);
            if ((ext & 0xff) == SQLITE_CONSTRAINT) {
                conflict = mem_strdup("entity id or unique semantic key already exists");
                apply_rc = 1;
            }
        }
        if (apply_rc != 0) {
            break;
        }
    }
    free(operations_json);
    free(expected_json);
    yyjson_doc_free(ops_doc);
    yyjson_doc_free(expected_doc);

    if (apply_rc != 0) {
        memory_rollback(m);
        yyjson_mut_doc_free(result_doc);
        if (apply_rc == 1) {
            mark_proposal_conflicted(m, proposal_id, conflict);
        }
        char *out = apply_rc == 1
                        ? conflict_result_json(m, proposal_id, conflict)
                        : json_error("commit_failed", m->error[0] ? m->error : "operation failed");
        free(conflict);
        free(agent_id);
        free(session_id);
        free(proposal_id);
        free(operation_id);
        return out;
    }
    free(conflict);

    char *result_json = json_write_mut(result_doc);
    yyjson_mut_doc_free(result_doc);
    if (!result_json) {
        memory_rollback(m);
        free(agent_id);
        free(session_id);
        free(proposal_id);
        free(operation_id);
        return NULL;
    }
    char activity_id[MEM_ID_MAX];
    char *activity_props = NULL;
    int final_rc = memory_new_id(m, "act", activity_id);
    if (final_rc == 0 &&
        memory_prepare(m,
                       "INSERT INTO memory_operations(operation_id,proposal_id,committed_epoch,"
                       " result_json,created_at) VALUES(?,?,?,?,?);",
                       &stmt) == 0) {
        bind_text_or_null(stmt, 1, operation_id);
        bind_text_or_null(stmt, 2, proposal_id);
        sqlite3_bind_int64(stmt, 3, epoch);
        bind_text_or_null(stmt, 4, result_json);
        bind_text_or_null(stmt, 5, now);
        final_rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
        sqlite3_finalize(stmt);
    } else {
        final_rc = -1;
    }
    if (final_rc == 0 &&
        memory_prepare(
            m,
            "INSERT INTO memory_activities(activity_id,operation_id,proposal_id,agent_id,"
            " session_id,action,base_epoch,committed_epoch,user_approved,details_json,"
            " created_at) VALUES(?,?,?,?,?,'commit',?,?,?,'{}',?);",
            &stmt) == 0) {
        bind_text_or_null(stmt, 1, activity_id);
        bind_text_or_null(stmt, 2, operation_id);
        bind_text_or_null(stmt, 3, proposal_id);
        bind_text_or_null(stmt, 4, agent_id);
        bind_text_or_null(stmt, 5, session_id);
        sqlite3_bind_int64(stmt, 6, base_epoch);
        sqlite3_bind_int64(stmt, 7, epoch);
        sqlite3_bind_int(stmt, 8, approved ? 1 : 0);
        bind_text_or_null(stmt, 9, now);
        final_rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
        sqlite3_finalize(stmt);
    } else {
        final_rc = -1;
    }
    activity_props = make_node_properties("activity_id", activity_id, "committed", 1, epoch);
    if (final_rc == 0 &&
        (!activity_props || memory_graph_node(m, "activity", "Activity", activity_id, operation_id,
                                              "", activity_props) <= 0)) {
        final_rc = -1;
    }
    free(activity_props);
    if (final_rc == 0 && memory_rebuild_projection_internal(m, true) != 0) {
        final_rc = -1;
    }
    if (final_rc == 0 &&
        memory_prepare(m,
                       "UPDATE memory_proposals SET status='committed',resolved_at=?,"
                       " committed_epoch=?,result_json=? WHERE proposal_id=?"
                       " AND status IN ('pending','approved');",
                       &stmt) == 0) {
        bind_text_or_null(stmt, 1, now);
        sqlite3_bind_int64(stmt, 2, epoch);
        bind_text_or_null(stmt, 3, result_json);
        bind_text_or_null(stmt, 4, proposal_id);
        final_rc = sqlite3_step(stmt) == SQLITE_DONE && sqlite3_changes(m->db) == 1 ? 0 : -1;
        sqlite3_finalize(stmt);
    } else {
        final_rc = -1;
    }
    if (final_rc != 0 || memory_commit_tx(m) != 0) {
        memory_rollback(m);
        free(result_json);
        free(agent_id);
        free(session_id);
        free(proposal_id);
        free(operation_id);
        return json_error("commit_failed", sqlite3_errmsg(m->db));
    }
    (void)memory_recover_outbox(m);
    free(agent_id);
    free(session_id);
    free(proposal_id);
    free(operation_id);
    return result_json;
}

typedef struct {
    const char *kind;
    const char *label;
    const char *sql;
    bool document;
} projection_query_t;

static int project_query(cbm_memory_t *m, const projection_query_t *query) {
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, query->sql, &stmt) != 0) {
        return -1;
    }
    int rc = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *id_col = (const char *)sqlite3_column_text(stmt, 0);
        const char *name_col = (const char *)sqlite3_column_text(stmt, 1);
        const char *file_col = (const char *)sqlite3_column_text(stmt, 2);
        const char *props_col = (const char *)sqlite3_column_text(stmt, 3);
        char *id = mem_strdup(id_col ? id_col : "");
        char *name = mem_strdup(name_col ? name_col : "");
        char *file = mem_strdup(file_col ? file_col : "");
        char *props = mem_strdup(props_col ? props_col : "{}");
        char *title = NULL;
        char *summary = NULL;
        char *body = NULL;
        char *metadata = NULL;
        if (query->document) {
            title = mem_strdup(
                sqlite3_column_text(stmt, 4) ? (const char *)sqlite3_column_text(stmt, 4) : "");
            summary = mem_strdup(
                sqlite3_column_text(stmt, 5) ? (const char *)sqlite3_column_text(stmt, 5) : "");
            body = mem_strdup(
                sqlite3_column_text(stmt, 6) ? (const char *)sqlite3_column_text(stmt, 6) : "");
            metadata = mem_strdup(
                sqlite3_column_text(stmt, 7) ? (const char *)sqlite3_column_text(stmt, 7) : "");
        }
        if (!id || !name || !file || !props ||
            (query->document && (!title || !summary || !body || !metadata)) ||
            memory_graph_node(m, query->kind, query->label, id, name, file, props) <= 0 ||
            (query->document &&
             memory_document_upsert(m, query->kind, id, title, summary, body, metadata) != 0)) {
            rc = -1;
        }
        free(id);
        free(name);
        free(file);
        free(props);
        free(title);
        free(summary);
        free(body);
        free(metadata);
        if (rc != 0) {
            break;
        }
    }
    sqlite3_finalize(stmt);
    return rc;
}

static int project_source_bodies(cbm_memory_t *m) {
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m,
                       "SELECT source_id,object_relpath,title,origin,metadata_json,content_hash"
                       " FROM memory_sources ORDER BY source_id;",
                       &stmt) != 0) {
        return -1;
    }
    int rc = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        char *id = mem_strdup((const char *)sqlite3_column_text(stmt, 0));
        char *rel = mem_strdup((const char *)sqlite3_column_text(stmt, 1));
        char *title = mem_strdup((const char *)sqlite3_column_text(stmt, 2));
        char *origin = mem_strdup((const char *)sqlite3_column_text(stmt, 3));
        char *metadata = mem_strdup((const char *)sqlite3_column_text(stmt, 4));
        char *expected_hash = mem_strdup((const char *)sqlite3_column_text(stmt, 5));
        char path[MEM_PATH_MAX];
        unsigned char *bytes = NULL;
        size_t len = 0;
        if (!id || !rel || !title || !origin || !metadata || !expected_hash ||
            snprintf(path, sizeof(path), "%s/%s", m->home, rel) >= (int)sizeof(path) ||
            read_file_bytes(path, &bytes, &len) != 0) {
            rc = -1;
        } else {
            char actual_hash[CBM_SHA256_HEX_LEN + 1];
            cbm_sha256_hex(bytes, len, actual_hash);
            if (strcmp(actual_hash, expected_hash) != 0) {
                rc = -1;
            } else {
                bool text = memchr(bytes, 0, len) == NULL;
                rc = memory_document_upsert(m, "source", id, title, origin,
                                            text ? (const char *)bytes : "", metadata);
            }
        }
        free(bytes);
        free(id);
        free(rel);
        free(title);
        free(origin);
        free(metadata);
        free(expected_hash);
        if (rc != 0) {
            break;
        }
    }
    sqlite3_finalize(stmt);
    return rc;
}

static int memory_rebuild_projection_internal(cbm_memory_t *m, bool in_transaction) {
    if (!m) {
        return -1;
    }
    bool own_tx = !in_transaction;
    if (own_tx && memory_begin(m) != 0) {
        return -1;
    }
    if (cbm_store_delete_nodes_by_project(m->graph, CBM_MEMORY_PROJECT) != CBM_STORE_OK ||
        memory_exec(m, "INSERT INTO nodes_fts(nodes_fts) VALUES('delete-all');") != 0 ||
        memory_exec(m, "DELETE FROM memory_documents;") != 0) {
        if (own_tx) {
            memory_rollback(m);
        }
        return -1;
    }

    static const projection_query_t queries[] = {
        {"source", "MemorySource",
         "SELECT source_id,title,object_relpath,json_object("
         "'source_id',source_id,'content_hash',content_hash,'origin',origin,"
         "'media_type',media_type,'publisher',publisher,'published_at',published_at,"
         "'retrieved_at',retrieved_at,'metadata',json(metadata_json),"
         "'revision_of',revision_of,'byte_size',byte_size) FROM memory_sources ORDER BY source_id;",
         false},
        {"page", "WikiPage",
         "SELECT p.page_id,p.title,'',json_object('page_id',p.page_id,'slug',p.slug,"
         "'page_kind',p.page_kind,'status',p.status,'current_revision',p.current_revision,"
         "'current_revision_id',p.current_revision_id),p.title,'',coalesce(r.markdown,''),"
         "json_object('page_kind',p.page_kind,'slug',p.slug)"
         " FROM memory_pages p LEFT JOIN memory_revisions r"
         " ON r.revision_id=p.current_revision_id ORDER BY p.page_id;",
         true},
        {"revision", "WikiRevision",
         "SELECT revision_id,revision_id,'',json_object('revision_id',revision_id,"
         "'page_id',page_id,'revision',revision,'base_revision',base_revision,"
         "'body_hash',body_hash,'author_agent',author_agent,'author_session',author_session,"
         "'created_epoch',created_epoch,'created_at',created_at)"
         " FROM memory_revisions ORDER BY revision_id;",
         false},
        {"claim", "Claim",
         "SELECT claim_id,subject,'',json_object('claim_id',claim_id,'claim_kind',claim_kind,"
         "'subject',subject,'predicate',predicate,'object',object_text,'scope',json(scope_json),"
         "'status',status,'valid_from',valid_from,'valid_to',valid_to,"
         "'recorded_from',recorded_from,'recorded_to',recorded_to,'observed_at',observed_at,"
         "'review_after',review_after,'volatility',volatility,'revision',revision,"
         "'created_epoch',created_epoch,'updated_epoch',updated_epoch),subject,predicate,"
         "subject||' '||predicate||' '||object_text,scope_json"
         " FROM memory_claims ORDER BY claim_id;",
         true},
        {"decision", "Decision",
         "SELECT decision_id,coalesce(nullif(title,''),chosen_option),'',json_object("
         "'decision_id',decision_id,'chosen_option',chosen_option,"
         "'alternatives',json(alternatives_json),'rejected_because',json(rejected_because_json),"
         "'assumptions',json(assumptions_json),'constraints',json(constraints_json),"
         "'applicability',json(applicability_json),'invalidation',json(invalidation_json),"
         "'expected_outcome',expected_outcome,'actual_outcome',actual_outcome,"
         "'review_after',review_after,'exit_criteria',json(exit_criteria_json),"
         "'status',status,'revision',revision),coalesce(nullif(title,''),chosen_option),"
         "expected_outcome,chosen_option,applicability_json FROM memory_decisions"
         " ORDER BY decision_id;",
         true},
        {"experience", "Experience",
         "SELECT experience_id,coalesce(nullif(title,''),observation),'',json_object("
         "'experience_id',experience_id,'context',json(context_json),"
         "'environment',json(environment_json),'action',action,'observation',observation,"
         "'outcome',outcome,'sample_size',sample_size,"
         "'generalization_limits',json(generalization_limits_json),"
         "'failure_signals',json(failure_signals_json),'status',status,'revision',revision),"
         "coalesce(nullif(title,''),observation),outcome,observation,context_json"
         " FROM memory_experiences ORDER BY experience_id;",
         true},
        {"preference", "Preference",
         "SELECT preference_id,coalesce(nullif(title,''),value_text),'',json_object("
         "'preference_id',preference_id,'value',value_text,'scope',json(scope_json),"
         "'context',json(context_json),'rationale',rationale,'status',status,'revision',revision),"
         "coalesce(nullif(title,''),value_text),rationale,value_text,scope_json"
         " FROM memory_preferences ORDER BY preference_id;",
         true},
        {"code_ref", "CodeRef",
         "SELECT code_ref_id,project||':'||coalesce(nullif(qualified_name,''),file_path),"
         "file_path,json_object('code_ref_id',code_ref_id,'project',project,"
         "'git_remote',git_remote,'ref_kind',ref_kind,'qualified_name',qualified_name,"
         "'file_path',file_path,'locator',json(locator_json),'properties',json(properties_json),"
         "'commit_or_tree_hash',commit_or_tree_hash,'last_resolved_at',last_resolved_at,"
         "'resolution_status',resolution_status,'revision',revision),"
         "project||':'||coalesce(nullif(qualified_name,''),file_path),ref_kind,qualified_name,"
         "properties_json FROM memory_code_refs ORDER BY code_ref_id;",
         true},
        {"activity", "Activity",
         "SELECT activity_id,action||':'||operation_id,'',json_object("
         "'activity_id',activity_id,'operation_id',operation_id,'proposal_id',proposal_id,"
         "'agent_id',agent_id,'session_id',session_id,'action',action,'base_epoch',base_epoch,"
         "'committed_epoch',committed_epoch,'user_approved',user_approved,'created_at',created_at)"
         " FROM memory_activities ORDER BY activity_id;",
         false},
    };
    int rc = 0;
    for (size_t i = 0; i < sizeof(queries) / sizeof(queries[0]); i++) {
        if (project_query(m, &queries[i]) != 0) {
            rc = -1;
            break;
        }
    }
    if (rc == 0) {
        rc = project_source_bodies(m);
    }
    sqlite3_stmt *stmt = NULL;
    if (rc == 0 &&
        memory_prepare(m,
                       "SELECT source_kind,source_id,target_kind,target_id,type,properties_json"
                       " FROM memory_relations WHERE recorded_to IS NULL ORDER BY relation_id;",
                       &stmt) == 0) {
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *sk = (const char *)sqlite3_column_text(stmt, 0);
            const char *sid = (const char *)sqlite3_column_text(stmt, 1);
            const char *tk = (const char *)sqlite3_column_text(stmt, 2);
            const char *tid = (const char *)sqlite3_column_text(stmt, 3);
            const char *type = (const char *)sqlite3_column_text(stmt, 4);
            const char *props = (const char *)sqlite3_column_text(stmt, 5);
            if (memory_graph_edge(m, sk, sid, tk, tid, type, props) != 0) {
                rc = -1;
                break;
            }
        }
        sqlite3_finalize(stmt);
    } else if (rc == 0) {
        rc = -1;
    }
    if (rc == 0 && memory_exec(m, "INSERT INTO memory_fts(memory_fts) VALUES('rebuild');") != 0) {
        rc = -1;
    }
    if (own_tx) {
        if (rc == 0) {
            rc = memory_commit_tx(m);
        } else {
            memory_rollback(m);
        }
    }
    return rc;
}

int cbm_memory_rebuild_projection(cbm_memory_t *m) {
    return m ? memory_rebuild_projection_internal(m, false) : -1;
}

/* ── Applicability-first retrieval ────────────────────────────── */

static char *fts_expression(const char *query) {
    if (!query) {
        return NULL;
    }
    size_t len = strlen(query);
    char *out = malloc(len * 3U + 8U);
    if (!out) {
        return NULL;
    }
    size_t pos = 0;
    const unsigned char *p = (const unsigned char *)query;
    bool first = true;
    while (*p) {
        while (*p && (*p == ' ' || *p == '\t' || *p == '\r' || *p == '\n')) {
            p++;
        }
        if (!*p) {
            break;
        }
        if (!first) {
            memcpy(out + pos, " OR ", 4);
            pos += 4;
        }
        out[pos++] = '"';
        while (*p && *p != ' ' && *p != '\t' && *p != '\r' && *p != '\n') {
            if (*p != '"') {
                out[pos++] = (char)*p;
            }
            p++;
        }
        out[pos++] = '"';
        first = false;
    }
    out[pos] = '\0';
    if (first) {
        free(out);
        return NULL;
    }
    return out;
}

static void add_relation_row(yyjson_mut_doc *doc, yyjson_mut_val *array, sqlite3_stmt *stmt) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "relation_id", (const char *)sqlite3_column_text(stmt, 0));
    yyjson_mut_obj_add_strcpy(doc, item, "source_kind", (const char *)sqlite3_column_text(stmt, 1));
    yyjson_mut_obj_add_strcpy(doc, item, "source_id", (const char *)sqlite3_column_text(stmt, 2));
    yyjson_mut_obj_add_strcpy(doc, item, "target_kind", (const char *)sqlite3_column_text(stmt, 3));
    yyjson_mut_obj_add_strcpy(doc, item, "target_id", (const char *)sqlite3_column_text(stmt, 4));
    yyjson_mut_obj_add_strcpy(doc, item, "type", (const char *)sqlite3_column_text(stmt, 5));
    const char *props = (const char *)sqlite3_column_text(stmt, 6);
    if (props) {
        yyjson_doc *props_doc = yyjson_read(props, strlen(props), 0);
        if (props_doc) {
            yyjson_mut_obj_add_val(doc, item, "properties",
                                   yyjson_val_mut_copy(doc, yyjson_doc_get_root(props_doc)));
            yyjson_doc_free(props_doc);
        }
    }
    yyjson_mut_arr_add_val(array, item);
}

static int query_overview(cbm_memory_t *m, yyjson_mut_doc *doc, yyjson_mut_val *root) {
    static const struct {
        const char *name;
        const char *table;
    } counts[] = {
        {"sources", "memory_sources"},
        {"pages", "memory_pages"},
        {"claims", "memory_claims"},
        {"decisions", "memory_decisions"},
        {"experiences", "memory_experiences"},
        {"preferences", "memory_preferences"},
        {"code_refs", "memory_code_refs"},
        {"dirty", "memory_dirty WHERE status='open'"},
        {"pending_materialization", "memory_outbox WHERE state IN ('pending','leased','failed')"}};
    yyjson_mut_val *count_obj = yyjson_mut_obj(doc);
    for (size_t i = 0; i < sizeof(counts) / sizeof(counts[0]); i++) {
        char sql[CBM_SZ_256];
        (void)snprintf(sql, sizeof(sql), "SELECT count(*) FROM %s;", counts[i].table);
        sqlite3_stmt *stmt = NULL;
        if (memory_prepare(m, sql, &stmt) != 0) {
            return -1;
        }
        int64_t count = sqlite3_step(stmt) == SQLITE_ROW ? sqlite3_column_int64(stmt, 0) : 0;
        sqlite3_finalize(stmt);
        yyjson_mut_obj_add_int(doc, count_obj, counts[i].name, count);
    }
    yyjson_mut_obj_add_val(doc, root, "counts", count_obj);
    return 0;
}

static int entity_state(cbm_memory_t *m, const char *kind, const char *id, char status[32],
                        char review_after[MEM_TIME_MAX]) {
    const char *sql = NULL;
    if (strcmp(kind, "claim") == 0) {
        sql = "SELECT status,coalesce(review_after,'') FROM memory_claims WHERE claim_id=?;";
    } else if (strcmp(kind, "decision") == 0) {
        sql = "SELECT status,coalesce(review_after,'') FROM memory_decisions WHERE decision_id=?;";
    } else if (strcmp(kind, "page") == 0) {
        sql = "SELECT status,'' FROM memory_pages WHERE page_id=?;";
    } else if (strcmp(kind, "experience") == 0) {
        sql = "SELECT status,'' FROM memory_experiences WHERE experience_id=?;";
    } else if (strcmp(kind, "preference") == 0) {
        sql = "SELECT status,'' FROM memory_preferences WHERE preference_id=?;";
    } else {
        status[0] = '\0';
        review_after[0] = '\0';
        return 0;
    }
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, id);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        (void)snprintf(status, 32, "%s",
                       sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0)
                                                    : "");
        (void)snprintf(review_after, MEM_TIME_MAX, "%s",
                       sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1)
                                                    : "");
    } else {
        status[0] = '\0';
        review_after[0] = '\0';
    }
    sqlite3_finalize(stmt);
    return 0;
}

typedef enum {
    CONTEXT_MATCH = 0,
    CONTEXT_MISMATCH = 1,
    CONTEXT_UNKNOWN = 2,
} context_match_t;

/* Treat an object as a set of required key/value pairs. Arrays are conjunctions of
 * conditions, and a string condition may refer to a boolean key in the current
 * context (for example ["linux", "ci"]). This deliberately stays deterministic:
 * the server compares recorded applicability but never asks an LLM to reinterpret it. */
static context_match_t context_condition_match(yyjson_val *required, yyjson_val *current) {
    if (!required) {
        return CONTEXT_MATCH;
    }
    if (!current) {
        return CONTEXT_UNKNOWN;
    }
    if (yyjson_is_obj(required)) {
        if (!yyjson_is_obj(current)) {
            return CONTEXT_MISMATCH;
        }
        bool unknown = false;
        size_t index;
        size_t max;
        yyjson_val *key;
        yyjson_val *value;
        yyjson_obj_foreach(required, index, max, key, value) {
            const char *name = yyjson_get_str(key);
            yyjson_val *actual = name ? yyjson_obj_get(current, name) : NULL;
            context_match_t state = context_condition_match(value, actual);
            if (state == CONTEXT_MISMATCH) {
                return CONTEXT_MISMATCH;
            }
            unknown |= state == CONTEXT_UNKNOWN;
        }
        return unknown ? CONTEXT_UNKNOWN : CONTEXT_MATCH;
    }
    if (yyjson_is_arr(required)) {
        if (yyjson_arr_size(required) == 0) {
            return CONTEXT_MATCH;
        }
        bool unknown = false;
        size_t index;
        size_t max;
        yyjson_val *condition;
        yyjson_arr_foreach(required, index, max, condition) {
            context_match_t state = context_condition_match(condition, current);
            if (state == CONTEXT_MISMATCH) {
                return CONTEXT_MISMATCH;
            }
            unknown |= state == CONTEXT_UNKNOWN;
        }
        return unknown ? CONTEXT_UNKNOWN : CONTEXT_MATCH;
    }
    if (yyjson_is_str(required) && yyjson_is_obj(current)) {
        const char *name = yyjson_get_str(required);
        yyjson_val *actual = name ? yyjson_obj_get(current, name) : NULL;
        if (!actual) {
            return CONTEXT_UNKNOWN;
        }
        if (yyjson_is_bool(actual)) {
            return yyjson_get_bool(actual) ? CONTEXT_MATCH : CONTEXT_MISMATCH;
        }
        if (yyjson_is_str(actual)) {
            return strcmp(yyjson_get_str(actual), name) == 0 ? CONTEXT_MATCH : CONTEXT_MISMATCH;
        }
        return CONTEXT_MISMATCH;
    }
    if (yyjson_is_arr(current)) {
        size_t index;
        size_t max;
        yyjson_val *actual;
        yyjson_arr_foreach(current, index, max, actual) {
            if (yyjson_equals(required, actual)) {
                return CONTEXT_MATCH;
            }
        }
        return CONTEXT_MISMATCH;
    }
    return yyjson_equals(required, current) ? CONTEXT_MATCH : CONTEXT_MISMATCH;
}

static bool json_has_conditions(yyjson_val *value) {
    if (!value || yyjson_is_null(value)) {
        return false;
    }
    if (yyjson_is_obj(value)) {
        return yyjson_obj_size(value) > 0;
    }
    if (yyjson_is_arr(value)) {
        return yyjson_arr_size(value) > 0;
    }
    return true;
}

static void context_add_reason(yyjson_mut_doc *doc, yyjson_mut_val *local, yyjson_mut_val *global,
                               const char *kind, const char *id, const char *field) {
    char reason[CBM_SZ_512];
    (void)snprintf(reason, sizeof(reason), "%s:%s:%s", kind ? kind : "memory", id ? id : "",
                   field ? field : "context");
    yyjson_mut_arr_add_strcpy(doc, local, reason);
    yyjson_mut_arr_add_strcpy(doc, global, reason);
}

static void evaluate_context_field(yyjson_mut_doc *doc, const char *kind, const char *id,
                                   const char *field, const char *json, yyjson_val *current_context,
                                   bool invalidation, yyjson_mut_val *local_matched,
                                   yyjson_mut_val *local_mismatched, yyjson_mut_val *local_unknown,
                                   yyjson_mut_val *global_matched,
                                   yyjson_mut_val *global_mismatched,
                                   yyjson_mut_val *global_unknown, bool *has_mismatch,
                                   bool *has_unknown) {
    yyjson_doc *condition_doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    yyjson_val *condition = condition_doc ? yyjson_doc_get_root(condition_doc) : NULL;
    if (!condition_doc || !json_has_conditions(condition)) {
        yyjson_doc_free(condition_doc);
        return;
    }
    context_match_t state = context_condition_match(condition, current_context);
    if (invalidation && state != CONTEXT_UNKNOWN) {
        state = state == CONTEXT_MATCH ? CONTEXT_MISMATCH : CONTEXT_MATCH;
    }
    if (state == CONTEXT_MATCH) {
        context_add_reason(doc, local_matched, global_matched, kind, id, field);
    } else if (state == CONTEXT_MISMATCH) {
        context_add_reason(doc, local_mismatched, global_mismatched, kind, id, field);
        *has_mismatch = true;
    } else {
        context_add_reason(doc, local_unknown, global_unknown, kind, id, field);
        *has_unknown = true;
    }
    yyjson_doc_free(condition_doc);
}

static int enrich_applicability(cbm_memory_t *m, yyjson_mut_doc *doc, yyjson_mut_val *item,
                                const char *kind, const char *id, yyjson_val *current_context,
                                yyjson_mut_val *global_matched, yyjson_mut_val *global_mismatched,
                                yyjson_mut_val *global_unknown, bool *has_mismatch,
                                bool *has_unknown, bool *item_mismatch_out,
                                bool *item_unknown_out) {
    const char *sql = NULL;
    int fields = 0;
    bool invalidation_second = false;
    if (strcmp(kind, "claim") == 0) {
        sql = "SELECT scope_json FROM memory_claims WHERE claim_id=?;";
        fields = 1;
    } else if (strcmp(kind, "decision") == 0) {
        sql = "SELECT applicability_json,invalidation_json FROM memory_decisions"
              " WHERE decision_id=?;";
        fields = 2;
        invalidation_second = true;
    } else if (strcmp(kind, "experience") == 0) {
        sql = "SELECT context_json FROM memory_experiences WHERE experience_id=?;";
        fields = 1;
    } else if (strcmp(kind, "preference") == 0) {
        sql = "SELECT scope_json,context_json FROM memory_preferences WHERE preference_id=?;";
        fields = 2;
    } else {
        return 0;
    }
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, id);
    if (sqlite3_step(stmt) != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        return 0;
    }
    char *first =
        mem_strdup(sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0)
                                                : (strcmp(kind, "decision") == 0 ? "[]" : "{}"));
    char *second = fields == 2 ? mem_strdup(sqlite3_column_text(stmt, 1)
                                                ? (const char *)sqlite3_column_text(stmt, 1)
                                                : "[]")
                               : NULL;
    sqlite3_finalize(stmt);
    if (!first || (fields == 2 && !second)) {
        free(first);
        free(second);
        return -1;
    }
    yyjson_mut_val *applicability = yyjson_mut_obj(doc);
    yyjson_mut_val *matched = yyjson_mut_arr(doc);
    yyjson_mut_val *mismatched = yyjson_mut_arr(doc);
    yyjson_mut_val *unknown = yyjson_mut_arr(doc);
    bool item_mismatch = false;
    bool item_unknown = false;
    const char *first_field = strcmp(kind, "claim") == 0
                                  ? "scope"
                                  : (strcmp(kind, "decision") == 0
                                         ? "applicability"
                                         : (strcmp(kind, "preference") == 0 ? "scope" : "context"));
    yyjson_doc *first_doc = yyjson_read(first, strlen(first), 0);
    bool missing_experience_context =
        strcmp(kind, "experience") == 0 &&
        (!first_doc || !json_has_conditions(yyjson_doc_get_root(first_doc)));
    yyjson_doc_free(first_doc);
    if (missing_experience_context) {
        context_add_reason(doc, unknown, global_unknown, kind, id, "context_missing");
        item_unknown = true;
    } else {
        evaluate_context_field(doc, kind, id, first_field, first, current_context, false, matched,
                               mismatched, unknown, global_matched, global_mismatched,
                               global_unknown, &item_mismatch, &item_unknown);
    }
    if (fields == 2) {
        evaluate_context_field(doc, kind, id, invalidation_second ? "invalidation" : "context",
                               second, current_context, invalidation_second, matched, mismatched,
                               unknown, global_matched, global_mismatched, global_unknown,
                               &item_mismatch, &item_unknown);
    }
    *has_mismatch |= item_mismatch;
    *has_unknown |= item_unknown;
    if (item_mismatch_out) {
        *item_mismatch_out = item_mismatch;
    }
    if (item_unknown_out) {
        *item_unknown_out = item_unknown;
    }
    yyjson_mut_obj_add_val(doc, applicability, "matched", matched);
    yyjson_mut_obj_add_val(doc, applicability, "mismatched", mismatched);
    yyjson_mut_obj_add_val(doc, applicability, "unknown", unknown);
    yyjson_mut_obj_add_val(doc, item, "applicability", applicability);
    free(first);
    free(second);
    return 0;
}

static int source_lineage_root(cbm_memory_t *m, const char *source_id, char root[MEM_ID_MAX]) {
    (void)snprintf(root, MEM_ID_MAX, "%s", source_id ? source_id : "");
    for (int depth = 0; depth < 64 && root[0]; depth++) {
        sqlite3_stmt *stmt = NULL;
        if (memory_prepare(m,
                           "SELECT coalesce(revision_of,'') FROM memory_sources WHERE source_id=?;",
                           &stmt) != 0) {
            return -1;
        }
        bind_text_or_null(stmt, 1, root);
        char parent[MEM_ID_MAX] = "";
        if (sqlite3_step(stmt) == SQLITE_ROW && sqlite3_column_text(stmt, 0)) {
            (void)snprintf(parent, sizeof(parent), "%s",
                           (const char *)sqlite3_column_text(stmt, 0));
        }
        sqlite3_finalize(stmt);
        if (!parent[0]) {
            return 0;
        }
        if (strcmp(parent, root) == 0) {
            return -1;
        }
        (void)snprintf(root, MEM_ID_MAX, "%s", parent);
    }
    /* A chain that did not reach a root within the bound is corrupt or cyclic. */
    return -1;
}

static int enrich_evidence_and_conflicts(cbm_memory_t *m, yyjson_mut_doc *doc, yyjson_mut_val *item,
                                         const char *kind, const char *id, bool *contested,
                                         bool *has_supported_evidence, bool *has_basis) {
    *has_supported_evidence = false;
    *has_basis = false;
    yyjson_mut_val *evidence = yyjson_mut_arr(doc);
    sqlite3_stmt *stmt = NULL;
    const char *evidence_sql =
        "SELECT type,target_kind,target_id FROM memory_relations WHERE source_kind=? AND "
        "source_id=?"
        " AND type IN ('SUPPORTED_BY','DECIDED_USING') AND recorded_to IS NULL"
        " ORDER BY type,target_kind,target_id;";
    if (memory_prepare(m, evidence_sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, kind);
    bind_text_or_null(stmt, 2, id);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *relation = (const char *)sqlite3_column_text(stmt, 0);
        const char *target_kind = (const char *)sqlite3_column_text(stmt, 1);
        const char *target_id = (const char *)sqlite3_column_text(stmt, 2);
        *has_basis = true;
        *has_supported_evidence |=
            relation && strcmp(relation, "SUPPORTED_BY") == 0 && target_kind &&
            (strcmp(target_kind, "source") == 0 || strcmp(target_kind, "experience") == 0);
        yyjson_mut_val *entry = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, entry, "relation", relation ? relation : "");
        yyjson_mut_obj_add_strcpy(doc, entry, "kind", target_kind ? target_kind : "");
        yyjson_mut_obj_add_strcpy(doc, entry, "id", target_id ? target_id : "");
        if (target_kind && target_id && strcmp(target_kind, "source") == 0) {
            char root[MEM_ID_MAX];
            if (source_lineage_root(m, target_id, root) == 0) {
                yyjson_mut_obj_add_strcpy(doc, entry, "lineage_root", root);
            }
        }
        yyjson_mut_arr_add_val(evidence, entry);
    }
    sqlite3_finalize(stmt);
    yyjson_mut_obj_add_val(doc, item, "evidence_lineage", evidence);

    yyjson_mut_val *warnings = yyjson_mut_arr(doc);
    const char *warning_sql =
        "SELECT type,source_kind,source_id,target_kind,target_id FROM memory_relations"
        " WHERE recorded_to IS NULL AND type IN ('CONTRADICTS','SUPERSEDES') AND"
        " ((source_kind=? AND source_id=?) OR (target_kind=? AND target_id=?))"
        " ORDER BY type,relation_id;";
    if (memory_prepare(m, warning_sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, kind);
    bind_text_or_null(stmt, 2, id);
    bind_text_or_null(stmt, 3, kind);
    bind_text_or_null(stmt, 4, id);
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *type = (const char *)sqlite3_column_text(stmt, 0);
        const char *source_kind = (const char *)sqlite3_column_text(stmt, 1);
        const char *source_id = (const char *)sqlite3_column_text(stmt, 2);
        const char *target_kind = (const char *)sqlite3_column_text(stmt, 3);
        const char *target_id = (const char *)sqlite3_column_text(stmt, 4);
        bool outgoing = source_kind && source_id && strcmp(source_kind, kind) == 0 &&
                        strcmp(source_id, id) == 0;
        yyjson_mut_val *warning = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, warning, "type", type ? type : "");
        yyjson_mut_obj_add_strcpy(doc, warning, "direction", outgoing ? "outgoing" : "incoming");
        yyjson_mut_obj_add_strcpy(doc, warning, "other_kind",
                                  outgoing ? (target_kind ? target_kind : "")
                                           : (source_kind ? source_kind : ""));
        yyjson_mut_obj_add_strcpy(doc, warning, "other_id",
                                  outgoing ? (target_id ? target_id : "")
                                           : (source_id ? source_id : ""));
        yyjson_mut_arr_add_val(warnings, warning);
        if (type && strcmp(type, "CONTRADICTS") == 0) {
            *contested = true;
        }
    }
    sqlite3_finalize(stmt);
    yyjson_mut_obj_add_val(doc, item, "relation_warnings", warnings);
    return 0;
}

static int enrich_dirty_state(cbm_memory_t *m, yyjson_mut_doc *doc, yyjson_mut_val *item,
                              const char *kind, const char *id, bool *is_dirty) {
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT reason,entity_kind,entity_id FROM memory_dirty WHERE status='open'"
                      " AND entity_kind=? AND entity_id=? UNION"
                      " SELECT d.reason,d.entity_kind,d.entity_id FROM memory_relations r"
                      " JOIN memory_dirty d ON d.entity_kind='code_ref' AND d.status='open'"
                      " AND ((r.target_kind='code_ref' AND r.target_id=d.entity_id"
                      "       AND r.source_kind=? AND r.source_id=?)"
                      "   OR (r.source_kind='code_ref' AND r.source_id=d.entity_id"
                      "       AND r.target_kind=? AND r.target_id=?))"
                      " WHERE r.recorded_to IS NULL ORDER BY 1,2,3;";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, kind);
    bind_text_or_null(stmt, 2, id);
    bind_text_or_null(stmt, 3, kind);
    bind_text_or_null(stmt, 4, id);
    bind_text_or_null(stmt, 5, kind);
    bind_text_or_null(stmt, 6, id);
    yyjson_mut_val *maintenance = yyjson_mut_obj(doc);
    yyjson_mut_val *reasons = yyjson_mut_arr(doc);
    bool dirty = false;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        dirty = true;
        yyjson_mut_val *reason = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(
            doc, reason, "reason",
            sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "");
        yyjson_mut_obj_add_strcpy(
            doc, reason, "source_kind",
            sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1) : "");
        yyjson_mut_obj_add_strcpy(
            doc, reason, "source_id",
            sqlite3_column_text(stmt, 2) ? (const char *)sqlite3_column_text(stmt, 2) : "");
        yyjson_mut_arr_add_val(reasons, reason);
    }
    sqlite3_finalize(stmt);
    yyjson_mut_obj_add_bool(doc, maintenance, "dirty", dirty);
    yyjson_mut_obj_add_val(doc, maintenance, "reasons", reasons);
    yyjson_mut_obj_add_val(doc, item, "maintenance", maintenance);
    *is_dirty = dirty;
    return 0;
}

static int add_claim_temporal_fields(cbm_memory_t *m, yyjson_mut_doc *doc, yyjson_mut_val *item,
                                     const char *claim_id, bool *is_hypothesis,
                                     char claim_kind_out[32]) {
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT claim_kind,scope_json,valid_from,valid_to,recorded_from,recorded_to,"
        " observed_at,review_after,volatility,revision FROM memory_claims WHERE claim_id=?;";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, claim_id);
    if (claim_kind_out) {
        claim_kind_out[0] = '\0';
    }
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        if (claim_kind_out) {
            (void)snprintf(claim_kind_out, 32, "%s",
                           sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0)
                                                        : "");
        }
        if (is_hypothesis) {
            *is_hypothesis = sqlite3_column_text(stmt, 0) &&
                             strcmp((const char *)sqlite3_column_text(stmt, 0), "hypothesis") == 0;
        }
        yyjson_mut_obj_add_strcpy(
            doc, item, "claim_kind",
            sqlite3_column_text(stmt, 0) ? (const char *)sqlite3_column_text(stmt, 0) : "");
        const char *scope =
            sqlite3_column_text(stmt, 1) ? (const char *)sqlite3_column_text(stmt, 1) : "{}";
        yyjson_mut_obj_add_val(doc, item, "scope", yyjson_mut_rawcpy(doc, scope));
        static const char *const names[] = {"valid_from",  "valid_to",    "recorded_from",
                                            "recorded_to", "observed_at", "review_after",
                                            "volatility"};
        for (int i = 0; i < 7; i++) {
            int column = i + 2;
            if (sqlite3_column_type(stmt, column) != SQLITE_NULL) {
                yyjson_mut_obj_add_strcpy(doc, item, names[i],
                                          (const char *)sqlite3_column_text(stmt, column));
            }
        }
        yyjson_mut_obj_add_int(doc, item, "revision", sqlite3_column_int(stmt, 9));
    }
    sqlite3_finalize(stmt);
    return 0;
}

static bool entity_has_project_code_ref(cbm_memory_t *m, const char *kind, const char *id,
                                        const char *project) {
    if (!project || !project[0]) {
        return false;
    }
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT 1 FROM memory_relations r JOIN memory_code_refs c ON"
        " ((r.target_kind='code_ref' AND r.target_id=c.code_ref_id AND r.source_kind=?"
        "   AND r.source_id=?) OR"
        "  (r.source_kind='code_ref' AND r.source_id=c.code_ref_id AND r.target_kind=?"
        "   AND r.target_id=?))"
        " WHERE r.recorded_to IS NULL AND c.project=? LIMIT 1;";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return false;
    }
    bind_text_or_null(stmt, 1, kind);
    bind_text_or_null(stmt, 2, id);
    bind_text_or_null(stmt, 3, kind);
    bind_text_or_null(stmt, 4, id);
    bind_text_or_null(stmt, 5, project);
    bool matched = sqlite3_step(stmt) == SQLITE_ROW;
    sqlite3_finalize(stmt);
    return matched;
}

char *cbm_memory_query_json(cbm_memory_t *m, const char *args_json) {
    if (!m) {
        return json_error("memory_unavailable", "memory handle is null");
    }
    yyjson_val *args = NULL;
    yyjson_doc *args_doc = parse_object_args(args_json, &args);
    if (!args_doc) {
        return json_error("invalid_arguments", "arguments must be a JSON object");
    }
    const char *mode = json_str(args, "mode");
    if (!mode) {
        mode = "search";
    }
    static const char *const query_modes[] = {"search", "get",      "overview", "neighbors",
                                              "path",   "timeline", "as_of",    "as-of"};
    bool known_mode = false;
    for (size_t i = 0; i < sizeof(query_modes) / sizeof(query_modes[0]); i++) {
        known_mode |= strcmp(mode, query_modes[i]) == 0;
    }
    if (!known_mode) {
        yyjson_doc_free(args_doc);
        return json_error("invalid_mode", "unsupported memory query mode");
    }
    yyjson_val *current_context = yyjson_obj_get(args, "current_context");
    if (current_context && !yyjson_is_obj(current_context)) {
        yyjson_doc_free(args_doc);
        return json_error("invalid_current_context", "current_context must be a JSON object");
    }
    const char *context_project = current_context ? json_str(current_context, "project") : NULL;
    const char *requested_kind = json_str(args, "entity_kind");
    int limit = (int)json_int(args, "limit", MEM_DEFAULT_LIMIT);
    if (limit < 1) {
        limit = 1;
    }
    if (limit > MEM_MAX_LIMIT) {
        limit = MEM_MAX_LIMIT;
    }
    int64_t snapshot = cbm_memory_snapshot_epoch(m);
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *items = doc ? yyjson_mut_arr(doc) : NULL;
    yyjson_mut_val *global_matched = doc ? yyjson_mut_arr(doc) : NULL;
    yyjson_mut_val *global_mismatched = doc ? yyjson_mut_arr(doc) : NULL;
    yyjson_mut_val *global_unknown = doc ? yyjson_mut_arr(doc) : NULL;
    if (!doc || !root || !items || !global_matched || !global_mismatched || !global_unknown) {
        yyjson_doc_free(args_doc);
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_bool(doc, root, "ok", true);
    yyjson_mut_obj_add_strcpy(doc, root, "mode", mode);
    yyjson_mut_obj_add_int(doc, root, "snapshot_epoch", snapshot);
    yyjson_mut_obj_add_strcpy(doc, root, "score_semantics", "retrieval_relevance_not_truth");

    int count = 0;
    bool stale = false;
    bool contested = false;
    bool retracted = false;
    bool has_unknown_applicability = false;
    bool has_mismatched_applicability = false;
    bool has_hypothesis = false;
    bool has_unverified = false;
    bool has_dirty = false;
    bool has_applicable_unknown = false;
    int candidate_count = 0;
    int mismatched_candidate_count = 0;
    bool route_candidate_set = false;
    bool route_stale = false;
    bool route_contested = false;
    bool route_retracted = false;
    bool route_hypothesis = false;
    bool route_unverified = false;
    bool route_dirty = false;
    bool route_unknown = false;
    char now[MEM_TIME_MAX];
    iso_now(now);
    sqlite3_stmt *stmt = NULL;
    int query_rc = 0;
    if (strcmp(mode, "overview") == 0) {
        query_rc = query_overview(m, doc, root);
    } else if (strcmp(mode, "neighbors") == 0) {
        const char *id = json_str(args, "id");
        if (!id) {
            id = json_str(args, "entity_id");
        }
        if (!id ||
            memory_prepare(m,
                           "SELECT relation_id,source_kind,source_id,target_kind,target_id,type,"
                           " properties_json FROM memory_relations WHERE recorded_to IS NULL"
                           " AND (source_id=? OR target_id=?) ORDER BY type,relation_id LIMIT ?;",
                           &stmt) != 0) {
            query_rc = -1;
        } else {
            bind_text_or_null(stmt, 1, id);
            bind_text_or_null(stmt, 2, id);
            sqlite3_bind_int(stmt, 3, limit);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                add_relation_row(doc, items, stmt);
                count++;
            }
            sqlite3_finalize(stmt);
        }
    } else if (strcmp(mode, "path") == 0) {
        const char *start_id = json_str(args, "start_id");
        const char *target_id = json_str(args, "target_id");
        int hops = (int)json_int(args, "hops", 3);
        if (hops < 1) {
            hops = 1;
        }
        if (hops > 6) {
            hops = 6;
        }
        const char *sql =
            "WITH RECURSIVE walk(id,depth,path) AS (VALUES(?,0,?) UNION ALL"
            " SELECT r.target_id,w.depth+1,w.path||'>'||r.target_id"
            " FROM walk w JOIN memory_relations r ON r.source_id=w.id"
            " WHERE r.recorded_to IS NULL AND w.depth<? AND instr(w.path,r.target_id)=0)"
            " SELECT id,depth,path FROM walk WHERE (?='' OR id=?) ORDER BY depth LIMIT ?;";
        if (!start_id || memory_prepare(m, sql, &stmt) != 0) {
            query_rc = -1;
        } else {
            bind_text_or_null(stmt, 1, start_id);
            bind_text_or_null(stmt, 2, start_id);
            sqlite3_bind_int(stmt, 3, hops);
            bind_text_or_null(stmt, 4, target_id ? target_id : "");
            bind_text_or_null(stmt, 5, target_id ? target_id : "");
            sqlite3_bind_int(stmt, 6, limit);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                yyjson_mut_val *item = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, item, "id",
                                          (const char *)sqlite3_column_text(stmt, 0));
                yyjson_mut_obj_add_int(doc, item, "depth", sqlite3_column_int(stmt, 1));
                yyjson_mut_obj_add_strcpy(doc, item, "path",
                                          (const char *)sqlite3_column_text(stmt, 2));
                yyjson_mut_arr_add_val(items, item);
                count++;
            }
            sqlite3_finalize(stmt);
        }
    } else if (strcmp(mode, "timeline") == 0) {
        const char *id = json_str(args, "id");
        if (!id) {
            id = json_str(args, "entity_id");
        }
        const char *sql =
            "SELECT 'page_revision',revision,created_at,revision_id FROM memory_revisions"
            " WHERE page_id=? UNION ALL SELECT 'claim_revision',revision,recorded_to,claim_id"
            " FROM memory_claim_revisions WHERE claim_id=? UNION ALL"
            " SELECT 'claim_current',revision,recorded_from,claim_id FROM memory_claims"
            " WHERE claim_id=? ORDER BY 3 DESC LIMIT ?;";
        if (!id || memory_prepare(m, sql, &stmt) != 0) {
            query_rc = -1;
        } else {
            bind_text_or_null(stmt, 1, id);
            bind_text_or_null(stmt, 2, id);
            bind_text_or_null(stmt, 3, id);
            sqlite3_bind_int(stmt, 4, limit);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                yyjson_mut_val *item = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, item, "event",
                                          (const char *)sqlite3_column_text(stmt, 0));
                yyjson_mut_obj_add_int(doc, item, "revision", sqlite3_column_int(stmt, 1));
                yyjson_mut_obj_add_strcpy(doc, item, "at",
                                          (const char *)sqlite3_column_text(stmt, 2));
                yyjson_mut_obj_add_strcpy(doc, item, "id",
                                          (const char *)sqlite3_column_text(stmt, 3));
                yyjson_mut_arr_add_val(items, item);
                count++;
            }
            sqlite3_finalize(stmt);
        }
    } else if (strcmp(mode, "as_of") == 0 || strcmp(mode, "as-of") == 0) {
        const char *as_of_id = json_str(args, "id");
        if (!as_of_id) {
            as_of_id = json_str(args, "entity_id");
        }
        const char *valid_at = json_str(args, "valid_at");
        if (!valid_at) {
            valid_at = json_str(args, "as_of");
        }
        const char *known_at = json_str(args, "known_at");
        if (!valid_at) {
            valid_at = now;
        }
        if (!known_at) {
            known_at = now;
        }
        const char *sql =
            "WITH versions AS ("
            " SELECT claim_id,revision,claim_kind,subject,predicate,object_text,status,scope_json,"
            " valid_from,valid_to,recorded_from,recorded_to,observed_at,review_after,volatility"
            " FROM memory_claims UNION ALL"
            " SELECT claim_id,revision,claim_kind,subject,predicate,object_text,status,scope_json,"
            " valid_from,valid_to,recorded_from,recorded_to,observed_at,review_after,volatility"
            " FROM memory_claim_revisions)"
            " SELECT claim_id,claim_kind,subject,predicate,object_text,status,scope_json,"
            " valid_from,valid_to,recorded_from,recorded_to,observed_at,review_after,volatility,"
            " revision FROM versions WHERE (valid_from IS NULL OR valid_from<=?)"
            " AND (valid_to IS NULL OR valid_to>?) AND recorded_from<=?"
            " AND (recorded_to IS NULL OR recorded_to>?)"
            " AND (?='' OR claim_id=?)"
            " ORDER BY recorded_from DESC,claim_id LIMIT ?;";
        if (memory_prepare(m, sql, &stmt) != 0) {
            query_rc = -1;
        } else {
            bind_text_or_null(stmt, 1, valid_at);
            bind_text_or_null(stmt, 2, valid_at);
            bind_text_or_null(stmt, 3, known_at);
            bind_text_or_null(stmt, 4, known_at);
            bind_text_or_null(stmt, 5, as_of_id ? as_of_id : "");
            bind_text_or_null(stmt, 6, as_of_id ? as_of_id : "");
            sqlite3_bind_int(stmt, 7, limit);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                yyjson_mut_val *item = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, item, "id",
                                          (const char *)sqlite3_column_text(stmt, 0));
                yyjson_mut_obj_add_strcpy(doc, item, "claim_kind",
                                          (const char *)sqlite3_column_text(stmt, 1));
                yyjson_mut_obj_add_strcpy(doc, item, "subject",
                                          (const char *)sqlite3_column_text(stmt, 2));
                yyjson_mut_obj_add_strcpy(doc, item, "predicate",
                                          (const char *)sqlite3_column_text(stmt, 3));
                yyjson_mut_obj_add_strcpy(doc, item, "object",
                                          (const char *)sqlite3_column_text(stmt, 4));
                const char *row_status = (const char *)sqlite3_column_text(stmt, 5);
                yyjson_mut_obj_add_strcpy(doc, item, "status", row_status);
                const char *scope = sqlite3_column_text(stmt, 6)
                                        ? (const char *)sqlite3_column_text(stmt, 6)
                                        : "{}";
                yyjson_mut_obj_add_val(doc, item, "scope", yyjson_mut_rawcpy(doc, scope));
                static const char *const temporal_names[] = {
                    "valid_from",  "valid_to",     "recorded_from", "recorded_to",
                    "observed_at", "review_after", "volatility"};
                for (int i = 0; i < 7; i++) {
                    int column = i + 7;
                    if (sqlite3_column_type(stmt, column) != SQLITE_NULL) {
                        yyjson_mut_obj_add_strcpy(doc, item, temporal_names[i],
                                                  (const char *)sqlite3_column_text(stmt, column));
                    }
                }
                yyjson_mut_obj_add_int(doc, item, "revision", sqlite3_column_int(stmt, 14));
                bool item_mismatch = false;
                bool item_unknown = false;
                bool item_contested = row_status && strcmp(row_status, "contested") == 0;
                bool item_dirty = false;
                bool has_supported_evidence = false;
                bool has_basis = false;
                if (enrich_applicability(
                        m, doc, item, "claim", (const char *)sqlite3_column_text(stmt, 0),
                        current_context, global_matched, global_mismatched, global_unknown,
                        &has_mismatched_applicability, &has_unknown_applicability, &item_mismatch,
                        &item_unknown) != 0 ||
                    enrich_evidence_and_conflicts(
                        m, doc, item, "claim", (const char *)sqlite3_column_text(stmt, 0),
                        &item_contested, &has_supported_evidence, &has_basis) != 0 ||
                    enrich_dirty_state(m, doc, item, "claim",
                                       (const char *)sqlite3_column_text(stmt, 0),
                                       &item_dirty) != 0) {
                    query_rc = -1;
                    break;
                }
                const char *row_claim_kind = (const char *)sqlite3_column_text(stmt, 1);
                bool live_status = row_status && (strcmp(row_status, "active") == 0 ||
                                                  strcmp(row_status, "unverified") == 0);
                bool missing_evidence =
                    live_status && row_claim_kind &&
                    ((strcmp(row_claim_kind, "fact") == 0 && !has_supported_evidence) ||
                     (strcmp(row_claim_kind, "inference") == 0 && !has_basis));
                if (missing_evidence) {
                    yyjson_mut_obj_add_strcpy(doc, item, "epistemic_quality", "missing_evidence");
                }
                yyjson_mut_obj_add_strcpy(doc, item, "applicability_state",
                                          item_mismatch ? "mismatched"
                                                        : (item_unknown ? "unknown" : "matched"));
                yyjson_mut_arr_add_val(items, item);
                candidate_count++;
                mismatched_candidate_count += item_mismatch;
                if (!item_mismatch) {
                    stale |= row_status && strcmp(row_status, "stale") == 0;
                    contested |= item_contested;
                    retracted |= row_status && strcmp(row_status, "retracted") == 0;
                    has_hypothesis |=
                        sqlite3_column_text(stmt, 1) &&
                        strcmp((const char *)sqlite3_column_text(stmt, 1), "hypothesis") == 0;
                    has_unverified |=
                        (row_status && strcmp(row_status, "unverified") == 0) || missing_evidence;
                    has_dirty |= item_dirty;
                    has_applicable_unknown |= item_unknown;
                    if (!route_candidate_set) {
                        route_candidate_set = true;
                        route_stale = row_status && strcmp(row_status, "stale") == 0;
                        route_contested = item_contested;
                        route_retracted = row_status && strcmp(row_status, "retracted") == 0;
                        route_hypothesis =
                            sqlite3_column_text(stmt, 1) &&
                            strcmp((const char *)sqlite3_column_text(stmt, 1), "hypothesis") == 0;
                        route_unverified = (row_status && strcmp(row_status, "unverified") == 0) ||
                                           missing_evidence;
                        route_dirty = item_dirty;
                        route_unknown = item_unknown;
                    }
                }
                count++;
            }
            sqlite3_finalize(stmt);
        }
    } else {
        const char *id = json_str(args, "id");
        if (!id) {
            id = json_str(args, "entity_id");
        }
        const char *query = json_str(args, "query");
        bool get_mode = strcmp(mode, "get") == 0;
        char *fts = get_mode ? NULL : fts_expression(query);
        bool sql_project_boost = !get_mode && context_project && context_project[0];
        const char *sql = NULL;
        if (get_mode) {
            sql = "SELECT entity_kind,entity_id,title,summary,body,0.0,0"
                  " FROM memory_documents WHERE entity_id=?"
                  " AND (?='' OR entity_kind=?) LIMIT 1;";
        } else if (fts && sql_project_boost) {
            sql = "SELECT d.entity_kind,d.entity_id,d.title,d.summary,"
                  " snippet(memory_fts,2,'','',' … ',24),bm25(memory_fts),"
                  " EXISTS(SELECT 1 FROM memory_relations r JOIN memory_code_refs c ON"
                  " ((r.target_kind='code_ref' AND r.target_id=c.code_ref_id"
                  "   AND r.source_kind=d.entity_kind AND r.source_id=d.entity_id) OR"
                  "  (r.source_kind='code_ref' AND r.source_id=c.code_ref_id"
                  "   AND r.target_kind=d.entity_kind AND r.target_id=d.entity_id))"
                  " WHERE r.recorded_to IS NULL AND c.project=?) AS project_match"
                  " FROM memory_fts JOIN memory_documents d ON d.doc_id=memory_fts.rowid"
                  " WHERE memory_fts MATCH ? AND (?='' OR d.entity_kind=?)"
                  " ORDER BY project_match DESC,bm25(memory_fts) LIMIT ?;";
        } else if (fts) {
            sql = "SELECT d.entity_kind,d.entity_id,d.title,d.summary,"
                  " snippet(memory_fts,2,'','',' … ',24),bm25(memory_fts),0"
                  " FROM memory_fts JOIN memory_documents d"
                  " ON d.doc_id=memory_fts.rowid WHERE memory_fts MATCH ?"
                  " AND (?='' OR d.entity_kind=?)"
                  " ORDER BY bm25(memory_fts) LIMIT ?;";
        } else if (sql_project_boost) {
            sql = "SELECT d.entity_kind,d.entity_id,d.title,d.summary,d.body,0.0,"
                  " EXISTS(SELECT 1 FROM memory_relations r JOIN memory_code_refs c ON"
                  " ((r.target_kind='code_ref' AND r.target_id=c.code_ref_id"
                  "   AND r.source_kind=d.entity_kind AND r.source_id=d.entity_id) OR"
                  "  (r.source_kind='code_ref' AND r.source_id=c.code_ref_id"
                  "   AND r.target_kind=d.entity_kind AND r.target_id=d.entity_id))"
                  " WHERE r.recorded_to IS NULL AND c.project=?) AS project_match"
                  " FROM memory_documents d WHERE (?='' OR d.entity_kind=?)"
                  " ORDER BY project_match DESC,d.doc_id DESC LIMIT ?;";
        } else {
            sql = "SELECT entity_kind,entity_id,title,summary,body,0.0,0"
                  " FROM memory_documents WHERE (?='' OR entity_kind=?)"
                  " ORDER BY doc_id DESC LIMIT ?;";
        }
        if ((get_mode && !id) || memory_prepare(m, sql, &stmt) != 0) {
            query_rc = -1;
        } else {
            int bind_index = 1;
            if (get_mode) {
                bind_text_or_null(stmt, bind_index++, id);
                bind_text_or_null(stmt, bind_index++, requested_kind ? requested_kind : "");
                bind_text_or_null(stmt, bind_index++, requested_kind ? requested_kind : "");
            } else {
                if (sql_project_boost) {
                    bind_text_or_null(stmt, bind_index++, context_project);
                }
                if (fts) {
                    bind_text_or_null(stmt, bind_index++, fts);
                }
                bind_text_or_null(stmt, bind_index++, requested_kind ? requested_kind : "");
                bind_text_or_null(stmt, bind_index++, requested_kind ? requested_kind : "");
                sqlite3_bind_int(stmt, bind_index++, limit);
            }
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *kind = (const char *)sqlite3_column_text(stmt, 0);
                const char *entity_id = (const char *)sqlite3_column_text(stmt, 1);
                yyjson_mut_val *item = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, item, "kind", kind ? kind : "");
                yyjson_mut_obj_add_strcpy(doc, item, "id", entity_id ? entity_id : "");
                yyjson_mut_obj_add_strcpy(
                    doc, item, "title",
                    sqlite3_column_text(stmt, 2) ? (const char *)sqlite3_column_text(stmt, 2) : "");
                yyjson_mut_obj_add_strcpy(
                    doc, item, "summary",
                    sqlite3_column_text(stmt, 3) ? (const char *)sqlite3_column_text(stmt, 3) : "");
                yyjson_mut_obj_add_strcpy(
                    doc, item, get_mode ? "body" : "snippet",
                    sqlite3_column_text(stmt, 4) ? (const char *)sqlite3_column_text(stmt, 4) : "");
                double base_score = -sqlite3_column_double(stmt, 5);
                bool project_match = sqlite3_column_int(stmt, 6) != 0;
                if (!project_match && context_project) {
                    project_match = entity_has_project_code_ref(
                        m, kind ? kind : "", entity_id ? entity_id : "", context_project);
                }
                yyjson_mut_obj_add_real(doc, item, "score", base_score);
                yyjson_mut_obj_add_real(doc, item, "ranking_boost", project_match ? 1.0 : 0.0);
                yyjson_mut_obj_add_real(doc, item, "ranking_score",
                                        base_score + (project_match ? 1.0 : 0.0));
                yyjson_mut_val *boost_reasons = yyjson_mut_arr(doc);
                if (project_match) {
                    yyjson_mut_arr_add_strcpy(doc, boost_reasons, "current_project_code_reference");
                }
                yyjson_mut_obj_add_val(doc, item, "boost_reasons", boost_reasons);
                char row_status[32];
                char review_after[MEM_TIME_MAX];
                bool item_stale = false;
                bool item_contested = false;
                bool item_retracted = false;
                bool item_unverified = false;
                bool item_hypothesis = false;
                bool item_mismatch = false;
                bool item_unknown = false;
                bool item_dirty = false;
                bool has_supported_evidence = false;
                bool has_basis = false;
                char item_claim_kind[32] = "";
                if (entity_state(m, kind ? kind : "", entity_id ? entity_id : "", row_status,
                                 review_after) == 0) {
                    if (row_status[0]) {
                        yyjson_mut_obj_add_strcpy(doc, item, "status", row_status);
                    }
                    if (review_after[0]) {
                        yyjson_mut_obj_add_strcpy(doc, item, "review_after", review_after);
                    }
                    item_stale = strcmp(row_status, "stale") == 0 ||
                                 (review_after[0] && strcmp(review_after, now) < 0);
                    item_contested = strcmp(row_status, "contested") == 0;
                    item_retracted = strcmp(row_status, "retracted") == 0;
                    item_unverified = strcmp(row_status, "unverified") == 0;
                }
                if (kind && entity_id && strcmp(kind, "claim") == 0 &&
                    add_claim_temporal_fields(m, doc, item, entity_id, &item_hypothesis,
                                              item_claim_kind) != 0) {
                    query_rc = -1;
                    break;
                }
                if (kind && entity_id &&
                    (enrich_applicability(m, doc, item, kind, entity_id, current_context,
                                          global_matched, global_mismatched, global_unknown,
                                          &has_mismatched_applicability, &has_unknown_applicability,
                                          &item_mismatch, &item_unknown) != 0 ||
                     enrich_evidence_and_conflicts(m, doc, item, kind, entity_id, &item_contested,
                                                   &has_supported_evidence, &has_basis) != 0 ||
                     enrich_dirty_state(m, doc, item, kind, entity_id, &item_dirty) != 0)) {
                    query_rc = -1;
                    break;
                }
                bool live_status =
                    strcmp(row_status, "active") == 0 || strcmp(row_status, "unverified") == 0;
                bool missing_evidence =
                    kind && strcmp(kind, "claim") == 0 && live_status &&
                    ((strcmp(item_claim_kind, "fact") == 0 && !has_supported_evidence) ||
                     (strcmp(item_claim_kind, "inference") == 0 && !has_basis));
                if (missing_evidence) {
                    item_unverified = true;
                    yyjson_mut_obj_add_strcpy(doc, item, "epistemic_quality", "missing_evidence");
                }
                yyjson_mut_obj_add_strcpy(doc, item, "applicability_state",
                                          item_mismatch ? "mismatched"
                                                        : (item_unknown ? "unknown" : "matched"));
                yyjson_mut_arr_add_val(items, item);
                candidate_count++;
                mismatched_candidate_count += item_mismatch;
                if (!item_mismatch) {
                    stale |= item_stale;
                    contested |= item_contested;
                    retracted |= item_retracted;
                    has_unverified |= item_unverified;
                    has_hypothesis |= item_hypothesis;
                    has_dirty |= item_dirty;
                    has_applicable_unknown |= item_unknown;
                    if (!route_candidate_set) {
                        route_candidate_set = true;
                        route_stale = item_stale;
                        route_contested = item_contested;
                        route_retracted = item_retracted;
                        route_hypothesis = item_hypothesis;
                        route_unverified = item_unverified;
                        route_dirty = item_dirty;
                        route_unknown = item_unknown;
                    }
                }
                count++;
            }
            if (sqlite3_errcode(m->db) != SQLITE_OK && sqlite3_errcode(m->db) != SQLITE_DONE) {
                query_rc = -1;
            }
            sqlite3_finalize(stmt);
        }
        free(fts);
    }

    if (query_rc != 0) {
        yyjson_doc_free(args_doc);
        yyjson_mut_doc_free(doc);
        return json_error("query_failed", sqlite3_errmsg(m->db));
    }
    yyjson_mut_obj_add_val(doc, root, "results", items);
    yyjson_mut_obj_add_int(doc, root, "count", count);

    yyjson_mut_val *applicability = yyjson_mut_obj(doc);
    if (stale) {
        yyjson_mut_arr_add_strcpy(doc, global_mismatched, "freshness:review_due");
    }
    yyjson_mut_obj_add_val(doc, applicability, "matched", global_matched);
    yyjson_mut_obj_add_val(doc, applicability, "mismatched", global_mismatched);
    yyjson_mut_obj_add_val(doc, applicability, "unknown", global_unknown);
    yyjson_mut_obj_add_val(doc, root, "applicability", applicability);

    const char *freshness = json_str(args, "freshness");
    const char *impact = json_str(args, "impact");
    bool reversible = json_bool(args, "reversible", false);
    bool require_current = freshness && strcmp(freshness, "require_current") == 0;
    bool high_impact = impact && strcmp(impact, "high") == 0;
    bool all_candidates_mismatched =
        candidate_count > 0 && mismatched_candidate_count == candidate_count;
    bool selected_stale = route_candidate_set ? route_stale : stale;
    bool selected_contested = route_candidate_set ? route_contested : contested;
    bool selected_retracted = route_candidate_set ? route_retracted : retracted;
    bool selected_hypothesis = route_candidate_set ? route_hypothesis : has_hypothesis;
    bool selected_unverified = route_candidate_set ? route_unverified : has_unverified;
    bool selected_dirty = route_candidate_set ? route_dirty : has_dirty;
    bool selected_unknown = route_candidate_set ? route_unknown : has_applicable_unknown;
    const char *route = "reuse";
    if (count == 0 && strcmp(mode, "overview") != 0) {
        route = "abstain";
    } else if (all_candidates_mismatched || selected_retracted ||
               ((selected_stale || selected_dirty) && require_current)) {
        route = "abstain";
    } else if ((selected_contested || selected_unknown || selected_hypothesis ||
                selected_unverified || selected_stale || selected_dirty) &&
               high_impact) {
        route = "deliberate";
    } else if ((selected_hypothesis || selected_contested || selected_unknown) && reversible) {
        route = "experiment";
    } else if (selected_contested || selected_unknown || selected_hypothesis ||
               selected_unverified || selected_dirty) {
        route = "verify";
    } else if (selected_stale) {
        route = "verify";
    }
    yyjson_mut_obj_add_strcpy(doc, root, "route", route);
    yyjson_mut_val *warnings = yyjson_mut_arr(doc);
    if (stale) {
        yyjson_mut_arr_add_strcpy(doc, warnings, "stale_or_review_due_memory");
    }
    if (contested) {
        yyjson_mut_arr_add_strcpy(doc, warnings, "unresolved_contested_claim");
    }
    if (retracted) {
        yyjson_mut_arr_add_strcpy(doc, warnings, "retracted_memory");
    }
    if (has_mismatched_applicability) {
        yyjson_mut_arr_add_strcpy(doc, warnings, "memory_not_applicable_to_current_context");
    }
    if (has_unknown_applicability) {
        yyjson_mut_arr_add_strcpy(doc, warnings, "memory_applicability_unknown");
    }
    if (has_dirty) {
        yyjson_mut_arr_add_strcpy(doc, warnings, "memory_requires_maintenance_review");
    }
    yyjson_mut_obj_add_val(doc, root, "warnings", warnings);

    char *out = json_write_mut(doc);
    yyjson_doc_free(args_doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* ── Epistemic / temporal / graph maintenance ─────────────────── */

static void lint_add_issue(yyjson_mut_doc *doc, yyjson_mut_val *issues, const char *code,
                           const char *severity, const char *kind, const char *id,
                           const char *detail) {
    yyjson_mut_val *issue = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, issue, "code", code);
    yyjson_mut_obj_add_strcpy(doc, issue, "severity", severity);
    yyjson_mut_obj_add_strcpy(doc, issue, "entity_kind", kind ? kind : "");
    yyjson_mut_obj_add_strcpy(doc, issue, "entity_id", id ? id : "");
    yyjson_mut_obj_add_strcpy(doc, issue, "detail", detail ? detail : "");
    yyjson_mut_arr_add_val(issues, issue);
}

typedef struct {
    const char *code;
    const char *severity;
    const char *kind;
    const char *sql;
    const char *detail;
} lint_sql_check_t;

static bool lint_check_selected(yyjson_val *requested, const char *code) {
    if (!requested) {
        return true;
    }
    size_t index;
    size_t max;
    yyjson_val *value;
    yyjson_arr_foreach(requested, index, max, value) {
        if (yyjson_is_str(value) && strcmp(yyjson_get_str(value), code) == 0) {
            return true;
        }
    }
    return false;
}

char *cbm_memory_lint_json(cbm_memory_t *m, const char *args_json) {
    if (!m) {
        return json_error("memory_unavailable", "memory handle is null");
    }
    yyjson_val *args = NULL;
    yyjson_doc *args_doc = parse_object_args(args_json, &args);
    if (!args_doc) {
        return json_error("invalid_arguments", "arguments must be a JSON object");
    }
    if (json_bool(args, "apply", false)) {
        yyjson_doc_free(args_doc);
        return json_error("apply_not_supported",
                          "lint is read-only; repairs must be committed through a proposal");
    }
    yyjson_val *requested_checks = yyjson_obj_get(args, "checks");
    if (requested_checks && !yyjson_is_arr(requested_checks)) {
        yyjson_doc_free(args_doc);
        return json_error("invalid_checks", "checks must be an array of lint check names");
    }
    int limit = (int)json_int(args, "limit", MEM_MAX_LIMIT);
    if (limit < 1) {
        limit = 1;
    }
    if (limit > 1000) {
        limit = 1000;
    }
    static const lint_sql_check_t checks[] = {
        {"unsupported_fact", "error", "claim",
         "SELECT c.claim_id FROM memory_claims c WHERE c.claim_kind='fact'"
         " AND c.status IN ('active','unverified') AND c.recorded_to IS NULL"
         " AND NOT EXISTS(SELECT 1 FROM memory_relations r WHERE r.source_kind='claim'"
         " AND r.source_id=c.claim_id AND r.type='SUPPORTED_BY' AND r.target_kind IN"
         " ('source','experience') AND r.recorded_to IS NULL) LIMIT ?;",
         "fact has no RawSource or Experience support"},
        {"single_lineage_support", "warning", "claim",
         "WITH RECURSIVE lineage(source_id,current_id,depth,path) AS ("
         " SELECT source_id,source_id,0,'|'||source_id||'|' FROM memory_sources UNION ALL"
         " SELECT l.source_id,s.revision_of,l.depth+1,l.path||s.revision_of||'|'"
         " FROM lineage l JOIN memory_sources s ON s.source_id=l.current_id"
         " WHERE s.revision_of IS NOT NULL AND l.depth<64"
         " AND instr(l.path,'|'||s.revision_of||'|')=0), roots(source_id,root_id) AS ("
         " SELECT l.source_id,l.current_id FROM lineage l JOIN memory_sources s"
         " ON s.source_id=l.current_id WHERE s.revision_of IS NULL)"
         " SELECT c.claim_id FROM memory_claims c"
         " JOIN memory_relations r ON r.source_kind='claim' AND r.source_id=c.claim_id"
         "  AND r.type='SUPPORTED_BY' AND r.target_kind='source' AND r.recorded_to IS NULL"
         " JOIN roots s ON s.source_id=r.target_id"
         " WHERE c.claim_kind='fact' AND c.status='active' AND c.recorded_to IS NULL"
         " GROUP BY c.claim_id HAVING count(DISTINCT s.root_id)=1"
         " LIMIT ?;",
         "active fact is supported by only one independent source lineage"},
        {"source_revision_cycle", "error", "source",
         "WITH RECURSIVE lineage(start_id,current_id,depth) AS ("
         " SELECT source_id,revision_of,1 FROM memory_sources WHERE revision_of IS NOT NULL"
         " UNION ALL SELECT l.start_id,s.revision_of,l.depth+1 FROM lineage l"
         " JOIN memory_sources s ON s.source_id=l.current_id"
         " WHERE s.revision_of IS NOT NULL AND l.depth<64)"
         " SELECT start_id FROM lineage WHERE current_id=start_id AND depth>1 LIMIT ?;",
         "RawSource REVISION_OF lineage contains a cycle"},
        {"volatile_fact_without_review", "warning", "claim",
         "SELECT claim_id FROM memory_claims WHERE claim_kind='fact'"
         " AND status IN ('active','unverified') AND recorded_to IS NULL"
         " AND lower(volatility) IN ('high','volatile') AND review_after IS NULL LIMIT ?;",
         "volatile fact has no review deadline"},
        {"inference_without_basis", "warning", "claim",
         "SELECT c.claim_id FROM memory_claims c WHERE c.claim_kind='inference'"
         " AND c.recorded_to IS NULL AND NOT EXISTS(SELECT 1 FROM memory_relations r"
         " WHERE r.source_id=c.claim_id AND r.type IN ('SUPPORTED_BY','DECIDED_USING')"
         " AND r.recorded_to IS NULL) LIMIT ?;",
         "inference has no recorded basis"},
        {"stale_claim", "warning", "claim",
         "SELECT claim_id FROM memory_claims WHERE recorded_to IS NULL AND"
         " (status='stale' OR (review_after IS NOT NULL AND "
         "review_after<strftime('%Y-%m-%dT%H:%M:%SZ','now')))"
         " LIMIT ?;",
         "claim requires revalidation"},
        {"unresolved_contradiction", "warning", "claim",
         "SELECT source_id FROM memory_relations WHERE type='CONTRADICTS' AND recorded_to IS NULL"
         " LIMIT ?;",
         "contradictory claims remain unresolved"},
        {"temporal_overlap", "error", "claim",
         "SELECT a.claim_id FROM memory_claims a JOIN memory_claims b"
         " ON a.subject=b.subject AND a.predicate=b.predicate AND a.claim_id<b.claim_id"
         " WHERE a.scope_json=b.scope_json AND a.recorded_to IS NULL AND b.recorded_to IS NULL"
         " AND a.status='active' AND b.status='active'"
         " AND coalesce(a.valid_to,'9999')>coalesce(b.valid_from,'0000')"
         " AND coalesce(b.valid_to,'9999')>coalesce(a.valid_from,'0000') LIMIT ?;",
         "active same-scope claims have overlapping valid intervals"},
        {"orphan_page", "warning", "page",
         "SELECT p.page_id FROM memory_pages p WHERE NOT EXISTS("
         " SELECT 1 FROM memory_relations r WHERE r.recorded_to IS NULL AND"
         " r.type NOT IN ('HAS_REVISION','GENERATED','USED') AND"
         " ((r.source_kind='page' AND r.source_id=p.page_id) OR"
         " (r.target_kind='page' AND r.target_id=p.page_id))) LIMIT ?;",
         "wiki page has no knowledge-graph relationships"},
        {"duplicate_page_title", "warning", "page",
         "SELECT min(page_id) FROM memory_pages GROUP BY lower(title) HAVING count(*)>1 LIMIT ?;",
         "multiple pages share a case-insensitive title"},
        {"contextless_experience", "error", "experience",
         "SELECT experience_id FROM memory_experiences WHERE context_json='{}' LIMIT ?;",
         "experience has no applicability context"},
        {"decision_missing_alternatives", "warning", "decision",
         "SELECT decision_id FROM memory_decisions WHERE alternatives_json='[]'"
         " OR (review_after IS NULL AND exit_criteria_json='[]') LIMIT ?;",
         "decision lacks alternatives or review/exit criteria"},
        {"unscoped_preference", "warning", "preference",
         "SELECT preference_id FROM memory_preferences WHERE scope_json='{}' AND context_json='{}'"
         " LIMIT ?;",
         "preference is global because no scope/context was recorded"},
        {"wiki_self_support", "error", "claim",
         "SELECT source_id FROM memory_relations WHERE source_kind='claim' AND type='SUPPORTED_BY'"
         " AND target_kind IN ('page','revision') AND recorded_to IS NULL LIMIT ?;",
         "wiki synthesis cannot be independent evidence"},
        {"circular_relation", "error", "relation",
         "SELECT relation_id FROM memory_relations WHERE source_id=target_id"
         " AND type IN ('SUPPORTED_BY','REVISION_OF','SUPERSEDES') AND recorded_to IS NULL LIMIT "
         "?;",
         "self-referential epistemic relation"},
        {"supported_by_cycle", "error", "relation",
         "WITH RECURSIVE support(start_kind,start_id,kind,id,depth) AS ("
         " SELECT source_kind,source_id,target_kind,target_id,1 FROM memory_relations"
         " WHERE type='SUPPORTED_BY' AND recorded_to IS NULL UNION"
         " SELECT p.start_kind,p.start_id,r.target_kind,r.target_id,p.depth+1"
         " FROM support p JOIN memory_relations r ON r.source_kind=p.kind"
         " AND r.source_id=p.id WHERE r.type='SUPPORTED_BY' AND r.recorded_to IS NULL"
         " AND p.depth<16) SELECT start_id FROM support WHERE depth>1"
         " AND kind=start_kind AND id=start_id LIMIT ?;",
         "multi-node SUPPORTED_BY cycle makes evidence provenance circular"},
        {"broken_relation_endpoint", "error", "relation",
         "WITH entities(kind,id) AS ("
         " SELECT 'source',source_id FROM memory_sources UNION ALL"
         " SELECT 'page',page_id FROM memory_pages UNION ALL"
         " SELECT 'revision',revision_id FROM memory_revisions UNION ALL"
         " SELECT 'claim',claim_id FROM memory_claims UNION ALL"
         " SELECT 'decision',decision_id FROM memory_decisions UNION ALL"
         " SELECT 'experience',experience_id FROM memory_experiences UNION ALL"
         " SELECT 'preference',preference_id FROM memory_preferences UNION ALL"
         " SELECT 'code_ref',code_ref_id FROM memory_code_refs UNION ALL"
         " SELECT 'activity',activity_id FROM memory_activities)"
         " SELECT r.relation_id FROM memory_relations r WHERE r.recorded_to IS NULL"
         " AND (NOT EXISTS(SELECT 1 FROM entities e WHERE e.kind=r.source_kind"
         " AND e.id=r.source_id) OR NOT EXISTS(SELECT 1 FROM entities e"
         " WHERE e.kind=r.target_kind AND e.id=r.target_id)) LIMIT ?;",
         "relationship endpoint is missing from canonical memory"},
        {"single_agent_dominance", "warning", "page",
         "SELECT page_id FROM memory_revisions GROUP BY page_id"
         " HAVING count(DISTINCT author_agent)=1 AND count(*)>=3 LIMIT ?;",
         "all revisions were produced by one agent"},
        {"dirty_code_reference", "warning", "code_ref",
         "SELECT code_ref_id FROM memory_code_refs WHERE resolution_status IN"
         " ('missing','changed','unresolved') LIMIT ?;",
         "symbolic code reference is unresolved or changed"},
        {"pending_materialization", "warning", "page",
         "SELECT aggregate_id FROM memory_outbox WHERE state IN ('pending','leased','failed')"
         " LIMIT ?;",
         "committed wiki revision is not fully materialized"},
        {"conflicting_proposal", "warning", "proposal",
         "SELECT proposal_id FROM memory_proposals WHERE status='conflicted' LIMIT ?;",
         "proposal requires rebase or adjudication"},
        {"audit_priority", "info", "memory",
         "WITH degree(kind,id) AS ("
         " SELECT source_kind,source_id FROM memory_relations WHERE recorded_to IS NULL"
         " UNION ALL SELECT target_kind,target_id FROM memory_relations WHERE recorded_to IS NULL)"
         " SELECT id FROM degree GROUP BY kind,id HAVING count(*)>=5 LIMIT ?;",
         "highly connected or reused memory should be audited sooner; this is not truth evidence"},
    };

    if (requested_checks) {
        size_t index;
        size_t max;
        yyjson_val *value;
        yyjson_arr_foreach(requested_checks, index, max, value) {
            bool known = false;
            if (yyjson_is_str(value)) {
                const char *name = yyjson_get_str(value);
                known = strcmp(name, "broken_graph_projection") == 0 ||
                        strcmp(name, "missing_raw_object") == 0 ||
                        strcmp(name, "raw_object_hash_mismatch") == 0;
                for (size_t i = 0; i < sizeof(checks) / sizeof(checks[0]); i++) {
                    known |= strcmp(name, checks[i].code) == 0;
                }
            }
            if (!known) {
                yyjson_doc_free(args_doc);
                return json_error("invalid_checks", "checks contains an unknown lint check");
            }
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *issues = doc ? yyjson_mut_arr(doc) : NULL;
    if (!doc || !root || !issues) {
        yyjson_doc_free(args_doc);
        yyjson_mut_doc_free(doc);
        return NULL;
    }
    yyjson_mut_doc_set_root(doc, root);
    int issue_count = 0;
    for (size_t i = 0; i < sizeof(checks) / sizeof(checks[0]) && issue_count < limit; i++) {
        if (!lint_check_selected(requested_checks, checks[i].code)) {
            continue;
        }
        sqlite3_stmt *stmt = NULL;
        if (memory_prepare(m, checks[i].sql, &stmt) != 0) {
            yyjson_doc_free(args_doc);
            yyjson_mut_doc_free(doc);
            return json_error("lint_failed", m->error);
        }
        sqlite3_bind_int(stmt, 1, limit - issue_count);
        while (issue_count < limit && sqlite3_step(stmt) == SQLITE_ROW) {
            const char *id = (const char *)sqlite3_column_text(stmt, 0);
            lint_add_issue(doc, issues, checks[i].code, checks[i].severity, checks[i].kind,
                           id ? id : "", checks[i].detail);
            issue_count++;
        }
        sqlite3_finalize(stmt);
    }

    sqlite3_stmt *graph_stmt = NULL;
    if (issue_count < limit && lint_check_selected(requested_checks, "broken_graph_projection") &&
        memory_prepare(m,
                       "SELECT relation_id,source_kind,source_id,target_kind,target_id"
                       " FROM memory_relations WHERE recorded_to IS NULL ORDER BY relation_id;",
                       &graph_stmt) == 0) {
        while (issue_count < limit && sqlite3_step(graph_stmt) == SQLITE_ROW) {
            const char *relation_col = (const char *)sqlite3_column_text(graph_stmt, 0);
            const char *source_kind_col = (const char *)sqlite3_column_text(graph_stmt, 1);
            const char *source_id_col = (const char *)sqlite3_column_text(graph_stmt, 2);
            const char *target_kind_col = (const char *)sqlite3_column_text(graph_stmt, 3);
            const char *target_id_col = (const char *)sqlite3_column_text(graph_stmt, 4);
            char *relation_id = mem_strdup(relation_col ? relation_col : "");
            char *source_kind = mem_strdup(source_kind_col ? source_kind_col : "");
            char *source_id = mem_strdup(source_id_col ? source_id_col : "");
            char *target_kind = mem_strdup(target_kind_col ? target_kind_col : "");
            char *target_id = mem_strdup(target_id_col ? target_id_col : "");
            if (!relation_id || !source_kind || !source_id || !target_kind || !target_id) {
                free(relation_id);
                free(source_kind);
                free(source_id);
                free(target_kind);
                free(target_id);
                break;
            }
            if (graph_node_id(m, source_kind, source_id) <= 0 ||
                graph_node_id(m, target_kind, target_id) <= 0) {
                lint_add_issue(doc, issues, "broken_graph_projection", "error", "relation",
                               relation_id,
                               "relationship endpoint is missing from the memory graph projection");
                issue_count++;
            }
            free(relation_id);
            free(source_kind);
            free(source_id);
            free(target_kind);
            free(target_id);
        }
        sqlite3_finalize(graph_stmt);
    }

    sqlite3_stmt *source_stmt = NULL;
    if (issue_count < limit &&
        (lint_check_selected(requested_checks, "missing_raw_object") ||
         lint_check_selected(requested_checks, "raw_object_hash_mismatch")) &&
        memory_prepare(m,
                       "SELECT source_id,object_relpath,content_hash FROM memory_sources"
                       " ORDER BY source_id;",
                       &source_stmt) == 0) {
        while (issue_count < limit && sqlite3_step(source_stmt) == SQLITE_ROW) {
            const char *id = (const char *)sqlite3_column_text(source_stmt, 0);
            const char *rel = (const char *)sqlite3_column_text(source_stmt, 1);
            const char *expected_hash = (const char *)sqlite3_column_text(source_stmt, 2);
            char path[MEM_PATH_MAX];
            int n = snprintf(path, sizeof(path), "%s/%s", m->home, rel ? rel : "");
            if (n < 0 || n >= (int)sizeof(path) || !cbm_file_exists(path)) {
                if (lint_check_selected(requested_checks, "missing_raw_object")) {
                    lint_add_issue(doc, issues, "missing_raw_object", "error", "source", id,
                                   "canonical raw source object is missing");
                    issue_count++;
                }
            } else if (lint_check_selected(requested_checks, "raw_object_hash_mismatch")) {
                unsigned char *bytes = NULL;
                size_t len = 0;
                if (read_file_bytes(path, &bytes, &len) != 0) {
                    lint_add_issue(doc, issues, "raw_object_hash_mismatch", "error", "source", id,
                                   "canonical raw source object cannot be verified");
                    issue_count++;
                } else {
                    char actual_hash[CBM_SHA256_HEX_LEN + 1] = "";
                    cbm_sha256_hex(bytes, len, actual_hash);
                    if (!expected_hash || strcmp(actual_hash, expected_hash) != 0) {
                        lint_add_issue(doc, issues, "raw_object_hash_mismatch", "error", "source",
                                       id, "canonical raw source bytes do not match content_hash");
                        issue_count++;
                    }
                }
                free(bytes);
            }
        }
        sqlite3_finalize(source_stmt);
    }

    yyjson_mut_obj_add_bool(doc, root, "ok", true);
    yyjson_mut_obj_add_int(doc, root, "snapshot_epoch", cbm_memory_snapshot_epoch(m));
    yyjson_mut_obj_add_int(doc, root, "issue_count", issue_count);
    yyjson_mut_obj_add_val(doc, root, "issues", issues);
    char *out = json_write_mut(doc);
    yyjson_doc_free(args_doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static int dirty_insert(cbm_memory_t *m, const char *kind, const char *id, const char *reason,
                        const char *source_id, int64_t epoch, const char *details,
                        const char *now) {
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "INSERT OR IGNORE INTO memory_dirty(entity_kind,entity_id,reason,source_id,"
        " detected_epoch,status,details_json,created_at) VALUES(?,?,?,?,?,'open',?,?);";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, kind);
    bind_text_or_null(stmt, 2, id);
    bind_text_or_null(stmt, 3, reason);
    bind_text_or_null(stmt, 4, source_id ? source_id : "");
    sqlite3_bind_int64(stmt, 5, epoch);
    bind_text_or_null(stmt, 6, details ? details : "{}");
    bind_text_or_null(stmt, 7, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE ? 0 : -1;
}

static int dirty_source_dependents(cbm_memory_t *m, const char *old_source_id,
                                   const char *new_source_id, int64_t epoch, const char *now) {
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT DISTINCT source_kind,source_id FROM memory_relations"
                      " WHERE target_kind='source' AND target_id=? AND type IN"
                      " ('SUPPORTED_BY','DECIDED_USING','APPLIES_TO') AND recorded_to IS NULL;";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, old_source_id);
    int rc = 0;
    char details[CBM_SZ_512];
    (void)snprintf(details, sizeof(details),
                   "{\"previous_source_id\":\"%s\",\"new_source_id\":\"%s\"}",
                   old_source_id ? old_source_id : "", new_source_id ? new_source_id : "");
    if (dirty_insert(m, "source", old_source_id, "source_revision_available", new_source_id, epoch,
                     details, now) != 0) {
        sqlite3_finalize(stmt);
        return -1;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *kind_col = (const char *)sqlite3_column_text(stmt, 0);
        const char *id_col = (const char *)sqlite3_column_text(stmt, 1);
        char *kind = mem_strdup(kind_col ? kind_col : "");
        char *id = mem_strdup(id_col ? id_col : "");
        if (!kind || !id ||
            dirty_insert(m, kind, id, "source_revision_available", new_source_id, epoch, details,
                         now) != 0) {
            rc = -1;
        }
        free(kind);
        free(id);
        if (rc != 0) {
            break;
        }
    }
    sqlite3_finalize(stmt);
    return rc;
}

static int dirty_linked_to_code_ref(cbm_memory_t *m, const char *code_ref_id, const char *reason,
                                    int64_t epoch, const char *now) {
    sqlite3_stmt *stmt = NULL;
    const char *sql =
        "SELECT source_kind,source_id FROM memory_relations WHERE target_kind='code_ref'"
        " AND target_id=? AND recorded_to IS NULL UNION SELECT target_kind,target_id"
        " FROM memory_relations WHERE source_kind='code_ref' AND source_id=?"
        " AND recorded_to IS NULL;";
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, code_ref_id);
    bind_text_or_null(stmt, 2, code_ref_id);
    int rc = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *kind_col = (const char *)sqlite3_column_text(stmt, 0);
        const char *id_col = (const char *)sqlite3_column_text(stmt, 1);
        char *kind = mem_strdup(kind_col ? kind_col : "");
        char *id = mem_strdup(id_col ? id_col : "");
        if (!kind || !id || dirty_insert(m, kind, id, reason, code_ref_id, epoch, "{}", now) != 0) {
            rc = -1;
        }
        free(kind);
        free(id);
        if (rc != 0) {
            break;
        }
    }
    sqlite3_finalize(stmt);
    return rc;
}

static int mark_code_ref(cbm_memory_t *m, const char *code_ref_id, const char *state,
                         const char *reason, int64_t epoch, const char *now) {
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m,
                       "UPDATE memory_code_refs SET resolution_status=?,revision=revision+1,"
                       " updated_epoch=?,updated_at=? WHERE code_ref_id=?;",
                       &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, state);
    sqlite3_bind_int64(stmt, 2, epoch);
    bind_text_or_null(stmt, 3, now);
    bind_text_or_null(stmt, 4, code_ref_id);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE ||
        dirty_insert(m, "code_ref", code_ref_id, reason, "", epoch, "{}", now) != 0 ||
        dirty_linked_to_code_ref(m, code_ref_id, reason, epoch, now) != 0) {
        return -1;
    }
    return 0;
}

typedef struct {
    char **ids;
    int count;
    int capacity;
} memory_id_list_t;

static void memory_id_list_free(memory_id_list_t *list) {
    if (!list) {
        return;
    }
    for (int i = 0; i < list->count; i++) {
        free(list->ids[i]);
    }
    free(list->ids);
    memset(list, 0, sizeof(*list));
}

static int memory_id_list_add(memory_id_list_t *list, const char *id) {
    for (int i = 0; i < list->count; i++) {
        if (strcmp(list->ids[i], id) == 0) {
            return 0;
        }
    }
    if (list->count == list->capacity) {
        int next = list->capacity ? list->capacity * 2 : 16;
        char **grown = realloc(list->ids, (size_t)next * sizeof(*grown));
        if (!grown) {
            return -1;
        }
        list->ids = grown;
        list->capacity = next;
    }
    list->ids[list->count] = mem_strdup(id);
    if (!list->ids[list->count]) {
        return -1;
    }
    list->count++;
    return 0;
}

static int collect_code_ref_ids(cbm_memory_t *m, const char *project, const char *column,
                                const char *value, memory_id_list_t *ids) {
    char sql[CBM_SZ_256];
    if (column) {
        (void)snprintf(sql, sizeof(sql),
                       "SELECT code_ref_id FROM memory_code_refs WHERE project=? AND %s=?;",
                       column);
    } else {
        (void)snprintf(sql, sizeof(sql),
                       "SELECT code_ref_id FROM memory_code_refs WHERE project=?;");
    }
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m, sql, &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, project);
    if (column) {
        bind_text_or_null(stmt, 2, value);
    }
    int rc = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *id = (const char *)sqlite3_column_text(stmt, 0);
        if (!id || memory_id_list_add(ids, id) != 0) {
            rc = -1;
            break;
        }
    }
    sqlite3_finalize(stmt);
    return rc;
}

char *cbm_memory_mark_code_changes_json(cbm_memory_t *m, const char *args_json) {
    if (!m) {
        return json_error("memory_unavailable", "memory handle is null");
    }
    yyjson_val *args = NULL;
    yyjson_doc *doc = parse_object_args(args_json, &args);
    if (!doc) {
        return json_error("invalid_arguments", "arguments must be a JSON object");
    }
    const char *project_arg = json_str(args, "project");
    if (!project_arg) {
        yyjson_doc_free(doc);
        return json_error("project_required", "project is required");
    }
    char *project = mem_strdup(project_arg);
    if (!project) {
        yyjson_doc_free(doc);
        return NULL;
    }
    yyjson_val *files = yyjson_obj_get(args, "files");
    yyjson_val *qns = yyjson_obj_get(args, "qualified_names");
    bool deleted = json_bool(args, "deleted", false);
    if ((files && !yyjson_is_arr(files)) || (qns && !yyjson_is_arr(qns))) {
        free(project);
        yyjson_doc_free(doc);
        return json_error("invalid_changes", "files and qualified_names must be arrays");
    }
    const char *reason_arg = json_str(args, "reason");
    const char *reason = reason_arg && reason_arg[0]
                             ? reason_arg
                             : (deleted ? "code_reference_deleted" : "code_reference_changed");
    yyjson_val *arrays[2] = {files, qns};
    for (int a = 0; a < 2; a++) {
        if (!arrays[a]) {
            continue;
        }
        size_t index;
        size_t max;
        yyjson_val *value;
        yyjson_arr_foreach(arrays[a], index, max, value) {
            if (!yyjson_is_str(value)) {
                free(project);
                yyjson_doc_free(doc);
                return json_error("invalid_changes", "change entries must be strings");
            }
        }
    }
    char now[MEM_TIME_MAX];
    iso_now(now);
    if (memory_begin(m) != 0) {
        free(project);
        yyjson_doc_free(doc);
        return json_error("database_busy", m->error);
    }
    memory_id_list_t ids = {0};
    int rc = 0;
    const char *state = deleted ? "missing" : "changed";
    size_t pass_count = (files ? yyjson_arr_size(files) : 0) + (qns ? yyjson_arr_size(qns) : 0);
    if (pass_count == 0) {
        rc = collect_code_ref_ids(m, project, NULL, NULL, &ids);
    } else {
        const char *columns[2] = {"file_path", "qualified_name"};
        for (int a = 0; a < 2 && rc == 0; a++) {
            if (!arrays[a]) {
                continue;
            }
            size_t index;
            size_t max;
            yyjson_val *value;
            yyjson_arr_foreach(arrays[a], index, max, value) {
                if (collect_code_ref_ids(m, project, columns[a], yyjson_get_str(value), &ids) !=
                    0) {
                    rc = -1;
                    break;
                }
            }
        }
    }
    if (rc != 0) {
        memory_rollback(m);
        memory_id_list_free(&ids);
        free(project);
        yyjson_doc_free(doc);
        return json_error("mark_changes_failed", sqlite3_errmsg(m->db));
    }
    if (ids.count == 0) {
        memory_rollback(m);
    }
    int64_t epoch = ids.count > 0 ? memory_bump_epoch(m, now) : cbm_memory_snapshot_epoch(m);
    if (ids.count > 0 && epoch < 0) {
        rc = -1;
    }
    for (int i = 0; i < ids.count && rc == 0; i++) {
        rc = mark_code_ref(m, ids.ids[i], state, reason, epoch, now);
    }

    char activity_id[MEM_ID_MAX] = "";
    char *details = NULL;
    if (rc == 0 && ids.count > 0 && memory_new_id(m, "act", activity_id) == 0) {
        yyjson_mut_doc *details_doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *details_root = details_doc ? yyjson_mut_obj(details_doc) : NULL;
        if (details_doc && details_root) {
            yyjson_mut_doc_set_root(details_doc, details_root);
            yyjson_mut_obj_add_strcpy(details_doc, details_root, "project", project);
            yyjson_mut_obj_add_strcpy(details_doc, details_root, "reason", reason);
            yyjson_mut_obj_add_strcpy(details_doc, details_root, "state", state);
            yyjson_mut_obj_add_int(details_doc, details_root, "code_ref_count", ids.count);
            details = json_write_mut(details_doc);
        }
        yyjson_mut_doc_free(details_doc);
        sqlite3_stmt *stmt = NULL;
        if (!details ||
            memory_prepare(m,
                           "INSERT INTO memory_activities(activity_id,operation_id,agent_id,"
                           " session_id,action,base_epoch,committed_epoch,user_approved,"
                           " details_json,created_at) VALUES(?,?, '', '', 'mark_code_changes',"
                           " ?,?,0,?,?);",
                           &stmt) != 0) {
            rc = -1;
        } else {
            char operation_id[MEM_ID_MAX + 32];
            (void)snprintf(operation_id, sizeof(operation_id), "maintenance:%s", activity_id);
            bind_text_or_null(stmt, 1, activity_id);
            bind_text_or_null(stmt, 2, operation_id);
            sqlite3_bind_int64(stmt, 3, epoch > 0 ? epoch - 1 : 0);
            sqlite3_bind_int64(stmt, 4, epoch);
            bind_text_or_null(stmt, 5, details);
            bind_text_or_null(stmt, 6, now);
            rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
            sqlite3_finalize(stmt);
        }
    } else if (rc == 0 && ids.count > 0) {
        rc = -1;
    }
    free(details);
    if (rc == 0 && ids.count > 0) {
        rc = memory_rebuild_projection_internal(m, true);
    }
    if (rc != 0 || (ids.count > 0 && memory_commit_tx(m) != 0)) {
        memory_rollback(m);
        memory_id_list_free(&ids);
        free(project);
        yyjson_doc_free(doc);
        return json_error("mark_changes_failed", sqlite3_errmsg(m->db));
    }

    yyjson_mut_doc *out_doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *out_root = out_doc ? yyjson_mut_obj(out_doc) : NULL;
    yyjson_mut_val *affected = out_doc ? yyjson_mut_arr(out_doc) : NULL;
    if (!out_doc || !out_root || !affected) {
        yyjson_mut_doc_free(out_doc);
        memory_id_list_free(&ids);
        free(project);
        yyjson_doc_free(doc);
        return NULL;
    }
    yyjson_mut_doc_set_root(out_doc, out_root);
    yyjson_mut_obj_add_bool(out_doc, out_root, "ok", true);
    yyjson_mut_obj_add_strcpy(out_doc, out_root, "project", project);
    yyjson_mut_obj_add_strcpy(out_doc, out_root, "reason", reason);
    yyjson_mut_obj_add_int(out_doc, out_root, "marked", ids.count);
    yyjson_mut_obj_add_int(out_doc, out_root, "snapshot_epoch", epoch);

    memory_id_list_t affected_keys = {0};
    for (int i = 0; i < ids.count; i++) {
        char key[MEM_PATH_MAX];
        (void)snprintf(key, sizeof(key), "code_ref:%s", ids.ids[i]);
        int before = affected_keys.count;
        if (memory_id_list_add(&affected_keys, key) == 0 && affected_keys.count > before) {
            yyjson_mut_val *entry = yyjson_mut_obj(out_doc);
            yyjson_mut_obj_add_strcpy(out_doc, entry, "kind", "code_ref");
            yyjson_mut_obj_add_strcpy(out_doc, entry, "id", ids.ids[i]);
            yyjson_mut_obj_add_strcpy(out_doc, entry, "reason", reason);
            yyjson_mut_arr_add_val(affected, entry);
        }
        sqlite3_stmt *stmt = NULL;
        const char *sql =
            "SELECT source_kind,source_id FROM memory_relations WHERE target_kind='code_ref'"
            " AND target_id=? AND recorded_to IS NULL UNION SELECT target_kind,target_id"
            " FROM memory_relations WHERE source_kind='code_ref' AND source_id=?"
            " AND recorded_to IS NULL;";
        if (memory_prepare(m, sql, &stmt) == 0) {
            bind_text_or_null(stmt, 1, ids.ids[i]);
            bind_text_or_null(stmt, 2, ids.ids[i]);
            while (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *kind = (const char *)sqlite3_column_text(stmt, 0);
                const char *id = (const char *)sqlite3_column_text(stmt, 1);
                (void)snprintf(key, sizeof(key), "%s:%s", kind ? kind : "", id ? id : "");
                before = affected_keys.count;
                if (memory_id_list_add(&affected_keys, key) == 0 && affected_keys.count > before) {
                    yyjson_mut_val *entry = yyjson_mut_obj(out_doc);
                    yyjson_mut_obj_add_strcpy(out_doc, entry, "kind", kind ? kind : "");
                    yyjson_mut_obj_add_strcpy(out_doc, entry, "id", id ? id : "");
                    yyjson_mut_obj_add_strcpy(out_doc, entry, "reason", reason);
                    yyjson_mut_arr_add_val(affected, entry);
                }
            }
            sqlite3_finalize(stmt);
        }
    }
    memory_id_list_free(&affected_keys);
    yyjson_mut_obj_add_val(out_doc, out_root, "affected_memory", affected);
    sqlite3_stmt *dirty_stmt = NULL;
    int64_t dirty_count = 0;
    if (memory_prepare(m, "SELECT count(*) FROM memory_dirty WHERE status='open';", &dirty_stmt) ==
        0) {
        if (sqlite3_step(dirty_stmt) == SQLITE_ROW) {
            dirty_count = sqlite3_column_int64(dirty_stmt, 0);
        }
        sqlite3_finalize(dirty_stmt);
    }
    yyjson_mut_obj_add_int(out_doc, out_root, "memory_dirty_count", dirty_count);
    char *out = json_write_mut(out_doc);
    yyjson_mut_doc_free(out_doc);
    memory_id_list_free(&ids);
    free(project);
    yyjson_doc_free(doc);
    return out;
}

typedef struct {
    char *id;
    char *kind;
    char *qn;
    char *file;
    char *status;
} code_ref_check_t;

static void code_ref_checks_free(code_ref_check_t *refs, int count) {
    for (int i = 0; i < count; i++) {
        free(refs[i].id);
        free(refs[i].kind);
        free(refs[i].qn);
        free(refs[i].file);
        free(refs[i].status);
    }
    free(refs);
}

int cbm_memory_validate_code_refs(cbm_memory_t *m, cbm_store_t *code_store, const char *project) {
    if (!m || !code_store || !project) {
        return -1;
    }
    sqlite3_stmt *stmt = NULL;
    if (memory_prepare(m,
                       "SELECT code_ref_id,ref_kind,qualified_name,file_path,resolution_status"
                       " FROM memory_code_refs WHERE project=? ORDER BY code_ref_id;",
                       &stmt) != 0) {
        return -1;
    }
    bind_text_or_null(stmt, 1, project);
    code_ref_check_t *refs = NULL;
    int count = 0;
    int cap = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (count == cap) {
            int next = cap ? cap * 2 : 16;
            code_ref_check_t *grown = realloc(refs, (size_t)next * sizeof(*refs));
            if (!grown) {
                sqlite3_finalize(stmt);
                code_ref_checks_free(refs, count);
                return -1;
            }
            refs = grown;
            memset(refs + cap, 0, (size_t)(next - cap) * sizeof(*refs));
            cap = next;
        }
        refs[count].id = mem_strdup((const char *)sqlite3_column_text(stmt, 0));
        refs[count].kind = mem_strdup((const char *)sqlite3_column_text(stmt, 1));
        refs[count].qn = mem_strdup((const char *)sqlite3_column_text(stmt, 2));
        refs[count].file = mem_strdup((const char *)sqlite3_column_text(stmt, 3));
        refs[count].status = mem_strdup((const char *)sqlite3_column_text(stmt, 4));
        if (!refs[count].id || !refs[count].kind || !refs[count].qn || !refs[count].file ||
            !refs[count].status) {
            sqlite3_finalize(stmt);
            code_ref_checks_free(refs, count + 1);
            return -1;
        }
        count++;
    }
    sqlite3_finalize(stmt);
    if (count == 0) {
        free(refs);
        return 0;
    }

    bool *resolved = calloc((size_t)count, sizeof(*resolved));
    if (!resolved) {
        code_ref_checks_free(refs, count);
        return -1;
    }
    int resolved_count = 0;
    for (int i = 0; i < count; i++) {
        if (strcmp(refs[i].kind, "symbol") == 0 && refs[i].qn[0]) {
            cbm_node_t node = {0};
            resolved[i] =
                cbm_store_find_node_by_qn(code_store, project, refs[i].qn, &node) == CBM_STORE_OK;
            if (resolved[i]) {
                cbm_node_free_fields(&node);
            }
        } else if (refs[i].file[0]) {
            cbm_node_t *nodes = NULL;
            int node_count = 0;
            resolved[i] = cbm_store_find_nodes_by_file(code_store, project, refs[i].file, &nodes,
                                                       &node_count) == CBM_STORE_OK &&
                          node_count > 0;
            cbm_store_free_nodes(nodes, node_count);
        }
        if (resolved[i]) {
            resolved_count++;
        }
    }

    int changed_count = 0;
    for (int i = 0; i < count; i++) {
        const char *next_status = resolved[i] ? "resolved" : "missing";
        changed_count += strcmp(refs[i].status, next_status) != 0;
    }
    if (changed_count == 0) {
        free(resolved);
        code_ref_checks_free(refs, count);
        return resolved_count;
    }

    char now[MEM_TIME_MAX];
    iso_now(now);
    if (memory_begin(m) != 0) {
        free(resolved);
        code_ref_checks_free(refs, count);
        return -1;
    }
    int64_t epoch = memory_bump_epoch(m, now);
    int rc = epoch >= 0 ? 0 : -1;
    for (int i = 0; i < count && rc == 0; i++) {
        const char *next_status = resolved[i] ? "resolved" : "missing";
        if (strcmp(refs[i].status, next_status) == 0) {
            continue;
        }
        if (memory_prepare(m,
                           "UPDATE memory_code_refs SET resolution_status=?,last_resolved_at=?,"
                           " revision=revision+1,updated_epoch=?,updated_at=? WHERE code_ref_id=?;",
                           &stmt) != 0) {
            rc = -1;
            break;
        }
        bind_text_or_null(stmt, 1, next_status);
        bind_text_or_null(stmt, 2, resolved[i] ? now : NULL);
        sqlite3_bind_int64(stmt, 3, epoch);
        bind_text_or_null(stmt, 4, now);
        bind_text_or_null(stmt, 5, refs[i].id);
        rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
        sqlite3_finalize(stmt);
        if (rc == 0 && !resolved[i]) {
            rc = dirty_insert(m, "code_ref", refs[i].id, "code_reference_missing", "", epoch, "{}",
                              now);
            if (rc == 0) {
                rc = dirty_linked_to_code_ref(m, refs[i].id, "code_reference_missing", epoch, now);
            }
        } else if (rc == 0) {
            if (memory_prepare(m,
                               "UPDATE memory_dirty SET status='resolved',resolved_at=?"
                               " WHERE entity_kind='code_ref' AND entity_id=?"
                               " AND reason='code_reference_missing' AND status='open';",
                               &stmt) != 0) {
                rc = -1;
            } else {
                bind_text_or_null(stmt, 1, now);
                bind_text_or_null(stmt, 2, refs[i].id);
                rc = sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
                sqlite3_finalize(stmt);
            }
        }
    }
    char activity_id[MEM_ID_MAX];
    if (rc == 0 && memory_new_id(m, "act", activity_id) == 0) {
        if (memory_prepare(m,
                           "INSERT INTO memory_activities(activity_id,operation_id,agent_id,"
                           " session_id,action,base_epoch,committed_epoch,user_approved,"
                           " details_json,created_at) VALUES(?,?, '', '', 'validate_code_refs',"
                           " ?,?,0,?,?);",
                           &stmt) != 0) {
            rc = -1;
        } else {
            char operation_id[MEM_ID_MAX + 32];
            (void)snprintf(operation_id, sizeof(operation_id), "maintenance:%s", activity_id);
            yyjson_mut_doc *details_doc = yyjson_mut_doc_new(NULL);
            yyjson_mut_val *details_root = details_doc ? yyjson_mut_obj(details_doc) : NULL;
            char *details = NULL;
            if (details_doc && details_root) {
                yyjson_mut_doc_set_root(details_doc, details_root);
                yyjson_mut_obj_add_strcpy(details_doc, details_root, "project", project);
                yyjson_mut_obj_add_int(details_doc, details_root, "changed_count", changed_count);
                yyjson_mut_obj_add_int(details_doc, details_root, "resolved_count", resolved_count);
                details = json_write_mut(details_doc);
            }
            yyjson_mut_doc_free(details_doc);
            bind_text_or_null(stmt, 1, activity_id);
            bind_text_or_null(stmt, 2, operation_id);
            sqlite3_bind_int64(stmt, 3, epoch > 0 ? epoch - 1 : 0);
            sqlite3_bind_int64(stmt, 4, epoch);
            bind_text_or_null(stmt, 5, details);
            bind_text_or_null(stmt, 6, now);
            rc = details && sqlite3_step(stmt) == SQLITE_DONE ? 0 : -1;
            free(details);
            sqlite3_finalize(stmt);
        }
    } else if (rc == 0) {
        rc = -1;
    }
    if (rc == 0) {
        rc = memory_rebuild_projection_internal(m, true);
    }
    if (rc != 0 || memory_commit_tx(m) != 0) {
        memory_rollback(m);
        resolved_count = -1;
    }
    free(resolved);
    code_ref_checks_free(refs, count);
    return resolved_count;
}
