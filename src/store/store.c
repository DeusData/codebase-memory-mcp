/*
 * store.c — SQLite graph store implementation.
 *
 * Implements the opaque cbm_store_t handle with prepared statement caching,
 * schema initialization, and all CRUD operations for nodes, edges, projects,
 * file hashes, search, BFS traversal, and schema introspection.
 */

// for ISO timestamp

#include "store/store.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_regex.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ── SQLite bind helpers ───────────────────────────────────────── */

/* Wrap sqlite3_bind_text with SQLITE_TRANSIENT to isolate the platform
   int-to-pointer cast ((void*)-1) in one place. */
// NOLINTNEXTLINE(performance-no-int-to-ptr)
static const sqlite3_destructor_type BIND_TRANSIENT = SQLITE_TRANSIENT;

static int bind_text(sqlite3_stmt *s, int col, const char *v) {
    return sqlite3_bind_text(s, col, v, -1, BIND_TRANSIENT);
}

/* ── Internal store structure ───────────────────────────────────── */

struct cbm_store {
    sqlite3 *db;
    const char *db_path; /* heap-allocated, or NULL for :memory: */
    char errbuf[512];

    /* Prepared statements (lazily initialized, cached for lifetime) */
    sqlite3_stmt *stmt_upsert_node;
    sqlite3_stmt *stmt_find_node_by_id;
    sqlite3_stmt *stmt_find_node_by_qn;
    sqlite3_stmt *stmt_find_node_by_qn_any; /* QN lookup without project filter */
    sqlite3_stmt *stmt_find_nodes_by_name;
    sqlite3_stmt *stmt_find_nodes_by_name_any; /* name lookup without project filter */
    sqlite3_stmt *stmt_find_nodes_by_label;
    sqlite3_stmt *stmt_find_nodes_by_file;
    sqlite3_stmt *stmt_count_nodes;
    sqlite3_stmt *stmt_delete_nodes_by_project;
    sqlite3_stmt *stmt_delete_nodes_by_file;
    sqlite3_stmt *stmt_delete_nodes_by_label;

    sqlite3_stmt *stmt_insert_edge;
    sqlite3_stmt *stmt_find_edges_by_source;
    sqlite3_stmt *stmt_find_edges_by_target;
    sqlite3_stmt *stmt_find_edges_by_source_type;
    sqlite3_stmt *stmt_find_edges_by_target_type;
    sqlite3_stmt *stmt_find_edges_by_type;
    sqlite3_stmt *stmt_count_edges;
    sqlite3_stmt *stmt_count_edges_by_type;
    sqlite3_stmt *stmt_delete_edges_by_project;
    sqlite3_stmt *stmt_delete_edges_by_type;

    sqlite3_stmt *stmt_upsert_project;
    sqlite3_stmt *stmt_get_project;
    sqlite3_stmt *stmt_list_projects;
    sqlite3_stmt *stmt_delete_project;

    sqlite3_stmt *stmt_upsert_file_hash;
    sqlite3_stmt *stmt_get_file_hashes;
    sqlite3_stmt *stmt_delete_file_hash;
    sqlite3_stmt *stmt_delete_file_hashes;
};

/* ── Helpers ────────────────────────────────────────────────────── */

static void store_set_error(cbm_store_t *s, const char *msg) {
    snprintf(s->errbuf, sizeof(s->errbuf), "%s", msg);
}

static void store_set_error_sqlite(cbm_store_t *s, const char *prefix) {
    snprintf(s->errbuf, sizeof(s->errbuf), "%s: %s", prefix, sqlite3_errmsg(s->db));
}

static int exec_sql(cbm_store_t *s, const char *sql) {
    if (!s || !s->db) {
        return CBM_STORE_ERR;
    }
    char *err = NULL;
    int rc = sqlite3_exec(s->db, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        snprintf(s->errbuf, sizeof(s->errbuf), "exec: %s", err ? err : "unknown");
        sqlite3_free(err);
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* Safe string: returns "" if NULL. */
static const char *safe_str(const char *s) {
    return s ? s : "";
}

/* Safe properties: returns "{}" if NULL. */
static const char *safe_props(const char *s) {
    return (s && s[0]) ? s : "{}";
}

/* Duplicate a string onto the heap. */
static char *heap_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *d = malloc(len + 1);
    if (d) {
        memcpy(d, s, len + 1);
    }
    return d;
}

/* Prepare a statement (cached). If already prepared, reset+clear. */
static sqlite3_stmt *prepare_cached(cbm_store_t *s, sqlite3_stmt **slot, const char *sql) {
    if (!s || !s->db) {
        return NULL;
    }
    if (*slot) {
        sqlite3_reset(*slot);
        sqlite3_clear_bindings(*slot);
        return *slot;
    }
    int rc = sqlite3_prepare_v2(s->db, sql, -1, slot, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "prepare");
        return NULL;
    }
    return *slot;
}

/* Get ISO-8601 timestamp. */
static void iso_now(char *buf, size_t sz) {
    time_t t = time(NULL);
    struct tm tm;
#ifdef _WIN32
    gmtime_s(&tm, &t); /* Windows: reversed arg order */
#else
    gmtime_r(&t, &tm);
#endif
    (void)strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ",
                   &tm); // cert-err33-c: strftime only fails if buffer is too small — 21-byte ISO
                         // timestamp always fits in caller-provided buffers
}

/* ── Schema ─────────────────────────────────────────────────────── */

static int init_schema(cbm_store_t *s) {
    const char *ddl = "CREATE TABLE IF NOT EXISTS projects ("
                      "  name TEXT PRIMARY KEY,"
                      "  indexed_at TEXT NOT NULL,"
                      "  root_path TEXT NOT NULL"
                      ");"
                      "CREATE TABLE IF NOT EXISTS file_hashes ("
                      "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
                      "  rel_path TEXT NOT NULL,"
                      "  sha256 TEXT NOT NULL,"
                      "  mtime_ns INTEGER NOT NULL DEFAULT 0,"
                      "  size INTEGER NOT NULL DEFAULT 0,"
                      "  PRIMARY KEY (project, rel_path)"
                      ");"
                      "CREATE TABLE IF NOT EXISTS nodes ("
                      "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                      "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
                      "  label TEXT NOT NULL,"
                      "  name TEXT NOT NULL,"
                      "  qualified_name TEXT NOT NULL,"
                      "  file_path TEXT DEFAULT '',"
                      "  start_line INTEGER DEFAULT 0,"
                      "  end_line INTEGER DEFAULT 0,"
                      "  properties TEXT DEFAULT '{}',"
                      "  UNIQUE(project, qualified_name)"
                      ");"
                      "CREATE TABLE IF NOT EXISTS edges ("
                      "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
                      "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
                      "  source_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,"
                      "  target_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,"
                      "  type TEXT NOT NULL,"
                      "  properties TEXT DEFAULT '{}',"
                      "  UNIQUE(source_id, target_id, type)"
                      ");"
                      "CREATE TABLE IF NOT EXISTS project_summaries ("
                      "  project TEXT PRIMARY KEY,"
                      "  summary TEXT NOT NULL,"
                      "  source_hash TEXT NOT NULL,"
                      "  created_at TEXT NOT NULL,"
                      "  updated_at TEXT NOT NULL"
                      ");";

    return exec_sql(s, ddl);
}

static int create_user_indexes(cbm_store_t *s) {
    const char *sql =
        "CREATE INDEX IF NOT EXISTS idx_nodes_label ON nodes(project, label);"
        "CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(project, name);"
        "CREATE INDEX IF NOT EXISTS idx_nodes_file ON nodes(project, file_path);"
        "CREATE INDEX IF NOT EXISTS idx_edges_source ON edges(source_id, type);"
        "CREATE INDEX IF NOT EXISTS idx_edges_target ON edges(target_id, type);"
        "CREATE INDEX IF NOT EXISTS idx_edges_type ON edges(project, type);"
        "CREATE INDEX IF NOT EXISTS idx_edges_target_type ON edges(project, target_id, type);"
        "CREATE INDEX IF NOT EXISTS idx_edges_source_type ON edges(project, source_id, type);";
    return exec_sql(s, sql);
}

static int configure_pragmas(cbm_store_t *s, bool in_memory) {
    int rc;
    rc = exec_sql(s, "PRAGMA foreign_keys = ON;");
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = exec_sql(s, "PRAGMA temp_store = MEMORY;");
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    if (in_memory) {
        rc = exec_sql(s, "PRAGMA synchronous = OFF;");
    } else {
        rc = exec_sql(s, "PRAGMA journal_mode = WAL;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = exec_sql(s, "PRAGMA synchronous = NORMAL;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = exec_sql(s, "PRAGMA busy_timeout = 10000;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = exec_sql(s, "PRAGMA mmap_size = 67108864;"); /* 64 MB */
    }
    return rc;
}

/* ── REGEXP function for SQLite ──────────────────────────────────── */

static void sqlite_regexp(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *pattern = (const char *)sqlite3_value_text(argv[0]);
    const char *text = (const char *)sqlite3_value_text(argv[1]);
    if (!pattern || !text) {
        sqlite3_result_int(ctx, 0);
        return;
    }

    cbm_regex_t re;
    int rc = cbm_regcomp(&re, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB);
    if (rc != 0) {
        sqlite3_result_error(ctx, "invalid regex", -1);
        return;
    }

    rc = cbm_regexec(&re, text, 0, NULL, 0);
    cbm_regfree(&re);
    sqlite3_result_int(ctx, rc == 0 ? 1 : 0);
}

/* Case-insensitive REGEXP variant */
static void sqlite_iregexp(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *pattern = (const char *)sqlite3_value_text(argv[0]);
    const char *text = (const char *)sqlite3_value_text(argv[1]);
    if (!pattern || !text) {
        sqlite3_result_int(ctx, 0);
        return;
    }

    cbm_regex_t re;
    int rc = cbm_regcomp(&re, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB | CBM_REG_ICASE);
    if (rc != 0) {
        sqlite3_result_error(ctx, "invalid regex", -1);
        return;
    }

    rc = cbm_regexec(&re, text, 0, NULL, 0);
    cbm_regfree(&re);
    sqlite3_result_int(ctx, rc == 0 ? 1 : 0);
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

static cbm_store_t *store_open_internal(const char *path, bool in_memory) {
    cbm_store_t *s = calloc(1, sizeof(cbm_store_t));
    if (!s) {
        return NULL;
    }

    int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE;
    if (in_memory) {
        flags |= SQLITE_OPEN_MEMORY;
    }

    int rc = sqlite3_open_v2(path, &s->db, flags, NULL);
    if (rc != SQLITE_OK) {
        free(s);
        return NULL;
    }

    if (path && !in_memory) {
        s->db_path = heap_strdup(path);
    }

    /* Register REGEXP function (SQLite doesn't have one built-in) */
    sqlite3_create_function(s->db, "regexp", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                            sqlite_regexp, NULL, NULL);
    /* Case-insensitive variant for search with case_sensitive=false */
    sqlite3_create_function(s->db, "iregexp", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                            sqlite_iregexp, NULL, NULL);

    if (configure_pragmas(s, in_memory) != CBM_STORE_OK || init_schema(s) != CBM_STORE_OK ||
        create_user_indexes(s) != CBM_STORE_OK) {
        sqlite3_close(s->db);
        free((void *)s->db_path);
        free(s);
        return NULL;
    }

    return s;
}

cbm_store_t *cbm_store_open_memory(void) {
    return store_open_internal(":memory:", true);
}

cbm_store_t *cbm_store_open_path(const char *db_path) {
    if (!db_path) {
        return NULL;
    }
    return store_open_internal(db_path, false);
}

cbm_store_t *cbm_store_open(const char *project) {
    if (!project) {
        return NULL;
    }
    /* Build path: ~/.cache/codebase-memory-mcp/<project>.db */
    const char *home = getenv("HOME"); // NOLINT(concurrency-mt-unsafe) — called once during
                                       // single-threaded store open, never concurrently
    if (!home) {
        home = "/tmp";
    }
    char path[1024];
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/%s.db", home, project);
    return store_open_internal(path, false);
}

static void finalize_stmt(sqlite3_stmt **s) {
    if (*s) {
        sqlite3_finalize(*s);
        *s = NULL;
    }
}

void cbm_store_close(cbm_store_t *s) {
    if (!s) {
        return;
    }

    /* Finalize all cached statements */
    finalize_stmt(&s->stmt_upsert_node);
    finalize_stmt(&s->stmt_find_node_by_id);
    finalize_stmt(&s->stmt_find_node_by_qn);
    finalize_stmt(&s->stmt_find_node_by_qn_any);
    finalize_stmt(&s->stmt_find_nodes_by_name);
    finalize_stmt(&s->stmt_find_nodes_by_name_any);
    finalize_stmt(&s->stmt_find_nodes_by_label);
    finalize_stmt(&s->stmt_find_nodes_by_file);
    finalize_stmt(&s->stmt_count_nodes);
    finalize_stmt(&s->stmt_delete_nodes_by_project);
    finalize_stmt(&s->stmt_delete_nodes_by_file);
    finalize_stmt(&s->stmt_delete_nodes_by_label);

    finalize_stmt(&s->stmt_insert_edge);
    finalize_stmt(&s->stmt_find_edges_by_source);
    finalize_stmt(&s->stmt_find_edges_by_target);
    finalize_stmt(&s->stmt_find_edges_by_source_type);
    finalize_stmt(&s->stmt_find_edges_by_target_type);
    finalize_stmt(&s->stmt_find_edges_by_type);
    finalize_stmt(&s->stmt_count_edges);
    finalize_stmt(&s->stmt_count_edges_by_type);
    finalize_stmt(&s->stmt_delete_edges_by_project);
    finalize_stmt(&s->stmt_delete_edges_by_type);

    finalize_stmt(&s->stmt_upsert_project);
    finalize_stmt(&s->stmt_get_project);
    finalize_stmt(&s->stmt_list_projects);
    finalize_stmt(&s->stmt_delete_project);

    finalize_stmt(&s->stmt_upsert_file_hash);
    finalize_stmt(&s->stmt_get_file_hashes);
    finalize_stmt(&s->stmt_delete_file_hash);
    finalize_stmt(&s->stmt_delete_file_hashes);

    sqlite3_close(s->db);
    free((void *)s->db_path);
    free(s);
}

const char *cbm_store_error(cbm_store_t *s) {
    return s ? s->errbuf : "null store";
}

/* ── Transaction ────────────────────────────────────────────────── */

int cbm_store_begin(cbm_store_t *s) {
    return exec_sql(s, "BEGIN IMMEDIATE;");
}

int cbm_store_commit(cbm_store_t *s) {
    return exec_sql(s, "COMMIT;");
}

int cbm_store_rollback(cbm_store_t *s) {
    return exec_sql(s, "ROLLBACK;");
}

/* ── Bulk write ─────────────────────────────────────────────────── */

int cbm_store_begin_bulk(cbm_store_t *s) {
    int rc = exec_sql(s, "PRAGMA journal_mode = MEMORY;");
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = exec_sql(s, "PRAGMA synchronous = OFF;");
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    return exec_sql(s, "PRAGMA cache_size = -65536;"); /* 64 MB */
}

int cbm_store_end_bulk(cbm_store_t *s) {
    int rc = exec_sql(s, "PRAGMA journal_mode = WAL;");
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = exec_sql(s, "PRAGMA synchronous = NORMAL;");
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    return exec_sql(s, "PRAGMA cache_size = -2000;"); /* default ~2 MB */
}

int cbm_store_drop_indexes(cbm_store_t *s) {
    return exec_sql(s, "DROP INDEX IF EXISTS idx_nodes_label;"
                       "DROP INDEX IF EXISTS idx_nodes_name;"
                       "DROP INDEX IF EXISTS idx_nodes_file;"
                       "DROP INDEX IF EXISTS idx_edges_source;"
                       "DROP INDEX IF EXISTS idx_edges_target;"
                       "DROP INDEX IF EXISTS idx_edges_type;"
                       "DROP INDEX IF EXISTS idx_edges_target_type;"
                       "DROP INDEX IF EXISTS idx_edges_source_type;");
}

int cbm_store_create_indexes(cbm_store_t *s) {
    return create_user_indexes(s);
}

/* ── Checkpoint ─────────────────────────────────────────────────── */

int cbm_store_checkpoint(cbm_store_t *s) {
    int rc = sqlite3_wal_checkpoint_v2(s->db, NULL, SQLITE_CHECKPOINT_TRUNCATE, NULL, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "checkpoint");
        return CBM_STORE_ERR;
    }
    return exec_sql(s, "PRAGMA optimize;");
}

/* ── Dump ───────────────────────────────────────────────────────── */

/* Dump entire in-memory database to a file via sqlite3_backup.
 * Writes to a temp file first, then atomically renames for crash safety.
 * sqlite3_backup_step(-1) copies ALL B-tree pages in one call —
 * the file on disk is an exact replica of the in-memory page layout. */
int cbm_store_dump_to_file(cbm_store_t *s, const char *dest_path) {
    if (!s || !dest_path) {
        return CBM_STORE_ERR;
    }

    /* Ensure parent directory exists */
    char dir[1024];
    snprintf(dir, sizeof(dir), "%s", dest_path);
    char *sl = strrchr(dir, '/');
    if (sl) {
        *sl = '\0';
        (void)cbm_mkdir(dir);
    }

    /* Write to temp file for atomic swap */
    char tmp_path[1024];
    snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", dest_path);
    (void)unlink(tmp_path);

    sqlite3 *dest_db = NULL;
    int rc = sqlite3_open(tmp_path, &dest_db);
    if (rc != SQLITE_OK) {
        store_set_error(s, "dump: cannot open temp file");
        return CBM_STORE_ERR;
    }

    sqlite3_backup *bk = sqlite3_backup_init(dest_db, "main", s->db, "main");
    if (!bk) {
        store_set_error(s, "dump: backup init failed");
        sqlite3_close(dest_db);
        (void)unlink(tmp_path);
        return CBM_STORE_ERR;
    }

    rc = sqlite3_backup_step(bk, -1); /* copy ALL pages in one shot */
    sqlite3_backup_finish(bk);

    if (rc != SQLITE_DONE) {
        store_set_error(s, "dump: backup step failed");
        sqlite3_close(dest_db);
        (void)unlink(tmp_path);
        return CBM_STORE_ERR;
    }

    /* Enable WAL on the dumped file so readers can connect concurrently */
    sqlite3_exec(dest_db, "PRAGMA journal_mode = WAL;", NULL, NULL, NULL);
    sqlite3_close(dest_db);

    /* Atomic rename: old WAL/SHM become stale and get recreated by
     * the next reader's configure_pragmas call. */
    if (rename(tmp_path, dest_path) != 0) {
        store_set_error(s, "dump: rename failed");
        (void)unlink(tmp_path);
        return CBM_STORE_ERR;
    }

    return CBM_STORE_OK;
}

/* ── Project CRUD ───────────────────────────────────────────────── */

int cbm_store_upsert_project(cbm_store_t *s, const char *name, const char *root_path) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_upsert_project,
                       "INSERT INTO projects (name, indexed_at, root_path) VALUES (?1, ?2, ?3) "
                       "ON CONFLICT(name) DO UPDATE SET indexed_at=?2, root_path=?3;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    char ts[64];
    iso_now(ts, sizeof(ts));

    bind_text(stmt, 1, name);
    bind_text(stmt, 2, ts);
    bind_text(stmt, 3, root_path);

    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_project");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_get_project(cbm_store_t *s, const char *name, cbm_project_t *out) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_get_project,
                       "SELECT name, indexed_at, root_path FROM projects WHERE name = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, name);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        out->name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        out->indexed_at = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
        out->root_path = heap_strdup((const char *)sqlite3_column_text(stmt, 2));
        return CBM_STORE_OK;
    }
    return CBM_STORE_NOT_FOUND;
}

int cbm_store_list_projects(cbm_store_t *s, cbm_project_t **out, int *count) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_list_projects,
                       "SELECT name, indexed_at, root_path FROM projects ORDER BY name;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    /* Collect into dynamic array */
    int cap = 8;
    int n = 0;
    cbm_project_t *arr = malloc(cap * sizeof(cbm_project_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_project_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].indexed_at = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
        arr[n].root_path = heap_strdup((const char *)sqlite3_column_text(stmt, 2));
        n++;
    }

    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

int cbm_store_delete_project(cbm_store_t *s, const char *name) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_delete_project, "DELETE FROM projects WHERE name = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, name);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_project");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── Node CRUD ──────────────────────────────────────────────────── */

// NOLINTNEXTLINE(misc-include-cleaner) — int64_t provided by standard header
int64_t cbm_store_upsert_node(cbm_store_t *s, const cbm_node_t *n) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_upsert_node,
                       "INSERT INTO nodes (project, label, name, qualified_name, file_path, "
                       "start_line, end_line, properties) "
                       "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8) "
                       "ON CONFLICT(project, qualified_name) DO UPDATE SET "
                       "label=?2, name=?3, file_path=?5, start_line=?6, end_line=?7, properties=?8 "
                       "RETURNING id;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, safe_str(n->project));
    bind_text(stmt, 2, safe_str(n->label));
    bind_text(stmt, 3, safe_str(n->name));
    bind_text(stmt, 4, safe_str(n->qualified_name));
    bind_text(stmt, 5, safe_str(n->file_path));
    sqlite3_bind_int(stmt, 6, n->start_line);
    sqlite3_bind_int(stmt, 7, n->end_line);
    bind_text(stmt, 8, safe_props(n->properties_json));

    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(stmt, 0);
        sqlite3_reset(stmt); /* unblock COMMIT — RETURNING leaves stmt active */
        return id;
    }
    sqlite3_reset(stmt);
    store_set_error_sqlite(s, "upsert_node");
    return CBM_STORE_ERR;
}

/* Scan a node from current row of stmt. Heap-allocates strings. */
static void scan_node(sqlite3_stmt *stmt, cbm_node_t *n) {
    n->id = sqlite3_column_int64(stmt, 0);
    n->project = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
    n->label = heap_strdup((const char *)sqlite3_column_text(stmt, 2));
    // NOLINTNEXTLINE(clang-analyzer-unix.Malloc)
    n->name = heap_strdup((const char *)sqlite3_column_text(stmt, 3));
    n->qualified_name = heap_strdup((const char *)sqlite3_column_text(stmt, 4));
    n->file_path = heap_strdup((const char *)sqlite3_column_text(stmt, 5));
    n->start_line = sqlite3_column_int(stmt, 6);
    n->end_line = sqlite3_column_int(stmt, 7);
    n->properties_json = heap_strdup((const char *)sqlite3_column_text(stmt, 8));
}

int cbm_store_find_node_by_id(cbm_store_t *s, int64_t id, cbm_node_t *out) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_find_node_by_id,
                       "SELECT id, project, label, name, qualified_name, file_path, "
                       "start_line, end_line, properties FROM nodes WHERE id = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    sqlite3_bind_int64(stmt, 1, id);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        scan_node(stmt, out);
        return CBM_STORE_OK;
    }
    return CBM_STORE_NOT_FOUND;
}

int cbm_store_find_node_by_qn(cbm_store_t *s, const char *project, const char *qn,
                              cbm_node_t *out) {
    if (!s || !s->db) {
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_find_node_by_qn,
                       "SELECT id, project, label, name, qualified_name, file_path, "
                       "start_line, end_line, properties FROM nodes "
                       "WHERE project = ?1 AND qualified_name = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, qn);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        scan_node(stmt, out);
        return CBM_STORE_OK;
    }
    return CBM_STORE_NOT_FOUND;
}

int cbm_store_find_node_by_qn_any(cbm_store_t *s, const char *qn, cbm_node_t *out) {
    if (!s || !s->db) {
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_find_node_by_qn_any,
                       "SELECT id, project, label, name, qualified_name, file_path, "
                       "start_line, end_line, properties FROM nodes "
                       "WHERE qualified_name = ?1 LIMIT 1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, qn);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        scan_node(stmt, out);
        return CBM_STORE_OK;
    }
    return CBM_STORE_NOT_FOUND;
}

int cbm_store_find_nodes_by_name_any(cbm_store_t *s, const char *name, cbm_node_t **out,
                                     int *count) {
    if (!s || !s->db) {
        *out = NULL;
        *count = 0;
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_find_nodes_by_name_any,
                       "SELECT id, project, label, name, qualified_name, file_path, "
                       "start_line, end_line, properties FROM nodes "
                       "WHERE name = ?1;");
    if (!stmt) {
        *out = NULL;
        *count = 0;
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, name);

    int cap = 16;
    int n = 0;
    cbm_node_t *arr = malloc(cap * sizeof(cbm_node_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_node_t));
        }
        scan_node(stmt, &arr[n]);
        n++;
    }
    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

int cbm_store_find_node_ids_by_qns(cbm_store_t *s, const char *project, const char **qns,
                                   int qn_count, int64_t *out_ids) {
    if (!s || !project || !qns || !out_ids || qn_count <= 0) {
        return 0;
    }

    /* Zero out results */
    memset(out_ids, 0, (size_t)qn_count * sizeof(int64_t));

    int found = 0;
    cbm_node_t node = {0};
    for (int i = 0; i < qn_count; i++) {
        if (!qns[i]) {
            continue;
        }
        int rc = cbm_store_find_node_by_qn(s, project, qns[i], &node);
        if (rc == CBM_STORE_OK) {
            out_ids[i] = node.id;
            found++;
            cbm_node_free_fields(&node);
            memset(&node, 0, sizeof(node));
        }
    }
    return found;
}

/* Generic: find multiple nodes by a single-column filter. */
// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int find_nodes_generic(cbm_store_t *s, sqlite3_stmt **slot, const char *sql,
                              const char *project, const char *val, cbm_node_t **out, int *count) {
    if (!s || !s->db) {
        *out = NULL;
        *count = 0;
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = prepare_cached(s, slot, sql);
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, val);

    int cap = 16;
    int n = 0;
    cbm_node_t *arr = malloc(cap * sizeof(cbm_node_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_node_t));
        }
        scan_node(stmt, &arr[n]);
        n++;
    }

    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

int cbm_store_find_nodes_by_name(cbm_store_t *s, const char *project, const char *name,
                                 cbm_node_t **out, int *count) {
    return find_nodes_generic(s, &s->stmt_find_nodes_by_name,
                              "SELECT id, project, label, name, qualified_name, file_path, "
                              "start_line, end_line, properties FROM nodes "
                              "WHERE project = ?1 AND name = ?2;",
                              project, name, out, count);
}

int cbm_store_find_nodes_by_label(cbm_store_t *s, const char *project, const char *label,
                                  cbm_node_t **out, int *count) {
    return find_nodes_generic(s, &s->stmt_find_nodes_by_label,
                              "SELECT id, project, label, name, qualified_name, file_path, "
                              "start_line, end_line, properties FROM nodes "
                              "WHERE project = ?1 AND label = ?2;",
                              project, label, out, count);
}

int cbm_store_find_nodes_by_file(cbm_store_t *s, const char *project, const char *file_path,
                                 cbm_node_t **out, int *count) {
    return find_nodes_generic(s, &s->stmt_find_nodes_by_file,
                              "SELECT id, project, label, name, qualified_name, file_path, "
                              "start_line, end_line, properties FROM nodes "
                              "WHERE project = ?1 AND file_path = ?2;",
                              project, file_path, out, count);
}

int cbm_store_count_nodes(cbm_store_t *s, const char *project) {
    if (!s || !s->db) {
        return 0;
    }
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_count_nodes, "SELECT COUNT(*) FROM nodes WHERE project = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        return sqlite3_column_int(stmt, 0);
    }
    return 0;
}

int cbm_store_delete_nodes_by_project(cbm_store_t *s, const char *project) {
    sqlite3_stmt *stmt = prepare_cached(s, &s->stmt_delete_nodes_by_project,
                                        "DELETE FROM nodes WHERE project = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_nodes_by_project");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_nodes_by_file(cbm_store_t *s, const char *project, const char *file_path) {
    sqlite3_stmt *stmt = prepare_cached(s, &s->stmt_delete_nodes_by_file,
                                        "DELETE FROM nodes WHERE project = ?1 AND file_path = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, file_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_nodes_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_nodes_by_label(cbm_store_t *s, const char *project, const char *label) {
    sqlite3_stmt *stmt = prepare_cached(s, &s->stmt_delete_nodes_by_label,
                                        "DELETE FROM nodes WHERE project = ?1 AND label = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, label);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_nodes_by_label");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── Node batch ─────────────────────────────────────────────────── */

int cbm_store_upsert_node_batch(cbm_store_t *s, const cbm_node_t *nodes, int count,
                                int64_t *out_ids) {
    if (count == 0) {
        return CBM_STORE_OK;
    }

    exec_sql(s, "BEGIN;");
    for (int i = 0; i < count; i++) {
        int64_t id = cbm_store_upsert_node(s, &nodes[i]);
        if (id == CBM_STORE_ERR) {
            exec_sql(s, "ROLLBACK;");
            return CBM_STORE_ERR;
        }
        if (out_ids) {
            out_ids[i] = id;
        }
    }
    exec_sql(s, "COMMIT;");
    return CBM_STORE_OK;
}

/* ── Edge CRUD ──────────────────────────────────────────────────── */

int64_t cbm_store_insert_edge(cbm_store_t *s, const cbm_edge_t *e) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_insert_edge,
                       "INSERT INTO edges (project, source_id, target_id, type, properties) "
                       "VALUES (?1, ?2, ?3, ?4, ?5) "
                       "ON CONFLICT(source_id, target_id, type) DO UPDATE SET "
                       "properties = json_patch(properties, ?5) "
                       "RETURNING id;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, safe_str(e->project));
    sqlite3_bind_int64(stmt, 2, e->source_id);
    sqlite3_bind_int64(stmt, 3, e->target_id);
    bind_text(stmt, 4, safe_str(e->type));
    bind_text(stmt, 5, safe_props(e->properties_json));

    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(stmt, 0);
        sqlite3_reset(stmt); /* unblock COMMIT — RETURNING leaves stmt active */
        return id;
    }
    sqlite3_reset(stmt);
    store_set_error_sqlite(s, "insert_edge");
    return CBM_STORE_ERR;
}

/* Scan an edge from current row of stmt. */
static void scan_edge(sqlite3_stmt *stmt, cbm_edge_t *e) {
    e->id = sqlite3_column_int64(stmt, 0);
    e->project = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
    e->source_id = sqlite3_column_int64(stmt, 2);
    e->target_id = sqlite3_column_int64(stmt, 3);
    e->type = heap_strdup((const char *)sqlite3_column_text(stmt, 4));
    e->properties_json = heap_strdup((const char *)sqlite3_column_text(stmt, 5));
}

/* Generic: find multiple edges by a filter. */
static int find_edges_generic(cbm_store_t *s, sqlite3_stmt **slot, const char *sql,
                              void (*bind_fn)(sqlite3_stmt *, const void *), const void *bind_data,
                              cbm_edge_t **out, int *count) {
    if (!s || !s->db) {
        *out = NULL;
        *count = 0;
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = prepare_cached(s, slot, sql);
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_fn(stmt, bind_data);

    int cap = 16;
    int n = 0;
    cbm_edge_t *arr = malloc(cap * sizeof(cbm_edge_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_edge_t));
        }
        scan_edge(stmt, &arr[n]);
        n++;
    }

    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

/* Bind helpers for edge queries */
typedef struct {
    int64_t id;
} bind_id_t;
typedef struct {
    int64_t id;
    const char *type;
} bind_id_type_t;
typedef struct {
    const char *project;
    const char *type;
} bind_proj_type_t;

static void bind_source_id(sqlite3_stmt *stmt, const void *data) {
    const bind_id_t *b = data;
    sqlite3_bind_int64(stmt, 1, b->id);
}

static void bind_id_and_type(sqlite3_stmt *stmt, const void *data) {
    const bind_id_type_t *b = data;
    sqlite3_bind_int64(stmt, 1, b->id);
    bind_text(stmt, 2, b->type);
}

static void bind_proj_and_type(sqlite3_stmt *stmt, const void *data) {
    const bind_proj_type_t *b = data;
    bind_text(stmt, 1, b->project);
    bind_text(stmt, 2, b->type);
}

int cbm_store_find_edges_by_source(cbm_store_t *s, int64_t source_id, cbm_edge_t **out,
                                   int *count) {
    bind_id_t b = {source_id};
    return find_edges_generic(s, &s->stmt_find_edges_by_source,
                              "SELECT id, project, source_id, target_id, type, properties "
                              "FROM edges WHERE source_id = ?1;",
                              bind_source_id, &b, out, count);
}

int cbm_store_find_edges_by_target(cbm_store_t *s, int64_t target_id, cbm_edge_t **out,
                                   int *count) {
    bind_id_t b = {target_id};
    return find_edges_generic(s, &s->stmt_find_edges_by_target,
                              "SELECT id, project, source_id, target_id, type, properties "
                              "FROM edges WHERE target_id = ?1;",
                              bind_source_id, &b, out, count);
}

int cbm_store_find_edges_by_source_type(cbm_store_t *s, int64_t source_id, const char *type,
                                        cbm_edge_t **out, int *count) {
    bind_id_type_t b = {source_id, type};
    return find_edges_generic(s, &s->stmt_find_edges_by_source_type,
                              "SELECT id, project, source_id, target_id, type, properties "
                              "FROM edges WHERE source_id = ?1 AND type = ?2;",
                              bind_id_and_type, &b, out, count);
}

int cbm_store_find_edges_by_target_type(cbm_store_t *s, int64_t target_id, const char *type,
                                        cbm_edge_t **out, int *count) {
    bind_id_type_t b = {target_id, type};
    return find_edges_generic(s, &s->stmt_find_edges_by_target_type,
                              "SELECT id, project, source_id, target_id, type, properties "
                              "FROM edges WHERE target_id = ?1 AND type = ?2;",
                              bind_id_and_type, &b, out, count);
}

int cbm_store_find_edges_by_type(cbm_store_t *s, const char *project, const char *type,
                                 cbm_edge_t **out, int *count) {
    bind_proj_type_t b = {project, type};
    return find_edges_generic(s, &s->stmt_find_edges_by_type,
                              "SELECT id, project, source_id, target_id, type, properties "
                              "FROM edges WHERE project = ?1 AND type = ?2;",
                              bind_proj_and_type, &b, out, count);
}

int cbm_store_count_edges(cbm_store_t *s, const char *project) {
    if (!s || !s->db) {
        return 0;
    }
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_count_edges, "SELECT COUNT(*) FROM edges WHERE project = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        return sqlite3_column_int(stmt, 0);
    }
    return 0;
}

int cbm_store_count_edges_by_type(cbm_store_t *s, const char *project, const char *type) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_count_edges_by_type,
                       "SELECT COUNT(*) FROM edges WHERE project = ?1 AND type = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, type);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        return sqlite3_column_int(stmt, 0);
    }
    return 0;
}

int cbm_store_delete_edges_by_project(cbm_store_t *s, const char *project) {
    sqlite3_stmt *stmt = prepare_cached(s, &s->stmt_delete_edges_by_project,
                                        "DELETE FROM edges WHERE project = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_edges_by_project");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_edges_by_type(cbm_store_t *s, const char *project, const char *type) {
    sqlite3_stmt *stmt = prepare_cached(s, &s->stmt_delete_edges_by_type,
                                        "DELETE FROM edges WHERE project = ?1 AND type = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, type);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_edges_by_type");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── Edge batch ─────────────────────────────────────────────────── */

int cbm_store_insert_edge_batch(cbm_store_t *s, const cbm_edge_t *edges, int count) {
    if (count == 0) {
        return CBM_STORE_OK;
    }

    exec_sql(s, "BEGIN;");
    for (int i = 0; i < count; i++) {
        int64_t id = cbm_store_insert_edge(s, &edges[i]);
        if (id == CBM_STORE_ERR) {
            exec_sql(s, "ROLLBACK;");
            return CBM_STORE_ERR;
        }
    }
    exec_sql(s, "COMMIT;");
    return CBM_STORE_OK;
}

/* ── File hash CRUD ─────────────────────────────────────────────── */

int cbm_store_upsert_file_hash(cbm_store_t *s, const char *project, const char *rel_path,
                               const char *sha256, int64_t mtime_ns, int64_t size) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_upsert_file_hash,
                       "INSERT INTO file_hashes (project, rel_path, sha256, mtime_ns, size) "
                       "VALUES (?1, ?2, ?3, ?4, ?5) "
                       "ON CONFLICT(project, rel_path) DO UPDATE SET "
                       "sha256=?3, mtime_ns=?4, size=?5;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, rel_path);
    bind_text(stmt, 3, sha256);
    sqlite3_bind_int64(stmt, 4, mtime_ns);
    sqlite3_bind_int64(stmt, 5, size);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_file_hash");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_get_file_hashes(cbm_store_t *s, const char *project, cbm_file_hash_t **out,
                              int *count) {
    sqlite3_stmt *stmt = prepare_cached(s, &s->stmt_get_file_hashes,
                                        "SELECT project, rel_path, sha256, mtime_ns, size "
                                        "FROM file_hashes WHERE project = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);

    int cap = 16;
    int n = 0;
    cbm_file_hash_t *arr = malloc(cap * sizeof(cbm_file_hash_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_file_hash_t));
        }
        arr[n].project = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].rel_path = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
        arr[n].sha256 = heap_strdup((const char *)sqlite3_column_text(stmt, 2));
        arr[n].mtime_ns = sqlite3_column_int64(stmt, 3);
        arr[n].size = sqlite3_column_int64(stmt, 4);
        n++;
    }

    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

int cbm_store_delete_file_hash(cbm_store_t *s, const char *project, const char *rel_path) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_delete_file_hash,
                       "DELETE FROM file_hashes WHERE project = ?1 AND rel_path = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_file_hash");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_file_hashes(cbm_store_t *s, const char *project) {
    sqlite3_stmt *stmt = prepare_cached(s, &s->stmt_delete_file_hashes,
                                        "DELETE FROM file_hashes WHERE project = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_file_hashes");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── FindNodesByFileOverlap ─────────────────────────────────────── */

int cbm_store_find_nodes_by_file_overlap(cbm_store_t *s, const char *project, const char *file_path,
                                         int start_line, int end_line, cbm_node_t **out,
                                         int *count) {
    *out = NULL;
    *count = 0;
    const char *sql = "SELECT id, project, label, name, qualified_name, file_path, "
                      "start_line, end_line, properties FROM nodes "
                      "WHERE project = ?1 AND file_path = ?2 "
                      "AND label NOT IN ('Module', 'Package', 'File', 'Folder') "
                      "AND start_line <= ?4 AND end_line >= ?3 "
                      "ORDER BY start_line";

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "overlap prepare");
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, file_path);
    sqlite3_bind_int(stmt, 3, start_line);
    sqlite3_bind_int(stmt, 4, end_line);

    int cap = 8;
    int n = 0;
    cbm_node_t *nodes = malloc(cap * sizeof(cbm_node_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            nodes = safe_realloc(nodes, cap * sizeof(cbm_node_t));
        }
        memset(&nodes[n], 0, sizeof(cbm_node_t));
        scan_node(stmt, &nodes[n]);
        n++;
    }
    sqlite3_finalize(stmt);
    *out = nodes;
    *count = n;
    return CBM_STORE_OK;
}

/* ── FindNodesByQNSuffix ───────────────────────────────────────── */

int cbm_store_find_nodes_by_qn_suffix(cbm_store_t *s, const char *project, const char *suffix,
                                      cbm_node_t **out, int *count) {
    *out = NULL;
    *count = 0;
    if (!s || !s->db) {
        return CBM_STORE_ERR;
    }
    /* Match QNs ending with ".suffix" or exactly equal to suffix */
    char like_pattern[512];
    snprintf(like_pattern, sizeof(like_pattern), "%%.%s", suffix);

    const char *sql_with_project =
        "SELECT id, project, label, name, qualified_name, file_path, "
        "start_line, end_line, properties FROM nodes "
        "WHERE project = ?1 AND (qualified_name LIKE ?2 OR qualified_name = ?3)";
    const char *sql_any = "SELECT id, project, label, name, qualified_name, file_path, "
                          "start_line, end_line, properties FROM nodes "
                          "WHERE (qualified_name LIKE ?1 OR qualified_name = ?2)";

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, project ? sql_with_project : sql_any, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "qn_suffix prepare");
        return CBM_STORE_ERR;
    }

    if (project) {
        bind_text(stmt, 1, project);
        bind_text(stmt, 2, like_pattern);
        bind_text(stmt, 3, suffix);
    } else {
        bind_text(stmt, 1, like_pattern);
        bind_text(stmt, 2, suffix);
    }

    int cap = 8;
    int n = 0;
    cbm_node_t *nodes = malloc(cap * sizeof(cbm_node_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            nodes = safe_realloc(nodes, cap * sizeof(cbm_node_t));
        }
        memset(&nodes[n], 0, sizeof(cbm_node_t));
        scan_node(stmt, &nodes[n]);
        n++;
    }
    sqlite3_finalize(stmt);
    *out = nodes;
    *count = n;
    return CBM_STORE_OK;
}

/* ── NodeDegree ────────────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
void cbm_store_node_degree(cbm_store_t *s, int64_t node_id, int *in_deg, int *out_deg) {
    *in_deg = 0;
    *out_deg = 0;

    const char *in_sql = "SELECT COUNT(*) FROM edges WHERE target_id = ?1 AND type = 'CALLS'";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, in_sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            *in_deg = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    const char *out_sql = "SELECT COUNT(*) FROM edges WHERE source_id = ?1 AND type = 'CALLS'";
    if (sqlite3_prepare_v2(s->db, out_sql, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            *out_deg = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }
}

/* ── Node neighbor names ──────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
static int query_neighbor_names(sqlite3 *db, const char *sql, int64_t node_id, int limit,
                                char ***out, int *out_count) {
    *out = NULL;
    *out_count = 0;
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return -1;
    }
    sqlite3_bind_int64(stmt, 1, node_id);
    sqlite3_bind_int(stmt, 2, limit);

    int cap = 8;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **names = malloc((size_t)cap * sizeof(char *));
    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        if (!name) {
            continue;
        }
        if (count >= cap) {
            cap *= 2;
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            names = safe_realloc(names, (size_t)cap * sizeof(char *));
        }
        // NOLINTNEXTLINE(misc-include-cleaner) — strdup provided by standard header
        names[count++] = strdup(name);
    }
    sqlite3_finalize(stmt);
    *out = names;
    *out_count = count;
    return 0;
}

int cbm_store_node_neighbor_names(cbm_store_t *s, int64_t node_id, int limit, char ***out_callers,
                                  int *caller_count, char ***out_callees, int *callee_count) {
    if (!s) {
        return -1;
    }
    *out_callers = NULL;
    *caller_count = 0;
    *out_callees = NULL;
    *callee_count = 0;

    query_neighbor_names(
        s->db,
        "SELECT DISTINCT n.name FROM edges e JOIN nodes n ON e.source_id = n.id "
        "WHERE e.target_id = ?1 AND e.type IN ('CALLS','HTTP_CALLS','ASYNC_CALLS') "
        "ORDER BY n.name LIMIT ?2",
        node_id, limit, out_callers, caller_count);

    query_neighbor_names(
        s->db,
        "SELECT DISTINCT n.name FROM edges e JOIN nodes n ON e.target_id = n.id "
        "WHERE e.source_id = ?1 AND e.type IN ('CALLS','HTTP_CALLS','ASYNC_CALLS') "
        "ORDER BY n.name LIMIT ?2",
        node_id, limit, out_callees, callee_count);

    return 0;
}

int cbm_store_batch_count_degrees(cbm_store_t *s, const int64_t *node_ids, int id_count,
                                  const char *edge_type, int *out_in, int *out_out) {
    if (!s || !node_ids || id_count <= 0 || !out_in || !out_out) {
        return CBM_STORE_ERR;
    }

    memset(out_in, 0, (size_t)id_count * sizeof(int));
    memset(out_out, 0, (size_t)id_count * sizeof(int));

    /* Build IN clause: (?,?,?) */
    char in_clause[4096];
    int pos = 0;
    for (int i = 0; i < id_count && pos < (int)sizeof(in_clause) - 4; i++) {
        if (i > 0) {
            in_clause[pos++] = ',';
        }
        in_clause[pos++] = '?';
    }
    in_clause[pos] = '\0';

    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool has_type = edge_type && edge_type[0] != '\0';

    /* Inbound: COUNT grouped by target_id */
    char sql[8192];
    if (has_type) {
        snprintf(sql, sizeof(sql),
                 "SELECT target_id, COUNT(*) FROM edges "
                 "WHERE target_id IN (%s) AND type = ? GROUP BY target_id",
                 in_clause);
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT target_id, COUNT(*) FROM edges "
                 "WHERE target_id IN (%s) GROUP BY target_id",
                 in_clause);
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }

    for (int i = 0; i < id_count; i++) {
        sqlite3_bind_int64(stmt, i + 1, node_ids[i]);
    }
    if (has_type) {
        bind_text(stmt, id_count + 1, edge_type);
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t nid = sqlite3_column_int64(stmt, 0);
        int cnt = sqlite3_column_int(stmt, 1);
        for (int i = 0; i < id_count; i++) {
            if (node_ids[i] == nid) {
                out_in[i] = cnt;
                break;
            }
        }
    }
    sqlite3_finalize(stmt);

    /* Outbound: COUNT grouped by source_id */
    if (has_type) {
        snprintf(sql, sizeof(sql),
                 "SELECT source_id, COUNT(*) FROM edges "
                 "WHERE source_id IN (%s) AND type = ? GROUP BY source_id",
                 in_clause);
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT source_id, COUNT(*) FROM edges "
                 "WHERE source_id IN (%s) GROUP BY source_id",
                 in_clause);
    }

    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }

    for (int i = 0; i < id_count; i++) {
        sqlite3_bind_int64(stmt, i + 1, node_ids[i]);
    }
    if (has_type) {
        bind_text(stmt, id_count + 1, edge_type);
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t nid = sqlite3_column_int64(stmt, 0);
        int cnt = sqlite3_column_int(stmt, 1);
        for (int i = 0; i < id_count; i++) {
            if (node_ids[i] == nid) {
                out_out[i] = cnt;
                break;
            }
        }
    }
    sqlite3_finalize(stmt);

    return CBM_STORE_OK;
}

/* ── UpsertFileHashBatch ───────────────────────────────────────── */

int cbm_store_upsert_file_hash_batch(cbm_store_t *s, const cbm_file_hash_t *hashes, int count) {
    if (count == 0) {
        return CBM_STORE_OK;
    }

    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    for (int i = 0; i < count; i++) {
        rc = cbm_store_upsert_file_hash(s, hashes[i].project, hashes[i].rel_path, hashes[i].sha256,
                                        hashes[i].mtime_ns, hashes[i].size);
        if (rc != CBM_STORE_OK) {
            cbm_store_rollback(s);
            return rc;
        }
    }

    return cbm_store_commit(s);
}

/* ── FindEdgesByURLPath ────────────────────────────────────────── */

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_store_find_edges_by_url_path(cbm_store_t *s, const char *project, const char *keyword,
                                     cbm_edge_t **out, int *count) {
    *out = NULL;
    *count = 0;

    /* Search properties JSON for url_path containing keyword */
    char like_pattern[512];
    snprintf(like_pattern, sizeof(like_pattern), "%%\"url_path\":\"%%%%%s%%%%\"%%", keyword);

    const char *sql = "SELECT id, project, source_id, target_id, type, properties FROM edges "
                      "WHERE project = ?1 AND properties LIKE ?2";

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "url_path prepare");
        return CBM_STORE_ERR;
    }

    bind_text(stmt, 1, project);
    bind_text(stmt, 2, like_pattern);

    int cap = 8;
    int n = 0;
    cbm_edge_t *edges = malloc(cap * sizeof(cbm_edge_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            edges = safe_realloc(edges, cap * sizeof(cbm_edge_t));
        }
        memset(&edges[n], 0, sizeof(cbm_edge_t));
        scan_edge(stmt, &edges[n]);
        n++;
    }
    sqlite3_finalize(stmt);
    *out = edges;
    *count = n;
    return CBM_STORE_OK;
}

/* ── RestoreFrom ───────────────────────────────────────────────── */

int cbm_store_restore_from(cbm_store_t *dst, cbm_store_t *src) {
    sqlite3_backup *bk = sqlite3_backup_init(dst->db, "main", src->db, "main");
    if (!bk) {
        store_set_error_sqlite(dst, "backup init");
        return CBM_STORE_ERR;
    }
    int rc = sqlite3_backup_step(bk, -1); /* copy all pages */
    sqlite3_backup_finish(bk);

    if (rc != SQLITE_DONE) {
        store_set_error(dst, "backup step failed");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── Search ─────────────────────────────────────────────────────── */

/* Convert a glob pattern to SQL LIKE pattern. */
char *cbm_glob_to_like(const char *pattern) {
    if (!pattern) {
        return NULL;
    }
    size_t len = strlen(pattern);
    char *out = malloc((len * 2) + 1);
    size_t j = 0;

    for (size_t i = 0; i < len; i++) {
        if (pattern[i] == '*' && i + 1 < len && pattern[i + 1] == '*') {
            /* Remove leading / from output if present (handles glob dir-star) */
            if (j > 0 && out[j - 1] == '/') {
                j--;
            }
            out[j++] = '%';
            i++; /* skip second * */
            if (i + 1 < len && pattern[i + 1] == '/') {
                i++; /* skip trailing / */
            }
        } else if (pattern[i] == '*') {
            out[j++] = '%';
        } else if (pattern[i] == '?') {
            out[j++] = '_';
        } else {
            out[j++] = pattern[i];
        }
    }
    out[j] = '\0';
    return out;
}

/* ── extractLikeHints ─────────────────────────────────────────── */

int cbm_extract_like_hints(const char *pattern, char **out, int max_out) {
    if (!pattern || !out || max_out <= 0) {
        return 0;
    }

    /* Bail on alternation — can't convert OR regex to AND LIKE */
    for (const char *p = pattern; *p; p++) {
        if (*p == '|') {
            return 0;
        }
    }

    int count = 0;
    char buf[256];
    int blen = 0;

    int i = 0;
    while (pattern[i]) {
        char ch = pattern[i];
        switch (ch) {
        case '\\':
            /* Escaped char — the next char is literal */
            if (pattern[i + 1]) {
                if (blen < (int)sizeof(buf) - 1) {
                    buf[blen++] = pattern[i + 1];
                }
                i += 2;
            } else {
                i++;
            }
            break;
        case '.':
        case '*':
        case '+':
        case '?':
        case '^':
        case '$':
        case '(':
        case ')':
        case '[':
        case ']':
        case '{':
        case '}':
            /* Meta character — flush current literal segment */
            if (blen >= 3 && count < max_out) {
                buf[blen] = '\0';
                out[count++] = strdup(buf);
            }
            blen = 0;
            i++;
            break;
        default:
            if (blen < (int)sizeof(buf) - 1) {
                buf[blen++] = ch;
            }
            i++;
            break;
        }
    }
    /* Flush trailing segment */
    if (blen >= 3 && count < max_out) {
        buf[blen] = '\0';
        out[count++] = strdup(buf);
    }
    return count;
}

/* ── ensureCaseInsensitive / stripCaseFlag ────────────────────── */

const char *cbm_ensure_case_insensitive(const char *pattern) {
    static char buf[2048];
    if (!pattern) {
        buf[0] = '\0';
        return buf;
    }
    /* Already has (?i) prefix? Return as-is. */
    if (strncmp(pattern, "(?i)", 4) == 0) {
        snprintf(buf, sizeof(buf), "%s", pattern);
    } else {
        snprintf(buf, sizeof(buf), "(?i)%s", pattern);
    }
    return buf;
}

const char *cbm_strip_case_flag(const char *pattern) {
    static char buf[2048];
    if (!pattern) {
        buf[0] = '\0';
        return buf;
    }
    if (strncmp(pattern, "(?i)", 4) == 0) {
        snprintf(buf, sizeof(buf), "%s", pattern + 4);
    } else {
        snprintf(buf, sizeof(buf), "%s", pattern);
    }
    return buf;
}

int cbm_store_search(cbm_store_t *s, const cbm_search_params_t *params, cbm_search_output_t *out) {
    memset(out, 0, sizeof(*out));
    if (!s || !s->db) {
        return CBM_STORE_ERR;
    }

    /* Build WHERE clauses dynamically */
    char sql[4096];
    char count_sql[4096];
    int bind_idx = 0;

    /* We build a query that selects nodes with optional degree subqueries */
    const char *select_cols =
        "SELECT n.id, n.project, n.label, n.name, n.qualified_name, "
        "n.file_path, n.start_line, n.end_line, n.properties, "
        "(SELECT COUNT(*) FROM edges e WHERE e.target_id = n.id AND e.type = 'CALLS') AS in_deg, "
        "(SELECT COUNT(*) FROM edges e WHERE e.source_id = n.id AND e.type = 'CALLS') AS out_deg ";

    /* Start building WHERE */
    char where[2048] = "";
    int wlen = 0;
    int nparams = 0;

    /* Track bind values */
    struct {
        enum { BV_TEXT } type;
        const char *text;
    } binds[16];

#define ADD_WHERE(cond)                                                    \
    do {                                                                   \
        if (nparams > 0)                                                   \
            wlen += snprintf(where + wlen, sizeof(where) - wlen, " AND "); \
        wlen += snprintf(where + wlen, sizeof(where) - wlen, "%s", cond);  \
        nparams++;                                                         \
    } while (0)

#define BIND_TEXT(val)                      \
    do {                                    \
        bind_idx++;                         \
        binds[bind_idx - 1].type = BV_TEXT; \
        binds[bind_idx - 1].text = val;     \
    } while (0)

    char bind_buf[64];
    char *like_pattern = NULL;

    if (params->project) {
        snprintf(bind_buf, sizeof(bind_buf), "n.project = ?%d", bind_idx + 1);
        ADD_WHERE(bind_buf);
        BIND_TEXT(params->project);
    }
    if (params->label) {
        snprintf(bind_buf, sizeof(bind_buf), "n.label = ?%d", bind_idx + 1);
        ADD_WHERE(bind_buf);
        BIND_TEXT(params->label);
    }
    if (params->name_pattern) {
        if (params->case_sensitive) {
            /* Case-sensitive: use built-in REGEXP operator */
            snprintf(bind_buf, sizeof(bind_buf), "n.name REGEXP ?%d", bind_idx + 1);
        } else {
            /* Case-insensitive: use iregexp() function call syntax */
            snprintf(bind_buf, sizeof(bind_buf), "iregexp(?%d, n.name)", bind_idx + 1);
        }
        ADD_WHERE(bind_buf);
        BIND_TEXT(params->name_pattern);
    }
    if (params->file_pattern) {
        like_pattern = cbm_glob_to_like(params->file_pattern);
        snprintf(bind_buf, sizeof(bind_buf), "n.file_path LIKE ?%d", bind_idx + 1);
        ADD_WHERE(bind_buf);
        BIND_TEXT(like_pattern);
    }

    /* Exclude labels: add NOT IN clause directly (no bind params — values are code-provided) */
    if (params->exclude_labels) {
        char excl_clause[512] = "n.label NOT IN (";
        int elen = (int)strlen(excl_clause);
        for (int i = 0; params->exclude_labels[i]; i++) {
            if (i > 0) {
                elen += snprintf(excl_clause + elen, sizeof(excl_clause) - elen, ",");
            }
            elen += snprintf(excl_clause + elen, sizeof(excl_clause) - elen, "'%s'",
                             params->exclude_labels[i]);
        }
        snprintf(excl_clause + elen, sizeof(excl_clause) - (size_t)elen, ")");
        ADD_WHERE(excl_clause);
    }

    /* Build full SQL */
    if (nparams > 0) {
        snprintf(sql, sizeof(sql), "%s FROM nodes n WHERE %s", select_cols, where);
    } else {
        snprintf(sql, sizeof(sql), "%s FROM nodes n", select_cols);
    }

    /* Degree filters: -1 = no filter, 0+ = active filter.
     * Wraps in subquery to filter on computed degree columns. */
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool has_degree_filter = (params->min_degree >= 0 || params->max_degree >= 0);
    if (has_degree_filter) {
        char inner_sql[4096];
        snprintf(inner_sql, sizeof(inner_sql), "%s", sql);
        if (params->min_degree >= 0 && params->max_degree >= 0) {
            snprintf(
                sql, sizeof(sql),
                "SELECT * FROM (%s) WHERE (in_deg + out_deg) >= %d AND (in_deg + out_deg) <= %d",
                inner_sql, params->min_degree, params->max_degree);
        } else if (params->min_degree >= 0) {
            snprintf(sql, sizeof(sql), "SELECT * FROM (%s) WHERE (in_deg + out_deg) >= %d",
                     inner_sql, params->min_degree);
        } else {
            snprintf(sql, sizeof(sql), "SELECT * FROM (%s) WHERE (in_deg + out_deg) <= %d",
                     inner_sql, params->max_degree);
        }
    }

    /* Count query (wrap the full query) */
    snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM (%s)", sql);

    /* Add ORDER BY + LIMIT.
     * When degree filter wraps in subquery, column refs lose the "n." prefix. */
    int limit = params->limit > 0 ? params->limit : 500000;
    int offset = params->offset;
    bool has_degree_wrap = has_degree_filter;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    const char *name_col = has_degree_wrap ? "name" : "n.name";
    char order_limit[128];
    snprintf(order_limit, sizeof(order_limit), " ORDER BY %s LIMIT %d OFFSET %d", name_col, limit,
             offset);
    strncat(sql, order_limit, sizeof(sql) - strlen(sql) - 1);

    /* Execute count query */
    sqlite3_stmt *cnt_stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, count_sql, -1, &cnt_stmt, NULL);
    if (rc == SQLITE_OK) {
        for (int i = 0; i < bind_idx; i++) {
            bind_text(cnt_stmt, i + 1, binds[i].text);
        }
        if (sqlite3_step(cnt_stmt) == SQLITE_ROW) {
            out->total = sqlite3_column_int(cnt_stmt, 0);
        }
        sqlite3_finalize(cnt_stmt);
    }

    /* Execute main query */
    sqlite3_stmt *main_stmt = NULL;
    rc = sqlite3_prepare_v2(s->db, sql, -1, &main_stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "search prepare");
        free(like_pattern);
        return CBM_STORE_ERR;
    }

    for (int i = 0; i < bind_idx; i++) {
        bind_text(main_stmt, i + 1, binds[i].text);
    }

    int cap = 16;
    int n = 0;
    cbm_search_result_t *results = malloc(cap * sizeof(cbm_search_result_t));

    while (sqlite3_step(main_stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            results = safe_realloc(results, cap * sizeof(cbm_search_result_t));
        }
        memset(&results[n], 0, sizeof(cbm_search_result_t));
        scan_node(main_stmt, &results[n].node);
        results[n].in_degree = sqlite3_column_int(main_stmt, 9);
        results[n].out_degree = sqlite3_column_int(main_stmt, 10);
        n++;
    }

    sqlite3_finalize(main_stmt);
    free(like_pattern);

    out->results = results;
    out->count = n;
    return CBM_STORE_OK;
}

void cbm_store_search_free(cbm_search_output_t *out) {
    if (!out) {
        return;
    }
    for (int i = 0; i < out->count; i++) {
        cbm_search_result_t *r = &out->results[i];
        free((void *)r->node.project);
        free((void *)r->node.label);
        free((void *)r->node.name);
        free((void *)r->node.qualified_name);
        free((void *)r->node.file_path);
        free((void *)r->node.properties_json);
        for (int j = 0; j < r->connected_count; j++) {
            free((void *)r->connected_names[j]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(r->connected_names);
    }
    free(out->results);
    memset(out, 0, sizeof(*out));
}

/* ── BFS Traversal ──────────────────────────────────────────────── */

int cbm_store_bfs(cbm_store_t *s, int64_t start_id, const char *direction, const char **edge_types,
                  // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                  int edge_type_count, int max_depth, int max_results, cbm_traverse_result_t *out) {
    memset(out, 0, sizeof(*out));

    /* Load root node */
    cbm_node_t root = {0};
    int rc = cbm_store_find_node_by_id(s, start_id, &root);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    out->root = root;

    /* Build edge type IN clause */
    char types_clause[512] = "'CALLS'";
    if (edge_type_count > 0) {
        int tlen = 0;
        for (int i = 0; i < edge_type_count; i++) {
            if (i > 0) {
                tlen += snprintf(types_clause + tlen, sizeof(types_clause) - tlen, ",");
            }
            tlen +=
                snprintf(types_clause + tlen, sizeof(types_clause) - tlen, "'%s'", edge_types[i]);
        }
    }

    /* Build recursive CTE for BFS */
    char sql[4096];
    const char *join_cond;
    const char *next_id;
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    bool is_inbound = direction && strcmp(direction, "inbound") == 0;

    if (is_inbound) {
        join_cond = "e.target_id = bfs.node_id";
        next_id = "e.source_id";
    } else {
        join_cond = "e.source_id = bfs.node_id";
        next_id = "e.target_id";
    }

    snprintf(sql, sizeof(sql),
             "WITH RECURSIVE bfs(node_id, hop) AS ("
             "  SELECT %lld, 0"
             "  UNION"
             "  SELECT %s, bfs.hop + 1"
             "  FROM bfs"
             "  JOIN edges e ON %s"
             "  WHERE e.type IN (%s) AND bfs.hop < %d"
             ")"
             "SELECT DISTINCT n.id, n.project, n.label, n.name, n.qualified_name, "
             "n.file_path, n.start_line, n.end_line, n.properties, bfs.hop "
             "FROM bfs "
             "JOIN nodes n ON n.id = bfs.node_id "
             "WHERE bfs.hop > 0 " /* exclude root */
             "ORDER BY bfs.hop "
             "LIMIT %d;",
             (long long)start_id, next_id, join_cond, types_clause, max_depth, max_results);

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "bfs prepare");
        return CBM_STORE_ERR;
    }

    int cap = 16;
    int n = 0;
    cbm_node_hop_t *visited = malloc(cap * sizeof(cbm_node_hop_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            visited = safe_realloc(visited, cap * sizeof(cbm_node_hop_t));
        }
        scan_node(stmt, &visited[n].node);
        visited[n].hop = sqlite3_column_int(stmt, 9);
        n++;
    }

    sqlite3_finalize(stmt);

    out->visited = visited;
    out->visited_count = n;

    /* Collect edges between visited nodes (including root) */
    if (n > 0) {
        /* Build ID set: root + all visited */
        char id_set[4096];
        int ilen = snprintf(id_set, sizeof(id_set), "%lld", (long long)start_id);
        for (int i = 0; i < n; i++) {
            ilen += snprintf(id_set + ilen, sizeof(id_set) - ilen, ",%lld",
                             (long long)out->visited[i].node.id);
        }

        char edge_sql[8192];
        snprintf(edge_sql, sizeof(edge_sql),
                 "SELECT n1.name, n2.name, e.type "
                 "FROM edges e "
                 "JOIN nodes n1 ON n1.id = e.source_id "
                 "JOIN nodes n2 ON n2.id = e.target_id "
                 "WHERE e.source_id IN (%s) AND e.target_id IN (%s) "
                 "AND e.type IN (%s)",
                 id_set, id_set, types_clause);

        sqlite3_stmt *estmt = NULL;
        rc = sqlite3_prepare_v2(s->db, edge_sql, -1, &estmt, NULL);
        if (rc == SQLITE_OK) {
            int ecap = 8;
            int en = 0;
            cbm_edge_info_t *edges = malloc(ecap * sizeof(cbm_edge_info_t));

            while (sqlite3_step(estmt) == SQLITE_ROW) {
                if (en >= ecap) {
                    ecap *= 2;
                    edges = safe_realloc(edges, ecap * sizeof(cbm_edge_info_t));
                }
                edges[en].from_name = heap_strdup((const char *)sqlite3_column_text(estmt, 0));
                edges[en].to_name = heap_strdup((const char *)sqlite3_column_text(estmt, 1));
                edges[en].type = heap_strdup((const char *)sqlite3_column_text(estmt, 2));
                edges[en].confidence = 1.0;
                en++;
            }
            sqlite3_finalize(estmt);

            out->edges = edges;
            out->edge_count = en;
        }
    } else {
        out->edges = NULL;
        out->edge_count = 0;
    }

    return CBM_STORE_OK;
}

void cbm_store_traverse_free(cbm_traverse_result_t *out) {
    if (!out) {
        return;
    }
    /* Free root */
    free((void *)out->root.project);
    free((void *)out->root.label);
    free((void *)out->root.name);
    free((void *)out->root.qualified_name);
    free((void *)out->root.file_path);
    free((void *)out->root.properties_json);

    /* Free visited */
    for (int i = 0; i < out->visited_count; i++) {
        cbm_node_hop_t *h = &out->visited[i];
        free((void *)h->node.project);
        free((void *)h->node.label);
        free((void *)h->node.name);
        free((void *)h->node.qualified_name);
        free((void *)h->node.file_path);
        free((void *)h->node.properties_json);
    }
    free(out->visited);

    /* Free edges */
    for (int i = 0; i < out->edge_count; i++) {
        free((void *)out->edges[i].from_name);
        free((void *)out->edges[i].to_name);
        free((void *)out->edges[i].type);
    }
    free(out->edges);

    memset(out, 0, sizeof(*out));
}

/* ── Impact analysis ────────────────────────────────────────────── */

cbm_risk_level_t cbm_hop_to_risk(int hop) {
    switch (hop) {
    case 1:
        return CBM_RISK_CRITICAL;
    case 2:
        return CBM_RISK_HIGH;
    case 3:
        return CBM_RISK_MEDIUM;
    default:
        return CBM_RISK_LOW;
    }
}

const char *cbm_risk_label(cbm_risk_level_t level) {
    switch (level) {
    case CBM_RISK_CRITICAL:
        return "CRITICAL";
    case CBM_RISK_HIGH:
        return "HIGH";
    case CBM_RISK_MEDIUM:
        return "MEDIUM";
    case CBM_RISK_LOW:
    default:
        return "LOW";
    }
}

cbm_impact_summary_t cbm_build_impact_summary(const cbm_node_hop_t *hops, int hop_count,
                                              const cbm_edge_info_t *edges, int edge_count) {
    cbm_impact_summary_t s = {0};
    for (int i = 0; i < hop_count; i++) {
        switch (cbm_hop_to_risk(hops[i].hop)) {
        case CBM_RISK_CRITICAL:
            s.critical++;
            break;
        case CBM_RISK_HIGH:
            s.high++;
            break;
        case CBM_RISK_MEDIUM:
            s.medium++;
            break;
        case CBM_RISK_LOW:
            s.low++;
            break;
        }
        s.total++;
    }
    for (int i = 0; i < edge_count; i++) {
        if (edges[i].type && (strcmp(edges[i].type, "HTTP_CALLS") == 0 ||
                              strcmp(edges[i].type, "ASYNC_CALLS") == 0)) {
            s.has_cross_service = true;
            break;
        }
    }
    return s;
}

int cbm_deduplicate_hops(const cbm_node_hop_t *hops, int hop_count, cbm_node_hop_t **out,
                         int *out_count) {
    *out = NULL;
    *out_count = 0;
    if (hop_count == 0) {
        return CBM_STORE_OK;
    }

    /* Simple O(n²) dedup — keep minimum hop per node ID */
    cbm_node_hop_t *result = malloc(hop_count * sizeof(cbm_node_hop_t));
    int n = 0;

    for (int i = 0; i < hop_count; i++) {
        int found = -1;
        for (int j = 0; j < n; j++) {
            if (result[j].node.id == hops[i].node.id) {
                found = j;
                break;
            }
        }
        if (found >= 0) {
            if (hops[i].hop < result[found].hop) {
                result[found].hop = hops[i].hop;
            }
        } else {
            result[n] = hops[i];
            n++;
        }
    }

    *out = safe_realloc(result, n * sizeof(cbm_node_hop_t));
    *out_count = n;
    return CBM_STORE_OK;
}

/* ── Schema ─────────────────────────────────────────────────────── */

int cbm_store_get_schema(cbm_store_t *s, const char *project, cbm_schema_info_t *out) {
    memset(out, 0, sizeof(*out));
    if (!s || !s->db) {
        return -1;
    }

    /* Node labels */
    {
        const char *sql = "SELECT label, COUNT(*) FROM nodes WHERE project = ?1 GROUP BY label "
                          "ORDER BY COUNT(*) DESC;";
        sqlite3_stmt *stmt = NULL;
        sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
        bind_text(stmt, 1, project);

        int cap = 8;
        int n = 0;
        cbm_label_count_t *arr = malloc(cap * sizeof(cbm_label_count_t));
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (n >= cap) {
                cap *= 2;
                arr = safe_realloc(arr, cap * sizeof(cbm_label_count_t));
            }
            arr[n].label = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
            arr[n].count = sqlite3_column_int(stmt, 1);
            n++;
        }
        sqlite3_finalize(stmt);
        out->node_labels = arr;
        out->node_label_count = n;
    }

    /* Edge types */
    {
        const char *sql = "SELECT type, COUNT(*) FROM edges WHERE project = ?1 GROUP BY type ORDER "
                          "BY COUNT(*) DESC;";
        sqlite3_stmt *stmt = NULL;
        sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
        bind_text(stmt, 1, project);

        int cap = 8;
        int n = 0;
        cbm_type_count_t *arr = malloc(cap * sizeof(cbm_type_count_t));
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (n >= cap) {
                cap *= 2;
                arr = safe_realloc(arr, cap * sizeof(cbm_type_count_t));
            }
            arr[n].type = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
            arr[n].count = sqlite3_column_int(stmt, 1);
            n++;
        }
        sqlite3_finalize(stmt);
        out->edge_types = arr;
        out->edge_type_count = n;
    }

    return CBM_STORE_OK;
}

void cbm_store_schema_free(cbm_schema_info_t *out) {
    if (!out) {
        return;
    }
    for (int i = 0; i < out->node_label_count; i++) {
        free((void *)out->node_labels[i].label);
    }
    free(out->node_labels);

    for (int i = 0; i < out->edge_type_count; i++) {
        free((void *)out->edge_types[i].type);
    }
    free(out->edge_types);

    for (int i = 0; i < out->rel_pattern_count; i++) {
        free((void *)out->rel_patterns[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(out->rel_patterns);

    for (int i = 0; i < out->sample_func_count; i++) {
        free((void *)out->sample_func_names[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(out->sample_func_names);

    for (int i = 0; i < out->sample_class_count; i++) {
        free((void *)out->sample_class_names[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(out->sample_class_names);

    for (int i = 0; i < out->sample_qn_count; i++) {
        free((void *)out->sample_qns[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(out->sample_qns);

    memset(out, 0, sizeof(*out));
}

/* ── Architecture helpers ───────────────────────────────────────── */

/* Extract sub-package from QN: project.dir1.dir2.sym → dir1 (4+ parts → [2], else [1]) */
const char *cbm_qn_to_package(const char *qn) {
    if (!qn || !qn[0]) {
        return "";
    }
    static CBM_TLS char buf[256];
    /* Find dots and extract segment */
    const char *dots[5] = {NULL};
    int ndots = 0;
    for (const char *p = qn; *p && ndots < 5; p++) {
        if (*p == '.') {
            dots[ndots++] = p;
        }
    }
    /* 4+ segments: return segment[2] */
    if (ndots >= 3) {
        const char *start = dots[1] + 1;
        int len = (int)(dots[2] - start);
        if (len > 0 && len < (int)sizeof(buf)) {
            memcpy(buf, start, len);
            buf[len] = '\0';
            return buf;
        }
    }
    /* 2+ segments: return segment[1] */
    if (ndots >= 1) {
        const char *start = dots[0] + 1;
        const char *end = (ndots >= 2) ? dots[1] : qn + strlen(qn);
        int len = (int)(end - start);
        if (len > 0 && len < (int)sizeof(buf)) {
            memcpy(buf, start, len);
            buf[len] = '\0';
            return buf;
        }
    }
    return "";
}

/* Extract top-level package from QN: project.dir1.rest → dir1 (segment[1]) */
const char *cbm_qn_to_top_package(const char *qn) {
    if (!qn || !qn[0]) {
        return "";
    }
    static CBM_TLS char buf[256];
    const char *first_dot = strchr(qn, '.');
    if (!first_dot) {
        return "";
    }
    const char *start = first_dot + 1;
    const char *second_dot = strchr(start, '.');
    const char *end = second_dot ? second_dot : qn + strlen(qn);
    int len = (int)(end - start);
    if (len > 0 && len < (int)sizeof(buf)) {
        memcpy(buf, start, len);
        buf[len] = '\0';
        return buf;
    }
    return "";
}

bool cbm_is_test_file_path(const char *fp) {
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    return fp && fp[0] && strstr(fp, "test") != NULL;
}

/* File extension → language name mapping */
static const char *ext_to_lang(const char *ext) {
    if (!ext) {
        return NULL;
    }
    /* Common extensions */
    if (strcmp(ext, ".py") == 0) {
        return "Python";
    }
    if (strcmp(ext, ".go") == 0) {
        return "Go";
    }
    if (strcmp(ext, ".js") == 0 || strcmp(ext, ".jsx") == 0) {
        return "JavaScript";
    }
    if (strcmp(ext, ".ts") == 0 || strcmp(ext, ".tsx") == 0) {
        return "TypeScript";
    }
    if (strcmp(ext, ".rs") == 0) {
        return "Rust";
    }
    if (strcmp(ext, ".java") == 0) {
        return "Java";
    }
    if (strcmp(ext, ".cpp") == 0 || strcmp(ext, ".cc") == 0 || strcmp(ext, ".cxx") == 0) {
        return "C++";
    }
    if (strcmp(ext, ".c") == 0 || strcmp(ext, ".h") == 0) {
        return "C";
    }
    if (strcmp(ext, ".cs") == 0) {
        return "C#";
    }
    if (strcmp(ext, ".php") == 0) {
        return "PHP";
    }
    if (strcmp(ext, ".lua") == 0) {
        return "Lua";
    }
    if (strcmp(ext, ".scala") == 0) {
        return "Scala";
    }
    if (strcmp(ext, ".kt") == 0) {
        return "Kotlin";
    }
    if (strcmp(ext, ".rb") == 0) {
        return "Ruby";
    }
    if (strcmp(ext, ".sh") == 0 || strcmp(ext, ".bash") == 0) {
        return "Bash";
    }
    if (strcmp(ext, ".zig") == 0) {
        return "Zig";
    }
    if (strcmp(ext, ".ex") == 0 || strcmp(ext, ".exs") == 0) {
        return "Elixir";
    }
    if (strcmp(ext, ".hs") == 0) {
        return "Haskell";
    }
    if (strcmp(ext, ".ml") == 0 || strcmp(ext, ".mli") == 0) {
        return "OCaml";
    }
    if (strcmp(ext, ".html") == 0) {
        return "HTML";
    }
    if (strcmp(ext, ".css") == 0) {
        return "CSS";
    }
    if (strcmp(ext, ".yaml") == 0 || strcmp(ext, ".yml") == 0) {
        return "YAML";
    }
    if (strcmp(ext, ".toml") == 0) {
        return "TOML";
    }
    if (strcmp(ext, ".hcl") == 0 || strcmp(ext, ".tf") == 0) {
        return "HCL";
    }
    if (strcmp(ext, ".sql") == 0) {
        return "SQL";
    }
    if (strcmp(ext, ".erl") == 0) {
        return "Erlang";
    }
    if (strcmp(ext, ".swift") == 0) {
        return "Swift";
    }
    if (strcmp(ext, ".dart") == 0) {
        return "Dart";
    }
    if (strcmp(ext, ".groovy") == 0) {
        return "Groovy";
    }
    if (strcmp(ext, ".pl") == 0) {
        return "Perl";
    }
    if (strcmp(ext, ".r") == 0) {
        return "R";
    }
    if (strcmp(ext, ".scss") == 0) {
        return "SCSS";
    }
    if (strcmp(ext, ".vue") == 0) {
        return "Vue";
    }
    if (strcmp(ext, ".svelte") == 0) {
        return "Svelte";
    }
    return NULL;
}

/* Get lowercase file extension from path */
static const char *file_ext(const char *path) {
    if (!path) {
        return NULL;
    }
    const char *dot = strrchr(path, '.');
    if (!dot) {
        return NULL;
    }
    static CBM_TLS char buf[16];
    int len = (int)strlen(dot);
    if (len >= (int)sizeof(buf)) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        buf[i] = (char)((dot[i] >= 'A' && dot[i] <= 'Z') ? dot[i] + 32 : dot[i]);
    }
    buf[len] = '\0';
    return buf;
}

/* ── Architecture aspect implementations ───────────────────────── */

static int arch_languages(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT file_path FROM nodes WHERE project=?1 AND label='File'";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_languages");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);

    /* Count per language using a simple parallel array */
    const char *lang_names[64];
    int lang_counts[64];
    int nlang = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *fp = (const char *)sqlite3_column_text(stmt, 0);
        const char *ext = file_ext(fp);
        const char *lang = ext_to_lang(ext);
        if (!lang) {
            continue;
        }
        int found = -1;
        for (int i = 0; i < nlang; i++) {
            if (strcmp(lang_names[i], lang) == 0) {
                found = i;
                break;
            }
        }
        if (found >= 0) {
            lang_counts[found]++;
        } else if (nlang < 64) {
            lang_names[nlang] = lang;
            lang_counts[nlang] = 1;
            nlang++;
        }
    }
    sqlite3_finalize(stmt);

    /* Sort by count descending (simple insertion sort) */
    for (int i = 1; i < nlang; i++) {
        int j = i;
        while (j > 0 && lang_counts[j] > lang_counts[j - 1]) {
            int tc = lang_counts[j];
            lang_counts[j] = lang_counts[j - 1];
            lang_counts[j - 1] = tc;
            const char *tn = lang_names[j];
            lang_names[j] = lang_names[j - 1];
            lang_names[j - 1] = tn;
            j--;
        }
    }
    if (nlang > 10) {
        nlang = 10;
    }

    // NOLINTNEXTLINE(clang-analyzer-optin.portability.UnixAPI)
    out->languages = calloc(nlang, sizeof(cbm_language_count_t));
    out->language_count = nlang;
    for (int i = 0; i < nlang; i++) {
        out->languages[i].language = heap_strdup(lang_names[i]);
        out->languages[i].file_count = lang_counts[i];
    }
    return CBM_STORE_OK;
}

static int arch_entry_points(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT name, qualified_name, file_path FROM nodes "
                      "WHERE project=?1 AND json_extract(properties, '$.is_entry_point') = 1 "
                      "AND (json_extract(properties, '$.is_test') IS NULL OR "
                      "json_extract(properties, '$.is_test') != 1) "
                      "AND file_path NOT LIKE '%test%' LIMIT 20";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_entry_points");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);

    int cap = 8;
    int n = 0;
    cbm_entry_point_t *arr = calloc(cap, sizeof(cbm_entry_point_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_entry_point_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].qualified_name = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
        arr[n].file = heap_strdup((const char *)sqlite3_column_text(stmt, 2));
        n++;
    }
    sqlite3_finalize(stmt);
    out->entry_points = arr;
    out->entry_point_count = n;
    return CBM_STORE_OK;
}

static int arch_routes(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT name, properties, COALESCE(file_path, '') FROM nodes "
                      "WHERE project=?1 AND label='Route' "
                      "AND (json_extract(properties, '$.is_test') IS NULL OR "
                      "json_extract(properties, '$.is_test') != 1) "
                      "LIMIT 20";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_routes");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);

    int cap = 8;
    int n = 0;
    cbm_route_info_t *arr = calloc(cap, sizeof(cbm_route_info_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        const char *props = (const char *)sqlite3_column_text(stmt, 1);
        const char *fp = (const char *)sqlite3_column_text(stmt, 2);
        if (cbm_is_test_file_path(fp)) {
            continue;
        }
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_route_info_t));
        }

        /* Parse JSON properties for method, path, handler */
        arr[n].method = heap_strdup("");
        arr[n].path = heap_strdup(name);
        arr[n].handler = heap_strdup("");

        if (props) {
            /* Simple JSON extraction — find "method":"...", "path":"...", "handler":"..." */
            const char *m;
            char vbuf[256];
            m = strstr(props, "\"method\"");
            if (m) {
                m = strchr(m + 8, '"');
                if (m) {
                    m++;
                    const char *end = strchr(m, '"');
                    if (end && end - m < (int)sizeof(vbuf)) {
                        memcpy(vbuf, m, end - m);
                        vbuf[end - m] = '\0';
                        free((void *)arr[n].method);
                        arr[n].method = heap_strdup(vbuf);
                    }
                }
            }
            m = strstr(props, "\"path\"");
            if (m) {
                m = strchr(m + 6, '"');
                if (m) {
                    m++;
                    const char *end = strchr(m, '"');
                    if (end && end - m < (int)sizeof(vbuf)) {
                        memcpy(vbuf, m, end - m);
                        vbuf[end - m] = '\0';
                        free((void *)arr[n].path);
                        arr[n].path = heap_strdup(vbuf);
                    }
                }
            }
            m = strstr(props, "\"handler\"");
            if (m) {
                m = strchr(m + 9, '"');
                if (m) {
                    m++;
                    const char *end = strchr(m, '"');
                    if (end && end - m < (int)sizeof(vbuf)) {
                        memcpy(vbuf, m, end - m);
                        vbuf[end - m] = '\0';
                        free((void *)arr[n].handler);
                        arr[n].handler = heap_strdup(vbuf);
                    }
                }
            }
        }
        n++;
    }
    sqlite3_finalize(stmt);
    out->routes = arr;
    out->route_count = n;
    return CBM_STORE_OK;
}

static int arch_hotspots(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT n.name, n.qualified_name, COUNT(*) as fan_in "
                      "FROM nodes n JOIN edges e ON e.target_id = n.id AND e.type = 'CALLS' "
                      "WHERE n.project=?1 AND n.label IN ('Function', 'Method') "
                      "AND (json_extract(n.properties, '$.is_test') IS NULL OR "
                      "json_extract(n.properties, '$.is_test') != 1) "
                      "AND n.file_path NOT LIKE '%test%' "
                      "GROUP BY n.id ORDER BY fan_in DESC LIMIT 10";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_hotspots");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);

    int cap = 8;
    int n = 0;
    cbm_hotspot_t *arr = calloc(cap, sizeof(cbm_hotspot_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_hotspot_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].qualified_name = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
        arr[n].fan_in = sqlite3_column_int(stmt, 2);
        n++;
    }
    sqlite3_finalize(stmt);
    out->hotspots = arr;
    out->hotspot_count = n;
    return CBM_STORE_OK;
}

static int arch_boundaries(cbm_store_t *s, const char *project, cbm_cross_pkg_boundary_t **out_arr,
                           int *out_count) {
    /* Build nodeID → package map */
    const char *nsql = "SELECT id, qualified_name FROM nodes WHERE project=?1 AND label IN "
                       "('Function','Method','Class')";
    sqlite3_stmt *nstmt = NULL;
    if (sqlite3_prepare_v2(s->db, nsql, -1, &nstmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_boundaries_nodes");
        return CBM_STORE_ERR;
    }
    bind_text(nstmt, 1, project);

    /* Simple parallel arrays for node → package mapping */
    int ncap = 256;
    int nn = 0;
    int64_t *nids = malloc(ncap * sizeof(int64_t));
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **npkgs = malloc(ncap * sizeof(char *));

    while (sqlite3_step(nstmt) == SQLITE_ROW) {
        if (nn >= ncap) {
            ncap *= 2;
            nids = safe_realloc(nids, ncap * sizeof(int64_t));
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            npkgs = safe_realloc(npkgs, ncap * sizeof(char *));
        }
        nids[nn] = sqlite3_column_int64(nstmt, 0);
        const char *qn = (const char *)sqlite3_column_text(nstmt, 1);
        npkgs[nn] = heap_strdup(cbm_qn_to_package(qn));
        nn++;
    }
    sqlite3_finalize(nstmt);

    /* Scan edges, count cross-package calls */
    const char *esql = "SELECT source_id, target_id FROM edges WHERE project=?1 AND type='CALLS'";
    sqlite3_stmt *estmt = NULL;
    if (sqlite3_prepare_v2(s->db, esql, -1, &estmt, NULL) != SQLITE_OK) {
        for (int i = 0; i < nn; i++) {
            free(npkgs[i]);
        }
        free(nids);
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(npkgs);
        store_set_error_sqlite(s, "arch_boundaries_edges");
        return CBM_STORE_ERR;
    }
    bind_text(estmt, 1, project);

    /* Boundary counts: parallel arrays for from→to→count */
    int bcap = 32;
    int bn = 0;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **bfroms = malloc(bcap * sizeof(char *));
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **btos = malloc(bcap * sizeof(char *));
    int *bcounts = malloc(bcap * sizeof(int));

    while (sqlite3_step(estmt) == SQLITE_ROW) {
        int64_t src_id = sqlite3_column_int64(estmt, 0);
        int64_t tgt_id = sqlite3_column_int64(estmt, 1);
        const char *src_pkg = NULL;
        const char *tgt_pkg = NULL;
        for (int i = 0; i < nn; i++) {
            if (nids[i] == src_id) {
                src_pkg = npkgs[i];
            }
            if (nids[i] == tgt_id) {
                tgt_pkg = npkgs[i];
            }
        }
        if (!src_pkg || !tgt_pkg || !src_pkg[0] || !tgt_pkg[0] || strcmp(src_pkg, tgt_pkg) == 0) {
            continue;
        }

        int found = -1;
        for (int i = 0; i < bn; i++) {
            if (strcmp(bfroms[i], src_pkg) == 0 && strcmp(btos[i], tgt_pkg) == 0) {
                found = i;
                break;
            }
        }
        if (found >= 0) {
            bcounts[found]++;
        } else if (bn < bcap) {
            bfroms[bn] = heap_strdup(src_pkg);
            btos[bn] = heap_strdup(tgt_pkg);
            bcounts[bn] = 1;
            bn++;
        }
    }
    sqlite3_finalize(estmt);
    for (int i = 0; i < nn; i++) {
        free(npkgs[i]);
    }
    free(nids);
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(npkgs);

    /* Sort by count descending */
    for (int i = 1; i < bn; i++) {
        int j = i;
        while (j > 0 && bcounts[j] > bcounts[j - 1]) {
            int tc = bcounts[j];
            bcounts[j] = bcounts[j - 1];
            bcounts[j - 1] = tc;
            char *tf = bfroms[j];
            bfroms[j] = bfroms[j - 1];
            bfroms[j - 1] = tf;
            char *tt = btos[j];
            btos[j] = btos[j - 1];
            btos[j - 1] = tt;
            j--;
        }
    }
    if (bn > 10) {
        for (int i = 10; i < bn; i++) {
            free(bfroms[i]);
            free(btos[i]);
        }
        bn = 10;
    }

    // NOLINTNEXTLINE(clang-analyzer-optin.portability.UnixAPI)
    cbm_cross_pkg_boundary_t *result = calloc(bn, sizeof(cbm_cross_pkg_boundary_t));
    for (int i = 0; i < bn; i++) {
        result[i].from = bfroms[i];
        result[i].to = btos[i];
        result[i].call_count = bcounts[i];
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(bfroms);
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(btos);
    free(bcounts);
    *out_arr = result;
    *out_count = bn;
    return CBM_STORE_OK;
}

static int arch_packages(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    /* Try Package nodes first */
    const char *sql =
        "SELECT n.name, COUNT(*) as cnt FROM nodes n "
        "WHERE n.project=?1 AND n.label='Package' GROUP BY n.name ORDER BY cnt DESC LIMIT 15";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_packages");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);

    int cap = 16;
    int n = 0;
    cbm_package_summary_t *arr = calloc(cap, sizeof(cbm_package_summary_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            arr = safe_realloc(arr, cap * sizeof(cbm_package_summary_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].node_count = sqlite3_column_int(stmt, 1);
        n++;
    }
    sqlite3_finalize(stmt);

    /* Fallback: group by QN segment if no Package nodes */
    if (n == 0) {
        free(arr);
        const char *qsql = "SELECT qualified_name FROM nodes WHERE project=?1 AND label IN "
                           "('Function','Method','Class')";
        if (sqlite3_prepare_v2(s->db, qsql, -1, &stmt, NULL) != SQLITE_OK) {
            store_set_error_sqlite(s, "arch_packages_qn");
            return CBM_STORE_ERR;
        }
        bind_text(stmt, 1, project);

        /* Count per package using parallel arrays */
        char *pnames[64];
        int pcounts[64];
        int np = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *qn = (const char *)sqlite3_column_text(stmt, 0);
            const char *pkg = cbm_qn_to_package(qn);
            if (!pkg[0]) {
                continue;
            }
            int found = -1;
            for (int i = 0; i < np; i++) {
                if (strcmp(pnames[i], pkg) == 0) {
                    found = i;
                    break;
                }
            }
            if (found >= 0) {
                {
                    pcounts[found]++;
                }
            } else if (np < 64) {
                pnames[np] = heap_strdup(pkg);
                pcounts[np] = 1;
                np++;
            }
        }
        sqlite3_finalize(stmt);

        /* Sort by count desc */
        for (int i = 1; i < np; i++) {
            int j = i;
            while (j > 0 && pcounts[j] > pcounts[j - 1]) {
                int tc = pcounts[j];
                pcounts[j] = pcounts[j - 1];
                pcounts[j - 1] = tc;
                char *tn = pnames[j];
                pnames[j] = pnames[j - 1];
                pnames[j - 1] = tn;
                j--;
            }
        }
#define MAX_PREVIEW_NAMES 15
        if (np > MAX_PREVIEW_NAMES) {
            for (int i = MAX_PREVIEW_NAMES; i < np; i++) {
                free(pnames[i]);
            }
            np = MAX_PREVIEW_NAMES;
        }

        // NOLINTNEXTLINE(clang-analyzer-optin.portability.UnixAPI)
        arr = calloc(np, sizeof(cbm_package_summary_t));
        n = np;
        for (int i = 0; i < np; i++) {
            arr[i].name = pnames[i];
            arr[i].node_count = pcounts[i];
        }
    }

    out->packages = arr;
    out->package_count = n;
    return CBM_STORE_OK;
}

static void classify_layer(const char *pkg, int in, int out_deg, bool has_routes,
                           // NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
                           bool has_entry_points, const char **layer, const char **reason) {
    static CBM_TLS char reason_buf[128];
    if (has_entry_points && out_deg > 0 && in == 0) {
        *layer = "entry";
        *reason = "has entry points, only outbound calls";
        return;
    }
    if (has_routes) {
        *layer = "api";
        *reason = "has HTTP route definitions";
        return;
    }
    if (in > out_deg && in > 3) {
        snprintf(reason_buf, sizeof(reason_buf), "high fan-in (%d in, %d out)", in, out_deg);
        *layer = "core";
        *reason = reason_buf;
        return;
    }
    if (out_deg == 0 && in > 0) {
        *layer = "leaf";
        *reason = "only inbound calls, no outbound";
        return;
    }
    if (in == 0 && out_deg > 0) {
        *layer = "entry";
        *reason = "only outbound calls";
        return;
    }
    snprintf(reason_buf, sizeof(reason_buf), "fan-in=%d, fan-out=%d", in, out_deg);
    *layer = "internal";
    *reason = reason_buf;
    (void)pkg;
}

static int arch_layers(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    /* Get boundaries for fan analysis */
    cbm_cross_pkg_boundary_t *boundaries = NULL;
    int bcount = 0;
    int rc = arch_boundaries(s, project, &boundaries, &bcount);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    /* Check which packages have Route nodes */
    char *route_pkgs[32];
    int nrpkgs = 0;
    {
        const char *sql = "SELECT qualified_name FROM nodes WHERE project=?1 AND label='Route'";
        sqlite3_stmt *stmt = NULL;
        sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
        bind_text(stmt, 1, project);
        while (sqlite3_step(stmt) == SQLITE_ROW && nrpkgs < 32) {
            const char *qn = (const char *)sqlite3_column_text(stmt, 0);
            route_pkgs[nrpkgs++] = heap_strdup(cbm_qn_to_package(qn));
        }
        sqlite3_finalize(stmt);
    }

    /* Check which packages have entry points */
    char *entry_pkgs[32];
    int nepkgs = 0;
    {
        const char *sql = "SELECT qualified_name FROM nodes WHERE project=?1 AND "
                          "json_extract(properties, '$.is_entry_point') = 1";
        sqlite3_stmt *stmt = NULL;
        sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL);
        bind_text(stmt, 1, project);
        while (sqlite3_step(stmt) == SQLITE_ROW && nepkgs < 32) {
            const char *qn = (const char *)sqlite3_column_text(stmt, 0);
            entry_pkgs[nepkgs++] = heap_strdup(cbm_qn_to_package(qn));
        }
        sqlite3_finalize(stmt);
    }

    /* Compute fan-in/out per package */
    char *all_pkgs[64];
    int fan_in[64];
    int fan_out[64];
    int npkgs = 0;
    memset(fan_in, 0, sizeof(fan_in));
    memset(fan_out, 0, sizeof(fan_out));

    for (int i = 0; i < bcount; i++) {
        /* Add or find "from" package */
        int fi = -1;
        for (int j = 0; j < npkgs; j++) {
            if (strcmp(all_pkgs[j], boundaries[i].from) == 0) {
                fi = j;
                break;
            }
        }
        if (fi < 0 && npkgs < 64) {
            fi = npkgs;
            all_pkgs[npkgs] = heap_strdup(boundaries[i].from);
            npkgs++;
        }
        if (fi >= 0) {
            fan_out[fi] += boundaries[i].call_count;
        }

        int ti = -1;
        for (int j = 0; j < npkgs; j++) {
            if (strcmp(all_pkgs[j], boundaries[i].to) == 0) {
                ti = j;
                break;
            }
        }
        if (ti < 0 && npkgs < 64) {
            ti = npkgs;
            all_pkgs[npkgs] = heap_strdup(boundaries[i].to);
            npkgs++;
        }
        if (ti >= 0) {
            fan_in[ti] += boundaries[i].call_count;
        }
    }

    /* Also include route/entry packages */
    for (int i = 0; i < nrpkgs; i++) {
        int found = -1;
        for (int j = 0; j < npkgs; j++) {
            if (strcmp(all_pkgs[j], route_pkgs[i]) == 0) {
                found = j;
                break;
            }
        }
        if (found < 0 && npkgs < 64) {
            all_pkgs[npkgs] = heap_strdup(route_pkgs[i]);
            npkgs++;
        }
    }
    for (int i = 0; i < nepkgs; i++) {
        int found = -1;
        for (int j = 0; j < npkgs; j++) {
            if (strcmp(all_pkgs[j], entry_pkgs[i]) == 0) {
                found = j;
                break;
            }
        }
        if (found < 0 && npkgs < 64) {
            all_pkgs[npkgs] = heap_strdup(entry_pkgs[i]);
            npkgs++;
        }
    }

    /* Classify each package */
    // NOLINTNEXTLINE(clang-analyzer-optin.portability.UnixAPI)
    out->layers = calloc(npkgs, sizeof(cbm_package_layer_t));
    out->layer_count = npkgs;
    for (int i = 0; i < npkgs; i++) {
        bool has_route = false, has_entry = false;
        for (int j = 0; j < nrpkgs; j++) {
            if (strcmp(all_pkgs[i], route_pkgs[j]) == 0) {
                has_route = true;
                break;
            }
        }
        for (int j = 0; j < nepkgs; j++) {
            if (strcmp(all_pkgs[i], entry_pkgs[j]) == 0) {
                has_entry = true;
                break;
            }
        }
        const char *layer;
        const char *reason;
        classify_layer(all_pkgs[i], fan_in[i], fan_out[i], has_route, has_entry, &layer, &reason);
        out->layers[i].name = all_pkgs[i]; /* transfer ownership */
        out->layers[i].layer = heap_strdup(layer);
        out->layers[i].reason = heap_strdup(reason);
    }

    /* Sort layers by name */
    for (int i = 1; i < npkgs; i++) {
        int j = i;
        while (j > 0 && strcmp(out->layers[j].name, out->layers[j - 1].name) < 0) {
            cbm_package_layer_t tmp = out->layers[j];
            out->layers[j] = out->layers[j - 1];
            out->layers[j - 1] = tmp;
            j--;
        }
    }

    /* Cleanup */
    for (int i = 0; i < bcount; i++) {
        free((void *)boundaries[i].from);
        free((void *)boundaries[i].to);
    }
    free(boundaries);
    for (int i = 0; i < nrpkgs; i++) {
        free(route_pkgs[i]);
    }
    for (int i = 0; i < nepkgs; i++) {
        free(entry_pkgs[i]);
    }

    return CBM_STORE_OK;
}

static int arch_file_tree(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT file_path FROM nodes WHERE project=?1 AND label='File'";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_file_tree");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);

    /* Collect all file paths + build directory children map */
    int fcap = 32;
    int fn = 0;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **files = malloc(fcap * sizeof(char *));

    /* Directory tree: parallel arrays of dir → children set */
    int dcap = 64;
    int dn = 0;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **dir_paths = calloc(dcap, sizeof(char *));
    int *dir_child_counts = calloc(dcap, sizeof(int));
    /* Track unique children per dir using a simple string array */
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char ***dir_children = calloc(dcap, sizeof(char **));
    int *dir_children_caps = calloc(dcap, sizeof(int));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *fp = (const char *)sqlite3_column_text(stmt, 0);
        if (!fp) {
            continue;
        }
        if (fn >= fcap) {
            fcap *= 2;
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            files = safe_realloc(files, fcap * sizeof(char *));
        }
        files[fn++] = heap_strdup(fp);

        /* Register path components in dir tree (up to 3 levels deep) */
        char tmp[512];
        strncpy(tmp, fp, sizeof(tmp) - 1);
        tmp[sizeof(tmp) - 1] = '\0';

        /* Split by '/' */
        char *parts[16];
        int nparts = 0;
        char *p = tmp;
        parts[nparts++] = p;
        while (*p && nparts < 16) {
            if (*p == '/') {
                *p = '\0';
                parts[nparts++] = p + 1;
            }
            p++;
        }

        /* Register root children */
        {
            int ri = -1;
            for (int i = 0; i < dn; i++) {
                if (strcmp(dir_paths[i], "") == 0) {
                    ri = i;
                    break;
                }
            }
            if (ri < 0 && dn < dcap) {
                ri = dn;
                dir_paths[dn] = heap_strdup("");
                dir_child_counts[dn] = 0;
                dir_children[dn] = NULL;
                dir_children_caps[dn] = 0;
                dn++;
            }
            if (ri >= 0 && nparts > 0) {
                /* Check if child already exists */
                bool exists = false;
                for (int k = 0; k < dir_child_counts[ri]; k++) {
                    if (strcmp(dir_children[ri][k], parts[0]) == 0) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    if (dir_child_counts[ri] >= dir_children_caps[ri]) {
                        dir_children_caps[ri] =
                            dir_children_caps[ri] ? dir_children_caps[ri] * 2 : 4;
                        dir_children[ri] =
                            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                            realloc(dir_children[ri], dir_children_caps[ri] * sizeof(char *));
                    }
                    dir_children[ri][dir_child_counts[ri]++] = heap_strdup(parts[0]);
                }
            }
        }

        /* Register deeper dir children (depth 0..2) */
        for (int depth = 0; depth < nparts - 1 && depth < 3; depth++) {
            /* Build dir path */
            char dir[512] = "";
            for (int k = 0; k <= depth; k++) {
                if (k > 0) {
                    strcat(dir, "/");
                }
                strcat(dir, parts[k]);
            }
            const char *child = (depth + 1 < nparts) ? parts[depth + 1] : NULL;
            if (!child) {
                continue;
            }

            int di = -1;
            for (int i = 0; i < dn; i++) {
                if (strcmp(dir_paths[i], dir) == 0) {
                    di = i;
                    break;
                }
            }
            if (di < 0 && dn < dcap) {
                di = dn;
                dir_paths[dn] = heap_strdup(dir);
                dir_child_counts[dn] = 0;
                dir_children[dn] = NULL;
                dir_children_caps[dn] = 0;
                dn++;
            }
            if (di >= 0) {
                bool exists = false;
                for (int k = 0; k < dir_child_counts[di]; k++) {
                    if (strcmp(dir_children[di][k], child) == 0) {
                        exists = true;
                        break;
                    }
                }
                if (!exists) {
                    if (dir_child_counts[di] >= dir_children_caps[di]) {
                        dir_children_caps[di] =
                            dir_children_caps[di] ? dir_children_caps[di] * 2 : 4;
                        dir_children[di] =
                            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
                            realloc(dir_children[di], dir_children_caps[di] * sizeof(char *));
                    }
                    dir_children[di][dir_child_counts[di]++] = heap_strdup(child);
                }
            }
        }
    }
    sqlite3_finalize(stmt);

    /* Build file set for type detection */
    /* Collect tree entries */
    int ecap = 64;
    int en = 0;
    cbm_file_tree_entry_t *entries = calloc(ecap, sizeof(cbm_file_tree_entry_t));

    /* Root children */
    for (int i = 0; i < dn; i++) {
        if (strcmp(dir_paths[i], "") != 0) {
            continue;
        }
        for (int k = 0; k < dir_child_counts[i]; k++) {
            if (en >= ecap) {
                ecap *= 2;
                entries = safe_realloc(entries, ecap * sizeof(cbm_file_tree_entry_t));
            }
            const char *child = dir_children[i][k];
            /* Check if it's a file */
            bool is_file = false;
            for (int f = 0; f < fn; f++) {
                if (strcmp(files[f], child) == 0) {
                    is_file = true;
                    break;
                }
            }
            /* Count its children in dir tree */
            int nch = 0;
            for (int d = 0; d < dn; d++) {
                if (strcmp(dir_paths[d], child) == 0) {
                    nch = dir_child_counts[d];
                    break;
                }
            }
            entries[en].path = heap_strdup(child);
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            entries[en].type = heap_strdup(is_file ? "file" : "dir");
            entries[en].children = nch;
            en++;
        }
    }

    /* Non-root dir children */
    for (int i = 0; i < dn; i++) {
        if (strcmp(dir_paths[i], "") == 0) {
            continue;
        }
        /* Limit depth to 3 levels */
        int slashes = 0;
        // NOLINTNEXTLINE(clang-analyzer-security.ArrayBound)
        for (const char *p = dir_paths[i]; *p; p++) {
            if (*p == '/') {
                slashes++;
            }
        }
        if (slashes >= 3) {
            continue;
        }

        for (int k = 0; k < dir_child_counts[i]; k++) {
            if (en >= ecap) {
                ecap *= 2;
                entries = safe_realloc(entries, ecap * sizeof(cbm_file_tree_entry_t));
            }
            char path[512];
            snprintf(path, sizeof(path), "%s/%s", dir_paths[i], dir_children[i][k]);
            bool is_file = false;
            for (int f = 0; f < fn; f++) {
                if (strcmp(files[f], path) == 0) {
                    is_file = true;
                    break;
                }
            }
            int nch = 0;
            for (int d = 0; d < dn; d++) {
                if (strcmp(dir_paths[d], path) == 0) {
                    nch = dir_child_counts[d];
                    break;
                }
            }
            entries[en].path = heap_strdup(path);
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
            entries[en].type = heap_strdup(is_file ? "file" : "dir");
            entries[en].children = nch;
            en++;
        }
    }

    /* Sort by path */
    for (int i = 1; i < en; i++) {
        int j = i;
        while (j > 0 && strcmp(entries[j].path, entries[j - 1].path) < 0) {
            cbm_file_tree_entry_t tmp = entries[j];
            entries[j] = entries[j - 1];
            entries[j - 1] = tmp;
            j--;
        }
    }

    /* Cleanup dir tree */
    for (int i = 0; i < dn; i++) {
        free(dir_paths[i]);
        for (int k = 0; k < dir_child_counts[i]; k++) {
            free(dir_children[i][k]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(dir_children[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(dir_paths);
    free(dir_child_counts);
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(dir_children);
    free(dir_children_caps);
    for (int i = 0; i < fn; i++) {
        free(files[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(files);

    out->file_tree = entries;
    out->file_tree_count = en;
    return CBM_STORE_OK;
}

/* ── Louvain community detection ───────────────────────────────── */

int cbm_louvain(const int64_t *nodes, int node_count, const cbm_louvain_edge_t *edges,
                int edge_count, cbm_louvain_result_t **out, int *out_count) {
    if (node_count <= 0) {
        *out = NULL;
        *out_count = 0;
        return CBM_STORE_OK;
    }

    int n = node_count;

    /* Build adjacency: edge weights */
    int wcap = edge_count > 0 ? edge_count : 1;
    int wn = 0;
    int *wsi = malloc(wcap * sizeof(int));
    int *wdi = malloc(wcap * sizeof(int));
    double *ww = malloc(wcap * sizeof(double));

    /* Map node IDs to indices */
    for (int e = 0; e < edge_count; e++) {
        int si = -1;
        int di = -1;
        for (int i = 0; i < n; i++) {
            if (nodes[i] == edges[e].src) {
                si = i;
            }
            if (nodes[i] == edges[e].dst) {
                di = i;
            }
        }
        if (si < 0 || di < 0 || si == di) {
            continue;
        }
        /* Normalize edge key */
        if (si > di) {
            int tmp = si;
            si = di;
            di = tmp;
        }
        /* Check if already exists */
        int found = -1;
        for (int i = 0; i < wn; i++) {
            if (wsi[i] == si && wdi[i] == di) {
                found = i;
                break;
            }
        }
        if (found >= 0) {
            ww[found] += 1.0;
        } else {
            if (wn >= wcap) {
                wcap *= 2;
                wsi = safe_realloc(wsi, wcap * sizeof(int));
                wdi = safe_realloc(wdi, wcap * sizeof(int));
                ww = safe_realloc(ww, wcap * sizeof(double));
            }
            wsi[wn] = si;
            wdi[wn] = di;
            ww[wn] = 1.0;
            wn++;
        }
    }

    /* Build adjacency lists */
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    int **adj = calloc(n, sizeof(int *));
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    double **adj_w = calloc(n, sizeof(double *));
    int *adj_n = calloc(n, sizeof(int));
    int *adj_cap = calloc(n, sizeof(int));

    double total_weight = 0;
    for (int i = 0; i < wn; i++) {
        int si = wsi[i];
        int di = wdi[i];
        double w = ww[i];
        total_weight += w;

        /* Add si → di */
        if (adj_n[si] >= adj_cap[si]) {
            adj_cap[si] = adj_cap[si] ? adj_cap[si] * 2 : 4;
            adj[si] = safe_realloc(adj[si], adj_cap[si] * sizeof(int));
            adj_w[si] = safe_realloc(adj_w[si], adj_cap[si] * sizeof(double));
        }
        adj[si][adj_n[si]] = di;
        adj_w[si][adj_n[si]] = w;
        adj_n[si]++;

        /* Add di → si */
        if (adj_n[di] >= adj_cap[di]) {
            adj_cap[di] = adj_cap[di] ? adj_cap[di] * 2 : 4;
            adj[di] = safe_realloc(adj[di], adj_cap[di] * sizeof(int));
            adj_w[di] = safe_realloc(adj_w[di], adj_cap[di] * sizeof(double));
        }
        adj[di][adj_n[di]] = si;
        adj_w[di][adj_n[di]] = w;
        adj_n[di]++;
    }
    free(wsi);
    free(wdi);
    free(ww);

    /* Initialize communities */
    int *community = malloc(n * sizeof(int));
    for (int i = 0; i < n; i++) {
        community[i] = i;
    }

    if (total_weight == 0) {
        /* No edges: each node in its own community */
        cbm_louvain_result_t *result = malloc(n * sizeof(cbm_louvain_result_t));
        for (int i = 0; i < n; i++) {
            result[i].node_id = nodes[i];
            result[i].community = i;
        }
        *out = result;
        *out_count = n;
        free(community);
        for (int i = 0; i < n; i++) {
            free(adj[i]);
            free(adj_w[i]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(adj);
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(adj_w);
        free(adj_n);
        free(adj_cap);
        return CBM_STORE_OK;
    }

    /* Compute node degrees */
    double *degree = calloc(n, sizeof(double));
    for (int i = 0; i < n; i++) {
        for (int j = 0; j < adj_n[i]; j++) {
            degree[i] += adj_w[i][j];
        }
    }

    /* Main Louvain loop (10 iterations max) */
    for (int iter = 0; iter < 10; iter++) {
        bool improved = false;

        /* Community total degree */
        double *comm_degree = calloc(n, sizeof(double));
        for (int i = 0; i < n; i++) {
            comm_degree[community[i]] += degree[i];
        }

        /* Random order (simple LCG shuffle) */
        int *order = calloc(n, sizeof(int));
        for (int i = 0; i < n; i++) {
            order[i] = i;
        }
        unsigned int seed = (unsigned int)((iter * 1000) + n);
        for (int i = n - 1; i > 0; i--) {
/* Linear congruential generator (glibc constants) */
#define LCG_MULTIPLIER 1103515245U
#define LCG_INCREMENT 12345U
            seed = (seed * LCG_MULTIPLIER) + LCG_INCREMENT;
            int j = (int)((seed >> 16) % (unsigned int)(i + 1));
            int tmp = order[i];
            order[i] = order[j];
            order[j] = tmp;
        }

        for (int oi = 0; oi < n; oi++) {
            int i = order[oi];
            int cur_comm = community[i];

            /* Compute weights to neighboring communities */
            double *nc_weight = calloc(n, sizeof(double));
            bool *nc_seen = calloc(n, sizeof(bool));
            for (int j = 0; j < adj_n[i]; j++) {
                int nc = community[adj[i][j]];
                nc_weight[nc] += adj_w[i][j];
                nc_seen[nc] = true;
            }

            /* Remove node from community */
            comm_degree[cur_comm] -= degree[i];

            int best_comm = cur_comm;
            double best_gain = 0.0;

            for (int c = 0; c < n; c++) {
                if (!nc_seen[c]) {
                    continue;
                }
                double gain = nc_weight[c] - (degree[i] * comm_degree[c] / (2.0 * total_weight));
                if (gain > best_gain) {
                    best_gain = gain;
                    best_comm = c;
                }
            }

            /* Also consider staying */
            double cur_gain =
                nc_weight[cur_comm] - (degree[i] * comm_degree[cur_comm] / (2.0 * total_weight));
            if (cur_gain >= best_gain) {
                best_comm = cur_comm;
            }

            community[i] = best_comm;
            comm_degree[best_comm] += degree[i];

            if (best_comm != cur_comm) {
                improved = true;
            }

            free(nc_weight);
            free(nc_seen);
        }
        free(order);
        free(comm_degree);

        if (!improved) {
            break;
        }
    }

    /* Build result */
    cbm_louvain_result_t *result = malloc(n * sizeof(cbm_louvain_result_t));
    for (int i = 0; i < n; i++) {
        result[i].node_id = nodes[i];
        result[i].community = community[i];
    }
    *out = result;
    *out_count = n;

    free(community);
    free(degree);
    for (int i = 0; i < n; i++) {
        free(adj[i]);
        free(adj_w[i]);
    }
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(adj);
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    free(adj_w);
    free(adj_n);
    free(adj_cap);
    return CBM_STORE_OK;
}

/* ── GetArchitecture dispatch ──────────────────────────────────── */

static bool want_aspect(const char **aspects, int aspect_count, const char *name) {
    if (!aspects || aspect_count == 0) {
        return true;
    }
    for (int i = 0; i < aspect_count; i++) {
        if (strcmp(aspects[i], "all") == 0) {
            return true;
        }
        if (strcmp(aspects[i], name) == 0) {
            return true;
        }
    }
    return false;
}

int cbm_store_get_architecture(cbm_store_t *s, const char *project, const char **aspects,
                               int aspect_count, cbm_architecture_info_t *out) {
    memset(out, 0, sizeof(*out));
    int rc;

    if (want_aspect(aspects, aspect_count, "languages")) {
        rc = arch_languages(s, project, out);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (want_aspect(aspects, aspect_count, "packages")) {
        rc = arch_packages(s, project, out);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (want_aspect(aspects, aspect_count, "entry_points")) {
        rc = arch_entry_points(s, project, out);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (want_aspect(aspects, aspect_count, "routes")) {
        rc = arch_routes(s, project, out);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (want_aspect(aspects, aspect_count, "hotspots")) {
        rc = arch_hotspots(s, project, out);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (want_aspect(aspects, aspect_count, "boundaries")) {
        cbm_cross_pkg_boundary_t *barr = NULL;
        int bcount = 0;
        rc = arch_boundaries(s, project, &barr, &bcount);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        out->boundaries = barr;
        out->boundary_count = bcount;
    }
    if (want_aspect(aspects, aspect_count, "layers")) {
        rc = arch_layers(s, project, out);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (want_aspect(aspects, aspect_count, "file_tree")) {
        rc = arch_file_tree(s, project, out);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }

    return CBM_STORE_OK;
}

void cbm_store_architecture_free(cbm_architecture_info_t *out) {
    if (!out) {
        return;
    }
    for (int i = 0; i < out->language_count; i++) {
        free((void *)out->languages[i].language);
    }
    free(out->languages);
    for (int i = 0; i < out->package_count; i++) {
        free((void *)out->packages[i].name);
    }
    free(out->packages);
    for (int i = 0; i < out->entry_point_count; i++) {
        free((void *)out->entry_points[i].name);
        free((void *)out->entry_points[i].qualified_name);
        free((void *)out->entry_points[i].file);
    }
    free(out->entry_points);
    for (int i = 0; i < out->route_count; i++) {
        free((void *)out->routes[i].method);
        free((void *)out->routes[i].path);
        free((void *)out->routes[i].handler);
    }
    free(out->routes);
    for (int i = 0; i < out->hotspot_count; i++) {
        free((void *)out->hotspots[i].name);
        free((void *)out->hotspots[i].qualified_name);
    }
    free(out->hotspots);
    for (int i = 0; i < out->boundary_count; i++) {
        free((void *)out->boundaries[i].from);
        free((void *)out->boundaries[i].to);
    }
    free(out->boundaries);
    for (int i = 0; i < out->service_count; i++) {
        free((void *)out->services[i].from);
        free((void *)out->services[i].to);
        free((void *)out->services[i].type);
    }
    free(out->services);
    for (int i = 0; i < out->layer_count; i++) {
        free((void *)out->layers[i].name);
        free((void *)out->layers[i].layer);
        free((void *)out->layers[i].reason);
    }
    free(out->layers);
    for (int i = 0; i < out->cluster_count; i++) {
        free((void *)out->clusters[i].label);
        for (int j = 0; j < out->clusters[i].top_node_count; j++) {
            free((void *)out->clusters[i].top_nodes[j]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(out->clusters[i].top_nodes);
        for (int j = 0; j < out->clusters[i].package_count; j++) {
            free((void *)out->clusters[i].packages[j]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(out->clusters[i].packages);
        for (int j = 0; j < out->clusters[i].edge_type_count; j++) {
            free((void *)out->clusters[i].edge_types[j]);
        }
        // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
        free(out->clusters[i].edge_types);
    }
    free(out->clusters);
    for (int i = 0; i < out->file_tree_count; i++) {
        free((void *)out->file_tree[i].path);
        free((void *)out->file_tree[i].type);
    }
    free(out->file_tree);
    memset(out, 0, sizeof(*out));
}

/* ── ADR (Architecture Decision Record) ────────────────────────── */

static const char *canonical_sections[] = {"PURPOSE",  "STACK",     "ARCHITECTURE",
                                           "PATTERNS", "TRADEOFFS", "PHILOSOPHY"};
static const int canonical_section_count = 6;

static bool is_canonical_section(const char *name) {
    for (int i = 0; i < canonical_section_count; i++) {
        if (strcmp(name, canonical_sections[i]) == 0) {
            return true;
        }
    }
    return false;
}

cbm_adr_sections_t cbm_adr_parse_sections(const char *content) {
    cbm_adr_sections_t result;
    memset(&result, 0, sizeof(result));
    if (!content || !content[0]) {
        return result;
    }

    const char *p = content;
    char *current_section = NULL;
    char current_content[8192] = "";
    int content_len = 0;

    while (*p) {
        /* Find end of line */
        const char *eol = strchr(p, '\n');
        int line_len = eol ? (int)(eol - p) : (int)strlen(p);

        /* Check for canonical section header */
        if (line_len > 3 && p[0] == '#' && p[1] == '#' && p[2] == ' ') {
            char header[64];
            int hlen = line_len - 3;
            if (hlen >= (int)sizeof(header)) {
                hlen = (int)sizeof(header) - 1;
            }
            memcpy(header, p + 3, hlen);
            header[hlen] = '\0';
            /* Trim trailing whitespace */
            while (hlen > 0 && (header[hlen - 1] == ' ' || header[hlen - 1] == '\t' ||
                                header[hlen - 1] == '\r')) {
                header[--hlen] = '\0';
            }

            if (is_canonical_section(header)) {
                /* Save previous section */
                if (current_section && result.count < 16) {
                    /* Trim content */
                    while (content_len > 0 && (current_content[content_len - 1] == '\n' ||
                                               current_content[content_len - 1] == ' ')) {
                        current_content[--content_len] = '\0';
                    }
                    /* Skip leading whitespace */
                    char *trimmed = current_content;
                    while (*trimmed == '\n' || *trimmed == ' ') {
                        trimmed++;
                    }
                    result.keys[result.count] = current_section;
                    result.values[result.count] = heap_strdup(trimmed);
                    result.count++;
                }
                current_section = heap_strdup(header);
                current_content[0] = '\0';
                content_len = 0;
                p = eol ? eol + 1 : p + line_len;
                continue;
            }
        }

        /* Append line to current content */
        if (current_section) {
            if (content_len > 0 || line_len > 0) {
                if (content_len > 0) {
                    current_content[content_len++] = '\n';
                }
                if (content_len + line_len < (int)sizeof(current_content) - 1) {
                    memcpy(current_content + content_len, p, line_len);
                    content_len += line_len;
                    current_content[content_len] = '\0';
                }
            }
        }

        p = eol ? eol + 1 : p + line_len;
    }

    /* Save last section */
    if (current_section && result.count < 16) {
        while (content_len > 0 && (current_content[content_len - 1] == '\n' ||
                                   current_content[content_len - 1] == ' ')) {
            current_content[--content_len] = '\0';
        }
        char *trimmed = current_content;
        while (*trimmed == '\n' || *trimmed == ' ') {
            trimmed++;
        }
        result.keys[result.count] = current_section;
        result.values[result.count] = heap_strdup(trimmed);
        result.count++;
    }

    return result;
}

char *cbm_adr_render(const cbm_adr_sections_t *sections) {
    if (!sections || sections->count == 0) {
        return heap_strdup("");
    }

    char buf[16384] = "";
    int pos = 0;
    bool rendered[16] = {false};

    /* Canonical sections first, in order */
    for (int c = 0; c < canonical_section_count; c++) {
        for (int i = 0; i < sections->count; i++) {
            if (rendered[i]) {
                continue;
            }
            if (strcmp(sections->keys[i], canonical_sections[c]) == 0) {
                if (pos > 0) {
                    pos += snprintf(buf + pos, sizeof(buf) - pos, "\n\n");
                }
                pos += snprintf(buf + pos, sizeof(buf) - pos, "## %s\n%s", sections->keys[i],
                                sections->values[i]);
                rendered[i] = true;
                break;
            }
        }
    }

    /* Non-canonical sections alphabetically */
    /* Collect indices of non-rendered sections */
    int extra[16];
    int nextra = 0;
    for (int i = 0; i < sections->count; i++) {
        if (!rendered[i]) {
            extra[nextra++] = i;
        }
    }
    /* Sort extra by key name */
    for (int i = 1; i < nextra; i++) {
        int j = i;
        while (j > 0 && strcmp(sections->keys[extra[j]], sections->keys[extra[j - 1]]) < 0) {
            int tmp = extra[j];
            extra[j] = extra[j - 1];
            extra[j - 1] = tmp;
            j--;
        }
    }
    for (int i = 0; i < nextra; i++) {
        int idx = extra[i];
        if (pos > 0) {
            pos += snprintf(buf + pos, sizeof(buf) - pos, "\n\n");
        }
        pos += snprintf(buf + pos, sizeof(buf) - pos, "## %s\n%s", sections->keys[idx],
                        sections->values[idx]);
    }

    return heap_strdup(buf);
}

int cbm_adr_validate_content(const char *content, char *errbuf, int errbuf_size) {
    cbm_adr_sections_t sections = cbm_adr_parse_sections(content);
    char missing[256] = "";
    int mlen = 0;
    int nmissing = 0;

    for (int c = 0; c < canonical_section_count; c++) {
        bool found = false;
        for (int i = 0; i < sections.count; i++) {
            if (strcmp(sections.keys[i], canonical_sections[c]) == 0) {
                found = true;
                break;
            }
        }
        if (!found) {
            if (mlen > 0) {
                mlen += snprintf(missing + mlen, sizeof(missing) - mlen, ", ");
            }
            mlen += snprintf(missing + mlen, sizeof(missing) - mlen, "%s", canonical_sections[c]);
            nmissing++;
        }
    }
    cbm_adr_sections_free(&sections);

    if (nmissing > 0) {
        snprintf(errbuf, errbuf_size,
                 "missing required sections: %s. All 6 required: PURPOSE, STACK, ARCHITECTURE, "
                 "PATTERNS, TRADEOFFS, PHILOSOPHY",
                 missing);
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_adr_validate_section_keys(const char **keys, int count, char *errbuf, int errbuf_size) {
    char invalid[256] = "";
    int ilen = 0;
    int ninvalid = 0;

    /* Collect and sort invalid keys */
    const char *inv_keys[16];
    int inv_n = 0;
    for (int i = 0; i < count; i++) {
        if (!is_canonical_section(keys[i])) {
            if (inv_n < 16) {
                inv_keys[inv_n++] = keys[i];
            }
        }
    }
    /* Sort alphabetically */
    for (int i = 1; i < inv_n; i++) {
        int j = i;
        while (j > 0 && strcmp(inv_keys[j], inv_keys[j - 1]) < 0) {
            const char *tmp = inv_keys[j];
            inv_keys[j] = inv_keys[j - 1];
            inv_keys[j - 1] = tmp;
            j--;
        }
    }

    for (int i = 0; i < inv_n; i++) {
        if (ilen > 0) {
            ilen += snprintf(invalid + ilen, sizeof(invalid) - ilen, ", ");
        }
        ilen += snprintf(invalid + ilen, sizeof(invalid) - ilen, "%s", inv_keys[i]);
        ninvalid++;
    }

    if (ninvalid > 0) {
        snprintf(errbuf, errbuf_size,
                 "invalid section names: %s. Valid sections: PURPOSE, STACK, ARCHITECTURE, "
                 "PATTERNS, TRADEOFFS, PHILOSOPHY",
                 invalid);
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

void cbm_adr_sections_free(cbm_adr_sections_t *s) {
    if (!s) {
        return;
    }
    for (int i = 0; i < s->count; i++) {
        free(s->keys[i]);
        free(s->values[i]);
    }
    memset(s, 0, sizeof(*s));
}

int cbm_store_adr_store(cbm_store_t *s, const char *project, const char *content) {
    char now[32];
    iso_now(now, sizeof(now));

    const char *sql =
        "INSERT INTO project_summaries (project, summary, source_hash, created_at, updated_at) "
        "VALUES (?1, ?2, '', ?3, ?4) "
        "ON CONFLICT(project) DO UPDATE SET summary=excluded.summary, "
        "updated_at=excluded.updated_at";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "adr_store");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);
    bind_text(stmt, 2, content);
    bind_text(stmt, 3, now);
    bind_text(stmt, 4, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? CBM_STORE_OK : CBM_STORE_ERR;
}

int cbm_store_adr_get(cbm_store_t *s, const char *project, cbm_adr_t *out) {
    const char *sql =
        "SELECT project, summary, created_at, updated_at FROM project_summaries WHERE project=?1";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "adr_get");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        store_set_error(s, "no ADR found");
        return CBM_STORE_NOT_FOUND;
    }
    out->project = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
    out->content = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
    out->created_at = heap_strdup((const char *)sqlite3_column_text(stmt, 2));
    out->updated_at = heap_strdup((const char *)sqlite3_column_text(stmt, 3));
    sqlite3_finalize(stmt);
    return CBM_STORE_OK;
}

int cbm_store_adr_delete(cbm_store_t *s, const char *project) {
    const char *sql = "DELETE FROM project_summaries WHERE project=?1";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "adr_delete");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);
    int rc = sqlite3_step(stmt);
    int changes = sqlite3_changes(s->db);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        return CBM_STORE_ERR;
    }
    if (changes == 0) {
        store_set_error(s, "no ADR found");
        return CBM_STORE_NOT_FOUND;
    }
    return CBM_STORE_OK;
}

// NOLINTNEXTLINE(bugprone-easily-swappable-parameters)
int cbm_store_adr_update_sections(cbm_store_t *s, const char *project, const char **keys,
                                  const char **values, int count, cbm_adr_t *out) {
    /* Get existing ADR */
    cbm_adr_t existing;
    int rc = cbm_store_adr_get(s, project, &existing);
    if (rc != CBM_STORE_OK) {
        store_set_error(s, "no existing ADR to update");
        return rc;
    }

    /* Parse existing sections */
    cbm_adr_sections_t sections = cbm_adr_parse_sections(existing.content);
    cbm_store_adr_free(&existing);

    /* Merge new sections */
    for (int i = 0; i < count; i++) {
        bool found = false;
        for (int j = 0; j < sections.count; j++) {
            if (strcmp(sections.keys[j], keys[i]) == 0) {
                free(sections.values[j]);
                sections.values[j] = heap_strdup(values[i]);
                found = true;
                break;
            }
        }
        if (!found && sections.count < 16) {
            sections.keys[sections.count] = heap_strdup(keys[i]);
            sections.values[sections.count] = heap_strdup(values[i]);
            sections.count++;
        }
    }

    /* Render merged */
    char *merged = cbm_adr_render(&sections);
    cbm_adr_sections_free(&sections);

    /* Check length */
    if ((int)strlen(merged) > CBM_ADR_MAX_LENGTH) {
        char msg[128];
        snprintf(msg, sizeof(msg), "merged ADR exceeds %d chars (%d chars)", CBM_ADR_MAX_LENGTH,
                 (int)strlen(merged));
        store_set_error(s, msg);
        free(merged);
        return CBM_STORE_ERR;
    }

    /* Store merged */
    rc = cbm_store_adr_store(s, project, merged);
    free(merged);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    return cbm_store_adr_get(s, project, out);
}

void cbm_store_adr_free(cbm_adr_t *adr) {
    if (!adr) {
        return;
    }
    free((void *)adr->project);
    free((void *)adr->content);
    free((void *)adr->created_at);
    free((void *)adr->updated_at);
    memset(adr, 0, sizeof(*adr));
}

/* ── Architecture doc discovery ────────────────────────────────── */

int cbm_store_find_architecture_docs(cbm_store_t *s, const char *project, char ***out, int *count) {
    const char *sql = "SELECT file_path FROM nodes WHERE project=?1 AND label='File' "
                      "AND (file_path LIKE '%ARCHITECTURE.md' OR file_path LIKE '%ADR.md' "
                      "OR file_path LIKE '%DECISIONS.md' OR file_path LIKE 'docs/adr/%' "
                      "OR file_path LIKE 'doc/adr/%' OR file_path LIKE 'adr/%') "
                      "ORDER BY file_path LIMIT 20";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "find_arch_docs");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, 1, project);

    int cap = 8;
    int n = 0;
    // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
    char **arr = malloc(cap * sizeof(char *));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= 2;
            // NOLINTNEXTLINE(bugprone-multi-level-implicit-pointer-conversion)
            arr = safe_realloc(arr, cap * sizeof(char *));
        }
        arr[n++] = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
    }
    sqlite3_finalize(stmt);
    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

/* ── Memory management ──────────────────────────────────────────── */

void cbm_node_free_fields(cbm_node_t *n) {
    free((void *)n->project);
    free((void *)n->label);
    free((void *)n->name);
    free((void *)n->qualified_name);
    free((void *)n->file_path);
    free((void *)n->properties_json);
}

void cbm_store_free_nodes(cbm_node_t *nodes, int count) {
    if (!nodes) {
        return;
    }
    for (int i = 0; i < count; i++) {
        cbm_node_free_fields(&nodes[i]);
    }
    free(nodes);
}

void cbm_store_free_edges(cbm_edge_t *edges, int count) {
    if (!edges) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free((void *)edges[i].project);
        free((void *)edges[i].type);
        free((void *)edges[i].properties_json);
    }
    free(edges);
}

void cbm_project_free_fields(cbm_project_t *p) {
    free((void *)p->name);
    free((void *)p->indexed_at);
    free((void *)p->root_path);
}

void cbm_store_free_projects(cbm_project_t *projects, int count) {
    if (!projects) {
        return;
    }
    for (int i = 0; i < count; i++) {
        cbm_project_free_fields(&projects[i]);
    }
    free(projects);
}

void cbm_store_free_file_hashes(cbm_file_hash_t *hashes, int count) {
    if (!hashes) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free((void *)hashes[i].project);
        free((void *)hashes[i].rel_path);
        free((void *)hashes[i].sha256);
    }
    free(hashes);
}
