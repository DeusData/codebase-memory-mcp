/*
 * store.c — SQLite graph store implementation.
 *
 * Implements the opaque cbm_store_t handle with prepared statement caching,
 * schema initialization, and all CRUD operations for nodes, edges, projects,
 * file hashes, search, BFS traversal, and schema introspection.
 */

// for ISO timestamp

#include <stdint.h>
#include "foundation/constants.h"

#include <limits.h>
#include <math.h>

enum {
    ST_COL_1 = 1,
    ST_COL_2 = 2,
    ST_COL_3 = 3,
    ST_COL_4 = 4,
    ST_COL_5 = 5,
    ST_COL_6 = 6,
    ST_COL_7 = 7,
    ST_COL_8 = 8,
    ST_COL_9 = 9,
    ST_COL_10 = 10,
    ST_FOUND = -1,
    ST_BUF_16 = 16,
    ST_BUF_64 = 64,
    ST_GROWTH = 2,
    ST_MAX_DEGREE = 8192,
    ST_HALF_SEC = 500000,
    ST_RETRY_WAIT_US = 1000,
    ST_INIT_CAP_8 = 8,
    ST_INIT_CAP_16 = 16,
    ST_SQLITE_BUSY_TIMEOUT_MS = 10000,
    ST_SQL_BUF = 8192,
    ST_ARCH_ROUTE_RESULT_LIMIT = 20,
    ST_ARCH_ROUTE_SCAN_LIMIT = ST_ARCH_ROUTE_RESULT_LIMIT * 10,
    ST_ARCH_ENTRY_POINT_LIMIT = 20,
    ST_ARCH_DOC_LIMIT = 20,
    ST_QN_MAX_DOTS = 5,
    ST_QN_MIN_DOTS = 3,
    ST_IN_CLAUSE_MARGIN = 4,
    ST_GLOB_MIN_LEN = 3,
    ST_GLOB_SKIP = 2,
    ST_MAX_LANG = 10,
    ST_SEARCH_MAX_BINDS = 32, /* increased: LIKE pre-filter adds binds per pattern */
    ST_LIKE_POOL_MAX = 12,    /* max malloc'd LIKE strings alive during one search */
    ST_LIKE_HINT_MAX = 2,     /* max LIKE hints extracted per regex pattern */
    ST_MAX_PKGS = 64,
    ST_INIT_CAP_4 = 4,
    ST_HEADER_PREFIX = 3,
    ST_MIN_INDEGREE = 3,
    ST_MAX_PATH_DEPTH = 3,
    ST_MAX_ITERATIONS = 10,
    ST_MAX_SECTIONS = 16,
    ST_METHOD_PROP_LEN = 8,
    ST_PATH_PROP_LEN = 6,
    ST_HANDLER_PROP_LEN = 9,
    ST_ARCH_PATH_LIKE_EXTRA = 3, /* "/%" plus NUL */
};

#define SLEN(s) (sizeof(s) - 1)
#include "store/store.h"
#include "service_patterns.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/log.h"
#include "foundation/profile.h"
#include "foundation/compat_regex.h"
#include "foundation/str_util.h"

#define XXH_INLINE_ALL
#include "xxhash/xxhash.h"

#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* ── SQLite bind helpers ───────────────────────────────────────── */

/* Isolate the int-to-ptr cast that SQLITE_TRANSIENT expands to.
   A union type-pun avoids the performance-no-int-to-ptr diagnostic. */
static sqlite3_destructor_type make_transient(void) {
    union {
        uintptr_t i;
        sqlite3_destructor_type fn;
    } u;
    u.i = (uintptr_t)CBM_NOT_FOUND;
    return u.fn;
}
#define BIND_TRANSIENT (make_transient())

static int bind_text(sqlite3_stmt *s, int col, const char *v) {
    return sqlite3_bind_text(s, col, v, CBM_NOT_FOUND, BIND_TRANSIENT);
}

/* ── Internal store structure ───────────────────────────────────── */

struct cbm_store {
    sqlite3 *db;
    const char *db_path; /* heap-allocated, or NULL for :memory: */
    char errbuf[CBM_SZ_512];

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

    sqlite3_stmt *stmt_upsert_file_state;
    sqlite3_stmt *stmt_get_file_state;
    sqlite3_stmt *stmt_delete_file_state;
    sqlite3_stmt *stmt_upsert_node_owner;
    sqlite3_stmt *stmt_upsert_edge_owner;
    sqlite3_stmt *stmt_delete_node_owners_by_file;
    sqlite3_stmt *stmt_delete_edge_owners_by_file;
    sqlite3_stmt *stmt_count_node_owners_by_file;
    sqlite3_stmt *stmt_count_edge_owners_by_file;
    sqlite3_stmt *stmt_list_file_delta_inbound_source_paths;
    sqlite3_stmt *stmt_upsert_symbol_export;
    sqlite3_stmt *stmt_delete_symbol_exports_by_file;
    sqlite3_stmt *stmt_list_symbol_exports_by_file;
    sqlite3_stmt *stmt_upsert_import_ref;
    sqlite3_stmt *stmt_delete_import_refs_by_file;
    sqlite3_stmt *stmt_list_import_ref_paths_by_target;
    sqlite3_stmt *stmt_list_import_ref_paths_for_export_file;
    sqlite3_stmt *stmt_delete_owned_edges_by_file;
    sqlite3_stmt *stmt_delete_owned_nodes_by_file;
    sqlite3_stmt *stmt_upsert_derived_view_state;
    sqlite3_stmt *stmt_get_derived_view_state;
};

/* ── Public accessor ────────────────────────────────────────────── */

sqlite3 *cbm_store_get_db(cbm_store_t *s) {
    return s ? s->db : NULL;
}

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
    char *d = malloc(len + SKIP_ONE);
    if (d) {
        memcpy(d, s, len + SKIP_ONE);
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
    int rc = sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, slot, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "prepare");
        return NULL;
    }
    return *slot;
}

static void store_free_text_array(char **items, int count) {
    if (!items) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(items[i]);
    }
    free(items);
}

static int store_collect_text_column(cbm_store_t *s, sqlite3_stmt *stmt, const char *op,
                                     char ***out, int *count) {
    if (!out || !count || !stmt) {
        return CBM_STORE_ERR;
    }
    *out = NULL;
    *count = 0;

    int cap = ST_INIT_CAP_8;
    int n = 0;
    char **arr = malloc((size_t)cap * sizeof(char *));
    if (!arr) {
        store_set_error(s, "collect_text_column out of memory");
        return CBM_STORE_ERR;
    }
    int rc = SQLITE_OK;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *text = (const char *)sqlite3_column_text(stmt, 0);
        if (!text) {
            continue;
        }
        if (n >= cap) {
            if (cap > INT_MAX / ST_GROWTH) {
                store_free_text_array(arr, n);
                store_set_error(s, "collect_text_column out of memory");
                return CBM_STORE_ERR;
            }
            int new_cap = cap * ST_GROWTH;
            char **next = realloc(arr, (size_t)new_cap * sizeof(*next));
            if (!next) {
                store_free_text_array(arr, n);
                store_set_error(s, "collect_text_column out of memory");
                return CBM_STORE_ERR;
            }
            arr = next;
            cap = new_cap;
        }
        char *copy = heap_strdup(text);
        if (!copy) {
            store_free_text_array(arr, n);
            store_set_error(s, "collect_text_column out of memory");
            return CBM_STORE_ERR;
        }
        arr[n++] = copy;
    }
    if (rc != SQLITE_DONE) {
        store_free_text_array(arr, n);
        store_set_error_sqlite(s, op);
        return CBM_STORE_ERR;
    }

    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

static int store_text_ptr_cmp(const void *a, const void *b) {
    const char *const *sa = (const char *const *)a;
    const char *const *sb = (const char *const *)b;
    return strcmp(*sa, *sb);
}

static int store_append_text(char ***items, int *count, int *cap, const char *text) {
    if (!items || !count || !cap || !text) {
        return CBM_STORE_ERR;
    }
    if (*count >= *cap) {
        if (*cap > INT_MAX / ST_GROWTH) {
            return CBM_STORE_ERR;
        }
        int new_cap = (*cap > 0) ? *cap * ST_GROWTH : ST_INIT_CAP_8;
        char **tmp = realloc(*items, (size_t)new_cap * sizeof(char *));
        if (!tmp) {
            return CBM_STORE_ERR;
        }
        *items = tmp;
        *cap = new_cap;
    }
    char *copy = heap_strdup(text);
    if (!copy) {
        return CBM_STORE_ERR;
    }
    (*items)[(*count)++] = copy;
    return CBM_STORE_OK;
}

static void store_sort_unique_text_array(char **items, int *count) {
    if (!items || !count || *count <= 1) {
        return;
    }
    qsort(items, (size_t)*count, sizeof(char *), store_text_ptr_cmp);
    int out = 1;
    for (int i = 1; i < *count; i++) {
        if (strcmp(items[i], items[out - 1]) == 0) {
            free(items[i]);
            continue;
        }
        items[out++] = items[i];
    }
    *count = out;
}

/* Get ISO-8601 timestamp. */
static void iso_now(char *buf, size_t sz) {
    time_t t = time(NULL);
    struct tm tm;
    cbm_gmtime_r(&t, &tm);
    (void)strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ",
                   &tm); // cert-err33-c: strftime only fails if buffer is too small — 21-byte ISO
                         // timestamp always fits in caller-provided buffers
}

/* ── Schema ─────────────────────────────────────────────────────── */

static int init_schema(cbm_store_t *s) {
    const char *ddl =
        "CREATE TABLE IF NOT EXISTS projects ("
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
        "  url_path_gen TEXT GENERATED ALWAYS AS (json_extract(properties,'$.url_path')),"
        "  UNIQUE(source_id, target_id, type)"
        ");"
        "CREATE TABLE IF NOT EXISTS project_summaries ("
        "  project TEXT PRIMARY KEY,"
        "  summary TEXT NOT NULL,"
        "  source_hash TEXT NOT NULL,"
        "  created_at TEXT NOT NULL,"
        "  updated_at TEXT NOT NULL"
        ");"
        /* fork delta: PageRank / LinkRank / node_degree ranking tables */
        "CREATE TABLE IF NOT EXISTS pagerank ("
        "  node_id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,"
        "  project TEXT NOT NULL,"
        "  rank REAL NOT NULL DEFAULT 0.0,"
        "  computed_at TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS linkrank ("
        "  edge_id INTEGER PRIMARY KEY REFERENCES edges(id) ON DELETE CASCADE,"
        "  project TEXT NOT NULL,"
        "  rank REAL NOT NULL DEFAULT 0.0,"
        "  computed_at TEXT NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS node_degree ("
        "  node_id INTEGER PRIMARY KEY REFERENCES nodes(id) ON DELETE CASCADE,"
        "  project TEXT NOT NULL,"
        "  total_in INTEGER DEFAULT 0,"
        "  total_out INTEGER DEFAULT 0,"
        "  calls_in INTEGER DEFAULT 0,"
        "  calls_out INTEGER DEFAULT 0,"
        "  weighted_in REAL DEFAULT 0,"
        "  weighted_out REAL DEFAULT 0,"
        "  linkrank_in REAL DEFAULT 0,"
        "  computed_at TEXT"
        ");"
        /* Exact-delta incremental metadata. These tables are intentionally
         * ownership/freshness indexes only; the canonical graph remains
         * nodes/edges so existing query APIs and writer-produced DBs migrate
         * through normal store open without changing their public schema. */
        "CREATE TABLE IF NOT EXISTS index_generations ("
        "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
        "  generation INTEGER NOT NULL,"
        "  started_at TEXT NOT NULL,"
        "  completed_at TEXT,"
        "  repo_fingerprint TEXT DEFAULT '',"
        "  config_fingerprint TEXT DEFAULT '',"
        "  status TEXT NOT NULL DEFAULT '" CBM_STORE_INDEX_STATUS_COMPLETE "',"
        "  PRIMARY KEY (project, generation)"
        ");"
        "CREATE TABLE IF NOT EXISTS file_state ("
        "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
        "  rel_path TEXT NOT NULL,"
        "  content_hash TEXT NOT NULL,"
        "  git_oid TEXT DEFAULT '',"
        "  mtime_ns INTEGER NOT NULL DEFAULT 0,"
        "  size INTEGER NOT NULL DEFAULT 0,"
        "  language TEXT DEFAULT '',"
        "  pass_fingerprint TEXT DEFAULT '',"
        "  generation INTEGER NOT NULL DEFAULT 0,"
        "  indexed_at TEXT NOT NULL,"
        "  PRIMARY KEY (project, rel_path)"
        ");"
        "CREATE TABLE IF NOT EXISTS node_owners ("
        "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
        "  node_id INTEGER NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,"
        "  rel_path TEXT NOT NULL,"
        "  generation INTEGER NOT NULL DEFAULT 0,"
        "  PRIMARY KEY (project, node_id)"
        ");"
        "CREATE TABLE IF NOT EXISTS edge_owners ("
        "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
        "  edge_id INTEGER NOT NULL REFERENCES edges(id) ON DELETE CASCADE,"
        "  rel_path TEXT NOT NULL,"
        "  derived_kind TEXT NOT NULL DEFAULT '" CBM_STORE_DERIVED_KIND_DIRECT "',"
        "  generation INTEGER NOT NULL DEFAULT 0,"
        "  PRIMARY KEY (project, edge_id)"
        ");"
        "CREATE TABLE IF NOT EXISTS symbol_exports ("
        "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
        "  qualified_name TEXT NOT NULL,"
        "  rel_path TEXT NOT NULL,"
        "  node_id INTEGER REFERENCES nodes(id) ON DELETE SET NULL,"
        "  generation INTEGER NOT NULL DEFAULT 0,"
        "  PRIMARY KEY (project, qualified_name, rel_path)"
        ");"
        "CREATE TABLE IF NOT EXISTS import_refs ("
        "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
        "  rel_path TEXT NOT NULL,"
        "  import_text TEXT NOT NULL,"
        "  local_name TEXT NOT NULL DEFAULT '',"
        "  target_qn TEXT DEFAULT '',"
        "  generation INTEGER NOT NULL DEFAULT 0,"
        "  PRIMARY KEY (project, rel_path, import_text, local_name)"
        ");"
        "CREATE TABLE IF NOT EXISTS derived_view_state ("
        "  project TEXT NOT NULL REFERENCES projects(name) ON DELETE CASCADE,"
        "  view_name TEXT NOT NULL,"
        "  source_generation INTEGER NOT NULL DEFAULT 0,"
        "  computed_at TEXT,"
        "  status TEXT NOT NULL DEFAULT '" CBM_STORE_DERIVED_STATUS_STALE "',"
        "  PRIMARY KEY (project, view_name)"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_node_degree_project"
        "  ON node_degree(project);";

    int rc = exec_sql(s, ddl);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    /* FTS5 contentless virtual table for BM25 full-text search.
     * Contentless (content='') means FTS5 stores only the inverted index,
     * not a copy of the source text — required for camelCase tokenization
     * because we feed it `cbm_camel_split(name)` at insert time but want
     * queries to match against the split tokens, not the original.
     * Fails silently if FTS5 is not compiled in (SQLITE_ENABLE_FTS5). */
    {
        char *fts_err = NULL;
        int fts_rc = sqlite3_exec(s->db,
                                  "CREATE VIRTUAL TABLE IF NOT EXISTS nodes_fts USING fts5("
                                  "  name, qualified_name, label, file_path,"
                                  "  content='',"
                                  "  tokenize='unicode61 remove_diacritics 2'"
                                  ");",
                                  NULL, NULL, &fts_err);
        if (fts_rc != SQLITE_OK && fts_err) {
            sqlite3_free(fts_err);
        }
    }
    return CBM_STORE_OK;
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
        "CREATE INDEX IF NOT EXISTS idx_edges_source_type ON edges(project, source_id, type);"
        "CREATE INDEX IF NOT EXISTS idx_edges_url_path ON edges(project, url_path_gen);"
        /* fork delta: ranking indexes */
        "CREATE INDEX IF NOT EXISTS idx_pagerank_project ON pagerank(project);"
        "CREATE INDEX IF NOT EXISTS idx_pagerank_rank ON pagerank(project, rank DESC);"
        "CREATE INDEX IF NOT EXISTS idx_linkrank_project ON linkrank(project);"
        "CREATE INDEX IF NOT EXISTS idx_file_state_hash ON file_state(project, content_hash);"
        "CREATE INDEX IF NOT EXISTS idx_node_owners_path ON node_owners(project, rel_path);"
        "CREATE INDEX IF NOT EXISTS idx_node_owners_node_id ON node_owners(node_id);"
        "CREATE INDEX IF NOT EXISTS idx_edge_owners_path ON edge_owners(project, rel_path);"
        "CREATE INDEX IF NOT EXISTS idx_edge_owners_edge_id ON edge_owners(edge_id);"
        "CREATE INDEX IF NOT EXISTS idx_symbol_exports_path ON symbol_exports(project, rel_path);"
        "CREATE INDEX IF NOT EXISTS idx_symbol_exports_node_id ON symbol_exports(node_id);"
        "CREATE INDEX IF NOT EXISTS idx_import_refs_target ON import_refs(project, target_qn);"
        "CREATE INDEX IF NOT EXISTS idx_derived_view_state_status"
        "  ON derived_view_state(project, status);";
    /* NOTE: a partial expression index on json_extract(properties,'$.is_entry_point')
     * was tried for arch_entry_points and REVERTED: json_extract in an index WHERE
     * aborts CREATE INDEX (and thus store open) on any row whose properties JSON is
     * malformed — and pre-fix databases contain such rows (see
     * pipeline_def_props_valid_json_when_oversized). Revisit only with a
     * json_valid()-guarded expression once legacy DBs have aged out. */
    return exec_sql(s, sql);
}

int64_t cbm_store_resolve_mmap_size(void) {
    enum { MMAP_DEFAULT = 67108864, BASE_10 = 10 }; /* default 64 MB; decimal radix */
    char buf[ST_BUF_64];
    if (cbm_safe_getenv("CBM_SQLITE_MMAP_SIZE", buf, sizeof(buf), NULL) == NULL) {
        return (int64_t)MMAP_DEFAULT;
    }
    char *end = NULL;
    long long parsed = strtoll(buf, &end, BASE_10);
    if (end == buf || *end != '\0') {
        /* Malformed — fall back to default rather than fail the store open. */
        return (int64_t)MMAP_DEFAULT;
    }
    if (parsed < 0) {
        return 0;
    }
    return (int64_t)parsed;
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
        /* busy_timeout must be set BEFORE journal_mode=WAL so that lock
         * contention during WAL mode activation is handled with a timeout
         * rather than an immediate SQLITE_BUSY error. */
        char busy_sql[ST_BUF_64];
        snprintf(busy_sql, sizeof(busy_sql), "PRAGMA busy_timeout = %d;",
                 ST_SQLITE_BUSY_TIMEOUT_MS);
        rc = exec_sql(s, busy_sql);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = exec_sql(s, "PRAGMA journal_mode = WAL;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        /* Recover stale WAL from previous crash (best-effort).
         * PASSIVE never blocks readers and never ftruncates.
         * May fail with SQLITE_BUSY if another process holds a lock. */
        (void)sqlite3_exec(s->db, "PRAGMA wal_checkpoint(PASSIVE)", NULL, NULL, NULL);
        rc = exec_sql(s, "PRAGMA synchronous = NORMAL;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        char mmap_sql[ST_BUF_64];
        snprintf(mmap_sql, sizeof(mmap_sql), "PRAGMA mmap_size = %lld;",
                 (long long)cbm_store_resolve_mmap_size());
        rc = exec_sql(s, mmap_sql);
    }
    return rc;
}

/* ── camelCase splitter for FTS5 indexing ───────────────────────── */

/* Emits the original identifier plus a space-separated split version, so FTS5's
 * whitespace tokenizer produces both `updateCloudClient` (exact match) and the
 * word tokens `update`, `cloud`, `client`.  Rules:
 *   1. insert space before an uppercase letter preceded by a lowercase letter
 *      ("updateCloud" → "update Cloud")
 *   2. insert space before an uppercase letter preceded by another uppercase
 *      but followed by a lowercase letter ("XMLParser" → "XML Parser", not
 *      "X M L Parser")
 * snake_case is already split by FTS5's unicode61 tokenizer on `_`. */
enum {
    CAMEL_SPLIT_BUF = 2048,
    SQLITE_AUTO_LEN = -1, /* sqlite3_result_text sentinel: use strlen(). */
    CAMEL_BUF_GUARD = 2,  /* reserve room for inserted space + NUL terminator. */
};

/* Denominator epsilon guard for double-precision cosine. */
#define CBM_STORE_DENOM_EPS_D 1e-10
#define CBM_STORE_DENOM_EPS_F 1e-10F
#define CBM_STORE_INT8_MAX 127.0F
#define CBM_STORE_UNIT_POS_F 1.0F
#define CBM_STORE_UNIT_POS_D 1.0

/* Module-local copy of SQLite's SQLITE_TRANSIENT sentinel ((void*)-1).
 * We construct it via memcpy from a volatile intptr_t sentinel so the
 * resulting expression isn't syntactically a direct int-to-ptr cast —
 * clang-tidy's performance-no-int-to-ptr sees the memcpy boundary and
 * doesn't flag it per-use-site. */
static sqlite3_destructor_type cbm_sqlite_transient_destructor(void) {
    static const volatile intptr_t raw = -1;
    sqlite3_destructor_type dtor = NULL;
    memcpy(&dtor, (const void *)&raw, sizeof(dtor));
    return dtor;
}
#define CBM_SQLITE_TRANSIENT (cbm_sqlite_transient_destructor())

/* True if we should insert a space BEFORE input[i] to split a camelCase word
 * boundary (lowercase→uppercase or uppercase-run→uppercase-then-lowercase). */
static bool camel_should_split(const char *input, int i) {
    if (i <= 0) {
        return false;
    }
    char curr = input[i];
    char prev = input[i - SKIP_ONE];
    char next = input[i + SKIP_ONE];
    if (curr >= 'A' && curr <= 'Z' && prev >= 'a' && prev <= 'z') {
        return true;
    }
    if (curr >= 'A' && curr <= 'Z' && prev >= 'A' && prev <= 'Z' && next >= 'a' && next <= 'z') {
        return true;
    }
    return false;
}

static void sqlite_camel_split(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *input = (const char *)sqlite3_value_text(argv[0]);
    if (!input || !input[0]) {
        sqlite3_result_text(ctx, input ? input : "", SQLITE_AUTO_LEN, CBM_SQLITE_TRANSIENT);
        return;
    }
    char buf[CAMEL_SPLIT_BUF];
    int len = snprintf(buf, sizeof(buf), "%s ", input);
    if (len < 0 || len >= (int)sizeof(buf)) {
        /* Input too long — fall back to the original string unmodified. */
        sqlite3_result_text(ctx, input, SQLITE_AUTO_LEN, CBM_SQLITE_TRANSIENT);
        return;
    }
    for (int i = 0; input[i] && len < (int)sizeof(buf) - CAMEL_BUF_GUARD; i++) {
        if (camel_should_split(input, i)) {
            buf[len++] = ' ';
        }
        buf[len++] = input[i];
    }
    buf[len] = '\0';
    sqlite3_result_text(ctx, buf, len, CBM_SQLITE_TRANSIENT);
}

/* ── REGEXP function for SQLite ──────────────────────────────────── */

/* Destructor passed to sqlite3_set_auxdata — frees the cached compiled regex. */
static void regex_free_cb(void *p) {
    cbm_regex_t *re = (cbm_regex_t *)p;
    cbm_regfree(re);
    free(re);
}

/* Cache the compiled regex on argument slot 0 for the lifetime of the statement.
 * sqlite3_get_auxdata returns the cached pointer on subsequent rows (same parameter
 * value), so cbm_regcomp is called exactly once per statement instead of once per row. */
static void sqlite_regexp(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *pattern = (const char *)sqlite3_value_text(argv[0]);
    const char *text = (const char *)sqlite3_value_text(argv[SKIP_ONE]);
    if (!pattern || !text) {
        sqlite3_result_int(ctx, 0);
        return;
    }

    cbm_regex_t *re = (cbm_regex_t *)sqlite3_get_auxdata(ctx, 0);
    if (!re) {
        re = malloc(sizeof(cbm_regex_t));
        if (!re) {
            sqlite3_result_error_nomem(ctx);
            return;
        }
        if (cbm_regcomp(re, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != 0) {
            free(re);
            sqlite3_result_error(ctx, "invalid regex", CBM_NOT_FOUND);
            return;
        }
        sqlite3_set_auxdata(ctx, 0, re, regex_free_cb);
    }

    sqlite3_result_int(ctx, cbm_regexec(re, text, 0, NULL, 0) == 0 ? SKIP_ONE : 0);
}

/* Case-insensitive REGEXP variant — same auxdata caching strategy. */
static void sqlite_iregexp(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *pattern = (const char *)sqlite3_value_text(argv[0]);
    const char *text = (const char *)sqlite3_value_text(argv[SKIP_ONE]);
    if (!pattern || !text) {
        sqlite3_result_int(ctx, 0);
        return;
    }

    cbm_regex_t *re = (cbm_regex_t *)sqlite3_get_auxdata(ctx, 0);
    if (!re) {
        re = malloc(sizeof(cbm_regex_t));
        if (!re) {
            sqlite3_result_error_nomem(ctx);
            return;
        }
        if (cbm_regcomp(re, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB | CBM_REG_ICASE) != 0) {
            free(re);
            sqlite3_result_error(ctx, "invalid regex", CBM_NOT_FOUND);
            return;
        }
        sqlite3_set_auxdata(ctx, 0, re, regex_free_cb);
    }

    sqlite3_result_int(ctx, cbm_regexec(re, text, 0, NULL, 0) == 0 ? SKIP_ONE : 0);
}

/* Cosine similarity between two int8 BLOB vectors.
 * Returns float in [-1, 1].  Used for vector search at query time. */
static void sqlite_cosine_i8(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB ||
        sqlite3_value_type(argv[SKIP_ONE]) != SQLITE_BLOB) {
        sqlite3_result_double(ctx, 0.0);
        return;
    }
    int len_a = sqlite3_value_bytes(argv[0]);
    int len_b = sqlite3_value_bytes(argv[SKIP_ONE]);
    if (len_a != len_b || len_a == 0) {
        sqlite3_result_double(ctx, 0.0);
        return;
    }
    const int8_t *a = (const int8_t *)sqlite3_value_blob(argv[0]);
    const int8_t *b = (const int8_t *)sqlite3_value_blob(argv[SKIP_ONE]);
    int32_t dot = 0;
    int32_t mag_a = 0;
    int32_t mag_b = 0;
    for (int i = 0; i < len_a; i++) {
        dot += (int32_t)a[i] * (int32_t)b[i];
        mag_a += (int32_t)a[i] * (int32_t)a[i];
        mag_b += (int32_t)b[i] * (int32_t)b[i];
    }
    double denom = sqrt((double)mag_a) * sqrt((double)mag_b);
    sqlite3_result_double(ctx, denom > CBM_STORE_DENOM_EPS_D ? (double)dot / denom : 0.0);
}

/* ── Lifecycle ──────────────────────────────────────────────────── */

/* SQLite authorizer: deny dangerous operations that could be exploited via
 * SQL injection through the Cypher→SQL translation layer. */
static int store_authorizer(void *user_data, int action, const char *p3, const char *p4,
                            const char *p5, const char *p6) {
    (void)user_data;
    (void)p3;
    (void)p4;
    (void)p5;
    (void)p6;
    switch (action) {
    case SQLITE_ATTACH: /* ATTACH DATABASE — could create/read arbitrary files */
    case SQLITE_DETACH: /* DETACH DATABASE */
        return SQLITE_DENY;
    default:
        return SQLITE_OK;
    }
}

static cbm_store_t *store_open_internal(const char *path, bool in_memory) {
    cbm_store_t *s = calloc(CBM_ALLOC_ONE, sizeof(cbm_store_t));
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

    /* Security: block ATTACH/DETACH to prevent file creation via SQL injection.
     * The authorizer runs inside SQLite's query planner — no string-level bypass. */
    sqlite3_set_authorizer(s->db, store_authorizer, NULL);

    /* Register REGEXP function (SQLite doesn't have one built-in) */
    sqlite3_create_function(s->db, "regexp", ST_COL_2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                            sqlite_regexp, NULL, NULL);
    /* Case-insensitive variant for search with case_sensitive=false */
    sqlite3_create_function(s->db, "iregexp", ST_COL_2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                            sqlite_iregexp, NULL, NULL);
    /* Int8 cosine similarity for vector search */
    sqlite3_create_function(s->db, "cbm_cosine_i8", ST_COL_2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            NULL, sqlite_cosine_i8, NULL, NULL);
    /* camelCase splitter for FTS5 BM25 indexing */
    sqlite3_create_function(s->db, "cbm_camel_split", SKIP_ONE, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            NULL, sqlite_camel_split, NULL, NULL);

    if (configure_pragmas(s, in_memory) != CBM_STORE_OK || init_schema(s) != CBM_STORE_OK ||
        create_user_indexes(s) != CBM_STORE_OK) {
        sqlite3_close(s->db);
        safe_str_free(&s->db_path);
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

/* Open a DB read-write but without SQLITE_OPEN_CREATE, so no ghost .db file
 * is created for unknown/unindexed projects. Returns NULL if file absent. */
cbm_store_t *cbm_store_open_path_query(const char *db_path) {
    if (!db_path) {
        return NULL;
    }

    cbm_store_t *s = calloc(CBM_ALLOC_ONE, sizeof(cbm_store_t));
    if (!s) {
        return NULL;
    }

    /* No SQLITE_OPEN_CREATE — returns SQLITE_CANTOPEN if file absent. */
    int rc = sqlite3_open_v2(db_path, &s->db, SQLITE_OPEN_READWRITE, NULL);
    if (rc != SQLITE_OK) {
        /* sqlite3_open_v2 allocates a handle even on failure — must close it. */
        sqlite3_close(s->db);
        free(s);
        return NULL;
    }

    s->db_path = heap_strdup(db_path);

    /* Security: block ATTACH/DETACH to prevent file creation via SQL injection. */
    sqlite3_set_authorizer(s->db, store_authorizer, NULL);

    /* Register REGEXP functions (same as store_open_internal). */
    sqlite3_create_function(s->db, "regexp", ST_COL_2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                            sqlite_regexp, NULL, NULL);
    sqlite3_create_function(s->db, "iregexp", ST_COL_2, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                            sqlite_iregexp, NULL, NULL);
    sqlite3_create_function(s->db, "cbm_cosine_i8", ST_COL_2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            NULL, sqlite_cosine_i8, NULL, NULL);
    sqlite3_create_function(s->db, "cbm_camel_split", SKIP_ONE, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            NULL, sqlite_camel_split, NULL, NULL);

    if (configure_pragmas(s, false) != CBM_STORE_OK) {
        sqlite3_close(s->db);
        safe_str_free(&s->db_path);
        free(s);
        return NULL;
    }

    return s;
}

/* ── Integrity check ───────────────────────────────────────────── */

bool cbm_store_check_integrity_full(cbm_store_t *s, bool *path_only_failure) {
    if (path_only_failure) {
        *path_only_failure = false;
    }
    if (!s || !s->db) {
        return false;
    }

    /* The writer creates one projects row initially, then dependency indexing
     * may add rows for projects stored in the parent DB. Treat the table as
     * sane if it is readable and non-empty; row count alone is not a corruption
     * signal. Real B1 corruption is caught below by malformed root_path values. */
    sqlite3_stmt *stmt = NULL;
    int rc =
        sqlite3_prepare_v2(s->db, "SELECT count(*) FROM projects;", CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return false;
    }

    bool ok = true;
    bool rows_ok = true;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        int row_count = sqlite3_column_int(stmt, 0);
        if (row_count < 0) {
            (void)fprintf(stderr, "ERROR store.corrupt table=projects rows=%d\n", row_count);
            ok = false;
            rows_ok = false;
        }
    } else {
        ok = false;
        rows_ok = false;
    }
    sqlite3_finalize(stmt);

    bool path_bad = false;
    if (ok) {
        /* Check that root_path in projects table starts with '/' or a drive
         * letter. Corrupt DBs often have numeric strings like "826" in
         * root_path. Drive letters may be upper- OR lower-case on Windows
         * (e.g. "c:/repo", "y:/share") — rejecting lowercase here flagged
         * valid Windows paths as corrupt and deleted the DB (#227/#367). */
        rc = sqlite3_prepare_v2(s->db,
                                "SELECT root_path FROM projects WHERE root_path != '' "
                                "AND NOT (substr(root_path, 1, 1) = '/' "
                                "OR (substr(root_path, 1, 1) BETWEEN 'A' AND 'Z') "
                                "OR (substr(root_path, 1, 1) BETWEEN 'a' AND 'z')) LIMIT 1;",
                                CBM_NOT_FOUND, &stmt, NULL);
        if (rc == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *bad_path = (const char *)sqlite3_column_text(stmt, 0);
                (void)fprintf(stderr, "ERROR store.corrupt table=projects bad_root_path=%s\n",
                              bad_path ? bad_path : "(null)");
                ok = false;
                path_bad = true;
            }
            sqlite3_finalize(stmt);
        }
    }

    /* A bad root_path with an otherwise-fine projects table is a cosmetic
     * project-row defect: the node/edge data is intact and queries (which key
     * off project name, not root_path) remain correct. Surface this so callers
     * can retain the DB instead of deleting it (#557 data loss). */
    if (path_only_failure && !ok && rows_ok && path_bad) {
        *path_only_failure = true;
    }

    return ok;
}

bool cbm_store_check_integrity(cbm_store_t *s) {
    return cbm_store_check_integrity_full(s, NULL);
}

cbm_store_t *cbm_store_open(const char *project) {
    if (!project) {
        return NULL;
    }
    if (!cbm_validate_project_name(project)) {
        return NULL;
    }
    const char *cdir = cbm_resolve_cache_dir();
    if (!cdir) {
        cdir = cbm_tmpdir();
    }
    char path[CBM_SZ_1K];
    int path_len = snprintf(path, sizeof(path), "%s/%s.db", cdir, project);
    if (path_len <= 0 || (size_t)path_len >= sizeof(path)) {
        return NULL;
    }
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

    /* Checkpoint WAL before close to prevent orphan WAL accumulation.
     * Best-effort — silently skips if concurrent reader holds a lock. */
    if (s->db && s->db_path) {
        (void)sqlite3_wal_checkpoint_v2(s->db, NULL, SQLITE_CHECKPOINT_PASSIVE, NULL, NULL);
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

    finalize_stmt(&s->stmt_upsert_file_state);
    finalize_stmt(&s->stmt_get_file_state);
    finalize_stmt(&s->stmt_delete_file_state);
    finalize_stmt(&s->stmt_upsert_node_owner);
    finalize_stmt(&s->stmt_upsert_edge_owner);
    finalize_stmt(&s->stmt_delete_node_owners_by_file);
    finalize_stmt(&s->stmt_delete_edge_owners_by_file);
    finalize_stmt(&s->stmt_count_node_owners_by_file);
    finalize_stmt(&s->stmt_count_edge_owners_by_file);
    finalize_stmt(&s->stmt_list_file_delta_inbound_source_paths);
    finalize_stmt(&s->stmt_upsert_symbol_export);
    finalize_stmt(&s->stmt_delete_symbol_exports_by_file);
    finalize_stmt(&s->stmt_list_symbol_exports_by_file);
    finalize_stmt(&s->stmt_upsert_import_ref);
    finalize_stmt(&s->stmt_delete_import_refs_by_file);
    finalize_stmt(&s->stmt_list_import_ref_paths_by_target);
    finalize_stmt(&s->stmt_list_import_ref_paths_for_export_file);
    finalize_stmt(&s->stmt_delete_owned_edges_by_file);
    finalize_stmt(&s->stmt_delete_owned_nodes_by_file);
    finalize_stmt(&s->stmt_upsert_derived_view_state);
    finalize_stmt(&s->stmt_get_derived_view_state);

    /* Use sqlite3_close_v2 — auto-deallocates when last statement finalizes.
     * Prevents ASan false-positive leaks from sqlite3 internal state. */
    sqlite3_close_v2(s->db);
    safe_str_free(&s->db_path);
    free(s);
}

int cbm_store_exec(cbm_store_t *s, const char *sql) {
    return exec_sql(s, sql);
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
    /* Stay in WAL mode throughout. Switching to MEMORY journal mode would
     * make the database unrecoverable if the process crashes mid-write,
     * because the in-memory rollback journal is lost on crash.
     * WAL mode is crash-safe: uncommitted WAL entries are simply discarded
     * on the next open. Performance is preserved via synchronous=OFF and a
     * larger cache, which are safe with WAL. */
    int rc = exec_sql(s, "PRAGMA synchronous = OFF;");
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    return exec_sql(s, "PRAGMA cache_size = -65536;"); /* CBM_SZ_64 MB */
}

int cbm_store_end_bulk(cbm_store_t *s) {
    /* Restore normal durability settings; WAL mode was preserved throughout. */
    int rc = exec_sql(s, "PRAGMA synchronous = NORMAL;");
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
    if (!s) {
        return CBM_STORE_ERR;
    }
    /* PASSIVE never blocks readers and never ftruncate()s either file.
     * SQLite recommends PASSIVE for shared databases — TRUNCATE shrinks
     * the WAL via ftruncate(fd, 0) on success, which on macOS can raise
     * SIGBUS in a sibling process that has the DB mmap'd through SQLite
     * when it next faults a page in the now-shorter region.
     * See https://www.sqlite.org/c3ref/c_checkpoint_full.html */
    int rc = sqlite3_wal_checkpoint_v2(s->db, NULL, SQLITE_CHECKPOINT_PASSIVE, NULL, NULL);
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
    char dir[CBM_SZ_1K];
    int dir_len = snprintf(dir, sizeof(dir), "%s", dest_path);
    if (dir_len <= 0 || (size_t)dir_len >= sizeof(dir)) {
        store_set_error(s, "dump: destination path too long");
        return CBM_STORE_ERR;
    }
    char *sl = strrchr(dir, '/');
    if (sl) {
        *sl = '\0';
        (void)cbm_mkdir(dir);
    }

    /* Write to a unique temp file for atomic swap. This avoids deleting or
     * colliding with a sibling writer's predictable "<dest>.tmp" file. */
    char tmp_path[CBM_SZ_1K];
    int tmp_len = snprintf(tmp_path, sizeof(tmp_path), "%s.tmp.XXXXXX", dest_path);
    if (tmp_len <= 0 || (size_t)tmp_len >= sizeof(tmp_path)) {
        store_set_error(s, "dump: temp path too long");
        return CBM_STORE_ERR;
    }
    int tmp_fd = cbm_mkstemp_s(tmp_path, sizeof(tmp_path));
    if (tmp_fd < 0) {
        store_set_error(s, "dump: cannot create temp file");
        return CBM_STORE_ERR;
    }
    cbm_close_fd(tmp_fd);

    sqlite3 *dest_db = NULL;
    int rc = sqlite3_open(tmp_path, &dest_db);
    if (rc != SQLITE_OK) {
        store_set_error(s, "dump: cannot open temp file");
        (void)cbm_unlink(tmp_path);
        return CBM_STORE_ERR;
    }

    sqlite3_backup *bk = sqlite3_backup_init(dest_db, "main", s->db, "main");
    if (!bk) {
        store_set_error(s, "dump: backup init failed");
        sqlite3_close(dest_db);
        (void)cbm_unlink(tmp_path);
        return CBM_STORE_ERR;
    }

    rc = sqlite3_backup_step(bk, CBM_NOT_FOUND); /* copy ALL pages in one shot */
    sqlite3_backup_finish(bk);

    if (rc != SQLITE_DONE) {
        store_set_error(s, "dump: backup step failed");
        sqlite3_close(dest_db);
        (void)cbm_unlink(tmp_path);
        return CBM_STORE_ERR;
    }

    /* Enable WAL on the dumped file so readers can connect concurrently */
    sqlite3_exec(dest_db, "PRAGMA journal_mode = WAL;", NULL, NULL, NULL);
    sqlite3_close(dest_db);

    /* Atomic rename: old WAL/SHM become stale and get recreated by
     * the next reader's configure_pragmas call. */
    if (cbm_replace_file(tmp_path, dest_path) != 0) {
        store_set_error(s, "dump: rename failed");
        (void)cbm_unlink(tmp_path);
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

    char ts[CBM_SZ_64];
    iso_now(ts, sizeof(ts));

    bind_text(stmt, SKIP_ONE, name);
    bind_text(stmt, ST_COL_2, ts);
    bind_text(stmt, ST_COL_3, root_path);

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

    bind_text(stmt, SKIP_ONE, name);
    int rc = sqlite3_step(stmt);
    if (rc == SQLITE_ROW) {
        out->name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        out->indexed_at = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
        out->root_path = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_2));
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
    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_project_t *arr = malloc(cap * sizeof(cbm_project_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
            arr = safe_realloc(arr, cap * sizeof(cbm_project_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].indexed_at = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
        arr[n].root_path = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_2));
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

    bind_text(stmt, SKIP_ONE, name);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_project");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── Node CRUD ──────────────────────────────────────────────────── */

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

    bind_text(stmt, SKIP_ONE, safe_str(n->project));
    bind_text(stmt, ST_COL_2, safe_str(n->label));
    bind_text(stmt, ST_COL_3, safe_str(n->name));
    bind_text(stmt, ST_COL_4, safe_str(n->qualified_name));
    bind_text(stmt, ST_COL_5, safe_str(n->file_path));
    sqlite3_bind_int(stmt, ST_COL_6, n->start_line);
    sqlite3_bind_int(stmt, ST_COL_7, n->end_line);
    bind_text(stmt, ST_COL_8, safe_props(n->properties_json));

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
    n->project = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
    n->label = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_2));
    n->name = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_3));
    n->qualified_name = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_4));
    n->file_path = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_5));
    n->start_line = sqlite3_column_int(stmt, CBM_SZ_6);
    n->end_line = sqlite3_column_int(stmt, CBM_SZ_7);
    n->properties_json = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_8));
}

int cbm_store_find_node_by_id(cbm_store_t *s, int64_t id, cbm_node_t *out) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_find_node_by_id,
                       "SELECT id, project, label, name, qualified_name, file_path, "
                       "start_line, end_line, properties FROM nodes WHERE id = ?1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    sqlite3_bind_int64(stmt, SKIP_ONE, id);
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, qn);
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

    bind_text(stmt, SKIP_ONE, qn);
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

    bind_text(stmt, SKIP_ONE, name);

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_node_t *arr = malloc(cap * sizeof(cbm_node_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
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

    memset(out_ids, 0, (size_t)qn_count * sizeof(int64_t));

    int found = 0;
    int offset = 0;
    const int sqlite_bind_limit = sqlite3_limit(s->db, SQLITE_LIMIT_VARIABLE_NUMBER, -1);
    const int qn_bind_limit =
        sqlite_bind_limit > SKIP_ONE ? (sqlite_bind_limit - SKIP_ONE) / PAIR_LEN : SKIP_ONE;
    static const char prefix[] = "WITH q(ord, qn) AS (VALUES ";
    static const char suffix[] =
        ") SELECT q.ord, n.id FROM q JOIN nodes n "
        "ON n.project=? AND n.qualified_name=q.qn;";

    while (offset < qn_count) {
        char sql[ST_SQL_BUF];
        int pos = snprintf(sql, sizeof(sql), "%s", prefix);
        if (pos < 0 || pos >= (int)sizeof(sql)) {
            return CBM_STORE_ERR;
        }

        int chunk_end = offset;
        int bind_count = 0;
        for (; chunk_end < qn_count && bind_count < qn_bind_limit; chunk_end++) {
            if (!qns[chunk_end] || out_ids[chunk_end] > CBM_STORE_NO_NODE_ID) {
                continue;
            }
            static const char row_sql[] = "(?,?)";
            int extra = (bind_count > 0 ? 1 : 0) + (int)SLEN(row_sql) + (int)SLEN(suffix);
            if (pos + extra + ST_IN_CLAUSE_MARGIN >= (int)sizeof(sql)) {
                break;
            }
            if (bind_count > 0) {
                sql[pos++] = ',';
            }
            memcpy(sql + pos, row_sql, SLEN(row_sql));
            pos += (int)SLEN(row_sql);
            bind_count++;
        }
        if (bind_count == 0) {
            offset++;
            continue;
        }
        if (pos + (int)SLEN(suffix) >= (int)sizeof(sql)) {
            return CBM_STORE_ERR;
        }
        memcpy(sql + pos, suffix, SLEN(suffix) + 1);

        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
            store_set_error_sqlite(s, "find_node_ids_by_qns");
            return CBM_STORE_ERR;
        }

        int bind_col = SKIP_ONE;
        for (int i = offset; i < chunk_end; i++) {
            if (!qns[i] || out_ids[i] > CBM_STORE_NO_NODE_ID) {
                continue;
            }
            sqlite3_bind_int(stmt, bind_col++, i);
            bind_text(stmt, bind_col++, qns[i]);
        }
        bind_text(stmt, bind_col, project);

        int step_rc = SQLITE_ROW;
        while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
            int index = sqlite3_column_int(stmt, 0);
            int64_t id = sqlite3_column_int64(stmt, SKIP_ONE);
            if (index < 0 || index >= qn_count || id <= CBM_STORE_NO_NODE_ID ||
                out_ids[index] > CBM_STORE_NO_NODE_ID) {
                continue;
            }
            out_ids[index] = id;
            found++;
        }
        if (step_rc != SQLITE_DONE) {
            store_set_error_sqlite(s, "find_node_ids_by_qns");
            sqlite3_finalize(stmt);
            return CBM_STORE_ERR;
        }
        sqlite3_finalize(stmt);
        offset = chunk_end;
    }
    return found;
}

/* Generic: find multiple nodes by a single-column filter. */
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, val);

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_node_t *arr = malloc(cap * sizeof(cbm_node_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
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

int cbm_store_visit_nodes_by_label(cbm_store_t *s, const char *project, const char *label,
                                   cbm_store_node_identity_visitor_fn visitor, void *userdata) {
    enum {
        VISIT_NODE_LABEL_COL = 0,
        VISIT_NODE_NAME_COL,
        VISIT_NODE_QN_COL,
        VISIT_NODE_FILE_PATH_COL,
    };
    if (!s || !s->db || !project || !label || !visitor) {
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db,
                                "SELECT label, name, qualified_name, file_path FROM nodes "
                                "WHERE project = ?1 AND label = ?2;",
                                CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK || !stmt) {
        store_set_error_sqlite(s, "visit_nodes_by_label prepare");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, label);
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *row_label = (const char *)sqlite3_column_text(stmt, VISIT_NODE_LABEL_COL);
        const char *row_name = (const char *)sqlite3_column_text(stmt, VISIT_NODE_NAME_COL);
        const char *row_qn = (const char *)sqlite3_column_text(stmt, VISIT_NODE_QN_COL);
        const char *row_path = (const char *)sqlite3_column_text(stmt, VISIT_NODE_FILE_PATH_COL);
        if (visitor(row_label, row_name, row_qn, row_path, userdata) != CBM_STORE_OK) {
            sqlite3_finalize(stmt);
            return CBM_STORE_ERR;
        }
    }
    if (rc != SQLITE_DONE) {
        store_set_error_sqlite(s, "visit_nodes_by_label");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    return CBM_STORE_OK;
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

    bind_text(stmt, SKIP_ONE, project);
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

    bind_text(stmt, SKIP_ONE, project);
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, file_path);
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, label);
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

    exec_sql(s, "BEGIN IMMEDIATE;");
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

    bind_text(stmt, SKIP_ONE, safe_str(e->project));
    sqlite3_bind_int64(stmt, PAIR_LEN, e->source_id);
    sqlite3_bind_int64(stmt, CBM_SZ_3, e->target_id);
    bind_text(stmt, ST_COL_4, safe_str(e->type));
    bind_text(stmt, ST_COL_5, safe_props(e->properties_json));

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
    e->project = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
    e->source_id = sqlite3_column_int64(stmt, CBM_SZ_2);
    e->target_id = sqlite3_column_int64(stmt, CBM_SZ_3);
    e->type = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_4));
    e->properties_json = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_5));
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

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_edge_t *arr = malloc(cap * sizeof(cbm_edge_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
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
    sqlite3_bind_int64(stmt, SKIP_ONE, b->id);
}

static void bind_id_and_type(sqlite3_stmt *stmt, const void *data) {
    const bind_id_type_t *b = data;
    sqlite3_bind_int64(stmt, SKIP_ONE, b->id);
    bind_text(stmt, ST_COL_2, b->type);
}

static void bind_proj_and_type(sqlite3_stmt *stmt, const void *data) {
    const bind_proj_type_t *b = data;
    bind_text(stmt, SKIP_ONE, b->project);
    bind_text(stmt, ST_COL_2, b->type);
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

    bind_text(stmt, SKIP_ONE, project);
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, type);
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

    bind_text(stmt, SKIP_ONE, project);
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, type);
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

    exec_sql(s, "BEGIN IMMEDIATE;");
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, rel_path);
    bind_text(stmt, ST_COL_3, sha256);
    sqlite3_bind_int64(stmt, CBM_SZ_4, mtime_ns);
    sqlite3_bind_int64(stmt, CBM_SZ_5, size);

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

    bind_text(stmt, SKIP_ONE, project);

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_file_hash_t *arr = malloc(cap * sizeof(cbm_file_hash_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
            arr = safe_realloc(arr, cap * sizeof(cbm_file_hash_t));
        }
        arr[n].project = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].rel_path = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
        arr[n].sha256 = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_2));
        arr[n].mtime_ns = sqlite3_column_int64(stmt, CBM_SZ_3);
        arr[n].size = sqlite3_column_int64(stmt, CBM_SZ_4);
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

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, rel_path);
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

    bind_text(stmt, SKIP_ONE, project);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_file_hashes");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

/* ── Exact-delta metadata ───────────────────────────────────────── */

int cbm_store_upsert_file_state(cbm_store_t *s, const cbm_file_state_t *state) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_upsert_file_state,
                       "INSERT INTO file_state (project, rel_path, content_hash, git_oid, "
                       "mtime_ns, size, language, pass_fingerprint, generation, indexed_at) "
                       "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10) "
                       "ON CONFLICT(project, rel_path) DO UPDATE SET "
                       "content_hash=?3, git_oid=?4, mtime_ns=?5, size=?6, language=?7, "
                       "pass_fingerprint=?8, generation=?9, indexed_at=?10;");
    if (!stmt || !state) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, state->project);
    bind_text(stmt, ST_COL_2, state->rel_path);
    bind_text(stmt, ST_COL_3, state->content_hash);
    bind_text(stmt, ST_COL_4, safe_str(state->git_oid));
    sqlite3_bind_int64(stmt, ST_COL_5, state->mtime_ns);
    sqlite3_bind_int64(stmt, ST_COL_6, state->size);
    bind_text(stmt, ST_COL_7, safe_str(state->language));
    bind_text(stmt, ST_COL_8, safe_str(state->pass_fingerprint));
    sqlite3_bind_int64(stmt, ST_COL_9, state->generation);
    bind_text(stmt, ST_COL_10, state->indexed_at);

    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_file_state");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_get_file_state(cbm_store_t *s, const char *project, const char *rel_path,
                             cbm_file_state_t *out) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_get_file_state,
                       "SELECT project, rel_path, content_hash, git_oid, mtime_ns, size, "
                       "language, pass_fingerprint, generation, indexed_at "
                       "FROM file_state WHERE project = ?1 AND rel_path = ?2;");
    if (!stmt || !out) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        return CBM_STORE_NOT_FOUND;
    }

    out->project = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
    out->rel_path = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_1));
    out->content_hash = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_2));
    out->git_oid = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_3));
    out->mtime_ns = sqlite3_column_int64(stmt, ST_COL_4);
    out->size = sqlite3_column_int64(stmt, ST_COL_5);
    out->language = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_6));
    out->pass_fingerprint = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_7));
    out->generation = sqlite3_column_int64(stmt, ST_COL_8);
    out->indexed_at = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_9));
    return CBM_STORE_OK;
}

int cbm_store_delete_file_state(cbm_store_t *s, const char *project, const char *rel_path) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_delete_file_state,
                       "DELETE FROM file_state WHERE project = ?1 AND rel_path = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_file_state");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_upsert_node_owner(cbm_store_t *s, const char *project, int64_t node_id,
                                const char *rel_path, int64_t generation) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_upsert_node_owner,
                       "INSERT INTO node_owners (project, node_id, rel_path, generation) "
                       "VALUES (?1, ?2, ?3, ?4) "
                       "ON CONFLICT(project, node_id) DO UPDATE SET rel_path=?3, generation=?4;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    sqlite3_bind_int64(stmt, ST_COL_2, node_id);
    bind_text(stmt, ST_COL_3, rel_path);
    sqlite3_bind_int64(stmt, ST_COL_4, generation);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_node_owner");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_upsert_edge_owner(cbm_store_t *s, const char *project, int64_t edge_id,
                                const char *rel_path, const char *derived_kind,
                                int64_t generation) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_upsert_edge_owner,
                       "INSERT INTO edge_owners (project, edge_id, rel_path, derived_kind, "
                       "generation) VALUES (?1, ?2, ?3, ?4, ?5) "
                       "ON CONFLICT(project, edge_id) DO UPDATE SET "
                       "rel_path=?3, derived_kind=?4, generation=?5;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    sqlite3_bind_int64(stmt, ST_COL_2, edge_id);
    bind_text(stmt, ST_COL_3, rel_path);
    bind_text(stmt, ST_COL_4, derived_kind ? derived_kind : CBM_STORE_DERIVED_KIND_DIRECT);
    sqlite3_bind_int64(stmt, ST_COL_5, generation);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_edge_owner");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int store_exec_rebuild_owner_sql(cbm_store_t *s, const char *sql, const char *project,
                                        int64_t generation, const char *op) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, op);
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    sqlite3_bind_int64(stmt, ST_COL_2, generation);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, op);
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    return CBM_STORE_OK;
}

int cbm_store_rebuild_file_delta_owners(cbm_store_t *s, const char *project,
                                        int64_t generation) {
    if (!s || !project || !project[0] || generation < 0) {
        if (s) {
            store_set_error(s, "rebuild_file_delta_owners: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    sqlite3_stmt *delete_nodes = NULL;
    sqlite3_stmt *delete_edges = NULL;
    const char *delete_node_sql = "DELETE FROM node_owners WHERE project = ?1;";
    const char *delete_edge_sql = "DELETE FROM edge_owners WHERE project = ?1;";
    if (sqlite3_prepare_v2(s->db, delete_node_sql, CBM_NOT_FOUND, &delete_nodes, NULL) !=
            SQLITE_OK ||
        sqlite3_prepare_v2(s->db, delete_edge_sql, CBM_NOT_FOUND, &delete_edges, NULL) !=
            SQLITE_OK) {
        store_set_error_sqlite(s, "rebuild_file_delta_owners delete prepare");
        sqlite3_finalize(delete_nodes);
        sqlite3_finalize(delete_edges);
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }
    bind_text(delete_nodes, ST_COL_1, project);
    bind_text(delete_edges, ST_COL_1, project);
    if (sqlite3_step(delete_nodes) != SQLITE_DONE || sqlite3_step(delete_edges) != SQLITE_DONE) {
        store_set_error_sqlite(s, "rebuild_file_delta_owners delete");
        sqlite3_finalize(delete_nodes);
        sqlite3_finalize(delete_edges);
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(delete_nodes);
    sqlite3_finalize(delete_edges);

    const char *node_sql =
        "INSERT INTO node_owners (project, node_id, rel_path, generation) "
        "SELECT n.project, n.id, n.file_path, ?2 FROM nodes n "
        "WHERE n.project = ?1 AND n.file_path IS NOT NULL AND n.file_path <> '' "
        "AND EXISTS (SELECT 1 FROM nodes f "
        "            WHERE f.project = n.project AND f.label = 'File' "
        "              AND f.file_path = n.file_path);";
    rc = store_exec_rebuild_owner_sql(s, node_sql, project, generation,
                                      "rebuild_file_delta_owners nodes");
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }

    /* Folder/Project structure nodes can carry directory paths in file_path.
     * Only paths backed by a File node are valid delta ownership rel_paths. */
    const char *edge_sql =
        "INSERT INTO edge_owners (project, edge_id, rel_path, derived_kind, generation) "
        "SELECT e.project, e.id, "
        "CASE WHEN sf.file_path IS NOT NULL THEN src.file_path ELSE tgt.file_path END, ?3, ?2 "
        "FROM edges e "
        "JOIN nodes src ON src.project = e.project AND src.id = e.source_id "
        "JOIN nodes tgt ON tgt.project = e.project AND tgt.id = e.target_id "
        "LEFT JOIN (SELECT DISTINCT project, file_path FROM nodes "
        "           WHERE label = 'File' AND file_path IS NOT NULL AND file_path <> '') sf "
        "  ON sf.project = e.project AND sf.file_path = src.file_path "
        "LEFT JOIN (SELECT DISTINCT project, file_path FROM nodes "
        "           WHERE label = 'File' AND file_path IS NOT NULL AND file_path <> '') tf "
        "  ON tf.project = e.project AND tf.file_path = tgt.file_path "
        "WHERE e.project = ?1 AND (sf.file_path IS NOT NULL OR tf.file_path IS NOT NULL);";
    sqlite3_stmt *edge_stmt = NULL;
    if (sqlite3_prepare_v2(s->db, edge_sql, CBM_NOT_FOUND, &edge_stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "rebuild_file_delta_owners edges prepare");
        sqlite3_finalize(edge_stmt);
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }
    bind_text(edge_stmt, ST_COL_1, project);
    sqlite3_bind_int64(edge_stmt, ST_COL_2, generation);
    bind_text(edge_stmt, ST_COL_3, CBM_STORE_DERIVED_KIND_DIRECT);
    if (sqlite3_step(edge_stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "rebuild_file_delta_owners edges");
        sqlite3_finalize(edge_stmt);
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(edge_stmt);

    rc = cbm_store_commit(s);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_node_owners_by_file(cbm_store_t *s, const char *project,
                                         const char *rel_path) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_delete_node_owners_by_file,
                       "DELETE FROM node_owners WHERE project = ?1 AND rel_path = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_node_owners_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_edge_owners_by_file(cbm_store_t *s, const char *project,
                                         const char *rel_path) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_delete_edge_owners_by_file,
                       "DELETE FROM edge_owners WHERE project = ?1 AND rel_path = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_edge_owners_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int store_count_file_owner_rows(cbm_store_t *s, sqlite3_stmt **slot, const char *sql,
                                       const char *project, const char *rel_path,
                                       const char *op, int *out_count) {
    if (out_count) {
        *out_count = 0;
    }
    if (!s || !project || !rel_path || !out_count) {
        if (s) {
            store_set_error(s, "count_file_owner_rows: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    sqlite3_stmt *stmt = prepare_cached(s, slot, sql);
    if (!stmt) {
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    int step = sqlite3_step(stmt);
    if (step != SQLITE_ROW) {
        store_set_error_sqlite(s, op);
        return CBM_STORE_ERR;
    }
    *out_count = sqlite3_column_int(stmt, 0);
    return CBM_STORE_OK;
}

int cbm_store_count_file_delta_owners(cbm_store_t *s, const char *project,
                                      const char *rel_path, int *out_node_owners,
                                      int *out_edge_owners) {
    static const char node_sql[] =
        "SELECT COUNT(*) FROM node_owners WHERE project = ?1 AND rel_path = ?2;";
    static const char edge_sql[] =
        "SELECT COUNT(*) FROM edge_owners WHERE project = ?1 AND rel_path = ?2;";
    if (!s || !out_node_owners || !out_edge_owners) {
        if (out_node_owners) {
            *out_node_owners = 0;
        }
        if (out_edge_owners) {
            *out_edge_owners = 0;
        }
        if (s) {
            store_set_error(s, "count_file_delta_owners: invalid argument");
        }
        return CBM_STORE_ERR;
    }
    int rc = store_count_file_owner_rows(s, &s->stmt_count_node_owners_by_file, node_sql, project,
                                         rel_path, "count_node_owners_by_file", out_node_owners);
    if (rc != CBM_STORE_OK) {
        if (out_edge_owners) {
            *out_edge_owners = 0;
        }
        return rc;
    }
    return store_count_file_owner_rows(s, &s->stmt_count_edge_owners_by_file, edge_sql, project,
                                       rel_path, "count_edge_owners_by_file", out_edge_owners);
}

int cbm_store_list_file_delta_inbound_source_paths(cbm_store_t *s, const char *project,
                                                   const char *rel_path, char ***out,
                                                   int *count) {
    if (out) {
        *out = NULL;
    }
    if (count) {
        *count = 0;
    }
    if (!s || !project || !rel_path || !out || !count) {
        if (s) {
            store_set_error(s, "list_file_delta_inbound_source_paths: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_list_file_delta_inbound_source_paths,
        "SELECT DISTINCT COALESCE(src_owner.rel_path, '') FROM edges e "
        "JOIN node_owners tgt_owner "
        "  ON tgt_owner.project = ?1 AND tgt_owner.rel_path = ?2 "
        " AND tgt_owner.node_id = e.target_id "
        "LEFT JOIN node_owners src_owner "
        "  ON src_owner.project = ?1 AND src_owner.node_id = e.source_id "
        "WHERE e.project = ?1 "
        "  AND (src_owner.rel_path IS NULL OR src_owner.rel_path != ?2) "
        "ORDER BY 1;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    return store_collect_text_column(s, stmt, "list_file_delta_inbound_source_paths", out, count);
}

void cbm_store_free_inbound_edges(cbm_store_inbound_edge_t *edges, int count) {
    if (!edges) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(edges[i].source_qn);
        free(edges[i].target_qn);
        free(edges[i].type);
        free(edges[i].source_rel_path);
        free(edges[i].target_rel_path);
        free(edges[i].edge_rel_path);
    }
    free(edges);
}

static int store_inbound_edges_grow(cbm_store_inbound_edge_t **items, int *cap) {
    if (!items || !cap) {
        return CBM_STORE_ERR;
    }
    if (*cap > INT_MAX / ST_GROWTH) {
        return CBM_STORE_ERR;
    }
    int new_cap = (*cap > 0) ? *cap * ST_GROWTH : ST_INIT_CAP_8;
    cbm_store_inbound_edge_t *next = realloc(*items, (size_t)new_cap * sizeof(*next));
    if (!next) {
        return CBM_STORE_ERR;
    }
    *items = next;
    *cap = new_cap;
    return CBM_STORE_OK;
}

int cbm_store_list_file_delta_inbound_edges(cbm_store_t *s, const char *project,
                                            const char *rel_path,
                                            cbm_store_inbound_edge_t **out, int *count) {
    if (out) {
        *out = NULL;
    }
    if (count) {
        *count = 0;
    }
    if (!s || !project || !rel_path || !out || !count) {
        if (s) {
            store_set_error(s, "list_file_delta_inbound_edges: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    const char *sql =
        "SELECT src.qualified_name, tgt.qualified_name, e.type, "
        "       COALESCE(src_owner.rel_path, ''), COALESCE(tgt_owner.rel_path, ''), "
        "       COALESCE(edge_owner.rel_path, '') "
        "FROM edges e "
        "JOIN node_owners tgt_owner "
        "  ON tgt_owner.project = ?1 AND tgt_owner.rel_path = ?2 "
        " AND tgt_owner.node_id = e.target_id "
        "JOIN nodes src ON src.project = e.project AND src.id = e.source_id "
        "JOIN nodes tgt ON tgt.project = e.project AND tgt.id = e.target_id "
        "LEFT JOIN node_owners src_owner "
        "  ON src_owner.project = ?1 AND src_owner.node_id = e.source_id "
        "LEFT JOIN edge_owners edge_owner "
        "  ON edge_owner.project = ?1 AND edge_owner.edge_id = e.id "
        "WHERE e.project = ?1 "
        "  AND (src_owner.rel_path IS NULL OR src_owner.rel_path != ?2) "
        "ORDER BY src.qualified_name, tgt.qualified_name, e.type;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "list_file_delta_inbound_edges prepare");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);

    int cap = 0;
    int n = 0;
    int partial = 0;
    cbm_store_inbound_edge_t *items = NULL;
    int rc = CBM_STORE_OK;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (n >= cap && store_inbound_edges_grow(&items, &cap) != CBM_STORE_OK) {
            rc = CBM_STORE_ERR;
            break;
        }
        cbm_store_inbound_edge_t *edge = &items[n];
        memset(edge, 0, sizeof(*edge));
        partial = 1;
        edge->source_qn = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        edge->target_qn = heap_strdup((const char *)sqlite3_column_text(stmt, 1));
        edge->type = heap_strdup((const char *)sqlite3_column_text(stmt, 2));
        edge->source_rel_path = heap_strdup((const char *)sqlite3_column_text(stmt, 3));
        edge->target_rel_path = heap_strdup((const char *)sqlite3_column_text(stmt, 4));
        edge->edge_rel_path = heap_strdup((const char *)sqlite3_column_text(stmt, 5));
        if (!edge->source_qn || !edge->target_qn || !edge->type || !edge->source_rel_path ||
            !edge->target_rel_path || !edge->edge_rel_path) {
            rc = CBM_STORE_ERR;
            break;
        }
        n++;
        partial = 0;
    }
    if (rc != SQLITE_DONE && rc != CBM_STORE_ERR) {
        store_set_error_sqlite(s, "list_file_delta_inbound_edges");
        rc = CBM_STORE_ERR;
    } else if (rc == SQLITE_DONE) {
        rc = CBM_STORE_OK;
    }
    sqlite3_finalize(stmt);
    if (rc != CBM_STORE_OK) {
        cbm_store_free_inbound_edges(items, n + partial);
        return rc;
    }
    *out = items;
    *count = n;
    return CBM_STORE_OK;
}

int cbm_store_upsert_symbol_export(cbm_store_t *s, const char *project,
                                   const char *qualified_name, const char *rel_path,
                                   int64_t node_id, int64_t generation) {
    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_upsert_symbol_export,
        "INSERT INTO symbol_exports (project, qualified_name, rel_path, node_id, generation) "
        "VALUES (?1, ?2, ?3, ?4, ?5) "
        "ON CONFLICT(project, qualified_name, rel_path) DO UPDATE SET "
        "node_id=?4, generation=?5;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, qualified_name);
    bind_text(stmt, ST_COL_3, rel_path);
    if (node_id > CBM_STORE_NO_NODE_ID) {
        sqlite3_bind_int64(stmt, ST_COL_4, node_id);
    } else {
        sqlite3_bind_null(stmt, ST_COL_4);
    }
    sqlite3_bind_int64(stmt, ST_COL_5, generation);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_symbol_export");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_symbol_exports_by_file(cbm_store_t *s, const char *project,
                                            const char *rel_path) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_delete_symbol_exports_by_file,
                       "DELETE FROM symbol_exports WHERE project = ?1 AND rel_path = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_symbol_exports_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_list_symbol_exports_by_file(cbm_store_t *s, const char *project,
                                          const char *rel_path, char ***out, int *count) {
    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_list_symbol_exports_by_file,
        "SELECT qualified_name FROM symbol_exports "
        "WHERE project = ?1 AND rel_path = ?2 ORDER BY qualified_name;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    return store_collect_text_column(s, stmt, "list_symbol_exports_by_file", out, count);
}

int cbm_store_upsert_import_ref(cbm_store_t *s, const char *project, const char *rel_path,
                                const char *import_text, const char *local_name,
                                const char *target_qn, int64_t generation) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_upsert_import_ref,
                       "INSERT INTO import_refs (project, rel_path, import_text, local_name, "
                       "target_qn, generation) VALUES (?1, ?2, ?3, ?4, ?5, ?6) "
                       "ON CONFLICT(project, rel_path, import_text, local_name) DO UPDATE SET "
                       "target_qn=?5, generation=?6;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    bind_text(stmt, ST_COL_3, import_text);
    bind_text(stmt, ST_COL_4, safe_str(local_name));
    bind_text(stmt, ST_COL_5, safe_str(target_qn));
    sqlite3_bind_int64(stmt, ST_COL_6, generation);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_import_ref");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_delete_import_refs_by_file(cbm_store_t *s, const char *project,
                                         const char *rel_path) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_delete_import_refs_by_file,
                       "DELETE FROM import_refs WHERE project = ?1 AND rel_path = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_import_refs_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

int cbm_store_list_import_ref_paths_by_target(cbm_store_t *s, const char *project,
                                              const char *target_qn, char ***out,
                                              int *count) {
    sqlite3_stmt *stmt =
        prepare_cached(s, &s->stmt_list_import_ref_paths_by_target,
                       "SELECT DISTINCT rel_path FROM import_refs "
                       "WHERE project = ?1 AND target_qn = ?2 ORDER BY rel_path;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, target_qn);
    return store_collect_text_column(s, stmt, "list_import_ref_paths_by_target", out, count);
}

int cbm_store_list_import_ref_paths_for_export_file(cbm_store_t *s, const char *project,
                                                   const char *export_rel_path, char ***out,
                                                   int *count) {
    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_list_import_ref_paths_for_export_file,
        "SELECT DISTINCT i.rel_path FROM import_refs i "
        "JOIN symbol_exports e ON e.project = i.project AND e.qualified_name = i.target_qn "
        "WHERE e.project = ?1 AND e.rel_path = ?2 AND i.rel_path != e.rel_path "
        "ORDER BY i.rel_path;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, export_rel_path);
    return store_collect_text_column(s, stmt, "list_import_ref_paths_for_export_file", out, count);
}

static int store_append_importers_for_target(cbm_store_t *s, const char *project,
                                             const char *target_qn, char ***items, int *count,
                                             int *cap) {
    char **importers = NULL;
    int importer_count = 0;
    int rc =
        cbm_store_list_import_ref_paths_by_target(s, project, target_qn, &importers, &importer_count);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    for (int i = 0; i < importer_count; i++) {
        rc = store_append_text(items, count, cap, importers[i]);
        if (rc != CBM_STORE_OK) {
            store_free_text_array(importers, importer_count);
            return rc;
        }
    }
    store_free_text_array(importers, importer_count);
    return CBM_STORE_OK;
}

int cbm_store_list_file_delta_affected_paths(cbm_store_t *s, const char *project,
                                             const char *rel_path,
                                             const char **new_export_qns, int new_export_count,
                                             char ***out, int *count) {
    if (!out || !count) {
        return CBM_STORE_ERR;
    }
    *out = NULL;
    *count = 0;
    if (!s || !project || !rel_path || new_export_count < 0 ||
        (new_export_count > 0 && !new_export_qns)) {
        return CBM_STORE_ERR;
    }

    int cap = ST_INIT_CAP_8;
    int n = 0;
    char **items = malloc((size_t)cap * sizeof(char *));
    if (!items) {
        return CBM_STORE_ERR;
    }

    int rc = store_append_text(&items, &n, &cap, rel_path);
    if (rc != CBM_STORE_OK) {
        store_free_text_array(items, n);
        return rc;
    }

    char **old_exports = NULL;
    int old_export_count = 0;
    rc = cbm_store_list_symbol_exports_by_file(s, project, rel_path, &old_exports, &old_export_count);
    if (rc != CBM_STORE_OK) {
        store_free_text_array(items, n);
        return rc;
    }
    for (int i = 0; i < old_export_count; i++) {
        rc = store_append_importers_for_target(s, project, old_exports[i], &items, &n, &cap);
        if (rc != CBM_STORE_OK) {
            store_free_text_array(old_exports, old_export_count);
            store_free_text_array(items, n);
            return rc;
        }
    }
    store_free_text_array(old_exports, old_export_count);

    for (int i = 0; i < new_export_count; i++) {
        if (!new_export_qns[i]) {
            store_free_text_array(items, n);
            return CBM_STORE_ERR;
        }
        rc = store_append_importers_for_target(s, project, new_export_qns[i], &items, &n, &cap);
        if (rc != CBM_STORE_OK) {
            store_free_text_array(items, n);
            return rc;
        }
    }

    store_sort_unique_text_array(items, &n);
    *out = items;
    *count = n;
    return CBM_STORE_OK;
}

static int store_delete_owned_edges_by_file(cbm_store_t *s, const char *project,
                                            const char *rel_path) {
    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_delete_owned_edges_by_file,
        "DELETE FROM edges WHERE id IN ("
        "  SELECT edge_id FROM edge_owners WHERE project = ?1 AND rel_path = ?2"
        ");");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_owned_edges_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int store_delete_owned_nodes_by_file(cbm_store_t *s, const char *project,
                                            const char *rel_path) {
    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_delete_owned_nodes_by_file,
        "DELETE FROM nodes WHERE id IN ("
        "  SELECT node_id FROM node_owners WHERE project = ?1 AND rel_path = ?2"
        ");");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "delete_owned_nodes_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int store_upsert_derived_view_state(cbm_store_t *s, const char *project,
                                           const char *view_name, int64_t generation,
                                           const char *status) {
    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_upsert_derived_view_state,
        "INSERT INTO derived_view_state (project, view_name, source_generation, computed_at, "
        "status) VALUES (?1, ?2, ?3, ?4, ?5) "
        "ON CONFLICT(project, view_name) DO UPDATE SET "
        "source_generation=?3, computed_at=?4, status=?5;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    char ts[CBM_SZ_64];
    iso_now(ts, sizeof(ts));
    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, view_name);
    sqlite3_bind_int64(stmt, ST_COL_3, generation);
    bind_text(stmt, ST_COL_4, ts);
    bind_text(stmt, ST_COL_5, status ? status : CBM_STORE_DERIVED_STATUS_COMPLETE);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        store_set_error_sqlite(s, "upsert_derived_view_state");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static bool store_derived_status_valid(const char *status) {
    return status && (strcmp(status, CBM_STORE_DERIVED_STATUS_STALE) == 0 ||
                      strcmp(status, CBM_STORE_DERIVED_STATUS_COMPLETE) == 0);
}

static const char *const store_graph_derived_view_names[] = {
    CBM_STORE_DERIVED_VIEW_PAGERANK,        CBM_STORE_DERIVED_VIEW_LINKRANK,
    CBM_STORE_DERIVED_VIEW_NODE_DEGREE,     CBM_STORE_DERIVED_VIEW_SEMANTIC_EDGES,
    CBM_STORE_DERIVED_VIEW_ROUTES,          CBM_STORE_DERIVED_VIEW_ARCHITECTURE,
};

static int store_graph_derived_view_count(void) {
    return (int)(sizeof(store_graph_derived_view_names) / sizeof(store_graph_derived_view_names[0]));
}

static int store_mark_derived_views_stale_body(cbm_store_t *s, const char *project,
                                               int64_t generation,
                                               const char *const *view_names, int view_count) {
    for (int i = 0; i < view_count; i++) {
        int rc = store_upsert_derived_view_state(s, project, view_names[i], generation,
                                                 CBM_STORE_DERIVED_STATUS_STALE);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    return CBM_STORE_OK;
}

static int store_mark_graph_derived_views_stale_body(cbm_store_t *s, const char *project,
                                                     int64_t generation) {
    return store_mark_derived_views_stale_body(s, project, generation,
                                               store_graph_derived_view_names,
                                               store_graph_derived_view_count());
}

int cbm_store_set_derived_view_state(cbm_store_t *s, const char *project,
                                     const char *view_name, int64_t generation,
                                     const char *status) {
    if (!s || !s->db || !project || !project[0] || !view_name || !view_name[0] ||
        generation < CBM_STORE_DERIVED_GENERATION_UNKNOWN ||
        !store_derived_status_valid(status)) {
        if (s) {
            store_set_error(s, "set_derived_view_state: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_upsert_derived_view_state(s, project, view_name, generation, status);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    rc = cbm_store_commit(s);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_store_mark_derived_views_stale(cbm_store_t *s, const char *project,
                                       int64_t generation, const char *const *view_names,
                                       int view_count) {
    if (!s || !s->db || !project || !project[0] ||
        generation < CBM_STORE_DERIVED_GENERATION_UNKNOWN || view_count < 0 ||
        (view_count > 0 && !view_names)) {
        if (s) {
            store_set_error(s, "mark_derived_views_stale: invalid argument");
        }
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < view_count; i++) {
        if (!view_names[i] || !view_names[i][0]) {
            store_set_error(s, "mark_derived_views_stale: invalid view name");
            return CBM_STORE_ERR;
        }
    }
    if (view_count == 0) {
        return CBM_STORE_OK;
    }

    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_mark_derived_views_stale_body(s, project, generation, view_names, view_count);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    rc = cbm_store_commit(s);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_store_get_derived_view_state(cbm_store_t *s, const char *project,
                                     const char *view_name,
                                     cbm_derived_view_state_t *out) {
    if (!s || !s->db || !project || !project[0] || !view_name || !view_name[0] || !out) {
        if (s) {
            store_set_error(s, "get_derived_view_state: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    memset(out, 0, sizeof(*out));
    sqlite3_stmt *stmt = prepare_cached(
        s, &s->stmt_get_derived_view_state,
        "SELECT project, view_name, source_generation, computed_at, status "
        "FROM derived_view_state WHERE project = ?1 AND view_name = ?2;");
    if (!stmt) {
        return CBM_STORE_ERR;
    }

    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, view_name);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        return CBM_STORE_NOT_FOUND;
    }

    out->project = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
    out->view_name = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_1));
    out->source_generation = sqlite3_column_int64(stmt, ST_COL_2);
    out->computed_at = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_3));
    out->status = heap_strdup((const char *)sqlite3_column_text(stmt, ST_COL_4));
    if (!out->project || !out->view_name || !out->computed_at || !out->status) {
        cbm_store_derived_view_state_free_fields(out);
        store_set_error(s, "get_derived_view_state: out of memory");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

bool cbm_store_derived_view_is_stale(cbm_store_t *s, const char *project,
                                     const char *view_name) {
    cbm_derived_view_state_t state = {0};
    int rc = cbm_store_get_derived_view_state(s, project, view_name, &state);
    if (rc != CBM_STORE_OK) {
        return false;
    }
    bool stale = state.status && strcmp(state.status, CBM_STORE_DERIVED_STATUS_STALE) == 0;
    cbm_store_derived_view_state_free_fields(&state);
    return stale;
}

int cbm_store_reserve_index_generation(cbm_store_t *s, const char *project,
                                       const char *repo_fingerprint,
                                       const char *config_fingerprint,
                                       int64_t *out_generation) {
    if (out_generation) {
        *out_generation = 0;
    }
    if (!s || !s->db || !project || !out_generation) {
        if (s) {
            store_set_error(s, "reserve_index_generation: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    int64_t generation = 0;
    sqlite3_stmt *stmt = NULL;
    const char *select_sql =
        "SELECT COALESCE(MAX(generation), 0) + 1 FROM index_generations WHERE project = ?1;";
    if (sqlite3_prepare_v2(s->db, select_sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "reserve_index_generation select prepare");
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        generation = sqlite3_column_int64(stmt, 0);
    } else {
        sqlite3_finalize(stmt);
        store_set_error_sqlite(s, "reserve_index_generation select");
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    stmt = NULL;

    const char *insert_sql =
        "INSERT INTO index_generations (project, generation, started_at, completed_at, "
        "repo_fingerprint, config_fingerprint, status) VALUES (?1, ?2, ?3, NULL, ?4, ?5, ?6);";
    if (sqlite3_prepare_v2(s->db, insert_sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "reserve_index_generation insert prepare");
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }

    char ts[CBM_SZ_64];
    iso_now(ts, sizeof(ts));
    bind_text(stmt, ST_COL_1, project);
    sqlite3_bind_int64(stmt, ST_COL_2, generation);
    bind_text(stmt, ST_COL_3, ts);
    bind_text(stmt, ST_COL_4, safe_str(repo_fingerprint));
    bind_text(stmt, ST_COL_5, safe_str(config_fingerprint));
    bind_text(stmt, ST_COL_6, CBM_STORE_INDEX_STATUS_RESERVED);
    rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        store_set_error_sqlite(s, "reserve_index_generation insert");
        (void)cbm_store_rollback(s);
        return CBM_STORE_ERR;
    }

    rc = cbm_store_commit(s);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }

    *out_generation = generation;
    return CBM_STORE_OK;
}

static bool store_index_finish_status_valid(const char *status) {
    return status && (strcmp(status, CBM_STORE_INDEX_STATUS_COMPLETE) == 0 ||
                      strcmp(status, CBM_STORE_INDEX_STATUS_FAILED) == 0);
}

static int store_finish_index_generation_body(cbm_store_t *s, const char *project,
                                              int64_t generation, const char *status) {
    const char *sql = "UPDATE index_generations SET completed_at = ?3, status = ?4 "
                      "WHERE project = ?1 AND generation = ?2 AND status = ?5;";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "finish_index_generation prepare");
        return CBM_STORE_ERR;
    }

    char ts[CBM_SZ_64];
    iso_now(ts, sizeof(ts));
    bind_text(stmt, ST_COL_1, project);
    sqlite3_bind_int64(stmt, ST_COL_2, generation);
    bind_text(stmt, ST_COL_3, ts);
    bind_text(stmt, ST_COL_4, status);
    bind_text(stmt, ST_COL_5, CBM_STORE_INDEX_STATUS_RESERVED);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (rc != SQLITE_DONE) {
        store_set_error_sqlite(s, "finish_index_generation");
        return CBM_STORE_ERR;
    }
    if (sqlite3_changes(s->db) != 1) {
        store_set_error(s, "finish_index_generation: reserved generation not found");
        return CBM_STORE_NOT_FOUND;
    }
    return CBM_STORE_OK;
}

int cbm_store_finish_index_generation(cbm_store_t *s, const char *project, int64_t generation,
                                      const char *status) {
    if (!s || !s->db || !project || generation <= 0 || !store_index_finish_status_valid(status)) {
        if (s) {
            store_set_error(s, "finish_index_generation: invalid argument");
        }
        return CBM_STORE_ERR;
    }

    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    rc = store_finish_index_generation_body(s, project, generation, status);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }

    rc = cbm_store_commit(s);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

static int store_resolve_node_id(cbm_store_t *s, const char *project, const char *qn,
                                 int64_t *out_id) {
    const char *qns[1] = {qn};
    int64_t ids[1] = {CBM_STORE_NO_NODE_ID};
    if (!qn || cbm_store_find_node_ids_by_qns(s, project, qns, 1, ids) != 1) {
        if (out_id) {
            *out_id = CBM_STORE_NO_NODE_ID;
        }
        return CBM_STORE_NOT_FOUND;
    }
    if (out_id) {
        *out_id = ids[0];
    }
    return CBM_STORE_OK;
}

static bool store_nodes_fts_unavailable(cbm_store_t *s) {
    const char *msg = (s && s->db) ? sqlite3_errmsg(s->db) : NULL;
    return msg && strstr(msg, "no such table: nodes_fts") != NULL;
}

static int store_nodes_fts_delete_by_file(cbm_store_t *s, const char *project,
                                          const char *rel_path) {
    static const char sql[] =
        "INSERT INTO nodes_fts(nodes_fts, rowid, name, qualified_name, label, file_path) "
        "SELECT 'delete', id, cbm_camel_split(name), qualified_name, label, file_path "
        "FROM nodes "
        "WHERE project = ?1 AND file_path = ?2 "
        "  AND EXISTS (SELECT 1 FROM nodes_fts WHERE rowid = nodes.id);";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        if (store_nodes_fts_unavailable(s)) {
            return CBM_STORE_OK;
        }
        store_set_error_sqlite(s, "nodes_fts_delete_by_file");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    int step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (step != SQLITE_DONE) {
        store_set_error_sqlite(s, "nodes_fts_delete_by_file");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int store_nodes_fts_delete_orphan_folders(cbm_store_t *s, const char *project) {
    static const char sql[] =
        "INSERT INTO nodes_fts(nodes_fts, rowid, name, qualified_name, label, file_path) "
        "SELECT 'delete', n.id, cbm_camel_split(n.name), n.qualified_name, n.label, n.file_path "
        "FROM nodes n "
        "WHERE n.project = ?1 AND n.label = 'Folder' "
        "  AND EXISTS (SELECT 1 FROM nodes_fts WHERE rowid = n.id) "
        "  AND NOT EXISTS (SELECT 1 FROM edges e "
        "                  WHERE e.project = n.project AND e.source_id = n.id "
        "                    AND e.type = 'CONTAINS_FILE') "
        "  AND NOT EXISTS (SELECT 1 FROM edges e "
        "                  WHERE e.project = n.project AND e.source_id = n.id "
        "                    AND e.type = 'CONTAINS_FOLDER');";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        if (store_nodes_fts_unavailable(s)) {
            return CBM_STORE_OK;
        }
        store_set_error_sqlite(s, "nodes_fts_delete_orphan_folders");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    int step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (step != SQLITE_DONE) {
        store_set_error_sqlite(s, "nodes_fts_delete_orphan_folders");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int store_nodes_fts_insert_node(cbm_store_t *s, int64_t node_id, const cbm_node_t *node) {
    static const char sql[] =
        "INSERT INTO nodes_fts(rowid, name, qualified_name, label, file_path) "
        "VALUES(?1, cbm_camel_split(?2), ?3, ?4, ?5);";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        if (store_nodes_fts_unavailable(s)) {
            return CBM_STORE_OK;
        }
        store_set_error_sqlite(s, "nodes_fts_insert_node");
        return CBM_STORE_ERR;
    }
    sqlite3_bind_int64(stmt, ST_COL_1, node_id);
    bind_text(stmt, ST_COL_2, node->name ? node->name : "");
    bind_text(stmt, ST_COL_3, node->qualified_name ? node->qualified_name : "");
    bind_text(stmt, ST_COL_4, node->label ? node->label : "");
    bind_text(stmt, ST_COL_5, node->file_path ? node->file_path : "");
    int step = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    if (step != SQLITE_DONE) {
        store_set_error_sqlite(s, "nodes_fts_insert_node");
        return CBM_STORE_ERR;
    }
    return CBM_STORE_OK;
}

static int store_prune_orphan_folders_body(cbm_store_t *s, const char *project) {
    static const char orphan_edges_sql[] =
        "DELETE FROM edges WHERE project = ?1 AND type = 'CONTAINS_FOLDER' "
        "AND target_id IN ("
        "  SELECT n.id FROM nodes n "
        "  WHERE n.project = ?1 AND n.label = 'Folder' "
        "    AND NOT EXISTS (SELECT 1 FROM edges e "
        "                    WHERE e.project = n.project AND e.source_id = n.id "
        "                      AND e.type = 'CONTAINS_FILE') "
        "    AND NOT EXISTS (SELECT 1 FROM edges e "
        "                    WHERE e.project = n.project AND e.source_id = n.id "
        "                      AND e.type = 'CONTAINS_FOLDER')"
        ");";
    static const char orphan_nodes_sql[] =
        "DELETE FROM nodes WHERE project = ?1 AND label = 'Folder' "
        "AND NOT EXISTS (SELECT 1 FROM edges e "
        "                WHERE e.project = nodes.project AND e.source_id = nodes.id "
        "                  AND e.type = 'CONTAINS_FILE') "
        "AND NOT EXISTS (SELECT 1 FROM edges e "
        "                WHERE e.project = nodes.project AND e.source_id = nodes.id "
        "                  AND e.type = 'CONTAINS_FOLDER');";

    for (;;) {
        int rc = store_nodes_fts_delete_orphan_folders(s, project);
        if (rc != CBM_STORE_OK) {
            return rc;
        }

        sqlite3_stmt *edge_stmt = NULL;
        if (sqlite3_prepare_v2(s->db, orphan_edges_sql, CBM_NOT_FOUND, &edge_stmt, NULL) !=
            SQLITE_OK) {
            store_set_error_sqlite(s, "prune_orphan_folders edges prepare");
            sqlite3_finalize(edge_stmt);
            return CBM_STORE_ERR;
        }
        bind_text(edge_stmt, ST_COL_1, project);
        if (sqlite3_step(edge_stmt) != SQLITE_DONE) {
            store_set_error_sqlite(s, "prune_orphan_folders edges");
            sqlite3_finalize(edge_stmt);
            return CBM_STORE_ERR;
        }
        sqlite3_finalize(edge_stmt);

        sqlite3_stmt *node_stmt = NULL;
        if (sqlite3_prepare_v2(s->db, orphan_nodes_sql, CBM_NOT_FOUND, &node_stmt, NULL) !=
            SQLITE_OK) {
            store_set_error_sqlite(s, "prune_orphan_folders nodes prepare");
            sqlite3_finalize(node_stmt);
            return CBM_STORE_ERR;
        }
        bind_text(node_stmt, ST_COL_1, project);
        if (sqlite3_step(node_stmt) != SQLITE_DONE) {
            store_set_error_sqlite(s, "prune_orphan_folders nodes");
            sqlite3_finalize(node_stmt);
            return CBM_STORE_ERR;
        }
        int removed = sqlite3_changes(s->db);
        sqlite3_finalize(node_stmt);
        if (removed == 0) {
            return CBM_STORE_OK;
        }
    }
}

static int store_delete_file_delta_body(cbm_store_t *s, const char *project, const char *rel_path,
                                        int64_t generation, const char *derived_view_name) {
    int rc = store_delete_owned_edges_by_file(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_nodes_fts_delete_by_file(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_delete_owned_nodes_by_file(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = cbm_store_delete_edge_owners_by_file(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = cbm_store_delete_node_owners_by_file(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = cbm_store_delete_symbol_exports_by_file(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = cbm_store_delete_import_refs_by_file(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = cbm_store_delete_file_hash(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = cbm_store_delete_file_state(s, project, rel_path);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_prune_orphan_folders_body(s, project);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    if (derived_view_name) {
        rc = store_upsert_derived_view_state(s, project, derived_view_name, generation,
                                             CBM_STORE_DERIVED_STATUS_STALE);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    return CBM_STORE_OK;
}

static int store_delete_file_delta_transaction(cbm_store_t *s, const char *project,
                                               const char *rel_path, int64_t generation,
                                               const char *derived_view_name,
                                               bool finish_generation) {
    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_delete_file_delta_body(s, project, rel_path, generation, derived_view_name);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    rc = store_mark_graph_derived_views_stale_body(s, project, generation);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    if (finish_generation) {
        rc = store_finish_index_generation_body(s, project, generation,
                                                CBM_STORE_INDEX_STATUS_COMPLETE);
        if (rc != CBM_STORE_OK) {
            (void)cbm_store_rollback(s);
            return rc;
        }
    }
    rc = cbm_store_commit(s);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

static int store_publish_file_delta_delete_body(cbm_store_t *s,
                                                const cbm_store_file_delta_t *delta) {
    CBM_PROF_START(t_edges);
    int rc = store_delete_owned_edges_by_file(s, delta->project, delta->rel_path);
    CBM_PROF_END("store_delta_delete", "1_edges", t_edges);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_fts);
    rc = store_nodes_fts_delete_by_file(s, delta->project, delta->rel_path);
    CBM_PROF_END("store_delta_delete", "2_fts", t_fts);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_nodes);
    rc = store_delete_owned_nodes_by_file(s, delta->project, delta->rel_path);
    CBM_PROF_END("store_delta_delete", "3_nodes", t_nodes);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_edge_owners);
    rc = cbm_store_delete_edge_owners_by_file(s, delta->project, delta->rel_path);
    CBM_PROF_END("store_delta_delete", "4_edge_owners", t_edge_owners);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_node_owners);
    rc = cbm_store_delete_node_owners_by_file(s, delta->project, delta->rel_path);
    CBM_PROF_END("store_delta_delete", "5_node_owners", t_node_owners);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_exports);
    rc = cbm_store_delete_symbol_exports_by_file(s, delta->project, delta->rel_path);
    CBM_PROF_END("store_delta_delete", "6_exports", t_exports);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_imports);
    rc = cbm_store_delete_import_refs_by_file(s, delta->project, delta->rel_path);
    CBM_PROF_END("store_delta_delete", "7_imports", t_imports);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    return CBM_STORE_OK;
}

static int store_publish_file_delta_nodes_body(cbm_store_t *s,
                                               const cbm_store_file_delta_t *delta) {
    int rc = CBM_STORE_OK;
    for (int i = 0; i < delta->node_count; i++) {
        int64_t id = cbm_store_upsert_node(s, &delta->nodes[i]);
        if (id <= CBM_STORE_NO_NODE_ID) {
            return CBM_STORE_ERR;
        }
        rc = cbm_store_upsert_node_owner(s, delta->project, id, delta->rel_path, delta->generation);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = store_nodes_fts_insert_node(s, id, &delta->nodes[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    return CBM_STORE_OK;
}

static int store_publish_file_delta_context_nodes_body(cbm_store_t *s,
                                                       const cbm_store_file_delta_t *delta) {
    int rc = CBM_STORE_OK;
    for (int i = 0; i < delta->context_node_count; i++) {
        int64_t id = cbm_store_upsert_node(s, &delta->context_nodes[i]);
        if (id <= CBM_STORE_NO_NODE_ID) {
            return CBM_STORE_ERR;
        }
        rc = store_nodes_fts_insert_node(s, id, &delta->context_nodes[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    return CBM_STORE_OK;
}

static int store_publish_delta_edges(cbm_store_t *s, const cbm_store_file_delta_t *delta,
                                     const cbm_store_delta_edge_t *edges, int edge_count,
                                     bool own_edges) {
    int rc = CBM_STORE_OK;
    if (edge_count <= 0) {
        return CBM_STORE_OK;
    }

    size_t endpoint_count = (size_t)edge_count * PAIR_LEN;
    if (endpoint_count > (size_t)INT_MAX ||
        endpoint_count > SIZE_MAX / sizeof(const char *) ||
        endpoint_count > SIZE_MAX / sizeof(int64_t)) {
        return CBM_STORE_ERR;
    }
    const char **endpoint_qns = malloc(endpoint_count * sizeof(*endpoint_qns));
    int64_t *endpoint_ids = malloc(endpoint_count * sizeof(*endpoint_ids));
    if (!endpoint_qns || !endpoint_ids) {
        free(endpoint_qns);
        free(endpoint_ids);
        return CBM_STORE_ERR;
    }

    for (int i = 0; i < edge_count; i++) {
        endpoint_qns[(size_t)i * PAIR_LEN] = edges[i].source_qn;
        endpoint_qns[(size_t)i * PAIR_LEN + SKIP_ONE] = edges[i].target_qn;
    }
    int found = cbm_store_find_node_ids_by_qns(s, delta->project, endpoint_qns, (int)endpoint_count,
                                               endpoint_ids);
    if (found < (int)endpoint_count) {
        free(endpoint_qns);
        free(endpoint_ids);
        return found < 0 ? CBM_STORE_ERR : CBM_STORE_NOT_FOUND;
    }

    for (int i = 0; i < edge_count; i++) {
        int64_t source_id = endpoint_ids[(size_t)i * PAIR_LEN];
        int64_t target_id = endpoint_ids[(size_t)i * PAIR_LEN + SKIP_ONE];
        if (source_id <= CBM_STORE_NO_NODE_ID || target_id <= CBM_STORE_NO_NODE_ID) {
            free(endpoint_qns);
            free(endpoint_ids);
            return CBM_STORE_NOT_FOUND;
        }
        cbm_edge_t edge = {.project = delta->project,
                           .source_id = source_id,
                           .target_id = target_id,
                           .type = edges[i].type,
                           .properties_json = edges[i].properties_json};
        int64_t edge_id = cbm_store_insert_edge(s, &edge);
        if (edge_id <= CBM_STORE_NO_NODE_ID) {
            free(endpoint_qns);
            free(endpoint_ids);
            return CBM_STORE_ERR;
        }
        if (own_edges) {
            rc = cbm_store_upsert_edge_owner(s, delta->project, edge_id, delta->rel_path,
                                             edges[i].derived_kind, delta->generation);
            if (rc != CBM_STORE_OK) {
                free(endpoint_qns);
                free(endpoint_ids);
                return rc;
            }
        }
    }
    free(endpoint_qns);
    free(endpoint_ids);
    return CBM_STORE_OK;
}

static int store_publish_file_delta_edges_body(cbm_store_t *s,
                                               const cbm_store_file_delta_t *delta) {
    return store_publish_delta_edges(s, delta, delta->edges, delta->edge_count, true);
}

static int store_publish_file_delta_context_edges_body(cbm_store_t *s,
                                                       const cbm_store_file_delta_t *delta) {
    return store_publish_delta_edges(s, delta, delta->context_edges, delta->context_edge_count,
                                     false);
}

static int store_publish_file_delta_metadata_body(cbm_store_t *s,
                                                  const cbm_store_file_delta_t *delta) {
    int rc = CBM_STORE_OK;
    for (int i = 0; i < delta->export_count; i++) {
        int64_t node_id = delta->exports[i].node_id;
        if (node_id <= CBM_STORE_NO_NODE_ID &&
            store_resolve_node_id(s, delta->project, delta->exports[i].qualified_name, &node_id) !=
                CBM_STORE_OK) {
            node_id = CBM_STORE_NO_NODE_ID;
        }
        rc = cbm_store_upsert_symbol_export(s, delta->project, delta->exports[i].qualified_name,
                                            delta->rel_path, node_id, delta->generation);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }

    for (int i = 0; i < delta->import_count; i++) {
        rc = cbm_store_upsert_import_ref(s, delta->project, delta->rel_path,
                                         delta->imports[i].import_text, delta->imports[i].local_name,
                                         delta->imports[i].target_qn, delta->generation);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }

    if (delta->file_hash) {
        rc = cbm_store_upsert_file_hash(s, delta->project, delta->rel_path, delta->file_hash->sha256,
                                        delta->file_hash->mtime_ns, delta->file_hash->size);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (delta->file_state) {
        rc = cbm_store_upsert_file_state(s, delta->file_state);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    if (delta->derived_view_name) {
        rc = store_upsert_derived_view_state(s, delta->project, delta->derived_view_name,
                                             delta->generation, delta->derived_status);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    return CBM_STORE_OK;
}

static bool store_file_delta_shape_valid(const cbm_store_file_delta_t *delta);

static int store_count_rows_i(cbm_store_t *s, const char *sql, const char *project,
                              const char *rel_path, int *out) {
    if (!s || !sql || !project || !rel_path || !out) {
        return CBM_STORE_ERR;
    }
    *out = 0;
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "count_rows_i prepare");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    bind_text(stmt, ST_COL_2, rel_path);
    int step = sqlite3_step(stmt);
    if (step == SQLITE_ROW) {
        *out = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
        return CBM_STORE_OK;
    }
    sqlite3_finalize(stmt);
    store_set_error_sqlite(s, "count_rows_i");
    return CBM_STORE_ERR;
}

static int store_delta_row_exists(sqlite3_stmt *stmt, bool *out_exists) {
    if (!stmt || !out_exists) {
        return CBM_STORE_ERR;
    }
    *out_exists = false;
    int step = sqlite3_step(stmt);
    if (step == SQLITE_ROW) {
        *out_exists = true;
        sqlite3_finalize(stmt);
        return CBM_STORE_OK;
    }
    sqlite3_finalize(stmt);
    if (step == SQLITE_DONE) {
        return CBM_STORE_OK;
    }
    return CBM_STORE_ERR;
}

static void store_delta_graph_equal_debug(const cbm_store_file_delta_t *delta, const char *reason,
                                          int expected, int actual) {
    char env[ST_BUF_16];
    if (!delta ||
        cbm_safe_getenv("CBM_DEBUG_DELTA_GRAPH_EQUAL", env, sizeof(env), NULL) == NULL ||
        env[0] == '\0' || env[0] == '0') {
        return;
    }
    char expected_buf[ST_BUF_16];
    char actual_buf[ST_BUF_16];
    if (snprintf(expected_buf, sizeof(expected_buf), "%d", expected) < 0 ||
        snprintf(actual_buf, sizeof(actual_buf), "%d", actual) < 0) {
        return;
    }
    cbm_log_debug("delta.graph_equal.mismatch", "reason", reason ? reason : "unknown",
                  "project", delta->project, "rel_path", delta->rel_path, "expected",
                  expected_buf, "actual", actual_buf);
}

static int store_delta_node_exists(cbm_store_t *s, const cbm_store_file_delta_t *delta,
                                   const cbm_node_t *node, bool require_owner,
                                   bool *out_exists) {
    static const char owned_sql[] =
        "SELECT 1 FROM nodes n "
        "JOIN node_owners o ON o.project = n.project AND o.node_id = n.id "
        "WHERE n.project = ?1 AND o.rel_path = ?2 AND n.label = ?3 AND n.name = ?4 "
        "  AND n.qualified_name = ?5 AND COALESCE(n.file_path, '') = ?6 "
        "  AND n.start_line = ?7 AND n.end_line = ?8 "
        "  AND COALESCE(n.properties, '{}') = ?9 LIMIT 1;";
    static const char context_sql[] =
        "SELECT 1 FROM nodes n "
        "WHERE n.project = ?1 AND n.label = ?3 AND n.name = ?4 "
        "  AND n.qualified_name = ?5 AND COALESCE(n.file_path, '') = ?6 "
        "  AND n.start_line = ?7 AND n.end_line = ?8 "
        "  AND COALESCE(n.properties, '{}') = ?9 LIMIT 1;";
    sqlite3_stmt *stmt = NULL;
    const char *sql = require_owner ? owned_sql : context_sql;
    if (!s || !delta || !node || !out_exists ||
        sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        if (s) {
            store_set_error_sqlite(s, "delta_node_exists prepare");
        }
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, delta->project);
    bind_text(stmt, ST_COL_2, delta->rel_path);
    bind_text(stmt, ST_COL_3, node->label);
    bind_text(stmt, ST_COL_4, node->name ? node->name : "");
    bind_text(stmt, ST_COL_5, node->qualified_name);
    bind_text(stmt, ST_COL_6, node->file_path ? node->file_path : "");
    sqlite3_bind_int(stmt, ST_COL_7, node->start_line);
    sqlite3_bind_int(stmt, ST_COL_8, node->end_line);
    bind_text(stmt, ST_COL_9, node->properties_json ? node->properties_json : "{}");
    return store_delta_row_exists(stmt, out_exists);
}

static int store_delta_edge_exists(cbm_store_t *s, const cbm_store_file_delta_t *delta,
                                   const cbm_store_delta_edge_t *edge, bool require_owner,
                                   bool *out_exists) {
    static const char owned_sql[] =
        "SELECT 1 FROM edges e "
        "JOIN edge_owners o ON o.project = e.project AND o.edge_id = e.id "
        "JOIN nodes src ON src.project = e.project AND src.id = e.source_id "
        "JOIN nodes tgt ON tgt.project = e.project AND tgt.id = e.target_id "
        "WHERE e.project = ?1 AND o.rel_path = ?2 AND src.qualified_name = ?3 "
        "  AND tgt.qualified_name = ?4 AND e.type = ?5 "
        "  AND COALESCE(e.properties, '{}') = ?6 LIMIT 1;";
    static const char context_sql[] =
        "SELECT 1 FROM edges e "
        "JOIN nodes src ON src.project = e.project AND src.id = e.source_id "
        "JOIN nodes tgt ON tgt.project = e.project AND tgt.id = e.target_id "
        "WHERE e.project = ?1 AND src.qualified_name = ?3 "
        "  AND tgt.qualified_name = ?4 AND e.type = ?5 "
        "  AND COALESCE(e.properties, '{}') = ?6 LIMIT 1;";
    sqlite3_stmt *stmt = NULL;
    const char *sql = require_owner ? owned_sql : context_sql;
    if (!s || !delta || !edge || !out_exists ||
        sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        if (s) {
            store_set_error_sqlite(s, "delta_edge_exists prepare");
        }
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, delta->project);
    bind_text(stmt, ST_COL_2, delta->rel_path);
    bind_text(stmt, ST_COL_3, edge->source_qn);
    bind_text(stmt, ST_COL_4, edge->target_qn);
    bind_text(stmt, ST_COL_5, edge->type);
    bind_text(stmt, ST_COL_6, edge->properties_json ? edge->properties_json : "{}");
    return store_delta_row_exists(stmt, out_exists);
}

static int store_file_delta_graph_equal_one(cbm_store_t *s, const cbm_store_file_delta_t *delta,
                                            bool *out_equal) {
    static const char node_count_sql[] =
        "SELECT COUNT(*) FROM node_owners WHERE project = ?1 AND rel_path = ?2;";
    static const char edge_count_sql[] =
        "SELECT COUNT(*) FROM edge_owners WHERE project = ?1 AND rel_path = ?2;";
    if (!out_equal) {
        return CBM_STORE_ERR;
    }
    *out_equal = false;
    if (!s || !store_file_delta_shape_valid(delta)) {
        return CBM_STORE_ERR;
    }

    int count = 0;
    int rc = store_count_rows_i(s, node_count_sql, delta->project, delta->rel_path, &count);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    if (count != delta->node_count) {
        store_delta_graph_equal_debug(delta, "node_owner_count", delta->node_count, count);
        return CBM_STORE_OK;
    }
    rc = store_count_rows_i(s, edge_count_sql, delta->project, delta->rel_path, &count);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    if (count != delta->edge_count) {
        store_delta_graph_equal_debug(delta, "edge_owner_count", delta->edge_count, count);
        return CBM_STORE_OK;
    }

    /* symbol_exports/import_refs are incremental metadata, not canonical graph rows.
     * The no-op refresh path replaces them after graph equality is proven. */
    bool exists = false;
    for (int i = 0; i < delta->node_count; i++) {
        int rc = store_delta_node_exists(s, delta, &delta->nodes[i], true, &exists);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        if (!exists) {
            store_delta_graph_equal_debug(delta, "node_missing", i, 0);
            return CBM_STORE_OK;
        }
    }
    for (int i = 0; i < delta->context_node_count; i++) {
        int rc = store_delta_node_exists(s, delta, &delta->context_nodes[i], false, &exists);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        if (!exists) {
            store_delta_graph_equal_debug(delta, "context_node_missing", i, 0);
            return CBM_STORE_OK;
        }
    }
    for (int i = 0; i < delta->edge_count; i++) {
        int rc = store_delta_edge_exists(s, delta, &delta->edges[i], true, &exists);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        if (!exists) {
            store_delta_graph_equal_debug(delta, "edge_missing", i, 0);
            return CBM_STORE_OK;
        }
    }
    for (int i = 0; i < delta->context_edge_count; i++) {
        int rc = store_delta_edge_exists(s, delta, &delta->context_edges[i], false, &exists);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        if (!exists) {
            store_delta_graph_equal_debug(delta, "context_edge_missing", i, 0);
            return CBM_STORE_OK;
        }
    }
    *out_equal = true;
    return CBM_STORE_OK;
}

static int store_publish_file_delta_body(cbm_store_t *s, const cbm_store_file_delta_t *delta) {
    int rc = store_publish_file_delta_delete_body(s, delta);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_publish_file_delta_context_nodes_body(s, delta);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_publish_file_delta_nodes_body(s, delta);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_publish_file_delta_context_edges_body(s, delta);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_publish_file_delta_edges_body(s, delta);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    return store_publish_file_delta_metadata_body(s, delta);
}

static int store_publish_file_delta_batch_body(cbm_store_t *s,
                                               const cbm_store_file_delta_t *const *deltas,
                                               int delta_count) {
    int rc = CBM_STORE_OK;
    CBM_PROF_START(t_delete);
    for (int i = 0; i < delta_count; i++) {
        rc = store_publish_file_delta_delete_body(s, deltas[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    CBM_PROF_END_N("store_delta_publish", "1_delete_old_rows", t_delete, delta_count);
    CBM_PROF_START(t_nodes);
    for (int i = 0; i < delta_count; i++) {
        rc = store_publish_file_delta_context_nodes_body(s, deltas[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = store_publish_file_delta_nodes_body(s, deltas[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    CBM_PROF_END_N("store_delta_publish", "2_upsert_nodes", t_nodes, delta_count);
    CBM_PROF_START(t_edges);
    for (int i = 0; i < delta_count; i++) {
        rc = store_publish_file_delta_context_edges_body(s, deltas[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = store_publish_file_delta_edges_body(s, deltas[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    CBM_PROF_END_N("store_delta_publish", "3_upsert_edges", t_edges, delta_count);
    CBM_PROF_START(t_metadata);
    for (int i = 0; i < delta_count; i++) {
        rc = store_publish_file_delta_metadata_body(s, deltas[i]);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }
    CBM_PROF_END_N("store_delta_publish", "4_upsert_metadata", t_metadata, delta_count);
    return CBM_STORE_OK;
}

int cbm_store_delete_file_delta(cbm_store_t *s, const char *project, const char *rel_path,
                                int64_t generation, const char *derived_view_name) {
    if (!s || !project || !project[0] || !rel_path || !rel_path[0] ||
        (derived_view_name && (!derived_view_name[0] || generation <= 0))) {
        if (s) {
            store_set_error(s, "delete_file_delta: invalid argument");
        }
        return CBM_STORE_ERR;
    }
    return store_delete_file_delta_transaction(s, project, rel_path, generation,
                                               derived_view_name, false);
}

int cbm_store_delete_file_delta_complete(cbm_store_t *s, const char *project,
                                         const char *rel_path, int64_t generation,
                                         const char *derived_view_name) {
    if (!s || !project || !project[0] || !rel_path || !rel_path[0] || generation <= 0 ||
        (derived_view_name && !derived_view_name[0])) {
        if (s) {
            store_set_error(s, "delete_file_delta_complete: invalid argument");
        }
        return CBM_STORE_ERR;
    }
    return store_delete_file_delta_transaction(s, project, rel_path, generation,
                                               derived_view_name, true);
}

static bool store_delta_field_matches(const char *actual, const char *expected) {
    return actual && expected && strcmp(actual, expected) == 0;
}

static bool store_delta_context_node_valid(const cbm_store_file_delta_t *delta,
                                           const cbm_node_t *node) {
    return node && store_delta_field_matches(node->project, delta->project) &&
           node->label && strcmp(node->label, "Folder") == 0 && node->qualified_name &&
           node->file_path && node->file_path[0] != '\0';
}

static bool store_delta_context_edge_valid(const cbm_store_delta_edge_t *edge) {
    return edge && edge->source_qn && edge->target_qn && edge->type &&
           strcmp(edge->type, "CONTAINS_FOLDER") == 0;
}

static bool store_file_delta_contract_valid(const cbm_store_file_delta_t *delta) {
    if (delta->file_hash &&
        (!store_delta_field_matches(delta->file_hash->project, delta->project) ||
         !store_delta_field_matches(delta->file_hash->rel_path, delta->rel_path) ||
         !delta->file_hash->sha256)) {
        return false;
    }
    if (delta->file_state &&
        (!store_delta_field_matches(delta->file_state->project, delta->project) ||
         !store_delta_field_matches(delta->file_state->rel_path, delta->rel_path) ||
         !delta->file_state->content_hash || !delta->file_state->indexed_at)) {
        return false;
    }
    for (int i = 0; i < delta->node_count; i++) {
        if (!store_delta_field_matches(delta->nodes[i].project, delta->project) ||
            !store_delta_field_matches(delta->nodes[i].file_path, delta->rel_path) ||
            !delta->nodes[i].qualified_name) {
            return false;
        }
    }
    for (int i = 0; i < delta->context_node_count; i++) {
        if (!store_delta_context_node_valid(delta, &delta->context_nodes[i])) {
            return false;
        }
    }
    for (int i = 0; i < delta->context_edge_count; i++) {
        if (!store_delta_context_edge_valid(&delta->context_edges[i])) {
            return false;
        }
    }
    for (int i = 0; i < delta->edge_count; i++) {
        if (!delta->edges[i].source_qn || !delta->edges[i].target_qn || !delta->edges[i].type) {
            return false;
        }
    }
    for (int i = 0; i < delta->export_count; i++) {
        if (!delta->exports[i].qualified_name) {
            return false;
        }
    }
    for (int i = 0; i < delta->import_count; i++) {
        if (!delta->imports[i].import_text) {
            return false;
        }
    }
    return true;
}

static bool store_file_delta_shape_valid(const cbm_store_file_delta_t *delta) {
    if (!delta || !delta->project || !delta->rel_path || delta->generation < 0 ||
        delta->context_node_count < 0 || delta->context_edge_count < 0 ||
        delta->node_count < 0 || delta->edge_count < 0 || delta->export_count < 0 ||
        delta->import_count < 0) {
        return false;
    }
    if ((delta->context_node_count > 0 && !delta->context_nodes) ||
        (delta->context_edge_count > 0 && !delta->context_edges) ||
        (delta->node_count > 0 && !delta->nodes) || (delta->edge_count > 0 && !delta->edges) ||
        (delta->export_count > 0 && !delta->exports) ||
        (delta->import_count > 0 && !delta->imports)) {
        return false;
    }
    return store_file_delta_contract_valid(delta);
}

static bool store_file_delta_batch_shape_valid(const cbm_store_file_delta_t *const *deltas,
                                               int delta_count, const char **out_project,
                                               int64_t *out_generation) {
    if (!deltas || delta_count <= 0) {
        return false;
    }
    const char *project = NULL;
    int64_t generation = -1;
    for (int i = 0; i < delta_count; i++) {
        const cbm_store_file_delta_t *delta = deltas[i];
        if (!store_file_delta_shape_valid(delta)) {
            return false;
        }
        if (!project) {
            project = delta->project;
            generation = delta->generation;
        } else if (strcmp(project, delta->project) != 0 || generation != delta->generation) {
            return false;
        }
    }
    if (out_project) {
        *out_project = project;
    }
    if (out_generation) {
        *out_generation = generation;
    }
    return true;
}

static bool store_file_delta_apply_batch_shape_valid(
    const cbm_store_file_delta_t *const *delete_deltas, int delete_count,
    const cbm_store_file_delta_t *const *upsert_deltas, int upsert_count,
    const char **out_project, int64_t *out_generation) {
    if (delete_count < 0 || upsert_count < 0 || delete_count + upsert_count <= 0) {
        return false;
    }
    const char *project = NULL;
    int64_t generation = -1;
    const cbm_store_file_delta_t *const *groups[] = {delete_deltas, upsert_deltas};
    const int counts[] = {delete_count, upsert_count};
    enum { APPLY_GROUP_COUNT = (int)(sizeof(counts) / sizeof(counts[0])) };
    for (int group = 0; group < APPLY_GROUP_COUNT; group++) {
        if (counts[group] > 0 && !groups[group]) {
            return false;
        }
        for (int i = 0; i < counts[group]; i++) {
            const cbm_store_file_delta_t *delta = groups[group][i];
            if (!store_file_delta_shape_valid(delta) || delta->generation <= 0) {
                return false;
            }
            if (!project) {
                project = delta->project;
                generation = delta->generation;
            } else if (strcmp(project, delta->project) != 0 || generation != delta->generation) {
                return false;
            }
        }
    }
    if (out_project) {
        *out_project = project;
    }
    if (out_generation) {
        *out_generation = generation;
    }
    return project && generation > 0;
}

int cbm_store_file_delta_batch_graph_equal(cbm_store_t *s,
                                           const cbm_store_file_delta_t *const *deltas,
                                           int delta_count, bool *out_equal) {
    if (out_equal) {
        *out_equal = false;
    }
    if (!s || !out_equal || !store_file_delta_batch_shape_valid(deltas, delta_count, NULL, NULL)) {
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < delta_count; i++) {
        bool equal = false;
        int rc = store_file_delta_graph_equal_one(s, deltas[i], &equal);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        if (!equal) {
            return CBM_STORE_OK;
        }
    }
    *out_equal = true;
    return CBM_STORE_OK;
}

int cbm_store_refresh_file_delta_metadata_batch_complete(
    cbm_store_t *s, const cbm_store_file_delta_t *const *deltas, int delta_count) {
    const char *project = NULL;
    int64_t generation = -1;
    if (!s || !store_file_delta_batch_shape_valid(deltas, delta_count, &project, &generation) ||
        generation <= 0) {
        return CBM_STORE_ERR;
    }

    CBM_PROF_START(t_begin);
    int rc = cbm_store_begin(s);
    CBM_PROF_END("store_delta_noop", "0_begin", t_begin);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_metadata);
    for (int i = 0; i < delta_count; i++) {
        rc = cbm_store_delete_symbol_exports_by_file(s, deltas[i]->project, deltas[i]->rel_path);
        if (rc != CBM_STORE_OK) {
            (void)cbm_store_rollback(s);
            return rc;
        }
        rc = cbm_store_delete_import_refs_by_file(s, deltas[i]->project, deltas[i]->rel_path);
        if (rc != CBM_STORE_OK) {
            (void)cbm_store_rollback(s);
            return rc;
        }
        rc = store_publish_file_delta_metadata_body(s, deltas[i]);
        if (rc != CBM_STORE_OK) {
            (void)cbm_store_rollback(s);
            return rc;
        }
    }
    CBM_PROF_END_N("store_delta_noop", "1_metadata", t_metadata, delta_count);
    CBM_PROF_START(t_finish);
    rc = store_finish_index_generation_body(s, project, generation, CBM_STORE_INDEX_STATUS_COMPLETE);
    CBM_PROF_END("store_delta_noop", "2_finish_generation", t_finish);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_commit);
    rc = cbm_store_commit(s);
    CBM_PROF_END("store_delta_noop", "3_commit", t_commit);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_store_publish_file_delta(cbm_store_t *s, const cbm_store_file_delta_t *delta) {
    if (!s || !store_file_delta_shape_valid(delta)) {
        return CBM_STORE_ERR;
    }

    int rc = cbm_store_begin(s);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    rc = store_publish_file_delta_body(s, delta);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    rc = store_mark_graph_derived_views_stale_body(s, delta->project, delta->generation);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    rc = cbm_store_commit(s);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_store_publish_file_delta_batch(cbm_store_t *s,
                                       const cbm_store_file_delta_t *const *deltas,
                                       int delta_count) {
    const char *project = NULL;
    int64_t generation = -1;
    if (!s || !store_file_delta_batch_shape_valid(deltas, delta_count, &project, &generation)) {
        return CBM_STORE_ERR;
    }

    CBM_PROF_START(t_begin);
    int rc = cbm_store_begin(s);
    CBM_PROF_END("store_delta_publish", "0_begin", t_begin);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_body);
    rc = store_publish_file_delta_batch_body(s, deltas, delta_count);
    CBM_PROF_END_N("store_delta_publish", "5_body_total", t_body, delta_count);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_stale);
    rc = store_mark_graph_derived_views_stale_body(s, project, generation);
    CBM_PROF_END("store_delta_publish", "6_mark_stale", t_stale);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_commit);
    rc = cbm_store_commit(s);
    CBM_PROF_END("store_delta_publish", "8_commit", t_commit);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_store_publish_file_delta_batch_complete(cbm_store_t *s,
                                                const cbm_store_file_delta_t *const *deltas,
                                                int delta_count) {
    const char *project = NULL;
    int64_t generation = -1;
    if (!s || !store_file_delta_batch_shape_valid(deltas, delta_count, &project, &generation) ||
        generation <= 0) {
        return CBM_STORE_ERR;
    }

    CBM_PROF_START(t_begin);
    int rc = cbm_store_begin(s);
    CBM_PROF_END("store_delta_publish", "0_begin", t_begin);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_body);
    rc = store_publish_file_delta_batch_body(s, deltas, delta_count);
    CBM_PROF_END_N("store_delta_publish", "5_body_total", t_body, delta_count);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_stale);
    rc = store_mark_graph_derived_views_stale_body(s, project, generation);
    CBM_PROF_END("store_delta_publish", "6_mark_stale", t_stale);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_finish);
    rc = store_finish_index_generation_body(s, project, generation, CBM_STORE_INDEX_STATUS_COMPLETE);
    CBM_PROF_END("store_delta_publish", "7_finish_generation", t_finish);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_commit);
    rc = cbm_store_commit(s);
    CBM_PROF_END("store_delta_publish", "8_commit", t_commit);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    return CBM_STORE_OK;
}

int cbm_store_apply_file_delta_batch_complete(cbm_store_t *s,
                                              const cbm_store_file_delta_t *const *delete_deltas,
                                              int delete_count,
                                              const cbm_store_file_delta_t *const *upsert_deltas,
                                              int upsert_count) {
    const char *project = NULL;
    int64_t generation = -1;
    if (!s || !store_file_delta_apply_batch_shape_valid(delete_deltas, delete_count, upsert_deltas,
                                                        upsert_count, &project, &generation)) {
        return CBM_STORE_ERR;
    }

    CBM_PROF_START(t_begin);
    int rc = cbm_store_begin(s);
    CBM_PROF_END("store_delta_apply", "0_begin", t_begin);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    CBM_PROF_START(t_delete);
    for (int i = 0; i < delete_count; i++) {
        rc = store_delete_file_delta_body(s, delete_deltas[i]->project, delete_deltas[i]->rel_path,
                                          delete_deltas[i]->generation,
                                          delete_deltas[i]->derived_view_name);
        if (rc != CBM_STORE_OK) {
            (void)cbm_store_rollback(s);
            return rc;
        }
    }
    CBM_PROF_END_N("store_delta_apply", "1_delete", t_delete, delete_count);
    CBM_PROF_START(t_publish);
    if (upsert_count > 0) {
        rc = store_publish_file_delta_batch_body(s, upsert_deltas, upsert_count);
        if (rc != CBM_STORE_OK) {
            (void)cbm_store_rollback(s);
            return rc;
        }
    }
    CBM_PROF_END_N("store_delta_apply", "2_publish", t_publish, upsert_count);
    CBM_PROF_START(t_stale);
    rc = store_mark_graph_derived_views_stale_body(s, project, generation);
    CBM_PROF_END("store_delta_apply", "3_mark_stale", t_stale);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_finish);
    rc = store_finish_index_generation_body(s, project, generation, CBM_STORE_INDEX_STATUS_COMPLETE);
    CBM_PROF_END("store_delta_apply", "4_finish_generation", t_finish);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
    }
    CBM_PROF_START(t_commit);
    rc = cbm_store_commit(s);
    CBM_PROF_END("store_delta_apply", "5_commit", t_commit);
    if (rc != CBM_STORE_OK) {
        (void)cbm_store_rollback(s);
        return rc;
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
    int rc = sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "overlap prepare");
        return CBM_STORE_ERR;
    }

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, file_path);
    sqlite3_bind_int(stmt, ST_COL_3, start_line);
    sqlite3_bind_int(stmt, ST_COL_4, end_line);

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_node_t *nodes = malloc(cap * sizeof(cbm_node_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
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
    char like_pattern[CBM_SZ_512];
    snprintf(like_pattern, sizeof(like_pattern), "%%.%s", suffix);

    const char *sql_with_project =
        "SELECT id, project, label, name, qualified_name, file_path, "
        "start_line, end_line, properties FROM nodes "
        "WHERE project = ?1 AND (qualified_name LIKE ?2 OR qualified_name = ?3)";
    const char *sql_any = "SELECT id, project, label, name, qualified_name, file_path, "
                          "start_line, end_line, properties FROM nodes "
                          "WHERE (qualified_name LIKE ?1 OR qualified_name = ?2)";

    sqlite3_stmt *stmt = NULL;
    int rc =
        sqlite3_prepare_v2(s->db, project ? sql_with_project : sql_any, CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "qn_suffix prepare");
        return CBM_STORE_ERR;
    }

    if (project) {
        bind_text(stmt, SKIP_ONE, project);
        bind_text(stmt, ST_COL_2, like_pattern);
        bind_text(stmt, ST_COL_3, suffix);
    } else {
        bind_text(stmt, SKIP_ONE, like_pattern);
        bind_text(stmt, ST_COL_2, suffix);
    }

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_node_t *nodes = malloc(cap * sizeof(cbm_node_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
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

void cbm_store_node_degree(cbm_store_t *s, int64_t node_id, int *in_deg, int *out_deg) {
    if (!s) {
        if (in_deg)
            *in_deg = 0;
        if (out_deg)
            *out_deg = 0;
        return;
    }
    *in_deg = 0;
    *out_deg = 0;

    /* Direct degree counts every edge type EXCEPT 'INHERITS'. The class
     * hierarchy edge (INHERITS) is structural, not a runtime call/usage, so
     * it is excluded here; a node that only inherits from a base contributes
     * 0 to its direct fan-out. cbm_store_search's result in_degree/out_degree,
     * by contrast, use total_in/out which DOES count INHERITS (see the
     * degree_mode dispatch in cbm_store_search) — search degree reflects
     * overall graph connectivity, direct degree reflects active references.
     *
     * The precomputed node_degree table has no "all-but-INHERITS" column
     * (total_* includes INHERITS, calls_* excludes USAGE too), so this is
     * always computed live. cbm_store_node_degree is only called from
     * per-node cypher/mcp lookups (not hot loops), so the COUNT cost is fine.
     * See test_store_nodes.c:store_node_degree (CALLS+USAGE counted) and
     * test_store_search.c:store_search_degree_counts_inherits (INHERITS
     * excluded) for the contract. */
    sqlite3_stmt *stmt = NULL;
    const char *in_sql =
        "SELECT COUNT(*) FROM edges WHERE target_id = ?1 AND type != 'INHERITS'";
    if (sqlite3_prepare_v2(s->db, in_sql, CBM_NOT_FOUND, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, SKIP_ONE, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) *in_deg = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
    const char *out_sql =
        "SELECT COUNT(*) FROM edges WHERE source_id = ?1 AND type != 'INHERITS'";
    if (sqlite3_prepare_v2(s->db, out_sql, CBM_NOT_FOUND, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, SKIP_ONE, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) *out_deg = sqlite3_column_int(stmt, 0);
        sqlite3_finalize(stmt);
    }
}

/* ── List distinct file paths ────────────────────────────────── */

int cbm_store_list_files(cbm_store_t *s, const char *project, char ***out, int *count) {
    *out = NULL;
    *count = 0;
    if (!s || !s->db || !project) {
        return CBM_STORE_ERR;
    }

    const char *sql = "SELECT DISTINCT file_path FROM nodes "
                      "WHERE project = ?1 AND file_path IS NOT NULL AND file_path != ''";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    sqlite3_bind_text(stmt, SKIP_ONE, project, CBM_NOT_FOUND, SQLITE_STATIC);

    int cap = CBM_SZ_64;
    int n = 0;
    char **files = malloc(cap * sizeof(char *));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *fp = (const char *)sqlite3_column_text(stmt, 0);
        if (!fp) {
            continue;
        }
        if (n >= cap) {
            cap *= ST_GROWTH;
            files = safe_realloc(files, cap * sizeof(char *));
        }
        files[n++] = heap_strdup(fp);
    }
    sqlite3_finalize(stmt);
    *out = files;
    *count = n;
    return CBM_STORE_OK;
}

/* ── Node neighbor names ──────────────────────────────────────── */

static int query_neighbor_names(sqlite3 *db, const char *sql, int64_t node_id, int limit,
                                char ***out, int *out_count) {
    *out = NULL;
    *out_count = 0;
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        return CBM_NOT_FOUND;
    }
    sqlite3_bind_int64(stmt, SKIP_ONE, node_id);
    sqlite3_bind_int(stmt, ST_COL_2, limit);

    int cap = ST_INIT_CAP_8;
    char **names = malloc((size_t)cap * sizeof(char *));
    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        if (!name) {
            continue;
        }
        if (count >= cap) {
            cap *= ST_GROWTH;
            names = safe_realloc(names, (size_t)cap * sizeof(char *));
        }
        names[count++] = heap_strdup(name);
    }
    sqlite3_finalize(stmt);
    *out = names;
    *out_count = count;
    return 0;
}

int cbm_store_node_neighbor_names(cbm_store_t *s, int64_t node_id, int limit, char ***out_callers,
                                  int *caller_count, char ***out_callees, int *callee_count) {
    if (!s) {
        return CBM_NOT_FOUND;
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

static int count_degrees_direction(cbm_store_t *s, const int64_t *node_ids, int id_count,
                                   const char *in_clause, bool has_type, const char *edge_type,
                                   bool inbound, int *out_counts) {
    char sql[ST_SQL_BUF];
    const char *id_col = inbound ? "target_id" : "source_id";
    if (has_type) {
        snprintf(sql, sizeof(sql),
                 "SELECT %s, COUNT(*) FROM edges "
                 "WHERE %s IN (%s) AND type = ? GROUP BY %s",
                 id_col, id_col, in_clause, id_col);
    } else {
        snprintf(sql, sizeof(sql),
                 "SELECT %s, COUNT(*) FROM edges "
                 "WHERE %s IN (%s) GROUP BY %s",
                 id_col, id_col, in_clause, id_col);
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }

    for (int i = 0; i < id_count; i++) {
        sqlite3_bind_int64(stmt, i + SKIP_ONE, node_ids[i]);
    }
    if (has_type) {
        bind_text(stmt, id_count + SKIP_ONE, edge_type);
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t nid = sqlite3_column_int64(stmt, 0);
        int cnt = sqlite3_column_int(stmt, SKIP_ONE);
        for (int i = 0; i < id_count; i++) {
            if (node_ids[i] == nid) {
                out_counts[i] = cnt;
                break;
            }
        }
    }
    sqlite3_finalize(stmt);
    return CBM_STORE_OK;
}

int cbm_store_batch_count_degrees(cbm_store_t *s, const int64_t *node_ids, int id_count,
                                  const char *edge_type, int *out_in, int *out_out) {
    if (!s || !node_ids || id_count <= 0 || !out_in || !out_out) {
        return CBM_STORE_ERR;
    }

    memset(out_in, 0, (size_t)id_count * sizeof(int));
    memset(out_out, 0, (size_t)id_count * sizeof(int));

    /* Build IN clause: (?,?,?) */
    char in_clause[CBM_SZ_4K];
    int pos = 0;
    for (int i = 0; i < id_count && pos < (int)sizeof(in_clause) - ST_IN_CLAUSE_MARGIN; i++) {
        if (i > 0) {
            in_clause[pos++] = ',';
        }
        in_clause[pos++] = '?';
    }
    in_clause[pos] = '\0';

    bool has_type = edge_type && edge_type[0] != '\0';

    int rc = count_degrees_direction(s, node_ids, id_count, in_clause, has_type, edge_type, true,
                                     out_in);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    return count_degrees_direction(s, node_ids, id_count, in_clause, has_type, edge_type, false,
                                   out_out);
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

int cbm_store_find_edges_by_url_path(cbm_store_t *s, const char *project, const char *keyword,
                                     cbm_edge_t **out, int *count) {
    *out = NULL;
    *count = 0;

    /* Search properties JSON for url_path containing keyword */
    char like_pattern[CBM_SZ_512];
    snprintf(like_pattern, sizeof(like_pattern), "%%\"url_path\":\"%%%s%%\"%%", keyword);

    const char *sql = "SELECT id, project, source_id, target_id, type, properties FROM edges "
                      "WHERE project = ?1 AND properties LIKE ?2";

    sqlite3_stmt *stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "url_path prepare");
        return CBM_STORE_ERR;
    }

    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, like_pattern);

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_edge_t *edges = malloc(cap * sizeof(cbm_edge_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
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
    if (!dst || !src) {
        return CBM_STORE_ERR;
    }
    sqlite3_backup *bk = sqlite3_backup_init(dst->db, "main", src->db, "main");
    if (!bk) {
        store_set_error_sqlite(dst, "backup init");
        return CBM_STORE_ERR;
    }
    int rc = sqlite3_backup_step(bk, CBM_NOT_FOUND); /* copy all pages */
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
    char *out = malloc((len * ST_GROWTH) + SKIP_ONE);
    size_t j = 0;

    for (size_t i = 0; i < len; i++) {
        if (pattern[i] == '*' && i + SKIP_ONE < len && pattern[i + SKIP_ONE] == '*') {
            /* Remove leading / from output if present (handles glob dir-star) */
            if (j > 0 && out[j - SKIP_ONE] == '/') {
                j--;
            }
            out[j++] = '%';
            i++; /* skip second * */
            if (i + SKIP_ONE < len && pattern[i + SKIP_ONE] == '/') {
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
    char buf[CBM_SZ_256];
    int blen = 0;

    int i = 0;
    while (pattern[i]) {
        char ch = pattern[i];
        switch (ch) {
        case '\\':
            /* Escaped char — the next char is literal */
            if (pattern[i + SKIP_ONE]) {
                if (blen < (int)sizeof(buf) - SKIP_ONE) {
                    buf[blen++] = pattern[i + SKIP_ONE];
                }
                i += ST_GLOB_SKIP;
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
            if (blen >= ST_GLOB_MIN_LEN && count < max_out) {
                buf[blen] = '\0';
                out[count++] = heap_strdup(buf);
            }
            blen = 0;
            i++;
            break;
        default:
            if (blen < (int)sizeof(buf) - SKIP_ONE) {
                buf[blen++] = ch;
            }
            i++;
            break;
        }
    }
    /* Flush trailing segment */
    if (blen >= ST_GLOB_MIN_LEN && count < max_out) {
        buf[blen] = '\0';
        out[count++] = heap_strdup(buf);
    }
    return count;
}

/* ── ensureCaseInsensitive / stripCaseFlag ────────────────────── */

const char *cbm_ensure_case_insensitive(const char *pattern) {
    static char buf[CBM_SZ_2K];
    if (!pattern) {
        buf[0] = '\0';
        return buf;
    }
    /* Already has (?i) prefix? Return as-is. */
    if (strncmp(pattern, "(?i)", SLEN("(?i)")) == 0) {
        snprintf(buf, sizeof(buf), "%s", pattern);
    } else {
        snprintf(buf, sizeof(buf), "(?i)%s", pattern);
    }
    return buf;
}

const char *cbm_strip_case_flag(const char *pattern) {
    static char buf[CBM_SZ_2K];
    if (!pattern) {
        buf[0] = '\0';
        return buf;
    }
    if (strncmp(pattern, "(?i)", SLEN("(?i)")) == 0) {
        snprintf(buf, sizeof(buf), "%s", pattern + 4);
    } else {
        snprintf(buf, sizeof(buf), "%s", pattern);
    }
    return buf;
}

/* Bind value tracker for dynamic query building */
typedef struct {
    const char *text;
} search_bind_t;

/* Pool of malloc'd strings that must outlive statement execution.
 * Freed in one shot after both statements are finalized. */
typedef struct {
    char *ptrs[ST_LIKE_POOL_MAX];
    int count;
} search_like_pool_t;

static void like_pool_add(search_like_pool_t *pool, char *ptr) {
    if (ptr && pool->count < ST_LIKE_POOL_MAX) {
        pool->ptrs[pool->count++] = ptr;
    } else {
        free(ptr); /* pool full — don't leak */
    }
}

static void like_pool_free(search_like_pool_t *pool) {
    for (int i = 0; i < pool->count; i++) {
        free(pool->ptrs[i]);
    }
    pool->count = 0;
}

/* Wrap a literal string in % for use as a LIKE pattern (%literal%). */
static char *make_like_hint(const char *literal) {
    size_t len = strlen(literal);
    char *buf = malloc(len + 3); /* % + literal + % + NUL */
    if (buf) {
        buf[0] = '%';
        memcpy(buf + SKIP_ONE, literal, len);
        buf[len + SKIP_ONE] = '%';
        buf[len + 2] = '\0';
    }
    return buf;
}

static void search_apply_degree_filter(char *sql, size_t sql_sz, const cbm_search_params_t *p) {
    bool has_degree_filter = (p->min_degree >= 0 || p->max_degree >= 0);
    if (!has_degree_filter) {
        return;
    }
    char inner_sql[CBM_SZ_4K];
    snprintf(inner_sql, sizeof(inner_sql), "%s", sql);
    if (p->min_degree >= 0 && p->max_degree >= 0) {
        snprintf(sql, sql_sz,
                 "SELECT * FROM (%s) WHERE (in_deg + out_deg) >= %d AND (in_deg + out_deg) <= %d",
                 inner_sql, p->min_degree, p->max_degree);
    } else if (p->min_degree >= 0) {
        snprintf(sql, sql_sz, "SELECT * FROM (%s) WHERE (in_deg + out_deg) >= %d", inner_sql,
                 p->min_degree);
    } else {
        snprintf(sql, sql_sz, "SELECT * FROM (%s) WHERE (in_deg + out_deg) <= %d", inner_sql,
                 p->max_degree);
    }
}

/* Append a WHERE clause fragment, joining with AND if not the first. */
static int where_append(char *where, int where_sz, int wlen, int *nparams, const char *cond) {
    if (*nparams > 0) {
        wlen += snprintf(where + wlen, where_sz - wlen, " AND ");
    }
    wlen += snprintf(where + wlen, where_sz - wlen, "%s", cond);
    (*nparams)++;
    return wlen;
}

/* Bind a text parameter and increment the bind index. */
static void where_bind_text(search_bind_t *binds, int *bind_idx, const char *val) {
    binds[*bind_idx].text = val;
    (*bind_idx)++;
}

/* Build exclude-labels NOT IN clause with bind placeholders. */
static void search_build_exclude_labels(const char **labels, search_bind_t *binds, int *bind_idx,
                                        char *clause, int clause_sz) {
    int elen = snprintf(clause, clause_sz, "n.label NOT IN (");
    for (int i = 0; labels[i]; i++) {
        if (i > 0) {
            elen += snprintf(clause + elen, clause_sz - elen, ",");
            if (elen >= clause_sz) {
                elen = clause_sz - SKIP_ONE;
            }
        }
        elen += snprintf(clause + elen, clause_sz - elen, "?%d", *bind_idx + SKIP_ONE);
        if (elen >= clause_sz) {
            elen = clause_sz - SKIP_ONE;
        }
        where_bind_text(binds, bind_idx, labels[i]);
    }
    snprintf(clause + elen, clause_sz - elen, ")");
}

/* Append a regex WHERE clause for a column (case-sensitive or insensitive). */
static void where_add_regex(char *where, int where_sz, int *wlen, int *nparams,
                            search_bind_t *binds, int *bind_idx, const char *column,
                            const char *pattern, bool case_sensitive) {
    char buf[CBM_SZ_128];
    if (case_sensitive) {
        snprintf(buf, sizeof(buf), "%s REGEXP ?%d", column, *bind_idx + SKIP_ONE);
    } else {
        snprintf(buf, sizeof(buf), "iregexp(?%d, %s)", *bind_idx + SKIP_ONE, column);
    }
    *wlen = where_append(where, where_sz, *wlen, nparams, buf);
    where_bind_text(binds, bind_idx, pattern);
}

/* Prepend LIKE pre-filter conditions for literal segments of a regex pattern.
 * The idx_nodes_name index satisfies LIKE '%literal%', cutting the rows that
 * reach the (more expensive) iregexp call to only those containing the literal.
 * cbm_extract_like_hints bails on alternation, so no false negatives. */
static void where_add_like_hints(const char *column, const char *pattern, char *where, int where_sz,
                                 int *wlen, int *nparams, search_bind_t *binds, int *bind_idx,
                                 search_like_pool_t *pool) {
    char *hints[ST_LIKE_HINT_MAX];
    int nhints = cbm_extract_like_hints(pattern, hints, ST_LIKE_HINT_MAX);
    char bind_buf[CBM_SZ_64];
    for (int i = 0; i < nhints; i++) {
        char *lp = make_like_hint(hints[i]);
        free(hints[i]);
        if (!lp)
            continue;
        int pool_was_full = (pool->count >= ST_LIKE_POOL_MAX);
        like_pool_add(pool, lp);
        if (pool_was_full)
            continue; /* lp was freed — skip bind */
        snprintf(bind_buf, sizeof(bind_buf), "%s LIKE ?%d", column, *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
        where_bind_text(binds, bind_idx, lp);
    }
}

/* Build basic WHERE clauses: project, label, name, file, qn patterns. */
static int search_where_basic(const cbm_search_params_t *params, char *where, int where_sz,
                              int *wlen, int *nparams, search_bind_t *binds, int *bind_idx,
                              search_like_pool_t *pool) {
    char bind_buf[CBM_SZ_64];

    /* MERGE: fork delta — project_pattern / project_exact smart project matching.
     * project_pattern: glob/LIKE pattern (e.g., "myapp.dep.%").
     * project_exact: exact match only (no prefix), used for "self" (project code, no deps).
     * project (default): prefix match — project itself + dep sub-projects. */
    if (params->project_pattern) {
        snprintf(bind_buf, sizeof(bind_buf), "n.project LIKE ?%d", *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
        where_bind_text(binds, bind_idx, params->project_pattern);
    } else if (params->project && params->project_exact) {
        snprintf(bind_buf, sizeof(bind_buf), "n.project = ?%d", *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
        where_bind_text(binds, bind_idx, params->project);
    } else if (params->project) {
        /* Prefix match: project + dep sub-projects (e.g., myapp.dep.pandas).
         * Allocates a LIKE pattern that must outlive the bind — pool-managed.
         * If the LIKE pool is full, degrade to exact-only (single placeholder)
         * to keep placeholders and bind values balanced. */
        size_t plen = strlen(params->project);
        char *proj_like = malloc(plen + 3); /* "proj" + ".%" + NUL */
        int pool_was_full = (pool->count >= ST_LIKE_POOL_MAX);
        if (proj_like) {
            memcpy(proj_like, params->project, plen);
            proj_like[plen] = '.';
            proj_like[plen + 1] = '%';
            proj_like[plen + 2] = '\0';
            like_pool_add(pool, proj_like);
        }
        if (pool_was_full || !proj_like) {
            /* Pool full or alloc failed — exact match only */
            snprintf(bind_buf, sizeof(bind_buf), "n.project = ?%d", *bind_idx + SKIP_ONE);
            *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
            where_bind_text(binds, bind_idx, params->project);
        } else {
            snprintf(bind_buf, sizeof(bind_buf), "(n.project = ?%d OR n.project LIKE ?%d)",
                     *bind_idx + SKIP_ONE, *bind_idx + ST_COL_2);
            *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
            where_bind_text(binds, bind_idx, params->project);
            where_bind_text(binds, bind_idx, proj_like);
        }
    }
    /* Ignore an empty-string label: it is non-NULL but should behave like an
     * omitted label (no filter), matching the BM25 query path. Without the
     * params->label[0] guard, name_pattern/qn_pattern searches that pass
     * label="" append `n.label = ''`, which matches no node and silently
     * returns zero results (issue #481). */
    if (params->label && params->label[0]) {
        snprintf(bind_buf, sizeof(bind_buf), "n.label = ?%d", *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
        where_bind_text(binds, bind_idx, params->label);
    }
    if (params->name_pattern) {
        where_add_like_hints("n.name", params->name_pattern, where, where_sz, wlen, nparams, binds,
                             bind_idx, pool);
        where_add_regex(where, where_sz, wlen, nparams, binds, bind_idx, "n.name",
                        params->name_pattern, params->case_sensitive);
    }
    if (params->qn_pattern) {
        where_add_like_hints("n.qualified_name", params->qn_pattern, where, where_sz, wlen, nparams,
                             binds, bind_idx, pool);
        where_add_regex(where, where_sz, wlen, nparams, binds, bind_idx, "n.qualified_name",
                        params->qn_pattern, params->case_sensitive);
    }
    if (params->file_pattern) {
        char *lp = cbm_glob_to_like(params->file_pattern);
        /* A file_pattern with no glob wildcards is treated as a path-substring
         * match (issue #200): file_pattern="offer-server" should match
         * "src/offer-server/x.js", not only a path equal to "offer-server".
         * Patterns that DO contain * / ? keep their explicit glob semantics. */
        if (lp && !strchr(params->file_pattern, '*') && !strchr(params->file_pattern, '?')) {
            size_t lplen = strlen(lp);
            char *contains = malloc(lplen + 3);
            if (contains) {
                contains[0] = '%';
                memcpy(contains + 1, lp, lplen);
                contains[lplen + 1] = '%';
                contains[lplen + 2] = '\0';
                free(lp);
                lp = contains;
            }
        }
        int pool_was_full = (pool->count >= ST_LIKE_POOL_MAX);
        like_pool_add(pool, lp);
        if (!pool_was_full && lp) {
            snprintf(bind_buf, sizeof(bind_buf), "n.file_path LIKE ?%d", *bind_idx + SKIP_ONE);
            *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
            where_bind_text(binds, bind_idx, lp);
        }
    }
    /* MERGE: fork delta — pattern OR-search (name OR qualified_name) for quick
     * symbol lookup, and exclude_paths (NOT LIKE per glob). */
    if (params->pattern) {
        char or_buf[CBM_SZ_128];
        if (params->case_sensitive) {
            snprintf(or_buf, sizeof(or_buf),
                     "(n.name REGEXP ?%d OR n.qualified_name REGEXP ?%d)",
                     *bind_idx + SKIP_ONE, *bind_idx + ST_COL_2);
        } else {
            snprintf(or_buf, sizeof(or_buf),
                     "(iregexp(?%d, n.name) OR iregexp(?%d, n.qualified_name))",
                     *bind_idx + SKIP_ONE, *bind_idx + ST_COL_2);
        }
        *wlen = where_append(where, where_sz, *wlen, nparams, or_buf);
        where_bind_text(binds, bind_idx, params->pattern);
        where_bind_text(binds, bind_idx, params->pattern);
    }
    if (params->exclude_paths) {
        for (int i = 0; params->exclude_paths[i]; i++) {
            char *ex_lp = cbm_glob_to_like(params->exclude_paths[i]);
            int pool_was_full = (pool->count >= ST_LIKE_POOL_MAX);
            like_pool_add(pool, ex_lp);
            if (pool_was_full)
                continue; /* ex_lp freed — skip bind */
            snprintf(bind_buf, sizeof(bind_buf), "n.file_path NOT LIKE ?%d",
                     *bind_idx + SKIP_ONE);
            *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
            where_bind_text(binds, bind_idx, ex_lp);
        }
    }
    return *nparams;
}

/* Build advanced WHERE clauses: relationship, entry points, exclude labels. */
static void search_where_advanced(const cbm_search_params_t *params, char *where, int where_sz,
                                  int *wlen, int *nparams, search_bind_t *binds, int *bind_idx) {
    if (params->relationship) {
        char rel_clause[CBM_SZ_256];
        snprintf(rel_clause, sizeof(rel_clause),
                 "EXISTS(SELECT 1 FROM edges e WHERE "
                 "(e.source_id = n.id OR e.target_id = n.id) AND e.type = ?%d)",
                 *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, rel_clause);
        where_bind_text(binds, bind_idx, params->relationship);
    }
    if (params->exclude_entry_points) {
        /* Exclude entry points: any node with zero inbound edges (in_deg = 0).
         * Matches the fork's "in_deg > 0" semantics — filters both true entry
         * points (no inbound, has outbound) and dead code (no edges at all). */
        *wlen = where_append(where, where_sz, *wlen, nparams,
                             "EXISTS(SELECT 1 FROM edges e WHERE e.target_id = n.id)");
    }
    if (params->exclude_labels) {
        char excl_clause[CBM_SZ_512];
        search_build_exclude_labels(params->exclude_labels, binds, bind_idx, excl_clause,
                                    (int)sizeof(excl_clause));
        (void)where_append(where, where_sz, *wlen, nparams, excl_clause);
    }
}

static int search_build_where(const cbm_search_params_t *params, char *where, int where_sz,
                              search_bind_t *binds, int *bind_idx, search_like_pool_t *pool) {
    int wlen = 0;
    int nparams = 0;

    search_where_basic(params, where, where_sz, &wlen, &nparams, binds, bind_idx, pool);
    search_where_advanced(params, where, where_sz, &wlen, &nparams, binds, bind_idx);

    return nparams;
}

int cbm_store_search(cbm_store_t *s, const cbm_search_params_t *params, cbm_search_output_t *out) {
    memset(out, 0, sizeof(*out));
    if (!s || !s->db) {
        return CBM_STORE_ERR;
    }

    char sql[CBM_SZ_4K];
    char count_sql[CBM_SZ_4K];
    int bind_idx = 0;

    const char *freshness_project =
        (params->project && params->project[0] && !params->project_pattern) ? params->project : NULL;
    out->pagerank_stale =
        freshness_project &&
        cbm_store_derived_view_is_stale(s, freshness_project, CBM_STORE_DERIVED_VIEW_PAGERANK);
    out->linkrank_stale =
        freshness_project &&
        cbm_store_derived_view_is_stale(s, freshness_project, CBM_STORE_DERIVED_VIEW_LINKRANK);
    out->node_degree_stale =
        freshness_project &&
        cbm_store_derived_view_is_stale(s, freshness_project, CBM_STORE_DERIVED_VIEW_NODE_DEGREE);

    /* Conditionally join pagerank table only when sort_by is relevance and the
     * freshness ledger has not explicitly marked PageRank stale. Missing ledger
     * rows are treated as legacy-compatible, so older indexes keep their rank data. */
    bool use_pagerank =
        (!params->sort_by || strcmp(params->sort_by, "relevance") == 0) && !out->pagerank_stale;
    /* DF-1: Use precomputed node_degree table when available (O(1) JOIN vs O(|E|) subquery).
     * HC-6: Falls back to edge COUNT when node_degree is empty. */
    bool has_degree_table = false;
    if (!out->node_degree_stale) {
        sqlite3_stmt *check = NULL;
        if (sqlite3_prepare_v2(s->db,
                "SELECT 1 FROM node_degree LIMIT 1", -1, &check, NULL) == SQLITE_OK) {
            has_degree_table = (sqlite3_step(check) == SQLITE_ROW);
            sqlite3_finalize(check);
        }
    }
    /* Choose degree columns based on degree_mode param.
     * degree_mode: "weighted"→weighted_in/out, "calls_only"→calls_in/out,
     * NULL/"unweighted"→total_in/out (default). Only applies when has_degree_table. */
    const char *in_expr  = "COALESCE(nd.total_in, 0)";
    const char *out_expr = "COALESCE(nd.total_out, 0)";
    if (has_degree_table && params->degree_mode) {
        if (strcmp(params->degree_mode, "weighted") == 0) {
            in_expr  = "COALESCE(nd.weighted_in, 0)";
            out_expr = "COALESCE(nd.weighted_out, 0)";
        } else if (strcmp(params->degree_mode, "calls_only") == 0) {
            in_expr  = "COALESCE(nd.calls_in, 0)";
            out_expr = "COALESCE(nd.calls_out, 0)";
        }
    }
    char sel_with_pr_deg[512];
    char sel_deg_only[512];
    snprintf(sel_with_pr_deg, sizeof(sel_with_pr_deg),
        "SELECT n.id, n.project, n.label, n.name, n.qualified_name, "
        "n.file_path, n.start_line, n.end_line, n.properties, "
        "%s AS in_deg, %s AS out_deg, COALESCE(pr.rank, 0.0) AS pr_rank ", in_expr, out_expr);
    snprintf(sel_deg_only, sizeof(sel_deg_only),
        "SELECT n.id, n.project, n.label, n.name, n.qualified_name, "
        "n.file_path, n.start_line, n.end_line, n.properties, "
        "%s AS in_deg, %s AS out_deg ", in_expr, out_expr);
    const char *select_cols;
    if (use_pagerank && has_degree_table) {
        select_cols = sel_with_pr_deg;
    } else if (use_pagerank) {
        select_cols =
            "SELECT n.id, n.project, n.label, n.name, n.qualified_name, "
            "n.file_path, n.start_line, n.end_line, n.properties, "
            "(SELECT COUNT(*) FROM edges e WHERE e.target_id = n.id) AS in_deg, "
            "(SELECT COUNT(*) FROM edges e WHERE e.source_id = n.id) AS out_deg, "
            "COALESCE(pr.rank, 0.0) AS pr_rank ";
    } else if (has_degree_table) {
        select_cols = sel_deg_only;
    } else {
        select_cols =
            "SELECT n.id, n.project, n.label, n.name, n.qualified_name, "
            "n.file_path, n.start_line, n.end_line, n.properties, "
            "(SELECT COUNT(*) FROM edges e WHERE e.target_id = n.id) AS in_deg, "
            "(SELECT COUNT(*) FROM edges e WHERE e.source_id = n.id) AS out_deg ";
    }

    char where[CBM_SZ_2K] = "";
    search_bind_t binds[ST_SEARCH_MAX_BINDS];
    search_like_pool_t like_pool = {0};

    int nparams =
        search_build_where(params, where, (int)sizeof(where), binds, &bind_idx, &like_pool);

    /* MERGE: fork delta — build full SQL with conditional PageRank/degree JOINs.
     * from_join dispatch wires up pagerank pr and node_degree nd tables so the
     * select_cols expressions (pr_rank, nd.* degree columns) resolve. */
    const char *from_join;
    if (use_pagerank && has_degree_table)
        from_join = "FROM nodes n LEFT JOIN pagerank pr ON pr.node_id = n.id "
                    "LEFT JOIN node_degree nd ON nd.node_id = n.id";
    else if (use_pagerank)
        from_join = "FROM nodes n LEFT JOIN pagerank pr ON pr.node_id = n.id";
    else if (has_degree_table)
        from_join = "FROM nodes n LEFT JOIN node_degree nd ON nd.node_id = n.id";
    else
        from_join = "FROM nodes n";
    if (nparams > 0) {
        snprintf(sql, sizeof(sql), "%s %s WHERE %s", select_cols, from_join, where);
    } else {
        snprintf(sql, sizeof(sql), "%s %s", select_cols, from_join);
    }

    /* Degree filters — upstream helper wraps in subquery on in_deg/out_deg. */
    bool has_degree_filter = (params->min_degree >= 0 || params->max_degree >= 0);
    search_apply_degree_filter(sql, sizeof(sql), params);

    /* Count query — stripped of per-row edge subqueries for the common (no-degree-filter)
     * case, since we only need the row count, not in_deg/out_deg.  The degree-filter
     * case must wrap the full query because the filter references those columns. */
    if (has_degree_filter) {
        snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM (%s)", sql);
    } else if (nparams > 0) {
        snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM nodes n WHERE %s", where);
    } else {
        snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM nodes n");
    }

    /* Add ORDER BY + LIMIT */
    int limit = params->limit > 0 ? params->limit : CBM_DEFAULT_SEARCH_LIMIT;
    int offset = params->offset;
    /* MERGE: fork delta — sort dispatch: relevance (PageRank), degree, calls,
     * linkrank, name. Stable pagination via secondary sort on name, id.
     * Note: exclude_entry_points is applied in the WHERE clause (upstream),
     * not via subquery wrap, so column unqualification keys off has_degree_filter. */
    const char *name_col = has_degree_filter ? "name" : "n.name";
    const char *id_col = has_degree_filter ? "id" : "n.id";
    const char *pr_col = "pr_rank"; /* pagerank JOIN alias is always pr_rank,
                                     * unlike name_col/proj_col which differ by
                                     * has_degree_filter (subquery wrap). */
    char order_limit[CBM_SZ_256];

    /* Dep-last: rank the project's own symbols above dependency sub-project
     * symbols (proj.dep.*) whenever a search is scoped to a project (prefix
     * match OR glob). Applied as a tiebreak right after the primary metric
     * across ALL sort modes, so a stdlib symbol like 'Path' never outranks the
     * user's own 'Path' on equal merit (#18, the Path concern). Previously this
     * only fired for glob (project_pattern) searches; the common prefix path
     * (params->project) omitted it, letting deps win the name/id tiebreak.
     * Empty string when unscoped (no project) — preserves prior behavior. */
    const char *proj_col = has_degree_filter ? "project" : "n.project";
    bool scope_has_project = (params->project != NULL || params->project_pattern != NULL);
    char dep_last[CBM_SZ_128];
    if (scope_has_project && !params->disable_dep_ranking) {
        snprintf(dep_last, sizeof(dep_last),
                 "CASE WHEN %s LIKE '%%.dep.%%' THEN 1 ELSE 0 END, ", proj_col);
    } else {
        dep_last[0] = '\0';
    }

    if (use_pagerank) {
        /* Relevance sort: PageRank DESC, dep-last, name, id (stable pagination). */
        snprintf(order_limit, sizeof(order_limit),
                 " ORDER BY %s DESC, %s%s, %s LIMIT %d OFFSET %d",
                 pr_col, dep_last, name_col, id_col, limit, offset);
    } else if (params->sort_by && strcmp(params->sort_by, "degree") == 0) {
        snprintf(order_limit, sizeof(order_limit),
                 " ORDER BY (in_deg + out_deg) DESC, %s%s, %s LIMIT %d OFFSET %d",
                 dep_last, name_col, id_col, limit, offset);
    } else if (params->sort_by && strcmp(params->sort_by, "calls") == 0) {
        if (has_degree_table && !has_degree_filter) {
            /* nd.* only accessible when not wrapped by the degree-filter subquery */
            snprintf(order_limit, sizeof(order_limit),
                     " ORDER BY COALESCE(nd.calls_in + nd.calls_out, 0) DESC, %s%s, %s"
                     " LIMIT %d OFFSET %d",
                     dep_last, name_col, id_col, limit, offset);
        } else {
            /* Fallback: no precomputed calls data, or query wrapped by degree
             * filter (nd alias out of scope) — use total degree */
            snprintf(order_limit, sizeof(order_limit),
                     " ORDER BY (in_deg + out_deg) DESC, %s%s, %s LIMIT %d OFFSET %d",
                     dep_last, name_col, id_col, limit, offset);
        }
    } else if (params->sort_by && strcmp(params->sort_by, "linkrank") == 0) {
        if (has_degree_table && !out->linkrank_stale && !has_degree_filter) {
            snprintf(order_limit, sizeof(order_limit),
                     " ORDER BY COALESCE(nd.linkrank_in, 0) DESC, %s%s, %s LIMIT %d OFFSET %d",
                     dep_last, name_col, id_col, limit, offset);
        } else {
            /* Fallback: no precomputed linkrank — use total degree */
            snprintf(order_limit, sizeof(order_limit),
                     " ORDER BY (in_deg + out_deg) DESC, %s%s, %s LIMIT %d OFFSET %d",
                     dep_last, name_col, id_col, limit, offset);
        }
    } else {
        /* name sort (explicit or fallback): dep-last, name, id. */
        snprintf(order_limit, sizeof(order_limit),
                 " ORDER BY %s%s, %s LIMIT %d OFFSET %d",
                 dep_last, name_col, id_col, limit, offset);
    }
    strncat(sql, order_limit, sizeof(sql) - strlen(sql) - 1);

    /* Execute count query */
    sqlite3_stmt *cnt_stmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, count_sql, CBM_NOT_FOUND, &cnt_stmt, NULL);
    if (rc == SQLITE_OK) {
        for (int i = 0; i < bind_idx; i++) {
            bind_text(cnt_stmt, i + SKIP_ONE, binds[i].text);
        }
        if (sqlite3_step(cnt_stmt) == SQLITE_ROW) {
            out->total = sqlite3_column_int(cnt_stmt, 0);
        }
        sqlite3_finalize(cnt_stmt);
    }

    /* Execute main query */
    sqlite3_stmt *main_stmt = NULL;
    rc = sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &main_stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "search prepare");
        like_pool_free(&like_pool);
        return CBM_STORE_ERR;
    }

    for (int i = 0; i < bind_idx; i++) {
        bind_text(main_stmt, i + SKIP_ONE, binds[i].text);
    }

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_search_result_t *results = malloc(cap * sizeof(cbm_search_result_t));

    while (sqlite3_step(main_stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
            results = safe_realloc(results, cap * sizeof(cbm_search_result_t));
        }
        memset(&results[n], 0, sizeof(cbm_search_result_t));
        scan_node(main_stmt, &results[n].node);
        results[n].in_degree = sqlite3_column_int(main_stmt, ST_COL_9);
        results[n].out_degree = sqlite3_column_int(main_stmt, CBM_DECIMAL_BASE);
        /* MERGE: fork delta — pagerank_score at column 11 (only when use_pagerank
         * selected the pr_rank column; otherwise no such column exists). */
        results[n].pagerank_score =
            use_pagerank ? sqlite3_column_double(main_stmt, 11) : 0.0;
        n++;
    }

    sqlite3_finalize(main_stmt);
    like_pool_free(&like_pool);

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
        safe_str_free(&r->node.project);
        safe_str_free(&r->node.label);
        safe_str_free(&r->node.name);
        safe_str_free(&r->node.qualified_name);
        safe_str_free(&r->node.file_path);
        safe_str_free(&r->node.properties_json);
        for (int j = 0; j < r->connected_count; j++) {
            safe_str_free(&r->connected_names[j]);
        }
        free(r->connected_names);
    }
    free(out->results);
    memset(out, 0, sizeof(*out));
}

/* ── BFS Traversal ────────────────────────────────────────────────
 * Note: upstream factored edge collection into bfs_collect_edges() and the
 * types-clause build into bfs_build_types_clause(), but both were unused after
 * the fork inlined the BFS with the bfs_et_count cap. They were removed to
 * satisfy -Werror=unused-function. The active implementation is the inline
 * logic within cbm_store_bfs() below. */

int cbm_store_bfs(cbm_store_t *s, int64_t start_id, const char *direction, const char **edge_types,
                  int edge_type_count, int max_depth, int max_results, cbm_traverse_result_t *out) {
    memset(out, 0, sizeof(*out));

    cbm_node_t root = {0};
    int rc = cbm_store_find_node_by_id(s, start_id, &root);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    out->root = root;
    const char *freshness_project = root.project && root.project[0] ? root.project : NULL;
    out->pagerank_stale =
        freshness_project &&
        cbm_store_derived_view_is_stale(s, freshness_project, CBM_STORE_DERIVED_VIEW_PAGERANK);
    out->linkrank_stale =
        freshness_project &&
        cbm_store_derived_view_is_stale(s, freshness_project, CBM_STORE_DERIVED_VIEW_LINKRANK);
    bool use_pagerank = !out->pagerank_stale;
    bool use_linkrank = !out->linkrank_stale;

    /* MERGE: fork delta — build edge type IN clause with ?N parameterized
     * placeholders and cap at 16 (bfs_et_count) so the bind loop and clause
     * stay consistent. edge_types[] come from MCP tool call args. */
    char types_clause[CBM_SZ_512] = "?1"; /* default: single placeholder for "CALLS" */
    const char *default_edge_type = "CALLS";
    int bfs_et_count = edge_type_count > 0 ? edge_type_count : 1;
    if (edge_type_count > 0) {
        int tlen = 0;
        for (int i = 0; i < edge_type_count && i < 16; i++) {
            if (i > 0) {
                tlen += snprintf(types_clause + tlen, sizeof(types_clause) - (size_t)tlen, ",");
                if (tlen >= (int)sizeof(types_clause)) {
                    tlen = (int)sizeof(types_clause) - 1;
                }
            }
            tlen +=
                snprintf(types_clause + tlen, sizeof(types_clause) - (size_t)tlen, "?%d", i + 1);
            if (tlen >= (int)sizeof(types_clause)) {
                tlen = (int)sizeof(types_clause) - 1;
            }
        }
        bfs_et_count = edge_type_count < 16 ? edge_type_count : 16;
    }

    /* Build recursive CTE for BFS */
    char sql[CBM_SZ_4K];
    const char *join_cond;
    const char *next_id;
    bool is_inbound = (direction != NULL) && (strcmp(direction, "inbound") == 0);

    if (is_inbound) {
        join_cond = "e.target_id = bfs.node_id";
        next_id = "e.source_id";
    } else {
        join_cond = "e.source_id = bfs.node_id";
        next_id = "e.target_id";
    }

    const char *pagerank_select =
        use_pagerank ? "COALESCE(pr.rank, 0.0) AS pr_rank " : "0.0 AS pr_rank ";
    const char *pagerank_join = use_pagerank ? "LEFT JOIN pagerank pr ON pr.node_id = n.id " : "";
    const char *pagerank_order = use_pagerank ? "bfs.hop, pr_rank DESC" : "bfs.hop, n.name, n.id";
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
             "n.file_path, n.start_line, n.end_line, n.properties, bfs.hop, "
             "%s"
             "FROM bfs "
             "JOIN nodes n ON n.id = bfs.node_id "
             "%s"
             "WHERE bfs.hop > 0 " /* exclude root */
             "ORDER BY %s "
             "LIMIT %d;",
             (long long)start_id, next_id, join_cond, types_clause, max_depth, pagerank_select,
             pagerank_join, pagerank_order, max_results);

    sqlite3_stmt *stmt = NULL;
    rc = sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "bfs prepare");
        return CBM_STORE_ERR;
    }

    /* Bind edge type parameters */
    if (edge_type_count > 0) {
        /* MERGE: fork delta — bind loop capped at bfs_et_count (max 16) */
        for (int i = 0; i < bfs_et_count; i++) {
            bind_text(stmt, i + SKIP_ONE, edge_types[i]);
        }
    } else {
        /* Default: only "CALLS" edges */
        bind_text(stmt, SKIP_ONE, default_edge_type);
    }

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_node_hop_t *visited = malloc(cap * sizeof(cbm_node_hop_t));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
            visited = safe_realloc(visited, cap * sizeof(cbm_node_hop_t));
        }
        scan_node(stmt, &visited[n].node);
        visited[n].hop = sqlite3_column_int(stmt, ST_COL_9);
        visited[n].pagerank_score = sqlite3_column_double(stmt, ST_COL_10);
        n++;
    }

    sqlite3_finalize(stmt);

    out->visited = visited;
    out->visited_count = n;

    /* Collect edges between visited nodes (including root) */
    if (n > 0) {
        /* MERGE: fork delta — inline edge collection with LinkRank ordering.
         * Upstream factored this into bfs_collect_edges() but that helper lacks
         * the LEFT JOIN linkrank / ORDER BY lr_rank DESC that ranks edges by
         * LinkRank. Kept inline here to preserve fork's edge ranking delta.
         * Uses bfs_et_count (capped at 16) for consistent bind/clause matching. */
        /* Build ID set: root + all visited */
        char id_set[CBM_SZ_4K];
        int ilen = snprintf(id_set, sizeof(id_set), "%lld", (long long)start_id);
        if (ilen >= (int)sizeof(id_set)) {
            ilen = (int)sizeof(id_set) - 1;
        }
        for (int i = 0; i < n; i++) {
            ilen += snprintf(id_set + ilen, sizeof(id_set) - (size_t)ilen, ",%lld",
                             (long long)out->visited[i].node.id);
            if (ilen >= (int)sizeof(id_set)) {
                ilen = (int)sizeof(id_set) - 1;
            }
        }

        /* Build edge query using the same ?N placeholders for edge types.
         * types_clause already contains "?1,?2,..." — safe placeholder string. */
        char edge_sql[ST_SQL_BUF];
        const char *linkrank_select =
            use_linkrank ? "COALESCE(lr.rank, 0.0) AS lr_rank " : "0.0 AS lr_rank ";
        const char *linkrank_join = use_linkrank ? "LEFT JOIN linkrank lr ON lr.edge_id = e.id " : "";
        const char *linkrank_order = use_linkrank ? "lr_rank DESC" : "n1.name, n2.name, e.type";
        snprintf(edge_sql, sizeof(edge_sql),
                 "SELECT n1.name, n2.name, e.type, "
                 "%s"
                 "FROM edges e "
                 "JOIN nodes n1 ON n1.id = e.source_id "
                 "JOIN nodes n2 ON n2.id = e.target_id "
                 "%s"
                 "WHERE e.source_id IN (%s) AND e.target_id IN (%s) "
                 "AND e.type IN (%s) "
                 "ORDER BY %s",
                 linkrank_select, linkrank_join, id_set, id_set, types_clause, linkrank_order);

        sqlite3_stmt *estmt = NULL;
        rc = sqlite3_prepare_v2(s->db, edge_sql, CBM_NOT_FOUND, &estmt, NULL);
        if (rc == SQLITE_OK) {
            /* Bind edge type parameters for the edge query */
            if (edge_type_count > 0) {
                for (int i = 0; i < bfs_et_count; i++) {
                    bind_text(estmt, i + SKIP_ONE, edge_types[i]);
                }
            } else {
                bind_text(estmt, SKIP_ONE, default_edge_type);
            }

            int ecap = ST_INIT_CAP_8;
            int en = 0;
            cbm_edge_info_t *edges = malloc(ecap * sizeof(cbm_edge_info_t));

            while (sqlite3_step(estmt) == SQLITE_ROW) {
                if (en >= ecap) {
                    ecap *= ST_GROWTH;
                    edges = safe_realloc(edges, ecap * sizeof(cbm_edge_info_t));
                }
                edges[en].from_name = heap_strdup((const char *)sqlite3_column_text(estmt, 0));
                edges[en].to_name = heap_strdup((const char *)sqlite3_column_text(estmt, 1));
                edges[en].type = heap_strdup((const char *)sqlite3_column_text(estmt, 2));
                edges[en].confidence = sqlite3_column_double(estmt, 3);
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
    safe_str_free(&out->root.project);
    safe_str_free(&out->root.label);
    safe_str_free(&out->root.name);
    safe_str_free(&out->root.qualified_name);
    safe_str_free(&out->root.file_path);
    safe_str_free(&out->root.properties_json);

    /* Free visited */
    for (int i = 0; i < out->visited_count; i++) {
        cbm_node_hop_t *h = &out->visited[i];
        safe_str_free(&h->node.project);
        safe_str_free(&h->node.label);
        safe_str_free(&h->node.name);
        safe_str_free(&h->node.qualified_name);
        safe_str_free(&h->node.file_path);
        safe_str_free(&h->node.properties_json);
    }
    free(out->visited);

    /* Free edges */
    for (int i = 0; i < out->edge_count; i++) {
        safe_str_free(&out->edges[i].from_name);
        safe_str_free(&out->edges[i].to_name);
        safe_str_free(&out->edges[i].type);
    }
    free(out->edges);

    memset(out, 0, sizeof(*out));
}

/* ── Impact analysis ────────────────────────────────────────────── */

cbm_risk_level_t cbm_hop_to_risk(int hop) {
    switch (hop) {
    case SKIP_ONE:
        return CBM_RISK_CRITICAL;
    case ST_COL_2:
        return CBM_RISK_HIGH;
    case ST_COL_3:
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
        int found = ST_FOUND;
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

enum { SCHEMA_MAX_JSON_KEYS = 50 };

/* Discover distinct JSON property keys for a table/column via json_each().
 * Prepends base_cols, then appends up to SCHEMA_MAX_JSON_KEYS from the query.
 * Caller must free the returned array and each string in it. */
static void schema_discover_props(sqlite3 *db, const char *sql, const char *project,
                                  const char *filter, const char **base_cols, int base_col_count,
                                  char ***out_props, int *out_count) {
    int pcap = base_col_count + SCHEMA_MAX_JSON_KEYS;
    char **props = malloc(pcap * sizeof(char *));
    int pn = 0;

    for (int b = 0; b < base_col_count; b++) {
        props[pn++] = heap_strdup(base_cols[b]);
    }

    sqlite3_stmt *pstmt = NULL;
    if (sqlite3_prepare_v2(db, sql, CBM_NOT_FOUND, &pstmt, NULL) == SQLITE_OK) {
        bind_text(pstmt, SKIP_ONE, project);
        bind_text(pstmt, PAIR_LEN, filter);
        while (sqlite3_step(pstmt) == SQLITE_ROW && pn < pcap) {
            props[pn++] = heap_strdup((const char *)sqlite3_column_text(pstmt, 0));
        }
        sqlite3_finalize(pstmt);
    }

    *out_props = props;
    *out_count = pn;
}

/* Path scoping for architecture/schema summaries. Paths are relative prefixes:
 * "src/foo", "./src/foo/", and "/src//foo" all normalize to "src/foo". */
static bool arch_path_is_set(const char *path) {
    if (!path) {
        return false;
    }
    while (*path == ' ' || *path == '\t' || *path == '\n' || *path == '\r') {
        path++;
    }
    return path[0] != '\0';
}

static bool arch_path_prepare(const char *path, char *norm_out, size_t norm_sz, char *like_out,
                              size_t like_sz) {
    if (!norm_out || norm_sz == 0 || !like_out || like_sz == 0 || !arch_path_is_set(path)) {
        return false;
    }
    while (*path == ' ' || *path == '\t' || *path == '\n' || *path == '\r') {
        path++;
    }
    while (path[0] == '.' && (path[1] == '/' || path[1] == '\\')) {
        path += SLEN("./");
    }
    while (*path == '/' || *path == '\\') {
        path++;
    }
    if (path[0] == '\0') {
        return false;
    }

    int n = snprintf(norm_out, norm_sz, "%s", path);
    if (n <= 0 || (size_t)n >= norm_sz) {
        norm_out[0] = '\0';
        return false;
    }

    size_t len = strlen(norm_out);
    while (len > 0 &&
           (norm_out[len - 1] == ' ' || norm_out[len - 1] == '\t' ||
            norm_out[len - 1] == '/' || norm_out[len - 1] == '\\')) {
        norm_out[--len] = '\0';
    }

    size_t w = 0;
    for (size_t r = 0; norm_out[r] != '\0'; r++) {
        char ch = norm_out[r] == '\\' ? '/' : norm_out[r];
        if (ch == '/' && w > 0 && norm_out[w - 1] == '/') {
            continue;
        }
        norm_out[w++] = ch;
    }
    norm_out[w] = '\0';
    if (norm_out[0] == '\0') {
        return false;
    }

    n = snprintf(like_out, like_sz, "%s/%%", norm_out);
    if (n <= 0 || (size_t)n >= like_sz) {
        like_out[0] = '\0';
        return false;
    }
    return true;
}

static const char *arch_path_scope_sql(void) {
    return " AND (file_path = ? OR file_path LIKE ?)";
}

static void arch_bind_path_scope(sqlite3_stmt *stmt, int exact_idx, int like_idx, const char *norm,
                                 const char *like_pat) {
    bind_text(stmt, exact_idx, norm);
    bind_text(stmt, like_idx, like_pat);
}

bool cbm_store_arch_path_scoped(const char *path) {
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    return arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
}

bool cbm_store_normalize_arch_path(const char *path, char *norm_out, size_t norm_sz) {
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    return arch_path_prepare(path, norm_out, norm_sz, like, sizeof(like));
}

int cbm_store_count_nodes_scoped(cbm_store_t *s, const char *project, const char *path) {
    if (!s || !s->db || !project) {
        return 0;
    }
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    if (!arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like))) {
        return cbm_store_count_nodes(s, project);
    }
    const char *sql = "SELECT COUNT(*) FROM nodes WHERE project = ?1 "
                      "AND (file_path = ?2 OR file_path LIKE ?3);";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    int count = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

int cbm_store_count_edges_scoped(cbm_store_t *s, const char *project, const char *path) {
    if (!s || !s->db || !project) {
        return 0;
    }
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    if (!arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like))) {
        return cbm_store_count_edges(s, project);
    }
    const char *sql =
        "SELECT COUNT(*) FROM edges e WHERE e.project = ?1 "
        "AND EXISTS (SELECT 1 FROM nodes ns WHERE ns.id = e.source_id AND ns.project = ?1 "
        "AND (ns.file_path = ?2 OR ns.file_path LIKE ?3)) "
        "AND EXISTS (SELECT 1 FROM nodes nt WHERE nt.id = e.target_id AND nt.project = ?1 "
        "AND (nt.file_path = ?2 OR nt.file_path LIKE ?3));";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return CBM_STORE_ERR;
    }
    bind_text(stmt, ST_COL_1, project);
    arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    int count = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

/* with_props=false skips the per-label/per-type JSON property-key discovery:
 * those json_each() scans walk EVERY row of each label/type (minutes-scale on
 * multi-million-node graphs) and get_architecture only needs the counts. */
static int get_schema_impl(cbm_store_t *s, const char *project, cbm_schema_info_t *out,
                           bool with_props) {
    memset(out, 0, sizeof(*out));
    if (!s || !s->db) {
        return CBM_NOT_FOUND;
    }

    /* Node labels */
    {
        const char *sql = "SELECT label, COUNT(*) FROM nodes WHERE project = ?1 GROUP BY label "
                          "ORDER BY COUNT(*) DESC;";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
            if (stmt) {
                sqlite3_finalize(stmt);
            }
            return CBM_NOT_FOUND;
        }
        bind_text(stmt, SKIP_ONE, project);

        int cap = ST_INIT_CAP_8;
        int n = 0;
        cbm_label_count_t *arr = malloc(cap * sizeof(cbm_label_count_t));
        if (!arr) {
            sqlite3_finalize(stmt);
            return CBM_NOT_FOUND;
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (n >= cap) {
                int new_cap = cap * ST_GROWTH;
                void *tmp = realloc(arr, new_cap * sizeof(cbm_label_count_t));
                if (!tmp) {
                    for (int i = 0; i < n; i++) {
                        safe_str_free(&arr[i].label);
                    }
                    free(arr);
                    sqlite3_finalize(stmt);
                    return CBM_NOT_FOUND;
                }
                arr = tmp;
                cap = new_cap;
            }
            arr[n].label = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
            arr[n].count = sqlite3_column_int(stmt, SKIP_ONE);
            arr[n].properties = NULL;
            arr[n].property_count = 0;
            n++;
        }
        sqlite3_finalize(stmt);
        out->node_labels = arr;
        out->node_label_count = n;
    }

    /* Node label property keys: base columns + distinct JSON property keys per label */
    if (with_props) {
        static const char *node_base_cols[] = {"name", "qualified_name", "file_path", "start_line",
                                               "end_line"};
        const char *prop_sql = "SELECT DISTINCT je.key "
                               "FROM nodes, json_each(nodes.properties) AS je "
                               "WHERE nodes.project = ?1 AND nodes.label = ?2 "
                               "  AND nodes.properties != '{}' "
                               "ORDER BY je.key "
                               "LIMIT 50;";

        for (int i = 0; i < out->node_label_count; i++) {
            schema_discover_props(
                s->db, prop_sql, project, out->node_labels[i].label, node_base_cols,
                (int)(sizeof(node_base_cols) / sizeof(node_base_cols[0])),
                &out->node_labels[i].properties, &out->node_labels[i].property_count);
        }
    }

    /* Edge types */
    {
        const char *sql = "SELECT type, COUNT(*) FROM edges WHERE project = ?1 GROUP BY type ORDER "
                          "BY COUNT(*) DESC;";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
            if (stmt) {
                sqlite3_finalize(stmt);
            }
            cbm_store_schema_free(out);
            return CBM_NOT_FOUND;
        }
        bind_text(stmt, SKIP_ONE, project);

        int cap = ST_INIT_CAP_8;
        int n = 0;
        cbm_type_count_t *arr = malloc(cap * sizeof(cbm_type_count_t));
        if (!arr) {
            sqlite3_finalize(stmt);
            cbm_store_schema_free(out);
            return CBM_NOT_FOUND;
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (n >= cap) {
                int new_cap = cap * ST_GROWTH;
                void *tmp = realloc(arr, new_cap * sizeof(cbm_type_count_t));
                if (!tmp) {
                    for (int i = 0; i < n; i++) {
                        safe_str_free(&arr[i].type);
                    }
                    free(arr);
                    sqlite3_finalize(stmt);
                    cbm_store_schema_free(out);
                    return CBM_NOT_FOUND;
                }
                arr = tmp;
                cap = new_cap;
            }
            arr[n].type = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
            arr[n].count = sqlite3_column_int(stmt, SKIP_ONE);
            arr[n].properties = NULL;
            arr[n].property_count = 0;
            n++;
        }
        sqlite3_finalize(stmt);
        out->edge_types = arr;
        out->edge_type_count = n;
    }

    /* Edge type property keys: base columns + distinct JSON property keys per type */
    if (with_props) {
        static const char *edge_base_cols[] = {"source_id", "target_id"};
        const char *prop_sql = "SELECT DISTINCT je.key "
                               "FROM edges, json_each(edges.properties) AS je "
                               "WHERE edges.project = ?1 AND edges.type = ?2 "
                               "  AND edges.properties != '{}' "
                               "ORDER BY je.key "
                               "LIMIT 50;";

        for (int i = 0; i < out->edge_type_count; i++) {
            schema_discover_props(s->db, prop_sql, project, out->edge_types[i].type, edge_base_cols,
                                  (int)(sizeof(edge_base_cols) / sizeof(edge_base_cols[0])),
                                  &out->edge_types[i].properties,
                                  &out->edge_types[i].property_count);
        }
    }

    return CBM_STORE_OK;
}

int cbm_store_get_schema(cbm_store_t *s, const char *project, cbm_schema_info_t *out) {
    return get_schema_impl(s, project, out, true);
}

int cbm_store_get_schema_counts(cbm_store_t *s, const char *project, cbm_schema_info_t *out) {
    return get_schema_impl(s, project, out, false);
}

int cbm_store_get_schema_counts_scoped(cbm_store_t *s, const char *project, const char *path,
                                       cbm_schema_info_t *out) {
    memset(out, 0, sizeof(*out));
    if (!s || !s->db) {
        return CBM_NOT_FOUND;
    }
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    if (!scoped) {
        return get_schema_impl(s, project, out, false);
    }

    char sqlbuf[ST_SQL_BUF];
    {
        const char *base = "SELECT label, COUNT(*) FROM nodes WHERE project = ?1";
        int nsql = snprintf(sqlbuf, sizeof(sqlbuf), "%s%s GROUP BY label ORDER BY COUNT(*) DESC;",
                            base, arch_path_scope_sql());
        if (nsql <= 0 || (size_t)nsql >= sizeof(sqlbuf)) {
            return CBM_NOT_FOUND;
        }
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(s->db, sqlbuf, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
            if (stmt) {
                sqlite3_finalize(stmt);
            }
            return CBM_NOT_FOUND;
        }
        bind_text(stmt, SKIP_ONE, project);
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);

        int cap = ST_INIT_CAP_8;
        int count = 0;
        cbm_label_count_t *arr = malloc((size_t)cap * sizeof(cbm_label_count_t));
        if (!arr) {
            sqlite3_finalize(stmt);
            return CBM_NOT_FOUND;
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (count >= cap) {
                int new_cap = cap * ST_GROWTH;
                void *tmp = realloc(arr, (size_t)new_cap * sizeof(cbm_label_count_t));
                if (!tmp) {
                    for (int i = 0; i < count; i++) {
                        safe_str_free(&arr[i].label);
                    }
                    free(arr);
                    sqlite3_finalize(stmt);
                    return CBM_NOT_FOUND;
                }
                arr = tmp;
                cap = new_cap;
            }
            arr[count].label = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
            arr[count].count = sqlite3_column_int(stmt, SKIP_ONE);
            arr[count].properties = NULL;
            arr[count].property_count = 0;
            count++;
        }
        sqlite3_finalize(stmt);
        out->node_labels = arr;
        out->node_label_count = count;
    }

    {
        const char *sql =
            "SELECT e.type, COUNT(*) FROM edges e WHERE e.project = ?1 "
            "AND EXISTS (SELECT 1 FROM nodes ns WHERE ns.id = e.source_id AND ns.project = ?1 "
            "AND (ns.file_path = ?2 OR ns.file_path LIKE ?3)) "
            "AND EXISTS (SELECT 1 FROM nodes nt WHERE nt.id = e.target_id AND nt.project = ?1 "
            "AND (nt.file_path = ?2 OR nt.file_path LIKE ?3)) "
            "GROUP BY e.type ORDER BY COUNT(*) DESC;";
        sqlite3_stmt *stmt = NULL;
        if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
            if (stmt) {
                sqlite3_finalize(stmt);
            }
            cbm_store_schema_free(out);
            return CBM_NOT_FOUND;
        }
        bind_text(stmt, SKIP_ONE, project);
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);

        int cap = ST_INIT_CAP_8;
        int count = 0;
        cbm_type_count_t *arr = malloc((size_t)cap * sizeof(cbm_type_count_t));
        if (!arr) {
            sqlite3_finalize(stmt);
            cbm_store_schema_free(out);
            return CBM_NOT_FOUND;
        }
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (count >= cap) {
                int new_cap = cap * ST_GROWTH;
                void *tmp = realloc(arr, (size_t)new_cap * sizeof(cbm_type_count_t));
                if (!tmp) {
                    for (int i = 0; i < count; i++) {
                        safe_str_free(&arr[i].type);
                    }
                    free(arr);
                    sqlite3_finalize(stmt);
                    cbm_store_schema_free(out);
                    return CBM_NOT_FOUND;
                }
                arr = tmp;
                cap = new_cap;
            }
            arr[count].type = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
            arr[count].count = sqlite3_column_int(stmt, SKIP_ONE);
            arr[count].properties = NULL;
            arr[count].property_count = 0;
            count++;
        }
        sqlite3_finalize(stmt);
        out->edge_types = arr;
        out->edge_type_count = count;
    }

    return CBM_STORE_OK;
}

void cbm_store_schema_free(cbm_schema_info_t *out) {
    if (!out) {
        return;
    }
    for (int i = 0; i < out->node_label_count; i++) {
        safe_str_free(&out->node_labels[i].label);
        for (int j = 0; j < out->node_labels[i].property_count; j++) {
            free(out->node_labels[i].properties[j]);
        }
        free(out->node_labels[i].properties);
    }
    free(out->node_labels);

    for (int i = 0; i < out->edge_type_count; i++) {
        safe_str_free(&out->edge_types[i].type);
        for (int j = 0; j < out->edge_types[i].property_count; j++) {
            free(out->edge_types[i].properties[j]);
        }
        free(out->edge_types[i].properties);
    }
    free(out->edge_types);

    for (int i = 0; i < out->rel_pattern_count; i++) {
        safe_str_free(&out->rel_patterns[i]);
    }
    free(out->rel_patterns);

    for (int i = 0; i < out->sample_func_count; i++) {
        safe_str_free(&out->sample_func_names[i]);
    }
    free(out->sample_func_names);

    for (int i = 0; i < out->sample_class_count; i++) {
        safe_str_free(&out->sample_class_names[i]);
    }
    free(out->sample_class_names);

    for (int i = 0; i < out->sample_qn_count; i++) {
        safe_str_free(&out->sample_qns[i]);
    }
    free(out->sample_qns);

    memset(out, 0, sizeof(*out));
}

/* ── Architecture helpers ───────────────────────────────────────── */

/* Extract sub-package from QN: project.dir1.dir2.sym → dir1 (4+ parts → [2], else [1]) */
const char *cbm_qn_to_package(const char *qn) {
    if (!qn || !qn[0]) {
        return "";
    }
    static CBM_TLS char buf[CBM_SZ_256];
    /* Find dots and extract segment */
    const char *dots[ST_QN_MAX_DOTS] = {NULL};
    int ndots = 0;
    for (const char *p = qn; *p && ndots < ST_QN_MAX_DOTS; p++) {
        if (*p == '.') {
            dots[ndots++] = p;
        }
    }
    /* 4+ segments: return segment[2] */
    if (ndots >= ST_QN_MIN_DOTS) {
        const char *start = dots[SKIP_ONE] + SKIP_ONE;
        int len = (int)(dots[ST_COL_2] - start);
        if (len > 0 && len < (int)sizeof(buf)) {
            memcpy(buf, start, len);
            buf[len] = '\0';
            return buf;
        }
    }
    /* 2+ segments: return segment[1] */
    if (ndots >= SKIP_ONE) {
        const char *start = dots[0] + SKIP_ONE;
        const char *end = (ndots >= ST_COL_2) ? dots[SKIP_ONE] : qn + strlen(qn);
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
    static CBM_TLS char buf[CBM_SZ_256];
    const char *first_dot = strchr(qn, '.');
    if (!first_dot) {
        return "";
    }
    const char *start = first_dot + SKIP_ONE;
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
    if (!fp || fp[0] == '\0') {
        return false;
    }
    return strstr(fp, "test") != NULL;
}

/* File extension → language name mapping (table-driven) */
typedef struct {
    const char *ext;
    const char *lang;
} ext_lang_entry_t;

static const ext_lang_entry_t ext_lang_table[] = {
    {".py", "Python"},     {".go", "Go"},          {".js", "JavaScript"}, {".jsx", "JavaScript"},
    {".ts", "TypeScript"}, {".tsx", "TypeScript"}, {".rs", "Rust"},       {".java", "Java"},
    {".cpp", "C++"},       {".cc", "C++"},         {".cxx", "C++"},       {".c", "C"},
    {".h", "C"},           {".cs", "C#"},          {".php", "PHP"},       {".lua", "Lua"},
    {".scala", "Scala"},   {".kt", "Kotlin"},      {".rb", "Ruby"},       {".sh", "Bash"},
    {".bash", "Bash"},     {".zig", "Zig"},        {".ex", "Elixir"},     {".exs", "Elixir"},
    {".hs", "Haskell"},    {".ml", "OCaml"},       {".mli", "OCaml"},     {".html", "HTML"},
    {".css", "CSS"},       {".yaml", "YAML"},      {".yml", "YAML"},      {".toml", "TOML"},
    {".hcl", "HCL"},       {".tf", "HCL"},         {".sql", "SQL"},       {".erl", "Erlang"},
    {".swift", "Swift"},   {".dart", "Dart"},      {".groovy", "Groovy"}, {".pl", "Perl"},
    {".r", "R"},           {".scss", "SCSS"},      {".vue", "Vue"},       {".svelte", "Svelte"},
    {NULL, NULL},
};

static const char *ext_to_lang(const char *ext) {
    if (!ext) {
        return NULL;
    }
    for (const ext_lang_entry_t *e = ext_lang_table; e->ext; e++) {
        if (strcmp(ext, e->ext) == 0) {
            return e->lang;
        }
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
    static CBM_TLS char buf[CBM_SZ_16];
    int len = (int)strlen(dot);
    if (len >= (int)sizeof(buf)) {
        return NULL;
    }
    for (int i = 0; i < len; i++) {
        buf[i] = (char)((dot[i] >= 'A' && dot[i] <= 'Z') ? dot[i] + CBM_SZ_32 : dot[i]);
    }
    buf[len] = '\0';
    return buf;
}

/* ── Architecture aspect implementations ───────────────────────── */

static int arch_grow_array(cbm_store_t *s, void **items, int *cap, size_t item_size,
                           const char *op) {
    if (!items || !cap || item_size == 0 || *cap <= 0 || *cap > INT_MAX / ST_GROWTH) {
        store_set_error(s, op);
        return CBM_STORE_ERR;
    }
    int old_cap = *cap;
    int new_cap = old_cap * ST_GROWTH;
    void *next = realloc(*items, (size_t)new_cap * item_size);
    if (!next) {
        store_set_error(s, op);
        return CBM_STORE_ERR;
    }
    memset((char *)next + ((size_t)old_cap * item_size), 0,
           (size_t)(new_cap - old_cap) * item_size);
    *items = next;
    *cap = new_cap;
    return CBM_STORE_OK;
}

static void arch_free_packages(cbm_package_summary_t *items, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&items[i].name);
    }
    free(items);
}

static void arch_free_entry_points(cbm_entry_point_t *items, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&items[i].name);
        safe_str_free(&items[i].qualified_name);
        safe_str_free(&items[i].file);
    }
    free(items);
}

static void arch_free_routes(cbm_route_info_t *items, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&items[i].method);
        safe_str_free(&items[i].path);
        safe_str_free(&items[i].handler);
    }
    free(items);
}

static void arch_free_hotspots(cbm_hotspot_t *items, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&items[i].name);
        safe_str_free(&items[i].qualified_name);
    }
    free(items);
}

static void arch_free_boundaries(cbm_cross_pkg_boundary_t *items, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&items[i].from);
        safe_str_free(&items[i].to);
    }
    free(items);
}

static void arch_free_layers(cbm_package_layer_t *items, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&items[i].name);
        safe_str_free(&items[i].layer);
        safe_str_free(&items[i].reason);
    }
    free(items);
}

static void arch_clear_cluster(cbm_cluster_info_t *item) {
    if (!item) {
        return;
    }
    safe_str_free(&item->label);
    for (int j = 0; j < item->top_node_count; j++) {
        safe_str_free(&item->top_nodes[j]);
    }
    free(item->top_nodes);
    for (int j = 0; j < item->package_count; j++) {
        safe_str_free(&item->packages[j]);
    }
    free(item->packages);
    for (int j = 0; j < item->edge_type_count; j++) {
        safe_str_free(&item->edge_types[j]);
    }
    free(item->edge_types);
    memset(item, 0, sizeof(*item));
}

static void arch_free_clusters(cbm_cluster_info_t *items, int count) {
    for (int i = 0; i < count; i++) {
        arch_clear_cluster(&items[i]);
    }
    free(items);
}

static int arch_languages(cbm_store_t *s, const char *project, const char *path,
                          cbm_architecture_info_t *out) {
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char sqlbuf[ST_SQL_BUF];
    const char *base = "SELECT file_path FROM nodes WHERE project=?1 AND label='File'";
    int nsql = scoped ? snprintf(sqlbuf, sizeof(sqlbuf), "%s%s", base, arch_path_scope_sql())
                      : snprintf(sqlbuf, sizeof(sqlbuf), "%s", base);
    if (nsql <= 0 || (size_t)nsql >= sizeof(sqlbuf)) {
        store_set_error(s, "arch_languages SQL truncated");
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sqlbuf, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_languages");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    }

    /* Count per language using a simple parallel array */
    const char *lang_names[CBM_SZ_64];
    int lang_counts[CBM_SZ_64];
    int nlang = 0;

    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *fp = (const char *)sqlite3_column_text(stmt, 0);
        const char *ext = file_ext(fp);
        const char *lang = ext_to_lang(ext);
        if (!lang) {
            continue;
        }
        int found = ST_FOUND;
        for (int i = 0; i < nlang; i++) {
            if (strcmp(lang_names[i], lang) == 0) {
                found = i;
                break;
            }
        }
        if (found >= 0) {
            lang_counts[found]++;
        } else if (nlang < CBM_SZ_64) {
            lang_names[nlang] = lang;
            lang_counts[nlang] = SKIP_ONE;
            nlang++;
        }
    }
    if (step_rc != SQLITE_DONE) {
        store_set_error_sqlite(s, "arch_languages");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);

    /* Sort by count descending (simple insertion sort) */
    for (int i = SKIP_ONE; i < nlang; i++) {
        int j = i;
        while (j > 0 && lang_counts[j] > lang_counts[j - SKIP_ONE]) {
            int tc = lang_counts[j];
            lang_counts[j] = lang_counts[j - SKIP_ONE];
            lang_counts[j - SKIP_ONE] = tc;
            const char *tn = lang_names[j];
            lang_names[j] = lang_names[j - SKIP_ONE];
            lang_names[j - SKIP_ONE] = tn;
            j--;
        }
    }
    if (nlang > CBM_DECIMAL_BASE) {
        nlang = ST_MAX_LANG;
    }

    cbm_language_count_t *languages =
        (nlang > 0) ? calloc((size_t)nlang, sizeof(cbm_language_count_t)) : NULL;
    if (nlang > 0 && !languages) {
        store_set_error(s, "arch_languages out of memory");
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < nlang; i++) {
        languages[i].language = heap_strdup(lang_names[i]);
        if (!languages[i].language) {
            for (int j = 0; j < i; j++) {
                safe_str_free(&languages[j].language);
            }
            free(languages);
            store_set_error(s, "arch_languages out of memory");
            return CBM_STORE_ERR;
        }
        languages[i].file_count = lang_counts[i];
    }
    out->languages = languages;
    out->language_count = nlang;
    return CBM_STORE_OK;
}

static int arch_entry_points(cbm_store_t *s, const char *project, const char *path,
                             cbm_architecture_info_t *out) {
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char sqlbuf[ST_SQL_BUF];
    const char *base = "SELECT name, qualified_name, file_path FROM nodes "
                       "WHERE project=?1 AND json_extract(properties, '$.is_entry_point') = 1 "
                       "AND (json_extract(properties, '$.is_test') IS NULL OR "
                       "json_extract(properties, '$.is_test') != 1) "
                       "AND file_path NOT LIKE '%test%'";
    int nsql = scoped ? snprintf(sqlbuf, sizeof(sqlbuf), "%s%s LIMIT ?4", base,
                                 arch_path_scope_sql())
                      : snprintf(sqlbuf, sizeof(sqlbuf), "%s LIMIT ?2", base);
    if (nsql <= 0 || (size_t)nsql >= sizeof(sqlbuf)) {
        store_set_error(s, "arch_entry_points SQL truncated");
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sqlbuf, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_entry_points");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
        sqlite3_bind_int(stmt, ST_COL_4, ST_ARCH_ENTRY_POINT_LIMIT);
    } else {
        sqlite3_bind_int(stmt, ST_COL_2, ST_ARCH_ENTRY_POINT_LIMIT);
    }

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_entry_point_t *arr = calloc((size_t)cap, sizeof(*arr));
    if (!arr) {
        sqlite3_finalize(stmt);
        store_set_error(s, "arch_entry_points out of memory");
        return CBM_STORE_ERR;
    }
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (n >= cap) {
            if (arch_grow_array(s, (void **)&arr, &cap, sizeof(*arr),
                                "arch_entry_points out of memory") != CBM_STORE_OK) {
                arch_free_entry_points(arr, n);
                sqlite3_finalize(stmt);
                return CBM_STORE_ERR;
            }
        }
        arr[n].name = heap_strdup(safe_str((const char *)sqlite3_column_text(stmt, 0)));
        arr[n].qualified_name =
            heap_strdup(safe_str((const char *)sqlite3_column_text(stmt, SKIP_ONE)));
        arr[n].file = heap_strdup(safe_str((const char *)sqlite3_column_text(stmt, CBM_SZ_2)));
        if (!arr[n].name || !arr[n].qualified_name || !arr[n].file) {
            arch_free_entry_points(arr, n + 1);
            sqlite3_finalize(stmt);
            store_set_error(s, "arch_entry_points out of memory");
            return CBM_STORE_ERR;
        }
        n++;
    }
    if (step_rc != SQLITE_DONE) {
        arch_free_entry_points(arr, n);
        store_set_error_sqlite(s, "arch_entry_points");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    out->entry_points = arr;
    out->entry_point_count = n;
    return CBM_STORE_OK;
}

/* Extract a JSON string value from a simple JSON object by key name. */
static char *extract_json_string_prop(const char *json, const char *key, int key_len) {
    if (!json) {
        return NULL;
    }
    const char *m = strstr(json, key);
    if (!m) {
        return NULL;
    }
    m = strchr(m + key_len, '"');
    if (!m) {
        return NULL;
    }
    m++;
    const char *end = strchr(m, '"');
    if (!end || end - m >= CBM_SZ_256) {
        return NULL;
    }
    char vbuf[CBM_SZ_256];
    memcpy(vbuf, m, end - m);
    vbuf[end - m] = '\0';
    return heap_strdup(vbuf);
}

static bool arch_route_should_include(const char *name, const char *qn) {
    if (!name || !name[0]) {
        return false;
    }
    bool http_like_name = name[0] == '/' || strstr(name, "://") != NULL;
    if (!http_like_name) {
        return true;
    }
    /* Absolute URL infra nodes support cross-service matching in the graph, but
     * __route__infra__ entries are endpoints from config/package metadata rather
     * than application routes. Keep code-created absolute URL routes visible:
     * many languages emit HTTP client routes as full URLs. */
    if (qn && strncmp(qn, "__route__infra__", SLEN("__route__infra__")) == 0) {
        return false;
    }
    if (qn && (strncmp(qn, "__grpc__", SLEN("__grpc__")) == 0 ||
               strncmp(qn, "__graphql__", SLEN("__graphql__")) == 0 ||
               strncmp(qn, "__trpc__", SLEN("__trpc__")) == 0)) {
        return true;
    }
    return cbm_service_pattern_is_http_route_literal(name, NULL);
}

static int arch_routes(cbm_store_t *s, const char *project, const char *path,
                       cbm_architecture_info_t *out) {
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char sql[ST_SQL_BUF];
    const char *base = "SELECT name, properties, COALESCE(file_path, ''), qualified_name FROM nodes "
                       "WHERE project=?1 AND label='Route' "
                       "AND (json_extract(properties, '$.is_test') IS NULL OR "
                       "json_extract(properties, '$.is_test') != 1) ";
    int nsql = scoped ? snprintf(sql, sizeof(sql), "%s%s LIMIT %d", base,
                                 arch_path_scope_sql(), ST_ARCH_ROUTE_SCAN_LIMIT)
                      : snprintf(sql, sizeof(sql), "%s LIMIT %d", base,
                                 ST_ARCH_ROUTE_SCAN_LIMIT);
    if (nsql <= 0 || (size_t)nsql >= sizeof(sql)) {
        store_set_error(s, "arch_routes SQL truncated");
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_routes");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    }

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_route_info_t *arr = calloc((size_t)cap, sizeof(*arr));
    if (!arr) {
        sqlite3_finalize(stmt);
        store_set_error(s, "arch_routes out of memory");
        return CBM_STORE_ERR;
    }
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        const char *props = (const char *)sqlite3_column_text(stmt, SKIP_ONE);
        const char *fp = (const char *)sqlite3_column_text(stmt, CBM_SZ_2);
        const char *qn = (const char *)sqlite3_column_text(stmt, CBM_SZ_3);
        if (cbm_is_test_file_path(fp)) {
            continue;
        }
        /* Output-level safety net for HTTP-like Route names only. Non-HTTP
         * Route classes (gRPC, GraphQL, tRPC, async topics) are legitimate
         * architecture signals even when their names are not URL paths. */
        if (!arch_route_should_include(name, qn)) {
            continue;
        }
        if (n >= ST_ARCH_ROUTE_RESULT_LIMIT) {
            break;
        }
        if (n >= cap) {
            if (arch_grow_array(s, (void **)&arr, &cap, sizeof(*arr),
                                "arch_routes out of memory") != CBM_STORE_OK) {
                arch_free_routes(arr, n);
                sqlite3_finalize(stmt);
                return CBM_STORE_ERR;
            }
        }

        arr[n].method = heap_strdup("");
        arr[n].path = heap_strdup(name);
        arr[n].handler = heap_strdup("");

        char *val;
        val = extract_json_string_prop(props, "\"method\"", ST_METHOD_PROP_LEN);
        if (val) {
            safe_str_free(&arr[n].method);
            arr[n].method = val;
        }
        val = extract_json_string_prop(props, "\"path\"", ST_PATH_PROP_LEN);
        if (val) {
            safe_str_free(&arr[n].path);
            arr[n].path = val;
        }
        val = extract_json_string_prop(props, "\"handler\"", ST_HANDLER_PROP_LEN);
        if (val) {
            safe_str_free(&arr[n].handler);
            arr[n].handler = val;
        }
        if (!arr[n].method || !arr[n].path || !arr[n].handler) {
            arch_free_routes(arr, n + 1);
            sqlite3_finalize(stmt);
            store_set_error(s, "arch_routes out of memory");
            return CBM_STORE_ERR;
        }
        n++;
    }
    if (step_rc != SQLITE_DONE && n < ST_ARCH_ROUTE_RESULT_LIMIT) {
        arch_free_routes(arr, n);
        store_set_error_sqlite(s, "arch_routes");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    out->routes = arr;
    out->route_count = n;
    return CBM_STORE_OK;
}

enum { CBM_ARCH_HOTSPOT_DEFAULT_LIMIT = 25 };

static int arch_hotspots(cbm_store_t *s, const char *project, const char *path,
                         cbm_architecture_info_t *out, int limit) {
    /* DF-1 Site 7: Use precomputed calls_in when available. HC-6: fallback to edge COUNT. */
    if (limit <= 0) limit = CBM_ARCH_HOTSPOT_DEFAULT_LIMIT;
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    bool has_degree = false;
    {
        sqlite3_stmt *chk = NULL;
        if (sqlite3_prepare_v2(s->db, "SELECT 1 FROM node_degree LIMIT 1", -1, &chk, NULL) == SQLITE_OK) {
            has_degree = (sqlite3_step(chk) == SQLITE_ROW);
            sqlite3_finalize(chk);
        }
    }
    char sql[ST_SQL_BUF];
    if (has_degree) {
        int nsql = scoped ? snprintf(sql, sizeof(sql),
            "SELECT n.name, n.qualified_name, COALESCE(nd.calls_in, 0) as fan_in "
            "FROM nodes n "
            "LEFT JOIN node_degree nd ON nd.node_id = n.id "
            "WHERE n.project=?1 AND n.label IN ('Function', 'Method') "
            "AND (json_extract(n.properties, '$.is_test') IS NULL OR "
            "json_extract(n.properties, '$.is_test') != 1) "
            "AND n.file_path NOT LIKE '%%test%%' "
            "AND (n.file_path = ?2 OR n.file_path LIKE ?3) "
            "AND COALESCE(nd.calls_in, 0) > 0 "
            "ORDER BY fan_in DESC LIMIT %d", limit)
            : snprintf(sql, sizeof(sql),
            "SELECT n.name, n.qualified_name, COALESCE(nd.calls_in, 0) as fan_in "
            "FROM nodes n "
            "LEFT JOIN node_degree nd ON nd.node_id = n.id "
            "WHERE n.project=?1 AND n.label IN ('Function', 'Method') "
            "AND (json_extract(n.properties, '$.is_test') IS NULL OR "
            "json_extract(n.properties, '$.is_test') != 1) "
            "AND n.file_path NOT LIKE '%%test%%' "
            "AND COALESCE(nd.calls_in, 0) > 0 "
            "ORDER BY fan_in DESC LIMIT %d", limit);
        if (nsql <= 0 || (size_t)nsql >= sizeof(sql)) {
            store_set_error(s, "arch_hotspots SQL truncated");
            return CBM_STORE_ERR;
        }
    } else {
        int nsql = scoped ? snprintf(sql, sizeof(sql),
            "SELECT n.name, n.qualified_name, COUNT(*) as fan_in "
            "FROM nodes n JOIN edges e ON e.target_id = n.id AND e.type = 'CALLS' "
            "WHERE n.project=?1 AND n.label IN ('Function', 'Method') "
            "AND (json_extract(n.properties, '$.is_test') IS NULL OR "
            "json_extract(n.properties, '$.is_test') != 1) "
            "AND n.file_path NOT LIKE '%%test%%' "
            "AND (n.file_path = ?2 OR n.file_path LIKE ?3) "
            "GROUP BY n.id ORDER BY fan_in DESC LIMIT %d", limit)
            : snprintf(sql, sizeof(sql),
            "SELECT n.name, n.qualified_name, COUNT(*) as fan_in "
            "FROM nodes n JOIN edges e ON e.target_id = n.id AND e.type = 'CALLS' "
            "WHERE n.project=?1 AND n.label IN ('Function', 'Method') "
            "AND (json_extract(n.properties, '$.is_test') IS NULL OR "
            "json_extract(n.properties, '$.is_test') != 1) "
            "AND n.file_path NOT LIKE '%%test%%' "
            "GROUP BY n.id ORDER BY fan_in DESC LIMIT %d", limit);
        if (nsql <= 0 || (size_t)nsql >= sizeof(sql)) {
            store_set_error(s, "arch_hotspots SQL truncated");
            return CBM_STORE_ERR;
        }
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_hotspots");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    }

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_hotspot_t *arr = calloc((size_t)cap, sizeof(*arr));
    if (!arr) {
        sqlite3_finalize(stmt);
        store_set_error(s, "arch_hotspots out of memory");
        return CBM_STORE_ERR;
    }
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (n >= cap) {
            if (arch_grow_array(s, (void **)&arr, &cap, sizeof(*arr),
                                "arch_hotspots out of memory") != CBM_STORE_OK) {
                arch_free_hotspots(arr, n);
                sqlite3_finalize(stmt);
                return CBM_STORE_ERR;
            }
        }
        arr[n].name = heap_strdup(safe_str((const char *)sqlite3_column_text(stmt, 0)));
        arr[n].qualified_name =
            heap_strdup(safe_str((const char *)sqlite3_column_text(stmt, SKIP_ONE)));
        if (!arr[n].name || !arr[n].qualified_name) {
            arch_free_hotspots(arr, n + 1);
            sqlite3_finalize(stmt);
            store_set_error(s, "arch_hotspots out of memory");
            return CBM_STORE_ERR;
        }
        arr[n].fan_in = sqlite3_column_int(stmt, CBM_SZ_2);
        n++;
    }
    if (step_rc != SQLITE_DONE) {
        arch_free_hotspots(arr, n);
        store_set_error_sqlite(s, "arch_hotspots");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    out->hotspots = arr;
    out->hotspot_count = n;
    return CBM_STORE_OK;
}

/* Look up package name for a node ID in the parallel arrays. nids must be
 * sorted ascending (the node query orders by id) — a linear scan here is
 * O(E×N) across the edge loop and spun for >10 minutes on Linux-kernel-sized
 * graphs (~1.4M defs × ~1.4M CALLS edges). */
static const char *lookup_pkg(const int64_t *nids, char **npkgs, int nn, int64_t id) {
    int lo = 0;
    int hi = nn - SKIP_ONE;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / PAIR_LEN;
        if (nids[mid] == id) {
            return npkgs[mid];
        }
        if (nids[mid] < id) {
            lo = mid + SKIP_ONE;
        } else {
            hi = mid - SKIP_ONE;
        }
    }
    return NULL;
}

static void arch_free_pkg_lookup(int64_t *nids, char **npkgs, int count) {
    if (npkgs) {
        for (int i = 0; i < count; i++) {
            free(npkgs[i]);
        }
    }
    free(nids);
    free(npkgs);
}

static void arch_free_boundary_scratch(char **bfroms, char **btos, int *bcounts, int count) {
    for (int i = 0; i < count; i++) {
        free(bfroms ? bfroms[i] : NULL);
        free(btos ? btos[i] : NULL);
    }
    free(bfroms);
    free(btos);
    free(bcounts);
}

/* Accumulate a cross-package boundary into parallel arrays. */
static int accum_boundary(const char *src_pkg, const char *tgt_pkg, char **bfroms, char **btos,
                          int *bcounts, int *bn, int bcap) {
    int found = ST_FOUND;
    for (int i = 0; i < *bn; i++) {
        if (strcmp(bfroms[i], src_pkg) == 0 && strcmp(btos[i], tgt_pkg) == 0) {
            found = i;
            break;
        }
    }
    if (found >= 0) {
        bcounts[found]++;
    } else if (*bn < bcap) {
        char *from = heap_strdup(src_pkg);
        char *to = heap_strdup(tgt_pkg);
        if (!from || !to) {
            free(from);
            free(to);
            return CBM_STORE_ERR;
        }
        bfroms[*bn] = from;
        btos[*bn] = to;
        bcounts[*bn] = SKIP_ONE;
        (*bn)++;
    }
    return CBM_STORE_OK;
}

static int arch_boundaries(cbm_store_t *s, const char *project, const char *path,
                           cbm_cross_pkg_boundary_t **out_arr, int *out_count) {
    if (!out_arr || !out_count) {
        store_set_error(s, "arch_boundaries invalid output");
        return CBM_STORE_ERR;
    }
    *out_arr = NULL;
    *out_count = 0;

    /* Build nodeID → package map. ORDER BY id so lookup_pkg can binary-search. */
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char nsqlbuf[ST_SQL_BUF];
    const char *nbase = "SELECT id, qualified_name FROM nodes WHERE project=?1 AND label IN "
                        "('Function','Method','Class')";
    int nsql = scoped ? snprintf(nsqlbuf, sizeof(nsqlbuf), "%s%s ORDER BY id", nbase,
                                 arch_path_scope_sql())
                      : snprintf(nsqlbuf, sizeof(nsqlbuf), "%s ORDER BY id", nbase);
    if (nsql <= 0 || (size_t)nsql >= sizeof(nsqlbuf)) {
        store_set_error(s, "arch_boundaries SQL truncated");
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *nstmt = NULL;
    if (sqlite3_prepare_v2(s->db, nsqlbuf, CBM_NOT_FOUND, &nstmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_boundaries_nodes");
        return CBM_STORE_ERR;
    }
    bind_text(nstmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(nstmt, ST_COL_2, ST_COL_3, norm, like);
    }

    int ncap = CBM_SZ_256;
    int nn = 0;
    int64_t *nids = malloc((size_t)ncap * sizeof(int64_t));
    char **npkgs = malloc((size_t)ncap * sizeof(char *));
    if (!nids || !npkgs) {
        arch_free_pkg_lookup(nids, npkgs, 0);
        sqlite3_finalize(nstmt);
        store_set_error(s, "arch_boundaries out of memory");
        return CBM_STORE_ERR;
    }

    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(nstmt)) == SQLITE_ROW) {
        if (nn >= ncap) {
            if (ncap > INT_MAX / ST_GROWTH) {
                arch_free_pkg_lookup(nids, npkgs, nn);
                sqlite3_finalize(nstmt);
                store_set_error(s, "arch_boundaries out of memory");
                return CBM_STORE_ERR;
            }
            int new_cap = ncap * ST_GROWTH;
            int64_t *new_ids = realloc(nids, (size_t)new_cap * sizeof(int64_t));
            if (!new_ids) {
                arch_free_pkg_lookup(nids, npkgs, nn);
                sqlite3_finalize(nstmt);
                store_set_error(s, "arch_boundaries out of memory");
                return CBM_STORE_ERR;
            }
            nids = new_ids;
            char **new_pkgs = realloc(npkgs, (size_t)new_cap * sizeof(char *));
            if (!new_pkgs) {
                arch_free_pkg_lookup(nids, npkgs, nn);
                sqlite3_finalize(nstmt);
                store_set_error(s, "arch_boundaries out of memory");
                return CBM_STORE_ERR;
            }
            npkgs = new_pkgs;
            ncap = new_cap;
        }
        nids[nn] = sqlite3_column_int64(nstmt, 0);
        const char *qn = (const char *)sqlite3_column_text(nstmt, SKIP_ONE);
        npkgs[nn] = heap_strdup(cbm_qn_to_package(qn));
        if (!npkgs[nn]) {
            arch_free_pkg_lookup(nids, npkgs, nn);
            sqlite3_finalize(nstmt);
            store_set_error(s, "arch_boundaries out of memory");
            return CBM_STORE_ERR;
        }
        nn++;
    }
    if (step_rc != SQLITE_DONE) {
        arch_free_pkg_lookup(nids, npkgs, nn);
        store_set_error_sqlite(s, "arch_boundaries_nodes");
        sqlite3_finalize(nstmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(nstmt);

    /* Scan edges, count cross-package calls */
    /* DF-1 Site 8: Include all behavioral edge types for boundary analysis */
    const char *esql = "SELECT source_id, target_id FROM edges "
                       "WHERE project=?1 AND type IN ('CALLS','HTTP_CALLS','ASYNC_CALLS')";
    sqlite3_stmt *estmt = NULL;
    if (sqlite3_prepare_v2(s->db, esql, CBM_NOT_FOUND, &estmt, NULL) != SQLITE_OK) {
        arch_free_pkg_lookup(nids, npkgs, nn);
        store_set_error_sqlite(s, "arch_boundaries_edges");
        return CBM_STORE_ERR;
    }
    bind_text(estmt, SKIP_ONE, project);

    int bcap = CBM_SZ_32;
    int bn = 0;
    char **bfroms = calloc((size_t)bcap, sizeof(char *));
    char **btos = calloc((size_t)bcap, sizeof(char *));
    int *bcounts = calloc((size_t)bcap, sizeof(int));
    if (!bfroms || !btos || !bcounts) {
        arch_free_pkg_lookup(nids, npkgs, nn);
        arch_free_boundary_scratch(bfroms, btos, bcounts, 0);
        sqlite3_finalize(estmt);
        store_set_error(s, "arch_boundaries out of memory");
        return CBM_STORE_ERR;
    }

    step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(estmt)) == SQLITE_ROW) {
        int64_t src_id = sqlite3_column_int64(estmt, 0);
        int64_t tgt_id = sqlite3_column_int64(estmt, SKIP_ONE);
        const char *src_pkg = lookup_pkg(nids, npkgs, nn, src_id);
        const char *tgt_pkg = lookup_pkg(nids, npkgs, nn, tgt_id);
        if (!src_pkg || !tgt_pkg || !src_pkg[0] || !tgt_pkg[0] || strcmp(src_pkg, tgt_pkg) == 0) {
            continue;
        }
        if (accum_boundary(src_pkg, tgt_pkg, bfroms, btos, bcounts, &bn, bcap) !=
            CBM_STORE_OK) {
            arch_free_pkg_lookup(nids, npkgs, nn);
            arch_free_boundary_scratch(bfroms, btos, bcounts, bn);
            sqlite3_finalize(estmt);
            store_set_error(s, "arch_boundaries out of memory");
            return CBM_STORE_ERR;
        }
    }
    if (step_rc != SQLITE_DONE) {
        arch_free_pkg_lookup(nids, npkgs, nn);
        arch_free_boundary_scratch(bfroms, btos, bcounts, bn);
        store_set_error_sqlite(s, "arch_boundaries_edges");
        sqlite3_finalize(estmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(estmt);
    arch_free_pkg_lookup(nids, npkgs, nn);

    /* Sort by count descending */
    for (int i = SKIP_ONE; i < bn; i++) {
        int j = i;
        while (j > 0 && bcounts[j] > bcounts[j - SKIP_ONE]) {
            int tc = bcounts[j];
            bcounts[j] = bcounts[j - SKIP_ONE];
            bcounts[j - SKIP_ONE] = tc;
            char *tf = bfroms[j];
            bfroms[j] = bfroms[j - SKIP_ONE];
            bfroms[j - SKIP_ONE] = tf;
            char *tt = btos[j];
            btos[j] = btos[j - SKIP_ONE];
            btos[j - SKIP_ONE] = tt;
            j--;
        }
    }
    if (bn > CBM_DECIMAL_BASE) {
        for (int i = ST_MAX_ITERATIONS; i < bn; i++) {
            free(bfroms[i]);
            free(btos[i]);
        }
        bn = ST_MAX_ITERATIONS;
    }

    cbm_cross_pkg_boundary_t *result =
        (bn > 0) ? calloc(bn, sizeof(cbm_cross_pkg_boundary_t)) : NULL;
    if (bn > 0 && !result) {
        arch_free_boundary_scratch(bfroms, btos, bcounts, bn);
        store_set_error(s, "arch_boundaries out of memory");
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < bn; i++) {
        result[i].from = bfroms[i];
        result[i].to = btos[i];
        result[i].call_count = bcounts[i];
    }
    free(bfroms);
    free(btos);
    free(bcounts);
    *out_arr = result;
    *out_count = bn;
    return CBM_STORE_OK;
}

#define MAX_PREVIEW_NAMES 15

/* Fallback: derive packages from QN segments when no Package nodes exist. */
static int arch_packages_from_qn(cbm_store_t *s, const char *project, const char *path,
                                 cbm_package_summary_t **out_arr, int *out_count) {
    if (!out_arr || !out_count) {
        store_set_error(s, "arch_packages_qn invalid output");
        return CBM_STORE_ERR;
    }
    *out_arr = NULL;
    *out_count = 0;

    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char qsql[ST_SQL_BUF];
    const char *base = "SELECT qualified_name FROM nodes WHERE project=?1 AND label IN "
                       "('Function','Method','Class')";
    int nsql = scoped ? snprintf(qsql, sizeof(qsql), "%s%s", base, arch_path_scope_sql())
                      : snprintf(qsql, sizeof(qsql), "%s", base);
    if (nsql <= 0 || (size_t)nsql >= sizeof(qsql)) {
        store_set_error(s, "arch_packages_qn SQL truncated");
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, qsql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_packages_qn");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    }

    char *pnames[CBM_SZ_64];
    int pcounts[CBM_SZ_64];
    int np = 0;
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(stmt, 0);
        const char *pkg = cbm_qn_to_package(qn);
        if (!pkg[0]) {
            continue;
        }
        int found = ST_FOUND;
        for (int i = 0; i < np; i++) {
            if (strcmp(pnames[i], pkg) == 0) {
                found = i;
                break;
            }
        }
        if (found >= 0) {
            pcounts[found]++;
        } else if (np < CBM_SZ_64) {
            pnames[np] = heap_strdup(pkg);
            if (!pnames[np]) {
                for (int i = 0; i < np; i++) {
                    free(pnames[i]);
                }
                sqlite3_finalize(stmt);
                store_set_error(s, "arch_packages_qn out of memory");
                return CBM_STORE_ERR;
            }
            pcounts[np] = SKIP_ONE;
            np++;
        }
    }
    if (step_rc != SQLITE_DONE) {
        for (int i = 0; i < np; i++) {
            free(pnames[i]);
        }
        store_set_error_sqlite(s, "arch_packages_qn");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);

    /* Sort by count desc */
    for (int i = SKIP_ONE; i < np; i++) {
        int j = i;
        while (j > 0 && pcounts[j] > pcounts[j - SKIP_ONE]) {
            int tc = pcounts[j];
            pcounts[j] = pcounts[j - SKIP_ONE];
            pcounts[j - SKIP_ONE] = tc;
            char *tn = pnames[j];
            pnames[j] = pnames[j - SKIP_ONE];
            pnames[j - SKIP_ONE] = tn;
            j--;
        }
    }
    if (np > MAX_PREVIEW_NAMES) {
        for (int i = MAX_PREVIEW_NAMES; i < np; i++) {
            free(pnames[i]);
        }
        np = MAX_PREVIEW_NAMES;
    }

    cbm_package_summary_t *arr = (np > 0) ? calloc(np, sizeof(cbm_package_summary_t)) : NULL;
    if (np > 0 && !arr) {
        for (int i = 0; i < np; i++) {
            free(pnames[i]);
        }
        store_set_error(s, "arch_packages_qn out of memory");
        return CBM_STORE_ERR;
    }
    for (int i = 0; i < np; i++) {
        arr[i].name = pnames[i];
        arr[i].node_count = pcounts[i];
    }
    *out_arr = arr;
    *out_count = np;
    return CBM_STORE_OK;
}

static int arch_packages(cbm_store_t *s, const char *project, const char *path,
                         cbm_architecture_info_t *out) {
    /* Try Package nodes first */
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char sql[ST_SQL_BUF];
    const char *base = "SELECT n.name, COUNT(*) as cnt FROM nodes n "
                       "WHERE n.project=?1 AND n.label='Package'";
    int nsql = scoped ? snprintf(sql, sizeof(sql),
                                 "%s AND (n.file_path = ?2 OR n.file_path LIKE ?3) "
                                 "GROUP BY n.name ORDER BY cnt DESC LIMIT 15",
                                 base)
                      : snprintf(sql, sizeof(sql),
                                 "%s GROUP BY n.name ORDER BY cnt DESC LIMIT 15", base);
    if (nsql <= 0 || (size_t)nsql >= sizeof(sql)) {
        store_set_error(s, "arch_packages SQL truncated");
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_packages");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    }

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_package_summary_t *arr = calloc((size_t)cap, sizeof(*arr));
    if (!arr) {
        sqlite3_finalize(stmt);
        store_set_error(s, "arch_packages out of memory");
        return CBM_STORE_ERR;
    }
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (n >= cap) {
            if (arch_grow_array(s, (void **)&arr, &cap, sizeof(*arr),
                                "arch_packages out of memory") != CBM_STORE_OK) {
                arch_free_packages(arr, n);
                sqlite3_finalize(stmt);
                return CBM_STORE_ERR;
            }
        }
        arr[n].name = heap_strdup(safe_str((const char *)sqlite3_column_text(stmt, 0)));
        if (!arr[n].name) {
            arch_free_packages(arr, n + 1);
            sqlite3_finalize(stmt);
            store_set_error(s, "arch_packages out of memory");
            return CBM_STORE_ERR;
        }
        arr[n].node_count = sqlite3_column_int(stmt, SKIP_ONE);
        n++;
    }
    if (step_rc != SQLITE_DONE) {
        arch_free_packages(arr, n);
        store_set_error_sqlite(s, "arch_packages");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);

    /* Fallback: group by QN segment if no Package nodes */
    if (n == 0) {
        free(arr);
        int rc = arch_packages_from_qn(s, project, path, &arr, &n);
        if (rc != CBM_STORE_OK) {
            return rc;
        }
    }

    out->packages = arr;
    out->package_count = n;
    return CBM_STORE_OK;
}

static void classify_layer(const char *pkg, int in, int out_deg, bool has_routes,
                           bool has_entry_points, const char **layer, const char **reason) {
    static CBM_TLS char reason_buf[CBM_SZ_128];
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
    if (in > out_deg && in > ST_MIN_INDEGREE) {
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

/* Find or insert a package name. A full fixed-size summary is nonfatal. */
static int find_or_add_pkg(char **all_pkgs, int *npkgs, int max_pkgs, const char *pkg,
                           int *out_idx) {
    if (!all_pkgs || !npkgs || !pkg || !out_idx) {
        return CBM_STORE_ERR;
    }
    *out_idx = CBM_STORE_NOT_FOUND;
    for (int j = 0; j < *npkgs; j++) {
        if (strcmp(all_pkgs[j], pkg) == 0) {
            *out_idx = j;
            return CBM_STORE_OK;
        }
    }
    if (*npkgs < max_pkgs) {
        int idx = *npkgs;
        all_pkgs[idx] = heap_strdup(pkg);
        if (!all_pkgs[idx]) {
            return CBM_STORE_ERR;
        }
        (*npkgs)++;
        *out_idx = idx;
        return CBM_STORE_OK;
    }
    return CBM_STORE_NOT_FOUND;
}

/* Check if a package name appears in an array. */
static bool pkg_in_list(const char *pkg, char **list, int count) {
    for (int j = 0; j < count; j++) {
        if (strcmp(pkg, list[j]) == 0) {
            return true;
        }
    }
    return false;
}

/* Collect package names from nodes matching a SQL query (must use ?1 = project). */
static int collect_pkg_names(cbm_store_t *s, const char *sql, const char *project, const char *path,
                             char **pkgs, int max_pkgs) {
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char sqlbuf[ST_SQL_BUF];
    int nsql = scoped ? snprintf(sqlbuf, sizeof(sqlbuf), "%s%s", sql, arch_path_scope_sql())
                      : snprintf(sqlbuf, sizeof(sqlbuf), "%s", sql);
    if (nsql <= 0 || (size_t)nsql >= sizeof(sqlbuf)) {
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sqlbuf, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    }
    int count = 0;
    int step_rc = SQLITE_OK;
    while (count < max_pkgs && (step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(stmt, 0);
        char *pkg = heap_strdup(cbm_qn_to_package(qn));
        if (!pkg) {
            for (int i = 0; i < count; i++) {
                free(pkgs[i]);
            }
            sqlite3_finalize(stmt);
            return CBM_STORE_ERR;
        }
        pkgs[count++] = pkg;
    }
    if (step_rc != SQLITE_DONE && count < max_pkgs) {
        for (int i = 0; i < count; i++) {
            free(pkgs[i]);
        }
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    return count;
}

static int arch_layers(cbm_store_t *s, const char *project, const char *path,
                       cbm_architecture_info_t *out) {
    out->layers = NULL;
    out->layer_count = 0;

    /* Get boundaries for fan analysis */
    cbm_cross_pkg_boundary_t *boundaries = NULL;
    int bcount = 0;
    int rc = arch_boundaries(s, project, path, &boundaries, &bcount);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    /* Collect route and entry point packages */
    char *route_pkgs[CBM_SZ_32];
    int nrpkgs =
        collect_pkg_names(s, "SELECT qualified_name FROM nodes WHERE project=?1 AND label='Route'",
                          project, path, route_pkgs, CBM_SZ_32);
    if (nrpkgs < 0) {
        arch_free_boundaries(boundaries, bcount);
        store_set_error(s, "arch_layers route package collection failed");
        return CBM_STORE_ERR;
    }

    char *entry_pkgs[CBM_SZ_32];
    int nepkgs = collect_pkg_names(s,
                                   "SELECT qualified_name FROM nodes WHERE project=?1 AND "
                                   "json_extract(properties, '$.is_entry_point') = 1",
                                   project, path, entry_pkgs, CBM_SZ_32);
    if (nepkgs < 0) {
        for (int i = 0; i < nrpkgs; i++) {
            free(route_pkgs[i]);
        }
        arch_free_boundaries(boundaries, bcount);
        store_set_error(s, "arch_layers entry package collection failed");
        return CBM_STORE_ERR;
    }

    /* Compute fan-in/out per package */
    char *all_pkgs[CBM_SZ_64];
    int fan_in[CBM_SZ_64];
    int fan_out[CBM_SZ_64];
    int npkgs = 0;
    memset(fan_in, 0, sizeof(fan_in));
    memset(fan_out, 0, sizeof(fan_out));

    for (int i = 0; i < bcount; i++) {
        int fi = CBM_STORE_NOT_FOUND;
        rc = find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, boundaries[i].from, &fi);
        if (rc == CBM_STORE_ERR) {
            goto oom;
        } else if (rc == CBM_STORE_OK && fi >= 0) {
            fan_out[fi] += boundaries[i].call_count;
        }
        int ti = CBM_STORE_NOT_FOUND;
        rc = find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, boundaries[i].to, &ti);
        if (rc == CBM_STORE_ERR) {
            goto oom;
        } else if (rc == CBM_STORE_OK && ti >= 0) {
            fan_in[ti] += boundaries[i].call_count;
        }
    }

    /* Also include route/entry packages */
    for (int i = 0; i < nrpkgs; i++) {
        int idx = CBM_STORE_NOT_FOUND;
        if (find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, route_pkgs[i], &idx) ==
            CBM_STORE_ERR) {
            goto oom;
        }
    }
    for (int i = 0; i < nepkgs; i++) {
        int idx = CBM_STORE_NOT_FOUND;
        if (find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, entry_pkgs[i], &idx) ==
            CBM_STORE_ERR) {
            goto oom;
        }
    }

    /* Classify each package */
    out->layers = (npkgs > 0) ? calloc(npkgs, sizeof(cbm_package_layer_t)) : NULL;
    if (npkgs > 0 && !out->layers) {
        goto oom;
    }
    out->layer_count = npkgs;
    for (int i = 0; i < npkgs; i++) {
        bool has_route = pkg_in_list(all_pkgs[i], route_pkgs, nrpkgs);
        bool has_entry = pkg_in_list(all_pkgs[i], entry_pkgs, nepkgs);
        const char *layer;
        const char *reason;
        classify_layer(all_pkgs[i], fan_in[i], fan_out[i], has_route, has_entry, &layer, &reason);
        out->layers[i].name = all_pkgs[i]; /* transfer ownership */
        out->layers[i].layer = heap_strdup(layer);
        out->layers[i].reason = heap_strdup(reason);
        if (!out->layers[i].layer || !out->layers[i].reason) {
            out->layer_count = i + 1;
            for (int j = i + 1; j < npkgs; j++) {
                free(all_pkgs[j]);
            }
            goto oom_after_layers;
        }
    }

    /* Sort layers by name */
    for (int i = SKIP_ONE; i < npkgs; i++) {
        int j = i;
        while (j > 0 && strcmp(out->layers[j].name, out->layers[j - SKIP_ONE].name) < 0) {
            cbm_package_layer_t tmp = out->layers[j];
            out->layers[j] = out->layers[j - SKIP_ONE];
            out->layers[j - SKIP_ONE] = tmp;
            j--;
        }
    }

    /* Cleanup */
    arch_free_boundaries(boundaries, bcount);
    for (int i = 0; i < nrpkgs; i++) {
        free(route_pkgs[i]);
    }
    for (int i = 0; i < nepkgs; i++) {
        free(entry_pkgs[i]);
    }

    return CBM_STORE_OK;

oom:
    for (int i = 0; i < npkgs; i++) {
        free(all_pkgs[i]);
    }
oom_after_layers:
    arch_free_layers(out->layers, out->layer_count);
    out->layers = NULL;
    out->layer_count = 0;
    for (int i = 0; i < nrpkgs; i++) {
        free(route_pkgs[i]);
    }
    for (int i = 0; i < nepkgs; i++) {
        free(entry_pkgs[i]);
    }
    arch_free_boundaries(boundaries, bcount);
    store_set_error(s, "arch_layers out of memory");
    return CBM_STORE_ERR;
}

/* Add a child to a dir entry if not already present. */
static int dir_add_child(char ***children, int *child_count, int *child_cap, const char *child) {
    for (int k = 0; k < *child_count; k++) {
        if (strcmp((*children)[k], child) == 0) {
            return CBM_STORE_OK;
        }
    }
    char *copy = heap_strdup(child);
    if (!copy) {
        return CBM_STORE_ERR;
    }
    if (*child_count >= *child_cap) {
        if (*child_cap > INT_MAX / PAIR_LEN) {
            free(copy);
            return CBM_STORE_ERR;
        }
        int new_cap = *child_cap ? *child_cap * PAIR_LEN : ST_INIT_CAP_4;
        char **next = realloc(*children, (size_t)new_cap * sizeof(*next));
        if (!next) {
            free(copy);
            return CBM_STORE_ERR;
        }
        *children = next;
        *child_cap = new_cap;
    }
    (*children)[(*child_count)++] = copy;
    return CBM_STORE_OK;
}

/* Find or create a directory entry by path. Returns index, CBM_STORE_ERR, or CBM_NOT_FOUND if full. */
static int dir_find_or_create(char **dir_paths, int *dir_child_counts, char ***dir_children,
                              int *dir_children_caps, int *dn, int dcap, const char *dir) {
    for (int i = 0; i < *dn; i++) {
        if (strcmp(dir_paths[i], dir) == 0) {
            return i;
        }
    }
    if (*dn < dcap) {
        int idx = *dn;
        char *path = heap_strdup(dir);
        if (!path) {
            return CBM_STORE_ERR;
        }
        dir_paths[idx] = path;
        dir_child_counts[idx] = 0;
        dir_children[idx] = NULL;
        dir_children_caps[idx] = 0;
        (*dn)++;
        return idx;
    }
    return CBM_NOT_FOUND;
}

/* Create a file tree entry by checking if a path is a file and counting dir children. */
static cbm_file_tree_entry_t make_tree_entry(const char *path, char **files, int fn,
                                             char **dir_paths, const int *dir_child_counts,
                                             int dn) {
    cbm_file_tree_entry_t e = {0};
    e.path = heap_strdup(path);
    bool is_file = false;
    for (int f = 0; f < fn; f++) {
        if (strcmp(files[f], path) == 0) {
            is_file = true;
            break;
        }
    }
    e.type = heap_strdup(is_file ? "file" : "dir");
    for (int d = 0; d < dn; d++) {
        if (strcmp(dir_paths[d], path) == 0) {
            e.children = dir_child_counts[d];
            break;
        }
    }
    return e;
}

/* Split a path by '/' into parts. Returns number of parts. */
static int split_path_parts(const char *fp, char *buf, int buf_sz, char **parts, int max_parts) {
    if (!fp || !buf || !parts || buf_sz <= 0 || max_parts <= 0) {
        return 0;
    }
    cbm_str_copy(buf, (size_t)buf_sz, fp);
    int nparts = 0;
    char *p = buf;
    parts[nparts++] = p;
    while (*p && nparts < max_parts) {
        if (*p == '/') {
            *p = '\0';
            parts[nparts++] = p + SKIP_ONE;
        }
        p++;
    }
    return nparts;
}

/* Register dir hierarchy for one file path. */
static int arch_register_file_dirs(const char *fp, char **dir_paths, int *dir_child_counts,
                                   char ***dir_children, int *dir_children_caps, int *dn,
                                   int dcap) {
    char tmp[CBM_SZ_512];
    char *parts[ST_SEARCH_MAX_BINDS];
    int nparts = split_path_parts(fp, tmp, (int)sizeof(tmp), parts, ST_SEARCH_MAX_BINDS);

    int ri = dir_find_or_create(dir_paths, dir_child_counts, dir_children, dir_children_caps, dn,
                                dcap, "");
    if (ri < 0 && *dn < dcap) {
        return CBM_STORE_ERR;
    }
    if (ri >= 0 && nparts > 0) {
        if (dir_add_child(&dir_children[ri], &dir_child_counts[ri], &dir_children_caps[ri],
                          parts[0]) != CBM_STORE_OK) {
            return CBM_STORE_ERR;
        }
    }

    for (int depth = 0; depth < nparts - SKIP_ONE && depth < ST_MAX_PATH_DEPTH; depth++) {
        char dir[CBM_SZ_512] = "";
        size_t dlen = 0;
        for (int k = 0; k <= depth; k++) {
            const char *seg = parts[k] ? parts[k] : "";
            size_t seglen = strlen(seg);
            /* Bounded append: ST_MAX_PATH_DEPTH limits component COUNT, not total
             * length — a path with >512 chars across components would overflow the
             * stack buffer via strcat. Stop appending (truncating this dir key) if
             * the next segment wouldn't fit. (#52 security sweep.) */
            size_t need = dlen + (k > 0 ? 1 : 0) + seglen;
            if (need >= sizeof(dir)) {
                break;
            }
            if (k > 0) {
                dir[dlen++] = '/';
            }
            memcpy(dir + dlen, seg, seglen);
            dlen += seglen;
            dir[dlen] = '\0';
        }
        const char *child = (depth + SKIP_ONE < nparts) ? parts[depth + SKIP_ONE] : NULL;
        if (!child) {
            continue;
        }
        int di = dir_find_or_create(dir_paths, dir_child_counts, dir_children, dir_children_caps,
                                    dn, dcap, dir);
        if (di < 0 && *dn < dcap) {
            return CBM_STORE_ERR;
        }
        if (di >= 0) {
            if (dir_add_child(&dir_children[di], &dir_child_counts[di], &dir_children_caps[di],
                              child) != CBM_STORE_OK) {
                return CBM_STORE_ERR;
            }
        }
    }
    return CBM_STORE_OK;
}

/* Count the number of '/' in a string. */
static int count_slashes(const char *s) {
    int n = 0;
    for (; *s; s++) {
        if (*s == '/') {
            n++;
        }
    }
    return n;
}

/* Push a tree entry, growing the array if needed. */
static int push_tree_entry(cbm_file_tree_entry_t **entries, int *en, int *ecap,
                           cbm_file_tree_entry_t e) {
    if (*en >= *ecap) {
        if (*ecap > INT_MAX / ST_GROWTH) {
            return CBM_STORE_ERR;
        }
        int new_cap = *ecap * ST_GROWTH;
        cbm_file_tree_entry_t *next =
            realloc(*entries, (size_t)new_cap * sizeof(cbm_file_tree_entry_t));
        if (!next) {
            return CBM_STORE_ERR;
        }
        *entries = next;
        *ecap = new_cap;
    }
    (*entries)[(*en)++] = e;
    return CBM_STORE_OK;
}

static void arch_free_tree_entries(cbm_file_tree_entry_t *entries, int count) {
    if (!entries) {
        return;
    }
    for (int j = 0; j < count; j++) {
        safe_str_free(&entries[j].path);
        safe_str_free(&entries[j].type);
    }
    free(entries);
}

/* Collect tree entries from dir arrays. */
static int arch_collect_entries(char **dir_paths, int *dir_child_counts, char ***dir_children,
                                int dn, char **files, int fn, cbm_file_tree_entry_t **entries_out,
                                int *en_out) {
    int ecap = CBM_SZ_64;
    int en = 0;
    cbm_file_tree_entry_t *entries = calloc(ecap, sizeof(cbm_file_tree_entry_t));
    if (!entries) {
        return CBM_STORE_ERR;
    }

    /* Root children */
    for (int i = 0; i < dn; i++) {
        if (strcmp(dir_paths[i], "") != 0) {
            continue;
        }
        for (int k = 0; k < dir_child_counts[i]; k++) {
            cbm_file_tree_entry_t e =
                make_tree_entry(dir_children[i][k], files, fn, dir_paths, dir_child_counts, dn);
            if (!e.path || !e.type ||
                push_tree_entry(&entries, &en, &ecap, e) != CBM_STORE_OK) {
                safe_str_free(&e.path);
                safe_str_free(&e.type);
                arch_free_tree_entries(entries, en);
                return CBM_STORE_ERR;
            }
        }
    }

    /* Non-root dir children (depth < ST_COL_3) */
    for (int i = 0; i < dn; i++) {
        if (strcmp(dir_paths[i], "") == 0 || count_slashes(dir_paths[i]) >= ST_MAX_PATH_DEPTH) {
            continue;
        }
        for (int k = 0; k < dir_child_counts[i]; k++) {
            char path[CBM_SZ_512];
            int npath = snprintf(path, sizeof(path), "%s/%s", dir_paths[i], dir_children[i][k]);
            if (npath <= 0 || (size_t)npath >= sizeof(path)) {
                arch_free_tree_entries(entries, en);
                return CBM_STORE_ERR;
            }
            cbm_file_tree_entry_t e =
                make_tree_entry(path, files, fn, dir_paths, dir_child_counts, dn);
            if (!e.path || !e.type ||
                push_tree_entry(&entries, &en, &ecap, e) != CBM_STORE_OK) {
                safe_str_free(&e.path);
                safe_str_free(&e.type);
                arch_free_tree_entries(entries, en);
                return CBM_STORE_ERR;
            }
        }
    }

    /* Sort by path */
    for (int i = SKIP_ONE; i < en; i++) {
        int j = i;
        while (j > 0 && strcmp(entries[j].path, entries[j - SKIP_ONE].path) < 0) {
            cbm_file_tree_entry_t tmp = entries[j];
            entries[j] = entries[j - SKIP_ONE];
            entries[j - SKIP_ONE] = tmp;
            j--;
        }
    }

    *entries_out = entries;
    *en_out = en;
    return CBM_STORE_OK;
}

/* Free dir arrays. */
static void arch_free_dirs(char **dir_paths, int *dir_child_counts, char ***dir_children,
                           int *dir_children_caps, int dn, char **files, int fn) {
    for (int i = 0; i < dn; i++) {
        free(dir_paths[i]);
        for (int k = 0; k < dir_child_counts[i]; k++) {
            free(dir_children[i][k]);
        }
        free(dir_children[i]);
    }
    free(dir_paths);
    free(dir_child_counts);
    free(dir_children);
    free(dir_children_caps);
    for (int i = 0; i < fn; i++) {
        free(files[i]);
    }
    free(files);
}

static int arch_file_tree(cbm_store_t *s, const char *project, const char *path,
                          cbm_architecture_info_t *out) {
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char sql[ST_SQL_BUF];
    const char *base = "SELECT file_path FROM nodes WHERE project=?1 AND label='File'";
    int nsql = scoped ? snprintf(sql, sizeof(sql), "%s%s", base, arch_path_scope_sql())
                      : snprintf(sql, sizeof(sql), "%s", base);
    if (nsql <= 0 || (size_t)nsql >= sizeof(sql)) {
        store_set_error(s, "arch_file_tree SQL truncated");
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_file_tree");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(stmt, ST_COL_2, ST_COL_3, norm, like);
    }

    int fcap = CBM_SZ_32;
    int fn = 0;
    char **files = malloc(fcap * sizeof(char *));

    int dcap = CBM_SZ_64;
    int dn = 0;
    char **dir_paths = calloc(dcap, sizeof(char *));
    int *dir_child_counts = calloc(dcap, sizeof(int));
    char ***dir_children = calloc(dcap, sizeof(char **));
    int *dir_children_caps = calloc(dcap, sizeof(int));
    int rc = CBM_STORE_ERR;
    if (!files || !dir_paths || !dir_child_counts || !dir_children || !dir_children_caps) {
        store_set_error(s, "arch_file_tree out of memory");
        goto cleanup;
    }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *fp = (const char *)sqlite3_column_text(stmt, 0);
        if (!fp) {
            continue;
        }
        if (fn >= fcap) {
            if (fcap > INT_MAX / ST_GROWTH) {
                store_set_error(s, "arch_file_tree out of memory");
                goto cleanup;
            }
            int new_cap = fcap * ST_GROWTH;
            char **next = realloc(files, (size_t)new_cap * sizeof(*next));
            if (!next) {
                store_set_error(s, "arch_file_tree out of memory");
                goto cleanup;
            }
            files = next;
            fcap = new_cap;
        }
        char *file_path = heap_strdup(fp);
        if (!file_path) {
            store_set_error(s, "arch_file_tree out of memory");
            goto cleanup;
        }
        files[fn++] = file_path;
        if (arch_register_file_dirs(fp, dir_paths, dir_child_counts, dir_children, dir_children_caps,
                                    &dn, dcap) != CBM_STORE_OK) {
            store_set_error(s, "arch_file_tree out of memory");
            goto cleanup;
        }
    }
    sqlite3_finalize(stmt);
    stmt = NULL;

    if (arch_collect_entries(dir_paths, dir_child_counts, dir_children, dn, files, fn,
                             &out->file_tree, &out->file_tree_count) != CBM_STORE_OK) {
        store_set_error(s, "arch_file_tree out of memory");
        goto cleanup;
    }

    rc = CBM_STORE_OK;

cleanup:
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    arch_free_dirs(dir_paths, dir_child_counts, dir_children, dir_children_caps, dn, files, fn);
    return rc;
}

/* ── Louvain community detection ───────────────────────────────── */

/* Build deduplicated, normalized edge weight arrays from raw edges.
 * Returns total number of unique edges in *out_wn. */
typedef struct {
    int64_t id;
    int index;
} cbm_louvain_node_ref_t;

typedef struct {
    int src;
    int dst;
} cbm_louvain_pair_t;

static int louvain_node_ref_cmp(const void *a, const void *b) {
    const cbm_louvain_node_ref_t *ra = a;
    const cbm_louvain_node_ref_t *rb = b;
    if (ra->id != rb->id) {
        return (ra->id > rb->id) - (ra->id < rb->id);
    }
    return (ra->index > rb->index) - (ra->index < rb->index);
}

static int louvain_node_ref_key_cmp(const void *key, const void *el) {
    int64_t id = *(const int64_t *)key;
    const cbm_louvain_node_ref_t *ref = el;
    return (id > ref->id) - (id < ref->id);
}

static int louvain_pair_cmp(const void *a, const void *b) {
    const cbm_louvain_pair_t *pa = a;
    const cbm_louvain_pair_t *pb = b;
    if (pa->src != pb->src) {
        return (pa->src > pb->src) - (pa->src < pb->src);
    }
    return (pa->dst > pb->dst) - (pa->dst < pb->dst);
}

static int louvain_node_index(const cbm_louvain_node_ref_t *refs, int n, int64_t id) {
    const cbm_louvain_node_ref_t *hit =
        bsearch(&id, refs, (size_t)n, sizeof(cbm_louvain_node_ref_t), louvain_node_ref_key_cmp);
    if (!hit) {
        return CBM_NOT_FOUND;
    }
    while (hit > refs && (hit - 1)->id == id) {
        hit--;
    }
    return hit->index;
}

static void louvain_build_weights(const int64_t *nodes, int n, const cbm_louvain_edge_t *edges,
                                  int edge_count, int **out_wsi, int **out_wdi, double **out_ww,
                                  int *out_wn) {
    cbm_louvain_node_ref_t *refs = malloc((size_t)n * sizeof(cbm_louvain_node_ref_t));
    cbm_louvain_pair_t *pairs =
        malloc((size_t)(edge_count > 0 ? edge_count : SKIP_ONE) * sizeof(cbm_louvain_pair_t));
    if (!refs || !pairs) {
        free(refs);
        free(pairs);
        *out_wsi = NULL;
        *out_wdi = NULL;
        *out_ww = NULL;
        *out_wn = 0;
        return;
    }

    for (int i = 0; i < n; i++) {
        refs[i] = (cbm_louvain_node_ref_t){nodes[i], i};
    }
    qsort(refs, (size_t)n, sizeof(cbm_louvain_node_ref_t), louvain_node_ref_cmp);

    int pair_count = 0;
    for (int e = 0; e < edge_count; e++) {
        int si = louvain_node_index(refs, n, edges[e].src);
        int di = louvain_node_index(refs, n, edges[e].dst);
        if (si < 0 || di < 0 || si == di) {
            continue;
        }
        if (si > di) {
            int tmp = si;
            si = di;
            di = tmp;
        }
        pairs[pair_count++] = (cbm_louvain_pair_t){si, di};
    }
    free(refs);

    if (pair_count > 1) {
        qsort(pairs, (size_t)pair_count, sizeof(cbm_louvain_pair_t), louvain_pair_cmp);
    }

    int *wsi = malloc((size_t)(pair_count > 0 ? pair_count : SKIP_ONE) * sizeof(int));
    int *wdi = malloc((size_t)(pair_count > 0 ? pair_count : SKIP_ONE) * sizeof(int));
    double *ww = malloc((size_t)(pair_count > 0 ? pair_count : SKIP_ONE) * sizeof(double));
    if (!wsi || !wdi || !ww) {
        free(pairs);
        free(wsi);
        free(wdi);
        free(ww);
        *out_wsi = NULL;
        *out_wdi = NULL;
        *out_ww = NULL;
        *out_wn = 0;
        return;
    }

    int wn = 0;
    for (int i = 0; i < pair_count; i++) {
        if (wn > 0 && wsi[wn - SKIP_ONE] == pairs[i].src &&
            wdi[wn - SKIP_ONE] == pairs[i].dst) {
            ww[wn - SKIP_ONE] += (double)SKIP_ONE;
            continue;
        }
        wsi[wn] = pairs[i].src;
        wdi[wn] = pairs[i].dst;
        ww[wn] = (double)SKIP_ONE;
        wn++;
    }
    free(pairs);

    *out_wsi = wsi;
    *out_wdi = wdi;
    *out_ww = ww;
    *out_wn = wn;
}

enum { LEIDEN_MAX_LEVELS = 64, LEIDEN_MOVE_PASS_CAP = 100 };

/* Weighted undirected graph in CSR form. Each undirected edge is stored as
 * two directed entries; k[i] is the weighted degree of node i. Self-loops are
 * never materialised — an aggregate node's intra-community weight is folded
 * into k[i] (which is preserved across levels), while nbr/w hold only
 * inter-community edges. The modularity gain therefore uses k[] for the
 * null-model term and nbr/w for connectivity, so intra weight affects the
 * degree but is never mistaken for an edge to another community. */
typedef struct {
    int n;
    int *off;  /* CSR offsets, length n + 1 */
    int *nbr;  /* neighbour indices, length off[n] */
    double *w; /* edge weights aligned with nbr */
    double *k; /* weighted degree per node */
} cbm_lg_t;

static void lg_free(cbm_lg_t *g) {
    free(g->off);
    free(g->nbr);
    free(g->w);
    free(g->k);
    g->off = NULL;
    g->nbr = NULL;
    g->w = NULL;
    g->k = NULL;
}

/* Build a CSR graph from a deduplicated undirected edge list. */
static int lg_build(int n, const int *wsi, const int *wdi, const double *ww, int wn,
                    cbm_lg_t *out) {
    int *off = calloc((size_t)n + 1, sizeof(int));
    double *k = calloc((size_t)n, sizeof(double));
    int *fill = malloc((size_t)n * sizeof(int));
    if (!off || !k || !fill) {
        free(off);
        free(k);
        free(fill);
        return CBM_NOT_FOUND;
    }
    for (int e = 0; e < wn; e++) {
        off[wsi[e] + 1]++;
        off[wdi[e] + 1]++;
    }
    for (int i = 0; i < n; i++) {
        off[i + 1] += off[i];
    }
    int total = off[n];
    int *nbr = malloc((size_t)(total > 0 ? total : 1) * sizeof(int));
    double *w = malloc((size_t)(total > 0 ? total : 1) * sizeof(double));
    if (!nbr || !w) {
        free(off);
        free(k);
        free(fill);
        free(nbr);
        free(w);
        return CBM_NOT_FOUND;
    }
    memcpy(fill, off, (size_t)n * sizeof(int));
    for (int e = 0; e < wn; e++) {
        int a = wsi[e];
        int b = wdi[e];
        double we = ww[e];
        nbr[fill[a]] = b;
        w[fill[a]] = we;
        fill[a]++;
        nbr[fill[b]] = a;
        w[fill[b]] = we;
        fill[b]++;
        k[a] += we;
        k[b] += we;
    }
    free(fill);
    out->n = n;
    out->off = off;
    out->nbr = nbr;
    out->w = w;
    out->k = k;
    return CBM_STORE_OK;
}

/* Local-moving phase: greedily move each node to the neighbouring community
 * with the highest modularity gain, using a work queue seeded with every node
 * and re-queueing only the neighbours of a node that actually moved. Mutates
 * comm[] in place. */
static void leiden_move(const cbm_lg_t *g, int *comm, double gamma, double twom) {
    int n = g->n;
    double *stot = calloc((size_t)n, sizeof(double));
    double *acc = calloc((size_t)n, sizeof(double));
    int *queue = malloc((size_t)n * sizeof(int));
    int *dirty = malloc((size_t)n * sizeof(int));
    bool *inq = calloc((size_t)n, sizeof(bool));
    if (!stot || !acc || !queue || !dirty || !inq) {
        free(stot);
        free(acc);
        free(queue);
        free(dirty);
        free(inq);
        return;
    }
    for (int i = 0; i < n; i++) {
        stot[comm[i]] += g->k[i];
        queue[i] = i;
        inq[i] = true;
    }
    int qhead = 0;
    int qcount = n;
    long cap = (long)n * LEIDEN_MOVE_PASS_CAP + LEIDEN_MAX_LEVELS;
    while (qcount > 0 && cap-- > 0) {
        int v = queue[qhead];
        qhead = (qhead + 1) % n;
        qcount--;
        inq[v] = false;
        int cv = comm[v];
        int ndirty = 0;
        for (int e = g->off[v]; e < g->off[v + 1]; e++) {
            int u = g->nbr[e];
            if (u == v) {
                continue;
            }
            int cu = comm[u];
            if (acc[cu] == 0.0) {
                dirty[ndirty++] = cu;
            }
            acc[cu] += g->w[e];
        }
        stot[cv] -= g->k[v];
        double kv = g->k[v];
        int best_c = cv;
        double best_gain = acc[cv] - gamma * kv * stot[cv] / twom;
        for (int d = 0; d < ndirty; d++) {
            int c = dirty[d];
            double gain = acc[c] - gamma * kv * stot[c] / twom;
            if (gain > best_gain) {
                best_gain = gain;
                best_c = c;
            }
        }
        stot[best_c] += kv;
        comm[v] = best_c;
        if (best_c != cv) {
            for (int e = g->off[v]; e < g->off[v + 1]; e++) {
                int u = g->nbr[e];
                if (comm[u] != best_c && !inq[u] && qcount < n) {
                    queue[(qhead + qcount) % n] = u;
                    qcount++;
                    inq[u] = true;
                }
            }
        }
        for (int d = 0; d < ndirty; d++) {
            acc[dirty[d]] = 0.0;
        }
    }
    free(stot);
    free(acc);
    free(queue);
    free(dirty);
    free(inq);
}

/* Compact community labels in comm[] to the dense range [0, returned count). */
static int leiden_relabel(int *comm, int n) {
    int *map = malloc((size_t)n * sizeof(int));
    if (!map) {
        return n;
    }
    for (int i = 0; i < n; i++) {
        map[i] = CBM_NOT_FOUND;
    }
    int next = 0;
    for (int i = 0; i < n; i++) {
        int c = comm[i];
        if (map[c] == CBM_NOT_FOUND) {
            map[c] = next++;
        }
        comm[i] = map[c];
    }
    free(map);
    return next;
}

/* Refinement phase: within each move-phase community, merge singleton nodes
 * into the best connected sub-community (positive modularity gain, edge must
 * exist). This re-derives communities bottom-up so each one is guaranteed
 * internally connected — the defect single-level Louvain suffers from, which
 * fragments the graph into hundreds of tiny noisy clusters. Writes
 * sub-community labels into refined[] and returns their count. */
static int leiden_refine(const cbm_lg_t *g, const int *comm, double gamma, double twom,
                         int *refined) {
    int n = g->n;
    double *stot = calloc((size_t)n, sizeof(double));
    double *acc = calloc((size_t)n, sizeof(double));
    int *rsize = malloc((size_t)n * sizeof(int));
    int *dirty = malloc((size_t)n * sizeof(int));
    if (!stot || !acc || !rsize || !dirty) {
        free(stot);
        free(acc);
        free(rsize);
        free(dirty);
        for (int i = 0; i < n; i++) {
            refined[i] = i;
        }
        return leiden_relabel(refined, n);
    }
    for (int i = 0; i < n; i++) {
        refined[i] = i;
        stot[i] = g->k[i];
        rsize[i] = 1;
    }
    for (int v = 0; v < n; v++) {
        if (rsize[refined[v]] != 1) {
            continue; /* only singletons merge, per the refinement rule */
        }
        int cv = comm[v];
        int ndirty = 0;
        for (int e = g->off[v]; e < g->off[v + 1]; e++) {
            int u = g->nbr[e];
            if (u == v || comm[u] != cv) {
                continue; /* stay within the move-phase community */
            }
            int ru = refined[u];
            if (acc[ru] == 0.0) {
                dirty[ndirty++] = ru;
            }
            acc[ru] += g->w[e];
        }
        int rv = refined[v];
        double kv = g->k[v];
        stot[rv] -= kv;
        int best_r = rv;
        double best_gain = 0.0;
        for (int d = 0; d < ndirty; d++) {
            int r = dirty[d];
            if (r == rv) {
                continue;
            }
            double gain = acc[r] - gamma * kv * stot[r] / twom;
            if (gain > best_gain) {
                best_gain = gain;
                best_r = r;
            }
        }
        if (best_r != rv) {
            refined[v] = best_r;
            stot[best_r] += kv;
            rsize[best_r]++;
            rsize[rv]--;
        } else {
            stot[rv] += kv;
        }
        for (int d = 0; d < ndirty; d++) {
            acc[dirty[d]] = 0.0;
        }
    }
    free(stot);
    free(acc);
    free(rsize);
    free(dirty);
    return leiden_relabel(refined, n);
}

/* Aggregation phase: collapse each refined sub-community into a single node.
 * Builds the coarser graph in *out and records, for each aggregate node, the
 * move-phase community it belonged to in seed[] so the next level starts from
 * the coarse structure rather than from singletons. */
static int leiden_aggregate(const cbm_lg_t *g, const int *refined, int r_count, const int *comm,
                            cbm_lg_t *out, int *seed) {
    int n = g->n;
    double *k2 = calloc((size_t)r_count, sizeof(double));
    int *gcount = calloc((size_t)r_count, sizeof(int));
    int *gstart = malloc(((size_t)r_count + 1) * sizeof(int));
    int *members = malloc((size_t)n * sizeof(int));
    int *fill = malloc((size_t)r_count * sizeof(int));
    double *acc = calloc((size_t)r_count, sizeof(double));
    int *dirty = malloc((size_t)r_count * sizeof(int));
    int *off2 = malloc(((size_t)r_count + 1) * sizeof(int));
    if (!k2 || !gcount || !gstart || !members || !fill || !acc || !dirty || !off2) {
        free(k2);
        free(gcount);
        free(gstart);
        free(members);
        free(fill);
        free(acc);
        free(dirty);
        free(off2);
        return CBM_NOT_FOUND;
    }
    for (int r = 0; r < r_count; r++) {
        seed[r] = CBM_NOT_FOUND;
    }
    for (int i = 0; i < n; i++) {
        int r = refined[i];
        k2[r] += g->k[i];
        gcount[r]++;
        if (seed[r] == CBM_NOT_FOUND) {
            seed[r] = comm[i];
        }
    }
    gstart[0] = 0;
    for (int r = 0; r < r_count; r++) {
        gstart[r + 1] = gstart[r] + gcount[r];
        fill[r] = gstart[r];
    }
    for (int i = 0; i < n; i++) {
        members[fill[refined[i]]++] = i;
    }
    /* Pass 1: count distinct inter-community neighbours per aggregate node. */
    off2[0] = 0;
    for (int r = 0; r < r_count; r++) {
        int nd = 0;
        for (int m = gstart[r]; m < gstart[r + 1]; m++) {
            int i = members[m];
            for (int e = g->off[i]; e < g->off[i + 1]; e++) {
                int rb = refined[g->nbr[e]];
                if (rb == r || acc[rb] != 0.0) {
                    continue;
                }
                acc[rb] = 1.0;
                dirty[nd++] = rb;
            }
        }
        off2[r + 1] = off2[r] + nd;
        for (int d = 0; d < nd; d++) {
            acc[dirty[d]] = 0.0;
        }
    }
    int total = off2[r_count];
    int *nbr2 = malloc((size_t)(total > 0 ? total : 1) * sizeof(int));
    double *w2 = malloc((size_t)(total > 0 ? total : 1) * sizeof(double));
    if (!nbr2 || !w2) {
        free(k2);
        free(gcount);
        free(gstart);
        free(members);
        free(fill);
        free(acc);
        free(dirty);
        free(off2);
        free(nbr2);
        free(w2);
        return CBM_NOT_FOUND;
    }
    /* Pass 2: accumulate inter-community edge weights. */
    for (int r = 0; r < r_count; r++) {
        int nd = 0;
        for (int m = gstart[r]; m < gstart[r + 1]; m++) {
            int i = members[m];
            for (int e = g->off[i]; e < g->off[i + 1]; e++) {
                int rb = refined[g->nbr[e]];
                if (rb == r) {
                    continue;
                }
                if (acc[rb] == 0.0) {
                    dirty[nd++] = rb;
                }
                acc[rb] += g->w[e];
            }
        }
        int base = off2[r];
        for (int d = 0; d < nd; d++) {
            nbr2[base + d] = dirty[d];
            w2[base + d] = acc[dirty[d]];
            acc[dirty[d]] = 0.0;
        }
    }
    free(gcount);
    free(gstart);
    free(members);
    free(fill);
    free(acc);
    free(dirty);
    out->n = r_count;
    out->off = off2;
    out->nbr = nbr2;
    out->w = w2;
    out->k = k2;
    return CBM_STORE_OK;
}

int cbm_leiden(const int64_t *nodes, int node_count, const cbm_louvain_edge_t *edges,
               int edge_count, double resolution, cbm_louvain_result_t **out, int *out_count) {
    if (node_count <= 0) {
        *out = NULL;
        *out_count = 0;
        return CBM_STORE_OK;
    }
    int n = node_count;
    double gamma = resolution > 0.0 ? resolution : 1.0;

    cbm_louvain_result_t *result = malloc((size_t)n * sizeof(*result));
    if (!result) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < n; i++) {
        result[i].node_id = nodes[i];
        result[i].community = i;
    }

    /* Build deduplicated undirected edge weights, then a CSR graph. */
    int *wsi;
    int *wdi;
    double *ww;
    int wn;
    louvain_build_weights(nodes, n, edges, edge_count, &wsi, &wdi, &ww, &wn);
    cbm_lg_t g;
    int built = (wn > 0) ? lg_build(n, wsi, wdi, ww, wn, &g) : CBM_NOT_FOUND;
    free(wsi);
    free(wdi);
    free(ww);
    if (built != CBM_STORE_OK) {
        /* No edges (or allocation failure): every node is its own community. */
        *out = result;
        *out_count = n;
        return CBM_STORE_OK;
    }

    double twom = 0.0;
    for (int i = 0; i < n; i++) {
        twom += g.k[i];
    }

    int *orig = malloc((size_t)n * sizeof(int)); /* original node -> current graph node */
    int *comm = malloc((size_t)n * sizeof(int));
    if (twom <= 0.0 || !orig || !comm) {
        free(orig);
        free(comm);
        lg_free(&g);
        *out = result;
        *out_count = n;
        return CBM_STORE_OK;
    }
    for (int i = 0; i < n; i++) {
        orig[i] = i;
        comm[i] = i;
    }

    for (int level = 0; level < LEIDEN_MAX_LEVELS; level++) {
        leiden_move(&g, comm, gamma, twom);
        int c_count = leiden_relabel(comm, g.n);
        if (c_count >= g.n) {
            break; /* every node already isolated — nothing to coarsen */
        }
        int *refined = malloc((size_t)g.n * sizeof(int));
        if (!refined) {
            break;
        }
        int r_count = leiden_refine(&g, comm, gamma, twom, refined);
        if (r_count >= g.n) {
            free(refined);
            break; /* refinement cannot reduce the graph further */
        }
        for (int i = 0; i < n; i++) {
            orig[i] = refined[orig[i]];
        }
        cbm_lg_t g2;
        int *seed = malloc((size_t)r_count * sizeof(int));
        if (!seed || leiden_aggregate(&g, refined, r_count, comm, &g2, seed) != CBM_STORE_OK) {
            free(seed);
            free(refined);
            break;
        }
        free(refined);
        lg_free(&g);
        g = g2;
        free(comm);
        comm = seed;
    }

    for (int i = 0; i < n; i++) {
        result[i].community = comm[orig[i]];
    }
    free(comm);
    free(orig);
    lg_free(&g);
    *out = result;
    *out_count = n;
    return CBM_STORE_OK;
}

int cbm_louvain(const int64_t *nodes, int node_count, const cbm_louvain_edge_t *edges,
                int edge_count, cbm_louvain_result_t **out, int *out_count) {
    return cbm_leiden(nodes, node_count, edges, edge_count, 1.0, out, out_count);
}

/* ── Architecture: community clusters via Leiden ───────────────── */

enum {
    CBM_CLUSTER_TOP_N = 12,       /* report at most this many clusters */
    CBM_CLUSTER_MAX_TOPNODES = 5, /* representative node names per cluster */
    CBM_CLUSTER_MAX_PKGS = 5,     /* packages listed per cluster */
    CBM_CLUSTER_MIN_MEMBERS = 2,  /* skip singletons */
    CBM_CLUSTER_NODE_CAP = 8000,  /* bound the work for very large graphs */
    CBM_CLUSTER_LABEL_CONTEXT_SEGMENTS = 2
};

static int cluster_id_cmp(const void *key, const void *el) {
    int64_t k = *(const int64_t *)key;
    int64_t e = *(const int64_t *)el;
    return (k > e) - (k < e);
}

/* Index of node id within the id-sorted ids[], or CBM_NOT_FOUND. */
static int cluster_id_index(const int64_t *ids, int n, int64_t id) {
    const int64_t *hit = bsearch(&id, ids, (size_t)n, sizeof(int64_t), cluster_id_cmp);
    return hit ? (int)(hit - ids) : CBM_NOT_FOUND;
}

static void cluster_free_node_arrays(int64_t *ids, const char **names, const char **qns, int count) {
    for (int i = 0; i < count; i++) {
        safe_str_free(&names[i]);
        safe_str_free(&qns[i]);
    }
    free(ids);
    free(names);
    free(qns);
}

static void cluster_free_edge_arrays(cbm_louvain_edge_t *edges, int *esrc, int *edst, int *degree) {
    free(edges);
    free(esrc);
    free(edst);
    free(degree);
}

static int cluster_grow_nodes(int64_t **ids, const char ***names, const char ***qns, int *cap) {
    if (!ids || !names || !qns || !cap || *cap <= 0 || *cap > INT_MAX / ST_GROWTH) {
        return CBM_STORE_ERR;
    }
    int new_cap = *cap * ST_GROWTH;
    int64_t *new_ids = realloc(*ids, (size_t)new_cap * sizeof(int64_t));
    if (!new_ids) {
        return CBM_STORE_ERR;
    }
    *ids = new_ids;
    const char **new_names = realloc((void *)*names, (size_t)new_cap * sizeof(char *));
    if (!new_names) {
        return CBM_STORE_ERR;
    }
    *names = new_names;
    const char **new_qns = realloc((void *)*qns, (size_t)new_cap * sizeof(char *));
    if (!new_qns) {
        return CBM_STORE_ERR;
    }
    *qns = new_qns;
    *cap = new_cap;
    return CBM_STORE_OK;
}

static int cluster_grow_edges(cbm_louvain_edge_t **edges, int **esrc, int **edst, int *cap) {
    if (!edges || !esrc || !edst || !cap || *cap <= 0 || *cap > INT_MAX / ST_GROWTH) {
        return CBM_STORE_ERR;
    }
    int new_cap = *cap * ST_GROWTH;
    cbm_louvain_edge_t *new_edges =
        realloc(*edges, (size_t)new_cap * sizeof(cbm_louvain_edge_t));
    if (!new_edges) {
        return CBM_STORE_ERR;
    }
    *edges = new_edges;
    int *new_esrc = realloc(*esrc, (size_t)new_cap * sizeof(int));
    if (!new_esrc) {
        return CBM_STORE_ERR;
    }
    *esrc = new_esrc;
    int *new_edst = realloc(*edst, (size_t)new_cap * sizeof(int));
    if (!new_edst) {
        return CBM_STORE_ERR;
    }
    *edst = new_edst;
    *cap = new_cap;
    return CBM_STORE_OK;
}

/* Append `pkg` to a distinct package list (with a per-package count). */
static int cluster_add_pkg(const char **pkgs, int *counts, int *count, int cap, const char *pkg) {
    if (!pkg || !pkg[0]) {
        return CBM_STORE_OK;
    }
    for (int i = 0; i < *count; i++) {
        if (strcmp(pkgs[i], pkg) == 0) {
            counts[i]++;
            return CBM_STORE_OK;
        }
    }
    if (*count < cap) {
        char *copy = heap_strdup(pkg);
        if (!copy) {
            return CBM_STORE_ERR;
        }
        pkgs[*count] = copy;
        counts[*count] = 1;
        (*count)++;
    }
    return CBM_STORE_OK;
}

static bool cluster_label_is_generic(const char *label) {
    static const char *const generic[] = {"get",  "set",   "run",   "main", "init",
                                          "new",  "open",  "close", "read", "write",
                                          "start", "stop", "test",  NULL};
    if (!label) {
        return false;
    }
    for (int i = 0; generic[i]; i++) {
        if (strcmp(label, generic[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Label-only namespace context: up to two segments after the project, excluding
 * the final symbol so `project.pkg.func` labels as `pkg`, not `pkg.func`. */
static const char *cluster_label_context_from_qn(const char *qn) {
    if (!qn || !qn[0]) {
        return "";
    }
    const char *dots[ST_QN_MAX_DOTS] = {NULL};
    int ndots = 0;
    for (const char *p = qn; *p && ndots < ST_QN_MAX_DOTS; p++) {
        if (*p == '.') {
            dots[ndots++] = p;
        }
    }
    if (ndots < ST_COL_2) {
        return "";
    }

    static CBM_TLS char buf[CBM_SZ_256];
    const char *start = dots[0] + SKIP_ONE;
    const char *end =
        (ndots > CBM_CLUSTER_LABEL_CONTEXT_SEGMENTS) ? dots[CBM_CLUSTER_LABEL_CONTEXT_SEGMENTS]
                                                     : dots[SKIP_ONE];
    size_t len = (size_t)(end - start);
    if (len == 0 || len >= sizeof(buf)) {
        return "";
    }
    memcpy(buf, start, len);
    buf[len] = '\0';
    return buf;
}

static const char *cluster_best_context(const char **qns, const int *comm, int n, int c) {
    const char *contexts[CBM_CLUSTER_MAX_PKGS];
    int counts[CBM_CLUSTER_MAX_PKGS];
    int count = 0;
    for (int i = 0; i < n; i++) {
        if (comm[i] != c) {
            continue;
        }
        if (cluster_add_pkg(contexts, counts, &count, CBM_CLUSTER_MAX_PKGS,
                            cluster_label_context_from_qn(qns[i])) != CBM_STORE_OK) {
            for (int j = 0; j < count; j++) {
                safe_str_free(&contexts[j]);
            }
            return "";
        }
    }
    const char *best = "";
    int best_count = 0;
    for (int i = 0; i < count; i++) {
        if (counts[i] > best_count) {
            best = contexts[i];
            best_count = counts[i];
        }
    }
    char *ret = best[0] ? heap_strdup(best) : NULL;
    for (int i = 0; i < count; i++) {
        safe_str_free(&contexts[i]);
    }
    return ret ? ret : "";
}

static char *cluster_make_label(const cbm_cluster_info_t *ci, const char *context) {
    if (ci->top_node_count > 0) {
        const char *primary = ci->top_nodes[0];
        if (cluster_label_is_generic(primary) && ci->top_node_count > 1) {
            const char *secondary = ci->top_nodes[1];
            const char *pkg = (context && context[0])
                                  ? context
                                  : (ci->package_count > 0 ? ci->packages[0] : "");
            size_t len = strlen(primary) + strlen(secondary) + strlen(pkg) + 4;
            char *label = malloc(len);
            if (label) {
                if (pkg[0]) {
                    snprintf(label, len, "%s/%s@%s", primary, secondary, pkg);
                } else {
                    snprintf(label, len, "%s/%s", primary, secondary);
                }
                return label;
            }
        }
        if (cluster_label_is_generic(primary) && ci->package_count > 0) {
            const char *pkg = (context && context[0]) ? context : ci->packages[0];
            size_t len = strlen(primary) + strlen(pkg) + 2;
            char *label = malloc(len);
            if (label) {
                snprintf(label, len, "%s@%s", primary, pkg);
                return label;
            }
        }
        return heap_strdup(primary);
    }
    if (ci->package_count > 0) {
        return heap_strdup(ci->packages[0]);
    }
    return heap_strdup("cluster");
}

static char *cluster_make_disambiguated_label(const cbm_cluster_info_t *ci, const char *context,
                                              int cluster_id, bool include_id) {
    if (!ci || ci->top_node_count <= 0 || !ci->top_nodes[0]) {
        return heap_strdup("cluster");
    }

    const char *primary = ci->top_nodes[0];
    const char *secondary = (ci->top_node_count > 1 && ci->top_nodes[1]) ? ci->top_nodes[1] : "";
    const char *ctx =
        (context && context[0]) ? context : (ci->package_count > 0 ? ci->packages[0] : "");
    char id_buf[ST_BUF_64];
    int id_len = include_id ? snprintf(id_buf, sizeof(id_buf), "cluster%d", cluster_id) : 0;
    if (!include_id || id_len <= 0 || (size_t)id_len >= sizeof(id_buf)) {
        id_buf[0] = '\0';
    }

    size_t len = strlen(primary) + 1;
    if (secondary[0]) {
        len += strlen(secondary) + 1; /* slash */
    }
    if (ctx[0]) {
        len += strlen(ctx) + 1; /* at sign */
    }
    if (id_buf[0]) {
        len += strlen(id_buf) + 1; /* hash */
    }

    char *label = malloc(len);
    if (!label) {
        return heap_strdup(primary);
    }

    int n;
    if (secondary[0] && ctx[0] && id_buf[0]) {
        n = snprintf(label, len, "%s/%s@%s#%s", primary, secondary, ctx, id_buf);
    } else if (secondary[0] && ctx[0]) {
        n = snprintf(label, len, "%s/%s@%s", primary, secondary, ctx);
    } else if (secondary[0] && id_buf[0]) {
        n = snprintf(label, len, "%s/%s#%s", primary, secondary, id_buf);
    } else if (ctx[0] && id_buf[0]) {
        n = snprintf(label, len, "%s@%s#%s", primary, ctx, id_buf);
    } else if (secondary[0]) {
        n = snprintf(label, len, "%s/%s", primary, secondary);
    } else if (ctx[0]) {
        n = snprintf(label, len, "%s@%s", primary, ctx);
    } else if (id_buf[0]) {
        n = snprintf(label, len, "%s#%s", primary, id_buf);
    } else {
        n = snprintf(label, len, "%s", primary);
    }
    if (n < 0 || (size_t)n >= len) {
        free(label);
        return heap_strdup(primary);
    }
    return label;
}

static void cluster_disambiguate_label_pass(cbm_cluster_info_t *clusters, int count, int n,
                                            const int *comm, const char **qns, bool include_id) {
    bool duplicate[CBM_CLUSTER_TOP_N] = {false};
    for (int i = 0; i < count; i++) {
        for (int j = 0; j < count; j++) {
            if (i != j && clusters[i].label && clusters[j].label &&
                strcmp(clusters[i].label, clusters[j].label) == 0) {
                duplicate[i] = true;
                break;
            }
        }
    }

    for (int i = 0; i < count; i++) {
        if (!duplicate[i]) {
            continue;
        }

        const char *context = cluster_best_context(qns, comm, n, clusters[i].id);
        char *label =
            cluster_make_disambiguated_label(&clusters[i], context, clusters[i].id, include_id);
        if (context && context[0]) {
            safe_str_free(&context);
        }
        if (label) {
            safe_str_free(&clusters[i].label);
            clusters[i].label = label;
        }
    }
}

static void cluster_disambiguate_duplicate_labels(cbm_cluster_info_t *clusters, int count,
                                                  int n, const int *comm, const char **qns) {
    cluster_disambiguate_label_pass(clusters, count, n, comm, qns, false);
    cluster_disambiguate_label_pass(clusters, count, n, comm, qns, true);
}

/* Build the cluster_info for one community c into *ci. */
static int cluster_build_one(cbm_cluster_info_t *ci, int c, int n, const int *comm,
                             const int *degree, const char **names, const char **qns, int members,
                             double cohesion) {
    memset(ci, 0, sizeof(*ci));
    ci->id = c;
    ci->members = members;
    ci->cohesion = cohesion;

    /* Top nodes by degree. */
    int top_idx[CBM_CLUSTER_MAX_TOPNODES];
    int top_deg[CBM_CLUSTER_MAX_TOPNODES];
    int tn = 0;
    for (int i = 0; i < n; i++) {
        if (comm[i] != c) {
            continue;
        }
        int d = degree[i];
        int pos = tn;
        while (pos > 0 && top_deg[pos - 1] < d) {
            pos--;
        }
        if (pos < CBM_CLUSTER_MAX_TOPNODES) {
            int last = (tn < CBM_CLUSTER_MAX_TOPNODES) ? tn : CBM_CLUSTER_MAX_TOPNODES - 1;
            for (int k = last; k > pos; k--) {
                top_idx[k] = top_idx[k - 1];
                top_deg[k] = top_deg[k - 1];
            }
            top_idx[pos] = i;
            top_deg[pos] = d;
            if (tn < CBM_CLUSTER_MAX_TOPNODES) {
                tn++;
            }
        }
    }
    if (tn > 0) {
        ci->top_nodes = malloc((size_t)tn * sizeof(char *));
        if (!ci->top_nodes) {
            return CBM_STORE_ERR;
        }
        for (int i = 0; i < tn; i++) {
            ci->top_nodes[i] = heap_strdup(names[top_idx[i]]);
            if (!ci->top_nodes[i]) {
                ci->top_node_count = i + 1;
                arch_clear_cluster(ci);
                return CBM_STORE_ERR;
            }
        }
        ci->top_node_count = tn;
    }

    /* Distinct packages (+ dominant one as the label). */
    const char *pkgs[CBM_CLUSTER_MAX_PKGS];
    int pkg_counts[CBM_CLUSTER_MAX_PKGS];
    int pc = 0;
    for (int i = 0; i < n; i++) {
        if (comm[i] == c) {
            if (cluster_add_pkg(pkgs, pkg_counts, &pc, CBM_CLUSTER_MAX_PKGS,
                                cbm_qn_to_top_package(qns[i])) != CBM_STORE_OK) {
                for (int j = 0; j < pc; j++) {
                    safe_str_free(&pkgs[j]);
                }
                arch_clear_cluster(ci);
                return CBM_STORE_ERR;
            }
        }
    }
    if (pc > 0) {
        ci->packages = malloc((size_t)pc * sizeof(char *));
        if (!ci->packages) {
            for (int i = 0; i < pc; i++) {
                safe_str_free(&pkgs[i]);
            }
            arch_clear_cluster(ci);
            return CBM_STORE_ERR;
        }
        for (int i = 0; i < pc; i++) {
            ci->packages[i] = heap_strdup(pkgs[i]);
            if (!ci->packages[i]) {
                ci->package_count = i + 1;
                for (int j = 0; j < pc; j++) {
                    safe_str_free(&pkgs[j]);
                }
                arch_clear_cluster(ci);
                return CBM_STORE_ERR;
            }
        }
        ci->package_count = pc;
        for (int i = 0; i < pc; i++) {
            safe_str_free(&pkgs[i]);
        }
    }
    /* Label: the top hub node is the most informative AND discriminable name
     * for the community (e.g. "create_task", "install_plugins",
     * "execute_tmux_command"). Labeling by the dominant package made every
     * cluster in a single-package repo share one identical, uninformative
     * label. The package list is preserved separately in `packages`. */
    const char *label_context = cluster_best_context(qns, comm, n, c);
    ci->label = cluster_make_label(ci, label_context);
    if (label_context && label_context[0]) {
        safe_str_free(&label_context);
    }
    if (!ci->label) {
        arch_clear_cluster(ci);
        return CBM_STORE_ERR;
    }

    ci->edge_types = malloc(sizeof(char *));
    if (!ci->edge_types) {
        arch_clear_cluster(ci);
        return CBM_STORE_ERR;
    }
    ci->edge_types[0] = heap_strdup("CALLS");
    if (!ci->edge_types[0]) {
        ci->edge_type_count = 1;
        arch_clear_cluster(ci);
        return CBM_STORE_ERR;
    }
    ci->edge_type_count = 1;
    return CBM_STORE_OK;
}

/* Comparator for sorting community indices by descending member count. */
typedef struct {
    int comm;
    int members;
} cluster_rank_t;
static int cluster_rank_cmp(const void *a, const void *b) {
    const cluster_rank_t *ca = a;
    const cluster_rank_t *cb = b;
    return cb->members - ca->members;
}

static int arch_clusters(cbm_store_t *s, const char *project, const char *path,
                         cbm_architecture_info_t *out, double resolution) {
    /* 1. Load Function/Method/Class nodes, ordered by id for bsearch. */
    char norm[CBM_SZ_512];
    char like[CBM_SZ_512 + ST_ARCH_PATH_LIKE_EXTRA];
    bool scoped = arch_path_prepare(path, norm, sizeof(norm), like, sizeof(like));
    char nsql[ST_SQL_BUF];
    const char *base = "SELECT id, name, qualified_name FROM nodes "
                       "WHERE project=?1 AND label IN ('Function','Method','Class')";
    int nsql_len = scoped ? snprintf(nsql, sizeof(nsql), "%s%s ORDER BY id LIMIT ?4", base,
                                     arch_path_scope_sql())
                          : snprintf(nsql, sizeof(nsql), "%s ORDER BY id LIMIT ?2", base);
    if (nsql_len <= 0 || (size_t)nsql_len >= sizeof(nsql)) {
        return CBM_STORE_OK; /* clusters are best-effort */
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(s->db, nsql, CBM_NOT_FOUND, &st, NULL) != SQLITE_OK) {
        return CBM_STORE_OK; /* clusters are best-effort */
    }
    bind_text(st, SKIP_ONE, project);
    if (scoped) {
        arch_bind_path_scope(st, ST_COL_2, ST_COL_3, norm, like);
        sqlite3_bind_int(st, ST_COL_4, CBM_CLUSTER_NODE_CAP);
    } else {
        sqlite3_bind_int(st, ST_COL_2, CBM_CLUSTER_NODE_CAP);
    }
    int cap = ST_INIT_CAP_8;
    int n = 0;
    int64_t *ids = malloc((size_t)cap * sizeof(int64_t));
    const char **names = malloc((size_t)cap * sizeof(char *));
    const char **qns = malloc((size_t)cap * sizeof(char *));
    if (!ids || !names || !qns) {
        sqlite3_finalize(st);
        cluster_free_node_arrays(ids, names, qns, 0);
        return CBM_STORE_OK; /* clusters are best-effort */
    }
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(st)) == SQLITE_ROW) {
        if (n >= cap) {
            if (cluster_grow_nodes(&ids, &names, &qns, &cap) != CBM_STORE_OK) {
                sqlite3_finalize(st);
                cluster_free_node_arrays(ids, names, qns, n);
                return CBM_STORE_OK; /* clusters are best-effort */
            }
        }
        ids[n] = sqlite3_column_int64(st, 0);
        names[n] = NULL;
        qns[n] = NULL;
        names[n] = heap_strdup((const char *)sqlite3_column_text(st, SKIP_ONE));
        qns[n] = heap_strdup((const char *)sqlite3_column_text(st, CBM_SZ_2));
        if (!names[n] || !qns[n]) {
            sqlite3_finalize(st);
            cluster_free_node_arrays(ids, names, qns, n + 1);
            return CBM_STORE_OK; /* clusters are best-effort */
        }
        n++;
    }
    if (step_rc != SQLITE_DONE) {
        sqlite3_finalize(st);
        cluster_free_node_arrays(ids, names, qns, n);
        return CBM_STORE_OK; /* clusters are best-effort */
    }
    sqlite3_finalize(st);
    if (n < CBM_CLUSTER_MIN_MEMBERS) {
        cluster_free_node_arrays(ids, names, qns, n);
        return CBM_STORE_OK;
    }

    /* 2. Load CALLS edges with both endpoints in the node set (store indices). */
    cbm_louvain_edge_t *edges = malloc((size_t)n * sizeof(cbm_louvain_edge_t));
    int *esrc = malloc((size_t)n * sizeof(int));
    int *edst = malloc((size_t)n * sizeof(int));
    int *degree = calloc((size_t)n, sizeof(int));
    if (!edges || !esrc || !edst || !degree) {
        cluster_free_node_arrays(ids, names, qns, n);
        cluster_free_edge_arrays(edges, esrc, edst, degree);
        return CBM_STORE_OK; /* clusters are best-effort */
    }
    int ne = 0;
    const char *esql = "SELECT source_id, target_id FROM edges WHERE project=?1 AND type='CALLS'";
    if (sqlite3_prepare_v2(s->db, esql, CBM_NOT_FOUND, &st, NULL) == SQLITE_OK) {
        int ecap = n;
        bind_text(st, SKIP_ONE, project);
        step_rc = SQLITE_OK;
        while ((step_rc = sqlite3_step(st)) == SQLITE_ROW) {
            int si = cluster_id_index(ids, n, sqlite3_column_int64(st, 0));
            int ti = cluster_id_index(ids, n, sqlite3_column_int64(st, SKIP_ONE));
            if (si < 0 || ti < 0 || si == ti) {
                continue;
            }
            if (ne >= ecap) {
                if (cluster_grow_edges(&edges, &esrc, &edst, &ecap) != CBM_STORE_OK) {
                    sqlite3_finalize(st);
                    cluster_free_node_arrays(ids, names, qns, n);
                    cluster_free_edge_arrays(edges, esrc, edst, degree);
                    return CBM_STORE_OK; /* clusters are best-effort */
                }
            }
            edges[ne].src = ids[si];
            edges[ne].dst = ids[ti];
            esrc[ne] = si;
            edst[ne] = ti;
            degree[si]++;
            degree[ti]++;
            ne++;
        }
        if (step_rc != SQLITE_DONE) {
            sqlite3_finalize(st);
            cluster_free_node_arrays(ids, names, qns, n);
            cluster_free_edge_arrays(edges, esrc, edst, degree);
            return CBM_STORE_OK; /* clusters are best-effort */
        }
        sqlite3_finalize(st);
    }

    /* 3. Community detection. */
    cbm_louvain_result_t *res = NULL;
    int rn = 0;
    int *comm = NULL;
    int C = 0;
    if (cbm_leiden(ids, n, edges, ne, resolution, &res, &rn) == CBM_STORE_OK && res && rn == n) {
        comm = malloc((size_t)n * sizeof(int));
        if (comm) {
            for (int i = 0; i < n; i++) {
                comm[i] = res[i].community;
                if (comm[i] + 1 > C) {
                    C = comm[i] + 1;
                }
            }
        }
    }
    free(res);

    if (comm && C > 0) {
        /* 4. Members + cohesion (internal vs boundary edges) per community. */
        int *members = calloc((size_t)C, sizeof(int));
        int *internal = calloc((size_t)C, sizeof(int));
        int *boundary = calloc((size_t)C, sizeof(int));
        if (!members || !internal || !boundary) {
            free(members);
            free(internal);
            free(boundary);
            goto clusters_done;
        }
        for (int i = 0; i < n; i++) {
            members[comm[i]]++;
        }
        for (int e = 0; e < ne; e++) {
            int cs = comm[esrc[e]];
            int cd = comm[edst[e]];
            if (cs == cd) {
                internal[cs]++;
            } else {
                boundary[cs]++;
                boundary[cd]++;
            }
        }

        /* 5. Rank communities by size, take the top N non-singletons. */
        cluster_rank_t *rank = malloc((size_t)C * sizeof(cluster_rank_t));
        if (!rank) {
            free(members);
            free(internal);
            free(boundary);
            goto clusters_done;
        }
        for (int c = 0; c < C; c++) {
            rank[c] = (cluster_rank_t){c, members[c]};
        }
        qsort(rank, (size_t)C, sizeof(cluster_rank_t), cluster_rank_cmp);

        cbm_cluster_info_t *clusters =
            calloc((size_t)CBM_CLUSTER_TOP_N, sizeof(cbm_cluster_info_t));
        if (!clusters) {
            free(members);
            free(internal);
            free(boundary);
            free(rank);
            goto clusters_done;
        }
        int cc = 0;
        for (int r = 0; r < C && cc < CBM_CLUSTER_TOP_N; r++) {
            int c = rank[r].comm;
            if (members[c] < CBM_CLUSTER_MIN_MEMBERS) {
                break; /* sorted desc — the rest are singletons too */
            }
            double denom = internal[c] + boundary[c];
            double cohesion = denom > 0 ? (double)internal[c] / denom : 0.0;
            if (cluster_build_one(&clusters[cc], c, n, comm, degree, names, qns, members[c],
                                  cohesion) != CBM_STORE_OK) {
                arch_free_clusters(clusters, cc);
                free(members);
                free(internal);
                free(boundary);
                free(rank);
                goto clusters_done;
            }
            cc++;
        }
        cluster_disambiguate_duplicate_labels(clusters, cc, n, comm, qns);
        out->clusters = clusters;
        out->cluster_count = cc;

        free(members);
        free(internal);
        free(boundary);
        free(rank);
    }

clusters_done:
    free(comm);
    cluster_free_node_arrays(ids, names, qns, n);
    cluster_free_edge_arrays(edges, esrc, edst, degree);
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

int cbm_store_get_architecture_scoped(cbm_store_t *s, const char *project, const char *path,
                                      const char **aspects, int aspect_count,
                                      cbm_architecture_info_t *out, int hotspot_limit,
                                      double leiden_resolution) {
    /* Leiden resolution (gamma): controls cluster granularity. >1 → smaller
     * clusters; <1 → larger. Reject NaN/non-positive (config-tunable since the
     * value flows in from CBM_CONFIG_ARCH_RESOLUTION). Default 1.0. */
    if (!(leiden_resolution > 0.0)) {
        leiden_resolution = 1.0;
    }
    memset(out, 0, sizeof(*out));
    int rc;

    if (want_aspect(aspects, aspect_count, "languages")) {
        rc = arch_languages(s, project, path, out);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }
    if (want_aspect(aspects, aspect_count, "packages")) {
        rc = arch_packages(s, project, path, out);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }
    if (want_aspect(aspects, aspect_count, "entry_points")) {
        rc = arch_entry_points(s, project, path, out);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }
    if (want_aspect(aspects, aspect_count, "routes")) {
        rc = arch_routes(s, project, path, out);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }
    if (want_aspect(aspects, aspect_count, "hotspots")) {
        rc = arch_hotspots(s, project, path, out,
                           hotspot_limit > 0 ? hotspot_limit : CBM_ARCH_HOTSPOT_DEFAULT_LIMIT);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }
    if (want_aspect(aspects, aspect_count, "boundaries")) {
        cbm_cross_pkg_boundary_t *barr = NULL;
        int bcount = 0;
        rc = arch_boundaries(s, project, path, &barr, &bcount);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
        out->boundaries = barr;
        out->boundary_count = bcount;
    }
    if (want_aspect(aspects, aspect_count, "layers")) {
        rc = arch_layers(s, project, path, out);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }
    if (want_aspect(aspects, aspect_count, "file_tree")) {
        rc = arch_file_tree(s, project, path, out);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }
    if (want_aspect(aspects, aspect_count, "clusters")) {
        rc = arch_clusters(s, project, path, out, leiden_resolution);
        if (rc != CBM_STORE_OK) {
            goto fail;
        }
    }

    return CBM_STORE_OK;

fail:
    cbm_store_architecture_free(out);
    return rc;
}

int cbm_store_get_architecture(cbm_store_t *s, const char *project, const char **aspects,
                               int aspect_count, cbm_architecture_info_t *out,
                               int hotspot_limit, double leiden_resolution) {
    return cbm_store_get_architecture_scoped(s, project, NULL, aspects, aspect_count, out,
                                             hotspot_limit, leiden_resolution);
}

void cbm_store_architecture_free(cbm_architecture_info_t *out) {
    if (!out) {
        return;
    }
    for (int i = 0; i < out->language_count; i++) {
        safe_str_free(&out->languages[i].language);
    }
    free(out->languages);
    arch_free_packages(out->packages, out->package_count);
    arch_free_entry_points(out->entry_points, out->entry_point_count);
    arch_free_routes(out->routes, out->route_count);
    arch_free_hotspots(out->hotspots, out->hotspot_count);
    arch_free_boundaries(out->boundaries, out->boundary_count);
    for (int i = 0; i < out->service_count; i++) {
        safe_str_free(&out->services[i].from);
        safe_str_free(&out->services[i].to);
        safe_str_free(&out->services[i].type);
    }
    free(out->services);
    arch_free_layers(out->layers, out->layer_count);
    arch_free_clusters(out->clusters, out->cluster_count);
    for (int i = 0; i < out->file_tree_count; i++) {
        safe_str_free(&out->file_tree[i].path);
        safe_str_free(&out->file_tree[i].type);
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

/* Save a completed ADR section into result, trimming whitespace. */
static void adr_save_section(cbm_adr_sections_t *result, char *section_name, char *buf,
                             int buf_len) {
    if (!section_name || result->count >= ST_BUF_16) {
        return;
    }
    /* Trim trailing whitespace */
    while (buf_len > 0 && (buf[buf_len - SKIP_ONE] == '\n' || buf[buf_len - SKIP_ONE] == ' ')) {
        buf[--buf_len] = '\0';
    }
    /* Skip leading whitespace */
    char *trimmed = buf;
    while (*trimmed == '\n' || *trimmed == ' ') {
        trimmed++;
    }
    result->keys[result->count] = section_name;
    result->values[result->count] = heap_strdup(trimmed);
    result->count++;
}

/* Try to extract a canonical section header from a "## Header" line.
 * Returns heap-allocated header string if canonical, NULL otherwise. */
static char *adr_try_section_header(const char *line, int line_len) {
    if (line_len <= ST_HEADER_PREFIX || line[0] != '#' || line[SKIP_ONE] != '#' ||
        line[PAIR_LEN] != ' ') {
        return NULL;
    }
    char header[CBM_SZ_64];
    int hlen = line_len - ST_HEADER_PREFIX;
    if (hlen >= (int)sizeof(header)) {
        hlen = (int)sizeof(header) - SKIP_ONE;
    }
    memcpy(header, line + 3, hlen);
    header[hlen] = '\0';
    while (hlen > 0 && (header[hlen - SKIP_ONE] == ' ' || header[hlen - SKIP_ONE] == '\t' ||
                        header[hlen - SKIP_ONE] == '\r')) {
        header[--hlen] = '\0';
    }
    if (!is_canonical_section(header)) {
        return NULL;
    }
    return heap_strdup(header);
}

/* Append a line to the current section content buffer. */
static void adr_append_line(char *buf, int buf_sz, int *len, const char *line, int line_len) {
    if (*len > 0) {
        buf[(*len)++] = '\n';
    }
    if (*len + line_len < buf_sz - SKIP_ONE) {
        memcpy(buf + *len, line, line_len);
        *len += line_len;
        buf[*len] = '\0';
    }
}

cbm_adr_sections_t cbm_adr_parse_sections(const char *content) {
    cbm_adr_sections_t result;
    memset(&result, 0, sizeof(result));
    if (!content || !content[0]) {
        return result;
    }

    const char *p = content;
    char *current_section = NULL;
    char current_content[ST_SQL_BUF] = "";
    int content_len = 0;

    while (*p) {
        const char *eol = strchr(p, '\n');
        int line_len = eol ? (int)(eol - p) : (int)strlen(p);

        char *new_section = adr_try_section_header(p, line_len);
        if (new_section) {
            adr_save_section(&result, current_section, current_content, content_len);
            current_section = new_section;
            current_content[0] = '\0';
            content_len = 0;
        } else if (current_section && (content_len > 0 || line_len > 0)) {
            adr_append_line(current_content, (int)sizeof(current_content), &content_len, p,
                            line_len);
        }

        p = eol ? eol + SKIP_ONE : p + line_len;
    }

    adr_save_section(&result, current_section, current_content, content_len);
    return result;
}

/* Append a section to the render buffer. */
static int adr_render_section(char *buf, int buf_sz, int pos, const char *key, const char *value) {
    if (pos > 0) {
        pos += snprintf(buf + pos, buf_sz - pos, "\n\n");
    }
    pos += snprintf(buf + pos, buf_sz - pos, "## %s\n%s", key, value);
    return pos;
}

char *cbm_adr_render(const cbm_adr_sections_t *sections) {
    if (!sections || sections->count == 0) {
        return heap_strdup("");
    }

    char buf[ST_MAX_DEGREE * ST_GROWTH] = "";
    int pos = 0;
    bool rendered[ST_MAX_SECTIONS] = {false};

    /* Canonical sections first, in order */
    for (int c = 0; c < canonical_section_count; c++) {
        for (int i = 0; i < sections->count; i++) {
            if (rendered[i]) {
                continue;
            }
            if (strcmp(sections->keys[i], canonical_sections[c]) == 0) {
                pos = adr_render_section(buf, (int)sizeof(buf), pos, sections->keys[i],
                                         sections->values[i]);
                rendered[i] = true;
                break;
            }
        }
    }

    /* Non-canonical sections alphabetically */
    int extra[ST_BUF_16];
    int nextra = 0;
    for (int i = 0; i < sections->count; i++) {
        if (!rendered[i]) {
            extra[nextra++] = i;
        }
    }
    for (int i = SKIP_ONE; i < nextra; i++) {
        int j = i;
        while (j > 0 && strcmp(sections->keys[extra[j]], sections->keys[extra[j - SKIP_ONE]]) < 0) {
            int tmp = extra[j];
            extra[j] = extra[j - SKIP_ONE];
            extra[j - SKIP_ONE] = tmp;
            j--;
        }
    }
    for (int i = 0; i < nextra; i++) {
        int idx = extra[i];
        pos = adr_render_section(buf, (int)sizeof(buf), pos, sections->keys[idx],
                                 sections->values[idx]);
    }

    return heap_strdup(buf);
}

int cbm_adr_validate_content(const char *content, char *errbuf, int errbuf_size) {
    cbm_adr_sections_t sections = cbm_adr_parse_sections(content);
    char missing[CBM_SZ_256] = "";
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
    char invalid[CBM_SZ_256] = "";
    int ilen = 0;
    int ninvalid = 0;

    /* Collect and sort invalid keys */
    const char *inv_keys[ST_MAX_SECTIONS];
    int inv_n = 0;
    for (int i = 0; i < count; i++) {
        if (!is_canonical_section(keys[i])) {
            if (inv_n < ST_BUF_16) {
                inv_keys[inv_n++] = keys[i];
            }
        }
    }
    /* Sort alphabetically */
    for (int i = SKIP_ONE; i < inv_n; i++) {
        int j = i;
        while (j > 0 && strcmp(inv_keys[j], inv_keys[j - SKIP_ONE]) < 0) {
            const char *tmp = inv_keys[j];
            inv_keys[j] = inv_keys[j - SKIP_ONE];
            inv_keys[j - SKIP_ONE] = tmp;
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
    char now[CBM_SZ_32];
    iso_now(now, sizeof(now));

    const char *sql =
        "INSERT INTO project_summaries (project, summary, source_hash, created_at, updated_at) "
        "VALUES (?1, ?2, '', ?3, ?4) "
        "ON CONFLICT(project) DO UPDATE SET summary=excluded.summary, "
        "updated_at=excluded.updated_at";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "adr_store");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    bind_text(stmt, ST_COL_2, content);
    bind_text(stmt, ST_COL_3, now);
    bind_text(stmt, ST_COL_4, now);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return (rc == SQLITE_DONE) ? CBM_STORE_OK : CBM_STORE_ERR;
}

int cbm_store_adr_get(cbm_store_t *s, const char *project, cbm_adr_t *out) {
    const char *sql = "SELECT project, summary, created_at, updated_at FROM project_summaries "
                      "WHERE project=?1";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "adr_get");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    int rc = sqlite3_step(stmt);
    if (rc != SQLITE_ROW) {
        sqlite3_finalize(stmt);
        store_set_error(s, "no ADR found");
        return CBM_STORE_NOT_FOUND;
    }
    out->project = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
    out->content = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
    out->created_at = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_2));
    out->updated_at = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_3));
    sqlite3_finalize(stmt);
    return CBM_STORE_OK;
}

int cbm_store_adr_delete(cbm_store_t *s, const char *project) {
    const char *sql = "DELETE FROM project_summaries WHERE project=?1";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "adr_delete");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
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
        if (!found && sections.count < ST_BUF_16) {
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
        char msg[CBM_SZ_128];
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
    safe_str_free(&adr->project);
    safe_str_free(&adr->content);
    safe_str_free(&adr->created_at);
    safe_str_free(&adr->updated_at);
    memset(adr, 0, sizeof(*adr));
}

/* ── Architecture doc discovery ────────────────────────────────── */

int cbm_store_find_architecture_docs(cbm_store_t *s, const char *project, char ***out, int *count) {
    if (!s || !out || !count) {
        return CBM_STORE_ERR;
    }
    *out = NULL;
    *count = 0;

    const char *sql = "SELECT file_path FROM nodes WHERE project=?1 AND label='File' "
                      "AND (file_path LIKE '%ARCHITECTURE.md' OR file_path LIKE '%ADR.md' "
                      "OR file_path LIKE '%DECISIONS.md' OR file_path LIKE 'docs/adr/%' "
                      "OR file_path LIKE 'doc/adr/%' OR file_path LIKE 'adr/%') "
                      "ORDER BY file_path LIMIT ?2";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "find_arch_docs");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);
    sqlite3_bind_int(stmt, ST_COL_2, ST_ARCH_DOC_LIMIT);

    int cap = ST_INIT_CAP_8;
    int n = 0;
    char **arr = malloc((size_t)cap * sizeof(*arr));
    if (!arr) {
        sqlite3_finalize(stmt);
        store_set_error(s, "find_arch_docs out of memory");
        return CBM_STORE_ERR;
    }
    int step_rc = SQLITE_OK;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        if (n >= cap) {
            if (cap > INT_MAX / ST_GROWTH) {
                store_free_text_array(arr, n);
                sqlite3_finalize(stmt);
                store_set_error(s, "find_arch_docs out of memory");
                return CBM_STORE_ERR;
            }
            int new_cap = cap * ST_GROWTH;
            char **next = realloc(arr, (size_t)new_cap * sizeof(*next));
            if (!next) {
                store_free_text_array(arr, n);
                sqlite3_finalize(stmt);
                store_set_error(s, "find_arch_docs out of memory");
                return CBM_STORE_ERR;
            }
            arr = next;
            cap = new_cap;
        }
        const char *path = (const char *)sqlite3_column_text(stmt, 0);
        if (!path) {
            continue;
        }
        char *copy = heap_strdup(path);
        if (!copy) {
            store_free_text_array(arr, n);
            sqlite3_finalize(stmt);
            store_set_error(s, "find_arch_docs out of memory");
            return CBM_STORE_ERR;
        }
        arr[n++] = copy;
    }
    if (step_rc != SQLITE_DONE) {
        store_free_text_array(arr, n);
        store_set_error_sqlite(s, "find_arch_docs");
        sqlite3_finalize(stmt);
        return CBM_STORE_ERR;
    }
    sqlite3_finalize(stmt);
    *out = arr;
    *count = n;
    return CBM_STORE_OK;
}

/* ── Memory management ──────────────────────────────────────────── */

void cbm_node_free_fields(cbm_node_t *n) {
    safe_str_free(&n->project);
    safe_str_free(&n->label);
    safe_str_free(&n->name);
    safe_str_free(&n->qualified_name);
    safe_str_free(&n->file_path);
    safe_str_free(&n->properties_json);
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
        safe_str_free(&edges[i].project);
        safe_str_free(&edges[i].type);
        safe_str_free(&edges[i].properties_json);
    }
    free(edges);
}

void cbm_project_free_fields(cbm_project_t *p) {
    safe_str_free(&p->name);
    safe_str_free(&p->indexed_at);
    safe_str_free(&p->root_path);
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
        safe_str_free(&hashes[i].project);
        safe_str_free(&hashes[i].rel_path);
        safe_str_free(&hashes[i].sha256);
    }
    free(hashes);
}

void cbm_store_file_state_free_fields(cbm_file_state_t *state) {
    if (!state) {
        return;
    }
    safe_str_free(&state->project);
    safe_str_free(&state->rel_path);
    safe_str_free(&state->content_hash);
    safe_str_free(&state->git_oid);
    safe_str_free(&state->language);
    safe_str_free(&state->pass_fingerprint);
    safe_str_free(&state->indexed_at);
}

void cbm_store_derived_view_state_free_fields(cbm_derived_view_state_t *state) {
    if (!state) {
        return;
    }
    safe_str_free(&state->project);
    safe_str_free(&state->view_name);
    safe_str_free(&state->computed_at);
    safe_str_free(&state->status);
}

/* ── Vector search ────────────────────────��──────────────────────── */

int cbm_store_count_vectors(cbm_store_t *s, const char *project) {
    if (!s || !project) {
        return 0;
    }
    sqlite3_stmt *stmt = NULL;
    const char *sql = "SELECT count(*) FROM node_vectors WHERE project = ?1";
    if (sqlite3_prepare_v2(s->db, sql, SQLITE_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(stmt, SKIP_ONE, project, SQLITE_AUTO_LEN, SQLITE_STATIC);
    int count = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        count = sqlite3_column_int(stmt, 0);
    }
    sqlite3_finalize(stmt);
    return count;
}

void cbm_store_free_vector_results(cbm_vector_result_t *results, int count) {
    if (!results) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(results[i].name);
        free(results[i].qualified_name);
        free(results[i].file_path);
        free(results[i].label);
    }
    free(results);
}

/* Per-keyword scoring: score each keyword independently against each node
 * vector, then combine using min(cosine_k) across keywords.  This ensures
 * ALL keywords must be relevant, not just the average. */
enum {
    VS_VEC_DIM = 768,
    VS_SPARSE_NNZE = 8,
    VS_RI_SEED = 0x52494E44,
    VS_MAX_KW = 32,
    VS_STR_BUF = 16,
};

/* Try to look up an enriched int8 vector for `token` in the token_vectors
 * table.  On success, writes the de-quantized float representation to
 * `out` and returns true. */
static bool vs_load_enriched_vector(cbm_store_t *s, const char *project, const char *token,
                                    float *out) {
    sqlite3_stmt *tv_stmt = NULL;
    const char *tv_sql = "SELECT vector, idf FROM token_vectors"
                         " WHERE project = ?1 AND token = ?2 LIMIT 1";
    if (sqlite3_prepare_v2(s->db, tv_sql, SQLITE_AUTO_LEN, &tv_stmt, NULL) != SQLITE_OK) {
        return false;
    }
    bool found = false;
    sqlite3_bind_text(tv_stmt, SKIP_ONE, project, SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_text(tv_stmt, ST_COL_2, token, SQLITE_AUTO_LEN, SQLITE_STATIC);
    if (sqlite3_step(tv_stmt) == SQLITE_ROW) {
        const int8_t *vec = (const int8_t *)sqlite3_value_blob(sqlite3_column_value(tv_stmt, 0));
        int vec_len = sqlite3_column_bytes(tv_stmt, 0);
        if (vec && vec_len == VS_VEC_DIM) {
            for (int d = 0; d < VS_VEC_DIM; d++) {
                out[d] = (float)vec[d] / CBM_STORE_INT8_MAX;
            }
            found = true;
        }
    }
    sqlite3_finalize(tv_stmt);
    return found;
}

/* Sparse-random-index fallback for tokens not present in the enriched table. */
static void vs_fill_sparse_random(const char *token, float *out) {
    uint64_t seed = XXH3_64bits(token, strlen(token));
    for (int i = 0; i < VS_SPARSE_NNZE; i++) {
        uint64_t h = XXH3_64bits_withSeed(&i, sizeof(i), seed + VS_RI_SEED);
        int pos = (int)(h % VS_VEC_DIM);
        float sign = (h & SKIP_ONE) ? CBM_STORE_UNIT_POS_F : -CBM_STORE_UNIT_POS_F;
        out[pos] += sign;
    }
}

/* Clamp one float into the int8 representable range. */
static int8_t vs_clamp_int8(float v) {
    if (v > CBM_STORE_INT8_MAX) {
        return (int8_t)CBM_STORE_INT8_MAX;
    }
    if (v < -CBM_STORE_INT8_MAX) {
        return (int8_t)-CBM_STORE_INT8_MAX;
    }
    return (int8_t)v;
}

/* Normalize + int8 quantize a float vector.  Returns false if the vector is
 * effectively zero (magnitude below epsilon), in which case `dst` is left in
 * an indeterminate state and the caller should skip this keyword. */
static bool vs_normalize_and_quantize(const float *src, int8_t *dst) {
    float mag = 0.0F;
    for (int d = 0; d < VS_VEC_DIM; d++) {
        mag += src[d] * src[d];
    }
    mag = sqrtf(mag);
    if (mag < CBM_STORE_DENOM_EPS_F) {
        return false;
    }
    float inv = CBM_STORE_UNIT_POS_F / mag;
    for (int d = 0; d < VS_VEC_DIM; d++) {
        dst[d] = vs_clamp_int8(src[d] * inv * CBM_STORE_INT8_MAX);
    }
    return true;
}

/* Build int8 query vectors for each keyword.  Returns the number of
 * successfully built vectors (may be less than keyword_count when some
 * keywords produce zero-magnitude vectors). */
static int vs_build_keyword_vectors(cbm_store_t *s, const char *project, const char **keywords,
                                    int keyword_count, int8_t (*kw_vecs)[VS_VEC_DIM]) {
    int actual_kw = 0;
    for (int k = 0; k < keyword_count && actual_kw < VS_MAX_KW; k++) {
        if (!keywords[k] || !keywords[k][0]) {
            continue;
        }
        float kw_f[VS_VEC_DIM];
        memset(kw_f, 0, sizeof(kw_f));
        if (!vs_load_enriched_vector(s, project, keywords[k], kw_f)) {
            vs_fill_sparse_random(keywords[k], kw_f);
        }
        if (vs_normalize_and_quantize(kw_f, kw_vecs[actual_kw])) {
            actual_kw++;
        }
    }
    return actual_kw;
}

/* Compute the per-keyword min cosine score between a node's int8 vector and
 * each of the query vectors.  Returns 0.0 if the node vector is unavailable
 * or mis-sized. */
static double vs_min_cosine_score(const int8_t *node_vec, int node_vec_len,
                                  const int8_t (*kw_vecs)[VS_VEC_DIM], int actual_kw) {
    if (!node_vec || node_vec_len != VS_VEC_DIM) {
        return 0.0;
    }
    double min_score = CBM_STORE_UNIT_POS_D;
    for (int k = 0; k < actual_kw; k++) {
        int32_t dot = 0;
        int32_t ma = 0;
        int32_t mb = 0;
        for (int d = 0; d < VS_VEC_DIM; d++) {
            dot += (int32_t)kw_vecs[k][d] * (int32_t)node_vec[d];
            ma += (int32_t)kw_vecs[k][d] * (int32_t)kw_vecs[k][d];
            mb += (int32_t)node_vec[d] * (int32_t)node_vec[d];
        }
        double denom = sqrt((double)ma) * sqrt((double)mb);
        double cos_k = denom > CBM_STORE_DENOM_EPS_D ? (double)dot / denom : 0.0;
        if (cos_k < min_score) {
            min_score = cos_k;
        }
    }
    return min_score;
}

/* Append one candidate row read from the scan statement into the result
 * vector.  Grows the results array geometrically on demand.  Returns the
 * (possibly grown) results pointer, or NULL on allocation failure. */
static cbm_vector_result_t *vs_append_result(cbm_vector_result_t *results, int *count, int *cap,
                                             sqlite3_stmt *stmt,
                                             const int8_t (*kw_vecs)[VS_VEC_DIM], int actual_kw) {
    if (*count >= *cap) {
        int nc = *cap < CBM_SZ_16 ? CBM_SZ_16 : *cap * ST_COL_2;
        cbm_vector_result_t *grown = realloc(results, (size_t)nc * sizeof(cbm_vector_result_t));
        if (!grown) {
            return NULL;
        }
        results = grown;
        *cap = nc;
    }
    const char *name = (const char *)sqlite3_column_text(stmt, SKIP_ONE);
    const char *qn = (const char *)sqlite3_column_text(stmt, ST_COL_2);
    const char *fp = (const char *)sqlite3_column_text(stmt, ST_COL_3);
    const char *label = (const char *)sqlite3_column_text(stmt, ST_COL_4);
    char *name_copy = heap_strdup(name ? name : "");
    char *qn_copy = heap_strdup(qn ? qn : "");
    char *fp_copy = heap_strdup(fp ? fp : "");
    char *label_copy = heap_strdup(label ? label : "");
    if (!name_copy || !qn_copy || !fp_copy || !label_copy) {
        free(name_copy);
        free(qn_copy);
        free(fp_copy);
        free(label_copy);
        return NULL;
    }
    int idx = (*count)++;
    results[idx].node_id = sqlite3_column_int64(stmt, 0);
    results[idx].name = name_copy;
    results[idx].qualified_name = qn_copy;
    results[idx].file_path = fp_copy;
    results[idx].label = label_copy;
    const int8_t *node_vec = (const int8_t *)sqlite3_column_blob(stmt, ST_COL_6);
    int node_vec_len = sqlite3_column_bytes(stmt, ST_COL_6);
    results[idx].score = vs_min_cosine_score(node_vec, node_vec_len, kw_vecs, actual_kw);
    return results;
}

int cbm_store_vector_search(cbm_store_t *s, const char *project, const char **keywords,
                            int keyword_count, int limit, cbm_vector_result_t **out,
                            int *out_count) {
    *out = NULL;
    *out_count = 0;
    if (!s || !project || !keywords || keyword_count <= 0) {
        return CBM_STORE_ERR;
    }

    int8_t kw_vecs[VS_MAX_KW][VS_VEC_DIM];
    int actual_kw = vs_build_keyword_vectors(s, project, keywords, keyword_count, kw_vecs);
    if (actual_kw == 0) {
        return CBM_STORE_OK;
    }

    /* Scan all node vectors, compute per-keyword cosine, take min.
     * We use the FIRST keyword as the SQL sort (for top-K pre-filter),
     * then re-score with min across all keywords in the append helper. */
    const char *sql = "SELECT n.id, n.name, n.qualified_name, n.file_path, n.label,"
                      "       cbm_cosine_i8(v.vector, ?1) as score, v.vector"
                      " FROM node_vectors v"
                      " INNER JOIN nodes n ON n.id = v.node_id"
                      " WHERE v.project = ?2"
                      " AND n.label IN ('Function','Method','Class')"
                      " ORDER BY score DESC"
                      " LIMIT ?3";

    sqlite3_stmt *stmt = NULL;
    int prep_rc = sqlite3_prepare_v2(s->db, sql, SQLITE_AUTO_LEN, &stmt, NULL);
    if (prep_rc != SQLITE_OK) {
        store_set_error_sqlite(s, "vector_search prepare");
        return CBM_STORE_ERR;
    }

    /* Use first keyword for SQL pre-filter, fetch more candidates for re-ranking */
    int fetch_limit = (limit > 0 ? limit : CBM_SZ_16) * ST_COL_5;
    sqlite3_bind_blob(stmt, SKIP_ONE, kw_vecs[0], VS_VEC_DIM, SQLITE_STATIC);
    sqlite3_bind_text(stmt, ST_COL_2, project, SQLITE_AUTO_LEN, SQLITE_STATIC);
    sqlite3_bind_int(stmt, ST_COL_3, fetch_limit);

    {
        char kw_buf[VS_STR_BUF];
        char fl_buf[VS_STR_BUF];
        snprintf(kw_buf, sizeof(kw_buf), "%d", actual_kw);
        snprintf(fl_buf, sizeof(fl_buf), "%d", fetch_limit);
        cbm_log_info("vector_search.exec", "kw_count", kw_buf, "fetch_limit", fl_buf, "project",
                     project);
    }

    cbm_vector_result_t *results = NULL;
    int count = 0;
    int cap = 0;
    int step_rc = 0;
    bool result_oom = false;
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        cbm_vector_result_t *grown =
            vs_append_result(results, &count, &cap, stmt, kw_vecs, actual_kw);
        if (!grown) {
            result_oom = true;
            break;
        }
        results = grown;
    }

    if (result_oom) {
        sqlite3_finalize(stmt);
        cbm_store_free_vector_results(results, count);
        store_set_error(s, "vector_search result allocation failed");
        return CBM_STORE_ERR;
    }

    if (step_rc != SQLITE_DONE) {
        char rc_buf[VS_STR_BUF];
        snprintf(rc_buf, sizeof(rc_buf), "%d", step_rc);
        cbm_log_warn("vector_search.step_error", "rc", rc_buf, "msg", sqlite3_errmsg(s->db));
    }
    {
        char cnt_buf[VS_STR_BUF];
        snprintf(cnt_buf, sizeof(cnt_buf), "%d", count);
        cbm_log_info("vector_search.done", "candidates", cnt_buf);
    }
    sqlite3_finalize(stmt);

    /* Re-sort by min-score (SQL sorted by first keyword only) */
    for (int i = 0; i < count - SKIP_ONE; i++) {
        for (int j = i + SKIP_ONE; j < count; j++) {
            if (results[j].score > results[i].score) {
                cbm_vector_result_t tmp = results[i];
                results[i] = results[j];
                results[j] = tmp;
            }
        }
    }

    /* Trim to requested limit */
    int final_limit = limit > 0 ? limit : CBM_SZ_16;
    if (count > final_limit) {
        for (int i = final_limit; i < count; i++) {
            free(results[i].name);
            free(results[i].qualified_name);
            free(results[i].file_path);
            free(results[i].label);
        }
        count = final_limit;
    }

    *out = results;
    *out_count = count;
    return CBM_STORE_OK;
}
