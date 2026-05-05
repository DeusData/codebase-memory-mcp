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
    ST_FOUND = -1,
    ST_BUF_16 = 16,
    ST_BUF_64 = 64,
    ST_GROWTH = 2,
    ST_MAX_DEGREE = 8192,
    ST_HALF_SEC = 500000,
    ST_RETRY_WAIT_US = 1000,
    ST_INIT_CAP_8 = 8,
    ST_INIT_CAP_16 = 16,
    ST_SQL_BUF = 8192,
    ST_MAX_ROW_CHECK = 5,
    ST_QN_MAX_DOTS = 5,
    ST_QN_MIN_DOTS = 3,
    ST_IN_CLAUSE_MARGIN = 4,
    ST_GLOB_MIN_LEN = 3,
    ST_GLOB_SKIP = 2,
    ST_MAX_LANG = 10,
    ST_SEARCH_MAX_BINDS = 16,
    ST_MAX_PKGS = 64,
    ST_INIT_CAP_4 = 4,
    ST_HEADER_PREFIX = 3,
    ST_MIN_INDEGREE = 3,
    ST_MAX_PATH_DEPTH = 3,
    ST_MAX_ITERATIONS = 10,
    ST_GRAPH_SEED_MULT = 1000,
    ST_MAX_SECTIONS = 16,
    ST_METHOD_PROP_LEN = 8,
    ST_PATH_PROP_LEN = 6,
    ST_HANDLER_PROP_LEN = 9,
};
#define ST_WEIGHT_2 2.0

#define SLEN(s) (sizeof(s) - 1)
#include "store/store.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/log.h"
#include "foundation/compat_regex.h"

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
        ");";

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
        "CREATE INDEX IF NOT EXISTS idx_edges_url_path ON edges(project, url_path_gen);";
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
        rc = exec_sql(s, "PRAGMA busy_timeout = 10000;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = exec_sql(s, "PRAGMA journal_mode = WAL;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = exec_sql(s, "PRAGMA synchronous = NORMAL;");
        if (rc != CBM_STORE_OK) {
            return rc;
        }
        rc = exec_sql(s, "PRAGMA mmap_size = 67108864;"); /* CBM_SZ_64 MB */
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

static void sqlite_regexp(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *pattern = (const char *)sqlite3_value_text(argv[0]);
    const char *text = (const char *)sqlite3_value_text(argv[SKIP_ONE]);
    if (!pattern || !text) {
        sqlite3_result_int(ctx, 0);
        return;
    }

    cbm_regex_t re;
    int rc = cbm_regcomp(&re, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB);
    if (rc != 0) {
        sqlite3_result_error(ctx, "invalid regex", CBM_NOT_FOUND);
        return;
    }

    rc = cbm_regexec(&re, text, 0, NULL, 0);
    cbm_regfree(&re);
    sqlite3_result_int(ctx, rc == 0 ? SKIP_ONE : 0);
}

/* Case-insensitive REGEXP variant */
static void sqlite_iregexp(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *pattern = (const char *)sqlite3_value_text(argv[0]);
    const char *text = (const char *)sqlite3_value_text(argv[SKIP_ONE]);
    if (!pattern || !text) {
        sqlite3_result_int(ctx, 0);
        return;
    }

    cbm_regex_t re;
    int rc = cbm_regcomp(&re, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB | CBM_REG_ICASE);
    if (rc != 0) {
        sqlite3_result_error(ctx, "invalid regex", CBM_NOT_FOUND);
        return;
    }

    rc = cbm_regexec(&re, text, 0, NULL, 0);
    cbm_regfree(&re);
    sqlite3_result_int(ctx, rc == 0 ? SKIP_ONE : 0);
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

cbm_store_t *cbm_store_open_path_query(const char *db_path) {
    if (!db_path) {
        return NULL;
    }

    cbm_store_t *s = calloc(CBM_ALLOC_ONE, sizeof(cbm_store_t));
    if (!s) {
        return NULL;
    }

    /* Open read-write but do NOT create — returns SQLITE_CANTOPEN if absent. */
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

    /* Register REGEXP functions. */
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
        free((void *)s->db_path);
        free(s);
        return NULL;
    }

    return s;
}

/* ── Integrity check ───────────────────────────────────────────── */

bool cbm_store_check_integrity(cbm_store_t *s) {
    if (!s || !s->db) {
        return false;
    }

    /* Each project gets its own .db file, so the projects table should have
     * exactly 1 row. More than 5 rows is definitely corrupt (allows some slack
     * for edge cases). Also check that root_path looks like a real path. */
    sqlite3_stmt *stmt = NULL;
    int rc =
        sqlite3_prepare_v2(s->db, "SELECT count(*) FROM projects;", CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK) {
        return false;
    }

    bool ok = true;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        int row_count = sqlite3_column_int(stmt, 0);
        if (row_count > ST_MAX_ROW_CHECK) {
            (void)fprintf(stderr, "ERROR store.corrupt table=projects rows=%d (expected 1)\n",
                          row_count);
            ok = false;
        }
    }
    sqlite3_finalize(stmt);

    if (ok) {
        /* Check that root_path in projects table starts with '/' or a drive letter.
         * Corrupt DBs often have numeric strings like "826" in root_path. */
        rc = sqlite3_prepare_v2(
            s->db,
            "SELECT root_path FROM projects WHERE root_path != '' "
            "AND substr(root_path, 1, 1) NOT IN ('/', 'A','B','C','D','E','F','G','H') LIMIT 1;",
            CBM_NOT_FOUND, &stmt, NULL);
        if (rc == SQLITE_OK) {
            if (sqlite3_step(stmt) == SQLITE_ROW) {
                const char *bad_path = (const char *)sqlite3_column_text(stmt, 0);
                (void)fprintf(stderr, "ERROR store.corrupt table=projects bad_root_path=%s\n",
                              bad_path ? bad_path : "(null)");
                ok = false;
            }
            sqlite3_finalize(stmt);
        }
    }

    return ok;
}

cbm_store_t *cbm_store_open(const char *project) {
    if (!project) {
        return NULL;
    }
    const char *cdir = cbm_resolve_cache_dir();
    if (!cdir) {
        cdir = cbm_tmpdir();
    }
    char path[CBM_SZ_1K];
    snprintf(path, sizeof(path), "%s/%s.db", cdir, project);
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

    /* Use sqlite3_close_v2 — auto-deallocates when last statement finalizes.
     * Prevents ASan false-positive leaks from sqlite3 internal state. */
    sqlite3_close_v2(s->db);
    free((void *)s->db_path);
    free(s);
}

sqlite3 *cbm_store_get_db(cbm_store_t *s) {
    return s ? s->db : NULL;
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
    char dir[CBM_SZ_1K];
    snprintf(dir, sizeof(dir), "%s", dest_path);
    char *sl = strrchr(dir, '/');
    if (sl) {
        *sl = '\0';
        (void)cbm_mkdir(dir);
    }

    /* Write to temp file for atomic swap */
    char tmp_path[CBM_SZ_1K];
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

    rc = sqlite3_backup_step(bk, CBM_NOT_FOUND); /* copy ALL pages in one shot */
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
    *in_deg = 0;
    *out_deg = 0;

    const char *in_sql = "SELECT COUNT(*) FROM edges WHERE target_id = ?1 AND type = 'CALLS'";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, in_sql, CBM_NOT_FOUND, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, SKIP_ONE, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            *in_deg = sqlite3_column_int(stmt, 0);
        }
        sqlite3_finalize(stmt);
    }

    const char *out_sql = "SELECT COUNT(*) FROM edges WHERE source_id = ?1 AND type = 'CALLS'";
    if (sqlite3_prepare_v2(s->db, out_sql, CBM_NOT_FOUND, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, SKIP_ONE, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            *out_deg = sqlite3_column_int(stmt, 0);
        }
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
    snprintf(like_pattern, sizeof(like_pattern), "%%\"url_path\":\"%%%%%s%%%%\"%%", keyword);

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
                out[count++] = strdup(buf);
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
        out[count++] = strdup(buf);
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

/* Build basic WHERE clauses: project, label, name, file, qn patterns. */
static int search_where_basic(const cbm_search_params_t *params, char *where, int where_sz,
                              int *wlen, int *nparams, search_bind_t *binds, int *bind_idx,
                              char **like_pattern_out) {
    char bind_buf[CBM_SZ_64];

    if (params->project) {
        snprintf(bind_buf, sizeof(bind_buf), "n.project = ?%d", *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
        where_bind_text(binds, bind_idx, params->project);
    }
    if (params->label) {
        snprintf(bind_buf, sizeof(bind_buf), "n.label = ?%d", *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
        where_bind_text(binds, bind_idx, params->label);
    }
    if (params->name_pattern) {
        where_add_regex(where, where_sz, wlen, nparams, binds, bind_idx, "n.name",
                        params->name_pattern, params->case_sensitive);
    }
    if (params->qn_pattern) {
        where_add_regex(where, where_sz, wlen, nparams, binds, bind_idx, "n.qualified_name",
                        params->qn_pattern, params->case_sensitive);
    }
    if (params->file_pattern) {
        *like_pattern_out = cbm_glob_to_like(params->file_pattern);
        snprintf(bind_buf, sizeof(bind_buf), "n.file_path LIKE ?%d", *bind_idx + SKIP_ONE);
        *wlen = where_append(where, where_sz, *wlen, nparams, bind_buf);
        where_bind_text(binds, bind_idx, *like_pattern_out);
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
        /* Exclude nodes with no inbound CALLS but at least one outbound CALLS.
         * Dead code (degree=0) is NOT excluded — only true entry points. */
        *wlen = where_append(where, where_sz, *wlen, nparams,
                             "NOT (NOT EXISTS(SELECT 1 FROM edges e WHERE e.target_id = n.id "
                             "AND e.type = 'CALLS') "
                             "AND EXISTS(SELECT 1 FROM edges e2 WHERE e2.source_id = n.id "
                             "AND e2.type = 'CALLS'))");
    }
    if (params->exclude_labels) {
        char excl_clause[CBM_SZ_512];
        search_build_exclude_labels(params->exclude_labels, binds, bind_idx, excl_clause,
                                    (int)sizeof(excl_clause));
        (void)where_append(where, where_sz, *wlen, nparams, excl_clause);
    }
}

static int search_build_where(const cbm_search_params_t *params, char *where, int where_sz,
                              search_bind_t *binds, int *bind_idx, char **like_pattern_out) {
    int wlen = 0;
    int nparams = 0;
    *like_pattern_out = NULL;

    search_where_basic(params, where, where_sz, &wlen, &nparams, binds, bind_idx, like_pattern_out);
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

    const char *select_cols = "SELECT n.id, n.project, n.label, n.name, n.qualified_name, "
                              "n.file_path, n.start_line, n.end_line, n.properties, "
                              "(SELECT COUNT(*) FROM edges e WHERE e.target_id = n.id AND "
                              "e.type = 'CALLS') AS in_deg, "
                              "(SELECT COUNT(*) FROM edges e WHERE e.source_id = n.id AND "
                              "e.type = 'CALLS') AS out_deg ";

    char where[CBM_SZ_2K] = "";
    search_bind_t binds[ST_SEARCH_MAX_BINDS];
    char *like_pattern = NULL;

    int nparams =
        search_build_where(params, where, (int)sizeof(where), binds, &bind_idx, &like_pattern);

    /* Build full SQL */
    if (nparams > 0) {
        snprintf(sql, sizeof(sql), "%s FROM nodes n WHERE %s", select_cols, where);
    } else {
        snprintf(sql, sizeof(sql), "%s FROM nodes n", select_cols);
    }

    /* Degree filters */
    bool has_degree_filter = (params->min_degree >= 0 || params->max_degree >= 0);
    search_apply_degree_filter(sql, sizeof(sql), params);

    /* Count query (wrap the full query) */
    snprintf(count_sql, sizeof(count_sql), "SELECT COUNT(*) FROM (%s)", sql);

    /* Add ORDER BY + LIMIT */
    int limit = params->limit > 0 ? params->limit : ST_HALF_SEC;
    int offset = params->offset;
    const char *name_col = has_degree_filter ? "name" : "n.name";
    char order_limit[CBM_SZ_128];
    snprintf(order_limit, sizeof(order_limit), " ORDER BY %s LIMIT %d OFFSET %d", name_col, limit,
             offset);
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
        free(like_pattern);
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
        free(r->connected_names);
    }
    free(out->results);
    memset(out, 0, sizeof(*out));
}

/* ── BFS Traversal ──────────────────────────────────────────────── */

static int bfs_collect_edges(cbm_store_t *s, int64_t start_id, const cbm_node_hop_t *visited,
                             int visited_count, const char *types_clause, const char **edge_types,
                             int edge_type_count, cbm_edge_info_t **out_edges,
                             int *out_edge_count) {
    /* Build ID set: root + all visited */
    char id_set[CBM_SZ_4K];
    int ilen = snprintf(id_set, sizeof(id_set), "%lld", (long long)start_id);
    if (ilen >= (int)sizeof(id_set)) {
        ilen = (int)sizeof(id_set) - SKIP_ONE;
    }
    for (int i = 0; i < visited_count; i++) {
        ilen += snprintf(id_set + ilen, sizeof(id_set) - (size_t)ilen, ",%lld",
                         (long long)visited[i].node.id);
        if (ilen >= (int)sizeof(id_set)) {
            ilen = (int)sizeof(id_set) - SKIP_ONE;
        }
    }

    char edge_sql[ST_SQL_BUF];
    snprintf(edge_sql, sizeof(edge_sql),
             "SELECT n1.name, n2.name, e.type "
             "FROM edges e "
             "JOIN nodes n1 ON n1.id = e.source_id "
             "JOIN nodes n2 ON n2.id = e.target_id "
             "WHERE e.source_id IN (%s) AND e.target_id IN (%s) "
             "AND e.type IN (%s)",
             id_set, id_set, types_clause);

    sqlite3_stmt *estmt = NULL;
    int rc = sqlite3_prepare_v2(s->db, edge_sql, CBM_NOT_FOUND, &estmt, NULL);
    if (rc != SQLITE_OK) {
        *out_edges = NULL;
        *out_edge_count = 0;
        return CBM_STORE_OK;
    }

    if (edge_type_count > 0) {
        for (int i = 0; i < edge_type_count; i++) {
            bind_text(estmt, i + SKIP_ONE, edge_types[i]);
        }
    } else {
        bind_text(estmt, SKIP_ONE, "CALLS");
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
        edges[en].to_name = heap_strdup((const char *)sqlite3_column_text(estmt, SKIP_ONE));
        edges[en].type = heap_strdup((const char *)sqlite3_column_text(estmt, CBM_SZ_2));
        edges[en].confidence = (double)SKIP_ONE;
        en++;
    }
    sqlite3_finalize(estmt);

    *out_edges = edges;
    *out_edge_count = en;
    return CBM_STORE_OK;
}

/* Build parameterized placeholder list "?1,?2,?3" for N edge types. */
static void bfs_build_types_clause(int edge_type_count, char *buf, int buf_sz) {
    if (edge_type_count <= 0) {
        snprintf(buf, buf_sz, "?1");
        return;
    }
    int tlen = 0;
    for (int i = 0; i < edge_type_count; i++) {
        if (i > 0) {
            tlen += snprintf(buf + tlen, buf_sz - tlen, ",");
            if (tlen >= buf_sz) {
                tlen = buf_sz - SKIP_ONE;
            }
        }
        tlen += snprintf(buf + tlen, buf_sz - tlen, "?%d", i + SKIP_ONE);
        if (tlen >= buf_sz) {
            tlen = buf_sz - SKIP_ONE;
        }
    }
}

int cbm_store_bfs(cbm_store_t *s, int64_t start_id, const char *direction, const char **edge_types,
                  int edge_type_count, int max_depth, int max_results, cbm_traverse_result_t *out) {
    memset(out, 0, sizeof(*out));

    cbm_node_t root = {0};
    int rc = cbm_store_find_node_by_id(s, start_id, &root);
    if (rc != CBM_STORE_OK) {
        return rc;
    }
    out->root = root;

    char types_clause[CBM_SZ_512];
    bfs_build_types_clause(edge_type_count, types_clause, (int)sizeof(types_clause));

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
    rc = sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL);
    if (rc != SQLITE_OK) {
        store_set_error_sqlite(s, "bfs prepare");
        return CBM_STORE_ERR;
    }

    /* Bind edge type parameters */
    if (edge_type_count > 0) {
        for (int i = 0; i < edge_type_count; i++) {
            bind_text(stmt, i + SKIP_ONE, edge_types[i]);
        }
    } else {
        bind_text(stmt, SKIP_ONE, "CALLS");
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
        n++;
    }

    sqlite3_finalize(stmt);

    out->visited = visited;
    out->visited_count = n;

    /* Collect edges between visited nodes (including root) */
    if (n > 0) {
        bfs_collect_edges(s, start_id, out->visited, n, types_clause, edge_types, edge_type_count,
                          &out->edges, &out->edge_count);
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

int cbm_store_get_schema(cbm_store_t *s, const char *project, cbm_schema_info_t *out) {
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
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (n >= cap) {
                cap *= ST_GROWTH;
                arr = safe_realloc(arr, cap * sizeof(cbm_label_count_t));
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
    {
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
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            if (n >= cap) {
                cap *= ST_GROWTH;
                arr = safe_realloc(arr, cap * sizeof(cbm_type_count_t));
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
    {
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

void cbm_store_schema_free(cbm_schema_info_t *out) {
    if (!out) {
        return;
    }
    for (int i = 0; i < out->node_label_count; i++) {
        free((void *)out->node_labels[i].label);
        for (int j = 0; j < out->node_labels[i].property_count; j++) {
            free(out->node_labels[i].properties[j]);
        }
        free(out->node_labels[i].properties);
    }
    free(out->node_labels);

    for (int i = 0; i < out->edge_type_count; i++) {
        free((void *)out->edge_types[i].type);
        for (int j = 0; j < out->edge_types[i].property_count; j++) {
            free(out->edge_types[i].properties[j]);
        }
        free(out->edge_types[i].properties);
    }
    free(out->edge_types);

    for (int i = 0; i < out->rel_pattern_count; i++) {
        free((void *)out->rel_patterns[i]);
    }
    free(out->rel_patterns);

    for (int i = 0; i < out->sample_func_count; i++) {
        free((void *)out->sample_func_names[i]);
    }
    free(out->sample_func_names);

    for (int i = 0; i < out->sample_class_count; i++) {
        free((void *)out->sample_class_names[i]);
    }
    free(out->sample_class_names);

    for (int i = 0; i < out->sample_qn_count; i++) {
        free((void *)out->sample_qns[i]);
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

static int arch_languages(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT file_path FROM nodes WHERE project=?1 AND label='File'";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_languages");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    /* Count per language using a simple parallel array */
    const char *lang_names[CBM_SZ_64];
    int lang_counts[CBM_SZ_64];
    int nlang = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW) {
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

    out->languages = (nlang > 0) ? calloc(nlang, sizeof(cbm_language_count_t)) : NULL;
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
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_entry_points");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_entry_point_t *arr = calloc(cap, sizeof(cbm_entry_point_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
            arr = safe_realloc(arr, cap * sizeof(cbm_entry_point_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].qualified_name = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
        arr[n].file = heap_strdup((const char *)sqlite3_column_text(stmt, CBM_SZ_2));
        n++;
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

static int arch_routes(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT name, properties, COALESCE(file_path, '') FROM nodes "
                      "WHERE project=?1 AND label='Route' "
                      "AND (json_extract(properties, '$.is_test') IS NULL OR "
                      "json_extract(properties, '$.is_test') != 1) "
                      "LIMIT 20";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_routes");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_route_info_t *arr = calloc(cap, sizeof(cbm_route_info_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *name = (const char *)sqlite3_column_text(stmt, 0);
        const char *props = (const char *)sqlite3_column_text(stmt, SKIP_ONE);
        const char *fp = (const char *)sqlite3_column_text(stmt, CBM_SZ_2);
        if (cbm_is_test_file_path(fp)) {
            continue;
        }
        if (n >= cap) {
            cap *= ST_GROWTH;
            arr = safe_realloc(arr, cap * sizeof(cbm_route_info_t));
        }

        arr[n].method = heap_strdup("");
        arr[n].path = heap_strdup(name);
        arr[n].handler = heap_strdup("");

        char *val;
        val = extract_json_string_prop(props, "\"method\"", ST_METHOD_PROP_LEN);
        if (val) {
            free((void *)arr[n].method);
            arr[n].method = val;
        }
        val = extract_json_string_prop(props, "\"path\"", ST_PATH_PROP_LEN);
        if (val) {
            free((void *)arr[n].path);
            arr[n].path = val;
        }
        val = extract_json_string_prop(props, "\"handler\"", ST_HANDLER_PROP_LEN);
        if (val) {
            free((void *)arr[n].handler);
            arr[n].handler = val;
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
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_hotspots");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    int cap = ST_INIT_CAP_8;
    int n = 0;
    cbm_hotspot_t *arr = calloc(cap, sizeof(cbm_hotspot_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
            arr = safe_realloc(arr, cap * sizeof(cbm_hotspot_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].qualified_name = heap_strdup((const char *)sqlite3_column_text(stmt, SKIP_ONE));
        arr[n].fan_in = sqlite3_column_int(stmt, CBM_SZ_2);
        n++;
    }
    sqlite3_finalize(stmt);
    out->hotspots = arr;
    out->hotspot_count = n;
    return CBM_STORE_OK;
}

/* Look up package name for a node ID in the parallel arrays. */
static const char *lookup_pkg(const int64_t *nids, char **npkgs, int nn, int64_t id) {
    for (int i = 0; i < nn; i++) {
        if (nids[i] == id) {
            return npkgs[i];
        }
    }
    return NULL;
}

/* Accumulate a cross-package boundary into parallel arrays. */
static void accum_boundary(const char *src_pkg, const char *tgt_pkg, char **bfroms, char **btos,
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
        bfroms[*bn] = heap_strdup(src_pkg);
        btos[*bn] = heap_strdup(tgt_pkg);
        bcounts[*bn] = SKIP_ONE;
        (*bn)++;
    }
}

static int arch_boundaries(cbm_store_t *s, const char *project, cbm_cross_pkg_boundary_t **out_arr,
                           int *out_count) {
    /* Build nodeID → package map */
    const char *nsql = "SELECT id, qualified_name FROM nodes WHERE project=?1 AND label IN "
                       "('Function','Method','Class')";
    sqlite3_stmt *nstmt = NULL;
    if (sqlite3_prepare_v2(s->db, nsql, CBM_NOT_FOUND, &nstmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_boundaries_nodes");
        return CBM_STORE_ERR;
    }
    bind_text(nstmt, SKIP_ONE, project);

    int ncap = CBM_SZ_256;
    int nn = 0;
    int64_t *nids = malloc(ncap * sizeof(int64_t));
    char **npkgs = malloc(ncap * sizeof(char *));

    while (sqlite3_step(nstmt) == SQLITE_ROW) {
        if (nn >= ncap) {
            ncap *= ST_GROWTH;
            nids = safe_realloc(nids, ncap * sizeof(int64_t));
            npkgs = safe_realloc(npkgs, ncap * sizeof(char *));
        }
        nids[nn] = sqlite3_column_int64(nstmt, 0);
        const char *qn = (const char *)sqlite3_column_text(nstmt, SKIP_ONE);
        npkgs[nn] = heap_strdup(cbm_qn_to_package(qn));
        nn++;
    }
    sqlite3_finalize(nstmt);

    /* Scan edges, count cross-package calls */
    const char *esql = "SELECT source_id, target_id FROM edges WHERE project=?1 AND type='CALLS'";
    sqlite3_stmt *estmt = NULL;
    if (sqlite3_prepare_v2(s->db, esql, CBM_NOT_FOUND, &estmt, NULL) != SQLITE_OK) {
        for (int i = 0; i < nn; i++) {
            free(npkgs[i]);
        }
        free(nids);
        free(npkgs);
        store_set_error_sqlite(s, "arch_boundaries_edges");
        return CBM_STORE_ERR;
    }
    bind_text(estmt, SKIP_ONE, project);

    int bcap = CBM_SZ_32;
    int bn = 0;
    char **bfroms = malloc(bcap * sizeof(char *));
    char **btos = malloc(bcap * sizeof(char *));
    int *bcounts = malloc(bcap * sizeof(int));

    while (sqlite3_step(estmt) == SQLITE_ROW) {
        int64_t src_id = sqlite3_column_int64(estmt, 0);
        int64_t tgt_id = sqlite3_column_int64(estmt, SKIP_ONE);
        const char *src_pkg = lookup_pkg(nids, npkgs, nn, src_id);
        const char *tgt_pkg = lookup_pkg(nids, npkgs, nn, tgt_id);
        if (!src_pkg || !tgt_pkg || !src_pkg[0] || !tgt_pkg[0] || strcmp(src_pkg, tgt_pkg) == 0) {
            continue;
        }
        accum_boundary(src_pkg, tgt_pkg, bfroms, btos, bcounts, &bn, bcap);
    }
    sqlite3_finalize(estmt);
    for (int i = 0; i < nn; i++) {
        free(npkgs[i]);
    }
    free(nids);
    free(npkgs);

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
static int arch_packages_from_qn(cbm_store_t *s, const char *project,
                                 cbm_package_summary_t **out_arr, int *out_count) {
    const char *qsql = "SELECT qualified_name FROM nodes WHERE project=?1 AND label IN "
                       "('Function','Method','Class')";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, qsql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_packages_qn");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    char *pnames[CBM_SZ_64];
    int pcounts[CBM_SZ_64];
    int np = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
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
            pcounts[np] = SKIP_ONE;
            np++;
        }
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
    for (int i = 0; i < np; i++) {
        arr[i].name = pnames[i];
        arr[i].node_count = pcounts[i];
    }
    *out_arr = arr;
    *out_count = np;
    return CBM_STORE_OK;
}

static int arch_packages(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    /* Try Package nodes first */
    const char *sql =
        "SELECT n.name, COUNT(*) as cnt FROM nodes n "
        "WHERE n.project=?1 AND n.label='Package' GROUP BY n.name ORDER BY cnt DESC LIMIT 15";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_packages");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    int cap = ST_INIT_CAP_16;
    int n = 0;
    cbm_package_summary_t *arr = calloc(cap, sizeof(cbm_package_summary_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
            arr = safe_realloc(arr, cap * sizeof(cbm_package_summary_t));
        }
        arr[n].name = heap_strdup((const char *)sqlite3_column_text(stmt, 0));
        arr[n].node_count = sqlite3_column_int(stmt, SKIP_ONE);
        n++;
    }
    sqlite3_finalize(stmt);

    /* Fallback: group by QN segment if no Package nodes */
    if (n == 0) {
        free(arr);
        int rc = arch_packages_from_qn(s, project, &arr, &n);
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

/* Find or insert a package name, returning its index. Returns -1 if full. */
static int find_or_add_pkg(char **all_pkgs, int *npkgs, int max_pkgs, const char *pkg) {
    for (int j = 0; j < *npkgs; j++) {
        if (strcmp(all_pkgs[j], pkg) == 0) {
            return j;
        }
    }
    if (*npkgs < max_pkgs) {
        int idx = *npkgs;
        all_pkgs[idx] = heap_strdup(pkg);
        (*npkgs)++;
        return idx;
    }
    return CBM_NOT_FOUND;
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

/* Collect package names from nodes matching a SQL query. */
static int collect_pkg_names(cbm_store_t *s, const char *sql, const char *project, char **pkgs,
                             int max_pkgs) {
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK || !stmt) {
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        return CBM_NOT_FOUND;
    }
    bind_text(stmt, SKIP_ONE, project);
    int count = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && count < max_pkgs) {
        const char *qn = (const char *)sqlite3_column_text(stmt, 0);
        pkgs[count++] = heap_strdup(cbm_qn_to_package(qn));
    }
    sqlite3_finalize(stmt);
    return count;
}

static int arch_layers(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    /* Get boundaries for fan analysis */
    cbm_cross_pkg_boundary_t *boundaries = NULL;
    int bcount = 0;
    int rc = arch_boundaries(s, project, &boundaries, &bcount);
    if (rc != CBM_STORE_OK) {
        return rc;
    }

    /* Collect route and entry point packages */
    char *route_pkgs[CBM_SZ_32];
    int nrpkgs =
        collect_pkg_names(s, "SELECT qualified_name FROM nodes WHERE project=?1 AND label='Route'",
                          project, route_pkgs, CBM_SZ_32);

    char *entry_pkgs[CBM_SZ_32];
    int nepkgs = collect_pkg_names(s,
                                   "SELECT qualified_name FROM nodes WHERE project=?1 AND "
                                   "json_extract(properties, '$.is_entry_point') = 1",
                                   project, entry_pkgs, CBM_SZ_32);

    /* Compute fan-in/out per package */
    char *all_pkgs[CBM_SZ_64];
    int fan_in[CBM_SZ_64];
    int fan_out[CBM_SZ_64];
    int npkgs = 0;
    memset(fan_in, 0, sizeof(fan_in));
    memset(fan_out, 0, sizeof(fan_out));

    for (int i = 0; i < bcount; i++) {
        int fi = find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, boundaries[i].from);
        if (fi >= 0) {
            fan_out[fi] += boundaries[i].call_count;
        }
        int ti = find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, boundaries[i].to);
        if (ti >= 0) {
            fan_in[ti] += boundaries[i].call_count;
        }
    }

    /* Also include route/entry packages */
    for (int i = 0; i < nrpkgs; i++) {
        find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, route_pkgs[i]);
    }
    for (int i = 0; i < nepkgs; i++) {
        find_or_add_pkg(all_pkgs, &npkgs, ST_MAX_PKGS, entry_pkgs[i]);
    }

    /* Classify each package */
    out->layers = (npkgs > 0) ? calloc(npkgs, sizeof(cbm_package_layer_t)) : NULL;
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

/* Add a child to a dir entry if not already present. */
static void dir_add_child(char ***children, int *child_count, int *child_cap, const char *child) {
    for (int k = 0; k < *child_count; k++) {
        if (strcmp((*children)[k], child) == 0) {
            return;
        }
    }
    if (*child_count >= *child_cap) {
        *child_cap = *child_cap ? *child_cap * PAIR_LEN : ST_INIT_CAP_4;
        *children = realloc(*children, *child_cap * sizeof(char *));
    }
    (*children)[(*child_count)++] = heap_strdup(child);
}

/* Find or create a directory entry by path. Returns its index, or -1 if full. */
static int dir_find_or_create(char **dir_paths, int *dir_child_counts, char ***dir_children,
                              int *dir_children_caps, int *dn, int dcap, const char *dir) {
    for (int i = 0; i < *dn; i++) {
        if (strcmp(dir_paths[i], dir) == 0) {
            return i;
        }
    }
    if (*dn < dcap) {
        int idx = *dn;
        dir_paths[idx] = heap_strdup(dir);
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
    strncpy(buf, fp, buf_sz - SKIP_ONE);
    buf[buf_sz - SKIP_ONE] = '\0';
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
static void arch_register_file_dirs(const char *fp, char **dir_paths, int *dir_child_counts,
                                    char ***dir_children, int *dir_children_caps, int *dn,
                                    int dcap) {
    char tmp[CBM_SZ_512];
    char *parts[ST_SEARCH_MAX_BINDS];
    int nparts = split_path_parts(fp, tmp, (int)sizeof(tmp), parts, ST_SEARCH_MAX_BINDS);

    int ri = dir_find_or_create(dir_paths, dir_child_counts, dir_children, dir_children_caps, dn,
                                dcap, "");
    if (ri >= 0 && nparts > 0) {
        dir_add_child(&dir_children[ri], &dir_child_counts[ri], &dir_children_caps[ri], parts[0]);
    }

    for (int depth = 0; depth < nparts - SKIP_ONE && depth < ST_MAX_PATH_DEPTH; depth++) {
        char dir[CBM_SZ_512] = "";
        for (int k = 0; k <= depth; k++) {
            if (k > 0) {
                strcat(dir, "/");
            }
            strcat(dir, parts[k]);
        }
        const char *child = (depth + SKIP_ONE < nparts) ? parts[depth + SKIP_ONE] : NULL;
        if (!child) {
            continue;
        }
        int di = dir_find_or_create(dir_paths, dir_child_counts, dir_children, dir_children_caps,
                                    dn, dcap, dir);
        if (di >= 0) {
            dir_add_child(&dir_children[di], &dir_child_counts[di], &dir_children_caps[di], child);
        }
    }
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
static void push_tree_entry(cbm_file_tree_entry_t **entries, int *en, int *ecap,
                            cbm_file_tree_entry_t e) {
    if (*en >= *ecap) {
        *ecap *= ST_GROWTH;
        *entries = safe_realloc(*entries, *ecap * sizeof(cbm_file_tree_entry_t));
    }
    (*entries)[(*en)++] = e;
}

/* Collect tree entries from dir arrays. */
static void arch_collect_entries(char **dir_paths, int *dir_child_counts, char ***dir_children,
                                 int dn, char **files, int fn, cbm_file_tree_entry_t **entries_out,
                                 int *en_out) {
    int ecap = CBM_SZ_64;
    int en = 0;
    cbm_file_tree_entry_t *entries = calloc(ecap, sizeof(cbm_file_tree_entry_t));

    /* Root children */
    for (int i = 0; i < dn; i++) {
        if (strcmp(dir_paths[i], "") != 0) {
            continue;
        }
        for (int k = 0; k < dir_child_counts[i]; k++) {
            push_tree_entry(
                &entries, &en, &ecap,
                make_tree_entry(dir_children[i][k], files, fn, dir_paths, dir_child_counts, dn));
        }
    }

    /* Non-root dir children (depth < ST_COL_3) */
    for (int i = 0; i < dn; i++) {
        if (strcmp(dir_paths[i], "") == 0 || count_slashes(dir_paths[i]) >= ST_MAX_PATH_DEPTH) {
            continue;
        }
        for (int k = 0; k < dir_child_counts[i]; k++) {
            char path[CBM_SZ_512];
            snprintf(path, sizeof(path), "%s/%s", dir_paths[i], dir_children[i][k]);
            push_tree_entry(&entries, &en, &ecap,
                            make_tree_entry(path, files, fn, dir_paths, dir_child_counts, dn));
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

static int arch_file_tree(cbm_store_t *s, const char *project, cbm_architecture_info_t *out) {
    const char *sql = "SELECT file_path FROM nodes WHERE project=?1 AND label='File'";
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "arch_file_tree");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    int fcap = CBM_SZ_32;
    int fn = 0;
    char **files = malloc(fcap * sizeof(char *));

    int dcap = CBM_SZ_64;
    int dn = 0;
    char **dir_paths = calloc(dcap, sizeof(char *));
    int *dir_child_counts = calloc(dcap, sizeof(int));
    char ***dir_children = calloc(dcap, sizeof(char **));
    int *dir_children_caps = calloc(dcap, sizeof(int));

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *fp = (const char *)sqlite3_column_text(stmt, 0);
        if (!fp) {
            continue;
        }
        if (fn >= fcap) {
            fcap *= ST_GROWTH;
            files = safe_realloc(files, fcap * sizeof(char *));
        }
        files[fn++] = heap_strdup(fp);
        arch_register_file_dirs(fp, dir_paths, dir_child_counts, dir_children, dir_children_caps,
                                &dn, dcap);
    }
    sqlite3_finalize(stmt);

    arch_collect_entries(dir_paths, dir_child_counts, dir_children, dn, files, fn, &out->file_tree,
                         &out->file_tree_count);

    arch_free_dirs(dir_paths, dir_child_counts, dir_children, dir_children_caps, dn, files, fn);
    return CBM_STORE_OK;
}

/* ── Louvain community detection ───────────────────────────────── */

/* Build deduplicated, normalized edge weight arrays from raw edges.
 * Returns total number of unique edges in *out_wn. */
/* Find the index of a node ID in the nodes array, or -1. */
static int louvain_node_index(const int64_t *nodes, int n, int64_t id) {
    for (int i = 0; i < n; i++) {
        if (nodes[i] == id) {
            return i;
        }
    }
    return CBM_NOT_FOUND;
}

static void louvain_build_weights(const int64_t *nodes, int n, const cbm_louvain_edge_t *edges,
                                  int edge_count, int **out_wsi, int **out_wdi, double **out_ww,
                                  int *out_wn) {
    int wcap = edge_count > 0 ? edge_count : SKIP_ONE;
    int wn = 0;
    int *wsi = malloc(wcap * sizeof(int));
    int *wdi = malloc(wcap * sizeof(int));
    double *ww = malloc(wcap * sizeof(double));

    for (int e = 0; e < edge_count; e++) {
        int si = louvain_node_index(nodes, n, edges[e].src);
        int di = louvain_node_index(nodes, n, edges[e].dst);
        if (si < 0 || di < 0 || si == di) {
            continue;
        }
        if (si > di) {
            int tmp = si;
            si = di;
            di = tmp;
        }
        int found = ST_FOUND;
        for (int i = 0; i < wn; i++) {
            if (wsi[i] == si && wdi[i] == di) {
                found = i;
                break;
            }
        }
        if (found >= 0) {
            ww[found] += (double)SKIP_ONE;
        } else {
            if (wn >= wcap) {
                wcap *= ST_GROWTH;
                wsi = safe_realloc(wsi, wcap * sizeof(int));
                wdi = safe_realloc(wdi, wcap * sizeof(int));
                ww = safe_realloc(ww, wcap * sizeof(double));
            }
            wsi[wn] = si;
            wdi[wn] = di;
            ww[wn] = (double)SKIP_ONE;
            wn++;
        }
    }
    *out_wsi = wsi;
    *out_wdi = wdi;
    *out_ww = ww;
    *out_wn = wn;
}

/* Add a bidirectional edge to adjacency lists. */
static void adj_add_edge(int **adj, double **adj_w, int *adj_n, int *adj_cap, int si, int di,
                         double w) {
    if (adj_n[si] >= adj_cap[si]) {
        int new_cap = adj_cap[si] ? adj_cap[si] * ST_GROWTH : ST_INIT_CAP_4;
        int *new_adj = safe_realloc(adj[si], new_cap * sizeof(int));
        double *new_w = safe_realloc(adj_w[si], new_cap * sizeof(double));
        /* Zero-init new slots to satisfy static analyzer */
        for (int k = adj_cap[si]; k < new_cap; k++) {
            new_adj[k] = 0;
            new_w[k] = 0.0;
        }
        adj[si] = new_adj;
        adj_w[si] = new_w;
        adj_cap[si] = new_cap;
    }
    adj[si][adj_n[si]] = di;
    adj_w[si][adj_n[si]] = w;
    adj_n[si]++;

    if (adj_n[di] >= adj_cap[di]) {
        adj_cap[di] = adj_cap[di] ? adj_cap[di] * ST_GROWTH : ST_INIT_CAP_4;
        adj[di] = safe_realloc(adj[di], adj_cap[di] * sizeof(int));
        adj_w[di] = safe_realloc(adj_w[di], adj_cap[di] * sizeof(double));
    }
    adj[di][adj_n[di]] = si;
    adj_w[di][adj_n[di]] = w;
    adj_n[di]++;
}

/* Run one Louvain iteration. Returns true if any node moved. */
/* Linear congruential generator (glibc constants) */
#define LCG_MULTIPLIER 1103515245U
#define LCG_INCREMENT 12345U

static bool louvain_iteration(int n, int **adj, double **adj_w, const int *adj_n, int *community,
                              const double *degree, double total_weight, int iter) {
    bool improved = false;

    double *comm_degree = calloc(n, sizeof(double));
    for (int i = 0; i < n; i++) {
        comm_degree[community[i]] += degree[i];
    }

    int *order = calloc(n, sizeof(int));
    for (int i = 0; i < n; i++) {
        order[i] = i;
    }
    unsigned int seed = (unsigned int)((iter * ST_GRAPH_SEED_MULT) + n);
    for (int i = n - SKIP_ONE; i > 0; i--) {
        seed = (seed * LCG_MULTIPLIER) + LCG_INCREMENT;
        int j = (int)((seed >> ST_BUF_16) % (unsigned int)(i + SKIP_ONE));
        int tmp = order[i];
        order[i] = order[j];
        order[j] = tmp;
    }

    for (int oi = 0; oi < n; oi++) {
        int i = order[oi];
        int cur_comm = community[i];

        double *nc_weight = calloc(n, sizeof(double));
        bool *nc_seen = calloc(n, sizeof(bool));
        for (int j = 0; j < adj_n[i]; j++) {
            int nc = community[adj[i][j]];
            nc_weight[nc] += adj_w[i][j];
            nc_seen[nc] = true;
        }

        comm_degree[cur_comm] -= degree[i];

        int best_comm = cur_comm;
        double best_gain = 0.0;
        for (int c = 0; c < n; c++) {
            if (!nc_seen[c]) {
                continue;
            }
            double gain =
                nc_weight[c] - (degree[i] * comm_degree[c] / (ST_WEIGHT_2 * total_weight));
            if (gain > best_gain) {
                best_gain = gain;
                best_comm = c;
            }
        }

        double cur_gain = nc_weight[cur_comm] -
                          (degree[i] * comm_degree[cur_comm] / (ST_WEIGHT_2 * total_weight));
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
    return improved;
}

static void louvain_free_adj(int **adj, double **adj_w, int *adj_n, int *adj_cap, int n) {
    for (int i = 0; i < n; i++) {
        free(adj[i]);
        free(adj_w[i]);
    }
    free(adj);
    free(adj_w);
    free(adj_n);
    free(adj_cap);
}

int cbm_louvain(const int64_t *nodes, int node_count, const cbm_louvain_edge_t *edges,
                int edge_count, cbm_louvain_result_t **out, int *out_count) {
    if (node_count <= 0) {
        *out = NULL;
        *out_count = 0;
        return CBM_STORE_OK;
    }

    int n = node_count;

    /* Build deduplicated edge weights */
    int *wsi;
    int *wdi;
    double *ww;
    int wn;
    louvain_build_weights(nodes, n, edges, edge_count, &wsi, &wdi, &ww, &wn);

    /* Build adjacency lists */
    int **adj = calloc(n, sizeof(int *));
    double **adj_w = calloc(n, sizeof(double *));
    int *adj_n = calloc(n, sizeof(int));
    int *adj_cap = calloc(n, sizeof(int));
    if (!adj || !adj_w || !adj_n || !adj_cap) {
        free(adj);
        free(adj_w);
        free(adj_n);
        free(adj_cap);
        free(wsi);
        free(wdi);
        free(ww);
        return CBM_NOT_FOUND;
    }

    double total_weight = 0;
    for (int i = 0; i < wn; i++) {
        total_weight += ww[i];
        adj_add_edge(adj, adj_w, adj_n, adj_cap, wsi[i], wdi[i], ww[i]);
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
        cbm_louvain_result_t *result = malloc(n * sizeof(cbm_louvain_result_t));
        for (int i = 0; i < n; i++) {
            result[i].node_id = nodes[i];
            result[i].community = i;
        }
        *out = result;
        *out_count = n;
        free(community);
        louvain_free_adj(adj, adj_w, adj_n, adj_cap, n);
        return CBM_STORE_OK;
    }

    /* Compute node degrees */
    double *degree = calloc(n, sizeof(double));
    if (!degree) {
        free(community);
        louvain_free_adj(adj, adj_w, adj_n, adj_cap, n);
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < n; i++) {
        if (adj_w[i]) {
            for (int j = 0; j < adj_n[i]; j++) {
                degree[i] += adj_w[i][j];
            }
        }
    }

    /* Main Louvain loop (10 iterations max) */
    for (int iter = 0; iter < ST_MAX_ITERATIONS; iter++) {
        if (!louvain_iteration(n, adj, adj_w, adj_n, community, degree, total_weight, iter)) {
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
    louvain_free_adj(adj, adj_w, adj_n, adj_cap, n);
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
        free(out->clusters[i].top_nodes);
        for (int j = 0; j < out->clusters[i].package_count; j++) {
            free((void *)out->clusters[i].packages[j]);
        }
        free(out->clusters[i].packages);
        for (int j = 0; j < out->clusters[i].edge_type_count; j++) {
            free((void *)out->clusters[i].edge_types[j]);
        }
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
    if (sqlite3_prepare_v2(s->db, sql, CBM_NOT_FOUND, &stmt, NULL) != SQLITE_OK) {
        store_set_error_sqlite(s, "find_arch_docs");
        return CBM_STORE_ERR;
    }
    bind_text(stmt, SKIP_ONE, project);

    int cap = ST_INIT_CAP_8;
    int n = 0;
    char **arr = malloc(cap * sizeof(char *));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) {
            cap *= ST_GROWTH;
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
    int idx = (*count)++;
    results[idx].node_id = sqlite3_column_int64(stmt, 0);
    const char *name = (const char *)sqlite3_column_text(stmt, SKIP_ONE);
    const char *qn = (const char *)sqlite3_column_text(stmt, ST_COL_2);
    const char *fp = (const char *)sqlite3_column_text(stmt, ST_COL_3);
    const char *label = (const char *)sqlite3_column_text(stmt, ST_COL_4);
    results[idx].name = name ? strdup(name) : strdup("");
    results[idx].qualified_name = qn ? strdup(qn) : strdup("");
    results[idx].file_path = fp ? strdup(fp) : strdup("");
    results[idx].label = label ? strdup(label) : strdup("");
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
        (void)fprintf(stderr, "vector_search: %s\n", sqlite3_errmsg(s->db));
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
    while ((step_rc = sqlite3_step(stmt)) == SQLITE_ROW) {
        cbm_vector_result_t *grown =
            vs_append_result(results, &count, &cap, stmt, kw_vecs, actual_kw);
        if (!grown) {
            break;
        }
        results = grown;
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
