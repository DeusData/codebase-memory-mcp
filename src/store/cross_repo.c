/*
 * cross_repo.c — Cross-repository index: build, search, channel matching.
 *
 * Scans all per-project .db files to build a unified _cross_repo.db with:
 * - cross_channels: all channel emit/listen from every repo
 * - cross_nodes: Function/Method/Class/Interface/Route stubs from all repos
 * - cross_nodes_fts: BM25 FTS5 index with camelCase splitting
 * - cross_embeddings: semantic vectors copied from per-project DBs
 *
 * The cross-repo DB is a standard SQLite file — no ATTACH needed.
 * Built by scanning each project DB via cbm_store_open_path_query().
 */

#include "store/cross_repo.h"
#include "store/store.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <sqlite3.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Helpers ────────────────────────────────────────────────────── */

static _Thread_local char _itoa[32];
static const char *itoa_cr(int v) { snprintf(_itoa, sizeof(_itoa), "%d", v); return _itoa; }

static const char *get_cross_repo_path(void) {
    static char path[1024];
    const char *home = getenv("HOME");
    if (!home) home = getenv("USERPROFILE");
    if (!home) return NULL;
    snprintf(path, sizeof(path), "%s/.cache/codebase-memory-mcp/_cross_repo.db", home);
    return path;
}

static const char *get_cache_dir(void) {
    static char dir[1024];
    const char *home = getenv("HOME");
    if (!home) home = getenv("USERPROFILE");
    if (!home) return NULL;
    snprintf(dir, sizeof(dir), "%s/.cache/codebase-memory-mcp", home);
    return dir;
}

/* CamelCase splitter — same as store.c. Duplicated to keep cross_repo self-contained. */
static void sqlite_camel_split_cr(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    const char *input = (const char *)sqlite3_value_text(argv[0]);
    if (!input || !input[0]) {
        sqlite3_result_text(ctx, input ? input : "", -1, SQLITE_TRANSIENT);
        return;
    }
    char buf[2048];
    int len = snprintf(buf, sizeof(buf), "%s ", input);
    for (int i = 0; input[i] && len < (int)sizeof(buf) - 2; i++) {
        if (i > 0) {
            bool split = false;
            if (input[i] >= 'A' && input[i] <= 'Z' &&
                input[i - 1] >= 'a' && input[i - 1] <= 'z') split = true;
            if (input[i] >= 'A' && input[i] <= 'Z' &&
                input[i - 1] >= 'A' && input[i - 1] <= 'Z' &&
                input[i + 1] >= 'a' && input[i + 1] <= 'z') split = true;
            if (split) buf[len++] = ' ';
        }
        buf[len++] = input[i];
    }
    buf[len] = '\0';
    sqlite3_result_text(ctx, buf, len, SQLITE_TRANSIENT);
}

/* Cosine similarity — same as store.c. */
static void sqlite_cosine_sim_cr(sqlite3_context *ctx, int argc, sqlite3_value **argv) {
    (void)argc;
    if (sqlite3_value_type(argv[0]) != SQLITE_BLOB ||
        sqlite3_value_type(argv[1]) != SQLITE_BLOB) {
        sqlite3_result_null(ctx); return;
    }
    const float *a = (const float *)sqlite3_value_blob(argv[0]);
    const float *b = (const float *)sqlite3_value_blob(argv[1]);
    int a_bytes = sqlite3_value_bytes(argv[0]);
    int b_bytes = sqlite3_value_bytes(argv[1]);
    if (a_bytes != b_bytes || a_bytes == 0 || (a_bytes % (int)sizeof(float)) != 0) {
        sqlite3_result_null(ctx); return;
    }
    int dims = a_bytes / (int)sizeof(float);
    float dot = 0.0f, na = 0.0f, nb = 0.0f;
    for (int i = 0; i < dims; i++) {
        dot += a[i] * b[i]; na += a[i] * a[i]; nb += b[i] * b[i];
    }
    if (na == 0.0f || nb == 0.0f) { sqlite3_result_double(ctx, 0.0); return; }
    sqlite3_result_double(ctx, (double)dot / (sqrt((double)na) * sqrt((double)nb)));
}

/* ── Cross-Repo Handle ──────────────────────────────────────────── */

struct cbm_cross_repo {
    sqlite3 *db;
};

cbm_cross_repo_t *cbm_cross_repo_open(void) {
    const char *path = get_cross_repo_path();
    if (!path) return NULL;

    sqlite3 *db = NULL;
    if (sqlite3_open_v2(path, &db, SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        return NULL;
    }

    /* Register custom functions */
    sqlite3_create_function(db, "cbm_camel_split", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            NULL, sqlite_camel_split_cr, NULL, NULL);
    sqlite3_create_function(db, "cbm_cosine_sim", 2, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            NULL, sqlite_cosine_sim_cr, NULL, NULL);

    cbm_cross_repo_t *cr = calloc(1, sizeof(cbm_cross_repo_t));
    if (!cr) { sqlite3_close(db); return NULL; }
    cr->db = db;
    return cr;
}

void cbm_cross_repo_close(cbm_cross_repo_t *cr) {
    if (!cr) return;
    if (cr->db) sqlite3_close(cr->db);
    free(cr);
}

/* ── Build ──────────────────────────────────────────────────────── */

static const char *CROSS_SCHEMA =
    "CREATE TABLE IF NOT EXISTS cross_channels ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  channel_name TEXT NOT NULL,"
    "  transport TEXT NOT NULL,"
    "  direction TEXT NOT NULL,"
    "  project TEXT NOT NULL,"
    "  file_path TEXT NOT NULL DEFAULT '',"
    "  function_name TEXT NOT NULL DEFAULT '',"
    "  node_id INTEGER NOT NULL DEFAULT 0"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_xch_name ON cross_channels(channel_name);"
    "CREATE INDEX IF NOT EXISTS idx_xch_project ON cross_channels(project);"
    "CREATE TABLE IF NOT EXISTS cross_nodes ("
    "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
    "  project TEXT NOT NULL,"
    "  orig_id INTEGER NOT NULL,"
    "  label TEXT NOT NULL,"
    "  name TEXT NOT NULL,"
    "  qualified_name TEXT NOT NULL,"
    "  file_path TEXT NOT NULL DEFAULT ''"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_xn_project ON cross_nodes(project);"
    "CREATE INDEX IF NOT EXISTS idx_xn_name ON cross_nodes(name);"
    "CREATE INDEX IF NOT EXISTS idx_xn_proj_orig ON cross_nodes(project, orig_id);"
    "CREATE TABLE IF NOT EXISTS cross_embeddings ("
    "  node_id INTEGER PRIMARY KEY,"
    "  project TEXT NOT NULL,"
    "  embedding BLOB NOT NULL,"
    "  dimensions INTEGER NOT NULL"
    ");"
    "CREATE INDEX IF NOT EXISTS idx_xe_project ON cross_embeddings(project);"
    "CREATE TABLE IF NOT EXISTS cross_meta ("
    "  key TEXT PRIMARY KEY,"
    "  value TEXT NOT NULL"
    ");";

static const char *CROSS_FTS =
    "CREATE VIRTUAL TABLE IF NOT EXISTS cross_nodes_fts USING fts5("
    "name, qualified_name, label, file_path, project,"
    "content='',"
    "tokenize='unicode61 remove_diacritics 2'"
    ");";

cbm_cross_repo_stats_t cbm_cross_repo_build(void) {
    cbm_cross_repo_stats_t stats = {0};
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    const char *db_path = get_cross_repo_path();
    const char *cache_dir = get_cache_dir();
    if (!db_path || !cache_dir) {
        stats.repos_scanned = -1;
        return stats;
    }

    /* Delete old cross-repo DB and create fresh */
    remove(db_path);

    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX,
                        NULL) != SQLITE_OK) {
        if (db) sqlite3_close(db);
        stats.repos_scanned = -1;
        return stats;
    }

    /* Register custom functions for FTS5 */
    sqlite3_create_function(db, "cbm_camel_split", 1, SQLITE_UTF8 | SQLITE_DETERMINISTIC,
                            NULL, sqlite_camel_split_cr, NULL, NULL);

    /* Pragmas for fast bulk write */
    sqlite3_exec(db, "PRAGMA journal_mode=WAL; PRAGMA synchronous=OFF; "
                     "PRAGMA cache_size=-32000;", NULL, NULL, NULL);

    /* Create schema */
    char *err = NULL;
    sqlite3_exec(db, CROSS_SCHEMA, NULL, NULL, &err);
    if (err) { sqlite3_free(err); err = NULL; }
    sqlite3_exec(db, CROSS_FTS, NULL, NULL, &err);
    if (err) { sqlite3_free(err); err = NULL; }

    sqlite3_exec(db, "BEGIN TRANSACTION", NULL, NULL, NULL);

    /* Scan all project DBs in cache directory */
    cbm_dir_t *dir = cbm_opendir(cache_dir);
    if (!dir) {
        sqlite3_close(db);
        stats.repos_scanned = -1;
        return stats;
    }

    /* Prepared statements for inserting into cross-repo DB */
    sqlite3_stmt *ins_ch = NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO cross_channels(channel_name, transport, direction, project, "
        "file_path, function_name, node_id) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        -1, &ins_ch, NULL);

    sqlite3_stmt *ins_node = NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO cross_nodes(project, orig_id, label, name, qualified_name, file_path) "
        "VALUES(?1,?2,?3,?4,?5,?6)",
        -1, &ins_node, NULL);

    sqlite3_stmt *ins_emb = NULL;
    sqlite3_prepare_v2(db,
        "INSERT INTO cross_embeddings(node_id, project, embedding, dimensions) "
        "VALUES(?1,?2,?3,?4)",
        -1, &ins_emb, NULL);

    cbm_dirent_t *dent;
    while ((dent = cbm_readdir(dir)) != NULL) {
        const char *entry = dent->name;
        /* Skip non-.db files, _cross_repo.db, _config.db */
        size_t elen = strlen(entry);
        if (elen < 4 || strcmp(entry + elen - 3, ".db") != 0) continue;
        if (strstr(entry, "_cross_repo") || strstr(entry, "_config")) continue;
        if (strstr(entry, "-wal") || strstr(entry, "-shm")) continue;

        char proj_db_path[2048];
        snprintf(proj_db_path, sizeof(proj_db_path), "%s/%s", cache_dir, entry);

        /* Derive project name from filename (remove .db suffix) */
        char project_name[512];
        snprintf(project_name, sizeof(project_name), "%.*s", (int)(elen - 3), entry);

        /* Open project DB read-only */
        sqlite3 *pdb = NULL;
        if (sqlite3_open_v2(proj_db_path, &pdb,
                            SQLITE_OPEN_READONLY | SQLITE_OPEN_NOMUTEX, NULL) != SQLITE_OK) {
            if (pdb) sqlite3_close(pdb);
            continue;
        }

        stats.repos_scanned++;

        /* Copy channels */
        {
            sqlite3_stmt *sel = NULL;
            if (sqlite3_prepare_v2(pdb,
                    "SELECT channel_name, transport, direction, project, file_path, "
                    "function_name, node_id FROM channels", -1, &sel, NULL) == SQLITE_OK) {
                while (sqlite3_step(sel) == SQLITE_ROW) {
                    sqlite3_reset(ins_ch);
                    for (int c = 0; c < 7; c++) {
                        if (sqlite3_column_type(sel, c) == SQLITE_INTEGER)
                            sqlite3_bind_int64(ins_ch, c + 1, sqlite3_column_int64(sel, c));
                        else
                            sqlite3_bind_text(ins_ch, c + 1,
                                (const char *)sqlite3_column_text(sel, c), -1, SQLITE_TRANSIENT);
                    }
                    sqlite3_step(ins_ch);
                    stats.channels_copied++;
                }
                sqlite3_finalize(sel);
            }
        }

        /* Copy embeddable nodes */
        {
            sqlite3_stmt *sel = NULL;
            if (sqlite3_prepare_v2(pdb,
                    "SELECT id, label, name, qualified_name, file_path FROM nodes "
                    "WHERE label IN ('Function','Method','Class','Interface','Route')",
                    -1, &sel, NULL) == SQLITE_OK) {
                while (sqlite3_step(sel) == SQLITE_ROW) {
                    sqlite3_reset(ins_node);
                    sqlite3_bind_text(ins_node, 1, project_name, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int64(ins_node, 2, sqlite3_column_int64(sel, 0)); /* orig_id */
                    sqlite3_bind_text(ins_node, 3,
                        (const char *)sqlite3_column_text(sel, 1), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(ins_node, 4,
                        (const char *)sqlite3_column_text(sel, 2), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(ins_node, 5,
                        (const char *)sqlite3_column_text(sel, 3), -1, SQLITE_TRANSIENT);
                    sqlite3_bind_text(ins_node, 6,
                        (const char *)sqlite3_column_text(sel, 4), -1, SQLITE_TRANSIENT);
                    sqlite3_step(ins_node);
                    stats.nodes_copied++;
                }
                sqlite3_finalize(sel);
            }
        }

        /* Copy embeddings — join with cross_nodes via a single efficient query.
         * First ensure we have the index on (project, orig_id) for the join. */
        {
            sqlite3_stmt *sel = NULL;
            /* Use the per-project DB to read embeddings, then look up cross_nodes.id
             * via a prepared statement (reuse for all rows in this project). */
            sqlite3_stmt *lu_emb = NULL;
            sqlite3_prepare_v2(db,
                "SELECT id FROM cross_nodes WHERE project=?1 AND orig_id=?2",
                -1, &lu_emb, NULL);

            if (lu_emb && sqlite3_prepare_v2(pdb,
                    "SELECT node_id, embedding, dimensions FROM embeddings",
                    -1, &sel, NULL) == SQLITE_OK) {
                while (sqlite3_step(sel) == SQLITE_ROW) {
                    int64_t orig_id = sqlite3_column_int64(sel, 0);
                    sqlite3_reset(lu_emb);
                    sqlite3_bind_text(lu_emb, 1, project_name, -1, SQLITE_TRANSIENT);
                    sqlite3_bind_int64(lu_emb, 2, orig_id);
                    if (sqlite3_step(lu_emb) == SQLITE_ROW) {
                        int64_t cross_id = sqlite3_column_int64(lu_emb, 0);
                        sqlite3_reset(ins_emb);
                        sqlite3_bind_int64(ins_emb, 1, cross_id);
                        sqlite3_bind_text(ins_emb, 2, project_name, -1, SQLITE_TRANSIENT);
                        sqlite3_bind_blob(ins_emb, 3,
                            sqlite3_column_blob(sel, 1),
                            sqlite3_column_bytes(sel, 1), SQLITE_TRANSIENT);
                        sqlite3_bind_int(ins_emb, 4, sqlite3_column_int(sel, 2));
                        sqlite3_step(ins_emb);
                        stats.embeddings_copied++;
                    }
                }
                sqlite3_finalize(sel);
            }
            if (lu_emb) sqlite3_finalize(lu_emb);
        }

        sqlite3_close(pdb);
    }
    cbm_closedir(dir);

    if (ins_ch) sqlite3_finalize(ins_ch);
    if (ins_node) sqlite3_finalize(ins_node);
    if (ins_emb) sqlite3_finalize(ins_emb);

    /* Suppress file-level ghost channel entries when named entries exist */
    sqlite3_exec(db,
        "DELETE FROM cross_channels WHERE function_name = '(file-level)' "
        "AND EXISTS (SELECT 1 FROM cross_channels c2 "
        "WHERE c2.channel_name = cross_channels.channel_name "
        "AND c2.file_path = cross_channels.file_path "
        "AND c2.project = cross_channels.project "
        "AND c2.direction = cross_channels.direction "
        "AND c2.function_name != '(file-level)')", NULL, NULL, NULL);

    /* Build FTS5 index with camelCase splitting */
    sqlite3_exec(db, "DELETE FROM cross_nodes_fts", NULL, NULL, NULL);
    sqlite3_exec(db,
        "INSERT INTO cross_nodes_fts(rowid, name, qualified_name, label, file_path, project) "
        "SELECT id, cbm_camel_split(name), qualified_name, label, file_path, project "
        "FROM cross_nodes",
        NULL, NULL, NULL);

    /* Count cross-repo channel matches */
    {
        sqlite3_stmt *cnt = NULL;
        if (sqlite3_prepare_v2(db,
                "SELECT COUNT(DISTINCT e.channel_name) FROM cross_channels e "
                "JOIN cross_channels l ON e.channel_name = l.channel_name "
                "WHERE e.direction = 'emit' AND l.direction = 'listen' "
                "AND e.project != l.project",
                -1, &cnt, NULL) == SQLITE_OK) {
            if (sqlite3_step(cnt) == SQLITE_ROW) {
                stats.cross_repo_matches = sqlite3_column_int(cnt, 0);
            }
            sqlite3_finalize(cnt);
        }
    }

    /* Store metadata */
    {
        time_t now = time(NULL);
        char ts[64];
        strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%SZ", gmtime(&now));
        sqlite3_stmt *meta = NULL;
        sqlite3_prepare_v2(db,
            "INSERT OR REPLACE INTO cross_meta(key, value) VALUES(?1, ?2)",
            -1, &meta, NULL);
        if (meta) {
            sqlite3_bind_text(meta, 1, "built_at", -1, SQLITE_STATIC);
            sqlite3_bind_text(meta, 2, ts, -1, SQLITE_TRANSIENT);
            sqlite3_step(meta);
            sqlite3_reset(meta);
            char buf[32];
            snprintf(buf, sizeof(buf), "%d", stats.repos_scanned);
            sqlite3_bind_text(meta, 1, "repos", -1, SQLITE_STATIC);
            sqlite3_bind_text(meta, 2, buf, -1, SQLITE_TRANSIENT);
            sqlite3_step(meta);
            sqlite3_finalize(meta);
        }
    }

    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
    sqlite3_exec(db, "PRAGMA synchronous=NORMAL", NULL, NULL, NULL);
    sqlite3_close(db);

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    stats.build_time_ms = (double)(t1.tv_sec - t0.tv_sec) * 1000.0 +
                          (double)(t1.tv_nsec - t0.tv_nsec) / 1000000.0;

    cbm_log_info("cross_repo.build", "repos", itoa_cr(stats.repos_scanned),
                 "nodes", itoa_cr(stats.nodes_copied),
                 "channels", itoa_cr(stats.channels_copied),
                 "embeddings", itoa_cr(stats.embeddings_copied),
                 "cross_matches", itoa_cr(stats.cross_repo_matches));

    return stats;
}

/* ── Cross-Repo Search ──────────────────────────────────────────── */

static char *heap_dup(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    char *d = malloc(len + 1);
    if (d) { memcpy(d, s, len + 1); }
    return d;
}

int cbm_cross_repo_search(cbm_cross_repo_t *cr, const char *query,
                          const float *query_vec, int dims,
                          int limit, cbm_cross_search_output_t *out) {
    if (!cr || !cr->db || !query || !out) return CBM_STORE_ERR;
    memset(out, 0, sizeof(*out));
    if (limit <= 0) limit = 50;

    /* Tokenize query for FTS5: split on whitespace, join with OR */
    char fts_query[1024];
    {
        char tmp[1024];
        snprintf(tmp, sizeof(tmp), "%s", query);
        int fq_len = 0;
        char *tok = strtok(tmp, " \t\n");
        while (tok && fq_len < (int)sizeof(fts_query) - 20) {
            if (fq_len > 0) fq_len += snprintf(fts_query + fq_len,
                sizeof(fts_query) - (size_t)fq_len, " OR ");
            fq_len += snprintf(fts_query + fq_len,
                sizeof(fts_query) - (size_t)fq_len, "%s", tok);
            tok = strtok(NULL, " \t\n");
        }
        fts_query[fq_len] = '\0';
    }

    /* BM25 search */
    int bm25_cap = limit * 2;
    int64_t *bm25_ids = calloc((size_t)bm25_cap, sizeof(int64_t));
    int bm25_count = 0;

    {
        sqlite3_stmt *stmt = NULL;
        const char *sql =
            "SELECT cn.id, cn.project, cn.orig_id, cn.label, cn.name, "
            "cn.qualified_name, cn.file_path, "
            "(bm25(cross_nodes_fts) "
            " - CASE WHEN cn.label IN ('Function','Method') THEN 10.0 "
            "        WHEN cn.label IN ('Class','Interface') THEN 5.0 "
            "        WHEN cn.label = 'Route' THEN 8.0 "
            "        ELSE 0.0 END) AS rank "
            "FROM cross_nodes_fts f "
            "JOIN cross_nodes cn ON cn.id = f.rowid "
            "WHERE cross_nodes_fts MATCH ?1 "
            "ORDER BY rank LIMIT ?2";
        if (sqlite3_prepare_v2(cr->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
            sqlite3_bind_text(stmt, 1, fts_query, -1, SQLITE_TRANSIENT);
            sqlite3_bind_int(stmt, 2, bm25_cap);
            while (sqlite3_step(stmt) == SQLITE_ROW && bm25_count < bm25_cap) {
                bm25_ids[bm25_count++] = sqlite3_column_int64(stmt, 0);
            }
            sqlite3_finalize(stmt);
        }
    }

    /* Vector search (if query_vec provided and embeddings exist) */
    int vec_cap = limit;
    int64_t *vec_ids = NULL;
    double *vec_sims = NULL;
    int vec_count = 0;

    if (query_vec && dims > 0) {
        int emb_count = 0;
        {
            sqlite3_stmt *cnt = NULL;
            sqlite3_prepare_v2(cr->db, "SELECT COUNT(*) FROM cross_embeddings", -1, &cnt, NULL);
            if (cnt && sqlite3_step(cnt) == SQLITE_ROW) emb_count = sqlite3_column_int(cnt, 0);
            if (cnt) sqlite3_finalize(cnt);
        }
        if (emb_count > 0) {
            vec_ids = calloc((size_t)vec_cap, sizeof(int64_t));
            vec_sims = calloc((size_t)vec_cap, sizeof(double));

            sqlite3_stmt *stmt = NULL;
            const char *sql =
                "SELECT ce.node_id, cbm_cosine_sim(?1, ce.embedding) AS sim "
                "FROM cross_embeddings ce "
                "WHERE sim > 0.3 "
                "ORDER BY sim DESC LIMIT ?2";
            if (sqlite3_prepare_v2(cr->db, sql, -1, &stmt, NULL) == SQLITE_OK) {
                sqlite3_bind_blob(stmt, 1, query_vec, dims * (int)sizeof(float), SQLITE_STATIC);
                sqlite3_bind_int(stmt, 2, vec_cap);
                while (sqlite3_step(stmt) == SQLITE_ROW && vec_count < vec_cap) {
                    vec_ids[vec_count] = sqlite3_column_int64(stmt, 0);
                    vec_sims[vec_count] = sqlite3_column_double(stmt, 1);
                    vec_count++;
                }
                sqlite3_finalize(stmt);
            }
            out->used_vector = (vec_count > 0);
        }
    }

    /* RRF merge (k=60) */
    int merge_cap = bm25_count + vec_count;
    if (merge_cap == 0) {
        free(bm25_ids); free(vec_ids); free(vec_sims);
        return CBM_STORE_OK;
    }

    typedef struct { int64_t id; double score; double sim; } rrf_entry_t;
    rrf_entry_t *merged = calloc((size_t)merge_cap, sizeof(rrf_entry_t));
    int merge_count = 0;

    for (int i = 0; i < bm25_count; i++) {
        merged[merge_count].id = bm25_ids[i];
        merged[merge_count].score = 1.0 / (60 + i);
        merged[merge_count].sim = 0;
        merge_count++;
    }
    for (int i = 0; i < vec_count; i++) {
        bool found = false;
        for (int j = 0; j < merge_count; j++) {
            if (merged[j].id == vec_ids[i]) {
                merged[j].score += 1.0 / (60 + i);
                merged[j].sim = vec_sims[i];
                found = true;
                break;
            }
        }
        if (!found && merge_count < merge_cap) {
            merged[merge_count].id = vec_ids[i];
            merged[merge_count].score = 1.0 / (60 + i);
            merged[merge_count].sim = vec_sims[i];
            merge_count++;
        }
    }

    /* Sort by RRF score descending */
    for (int i = 0; i < merge_count - 1; i++) {
        for (int j = i + 1; j < merge_count; j++) {
            if (merged[j].score > merged[i].score) {
                rrf_entry_t tmp = merged[i]; merged[i] = merged[j]; merged[j] = tmp;
            }
        }
    }

    /* Build output — look up node details from cross_nodes */
    int result_count = merge_count < limit ? merge_count : limit;
    out->results = calloc((size_t)result_count, sizeof(cbm_cross_search_result_t));
    out->total = merge_count;

    sqlite3_stmt *lu = NULL;
    sqlite3_prepare_v2(cr->db,
        "SELECT project, orig_id, label, name, qualified_name, file_path "
        "FROM cross_nodes WHERE id = ?1", -1, &lu, NULL);

    for (int i = 0; i < result_count && lu; i++) {
        sqlite3_reset(lu);
        sqlite3_bind_int64(lu, 1, merged[i].id);
        if (sqlite3_step(lu) == SQLITE_ROW) {
            cbm_cross_search_result_t *r = &out->results[out->count];
            r->project = heap_dup((const char *)sqlite3_column_text(lu, 0));
            r->orig_id = sqlite3_column_int64(lu, 1);
            r->label = heap_dup((const char *)sqlite3_column_text(lu, 2));
            r->name = heap_dup((const char *)sqlite3_column_text(lu, 3));
            r->qualified_name = heap_dup((const char *)sqlite3_column_text(lu, 4));
            r->file_path = heap_dup((const char *)sqlite3_column_text(lu, 5));
            r->score = merged[i].score;
            r->similarity = merged[i].sim;
            out->count++;
        }
    }
    if (lu) sqlite3_finalize(lu);

    free(bm25_ids); free(vec_ids); free(vec_sims); free(merged);
    return CBM_STORE_OK;
}

void cbm_cross_search_free(cbm_cross_search_output_t *out) {
    if (!out || !out->results) return;
    for (int i = 0; i < out->count; i++) {
        free((void *)out->results[i].project);
        free((void *)out->results[i].label);
        free((void *)out->results[i].name);
        free((void *)out->results[i].qualified_name);
        free((void *)out->results[i].file_path);
    }
    free(out->results);
    memset(out, 0, sizeof(*out));
}

/* ── Cross-Repo Channel Matching ────────────────────────────────── */

int cbm_cross_repo_match_channels(cbm_cross_repo_t *cr, const char *channel_filter,
                                  cbm_cross_channel_match_t **out, int *count) {
    if (!cr || !cr->db || !out || !count) return CBM_STORE_ERR;
    *out = NULL;
    *count = 0;

    const char *sql =
        "SELECT e.channel_name, e.transport, "
        "e.project, e.file_path, e.function_name, "
        "l.project, l.file_path, l.function_name "
        "FROM cross_channels e "
        "JOIN cross_channels l ON e.channel_name = l.channel_name "
        "WHERE e.direction = 'emit' AND l.direction = 'listen' "
        "AND e.project != l.project "
        "%s "
        "ORDER BY e.channel_name LIMIT 200";

    char full_sql[2048];
    if (channel_filter && channel_filter[0]) {
        char filter_clause[256];
        snprintf(filter_clause, sizeof(filter_clause),
                 "AND e.channel_name LIKE '%%%s%%'", channel_filter);
        snprintf(full_sql, sizeof(full_sql), sql, filter_clause);
    } else {
        snprintf(full_sql, sizeof(full_sql), sql, "");
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cr->db, full_sql, -1, &stmt, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }

    int cap = 200;
    cbm_cross_channel_match_t *matches = calloc((size_t)cap, sizeof(cbm_cross_channel_match_t));
    int n = 0;

    while (sqlite3_step(stmt) == SQLITE_ROW && n < cap) {
        cbm_cross_channel_match_t *m = &matches[n];
        m->channel_name = heap_dup((const char *)sqlite3_column_text(stmt, 0));
        m->transport = heap_dup((const char *)sqlite3_column_text(stmt, 1));
        m->emit_project = heap_dup((const char *)sqlite3_column_text(stmt, 2));
        m->emit_file = heap_dup((const char *)sqlite3_column_text(stmt, 3));
        m->emit_function = heap_dup((const char *)sqlite3_column_text(stmt, 4));
        m->listen_project = heap_dup((const char *)sqlite3_column_text(stmt, 5));
        m->listen_file = heap_dup((const char *)sqlite3_column_text(stmt, 6));
        m->listen_function = heap_dup((const char *)sqlite3_column_text(stmt, 7));
        n++;
    }
    sqlite3_finalize(stmt);

    *out = matches;
    *count = n;
    return CBM_STORE_OK;
}

void cbm_cross_channel_free(cbm_cross_channel_match_t *matches, int count) {
    if (!matches) return;
    for (int i = 0; i < count; i++) {
        free((void *)matches[i].channel_name);
        free((void *)matches[i].transport);
        free((void *)matches[i].emit_project);
        free((void *)matches[i].emit_file);
        free((void *)matches[i].emit_function);
        free((void *)matches[i].listen_project);
        free((void *)matches[i].listen_file);
        free((void *)matches[i].listen_function);
    }
    free(matches);
}

/* ── Cross-Repo Stats ───────────────────────────────────────────── */

int cbm_cross_repo_get_info(cbm_cross_repo_t *cr, cbm_cross_repo_info_t *out) {
    if (!cr || !cr->db || !out) return CBM_STORE_ERR;
    memset(out, 0, sizeof(*out));

    sqlite3_stmt *s = NULL;
    sqlite3_prepare_v2(cr->db, "SELECT COUNT(DISTINCT project) FROM cross_nodes", -1, &s, NULL);
    if (s && sqlite3_step(s) == SQLITE_ROW) out->total_repos = sqlite3_column_int(s, 0);
    if (s) sqlite3_finalize(s);

    sqlite3_prepare_v2(cr->db, "SELECT COUNT(*) FROM cross_nodes", -1, &s, NULL);
    if (s && sqlite3_step(s) == SQLITE_ROW) out->total_nodes = sqlite3_column_int(s, 0);
    if (s) sqlite3_finalize(s);

    sqlite3_prepare_v2(cr->db, "SELECT COUNT(*) FROM cross_channels", -1, &s, NULL);
    if (s && sqlite3_step(s) == SQLITE_ROW) out->total_channels = sqlite3_column_int(s, 0);
    if (s) sqlite3_finalize(s);

    sqlite3_prepare_v2(cr->db, "SELECT COUNT(*) FROM cross_embeddings", -1, &s, NULL);
    if (s && sqlite3_step(s) == SQLITE_ROW) out->total_embeddings = sqlite3_column_int(s, 0);
    if (s) sqlite3_finalize(s);

    /* Cross-repo channel count */
    sqlite3_prepare_v2(cr->db,
        "SELECT COUNT(DISTINCT e.channel_name) FROM cross_channels e "
        "JOIN cross_channels l ON e.channel_name = l.channel_name "
        "WHERE e.direction = 'emit' AND l.direction = 'listen' "
        "AND e.project != l.project", -1, &s, NULL);
    if (s && sqlite3_step(s) == SQLITE_ROW) out->cross_repo_channel_count = sqlite3_column_int(s, 0);
    if (s) sqlite3_finalize(s);

    sqlite3_prepare_v2(cr->db,
        "SELECT value FROM cross_meta WHERE key = 'built_at'", -1, &s, NULL);
    if (s && sqlite3_step(s) == SQLITE_ROW)
        out->built_at = heap_dup((const char *)sqlite3_column_text(s, 0));
    if (s) sqlite3_finalize(s);

    return CBM_STORE_OK;
}

void cbm_cross_repo_info_free(cbm_cross_repo_info_t *info) {
    if (!info) return;
    free((void *)info->built_at);
    info->built_at = NULL;
}

/* ── Cross-Repo Trace Helper ────────────────────────────────────── */

int cbm_cross_repo_trace_in_project(
    const char *project_db_path,
    const char *function_name,
    const char *file_path_hint,
    const char *channel_name,
    const char *direction,
    int max_depth,
    cbm_cross_trace_step_t **out, int *out_count) {

    if (!project_db_path || !function_name || !direction || !out || !out_count) {
        return CBM_STORE_ERR;
    }
    *out = NULL;
    *out_count = 0;
    if (max_depth <= 0) max_depth = 2;

    /* Open project DB read-only */
    cbm_store_t *store = cbm_store_open_path_query(project_db_path);
    if (!store) return CBM_STORE_ERR;

    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db) { cbm_store_close(store); return CBM_STORE_ERR; }

    int64_t start_id = 0;

    /* Resolve start node — handle special cases */
    if (strcmp(function_name, "(file-level)") == 0 && file_path_hint) {
        /* File-level listener: find the actual handler function via channels table */
        if (channel_name) {
            sqlite3_stmt *s = NULL;
            sqlite3_prepare_v2(db,
                "SELECT DISTINCT c.node_id FROM channels c "
                "WHERE c.file_path = ?1 AND c.channel_name = ?2 AND c.node_id > 0 "
                "LIMIT 1", -1, &s, NULL);
            if (s) {
                sqlite3_bind_text(s, 1, file_path_hint, -1, SQLITE_STATIC);
                sqlite3_bind_text(s, 2, channel_name, -1, SQLITE_STATIC);
                if (sqlite3_step(s) == SQLITE_ROW) {
                    start_id = sqlite3_column_int64(s, 0);
                }
                sqlite3_finalize(s);
            }
        }
        /* Fallback: first Function/Method in the file */
        if (start_id == 0) {
            sqlite3_stmt *s = NULL;
            sqlite3_prepare_v2(db,
                "SELECT id FROM nodes WHERE file_path = ?1 "
                "AND label IN ('Function','Method') ORDER BY start_line LIMIT 1",
                -1, &s, NULL);
            if (s) {
                sqlite3_bind_text(s, 1, file_path_hint, -1, SQLITE_STATIC);
                if (sqlite3_step(s) == SQLITE_ROW) {
                    start_id = sqlite3_column_int64(s, 0);
                }
                sqlite3_finalize(s);
            }
        }
    } else {
        /* Normal case: find by name, optionally filtered by file_path */
        const char *sql = file_path_hint
            ? "SELECT id, label FROM nodes WHERE name = ?1 AND file_path = ?2 "
              "AND label IN ('Function','Method','Class') LIMIT 1"
            : "SELECT id, label FROM nodes WHERE name = ?1 "
              "AND label IN ('Function','Method','Class') LIMIT 1";
        sqlite3_stmt *s = NULL;
        sqlite3_prepare_v2(db, sql, -1, &s, NULL);
        if (s) {
            sqlite3_bind_text(s, 1, function_name, -1, SQLITE_STATIC);
            if (file_path_hint)
                sqlite3_bind_text(s, 2, file_path_hint, -1, SQLITE_STATIC);
            if (sqlite3_step(s) == SQLITE_ROW) {
                start_id = sqlite3_column_int64(s, 0);
                const char *label = (const char *)sqlite3_column_text(s, 1);
                /* If it's a Class, resolve through DEFINES_METHOD → use first method */
                if (label && strcmp(label, "Class") == 0) {
                    int64_t class_id = start_id;
                    sqlite3_stmt *m = NULL;
                    sqlite3_prepare_v2(db,
                        "SELECT target_id FROM edges WHERE source_id = ?1 "
                        "AND type = 'DEFINES_METHOD' LIMIT 1", -1, &m, NULL);
                    if (m) {
                        sqlite3_bind_int64(m, 1, class_id);
                        if (sqlite3_step(m) == SQLITE_ROW) {
                            start_id = sqlite3_column_int64(m, 0);
                        }
                        sqlite3_finalize(m);
                    }
                }
            }
            sqlite3_finalize(s);
        }
    }

    if (start_id == 0) {
        cbm_store_close(store);
        return CBM_STORE_OK; /* no results, not an error */
    }

    /* Run BFS */
    const char *edge_types[] = {"CALLS"};
    cbm_traverse_result_t trav = {0};
    cbm_store_bfs(store, start_id, direction, edge_types, 1,
                  max_depth, 20, &trav);

    /* Convert to output format */
    int cap = trav.visited_count;
    if (cap > 0) {
        cbm_cross_trace_step_t *steps = calloc((size_t)cap, sizeof(cbm_cross_trace_step_t));
        int count = 0;
        for (int i = 0; i < trav.visited_count && count < cap; i++) {
            cbm_node_hop_t *h = &trav.visited[i];
            if (h->node.id == start_id) continue; /* skip the start node itself */
            steps[count].name = heap_dup(h->node.name);
            steps[count].label = heap_dup(h->node.label);
            steps[count].file_path = heap_dup(h->node.file_path);
            steps[count].depth = h->hop;
            count++;
        }
        *out = steps;
        *out_count = count;
    }

    cbm_store_traverse_free(&trav);
    cbm_store_close(store);
    return CBM_STORE_OK;
}

void cbm_cross_trace_free(cbm_cross_trace_step_t *steps, int count) {
    if (!steps) return;
    for (int i = 0; i < count; i++) {
        free((void *)steps[i].name);
        free((void *)steps[i].label);
        free((void *)steps[i].file_path);
    }
    free(steps);
}
