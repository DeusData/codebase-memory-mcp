/*
 * store_content_digest.c — the digest of a project's graph CONTENT.
 *
 * cbm_store_graph_digest binds one database exactly: row ids, the random
 * db_uid, generation counters and file mtimes included. That is right for
 * proving an imported copy is the published one, and useless for asking
 * whether two independently built graphs are the same graph. This digest
 * answers the second question: every row is keyed by what it says, never by
 * where it was stored. Nodes by their own columns; edges by the content keys
 * of their endpoints; file hashes by path, content hash and size; LSP
 * surfaces, coverage rows and summaries without their timestamps. An
 * incremental index equals a full one exactly when their content digests are
 * equal.
 */
#include "store/store.h"

#include "foundation/sha256.h"
#include "sqlite3.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

typedef struct {
    const char *name;
    const char *sql; /* ?1 = project; every column is hashed, rows in this order */
} sc_query_t;

static const sc_query_t sc_queries[] = {
    {"nodes", "SELECT label,name,qualified_name,file_path,start_line,end_line,properties "
              "FROM main.nodes WHERE project=?1 ORDER BY 3,1,4,5,6,2,7;"},
    {"edges", "SELECT s.label,s.qualified_name,s.file_path,s.start_line,e.type,t.label,"
              "t.qualified_name,t.file_path,t.start_line,e.properties,e.url_path_gen,"
              "e.local_name_gen FROM main.edges e JOIN main.nodes s ON s.id=e.source_id "
              "JOIN main.nodes t ON t.id=e.target_id WHERE e.project=?1 "
              "ORDER BY 2,1,3,4,5,7,6,8,9,10,11,12;"},
    {"dangling_edges", "SELECT COUNT(*) FROM main.edges e WHERE e.project=?1 AND (NOT EXISTS "
                       "(SELECT 1 FROM main.nodes n WHERE n.id=e.source_id) OR NOT EXISTS "
                       "(SELECT 1 FROM main.nodes n WHERE n.id=e.target_id));"},
    {"file_hashes", "SELECT rel_path,sha256,size FROM main.file_hashes WHERE project=?1 "
                    "ORDER BY 1,2,3;"},
    {"lsp_surface", "SELECT rel_path,surface_sha,defs_json,ref_bloom,config_ctx FROM "
                    "main.lsp_surface WHERE project=?1 ORDER BY 1,2,3,4,5;"},
    {"index_coverage", "SELECT rel_path,kind,detail FROM main.index_coverage WHERE project=?1 "
                       "ORDER BY 1,2,3;"},
    {"index_coverage_meta", "SELECT index_mode,recording_status,ignored_files_stored,"
                            "ignored_files_total,coverage_version,hash_records_complete FROM "
                            "main.index_coverage_meta WHERE project=?1;"},
    {"project_summaries", "SELECT summary,source_hash FROM main.project_summaries WHERE "
                          "project=?1;"},
};

/* What test selection reads: node identity and every edge by its endpoints
 * and type, but no edge properties, and not the two corpus-statistical edge
 * types (similarity over the whole corpus, which the closure-repair route
 * recomputes only around the repaired files). */
static const sc_query_t sc_topology_queries[] = {
    {"nodes", "SELECT label,name,qualified_name,file_path,start_line,end_line "
              "FROM main.nodes WHERE project=?1 ORDER BY 3,1,4,5,6,2;"},
    {"edges", "SELECT s.label,s.qualified_name,s.file_path,s.start_line,e.type,t.label,"
              "t.qualified_name,t.file_path,t.start_line,e.url_path_gen,e.local_name_gen "
              "FROM main.edges e JOIN main.nodes s ON s.id=e.source_id "
              "JOIN main.nodes t ON t.id=e.target_id WHERE e.project=?1 AND e.type NOT IN "
              "('SIMILAR_TO','SEMANTICALLY_RELATED') ORDER BY 2,1,3,4,5,7,6,8,9,10,11;"},
    {"file_hashes", "SELECT rel_path,sha256,size FROM main.file_hashes WHERE project=?1 "
                    "ORDER BY 1,2,3;"},
    {"index_coverage", "SELECT rel_path,kind,detail FROM main.index_coverage WHERE project=?1 "
                       "ORDER BY 1,2,3;"},
};

static void sc_frame(cbm_sha256_ctx *ctx, unsigned char tag, const void *bytes, uint64_t length) {
    unsigned char head[9];
    head[0] = tag;
    for (int i = 0; i < 8; i++) {
        head[1 + i] = (unsigned char)(length >> (56 - 8 * i));
    }
    cbm_sha256_update(ctx, head, sizeof(head));
    if (length) {
        cbm_sha256_update(ctx, bytes, (size_t)length);
    }
}

static bool sc_table(sqlite3 *db, const sc_query_t *q, const char *project, cbm_sha256_ctx *ctx) {
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, q->sql, -1, &st, NULL) != SQLITE_OK) {
        return false;
    }
    bool ok = sqlite3_bind_text(st, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    sc_frame(ctx, 'T', q->name, strlen(q->name));
    uint64_t rows = 0;
    int rc = SQLITE_ROW;
    while (ok && (rc = sqlite3_step(st)) == SQLITE_ROW) {
        int columns = sqlite3_column_count(st);
        unsigned char width[4] = {(unsigned char)(columns >> 24), (unsigned char)(columns >> 16),
                                  (unsigned char)(columns >> 8), (unsigned char)columns};
        sc_frame(ctx, 'R', width, sizeof(width));
        for (int c = 0; c < columns; c++) {
            switch (sqlite3_column_type(st, c)) {
            case SQLITE_NULL:
                sc_frame(ctx, 'N', NULL, 0);
                break;
            case SQLITE_INTEGER: {
                char text[32];
                long long value = (long long)sqlite3_column_int64(st, c);
                int n = snprintf(text, sizeof(text), "%lld", value);
                sc_frame(ctx, 'I', text, (uint64_t)n);
                break;
            }
            case SQLITE_FLOAT: {
                char text[64];
                double value = sqlite3_column_double(st, c);
                int n = snprintf(text, sizeof(text), "%.17g", value);
                sc_frame(ctx, 'F', text, (uint64_t)n);
                break;
            }
            case SQLITE_BLOB:
                sc_frame(ctx, 'B', sqlite3_column_blob(st, c),
                         (uint64_t)sqlite3_column_bytes(st, c));
                break;
            default:
                sc_frame(ctx, 'S', sqlite3_column_text(st, c),
                         (uint64_t)sqlite3_column_bytes(st, c));
                break;
            }
        }
        rows++;
    }
    ok = ok && rc == SQLITE_DONE;
    sqlite3_finalize(st);
    unsigned char count[8];
    for (int i = 0; i < 8; i++) {
        count[i] = (unsigned char)(rows >> (56 - 8 * i));
    }
    sc_frame(ctx, 'C', count, sizeof(count));
    return ok;
}

static int sc_digest(cbm_store_t *s, const char *project, const char *domain,
                     const sc_query_t *queries, size_t query_count,
                     unsigned char out[CBM_SHA256_DIGEST_LEN]) {
    sqlite3 *db = s && project ? cbm_store_get_db(s) : NULL;
    if (!db || !out) {
        return CBM_STORE_ERR;
    }
    cbm_sha256_ctx ctx;
    cbm_sha256_init(&ctx);
    sc_frame(&ctx, 'D', domain, strlen(domain));
    /* One read transaction, so every table is read from the same snapshot. */
    bool own = sqlite3_get_autocommit(db) != 0;
    if (own && sqlite3_exec(db, "BEGIN", NULL, NULL, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    bool ok = true;
    for (size_t i = 0; ok && i < query_count; i++) {
        ok = sc_table(db, &queries[i], project, &ctx);
    }
    if (own && sqlite3_exec(db, ok ? "COMMIT" : "ROLLBACK", NULL, NULL, NULL) != SQLITE_OK) {
        ok = false;
    }
    if (!ok) {
        return CBM_STORE_ERR;
    }
    cbm_sha256_final(&ctx, out);
    return CBM_STORE_OK;
}

int cbm_store_graph_content_digest(cbm_store_t *s, const char *project,
                                   unsigned char out[CBM_SHA256_DIGEST_LEN]) {
    return sc_digest(s, project, "cbm.graph.content.v1", sc_queries,
                     sizeof(sc_queries) / sizeof(sc_queries[0]), out);
}

int cbm_store_graph_topology_digest(cbm_store_t *s, const char *project,
                                    unsigned char out[CBM_SHA256_DIGEST_LEN]) {
    return sc_digest(s, project, "cbm.graph.topology.v1", sc_topology_queries,
                     sizeof(sc_topology_queries) / sizeof(sc_topology_queries[0]), out);
}
