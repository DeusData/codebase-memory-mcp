/*
 * pass_crossrepolinks.c — Cross-project protocol endpoint matching.
 *
 * Two entry points:
 *   1. cbm_persist_endpoints() — write discovered endpoints to a project's .db
 *   2. cbm_cross_project_link() — scan all project DBs, match producers to
 *      consumers across project boundaries, write to _crosslinks.db
 */
#include "servicelink.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include <store/store.h>
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <dirent.h>
#include <time.h>

/* Thread-local int-to-string helper (same pattern as pipeline.c itoa_buf). */
static const char *itoa_buf(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Endpoint Persistence ─────────────────────────────────────────── */

int cbm_persist_endpoints(const char *db_path, const char *project,
                          const cbm_sl_endpoint_list_t *endpoints) {
    if (!db_path || !project || !endpoints || endpoints->count == 0) return 0;

    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_log_warn("persist_endpoints.open_failed", "path", db_path);
        return -1;
    }

    /* Ensure table exists (for DBs created before this feature) */
    cbm_store_exec(store,
        "CREATE TABLE IF NOT EXISTS protocol_endpoints ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  project TEXT NOT NULL,"
        "  protocol TEXT NOT NULL,"
        "  role TEXT NOT NULL,"
        "  identifier TEXT NOT NULL,"
        "  node_qn TEXT NOT NULL,"
        "  file_path TEXT NOT NULL,"
        "  extra TEXT DEFAULT '{}',"
        "  UNIQUE(project, protocol, role, identifier, node_qn)"
        ");");

    /* Clear stale endpoints for this project */
    {
        sqlite3_stmt *del = NULL;
        sqlite3_prepare_v2(cbm_store_get_db(store),
            "DELETE FROM protocol_endpoints WHERE project = ?;", -1, &del, NULL);
        if (del) {
            sqlite3_bind_text(del, 1, project, -1, SQLITE_STATIC);
            sqlite3_step(del);
            sqlite3_finalize(del);
        }
    }

    /* Insert all endpoints using prepared statement */
    cbm_store_exec(store, "BEGIN TRANSACTION;");
    sqlite3_stmt *ins = NULL;
    sqlite3_prepare_v2(cbm_store_get_db(store),
        "INSERT OR IGNORE INTO protocol_endpoints "
        "(project, protocol, role, identifier, node_qn, file_path, extra) "
        "VALUES (?,?,?,?,?,?,?);", -1, &ins, NULL);

    if (ins) {
        for (int i = 0; i < endpoints->count; i++) {
            const cbm_sl_endpoint_t *ep = &endpoints->items[i];
            sqlite3_bind_text(ins, 1, ep->project, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 2, ep->protocol, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 3, ep->role, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4, ep->identifier, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 5, ep->node_qn, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 6, ep->file_path, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 7, ep->extra, -1, SQLITE_STATIC);
            sqlite3_step(ins);
            sqlite3_reset(ins);
        }
        sqlite3_finalize(ins);
    }
    cbm_store_exec(store, "COMMIT;");

    cbm_log_info("persist_endpoints.done", "count", itoa_buf(endpoints->count));
    cbm_store_close(store);
    return endpoints->count;
}

/* ── Cross-Project Matching ──────────────────────────────────────── */

/* Collected endpoint from any project DB */
typedef struct {
    char project[256];
    char protocol[32];
    char role[16];
    char identifier[256];
    char node_qn[512];
    char file_path[256];
    char identifier_norm[256];  /* lowercased, separators stripped */
} xl_endpoint_t;

/* Normalize identifier for matching: lowercase, strip -, _, . */
static void normalize_identifier(const char *src, char *dst, int dst_sz) {
    int j = 0;
    for (int i = 0; src[i] && j < dst_sz - 1; i++) {
        char c = src[i];
        if (c == '-' || c == '_' || c == '.') continue;
        dst[j++] = (c >= 'A' && c <= 'Z') ? (char)(c + 32) : c;
    }
    dst[j] = '\0';
}

/* Load endpoints from a single project DB */
static int load_endpoints_from_db(const char *db_path,
                                  xl_endpoint_t **out, int *out_count,
                                  int *out_cap) {
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(db_path, &db, SQLITE_OPEN_READONLY, NULL) != SQLITE_OK) {
        return -1;
    }

    /* Check if table exists */
    sqlite3_stmt *check = NULL;
    if (sqlite3_prepare_v2(db,
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='protocol_endpoints';",
            -1, &check, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return -1;
    }
    int has_table = (sqlite3_step(check) == SQLITE_ROW);
    sqlite3_finalize(check);
    if (!has_table) {
        sqlite3_close(db);
        return 0;  /* no table — old DB, skip silently */
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db,
            "SELECT project, protocol, role, identifier, node_qn, file_path "
            "FROM protocol_endpoints;", -1, &stmt, NULL) != SQLITE_OK) {
        sqlite3_close(db);
        return -1;
    }

    int added = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (*out_count >= *out_cap) {
            int new_cap = (*out_cap == 0) ? 1024 : *out_cap * 2;
            xl_endpoint_t *new_buf = realloc(*out, (size_t)new_cap * sizeof(xl_endpoint_t));
            if (!new_buf) break;
            *out = new_buf;
            *out_cap = new_cap;
        }
        xl_endpoint_t *ep = &(*out)[*out_count];
        memset(ep, 0, sizeof(*ep));
        const char *col;
        col = (const char *)sqlite3_column_text(stmt, 0);
        if (col) snprintf(ep->project, sizeof(ep->project), "%s", col);
        col = (const char *)sqlite3_column_text(stmt, 1);
        if (col) snprintf(ep->protocol, sizeof(ep->protocol), "%s", col);
        col = (const char *)sqlite3_column_text(stmt, 2);
        if (col) snprintf(ep->role, sizeof(ep->role), "%s", col);
        col = (const char *)sqlite3_column_text(stmt, 3);
        if (col) snprintf(ep->identifier, sizeof(ep->identifier), "%s", col);
        col = (const char *)sqlite3_column_text(stmt, 4);
        if (col) snprintf(ep->node_qn, sizeof(ep->node_qn), "%s", col);
        col = (const char *)sqlite3_column_text(stmt, 5);
        if (col) snprintf(ep->file_path, sizeof(ep->file_path), "%s", col);

        normalize_identifier(ep->identifier, ep->identifier_norm,
                             (int)sizeof(ep->identifier_norm));
        (*out_count)++;
        added++;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return added;
}

/* Write cross-links to _crosslinks.db */
static int write_crosslinks(const char *cache_dir,
                            const xl_endpoint_t *endpoints, int count) {
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/_crosslinks.db", cache_dir);

    sqlite3 *db = NULL;
    if (sqlite3_open(db_path, &db) != SQLITE_OK) {
        cbm_log_error("crosslink.open_failed", "path", db_path);
        return -1;
    }

    /* Create schema */
    sqlite3_exec(db,
        "CREATE TABLE IF NOT EXISTS cross_links ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  protocol TEXT NOT NULL,"
        "  identifier TEXT NOT NULL,"
        "  producer_project TEXT NOT NULL,"
        "  producer_qn TEXT NOT NULL,"
        "  producer_file TEXT NOT NULL,"
        "  consumer_project TEXT NOT NULL,"
        "  consumer_qn TEXT NOT NULL,"
        "  consumer_file TEXT NOT NULL,"
        "  confidence REAL NOT NULL,"
        "  updated_at TEXT NOT NULL,"
        "  UNIQUE(protocol, identifier, producer_qn, consumer_qn)"
        ");", NULL, NULL, NULL);

    /* Full rebuild */
    sqlite3_exec(db, "DELETE FROM cross_links;", NULL, NULL, NULL);

    /* Get current timestamp */
    char timestamp[64];
    time_t now = time(NULL);
    struct tm *tm = gmtime(&now);
    strftime(timestamp, sizeof(timestamp), "%Y-%m-%dT%H:%M:%SZ", tm);

    sqlite3_stmt *ins = NULL;
    sqlite3_prepare_v2(db,
        "INSERT OR IGNORE INTO cross_links "
        "(protocol, identifier, producer_project, producer_qn, producer_file, "
        " consumer_project, consumer_qn, consumer_file, confidence, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?);", -1, &ins, NULL);
    if (!ins) {
        cbm_log_warn("crosslink.prepare_failed", "path", db_path);
        sqlite3_close(db);
        return -1;
    }

    sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    int link_count = 0;

    /* O(n^2) matching — acceptable for expected sizes (few thousand endpoints) */
    for (int pi = 0; pi < count; pi++) {
        if (strcmp(endpoints[pi].role, "producer") != 0) continue;
        const xl_endpoint_t *prod = &endpoints[pi];

        for (int ci = 0; ci < count; ci++) {
            if (strcmp(endpoints[ci].role, "consumer") != 0) continue;
            const xl_endpoint_t *cons = &endpoints[ci];

            /* Skip same project */
            if (strcmp(prod->project, cons->project) == 0) continue;
            /* Must be same protocol */
            if (strcmp(prod->protocol, cons->protocol) != 0) continue;

            double confidence = 0.0;
            const char *match_ident = prod->identifier;

            /* Exact match */
            if (strcmp(prod->identifier, cons->identifier) == 0) {
                confidence = 0.95;
            }
            /* Normalized match */
            else if (strcmp(prod->identifier_norm, cons->identifier_norm) == 0 &&
                     prod->identifier_norm[0] != '\0') {
                confidence = 0.85;
            }

            if (confidence > 0.0 && ins) {
                sqlite3_bind_text(ins, 1, prod->protocol, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 2, match_ident, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 3, prod->project, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 4, prod->node_qn, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 5, prod->file_path, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 6, cons->project, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 7, cons->node_qn, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 8, cons->file_path, -1, SQLITE_STATIC);
                sqlite3_bind_double(ins, 9, confidence);
                sqlite3_bind_text(ins, 10, timestamp, -1, SQLITE_STATIC);
                sqlite3_step(ins);
                sqlite3_reset(ins);
                link_count++;
            }
        }
    }

    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    if (ins) sqlite3_finalize(ins);
    sqlite3_close(db);
    return link_count;
}

/* Main entry point: scan cache_dir for *.db, load endpoints, match across projects */
int cbm_cross_project_link(const char *cache_dir) {
    if (!cache_dir) return -1;

    cbm_log_info("crosslink.start", "cache_dir", cache_dir);

    DIR *dir = opendir(cache_dir);
    if (!dir) {
        cbm_log_warn("crosslink.opendir_failed", "dir", cache_dir);
        return -1;
    }

    /* Collect all endpoints from all project DBs */
    xl_endpoint_t *all_endpoints = NULL;
    int total = 0, cap = 0;

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        const char *name = ent->d_name;
        int len = (int)strlen(name);

        /* Skip non-.db files */
        if (len < 4 || strcmp(name + len - 3, ".db") != 0) continue;
        /* Skip _crosslinks.db, tmp-*, _* */
        if (name[0] == '_' || strncmp(name, "tmp-", 4) == 0) continue;

        char db_path[1024];
        snprintf(db_path, sizeof(db_path), "%s/%s", cache_dir, name);

        int loaded = load_endpoints_from_db(db_path, &all_endpoints, &total, &cap);
        if (loaded > 0) {
            cbm_log_info("crosslink.loaded", "db", name,
                         "endpoints", itoa_buf(loaded));
        }
    }
    closedir(dir);

    if (total == 0) {
        cbm_log_info("crosslink.done", "links", "0", "reason", "no_endpoints");
        free(all_endpoints);
        return 0;
    }

    /* Match across projects and write to _crosslinks.db */
    int links = write_crosslinks(cache_dir, all_endpoints, total);

    cbm_log_info("crosslink.done", "total_endpoints", itoa_buf(total),
                 "cross_links", itoa_buf(links));

    free(all_endpoints);
    return links;
}
