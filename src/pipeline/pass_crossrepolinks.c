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
#include <stdbool.h>
#include <stdint.h>
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
    char extra[256];            /* protocol-specific metadata (JSON) */
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

/* Extract a JSON string value by key (simple strstr-based, no full parse). */
static const char *xl_json_str(const char *json, const char *key,
                               char *buf, int bufsize) {
    if (!json || !key || bufsize <= 0) return NULL;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\":\"", key);
    const char *start = strstr(json, search);
    if (!start) return NULL;
    start += strlen(search);
    const char *end = strchr(start, '"');
    if (!end) return NULL;
    int len = (int)(end - start);
    if (len >= bufsize) len = bufsize - 1;
    memcpy(buf, start, (size_t)len);
    buf[len] = '\0';
    return buf;
}

/* Extract a JSON integer value by key. Returns true if found. */
static bool xl_json_int(const char *json, const char *key, long *out) {
    if (!json || !key || !out) return false;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\":", key);
    const char *start = strstr(json, search);
    if (!start) return false;
    start += strlen(search);
    while (*start == ' ') start++;
    /* Must be numeric (not a quoted string) */
    if (*start == '"') return false;
    char *endp = NULL;
    long v = strtol(start, &endp, 10);
    if (endp == start) return false;
    *out = v;
    return true;
}

/* Extract a JSON boolean value by key. Returns true if found, sets *out. */
static bool xl_json_bool(const char *json, const char *key, bool *out) {
    if (!json || !key || !out) return false;
    char search[64];
    snprintf(search, sizeof(search), "\"%s\":", key);
    const char *start = strstr(json, search);
    if (!start) return false;
    start += strlen(search);
    while (*start == ' ') start++;
    if (strncmp(start, "true", 4) == 0)  { *out = true;  return true; }
    if (strncmp(start, "false", 5) == 0) { *out = false; return true; }
    return false;
}

/* ── Per-protocol match functions ───────────────────────────────── */

/* Generic matcher: preserves pre-HTTP behavior (0.95 exact, 0.85 normalized). */
static double match_generic(const xl_endpoint_t *prod, const xl_endpoint_t *cons) {
    if (strcmp(prod->identifier, cons->identifier) == 0) return 0.95;
    if (prod->identifier_norm[0] != '\0' &&
        strcmp(prod->identifier_norm, cons->identifier_norm) == 0) {
        return 0.85;
    }
    return 0.0;
}

/* HTTP matcher: dispatches on producer identifier shape (route / service / env). */
static double match_http(const xl_endpoint_t *prod, const xl_endpoint_t *cons,
                         uint32_t *signals_used) {
    if (signals_used) *signals_used = 0;
    const char *pid = prod->identifier;
    const char *cid = cons->identifier;

    /* Env-level: "env:<VAR>" */
    if (strncmp(pid, "env:", 4) == 0) {
        /* Require consumer signals bitmask includes S3 (bit 4) OR S4 (bit 8). */
        long signals = 0;
        if (!xl_json_int(cons->extra, "signals", &signals)) return 0.0;
        if ((signals & 0x04) == 0 && (signals & 0x08) == 0) return 0.0;

        /* Suppress generic env-var consumers. */
        bool generic = false;
        if (xl_json_bool(cons->extra, "generic", &generic) && generic) return 0.0;

        /* Match producer VAR against consumer's declared env_var. */
        char env_var[128];
        if (!xl_json_str(cons->extra, "env_var", env_var, sizeof(env_var))) return 0.0;
        const char *prod_var = pid + 4;
        if (strcmp(prod_var, env_var) == 0) {
            if (signals_used) *signals_used = (uint32_t)(signals & 0x0C);
            return 0.50;
        }
        return 0.0;
    }

    /* Service-level: "http://<host>" */
    if (strncmp(pid, "http://", 7) == 0) {
        const char *prod_host = pid + 7;
        char svc_name[128];
        if (!xl_json_str(cons->extra, "service_name", svc_name, sizeof(svc_name))) {
            return 0.0;
        }
        if (strcmp(prod_host, svc_name) == 0) {
            if (signals_used) *signals_used = 0x01;
            return 0.60;
        }
        return 0.0;
    }

    /* Route-level: "<METHOD> <path>" — has a space, no env:/http:// prefix. */
    const char *prod_sp = strchr(pid, ' ');
    if (!prod_sp) return 0.0;

    /* Consumer must also be route-level (has a space, no env:/http:// prefix). */
    if (strncmp(cid, "env:", 4) == 0) return 0.0;
    if (strncmp(cid, "http://", 7) == 0) return 0.0;
    const char *cons_sp = strchr(cid, ' ');
    if (!cons_sp) return 0.0;

    /* Exact route-level match. */
    if (strcmp(pid, cid) == 0) {
        if (signals_used) *signals_used = 0x02;
        return 0.95;
    }

    /* Path-only fuzzy via cbm_path_match_score. */
    const char *prod_path = prod_sp + 1;
    const char *cons_path = cons_sp + 1;
    double score = cbm_path_match_score(prod_path, cons_path);
    if (score > 0.0) {
        if (signals_used) *signals_used = 0x02;
        return score;
    }
    return 0.0;
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
            "SELECT project, protocol, role, identifier, node_qn, file_path, extra "
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
        col = (const char *)sqlite3_column_text(stmt, 6);
        if (col) snprintf(ep->extra, sizeof(ep->extra), "%s", col);

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
        "  extra_json TEXT DEFAULT '{}',"
        "  updated_at TEXT NOT NULL,"
        "  UNIQUE(protocol, identifier, producer_qn, consumer_qn)"
        ");", NULL, NULL, NULL);

    /* Migrate older DBs that may be missing extra_json */
    sqlite3_exec(db, "ALTER TABLE cross_links ADD COLUMN extra_json TEXT DEFAULT '{}';",
                 NULL, NULL, NULL);

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
        " consumer_project, consumer_qn, consumer_file, confidence, extra_json, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?);", -1, &ins, NULL);
    if (!ins) {
        cbm_log_warn("crosslink.prepare_failed", "path", db_path);
        sqlite3_close(db);
        return -1;
    }

    sqlite3_exec(db, "BEGIN TRANSACTION;", NULL, NULL, NULL);

    int link_count = 0;
    int ambiguous_dropped = 0;

    /* Candidate buffer for HTTP ambiguity handling. */
    typedef struct {
        int consumer_idx;
        double raw_conf;
    } http_candidate_t;
    const int MAX_CANDIDATES = 64;
    http_candidate_t cands[MAX_CANDIDATES];

    /* O(n^2) matching — acceptable for expected sizes (few thousand endpoints) */
    for (int pi = 0; pi < count; pi++) {
        if (strcmp(endpoints[pi].role, "producer") != 0) continue;
        const xl_endpoint_t *prod = &endpoints[pi];
        const bool is_http = (strcmp(prod->protocol, "http") == 0);

        /* Collect candidate consumers for this producer. */
        int n_cands = 0;
        for (int ci = 0; ci < count; ci++) {
            if (strcmp(endpoints[ci].role, "consumer") != 0) continue;
            const xl_endpoint_t *cons = &endpoints[ci];

            /* Skip same project */
            if (strcmp(prod->project, cons->project) == 0) continue;
            /* Must be same protocol */
            if (strcmp(prod->protocol, cons->protocol) != 0) continue;

            double conf;
            uint32_t signals_used = 0;
            if (is_http) {
                conf = match_http(prod, cons, &signals_used);
            } else {
                conf = match_generic(prod, cons);
            }
            if (conf <= 0.0) continue;
            if (is_http && conf < SL_MIN_CONFIDENCE) continue;

            if (n_cands < MAX_CANDIDATES) {
                cands[n_cands].consumer_idx = ci;
                cands[n_cands].raw_conf = conf;
                n_cands++;
            }
        }

        if (n_cands == 0) continue;

        /* Non-HTTP: emit one row per candidate, raw confidence, no ambiguity. */
        if (!is_http) {
            for (int k = 0; k < n_cands; k++) {
                const xl_endpoint_t *cons = &endpoints[cands[k].consumer_idx];
                sqlite3_bind_text(ins, 1, prod->protocol, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 2, prod->identifier, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 3, prod->project, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 4, prod->node_qn, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 5, prod->file_path, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 6, cons->project, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 7, cons->node_qn, -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 8, cons->file_path, -1, SQLITE_STATIC);
                sqlite3_bind_double(ins, 9, cands[k].raw_conf);
                sqlite3_bind_text(ins, 10, "{}", -1, SQLITE_STATIC);
                sqlite3_bind_text(ins, 11, timestamp, -1, SQLITE_STATIC);
                sqlite3_step(ins);
                sqlite3_reset(ins);
                link_count++;
            }
            continue;
        }

        /* HTTP: apply ambiguity handling. */
        int emit_count = n_cands;
        if (emit_count > 3) {
            /* Pick top-3 by raw_conf (simple partial selection sort). */
            for (int a = 0; a < 3; a++) {
                int best = a;
                for (int b = a + 1; b < n_cands; b++) {
                    if (cands[b].raw_conf > cands[best].raw_conf) best = b;
                }
                if (best != a) {
                    http_candidate_t tmp = cands[a];
                    cands[a] = cands[best];
                    cands[best] = tmp;
                }
            }
            ambiguous_dropped++;
            cbm_log_info("http.ambiguous_dropped",
                         "producer", prod->identifier,
                         "candidates", itoa_buf(n_cands));
            emit_count = 3;
        }

        double divisor = (double)emit_count;
        for (int k = 0; k < emit_count; k++) {
            const xl_endpoint_t *cons = &endpoints[cands[k].consumer_idx];

            /* Build ambiguous_with JSON array of other consumer projects. */
            char extra_json[512];
            if (emit_count > 1) {
                char list[400];
                list[0] = '\0';
                int off = 0;
                for (int j = 0; j < emit_count; j++) {
                    if (j == k) continue;
                    const xl_endpoint_t *other = &endpoints[cands[j].consumer_idx];
                    int written = snprintf(list + off, sizeof(list) - (size_t)off,
                                           "%s\"%s\"",
                                           off == 0 ? "" : ",",
                                           other->project);
                    if (written < 0 || written >= (int)(sizeof(list) - (size_t)off)) break;
                    off += written;
                }
                snprintf(extra_json, sizeof(extra_json),
                         "{\"ambiguous_with\":[%s]}", list);
            } else {
                snprintf(extra_json, sizeof(extra_json), "{}");
            }

            double emit_conf = cands[k].raw_conf / divisor;

            sqlite3_bind_text(ins, 1, prod->protocol, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 2, prod->identifier, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 3, prod->project, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 4, prod->node_qn, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 5, prod->file_path, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 6, cons->project, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 7, cons->node_qn, -1, SQLITE_STATIC);
            sqlite3_bind_text(ins, 8, cons->file_path, -1, SQLITE_STATIC);
            sqlite3_bind_double(ins, 9, emit_conf);
            sqlite3_bind_text(ins, 10, extra_json, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(ins, 11, timestamp, -1, SQLITE_STATIC);
            sqlite3_step(ins);
            sqlite3_reset(ins);
            link_count++;
        }
    }

    sqlite3_exec(db, "COMMIT;", NULL, NULL, NULL);
    if (ins) sqlite3_finalize(ins);
    sqlite3_close(db);

    if (ambiguous_dropped > 0) {
        cbm_log_info("crosslink.http_ambiguous_total",
                     "count", itoa_buf(ambiguous_dropped));
    }
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
