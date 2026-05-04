/*
 * pass_crossrepolinks.c — Cross-project messaging endpoint matching.
 *
 * Two entry points:
 *   1. cbm_persist_endpoints() — write discovered endpoints to a project's .db
 *   2. cbm_cross_project_link() — scan all project DBs, match producers to
 *      consumers across project boundaries for messaging protocols, write
 *      bidirectional CROSS_* edges into each project's edges table.
 *
 * HTTP/gRPC/GraphQL/tRPC are owned by the upstream Route-QN matcher in
 * pass_cross_repo.c and are intentionally skipped here.
 */
#include "servicelink.h"
#include "pass_cross_repo.h"
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
    const char *edge_type;      /* CROSS_* edge type for this protocol */
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

/* Generic matcher: 0.95 exact, 0.85 normalized. */
static double match_generic(const xl_endpoint_t *prod, const xl_endpoint_t *cons) {
    if (strcmp(prod->identifier, cons->identifier) == 0) return 0.95;
    if (prod->identifier_norm[0] != '\0' &&
        strcmp(prod->identifier_norm, cons->identifier_norm) == 0) {
        return 0.85;
    }
    return 0.0;
}

/* Load endpoints from a single project DB. Skips endpoints whose protocol is
 * owned by upstream's Route-QN matcher (http/grpc/graphql/trpc) — those
 * return NULL from cbm_messaging_protocol_to_cross_edge(). */
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
        return 0;
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
        const char *protocol_col = (const char *)sqlite3_column_text(stmt, 1);
        const char *edge_type = cbm_messaging_protocol_to_cross_edge(protocol_col);
        if (!edge_type) {
            /* http/grpc/graphql/trpc — owned by upstream matcher */
            continue;
        }

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
        snprintf(ep->protocol, sizeof(ep->protocol), "%s", protocol_col);
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
        ep->edge_type = edge_type;

        normalize_identifier(ep->identifier, ep->identifier_norm,
                             (int)sizeof(ep->identifier_norm));
        (*out_count)++;
        added++;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return added;
}

/* ── Edge emission ────────────────────────────────────────────────── */

/* Store cache keyed by project name. We open each DB lazily and keep it open
 * until the run completes. */
typedef struct {
    char project[256];
    cbm_store_t *store;
} xl_store_cache_entry_t;

typedef struct {
    xl_store_cache_entry_t *items;
    int count;
    int cap;
    char cache_dir[1024];
} xl_store_cache_t;

static cbm_store_t *xl_store_for(xl_store_cache_t *cache, const char *project) {
    for (int i = 0; i < cache->count; i++) {
        if (strcmp(cache->items[i].project, project) == 0) {
            return cache->items[i].store;
        }
    }
    if (cache->count >= cache->cap) {
        int new_cap = cache->cap == 0 ? 16 : cache->cap * 2;
        xl_store_cache_entry_t *new_items = realloc(
            cache->items, (size_t)new_cap * sizeof(xl_store_cache_entry_t));
        if (!new_items) return NULL;
        cache->items = new_items;
        cache->cap = new_cap;
    }
    char db_path[1280];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache->cache_dir, project);
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        cbm_log_warn("crosslink.store_open_failed", "project", project);
        return NULL;
    }
    xl_store_cache_entry_t *e = &cache->items[cache->count++];
    snprintf(e->project, sizeof(e->project), "%s", project);
    e->store = store;
    return store;
}

static void xl_store_cache_close_all(xl_store_cache_t *cache) {
    for (int i = 0; i < cache->count; i++) {
        cbm_store_close(cache->items[i].store);
    }
    free(cache->items);
    cache->items = NULL;
    cache->count = 0;
    cache->cap = 0;
}

/* Resolve a node_qn in `project`'s DB to a node id. Returns 0 if not found. */
static int64_t resolve_node_id(cbm_store_t *store, const char *project, const char *qn) {
    if (!store || !qn || !qn[0]) return 0;
    cbm_node_t n = {0};
    if (cbm_store_find_node_by_qn(store, project, qn, &n) != 0) {
        return 0;
    }
    int64_t id = n.id;
    /* scan_node heap_strdup's strings — free them */
    free((void *)n.project);
    free((void *)n.label);
    free((void *)n.name);
    free((void *)n.qualified_name);
    free((void *)n.file_path);
    free((void *)n.properties_json);
    return id;
}

/* JSON string escaping for identifier/qn embedded in edge properties. Simple:
 * escape '"' and '\'. Messaging identifiers are alphanumeric-plus-dots/slashes
 * in practice, so we don't need full RFC 8259 escaping. */
static void json_escape(const char *src, char *dst, size_t dst_sz) {
    size_t j = 0;
    for (size_t i = 0; src[i] && j + 2 < dst_sz; i++) {
        if (src[i] == '"' || src[i] == '\\') {
            if (j + 3 >= dst_sz) break;
            dst[j++] = '\\';
        }
        dst[j++] = src[i];
    }
    if (j < dst_sz) dst[j] = '\0'; else dst[dst_sz - 1] = '\0';
}

/* MessagingChannel synthetic-anchor label and QN prefix. Mirrors upstream's
 * Route-QN anchor pattern (`__route__<method>__<path>`). The channel node is
 * a per-project local anchor that lets cross-project edges stay within one DB
 * (FK-safe) while still encoding the protocol+identifier the match is about. */
#define CBM_MESSAGING_CHANNEL_LABEL "MessagingChannel"
#define CBM_MESSAGING_CHANNEL_FILE  "<cross-repo-link>"

/* Build the QN for a channel anchor node: __channel__<protocol>__<identifier>. */
static void build_channel_qn(char *buf, size_t bufsz,
                             const char *protocol, const char *identifier) {
    snprintf(buf, bufsz, "__channel__%s__%s",
             protocol ? protocol : "", identifier ? identifier : "");
}

/* Build properties JSON for a MessagingChannel anchor node. */
static void build_channel_props(char *buf, size_t bufsz,
                                const char *protocol, const char *identifier) {
    char p[64], id[300];
    json_escape(protocol ? protocol : "", p, sizeof(p));
    json_escape(identifier ? identifier : "", id, sizeof(id));
    snprintf(buf, bufsz, "{\"protocol\":\"%s\",\"identifier\":\"%s\"}", p, id);
}

/* Find or create a MessagingChannel anchor node in `store` for the given
 * project/protocol/identifier. Returns the local node id, or 0 on failure. */
static int64_t find_or_create_channel(cbm_store_t *store, const char *project,
                                      const char *protocol, const char *identifier) {
    char qn[512];
    build_channel_qn(qn, sizeof(qn), protocol, identifier);

    cbm_node_t existing = {0};
    if (cbm_store_find_node_by_qn(store, project, qn, &existing) == 0) {
        int64_t id = existing.id;
        cbm_node_free_fields(&existing);
        return id;
    }

    char props[640];
    build_channel_props(props, sizeof(props), protocol, identifier);
    cbm_node_t channel = {
        .project = project,
        .label = CBM_MESSAGING_CHANNEL_LABEL,
        .name = identifier,
        .qualified_name = qn,
        .file_path = CBM_MESSAGING_CHANNEL_FILE,
        .properties_json = props,
    };
    int64_t id = cbm_store_upsert_node(store, &channel);
    return id > 0 ? id : 0;
}

/* Build the properties JSON for a producer-side CROSS_* edge. */
static void build_producer_props(char *buf, size_t bufsz,
                                 const char *target_project,
                                 const char *target_function,
                                 const char *target_file,
                                 const char *identifier,
                                 double confidence,
                                 const char *protocol) {
    char tp[300], tf[600], tfile[300], id[300];
    json_escape(target_project ? target_project : "", tp, sizeof(tp));
    json_escape(target_function ? target_function : "", tf, sizeof(tf));
    json_escape(target_file ? target_file : "", tfile, sizeof(tfile));
    json_escape(identifier ? identifier : "", id, sizeof(id));

    snprintf(buf, bufsz,
        "{\"target_project\":\"%s\",\"target_function\":\"%s\","
        "\"target_file\":\"%s\",\"identifier\":\"%s\","
        "\"protocol\":\"%s\",\"confidence\":%.3f}",
        tp, tf, tfile, id, protocol ? protocol : "", confidence);
}

/* Build the properties JSON for a consumer-side CROSS_* edge. */
static void build_consumer_props(char *buf, size_t bufsz,
                                 const char *source_project,
                                 const char *source_function,
                                 const char *source_file,
                                 const char *identifier,
                                 double confidence,
                                 const char *protocol) {
    char sp[300], sf[600], sfile[300], id[300];
    json_escape(source_project ? source_project : "", sp, sizeof(sp));
    json_escape(source_function ? source_function : "", sf, sizeof(sf));
    json_escape(source_file ? source_file : "", sfile, sizeof(sfile));
    json_escape(identifier ? identifier : "", id, sizeof(id));

    snprintf(buf, bufsz,
        "{\"source_project\":\"%s\",\"source_function\":\"%s\","
        "\"source_file\":\"%s\",\"identifier\":\"%s\","
        "\"protocol\":\"%s\",\"confidence\":%.3f}",
        sp, sf, sfile, id, protocol ? protocol : "", confidence);
}

/* Emit a bidirectional CROSS_* edge pair for a producer→consumer match.
 *
 * Each side's edge is intra-DB and anchored on a local MessagingChannel
 * node (created on demand, mirrored across DBs by sharing the same QN
 * `__channel__<proto>__<id>`):
 *   producer DB:  function → channel  (CROSS_<PROTO>_CALLS)
 *   consumer DB:  channel  → function (CROSS_<PROTO>_CALLS)
 *
 * This keeps target_id within the local nodes table (FK-safe) and lets
 * fanout (one producer, many consumers) record distinct edges per match
 * via the channel anchor. */
static int emit_cross_edge_pair(xl_store_cache_t *cache,
                                const xl_endpoint_t *prod,
                                const xl_endpoint_t *cons,
                                double confidence) {
    cbm_store_t *prod_store = xl_store_for(cache, prod->project);
    cbm_store_t *cons_store = xl_store_for(cache, cons->project);
    if (!prod_store || !cons_store) return 0;

    int64_t prod_id = resolve_node_id(prod_store, prod->project, prod->node_qn);
    int64_t cons_id = resolve_node_id(cons_store, cons->project, cons->node_qn);
    if (prod_id == 0) {
        cbm_log_warn("crosslink.unresolved_qn", "project", prod->project,
                     "qn", prod->node_qn);
        return 0;
    }
    if (cons_id == 0) {
        cbm_log_warn("crosslink.unresolved_qn", "project", cons->project,
                     "qn", cons->node_qn);
        return 0;
    }

    int64_t prod_channel = find_or_create_channel(prod_store, prod->project,
                                                  prod->protocol, prod->identifier);
    int64_t cons_channel = find_or_create_channel(cons_store, cons->project,
                                                  cons->protocol, cons->identifier);
    if (prod_channel == 0 || cons_channel == 0) {
        cbm_log_warn("crosslink.channel_create_failed", "prod", prod->project,
                     "cons", cons->project);
        return 0;
    }

    /* Forward: function → channel in producer's DB. */
    char fwd[1536];
    build_producer_props(fwd, sizeof(fwd), cons->project, cons->node_qn, cons->file_path,
                         prod->identifier, confidence, prod->protocol);
    cbm_edge_t fwd_edge = {
        .project = prod->project,
        .source_id = prod_id,
        .target_id = prod_channel,
        .type = prod->edge_type,
        .properties_json = fwd,
    };
    int64_t fwd_rc = cbm_store_insert_edge(prod_store, &fwd_edge);

    /* Reverse: channel → function in consumer's DB. */
    char rev[1536];
    build_consumer_props(rev, sizeof(rev), prod->project, prod->node_qn, prod->file_path,
                         prod->identifier, confidence, prod->protocol);
    cbm_edge_t rev_edge = {
        .project = cons->project,
        .source_id = cons_channel,
        .target_id = cons_id,
        .type = cons->edge_type,
        .properties_json = rev,
    };
    int64_t rev_rc = cbm_store_insert_edge(cons_store, &rev_edge);

    if (fwd_rc <= 0 || rev_rc <= 0) {
        cbm_log_warn("crosslink.insert_failed", "prod", prod->project,
                     "cons", cons->project);
        return 0;
    }
    return 1;
}

/* Wipe existing messaging CROSS_* edges for a project's DB. Called once per
 * project per run to keep output idempotent. */
static void wipe_messaging_cross_edges(cbm_store_t *store, const char *project) {
    for (int t = 0; t < CBM_MESSAGING_CROSS_EDGE_TYPE_COUNT; t++) {
        cbm_store_delete_edges_by_type(store, project, CBM_MESSAGING_CROSS_EDGE_TYPES[t]);
    }
}

/* Track which projects have been wiped this run. */
typedef struct {
    char names[256][256];
    int count;
} xl_wiped_set_t;

static bool xl_wiped_contains(const xl_wiped_set_t *set, const char *project) {
    for (int i = 0; i < set->count; i++) {
        if (strcmp(set->names[i], project) == 0) return true;
    }
    return false;
}

static void xl_wiped_add(xl_wiped_set_t *set, const char *project) {
    if (set->count >= 256) return;
    snprintf(set->names[set->count++], sizeof(set->names[0]), "%s", project);
}

/* Ensure a project's messaging CROSS_* edges have been wiped exactly once. */
static void ensure_wiped(xl_store_cache_t *cache, xl_wiped_set_t *wiped,
                         const char *project) {
    if (xl_wiped_contains(wiped, project)) return;
    cbm_store_t *store = xl_store_for(cache, project);
    if (!store) return;
    wipe_messaging_cross_edges(store, project);
    xl_wiped_add(wiped, project);
}

/* Match producers to consumers and emit CROSS_* edges. */
static int match_and_emit(xl_store_cache_t *cache,
                          const xl_endpoint_t *endpoints, int count) {
    xl_wiped_set_t wiped = {0};

    /* Wipe every project that owns an endpoint (even ones that will produce
     * no matches this run) so stale messaging CROSS_* edges don't linger. */
    for (int i = 0; i < count; i++) {
        ensure_wiped(cache, &wiped, endpoints[i].project);
    }

    int link_count = 0;
    for (int pi = 0; pi < count; pi++) {
        if (strcmp(endpoints[pi].role, "producer") != 0) continue;
        const xl_endpoint_t *prod = &endpoints[pi];

        for (int ci = 0; ci < count; ci++) {
            if (strcmp(endpoints[ci].role, "consumer") != 0) continue;
            const xl_endpoint_t *cons = &endpoints[ci];

            if (strcmp(prod->project, cons->project) == 0) continue;
            if (strcmp(prod->protocol, cons->protocol) != 0) continue;

            double conf = match_generic(prod, cons);
            if (conf <= 0.0) continue;

            link_count += emit_cross_edge_pair(cache, prod, cons, conf);
        }
    }
    return link_count;
}

/* Main entry point: scan cache_dir for *.db, load messaging endpoints, match
 * across projects, emit bidirectional CROSS_* edges. */
int cbm_cross_project_link(const char *cache_dir) {
    if (!cache_dir) return -1;

    cbm_log_info("crosslink.start", "cache_dir", cache_dir);

    DIR *dir = opendir(cache_dir);
    if (!dir) {
        cbm_log_warn("crosslink.opendir_failed", "dir", cache_dir);
        return -1;
    }

    /* Collect messaging endpoints from all project DBs */
    xl_endpoint_t *all_endpoints = NULL;
    int total = 0, cap = 0;

    struct dirent *ent;
    while ((ent = readdir(dir)) != NULL) {
        const char *name = ent->d_name;
        int len = (int)strlen(name);

        if (len < 4 || strcmp(name + len - 3, ".db") != 0) continue;
        /* Skip leading-underscore (catches legacy _crosslinks.db) and tmp-*. */
        if (name[0] == '_' || strncmp(name, "tmp-", 4) == 0) continue;

        char db_path[1280];
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

    xl_store_cache_t cache = {0};
    snprintf(cache.cache_dir, sizeof(cache.cache_dir), "%s", cache_dir);

    int links = match_and_emit(&cache, all_endpoints, total);

    xl_store_cache_close_all(&cache);
    free(all_endpoints);

    cbm_log_info("crosslink.done", "total_endpoints", itoa_buf(total),
                 "cross_links", itoa_buf(links));
    return links;
}
