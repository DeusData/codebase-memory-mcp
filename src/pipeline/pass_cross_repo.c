/*
 * pass_cross_repo.c — Cross-repo intelligence: match Routes, Channels, and
 * async topics across indexed projects to create CROSS_* edges.
 *
 * For each HTTP_CALLS/ASYNC_CALLS edge in the source project, looks up the
 * target Route QN in other project DBs. For each Channel node with EMITS
 * edges, looks for matching LISTENS_ON in other projects (and vice versa).
 *
 * Edges are written bidirectionally: both source and target project DBs
 * get a CROSS_* edge so the link is visible from either side.
 */
#include "pipeline/pass_cross_repo.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"

#include <sqlite3/sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Constants ───────────────────────────────────────────────────── */

enum {
    CR_PATH_BUF = 1024,
    CR_QN_BUF = 512,
    CR_PROPS_BUF = 2048,
    CR_MAX_EDGES = 4096,
    CR_DB_EXT_LEN = 3, /* strlen(".db") */
    CR_INIT_CAP = 32,
    CR_COL_3 = 3,
    CR_COL_4 = 4,
};

#define CR_MS_PER_SEC 1000.0
#define CR_NS_PER_MS 1000000.0

/* TLS buffer for integer-to-string in log calls. */
static CBM_TLS char cr_ibuf[CBM_SZ_32];
static const char *cr_itoa(int v) {
    snprintf(cr_ibuf, sizeof(cr_ibuf), "%d", v);
    return cr_ibuf;
}

/* ── Helpers ─────────────────────────────────────────────────────── */

static const char *cr_cache_dir(void) {
    const char *dir = cbm_resolve_cache_dir();
    return dir ? dir : cbm_tmpdir();
}

static void cr_db_path(const char *project, char *buf, size_t bufsz) {
    snprintf(buf, bufsz, "%s/%s.db", cr_cache_dir(), project);
}

/* Extract a JSON string property from properties_json.
 * Writes into buf, returns buf on success, NULL on miss. */
static const char *json_str_prop(const char *json, const char *key, char *buf, size_t bufsz) {
    if (!json || !key) {
        return NULL;
    }
    char pat[CBM_SZ_128];
    snprintf(pat, sizeof(pat), "\"%s\":\"", key);
    const char *start = strstr(json, pat);
    if (!start) {
        return NULL;
    }
    start += strlen(pat);
    const char *end = strchr(start, '"');
    if (!end) {
        return NULL;
    }
    size_t len = (size_t)(end - start);
    if (len >= bufsz) {
        len = bufsz - SKIP_ONE;
    }
    memcpy(buf, start, len);
    buf[len] = '\0';
    return buf;
}

/* Build CROSS_* edge properties JSON. */
static void build_cross_props(char *buf, size_t bufsz, const char *target_project,
                              const char *target_function, const char *target_file,
                              const char *url_or_channel, const char *extra_key,
                              const char *extra_val) {
    int n = snprintf(buf, bufsz,
                     "{\"target_project\":\"%s\",\"target_function\":\"%s\","
                     "\"target_file\":\"%s\"",
                     target_project ? target_project : "", target_function ? target_function : "",
                     target_file ? target_file : "");
    if (url_or_channel && url_or_channel[0]) {
        n += snprintf(buf + n, bufsz - (size_t)n, ",\"%s\":\"%s\"",
                      extra_key ? extra_key : "url_path", url_or_channel);
    }
    if (extra_val && extra_val[0]) {
        n += snprintf(buf + n, bufsz - (size_t)n, ",\"%s\":\"%s\"",
                      extra_key ? "transport" : "method", extra_val);
    }
    snprintf(buf + n, bufsz - (size_t)n, "}");
}

/* Delete all CROSS_* edges for a project from a store. */
static void delete_cross_edges(cbm_store_t *store, const char *project) {
    cbm_store_delete_edges_by_type(store, project, "CROSS_HTTP_CALLS");
    cbm_store_delete_edges_by_type(store, project, "CROSS_ASYNC_CALLS");
    cbm_store_delete_edges_by_type(store, project, "CROSS_CHANNEL");
    cbm_store_delete_edges_by_type(store, project, "CROSS_GRPC_CALLS");
    cbm_store_delete_edges_by_type(store, project, "CROSS_GRAPHQL_CALLS");
    cbm_store_delete_edges_by_type(store, project, "CROSS_TRPC_CALLS");
}

/* Insert a CROSS_* edge into a store. */
static void insert_cross_edge(cbm_store_t *store, const char *project, int64_t from_id,
                              int64_t to_id, const char *edge_type, const char *props) {
    cbm_edge_t edge = {
        .project = project,
        .source_id = from_id,
        .target_id = to_id,
        .type = edge_type,
        .properties_json = props,
    };
    cbm_store_insert_edge(store, &edge);
}

/* Look up a node's name and file_path by id. */
static void lookup_node_info(struct sqlite3 *db, int64_t node_id, char *name_out, size_t name_sz,
                             char *file_out, size_t file_sz) {
    name_out[0] = '\0';
    file_out[0] = '\0';
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT name, file_path FROM nodes WHERE id = ?1", CBM_NOT_FOUND,
                           &st, NULL) != SQLITE_OK) {
        return;
    }
    sqlite3_bind_int64(st, SKIP_ONE, node_id);
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *nm = (const char *)sqlite3_column_text(st, 0);
        const char *fp = (const char *)sqlite3_column_text(st, SKIP_ONE);
        if (nm) {
            snprintf(name_out, name_sz, "%s", nm);
        }
        if (fp) {
            snprintf(file_out, file_sz, "%s", fp);
        }
    }
    sqlite3_finalize(st);
}

/* ── Phase A: HTTP Route matching ────────────────────────────────── */

/* Find a Route node in target_store by QN and return the handler function's
 * node id, name, and file_path via HANDLES edges. Returns 0 if not found. */
static int64_t find_route_handler(cbm_store_t *target_store, const char *route_qn,
                                  char *handler_name, size_t name_sz, char *handler_file,
                                  size_t file_sz) {
    handler_name[0] = '\0';
    handler_file[0] = '\0';
    struct sqlite3 *db = cbm_store_get_db(target_store);
    if (!db) {
        return 0;
    }

    /* Find Route node by QN */
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(
            db, "SELECT id FROM nodes WHERE qualified_name = ?1 AND label = 'Route' LIMIT 1",
            CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(s, SKIP_ONE, route_qn, CBM_NOT_FOUND, SQLITE_STATIC);
    int64_t route_id = 0;
    if (sqlite3_step(s) == SQLITE_ROW) {
        route_id = sqlite3_column_int64(s, 0);
    }
    sqlite3_finalize(s);
    if (route_id == 0) {
        return 0;
    }

    /* Follow HANDLES edge to find the handler function */
    if (sqlite3_prepare_v2(db,
                           "SELECT n.id, n.name, n.file_path FROM edges e "
                           "JOIN nodes n ON n.id = e.source_id "
                           "WHERE e.target_id = ?1 AND e.type = 'HANDLES' LIMIT 1",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_int64(s, SKIP_ONE, route_id);
    int64_t handler_id = 0;
    if (sqlite3_step(s) == SQLITE_ROW) {
        handler_id = sqlite3_column_int64(s, 0);
        const char *n = (const char *)sqlite3_column_text(s, SKIP_ONE);
        const char *f = (const char *)sqlite3_column_text(s, PAIR_LEN);
        if (n) {
            snprintf(handler_name, name_sz, "%s", n);
        }
        if (f) {
            snprintf(handler_file, file_sz, "%s", f);
        }
    }
    sqlite3_finalize(s);
    return handler_id;
}

/* Emit CROSS_* edge for a route match: forward into source, reverse into target. */
static void emit_cross_route_bidirectional(cbm_store_t *src_store, const char *src_project,
                                           struct sqlite3 *src_db, int64_t caller_id,
                                           int64_t local_route_id, cbm_store_t *tgt_store,
                                           const char *tgt_project, int64_t handler_id,
                                           const char *route_qn, const char *handler_name,
                                           const char *handler_file, const char *url_path,
                                           const char *method, const char *edge_type) {
    /* Forward: caller → local Route in source DB */
    char fwd[CR_PROPS_BUF];
    build_cross_props(fwd, sizeof(fwd), tgt_project, handler_name, handler_file, url_path,
                      "url_path", method);
    insert_cross_edge(src_store, src_project, caller_id, local_route_id, edge_type, fwd);

    /* Reverse: handler → Route in target DB */
    struct sqlite3 *tgt_db = cbm_store_get_db(tgt_store);
    if (!tgt_db) {
        return;
    }
    sqlite3_stmt *rq = NULL;
    if (sqlite3_prepare_v2(tgt_db, "SELECT id FROM nodes WHERE qualified_name = ?1 LIMIT 1",
                           CBM_NOT_FOUND, &rq, NULL) != SQLITE_OK) {
        return;
    }
    sqlite3_bind_text(rq, SKIP_ONE, route_qn, CBM_NOT_FOUND, SQLITE_STATIC);
    int64_t tgt_route_id = 0;
    if (sqlite3_step(rq) == SQLITE_ROW) {
        tgt_route_id = sqlite3_column_int64(rq, 0);
    }
    sqlite3_finalize(rq);
    if (tgt_route_id == 0) {
        return;
    }

    char caller_name[CBM_SZ_256] = {0};
    char caller_file[CBM_SZ_512] = {0};
    lookup_node_info(src_db, caller_id, caller_name, sizeof(caller_name), caller_file,
                     sizeof(caller_file));

    char rev[CR_PROPS_BUF];
    build_cross_props(rev, sizeof(rev), src_project, caller_name, caller_file, url_path, "url_path",
                      method);
    insert_cross_edge(tgt_store, tgt_project, handler_id, tgt_route_id, edge_type, rev);
}

static int match_http_routes(cbm_store_t *src_store, const char *src_project,
                             cbm_store_t *tgt_store, const char *tgt_project) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return 0;
    }

    /* Find all HTTP_CALLS edges in source project */
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db,
                           "SELECT e.source_id, e.target_id, e.properties FROM edges e "
                           "WHERE e.project = ?1 AND e.type = 'HTTP_CALLS'",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC);

    int count = 0;
    while (sqlite3_step(s) == SQLITE_ROW && count < CR_MAX_EDGES) {
        int64_t caller_id = sqlite3_column_int64(s, 0);
        int64_t route_id = sqlite3_column_int64(s, SKIP_ONE);
        const char *props = (const char *)sqlite3_column_text(s, PAIR_LEN);

        char url_path[CBM_SZ_256] = {0};
        char method[CBM_SZ_32] = {0};
        json_str_prop(props, "url_path", url_path, sizeof(url_path));
        json_str_prop(props, "method", method, sizeof(method));
        if (!url_path[0]) {
            continue;
        }

        /* Build the expected Route QN in the target project */
        char route_qn[CR_QN_BUF];
        snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", method[0] ? method : "ANY",
                 url_path);

        char handler_name[CBM_SZ_256] = {0};
        char handler_file[CBM_SZ_512] = {0};
        int64_t handler_id =
            find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                               handler_file, sizeof(handler_file));
        if (handler_id == 0) {
            /* Try without method (ANY) */
            snprintf(route_qn, sizeof(route_qn), "__route__ANY__%s", url_path);
            handler_id = find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                                            handler_file, sizeof(handler_file));
        }
        if (handler_id == 0) {
            continue;
        }

        emit_cross_route_bidirectional(src_store, src_project, src_db, caller_id, route_id,
                                       tgt_store, tgt_project, handler_id, route_qn, handler_name,
                                       handler_file, url_path, method, "CROSS_HTTP_CALLS");

        count++;
    }
    sqlite3_finalize(s);
    return count;
}

/* ── Phase B: Async matching ─────────────────────────────────────── */

static int match_async_routes(cbm_store_t *src_store, const char *src_project,
                              cbm_store_t *tgt_store, const char *tgt_project) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return 0;
    }

    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db,
                           "SELECT e.source_id, e.target_id, e.properties FROM edges e "
                           "WHERE e.project = ?1 AND e.type = 'ASYNC_CALLS'",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC);

    int count = 0;
    while (sqlite3_step(s) == SQLITE_ROW && count < CR_MAX_EDGES) {
        int64_t caller_id = sqlite3_column_int64(s, 0);
        int64_t route_id = sqlite3_column_int64(s, SKIP_ONE);
        const char *props = (const char *)sqlite3_column_text(s, PAIR_LEN);

        char url_path[CBM_SZ_256] = {0};
        char broker[CBM_SZ_128] = {0};
        json_str_prop(props, "url_path", url_path, sizeof(url_path));
        json_str_prop(props, "broker", broker, sizeof(broker));
        if (!url_path[0]) {
            continue;
        }

        char route_qn[CR_QN_BUF];
        snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", broker[0] ? broker : "async",
                 url_path);

        char handler_name[CBM_SZ_256] = {0};
        char handler_file[CBM_SZ_512] = {0};
        int64_t handler_id =
            find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                               handler_file, sizeof(handler_file));
        if (handler_id == 0) {
            continue;
        }

        char edge_props[CR_PROPS_BUF];
        build_cross_props(edge_props, sizeof(edge_props), tgt_project, handler_name, handler_file,
                          url_path, "url_path", broker);
        insert_cross_edge(src_store, src_project, caller_id, route_id, "CROSS_ASYNC_CALLS",
                          edge_props);
        count++;
    }
    sqlite3_finalize(s);
    return count;
}

/* ── Phase C: Channel matching ───────────────────────────────────── */

/* Try to find a matching listener in target DB for a channel name. */
static bool try_match_channel_listener(cbm_store_t *src_store, const char *src_project,
                                       cbm_store_t *tgt_store, const char *tgt_project,
                                       const char *channel_name, const char *transport,
                                       int64_t emitter_id, int64_t channel_id) {
    struct sqlite3 *tgt_db = cbm_store_get_db(tgt_store);
    if (!tgt_db) {
        return false;
    }
    sqlite3_stmt *tq = NULL;
    if (sqlite3_prepare_v2(tgt_db,
                           "SELECT n.id, e.source_id, fn.name, fn.file_path FROM nodes n "
                           "JOIN edges e ON e.target_id = n.id AND e.type = 'LISTENS_ON' "
                           "JOIN nodes fn ON fn.id = e.source_id "
                           "WHERE n.project = ?1 AND n.name = ?2 AND n.label = 'Channel' LIMIT 1",
                           CBM_NOT_FOUND, &tq, NULL) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_text(tq, SKIP_ONE, tgt_project, CBM_NOT_FOUND, SQLITE_STATIC);
    sqlite3_bind_text(tq, PAIR_LEN, channel_name, CBM_NOT_FOUND, SQLITE_STATIC);

    bool matched = false;
    if (sqlite3_step(tq) == SQLITE_ROW) {
        int64_t tgt_channel_id = sqlite3_column_int64(tq, 0);
        int64_t listener_id = sqlite3_column_int64(tq, SKIP_ONE);
        const char *listener_name = (const char *)sqlite3_column_text(tq, PAIR_LEN);
        const char *listener_file = (const char *)sqlite3_column_text(tq, CR_COL_3);

        /* Forward edge: emitter → local Channel */
        char fwd[CR_PROPS_BUF];
        build_cross_props(fwd, sizeof(fwd), tgt_project, listener_name ? listener_name : "",
                          listener_file ? listener_file : "", channel_name, "channel_name",
                          transport);
        insert_cross_edge(src_store, src_project, emitter_id, channel_id, "CROSS_CHANNEL", fwd);

        /* Reverse edge: listener → target Channel */
        char caller_name[CBM_SZ_256] = {0};
        char caller_file[CBM_SZ_512] = {0};
        lookup_node_info(cbm_store_get_db(src_store), emitter_id, caller_name, sizeof(caller_name),
                         caller_file, sizeof(caller_file));

        char rev[CR_PROPS_BUF];
        build_cross_props(rev, sizeof(rev), src_project, caller_name, caller_file, channel_name,
                          "channel_name", transport);
        insert_cross_edge(tgt_store, tgt_project, listener_id, tgt_channel_id, "CROSS_CHANNEL",
                          rev);
        matched = true;
    }
    sqlite3_finalize(tq);
    return matched;
}

static int match_channels(cbm_store_t *src_store, const char *src_project, cbm_store_t *tgt_store,
                          const char *tgt_project) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return 0;
    }

    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db,
                           "SELECT DISTINCT n.id, n.name, n.qualified_name, n.properties, "
                           "e.source_id FROM nodes n "
                           "JOIN edges e ON e.target_id = n.id AND e.type = 'EMITS' "
                           "WHERE n.project = ?1 AND n.label = 'Channel'",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC);

    int count = 0;
    while (sqlite3_step(s) == SQLITE_ROW && count < CR_MAX_EDGES) {
        const char *channel_name = (const char *)sqlite3_column_text(s, SKIP_ONE);
        const char *channel_qn = (const char *)sqlite3_column_text(s, PAIR_LEN);
        if (!channel_name || !channel_qn) {
            continue;
        }
        int64_t channel_id = sqlite3_column_int64(s, 0);
        const char *channel_props = (const char *)sqlite3_column_text(s, CR_COL_3);
        int64_t emitter_id = sqlite3_column_int64(s, CR_COL_4);

        char transport[CBM_SZ_64] = {0};
        json_str_prop(channel_props, "transport", transport, sizeof(transport));

        if (try_match_channel_listener(src_store, src_project, tgt_store, tgt_project, channel_name,
                                       transport, emitter_id, channel_id)) {
            count++;
        }
    }
    sqlite3_finalize(s);
    return count;
}

/* ── Phase D: Generic route-type matcher (gRPC, GraphQL, tRPC) ──── */

/* Look up a node's qualified_name by id. Returns true if found. */
static bool lookup_node_qn(struct sqlite3 *db, int64_t node_id, char *out, size_t out_sz) {
    out[0] = '\0';
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT qualified_name FROM nodes WHERE id = ?1", CBM_NOT_FOUND, &st,
                           NULL) != SQLITE_OK) {
        return false;
    }
    sqlite3_bind_int64(st, SKIP_ONE, node_id);
    bool found = false;
    if (sqlite3_step(st) == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(st, 0);
        if (qn) {
            snprintf(out, out_sz, "%s", qn);
            found = true;
        }
    }
    sqlite3_finalize(st);
    return found;
}

/* Match edges of a given type against Route nodes with a given QN prefix.
 * Reuses the same infrastructure as HTTP/async matching. */
static int match_typed_routes(cbm_store_t *src_store, const char *src_project,
                              cbm_store_t *tgt_store, const char *tgt_project,
                              const char *edge_type, const char *svc_key, const char *method_key,
                              const char *cross_edge_type) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return 0;
    }

    char sql[CBM_SZ_256];
    snprintf(sql, sizeof(sql),
             "SELECT e.source_id, e.target_id, e.properties FROM edges e "
             "WHERE e.project = ?1 AND e.type = '%s'",
             edge_type);

    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db, sql, CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return 0;
    }
    sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC);

    int count = 0;
    while (sqlite3_step(s) == SQLITE_ROW && count < CR_MAX_EDGES) {
        int64_t caller_id = sqlite3_column_int64(s, 0);
        int64_t route_id = sqlite3_column_int64(s, SKIP_ONE);
        const char *props = (const char *)sqlite3_column_text(s, PAIR_LEN);

        char svc_val[CBM_SZ_256] = {0};
        char meth_val[CBM_SZ_256] = {0};
        json_str_prop(props, svc_key, svc_val, sizeof(svc_val));
        json_str_prop(props, method_key, meth_val, sizeof(meth_val));
        if (!svc_val[0] && !meth_val[0]) {
            continue;
        }

        /* Look up the Route QN from the target node (already points to the Route). */
        char route_qn[CR_QN_BUF] = {0};
        if (!lookup_node_qn(src_db, route_id, route_qn, sizeof(route_qn))) {
            continue;
        }

        char handler_name[CBM_SZ_256] = {0};
        char handler_file[CBM_SZ_512] = {0};
        int64_t handler_id =
            find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                               handler_file, sizeof(handler_file));
        if (handler_id == 0) {
            continue;
        }

        emit_cross_route_bidirectional(src_store, src_project, src_db, caller_id, route_id,
                                       tgt_store, tgt_project, handler_id, route_qn, handler_name,
                                       handler_file, svc_val, svc_key, cross_edge_type);
        count++;
    }
    sqlite3_finalize(s);
    return count;
}

/* ── Collect target projects ─────────────────────────────────────── */

/* When target_projects = ["*"], scan the cache directory for all .db files. */
static int collect_all_projects(char ***out) {
    const char *dir = cr_cache_dir();
    cbm_dir_t *d = cbm_opendir(dir);
    if (!d) {
        *out = NULL;
        return 0;
    }

    int cap = CR_INIT_CAP;
    int count = 0;
    char **projects = malloc((size_t)cap * sizeof(char *));

    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        size_t len = strlen(ent->name);
        if (len < CR_COL_4 || strcmp(ent->name + len - CR_DB_EXT_LEN, ".db") != 0) {
            continue;
        }
        if (strstr(ent->name, "_cross_repo") || strstr(ent->name, "_config")) {
            continue;
        }
        if (strstr(ent->name, "-wal") || strstr(ent->name, "-shm")) {
            continue;
        }
        if (count >= cap) {
            cap *= PAIR_LEN;
            char **tmp = realloc(projects, (size_t)cap * sizeof(char *));
            if (!tmp) {
                break;
            }
            projects = tmp;
        }
        /* Strip .db extension */
        projects[count] = malloc(len - PAIR_LEN);
        memcpy(projects[count], ent->name, len - CR_DB_EXT_LEN);
        projects[count][len - CR_DB_EXT_LEN] = '\0';
        count++;
    }
    cbm_closedir(d);

    *out = projects;
    return count;
}

static void free_project_list(char **projects, int count) {
    for (int i = 0; i < count; i++) {
        free(projects[i]);
    }
    free(projects);
}

/* ── Entry point ─────────────────────────────────────────────────── */

cbm_cross_repo_result_t cbm_cross_repo_match(const char *project, const char **target_projects,
                                             int target_count) {
    cbm_cross_repo_result_t result = {0};
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    /* Open source project store (read-write) */
    char src_path[CR_PATH_BUF];
    cr_db_path(project, src_path, sizeof(src_path));
    cbm_store_t *src_store = cbm_store_open_path(src_path);
    if (!src_store) {
        return result;
    }

    /* Clean existing CROSS_* edges for this project */
    delete_cross_edges(src_store, project);

    /* Resolve target projects */
    char **resolved = NULL;
    int resolved_count = 0;
    bool own_list = false;

    if (target_count == SKIP_ONE && strcmp(target_projects[0], "*") == 0) {
        resolved_count = collect_all_projects(&resolved);
        own_list = true;
    } else {
        resolved = (char **)target_projects;
        resolved_count = target_count;
    }

    /* Match against each target */
    for (int i = 0; i < resolved_count; i++) {
        const char *tgt = resolved[i];
        if (strcmp(tgt, project) == 0) {
            continue; /* skip self */
        }

        char tgt_path[CR_PATH_BUF];
        cr_db_path(tgt, tgt_path, sizeof(tgt_path));

        /* Open target store read-write (for bidirectional edge writes) */
        cbm_store_t *tgt_store = cbm_store_open_path(tgt_path);
        if (!tgt_store) {
            continue;
        }

        result.http_edges += match_http_routes(src_store, project, tgt_store, tgt);
        result.async_edges += match_async_routes(src_store, project, tgt_store, tgt);
        result.channel_edges += match_channels(src_store, project, tgt_store, tgt);
        result.grpc_edges += match_typed_routes(src_store, project, tgt_store, tgt, "GRPC_CALLS",
                                                "service", "method", "CROSS_GRPC_CALLS");
        result.graphql_edges +=
            match_typed_routes(src_store, project, tgt_store, tgt, "GRAPHQL_CALLS", "operation",
                               "operation", "CROSS_GRAPHQL_CALLS");
        result.trpc_edges += match_typed_routes(src_store, project, tgt_store, tgt, "TRPC_CALLS",
                                                "procedure", "procedure", "CROSS_TRPC_CALLS");
        result.projects_scanned++;

        cbm_store_close(tgt_store);
    }

    cbm_store_close(src_store);

    if (own_list) {
        free_project_list(resolved, resolved_count);
    }

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    result.elapsed_ms = ((double)(t1.tv_sec - t0.tv_sec) * CR_MS_PER_SEC) +
                        ((double)(t1.tv_nsec - t0.tv_nsec) / CR_NS_PER_MS);

    int total = result.http_edges + result.async_edges + result.channel_edges + result.grpc_edges +
                result.graphql_edges + result.trpc_edges;
    cbm_log_info("cross_repo.done", "project", project, "total", cr_itoa(total));

    return result;
}
