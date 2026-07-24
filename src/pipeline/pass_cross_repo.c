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
#include "pipeline/pipeline_internal.h" // cbm_route_canon_path
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/str_util.h"

#include <sqlite3/sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* ── Constants ───────────────────────────────────────────────────── */

enum {
    CR_PATH_BUF = 4096,
    CR_QN_BUF = 512,
    CR_PROPS_BUF = 2048,
    CR_MAX_EDGES = 4096,
    CR_DB_EXT_LEN = 3, /* strlen(".db") */
    CR_INIT_CAP = 32,
    CR_MAX_PROJECTS = 4096,
    CR_MAX_CACHE_ENTRIES = 16384,
    CR_COL_3 = 3,
    CR_COL_4 = 4,
    CR_SCHEME_SKIP = 3,      /* strlen("://") */
    CR_ROUTE_PREFIX_LEN = 9, /* strlen("__route__") */
    CR_ANY_LEN = 3,          /* strlen("ANY") */
};

#define CR_MS_PER_SEC 1000.0
#define CR_NS_PER_MS 1000000.0

typedef enum {
    CR_RUN_OK = 0,
    CR_RUN_FAILED,
    CR_RUN_CANCELLED,
} cr_run_status_t;

typedef struct {
    const atomic_int *cancelled;
    bool cancellation_observed;
    bool mutated;
} cr_run_context_t;

static CBM_TLS cbm_cross_repo_after_insert_test_hook_t cr_after_insert_test_hook = NULL;
static CBM_TLS void *cr_after_insert_test_context = NULL;

void cbm_cross_repo_set_after_insert_hook_for_tests(cbm_cross_repo_after_insert_test_hook_t hook,
                                                    void *context) {
    cr_after_insert_test_hook = hook;
    cr_after_insert_test_context = context;
}

typedef struct {
    int count;
    cr_run_status_t status;
} cr_match_result_t;

static bool cr_cancel_requested(cr_run_context_t *ctx) {
    if (!ctx) {
        return false;
    }
    if (!ctx->cancellation_observed && ctx->cancelled &&
        atomic_load_explicit(ctx->cancelled, memory_order_acquire) != 0) {
        ctx->cancellation_observed = true;
    }
    return ctx->cancellation_observed;
}

static cr_match_result_t cr_match_finish(cr_run_context_t *ctx, int count, bool failed) {
    cr_match_result_t result = {
        .count = count,
        .status =
            failed ? CR_RUN_FAILED : (cr_cancel_requested(ctx) ? CR_RUN_CANCELLED : CR_RUN_OK),
    };
    return result;
}

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

static bool cr_db_path(const char *project, char *buf, size_t bufsz) {
    if (!project || !cbm_validate_project_name(project)) {
        return false;
    }
    int written = snprintf(buf, bufsz, "%s/%s.db", cr_cache_dir(), project);
    return written > 0 && (size_t)written < bufsz;
}

static int cr_project_compare(const void *left, const void *right) {
    const char *const *left_project = left;
    const char *const *right_project = right;
    return strcmp(*left_project, *right_project);
}

static bool cr_store_has_exact_project(cbm_store_t *store, const char *project) {
    cbm_project_t *projects = NULL;
    int count = 0;
    bool matches = store && cbm_store_check_integrity(store) &&
                   cbm_store_list_projects(store, &projects, &count) == CBM_STORE_OK &&
                   count == 1 && projects[0].name && strcmp(projects[0].name, project) == 0;
    cbm_store_free_projects(projects, count);
    return matches;
}

static bool cr_project_exists(const char *project) {
    char path[CR_PATH_BUF];
    if (!cr_db_path(project, path, sizeof(path))) {
        return false;
    }
    cbm_store_t *store = cbm_store_open_path_query(path);
    bool exists = cr_store_has_exact_project(store, project);
    cbm_store_close(store);
    return exists;
}

static cbm_store_t *cr_open_existing_project(const char *project) {
    char path[CR_PATH_BUF];
    if (!cr_db_path(project, path, sizeof(path))) {
        return NULL;
    }
    cbm_store_t *store = cbm_store_open_path_existing(path);
    if (!cr_store_has_exact_project(store, project)) {
        cbm_store_close(store);
        return NULL;
    }
    return store;
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

/* Delete all CROSS_* edges for a project from a store. Cancellation is checked
 * before every destructive statement so a pre-cancelled request never erases
 * the previous generation. */
static cr_run_status_t delete_cross_edges(cbm_store_t *store, const char *project,
                                          cr_run_context_t *ctx) {
    static const char *const edge_types[] = {
        "CROSS_HTTP_CALLS", "CROSS_ASYNC_CALLS",   "CROSS_CHANNEL",
        "CROSS_GRPC_CALLS", "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS",
    };
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return CR_RUN_FAILED;
    }
    for (size_t i = 0; i < sizeof(edge_types) / sizeof(edge_types[0]); i++) {
        if (cr_cancel_requested(ctx)) {
            return CR_RUN_CANCELLED;
        }
        int changes_before = sqlite3_total_changes(db);
        if (cbm_store_delete_edges_by_type(store, project, edge_types[i]) != CBM_STORE_OK) {
            return CR_RUN_FAILED;
        }
        if (sqlite3_total_changes(db) > changes_before) {
            ctx->mutated = true;
        }
    }
    return cr_cancel_requested(ctx) ? CR_RUN_CANCELLED : CR_RUN_OK;
}

/* Insert a CROSS_* edge into a store. Idempotent by construction: the edges
 * table is UNIQUE(source_id, target_id, type) and cbm_store_insert_edge
 * upserts on conflict, so a pair reached from both match directions or on a
 * repeat run never duplicates a row. (#523) */
static bool insert_cross_edge(cbm_store_t *store, const char *project, int64_t from_id,
                              int64_t to_id, const char *edge_type, const char *props,
                              cr_run_context_t *ctx) {
    if (cr_cancel_requested(ctx)) {
        return false;
    }
    cbm_edge_t edge = {
        .project = project,
        .source_id = from_id,
        .target_id = to_id,
        .type = edge_type,
        .properties_json = props,
    };
    bool inserted = cbm_store_insert_edge(store, &edge) > 0;
    if (inserted) {
        ctx->mutated = true;
        if (cr_after_insert_test_hook) {
            cr_after_insert_test_hook(project, edge_type, cr_after_insert_test_context);
        }
    }
    return inserted;
}

/* Strip "scheme://host[:port]" from a stored HTTP_CALLS url, returning the
 * path. url_path property values are stored raw from the call's first string
 * argument, so they can be full URLs ("scheme://host:port/v2/x") — and
 * cbm_route_canon_path only canonicalizes placeholder syntax, never strips
 * authorities. Returns "/" for a URL with no path after the host (a request
 * against the bare base URL targets the root route). (#523) */
static const char *cr_url_path(const char *url) {
    if (!url) {
        return url;
    }
    const char *scheme_end = strstr(url, "://");
    if (!scheme_end) {
        return url; /* already a bare path */
    }
    const char *path_start = strchr(scheme_end + CR_SCHEME_SKIP, '/');
    return path_start ? path_start : "/";
}

/* Look up a node's name and file_path by id. */
static bool lookup_node_info(struct sqlite3 *db, int64_t node_id, char *name_out, size_t name_sz,
                             char *file_out, size_t file_sz) {
    name_out[0] = '\0';
    file_out[0] = '\0';
    if (!db) {
        return false;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT name, file_path FROM nodes WHERE id = ?1", CBM_NOT_FOUND,
                           &st, NULL) != SQLITE_OK) {
        return false;
    }
    if (sqlite3_bind_int64(st, SKIP_ONE, node_id) != SQLITE_OK) {
        sqlite3_finalize(st);
        return false;
    }
    int step_rc = sqlite3_step(st);
    if (step_rc == SQLITE_ROW) {
        const char *nm = (const char *)sqlite3_column_text(st, 0);
        const char *fp = (const char *)sqlite3_column_text(st, SKIP_ONE);
        if (nm) {
            snprintf(name_out, name_sz, "%s", nm);
        }
        if (fp) {
            snprintf(file_out, file_sz, "%s", fp);
        }
    }
    int finalize_rc = sqlite3_finalize(st);
    return (step_rc == SQLITE_ROW || step_rc == SQLITE_DONE) && finalize_rc == SQLITE_OK;
}

/* ── Phase A: HTTP Route matching ────────────────────────────────── */

/* Find a Route node in target_store by QN and return the handler function's
 * node id, name, and file_path via HANDLES edges. Returns 0 if not found. */
static int64_t find_route_handler(cbm_store_t *target_store, const char *route_qn,
                                  char *handler_name, size_t name_sz, char *handler_file,
                                  size_t file_sz, bool *failed) {
    *failed = false;
    handler_name[0] = '\0';
    handler_file[0] = '\0';
    struct sqlite3 *db = cbm_store_get_db(target_store);
    if (!db) {
        *failed = true;
        return 0;
    }

    /* Find Route node by QN */
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(
            db, "SELECT id FROM nodes WHERE qualified_name = ?1 AND label = 'Route' LIMIT 1",
            CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        *failed = true;
        return 0;
    }
    if (sqlite3_bind_text(s, SKIP_ONE, route_qn, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(s);
        *failed = true;
        return 0;
    }
    int64_t route_id = 0;
    int step_rc = sqlite3_step(s);
    if (step_rc == SQLITE_ROW) {
        route_id = sqlite3_column_int64(s, 0);
    } else if (step_rc != SQLITE_DONE) {
        *failed = true;
    }
    if (sqlite3_finalize(s) != SQLITE_OK) {
        *failed = true;
    }
    if (*failed || route_id == 0) {
        return 0;
    }

    /* Follow HANDLES edge to find the handler function */
    if (sqlite3_prepare_v2(db,
                           "SELECT n.id, n.name, n.file_path FROM edges e "
                           "JOIN nodes n ON n.id = e.source_id "
                           "WHERE e.target_id = ?1 AND e.type = 'HANDLES' LIMIT 1",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        *failed = true;
        return 0;
    }
    if (sqlite3_bind_int64(s, SKIP_ONE, route_id) != SQLITE_OK) {
        sqlite3_finalize(s);
        *failed = true;
        return 0;
    }
    int64_t handler_id = 0;
    step_rc = sqlite3_step(s);
    if (step_rc == SQLITE_ROW) {
        handler_id = sqlite3_column_int64(s, 0);
        const char *n = (const char *)sqlite3_column_text(s, SKIP_ONE);
        const char *f = (const char *)sqlite3_column_text(s, PAIR_LEN);
        if (n) {
            snprintf(handler_name, name_sz, "%s", n);
        }
        if (f) {
            snprintf(handler_file, file_sz, "%s", f);
        }
    } else if (step_rc != SQLITE_DONE) {
        *failed = true;
    }
    if (sqlite3_finalize(s) != SQLITE_OK) {
        *failed = true;
    }
    return handler_id;
}

/* Segment-wise match of a concrete path against a route template path, where a
 * "{...}" segment in the template matches any single non-empty concrete
 * segment. Both inputs are bare paths (no method prefix, no authority).
 * Leading/trailing slashes are insignificant. Returns true on a full match. */
static bool cr_path_matches_template(const char *concrete, const char *templ) {
    const char *c = concrete;
    const char *t = templ;
    while (*c && *t) {
        if (*c == '/') {
            c++;
        }
        if (*t == '/') {
            t++;
        }
        const char *cseg = c;
        while (*c && *c != '/') {
            c++;
        }
        const char *tseg = t;
        while (*t && *t != '/') {
            t++;
        }
        size_t clen = (size_t)(c - cseg);
        size_t tlen = (size_t)(t - tseg);
        bool t_is_param = (tlen >= PAIR_LEN && tseg[0] == '{' && tseg[tlen - 1] == '}');
        if (!t_is_param) {
            if (clen != tlen || strncmp(cseg, tseg, clen) != 0) {
                return false;
            }
        } else if (clen == 0) {
            return false; /* a parameter never matches an empty segment */
        }
    }
    while (*c == '/') {
        c++;
    }
    while (*t == '/') {
        t++;
    }
    return *c == '\0' && *t == '\0';
}

/* Fallback for when the exact route-QN lookup misses: a concrete client path
 * ("/v2/orders/123") never exact-matches a templated route QN
 * ("__route__GET__/v2/orders/{}"). Enumerate the target store's Route nodes
 * and segment-match the concrete path against each template. On a match, copy
 * the route QN into out_qn and return the handler id; returns 0 on no match.
 *
 * COST: this scans every Route node of the target project once per unmatched
 * HTTP_CALLS edge — O(calls × routes) per project pair. Acceptable while both
 * factors stay small (calls are capped at CR_MAX_EDGES and it only runs for
 * edges the exact lookup missed); revisit with a prepared template index if
 * cross-repo matching ever shows up in profiles. (#523) */
static int64_t find_route_handler_fuzzy(cbm_store_t *target_store, const char *concrete_path,
                                        const char *method, char *out_qn, size_t out_qn_sz,
                                        char *handler_name, size_t name_sz, char *handler_file,
                                        size_t file_sz, bool *failed, cr_run_context_t *ctx) {
    *failed = false;
    struct sqlite3 *db = cbm_store_get_db(target_store);
    if (!db || !concrete_path || !concrete_path[0]) {
        if (!db) {
            *failed = true;
        }
        return 0;
    }
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(db, "SELECT qualified_name FROM nodes WHERE label = 'Route' ORDER BY id",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        *failed = true;
        return 0;
    }
    int64_t found = 0;
    int scanned = 0;
    int step_rc = SQLITE_DONE;
    while (scanned < CR_MAX_EDGES && !cr_cancel_requested(ctx) &&
           (step_rc = sqlite3_step(s)) == SQLITE_ROW) {
        scanned++;
        const char *qn = (const char *)sqlite3_column_text(s, 0);
        if (!qn || strncmp(qn, "__route__", CR_ROUTE_PREFIX_LEN) != 0) {
            continue;
        }
        /* Split "__route__<METHOD>__<path>" */
        const char *rest = qn + CR_ROUTE_PREFIX_LEN;
        const char *sep = strstr(rest, "__");
        if (!sep) {
            continue;
        }
        size_t mlen = (size_t)(sep - rest);
        const char *rpath = sep + PAIR_LEN;
        /* Method gate: the route's method must equal the caller's, or be ANY.
         * A missing caller method matches any route method. */
        if (method && method[0]) {
            bool same_method = (strncmp(rest, method, mlen) == 0 && method[mlen] == '\0');
            bool route_any = (mlen == CR_ANY_LEN && strncmp(rest, "ANY", CR_ANY_LEN) == 0);
            if (!same_method && !route_any) {
                continue;
            }
        }
        if (!cr_path_matches_template(concrete_path, rpath)) {
            continue;
        }
        /* A concrete path can match more than one stored template (e.g. a raw
         * "{id}" variant and its canonical "{}" form). Only accept a Route that
         * actually has a HANDLES edge — the handler is attached to the
         * canonical node. Keep scanning otherwise. */
        int64_t hid = find_route_handler(target_store, qn, handler_name, name_sz, handler_file,
                                         file_sz, failed);
        if (*failed) {
            step_rc = SQLITE_DONE;
            break;
        }
        if (hid != 0) {
            snprintf(out_qn, out_qn_sz, "%s", qn);
            found = hid;
            step_rc = SQLITE_DONE;
            break;
        }
    }
    if (!cr_cancel_requested(ctx) && scanned < CR_MAX_EDGES && step_rc != SQLITE_DONE) {
        *failed = true;
    }
    if (sqlite3_finalize(s) != SQLITE_OK) {
        *failed = true;
    }
    return found;
}

/* Emit CROSS_* edge for a route match: forward into source, reverse into target.
 * A pair is successful only when both writes complete. */
static bool emit_cross_route_bidirectional(
    cbm_store_t *src_store, const char *src_project, struct sqlite3 *src_db, int64_t caller_id,
    int64_t local_route_id, cbm_store_t *tgt_store, const char *tgt_project, int64_t handler_id,
    const char *route_qn, const char *handler_name, const char *handler_file, const char *url_path,
    const char *method, const char *edge_type, cr_run_context_t *ctx) {
    /* Forward: caller → local Route in source DB */
    char fwd[CR_PROPS_BUF];
    build_cross_props(fwd, sizeof(fwd), tgt_project, handler_name, handler_file, url_path,
                      "url_path", method);
    if (!insert_cross_edge(src_store, src_project, caller_id, local_route_id, edge_type, fwd,
                           ctx)) {
        return false;
    }

    if (cr_cancel_requested(ctx)) {
        return false;
    }

    /* Reverse: handler → Route in target DB */
    struct sqlite3 *tgt_db = cbm_store_get_db(tgt_store);
    if (!tgt_db) {
        return false;
    }
    sqlite3_stmt *rq = NULL;
    if (sqlite3_prepare_v2(tgt_db, "SELECT id FROM nodes WHERE qualified_name = ?1 LIMIT 1",
                           CBM_NOT_FOUND, &rq, NULL) != SQLITE_OK) {
        return false;
    }
    if (sqlite3_bind_text(rq, SKIP_ONE, route_qn, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(rq);
        return false;
    }
    int64_t tgt_route_id = 0;
    int step_rc = sqlite3_step(rq);
    if (step_rc == SQLITE_ROW) {
        tgt_route_id = sqlite3_column_int64(rq, 0);
    }
    int finalize_rc = sqlite3_finalize(rq);
    if ((step_rc != SQLITE_ROW && step_rc != SQLITE_DONE) || finalize_rc != SQLITE_OK ||
        tgt_route_id == 0) {
        return false;
    }

    char caller_name[CBM_SZ_256] = {0};
    char caller_file[CBM_SZ_512] = {0};
    if (!lookup_node_info(src_db, caller_id, caller_name, sizeof(caller_name), caller_file,
                          sizeof(caller_file))) {
        return false;
    }

    char rev[CR_PROPS_BUF];
    build_cross_props(rev, sizeof(rev), src_project, caller_name, caller_file, url_path, "url_path",
                      method);
    return insert_cross_edge(tgt_store, tgt_project, handler_id, tgt_route_id, edge_type, rev, ctx);
}

static cr_match_result_t match_http_routes(cbm_store_t *src_store, const char *src_project,
                                           cbm_store_t *tgt_store, const char *tgt_project,
                                           cr_run_context_t *ctx) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return cr_match_finish(ctx, 0, true);
    }

    /* Find all HTTP_CALLS edges in source project */
    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db,
                           "SELECT e.source_id, e.target_id, e.properties FROM edges e "
                           "WHERE e.project = ?1 AND e.type = 'HTTP_CALLS' ORDER BY e.id",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return cr_match_finish(ctx, 0, true);
    }
    if (sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(s);
        return cr_match_finish(ctx, 0, true);
    }

    int count = 0;
    int scanned = 0;
    int step_rc = SQLITE_DONE;
    bool failed = false;
    while (scanned < CR_MAX_EDGES && !cr_cancel_requested(ctx) &&
           (step_rc = sqlite3_step(s)) == SQLITE_ROW) {
        scanned++;
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

        /* Build the expected Route QN in the target project (authority-stripped
         * and param-canonicalized so client url_path matches the server handler
         * regardless of base URL and framework placeholder syntax). */
        char route_qn[CR_QN_BUF];
        char cpath[CBM_SZ_256];
        const char *curl = cbm_route_canon_path(cr_url_path(url_path), cpath, sizeof(cpath));
        snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", method[0] ? method : "ANY", curl);

        char handler_name[CBM_SZ_256] = {0};
        char handler_file[CBM_SZ_512] = {0};
        bool query_failed = false;
        int64_t handler_id =
            find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                               handler_file, sizeof(handler_file), &query_failed);
        if (!query_failed && handler_id == 0) {
            /* Try without method (ANY) */
            snprintf(route_qn, sizeof(route_qn), "__route__ANY__%s", curl);
            handler_id = find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                                            handler_file, sizeof(handler_file), &query_failed);
        }
        if (!query_failed && handler_id == 0) {
            /* Exact QN lookup missed. A concrete client path ("/v2/orders/123")
             * never exact-matches a templated route ("/v2/orders/{}"), so fall
             * back to segment-wise template matching. (#523) */
            handler_id =
                find_route_handler_fuzzy(tgt_store, curl, method[0] ? method : NULL, route_qn,
                                         sizeof(route_qn), handler_name, sizeof(handler_name),
                                         handler_file, sizeof(handler_file), &query_failed, ctx);
        }
        if (query_failed) {
            failed = true;
            break;
        }
        if (handler_id == 0) {
            continue;
        }

        if (cr_cancel_requested(ctx)) {
            break;
        }

        if (!emit_cross_route_bidirectional(src_store, src_project, src_db, caller_id, route_id,
                                            tgt_store, tgt_project, handler_id, route_qn,
                                            handler_name, handler_file, url_path, method,
                                            "CROSS_HTTP_CALLS", ctx)) {
            failed = !cr_cancel_requested(ctx);
            break;
        }

        count++;
    }
    if (!failed && !cr_cancel_requested(ctx) && scanned < CR_MAX_EDGES && step_rc != SQLITE_DONE) {
        failed = true;
    }
    if (sqlite3_finalize(s) != SQLITE_OK) {
        failed = true;
    }
    return cr_match_finish(ctx, count, failed);
}

/* ── Phase B: Async matching ─────────────────────────────────────── */

static cr_match_result_t match_async_routes(cbm_store_t *src_store, const char *src_project,
                                            cbm_store_t *tgt_store, const char *tgt_project,
                                            cr_run_context_t *ctx) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return cr_match_finish(ctx, 0, true);
    }

    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db,
                           "SELECT e.source_id, e.target_id, e.properties FROM edges e "
                           "WHERE e.project = ?1 AND e.type = 'ASYNC_CALLS' ORDER BY e.id",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return cr_match_finish(ctx, 0, true);
    }
    if (sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(s);
        return cr_match_finish(ctx, 0, true);
    }

    int count = 0;
    int scanned = 0;
    int step_rc = SQLITE_DONE;
    bool failed = false;
    while (scanned < CR_MAX_EDGES && !cr_cancel_requested(ctx) &&
           (step_rc = sqlite3_step(s)) == SQLITE_ROW) {
        scanned++;
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
        bool query_failed = false;
        int64_t handler_id =
            find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                               handler_file, sizeof(handler_file), &query_failed);
        if (query_failed) {
            failed = true;
            break;
        }
        if (handler_id == 0) {
            continue;
        }

        if (cr_cancel_requested(ctx)) {
            break;
        }

        char edge_props[CR_PROPS_BUF];
        build_cross_props(edge_props, sizeof(edge_props), tgt_project, handler_name, handler_file,
                          url_path, "url_path", broker);
        if (!insert_cross_edge(src_store, src_project, caller_id, route_id, "CROSS_ASYNC_CALLS",
                               edge_props, ctx)) {
            failed = !cr_cancel_requested(ctx);
            break;
        }
        count++;
    }
    if (!failed && !cr_cancel_requested(ctx) && scanned < CR_MAX_EDGES && step_rc != SQLITE_DONE) {
        failed = true;
    }
    if (sqlite3_finalize(s) != SQLITE_OK) {
        failed = true;
    }
    return cr_match_finish(ctx, count, failed);
}

/* ── Phase C: Channel matching ───────────────────────────────────── */

/* Try to find a matching listener in target DB for a channel name. Returns 1
 * for a complete bidirectional pair, 0 for no match, and CBM_STORE_ERR when a
 * query or write fails. */
static int try_match_channel_listener(cbm_store_t *src_store, const char *src_project,
                                      cbm_store_t *tgt_store, const char *tgt_project,
                                      const char *channel_name, const char *transport,
                                      int64_t emitter_id, int64_t channel_id,
                                      cr_run_context_t *ctx) {
    if (cr_cancel_requested(ctx)) {
        return CBM_STORE_NOT_FOUND;
    }
    struct sqlite3 *tgt_db = cbm_store_get_db(tgt_store);
    if (!tgt_db) {
        return CBM_STORE_ERR;
    }
    sqlite3_stmt *tq = NULL;
    if (sqlite3_prepare_v2(tgt_db,
                           "SELECT n.id, e.source_id, fn.name, fn.file_path FROM nodes n "
                           "JOIN edges e ON e.target_id = n.id AND e.type = 'LISTENS_ON' "
                           "JOIN nodes fn ON fn.id = e.source_id "
                           "WHERE n.project = ?1 AND n.name = ?2 AND n.label = 'Channel' LIMIT 1",
                           CBM_NOT_FOUND, &tq, NULL) != SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    if (sqlite3_bind_text(tq, SKIP_ONE, tgt_project, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK ||
        sqlite3_bind_text(tq, PAIR_LEN, channel_name, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(tq);
        return CBM_STORE_ERR;
    }

    int64_t tgt_channel_id = 0;
    int64_t listener_id = 0;
    char listener_name[CBM_SZ_256] = {0};
    char listener_file[CBM_SZ_512] = {0};
    int step_rc = sqlite3_step(tq);
    if (step_rc == SQLITE_ROW) {
        tgt_channel_id = sqlite3_column_int64(tq, 0);
        listener_id = sqlite3_column_int64(tq, SKIP_ONE);
        const char *name = (const char *)sqlite3_column_text(tq, PAIR_LEN);
        const char *file = (const char *)sqlite3_column_text(tq, CR_COL_3);
        snprintf(listener_name, sizeof(listener_name), "%s", name ? name : "");
        snprintf(listener_file, sizeof(listener_file), "%s", file ? file : "");
    }
    int finalize_rc = sqlite3_finalize(tq);
    if ((step_rc != SQLITE_ROW && step_rc != SQLITE_DONE) || finalize_rc != SQLITE_OK) {
        return CBM_STORE_ERR;
    }
    if (step_rc == SQLITE_DONE) {
        return 0;
    }

    char caller_name[CBM_SZ_256] = {0};
    char caller_file[CBM_SZ_512] = {0};
    if (!lookup_node_info(cbm_store_get_db(src_store), emitter_id, caller_name, sizeof(caller_name),
                          caller_file, sizeof(caller_file))) {
        return CBM_STORE_ERR;
    }

    /* Forward edge: emitter → local Channel */
    char fwd[CR_PROPS_BUF];
    build_cross_props(fwd, sizeof(fwd), tgt_project, listener_name, listener_file, channel_name,
                      "channel_name", transport);
    if (!insert_cross_edge(src_store, src_project, emitter_id, channel_id, "CROSS_CHANNEL", fwd,
                           ctx)) {
        return cr_cancel_requested(ctx) ? CBM_STORE_NOT_FOUND : CBM_STORE_ERR;
    }
    if (cr_cancel_requested(ctx)) {
        return CBM_STORE_NOT_FOUND;
    }

    /* Reverse edge: listener → target Channel */
    char rev[CR_PROPS_BUF];
    build_cross_props(rev, sizeof(rev), src_project, caller_name, caller_file, channel_name,
                      "channel_name", transport);
    return insert_cross_edge(tgt_store, tgt_project, listener_id, tgt_channel_id, "CROSS_CHANNEL",
                             rev, ctx)
               ? 1
               : (cr_cancel_requested(ctx) ? CBM_STORE_NOT_FOUND : CBM_STORE_ERR);
}

static cr_match_result_t match_channels(cbm_store_t *src_store, const char *src_project,
                                        cbm_store_t *tgt_store, const char *tgt_project,
                                        cr_run_context_t *ctx) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return cr_match_finish(ctx, 0, true);
    }

    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db,
                           "SELECT DISTINCT n.id, n.name, n.qualified_name, n.properties, "
                           "e.source_id FROM nodes n "
                           "JOIN edges e ON e.target_id = n.id AND e.type = 'EMITS' "
                           "WHERE n.project = ?1 AND n.label = 'Channel' "
                           "ORDER BY n.id, e.source_id",
                           CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return cr_match_finish(ctx, 0, true);
    }
    if (sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(s);
        return cr_match_finish(ctx, 0, true);
    }

    int count = 0;
    int scanned = 0;
    int step_rc = SQLITE_DONE;
    bool failed = false;
    while (scanned < CR_MAX_EDGES && !cr_cancel_requested(ctx) &&
           (step_rc = sqlite3_step(s)) == SQLITE_ROW) {
        scanned++;
        const char *channel_name = (const char *)sqlite3_column_text(s, SKIP_ONE);
        const char *channel_qn = (const char *)sqlite3_column_text(s, PAIR_LEN);
        if (!channel_name || !channel_qn) {
            continue;
        }
        int64_t channel_id = sqlite3_column_int64(s, 0);
        const char *channel_props = (const char *)sqlite3_column_text(s, CR_COL_3);
        int64_t emitter_id = sqlite3_column_int64(s, CR_COL_4);

        char *channel_name_copy = cbm_strdup(channel_name);
        if (!channel_name_copy) {
            failed = true;
            break;
        }
        char transport[CBM_SZ_64] = {0};
        json_str_prop(channel_props, "transport", transport, sizeof(transport));

        int matched =
            try_match_channel_listener(src_store, src_project, tgt_store, tgt_project,
                                       channel_name_copy, transport, emitter_id, channel_id, ctx);
        free(channel_name_copy);
        if (matched == CBM_STORE_ERR) {
            failed = true;
            break;
        }
        if (matched == CBM_STORE_NOT_FOUND && cr_cancel_requested(ctx)) {
            break;
        }
        if (matched > 0) {
            count++;
        }
    }
    if (!failed && !cr_cancel_requested(ctx) && scanned < CR_MAX_EDGES && step_rc != SQLITE_DONE) {
        failed = true;
    }
    if (sqlite3_finalize(s) != SQLITE_OK) {
        failed = true;
    }
    return cr_match_finish(ctx, count, failed);
}

/* ── Phase D: Generic route-type matcher (gRPC, GraphQL, tRPC) ──── */

/* Look up a node's qualified_name by id. Returns true if found and reports SQL
 * failures separately from an ordinary miss. */
static bool lookup_node_qn(struct sqlite3 *db, int64_t node_id, char *out, size_t out_sz,
                           bool *failed) {
    *failed = false;
    out[0] = '\0';
    if (!db) {
        *failed = true;
        return false;
    }
    sqlite3_stmt *st = NULL;
    if (sqlite3_prepare_v2(db, "SELECT qualified_name FROM nodes WHERE id = ?1", CBM_NOT_FOUND, &st,
                           NULL) != SQLITE_OK) {
        *failed = true;
        return false;
    }
    if (sqlite3_bind_int64(st, SKIP_ONE, node_id) != SQLITE_OK) {
        sqlite3_finalize(st);
        *failed = true;
        return false;
    }
    bool found = false;
    int step_rc = sqlite3_step(st);
    if (step_rc == SQLITE_ROW) {
        const char *qn = (const char *)sqlite3_column_text(st, 0);
        if (qn) {
            snprintf(out, out_sz, "%s", qn);
            found = true;
        }
    } else if (step_rc != SQLITE_DONE) {
        *failed = true;
    }
    if (sqlite3_finalize(st) != SQLITE_OK) {
        *failed = true;
    }
    return found;
}

/* Match edges of a given type against Route nodes with a given QN prefix.
 * Reuses the same infrastructure as HTTP/async matching. */
static cr_match_result_t match_typed_routes(cbm_store_t *src_store, const char *src_project,
                                            cbm_store_t *tgt_store, const char *tgt_project,
                                            const char *edge_type, const char *svc_key,
                                            const char *method_key, const char *cross_edge_type,
                                            cr_run_context_t *ctx) {
    struct sqlite3 *src_db = cbm_store_get_db(src_store);
    if (!src_db) {
        return cr_match_finish(ctx, 0, true);
    }

    char sql[CBM_SZ_256];
    snprintf(sql, sizeof(sql),
             "SELECT e.source_id, e.target_id, e.properties FROM edges e "
             "WHERE e.project = ?1 AND e.type = '%s' ORDER BY e.id",
             edge_type);

    sqlite3_stmt *s = NULL;
    if (sqlite3_prepare_v2(src_db, sql, CBM_NOT_FOUND, &s, NULL) != SQLITE_OK) {
        return cr_match_finish(ctx, 0, true);
    }
    if (sqlite3_bind_text(s, SKIP_ONE, src_project, CBM_NOT_FOUND, SQLITE_STATIC) != SQLITE_OK) {
        sqlite3_finalize(s);
        return cr_match_finish(ctx, 0, true);
    }

    int count = 0;
    int scanned = 0;
    int step_rc = SQLITE_DONE;
    bool failed = false;
    while (scanned < CR_MAX_EDGES && !cr_cancel_requested(ctx) &&
           (step_rc = sqlite3_step(s)) == SQLITE_ROW) {
        scanned++;
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
        bool query_failed = false;
        if (!lookup_node_qn(src_db, route_id, route_qn, sizeof(route_qn), &query_failed)) {
            if (query_failed) {
                failed = true;
                break;
            }
            continue;
        }

        char handler_name[CBM_SZ_256] = {0};
        char handler_file[CBM_SZ_512] = {0};
        int64_t handler_id =
            find_route_handler(tgt_store, route_qn, handler_name, sizeof(handler_name),
                               handler_file, sizeof(handler_file), &query_failed);
        if (query_failed) {
            failed = true;
            break;
        }
        if (handler_id == 0) {
            continue;
        }

        if (cr_cancel_requested(ctx)) {
            break;
        }

        if (!emit_cross_route_bidirectional(src_store, src_project, src_db, caller_id, route_id,
                                            tgt_store, tgt_project, handler_id, route_qn,
                                            handler_name, handler_file, svc_val, svc_key,
                                            cross_edge_type, ctx)) {
            failed = !cr_cancel_requested(ctx);
            break;
        }
        count++;
    }
    if (!failed && !cr_cancel_requested(ctx) && scanned < CR_MAX_EDGES && step_rc != SQLITE_DONE) {
        failed = true;
    }
    if (sqlite3_finalize(s) != SQLITE_OK) {
        failed = true;
    }
    return cr_match_finish(ctx, count, failed);
}

/* ── Collect target projects ─────────────────────────────────────── */

static void free_project_list(char **projects, int count);

/* When target_projects = ["*"], scan the cache directory for all .db files. */
static int collect_all_projects(char ***out, cr_run_context_t *ctx) {
    *out = NULL;
    const char *dir = cr_cache_dir();
    cbm_dir_t *d = cbm_opendir(dir);
    if (!d) {
        return -1;
    }

    int cap = CR_INIT_CAP;
    int count = 0;
    char **projects = malloc((size_t)cap * sizeof(char *));
    if (!projects) {
        cbm_closedir(d);
        return -1;
    }

    bool failed = false;
    int entries_scanned = 0;

    cbm_dirent_t *ent;
    while ((ent = cbm_readdir(d)) != NULL) {
        if (cr_cancel_requested(ctx)) {
            failed = true;
            break;
        }
        entries_scanned++;
        if (entries_scanned > CR_MAX_CACHE_ENTRIES) {
            failed = true;
            break;
        }
        size_t len = strlen(ent->name);
        if (len < CR_COL_4 || strcmp(ent->name + len - CR_DB_EXT_LEN, ".db") != 0) {
            continue;
        }
        /* Internal stores are exact filenames. Substring filtering would hide
         * legitimate projects such as orders_config_service or api-wal. */
        if (strcmp(ent->name, "_cross_repo.db") == 0 || strcmp(ent->name, "_config.db") == 0) {
            continue;
        }
        if (count >= CR_MAX_PROJECTS) {
            failed = true;
            break;
        }
        char project[CBM_DIRENT_NAME_MAX];
        size_t project_length = len - CR_DB_EXT_LEN;
        if (project_length == 0 || project_length >= sizeof(project)) {
            continue;
        }
        memcpy(project, ent->name, project_length);
        project[project_length] = '\0';
        if (!cbm_validate_project_name(project) || !cr_project_exists(project)) {
            continue;
        }
        if (count >= cap) {
            cap *= PAIR_LEN;
            char **tmp = realloc(projects, (size_t)cap * sizeof(char *));
            if (!tmp) {
                failed = true;
                break;
            }
            projects = tmp;
        }
        projects[count] = cbm_strdup(project);
        if (!projects[count]) {
            failed = true;
            break;
        }
        count++;
    }
    cbm_closedir(d);

    if (failed) {
        free_project_list(projects, count);
        return cr_cancel_requested(ctx) ? CBM_STORE_NOT_FOUND : CBM_STORE_ERR;
    }
    qsort(projects, (size_t)count, sizeof(*projects), cr_project_compare);
    int unique_count = 0;
    for (int i = 0; i < count; i++) {
        if (unique_count == 0 || strcmp(projects[i], projects[unique_count - 1]) != 0) {
            projects[unique_count++] = projects[i];
        } else {
            free(projects[i]);
        }
    }
    *out = projects;
    return unique_count;
}

static void free_project_list(char **projects, int count) {
    for (int i = 0; i < count; i++) {
        free(projects[i]);
    }
    free(projects);
}

static int collect_named_projects(const char **targets, int target_count, char ***out,
                                  cr_run_context_t *ctx) {
    *out = NULL;
    if (!targets || target_count <= 0 || target_count > CR_MAX_PROJECTS) {
        return -1;
    }
    char **projects = calloc((size_t)target_count, sizeof(*projects));
    if (!projects) {
        return -1;
    }
    int count = 0;
    for (int i = 0; i < target_count; i++) {
        if (cr_cancel_requested(ctx)) {
            free_project_list(projects, count);
            return CBM_STORE_NOT_FOUND;
        }
        if (!targets[i] || strcmp(targets[i], "*") == 0 || !cbm_validate_project_name(targets[i])) {
            free_project_list(projects, count);
            return -1;
        }
        projects[count] = cbm_strdup(targets[i]);
        if (!projects[count]) {
            free_project_list(projects, count);
            return -1;
        }
        count++;
    }
    qsort(projects, (size_t)count, sizeof(*projects), cr_project_compare);
    int unique_count = 0;
    for (int i = 0; i < count; i++) {
        if (unique_count == 0 || strcmp(projects[i], projects[unique_count - 1]) != 0) {
            projects[unique_count++] = projects[i];
        } else {
            free(projects[i]);
        }
    }
    *out = projects;
    return unique_count;
}

static cr_run_status_t add_match_count(int *total, cr_match_result_t matched) {
    *total += matched.count;
    return matched.status;
}

/* ── Entry point ─────────────────────────────────────────────────── */

cbm_cross_repo_result_t cbm_cross_repo_match_cancellable(const char *project,
                                                         const char **target_projects,
                                                         int target_count,
                                                         const atomic_int *cancelled) {
    cbm_cross_repo_result_t result = {0};
    cr_run_context_t run = {.cancelled = cancelled};
    struct timespec t0;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    if (!project || !cbm_validate_project_name(project) || !target_projects || target_count <= 0 ||
        (target_count == SKIP_ONE && !target_projects[0]) || !cr_project_exists(project)) {
        result.failed = true;
        return result;
    }
    if (cr_cancel_requested(&run)) {
        result.cancelled = true;
        return result;
    }

    /* Resolve target projects */
    char **resolved = NULL;
    int resolved_count = 0;

    if (target_count == SKIP_ONE && target_projects[0] && strcmp(target_projects[0], "*") == 0) {
        resolved_count = collect_all_projects(&resolved, &run);
    } else {
        resolved_count = collect_named_projects(target_projects, target_count, &resolved, &run);
    }
    if (resolved_count < 0) {
        result.cancelled = cr_cancel_requested(&run);
        result.failed = !result.cancelled;
        return result;
    }
    for (int i = 0; i < resolved_count; i++) {
        if (cr_cancel_requested(&run)) {
            result.cancelled = true;
            free_project_list(resolved, resolved_count);
            return result;
        }
        if (strcmp(resolved[i], project) != 0 && !cr_project_exists(resolved[i])) {
            result.failed = true;
            free_project_list(resolved, resolved_count);
            return result;
        }
    }

    /* Every input is known to exist before destructive source cleanup. The
     * read-write open itself also omits CREATE so a race cannot create ghosts. */
    cbm_store_t *src_store = cr_open_existing_project(project);
    if (!src_store) {
        result.failed = true;
        free_project_list(resolved, resolved_count);
        return result;
    }

    if (cr_cancel_requested(&run)) {
        result.cancelled = true;
        cbm_store_close(src_store);
        free_project_list(resolved, resolved_count);
        return result;
    }

    /* Clean existing CROSS_* edges for this project */
    cr_run_status_t cleanup_status = delete_cross_edges(src_store, project, &run);
    if (cleanup_status != CR_RUN_OK) {
        result.cancelled = cleanup_status == CR_RUN_CANCELLED;
        result.partial_results = result.cancelled && run.mutated;
        result.failed = cleanup_status == CR_RUN_FAILED;
        cbm_store_close(src_store);
        free_project_list(resolved, resolved_count);
        return result;
    }

    /* Match against each target */
    for (int i = 0; i < resolved_count; i++) {
        if (cr_cancel_requested(&run)) {
            result.cancelled = true;
            result.partial_results = run.mutated;
            break;
        }
        const char *tgt = resolved[i];
        if (strcmp(tgt, project) == 0) {
            continue; /* skip self */
        }

        /* Open target store read-write (for bidirectional edge writes) */
        cbm_store_t *tgt_store = cr_open_existing_project(tgt);
        if (!tgt_store) {
            result.failed = true;
            break;
        }

        cr_run_status_t match_status = add_match_count(
            &result.http_edges, match_http_routes(src_store, project, tgt_store, tgt, &run));
        /* Reverse direction: when this pass runs from the provider side, the
         * consumer's HTTP_CALLS live in tgt, not src — the forward pass above
         * finds nothing because the provider has no outbound calls. This also
         * re-creates the provider-side reverse edges that delete_cross_edges
         * just wiped, which a provider-side run previously destroyed for good.
         * A caller edge lives in exactly one DB, so the two directions scan
         * disjoint edge sets and never double-count a pair; the store's
         * (source, target, type) upsert keeps re-recorded rows unique. (#523) */
        if (match_status == CR_RUN_OK) {
            match_status = add_match_count(
                &result.http_edges, match_http_routes(tgt_store, tgt, src_store, project, &run));
        }
        if (match_status == CR_RUN_OK) {
            match_status = add_match_count(
                &result.async_edges, match_async_routes(src_store, project, tgt_store, tgt, &run));
        }
        if (match_status == CR_RUN_OK) {
            match_status = add_match_count(
                &result.channel_edges, match_channels(src_store, project, tgt_store, tgt, &run));
        }
        if (match_status == CR_RUN_OK) {
            match_status =
                add_match_count(&result.grpc_edges,
                                match_typed_routes(src_store, project, tgt_store, tgt, "GRPC_CALLS",
                                                   "service", "method", "CROSS_GRPC_CALLS", &run));
        }
        if (match_status == CR_RUN_OK) {
            match_status = add_match_count(
                &result.graphql_edges,
                match_typed_routes(src_store, project, tgt_store, tgt, "GRAPHQL_CALLS", "operation",
                                   "operation", "CROSS_GRAPHQL_CALLS", &run));
        }
        if (match_status == CR_RUN_OK) {
            match_status = add_match_count(
                &result.trpc_edges,
                match_typed_routes(src_store, project, tgt_store, tgt, "TRPC_CALLS", "procedure",
                                   "procedure", "CROSS_TRPC_CALLS", &run));
        }

        cbm_store_close(tgt_store);
        if (match_status == CR_RUN_FAILED) {
            result.failed = true;
            break;
        }
        if (match_status == CR_RUN_CANCELLED || cr_cancel_requested(&run)) {
            result.cancelled = true;
            result.partial_results = run.mutated;
            break;
        }
        result.projects_scanned++;
    }

    cbm_store_close(src_store);

    free_project_list(resolved, resolved_count);

    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t1);
    result.elapsed_ms = ((double)(t1.tv_sec - t0.tv_sec) * CR_MS_PER_SEC) +
                        ((double)(t1.tv_nsec - t0.tv_nsec) / CR_NS_PER_MS);

    if (result.cancelled) {
        cbm_log_info("cross_repo.cancelled", "project", project, "partial_results",
                     result.partial_results ? "true" : "false");
    } else {
        int total = result.http_edges + result.async_edges + result.channel_edges +
                    result.grpc_edges + result.graphql_edges + result.trpc_edges;
        cbm_log_info("cross_repo.done", "project", project, "total", cr_itoa(total));
    }

    return result;
}

cbm_cross_repo_result_t cbm_cross_repo_match(const char *project, const char **target_projects,
                                             int target_count) {
    return cbm_cross_repo_match_cancellable(project, target_projects, target_count, NULL);
}
