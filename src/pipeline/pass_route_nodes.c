/*
 * pass_route_nodes.c — Create Route nodes for HTTP_CALLS/ASYNC_CALLS edges.
 *
 * After parallel resolve merges edges into the main gbuf, HTTP_CALLS and
 * ASYNC_CALLS edges have url_path/method/broker in properties but point to
 * the library function (e.g., requests.get). This pass:
 *
 *   1. Scans all HTTP_CALLS/ASYNC_CALLS edges
 *   2. Extracts url_path from edge properties
 *   3. Creates Route nodes with deterministic QNs (__route__METHOD__/path)
 *   4. Re-targets edges from library function → Route node
 *
 * Route nodes are the rendezvous point for cross-service communication:
 *   Service A: checkout() → HTTP_CALLS → Route("POST /api/orders")
 *   Service B: create_order() → HANDLES → Route("POST /api/orders")
 */
#include "foundation/constants.h"

enum {
    RN_MAX_SCHEME = 12,
    RN_MIN_SCHEME = 3,
    RN_STRIP_PASSES = 2,
    RN_SCHEME_SKIP = 3,
    RN_ARGS_SKIP = 7,
    RN_PROPS_MARGIN = 100,
    RN_CALLEE_SKIP = 14, /* strlen('"callee_args":[') - 1 */
};

#define SLEN(s) (sizeof(s) - 1)
#include "pipeline/pipeline_internal.h"
#include <stdint.h>
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"

#include <stdio.h>
#include <string.h>

/* Extract a JSON string value by key from properties.
 * Returns pointer into buf (caller provides buffer). NULL if not found. */
static const char *json_extract(const char *json, const char *key, char *buf, int bufsz) {
    if (!json || !key) {
        return NULL;
    }
    /* Build "key":" pattern */
    char pattern[CBM_SZ_128];
    snprintf(pattern, sizeof(pattern), "\"%s\":\"", key);
    const char *start = strstr(json, pattern);
    if (!start) {
        return NULL;
    }
    start += strlen(pattern);
    const char *end = strchr(start, '"');
    if (!end || end == start) {
        return NULL;
    }
    int len = (int)(end - start);
    if (len >= bufsz) {
        len = bufsz - SKIP_ONE;
    }
    memcpy(buf, start, (size_t)len);
    buf[len] = '\0';
    return buf;
}

/* Visitor context for edge scanning */
typedef struct {
    cbm_gbuf_t *gb;
    int created;
} route_ctx_t;

static void route_edge_visitor(const cbm_gbuf_edge_t *edge, void *userdata) {
    route_ctx_t *ctx = (route_ctx_t *)userdata;

    /* Only process HTTP_CALLS and ASYNC_CALLS */
    if (strcmp(edge->type, "HTTP_CALLS") != 0 && strcmp(edge->type, "ASYNC_CALLS") != 0) {
        return;
    }

    /* Extract url_path from properties */
    char url_buf[CBM_SZ_512];
    const char *url = json_extract(edge->properties_json, "url_path", url_buf, sizeof(url_buf));
    if (!url || !url[0]) {
        return;
    }

    /* Extract method or broker */
    char method_buf[CBM_SZ_16];
    char broker_buf[CBM_SZ_64];
    const char *method =
        json_extract(edge->properties_json, "method", method_buf, sizeof(method_buf));
    const char *broker =
        json_extract(edge->properties_json, "broker", broker_buf, sizeof(broker_buf));

    /* Build Route QN */
    char route_qn[CBM_ROUTE_QN_SIZE];
    if (strcmp(edge->type, "HTTP_CALLS") == 0) {
        snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", method ? method : "ANY", url);
    } else {
        snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", broker ? broker : "async", url);
    }

    /* Build properties for Route node */
    char route_props[CBM_SZ_256];
    if (method) {
        snprintf(route_props, sizeof(route_props), "{\"method\":\"%s\"}", method);
    } else if (broker) {
        snprintf(route_props, sizeof(route_props), "{\"broker\":\"%s\"}", broker);
    } else {
        snprintf(route_props, sizeof(route_props), "{}");
    }

    /* Create or find Route node (deduped by QN) */
    cbm_gbuf_upsert_node(ctx->gb, "Route", url, route_qn, "", 0, 0, route_props);
    ctx->created++;

    /* Note: we do NOT re-target the edge here because modifying edges during
     * iteration is unsafe. The edge stays pointing to the library function.
     * The URL-in-args detection in pass_parallel will create Route→handler HANDLES
     * edge separately. The caller→Route edge is created by pass_calls for
     * the sequential path; for the parallel path, the caller→library edge
     * with url_path in properties is sufficient for query_graph to find
     * the Route via: caller → HTTP_CALLS(url_path="/api/x") + Route("/api/x"). */
}

/* Extract URL path from full URL: "https://host/path/" → "/path/" */
static const char *url_path(const char *url) {
    if (!url) {
        return NULL;
    }
    const char *scheme_end = strstr(url, "://");
    if (!scheme_end) {
        return url; /* Already a path */
    }
    const char *path = strchr(scheme_end + RN_SCHEME_SKIP, '/');
    return path ? path : "/";
}

/* Check if a trailing dash-segment is a hash/project-number (strippable).
 * Returns true if the segment is short alphanumeric but not a meaningful word. */
static bool is_hash_segment(const char *seg, size_t slen) {
    if (slen > RN_MAX_SCHEME || slen == 0) {
        return false;
    }
    int has_letter = 0;
    for (size_t si = 0; si < slen; si++) {
        if (!((seg[si] >= '0' && seg[si] <= '9') || (seg[si] >= 'a' && seg[si] <= 'z'))) {
            return false;
        }
        if (seg[si] >= 'a' && seg[si] <= 'z') {
            has_letter = SKIP_ONE;
        }
    }
    /* Don't strip meaningful words (>=3 chars with letters like "api", "endpoint") */
    return !(has_letter && slen >= RN_MIN_SCHEME);
}

/* Strip up to 2 trailing hash/project-number segments from a Cloud Run hostname. */
static void strip_cloud_run_suffixes(char *hostname) {
    for (int strip = 0; strip < RN_STRIP_PASSES; strip++) {
        char *last_dash = strrchr(hostname, '-');
        if (!last_dash) {
            break;
        }
        if (!is_hash_segment(last_dash + SKIP_ONE, strlen(last_dash + SKIP_ONE))) {
            break;
        }
        *last_dash = '\0';
    }
}

/* Extract service name from Cloud Run URL hostname.
 * "my-svc-ab12cd34ef-uc.a.run.app/path" → "my-svc" */
static const char *extract_service_name(const char *url, char *buf, int bufsz) {
    if (!url) {
        return NULL;
    }
    const char *scheme_end = strstr(url, "://");
    if (!scheme_end) {
        return NULL;
    }
    const char *host_start = scheme_end + RN_SCHEME_SKIP;
    const char *end = host_start;
    while (*end && *end != '.' && *end != '/') {
        end++;
    }
    int hlen = (int)(end - host_start);
    if (hlen <= 0 || hlen >= bufsz) {
        return NULL;
    }

    char tmp[CBM_SZ_256];
    if (hlen >= (int)sizeof(tmp)) {
        return NULL;
    }
    memcpy(tmp, host_start, (size_t)hlen);
    tmp[hlen] = '\0';

    strip_cloud_run_suffixes(tmp);

    snprintf(buf, (size_t)bufsz, "%s", tmp);
    return buf;
}

/* Phase 2: Match infra Route URLs to handler Route nodes by URL path + service name. */
/* Check if a Route QN indicates an infra/async/broker Route (not a handler). */
static bool is_broker_route(const char *qn) {
    static const char *const prefixes[] = {
        "__route__infra__", "__route__pubsub__",          "__route__cloud_tasks__",
        "__route__async__", "__route__cloud_scheduler__", "__route__kafka__",
        "__route__sqs__"};
    if (!qn) {
        return false;
    }
    for (int i = 0; i < (int)(sizeof(prefixes) / sizeof(prefixes[0])); i++) {
        if (strstr(qn, prefixes[i]) == qn) {
            return true;
        }
    }
    return false;
}

/* Try to match a single infra Route to a handler Route and create HANDLES bridge.
 * Returns 1 if matched, 0 otherwise. */
static int match_one_infra_route(cbm_gbuf_t *gb, const cbm_gbuf_node_t *infra,
                                 const char *infra_path, const char *svc_name,
                                 const cbm_gbuf_node_t **all_routes, int route_count) {
    for (int j = 0; j < route_count; j++) {
        const cbm_gbuf_node_t *handler_route = all_routes[j];
        if (is_broker_route(handler_route->qualified_name)) {
            continue;
        }
        int file_matches = (handler_route->file_path != NULL &&
                            strstr(handler_route->file_path, svc_name) != NULL);
        int is_prefix_route =
            (handler_route->qualified_name != NULL &&
             strncmp(handler_route->qualified_name, "__route__ANY__", SLEN("__route__ANY__")) == 0);
        if (!file_matches && !is_prefix_route) {
            continue;
        }
        const char *handler_name = handler_route->name;
        if (!handler_name) {
            continue;
        }
        const char *handler_path = strchr(handler_name, '/');
        if (!handler_path) {
            continue;
        }

        int path_match =
            (strlen(handler_path) > SKIP_ONE && (strstr(infra_path, handler_path) != NULL ||
                                                 strstr(handler_path, infra_path) != NULL));
        int root_svc_match = (strcmp(handler_path, "/") == 0);
        if (path_match || root_svc_match) {
            const cbm_gbuf_edge_t **fn_handles = NULL;
            int fn_hcount = 0;
            cbm_gbuf_find_edges_by_target_type(gb, handler_route->id, "HANDLES", &fn_handles,
                                               &fn_hcount);
            for (int fh = 0; fh < fn_hcount; fh++) {
                cbm_gbuf_insert_edge(gb, fn_handles[fh]->source_id, infra->id, "HANDLES",
                                     "{\"source\":\"infra_match\"}");
            }
            return SKIP_ONE;
        }
    }
    return 0;
}

/* Phase 2: Match infra Route URLs to handler Route nodes by URL path + service name. */
static void match_infra_routes(cbm_gbuf_t *gb) {
    const cbm_gbuf_node_t **all_routes = NULL;
    int route_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Route", &all_routes, &route_count) != 0 || route_count == 0) {
        return;
    }

    int matched = 0;

    for (int i = 0; i < route_count; i++) {
        const cbm_gbuf_node_t *infra = all_routes[i];
        if (!infra->qualified_name ||
            strncmp(infra->qualified_name, "__route__infra__", SLEN("__route__infra__")) != 0) {
            continue;
        }
        if (!infra->name || !strstr(infra->name, "://")) {
            continue;
        }

        const char *infra_path = url_path(infra->name);
        char svc_buf[CBM_SZ_128];
        const char *svc_name = extract_service_name(infra->name, svc_buf, sizeof(svc_buf));
        if (!infra_path || !svc_name) {
            continue;
        }

        matched += match_one_infra_route(gb, infra, infra_path, svc_name, all_routes, route_count);
    }

    if (matched > 0) {
        char buf[CBM_SZ_16];
        snprintf(buf, sizeof(buf), "%d", matched);
        cbm_log_info("pass.route_match", "infra_matched", buf);
    }
}

/* Phase 2a: Ensure all functions with route_path properties have Route+HANDLES edges.
 * During incremental indexing, only changed files get Route nodes from extraction.
 * This pass scans ALL Function/Method nodes and creates missing Route+HANDLES. */
/* Extract a JSON string property value into buf. Returns true if found. */
static bool extract_json_prop(const char *json, const char *key, char *buf, int bufsz) {
    if (!json) {
        return false;
    }
    char pattern[CBM_SZ_64];
    snprintf(pattern, sizeof(pattern), "\"%s\":\"", key);
    const char *p = strstr(json, pattern);
    if (!p) {
        return false;
    }
    p += strlen(pattern);
    const char *end = strchr(p, '"');
    if (!end || end <= p) {
        return false;
    }
    int len = (int)(end - p);
    if (len >= bufsz) {
        return false;
    }
    memcpy(buf, p, (size_t)len);
    buf[len] = '\0';
    return true;
}

/* Process a single Function/Method node: create Route+HANDLES if it has route_path.
 * Returns 1 if a new HANDLES edge was created, 0 otherwise. */
static int ensure_one_decorator_route(cbm_gbuf_t *gb, const cbm_gbuf_node_t *func) {
    if (!func->properties_json) {
        return 0;
    }

    char path[CBM_SZ_256];
    if (!extract_json_prop(func->properties_json, "route_path", path, sizeof(path))) {
        return 0;
    }
    if (path[0] != '/') {
        return 0;
    }

    char method[CBM_SZ_16] = "ANY";
    extract_json_prop(func->properties_json, "route_method", method, sizeof(method));

    char route_qn[CBM_ROUTE_QN_SIZE];
    snprintf(route_qn, sizeof(route_qn), "__route__%s__%s", method, path);
    const cbm_gbuf_node_t *existing = cbm_gbuf_find_by_qn(gb, route_qn);

    char rprops[CBM_SZ_256];
    snprintf(rprops, sizeof(rprops), "{\"method\":\"%s\",\"source\":\"decorator\"}", method);
    int64_t route_id = cbm_gbuf_upsert_node(gb, "Route", path, route_qn,
                                            func->file_path ? func->file_path : "", 0, 0, rprops);

    if (existing) {
        const cbm_gbuf_edge_t **existing_handles = NULL;
        int eh_count = 0;
        cbm_gbuf_find_edges_by_target_type(gb, route_id, "HANDLES", &existing_handles, &eh_count);
        for (int eh = 0; eh < eh_count; eh++) {
            if (existing_handles[eh]->source_id == func->id) {
                return 0;
            }
        }
    }

    char hprops[CBM_SZ_512];
    snprintf(hprops, sizeof(hprops), "{\"handler\":\"%s\"}",
             func->qualified_name ? func->qualified_name : "");
    cbm_gbuf_insert_edge(gb, func->id, route_id, "HANDLES", hprops);
    return SKIP_ONE;
}

/* Phase 2a: Ensure all functions with route_path properties have Route+HANDLES edges. */
static void ensure_decorator_routes(cbm_gbuf_t *gb) {
    const char *labels[] = {"Function", "Method"};
    int created = 0;

    for (int li = 0; li < RN_STRIP_PASSES; li++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[li], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            created += ensure_one_decorator_route(gb, nodes[i]);
        }
    }

    if (created > 0) {
        char buf[CBM_SZ_16];
        snprintf(buf, sizeof(buf), "%d", created);
        cbm_log_info("pass.ensure_decorator_routes", "created", buf);
    }
}

/* Phase 2b: Connect prefix Routes to decorator handler Functions.
 * For each prefix Route (__route__ANY__/path), find the CALLS edge leading to it
 * (from the registering file), derive the service directory, then find decorator
 * Routes in that directory tree and create HANDLES from their handler Functions
 * to the prefix Route. This bridges include_router → decorator → handler. */
/* Bridge decorator handler Functions to a prefix Route. Returns number connected. */
static int bridge_funcs_to_prefix(cbm_gbuf_t *gb, const cbm_gbuf_node_t *prefix_route,
                                  const char *registrar_path, int dir_len,
                                  const char *prefix_segs) {
    const cbm_gbuf_node_t **funcs = NULL;
    int func_count = 0;
    cbm_gbuf_find_by_label(gb, "Function", &funcs, &func_count);

    int connected = 0;
    for (int fi = 0; fi < func_count; fi++) {
        const cbm_gbuf_node_t *func = funcs[fi];
        if (!func->file_path) {
            continue;
        }
        if (strncmp(func->file_path, registrar_path, (size_t)dir_len) != 0) {
            continue;
        }
        if (!func->properties_json || !strstr(func->properties_json, "\"route_path\"")) {
            continue;
        }
        if (prefix_segs && prefix_segs[0] && !strstr(func->file_path, prefix_segs)) {
            continue;
        }
        cbm_gbuf_insert_edge(gb, func->id, prefix_route->id, "HANDLES",
                             "{\"source\":\"prefix_decorator_bridge\"}");
        connected++;
    }
    return connected;
}

/* Phase 2b: Connect prefix Routes to decorator handler Functions. */
static void connect_prefix_to_decorators(cbm_gbuf_t *gb) {
    const cbm_gbuf_node_t **routes = NULL;
    int route_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Route", &routes, &route_count) != 0) {
        return;
    }

    int connected = 0;

    for (int ri = 0; ri < route_count; ri++) {
        const cbm_gbuf_node_t *prefix_route = routes[ri];
        if (!prefix_route->qualified_name ||
            strncmp(prefix_route->qualified_name, "__route__ANY__/", SLEN("__route__ANY__/")) !=
                0) {
            continue;
        }

        const cbm_gbuf_edge_t **calls_in = NULL;
        int calls_count = 0;
        cbm_gbuf_find_edges_by_target_type(gb, prefix_route->id, "CALLS", &calls_in, &calls_count);
        if (calls_count == 0) {
            continue;
        }

        const cbm_gbuf_node_t *registrar = cbm_gbuf_find_by_id(gb, calls_in[0]->source_id);
        if (!registrar || !registrar->file_path) {
            continue;
        }
        const char *last_slash = strrchr(registrar->file_path, '/');
        if (!last_slash) {
            continue;
        }
        int dir_len = (int)(last_slash - registrar->file_path) + SKIP_ONE;

        const char *prefix_path = prefix_route->name;
        const char *prefix_segs =
            (prefix_path && prefix_path[0] == '/') ? prefix_path + SKIP_ONE : prefix_path;

        connected +=
            bridge_funcs_to_prefix(gb, prefix_route, registrar->file_path, dir_len, prefix_segs);
    }

    if (connected > 0) {
        char buf[CBM_SZ_16];
        snprintf(buf, sizeof(buf), "%d", connected);
        cbm_log_info("pass.prefix_bridge", "connected", buf);
    }
}

/* Phase 3: Create DATA_FLOWS edges by linking callers through Route to handlers.
 * For each HTTP_CALLS/ASYNC_CALLS edge (caller → Route), find the HANDLES edge
 * (handler → Route) and create DATA_FLOWS (caller → handler) with route context. */
/* Check if a direct CALLS edge already exists between two nodes */
static int has_direct_call(const cbm_gbuf_t *gb, int64_t source, int64_t target) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, source, "CALLS", &edges, &count);
    for (int i = 0; i < count; i++) {
        if (edges[i]->target_id == target) {
            return SKIP_ONE;
        }
    }
    return 0;
}

/* Extract param_names from a node's properties_json.
 * Returns a comma-separated string in buf, or empty string. */
static void extract_param_names(const cbm_gbuf_node_t *node, char *buf, int bufsize) {
    buf[0] = '\0';
    if (!node || !node->properties_json) {
        return;
    }
    const char *p = strstr(node->properties_json, "\"param_names\":");
    if (!p) {
        return;
    }
    p = strchr(p, '[');
    if (!p) {
        return;
    }
    p++; /* skip '[' */
    const char *end = strchr(p, ']');
    if (!end || end <= p) {
        return;
    }
    int len = (int)(end - p);
    if (len >= bufsize) {
        len = bufsize - SKIP_ONE;
    }
    memcpy(buf, p, (size_t)len);
    buf[len] = '\0';
}

/* Extract the "args" JSON fragment from an edge's properties.
 * Returns pointer into the properties string (not copied). */
static const char *find_args_in_props(const char *props) {
    if (!props) {
        return NULL;
    }
    const char *p = strstr(props, "\"args\":[");
    if (!p) {
        return NULL;
    }
    return p + RN_ARGS_SKIP; /* skip "args":[ , points to first { or ] */
}

/* Append handler_params and caller_args to DATA_FLOWS props. Closes with '}'. */
static void finish_data_flow_props(char *props, size_t propsz, size_t pos,
                                   const char *handler_params, const char *args_json) {
    if (handler_params[0]) {
        int w = snprintf(props + pos, propsz - pos, ",\"handler_params\":[%s]", handler_params);
        if (w > 0) {
            pos += (size_t)w;
        }
    }
    if (args_json) {
        int w = snprintf(props + pos, propsz - pos, ",\"caller_args\":[%.*s", 400, args_json);
        if (w > 0) {
            pos += (size_t)w;
            char *close = strchr(props + (pos - (size_t)w) + RN_CALLEE_SKIP, ']');
            if (close && close < props + propsz - PAIR_LEN) {
                pos = (size_t)(close - props) + SKIP_ONE;
            }
        }
    }
    if (pos < propsz - SKIP_ONE) {
        props[pos] = '}';
        props[pos + SKIP_ONE] = '\0';
    }
}

typedef struct {
    int64_t source_id;
    const char *props;
    const char *edge_type;
} caller_edge_ref_t;

/* Try to create a DATA_FLOWS edge between caller and handler via a route.
 * Returns: 1=created, 0=skipped (self/duplicate), -1=skipped (has direct call). */
static int try_create_data_flow(cbm_gbuf_t *gb, int64_t caller_id, int64_t handler_id,
                                const cbm_gbuf_node_t *route, const char *edge_type,
                                const char *caller_props, bool via_infra) {
    if (caller_id == handler_id) {
        return 0;
    }
    if (has_direct_call(gb, caller_id, handler_id)) {
        return CBM_NOT_FOUND;
    }

    const char *args_json = find_args_in_props(caller_props);
    const cbm_gbuf_node_t *handler_node = cbm_gbuf_find_by_id(gb, handler_id);
    char handler_params[CBM_SZ_512];
    extract_param_names(handler_node, handler_params, sizeof(handler_params));

    char props[CBM_SZ_2K];
    int n;
    if (via_infra) {
        n = snprintf(props, sizeof(props),
                     "{\"via\":\"%s\",\"route\":\"%s\",\"edge_type\":\"%s\",\"via_infra\":true",
                     route->name ? route->name : "",
                     route->qualified_name ? route->qualified_name : "", edge_type);
    } else {
        n = snprintf(props, sizeof(props), "{\"via\":\"%s\",\"route\":\"%s\",\"edge_type\":\"%s\"",
                     route->name ? route->name : "",
                     route->qualified_name ? route->qualified_name : "", edge_type);
    }

    if (n > 0 && (size_t)n < sizeof(props) - RN_PROPS_MARGIN) {
        finish_data_flow_props(props, sizeof(props), (size_t)n, handler_params, args_json);
    }
    cbm_gbuf_insert_edge(gb, caller_id, handler_id, "DATA_FLOWS", props);
    return SKIP_ONE;
}

/* Collect extra handler IDs reachable via INFRA_MAPS chain from a route. */
static int collect_infra_handlers(cbm_gbuf_t *gb, int64_t route_id, int64_t *out, int max_out) {
    const cbm_gbuf_edge_t **infra_edges = NULL;
    int infra_count = 0;
    cbm_gbuf_find_edges_by_source_type(gb, route_id, "INFRA_MAPS", &infra_edges, &infra_count);

    int n = 0;
    for (int ie = 0; ie < infra_count; ie++) {
        const cbm_gbuf_edge_t **ep_handles = NULL;
        int ep_hcount = 0;
        cbm_gbuf_find_edges_by_target_type(gb, infra_edges[ie]->target_id, "HANDLES", &ep_handles,
                                           &ep_hcount);
        for (int eh = 0; eh < ep_hcount && n < max_out; eh++) {
            out[n++] = ep_handles[eh]->source_id;
        }
    }
    return n;
}

/* Collect caller edges (HTTP_CALLS + ASYNC_CALLS) targeting a route. */
static int collect_caller_edges(cbm_gbuf_t *gb, int64_t route_id, caller_edge_ref_t *out,
                                int max_out) {
    int n = 0;
    const cbm_gbuf_edge_t **http_edges = NULL;
    int http_count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, route_id, "HTTP_CALLS", &http_edges, &http_count);
    for (int i = 0; i < http_count && n < max_out; i++) {
        out[n].source_id = http_edges[i]->source_id;
        out[n].props = http_edges[i]->properties_json;
        out[n].edge_type = "HTTP_CALLS";
        n++;
    }
    const cbm_gbuf_edge_t **async_edges = NULL;
    int async_count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, route_id, "ASYNC_CALLS", &async_edges, &async_count);
    for (int i = 0; i < async_count && n < max_out; i++) {
        out[n].source_id = async_edges[i]->source_id;
        out[n].props = async_edges[i]->properties_json;
        out[n].edge_type = "ASYNC_CALLS";
        n++;
    }
    return n;
}

/* Create DATA_FLOWS between callers and handlers for one route node. */
static void create_route_data_flows(cbm_gbuf_t *gb, const cbm_gbuf_node_t *route,
                                    const caller_edge_ref_t *callers, int n_callers, int *flows,
                                    int *skipped) {
    const cbm_gbuf_edge_t **handles_edges = NULL;
    int handles_count = 0;
    cbm_gbuf_find_edges_by_target_type(gb, route->id, "HANDLES", &handles_edges, &handles_count);

    int64_t extra_handlers[CBM_SZ_32];
    int n_extra = collect_infra_handlers(gb, route->id, extra_handlers, CBM_SZ_32);

    for (int ci = 0; ci < n_callers; ci++) {
        for (int hi = 0; hi < handles_count; hi++) {
            int rc = try_create_data_flow(gb, callers[ci].source_id, handles_edges[hi]->source_id,
                                          route, callers[ci].edge_type, callers[ci].props, false);
            if (rc > 0) {
                (*flows)++;
            } else if (rc < 0) {
                (*skipped)++;
            }
        }
        for (int xi = 0; xi < n_extra; xi++) {
            int rc = try_create_data_flow(gb, callers[ci].source_id, extra_handlers[xi], route,
                                          callers[ci].edge_type, callers[ci].props, true);
            if (rc > 0) {
                (*flows)++;
            } else if (rc < 0) {
                (*skipped)++;
            }
        }
    }
}

/* Phase 3: Create DATA_FLOWS edges by linking callers through Route to handlers. */
static void create_data_flows(cbm_gbuf_t *gb) {
    const cbm_gbuf_node_t **routes = NULL;
    int route_count = 0;
    if (cbm_gbuf_find_by_label(gb, "Route", &routes, &route_count) != 0 || route_count == 0) {
        return;
    }

    int flows = 0;
    int skipped = 0;

    for (int ri = 0; ri < route_count; ri++) {
        caller_edge_ref_t caller_edges[CBM_SZ_64];
        int n_callers = collect_caller_edges(gb, routes[ri]->id, caller_edges, CBM_SZ_64);
        create_route_data_flows(gb, routes[ri], caller_edges, n_callers, &flows, &skipped);
    }

    if (flows > 0 || skipped > 0) {
        char buf1[CBM_SZ_16];
        char buf2[CBM_SZ_16];
        snprintf(buf1, sizeof(buf1), "%d", flows);
        snprintf(buf2, sizeof(buf2), "%d", skipped);
        cbm_log_info("pass.data_flows", "created", buf1, "skipped_has_call", buf2);
    }
}

void cbm_pipeline_create_route_nodes(cbm_gbuf_t *gb) {
    if (!gb) {
        return;
    }

    route_ctx_t ctx = {.gb = gb, .created = 0};
    cbm_gbuf_foreach_edge(gb, route_edge_visitor, &ctx);

    if (ctx.created > 0) {
        char buf[CBM_SZ_16];
        snprintf(buf, sizeof(buf), "%d", ctx.created);
        cbm_log_info("pass.route_nodes", "created", buf);
    }

    /* Phase 2a: ensure all functions with route_path have Route+HANDLES.
     * Handles incremental mode where unchanged files don't re-extract. */
    ensure_decorator_routes(gb);

    /* Phase 2b: connect prefix Routes to decorator handler Functions.
     * Must run BEFORE match_infra_routes so infra matching can find
     * HANDLES edges on prefix Routes for the bridge. */
    connect_prefix_to_decorators(gb);

    /* Phase 2b: match infra Routes to handler Routes by URL path */
    match_infra_routes(gb);

    /* Phase 3: create DATA_FLOWS edges through Routes */
    create_data_flows(gb);
}
