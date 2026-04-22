/*
 * servicelink_http.c — Cross-project HTTP endpoint registration.
 *
 * Unlike the other servicelinkers, HTTP detection is already performed
 * by pass_calls.c / pass_parallel.c using cbm_service_patterns. This
 * linker is a registrar + enrichment pass: it walks existing HTTP_CALLS
 * edges and Route nodes, enriches weak endpoints (env-var regex, k8s
 * Service host match), and registers them in protocol_endpoints for
 * cross-repo matching.
 *
 * Unlike other linkers it does NOT emit new gbuf edges. HTTP_CALLS
 * edges already exist from pass_calls.c; new edges would duplicate.
 */

#include "servicelink.h"
#include "foundation/compat.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Constants ─────────────────────────────────────────────────── */

#define HTTP_CONF_S1    0.55  /* literal path / method */
#define HTTP_CONF_S2    0.30  /* env-var enrichment (raised from 0.20 so S2-alone crosses SL_MIN_CONFIDENCE=0.25) */
#define HTTP_CONF_S3    0.25  /* k8s service host match */
#define HTTP_PATH_MAX   256
#define HTTP_IDENT_MAX  256

/* Signal bits */
#define HTTP_SIG_S1  0x01
#define HTTP_SIG_S2  0x02
#define HTTP_SIG_S3  0x04

/* ── itoa helper (thread-local rotating buffers) ────────────────── */

static const char *itoa_http(int val) {
    static CBM_TLS char bufs[4][32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & 3;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* ── Endpoint struct ────────────────────────────────────────────── */

typedef struct {
    int64_t caller_id;       /* HTTP_CALLS source or route handler */
    int64_t route_node_id;   /* target Route (clients only) */
    char method[16];
    char url_path[HTTP_PATH_MAX];
    char host[HTTP_PATH_MAX];
    char env_var[128];
    char source_qn[512];
    char file_path[HTTP_PATH_MAX];
    uint32_t signals;        /* bitmask: S1/S2/S3 */
    double confidence;
    bool generic_env;        /* true if env_var is DATABASE_URL/etc. */
} http_endpoint_t;

/* ── JSON helper (copied from pass_semantic_edges.c:302) ────────── */

static const char *json_str_value(const char *json, const char *key,
                                   char *buf, int bufsize) {
    if (!json || !key) return NULL;
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

/* ── URL parsing ────────────────────────────────────────────────── */

/* Parse a URL into host + path. If no scheme, the whole input is treated
 * as a path (host stays empty). Output buffers are always NUL-terminated. */
static void parse_url(const char *url, char *host_out, size_t host_sz,
                      char *path_out, size_t path_sz) {
    host_out[0] = '\0';
    path_out[0] = '\0';
    if (!url || !*url) return;

    const char *scheme_end = strstr(url, "://");
    if (!scheme_end) {
        snprintf(path_out, path_sz, "%s", url);
        return;
    }
    const char *host_start = scheme_end + 3;
    const char *slash = strchr(host_start, '/');
    const char *host_end = slash ? slash : host_start + strlen(host_start);

    /* Strip port from host */
    const char *colon = memchr(host_start, ':', (size_t)(host_end - host_start));
    const char *host_stop = colon ? colon : host_end;

    size_t hlen = (size_t)(host_stop - host_start);
    if (hlen >= host_sz) hlen = host_sz - 1;
    memcpy(host_out, host_start, hlen);
    host_out[hlen] = '\0';

    if (slash) {
        snprintf(path_out, path_sz, "%s", slash);
    } else {
        snprintf(path_out, path_sz, "/");
    }
}

/* Strip query string: everything after '?' is dropped in place. */
static void strip_query_string(char *path) {
    if (!path) return;
    char *q = strchr(path, '?');
    if (q) *q = '\0';
}

/* Truncate path to max_len (preserving leading portion — most discriminating). */
static void truncate_path_preserving_leading(char *path, size_t max_len) {
    if (!path) return;
    size_t len = strlen(path);
    if (len <= max_len) return;
    path[max_len] = '\0';
}

/* Check if path is weak (empty or template-placeholder prefixed). */
static bool is_weak_path(const char *path) {
    if (!path || !*path) return true;
    /* Starts with {...} template placeholder → weak */
    if (path[0] == '{') return true;
    /* "/" alone is weak */
    if (strcmp(path, "/") == 0) return true;
    return false;
}

/* ── Generic env-var list ───────────────────────────────────────── */

static bool is_generic_env_var(const char *name) {
    if (!name || !*name) return false;
    static const char *const GENERIC[] = {
        "DATABASE_URL", "REDIS_URL", "BASE_URL",
        "API_URL", "HTTP_PROXY", "HTTPS_PROXY"
    };
    for (size_t i = 0; i < sizeof(GENERIC) / sizeof(GENERIC[0]); i++) {
        if (strcmp(name, GENERIC[i]) == 0) return true;
    }
    return false;
}

/* ── Regex-based enrichment helpers ─────────────────────────────── */

/* Try to find first capture group matching pattern; write into out (bufsz). */
static bool regex_find_first(const char *source, const char *pattern,
                             char *out, size_t bufsz) {
    cbm_regex_t re;
    cbm_regmatch_t matches[2];
    if (cbm_regcomp(&re, pattern, CBM_REG_EXTENDED) != CBM_REG_OK) {
        return false;
    }
    bool found = false;
    if (cbm_regexec(&re, source, 2, matches, 0) == CBM_REG_OK
        && matches[1].rm_so >= 0) {
        int len = matches[1].rm_eo - matches[1].rm_so;
        if ((size_t)len >= bufsz) len = (int)bufsz - 1;
        memcpy(out, source + matches[1].rm_so, (size_t)len);
        out[len] = '\0';
        found = true;
    }
    cbm_regfree(&re);
    return found;
}

/* S2 enrichment: scan the caller's source for env-var references.
 * Sets ep->env_var if found; flips S2 signal bit. */
static void enrich_env_var_from_source(const cbm_pipeline_ctx_t *ctx,
                                        http_endpoint_t *ep) {
    if (!ctx || !ctx->gbuf) return;
    if (ep->env_var[0]) return;

    const cbm_gbuf_node_t *node = cbm_gbuf_find_by_id(ctx->gbuf, ep->caller_id);
    if (!node || !node->file_path) return;

    char *src = sl_read_node_source(ctx, node);
    if (!src) return;

    static const char *const PATTERNS[] = {
        "process\\.env\\.([A-Za-z_][A-Za-z0-9_]*)",              /* JS/TS */
        "os\\.getenv\\([ \t]*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]", /* Python */
        "os\\.Getenv\\([ \t]*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]", /* Go */
        "ENV\\[[ \t]*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]",         /* Ruby */
        "System\\.getenv\\([ \t]*['\"]([A-Za-z_][A-Za-z0-9_]*)['\"]" /* Java */
    };

    for (size_t i = 0; i < sizeof(PATTERNS) / sizeof(PATTERNS[0]); i++) {
        if (regex_find_first(src, PATTERNS[i], ep->env_var, sizeof(ep->env_var))) {
            ep->signals |= HTTP_SIG_S2;
            ep->generic_env = is_generic_env_var(ep->env_var);
            break;
        }
    }

    free(src);
}

/* Return tail after last '/' in a compound "Kind/name" string.
 * Returns full input if no slash. */
static const char *resource_tail(const char *name) {
    if (!name) return "";
    const char *slash = strrchr(name, '/');
    return slash ? slash + 1 : name;
}

/* S3 enrichment: iterate Resource nodes and look for a k8s Service whose
 * tail name matches ep->host. On match, ep->host stays (canonicalized) and
 * S3 bit is set. */
static void enrich_host_from_services(const cbm_pipeline_ctx_t *ctx,
                                       http_endpoint_t *ep) {
    if (!ctx || !ctx->gbuf) return;
    if (!ep->host[0]) return;

    const cbm_gbuf_node_t **resources = NULL;
    int nres = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Resource", &resources, &nres);
    if (nres <= 0) return;

    for (int i = 0; i < nres; i++) {
        const cbm_gbuf_node_t *r = resources[i];
        if (!r || !r->name) continue;
        /* Compound name format: "Kind/metadata-name" (pass_k8s.c). */
        if (strncmp(r->name, "Service/", 8) != 0) continue;
        const char *svc = resource_tail(r->name);
        if (!svc || !*svc) continue;
        if (strcmp(svc, ep->host) == 0) {
            ep->signals |= HTTP_SIG_S3;
            return;
        }
    }
}

/* ── Self-call suppression ──────────────────────────────────────── */

static bool is_loopback_host(const char *host) {
    if (!host || !*host) return false;
    return strcmp(host, "localhost") == 0
        || strcmp(host, "127.0.0.1") == 0
        || strcmp(host, "0.0.0.0") == 0;
}

/* Returns true if the endpoint resolves to the current project itself.
 * Only loopback addresses are treated as unambiguous self-calls here.
 * Service-name matches with local Resource nodes do NOT suppress registration —
 * pass_crossrepolinks.c filters same-project matches at match time. Suppressing
 * at the linker would prevent cross-project service-level matches from ever firing. */
static bool is_self_call(const http_endpoint_t *ep,
                         const cbm_pipeline_ctx_t *ctx) {
    (void)ctx;
    if (!ep) return false;
    return is_loopback_host(ep->host);
}

/* ── Confidence scoring ─────────────────────────────────────────── */

static double compute_confidence(uint32_t signals) {
    double c = 0.0;
    if (signals & HTTP_SIG_S1) c += HTTP_CONF_S1;
    if (signals & HTTP_SIG_S2) c += HTTP_CONF_S2;
    if (signals & HTTP_SIG_S3) c += HTTP_CONF_S3;
    if (c > 1.0) c = 1.0;
    return c;
}

/* ── Canonical identifier + extra JSON ─────────────────────────── */

static void canonicalize_identifier(const http_endpoint_t *ep,
                                    char *out, size_t out_sz) {
    out[0] = '\0';
    if (ep->method[0] && ep->url_path[0] && !is_weak_path(ep->url_path)) {
        snprintf(out, out_sz, "%s %s", ep->method, ep->url_path);
        return;
    }
    if (ep->url_path[0] && !is_weak_path(ep->url_path)) {
        /* Path only (method unknown) */
        snprintf(out, out_sz, "%s", ep->url_path);
        return;
    }
    if (ep->host[0]) {
        snprintf(out, out_sz, "http://%s", ep->host);
        return;
    }
    if (ep->env_var[0]) {
        snprintf(out, out_sz, "env:%s", ep->env_var);
        return;
    }
}

/* Escape a string for inclusion inside a JSON string literal. */
static void json_escape(char *dst, size_t dst_sz, const char *src) {
    size_t di = 0;
    if (!src) { if (dst_sz) dst[0] = '\0'; return; }
    for (size_t si = 0; src[si] && di + 2 < dst_sz; si++) {
        char c = src[si];
        if (c == '"' || c == '\\') {
            if (di + 3 >= dst_sz) break;
            dst[di++] = '\\';
            dst[di++] = c;
        } else if ((unsigned char)c < 0x20) {
            /* skip control chars */
            continue;
        } else {
            dst[di++] = c;
        }
    }
    dst[di] = '\0';
}

static void build_extra_json(const http_endpoint_t *ep,
                             const char *service_name,
                             char *out, size_t out_sz) {
    char em[32], ep_path[HTTP_PATH_MAX * 2], eh[HTTP_PATH_MAX * 2];
    char ev[256], es[32];
    json_escape(em, sizeof(em), ep->method);
    json_escape(ep_path, sizeof(ep_path), ep->url_path);
    json_escape(eh, sizeof(eh), ep->host);
    json_escape(ev, sizeof(ev), ep->env_var);
    json_escape(es, sizeof(es), service_name ? service_name : "");
    snprintf(out, out_sz,
        "{\"method\":\"%s\",\"path\":\"%s\",\"host\":\"%s\","
        "\"env_var\":\"%s\",\"signals\":%u,\"generic\":%s,"
        "\"scheme\":\"http\",\"service_name\":\"%s\"}",
        em, ep_path, eh, ev,
        (unsigned)ep->signals,
        ep->generic_env ? "true" : "false",
        es);
}

/* ── Route QN parsing ───────────────────────────────────────────── */

/* Parse `__route__<METHOD>__<path>` into method and path (path starts with /
 * or is empty). Returns true if the QN matched the expected prefix. */
static bool parse_route_qn(const char *qn, char *method_out, size_t m_sz,
                           char *path_out, size_t p_sz) {
    method_out[0] = '\0';
    path_out[0] = '\0';
    if (!qn) return false;
    static const char *PREFIX = "__route__";
    size_t plen = strlen(PREFIX);
    if (strncmp(qn, PREFIX, plen) != 0) return false;

    const char *method_start = qn + plen;
    const char *sep = strstr(method_start, "__");
    if (!sep) return false;

    size_t mlen = (size_t)(sep - method_start);
    if (mlen >= m_sz) mlen = m_sz - 1;
    memcpy(method_out, method_start, mlen);
    method_out[mlen] = '\0';

    snprintf(path_out, p_sz, "%s", sep + 2);
    return true;
}

/* ── Per-edge processing (client side) ─────────────────────────── */

static void process_client_edge(cbm_pipeline_ctx_t *ctx,
                                const cbm_gbuf_edge_t *edge,
                                cbm_sl_result_t *result) {
    if (!edge || !edge->properties_json) {
        result->unresolved_items++;
        return;
    }

    const cbm_gbuf_node_t *caller = cbm_gbuf_find_by_id(ctx->gbuf, edge->source_id);
    if (!caller) {
        result->unresolved_items++;
        return;
    }

    http_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));
    ep.caller_id = edge->source_id;
    ep.route_node_id = edge->target_id;

    char method_buf[16];
    char url_buf[HTTP_PATH_MAX];
    if (json_str_value(edge->properties_json, "method", method_buf, sizeof(method_buf))) {
        snprintf(ep.method, sizeof(ep.method), "%s", method_buf);
    }
    if (json_str_value(edge->properties_json, "url_path", url_buf, sizeof(url_buf))) {
        /* url_path may be a full URL or a plain path. */
        char host_tmp[HTTP_PATH_MAX];
        char path_tmp[HTTP_PATH_MAX];
        parse_url(url_buf, host_tmp, sizeof(host_tmp), path_tmp, sizeof(path_tmp));
        snprintf(ep.host, sizeof(ep.host), "%s", host_tmp);
        snprintf(ep.url_path, sizeof(ep.url_path), "%s", path_tmp);
    }

    strip_query_string(ep.url_path);
    truncate_path_preserving_leading(ep.url_path, HTTP_PATH_MAX - 1);

    if (ep.url_path[0] && !is_weak_path(ep.url_path)) {
        ep.signals |= HTTP_SIG_S1;
    }

    snprintf(ep.source_qn, sizeof(ep.source_qn), "%s",
             caller->qualified_name ? caller->qualified_name : "");
    snprintf(ep.file_path, sizeof(ep.file_path), "%s",
             caller->file_path ? caller->file_path : "");

    /* S2 enrichment: env-var if path is weak */
    if (is_weak_path(ep.url_path)) {
        enrich_env_var_from_source(ctx, &ep);
    }

    /* S3 enrichment: match host to k8s Service if we have one */
    if (ep.host[0]) {
        enrich_host_from_services(ctx, &ep);
    }

    ep.confidence = compute_confidence(ep.signals);

    if (ep.confidence < SL_MIN_CONFIDENCE) {
        result->unresolved_items++;
        return;
    }

    if (is_self_call(&ep, ctx)) {
        return;
    }

    char identifier[HTTP_IDENT_MAX];
    canonicalize_identifier(&ep, identifier, sizeof(identifier));
    if (!identifier[0]) {
        result->unresolved_items++;
        return;
    }

    char extra[768];
    build_extra_json(&ep, NULL, extra, sizeof(extra));

    if (ctx->endpoints) {
        sl_register_endpoint(ctx->endpoints, ctx->project_name, "http",
                             "producer", identifier,
                             ep.source_qn, ep.file_path, extra);
    }
    result->producers_found++;
}

/* ── Per-node processing (server side, Route nodes) ─────────────── */

static void process_route_node(cbm_pipeline_ctx_t *ctx,
                               const cbm_gbuf_node_t *route,
                               cbm_sl_result_t *result) {
    if (!route) return;

    /* Skip broker Routes — those are handled by their own linkers. */
    if (route->qualified_name) {
        static const char *const BROKER_PREFIXES[] = {
            "__route__infra__", "__route__pubsub__", "__route__cloud_tasks__",
            "__route__async__", "__route__cloud_scheduler__", "__route__kafka__",
            "__route__sqs__"
        };
        for (size_t i = 0; i < sizeof(BROKER_PREFIXES) / sizeof(BROKER_PREFIXES[0]); i++) {
            if (strncmp(route->qualified_name, BROKER_PREFIXES[i],
                        strlen(BROKER_PREFIXES[i])) == 0) {
                return;
            }
        }
    }

    http_endpoint_t ep;
    memset(&ep, 0, sizeof(ep));

    /* Prefer method from properties; fall back to QN parse. */
    char method_buf[16] = {0};
    char qn_method[16] = {0};
    char qn_path[HTTP_PATH_MAX] = {0};
    parse_route_qn(route->qualified_name, qn_method, sizeof(qn_method),
                   qn_path, sizeof(qn_path));

    if (route->properties_json
        && json_str_value(route->properties_json, "method", method_buf, sizeof(method_buf))) {
        snprintf(ep.method, sizeof(ep.method), "%s", method_buf);
    } else if (qn_method[0]) {
        snprintf(ep.method, sizeof(ep.method), "%s", qn_method);
    }

    /* Route name is the URL path (set by pass_route_nodes.c). */
    if (route->name && route->name[0]) {
        snprintf(ep.url_path, sizeof(ep.url_path), "%s", route->name);
    } else if (qn_path[0]) {
        snprintf(ep.url_path, sizeof(ep.url_path), "%s", qn_path);
    }

    strip_query_string(ep.url_path);
    truncate_path_preserving_leading(ep.url_path, HTTP_PATH_MAX - 1);

    if (!ep.url_path[0] || is_weak_path(ep.url_path)) {
        result->unresolved_items++;
        return;
    }
    ep.signals |= HTTP_SIG_S1;

    /* Find the handler function via HANDLES edges → use as source_qn. */
    const cbm_gbuf_edge_t **handles = NULL;
    int nhandles = 0;
    cbm_gbuf_find_edges_by_target_type(ctx->gbuf, route->id, "HANDLES",
                                       &handles, &nhandles);

    if (nhandles > 0) {
        const cbm_gbuf_node_t *handler = cbm_gbuf_find_by_id(ctx->gbuf,
                                                             handles[0]->source_id);
        if (handler) {
            ep.caller_id = handler->id;
            snprintf(ep.source_qn, sizeof(ep.source_qn), "%s",
                     handler->qualified_name ? handler->qualified_name : "");
            snprintf(ep.file_path, sizeof(ep.file_path), "%s",
                     handler->file_path ? handler->file_path : "");
        }
    }
    if (!ep.source_qn[0]) {
        /* No handler — use the Route's own info */
        snprintf(ep.source_qn, sizeof(ep.source_qn), "%s",
                 route->qualified_name ? route->qualified_name : "");
        snprintf(ep.file_path, sizeof(ep.file_path), "%s",
                 route->file_path ? route->file_path : "");
    }

    ep.confidence = compute_confidence(ep.signals);
    if (ep.confidence < SL_MIN_CONFIDENCE) {
        result->unresolved_items++;
        return;
    }

    char identifier[HTTP_IDENT_MAX];
    canonicalize_identifier(&ep, identifier, sizeof(identifier));
    if (!identifier[0]) {
        result->unresolved_items++;
        return;
    }

    char extra[768];
    build_extra_json(&ep, ctx->project_name, extra, sizeof(extra));

    if (ctx->endpoints) {
        sl_register_endpoint(ctx->endpoints, ctx->project_name, "http",
                             "consumer", identifier,
                             ep.source_qn, ep.file_path, extra);
    }
    result->consumers_found++;
}

/* ── Main entry point ──────────────────────────────────────────── */

int cbm_servicelink_http(cbm_pipeline_ctx_t *ctx) {
    if (!ctx || !ctx->gbuf) return 0;

    cbm_log_info("servicelink.start", "protocol", "http");

    cbm_sl_result_t result = {
        .name = "http",
        .links_created = 0,
        .producers_found = 0,
        .consumers_found = 0,
        .unresolved_items = 0,
        .ambiguous_dropped = 0
    };

    /* 1) Walk HTTP_CALLS edges (clients). */
    const cbm_gbuf_edge_t **edges = NULL;
    int nedges = 0;
    cbm_gbuf_find_edges_by_type(ctx->gbuf, SL_EDGE_HTTP, &edges, &nedges);
    for (int i = 0; i < nedges; i++) {
        process_client_edge(ctx, edges[i], &result);
    }

    /* 2) Walk Route nodes (servers). */
    const cbm_gbuf_node_t **routes = NULL;
    int nroutes = 0;
    cbm_gbuf_find_by_label(ctx->gbuf, "Route", &routes, &nroutes);
    for (int i = 0; i < nroutes; i++) {
        process_route_node(ctx, routes[i], &result);
    }

    cbm_log_info("servicelink.http.done",
                 "producers", itoa_http(result.producers_found),
                 "consumers", itoa_http(result.consumers_found),
                 "unresolved", itoa_http(result.unresolved_items));

    /* This linker emits no gbuf edges — return 0 (not a failure). */
    return 0;
}
