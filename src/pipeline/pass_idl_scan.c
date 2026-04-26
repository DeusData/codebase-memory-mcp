/*
 * pass_idl_scan.c — IDL-driven cross-repo binding for gRPC.
 *
 * Runs after pass_definitions / pass_calls. Three responsibilities:
 *
 *   1. Emit Route nodes derived from .proto-defined services and rpcs.
 *      For each Class node whose file_path ends in ".proto", iterate its
 *      DEFINES_METHOD edges to find rpc methods, and create:
 *        Route node with QN __route__grpc__<Service>/<rpc>
 *        HANDLES edge from the rpc Function node back to the Route
 *
 *   2. Bind consumer-side gRPC handler classes.
 *      For each INHERITS edge whose base name matches a server-stub suffix
 *      (Servicer, ServicerBase, ImplBase, ServiceBase, Base, AsyncServicer),
 *      strip the suffix to derive the expected service name, walk methods
 *      of the inheriting class via DEFINES_METHOD edges, and emit:
 *        HANDLES edge from each method to the matching IDL Route
 *
 *   3. Emit producer-side GRPC_CALLS edges from typed-client method calls.
 *      Walk per-file extraction results (CBMFileResult.type_assigns) to
 *      build a (var, enclosing_func_qn) → service_name map for variables
 *      typed as a generated client/stub (*Stub, *BlockingStub, *FutureStub,
 *      *AsyncStub, *Client, *AsyncClient). For each call whose callee_name
 *      is "var.Method", look up var in the map; if found and the derived
 *      service matches a known proto Service, upsert a local Route node and
 *      emit a GRPC_CALLS edge from the caller to the Route with
 *      {service, method} properties.
 *
 * The HANDLES edges are the rendezvous point for pass_cross_repo's existing
 * Phase D matcher (match_typed_routes for GRPC_CALLS): producer-side
 * GRPC_CALLS edges + consumer-side HANDLES edges close the loop, and the
 * cross-repo pass emits CROSS_GRPC_CALLS without further changes here.
 *
 * Producer-side detection only fires when the producer repo also indexes the
 * .proto contract (vendored, submoduled, or inline). Cross-repo matching
 * works through Phase D regardless of which repo the .proto lives in, but
 * detecting that a given call IS a gRPC client call requires a Service node
 * to exist somewhere in the producer's gbuf. Repos that import compiled
 * stubs without source access fall through to ordinary CALLS edges.
 *
 * Builds on the cross-repo scaffolding in pass_cross_repo.c without modifying it.
 */
#include "foundation/constants.h"

enum {
    IDL_QN_BUF = 768,
    IDL_PROPS_BUF = 256,
    IDL_NAME_BUF = 256,
    IDL_LOG_BUF = 16,
    IDL_VAR_INIT_CAP = 16,
    IDL_SVC_INIT_CAP = 8,
};

#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"
#include "cbm.h"

#include <ctype.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── Small helpers ───────────────────────────────────────────────── */

static bool idl_ends_with(const char *s, const char *suffix) {
    if (!s || !suffix) {
        return false;
    }
    size_t sl = strlen(s);
    size_t fl = strlen(suffix);
    if (fl > sl) {
        return false;
    }
    return strcmp(s + sl - fl, suffix) == 0;
}

static bool idl_is_proto_file(const char *path) {
    return idl_ends_with(path, ".proto");
}

/* Strip the longest matching suffix from name, returning a heap-allocated
 * copy of the prefix. Returns NULL if no suffix matched or on alloc error.
 * Suffix table is NULL-terminated; longer entries should appear first so
 * "ServicerBase" matches before "Servicer". */
static char *idl_strip_suffix(const char *name, const char *const *suffixes) {
    if (!name) {
        return NULL;
    }
    size_t nl = strlen(name);
    for (int i = 0; suffixes[i]; i++) {
        size_t sl = strlen(suffixes[i]);
        if (nl > sl && strcmp(name + nl - sl, suffixes[i]) == 0) {
            char *out = malloc(nl - sl + SKIP_ONE);
            if (!out) {
                return NULL;
            }
            memcpy(out, name, nl - sl);
            out[nl - sl] = '\0';
            return out;
        }
    }
    return NULL;
}

/* Strip language-specific async wrappers from a method name in-place.
 * "GetVoucherAsync" → "GetVoucher". Leaves bare names unchanged. */
static void idl_strip_async_suffix(char *name) {
    if (!name) {
        return;
    }
    static const char *const k_async[] = {"Async", "_async", NULL};
    for (int i = 0; k_async[i]; i++) {
        size_t sl = strlen(k_async[i]);
        size_t nl = strlen(name);
        if (nl > sl && strcmp(name + nl - sl, k_async[i]) == 0) {
            name[nl - sl] = '\0';
            return;
        }
    }
}

static void idl_capitalize_first(char *s) {
    if (s && s[0] && islower((unsigned char)s[0])) {
        s[0] = (char)toupper((unsigned char)s[0]);
    }
}

static void idl_build_route_qn(char *buf, size_t bufsz, const char *service, const char *method) {
    snprintf(buf, bufsz, "__route__grpc__%s/%s", service, method);
}

/* Emit a single Route node + HANDLES edge from the rpc method node. */
static void idl_emit_route_for_rpc(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *service_node,
                                   const cbm_gbuf_node_t *rpc_node, int *out_count) {
    if (!service_node->name || !rpc_node->name) {
        return;
    }
    char qn[IDL_QN_BUF];
    idl_build_route_qn(qn, sizeof(qn), service_node->name, rpc_node->name);

    char display[IDL_QN_BUF];
    snprintf(display, sizeof(display), "%s/%s", service_node->name, rpc_node->name);

    char props[IDL_PROPS_BUF];
    snprintf(props, sizeof(props),
             "{\"protocol\":\"grpc\",\"service\":\"%s\",\"method\":\"%s\"}", service_node->name,
             rpc_node->name);

    int64_t route_id =
        cbm_gbuf_upsert_node(gbuf, "Route", display, qn,
                             service_node->file_path ? service_node->file_path : "", 0, 0, props);
    if (route_id <= 0) {
        return;
    }
    cbm_gbuf_insert_edge(gbuf, rpc_node->id, route_id, "HANDLES", "{\"via\":\"idl_grpc\"}");
    if (out_count) {
        (*out_count)++;
    }
}

/* Visitor state for the proto Class walk. */
typedef struct {
    cbm_gbuf_t *gbuf;
    int services;
    int routes;
} idl_walk_ctx_t;

static void idl_proto_class_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    idl_walk_ctx_t *ctx = (idl_walk_ctx_t *)userdata;
    if (!node || !node->label || !node->file_path) {
        return;
    }
    if (strcmp(node->label, "Class") != 0) {
        return;
    }
    if (!idl_is_proto_file(node->file_path)) {
        return;
    }
    /* Iterate DEFINES_METHOD edges to find rpc method nodes. */
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(ctx->gbuf, node->id, "DEFINES_METHOD", &edges,
                                           &edge_count) != 0) {
        return;
    }
    if (edge_count == 0) {
        return;
    }
    ctx->services++;
    for (int i = 0; i < edge_count; i++) {
        const cbm_gbuf_node_t *rpc = cbm_gbuf_find_by_id(ctx->gbuf, edges[i]->target_id);
        if (!rpc) {
            continue;
        }
        idl_emit_route_for_rpc(ctx->gbuf, node, rpc, &ctx->routes);
    }
}

/* Server-side base class suffixes — longest first so e.g. "GreeterImplBase"
 * matches "ImplBase" before falling through to "Base". Source-language coverage:
 *   Python grpcio:    *Servicer
 *   Java protoc-gen-grpc: *ImplBase
 *   C# Grpc.Tools:    *ServiceBase, *Base (matches the Grpc.Tools `<Service>.<Service>Base`)
 *   Rust tonic:       impl <S>Server for ... (handled by IMPLEMENTS, not INHERITS)
 *   Go grpc-go:       UnimplementedXXXServer (struct embedding) — out of scope for v1
 *
 * "Base" is intentionally last and shortest. False positives (e.g. inheriting from
 * a non-gRPC class that happens to end in "Base") are filtered downstream because
 * idl_bind_inheritance_edge only emits HANDLES when a Route node with the derived
 * service name actually exists in the gbuf.
 */
static const char *const k_grpc_server_suffixes[] = {
    "ServicerBase", "AsyncServicer", "ServiceBase", "ImplBase", "Servicer", "Base", NULL,
};

/* Given an inheritance edge (impl class → base class), if base name matches a
 * known gRPC server-stub suffix, bind methods of impl class to matching Routes.
 * Tries case-tolerant variants when looking up the Route QN to bridge naming
 * conventions across languages (snake_case vs CamelCase). */
static int idl_bind_inheritance_edge(cbm_gbuf_t *gbuf, const cbm_gbuf_edge_t *edge) {
    const cbm_gbuf_node_t *base = cbm_gbuf_find_by_id(gbuf, edge->target_id);
    if (!base || !base->name) {
        return 0;
    }
    char *service = idl_strip_suffix(base->name, k_grpc_server_suffixes);
    if (!service || !service[0]) {
        free(service);
        return 0;
    }
    const cbm_gbuf_node_t *impl = cbm_gbuf_find_by_id(gbuf, edge->source_id);
    if (!impl) {
        free(service);
        return 0;
    }

    int handles = 0;
    const cbm_gbuf_edge_t **method_edges = NULL;
    int method_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, impl->id, "DEFINES_METHOD", &method_edges,
                                           &method_count) != 0) {
        free(service);
        return 0;
    }

    for (int i = 0; i < method_count; i++) {
        const cbm_gbuf_node_t *m = cbm_gbuf_find_by_id(gbuf, method_edges[i]->target_id);
        if (!m || !m->name) {
            continue;
        }

        char bare[IDL_NAME_BUF];
        snprintf(bare, sizeof(bare), "%s", m->name);
        idl_strip_async_suffix(bare);

        char qn[IDL_QN_BUF];
        idl_build_route_qn(qn, sizeof(qn), service, bare);
        const cbm_gbuf_node_t *route = cbm_gbuf_find_by_qn(gbuf, qn);

        if (!route) {
            char cap[IDL_NAME_BUF];
            snprintf(cap, sizeof(cap), "%s", bare);
            idl_capitalize_first(cap);
            if (strcmp(cap, bare) != 0) {
                idl_build_route_qn(qn, sizeof(qn), service, cap);
                route = cbm_gbuf_find_by_qn(gbuf, qn);
            }
        }

        if (!route) {
            continue;
        }

        cbm_gbuf_insert_edge(gbuf, m->id, route->id, "HANDLES", "{\"via\":\"idl_grpc\"}");
        handles++;
    }

    free(service);
    return handles;
}

static int idl_bind_consumer_handlers(cbm_gbuf_t *gbuf) {
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_type(gbuf, "INHERITS", &edges, &edge_count) != 0) {
        return 0;
    }
    int handles = 0;
    for (int i = 0; i < edge_count; i++) {
        handles += idl_bind_inheritance_edge(gbuf, edges[i]);
    }
    return handles;
}

/* ── Producer-side typed-client detection ────────────────────────────
 *
 * Source-language coverage of stub/client suffixes (longest first):
 *   Java/Kotlin protoc-gen-grpc-java: <Service>BlockingStub, <Service>FutureStub
 *   Python grpcio:                    <Service>Stub
 *   C# Grpc.Tools:                    <Service>Client, <Service>AsyncClient
 *   Rust tonic:                       <Service>Client
 *
 * Go grpc-go uses pointer types like *PromoCodeClient produced by NewPromoCodeClient(conn);
 * extracting the type from the call expression rather than a typed assignment is feasible
 * but needs more plumbing — left as a follow-up.
 */
static const char *const k_grpc_client_suffixes[] = {
    "BlockingStub", "FutureStub", "AsyncStub", "AsyncClient", "Stub", "Client", NULL,
};

/* In-pass index of proto-derived service names (Class nodes from .proto files).
 * Built once during route emission so producer-side detection can validate the
 * derived service name actually corresponds to an indexed gRPC service. */
typedef struct {
    char **names;
    int count;
    int cap;
} idl_service_set_t;

static void idl_service_set_init(idl_service_set_t *s) {
    s->names = NULL;
    s->count = 0;
    s->cap = 0;
}

static void idl_service_set_add(idl_service_set_t *s, const char *name) {
    if (!name || !name[0]) {
        return;
    }
    for (int i = 0; i < s->count; i++) {
        if (strcmp(s->names[i], name) == 0) {
            return;
        }
    }
    if (s->count >= s->cap) {
        int new_cap = s->cap == 0 ? IDL_SVC_INIT_CAP : s->cap * 2;
        char **grow = realloc(s->names, (size_t)new_cap * sizeof(char *));
        if (!grow) {
            return;
        }
        s->names = grow;
        s->cap = new_cap;
    }
    s->names[s->count] = strdup(name);
    if (s->names[s->count]) {
        s->count++;
    }
}

static bool idl_service_set_contains(const idl_service_set_t *s, const char *name) {
    if (!name) {
        return false;
    }
    for (int i = 0; i < s->count; i++) {
        if (strcmp(s->names[i], name) == 0) {
            return true;
        }
    }
    return false;
}

static void idl_service_set_free(idl_service_set_t *s) {
    for (int i = 0; i < s->count; i++) {
        free(s->names[i]);
    }
    free(s->names);
    s->names = NULL;
    s->count = 0;
    s->cap = 0;
}

/* Per-function scoped record: var name → derived service name. */
typedef struct {
    char *enclosing_qn;
    char *var_name;
    char *service_name;
} idl_stub_var_t;

typedef struct {
    idl_stub_var_t *items;
    int count;
    int cap;
} idl_stub_var_arr_t;

static void idl_stub_var_arr_init(idl_stub_var_arr_t *a) {
    a->items = NULL;
    a->count = 0;
    a->cap = 0;
}

static void idl_stub_var_arr_push(idl_stub_var_arr_t *a, const char *enclosing_qn,
                                  const char *var_name, const char *service_name) {
    if (a->count >= a->cap) {
        int new_cap = a->cap == 0 ? IDL_VAR_INIT_CAP : a->cap * 2;
        idl_stub_var_t *grow = realloc(a->items, (size_t)new_cap * sizeof(idl_stub_var_t));
        if (!grow) {
            return;
        }
        a->items = grow;
        a->cap = new_cap;
    }
    idl_stub_var_t *e = &a->items[a->count];
    e->enclosing_qn = enclosing_qn ? strdup(enclosing_qn) : NULL;
    e->var_name = strdup(var_name);
    e->service_name = strdup(service_name);
    if (e->var_name && e->service_name) {
        a->count++;
    } else {
        free(e->enclosing_qn);
        free(e->var_name);
        free(e->service_name);
    }
}

static const idl_stub_var_t *idl_stub_var_arr_find(const idl_stub_var_arr_t *a,
                                                   const char *enclosing_qn, const char *var_name) {
    if (!var_name) {
        return NULL;
    }
    for (int i = 0; i < a->count; i++) {
        const idl_stub_var_t *e = &a->items[i];
        if (strcmp(e->var_name, var_name) != 0) {
            continue;
        }
        /* Require enclosing QN match when both sides specify one; allow a NULL
         * call-site enclosing to match any (module-scope variables). */
        if (enclosing_qn && e->enclosing_qn && strcmp(enclosing_qn, e->enclosing_qn) != 0) {
            continue;
        }
        return e;
    }
    return NULL;
}

static void idl_stub_var_arr_free(idl_stub_var_arr_t *a) {
    for (int i = 0; i < a->count; i++) {
        free(a->items[i].enclosing_qn);
        free(a->items[i].var_name);
        free(a->items[i].service_name);
    }
    free(a->items);
    a->items = NULL;
    a->count = 0;
    a->cap = 0;
}

/* Get the unqualified (basename) form of a possibly-qualified type name.
 * "promo_pb2_grpc.PromoCodeStub" → "PromoCodeStub". */
static const char *idl_type_basename(const char *qualified) {
    if (!qualified) {
        return NULL;
    }
    const char *dot = strrchr(qualified, '.');
    return dot ? dot + 1 : qualified;
}

/* Scan one CBMFileResult's type_assigns; for each assignment whose RHS type
 * matches a stub/client suffix AND the suffix-stripped base name matches a
 * known proto service, record (enclosing_qn, var_name, service_name). */
static void idl_collect_stub_vars_for_file(const CBMFileResult *result,
                                           const idl_service_set_t *known_services,
                                           idl_stub_var_arr_t *out) {
    if (!result) {
        return;
    }
    for (int i = 0; i < result->type_assigns.count; i++) {
        const CBMTypeAssign *ta = &result->type_assigns.items[i];
        if (!ta->var_name || !ta->type_name) {
            continue;
        }
        const char *base = idl_type_basename(ta->type_name);
        char *service = idl_strip_suffix(base, k_grpc_client_suffixes);
        if (!service || !service[0]) {
            free(service);
            continue;
        }
        if (!idl_service_set_contains(known_services, service)) {
            free(service);
            continue;
        }
        idl_stub_var_arr_push(out, ta->enclosing_func_qn, ta->var_name, service);
        free(service);
    }
}

/* Locate the caller node for a producer-side edge: prefer the enclosing function
 * QN's gbuf node, fall back to the file node. Mirrors pass_calls' calls_find_source. */
static const cbm_gbuf_node_t *idl_find_caller(cbm_pipeline_ctx_t *ctx, const char *rel_path,
                                              const char *enclosing_qn) {
    const cbm_gbuf_node_t *src = NULL;
    if (enclosing_qn && enclosing_qn[0]) {
        src = cbm_gbuf_find_by_qn(ctx->gbuf, enclosing_qn);
    }
    if (!src && rel_path) {
        char *fqn = cbm_pipeline_fqn_compute(ctx->project_name, rel_path, "__file__");
        if (fqn) {
            src = cbm_gbuf_find_by_qn(ctx->gbuf, fqn);
            free(fqn);
        }
    }
    return src;
}

/* Walk one file's calls and emit GRPC_CALLS edges for matched stub-var.method patterns. */
static int idl_emit_producer_edges_for_file(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *fi,
                                            const CBMFileResult *result,
                                            const idl_stub_var_arr_t *stub_vars) {
    if (!result || stub_vars->count == 0) {
        return 0;
    }
    int emitted = 0;
    for (int c = 0; c < result->calls.count; c++) {
        const CBMCall *call = &result->calls.items[c];
        if (!call->callee_name) {
            continue;
        }
        const char *dot = strchr(call->callee_name, '.');
        if (!dot || dot == call->callee_name) {
            continue;
        }
        size_t var_len = (size_t)(dot - call->callee_name);
        if (var_len == 0 || var_len >= IDL_NAME_BUF) {
            continue;
        }
        char var_buf[IDL_NAME_BUF];
        memcpy(var_buf, call->callee_name, var_len);
        var_buf[var_len] = '\0';

        const char *rest = dot + 1;
        if (!rest[0] || strchr(rest, '.') != NULL) {
            /* Skip multi-segment receivers like "self.client.Method" — out of scope for v1. */
            continue;
        }

        const idl_stub_var_t *stub =
            idl_stub_var_arr_find(stub_vars, call->enclosing_func_qn, var_buf);
        if (!stub) {
            continue;
        }

        char method_buf[IDL_NAME_BUF];
        snprintf(method_buf, sizeof(method_buf), "%s", rest);
        idl_strip_async_suffix(method_buf);
        idl_capitalize_first(method_buf);

        char route_qn[IDL_QN_BUF];
        idl_build_route_qn(route_qn, sizeof(route_qn), stub->service_name, method_buf);
        char display[IDL_QN_BUF];
        snprintf(display, sizeof(display), "%s/%s", stub->service_name, method_buf);
        char route_props[IDL_PROPS_BUF];
        snprintf(route_props, sizeof(route_props),
                 "{\"protocol\":\"grpc\",\"service\":\"%s\",\"method\":\"%s\"}", stub->service_name,
                 method_buf);
        int64_t route_id = cbm_gbuf_upsert_node(ctx->gbuf, "Route", display, route_qn, "", 0, 0,
                                                route_props);
        if (route_id <= 0) {
            continue;
        }

        const cbm_gbuf_node_t *caller =
            idl_find_caller(ctx, fi ? fi->rel_path : NULL, call->enclosing_func_qn);
        if (!caller) {
            continue;
        }

        char edge_props[IDL_PROPS_BUF];
        snprintf(edge_props, sizeof(edge_props),
                 "{\"service\":\"%s\",\"method\":\"%s\",\"via\":\"idl_grpc_stub\"}",
                 stub->service_name, method_buf);
        cbm_gbuf_insert_edge(ctx->gbuf, caller->id, route_id, "GRPC_CALLS", edge_props);
        emitted++;
    }
    return emitted;
}

/* Build the proto-service set by scanning Class nodes with .proto file_path. */
typedef struct {
    idl_service_set_t *set;
} idl_svc_collect_ctx_t;

static void idl_svc_collect_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    idl_svc_collect_ctx_t *c = (idl_svc_collect_ctx_t *)userdata;
    if (!node || !node->label || !node->file_path || !node->name) {
        return;
    }
    if (strcmp(node->label, "Class") == 0 && idl_is_proto_file(node->file_path)) {
        idl_service_set_add(c->set, node->name);
    }
}

static int idl_emit_producer_edges(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                   int file_count) {
    if (!ctx->result_cache || file_count <= 0) {
        return 0;
    }

    idl_service_set_t known_services;
    idl_service_set_init(&known_services);
    idl_svc_collect_ctx_t collect = {.set = &known_services};
    cbm_gbuf_foreach_node(ctx->gbuf, idl_svc_collect_visitor, &collect);

    int total_emitted = 0;
    if (known_services.count == 0) {
        idl_service_set_free(&known_services);
        return 0;
    }

    for (int i = 0; i < file_count; i++) {
        const CBMFileResult *result = ctx->result_cache[i];
        if (!result) {
            continue;
        }
        idl_stub_var_arr_t stub_vars;
        idl_stub_var_arr_init(&stub_vars);
        idl_collect_stub_vars_for_file(result, &known_services, &stub_vars);
        if (stub_vars.count > 0) {
            total_emitted += idl_emit_producer_edges_for_file(ctx, &files[i], result, &stub_vars);
        }
        idl_stub_var_arr_free(&stub_vars);
    }

    idl_service_set_free(&known_services);
    return total_emitted;
}

/* TLS-backed itoa for log calls. */
static const char *idl_itoa(int v) {
    static CBM_TLS char buf[IDL_LOG_BUF];
    snprintf(buf, sizeof(buf), "%d", v);
    return buf;
}

/* Public entry point. Idempotent: re-running over the same gbuf only adds the
 * same Route + HANDLES + GRPC_CALLS tuples (deduped by gbuf upsert/insert semantics). */
int cbm_pipeline_pass_idl_scan(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count) {
    if (!ctx || !ctx->gbuf) {
        return 0;
    }

    cbm_log_info("pass.start", "pass", "idl_scan");

    idl_walk_ctx_t walk = {.gbuf = ctx->gbuf, .services = 0, .routes = 0};
    cbm_gbuf_foreach_node(ctx->gbuf, idl_proto_class_visitor, &walk);

    int handles = idl_bind_consumer_handlers(ctx->gbuf);
    int grpc_calls = idl_emit_producer_edges(ctx, files, file_count);

    cbm_log_info("pass.done", "pass", "idl_scan", "services", idl_itoa(walk.services), "routes",
                 idl_itoa(walk.routes), "handles", idl_itoa(handles), "grpc_calls",
                 idl_itoa(grpc_calls));

    return 0;
}
