/*
 * pass_idl_scan.c — IDL-driven cross-repo binding for gRPC.
 *
 * Runs after pass_definitions / pass_calls. Two responsibilities:
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
 * The HANDLES edges are the rendezvous point for pass_cross_repo's existing
 * Phase D matcher (match_typed_routes for GRPC_CALLS), which already looks
 * up Routes by QN and follows HANDLES edges to find handlers.
 *
 * Producer-side typed gRPC client detection (emitting GRPC_CALLS edges) is
 * intentionally deferred to a follow-up pass — it requires call-site type
 * resolution that is not yet wired through the call resolution pipeline.
 *
 * Builds on the cross-repo scaffolding in pass_cross_repo.c without modifying it.
 */
#include "foundation/constants.h"

enum {
    IDL_QN_BUF = 768,
    IDL_PROPS_BUF = 256,
    IDL_NAME_BUF = 256,
    IDL_LOG_BUF = 16,
};

#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/compat.h"

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

/* TLS-backed itoa for log calls. */
static const char *idl_itoa(int v) {
    static CBM_TLS char buf[IDL_LOG_BUF];
    snprintf(buf, sizeof(buf), "%d", v);
    return buf;
}

/* Public entry point. Idempotent: re-running over the same gbuf only adds the
 * same Route + HANDLES tuples (deduped by gbuf upsert/insert semantics). */
int cbm_pipeline_pass_idl_scan(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count) {
    (void)files;
    (void)file_count;
    if (!ctx || !ctx->gbuf) {
        return 0;
    }

    cbm_log_info("pass.start", "pass", "idl_scan");

    idl_walk_ctx_t walk = {.gbuf = ctx->gbuf, .services = 0, .routes = 0};
    cbm_gbuf_foreach_node(ctx->gbuf, idl_proto_class_visitor, &walk);

    int handles = idl_bind_consumer_handlers(ctx->gbuf);

    cbm_log_info("pass.done", "pass", "idl_scan", "services", idl_itoa(walk.services), "routes",
                 idl_itoa(walk.routes), "handles", idl_itoa(handles));

    return 0;
}
