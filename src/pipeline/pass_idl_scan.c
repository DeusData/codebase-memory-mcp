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
    IDL_PARAM_MAX = 32,
    IDL_DI_INIT_CAP = 16,
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

/* Emit a single Route node + HANDLES edge from the rpc method node.
 *
 * Cross-package collision visibility (Gap 7 mitigation): when two .proto
 * files declare the same <service>/<method>, the second emission's upsert
 * silently overwrites the first's properties. Logs a warning at
 * idl_scan.route_collision when the existing Route's file_path differs,
 * and writes the service node's qualified_name as a service_qn property
 * so a future FQN-aware matcher (Tier 1g) can recover provenance even
 * after the upsert. */
static void idl_emit_route_for_rpc(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *service_node,
                                   const cbm_gbuf_node_t *rpc_node, int *out_count) {
    if (!service_node->name || !rpc_node->name) {
        return;
    }
    char qn[IDL_QN_BUF];
    idl_build_route_qn(qn, sizeof(qn), service_node->name, rpc_node->name);

    char display[IDL_QN_BUF];
    snprintf(display, sizeof(display), "%s/%s", service_node->name, rpc_node->name);

    const char *src_file = service_node->file_path ? service_node->file_path : "";

    /* Detect cross-package collision via file_path mismatch on existing
     * Route. Useful as an operator signal; does not change emission. */
    const cbm_gbuf_node_t *existing = cbm_gbuf_find_by_qn(gbuf, qn);
    if (existing && existing->file_path && existing->file_path[0] && src_file[0] &&
        strcmp(existing->file_path, src_file) != 0) {
        cbm_log_warn("idl_scan.route_collision", "qn", qn, "first_file", existing->file_path,
                     "second_file", src_file);
    }

    char props[IDL_PROPS_BUF];
    snprintf(props, sizeof(props),
             "{\"protocol\":\"grpc\",\"service\":\"%s\",\"method\":\"%s\",\"service_qn\":\"%s\"}",
             service_node->name, rpc_node->name,
             service_node->qualified_name ? service_node->qualified_name : service_node->name);

    int64_t route_id = cbm_gbuf_upsert_node(gbuf, "Route", display, qn, src_file, 0, 0, props);
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

/* Pre-collected per-pass index of proto-file nodes. Built in a single O(M)
 * walk before per-Class processing so the fallback rpc-by-line-range match
 * stays linear in the number of proto Functions, not quadratic in gbuf size. */
typedef struct {
    const cbm_gbuf_node_t **classes;
    int class_count;
    int class_cap;
    const cbm_gbuf_node_t **functions;
    int func_count;
    int func_cap;
} idl_proto_index_t;

static void idl_proto_index_init(idl_proto_index_t *idx) {
    memset(idx, 0, sizeof(*idx));
}

static void idl_proto_index_free(idl_proto_index_t *idx) {
    free(idx->classes);
    free(idx->functions);
    memset(idx, 0, sizeof(*idx));
}

static void idl_proto_index_push_class(idl_proto_index_t *idx, const cbm_gbuf_node_t *node) {
    if (idx->class_count >= idx->class_cap) {
        int nc = idx->class_cap == 0 ? IDL_VAR_INIT_CAP : idx->class_cap * 2;
        const cbm_gbuf_node_t **g = realloc(idx->classes, (size_t)nc * sizeof(*idx->classes));
        if (!g) return;
        idx->classes = g;
        idx->class_cap = nc;
    }
    idx->classes[idx->class_count++] = node;
}

static void idl_proto_index_push_function(idl_proto_index_t *idx, const cbm_gbuf_node_t *node) {
    if (idx->func_count >= idx->func_cap) {
        int nc = idx->func_cap == 0 ? IDL_VAR_INIT_CAP : idx->func_cap * 2;
        const cbm_gbuf_node_t **g = realloc(idx->functions, (size_t)nc * sizeof(*idx->functions));
        if (!g) return;
        idx->functions = g;
        idx->func_cap = nc;
    }
    idx->functions[idx->func_count++] = node;
}

static void idl_proto_index_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    idl_proto_index_t *idx = (idl_proto_index_t *)userdata;
    if (!node || !node->label || !node->file_path) {
        return;
    }
    if (!idl_is_proto_file(node->file_path)) {
        return;
    }
    if (strcmp(node->label, "Class") == 0) {
        idl_proto_index_push_class(idx, node);
    } else if (strcmp(node->label, "Function") == 0) {
        idl_proto_index_push_function(idx, node);
    }
}

/* Process one proto service Class. Tries DEFINES_METHOD edges first; falls
 * back to file_path + line-range scan over the pre-collected proto Functions
 * (typical for tree-sitter-protobuf, which emits rpc Functions as flat
 * siblings of the service Class rather than children). */
static void idl_emit_routes_for_proto_class(cbm_gbuf_t *gbuf, const cbm_gbuf_node_t *cls,
                                            const idl_proto_index_t *idx, int *services,
                                            int *routes) {
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (cbm_gbuf_find_edges_by_source_type(gbuf, cls->id, "DEFINES_METHOD", &edges, &edge_count) ==
            0 &&
        edge_count > 0) {
        (*services)++;
        for (int i = 0; i < edge_count; i++) {
            const cbm_gbuf_node_t *rpc = cbm_gbuf_find_by_id(gbuf, edges[i]->target_id);
            if (rpc) {
                idl_emit_route_for_rpc(gbuf, cls, rpc, routes);
            }
        }
        return;
    }

    /* Fallback: filter pre-collected proto Functions by same file_path +
     * line-range containment. O(F) per class; F is small (proto-file rpcs). */
    int matched = 0;
    for (int i = 0; i < idx->func_count; i++) {
        const cbm_gbuf_node_t *fn = idx->functions[i];
        if (!fn->file_path || strcmp(fn->file_path, cls->file_path) != 0) {
            continue;
        }
        if (fn->start_line < cls->start_line || fn->end_line > cls->end_line) {
            continue;
        }
        if (matched == 0) {
            (*services)++;
        }
        matched++;
        idl_emit_route_for_rpc(gbuf, cls, fn, routes);
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
 *
 * Note: producer-side detection runs WITHOUT requiring a local .proto file.
 * In real microservice fleets, contracts are commonly distributed via
 * NuGet/Maven/PyPI packages (e.g. `Snoonu.PromoCodeService.Contracts`) so the
 * consumer never has the .proto in source. pass_cross_repo's Phase D matches
 * GRPC_CALLS edges against Route nodes in TARGET stores, so consumer-side
 * indexing of the contracts repo (or wherever the .proto lives) is what
 * actually closes the loop. False-positive surface is limited by:
 *   1. Suffix shape (BlockingStub/FutureStub/Stub are gRPC-conventional)
 *   2. Type-name denylist (System.Net.*, Microsoft.Extensions.Http, Refit, ...)
 *   3. Phase D filter — non-matching GRPC_CALLS produce no CROSS_GRPC_CALLS
 */
static const char *const k_grpc_client_suffixes[] = {
    "BlockingStub", "FutureStub", "AsyncStub", "AsyncClient", "Stub", "Client", NULL,
};

/* Type-name prefixes that look like client/stub suffixes but are definitively
 * NOT gRPC. Matched as substring against the (possibly qualified) type_name.
 * Keep the list short; Phase D filters anything that slips through. */
static const char *const k_non_grpc_type_markers[] = {
    "System.Net.",          /* HttpClient, WebClient, TcpClient, etc. */
    "System.Web.",          /* legacy WebForms / WebClient */
    "Microsoft.Extensions.Http", /* IHttpClientFactory, HttpClient DI */
    "RestSharp",            /* REST client, not gRPC */
    "Refit",                /* attribute-routed REST client */
    "Flurl",                /* fluent HTTP client */
    "java.net.http",        /* JDK HttpClient */
    "okhttp3",              /* OkHttp */
    "reqwest",              /* Rust HTTP */
    "urllib",               /* Python HTTP */
    "httpx",                /* Python HTTP */
    NULL,
};

static bool idl_type_is_denylisted(const char *type_name) {
    if (!type_name) {
        return false;
    }
    for (int i = 0; k_non_grpc_type_markers[i]; i++) {
        if (strstr(type_name, k_non_grpc_type_markers[i]) != NULL) {
            return true;
        }
    }
    return false;
}

/* Var → service map entry. Scope is determined by which fields are populated:
 *
 *   function-scope: enclosing_qn set, class_qn empty.
 *       Matches when a call's enclosing_func_qn equals enclosing_qn exactly.
 *       Source: type_assigns (Tier 1b), local var = SomeStub(...).
 *
 *   class-scope: class_qn set, enclosing_qn empty.
 *       Matches when a call's enclosing_func_qn starts with class_qn + ".".
 *       Source: ctor params (Tier 1c) + class fields (Tier 1f).
 *
 *   file-scope fallback: any var match within an array attached to one file.
 *       Used when both function and class scope miss but the call's file's
 *       per-file array has the var name. Same as #293 behavior.
 */
typedef struct {
    char *enclosing_qn;
    char *class_qn;
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

static void idl_stub_var_arr_push_scoped(idl_stub_var_arr_t *a, const char *enclosing_qn,
                                         const char *class_qn, const char *var_name,
                                         const char *service_name) {
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
    e->enclosing_qn = (enclosing_qn && enclosing_qn[0]) ? strdup(enclosing_qn) : NULL;
    e->class_qn = (class_qn && class_qn[0]) ? strdup(class_qn) : NULL;
    e->var_name = strdup(var_name);
    e->service_name = strdup(service_name);
    if (e->var_name && e->service_name) {
        a->count++;
    } else {
        free(e->enclosing_qn);
        free(e->class_qn);
        free(e->var_name);
        free(e->service_name);
    }
}

static void idl_stub_var_arr_push(idl_stub_var_arr_t *a, const char *enclosing_qn,
                                  const char *var_name, const char *service_name) {
    idl_stub_var_arr_push_scoped(a, enclosing_qn, NULL, var_name, service_name);
}

/* True when call_qn is "<class_qn>.<something>" — i.e., the call's enclosing
 * function is a method of the given class. Matches both single-method and
 * nested-scope cases. */
static bool idl_qn_in_class(const char *call_qn, const char *class_qn) {
    if (!call_qn || !class_qn || !class_qn[0]) {
        return false;
    }
    size_t cl = strlen(class_qn);
    if (strncmp(call_qn, class_qn, cl) != 0) {
        return false;
    }
    return call_qn[cl] == '.';
}

/* Look up a stub var with scope priority:
 *   1. function-scope exact match
 *   2. class-scope (call enclosing is method of var's class)
 *   3. file-scope fallback (any matching var name in this array)
 */
/* allow_name_only_fallback:
 *   true  — Pass 3 (name-only match) is allowed. Safe for per-file arrays
 *           where every entry is in the caller's translation unit and the
 *           worst-case false positive stays inside one file.
 *   false — Pass 3 is suppressed. Required for project-wide arrays
 *           (class_vars). Without a class/file check, a bare name match
 *           could bind a `_client` call to an unrelated class's stub and
 *           emit a wrong (CROSS_)GRPC_CALLS edge.
 */
static const idl_stub_var_t *idl_stub_var_arr_find_ext(const idl_stub_var_arr_t *a,
                                                       const char *enclosing_qn,
                                                       const char *var_name,
                                                       bool allow_name_only_fallback) {
    if (!var_name) {
        return NULL;
    }
    /* Pass 1: function-scope exact match. */
    if (enclosing_qn && enclosing_qn[0]) {
        for (int i = 0; i < a->count; i++) {
            const idl_stub_var_t *e = &a->items[i];
            if (strcmp(e->var_name, var_name) == 0 && e->enclosing_qn &&
                strcmp(enclosing_qn, e->enclosing_qn) == 0) {
                return e;
            }
        }
    }
    /* Pass 2: class-scope (var declared on the class whose method is calling). */
    if (enclosing_qn && enclosing_qn[0]) {
        for (int i = 0; i < a->count; i++) {
            const idl_stub_var_t *e = &a->items[i];
            if (strcmp(e->var_name, var_name) == 0 && e->class_qn &&
                idl_qn_in_class(enclosing_qn, e->class_qn)) {
                return e;
            }
        }
    }
    /* Pass 3: name-only fallback — only when the array is known to be
     * single-file-scope. Project-wide arrays must fail closed here. */
    if (allow_name_only_fallback) {
        for (int i = 0; i < a->count; i++) {
            const idl_stub_var_t *e = &a->items[i];
            if (strcmp(e->var_name, var_name) == 0) {
                return e;
            }
        }
    }
    return NULL;
}

static const idl_stub_var_t *idl_stub_var_arr_find(const idl_stub_var_arr_t *a,
                                                   const char *enclosing_qn, const char *var_name) {
    return idl_stub_var_arr_find_ext(a, enclosing_qn, var_name, true);
}

static void idl_stub_var_arr_free(idl_stub_var_arr_t *a) {
    for (int i = 0; i < a->count; i++) {
        free(a->items[i].enclosing_qn);
        free(a->items[i].class_qn);
        free(a->items[i].var_name);
        free(a->items[i].service_name);
    }
    free(a->items);
    a->items = NULL;
    a->count = 0;
    a->cap = 0;
}

/* DI-registered stub-type registry (Tier 1e). Holds FQNs or basenames of types
 * that have been positively identified as gRPC stubs even when they don't carry
 * the conventional Stub/Client suffix. Populated by scanning AddGrpcClient<T>
 * call sites and @GrpcClient annotations. Consumed by Tier 1c/1f to upgrade
 * non-suffix types into stub vars. */
typedef struct {
    char **types;
    int count;
    int cap;
} idl_di_registry_t;

static void idl_di_registry_init(idl_di_registry_t *r) {
    r->types = NULL;
    r->count = 0;
    r->cap = 0;
}

static void idl_di_registry_free(idl_di_registry_t *r) {
    for (int i = 0; i < r->count; i++) {
        free(r->types[i]);
    }
    free(r->types);
    r->types = NULL;
    r->count = 0;
    r->cap = 0;
}

static void idl_di_registry_add(idl_di_registry_t *r, const char *type_name) {
    if (!type_name || !type_name[0]) {
        return;
    }
    for (int i = 0; i < r->count; i++) {
        if (strcmp(r->types[i], type_name) == 0) {
            return;
        }
    }
    if (r->count >= r->cap) {
        int new_cap = r->cap == 0 ? IDL_DI_INIT_CAP : r->cap * 2;
        char **grow = realloc(r->types, (size_t)new_cap * sizeof(char *));
        if (!grow) {
            return;
        }
        r->types = grow;
        r->cap = new_cap;
    }
    r->types[r->count++] = strdup(type_name);
}

static bool idl_di_registry_contains(const idl_di_registry_t *r, const char *type_name) {
    if (!r || !type_name) {
        return false;
    }
    for (int i = 0; i < r->count; i++) {
        if (strcmp(r->types[i], type_name) == 0) {
            return true;
        }
    }
    return false;
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

/* Tier 1d — Factory function return-type inference.
 *
 * Resolve a service name from a factory call's qualified text. Handles:
 *   Java/Kotlin: "FooGrpc.newBlockingStub" → "Foo"
 *                "FooGrpc.newFutureStub"  → "Foo"
 *                "FooGrpc.newAsyncStub"   → "Foo"
 *                "FooGrpc.newStub"        → "Foo"
 *   Go:          "pb.NewFooClient"        → "Foo"
 *                "client.NewFooClient"    → "Foo"
 *   Rust:        "FooClient.connect"      → handled by suffix path, not here
 *
 * Returns heap-allocated service name or NULL on miss. Caller frees. */
static char *idl_factory_to_service(const char *qualified_type) {
    if (!qualified_type) {
        return NULL;
    }
    const char *dot = strrchr(qualified_type, '.');
    const char *factory = dot ? dot + 1 : qualified_type;
    size_t pkg_len = dot ? (size_t)(dot - qualified_type) : 0;

    /* Strip "New" / "new" prefix from factory name. */
    const char *body = NULL;
    if (strncmp(factory, "New", 3) == 0 && factory[3]) {
        body = factory + 3;
    } else if (strncmp(factory, "new", 3) == 0 && factory[3]) {
        body = factory + 3;
    } else {
        return NULL;
    }

    /* Case A: body has a stub/client suffix where prefix carries the service.
     * Go: "FooClient" → "Foo". */
    char *stripped = idl_strip_suffix(body, k_grpc_client_suffixes);
    if (stripped && stripped[0]) {
        return stripped;
    }
    free(stripped);

    /* Case B: body is the raw stub kind ("BlockingStub"/"FutureStub"/...).
     * Service lives in the package prefix. Java: "FooGrpc.newBlockingStub". */
    if (pkg_len == 0) {
        return NULL;
    }
    bool body_is_pure_suffix = false;
    for (int i = 0; k_grpc_client_suffixes[i]; i++) {
        if (strcmp(body, k_grpc_client_suffixes[i]) == 0) {
            body_is_pure_suffix = true;
            break;
        }
    }
    if (!body_is_pure_suffix) {
        return NULL;
    }
    char pkg[IDL_NAME_BUF];
    size_t copy = pkg_len < sizeof(pkg) - 1 ? pkg_len : sizeof(pkg) - 1;
    memcpy(pkg, qualified_type, copy);
    pkg[copy] = '\0';
    /* Take last segment if pkg is itself qualified ("com.foo.FooGrpc" → "FooGrpc"). */
    char *last_seg = strrchr(pkg, '.');
    const char *seg = last_seg ? last_seg + 1 : pkg;
    static const char *const k_pkg_suffixes[] = {"Grpc", NULL};
    char *svc = idl_strip_suffix(seg, k_pkg_suffixes);
    if (svc && svc[0]) {
        return svc;
    }
    free(svc);
    return NULL;
}

/* Extract a single JSON string property from properties_json. Returns buf on
 * success or NULL on miss. Mirrors json_str_value in pass_semantic_edges.c. */
static const char *idl_json_string(const char *json, const char *key, char *buf, size_t bufsize) {
    if (!json || !key || !buf || bufsize == 0) {
        return NULL;
    }
    char pat[64];
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
    if (len >= bufsize) {
        len = bufsize - 1;
    }
    memcpy(buf, start, len);
    buf[len] = '\0';
    return buf;
}

/* Extract a JSON string array. Caller frees out[i] on success.
 * Returns count, 0 on miss. */
static int idl_json_str_array(const char *json, const char *key, char **out, int max_out) {
    if (!json || !key || !out || max_out <= 0) {
        return 0;
    }
    char pat[64];
    snprintf(pat, sizeof(pat), "\"%s\":[", key);
    const char *start = strstr(json, pat);
    if (!start) {
        return 0;
    }
    start += strlen(pat);
    int count = 0;
    while (*start && *start != ']' && count < max_out) {
        if (*start == '"') {
            start++;
            const char *end = strchr(start, '"');
            if (!end) {
                break;
            }
            size_t len = (size_t)(end - start);
            char *s = malloc(len + 1);
            if (!s) {
                break;
            }
            memcpy(s, start, len);
            s[len] = '\0';
            out[count++] = s;
            start = end + 1;
        } else {
            start++;
        }
    }
    return count;
}

static void idl_free_str_array(char **arr, int count) {
    for (int i = 0; i < count; i++) {
        free(arr[i]);
    }
}

/* Last segment of a dotted QN. "pkg.Foo" → "Foo". Returns pointer into qn. */
static const char *idl_qn_basename(const char *qn) {
    if (!qn) {
        return NULL;
    }
    const char *dot = strrchr(qn, '.');
    return dot ? dot + 1 : qn;
}

/* Decide whether type_name should register a stub var. Returns heap-allocated
 * service name or NULL. Caller frees.
 *
 * Resolution order:
 *   1. denylist → reject
 *   2. DI registry exact match → service = type basename (already known stub)
 *   3. factory pattern → service derived from factory name/package
 *   4. suffix match → service = prefix before suffix
 */
static char *idl_type_to_service(const char *type_name, const idl_di_registry_t *di_registry) {
    if (!type_name) {
        return NULL;
    }
    if (idl_type_is_denylisted(type_name)) {
        return NULL;
    }
    const char *base = idl_type_basename(type_name);
    /* DI registry hit: emit raw basename as service even if no suffix matched. */
    if (di_registry && (idl_di_registry_contains(di_registry, type_name) ||
                        idl_di_registry_contains(di_registry, base))) {
        char *stripped = idl_strip_suffix(base, k_grpc_client_suffixes);
        if (stripped && stripped[0]) {
            return stripped;
        }
        free(stripped);
        return strdup(base);
    }
    /* Factory pattern (Tier 1d). */
    char *factory_svc = idl_factory_to_service(type_name);
    if (factory_svc && factory_svc[0]) {
        return factory_svc;
    }
    free(factory_svc);
    /* Default: suffix strip. */
    char *suffix_svc = idl_strip_suffix(base, k_grpc_client_suffixes);
    if (suffix_svc && suffix_svc[0]) {
        return suffix_svc;
    }
    free(suffix_svc);
    return NULL;
}

/* Scan one CBMFileResult's type_assigns; for each assignment whose RHS type
 * matches a stub/client suffix and is not denylisted, record
 * (enclosing_qn, var_name, service_name).
 *
 * Detection runs WITHOUT requiring a local Route in the gbuf: in real
 * microservice fleets contracts ship via NuGet/Maven/PyPI and the producer
 * repo never has the .proto in source. Phase D in pass_cross_repo.c handles
 * the actual cross-repo match; non-matching GRPC_CALLS edges produced here
 * are inert (no CROSS_GRPC_CALLS) and cost is one stray local Route per
 * unique stub type. The denylist (k_non_grpc_type_markers) cuts off the
 * obvious false positives like System.Net.Http.HttpClient. */
static void idl_collect_stub_vars_for_file(const CBMFileResult *result,
                                           const idl_di_registry_t *di_registry,
                                           idl_stub_var_arr_t *out) {
    if (!result) {
        return;
    }
    for (int i = 0; i < result->type_assigns.count; i++) {
        const CBMTypeAssign *ta = &result->type_assigns.items[i];
        if (!ta->var_name || !ta->type_name) {
            continue;
        }
        char *service = idl_type_to_service(ta->type_name, di_registry);
        if (!service || !service[0]) {
            free(service);
            continue;
        }
        idl_stub_var_arr_push(out, ta->enclosing_func_qn, ta->var_name, service);
        free(service);
    }
}

/* Tier 1e — Scan calls for DI-registration patterns and harvest stub-type
 * generic args. Currently handles:
 *   C# `services.AddGrpcClient<FooClient>(...)` — generic type arg appears
 *       inside the callee_name text as "AddGrpcClient<FooClient>".
 *   C# `services.AddGrpcClientFactory<FooClient>` and similar variants
 *       caught by the substring search.
 *
 * Spring `@GrpcClient("name")` annotations on fields are picked up by the
 * field walker via decorators (Tier 1f) so they don't need a separate path
 * here. */
static void idl_extract_di_generic_arg(const char *callee_name, idl_di_registry_t *out) {
    if (!callee_name) {
        return;
    }
    const char *anchor = strstr(callee_name, "AddGrpcClient");
    if (!anchor) {
        return;
    }
    const char *lt = strchr(anchor, '<');
    if (!lt) {
        return;
    }
    const char *gt = strchr(lt, '>');
    if (!gt || gt <= lt + 1) {
        return;
    }
    char type_buf[IDL_NAME_BUF];
    size_t len = (size_t)(gt - lt - 1);
    if (len >= sizeof(type_buf)) {
        len = sizeof(type_buf) - 1;
    }
    memcpy(type_buf, lt + 1, len);
    type_buf[len] = '\0';
    /* Trim whitespace and any pointer/ref artifacts. */
    char *p = type_buf;
    while (*p == ' ' || *p == '*' || *p == '&') {
        p++;
    }
    char *end = p + strlen(p);
    while (end > p && (end[-1] == ' ' || end[-1] == ',')) {
        *--end = '\0';
    }
    if (!*p) {
        return;
    }
    idl_di_registry_add(out, p);
    /* Also register the basename (last segment) for non-FQN matches. */
    const char *base = idl_type_basename(p);
    if (base && base != p) {
        idl_di_registry_add(out, base);
    }
}

static void idl_collect_di_registry(const CBMFileResult *const *results, int file_count,
                                    idl_di_registry_t *out) {
    if (!results) {
        return;
    }
    for (int f = 0; f < file_count; f++) {
        const CBMFileResult *r = results[f];
        if (!r) {
            continue;
        }
        for (int i = 0; i < r->calls.count; i++) {
            const CBMCall *c = &r->calls.items[i];
            idl_extract_di_generic_arg(c->callee_name, out);
        }
        /* Also register field decorator-driven Spring/NestJS clients via
         * the field walker — see idl_collect_class_scope_stubs. */
    }
}

/* True when method name is a constructor for parent_class.
 *   Python: "__init__"
 *   JS/TS:  "constructor"
 *   C#/Java/Kotlin: name == basename(parent_class_qn)
 */
static bool idl_is_constructor(const char *method_name, const char *parent_class_qn) {
    if (!method_name) {
        return false;
    }
    if (strcmp(method_name, "__init__") == 0 || strcmp(method_name, "constructor") == 0) {
        return true;
    }
    if (!parent_class_qn) {
        return false;
    }
    const char *cls = idl_qn_basename(parent_class_qn);
    return cls && strcmp(method_name, cls) == 0;
}

/* True when decorator string smells like a Spring/NestJS gRPC client
 * annotation. The decorator strings come from CBMDefinition.decorators which
 * preserves the leading '@' for Java/Kotlin and the call-form text for TS. */
static bool idl_decorator_marks_grpc_client(const char *decorator) {
    if (!decorator) {
        return false;
    }
    if (strstr(decorator, "GrpcClient") || strstr(decorator, "grpcClient")) {
        return true;
    }
    if (strstr(decorator, "@Client") || strstr(decorator, "ClientGrpc")) {
        return true;
    }
    return false;
}

/* Tier 1c — Walk Method nodes; for each constructor-shaped method, harvest
 * stub-typed parameters and register them as class-scope vars.
 * Tier 1f — Walk Field nodes and register stub-typed fields class-scoped. */
typedef struct {
    idl_stub_var_arr_t *out;
    const idl_di_registry_t *di_registry;
    int ctor_params;
    int fields;
} idl_class_scope_ctx_t;

static void idl_class_scope_visitor(const cbm_gbuf_node_t *node, void *userdata) {
    idl_class_scope_ctx_t *ctx = (idl_class_scope_ctx_t *)userdata;
    if (!node || !node->label || !node->properties_json) {
        return;
    }
    const char *props = node->properties_json;

    char parent_buf[IDL_QN_BUF];
    const char *parent_class = idl_json_string(props, "parent_class", parent_buf, sizeof(parent_buf));

    if (strcmp(node->label, "Method") == 0) {
        if (!parent_class || !idl_is_constructor(node->name, parent_class)) {
            return;
        }
        char *names[IDL_PARAM_MAX];
        char *types[IDL_PARAM_MAX];
        int nname = idl_json_str_array(props, "param_names", names, IDL_PARAM_MAX);
        int ntype = idl_json_str_array(props, "param_types", types, IDL_PARAM_MAX);
        int n = nname < ntype ? nname : ntype;
        for (int i = 0; i < n; i++) {
            char *service = idl_type_to_service(types[i], ctx->di_registry);
            if (service && service[0]) {
                idl_stub_var_arr_push_scoped(ctx->out, NULL, parent_class, names[i], service);
                ctx->ctor_params++;
            }
            free(service);
        }
        idl_free_str_array(names, nname);
        idl_free_str_array(types, ntype);
        return;
    }

    if (strcmp(node->label, "Field") == 0) {
        if (!parent_class || !node->name) {
            return;
        }
        char type_buf[IDL_NAME_BUF];
        const char *field_type = idl_json_string(props, "return_type", type_buf, sizeof(type_buf));
        bool decorator_hit = false;
        char *decos[IDL_PARAM_MAX];
        int dc = idl_json_str_array(props, "decorators", decos, IDL_PARAM_MAX);
        for (int i = 0; i < dc; i++) {
            if (idl_decorator_marks_grpc_client(decos[i])) {
                decorator_hit = true;
            }
        }
        if (!field_type) {
            idl_free_str_array(decos, dc);
            return;
        }
        char *service = idl_type_to_service(field_type, ctx->di_registry);
        if (!service && decorator_hit) {
            /* Spring/NestJS @GrpcClient on a field whose type doesn't follow
             * the conventional Stub/Client suffix. The annotation itself is
             * authoritative — derive service from the type basename. */
            const char *base = idl_type_basename(field_type);
            char *stripped = idl_strip_suffix(base, k_grpc_client_suffixes);
            if (stripped && stripped[0]) {
                service = stripped;
            } else {
                free(stripped);
                service = strdup(base);
            }
        }
        if (service && service[0]) {
            idl_stub_var_arr_push_scoped(ctx->out, NULL, parent_class, node->name, service);
            ctx->fields++;
        }
        free(service);
        idl_free_str_array(decos, dc);
    }
}

static void idl_collect_class_scope_stubs(cbm_gbuf_t *gbuf, const idl_di_registry_t *di_registry,
                                          idl_stub_var_arr_t *out, int *ctor_count,
                                          int *field_count) {
    idl_class_scope_ctx_t ctx = {
        .out = out, .di_registry = di_registry, .ctor_params = 0, .fields = 0};
    cbm_gbuf_foreach_node(gbuf, idl_class_scope_visitor, &ctx);
    if (ctor_count) {
        *ctor_count = ctx.ctor_params;
    }
    if (field_count) {
        *field_count = ctx.fields;
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

/* Walk one file's calls and emit GRPC_CALLS edges for matched stub-var.method
 * patterns. file_vars holds per-file (function/file-scope) entries; class_vars
 * holds project-wide class-scope entries from Tier 1c/1f. */
static int idl_emit_producer_edges_for_file(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *fi,
                                            const CBMFileResult *result,
                                            const idl_stub_var_arr_t *file_vars,
                                            const idl_stub_var_arr_t *class_vars) {
    if (!result) {
        return 0;
    }
    if (file_vars->count == 0 && (!class_vars || class_vars->count == 0)) {
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
            idl_stub_var_arr_find(file_vars, call->enclosing_func_qn, var_buf);
        if (!stub && class_vars) {
            /* class_vars is project-wide. Disallow the name-only fallback so
             * a stray `_client` field on an unrelated class can't silently
             * bind this call. Pass 1/2 still match when the call's enclosing
             * function lives under the var's class. */
            stub = idl_stub_var_arr_find_ext(class_vars, call->enclosing_func_qn, var_buf, false);
        }
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

        /* If the proto Class emission (Tier 1g) already created this Route
         * with rich properties (proto_package, key_kind, etc.), reuse the
         * existing node — upsert overwrites properties last-write-wins,
         * which would drop the FQN provenance. The consumer-side emission
         * only needs the Route id for the GRPC_CALLS edge. */
        const cbm_gbuf_node_t *existing = cbm_gbuf_find_by_qn(ctx->gbuf, route_qn);
        int64_t route_id;
        if (existing) {
            route_id = existing->id;
        } else {
            char route_props[IDL_PROPS_BUF];
            snprintf(route_props, sizeof(route_props),
                     "{\"protocol\":\"grpc\",\"service\":\"%s\",\"method\":\"%s\","
                     "\"key_kind\":\"bare\"}",
                     stub->service_name, method_buf);
            route_id = cbm_gbuf_upsert_node(ctx->gbuf, "Route", display, route_qn, "", 0, 0,
                                            route_props);
        }
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

static int idl_emit_producer_edges(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                   int file_count, int *ctor_count_out, int *field_count_out,
                                   int *di_count_out) {
    if (!ctx->result_cache || file_count <= 0) {
        return 0;
    }

    /* Tier 1e: walk all calls once to harvest DI-registered stub types. */
    idl_di_registry_t di_registry;
    idl_di_registry_init(&di_registry);
    idl_collect_di_registry((const CBMFileResult *const *)ctx->result_cache, file_count,
                            &di_registry);
    if (di_count_out) {
        *di_count_out = di_registry.count;
    }

    /* Tier 1c + 1f: walk gbuf for ctor params + class fields. Class-scope vars
     * apply to any method whose enclosing_func_qn lives under the class. */
    idl_stub_var_arr_t class_vars;
    idl_stub_var_arr_init(&class_vars);
    idl_collect_class_scope_stubs(ctx->gbuf, &di_registry, &class_vars, ctor_count_out,
                                  field_count_out);

    int total_emitted = 0;
    for (int i = 0; i < file_count; i++) {
        const CBMFileResult *result = ctx->result_cache[i];
        if (!result) {
            continue;
        }
        idl_stub_var_arr_t file_vars;
        idl_stub_var_arr_init(&file_vars);
        idl_collect_stub_vars_for_file(result, &di_registry, &file_vars);
        if (file_vars.count > 0 || class_vars.count > 0) {
            total_emitted +=
                idl_emit_producer_edges_for_file(ctx, &files[i], result, &file_vars, &class_vars);
        }
        idl_stub_var_arr_free(&file_vars);
    }

    idl_stub_var_arr_free(&class_vars);
    idl_di_registry_free(&di_registry);
    return total_emitted;
}

/* Public entry point. Idempotent: re-running over the same gbuf only adds the
 * same Route + HANDLES + GRPC_CALLS tuples (deduped by gbuf upsert/insert semantics). */
int cbm_pipeline_pass_idl_scan(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count) {
    if (!ctx || !ctx->gbuf) {
        return 0;
    }

    cbm_log_info("pass.start", "pass", "idl_scan");

    /* Single O(M) walk to collect proto Classes + proto Functions. Subsequent
     * per-class processing is O(F) where F is the count of proto Functions —
     * keeps the pass linear in graph size even on monorepos with hundreds of
     * service-bearing .proto files. */
    idl_proto_index_t proto_idx;
    idl_proto_index_init(&proto_idx);
    cbm_gbuf_foreach_node(ctx->gbuf, idl_proto_index_visitor, &proto_idx);
    idl_walk_ctx_t walk = {.gbuf = ctx->gbuf, .services = 0, .routes = 0};
    for (int i = 0; i < proto_idx.class_count; i++) {
        idl_emit_routes_for_proto_class(ctx->gbuf, proto_idx.classes[i], &proto_idx,
                                        &walk.services, &walk.routes);
    }
    idl_proto_index_free(&proto_idx);

    int handles = idl_bind_consumer_handlers(ctx->gbuf);
    int ctor_count = 0;
    int field_count = 0;
    int di_count = 0;
    int grpc_calls =
        idl_emit_producer_edges(ctx, files, file_count, &ctor_count, &field_count, &di_count);

    /* Per-call stack buffers — idl_itoa shares a single TLS buffer that gets
     * clobbered when multiple calls share one log statement. */
    char b_svc[IDL_LOG_BUF];
    char b_rt[IDL_LOG_BUF];
    char b_h[IDL_LOG_BUF];
    char b_g[IDL_LOG_BUF];
    char b_ct[IDL_LOG_BUF];
    char b_fd[IDL_LOG_BUF];
    char b_di[IDL_LOG_BUF];
    snprintf(b_svc, sizeof(b_svc), "%d", walk.services);
    snprintf(b_rt, sizeof(b_rt), "%d", walk.routes);
    snprintf(b_h, sizeof(b_h), "%d", handles);
    snprintf(b_g, sizeof(b_g), "%d", grpc_calls);
    snprintf(b_ct, sizeof(b_ct), "%d", ctor_count);
    snprintf(b_fd, sizeof(b_fd), "%d", field_count);
    snprintf(b_di, sizeof(b_di), "%d", di_count);
    cbm_log_info("pass.done", "pass", "idl_scan", "services", b_svc, "routes", b_rt, "handles", b_h,
                 "grpc_calls", b_g, "ctor_params", b_ct, "fields", b_fd, "di_types", b_di);

    return 0;
}
