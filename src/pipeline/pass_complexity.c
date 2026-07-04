/*
 * pass_complexity.c — Interprocedural complexity propagation (Tier B).
 *
 * Tier A (in the extraction walk) stamps each Function/Method node with local
 * structural metrics: complexity (cyclomatic), cognitive, loop_count, loop_depth.
 * This pass propagates loop_depth along CALLS edges to estimate a worst-case
 * *transitive* nested-loop degree: a function with a depth-1 loop that calls an
 * O(n) helper is effectively O(n^2). The estimate assumes calls may occur inside
 * loops (an upper bound) — it is a queryable bottleneck *candidate* signal, not a
 * proof (true big-O is undecidable; cf. SPEED / Loopus). Recursive cycles are
 * collapsed into strongly connected components before propagation so full and
 * incremental runs do not depend on transient node or edge visitation order.
 *
 * Writes two extra node properties: transitive_loop_depth, recursive.
 */
#include "foundation/constants.h"
#include "pipeline/pipeline.h"
#include "pipeline/pipeline_internal.h"
#include "graph_buffer/graph_buffer.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "cbm.h"
#include "yyjson/yyjson.h"

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

enum {
    CBM_TLD_MAX_DEPTH = 256, /* recursion-depth cap (stack guard on condensed DAG) */
    CBM_SCC_ADJ_INIT_CAP = CBM_SZ_4,
};

/* Int → string for structured logging (thread-safe ring buffer). */
static const char *itoa_cx(int val) {
    enum { RING = 2, MASK = 1 };
    static CBM_TLS char bufs[RING][CBM_SZ_32];
    static CBM_TLS int idx = 0;
    int i = idx;
    idx = (idx + 1) & MASK;
    snprintf(bufs[i], sizeof(bufs[i]), "%d", val);
    return bufs[i];
}

/* Parse an integer "key":N from a flat JSON object. Returns def if absent. */
static int json_get_int(const char *json, const char *key, int dflt) {
    if (!json) {
        return dflt;
    }
    char pat[CBM_SZ_64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(json, pat);
    if (!p) {
        return dflt;
    }
    p += strlen(pat);
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    return (int)strtol(p, NULL, CBM_DECIMAL_BASE);
}

/* Parse a boolean "key":true/false from a flat JSON object. */
static bool json_get_bool(const char *json, const char *key) {
    if (!json) {
        return false;
    }
    char pat[CBM_SZ_64];
    snprintf(pat, sizeof(pat), "\"%s\":", key);
    const char *p = strstr(json, pat);
    if (!p) {
        return false;
    }
    p += strlen(pat);
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    return *p == 't';
}

static int max_int(int a, int b) {
    return a > b ? a : b;
}

static bool path_in_scope(const char *path, const char *const *paths, int path_count) {
    if (!paths || path_count <= 0) {
        return true;
    }
    if (!path) {
        return false;
    }
    for (int i = 0; i < path_count; i++) {
        if (paths[i] && strcmp(path, paths[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Set transitive_loop_depth + recursive on a node's properties JSON object. */
static void set_complexity_props(cbm_gbuf_node_t *node, int tld, bool recursive) {
    const char *old = node->properties_json ? node->properties_json : "{}";
    yyjson_doc *doc = yyjson_read(old, strlen(old), 0);
    if (!doc) {
        return;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!root || !yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return;
    }
    yyjson_mut_doc *mdoc = yyjson_doc_mut_copy(doc, NULL);
    yyjson_doc_free(doc);
    if (!mdoc) {
        return;
    }
    yyjson_mut_val *mroot = yyjson_mut_doc_get_root(mdoc);
    if (!mroot || !yyjson_mut_is_obj(mroot)) {
        yyjson_mut_doc_free(mdoc);
        return;
    }
    (void)yyjson_mut_obj_remove_key(mroot, "transitive_loop_depth");
    (void)yyjson_mut_obj_remove_key(mroot, "recursive");
    if (!yyjson_mut_obj_add_int(mdoc, mroot, "transitive_loop_depth", tld) ||
        !yyjson_mut_obj_add_bool(mdoc, mroot, "recursive", recursive)) {
        yyjson_mut_doc_free(mdoc);
        return;
    }
    char *neu = yyjson_mut_write(mdoc, 0, NULL);
    yyjson_mut_doc_free(mdoc);
    if (!neu) {
        return;
    }
    free(node->properties_json);
    node->properties_json = neu;
}

typedef struct {
    int *targets;
    int count;
    int cap;
} scc_adj_t;

/* Seed each Function/Method node's loop_depth and self_recursive flag, and
 * remember the node pointer for write-back. SCC detection below ORs in mutual
 * recursion discovered from CALLS cycles. */
static void seed_loop_depths(const cbm_gbuf_t *gb, const char *label, int *loop_depth,
                             int *stored_tld, bool *recursive, cbm_gbuf_node_t **nptr,
                             int64_t maxid, bool use_stored_derived,
                             const char *const *paths, int path_count) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(gb, label, &nodes, &count) != 0) {
        return;
    }
    for (int i = 0; i < count; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        if (n->id >= 1 && n->id <= maxid) {
            bool use_node_stored =
                use_stored_derived && !path_in_scope(n->file_path, paths, path_count);
            loop_depth[n->id] = json_get_int(n->properties_json, "loop_depth", 0);
            stored_tld[n->id] =
                json_get_int(n->properties_json, "transitive_loop_depth", CBM_NOT_FOUND);
            recursive[n->id] = json_get_bool(n->properties_json, "self_recursive") ||
                               (use_node_stored &&
                                json_get_bool(n->properties_json, "recursive"));
            nptr[n->id] = (cbm_gbuf_node_t *)n;
        }
    }
}

typedef struct {
    const cbm_gbuf_t *gb;
    cbm_gbuf_node_t **nptr;
    int *index;
    int *lowlink;
    bool *on_stack;
    int64_t *stack;
    int stack_len;
    int next_index;
    int next_component;
    bool *recursive;
    int *component;
    int64_t maxid;
} recursion_scc_ctx_t;

static void mark_scc_recursive(recursion_scc_ctx_t *ctx, int64_t root, int component_start,
                               bool has_self_edge) {
    int component_size = ctx->stack_len - component_start;
    bool is_recursive = has_self_edge || component_size > 1;
    int component_id = ctx->next_component++;
    int64_t node_id = 0;
    do {
        node_id = ctx->stack[--ctx->stack_len];
        ctx->on_stack[node_id] = false;
        ctx->component[node_id] = component_id;
        if (is_recursive) {
            ctx->recursive[node_id] = true;
        }
    } while (node_id != root && ctx->stack_len > 0);
}

static void scc_visit(recursion_scc_ctx_t *ctx, int64_t id) {
    ctx->index[id] = ctx->next_index;
    ctx->lowlink[id] = ctx->next_index;
    ctx->next_index++;
    ctx->stack[ctx->stack_len++] = id;
    ctx->on_stack[id] = true;

    bool has_self_edge = false;
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    cbm_gbuf_find_edges_by_source_type(ctx->gb, id, "CALLS", &edges, &edge_count);
    for (int i = 0; i < edge_count; i++) {
        int64_t target = edges[i]->target_id;
        if (target < 1 || target > ctx->maxid || !ctx->nptr[target]) {
            continue;
        }
        if (target == id) {
            has_self_edge = true;
        }
        if (ctx->index[target] == 0) {
            scc_visit(ctx, target);
            if (ctx->lowlink[target] < ctx->lowlink[id]) {
                ctx->lowlink[id] = ctx->lowlink[target];
            }
        } else if (ctx->on_stack[target] && ctx->index[target] < ctx->lowlink[id]) {
            ctx->lowlink[id] = ctx->index[target];
        }
    }

    if (ctx->lowlink[id] == ctx->index[id]) {
        int component_start = ctx->stack_len - 1;
        while (component_start > 0 && ctx->stack[component_start] != id) {
            component_start--;
        }
        mark_scc_recursive(ctx, id, component_start, has_self_edge);
    }
}

static int mark_recursive_sccs(const cbm_gbuf_t *gb, cbm_gbuf_node_t **nptr, bool *recursive,
                               int *component, int64_t maxid) {
    size_t sz = (size_t)maxid + 1;
    int *index = calloc(sz, sizeof(int));
    int *lowlink = calloc(sz, sizeof(int));
    bool *on_stack = calloc(sz, sizeof(bool));
    int64_t *stack = calloc(sz, sizeof(int64_t));
    if (!index || !lowlink || !on_stack || !stack) {
        free(index);
        free(lowlink);
        free(on_stack);
        free(stack);
        return CBM_NOT_FOUND;
    }

    recursion_scc_ctx_t ctx = {
        .gb = gb,
        .nptr = nptr,
        .index = index,
        .lowlink = lowlink,
        .on_stack = on_stack,
        .stack = stack,
        .stack_len = 0,
        .next_index = 1,
        .next_component = 0,
        .recursive = recursive,
        .component = component,
        .maxid = maxid,
    };
    for (int64_t id = 1; id <= maxid; id++) {
        if (nptr[id] && ctx.index[id] == 0) {
            scc_visit(&ctx, id);
        }
    }

    free(index);
    free(lowlink);
    free(on_stack);
    free(stack);
    return ctx.next_component;
}

static void free_scc_adj(scc_adj_t *adj, int count) {
    if (!adj) {
        return;
    }
    for (int i = 0; i < count; i++) {
        free(adj[i].targets);
    }
    free(adj);
}

static int scc_adj_push(scc_adj_t *adj, int target) {
    if (adj->count == adj->cap) {
        int next_cap = adj->cap ? adj->cap * PAIR_LEN : CBM_SCC_ADJ_INIT_CAP;
        int *next = realloc(adj->targets, (size_t)next_cap * sizeof(*next));
        if (!next) {
            return CBM_NOT_FOUND;
        }
        adj->targets = next;
        adj->cap = next_cap;
    }
    adj->targets[adj->count++] = target;
    return 0;
}

static scc_adj_t *build_scc_dag(const cbm_gbuf_t *gb, cbm_gbuf_node_t **nptr,
                                const int *component, int component_count, int64_t maxid) {
    scc_adj_t *adj = calloc((size_t)component_count, sizeof(*adj));
    if (!adj) {
        return NULL;
    }
    for (int64_t id = 1; id <= maxid; id++) {
        if (!nptr[id]) {
            continue;
        }
        int source_component = component[id];
        if (source_component < 0 || source_component >= component_count) {
            continue;
        }
        const cbm_gbuf_edge_t **edges = NULL;
        int edge_count = 0;
        cbm_gbuf_find_edges_by_source_type(gb, id, "CALLS", &edges, &edge_count);
        for (int i = 0; i < edge_count; i++) {
            int64_t target = edges[i]->target_id;
            if (target < 1 || target > maxid || !nptr[target]) {
                continue;
            }
            int target_component = component[target];
            if (target_component < 0 || target_component >= component_count ||
                target_component == source_component) {
                continue;
            }
            if (scc_adj_push(&adj[source_component], target_component) != 0) {
                free_scc_adj(adj, component_count);
                return NULL;
            }
        }
    }
    return adj;
}

/* Propagate loop depth over the SCC-condensed call graph. SCC condensation
 * converts recursive call cycles into DAG nodes, making the bound deterministic
 * across full and incremental ID/order differences. */
static int scc_tld_dfs(int component_id, const scc_adj_t *adj, const int *component_loop,
                       int *component_tld, char *state, int depth) {
    if (state[component_id] == 2) {
        return component_tld[component_id];
    }
    if (state[component_id] == 1 || depth > CBM_TLD_MAX_DEPTH) {
        return component_loop[component_id];
    }
    state[component_id] = 1;
    int best = 0;
    for (int i = 0; i < adj[component_id].count; i++) {
        int child = adj[component_id].targets[i];
        int child_tld = scc_tld_dfs(child, adj, component_loop, component_tld, state, depth + 1);
        if (child_tld > best) {
            best = child_tld;
        }
    }
    component_tld[component_id] = component_loop[component_id] + best;
    state[component_id] = 2;
    return component_tld[component_id];
}

static void pass_complexity_impl(cbm_pipeline_ctx_t *ctx, const char *const *paths,
                                 int path_count, bool use_stored_tld) {
    cbm_gbuf_t *gb = ctx->gbuf;
    /* Node and edge IDs are drawn from one shared counter, so node IDs are NOT
     * contiguous 1..node_count — they interleave with edge IDs. Size the lookup
     * arrays by the id ceiling (next_id) so every node id is addressable. */
    int64_t maxid = cbm_gbuf_next_id(gb) - 1;
    if (maxid < 1) {
        return;
    }
    size_t sz = (size_t)maxid + 1;
    int *loop_depth = calloc(sz, sizeof(int));
    int *stored_tld = malloc(sz * sizeof(int));
    bool *recursive = calloc(sz, sizeof(bool));
    cbm_gbuf_node_t **nptr = calloc(sz, sizeof(cbm_gbuf_node_t *));
    int *component = malloc(sz * sizeof(int));
    if (!loop_depth || !stored_tld || !recursive || !nptr || !component) {
        free(loop_depth);
        free(stored_tld);
        free(recursive);
        free(nptr);
        free(component);
        return;
    }
    for (int64_t id = 0; id <= maxid; id++) {
        stored_tld[id] = CBM_NOT_FOUND;
        component[id] = CBM_NOT_FOUND;
    }

    seed_loop_depths(gb, "Function", loop_depth, stored_tld, recursive, nptr, maxid,
                     use_stored_tld, paths, path_count);
    seed_loop_depths(gb, "Method", loop_depth, stored_tld, recursive, nptr, maxid,
                     use_stored_tld, paths, path_count);
    int component_count = mark_recursive_sccs(gb, nptr, recursive, component, maxid);
    if (component_count <= 0) {
        free(loop_depth);
        free(stored_tld);
        free(recursive);
        free(nptr);
        free(component);
        return;
    }

    scc_adj_t *adj = build_scc_dag(gb, nptr, component, component_count, maxid);
    int *component_loop = calloc((size_t)component_count, sizeof(int));
    int *component_tld = calloc((size_t)component_count, sizeof(int));
    char *component_state = calloc((size_t)component_count, sizeof(char));
    if (!adj || !component_loop || !component_tld || !component_state) {
        free_scc_adj(adj, component_count);
        free(component_loop);
        free(component_tld);
        free(component_state);
        free(loop_depth);
        free(stored_tld);
        free(recursive);
        free(nptr);
        free(component);
        return;
    }

    for (int64_t id = 1; id <= maxid; id++) {
        if (!nptr[id]) {
            continue;
        }
        int component_id = component[id];
        bool use_node_stored =
            use_stored_tld && !path_in_scope(nptr[id]->file_path, paths, path_count);
        int base_tld = (use_node_stored && stored_tld[id] >= 0)
                           ? max_int(loop_depth[id], stored_tld[id])
                           : loop_depth[id];
        if (component_id >= 0 && base_tld > component_loop[component_id]) {
            component_loop[component_id] = base_tld;
        }
    }
    for (int component_id = 0; component_id < component_count; component_id++) {
        if (component_state[component_id] != 2) {
            scc_tld_dfs(component_id, adj, component_loop, component_tld, component_state, 0);
        }
    }

    int updated = 0;
    for (int64_t id = 1; id <= maxid; id++) {
        if (!nptr[id]) {
            continue; /* only Function/Method nodes */
        }
        if (!path_in_scope(nptr[id]->file_path, paths, path_count)) {
            continue;
        }
        int component_id = component[id];
        int tld = component_id >= 0 ? component_tld[component_id] : loop_depth[id];
        set_complexity_props(nptr[id], tld, recursive[id]);
        updated++;
    }

    cbm_log_info("pass.complexity", "functions", itoa_cx(updated));

    free_scc_adj(adj, component_count);
    free(component_loop);
    free(component_tld);
    free(component_state);
    free(loop_depth);
    free(stored_tld);
    free(recursive);
    free(nptr);
    free(component);
}

void cbm_pipeline_pass_complexity(cbm_pipeline_ctx_t *ctx) {
    pass_complexity_impl(ctx, NULL, 0, false);
}

void cbm_pipeline_pass_complexity_for_paths(cbm_pipeline_ctx_t *ctx, const char *const *paths,
                                            int path_count) {
    pass_complexity_impl(ctx, paths, path_count, true);
}
