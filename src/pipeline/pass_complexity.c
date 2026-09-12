/*
 * pass_complexity.c — Interprocedural complexity propagation (Tier B).
 *
 * Tier A (in the extraction walk) stamps each Function/Method node with local
 * structural metrics: complexity (cyclomatic), cognitive, loop_count, loop_depth.
 * This pass propagates loop_depth along CALLS edges to estimate a worst-case
 * *transitive* nested-loop degree: a function with a depth-1 loop that calls an
 * O(n) helper is effectively O(n^2). The estimate assumes calls may occur inside
 * loops (an upper bound) — it is a queryable bottleneck *candidate* signal, not a
 * proof (true big-O is undecidable; cf. SPEED / Loopus). Cycles in the call graph
 * are broken and flagged via a `recursive` property.
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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdbool.h>

enum { CBM_TLD_MAX_DEPTH = 256 }; /* recursion-depth cap (cycle/stack guard) */

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

/* Append transitive_loop_depth + recursive to a node's properties JSON object. */
static void append_complexity_props(cbm_gbuf_node_t *node, int tld, bool recursive) {
    const char *old = node->properties_json ? node->properties_json : "{}";
    size_t olen = strlen(old);
    if (olen < 2 || old[olen - 1] != '}') {
        return; /* not a JSON object — leave untouched */
    }
    bool empty = (olen == 2); /* "{}" */
    char *neu = malloc(olen + CBM_SZ_64);
    if (!neu) {
        return;
    }
    memcpy(neu, old, olen - 1); /* copy without trailing '}' */
    int w =
        snprintf(neu + (olen - 1), CBM_SZ_64, "%s\"transitive_loop_depth\":%d,\"recursive\":%s}",
                 empty ? "" : ",", tld, recursive ? "true" : "false");
    if (w < 0) {
        free(neu);
        return;
    }
    free(node->properties_json);
    node->properties_json = neu;
}

/* Content-only node order: qualified_name, then file path and start line.
 * Never the temp id: extract workers draw ids from one shared counter, so id
 * order is worker-scheduling order and differs run to run. The cycle guard
 * flags whichever member the DFS ENTERS first, so both the seed order and the
 * callee order must be a function of the inputs alone, or `recursive` flips
 * between otherwise identical multi-worker runs. A dangling target (no node
 * for the id) has no edges of its own, so where it sorts cannot move a flag;
 * it keys as empty strings. */
enum { TLD_CMP_LESS = -1, TLD_CMP_GREATER = 1 };

static const char *str_or_empty(const char *s) {
    return s ? s : "";
}

static int cmp_node_canonical(const cbm_gbuf_node_t *a, const cbm_gbuf_node_t *b) {
    int r = strcmp(str_or_empty(a ? a->qualified_name : NULL),
                   str_or_empty(b ? b->qualified_name : NULL));
    if (r != 0) {
        return r;
    }
    r = strcmp(str_or_empty(a ? a->file_path : NULL), str_or_empty(b ? b->file_path : NULL));
    if (r != 0) {
        return r;
    }
    int la = a ? a->start_line : 0;
    int lb = b ? b->start_line : 0;
    if (la != lb) {
        return la < lb ? TLD_CMP_LESS : TLD_CMP_GREATER;
    }
    return 0;
}

static int cmp_seed_canonical(const void *pa, const void *pb) {
    return cmp_node_canonical(*(const cbm_gbuf_node_t *const *)pa,
                              *(const cbm_gbuf_node_t *const *)pb);
}

typedef struct {
    const cbm_gbuf_node_t *node; /* NULL for a dangling target id */
    int64_t id;
} tld_callee_t;

static int cmp_callee_canonical(const void *pa, const void *pb) {
    return cmp_node_canonical(((const tld_callee_t *)pa)->node, ((const tld_callee_t *)pb)->node);
}

/* Traversal state. `callees` is one bump stack shared by every DFS frame: a
 * frame takes its out-degree worth of slots, sorts them, recurses, then
 * releases them. Nodes on the recursion path are distinct (state 1 blocks
 * re-entry), so the live slots never exceed the CALLS edge count the stack is
 * sized for. */
typedef struct {
    const cbm_gbuf_t *gb;
    int64_t maxid;
    int *loop_depth;
    int *tld;
    char *state;
    bool *recursive;
    cbm_gbuf_node_t **seeds; /* Function/Method nodes, canonical order */
    int seed_count;
    int seed_cap;
    tld_callee_t *callees;
    int callee_top;
    int callee_cap;
} tld_ctx_t;

static void tld_ctx_free(tld_ctx_t *cx) {
    free(cx->loop_depth);
    free(cx->tld);
    free(cx->state);
    free(cx->recursive);
    free(cx->seeds);
    free(cx->callees);
}

/* Memoized DFS: tld(id) = loop_depth(id) + max over CALLS-callees of tld(callee).
 * state: 0=unvisited, 1=in-progress (back-edge -> cycle), 2=done. */
static int tld_dfs(tld_ctx_t *cx, int64_t id, int depth) {
    if (id < 1 || id > cx->maxid) {
        return 0;
    }
    if (cx->state[id] == 2) {
        return cx->tld[id];
    }
    if (cx->state[id] == 1) {
        cx->recursive[id] = true; /* back edge -> call-graph cycle */
        return 0;
    }
    if (depth > CBM_TLD_MAX_DEPTH) {
        return cx->loop_depth[id];
    }
    const cbm_gbuf_edge_t **edges = NULL;
    int ne = 0;
    cbm_gbuf_find_edges_by_source_type(cx->gb, id, "CALLS", &edges, &ne);
    if (ne > cx->callee_cap - cx->callee_top) {
        return cx->loop_depth[id]; /* unreachable by construction; same as the depth cap */
    }
    cx->state[id] = 1;
    tld_callee_t *callees = cx->callees + cx->callee_top;
    int nc = 0;
    for (int i = 0; i < ne; i++) {
        int64_t c = edges[i]->target_id;
        if (c == id) {
            cx->recursive[id] = true; /* direct self-recursion */
            continue;
        }
        callees[nc].id = c;
        callees[nc].node = cbm_gbuf_find_by_id(cx->gb, c);
        nc++;
    }
    cx->callee_top += nc;
    qsort(callees, (size_t)nc, sizeof(*callees), cmp_callee_canonical);
    int best = 0;
    for (int i = 0; i < nc; i++) {
        int ct = tld_dfs(cx, callees[i].id, depth + 1);
        if (ct > best) {
            best = ct;
        }
    }
    cx->callee_top -= nc;
    cx->tld[id] = cx->loop_depth[id] + best;
    cx->state[id] = 2;
    return cx->tld[id];
}

static int label_count(const cbm_gbuf_t *gb, const char *label) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(gb, label, &nodes, &count) != 0) {
        return 0;
    }
    return count;
}

static int calls_edge_count(const cbm_gbuf_t *gb) {
    const cbm_gbuf_edge_t **edges = NULL;
    int count = 0;
    if (cbm_gbuf_find_edges_by_type(gb, "CALLS", &edges, &count) != 0) {
        return 0;
    }
    return count;
}

/* Seed each Function/Method node's loop_depth and self_recursive flag, and
 * collect the node as a traversal seed (also the write-back target). The
 * self_recursive seed (set at extraction) feeds the final recursive flag;
 * tld_dfs additionally ORs in mutual recursion discovered as a call-graph
 * cycle. */
static void seed_loop_depths(tld_ctx_t *cx, const char *label) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(cx->gb, label, &nodes, &count) != 0) {
        return;
    }
    for (int i = 0; i < count && cx->seed_count < cx->seed_cap; i++) {
        const cbm_gbuf_node_t *n = nodes[i];
        if (n->id >= 1 && n->id <= cx->maxid) {
            cx->loop_depth[n->id] = json_get_int(n->properties_json, "loop_depth", 0);
            cx->recursive[n->id] = json_get_bool(n->properties_json, "self_recursive");
            cx->seeds[cx->seed_count++] = (cbm_gbuf_node_t *)n;
        }
    }
}

void cbm_pipeline_pass_complexity(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;
    /* Node and edge IDs are drawn from one shared counter, so node IDs are NOT
     * contiguous 1..node_count — they interleave with edge IDs. Size the lookup
     * arrays by the id ceiling (next_id) so every node id is addressable. */
    int64_t maxid = cbm_gbuf_next_id(gb) - 1;
    if (maxid < 1) {
        return;
    }
    size_t sz = (size_t)maxid + 1;
    tld_ctx_t cx = {0};
    cx.gb = gb;
    cx.maxid = maxid;
    cx.seed_cap = label_count(gb, "Function") + label_count(gb, "Method");
    cx.callee_cap = calls_edge_count(gb);
    cx.loop_depth = calloc(sz, sizeof(int));
    cx.tld = calloc(sz, sizeof(int));
    cx.state = calloc(sz, sizeof(char));
    cx.recursive = calloc(sz, sizeof(bool));
    cx.seeds = calloc((size_t)cx.seed_cap + 1, sizeof(cbm_gbuf_node_t *));
    cx.callees = calloc((size_t)cx.callee_cap + 1, sizeof(tld_callee_t));
    if (!cx.loop_depth || !cx.tld || !cx.state || !cx.recursive || !cx.seeds || !cx.callees) {
        tld_ctx_free(&cx);
        return;
    }

    seed_loop_depths(&cx, "Function");
    seed_loop_depths(&cx, "Method");
    qsort(cx.seeds, (size_t)cx.seed_count, sizeof(*cx.seeds), cmp_seed_canonical);

    for (int i = 0; i < cx.seed_count; i++) {
        cbm_gbuf_node_t *n = cx.seeds[i];
        if (cx.state[n->id] != 2) {
            tld_dfs(&cx, n->id, 0);
        }
        append_complexity_props(n, cx.tld[n->id], cx.recursive[n->id]);
    }

    cbm_log_info("pass.complexity", "functions", itoa_cx(cx.seed_count));

    tld_ctx_free(&cx);
}
