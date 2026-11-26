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
#include "foundation/arena.h"
#include "foundation/mem_core.h"
#include "store/store.h"
#include "cbm.h"
#include "sqlite3.h"

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
    char *neu = cbm_alloc(CBM_MEM_CLASS_OTHER, olen + CBM_SZ_64);
    if (!neu) {
        return;
    }
    memcpy(neu, old, olen - 1); /* copy without trailing '}' */
    int w =
        snprintf(neu + (olen - 1), CBM_SZ_64, "%s\"transitive_loop_depth\":%d,\"recursive\":%s}",
                 empty ? "" : ",", tld, recursive ? "true" : "false");
    if (w < 0) {
        cbm_free(CBM_MEM_CLASS_OTHER, neu);
        return;
    }
    (void)cbm_gbuf_node_set_properties_json(node, neu);
    cbm_free(CBM_MEM_CLASS_OTHER, neu);
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

typedef struct {
    const char *qualified_name;
    const char *file_path;
    int start_line;
} tld_key_t;

static int cmp_key_canonical(const tld_key_t *a, const tld_key_t *b) {
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

typedef struct {
    const tld_key_t *key; /* NULL for a dangling target id */
    int64_t id;
} tld_callee_t;

static int cmp_callee_canonical(const void *pa, const void *pb) {
    return cmp_key_canonical(((const tld_callee_t *)pa)->key, ((const tld_callee_t *)pb)->key);
}

/* The call graph the traversal reads, whoever gathered it: the graph buffer
 * (a full build) or the staging store (the closure-delta incremental route).
 * One traversal for both is what keeps an incremental graph equal to a full
 * one. Ids index every array directly; first[] is a CSR of CALLS edges by
 * source id in gathering order (each frame sorts its callees canonically). */
typedef struct {
    int64_t maxid;
    tld_key_t *keys;     /* [maxid + 1] */
    bool *present;       /* [maxid + 1]: a node exists for the id */
    int *loop_depth;     /* [maxid + 1]: seeds only */
    bool *recursive;     /* [maxid + 1]: self_recursive, then cycles */
    tld_callee_t *seeds; /* Function/Method nodes */
    int seed_count;
    int *first;      /* [maxid + 2] */
    int64_t *callee; /* [edge_count] */
    int edge_count;
} tld_graph_t;

static void tld_graph_free(tld_graph_t *g) {
    cbm_free(CBM_MEM_CLASS_OTHER, g->keys);
    cbm_free(CBM_MEM_CLASS_OTHER, g->present);
    cbm_free(CBM_MEM_CLASS_OTHER, g->loop_depth);
    cbm_free(CBM_MEM_CLASS_OTHER, g->recursive);
    cbm_free(CBM_MEM_CLASS_OTHER, g->seeds);
    cbm_free(CBM_MEM_CLASS_OTHER, g->first);
    cbm_free(CBM_MEM_CLASS_OTHER, g->callee);
}

static bool tld_graph_alloc(tld_graph_t *g, int64_t maxid, int seed_cap, int edge_count) {
    size_t sz = (size_t)maxid + 1;
    g->maxid = maxid;
    g->keys = cbm_calloc(CBM_MEM_CLASS_OTHER, sz * sizeof(*g->keys));
    g->present = cbm_calloc(CBM_MEM_CLASS_OTHER, sz * sizeof(*g->present));
    g->loop_depth = cbm_calloc(CBM_MEM_CLASS_OTHER, sz * sizeof(*g->loop_depth));
    g->recursive = cbm_calloc(CBM_MEM_CLASS_OTHER, sz * sizeof(*g->recursive));
    g->seeds = cbm_calloc(CBM_MEM_CLASS_OTHER, ((size_t)seed_cap + 1) * sizeof(*g->seeds));
    g->first = cbm_calloc(CBM_MEM_CLASS_OTHER, (sz + 1) * sizeof(*g->first));
    g->callee = cbm_calloc(CBM_MEM_CLASS_OTHER, ((size_t)edge_count + 1) * sizeof(*g->callee));
    return g->keys && g->present && g->loop_depth && g->recursive && g->seeds && g->first &&
           g->callee;
}

/* Sources must be counted into first[src + 1] before this; turns the counts
 * into offsets. */
static void tld_graph_offsets(tld_graph_t *g) {
    for (int64_t id = 1; id <= g->maxid + 1; id++) {
        g->first[id] += g->first[id - 1];
    }
}

static void tld_key_set(tld_graph_t *g, int64_t id, const char *qn, const char *file, int line) {
    if (id >= 1 && id <= g->maxid) {
        g->keys[id] = (tld_key_t){qn, file, line};
        g->present[id] = true;
    }
}

static void tld_seed(tld_graph_t *g, int64_t id, const char *properties, int seed_cap) {
    if (id >= 1 && id <= g->maxid && g->seed_count < seed_cap) {
        g->loop_depth[id] = json_get_int(properties, "loop_depth", 0);
        g->recursive[id] = json_get_bool(properties, "self_recursive");
        g->seeds[g->seed_count].id = id;
        g->seeds[g->seed_count].key = &g->keys[id];
        g->seed_count++;
    }
}

/* Traversal state. `callees` is one bump stack shared by every DFS frame: a
 * frame takes its out-degree worth of slots, sorts them, recurses, then
 * releases them. Nodes on the recursion path are distinct (state 1 blocks
 * re-entry), so the live slots never exceed the CALLS edge count the stack is
 * sized for. */
typedef struct {
    tld_graph_t *g;
    int *tld;
    char *state;
    tld_callee_t *callees;
    int callee_top;
    int callee_cap;
} tld_ctx_t;

/* Memoized DFS: tld(id) = loop_depth(id) + max over CALLS-callees of tld(callee).
 * state: 0=unvisited, 1=in-progress (back-edge -> cycle), 2=done. */
static int tld_dfs(tld_ctx_t *cx, int64_t id, int depth) {
    tld_graph_t *g = cx->g;
    if (id < 1 || id > g->maxid) {
        return 0;
    }
    if (cx->state[id] == 2) {
        return cx->tld[id];
    }
    if (cx->state[id] == 1) {
        g->recursive[id] = true; /* back edge -> call-graph cycle */
        return 0;
    }
    if (depth > CBM_TLD_MAX_DEPTH) {
        return g->loop_depth[id];
    }
    int ne = g->first[id + 1] - g->first[id];
    if (ne > cx->callee_cap - cx->callee_top) {
        return g->loop_depth[id]; /* unreachable by construction; same as the depth cap */
    }
    cx->state[id] = 1;
    tld_callee_t *callees = cx->callees + cx->callee_top;
    int nc = 0;
    for (int i = g->first[id]; i < g->first[id + 1]; i++) {
        int64_t c = g->callee[i];
        if (c == id) {
            g->recursive[id] = true; /* direct self-recursion */
            continue;
        }
        callees[nc].id = c;
        callees[nc].key = c >= 1 && c <= g->maxid && g->present[c] ? &g->keys[c] : NULL;
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
    cx->tld[id] = g->loop_depth[id] + best;
    cx->state[id] = 2;
    return cx->tld[id];
}

/* Runs the traversal from every seed in canonical order; *tld_out[id] and
 * g->recursive[id] then hold each seed's result. */
static bool tld_run(tld_graph_t *g, int **tld_out) {
    size_t sz = (size_t)g->maxid + 1;
    tld_ctx_t cx = {.g = g, .callee_cap = g->edge_count};
    cx.tld = cbm_calloc(CBM_MEM_CLASS_OTHER, sz * sizeof(*cx.tld));
    cx.state = cbm_calloc(CBM_MEM_CLASS_OTHER, sz * sizeof(*cx.state));
    cx.callees = cbm_calloc(CBM_MEM_CLASS_OTHER, ((size_t)g->edge_count + 1) * sizeof(*cx.callees));
    if (!cx.tld || !cx.state || !cx.callees) {
        cbm_free(CBM_MEM_CLASS_OTHER, cx.tld);
        cbm_free(CBM_MEM_CLASS_OTHER, cx.state);
        cbm_free(CBM_MEM_CLASS_OTHER, cx.callees);
        return false;
    }
    qsort(g->seeds, (size_t)g->seed_count, sizeof(*g->seeds), cmp_callee_canonical);
    for (int i = 0; i < g->seed_count; i++) {
        if (cx.state[g->seeds[i].id] != 2) {
            tld_dfs(&cx, g->seeds[i].id, 0);
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, cx.state);
    cbm_free(CBM_MEM_CLASS_OTHER, cx.callees);
    *tld_out = cx.tld;
    return true;
}

static int label_count(const cbm_gbuf_t *gb, const char *label) {
    const cbm_gbuf_node_t **nodes = NULL;
    int count = 0;
    if (cbm_gbuf_find_by_label(gb, label, &nodes, &count) != 0) {
        return 0;
    }
    return count;
}

/* Gathers the graph buffer: every Function/Method node as a seed, every CALLS
 * edge, and the key of every node a CALLS edge targets. */
static bool tld_gather_gbuf(const cbm_gbuf_t *gb, tld_graph_t *g) {
    /* Node and edge IDs are drawn from one shared counter, so node IDs are NOT
     * contiguous 1..node_count — they interleave with edge IDs. Size the lookup
     * arrays by the id ceiling (next_id) so every node id is addressable. */
    int64_t maxid = cbm_gbuf_next_id(gb) - 1;
    const cbm_gbuf_edge_t **edges = NULL;
    int edge_count = 0;
    if (maxid < 1 || cbm_gbuf_find_edges_by_type(gb, "CALLS", &edges, &edge_count) != 0) {
        edge_count = 0;
    }
    int seed_cap = label_count(gb, "Function") + label_count(gb, "Method");
    if (maxid < 1 || !tld_graph_alloc(g, maxid, seed_cap, edge_count)) {
        return false;
    }
    static const char *const labels[] = {"Function", "Method"};
    for (size_t l = 0; l < sizeof(labels) / sizeof(labels[0]); l++) {
        const cbm_gbuf_node_t **nodes = NULL;
        int count = 0;
        if (cbm_gbuf_find_by_label(gb, labels[l], &nodes, &count) != 0) {
            continue;
        }
        for (int i = 0; i < count; i++) {
            const cbm_gbuf_node_t *n = nodes[i];
            tld_key_set(g, n->id, n->qualified_name, n->file_path, n->start_line);
            tld_seed(g, n->id, n->properties_json, seed_cap);
        }
    }
    for (int i = 0; i < edge_count; i++) {
        int64_t source = edges[i]->source_id;
        int64_t target = edges[i]->target_id;
        if (source >= 1 && source <= maxid) {
            g->first[source + 1]++;
        }
        const cbm_gbuf_node_t *t = cbm_gbuf_find_by_id(gb, target);
        if (t && !(target >= 1 && target <= maxid && g->present[target])) {
            tld_key_set(g, target, t->qualified_name, t->file_path, t->start_line);
        }
    }
    tld_graph_offsets(g);
    int *fill = cbm_calloc(CBM_MEM_CLASS_OTHER, ((size_t)maxid + 1) * sizeof(*fill));
    if (!fill) {
        return false;
    }
    for (int i = 0; i < edge_count; i++) {
        int64_t source = edges[i]->source_id;
        if (source >= 1 && source <= maxid) {
            g->callee[g->first[source] + fill[source]++] = edges[i]->target_id;
        }
    }
    cbm_free(CBM_MEM_CLASS_OTHER, fill);
    g->edge_count = edge_count;
    return true;
}

void cbm_pipeline_pass_complexity(cbm_pipeline_ctx_t *ctx) {
    cbm_gbuf_t *gb = ctx->gbuf;
    tld_graph_t g = {0};
    int *tld = NULL;
    if (!tld_gather_gbuf(gb, &g) || !tld_run(&g, &tld)) {
        tld_graph_free(&g);
        return;
    }
    for (int i = 0; i < g.seed_count; i++) {
        int64_t id = g.seeds[i].id;
        cbm_gbuf_node_t *n = (cbm_gbuf_node_t *)cbm_gbuf_find_by_id(gb, id);
        if (n) {
            append_complexity_props(n, tld[id], g.recursive[id]);
        }
    }
    cbm_log_info("pass.complexity", "functions", itoa_cx(g.seed_count));
    cbm_free(CBM_MEM_CLASS_OTHER, tld);
    tld_graph_free(&g);
}

/* ── Store-level recompute (closure-delta incremental) ────────────────
 *
 * The closure-delta route patches the staging store with the re-extracted
 * files only; transitive_loop_depth and recursive are properties of the whole
 * call graph, so a body edit can move them for callers in files that were not
 * re-parsed, and the re-parsed nodes arrive without them. Like importance, the
 * recompute therefore runs over the complete staging store after the patch,
 * with the same traversal as the in-memory pass (only the gathering differs)
 * and BEFORE the importance rescore, so the keys land where a full build puts
 * them. Existing keys are rewritten in place; absent ones are appended. A
 * failure fails the delta route, which then rebuilds in full. */

/* properties with transitive_loop_depth/recursive set: rewritten in place
 * when present, appended exactly as the in-memory pass appends otherwise.
 * NULL: allocation failure or a malformed existing pair. */
static char *complexity_set_props(const char *old, int tld, bool recursive) {
    static const char key[] = "\"transitive_loop_depth\":";
    static const char rec_key[] = ",\"recursive\":";
    const char *props = old ? old : "{}";
    size_t olen = strlen(props);
    char value[CBM_SZ_64];
    int w =
        snprintf(value, sizeof(value), "%s%d%s%s", key, tld, rec_key, recursive ? "true" : "false");
    if (w < 0 || (size_t)w >= sizeof(value)) {
        return NULL;
    }
    const char *at = strstr(props, key);
    size_t head = 0;
    size_t tail = 0; /* offset where the old pair ends */
    const char *sep = "";
    if (at) {
        const char *p = at + sizeof(key) - 1;
        if (*p == '-') {
            p++;
        }
        while (*p >= '0' && *p <= '9') {
            p++;
        }
        if (strncmp(p, rec_key, sizeof(rec_key) - 1) != 0) {
            return NULL;
        }
        p += sizeof(rec_key) - 1;
        if (strncmp(p, "true", 4) == 0) {
            p += 4;
        } else if (strncmp(p, "false", 5) == 0) {
            p += 5;
        } else {
            return NULL;
        }
        head = (size_t)(at - props);
        tail = (size_t)(p - props);
    } else {
        if (olen < 2 || props[olen - 1] != '}') {
            return cbm_mem_strdup(CBM_MEM_CLASS_OTHER, props); /* not an object: untouched */
        }
        head = olen - 1;
        tail = olen - 1;
        sep = olen == 2 ? "" : ",";
    }
    size_t len = head + strlen(sep) + (size_t)w + (olen - tail);
    char *neu = cbm_alloc(CBM_MEM_CLASS_OTHER, len + 1);
    if (!neu) {
        return NULL;
    }
    memcpy(neu, props, head);
    size_t off = head;
    memcpy(neu + off, sep, strlen(sep));
    off += strlen(sep);
    memcpy(neu + off, value, (size_t)w);
    off += (size_t)w;
    memcpy(neu + off, props + tail, olen - tail);
    neu[len] = '\0';
    return neu;
}

/* UDF: cbm_cx_set(properties, tld, recursive). */
static void sql_complexity_set(sqlite3_context *c, int argc, sqlite3_value **argv) {
    if (argc != 3) {
        sqlite3_result_error(c, "cbm_cx_set arity", -1);
        return;
    }
    char *neu = complexity_set_props((const char *)sqlite3_value_text(argv[0]),
                                     sqlite3_value_int(argv[1]), sqlite3_value_int(argv[2]) != 0);
    if (!neu) {
        sqlite3_result_error(c, "cbm_cx_set: malformed complexity properties", -1);
        return;
    }
    sqlite3_result_text(c, neu, -1, SQLITE_TRANSIENT);
    cbm_free(CBM_MEM_CLASS_OTHER, neu);
}

static bool tld_step_int64(sqlite3_stmt *st, int64_t *out) {
    if (sqlite3_step(st) != SQLITE_ROW) {
        return false;
    }
    *out = sqlite3_column_int64(st, 0);
    return true;
}

/* Gathers the staging store the way tld_gather_gbuf gathers the buffer. Key
 * strings are copied into `arena`. */
static bool tld_gather_store(sqlite3 *db, const char *project, CBMArena *arena, tld_graph_t *g) {
    static const char kMax[] = "SELECT COALESCE(MAX(id), 0) FROM nodes WHERE project = ?1";
    static const char kCounts[] =
        "SELECT (SELECT COUNT(*) FROM nodes WHERE project = ?1 AND label IN "
        "('Function','Method')), (SELECT COUNT(*) FROM edges WHERE project = ?1 AND type = "
        "'CALLS')";
    static const char kNodes[] =
        "SELECT id, label, qualified_name, file_path, start_line, properties FROM nodes "
        "WHERE project = ?1 AND (label IN ('Function','Method') OR id IN (SELECT target_id "
        "FROM edges WHERE project = ?1 AND type = 'CALLS'))";
    static const char kEdges[] =
        "SELECT source_id, target_id FROM edges WHERE project = ?1 AND type = 'CALLS' ORDER BY id";
    sqlite3_stmt *st = NULL;
    int64_t maxid = 0;
    bool ok = sqlite3_prepare_v2(db, kMax, -1, &st, NULL) == SQLITE_OK &&
              sqlite3_bind_text(st, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
              tld_step_int64(st, &maxid);
    sqlite3_finalize(st);
    st = NULL;
    int seed_cap = 0;
    int edge_count = 0;
    ok = ok && sqlite3_prepare_v2(db, kCounts, -1, &st, NULL) == SQLITE_OK &&
         sqlite3_bind_text(st, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK &&
         sqlite3_step(st) == SQLITE_ROW;
    if (ok) {
        seed_cap = sqlite3_column_int(st, 0);
        edge_count = sqlite3_column_int(st, 1);
    }
    sqlite3_finalize(st);
    st = NULL;
    if (!ok || maxid < 1) {
        return ok; /* an empty project: nothing to recompute */
    }
    if (!tld_graph_alloc(g, maxid, seed_cap, edge_count)) {
        return false;
    }
    ok = sqlite3_prepare_v2(db, kNodes, -1, &st, NULL) == SQLITE_OK &&
         sqlite3_bind_text(st, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    int rc = SQLITE_ROW;
    while (ok && (rc = sqlite3_step(st)) == SQLITE_ROW) {
        int64_t id = sqlite3_column_int64(st, 0);
        const char *label = (const char *)sqlite3_column_text(st, 1);
        const char *qn = (const char *)sqlite3_column_text(st, 2);
        const char *file = (const char *)sqlite3_column_text(st, 3);
        char *qn_copy = qn ? cbm_arena_strdup(arena, qn) : NULL;
        char *file_copy = file ? cbm_arena_strdup(arena, file) : NULL;
        ok = (!qn || qn_copy) && (!file || file_copy) && id >= 1 && id <= maxid;
        if (!ok) {
            break;
        }
        tld_key_set(g, id, qn_copy, file_copy, sqlite3_column_int(st, 4));
        if (label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0)) {
            tld_seed(g, id, (const char *)sqlite3_column_text(st, 5), seed_cap);
        }
    }
    ok = ok && rc == SQLITE_DONE;
    sqlite3_finalize(st);
    st = NULL;
    int64_t *sources =
        ok ? cbm_alloc(CBM_MEM_CLASS_OTHER, ((size_t)edge_count + 1) * sizeof(*sources)) : NULL;
    ok = ok && sources && sqlite3_prepare_v2(db, kEdges, -1, &st, NULL) == SQLITE_OK &&
         sqlite3_bind_text(st, 1, project, -1, SQLITE_TRANSIENT) == SQLITE_OK;
    int n = 0;
    while (ok && (rc = sqlite3_step(st)) == SQLITE_ROW) {
        int64_t source = sqlite3_column_int64(st, 0);
        int64_t target = sqlite3_column_int64(st, 1);
        ok = n < edge_count && source >= 1 && source <= maxid;
        if (ok) {
            sources[n] = source;
            g->callee[n] = target;
            g->first[source + 1]++;
            n++;
        }
    }
    ok = ok && rc == SQLITE_DONE && n == edge_count;
    sqlite3_finalize(st);
    if (ok) {
        /* Rows arrived in edge order; place each one by its source. */
        tld_graph_offsets(g);
        int64_t *ordered = cbm_alloc(CBM_MEM_CLASS_OTHER, ((size_t)n + 1) * sizeof(*ordered));
        int *fill = cbm_calloc(CBM_MEM_CLASS_OTHER, ((size_t)maxid + 1) * sizeof(*fill));
        ok = ordered && fill;
        for (int i = 0; ok && i < n; i++) {
            ordered[g->first[sources[i]] + fill[sources[i]]++] = g->callee[i];
        }
        if (ok) {
            memcpy(g->callee, ordered, (size_t)n * sizeof(*ordered));
        }
        cbm_free(CBM_MEM_CLASS_OTHER, ordered);
        cbm_free(CBM_MEM_CLASS_OTHER, fill);
    }
    cbm_free(CBM_MEM_CLASS_OTHER, sources);
    g->edge_count = n;
    return ok;
}

int cbm_pipeline_complexity_recompute_store(cbm_store_t *store, const char *project) {
    sqlite3 *db = store && project ? cbm_store_get_db(store) : NULL;
    if (!db) {
        return -1;
    }
    if (sqlite3_create_function(db, "cbm_cx_set", 3, SQLITE_UTF8 | SQLITE_DETERMINISTIC, NULL,
                                sql_complexity_set, NULL, NULL) != SQLITE_OK) {
        cbm_log_error("complexity.store_udf_failed", "err", sqlite3_errmsg(db));
        return -1;
    }
    CBMArena arena;
    cbm_arena_init_lazy(&arena, CBM_ARENA_DEFAULT_BLOCK_SIZE);
    tld_graph_t g = {0};
    int *tld = NULL;
    bool ok = tld_gather_store(db, project, &arena, &g) && (!g.maxid || tld_run(&g, &tld));
    if (ok && g.seed_count && cbm_store_begin(store) == CBM_STORE_OK) {
        sqlite3_stmt *st = NULL;
        ok = sqlite3_prepare_v2(db,
                                "UPDATE nodes SET properties = cbm_cx_set(properties, ?1, ?2) "
                                "WHERE id = ?3",
                                -1, &st, NULL) == SQLITE_OK;
        for (int i = 0; ok && i < g.seed_count; i++) {
            int64_t id = g.seeds[i].id;
            ok = sqlite3_bind_int(st, 1, tld[id]) == SQLITE_OK &&
                 sqlite3_bind_int(st, 2, g.recursive[id] ? 1 : 0) == SQLITE_OK &&
                 sqlite3_bind_int64(st, 3, id) == SQLITE_OK && sqlite3_step(st) == SQLITE_DONE &&
                 sqlite3_reset(st) == SQLITE_OK;
        }
        sqlite3_finalize(st);
        if (!ok) {
            cbm_log_error("complexity.store_update_failed", "err", sqlite3_errmsg(db));
            (void)cbm_store_rollback(store);
        } else {
            ok = cbm_store_commit(store) == CBM_STORE_OK;
        }
    } else if (ok && g.seed_count) {
        ok = false;
    }
    cbm_log_info("pass.complexity_store", "functions", itoa_cx(g.seed_count));
    cbm_free(CBM_MEM_CLASS_OTHER, tld);
    tld_graph_free(&g);
    cbm_arena_destroy(&arena);
    return ok ? 0 : -1;
}
