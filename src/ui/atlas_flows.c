/*
 * atlas_flows.c — CBM Atlas flows: named entry→terminal call journeys.
 *
 * A flow is what a human reads to learn how the codebase RUNS: pick an
 * entry point, follow CALLS with bounded depth and branching, stop at
 * sinks. Deterministic (ties break by id), honest about truncation
 * (dropped candidates and capped steps are counted, never silent), and
 * cached per (project, indexed_at) like the region level.
 *
 * Entry scoring (deterministic, from stored data only):
 *   +4  is_entry_point / url_path (route handler) property
 *   +2  no callers but real callees (a root of the call DAG)
 *   +1  entry-ish name (main, run*, handle*, serve*, cmd*, on[A-Z]…, *_main)
 *   ratio bonus: callees outweigh callers
 */
#include "foundation/constants.h"
#include "ui/atlas.h"
#include "foundation/hash_table.h"

#include <sqlite3.h>
#include <yyjson/yyjson.h>

#include <ctype.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    FLOW_MAX_CANDIDATES = 200, /* scored entries considered */
    FLOW_MAX_FLOWS = 60,       /* flows kept after dedup + rank */
    FLOW_MAX_DEPTH = 6,        /* call depth per flow */
    FLOW_MAX_CHILDREN = 4,     /* callees followed per step */
    FLOW_MAX_STEPS = 40,       /* steps per flow */
    FLOW_MIN_STEPS = 3,        /* smaller journeys teach nothing */
    FLOW_NODE_CAP = 500000,    /* callable-graph size bound for the cache */
};

/* ── In-memory call graph (cache) ─────────────────────────────── */

typedef struct {
    int64_t id;
    char *name;
    char *file_path;
    int32_t first_out; /* index into out_edges, -1 none */
    int32_t out_count;
    int32_t in_count;
    uint8_t flagged_entry; /* is_entry_point or route property */
} flow_node_t;

typedef struct {
    int32_t steps[FLOW_MAX_STEPS]; /* node indices, DFS order */
    int32_t parent[FLOW_MAX_STEPS];
    uint8_t depth[FLOW_MAX_STEPS];
    int step_count;
    int32_t entry;
    int32_t terminal; /* deepest sink (or deepest step) */
    bool sink_terminated;
    bool cross_region;
    int steps_capped;
} flow_t;

typedef struct {
    char key[1152];
    flow_node_t *nodes;
    int node_count;
    int32_t *out_edges; /* callee node indices, grouped by caller (CALLS) */
    /* Reverse CALLS adjacency: caller indices grouped by callee — the
     * impact question ("who can reach this?") walks edges backwards. */
    int32_t *in_first; /* per node: index into in_edges (in_count on the node) */
    int32_t *in_edges;
    /* DATA_FLOWS adjacency (same node set, its own CSR). */
    int32_t *data_first; /* per node: index into data_edges, -1 none */
    int32_t *data_count;
    int32_t *data_edges;
    flow_t *flows;
    int flow_count;
    int candidates_dropped;
    long long callable_total;
} flow_cache_t;

static flow_cache_t g_flow_cache;
static pthread_mutex_t g_flow_mu = PTHREAD_MUTEX_INITIALIZER;

static char *fl_strdup(const char *s) {
    if (!s)
        return NULL;
    size_t len = strlen(s) + 1;
    char *copy = malloc(len);
    if (copy)
        memcpy(copy, s, len);
    return copy;
}

static void flow_cache_clear_locked(void) {
    for (int i = 0; i < g_flow_cache.node_count; i++) {
        free(g_flow_cache.nodes[i].name);
        free(g_flow_cache.nodes[i].file_path);
    }
    free(g_flow_cache.nodes);
    free(g_flow_cache.out_edges);
    free(g_flow_cache.in_first);
    free(g_flow_cache.in_edges);
    free(g_flow_cache.data_first);
    free(g_flow_cache.data_count);
    free(g_flow_cache.data_edges);
    free(g_flow_cache.flows);
    memset(&g_flow_cache, 0, sizeof(g_flow_cache));
}

void cbm_atlas_flows_cache_clear(void) {
    pthread_mutex_lock(&g_flow_mu);
    flow_cache_clear_locked();
    pthread_mutex_unlock(&g_flow_mu);
}

static void flow_cache_key(cbm_store_t *store, const char *project, char *out, size_t cap) {
    char stamp[128] = {0};
    struct sqlite3 *db = cbm_store_get_db(store);
    sqlite3_stmt *st = NULL;
    if (db && sqlite3_prepare_v2(db, "SELECT indexed_at FROM projects WHERE name=?1", -1, &st,
                                 NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        if (sqlite3_step(st) == SQLITE_ROW) {
            const char *t = (const char *)sqlite3_column_text(st, 0);
            if (t)
                snprintf(stamp, sizeof(stamp), "%s", t);
        }
        sqlite3_finalize(st);
    }
    snprintf(out, cap, "%s|%s", project, stamp);
}

/* ── Entry scoring ────────────────────────────────────────────── */

static bool fl_entryish_name(const char *name) {
    if (!name || !name[0])
        return false;
    if (strcmp(name, "main") == 0 || strcmp(name, "run") == 0)
        return true;
    static const char *const prefixes[] = {"handle", "run_", "serve", "cmd_", "do_", "start"};
    for (size_t i = 0; i < sizeof(prefixes) / sizeof(prefixes[0]); i++)
        if (strncmp(name, prefixes[i], strlen(prefixes[i])) == 0)
            return true;
    if (name[0] == 'o' && name[1] == 'n' && isupper((unsigned char)name[2]))
        return true;
    size_t len = strlen(name);
    if (len > 5 && strcmp(name + len - 5, "_main") == 0)
        return true;
    return false;
}

static int fl_score(const flow_node_t *node) {
    /* Tests are journeys into the code, not of it: a test function is never
     * an interesting entry unless the indexer flagged it as a real one. */
    if (!node->flagged_entry && node->file_path && cbm_is_test_file_path(node->file_path))
        return 0;
    int score = 0;
    if (node->flagged_entry)
        score += 4;
    if (node->in_count == 0 && node->out_count > 0)
        score += 2;
    if (fl_entryish_name(node->name))
        score += 1;
    if (node->out_count > node->in_count)
        score += 1;
    return score;
}

typedef struct {
    int32_t node;
    int score;
} fl_candidate_t;

static int fl_candidate_cmp(const void *a, const void *b) {
    const fl_candidate_t *ca = a, *cb = b;
    if (cb->score != ca->score)
        return cb->score - ca->score;
    return ca->node - cb->node; /* deterministic tie-break */
}

/* Rank comparator: sink-terminated first, then longer, then entry order. */
static int fl_flow_cmp(const void *a, const void *b) {
    const flow_t *fa = a, *fb = b;
    if (fa->sink_terminated != fb->sink_terminated)
        return fb->sink_terminated - fa->sink_terminated;
    if (fb->step_count != fa->step_count)
        return fb->step_count - fa->step_count;
    return fa->entry - fb->entry;
}

/* ── Build ────────────────────────────────────────────────────── */

typedef struct {
    int64_t id;
    int32_t idx;
} fl_id_idx_t;

static int fl_id_idx_cmp(const void *a, const void *b) {
    int64_t ia = ((const fl_id_idx_t *)a)->id, ib = ((const fl_id_idx_t *)b)->id;
    return (ia > ib) - (ia < ib);
}

static int32_t fl_index_of(const fl_id_idx_t *map, int n, int64_t id) {
    int lo = 0, hi = n - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (map[mid].id == id)
            return map[mid].idx;
        if (map[mid].id < id)
            lo = mid + 1;
        else
            hi = mid - 1;
    }
    return -1;
}

/* DFS from `entry`, children capped and ordered by (desc out_count, id):
 * the busiest callee first reads as the "trunk" of the journey. */
static void fl_walk(const flow_cache_t *cache, flow_t *flow, int32_t entry) {
    memset(flow, 0, sizeof(*flow));
    flow->entry = entry;
    flow->terminal = entry;
    bool *visited = calloc((size_t)cache->node_count, sizeof(bool));
    if (!visited)
        return;

    typedef struct {
        int32_t node;
        int32_t parent_step;
        uint8_t depth;
    } frame_t;
    frame_t stack[FLOW_MAX_STEPS * FLOW_MAX_CHILDREN];
    int sp = 0;
    stack[sp++] = (frame_t){entry, -1, 0};
    int deepest = -1;
    while (sp > 0 && flow->step_count < FLOW_MAX_STEPS) {
        frame_t frame = stack[--sp];
        if (visited[frame.node])
            continue;
        visited[frame.node] = true;
        int step = flow->step_count;
        flow->steps[step] = frame.node;
        flow->parent[step] = frame.parent_step;
        flow->depth[step] = frame.depth;
        flow->step_count++;
        const flow_node_t *node = &cache->nodes[frame.node];
        if (frame.depth > deepest ||
            (frame.depth == deepest && node->out_count == 0 && !flow->sink_terminated)) {
            deepest = frame.depth;
            flow->terminal = frame.node;
            flow->sink_terminated = node->out_count == 0;
        }
        if (frame.depth >= FLOW_MAX_DEPTH)
            continue;
        /* Collect up to FLOW_MAX_CHILDREN unvisited callees, best last so
         * the busiest pops first (LIFO). */
        int32_t picked[FLOW_MAX_CHILDREN];
        int npicked = 0;
        for (int e = 0; e < node->out_count; e++) {
            int32_t callee = cache->out_edges[node->first_out + e];
            if (visited[callee])
                continue;
            /* Insertion into a small best-of list by (out_count desc, id). */
            int pos = npicked;
            while (pos > 0) {
                const flow_node_t *have = &cache->nodes[picked[pos - 1]];
                const flow_node_t *cand = &cache->nodes[callee];
                if (have->out_count > cand->out_count ||
                    (have->out_count == cand->out_count && picked[pos - 1] < callee))
                    break;
                pos--;
            }
            if (npicked < FLOW_MAX_CHILDREN) {
                for (int m = npicked; m > pos; m--)
                    picked[m] = picked[m - 1];
                picked[pos] = callee;
                npicked++;
            } else if (pos < FLOW_MAX_CHILDREN) {
                for (int m = FLOW_MAX_CHILDREN - 1; m > pos; m--)
                    picked[m] = picked[m - 1];
                picked[pos] = callee;
                flow->steps_capped++;
            } else {
                flow->steps_capped++;
            }
        }
        /* Push worst-first so the best is on top of the stack. */
        for (int m = npicked - 1; m >= 0; m--) {
            if (sp < (int)(sizeof(stack) / sizeof(stack[0])))
                stack[sp++] = (frame_t){picked[m], step, (uint8_t)(frame.depth + 1)};
        }
    }
    free(visited);
}

static int fl_rebuild_locked(cbm_store_t *store, const char *project, const char *key) {
    flow_cache_clear_locked();
    struct sqlite3 *db = cbm_store_get_db(store);
    if (!db)
        return -1;

    /* 1. Callable nodes with entry flags. */
    sqlite3_stmt *st = NULL;
    const char *nsql =
        "SELECT id, name, file_path, "
        "COALESCE(json_extract(properties,'$.is_entry_point'),0), "
        "CASE WHEN json_extract(properties,'$.url_path') IS NOT NULL THEN 1 ELSE 0 END "
        "FROM nodes WHERE project=?1 AND label IN (" CBM_SQL_CALLABLE_LABELS ") "
        "ORDER BY id LIMIT ?2";
    if (sqlite3_prepare_v2(db, nsql, -1, &st, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
    sqlite3_bind_int(st, 2, FLOW_NODE_CAP + 1);
    int cap = 0, n = 0;
    flow_node_t *nodes = NULL;
    while (sqlite3_step(st) == SQLITE_ROW) {
        if (n >= cap) {
            int nc = cap ? cap * 2 : 4096;
            flow_node_t *grown = realloc(nodes, (size_t)nc * sizeof(flow_node_t));
            if (!grown)
                break;
            nodes = grown;
            cap = nc;
        }
        memset(&nodes[n], 0, sizeof(flow_node_t));
        nodes[n].id = sqlite3_column_int64(st, 0);
        nodes[n].name = fl_strdup((const char *)sqlite3_column_text(st, 1));
        nodes[n].file_path = fl_strdup((const char *)sqlite3_column_text(st, 2));
        nodes[n].first_out = -1;
        nodes[n].flagged_entry = sqlite3_column_int(st, 3) != 0 || sqlite3_column_int(st, 4) != 0;
        n++;
    }
    sqlite3_finalize(st);
    if (n == 0 || n > FLOW_NODE_CAP) {
        /* Empty graph, or beyond the honest bound — no flows rather than
         * silently partial ones. */
        g_flow_cache.nodes = nodes;
        g_flow_cache.node_count = n > FLOW_NODE_CAP ? 0 : n;
        g_flow_cache.callable_total = n;
        if (n > FLOW_NODE_CAP) {
            for (int i = 0; i < n; i++) {
                free(nodes[i].name);
                free(nodes[i].file_path);
            }
            free(nodes);
            g_flow_cache.nodes = NULL;
        }
        snprintf(g_flow_cache.key, sizeof(g_flow_cache.key), "%s", key);
        return 0;
    }

    fl_id_idx_t *idmap = malloc((size_t)n * sizeof(fl_id_idx_t));
    if (!idmap) {
        g_flow_cache.nodes = nodes;
        g_flow_cache.node_count = n;
        flow_cache_clear_locked();
        return -1;
    }
    for (int i = 0; i < n; i++)
        idmap[i] = (fl_id_idx_t){nodes[i].id, i};
    qsort(idmap, (size_t)n, sizeof(fl_id_idx_t), fl_id_idx_cmp);

    /* 2. CALLS edges → CSR adjacency (two passes over one query result
     * would need materialization; count first via GROUP BY, then fill). */
    int32_t *src = NULL, *dst = NULL;
    int ecap = 0, ne = 0;
    if (sqlite3_prepare_v2(db,
                           "SELECT source_id, target_id FROM edges WHERE project=?1 AND "
                           "type='CALLS'",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            int32_t si = fl_index_of(idmap, n, sqlite3_column_int64(st, 0));
            int32_t di = fl_index_of(idmap, n, sqlite3_column_int64(st, 1));
            if (si < 0 || di < 0 || si == di)
                continue;
            if (ne >= ecap) {
                int nc = ecap ? ecap * 2 : 8192;
                int32_t *ns = realloc(src, (size_t)nc * sizeof(int32_t));
                int32_t *nd = realloc(dst, (size_t)nc * sizeof(int32_t));
                if (!ns || !nd) {
                    free(ns ? ns : src);
                    free(nd ? nd : dst);
                    src = dst = NULL;
                    ne = 0;
                    break;
                }
                src = ns;
                dst = nd;
                ecap = nc;
            }
            src[ne] = si;
            dst[ne] = di;
            ne++;
        }
        sqlite3_finalize(st);
    }

    /* DATA_FLOWS among the same callables (the "follow the value" family). */
    int32_t *dsrc = NULL, *ddst = NULL;
    int dcap = 0, dne = 0;
    if (sqlite3_prepare_v2(db,
                           "SELECT source_id, target_id FROM edges WHERE project=?1 AND "
                           "type='DATA_FLOWS'",
                           -1, &st, NULL) == SQLITE_OK) {
        sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
        while (sqlite3_step(st) == SQLITE_ROW) {
            int32_t si = fl_index_of(idmap, n, sqlite3_column_int64(st, 0));
            int32_t di = fl_index_of(idmap, n, sqlite3_column_int64(st, 1));
            if (si < 0 || di < 0 || si == di)
                continue;
            if (dne >= dcap) {
                int nc = dcap ? dcap * 2 : 1024;
                int32_t *ns = realloc(dsrc, (size_t)nc * sizeof(int32_t));
                int32_t *nd = realloc(ddst, (size_t)nc * sizeof(int32_t));
                if (!ns || !nd) {
                    free(ns ? ns : dsrc);
                    free(nd ? nd : ddst);
                    dsrc = ddst = NULL;
                    dne = 0;
                    break;
                }
                dsrc = ns;
                ddst = nd;
                dcap = nc;
            }
            dsrc[dne] = si;
            ddst[dne] = di;
            dne++;
        }
        sqlite3_finalize(st);
    }
    free(idmap);

    g_flow_cache.data_first = malloc((size_t)n * sizeof(int32_t));
    g_flow_cache.data_count = calloc((size_t)n, sizeof(int32_t));
    g_flow_cache.data_edges = malloc(dne > 0 ? (size_t)dne * sizeof(int32_t) : sizeof(int32_t));
    if (g_flow_cache.data_first && g_flow_cache.data_count && g_flow_cache.data_edges) {
        for (int e = 0; e < dne; e++)
            g_flow_cache.data_count[dsrc[e]]++;
        int32_t offset = 0;
        for (int i = 0; i < n; i++) {
            g_flow_cache.data_first[i] = offset;
            offset += g_flow_cache.data_count[i];
        }
        int32_t *cursor2 = calloc((size_t)n, sizeof(int32_t));
        if (cursor2) {
            for (int e = 0; e < dne; e++)
                g_flow_cache.data_edges[g_flow_cache.data_first[dsrc[e]] + cursor2[dsrc[e]]++] =
                    ddst[e];
        }
        free(cursor2);
    }
    free(dsrc);
    free(ddst);

    for (int e = 0; e < ne; e++) {
        nodes[src[e]].out_count++;
        nodes[dst[e]].in_count++;
    }
    int32_t *out_edges = malloc(ne > 0 ? (size_t)ne * sizeof(int32_t) : sizeof(int32_t));
    int32_t *cursor = calloc((size_t)n, sizeof(int32_t));
    if (out_edges && cursor) {
        int32_t offset = 0;
        for (int i = 0; i < n; i++) {
            nodes[i].first_out = offset;
            offset += nodes[i].out_count;
        }
        for (int e = 0; e < ne; e++)
            out_edges[nodes[src[e]].first_out + cursor[src[e]]++] = dst[e];
    }
    free(cursor);

    /* Reverse CSR over the same edge list (in_count already tallied). */
    g_flow_cache.in_first = malloc((size_t)n * sizeof(int32_t));
    g_flow_cache.in_edges = malloc(ne > 0 ? (size_t)ne * sizeof(int32_t) : sizeof(int32_t));
    int32_t *rcursor = calloc((size_t)n, sizeof(int32_t));
    if (g_flow_cache.in_first && g_flow_cache.in_edges && rcursor) {
        int32_t offset = 0;
        for (int i = 0; i < n; i++) {
            g_flow_cache.in_first[i] = offset;
            offset += nodes[i].in_count;
        }
        for (int e = 0; e < ne; e++)
            g_flow_cache.in_edges[g_flow_cache.in_first[dst[e]] + rcursor[dst[e]]++] = src[e];
    } else {
        free(g_flow_cache.in_first);
        free(g_flow_cache.in_edges);
        g_flow_cache.in_first = NULL;
        g_flow_cache.in_edges = NULL;
    }
    free(rcursor);
    free(src);
    free(dst);
    if (!out_edges) {
        for (int i = 0; i < n; i++) {
            free(nodes[i].name);
            free(nodes[i].file_path);
        }
        free(nodes);
        free(g_flow_cache.in_first);
        free(g_flow_cache.in_edges);
        g_flow_cache.in_first = NULL;
        g_flow_cache.in_edges = NULL;
        return -1;
    }

    g_flow_cache.nodes = nodes;
    g_flow_cache.node_count = n;
    g_flow_cache.out_edges = out_edges;
    g_flow_cache.callable_total = n;

    /* 3. Score candidates, walk flows, dedup by (entry, terminal). */
    fl_candidate_t *cands = malloc((size_t)n * sizeof(fl_candidate_t));
    if (!cands)
        return -1;
    int ncand = 0;
    for (int i = 0; i < n; i++) {
        int score = fl_score(&nodes[i]);
        if (score > 0 && nodes[i].out_count > 0)
            cands[ncand++] = (fl_candidate_t){i, score};
    }
    qsort(cands, (size_t)ncand, sizeof(fl_candidate_t), fl_candidate_cmp);
    int considered = ncand < FLOW_MAX_CANDIDATES ? ncand : FLOW_MAX_CANDIDATES;
    g_flow_cache.candidates_dropped = ncand - considered;

    flow_t *flows = malloc(sizeof(flow_t) * FLOW_MAX_FLOWS * 2);
    CBMHashTable *seen_pair = cbm_ht_create(256);
    char **pair_keys = calloc(FLOW_MAX_FLOWS * 2, sizeof(char *));
    int nflows = 0;
    if (flows && seen_pair && pair_keys) {
        for (int c = 0; c < considered && nflows < FLOW_MAX_FLOWS * 2; c++) {
            flow_t flow;
            fl_walk(&g_flow_cache, &flow, cands[c].node);
            if (flow.step_count < FLOW_MIN_STEPS)
                continue;
            char pair[64];
            snprintf(pair, sizeof(pair), "%lld>%lld", (long long)nodes[flow.entry].id,
                     (long long)nodes[flow.terminal].id);
            if (cbm_ht_has(seen_pair, pair))
                continue;
            pair_keys[nflows] = fl_strdup(pair);
            if (pair_keys[nflows])
                cbm_ht_set(seen_pair, pair_keys[nflows], (void *)(intptr_t)1);
            /* Cross-region when any two steps live in different regions. */
            int first_region = -1;
            flow.cross_region = false;
            for (int s2 = 0; s2 < flow.step_count && !flow.cross_region; s2++) {
                const char *fp = nodes[flow.steps[s2]].file_path;
                if (!fp)
                    continue;
                int region = cbm_layout_region_for_file(store, project, fp, NULL);
                if (region < 0)
                    continue;
                if (first_region < 0)
                    first_region = region;
                else if (region != first_region)
                    flow.cross_region = true;
            }
            flows[nflows++] = flow;
        }
    }
    for (int i = 0; i < FLOW_MAX_FLOWS * 2; i++)
        free(pair_keys ? pair_keys[i] : NULL);
    free(pair_keys);
    cbm_ht_free(seen_pair);
    free(cands);
    if (!flows)
        return -1;
    qsort(flows, (size_t)nflows, sizeof(flow_t), fl_flow_cmp);
    if (nflows > FLOW_MAX_FLOWS)
        nflows = FLOW_MAX_FLOWS;
    g_flow_cache.flows = flows;
    g_flow_cache.flow_count = nflows;
    snprintf(g_flow_cache.key, sizeof(g_flow_cache.key), "%s", key);
    return 0;
}

static bool fl_cache_fresh_locked(const char *key) {
    return g_flow_cache.key[0] && strcmp(g_flow_cache.key, key) == 0;
}

static int fl_ensure_locked(cbm_store_t *store, const char *project) {
    char key[1152];
    flow_cache_key(store, project, key, sizeof(key));
    if (fl_cache_fresh_locked(key))
        return 0;
    return fl_rebuild_locked(store, project, key);
}

/* ── JSON ─────────────────────────────────────────────────────── */

static void fl_add_node_ref(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *field,
                            const flow_node_t *node) {
    yyjson_mut_val *ref = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, ref, "id", node->id);
    yyjson_mut_obj_add_strcpy(doc, ref, "name", node->name ? node->name : "?");
    if (node->file_path)
        yyjson_mut_obj_add_strcpy(doc, ref, "file_path", node->file_path);
    yyjson_mut_obj_add_val(doc, obj, field, ref);
}

/* A display name with context when the bare name is ambiguous: dunders and
 * very short names get their file stem prefixed ("datastructures.__init__"),
 * because "__init__ → clone" teaches nobody anything. */
static void fl_display_name(const flow_node_t *node, char *out, size_t cap) {
    const char *name = node->name ? node->name : "?";
    bool ambiguous = name[0] == '_' || strlen(name) <= 3;
    if (ambiguous && node->file_path) {
        const char *base = strrchr(node->file_path, '/');
        base = base ? base + 1 : node->file_path;
        char stem[128];
        snprintf(stem, sizeof(stem), "%s", base);
        char *dot = strrchr(stem, '.');
        if (dot && dot != stem)
            *dot = '\0';
        snprintf(out, cap, "%s.%s", stem, name);
        return;
    }
    snprintf(out, cap, "%s", name);
}

char *cbm_atlas_flows_json(cbm_store_t *store, const char *project) {
    if (!store || !project)
        return NULL;
    pthread_mutex_lock(&g_flow_mu);
    if (fl_ensure_locked(store, project) != 0) {
        pthread_mutex_unlock(&g_flow_mu);
        return NULL;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "callable_total", g_flow_cache.callable_total);
    yyjson_mut_obj_add_int(doc, root, "candidates_dropped", g_flow_cache.candidates_dropped);
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < g_flow_cache.flow_count; i++) {
        const flow_t *flow = &g_flow_cache.flows[i];
        yyjson_mut_val *obj = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, obj, "id", i);
        char entry_name[192];
        char terminal_name[192];
        fl_display_name(&g_flow_cache.nodes[flow->entry], entry_name, sizeof(entry_name));
        fl_display_name(&g_flow_cache.nodes[flow->terminal], terminal_name, sizeof(terminal_name));
        char label[CBM_SZ_512];
        snprintf(label, sizeof(label), "%s → %s", entry_name, terminal_name);
        yyjson_mut_obj_add_strcpy(doc, obj, "label", label);
        fl_add_node_ref(doc, obj, "entry", &g_flow_cache.nodes[flow->entry]);
        fl_add_node_ref(doc, obj, "terminal", &g_flow_cache.nodes[flow->terminal]);
        yyjson_mut_obj_add_int(doc, obj, "steps", flow->step_count);
        yyjson_mut_obj_add_bool(doc, obj, "sink_terminated", flow->sink_terminated);
        yyjson_mut_obj_add_bool(doc, obj, "cross_region", flow->cross_region);
        if (flow->steps_capped > 0)
            yyjson_mut_obj_add_int(doc, obj, "steps_capped", flow->steps_capped);
        yyjson_mut_arr_append(arr, obj);
    }
    yyjson_mut_obj_add_val(doc, root, "flows", arr);
    pthread_mutex_unlock(&g_flow_mu);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

char *cbm_atlas_flow_json(cbm_store_t *store, const char *project, int flow_id) {
    if (!store || !project || flow_id < 0)
        return NULL;
    pthread_mutex_lock(&g_flow_mu);
    if (fl_ensure_locked(store, project) != 0 || flow_id >= g_flow_cache.flow_count) {
        pthread_mutex_unlock(&g_flow_mu);
        return NULL;
    }
    const flow_t *flow = &g_flow_cache.flows[flow_id];
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "id", flow_id);
    fl_add_node_ref(doc, root, "entry", &g_flow_cache.nodes[flow->entry]);
    fl_add_node_ref(doc, root, "terminal", &g_flow_cache.nodes[flow->terminal]);
    yyjson_mut_obj_add_bool(doc, root, "sink_terminated", flow->sink_terminated);
    yyjson_mut_obj_add_bool(doc, root, "cross_region", flow->cross_region);
    if (flow->steps_capped > 0)
        yyjson_mut_obj_add_int(doc, root, "steps_capped", flow->steps_capped);
    /* Per-step resolver confidence: the CALLS edge from the parent step.
     * A low value marks steps the resolver was unsure about (e.g. a short
     * name matched across languages) — shown, never hidden. */
    struct sqlite3 *conf_db = cbm_store_get_db(store);
    sqlite3_stmt *conf_st = NULL;
    if (conf_db)
        sqlite3_prepare_v2(conf_db,
                           "SELECT properties FROM edges WHERE project=?1 AND source_id=?2 "
                           "AND target_id=?3 AND type='CALLS' LIMIT 1",
                           -1, &conf_st, NULL);
    yyjson_mut_val *steps = yyjson_mut_arr(doc);
    for (int s = 0; s < flow->step_count; s++) {
        const flow_node_t *node = &g_flow_cache.nodes[flow->steps[s]];
        yyjson_mut_val *obj = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, obj, "id", node->id);
        yyjson_mut_obj_add_strcpy(doc, obj, "name", node->name ? node->name : "?");
        if (node->file_path)
            yyjson_mut_obj_add_strcpy(doc, obj, "file_path", node->file_path);
        yyjson_mut_obj_add_int(doc, obj, "depth", flow->depth[s]);
        yyjson_mut_obj_add_int(doc, obj, "parent", flow->parent[s]);
        if (conf_st && flow->parent[s] >= 0) {
            sqlite3_reset(conf_st);
            sqlite3_bind_text(conf_st, 1, project, -1, SQLITE_STATIC);
            sqlite3_bind_int64(conf_st, 2, g_flow_cache.nodes[flow->steps[flow->parent[s]]].id);
            sqlite3_bind_int64(conf_st, 3, node->id);
            if (sqlite3_step(conf_st) == SQLITE_ROW) {
                const char *props = (const char *)sqlite3_column_text(conf_st, 0);
                if (props) {
                    yyjson_doc *pd = yyjson_read(props, strlen(props), 0);
                    if (pd) {
                        yyjson_val *cv = yyjson_obj_get(yyjson_doc_get_root(pd), "confidence");
                        if (cv)
                            yyjson_mut_obj_add_real(doc, obj, "confidence", yyjson_get_num(cv));
                        yyjson_doc_free(pd);
                    }
                }
            }
        }
        yyjson_mut_arr_append(steps, obj);
    }
    if (conf_st)
        sqlite3_finalize(conf_st);
    yyjson_mut_obj_add_val(doc, root, "steps", steps);
    pthread_mutex_unlock(&g_flow_mu);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* ── A→B trace: shortest path over one edge family ────────────── */

enum { TRACE_MAX_DEPTH = 12 };

static int32_t fl_resolve_endpoint(struct sqlite3 *db, const char *project, int64_t id,
                                   const char *qn) {
    if (id < 0 && qn && qn[0] && db) {
        sqlite3_stmt *st = NULL;
        if (sqlite3_prepare_v2(db,
                               "SELECT id FROM nodes WHERE project=?1 AND "
                               "qualified_name=?2 ORDER BY id LIMIT 1",
                               -1, &st, NULL) == SQLITE_OK) {
            sqlite3_bind_text(st, 1, project, -1, SQLITE_STATIC);
            sqlite3_bind_text(st, 2, qn, -1, SQLITE_STATIC);
            if (sqlite3_step(st) == SQLITE_ROW)
                id = sqlite3_column_int64(st, 0);
            sqlite3_finalize(st);
        }
    }
    if (id < 0)
        return -1;
    /* Map the store id into the cache index (nodes are ordered by id). */
    int lo = 0, hi = g_flow_cache.node_count - 1;
    while (lo <= hi) {
        int mid = lo + (hi - lo) / 2;
        if (g_flow_cache.nodes[mid].id == id)
            return mid;
        if (g_flow_cache.nodes[mid].id < id)
            lo = mid + 1;
        else
            hi = mid - 1;
    }
    return -1;
}

char *cbm_atlas_trace_json(cbm_store_t *store, const char *project, int64_t from_id,
                           const char *from_qn, int64_t to_id, const char *to_qn,
                           const char *mode) {
    if (!store || !project)
        return NULL;
    bool data_mode = mode && strcmp(mode, "data") == 0;
    pthread_mutex_lock(&g_flow_mu);
    if (fl_ensure_locked(store, project) != 0 || g_flow_cache.node_count == 0) {
        pthread_mutex_unlock(&g_flow_mu);
        return NULL;
    }
    struct sqlite3 *db = cbm_store_get_db(store);
    int32_t from = fl_resolve_endpoint(db, project, from_id, from_qn);
    int32_t to = fl_resolve_endpoint(db, project, to_id, to_qn);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "mode", data_mode ? "data" : "calls");
    yyjson_mut_obj_add_int(doc, root, "max_depth", TRACE_MAX_DEPTH);
    if (from < 0 || to < 0) {
        yyjson_mut_obj_add_bool(doc, root, "reachable", false);
        yyjson_mut_obj_add_str(doc, root, "error",
                               from < 0 ? "source is not an indexed callable"
                                        : "target is not an indexed callable");
        pthread_mutex_unlock(&g_flow_mu);
        char *json = yyjson_mut_write(doc, 0, NULL);
        yyjson_mut_doc_free(doc);
        return json;
    }

    /* BFS with parent tracking, bounded depth. */
    int n = g_flow_cache.node_count;
    int32_t *parent = malloc((size_t)n * sizeof(int32_t));
    uint8_t *depth = calloc((size_t)n, sizeof(uint8_t));
    int32_t *queue = malloc((size_t)n * sizeof(int32_t));
    long long explored = 0;
    bool found = from == to;
    if (parent && depth && queue) {
        for (int i = 0; i < n; i++)
            parent[i] = -2; /* unvisited */
        int head = 0, tail = 0;
        queue[tail++] = from;
        parent[from] = -1;
        while (head < tail && !found) {
            int32_t node = queue[head++];
            if (depth[node] >= TRACE_MAX_DEPTH)
                continue;
            const int32_t first =
                data_mode ? g_flow_cache.data_first[node] : g_flow_cache.nodes[node].first_out;
            const int32_t count =
                data_mode ? g_flow_cache.data_count[node] : g_flow_cache.nodes[node].out_count;
            for (int e = 0; e < count; e++) {
                int32_t next = data_mode ? g_flow_cache.data_edges[first + e]
                                         : g_flow_cache.out_edges[first + e];
                if (parent[next] != -2)
                    continue;
                parent[next] = node;
                depth[next] = (uint8_t)(depth[node] + 1);
                explored++;
                if (next == to) {
                    found = true;
                    break;
                }
                queue[tail++] = next;
            }
        }
    }
    yyjson_mut_obj_add_bool(doc, root, "reachable", found);
    yyjson_mut_obj_add_int(doc, root, "explored", explored);
    if (found && parent) {
        /* Reconstruct, then emit source→target order. */
        int32_t chain[TRACE_MAX_DEPTH + 2];
        int chain_len = 0;
        for (int32_t node = to; node != -1 && chain_len < TRACE_MAX_DEPTH + 2;
             node = from == node ? -1 : parent[node])
            chain[chain_len++] = node;
        yyjson_mut_val *path = yyjson_mut_arr(doc);
        for (int i = chain_len - 1; i >= 0; i--) {
            const flow_node_t *node = &g_flow_cache.nodes[chain[i]];
            yyjson_mut_val *obj = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_int(doc, obj, "id", node->id);
            yyjson_mut_obj_add_strcpy(doc, obj, "name", node->name ? node->name : "?");
            if (node->file_path)
                yyjson_mut_obj_add_strcpy(doc, obj, "file_path", node->file_path);
            yyjson_mut_arr_append(path, obj);
        }
        yyjson_mut_obj_add_val(doc, root, "path", path);
        yyjson_mut_obj_add_int(doc, root, "hops", chain_len - 1);
    }
    free(parent);
    free(depth);
    free(queue);
    pthread_mutex_unlock(&g_flow_mu);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}

/* ── Impact: who can reach this symbol? ───────────────────────────
 *
 * The change-impact question ("if I change this, what breaks — and what
 * must I retest?") is a reverse-reachability walk over CALLS edges:
 * every symbol that can reach the target could notice a change to it.
 * Distance-labelled BFS, honest caps, regions aggregated for the map
 * view, and test-file reachers reported separately — those are the
 * tests to run first. Unresolved dynamic calls are not in the edge set,
 * so the count is a floor, never a ceiling; the UI says so. */

enum {
    IMPACT_MAX_DEPTH = 10,
    IMPACT_VISIT_CAP = 50000,
    IMPACT_NEAREST_CAP = 30,
    IMPACT_TESTS_CAP = 20,
    IMPACT_REGIONS_CAP = 64,
    IMPACT_REGIONS_SHOWN = 12,
};

typedef struct {
    char *name; /* owned */
    int count;
} impact_region_t;

static void impact_add_node(yyjson_mut_doc *doc, yyjson_mut_val *arr, const flow_node_t *node,
                            int distance) {
    yyjson_mut_val *obj = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, obj, "id", node->id);
    yyjson_mut_obj_add_strcpy(doc, obj, "name", node->name ? node->name : "?");
    if (node->file_path)
        yyjson_mut_obj_add_strcpy(doc, obj, "file_path", node->file_path);
    yyjson_mut_obj_add_int(doc, obj, "distance", distance);
    yyjson_mut_arr_append(arr, obj);
}

char *cbm_atlas_impact_json(cbm_store_t *store, const char *project, int64_t node_id,
                            const char *node_qn) {
    if (!store || !project)
        return NULL;
    pthread_mutex_lock(&g_flow_mu);
    if (fl_ensure_locked(store, project) != 0 || g_flow_cache.node_count == 0 ||
        !g_flow_cache.in_first || !g_flow_cache.in_edges) {
        pthread_mutex_unlock(&g_flow_mu);
        return NULL;
    }
    struct sqlite3 *db = cbm_store_get_db(store);
    int32_t origin = fl_resolve_endpoint(db, project, node_id, node_qn);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "basis", "CALLS");
    yyjson_mut_obj_add_int(doc, root, "max_depth", IMPACT_MAX_DEPTH);
    yyjson_mut_obj_add_int(doc, root, "visit_cap", IMPACT_VISIT_CAP);
    yyjson_mut_obj_add_int(doc, root, "callable_total", g_flow_cache.callable_total);
    if (origin < 0) {
        yyjson_mut_obj_add_str(doc, root, "error", "symbol is not an indexed callable");
        pthread_mutex_unlock(&g_flow_mu);
        char *json = yyjson_mut_write(doc, 0, NULL);
        yyjson_mut_doc_free(doc);
        return json;
    }

    const int n = g_flow_cache.node_count;
    const flow_node_t *nodes = g_flow_cache.nodes;
    yyjson_mut_val *node_obj = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, node_obj, "id", nodes[origin].id);
    yyjson_mut_obj_add_strcpy(doc, node_obj, "name", nodes[origin].name ? nodes[origin].name : "?");
    if (nodes[origin].file_path)
        yyjson_mut_obj_add_strcpy(doc, node_obj, "file_path", nodes[origin].file_path);
    yyjson_mut_obj_add_val(doc, root, "node", node_obj);

    uint8_t *dist = calloc((size_t)n, sizeof(uint8_t));
    uint8_t *seen = calloc((size_t)n, sizeof(uint8_t));
    int32_t *queue = malloc((size_t)n * sizeof(int32_t));
    yyjson_mut_val *nearest = yyjson_mut_arr(doc);
    yyjson_mut_val *tests_nearest = yyjson_mut_arr(doc);
    impact_region_t regions[IMPACT_REGIONS_CAP];
    int region_count = 0;
    long long by_distance[IMPACT_MAX_DEPTH] = {0};
    long long reachable = 0, test_count = 0, unregioned = 0;
    int nearest_emitted = 0, tests_emitted = 0, max_dist = 0;
    bool truncated = false, depth_capped = false;
    /* file → region-name cache: keys borrow the cache's file_path (stable
     * under g_flow_mu); values are region index + 2 (1 = no region). */
    CBMHashTable *file_region = cbm_ht_create(1024);

    if (dist && seen && queue && file_region) {
        int head = 0, tail = 0;
        queue[tail++] = origin;
        seen[origin] = 1;
        long long visited = 0;
        while (head < tail) {
            const int32_t node = queue[head++];
            if (dist[node] >= IMPACT_MAX_DEPTH) {
                if (nodes[node].in_count > 0)
                    depth_capped = true;
                continue;
            }
            const int32_t first = g_flow_cache.in_first[node];
            for (int e = 0; e < nodes[node].in_count; e++) {
                const int32_t caller = g_flow_cache.in_edges[first + e];
                if (seen[caller])
                    continue;
                if (visited >= IMPACT_VISIT_CAP) {
                    truncated = true;
                    break;
                }
                seen[caller] = 1;
                dist[caller] = (uint8_t)(dist[node] + 1);
                visited++;
                reachable++;
                by_distance[dist[caller] - 1]++;
                if (dist[caller] > max_dist)
                    max_dist = dist[caller];
                const char *fp = nodes[caller].file_path;
                const bool is_test = fp && cbm_is_test_file_path(fp);
                if (is_test) {
                    test_count++;
                    if (tests_emitted < IMPACT_TESTS_CAP) {
                        impact_add_node(doc, tests_nearest, &nodes[caller], dist[caller]);
                        tests_emitted++;
                    }
                } else {
                    if (nearest_emitted < IMPACT_NEAREST_CAP) {
                        impact_add_node(doc, nearest, &nodes[caller], dist[caller]);
                        nearest_emitted++;
                    }
                    /* Region rollup (non-test reachers only). */
                    int ridx = -1;
                    if (fp) {
                        void *hit = cbm_ht_get(file_region, fp);
                        if (hit) {
                            ridx = (int)(intptr_t)hit - 2;
                        } else {
                            char *rname = NULL;
                            int rid = cbm_layout_region_for_file(store, project, fp, &rname);
                            if (rid >= 0 && rname) {
                                for (int r = 0; r < region_count; r++)
                                    if (strcmp(regions[r].name, rname) == 0) {
                                        ridx = r;
                                        break;
                                    }
                                if (ridx < 0 && region_count < IMPACT_REGIONS_CAP) {
                                    regions[region_count].name = rname;
                                    regions[region_count].count = 0;
                                    ridx = region_count++;
                                    rname = NULL;
                                }
                            }
                            free(rname);
                            cbm_ht_set(file_region, fp, (void *)(intptr_t)(ridx + 2));
                        }
                    }
                    if (ridx >= 0)
                        regions[ridx].count++;
                    else
                        unregioned++;
                }
                queue[tail++] = caller;
            }
            if (truncated)
                break;
        }
    }

    yyjson_mut_obj_add_int(doc, root, "reachable", reachable);
    yyjson_mut_obj_add_int(doc, root, "max_distance", max_dist);
    yyjson_mut_obj_add_bool(doc, root, "truncated", truncated);
    yyjson_mut_obj_add_bool(doc, root, "depth_capped", depth_capped);
    yyjson_mut_val *by_d = yyjson_mut_arr(doc);
    for (int d = 0; d < max_dist; d++)
        yyjson_mut_arr_add_int(doc, by_d, by_distance[d]);
    yyjson_mut_obj_add_val(doc, root, "by_distance", by_d);

    /* Regions, largest first, honest tail. */
    for (int a = 0; a < region_count; a++)
        for (int b = a + 1; b < region_count; b++)
            if (regions[b].count > regions[a].count) {
                impact_region_t swap = regions[a];
                regions[a] = regions[b];
                regions[b] = swap;
            }
    yyjson_mut_val *rarr = yyjson_mut_arr(doc);
    const int shown = region_count < IMPACT_REGIONS_SHOWN ? region_count : IMPACT_REGIONS_SHOWN;
    for (int r = 0; r < shown; r++) {
        yyjson_mut_val *obj = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, obj, "name", regions[r].name);
        yyjson_mut_obj_add_int(doc, obj, "count", regions[r].count);
        yyjson_mut_arr_append(rarr, obj);
    }
    yyjson_mut_obj_add_val(doc, root, "regions", rarr);
    yyjson_mut_obj_add_int(doc, root, "regions_more", region_count - shown);
    yyjson_mut_obj_add_int(doc, root, "unregioned", unregioned);
    yyjson_mut_obj_add_val(doc, root, "nearest", nearest);
    yyjson_mut_val *tests_obj = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, tests_obj, "count", test_count);
    yyjson_mut_obj_add_val(doc, tests_obj, "nearest", tests_nearest);
    yyjson_mut_obj_add_val(doc, root, "tests", tests_obj);

    for (int r = 0; r < region_count; r++)
        free(regions[r].name);
    cbm_ht_free(file_region);
    free(dist);
    free(seen);
    free(queue);
    pthread_mutex_unlock(&g_flow_mu);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json;
}
