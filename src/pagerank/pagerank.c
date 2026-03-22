/*
 * pagerank.c — PageRank (node) + LinkRank (edge) ranking for codebase graphs.
 *
 * References:
 *   - aider repomap.py (github.com/Aider-AI/aider/blob/main/aider/repomap.py)
 *   - NetworkX pagerank (networkx/algorithms/link_analysis/pagerank_alg.py)
 *   - Kim et al. (2010) LinkRank, arXiv:0902.3728
 *   - nazgob/PageRank (github.com/nazgob/PageRank/blob/master/algorithm.c)
 */

#include "pagerank.h"
#include <foundation/log.h>
#include <foundation/platform.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sqlite3.h>

/* ── Default edge weights (aider/RepoMapper-inspired) ──────── */

const cbm_edge_weights_t CBM_DEFAULT_EDGE_WEIGHTS = {
    .calls = 1.0, .defines_method = 0.8, .defines = 0.5,
    .imports = 0.3, .usage = 0.2, .configures = 0.1,
    .http_calls = 0.5, .async_calls = 0.8, .default_weight = 0.3
};

/* ── Edge weight lookup (ordered by frequency) ─────────────── */

static double edge_type_weight(const cbm_edge_weights_t *w, const char *type) {
    if (!type) return w->default_weight;
    if (strcmp(type, "CALLS") == 0)          return w->calls;
    if (strcmp(type, "IMPORTS") == 0)        return w->imports;
    if (strcmp(type, "USAGE") == 0)          return w->usage;
    if (strcmp(type, "DEFINES") == 0)        return w->defines;
    if (strcmp(type, "DEFINES_METHOD") == 0) return w->defines_method;
    if (strcmp(type, "CONFIGURES") == 0)     return w->configures;
    if (strcmp(type, "HTTP_CALLS") == 0)     return w->http_calls;
    if (strcmp(type, "ASYNC_CALLS") == 0)    return w->async_calls;
    return w->default_weight;
}

/* ── Internal edge struct ────────────────────────────────────── */

typedef struct {
    int src_idx;
    int dst_idx;
    int64_t edge_id;
    double weight;
} pr_edge_t;

/* ── ISO timestamp helper ────────────────────────────────────── */

static void iso_now(char *buf, size_t sz) {
    time_t t = time(NULL);
    struct tm tm;
#ifdef _WIN32
    gmtime_s(&tm, &t);
#else
    gmtime_r(&t, &tm);
#endif
    strftime(buf, sz, "%Y-%m-%dT%H:%M:%SZ", &tm);
}

/* ── Hash map: node_id -> array index (linear probing) ──────── */

typedef struct {
    int64_t *keys;
    int *vals;
    int cap;
} id_map_t;

static int id_map_init(id_map_t *m, int n) {
    m->cap = n * CBM_HASHMAP_LOAD_FACTOR + 1;
    m->keys = calloc((size_t)m->cap, sizeof(int64_t));
    m->vals = calloc((size_t)m->cap, sizeof(int));
    if (!m->keys || !m->vals) {
        free(m->keys); free(m->vals);
        m->keys = NULL; m->vals = NULL;
        return -1;
    }
    memset(m->vals, -1, (size_t)m->cap * sizeof(int));
    return 0;
}

static void id_map_put(id_map_t *m, int64_t key, int val) {
    int h = (int)((uint64_t)key % (uint64_t)m->cap);
    while (m->keys[h] != 0 && m->keys[h] != key)
        h = (h + 1) % m->cap;
    m->keys[h] = key;
    m->vals[h] = val;
}

static int id_map_get(const id_map_t *m, int64_t key) {
    int h = (int)((uint64_t)key % (uint64_t)m->cap);
    while (m->keys[h] != 0) {
        if (m->keys[h] == key) return m->vals[h];
        h = (h + 1) % m->cap;
    }
    return -1;
}

static void id_map_free(id_map_t *m) {
    free(m->keys);
    free(m->vals);
    m->keys = NULL;
    m->vals = NULL;
}

/* ── Scope -> SQL WHERE clause (DRY: one function) ──────────── */

static const char *scope_where(cbm_rank_scope_t scope) {
    switch (scope) {
    case CBM_RANK_SCOPE_PROJECT: return "project = ?1";
    case CBM_RANK_SCOPE_DEPS:   return "project LIKE ?1 || '.dep.%'";
    case CBM_RANK_SCOPE_FULL:
    default:                     return "(project = ?1 OR project LIKE ?1 || '.dep.%')";
    }
}

/* ── Core PageRank + LinkRank ────────────────────────────────── */

int cbm_pagerank_compute(cbm_store_t *store, const char *project,
                         double damping, double epsilon, int max_iter,
                         const cbm_edge_weights_t *weights,
                         cbm_rank_scope_t scope) {
    if (!store || !project || !project[0]) return -1;
    if (!weights) weights = &CBM_DEFAULT_EDGE_WEIGHTS;
    if (damping < 0.0 || damping > 1.0) damping = CBM_PAGERANK_DAMPING;
    if (max_iter <= 0) max_iter = CBM_PAGERANK_MAX_ITER;
    if (epsilon <= 0.0) epsilon = CBM_PAGERANK_EPSILON;

    sqlite3 *db = cbm_store_get_db(store);
    if (!db) return -1;

    /* All heap pointers initialized to NULL for safe cleanup via goto */
    int64_t *node_ids = NULL;
    pr_edge_t *edges = NULL;
    double *out_weight = NULL, *rank = NULL, *new_rank = NULL;
    id_map_t map = {0};
    int N = 0, E = 0, result = -1;

    /* ── Step 1: Load node IDs ────────────────────────────── */
    char sql_buf[512];
    snprintf(sql_buf, sizeof(sql_buf), "SELECT id FROM nodes WHERE %s",
             scope_where(scope));

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql_buf, -1, &stmt, NULL) != SQLITE_OK)
        return -1;
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);

    int cap = CBM_PAGERANK_INITIAL_CAP;
    node_ids = malloc((size_t)cap * sizeof(int64_t));
    if (!node_ids) { sqlite3_finalize(stmt); return -1; }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (N >= cap) {
            cap *= 2;
            node_ids = safe_realloc(node_ids, (size_t)cap * sizeof(int64_t));
            if (!node_ids) { sqlite3_finalize(stmt); return -1; }
        }
        node_ids[N++] = sqlite3_column_int64(stmt, 0);
    }
    sqlite3_finalize(stmt);
    stmt = NULL;

    if (N == 0) { free(node_ids); return 0; }

    /* Build id->index map */
    if (id_map_init(&map, N) != 0) { free(node_ids); return -1; }
    for (int i = 0; i < N; i++) id_map_put(&map, node_ids[i], i);

    /* ── Step 2: Load weighted edges ──────────────────────── */
    snprintf(sql_buf, sizeof(sql_buf),
             "SELECT id, source_id, target_id, type FROM edges WHERE %s",
             scope_where(scope));
    if (sqlite3_prepare_v2(db, sql_buf, -1, &stmt, NULL) != SQLITE_OK)
        goto cleanup;
    sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);

    int ecap = CBM_PAGERANK_INITIAL_CAP;
    edges = malloc((size_t)ecap * sizeof(pr_edge_t));
    if (!edges) { sqlite3_finalize(stmt); goto cleanup; }

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        int64_t eid = sqlite3_column_int64(stmt, 0);
        int64_t src = sqlite3_column_int64(stmt, 1);
        int64_t dst = sqlite3_column_int64(stmt, 2);
        const char *type = (const char *)sqlite3_column_text(stmt, 3);

        int si = id_map_get(&map, src);
        int di = id_map_get(&map, dst);
        if (si < 0 || di < 0) continue;

        if (E >= ecap) {
            ecap *= 2;
            edges = safe_realloc(edges, (size_t)ecap * sizeof(pr_edge_t));
            if (!edges) { sqlite3_finalize(stmt); goto cleanup; }
        }
        edges[E].src_idx = si;
        edges[E].dst_idx = di;
        edges[E].edge_id = eid;
        edges[E].weight = edge_type_weight(weights, type);
        E++;
    }
    sqlite3_finalize(stmt);
    stmt = NULL;

    /* ── Step 3: Allocate computation buffers ─────────────── */
    out_weight = calloc((size_t)N, sizeof(double));
    rank = malloc((size_t)N * sizeof(double));
    new_rank = malloc((size_t)N * sizeof(double));
    if (!out_weight || !rank || !new_rank) goto cleanup;

    for (int e = 0; e < E; e++)
        out_weight[edges[e].src_idx] += edges[e].weight;

    /* ── Step 4: Power iteration ──────────────────────────── */
    double init_rank = 1.0 / N;
    for (int i = 0; i < N; i++) rank[i] = init_rank;

    double base = (1.0 - damping) / N;
    int iter;
    for (iter = 0; iter < max_iter; iter++) {
        for (int i = 0; i < N; i++) new_rank[i] = base;

        /* Distribute rank along weighted edges */
        for (int e = 0; e < E; e++) {
            int s = edges[e].src_idx;
            if (out_weight[s] > 0.0) {
                new_rank[edges[e].dst_idx] +=
                    damping * rank[s] * edges[e].weight / out_weight[s];
            }
        }

        /* Dangling node handling (NetworkX convention) */
        double dangling_sum = 0.0;
        for (int i = 0; i < N; i++) {
            if (out_weight[i] == 0.0) dangling_sum += rank[i];
        }
        if (dangling_sum > 0.0) {
            double add = damping * dangling_sum / N;
            for (int i = 0; i < N; i++) new_rank[i] += add;
        }

        /* Convergence: L2 norm of rank delta */
        double delta = 0.0;
        for (int i = 0; i < N; i++) {
            double d = new_rank[i] - rank[i];
            delta += d * d;
        }
        delta = sqrt(delta);

        /* Swap buffers */
        double *tmp = rank; rank = new_rank; new_rank = tmp;

        if (delta < epsilon) { iter++; break; }
    }

    /* ── Step 5: Store PageRank in db ─────────────────────── */
    char ts[CBM_ISO_TIMESTAMP_LEN];
    iso_now(ts, sizeof(ts));

    /* Clear old ranks for this scope */
    snprintf(sql_buf, sizeof(sql_buf), "DELETE FROM pagerank WHERE %s",
             scope_where(scope));
    if (sqlite3_prepare_v2(db, sql_buf, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        stmt = NULL;
    }

    /* Batch insert within transaction */
    sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
    const char *ins_sql =
        "INSERT OR REPLACE INTO pagerank "
        "(node_id, project, rank, computed_at) "
        "SELECT ?1, project, ?2, ?3 FROM nodes WHERE id = ?1";
    sqlite3_stmt *ins_stmt = NULL;
    if (sqlite3_prepare_v2(db, ins_sql, -1, &ins_stmt, NULL) == SQLITE_OK) {
        for (int i = 0; i < N; i++) {
            sqlite3_bind_int64(ins_stmt, 1, node_ids[i]);
            sqlite3_bind_double(ins_stmt, 2, rank[i]);
            sqlite3_bind_text(ins_stmt, 3, ts, -1, SQLITE_TRANSIENT);
            sqlite3_step(ins_stmt);
            sqlite3_reset(ins_stmt);
        }
        sqlite3_finalize(ins_stmt);
    }
    sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);

    /* ── Step 6: Compute LinkRank for edges ───────────────── */
    snprintf(sql_buf, sizeof(sql_buf), "DELETE FROM linkrank WHERE %s",
             scope_where(scope));
    if (sqlite3_prepare_v2(db, sql_buf, -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, project, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
        stmt = NULL;
    }

    const char *lr_sql =
        "INSERT OR REPLACE INTO linkrank "
        "(edge_id, project, rank, computed_at) "
        "SELECT ?1, project, ?2, ?3 FROM edges WHERE id = ?1";
    sqlite3_stmt *lr_stmt = NULL;
    if (sqlite3_prepare_v2(db, lr_sql, -1, &lr_stmt, NULL) == SQLITE_OK) {
        sqlite3_exec(db, "BEGIN", NULL, NULL, NULL);
        for (int e = 0; e < E; e++) {
            int s_idx = edges[e].src_idx;
            double lr = 0.0;
            if (out_weight[s_idx] > 0.0)
                lr = rank[s_idx] * edges[e].weight / out_weight[s_idx];
            sqlite3_bind_int64(lr_stmt, 1, edges[e].edge_id);
            sqlite3_bind_double(lr_stmt, 2, lr);
            sqlite3_bind_text(lr_stmt, 3, ts, -1, SQLITE_TRANSIENT);
            sqlite3_step(lr_stmt);
            sqlite3_reset(lr_stmt);
        }
        sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
        sqlite3_finalize(lr_stmt);
    }

    /* ── Logging ──────────────────────────────────────────── */
    char iter_s[CBM_LOG_INT_BUF], n_s[CBM_LOG_INT_BUF], e_s[CBM_LOG_INT_BUF];
    snprintf(iter_s, sizeof(iter_s), "%d", iter);
    snprintf(n_s, sizeof(n_s), "%d", N);
    snprintf(e_s, sizeof(e_s), "%d", E);
    cbm_log_info("pagerank.done", "project", project,
                 "nodes", n_s, "edges", e_s, "iterations", iter_s);

    result = N;

cleanup:
    if (stmt) sqlite3_finalize(stmt);  /* defensive: finalize any in-flight stmt */
    free(node_ids);
    id_map_free(&map);
    free(edges);
    free(out_weight);
    free(rank);
    free(new_rank);
    return result;
}

int cbm_pagerank_compute_default(cbm_store_t *store, const char *project) {
    return cbm_pagerank_compute(store, project,
        CBM_PAGERANK_DAMPING, CBM_PAGERANK_EPSILON,
        CBM_PAGERANK_MAX_ITER, &CBM_DEFAULT_EDGE_WEIGHTS,
        CBM_DEFAULT_RANK_SCOPE);
}

double cbm_pagerank_get(cbm_store_t *store, int64_t node_id) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) return 0.0;
    sqlite3_stmt *stmt = NULL;
    double r = 0.0;
    if (sqlite3_prepare_v2(db, "SELECT rank FROM pagerank WHERE node_id = ?1",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, node_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) r = sqlite3_column_double(stmt, 0);
        sqlite3_finalize(stmt);
    }
    return r;
}

double cbm_linkrank_get(cbm_store_t *store, int64_t edge_id) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) return 0.0;
    sqlite3_stmt *stmt = NULL;
    double r = 0.0;
    if (sqlite3_prepare_v2(db, "SELECT rank FROM linkrank WHERE edge_id = ?1",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_int64(stmt, 1, edge_id);
        if (sqlite3_step(stmt) == SQLITE_ROW) r = sqlite3_column_double(stmt, 0);
        sqlite3_finalize(stmt);
    }
    return r;
}
