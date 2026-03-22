/* pagerank.h — PageRank (node) + LinkRank (edge) ranking for codebase graphs.
 *
 * References:
 *   - aider repomap (github.com/Aider-AI/aider/blob/main/aider/repomap.py)
 *   - NetworkX pagerank (networkx/algorithms/link_analysis/pagerank_alg.py)
 *   - RepoGraph (github.com/ozyyshr/RepoGraph) — peer-reviewed
 *   - Kim et al. (2010) LinkRank, arXiv:0902.3728
 */

#ifndef CBM_PAGERANK_H
#define CBM_PAGERANK_H

#include <store/store.h>

/* ── Algorithm defaults (config-overridable) ──────────────── */

#define CBM_PAGERANK_DAMPING    0.85   /* Standard Google PageRank damping */
#define CBM_PAGERANK_EPSILON    1e-6   /* L2 convergence threshold */
#define CBM_PAGERANK_MAX_ITER   20     /* Max power iterations */

/* Config keys for runtime tuning */
#define CBM_CONFIG_PAGERANK_MAX_ITER "pagerank_max_iter"
#define CBM_CONFIG_RANK_SCOPE        "rank_scope"

/* ── Internal tuning constants ────────────────────────────── */

#define CBM_PAGERANK_INITIAL_CAP  256  /* Initial array capacity for nodes/edges */
#define CBM_ISO_TIMESTAMP_LEN      32  /* ISO-8601 timestamp buffer size */
#define CBM_LOG_INT_BUF            16  /* int->string buffer for logging */
#define CBM_HASHMAP_LOAD_FACTOR     2  /* Hash map capacity = N * factor + 1 */

/* ── Scope control ────────────────────────────────────────── */

typedef enum {
    CBM_RANK_SCOPE_PROJECT = 0,  /* project nodes only */
    CBM_RANK_SCOPE_FULL    = 1,  /* project + all deps (default) */
    CBM_RANK_SCOPE_DEPS    = 2,  /* deps only */
} cbm_rank_scope_t;

#define CBM_DEFAULT_RANK_SCOPE CBM_RANK_SCOPE_FULL

/* ── Edge type weights ────────────────────────────────────── */

typedef struct {
    double calls;           /* CALLS edges — direct function calls */
    double defines_method;  /* DEFINES_METHOD — class->method */
    double defines;         /* DEFINES — declaration->definition */
    double imports;         /* IMPORTS — module imports */
    double usage;           /* USAGE — variable/type references */
    double configures;      /* CONFIGURES — config file links */
    double http_calls;      /* HTTP_CALLS — cross-service */
    double async_calls;     /* ASYNC_CALLS — async function calls */
    double default_weight;  /* Fallback for unknown edge types */
} cbm_edge_weights_t;

extern const cbm_edge_weights_t CBM_DEFAULT_EDGE_WEIGHTS;

/* ── PageRank API ─────────────────────────────────────────── */

/* Compute PageRank + LinkRank for all nodes/edges in a project scope.
 * Stores results in pagerank and linkrank tables.
 * Called after index_repository dump/flush.
 *
 * Runtime:  O(max_iter * (V + E)), typically 20 * (V + E).
 * Memory:   O(V) for rank arrays + O(E) for edge list.
 * Returns:  number of nodes ranked, or -1 on error. */
int cbm_pagerank_compute(cbm_store_t *store, const char *project,
                         double damping, double epsilon, int max_iter,
                         const cbm_edge_weights_t *weights,
                         cbm_rank_scope_t scope);

/* Convenience: compute with defaults (FULL scope, d=0.85, eps=1e-6, 20 iter) */
int cbm_pagerank_compute_default(cbm_store_t *store, const char *project);

/* Get PageRank score for a single node. Returns 0.0 if not computed. */
double cbm_pagerank_get(cbm_store_t *store, int64_t node_id);

/* ── LinkRank API ─────────────────────────────────────────── */

/* Get LinkRank score for a single edge. Returns 0.0 if not computed. */
double cbm_linkrank_get(cbm_store_t *store, int64_t edge_id);

#endif /* CBM_PAGERANK_H */
