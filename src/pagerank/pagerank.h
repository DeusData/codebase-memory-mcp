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

#include <stdbool.h>
#include <pipeline/pipeline.h>
#include <store/store.h>

/* Forward declaration — full definition in cli/cli.h */
struct cbm_config;

/* ── Algorithm defaults (config-overridable) ──────────────── */

#define CBM_PAGERANK_DAMPING    0.85   /* Standard Google PageRank damping */
#define CBM_PAGERANK_EPSILON    1e-6   /* L2 convergence threshold */
#define CBM_PAGERANK_MAX_ITER   20     /* Max power iterations */

/* Config keys for runtime tuning */
#define CBM_CONFIG_PAGERANK_MAX_ITER "pagerank_max_iter"
#define CBM_CONFIG_PAGERANK_DAMPING  "pagerank_damping"
#define CBM_CONFIG_PAGERANK_EPSILON  "pagerank_epsilon"
#define CBM_CONFIG_RANK_SCOPE        "rank_scope"
#define CBM_CONFIG_RANK_REFRESH      "rank_refresh"

#define CBM_RANK_REFRESH_EAGER          "eager"
#define CBM_RANK_REFRESH_STALE_ON_EXACT "stale_on_exact"
#define CBM_RANK_REFRESH_STALE_ON_INCREMENTAL "stale_on_incremental"
#define CBM_RANK_REFRESH_DEFAULT        CBM_RANK_REFRESH_STALE_ON_INCREMENTAL

typedef enum {
    CBM_RANK_REFRESH_PUBLISH_FULL = 0,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_EXACT = 1,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_CONTAINMENT = 2,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_NOOP = 3,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_FALLBACK = 4,
} cbm_rank_refresh_publish_t;

cbm_rank_refresh_publish_t
cbm_rank_refresh_publish_from_pipeline(cbm_pipeline_publish_kind_t publish_kind,
                                       bool incremental_fallback);

/* Config keys for edge type weights (all doubles, override via `config set`) */
#define CBM_CONFIG_EDGE_WEIGHT_CALLS          "edge_weight_calls"
#define CBM_CONFIG_EDGE_WEIGHT_DEFINES_METHOD  "edge_weight_defines_method"
#define CBM_CONFIG_EDGE_WEIGHT_DEFINES         "edge_weight_defines"
#define CBM_CONFIG_EDGE_WEIGHT_IMPORTS         "edge_weight_imports"
#define CBM_CONFIG_EDGE_WEIGHT_USAGE           "edge_weight_usage"
#define CBM_CONFIG_EDGE_WEIGHT_CONFIGURES      "edge_weight_configures"
#define CBM_CONFIG_EDGE_WEIGHT_HTTP_CALLS      "edge_weight_http_calls"
#define CBM_CONFIG_EDGE_WEIGHT_ASYNC_CALLS     "edge_weight_async_calls"
#define CBM_CONFIG_EDGE_WEIGHT_TESTS            "edge_weight_tests"
#define CBM_CONFIG_EDGE_WEIGHT_WRITES           "edge_weight_writes"
#define CBM_CONFIG_EDGE_WEIGHT_DECORATES        "edge_weight_decorates"
#define CBM_CONFIG_EDGE_WEIGHT_DEFAULT          "edge_weight_default"
#define CBM_CONFIG_EDGE_WEIGHT_MEMBER_OF       "edge_weight_member_of"

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
    double calls;           /* CALLS — direct function/method calls */
    double defines_method;  /* DEFINES_METHOD — class defines method (structural) */
    double defines;         /* DEFINES — module/file defines symbol (structural, low signal) */
    double imports;         /* IMPORTS — module imports */
    double usage;           /* USAGE — type references, attribute access, isinstance (high for Python) */
    double configures;      /* CONFIGURES — config file links */
    double http_calls;      /* HTTP_CALLS — cross-service calls */
    double async_calls;     /* ASYNC_CALLS — async function calls */
    double tests;           /* TESTS — test function tests production code (dampened) */
    double writes;          /* WRITES — function writes to variable/file */
    double decorates;       /* DECORATES — decorator applied to function */
    double default_weight;  /* Fallback for unknown edge types */
    double member_rank_factor; /* Fraction of member rank aggregated to parent class (0=disabled) */
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

/* Convenience: compute with config-backed rank settings.
 * Reads rank_scope, pagerank_* and edge_weight_* config keys; invalid values
 * fall back to the same defaults as cbm_pagerank_compute_default().
 * cfg may be NULL (uses defaults). */
int cbm_pagerank_compute_with_config(cbm_store_t *store, const char *project,
                                     struct cbm_config *cfg);

/* Refresh rank-derived views after an index publish when needed.
 * Computes when the graph changed, dependencies were reindexed, or existing
 * PageRank/LinkRank/node_degree views are missing/incomplete. With an opt-in
 * stale policy, eligible incremental publishes may defer recompute only when
 * rank-derived views are already marked stale. Returns ranked node count from
 * compute, 0 when skipped/deferred, or -1 on invalid input/compute error. cfg
 * may be NULL (uses defaults). */
int cbm_pagerank_refresh_after_publish(cbm_store_t *store, const char *project,
                                       struct cbm_config *cfg, bool graph_changed,
                                       int deps_reindexed,
                                       cbm_rank_refresh_publish_t publish_kind);

/* Backwards-compatible wrapper for older callers: exact_incremental_publish=true
 * maps to CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_EXACT, false maps to FULL. */
int cbm_pagerank_refresh_if_needed(cbm_store_t *store, const char *project,
                                   struct cbm_config *cfg, bool graph_changed,
                                   int deps_reindexed, bool exact_incremental_publish);

/* True only when PageRank, LinkRank, and node_degree derived views are all
 * recorded complete for the project. Missing rows return false so callers
 * repair older DBs instead of skipping necessary work. */
bool cbm_pagerank_views_complete(cbm_store_t *store, const char *project);

/* Get PageRank score for a single node. Returns 0.0 if not computed. */
double cbm_pagerank_get(cbm_store_t *store, int64_t node_id);

/* ── LinkRank API ─────────────────────────────────────────── */

/* Get LinkRank score for a single edge. Returns 0.0 if not computed. */
double cbm_linkrank_get(cbm_store_t *store, int64_t edge_id);

#endif /* CBM_PAGERANK_H */
