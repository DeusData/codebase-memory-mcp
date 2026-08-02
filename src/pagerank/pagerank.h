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
#include <float.h>
#include <foundation/constants.h>
#include <limits.h>
#include <pipeline/pipeline.h>
#include <store/store.h>

/* Forward declaration — full definition in cli/cli.h */
struct cbm_config;

/* ── Algorithm defaults (config-overridable) ──────────────── */

/* NetworkX uses 0.85/100/1e-6. Keep those interoperable defaults while using
 * this implementation's L2 convergence test and fail-without-publication
 * contract. Damping is a probability, hence the exact closed [0,1] domain;
 * 0.7-0.9 is an advisory neighborhood around the established default that
 * trades propagation distance against convergence latency. */
#define CBM_PAGERANK_DAMPING 0.85
#define CBM_PAGERANK_DAMPING_STR CBM_STRINGIFY(CBM_PAGERANK_DAMPING)
#define CBM_PAGERANK_DAMPING_MIN 0.0
#define CBM_PAGERANK_DAMPING_MAX 1.0
#define CBM_PAGERANK_DAMPING_RECOMMENDED_MIN 0.7
#define CBM_PAGERANK_DAMPING_RECOMMENDED_MAX 0.9
#define CBM_PAGERANK_DAMPING_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_DAMPING_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_DAMPING_RECOMMENDED_MAX)

/* Epsilon must be positive and finite. DBL_MAX is the representation bound,
 * not an algorithmic cap. The 1e-8..1e-4 advisory window spans higher-accuracy
 * through lower-latency tuning around the 1e-6 default. */
#define CBM_PAGERANK_EPSILON 1e-6
#define CBM_PAGERANK_EPSILON_STR CBM_STRINGIFY(CBM_PAGERANK_EPSILON)
#define CBM_PAGERANK_EPSILON_MIN_EXCLUSIVE 0.0
#define CBM_PAGERANK_EPSILON_MAX DBL_MAX
#define CBM_PAGERANK_EPSILON_RECOMMENDED_MIN 1e-8
#define CBM_PAGERANK_EPSILON_RECOMMENDED_MAX 1e-4
#define CBM_PAGERANK_EPSILON_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_EPSILON_RECOMMENDED_MIN) \
    " to " CBM_STRINGIFY(CBM_PAGERANK_EPSILON_RECOMMENDED_MAX)
/* NetworkX's established PageRank default. Unlike the historical 20-step
 * value, this converges the repository's 100-node linear regression fixture at
 * the default epsilon; exhaustion now fails instead of publishing partial
 * ranks. One iteration is the mathematical minimum and INT_MAX is the actual
 * API representation maximum; start at 100, then select the smallest measured
 * budget above the workload's logged convergence point. */
#define CBM_PAGERANK_MAX_ITER 100
#define CBM_PAGERANK_MAX_ITER_STR CBM_STRINGIFY(CBM_PAGERANK_MAX_ITER)
#define CBM_PAGERANK_MAX_ITER_MIN 1
#define CBM_PAGERANK_MAX_ITER_MAX INT_MAX
#define CBM_PAGERANK_MAX_ITER_RECOMMENDED_START CBM_PAGERANK_MAX_ITER_STR

/* Config keys for runtime tuning */
#define CBM_CONFIG_PAGERANK_MAX_ITER "pagerank_max_iter"
#define CBM_CONFIG_PAGERANK_DAMPING "pagerank_damping"
#define CBM_CONFIG_PAGERANK_EPSILON "pagerank_epsilon"
#define CBM_CONFIG_RANK_SCOPE "rank_scope"
#define CBM_CONFIG_RANK_REFRESH "rank_refresh"
#define CBM_CONFIG_RANK_ENABLED "rank_enabled"

#define CBM_RANK_REFRESH_AT_PUBLISH "at_publish"
#define CBM_RANK_REFRESH_DEFER_EXACT_DELTA_REINDEXES "defer_exact_delta_reindexes"
#define CBM_RANK_REFRESH_DEFER_ALL_INCREMENTAL_REINDEXES "defer_all_incremental_reindexes"
#define CBM_RANK_REFRESH_DEFAULT CBM_RANK_REFRESH_DEFER_ALL_INCREMENTAL_REINDEXES

typedef enum {
    CBM_RANK_REFRESH_PUBLISH_FULL = 0,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_EXACT = 1,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_CONTAINMENT = 2,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_NOOP = 3,
    CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_FALLBACK = 4,
} cbm_rank_refresh_publish_t;

cbm_rank_refresh_publish_t cbm_rank_refresh_publish_from_pipeline(
    cbm_pipeline_publish_kind_t publish_kind, bool incremental_fallback);

/* Config keys for edge type weights (all doubles, override via `config set`) */
#define CBM_CONFIG_EDGE_WEIGHT_CALLS "edge_weight_calls"
#define CBM_CONFIG_EDGE_WEIGHT_DEFINES_METHOD "edge_weight_defines_method"
#define CBM_CONFIG_EDGE_WEIGHT_DEFINES "edge_weight_defines"
#define CBM_CONFIG_EDGE_WEIGHT_IMPORTS "edge_weight_imports"
#define CBM_CONFIG_EDGE_WEIGHT_USAGE "edge_weight_usage"
#define CBM_CONFIG_EDGE_WEIGHT_CONFIGURES "edge_weight_configures"
#define CBM_CONFIG_EDGE_WEIGHT_HTTP_CALLS "edge_weight_http_calls"
#define CBM_CONFIG_EDGE_WEIGHT_ASYNC_CALLS "edge_weight_async_calls"
#define CBM_CONFIG_EDGE_WEIGHT_TESTS "edge_weight_tests"
#define CBM_CONFIG_EDGE_WEIGHT_WRITES "edge_weight_writes"
#define CBM_CONFIG_EDGE_WEIGHT_DECORATES "edge_weight_decorates"
#define CBM_CONFIG_EDGE_WEIGHT_DEFAULT "edge_weight_default"
#define CBM_CONFIG_EDGE_WEIGHT_MEMBER_OF "edge_weight_member_of"

/* One owner for runtime defaults and generated registry/help strings. The
 * accepted extent reaches the full finite double representation: PageRank
 * weights must be finite and nonnegative, while the narrower ranges are
 * advisory starting points rather than capability limits. Defaults were tuned
 * on the repository's code-search ranking fixture (see pagerank.c and
 * benchmarks/autotune.py); recommendations preserve each edge kind's intended
 * scale relative to CALLS=1.0 and require workload measurement before changes. */
#define CBM_PAGERANK_EDGE_WEIGHT_MIN 0.0
#define CBM_PAGERANK_EDGE_WEIGHT_MAX DBL_MAX

#define CBM_PAGERANK_WEIGHT_CALLS_DEFAULT 1.0
#define CBM_PAGERANK_WEIGHT_CALLS_DEFAULT_STR CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_CALLS_DEFAULT)
/* CALLS is the relative anchor; half-to-double keeps direct control flow dominant. */
#define CBM_PAGERANK_WEIGHT_CALLS_RECOMMENDED_MIN 0.5
#define CBM_PAGERANK_WEIGHT_CALLS_RECOMMENDED_MAX 2.0
#define CBM_PAGERANK_WEIGHT_CALLS_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_CALLS_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_CALLS_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_USAGE_DEFAULT 0.7
#define CBM_PAGERANK_WEIGHT_USAGE_DEFAULT_STR CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_USAGE_DEFAULT)
/* USAGE is dense in typed/dynamic OO code; the window permits deliberate damping. */
#define CBM_PAGERANK_WEIGHT_USAGE_RECOMMENDED_MIN 0.2
#define CBM_PAGERANK_WEIGHT_USAGE_RECOMMENDED_MAX 1.0
#define CBM_PAGERANK_WEIGHT_USAGE_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_USAGE_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_USAGE_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_DEFINES_METHOD_DEFAULT 0.5
#define CBM_PAGERANK_WEIGHT_DEFINES_METHOD_DEFAULT_STR \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DEFINES_METHOD_DEFAULT)
/* Structural fan-out must remain below CALLS so large classes do not win by size alone. */
#define CBM_PAGERANK_WEIGHT_DEFINES_METHOD_RECOMMENDED_MIN 0.1
#define CBM_PAGERANK_WEIGHT_DEFINES_METHOD_RECOMMENDED_MAX 0.5
#define CBM_PAGERANK_WEIGHT_DEFINES_METHOD_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DEFINES_METHOD_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DEFINES_METHOD_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_IMPORTS_DEFAULT 0.3
#define CBM_PAGERANK_WEIGHT_IMPORTS_DEFAULT_STR CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_IMPORTS_DEFAULT)
/* IMPORTS promotes shared modules but stays below direct calls to limit utility noise. */
#define CBM_PAGERANK_WEIGHT_IMPORTS_RECOMMENDED_MIN 0.3
#define CBM_PAGERANK_WEIGHT_IMPORTS_RECOMMENDED_MAX 0.8
#define CBM_PAGERANK_WEIGHT_IMPORTS_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_IMPORTS_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_IMPORTS_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_DECORATES_DEFAULT 0.2
#define CBM_PAGERANK_WEIGHT_DECORATES_DEFAULT_STR \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DECORATES_DEFAULT)
/* DECORATES is sparse semantic signal; frameworks may justify raising it toward 0.5. */
#define CBM_PAGERANK_WEIGHT_DECORATES_RECOMMENDED_MIN 0.2
#define CBM_PAGERANK_WEIGHT_DECORATES_RECOMMENDED_MAX 0.5
#define CBM_PAGERANK_WEIGHT_DECORATES_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DECORATES_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DECORATES_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_WRITES_DEFAULT 0.15
#define CBM_PAGERANK_WEIGHT_WRITES_DEFAULT_STR CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_WRITES_DEFAULT)
/* WRITES is low by default; data pipelines may raise it when sinks define architecture. */
#define CBM_PAGERANK_WEIGHT_WRITES_RECOMMENDED_MIN 0.05
#define CBM_PAGERANK_WEIGHT_WRITES_RECOMMENDED_MAX 0.5
#define CBM_PAGERANK_WEIGHT_WRITES_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_WRITES_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_WRITES_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_DEFINES_DEFAULT 0.1
#define CBM_PAGERANK_WEIGHT_DEFINES_DEFAULT_STR CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DEFINES_DEFAULT)
/* Every symbol has a DEFINES edge, so a narrow low range limits structural inflation. */
#define CBM_PAGERANK_WEIGHT_DEFINES_RECOMMENDED_MIN 0.01
#define CBM_PAGERANK_WEIGHT_DEFINES_RECOMMENDED_MAX 0.1
#define CBM_PAGERANK_WEIGHT_DEFINES_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DEFINES_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_DEFINES_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_CONFIGURES_DEFAULT 0.1
#define CBM_PAGERANK_WEIGHT_CONFIGURES_DEFAULT_STR \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_CONFIGURES_DEFAULT)
/* CONFIGURES is sparse; infrastructure repositories may raise it without exceeding imports. */
#define CBM_PAGERANK_WEIGHT_CONFIGURES_RECOMMENDED_MIN 0.1
#define CBM_PAGERANK_WEIGHT_CONFIGURES_RECOMMENDED_MAX 0.3
#define CBM_PAGERANK_WEIGHT_CONFIGURES_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_CONFIGURES_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_CONFIGURES_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_TESTS_DEFAULT 0.05
#define CBM_PAGERANK_WEIGHT_TESTS_DEFAULT_STR CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_TESTS_DEFAULT)
/* TESTS is deliberately damped so test multiplicity does not dominate production calls. */
#define CBM_PAGERANK_WEIGHT_TESTS_RECOMMENDED_MIN 0.01
#define CBM_PAGERANK_WEIGHT_TESTS_RECOMMENDED_MAX 0.1
#define CBM_PAGERANK_WEIGHT_TESTS_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_TESTS_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_TESTS_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_HTTP_CALLS_DEFAULT 0.5
#define CBM_PAGERANK_WEIGHT_HTTP_CALLS_DEFAULT_STR \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_HTTP_CALLS_DEFAULT)
/* HTTP_CALLS may be the primary cross-service coupling, so its window reaches 2x CALLS. */
#define CBM_PAGERANK_WEIGHT_HTTP_CALLS_RECOMMENDED_MIN 0.5
#define CBM_PAGERANK_WEIGHT_HTTP_CALLS_RECOMMENDED_MAX 2.0
#define CBM_PAGERANK_WEIGHT_HTTP_CALLS_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_HTTP_CALLS_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_HTTP_CALLS_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_ASYNC_CALLS_DEFAULT 0.8
#define CBM_PAGERANK_WEIGHT_ASYNC_CALLS_DEFAULT_STR \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_ASYNC_CALLS_DEFAULT)
/* ASYNC_CALLS remains near CALLS but can be damped in event-dense codebases. */
#define CBM_PAGERANK_WEIGHT_ASYNC_CALLS_RECOMMENDED_MIN 0.3
#define CBM_PAGERANK_WEIGHT_ASYNC_CALLS_RECOMMENDED_MAX 1.0
#define CBM_PAGERANK_WEIGHT_ASYNC_CALLS_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_ASYNC_CALLS_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_ASYNC_CALLS_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_FALLBACK_DEFAULT 0.1
#define CBM_PAGERANK_WEIGHT_FALLBACK_DEFAULT_STR CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_FALLBACK_DEFAULT)
/* Unknown future edge kinds start low until their ranking semantics are measured. */
#define CBM_PAGERANK_WEIGHT_FALLBACK_RECOMMENDED_MIN 0.01
#define CBM_PAGERANK_WEIGHT_FALLBACK_RECOMMENDED_MAX 0.1
#define CBM_PAGERANK_WEIGHT_FALLBACK_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_FALLBACK_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_FALLBACK_RECOMMENDED_MAX)
#define CBM_PAGERANK_WEIGHT_MEMBER_OF_DEFAULT 0.5
#define CBM_PAGERANK_WEIGHT_MEMBER_OF_DEFAULT_STR \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_MEMBER_OF_DEFAULT)
/* MEMBER_OF propagates method importance to classes; zero disables that propagation. */
#define CBM_PAGERANK_WEIGHT_MEMBER_OF_RECOMMENDED_MIN 0.0
#define CBM_PAGERANK_WEIGHT_MEMBER_OF_RECOMMENDED_MAX 0.8
#define CBM_PAGERANK_WEIGHT_MEMBER_OF_RECOMMENDED_RANGE          \
    CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_MEMBER_OF_RECOMMENDED_MIN) \
    "-" CBM_STRINGIFY(CBM_PAGERANK_WEIGHT_MEMBER_OF_RECOMMENDED_MAX)

/* Canonical edge-type/default-token/config-token/struct-field mapping.
 * Frequency order retains the lookup fast path. Consumers expand this list
 * for lookup, initialization, config loading, validation, or contract tests so
 * adding an edge kind cannot silently update only one surface. "DEFAULT" maps
 * the fallback field and is harmless as an explicit edge type. Expansion is
 * compile-time only and adds no runtime or memory cost. */
#define CBM_PAGERANK_EDGE_WEIGHT_FIELDS(X)                              \
    X("CALLS", CALLS, CALLS, calls)                                     \
    X("DEFINES", DEFINES, DEFINES, defines)                             \
    X("TESTS", TESTS, TESTS, tests)                                     \
    X("USAGE", USAGE, USAGE, usage)                                     \
    X("DEFINES_METHOD", DEFINES_METHOD, DEFINES_METHOD, defines_method) \
    X("WRITES", WRITES, WRITES, writes)                                 \
    X("CONFIGURES", CONFIGURES, CONFIGURES, configures)                 \
    X("IMPORTS", IMPORTS, IMPORTS, imports)                             \
    X("DECORATES", DECORATES, DECORATES, decorates)                     \
    X("MEMBER_OF", MEMBER_OF, MEMBER_OF, member_rank_factor)            \
    X("HTTP_CALLS", HTTP_CALLS, HTTP_CALLS, http_calls)                 \
    X("ASYNC_CALLS", ASYNC_CALLS, ASYNC_CALLS, async_calls)             \
    X("DEFAULT", FALLBACK, DEFAULT, default_weight)

/* ── Internal tuning constants ────────────────────────────── */

#define CBM_PAGERANK_INITIAL_CAP 256 /* Initial array capacity for nodes/edges */
#define CBM_ISO_TIMESTAMP_LEN 32     /* ISO-8601 timestamp buffer size */
#define CBM_LOG_INT_BUF 16           /* int->string buffer for logging */
#define CBM_HASHMAP_LOAD_FACTOR 2    /* Hash map capacity = N * factor + 1 */

/* ── Scope control ────────────────────────────────────────── */

typedef enum {
    CBM_RANK_SCOPE_PROJECT = 0, /* project nodes only */
    CBM_RANK_SCOPE_FULL = 1,    /* project + all deps (default) */
    CBM_RANK_SCOPE_DEPS = 2,    /* deps only */
} cbm_rank_scope_t;

#define CBM_DEFAULT_RANK_SCOPE CBM_RANK_SCOPE_FULL

/* ── Edge type weights ────────────────────────────────────── */

typedef struct {
    double calls;          /* CALLS — direct function/method calls */
    double defines_method; /* DEFINES_METHOD — class defines method (structural) */
    double defines;        /* DEFINES — module/file defines symbol (structural, low signal) */
    double imports;        /* IMPORTS — module imports */
    double usage;      /* USAGE — type references, attribute access, isinstance (high for Python) */
    double configures; /* CONFIGURES — config file links */
    double http_calls; /* HTTP_CALLS — cross-service calls */
    double async_calls;        /* ASYNC_CALLS — async function calls */
    double tests;              /* TESTS — test function tests production code (dampened) */
    double writes;             /* WRITES — function writes to variable/file */
    double decorates;          /* DECORATES — decorator applied to function */
    double default_weight;     /* Fallback for unknown edge types */
    double member_rank_factor; /* Fraction of member rank aggregated to parent class (0=disabled) */
} cbm_edge_weights_t;

extern const cbm_edge_weights_t CBM_DEFAULT_EDGE_WEIGHTS;

/* ── PageRank API ─────────────────────────────────────────── */

/* Compute PageRank + LinkRank for all nodes/edges in a project scope.
 * Stores results in pagerank and linkrank tables.
 * Called after index_repository dump/flush.
 *
 * Runtime:  O(max_iter * (V + E)); the default budget is
 * CBM_PAGERANK_MAX_ITER iterations and successful publication requires
 * convergence before that budget is exhausted.
 * Memory:   O(V) for rank arrays + O(E) for edge list.
 * Returns:  number of nodes ranked, or -1 on error. */
int cbm_pagerank_compute(cbm_store_t *store, const char *project, double damping, double epsilon,
                         int max_iter, const cbm_edge_weights_t *weights, cbm_rank_scope_t scope);

/* Convenience: compute with CBM_DEFAULT_RANK_SCOPE, CBM_PAGERANK_DAMPING,
 * CBM_PAGERANK_EPSILON, and CBM_PAGERANK_MAX_ITER. */
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
                                       int deps_reindexed, cbm_rank_refresh_publish_t publish_kind);

/* Backwards-compatible wrapper for older callers: exact_incremental_publish=true
 * maps to CBM_RANK_REFRESH_PUBLISH_INCREMENTAL_EXACT, false maps to FULL. */
int cbm_pagerank_refresh_if_needed(cbm_store_t *store, const char *project, struct cbm_config *cfg,
                                   bool graph_changed, int deps_reindexed,
                                   bool exact_incremental_publish);

/* True only when PageRank, LinkRank, and node_degree derived views are all
 * recorded complete for the project. Missing rows return false so callers
 * repair older DBs instead of skipping necessary work. */
bool cbm_pagerank_views_complete(cbm_store_t *store, const char *project);

/* Get PageRank score for a single node. Returns 0.0 if not computed. */
double cbm_pagerank_get(cbm_store_t *store, int64_t node_id);

/* ── LinkRank API ─────────────────────────────────────────── */

/* Get LinkRank score for a single edge. Returns 0.0 if not computed. */
double cbm_linkrank_get(cbm_store_t *store, int64_t edge_id);

#ifdef CBM_ENABLE_TEST_SEAMS
typedef enum {
    CBM_PAGERANK_TEST_SCAN_NODES = 0,
    CBM_PAGERANK_TEST_SCAN_EDGES = 1,
} cbm_pagerank_test_scan_t;

/* Inject one SQLite scan failure after successful_rows rows. The failpoint is
 * thread-local and consumed when its target scan fails or reaches a terminal
 * result, so parallel tests and later target scans do not inherit it. */
void cbm_pagerank_test_fail_scan_after(cbm_pagerank_test_scan_t scan, int successful_rows);
bool cbm_pagerank_test_id_map_capacity(int node_count, int *capacity);
#endif

#endif /* CBM_PAGERANK_H */
