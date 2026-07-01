/*
 * pipeline.h — Indexing pipeline orchestrator.
 *
 * Orchestrates multi-pass indexing of a repository:
 *   1. Structure: Project/Folder/Package/File nodes
 *   2. Definitions: Extract + write nodes + build registry
 *   3. Imports: Resolve import edges
 *   4. Calls: Call resolution (registry + LSP)
 *   5. Usages: Usage/type_ref edges
 *   6. Semantic: Inherits/decorates/implements
 *   7. Post: Tests, communities, HTTP links, config, git history
 *
 * Depends on: foundation, extraction, lsp, store, graph_buffer, discover
 */
#ifndef CBM_PIPELINE_H
#define CBM_PIPELINE_H

#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>

/* Forward declarations */
typedef struct cbm_store cbm_store_t;
typedef struct cbm_gbuf cbm_gbuf_t;
typedef struct cbm_config cbm_config_t;

/* ── Opaque handle ──────────────────────────────────────────────── */

typedef struct cbm_pipeline cbm_pipeline_t;

/* ── Index mode ─────────────────────────────────────────────────── */

#ifndef CBM_INDEX_MODE_T_DEFINED
#define CBM_INDEX_MODE_T_DEFINED
typedef enum {
    /* All modes run the LSP type-aware call/usage resolution (per-file +
     * cross-file). The mode only controls file discovery breadth and whether
     * SIMILAR_TO / SEMANTICALLY_RELATED edges are computed. */
    CBM_MODE_FULL = 0,     /* Full: everything including SIMILAR_TO + SEMANTICALLY_RELATED */
    CBM_MODE_MODERATE = 1, /* Moderate: fast discovery + SIMILAR_TO + SEMANTICALLY_RELATED */
    CBM_MODE_FAST = 2,     /* Fast: skip non-essential files, no similarity/semantic edges */
    CBM_MODE_DEP  = 3,     /* Dep: like FAST but keeps vendor/, .d.ts, third_party/ (fork depindex) */
} cbm_index_mode_t;
#endif

typedef enum {
    CBM_PIPELINE_PUBLISH_NONE = 0,
    CBM_PIPELINE_PUBLISH_FULL,
    CBM_PIPELINE_PUBLISH_INCREMENTAL_NOOP,
    CBM_PIPELINE_PUBLISH_INCREMENTAL_EXACT,
    CBM_PIPELINE_PUBLISH_INCREMENTAL_CONTAINMENT,
} cbm_pipeline_publish_kind_t;

/* ── Pipeline lifecycle ─────────────────────────────────────────── */

/* Create a new pipeline. Caller owns the result. */
cbm_pipeline_t *cbm_pipeline_new(const char *repo_path, const char *db_path, cbm_index_mode_t mode);

/* Enable persistent artifact export (.codebase-memory/graph.db.zst).
 * When enabled, the pipeline writes a compressed artifact after indexing. */
void cbm_pipeline_set_persistence(cbm_pipeline_t *p, bool enabled);

/* Free a pipeline and all its internal state. NULL-safe. */
void cbm_pipeline_free(cbm_pipeline_t *p);

/* Release all process-lifetime global state held by the pipeline subsystem
 * (e.g., lazily-compiled regex patterns used by pass_envscan).
 * Call once at server shutdown, after all pipelines have been freed and all
 * background indexing threads have been joined.  Safe to call multiple times. */
void cbm_pipeline_global_cleanup(void);

/* Run the full indexing pipeline. Returns 0 on success, -1 on error.
 * Discovers files, extracts, resolves, and dumps to SQLite. */
int cbm_pipeline_run(cbm_pipeline_t *p);

/* Request cancellation of a running pipeline (thread-safe). */
void cbm_pipeline_cancel(cbm_pipeline_t *p);

/* Override the auto-derived project name (e.g., for myapp.dep.pandas).
 * Must be called before cbm_pipeline_run(). Copies the string. */
void cbm_pipeline_set_project_name(cbm_pipeline_t *p, const char *name);

/* Set a store to flush into instead of dumping to a new SQLite file.
 * When set, pipeline uses cbm_gbuf_flush_to_store() which upserts by project name.
 * Must be called before cbm_pipeline_run(). Pipeline does NOT own the store. */
void cbm_pipeline_set_flush_store(cbm_pipeline_t *p, cbm_store_t *store);

/* Config keys consumed by cbm_pipeline_apply_config(). A value <=0 leaves the
 * corresponding pass on its compiled-in default. */
#define CBM_CONFIG_SIMILARITY_THRESHOLD "similarity_threshold"
#define CBM_CONFIG_HTTPLINK_MIN_CONFIDENCE "httplink_min_confidence"
#define CBM_CONFIG_SEMANTIC_THRESHOLD "semantic_threshold"
#define CBM_CONFIG_GITHISTORY_MIN_COUPLING "githistory_min_coupling"
#define CBM_CONFIG_LSP_CONFIDENCE_FLOOR "lsp_confidence_floor"
#define CBM_CONFIG_INCREMENTAL_REINDEX "incremental_reindex"

/* Set the Jaccard similarity threshold for SIMILAR-edge creation (pass_similarity).
 * <=0 (or unset) uses the CBM_MINHASH_JACCARD_THRESHOLD default. Before run(). */
void cbm_pipeline_set_similarity_threshold(cbm_pipeline_t *p, double threshold);
void cbm_pipeline_set_httplink_min_confidence(cbm_pipeline_t *p, double threshold);
void cbm_pipeline_set_semantic_threshold(cbm_pipeline_t *p, double threshold);
void cbm_pipeline_set_githistory_min_coupling(cbm_pipeline_t *p, double threshold);
void cbm_pipeline_set_lsp_confidence_floor(cbm_pipeline_t *p, double threshold);
/* Apply config-backed thresholds. NULL cfg is allowed and leaves defaults. */
void cbm_pipeline_apply_config(cbm_pipeline_t *p, cbm_config_t *cfg);
double cbm_pipeline_similarity_threshold(const cbm_pipeline_t *p);
double cbm_pipeline_httplink_min_confidence(const cbm_pipeline_t *p);
double cbm_pipeline_semantic_threshold(const cbm_pipeline_t *p);
double cbm_pipeline_githistory_min_coupling(const cbm_pipeline_t *p);
double cbm_pipeline_lsp_confidence_floor(const cbm_pipeline_t *p);

/* Get the project name derived from repo_path. Returned string is
 * owned by the pipeline. Valid until cbm_pipeline_free(). */
const char *cbm_pipeline_project_name(const cbm_pipeline_t *p);

/* Get the repo path. Returned string is owned by the pipeline. */
const char *cbm_pipeline_repo_path(const cbm_pipeline_t *p);

/* Get a pointer to the pipeline's cancellation flag. Used by incremental
 * pipeline to propagate cancellation into the sub-pipeline context. */
atomic_int *cbm_pipeline_cancelled_ptr(cbm_pipeline_t *p);

/* Get the index mode (CBM_MODE_FULL, CBM_MODE_MODERATE, CBM_MODE_FAST, CBM_MODE_DEP). */
int cbm_pipeline_get_mode(const cbm_pipeline_t *p);

/* Get the list of directory subtrees skipped during discovery (#411).
 * *out receives a borrowed array of rel-path strings (owned by the pipeline,
 * valid until cbm_pipeline_free()); *count receives its length. Both are set
 * to NULL/0 when p is NULL or nothing was excluded. Do not free. */
void cbm_pipeline_get_excluded(const cbm_pipeline_t *p, char ***out, int *count);

/* Committed node/edge counts captured at dump time (-1 when dump did not run).
 * Nodes are the #334 plausibility-gate axis; edges are informational only. */
void cbm_pipeline_get_committed_counts(const cbm_pipeline_t *p, int *nodes, int *edges);

/* True when the last successful run changed persisted graph contents.
 * Incremental no-op runs return false so callers can skip derived-view
 * recomputation when the existing derived views are already complete. */
bool cbm_pipeline_graph_changed(const cbm_pipeline_t *p);

/* Last publish route for the most recent run. This is observability for callers
 * that need derived-view policy decisions; it does not change graph contents. */
cbm_pipeline_publish_kind_t cbm_pipeline_publish_kind(const cbm_pipeline_t *p);

/* ── Index lock (prevents concurrent pipeline runs on same DB) ──── */

/* Try to acquire the global index lock. Returns true if acquired,
 * false if another pipeline is already running (non-blocking).
 * Use this in the watcher — skip reindex if busy. */
bool cbm_pipeline_try_lock(void);

/* Acquire the global index lock, blocking until available.
 * Use this in MCP handler and autoindex — wait for busy watcher to finish. */
void cbm_pipeline_lock(void);

/* Release the global index lock. */
void cbm_pipeline_unlock(void);

/* ── FQN helpers (used by passes and external callers) ──────────── */

/* Compute a qualified name: project.dir.parts.name
 * Strips extension, converts / to ., drops __init__ and index.
 * Caller must free() the returned string. */
char *cbm_pipeline_fqn_compute(const char *project, const char *rel_path, const char *name);

/* Module QN: project.dir.parts (no name). Caller must free(). */
char *cbm_pipeline_fqn_module(const char *project, const char *rel_path);

/* Folder QN: project.dir.parts. Caller must free(). */
char *cbm_pipeline_fqn_folder(const char *project, const char *rel_dir);

/* Resolve an import specifier that uses a relative path (./foo, ../bar, .foo,
 * or an unqualified local name like "foo.h") against the importing file's
 * path.  Returns a malloc'd normalized relative path without extension
 * (e.g. "src/api/helpers") suitable for passing to cbm_pipeline_fqn_module,
 * or NULL if the specifier is not a relative path (bare module names like
 * "lodash", "django", "github.com/foo/bar" return NULL — the caller should
 * treat those as external/unresolvable). Handles ".", "..", and leading
 * dot-only segments used by Python relative imports. */
char *cbm_pipeline_resolve_relative_import(const char *source_rel, const char *module_path);

/* Derive project name from an absolute path.
 * Replaces / and : with -, collapses --, trims leading -.
 * Caller must free() the returned string. */
char *cbm_project_name_from_path(const char *abs_path);

/* ── Function Registry ──────────────────────────────────────────── */

typedef struct cbm_registry cbm_registry_t;

typedef struct {
    const char *qualified_name; /* borrowed from registry */
    const char *strategy;       /* resolution strategy name */
    double confidence;          /* 0.0–1.0 */
    int candidate_count;
} cbm_resolution_t;

/* Create/free a function registry. */
cbm_registry_t *cbm_registry_new(void);
void cbm_registry_free(cbm_registry_t *r);

/* Register a function/method/class. All strings are copied. */
void cbm_registry_add(cbm_registry_t *r, const char *name, const char *qualified_name,
                      const char *label);

/* Resolve a callee name using prioritized strategies.
 * import_map: NULL-terminated array of {local_name, resolved_qn} pairs, or NULL.
 * Returns result with qualified_name="" if unresolved. */
cbm_resolution_t cbm_registry_resolve(const cbm_registry_t *r, const char *callee_name,
                                      const char *module_qn, const char **import_map_keys,
                                      const char **import_map_vals, int import_map_count);

/* Per-file memoization cache for is_import_reachable. Thread-local —
 * each resolve worker owns its own cache. Call _begin at the start
 * of resolve_file_calls (or any per-file resolve loop) and _end at
 * the end. The cache MUST be invalidated between files because
 * is_import_reachable's truth depends on the file's import_vals. */
void cbm_registry_reach_cache_begin(int estimated_capacity);
void cbm_registry_reach_cache_end(void);

/* Per-file import-map prefix → module-QN hash. Turns the linear
 * strcmp scan inside resolve_import_map into O(1). Keys/values are
 * BORROWED — caller must keep the import_map arrays alive for the
 * cache lifetime. Invalidate between files via _end. */
void cbm_registry_import_map_cache_begin(const char **keys, const char **vals, int count);
void cbm_registry_import_map_cache_end(void);

/* Per-file full-result cache for cbm_registry_resolve. The same
 * callee_name appears in many call sites within a file; module_qn
 * is constant per file so each name resolves identically. First
 * lookup does the full strategy chain; repeats are O(1) hash hits.
 * This eliminates ~75% of the resolve-chain work on K8s where the
 * same names ("Get", "Add", "New", etc) appear hundreds of times. */
void cbm_registry_resolve_cache_begin(int estimated_capacity);
void cbm_registry_resolve_cache_end(void);

/* Check if a qualified name exists in the registry. */
bool cbm_registry_exists(const cbm_registry_t *r, const char *qn);

/* True if `name` is one of the curated Perl core builtins (perlfunc). Used by
 * the call-resolution passes to suppress generic-resolver CALLS edges from Perl
 * builtin invocations (push/shift/keys/...) to project subs that merely share
 * the name. Perl-scoped: callers gate on the file language. */
bool cbm_perl_is_builtin(const char *name);

/* Decide whether a resolved Perl call edge is generic-resolver noise to drop
 * (#476): true only for Perl, only for a builtin/method call, and only when the
 * match used a weak short-name strategy — high-confidence same_module/import_map
 * matches are kept. Pure; unit-tested in test_registry.c. */
bool cbm_perl_suppress_generic_match(bool is_perl, bool is_method, const char *callee_name,
                                     const char *strategy);

/* Get the label of a qualified name, or NULL if not found. */
const char *cbm_registry_label_of(const cbm_registry_t *r, const char *qn);

/* Find all QNs with a given simple name. Sets *out and *count.
 * Caller does NOT free the array (owned by registry). */
int cbm_registry_find_by_name(const cbm_registry_t *r, const char *name, const char ***out,
                              int *count);

/* Return total number of entries. */
int cbm_registry_size(const cbm_registry_t *r);

/* Find all qualified names ending with ".suffix".
 * Sets *out to heap-allocated array of borrowed string pointers.
 * Caller must free(*out) but NOT the individual strings.
 * Returns count of matches. */
int cbm_registry_find_ending_with(const cbm_registry_t *r, const char *suffix, const char ***out);

/* Check if candidate QN's module prefix is reachable via any import value. */
bool cbm_registry_is_import_reachable(const char *candidate_qn, const char **import_vals,
                                      int import_count);

/* Fuzzy resolve: match callee by bare function name (last segment after dots).
 * Returns result with ok=true if found, ok=false if not.
 * Lower confidence than Resolve (0.40 single, 0.30 multiple). */
typedef struct {
    cbm_resolution_t result;
    bool ok;
} cbm_fuzzy_result_t;

cbm_fuzzy_result_t cbm_registry_fuzzy_resolve(const cbm_registry_t *r, const char *callee_name,
                                              const char *module_qn, const char **import_map_keys,
                                              const char **import_map_vals, int import_map_count);

const char *cbm_confidence_band(double score);

#endif /* CBM_PIPELINE_H */
