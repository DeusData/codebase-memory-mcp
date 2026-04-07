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
#include <stdint.h>

/* Forward declarations */
typedef struct cbm_store cbm_store_t;
typedef struct cbm_gbuf cbm_gbuf_t;

/* ── Opaque handle ──────────────────────────────────────────────── */

typedef struct cbm_pipeline cbm_pipeline_t;

/* ── Index mode ─────────────────────────────────────────────────── */

#ifndef CBM_INDEX_MODE_T_DEFINED
#define CBM_INDEX_MODE_T_DEFINED
typedef enum {
    CBM_MODE_FULL = 0,     /* Full: everything including SIMILAR_TO + SEMANTICALLY_RELATED */
    CBM_MODE_MODERATE = 1, /* Moderate: fast discovery + SIMILAR_TO + SEMANTICALLY_RELATED */
    CBM_MODE_FAST = 2,     /* Fast: skip non-essential files, no similarity/semantic edges */
} cbm_index_mode_t;
#endif

/* ── Pipeline lifecycle ─────────────────────────────────────────── */

/* Create a new pipeline. Caller owns the result. */
cbm_pipeline_t *cbm_pipeline_new(const char *repo_path, const char *db_path, cbm_index_mode_t mode);

/* Free a pipeline and all its internal state. NULL-safe. */
void cbm_pipeline_free(cbm_pipeline_t *p);

/* Run the full indexing pipeline. Returns 0 on success, -1 on error.
 * Discovers files, extracts, resolves, and dumps to SQLite. */
int cbm_pipeline_run(cbm_pipeline_t *p);

/* Request cancellation of a running pipeline (thread-safe). */
void cbm_pipeline_cancel(cbm_pipeline_t *p);

/* Get the project name derived from repo_path. Returned string is
 * owned by the pipeline. Valid until cbm_pipeline_free(). */
const char *cbm_pipeline_project_name(const cbm_pipeline_t *p);

/* Get the index mode (CBM_MODE_FULL, CBM_MODE_MODERATE, CBM_MODE_FAST). */
int cbm_pipeline_get_mode(const cbm_pipeline_t *p);

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

/* Check if a qualified name exists in the registry. */
bool cbm_registry_exists(const cbm_registry_t *r, const char *qn);

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
