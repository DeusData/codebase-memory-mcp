#include "foundation/constants.h"
/*
 * pipeline_internal.h — Internal pipeline state shared between pass files.
 *
 * NOT a public header. Only included by pipeline.c and pass_*.c files.
 * Exposes the pipeline context struct for direct field access by passes.
 */
#ifndef CBM_PIPELINE_INTERNAL_H
#define CBM_PIPELINE_INTERNAL_H

#include "pipeline/pipeline.h"
#include "pipeline/path_alias.h"
#include "graph_buffer/graph_buffer.h"
#include "store/store.h"
#include "discover/discover.h"
#include "foundation/hash_table.h"
#include "cbm.h"
#include "service_patterns.h"
#include "lsp/go_lsp.h" /* CBMLSPDef for cbm_parallel_resolve cross-LSP inputs */
#include <stdatomic.h>
#include <string.h>
#include <sys/stat.h>

/* ── Shared pipeline constants ─────────────────────────────────── */

/* Maximum byte budget for tree-sitter extraction per file */
#define CBM_EXTRACT_BUDGET 5000000

/* Route node QN buffer size (must fit __route__METHOD__/full/url/path) */
#define CBM_ROUTE_QN_SIZE 768
#define CBM_ROUTE_DEFAULT_METHOD "ANY"
#define CBM_ROUTE_DEFAULT_ASYNC_BROKER "async"

/* Canonicalize route-path parameter placeholders (":id", "{id}", "<id>",
 * "${...}") to a single "{}" token so that client call sites and server
 * handlers rendezvous on the same Route QN regardless of framework syntax.
 * Parameter names are intentionally discarded ("/u/{id}" and "/u/{slug}" both
 * canonicalize to "/u/{}"). The result never exceeds the input length, so
 * out_sz >= strlen(in) + 1 always suffices. Returns out. */
const char *cbm_route_canon_path(const char *in, char *out, size_t out_sz);

/* Build the deterministic Route qualified_name and JSON properties for
 * HTTP/async service edges. This keeps sequential, parallel, and post-merge
 * Route-node paths on the same canonicalization and properties schema. */
bool cbm_pipeline_build_service_route_identity(const char *path, cbm_svc_kind_t svc,
                                               const char *method, const char *broker,
                                               const char *source, char *route_qn,
                                               size_t route_qn_sz, char *route_props,
                                               size_t route_props_sz);

int64_t cbm_pipeline_upsert_service_route(cbm_gbuf_t *gb, const char *path, cbm_svc_kind_t svc,
                                          const char *method, const char *broker,
                                          const char *source, const char *file_path);

static inline bool cbm_pipeline_label_is_registry_symbol(const char *label) {
    return label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0 ||
                     cbm_label_is_type_like(label) || strcmp(label, "Variable") == 0 ||
                     strcmp(label, "Field") == 0);
}

static inline bool cbm_pipeline_label_is_import_target(const char *label) {
    return label && (cbm_label_is_type_like(label) || strcmp(label, "Function") == 0 ||
                     strcmp(label, "Method") == 0 || strcmp(label, "Module") == 0 ||
                     strcmp(label, "File") == 0);
}

/* Time unit conversions */
#define CBM_NS_PER_SEC 1000000000LL
#define CBM_US_PER_SEC 1000000LL
#define CBM_MS_PER_SEC 1000.0
#define CBM_US_PER_SEC_F 1e6

/* Generation used by full/containment indexing paths before exact-delta
 * generation reservation is active. Matches the store schema default. */
enum { CBM_PIPELINE_COMPAT_GENERATION = 0 };

/* Test-only incremental fault injection. Values name internal phases and are
 * intentionally not user configuration. */
#define CBM_TEST_FAIL_INCREMENTAL_PHASE "CBM_TEST_FAIL_INCREMENTAL_PHASE"
#define CBM_TEST_FAIL_INCREMENTAL_EXTRACT "incr_extract"
#define CBM_TEST_FAIL_INCREMENTAL_REGISTRY "incr_registry"
#define CBM_TEST_FAIL_INCREMENTAL_RESOLVE "incr_resolve"
#define CBM_TEST_FAIL_INCREMENTAL_POSTPASS "postpass"
#define CBM_TEST_FAIL_INCREMENTAL_HASH_PERSIST "hash_persist"

/* ── Pipeline context (internal) ─────────────────────────────────── */

/* Per-worker manifest collection entry. */
typedef struct {
    char *pkg_name;  /* heap: "@myorg/pkg", "github.com/foo/bar" */
    char *entry_rel; /* heap: "packages/pkg/src/index" (no extension) */
} cbm_pkg_entry_t;

/* Growable array of package entries (per-worker, no thread contention). */
typedef struct {
    cbm_pkg_entry_t *items;
    int count;
    int cap;
} cbm_pkg_entries_t;

void cbm_pkg_entries_init(cbm_pkg_entries_t *e);
void cbm_pkg_entries_free(cbm_pkg_entries_t *e);

/* Shared context passed to each pass function.
 * Derived from cbm_pipeline_t fields during run. */
typedef struct {
    const char *project_name; /* borrowed from pipeline */
    const char *repo_path;    /* borrowed from pipeline */
    cbm_gbuf_t *gbuf;         /* owned by pipeline */
    cbm_registry_t *registry; /* owned by pipeline */
    atomic_int *cancelled;    /* pointer to pipeline's cancelled flag */
    int mode;                 /* cbm_index_mode_t (0=full, 1=moderate, 2=fast, 3=advanced) */
    double similarity_threshold; /* Jaccard threshold for SIMILAR edges; <=0 means
                                  * use the CBM_MINHASH_JACCARD_THRESHOLD default (#41). */
    double httplink_min_confidence;    /* <=0 uses httplink pass default 0.25 */
    double semantic_threshold;         /* <=0 uses semantic default 0.75 */
    double githistory_min_coupling;    /* <=0 uses git-history default 0.3 */
    double lsp_confidence_floor;       /* <=0 uses LSP default 0.6 */

    /* Extraction result cache (sequential pipeline optimization).
     * When non-NULL, pass_definitions stores results here instead of freeing,
     * and pass_calls/usages/semantic reuse cached results instead of re-extracting.
     * Indexed by file position in the files[] array. Owned by pipeline.c. */
    CBMFileResult **result_cache;

    /* Build-tool path aliases (tsconfig/jsconfig today; webpack/vite-style
     * configs are an easy follow-on). NULL when no usable configs were found.
     * Owned by pipeline.c / pipeline_incremental.c. */
    const cbm_path_alias_collection_t *path_aliases;

    /* Exact-delta scratch optimization: when set on the single-threaded exact
     * upsert route, resolvers may materialize referenced unchanged nodes from
     * the store on demand instead of preloading every stored symbol node. The
     * changed-path list prevents stale stored nodes for files being reparsed
     * from re-entering the scratch graph. Leave NULL/0 on full, containment,
     * dependency, and parallel worker paths. */
    cbm_store_t *store_backed_node_lookup;
    const char *const *store_backed_changed_paths;
    int store_backed_changed_path_count;
} cbm_pipeline_ctx_t;

typedef struct {
    cbm_store_file_delta_t delta;
    cbm_file_hash_t file_hash;
    cbm_file_state_t file_state;
    char file_content_hash[CBM_SZ_32];
    char file_indexed_at[CBM_SZ_32];
    cbm_node_t *context_nodes;
    cbm_store_delta_edge_t *context_edges;
    cbm_node_t *nodes;
    cbm_store_delta_edge_t *edges;
    cbm_store_symbol_export_t *exports;
    cbm_store_import_ref_t *imports;
    int unsupported_edge_count;
    enum {
        CBM_PIPELINE_DELTA_CHANGE_UPSERT = 0,
        CBM_PIPELINE_DELTA_CHANGE_DELETE = 1,
        CBM_PIPELINE_DELTA_CHANGE_RENAME = 2,
    } change_kind;
    const char *old_rel_path; /* borrowed; set for rename preflight only */
} cbm_pipeline_file_delta_t;

typedef enum {
    CBM_PIPELINE_DELTA_ROUTE_FALLBACK = 0,
    CBM_PIPELINE_DELTA_ROUTE_EXACT_CANDIDATE = 1,
} cbm_pipeline_delta_route_t;

typedef struct {
    cbm_pipeline_delta_route_t route;
    const char *reason;
    char **affected_paths;
    int affected_count;
} cbm_pipeline_file_delta_plan_t;

/* Conservative live exact-delta frontier cap. Larger affected sets fall back
 * to the existing containment reindex path until broader parity benchmarks pass. */
enum { CBM_PIPELINE_EXACT_DELTA_MAX_AFFECTED_PATHS = CBM_SZ_4 };
/* Live exact upserts are currently limited to two changed files. Larger
 * batches keep the existing fallback path until same-batch parity coverage
 * includes deletes, renames, new folders, and derived-view freshness. */
enum { CBM_PIPELINE_EXACT_DELTA_MAX_CHANGED_PATHS = CBM_SZ_2 };

/* Get the current pipeline's package map (NULL if none). */
CBMHashTable *cbm_pipeline_get_pkgmap(void);
void cbm_pipeline_set_pkgmap(CBMHashTable *map);

/* Unified module resolver: relative → pkgmap → fqn_module fallback.
 * Handles bare specifiers via pkgmap lookup with prefix matching.
 * Caller must free() the returned string. */
char *cbm_pipeline_resolve_module(const cbm_pipeline_ctx_t *ctx, const char *source_rel,
                                  const char *module_path);

/* Resolve an import to its in-graph target node, or NULL if unresolvable.
 *
 * Resolution order (first hit wins):
 *   1. Module-path resolution (relative / pkgmap / fqn_module) → existing node.
 *      This preserves the behavior for Python/TS/Go whose module path maps
 *      directly to a sibling Module/File QN.
 *   2. namespace_map[module_path-prefix] → File node QN (Java/Kotlin/C#/PHP
 *      `using`/`import` of a NAMESPACE that the path-based QN cannot express).
 *   3. Symbol-name fallback: the import's last path segment matched against an
 *      in-graph definition node of the same simple name in a different file
 *      (Rust `use crate::util::helper`, Java `import com.example.Util`, ...).
 *
 * `namespace_map` may be NULL (skips step 2).  `source_file_qn` is the importing
 * file's __file__ QN, used to avoid self-imports in step 3. */
const cbm_gbuf_node_t *cbm_pipeline_resolve_import_node(cbm_pipeline_ctx_t *ctx,
                                                        const char *source_rel,
                                                        const char *source_file_qn,
                                                        const CBMImport *imp,
                                                        CBMHashTable *namespace_map);

/* Insert an IMPORTS edge with canonical JSON properties.
 * Shared by sequential and parallel definition passes so escaping and
 * self-edge filtering cannot drift. Returns 1 when an edge is emitted. */
int cbm_pipeline_insert_import_edge(cbm_pipeline_ctx_t *ctx, int64_t source_id,
                                    const cbm_gbuf_node_t *target, const char *local_name);

/* Resolve and insert all IMPORTS edges for one file.
 * Shared by sequential and parallel definition passes so source-file lookup,
 * resolver order, JSON properties, and self-edge filtering cannot drift. */
int cbm_pipeline_create_import_edges_for_file(cbm_pipeline_ctx_t *ctx,
                                              const CBMFileResult *result,
                                              const char *rel_path,
                                              CBMHashTable *namespace_map);

/* Extract IMPORTS edge local_name from the canonical edge JSON. Caller frees. */
char *cbm_pipeline_import_edge_local_name_dup(const cbm_gbuf_edge_t *edge);

/* Build a per-file import map from already-resolved IMPORTS edges.
 * Returned keys are heap strings; values are borrowed graph-buffer QNs. */
int cbm_pipeline_build_import_map_from_edges(const cbm_gbuf_t *gbuf, const char *project_name,
                                             const char *rel_path, const char ***out_keys,
                                             const char ***out_vals, int *out_count);
void cbm_pipeline_free_import_map(const char **keys, const char **vals, int count);

/* Build a store-level per-file delta descriptor from graph-buffer facts.
 * Returns CBM_STORE_OK even when unsupported_edge_count > 0; callers must fall
 * back instead of publishing when unsupported edges are present. */
int64_t cbm_pipeline_stat_mtime_ns(const struct stat *st);
const char *cbm_pipeline_file_delta_pass_fingerprint(void);
int cbm_pipeline_format_file_delta_pass_fingerprint(char *out, size_t out_sz, int mode,
                                                    double similarity_threshold,
                                                    double httplink_min_confidence,
                                                    double semantic_threshold,
                                                    double githistory_min_coupling,
                                                    double lsp_confidence_floor);
int cbm_pipeline_current_pass_fingerprint(const cbm_pipeline_t *p, char *out, size_t out_sz);
int cbm_pipeline_content_hash_file(const char *path, char *out, size_t out_sz);
bool cbm_pipeline_file_state_is_current_or_legacy(cbm_store_t *store, const char *project,
                                                  const cbm_file_info_t *file,
                                                  const char *pass_fingerprint);
bool cbm_pipeline_file_state_content_matches_current(cbm_store_t *store, const char *project,
                                                     const cbm_file_info_t *file,
                                                     const char *pass_fingerprint);
/* Persists file_state rows in its own transaction. */
int cbm_pipeline_persist_file_states(cbm_store_t *store, const char *project,
                                     const cbm_file_info_t *files, int file_count,
                                     int64_t generation, const char *pass_fingerprint);
int cbm_pipeline_build_file_delta_from_gbuf(const cbm_gbuf_t *gbuf, const char *project,
                                            const char *rel_path, int64_t generation,
                                            cbm_pipeline_file_delta_t *out);
int cbm_pipeline_attach_file_delta_metadata_with_fingerprint(cbm_pipeline_file_delta_t *delta,
                                                             const cbm_file_info_t *file,
                                                             const char *pass_fingerprint);
int cbm_pipeline_attach_file_delta_metadata(cbm_pipeline_file_delta_t *delta,
                                            const cbm_file_info_t *file);
/* Stamp the reserved generation after exact-delta planning and before publish. */
int cbm_pipeline_file_delta_stamp_generation(cbm_pipeline_file_delta_t *delta,
                                             int64_t generation);
void cbm_pipeline_file_delta_free(cbm_pipeline_file_delta_t *delta);

/* Preflight an exact-delta publish candidate. This never writes the store. */
int cbm_pipeline_plan_file_delta(cbm_store_t *store, const cbm_pipeline_file_delta_t *delta,
                                 int max_affected_paths, cbm_pipeline_file_delta_plan_t *out);
int cbm_pipeline_plan_file_delta_batch(cbm_store_t *store,
                                       const cbm_pipeline_file_delta_t *const *deltas,
                                       int delta_count, int max_affected_paths,
                                       cbm_pipeline_file_delta_plan_t *out);
int cbm_pipeline_apply_file_delta_batch(cbm_store_t *store,
                                        const cbm_pipeline_file_delta_t *const *deltas,
                                        int delta_count, int max_affected_paths,
                                        cbm_pipeline_file_delta_plan_t *out);
void cbm_pipeline_file_delta_plan_free(cbm_pipeline_file_delta_plan_t *plan);

/* Seed a scratch graph with persisted unchanged nodes needed by import and
 * symbol resolution. Used to build exact-delta descriptors without loading the
 * full stored graph. `changed_paths` entries are borrowed and skipped. */
int cbm_pipeline_seed_file_delta_scratch_from_store(cbm_store_t *store, cbm_gbuf_t *gbuf,
                                                    cbm_registry_t *registry,
                                                    const char *project,
                                                    const char *const *changed_paths,
                                                    int changed_path_count);
const cbm_gbuf_node_t *cbm_pipeline_find_node_by_qn(cbm_pipeline_ctx_t *ctx, const char *qn);

/* Build a namespace → File-node-QN map from a set of extraction results.
 * Each result that declared a namespace/package contributes one entry keyed by
 * the namespace string (e.g. "App.Utils", "com.example").  Returns NULL when no
 * results declared a namespace.  Caller frees via cbm_pipeline_namespace_map_free. */
CBMHashTable *cbm_pipeline_namespace_map_build(const char *project_name,
                                               CBMFileResult *const *results,
                                               const char *const *rels, int count);
void cbm_pipeline_namespace_map_free(CBMHashTable *map);

/* Parse a manifest file and collect pkg entries. Returns true if basename matched. */
bool cbm_pkgmap_try_parse(const char *basename, const char *rel_path, const char *source,
                          int source_len, cbm_pkg_entries_t *entries);

/* Merge per-worker entries into a hash table. Returns NULL if no entries. */
CBMHashTable *cbm_pkgmap_build(cbm_pkg_entries_t *worker_entries, int worker_count,
                               const char *project_name);

/* Build pkgmap by reading manifest files from the files array (sequential path). */
int cbm_pkgmap_scan_repo(const char *repo_path, cbm_pkg_entries_t *entries);
CBMHashTable *cbm_pkgmap_build_from_repo(const char *repo_path, const cbm_file_info_t *files,
                                         int file_count, const char *project_name);
CBMHashTable *cbm_pkgmap_build_from_files(const cbm_file_info_t *files, int file_count,
                                          const char *project_name);

/* Free pkgmap and all owned strings. */
void cbm_pkgmap_free(CBMHashTable *pkgmap);

/* Check cancellation. Returns non-zero if cancelled. */
static inline int cbm_pipeline_check_cancel(const cbm_pipeline_ctx_t *ctx) {
    return atomic_load(ctx->cancelled) ? CBM_NOT_FOUND : 0;
}

/* ── Testable helpers ────────────────────────────────────────────── */

/* Check if a file path is worth tracking for git history analysis. */
bool cbm_is_trackable_file(const char *path);

/* Check if a file path looks like a test file (language-agnostic). */
bool cbm_is_test_path(const char *path);

/* Check if a function name looks like a test function (language-agnostic). */
bool cbm_is_test_func_name(const char *name);

/* Coupling result from computeChangeCoupling */
typedef struct {
    char file_a[CBM_SZ_512];
    char file_b[CBM_SZ_512];
    int co_change_count;
    double coupling_score;
    /* Unix epoch of the most recent commit that touched both files together.
     * 0 when no timestamp was available (e.g. older callers / popen path
     * without %ct). */
    long long last_co_change;
} cbm_change_coupling_t;

/* Commit data for coupling analysis */
typedef struct {
    char **files;
    int count;
    /* Unix epoch of the commit. 0 means unknown — coupling computation
     * still works but last_co_change on the resulting edge will be 0. */
    long long timestamp;
} cbm_commit_files_t;

/* Per-file temporal metadata. Populated alongside change-coupling so File
 * nodes can carry change_count and last_modified for hotspot / risk
 * analysis queries. */
typedef struct {
    char file_path[CBM_SZ_512];
    int change_count;
    long long last_modified; /* unix epoch of most recent commit */
} cbm_file_temporal_t;

/* Compute change coupling from commit history.
 * Returns number of couplings written to out (up to max_out).
 * Caller owns out[]. */
int cbm_compute_change_coupling(const cbm_commit_files_t *commits, int commit_count,
                                cbm_change_coupling_t *out, int max_out);
int cbm_compute_change_coupling_with_threshold(const cbm_commit_files_t *commits,
                                               int commit_count,
                                               cbm_change_coupling_t *out, int max_out,
                                               double min_coupling_score);

/* Go-style implicit interface satisfaction on graph buffer.
 * Finds Interface nodes, matches method sets against Class nodes,
 * creates IMPLEMENTS + OVERRIDE edges. Returns edge count created. */
int cbm_pipeline_implements_go(cbm_pipeline_ctx_t *ctx);

/* ── Git diff helpers (pass_gitdiff.c) ───────────────────────────── */

typedef struct {
    char status[CBM_SZ_4]; /* M/A/D/R */ /* "M", "A", "D", "R" */
    char path[CBM_SZ_512];
    char old_path[CBM_SZ_512]; /* non-empty only for renames */
} cbm_changed_file_t;

typedef struct {
    char path[CBM_SZ_512];
    int start_line;
    int end_line;
} cbm_changed_hunk_t;

/* Parse git diff --name-status output. Returns count written to out. */
int cbm_parse_name_status(const char *output, cbm_changed_file_t *out, int max_out);

/* Parse git diff --unified=0 output. Returns count written to out. */
int cbm_parse_hunks(const char *output, cbm_changed_hunk_t *out, int max_out);

/* Parse "start,count" or "start" → (start, count). */
void cbm_parse_range(const char *s, int *out_start, int *out_count);

/* ── Config helpers (pass_configures.c) ──────────────────────────── */

/* Check if a string looks like an environment variable name
 * (uppercase + underscore + digits, at least 2 chars with uppercase). */
bool cbm_is_env_var_name(const char *s);

/* Normalize a config key: split camelCase/snake/dots, lowercase.
 * Writes normalized form to norm_out (underscore-joined).
 * Returns token count. tokens_out[] receives borrowed pointers into norm_out. */
int cbm_normalize_config_key(const char *key, char *norm_out, size_t norm_sz);

/* Check if a file path has a config file extension (.toml, .yaml, .env, etc.) */
bool cbm_has_config_extension(const char *path);

/* ── Enrichment helpers (pass_enrichment.c) ──────────────────────── */

/* Split camelCase string on lowercase→uppercase transitions.
 * Writes substrings to out[]. Returns count. Caller must free each out[i]. */
int cbm_split_camel_case(const char *s, char **out, int max_out);

/* Tokenize a decorator into lowercase words, filtering stopwords.
 * E.g. "@login_required" → ["login", "required"].
 * Writes words to out[]. Returns count. Caller must free each out[i]. */
int cbm_tokenize_decorator(const char *dec, char **out, int max_out);

/* ── Compile commands helpers (pass_compile_commands.c) ──────────── */

typedef struct {
    char **include_paths;
    int include_count;
    char **defines;
    int define_count;
    char standard[CBM_SZ_32];
} cbm_compile_flags_t;

/* Split a shell command string into arguments (handles quoting).
 * Writes args to out[]. Returns count. Caller must free each out[i]. */
int cbm_split_command(const char *cmd, char **out, int max_out);

/* Extract -I, -isystem, -D, -std= flags from compiler arguments.
 * Caller must free result with cbm_compile_flags_free(). */
cbm_compile_flags_t *cbm_extract_flags(const char **args, int argc, const char *directory);

/* Free a compile_flags_t allocated by cbm_extract_flags(). */
void cbm_compile_flags_free(cbm_compile_flags_t *f);

/* Parse compile_commands.json content. Returns map as parallel arrays.
 * out_paths[i] is the relative file path, out_flags[i] is its flags.
 * Returns count. Caller must free out_paths[i] and cbm_compile_flags_free(out_flags[i]). */
int cbm_parse_compile_commands(const char *json_data, const char *repo_path, char ***out_paths,
                               cbm_compile_flags_t ***out_flags);

/* ── Infrascan helpers (pass_infrascan.c) ─────────────────────────── */

/* File identification helpers */
bool cbm_is_dockerfile(const char *name);
bool cbm_is_compose_file(const char *name);
bool cbm_is_cloudbuild_file(const char *name);
bool cbm_is_env_file(const char *name);
bool cbm_is_shell_script(const char *name, const char *ext);
bool cbm_is_kustomize_file(const char *name);
bool cbm_is_k8s_manifest(const char *name, const char *content);

/* Secret detection */
bool cbm_is_secret_binding(const char *key, const char *value);
bool cbm_is_secret_value(const char *value);

/* Clean JSON array brackets from CMD/ENTRYPOINT values.
 * E.g. ["./app", "--flag"] → ./app --flag
 * Writes result to out (up to out_sz). */
void cbm_clean_json_brackets(const char *s, char *out, size_t out_sz);

/* Key-value pair for environment variables / config entries */
typedef struct {
    char key[CBM_SZ_128];
    char value[CBM_SZ_512];
} cbm_env_kv_t;

/* Dockerfile parsing result */
typedef struct {
    char base_image[CBM_SZ_256];
    char stage_images[CBM_SZ_16][CBM_SZ_256];
    char stage_names[CBM_SZ_16][CBM_SZ_128];
    int stage_count;
    char exposed_ports[CBM_SZ_16][CBM_SZ_32];
    int port_count;
    cbm_env_kv_t env_vars[CBM_SZ_64];
    int env_count;
    char build_args[CBM_SZ_32][CBM_SZ_128];
    int build_arg_count;
    char workdir[CBM_SZ_256];
    char cmd[CBM_SZ_512];
    char entrypoint[CBM_SZ_512];
    char healthcheck[CBM_SZ_512];
    char user[CBM_SZ_64];
} cbm_dockerfile_result_t;

/* Dotenv parsing result */
typedef struct {
    cbm_env_kv_t env_vars[CBM_SZ_64];
    int env_count;
} cbm_dotenv_result_t;

/* Shell script parsing result */
typedef struct {
    char shebang[CBM_SZ_256];
    cbm_env_kv_t env_vars[CBM_SZ_64];
    int env_count;
    char sources[CBM_SZ_16][CBM_SZ_256];
    int source_count;
    char docker_cmds[CBM_SZ_16][CBM_SZ_256];
    int docker_cmd_count;
} cbm_shell_result_t;

/* Terraform variable */
typedef struct {
    char name[CBM_SZ_128];
    char type[CBM_SZ_64];
    char default_val[CBM_SZ_256];
    char description[CBM_SZ_256];
} cbm_tf_variable_t;

/* Terraform resource / data source */
typedef struct {
    char type[CBM_SZ_128];
    char name[CBM_SZ_128];
} cbm_tf_resource_t;

/* Terraform module */
typedef struct {
    char tf_name[CBM_SZ_128];
    char source[CBM_SZ_256];
} cbm_tf_module_t;

/* Terraform parsing result */
typedef struct {
    cbm_tf_resource_t resources[CBM_SZ_32];
    int resource_count;
    cbm_tf_variable_t variables[CBM_SZ_32];
    int variable_count;
    char outputs[CBM_SZ_32][CBM_SZ_128];
    int output_count;
    char providers[CBM_SZ_16][CBM_SZ_128];
    int provider_count;
    cbm_tf_module_t modules[CBM_SZ_16];
    int module_count;
    cbm_tf_resource_t data_sources[CBM_SZ_16];
    int data_source_count;
    char backend[CBM_SZ_128];
    bool has_locals;
} cbm_terraform_result_t;

/* Parse a Dockerfile from source text. Returns 0 if parsed, -1 if empty/invalid. */
int cbm_parse_dockerfile_source(const char *source, cbm_dockerfile_result_t *out);

/* Parse a .env file from source text. Returns 0 if parsed, -1 if empty. */
int cbm_parse_dotenv_source(const char *source, cbm_dotenv_result_t *out);

/* Parse a shell script from source text. Returns 0 if parsed, -1 if empty. */
int cbm_parse_shell_source(const char *source, cbm_shell_result_t *out);

/* Parse a Terraform file from source text. Returns 0 if parsed, -1 if empty. */
int cbm_parse_terraform_source(const char *source, cbm_terraform_result_t *out);

/* Helm Chart.yaml parse result: chart name + dependency chart names (#338). */
enum { CBM_HELM_MAX_DEPS = 128, CBM_HELM_NAME_MAX = 128 };
typedef struct {
    char chart_name[CBM_HELM_NAME_MAX];
    char deps[CBM_HELM_MAX_DEPS][CBM_HELM_NAME_MAX];
    int dep_count;
} cbm_helm_chart_t;

/* Parse a Helm Chart.yaml: top-level `name:` and `dependencies:` list names.
 * Returns 0 if parsed (name or deps found), -1 otherwise. */
int cbm_parse_helm_chart(const char *source, cbm_helm_chart_t *out);

/* Build an infrastructure QN. Caller must free the returned string. */
char *cbm_infra_qn(const char *project_name, const char *rel_path, const char *infra_type,
                   const char *service_name);

/* ── Parallel pipeline prototypes (pass_parallel.c) ─────────────── */

/* Phase 3A: Parallel extract + create definition nodes.
 * Each worker creates nodes in a per-worker gbuf, then merges into ctx->gbuf.
 * Caches CBMFileResult* in result_cache[file_idx] for reuse in Phase 3B/4.
 * shared_ids provides globally unique node/edge IDs across workers. */
int cbm_parallel_extract(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                         CBMFileResult **result_cache, _Atomic int64_t *shared_ids,
                         int worker_count);

/* Phase 3B: Serial registry build from cached extraction results.
 * Creates DEFINES, DEFINES_METHOD, and IMPORTS edges in ctx->gbuf.
 * Registers callable symbols (Function/Method/Class) in ctx->registry. */
int cbm_build_registry_from_cache(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count, CBMFileResult **result_cache);

/* Phase 4: Parallel call/usage/semantic resolution.
 * Each worker resolves calls, usages, throws, rw, inherits, decorates,
 * and implements edges into per-worker edge bufs, then merges.
 * Runs Go-style implicit IMPLEMENTS as serial post-step. */
/* Opaque module-def index — defined in pass_lsp_cross.c. Forward-declared
 * here so we can include it in cbm_parallel_resolve's signature without
 * pulling the pass header into every consumer of pipeline_internal.h. */
struct CBMModuleDefIndex;

/* cbm_parallel_resolve's cross_registries param is typed `void*` to avoid
 * pulling lsp/go_lsp.h into every TU that includes pipeline_internal.h.
 * Callers cast a CBMCrossLspRegistries* (defined in pass_lsp_cross.h). */

int cbm_parallel_resolve(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                         CBMFileResult **result_cache, _Atomic int64_t *shared_ids,
                         int worker_count,
                         /* Cross-file LSP inputs — pre-built once by the caller and
                          * shared read-only across workers (typed non-const to match
                          * the existing cbm_run_X_lsp_cross signatures the resolve
                          * worker forwards them to). Pass NULL/0/NULL to skip. */
                         CBMLSPDef *all_defs, int def_count, char *const *def_modules,
                         /* Optional inverted index module_qn → defs[] — fallback
                          * path when there's no pre-built registry for this lang. */
                         struct CBMModuleDefIndex *module_def_index,
                         /* Optional Tier 2 full: pre-built per-language registries.
                          * For each language with a non-NULL entry, workers use the
                          * cbm_run_X_lsp_cross_with_registry fast path (skip per-
                          * file registry build entirely). Falls back to the filter
                          * + per-file build path when entry is NULL or struct is NULL.
                          * Typed as void* here to dodge the typedef/tag ordering
                          * problem — pass_parallel.c casts back to CBMCrossLspRegistries*. */
                         void *cross_registries);

/* Post-merge: create Route nodes for HTTP_CALLS/ASYNC_CALLS edges that
 * have url_path in properties but point to library functions instead of routes.
 * Re-targets these edges to Route nodes for cross-service traversal. */
void cbm_pipeline_clear_route_derived_edges(cbm_gbuf_t *gb);
void cbm_pipeline_create_route_nodes(cbm_gbuf_t *gb);

int cbm_pipeline_ensure_file_structure(cbm_gbuf_t *gbuf, const char *project,
                                       const char *root_qn, const char *rel_path,
                                       CBMHashTable *seen_dirs);

/* ── Pass function prototypes ────────────────────────────────────── */

int cbm_pipeline_pass_definitions(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count);

int cbm_pipeline_pass_k8s(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count);

int cbm_pipeline_pass_calls(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count);

/* Cross-file LSP type-aware call resolution pass. Augments per-file
 * resolved_calls with cross-file resolutions before call edges are emitted.
 * Implementation: src/pipeline/pass_lsp_cross.c. */
int cbm_pipeline_pass_lsp_cross(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                int file_count, CBMFileResult **cache);

/* Sub-passes called from pass_calls: pattern-based edge extraction */
void cbm_pipeline_pass_fastapi_depends(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                       int file_count);

int cbm_pipeline_pass_usages(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count);

int cbm_pipeline_pass_semantic(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                               int file_count);

int cbm_pipeline_pass_tests(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count);

int cbm_pipeline_pass_githistory(cbm_pipeline_ctx_t *ctx);

/* Pre-computed git history result for fused post-pass parallelism. */
typedef struct {
    cbm_change_coupling_t *couplings;
    int count;
    int commit_count;
    /* Per-file temporal data (change_count + last_modified) for File nodes.
     * NULL when the history pass had no commits to analyse. */
    cbm_file_temporal_t *file_temporal;
    int file_temporal_count;
} cbm_githistory_result_t;

/* Compute change couplings without touching the graph buffer.
 * Can run on a separate thread while other passes use the gbuf. */
int cbm_pipeline_githistory_compute(const char *repo_path, cbm_githistory_result_t *result);
int cbm_pipeline_githistory_compute_with_threshold(const char *repo_path,
                                                   cbm_githistory_result_t *result,
                                                   double min_coupling_score);

/* Apply pre-computed couplings to the graph buffer (main thread only). */
int cbm_pipeline_githistory_apply(cbm_pipeline_ctx_t *ctx, const cbm_githistory_result_t *result);

/* Pre-dump pass: decorator tags enrichment (operates on gbuf). */
int cbm_pipeline_pass_decorator_tags(cbm_gbuf_t *gbuf, const char *project);

/* Pre-dump pass: config ↔ code linking. */
int cbm_pipeline_pass_configlink(cbm_pipeline_ctx_t *ctx);

/* K8s / Kustomize pass: emits Module nodes for kustomization.yaml overlays and
 * Resource nodes for generic Kubernetes manifests. */
int cbm_pipeline_pass_k8s(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count);

/* Pre-dump pass: structural invariant enforcement (Method→Class, Field→Class edges). */
void cbm_pipeline_pass_normalize(cbm_gbuf_t *gb);

/* HTTP endpoint discovery pass (fork-only; upstream dropped httplinks). Extracts HTTP
 * routes from Go/Express/Laravel/Ktor/Python frameworks and links them into the graph. */
int cbm_pipeline_pass_httplinks(cbm_pipeline_ctx_t *ctx);

/* Pre-dump pass: SIMILAR_TO edges via MinHash fingerprinting. */
int cbm_pipeline_pass_similarity(cbm_pipeline_ctx_t *ctx);

/* Pre-dump pass: SEMANTICALLY_RELATED edges via algorithmic embeddings.
 * Opt-in: only runs when CBM_SEMANTIC_ENABLED=1. */
int cbm_pipeline_pass_semantic_edges(cbm_pipeline_ctx_t *ctx);

/* Pre-dump pass: interprocedural complexity propagation (Tier B).
 * Propagates per-function loop_depth along CALLS edges into a transitive
 * worst-case nested-loop estimate (transitive_loop_depth) and flags call-graph
 * cycles (recursive). Runs on the graph buffer before the dump. */
void cbm_pipeline_pass_complexity(cbm_pipeline_ctx_t *ctx);
void cbm_pipeline_pass_complexity_for_paths(cbm_pipeline_ctx_t *ctx, const char *const *paths,
                                            int path_count);

/* ── Env URL scanner (pass_envscan.c) ────────────────────────────── */

typedef struct {
    char key[CBM_SZ_128];
    char value[CBM_SZ_512];
    char file_path[CBM_SZ_256];
} cbm_env_binding_t;

/* Scan a project directory for environment variable assignments with URL values.
 * Walks the filesystem, scans Dockerfiles, shell scripts, .env, YAML, TOML,
 * Terraform, and .properties files. Filters out secrets.
 * Returns number of bindings written to out (up to max_out). */
int cbm_scan_project_env_urls(const char *root_path, cbm_env_binding_t *out, int max_out);

/* Free all compiled regex patterns used by cbm_scan_project_env_urls.
 * Patterns are compiled lazily on first use and cached for the process lifetime.
 * Call this in test teardown to release ~26KB of regex memory cleanly. */
void cbm_envscan_free_patterns(void);

/* ── Incremental pipeline (pipeline_incremental.c) ───────────────── */

/* Run incremental re-index on an existing disk DB.
 * Classifies files by metadata, hash-confirms metadata-equal files when
 * compatible file_state rows exist, loads the current DB into a graph buffer,
 * reparses changed files, and republishes the graph. Returns 0 on success. */
int cbm_pipeline_run_incremental(cbm_pipeline_t *p, const char *db_path, cbm_file_info_t *files,
                                 int file_count);

/* Pipeline accessors for incremental use */
const char *cbm_pipeline_repo_path(const cbm_pipeline_t *p);
atomic_int *cbm_pipeline_cancelled_ptr(cbm_pipeline_t *p);
/* Record committed graph size (#334 gate axis) from the incremental path,
 * which cannot see the opaque cbm_pipeline struct. Call before the dump. */
void cbm_pipeline_set_committed_counts(cbm_pipeline_t *p, int nodes, int edges);
void cbm_pipeline_set_graph_changed(cbm_pipeline_t *p, bool changed);
void cbm_pipeline_set_publish_kind(cbm_pipeline_t *p, cbm_pipeline_publish_kind_t kind);
void cbm_pipeline_set_publish_reason(cbm_pipeline_t *p, const char *reason);

/* Parse a gRPC stub call "<service-stub>.<method>" into the canonical proto
 * service name + method. Returns true ONLY when a recognized gRPC stub/client
 * suffix is present (the stub-type signal that gates Route emission, #294).
 * Exposed for testing. */
bool extract_grpc_service_method(const char *callee, char *service, size_t srv_sz, char *method,
                                 size_t meth_sz);

#endif /* CBM_PIPELINE_INTERNAL_H */
