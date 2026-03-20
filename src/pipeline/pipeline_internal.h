/*
 * pipeline_internal.h — Internal pipeline state shared between pass files.
 *
 * NOT a public header. Only included by pipeline.c and pass_*.c files.
 * Exposes the pipeline context struct for direct field access by passes.
 */
#ifndef CBM_PIPELINE_INTERNAL_H
#define CBM_PIPELINE_INTERNAL_H

#include "pipeline/pipeline.h"
#include "graph_buffer/graph_buffer.h"
#include "discover/discover.h"
#include "foundation/hash_table.h"
#include "cbm.h"
#include <stdatomic.h>

/* ── Shared pipeline constants ─────────────────────────────────── */

/* Maximum byte budget for tree-sitter extraction per file */
#define CBM_EXTRACT_BUDGET 5000000

/* Time unit conversions */
#define CBM_NS_PER_SEC 1000000000LL
#define CBM_US_PER_SEC 1000000LL
#define CBM_MS_PER_SEC 1000.0
#define CBM_US_PER_SEC_F 1e6

/* ── Pre-scan results (extracted during parallel phase) ──────────── */

/* HTTP call site discovered during extraction (source already in memory).
 * Eliminates 2M+ disk reads in httplinks pass. */
typedef struct {
    char path[256];        /* extracted URL path */
    char method[16];       /* HTTP method (empty if unknown) */
    char source_name[256]; /* function name */
    char source_qn[512];   /* function qualified name */
    char source_label[32]; /* "Function" or "Method" */
    bool is_async;         /* async dispatch vs HTTP call */
} cbm_prescan_http_site_t;

/* Config file reference found in source during extraction.
 * Eliminates 62K+ disk reads in configlink pass. */
typedef struct {
    char ref_path[256]; /* referenced config file path (e.g. "config.yaml") */
} cbm_prescan_config_ref_t;

/* HTTP route discovered during extraction. */
typedef struct {
    char path[256];
    char method[16];
    char function_name[256];
    char qualified_name[512];
    char handler_ref[256];
    char protocol[8]; /* "ws", "sse", or "" */
} cbm_prescan_route_t;

/* Per-file prescan results, parallel to result_cache[file_idx]. */
typedef struct {
    cbm_prescan_http_site_t *http_sites;
    int http_site_count;
    cbm_prescan_config_ref_t *config_refs;
    int config_ref_count;
    cbm_prescan_route_t *routes;
    int route_count;
} cbm_prescan_t;

/* ── Pipeline context (internal) ─────────────────────────────────── */

/* Shared context passed to each pass function.
 * Derived from cbm_pipeline_t fields during run. */
typedef struct {
    const char *project_name; /* borrowed from pipeline */
    const char *repo_path;    /* borrowed from pipeline */
    cbm_gbuf_t *gbuf;         /* owned by pipeline */
    cbm_registry_t *registry; /* owned by pipeline */
    atomic_int *cancelled;    /* pointer to pipeline's cancelled flag */

    /* Extraction result cache (sequential pipeline optimization).
     * When non-NULL, pass_definitions stores results here instead of freeing,
     * and pass_calls/usages/semantic reuse cached results instead of re-extracting.
     * Indexed by file position in the files[] array. Owned by pipeline.c. */
    CBMFileResult **result_cache;

    /* Pre-scan cache — indexed by file position, parallel to result_cache.
     * Populated during extraction phase while source is in memory.
     * Contains HTTP call sites and config file references extracted from source.
     * Eliminates disk re-reads in httplinks and configlink passes. */
    cbm_prescan_t *prescan_cache;
    int prescan_count;

    /* File path → index hash map for prescan lookup by rel_path.
     * Allows httplinks (which iterates graph nodes, not files) to find
     * prescan data by node->file_path. */
    CBMHashTable *prescan_path_map;
} cbm_pipeline_ctx_t;

/* Check cancellation. Returns non-zero if cancelled. */
static inline int cbm_pipeline_check_cancel(const cbm_pipeline_ctx_t *ctx) {
    return atomic_load(ctx->cancelled) ? -1 : 0;
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
    char file_a[512];
    char file_b[512];
    int co_change_count;
    double coupling_score;
} cbm_change_coupling_t;

/* Commit data for coupling analysis */
typedef struct {
    char **files;
    int count;
} cbm_commit_files_t;

/* Compute change coupling from commit history.
 * Returns number of couplings written to out (up to max_out).
 * Caller owns out[]. */
int cbm_compute_change_coupling(const cbm_commit_files_t *commits, int commit_count,
                                cbm_change_coupling_t *out, int max_out);

/* Go-style implicit interface satisfaction on graph buffer.
 * Finds Interface nodes, matches method sets against Class nodes,
 * creates IMPLEMENTS + OVERRIDE edges. Returns edge count created. */
int cbm_pipeline_implements_go(cbm_pipeline_ctx_t *ctx);

/* ── Git diff helpers (pass_gitdiff.c) ───────────────────────────── */

typedef struct {
    char status[4]; /* "M", "A", "D", "R" */
    char path[512];
    char old_path[512]; /* non-empty only for renames */
} cbm_changed_file_t;

typedef struct {
    char path[512];
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
    char standard[32];
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

/* Secret detection */
bool cbm_is_secret_binding(const char *key, const char *value);
bool cbm_is_secret_value(const char *value);

/* Clean JSON array brackets from CMD/ENTRYPOINT values.
 * E.g. ["./app", "--flag"] → ./app --flag
 * Writes result to out (up to out_sz). */
void cbm_clean_json_brackets(const char *s, char *out, size_t out_sz);

/* Key-value pair for environment variables / config entries */
typedef struct {
    char key[128];
    char value[512];
} cbm_env_kv_t;

/* Dockerfile parsing result */
typedef struct {
    char base_image[256];
    char stage_images[16][256];
    char stage_names[16][128];
    int stage_count;
    char exposed_ports[16][32];
    int port_count;
    cbm_env_kv_t env_vars[64];
    int env_count;
    char build_args[32][128];
    int build_arg_count;
    char workdir[256];
    char cmd[512];
    char entrypoint[512];
    char healthcheck[512];
    char user[64];
} cbm_dockerfile_result_t;

/* Dotenv parsing result */
typedef struct {
    cbm_env_kv_t env_vars[64];
    int env_count;
} cbm_dotenv_result_t;

/* Shell script parsing result */
typedef struct {
    char shebang[256];
    cbm_env_kv_t env_vars[64];
    int env_count;
    char sources[16][256];
    int source_count;
    char docker_cmds[16][256];
    int docker_cmd_count;
} cbm_shell_result_t;

/* Terraform variable */
typedef struct {
    char name[128];
    char type[64];
    char default_val[256];
    char description[256];
} cbm_tf_variable_t;

/* Terraform resource / data source */
typedef struct {
    char type[128];
    char name[128];
} cbm_tf_resource_t;

/* Terraform module */
typedef struct {
    char tf_name[128];
    char source[256];
} cbm_tf_module_t;

/* Terraform parsing result */
typedef struct {
    cbm_tf_resource_t resources[32];
    int resource_count;
    cbm_tf_variable_t variables[32];
    int variable_count;
    char outputs[32][128];
    int output_count;
    char providers[16][128];
    int provider_count;
    cbm_tf_module_t modules[16];
    int module_count;
    cbm_tf_resource_t data_sources[16];
    int data_source_count;
    char backend[128];
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

/* Build an infrastructure QN. Caller must free the returned string. */
char *cbm_infra_qn(const char *project_name, const char *rel_path, const char *infra_type,
                   const char *service_name);

/* ── Parallel pipeline prototypes (pass_parallel.c) ─────────────── */

/* Phase 3A: Parallel extract + create definition nodes.
 * Each worker creates nodes in a per-worker gbuf, then merges into ctx->gbuf.
 * Caches CBMFileResult* in result_cache[file_idx] for reuse in Phase 3B/4.
 * shared_ids provides globally unique node/edge IDs across workers. */
int cbm_parallel_extract(cbm_pipeline_ctx_t *ctx, cbm_file_info_t *files, int file_count,
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
int cbm_parallel_resolve(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count,
                         CBMFileResult **result_cache, _Atomic int64_t *shared_ids,
                         int worker_count);

/* ── Pass function prototypes ────────────────────────────────────── */

int cbm_pipeline_pass_definitions(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files,
                                  int file_count);

int cbm_pipeline_pass_calls(cbm_pipeline_ctx_t *ctx, const cbm_file_info_t *files, int file_count);

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
} cbm_githistory_result_t;

/* Compute change couplings without touching the graph buffer.
 * Can run on a separate thread while other passes use the gbuf. */
int cbm_pipeline_githistory_compute(const char *repo_path, cbm_githistory_result_t *result);

/* Apply pre-computed couplings to the graph buffer (main thread only). */
int cbm_pipeline_githistory_apply(cbm_pipeline_ctx_t *ctx, const cbm_githistory_result_t *result);

int cbm_pipeline_pass_httplinks(cbm_pipeline_ctx_t *ctx);

/* Pre-dump pass: decorator tags enrichment (operates on gbuf). */
int cbm_pipeline_pass_decorator_tags(cbm_gbuf_t *gbuf, const char *project);

/* Pre-dump pass: config ↔ code linking.
 * Uses prescan cache when available, falls back to disk reads. */
int cbm_pipeline_pass_configlink(cbm_pipeline_ctx_t *ctx);

/* ── Env URL scanner (pass_envscan.c) ────────────────────────────── */

typedef struct {
    char key[128];
    char value[512];
    char file_path[256];
} cbm_env_binding_t;

/* Scan a project directory for environment variable assignments with URL values.
 * Walks the filesystem, scans Dockerfiles, shell scripts, .env, YAML, TOML,
 * Terraform, and .properties files. Filters out secrets.
 * Returns number of bindings written to out (up to max_out). */
int cbm_scan_project_env_urls(const char *root_path, cbm_env_binding_t *out, int max_out);

/* ── Incremental pipeline (pipeline_incremental.c) ───────────────── */

/* Run incremental re-index on an existing disk DB.
 * Classifies files by mtime+size, deletes changed nodes, re-parses changed
 * files, merges into disk DB. Returns 0 on success. */
int cbm_pipeline_run_incremental(cbm_pipeline_t *p, const char *db_path, cbm_file_info_t *files,
                                 int file_count);

/* Pipeline accessors for incremental use */
const char *cbm_pipeline_repo_path(const cbm_pipeline_t *p);
atomic_int *cbm_pipeline_cancelled_ptr(cbm_pipeline_t *p);

#endif /* CBM_PIPELINE_INTERNAL_H */
