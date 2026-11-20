/*
 * pass_lsp_cross.h — Cross-file LSP helpers shared with the parallel
 * resolve pass.
 *
 * Per-file LSP (cbm_run_X_lsp inside cbm_extract_file) only sees a single
 * file's defs in its registry, so callees whose receiver type comes from
 * an imported module stay unresolved. The helpers declared here close
 * that gap: they let the parallel resolve worker (pass_parallel.c) build
 * a project-wide CBMLSPDef[] and invoke the language-specific
 * cbm_run_X_lsp_cross resolver on each file using the file's already-
 * built import map. Resolved calls are appended to result->resolved_calls
 * so the same cbm_pipeline_find_lsp_resolution path that handles per-
 * file LSP picks them up.
 *
 * The languages covered are the rows of one engine table (pass_lsp_cross.c
 * pxc_engines): Go, C/C++/CUDA, Python, TypeScript/JavaScript/JSX/TSX, PHP,
 * C#, Ruby, Java, Kotlin and Rust. Anything else short-circuits via
 * cbm_pxc_has_cross_lsp.
 *
 * Previously this work ran as a separate sequential pipeline pass
 * (cbm_pipeline_pass_lsp_cross) that re-read every source file from
 * disk and re-parsed each tree-sitter tree on a single thread — a 50×
 * regression vs the parallel extract pass on large repos. The pass was
 * deleted; the resolve worker now invokes these helpers directly using
 * the source bytes retained in result->arena during extract.
 */
#ifndef CBM_PIPELINE_PASS_LSP_CROSS_H
#define CBM_PIPELINE_PASS_LSP_CROSS_H

#include "cbm.h"
/* CBMLSPDef historically lives in lsp/go_lsp.h (not lsp/type_rep.h)
 * — type_rep.h covers the type-representation primitives while
 * go_lsp.h was where the project-wide def descriptor landed first. */
#include "lsp/go_lsp.h"
#include "lsp/py_lsp.h"   /* cbm_py_build_cross_registry / cbm_run_py_lsp_cross_with_registry */
#include "lsp/c_lsp.h"    /* cbm_c_build_cross_registry / cbm_run_c_lsp_cross_with_registry */
#include "lsp/cs_lsp.h"   /* cbm_cs_build_cross_registry / cbm_run_cs_lsp_cross_with_registry */
#include "lsp/ts_lsp.h"   /* cbm_ts_build_cross_registry / cbm_run_ts_lsp_cross_with_registry */
#include "lsp/java_lsp.h" /* cbm_java_build_cross_registry / cbm_run_java_lsp_cross_with_registry */
#include "lsp/rust_lsp.h" /* cbm_rust_build_cross_registry / cbm_run_rust_lsp_cross_with_registry */
#include "lsp/ruby_lsp.h" /* cbm_ruby_build_cross_registry; _cross is the per-file fallback */
#include "pipeline/pipeline_internal.h"
#include "pipeline/type_scope.h" /* cbm_type_scope_file_t */
#include <stdbool.h>

/* ── The engine table ──
 *
 * One row per type-aware engine (pass_lsp_cross.c pxc_engines): the
 * languages it serves, the shared registry it builds once over all defs, its
 * walk of one file against a registry, its walk of one file over a def list,
 * and whether it reads declared-type identities (type_scope.h). Every
 * per-language decision of the cross-file pass is a field of that row; an
 * engine plugs in by adding its row. A row's index is its engine number. */
enum { CBM_PXC_MAX_ENGINES = 16 };

/* Number of rows; a row's name ("go", "python", ...; NULL out of range); the
 * row that serves `lang` (-1: none). */
int cbm_pxc_engine_count(void);
const char *cbm_pxc_engine_name(int engine);
int cbm_pxc_engine_of_lang(CBMLanguage lang);

/* True iff this language has an engine row. */
bool cbm_pxc_has_cross_lsp(CBMLanguage lang);
/* True iff the language's engine reads declared-type identities: the
 * collector attaches them (cbm_type_scope_lang must have the language's
 * row) and the per-file def selection follows them. */
bool cbm_pxc_wants_identities(CBMLanguage lang);

/* One file's declared-type scope, exactly as the collector builds it to
 * attach the file's identities (the project registry, the file's module and
 * LSP import map, and for a lexical row the file's definitions and module
 * facts): a walk reads the calling file's own identity table from it
 * (cbm_type_scope_file_imports). NULL without a pipeline context or for an
 * engine that reads no identities; else end it with cbm_type_scope_file_end
 * once the walk is done. */
cbm_type_scope_file_t *cbm_pxc_file_scope(const cbm_pipeline_ctx_t *ctx, CBMLanguage lang,
                                          const CBMFileResult *result, const char *module_qn,
                                          const char **imp_keys, const char **imp_vals,
                                          int imp_count);

/* Collect a project-wide CBMLSPDef[] from every cached file result.
 * def_modules[i] receives the module QN for files[i] (malloc'd; the
 * caller frees each entry then the array). String fields in the
 * returned CBMLSPDef[] are borrowed from cache[i]->arena and from
 * def_modules[i] — caller must keep both alive while the array is in
 * use. Returns the malloc'd array (free() it) and writes the entry
 * count to *out_count. Returns NULL on alloc failure or when no defs
 * exist. out_def_starts (optional, file_count + 1 entries, caller-owned)
 * receives per-file prefix offsets: file i's defs occupy
 * [out_def_starts[i], out_def_starts[i+1]) — the LSP-surface serializer
 * needs the per-file slices, which the flat array does not otherwise
 * record.
 *
 * `ctx` (nullable) enables cross-file base-class resolution: for the
 * languages whose cross registrars read embedded_types as qualified names
 * (Python, JS/TS/TSX), every CBMDefinition.base_classes spelling is resolved
 * to a project QN through ctx->registry plus the file's import map — the same
 * inputs pass_semantic uses to draw its INHERITS edge, so the LSP's
 * inheritance view and the graph's cannot diverge. Pass NULL to keep the raw
 * source spelling (surface-probe paths that build no registry). */
CBMLSPDef *cbm_pxc_collect_all_defs(const cbm_pipeline_ctx_t *ctx, CBMArena *arena,
                                    CBMFileResult **cache, const cbm_file_info_t *files,
                                    int file_count, const char *project_name, char **def_modules,
                                    int *out_count, int *out_def_starts);

/* Detect TS dialect flags from a relative path. */
void cbm_pxc_ts_modes(CBMLanguage lang, const char *rel_path, bool *out_js, bool *out_jsx,
                      bool *out_dts);

/* Build the local-name -> semantic import-QN map consumed by cross-file LSPs.
 * Both sequential and parallel drivers use this exact helper so import
 * metadata cannot diverge between pipelines. Values are owned by the returned
 * map (not borrowed from gbuf); release both arrays with
 * cbm_pxc_free_import_map(). A complete PHP binding snapshot also permits an
 * allocated empty map. This compatibility projection alone never proves
 * vendor absence: consumers require cbm_pipeline_php_vendor_names_build's
 * separate typed evidence, including completion and full import identities. */
int cbm_pxc_build_import_map(const cbm_gbuf_t *gbuf, const char *project_name, const char *rel_path,
                             CBMLanguage lang, const CBMFileResult *result, const char ***out_keys,
                             const char ***out_vals, int *out_count);
/* As cbm_pxc_build_import_map (which is include_type_only = false, the CALLS
 * map), but with include_type_only also binding annotation-only imports
 * (Python `if TYPE_CHECKING:`), for the consumers that resolve annotations:
 * the Python LSP and the cross-file base/field-type resolution (#1277). */
int cbm_pxc_build_import_map_ex(const cbm_gbuf_t *gbuf, const char *project_name,
                                const char *rel_path, CBMLanguage lang, const CBMFileResult *result,
                                bool include_type_only, const char ***out_keys,
                                const char ***out_vals, int *out_count);
/* True when the file records an annotation-only import (the LSP map then
 * differs from the CALLS map and must be built separately). */
bool cbm_pxc_result_has_type_only_import(const CBMFileResult *result);

void cbm_pxc_free_import_map(const char **keys, const char **vals, int count);

/* ── Per-module def index (the gopls "package summary" pattern) ──
 *
 * The hot path used to register ALL all_defs[] into a fresh registry
 * per file (~110k defs × 11k files for kubernetes = ~21,000 CPU-s of
 * arena_strdup). Most of those defs are irrelevant to any one file —
 * each file only references defs from its own module + its imported
 * modules. gopls observed the same: it builds per-package summaries
 * and per-file only loads the summaries the file imports.
 *
 * cbm_pxc_build_module_def_index() builds inverted indexes once (O(D)):
 * def_module_qn → defs and declared namespace/package → defs.
 * cbm_pxc_filter_defs_for_file() then returns own_module + imp_qns for
 * most languages. For Java/Kotlin callers it additionally returns
 * same-namespace JVM defs so Gradle/Maven mixed source roots
 * (`src/main/java/...` + `src/main/kotlin/...`) resolve same-package
 * references without falling back to a full project registry per file. */
typedef struct CBMModuleDefIndex CBMModuleDefIndex;

CBMModuleDefIndex *cbm_pxc_build_module_def_index(CBMLSPDef *all_defs, int def_count);

void cbm_pxc_free_module_def_index(CBMModuleDefIndex *idx);

/* Return a malloc'd CBMLSPDef[] containing all defs whose
 * def_module_qn matches own_module OR any of imp_qns. For Java/Kotlin
 * callers, also include defs from the same declared package/namespace:
 * JVM same-package references often cross `src/main/java` and
 * `src/main/kotlin` roots without import statements. String fields inside
 * each entry are borrowed from the original all_defs[] arena (caller keeps
 * it alive). Caller frees the returned array with free(). Writes the entry
 * count to *out_count and sets *out_success on every valid selection. A valid
 * empty selection returns NULL with *out_count = 0 and *out_success = true;
 * NULL with *out_success = false means invalid input or allocation failure. */
CBMLSPDef *cbm_pxc_filter_defs_for_file(const CBMModuleDefIndex *idx, CBMLSPDef *all_defs,
                                        CBMLanguage caller_lang, const char *caller_namespace,
                                        const char *own_module, const char *const *imp_qns,
                                        int imp_count, int *out_count, bool *out_success);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Work counter for the growth tests: module-entry defs walked and identities
 * followed by the per-file selection's worklist. Test builds only. */
uint64_t cbm_pxc_test_reach_work(void);
#endif

/* ── Tier 2 full: pre-built per-engine cross-LSP registries ─────
 *
 * Each non-NULL registry is built ONCE per run (in a dedicated arena),
 * finalized, and shared READ-ONLY across all resolve workers for files of
 * that engine's languages. The walk then runs against a per-file overlay
 * chained to it and skips the per-file registry build entirely. NULL → the
 * per-file path. A row whose registry is lazy (Rust) is not built here: the
 * parallel resolver builds it on the first file that needs it. */
typedef struct {
    CBMTypeRegistry *by_engine[CBM_PXC_MAX_ENGINES]; /* indexed by engine row */
    /* The pipeline context the walks may read (the project registry, for the
     * calling file's own identities: cbm_pxc_file_scope). Every driver sets
     * it, also when it builds no registry; NULL in a harness without one. */
    const cbm_pipeline_ctx_t *ctx;
} CBMCrossLspRegistries;

/* Build one row's shared registry over all defs into out->by_engine[engine].
 * False (slot untouched) for a row without an eagerly built registry. */
bool cbm_pxc_build_engine_registry(int engine, CBMArena *arena, CBMLSPDef *defs, int def_count,
                                   CBMCrossLspRegistries *out);
/* Every row's, in row order. */
void cbm_pxc_build_cross_registries(CBMArena *arena, CBMLSPDef *defs, int def_count,
                                    CBMCrossLspRegistries *out);
/* The pre-built registry of the engine serving `lang`, or NULL if none was
 * built (or the language has no engine; r may be NULL). */
CBMTypeRegistry *cbm_pxc_registry_for_lang(const CBMCrossLspRegistries *r, CBMLanguage lang);

/* Per-file registry-build cost (#1669): how many defs the per-file cross-LSP
 * path actually registered, and how often the module filter failed. */
/* Count defs an overlay registered for one file (complexity-gate telemetry). */
void cbm_pxc_count_perfile_defs(uint64_t defs);

void cbm_pxc_filter_stats(uint64_t *defs_registered, uint64_t *build_files, uint64_t *filter_files,
                          uint64_t *filter_failed);

/* Build and borrow the Rust Cargo manifest used for cross-crate (#56) routing.
 * The manifest owns strings in manifest_arena; callers keep that arena alive
 * until every resolver worker has joined.  Each worker must install the shared
 * immutable pointer in its own TLS slot before dispatch and clear it afterward. */
struct CBMCargoManifest;
bool cbm_pxc_build_rust_manifest(const cbm_pipeline_ctx_t *ctx, CBMArena *manifest_arena,
                                 struct CBMCargoManifest *out_manifest);
void cbm_pxc_set_rust_manifest(const struct CBMCargoManifest *manifest);
/* A resolve worker keeps its per-file cross-LSP scratch arenas between files
 * from _begin to _end; _end must run on the same thread before it exits. */
void cbm_pxc_thread_scratch_begin(void);
void cbm_pxc_thread_scratch_end(void);
const struct CBMCargoManifest *cbm_pxc_get_rust_manifest(void);

/* Run the engine's per-file cross-file walk for one file over `defs`.
 * Appends resolved CALLS into r->resolved_calls (lives in r->arena). Caller
 * owns source, module_qn, defs, imp_names, imp_qns.
 * NOTE: defs is read-only in practice but typed non-const to match
 * the existing cbm_run_X_lsp_cross callee signatures. */
void cbm_pxc_run_one(CBMLanguage lang, CBMFileResult *r, const char *source, int source_len,
                     const char *module_qn, CBMLSPDef *defs, int def_count, const char **imp_names,
                     const char **imp_qns, int imp_count);

/* rel_path: the file's repo-relative path. Go needs it to name the file's
 * init() functions as the def extractor does (#1910); cbm_pxc_run_one passes
 * NULL (Go init() callers then stay flat). */
void cbm_pxc_run_one_with_php_bindings(CBMLanguage lang, CBMFileResult *r, const char *source,
                                       int source_len, const char *module_qn, const char *rel_path,
                                       CBMLSPDef *defs, int def_count, const char **imp_names,
                                       const char **imp_qns, int imp_count,
                                       const CBMPHPImportBindings *bindings);

/* Per-file cross-LSP dispatch shared by the parallel resolve worker AND the
 * sequential driver (one path = one semantics): shared prebuilt registry
 * (overlay pattern, no per-file registry build) → per-file fallback with
 * the module-def-index's FILTERED defs for engines without one.
 * rust_shared_get (nullable) supplies the lazily-built shared registry of
 * the engine whose registry is lazy (Rust). */
void cbm_pxc_dispatch_file(CBMLanguage lang, CBMFileResult *result, const char *source,
                           int source_len, const char *rel, const char *def_module,
                           const CBMCrossLspRegistries *cross_registries,
                           const CBMModuleDefIndex *module_def_index, CBMLSPDef *all_defs,
                           int all_def_count, const char **imp_keys, const char **imp_vals,
                           int imp_count, CBMTypeRegistry *(*rust_shared_get)(void *),
                           void *rust_shared_ctx);

void cbm_pxc_dispatch_file_with_php_bindings(
    CBMLanguage lang, CBMFileResult *result, const char *source, int source_len, const char *rel,
    const char *def_module, const CBMCrossLspRegistries *cross_registries,
    const CBMModuleDefIndex *module_def_index, CBMLSPDef *all_defs, int all_def_count,
    const char **imp_keys, const char **imp_vals, int imp_count,
    CBMTypeRegistry *(*rust_shared_get)(void *), void *rust_shared_ctx,
    const CBMPHPImportBindings *bindings);

#endif /* CBM_PIPELINE_PASS_LSP_CROSS_H */
