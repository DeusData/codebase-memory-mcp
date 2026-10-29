#ifndef CBM_PIPELINE_IMPORT_RESOLVE_H
#define CBM_PIPELINE_IMPORT_RESOLVE_H

#include "pipeline_internal.h"
#include "resolve/model.h"

/* The dependency endpoint is separate from the imported local binding. A
 * returned File/Module target does not prove which exported member an alias
 * names. Unresolved local names still bind and must block textual fallback. */
cbm_resolve_result_t cbm_pipeline_resolve_import_result(
    const cbm_pipeline_ctx_t *ctx, const char *source_rel, const char *source_file_qn,
    const CBMImport *imp, CBMHashTable *namespace_map, const cbm_gbuf_node_t **target);

/* Exact same-module ownership; discovery of another go.mod is insufficient. */
bool cbm_pipeline_go_import_owned(const cbm_pipeline_ctx_t *, const char *, const CBMImport *,
                                  const cbm_gbuf_node_t *);

/* Source/path facts for Go hybrid consumers, independently of dependency edges. */
bool cbm_pipeline_capture_go_imports(const cbm_pipeline_ctx_t *, const char *,
                                     const CBMFileResult *);

/* Shared coordinator writer used by full sequential and parallel extraction. */
int cbm_pipeline_create_import_edges(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                     const char *rel, CBMHashTable *namespace_map);

/* A module namespace slot is a candidate, never proof that it is exported.
 * FROM retains that slot even for a re-export without a definition node.
 * The caller releases the returned string with cbm_free(CBM_MEM_CLASS_SEMANTIC, ...). */
char *cbm_pipeline_python_import_slot(const cbm_gbuf_t *graph, const char *project,
                                      const char *source_rel, const CBMImport *import,
                                      bool *module);

/* Candidate paths only. Caller must independently prove the parent lacks
 * the requested attribute before falling back to this exact submodule. */
bool cbm_pipeline_python_submodule_candidate(const cbm_gbuf_t *, const char *, const char *,
                                             const CBMImport *, const cbm_gbuf_node_t **,
                                             const cbm_gbuf_node_t **);

/* Source-local Python bindings, independent of dependency edge endpoints.
 * Dotted module imports retain the exact loaded prefix and block the root's
 * unimported descendants. Both arrays and their strings are owned by caller;
 * cbm_pxc_free_import_map releases them. Allocation failure returns -1. */
int cbm_pipeline_python_import_map(const cbm_gbuf_t *gbuf, const char *project,
                                   const char *source_rel, const CBMFileResult *result,
                                   bool include_type_only, const char ***out_keys,
                                   const char ***out_vals, int *out_count);

/* ECMAScript named/namespace bindings use the source member and canonical
 * module identity, never the dependency File's extension-bearing QN. The actual
 * immutable pipeline context retains the admitted path-alias configuration. */
int cbm_pipeline_ecma_import_map(const cbm_pipeline_ctx_t *, const char *, const CBMFileResult *,
                                 bool, const char ***, const char ***, int *);

/* Positive PHP class bindings are independent of unresolved sibling imports.
 * Owned arrays/strings use the same release contract as Python bindings. */
int cbm_pipeline_php_import_map(const cbm_gbuf_t *graph, const char *project,
                                const char *source_rel, const CBMFileResult *result,
                                const char ***keys, const char ***values, int *count);

/* Immutable facts matching the current captured source and configuration.
 * Returned storage belongs to graph; invalid or incomplete snapshots return NULL. */
const cbm_resolve_import_snapshot_t *cbm_pipeline_import_snapshot(const cbm_gbuf_t *, const char *,
                                                                  const char *,
                                                                  const CBMFileResult *);

/* Source-audited Rust use bindings, separate from dependency File targets. */
int cbm_pipeline_rust_import_map(const cbm_gbuf_t *, const char *, const char *,
                                 const CBMFileResult *, const char ***, const char ***, int *);
/* Exact source include path -> declaration namespace, from the admission
 * snapshot; dependency File QNs keep their separate extension-bearing identity. */
int cbm_pipeline_c_include_map(const cbm_gbuf_t *, const char *, const char *,
                               const CBMFileResult *, const char ***, const char ***, int *);
#endif
