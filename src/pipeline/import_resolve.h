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

/* Shared coordinator writer used by full sequential and parallel extraction. */
int cbm_pipeline_create_import_edges(cbm_pipeline_ctx_t *ctx, const CBMFileResult *result,
                                     const char *rel, CBMHashTable *namespace_map);

/* A module namespace slot is a candidate, never proof that it is exported.
 * FROM retains that slot even for a re-export without a definition node. */
char *cbm_pipeline_python_import_slot(const cbm_gbuf_t *graph, const char *project,
                                      const char *source_rel, const CBMImport *import,
                                      bool *module);

/* Source-local Python bindings, independent of dependency edge endpoints.
 * Dotted module imports retain the exact loaded prefix and block the root's
 * unimported descendants. Both arrays and their strings are owned by caller;
 * cbm_pxc_free_import_map releases them. Allocation failure returns -1. */
int cbm_pipeline_python_import_map(const cbm_gbuf_t *gbuf, const char *project,
                                   const char *source_rel, const CBMFileResult *result,
                                   bool include_type_only, const char ***out_keys,
                                   const char ***out_vals, int *out_count);

/* ECMAScript named/namespace bindings use the source member and canonical
 * module identity, never the dependency File's extension-bearing QN. */
int cbm_pipeline_ecma_import_map(const cbm_gbuf_t *, const char *, const char *,
                                 const CBMFileResult *, bool, const char ***, const char ***,
                                 int *);

/* Positive PHP class bindings are independent of unresolved sibling imports.
 * Owned arrays/strings use the same release contract as Python bindings. */
int cbm_pipeline_php_import_map(const cbm_gbuf_t *graph, const char *project,
                                const char *source_rel, const CBMFileResult *result,
                                const char ***keys, const char ***values, int *count);

/* Source-audited Rust use bindings, separate from dependency File targets. */
int cbm_pipeline_rust_import_map(const cbm_gbuf_t *, const char *, const char *,
                                 const CBMFileResult *, const char ***, const char ***, int *);
#endif
