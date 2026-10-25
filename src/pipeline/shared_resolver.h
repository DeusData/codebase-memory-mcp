#ifndef CBM_PIPELINE_SHARED_RESOLVER_H
#define CBM_PIPELINE_SHARED_RESOLVER_H

#include "pipeline_internal.h"
#include "resolve/solver.h"

typedef struct cbm_shared_resolver cbm_shared_resolver_t;

/* Immutable after build, shared by every file worker. Cache access includes
 * spilled results; exported binding proofs are computed once per file. */
cbm_shared_resolver_t *cbm_shared_resolver_build(const cbm_pipeline_ctx_t *ctx,
                                                 const cbm_file_info_t *files, int file_count,
                                                 CBMFileResult **cache);
void cbm_shared_resolver_free(cbm_shared_resolver_t *resolver);

typedef struct {
    const char *file;
    cbm_resolve_solver_result_t binding;
} cbm_shared_resolver_export_summary_t;

/* Canonically sorted real exported bindings, excluding private import/query
 * facts. Borrowed strings and proof roots live until shared resolver teardown.
 * Persist in a generation-owned proof pool; never clone into each file arena. */
size_t cbm_shared_resolver_export_count(const cbm_shared_resolver_t *resolver);
const cbm_shared_resolver_export_summary_t *cbm_shared_resolver_export_at(
    const cbm_shared_resolver_t *resolver, size_t index);
bool cbm_shared_resolver_exports_complete(const cbm_shared_resolver_t *resolver);
bool cbm_shared_resolver_owns_proof(const cbm_shared_resolver_t *resolver,
                                    const cbm_resolve_proof_t *proof);

typedef struct {
    const cbm_pipeline_ctx_t *ctx;
    const cbm_registry_t *registry;
    const cbm_gbuf_t *graph;
    cbm_gbuf_t *output;
    const CBMFileResult *result;
    const char *rel_path;
    const char *module_qn;
    CBMLanguage language;
    const char **import_keys;
    const char **import_values;
    int import_count;
    /* Optional raw source from the extraction generation. The orchestration
     * verifies the manifest before publishing; an absent buffer is read once. */
    const char *source;
    size_t source_len;
} cbm_resolver_file_t;

/* Records every extracted in-scope occurrence, including failures and
 * unsupported syntax, and emits only admitted exact targets. False means an
 * allocation/record/edge-write failed; the generation must not be published. */
bool cbm_shared_resolver_file(const cbm_shared_resolver_t *resolver,
                              const cbm_resolver_file_t *file);

/* The only projection authority for CALLS/USAGE/CALL_REFERENCE/READS/WRITES/
 * USES_TYPE/IMPORTS. Removes legacy bypasses and rebuilds in deterministic
 * occurrence order. Invalid emitted claims fail closed. */
bool cbm_shared_resolver_project(cbm_gbuf_t *graph);

#endif
