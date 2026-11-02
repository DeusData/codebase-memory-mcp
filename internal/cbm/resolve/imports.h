#ifndef CBM_RESOLVE_IMPORTS_H
#define CBM_RESOLVE_IMPORTS_H

#include "proof.h"
#include <string.h>

/* Normalized source facts and path-resolution evidence, prior to graph
 * projection. A dependency target is not an exported-member binding. Adapters
 * must prove that separate step; unresolved entries still shadow local names. */
typedef struct {
    const char *local_name;
    const char *module_path;
    const char *member;
    uint32_t kind;
    bool is_default, type_checking_only;
    uint32_t start_byte, end_byte, source_origin;
    cbm_resolve_result_t dependency;
    /* Exact exported member or declaration namespace, with dependency proof. */
    cbm_resolve_result_t binding;
    bool closed;
} cbm_resolve_import_fact_t;

typedef struct {
    cbm_resolve_proof_source_t source;
    const cbm_resolve_import_fact_t *items;
    size_t count;
    bool complete;
} cbm_resolve_import_snapshot_t;

/* Scope differs between dependency and expression proofs; captured source
 * identities must remain identical when an adapter consumes either. */
static inline bool cbm_resolve_import_source_matches(const cbm_resolve_import_snapshot_t *snapshot,
                                                     const cbm_resolve_proof_source_t *source) {
    if (!snapshot || !snapshot->complete || !source ||
        snapshot->source.source_origin != source->source_origin)
        return false;
    const char *captured[] = {snapshot->source.project, snapshot->source.file,
                              snapshot->source.module, snapshot->source.content_identity,
                              snapshot->source.configuration_identity};
    const char *current[] = {source->project, source->file, source->module,
                             source->content_identity, source->configuration_identity};
    for (size_t i = 0; i < sizeof(captured) / sizeof(captured[0]); i++)
        if (!captured[i] || !current[i] || strcmp(captured[i], current[i]))
            return false;
    return true;
}

#endif
