#ifndef CBM_RESOLVE_LOOKUP_H
#define CBM_RESOLVE_LOOKUP_H

#include "proof.h"

/* Explicit per-file context; never stored in a shared registry or TLS. Source
 * identity comes from extraction. Missing identity remains UNKNOWN. Domain
 * observations describe the exact map/path consulted, not corpus coverage. */
typedef struct {
    cbm_resolve_proof_pool_t *pool;
    cbm_resolve_proof_source_t source;
    const cbm_resolve_proof_t *source_fact;
} cbm_resolve_lookup_context_t;

typedef struct {
    cbm_resolve_lookup_context_t *context;
    cbm_resolve_proof_key_t key;
    const cbm_resolve_proof_t **inputs;
    size_t count, capacity;
    bool closed;
    bool failed;
} cbm_resolve_lookup_t;

bool cbm_resolve_lookup_context_init(cbm_resolve_lookup_context_t *, cbm_resolve_proof_pool_t *,
                                     cbm_resolve_proof_source_t source);
void cbm_resolve_lookup_begin(cbm_resolve_lookup_t *, cbm_resolve_lookup_context_t *,
                              const char *domain, const char *key, uint32_t start, uint32_t end,
                              uint32_t origin);
/* Record immediately after the actual lookup/audit, including an observed
 * absence. COMPLETE describes that bounded operation, not whole-project
 * semantic completeness. Unknown search domains must use UNKNOWN/open. */
bool cbm_resolve_lookup_observe(cbm_resolve_lookup_t *, const char *domain, const char *key,
                                const char *target, cbm_resolve_binding_t binding,
                                cbm_resolve_analysis_t analysis, uint32_t flags, bool closed,
                                const char *detail);
/* Retain an audited dependency. Same-pool inputs are reused without copying.
 * Callers select the applicable rule inputs before adding support. */
bool cbm_resolve_lookup_support(cbm_resolve_lookup_t *, const cbm_resolve_proof_t *);
/* Semantic intermediate outcome supplied by the rule, before admission.
 * This never manufactures flags or upgrades unknown/open premises. */
const cbm_resolve_proof_t *cbm_resolve_lookup_finish(cbm_resolve_lookup_t *,
                                                     cbm_resolve_proof_kind_t,
                                                     cbm_resolve_result_t outcome);
#endif
