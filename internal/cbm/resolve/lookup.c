#include "lookup.h"
#include <string.h>

enum { LOOKUP_MAX_INPUTS = 2048 };

bool cbm_resolve_lookup_context_init(cbm_resolve_lookup_context_t *context,
                                     cbm_resolve_proof_pool_t *pool,
                                     cbm_resolve_proof_source_t source) {
    if (!context || !pool)
        return false;
    *context = (cbm_resolve_lookup_context_t){.pool = pool, .source = source};
    bool known = source.content_identity && source.content_identity[0] &&
                 ((source.file && source.file[0]) || (source.module && source.module[0]));
    cbm_resolve_result_t result = {.analysis = known ? CBM_RESOLVE_ANALYSIS_COMPLETE
                                                     : CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                   .reason = "captured_extraction_source_identity"};
    cbm_resolve_proof_key_t key = {.detail = "captured_extraction_source_identity"};
    context->source_fact =
        cbm_resolve_proof_leaf(pool, CBM_PROOF_SOURCE_FACT, &source, &key, &result);
    if (context->source_fact)
        context->source = cbm_resolve_proof_view(context->source_fact)->source;
    return context->source_fact != NULL;
}

static bool add_input(cbm_resolve_lookup_t *query, const cbm_resolve_proof_t *proof) {
    if (!query || !query->context || !proof || query->count >= LOOKUP_MAX_INPUTS) {
        if (query)
            query->failed = true;
        return false;
    }
    if (query->count == query->capacity) {
        size_t capacity = query->capacity ? query->capacity * 2 : 8;
        const cbm_resolve_proof_t **inputs =
            cbm_arena_alloc(query->context->pool->arena, capacity * sizeof(*inputs));
        if (!inputs) {
            query->failed = true;
            return false;
        }
        if (query->count)
            memcpy(inputs, query->inputs, query->count * sizeof(*inputs));
        query->inputs = inputs;
        query->capacity = capacity;
    }
    query->inputs[query->count++] = proof;
    return true;
}

void cbm_resolve_lookup_begin(cbm_resolve_lookup_t *query, cbm_resolve_lookup_context_t *context,
                              const char *domain, const char *key, uint32_t start, uint32_t end,
                              uint32_t origin) {
    if (!query)
        return;
    *query = (cbm_resolve_lookup_t){.context = context,
                                    .key = {.lookup_domain = domain,
                                            .lookup_key = key,
                                            .start_byte = start,
                                            .end_byte = end,
                                            .origin = origin},
                                    .closed = true};
    if (context)
        (void)add_input(query, context->source_fact);
}

bool cbm_resolve_lookup_observe(cbm_resolve_lookup_t *query, const char *domain, const char *key,
                                const char *target, cbm_resolve_binding_t binding,
                                cbm_resolve_analysis_t analysis, uint32_t flags, bool closed,
                                const char *detail) {
    if (!query || !query->context)
        return true; /* Existing non-observed callers retain their contract. */
    if (query->failed)
        return false;
    cbm_resolve_proof_key_t observation = query->key;
    observation.lookup_domain = domain;
    observation.lookup_key = key;
    observation.declaration_identity = target;
    observation.flags = flags;
    observation.lookup_closed = closed;
    observation.detail = detail;
    cbm_resolve_result_t outcome = {.analysis = analysis,
                                    .binding = binding,
                                    .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                    .target = target,
                                    .candidate_count = binding == CBM_RESOLVE_BINDING_AMBIGUOUS ? 2
                                                       : binding == CBM_RESOLVE_BINDING_UNIQUE  ? 1
                                                                                                : 0,
                                    .reason = detail};
    query->closed &= closed;
    const cbm_resolve_proof_t *proof =
        cbm_resolve_proof_leaf(query->context->pool, CBM_PROOF_ADAPTER_DOMAIN,
                               &query->context->source, &observation, &outcome);
    return add_input(query, proof);
}

bool cbm_resolve_lookup_support(cbm_resolve_lookup_t *query, const cbm_resolve_proof_t *proof) {
    if (!query || !query->context)
        return true;
    if (proof)
        query->closed &= cbm_resolve_proof_view(proof)->predecessors_closed;
    return add_input(query, proof);
}

const cbm_resolve_proof_t *cbm_resolve_lookup_finish(cbm_resolve_lookup_t *query,
                                                     cbm_resolve_proof_kind_t kind,
                                                     cbm_resolve_result_t outcome) {
    if (!query || !query->context || query->failed || query->count < 2)
        return NULL;
    outcome.proof = NULL;
    return cbm_resolve_proof_derive(query->context->pool, kind, &query->context->source,
                                    &query->key, &outcome, query->inputs, query->count,
                                    query->closed);
}
