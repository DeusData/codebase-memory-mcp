#ifndef CBM_LSP_PROOF_BRIDGE_H
#define CBM_LSP_PROOF_BRIDGE_H

#include "../resolve/snapshot.h"
#include "../resolve/types.h"
#include <string.h>

/* Adapter helpers record inputs at the audited lookup, never at final graph
 * admission. Unknown captured identities stay unknown on every source node. */
static inline bool cbm_lsp_proof_context(CBMResolveTypeMap *types,
                                         const cbm_resolve_proof_source_t *source,
                                         const char *module) {
    cbm_resolve_proof_source_t unknown = {.module = module, .source_origin = CBM_SOURCE_ORIGIN_RAW};
    if (cbm_resolve_types_set_context(types, source ? source : &unknown))
        return true;
    if (types) {
        types->failure = CBM_RESOLVE_TYPE_FAILED;
        types->proofs.failure = CBM_RESOLVE_ANALYSIS_FAILED;
    }
    return false;
}

/* Standalone entrypoints know the immutable input bytes and module, but not a
 * project file or extraction configuration. Preserve those unknowns rather
 * than manufacturing publication authority. Result-aware callers replace this
 * context with their captured snapshot before creating any proof nodes. */
static inline bool cbm_lsp_proof_standalone_context(CBMResolveTypeMap *types, const char *source,
                                                    int source_len, const char *module,
                                                    CBMSourceOrigin origin) {
    if (!source || source_len < 0 || !module || !module[0] || origin < CBM_SOURCE_ORIGIN_RAW ||
        origin > CBM_SOURCE_ORIGIN_PREPROCESSED)
        return cbm_lsp_proof_context(types, NULL, module);
    cbm_sha256_ctx hash;
    char identity[65];
    cbm_sha256_init(&hash);
    cbm_resolve_hash_text(&hash, "standalone-lsp-source-v1");
    cbm_resolve_hash_text(&hash, module);
    cbm_resolve_hash_number(&hash, (uint32_t)origin);
    cbm_resolve_hash_number(&hash, (uint64_t)source_len);
    cbm_sha256_update(&hash, source, (size_t)source_len);
    cbm_resolve_hash_finish(&hash, identity);
    cbm_resolve_proof_source_t captured = {
        .module = module, .content_identity = identity, .source_origin = (uint32_t)origin};
    /* The setter owns the digest before this stack buffer expires. */
    return cbm_lsp_proof_context(types, &captured, module);
}

/* Called only after the language has checked the complete name domain and
 * retained one exact declaration. The three premises keep the source spelling,
 * the language's audited domain decision and the registry identity separate. */
static inline cbm_resolve_result_t cbm_lsp_proof_type_lookup(
    CBMResolveTypeMap *types, const char *spelling, TSNode site, const char *domain,
    const char *qualified_name, const char *declaration_identity, const char *graph_target,
    uint32_t source_flags, const char *decision) {
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                   .reason = "adapter_type_identity_missing"};
    if (!types || !spelling || !domain || !qualified_name || !declaration_identity ||
        !graph_target || ts_node_is_null(site))
        return result;
    cbm_resolve_proof_key_t key = {.lookup_domain = domain,
                                   .lookup_key = spelling,
                                   .type_identity = qualified_name,
                                   .start_byte = ts_node_start_byte(site),
                                   .end_byte = ts_node_end_byte(site),
                                   .flags = source_flags,
                                   .detail = "type_lookup_occurrence"};
    cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .reason = "type_lookup_occurrence"};
    const cbm_resolve_proof_t *inputs[3];
    inputs[0] = cbm_resolve_proof_leaf(&types->proofs, CBM_PROOF_SOURCE_FACT, &types->proof_source,
                                       &key, &audit);
    key.detail = decision;
    audit.reason = decision;
    inputs[1] = cbm_resolve_proof_leaf(&types->proofs, CBM_PROOF_ADAPTER_DOMAIN,
                                       &types->proof_source, &key, &audit);
    key.declaration_identity = declaration_identity;
    key.binding_identity = graph_target;
    key.detail = "selected_type_declaration";
    audit.binding = CBM_RESOLVE_BINDING_UNIQUE;
    audit.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    audit.target = graph_target;
    audit.facts = CBM_RESOLVE_FACT_EXACT_TARGET;
    audit.candidate_count = 1;
    inputs[2] = cbm_resolve_proof_leaf(&types->proofs, CBM_PROOF_REGISTERED_DECLARATION, NULL, &key,
                                       &audit);
    result = (cbm_resolve_result_t){
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_TYPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = graph_target,
        .candidate_count = 1,
        .reason = decision};
    key.detail = "complete_adapter_type_lookup";
    result.proof = cbm_resolve_proof_derive(&types->proofs, CBM_PROOF_REGISTRY_LOOKUP,
                                            &types->proof_source, &key, &result, inputs, 3, true);
    if (!result.proof || result.proof->value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
        result.analysis = types->proofs.failure == CBM_RESOLVE_ANALYSIS_COMPLETE
                              ? CBM_RESOLVE_ANALYSIS_UNSUPPORTED
                              : types->proofs.failure;
        result.binding = CBM_RESOLVE_BINDING_UNRESOLVED;
    }
    return result;
}

/* The adapter calls this at the checked rule application, with its real
 * predecessor and occurrence. The audit describes the additional language
 * decision; the predecessor continues to carry declaration/type authority. */
static inline cbm_resolve_result_t cbm_lsp_proof_rule(CBMResolveTypeMap *types,
                                                      cbm_resolve_result_t result, TSNode site,
                                                      const char *domain, const char *name,
                                                      const char *decision,
                                                      const cbm_resolve_proof_t *additional) {
    cbm_resolve_proof_key_t key = {.lookup_domain = domain,
                                   .lookup_key = name,
                                   .binding_identity = result.target,
                                   .start_byte =
                                       ts_node_is_null(site) ? 0 : ts_node_start_byte(site),
                                   .end_byte = ts_node_is_null(site) ? 0 : ts_node_end_byte(site),
                                   .detail = decision};
    cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE, .reason = decision};
    const cbm_resolve_proof_t *inputs[3] = {
        result.proof,
        cbm_resolve_proof_leaf(&types->proofs, CBM_PROOF_ADAPTER_DOMAIN, &types->proof_source, &key,
                               &audit),
        additional};
    result.proof =
        cbm_resolve_proof_derive(&types->proofs, CBM_PROOF_NAMED_JOIN, &types->proof_source, &key,
                                 &result, inputs, additional ? 3 : 2, true);
    if (!result.proof || result.proof->value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
        result.analysis = types->proofs.failure == CBM_RESOLVE_ANALYSIS_COMPLETE
                              ? CBM_RESOLVE_ANALYSIS_UNSUPPORTED
                              : types->proofs.failure;
        result.binding = CBM_RESOLVE_BINDING_UNRESOLVED;
    }
    return result;
}

/* The language producer invokes this only at its audited AST occurrence and
 * supplies the enclosing declaration and semantic role established there.
 * Reusable symbol/type/export roots alone never attest an occurrence. */
static inline cbm_resolve_result_t cbm_lsp_proof_occurrence(CBMResolveTypeMap *types,
                                                            cbm_resolve_result_t result,
                                                            TSNode site, const char *scope,
                                                            const char *spelling,
                                                            const char *relation) {
    const cbm_resolve_proof_view_t *semantic = cbm_resolve_proof_view(result.proof);
    if (!types || !semantic || ts_node_is_null(site) || !scope || !scope[0] || !spelling ||
        !spelling[0] || !relation || !relation[0] ||
        semantic->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        !semantic->predecessors_closed || semantic->outcome.binding != result.binding ||
        semantic->outcome.domain != result.domain || semantic->outcome.rule != result.rule ||
        semantic->outcome.candidate_count != result.candidate_count || !semantic->outcome.target ||
        !result.target || strcmp(semantic->outcome.target, result.target) ||
        (result.facts & ~CBM_RESOLVE_FACT_OCCURRENCE_EXACT & ~semantic->outcome.facts)) {
        result.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
        result.facts = 0;
        result.reason = "producer_occurrence_authority_missing";
        return result;
    }
    cbm_resolve_proof_source_t source = types->proof_source;
    source.scope = scope;
    cbm_resolve_proof_key_t key = {.lookup_domain = scope,
                                   .lookup_key = spelling,
                                   .binding_identity = result.target,
                                   .start_byte = ts_node_start_byte(site),
                                   .end_byte = ts_node_end_byte(site),
                                   .origin = source.source_origin,
                                   .detail = relation};
    cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .facts = CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                  .reason = "audited_ast_occurrence_role"};
    const cbm_resolve_proof_t *premises[] = {
        result.proof,
        cbm_resolve_proof_leaf(&types->proofs, CBM_PROOF_SOURCE_FACT, &source, &key, &audit)};
    result.facts |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    result.proof = cbm_resolve_proof_derive(&types->proofs, CBM_PROOF_NAMED_JOIN, &source, &key,
                                            &result, premises, 2, true);
    if (!result.proof || result.proof->value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE) {
        result.analysis = types->proofs.failure == CBM_RESOLVE_ANALYSIS_COMPLETE
                              ? CBM_RESOLVE_ANALYSIS_UNSUPPORTED
                              : types->proofs.failure;
        result.facts = 0;
    }
    return result;
}

static inline cbm_resolve_evidence_t cbm_lsp_proof_evidence(cbm_resolve_result_t result) {
    return (cbm_resolve_evidence_t){.analysis = result.analysis,
                                    .rule = result.rule,
                                    .facts = result.facts,
                                    .proof = result.proof};
}

static inline cbm_resolve_evidence_t cbm_lsp_proof_copy(cbm_resolve_proof_pool_t *pool,
                                                        cbm_resolve_evidence_t evidence) {
    if (evidence.proof) {
        evidence.proof = cbm_resolve_proof_clone(pool, evidence.proof);
        if (!evidence.proof)
            evidence.analysis = pool->failure == CBM_RESOLVE_ANALYSIS_COMPLETE
                                    ? CBM_RESOLVE_ANALYSIS_FAILED
                                    : pool->failure;
    }
    return evidence;
}

#endif
