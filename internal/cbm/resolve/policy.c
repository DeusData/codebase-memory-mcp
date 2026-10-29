#include "policy.h"
#include "proof.h"

#include <string.h>

uint32_t cbm_resolve_rule_required_facts(cbm_resolve_rule_t rule) {
    const uint32_t common = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE;
    switch (rule) {
    case CBM_RESOLVE_RULE_SCOPE_BINDING:
        return common | CBM_RESOLVE_FACT_SCOPE_BOUND;
    case CBM_RESOLVE_RULE_IMPORT_BINDING:
        return common | CBM_RESOLVE_FACT_IMPORT_BOUND;
    case CBM_RESOLVE_RULE_TYPE_MEMBER:
        return common | CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    case CBM_RESOLVE_RULE_CALLABLE_BINDING:
        return common | CBM_RESOLVE_FACT_CALLABLE_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    case CBM_RESOLVE_RULE_DECLARED_TYPE:
        return common | CBM_RESOLVE_FACT_TYPE_BOUND;
    case CBM_RESOLVE_RULE_MODULE_PATH:
        return common | CBM_RESOLVE_FACT_PATH_BOUND;
    case CBM_RESOLVE_RULE_LANGUAGE_BINDING:
        return common | CBM_RESOLVE_FACT_LANGUAGE_BOUND;
    case CBM_RESOLVE_RULE_NONE:
        return 0;
    }
    return 0;
}

bool cbm_resolve_admit(const cbm_resolve_result_t *result) {
    if (!result || result->binding != CBM_RESOLVE_BINDING_UNIQUE ||
        result->domain != CBM_RESOLVE_DOMAIN_PROJECT ||
        result->analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || result->candidate_count != 1 ||
        !result->target || !result->target[0]) {
        return false;
    }
    uint32_t required = cbm_resolve_rule_required_facts(result->rule);
    return required && (result->facts & required) == required;
}

bool cbm_resolve_record_candidate(const cbm_resolve_record_t *record) {
    if (!record || !record->file_path || !record->file_path[0] || !record->scope ||
        !record->scope[0] || !record->relation || record->source_origin > 1 ||
        !cbm_resolve_admit(&record->result))
        return false;
    const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(record->result.proof);
    if (!record->content_identity || !record->content_identity[0] ||
        !record->configuration_identity || !record->configuration_identity[0] || !proof ||
        proof->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        proof->outcome.binding != CBM_RESOLVE_BINDING_UNIQUE ||
        proof->outcome.domain != CBM_RESOLVE_DOMAIN_PROJECT ||
        proof->outcome.candidate_count != record->result.candidate_count ||
        !proof->predecessors_closed || !proof->outcome.target ||
        strcmp(proof->outcome.target, record->result.target))
        return false;
    static const char *const relations[] = {"IMPORTS", "CALLS",  "USAGE",    "CALL_REFERENCE",
                                            "READS",   "WRITES", "USES_TYPE"};
    bool known = false;
    for (size_t i = 0; i < sizeof(relations) / sizeof(relations[0]); i++) {
        if (strcmp(record->relation, relations[i]) == 0) {
            known = true;
            break;
        }
    }
    return known && record->end_byte > record->start_byte && record->source_origin <= 1 &&
           (record->result.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT) != 0;
}

static bool same_text(const char *left, const char *right) {
    return strcmp(left ? left : "", right ? right : "") == 0;
}

static bool occurrence_role_matches(const char *proved, const char *requested) {
    if (same_text(proved, requested))
        return true;
    /* A type mention or a field read/write is also a usage at that same site.
     * No converse conversion, invocation, or write authority is implied. */
    return same_text(requested, "USAGE") &&
           (same_text(proved, "USES_TYPE") || same_text(proved, "READS") ||
            same_text(proved, "WRITES"));
}

bool cbm_resolve_record_site_proof(const cbm_resolve_record_t *record,
                                   const cbm_resolve_proof_t *proof, const char *project) {
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    if (!record || !view || !project || !project[0] || !view->predecessors_closed ||
        view->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        view->outcome.binding != CBM_RESOLVE_BINDING_UNIQUE ||
        view->outcome.domain != CBM_RESOLVE_DOMAIN_PROJECT || view->outcome.candidate_count != 1 ||
        view->outcome.rule != record->result.rule ||
        !same_text(view->outcome.target, record->result.target))
        return false;
    return (view->outcome.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT) &&
           record->end_byte > record->start_byte && same_text(view->source.project, project) &&
           same_text(view->source.file, record->file_path) &&
           same_text(view->source.scope, record->scope) &&
           same_text(view->source.content_identity, record->content_identity) &&
           same_text(view->source.configuration_identity, record->configuration_identity) &&
           view->source.source_origin == record->source_origin &&
           view->key.origin == record->source_origin &&
           view->key.start_byte == record->start_byte && view->key.end_byte == record->end_byte &&
           same_text(view->key.lookup_key, record->spelling) &&
           same_text(view->key.binding_identity, record->result.target) &&
           occurrence_role_matches(view->key.detail, record->relation);
}

bool cbm_resolve_record_admit(const cbm_resolve_record_t *record) {
    if (!cbm_resolve_record_candidate(record) || record->proof_borrowed)
        return false;
    const cbm_resolve_proof_view_t *proof = cbm_resolve_proof_view(record->result.proof);
    bool site = false;
    uint32_t supported = 0;
    for (size_t i = 0; i < proof->premise_count; i++) {
        const cbm_resolve_proof_t *premise = proof->premises[i];
        supported |= premise->value.outcome.facts;
        site |= cbm_resolve_record_site_proof(record, premise, proof->source.project);
    }
    return site && (supported & record->result.facts) == record->result.facts &&
           proof->kind == CBM_PROOF_OCCURRENCE_BINDING && proof->premise_count > 0 &&
           same_text(proof->source.file, record->file_path) &&
           same_text(proof->source.scope, record->scope) &&
           same_text(proof->source.content_identity, record->content_identity) &&
           same_text(proof->source.configuration_identity, record->configuration_identity) &&
           proof->source.source_origin == record->source_origin &&
           proof->key.start_byte == record->start_byte && proof->key.end_byte == record->end_byte &&
           same_text(proof->key.lookup_domain, record->scope) &&
           same_text(proof->key.lookup_key, record->spelling) &&
           same_text(proof->key.detail, record->relation) &&
           proof->outcome.rule == record->result.rule &&
           proof->outcome.facts == record->result.facts &&
           proof->outcome.candidate_count == record->result.candidate_count;
}
