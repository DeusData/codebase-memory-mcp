#include "policy.h"

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

bool cbm_resolve_record_admit(const cbm_resolve_record_t *record) {
    if (!record || !record->file_path || !record->file_path[0] || !record->scope ||
        !record->scope[0] || !record->relation || !cbm_resolve_admit(&record->result))
        return false;
    if (strcmp(record->relation, "IMPORTS") == 0)
        return true;
    static const char *const relations[] = {"CALLS", "USAGE",  "CALL_REFERENCE",
                                            "READS", "WRITES", "USES_TYPE"};
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
