#ifndef CBM_RESOLVE_MODEL_H
#define CBM_RESOLVE_MODEL_H

#include <stdint.h>

/* Bump when binding or admission semantics change; persisted resolver facts
 * from a different version must be rebuilt, never silently reclassified. */
#define CBM_RESOLVER_RULES_VERSION 2

typedef enum {
    CBM_RESOLVE_BINDING_UNRESOLVED = 0,
    CBM_RESOLVE_BINDING_UNIQUE,
    CBM_RESOLVE_BINDING_AMBIGUOUS
} cbm_resolve_binding_t;

typedef enum {
    CBM_RESOLVE_DOMAIN_UNKNOWN = 0,
    CBM_RESOLVE_DOMAIN_PROJECT,
    CBM_RESOLVE_DOMAIN_EXTERNAL
} cbm_resolve_domain_t;

typedef enum {
    CBM_RESOLVE_ANALYSIS_UNSUPPORTED = 0,
    CBM_RESOLVE_ANALYSIS_COMPLETE,
    CBM_RESOLVE_ANALYSIS_LIMITED,
    CBM_RESOLVE_ANALYSIS_FAILED,
    CBM_RESOLVE_ANALYSIS_PENDING,
    CBM_RESOLVE_ANALYSIS_UNKNOWN
} cbm_resolve_analysis_t;

typedef enum {
    CBM_RESOLVE_RULE_NONE = 0,
    CBM_RESOLVE_RULE_SCOPE_BINDING,
    CBM_RESOLVE_RULE_IMPORT_BINDING,
    CBM_RESOLVE_RULE_TYPE_MEMBER,
    CBM_RESOLVE_RULE_CALLABLE_BINDING,
    CBM_RESOLVE_RULE_DECLARED_TYPE,
    CBM_RESOLVE_RULE_MODULE_PATH,
    CBM_RESOLVE_RULE_LANGUAGE_BINDING
} cbm_resolve_rule_t;

/* Facts are established by an adapter's actual lookup. A confidence score,
 * strategy string, unique short name, or a guessed type provides none of
 * them. LOOKUP_COMPLETE means the language lookup domain was complete, not
 * merely that every currently indexed candidate was examined. */
enum {
    CBM_RESOLVE_FACT_EXACT_TARGET = 1u << 0,
    CBM_RESOLVE_FACT_LOOKUP_COMPLETE = 1u << 1,
    CBM_RESOLVE_FACT_SCOPE_BOUND = 1u << 2,
    CBM_RESOLVE_FACT_IMPORT_BOUND = 1u << 3,
    CBM_RESOLVE_FACT_RECEIVER_TYPED = 1u << 4,
    CBM_RESOLVE_FACT_CALLABLE_TYPED = 1u << 5,
    CBM_RESOLVE_FACT_TYPE_BOUND = 1u << 6,
    CBM_RESOLVE_FACT_PATH_BOUND = 1u << 7,
    CBM_RESOLVE_FACT_LANGUAGE_BOUND = 1u << 8,
    CBM_RESOLVE_FACT_OCCURRENCE_EXACT = 1u << 9,
    /* The statically bound value is a class declaration, not a guessed __init__. */
    CBM_RESOLVE_FACT_CLASS_CALLABLE = 1u << 10
};

typedef struct cbm_resolve_proof cbm_resolve_proof_t;

/* Evidence carried by language adapters. The occurrence join
 * establishes unique binding and the graph projection supplies the target. */
typedef struct {
    cbm_resolve_analysis_t analysis;
    cbm_resolve_rule_t rule;
    uint32_t facts;
    const cbm_resolve_proof_t *proof;
} cbm_resolve_evidence_t;

/* Borrowed strings; the provider owns their lifetime. Outcomes also exist
 * when there is no graph edge: abstention prevents a correctness bug but
 * remains a quality gap until a genuine reference is resolved. */
typedef struct {
    cbm_resolve_binding_t binding;
    cbm_resolve_domain_t domain;
    cbm_resolve_analysis_t analysis;
    cbm_resolve_rule_t rule;
    uint32_t facts;
    const char *target;
    uint32_t candidate_count;
    const char *reason;
    const cbm_resolve_proof_t *proof;
} cbm_resolve_result_t;

#endif
