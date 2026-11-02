#ifndef CBM_RESOLVE_SYMBOLS_H
#define CBM_RESOLVE_SYMBOLS_H

#include "types.h"
#include "../lsp/type_registry.h"

/* The adapter explicitly establishes these for this occurrence. They are not
 * inferred from a finalized registry, file language, score, or strategy name.
 * DIRECT_DOMAIN_COMPLETE excludes unvisited inherited/extension/dynamic lookup
 * branches. FIXED_ARITY means signature arrays exclude implicit receivers and
 * have no unmodelled variadic/spread/keyword arguments. */
enum {
    CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE = 1u << 0,
    CBM_RESOLVE_METHOD_FIXED_ARITY = 1u << 1,
    CBM_RESOLVE_METHOD_OCCURRENCE_EXACT = 1u << 2,
    /* Resolve a callable name/reference without asserting call applicability.
     * No arity is invented for variadics or reference sites. Every exposed
     * declaration participates; argument filtering cannot choose an overload.
     * FIXED_ARITY and argument_count are ignored for this mode. */
    CBM_RESOLVE_METHOD_NAME_IDENTITY = 1u << 3
};

typedef enum {
    CBM_RESOLVE_METHOD_UNKNOWN = 0,
    CBM_RESOLVE_METHOD_NO,
    CBM_RESOLVE_METHOD_YES
} cbm_resolve_method_verdict_t;

typedef struct {
    /* Visibility/overlay/inheritance exposure is a language decision. NO
     * excludes this declaration; UNKNOWN prevents a unique proof. */
    cbm_resolve_method_verdict_t exposed;
    /* Apply language conversions/constraints here, never score or rank. NO
     * must prove inapplicability. YES proves applicability, UNKNOWN withholds.
     * Unknown argument types cannot select among competing visible overloads. */
    cbm_resolve_method_verdict_t applicable;
    /* Registry membership alone does not establish project ownership. */
    cbm_resolve_domain_t domain;
    /* Exact canonical declaration identity, including a signature suffix.
     * Must remain valid for the returned result's lifetime. The helper checks
     * it matches this registry entry. Graph projection separately verifies
     * that this exact node is materialized before admitting an edge. */
    const char *graph_target;
    bool graph_target_exact;
    /* Optional exact declaration identity supplied by the language adapter,
     * including its canonical signature. Only equal proved identities with
     * equal graph targets may coalesce declaration/definition entries. A QN
     * or score alone must never populate this field. */
    const char *declaration_identity;
    bool declaration_identity_exact;
} cbm_resolve_method_candidate_t;

struct cbm_resolve_method_query;
typedef cbm_resolve_method_candidate_t (*cbm_resolve_method_classify_fn)(
    const struct cbm_resolve_method_query *query, const CBMTypeRegistry *owner,
    const CBMRegisteredFunc *candidate, void *context);

typedef struct cbm_resolve_method_query {
    const CBMTypeRegistry *registry;
    const cbm_resolve_type_fact_t *receiver;
    const char *receiver_qn;
    const char *method_name;
    /* Used only by cbm_resolve_function: exact scope/import-bound name domain.
     * This proves where the name binds, not which overload is selected. The
     * helper separately proves uniqueness and materialized graph identity. */
    const char *function_qn;
    cbm_resolve_evidence_t function_binding;
    const CBMType *const *argument_types; /* NULL/UNKNOWN entries are unknown */
    int argument_count;                   /* -1 means unknown; ignored in NAME_IDENTITY mode */
    uint32_t adapter_facts;
    uint64_t work_limit; /* zero selects a finite default */
    cbm_resolve_method_classify_fn classify;
    void *context;
    /* Per-analysis arena, never a shared registry arena. Method queries may
     * use receiver->proof_pool when this is NULL. Immutable standalone
     * receiver/function facts must provide a pool explicitly. */
    cbm_resolve_proof_pool_t *proof_pool;
    const cbm_resolve_proof_source_t *proof_source;
    const char *lookup_domain;
} cbm_resolve_method_query_t;

typedef struct {
    cbm_resolve_result_t result;
    const CBMTypeRegistry *owner;
    union {
        const CBMRegisteredFunc *method;
        const CBMRegisteredFunc *function;
    };
    uint64_t work;
} cbm_resolve_method_result_t;

/* Read-only; no registry construction, global-name scan, first-match choice,
 * or implicit inheritance/alias/overlay preference. All registry links must
 * remain immutable during this call and cover their full finalized arrays.
 * The receiver is a fresh KNOWN type-map fact for the occurrence. Initially
 * only exact NAMED owners are supported; language-specific dereference and
 * generic substitution require their own checked adapter before this API.
 * The chosen borrowed method pointer is returned only with an admitted proof. */
cbm_resolve_method_result_t cbm_resolve_method(const cbm_resolve_method_query_t *query);

/* The same callback and argument/adapter facts apply to exact free functions.
 * receiver/receiver_qn/method_name are ignored. function_binding must prove
 * COMPLETE scope, import, callable or language name-domain binding with the
 * corresponding BOUND fact and OCCURRENCE_EXACT. EXACT_TARGET/LOOKUP_COMPLETE
 * are outputs of this lookup, not required in the input binding evidence.
 * Function buckets use the entire function_qn; methods and unindexed registry
 * tails never enter this domain. The adapter callback must do bounded work;
 * the returned work count measures registry links, bucket entries and params. */
typedef cbm_resolve_method_query_t cbm_resolve_function_query_t;
typedef cbm_resolve_method_result_t cbm_resolve_function_result_t;
cbm_resolve_function_result_t cbm_resolve_function(const cbm_resolve_function_query_t *query);

#endif
