#include "symbols.h"
#include "policy.h"
#include <limits.h>
#include <string.h>

#define SYMBOL_DEFAULT_WORK_LIMIT UINT64_C(16384)
#define SYMBOL_MAX_REGISTRY_LINKS 64

typedef struct {
    const cbm_resolve_method_query_t *query;
    cbm_resolve_method_result_t output;
    uint64_t limit;
    uint32_t compatible;
    uint32_t applicable;
    bool function;
    bool arguments_unknown;
    bool name_identity;
    bool incomplete;
    const char *incomplete_reason;
    const CBMTypeRegistry *chosen_owner;
    const CBMRegisteredFunc *chosen;
    cbm_resolve_method_candidate_t chosen_proof;
} symbol_lookup_t;

static bool symbol_fail(symbol_lookup_t *lookup, cbm_resolve_analysis_t analysis,
                        const char *reason) {
    lookup->output.result.analysis = analysis;
    lookup->output.result.reason = reason;
    return false;
}

static bool symbol_step(symbol_lookup_t *lookup) {
    if (lookup->output.work >= lookup->limit) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_LIMITED, "symbol_work_limit");
    }
    lookup->output.work++;
    return true;
}

static void symbol_incomplete(symbol_lookup_t *lookup, const char *reason) {
    lookup->incomplete = true;
    /* A stable reason, independent of hash-chain or registration order. */
    if (!lookup->incomplete_reason || strcmp(reason, lookup->incomplete_reason) < 0) {
        lookup->incomplete_reason = reason;
    }
}

static bool method_receiver_proven(symbol_lookup_t *lookup) {
    const cbm_resolve_method_query_t *query = lookup->query;
    const cbm_resolve_type_fact_t *receiver = query->receiver;
    if (!receiver || receiver->status != CBM_RESOLVE_TYPE_KNOWN || !receiver->type ||
        receiver->type->kind != CBM_TYPE_NAMED || !query->receiver_qn || !query->receiver_qn[0] ||
        !query->method_name || !query->method_name[0] ||
        !receiver->type->data.named.qualified_name ||
        strcmp(receiver->type->data.named.qualified_name, query->receiver_qn) != 0 ||
        !cbm_resolve_admit(&receiver->evidence) ||
        !(receiver->evidence.facts & CBM_RESOLVE_FACT_TYPE_BOUND)) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "method_receiver_unproven");
    }
    if (receiver->binding && receiver->binding->type != receiver->type) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "method_receiver_stale");
    }
    lookup->output.result.rule = CBM_RESOLVE_RULE_TYPE_MEMBER;
    return true;
}

static bool function_binding_proven(symbol_lookup_t *lookup) {
    const cbm_resolve_method_query_t *query = lookup->query;
    cbm_resolve_evidence_t binding = query->function_binding;
    uint32_t required = 0;
    switch (binding.rule) {
    case CBM_RESOLVE_RULE_SCOPE_BINDING:
        required = CBM_RESOLVE_FACT_SCOPE_BOUND;
        break;
    case CBM_RESOLVE_RULE_IMPORT_BINDING:
        required = CBM_RESOLVE_FACT_IMPORT_BOUND;
        break;
    case CBM_RESOLVE_RULE_CALLABLE_BINDING:
        required = CBM_RESOLVE_FACT_CALLABLE_TYPED;
        break;
    case CBM_RESOLVE_RULE_LANGUAGE_BINDING:
        required = CBM_RESOLVE_FACT_LANGUAGE_BOUND;
        break;
    default:
        break;
    }
    required |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    if (required == CBM_RESOLVE_FACT_OCCURRENCE_EXACT || !query->function_qn ||
        !query->function_qn[0] || binding.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        (binding.facts & required) != required) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "function_binding_unproven");
    }
    lookup->output.result.rule = binding.rule;
    return true;
}

static bool symbol_arguments(symbol_lookup_t *lookup) {
    const cbm_resolve_method_query_t *query = lookup->query;
    if (lookup->name_identity) {
        return true;
    }
    if (query->argument_count < 0) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "symbol_arity_unknown");
    }
    for (int i = 0; i < query->argument_count; i++) {
        if (!symbol_step(lookup)) {
            return false;
        }
        const CBMType *argument = query->argument_types ? query->argument_types[i] : NULL;
        /* Composite and inferred types need a language-specific applicability
         * proof; do not erase unknown arms through a generic normalizer. */
        if (!argument || (argument->kind != CBM_TYPE_NAMED && argument->kind != CBM_TYPE_BUILTIN)) {
            lookup->arguments_unknown = true;
        }
    }
    return true;
}

static bool symbol_query_proven(symbol_lookup_t *lookup) {
    const cbm_resolve_method_query_t *query = lookup->query;
    uint32_t required =
        CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE | CBM_RESOLVE_METHOD_OCCURRENCE_EXACT;
    if (!lookup->name_identity) {
        required |= CBM_RESOLVE_METHOD_FIXED_ARITY;
    }
    if (!query || !query->registry || !query->classify) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "symbol_query_incomplete");
    }
    if ((query->adapter_facts & required) != required) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED, "symbol_domain_unproven");
    }
    if (!(lookup->function ? function_binding_proven(lookup) : method_receiver_proven(lookup))) {
        return false;
    }
    lookup->output.result.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    return symbol_arguments(lookup);
}

static bool symbol_index_ready(symbol_lookup_t *lookup, const CBMTypeRegistry *registry) {
    if (registry->func_count < 0 || registry->func_qn_entry_count != registry->func_count) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                           "method_registry_not_finalized");
    }
    if (registry->func_count == 0) {
        return true;
    }
    int buckets = lookup->function ? registry->func_qn_bucket_count : registry->method_bucket_count;
    const int *heads = lookup->function ? registry->func_qn_buckets : registry->method_buckets;
    const CBMRegistryHashEntry *entries =
        lookup->function ? registry->func_qn_entries : registry->method_entries;
    int count = lookup->function ? registry->func_qn_entry_count : registry->method_entry_count;
    if (!registry->funcs || !heads || !entries || buckets <= 0 || (buckets & (buckets - 1)) ||
        count < 0 || count > registry->func_count) {
        return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                           "method_registry_not_finalized");
    }
    return true;
}

static bool symbol_arity(symbol_lookup_t *lookup, const CBMRegisteredFunc *candidate,
                         bool *matches) {
    *matches = lookup->name_identity;
    if (lookup->name_identity) {
        return true;
    }
    const CBMType *signature = candidate->signature;
    if (!signature || signature->kind != CBM_TYPE_FUNC || candidate->min_params < 0) {
        symbol_incomplete(lookup, "symbol_signature_incomplete");
        return true;
    }
    int count = 0;
    const CBMType *const *params = signature->data.func.param_types;
    while (params && params[count]) {
        if (!symbol_step(lookup)) {
            return false;
        }
        if (count == INT_MAX) {
            return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_LIMITED, "symbol_signature_limit");
        }
        count++;
    }
    if (candidate->min_params > count) {
        symbol_incomplete(lookup, "symbol_signature_incomplete");
        return true;
    }
    *matches = lookup->query->argument_count >= candidate->min_params &&
               lookup->query->argument_count <= count;
    return true;
}

static bool symbol_target_exact(const CBMRegisteredFunc *candidate,
                                const cbm_resolve_method_candidate_t *proof) {
    if (!proof->graph_target_exact || !proof->graph_target || !candidate->qualified_name ||
        !candidate->qualified_name[0]) {
        return false;
    }
    if (candidate->graph_qualified_name) {
        return candidate->graph_qualified_name[0] &&
               strcmp(proof->graph_target, candidate->graph_qualified_name) == 0;
    }
    size_t prefix = strlen(candidate->qualified_name);
    const char *suffix = candidate->sig ? candidate->sig : "";
    size_t suffix_size = strlen(suffix);
    size_t target_size = strlen(proof->graph_target);
    return target_size >= prefix && target_size - prefix == suffix_size &&
           memcmp(proof->graph_target, candidate->qualified_name, prefix) == 0 &&
           strcmp(proof->graph_target + prefix, suffix) == 0;
}

static bool symbol_same_declaration(const symbol_lookup_t *lookup,
                                    const cbm_resolve_method_candidate_t *proof) {
    const cbm_resolve_method_candidate_t *chosen = &lookup->chosen_proof;
    return lookup->chosen && chosen->declaration_identity_exact &&
           proof->declaration_identity_exact && chosen->declaration_identity &&
           chosen->declaration_identity[0] && proof->declaration_identity &&
           strcmp(chosen->declaration_identity, proof->declaration_identity) == 0 &&
           chosen->domain == proof->domain &&
           strcmp(chosen->graph_target, proof->graph_target) == 0;
}

static bool symbol_candidate(symbol_lookup_t *lookup, const CBMTypeRegistry *owner,
                             const CBMRegisteredFunc *candidate) {
    cbm_resolve_method_candidate_t proof =
        lookup->query->classify(lookup->query, owner, candidate, lookup->query->context);
    if (proof.exposed == CBM_RESOLVE_METHOD_NO) {
        return true;
    }
    if (proof.exposed != CBM_RESOLVE_METHOD_YES) {
        symbol_incomplete(lookup, "symbol_exposure_unknown");
        return true;
    }
    bool matches = false;
    if (!symbol_arity(lookup, candidate, &matches)) {
        return false;
    }
    if (!matches) {
        return true;
    }
    if (lookup->compatible < UINT32_MAX) {
        lookup->compatible++;
    }
    if (candidate->flags & CBM_FUNC_FLAG_AMBIGUOUS_BINDING) {
        symbol_incomplete(lookup, "symbol_declaration_ambiguous");
        return true;
    }
    if (proof.applicable == CBM_RESOLVE_METHOD_NO && !lookup->name_identity) {
        return true;
    }
    if (proof.applicable != CBM_RESOLVE_METHOD_YES) {
        symbol_incomplete(lookup, "symbol_applicability_unknown");
        return true;
    }
    if (proof.domain != CBM_RESOLVE_DOMAIN_PROJECT && proof.domain != CBM_RESOLVE_DOMAIN_EXTERNAL) {
        symbol_incomplete(lookup, "symbol_target_domain_unknown");
        return true;
    }
    if (!symbol_target_exact(candidate, &proof)) {
        symbol_incomplete(lookup, "method_graph_identity_unproven");
        return true;
    }
    if (symbol_same_declaration(lookup, &proof)) {
        lookup->compatible--;
        return true;
    }
    if (lookup->applicable < UINT32_MAX) {
        lookup->applicable++;
    }
    if (!lookup->chosen) {
        lookup->chosen = candidate;
        lookup->chosen_owner = owner;
        lookup->chosen_proof = proof;
    }
    return true;
}

static uint64_t symbol_qn_hash(const char *name) {
    uint64_t hash = UINT64_C(0xcbf29ce484222325);
    while (*name) {
        hash ^= (unsigned char)*name++;
        hash *= UINT64_C(0x100000001b3);
    }
    return hash;
}

static bool symbol_key_matches(const symbol_lookup_t *lookup, const CBMRegisteredFunc *candidate) {
    const cbm_resolve_method_query_t *query = lookup->query;
    if (lookup->function) {
        return !candidate->receiver_type && candidate->qualified_name &&
               strcmp(candidate->qualified_name, query->function_qn) == 0;
    }
    return candidate->receiver_type && candidate->short_name &&
           strcmp(candidate->receiver_type, query->receiver_qn) == 0 &&
           strcmp(candidate->short_name, query->method_name) == 0;
}

static bool symbol_registry(symbol_lookup_t *lookup, const CBMTypeRegistry *registry) {
    if (!symbol_index_ready(lookup, registry)) {
        return false;
    }
    if (registry->func_count == 0) {
        return true;
    }
    uint64_t hash;
    int index;
    int entry_count;
    const CBMRegistryHashEntry *entries;
    if (lookup->function) {
        hash = symbol_qn_hash(lookup->query->function_qn);
        index = registry->func_qn_buckets[hash & (uint64_t)(registry->func_qn_bucket_count - 1)];
        entries = registry->func_qn_entries;
        entry_count = registry->func_qn_entry_count;
    } else {
        CBMMethodIter iterator;
        cbm_registry_methods(registry, lookup->query->receiver_qn, lookup->query->method_name,
                             &iterator);
        hash = iterator.hash;
        index = iterator.chain_idx;
        entries = registry->method_entries;
        entry_count = registry->method_entry_count;
    }
    /* Walk the finalized bucket directly. Legacy iterators may fall back to an
     * unindexed tail and silently shadow fallback declarations; neither is a
     * complete binding proof. Charge collisions as well as matching entries. */
    for (int visits = 0; index != -1; visits++) {
        if (!symbol_step(lookup)) {
            return false;
        }
        if (index < 0 || index >= entry_count || visits >= entry_count) {
            return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_FAILED,
                               "symbol_registry_index_invalid");
        }
        const CBMRegistryHashEntry *entry = &entries[index];
        if (entry->payload_index < 0 || entry->payload_index >= registry->func_count) {
            return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_FAILED,
                               "symbol_registry_index_invalid");
        }
        const CBMRegisteredFunc *candidate = &registry->funcs[entry->payload_index];
        if (entry->hash == hash && symbol_key_matches(lookup, candidate) &&
            !symbol_candidate(lookup, registry, candidate)) {
            return false;
        }
        index = entry->next_index;
    }
    return true;
}

static bool symbol_registries(symbol_lookup_t *lookup) {
    const CBMTypeRegistry *seen[SYMBOL_MAX_REGISTRY_LINKS];
    size_t count = 0;
    for (const CBMTypeRegistry *registry = lookup->query->registry; registry;
         registry = registry->fallback) {
        if (!symbol_step(lookup)) {
            return false;
        }
        for (size_t i = 0; i < count; i++) {
            if (seen[i] == registry) {
                return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_FAILED, "method_registry_cycle");
            }
        }
        if (count == SYMBOL_MAX_REGISTRY_LINKS) {
            return symbol_fail(lookup, CBM_RESOLVE_ANALYSIS_LIMITED, "symbol_registry_depth_limit");
        }
        seen[count++] = registry;
        if (!symbol_registry(lookup, registry)) {
            return false;
        }
    }
    return true;
}

static cbm_resolve_method_result_t symbol_finish(symbol_lookup_t *lookup) {
    cbm_resolve_result_t *result = &lookup->output.result;
    result->candidate_count = lookup->applicable;
    if (lookup->name_identity && lookup->compatible > 1) {
        result->binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        result->candidate_count = lookup->compatible;
        result->analysis =
            lookup->incomplete ? CBM_RESOLVE_ANALYSIS_UNSUPPORTED : CBM_RESOLVE_ANALYSIS_COMPLETE;
        result->reason = "symbol_name_multiple_declarations";
    } else if (lookup->arguments_unknown && lookup->compatible > 1) {
        result->binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        result->candidate_count = lookup->compatible;
        result->analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
        result->reason = "method_overload_arguments_unknown";
    } else if (lookup->incomplete) {
        result->analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
        result->reason = lookup->incomplete_reason;
    } else if (lookup->applicable > 1) {
        result->binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        result->analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        result->reason = "symbol_multiple_applicable_declarations";
    } else if (lookup->applicable == 0) {
        result->analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        result->reason = "symbol_no_applicable_declaration";
    } else {
        result->binding = CBM_RESOLVE_BINDING_UNIQUE;
        result->domain = lookup->chosen_proof.domain;
        result->analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
        result->facts =
            cbm_resolve_rule_required_facts(result->rule) | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
        result->target = lookup->chosen_proof.graph_target;
        result->reason =
            lookup->function ? "exact_function_binding" : "exact_direct_method_binding";
        if (cbm_resolve_admit(result)) {
            lookup->output.owner = lookup->chosen_owner;
            lookup->output.method = lookup->chosen;
        }
    }
    return lookup->output;
}

static cbm_resolve_method_result_t symbol_resolve(const cbm_resolve_method_query_t *query,
                                                  bool function) {
    symbol_lookup_t lookup = {
        .query = query,
        .limit = query && query->work_limit ? query->work_limit : SYMBOL_DEFAULT_WORK_LIMIT,
        .function = function,
        .name_identity = query && (query->adapter_facts & CBM_RESOLVE_METHOD_NAME_IDENTITY)};
    if (!symbol_query_proven(&lookup) || !symbol_registries(&lookup)) {
        return lookup.output;
    }
    return symbol_finish(&lookup);
}

cbm_resolve_method_result_t cbm_resolve_method(const cbm_resolve_method_query_t *query) {
    return symbol_resolve(query, false);
}

cbm_resolve_function_result_t cbm_resolve_function(const cbm_resolve_function_query_t *query) {
    return symbol_resolve(query, true);
}
