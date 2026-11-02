#include "test_framework.h"
#include "resolve/symbols.h"
#include "resolve/policy.h"
#include <string.h>

#define METHOD_FACTS                                                              \
    (CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE | CBM_RESOLVE_METHOD_FIXED_ARITY | \
     CBM_RESOLVE_METHOD_OCCURRENCE_EXACT)

typedef struct {
    CBMArena *arena;
    const CBMTypeRegistry *hidden_owner;
    bool compare_types;
    bool unknown_exposure;
    bool wrong_target;
    bool coalesce_identical_declarations;
    bool proof_initialized;
    cbm_resolve_proof_pool_t proofs;
    bool external;
    bool unknown_domain;
    int calls;
} method_adapter_t;

/* This suite constructs registry entries directly and has no parsed source.
 * Its context explicitly attests those standalone fixture inputs; it is never
 * presented as captured file content or admitted by generation persistence. */
static const cbm_resolve_proof_source_t symbol_fixture_source = {
    .module = "standalone.symbol.fixture",
    .scope = "standalone.symbol.fixture.query",
    .content_identity = "attestation:resolver-symbol-fixtures-v1",
    .configuration_identity = "standalone-registry-test-v1"};

static void method_adapter_proofs(method_adapter_t *adapter) {
    if (!adapter->proof_initialized) {
        cbm_resolve_proof_pool_init(&adapter->proofs, adapter->arena, 0, 0, 0);
        adapter->proof_initialized = true;
    }
}

static cbm_resolve_method_candidate_t classify_method(const cbm_resolve_method_query_t *query,
                                                      const CBMTypeRegistry *owner,
                                                      const CBMRegisteredFunc *method,
                                                      void *opaque) {
    method_adapter_t *adapter = opaque;
    adapter->calls++;
    cbm_resolve_method_candidate_t result = {
        .exposed = owner == adapter->hidden_owner ? CBM_RESOLVE_METHOD_NO : CBM_RESOLVE_METHOD_YES,
        .applicable = CBM_RESOLVE_METHOD_YES,
        .domain = adapter->unknown_domain ? CBM_RESOLVE_DOMAIN_UNKNOWN
                  : adapter->external     ? CBM_RESOLVE_DOMAIN_EXTERNAL
                                          : CBM_RESOLVE_DOMAIN_PROJECT,
        .graph_target = cbm_arena_sprintf(adapter->arena, "%s%s", method->qualified_name,
                                          method->sig ? method->sig : ""),
        .graph_target_exact = true,
    };
    if (adapter->unknown_exposure) {
        result.exposed = CBM_RESOLVE_METHOD_UNKNOWN;
    }
    /* These fixtures have at most one fallback registry. Independent rows
     * remain distinct even when their graph names and signatures coincide. */
    if (method->declaration_identity && method->declaration_identity[0]) {
        result.declaration_identity = cbm_arena_sprintf(
            adapter->arena, "%s:%s", owner == query->registry ? "primary" : "fallback",
            method->declaration_identity);
        result.declaration_identity_exact = result.declaration_identity != NULL;
    }
    if (adapter->coalesce_identical_declarations) {
        result.declaration_identity = result.graph_target;
        result.declaration_identity_exact = true;
    }
    if (adapter->wrong_target) {
        result.graph_target = "project.decoy.Widget.run";
    }
    if (adapter->compare_types && query->argument_count == 1 && method->signature &&
        method->signature->kind == CBM_TYPE_FUNC && method->signature->data.func.param_types &&
        method->signature->data.func.param_types[0]) {
        const CBMType *argument = query->argument_types ? query->argument_types[0] : NULL;
        const CBMType *parameter = method->signature->data.func.param_types[0];
        if (!argument || argument->kind == CBM_TYPE_UNKNOWN ||
            parameter->kind == CBM_TYPE_UNKNOWN) {
            result.applicable = CBM_RESOLVE_METHOD_UNKNOWN;
        } else if (argument->kind == CBM_TYPE_BUILTIN && parameter->kind == CBM_TYPE_BUILTIN) {
            result.applicable =
                strcmp(argument->data.builtin.name, parameter->data.builtin.name) == 0
                    ? CBM_RESOLVE_METHOD_YES
                    : CBM_RESOLVE_METHOD_NO;
        } else {
            result.applicable = CBM_RESOLVE_METHOD_UNKNOWN;
        }
    }
    return result;
}

static void add_method(CBMTypeRegistry *registry, const char *receiver, const char *method,
                       const char *sig, const char *parameter) {
    const CBMType **params = cbm_arena_calloc(registry->arena, 2 * sizeof(*params));
    if (parameter) {
        params[0] = cbm_type_builtin(registry->arena, parameter);
    }
    cbm_registry_add_func(
        registry,
        (CBMRegisteredFunc){
            .qualified_name = cbm_arena_sprintf(registry->arena, "%s.%s", receiver, method),
            .receiver_type = receiver,
            .short_name = method,
            .signature = cbm_type_func(registry->arena, NULL, params, NULL),
            .min_params = parameter ? 1 : 0,
            .sig = sig,
            .declaration_identity =
                cbm_arena_sprintf(registry->arena, "fixture:%s.%s%s:entry:%d", receiver, method,
                                  sig ? sig : "", registry->func_count),
        });
}

static cbm_resolve_type_fact_t method_receiver(CBMArena *arena, const char *name) {
    return (cbm_resolve_type_fact_t){
        .type = cbm_type_named(arena, name),
        .status = CBM_RESOLVE_TYPE_KNOWN,
        .origin = CBM_RESOLVE_TYPE_ANNOTATION,
        .evidence = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                     .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                     .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                     .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                     .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                              CBM_RESOLVE_FACT_TYPE_BOUND,
                     .target = name,
                     .candidate_count = 1},
    };
}

static cbm_resolve_method_query_t method_query(CBMTypeRegistry *registry,
                                               const cbm_resolve_type_fact_t *receiver,
                                               method_adapter_t *adapter) {
    method_adapter_proofs(adapter);
    return (cbm_resolve_method_query_t){.registry = registry,
                                        .receiver = receiver,
                                        .receiver_qn = "project.Widget",
                                        .method_name = "run",
                                        .argument_count = 0,
                                        .adapter_facts = METHOD_FACTS,
                                        .classify = classify_method,
                                        .context = adapter,
                                        .proof_pool = &adapter->proofs,
                                        .proof_source = &symbol_fixture_source};
}

TEST(resolver_symbol_exact_receiver_and_method) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_method(&registry, "project.Widget", "run", NULL, NULL);
    add_method(&registry, "other.Widget", "run", NULL, NULL);
    add_method(&registry, "project.Widget", "runner", NULL, NULL);
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
    cbm_resolve_method_query_t query = method_query(&registry, &receiver, &adapter);
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.target, "project.Widget.run");
    ASSERT(result.owner == &registry);
    ASSERT(result.method == &registry.funcs[0]);
    ASSERT_EQ(adapter.calls, 1);
    query.proof_pool = NULL;
    result = cbm_resolve_method(&query);
    ASSERT_FALSE(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.reason, "symbol_proof_context_missing");
    query.proof_pool = &adapter.proofs;
    const char *identity = registry.funcs[0].declaration_identity;
    registry.funcs[0].declaration_identity = NULL;
    result = cbm_resolve_method(&query);
    ASSERT_FALSE(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.reason, "symbol_declaration_identity_unproven");
    registry.funcs[0].declaration_identity = identity;
    query.receiver_qn = "Widget";
    result = cbm_resolve_method(&query);
    ASSERT(!cbm_resolve_admit(&result.result));
    query.receiver_qn = "project.Widget";
    query.method_name = "missing";
    result = cbm_resolve_method(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_UNRESOLVED);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT(result.method == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_known_overload_and_signature_identity) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_method(&registry, "project.Widget", "run", "(int)", "int");
    add_method(&registry, "project.Widget", "run", "(str)", "str");
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena, .compare_types = true};
    cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
    const CBMType *arguments[] = {cbm_type_builtin(&arena, "str")};
    cbm_resolve_method_query_t query = method_query(&registry, &receiver, &adapter);
    query.argument_count = 1;
    query.argument_types = arguments;
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.target, "project.Widget.run(str)");
    ASSERT(result.method == &registry.funcs[1]);
    adapter.wrong_target = true;
    result = cbm_resolve_method(&query);
    ASSERT(!cbm_resolve_admit(&result.result));
    ASSERT(result.method == NULL);
    ASSERT_STR_EQ(result.result.reason, "method_graph_identity_unproven");
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_unknown_overload_never_picks_first) {
    CBMArena arena;
    cbm_arena_init(&arena);
    for (int first = 0; first < 2; first++) {
        CBMTypeRegistry registry;
        cbm_registry_init(&registry, &arena);
        registry.index_first_registered = first != 0;
        add_method(&registry, "project.Widget", "run", "(int)", "int");
        add_method(&registry, "project.Widget", "run", "(str)", "str");
        cbm_registry_finalize(&registry);
        /* Even an adapter claiming both apply cannot resolve missing types. */
        method_adapter_t adapter = {.arena = &arena};
        cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
        cbm_resolve_method_query_t query = method_query(&registry, &receiver, &adapter);
        query.argument_count = 1;
        cbm_resolve_method_result_t result = cbm_resolve_method(&query);
        ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
        ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
        ASSERT_EQ(result.result.candidate_count, 2);
        ASSERT_STR_EQ(result.result.reason, "method_overload_arguments_unknown");
        ASSERT(result.method == NULL);
    }
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_known_tie_and_arity_mismatch) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_method(&registry, "project.Widget", "run", "(int)", "int");
    add_method(&registry, "project.Widget", "run", "(number)", "number");
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
    const CBMType *arguments[] = {cbm_type_builtin(&arena, "int")};
    cbm_resolve_method_query_t query = method_query(&registry, &receiver, &adapter);
    query.argument_count = 1;
    query.argument_types = arguments;
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT(result.method == NULL);
    query.argument_count = 0;
    result = cbm_resolve_method(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_UNRESOLVED);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT_EQ(result.result.candidate_count, 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_receiver_scope_and_domain_evidence_required) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_method(&registry, "project.Widget", "run", NULL, NULL);
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
    cbm_resolve_method_query_t query = method_query(&registry, &receiver, &adapter);
    for (uint32_t flag = 1; flag <= CBM_RESOLVE_METHOD_OCCURRENCE_EXACT; flag <<= 1) {
        query.adapter_facts = METHOD_FACTS & ~flag;
        cbm_resolve_method_result_t result = cbm_resolve_method(&query);
        ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
        ASSERT(result.method == NULL);
    }
    query.adapter_facts = METHOD_FACTS;
    receiver.evidence.facts &= ~CBM_RESOLVE_FACT_TYPE_BOUND;
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    receiver = method_receiver(&arena, "project.Widget");
    receiver.status = CBM_RESOLVE_TYPE_STALE;
    result = cbm_resolve_method(&query);
    ASSERT(result.method == NULL);
    receiver = method_receiver(&arena, "project.Widget");
    CBMVarBinding binding = {.name = "item", .type = cbm_type_unknown()};
    receiver.binding = &binding;
    result = cbm_resolve_method(&query);
    ASSERT_STR_EQ(result.result.reason, "method_receiver_stale");
    ASSERT_EQ(adapter.calls, 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_unfinalized_and_tail_are_not_complete) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_method(&registry, "project.Widget", "run", NULL, NULL);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
    cbm_resolve_method_query_t query = method_query(&registry, &receiver, &adapter);
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT_STR_EQ(result.result.reason, "method_registry_not_finalized");
    cbm_registry_finalize(&registry);
    add_method(&registry, "project.Widget", "run", "(duplicate)", NULL);
    result = cbm_resolve_method(&query);
    ASSERT_STR_EQ(result.result.reason, "method_registry_not_finalized");
    ASSERT(result.method == NULL);
    ASSERT_EQ(adapter.calls, 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_overlay_requires_explicit_exposure) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry head;
    CBMTypeRegistry base;
    cbm_registry_init(&head, &arena);
    cbm_registry_init(&base, &arena);
    add_method(&head, "project.Widget", "run", NULL, NULL);
    add_method(&base, "project.Widget", "run", NULL, NULL);
    cbm_registry_finalize(&head);
    cbm_registry_finalize(&base);
    head.fallback = &base;
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
    cbm_resolve_method_query_t query = method_query(&head, &receiver, &adapter);
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT(result.method == NULL);
    adapter.hidden_owner = &base;
    result = cbm_resolve_method(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT(result.owner == &head);
    adapter.unknown_exposure = true;
    result = cbm_resolve_method(&query);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    ASSERT(result.method == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_budget_cycle_and_unrelated_corpus) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_method(&registry, "project.Widget", "run", NULL, NULL);
    for (int i = 0; i < 4096; i++) {
        add_method(&registry, cbm_arena_sprintf(&arena, "noise.Type%d", i), "run", NULL, NULL);
    }
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_type_fact_t receiver = method_receiver(&arena, "project.Widget");
    cbm_resolve_method_query_t query = method_query(&registry, &receiver, &adapter);
    query.work_limit = 64;
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_EQ(adapter.calls, 1);
    ASSERT(result.work <= query.work_limit);
    query.work_limit = 1;
    cbm_resolve_method_result_t first = cbm_resolve_method(&query);
    cbm_resolve_method_result_t second = cbm_resolve_method(&query);
    ASSERT_EQ(first.result.analysis, CBM_RESOLVE_ANALYSIS_LIMITED);
    ASSERT_EQ(first.work, second.work);
    ASSERT(first.method == NULL);
    query.work_limit = 64;
    registry.fallback = &registry;
    result = cbm_resolve_method(&query);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_FAILED);
    ASSERT_STR_EQ(result.result.reason, "method_registry_cycle");
    ASSERT(result.method == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

static void add_function(CBMTypeRegistry *registry, const char *qn, const char *sig,
                         const char *parameter) {
    const CBMType **params = cbm_arena_calloc(registry->arena, 2 * sizeof(*params));
    if (parameter) {
        params[0] = cbm_type_builtin(registry->arena, parameter);
    }
    cbm_registry_add_func(
        registry,
        (CBMRegisteredFunc){.qualified_name = qn,
                            .short_name = "run",
                            .signature = cbm_type_func(registry->arena, NULL, params, NULL),
                            .min_params = parameter ? 1 : 0,
                            .sig = sig,
                            .declaration_identity =
                                cbm_arena_sprintf(registry->arena, "fixture:%s%s:entry:%d", qn,
                                                  sig ? sig : "", registry->func_count)});
}

static cbm_resolve_function_query_t function_query(CBMTypeRegistry *registry,
                                                   method_adapter_t *adapter) {
    method_adapter_proofs(adapter);
    return (cbm_resolve_function_query_t){
        .registry = registry,
        .function_qn = "project.worker.run",
        .function_binding = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                             .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                             .facts =
                                 CBM_RESOLVE_FACT_IMPORT_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT},
        .adapter_facts = METHOD_FACTS,
        .classify = classify_method,
        .context = adapter,
        .proof_pool = &adapter->proofs,
        .proof_source = &symbol_fixture_source,
    };
}

TEST(resolver_symbol_exact_function_bucket_excludes_method_and_short_names) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_function(&registry, "project.worker.run", NULL, NULL);
    add_method(&registry, "project.worker", "run", NULL, NULL);
    for (int i = 0; i < 4096; i++) {
        add_function(&registry, cbm_arena_sprintf(&arena, "decoy.module%d.run", i), NULL, NULL);
    }
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_function_query_t query = function_query(&registry, &adapter);
    query.work_limit = 64;
    cbm_resolve_function_result_t result = cbm_resolve_function(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.target, "project.worker.run");
    ASSERT_EQ(result.result.rule, CBM_RESOLVE_RULE_IMPORT_BINDING);
    ASSERT(result.function == &registry.funcs[0]);
    ASSERT_EQ(adapter.calls, 1);
    ASSERT(result.work <= query.work_limit);
    query.function_qn = "run";
    result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_UNRESOLVED);
    ASSERT(result.function == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_function_binding_and_complete_bucket_required) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_function(&registry, "project.worker.run", NULL, NULL);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_function_query_t query = function_query(&registry, &adapter);
    cbm_resolve_function_result_t result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    cbm_registry_finalize(&registry);
    query.function_binding.facts &= ~CBM_RESOLVE_FACT_IMPORT_BOUND;
    result = cbm_resolve_function(&query);
    ASSERT_STR_EQ(result.result.reason, "function_binding_unproven");
    query = function_query(&registry, &adapter);
    query.function_binding.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
    result = cbm_resolve_function(&query);
    ASSERT(result.function == NULL);
    query = function_query(&registry, &adapter);
    add_function(&registry, "project.worker.run", "(duplicate)", NULL);
    result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    ASSERT_EQ(adapter.calls, 0);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_function_duplicate_needs_proved_declaration_identity) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry head;
    CBMTypeRegistry base;
    cbm_registry_init(&head, &arena);
    cbm_registry_init(&base, &arena);
    add_function(&head, "project.worker.run", "(int)", "int");
    add_function(&base, "project.worker.run", "(int)", "int");
    cbm_registry_finalize(&head);
    cbm_registry_finalize(&base);
    head.fallback = &base;
    method_adapter_t adapter = {.arena = &arena};
    const CBMType *arguments[] = {cbm_type_builtin(&arena, "int")};
    cbm_resolve_function_query_t query = function_query(&head, &adapter);
    query.argument_types = arguments;
    query.argument_count = 1;
    cbm_resolve_function_result_t result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT(result.function == NULL);
    /* These fixtures have identical parameter signatures. Only the explicit
     * equivalence proof lets their otherwise distinct identities coalesce. */
    adapter.coalesce_identical_declarations = true;
    result = cbm_resolve_function(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.target, "project.worker.run(int)");
    ASSERT_EQ(result.result.candidate_count, 1);
    add_function(&base, "project.worker.run", "(number)", "number");
    cbm_registry_finalize(&base);
    result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT(result.function == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_function_overload_requires_known_arguments) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_function(&registry, "project.worker.run", "(int)", "int");
    add_function(&registry, "project.worker.run", "(str)", "str");
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena, .compare_types = true};
    cbm_resolve_function_query_t query = function_query(&registry, &adapter);
    query.argument_count = 1;
    cbm_resolve_function_result_t result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    const CBMType *arguments[] = {cbm_type_builtin(&arena, "int")};
    query.argument_types = arguments;
    result = cbm_resolve_function(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.target, "project.worker.run(int)");
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_target_domain_is_explicit) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_function(&registry, "project.worker.run", NULL, NULL);
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena, .external = true};
    cbm_resolve_function_query_t query = function_query(&registry, &adapter);
    cbm_resolve_function_result_t result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_UNIQUE);
    ASSERT_EQ(result.result.domain, CBM_RESOLVE_DOMAIN_EXTERNAL);
    ASSERT(!cbm_resolve_admit(&result.result));
    ASSERT(result.function == NULL);
    adapter.unknown_domain = true;
    result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    ASSERT_STR_EQ(result.result.reason, "symbol_target_domain_unknown");
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_name_identity_needs_no_invented_call_arity) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    add_function(&registry, "project.worker.run", "(int)", "int");
    registry.funcs[0].min_params = -1;
    cbm_registry_finalize(&registry);
    method_adapter_t adapter = {.arena = &arena};
    cbm_resolve_function_query_t query = function_query(&registry, &adapter);
    query.argument_count = -1;
    query.adapter_facts &= ~CBM_RESOLVE_METHOD_FIXED_ARITY;
    query.adapter_facts |= CBM_RESOLVE_METHOD_NAME_IDENTITY;
    cbm_resolve_function_result_t result = cbm_resolve_function(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_STR_EQ(result.result.target, "project.worker.run(int)");
    add_function(&registry, "project.worker.run", "(str)", "str");
    cbm_registry_finalize(&registry);
    result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT(result.function == NULL);
    /* A reference to an overloaded name cannot silently become a typed call. */
    adapter.compare_types = true;
    const CBMType *arguments[] = {cbm_type_builtin(&arena, "int")};
    query.argument_count = 1;
    query.argument_types = arguments;
    result = cbm_resolve_function(&query);
    ASSERT_EQ(result.result.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT(result.function == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_symbol_cached_receiver_loses_revoked_observation) {
    for (int adapted = 0; adapted < 2; adapted++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMTypeRegistry registry;
        cbm_registry_init(&registry, &arena);
        add_method(&registry, "project.Widget", "run", NULL, NULL);
        cbm_registry_finalize(&registry);
        CBMScope *scope = cbm_scope_push(&arena, NULL);
        CBMResolveTypeMap map;
        cbm_resolve_types_init(&map, &arena, 0);
        ASSERT(cbm_resolve_types_set_context(&map, &symbol_fixture_source));
        cbm_resolve_type_fact_t immutable = method_receiver(&arena, "project.Widget");
        ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, immutable.type,
                                         CBM_RESOLVE_TYPE_ANNOTATION, &immutable.evidence));
        cbm_resolve_type_fact_t cached = cbm_resolve_types_lookup(&map, scope, "item", 2);
        /* A checked pointer-to-owner adaptation may clear this convenience
         * field. It must not detach the source binding's revocation state. */
        if (adapted)
            cached.binding = NULL;
        method_adapter_t adapter = {.arena = &arena};
        cbm_resolve_method_query_t query = method_query(&registry, &cached, &adapter);
        cbm_resolve_method_result_t result = cbm_resolve_method(&query);
        ASSERT(cbm_resolve_admit(&result.result));
        cbm_resolve_result_t unproven = immutable.evidence;
        unproven.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
        ASSERT(cbm_resolve_types_observe(&map, scope, "item", 3, immutable.type,
                                         CBM_RESOLVE_TYPE_OBSERVED_BINDING, &unproven));
        ASSERT(cbm_scope_lookup(scope, "item") == immutable.type);
        ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 4).status,
                  CBM_RESOLVE_TYPE_UNKNOWN);
        adapter.calls = 0;
        result = cbm_resolve_method(&query);
        ASSERT_FALSE(cbm_resolve_admit(&result.result));
        ASSERT(result.method == NULL);
        ASSERT_STR_EQ(result.result.reason, "method_receiver_stale");
        ASSERT_EQ(adapter.calls, 0);
        cbm_arena_destroy(&arena);
    }
    PASS();
}

TEST(resolver_symbol_cached_receiver_loses_failed_or_reinitialized_map) {
    for (int example = 0; example < 3; example++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMTypeRegistry registry;
        cbm_registry_init(&registry, &arena);
        add_method(&registry, "project.Widget", "run", NULL, NULL);
        cbm_registry_finalize(&registry);
        CBMScope *scope = cbm_scope_push(&arena, NULL);
        CBMResolveTypeMap map;
        cbm_resolve_types_init(&map, &arena, example == 1 ? 64 : 0);
        ASSERT(cbm_resolve_types_set_context(&map, &symbol_fixture_source));
        cbm_resolve_type_fact_t immutable = method_receiver(&arena, "project.Widget");
        ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, immutable.type,
                                         CBM_RESOLVE_TYPE_ANNOTATION, &immutable.evidence));
        cbm_resolve_type_fact_t cached = cbm_resolve_types_lookup(&map, scope, "item", 2);
        ASSERT_EQ(cached.status, CBM_RESOLVE_TYPE_KNOWN);
        if (example == 0) {
            cbm_resolve_types_init(&map, &arena, 0);
            ASSERT(cbm_resolve_types_set_context(&map, &symbol_fixture_source));
            ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, immutable.type,
                                             CBM_RESOLVE_TYPE_ANNOTATION, &immutable.evidence));
            cbm_resolve_type_fact_t current = cbm_resolve_types_lookup(&map, scope, "item", 2);
            ASSERT_EQ(current.status, CBM_RESOLVE_TYPE_KNOWN);
            ASSERT_EQ(current.version, cached.version);
        } else if (example == 1) {
            cbm_resolve_type_fact_t current = cached;
            for (int i = 0; i < 128 && current.status == CBM_RESOLVE_TYPE_KNOWN; i++)
                current = cbm_resolve_types_lookup(&map, scope, "item", 2);
            ASSERT_EQ(current.status, CBM_RESOLVE_TYPE_LIMITED);
        } else {
            CBMScope exhausted = {.parent = scope};
            ASSERT_FALSE(cbm_resolve_types_declare(&map, &exhausted, "other", 3, immutable.type,
                                                   CBM_RESOLVE_TYPE_ANNOTATION,
                                                   &immutable.evidence));
            ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 4).status,
                      CBM_RESOLVE_TYPE_FAILED);
        }
        ASSERT(cbm_scope_lookup(scope, "item") == immutable.type);
        method_adapter_t adapter = {.arena = &arena};
        cbm_resolve_method_query_t query = method_query(&registry, &cached, &adapter);
        cbm_resolve_method_result_t result = cbm_resolve_method(&query);
        ASSERT_FALSE(cbm_resolve_admit(&result.result));
        ASSERT(result.method == NULL);
        ASSERT_EQ(result.result.analysis, example == 1   ? CBM_RESOLVE_ANALYSIS_LIMITED
                                          : example == 2 ? CBM_RESOLVE_ANALYSIS_FAILED
                                                         : CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
        ASSERT_EQ(adapter.calls, 0);
        cbm_arena_destroy(&arena);
    }
    PASS();
}

SUITE(resolver_symbols) {
    RUN_TEST(resolver_symbol_cached_receiver_loses_revoked_observation);
    RUN_TEST(resolver_symbol_cached_receiver_loses_failed_or_reinitialized_map);
    RUN_TEST(resolver_symbol_exact_receiver_and_method);
    RUN_TEST(resolver_symbol_known_overload_and_signature_identity);
    RUN_TEST(resolver_symbol_unknown_overload_never_picks_first);
    RUN_TEST(resolver_symbol_known_tie_and_arity_mismatch);
    RUN_TEST(resolver_symbol_receiver_scope_and_domain_evidence_required);
    RUN_TEST(resolver_symbol_unfinalized_and_tail_are_not_complete);
    RUN_TEST(resolver_symbol_overlay_requires_explicit_exposure);
    RUN_TEST(resolver_symbol_budget_cycle_and_unrelated_corpus);
    RUN_TEST(resolver_symbol_exact_function_bucket_excludes_method_and_short_names);
    RUN_TEST(resolver_symbol_function_binding_and_complete_bucket_required);
    RUN_TEST(resolver_symbol_function_duplicate_needs_proved_declaration_identity);
    RUN_TEST(resolver_symbol_function_overload_requires_known_arguments);
    RUN_TEST(resolver_symbol_target_domain_is_explicit);
    RUN_TEST(resolver_symbol_name_identity_needs_no_invented_call_arity);
}
