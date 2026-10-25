#include "test_framework.h"
#include "resolve/proof.h"
#include "resolve/policy.h"
#include "resolve/symbols.h"
#include "lsp/proof_bridge.h"
#include <string.h>

static cbm_resolve_result_t proof_outcome(const char *target) {
    return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                                  .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                           CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                           CBM_RESOLVE_FACT_TYPE_BOUND,
                                  .target = target,
                                  .candidate_count = 1,
                                  .reason = "test_exact_binding"};
}
static const cbm_resolve_proof_t *proof_leaf(cbm_resolve_proof_pool_t *pool, const char *name,
                                             cbm_resolve_analysis_t state) {
    cbm_resolve_proof_source_t source = {.module = "project.module",
                                         .content_identity = "captured-bytes",
                                         .configuration_identity = "grammar-config-v1"};
    cbm_resolve_proof_key_t key = {
        .lookup_domain = "project.module", .lookup_key = name, .declaration_identity = name};
    cbm_resolve_result_t outcome = proof_outcome(name);
    outcome.analysis = state;
    return cbm_resolve_proof_leaf(pool, CBM_PROOF_SOURCE_FACT, &source, &key, &outcome);
}
static const cbm_resolve_proof_t *proof_join(cbm_resolve_proof_pool_t *pool,
                                             const cbm_resolve_proof_t *const *inputs, size_t count,
                                             bool closed) {
    cbm_resolve_result_t outcome = proof_outcome("project.Widget");
    cbm_resolve_proof_key_t key = {.lookup_domain = "flow-join", .lookup_key = "join:40"};
    return cbm_resolve_proof_derive(pool, CBM_PROOF_TYPE_JOIN, NULL, &key, &outcome, inputs, count,
                                    closed);
}

TEST(resolver_proof_full_key_interning_and_collision_equality) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    const cbm_resolve_proof_t *a = proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE);
    const cbm_resolve_proof_t *b = proof_leaf(&pool, "project.b", CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);
    ASSERT(a == proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE));
    ASSERT(a != b);
    cbm_resolve_proof_t collision = *b;
    collision.value.fingerprint = a->value.fingerprint;
    ASSERT(cbm_resolve_proof_compare(a, &collision) != 0);
    cbm_resolve_proof_source_t changed = a->value.source;
    changed.configuration_identity = "grammar-config-v2";
    const cbm_resolve_proof_t *configuration = cbm_resolve_proof_leaf(
        &pool, CBM_PROOF_SOURCE_FACT, &changed, &a->value.key, &a->value.outcome);
    ASSERT_NOT_NULL(configuration);
    ASSERT(configuration != a);
    changed = a->value.source;
    changed.content_identity = "different-bytes";
    ASSERT(cbm_resolve_proof_leaf(&pool, CBM_PROOF_SOURCE_FACT, &changed, &a->value.key,
                                  &a->value.outcome) != a);
    ASSERT_EQ(pool.count, 4);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_closed_join_keeps_all_distinct_supports) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    const cbm_resolve_proof_t *a = proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE);
    const cbm_resolve_proof_t *b = proof_leaf(&pool, "project.b", CBM_RESOLVE_ANALYSIS_COMPLETE);
    const cbm_resolve_proof_t *inputs[] = {a, b, a};
    const cbm_resolve_proof_t *reverse[] = {b, a};
    const cbm_resolve_proof_t *joined = proof_join(&pool, inputs, 3, true);
    ASSERT_NOT_NULL(joined);
    ASSERT(joined == proof_join(&pool, reverse, 2, true));
    ASSERT_EQ(joined->value.premise_count, 2);
    ASSERT(joined->value.premises[0] != joined->value.premises[1]);
    ASSERT(joined->value.predecessors_closed);
    ASSERT(cbm_resolve_admit(&joined->value.outcome));
    ASSERT(joined->value.outcome.proof == NULL);
    ASSERT(joined != proof_join(&pool, &a, 1, true));
    cbm_arena_destroy(&arena);
    PASS();
}

typedef struct {
    const cbm_resolve_proof_t *nodes[128];
    size_t count;
} proof_order_t;
static bool proof_postorder(const cbm_resolve_proof_t *node, void *opaque) {
    proof_order_t *order = opaque;
    if (order->count == sizeof(order->nodes) / sizeof(order->nodes[0]))
        return false;
    for (size_t i = 0; i < node->value.premise_count; i++) {
        bool found = false;
        for (size_t j = 0; j < order->count; j++)
            if (order->nodes[j] == node->value.premises[i])
                found = true;
        if (!found)
            return false;
    }
    order->nodes[order->count++] = node;
    return true;
}
TEST(resolver_proof_clone_owns_dag_and_canonical_traversal) {
    CBMArena source_arena, destination_arena;
    cbm_arena_init(&source_arena);
    cbm_arena_init(&destination_arena);
    cbm_resolve_proof_pool_t source, destination;
    cbm_resolve_proof_pool_init(&source, &source_arena, 0, 0, 0);
    cbm_resolve_proof_pool_init(&destination, &destination_arena, 0, 0, 0);
    const cbm_resolve_proof_t *inputs[] = {
        proof_leaf(&source, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE),
        proof_leaf(&source, "project.b", CBM_RESOLVE_ANALYSIS_COMPLETE)};
    const cbm_resolve_proof_t *joined = proof_join(&source, inputs, 2, true);
    const cbm_resolve_proof_t *copy = cbm_resolve_proof_clone(&destination, joined);
    ASSERT_NOT_NULL(copy);
    ASSERT(copy != joined);
    ASSERT_EQ(cbm_resolve_proof_compare(copy, joined), 0);
    ASSERT(copy->value.premises != joined->value.premises);
    ASSERT(copy->value.premises[0] != joined->value.premises[0]);
    ASSERT(copy == cbm_resolve_proof_clone(&destination, joined));
    cbm_arena_destroy(&source_arena);
    proof_order_t order = {0};
    ASSERT(cbm_resolve_proof_visit(copy, 10, proof_postorder, &order));
    ASSERT_EQ(order.count, 3);
    ASSERT(order.nodes[2] == copy);
    ASSERT_STR_EQ(order.nodes[0]->value.source.content_identity, "captured-bytes");
    ASSERT_STR_EQ(order.nodes[0]->value.source.configuration_identity, "grammar-config-v1");
    cbm_arena_destroy(&destination_arena);
    PASS();
}

TEST(resolver_proof_incomplete_predecessors_revoke_authority) {
    const cbm_resolve_analysis_t states[] = {
        CBM_RESOLVE_ANALYSIS_PENDING, CBM_RESOLVE_ANALYSIS_UNKNOWN,
        CBM_RESOLVE_ANALYSIS_UNSUPPORTED, CBM_RESOLVE_ANALYSIS_LIMITED,
        CBM_RESOLVE_ANALYSIS_FAILED};
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    const cbm_resolve_proof_t *good = proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE);
    for (size_t i = 0; i < sizeof(states) / sizeof(states[0]); i++) {
        const cbm_resolve_proof_t *inputs[] = {good, proof_leaf(&pool, "project.b", states[i])};
        const cbm_resolve_proof_t *joined = proof_join(&pool, inputs, 2, true);
        ASSERT_NOT_NULL(joined);
        ASSERT_EQ(joined->value.outcome.analysis, states[i]);
        ASSERT_EQ(joined->value.premise_count, 2);
        ASSERT_EQ(joined->value.outcome.binding, CBM_RESOLVE_BINDING_UNRESOLVED);
        ASSERT_EQ(joined->value.outcome.facts, 0);
        ASSERT(joined->value.outcome.target == NULL);
        ASSERT(!cbm_resolve_admit(&joined->value.outcome));
    }
    const cbm_resolve_proof_t *pending = proof_join(&pool, &good, 1, false);
    ASSERT_NOT_NULL(pending);
    ASSERT_EQ(pending->value.outcome.analysis, CBM_RESOLVE_ANALYSIS_PENDING);
    ASSERT(!pending->value.predecessors_closed);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_limits_and_unknown_source_are_explicit) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 1, 0, 0);
    ASSERT_NOT_NULL(proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE));
    ASSERT(proof_leaf(&pool, "project.b", CBM_RESOLVE_ANALYSIS_COMPLETE) == NULL);
    ASSERT_EQ(pool.failure, CBM_RESOLVE_ANALYSIS_LIMITED);
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 1);
    ASSERT(proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE) == NULL);
    ASSERT_EQ(pool.failure, CBM_RESOLVE_ANALYSIS_LIMITED);
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_result_t outcome = proof_outcome("project.Widget");
    cbm_resolve_proof_key_t key = {.type_identity = "project.Widget", .origin = 1};
    const cbm_resolve_proof_t *audit =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_ADAPTER_TYPE, NULL, &key, &outcome);
    ASSERT_NOT_NULL(audit);
    ASSERT(audit->value.source.content_identity == NULL);
    ASSERT_EQ(audit->value.kind, CBM_PROOF_ADAPTER_TYPE);
    const cbm_resolve_proof_t *missing =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_SOURCE_FACT, NULL, &key, &outcome);
    ASSERT_NOT_NULL(missing);
    ASSERT_EQ(missing->value.outcome.analysis, CBM_RESOLVE_ANALYSIS_UNSUPPORTED);
    ASSERT_EQ(pool.failure, CBM_RESOLVE_ANALYSIS_COMPLETE);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_same_pool_chain_does_not_revisit_ancestors) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    const cbm_resolve_proof_t *node = proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE);
    uint64_t first_half = 0;
    for (unsigned i = 0; i < 64; i++) {
        cbm_resolve_proof_key_t key = {
            .lookup_domain = "copy", .lookup_key = "same-width", .start_byte = i};
        cbm_resolve_result_t outcome = proof_outcome("project.Widget");
        node = cbm_resolve_proof_derive(&pool, CBM_PROOF_TYPE_COPY, NULL, &key, &outcome, &node, 1,
                                        true);
        ASSERT_NOT_NULL(node);
        if (i == 31)
            first_half = pool.work;
    }
    ASSERT(pool.work < first_half * 3);
    ASSERT_EQ(pool.count, 65);
    proof_order_t order = {0};
    ASSERT(cbm_resolve_proof_visit(node, 65, proof_postorder, &order));
    ASSERT_EQ(order.count, 65);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_named_copy_keeps_rule_and_dependency_without_admission_flags) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    const cbm_resolve_proof_t *inputs[] = {
        proof_leaf(&pool, "project.export_rule", CBM_RESOLVE_ANALYSIS_COMPLETE),
        proof_leaf(&pool, "project.target", CBM_RESOLVE_ANALYSIS_COMPLETE)};
    cbm_resolve_result_t outcome = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                    .binding = CBM_RESOLVE_BINDING_UNIQUE,
                                    .target = "project.target"};
    cbm_resolve_proof_key_t key = {.lookup_domain = "exports", .lookup_key = "public_name"};
    const cbm_resolve_proof_t *node = cbm_resolve_proof_derive(&pool, CBM_PROOF_NAMED_COPY, NULL,
                                                               &key, &outcome, inputs, 2, true);
    ASSERT_NOT_NULL(node);
    ASSERT_EQ(node->value.outcome.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT_EQ(node->value.outcome.binding, CBM_RESOLVE_BINDING_UNIQUE);
    ASSERT_EQ(node->value.outcome.facts, 0);
    ASSERT_EQ(node->value.premise_count, 2);
    ASSERT(!cbm_resolve_admit(&node->value.outcome));
    cbm_arena_destroy(&arena);
    PASS();
}

static cbm_resolve_method_candidate_t proof_classify(const cbm_resolve_method_query_t *query,
                                                     const CBMTypeRegistry *owner,
                                                     const CBMRegisteredFunc *candidate,
                                                     void *context) {
    (void)query;
    (void)owner;
    (void)context;
    return (cbm_resolve_method_candidate_t){
        .exposed = strcmp(candidate->graph_qualified_name, "project.Widget.run#hidden") == 0
                       ? CBM_RESOLVE_METHOD_NO
                       : CBM_RESOLVE_METHOD_YES,
        .applicable = CBM_RESOLVE_METHOD_YES,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .graph_target = candidate->graph_qualified_name,
        .graph_target_exact = true,
        .declaration_identity = candidate->graph_qualified_name,
        .declaration_identity_exact = true};
}
TEST(resolver_proof_member_lookup_retains_receiver_domain_and_excluded_candidate) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, &arena);
    const char *targets[] = {"project.Widget.run#visible", "project.Widget.run#hidden"};
    for (size_t i = 0; i < 2; i++) {
        cbm_registry_add_func(
            &registry, (CBMRegisteredFunc){.qualified_name = "project.Widget.run",
                                           .graph_qualified_name = targets[i],
                                           .receiver_type = "project.Widget",
                                           .short_name = "run",
                                           .signature = cbm_type_func(&arena, NULL, NULL, NULL),
                                           .min_params = 0});
    }
    cbm_registry_finalize(&registry);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    cbm_resolve_proof_source_t source = {.module = "project.caller",
                                         .content_identity = "captured"};
    ASSERT(cbm_resolve_types_set_context(&map, &source));
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    cbm_resolve_result_t evidence = proof_outcome("project.Widget");
    ASSERT(cbm_resolve_types_declare(&map, scope, "value", 10,
                                     cbm_type_named(&arena, "project.Widget"),
                                     CBM_RESOLVE_TYPE_ANNOTATION, &evidence));
    cbm_resolve_type_fact_t receiver = cbm_resolve_types_lookup(&map, scope, "value", 20);
    cbm_resolve_method_query_t query = {.registry = &registry,
                                        .receiver = &receiver,
                                        .receiver_qn = "project.Widget",
                                        .method_name = "run",
                                        .argument_count = 0,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_FIXED_ARITY |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT,
                                        .classify = proof_classify};
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    ASSERT(cbm_resolve_admit(&result.result));
    ASSERT_NOT_NULL(result.result.proof);
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(result.result.proof);
    ASSERT_EQ(view->kind, CBM_PROOF_REGISTRY_LOOKUP);
    ASSERT_STR_EQ(view->key.lookup_domain, "project.Widget");
    ASSERT_STR_EQ(view->key.lookup_key, "run");
    ASSERT_STR_EQ(view->key.declaration_identity, "project.Widget.run#visible");
    ASSERT_EQ(view->premise_count, 5);
    bool has_receiver = false, has_excluded = false;
    for (size_t i = 0; i < view->premise_count; i++) {
        const cbm_resolve_proof_t *support = view->premises[i];
        has_receiver |= support == receiver.evidence.proof;
        has_excluded |=
            support->value.key.declaration_identity &&
            strcmp(support->value.key.declaration_identity, "project.Widget.run#hidden") == 0;
    }
    ASSERT(has_receiver);
    ASSERT(has_excluded);
    ASSERT(cbm_resolve_types_invalidate(&map, scope, "value", 30, CBM_RESOLVE_TYPE_UNKNOWN));
    result = cbm_resolve_method(&query);
    ASSERT(!cbm_resolve_admit(&result.result));
    ASSERT(result.result.proof == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_generation_import_session_visits_shared_ancestors_once) {
    CBMArena source_arena, destination_arena;
    cbm_arena_init(&source_arena);
    cbm_arena_init(&destination_arena);
    cbm_resolve_proof_pool_t source, destination;
    cbm_resolve_proof_pool_init(&source, &source_arena, 0, 0, 0);
    cbm_resolve_proof_pool_init(&destination, &destination_arena, 0, 0, 0);
    const cbm_resolve_proof_t *roots[64];
    const cbm_resolve_proof_t *node =
        proof_leaf(&source, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE);
    for (unsigned i = 0; i < 64; i++) {
        cbm_resolve_proof_key_t key = {
            .lookup_domain = "copy", .lookup_key = "same-width", .start_byte = i};
        cbm_resolve_result_t outcome = proof_outcome("project.Widget");
        node = cbm_resolve_proof_derive(&source, CBM_PROOF_TYPE_COPY, NULL, &key, &outcome, &node,
                                        1, true);
        ASSERT_NOT_NULL(node);
        roots[i] = node;
    }
    cbm_resolve_proof_clone_session_t session;
    ASSERT(cbm_resolve_proof_clone_begin(&session, &destination));
    const cbm_resolve_proof_t *last = cbm_resolve_proof_clone_root(&session, roots[63]);
    ASSERT_NOT_NULL(last);
    uint64_t initial_work = destination.work;
    for (size_t i = 0; i < 64; i++)
        ASSERT_NOT_NULL(cbm_resolve_proof_clone_root(&session, roots[i]));
    ASSERT_EQ(destination.work - initial_work, 64);
    ASSERT_EQ(destination.count, 65);
    cbm_resolve_proof_clone_end(&session);
    cbm_arena_destroy(&source_arena);
    proof_order_t order = {0};
    ASSERT(cbm_resolve_proof_visit(last, 65, proof_postorder, &order));
    ASSERT_EQ(order.count, 65);
    cbm_arena_destroy(&destination_arena);
    PASS();
}

TEST(resolver_proof_multiple_roots_have_canonical_shared_postorder) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    const cbm_resolve_proof_t *a = proof_leaf(&pool, "project.a", CBM_RESOLVE_ANALYSIS_COMPLETE);
    const cbm_resolve_proof_t *b = proof_leaf(&pool, "project.b", CBM_RESOLVE_ANALYSIS_COMPLETE);
    const cbm_resolve_proof_t *inputs[] = {a, b};
    const cbm_resolve_proof_t *joined = proof_join(&pool, inputs, 2, true);
    const cbm_resolve_proof_t *roots[] = {a, joined, b, joined};
    const cbm_resolve_proof_t *reversed[] = {joined, b, a};
    proof_order_t first = {0}, second = {0};
    ASSERT(cbm_resolve_proof_visit_roots(roots, 4, 3, proof_postorder, &first));
    ASSERT(cbm_resolve_proof_visit_roots(reversed, 3, 3, proof_postorder, &second));
    ASSERT_EQ(first.count, 3);
    ASSERT_EQ(second.count, first.count);
    for (size_t i = 0; i < first.count; i++)
        ASSERT(first.nodes[i] == second.nodes[i]);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_standalone_context_keeps_publication_identity_unknown) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolveTypeMap maps[4];
    const char *sources[] = {"a\0b", "a\0b", "a\0c", "a\0b"};
    for (size_t i = 0; i < 4; i++) {
        cbm_resolve_types_init(&maps[i], &arena, 0);
        CBMSourceOrigin origin = i == 3 ? CBM_SOURCE_ORIGIN_PREPROCESSED : CBM_SOURCE_ORIGIN_RAW;
        ASSERT(cbm_lsp_proof_standalone_context(&maps[i], sources[i], 3, "project.main", origin));
        ASSERT_NOT_NULL(maps[i].proof_source.content_identity);
        ASSERT_EQ(strlen(maps[i].proof_source.content_identity), 64);
        ASSERT_EQ(maps[i].proof_source.source_origin, (uint32_t)origin);
        ASSERT(maps[i].proof_source.project == NULL);
        ASSERT(maps[i].proof_source.file == NULL);
        ASSERT(maps[i].proof_source.configuration_identity == NULL);
        ASSERT_EQ(maps[i].proofs.count, 0);
    }
    ASSERT_STR_EQ(maps[0].proof_source.content_identity, maps[1].proof_source.content_identity);
    ASSERT(strcmp(maps[0].proof_source.content_identity, maps[2].proof_source.content_identity));
    ASSERT(strcmp(maps[0].proof_source.content_identity, maps[3].proof_source.content_identity));
    cbm_resolve_proof_key_t key = {.lookup_domain = "project.main.run", .lookup_key = "Widget"};
    cbm_resolve_result_t outcome = proof_outcome("project.Widget");
    outcome.facts |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    outcome.proof = cbm_resolve_proof_leaf(&maps[0].proofs, CBM_PROOF_SOURCE_FACT,
                                           &maps[0].proof_source, &key, &outcome);
    ASSERT_NOT_NULL(outcome.proof);
    ASSERT_EQ(outcome.proof->value.outcome.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT(cbm_resolve_admit(&outcome));
    cbm_resolve_record_t record = {.file_path = "main.cpp",
                                   .scope = "project.main.run",
                                   .relation = "USES_TYPE",
                                   .spelling = "Widget",
                                   .start_byte = 0,
                                   .end_byte = 6,
                                   .content_identity = maps[0].proof_source.content_identity,
                                   .source_origin = CBM_SOURCE_ORIGIN_RAW,
                                   .result = outcome};
    ASSERT(!cbm_resolve_record_candidate(&record));
    ASSERT(!cbm_resolve_record_admit(&record));
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_proof_result_snapshot_replaces_standalone_before_donation) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    ASSERT(cbm_lsp_proof_standalone_context(&map, "Widget value;", 13, "project.main",
                                            CBM_SOURCE_ORIGIN_RAW));
    ASSERT_EQ(map.proofs.count, 0);
    CBMFileResult file = {.module_qn = "project.main",
                          .source_project = "project",
                          .source_file = "main.cpp",
                          .source_content_id = {"captured-raw", "captured-expanded"},
                          .source_configuration_id = "captured-configuration"};
    cbm_resolve_proof_source_t captured =
        cbm_result_proof_source(&file, CBM_SOURCE_ORIGIN_PREPROCESSED, "project.main.run");
    ASSERT(cbm_lsp_proof_context(&map, &captured, file.module_qn));
    ASSERT_EQ(map.proofs.count, 0);
    cbm_resolve_proof_key_t key = {.lookup_domain = "project.main.run", .lookup_key = "Widget"};
    cbm_resolve_result_t outcome = proof_outcome("project.Widget");
    const cbm_resolve_proof_t *leaf = cbm_resolve_proof_leaf(&map.proofs, CBM_PROOF_SOURCE_FACT,
                                                             &map.proof_source, &key, &outcome);
    ASSERT_NOT_NULL(leaf);
    ASSERT_EQ(leaf->value.outcome.analysis, CBM_RESOLVE_ANALYSIS_COMPLETE);
    ASSERT_STR_EQ(leaf->value.source.project, "project");
    ASSERT_STR_EQ(leaf->value.source.file, "main.cpp");
    ASSERT_STR_EQ(leaf->value.source.scope, "project.main.run");
    ASSERT_STR_EQ(leaf->value.source.content_identity, "captured-expanded");
    ASSERT_STR_EQ(leaf->value.source.configuration_identity, "captured-configuration");
    ASSERT_EQ(leaf->value.source.source_origin, CBM_SOURCE_ORIGIN_PREPROCESSED);
    ASSERT_EQ(map.proofs.count, 1);
    cbm_arena_destroy(&arena);
    PASS();
}

SUITE(resolver_proof) {
    RUN_TEST(resolver_proof_standalone_context_keeps_publication_identity_unknown);
    RUN_TEST(resolver_proof_result_snapshot_replaces_standalone_before_donation);
    RUN_TEST(resolver_proof_multiple_roots_have_canonical_shared_postorder);
    RUN_TEST(resolver_proof_generation_import_session_visits_shared_ancestors_once);
    RUN_TEST(resolver_proof_member_lookup_retains_receiver_domain_and_excluded_candidate);
    RUN_TEST(resolver_proof_full_key_interning_and_collision_equality);
    RUN_TEST(resolver_proof_closed_join_keeps_all_distinct_supports);
    RUN_TEST(resolver_proof_clone_owns_dag_and_canonical_traversal);
    RUN_TEST(resolver_proof_incomplete_predecessors_revoke_authority);
    RUN_TEST(resolver_proof_limits_and_unknown_source_are_explicit);
    RUN_TEST(resolver_proof_same_pool_chain_does_not_revisit_ancestors);
    RUN_TEST(resolver_proof_named_copy_keeps_rule_and_dependency_without_admission_flags);
}
