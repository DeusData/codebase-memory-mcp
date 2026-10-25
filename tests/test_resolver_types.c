#include "test_framework.h"
#include "cbm.h"
#include "resolve/types.h"
#include <string.h>

static cbm_resolve_result_t type_proof(const char *target) {
    return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                                  .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                           CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                           CBM_RESOLVE_FACT_TYPE_BOUND,
                                  .target = target,
                                  .candidate_count = 1};
}

TEST(resolver_type_exact_annotation_and_copy) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    const CBMType *type = cbm_type_named(&arena, "project.model.Widget");
    cbm_resolve_result_t proof = type_proof("project.model.Widget");
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    ASSERT(cbm_resolve_types_declare(&map, scope, "widget", 10, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT(cbm_resolve_types_copy(&map, scope, "alias", scope, "widget", 20, true));
    cbm_resolve_type_fact_t fact = cbm_resolve_types_lookup(&map, scope, "alias", 30);
    ASSERT_EQ(fact.status, CBM_RESOLVE_TYPE_KNOWN);
    ASSERT(fact.type == type);
    ASSERT(fact.binding == cbm_scope_lookup_local(scope, "alias"));
    ASSERT_EQ(fact.valid_from, 20);
    ASSERT_EQ(fact.origin, CBM_RESOLVE_TYPE_ASSIGNMENT);
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "alias", 19).status, CBM_RESOLVE_TYPE_STALE);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_unknown_shadow_blocks_parent) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *outer = cbm_scope_push(&arena, NULL);
    CBMScope *inner = cbm_scope_push(&arena, outer);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    const CBMType *type = cbm_type_named(&arena, "project.Widget");
    cbm_resolve_result_t proof = type_proof("project.Widget");
    ASSERT(cbm_resolve_types_declare(&map, outer, "item", 1, type, CBM_RESOLVE_TYPE_CONSTRUCTOR,
                                     &proof));
    ASSERT(cbm_resolve_types_declare(&map, inner, "item", 2, type, CBM_RESOLVE_TYPE_NO_EVIDENCE,
                                     NULL));
    cbm_resolve_type_fact_t fact = cbm_resolve_types_lookup(&map, inner, "item", 3);
    ASSERT_EQ(fact.status, CBM_RESOLVE_TYPE_UNKNOWN);
    ASSERT(fact.scope == inner);
    ASSERT(cbm_type_is_unknown(fact.type));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, outer, "item", 3).status, CBM_RESOLVE_TYPE_KNOWN);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_unknown_assignment_versions_and_clears_callable) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    const CBMType *type = cbm_type_named(&arena, "project.Widget");
    cbm_resolve_result_t proof = type_proof("project.Widget");
    ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT(cbm_scope_update_callable(scope, "item", "project.make"));
    ASSERT(cbm_resolve_types_assign(&map, scope, "item", 2, cbm_type_unknown(),
                                    CBM_RESOLVE_TYPE_NO_EVIDENCE, NULL));
    cbm_resolve_type_fact_t fact = cbm_resolve_types_lookup(&map, scope, "item", 3);
    ASSERT_EQ(fact.status, CBM_RESOLVE_TYPE_UNKNOWN);
    ASSERT_EQ(fact.version, 2);
    ASSERT_EQ(fact.declared_at, 1);
    ASSERT(cbm_scope_lookup_callable(scope, "item") == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_incomplete_binding_is_not_evidence) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    const CBMType *type = cbm_type_named(&arena, "project.Widget");
    cbm_resolve_result_t proof = type_proof("project.Widget");
    proof.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
    ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, type, CBM_RESOLVE_TYPE_CONSTRUCTOR,
                                     &proof));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 2).status, CBM_RESOLVE_TYPE_UNKNOWN);
    proof = type_proof("project.Widget");
    proof.facts &= ~CBM_RESOLVE_FACT_TYPE_BOUND;
    ASSERT(cbm_resolve_types_assign(&map, scope, "item", 3, type, CBM_RESOLVE_TYPE_RETURN, &proof));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 4).status, CBM_RESOLVE_TYPE_UNKNOWN);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_union_and_unknown_branch_abstain) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    const CBMType *type = cbm_type_named(&arena, "project.Widget");
    const CBMType *members[] = {type, cbm_type_unknown()};
    /* Preserve the real input arms. The legacy LSP constructor drops UNKNOWN
     * and would erase the very uncertainty this resolver must retain. */
    const CBMType both = {.kind = CBM_TYPE_UNION,
                          .data.union_type = {.members = members, .count = 2}};
    cbm_resolve_result_t proof = type_proof("project.Widget");
    ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, &both, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT(cbm_resolve_types_lookup(&map, scope, "item", 2).status != CBM_RESOLVE_TYPE_KNOWN);
    ASSERT(cbm_resolve_types_declare(&map, scope, "known", 3, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    cbm_resolve_type_fact_t arms[2] = {
        cbm_resolve_types_lookup(&map, scope, "known", 4),
        cbm_resolve_types_lookup(&map, scope, "missing", 4),
    };
    ASSERT_EQ(cbm_resolve_types_join(&map, arms, 2, 5).status, CBM_RESOLVE_TYPE_UNKNOWN);
    ASSERT(cbm_resolve_types_merge(&map, scope, "joined", 5, arms, 2, true));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "joined", 6).status, CBM_RESOLVE_TYPE_UNKNOWN);
    ASSERT(cbm_type_is_unknown(cbm_scope_lookup(scope, "joined")));
    arms[1] = arms[0];
    ASSERT_EQ(cbm_resolve_types_join(&map, arms, 2, 5).status, CBM_RESOLVE_TYPE_KNOWN);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_rejected_observation_preserves_hybrid_scope) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    cbm_resolve_result_t proof = type_proof("project.Widget");
    const CBMType *type = cbm_type_slice(&arena, cbm_type_named(&arena, proof.target));
    cbm_scope_bind_callable(scope, "item", type, "project.make");
    ASSERT(cbm_resolve_types_observe(&map, scope, "item", 1, type,
                                     CBM_RESOLVE_TYPE_OBSERVED_BINDING, &proof));
    cbm_resolve_type_fact_t fact = cbm_resolve_types_lookup(&map, scope, "item", 2);
    ASSERT_EQ(fact.status, CBM_RESOLVE_TYPE_UNSUPPORTED);
    ASSERT(cbm_type_is_unknown(fact.type));
    ASSERT(cbm_scope_lookup(scope, "item") == type);
    ASSERT_STR_EQ(cbm_scope_lookup_callable(scope, "item"), "project.make");
    proof.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
    ASSERT(cbm_resolve_types_observe(&map, scope, "item", 3, type,
                                     CBM_RESOLVE_TYPE_OBSERVED_BINDING, &proof));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 4).status, CBM_RESOLVE_TYPE_UNKNOWN);
    ASSERT(cbm_scope_lookup(scope, "item") == type);
    ASSERT_STR_EQ(cbm_scope_lookup_callable(scope, "item"), "project.make");
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_distinct_branch_types_are_ambiguous) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    cbm_resolve_result_t first = type_proof("project.First");
    cbm_resolve_result_t second = type_proof("project.Second");
    ASSERT(cbm_resolve_types_declare(&map, scope, "first", 1, cbm_type_named(&arena, first.target),
                                     CBM_RESOLVE_TYPE_RETURN, &first));
    ASSERT(cbm_resolve_types_declare(&map, scope, "second", 2,
                                     cbm_type_named(&arena, second.target), CBM_RESOLVE_TYPE_RETURN,
                                     &second));
    cbm_resolve_type_fact_t arms[] = {
        cbm_resolve_types_lookup(&map, scope, "first", 3),
        cbm_resolve_types_lookup(&map, scope, "second", 3),
    };
    ASSERT_EQ(cbm_resolve_types_join(&map, arms, 2, 4).status, CBM_RESOLVE_TYPE_AMBIGUOUS);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_unknown_effect_invalidates_captured_bindings) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *outer = cbm_scope_push(&arena, NULL);
    CBMScope *inner = cbm_scope_push(&arena, outer);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    cbm_resolve_result_t proof = type_proof("project.Widget");
    const CBMType *type = cbm_type_named(&arena, proof.target);
    ASSERT(cbm_resolve_types_declare(&map, outer, "captured", 1, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT(cbm_resolve_types_declare(&map, inner, "local", 2, type, CBM_RESOLVE_TYPE_CONSTRUCTOR,
                                     &proof));
    cbm_resolve_types_invalidate_visible(&map, inner, 3, CBM_RESOLVE_TYPE_UNSUPPORTED);
    ASSERT_EQ(cbm_resolve_types_lookup(&map, inner, "captured", 4).status,
              CBM_RESOLVE_TYPE_UNSUPPORTED);
    ASSERT_EQ(cbm_resolve_types_lookup(&map, inner, "local", 4).status,
              CBM_RESOLVE_TYPE_UNSUPPORTED);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_hybrid_observation_and_untracked_rebind) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    cbm_resolve_result_t proof = type_proof("project.Widget");
    const CBMType *type = cbm_type_named(&arena, proof.target);
    cbm_scope_bind(scope, "item", type);
    ASSERT(cbm_resolve_types_observe(&map, scope, "item", 1, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 2).status, CBM_RESOLVE_TYPE_KNOWN);
    cbm_scope_bind(scope, "item", cbm_type_unknown());
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 3).status, CBM_RESOLVE_TYPE_STALE);
    ASSERT(!cbm_resolve_types_observe(&map, scope, "item", 4, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                      &proof));
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_budget_and_binding_failure_fail_closed) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *outer = cbm_scope_push(&arena, NULL);
    cbm_resolve_result_t proof = type_proof("project.Widget");
    const CBMType *type = cbm_type_named(&arena, proof.target);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 1);
    ASSERT(!cbm_resolve_types_declare(&map, outer, "item", 1, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                      &proof));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, outer, "item", 2).status, CBM_RESOLVE_TYPE_LIMITED);
    cbm_resolve_types_init(&map, &arena, 0);
    ASSERT(cbm_resolve_types_declare(&map, outer, "item", 1, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    CBMScope exhausted_child = {.parent = outer};
    ASSERT(!cbm_resolve_types_declare(&map, &exhausted_child, "item", 2, type,
                                      CBM_RESOLVE_TYPE_ANNOTATION, &proof));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, &exhausted_child, "item", 3).status,
              CBM_RESOLVE_TYPE_FAILED);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_out_of_order_write_invalidates_map) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    cbm_resolve_result_t proof = type_proof("project.Widget");
    const CBMType *type = cbm_type_named(&arena, proof.target);
    ASSERT(cbm_resolve_types_declare(&map, scope, "item", 10, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT(!cbm_resolve_types_assign(&map, scope, "item", 9, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT_EQ(cbm_resolve_types_lookup(&map, scope, "item", 11).status, CBM_RESOLVE_TYPE_STALE);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_constructor_extraction_retains_unproven_kind_and_span) {
    const char *source = "function make() { let exact = new Widget(); let hint = Widget(); }";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_JAVASCRIPT,
                                             "test", "example.js", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    int constructors = 0;
    int hints = 0;
    for (int i = 0; i < result->type_assigns.count; i++) {
        CBMTypeAssign *fact = &result->type_assigns.items[i];
        ASSERT(fact->site_end_byte > fact->site_start_byte);
        ASSERT(fact->value_end_byte > fact->value_start_byte);
        ASSERT_EQ(fact->source_origin, CBM_SOURCE_ORIGIN_RAW);
        if (strcmp(fact->var_name, "exact") == 0) {
            ASSERT_EQ(fact->kind, CBM_TYPE_ASSIGN_CONSTRUCTOR_SYNTAX);
            ASSERT_EQ(fact->value_start_byte, (uint32_t)(strstr(source, "new Widget") - source));
            constructors++;
        } else if (strcmp(fact->var_name, "hint") == 0) {
            ASSERT_EQ(fact->kind, CBM_TYPE_ASSIGN_NAMING_HINT);
            hints++;
        }
    }
    ASSERT_EQ(constructors, 1);
    ASSERT_EQ(hints, 1);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_type_annotation_extraction_retains_reference_span) {
    const char *source = "def use(value: Widget) -> Result:\n    return value\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "test",
                                             "example.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    int found = 0;
    for (int i = 0; i < result->type_refs.count; i++) {
        CBMTypeRef *fact = &result->type_refs.items[i];
        if (strcmp(fact->type_name, "Widget") == 0) {
            ASSERT(fact->site_is_exact);
            ASSERT_EQ(fact->site_start_byte, (uint32_t)(strstr(source, "Widget") - source));
            ASSERT_EQ(fact->site_end_byte - fact->site_start_byte, strlen("Widget"));
            found++;
        }
    }
    ASSERT_EQ(found, 1);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_type_adapted_snapshot_cannot_hide_revocation_or_map_reinitialization) {
    for (int reinitialize = 0; reinitialize < 2; reinitialize++) {
        CBMArena arena;
        cbm_arena_init(&arena);
        CBMScope *scope = cbm_scope_push(&arena, NULL);
        CBMResolveTypeMap map;
        cbm_resolve_types_init(&map, &arena, 0);
        cbm_resolve_result_t proof = type_proof("project.Widget");
        const CBMType *type = cbm_type_named(&arena, proof.target);
        ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                         &proof));
        cbm_resolve_type_fact_t cached = cbm_resolve_types_lookup(&map, scope, "item", 2);
        cached.binding = NULL;
        if (reinitialize) {
            cbm_resolve_types_init(&map, &arena, 0);
            ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, type,
                                             CBM_RESOLVE_TYPE_ANNOTATION, &proof));
        } else {
            proof.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
            ASSERT(cbm_resolve_types_observe(&map, scope, "item", 3, type,
                                             CBM_RESOLVE_TYPE_OBSERVED_BINDING, &proof));
        }
        ASSERT(cbm_scope_lookup(scope, "item") == type);
        ASSERT_EQ(cbm_resolve_types_join(&map, &cached, 1, 4).status, CBM_RESOLVE_TYPE_STALE);
        cbm_arena_destroy(&arena);
    }
    PASS();
}

TEST(resolver_type_cached_copy_survives_table_growth_and_source_reassignment) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    cbm_resolve_result_t proof = type_proof("project.Widget");
    const CBMType *type = cbm_type_named(&arena, proof.target);
    ASSERT(cbm_resolve_types_declare(&map, scope, "item", 1, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &proof));
    ASSERT(cbm_resolve_types_copy(&map, scope, "copy", scope, "item", 2, true));
    cbm_resolve_type_fact_t cached = cbm_resolve_types_lookup(&map, scope, "copy", 3);
    for (int i = 0; i < 64; i++) {
        const char *name = cbm_arena_sprintf(&arena, "other_%d", i);
        ASSERT_NOT_NULL(name);
        ASSERT(cbm_resolve_types_declare(&map, scope, name, 4, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                         &proof));
    }
    ASSERT(cbm_resolve_types_invalidate(&map, scope, "item", 5, CBM_RESOLVE_TYPE_UNKNOWN));
    ASSERT_EQ(cbm_resolve_types_join(&map, &cached, 1, 6).status, CBM_RESOLVE_TYPE_KNOWN);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_copy_and_join_preserve_actual_support_dag) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *scope = cbm_scope_push(&arena, NULL);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    char content[] = "captured-source";
    cbm_resolve_proof_source_t source = {.module = "project.module",
                                         .content_identity = content,
                                         .configuration_identity = "config-v1"};
    ASSERT(cbm_resolve_types_set_context(&map, &source));
    content[0] = 'X';
    const CBMType *type = cbm_type_named(&arena, "project.Widget");
    cbm_resolve_result_t evidence = type_proof("project.Widget");
    ASSERT(cbm_resolve_types_declare(&map, scope, "first", 10, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &evidence));
    ASSERT(cbm_resolve_types_declare(&map, scope, "second", 20, type, CBM_RESOLVE_TYPE_CONSTRUCTOR,
                                     &evidence));
    cbm_resolve_type_fact_t arms[] = {cbm_resolve_types_lookup(&map, scope, "first", 30),
                                      cbm_resolve_types_lookup(&map, scope, "second", 30)};
    ASSERT_NOT_NULL(arms[0].evidence.proof);
    ASSERT_NOT_NULL(arms[1].evidence.proof);
    ASSERT(arms[0].evidence.proof != arms[1].evidence.proof);
    ASSERT_EQ(arms[0].evidence.proof->value.kind, CBM_PROOF_TYPE_BIND);
    ASSERT_EQ(arms[0].evidence.proof->value.premises[0]->value.kind, CBM_PROOF_ADAPTER_TYPE);
    ASSERT_STR_EQ(arms[0].evidence.proof->value.source.content_identity, "captured-source");
    ASSERT(cbm_resolve_types_copy(&map, scope, "copied", scope, "first", 30, true));
    cbm_resolve_type_fact_t copied = cbm_resolve_types_lookup(&map, scope, "copied", 30);
    ASSERT_EQ(copied.evidence.proof->value.kind, CBM_PROOF_TYPE_COPY);
    ASSERT_EQ(copied.evidence.proof->value.premise_count, 1);
    ASSERT(copied.evidence.proof->value.premises[0] == arms[0].evidence.proof);
    cbm_resolve_type_fact_t joined = cbm_resolve_types_join(&map, arms, 2, 40);
    ASSERT_EQ(joined.status, CBM_RESOLVE_TYPE_KNOWN);
    ASSERT_EQ(joined.evidence.proof->value.kind, CBM_PROOF_TYPE_JOIN);
    ASSERT_EQ(joined.evidence.proof->value.premise_count, 2);
    ASSERT(joined.evidence.proof != arms[0].evidence.proof);
    cbm_resolve_type_fact_t reversed[] = {arms[1], arms[0]};
    ASSERT(cbm_resolve_types_join(&map, reversed, 2, 40).evidence.proof == joined.evidence.proof);
    ASSERT(cbm_resolve_types_invalidate(&map, scope, "second", 50, CBM_RESOLVE_TYPE_UNKNOWN));
    cbm_resolve_type_fact_t failed = cbm_resolve_types_join(&map, arms, 2, 60);
    ASSERT_EQ(failed.status, CBM_RESOLVE_TYPE_STALE);
    ASSERT(failed.evidence.proof == NULL);
    ASSERT(cbm_resolve_types_lookup(&map, scope, "second", 60).evidence.proof == NULL);
    ASSERT_EQ(cbm_resolve_types_fact_status(&joined), CBM_RESOLVE_TYPE_KNOWN);
    ASSERT(!cbm_resolve_types_set_context(&map, &source));
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_type_equal_offset_shadow_bindings_have_distinct_identities) {
    CBMArena arena;
    cbm_arena_init(&arena);
    CBMScope *outer = cbm_scope_push(&arena, NULL);
    CBMScope *inner = cbm_scope_push(&arena, outer);
    CBMResolveTypeMap map;
    cbm_resolve_types_init(&map, &arena, 0);
    const CBMType *type = cbm_type_named(&arena, "project.Widget");
    cbm_resolve_result_t evidence = type_proof("project.Widget");
    ASSERT(cbm_resolve_types_declare(&map, outer, "same", 0, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &evidence));
    ASSERT(cbm_resolve_types_declare(&map, inner, "same", 0, type, CBM_RESOLVE_TYPE_ANNOTATION,
                                     &evidence));
    cbm_resolve_type_fact_t a = cbm_resolve_types_lookup(&map, outer, "same", 0);
    cbm_resolve_type_fact_t b = cbm_resolve_types_lookup(&map, inner, "same", 0);
    ASSERT_NOT_NULL(a.evidence.proof);
    ASSERT_NOT_NULL(b.evidence.proof);
    ASSERT(a.evidence.proof != b.evidence.proof);
    ASSERT(strcmp(a.evidence.proof->value.key.binding_identity,
                  b.evidence.proof->value.key.binding_identity) != 0);
    ASSERT(a.evidence.proof->value.source.content_identity == NULL);
    cbm_arena_destroy(&arena);
    PASS();
}

SUITE(resolver_types) {
    RUN_TEST(resolver_type_copy_and_join_preserve_actual_support_dag);
    RUN_TEST(resolver_type_equal_offset_shadow_bindings_have_distinct_identities);
    RUN_TEST(resolver_type_adapted_snapshot_cannot_hide_revocation_or_map_reinitialization);
    RUN_TEST(resolver_type_cached_copy_survives_table_growth_and_source_reassignment);
    RUN_TEST(resolver_type_exact_annotation_and_copy);
    RUN_TEST(resolver_type_unknown_shadow_blocks_parent);
    RUN_TEST(resolver_type_unknown_assignment_versions_and_clears_callable);
    RUN_TEST(resolver_type_incomplete_binding_is_not_evidence);
    RUN_TEST(resolver_type_union_and_unknown_branch_abstain);
    RUN_TEST(resolver_type_rejected_observation_preserves_hybrid_scope);
    RUN_TEST(resolver_type_distinct_branch_types_are_ambiguous);
    RUN_TEST(resolver_type_unknown_effect_invalidates_captured_bindings);
    RUN_TEST(resolver_type_hybrid_observation_and_untracked_rebind);
    RUN_TEST(resolver_type_budget_and_binding_failure_fail_closed);
    RUN_TEST(resolver_type_out_of_order_write_invalidates_map);
    RUN_TEST(resolver_type_constructor_extraction_retains_unproven_kind_and_span);
    RUN_TEST(resolver_type_annotation_extraction_retains_reference_span);
}
