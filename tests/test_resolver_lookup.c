#include "test_framework.h"
#include "cbm.h"
#include "pipeline/pipeline_internal.h"
#include "resolve/binding.h"
#include "resolve/lookup.h"
#include "resolve/policy.h"

#include <string.h>

typedef struct {
    bool captured, absent, closed_absent, found, imported, ambiguous;
} lookup_seen_t;

static bool lookup_visit(const cbm_resolve_proof_t *proof, void *context) {
    lookup_seen_t *seen = context;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof);
    const char *detail = view->key.detail ? view->key.detail : "";
    seen->captured |= view->kind == CBM_PROOF_SOURCE_FACT && view->source.content_identity &&
                      view->source.content_identity[0];
    seen->absent |= strstr(detail, "absent") != NULL;
    seen->closed_absent |= view->key.lookup_closed && strstr(detail, "absent") != NULL;
    seen->found |= strstr(detail, "observed") != NULL;
    seen->imported |= !strcmp(detail, "import_key_target_observed");
    seen->ambiguous |= view->outcome.binding == CBM_RESOLVE_BINDING_AMBIGUOUS;
    return true;
}

static cbm_resolve_proof_source_t lookup_source(void) {
    return (cbm_resolve_proof_source_t){
        .project = "project",
        .file = "main.py",
        .module = "project.main",
        .scope = "project.main",
        .content_identity = "9b20f13c568749e8531a9777123a60c98719b311c371314a40a1e4c9d2ba65fb",
        .configuration_identity = "test_configuration",
        .source_origin = CBM_SOURCE_ORIGIN_RAW};
}

TEST(resolver_lookup_missing_source_cannot_certify_success) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_lookup_context_t context;
    cbm_resolve_proof_source_t source = lookup_source();
    source.content_identity = NULL;
    ASSERT(cbm_resolve_lookup_context_init(&context, &pool, source));
    cbm_resolve_lookup_t query;
    cbm_resolve_lookup_begin(&query, &context, "registry.exact", "project.target", 0, 0, 0);
    ASSERT(cbm_resolve_lookup_observe(&query, "registry.exact", "project.target", "project.target",
                                      CBM_RESOLVE_BINDING_UNIQUE, CBM_RESOLVE_ANALYSIS_COMPLETE, 0,
                                      true, "exact_key_observed"));
    cbm_resolve_result_t known = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .target = "project.target",
                                  .candidate_count = 1};
    const cbm_resolve_proof_t *proof =
        cbm_resolve_lookup_finish(&query, CBM_PROOF_REGISTRY_LOOKUP, known);
    ASSERT_NOT_NULL(proof);
    ASSERT_EQ(cbm_resolve_proof_view(proof)->outcome.analysis, CBM_RESOLVE_ANALYSIS_UNKNOWN);
    ASSERT_NULL(cbm_resolve_proof_view(proof)->outcome.target);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_lookup_registry_keeps_positive_and_negative_dependencies) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_lookup_context_t context;
    ASSERT(cbm_resolve_lookup_context_init(&context, &pool, lookup_source()));
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(registry);
    cbm_registry_add(registry, "run", "project.lib.run", "Function");
    const char *keys[] = {"alias"}, *values[] = {"project.lib"};
    cbm_resolution_t result =
        cbm_registry_resolve_observed(registry, "alias.run", "project.main", keys, values, 1,
                                      CBM_LANG_PYTHON, "project.main.caller", true, &context);
    ASSERT_STR_EQ(result.qualified_name, "project.lib.run");
    ASSERT_NOT_NULL(result.proof.proof);
    ASSERT(!cbm_resolve_admit(&result.proof)); /* lexical validation is still required */
    lookup_seen_t seen = {0};
    ASSERT(cbm_resolve_proof_visit(result.proof.proof, 64, lookup_visit, &seen));
    ASSERT(seen.captured);
    ASSERT(seen.absent); /* alias.run import-key probe precedes alias */
    ASSERT(seen.closed_absent);
    ASSERT(seen.imported);
    ASSERT(seen.found);
    cbm_resolution_t absent =
        cbm_registry_resolve_observed(registry, "missing", "project.main", keys, values, 1,
                                      CBM_LANG_PYTHON, "project.main", false, &context);
    ASSERT_NULL(absent.qualified_name);
    ASSERT_NOT_NULL(absent.proof.proof);
    ASSERT_EQ(cbm_resolve_proof_view(absent.proof.proof)->outcome.analysis,
              CBM_RESOLVE_ANALYSIS_UNKNOWN);
    seen = (lookup_seen_t){0};
    ASSERT(cbm_resolve_proof_visit(absent.proof.proof, 64, lookup_visit, &seen));
    ASSERT(seen.absent);
    cbm_registry_free(registry);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_lookup_registry_ambiguous_import_is_retained) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_lookup_context_t context;
    ASSERT(cbm_resolve_lookup_context_init(&context, &pool, lookup_source()));
    cbm_registry_t *registry = cbm_registry_new();
    ASSERT_NOT_NULL(registry);
    const char *keys[] = {"alias", "alias"}, *values[] = {"project.a", "project.b"};
    cbm_resolution_t result =
        cbm_registry_resolve_observed(registry, "alias", "project.main", keys, values, 2,
                                      CBM_LANG_PYTHON, "project.main", false, &context);
    ASSERT_EQ(result.proof.binding, CBM_RESOLVE_BINDING_AMBIGUOUS);
    ASSERT_NULL(result.qualified_name);
    lookup_seen_t seen = {0};
    ASSERT(cbm_resolve_proof_visit(result.proof.proof, 64, lookup_visit, &seen));
    ASSERT(seen.ambiguous);
    cbm_registry_free(registry);
    cbm_arena_destroy(&arena);
    PASS();
}

TEST(resolver_lookup_binding_captures_site_and_preserves_shared_root) {
    const char *source = "from lib import target as alias\ndef caller():\n    return alias()\n";
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON,
                                             "project", "main.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(result);
    const CBMCall *call = NULL;
    for (int i = 0; i < result->calls.count; i++)
        if (!strcmp(result->calls.items[i].callee_name, "alias"))
            call = &result->calls.items[i];
    ASSERT_NOT_NULL(call);
    CBMArena arena, shared_arena;
    cbm_arena_init(&arena);
    cbm_arena_init(&shared_arena);
    cbm_resolve_binding_index_t *index =
        cbm_resolve_binding_build(&arena, result, CBM_LANG_PYTHON, source, strlen(source));
    ASSERT_NOT_NULL(index);
    ASSERT(cbm_resolve_binding_set_identity(index, "project", "main.py"));
    cbm_resolve_proof_pool_t shared_pool;
    cbm_resolve_proof_pool_init(&shared_pool, &shared_arena, 0, 0, 0);
    cbm_resolve_proof_source_t captured = lookup_source();
    cbm_resolve_proof_key_t key = {.lookup_domain = "project.lib", .lookup_key = "target"};
    cbm_resolve_result_t candidate = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                               CBM_RESOLVE_FACT_IMPORT_BOUND |
                                               CBM_RESOLVE_FACT_LOOKUP_COMPLETE,
                                      .target = "project.lib.target",
                                      .candidate_count = 1};
    const cbm_resolve_proof_t *shared =
        cbm_resolve_proof_leaf(&shared_pool, CBM_PROOF_ADAPTER_DOMAIN, &captured, &key, &candidate);
    ASSERT_NOT_NULL(shared);
    candidate.proof = shared;
    cbm_resolve_result_t resolved = cbm_resolve_binding_validate(
        index, "alias", call->site_start_byte, call->site_end_byte, call->source_origin, candidate);
    ASSERT(cbm_resolve_admit(&resolved));
    ASSERT(resolved.proof == shared);
    const cbm_resolve_proof_t *local = cbm_resolve_binding_site_proof(
        index, "alias", call->site_start_byte, call->site_end_byte, call->source_origin);
    ASSERT_NOT_NULL(local);
    ASSERT(local != shared);
    ASSERT_STR_EQ(cbm_resolve_proof_view(local)->source.file, "main.py");
    ASSERT_STR_EQ(cbm_resolve_proof_view(local)->source.content_identity,
                  result->source_content_id[0]);
    ASSERT_NULL(cbm_resolve_binding_site_proof(index, "different", call->site_start_byte,
                                               call->site_end_byte, call->source_origin));
    cbm_arena_destroy(&arena);
    cbm_arena_destroy(&shared_arena);
    cbm_free_result(result);
    PASS();
}

TEST(resolver_lookup_closure_is_part_of_full_fact_identity) {
    CBMArena arena;
    cbm_arena_init(&arena);
    cbm_resolve_proof_pool_t pool;
    cbm_resolve_proof_pool_init(&pool, &arena, 0, 0, 0);
    cbm_resolve_proof_source_t source = lookup_source();
    cbm_resolve_proof_key_t key = {
        .lookup_domain = "registry.exact", .lookup_key = "missing", .lookup_closed = true};
    cbm_resolve_result_t outcome = {.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN};
    const cbm_resolve_proof_t *closed =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_ADAPTER_DOMAIN, &source, &key, &outcome);
    key.lookup_closed = false;
    const cbm_resolve_proof_t *open =
        cbm_resolve_proof_leaf(&pool, CBM_PROOF_ADAPTER_DOMAIN, &source, &key, &outcome);
    ASSERT_NOT_NULL(closed);
    ASSERT_NOT_NULL(open);
    ASSERT(closed != open);
    ASSERT(cbm_resolve_proof_compare(closed, open) != 0);
    cbm_resolve_proof_t collision = *open;
    collision.value.fingerprint = closed->value.fingerprint;
    ASSERT(cbm_resolve_proof_compare(closed, &collision) != 0);
    cbm_arena_destroy(&arena);
    PASS();
}

SUITE(resolver_lookup) {
    RUN_TEST(resolver_lookup_closure_is_part_of_full_fact_identity);
    RUN_TEST(resolver_lookup_missing_source_cannot_certify_success);
    RUN_TEST(resolver_lookup_registry_keeps_positive_and_negative_dependencies);
    RUN_TEST(resolver_lookup_registry_ambiguous_import_is_retained);
    RUN_TEST(resolver_lookup_binding_captures_site_and_preserves_shared_root);
}
