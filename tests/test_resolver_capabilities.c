#include "test_framework.h"
#include "resolve/capabilities.h"
#include "resolve/policy.h"

TEST(resolver_capabilities_cover_every_language_without_claiming_completeness) {
    for (int i = 0; i < CBM_LANG_COUNT; i++) {
        cbm_resolve_capabilities_t capability;
        ASSERT(cbm_resolve_capabilities((CBMLanguage)i, &capability));
        ASSERT_EQ(capability.language, i);
        ASSERT_NOT_NULL(capability.adapter);
        ASSERT_NOT_NULL(capability.limitation);
        ASSERT(strlen(capability.limitation) > 0);
    }
    cbm_resolve_capabilities_t invalid;
    ASSERT(!cbm_resolve_capabilities((CBMLanguage)-1, &invalid));
    ASSERT(!cbm_resolve_capabilities(CBM_LANG_COUNT, &invalid));
    PASS();
}
TEST(resolver_capability_is_never_an_occurrence_proof) {
    cbm_resolve_capabilities_t capability;
    ASSERT(cbm_resolve_capabilities(CBM_LANG_GO, &capability));
    ASSERT(capability.proof_subsets & CBM_RESOLVE_CAP_MEMBER);
    cbm_resolve_result_t missing = {0};
    ASSERT(!cbm_resolve_admit(&missing));
    ASSERT(cbm_resolve_capabilities(CBM_LANG_JULIA, &capability));
    ASSERT_EQ(capability.proof_subsets, 0);
    ASSERT(cbm_resolve_capabilities(CBM_LANG_LUA, &capability));
    ASSERT_EQ(capability.proof_subsets, CBM_RESOLVE_CAP_SCOPE);
    ASSERT(cbm_resolve_capabilities(CBM_LANG_DART, &capability));
    ASSERT(!(capability.proof_subsets & CBM_RESOLVE_CAP_TYPE_FLOW));
    PASS();
}
static const char *const capability_relations[] = {"IMPORTS", "CALLS",  "CALL_REFERENCE", "USAGE",
                                                   "READS",   "WRITES", "USES_TYPE"};

TEST(resolver_capabilities_cover_every_relation_for_every_language) {
    ASSERT_EQ(CBM_RESOLVE_EDGE_COUNT,
              sizeof(capability_relations) / sizeof(capability_relations[0]));
    for (int language = 0; language < CBM_LANG_COUNT; language++) {
        cbm_resolve_capabilities_t capability;
        ASSERT(cbm_resolve_capabilities((CBMLanguage)language, &capability));
        for (int relation = 0; relation < CBM_RESOLVE_EDGE_COUNT; relation++) {
            const cbm_resolve_edge_capability_t *entry = &capability.edges[relation];
            cbm_resolve_edge_capability_t queried;
            ASSERT_NOT_NULL(entry->relation);
            ASSERT_EQ(strcmp(entry->relation, capability_relations[relation]), 0);
            ASSERT(entry->availability == CBM_RESOLVE_EDGE_UNSUPPORTED ||
                   entry->availability == CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
            ASSERT_NOT_NULL(entry->limitation);
            ASSERT(entry->limitation[0]);
            ASSERT(cbm_resolve_edge_capability((CBMLanguage)language, entry->relation, &queried));
            ASSERT_EQ(queried.availability, entry->availability);
            ASSERT_EQ(strcmp(queried.relation, entry->relation), 0);
            ASSERT_EQ(strcmp(queried.limitation, entry->limitation), 0);
        }
    }
    PASS();
}

TEST(resolver_capabilities_report_supported_subsets_and_quality_gaps) {
    cbm_resolve_edge_capability_t edge;
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_GO, "CALLS", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_GO, "CALL_REFERENCE", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_GO, "WRITES", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_GO, "USES_TYPE", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_GO, "READS", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_UNSUPPORTED);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_PYTHON, "WRITES", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_DART, "IMPORTS", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_DART, "CALLS", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_edge_capability(CBM_LANG_DART, "WRITES", &edge));
    ASSERT_EQ(edge.availability, CBM_RESOLVE_EDGE_UNSUPPORTED);
    const CBMLanguage unsupported[] = {CBM_LANG_JULIA, CBM_LANG_JSON};
    for (size_t i = 0; i < sizeof(unsupported) / sizeof(unsupported[0]); i++) {
        cbm_resolve_capabilities_t capability;
        ASSERT(cbm_resolve_capabilities(unsupported[i], &capability));
        for (int relation = 0; relation < CBM_RESOLVE_EDGE_COUNT; relation++)
            ASSERT_EQ(capability.edges[relation].availability, CBM_RESOLVE_EDGE_UNSUPPORTED);
    }
    /* Syntax extraction and legacy semantic engines cannot enable a relation
     * without an admitted producer. Missing coverage remains a quality gap. */
    cbm_resolve_capabilities_t julia;
    ASSERT(cbm_resolve_capabilities(CBM_LANG_JULIA, &julia));
    ASSERT(julia.syntax_declarations);
    ASSERT_EQ(julia.edges[CBM_RESOLVE_EDGE_CALLS].availability, CBM_RESOLVE_EDGE_UNSUPPORTED);
    PASS();
}

TEST(resolver_capability_relation_query_rejects_invalid_input) {
    cbm_resolve_edge_capability_t edge;
    ASSERT(!cbm_resolve_edge_capability((CBMLanguage)-1, "CALLS", &edge));
    ASSERT(!cbm_resolve_edge_capability(CBM_LANG_COUNT, "CALLS", &edge));
    ASSERT(!cbm_resolve_edge_capability(CBM_LANG_GO, NULL, &edge));
    ASSERT(!cbm_resolve_edge_capability(CBM_LANG_GO, "", &edge));
    ASSERT(!cbm_resolve_edge_capability(CBM_LANG_GO, "calls", &edge));
    ASSERT(!cbm_resolve_edge_capability(CBM_LANG_GO, "CONTAINS", &edge));
    ASSERT(!cbm_resolve_edge_capability(CBM_LANG_GO, "CALLS", NULL));
    PASS();
}

TEST(resolver_capabilities_name_the_source_adapters_that_produce_calls) {
    static const struct {
        CBMLanguage language;
        const char *adapter;
    } rows[] = {{CBM_LANG_PHP, "php_source"},       {CBM_LANG_ELIXIR, "elixir_source"},
                {CBM_LANG_ERLANG, "erlang_source"}, {CBM_LANG_ZIG, "zig_source"},
                {CBM_LANG_PERL, "perl_source"},     {CBM_LANG_RUBY, "ruby_source"}};
    for (size_t i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        cbm_resolve_capabilities_t capability;
        ASSERT(cbm_resolve_capabilities(rows[i].language, &capability));
        ASSERT_STR_EQ(capability.adapter, rows[i].adapter);
        ASSERT(capability.proof_subsets & CBM_RESOLVE_CAP_SCOPE);
        ASSERT_EQ(capability.edges[CBM_RESOLVE_EDGE_CALLS].availability,
                  CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    }
    PASS();
}

TEST(resolver_capabilities_match_existing_bounded_producers) {
    cbm_resolve_capabilities_t capability;
    ASSERT(cbm_resolve_capabilities(CBM_LANG_KOTLIN, &capability));
    ASSERT_EQ(capability.edges[CBM_RESOLVE_EDGE_USAGE].availability,
              CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT(cbm_resolve_capabilities(CBM_LANG_K8S, &capability));
    ASSERT_STR_EQ(capability.adapter, "kubernetes_infrastructure");
    ASSERT(capability.proof_subsets & CBM_RESOLVE_CAP_SCOPE);
    ASSERT_EQ(capability.edges[CBM_RESOLVE_EDGE_USAGE].availability,
              CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
    ASSERT_EQ(capability.edges[CBM_RESOLVE_EDGE_CALLS].availability, CBM_RESOLVE_EDGE_UNSUPPORTED);
    const CBMLanguage embedded[] = {CBM_LANG_VUE, CBM_LANG_SVELTE};
    for (size_t i = 0; i < sizeof(embedded) / sizeof(embedded[0]); i++) {
        ASSERT(cbm_resolve_capabilities(embedded[i], &capability));
        ASSERT_STR_EQ(capability.adapter, "embedded_javascript_lexical");
        ASSERT_EQ(capability.proof_subsets, CBM_RESOLVE_CAP_SCOPE | CBM_RESOLVE_CAP_CALLABLE);
        ASSERT_EQ(capability.edges[CBM_RESOLVE_EDGE_CALLS].availability,
                  CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
        ASSERT_EQ(capability.edges[CBM_RESOLVE_EDGE_CALL_REFERENCE].availability,
                  CBM_RESOLVE_EDGE_SUPPORTED_SUBSET);
        ASSERT_EQ(capability.edges[CBM_RESOLVE_EDGE_IMPORTS].availability,
                  CBM_RESOLVE_EDGE_UNSUPPORTED);
        ASSERT_NOT_NULL(strstr(capability.limitation, "inline script"));
    }
    ASSERT(cbm_resolve_capabilities(CBM_LANG_YAML, &capability));
    ASSERT_NOT_NULL(strstr(capability.edges[CBM_RESOLVE_EDGE_USAGE].limitation, "Compose"));
    PASS();
}

SUITE(resolver_capabilities) {
    RUN_TEST(resolver_capabilities_name_the_source_adapters_that_produce_calls);
    RUN_TEST(resolver_capabilities_match_existing_bounded_producers);
    RUN_TEST(resolver_capabilities_cover_every_relation_for_every_language);
    RUN_TEST(resolver_capabilities_report_supported_subsets_and_quality_gaps);
    RUN_TEST(resolver_capability_relation_query_rejects_invalid_input);
    RUN_TEST(resolver_capabilities_cover_every_language_without_claiming_completeness);
    RUN_TEST(resolver_capability_is_never_an_occurrence_proof);
}
