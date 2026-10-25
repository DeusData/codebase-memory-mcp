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
    ASSERT(cbm_resolve_capabilities(CBM_LANG_SWIFT, &capability));
    ASSERT_EQ(capability.proof_subsets, 0);
    ASSERT(cbm_resolve_capabilities(CBM_LANG_LUA, &capability));
    ASSERT_EQ(capability.proof_subsets, CBM_RESOLVE_CAP_SCOPE);
    ASSERT(cbm_resolve_capabilities(CBM_LANG_DART, &capability));
    ASSERT(!(capability.proof_subsets & CBM_RESOLVE_CAP_TYPE_FLOW));
    PASS();
}
SUITE(resolver_capabilities) {
    RUN_TEST(resolver_capabilities_cover_every_language_without_claiming_completeness);
    RUN_TEST(resolver_capability_is_never_an_occurrence_proof);
}
