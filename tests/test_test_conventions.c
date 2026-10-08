#include "test_framework.h"
#include "test_helpers.h"
#include "../internal/cbm/cbm.h"
#include "../internal/cbm/lsp/c_lsp.h"
#include "../internal/cbm/result_spill.h"
#include <discover/test_conventions.h>

#include <stdint.h>
#include <stdio.h>
#include <string.h>

/* Independent tests of the frozen extractor API. No extractor implementation
 * helpers or implementation drafts are used by these fixtures. */
static int tc_init_status;

#define TC_CASE0 "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0]}"
#define TC_CASE1 "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[1]}"
#define TC_SUITE0 "{\"language\":\"c\",\"role\":\"suite\",\"define_macro\":\"GROUP\",\"name_args\":[0]}"

static bool tc_text(const char *left, const char *right) {
    return left && right ? strcmp(left, right) == 0 : left == right;
}

static cbm_test_declarations_t *tc_config(const char *records, const char *presets) {
    char json[8192];
    int n = snprintf(json, sizeof(json),
        "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[%s],\"presets\":{%s}}}}",
        records ? records : "", presets ? presets : "");
    return n > 0 && (size_t)n < sizeof(json) ?
        cbm_test_declarations_parse(json, (size_t)n, false) : NULL;
}

static CBMFileResult *tc_extract(const char *source, CBMLanguage language, const char *path,
                               const cbm_test_declarations_t *config) {
    return cbm_extract_file_ex_with_tests(source, (int)strlen(source), language,
        "convproj", path, 0, NULL, NULL, NULL, NULL, config);
}

static CBMFileResult *tc_legacy(const char *source, CBMLanguage language, const char *path) {
    return cbm_extract_file_ex(source, (int)strlen(source), language,
        "convproj", path, 0, NULL, NULL, NULL, NULL);
}

static const CBMDefinition *tc_def(const CBMFileResult *result, const char *name) {
    const CBMDefinition *found = NULL;
    if (!result) return NULL;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        if (!tc_text(def->name, name)) continue;
        if (found) return NULL; /* duplicate identities never satisfy a unique lookup */
        found = def;
    }
    return found;
}

static bool tc_ok(const CBMFileResult *result) {
    return result && result->test_declarations_status == CBM_TEST_EXTRACT_OK;
}

static bool tc_role(const CBMFileResult *result, const char *name,
                    CBMTestDefinitionRole role) {
    const CBMDefinition *def = tc_def(result, name);
    char expected[512];
    if (!def || !result->module_qn || def->test_role != role) return false;
    int n = snprintf(expected, sizeof(expected), "%s.%s", result->module_qn, name);
    return n > 0 && (size_t)n < sizeof(expected) && tc_text(def->qualified_name, expected);
}

static bool tc_none(const CBMFileResult *result) {
    if (!tc_ok(result)) return false;
    for (int i = 0; i < result->defs.count; i++)
        if (result->defs.items[i].test_role != CBM_TEST_ROLE_NONE) return false;
    return true;
}

static bool tc_only_case(const CBMFileResult *result, const char *name) {
    if (!tc_ok(result) || !tc_role(result, name, CBM_TEST_ROLE_CASE)) return false;
    int cases = 0;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        if (def->test_role == CBM_TEST_ROLE_NONE) continue;
        if (def->test_role != CBM_TEST_ROLE_CASE || !tc_text(def->name, name)) return false;
        cases++;
    }
    return cases == 1;
}

/* Compare the existing definition/carrier identities, not raw pointer bytes.
 * The new scalar has to be zero on both paths. This is an extraction oracle,
 * not a claim to serialize or certify a complete pipeline graph. */
static bool tc_legacy_equal(const CBMFileResult *left, const CBMFileResult *right) {
    if (!tc_none(left) || !tc_none(right) || !tc_text(left->module_qn, right->module_qn) ||
        left->has_error != right->has_error || left->is_test_file != right->is_test_file ||
        left->defs.count != right->defs.count || left->calls.count != right->calls.count ||
        left->usages.count != right->usages.count) return false;
    for (int i = 0; i < left->defs.count; i++) {
        const CBMDefinition *a = &left->defs.items[i], *b = &right->defs.items[i];
        if (!tc_text(a->name, b->name) || !tc_text(a->qualified_name, b->qualified_name) ||
            !tc_text(a->label, b->label) || !tc_text(a->file_path, b->file_path) ||
            !tc_text(a->signature, b->signature) || a->start_line != b->start_line ||
            a->end_line != b->end_line || a->is_test != b->is_test) return false;
    }
    for (int i = 0; i < left->calls.count; i++) {
        const CBMCall *a = &left->calls.items[i], *b = &right->calls.items[i];
        if (!tc_text(a->callee_name, b->callee_name) ||
            !tc_text(a->enclosing_func_qn, b->enclosing_func_qn) ||
            a->start_line != b->start_line || a->site_start_byte != b->site_start_byte ||
            a->site_end_byte != b->site_end_byte || a->source_origin != b->source_origin)
            return false;
    }
    for (int i = 0; i < left->usages.count; i++) {
        const CBMUsage *a = &left->usages.items[i], *b = &right->usages.items[i];
        if (!tc_text(a->ref_name, b->ref_name) ||
            !tc_text(a->enclosing_func_qn, b->enclosing_func_qn) ||
            a->site_start_byte != b->site_start_byte || a->site_end_byte != b->site_end_byte ||
            a->source_origin != b->source_origin) return false;
    }
    return true;
}

static unsigned tc_line_at(const char *source, uint32_t offset) {
    size_t length = strlen(source);
    if (offset >= length) return 0;
    unsigned line = 1;
    for (uint32_t i = 0; i < offset; i++) if (source[i] == '\n') line++;
    return line;
}

static bool tc_body_owner(const CBMFileResult *result, const char *source,
                          const char *name, unsigned line) {
    const CBMDefinition *def = tc_def(result, name);
    if (!def || !def->qualified_name) return false;
    int calls = 0, usages = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (call->source_origin != CBM_SOURCE_ORIGIN_RAW || call->start_line != (int)line ||
            !tc_text(call->callee_name, "sink")) continue;
        if (!tc_text(call->enclosing_func_qn, def->qualified_name)) return false;
        calls++;
    }
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->source_origin != CBM_SOURCE_ORIGIN_RAW ||
            tc_line_at(source, usage->site_start_byte) != line ||
            !tc_text(usage->ref_name, "payload")) continue;
        if (!tc_text(usage->enclosing_func_qn, def->qualified_name)) return false;
        usages++;
    }
    return calls > 0 && usages > 0;
}

/* A configured form the extractor cannot map is detected with its exact
 * status and location, then degrades the FILE (user decision 2026-10-04):
 * the result reads OK, carries no configured roles, and keeps the issue. */
static bool tc_issue(const CBMFileResult *result, CBMTestExtractStatus status,
                     int index, uint32_t line) {
    return result && result->test_declarations_degraded &&
        result->test_declarations_degraded_status == status &&
        result->test_declarations_status == CBM_TEST_EXTRACT_OK && !result->has_error &&
        (index == -2 ? result->test_declaration_index >= 0 :
                       result->test_declaration_index == index) &&
        result->test_declaration_line == line;
}
static bool tc_degraded(const CBMFileResult *result) {
    return result && result->test_declarations_degraded &&
        result->test_declarations_degraded_status != CBM_TEST_EXTRACT_OK &&
        result->test_declarations_status == CBM_TEST_EXTRACT_OK;
}

TEST(conventions_null_absent_and_defaults_preserve_legacy) {
    ASSERT_EQ(tc_init_status, 0);
    cbm_test_declarations_t *absent = cbm_test_declarations_parse(NULL, 0, true);
    cbm_test_declarations_t *defaults = tc_config(NULL, NULL);
    const char *sources[] = {
        "int payload;\nvoid sink(int x) {}\nvoid helper(void) { sink(payload); }\n",
        "TEST(Group, Alpha) { }\nTEST_F(Fixture, Beta) { }\nvoid helper() {}\n"
    };
    const CBMLanguage languages[] = {CBM_LANG_C, CBM_LANG_CPP};
    const char *paths[] = {"tests/test_defaults.c", "src/defaults.cpp"};
    bool snapshots = absent && defaults;
    bool same = snapshots;
    for (size_t i = 0; i < 2; i++) {
        CBMFileResult *old = tc_legacy(sources[i], languages[i], paths[i]);
        CBMFileResult *nil = tc_extract(sources[i], languages[i], paths[i], NULL);
        CBMFileResult *missing = absent ? tc_extract(sources[i], languages[i], paths[i], absent) : NULL;
        CBMFileResult *configured = defaults ? tc_extract(sources[i], languages[i], paths[i], defaults) : NULL;
        same = old && old->defs.count > 0 && tc_legacy_equal(old, nil) &&
            tc_legacy_equal(old, missing) && tc_legacy_equal(old, configured) && same;
        const CBMDefinition *helper = tc_def(configured, "helper");
        same = helper && helper->test_role == CBM_TEST_ROLE_NONE && same;
        cbm_free_result(old); cbm_free_result(nil);
        cbm_free_result(missing); cbm_free_result(configured);
    }
    cbm_test_declarations_free(absent); cbm_test_declarations_free(defaults);
    ASSERT_TRUE(snapshots);
    ASSERT_TRUE(same);
    PASS();
}

TEST(conventions_names_roles_arguments_and_body_owners) {
    ASSERT_EQ(tc_init_status, 0);
    const char *source =
        "int payload;\nvoid sink(int x) {}\n"
        "CHECK(pair(1, 2), alpha) { sink(payload); }\n"
        "CHECK(\"left,right\", beta) { sink(payload); }\n"
        "GROUP(core) { sink(payload); }\n"
        "void helper(void) { sink(payload); }\n";
    cbm_test_declarations_t *config = tc_config(TC_CASE1 "," TC_SUITE0, NULL);
    bool parsed = config != NULL;
    CBMFileResult *result = config ? tc_extract(source, CBM_LANG_C, "src/conventions.c", config) : NULL;
    cbm_test_declarations_free(config);
    bool roles = tc_ok(result) && tc_role(result, "CHECK_alpha", CBM_TEST_ROLE_CASE) &&
        tc_role(result, "CHECK_beta", CBM_TEST_ROLE_CASE) &&
        tc_role(result, "GROUP_core", CBM_TEST_ROLE_SUITE) &&
        tc_role(result, "helper", CBM_TEST_ROLE_NONE);
    const CBMDefinition *alpha = tc_def(result, "CHECK_alpha"), *beta = tc_def(result, "CHECK_beta");
    const CBMDefinition *suite = tc_def(result, "GROUP_core");
    roles = alpha && beta && suite && alpha->is_test && beta->is_test && !suite->is_test && roles;
    bool owners = tc_body_owner(result, source, "CHECK_alpha", 3) &&
        tc_body_owner(result, source, "CHECK_beta", 4) &&
        tc_body_owner(result, source, "GROUP_core", 5) &&
        tc_body_owner(result, source, "helper", 6);
    cbm_free_result(result);
    ASSERT_TRUE(parsed);
    ASSERT_TRUE(roles);
    ASSERT_TRUE(owners);
    PASS();
}

TEST(conventions_duplicates_coalesce_and_conflicts_fail_typed) {
    ASSERT_EQ(tc_init_status, 0);
    cbm_test_declarations_t *duplicate = tc_config(TC_CASE0 "," TC_CASE0, NULL);
    bool parsed = duplicate != NULL;
    CBMFileResult *result = duplicate ? tc_extract("CHECK(alpha) {}\n", CBM_LANG_C,
        "src/duplicate.c", duplicate) : NULL;
    bool coalesced = tc_ok(result) && tc_role(result, "CHECK_alpha", CBM_TEST_ROLE_CASE);
    cbm_free_result(result); cbm_test_declarations_free(duplicate);
    const char *records[] = {
        TC_CASE0 ",{\"language\":\"c\",\"role\":\"suite\",\"define_macro\":\"CHECK\",\"name_args\":[0]}",
        "{\"language\":\"c\",\"role\":\"suite\",\"define_macro\":\"CHECK\",\"name_args\":[0]}," TC_CASE0,
        TC_CASE0 "," TC_CASE1
    };
    bool rejected = true;
    for (size_t i = 0; i < 3; i++) {
        cbm_test_declarations_t *config = tc_config(records[i], NULL);
        parsed = config && parsed;
        CBMFileResult *first = config ? tc_extract("CHECK(alpha, alpha) {}\n", CBM_LANG_C,
            "src/conflict.c", config) : NULL;
        CBMFileResult *again = config ? tc_extract("CHECK(alpha, alpha) {}\n", CBM_LANG_C,
            "src/conflict.c", config) : NULL;
        bool bound = tc_issue(first, CBM_TEST_EXTRACT_AMBIGUOUS, -2, 1) &&
            first->test_declaration_index < 2 && tc_issue(again, CBM_TEST_EXTRACT_AMBIGUOUS,
            first->test_declaration_index, 1);
        if (!bound) fprintf(stderr, "convention conflict fixture %zu failed\n", i);
        rejected = bound && rejected;
        cbm_free_result(first); cbm_free_result(again); cbm_test_declarations_free(config);
    }
    ASSERT_TRUE(parsed);
    ASSERT_TRUE(coalesced);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(conventions_invalid_invocations_have_exact_typed_locations) {
    ASSERT_EQ(tc_init_status, 0);
    const char *sources[] = {
        "CHECK(only) {}\n", "CHECK(ignored, 123) {}\n",
        "CHECK(ignored, \"title\") {}\n", "CHECK(ignored, alpha + beta) {}\n"
    };
    const CBMTestExtractStatus statuses[] = {CBM_TEST_EXTRACT_MISSING_ARGUMENT,
        CBM_TEST_EXTRACT_UNSUPPORTED_ARGUMENT, CBM_TEST_EXTRACT_UNSUPPORTED_ARGUMENT,
        CBM_TEST_EXTRACT_UNSUPPORTED_ARGUMENT};
    cbm_test_declarations_t *config = tc_config(TC_CASE1, NULL);
    bool parsed = config != NULL;
    bool rejected = config != NULL;
    for (size_t i = 0; i < 4; i++) {
        CBMFileResult *result = config ? tc_extract(sources[i], CBM_LANG_C, "src/invalid.c", config) : NULL;
        bool bound = tc_issue(result, statuses[i], 0, 1);
        if (!bound) fprintf(stderr, "convention argument fixture %zu failed\n", i);
        rejected = bound && rejected;
        cbm_free_result(result);
    }
    cbm_test_declarations_free(config);
    config = tc_config("{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[2147483647]}", NULL);
    parsed = config && parsed;
    CBMFileResult *huge = config ? tc_extract("CHECK(alpha) {}\n", CBM_LANG_C, "src/huge.c", config) : NULL;
    bool huge_missing = tc_issue(huge, CBM_TEST_EXTRACT_MISSING_ARGUMENT, 0, 1);
    cbm_free_result(huge); cbm_test_declarations_free(config);
    config = tc_config(
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"LATER\",\"name_args\":[1]},"
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"EARLIER\",\"name_args\":[1]}", NULL);
    parsed = config && parsed;
    CBMFileResult *ordered = config ? tc_extract("\nEARLIER(only) {}\nLATER(only) {}\n", CBM_LANG_C,
        "src/order.c", config) : NULL;
    bool earliest = tc_issue(ordered, CBM_TEST_EXTRACT_MISSING_ARGUMENT, 1, 2);
    cbm_free_result(ordered); cbm_test_declarations_free(config);
    ASSERT_TRUE(parsed);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(huge_missing);
    ASSERT_TRUE(earliest);
    PASS();
}

TEST(conventions_unsupported_consumed_descriptors_fail_preflight) {
    ASSERT_EQ(tc_init_status, 0);
    const char *records[] = {
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0,1]}",
        "{\"language\":\"future-language\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0]}",
        ""
    };
    const char *presets[] = {"", "", "\"pytest\":false"};
    const CBMTestExtractStatus statuses[] = {CBM_TEST_EXTRACT_UNSUPPORTED_NAME,
        CBM_TEST_EXTRACT_UNSUPPORTED_LANGUAGE, CBM_TEST_EXTRACT_UNSUPPORTED_PRESET};
    bool rejected = true, parsed = true;
    for (size_t i = 0; i < 3; i++) {
        cbm_test_declarations_t *config = tc_config(records[i], presets[i]);
        parsed = config && parsed;
        CBMFileResult *result = config ? tc_extract("", CBM_LANG_C, "src/preflight.c", config) : NULL;
        bool bound = tc_issue(result, statuses[i], i == 2 ? -1 : 0, 0);
        if (!bound) fprintf(stderr, "convention preflight fixture %zu failed\n", i);
        rejected = bound && rejected;
        cbm_free_result(result); cbm_test_declarations_free(config);
    }
    /* These retained fields belong to the runtime adapter, not definition naming. */
    cbm_test_declarations_t *opaque = tc_config(
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0],\"runner_id\":\"opaque:{future}\"},"
        "{\"language\":\"future-language\",\"role\":\"registration\",\"macro\":\"REG\",\"test_arg\":0},"
        "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"RUN\",\"suite_arg\":0,\"perf_macro\":\"PERF\"}", NULL);
    parsed = opaque && parsed;
    CBMFileResult *result = opaque ? tc_extract("CHECK(alpha) {}\n", CBM_LANG_C,
        "src/opaque.c", opaque) : NULL;
    bool retained = tc_ok(result) && tc_role(result, "CHECK_alpha", CBM_TEST_ROLE_CASE);
    cbm_free_result(result); cbm_test_declarations_free(opaque);
    ASSERT_TRUE(parsed);
    ASSERT_TRUE(rejected);
    ASSERT_TRUE(retained);
    PASS();
}

TEST(conventions_presets_and_languages_are_isolated) {
    ASSERT_EQ(tc_init_status, 0);
    const char *source = "TEST(alpha) {}\nSUITE(core) {}\nvoid helper(void) {}\n";
    cbm_test_declarations_t *cbm = tc_config(NULL, "\"c-cbm\":true");
    bool parsed = cbm != NULL;
    CBMFileResult *c = cbm ? tc_extract(source, CBM_LANG_C, "tests/test_preset.c", cbm) : NULL;
    bool c_roles = tc_ok(c) && tc_role(c, "TEST_alpha", CBM_TEST_ROLE_CASE) &&
        tc_role(c, "SUITE_core", CBM_TEST_ROLE_SUITE) &&
        tc_role(c, "helper", CBM_TEST_ROLE_NONE);
    cbm_free_result(c);
    const CBMLanguage others[] = {CBM_LANG_CPP, CBM_LANG_CUDA};
    const char *paths[] = {"src/preset.cpp", "src/preset.cu"};
    bool isolated = cbm != NULL;
    for (size_t i = 0; i < 2; i++) {
        CBMFileResult *old = tc_legacy(source, others[i], paths[i]);
        CBMFileResult *result = cbm ? tc_extract(source, others[i], paths[i], cbm) : NULL;
        isolated = tc_def(old, "helper") && tc_legacy_equal(old, result) && isolated;
        cbm_free_result(old); cbm_free_result(result);
    }
    cbm_test_declarations_free(cbm);
    cbm_test_declarations_t *disabled = tc_config(NULL, "\"gtest\":false");
    parsed = disabled && parsed;
    const char *gtest = "TEST(Group, Alpha) {}\n";
    CBMFileResult *old = tc_legacy(gtest, CBM_LANG_CPP, "src/gtest.cpp");
    CBMFileResult *off = disabled ? tc_extract(gtest, CBM_LANG_CPP, "src/gtest.cpp", disabled) : NULL;
    const char *legacy_name = NULL;
    if (old) for (int i = 0; i < old->defs.count; i++) {
        const CBMDefinition *def = &old->defs.items[i];
        if (def->name && strstr(def->name, "Alpha")) legacy_name = def->name;
    }
    bool gtest_control = legacy_name != NULL;
    bool switched = gtest_control && tc_none(off) && !tc_def(off, legacy_name);
    cbm_free_result(old); cbm_free_result(off); cbm_test_declarations_free(disabled);
    cbm_test_declarations_t *explicit_languages = tc_config(TC_CASE0 ","
        "{\"language\":\"cpp\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0]},"
        "{\"language\":\"cuda\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0]}", NULL);
    parsed = explicit_languages && parsed;
    const CBMLanguage languages[] = {CBM_LANG_C, CBM_LANG_CPP, CBM_LANG_CUDA};
    bool mapped = explicit_languages != NULL;
    for (size_t i = 0; i < 3; i++) {
        CBMFileResult *result = explicit_languages ? tc_extract("CHECK(value) {}\n", languages[i],
            "src/languages.c", explicit_languages) : NULL;
        mapped = tc_ok(result) && tc_role(result, "CHECK_value", CBM_TEST_ROLE_CASE) && mapped;
        cbm_free_result(result);
    }
    cbm_test_declarations_free(explicit_languages);
    ASSERT_TRUE(parsed); ASSERT_TRUE(gtest_control);
    ASSERT_TRUE(c_roles); ASSERT_TRUE(isolated); ASSERT_TRUE(switched); ASSERT_TRUE(mapped);
    PASS();
}

TEST(conventions_snapshot_and_result_strings_are_independently_owned) {
    ASSERT_EQ(tc_init_status, 0);
    char json_a[] = "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[" TC_CASE0 "]}}}";
    char json_b[] = "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":["
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"ALT\",\"name_args\":[0]}]}}}";
    cbm_test_declarations_t *a = cbm_test_declarations_parse(json_a, strlen(json_a), false);
    cbm_test_declarations_t *b = cbm_test_declarations_parse(json_b, strlen(json_b), false);
    bool parsed = a && b;
    memset(json_a, 'x', sizeof(json_a) - 1); memset(json_b, 'y', sizeof(json_b) - 1);
    char source[] = "CHECK(alpha) {}\nALT(beta) {}\n";
    CBMFileResult *first = a ? tc_extract(source, CBM_LANG_C, "src/owned.c", a) : NULL;
    CBMFileResult *other = b ? tc_extract(source, CBM_LANG_C, "src/owned.c", b) : NULL;
    CBMFileResult *again = a ? tc_extract(source, CBM_LANG_C, "src/owned.c", a) : NULL;
    cbm_test_declarations_free(a); cbm_test_declarations_free(b);
    memset(source, 'z', sizeof(source) - 1);
    bool owned = tc_ok(first) && tc_ok(other) && tc_ok(again) &&
        tc_only_case(first, "CHECK_alpha") &&
        tc_only_case(again, "CHECK_alpha") &&
        tc_only_case(other, "ALT_beta") &&
        tc_text(tc_def(first, "CHECK_alpha")->qualified_name,
                tc_def(again, "CHECK_alpha")->qualified_name);
    cbm_free_result(first); cbm_free_result(other); cbm_free_result(again);
    ASSERT_TRUE(parsed);
    ASSERT_TRUE(owned);
    PASS();
}

TEST(conventions_roles_and_errors_survive_compaction_and_spill) {
    ASSERT_EQ(tc_init_status, 0);
    cbm_test_declarations_t *config = tc_config(TC_CASE1, NULL);
    bool parsed = config != NULL;
    CBMFileResult *good = config ? tc_extract("CHECK(ignore, alive) {}\n", CBM_LANG_C,
        "src/spill.c", config) : NULL;
    CBMFileResult *bad = config ? tc_extract("CHECK(only) {}\n", CBM_LANG_C,
        "src/spill_error.c", config) : NULL;
    cbm_test_declarations_free(config);
    bool before = tc_ok(good) && tc_role(good, "CHECK_alive", CBM_TEST_ROLE_CASE) &&
        tc_issue(bad, CBM_TEST_EXTRACT_MISSING_ARGUMENT, 0, 1);
    if (good) cbm_result_compact(good);
    if (bad) cbm_result_compact(bad);
    bool compacted = tc_ok(good) && tc_role(good, "CHECK_alive", CBM_TEST_ROLE_CASE) &&
        tc_issue(bad, CBM_TEST_EXTRACT_MISSING_ARGUMENT, 0, 1);
    const char *temporary = th_mktempdir("cbm-test-conventions");
    char directory[512] = {0};
    bool dir_ok = temporary && strlen(temporary) < sizeof(directory);
    if (dir_ok) memcpy(directory, temporary, strlen(temporary) + 1);
    cbm_result_spill_t *spill = dir_ok ? cbm_result_spill_open(directory, 1, 2) : NULL;
    bool spill_opened = spill != NULL;
    bool parked_good = spill && good && cbm_result_spill_park(spill, 0, 0, good);
    if (parked_good) good = NULL; /* park consumed ownership */
    bool parked_bad = spill && bad && cbm_result_spill_park(spill, 0, 1, bad);
    if (parked_bad) bad = NULL;
    CBMFileResult header = {0};
    bool header_ok = parked_bad && cbm_result_spill_peek_header(spill, 1, &header) &&
        header.test_declarations_degraded &&
        header.test_declarations_degraded_status == CBM_TEST_EXTRACT_MISSING_ARGUMENT &&
        header.test_declarations_status == CBM_TEST_EXTRACT_OK &&
        header.test_declaration_index == 0 && header.test_declaration_line == 1 && !header.has_error;
    CBMFileResult *loaded_good = parked_good ? cbm_result_spill_load(spill, 0) : NULL;
    CBMFileResult *loaded_bad = parked_bad ? cbm_result_spill_load(spill, 1) : NULL;
    if (spill) cbm_result_spill_close(spill);
    bool reloaded = tc_ok(loaded_good) && tc_role(loaded_good, "CHECK_alive", CBM_TEST_ROLE_CASE) &&
        tc_def(loaded_good, "CHECK_alive")->is_test &&
        tc_issue(loaded_bad, CBM_TEST_EXTRACT_MISSING_ARGUMENT, 0, 1);
    cbm_free_result(good); cbm_free_result(bad);
    cbm_free_result(loaded_good); cbm_free_result(loaded_bad);
    int cleanup = dir_ok ? th_rmtree(directory) : -1;
    ASSERT_EQ(cleanup, 0);
    ASSERT_TRUE(parsed); ASSERT_TRUE(spill_opened);
    ASSERT_TRUE(before); ASSERT_TRUE(compacted);
    ASSERT_TRUE(parked_good); ASSERT_TRUE(parked_bad); ASSERT_TRUE(header_ok); ASSERT_TRUE(reloaded);
    PASS();
}

TEST(conventions_only_raw_invocations_receive_configured_roles) {
    ASSERT_EQ(tc_init_status, 0);
    const char *source =
        "void sink(void);\n"
        "#define OPEN_CASE(name) void CHECK_##name(void) {\n"
        "#define CLOSE_CASE }\n"
        "OPEN_CASE(hidden)\n"
        "    sink();\n"
        "CLOSE_CASE\n"
        "CHECK(visible) {}\n";
    cbm_test_declarations_t *config = tc_config(TC_CASE0, NULL);
    bool parsed = config != NULL;
    CBMFileResult *result = config ? tc_extract(source, CBM_LANG_C, "src/origin.c", config) : NULL;
    cbm_test_declarations_free(config);
    /* Expanded definitions need not be adopted into the raw definition set.
     * A call with an expanded-source origin and the generated function owner
     * proves this fixture actually exercised preprocessing. */
    bool preprocessed_control = false;
    if (result && result->module_qn) {
        char enclosing[512];
        int n = snprintf(enclosing, sizeof(enclosing), "%s.CHECK_hidden", result->module_qn);
        if (n > 0 && (size_t)n < sizeof(enclosing)) {
            for (int i = 0; i < result->calls.count; i++) {
                const CBMCall *call = &result->calls.items[i];
                if (call->source_origin == CBM_SOURCE_ORIGIN_PREPROCESSED &&
                    tc_text(call->callee_name, "sink") &&
                    tc_text(call->enclosing_func_qn, enclosing))
                    preprocessed_control = true;
            }
        }
    }
    /* The raw visible case is the only allowed configured role. This also
     * rejects promotion of a generated definition if one is retained. */
    bool origin = tc_only_case(result, "CHECK_visible");
    cbm_free_result(result);
    ASSERT_TRUE(parsed); ASSERT_TRUE(preprocessed_control);
    ASSERT_TRUE(origin);
    PASS();
}

/* Add below the original independent fixtures, before SUITE(test_conventions). */
#define TC_OWNER_SOURCE "void foo(int x) {} int bar;\nCHECK(one) { foo (bar); }\n"
#define TC_STALE_SOURCE "void foo(int x) {} int bar;\nCHECK(one) { foo + bar; }\n"

typedef struct {
    CBMFileResult *owners;
    CBMFileResult *stale;
    const CBMDefinition *one;
    CBMLSPDef foo;
    char caller[512];
    char callee[512];
} tc_owner_fixture_t;

static bool tc_owner_open(tc_owner_fixture_t *f) {
    *f = (tc_owner_fixture_t){0};
    cbm_test_declarations_t *config = tc_config(TC_CASE0, NULL);
    f->owners = config ? tc_extract(TC_OWNER_SOURCE, CBM_LANG_C,
        "src/owner.c", config) : NULL;
    cbm_test_declarations_free(config);
    f->stale = tc_legacy(TC_STALE_SOURCE, CBM_LANG_C, "src/owner.c");
    f->one = tc_def(f->owners, "CHECK_one");
    if (!tc_ok(f->owners) || !tc_role(f->owners, "CHECK_one", CBM_TEST_ROLE_CASE) ||
        !f->owners->has_test_definition_owners || !f->owners->cached_tree ||
        !f->stale || !f->stale->cached_tree || !f->one ||
        strlen(TC_OWNER_SOURCE) != strlen(TC_STALE_SOURCE)) return false;
    int a = snprintf(f->caller, sizeof(f->caller), "%s", f->one->qualified_name);
    int b = snprintf(f->callee, sizeof(f->callee), "%s.foo", f->owners->module_qn);
    if (a <= 0 || (size_t)a >= sizeof(f->caller) ||
        b <= 0 || (size_t)b >= sizeof(f->callee)) return false;
    static const char *parameters[] = {"int"};
    f->foo = (CBMLSPDef){
        .qualified_name = f->callee, .short_name = "foo", .label = "Function",
        .def_module_qn = f->owners->module_qn, .return_types = "void",
        .signature_param_types = parameters, .signature_param_count = 1,
        .lang = CBM_LANG_C};
    return true;
}

static void tc_owner_close(tc_owner_fixture_t *f) {
    cbm_free_result(f->owners);
    cbm_free_result(f->stale);
    f->owners = NULL;
    f->stale = NULL;
    f->one = NULL;
}

static bool tc_resolved_equal(const CBMResolvedCallArray *a,
                              const CBMResolvedCallArray *b) {
    if (a->count != b->count) return false;
    for (int i = 0; i < a->count; i++) {
        const CBMResolvedCall *x = &a->items[i], *y = &b->items[i];
        if (!tc_text(x->caller_qn, y->caller_qn) || !tc_text(x->callee_qn, y->callee_qn) ||
            !tc_text(x->strategy, y->strategy) || !tc_text(x->reason, y->reason) ||
            x->confidence != y->confidence || x->kind != y->kind ||
            x->site_start_byte != y->site_start_byte || x->site_end_byte != y->site_end_byte ||
            x->source_origin != y->source_origin) return false;
    }
    return true;
}

static bool tc_resolved_foo(const CBMResolvedCallArray *out,
                            const char *caller, const char *callee) {
    int matches = 0;
    const char *source = TC_OWNER_SOURCE;
    const char *site = strstr(source, "foo (bar)");
    uint32_t start = (uint32_t)(site - source);
    for (int i = 0; i < out->count; i++) {
        const CBMResolvedCall *call = &out->items[i];
        if (!tc_text(call->callee_qn, callee) || call->kind != CBM_RESOLVED_INVOCATION) continue;
        if (!tc_text(call->caller_qn, caller) || call->source_origin != CBM_SOURCE_ORIGIN_RAW ||
            call->site_start_byte != start || call->site_end_byte <= start ||
            call->site_end_byte > start + 9) return false;
        matches++;
    }
    return matches == 1;
}

static bool tc_owner_cross(tc_owner_fixture_t *f, CBMArena *arena,
                           CBMResolvedCallArray *out, CBMTypeRegistry *registry,
                           const char *source, int length, const char *module,
                           bool cpp, CBMSourceOrigin origin, TSTree *tree) {
    if (registry)
        return cbm_run_c_lsp_cross_with_registry_with_test_owners(arena, source, length,
            module, cpp, registry, NULL, NULL, 0, tree, out, origin, f->owners);
    return cbm_run_c_lsp_cross_with_test_owners(arena, source, length, module, cpp,
        &f->foo, 1, NULL, NULL, 0, tree, out, origin, f->owners);
}

TEST(conventions_cached_body_tree_cannot_drop_calls) {
    ASSERT_EQ(tc_init_status, 0);
    tc_owner_fixture_t f;
    bool fixture = tc_owner_open(&f);
    CBMArena registry_arena;
    cbm_arena_init_lazy(&registry_arena, 4096);
    CBMTypeRegistry *registry = fixture ?
        cbm_c_build_cross_registry(&registry_arena, &f.foo, 1) : NULL;
    bool registry_ok = registry != NULL;
    bool controls = fixture && registry_ok, protected = controls;
    for (int variant = 0; variant < 2 && fixture && registry_ok; variant++) {
        CBMArena fresh_arena, stale_arena;
        cbm_arena_init_lazy(&fresh_arena, 4096);
        cbm_arena_init_lazy(&stale_arena, 4096);
        CBMResolvedCallArray fresh = {0}, stale = {0};
        CBMTypeRegistry *selected = variant ? registry : NULL;
        bool fresh_ok = tc_owner_cross(&f, &fresh_arena, &fresh, selected,
            TC_OWNER_SOURCE, (int)strlen(TC_OWNER_SOURCE), f.owners->module_qn, false,
            CBM_SOURCE_ORIGIN_RAW, NULL);
        bool stale_ok = tc_owner_cross(&f, &stale_arena, &stale, selected,
            TC_OWNER_SOURCE, (int)strlen(TC_OWNER_SOURCE), f.owners->module_qn, false,
            CBM_SOURCE_ORIGIN_RAW, f.stale->cached_tree);
        bool control = fresh_ok && tc_resolved_foo(&fresh, f.caller, f.callee);
        bool bound = !stale_ok || tc_resolved_equal(&fresh, &stale);
        if (!control || !bound) fprintf(stderr, "convention stale cross variant %d failed\n", variant);
        controls = control && controls;
        protected = bound && protected;
        cbm_arena_destroy(&fresh_arena);
        cbm_arena_destroy(&stale_arena);
    }
    cbm_arena_destroy(&registry_arena);
    tc_owner_close(&f);
    ASSERT_TRUE(fixture); ASSERT_TRUE(registry_ok); ASSERT_TRUE(controls);
    ASSERT_TRUE(protected);
    PASS();
}

TEST(conventions_root_body_tree_cannot_drop_calls) {
    ASSERT_EQ(tc_init_status, 0);
    tc_owner_fixture_t f;
    bool fixture = tc_owner_open(&f);
    CBMFileResult *fresh = tc_legacy(TC_OWNER_SOURCE, CBM_LANG_C, "src/owner.c");
    CBMFileResult *stale = tc_legacy(TC_OWNER_SOURCE, CBM_LANG_C, "src/owner.c");
    bool inputs = fixture && fresh && fresh->cached_tree && stale;
    bool fresh_ok = false, stale_ok = false;
    if (inputs) {
        fresh->resolved_calls = (CBMResolvedCallArray){0};
        stale->resolved_calls = (CBMResolvedCallArray){0};
        fresh_ok = cbm_run_c_lsp_with_test_owners(&fresh->arena, fresh,
            TC_OWNER_SOURCE, (int)strlen(TC_OWNER_SOURCE),
            ts_tree_root_node(fresh->cached_tree), false, CBM_SOURCE_ORIGIN_RAW, f.owners);
        stale_ok = cbm_run_c_lsp_with_test_owners(&stale->arena, stale,
            TC_OWNER_SOURCE, (int)strlen(TC_OWNER_SOURCE),
            ts_tree_root_node(f.stale->cached_tree), false, CBM_SOURCE_ORIGIN_RAW, f.owners);
    }
    bool control = inputs && fresh_ok && tc_resolved_foo(&fresh->resolved_calls, f.caller, f.callee);
    bool protected = inputs && (!stale_ok || tc_resolved_equal(&fresh->resolved_calls,
                                                              &stale->resolved_calls));
    cbm_free_result(fresh); cbm_free_result(stale); tc_owner_close(&f);
    ASSERT_TRUE(inputs); ASSERT_TRUE(control); ASSERT_TRUE(protected);
    PASS();
}

TEST(conventions_owner_identity_and_output_lifetime) {
    ASSERT_EQ(tc_init_status, 0);
    tc_owner_fixture_t f;
    bool fixture = tc_owner_open(&f);
    CBMArena registry_arena, first_arena, second_arena;
    cbm_arena_init_lazy(&registry_arena, 4096);
    cbm_arena_init_lazy(&first_arena, 4096);
    cbm_arena_init_lazy(&second_arena, 4096);
    CBMTypeRegistry *registry = fixture ?
        cbm_c_build_cross_registry(&registry_arena, &f.foo, 1) : NULL;
    CBMResolvedCallArray first = {0}, second = {0};
    CBMFileResult *single = tc_legacy(TC_OWNER_SOURCE, CBM_LANG_C, "src/owner.c");
    bool inputs = fixture && registry && single && single->cached_tree;
    bool controls = false, rejected = inputs;
    if (inputs) {
        single->resolved_calls = (CBMResolvedCallArray){0};
        bool a = tc_owner_cross(&f, &first_arena, &first, NULL, TC_OWNER_SOURCE,
            (int)strlen(TC_OWNER_SOURCE), f.owners->module_qn, false, CBM_SOURCE_ORIGIN_RAW, NULL);
        bool b = tc_owner_cross(&f, &second_arena, &second, registry, TC_OWNER_SOURCE,
            (int)strlen(TC_OWNER_SOURCE), f.owners->module_qn, false, CBM_SOURCE_ORIGIN_RAW, NULL);
        bool c = cbm_run_c_lsp_with_test_owners(&single->arena, single,
            TC_OWNER_SOURCE, (int)strlen(TC_OWNER_SOURCE), ts_tree_root_node(single->cached_tree),
            false, CBM_SOURCE_ORIGIN_RAW, f.owners);
        controls = a && b && c && tc_resolved_foo(&first, f.caller, f.callee) &&
            tc_resolved_foo(&second, f.caller, f.callee) &&
            tc_resolved_foo(&single->resolved_calls, f.caller, f.callee);
    }
    for (int variant = 0; variant < 5 && inputs; variant++) {
        const char *source = variant == 0 ? TC_STALE_SOURCE : TC_OWNER_SOURCE;
        int length = (int)strlen(source) - (variant == 4 ? 1 : 0);
        const char *module = variant == 3 ? "convproj.wrong" : f.owners->module_qn;
        bool cpp = variant == 2;
        CBMSourceOrigin origin = variant == 1 ? CBM_SOURCE_ORIGIN_PREPROCESSED : CBM_SOURCE_ORIGIN_RAW;
        for (int api = 0; api < 2; api++) {
            CBMArena arena;
            cbm_arena_init_lazy(&arena, 4096);
            CBMResolvedCallArray out = {0};
            bool ok = tc_owner_cross(&f, &arena, &out, api ? registry : NULL,
                source, length, module, cpp, origin, f.owners->cached_tree);
            bool bound = !ok && out.count == 0;
            if (!bound) fprintf(stderr, "convention owner identity variant %d API %d failed\n", variant, api);
            rejected = bound && rejected;
            cbm_arena_destroy(&arena);
        }
        CBMFileResult *out = tc_legacy(TC_OWNER_SOURCE, CBM_LANG_C, "src/owner.c");
        if (!out) { rejected = false; continue; }
        out->resolved_calls = (CBMResolvedCallArray){0};
        if (variant == 3) out->module_qn = "convproj.wrong";
        bool ok = cbm_run_c_lsp_with_test_owners(&out->arena, out, source, length,
            ts_tree_root_node(f.owners->cached_tree), cpp, origin, f.owners);
        bool bound = !ok && out->resolved_calls.count == 0;
        if (!bound) fprintf(stderr, "convention owner identity variant %d root API failed\n", variant);
        rejected = bound && rejected;
        cbm_free_result(out);
    }
    char expected_caller[512] = {0}, expected_callee[512] = {0};
    if (inputs) {
        memcpy(expected_caller, f.caller, sizeof(expected_caller));
        memcpy(expected_callee, f.callee, sizeof(expected_callee));
        /* These strings are documented result-arena-owned. Poison while still
         * live: borrowed output fails deterministically, without allocator luck. */
        memset((char *)f.one->qualified_name, 'X', strlen(f.one->qualified_name));
        memset(f.callee, 'Y', strlen(f.callee));
    }
    bool copied = inputs && tc_resolved_foo(&first, expected_caller, expected_callee) &&
        tc_resolved_foo(&second, expected_caller, expected_callee) &&
        tc_resolved_foo(&single->resolved_calls, expected_caller, expected_callee);
    tc_owner_close(&f);
    cbm_arena_destroy(&registry_arena);
    bool alive = copied && tc_resolved_foo(&first, expected_caller, expected_callee) &&
        tc_resolved_foo(&second, expected_caller, expected_callee) &&
        tc_resolved_foo(&single->resolved_calls, expected_caller, expected_callee);
    cbm_arena_destroy(&first_arena); cbm_arena_destroy(&second_arena); cbm_free_result(single);
    ASSERT_TRUE(inputs); ASSERT_TRUE(controls); ASSERT_TRUE(rejected);
    ASSERT_TRUE(copied); ASSERT_TRUE(alive);
    PASS();
}

TEST(conventions_cpp_digit_separator_does_not_hide_mapping) {
    ASSERT_EQ(tc_init_status, 0);
    cbm_test_declarations_t *config = tc_config(
        "{\"language\":\"cpp\",\"role\":\"case\",\"define_macro\":\"CHECK\",\"name_args\":[0]}", NULL);
    bool parsed = config != NULL;
    CBMFileResult *good = config ? tc_extract("int n=1'000;\nCHECK(alpha) {}\n",
        CBM_LANG_CPP, "src/digits.cpp", config) : NULL;
    CBMFileResult *bad = config ? tc_extract("int n=1'000;\nCHECK(alpha);\n",
        CBM_LANG_CPP, "src/digits.cpp", config) : NULL;
    bool control = tc_ok(good) && tc_only_case(good, "CHECK_alpha");
    bool rejected = tc_degraded(bad) && bad->test_declaration_index == 0 &&
        bad->test_declaration_line == 2;
    cbm_free_result(good); cbm_free_result(bad); cbm_test_declarations_free(config);
    ASSERT_TRUE(parsed); ASSERT_TRUE(control); ASSERT_TRUE(rejected);
    PASS();
}

TEST(conventions_define_multiline_comment_preserves_complete_audit) {
    ASSERT_EQ(tc_init_status, 0);
    cbm_test_declarations_t *config = tc_config(TC_CASE0, NULL);
    bool parsed = config != NULL;
    CBMFileResult *good = config ? tc_extract(
        "#define N 1 /* ordinary\ncomment */\nCHECK(ok) {}\n",
        CBM_LANG_C, "src/comments.c", config) : NULL;
    CBMFileResult *bad = config ? tc_extract(
        "#define N 1 /* ordinary\ncomment\nCHECK(ok) {}\n",
        CBM_LANG_C, "src/comments.c", config) : NULL;
    bool control = tc_ok(good) && tc_only_case(good, "CHECK_ok");
    bool rejected = tc_degraded(bad);
    cbm_free_result(good); cbm_free_result(bad); cbm_test_declarations_free(config);
    ASSERT_TRUE(parsed); ASSERT_TRUE(control); ASSERT_TRUE(rejected);
    PASS();
}

TEST(conventions_same_line_error_keeps_failing_declaration) {
    ASSERT_EQ(tc_init_status, 0);
    cbm_test_declarations_t *config = tc_config(
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"B\",\"name_args\":[0]},"
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"A\",\"name_args\":[0]}", NULL);
    bool parsed = config != NULL;
    CBMFileResult *good = config ? tc_extract("A(yes){} B(ok){}\n", CBM_LANG_C,
        "src/same_line.c", config) : NULL;
    CBMFileResult *bad = config ? tc_extract("A(){} B(ok){}\n", CBM_LANG_C,
        "src/same_line.c", config) : NULL;
    bool control = tc_ok(good) && tc_role(good, "A_yes", CBM_TEST_ROLE_CASE) &&
        tc_role(good, "B_ok", CBM_TEST_ROLE_CASE);
    bool rejected = tc_issue(bad, CBM_TEST_EXTRACT_MISSING_ARGUMENT, 1, 1);
    cbm_free_result(good); cbm_free_result(bad); cbm_test_declarations_free(config);
    ASSERT_TRUE(parsed); ASSERT_TRUE(control); ASSERT_TRUE(rejected);
    PASS();
}

SUITE(test_conventions) {
    tc_init_status = cbm_init();
    RUN_TEST(conventions_null_absent_and_defaults_preserve_legacy);
    RUN_TEST(conventions_names_roles_arguments_and_body_owners);
    RUN_TEST(conventions_duplicates_coalesce_and_conflicts_fail_typed);
    RUN_TEST(conventions_invalid_invocations_have_exact_typed_locations);
    RUN_TEST(conventions_unsupported_consumed_descriptors_fail_preflight);
    RUN_TEST(conventions_presets_and_languages_are_isolated);
    RUN_TEST(conventions_snapshot_and_result_strings_are_independently_owned);
    RUN_TEST(conventions_roles_and_errors_survive_compaction_and_spill);
    RUN_TEST(conventions_only_raw_invocations_receive_configured_roles);
    RUN_TEST(conventions_cached_body_tree_cannot_drop_calls);
    RUN_TEST(conventions_root_body_tree_cannot_drop_calls);
    RUN_TEST(conventions_owner_identity_and_output_lifetime);
    RUN_TEST(conventions_cpp_digit_separator_does_not_hide_mapping);
    RUN_TEST(conventions_define_multiline_comment_preserves_complete_audit);
    RUN_TEST(conventions_same_line_error_keeps_failing_declaration);
    cbm_work_arena_release();
}
