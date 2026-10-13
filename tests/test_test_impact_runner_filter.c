/* This consumer boundary does not replace the broader result-codec suite. */
#include "test_framework.h"
#include "test_test_impact_runner_filter.h"

static bool rf_clean_run(const rf_run_t *run, int expected) {
    return run->process.outcome == CBM_PROC_CLEAN && run->process.exit_code == 0 &&
           run->passed == expected && run->pass_lines == expected &&
           !strstr(run->text, "malformed test selection token:") &&
           !strstr(run->text, "selected test not compiled into this build or unknown:");
}

static bool rf_baseline(int *count) {
    rf_fixture_t fixture;
    rf_run_t run = {0};
    bool opened = rf_fixture_open(&fixture);
    bool ran = opened && rf_run_child(&fixture, "str_intern", NULL, false, &run);
    bool ok = ran && run.passed > 0 && rf_clean_run(&run, run.passed);
    *count = run.passed;
    bool removed = rf_fixture_close(&fixture);
    return ok && removed;
}

static bool rf_consume(const rf_spec_t *spec, const char *expected, int count) {
    rf_fixture_t fixture;
    rf_run_t run = {0};
    bool prepared = rf_fixture_open(&fixture) && rf_prepare(&fixture, spec);
    cbm_test_result_status_t status =
        prepared ? rf_build(&fixture, RF_JSON_CAP, false) : CBM_TEST_RESULT_INVALID;
    bool generated = status == CBM_TEST_RESULT_OK && rf_exact_filter(&fixture, expected);
    bool ran = generated && rf_run_child(&fixture, "dyn_array", "str_intern", true, &run);
    bool ok = ran && rf_clean_run(&run, count);
    if (!spec->whole) {
        ok = ok && strstr(run.text, "  da_last ") && !strstr(run.text, "  da_clear ");
    }
    if (!ok) {
        printf("  runner filter consumer: suite=%s whole=%d mixed=%d status=%d passed=%d\n",
               spec->suite, spec->whole, spec->mixed, status, run.passed);
    }
    bool removed = rf_fixture_close(&fixture);
    return ok && removed;
}

TEST(test_impact_runner_filter_consumes_public_codec_bytes) {
    int whole_count = 0;
    bool baseline = rf_baseline(&whole_count);
    const rf_spec_t whole = {.suite = "str_intern", .test = "rf_case", .whole = true};
    const rf_spec_t individual = {.suite = "dyn_array", .test = "da_last"};
    const rf_spec_t mixed = {.suite = "dyn_array", .test = "da_last", .mixed = true};
    bool whole_ok = baseline && rf_consume(&whole, "str_intern\n", whole_count);
    bool individual_ok = baseline && rf_consume(&individual, "dyn_array:da_last\n", 1);
    bool mixed_ok =
        baseline && rf_consume(&mixed, "dyn_array:da_last\nstr_intern\n", whole_count + 1);
    ASSERT_TRUE(baseline);
    ASSERT_TRUE(whole_ok);
    ASSERT_TRUE(individual_ok);
    ASSERT_TRUE(mixed_ok);
    PASS();
}

static bool rf_unknown_run(rf_fixture_t *fixture, const char *token, size_t length) {
    rf_run_t run = {0};
    char diagnostic[640];
    int size =
        snprintf(diagnostic, sizeof(diagnostic),
                 "selected test not compiled into this build or unknown: %.*s", (int)length, token);
    bool ran = rf_run_child(fixture, "dyn_array", NULL, true, &run);
    return size > 0 && (size_t)size < sizeof(diagnostic) && ran &&
           run.process.outcome == CBM_PROC_EXIT_NONZERO && run.process.exit_code != 0 &&
           run.pass_lines == 0 && strstr(run.text, diagnostic) &&
           !strstr(run.text, "malformed test selection token:");
}

static void rf_identifier(char *name, size_t length) {
    memset(name, 'q', length);
    name[length] = '\0';
}

static bool rf_width_case(bool whole, size_t width) {
    char name[513];
    rf_identifier(name, whole ? width : width - strlen("dyn_array:"));
    rf_spec_t spec = {
        .suite = whole ? name : "dyn_array", .test = whole ? "rf_case" : name, .whole = whole};
    char expected[515];
    int length = snprintf(expected, sizeof(expected), "%s%s%s\n", spec.suite, whole ? "" : ":",
                          whole ? "" : spec.test);
    rf_fixture_t fixture;
    bool prepared = rf_fixture_open(&fixture) && rf_prepare(&fixture, &spec);
    cbm_test_result_status_t status =
        prepared ? rf_build(&fixture, RF_JSON_CAP, false) : CBM_TEST_RESULT_INVALID;
    bool ok;
    if (width == 511) {
        ok = length == 512 && status == CBM_TEST_RESULT_OK && rf_exact_filter(&fixture, expected) &&
             rf_unknown_run(&fixture, expected, width);
    } else {
        ok = length == 513 && status == CBM_TEST_RESULT_UNSUPPORTED && fixture.result == NULL;
    }
    if (!ok) {
        printf("  runner filter width: whole=%d width=%zu status=%d\n", whole, width, status);
    }
    bool removed = rf_fixture_close(&fixture);
    return ok && removed;
}

TEST(test_impact_runner_filter_literal_width_boundaries) {
    /* Literal wire boundaries keep this test sensitive to a changed shared constant. */
    bool whole_511 = rf_width_case(true, 511);
    bool whole_512 = rf_width_case(true, 512);
    bool individual_511 = rf_width_case(false, 511);
    bool individual_512 = rf_width_case(false, 512);
    ASSERT_TRUE(whole_511);
    ASSERT_TRUE(whole_512);
    ASSERT_TRUE(individual_511);
    ASSERT_TRUE(individual_512);
    PASS();
}

static bool rf_limit_control(void) {
    rf_fixture_t fixture;
    const rf_spec_t spec = {.suite = "dyn_array", .test = "da_last"};
    bool prepared = rf_fixture_open(&fixture) && rf_prepare(&fixture, &spec);
    cbm_test_result_status_t limited =
        prepared ? rf_build(&fixture, 1, false) : CBM_TEST_RESULT_INVALID;
    bool empty = fixture.result == NULL;
    cbm_test_result_status_t full =
        prepared ? rf_build(&fixture, RF_JSON_CAP, false) : CBM_TEST_RESULT_INVALID;
    bool ok = limited == CBM_TEST_RESULT_LIMIT && empty && full == CBM_TEST_RESULT_OK &&
              rf_exact_filter(&fixture, "dyn_array:da_last\n");
    bool removed = rf_fixture_close(&fixture);
    return ok && removed;
}

static bool rf_diagnostic_control(void) {
    char name[513];
    rf_identifier(name, 512);
    const rf_spec_t spec = {.suite = name, .test = "rf_case", .whole = true};
    rf_fixture_t fixture;
    bool prepared = rf_fixture_open(&fixture) && rf_prepare(&fixture, &spec);
    cbm_test_result_status_t status =
        prepared ? rf_build(&fixture, RF_JSON_CAP, true) : CBM_TEST_RESULT_INVALID;
    bool parsed = status == CBM_TEST_RESULT_OK && rf_parse(&fixture);
    yyjson_val *root = yyjson_doc_get_root(fixture.json);
    yyjson_val *lane = rf_lane(&fixture);
    yyjson_val *suites = yyjson_obj_get(lane, "suites");
    yyjson_val *suite = yyjson_arr_get(suites, 0);
    yyjson_val *reasons = yyjson_obj_get(root, "run_all_reasons");
    bool ok = parsed && yyjson_equals_str(yyjson_obj_get(root, "decision"), "run_all") &&
              yyjson_equals_str(yyjson_obj_get(lane, "decision"), "run_all") &&
              yyjson_is_null(yyjson_obj_get(lane, "runner_filter")) &&
              yyjson_arr_size(suites) == 1 &&
              yyjson_equals_str(yyjson_obj_get(suite, "suite"), name) &&
              yyjson_equals_str(yyjson_obj_get(suite, "mode"), "whole") &&
              yyjson_arr_size(reasons) == 1 &&
              yyjson_equals_str(yyjson_arr_get(reasons, 0), "GRAPH_UNAVAILABLE");
    bool removed = rf_fixture_close(&fixture);
    return ok && removed;
}

TEST(test_impact_runner_filter_limits_and_diagnostic_domain) {
    bool limit_ok = rf_limit_control();
    bool diagnostic_ok = rf_diagnostic_control();
    ASSERT_TRUE(limit_ok);
    ASSERT_TRUE(diagnostic_ok);
    PASS();
}

SUITE(test_impact_runner_filter) {
    RUN_TEST(test_impact_runner_filter_consumes_public_codec_bytes);
    RUN_TEST(test_impact_runner_filter_literal_width_boundaries);
    RUN_TEST(test_impact_runner_filter_limits_and_diagnostic_domain);
}
