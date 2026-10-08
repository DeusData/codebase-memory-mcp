/* Public-API fixtures only: synthetic facts never establish provider admission. */
#include "test_test_impact_runner_filter.h"
#include "test_helpers.h"

bool rf_fixture_open(rf_fixture_t *fixture) {
    memset(fixture, 0, sizeof(*fixture));
    const char *root = th_mktempdir("cbm-runner-filter");
    if (!root || strlen(root) >= sizeof(fixture->root)) {
        return false;
    }
    memcpy(fixture->root, root, strlen(root) + 1);
    int a = snprintf(fixture->filter_path, sizeof(fixture->filter_path), "%s/filter.txt", root);
    int b = snprintf(fixture->log_path, sizeof(fixture->log_path), "%s/child.log", root);
    return a > 0 && (size_t)a < sizeof(fixture->filter_path) && b > 0 &&
           (size_t)b < sizeof(fixture->log_path);
}

bool rf_fixture_close(rf_fixture_t *fixture) {
    yyjson_doc_free(fixture->json);
    cbm_test_result_free(fixture->result);
    cbm_test_selection_free(fixture->selection);
    cbm_test_policy_free(fixture->policy);
    cbm_test_config_free(fixture->config);
    cbm_coverage_map_free(fixture->coverage);
    cbm_test_model_free(fixture->model);
    bool removed = !fixture->root[0] || th_rmtree(fixture->root) == 0;
    memset(fixture, 0, sizeof(*fixture));
    return removed;
}

static bool rf_model(rf_fixture_t *fixture, const rf_spec_t *spec) {
    char source[8192];
    int count = snprintf(source, sizeof(source),
                         "TEST(%s) {}\nTEST(rf_spare) {}\n"
                         "SUITE(%s) { RUN_TEST(%s); RUN_TEST(rf_spare); }\n"
                         "void main(void) { RUN_SELECTED_SUITE(%s); }\n",
                         spec->test, spec->suite, spec->test, spec->suite);
    fixture->model = cbm_test_model_new(cbm_test_conventions_cbm());
    if (count < 0 || (size_t)count >= sizeof(source) || !fixture->model ||
        !cbm_test_model_add_source(fixture->model, "tests/rf_subject.c", source, (size_t)count)) {
        return false;
    }
    static const char other[] = "TEST(rf_other) {}\nSUITE(str_intern) { RUN_TEST(rf_other); }\n"
                                "void other_main(void) { RUN_SELECTED_SUITE(str_intern); }\n";
    if (spec->mixed &&
        !cbm_test_model_add_source(fixture->model, "tests/rf_whole.c", other, sizeof(other) - 1)) {
        return false;
    }
    return cbm_test_model_finish(fixture->model) && cbm_test_model_complete(fixture->model);
}

static bool rf_selection(rf_fixture_t *fixture, const rf_spec_t *spec) {
    char rows[4096];
    int count = snprintf(rows, sizeof(rows),
                         "%s:*\tcomplete\t\t1\n%s:%s\tcomplete\t\t0\n"
                         "%s:rf_spare\tcomplete\t\t1\n%s",
                         spec->suite, spec->suite, spec->test, spec->suite,
                         spec->mixed ? "str_intern:*\tcomplete\t\t1\n"
                                       "str_intern:rf_other\tcomplete\t\t1\n"
                                     : "");
    static const char functions[] = "0\tsrc/changed.c\tchanged\n1\tsrc/quiet.c\tquiet\n";
    if (count < 0 || (size_t)count >= sizeof(rows)) {
        return false;
    }
    fixture->coverage =
        cbm_coverage_map_parse(functions, sizeof(functions) - 1, rows, (size_t)count);
    cbm_test_reach_t reach[] = {
        {.file = "tests/rf_subject.c", .test = spec->test, .mapped = true, .reached = true},
        {.file = "tests/rf_subject.c", .test = "rf_spare", .mapped = true},
        {.file = "tests/rf_whole.c", .test = "rf_other", .mapped = true},
    };
    cbm_test_suite_trigger_t trigger = {.suite = spec->mixed ? "str_intern" : spec->suite,
                                        .reached = true};
    const int changed = 0;
    cbm_test_selection_input_t input = {
        .model = fixture->model,
        .coverage = fixture->coverage,
        .reach = reach,
        .reach_count = spec->mixed ? 3 : 2,
        .suite_triggers = &trigger,
        .suite_trigger_count = spec->whole || spec->mixed ? 1 : 0,
        .changed_function_ids = &changed,
        .changed_function_count = 1,
        .has_changes = true,
        .diff_complete = true,
        .inventory_complete = true,
        .static_complete = true,
        .coverage_admitted = true,
        .coverage_changes_complete = true,
    };
    if (!fixture->coverage) {
        return false;
    }
    fixture->selection = cbm_test_select(&input);
    return fixture->selection && cbm_test_selection_run_all(fixture->selection) == 0;
}

bool rf_prepare(rf_fixture_t *fixture, const rf_spec_t *spec) {
    char absent[RF_PATH_CAP];
    int count = snprintf(absent, sizeof(absent), "%s/absent-policy.json", fixture->root);
    if (count < 0 || (size_t)count >= sizeof(absent) || !rf_model(fixture, spec) ||
        !rf_selection(fixture, spec)) {
        return false;
    }
    fixture->config = cbm_test_config_load(absent, true);
    fixture->policy = fixture->config ? cbm_test_policy_new(fixture->config) : NULL;
    return fixture->policy != NULL;
}

static bool rf_policy_digest(const cbm_test_policy_t *policy, cbm_test_result_digest_t *digest) {
    const char *text = cbm_test_policy_digest(policy);
    if (!text || strlen(text) != 64) {
        return false;
    }
    static const char digits[] = "0123456789abcdef";
    for (size_t i = 0; i < 32; i++) {
        const char *high = strchr(digits, text[i * 2]);
        const char *low = strchr(digits, text[i * 2 + 1]);
        if (!high || !low) {
            return false;
        }
        digest->bytes[i] = (unsigned char)((high - digits) * 16 + (low - digits));
    }
    digest->present = true;
    return true;
}

cbm_test_result_status_t rf_build(rf_fixture_t *fixture, size_t output_limit, bool fallback) {
    yyjson_doc_free(fixture->json);
    fixture->json = NULL;
    cbm_test_result_free(fixture->result);
    fixture->result = NULL;
    fixture->filter = NULL;
    fixture->filter_length = 0;
    cbm_test_result_receipt_t receipt = {0};
    receipt.object_format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.base.format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.head.format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.merge_base.format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.diff_sha256.present = true;
    receipt.name_status_sha256.present = true;
    if (!rf_policy_digest(fixture->policy, &receipt.policy_sha256)) {
        return CBM_TEST_RESULT_INVALID;
    }
    cbm_test_result_fallback_t reason = CBM_TEST_RESULT_FALLBACK_GRAPH_UNAVAILABLE;
    cbm_test_result_input_t input = {
        .comparison = CBM_TEST_RESULT_COMPARISON_CHANGED,
        .selection = fixture->selection,
        .model = fixture->model,
        .policy = fixture->policy,
        .inventory_complete = true,
        .activation_complete = true,
        .receipt = &receipt,
        .fallbacks = &reason,
        .fallback_count = fallback ? 1 : 0,
    };
    cbm_test_result_limits_t limits = {
        .max_input_bytes = 1048576,
        .max_items = 100000,
        .max_alloc_bytes = 16777216,
        .max_output_bytes = output_limit,
    };
    return cbm_test_result_build(&input, &limits, NULL, NULL, &fixture->result);
}

bool rf_parse(rf_fixture_t *fixture) {
    size_t length = 0;
    const char *json = cbm_test_result_json(fixture->result, &length);
    if (!json || !length || length > RF_JSON_CAP) {
        return false;
    }
    fixture->json = yyjson_read(json, length, 0);
    return fixture->json != NULL;
}

yyjson_val *rf_lane(const rf_fixture_t *fixture) {
    yyjson_val *root = yyjson_doc_get_root(fixture->json);
    yyjson_val *lanes = yyjson_obj_get(root, "lanes");
    return yyjson_arr_size(lanes) == 1 ? yyjson_arr_get(lanes, 0) : NULL;
}

bool rf_exact_filter(rf_fixture_t *fixture, const char *expected) {
    if (!rf_parse(fixture)) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(fixture->json);
    yyjson_val *lane = rf_lane(fixture);
    yyjson_val *filter = yyjson_obj_get(lane, "runner_filter");
    yyjson_val *value = yyjson_obj_get(filter, "value");
    fixture->filter = yyjson_get_str(value);
    fixture->filter_length = yyjson_get_len(value);
    return yyjson_equals_str(yyjson_obj_get(root, "decision"), "selected") &&
           yyjson_equals_str(yyjson_obj_get(lane, "decision"), "selected") &&
           yyjson_equals_str(yyjson_obj_get(filter, "encoding"), "cbm-test-only-lines") &&
           fixture->filter && fixture->filter_length == strlen(expected) &&
           memcmp(fixture->filter, expected, fixture->filter_length) == 0;
}
