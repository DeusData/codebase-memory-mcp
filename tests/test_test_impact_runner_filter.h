#ifndef TEST_TEST_IMPACT_RUNNER_FILTER_H
#define TEST_TEST_IMPACT_RUNNER_FILTER_H

#include <mcp/test_impact_result.h>
#include <foundation/subprocess.h>
#include <yyjson/yyjson.h>

enum { RF_PATH_CAP = 1200, RF_OUTPUT_CAP = 32768, RF_JSON_CAP = 65536 };

typedef struct {
    const char *suite;
    const char *test;
    bool whole;
    bool mixed;
} rf_spec_t;

typedef struct {
    char root[1024];
    char filter_path[RF_PATH_CAP];
    char log_path[RF_PATH_CAP];
    cbm_test_model_t *model;
    cbm_coverage_map_t *coverage;
    cbm_test_config_t *config;
    cbm_test_policy_t *policy;
    cbm_test_selection_t *selection;
    cbm_test_result_t *result;
    yyjson_doc *json;
    const char *filter;
    size_t filter_length;
} rf_fixture_t;

typedef struct {
    cbm_proc_result_t process;
    char text[RF_OUTPUT_CAP];
    int passed;
    int pass_lines;
} rf_run_t;

void tf_test_impact_runner_filter_set_binary(const char *path);
bool rf_fixture_open(rf_fixture_t *fixture);
bool rf_fixture_close(rf_fixture_t *fixture);
bool rf_prepare(rf_fixture_t *fixture, const rf_spec_t *spec);
cbm_test_result_status_t rf_build(rf_fixture_t *fixture, size_t output_limit, bool fallback);
bool rf_parse(rf_fixture_t *fixture);
yyjson_val *rf_lane(const rf_fixture_t *fixture);
bool rf_exact_filter(rf_fixture_t *fixture, const char *expected);
bool rf_run_child(rf_fixture_t *fixture, const char *suite, const char *other, bool filtered,
                  rf_run_t *run);

#endif
