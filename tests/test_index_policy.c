#include "test_framework.h"
#include "test_helpers.h"

#include "cli/cli.h"
#include "foundation/compat.h"
#include "foundation/index_policy.h"
#include "mcp/index_supervisor.h"
#include "mcp/mcp.h"
#include "store/store.h"

#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

TEST(index_policy_defaults_are_disabled) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);

    ASSERT_FALSE(policy.max_files.enabled);
    ASSERT_FALSE(policy.max_source_bytes.enabled);
    ASSERT_FALSE(policy.max_rss_bytes.enabled);
    ASSERT_FALSE(policy.max_duration_ms.enabled);
    ASSERT_FALSE(cbm_index_policy_enabled(&policy));
    ASSERT_FALSE(cbm_index_policy_discovery_enabled(&policy));
    ASSERT_FALSE(cbm_index_policy_worker_enabled(&policy));
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_FILES), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_SOURCE_MB), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_RSS_MB), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_DURATION_SECONDS), "off");
    PASS();
}

TEST(index_policy_file_limit_accepts_off_and_exact_range) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_FILES, "1", error, sizeof(error)));
    ASSERT_TRUE(policy.max_files.enabled);
    ASSERT_EQ(policy.max_files.value, 1);
    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_FILES, "10000000", error,
                                     sizeof(error)));
    ASSERT_EQ(policy.max_files.value, 10000000);
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_FILES, "off", error, sizeof(error)));
    ASSERT_FALSE(policy.max_files.enabled);
    PASS();
}

TEST(index_policy_source_limit_converts_mib_without_overflow) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_SOURCE_MB, "1", error, sizeof(error)));
    ASSERT_TRUE(policy.max_source_bytes.enabled);
    ASSERT_EQ(policy.max_source_bytes.value, UINT64_C(1024) * UINT64_C(1024));
    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_SOURCE_MB, "1048576", error,
                                     sizeof(error)));
    ASSERT_EQ(policy.max_source_bytes.value, UINT64_C(1048576) * UINT64_C(1024) * UINT64_C(1024));
    PASS();
}

TEST(index_policy_worker_limits_validate_and_convert_units) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, "64", error, sizeof(error)));
    ASSERT_TRUE(cbm_index_policy_enabled(&policy));
    ASSERT_TRUE(cbm_index_policy_worker_enabled(&policy));
    ASSERT_FALSE(cbm_index_policy_discovery_enabled(&policy));
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(64) * CBM_INDEX_MIB_BYTES);
    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, "1048576", error,
                                     sizeof(error)));
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(1048576) * CBM_INDEX_MIB_BYTES);
    cbm_index_resource_policy_t before = policy;
    ASSERT_FALSE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, "63", error, sizeof(error)));
    ASSERT_EQ(memcmp(&policy, &before, sizeof(policy)), 0);

    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, "1", error,
                                     sizeof(error)));
    ASSERT_EQ(policy.max_duration_ms.value, 1000);
    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, "86400", error,
                                     sizeof(error)));
    ASSERT_EQ(policy.max_duration_ms.value, UINT64_C(86400000));
    before = policy;
    ASSERT_FALSE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, "86401",
                                      error, sizeof(error)));
    ASSERT_EQ(memcmp(&policy, &before, sizeof(policy)), 0);
    PASS();
}

TEST(index_policy_invalid_value_is_rejected_atomically) {
    static const char *const invalid[] = {"",   "0",  "-1",       "1MB",
                                          "1 ", "+1", "10000001", "18446744073709551616"};
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_FILES, "42", error, sizeof(error)));

    for (size_t index = 0; index < sizeof(invalid) / sizeof(invalid[0]); index++) {
        cbm_index_resource_policy_t before = policy;
        ASSERT_FALSE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_FILES, invalid[index],
                                          error, sizeof(error)));
        ASSERT_EQ(memcmp(&policy, &before, sizeof(policy)), 0);
        ASSERT_TRUE(error[0] != '\0');
    }
    ASSERT_FALSE(cbm_index_policy_set(&policy, "index_unknown_limit", "1", error, sizeof(error)));
    PASS();
}

TEST(index_policy_format_round_trips_public_values) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];
    char value[64];

    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_FILES, "321", error, sizeof(error)));
    ASSERT_TRUE(cbm_index_policy_format(&policy, CBM_INDEX_CONFIG_MAX_FILES, value, sizeof(value)));
    ASSERT_STR_EQ(value, "321");
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_SOURCE_MB, "7", error, sizeof(error)));
    ASSERT_TRUE(
        cbm_index_policy_format(&policy, CBM_INDEX_CONFIG_MAX_SOURCE_MB, value, sizeof(value)));
    ASSERT_STR_EQ(value, "7");
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, "64", error, sizeof(error)));
    ASSERT_TRUE(
        cbm_index_policy_format(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, value, sizeof(value)));
    ASSERT_STR_EQ(value, "64");
    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, "9", error,
                                     sizeof(error)));
    ASSERT_TRUE(cbm_index_policy_format(&policy, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, value,
                                        sizeof(value)));
    ASSERT_STR_EQ(value, "9");
    PASS();
}

TEST(index_policy_violation_metadata_is_stable) {
    ASSERT_STR_EQ(cbm_index_resource_name(CBM_INDEX_RESOURCE_FILES), "files");
    ASSERT_STR_EQ(cbm_index_resource_name(CBM_INDEX_RESOURCE_SOURCE_BYTES), "source_bytes");
    ASSERT_STR_EQ(cbm_index_resource_unit(CBM_INDEX_RESOURCE_FILES), "files");
    ASSERT_STR_EQ(cbm_index_resource_unit(CBM_INDEX_RESOURCE_SOURCE_BYTES), "bytes");
    ASSERT_STR_EQ(cbm_index_resource_name(CBM_INDEX_RESOURCE_RSS_BYTES), "rss_bytes");
    ASSERT_STR_EQ(cbm_index_resource_name(CBM_INDEX_RESOURCE_DURATION_MS), "duration_ms");
    ASSERT_STR_EQ(cbm_index_resource_unit(CBM_INDEX_RESOURCE_RSS_BYTES), "bytes");
    ASSERT_STR_EQ(cbm_index_resource_unit(CBM_INDEX_RESOURCE_DURATION_MS), "milliseconds");
    ASSERT_STR_EQ(cbm_index_resource_config_key(CBM_INDEX_RESOURCE_FILES),
                  CBM_INDEX_CONFIG_MAX_FILES);
    ASSERT_STR_EQ(cbm_index_resource_config_key(CBM_INDEX_RESOURCE_SOURCE_BYTES),
                  CBM_INDEX_CONFIG_MAX_SOURCE_MB);
    ASSERT_STR_EQ(cbm_index_resource_config_key(CBM_INDEX_RESOURCE_RSS_BYTES),
                  CBM_INDEX_CONFIG_MAX_RSS_MB);
    ASSERT_STR_EQ(cbm_index_resource_config_key(CBM_INDEX_RESOURCE_DURATION_MS),
                  CBM_INDEX_CONFIG_MAX_DURATION_SECONDS);
    PASS();
}

TEST(index_policy_config_loads_defaults_values_and_rejects_corruption) {
    char *cache = th_mktempdir("cbm_index_policy_config");
    ASSERT_NOT_NULL(cache);
    cbm_config_t *config = cbm_config_open(cache);
    ASSERT_NOT_NULL(config);
    cbm_index_resource_policy_t policy;
    char error[256];

    ASSERT_TRUE(cbm_config_load_index_policy(config, &policy, error, sizeof(error)));
    ASSERT_FALSE(cbm_index_policy_enabled(&policy));
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_FILES, "9"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_SOURCE_MB, "3"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_RSS_MB, "64"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, "7"), 0);
    ASSERT_TRUE(cbm_config_load_index_policy(config, &policy, error, sizeof(error)));
    ASSERT_EQ(policy.max_files.value, 9);
    ASSERT_EQ(policy.max_source_bytes.value, UINT64_C(3) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(64) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_duration_ms.value, UINT64_C(7000));

    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_FILES, "corrupt"), 0);
    ASSERT_FALSE(cbm_config_load_index_policy(config, &policy, error, sizeof(error)));
    ASSERT_TRUE(strstr(error, CBM_INDEX_CONFIG_MAX_FILES) != NULL);

    cbm_config_close(config);
    th_cleanup(cache);
    PASS();
}

TEST(index_policy_cli_lists_all_operator_keys) {
    bool files_found = false;
    bool bytes_found = false;
    bool rss_found = false;
    bool duration_found = false;
    for (size_t index = 0; index < cbm_cli_config_key_count_for_testing(); index++) {
        const char *key = cbm_cli_config_key_at_for_testing(index);
        files_found = files_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_FILES) == 0);
        bytes_found = bytes_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_SOURCE_MB) == 0);
        rss_found = rss_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_RSS_MB) == 0);
        duration_found =
            duration_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS) == 0);
    }
    ASSERT_TRUE(files_found);
    ASSERT_TRUE(bytes_found);
    ASSERT_TRUE(rss_found);
    ASSERT_TRUE(duration_found);
    PASS();
}

TEST(index_policy_cli_set_rejects_invalid_value_without_overwrite) {
    char *cache = th_mktempdir("cbm_index_policy_cli");
    ASSERT_NOT_NULL(cache);
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char *set_valid[] = {"set", CBM_INDEX_CONFIG_MAX_FILES, "7"};
    char *set_invalid[] = {"set", CBM_INDEX_CONFIG_MAX_FILES, "0"};
    char *reset[] = {"reset", CBM_INDEX_CONFIG_MAX_FILES};
    int valid_rc = cbm_cmd_config(3, set_valid);
    cbm_config_t *config = cbm_config_open(cache);
    const char *stored =
        config ? cbm_config_get(config, CBM_INDEX_CONFIG_MAX_FILES, "missing") : "missing";
    bool valid_stored = strcmp(stored, "7") == 0;
    int invalid_rc = cbm_cmd_config(3, set_invalid);
    stored = config ? cbm_config_get(config, CBM_INDEX_CONFIG_MAX_FILES, "missing") : "missing";
    bool invalid_preserved = strcmp(stored, "7") == 0;
    int reset_rc = cbm_cmd_config(2, reset);
    stored = config ? cbm_config_get(config, CBM_INDEX_CONFIG_MAX_FILES, "off") : "missing";
    bool reset_to_default = strcmp(stored, "off") == 0;
    cbm_config_close(config);

    if (saved_cache_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_cache_copy);
    th_cleanup(cache);

    ASSERT_EQ(valid_rc, 0);
    ASSERT_TRUE(valid_stored);
    ASSERT_TRUE(invalid_rc != 0);
    ASSERT_TRUE(invalid_preserved);
    ASSERT_EQ(reset_rc, 0);
    ASSERT_TRUE(reset_to_default);
    PASS();
}

/* Capture stderr across one cbm_cmd_config invocation, mirroring the stdout
 * idiom in test_cli.c: tmpfile+dup2+rewind is what works on the MinGW leg. */
static int index_policy_config_stderr(int argc, char **argv, char *out, size_t cap) {
    out[0] = '\0';
    FILE *capture = tmpfile();
    int saved = capture ? dup(fileno(stderr)) : -1;
    if (!capture || saved < 0) {
        if (capture) {
            (void)fclose(capture);
        }
        if (saved >= 0) {
            (void)close(saved);
        }
        return -1000;
    }
    (void)fflush(stderr);
    if (dup2(fileno(capture), fileno(stderr)) < 0) {
        (void)fclose(capture);
        (void)close(saved);
        return -1000;
    }
    int rc = cbm_cmd_config(argc, argv);
    (void)fflush(stderr);
    (void)dup2(saved, fileno(stderr));
    (void)close(saved);
    rewind(capture);
    size_t got = fread(out, 1, cap - 1, capture);
    out[got] = '\0';
    (void)fclose(capture);
    return rc;
}

/* `config set` suppresses its own generic message for a policy key, trusting
 * the policy writer to name the precise reason. That trust held only for a
 * value the writer rejects. A value it accepts whose write then fails — a
 * _config.db that cannot be written — exited non-zero having printed nothing
 * at all, which is a CLI that failed in silence.
 *
 * The unwritable database here is a `config` that is a view: CREATE TABLE IF
 * NOT EXISTS finds the name taken and succeeds, so the command gets past open
 * with a healthy handle and fails at the INSERT. That is the exact path with
 * no message, and it needs no file permissions, so it behaves the same on
 * every leg and under a root CI container. */
TEST(index_policy_cli_set_reports_a_failed_write) {
    char *cache = th_mktempdir("cbm_index_policy_cli_write");
    ASSERT_NOT_NULL(cache);
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);

    char db_path[1024];
    (void)snprintf(db_path, sizeof(db_path), "%s/_config.db", cache);
    sqlite3 *db = NULL;
    bool fixture_ready =
        sqlite3_open(db_path, &db) == SQLITE_OK &&
        sqlite3_exec(db, "CREATE VIEW config(key, value) AS SELECT 'pinned', 'pinned'", NULL, NULL,
                     NULL) == SQLITE_OK;
    if (db) {
        (void)sqlite3_close(db);
    }

    char captured[1024];
    char *set_valid[] = {"set", CBM_INDEX_CONFIG_MAX_FILES, "7"};
    int rc = index_policy_config_stderr(3, set_valid, captured, sizeof(captured));

    if (saved_cache_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_cache_copy);
    th_cleanup(cache);

    ASSERT_TRUE(fixture_ready);
    ASSERT_TRUE(rc != 0);
    ASSERT_TRUE(strstr(captured, CBM_INDEX_CONFIG_MAX_FILES) != NULL);
    PASS();
}

TEST(index_policy_worker_rejects_missing_parent_policy) {
    char *repo = th_mktempdir("cbm_index_policy_worker");
    ASSERT_NOT_NULL(repo);
    char args[1024];
    (void)snprintf(args, sizeof(args), "{\"repo_path\":\"%s\",\"mode\":\"fast\"}", repo);
    cbm_mcp_server_t *server = cbm_mcp_server_new(NULL);

    cbm_index_set_worker_role(true, NULL);
    char *response = server ? cbm_mcp_handle_tool(server, "index_repository", args) : NULL;
    cbm_index_set_worker_role(false, NULL);
    bool rejected = response && strstr(response, "missing or incomplete trusted worker policy");

    free(response);
    cbm_mcp_server_free(server);
    th_cleanup(repo);
    ASSERT_TRUE(rejected);
    PASS();
}

TEST(index_policy_mcp_rejects_forged_override_and_preserves_serving_index) {
    char *repo = th_mktempdir("cbm_index_policy_mcp_repo");
    char *cache = th_mktempdir("cbm_index_policy_mcp_cache");
    ASSERT_NOT_NULL(repo);
    ASSERT_NOT_NULL(cache);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "first.c"), "int first(void) { return 1; }\n"), 0);

    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_config_t *config = cbm_config_open(cache);
    cbm_mcp_server_t *server = cbm_mcp_server_new(NULL);
    if (server && config) {
        cbm_mcp_server_set_config(server, config);
    }

    char args[2048];
    (void)snprintf(args, sizeof(args),
                   "{\"repo_path\":\"%s\",\"name\":\"ResourcePolicyFixture\","
                   "\"mode\":\"fast\"}",
                   repo);
    char *first_response =
        server && config ? cbm_mcp_handle_tool(server, "index_repository", args) : NULL;
    bool first_indexed = first_response && strstr(first_response, "\\\"status\\\":\\\"indexed\\\"");
    free(first_response);

    char db_path[2048];
    (void)snprintf(db_path, sizeof(db_path), "%s/ResourcePolicyFixture.db", cache);
    cbm_store_t *before_store = cbm_store_open_path_query(db_path);
    cbm_project_t before_project = {0};
    bool before_read = before_store && cbm_store_get_project(before_store, "ResourcePolicyFixture",
                                                             &before_project) == CBM_STORE_OK;
    char *indexed_at_before =
        before_read && before_project.indexed_at ? strdup(before_project.indexed_at) : NULL;
    cbm_project_free_fields(&before_project);
    cbm_store_close(before_store);

    bool configured =
        config && cbm_config_set(config, CBM_INDEX_CONFIG_MAX_FILES, "1") == 0 &&
        th_write_file(TH_PATH(repo, "second.py"), "def second():\n    return 2\n") == 0;
    (void)snprintf(args, sizeof(args),
                   "{\"repo_path\":\"%s\",\"name\":\"ResourcePolicyFixture\","
                   "\"mode\":\"fast\",\"_cbm_index_policy\":{"
                   "\"index_max_files\":\"off\",\"index_max_source_mb\":\"off\"}}",
                   repo);
    char *limited_response =
        configured && server ? cbm_mcp_handle_tool(server, "index_repository", args) : NULL;
    bool contract_ok = limited_response && strstr(limited_response, "resource_limit_exceeded") &&
                       strstr(limited_response, "\\\"stage\\\":\\\"discovery\\\"") &&
                       strstr(limited_response, "\\\"resource\\\":\\\"files\\\"") &&
                       strstr(limited_response, "\\\"observed\\\":2") &&
                       strstr(limited_response, "\\\"limit\\\":1") &&
                       strstr(limited_response, "\\\"unit\\\":\\\"files\\\"") &&
                       strstr(limited_response, "\\\"retryable\\\":true") &&
                       strstr(limited_response, "\\\"serving_index_preserved\\\":true");
    free(limited_response);

    cbm_store_t *after_store = cbm_store_open_path_query(db_path);
    cbm_project_t after_project = {0};
    bool after_read = after_store && cbm_store_get_project(after_store, "ResourcePolicyFixture",
                                                           &after_project) == CBM_STORE_OK;
    bool generation_preserved = indexed_at_before && after_read && after_project.indexed_at &&
                                strcmp(indexed_at_before, after_project.indexed_at) == 0;
    cbm_project_free_fields(&after_project);
    cbm_store_close(after_store);
    free(indexed_at_before);

    (void)snprintf(args, sizeof(args),
                   "{\"repo_path\":\"%s\",\"name\":\"ResourcePolicyFixture\","
                   "\"mode\":\"cross-repo-intelligence\"}",
                   repo);
    char *cross_response = server ? cbm_mcp_handle_tool(server, "index_repository", args) : NULL;
    bool cross_repo_unaffected =
        cross_response && strstr(cross_response, "resource_limit_exceeded") == NULL;
    free(cross_response);

    cbm_mcp_server_free(server);
    cbm_config_close(config);
    (void)cbm_unlink(db_path);
    char sidecar[2100];
    (void)snprintf(sidecar, sizeof(sidecar), "%s-wal", db_path);
    (void)cbm_unlink(sidecar);
    (void)snprintf(sidecar, sizeof(sidecar), "%s-shm", db_path);
    (void)cbm_unlink(sidecar);
    th_cleanup(repo);
    th_cleanup(cache);
    if (saved_cache_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_cache_copy);

    ASSERT_TRUE(first_indexed);
    ASSERT_TRUE(configured);
    ASSERT_TRUE(contract_ok);
    ASSERT_TRUE(generation_preserved);
    ASSERT_TRUE(cross_repo_unaffected);
    PASS();
}

SUITE(index_policy) {
    RUN_TEST(index_policy_defaults_are_disabled);
    RUN_TEST(index_policy_file_limit_accepts_off_and_exact_range);
    RUN_TEST(index_policy_source_limit_converts_mib_without_overflow);
    RUN_TEST(index_policy_worker_limits_validate_and_convert_units);
    RUN_TEST(index_policy_invalid_value_is_rejected_atomically);
    RUN_TEST(index_policy_format_round_trips_public_values);
    RUN_TEST(index_policy_violation_metadata_is_stable);
    RUN_TEST(index_policy_config_loads_defaults_values_and_rejects_corruption);
    RUN_TEST(index_policy_cli_lists_all_operator_keys);
    RUN_TEST(index_policy_cli_set_rejects_invalid_value_without_overwrite);
    RUN_TEST(index_policy_cli_set_reports_a_failed_write);
    RUN_TEST(index_policy_worker_rejects_missing_parent_policy);
    RUN_TEST(index_policy_mcp_rejects_forged_override_and_preserves_serving_index);
}
