#include "test_framework.h"
#include "test_helpers.h"

#include "cli/cli.h"
#include "foundation/compat.h"
#include "foundation/constants.h"
#include "foundation/index_policy.h"
#include "mcp/index_supervisor.h"
#include "mcp/mcp.h"
#include "mcp/mcp_internal.h"
#include "store/store.h"
#include <yyjson/yyjson.h>

#include <sqlite3.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

TEST(index_policy_defaults_are_disabled) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);

    ASSERT_STR_EQ(cbm_index_policy_profile_name(&policy), "off");
    ASSERT_STR_EQ(cbm_index_policy_source_name(&policy), "off");
    ASSERT_FALSE(policy.max_files.enabled);
    ASSERT_FALSE(policy.max_directories.enabled);
    ASSERT_FALSE(policy.max_entries.enabled);
    ASSERT_FALSE(policy.max_depth.enabled);
    ASSERT_FALSE(policy.max_source_bytes.enabled);
    ASSERT_FALSE(policy.discovery_deadline_ms.enabled);
    ASSERT_FALSE(policy.max_rss_bytes.enabled);
    ASSERT_FALSE(policy.max_duration_ms.enabled);
    ASSERT_FALSE(policy.max_cache_bytes.enabled);
    ASSERT_FALSE(policy.min_free_disk_bytes.enabled);
    ASSERT_FALSE(policy.max_final_db_bytes.enabled);
    ASSERT_FALSE(policy.max_staging_bytes.enabled);
    ASSERT_FALSE(policy.max_task_temp_bytes.enabled);
    ASSERT_FALSE(cbm_index_policy_enabled(&policy));
    ASSERT_FALSE(cbm_index_policy_discovery_enabled(&policy));
    ASSERT_FALSE(cbm_index_policy_worker_enabled(&policy));
    ASSERT_FALSE(cbm_index_policy_storage_enabled(&policy));
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_FILES), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_SOURCE_MB), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_RSS_MB), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MAX_DURATION_SECONDS), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_CACHE_MAX_MB), "off");
    ASSERT_STR_EQ(cbm_index_policy_default_value(CBM_INDEX_CONFIG_MIN_FREE_DISK_MB), "off");
    PASS();
}

TEST(index_policy_balanced_profile_expands_exact_baseline) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(cbm_index_policy_set_profile(&policy, "balanced", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, UINT64_C(20) * 1024 * CBM_INDEX_MIB_BYTES,
                              UINT64_C(4) * 1024 * CBM_INDEX_MIB_BYTES);

    ASSERT_STR_EQ(cbm_index_policy_profile_name(&policy), "balanced");
    ASSERT_STR_EQ(cbm_index_policy_source_name(&policy), "profile");
    ASSERT_EQ(policy.max_files.value, UINT64_C(500000));
    ASSERT_EQ(policy.max_directories.value, UINT64_C(250000));
    ASSERT_EQ(policy.max_entries.value, UINT64_C(2000000));
    ASSERT_EQ(policy.max_depth.value, UINT64_C(128));
    ASSERT_EQ(policy.max_source_bytes.value, UINT64_C(65536) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.discovery_deadline_ms.value, UINT64_C(1800000));
    /* Four fifths of a 20 GiB host. */
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(16) * 1024 * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_duration_ms.value, UINT64_C(7200000));
    ASSERT_EQ(policy.max_final_db_bytes.value, UINT64_C(65536) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_staging_bytes.value, UINT64_C(81920) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_task_temp_bytes.value, UINT64_C(98304) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_cache_bytes.value, UINT64_C(131072) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.min_free_disk_bytes.value, UINT64_C(4096) * CBM_INDEX_MIB_BYTES);
    PASS();
}

/* A balanced ceiling must never sit below what the host can actually finish.
 * The large-repository runs this project records as reference workloads peak
 * in the tens of gigabytes, so a fixed balanced ceiling would reject indexes
 * that complete today with the profile off. */
TEST(index_policy_balanced_rss_follows_the_host_share) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(cbm_index_policy_set_profile(&policy, "balanced", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, UINT64_C(40) * 1024 * CBM_INDEX_MIB_BYTES,
                              UINT64_C(4) * 1024 * CBM_INDEX_MIB_BYTES);
    ASSERT_TRUE(policy.max_rss_bytes.enabled);
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(32) * 1024 * CBM_INDEX_MIB_BYTES);

    /* A small host still receives a real ceiling below its own memory. */
    cbm_index_policy_init(&policy);
    ASSERT_TRUE(cbm_index_policy_set_profile(&policy, "balanced", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, UINT64_C(10) * 1024 * CBM_INDEX_MIB_BYTES,
                              UINT64_C(2) * 1024 * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(8) * 1024 * CBM_INDEX_MIB_BYTES);

    /* Host memory that cannot be read falls back to the fixed floor. */
    cbm_index_policy_init(&policy);
    ASSERT_TRUE(cbm_index_policy_set_profile(&policy, "balanced", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, 0, UINT64_C(6) * 1024 * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(12) * 1024 * CBM_INDEX_MIB_BYTES);
    PASS();
}

TEST(index_policy_strict_profile_expands_exact_baseline) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(cbm_index_policy_set_profile(&policy, "strict", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, 0, UINT64_C(3) * 1024 * CBM_INDEX_MIB_BYTES);

    ASSERT_STR_EQ(cbm_index_policy_profile_name(&policy), "strict");
    ASSERT_EQ(policy.max_files.value, UINT64_C(100000));
    ASSERT_EQ(policy.max_directories.value, UINT64_C(20000));
    ASSERT_EQ(policy.max_entries.value, UINT64_C(500000));
    ASSERT_EQ(policy.max_depth.value, UINT64_C(64));
    ASSERT_EQ(policy.max_source_bytes.value, UINT64_C(4096) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.discovery_deadline_ms.value, UINT64_C(30000));
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(3072) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_duration_ms.value, UINT64_C(3600000));
    ASSERT_EQ(policy.max_final_db_bytes.value, UINT64_C(16384) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_staging_bytes.value, UINT64_C(20480) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_task_temp_bytes.value, UINT64_C(24576) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_cache_bytes.value, UINT64_C(32768) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.min_free_disk_bytes.value, UINT64_C(4096) * CBM_INDEX_MIB_BYTES);
    PASS();
}

TEST(index_policy_explicit_override_and_off_replace_profile_dimensions) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(cbm_index_policy_set_profile(&policy, "balanced", error, sizeof(error)));
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_FILES, "42", error, sizeof(error)));
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, "off", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, UINT64_C(1024) * CBM_INDEX_MIB_BYTES,
                              UINT64_C(512) * CBM_INDEX_MIB_BYTES);

    ASSERT_EQ(policy.max_files.value, UINT64_C(42));
    ASSERT_FALSE(policy.max_rss_bytes.enabled);
    ASSERT_TRUE(policy.max_entries.enabled);
    ASSERT_STR_EQ(cbm_index_policy_source_name(&policy), "profile+override");
    PASS();
}

TEST(index_policy_host_clamp_only_tightens_explicit_rss) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];

    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, "4096", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, UINT64_C(2048) * CBM_INDEX_MIB_BYTES, 0);
    ASSERT_EQ(policy.max_rss_bytes.value,
              (UINT64_C(2048) * CBM_INDEX_MIB_BYTES * UINT64_C(4)) / UINT64_C(5));

    cbm_index_policy_init(&policy);
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MAX_RSS_MB, "512", error, sizeof(error)));
    cbm_index_policy_finalize(&policy, UINT64_C(2048) * CBM_INDEX_MIB_BYTES, 0);
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(512) * CBM_INDEX_MIB_BYTES);
    PASS();
}

TEST(index_policy_profile_validation_is_atomic) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];
    ASSERT_TRUE(cbm_index_policy_set_profile(&policy, "strict", error, sizeof(error)));
    cbm_index_resource_policy_t before = policy;
    ASSERT_FALSE(cbm_index_policy_set_profile(&policy, "STRICT", error, sizeof(error)));
    ASSERT_EQ(memcmp(&policy, &before, sizeof(policy)), 0);
    ASSERT_TRUE(strstr(error, CBM_INDEX_CONFIG_RESOURCE_PROFILE) != NULL);
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

TEST(index_policy_storage_limits_validate_and_convert_units) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    char error[256];
    char value[64];

    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_CACHE_MAX_MB, "131072", error,
                                     sizeof(error)));
    ASSERT_TRUE(cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_MIN_FREE_DISK_MB, "4096", error,
                                     sizeof(error)));
    ASSERT_EQ(policy.max_cache_bytes.value, UINT64_C(131072) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.min_free_disk_bytes.value, UINT64_C(4096) * CBM_INDEX_MIB_BYTES);
    ASSERT_TRUE(cbm_index_policy_storage_enabled(&policy));
    ASSERT_TRUE(
        cbm_index_policy_format(&policy, CBM_INDEX_CONFIG_CACHE_MAX_MB, value, sizeof(value)));
    ASSERT_STR_EQ(value, "131072");
    ASSERT_TRUE(
        cbm_index_policy_set(&policy, CBM_INDEX_CONFIG_CACHE_MAX_MB, "off", error, sizeof(error)));
    ASSERT_FALSE(policy.max_cache_bytes.enabled);
    PASS();
}

TEST(index_policy_storage_projection_boundaries_are_overflow_safe) {
    cbm_index_resource_policy_t policy;
    cbm_index_policy_init(&policy);
    policy.max_cache_bytes = (cbm_index_limit_u64_t){.enabled = true, .value = 100};
    policy.min_free_disk_bytes = (cbm_index_limit_u64_t){.enabled = true, .value = 40};
    policy.max_final_db_bytes = (cbm_index_limit_u64_t){.enabled = true, .value = 60};
    policy.max_staging_bytes = (cbm_index_limit_u64_t){.enabled = true, .value = 70};
    policy.max_task_temp_bytes = (cbm_index_limit_u64_t){.enabled = true, .value = 80};
    cbm_index_storage_sample_t sample = {
        .current_cache_bytes = 130,
        .replaceable_old_bytes = 50,
        .operation_bytes = 20,
        .free_disk_bytes = 40,
        .final_db_bytes = 60,
        .staging_bytes = 70,
        .task_temp_bytes = 80,
    };
    cbm_index_resource_violation_t violation = {0};

    ASSERT_TRUE(cbm_index_policy_check_storage(&policy, &sample, &violation));
    sample.current_cache_bytes++;
    ASSERT_FALSE(cbm_index_policy_check_storage(&policy, &sample, &violation));
    ASSERT_EQ(violation.resource, CBM_INDEX_RESOURCE_CACHE_BYTES);
    ASSERT_EQ(violation.observed, 101);
    ASSERT_EQ(violation.limit, 100);
    sample.current_cache_bytes--;
    sample.free_disk_bytes--;
    ASSERT_FALSE(cbm_index_policy_check_storage(&policy, &sample, &violation));
    ASSERT_EQ(violation.resource, CBM_INDEX_RESOURCE_FREE_DISK_BYTES);
    ASSERT_EQ(violation.observed, 39);
    sample = (cbm_index_storage_sample_t){
        .current_cache_bytes = UINT64_MAX,
        .replaceable_old_bytes = 0,
        .operation_bytes = UINT64_MAX,
        .free_disk_bytes = UINT64_MAX,
    };
    ASSERT_FALSE(cbm_index_policy_check_storage(&policy, &sample, &violation));
    ASSERT_EQ(violation.resource, CBM_INDEX_RESOURCE_CACHE_BYTES);
    ASSERT_EQ(violation.observed, UINT64_MAX);
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
    ASSERT_STR_EQ(cbm_index_resource_name(CBM_INDEX_RESOURCE_CACHE_BYTES), "cache_bytes");
    ASSERT_STR_EQ(cbm_index_resource_name(CBM_INDEX_RESOURCE_FREE_DISK_BYTES), "free_disk_bytes");
    ASSERT_STR_EQ(cbm_index_resource_name(CBM_INDEX_RESOURCE_STAGING_BYTES), "staging_bytes");
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
    ASSERT_STR_EQ(cbm_index_resource_config_key(CBM_INDEX_RESOURCE_CACHE_BYTES),
                  CBM_INDEX_CONFIG_CACHE_MAX_MB);
    ASSERT_STR_EQ(cbm_index_resource_config_key(CBM_INDEX_RESOURCE_FREE_DISK_BYTES),
                  CBM_INDEX_CONFIG_MIN_FREE_DISK_MB);
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
    ASSERT_STR_EQ(cbm_index_policy_profile_name(&policy), "off");
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_RESOURCE_PROFILE, "strict"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_FILES, "9"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_SOURCE_MB, "3"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_RSS_MB, "64"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS, "7"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_CACHE_MAX_MB, "17"), 0);
    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MIN_FREE_DISK_MB, "5"), 0);
    ASSERT_TRUE(cbm_config_load_index_policy(config, &policy, error, sizeof(error)));
    ASSERT_EQ(policy.max_files.value, 9);
    ASSERT_STR_EQ(cbm_index_policy_profile_name(&policy), "strict");
    ASSERT_EQ(policy.max_directories.value, UINT64_C(20000));
    ASSERT_EQ(policy.max_source_bytes.value, UINT64_C(3) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_rss_bytes.value, UINT64_C(64) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.max_duration_ms.value, UINT64_C(7000));
    ASSERT_EQ(policy.max_cache_bytes.value, UINT64_C(17) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(policy.min_free_disk_bytes.value, UINT64_C(5) * CBM_INDEX_MIB_BYTES);

    ASSERT_EQ(cbm_config_set(config, CBM_INDEX_CONFIG_MAX_FILES, "corrupt"), 0);
    ASSERT_FALSE(cbm_config_load_index_policy(config, &policy, error, sizeof(error)));
    ASSERT_TRUE(strstr(error, CBM_INDEX_CONFIG_MAX_FILES) != NULL);

    cbm_config_close(config);
    th_cleanup(cache);
    PASS();
}

TEST(index_policy_cli_lists_all_operator_keys) {
    bool profile_found = false;
    bool files_found = false;
    bool bytes_found = false;
    bool rss_found = false;
    bool duration_found = false;
    bool cache_found = false;
    bool free_found = false;
    for (size_t index = 0; index < cbm_cli_config_key_count_for_testing(); index++) {
        const char *key = cbm_cli_config_key_at_for_testing(index);
        profile_found =
            profile_found || (key && strcmp(key, CBM_INDEX_CONFIG_RESOURCE_PROFILE) == 0);
        files_found = files_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_FILES) == 0);
        bytes_found = bytes_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_SOURCE_MB) == 0);
        rss_found = rss_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_RSS_MB) == 0);
        duration_found =
            duration_found || (key && strcmp(key, CBM_INDEX_CONFIG_MAX_DURATION_SECONDS) == 0);
        cache_found = cache_found || (key && strcmp(key, CBM_INDEX_CONFIG_CACHE_MAX_MB) == 0);
        free_found = free_found || (key && strcmp(key, CBM_INDEX_CONFIG_MIN_FREE_DISK_MB) == 0);
    }
    ASSERT_TRUE(profile_found);
    ASSERT_TRUE(files_found);
    ASSERT_TRUE(bytes_found);
    ASSERT_TRUE(rss_found);
    ASSERT_TRUE(duration_found);
    ASSERT_TRUE(cache_found);
    ASSERT_TRUE(free_found);
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
    char *set_profile[] = {"set", CBM_INDEX_CONFIG_RESOURCE_PROFILE, "balanced"};
    char *set_invalid_profile[] = {"set", CBM_INDEX_CONFIG_RESOURCE_PROFILE, "BALANCED"};
    char *reset[] = {"reset", CBM_INDEX_CONFIG_MAX_FILES};
    int valid_rc = cbm_cmd_config(3, set_valid);
    cbm_config_t *config = cbm_config_open(cache);
    const char *stored =
        config ? cbm_config_get(config, CBM_INDEX_CONFIG_MAX_FILES, "missing") : "missing";
    bool valid_stored = strcmp(stored, "7") == 0;
    int invalid_rc = cbm_cmd_config(3, set_invalid);
    stored = config ? cbm_config_get(config, CBM_INDEX_CONFIG_MAX_FILES, "missing") : "missing";
    bool invalid_preserved = strcmp(stored, "7") == 0;
    int profile_rc = cbm_cmd_config(3, set_profile);
    stored =
        config ? cbm_config_get(config, CBM_INDEX_CONFIG_RESOURCE_PROFILE, "missing") : "missing";
    bool profile_stored = strcmp(stored, "balanced") == 0;
    int invalid_profile_rc = cbm_cmd_config(3, set_invalid_profile);
    stored =
        config ? cbm_config_get(config, CBM_INDEX_CONFIG_RESOURCE_PROFILE, "missing") : "missing";
    bool invalid_profile_preserved = strcmp(stored, "balanced") == 0;
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
    ASSERT_EQ(profile_rc, 0);
    ASSERT_TRUE(profile_stored);
    ASSERT_TRUE(invalid_profile_rc != 0);
    ASSERT_TRUE(invalid_profile_preserved);
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

TEST(index_policy_worker_contract_preserves_profile_only_dimensions) {
    cbm_index_resource_policy_t parent;
    cbm_index_policy_init(&parent);
    char error[256];
    ASSERT_TRUE(cbm_index_policy_set_profile(&parent, "strict", error, sizeof(error)));
    cbm_index_policy_finalize(&parent, UINT64_C(16) * 1024 * CBM_INDEX_MIB_BYTES,
                              UINT64_C(32) * CBM_INDEX_MIB_BYTES);
    ASSERT_EQ(parent.max_rss_bytes.value, CBM_INDEX_MIN_RSS_MB_VALUE * CBM_INDEX_MIB_BYTES);

    yyjson_mut_doc *document = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(document);
    yyjson_mut_doc_set_root(document, root);
    ASSERT_TRUE(cbm_mcp_index_policy_add_to_args(document, root, &parent));
    char *args = yyjson_mut_write(document, 0, NULL);
    ASSERT_NOT_NULL(args);

    cbm_index_resource_policy_t worker;
    ASSERT_TRUE(cbm_mcp_index_policy_from_internal_args(args, &worker, error, sizeof(error)));
    ASSERT_STR_EQ(cbm_index_policy_profile_name(&worker), "strict");
    ASSERT_STR_EQ(cbm_index_policy_source_name(&worker), "profile");
    ASSERT_EQ(worker.max_directories.value, parent.max_directories.value);
    ASSERT_EQ(worker.max_entries.value, parent.max_entries.value);
    ASSERT_EQ(worker.max_depth.value, parent.max_depth.value);
    ASSERT_EQ(worker.discovery_deadline_ms.value, parent.discovery_deadline_ms.value);
    ASSERT_EQ(worker.max_final_db_bytes.value, parent.max_final_db_bytes.value);
    ASSERT_EQ(worker.max_staging_bytes.value, parent.max_staging_bytes.value);
    ASSERT_EQ(worker.max_task_temp_bytes.value, parent.max_task_temp_bytes.value);
    ASSERT_EQ(worker.max_rss_bytes.value, parent.max_rss_bytes.value);

    free(args);
    yyjson_mut_doc_free(document);
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
                   "\"mode\":\"fast\",\"_cbm_index_origin\":\"watcher\","
                   "\"_cbm_index_policy\":{"
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
    char attempt_path[2048];
    (void)snprintf(attempt_path, sizeof(attempt_path), "%s/status/ResourcePolicyFixture.json",
                   cache);
    yyjson_doc *attempt_record = yyjson_read_file(attempt_path, 0, NULL, NULL);
    yyjson_val *attempt_root = attempt_record ? yyjson_doc_get_root(attempt_record) : NULL;
    yyjson_val *attempt_origin =
        attempt_root && yyjson_is_obj(attempt_root) ? yyjson_obj_get(attempt_root, "origin") : NULL;
    bool origin_not_forged = attempt_origin && yyjson_is_str(attempt_origin) &&
                             strcmp(yyjson_get_str(attempt_origin), "explicit") == 0;
    yyjson_doc_free(attempt_record);

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
    ASSERT_TRUE(origin_not_forged);
    ASSERT_TRUE(generation_preserved);
    ASSERT_TRUE(cross_repo_unaffected);
    PASS();
}

TEST(index_policy_storage_limit_preserves_old_index_and_unrelated_cache_files) {
    char *repo = th_mktempdir("cbm_index_storage_repo");
    char *cache = th_mktempdir("cbm_index_storage_cache");
    ASSERT_NOT_NULL(repo);
    ASSERT_NOT_NULL(cache);
    ASSERT_EQ(th_write_file(TH_PATH(repo, "main.c"), "int original(void) { return 1; }\n"), 0);
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    (void)cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_config_t *config = cbm_config_open(cache);
    cbm_mcp_server_t *server = cbm_mcp_server_new(NULL);
    if (server && config) {
        cbm_mcp_server_set_config(server, config);
    }
    char args[CBM_SZ_4K];
    (void)snprintf(args, sizeof(args),
                   "{\"repo_path\":\"%s\",\"name\":\"StoragePolicyFixture\",\"mode\":\"fast\"}",
                   repo);
    char *first = server ? cbm_mcp_handle_tool(server, "index_repository", args) : NULL;
    bool first_ok = first && strstr(first, "\\\"status\\\":\\\"indexed\\\"");
    free(first);

    char unrelated[CBM_SZ_4K];
    (void)snprintf(unrelated, sizeof(unrelated), "%s/unrelated.bin", cache);
    FILE *large = cbm_fopen(unrelated, "wb");
    char block[4096] = {0};
    bool wrote_large = large != NULL;
    for (int index = 0; wrote_large && index < 512; index++) {
        wrote_large = fwrite(block, 1, sizeof(block), large) == sizeof(block);
    }
    if (large) {
        wrote_large = fclose(large) == 0 && wrote_large;
    }
    bool configured =
        config && cbm_config_set(config, CBM_INDEX_CONFIG_CACHE_MAX_MB, "1") == 0 &&
        th_write_file(TH_PATH(repo, "main.c"), "int changed(void) { return 2; }\n") == 0;
    (void)snprintf(args, sizeof(args),
                   "{\"repo_path\":\"%s\",\"name\":\"StoragePolicyFixture\",\"mode\":\"fast\","
                   "\"_cbm_index_policy\":{\"index_max_files\":\"off\","
                   "\"index_max_source_mb\":\"off\",\"index_cache_max_mb\":\"off\","
                   "\"index_min_free_disk_mb\":\"off\"}}",
                   repo);
    char *limited = configured && wrote_large && server
                        ? cbm_mcp_handle_tool(server, "index_repository", args)
                        : NULL;
    bool contract_ok = limited && strstr(limited, "resource_limit_exceeded") &&
                       strstr(limited, "\\\"stage\\\":\\\"storage\\\"") &&
                       strstr(limited, "\\\"resource\\\":\\\"cache_bytes\\\"") &&
                       strstr(limited, "\\\"limit\\\":1048576") &&
                       strstr(limited, "\\\"unit\\\":\\\"bytes\\\"") &&
                       strstr(limited, "\\\"serving_index_preserved\\\":true");
    free(limited);
    int64_t unrelated_size = cbm_file_size(unrelated);
    char db_path[CBM_SZ_4K];
    (void)snprintf(db_path, sizeof(db_path), "%s/StoragePolicyFixture.db", cache);
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    bool old_queryable = store && cbm_store_count_nodes(store, "StoragePolicyFixture") > 0;
    cbm_store_close(store);

    cbm_mcp_server_free(server);
    cbm_config_close(config);
    th_cleanup(repo);
    th_cleanup(cache);
    if (saved_cache_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_cache_copy);

    ASSERT_TRUE(first_ok);
    ASSERT_TRUE(wrote_large);
    ASSERT_TRUE(configured);
    ASSERT_TRUE(contract_ok);
    ASSERT_EQ(unrelated_size, (int64_t)sizeof(block) * 512);
    ASSERT_TRUE(old_queryable);
    PASS();
}

SUITE(index_policy) {
    RUN_TEST(index_policy_defaults_are_disabled);
    RUN_TEST(index_policy_balanced_profile_expands_exact_baseline);
    RUN_TEST(index_policy_balanced_rss_follows_the_host_share);
    RUN_TEST(index_policy_strict_profile_expands_exact_baseline);
    RUN_TEST(index_policy_explicit_override_and_off_replace_profile_dimensions);
    RUN_TEST(index_policy_host_clamp_only_tightens_explicit_rss);
    RUN_TEST(index_policy_profile_validation_is_atomic);
    RUN_TEST(index_policy_file_limit_accepts_off_and_exact_range);
    RUN_TEST(index_policy_source_limit_converts_mib_without_overflow);
    RUN_TEST(index_policy_worker_limits_validate_and_convert_units);
    RUN_TEST(index_policy_storage_limits_validate_and_convert_units);
    RUN_TEST(index_policy_storage_projection_boundaries_are_overflow_safe);
    RUN_TEST(index_policy_invalid_value_is_rejected_atomically);
    RUN_TEST(index_policy_format_round_trips_public_values);
    RUN_TEST(index_policy_violation_metadata_is_stable);
    RUN_TEST(index_policy_config_loads_defaults_values_and_rejects_corruption);
    RUN_TEST(index_policy_cli_lists_all_operator_keys);
    RUN_TEST(index_policy_cli_set_rejects_invalid_value_without_overwrite);
    RUN_TEST(index_policy_cli_set_reports_a_failed_write);
    RUN_TEST(index_policy_worker_rejects_missing_parent_policy);
    RUN_TEST(index_policy_worker_contract_preserves_profile_only_dimensions);
    RUN_TEST(index_policy_mcp_rejects_forged_override_and_preserves_serving_index);
    RUN_TEST(index_policy_storage_limit_preserves_old_index_and_unrelated_cache_files);
}
