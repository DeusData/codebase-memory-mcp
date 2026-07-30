/*
 * test_index_limits.c — Unified index resource policy tests.
 */
#include "foundation/index_limits.h"
#include "cli/cli.h"
#include "test_framework.h"
#include "test_helpers.h"

#include <stdint.h>
#include <string.h>

#define TEST_MIB (UINT64_C(1024) * UINT64_C(1024))

TEST(index_limits_defaults_are_bounded) {
    cbm_index_limits_t limits;
    cbm_index_limits_defaults(&limits);

    ASSERT_EQ(limits.max_files, 100000);
    ASSERT_EQ(limits.max_directories, 20000);
    ASSERT_EQ(limits.max_entries, 500000);
    ASSERT_EQ(limits.max_depth, 64);
    ASSERT_EQ(limits.max_source_bytes, UINT64_C(4096) * TEST_MIB);
    ASSERT_EQ(limits.max_file_bytes, UINT64_C(64) * TEST_MIB);
    ASSERT_EQ(limits.scan_timeout_ms, 30000);
    ASSERT_EQ(limits.cpu_cores, 4);
    ASSERT_EQ(limits.concurrent_jobs, 1);
    ASSERT_EQ(limits.memory_limit_bytes, UINT64_C(8192) * TEST_MIB);
    ASSERT_EQ(limits.max_db_bytes, UINT64_C(16384) * TEST_MIB);
    ASSERT_EQ(limits.max_staging_bytes, UINT64_C(20480) * TEST_MIB);
    ASSERT_EQ(limits.max_task_temp_bytes, UINT64_C(24576) * TEST_MIB);
    ASSERT_EQ(limits.cache_max_bytes, UINT64_C(32768) * TEST_MIB);
    ASSERT_EQ(limits.min_free_disk_bytes, UINT64_C(4096) * TEST_MIB);
    ASSERT_EQ(limits.max_duration_ms, UINT64_C(3600000));
    ASSERT_TRUE(limits.low_priority);
    PASS();
}

TEST(index_limits_config_set_converts_units_and_rejects_unsafe_values) {
    cbm_index_limits_t limits;
    cbm_index_limits_defaults(&limits);
    char error[128] = {0};

    ASSERT_TRUE(cbm_index_limits_set(&limits, "index_max_files", "75000", error, sizeof(error)));
    ASSERT_EQ(limits.max_files, 75000);
    ASSERT_TRUE(
        cbm_index_limits_set(&limits, "index_memory_limit_mb", "4096", error, sizeof(error)));
    ASSERT_EQ(limits.memory_limit_bytes, UINT64_C(4096) * TEST_MIB);
    ASSERT_TRUE(cbm_index_limits_set(&limits, "index_low_priority", "false", error, sizeof(error)));
    ASSERT_FALSE(limits.low_priority);

    cbm_index_limits_t unchanged = limits;
    ASSERT_FALSE(cbm_index_limits_set(&limits, "index_max_files", "0", error, sizeof(error)));
    ASSERT(strstr(error, "positive") != NULL);
    ASSERT_EQ(memcmp(&limits, &unchanged, sizeof(limits)), 0);

    ASSERT_FALSE(
        cbm_index_limits_set(&limits, "index_max_files", "not-a-number", error, sizeof(error)));
    ASSERT_EQ(memcmp(&limits, &unchanged, sizeof(limits)), 0);
    ASSERT_FALSE(cbm_index_limits_set(&limits, "index_memory_limit_mb", "999999999999999999999999",
                                      error, sizeof(error)));
    ASSERT_EQ(memcmp(&limits, &unchanged, sizeof(limits)), 0);
    ASSERT_FALSE(cbm_index_limits_set(&limits, "unknown_index_limit", "1", error, sizeof(error)));
    ASSERT(strstr(error, "unknown") != NULL);
    ASSERT_EQ(memcmp(&limits, &unchanged, sizeof(limits)), 0);
    PASS();
}

TEST(index_limits_config_registry_identifies_only_public_resource_keys) {
    ASSERT_TRUE(cbm_index_limits_is_config_key("index_max_files"));
    ASSERT_TRUE(cbm_index_limits_is_config_key("index_max_db_mb"));
    ASSERT_TRUE(cbm_index_limits_is_config_key("index_low_priority"));
    ASSERT_FALSE(cbm_index_limits_is_config_key("auto_index"));
    ASSERT_FALSE(cbm_index_limits_is_config_key("_cbm_index_max_files"));
    ASSERT_FALSE(cbm_index_limits_is_config_key(NULL));

    size_t count = cbm_index_limits_config_key_count();
    ASSERT_GT(count, 0);
    for (size_t i = 0; i < count; i++) {
        const char *key = cbm_index_limits_config_key_at(i);
        ASSERT_NOT_NULL(key);
        ASSERT_TRUE(cbm_index_limits_is_config_key(key));
    }
    ASSERT_NULL(cbm_index_limits_config_key_at(count));
    PASS();
}

TEST(index_limits_persisted_config_rejects_invalid_resource_values_atomically) {
    char cache[512];
    (void)snprintf(cache, sizeof(cache), "%s/cbm-index-config-validation-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(cache));
    cbm_config_t *config = cbm_config_open(cache);
    ASSERT_NOT_NULL(config);

    ASSERT_EQ(cbm_config_set(config, "index_max_files", "75000"), 0);
    ASSERT_STR_EQ(cbm_config_get(config, "index_max_files", ""), "75000");
    ASSERT_TRUE(cbm_config_set(config, "index_max_files", "0") != 0);
    ASSERT_STR_EQ(cbm_config_get(config, "index_max_files", ""), "75000");
    ASSERT_TRUE(cbm_config_set(config, "index_unknown_limit", "1") != 0);
    ASSERT_EQ(cbm_config_set(config, "extension_setting", "preserved"), 0);

    cbm_config_close(config);
    th_rmtree(cache);
    PASS();
}

TEST(index_limits_rejects_dangerous_builtin_roots) {
    char reason[64] = {0};

    ASSERT_FALSE(
        cbm_index_root_allowed("/", "/home/example", "/var/cache/cbm", "", reason, sizeof(reason)));
    ASSERT_STR_EQ(reason, "filesystem_root");

    ASSERT_FALSE(cbm_index_root_allowed("/home/example/", "/home/example", "/var/cache/cbm", "",
                                        reason, sizeof(reason)));
    ASSERT_STR_EQ(reason, "home_directory");

    ASSERT_FALSE(cbm_index_root_allowed("/var/cache/cbm", "/home/example", "/var/cache/cbm", "",
                                        reason, sizeof(reason)));
    ASSERT_STR_EQ(reason, "cache_directory");

    ASSERT_TRUE(cbm_index_root_allowed("/workspace/repository", "/home/example", "/var/cache/cbm",
                                       "", reason, sizeof(reason)));
    ASSERT_STR_EQ(reason, "");
    PASS();
}

TEST(index_limits_rejects_exact_configured_roots_only) {
    char reason[64] = {0};
    const char *denied = "/workspace;/srv/source";

    ASSERT_FALSE(cbm_index_root_allowed("/workspace/", "/home/example", "/var/cache/cbm", denied,
                                        reason, sizeof(reason)));
    ASSERT_STR_EQ(reason, "configured_root");

    ASSERT_TRUE(cbm_index_root_allowed("/workspace/repository", "/home/example", "/var/cache/cbm",
                                       denied, reason, sizeof(reason)));
    ASSERT_STR_EQ(reason, "");
    PASS();
}

TEST(index_memory_limit_is_clamped_to_detected_budget) {
    ASSERT_EQ(cbm_index_effective_memory_limit(8U * 1024U, 4U * 1024U), 4U * 1024U);
    ASSERT_EQ(cbm_index_effective_memory_limit(4U * 1024U, 8U * 1024U), 4U * 1024U);
    ASSERT_EQ(cbm_index_effective_memory_limit(4U * 1024U, 0), 4U * 1024U);
    PASS();
}

SUITE(index_limits) {
    RUN_TEST(index_limits_defaults_are_bounded);
    RUN_TEST(index_limits_config_set_converts_units_and_rejects_unsafe_values);
    RUN_TEST(index_limits_config_registry_identifies_only_public_resource_keys);
    RUN_TEST(index_limits_persisted_config_rejects_invalid_resource_values_atomically);
    RUN_TEST(index_limits_rejects_dangerous_builtin_roots);
    RUN_TEST(index_limits_rejects_exact_configured_roots_only);
    RUN_TEST(index_memory_limit_is_clamped_to_detected_budget);
}
