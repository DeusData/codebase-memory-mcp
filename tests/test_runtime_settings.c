#include "test_framework.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#include "foundation/limits.h"
#include "cli/cli.h"
#include "cli/runtime_settings.h"
#include "ui/config.h"
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    char path[256];
    char *cache;
    char *workers;
    char *cross;
} settings_fixture_t;
static bool fixture_open(settings_fixture_t *fixture) {
    memset(fixture, 0, sizeof(*fixture));
    snprintf(fixture->path, sizeof(fixture->path), "%s/cbm-settings-XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(fixture->path))
        return false;
    fixture->cache = getenv("CBM_CACHE_DIR") ? strdup(getenv("CBM_CACHE_DIR")) : NULL;
    fixture->workers = getenv("CBM_WORKERS") ? strdup(getenv("CBM_WORKERS")) : NULL;
    fixture->cross =
        getenv("CBM_DISABLE_LSP_CROSS") ? strdup(getenv("CBM_DISABLE_LSP_CROSS")) : NULL;
    return cbm_setenv("CBM_CACHE_DIR", fixture->path, 1) == 0;
}
static void restore_env(const char *key, char *value) {
    if (value)
        cbm_setenv(key, value, 1);
    else
        cbm_unsetenv(key);
    free(value);
}
static void fixture_close(settings_fixture_t *fixture) {
    cbm_runtime_settings_reset_for_tests();
    restore_env("CBM_CACHE_DIR", fixture->cache);
    restore_env("CBM_WORKERS", fixture->workers);
    restore_env("CBM_DISABLE_LSP_CROSS", fixture->cross);
    char path[512];
    snprintf(path, sizeof(path), "%s/_config.db", fixture->path);
    remove(path);
    snprintf(path, sizeof(path), "%s/config.json", fixture->path);
    remove(path);
    cbm_rmdir(fixture->path);
}
static yyjson_val *item(yyjson_doc *document, const char *key) {
    yyjson_val *items = yyjson_obj_get(yyjson_doc_get_root(document), "settings");
    size_t index, max;
    yyjson_val *value;
    yyjson_arr_foreach(items, index, max,
                       value) if (strcmp(yyjson_get_str(yyjson_obj_get(value, "key")), key) ==
                                  0) return value;
    return NULL;
}
static yyjson_doc *read_snapshot(const char *path) {
    char *json = cbm_runtime_settings_get_json(path);
    yyjson_doc *document = json ? yyjson_read(json, strlen(json), 0) : NULL;
    free(json);
    return document;
}
static char *apply(const char *path, const char *changes, int *status) {
    yyjson_doc *before = read_snapshot(path);
    if (!before)
        return NULL;
    const char *revision = yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(before), "revision"));
    char request[4096];
    snprintf(request, sizeof(request), "{\"revision\":\"%s\",\"changes\":%s}", revision, changes);
    yyjson_doc_free(before);
    return cbm_runtime_settings_apply_json(path, request, strlen(request), status);
}

TEST(runtime_settings_atomic_validation_and_conflict) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    yyjson_doc *before = read_snapshot(fixture.path);
    ASSERT_NOT_NULL(before);
    char revision[32];
    snprintf(revision, sizeof(revision), "%s",
             yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(before), "revision")));
    yyjson_doc_free(before);
    int status = 0;
    char *result =
        apply(fixture.path, "{\"auto_index\":\"true\",\"CBM_WORKERS\":\"999\"}", &status);
    ASSERT_EQ(status, 400);
    free(result);
    yyjson_doc *after = read_snapshot(fixture.path);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(after, "auto_index"), "value")), "false");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(after), "revision")), revision);
    yyjson_doc_free(after);
    result = apply(fixture.path,
                   "{\"auto_index\":\"true\",\"CBM_WORKERS\":\"4\",\"CBM_SQLITE_MMAP_SIZE\":\"0\"}",
                   &status);
    ASSERT_EQ(status, 200);
    free(result);
    char request[256];
    snprintf(request, sizeof(request),
             "{\"revision\":\"%s\",\"changes\":{\"auto_index\":\"false\"}}", revision);
    result = cbm_runtime_settings_apply_json(fixture.path, request, strlen(request), &status);
    ASSERT_EQ(status, 409);
    free(result);
    cbm_config_t *config = cbm_config_open(fixture.path);
    ASSERT_TRUE(cbm_config_get_bool(config, "auto_index", false));
    cbm_config_close(config);
    fixture_close(&fixture);
    PASS();
}

TEST(runtime_settings_strict_allowlist_and_types) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    const char *invalid[] = {"{\"CBM_TEST_CRASH_ON\":\"1\"}",
                             "{\"CBM_CACHE_DIR\":\"/tmp/no\"}",
                             "{\"CBM_WORKERS\":4}",
                             "{\"CBM_WORKERS\":\"4\",\"CBM_WORKERS\":\"8\"}",
                             "{\"CBM_SEMANTIC_THRESHOLD\":\"NaN\"}",
                             "{\"CBM_SEMANTIC_THRESHOLD\":\"0\"}",
                             "{\"CBM_LOG_LEVEL\":\"secret\"}",
                             "{\"auto_index\":\"1\"}",
                             "{\"ui_port\":\"65536\"}",
                             "{\"CBM_MEM_STATS_OUT\":\"/tmp/out\"}"};
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        int status = 0;
        char *result = apply(fixture.path, invalid[i], &status);
        ASSERT_EQ(status, 400);
        free(result);
    }
    yyjson_doc *snapshot = read_snapshot(fixture.path);
    ASSERT_NOT_NULL(snapshot);
    ASSERT_NULL(item(snapshot, "CBM_TEST_CRASH_ON"));
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(item(snapshot, "CBM_CACHE_DIR"), "editable")));
    yyjson_doc_free(snapshot);
    fixture_close(&fixture);
    PASS();
}

TEST(runtime_settings_snapshot_false_mask_reset_and_real_consumer) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    ASSERT_EQ(cbm_setenv("CBM_WORKERS", "2", 1), 0);
    ASSERT_EQ(cbm_setenv("CBM_DISABLE_LSP_CROSS", "0", 1), 0);
    int status = 0;
    char *result = apply(fixture.path,
                         "{\"CBM_WORKERS\":\"4\",\"CBM_DISABLE_LSP_CROSS\":\"false\",\"CBM_MAX_"
                         "FILE_BYTES\":\"4096\"}",
                         &status);
    ASSERT_EQ(status, 200);
    free(result);
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    ASSERT_STR_EQ(cbm_runtime_getenv("CBM_WORKERS"), "4");
    ASSERT_NULL(cbm_runtime_getenv("CBM_DISABLE_LSP_CROSS"));
    char safe[64];
    ASSERT_NULL(cbm_safe_getenv("CBM_DISABLE_LSP_CROSS", safe, sizeof(safe), NULL));
    ASSERT_EQ(cbm_max_file_bytes(), 4096);
    ASSERT_STR_EQ(getenv("CBM_WORKERS"), "2");
    ASSERT_STR_EQ(getenv("CBM_DISABLE_LSP_CROSS"), "0");
    result = apply(fixture.path, "{\"CBM_WORKERS\":\"8\",\"CBM_DISABLE_LSP_CROSS\":null}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    ASSERT_STR_EQ(cbm_runtime_getenv("CBM_WORKERS"), "4");
    yyjson_doc *snapshot = read_snapshot(fixture.path);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(snapshot, "CBM_WORKERS"), "effective")), "4");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(snapshot, "CBM_WORKERS"), "value")), "8");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(item(snapshot, "CBM_WORKERS"), "pendingRestart")));
    ASSERT_TRUE(
        yyjson_get_bool(yyjson_obj_get(item(snapshot, "CBM_DISABLE_LSP_CROSS"), "pendingRestart")));
    yyjson_doc_free(snapshot);
    result = apply(fixture.path, "{\"CBM_WORKERS\":null}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    cbm_runtime_settings_reset_for_tests();
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    ASSERT_STR_EQ(cbm_runtime_getenv("CBM_WORKERS"), "2");
    ASSERT_STR_EQ(cbm_runtime_getenv("CBM_DISABLE_LSP_CROSS"), "0");
    fixture_close(&fixture);
    PASS();
}

TEST(runtime_settings_cli_revision_and_ui_last_writer) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    cbm_ui_config_t legacy = {.ui_enabled = false, .ui_port = 8111};
    ASSERT_TRUE(cbm_ui_config_save(&legacy));
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    int status = 0;
    char *result =
        apply(fixture.path,
              "{\"ui_enabled\":\"true\",\"ui_port\":\"8222\",\"auto_index\":\"true\"}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    cbm_ui_config_t actual;
    cbm_ui_config_load(&actual);
    ASSERT_TRUE(actual.ui_enabled);
    ASSERT_EQ(actual.ui_port, 8222);
    yyjson_doc *before = read_snapshot(fixture.path);
    char old_revision[32];
    snprintf(old_revision, sizeof(old_revision), "%s",
             yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(before), "revision")));
    yyjson_doc_free(before);
    cbm_config_t *config = cbm_config_open(fixture.path);
    ASSERT_EQ(cbm_config_set(config, "auto_index", "false"), 0);
    cbm_config_close(config);
    yyjson_doc *after = read_snapshot(fixture.path);
    ASSERT_TRUE(strcmp(old_revision, yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(after),
                                                                   "revision"))) != 0);
    yyjson_doc_free(after);
    actual.ui_enabled = false;
    actual.ui_port = 8333;
    ASSERT_TRUE(cbm_ui_config_save(&actual));
    cbm_ui_config_load(&actual);
    ASSERT_FALSE(actual.ui_enabled);
    ASSERT_EQ(actual.ui_port, 8333);
    result = apply(fixture.path, "{\"ui_enabled\":null,\"ui_port\":null}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    cbm_ui_config_load(&actual);
    ASSERT_FALSE(actual.ui_enabled);
    ASSERT_EQ(actual.ui_port, 8111);
    fixture_close(&fixture);
    PASS();
}

TEST(runtime_settings_sql_failure_rolls_back_whole_batch) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    yyjson_doc *initial = read_snapshot(fixture.path);
    ASSERT_NOT_NULL(initial);
    yyjson_doc_free(initial);
    char path[512];
    snprintf(path, sizeof(path), "%s/_config.db", fixture.path);
    sqlite3 *db = NULL;
    ASSERT_EQ(sqlite3_open(path, &db), SQLITE_OK);
    ASSERT_EQ(
        sqlite3_exec(db,
                     "CREATE TRIGGER reject_workers BEFORE INSERT ON runtime_settings WHEN "
                     "NEW.key='CBM_WORKERS' BEGIN SELECT RAISE(ABORT,'test write failure'); END",
                     NULL, NULL, NULL),
        SQLITE_OK);
    sqlite3_close(db);
    int status = 0;
    char *result = apply(fixture.path, "{\"auto_index\":\"true\",\"CBM_WORKERS\":\"3\"}", &status);
    ASSERT_EQ(status, 500);
    free(result);
    yyjson_doc *snapshot = read_snapshot(fixture.path);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(snapshot, "auto_index"), "value")), "false");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(snapshot), "revision")), "0");
    yyjson_doc_free(snapshot);
    fixture_close(&fixture);
    PASS();
}

TEST(runtime_settings_canonical_alias_and_actual_listener) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    char alias[512];
    snprintf(alias, sizeof(alias), "%s/.", fixture.path);
    int status = 0;
    char *result = apply(alias, "{\"CBM_WORKERS\":\"4\"}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    ASSERT_TRUE(cbm_runtime_settings_init(alias));
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    result = apply(fixture.path, "{\"CBM_WORKERS\":\"8\",\"ui_port\":\"9768\"}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    cbm_runtime_settings_note_http_port(9768);
    yyjson_doc *snapshot = read_snapshot(fixture.path);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(snapshot, "CBM_WORKERS"), "effective")), "4");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(item(snapshot, "CBM_WORKERS"), "pendingRestart")));
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(snapshot, "ui_port"), "effective")), "9768");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(item(snapshot, "ui_port"), "pendingRestart")));
    yyjson_doc_free(snapshot);
    fixture_close(&fixture);
    PASS();
}

TEST(runtime_settings_inherited_clamps_and_unbuilt_features) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    const char *keys[] = {"CBM_STARTUP_TIMEOUT_MS", "CBM_SQLITE_MMAP_SIZE", "CBM_HOOK_DEADLINE_MS"};
    const char *values[] = {"2", "-1", "1"};
    const char *expected[] = {"1000", "0", "50"};
    char *previous[3];
    for (size_t i = 0; i < 3; i++) {
        previous[i] = getenv(keys[i]) ? strdup(getenv(keys[i])) : NULL;
        cbm_setenv(keys[i], values[i], 1);
    }
    yyjson_doc *snapshot = read_snapshot(fixture.path);
    ASSERT_NOT_NULL(snapshot);
    for (size_t i = 0; i < 3; i++)
        ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(snapshot, keys[i]), "value")),
                      expected[i]);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(item(snapshot, "CBM_MEM_PROFILE"), "editable")));
    ASSERT_FALSE(
        yyjson_get_bool(yyjson_obj_get(item(snapshot, "CBM_MEM_PROFILE_MIN"), "editable")));
    yyjson_doc_free(snapshot);
    for (size_t i = 0; i < 3; i++)
        restore_env(keys[i], previous[i]);
    fixture_close(&fixture);
    PASS();
}

TEST(runtime_settings_preserves_unknown_inherited_values_and_reset) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    char *previous_budget =
        getenv("CBM_TS_TYPE_BUDGET") ? strdup(getenv("CBM_TS_TYPE_BUDGET")) : NULL;
    ASSERT_EQ(cbm_setenv("CBM_WORKERS", "3junk", 1), 0);
    ASSERT_EQ(cbm_setenv("CBM_TS_TYPE_BUDGET", "-1", 1), 0);
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    yyjson_doc *snapshot = read_snapshot(fixture.path);
    ASSERT_NOT_NULL(snapshot);
    yyjson_val *workers = item(snapshot, "CBM_WORKERS");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "value")), "3junk");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "effective")), "3junk");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "source")), "environment");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "effectiveKnown")));
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "pendingRestart")));
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(snapshot, "CBM_TS_TYPE_BUDGET"), "value")),
                  "-1");
    ASSERT_FALSE(
        yyjson_get_bool(yyjson_obj_get(item(snapshot, "CBM_TS_TYPE_BUDGET"), "effectiveKnown")));
    yyjson_doc_free(snapshot);
    ASSERT_EQ(cbm_default_worker_count(true), 3);

    int status = 0;
    char *result = apply(fixture.path, "{\"CBM_WORKERS\":\"5\"}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    snapshot = read_snapshot(fixture.path);
    workers = item(snapshot, "CBM_WORKERS");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "value")), "5");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "effective")), "3junk");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "effectiveKnown")));
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(workers, "pendingRestart")));
    yyjson_doc_free(snapshot);
    result = apply(fixture.path, "{\"CBM_WORKERS\":null}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    snapshot = read_snapshot(fixture.path);
    workers = item(snapshot, "CBM_WORKERS");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "value")), "3junk");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "source")), "environment");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "effectiveKnown")));
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "pendingRestart")));
    yyjson_doc_free(snapshot);

    result = apply(fixture.path, "{\"CBM_WORKERS\":\"5\"}", &status);
    ASSERT_EQ(status, 200);
    free(result);
    cbm_runtime_settings_reset_for_tests();
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    snapshot = read_snapshot(fixture.path);
    workers = item(snapshot, "CBM_WORKERS");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "effective")), "5");
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(workers, "effectiveKnown")));
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "pendingRestart")));
    yyjson_doc_free(snapshot);
    ASSERT_EQ(cbm_default_worker_count(true), 5);
    restore_env("CBM_TS_TYPE_BUDGET", previous_budget);
    fixture_close(&fixture);
    PASS();
}

SUITE(runtime_settings) {
    RUN_TEST(runtime_settings_preserves_unknown_inherited_values_and_reset);
    RUN_TEST(runtime_settings_canonical_alias_and_actual_listener);
    RUN_TEST(runtime_settings_inherited_clamps_and_unbuilt_features);
    RUN_TEST(runtime_settings_atomic_validation_and_conflict);
    RUN_TEST(runtime_settings_strict_allowlist_and_types);
    RUN_TEST(runtime_settings_snapshot_false_mask_reset_and_real_consumer);
    RUN_TEST(runtime_settings_cli_revision_and_ui_last_writer);
    RUN_TEST(runtime_settings_sql_failure_rolls_back_whole_batch);
}
