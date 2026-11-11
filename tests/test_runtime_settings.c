#include "test_framework.h"
#include "foundation/mem_core.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"
#include "foundation/limits.h"
#include "foundation/log.h"
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
    cbm_free(CBM_MEM_CLASS_OTHER, json);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
    yyjson_doc *after = read_snapshot(fixture.path);
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(item(after, "auto_index"), "value")), "false");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(after), "revision")), revision);
    yyjson_doc_free(after);
    result = apply(fixture.path,
                   "{\"auto_index\":\"true\",\"CBM_WORKERS\":\"4\",\"CBM_SQLITE_MMAP_SIZE\":\"0\"}",
                   &status);
    ASSERT_EQ(status, 200);
    cbm_free(CBM_MEM_CLASS_OTHER, result);
    char request[256];
    snprintf(request, sizeof(request),
             "{\"revision\":\"%s\",\"changes\":{\"auto_index\":\"false\"}}", revision);
    result = cbm_runtime_settings_apply_json(fixture.path, request, strlen(request), &status);
    ASSERT_EQ(status, 409);
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
                             "{\"CBM_WALK_DEFS_MAX\":\"1024\"}",
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
        cbm_free(CBM_MEM_CLASS_OTHER, result);
    }
    yyjson_doc *snapshot = read_snapshot(fixture.path);
    ASSERT_NOT_NULL(snapshot);
    ASSERT_NULL(item(snapshot, "CBM_TEST_CRASH_ON"));
    ASSERT_NULL(item(snapshot, "CBM_WALK_DEFS_MAX"));
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
    ASSERT_TRUE(cbm_runtime_settings_init(alias));
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    result = apply(fixture.path, "{\"CBM_WORKERS\":\"8\",\"ui_port\":\"9768\"}", &status);
    ASSERT_EQ(status, 200);
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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
    cbm_free(CBM_MEM_CLASS_OTHER, result);
    snapshot = read_snapshot(fixture.path);
    workers = item(snapshot, "CBM_WORKERS");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "value")), "5");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "effective")), "3junk");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "effectiveKnown")));
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(workers, "pendingRestart")));
    yyjson_doc_free(snapshot);
    result = apply(fixture.path, "{\"CBM_WORKERS\":null}", &status);
    ASSERT_EQ(status, 200);
    cbm_free(CBM_MEM_CLASS_OTHER, result);
    snapshot = read_snapshot(fixture.path);
    workers = item(snapshot, "CBM_WORKERS");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "value")), "3junk");
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(workers, "source")), "environment");
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "effectiveKnown")));
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(workers, "pendingRestart")));
    yyjson_doc_free(snapshot);

    result = apply(fixture.path, "{\"CBM_WORKERS\":\"5\"}", &status);
    ASSERT_EQ(status, 200);
    cbm_free(CBM_MEM_CLASS_OTHER, result);
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

TEST(runtime_settings_response_memory_returns_to_caller_baseline) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fixture_open(&fixture));
    size_t blocks = cbm_mem_class_live_blocks(CBM_MEM_CLASS_OTHER);
    size_t bytes = cbm_mem_class_live_bytes(CBM_MEM_CLASS_OTHER);
    for (int request = 0; request < 3; request++) {
        int status = 0;
        char *json;
        if (request == 0)
            json = cbm_runtime_settings_get_json(fixture.path);
        else if (request == 1) {
            json = cbm_runtime_settings_apply_json(fixture.path, "{}", 2, &status);
            ASSERT_EQ(status, 400);
        } else {
            json = apply(fixture.path, "{\"CBM_WORKERS\":\"4\"}", &status);
            ASSERT_EQ(status, 200);
        }
        ASSERT_NOT_NULL(json);
        ASSERT_EQ(cbm_mem_class_live_blocks(CBM_MEM_CLASS_OTHER), blocks + 1);
        ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_OTHER), bytes + cbm_mem_usable_size(json));
        cbm_free(CBM_MEM_CLASS_OTHER, json);
        ASSERT_EQ(cbm_mem_class_live_blocks(CBM_MEM_CLASS_OTHER), blocks);
        ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_OTHER), bytes);
    }
    fixture_close(&fixture);
    PASS();
}

/* A second connection holds _config.db's lock, as another process mid-commit
 * would. The sink commits the holder the moment the waiter logs
 * config_db.busy_wait, so the wait is observed as an event, single-threaded and
 * without a duration. A call that fails on the lock instead never logs, so the
 * holder stays held through it and the call reports the failure. Results are
 * captured before any ASSERT so a failure never leaks the lock or the sink. */
static sqlite3 *g_lock_holder;
static int g_lock_waits;
static CBMLogLevel g_lock_saved_level;

static void release_holder_on_wait(const char *line) {
    if (!g_lock_holder || !strstr(line, "config_db.busy_wait"))
        return;
    g_lock_waits++;
    (void)sqlite3_exec(g_lock_holder, "COMMIT", NULL, NULL, NULL);
}

static bool hold_config_lock(const char *cache, const char *lock_sql) {
    char path[512];
    snprintf(path, sizeof(path), "%s/_config.db", cache);
    g_lock_waits = 0;
    if (sqlite3_open(path, &g_lock_holder) != SQLITE_OK ||
        sqlite3_exec(g_lock_holder, lock_sql, NULL, NULL, NULL) != SQLITE_OK) {
        sqlite3_close(g_lock_holder);
        g_lock_holder = NULL;
        return false;
    }
    g_lock_saved_level = cbm_log_get_level();
    if (g_lock_saved_level > CBM_LOG_WARN)
        cbm_log_set_level(CBM_LOG_WARN);
    cbm_log_set_sink(release_holder_on_wait);
    return true;
}

static void release_config_lock(void) {
    cbm_log_set_sink(NULL);
    cbm_log_set_level(g_lock_saved_level);
    if (g_lock_holder) {
        if (!sqlite3_get_autocommit(g_lock_holder))
            (void)sqlite3_exec(g_lock_holder, "COMMIT", NULL, NULL, NULL);
        sqlite3_close(g_lock_holder);
        g_lock_holder = NULL;
    }
}

static long long settings_revision(const char *cache) {
    yyjson_doc *snapshot = read_snapshot(cache);
    const char *text =
        snapshot ? yyjson_get_str(yyjson_obj_get(yyjson_doc_get_root(snapshot), "revision")) : NULL;
    long long revision = text ? strtoll(text, NULL, 10) : -1;
    yyjson_doc_free(snapshot);
    return revision;
}

/* A test that fails between init and fixture_close leaves the settings bound
 * to its own cache; the lock tests reset first so each one fails on its own
 * defect, never on a predecessor's leftover state. */
static bool fresh_fixture_open(settings_fixture_t *fixture) {
    cbm_runtime_settings_reset_for_tests();
    return fixture_open(fixture);
}

/* CI churn: a daemon committing _config.db made an unrelated CLI start exit
 * with "could not load saved runtime settings" (50 ms window). */
TEST(runtime_settings_startup_waits_out_a_committing_writer) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fresh_fixture_open(&fixture));
    int status = 0;
    char *result = apply(fixture.path, "{\"CBM_WORKERS\":\"4\"}", &status);
    ASSERT_EQ(status, 200);
    cbm_free(CBM_MEM_CLASS_OTHER, result);
    ASSERT_TRUE(hold_config_lock(fixture.path, "BEGIN EXCLUSIVE"));
    bool initialized = cbm_runtime_settings_init(fixture.path);
    int waits = g_lock_waits;
    release_config_lock();
    ASSERT_TRUE(initialized);
    ASSERT_EQ(waits, 1);
    ASSERT_STR_EQ(cbm_runtime_getenv("CBM_WORKERS"), "4");
    fixture_close(&fixture);
    PASS();
}

/* The daemon's set_ui_config handler: a concurrent commit failed the save at
 * once (cbm_config_open had no wait) and made the load silently return the
 * legacy defaults instead of the saved port. */
TEST(runtime_settings_ui_save_and_load_wait_out_a_committing_writer) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fresh_fixture_open(&fixture));
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    cbm_ui_config_t config = {.ui_enabled = true, .ui_port = 8222};
    ASSERT_TRUE(cbm_ui_config_save(&config));

    ASSERT_TRUE(hold_config_lock(fixture.path, "BEGIN EXCLUSIVE"));
    config.ui_port = 8333;
    bool saved = cbm_ui_config_save(&config);
    int save_waits = g_lock_waits;
    release_config_lock();
    ASSERT_TRUE(saved);
    ASSERT_TRUE(save_waits >= 1);

    ASSERT_TRUE(hold_config_lock(fixture.path, "BEGIN EXCLUSIVE"));
    cbm_ui_config_t loaded = {0};
    cbm_ui_config_load(&loaded);
    int load_waits = g_lock_waits;
    release_config_lock();
    ASSERT_EQ(load_waits, 1);
    ASSERT_TRUE(loaded.ui_enabled);
    ASSERT_EQ(loaded.ui_port, 8333);
    fixture_close(&fixture);
    PASS();
}

/* Every `daemon start` on a UI build saved the same two values; the upsert
 * still updated the rows, fired the revision trigger and committed. */
TEST(runtime_settings_unchanged_ui_save_does_not_rewrite) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fresh_fixture_open(&fixture));
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    cbm_ui_config_t config = {.ui_enabled = true, .ui_port = 8222};
    ASSERT_TRUE(cbm_ui_config_save(&config));
    long long first = settings_revision(fixture.path);
    ASSERT_TRUE(first > 0);
    ASSERT_TRUE(cbm_ui_config_save(&config));
    ASSERT_TRUE(cbm_ui_config_save(&config));
    ASSERT_EQ(settings_revision(fixture.path), first);
    config.ui_port = 8223;
    ASSERT_TRUE(cbm_ui_config_save(&config));
    ASSERT_EQ(settings_revision(fixture.path), first + 1);
    fixture_close(&fixture);
    PASS();
}

/* A process killed mid-write (crash, taskkill /F, power loss) leaves _config.db
 * with a hot rollback journal. Made portably and deterministically: snapshot the
 * database and its journal while a spilled write transaction is open, roll the
 * transaction back, then put the snapshot back. The journal is checked to be
 * hot (SQLite's magic and a nonzero record count) before the code under test
 * runs, so these tests cannot pass on a journal that was never hot. */
static bool copy_file(const char *from, const char *to) {
    FILE *in = cbm_fopen(from, "rb");
    FILE *out = in ? cbm_fopen(to, "wb") : NULL;
    bool ok = in && out;
    char buffer[8192];
    size_t got = 0;
    while (ok && (got = fread(buffer, 1, sizeof(buffer), in)) > 0)
        ok = fwrite(buffer, 1, got, out) == got;
    if (in)
        fclose(in);
    if (out && fclose(out) != 0)
        ok = false;
    return ok;
}

static bool journal_is_hot(const char *path) {
    static const unsigned char magic[8] = {0xd9, 0xd5, 0x05, 0xf9, 0x20, 0xa1, 0x63, 0xd7};
    unsigned char header[12] = {0};
    FILE *file = cbm_fopen(path, "rb");
    bool read = file && fread(header, 1, sizeof(header), file) == sizeof(header);
    if (file)
        fclose(file);
    unsigned records = ((unsigned)header[8] << 24) | ((unsigned)header[9] << 16) |
                       ((unsigned)header[10] << 8) | (unsigned)header[11];
    return read && memcmp(header, magic, sizeof(magic)) == 0 && records > 0;
}

static bool leave_hot_journal(const char *cache, const char *update_sql) {
    char db_path[512], journal_path[512], db_copy[512], journal_copy[512];
    snprintf(db_path, sizeof(db_path), "%s/_config.db", cache);
    snprintf(journal_path, sizeof(journal_path), "%s/_config.db-journal", cache);
    snprintf(db_copy, sizeof(db_copy), "%s/snapshot.db", cache);
    snprintf(journal_copy, sizeof(journal_copy), "%s/snapshot.journal", cache);
    sqlite3 *writer = NULL;
    bool ok = sqlite3_open(db_path, &writer) == SQLITE_OK &&
              sqlite3_exec(writer, "PRAGMA cache_size=1; BEGIN IMMEDIATE; CREATE TABLE spill(x BLOB)",
                           NULL, NULL, NULL) == SQLITE_OK &&
              sqlite3_exec(writer, update_sql, NULL, NULL, NULL) == SQLITE_OK;
    for (int i = 0; ok && i < 64; i++)
        ok = sqlite3_exec(writer, "INSERT INTO spill VALUES(randomblob(8192))", NULL, NULL, NULL) ==
             SQLITE_OK;
    ok = ok && copy_file(db_path, db_copy) && copy_file(journal_path, journal_copy);
    if (writer) {
        (void)sqlite3_exec(writer, "ROLLBACK", NULL, NULL, NULL);
        sqlite3_close(writer);
    }
    ok = ok && journal_is_hot(journal_copy) && copy_file(db_copy, db_path) &&
         copy_file(journal_copy, journal_path);
    remove(db_copy);
    remove(journal_copy);
    return ok;
}

/* The hot journal made every later start fail with "could not load saved
 * runtime settings", permanently: a read-only connection cannot roll it back. */
TEST(runtime_settings_startup_recovers_a_hot_journal) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fresh_fixture_open(&fixture));
    int status = 0;
    char *result = apply(fixture.path, "{\"CBM_WORKERS\":\"4\"}", &status);
    ASSERT_EQ(status, 200);
    cbm_free(CBM_MEM_CLASS_OTHER, result);
    ASSERT_TRUE(leave_hot_journal(fixture.path,
                                  "UPDATE runtime_settings SET value='9' WHERE key='CBM_WORKERS'"));
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    ASSERT_STR_EQ(cbm_runtime_getenv("CBM_WORKERS"), "4");
    char journal[512];
    snprintf(journal, sizeof(journal), "%s/_config.db-journal", fixture.path);
    ASSERT_FALSE(cbm_file_exists(journal));
    fixture_close(&fixture);
    PASS();
}

/* The same journal made the UI load silently fall back to the legacy defaults. */
TEST(runtime_settings_ui_load_recovers_a_hot_journal) {
    settings_fixture_t fixture;
    ASSERT_TRUE(fresh_fixture_open(&fixture));
    ASSERT_TRUE(cbm_runtime_settings_init(fixture.path));
    cbm_ui_config_t config = {.ui_enabled = true, .ui_port = 8222};
    ASSERT_TRUE(cbm_ui_config_save(&config));
    ASSERT_TRUE(leave_hot_journal(fixture.path,
                                  "UPDATE runtime_settings SET value='9999' WHERE key='ui_port'"));
    cbm_ui_config_t loaded = {0};
    cbm_ui_config_load(&loaded);
    ASSERT_TRUE(loaded.ui_enabled);
    ASSERT_EQ(loaded.ui_port, 8222);
    fixture_close(&fixture);
    PASS();
}

SUITE(runtime_settings) {
    RUN_TEST(runtime_settings_startup_recovers_a_hot_journal);
    RUN_TEST(runtime_settings_ui_load_recovers_a_hot_journal);
    RUN_TEST(runtime_settings_startup_waits_out_a_committing_writer);
    RUN_TEST(runtime_settings_ui_save_and_load_wait_out_a_committing_writer);
    RUN_TEST(runtime_settings_unchanged_ui_save_does_not_rewrite);
    RUN_TEST(runtime_settings_response_memory_returns_to_caller_baseline);
    RUN_TEST(runtime_settings_preserves_unknown_inherited_values_and_reset);
    RUN_TEST(runtime_settings_canonical_alias_and_actual_listener);
    RUN_TEST(runtime_settings_inherited_clamps_and_unbuilt_features);
    RUN_TEST(runtime_settings_atomic_validation_and_conflict);
    RUN_TEST(runtime_settings_strict_allowlist_and_types);
    RUN_TEST(runtime_settings_snapshot_false_mask_reset_and_real_consumer);
    RUN_TEST(runtime_settings_cli_revision_and_ui_last_writer);
    RUN_TEST(runtime_settings_sql_failure_rolls_back_whole_batch);
}
