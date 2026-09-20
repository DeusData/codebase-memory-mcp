#include "test_framework.h"
#include "test_helpers.h"
#include "mcp/cache.h"
#include "cli/cli.h"
#include "daemon/bootstrap.h"
#include <sqlite3.h>
#include <yyjson/yyjson.h>
#include <errno.h>

static bool cache_fixture(const char *directory, const char *name, const char *root,
                          const char *timestamp) {
    char file[1024];
    snprintf(file, sizeof(file), "%s/%s.db", directory, name);
    sqlite3 *db = NULL;
    if (sqlite3_open(file, &db) != SQLITE_OK) {
        sqlite3_close(db);
        return false;
    }
    char *sql = sqlite3_mprintf("CREATE TABLE projects(name TEXT, root_path TEXT, indexed_at TEXT);"
                                "INSERT INTO projects VALUES(%Q,%Q,%Q);"
                                "INSERT INTO projects VALUES(%Q || '::missed','','');",
                                name, root, timestamp, name);
    bool ok = sql && sqlite3_exec(db, sql, NULL, NULL, NULL) == SQLITE_OK;
    sqlite3_free(sql);
    sqlite3_close(db);
    return ok;
}

static yyjson_doc *cache_call(const char *directory, const char *args, bool prune,
                              const cbm_cache_ops_t *ops, bool *error) {
    char *json = cbm_cache_run(directory, args, prune, ops, error);
    yyjson_doc *doc = json ? yyjson_read(json, strlen(json), 0) : NULL;
    free(json);
    return doc;
}

static int64_t cache_number(yyjson_doc *doc, const char *key) {
    return yyjson_get_sint(yyjson_obj_get(yyjson_doc_get_root(doc), key));
}

typedef struct {
    int began;
    int ended;
    bool busy;
    const char *restore_root;
    const char *refresh_db;
    const char *replace_db;
    const char *publish_db;
} cache_guard_t;

static bool cache_guard_begin(void *context, const char *project) {
    (void)project;
    cache_guard_t *guard = context;
    guard->began++;
    if (guard->publish_db) {
        (void)th_write_file(guard->publish_db, "new database");
    }
    if (guard->restore_root) {
        cbm_mkdir_p(guard->restore_root, 0700);
    }
    if (guard->refresh_db) {
        sqlite3 *db = NULL;
        sqlite3_open(guard->refresh_db, &db);
        sqlite3_exec(db, "UPDATE projects SET indexed_at = '2999-01-01T00:00:00Z'", NULL, NULL,
                     NULL);
        sqlite3_close(db);
    }
    return !guard->busy;
}

static void cache_guard_end(void *context, const char *project) {
    (void)project;
    ((cache_guard_t *)context)->ended++;
}

static void cache_guard_before(void *context, const char *project) {
    (void)project;
    cache_guard_t *guard = context;
    if (guard->replace_db) {
        cbm_unlink(guard->replace_db);
        cbm_mkdir_p(guard->replace_db, 0700);
    }
}

TEST(cache_stats_counts_primary_projects_and_sidecars) {
    char *directory = th_mktempdir("cbm-cache-stats");
    ASSERT_NOT_NULL(directory);
    ASSERT(cache_fixture(directory, "live", directory, "2020-01-01T00:00:00Z"));
    ASSERT(cache_fixture(directory, "gone", TH_PATH(directory, "missing"), "2020-01-01T00:00:00Z"));
    ASSERT_EQ(th_write_file(TH_PATH(directory, "_config.db"), "config"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(directory, "gone.db.tmp"), "temporary"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(directory, "logs/old.log"), "log"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(directory, "orphan.db-wal"), "orphan"), 0);
    ASSERT_EQ(th_write_file(TH_PATH(directory, ".hidden"), "hidden"), 0);
    bool error;
    yyjson_doc *doc = cache_call(directory, "{}", false, NULL, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "project_count"), 2);
    ASSERT_EQ(cache_number(doc, "database_count"), 2);
    ASSERT_EQ(cache_number(doc, "missing_root_count"), 1);
    yyjson_val *report = yyjson_doc_get_root(doc);
    ASSERT_EQ(yyjson_arr_size(yyjson_obj_get(report, "projects")), 2);
    ASSERT_FALSE(yyjson_get_bool(yyjson_obj_get(report, "has_more")));
    yyjson_val *inventory = yyjson_obj_get(report, "directory_inventory");
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(inventory, "entry_count")), 7);
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(inventory, "orphan_sidecar_count")), 1);
    ASSERT_EQ(yyjson_get_sint(yyjson_obj_get(inventory, "orphan_sidecar_bytes")), 6);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(inventory, "complete")));
    int64_t bytes = cache_number(doc, "size_bytes");
    ASSERT_GT(bytes, 0);
    yyjson_doc_free(doc);
    ASSERT_EQ(th_write_file(TH_PATH(directory, "gone.db-shm"), "sidecar"), 0);
    doc = cache_call(directory, "{}", false, NULL, &error);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "size_bytes"), bytes + 7);
    yyjson_doc_free(doc);
    th_rmtree(directory);
    PASS();
}

TEST(cache_prune_dry_run_and_conditions_are_conjunctive) {
    char *directory = th_mktempdir("cbm-cache-prune");
    ASSERT_NOT_NULL(directory);
    ASSERT(cache_fixture(directory, "old-live", directory, "2020-01-01T00:00:00Z"));
    ASSERT(
        cache_fixture(directory, "old-gone", TH_PATH(directory, "gone"), "2020-01-01T00:00:00Z"));
    ASSERT(
        cache_fixture(directory, "new-gone", TH_PATH(directory, "gone"), "2999-01-01T00:00:00Z"));
    ASSERT_EQ(th_write_file(TH_PATH(directory, "_config.db"), "config"), 0);
    cache_guard_t guard = {0};
    cbm_cache_ops_t ops = {
        .context = &guard, .try_begin = cache_guard_begin, .end = cache_guard_end};
    bool error;
    yyjson_doc *doc =
        cache_call(directory, "{\"missing_root\":true,\"older_than\":\"30d\",\"dry_run\":true}",
                   true, &ops, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "candidate_count"), 1);
    ASSERT_EQ(cache_number(doc, "deleted_count"), 0);
    ASSERT_EQ(guard.began, 0);
    ASSERT(cbm_file_exists(TH_PATH(directory, "old-gone.db")));
    yyjson_doc_free(doc);
    doc =
        cache_call(directory, "{\"missing_root\":true,\"older_than\":\"30d\"}", true, &ops, &error);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "deleted_count"), 1);
    ASSERT_GT(cache_number(doc, "removed_bytes"), 0);
    ASSERT_EQ(guard.began, 1);
    ASSERT_EQ(guard.ended, 1);
    ASSERT(!cbm_file_exists(TH_PATH(directory, "old-gone.db")));
    ASSERT(cbm_file_exists(TH_PATH(directory, "new-gone.db")));
    ASSERT(cbm_file_exists(TH_PATH(directory, "old-live.db")));
    ASSERT(cbm_file_exists(TH_PATH(directory, "_config.db")));
    yyjson_doc_free(doc);
    th_rmtree(directory);
    PASS();
}

TEST(cache_prune_skips_busy_and_rechecks_after_lock) {
    char *directory = th_mktempdir("cbm-cache-race");
    ASSERT_NOT_NULL(directory);
    char root[1024], path[1024];
    snprintf(root, sizeof(root), "%s/missing", directory);
    snprintf(path, sizeof(path), "%s/gone.db", directory);
    ASSERT(cache_fixture(directory, "gone", root, "2020-01-01T00:00:00Z"));
    cache_guard_t guard = {.busy = true};
    cbm_cache_ops_t ops = {
        .context = &guard, .try_begin = cache_guard_begin, .end = cache_guard_end};
    bool error;
    yyjson_doc *doc = cache_call(directory, "{\"missing_root\":true}", true, &ops, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(error);
    ASSERT_EQ(cache_number(doc, "busy_count"), 1);
    ASSERT_EQ(guard.ended, 0);
    ASSERT(cbm_file_exists(path));
    yyjson_doc_free(doc);
    guard.busy = false;
    guard.restore_root = root;
    doc = cache_call(directory, "{\"missing_root\":true}", true, &ops, &error);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "deleted_count"), 0);
    ASSERT_EQ(guard.ended, 1);
    ASSERT(cbm_file_exists(path));
    yyjson_doc_free(doc);
    guard.restore_root = NULL;
    guard.refresh_db = path;
    doc = cache_call(directory, "{\"older_than\":\"30d\"}", true, &ops, &error);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "deleted_count"), 0);
    ASSERT_EQ(guard.ended, 2);
    ASSERT(cbm_file_exists(path));
    yyjson_doc_free(doc);
    th_rmtree(directory);
    PASS();
}

TEST(cache_prune_rejects_invalid_or_unfiltered_requests) {
    const char *invalid[] = {"{}",
                             "{\"orphan_sidecars\":true,\"missing_root\":true,\"dry_run\":true}",
                             "{\"orphan_sidecars\":true,\"older_than\":\"1d\",\"dry_run\":true}",
                             "{\"orphan_sidecars\":\"true\"}",
                             "{\"missing_root\":true,\"missing_root\":false,\"dry_run\":true}",
                             "{\"dry_run\":true}",
                             "{\"missing_root\":false}",
                             "{\"missing_root\":\"true\"}",
                             "{\"older_than\":\"-1d\"}",
                             "{\"older_than\":\"0d\"}",
                             "{\"older_than\":\"1\"}",
                             "{\"older_than\":\"1.5d\"}",
                             "{\"older_than\":\"9223372036854775807w\"}",
                             "{\"missing_root\":true,\"typo\":true}",
                             "[]"};
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        bool error;
        yyjson_doc *doc = cache_call("unused", invalid[i], true, NULL, &error);
        ASSERT_NOT_NULL(doc);
        ASSERT(error);
        yyjson_doc_free(doc);
    }
    bool error;
    yyjson_doc *doc = cache_call("unused", "{\"missing_root\":true}", true, NULL, &error);
    ASSERT(error); /* actual pruning cannot bypass the lease */
    yyjson_doc_free(doc);
    PASS();
}

TEST(cache_prune_keeps_unreadable_ambiguous_and_unknown_roots) {
    char *directory = th_mktempdir("cbm-cache-unknown");
    ASSERT_NOT_NULL(directory);
    ASSERT(cache_fixture(directory, "unknown", "", "invalid"));
    ASSERT(cache_fixture(directory, "alias", TH_PATH(directory, "gone"), "2020-01-01T00:00:00Z"));
    ASSERT_EQ(rename(TH_PATH(directory, "alias.db"), TH_PATH(directory, "renamed.db")), 0);
    ASSERT_EQ(th_write_file(TH_PATH(directory, "broken.db"), "not a database"), 0);
#ifndef _WIN32
    ASSERT_EQ(symlink(TH_PATH(directory, "renamed.db"), TH_PATH(directory, "link.db")), 0);
#endif
    cache_guard_t guard = {0};
    cbm_cache_ops_t ops = {
        .context = &guard, .try_begin = cache_guard_begin, .end = cache_guard_end};
    bool error;
    yyjson_doc *doc = cache_call(directory, "{\"missing_root\":true}", true, &ops, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "candidate_count"), 0);
    ASSERT_EQ(guard.began, 0);
    ASSERT(cbm_file_exists(TH_PATH(directory, "renamed.db")));
    ASSERT(cbm_file_exists(TH_PATH(directory, "broken.db")));
    yyjson_doc_free(doc);
    th_rmtree(directory);
    PASS();
}

TEST(cache_prune_failed_db_delete_preserves_sidecars) {
    char *directory = th_mktempdir("cbm-cache-failure");
    ASSERT_NOT_NULL(directory);
    ASSERT(cache_fixture(directory, "gone", TH_PATH(directory, "missing"), "2020-01-01T00:00:00Z"));
    char path[1024];
    snprintf(path, sizeof(path), "%s/gone.db", directory);
    ASSERT_EQ(th_write_file(TH_PATH(directory, "gone.db-shm"), "preserve"), 0);
    cache_guard_t guard = {.replace_db = path};
    cbm_cache_ops_t ops = {.context = &guard,
                           .try_begin = cache_guard_begin,
                           .end = cache_guard_end,
                           .before_delete = cache_guard_before};
    bool error;
    yyjson_doc *doc = cache_call(directory, "{\"missing_root\":true}", true, &ops, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(error);
    ASSERT_EQ(cache_number(doc, "failed_count"), 1);
    ASSERT_EQ(cache_number(doc, "removed_bytes"), 0);
    ASSERT_EQ(guard.ended, 1);
    ASSERT(cbm_file_exists(TH_PATH(directory, "gone.db-shm")));
    yyjson_doc_free(doc);
    th_rmtree(directory);
    PASS();
}

TEST(cache_empty_directory_and_duration_units) {
    char *directory = th_mktempdir("cbm-cache-empty");
    ASSERT_NOT_NULL(directory);
    bool error;
    errno = EACCES; /* Directory wrappers need not set POSIX errno on Windows. */
    yyjson_doc *doc = cache_call(TH_PATH(directory, "absent"), "{}", false, NULL, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "project_count"), 0);
    yyjson_doc_free(doc);
    ASSERT_EQ(th_write_file(TH_PATH(directory, "not-a-directory"), "keep"), 0);
    errno = ENOENT; /* A stale errno must not hide an existing invalid path. */
    doc = cache_call(TH_PATH(directory, "not-a-directory"), "{}", false, NULL, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(error);
    yyjson_doc_free(doc);
    const char *durations[] = {"1s", "2m", "3h", "4d", "5w"};
    const int64_t seconds[] = {1, 120, 10800, 345600, 3024000};
    for (size_t i = 0; i < sizeof(seconds) / sizeof(seconds[0]); i++) {
        char args[128];
        snprintf(args, sizeof(args), "{\"older_than\":\"%s\",\"dry_run\":true}", durations[i]);
        doc = cache_call(directory, args, true, NULL, &error);
        ASSERT_NOT_NULL(doc);
        ASSERT(!error);
        ASSERT_EQ(cache_number(doc, "older_than_seconds"), seconds[i]);
        yyjson_doc_free(doc);
    }
    th_rmtree(directory);
    PASS();
}

#ifndef _WIN32
TEST(cache_reads_and_prunes_through_directory_alias) {
    char *directory = th_mktempdir("cbm-cache-alias");
    ASSERT_NOT_NULL(directory);
    char alias[1024];
    snprintf(alias, sizeof(alias), "%s/alias", directory);
    ASSERT_EQ(symlink(directory, alias), 0);
    ASSERT(cache_fixture(directory, "gone", TH_PATH(directory, "missing"),
                         "2020-01-01T00:00:00Z"));
    ASSERT_EQ(symlink(TH_PATH(directory, "gone.db"), TH_PATH(directory, "link.db")), 0);
    bool error;
    yyjson_doc *doc = cache_call(alias, "{}", false, NULL, &error);
    ASSERT_NOT_NULL(doc);
    bool inspected = !error && cache_number(doc, "project_count") == 1 &&
                     cache_number(doc, "uninspectable_count") == 1;
    yyjson_doc_free(doc);
    cache_guard_t guard = {0};
    cbm_cache_ops_t ops = {
        .context = &guard, .try_begin = cache_guard_begin, .end = cache_guard_end};
    doc = cache_call(alias, "{\"missing_root\":true}", true, &ops, &error);
    ASSERT_NOT_NULL(doc);
    bool deleted = !error && cache_number(doc, "deleted_count") == 1 &&
                   guard.began == 1 && guard.ended == 1 &&
                   !cbm_file_exists(TH_PATH(directory, "gone.db"));
    yyjson_doc_free(doc);
    cbm_unlink(alias);
    cbm_unlink(TH_PATH(directory, "link.db"));
    th_rmtree(directory);
    ASSERT_TRUE(inspected);
    ASSERT_TRUE(deleted);
    PASS();
}
#endif

TEST(cache_cli_flags_map_to_tool_conditions) {
    char *argv[] = {"--missing-root", "--older-than", "30d", "--dry-run"};
    char *error = NULL;
    char *args = cbm_cli_build_args_json("cache_prune", 4, argv, &error);
    ASSERT_NULL(error);
    ASSERT_NOT_NULL(args);
    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    ASSERT_NOT_NULL(doc);
    yyjson_val *root = yyjson_doc_get_root(doc);
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "missing_root")));
    ASSERT_TRUE(yyjson_get_bool(yyjson_obj_get(root, "dry_run")));
    ASSERT_STR_EQ(yyjson_get_str(yyjson_obj_get(root, "older_than")), "30d");
    yyjson_doc_free(doc);
    free(args);
    PASS();
}

TEST(cache_orphan_sidecars_preview_delete_and_recheck) {
    char *directory = th_mktempdir("cbm-cache-orphans");
    ASSERT_NOT_NULL(directory);
    const char *files[] = {"gone.db-wal", "gone.db-shm", "single.db-shm",  "_config.db-wal",
                           "live.db-wal", "live.db",     "gone.db.corrupt"};
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_EQ(th_write_file(TH_PATH(directory, files[i]), "keep"), 0);
    }
#ifndef _WIN32
    ASSERT_EQ(symlink(TH_PATH(directory, "gone.db.corrupt"), TH_PATH(directory, "link.db-wal")), 0);
#endif
    cache_guard_t guard = {.busy = true};
    cbm_cache_ops_t ops = {
        .context = &guard, .try_begin = cache_guard_begin, .end = cache_guard_end};
    bool error;
    yyjson_doc *doc =
        cache_call(directory, "{\"orphan_sidecars\":true,\"dry_run\":true}", true, &ops, &error);
    ASSERT_NOT_NULL(doc);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "candidate_count"), 3);
    ASSERT_EQ(cache_number(doc, "candidate_bytes"), 12);
    ASSERT_EQ(guard.began, 0);
    yyjson_doc_free(doc);
    doc = cache_call(directory, "{\"orphan_sidecars\":true}", true, &ops, &error);
    ASSERT(error);
    ASSERT_EQ(cache_number(doc, "busy_count"), 3);
    ASSERT_EQ(guard.ended, 0);
    yyjson_doc_free(doc);
    guard.busy = false;
    char path[1024];
    snprintf(path, sizeof(path), "%s/gone.db", directory);
    guard.publish_db = path; /* DB appears after discovery, before the lease is granted. */
    doc = cache_call(directory, "{\"orphan_sidecars\":true}", true, &ops, &error);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "deleted_count"), 1);
    ASSERT(cbm_file_exists(TH_PATH(directory, "gone.db-wal")));
    ASSERT(cbm_file_exists(TH_PATH(directory, "gone.db-shm")));
    yyjson_doc_free(doc);
    guard.publish_db = NULL;
    ASSERT_EQ(cbm_unlink(path), 0);
    doc = cache_call(directory, "{\"orphan_sidecars\":true}", true, &ops, &error);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "deleted_count"), 2);
    ASSERT_EQ(cache_number(doc, "removed_bytes"), 8);
    ASSERT(cbm_file_exists(TH_PATH(directory, "live.db-wal")));
    ASSERT(cbm_file_exists(TH_PATH(directory, "_config.db-wal")));
    ASSERT(cbm_file_exists(TH_PATH(directory, "gone.db.corrupt")));
    yyjson_doc_free(doc);
    doc = cache_call(directory, "{\"orphan_sidecars\":true}", true, &ops, &error);
    ASSERT(!error);
    ASSERT_EQ(cache_number(doc, "candidate_count"), 0);
    yyjson_doc_free(doc);
    th_rmtree(directory);
    PASS();
}

SUITE(cache) {
#ifndef _WIN32
    RUN_TEST(cache_reads_and_prunes_through_directory_alias);
#endif
    RUN_TEST(cache_orphan_sidecars_preview_delete_and_recheck);
    RUN_TEST(cache_empty_directory_and_duration_units);
    RUN_TEST(cache_cli_flags_map_to_tool_conditions);
    RUN_TEST(cache_stats_counts_primary_projects_and_sidecars);
    RUN_TEST(cache_prune_dry_run_and_conditions_are_conjunctive);
    RUN_TEST(cache_prune_skips_busy_and_rechecks_after_lock);
    RUN_TEST(cache_prune_rejects_invalid_or_unfiltered_requests);
    RUN_TEST(cache_prune_keeps_unreadable_ambiguous_and_unknown_roots);
    RUN_TEST(cache_prune_failed_db_delete_preserves_sidecars);
}
