/*
 * test_watcher.c — Tests for the file change watcher module.
 *
 * Covers: adaptive interval, watch/unwatch lifecycle, git change detection,
 * poll_once behavior.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/compat_thread.h"
#include "../src/foundation/constants.h"
#include "../src/foundation/platform.h"
#include "test_framework.h"
#include "test_helpers.h"
#include <daemon/application.h>
#include <git/git_snapshot.h>
#include <watcher/watcher.h>
#include <store/store.h>
#include <errno.h>
#include <stdatomic.h>
#include <signal.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#ifdef _WIN32
#include "../src/foundation/win_utf8.h"
#endif
#ifdef __APPLE__
#include <libproc.h>
#endif

/* Portable git: `git -C "<dir>" <args>` with identity + non-interactive
 * config injected via -c, so it needs no global config and no POSIX shell
 * (runs under cmd.exe on Windows). Returns the git exit status. */
static int wt_git(const char *dir, const char *args) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" -c user.name=t -c user.email=t@t.io "
             "-c init.defaultBranch=master -c commit.gpgsign=false %s",
             dir, args);
    return system(cmd);
}
/* Build "<dir>/<rel>" into buf (forward slashes work on Windows + git). */
static const char *wt_path(char *buf, size_t n, const char *dir, const char *rel) {
    snprintf(buf, n, "%s/%s", dir, rel);
    return buf;
}

static bool wt_path_list_contains(char **paths, int count, const char *needle) {
    for (int i = 0; i < count; i++) {
        if (paths[i] && strcmp(paths[i], needle) == 0) {
            return true;
        }
    }
    return false;
}

static bool wt_mktempdir(char *buf, size_t n, const char *prefix) {
    char *path = th_mktempdir(prefix);
    if (!path) {
        return false;
    }
    int written = snprintf(buf, n, "%s", path);
    if (written < 0 || (size_t)written >= n) {
        th_rmtree(path);
        return false;
    }
    return true;
}

/* ══════════════════════════════════════════════════════════════════
 *  ADAPTIVE INTERVAL
 * ══════════════════════════════════════════════════════════════════ */

TEST(poll_interval_base) {
    /* 0 files → 5s base */
    int ms = cbm_watcher_poll_interval_ms(0, 0, 0);
    ASSERT_EQ(ms, 5000);
    PASS();
}

TEST(poll_interval_scaling) {
    /* 1000 files → 5000 + 2*1000 = 7000ms */
    int ms = cbm_watcher_poll_interval_ms(1000, 0, 0);
    ASSERT_EQ(ms, 7000);

    /* 5000 files → 5000 + 10*1000 = 15000ms */
    ms = cbm_watcher_poll_interval_ms(5000, 0, 0);
    ASSERT_EQ(ms, 15000);
    PASS();
}

TEST(poll_interval_cap) {
    /* 100K files → capped at 60s */
    int ms = cbm_watcher_poll_interval_ms(100000, 0, 0);
    ASSERT_EQ(ms, 60000);
    PASS();
}

TEST(poll_interval_small) {
    /* 499 files → 5000 + 0*1000 = 5000ms (integer division) */
    int ms = cbm_watcher_poll_interval_ms(499, 0, 0);
    ASSERT_EQ(ms, 5000);

    /* 500 files → 5000 + 1*1000 = 6000ms */
    ms = cbm_watcher_poll_interval_ms(500, 0, 0);
    ASSERT_EQ(ms, 6000);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  LIFECYCLE
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_create_free) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);
    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_watch_unwatch) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    cbm_watcher_watch(w, "project-a", "/tmp/project-a");
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    cbm_watcher_watch(w, "project-b", "/tmp/project-b");
    ASSERT_EQ(cbm_watcher_watch_count(w), 2);

    cbm_watcher_unwatch(w, "project-a");
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    cbm_watcher_unwatch(w, "project-b");
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_unwatch_nonexistent) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    /* Should not crash */
    cbm_watcher_unwatch(w, "nonexistent");
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_watch_replace) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    cbm_watcher_watch(w, "project-a", "/tmp/old-path");
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    /* Replace with new path */
    cbm_watcher_watch(w, "project-a", "/tmp/new-path");
    ASSERT_EQ(cbm_watcher_watch_count(w), 1); /* still 1 */

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_stopped_rejects_new_registration) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(w);

    cbm_watcher_stop(w);
    ASSERT_FALSE(cbm_watcher_watch(w, "late-project", "/tmp/late-project"));
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_null_safety) {
    /* All functions should be NULL-safe */
    cbm_watcher_free(NULL);
    cbm_watcher_watch(NULL, "x", "/x");
    cbm_watcher_unwatch(NULL, "x");
    cbm_watcher_touch(NULL, "x");
    ASSERT_EQ(cbm_watcher_watch_count(NULL), 0);
    ASSERT_EQ(cbm_watcher_poll_once(NULL), 0);
    PASS();
}

/* argv execution has no fixed shell-command formatting buffer. Registration
 * should not reject a nonempty path merely because its textual length exceeds
 * the old 1 KiB command string; filesystem existence is handled by polling. */
TEST(watcher_has_no_shell_command_buffer_path_limit) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    char long_path[CBM_SZ_2K];
    long_path[0] = '/';
    memset(long_path + 1, 'a', sizeof(long_path) - CBM_SZ_2);
    long_path[sizeof(long_path) - 1] = '\0';

    ASSERT_TRUE(cbm_watcher_watch(w, "long-argv-path", long_path));
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(git_snapshot_has_no_shell_command_buffer_path_limit) {
    char long_path[CBM_SZ_2K];
    long_path[0] = '/';
    memset(long_path + 1, 'b', sizeof(long_path) - CBM_SZ_2);
    long_path[sizeof(long_path) - 1] = '\0';

    cbm_git_snapshot_t snap = {0};
    ASSERT_TRUE(cbm_git_snapshot_path_supported(long_path));
    ASSERT_EQ(cbm_git_snapshot_read(long_path, CBM_GIT_SNAPSHOT_HEAD, &snap), 0);
    ASSERT_TRUE(snap.path_supported);
    ASSERT_FALSE(snap.is_git);
    PASS();
}

TEST(git_snapshot_non_git_path) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_git_snapshot_nongit_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed");
    }

    cbm_git_snapshot_t snap = {0};
    ASSERT_EQ(cbm_git_snapshot_read(tmpdir,
                                    CBM_GIT_SNAPSHOT_HEAD | CBM_GIT_SNAPSHOT_DIRTY |
                                        CBM_GIT_SNAPSHOT_FILE_COUNT,
                                    &snap),
              0);
    ASSERT_TRUE(snap.path_supported);
    ASSERT_FALSE(snap.is_git);
    ASSERT_EQ(snap.file_count, 0);
    ASSERT_STR_EQ(snap.dirty_hash, "0000000000000000");

    th_rmtree(tmpdir);
    PASS();
}

TEST(git_snapshot_clean_and_dirty_repo) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_git_snapshot_repo_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed");
    }

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_git_snapshot_t snap = {0};
    ASSERT_EQ(cbm_git_snapshot_read(tmpdir,
                                    CBM_GIT_SNAPSHOT_HEAD | CBM_GIT_SNAPSHOT_DIRTY |
                                        CBM_GIT_SNAPSHOT_FILE_COUNT,
                                    &snap),
              0);
    ASSERT_TRUE(snap.path_supported);
    ASSERT_TRUE(snap.is_git);
    ASSERT_TRUE(snap.head[0] != '\0');
    ASSERT_EQ(snap.file_count, 1);
    ASSERT_EQ(snap.dirty_bytes, 0);
    ASSERT_STR_EQ(snap.dirty_hash, "0000000000000000");

    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "new.go"), "package main\n");
    }
    ASSERT_EQ(cbm_git_snapshot_read(tmpdir, CBM_GIT_SNAPSHOT_DIRTY, &snap), 0);
    ASSERT_TRUE(snap.is_git);
    ASSERT_TRUE(snap.dirty_bytes > 0);
    ASSERT_TRUE(strcmp(snap.dirty_hash, "0000000000000000") != 0);

    th_rmtree(tmpdir);
    PASS();
}

TEST(git_status_paths_tracks_rename_current_and_previous_path) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_gitpaths_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) { th_rmtree(tmpdir); FAIL("git init failed"); }
    { char p[300]; th_write_file(wt_path(p, sizeof(p), tmpdir, "old.go"),
                                 "package main\n\nfunc Old() {}\n"); }
    wt_git(tmpdir, "add old.go");
    wt_git(tmpdir, "commit -q -m init");

    ASSERT_EQ(wt_git(tmpdir, "mv old.go new.go"), 0);
    char **paths = NULL;
    int count = 0;
    ASSERT_EQ(cbm_git_status_paths(tmpdir, &paths, &count), 0);
    ASSERT_TRUE(wt_path_list_contains(paths, count, "new.go"));
    ASSERT_TRUE(wt_path_list_contains(paths, count, "old.go"));
    cbm_git_status_paths_free(paths, count);

    th_rmtree(tmpdir);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  POLL WITH REAL GIT REPO
 * ══════════════════════════════════════════════════════════════════ */

/* Index callback counter */
static int index_call_count = 0;
static int index_callback(const char *name, const char *path, void *ud) {
    (void)name;
    (void)path;
    (void)ud;
    index_call_count++;
    return 0;
}

static int always_failing_index_callback(const char *name, const char *path, void *ud) {
    (void)name;
    (void)path;
    (void)ud;
    index_call_count++;
    return CBM_NOT_FOUND;
}

typedef struct {
    cbm_watcher_t *watcher;
    const char *replacement_path;
    int calls;
} watcher_mutating_cb_t;

static int unwatching_index_callback(const char *name, const char *path, void *ud) {
    (void)path;
    watcher_mutating_cb_t *ctx = (watcher_mutating_cb_t *)ud;
    ctx->calls++;
    cbm_watcher_unwatch(ctx->watcher, name);
    return 0;
}

static int replacing_index_callback(const char *name, const char *path, void *ud) {
    (void)path;
    watcher_mutating_cb_t *ctx = (watcher_mutating_cb_t *)ud;
    ctx->calls++;
    cbm_watcher_watch(ctx->watcher, name, ctx->replacement_path);
    return 0;
}

TEST(watcher_poll_no_projects) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    int reindexed = cbm_watcher_poll_once(w);
    ASSERT_EQ(reindexed, 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_unwatch_during_poll_callback_no_uaf) {
    char tmpdir[CBM_SZ_256];
    if (!wt_mktempdir(tmpdir, sizeof(tmpdir), "cbm_watcher_unwatch_cb")) {
        FAIL("cbm_mkdtemp failed");
    }
    if (wt_git(tmpdir, "init -q") != 0) { th_rmtree(tmpdir); FAIL("git init failed"); }
    { char p[CBM_SZ_512]; th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n"); }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    watcher_mutating_cb_t cb = {0};
    cbm_watcher_t *w = cbm_watcher_new(NULL, unwatching_index_callback, &cb);
    cb.watcher = w;
    cbm_watcher_watch(w, "unwatch-cb", tmpdir);
    cbm_watcher_poll_once(w); /* baseline */

    { char p[CBM_SZ_512]; th_append_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "changed\n"); }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m changed");

    cbm_watcher_touch(w, "unwatch-cb");
    ASSERT_EQ(cbm_watcher_poll_once(w), 1);
    ASSERT_EQ(cb.calls, 1);
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);
    ASSERT_EQ(cbm_watcher_poll_once(w), 0);

    cbm_watcher_free(w);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_replace_during_poll_callback_no_uaf) {
    char tmpdir_a[CBM_SZ_256];
    char tmpdir_b[CBM_SZ_256];
    bool made_a = wt_mktempdir(tmpdir_a, sizeof(tmpdir_a), "cbm_watcher_replace_a");
    bool made_b = wt_mktempdir(tmpdir_b, sizeof(tmpdir_b), "cbm_watcher_replace_b");
    if (!made_a || !made_b) {
        if (made_a) th_rmtree(tmpdir_a);
        if (made_b) th_rmtree(tmpdir_b);
        FAIL("cbm_mkdtemp failed");
    }
    if (wt_git(tmpdir_a, "init -q") != 0) {
        th_rmtree(tmpdir_a);
        th_rmtree(tmpdir_b);
        FAIL("git init failed");
    }
    { char p[CBM_SZ_512]; th_write_file(wt_path(p, sizeof(p), tmpdir_a, "file.txt"), "hello\n"); }
    wt_git(tmpdir_a, "add file.txt");
    wt_git(tmpdir_a, "commit -q -m init");

    watcher_mutating_cb_t cb = {.replacement_path = tmpdir_b};
    cbm_watcher_t *w = cbm_watcher_new(NULL, replacing_index_callback, &cb);
    cb.watcher = w;
    cbm_watcher_watch(w, "replace-cb", tmpdir_a);
    cbm_watcher_poll_once(w); /* baseline */

    { char p[CBM_SZ_512]; th_append_file(wt_path(p, sizeof(p), tmpdir_a, "file.txt"), "changed\n"); }
    wt_git(tmpdir_a, "add file.txt");
    wt_git(tmpdir_a, "commit -q -m changed");

    cbm_watcher_touch(w, "replace-cb");
    ASSERT_EQ(cbm_watcher_poll_once(w), 1);
    ASSERT_EQ(cb.calls, 1);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    ASSERT_EQ(cbm_watcher_poll_once(w), 0); /* replacement path gets its own baseline */

    cbm_watcher_free(w);
    th_rmtree(tmpdir_a);
    th_rmtree(tmpdir_b);
    PASS();
}

TEST(watcher_poll_nonexistent_path) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "ghost", "/tmp/cbm_test_nonexistent_path_12345");

    /* First poll → init_baseline (path doesn't exist → skip) */
    index_call_count = 0;
    int reindexed = cbm_watcher_poll_once(w);
    ASSERT_EQ(reindexed, 0);
    ASSERT_EQ(index_call_count, 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  STALE-ROOT PRUNING (#286)
 * ══════════════════════════════════════════════════════════════════ */

/* Shared fixture for the stale-root pruning tests: a temp project root, a
 * temp CBM_CACHE_DIR seeded with db/-wal/-shm files for "stale-project",
 * and saved copies of the env vars the tests override. */
typedef struct {
    char rootdir[256];
    char cachedir[256];
    char db_path[512];
    char wal_path[512];
    char shm_path[512];
    char saved_cache_dir[1024];
    bool had_cache_dir;
    char saved_grace[64];
    bool had_grace;
} prune_fixture_t;

/* Returns false (with partial state cleaned up) if setup failed. */
static bool prune_fixture_setup(prune_fixture_t *f, const char *grace_s) {
    snprintf(f->rootdir, sizeof(f->rootdir), "/tmp/cbm_watcher_stale_root_XXXXXX");
    if (!cbm_mkdtemp(f->rootdir)) {
        return false;
    }
    snprintf(f->cachedir, sizeof(f->cachedir), "/tmp/cbm_watcher_stale_cache_XXXXXX");
    if (!cbm_mkdtemp(f->cachedir)) {
        th_rmtree(f->rootdir);
        return false;
    }

    f->had_cache_dir = cbm_safe_getenv("CBM_CACHE_DIR", f->saved_cache_dir,
                                       sizeof(f->saved_cache_dir), NULL) != NULL;
    f->had_grace = cbm_safe_getenv("CBM_WATCHER_PRUNE_GRACE_S", f->saved_grace,
                                   sizeof(f->saved_grace), NULL) != NULL;
    cbm_setenv("CBM_CACHE_DIR", f->cachedir, 1);
    cbm_setenv("CBM_WATCHER_PRUNE_GRACE_S", grace_s, 1);

    snprintf(f->db_path, sizeof(f->db_path), "%s/stale-project.db", f->cachedir);
    snprintf(f->wal_path, sizeof(f->wal_path), "%s/stale-project.db-wal", f->cachedir);
    snprintf(f->shm_path, sizeof(f->shm_path), "%s/stale-project.db-shm", f->cachedir);
    th_write_file(f->db_path, "db\n");
    th_write_file(f->wal_path, "wal\n");
    th_write_file(f->shm_path, "shm\n");
    return true;
}

static void prune_fixture_teardown(prune_fixture_t *f) {
    if (f->had_cache_dir) {
        cbm_setenv("CBM_CACHE_DIR", f->saved_cache_dir, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    if (f->had_grace) {
        cbm_setenv("CBM_WATCHER_PRUNE_GRACE_S", f->saved_grace, 1);
    } else {
        cbm_unsetenv("CBM_WATCHER_PRUNE_GRACE_S");
    }
    th_rmtree(f->rootdir);
    th_rmtree(f->cachedir);
}

typedef struct {
    const char *root_to_restore;
    bool allow;
    bool restore_root;
    int begins;
    int ends;
    int pruned;
} prune_guard_probe_t;

static bool prune_guard_probe_begin(void *context, const char *project) {
    (void)project;
    prune_guard_probe_t *probe = context;
    probe->begins++;
    if (probe->restore_root && probe->root_to_restore) {
        (void)cbm_mkdir_p(probe->root_to_restore, 0755);
    }
    return probe->allow;
}

static void prune_guard_probe_end(void *context, const char *project) {
    (void)project;
    prune_guard_probe_t *probe = context;
    probe->ends++;
}

static void prune_guard_probe_pruned(void *context, const char *project) {
    (void)project;
    prune_guard_probe_t *probe = context;
    probe->pruned++;
}

TEST(watcher_prunes_sustained_missing_root) {
    /* Positive prune path. Grace window 0s isolates the streak-threshold
     * logic; the time gate is guarded by watcher_grace_window_blocks_prune. */
    prune_fixture_t f;
    if (!prune_fixture_setup(&f, "0")) {
        FAIL("prune fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "stale-project", f.rootdir);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    /* Existing root: first poll initializes baseline only. */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    th_rmtree(f.rootdir);

    /* Misses #1 and #2: below the streak threshold — keep project + DB. */
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    ASSERT_EQ(access(f.db_path, F_OK), 0);
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    ASSERT_EQ(access(f.db_path, F_OK), 0);

    /* Miss #3 with the grace window already satisfied: prune the watch
     * entry and the cached DB files. */
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);
    ASSERT_NEQ(access(f.db_path, F_OK), 0);
    ASSERT_NEQ(access(f.wal_path, F_OK), 0);
    ASSERT_NEQ(access(f.shm_path, F_OK), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    prune_fixture_teardown(&f);
    PASS();
}

TEST(watcher_prune_waits_for_daemon_project_mutation) {
    /* The daemon application owns project-operation coordination. Supplying
     * its watcher must route destructive stale-root pruning through that
     * coordination boundary: a busy project is retained and retried after
     * the active operation releases its lease, never unlinked directly. */
    prune_fixture_t f;
    if (!prune_fixture_setup(&f, "0")) {
        FAIL("prune fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_daemon_application_config_t config = {.watcher = w};
    cbm_daemon_application_t *application = cbm_daemon_application_new(&config);
    if (!store || !w || !application) {
        cbm_daemon_application_free(application);
        cbm_watcher_free(w);
        cbm_store_close(store);
        prune_fixture_teardown(&f);
        FAIL("coordinated prune fixture setup failed");
    }

    cbm_watcher_watch(w, "stale-project", f.rootdir);
    cbm_watcher_poll_once(w); /* establish the existing-root baseline */
    th_rmtree(f.rootdir);

    bool mutation_held =
        cbm_daemon_application_project_mutation_try_begin(application, "stale-project");
    for (int miss = 0; mutation_held && miss < 3; miss++) {
        cbm_watcher_touch(w, "stale-project");
        cbm_watcher_poll_once(w);
    }

    bool watch_preserved_while_busy = cbm_watcher_watch_count(w) == 1;
    bool files_preserved_while_busy = access(f.db_path, F_OK) == 0 &&
                                      access(f.wal_path, F_OK) == 0 &&
                                      access(f.shm_path, F_OK) == 0;

    if (mutation_held) {
        cbm_daemon_application_project_mutation_end(application, "stale-project");
    }
    if (cbm_watcher_watch_count(w) == 1) {
        cbm_watcher_touch(w, "stale-project");
        cbm_watcher_poll_once(w);
    }
    bool pruned_after_release = cbm_watcher_watch_count(w) == 0 && access(f.db_path, F_OK) != 0 &&
                                access(f.wal_path, F_OK) != 0 && access(f.shm_path, F_OK) != 0;

    cbm_daemon_application_free(application);
    cbm_watcher_free(w);
    cbm_store_close(store);
    prune_fixture_teardown(&f);

    ASSERT_TRUE(mutation_held);
    ASSERT_TRUE(watch_preserved_while_busy);
    ASSERT_TRUE(files_preserved_while_busy);
    ASSERT_TRUE(pruned_after_release);
    PASS();
}

TEST(watcher_prune_guard_denial_and_success_are_balanced) {
    prune_fixture_t f;
    if (!prune_fixture_setup(&f, "0")) {
        FAIL("prune fixture setup failed");
    }
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    prune_guard_probe_t probe = {0};
    cbm_watcher_set_project_mutation_guard(w, prune_guard_probe_begin, prune_guard_probe_end,
                                           prune_guard_probe_pruned, &probe);
    cbm_watcher_watch(w, "stale-project", f.rootdir);
    cbm_watcher_poll_once(w);
    th_rmtree(f.rootdir);
    for (int miss = 0; miss < 3; miss++) {
        cbm_watcher_touch(w, "stale-project");
        cbm_watcher_poll_once(w);
    }
    bool denied_preserved = cbm_watcher_watch_count(w) == 1 && access(f.db_path, F_OK) == 0 &&
                            probe.begins == 1 && probe.ends == 0 && probe.pruned == 0;

    probe.allow = true;
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    bool success_balanced = cbm_watcher_watch_count(w) == 0 && access(f.db_path, F_OK) != 0 &&
                            probe.begins == 2 && probe.ends == 1 && probe.pruned == 1;

    cbm_watcher_set_project_mutation_guard(w, NULL, NULL, NULL, NULL);
    cbm_watcher_free(w);
    cbm_store_close(store);
    prune_fixture_teardown(&f);
    ASSERT_TRUE(denied_preserved);
    ASSERT_TRUE(success_balanced);
    PASS();
}

TEST(watcher_prune_restats_root_after_guard_acquisition) {
    prune_fixture_t f;
    if (!prune_fixture_setup(&f, "0")) {
        FAIL("prune fixture setup failed");
    }
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    prune_guard_probe_t probe = {
        .root_to_restore = f.rootdir,
        .allow = true,
        .restore_root = true,
    };
    cbm_watcher_set_project_mutation_guard(w, prune_guard_probe_begin, prune_guard_probe_end,
                                           prune_guard_probe_pruned, &probe);
    cbm_watcher_watch(w, "stale-project", f.rootdir);
    cbm_watcher_poll_once(w);
    th_rmtree(f.rootdir);
    for (int miss = 0; miss < 3; miss++) {
        cbm_watcher_touch(w, "stale-project");
        cbm_watcher_poll_once(w);
    }

    bool restored = access(f.rootdir, F_OK) == 0;
    bool retained = cbm_watcher_watch_count(w) == 1 && access(f.db_path, F_OK) == 0 &&
                    access(f.wal_path, F_OK) == 0 && access(f.shm_path, F_OK) == 0;
    bool balanced = probe.begins == 1 && probe.ends == 1 && probe.pruned == 0;
    cbm_watcher_set_project_mutation_guard(w, NULL, NULL, NULL, NULL);
    cbm_watcher_free(w);
    cbm_store_close(store);
    prune_fixture_teardown(&f);
    ASSERT_TRUE(restored);
    ASSERT_TRUE(retained);
    ASSERT_TRUE(balanced);
    PASS();
}

TEST(watcher_prune_delete_failure_retains_watch_for_retry) {
    prune_fixture_t f;
    if (!prune_fixture_setup(&f, "0")) {
        FAIL("prune fixture setup failed");
    }
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "stale-project", f.rootdir);
    cbm_watcher_poll_once(w);
    th_rmtree(f.rootdir);

    /* A directory at the main DB path makes unlink fail deterministically on
     * POSIX and Windows. The watcher must remain registered for a later retry
     * and must not delete recoverable sidecars after that failure. */
    bool failure_ready = cbm_unlink(f.db_path) == 0 && cbm_mkdir_p(f.db_path, 0755);
    for (int miss = 0; failure_ready && miss < 3; miss++) {
        cbm_watcher_touch(w, "stale-project");
        cbm_watcher_poll_once(w);
    }
    bool watch_retained = cbm_watcher_watch_count(w) == 1;
    bool artifacts_retained = access(f.db_path, F_OK) == 0 && access(f.wal_path, F_OK) == 0 &&
                              access(f.shm_path, F_OK) == 0;

    cbm_watcher_free(w);
    cbm_store_close(store);
    prune_fixture_teardown(&f);
    ASSERT_TRUE(failure_ready);
    ASSERT_TRUE(watch_retained);
    ASSERT_TRUE(artifacts_retained);
    PASS();
}

TEST(watcher_grace_window_blocks_prune) {
    /* 3+ missing polls but elapsed < grace → NOT pruned. Uses an explicit
     * 600s window so a fast poll burst can never satisfy the time gate. */
    prune_fixture_t f;
    if (!prune_fixture_setup(&f, "600")) {
        FAIL("prune fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "stale-project", f.rootdir);

    cbm_watcher_poll_once(w); /* baseline */
    th_rmtree(f.rootdir);

    /* 4 consecutive misses in quick succession: streak threshold reached,
     * but the sustained-absence window (600s) has not elapsed. */
    for (int i = 0; i < 4; i++) {
        cbm_watcher_touch(w, "stale-project");
        cbm_watcher_poll_once(w);
    }
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    ASSERT_EQ(access(f.db_path, F_OK), 0);
    ASSERT_EQ(access(f.wal_path, F_OK), 0);
    ASSERT_EQ(access(f.shm_path, F_OK), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    prune_fixture_teardown(&f);
    PASS();
}

TEST(watcher_root_missing_errno_classification) {
    /* Only ENOENT/ENOTDIR may count toward pruning; EACCES-style failures
     * (permissions, I/O errors, transient mounts, macOS TCC revocation)
     * must never increment the missing streak. The classifier is unit-
     * tested with injected errno values because a real EACCES cannot be
     * simulated portably (tests may run as root on CI; Windows ACLs). */
    ASSERT_TRUE(cbm_watcher_root_missing_errno(ENOENT));
    ASSERT_TRUE(cbm_watcher_root_missing_errno(ENOTDIR));
    ASSERT_FALSE(cbm_watcher_root_missing_errno(0));
    ASSERT_FALSE(cbm_watcher_root_missing_errno(EACCES));
    ASSERT_FALSE(cbm_watcher_root_missing_errno(EIO));
    ASSERT_FALSE(cbm_watcher_root_missing_errno(EINVAL));
    ASSERT_FALSE(cbm_watcher_root_missing_errno(ENAMETOOLONG));
    PASS();
}

TEST(watcher_root_restore_resets_prune_streak) {
    /* A reappearing root must reset the missing streak AND its first-miss
     * timestamp — pruning requires a fresh uninterrupted streak. */
    prune_fixture_t f;
    if (!prune_fixture_setup(&f, "0")) {
        FAIL("prune fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "stale-project", f.rootdir);

    cbm_watcher_poll_once(w); /* baseline */
    th_rmtree(f.rootdir);

    /* Misses #1 and #2 — one short of the threshold. */
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    /* Root comes back (e.g. remount / re-clone): streak resets. */
    if (!cbm_mkdir_p(f.rootdir, 0755)) {
        FAIL("mkdir_p restore failed");
    }
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    th_rmtree(f.rootdir);

    /* Misses #1 and #2 of the NEW streak: must not prune even though the
     * total number of misses is now four. */
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    ASSERT_EQ(access(f.db_path, F_OK), 0);

    /* Miss #3 of the new streak → prune. */
    cbm_watcher_touch(w, "stale-project");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);
    ASSERT_NEQ(access(f.db_path, F_OK), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    prune_fixture_teardown(&f);
    PASS();
}

TEST(watcher_poll_this_repo) {
    /* Use this project's own repo as a real git repo test */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    /* Watch our own repo root (we know it's a git repo) */
    char cwd[4096];
    if (!getcwd(cwd, sizeof(cwd))) {
        cbm_watcher_free(w);
        cbm_store_close(store);
        FAIL("getcwd failed");
    }

    cbm_watcher_watch(w, "self", cwd);

    /* First poll: init baseline (no reindex expected) */
    index_call_count = 0;
    int reindexed = cbm_watcher_poll_once(w);
    ASSERT_EQ(reindexed, 0); /* baseline only */

    /* Second poll: check for changes. This repo has dirty working tree
     * (from the tests we just created), so it should detect changes.
     * But the adaptive interval hasn't elapsed yet, so it won't poll. */

    /* Touch to reset interval, then poll */
    cbm_watcher_touch(w, "self");
    reindexed = cbm_watcher_poll_once(w);
    /* May or may not reindex depending on whether working tree is dirty.
     * In CI, working tree might be clean. Just verify it doesn't crash. */

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_stop_flag) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    /* Set stop flag */
    cbm_watcher_stop(w);

    /* Run should return immediately */
    int rc = cbm_watcher_run(w, 1000, 0);
    ASSERT_EQ(rc, 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

typedef struct {
    cbm_watcher_t *watcher;
    int run_status;
} watcher_run_context_t;

static void *watcher_run_for_stop_test(void *opaque) {
    watcher_run_context_t *context = opaque;
    enum { WATCHER_TEST_LONG_POLL_MS = 5000 };
    context->run_status =
        cbm_watcher_run(context->watcher, WATCHER_TEST_LONG_POLL_MS, WATCHER_TEST_LONG_POLL_MS);
    return NULL;
}

TEST(watcher_stop_wakes_parked_run_loop) {
    enum {
        WATCHER_TEST_WAIT_READY_MS = 5000,
        /* A synchronized condition wake is normally sub-millisecond. This is
         * deliberately a coarse hang detector: it remains tolerant of loaded
         * sanitizer/Windows runners while rejecting the former 500 ms polling
         * sleep that delayed every retiring daemon. */
        WATCHER_TEST_STOP_WAKE_MAX_MS = 400,
    };
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *watcher = cbm_watcher_new(store, NULL, NULL);
    watcher_run_context_t context = {
        .watcher = watcher,
        .run_status = CBM_NOT_FOUND,
    };
    cbm_thread_t thread = {0};
    int create_status = watcher ? cbm_thread_create(&thread, 0, watcher_run_for_stop_test, &context)
                                : CBM_NOT_FOUND;
    uint64_t ready_deadline = cbm_now_ms() + WATCHER_TEST_WAIT_READY_MS;
    while (create_status == 0 && cbm_watcher_waiter_count_for_test(watcher) == 0 &&
           cbm_now_ms() < ready_deadline) {
        cbm_usleep(CBM_USEC_PER_MSEC);
    }
    bool parked = create_status == 0 && cbm_watcher_waiter_count_for_test(watcher) == 1;
    uint64_t stop_started_ms = cbm_now_ms();
    if (watcher) {
        cbm_watcher_stop(watcher);
    }
    int join_status = create_status == 0 ? cbm_thread_join(&thread) : CBM_NOT_FOUND;
    uint64_t stop_elapsed_ms = cbm_now_ms() - stop_started_ms;

    ASSERT_TRUE(parked);
    ASSERT_EQ(join_status, 0);
    ASSERT_EQ(context.run_status, 0);
    ASSERT_TRUE(stop_elapsed_ms <= WATCHER_TEST_STOP_WAKE_MAX_MS);
    cbm_watcher_free(watcher);
    cbm_store_close(store);
    PASS();
}

/* test_main turns an exact private-path alias named git/git.exe into a
 * deterministic child that publishes its PID and ignores graceful shutdown.
 * This reproduces the production failure where popen() left watcher shutdown
 * blocked forever in pclose(). The test owns a verified forced-termination
 * backstop so the pre-fix RED cannot wedge the rest of the suite. */
#define WATCHER_TEST_BLOCKING_GIT_MARKER_ENV "CBM_TEST_RUNTIME_BLOCKING_GIT_PID_FILE"
/* Budget for the blocked-git child to spawn and write its PID marker (watcher
 * poll + process spawn + first write). 5s was missed twice on starved 4-vCPU
 * windows-11-arm runners (release run 30300256783, both attempts) with no
 * failure on any faster leg — spawn latency under CPU starvation, not a
 * watcher defect. 20s keeps the wait bounded without re-arming that miss. */
#define WATCHER_TEST_SPAWN_MARKER_BUDGET_MS 20000

#ifdef _WIN32
typedef struct {
    cbm_watcher_t *watcher;
    atomic_bool completed;
} watcher_windows_blocked_run_t;

typedef struct {
    char canonical_path[CBM_SZ_4K];
    BY_HANDLE_FILE_INFORMATION information;
} watcher_windows_image_identity_t;

static void *watcher_windows_blocked_run_thread(void *opaque) {
    watcher_windows_blocked_run_t *run = opaque;
    /* base_ms == max_ms pins the cadence at upstream's fixed 10 ms; passing 0
     * for max_ms would restore the 60 s adaptive cap this test must not wait on. */
    (void)cbm_watcher_run(run->watcher, 10, 10);
    atomic_store_explicit(&run->completed, true, memory_order_release);
    return NULL;
}

static bool watcher_windows_copy_self(const char *destination) {
    wchar_t source[32768];
    DWORD source_length =
        GetModuleFileNameW(NULL, source, (DWORD)(sizeof(source) / sizeof(source[0])));
    wchar_t *destination_wide = cbm_utf8_to_wide(destination);
    bool copied = source_length > 0 &&
                  source_length < (DWORD)(sizeof(source) / sizeof(source[0])) && destination_wide &&
                  CopyFileW(source, destination_wide, TRUE) != 0;
    free(destination_wide);
    return copied;
}

static bool watcher_windows_capture_image_identity(const char *path,
                                                   watcher_windows_image_identity_t *identity) {
    if (!path || !identity) {
        return false;
    }
    memset(identity, 0, sizeof(*identity));
    if (!cbm_canonical_path(path, identity->canonical_path, sizeof(identity->canonical_path))) {
        return false;
    }
    wchar_t *wide = cbm_utf8_to_wide(identity->canonical_path);
    HANDLE file = wide ? CreateFileW(wide, FILE_READ_ATTRIBUTES,
                                     FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                     OPEN_EXISTING, FILE_FLAG_OPEN_REPARSE_POINT, NULL)
                       : INVALID_HANDLE_VALUE;
    bool captured = file != INVALID_HANDLE_VALUE && GetFileType(file) == FILE_TYPE_DISK &&
                    GetFileInformationByHandle(file, &identity->information) != 0 &&
                    (identity->information.dwFileAttributes &
                     (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0;
    if (file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(file);
    }
    free(wide);
    if (!captured) {
        memset(identity, 0, sizeof(*identity));
    }
    return captured;
}

static bool watcher_windows_same_image_identity(const watcher_windows_image_identity_t *first,
                                                const watcher_windows_image_identity_t *second) {
    return first && second && _stricmp(first->canonical_path, second->canonical_path) == 0 &&
           first->information.dwVolumeSerialNumber == second->information.dwVolumeSerialNumber &&
           first->information.nFileIndexHigh == second->information.nFileIndexHigh &&
           first->information.nFileIndexLow == second->information.nFileIndexLow;
}

static bool watcher_windows_process_matches_image(
    HANDLE process, const watcher_windows_image_identity_t *expected) {
    if (!process || !expected) {
        return false;
    }
    DWORD exit_code = 0;
    wchar_t image[32768];
    DWORD image_length = (DWORD)(sizeof(image) / sizeof(image[0]));
    bool queried = GetExitCodeProcess(process, &exit_code) != 0 && exit_code == STILL_ACTIVE &&
                   QueryFullProcessImageNameW(process, 0U, image, &image_length) != 0 &&
                   image_length > 0 && image_length < (DWORD)(sizeof(image) / sizeof(image[0]));
    if (queried) {
        image[image_length] = L'\0';
    }
    char *image_utf8 = queried ? cbm_wide_to_utf8(image) : NULL;
    watcher_windows_image_identity_t actual;
    bool captured = image_utf8 && watcher_windows_capture_image_identity(image_utf8, &actual);
    free(image_utf8);
    return captured && watcher_windows_same_image_identity(&actual, expected);
}

static bool watcher_windows_wait_pid(const char *marker, uint64_t deadline_ms,
                                     uint64_t *process_id_out) {
    if (process_id_out) {
        *process_id_out = 0;
    }
    while (cbm_now_ms() < deadline_ms) {
        FILE *file = cbm_fopen(marker, "rb");
        unsigned long long parsed = 0;
        bool valid = file && fscanf(file, "%llu", &parsed) == 1 && parsed > 1 &&
                     parsed <= MAXDWORD && parsed != (unsigned long long)GetCurrentProcessId();
        if (file) {
            (void)fclose(file);
        }
        if (valid) {
            if (process_id_out) {
                *process_id_out = (uint64_t)parsed;
            }
            return true;
        }
        cbm_usleep(1000);
    }
    return false;
}

static HANDLE watcher_windows_open_exact_process(uint64_t process_id,
                                                 const watcher_windows_image_identity_t *expected) {
    if (process_id <= 1 || process_id > MAXDWORD || process_id == (uint64_t)GetCurrentProcessId()) {
        return NULL;
    }
    HANDLE process =
        OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION | PROCESS_TERMINATE | SYNCHRONIZE, FALSE,
                    (DWORD)process_id);
    if (!watcher_windows_process_matches_image(process, expected)) {
        if (process) {
            (void)CloseHandle(process);
        }
        return NULL;
    }
    return process;
}

static bool watcher_windows_wait_complete(atomic_bool *completed, uint64_t deadline_ms) {
    while (cbm_now_ms() < deadline_ms) {
        if (atomic_load_explicit(completed, memory_order_acquire)) {
            return true;
        }
        cbm_usleep(1000);
    }
    return atomic_load_explicit(completed, memory_order_acquire);
}

static bool watcher_windows_wait_process_gone(HANDLE process, uint64_t deadline_ms) {
    while (cbm_now_ms() < deadline_ms) {
        if (WaitForSingleObject(process, 0U) == WAIT_OBJECT_0) {
            return true;
        }
        cbm_usleep(1000);
    }
    return WaitForSingleObject(process, 0U) == WAIT_OBJECT_0;
}

static bool watcher_windows_terminate_verified(HANDLE process,
                                               const watcher_windows_image_identity_t *expected,
                                               uint64_t deadline_ms) {
    if (!process) {
        return false;
    }
    if (WaitForSingleObject(process, 0U) == WAIT_OBJECT_0) {
        return true;
    }
    /* Re-query the live process image immediately before the only test-side
     * kill. The retained handle fixes the process instance; canonical path and
     * file identity ensure a cleanup backstop can target only our private copy. */
    if (!watcher_windows_process_matches_image(process, expected) ||
        TerminateProcess(process, 99U) == 0) {
        return false;
    }
    return watcher_windows_wait_process_gone(process, deadline_ms);
}
#endif

#if defined(__APPLE__) || defined(__linux__)
typedef struct {
    cbm_watcher_t *watcher;
    atomic_bool completed;
} watcher_blocked_run_t;

static void *watcher_blocked_run_thread(void *opaque) {
    watcher_blocked_run_t *run = opaque;
    /* base_ms == max_ms pins the cadence at upstream's fixed 10 ms; passing 0
     * for max_ms would restore the 60 s adaptive cap this test must not wait on. */
    (void)cbm_watcher_run(run->watcher, 10, 10);
    atomic_store_explicit(&run->completed, true, memory_order_release);
    return NULL;
}

static bool watcher_test_self_image(char out[CBM_SZ_4K]) {
#ifdef __APPLE__
    int length = proc_pidpath(getpid(), out, CBM_SZ_4K);
    if (length <= 0 || length >= CBM_SZ_4K) {
        return false;
    }
    out[length] = '\0';
    return true;
#else
    ssize_t length = readlink("/proc/self/exe", out, CBM_SZ_4K - 1);
    if (length <= 0 || length >= CBM_SZ_4K - 1) {
        return false;
    }
    out[length] = '\0';
    return true;
#endif
}

static bool watcher_test_wait_pid(const char *marker, uint64_t deadline_ms, pid_t *pid_out) {
    while (cbm_now_ms() < deadline_ms) {
        FILE *file = cbm_fopen(marker, "rb");
        long parsed = 0;
        bool valid = file && fscanf(file, "%ld", &parsed) == 1 && parsed > 1 && parsed != getpid();
        if (file) {
            (void)fclose(file);
        }
        if (valid) {
            *pid_out = (pid_t)parsed;
            return true;
        }
        cbm_usleep(1000);
    }
    return false;
}

static bool watcher_test_wait_complete(atomic_bool *completed, uint64_t deadline_ms) {
    while (cbm_now_ms() < deadline_ms) {
        if (atomic_load_explicit(completed, memory_order_acquire)) {
            return true;
        }
        cbm_usleep(1000);
    }
    return atomic_load_explicit(completed, memory_order_acquire);
}

static bool watcher_test_wait_process_gone(pid_t process, uint64_t deadline_ms) {
    while (cbm_now_ms() < deadline_ms) {
        errno = 0;
        if (kill(process, 0) != 0 && errno == ESRCH) {
            return true;
        }
        cbm_usleep(1000);
    }
    errno = 0;
    return kill(process, 0) != 0 && errno == ESRCH;
}
#endif

TEST(watcher_stop_and_unwatch_cancel_blocked_git_without_backstop) {
#ifdef _WIN32
    char work[CBM_SZ_1K] = "/tmp/cbm-watcher-blocked-git-XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(work));
    char root[CBM_SZ_1K];
    char bin[CBM_SZ_1K];
    char fake_git[CBM_SZ_1K];
    char marker[CBM_SZ_1K];
    char unwatch_marker[CBM_SZ_1K];
    ASSERT_TRUE(snprintf(root, sizeof(root), "%s/root", work) > 0);
    ASSERT_TRUE(snprintf(bin, sizeof(bin), "%s/bin", work) > 0);
    ASSERT_TRUE(snprintf(fake_git, sizeof(fake_git), "%s/git.exe", bin) > 0);
    ASSERT_TRUE(snprintf(marker, sizeof(marker), "%s/git.pid", work) > 0);
    ASSERT_TRUE(snprintf(unwatch_marker, sizeof(unwatch_marker), "%s/unwatch-git.pid", work) > 0);
    ASSERT_TRUE(cbm_mkdir_p(root, 0700));
    ASSERT_TRUE(cbm_mkdir_p(bin, 0700));
    ASSERT_TRUE(watcher_windows_copy_self(fake_git));
    watcher_windows_image_identity_t expected_git;
    ASSERT_TRUE(watcher_windows_capture_image_identity(fake_git, &expected_git));

    const char *old_path_raw = getenv("PATH");
    const char *old_marker_raw = getenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV);
    char *old_path = old_path_raw ? cbm_strdup(old_path_raw) : NULL;
    char *old_marker = old_marker_raw ? cbm_strdup(old_marker_raw) : NULL;
    size_t path_size = strlen(bin) + 2 + (old_path ? strlen(old_path) : 0);
    char *path = malloc(path_size);
    ASSERT_NOT_NULL(path);
    (void)snprintf(path, path_size, "%s;%s", bin, old_path ? old_path : "");
    ASSERT_EQ(cbm_setenv("PATH", path, 1), 0);
    ASSERT_EQ(cbm_setenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV, marker, 1), 0);

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *watcher = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(watcher);
    ASSERT_TRUE(cbm_watcher_watch(watcher, "blocked-git", root));
    watcher_windows_blocked_run_t run = {.watcher = watcher};
    atomic_init(&run.completed, false);
    cbm_thread_t thread;
    ASSERT_EQ(cbm_thread_create(&thread, 128U * 1024U, watcher_windows_blocked_run_thread, &run),
              0);

    uint64_t child_id = 0;
    bool marker_ready = watcher_windows_wait_pid(
        marker, cbm_now_ms() + WATCHER_TEST_SPAWN_MARKER_BUDGET_MS, &child_id);
    HANDLE child_process =
        marker_ready ? watcher_windows_open_exact_process(child_id, &expected_git) : NULL;
    bool exact_stop_image = child_process != NULL;
    cbm_watcher_stop(watcher);
    uint64_t stop_deadline = cbm_now_ms() + 2500;
    bool stop_process_gone =
        exact_stop_image && watcher_windows_wait_process_gone(child_process, stop_deadline);
    bool stop_run_completed = watcher_windows_wait_complete(&run.completed, stop_deadline);
    bool stopped_without_backstop = stop_process_gone && stop_run_completed;
    bool stop_cleanup_terminated = true;
    if (exact_stop_image && !stop_process_gone) {
        stop_cleanup_terminated =
            watcher_windows_terminate_verified(child_process, &expected_git, cbm_now_ms() + 5000);
    }
    bool cleanup_process_gone =
        exact_stop_image && watcher_windows_wait_process_gone(child_process, cbm_now_ms() + 5000);
    bool cleanup_completed = watcher_windows_wait_complete(&run.completed, cbm_now_ms() + 5000);
    int join_rc = cleanup_completed ? cbm_thread_join(&thread) : -1;
    if (child_process) {
        (void)CloseHandle(child_process);
    }
    cbm_watcher_free(watcher);

    /* A project losing its final daemon-session owner must cancel that
     * project's in-flight Git tree even while the shared watcher stays live. */
    ASSERT_EQ(cbm_setenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV, unwatch_marker, 1), 0);
    cbm_watcher_t *unwatch_watcher = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(unwatch_watcher);
    ASSERT_TRUE(cbm_watcher_watch(unwatch_watcher, "blocked-unwatch", root));
    watcher_windows_blocked_run_t unwatch_run = {.watcher = unwatch_watcher};
    atomic_init(&unwatch_run.completed, false);
    cbm_thread_t unwatch_thread;
    ASSERT_EQ(cbm_thread_create(&unwatch_thread, 128U * 1024U, watcher_windows_blocked_run_thread,
                                &unwatch_run),
              0);
    uint64_t unwatch_child_id = 0;
    bool unwatch_marker_ready = watcher_windows_wait_pid(
        unwatch_marker, cbm_now_ms() + WATCHER_TEST_SPAWN_MARKER_BUDGET_MS, &unwatch_child_id);
    HANDLE unwatch_process =
        unwatch_marker_ready ? watcher_windows_open_exact_process(unwatch_child_id, &expected_git)
                             : NULL;
    bool exact_unwatch_image = unwatch_process != NULL;
    cbm_watcher_unwatch(unwatch_watcher, "blocked-unwatch");
    bool unwatch_cancelled_without_backstop =
        exact_unwatch_image &&
        watcher_windows_wait_process_gone(unwatch_process, cbm_now_ms() + 2500);
    bool unwatch_cleanup_terminated = true;
    if (exact_unwatch_image && !unwatch_cancelled_without_backstop) {
        unwatch_cleanup_terminated =
            watcher_windows_terminate_verified(unwatch_process, &expected_git, cbm_now_ms() + 5000);
    }
    bool unwatch_cleanup_gone = exact_unwatch_image && watcher_windows_wait_process_gone(
                                                           unwatch_process, cbm_now_ms() + 5000);
    cbm_watcher_stop(unwatch_watcher);
    bool unwatch_run_completed =
        watcher_windows_wait_complete(&unwatch_run.completed, cbm_now_ms() + 5000);
    int unwatch_join_rc = unwatch_run_completed ? cbm_thread_join(&unwatch_thread) : -1;
    if (unwatch_process) {
        (void)CloseHandle(unwatch_process);
    }
    cbm_watcher_free(unwatch_watcher);
    cbm_store_close(store);
    if (old_path) {
        (void)cbm_setenv("PATH", old_path, 1);
    } else {
        (void)cbm_unsetenv("PATH");
    }
    if (old_marker) {
        (void)cbm_setenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV, old_marker, 1);
    } else {
        (void)cbm_unsetenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV);
    }
    free(path);
    free(old_marker);
    free(old_path);
    (void)th_rmtree(work);

    ASSERT_TRUE(marker_ready);
    ASSERT_TRUE(exact_stop_image);
    ASSERT_TRUE(stop_cleanup_terminated);
    ASSERT_TRUE(cleanup_process_gone);
    ASSERT_TRUE(cleanup_completed);
    ASSERT_EQ(join_rc, 0);
    ASSERT_TRUE(stopped_without_backstop);
    ASSERT_TRUE(unwatch_marker_ready);
    ASSERT_TRUE(exact_unwatch_image);
    ASSERT_TRUE(unwatch_cleanup_terminated);
    ASSERT_TRUE(unwatch_cancelled_without_backstop);
    ASSERT_TRUE(unwatch_cleanup_gone);
    ASSERT_TRUE(unwatch_run_completed);
    ASSERT_EQ(unwatch_join_rc, 0);
    PASS();
#elif defined(__APPLE__) || defined(__linux__)
    char work[CBM_SZ_1K] = "/tmp/cbm-watcher-blocked-git-XXXXXX";
    ASSERT_NOT_NULL(cbm_mkdtemp(work));
    char root[CBM_SZ_1K];
    char bin[CBM_SZ_1K];
    char fake_git[CBM_SZ_1K];
    char marker[CBM_SZ_1K];
    char unwatch_marker[CBM_SZ_1K];
    char self[CBM_SZ_4K];
    ASSERT_TRUE(snprintf(root, sizeof(root), "%s/root", work) > 0);
    ASSERT_TRUE(snprintf(bin, sizeof(bin), "%s/bin", work) > 0);
    ASSERT_TRUE(snprintf(fake_git, sizeof(fake_git), "%s/git", bin) > 0);
    ASSERT_TRUE(snprintf(marker, sizeof(marker), "%s/git.pid", work) > 0);
    ASSERT_TRUE(snprintf(unwatch_marker, sizeof(unwatch_marker), "%s/unwatch-git.pid", work) > 0);
    ASSERT_TRUE(cbm_mkdir_p(root, 0700));
    ASSERT_TRUE(cbm_mkdir_p(bin, 0700));
    ASSERT_TRUE(watcher_test_self_image(self));
    ASSERT_EQ(symlink(self, fake_git), 0);

    const char *old_path_raw = getenv("PATH");
    const char *old_marker_raw = getenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV);
    char *old_path = old_path_raw ? strdup(old_path_raw) : NULL;
    char *old_marker = old_marker_raw ? strdup(old_marker_raw) : NULL;
    size_t path_size = strlen(bin) + 2 + (old_path ? strlen(old_path) : 0);
    char *path = malloc(path_size);
    ASSERT_NOT_NULL(path);
    (void)snprintf(path, path_size, "%s:%s", bin, old_path ? old_path : "");
    ASSERT_EQ(cbm_setenv("PATH", path, 1), 0);
    ASSERT_EQ(cbm_setenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV, marker, 1), 0);

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *watcher = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(watcher);
    cbm_watcher_watch(watcher, "blocked-git", root);
    watcher_blocked_run_t run = {.watcher = watcher};
    atomic_init(&run.completed, false);
    cbm_thread_t thread;
    ASSERT_EQ(cbm_thread_create(&thread, 128U * 1024U, watcher_blocked_run_thread, &run), 0);

    pid_t child = 0;
    bool marker_ready =
        watcher_test_wait_pid(marker, cbm_now_ms() + WATCHER_TEST_SPAWN_MARKER_BUDGET_MS, &child);
    cbm_watcher_stop(watcher);
    bool stopped_without_backstop =
        marker_ready && watcher_test_wait_complete(&run.completed, cbm_now_ms() + 2500);
    if (marker_ready && !stopped_without_backstop) {
        (void)kill(child, SIGKILL);
    }
    bool cleanup_completed = watcher_test_wait_complete(&run.completed, cbm_now_ms() + 5000);
    int join_rc = cleanup_completed ? cbm_thread_join(&thread) : -1;

    cbm_watcher_free(watcher);

    /* A project losing its final daemon-session owner must cancel that
     * project's in-flight Git tree even while the shared watcher stays live. */
    ASSERT_EQ(cbm_setenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV, unwatch_marker, 1), 0);
    cbm_watcher_t *unwatch_watcher = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(unwatch_watcher);
    ASSERT_TRUE(cbm_watcher_watch(unwatch_watcher, "blocked-unwatch", root));
    watcher_blocked_run_t unwatch_run = {.watcher = unwatch_watcher};
    atomic_init(&unwatch_run.completed, false);
    cbm_thread_t unwatch_thread;
    ASSERT_EQ(
        cbm_thread_create(&unwatch_thread, 128U * 1024U, watcher_blocked_run_thread, &unwatch_run),
        0);
    pid_t unwatch_child = 0;
    bool unwatch_marker_ready = watcher_test_wait_pid(
        unwatch_marker, cbm_now_ms() + WATCHER_TEST_SPAWN_MARKER_BUDGET_MS, &unwatch_child);
    cbm_watcher_unwatch(unwatch_watcher, "blocked-unwatch");
    bool unwatch_cancelled_without_backstop =
        unwatch_marker_ready && watcher_test_wait_process_gone(unwatch_child, cbm_now_ms() + 2500);
    if (unwatch_marker_ready && !unwatch_cancelled_without_backstop) {
        (void)kill(unwatch_child, SIGKILL);
    }
    bool unwatch_cleanup_gone =
        unwatch_marker_ready && watcher_test_wait_process_gone(unwatch_child, cbm_now_ms() + 5000);
    cbm_watcher_stop(unwatch_watcher);
    bool unwatch_run_completed =
        watcher_test_wait_complete(&unwatch_run.completed, cbm_now_ms() + 5000);
    int unwatch_join_rc = unwatch_run_completed ? cbm_thread_join(&unwatch_thread) : -1;
    cbm_watcher_free(unwatch_watcher);
    cbm_store_close(store);
    if (old_path) {
        (void)cbm_setenv("PATH", old_path, 1);
    } else {
        (void)cbm_unsetenv("PATH");
    }
    if (old_marker) {
        (void)cbm_setenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV, old_marker, 1);
    } else {
        (void)cbm_unsetenv(WATCHER_TEST_BLOCKING_GIT_MARKER_ENV);
    }
    free(path);
    free(old_marker);
    free(old_path);
    (void)th_rmtree(work);

    ASSERT_TRUE(marker_ready);
    ASSERT_TRUE(cleanup_completed);
    ASSERT_EQ(join_rc, 0);
    ASSERT_TRUE(stopped_without_backstop);
    ASSERT_TRUE(unwatch_marker_ready);
    ASSERT_TRUE(unwatch_cancelled_without_backstop);
    ASSERT_TRUE(unwatch_cleanup_gone);
    ASSERT_TRUE(unwatch_run_completed);
    ASSERT_EQ(unwatch_join_rc, 0);
    PASS();
#else
    SKIP_PLATFORM("copied test-runner git probe is supported on Windows/macOS/Linux");
#endif
}

/* ══════════════════════════════════════════════════════════════════
 *  GIT CHANGE DETECTION (with temp repo)
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_detects_git_commit) {
    /* Create a temporary git repo */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_test_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char file_path[512];
    if (wt_git(tmpdir, "init -q") != 0 ||
        th_write_file(wt_path(file_path, sizeof(file_path), tmpdir, "file.txt"), "hello\n") != 0 ||
        wt_git(tmpdir, "add file.txt") != 0 || wt_git(tmpdir, "commit -q -m init") != 0) {
        th_rmtree(tmpdir);
        FAIL("git fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "temp-repo", tmpdir);
    index_call_count = 0;

    /* First poll: baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make a change: new commit */
    if (th_append_file(file_path, "world\n") != 0 || wt_git(tmpdir, "add file.txt") != 0 ||
        wt_git(tmpdir, "commit -q -m add-world") != 0) {
        cbm_watcher_free(w);
        cbm_store_close(store);
        th_rmtree(tmpdir);
        FAIL("git fixture mutation failed");
    }

    /* Touch to bypass interval, then poll */
    cbm_watcher_touch(w, "temp-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* should detect HEAD change */

    /* Poll again without changes → no reindex */
    cbm_watcher_touch(w, "temp-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* still 1, no new changes */

    /* Cleanup */
    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

/* SHA-256 repositories emit a 64-hex-character HEAD. The watcher must retain
 * the complete object ID (plus line terminator/NUL while reading it), otherwise
 * baseline initialization silently retries forever and auto-refresh never runs. */
TEST(watcher_detects_sha256_git_commit) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_sha256_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed");
    }
    if (wt_git(tmpdir, "init -q --object-format=sha256") != 0) {
        th_rmtree(tmpdir);
        SKIP_PLATFORM("installed Git does not support SHA-256 repositories");
    }
    char path[300];
    th_write_file(wt_path(path, sizeof(path), tmpdir, "file.txt"), "first\n");
    ASSERT_EQ(wt_git(tmpdir, "add file.txt"), 0);
    ASSERT_EQ(wt_git(tmpdir, "commit -q -m first"), 0);

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *watcher = cbm_watcher_new(store, index_callback, NULL);
    ASSERT_NOT_NULL(watcher);
    ASSERT_TRUE(cbm_watcher_watch(watcher, "sha256-repo", tmpdir));
    index_call_count = 0;
    ASSERT_EQ(cbm_watcher_poll_once(watcher), 0); /* establish baseline */

    th_append_file(path, "second\n");
    ASSERT_EQ(wt_git(tmpdir, "add file.txt"), 0);
    ASSERT_EQ(wt_git(tmpdir, "commit -q -m second"), 0);
    cbm_watcher_touch(watcher, "sha256-repo");
    (void)cbm_watcher_poll_once(watcher);

    int observed = index_call_count;
    cbm_watcher_free(watcher);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    ASSERT_EQ(observed, 1);
    PASS();
}

TEST(watcher_detects_dirty_worktree) {
    /* Create a temporary git repo */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_dirty_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "dirty-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);

    /* Make working tree dirty (uncommitted change) */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "modified\n");
    }

    /* Poll → should detect dirty worktree */
    cbm_watcher_touch(w, "dirty-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* Cleanup */
    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_marks_dirty_file_before_failed_index_callback) {
    char tmpdir[256];
    char dirty_path[300];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_dirty_ledger_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) { th_rmtree(tmpdir); FAIL("git init failed"); }
    { char p[300]; th_write_file(wt_path(p, sizeof(p), tmpdir, "file.go"),
                                 "package main\n\nfunc Old() {}\n"); }
    wt_git(tmpdir, "add file.go");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "dirty-ledger-repo", tmpdir), CBM_STORE_OK);
    cbm_watcher_t *w = cbm_watcher_new(store, always_failing_index_callback, NULL);
    ASSERT_NOT_NULL(w);

    cbm_watcher_watch(w, "dirty-ledger-repo", tmpdir);
    index_call_count = 0;
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    th_append_file(wt_path(dirty_path, sizeof(dirty_path), tmpdir, "file.go"),
                   "\nfunc NewDirty() {}\n");

    cbm_watcher_touch(w, "dirty-ledger-repo");
    ASSERT_EQ(cbm_watcher_poll_once(w), 0);
    ASSERT_EQ(index_call_count, 1);

    int pending = -1;
    int overlay_ready = -1;
    ASSERT_EQ(cbm_store_count_dirty_files(store, "dirty-ledger-repo", &pending,
                                          &overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(pending, 1);
    ASSERT_EQ(overlay_ready, 0);
    cbm_dirty_file_state_t *dirty_rows = NULL;
    int dirty_row_count = 0;
    ASSERT_EQ(cbm_store_list_dirty_files(store, "dirty-ledger-repo", &dirty_rows,
                                         &dirty_row_count),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_row_count, 1);
    ASSERT_STR_EQ(dirty_rows[0].rel_path, "file.go");
    char expected_hash[CBM_FILE_CONTENT_HASH_BUFSZ] = "";
    ASSERT_EQ(cbm_file_content_hash(dirty_path, expected_hash, sizeof(expected_hash)), 0);
    ASSERT_STR_EQ(dirty_rows[0].observed_hash, expected_hash);
    struct stat dirty_stat;
    ASSERT_EQ(stat(dirty_path, &dirty_stat), 0);
    ASSERT_EQ(dirty_rows[0].observed_mtime_ns, cbm_stat_mtime_ns(&dirty_stat));
    ASSERT_EQ(dirty_rows[0].observed_size, dirty_stat.st_size);
    cbm_store_free_dirty_files(dirty_rows, dirty_row_count);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_dirty_ledger_records_paths_after_former_4096_limit) {
    enum { DIRTY_FILE_COUNT = CBM_SZ_4K + SKIP_ONE };
    char tmpdir[256];
    char abs_path[384];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_dirty_ledger_many_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    th_write_file(wt_path(abs_path, sizeof(abs_path), tmpdir, "seed.txt"), "seed\n");
    wt_git(tmpdir, "add seed.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, "dirty-ledger-many", tmpdir), CBM_STORE_OK);
    cbm_watcher_t *w = cbm_watcher_new(store, always_failing_index_callback, NULL);
    ASSERT_NOT_NULL(w);

    cbm_watcher_watch(w, "dirty-ledger-many", tmpdir);
    index_call_count = 0;
    ASSERT_EQ(cbm_watcher_poll_once(w), 0);
    ASSERT_EQ(index_call_count, 0);

    char rel_path[64];
    for (int i = 0; i < DIRTY_FILE_COUNT; i++) {
        snprintf(rel_path, sizeof(rel_path), "dirty-%04d.txt", i);
        th_write_file(wt_path(abs_path, sizeof(abs_path), tmpdir, rel_path), "x\n");
    }

    cbm_watcher_touch(w, "dirty-ledger-many");
    ASSERT_EQ(cbm_watcher_poll_once(w), 0);
    ASSERT_EQ(index_call_count, 1);

    int pending = -1;
    int overlay_ready = -1;
    ASSERT_EQ(cbm_store_count_dirty_files(store, "dirty-ledger-many", &pending,
                                          &overlay_ready),
              CBM_STORE_OK);
    ASSERT_EQ(pending, DIRTY_FILE_COUNT);
    ASSERT_EQ(overlay_ready, 0);

    cbm_dirty_file_state_t *dirty_rows = NULL;
    int dirty_row_count = 0;
    ASSERT_EQ(cbm_store_list_dirty_files(store, "dirty-ledger-many", &dirty_rows,
                                         &dirty_row_count),
              CBM_STORE_OK);
    ASSERT_EQ(dirty_row_count, DIRTY_FILE_COUNT);
    bool found_last = false;
    for (int i = 0; i < dirty_row_count; i++) {
        if (dirty_rows[i].rel_path &&
            strcmp(dirty_rows[i].rel_path, "dirty-4096.txt") == 0) {
            found_last = true;
            break;
        }
    }
    ASSERT_TRUE(found_last);
    cbm_store_free_dirty_files(dirty_rows, dirty_row_count);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_identical_watch_preserves_dirty_baseline) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_same_root_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "same-root-repo", tmpdir);
    index_call_count = 0;

    /* Establish the clean baseline before making the worktree dirty. */
    ASSERT_EQ(cbm_watcher_poll_once(w), 0);
    ASSERT_EQ(index_call_count, 0);

    {
        char p[300];
        th_append_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "modified\n");
    }

    cbm_watcher_touch(w, "same-root-repo");

    /* An identical registration must preserve the established baseline. */
    cbm_watcher_watch(w, "same-root-repo", tmpdir);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    ASSERT_EQ(cbm_watcher_poll_once(w), 1);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_detects_new_file) {
    /* Create a temporary git repo */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_newf_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "newf-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Add a new untracked file */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/newfile.go", tmpdir);
        th_write_file(_p, "new content\n");
    }

    /* Touch to bypass interval, then poll */
    cbm_watcher_touch(w, "newf-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* should detect untracked file */

    /* Cleanup */
    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_no_change_no_reindex) {
    /* Create a temporary git repo */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_nochg_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "nochg-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Poll multiple times with no changes — never triggers reindex */
    for (int i = 0; i < 5; i++) {
        cbm_watcher_touch(w, "nochg-repo");
        cbm_watcher_poll_once(w);
    }
    ASSERT_EQ(index_call_count, 0);

    /* Cleanup */
    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

/* #937: a PERSISTENTLY dirty worktree must reindex ONCE per distinct dirty
 * state, not on every poll. The watcher used to treat "tree is dirty" as
 * "tree changed", so an idle repo with one uncommitted file re-triggered a
 * full reindex (and its DB/artifact rewrite) every poll cycle — the reported
 * 1 TB/day write amplification. A dirty-state signature (porcelain entries +
 * per-file size/mtime) must gate the trigger: same signature → no reindex;
 * editing a dirty file again, or reverting the tree to clean, are NEW states
 * that must each trigger exactly one reindex. */
TEST(watcher_dirty_state_reindexes_once_issue937) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_amp_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "amp-repo", tmpdir);
    index_call_count = 0;

    /* Baseline (clean tree) */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Dirty the tree once (uncommitted modification). */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "modified\n");
    }
    cbm_watcher_touch(w, "amp-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* new dirty state → one reindex */

    /* Idle polls on the SAME dirty state must not re-trigger. */
    for (int i = 0; i < 3; i++) {
        cbm_watcher_touch(w, "amp-repo");
        cbm_watcher_poll_once(w);
    }
    ASSERT_EQ(index_call_count, 1); /* was 4 before the fix: one per poll */

    /* Editing the dirty file AGAIN is a new state (size changes). */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "modified again\n");
    }
    cbm_watcher_touch(w, "amp-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2);

    /* Same-state polls stay quiet again. */
    cbm_watcher_touch(w, "amp-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2);

    /* Reverting to a clean tree changes on-disk content (back to HEAD) —
     * that is a new state and must reindex exactly once. */
    wt_git(tmpdir, "checkout -- file.txt");
    cbm_watcher_touch(w, "amp-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 3);

    /* Stable clean tree: quiet. */
    cbm_watcher_touch(w, "amp-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 3);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

/* #937 companion: a change whose reindex FAILS (or is skipped busy) must be
 * retried on the next poll. The watcher used to commit the new HEAD at CHECK
 * time, so a commit observed while the pipeline was busy/failing was recorded
 * as seen and never indexed — a silent lost update. Baselines (HEAD and dirty
 * signature) may only be committed after a SUCCESSFUL reindex. */
static int failing_index_calls = 0;
static int failing_index_fail_first_n = 0;
static int failing_index_callback(const char *name, const char *path, void *ud) {
    (void)name;
    (void)path;
    (void)ud;
    failing_index_calls++;
    if (failing_index_calls <= failing_index_fail_first_n) {
        return -1; /* simulated pipeline failure */
    }
    return 0;
}

TEST(watcher_failed_reindex_retries_issue937) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_rty_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, failing_index_callback, NULL);

    cbm_watcher_watch(w, "rty-repo", tmpdir);
    failing_index_calls = 0;
    failing_index_fail_first_n = 1; /* first reindex attempt fails */

    /* Baseline (clean tree) */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(failing_index_calls, 0);

    /* HEAD moves (new commit). */
    {
        char p[300];
        th_append_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "world\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m add-world");

    /* First poll: change detected, reindex attempt FAILS. */
    cbm_watcher_touch(w, "rty-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(failing_index_calls, 1);

    /* The failed change must NOT have been recorded as seen: the next poll
     * retries and succeeds. Before the fix, the new HEAD was stored at check
     * time and the commit was silently lost (calls stayed at 1). */
    cbm_watcher_touch(w, "rty-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(failing_index_calls, 2);

    /* Successful reindex commits the baseline: no further triggers. */
    cbm_watcher_touch(w, "rty-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(failing_index_calls, 2);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_multiple_projects) {
    /* Create two temporary git repos */
    char tmpdirA[256];
    snprintf(tmpdirA, sizeof(tmpdirA), "/tmp/cbm_watcher_mA_XXXXXX");
    char tmpdirB[256];
    snprintf(tmpdirB, sizeof(tmpdirB), "/tmp/cbm_watcher_mB_XXXXXX");
    if (!cbm_mkdtemp(tmpdirA) || !cbm_mkdtemp(tmpdirB))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdirA, "init -q") != 0) {
        th_rmtree(tmpdirA);
        th_rmtree(tmpdirB);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdirA, "a.txt"), "a\n");
    }
    wt_git(tmpdirA, "add a.txt");
    wt_git(tmpdirA, "commit -q -m init");

    if (wt_git(tmpdirB, "init -q") != 0) {
        th_rmtree(tmpdirA);
        th_rmtree(tmpdirB);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdirB, "b.txt"), "b\n");
    }
    wt_git(tmpdirB, "add b.txt");
    wt_git(tmpdirB, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    cbm_watcher_watch(w, "projA", tmpdirA);
    cbm_watcher_watch(w, "projB", tmpdirB);
    ASSERT_EQ(cbm_watcher_watch_count(w), 2);
    index_call_count = 0;

    /* Baseline both */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Modify only A */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/a.txt", tmpdirA);
        th_append_file(_p, "modified\n");
    }

    /* Poll — only A should trigger */
    cbm_watcher_touch(w, "projA");
    cbm_watcher_touch(w, "projB");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* only A changed */

    /* Cleanup */
    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdirA);
    th_rmtree(tmpdirB);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  NON-GIT PROJECT
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_non_git_skips) {
    /* Non-git dir → baseline sets is_git=false → poll never reindexes.
     * Port of TestProbeStrategyNonGit behavior. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_nongit_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Create a file so it's not empty */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_write_file(_p, "hello\n");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "nongit", tmpdir);
    index_call_count = 0;

    /* Baseline — should detect non-git and set is_git=false */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Modify file */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "modified\n");
    }

    /* Touch + poll — should NOT trigger (non-git projects are skipped) */
    cbm_watcher_touch(w, "nongit");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Even add a new file — still no reindex */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/new.txt", tmpdir);
        th_write_file(_p, "new\n");
    }
    cbm_watcher_touch(w, "nongit");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  ADAPTIVE INTERVAL BEHAVIOR
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_interval_blocks_repoll) {
    /* After baseline, the adaptive interval (5s minimum) should block
     * immediate re-polling. Without touch(), the next poll is a no-op.
     * Port of TestWatcherGitNoChanges' interval behavior. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_intv_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "intv-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make repo dirty */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty\n");
    }

    /* Poll WITHOUT touch — interval should block checking */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0); /* blocked by interval */

    /* Now touch to bypass interval */
    cbm_watcher_touch(w, "intv-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* now detected */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_poll_interval_full_table) {
    /* Full table-driven test matching Go TestPollInterval exactly */
    struct {
        int files;
        int expected_ms;
    } tests[] = {
        {0, 5000},     {70, 5000},     {499, 5000},    {500, 6000},     {2000, 9000},
        {5000, 15000}, {10000, 25000}, {50000, 60000}, {100000, 60000},
    };
    int n = (int)(sizeof(tests) / sizeof(tests[0]));
    for (int i = 0; i < n; i++) {
        int got = cbm_watcher_poll_interval_ms(tests[i].files, 0, 0);
        if (got != tests[i].expected_ms) {
            fprintf(stderr, "FAIL pollInterval(%d) = %d, want %d\n", tests[i].files, got,
                    tests[i].expected_ms);
            return 1;
        }
    }
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  GIT REMOVAL + CONTINUED DIRTY + BASELINE DIRTY
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_git_removed_no_crash) {
    /* Init git repo, baseline, remove .git, poll → should not crash.
     * Port of TestStrategyDowngradeGitToDirMtime behavior (C version
     * doesn't downgrade — just git commands fail silently). */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_rmgit_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "rmgit-repo", tmpdir);
    index_call_count = 0;

    /* Baseline — detects git */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Remove .git — git commands will fail */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/.git", tmpdir);
        th_rmtree(_p);
    }

    /* Poll — should not crash, git_head() and git_is_dirty() fail gracefully */
    cbm_watcher_touch(w, "rmgit-repo");
    cbm_watcher_poll_once(w);
    /* No assertion on index_call_count — behavior is implementation-defined.
     * Main assertion: no crash, no ASan violation. */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_continued_dirty) {
    /* A tree that STAYS dirty re-triggers only when the dirty state itself
     * changes (#937): repeat polls over the untouched state are quiet, a
     * further edit re-triggers, and the cleaning commit triggers once more
     * (HEAD move + tree back to clean). Historically this test asserted one
     * reindex per poll while dirty — that WAS the #937 write amplification. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_cont_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "cont-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make dirty */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty\n");
    }

    /* First detection */
    cbm_watcher_touch(w, "cont-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* Still dirty but UNCHANGED — must stay quiet (#937) */
    cbm_watcher_touch(w, "cont-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* A further edit is a NEW dirty state — detect again */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirtier\n");
    }
    cbm_watcher_touch(w, "cont-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2);

    /* Commit to clean up, then poll — should not trigger */
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m clean");

    /* HEAD changed → will trigger one more reindex */
    cbm_watcher_touch(w, "cont-repo");
    cbm_watcher_poll_once(w);
    /* HEAD change from commit → reindex again (count = 3) */

    /* Now truly clean — no more reindexes */
    cbm_watcher_touch(w, "cont-repo");
    cbm_watcher_poll_once(w);
    int final_count = index_call_count;

    /* Touch and poll one more time to verify stability */
    cbm_watcher_touch(w, "cont-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, final_count); /* stable */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_baseline_dirty_repo) {
    /* #937: a dirty tree present before baseline is indexed once because a
     * restored artifact may not contain that state. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_bld_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file2.txt"), "world\n");
    }
    wt_git(tmpdir, "add file.txt file2.txt");
    wt_git(tmpdir, "commit -q -m init");

    /* Make dirty BEFORE baseline */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty from start\n");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "bld-repo", tmpdir);
    index_call_count = 0;

    /* Baseline captures HEAD but deliberately leaves dirty state pending. */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0); /* baseline never triggers */

    /* First real poll indexes the pre-existing dirty state once. */
    cbm_watcher_touch(w, "bld-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* A distinct dirty state is indexed again. */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file2.txt", tmpdir);
        th_append_file(_p, "dirty after baseline\n");
    }
    cbm_watcher_touch(w, "bld-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_unwatch_prunes_state) {
    /* Watch, baseline, unwatch → project state removed.
     * Port of TestPollAllPrunesUnwatched + TestWatcherPrunesDeletedProjects. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_prune_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "prune-repo", tmpdir);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    /* Unwatch — should remove project state immediately */
    cbm_watcher_unwatch(w, "prune-repo");
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    /* Make dirty + poll — nothing should happen */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty\n");
    }
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0); /* no projects to poll */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_watch_after_unwatch) {
    /* Re-watching after unwatch should start fresh (new baseline).
     * Tests lifecycle correctness. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_rewatch_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file2.txt"), "world\n");
    }
    wt_git(tmpdir, "add file.txt file2.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    /* Watch → baseline → unwatch */
    cbm_watcher_watch(w, "rewatch-repo", tmpdir);
    cbm_watcher_poll_once(w); /* baseline */
    cbm_watcher_unwatch(w, "rewatch-repo");
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    /* Make dirty while unwatched */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty\n");
    }

    /* Re-watch — needs fresh baseline */
    cbm_watcher_watch(w, "rewatch-repo", tmpdir);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    index_call_count = 0;

    /* Baseline again (first poll after re-watch) */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0); /* baseline never triggers */

    /* The dirty state appeared while unwatched, so the fresh baseline cannot
     * prove it is present in the DB. Index it once (#937); callers that just
     * completed an explicit index use cbm_watcher_mark_indexed() instead. */
    cbm_watcher_touch(w, "rewatch-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* The same dirty signature must then stay quiet (no write amplification). */
    cbm_watcher_touch(w, "rewatch-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* A later dirty-status change still triggers. */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file2.txt", tmpdir);
        th_append_file(_p, "dirty after rewatch baseline\n");
    }
    cbm_watcher_touch(w, "rewatch-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  FSNOTIFY PORTS (adapted for git-based change detection)
 *
 *  The Go watcher has fsnotify/dir-mtime strategies alongside git.
 *  The C watcher is git-only. These tests verify the same SEMANTIC
 *  behaviors (file create, delete, subdir, cleanup) through git.
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_detects_file_delete) {
    /* Port of TestFSNotifyDetectsFileDelete:
     * Delete a tracked file → git status shows change → reindex triggered. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_del_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "todelete.go"), "todelete\n");
    }
    wt_git(tmpdir, "add -A");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "del-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Delete tracked file → dirty worktree */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/todelete.go", tmpdir);
        cbm_unlink(_p);
    }

    /* Touch + poll → should detect deletion */
    cbm_watcher_touch(w, "del-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_detects_subdir_file) {
    /* Port of TestFSNotifyWatchesNewSubdir:
     * Create new subdir + file in it → git detects untracked → reindex. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_sub_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "main.go"), "hello\n");
    }
    wt_git(tmpdir, "add main.go");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "sub-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Create new subdir and file in it */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/pkg/lib.go", tmpdir);
        th_write_file(_p, "package pkg\n");
    }

    /* Touch + poll → should detect untracked file in subdir */
    cbm_watcher_touch(w, "sub-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_free_idempotent) {
    /* Port of TestFSNotifyCleanup:
     * Verify that free() properly cleans up, and free(NULL) is safe.
     * Tests resource cleanup correctness. */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(w);

    /* Watch some projects to create internal state */
    cbm_watcher_watch(w, "proj-a", "/tmp/a");
    cbm_watcher_watch(w, "proj-b", "/tmp/b");
    ASSERT_EQ(cbm_watcher_watch_count(w), 2);

    /* Free the watcher — should clean up all project state */
    cbm_watcher_free(w);

    /* Free(NULL) should be safe (already tested in null_safety,
     * but repeated here for parity with Go's close() test) */
    cbm_watcher_free(NULL);

    cbm_store_close(store);
    PASS();
}

TEST(watcher_full_flow_new_file) {
    /* Port of TestWatcherFSNotifyDetectsNewFile:
     * Full lifecycle: watch → baseline → add file → detect change.
     * This is a more thorough version of watcher_detects_new_file
     * that mirrors the Go test's structure exactly. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_ffnf_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "main.go"), "package main\n");
    }
    wt_git(tmpdir, "add main.go");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "ffnf-repo", tmpdir);
    index_call_count = 0;

    /* Baseline — sets up git strategy, captures HEAD */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Poll again immediately — should be blocked by interval */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Create a new file */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/util.go", tmpdir);
        th_write_file(_p, "package main\n");
    }

    /* Touch to bypass interval, then poll — should detect */
    cbm_watcher_touch(w, "ffnf-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_fallback_still_detects) {
    /* Port of TestFSNotifyFallbackToDirMtime:
     * Even when the "primary" strategy has issues, the watcher
     * still detects changes. In C, we test that after removing .git
     * and re-creating it, changes are still detected on re-watch. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_fb_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "main.go"), "hello\n");
    }
    wt_git(tmpdir, "add main.go");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "fb-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Remove .git and re-init (simulates strategy reset) */
    {
        char p[300];
        th_rmtree(wt_path(p, sizeof(p), tmpdir, ".git"));
    }
    wt_git(tmpdir, "init -q");
    wt_git(tmpdir, "add -A");
    wt_git(tmpdir, "commit -q -m reinit");

    /* Re-watch with fresh state */
    cbm_watcher_unwatch(w, "fb-repo");
    cbm_watcher_watch(w, "fb-repo", tmpdir);
    cbm_watcher_poll_once(w); /* new baseline */

    /* Add new file */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/new.go", tmpdir);
        th_write_file(_p, "package main\n");
    }

    /* Detect change with fresh git strategy */
    cbm_watcher_touch(w, "fb-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_poll_only_watched_projects) {
    /* Port of TestPollAllOnlyWatched:
     * Two repos exist, only one is watched → only the watched one
     * gets polled and can trigger reindex. */
    char tmpdirA[256];
    snprintf(tmpdirA, sizeof(tmpdirA), "/tmp/cbm_watcher_owA_XXXXXX");
    char tmpdirB[256];
    snprintf(tmpdirB, sizeof(tmpdirB), "/tmp/cbm_watcher_owB_XXXXXX");
    if (!cbm_mkdtemp(tmpdirA) || !cbm_mkdtemp(tmpdirB))
        FAIL("cbm_mkdtemp failed");

    /* Init both repos */
    if (wt_git(tmpdirA, "init -q") != 0) {
        th_rmtree(tmpdirA);
        th_rmtree(tmpdirB);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdirA, "a.txt"), "a\n");
    }
    wt_git(tmpdirA, "add a.txt");
    wt_git(tmpdirA, "commit -q -m init");

    if (wt_git(tmpdirB, "init -q") != 0) {
        th_rmtree(tmpdirA);
        th_rmtree(tmpdirB);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdirB, "b.txt"), "b\n");
    }
    wt_git(tmpdirB, "add b.txt");
    wt_git(tmpdirB, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    /* Only watch A — B is NOT watched */
    cbm_watcher_watch(w, "projA-ow", tmpdirA);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make BOTH repos dirty */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/a.txt", tmpdirA);
        th_append_file(_p, "dirty\n");
    }
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/b.txt", tmpdirB);
        th_append_file(_p, "dirty\n");
    }

    /* Poll — only A should trigger (B is not watched) */
    cbm_watcher_touch(w, "projA-ow");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdirA);
    th_rmtree(tmpdirB);
    PASS();
}

TEST(watcher_touch_resets_immediate) {
    /* Port of TestTouchProjectUpdatesTimestamp:
     * Verify that touch() resets the adaptive backoff so the next
     * poll actually checks for changes immediately. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_tch_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "tch-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make dirty */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty\n");
    }

    /* Without touch: interval blocks poll */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0); /* blocked */

    /* With touch: poll proceeds */
    cbm_watcher_touch(w, "tch-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* detected */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_modify_tracked_file) {
    /* Port of TestWatcherTriggersOnChange / TestWatcherGitDetectsEdit:
     * Modify tracked file content (not just create/delete) → detected.
     * Similar to watcher_detects_dirty_worktree but modifies specific
     * tracked file content rather than appending. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_mod_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "main.go"), "package main\n");
    }
    wt_git(tmpdir, "add main.go");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "mod-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* No-change poll */
    cbm_watcher_touch(w, "mod-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Overwrite file with new content */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/main.go", tmpdir);
        th_write_file(_p, "package main\n\nfunc main() {}\n");
    }

    /* Touch + poll → should detect modification */
    cbm_watcher_touch(w, "mod-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_dirty_hash_stable) {
    /* Core fix: after first reindex for a dirty tree, repeated polls with the
     * same dirty content must NOT retrigger (same porcelain hash). */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_dhs_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char file_path[512];
    if (wt_git(tmpdir, "init -q") != 0 ||
        th_write_file(wt_path(file_path, sizeof(file_path), tmpdir, "file.txt"), "hello\n") != 0 ||
        wt_git(tmpdir, "add file.txt") != 0 || wt_git(tmpdir, "commit -q -m init") != 0) {
        th_rmtree(tmpdir);
        FAIL("git fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "dhs-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make dirty */
    ASSERT_EQ(th_append_file(file_path, "dirty\n"), 0);

    /* First poll after edit → reindex */
    cbm_watcher_touch(w, "dhs-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* Three more polls with identical dirty content → no further reindexes */
    cbm_watcher_touch(w, "dhs-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_touch(w, "dhs-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_touch(w, "dhs-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* stable — hash-based dedup works */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_dirty_content_change_retriggered) {
    /* Fix 1 bidirectionality: after first dirty reindex, changing dirty content
     * (different porcelain output → different hash) MUST fire a second reindex.
     * This proves hash-based detection allows new changes, not just blocks all. */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_dcc_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char file_path[512];
    char file2_path[512];
    /* Commit two files so dirtying each produces a distinct porcelain output. */
    if (wt_git(tmpdir, "init -q") != 0 ||
        th_write_file(wt_path(file_path, sizeof(file_path), tmpdir, "file.txt"), "hello\n") != 0 ||
        th_write_file(wt_path(file2_path, sizeof(file2_path), tmpdir, "file2.txt"), "world\n") != 0 ||
        wt_git(tmpdir, "add file.txt file2.txt") != 0 ||
        wt_git(tmpdir, "commit -q -m init") != 0) {
        th_rmtree(tmpdir);
        FAIL("git fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "dcc-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* First edit — dirty file.txt only; porcelain = " M file.txt" */
    ASSERT_EQ(th_append_file(file_path, "edit-A\n"), 0);
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* first dirty detection */

    /* Same dirty state — no retrigger */
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* hash stable → no reindex */

    /* Second edit — also dirty file2.txt; porcelain now has two modified files
     * → different hash → must trigger a second reindex */
    ASSERT_EQ(th_append_file(file2_path, "edit-B\n"), 0);
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2); /* new dirty hash → second reindex */

    /* Same dirty state again — stable */
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2); /* stable */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_watch_path_change_resets_state) {
    /* Fix 2 correctness: re-watching same project with a DIFFERENT path must
     * replace state (new baseline), not return early. This verifies the path
     * comparison in cbm_watcher_watch() is working correctly. */
    char tmpdirA[256]; snprintf(tmpdirA, sizeof(tmpdirA), "/tmp/cbm_watcher_pca_XXXXXX");
    char tmpdirB[256]; snprintf(tmpdirB, sizeof(tmpdirB), "/tmp/cbm_watcher_pcb_XXXXXX");
    if (!cbm_mkdtemp(tmpdirA)) {
        FAIL("cbm_mkdtemp failed for fixture A");
    }
    if (!cbm_mkdtemp(tmpdirB)) {
        th_rmtree(tmpdirA);
        FAIL("cbm_mkdtemp failed for fixture B");
    }

    char path_a[512];
    char path_b[512];
    /* Init repo A. */
    if (wt_git(tmpdirA, "init -q") != 0 ||
        th_write_file(wt_path(path_a, sizeof(path_a), tmpdirA, "a.txt"), "repoA\n") != 0 ||
        wt_git(tmpdirA, "add a.txt") != 0 || wt_git(tmpdirA, "commit -q -m init-A") != 0) {
        th_rmtree(tmpdirA);
        th_rmtree(tmpdirB);
        FAIL("git fixture A setup failed");
    }
    /* Init repo B (already clean — nothing to detect after baseline) */
    if (wt_git(tmpdirB, "init -q") != 0 ||
        th_write_file(wt_path(path_b, sizeof(path_b), tmpdirB, "b.txt"), "repoB\n") != 0 ||
        wt_git(tmpdirB, "add b.txt") != 0 || wt_git(tmpdirB, "commit -q -m init-B") != 0) {
        th_rmtree(tmpdirA);
        th_rmtree(tmpdirB);
        FAIL("git fixture B setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    /* Watch project-X pointing at A */
    cbm_watcher_watch(w, "project-X", tmpdirA);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    index_call_count = 0;

    /* Baseline on A */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make A dirty and trigger reindex so state has accumulated head+hash */
    ASSERT_EQ(th_append_file(path_a, "dirty-A\n"), 0);
    cbm_watcher_touch(w, "project-X");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* Re-watch project-X with path B — must replace state, not return early */
    cbm_watcher_watch(w, "project-X", tmpdirB);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1); /* still 1 project */

    /* First poll on B → baseline (no reindex) */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* baseline never triggers */

    /* Second poll on B (clean) → no reindex */
    cbm_watcher_touch(w, "project-X");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* B is clean */

    /* Now dirty B → detect */
    ASSERT_EQ(th_append_file(path_b, "dirty-B\n"), 0);
    cbm_watcher_touch(w, "project-X");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2); /* B's dirty state detected */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdirA);
    th_rmtree(tmpdirB);
    PASS();
}

TEST(watcher_watch_idempotent) {
    /* Fix 2: calling cbm_watcher_watch() twice with same project+path must be
     * idempotent — state is preserved (no reset of baseline or dirty hash). */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_wid_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char file_path[512];
    if (wt_git(tmpdir, "init -q") != 0 ||
        th_write_file(wt_path(file_path, sizeof(file_path), tmpdir, "file.txt"), "hello\n") != 0 ||
        wt_git(tmpdir, "add file.txt") != 0 || wt_git(tmpdir, "commit -q -m init") != 0) {
        th_rmtree(tmpdir);
        FAIL("git fixture setup failed");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);

    /* First watch */
    cbm_watcher_watch(w, "wid-repo", tmpdir);
    index_call_count = 0;

    /* Baseline poll */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make dirty and trigger reindex */
    ASSERT_EQ(th_append_file(file_path, "dirty\n"), 0);
    cbm_watcher_touch(w, "wid-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* Re-watch same project+path (idempotent — must not reset state) */
    cbm_watcher_watch(w, "wid-repo", tmpdir);

    /* Poll with same dirty content — if state was reset, baseline re-runs then
     * dirty detection fires again (count would become 2). With the fix it stays 1. */
    cbm_watcher_touch(w, "wid-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* idempotent watch: state preserved */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_mark_indexed_refreshes_existing_baseline) {
    /* Explicit index_repository observes the current worktree. The watcher must
     * not reindex that same dirty status after the response, but later status
     * changes still need to trigger. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_mark_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) { th_rmtree(tmpdir); FAIL("git init failed"); }
    { char p[300]; th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n"); }
    { char p[300]; th_write_file(wt_path(p, sizeof(p), tmpdir, "file2.txt"), "world\n"); }
    wt_git(tmpdir, "add file.txt file2.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "mark-repo", tmpdir);
    index_call_count = 0;

    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty indexed explicitly\n");
    }
    cbm_watcher_mark_indexed(w, "mark-repo", tmpdir);
    cbm_watcher_touch(w, "mark-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file2.txt", tmpdir);
        th_append_file(_p, "dirty after explicit index\n");
    }
    cbm_watcher_touch(w, "mark-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  RESOURCE MANAGEMENT & AUTO-INDEXING BEHAVIOR
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_null_store_handling) {
    /* watcher_new with NULL store — verify behavior */
    cbm_watcher_t *w = cbm_watcher_new(NULL, NULL, NULL);
    /* Implementation may return NULL or a valid watcher.
     * Either is acceptable — key is no crash. */
    if (w) {
        ASSERT_EQ(cbm_watcher_watch_count(w), 0);
        cbm_watcher_free(w);
    }
    PASS();
}

TEST(watcher_free_null_safe) {
    /* Explicit test: free(NULL) must not crash */
    cbm_watcher_free(NULL);
    cbm_watcher_free(NULL);
    PASS();
}

TEST(watcher_empty_count) {
    /* Fresh watcher with no projects → count 0 */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);
    ASSERT_NOT_NULL(w);
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);
    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_watch_multiple_verify_count) {
    /* Watch 5 projects, verify count at each step */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    for (int i = 0; i < 5; i++) {
        char name[32], path[64];
        snprintf(name, sizeof(name), "proj-%d", i);
        snprintf(path, sizeof(path), "/tmp/proj-%d", i);
        cbm_watcher_watch(w, name, path);
        ASSERT_EQ(cbm_watcher_watch_count(w), i + 1);
    }

    /* Unwatch all */
    for (int i = 0; i < 5; i++) {
        char name[32];
        snprintf(name, sizeof(name), "proj-%d", i);
        cbm_watcher_unwatch(w, name);
    }
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_watch_same_project_idempotent) {
    /* Watching the same project twice updates the path, count stays 1 */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    cbm_watcher_watch(w, "proj", "/tmp/path-a");
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    cbm_watcher_watch(w, "proj", "/tmp/path-b");
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    cbm_watcher_watch(w, "proj", "/tmp/path-c");
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_unwatch_nonexistent_safe) {
    /* Unwatch a project that was never watched — no crash */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    cbm_watcher_unwatch(w, "never-existed");
    cbm_watcher_unwatch(w, "also-never-existed");
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_touch_nonexistent_project) {
    /* touch() on a project not in the watch list — no crash */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    cbm_watcher_touch(w, "nonexistent-project");
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_poll_interval_zero_files) {
    /* 0 files → base interval (5000ms) */
    int ms = cbm_watcher_poll_interval_ms(0, 0, 0);
    ASSERT_EQ(ms, 5000);
    PASS();
}

TEST(watcher_poll_interval_small_files) {
    /* 100 files → should be close to base (5000ms) */
    int ms = cbm_watcher_poll_interval_ms(100, 0, 0);
    ASSERT_GTE(ms, 5000);
    /* 100 files / 500 = 0 extra seconds of scaling → 5000ms */
    ASSERT_EQ(ms, 5000);
    PASS();
}

TEST(watcher_poll_interval_medium_files) {
    /* 10000 files → 5000 + 20*1000 = 25000ms */
    int ms = cbm_watcher_poll_interval_ms(10000, 0, 0);
    ASSERT_EQ(ms, 25000);
    PASS();
}

TEST(watcher_poll_interval_capped) {
    /* 100000 files → capped at 60000ms */
    int ms = cbm_watcher_poll_interval_ms(100000, 0, 0);
    ASSERT_EQ(ms, 60000);
    /* Even larger → still capped */
    ms = cbm_watcher_poll_interval_ms(500000, 0, 0);
    ASSERT_EQ(ms, 60000);
    PASS();
}

TEST(watcher_poll_interval_negative) {
    /* Negative file count → should handle gracefully (no crash) */
    int ms = cbm_watcher_poll_interval_ms(-1, 0, 0);
    /* Result should be at least the base interval or 0 — just no crash */
    ASSERT_GTE(ms, 0);
    PASS();
}

TEST(watcher_poll_empty_returns_zero) {
    /* poll_once with empty watch list → 0 reindexed */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    index_call_count = 0;

    int reindexed = cbm_watcher_poll_once(w);
    ASSERT_EQ(reindexed, 0);
    ASSERT_EQ(index_call_count, 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_poll_non_git_dir) {
    /* poll_once with a non-git directory → 0 changes detected */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_ng2_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /* Create a regular file so directory is not empty */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_write_file(_p, "hello\n");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "nongit2", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);

    /* Modify file */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "world\n");
    }

    /* Poll — non-git directory, should not trigger reindex */
    cbm_watcher_touch(w, "nongit2");
    int reindexed = cbm_watcher_poll_once(w);
    ASSERT_EQ(reindexed, 0);
    ASSERT_EQ(index_call_count, 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_stop_prevents_run) {
    /* Setting stop before run → run returns immediately */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    cbm_watcher_stop(w);
    int rc = cbm_watcher_run(w, 60000, 0);
    ASSERT_EQ(rc, 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(watcher_watch_unwatch_rapid_cycle) {
    /* Rapid watch/unwatch cycles — stress lifecycle management */
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    for (int i = 0; i < 20; i++) {
        cbm_watcher_watch(w, "rapid", "/tmp/rapid");
        ASSERT_EQ(cbm_watcher_watch_count(w), 1);
        cbm_watcher_unwatch(w, "rapid");
        ASSERT_EQ(cbm_watcher_watch_count(w), 0);
    }

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

/* Callback and state for watcher_callback_data_passed test */
static int g_cbdata_value = 42;
static int *g_cbdata_received = NULL;

static int capture_data_callback(const char *name, const char *path, void *ud) {
    (void)name;
    (void)path;
    g_cbdata_received = (int *)ud;
    return 0;
}

TEST(watcher_callback_data_passed) {
    /* Verify that user_data pointer is accessible in the callback */
    g_cbdata_received = NULL;

    /* Create a temp git repo */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_cbdata_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, capture_data_callback, &g_cbdata_value);
    cbm_watcher_watch(w, "cbdata-repo", tmpdir);

    /* Baseline */
    cbm_watcher_poll_once(w);

    /* Make dirty to trigger callback */
    {
        char _p[1024];
        snprintf(_p, sizeof(_p), "%s/file.txt", tmpdir);
        th_append_file(_p, "dirty\n");
    }

    cbm_watcher_touch(w, "cbdata-repo");
    cbm_watcher_poll_once(w);

    /* If callback was invoked, g_cbdata_received should point to g_cbdata_value */
    if (g_cbdata_received) {
        ASSERT_EQ(*g_cbdata_received, 42);
    }

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

TEST(watcher_unwatch_drains_pending_free) {
    /* Unwatch moves project_state to pending_free; the next poll_once
     * must drain it without crash or leak. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_df_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (wt_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "df-repo", tmpdir);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make dirty + detect change */
    {
        char p[300];
        th_append_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "dirty\n");
    }
    cbm_watcher_touch(w, "df-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    /* Unwatch — state moves to pending_free */
    cbm_watcher_unwatch(w, "df-repo");
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    /* Next poll drains pending_free — no crash, no double-free */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1);

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(tmpdir);
    PASS();
}

typedef struct {
    cbm_watcher_t *watcher;
    const char *first_project;
    const char *second_project;
    int calls;
} unwatch_snapshot_ctx_t;

static int unwatch_snapshot_callback(const char *name, const char *path, void *ud) {
    (void)name;
    (void)path;
    unwatch_snapshot_ctx_t *ctx = ud;
    ctx->calls++;
    if (ctx->calls == 1) {
        /* Both states are already present in poll_once's pointer snapshot.
         * Removing them must invalidate the not-yet-admitted callback. */
        cbm_watcher_unwatch(ctx->watcher, ctx->first_project);
        cbm_watcher_unwatch(ctx->watcher, ctx->second_project);
    }
    return 0;
}

TEST(watcher_unwatch_invalidates_remaining_poll_snapshot) {
    char first[256];
    char second[256];
    snprintf(first, sizeof(first), "/tmp/cbm_watcher_unwatch_snap_a_XXXXXX");
    snprintf(second, sizeof(second), "/tmp/cbm_watcher_unwatch_snap_b_XXXXXX");
    if (!cbm_mkdtemp(first) || !cbm_mkdtemp(second)) {
        th_rmtree(first);
        th_rmtree(second);
        FAIL("cbm_mkdtemp failed");
    }
    if (wt_git(first, "init -q") != 0 || wt_git(second, "init -q") != 0) {
        th_rmtree(first);
        th_rmtree(second);
        FAIL("git init failed");
    }
    char path[300];
    th_write_file(wt_path(path, sizeof(path), first, "file.txt"), "first\n");
    th_write_file(wt_path(path, sizeof(path), second, "file.txt"), "second\n");
    wt_git(first, "add file.txt");
    wt_git(first, "commit -q -m init");
    wt_git(second, "add file.txt");
    wt_git(second, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    unwatch_snapshot_ctx_t ctx = {
        .first_project = "unwatch-snapshot-a",
        .second_project = "unwatch-snapshot-b",
    };
    cbm_watcher_t *w = cbm_watcher_new(store, unwatch_snapshot_callback, &ctx);
    ctx.watcher = w;
    cbm_watcher_watch(w, ctx.first_project, first);
    cbm_watcher_watch(w, ctx.second_project, second);
    (void)cbm_watcher_poll_once(w);

    th_append_file(wt_path(path, sizeof(path), first, "file.txt"), "dirty\n");
    th_append_file(wt_path(path, sizeof(path), second, "file.txt"), "dirty\n");
    cbm_watcher_touch(w, ctx.first_project);
    cbm_watcher_touch(w, ctx.second_project);
    int reindexed = cbm_watcher_poll_once(w);
    int watches_after_callback = cbm_watcher_watch_count(w);
    (void)cbm_watcher_poll_once(w); /* drain the two deferred states */

    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(first);
    th_rmtree(second);

    ASSERT_EQ(ctx.calls, 1);
    ASSERT_EQ(reindexed, 1);
    ASSERT_EQ(watches_after_callback, 0);
    PASS();
}

typedef struct {
    cbm_watcher_t *watcher;
    const char *replacement_root;
    int calls;
} replace_during_poll_ctx_t;

static int replace_during_poll_callback(const char *name, const char *path, void *ud) {
    (void)path;
    replace_during_poll_ctx_t *ctx = ud;
    ctx->calls++;
    /* Re-registering can happen when a second daemon session initializes while
     * this project's watcher snapshot is being polled. The old state must stay
     * alive until the snapshot finishes using it. */
    cbm_watcher_watch(ctx->watcher, name, ctx->replacement_root);
    return 0;
}

TEST(watcher_replace_during_poll_defers_old_state_free) {
    char original[256];
    char replacement[256];
    snprintf(original, sizeof(original), "/tmp/cbm_watcher_replace_old_XXXXXX");
    snprintf(replacement, sizeof(replacement), "/tmp/cbm_watcher_replace_new_XXXXXX");
    if (!cbm_mkdtemp(original) || !cbm_mkdtemp(replacement)) {
        th_rmtree(original);
        th_rmtree(replacement);
        FAIL("cbm_mkdtemp failed");
    }
    if (wt_git(original, "init -q") != 0) {
        th_rmtree(original);
        th_rmtree(replacement);
        FAIL("git init failed");
    }
    {
        char p[300];
        th_write_file(wt_path(p, sizeof(p), original, "file.txt"), "hello\n");
    }
    wt_git(original, "add file.txt");
    wt_git(original, "commit -q -m init");

    cbm_store_t *store = cbm_store_open_memory();
    replace_during_poll_ctx_t ctx = {.replacement_root = replacement};
    cbm_watcher_t *w = cbm_watcher_new(store, replace_during_poll_callback, &ctx);
    ASSERT_NOT_NULL(w);
    ctx.watcher = w;
    cbm_watcher_watch(w, "replace-repo", original);

    cbm_watcher_poll_once(w); /* baseline */
    {
        char p[300];
        th_append_file(wt_path(p, sizeof(p), original, "file.txt"), "dirty\n");
    }
    cbm_watcher_touch(w, "replace-repo");
    ASSERT_EQ(cbm_watcher_poll_once(w), 1);
    ASSERT_EQ(ctx.calls, 1);
    ASSERT_EQ(cbm_watcher_watch_count(w), 1);

    /* Drains the deferred old snapshot state after the prior poll completed. */
    (void)cbm_watcher_poll_once(w);
    cbm_watcher_free(w);
    cbm_store_close(store);
    th_rmtree(original);
    th_rmtree(replacement);
    PASS();
}

TEST(watcher_null_poll_once) {
    /* poll_once(NULL) → 0 */
    int reindexed = cbm_watcher_poll_once(NULL);
    ASSERT_EQ(reindexed, 0);
    PASS();
}

TEST(watcher_null_watch_count) {
    /* watch_count(NULL) → 0 */
    int count = cbm_watcher_watch_count(NULL);
    ASSERT_EQ(count, 0);
    PASS();
}

/* ══════════════════════════════════════════════════════════════════
 *  SUITE
 * ══════════════════════════════════════════════════════════════════ */

SUITE(watcher) {
    /* Adaptive interval */
    RUN_TEST(poll_interval_base);
    RUN_TEST(poll_interval_scaling);
    RUN_TEST(poll_interval_cap);
    RUN_TEST(poll_interval_small);

    /* Lifecycle */
    RUN_TEST(watcher_create_free);
    RUN_TEST(watcher_watch_unwatch);
    RUN_TEST(watcher_unwatch_nonexistent);
    RUN_TEST(watcher_watch_replace);
    RUN_TEST(watcher_stopped_rejects_new_registration);
    RUN_TEST(watcher_null_safety);
    RUN_TEST(watcher_has_no_shell_command_buffer_path_limit);
    RUN_TEST(git_snapshot_has_no_shell_command_buffer_path_limit);
    RUN_TEST(git_snapshot_non_git_path);
    RUN_TEST(git_snapshot_clean_and_dirty_repo);
    RUN_TEST(git_status_paths_tracks_rename_current_and_previous_path);

    /* Polling */
    RUN_TEST(watcher_poll_no_projects);
    RUN_TEST(watcher_poll_nonexistent_path);
    RUN_TEST(watcher_prunes_sustained_missing_root);
    RUN_TEST(watcher_prune_waits_for_daemon_project_mutation);
    RUN_TEST(watcher_prune_guard_denial_and_success_are_balanced);
    RUN_TEST(watcher_prune_restats_root_after_guard_acquisition);
    RUN_TEST(watcher_prune_delete_failure_retains_watch_for_retry);
    RUN_TEST(watcher_grace_window_blocks_prune);
    RUN_TEST(watcher_root_missing_errno_classification);
    RUN_TEST(watcher_root_restore_resets_prune_streak);
    RUN_TEST(watcher_poll_this_repo);
    RUN_TEST(watcher_stop_flag);
    RUN_TEST(watcher_stop_wakes_parked_run_loop);
    RUN_TEST(watcher_stop_and_unwatch_cancel_blocked_git_without_backstop);

    /* Git change detection */
    RUN_TEST(watcher_detects_git_commit);
    RUN_TEST(watcher_detects_sha256_git_commit);
    RUN_TEST(watcher_detects_dirty_worktree);
    RUN_TEST(watcher_marks_dirty_file_before_failed_index_callback);
    RUN_TEST(watcher_dirty_ledger_records_paths_after_former_4096_limit);
    RUN_TEST(watcher_identical_watch_preserves_dirty_baseline);
    RUN_TEST(watcher_detects_new_file);
    RUN_TEST(watcher_no_change_no_reindex);
    RUN_TEST(watcher_dirty_state_reindexes_once_issue937);
    RUN_TEST(watcher_failed_reindex_retries_issue937);
    RUN_TEST(watcher_multiple_projects);

    /* Non-git project */
    RUN_TEST(watcher_non_git_skips);

    /* Adaptive interval behavior */
    RUN_TEST(watcher_interval_blocks_repoll);
    RUN_TEST(watcher_poll_interval_full_table);

    /* Git removal + continued dirty + baseline dirty */
    RUN_TEST(watcher_git_removed_no_crash);
    RUN_TEST(watcher_continued_dirty);
    RUN_TEST(watcher_baseline_dirty_repo);
    RUN_TEST(watcher_dirty_hash_stable);
    RUN_TEST(watcher_dirty_content_change_retriggered);
    RUN_TEST(watcher_watch_path_change_resets_state);
    RUN_TEST(watcher_watch_idempotent);
    RUN_TEST(watcher_mark_indexed_refreshes_existing_baseline);
    RUN_TEST(watcher_unwatch_prunes_state);
    RUN_TEST(watcher_watch_after_unwatch);
    RUN_TEST(watcher_unwatch_during_poll_callback_no_uaf);
    RUN_TEST(watcher_replace_during_poll_callback_no_uaf);

    /* FSNotify ports (adapted for git-based detection) */
    RUN_TEST(watcher_detects_file_delete);
    RUN_TEST(watcher_detects_subdir_file);
    RUN_TEST(watcher_free_idempotent);
    RUN_TEST(watcher_full_flow_new_file);
    RUN_TEST(watcher_fallback_still_detects);
    RUN_TEST(watcher_poll_only_watched_projects);
    RUN_TEST(watcher_touch_resets_immediate);
    RUN_TEST(watcher_modify_tracked_file);

    /* Resource management & auto-indexing behavior */
    RUN_TEST(watcher_null_store_handling);
    RUN_TEST(watcher_free_null_safe);
    RUN_TEST(watcher_empty_count);
    RUN_TEST(watcher_watch_multiple_verify_count);
    RUN_TEST(watcher_watch_same_project_idempotent);
    RUN_TEST(watcher_unwatch_nonexistent_safe);
    RUN_TEST(watcher_touch_nonexistent_project);
    /* Poll interval edge cases */
    RUN_TEST(watcher_poll_interval_zero_files);
    RUN_TEST(watcher_poll_interval_small_files);
    RUN_TEST(watcher_poll_interval_medium_files);
    RUN_TEST(watcher_poll_interval_capped);
    RUN_TEST(watcher_poll_interval_negative);
    /* Poll edge cases */
    RUN_TEST(watcher_poll_empty_returns_zero);
    RUN_TEST(watcher_poll_non_git_dir);
    RUN_TEST(watcher_stop_prevents_run);
    RUN_TEST(watcher_watch_unwatch_rapid_cycle);
    RUN_TEST(watcher_unwatch_drains_pending_free);
    RUN_TEST(watcher_unwatch_invalidates_remaining_poll_snapshot);
    RUN_TEST(watcher_replace_during_poll_defers_old_state_free);
    RUN_TEST(watcher_callback_data_passed);
    RUN_TEST(watcher_null_poll_once);
    RUN_TEST(watcher_null_watch_count);
}
