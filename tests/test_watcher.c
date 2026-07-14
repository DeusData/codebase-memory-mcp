/*
 * test_watcher.c — Tests for the file change watcher module.
 *
 * Covers: adaptive interval, watch/unwatch lifecycle, git change detection,
 * poll_once behavior.
 */
#include "../src/foundation/compat.h"
#include "../src/foundation/constants.h"
#include "../src/foundation/platform.h"
#include "test_framework.h"
#include "test_helpers.h"
#include <git/git_snapshot.h>
#include <watcher/watcher.h>
#include <store/store.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

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

TEST(watcher_rejects_overlong_git_command_path) {
    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, NULL, NULL);

    char long_path[CBM_SZ_2K];
    long_path[0] = '/';
    memset(long_path + 1, 'a', sizeof(long_path) - CBM_SZ_2);
    long_path[sizeof(long_path) - 1] = '\0';

    cbm_watcher_watch(w, "too-long", long_path);
    ASSERT_EQ(cbm_watcher_watch_count(w), 0);

    cbm_watcher_free(w);
    cbm_store_close(store);
    PASS();
}

TEST(git_snapshot_rejects_overlong_path) {
    char long_path[CBM_SZ_2K];
    long_path[0] = '/';
    memset(long_path + 1, 'b', sizeof(long_path) - CBM_SZ_2);
    long_path[sizeof(long_path) - 1] = '\0';

    cbm_git_snapshot_t snap = {0};
    ASSERT_FALSE(cbm_git_snapshot_path_supported(long_path));
    ASSERT_EQ(cbm_git_snapshot_read(long_path, CBM_GIT_SNAPSHOT_HEAD, &snap), CBM_NOT_FOUND);
    ASSERT_FALSE(snap.path_supported);
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

/* ══════════════════════════════════════════════════════════════════
 *  GIT CHANGE DETECTION (with temp repo)
 * ══════════════════════════════════════════════════════════════════ */

TEST(watcher_detects_git_commit) {
    /* Create a temporary git repo */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_test_XXXXXX");
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

    cbm_watcher_watch(w, "temp-repo", tmpdir);
    index_call_count = 0;

    /* First poll: baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make a change: new commit */
    {
        char p[300];
        th_append_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "world\n");
    }
    wt_git(tmpdir, "add file.txt");
    wt_git(tmpdir, "commit -q -m add-world");

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

    {
        char p[300];
        th_append_file(wt_path(p, sizeof(p), tmpdir, "file.go"), "\nfunc NewDirty() {}\n");
    }

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
        SKIP("cbm_mkdtemp failed");

    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && git init -q && git config user.email test@test && "
             "git config user.name test && echo 'hello' > file.txt && "
             "git add file.txt && git commit -q -m 'init'",
             tmpdir);
    if (system(cmd) != 0) {
        snprintf(cmd, sizeof(cmd), "rm -rf '%s'", tmpdir);
        system(cmd);
        SKIP("git not available");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "dhs-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* Make dirty */
    snprintf(cmd, sizeof(cmd), "echo 'dirty' >> '%s/file.txt'", tmpdir);
    system(cmd);

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
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", tmpdir);
    system(cmd);
    PASS();
}

TEST(watcher_dirty_content_change_retriggered) {
    /* Fix 1 bidirectionality: after first dirty reindex, changing dirty content
     * (different porcelain output → different hash) MUST fire a second reindex.
     * This proves hash-based detection allows new changes, not just blocks all. */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_dcc_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        SKIP("cbm_mkdtemp failed");

    char cmd[512];
    /* Commit two files so dirtying each produces a distinct porcelain output */
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && git init -q && git config user.email test@test && "
             "git config user.name test && echo 'hello' > file.txt && "
             "echo 'world' > file2.txt && "
             "git add file.txt file2.txt && git commit -q -m 'init'",
             tmpdir);
    if (system(cmd) != 0) {
        snprintf(cmd, sizeof(cmd), "rm -rf '%s'", tmpdir);
        system(cmd);
        SKIP("git not available");
    }

    cbm_store_t *store = cbm_store_open_memory();
    cbm_watcher_t *w = cbm_watcher_new(store, index_callback, NULL);
    cbm_watcher_watch(w, "dcc-repo", tmpdir);
    index_call_count = 0;

    /* Baseline */
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 0);

    /* First edit — dirty file.txt only; porcelain = " M file.txt" */
    snprintf(cmd, sizeof(cmd), "echo 'edit-A' >> '%s/file.txt'", tmpdir);
    system(cmd);
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* first dirty detection */

    /* Same dirty state — no retrigger */
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 1); /* hash stable → no reindex */

    /* Second edit — also dirty file2.txt; porcelain now has two modified files
     * → different hash → must trigger a second reindex */
    snprintf(cmd, sizeof(cmd), "echo 'edit-B' >> '%s/file2.txt'", tmpdir);
    system(cmd);
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2); /* new dirty hash → second reindex */

    /* Same dirty state again — stable */
    cbm_watcher_touch(w, "dcc-repo");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2); /* stable */

    cbm_watcher_free(w);
    cbm_store_close(store);
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", tmpdir);
    system(cmd);
    PASS();
}

TEST(watcher_watch_path_change_resets_state) {
    /* Fix 2 correctness: re-watching same project with a DIFFERENT path must
     * replace state (new baseline), not return early. This verifies the path
     * comparison in cbm_watcher_watch() is working correctly. */
    char tmpdirA[256]; snprintf(tmpdirA, sizeof(tmpdirA), "/tmp/cbm_watcher_pca_XXXXXX");
    char tmpdirB[256]; snprintf(tmpdirB, sizeof(tmpdirB), "/tmp/cbm_watcher_pcb_XXXXXX");
    if (!cbm_mkdtemp(tmpdirA) || !cbm_mkdtemp(tmpdirB))
        SKIP("cbm_mkdtemp failed");

    char cmd[512];
    /* Init repo A */
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && git init -q && git config user.email test@test && "
             "git config user.name test && echo 'repoA' > a.txt && "
             "git add a.txt && git commit -q -m 'init-A'",
             tmpdirA);
    if (system(cmd) != 0) {
        SKIP("git not available");
    }
    /* Init repo B (already clean — nothing to detect after baseline) */
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && git init -q && git config user.email test@test && "
             "git config user.name test && echo 'repoB' > b.txt && "
             "git add b.txt && git commit -q -m 'init-B'",
             tmpdirB);
    if (system(cmd) != 0) {
        SKIP("git not available");
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
    snprintf(cmd, sizeof(cmd), "echo 'dirty-A' >> '%s/a.txt'", tmpdirA);
    system(cmd);
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
    snprintf(cmd, sizeof(cmd), "echo 'dirty-B' >> '%s/b.txt'", tmpdirB);
    system(cmd);
    cbm_watcher_touch(w, "project-X");
    cbm_watcher_poll_once(w);
    ASSERT_EQ(index_call_count, 2); /* B's dirty state detected */

    cbm_watcher_free(w);
    cbm_store_close(store);
    snprintf(cmd, sizeof(cmd), "rm -rf '%s' '%s'", tmpdirA, tmpdirB);
    system(cmd);
    PASS();
}

TEST(watcher_watch_idempotent) {
    /* Fix 2: calling cbm_watcher_watch() twice with same project+path must be
     * idempotent — state is preserved (no reset of baseline or dirty hash). */
    char tmpdir[256]; snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_wid_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        SKIP("cbm_mkdtemp failed");

    char cmd[512];
    snprintf(cmd, sizeof(cmd),
             "cd '%s' && git init -q && git config user.email test@test && "
             "git config user.name test && echo 'hello' > file.txt && "
             "git add file.txt && git commit -q -m 'init'",
             tmpdir);
    if (system(cmd) != 0) {
        snprintf(cmd, sizeof(cmd), "rm -rf '%s'", tmpdir);
        system(cmd);
        SKIP("git not available");
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
    snprintf(cmd, sizeof(cmd), "echo 'dirty' >> '%s/file.txt'", tmpdir);
    system(cmd);
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
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", tmpdir);
    system(cmd);
    PASS();
}

TEST(watcher_mark_indexed_refreshes_existing_baseline) {
    /* Explicit index_repository observes the current worktree. The watcher must
     * not reindex that same dirty status after the response, but later status
     * changes still need to trigger. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_watcher_mark_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        SKIP("cbm_mkdtemp failed");

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

    if (wt_git(tmpdir, "init -q") != 0) { th_rmtree(tmpdir); FAIL("git init failed"); }
    { char p[300]; th_write_file(wt_path(p, sizeof(p), tmpdir, "file.txt"), "hello\n"); }
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
    RUN_TEST(watcher_null_safety);
    RUN_TEST(watcher_rejects_overlong_git_command_path);
    RUN_TEST(git_snapshot_rejects_overlong_path);
    RUN_TEST(git_snapshot_non_git_path);
    RUN_TEST(git_snapshot_clean_and_dirty_repo);
    RUN_TEST(git_status_paths_tracks_rename_current_and_previous_path);

    /* Polling */
    RUN_TEST(watcher_poll_no_projects);
    RUN_TEST(watcher_poll_nonexistent_path);
    RUN_TEST(watcher_prunes_sustained_missing_root);
    RUN_TEST(watcher_grace_window_blocks_prune);
    RUN_TEST(watcher_root_missing_errno_classification);
    RUN_TEST(watcher_root_restore_resets_prune_streak);
    RUN_TEST(watcher_poll_this_repo);
    RUN_TEST(watcher_stop_flag);

    /* Git change detection */
    RUN_TEST(watcher_detects_git_commit);
    RUN_TEST(watcher_detects_dirty_worktree);
    RUN_TEST(watcher_marks_dirty_file_before_failed_index_callback);
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
    RUN_TEST(watcher_callback_data_passed);
    RUN_TEST(watcher_null_poll_once);
    RUN_TEST(watcher_null_watch_count);
}
