/*
 * test_project_resolve.c — Canonical project identity and duplicate-index prevention.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/project_resolve.h"
#include "pipeline/pipeline.h"
#include "git/git_context.h"
#include <store/store.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static const char *saved_cache_dir;

static void push_cache_dir(const char *cache) {
    saved_cache_dir = getenv("CBM_CACHE_DIR");
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    cbm_project_identity_cache_invalidate();
}

static void pop_cache_dir(void) {
    if (saved_cache_dir) {
        cbm_setenv("CBM_CACHE_DIR", saved_cache_dir, 1);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
    cbm_project_identity_cache_invalidate();
    saved_cache_dir = NULL;
}

static int seed_project_db(const char *cache, const char *project, const char *root) {
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", cache, project);
    cbm_store_t *store = cbm_store_open_path(db_path);
    if (!store) {
        return -1;
    }
    int rc = cbm_store_upsert_project(store, project, root);
    cbm_store_close(store);
    return rc == CBM_STORE_OK ? 0 : -1;
}

static bool git_available(void) {
#ifdef _WIN32
    return system("git --version >NUL 2>&1") == 0;
#else
    return system("git --version >/dev/null 2>&1") == 0;
#endif
}

static int run_cmd(const char *cmd) {
    return system(cmd);
}

TEST(project_resolve_path_canonicalize) {
    char *tmpdir = th_mktempdir("cbm-projres");
    if (!tmpdir) {
        FAIL("th_mktempdir failed");
    }

    const char *file = TH_PATH(tmpdir, "readme.txt");
    ASSERT_EQ(th_write_file(file, "x"), 0);

    char canon[1024];
    ASSERT_TRUE(cbm_path_canonicalize(file, canon, sizeof(canon)));
    ASSERT(strstr(canon, "readme.txt") != NULL);

    th_cleanup(tmpdir);
    PASS();
}

TEST(project_resolve_identity_key_stable) {
    char key1[1024];
    char key2[1024];
    ASSERT_TRUE(cbm_project_identity_key("/tmp/foo/bar", key1, sizeof(key1)));
    ASSERT_TRUE(cbm_project_identity_key("/tmp/foo/bar/", key2, sizeof(key2)));
    ASSERT_STR_EQ(key1, key2);
    PASS();
}

TEST(project_resolve_find_existing_by_root_path) {
    char *cache = th_mktempdir("cbm-projres-cache");
    if (!cache) {
        FAIL("th_mktempdir failed");
    }

    const char *root = TH_PATH(cache, "repo-root");
    ASSERT_EQ(th_mkdir_p(root), 0);

    push_cache_dir(cache);
    ASSERT_EQ(seed_project_db(cache, "indexed-project", root), 0);

    char *found = cbm_find_existing_project_name(root);
    pop_cache_dir();

    ASSERT_NOT_NULL(found);
    ASSERT_STR_EQ(found, "indexed-project");
    free(found);

    th_cleanup(cache);
    PASS();
}

TEST(project_resolve_pipeline_reuses_existing_name) {
    char *cache = th_mktempdir("cbm-projres-pl");
    if (!cache) {
        FAIL("th_mktempdir failed");
    }

    const char *root = TH_PATH(cache, "worktree");
    ASSERT_EQ(th_mkdir_p(root), 0);

    push_cache_dir(cache);
    ASSERT_EQ(seed_project_db(cache, "canonical-name", root), 0);

    cbm_pipeline_t *p = cbm_pipeline_new(root, NULL, CBM_MODE_FAST);
    pop_cache_dir();

    ASSERT_NOT_NULL(p);
    ASSERT_STR_EQ(cbm_pipeline_project_name(p), "canonical-name");
    cbm_pipeline_free(p);

    th_cleanup(cache);
    PASS();
}

TEST(project_resolve_worktrees_distinct) {
    if (!git_available()) {
        FAIL("git unavailable");
    }

    char *tmp = th_mktempdir("cbm-projres-wt");
    if (!tmp) {
        FAIL("th_mktempdir failed");
    }

    const char *repo = TH_PATH(tmp, "repo");
    const char *wt = TH_PATH(tmp, "wt");
    ASSERT_EQ(th_mkdir_p(repo), 0);

#ifdef _WIN32
    const char *null_dev = "NUL";
#else
    const char *null_dev = "/dev/null";
#endif

    char cmd[2048];
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" init >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" config user.email \"t@t.com\" >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" config user.name \"t\" >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" commit --allow-empty -m init >%s 2>&1", repo, null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" worktree add -b feature/wt \"%s\" >%s 2>&1", repo, wt,
             null_dev);
    ASSERT_EQ(run_cmd(cmd), 0);

    cbm_git_context_t main_ctx = {0};
    cbm_git_context_t wt_ctx = {0};
    ASSERT_EQ(cbm_git_context_resolve(repo, &main_ctx), 0);
    ASSERT_EQ(cbm_git_context_resolve(wt, &wt_ctx), 0);
    ASSERT_STR_EQ(main_ctx.canonical_root, wt_ctx.canonical_root);

    char main_key[4096];
    char wt_key[4096];
    ASSERT_TRUE(cbm_project_identity_key(repo, main_key, sizeof(main_key)));
    ASSERT_TRUE(cbm_project_identity_key(wt, wt_key, sizeof(wt_key)));
    ASSERT_STR_NEQ(main_key, wt_key);

    cbm_git_context_free(&main_ctx);
    cbm_git_context_free(&wt_ctx);
    th_cleanup(tmp);
    PASS();
}

SUITE(project_resolve) {
    RUN_TEST(project_resolve_path_canonicalize);
    RUN_TEST(project_resolve_identity_key_stable);
    RUN_TEST(project_resolve_find_existing_by_root_path);
    RUN_TEST(project_resolve_pipeline_reuses_existing_name);
    RUN_TEST(project_resolve_worktrees_distinct);
}
