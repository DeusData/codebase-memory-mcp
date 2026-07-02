/*
 * test_project_resolve.c — Canonical project identity and duplicate-index prevention.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "pipeline/project_resolve.h"
#include "pipeline/pipeline.h"
#include <store/store.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

typedef struct {
    const char *cache;
    const char *project;
    const char *root;
} seed_ctx_t;

typedef struct {
    const char *query_root;
    char **found;
} find_ctx_t;

typedef struct {
    const char *root;
    cbm_pipeline_t **pipeline;
} pipeline_ctx_t;

static void with_cache_dir(const char *cache, void (*fn)(void *), void *ctx) {
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    cbm_setenv("CBM_CACHE_DIR", cache, 1);
    fn(ctx);
    if (saved_copy) {
        cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
        free(saved_copy);
    } else {
        cbm_unsetenv("CBM_CACHE_DIR");
    }
}

static void seed_project_db(void *vctx) {
    seed_ctx_t *ctx = (seed_ctx_t *)vctx;
    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/%s.db", ctx->cache, ctx->project);
    cbm_store_t *store = cbm_store_open_path(db_path);
    ASSERT_NOT_NULL(store);
    ASSERT_EQ(cbm_store_upsert_project(store, ctx->project, ctx->root), CBM_STORE_OK);
    cbm_store_close(store);
}

static void find_existing_project(void *vctx) {
    find_ctx_t *ctx = (find_ctx_t *)vctx;
    *(ctx->found) = cbm_find_existing_project_name(ctx->query_root);
}

static void open_pipeline_for_root(void *vctx) {
    pipeline_ctx_t *ctx = (pipeline_ctx_t *)vctx;
    *(ctx->pipeline) = cbm_pipeline_new(ctx->root, NULL, CBM_MODE_FAST);
}

TEST(project_resolve_path_canonicalize) {
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm-projres-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    char file[512];
    snprintf(file, sizeof(file), "%s/readme.txt", tmpdir);
    th_write_file(file, "x");

    char canon[1024];
    ASSERT_TRUE(cbm_path_canonicalize(file, canon, sizeof(canon)));
    ASSERT(strstr(canon, "readme.txt") != NULL);

    test_rmdir_r(tmpdir);
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
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-projres-cache-XXXXXX");
    if (!cbm_mkdtemp(cache))
        FAIL("cbm_mkdtemp failed");

    char root[512];
    snprintf(root, sizeof(root), "%s/repo-root", cache);
    test_mkdirp(root);

    seed_ctx_t seed = {.cache = cache, .project = "indexed-project", .root = root};
    with_cache_dir(cache, seed_project_db, &seed);

    char *found = NULL;
    find_ctx_t fctx = {.query_root = root, .found = &found};
    with_cache_dir(cache, find_existing_project, &fctx);

    ASSERT_NOT_NULL(found);
    ASSERT_STR_EQ(found, "indexed-project");
    free(found);

    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/indexed-project.db", cache);
    cbm_unlink(db_path);
    test_rmdir_r(root);
    cbm_rmdir(cache);
    PASS();
}

TEST(project_resolve_pipeline_reuses_existing_name) {
    char cache[256];
    snprintf(cache, sizeof(cache), "/tmp/cbm-projres-pl-XXXXXX");
    if (!cbm_mkdtemp(cache))
        FAIL("cbm_mkdtemp failed");

    char root[512];
    snprintf(root, sizeof(root), "%s/worktree", cache);
    test_mkdirp(root);

    seed_ctx_t seed = {.cache = cache, .project = "canonical-name", .root = root};
    with_cache_dir(cache, seed_project_db, &seed);

    cbm_pipeline_t *p = NULL;
    pipeline_ctx_t pctx = {.root = root, .pipeline = &p};
    with_cache_dir(cache, open_pipeline_for_root, &pctx);

    ASSERT_NOT_NULL(p);
    ASSERT_STR_EQ(cbm_pipeline_project_name(p), "canonical-name");
    cbm_pipeline_free(p);

    char db_path[1024];
    snprintf(db_path, sizeof(db_path), "%s/canonical-name.db", cache);
    cbm_unlink(db_path);
    test_rmdir_r(root);
    cbm_rmdir(cache);
    PASS();
}

SUITE(project_resolve) {
    RUN_TEST(project_resolve_path_canonicalize);
    RUN_TEST(project_resolve_identity_key_stable);
    RUN_TEST(project_resolve_find_existing_by_root_path);
    RUN_TEST(project_resolve_pipeline_reuses_existing_name);
}
