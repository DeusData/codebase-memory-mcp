/*
 * test_git_context.c — Tests for cbm_git_context_resolve(), focusing on
 * the canonical_root derivation for git worktrees and subdirectory projects.
 *
 * Issue #659: canonical_root was computed incorrectly for linked worktrees
 * and projects indexed from a subdirectory of the repository root.
 * git rev-parse --git-common-dir outputs a path relative to the -C directory
 * (input_path), not to worktree_root. Joining it with worktree_root and then
 * string-stripping "/.git" left unresolved ".." components in the result.
 *
 * These tests shell out to `git`, so they SKIP_PLATFORM on Windows (the CI
 * shell there cannot init a repo via system()).
 *
 * Reproduce-first guard: canonical_root_subdir is the genuine RED-without-the-fix
 * guard — a repo indexed from a subdirectory yields a relative --git-common-dir
 * ("../.git"), so the unfixed code returns an un-normalized "<root>/subdir/.."
 * (verified FAIL on the unfixed derive_canonical_root; GREEN with the realpath
 * normalization). canonical_root_linked_worktree is a SUPPORTING INVARIANT, not
 * the #659 reproducer: on git that emits an *absolute* --git-common-dir for a
 * linked worktree (e.g. 2.48.x) the bug does not manifest there, so that test
 * passes with or without the fix. It still enforces the worktree->main-root
 * invariant and would catch the bug on git builds that emit a relative
 * worktree common-dir. canonical_root_repo_root is a baseline (no `..` to
 * normalize), not a guard.
 */
#include "test_framework.h"
#include "test_helpers.h"
#include "git/git_context.h"
#include "foundation/compat.h"
#include "foundation/git_env.h"
#include "pipeline/pipeline_internal.h"

#include <stdlib.h>

#include <stdio.h>
#include <string.h>

#ifndef _WIN32
#include <limits.h>
#endif

/* These helpers shell out to git and are only used by the non-Windows test
 * bodies below; on Windows every test SKIP_PLATFORMs, so guard them here too or
 * they'd be unused-static functions and fail the -Werror build. */
#ifndef _WIN32
/* Run a git command inside dir, return 0 on success. */
static int git_run(const char *dir, const char *args) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" %s >/dev/null 2>&1", dir, args);
    return system(cmd);
}

/* Create a minimal git repo at dir (init + empty commit so HEAD exists). */
static int make_git_repo(const char *dir) {
    if (th_mkdir_p(dir) != 0) return -1;
    if (git_run(dir, "init -q") != 0) return -1;
    if (git_run(dir, "config user.email test@example.com") != 0) return -1;
    if (git_run(dir, "config user.name Test") != 0) return -1;
    /* Create a file so HEAD points to a real commit. */
    char path[1024];
    snprintf(path, sizeof(path), "%s/.keep", dir);
    th_write_file(path, "");
    if (git_run(dir, "add .keep") != 0) return -1;
    if (git_run(dir, "commit -q -m init") != 0) return -1;
    return 0;
}
#endif /* _WIN32 */

#ifndef _WIN32
/* ── Inherited repository-local git environment (#2003) ────────────
 * A cbm started from a git hook (or an editor that exports GIT_DIR) inherits
 * GIT_DIR/GIT_WORK_TREE/GIT_INDEX_FILE/..., which override `git -C <dir>`.
 * Production must clear them for its own git children (per spawn). These
 * tests set the hook set explicitly (the runner may clear it at startup) and
 * restore the caller's values before asserting, so a failure never leaks the
 * decoy environment into later tests. */

typedef struct {
    char *saved[CBM_GIT_REPO_ENV_VAR_COUNT];
    bool present[CBM_GIT_REPO_ENV_VAR_COUNT];
} git_env_snapshot_t;

static void git_env_hook_enter(git_env_snapshot_t *snap, const char *decoy) {
    for (int i = 0; i < CBM_GIT_REPO_ENV_VAR_COUNT; i++) {
        const char *v = getenv(cbm_git_repo_env_vars[i]);
        snap->present[i] = v != NULL;
        snap->saved[i] = v ? strdup(v) : NULL;
    }
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s/.git", decoy);
    cbm_setenv("GIT_DIR", buf, 1);
    cbm_setenv("GIT_COMMON_DIR", buf, 1);
    cbm_setenv("GIT_WORK_TREE", decoy, 1);
    snprintf(buf, sizeof(buf), "%s/.git/index", decoy);
    cbm_setenv("GIT_INDEX_FILE", buf, 1);
    snprintf(buf, sizeof(buf), "%s/.git/objects", decoy);
    cbm_setenv("GIT_OBJECT_DIRECTORY", buf, 1);
    cbm_setenv("GIT_PREFIX", "", 1);
}

static void git_env_hook_leave(git_env_snapshot_t *snap) {
    for (int i = 0; i < CBM_GIT_REPO_ENV_VAR_COUNT; i++) {
        if (snap->present[i]) {
            cbm_setenv(cbm_git_repo_env_vars[i], snap->saved[i], 1);
        } else {
            cbm_unsetenv(cbm_git_repo_env_vars[i]);
        }
        free(snap->saved[i]);
        snap->saved[i] = NULL;
    }
}

/* First output line of `git -C dir <args>` (run with the test's clean env). */
static void git_read_line(const char *dir, const char *args, char *out, size_t out_sz) {
    char cmd[1024];
    out[0] = '\0';
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" %s 2>/dev/null", dir, args);
    FILE *fp = popen(cmd, "r");
    if (!fp)
        return;
    if (fgets(out, (int)out_sz, fp)) {
        out[strcspn(out, "\r\n")] = '\0';
    }
    pclose(fp);
}

/* root/decoy: a repo on branch decoy-branch with coupled a.go+b.go commits;
 * root/fixture: a repo on branch fixture-branch; root/plain: not a repo. */
static int make_env_isolation_tree(const char *root) {
    char decoy[512], fixture[512], plain[512], path[600];
    snprintf(decoy, sizeof(decoy), "%s/decoy", root);
    snprintf(fixture, sizeof(fixture), "%s/fixture", root);
    snprintf(plain, sizeof(plain), "%s/plain", root);
    if (make_git_repo(decoy) != 0 || make_git_repo(fixture) != 0)
        return -1;
    if (git_run(decoy, "checkout -q -b decoy-branch") != 0)
        return -1;
    if (git_run(fixture, "checkout -q -b fixture-branch") != 0)
        return -1;
    for (int i = 0; i < 3; i++) {
        char content[32];
        snprintf(content, sizeof(content), "package a // %d\n", i);
        snprintf(path, sizeof(path), "%s/a.go", decoy);
        th_write_file(path, content);
        snprintf(path, sizeof(path), "%s/b.go", decoy);
        th_write_file(path, content);
        if (git_run(decoy, "add a.go b.go") != 0)
            return -1;
        if (git_run(decoy, "commit -q -m decoy") != 0)
            return -1;
    }
    return th_mkdir_p(plain);
}

#endif /* _WIN32 */

/* ── canonical_root: normal repo indexed from its root ──────────── */

TEST(canonical_root_repo_root) {
#ifdef _WIN32
    SKIP_PLATFORM("git-based canonical_root test not supported on Windows CI");
#else
    char *tmp = th_mktempdir("cbm_gitctx");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (make_git_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }

    cbm_git_context_t ctx = {0};
    int rc = cbm_git_context_resolve(tmp, &ctx);
    if (rc != 0 || !ctx.is_git) {
        cbm_git_context_free(&ctx);
        th_rmtree(tmp);
        FAIL("cbm_git_context_resolve failed or not a git repo");
    }

    char expected[4096];
    if (realpath(tmp, expected) == NULL) {
        cbm_git_context_free(&ctx);
        th_rmtree(tmp);
        FAIL("realpath(tmp) failed");
    }

    ASSERT_STR_EQ(ctx.canonical_root, expected);

    cbm_git_context_free(&ctx);
    th_rmtree(tmp);
    PASS();
#endif /* _WIN32 */
}

/* ── canonical_root: indexed from a subdirectory (issue #659) ─────
 * THE reproduce-first guard: from a subdir, --git-common-dir is relative, so the
 * unfixed derive_canonical_root joins it against worktree_root and strips "/.git"
 * textually, leaving canonical_root = "<root>/subdir/.." (or "<root>/..") instead
 * of "<root>". Verified RED on the unfixed code, GREEN with the realpath fix. */

TEST(canonical_root_subdir) {
#ifdef _WIN32
    SKIP_PLATFORM("git-based canonical_root test not supported on Windows CI");
#else
    char *tmp = th_mktempdir("cbm_gitctx");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (make_git_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }

    /* Create a subdirectory inside the repo. */
    char subdir[1024];
    snprintf(subdir, sizeof(subdir), "%s/scripts", tmp);
    if (th_mkdir_p(subdir) != 0) {
        th_rmtree(tmp);
        FAIL("failed to create subdir");
    }

    cbm_git_context_t ctx = {0};
    int rc = cbm_git_context_resolve(subdir, &ctx);
    if (rc != 0 || !ctx.is_git) {
        cbm_git_context_free(&ctx);
        th_rmtree(tmp);
        FAIL("cbm_git_context_resolve on subdir failed or not a git repo");
    }

    /* canonical_root must equal the repo root, NOT "<repo>/.." or "<subdir>/..". */
    char expected[4096];
    if (realpath(tmp, expected) == NULL) {
        cbm_git_context_free(&ctx);
        th_rmtree(tmp);
        FAIL("realpath(tmp) failed");
    }

    ASSERT_STR_EQ(ctx.canonical_root, expected);

    /* Sanity: canonical_root must not contain ".." or end with a slash. */
    ASSERT(strstr(ctx.canonical_root, "..") == NULL);
    ASSERT(ctx.canonical_root[strlen(ctx.canonical_root) - 1] != '/');

    cbm_git_context_free(&ctx);
    th_rmtree(tmp);
    PASS();
#endif /* _WIN32 */
}

/* ── canonical_root: linked git worktree (supporting invariant) ────
 * NOT the #659 reproducer on modern git: git that emits an *absolute*
 * --git-common-dir for a linked worktree (e.g. 2.48.x) takes the path_is_absolute
 * branch, so the bug does not manifest and this passes with or without the fix.
 * It is kept as an invariant — canonical_root of a linked worktree must equal the
 * MAIN repo root (never the worktree root or its parent) — and would fail on a git
 * build that emits a *relative* worktree common-dir. The genuine RED-without-fix
 * guard for #659 is canonical_root_subdir above. */

TEST(canonical_root_linked_worktree) {
#ifdef _WIN32
    SKIP_PLATFORM("git worktree test not implemented for Windows");
#else
    /* th_mktempdir() returns a static buffer — copy before the second call. */
    char main_tmp[256];
    char *raw = th_mktempdir("cbm_main");
    if (!raw) FAIL("th_mktempdir returned NULL");
    strncpy(main_tmp, raw, sizeof(main_tmp) - 1);
    main_tmp[sizeof(main_tmp) - 1] = '\0';

    char wt_tmp[256];
    raw = th_mktempdir("cbm_worktree");
    if (!raw) FAIL("th_mktempdir returned NULL");
    strncpy(wt_tmp, raw, sizeof(wt_tmp) - 1);
    wt_tmp[sizeof(wt_tmp) - 1] = '\0';

    /* Remove the worktree dir first — git worktree add creates it. */
    th_rmtree(wt_tmp);

    if (make_git_repo(main_tmp) != 0) {
        th_rmtree(main_tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }

    /* Create a branch for the worktree. */
    if (git_run(main_tmp, "branch wt-branch") != 0) {
        th_rmtree(main_tmp);
        FAIL("failed to create branch for worktree");
    }

    /* Add a linked worktree. */
    char wt_cmd[1024];
    snprintf(wt_cmd, sizeof(wt_cmd), "worktree add \"%s\" wt-branch", wt_tmp);
    if (git_run(main_tmp, wt_cmd) != 0) {
        th_rmtree(wt_tmp);
        th_rmtree(main_tmp);
        SKIP_PLATFORM("git worktree add unavailable (git 2.5+ required)");
    }

    cbm_git_context_t ctx = {0};
    int rc = cbm_git_context_resolve(wt_tmp, &ctx);
    if (rc != 0 || !ctx.is_git) {
        cbm_git_context_free(&ctx);
        git_run(main_tmp, "worktree prune");
        th_rmtree(main_tmp);
        th_rmtree(wt_tmp);
        FAIL("cbm_git_context_resolve on linked worktree failed");
    }

    /* canonical_root must be the MAIN repo root, not the worktree root or its parent. */
    char expected[4096];
    if (realpath(main_tmp, expected) == NULL) {
        cbm_git_context_free(&ctx);
        git_run(main_tmp, "worktree prune");
        th_rmtree(main_tmp);
        th_rmtree(wt_tmp);
        FAIL("realpath(main_tmp) failed");
    }

    ASSERT_STR_EQ(ctx.canonical_root, expected);
    ASSERT(strstr(ctx.canonical_root, "..") == NULL);

    cbm_git_context_free(&ctx);
    git_run(main_tmp, "worktree prune");
    th_rmtree(main_tmp);
    th_rmtree(wt_tmp);
    PASS();
#endif /* _WIN32 */
}

TEST(git_context_ignores_inherited_repo_env) {
#ifdef _WIN32
    SKIP_PLATFORM("git-based env-isolation test not supported on Windows CI");
#else
    char root[256];
    char *raw = th_mktempdir("cbm_gitenv");
    if (!raw)
        FAIL("th_mktempdir returned NULL");
    snprintf(root, sizeof(root), "%s", raw);
    if (make_env_isolation_tree(root) != 0) {
        th_rmtree(root);
        SKIP_PLATFORM("git not available to init a repo");
    }
    char decoy[512], fixture[512], plain[512];
    snprintf(decoy, sizeof(decoy), "%s/decoy", root);
    snprintf(fixture, sizeof(fixture), "%s/fixture", root);
    snprintf(plain, sizeof(plain), "%s/plain", root);

    char decoy_head[128], decoy_branch[128], fixture_head[128];
    git_read_line(decoy, "rev-parse HEAD", decoy_head, sizeof(decoy_head));
    git_read_line(decoy, "rev-parse --abbrev-ref HEAD", decoy_branch, sizeof(decoy_branch));
    git_read_line(fixture, "rev-parse HEAD", fixture_head, sizeof(fixture_head));

    git_env_snapshot_t snap;
    git_env_hook_enter(&snap, decoy);
    cbm_git_context_t plain_ctx = {0};
    int plain_rc = cbm_git_context_resolve(plain, &plain_ctx);
    cbm_git_context_t fix_ctx = {0};
    int fix_rc = cbm_git_context_resolve(fixture, &fix_ctx);
    git_env_hook_leave(&snap);

    bool plain_is_git = plain_ctx.is_git;
    char fix_head[128], fix_branch[128];
    snprintf(fix_head, sizeof(fix_head), "%s", fix_ctx.head_sha ? fix_ctx.head_sha : "");
    snprintf(fix_branch, sizeof(fix_branch), "%s", fix_ctx.branch ? fix_ctx.branch : "");
    bool fix_is_git = fix_ctx.is_git;
    cbm_git_context_free(&plain_ctx);
    cbm_git_context_free(&fix_ctx);

    char decoy_head_after[128], decoy_branch_after[128];
    git_read_line(decoy, "rev-parse HEAD", decoy_head_after, sizeof(decoy_head_after));
    git_read_line(decoy, "rev-parse --abbrev-ref HEAD", decoy_branch_after,
                  sizeof(decoy_branch_after));
    th_rmtree(root);

    ASSERT_EQ(plain_rc, 0);
    ASSERT_FALSE(plain_is_git); /* a plain dir is not the decoy's repo */
    ASSERT_EQ(fix_rc, 0);
    ASSERT_TRUE(fix_is_git);
    ASSERT_STR_EQ(fix_head, fixture_head); /* the fixture's HEAD, not the decoy's */
    ASSERT_STR_EQ(fix_branch, "fixture-branch");
    ASSERT_STR_EQ(decoy_head_after, decoy_head); /* the decoy is untouched */
    ASSERT_STR_EQ(decoy_branch_after, decoy_branch);
    PASS();
#endif /* _WIN32 */
}

TEST(githistory_ignores_inherited_repo_env) {
#ifdef _WIN32
    SKIP_PLATFORM("git-based env-isolation test not supported on Windows CI");
#else
    char root[256];
    char *raw = th_mktempdir("cbm_gitenv_hist");
    if (!raw)
        FAIL("th_mktempdir returned NULL");
    snprintf(root, sizeof(root), "%s", raw);
    if (make_env_isolation_tree(root) != 0) {
        th_rmtree(root);
        SKIP_PLATFORM("git not available to init a repo");
    }
    char decoy[512], plain[512];
    snprintf(decoy, sizeof(decoy), "%s/decoy", root);
    snprintf(plain, sizeof(plain), "%s/plain", root);

    git_env_snapshot_t snap;
    git_env_hook_enter(&snap, decoy);
    cbm_githistory_result_t result = {0};
    (void)cbm_pipeline_githistory_compute(plain, &result);
    git_env_hook_leave(&snap);

    int commits = result.commit_count;
    int couplings = result.count;
    free(result.couplings);
    free(result.file_temporal);
    th_rmtree(root);

    /* A non-git directory has no history: the decoy's 3 coupled commits
     * must not be attributed to it. */
    ASSERT_EQ(commits, 0);
    ASSERT_EQ(couplings, 0);
    PASS();
#endif /* _WIN32 */
}

/* ── Suite ──────────────────────────────────────────────────────── */

SUITE(git_context) {
    RUN_TEST(canonical_root_repo_root);
    RUN_TEST(canonical_root_subdir);
    RUN_TEST(canonical_root_linked_worktree);
    RUN_TEST(git_context_ignores_inherited_repo_env);
    RUN_TEST(githistory_ignores_inherited_repo_env);
}
