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
 * These tests shell out to `git`, so the canonical_root tests SKIP_PLATFORM on
 * Windows (the CI shell there cannot init a repo via system()). The bounded
 * worktree-status tests below shell out through cbm_popen instead, so they run
 * on every platform that has git on PATH.
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

/* ── Bounded worktree status (BT-240 increment 3) ──────────────────────
 * These run on EVERY platform, not just POSIX: they shell out through
 * cbm_popen (the same isolated spawn production git_context uses) instead of
 * system(), so they work whenever git is on PATH — Windows CI included. The
 * core status parser is platform-neutral and must be exercised everywhere git
 * is available; only a genuinely missing git skips. */

static int status_git_run(const char *dir, const char *args) {
    char cmd[1024];
#ifdef _WIN32
    const char *null_dev = "NUL";
#else
    const char *null_dev = "/dev/null";
#endif
    snprintf(cmd, sizeof(cmd), "git -C \"%s\" %s 2>%s", dir, args, null_dev);
    FILE *fp = cbm_popen(cmd, "r");
    if (!fp) {
        return -1;
    }
    char drain[256];
    while (fgets(drain, sizeof(drain), fp)) {
    }
    return cbm_pclose(fp);
}

static int status_make_repo(const char *dir) {
    if (th_mkdir_p(dir) != 0) return -1;
    if (status_git_run(dir, "init -q") != 0) return -1;
    if (status_git_run(dir, "config user.email test@example.com") != 0) return -1;
    if (status_git_run(dir, "config user.name Test") != 0) return -1;
    if (th_write_file(TH_PATH(dir, ".keep"), "") != 0) return -1;
    if (status_git_run(dir, "add -A") != 0) return -1;
    if (status_git_run(dir, "commit -qm init") != 0) return -1;
    return 0;
}

TEST(worktree_status_clean) {
    char *tmp = th_mktempdir("cbm_wtstatus");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (status_make_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }

    cbm_worktree_status_t st = {0};
    int rc = cbm_git_worktree_status(tmp, 8, &st);
    if (rc != 0 || !st.available) {
        cbm_git_worktree_status_free(&st);
        th_rmtree(tmp);
        FAIL("clean repo status not available");
    }
    ASSERT_EQ(st.tracked_count, 0);
    ASSERT_EQ(st.untracked_count, 0);
    ASSERT_EQ(st.tracked_sample_count, 0);
    ASSERT_EQ(st.untracked_sample_count, 0);
    ASSERT_FALSE(st.tracked_truncated);
    ASSERT_FALSE(st.untracked_truncated);

    cbm_git_worktree_status_free(&st);
    th_rmtree(tmp);
    PASS();
}

TEST(worktree_status_tracked_modified) {
    char *tmp = th_mktempdir("cbm_wtstatus");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (status_make_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }
    if (th_write_file(TH_PATH(tmp, "tracked.txt"), "v1") != 0 ||
        status_git_run(tmp, "add -f -- tracked.txt") != 0 ||
        status_git_run(tmp, "commit -q -m add-tracked") != 0) {
        th_rmtree(tmp);
        FAIL("failed to commit tracked file");
    }
    if (th_write_file(TH_PATH(tmp, "tracked.txt"), "v2") != 0) {
        th_rmtree(tmp);
        FAIL("failed to modify tracked file");
    }

    cbm_worktree_status_t st = {0};
    int rc = cbm_git_worktree_status(tmp, 8, &st);
    if (rc != 0 || !st.available) {
        cbm_git_worktree_status_free(&st);
        th_rmtree(tmp);
        FAIL("status not available");
    }
    ASSERT_EQ(st.tracked_count, 1);
    ASSERT_EQ(st.untracked_count, 0);
    ASSERT_EQ(st.tracked_sample_count, 1);
    ASSERT_STR_EQ(st.tracked_paths[0], "tracked.txt");
    ASSERT_FALSE(st.tracked_truncated);

    cbm_git_worktree_status_free(&st);
    th_rmtree(tmp);
    PASS();
}

TEST(worktree_status_untracked) {
    char *tmp = th_mktempdir("cbm_wtstatus");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (status_make_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }
    if (th_write_file(TH_PATH(tmp, "newfile.txt"), "n") != 0) {
        th_rmtree(tmp);
        FAIL("failed to create untracked file");
    }

    cbm_worktree_status_t st = {0};
    int rc = cbm_git_worktree_status(tmp, 8, &st);
    if (rc != 0 || !st.available) {
        cbm_git_worktree_status_free(&st);
        th_rmtree(tmp);
        FAIL("status not available");
    }
    ASSERT_EQ(st.untracked_count, 1);
    ASSERT_EQ(st.tracked_count, 0);
    ASSERT_EQ(st.untracked_sample_count, 1);
    ASSERT_STR_EQ(st.untracked_paths[0], "newfile.txt");
    ASSERT_FALSE(st.untracked_truncated);

    cbm_git_worktree_status_free(&st);
    th_rmtree(tmp);
    PASS();
}

TEST(worktree_status_rename_counts_once) {
    char *tmp = th_mktempdir("cbm_wtstatus");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (status_make_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }
    if (th_write_file(TH_PATH(tmp, "old.txt"), "o") != 0 ||
        th_write_file(TH_PATH(tmp, "other.txt"), "x") != 0 ||
        status_git_run(tmp, "add -A") != 0 ||
        status_git_run(tmp, "commit -qm base") != 0 ||
        status_git_run(tmp, "mv old.txt new.txt") != 0) {
        th_rmtree(tmp);
        FAIL("failed to stage a rename");
    }

    cbm_worktree_status_t st = {0};
    int rc = cbm_git_worktree_status(tmp, 8, &st);
    if (rc != 0 || !st.available) {
        cbm_git_worktree_status_free(&st);
        th_rmtree(tmp);
        FAIL("status not available");
    }
    ASSERT_EQ(st.tracked_count, 1); /* rename/copy counts ONCE, not twice */
    ASSERT_EQ(st.tracked_sample_count, 1);
    ASSERT_STR_EQ(st.tracked_paths[0], "new.txt"); /* sampled NEW path */
    ASSERT_EQ(st.untracked_count, 0);

    cbm_git_worktree_status_free(&st);
    th_rmtree(tmp);
    PASS();
}

TEST(worktree_status_sample_cap_truncates) {
    char *tmp = th_mktempdir("cbm_wtstatus");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (status_make_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }
    enum { N_FILES = 5, SAMPLE_MAX = 2 };
    for (int i = 0; i < N_FILES; i++) {
        char path[1024];
        snprintf(path, sizeof(path), "%s/u%d.txt", tmp, i);
        if (th_write_file(path, "u") != 0) {
            th_rmtree(tmp);
            FAIL("failed to create untracked file");
        }
    }

    cbm_worktree_status_t st = {0};
    int rc = cbm_git_worktree_status(tmp, SAMPLE_MAX, &st);
    if (rc != 0 || !st.available) {
        cbm_git_worktree_status_free(&st);
        th_rmtree(tmp);
        FAIL("status not available");
    }
    ASSERT_EQ(st.untracked_count, N_FILES); /* full stream counted... */
    ASSERT_EQ(st.untracked_sample_count, SAMPLE_MAX); /* ...samples bounded */
    ASSERT_TRUE(st.untracked_truncated);
    ASSERT_EQ(st.tracked_count, 0);
    ASSERT_FALSE(st.tracked_truncated);

    cbm_git_worktree_status_free(&st);
    th_rmtree(tmp);
    PASS();
}

TEST(worktree_status_mixed_tracked_and_untracked) {
    char *tmp = th_mktempdir("cbm_wtstatus");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    if (status_make_repo(tmp) != 0) {
        th_rmtree(tmp);
        SKIP_PLATFORM("git not available to init a repo");
    }
    if (th_write_file(TH_PATH(tmp, "tracked.txt"), "v1") != 0 ||
        status_git_run(tmp, "add -f -- tracked.txt") != 0 ||
        status_git_run(tmp, "commit -q -m add-tracked") != 0) {
        th_rmtree(tmp);
        FAIL("failed to commit tracked file");
    }
    if (th_write_file(TH_PATH(tmp, "tracked.txt"), "v2") != 0 ||
        th_write_file(TH_PATH(tmp, "untracked.txt"), "n") != 0) {
        th_rmtree(tmp);
        FAIL("failed to prepare mixed worktree");
    }

    cbm_worktree_status_t st = {0};
    int rc = cbm_git_worktree_status(tmp, 8, &st);
    if (rc != 0 || !st.available) {
        cbm_git_worktree_status_free(&st);
        th_rmtree(tmp);
        FAIL("status not available");
    }
    ASSERT_EQ(st.tracked_count, 1);
    ASSERT_EQ(st.untracked_count, 1);
    ASSERT_STR_EQ(st.tracked_paths[0], "tracked.txt");
    ASSERT_STR_EQ(st.untracked_paths[0], "untracked.txt");

    cbm_git_worktree_status_free(&st);
    th_rmtree(tmp);
    PASS();
}

/* Fail-closed contract: a non-repo directory must report unavailable with
 * zero counts — never a silent "clean" tree. Does not need git on PATH. */
TEST(worktree_status_unavailable_non_repo) {
    char *tmp = th_mktempdir("cbm_wtstatus");
    if (!tmp) FAIL("th_mktempdir returned NULL");

    cbm_worktree_status_t st = {0};
    int rc = cbm_git_worktree_status(tmp, 8, &st);
    if (rc != 0) {
        th_rmtree(tmp);
        FAIL("status call returned an error");
    }
    ASSERT_FALSE(st.available);
    ASSERT_EQ(st.tracked_count, 0);
    ASSERT_EQ(st.untracked_count, 0);
    ASSERT_EQ(st.tracked_sample_count, 0);
    ASSERT_EQ(st.untracked_sample_count, 0);

    cbm_git_worktree_status_free(&st);
    th_rmtree(tmp);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────── */

SUITE(git_context) {
    RUN_TEST(canonical_root_repo_root);
    RUN_TEST(canonical_root_subdir);
    RUN_TEST(canonical_root_linked_worktree);
    RUN_TEST(worktree_status_clean);
    RUN_TEST(worktree_status_tracked_modified);
    RUN_TEST(worktree_status_untracked);
    RUN_TEST(worktree_status_rename_counts_once);
    RUN_TEST(worktree_status_sample_cap_truncates);
    RUN_TEST(worktree_status_mixed_tracked_and_untracked);
    RUN_TEST(worktree_status_unavailable_non_repo);
}
