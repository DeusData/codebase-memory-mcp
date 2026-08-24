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
#include "git/git_command.h"
#include "git/git_context.h"
#include "git/git_snapshot.h"

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

/* Cross-platform setup for branch-only tests. Unlike git_run(), this uses the
 * production command formatter and cbm_popen/cbm_pclose lifecycle on Windows
 * as well as POSIX. */
static int make_git_repo_portable(const char *dir) {
    if (th_mkdir_p(dir) != 0) return -1;
    const char *const init_args[] = {"init", "-q", NULL};
    const char *const email_args[] = {"config", "user.email", "test@example.com", NULL};
    const char *const name_args[] = {"config", "user.name", "Test", NULL};
    if (cbm_git_drain_command(dir, init_args) != 0) return -1;
    if (cbm_git_drain_command(dir, email_args) != 0) return -1;
    if (cbm_git_drain_command(dir, name_args) != 0) return -1;
    char path[CBM_SZ_1K];
    int n = snprintf(path, sizeof(path), "%s/.keep", dir);
    if (n <= 0 || (size_t)n >= sizeof(path)) return -1;
    th_write_file(path, "");
    const char *const add_args[] = {"add", ".keep", NULL};
    const char *const commit_args[] = {"commit", "-q", "-m", "init", NULL};
    if (cbm_git_drain_command(dir, add_args) != 0) return -1;
    return cbm_git_drain_command(dir, commit_args);
}

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

TEST(current_branch_resolves_attached_detached_unborn_and_non_git) {
    char repo[256];
    char *raw = th_mktempdir("cbm_branch_repo");
    if (!raw) FAIL("th_mktempdir returned NULL");
    snprintf(repo, sizeof(repo), "%s", raw);

    char non_git[256];
    raw = th_mktempdir("cbm_branch_plain");
    if (!raw) {
        th_rmtree(repo);
        FAIL("th_mktempdir returned NULL");
    }
    snprintf(non_git, sizeof(non_git), "%s", raw);

    const char *const branch_args[] = {"checkout", "-q", "-b", "branch-probe", NULL};
    bool setup_ok =
        make_git_repo_portable(repo) == 0 && cbm_git_drain_command(repo, branch_args) == 0;
    char *attached = NULL;
    char *detached = NULL;
    char *unborn = NULL;
    char *plain = NULL;
    int attached_rc = setup_ok ? cbm_git_current_branch(repo, &attached) : CBM_NOT_FOUND;
    const char *const detach_args[] = {"checkout", "-q", "--detach", NULL};
    bool detach_ok = setup_ok && cbm_git_drain_command(repo, detach_args) == 0;
    int detached_rc = detach_ok ? cbm_git_current_branch(repo, &detached) : CBM_NOT_FOUND;
    cbm_git_context_t detached_context = {0};
    int detached_context_rc =
        detach_ok ? cbm_git_context_resolve(repo, &detached_context) : CBM_NOT_FOUND;
    const char *const unborn_init_args[] = {"init", "-q", NULL};
    const char *const unborn_ref_args[] = {
        "symbolic-ref", "HEAD", "refs/heads/unborn-probe", NULL};
    bool unborn_setup_ok = cbm_git_drain_command(non_git, unborn_init_args) == 0 &&
                           cbm_git_drain_command(non_git, unborn_ref_args) == 0;
    int unborn_rc =
        unborn_setup_ok ? cbm_git_current_branch(non_git, &unborn) : CBM_NOT_FOUND;
    cbm_git_context_t unborn_context = {0};
    int unborn_context_rc =
        unborn_setup_ok ? cbm_git_context_resolve(non_git, &unborn_context) : CBM_NOT_FOUND;
    char plain_dir[256];
    raw = th_mktempdir("cbm_branch_plain_after_unborn");
    bool plain_setup_ok = raw != NULL;
    snprintf(plain_dir, sizeof(plain_dir), "%s", raw ? raw : "");
    int plain_rc = plain_setup_ok ? cbm_git_current_branch(plain_dir, &plain) : CBM_NOT_FOUND;

    bool attached_ok = attached_rc == 0 && attached && strcmp(attached, "branch-probe") == 0;
    bool detached_ok = detached_rc == 0 && detached && strcmp(detached, "DETACHED") == 0;
    bool detached_context_ok = detached_context_rc == 0 && detached_context.is_detached &&
                               detached_context.branch &&
                               strcmp(detached_context.branch, "DETACHED") == 0;
    bool unborn_ok = unborn_rc == 0 && unborn && strcmp(unborn, "unborn-probe") == 0;
    bool unborn_context_ok = unborn_context_rc == 0 && unborn_context.is_git &&
                             !unborn_context.is_detached && unborn_context.branch &&
                             strcmp(unborn_context.branch, "unborn-probe") == 0 &&
                             unborn_context.head_sha && unborn_context.head_sha[0] == '\0' &&
                             unborn_context.base_sha && unborn_context.base_sha[0] == '\0';
    bool plain_ok = plain_rc == CBM_NOT_FOUND && plain == NULL;
    free(attached);
    free(detached);
    free(unborn);
    free(plain);
    cbm_git_context_free(&detached_context);
    cbm_git_context_free(&unborn_context);
    if (plain_setup_ok) th_rmtree(plain_dir);
    th_rmtree(non_git);
    th_rmtree(repo);

    ASSERT_TRUE(setup_ok);
    ASSERT_TRUE(detach_ok);
    ASSERT_TRUE(unborn_setup_ok);
    ASSERT_TRUE(plain_setup_ok);
    ASSERT_TRUE(attached_ok);
    ASSERT_TRUE(detached_ok);
    ASSERT_TRUE(detached_context_ok);
    ASSERT_TRUE(unborn_ok);
    ASSERT_TRUE(unborn_context_ok);
    ASSERT_TRUE(plain_ok);
    PASS();
}

/* Shell command strings either reject or reinterpret these characters,
 * especially through cmd.exe. The shared Git runner must pass the repository
 * path as one literal argv element on every platform. This exercises command
 * setup, context resolution, and snapshot capture through the real Git binary. */
TEST(literal_metacharacter_repo_path_round_trips_through_git_argv) {
    char base[CBM_PATH_MAX];
    char *raw = th_mktempdir("cbm_git_argv_literal");
    if (!raw) FAIL("th_mktempdir returned NULL");
    int base_written = snprintf(base, sizeof(base), "%s", raw);
    if (base_written <= 0 || (size_t)base_written >= sizeof(base)) {
        FAIL("temporary base path does not fit");
    }

    char repo[CBM_PATH_MAX];
    int repo_written = snprintf(repo, sizeof(repo), "%s/repo %%!^&; literal", base);
    if (repo_written <= 0 || (size_t)repo_written >= sizeof(repo)) {
        th_rmtree(base);
        FAIL("literal repository path does not fit");
    }
    if (make_git_repo_portable(repo) != 0) {
        th_rmtree(base);
        SKIP_PLATFORM("git not available to initialize literal-path repository");
    }

    cbm_git_context_t context = {0};
    cbm_git_snapshot_t snapshot = {0};
    int context_rc = cbm_git_context_resolve(repo, &context);
    int snapshot_rc = cbm_git_snapshot_read(
        repo, CBM_GIT_SNAPSHOT_HEAD | CBM_GIT_SNAPSHOT_DIRTY | CBM_GIT_SNAPSHOT_FILE_COUNT,
        &snapshot);
    bool context_ok = context_rc == 0 && context.is_git && context.worktree_root &&
                      strstr(context.worktree_root, "repo %!^&; literal") != NULL;
    bool snapshot_ok = snapshot_rc == 0 && snapshot.path_supported && snapshot.is_git &&
                       snapshot.head[0] != '\0' && snapshot.file_count == 1;

    cbm_git_context_free(&context);
    th_rmtree(base);

    ASSERT_TRUE(cbm_git_validate_repo_path(repo));
    ASSERT_TRUE(context_ok);
    ASSERT_TRUE(snapshot_ok);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────── */

SUITE(git_context) {
    RUN_TEST(canonical_root_repo_root);
    RUN_TEST(canonical_root_subdir);
    RUN_TEST(canonical_root_linked_worktree);
    RUN_TEST(current_branch_resolves_attached_detached_unborn_and_non_git);
    RUN_TEST(literal_metacharacter_repo_path_round_trips_through_git_argv);
}
