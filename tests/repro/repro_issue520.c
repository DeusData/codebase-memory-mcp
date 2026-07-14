/*
 * repro_issue520.c -- Reproduce-first case for OPEN bug #520.
 *
 * Issue: #520 -- "New files not detected without explicit re-index
 *                (watcher doesn't trigger for file creation)"
 *
 * Root cause (src/mcp/mcp.c: handle_detect_changes):
 *   detect_changes builds its changed-file list by running two git commands:
 *     (1) git diff --name-only <base>...HEAD  (committed changes)
 *     (2) git diff --name-only               (unstaged tracked changes)
 *   Neither command reports UNTRACKED new files.  Those only appear in
 *   git status --porcelain (prefix "??").  Because handle_detect_changes
 *   never calls git status, a brand-new file that has not been git-added
 *   is completely invisible to the tool until the user manually calls
 *   index_repository again.
 *
 * Expected (correct) behaviour:
 *   After creating a new source file in a watched repo, calling
 *   detect_changes MUST include that file in "changed_files" so callers
 *   know the graph is stale and needs re-indexing (or so the incremental
 *   path can pick it up automatically).
 *
 * Actual (buggy) behaviour:
 *   detect_changes returns {"changed_files":[], "changed_count":0}.
 *   The new file is invisible until the user manually calls index_repository.
 *
 * Why RED on current code:
 *   The assertion below checks that "new_func.py" appears somewhere in the
 *   detect_changes JSON response.  On current code the response contains an
 *   empty changed_files array, so strstr returns NULL and ASSERT_NOT_NULL
 *   fails.
 *
 * Fix location (not implemented here):
 *   src/mcp/mcp.c, handle_detect_changes(): after the existing git-diff
 *   popen block, add a second popen for:
 *     git --no-optional-locks -C <root> status --porcelain
 *         --untracked-files=normal 2>/dev/null
 *   and include lines prefixed "??" (untracked) and "A " (staged new file)
 *   in the changed_files output.  The watcher already does exactly this via
 *   git_is_dirty() in src/watcher/watcher.c:140.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "test_helpers.h"
#include <mcp/mcp.h>
#include <pipeline/pipeline.h> /* cbm_project_name_from_path */

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>

/* ── Local git helper (mirrors test_watcher.c:wt_git) ─────────── */

/* Run "git -C <dir> <args>" with a neutral identity so the test
 * needs no global git config and works under cmd.exe on Windows.
 * Returns the git exit status. */
static int r520_git(const char *dir, const char *args) {
    char cmd[1024];
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" -c user.name=t -c user.email=t@t.io "
             "-c init.defaultBranch=main -c commit.gpgsign=false %s",
             dir, args);
    return system(cmd);
}

/* ── Test ──────────────────────────────────────────────────────── */

/*
 * Scenario (matches the exact steps from issue #520 comment):
 *
 *   1. Create a fresh git repo with one committed Python file.
 *   2. Index the repo via the MCP index_repository tool so the server
 *      has a valid project handle (needed for detect_changes to resolve
 *      the project root).
 *   3. Write a NEW untracked Python file (not git-added, not committed).
 *   4. Call detect_changes -- this is the tool users call to discover
 *      what has changed since the last index.
 *   5. Assert the new file name ("new_func.py") appears in the response.
 *
 * On current code step 5 FAILS: detect_changes only runs git-diff and
 * misses untracked files entirely.
 *
 * No sleep is used: detect_changes is a synchronous, single-call API
 * that runs git commands inline.  There is no background thread or timer
 * to wait for; the bug is purely in which git command is chosen.
 */
TEST(repro_issue520_detect_changes_includes_new_untracked_file) {
    /* --- set up a temporary git repo -------------------------------- */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_r520_XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    if (r520_git(tmpdir, "init -q") != 0) {
        th_rmtree(tmpdir);
        FAIL("git init failed");
    }

    /* Commit one baseline file so HEAD exists (needed for git diff base...HEAD) */
    {
        char p[512];
        snprintf(p, sizeof(p), "%s/existing.py", tmpdir);
        th_write_file(p, "def existing(): pass\n");
    }
    if (r520_git(tmpdir, "add existing.py") != 0 ||
        r520_git(tmpdir, "commit -q -m \"init\"") != 0) {
        th_rmtree(tmpdir);
        FAIL("git commit failed");
    }

    /* --- index the repo via the MCP production flow ----------------- */
    cbm_mcp_server_t *srv = cbm_mcp_server_new(NULL);
    if (!srv) {
        th_rmtree(tmpdir);
        FAIL("cbm_mcp_server_new returned NULL");
    }

    {
        char args[512];
        snprintf(args, sizeof(args), "{\"repo_path\":\"%s\"}", tmpdir);
        char *resp = cbm_mcp_handle_tool(srv, "index_repository", args);
        free(resp);
    }

    /* --- create a brand-new untracked file (never git-added) -------- */
    {
        char p[512];
        snprintf(p, sizeof(p), "%s/new_func.py", tmpdir);
        th_write_file(p, "def new_func(): return 42\n");
    }

    /* --- call detect_changes synchronously -------------------------- */
    /* Use base_branch="main" -- the branch name matches init.defaultBranch
     * set above.  detect_changes runs git diff main...HEAD (same commit,
     * no committed change) + git diff (no staged change), so on current
     * code the result is always {"changed_files":[],"changed_count":0}.
     * After the fix, git status --porcelain would also be consulted and
     * new_func.py (marked "??") would appear in the output.
     *
     * The `project` argument is REQUIRED: detect_changes (like every other
     * MCP tool) resolves the project DB via resolve_store(), which has no
     * implicit fallback for a NULL project.  The real issue #520 reproduction
     * calls detect_changes(project="...") explicitly; the project name is
     * derived from the indexed repo path exactly as the pipeline derives it. */
    char *dc_project = cbm_project_name_from_path(tmpdir);
    if (!dc_project) {
        cbm_mcp_server_free(srv);
        th_rmtree(tmpdir);
        FAIL("cbm_project_name_from_path failed");
    }
    char dc_args[640];
    snprintf(dc_args, sizeof(dc_args),
             "{\"base_branch\":\"main\",\"project\":\"%s\"}", dc_project);
    free(dc_project);
    char *dc_resp = cbm_mcp_handle_tool(srv, "detect_changes", dc_args);

    /* --- assert the new file is reported ---------------------------- */
    /* Expected: dc_resp contains "new_func.py" in the changed_files list.
     * Actual (buggy): dc_resp contains "changed_count":0 and an empty
     * changed_files array -- strstr returns NULL -- ASSERT_NOT_NULL FAILS. */
    ASSERT_NOT_NULL(dc_resp);
    int found = (strstr(dc_resp, "new_func.py") != NULL) ? 1 : 0;

    free(dc_resp);
    cbm_mcp_server_free(srv);
    th_rmtree(tmpdir);

    /* This is the reproduce-first assertion: RED until the fix lands.
     * found == 0 means detect_changes ignored the untracked new file. */
    ASSERT_EQ(found, 1);

    PASS();
}

/* ── Suite entry point ─────────────────────────────────────────── */

SUITE(repro_issue520) {
    RUN_TEST(repro_issue520_detect_changes_includes_new_untracked_file);
}
