/*
 * repro_issue510.c — Reproduce-first case for OPEN bug #510.
 *
 * Issue: #510 — ".gitignore (non repo root) gaps and overrides"
 *
 * Root cause (discovered via discover.c):
 *   cbm_discover_ex() loads the root .gitignore ONLY when a .git directory is
 *   present at repo_path (is_git_repo gate, ~line 777).  For a non-git-root
 *   call (e.g. indexing pkg/ directly), is_git_repo = false and gitignore =
 *   NULL.  The nested-gitignore fallback also fails: try_load_nested_gitignore()
 *   has the guard "if (frame->local_gi || frame->prefix[0] == '\0') return NULL"
 *   (line 630).  The initial walk frame always has prefix == "" (empty), so
 *   prefix[0] == '\0' is true and the function returns NULL without even
 *   stat-ing the .gitignore file.  Result: the .gitignore sitting at the root
 *   of the indexed directory is completely silently ignored, so every file
 *   that it excludes gets indexed anyway.
 *
 * Expected (correct) behaviour:
 *   When cbm_discover() is called on a directory that is NOT a git repo root
 *   but DOES contain a .gitignore, that .gitignore MUST be honoured.
 *   A file matching a pattern in that .gitignore must NOT appear in the
 *   discovered file list.
 *
 * Actual (buggy) behaviour:
 *   cbm_discover() returns the excluded file as a normal discovered file
 *   because try_load_nested_gitignore() refuses to load .gitignore when
 *   the walk frame prefix is empty (i.e. the indexed directory itself).
 *
 * Why RED on current code:
 *   The fixture creates a directory WITHOUT a .git sub-directory (so the
 *   is_git_repo gate stays false), writes a .gitignore containing "secret.py",
 *   and writes secret.py + keep.py.  After cbm_discover(), the loop below
 *   checks that secret.py is NOT in the result.  On the current code the
 *   check FAILS because secret.py is present in the discovered list.
 *
 * Fix location (not implemented here):
 *   src/discover/discover.c, function try_load_nested_gitignore():
 *   Remove (or invert) the "frame->prefix[0] == '\0'" early-return guard so
 *   that the function also loads .gitignore from the root indexed directory.
 *   Additionally, cbm_discover_ex() should attempt to load a root .gitignore
 *   even when the directory is not a git repo.
 */
#include <foundation/compat.h>
#include "test_framework.h"
#include "test_helpers.h"
#include "discover/discover.h"

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Fixture ────────────────────────────────────────────────────────────────
 *
 * Directory layout (NOT a git repo — no .git/ subdir):
 *
 *   <tmpdir>/
 *     .gitignore        <- contains "secret.py"
 *     secret.py         <- should be EXCLUDED by .gitignore
 *     keep.py           <- should be INCLUDED (not matched by any pattern)
 *
 * Precondition check (to isolate the discovery layer from extraction):
 *   The root .gitignore is parseable and matches "secret.py".
 *   cbm_gitignore_matches(gi, "secret.py", false) == true.
 *   This GREEN precondition proves the matcher itself is correct; if it
 *   turns RED instead, the bug is in the matcher, not discovery.
 *
 * Primary assertion (RED on buggy code):
 *   After cbm_discover(), "secret.py" must NOT appear in the file list.
 *
 * The test does NOT create a .git directory, mirroring the exact scenario
 * from issue #510 Repro 1-A: indexing a sub-package directly rather than
 * the repo root.
 */
TEST(repro_issue510_nested_gitignore_honored) {
    /* --- set up temp directory --- */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "%s/cbm_repro510_XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Write fixture files */
    ASSERT_EQ(0, th_write_file(TH_PATH(tmpdir, ".gitignore"), "secret.py\n"));
    ASSERT_EQ(0, th_write_file(TH_PATH(tmpdir, "secret.py"),
                               "def secret(): return \"SECRET_TOKEN_111\"\n"));
    ASSERT_EQ(0, th_write_file(TH_PATH(tmpdir, "keep.py"),
                               "def ok(): return 1\n"));

    /* --- Precondition: matcher itself handles the pattern correctly --- */
    cbm_gitignore_t *gi = cbm_gitignore_parse("secret.py\n");
    ASSERT_NOT_NULL(gi);
    /* If this assertion fails, the bug is in the gitignore matcher, not
     * in discovery — a different bug, not #510. */
    ASSERT_TRUE(cbm_gitignore_matches(gi, "secret.py", false));
    cbm_gitignore_free(gi);

    /* --- Run discovery on the directory (no .git present) --- */
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(tmpdir, NULL, &files, &count);
    ASSERT_EQ(0, rc);

    /* --- Primary assertion: secret.py must NOT be discovered --- */
    bool secret_found = false;
    bool keep_found   = false;
    for (int i = 0; i < count; i++) {
        if (strcmp(files[i].rel_path, "secret.py") == 0) {
            secret_found = true;
        }
        if (strcmp(files[i].rel_path, "keep.py") == 0) {
            keep_found = true;
        }
    }
    cbm_discover_free(files, count);
    th_rmtree(tmpdir);

    /* keep.py is a valid Python file and MUST be discovered. */
    ASSERT_TRUE(keep_found);

    /*
     * RED assertion: secret.py matches the root .gitignore pattern and
     * must be excluded.  On buggy code try_load_nested_gitignore() skips
     * the root frame (prefix == ""), so secret.py IS discovered and this
     * ASSERT_FALSE fires RED.
     */
    ASSERT_FALSE(secret_found);

    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */

SUITE(repro_issue510) {
    RUN_TEST(repro_issue510_nested_gitignore_honored);
}
