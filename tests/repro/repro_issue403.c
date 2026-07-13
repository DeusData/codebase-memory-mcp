/*
 * repro_issue403.c -- Reproduce-first case for OPEN bug #403.
 *
 * Issue: #403 -- "The IDE's installation directory is unnecessarily indexed"
 * https://github.com/DeusData/codebase-memory-mcp/issues/403
 *
 * Wrongly-indexed directory: AppData/Local/Programs/Antigravity
 *   (the Antigravity IDE install tree; reported name confirmed in issue comments)
 *
 * Root cause (src/discover/discover.c):
 *   cbm_should_skip_dir() (line 339) tests only the BARE directory name
 *   (entry->name, the last path component) against ALWAYS_SKIP_DIRS and
 *   FAST_SKIP_DIRS.  None of "AppData", "Local", "Programs", or "Antigravity"
 *   appears in either list.  Therefore cbm_discover() walks straight into the
 *   IDE install tree and indexes every source-like file it contains.
 *
 *   There is no install-directory guard at ANY layer:
 *     - ALWAYS_SKIP_DIRS covers VCS, build tools, and caches -- not IDE
 *       install prefixes (Programs, AppData/Local/Programs, etc.).
 *     - The .gitignore path is only loaded when a .git directory is present
 *       (is_git_repo gate, line 777 of discover.c).  An IDE install dir does
 *       not contain .git, so .gitignore exclusions never fire.
 *     - The cbmignore path (opts->ignore_file or .cbmignore at root) is
 *       similarly absent from an install dir by default.
 *   Result: any source-extension file found under Antigravity/ is returned
 *   as a discovered file, bloating the graph with IDE internals.
 *
 * Expected (correct) behaviour:
 *   When cbm_discover() is called on a directory that contains an
 *   "Antigravity" subdirectory (or more generally any IDE install subtree),
 *   files under that subdirectory must NOT appear in the discovered file list.
 *   The correct fix (per the issue owner's comment) is to add "Antigravity"
 *   (and the broader "Programs" / install-dir pattern) to the exclusion layer,
 *   OR to extend the exclusion to root-path patterns so auto-index never picks
 *   an install dir as a project root in the first place.
 *
 * Actual (buggy) behaviour:
 *   cbm_discover() returns files under Antigravity/ as normal discovered
 *   files because the bare dirname "Antigravity" is absent from ALWAYS_SKIP_DIRS.
 *
 * Why RED on current code:
 *   The fixture creates a temp dir with:
 *     normal.py           -- a legitimate source file (control: MUST appear)
 *     Antigravity/ide.py  -- sentinel inside the IDE install dir (MUST NOT appear)
 *   cbm_discover() is called on the temp dir.  The loop below asserts that
 *   ide.py is NOT in the result.  On current code "Antigravity" is not skipped,
 *   so ide.py IS discovered and the ASSERT_FALSE fires RED.
 *
 * Fix location (not implemented here):
 *   src/discover/discover.c, ALWAYS_SKIP_DIRS array:
 *   Add "Antigravity" (and any other IDE install dir names to be excluded)
 *   to the NULL-terminated list.  The broader fix is to extend the list with
 *   install-path components ("Programs", "AppData") or, per the issue owner,
 *   to implement a root-path exclusion in the auto-index root-selection logic
 *   so directories under AppData/Local/Programs are never chosen as repo roots.
 *
 * Exclusion is NOT config-driven in the current code.  The closest knob is a
 * .cbmignore file at the repo root (loaded unconditionally, unlike .gitignore
 * which requires .git/).  Passing opts->ignore_file also works.  However,
 * neither is set in this test -- we assert on the default behaviour, which is
 * what the bug reporter experiences.
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
 * Directory layout (NOT a git repo -- no .git/ subdir):
 *
 *   <tmpdir>/
 *     normal.py           <- legitimate source file; MUST be discovered
 *     Antigravity/
 *       ide.py            <- sentinel inside IDE install dir; must NOT appear
 *
 * cbm_discover() is called on <tmpdir> with no opts (NULL) so all default
 * exclusions apply and no extra ignore file is consulted.
 *
 * Control assertion (expected GREEN even on buggy code):
 *   normal.py IS in the result -- proves discovery ran at all.
 *
 * Primary assertion (RED on buggy code):
 *   ide.py is NOT in the result -- the Antigravity subtree was skipped.
 */

TEST(repro_issue403_install_dir_excluded) {
    /* --- set up temp directory --- */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "%s/cbm_repro403_XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(tmpdir));

    /* Control file: a normal Python source at the repo root. */
    ASSERT_EQ(0, th_write_file(TH_PATH(tmpdir, "normal.py"),
                               "def hello(): return 1\n"));

    /* Sentinel file: a Python source inside the Antigravity install dir.
     * This is the file that MUST be absent from discovery results.
     * th_write_file creates intermediate directories automatically. */
    ASSERT_EQ(0, th_write_file(TH_PATH(tmpdir, "Antigravity/ide.py"),
                               "# Antigravity IDE internal module\ndef _internal(): pass\n"));

    /* --- Run discovery (default opts: no .git, no .cbmignore, no opts) --- */
    cbm_file_info_t *files = NULL;
    int count = 0;
    int rc = cbm_discover(tmpdir, NULL, &files, &count);
    ASSERT_EQ(0, rc);

    /* --- Scan results --- */
    bool normal_found    = false;
    bool ide_file_found  = false;
    for (int i = 0; i < count; i++) {
        if (strcmp(files[i].rel_path, "normal.py") == 0) {
            normal_found = true;
        }
        /* Match any path that descends into the Antigravity directory. */
        if (strncmp(files[i].rel_path, "Antigravity/", 12) == 0 ||
            strcmp(files[i].rel_path, "Antigravity") == 0) {
            ide_file_found = true;
            printf("  BUG #403 reproduced: IDE install-dir file indexed: %s\n",
                   files[i].rel_path);
        }
    }

    cbm_discover_free(files, count);
    th_rmtree(tmpdir);

    /* Control: normal.py must be discovered -- discovery ran correctly. */
    ASSERT_TRUE(normal_found);

    /*
     * PRIMARY assertion (RED on buggy code):
     *
     * No file under Antigravity/ may appear in the discovered set.
     * On current code, "Antigravity" is absent from ALWAYS_SKIP_DIRS so
     * cbm_should_skip_dir("Antigravity", ...) returns false and the walk
     * descends into it.  ide.py is discovered, ide_file_found is true, and
     * this ASSERT_FALSE fires RED.
     *
     * After the fix -- "Antigravity" added to ALWAYS_SKIP_DIRS (or an
     * equivalent install-path exclusion applied) -- cbm_should_skip_dir
     * returns true, the subtree is skipped, ide_file_found stays false,
     * and this assertion passes GREEN.
     */
    ASSERT_FALSE(ide_file_found);

    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */

SUITE(repro_issue403) {
    RUN_TEST(repro_issue403_install_dir_excluded);
}
