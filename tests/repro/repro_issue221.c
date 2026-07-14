/*
 * repro_issue221.c  --  Regression guard for bug #221.
 *
 * Bug #221: "'install' command does not work for opencode in windows 11"
 *
 * ROOT CAUSE:
 *   find_in_path (src/cli/cli.c) probed only the bare executable name
 *   "opencode" for each PATH entry.  On Windows, CLI tools installed via
 *   mise/npm/scoop ship as extension-bearing shims (.cmd, .ps1, .exe), so
 *   the bare-name probe never matched and cbm_find_cli("opencode", ...) always
 *   returned an empty string.  The installer therefore concluded opencode was
 *   absent and skipped wiring it even when it was present on PATH.
 *
 * FIX (commit 0485d3f, "fix(cli): probe Windows PATHEXT variants in
 *   find_in_path (#221)"):
 *   On _WIN32, find_in_path now iterates the common PATHEXT variants
 *   (.exe, .cmd, .bat, .ps1) for each PATH directory after the bare-name
 *   probe fails, matching whichever extension-qualified file is present.
 *
 * REGRESSION GUARD -- expected GREEN on current main (fix is in):
 *   The fix was committed as 0485d3f and CI (build-windows + test-windows)
 *   was green before merge.  This test is therefore expected to PASS on the
 *   current codebase.  It will turn RED if find_in_path is accidentally
 *   regressed to bare-name-only lookup.
 *
 * CROSS-PLATFORM STRATEGY:
 *   On POSIX: create a plain executable named "opencode" (no extension).
 *             Bare-name lookup has always worked here, so the test confirms
 *             cbm_find_cli("opencode", ...) resolves correctly -- the baseline.
 *   On Windows: create "opencode.cmd" (the most common shim format).
 *             Before the fix, find_in_path returned "" for this case; after
 *             the fix it returns the .cmd path -- the regression guard proper.
 *   Both branches exercise the same public function and assertion; only the
 *   fixture filename differs.
 *
 * NOTE: no slash-star inside this block comment to avoid nested-comment UB.
 */

#include <foundation/compat.h>
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>

#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ── Minimal local helpers (mirror test_cli.c pattern) ──────────────────── */

static int repro221_write_file(const char *path, const char *content) {
    FILE *f = fopen(path, "w");
    if (!f)
        return -1;
    fprintf(f, "%s", content);
    fclose(f);
    return 0;
}

/* ── Test ───────────────────────────────────────────────────────────────── */

/*
 * repro_issue221_opencode_pathext_lookup
 *
 * Verify that cbm_find_cli("opencode", ...) resolves the opencode executable
 * (or its Windows .cmd shim) when the containing directory is on PATH.
 *
 * CORRECT BEHAVIOUR (post-fix):
 *   cbm_find_cli returns a non-empty string whose basename starts with
 *   "opencode" -- meaning find_in_path found the file.
 *
 * BUGGY BEHAVIOUR (pre-fix, Windows only):
 *   cbm_find_cli returns "" because find_in_path only probed the bare name
 *   "opencode" and never tried "opencode.cmd" / "opencode.exe" / etc.
 *
 * GREEN on current main (fix present): ASSERT fires with a non-empty result.
 * RED if regressed: ASSERT fires because result is empty.
 */
TEST(repro_issue221_opencode_pathext_lookup) {
    /* Create an isolated temp directory to act as a fake PATH entry. */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/repro221-XXXXXX");
    if (!cbm_mkdtemp(tmpdir))
        FAIL("cbm_mkdtemp failed");

    /*
     * Choose the fixture filename to match the platform convention:
     *   POSIX   -- "opencode"      (plain executable; bare-name lookup)
     *   Windows -- "opencode.cmd"  (most common shim installed by mise/npm)
     *
     * On Windows (pre-fix) find_in_path returned "" for "opencode.cmd"
     * because only the bare name was probed.  The fix tries .cmd before
     * moving to the next PATH entry, so the shim is found.
     */
#ifdef _WIN32
    const char *fixture_name = "opencode.cmd";
    const char *fixture_content = "@echo off\r\nrem fake opencode shim\r\n";
#else
    const char *fixture_name = "opencode";
    const char *fixture_content = "#!/bin/sh\n# fake opencode\n";
#endif

    char fixture_path[512];
    snprintf(fixture_path, sizeof(fixture_path), "%s/%s", tmpdir, fixture_name);

    if (repro221_write_file(fixture_path, fixture_content) != 0)
        FAIL("failed to write opencode fixture");

    /* Make executable (no-op on Windows -- extension decides executability). */
    th_make_executable(fixture_path);

    /* Swap PATH so only tmpdir is searched, isolating the lookup. */
    const char *raw_path = getenv("PATH");
    char *old_path = raw_path ? strdup(raw_path) : NULL;
    cbm_setenv("PATH", tmpdir, 1);

    /*
     * The function under test: cbm_find_cli is the public API that calls
     * find_in_path internally.  We pass a non-existent home_dir so fallback
     * paths (~/.local/bin etc.) are never tried -- the only possible match
     * is the fixture file created above.
     *
     * Pre-fix (Windows): find_in_path probed "<tmpdir>/opencode" (absent)
     *   and returned false.  cbm_find_cli returned "".
     * Post-fix (Windows): find_in_path also probes "<tmpdir>/opencode.cmd"
     *   (present), finds it, and cbm_find_cli returns the full path.
     * POSIX (before and after): bare-name probe succeeds immediately.
     */
    const char *result = cbm_find_cli("opencode", "/nonexistent-home-dir");

    /* Restore PATH before any assertion so cleanup is always reached. */
    if (old_path) {
        cbm_setenv("PATH", old_path, 1);
        free(old_path);
    }

    /*
     * PRIMARY ASSERTION -- regression guard for #221.
     *
     * cbm_find_cli MUST return a non-empty path that contains "opencode".
     *
     * GREEN (current main, fix present): result points to the fixture file.
     * RED (if regressed to bare-name-only on Windows): result is "".
     */
    ASSERT_FALSE(result == NULL);
    ASSERT(result[0] != '\0');
    ASSERT(strstr(result, "opencode") != NULL);

    /* Cleanup fixture and temp dir. */
    (void)remove(fixture_path);
    (void)rmdir(tmpdir);

    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────────────── */
SUITE(repro_issue221) {
    RUN_TEST(repro_issue221_opencode_pathext_lookup);
}
