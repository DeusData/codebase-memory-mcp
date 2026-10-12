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

/* ── #221 regression: PATH longer than the old 4 KB copy ───────────────── */

/* Snapshot/restore one environment variable. The suite swaps PATH and the
 * Windows profile roots, and a leaked swap would poison every later suite. */
static char *repro221_save_env(const char *name) {
    const char *value = getenv(name);
    return value ? strdup(value) : NULL;
}

static void repro221_restore_env(const char *name, char *saved) {
    if (saved) {
        cbm_setenv(name, saved, 1);
        free(saved);
    } else {
        cbm_unsetenv(name);
    }
}

/*
 * repro_issue221_long_path_beyond_four_kb
 *
 * find_in_path() used to copy PATH into a fixed 4096-byte stack buffer through
 * cbm_safe_getenv(). That helper does not truncate -- it REFUSES a value it
 * cannot fit -- so on a machine whose PATH exceeds 4096 characters the whole
 * PATH was skipped and every agent on it read as "not installed". That is the
 * layout the #221 reporter had: mise, npm and scoop each append entries.
 *
 * The fixture directory is the LAST PATH entry, so any truncation of the copy
 * hides exactly the directory under test and nothing else. This case is not
 * Windows-specific -- the same 4 KB cap hid entries on macOS and Linux.
 *
 * RED before the fix (empty result), GREEN after it.
 */
TEST(repro_issue221_long_path_beyond_four_kb) {
    const char *created = th_mktempdir("repro221long");
    if (!created) {
        FAIL("th_mktempdir failed");
    }
    char tmpdir[512];
    snprintf(tmpdir, sizeof(tmpdir), "%s", created);

#ifdef _WIN32
    const char *fixture_name = "cbm-repro221-long.cmd";
    const char *fixture_body = "@echo off\r\nrem fake long-PATH agent\r\n";
    const char path_delim = ';';
#else
    const char *fixture_name = "cbm-repro221-long";
    const char *fixture_body = "#!/bin/sh\n# fake long-PATH agent\n";
    const char path_delim = ':';
#endif

    char fixture_path[1024];
    snprintf(fixture_path, sizeof(fixture_path), "%s/%s", tmpdir, fixture_name);
    if (th_write_file(fixture_path, fixture_body) != 0) {
        th_cleanup(tmpdir);
        FAIL("could not write the long-PATH fixture");
    }
    th_make_executable(fixture_path);

    /*
     * Filler entries name directories that do not exist: the scan has to walk
     * past every one of them to reach the fixture, which is appended after the
     * filler so that it always sits beyond the 4096-byte mark.
     */
    enum { FILLER_TARGET = 4600, PATH_CAP = FILLER_TARGET + 1024 };
    char long_path[PATH_CAP];
    size_t used = 0U;
    while (used < (size_t)FILLER_TARGET) {
        int written = snprintf(long_path + used, sizeof(long_path) - used,
                               "/nonexistent-repro221-filler%05zu%c", used, path_delim);
        if (written <= 0 || (size_t)written >= sizeof(long_path) - used) {
            th_cleanup(tmpdir);
            FAIL("could not build the long PATH filler");
        }
        used += (size_t)written;
    }
    ASSERT_GT(used, 4096);
    int tail = snprintf(long_path + used, sizeof(long_path) - used, "%s", tmpdir);
    if (tail <= 0 || (size_t)tail >= sizeof(long_path) - used) {
        th_cleanup(tmpdir);
        FAIL("could not append the fixture directory to the long PATH");
    }

    char *saved_path = repro221_save_env("PATH");
    cbm_setenv("PATH", long_path, 1);

    /* A home that does not exist keeps the PATH-independent fallbacks out of
     * the way: the fixture's directory is the only place that can satisfy it. */
    const char *result = cbm_find_cli("cbm-repro221-long", "/nonexistent-repro221-home");

    repro221_restore_env("PATH", saved_path);

    ASSERT_NOT_NULL(result);
    ASSERT(result[0] != '\0');
    ASSERT(strncmp(result, tmpdir, strlen(tmpdir)) == 0);
    ASSERT(strstr(result, "cbm-repro221-long") != NULL);

    th_cleanup(tmpdir);
    PASS();
}

/* ── #221 regression: the Windows fallback shim locations ───────────────── */

/*
 * repro_issue221_windows_fallback_shims
 *
 * An agent the PATH scan cannot see must still be found where Windows actually
 * puts CLIs: %APPDATA%\npm (npm's global bin), %LOCALAPPDATA%\mise\shims,
 * %USERPROFILE%\scoop\shims, plus the pipx and cargo homes (~\.local\bin,
 * ~\.cargo\bin) that were already probed before #221 passed and must not be
 * dropped by the fix.
 *
 * Each location is reached through its own seam -- APPDATA / LOCALAPPDATA, or
 * the home_dir argument -- so this never reads the developer's real profile.
 * Every shim carries a .cmd extension, which is why the fallback probe needs
 * the same PATHEXT treatment the PATH scan got.
 *
 * Windows-only: the directory layout it asserts is the one Windows installers
 * produce. */
TEST(repro_issue221_windows_fallback_shims) {
#ifndef _WIN32
    SKIP_PLATFORM("Windows-only: npm/mise/scoop shim fallback layout");
#else
    const char *created = th_mktempdir("repro221fb");
    if (!created) {
        FAIL("th_mktempdir failed");
    }
    char root[512];
    snprintf(root, sizeof(root), "%s", created);

    char *saved_appdata = repro221_save_env("APPDATA");
    char *saved_localappdata = repro221_save_env("LOCALAPPDATA");
    char *saved_userprofile = repro221_save_env("USERPROFILE");
    char *saved_path = repro221_save_env("PATH");

    /* APPDATA/LOCALAPPDATA/USERPROFILE all point at the fixture root. The
     * scoop, pipx and cargo entries are built from the home_dir ARGUMENT, so
     * USERPROFILE here mirrors what a real profile would supply. */
    cbm_setenv("APPDATA", root, 1);
    cbm_setenv("LOCALAPPDATA", root, 1);
    cbm_setenv("USERPROFILE", root, 1);
    cbm_setenv("PATH", "/nonexistent-repro221-path", 1);

    /* One shim at a time, so the returned path proves which entry matched
     * rather than merely that something did. Each case removes its shim before
     * the next one runs, so the earlier entries cannot shadow the later ones. */
    const struct {
        const char *subdir;
        const char *label;
    } cases[] = {
        {".local/bin", "pipx home"},         {".cargo/bin", "cargo home"},  {"npm", "APPDATA npm"},
        {"mise/shims", "LOCALAPPDATA mise"}, {"scoop/shims", "scoop home"},
    };
    const char *shim_name = "cbm-repro221-fb.cmd";
    const size_t case_count = sizeof(cases) / sizeof(cases[0]);

    for (size_t i = 0; i < case_count; i++) {
        char dir[512];
        snprintf(dir, sizeof(dir), "%s/%s", root, cases[i].subdir);
        if (th_mkdir_p(dir) != 0) {
            repro221_restore_env("PATH", saved_path);
            repro221_restore_env("USERPROFILE", saved_userprofile);
            repro221_restore_env("LOCALAPPDATA", saved_localappdata);
            repro221_restore_env("APPDATA", saved_appdata);
            th_cleanup(root);
            FAIL("could not create the fallback fixture directory");
        }
        char shim[1024];
        snprintf(shim, sizeof(shim), "%s/%s", dir, shim_name);
        if (th_write_file(shim, "@echo off\r\nrem fake fallback shim\r\n") != 0) {
            repro221_restore_env("PATH", saved_path);
            repro221_restore_env("USERPROFILE", saved_userprofile);
            repro221_restore_env("LOCALAPPDATA", saved_localappdata);
            repro221_restore_env("APPDATA", saved_appdata);
            th_cleanup(root);
            FAIL("could not write the fallback shim");
        }

        const char *result = cbm_find_cli("cbm-repro221-fb", root);
        char expected_prefix[512];
        snprintf(expected_prefix, sizeof(expected_prefix), "%s/%s/", root, cases[i].subdir);
        bool matched = result != NULL && result[0] != '\0' &&
                       strncmp(result, expected_prefix, strlen(expected_prefix)) == 0;
        (void)remove(shim);

        if (!matched) {
            printf("  fallback case %zu (%s) did not resolve in %s\n", i, cases[i].label, dir);
            repro221_restore_env("PATH", saved_path);
            repro221_restore_env("USERPROFILE", saved_userprofile);
            repro221_restore_env("LOCALAPPDATA", saved_localappdata);
            repro221_restore_env("APPDATA", saved_appdata);
            th_cleanup(root);
            FAIL("a Windows fallback shim location was not probed");
        }
    }

    repro221_restore_env("PATH", saved_path);
    repro221_restore_env("USERPROFILE", saved_userprofile);
    repro221_restore_env("LOCALAPPDATA", saved_localappdata);
    repro221_restore_env("APPDATA", saved_appdata);

    th_cleanup(root);
    PASS();
#endif
}

/* ── Suite ──────────────────────────────────────────────────────────────── */
SUITE(repro_issue221) {
    RUN_TEST(repro_issue221_opencode_pathext_lookup);
    RUN_TEST(repro_issue221_long_path_beyond_four_kb);
    RUN_TEST(repro_issue221_windows_fallback_shims);
}
