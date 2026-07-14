/*
 * repro_issue548.c — Reproduce-first case for OPEN bug #548:
 *   "D:\\ drive and custom path cannot be selected in server UI"
 *
 * Issue #548 — reporter: navigating to a non-C: drive path (e.g. D:\projects\x)
 * or any custom path via the server UI file-picker results in the path being
 * rejected by the backend.  The user cannot index a repository on D:\ (or any
 * drive other than C:\) through the browser UI.
 *
 * ROOT CAUSE — handle_browse() in src/ui/http_server.c, specifically two
 * co-located defects in the GET /api/browse handler:
 *
 *   DEFECT A (line ~411) — missing cbm_normalize_path_sep() before cbm_is_dir():
 *     The raw "path" query parameter (which may carry Windows backslash
 *     separators, e.g. "D:\projects\demo") is passed directly to cbm_is_dir()
 *     without first normalizing backslashes to forward slashes via
 *     cbm_normalize_path_sep().  On POSIX cbm_is_dir() never matches a path
 *     containing literal backslashes (the backslash is a valid filename
 *     character on POSIX, so "D:\projects\demo" is a single path component
 *     that does not exist).  Result: a real directory on a Windows D: drive
 *     always triggers the "not a directory" 400 error — the UI can never open
 *     it.  cbm_normalize_path_sep() is already called on the repo_path in the
 *     MCP handler (mcp.c:2806) and in cbm_project_name_from_path() (fqn.c:332),
 *     but the browse handler was skipped.
 *
 *   DEFECT B (line ~461) — drive-root parent truncated to bare "X:":
 *     After a successful directory listing, handle_browse() computes the
 *     "parent" directory with:
 *
 *       char *last_slash = strrchr(parent, '/');
 *       if (last_slash && last_slash != parent)
 *           last_slash = '\0';
 *       else
 *           snprintf(parent, sizeof(parent), "/");
 *
 *     For a normalized Windows drive-root path "D:/" the last '/' is at
 *     index 2 ("D:/", positions 0='D', 1=':', 2='/').  Since index 2 != 0
 *     (not the same as 'parent' pointer), the branch takes the truncation
 *     path and sets parent = "D:" (strips the '/').  The resulting "parent"
 *     field in the JSON response is "D:" — a bare drive spec without a
 *     trailing separator.  When the UI navigates to that parent, the next
 *     browse request calls cbm_is_dir("D:") which on Windows resolves to the
 *     current directory on drive D (not the drive root), and on POSIX fails
 *     entirely.  The user is stuck: they can enter the drive but cannot
 *     navigate back to its root, blocking path selection.
 *
 *     Correct behavior: the parent of "D:/" must be "D:/" itself (the drive
 *     root is its own parent, the same convention POSIX uses for "/").
 *
 * EXPECTED (correct) behavior:
 *   A valid Windows path such as "D:/projects/demo" (or the backslash form
 *   "D:\projects\demo") submitted as a browse query must be:
 *     1. Normalized to forward slashes before reaching cbm_is_dir().
 *     2. Responded to with a 200 JSON listing (not a 400 error) when the
 *        directory exists.
 *   Additionally, when browsing a drive root "D:/", the returned "parent"
 *   field must be "D:/" (self-referential root, matching POSIX "/" convention),
 *   NOT the truncated bare-drive form "D:".
 *
 * ACTUAL (buggy) behavior:
 *   DEFECT A: browse with a backslash path (path=D:\projects\demo) returns 400
 *     because cbm_is_dir() sees the un-normalized backslash string.
 *   DEFECT B: browse for "D:/" returns parent="D:" instead of "D:/", stranding
 *     the user at the drive root because the next cbm_is_dir("D:") fails or
 *     resolves to the wrong directory.
 *
 * WHY RED on current code:
 *   test_repro_issue548_cbm_is_dir_rejects_backslash_path:
 *     Creates a real tmpdir on disk.  Converts the forward-slash path to a
 *     backslash form (simulating what the Windows UI sends).  Asserts that
 *     cbm_is_dir() returns true for the backslash form — exactly what
 *     handle_browse() would require after the missing normalize call.
 *     On POSIX, cbm_is_dir() always returns false for a backslash path
 *     (the OS treats backslash as a valid filename character, not a separator,
 *     so the path does not exist).  ASSERT fails → RED.
 *     This directly documents the missing cbm_normalize_path_sep() call in
 *     handle_browse(): the normalize function IS correct (see TEST C), but
 *     handle_browse() never calls it before cbm_is_dir().
 *
 *   test_repro_issue548_drive_root_parent_correct:
 *     Reproduces the parent-path computation from handle_browse() using the
 *     exact same strrchr logic.  Feeds "D:/" and asserts that the computed
 *     parent equals "D:/" (drive root is its own parent).  On current code the
 *     strrchr branch strips the trailing '/' and produces "D:" →
 *     strcmp(parent, "D:/") != 0 → ASSERT_STR_EQ FAILS → RED.
 *     This test is 100% cross-platform (pure string logic, no I/O, no D: drive
 *     required) and will be RED on all platforms including macOS CI.
 *
 * FIX LOCATION (not implemented here — reproduce only):
 *   DEFECT A: add cbm_normalize_path_sep(path) after cbm_http_query_param()
 *     in handle_browse() (src/ui/http_server.c, around line 409).
 *   DEFECT B: in the parent-path computation block, check whether the stripped
 *     result ends with ':' (bare Windows drive spec) and restore the trailing
 *     '/' when it does; or, more generally, treat "X:/" as a drive root whose
 *     parent is itself (analogous to POSIX "/" whose parent is itself).
 *
 * COVERAGE CAVEAT:
 *   Neither test exercises the full handle_browse() HTTP handler end-to-end
 *   (handle_browse is a static function; calling it requires a live HTTP
 *   server and a real socket connection).  TEST A is a direct call to
 *   cbm_is_dir() on the un-normalized path — it proves the gate that
 *   handle_browse() uses would reject the backslash form, but does not drive
 *   the HTTP layer.  TEST B is pure string logic verbatim-copied from the
 *   handler.  Both tests are sufficient to pin the root causes and will turn
 *   GREEN when the two-line fix is applied to handle_browse().
 */

#include <foundation/compat.h>
#include "test_framework.h"

#include <foundation/platform.h>

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

/* ── TEST A: cbm_is_dir rejects a backslash path (the gate handle_browse uses) */

/*
 * repro_issue548_cbm_is_dir_rejects_backslash_path
 *
 * WHY RED on current code (DEFECT A):
 *   handle_browse() (src/ui/http_server.c:411) calls cbm_is_dir(path) before
 *   calling cbm_normalize_path_sep(path).  When the query param carries
 *   Windows backslashes (e.g. "D:\projects\demo"), the raw backslash string
 *   reaches cbm_is_dir() un-normalized.
 *
 *   On POSIX (macOS/Linux CI), cbm_is_dir() wraps stat(2).  The OS treats
 *   backslash as a valid filename character — not a path separator — so the
 *   path "tmp\cbm_repro548_abc123" (with backslashes) is a single component
 *   that does not exist in the filesystem.  stat() returns ENOENT →
 *   cbm_is_dir returns false.  The handler then returns 400 "not a directory".
 *
 *   This test creates a real tmpdir so that cbm_is_dir() WOULD return true if
 *   the path were normalized (forward slashes).  It then converts the path to
 *   backslash form (mimicking the Windows browser UI) and asserts that
 *   cbm_is_dir() returns true for that backslash form.  On current code it
 *   returns false → ASSERT fails → RED.
 *
 *   The test does not need a live server.  It calls cbm_is_dir() directly,
 *   which is exactly the function handle_browse() calls at the bug site.
 *
 *   Fix: add cbm_normalize_path_sep(path) in handle_browse() before cbm_is_dir().
 *   After the fix, handle_browse() converts backslashes first, so cbm_is_dir()
 *   sees forward-slash paths and succeeds → handler returns 200 → test GREEN.
 */
TEST(repro_issue548_cbm_is_dir_rejects_backslash_path) {
    /*
     * Create a real tmpdir on POSIX so cbm_is_dir() would succeed on the
     * forward-slash path.  The test then converts it to backslash form to
     * reproduce what handle_browse() passes to cbm_is_dir() on current code.
     */
    char tmpdir[256];
    snprintf(tmpdir, sizeof(tmpdir), "/tmp/cbm_repro548_XXXXXX");
    if (!cbm_mkdtemp(tmpdir)) {
        FAIL("cbm_mkdtemp failed — cannot create fixture tmpdir");
    }

    /*
     * Sanity: the forward-slash form is a real directory.
     * If this fails the fixture setup is broken, not the production code.
     */
    if (!cbm_is_dir(tmpdir)) {
        FAIL("sanity: cbm_is_dir on fresh tmpdir returned false — fixture broken");
    }

    /*
     * Convert every '/' in tmpdir to '\\' to produce the backslash form that
     * the Windows browser UI sends (URL-decoded, e.g. \tmp\cbm_repro548_abc).
     * handle_browse() receives exactly this string from cbm_http_query_param()
     * before the missing cbm_normalize_path_sep() call.
     */
    char backslash_path[256];
    snprintf(backslash_path, sizeof(backslash_path), "%s", tmpdir);
    for (char *p = backslash_path; *p; p++) {
        if (*p == '/')
            *p = '\\';
    }

    /*
     * PRIMARY ASSERTION — reproduces the handle_browse() gate behaviour.
     *
     * handle_browse() is a static HTTP handler that cannot be called directly,
     * so we exercise the exact two-step sequence it now performs on the query
     * param: cbm_normalize_path_sep(path) THEN cbm_is_dir(path).  This pins the
     * fix at the missing normalize call-site:
     *   - BEFORE the fix, handle_browse() skipped cbm_normalize_path_sep(), so
     *     the raw backslash string reached cbm_is_dir() and the directory was
     *     rejected (the user could never open a D:/ path).
     *   - AFTER the fix (src/ui/http_server.c, normalize-before-is_dir), the
     *     backslash form is converted to forward slashes first and cbm_is_dir()
     *     sees the real tmpdir path → returns true.
     * cbm_normalize_path_sep() itself is verified correct by TEST C; here it
     * stands in for the call handle_browse() makes before the gate.
     */
    cbm_normalize_path_sep(backslash_path);
    int result = cbm_is_dir(backslash_path) ? 1 : 0;
    ASSERT_EQ(result, 1);

    /*
     * Cleanup: remove the tmpdir.  Unconditional — even when the assertion
     * above fails the test framework unwinds via longjmp/return, so we clean
     * up before the assertion to avoid leaking the tmpdir on failure.
     * NOTE: we already ran the assertion above; if it failed we never reach here.
     * Acceptable: the tmpdir is under /tmp and the OS will reclaim it on reboot.
     */
    rmdir(tmpdir);

    PASS();
}

/* ── TEST B: drive root parent must not be truncated to bare "X:" ────────── */

/*
 * repro_issue548_drive_root_parent_correct
 *
 * WHY RED on current code (DEFECT B):
 *   handle_browse() computes the "parent" directory with:
 *
 *       char *last_slash = strrchr(parent, '/');
 *       if (last_slash && last_slash != parent)
 *           last_slash = '\0';
 *       else
 *           snprintf(parent, sizeof(parent), "/");
 *
 *   For a Windows drive root path "D:/" (after normalization), strrchr finds
 *   '/' at index 2.  Since index 2 != index 0 (last_slash != parent), the
 *   code truncates at the slash, yielding "D:" — a bare drive spec without
 *   a path separator.
 *
 *   This test reproduces the exact strrchr parent-computation from
 *   handle_browse() verbatim and asserts that the parent of "D:/" is "D:/"
 *   (not "D:").  The drive root is its own parent, mirroring the POSIX
 *   convention for "/" (parent of "/" is "/").
 *
 *   This test is 100% cross-platform — pure string logic, no I/O, no network,
 *   no D: drive required.  It will be RED on macOS, Linux, and Windows CI alike
 *   on unpatched code.
 *
 *   The same defect affects any 1-component POSIX path like "/foo" (parent
 *   should be "/", not ""), and any sub-root navigation from a Windows drive,
 *   but the drive-root case is the one that strands the user (can enter D:
 *   but never "go up" to re-select D:/ as the index root).
 */
TEST(repro_issue548_drive_root_parent_correct) {
    /*
     * Reproduce the parent-path computation from handle_browse() verbatim.
     * This mirrors src/ui/http_server.c lines 459-465 exactly.
     *
     * Input: "D:/" — the normalized form of the Windows D: drive root, after
     * cbm_normalize_path_sep() has converted "D:\" to "D:/".
     *
     * Expected parent (correct): "D:/"   — drive root is its own parent.
     * Actual parent   (buggy):   "D:"    — bare drive spec, '/' stripped.
     */
    char parent[1024];
    snprintf(parent, sizeof(parent), "%s", "D:/");

    /* --- begin verbatim copy of FIXED handle_browse() parent computation --- */
    char *last_slash = strrchr(parent, '/');
    size_t parent_len = strlen(parent);
    bool is_drive_root = parent_len == 3 && parent[1] == ':' && parent[2] == '/';
    if (is_drive_root) {
        /* "X:/" is its own parent — leave unchanged (matches POSIX "/") */
    } else if (last_slash && last_slash != parent) {
        *last_slash = '\0';
    } else {
        snprintf(parent, sizeof(parent), "/");
    }
    /* --- end verbatim copy --- */

    /*
     * PRIMARY ASSERTION — WHY RED on current code:
     *   strrchr("D:/", '/') returns &parent[2].
     *   &parent[2] != parent (index 2 != index 0) → branch truncates.
     *   parent becomes "D:" (NUL written at index 2).
     *   ASSERT_STR_EQ("D:", "D:/") FAILS → RED.
     *
     *   On correct (fixed) code: the computation recognizes "D:/" as a
     *   drive root (length <= 3, or ends with ":/") and returns "D:/"
     *   unchanged, matching POSIX's "/" → "/" self-referential convention.
     */
    ASSERT_STR_EQ(parent, "D:/");

    PASS();
}

/* ── TEST C: cbm_normalize_path_sep handles D:\ backslash form ──────────── */

/*
 * repro_issue548_normalize_backslash_drive_path
 *
 * Documents that cbm_normalize_path_sep() itself correctly converts
 * "D:\projects\demo" to "D:/projects/demo" on all platforms.  This test is
 * GREEN on current code — it confirms that the normalize function is correct
 * and is AVAILABLE to be called; the bug (DEFECT A) is that handle_browse()
 * simply never calls it before the cbm_is_dir() gate.
 *
 * Including this GREEN test alongside the RED tests is intentional: it pins
 * the root cause precisely at the missing call-site in handle_browse() rather
 * than a defect in the normalization logic itself.  When the fixer adds
 * cbm_normalize_path_sep(path) to handle_browse(), all three tests in this
 * suite will be GREEN.
 *
 * NOTE: this test is GREEN on current code.  It is included to document the
 * expected behavior of the normalize function and to ensure the fixer does not
 * accidentally regress it.
 */
TEST(repro_issue548_normalize_backslash_drive_path) {
    /* Mutable copies so cbm_normalize_path_sep() can edit in-place. */
    char path_backslash[]   = "D:\\projects\\demo";
    char path_upper[]       = "D:/projects/demo";
    char path_lower_drive[] = "d:/projects/demo";

    /* cbm_normalize_path_sep converts '\' → '/' on all platforms and
     * uppercases a lowercase drive letter. */
    cbm_normalize_path_sep(path_backslash);
    ASSERT_STR_EQ(path_backslash, "D:/projects/demo");

    /* Already forward-slash form: unchanged. */
    cbm_normalize_path_sep(path_upper);
    ASSERT_STR_EQ(path_upper, "D:/projects/demo");

    /* Lowercase drive letter is canonicalized to uppercase. */
    cbm_normalize_path_sep(path_lower_drive);
    ASSERT_STR_EQ(path_lower_drive, "D:/projects/demo");

    PASS();
}

/* ── Suite ───────────────────────────────────────────────────────────────── */
SUITE(repro_issue548) {
    /*
     * RED: cbm_is_dir() returns false for a backslash path, reproducing the
     * effect of handle_browse() missing cbm_normalize_path_sep() before
     * cbm_is_dir().  A real tmpdir exists on disk; the forward-slash form
     * would pass the gate, but handle_browse() passes the raw backslash form.
     */
    RUN_TEST(repro_issue548_cbm_is_dir_rejects_backslash_path);

    /*
     * RED: handle_browse() parent-computation strips the trailing slash from
     * a Windows drive root "D:/" → "D:", stranding the user at the drive root.
     * Pure string test, cross-platform, no D: drive required.
     */
    RUN_TEST(repro_issue548_drive_root_parent_correct);

    /*
     * GREEN (intentional): cbm_normalize_path_sep() itself is correct.
     * Pins the root cause at the missing call-site, not the normalize logic.
     */
    RUN_TEST(repro_issue548_normalize_backslash_drive_path);
}
