/*
 * repro_issue571.c — Reproduce-first case for OPEN bug #571.
 *
 * BUG: "Project name strips non-ASCII (CJK) characters from path,
 *       resulting in truncated/unrecognizable names"
 *   https://github.com/DeusData/codebase-memory-mcp/issues/571
 *
 * ROOT CAUSE (src/pipeline/fqn.c, cbm_project_name_from_path, lines ~341-348):
 *
 *   The function maps every byte that is not in [A-Za-z0-9._-] to '-':
 *
 *     unsigned char c = (unsigned char)path[i];
 *     bool safe = (c >= 'a' && c <= 'z') || ... || c == '-';
 *     if (!safe) path[i] = '-';
 *
 *   UTF-8 encodes each CJK code-point as 3 consecutive bytes, all with
 *   values >= 0x80 (> 127). Every one of those bytes fails the safe-char
 *   test and is rewritten to '-'. The subsequent dash-collapse pass then
 *   folds the run of dashes from a CJK segment into a single '-', and the
 *   leading/trailing trim can erase it entirely if it was the final segment.
 *
 *   For the exact path from the issue report:
 *     Input:  "/Users/yunxin/Desktop/开发/后端/信租风控通后端"
 *     Buggy:  "Users-yunxin-Desktop"   (all three CJK segments stripped)
 *     Correct: result MUST contain something beyond "Users-yunxin-Desktop"
 *              and MUST NOT be empty.  Whether the fix preserves the raw
 *              UTF-8 bytes ("开发"), percent-encodes them ("%E5%BC%80%E5%8F%91"),
 *              or uses another scheme is left to the implementer — this test
 *              pins the invariants:
 *                (a) non-NULL and non-empty result
 *                (b) for a path whose last segment is purely CJK, the output
 *                    is LONGER than the result produced from the ASCII-only
 *                    prefix of that same path (proving the CJK segment
 *                    contributes something rather than collapsing to nothing)
 *                (c) the result is NOT equal to the ASCII-prefix-only slug
 *                    "Users-yunxin-Desktop" that the buggy code returns
 *
 * EXPECTED vs ACTUAL:
 *   Input path : /Users/yunxin/Desktop/开发/后端/信租风控通后端
 *   Expected   : non-empty slug that encodes the CJK components somehow
 *   Actual     : "Users-yunxin-Desktop"  (CJK segments silently dropped)
 *
 *   The PRIMARY assertion — ASSERT_STR_NEQ(name, ascii_only_slug) — is RED
 *   on unpatched code because the buggy function returns exactly
 *   "Users-yunxin-Desktop", which IS the ascii_only_slug.
 *
 * DECLARATION:
 *   char *cbm_project_name_from_path(const char *abs_path);
 *   declared in  <pipeline/pipeline.h>
 */

#include "test_framework.h"
#include <pipeline/pipeline.h>

#include <stdlib.h>
#include <string.h>

/* ── Test ─────────────────────────────────────────────────────────── */

/*
 * Single test with three layered assertions (all RED on unpatched code):
 *
 *  1. Result is non-NULL and non-empty (the fallback "root" would be wrong
 *     too, but the primary bug is the silent CJK strip).
 *  2. Result is NOT equal to the ASCII-prefix-only slug.  On buggy code the
 *     function returns exactly that slug, so this fires.
 *  3. Result is strictly longer than the ASCII-prefix slug.  Any scheme that
 *     preserves CJK (raw UTF-8, percent-encoding, or even a hex dump) must
 *     produce a longer string than the stripped version.
 */
TEST(repro_issue571_cjk_project_name) {
    /*
     * Exact path from the issue report.  The last three path segments
     * (开发, 后端, 信租风控通后端) are all CJK-only; none contains any
     * ASCII byte.  The ASCII-only prefix ends at "Desktop".
     */
    static const char *cjk_path =
        "/Users/yunxin/Desktop/\xe5\xbc\x80\xe5\x8f\x91"
        "/\xe5\x90\x8e\xe7\xab\xaf"
        "/\xe4\xbf\xa1\xe7\xa7\x9f\xe9\xa3\x8e\xe6\x8e\xa7\xe9\x80\x9a\xe5\x90\x8e\xe7\xab\xaf";
    /*
     * UTF-8 bytes spelled out above:
     *   开发  = U+5F00 U+53D1 = \xe5\xbc\x80 \xe5\x8f\x91
     *   后端  = U+540E U+7AEF = \xe5\x90\x8e \xe7\xab\xaf
     *   信租风控通后端 = U+4FE1 U+79DF U+98CE U+63A7 U+901A U+540E U+7AEF
     *             = \xe4\xbf\xa1 \xe7\xa7\x9f \xe9\xa3\x8e
     *               \xe6\x8e\xa7 \xe9\x80\x9a \xe5\x90\x8e \xe7\xab\xaf
     *
     * The ASCII-only prefix slug produced by the BUGGY implementation:
     *   "Users-yunxin-Desktop"
     * This string is used in assertions 2 and 3 to prove the CJK segments
     * were silently erased.
     */
    static const char *ascii_only_slug = "Users-yunxin-Desktop";

    char *name = cbm_project_name_from_path(cjk_path);

    /* ── Assertion 1: result must exist and be non-empty ─────────── */
    /* Even on buggy code this passes (the function returns the ASCII
     * prefix rather than NULL or "root"), so it serves only as a
     * pre-condition that the function did not crash or return NULL. */
    ASSERT_NOT_NULL(name);
    ASSERT_TRUE(strlen(name) > 0);

    /* ── Assertion 2 (PRIMARY RED): CJK segments must not vanish ─── */
    /* On buggy code name == "Users-yunxin-Desktop" == ascii_only_slug.
     * After a correct fix name will encode the CJK components somehow
     * and therefore differ from the stripped ASCII slug.             */
    ASSERT_STR_NEQ(name, ascii_only_slug);

    /* ── Assertion 3 (SECONDARY RED): CJK contribution lengthens result */
    /* Any faithful encoding of the CJK bytes (raw UTF-8, percent-encode,
     * hex) is longer than the ASCII-only slug.  On buggy code
     * strlen(name) == strlen(ascii_only_slug) == 20, so this also FAILS. */
    ASSERT_TRUE(strlen(name) > strlen(ascii_only_slug));

    free(name);
    PASS();
}

/* ── Suite ────────────────────────────────────────────────────────── */
SUITE(repro_issue571) {
    RUN_TEST(repro_issue571_cjk_project_name);
}
