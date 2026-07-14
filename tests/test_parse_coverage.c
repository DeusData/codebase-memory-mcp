/*
 * test_parse_coverage.c — Reproduce-first suite for the best-effort
 * parse-coverage signal (#963, Signal A).
 *
 * ── The gap being reproduced ────────────────────────────────────────────────
 * When tree-sitter hits a construct it cannot parse (ERROR/MISSING nodes in
 * the tree), extraction silently drops every definition inside the failed
 * region — the file looks fully indexed but is not. `ts_node_has_error(root)`
 * detects this, yet nothing consumed it: CBMFileResult gained the fields
 * parse_incomplete / error_ranges / error_region_count, but the parse site in
 * cbm_extract_file_impl never sets them.
 *
 * Canonical trigger: the preprocessor-blind #ifdef-split-brace pattern in C —
 * both branches open `fn(...) {` and share ONE closing brace, so the raw text
 * is brace-unbalanced → ERROR node → the guarded function never becomes a
 * Function node while neighbors extract fine.
 *
 * ── The contract these tests enforce ────────────────────────────────────────
 *   RED  (unfixed): parse_incomplete is never set → flagged-file tests fail.
 *   GREEN (fixed):  cbm_extract_file sets parse_incomplete=true iff the tree
 *                   contains ERROR/MISSING nodes, records the 1-based line
 *                   ranges of the TOP-MOST error regions ("start-end,..."),
 *                   bounded by the 64-region cap, and clean files stay
 *                   completely unflagged (no false positives).
 *
 * BEST-EFFORT framing (must never be weakened the other way): a flag means
 * "constructs here were dropped — prefer grep"; the ABSENCE of a flag is NOT
 * a completeness guarantee. These tests only pin down the detectable class.
 */

#include "test_framework.h"
#include "cbm.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Convenience extract wrapper (same shape as test_extraction_imports.c). */
static CBMFileResult *do_extract(const char *src, CBMLanguage lang, const char *path) {
    return cbm_extract_file(src, (int)strlen(src), lang, "covproj", path, 0, NULL, NULL);
}

/* Return 1 if any extracted definition has the given short name. */
static int has_def(CBMFileResult *r, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        if (r->defs.items[i].name && strcmp(r->defs.items[i].name, name) == 0) {
            return 1;
        }
    }
    return 0;
}

/* ── Fixtures ─────────────────────────────────────────────────────────────── */

/* #ifdef-split-brace: both branches open `guarded(...) {`, one shared `}`.
 * Preprocessor-blind parse sees unbalanced braces → ERROR region around
 * lines 5–11; ok_before/ok_after remain extractable. */
static const char *C_IFDEF_SPLIT = "#include <stdio.h>\n"                      /* 1 */
                                   "\n"                                        /* 2 */
                                   "void ok_before(void) { printf(\"a\"); }\n" /* 3 */
                                   "\n"                                        /* 4 */
                                   "#ifdef FEATURE_A\n"                        /* 5 */
                                   "static int guarded(int x) {\n"             /* 6 */
                                   "#else\n"                                   /* 7 */
                                   "static int guarded_alt(int x) {\n"         /* 8 */
                                   "#endif\n"                                  /* 9 */
                                   "    return x + 1;\n"                       /* 10 */
                                   "}\n"                                       /* 11 */
                                   "\n"                                        /* 12 */
                                   "void ok_after(void) { printf(\"b\"); }\n"; /* 13 */

static const char *C_CLEAN = "#include <stdio.h>\n"
                             "\n"
                             "void alpha(void) { printf(\"a\"); }\n"
                             "\n"
                             "static int beta(int x) {\n"
                             "    return x + 1;\n"
                             "}\n";

/* `def broken(:` parses with an ERROR region, but tree-sitter error recovery
 * still yields the `broken` function def — a DEFINITELY RECOVERED miss. */
static const char *PY_BROKEN_RECOVERED = "def ok():\n"
                                         "    return 1\n"
                                         "\n"
                                         "def broken(:\n"
                                         "    pass\n"
                                         "\n"
                                         "def ok2():\n"
                                         "    return 2\n";

/* Pure operator garbage between defs: an ERROR region no def walker can
 * recover anything from — a genuine, unrecovered miss. */
static const char *PY_GARBAGE = "def ok():\n"
                                "    return 1\n"
                                "\n"
                                "%%% ((( garbage ))) %%%\n"
                                "??? !!!\n"
                                "\n"
                                "def ok2():\n"
                                "    return 2\n";

static const char *PY_CLEAN = "def ok():\n"
                              "    return 1\n"
                              "\n"
                              "def ok2():\n"
                              "    return 2\n";

/* ── Tests ────────────────────────────────────────────────────────────────── */

TEST(c_ifdef_split_brace_sets_parse_incomplete) {
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT, CBM_LANG_C, "split.c");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error); /* parse succeeded — this is the silent-partial class */
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_GTE(r->error_region_count, 1);
    ASSERT_NOT_NULL(r->error_ranges);
    ASSERT_GT((int)strlen(r->error_ranges), 0);
    cbm_free_result(r);
    PASS();
}

TEST(c_ifdef_split_brace_neighbors_still_extracted) {
    /* Documents WHY the flag matters: the file is partially indexed —
     * neighbors extract, so nothing else hints at the dropped region. */
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT, CBM_LANG_C, "split.c");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(has_def(r, "ok_before"));
    ASSERT_TRUE(r->parse_incomplete);
    cbm_free_result(r);
    PASS();
}

TEST(c_error_range_points_at_failed_region) {
    /* The recorded range must overlap the #ifdef construct (lines 5–11) so an
     * agent can be pointed at the exact unparsed region. Format is
     * "start-end[,start-end...]", 1-based, inclusive. */
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT, CBM_LANG_C, "split.c");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_NOT_NULL(r->error_ranges);
    unsigned int start = 0;
    unsigned int end = 0;
    ASSERT_EQ(sscanf(r->error_ranges, "%u-%u", &start, &end), 2);
    ASSERT_GTE(start, 1u);
    ASSERT_LTE(start, 11u); /* starts at or before the region's last line */
    ASSERT_GTE(end, 5u);    /* ends at or after the region's first line   */
    ASSERT_LTE(end, 13u);   /* never past EOF */
    ASSERT_LTE(start, end);
    cbm_free_result(r);
    PASS();
}

TEST(c_clean_file_not_flagged) {
    /* No false positives: a clean parse must stay completely unflagged. */
    CBMFileResult *r = do_extract(C_CLEAN, CBM_LANG_C, "clean.c");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_FALSE(r->parse_incomplete);
    ASSERT_EQ(r->error_region_count, 0);
    ASSERT_NULL(r->error_ranges);
    ASSERT_TRUE(has_def(r, "alpha"));
    ASSERT_TRUE(has_def(r, "beta"));
    cbm_free_result(r);
    PASS();
}

TEST(py_unrecovered_garbage_sets_parse_incomplete) {
    CBMFileResult *r = do_extract(PY_GARBAGE, CBM_LANG_PYTHON, "garbage.py");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_GTE(r->error_region_count, 1);
    ASSERT_NOT_NULL(r->error_ranges);
    ASSERT_TRUE(has_def(r, "ok")); /* partial: clean defs still extracted */
    cbm_free_result(r);
    PASS();
}

TEST(py_recovered_def_not_flagged) {
    /* Recovery subtraction: `def broken(:` produces an ERROR region, but the
     * def walker still recovers `broken` covering the whole region — the
     * construct IS in the graph, so flagging it would be a false miss. */
    CBMFileResult *r = do_extract(PY_BROKEN_RECOVERED, CBM_LANG_PYTHON, "broken.py");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(has_def(r, "broken")); /* the recovery that justifies unflagging */
    ASSERT_FALSE(r->parse_incomplete);
    ASSERT_EQ(r->error_region_count, 0);
    ASSERT_NULL(r->error_ranges);
    cbm_free_result(r);
    PASS();
}

TEST(py_clean_file_not_flagged) {
    CBMFileResult *r = do_extract(PY_CLEAN, CBM_LANG_PYTHON, "clean.py");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->parse_incomplete);
    ASSERT_EQ(r->error_region_count, 0);
    ASSERT_NULL(r->error_ranges);
    cbm_free_result(r);
    PASS();
}

TEST(error_region_cap_is_honored) {
    /* Pathological input: many separate unrecoverable garbage blocks
     * interleaved with valid defs. The collector must stay bounded by its
     * 64-region cap (matches CBM_MAX_ERROR_REGIONS in cbm.c) — pathological
     * input can't blow up the report, and the flag itself still fires. */
    enum { GARBAGE_BLOCKS = 200, LINE_CAP = 64 };
    char *src = (char *)malloc(GARBAGE_BLOCKS * 96 + 1);
    ASSERT_NOT_NULL(src);
    size_t off = 0;
    for (int i = 0; i < GARBAGE_BLOCKS; i++) {
        off += (size_t)snprintf(
            src + off, 96, "def ok%d():\n    return %d\n%%%%%% garbage%d ((( %%%%%%\n", i, i, i);
    }
    CBMFileResult *r = do_extract(src, CBM_LANG_PYTHON, "many_errors.py");
    free(src);
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_GTE(r->error_region_count, 1);
    ASSERT_LTE(r->error_region_count, LINE_CAP);
    ASSERT_NOT_NULL(r->error_ranges);
    cbm_free_result(r);
    PASS();
}

/* Trailing recovered functions AFTER the failed #ifdef region must not
 * unflag it: recovery evidence must originate INSIDE the region, and the
 * unrecovered lines (the first branch's `guarded`) keep it flagged. */
TEST(c_trailing_recovered_defs_keep_flag) {
    const char *src = "void ok_before(void) { }\n"
                      "#ifdef A\n"
                      "static int guarded(int x) {\n"
                      "#else\n"
                      "static int guarded_alt(int x) {\n"
                      "#endif\n"
                      "    return x + 1;\n"
                      "}\n"
                      "void ok_after(void) { }\n"
                      "static int nested_ok(int y) { return y; }\n";
    CBMFileResult *r = do_extract(src, CBM_LANG_C, "probe.c");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(has_def(r, "guarded_alt")); /* partial recovery inside the region */
    ASSERT_TRUE(r->parse_incomplete);       /* ...but `guarded` is still lost */
    ASSERT_GTE(r->error_region_count, 1);
    cbm_free_result(r);
    PASS();
}

/* ── Suite ────────────────────────────────────────────────────────────────── */

SUITE(parse_coverage) {
    RUN_TEST(c_ifdef_split_brace_sets_parse_incomplete);
    RUN_TEST(c_ifdef_split_brace_neighbors_still_extracted);
    RUN_TEST(c_error_range_points_at_failed_region);
    RUN_TEST(c_clean_file_not_flagged);
    RUN_TEST(py_unrecovered_garbage_sets_parse_incomplete);
    RUN_TEST(py_recovered_def_not_flagged);
    RUN_TEST(py_clean_file_not_flagged);
    RUN_TEST(error_region_cap_is_honored);
    RUN_TEST(c_trailing_recovered_defs_keep_flag);
}
