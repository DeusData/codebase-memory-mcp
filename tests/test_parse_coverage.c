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
 *                   bounded by the 256-region cap, and clean files stay
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
#include <ctype.h>

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

/* #1610 fixtures follow. Refinement fixtures live here so they sit beside the
 * split-brace fixture they build on. */

/* Same split-brace shape as C_IFDEF_SPLIT, plus real garbage further down.
 * Guards against over-suppression: the preprocessor explains the guarded
 * region but explains nothing about the garbage, so BOTH must stay flagged
 * and they must be reported as two separate ranges, not one big one. */
static const char *C_IFDEF_SPLIT_PLUS_GARBAGE = "#include <stdio.h>\n"           /* 1 */
                                                "\n"                            /* 2 */
                                                "void ok_before(void) { }\n"    /* 3 */
                                                "\n"                            /* 4 */
                                                "#ifdef FEATURE_A\n"            /* 5 */
                                                "static int guarded(int x) {\n" /* 6 */
                                                "#else\n"                       /* 7 */
                                                "static int guarded_alt(int x) {\n" /* 8 */
                                                "#endif\n"                      /* 9 */
                                                "    return x + 1;\n"           /* 10 */
                                                "}\n"                           /* 11 */
                                                "\n"                            /* 12 */
                                                "%%% ((( &&& ))) %%%\n"         /* 13 */
                                                "\n"                            /* 14 */
                                                "void ok_after(void) { }\n";    /* 15 */

/* Perl formats have a line-oriented body terminated by a lone dot.  The
 * following named sub pins the important recovery boundary: a grammar must
 * both accept the format and resume normal declaration parsing afterwards. */
static const char *PERL_FORMAT_WITH_FOLLOWING_SUB = "package Report;\n"
                                                    "format REPORT =\n"
                                                    "@<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"
                                                    "$headline\n"
                                                    ".\n"
                                                    "sub after_format { return 1; }\n";

/* A grammar refresh must not hide real Perl syntax loss. */
static const char *PERL_MALFORMED = "package Broken;\n"
                                    "sub before_error { return 1; }\n"
                                    "} ] } ]\n"
                                    "sub after_error { return 2; }\n";

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

/* Read the trailing "+<N>" truncation marker off a range string. Returns N, or
 * 0 when the string carries no marker. */
static int ranges_dropped_marker(const char *ranges) {
    const char *plus = ranges ? strrchr(ranges, '+') : NULL;
    if (!plus || !isdigit((unsigned char)plus[1])) {
        return 0;
    }
    return atoi(plus + 1);
}

TEST(error_region_cap_is_honored) {
    /* Pathological input: many separate unrecoverable garbage blocks
     * interleaved with valid defs. The collector must stay bounded by its
     * 256-region cap (matches CBM_MAX_ERROR_REGIONS in cbm.c) — pathological
     * input can't blow up the report, and the flag itself still fires. */
    enum { GARBAGE_BLOCKS = 400, LINE_CAP = 256 };
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

/* A clipped range list must say so. 400 garbage blocks overrun the 256-region
 * cap, so the report keeps 256 ranges and ends with a "+<N>" marker naming the
 * number thrown away. Without the marker the short list reads as a complete
 * one, which is the whole defect this guards. */
TEST(error_region_cap_reports_what_it_dropped) {
    enum { GARBAGE_BLOCKS = 400, LINE_CAP = 256 };
    char *src = (char *)malloc(GARBAGE_BLOCKS * 96 + 1);
    ASSERT_NOT_NULL(src);
    size_t off = 0;
    for (int i = 0; i < GARBAGE_BLOCKS; i++) {
        off += (size_t)snprintf(
            src + off, 96, "def ok%d():\n    return %d\n%%%%%% garbage%d ((( %%%%%%\n", i, i, i);
    }
    CBMFileResult *r = do_extract(src, CBM_LANG_PYTHON, "cap_marker.py");
    free(src);
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->error_ranges);
    /* The cap bound, so the kept list is full and the marker is present. */
    ASSERT_EQ(r->error_region_count, LINE_CAP);
    int dropped = ranges_dropped_marker(r->error_ranges);
    ASSERT_GTE(dropped, 1);
    /* Every block produces at most one region, so the total cannot exceed the
     * number of blocks — a marker that overcounts would fail here. */
    ASSERT_LTE(r->error_region_count + dropped, GARBAGE_BLOCKS);
    /* The marker is a SUFFIX: nothing follows it, or a reader stops early and
     * silently loses every range after it. */
    const char *plus = strrchr(r->error_ranges, '+');
    ASSERT_NOT_NULL(plus);
    for (const char *c = plus + 1; *c; c++) {
        ASSERT_TRUE(isdigit((unsigned char)*c));
    }
    cbm_free_result(r);
    PASS();
}

/* Inverse guard: a file that stays under the cap must carry NO marker, or
 * every ordinary report would look clipped. */
TEST(uncapped_ranges_carry_no_marker) {
    const char *src = "def ok():\n    return 1\n%%% garbage (((\ndef ok2():\n    return 2\n";
    CBMFileResult *r = do_extract(src, CBM_LANG_PYTHON, "small.py");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_NOT_NULL(r->error_ranges);
    ASSERT_EQ(ranges_dropped_marker(r->error_ranges), 0);
    ASSERT_NULL(strchr(r->error_ranges, '+'));
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

/* ── #1610: a missing FINAL NEWLINE is not a parse failure ────────────────────
 *
 * A file that does not end with "\n" leaves the grammar's mandatory line
 * terminator MISSING. That node is ZERO-WIDTH and sits at EOF: the parser
 * consumed no source for it, so by construction nothing was dropped — no
 * construct can live in a zero-byte span. Every instruction still parses.
 *
 * Reported on #1610 for Dockerfile, where a reporter proved with a byte-exact
 * matrix that the trigger is independent of BOM, CRLF/LF, exec-form vs
 * shell-form and file length — it is purely the absent final newline.
 *
 * It was never Dockerfile-specific: tcl, fish, gomod and hyprlang flag the same
 * way, while ini, fsharp, beancount and others do NOT — only because those
 * grammars declare the terminator token hidden rather than visible. Whether a
 * user saw a phantom parse_partial came down to a grammar-authoring accident.
 *
 * The cost was not cosmetic: a phantom flag writes a "<project>::missed" shadow
 * row, and until #1609 that row removed the whole project from cross-repo
 * linking, as source AND as target. */
TEST(dockerfile_missing_final_newline_not_flagged_issue1610) {
    const char *src = "FROM mcr.microsoft.com/dotnet/aspnet:8.0\n"
                      "ENTRYPOINT [\"dotnet\", \"App.dll\"]"; /* deliberately no \n */
    CBMFileResult *r = do_extract(src, CBM_LANG_DOCKERFILE, "Dockerfile");
    ASSERT_NOT_NULL(r);
    bool flagged = r->parse_incomplete;
    cbm_free_result(r);
    if (flagged) {
        FAIL("a Dockerfile lacking only its final newline must not be parse_partial");
    }
    PASS();
}

/* The same bytes WITH the newline must stay clean — pins the equivalence the
 * reporter's matrix proved, so a future change cannot "fix" one by breaking the
 * other. */
TEST(dockerfile_with_final_newline_still_clean_issue1610) {
    const char *src = "FROM mcr.microsoft.com/dotnet/aspnet:8.0\n"
                      "ENTRYPOINT [\"dotnet\", \"App.dll\"]\n";
    CBMFileResult *r = do_extract(src, CBM_LANG_DOCKERFILE, "Dockerfile");
    ASSERT_NOT_NULL(r);
    bool flagged = r->parse_incomplete;
    cbm_free_result(r);
    if (flagged) {
        FAIL("a terminated Dockerfile must not be parse_partial");
    }
    PASS();
}

/* #1746: on Windows, a Dockerfile whose final instruction is followed by one
 * ASCII space and then EOF was reported as parse_partial. */
TEST(dockerfile_trailing_space_at_eof_not_flagged_issue1746) {
    const char *src = "FROM scratch\nENTRYPOINT [\"a\"] ";
    CBMFileResult *r = do_extract(src, CBM_LANG_DOCKERFILE, "Dockerfile");
    ASSERT_NOT_NULL(r);
    bool flagged = r->parse_incomplete;
    if (flagged) {
        fprintf(stderr, "  exact issue #1746 fixture flagged: ranges=%s\n",
                r->error_ranges ? r->error_ranges : "(none)");
    }
    cbm_free_result(r);
    if (flagged) {
        FAIL("trailing horizontal whitespace at Dockerfile EOF must not be parse_partial");
    }
    PASS();
}

/* The same bytes WITH the LF must stay clean. This is a separate test so the
 * control executes even while the exact affected fixture is RED. */
TEST(dockerfile_trailing_space_with_final_newline_clean_issue1746) {
    const char *src = "FROM scratch\nENTRYPOINT [\"a\"] \n";
    CBMFileResult *r = do_extract(src, CBM_LANG_DOCKERFILE, "Dockerfile");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->parse_incomplete);
    cbm_free_result(r);
    PASS();
}

/* Removing the trailing space while retaining EOF must also stay clean. */
TEST(dockerfile_without_trailing_space_at_eof_clean_issue1746) {
    const char *src = "FROM scratch\nENTRYPOINT [\"a\"]";
    CBMFileResult *r = do_extract(src, CBM_LANG_DOCKERFILE, "Dockerfile");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->parse_incomplete);
    cbm_free_result(r);
    PASS();
}

/* The reporter also observed the same failure when the first line uses CRLF. */
TEST(dockerfile_crlf_trailing_space_at_eof_not_flagged_issue1746) {
    const char *src = "FROM scratch\r\nENTRYPOINT [\"a\"] ";
    CBMFileResult *r = do_extract(src, CBM_LANG_DOCKERFILE, "Dockerfile");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->parse_incomplete);
    cbm_free_result(r);
    PASS();
}

/* Language-general, not a Dockerfile patch: these four were each proven to flag
 * on a stripped trailing newline. */
TEST(missing_final_newline_not_flagged_across_grammars_issue1610) {
    struct {
        const char *src;
        CBMLanguage lang;
        const char *path;
    } cases[] = {
        {"proc foo {} {}\nproc bar {} {}", CBM_LANG_TCL, "a.tcl"},
        {"function foo\n  echo hi\nend", CBM_LANG_FISH, "a.fish"},
        {"module example.com/m\n\ngo 1.21", CBM_LANG_GOMOD, "go.mod"},
        {"general {\n  gaps_in = 5\n}", CBM_LANG_HYPRLANG, "hypr.conf"},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CBMFileResult *r = do_extract(cases[i].src, cases[i].lang, cases[i].path);
        ASSERT_NOT_NULL(r);
        bool flagged = r->parse_incomplete;
        if (flagged) {
            fprintf(stderr, "  %s flagged: ranges=%s\n", cases[i].path,
                    r->error_ranges ? r->error_ranges : "(none)");
        }
        cbm_free_result(r);
        if (flagged) {
            FAIL("an unterminated final line must not be parse_partial in any grammar");
        }
    }
    PASS();
}

/* GUARD (the reason this suppression is safe rather than convenient): the rule
 * is ZERO-WIDTH AT EOF only. A real failure earlier in the file must still be
 * reported, and its range must name the broken line — not be swallowed along
 * with the terminator. */
TEST(real_error_before_eof_still_flagged_without_final_newline_issue1610) {
    /* Built from C_IFDEF_SPLIT, the fixture this suite already proves is
     * flagged, with its trailing newline removed. Two conditions now hold at
     * once: a genuine width-bearing ERROR mid-file, AND an unterminated last
     * line. Suppressing the EOF terminator must not swallow the real one. */
    size_t n = strlen(C_IFDEF_SPLIT);
    char *unterminated = (char *)malloc(n + 1);
    ASSERT_NOT_NULL(unterminated);
    memcpy(unterminated, C_IFDEF_SPLIT, n);
    unterminated[n - 1] = '\0'; /* drop the final newline */

    CBMFileResult *r = do_extract(unterminated, CBM_LANG_C, "split.c");
    free(unterminated);
    ASSERT_NOT_NULL(r);
    bool flagged = r->parse_incomplete;
    bool has_ranges = r->error_ranges != NULL;
    cbm_free_result(r);
    if (!flagged) {
        FAIL("a real mid-file parse failure must still be reported when the file also lacks its final newline");
    }
    if (!has_ranges) {
        FAIL("a reported failure must still name its line range");
    }
    PASS();
}

/* GUARD: a MISSING/ERROR node WITH WIDTH at EOF is a genuine loss and must
 * still be flagged. A Makefile whose final recipe line lacks its newline really
 * does drop the recipe from the tree — cbm's flag is honest there. */
TEST(width_bearing_error_at_eof_still_flagged_issue1610) {
    const char *src = "all:\n\techo hi"; /* no trailing newline; recipe is lost */
    CBMFileResult *r = do_extract(src, CBM_LANG_MAKEFILE, "Makefile");
    ASSERT_NOT_NULL(r);
    bool flagged = r->parse_incomplete;
    cbm_free_result(r);
    if (!flagged) {
        FAIL("a width-bearing parse failure at EOF must still be reported");
    }
    PASS();
}

/* #1838: tree-sitter-perl v1.0.0 rejects a valid line-oriented format and
 * reports the file as partial.  The supported upstream v1.2.1 grammar accepts
 * the format and preserves declaration extraction beyond its dot terminator. */
TEST(perl_format_followed_by_named_sub_is_complete_issue1838) {
    CBMFileResult *r = do_extract(PERL_FORMAT_WITH_FOLLOWING_SUB, CBM_LANG_PERL, "report.pl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    bool partial = r->parse_incomplete;
    int error_regions = r->error_region_count;
    bool has_error_ranges = r->error_ranges != NULL;
    bool has_following_sub = has_def(r, "after_format");
    if (partial || error_regions != 0 || has_error_ranges || !has_following_sub) {
        fprintf(stderr,
                "  Perl format result: partial=%d regions=%d ranges=%s following_sub=%d\n",
                partial,
                error_regions,
                r->error_ranges ? r->error_ranges : "(none)",
                has_following_sub);
        cbm_free_result(r);
        FAIL("a valid Perl format and its following named sub must parse completely");
    }
    cbm_free_result(r);
    PASS();
}

TEST(perl_malformed_source_remains_partial_issue1838) {
    CBMFileResult *r = do_extract(PERL_MALFORMED, CBM_LANG_PERL, "broken.pl");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_GTE(r->error_region_count, 1);
    ASSERT_NOT_NULL(r->error_ranges);
    cbm_free_result(r);
    PASS();
}

/* ── #1746: trailing blanks before EOF are still just an absent newline ───────
 *
 * #1610 suppressed the zero-width MISSING terminator but tested for it with
 * `end == source_len`. Trailing blanks are extras owned by no node, so
 * `ENTRYPOINT ["a"] ` + EOF parks it at [29,29) while source_len is 30.
 *
 * The reporter's byte-exact controls pin the trigger to the PAIR: `] ` + EOF
 * flags, `] ` + newline is clean, `]` + EOF is clean. */
TEST(dockerfile_trailing_blank_at_eof_not_flagged_issue1746) {
    const char *cases[] = {
        "FROM scratch\nENTRYPOINT [\"a\"] ",     /* space + EOF — the report */
        "FROM scratch\nENTRYPOINT [\"a\"]\t",    /* tab + EOF */
        "FROM scratch\nENTRYPOINT [\"a\"]\v",    /* vertical tab + EOF */
        "FROM scratch\nENTRYPOINT [\"a\"]\f",    /* form feed + EOF */
        "FROM scratch\nENTRYPOINT [\"a\"]  \t ", /* run of blanks + EOF */
        "FROM scratch\nENTRYPOINT [\"a\"] \r",   /* CRLF file truncated to CR */
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CBMFileResult *r = do_extract(cases[i], CBM_LANG_DOCKERFILE, "Dockerfile");
        ASSERT_NOT_NULL(r);
        bool flagged = r->parse_incomplete;
        if (flagged) {
            fprintf(stderr, "  case %zu flagged: ranges=%s\n", i,
                    r->error_ranges ? r->error_ranges : "(none)");
        }
        cbm_free_result(r);
        if (flagged) {
            FAIL("trailing blanks must not turn an absent final newline into parse_partial");
        }
    }
    PASS();
}

/* GUARD: widening the tail must not swallow a genuine mid-file failure just
 * because the file happens to end in blanks. */
TEST(real_error_before_eof_still_flagged_with_trailing_blank_issue1746) {
    size_t n = strlen(C_IFDEF_SPLIT);
    char *buf = (char *)malloc(n + 1);
    ASSERT_NOT_NULL(buf);
    memcpy(buf, C_IFDEF_SPLIT, n + 1);
    buf[n - 1] = ' '; /* final newline becomes a blank */

    CBMFileResult *r = do_extract(buf, CBM_LANG_C, "split.c");
    free(buf);
    ASSERT_NOT_NULL(r);
    bool flagged = r->parse_incomplete;
    bool has_ranges = r->error_ranges != NULL;
    cbm_free_result(r);
    if (!flagged) {
        FAIL("a real mid-file parse failure must still be reported when the file ends in blanks");
    }
    if (!has_ranges) {
        FAIL("a reported failure must still name its line range");
    }
    PASS();
}

/* GUARD: a WIDTH-BEARING loss at EOF stays honest with a blank tail too — the
 * Makefile recipe really is dropped, and only zero-width nodes are excused. */
TEST(width_bearing_error_at_eof_still_flagged_with_trailing_blank_issue1746) {
    const char *src = "all:\n\techo hi ";
    CBMFileResult *r = do_extract(src, CBM_LANG_MAKEFILE, "Makefile");
    ASSERT_NOT_NULL(r);
    bool flagged = r->parse_incomplete;
    cbm_free_result(r);
    if (!flagged) {
        FAIL("a width-bearing parse failure at EOF must still be reported when the file ends in blanks");
    }
    PASS();
}

/* ── Phase 2: refine raw ranges with the preprocessed tree ──────────────────
 *
 * The raw parse sees both #ifdef branches at once, so its ERROR node covers
 * the whole guarded construct (lines 5-11). The PREPROCESSED parse sees only
 * the branch the preprocessor picked, and parses it clean. Every original
 * line that shows up clean in that second parse is therefore accounted for,
 * and reporting it as unparsed is false.
 *
 * What is left is the branch the preprocessor threw away — line 6 here. That
 * one really is missing from the graph, so it stays flagged. Directive lines
 * (#ifdef / #else / #endif) hold no construct, so a range never starts or
 * ends on one.
 */

/* Return 1 if the "a-b,c-d" range string covers 1-based `line`. */
static int ranges_cover_line(const char *ranges, unsigned int line) {
    const char *p = ranges;
    while (p && *p) {
        unsigned int s = 0, e = 0;
        if (sscanf(p, "%u-%u", &s, &e) == 2 && line >= s && line <= e) {
            return 1;
        }
        p = strchr(p, ',');
        if (p) {
            p++;
        }
    }
    return 0;
}

/* Total lines covered by every range in the string. */
static unsigned int ranges_total_span(const char *ranges) {
    const char *p = ranges;
    unsigned int total = 0;
    while (p && *p) {
        unsigned int s = 0, e = 0;
        if (sscanf(p, "%u-%u", &s, &e) == 2 && e >= s) {
            total += e - s + 1;
        }
        p = strchr(p, ',');
        if (p) {
            p++;
        }
    }
    return total;
}

TEST(c_ifdef_split_range_narrows_to_dropped_branch) {
    /* RED before the refinement: the raw range covers the whole 5-11
     * construct. GREEN after: only line 6, the branch the preprocessor did
     * not pick, is still reported. */
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT, CBM_LANG_C, "split.c");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_NOT_NULL(r->error_ranges);
    ASSERT_TRUE(ranges_cover_line(r->error_ranges, 6u)); /* dropped branch */
    ASSERT_LTE(ranges_total_span(r->error_ranges), 3u);  /* was 7 lines */
    cbm_free_result(r);
    PASS();
}

TEST(c_ifdef_split_range_excludes_lines_the_preprocessor_explained) {
    /* Lines 10 and 11 are the shared body and closing brace. They parse
     * clean once a branch is chosen, so pointing an agent at them is wrong. */
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT, CBM_LANG_C, "split.c");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->error_ranges);
    ASSERT_FALSE(ranges_cover_line(r->error_ranges, 10u));
    ASSERT_FALSE(ranges_cover_line(r->error_ranges, 11u));
    ASSERT_FALSE(ranges_cover_line(r->error_ranges, 3u)); /* ok_before */
    cbm_free_result(r);
    PASS();
}

TEST(c_ifdef_split_range_never_starts_on_a_directive) {
    /* Lines 5, 7 and 9 are bare #ifdef / #else / #endif. No construct can
     * live on them, so they must not appear in a range. */
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT, CBM_LANG_C, "split.c");
    ASSERT_NOT_NULL(r);
    ASSERT_NOT_NULL(r->error_ranges);
    ASSERT_FALSE(ranges_cover_line(r->error_ranges, 5u));
    ASSERT_FALSE(ranges_cover_line(r->error_ranges, 7u));
    ASSERT_FALSE(ranges_cover_line(r->error_ranges, 9u));
    cbm_free_result(r);
    PASS();
}

TEST(c_refinement_does_not_suppress_real_garbage) {
    /* Anti-over-suppression. The preprocessor cannot explain line 13, so it
     * stays flagged even though the guarded region above it narrowed. */
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT_PLUS_GARBAGE, CBM_LANG_C, "both.c");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_NOT_NULL(r->error_ranges);
    ASSERT_TRUE(ranges_cover_line(r->error_ranges, 13u)); /* the garbage */
    ASSERT_FALSE(ranges_cover_line(r->error_ranges, 3u)); /* ok_before */
    cbm_free_result(r);
    PASS();
}

TEST(c_clean_file_stays_unflagged_after_refinement) {
    /* The refinement must never invent a range on a file that parses. */
    CBMFileResult *r = do_extract(C_CLEAN, CBM_LANG_C, "clean.c");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->parse_incomplete);
    ASSERT_NULL(r->error_ranges);
    cbm_free_result(r);
    PASS();
}


/* The whole-file class, and the reason the parse_unusable kind exists.
 *
 * The Phase 2 refinement that narrows a whole-file range using the
 * preprocessed parse only runs for C, C++ and CUDA. A Python file whose root
 * node is ERROR gets no such help, so it still reports one range covering
 * every line — and one range over 80% of a file is not advice worth printing. */
TEST(python_whole_file_error_is_unusable) {
    const char *src = ")))\n((( \n]]] [[[\ndef x(:\n";
    CBMFileResult *r = do_extract(src, CBM_LANG_PYTHON, "unparseable.py");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_TRUE(r->parse_unusable);
    ASSERT_EQ(r->error_region_count, 1);
    ASSERT_NOT_NULL(r->error_ranges);
    cbm_free_result(r);
    PASS();
}

/* Inverse guard, and the one that keeps the kind meaningful: a file with a
 * real but LOCAL parse failure must stay parse_partial. If this flipped, every
 * flagged file would say "read the source" and the ranges would stop earning
 * their keep. */
TEST(local_error_stays_partial_not_unusable) {
    const char *src = "def ok():\n    return 1\n%%% garbage (((\ndef ok2():\n    return 2\n"
                      "def ok3():\n    return 3\ndef ok4():\n    return 4\n"
                      "def ok5():\n    return 5\ndef ok6():\n    return 6\n";
    CBMFileResult *r = do_extract(src, CBM_LANG_PYTHON, "local_error.py");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_FALSE(r->parse_unusable);
    cbm_free_result(r);
    PASS();
}

/* A clean file is neither. */
TEST(clean_file_is_neither_partial_nor_unusable) {
    CBMFileResult *r = do_extract(C_CLEAN, CBM_LANG_C, "clean_kinds.c");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->parse_incomplete);
    ASSERT_FALSE(r->parse_unusable);
    cbm_free_result(r);
    PASS();
}

/* The C file that started this work must NOT land in the unusable class. Its
 * whole-file range is exactly what Phase 2 broke up, so if this ever flips
 * back to true the refinement has stopped working. */
TEST(c_ifdef_split_is_partial_never_unusable) {
    CBMFileResult *r = do_extract(C_IFDEF_SPLIT, CBM_LANG_C, "split_kind.c");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_FALSE(r->parse_unusable);
    cbm_free_result(r);
    PASS();
}

/* Phase 0 finding 2, pinned so a tree-sitter bump cannot change it quietly.
 *
 * The C grammar handles `_Thread_local` unevenly, and these are the three
 * forms measured on the grammar shipped today:
 *
 *   static _Thread_local int x = 0;   parses clean
 *   static _Thread_local int *p;      parses clean
 *   static _Thread_local char b[8];   fails — flagged as range 1-1
 *
 * The array line really is missing from the graph, so flagging it is the
 * honest answer, not a false positive. This test exists to make a grammar
 * bump visible: if a newer grammar fixes the array form, this goes red and
 * says so, instead of leaving a wrong note in the plan. (The plan's Phase 0
 * also listed the pointer form as failing. It does not fail today.) */
TEST(c_thread_local_grammar_limit_is_pinned_issue963) {
    CBMFileResult *ok = do_extract("static _Thread_local int x = 0;\n"
                                   "void f(void) { x = 1; }\n",
                                   CBM_LANG_C, "tls_init.c");
    ASSERT_NOT_NULL(ok);
    ASSERT_FALSE(ok->parse_incomplete);
    cbm_free_result(ok);

    CBMFileResult *ptr = do_extract("static _Thread_local int *p;\n"
                                    "void f(void) { p = 0; }\n",
                                    CBM_LANG_C, "tls_ptr.c");
    ASSERT_NOT_NULL(ptr);
    ASSERT_FALSE(ptr->parse_incomplete);
    cbm_free_result(ptr);

    CBMFileResult *arr = do_extract("static _Thread_local char b[8];\n"
                                    "void f(void) { b[0] = 0; }\n",
                                    CBM_LANG_C, "tls_arr.c");
    ASSERT_NOT_NULL(arr);
    ASSERT_TRUE(arr->parse_incomplete);
    ASSERT_NOT_NULL(arr->error_ranges);
    /* The range names the one broken line, not the whole file. */
    ASSERT_STR_EQ("1-1", arr->error_ranges);
    /* The clean function below it still reaches the graph. */
    ASSERT_TRUE(has_def(arr, "f"));
    cbm_free_result(arr);
    PASS();
}

/* Two error nodes can sit on ONE line. Line 113 of scripts/setup-windows.ps1
 * does exactly that, and the report used to read "113-113,113-113" — the same
 * line named twice. A line range says nothing new the second time, so repeated
 * or overlapping regions must collapse into one. */
static const char *PS_TWO_ERRORS_ONE_LINE = "Write-Host \"start\"\n"             /* 1 */
                                            "wsl.exe -- bash -c $Command 2>&1\n" /* 2 */
                                            "Write-Host \"end\"\n";              /* 3 */

/* An error region that runs to the end of the file stops just after the last
 * newline. Tree-sitter calls that position row N, column 0 — a row that holds
 * no text. Reading it as a line number named a line past the end of the file:
 * scripts/setup-windows.ps1 has 326 lines and the report said "245-327". */
static const char *PS_ERROR_TO_EOF = "} else {\n"             /* 1 */
                                     "    if ($a) {\n"        /* 2 */
                                     "        Write-Host x\n" /* 3 */
                                     "}\n";                   /* 4 */

TEST(coverage_repeated_error_line_reports_one_range_issue963) {
    CBMFileResult *r =
        cbm_extract_file(PS_TWO_ERRORS_ONE_LINE, (int)strlen(PS_TWO_ERRORS_ONE_LINE),
                         CBM_LANG_POWERSHELL, "covproj", "two_errors.ps1", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_NOT_NULL(r->error_ranges);
    /* Line 2 carries two separate error nodes. It must be named once. */
    ASSERT_STR_EQ(r->error_ranges, "2-2");
    ASSERT_EQ(r->error_region_count, 1);
    cbm_free_result(r);
    PASS();
}

TEST(coverage_range_never_ends_past_the_last_line_issue963) {
    int len = (int)strlen(PS_ERROR_TO_EOF);
    CBMFileResult *r = cbm_extract_file(PS_ERROR_TO_EOF, len, CBM_LANG_POWERSHELL, "covproj",
                                        "error_to_eof.ps1", 0, NULL, NULL);
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(r->parse_incomplete);
    ASSERT_NOT_NULL(r->error_ranges);
    /* The file has four lines and ends with a newline. Line 5 does not exist. */
    ASSERT_STR_EQ(r->error_ranges, "1-4");
    ASSERT_NULL(strstr(r->error_ranges, "5"));
    cbm_free_result(r);
    PASS();
}

SUITE(parse_coverage) {
    RUN_TEST(c_ifdef_split_brace_sets_parse_incomplete);
    RUN_TEST(c_ifdef_split_brace_neighbors_still_extracted);
    RUN_TEST(c_error_range_points_at_failed_region);
    RUN_TEST(c_clean_file_not_flagged);
    RUN_TEST(py_unrecovered_garbage_sets_parse_incomplete);
    RUN_TEST(py_recovered_def_not_flagged);
    RUN_TEST(py_clean_file_not_flagged);
    RUN_TEST(error_region_cap_is_honored);
    RUN_TEST(error_region_cap_reports_what_it_dropped);
    RUN_TEST(uncapped_ranges_carry_no_marker);
    RUN_TEST(python_whole_file_error_is_unusable);
    RUN_TEST(local_error_stays_partial_not_unusable);
    RUN_TEST(clean_file_is_neither_partial_nor_unusable);
    RUN_TEST(c_ifdef_split_is_partial_never_unusable);
    RUN_TEST(c_trailing_recovered_defs_keep_flag);
    RUN_TEST(dockerfile_missing_final_newline_not_flagged_issue1610);
    RUN_TEST(dockerfile_with_final_newline_still_clean_issue1610);
    RUN_TEST(dockerfile_trailing_space_at_eof_not_flagged_issue1746);
    RUN_TEST(dockerfile_trailing_space_with_final_newline_clean_issue1746);
    RUN_TEST(dockerfile_without_trailing_space_at_eof_clean_issue1746);
    RUN_TEST(dockerfile_crlf_trailing_space_at_eof_not_flagged_issue1746);
    RUN_TEST(missing_final_newline_not_flagged_across_grammars_issue1610);
    RUN_TEST(real_error_before_eof_still_flagged_without_final_newline_issue1610);
    RUN_TEST(width_bearing_error_at_eof_still_flagged_issue1610);
    RUN_TEST(c_ifdef_split_range_narrows_to_dropped_branch);
    RUN_TEST(c_ifdef_split_range_excludes_lines_the_preprocessor_explained);
    RUN_TEST(c_ifdef_split_range_never_starts_on_a_directive);
    RUN_TEST(c_refinement_does_not_suppress_real_garbage);
    RUN_TEST(c_clean_file_stays_unflagged_after_refinement);
    RUN_TEST(perl_format_followed_by_named_sub_is_complete_issue1838);
    RUN_TEST(perl_malformed_source_remains_partial_issue1838);
    RUN_TEST(dockerfile_trailing_blank_at_eof_not_flagged_issue1746);
    RUN_TEST(real_error_before_eof_still_flagged_with_trailing_blank_issue1746);
    RUN_TEST(width_bearing_error_at_eof_still_flagged_with_trailing_blank_issue1746);
    RUN_TEST(c_thread_local_grammar_limit_is_pinned_issue963);
    RUN_TEST(coverage_repeated_error_line_reports_one_range_issue963);
    RUN_TEST(coverage_range_never_ends_past_the_last_line_issue963);
}
