/*
 * test_doc_mentions_perl.c -- Perl POD L<> codes as doc-comment references.
 *
 * A sub's doc is the POD section that names it; the file's module doc (its
 * File node's docstring) is the POD no sub holds. The L<> codes of both are
 * the references: module-level ones come from the File node.
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

#include "cbm.h"
#include "doclink.h"

#include <stdio.h>
#include <string.h>

/* ── extraction ──────────────────────────────────────────────────── */

static const char *const PL_KNOB =
    "package Knob;\n"                                          /* 1 */
    "\n"                                                       /* 2 */
    "=head1 NAME\n"                                            /* 3 */
    "\n"                                                       /* 4 */
    "Knob - see L<Dial> and L<the knob|Knob::Turn/turning>.\n" /* 5 */
    "\n"                                                       /* 6 */
    "=cut\n"                                                   /* 7 */
    "\n"                                                       /* 8 */
    "sub turn { return 1; }\n"                                 /* 9 */
    "1;\n"                                                     /* 10 */
    "__END__\n"                                                /* 11 */
    "\n"                                                       /* 12 */
    "=head1 METHODS\n"                                         /* 13 */
    "\n"                                                       /* 14 */
    "Methods, see L</turn> and L<https://example.org/k>.\n"    /* 15 */
    "\n"                                                       /* 16 */
    "=head2 turn\n"                                            /* 17 */
    "\n"                                                       /* 18 */
    "Turns, like L<Knob::Spin>.\n"                             /* 19 */
    "\n"                                                       /* 20 */
    "=head2 spin\n"                                            /* 21 */
    "\n"                                                       /* 22 */
    "About L<perlfunc/open> and L<crontab(5)>.\n"              /* 23 */
    "\n"                                                       /* 24 */
    "=cut\n";                                                  /* 25 */

/* The one token `raw`: its line, whether it is the file's, its family. */
static bool pl_token_is(const CBMFileResult *r, const char *raw, uint32_t line, bool file,
                        int syntax) {
    if (dm_count_tokens(r, raw) != 1) {
        printf("  token %s: %d found\n", raw, dm_count_tokens(r, raw));
        return false;
    }
    const CBMDocLink *t = dm_find_token(r, raw);
    bool is_file = (t->flags & CBM_DOCLINK_FLAG_FILE) != 0;
    if (t->line != line || is_file != file || t->syntax != syntax) {
        printf("  token %s: line %u file %d syntax %d\n", raw, t->line, is_file, t->syntax);
        return false;
    }
    return true;
}

TEST(doc_mentions_perl_tokens) {
    CBMFileResult *r = dm_extract(PL_KNOB, CBM_LANG_PERL, "lib/Knob.pm");
    ASSERT_NOT_NULL(r);
    /* the module doc's references come from the file, each on its own line */
    ASSERT_TRUE(pl_token_is(r, "Dial", 5, true, CBM_DOCLINK_PERL_POD_L));
    ASSERT_TRUE(pl_token_is(r, "the knob|Knob::Turn/turning", 5, true, CBM_DOCLINK_PERL_POD_L));
    ASSERT_TRUE(pl_token_is(r, "/turn", 15, true, CBM_DOCLINK_PERL_POD_L));
    ASSERT_TRUE(pl_token_is(r, "https://example.org/k", 15, true, CBM_DOCLINK_HREF));
    ASSERT_TRUE(pl_token_is(r, "perlfunc/open", 23, true, CBM_DOCLINK_PERL_POD_L));
    ASSERT_TRUE(pl_token_is(r, "crontab(5)", 23, true, CBM_DOCLINK_PERL_POD_L));
    /* a sub's section is the sub's, and only the sub's */
    ASSERT_TRUE(pl_token_is(r, "Knob::Spin", 19, false, CBM_DOCLINK_PERL_POD_L));
    const CBMDocLink *t = dm_find_token(r, "Knob::Spin");
    ASSERT_NOT_NULL(strstr(t->source_qn, "turn"));
    ASSERT_EQ(r->doc_links.count, 7);
    cbm_free_result(r);
    PASS();
}

/* ── resolution ──────────────────────────────────────────────────── */

static void pl_write_repo(const char *repo) {
    th_write_file(TH_PATH(repo, "lib/Knob.pm"),
                  "package Knob;\n"
                  "\n"
                  "=head1 NAME\n"
                  "\n"
                  "Knob - L<Knob::Dial>, L<Knob::Dial/spin>, L<Knob::Dial/OVERVIEW>.\n"
                  "\n"
                  "=head1 SEE ALSO\n"
                  "\n"
                  "L</turn>, L</NOTES>, L</GONE>, L<Mojo::Base>, L<crontab(5)>, L<TestHelper>,\n"
                  "L<Dup>.\n"
                  "\n"
                  "=head1 NOTES\n"
                  "\n"
                  "Nothing.\n"
                  "\n"
                  "=cut\n"
                  "\n"
                  "sub turn { return 1; }\n"
                  "\n"
                  "=head2 turn\n"
                  "\n"
                  "Turns, see L<Knob::Dial/spin>.\n"
                  "\n"
                  "=cut\n"
                  "\n"
                  "1;\n");
    th_write_file(TH_PATH(repo, "lib/Knob/Dial.pm"), "package Knob::Dial;\n"
                                                     "\n"
                                                     "=head1 OVERVIEW\n"
                                                     "\n"
                                                     "The dial of L<Knob>.\n"
                                                     "\n"
                                                     "=cut\n"
                                                     "\n"
                                                     "sub spin { return 2; }\n"
                                                     "\n"
                                                     "=head2 spin\n"
                                                     "\n"
                                                     "Spins.\n"
                                                     "\n"
                                                     "=cut\n"
                                                     "\n"
                                                     "1;\n");
    /* a second declaration of Knob::Dial, not where the library layout puts it */
    th_write_file(TH_PATH(repo, "lib/Extra.pm"), "package Knob::Dial;\nsub extra { 3 }\n1;\n");
    /* Dup twice, neither at its layout path */
    th_write_file(TH_PATH(repo, "lib/A.pm"), "package Dup;\nsub a { 1 }\n1;\n");
    th_write_file(TH_PATH(repo, "lib/B.pm"), "package Dup;\nsub b { 1 }\n1;\n");
    th_write_file(TH_PATH(repo, "t/lib/TestHelper.pm"),
                  "package TestHelper;\nsub help { 1 }\n1;\n");
}

static bool pl_edge(const char *db, const char *src, const char *tgt) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    if (n != 1 || !strstr(props, "\"tier\":\"exact\"")) {
        printf("  edge %s -> %s: %d found %s\n", src, tgt, n, props);
        return false;
    }
    return true;
}

static bool pl_row(const char *db, const char *rel, const char *raw, const char *reason) {
    char got[64];
    dm_row(db, rel, raw, got, sizeof(got), NULL, 0);
    if (strcmp(got, reason) != 0) {
        printf("  row %s [%s]: \"%s\", expected \"%s\"\n", rel, raw, got, reason);
        return false;
    }
    return true;
}

TEST(doc_mentions_perl_resolution) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlperl_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    pl_write_repo(repo);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    const char *knob = "lib/Knob.pm";
    /* a module the library layout names, though another file declares it too */
    ASSERT_TRUE(pl_edge(db, "lib.Knob.pm.__file__", "lib.Knob.Dial.pm.__file__"));
    /* a section whose heading names a sub of the module: the sub */
    ASSERT_TRUE(pl_edge(db, "lib.Knob.pm.__file__", "lib.Knob.Dial.spin"));
    ASSERT_TRUE(pl_edge(db, "lib.Knob.turn", "lib.Knob.Dial.spin"));
    /* the same page: a sub binds, a section that names none is local */
    ASSERT_TRUE(pl_edge(db, "lib.Knob.pm.__file__", "lib.Knob.turn"));
    ASSERT_TRUE(pl_row(db, knob, "/NOTES", ""));
    /* ... and one the page has no heading for is doc rot */
    ASSERT_TRUE(pl_row(db, knob, "/GONE", "missing"));
    /* outside the repository, a man page, test code, two declarations */
    ASSERT_TRUE(pl_row(db, knob, "Mojo::Base", "external"));
    ASSERT_TRUE(pl_row(db, knob, "crontab(5)", "external"));
    ASSERT_TRUE(pl_row(db, knob, "TestHelper", "test_only_target"));
    ASSERT_TRUE(pl_row(db, knob, "Dup", "ambiguous"));
    /* the page is the file, also where a Folder holds lib/Knob.pm's module name */
    ASSERT_TRUE(pl_edge(db, "lib.Knob.Dial.pm.__file__", "lib.Knob.pm.__file__"));
    th_cleanup(tmp);
    PASS();
}

/* ── incremental == full; one worker == several ──────────────────── */

#define PL_INC_DIAL                                                                     \
    "package Knob::Dial;\n\n=head1 OVERVIEW\n\nThe dial.\n\n=cut\n\nsub spin { 2 }\n\n" \
    "=head2 spin\n\nSpins.\n\n=cut\n\n1;\n"

TEST(doc_mentions_perl_incremental_equals_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlperl_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char inc_db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    th_write_file(TH_PATH(repo, "lib/Knob.pm"),
                  "package Knob;\n\n=head1 NAME\n\nKnob - see L<Knob::Dial/spin>.\n\n=cut\n\n"
                  "sub turn { 1 }\n1;\n");
    th_write_file(TH_PATH(repo, "lib/Knob/Dial.pm"), PL_INC_DIAL);
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    ASSERT_TRUE(pl_edge(inc_db, "lib.Knob.pm.__file__", "lib.Knob.Dial.spin"));

    /* the module doc's text changes: the file is repaired alone */
    th_write_file(
        TH_PATH(repo, "lib/Knob.pm"),
        "package Knob;\n\n=head1 NAME\n\nKnob, a knob - see L<Knob::Dial/spin>.\n\n=cut\n\n"
        "sub turn { 1 }\n1;\n");
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "module doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(pl_edge(inc_db, "lib.Knob.pm.__file__", "lib.Knob.Dial.spin"));

    /* a reference added to the module doc: an edge from the File node */
    th_write_file(TH_PATH(repo, "lib/Knob.pm"),
                  "package Knob;\n\n=head1 NAME\n\nKnob, a knob - see L<Knob::Dial/spin> and\n"
                  "L<Knob::Dial>.\n\n=cut\n\nsub turn { 1 }\n1;\n");
    ASSERT_EQ(
        dm_step(repo, inc_db, full_db, "module doc link", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(pl_edge(inc_db, "lib.Knob.pm.__file__", "lib.Knob.Dial.pm.__file__"));

    /* a new heading changes how a section is found: a full run */
    th_write_file(TH_PATH(repo, "lib/Knob.pm"),
                  "package Knob;\n\n=head1 NAME\n\nKnob, a knob - see L<Knob::Dial/spin> and\n"
                  "L<Knob::Dial>.\n\n=head1 SEE ALSO\n\nL<Knob::Dial/OVERVIEW>\n\n=cut\n\n"
                  "sub turn { 1 }\n1;\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "heading added", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);

    /* the heading that named the sub goes: another file's section reference
     * now names a place in the module's documentation, the module */
    th_write_file(
        TH_PATH(repo, "lib/Knob/Dial.pm"),
        "package Knob::Dial;\n\n=head1 OVERVIEW\n\nThe dial.\n\n=cut\n\nsub spin { 2 }\n1;\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "heading removed", CBM_INCREMENTAL_ROUTE_FORCED_FULL),
              0);
    char props[512];
    int n = 0;
    dm_edge(inc_db, "lib.Knob.pm.__file__", "lib.Knob.Dial.spin", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);

    /* the sub goes: the files that name it are repaired, nothing else */
    th_write_file(TH_PATH(repo, "lib/Knob/Dial.pm"),
                  "package Knob::Dial;\n\n=head1 OVERVIEW\n\nThe dial.\n\n=cut\n\n1;\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "sub removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR),
              0);
    th_cleanup(tmp);
    PASS();
}

TEST(doc_mentions_perl_workers_agree) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlperl_par_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char par_db[512];
    char seq_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(par_db, sizeof(par_db), "%s/par.db", tmp);
    snprintf(seq_db, sizeof(seq_db), "%s/seq.db", tmp);
    enum { MODULES = 60 }; /* past the parallel threshold */
    for (int i = 0; i < MODULES; i++) {
        char rel[64];
        char src[1024];
        int next = (i + 1) % MODULES;
        snprintf(rel, sizeof(rel), "lib/Par/M%02d.pm", i);
        snprintf(src, sizeof(src),
                 "package Par::M%02d;\n\n=head1 NAME\n\nPar::M%02d - see L<Par::M%02d>,\n"
                 "L<Par::M%02d/run%02d>, L</run%02d>, L<Gone::Away> and L</GONE>.\n\n=cut\n\n"
                 "sub run%02d { %d }\n\n=head2 run%02d\n\nRuns; also L<Par::M%02d/OVERVIEW>.\n\n"
                 "=cut\n\n1;\n",
                 i, i, next, next, next, i, i, i, i, next);
        th_write_file(TH_PATH(repo, rel), src);
    }
    ASSERT_EQ(dm_workers_agree(repo, par_db, seq_db), 0);
    ASSERT_TRUE(pl_edge(par_db, "lib.Par.M00.pm.__file__", "lib.Par.M01.run01"));
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_mentions_perl) {
    dm_ship_held_families();
    RUN_TEST(doc_mentions_perl_tokens);
    RUN_TEST(doc_mentions_perl_resolution);
    RUN_TEST(doc_mentions_perl_incremental_equals_full);
    RUN_TEST(doc_mentions_perl_workers_agree);
}
