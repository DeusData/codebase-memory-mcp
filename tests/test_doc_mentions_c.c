/*
 * test_doc_mentions_c.c -- C doc-comment references (Doxygen @ref, @see).
 */
#include "../src/foundation/compat.h"
#include "test_framework.h"
#include "test_helpers.h"
#include "test_doc_mentions_helpers.h"

#include "cbm.h"
#include "doclink.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* ── extraction ──────────────────────────────────────────────────── */

static const char *const C_WIDGET =
    "#include \"api.h\"\n"                                                    /* 1 */
    "\n"                                                                      /* 2 */
    "/**\n"                                                                   /* 3 */
    " * Makes widgets. @ref widget_free \"free it\" and \\ref api_counter.\n" /* 4 */
    " * Not `@ref Hidden` nor \"@ref Quoted\".\n"                             /* 5 */
    " * @code\n"                                                              /* 6 */
    " * @ref InCode\n"                                                        /* 7 */
    " * @endcode\n"                                                           /* 8 */
    " * ```\n"                                                                /* 9 */
    " * @ref Fenced\n"                                                        /* 10 */
    " * ```\n"                                                                /* 11 */
    " * @see widget_free(), helper, ::api_counter, https://example.com/w\n"   /* 12 */
    " *      also_this\n"                                                     /* 13 */
    " *\n"                                                                    /* 14 */
    " * after_blank\n"                                                        /* 15 */
    " * \\sa \\c not_this, Other_thing.\n"                                    /* 16 */
    " * @param n not_a_see\n"                                                 /* 17 */
    " */\n"                                                                   /* 18 */
    "int widget_new(int n) { return n; }\n"                                   /* 19 */
    "\n"                                                                      /* 20 */
    "/// Line doc. @ref API_MAX\n"                                            /* 21 */
    "/// @sa widget_new()\n"                                                  /* 22 */
    "static int helper(int x) { return x; }\n"                                /* 23 */
    "\n"                                                                      /* 24 */
    "/** @defgroup grp The group */\n"                                        /* 25 */
    "\n"                                                                      /* 26 */
    "/** A prototype's doc: @ref grp and @ref widget_new. */\n"               /* 27 */
    "int f2(void);\n"                                                         /* 28 */
    "#define API_MAX 3\n"                                                     /* 29 */
    "#define TWICE(x) ((x) * 2)\n"                                            /* 30 */
    "static int counter;\n"                                                   /* 31 */
    "int api_counter = 0;\n"                                                  /* 32 */
    "enum mode { MODE_A };\n";                                                /* 33 */

static bool c_token_is(const CBMFileResult *r, const char *raw, uint32_t line, int syntax,
                       bool file) {
    if (dm_count_tokens(r, raw) != 1) {
        printf("  token %s: %d found\n", raw, dm_count_tokens(r, raw));
        return false;
    }
    const CBMDocLink *t = dm_find_token(r, raw);
    bool is_file = (t->flags & CBM_DOCLINK_FLAG_FILE) != 0;
    if (t->line != line || t->syntax != syntax || is_file != file) {
        printf("  token %s: line %u syntax %d file %d\n", raw, t->line, t->syntax, is_file);
        return false;
    }
    return true;
}

TEST(doc_mentions_c_tokens) {
    CBMFileResult *r = dm_extract(C_WIDGET, CBM_LANG_C, "src/widget.c");
    ASSERT_NOT_NULL(r);
    ASSERT_TRUE(c_token_is(r, "widget_free", 4, CBM_DOCLINK_C_REF, false));
    ASSERT_TRUE(c_token_is(r, "api_counter", 4, CBM_DOCLINK_C_REF, false));
    /* the names an @see paragraph lists, to its empty line */
    ASSERT_TRUE(c_token_is(r, "widget_free()", 12, CBM_DOCLINK_C_SEE, false));
    ASSERT_TRUE(c_token_is(r, "helper", 12, CBM_DOCLINK_C_SEE, false));
    ASSERT_TRUE(c_token_is(r, "::api_counter", 12, CBM_DOCLINK_C_SEE, false));
    ASSERT_TRUE(c_token_is(r, "https://example.com/w", 12, CBM_DOCLINK_HREF, false));
    ASSERT_TRUE(c_token_is(r, "also_this", 13, CBM_DOCLINK_C_SEE, false));
    ASSERT_TRUE(c_token_is(r, "Other_thing", 16, CBM_DOCLINK_C_SEE, false));
    /* a /// run */
    ASSERT_TRUE(c_token_is(r, "API_MAX", 21, CBM_DOCLINK_C_REF, false));
    ASSERT_TRUE(c_token_is(r, "widget_new()", 22, CBM_DOCLINK_C_SEE, false));
    /* a prototype has no node: its doc belongs to the file */
    ASSERT_TRUE(c_token_is(r, "grp", 27, CBM_DOCLINK_C_REF, true));
    ASSERT_TRUE(c_token_is(r, "widget_new", 27, CBM_DOCLINK_C_REF, true));
    /* code spans, quotes, code blocks, fences, a new paragraph, \c */
    ASSERT_EQ(dm_count_tokens(r, "Hidden"), 0);
    ASSERT_EQ(dm_count_tokens(r, "Quoted"), 0);
    ASSERT_EQ(dm_count_tokens(r, "InCode"), 0);
    ASSERT_EQ(dm_count_tokens(r, "Fenced"), 0);
    ASSERT_EQ(dm_count_tokens(r, "after_blank"), 0);
    ASSERT_EQ(dm_count_tokens(r, "not_this"), 0);
    ASSERT_EQ(dm_count_tokens(r, "not_a_see"), 0);
    ASSERT_EQ(r->doc_links.count, 12);
    cbm_free_result(r);
    PASS();
}

/* A C++ file is not part of the leg; a header is. */
TEST(doc_mentions_c_files) {
    const char *src = "/** See @ref other_fn. */\nint fn(void) { return 0; }\n";
    CBMFileResult *r = dm_extract(src, CBM_LANG_CPP, "src/x.cpp");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(r->doc_links.count, 0);
    ASSERT_NULL(r->doc_scope);
    cbm_free_result(r);
    r = dm_extract(src, CBM_LANG_CPP, "include/x.h");
    ASSERT_NOT_NULL(r);
    ASSERT_EQ(dm_count_tokens(r, "other_fn"), 1);
    cbm_free_result(r);
    PASS();
}

static bool c_has_prefix_line(const char *blob, const char *prefix) {
    size_t n = strlen(prefix);
    for (const char *p = blob; p && *p;) {
        const char *nl = strchr(p, '\n');
        size_t len = nl ? (size_t)(nl - p) : strlen(p);
        if (len >= n && memcmp(p, prefix, n) == 0) {
            return true;
        }
        p = nl ? nl + 1 : NULL;
    }
    printf("  no line: %s...\n", prefix);
    return false;
}

TEST(doc_mentions_c_scope_blob) {
    CBMFileResult *r = dm_extract(C_WIDGET, CBM_LANG_C, "src/widget.c");
    ASSERT_NOT_NULL(r);
    const char *s = r->doc_scope;
    ASSERT_NOT_NULL(s);
    ASSERT(strncmp(s, "c1\n", 3) == 0);
    ASSERT(c_has_prefix_line(s, "D\tf\tx\twidget_new\t"));
    ASSERT(c_has_prefix_line(s, "D\tf\ts\thelper\t"));
    ASSERT(c_has_prefix_line(s, "D\tv\ts\tcounter\t"));
    ASSERT(c_has_prefix_line(s, "D\tv\tx\tapi_counter\t"));
    /* types, constants and macros of a .c file are its own */
    ASSERT(c_has_prefix_line(s, "D\tm\ts\tAPI_MAX\t"));
    ASSERT(c_has_prefix_line(s, "D\tM\ts\tTWICE\t"));
    ASSERT(c_has_prefix_line(s, "D\tt\ts\tmode\t"));
    ASSERT(c_has_prefix_line(s, "D\te\ts\tMODE_A\t"));
    ASSERT(c_has_prefix_line(s, "A\tgrp"));
    cbm_free_result(r);
    PASS();
}

/* ── resolution ──────────────────────────────────────────────────── */

static void c_write_repo(const char *repo) {
    th_write_file(TH_PATH(repo, "include/api.h"), "#ifndef API_H\n"
                                                  "#define API_H\n"
                                                  "/** @defgroup widgets Widgets */\n"
                                                  "/** Makes one, see @ref widget_free. */\n"
                                                  "int widget_new(int n);\n"
                                                  "void widget_free(int w);\n"
                                                  "#define API_MAX 16\n"
                                                  "#define api_twice(x) ((x) * 2)\n"
                                                  "int dual(int x);\n"
                                                  "#define dual(x) ((x) + 1)\n"
                                                  "#define level(x) (x)\n"
                                                  "enum color { RED, GREEN };\n"
                                                  "struct st { int fld; };\n"
                                                  "typedef struct widget { int n; } widget_t;\n"
                                                  "extern int api_counter;\n"
                                                  "#endif\n");
    th_write_file(TH_PATH(repo, "src/widget.c"),
                  "#include \"api.h\"\n"
                  "int api_counter;\n"
                  "int level;\n"
                  "static int helper(int x) { return x; }\n"
                  "int widget_new(int n) { return n; }\n"
                  "void widget_free(int w) { (void)w; }\n"
                  "int (dual)(int x) { return x; }\n"
                  "/**\n"
                  " * The core. @ref helper, @ref api_counter, @ref API_MAX, @ref dual,\n"
                  " * @ref RED, @ref widgets, @ref other_static, @ref twin, @ref nope,\n"
                  " * @ref widget_t, @ref test_only_fn, @ref st::fld, @ref color::GREEN,\n"
                  " * @ref lfield, @ref st::nofield.\n"
                  " * @see api_twice() for details, unknown_fn()\n"
                  " */\n"
                  "int core(void) { return helper(0); }\n"
                  "struct local { int lfield; };\n");
    th_write_file(TH_PATH(repo, "src/other.c"), "#include \"api.h\"\n"
                                                "static int other_static(void) { return 0; }\n"
                                                "int twin(void) { return 1; }\n"
                                                "/** Other: @ref dual, @ref level and\n"
                                                " * @ref level(). */\n"
                                                "int other(void) { return other_static(); }\n");
    th_write_file(TH_PATH(repo, "src/third.c"), "int twin(void) { return 2; }\n");
    th_write_file(TH_PATH(repo, "tests/test_widget.c"), "int test_only_fn(void) { return 0; }\n");
}

static bool c_edge(const char *db, const char *src, const char *tgt) {
    char props[512];
    int n = 0;
    dm_edge(db, src, tgt, props, sizeof(props), &n);
    if (n != 1) {
        printf("  edge %s -> %s: %d found\n", src, tgt, n);
        return false;
    }
    return true;
}

static bool c_row(const char *db, const char *raw, const char *reason) {
    char got[64];
    dm_row(db, "src/widget.c", raw, got, sizeof(got), NULL, 0);
    if (strcmp(got, reason) != 0) {
        printf("  row [%s]: \"%s\", expected \"%s\"\n", raw, got, reason);
        return false;
    }
    return true;
}

TEST(doc_mentions_c_resolution) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(db, sizeof(db), "%s/g.db", tmp);
    c_write_repo(repo);
    ASSERT_EQ(dm_index(repo, db, NULL), 0);
    const char *core = "src.widget.core";
    /* the documented file's own definitions, a static one too */
    ASSERT_TRUE(c_edge(db, core, "src.widget.helper"));
    ASSERT_TRUE(c_edge(db, core, "src.widget.api_counter"));
    ASSERT_TRUE(c_edge(db, core, "src.widget.dual"));
    /* what every file sees: a header's macros, constants and types */
    ASSERT_TRUE(c_edge(db, core, "include.api.API_MAX#macro"));
    ASSERT_TRUE(c_edge(db, core, "include.api.RED"));
    ASSERT_TRUE(c_edge(db, core, "include.api.widget_t"));
    ASSERT_TRUE(c_edge(db, core, "include.api.api_twice#macro"));
    /* a group is no code entity; another file's static is not visible */
    ASSERT_TRUE(c_row(db, "widgets", "graph_gap"));
    ASSERT_TRUE(c_row(db, "other_static", "missing"));
    ASSERT_TRUE(c_row(db, "nope", "missing"));
    ASSERT_TRUE(c_row(db, "twin", "ambiguous"));
    ASSERT_TRUE(c_row(db, "test_only_fn", "test_only_target"));
    /* Type::member: a struct field, an enum constant; a field of the file */
    ASSERT_TRUE(c_edge(db, core, "include.api.st.fld"));
    ASSERT_TRUE(c_edge(db, core, "include.api.GREEN"));
    ASSERT_TRUE(c_edge(db, core, "src.widget.local.lfield"));
    ASSERT_TRUE(c_row(db, "st::nofield", "missing"));
    /* prose in an @see paragraph is text; a code-shaped name is missing */
    ASSERT_TRUE(c_row(db, "for", ""));
    ASSERT_TRUE(c_row(db, "details", ""));
    ASSERT_TRUE(c_row(db, "unknown_fn()", "missing"));
    /* a definition before a macro; with (), a function-like macro first */
    ASSERT_TRUE(c_edge(db, "src.other.other", "src.widget.dual"));
    ASSERT_TRUE(c_edge(db, "src.other.other", "src.widget.level"));
    ASSERT_TRUE(c_edge(db, "src.other.other", "include.api.level#macro"));
    /* a header prototype's doc: the header file is its source */
    ASSERT_TRUE(c_edge(db, "include.api.h.__file__", "src.widget.widget_free"));
    th_cleanup(tmp);
    PASS();
}

/* ── incremental == full; one worker == several ──────────────────── */

TEST(doc_mentions_c_incremental_equals_full) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlc_inc_XXXXXX");
    ASSERT_NOT_NULL(cbm_mkdtemp(tmp));
    char repo[512];
    char inc_db[512];
    char full_db[512];
    snprintf(repo, sizeof(repo), "%s/repo", tmp);
    snprintf(inc_db, sizeof(inc_db), "%s/inc.db", tmp);
    snprintf(full_db, sizeof(full_db), "%s/full.db", tmp);
    th_write_file(TH_PATH(repo, "src/lib.c"),
                  "int a(void) { return 0; }\nint b(void) { return 1; }\n");
    th_write_file(TH_PATH(repo, "src/use.c"),
                  "/** Uses @ref a. */\nint user(void) { return 0; }\n");
    ASSERT_EQ(dm_index(repo, inc_db, NULL), 0);
    ASSERT_TRUE(c_edge(inc_db, "src.use.user", "src.lib.a"));

    /* a doc edit: the file is repaired alone */
    th_write_file(TH_PATH(repo, "src/use.c"),
                  "/** Uses @ref a and @ref nope. */\nint user(void) { return 0; }\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "doc edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);

    /* a body edit in the target file: its scope is unchanged */
    th_write_file(TH_PATH(repo, "src/lib.c"),
                  "int a(void) { return 2; }\nint b(void) { return 1; }\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "body edit", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(c_edge(inc_db, "src.use.user", "src.lib.a"));

    /* b goes: the files that name it are repaired, nothing else */
    th_write_file(TH_PATH(repo, "src/lib.c"), "int a(void) { return 2; }\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "b removed", CBM_INCREMENTAL_ROUTE_CLOSURE_REPAIR), 0);
    ASSERT_TRUE(c_edge(inc_db, "src.use.user", "src.lib.a"));

    /* a becomes static (and b is back): changed records, a full run */
    th_write_file(TH_PATH(repo, "src/lib.c"),
                  "static int a(void) { return 0; }\nint b(void) { return 1; }\n");
    ASSERT_EQ(dm_step(repo, inc_db, full_db, "made static", CBM_INCREMENTAL_ROUTE_FORCED_FULL), 0);
    char props[512];
    int n = 0;
    dm_edge(inc_db, "src.use.user", "src.lib.a", props, sizeof(props), &n);
    ASSERT_EQ(n, 0);
    th_cleanup(tmp);
    PASS();
}

TEST(doc_mentions_c_workers_agree) {
    char tmp[256];
    snprintf(tmp, sizeof(tmp), "/tmp/cbm_dlc_par_XXXXXX");
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
        snprintf(rel, sizeof(rel), "src/m%02d.c", i);
        snprintf(src, sizeof(src),
                 "/** F%02d; see @ref f%02d, @ref K%02d, @ref nope.\n"
                 " * @see g%02d() */\n"
                 "int f%02d(void) { return 0; }\n"
                 "int g%02d(void) { return 1; }\n"
                 "#define K%02d 1\n",
                 i, next, next, next, i, i, i);
        th_write_file(TH_PATH(repo, rel), src);
    }
    ASSERT_EQ(dm_workers_agree(repo, par_db, seq_db), 0);
    ASSERT_TRUE(c_edge(par_db, "src.m00.f00", "src.m01.f01"));
    th_cleanup(tmp);
    PASS();
}

SUITE(doc_mentions_c) {
    dm_ship_held_families();
    RUN_TEST(doc_mentions_c_tokens);
    RUN_TEST(doc_mentions_c_files);
    RUN_TEST(doc_mentions_c_scope_blob);
    RUN_TEST(doc_mentions_c_resolution);
    RUN_TEST(doc_mentions_c_incremental_equals_full);
    RUN_TEST(doc_mentions_c_workers_agree);
}
