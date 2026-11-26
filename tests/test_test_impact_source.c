/*
 * test_test_impact_source.c — text facts about a snapshot's C-family sources
 * (src/mcp/test_impact_source.c).
 *
 * The line patterns are a hand port of the regular expressions of the
 * measured reference (replay4.py). The expected values in the table below are
 * what those expressions return for each line (computed with Python's `re`),
 * quirks included: a port that disagrees seeds differently from what was
 * measured.
 */
#include "test_framework.h"

#include "mcp/test_impact_source.h"

#include <stdbool.h>
#include <string.h>

typedef struct {
    const char *line;
    bool comment;
    bool pp;
    const char *define_name;
    const char *typedef_name;
    const char *tag_name;
    const char *extern_name;
    const char *proto_name;
    const char *include_path;
} tis_line_t;

static const tis_line_t tis_lines[] = {
    {"", true, false, "", "", "", "", "", ""},
    {"   ", true, false, "", "", "", "", "", ""},
    {"// x", true, false, "", "", "", "", "", ""},
    {"  /* x", true, false, "", "", "", "", "", ""},
    {" * x", true, false, "", "", "", "", "", ""},
    /* Code with a trailing comment is code (the reference called it a comment). */
    {"int x; /* c */", false, false, "", "", "", "", "", ""},
    {"int x;", false, false, "", "", "", "", "", ""},
    {"x */  ", true, false, "", "", "", "", "", ""},
    {"*/", true, false, "", "", "", "", "", ""},
    {"a*/b", false, false, "", "", "", "", "", ""},
    {"\t\r", true, false, "", "", "", "", "", ""},
    {"#if X", false, true, "", "", "", "", "", ""},
    {"#ifdef X", false, true, "", "", "", "", "", ""},
    {"  #  ifndef X", false, true, "", "", "", "", "", ""},
    {"#elif", false, true, "", "", "", "", "", ""},
    {"#else", false, true, "", "", "", "", "", ""},
    {"#endif", false, true, "", "", "", "", "", ""},
    {"#undef X", false, true, "", "", "", "", "", ""},
    {"#include <x>", false, true, "", "", "", "", "", ""},
    {"#include\"x\"", false, true, "", "", "", "", "", ""},
    {"#define X", false, false, "X", "", "", "", "", ""},
    {"#iffy", false, false, "", "", "", "", "", ""},
    {"#if(x)", false, true, "", "", "", "", "", ""},
    {"#pragma once", false, false, "", "", "", "", "", ""},
    {"#includes", false, false, "", "", "", "", "", ""},
    {"#define X 1", false, false, "X", "", "", "", "", ""},
    {"  #  define  F(a, b) a##b", false, false, "F", "", "", "", "", ""},
    {"#define", false, false, "", "", "", "", "", ""},
    {"#definex", false, false, "", "", "", "", "", ""},
    {"#define(X)", false, false, "", "", "", "", "", ""},
    {"#define 1X", false, false, "", "", "", "", "", ""},
    {"#define\tX", false, false, "X", "", "", "", "", ""},
    {"#define F(x) f(x)", false, false, "F", "", "", "", "", ""},
    {"typedef int myint;", false, false, "", "myint", "", "", "", ""},
    {"typedef struct foo bar;", false, false, "", "bar", "", "", "", ""},
    {"typedef int arr[3][4];", false, false, "", "", "", "", "", ""},
    {"typedef int arr[3];", false, false, "", "arr", "", "", "", ""},
    {"typedef void (*fn)(int);", false, false, "", "", "", "", "void", ""},
    {"x typedef y;", false, false, "", "y", "", "", "", ""},
    {"typedefs x;", false, false, "", "", "", "", "", ""},
    {"typedef int a, b;", false, false, "", "b", "", "", "", ""},
    {"int typedef_x;", false, false, "", "", "", "", "", ""},
    {"typedef int x ;", false, false, "", "x", "", "", "", ""},
    {"typedef struct { int a; } s_t;", false, false, "", "a", "", "", "", ""},
    {"typedef unsigned long ulong_t [ 8 ] ;", false, false, "", "ulong_t", "", "", "", ""},
    {"struct foo {", false, false, "", "", "foo", "", "", ""},
    {"typedef struct foo {", false, false, "", "", "foo", "", "", ""},
    {"  union u{", false, false, "", "", "u", "", "", ""},
    {"enum e {", false, false, "", "", "e", "", "", ""},
    {"struct foo;", false, false, "", "", "", "", "", ""},
    {"structfoo {", false, false, "", "", "", "", "", ""},
    {"struct {", false, false, "", "", "", "", "", ""},
    {"typedef  enum   color  {", false, false, "", "", "color", "", "", ""},
    {"extern int x;", false, false, "", "", "", "x", "", ""},
    {"extern const char *names[];", false, false, "", "", "", "names", "", ""},
    {"extern int f(void);", false, false, "", "", "", "", "f", ""},
    {"extern int a, b;", false, false, "", "", "", "b", "", ""},
    {"  extern T x [N] ;", false, false, "", "", "", "x", "", ""},
    {"externs int x;", false, false, "", "", "", "", "", ""},
    {"extern \"C\" {", false, false, "", "", "", "", "", ""},
    {"int foo(void)", false, false, "", "", "", "", "foo", ""},
    {"foo(x)", false, false, "", "", "", "", "", ""},
    {"static void foo (int a)", false, false, "", "", "", "", "foo", ""},
    {"  int foo(void)", false, false, "", "", "", "", "", ""},
    {"int *bar(void)", false, false, "", "", "", "", "bar", ""},
    {"a.b(c)", false, false, "", "", "", "", "", ""},
    {"int foo\t(x)", false, false, "", "", "", "", "foo", ""},
    {"return foo(x);", false, false, "", "", "", "", "foo", ""},
    {"static inline T *g (void) {", false, false, "", "", "", "", "g", ""},
    {"#include \"a/b.h\"", false, true, "", "", "", "", "", "a/b.h"},
    {"  # include  \"x.h\"", false, true, "", "", "", "", "", "x.h"},
    {"#include <x.h>", false, true, "", "", "", "", "", ""},
    {"#include\"\"", false, true, "", "", "", "", "", ""},
    {"#include  \"../up/y.h\" // c", false, true, "", "", "", "", "", "../up/y.h"},
};

static bool tis_same(const char *line, bool matched, cbm_ti_span_t span, const char *expected) {
    if (!*expected) {
        return !matched;
    }
    return matched && span.len == strlen(expected) &&
           memcmp(line + span.start, expected, span.len) == 0;
}

/* Every pattern gives on every line what the reference expression gives. */
TEST(test_impact_source_line_patterns_match_the_reference) {
    for (size_t i = 0; i < sizeof(tis_lines) / sizeof(tis_lines[0]); i++) {
        const tis_line_t *c = &tis_lines[i];
        const char *s = c->line;
        size_t n = strlen(s);
        cbm_ti_span_t span = {0};
        size_t body = 0;
        bool ok = cbm_ti_line_is_comment(s, n) == c->comment &&
                  cbm_ti_line_is_pp_condition(s, n) == c->pp &&
                  tis_same(s, cbm_ti_match_define(s, n, &span, &body), span, c->define_name) &&
                  tis_same(s, cbm_ti_match_typedef(s, n, &span), span, c->typedef_name) &&
                  tis_same(s, cbm_ti_match_tag_declaration(s, n, &span), span, c->tag_name) &&
                  tis_same(s, cbm_ti_match_extern(s, n, &span), span, c->extern_name) &&
                  tis_same(s, cbm_ti_match_prototype(s, n, &span), span, c->proto_name) &&
                  tis_same(s, cbm_ti_match_include(s, n, &span), span, c->include_path);
        if (!ok) {
            printf("  line %zu: \"%s\"\n", i, s);
        }
        ASSERT_TRUE(ok);
    }
    PASS();
}

/* Only a line that is nothing but comment counts as one: a change to any
 * other line is a change to code. A dereference at the start of a line read
 * as a block-comment line once and dropped a real edit's seed. */
TEST(test_impact_source_only_whole_comment_lines_are_comments) {
    static const struct {
        const char *line;
        bool comment;
    } cases[] = {
        {"                    *out_jax_path = extract_route_path_from_args(a, args, source, true);",
         false},
        {"*out = 1;", false},
        {"  **pp = q;", false},
        {"  *(p + 1) = 0;", false},
        {"  x = f(); /* why */", false},
        {"/* a */ x = 1;", false},
        {"*/ x = 1;", false},
        {" * a block-comment line", true},
        {" *", true},
        {" */", true},
        {"\t*\tindented */", true},
        {"/* whole */", true},
        {"/** doc", true},
        {"   end of the comment */", true},
        {"// line", true},
        {"", true},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        if (cbm_ti_line_is_comment(cases[i].line, strlen(cases[i].line)) != cases[i].comment) {
            printf("  comment-line case %zu: \"%s\" expected %d\n", i, cases[i].line,
                   cases[i].comment);
            FAIL("a line was classified wrongly");
        }
    }
    PASS();
}

/* The declaration of a changed header line tries define, typedef, tag,
 * extern, prototype in that order. */
TEST(test_impact_source_declaration_tries_the_patterns_in_order) {
    static const struct {
        const char *line;
        const char *name;
    } cases[] = {{"#define F(x) f(x)", "F"},
                 {"typedef void (*fn)(int);", "void"},
                 {"extern int f(void);", "f"},
                 {"typedef struct foo {", "foo"},
                 {"int x;", ""}};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        cbm_ti_span_t span = {0};
        const char *s = cases[i].line;
        ASSERT_TRUE(
            tis_same(s, cbm_ti_match_declaration(s, strlen(s), &span), span, cases[i].name));
    }
    PASS();
}

static cbm_ti_source_t *tis_build(const char *const *files, int count) {
    cbm_ti_source_t *s = cbm_ti_source_new();
    if (!s) {
        return NULL;
    }
    for (int i = 0; i < count; i += 2) {
        if (!cbm_ti_source_add(s, files[i], files[i + 1], strlen(files[i + 1]))) {
            cbm_ti_source_free(s);
            return NULL;
        }
    }
    if (!cbm_ti_source_finish(s)) {
        cbm_ti_source_free(s);
        return NULL;
    }
    return s;
}

static bool tis_has(const char *const *list, int count, const char *word) {
    for (int i = 0; i < count; i++) {
        if (strcmp(list[i], word) == 0) {
            return true;
        }
    }
    return false;
}

/* A header's macros carry the identifiers of their body (keywords and the
 * macro's own name removed, continuation lines included) and the operands of
 * `##`, as the reference collects them. */
TEST(test_impact_source_macros_carry_body_names_and_pastes) {
    const char *files[] = {"inc/m.h",
                           "#define CASE(tag_) \\\n"
                           "    static int CASE_##tag_ = helper(x2y, 0x1F); \\\n"
                           "    int tag_ ## _end;\n"
                           "#define PLAIN return NULL\n",
                           "src/a.c", "#define NOT_A_HEADER helper\n"};
    cbm_ti_source_t *s = tis_build(files, 4);
    ASSERT_NOT_NULL(s);
    int count = 0;
    const cbm_ti_macro_t *macros = cbm_ti_source_macros(s, &count);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(macros[0].name, "CASE");
    /* `tag_` is the parameter: the reference keeps it, as it keeps `x1F` */
    const char *want[] = {"CASE_", "_end", "helper", "tag_", "x1F", "x2y"};
    ASSERT_EQ(macros[0].token_count, 6);
    for (int i = 0; i < 6; i++) {
        ASSERT_STR_EQ(macros[0].tokens[i], want[i]);
    }
    ASSERT_EQ(macros[0].paste_prefix_count, 2);
    ASSERT_TRUE(tis_has(macros[0].paste_prefixes, 2, "CASE_"));
    ASSERT_TRUE(tis_has(macros[0].paste_prefixes, 2, "tag_"));
    ASSERT_EQ(macros[0].paste_suffix_count, 0);
    ASSERT_STR_EQ(macros[1].name, "PLAIN");
    ASSERT_EQ(macros[1].token_count, 0);
    cbm_ti_source_free(s);
    PASS();
}

/* `x ## y ## z`: the reference reads x and y as prefixes and never z (the
 * second `##` was taken by y); `## q` alone is a suffix. */
TEST(test_impact_source_paste_scan_follows_the_reference) {
    const char *files[] = {"p.h", "#define A x ## y ## z\n#define B ## q\n"};
    cbm_ti_source_t *s = tis_build(files, 2);
    ASSERT_NOT_NULL(s);
    int count = 0;
    const cbm_ti_macro_t *macros = cbm_ti_source_macros(s, &count);
    ASSERT_EQ(count, 2);
    ASSERT_EQ(macros[0].paste_prefix_count, 2);
    ASSERT_TRUE(tis_has(macros[0].paste_prefixes, 2, "x"));
    ASSERT_TRUE(tis_has(macros[0].paste_prefixes, 2, "y"));
    ASSERT_EQ(macros[0].paste_suffix_count, 0);
    ASSERT_EQ(macros[1].paste_prefix_count, 0);
    ASSERT_EQ(macros[1].paste_suffix_count, 1);
    ASSERT_STR_EQ(macros[1].paste_suffixes[0], "q");
    cbm_ti_source_free(s);
    PASS();
}

/* An include names the file it resolves to from the including directory
 * when that file exists, otherwise every file whose path ends with it
 * (leading `.` and `/` removed). The closure follows includers transitively. */
TEST(test_impact_source_include_closure_follows_the_reference) {
    const char *files[] = {"src/core/x.h",    "int x;\n",
                           "src/core/y.h",    "#include \"x.h\"\n",
                           "src/app/main.c",  "#include \"core/y.h\"\n",
                           "src/app/other.c", "#include \"../core/x.h\"\n",
                           "lib/x.h",         "int other_x;\n",
                           "tests/t.c",       "#include \"../../x.h\"\n",
                           "tests/u.c",       "#include <x.h>\n"};
    cbm_ti_source_t *s = tis_build(files, 14);
    ASSERT_NOT_NULL(s);
    int n = cbm_ti_source_file_count(s);
    ASSERT_EQ(n, 7);
    bool marked[7] = {false};
    marked[cbm_ti_source_find(s, "src/core/x.h")] = true;
    cbm_ti_source_include_closure(s, marked);
    /* y.h resolves x.h beside it; main.c reaches y.h through the suffix
     * "core/y.h"; other.c through ../core/x.h; t.c's "../../x.h" does not
     * exist relative to tests/, so its key "x.h" names BOTH x.h files. */
    ASSERT_TRUE(marked[cbm_ti_source_find(s, "src/core/y.h")]);
    ASSERT_TRUE(marked[cbm_ti_source_find(s, "src/app/main.c")]);
    ASSERT_TRUE(marked[cbm_ti_source_find(s, "src/app/other.c")]);
    ASSERT_TRUE(marked[cbm_ti_source_find(s, "tests/t.c")]);
    ASSERT_FALSE(marked[cbm_ti_source_find(s, "lib/x.h")]);
    ASSERT_FALSE(marked[cbm_ti_source_find(s, "tests/u.c")]);
    cbm_ti_source_free(s);
    PASS();
}

/* One place per line; comment-led lines and keywords hold none; identifiers
 * in strings and trailing comments count (an over-approximation that only
 * adds seeds); `0x1F` holds `x1F` as the reference's token pattern reads it. */
TEST(test_impact_source_occurrences_follow_the_reference) {
    const char *files[] = {"b.c",
                           "int f(void) { return f2(f); }\n"
                           "// f in a comment line\n"
                           " * f in a block comment line\n"
                           "const char *s = \"f\"; /* f */\n"
                           "int y = 0x1F;\n",
                           "a.c", "void g(void) { f(); }\n"};
    cbm_ti_source_t *s = tis_build(files, 4);
    ASSERT_NOT_NULL(s);
    ASSERT_TRUE(cbm_ti_source_build_occurrences(s));
    int count = 0;
    const cbm_ti_place_t *places = cbm_ti_source_occurrences(s, "f", 1, &count);
    ASSERT_EQ(count, 3);
    ASSERT_STR_EQ(cbm_ti_source_path(s, places[0].file), "a.c");
    ASSERT_EQ(places[0].line, 1);
    ASSERT_STR_EQ(cbm_ti_source_path(s, places[1].file), "b.c");
    ASSERT_EQ(places[1].line, 1);
    ASSERT_EQ(places[2].line, 4);
    (void)cbm_ti_source_occurrences(s, "x1F", 3, &count);
    ASSERT_EQ(count, 1);
    (void)cbm_ti_source_occurrences(s, "int", 3, &count);
    ASSERT_EQ(count, 0);
    cbm_ti_source_free(s);
    PASS();
}

/* Lines split as `text.split('\n')`: a final newline leaves an empty line. */
TEST(test_impact_source_lines_split_like_the_reference) {
    const char *files[] = {"x.c", "a\r\nb\n"};
    cbm_ti_source_t *s = tis_build(files, 2);
    ASSERT_NOT_NULL(s);
    ASSERT_EQ(cbm_ti_source_line_count(s, 0), 3);
    size_t len = 0;
    const char *line = cbm_ti_source_line(s, 0, 1, &len);
    ASSERT_EQ(len, 2);
    ASSERT_TRUE(memcmp(line, "a\r", 2) == 0);
    (void)cbm_ti_source_line(s, 0, 3, &len);
    ASSERT_EQ(len, 0);
    ASSERT_NULL(cbm_ti_source_line(s, 0, 4, &len));
    ASSERT_FALSE(cbm_ti_source_add(s, "late.c", "", 0));
    cbm_ti_source_free(s);
    PASS();
}

SUITE(test_impact_source) {
    RUN_TEST(test_impact_source_line_patterns_match_the_reference);
    RUN_TEST(test_impact_source_only_whole_comment_lines_are_comments);
    RUN_TEST(test_impact_source_declaration_tries_the_patterns_in_order);
    RUN_TEST(test_impact_source_macros_carry_body_names_and_pastes);
    RUN_TEST(test_impact_source_paste_scan_follows_the_reference);
    RUN_TEST(test_impact_source_include_closure_follows_the_reference);
    RUN_TEST(test_impact_source_occurrences_follow_the_reference);
    RUN_TEST(test_impact_source_lines_split_like_the_reference);
}
