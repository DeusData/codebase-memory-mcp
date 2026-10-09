/*
 * test_test_impact.c — the test model of the test-impact engine
 * (src/mcp/test_impact.c).
 *
 * The model says which test belongs to which suite, and the selection names
 * tests by exactly that. A wrong entry is a red run (a test named that its
 * suite never runs) or, worse, a test that should have run and was not named.
 */
#include "test_framework.h"
#include "test_helpers.h"
#include <cli/cli.h>
#include <foundation/subprocess.h>
#include <mcp/test_impact.h>
#include <mcp/test_impact_result.h>
#include <discover/test_conventions.h>
#include <foundation/mem_core.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *file;
    const char *text;
} tm_source_t;

static cbm_test_model_t *model_of(const tm_source_t *sources, int count) {
    cbm_test_model_t *m = cbm_test_model_new(cbm_test_conventions_cbm());
    if (!m) {
        return NULL;
    }
    for (int i = 0; i < count; i++) {
        if (!cbm_test_model_add_source(m, sources[i].file, sources[i].text,
                                       strlen(sources[i].text))) {
            cbm_test_model_free(m);
            return NULL;
        }
    }
    if (!cbm_test_model_finish(m)) {
        cbm_test_model_free(m);
        return NULL;
    }
    return m;
}

static const cbm_test_case_t *case_named(const cbm_test_model_t *m, const char *name) {
    int n = 0;
    const cbm_test_case_t *cases = cbm_test_model_cases(m, &n);
    for (int i = 0; i < n; i++) {
        if (strcmp(cases[i].name, name) == 0) {
            return &cases[i];
        }
    }
    return NULL;
}

static const cbm_test_registration_t *registration_of(const cbm_test_model_t *m, const char *test) {
    int n = 0;
    const cbm_test_registration_t *regs = cbm_test_model_registrations(m, &n);
    for (int i = 0; i < n; i++) {
        if (strcmp(regs[i].test, test) == 0) {
            return &regs[i];
        }
    }
    return NULL;
}

static int count_cases(const cbm_test_model_t *m) {
    int n = 0;
    (void)cbm_test_model_cases(m, &n);
    return n;
}

static int count_suites(const cbm_test_model_t *m) {
    int n = 0;
    (void)cbm_test_model_suites(m, &n);
    return n;
}

static int count_registrations(const cbm_test_model_t *m) {
    int n = 0;
    (void)cbm_test_model_registrations(m, &n);
    return n;
}

TEST(test_model_reads_cases_suites_and_registrations) {
    static const tm_source_t src[] = {{"tests/test_a.c", "#include \"test_framework.h\"\n" /* 1 */
                                                         "\n"                              /* 2 */
                                                         "static int helper(void) {\n"     /* 3 */
                                                         "    return 1;\n"                 /* 4 */
                                                         "}\n"                             /* 5 */
                                                         "\n"                              /* 6 */
                                                         "TEST(alpha) {\n"                 /* 7 */
                                                         "    ASSERT_EQ(helper(), 1);\n"   /* 8 */
                                                         "    PASS();\n"                   /* 9 */
                                                         "}\n"                             /* 10 */
                                                         "\n"                              /* 11 */
                                                         "TEST(beta)\n"                    /* 12 */
                                                         "{\n"                             /* 13 */
                                                         "    if (helper()) {\n"           /* 14 */
                                                         "        PASS();\n"               /* 15 */
                                                         "    }\n"                         /* 16 */
                                                         "    FAIL(\"no\");\n"             /* 17 */
                                                         "}\n"                             /* 18 */
                                                         "\n"                              /* 19 */
                                                         "SUITE(a) {\n"                    /* 20 */
                                                         "    RUN_TEST(alpha);\n"          /* 21 */
                                                         "    RUN_TEST(beta);\n"           /* 22 */
                                                         "}\n"}};                          /* 23 */
    cbm_test_model_t *m = model_of(src, 1);
    ASSERT_NOT_NULL(m);
    ASSERT_EQ(count_cases(m), 2);
    ASSERT_EQ(count_suites(m), 1);
    ASSERT_EQ(count_registrations(m), 2);

    const cbm_test_case_t *alpha = case_named(m, "alpha");
    const cbm_test_case_t *beta = case_named(m, "beta");
    ASSERT_NOT_NULL(alpha);
    ASSERT_NOT_NULL(beta);
    ASSERT_STR_EQ(alpha->file, "tests/test_a.c");
    ASSERT_EQ(alpha->start_line, 7);
    ASSERT_EQ(alpha->end_line, 10);
    ASSERT_FALSE(alpha->conditional);
    /* The body's own braces do not end the case. */
    ASSERT_EQ(beta->start_line, 12);
    ASSERT_EQ(beta->end_line, 18);

    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(m, &n);
    ASSERT_STR_EQ(suites[0].name, "a");
    ASSERT_STR_EQ(suites[0].file, "tests/test_a.c");
    ASSERT_EQ(suites[0].start_line, 20);
    ASSERT_EQ(suites[0].end_line, 23);

    const cbm_test_registration_t *regs = cbm_test_model_registrations(m, &n);
    ASSERT_STR_EQ(regs[0].test, "alpha");
    ASSERT_STR_EQ(regs[0].suite, "a");
    ASSERT_EQ(regs[0].line, 21);
    ASSERT_TRUE(regs[0].resolved);
    ASSERT_FALSE(regs[0].conditional);
    ASSERT_STR_EQ(regs[1].test, "beta");
    ASSERT_EQ(regs[1].line, 22);
    cbm_test_model_free(m);
    PASS();
}

/* What a comment or a string says is not code: a commented-out registration
 * names a test its suite does not run. */
TEST(test_model_ignores_comments_and_literals) {
    static const tm_source_t src[] = {
        {"tests/test_b.c",
         "/* TEST(in_block_comment) { } */\n"
         "// TEST(in_line_comment) {\n"
         "static const char *text = \"TEST(in_string) { RUN_TEST(in_string); }\";\n"
         "static const char quote = '\"';\n"
         "static const char *escaped = \"a \\\" TEST(after_escape) {\";\n"
         "TEST(real) {\n"
         "    const char *s = \"}\"; /* a brace that closes nothing */\n"
         "    char c = '}';\n"
         "    PASS();\n"
         "}\n"
         "SUITE(b) {\n"
         "    RUN_TEST(real);\n"
         "    // RUN_TEST(commented_out);\n"
         "    /* RUN_TEST(blocked_out); */\n"
         "}\n"}};
    cbm_test_model_t *m = model_of(src, 1);
    ASSERT_NOT_NULL(m);
    ASSERT_EQ(count_cases(m), 1);
    ASSERT_EQ(count_registrations(m), 1);
    const cbm_test_case_t *real = case_named(m, "real");
    ASSERT_NOT_NULL(real);
    ASSERT_EQ(real->start_line, 6);
    ASSERT_EQ(real->end_line, 10);
    ASSERT_NOT_NULL(registration_of(m, "real"));
    ASSERT_NULL(registration_of(m, "commented_out"));
    ASSERT_NULL(registration_of(m, "blocked_out"));
    ASSERT_NULL(registration_of(m, "in_string"));
    cbm_test_model_free(m);
    PASS();
}

/* `void suite_x(void) { ... }` is what SUITE(x) expands to, and some files
 * write it out. A prototype of it is not a suite, and a registration belongs
 * to the suite whose body holds it. */
TEST(test_model_reads_suites_written_as_functions) {
    static const tm_source_t src[] = {{"tests/test_c.c", "void suite_c_two(void);\n"  /* 1 */
                                                         "TEST(one) {\n"              /* 2 */
                                                         "    PASS();\n"              /* 3 */
                                                         "}\n"                        /* 4 */
                                                         "TEST(two) {\n"              /* 5 */
                                                         "    PASS();\n"              /* 6 */
                                                         "}\n"                        /* 7 */
                                                         "void suite_c_one(void) {\n" /* 8 */
                                                         "    RUN_TEST(one);\n"       /* 9 */
                                                         "}\n"                        /* 10 */
                                                         /* after a suite, and in no suite */
                                                         "static void not_a_suite(void) {\n"
                                                         "    RUN_TEST(one);\n"
                                                         "}\n"
                                                         "void suite_c_two( void )\n" /* 14 */
                                                         "{\n"                        /* 15 */
                                                         "    RUN_TEST(two);\n"       /* 16 */
                                                         "}\n"}};
    cbm_test_model_t *m = model_of(src, 1);
    ASSERT_NOT_NULL(m);
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(m, &n);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(suites[0].name, "c_one");
    ASSERT_EQ(suites[0].start_line, 8);
    ASSERT_EQ(suites[0].end_line, 10);
    ASSERT_STR_EQ(suites[1].name, "c_two");
    ASSERT_EQ(suites[1].start_line, 14);
    const cbm_test_registration_t *regs = cbm_test_model_registrations(m, &n);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(regs[0].suite, "c_one");
    ASSERT_STR_EQ(regs[0].test, "one");
    ASSERT_EQ(regs[0].line, 9);
    ASSERT_STR_EQ(regs[1].suite, "c_two");
    ASSERT_STR_EQ(regs[1].test, "two");
    ASSERT_EQ(regs[1].line, 16);
    cbm_test_model_free(m);
    PASS();
}

/* A registration under #if is one that some builds do not have. The selection
 * must know, because the runner fails a named test its suite never ran. An
 * include guard is not such a condition. */
TEST(test_model_marks_what_is_under_a_condition) {
    static const tm_source_t src[] = {{"tests/test_d.c", "#ifndef TEST_D_GUARD\n"
                                                         "#define TEST_D_GUARD\n"
                                                         "TEST(everywhere) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "#ifdef _WIN32\n"
                                                         "TEST(windows_only) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "#else\n"
                                                         "TEST(posix_only) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "#endif\n"
                                                         "TEST(after) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "SUITE(d) {\n"
                                                         "    RUN_TEST(everywhere);\n"
                                                         "#ifdef _WIN32\n"
                                                         "    RUN_TEST(windows_only);\n"
                                                         "#else\n"
                                                         "    RUN_TEST(posix_only);\n"
                                                         "#endif\n"
                                                         "#if defined(SEAMS) && \\\n"
                                                         "    SEAMS\n"
                                                         "    RUN_TEST(after);\n"
                                                         "#endif\n"
                                                         "}\n"
                                                         "#endif\n"}};
    cbm_test_model_t *m = model_of(src, 1);
    ASSERT_NOT_NULL(m);
    ASSERT_EQ(count_cases(m), 4);
    ASSERT_EQ(count_registrations(m), 4);
    ASSERT_FALSE(case_named(m, "everywhere")->conditional);
    ASSERT_TRUE(case_named(m, "windows_only")->conditional);
    ASSERT_TRUE(case_named(m, "posix_only")->conditional);
    ASSERT_FALSE(case_named(m, "after")->conditional);
    ASSERT_FALSE(registration_of(m, "everywhere")->conditional);
    ASSERT_TRUE(registration_of(m, "windows_only")->conditional);
    ASSERT_TRUE(registration_of(m, "posix_only")->conditional);
    /* The case is unconditional, its registration is not. */
    ASSERT_TRUE(registration_of(m, "after")->conditional);
    cbm_test_model_free(m);
    PASS();
}

/* Both branches of an #if may open the same block. Counting both braces would
 * leave the scanner one level deep for the rest of the file, and every later
 * test and suite would be missed. */
TEST(test_model_keeps_brace_depth_across_else_branches) {
    static const tm_source_t src[] = {{"tests/test_e.c", "TEST(branchy) {\n"    /* 1 */
                                                         "#ifdef _WIN32\n"      /* 2 */
                                                         "    if (win()) {\n"   /* 3 */
                                                         "#elif defined(X)\n"   /* 4 */
                                                         "    if (x()) {\n"     /* 5 */
                                                         "#else\n"              /* 6 */
                                                         "    if (posix()) {\n" /* 7 */
                                                         "#endif\n"             /* 8 */
                                                         "        PASS();\n"    /* 9 */
                                                         "    }\n"              /* 10 */
                                                         "    FAIL(\"no\");\n"  /* 11 */
                                                         "}\n"                  /* 12 */
                                                         "TEST(next) {\n"       /* 13 */
                                                         "    PASS();\n"        /* 14 */
                                                         "}\n"                  /* 15 */
                                                         "SUITE(e) {\n"         /* 16 */
                                                         "    RUN_TEST(branchy);\n"
                                                         "    RUN_TEST(next);\n"
                                                         "}\n"}};
    cbm_test_model_t *m = model_of(src, 1);
    ASSERT_NOT_NULL(m);
    ASSERT_EQ(count_cases(m), 2);
    ASSERT_EQ(case_named(m, "branchy")->end_line, 12);
    ASSERT_NOT_NULL(case_named(m, "next"));
    ASSERT_EQ(case_named(m, "next")->start_line, 13);
    ASSERT_EQ(count_suites(m), 1);
    ASSERT_EQ(count_registrations(m), 2);
    cbm_test_model_free(m);
    PASS();
}

/* A registration resolves to the TEST of that name in the SAME file. A test
 * a macro generates has no TEST of its own, and a same-named test in another
 * file is a different test. */
TEST(test_model_resolves_registrations_in_their_own_file) {
    static const tm_source_t src[] = {{"tests/test_f.c", "#define GEN(n) static int n(void)\n"
                                                         "GEN(generated) { return 0; }\n"
                                                         "TEST(written) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "SUITE(f) {\n"
                                                         "    RUN_TEST(written);\n"
                                                         "    RUN_TEST(generated);\n"
                                                         "    RUN_TEST(elsewhere);\n"
                                                         "}\n"},
                                      {"tests/test_g.c", "TEST(elsewhere) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "TEST(unregistered) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "SUITE(g) {\n"
                                                         "    RUN_TEST(elsewhere);\n"
                                                         "}\n"}};
    cbm_test_model_t *m = model_of(src, 2);
    ASSERT_NOT_NULL(m);
    int n = 0;
    const cbm_test_registration_t *regs = cbm_test_model_registrations(m, &n);
    ASSERT_EQ(n, 4);
    ASSERT_STR_EQ(regs[0].test, "written");
    ASSERT_TRUE(regs[0].resolved);
    ASSERT_STR_EQ(regs[1].test, "generated");
    ASSERT_FALSE(regs[1].resolved);
    ASSERT_STR_EQ(regs[2].test, "elsewhere");
    ASSERT_STR_EQ(regs[2].file, "tests/test_f.c");
    ASSERT_FALSE(regs[2].resolved);
    ASSERT_STR_EQ(regs[3].test, "elsewhere");
    ASSERT_STR_EQ(regs[3].file, "tests/test_g.c");
    ASSERT_TRUE(regs[3].resolved);
    /* A macro's own definition line registers nothing. */
    ASSERT_EQ(count_cases(m), 3);
    cbm_test_model_free(m);
    PASS();
}

/* A file may register tests through a macro of its own. The names are then
 * out of the scanner's reach, and the suite must say so: it can only run
 * whole. The macro's body itself registers nothing. */
TEST(test_model_marks_suites_that_register_through_a_macro) {
    static const tm_source_t src[] = {{"tests/test_m.c",
                                       "#define RUN_ROW(test_name_, lang_) RUN_TEST(test_name_);\n"
                                       "#define ROWS(X) X(row_one, c) X(row_two, go)\n"
                                       "TEST(plain) {\n"
                                       "    PASS();\n"
                                       "}\n"
                                       "SUITE(m) {\n"
                                       "    RUN_TEST(plain);\n"
                                       "    ROWS(RUN_ROW)\n"
                                       "}\n"},
                                      {"tests/test_n.c", "#define UNRELATED(x) ((x) + 1)\n"
                                                         "TEST(only) {\n"
                                                         "    PASS();\n"
                                                         "}\n"
                                                         "SUITE(n) {\n"
                                                         "    RUN_TEST(only);\n"
                                                         "}\n"}};
    cbm_test_model_t *m = model_of(src, 2);
    ASSERT_NOT_NULL(m);
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(m, &n);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(suites[0].name, "m");
    ASSERT_TRUE(suites[0].macro_registrations);
    ASSERT_STR_EQ(suites[1].name, "n");
    ASSERT_FALSE(suites[1].macro_registrations);
    ASSERT_EQ(count_registrations(m), 2);
    ASSERT_NULL(registration_of(m, "test_name_"));
    cbm_test_model_free(m);
    PASS();
}

/* The runner's list of suites is the runnable universe. The macro's own
 * #define names no suite. */
TEST(test_model_reads_the_runner_suites) {
    static const tm_source_t src[] = {
        {"tests/test_main.c", "#define RUN_SELECTED_SUITE(name) run_suite(#name, suite_##name)\n"
                              "#define RUN_SELECTED_SUITE_PERF(name) \\\n"
                              "    run_perf_suite(#name, suite_##name)\n"
                              "int main(void) {\n"
                              "    RUN_SELECTED_SUITE(zeta);\n"
                              "    RUN_SELECTED_SUITE(alpha);\n"
                              "    RUN_SELECTED_SUITE_PERF(bench);\n"
                              "    RUN_SELECTED_SUITE(alpha);\n"
                              "    return 0;\n"
                              "}\n"}};
    cbm_test_model_t *m = model_of(src, 1);
    ASSERT_NOT_NULL(m);
    int n = 0;
    const cbm_test_runner_suite_t *suites = cbm_test_model_runner_suites(m, &n);
    ASSERT_EQ(n, 3);
    ASSERT_STR_EQ(suites[0].name, "alpha");
    ASSERT_FALSE(suites[0].perf);
    ASSERT_STR_EQ(suites[1].name, "bench");
    ASSERT_TRUE(suites[1].perf);
    ASSERT_STR_EQ(suites[2].name, "zeta");
    ASSERT_FALSE(suites[2].perf);
    cbm_test_model_free(m);
    PASS();
}

/* The model is a function of the sources, not of the order they were read
 * in: a directory listing differs between file systems. */
TEST(test_model_order_is_independent_of_input_order) {
    static const tm_source_t forward[] = {{"tests/test_a.c", "TEST(a1) {\n"
                                                             "    PASS();\n"
                                                             "}\n"
                                                             "SUITE(a) {\n"
                                                             "    RUN_TEST(a1);\n"
                                                             "}\n"},
                                          {"tests/test_b.c", "TEST(b1) {\n"
                                                             "    PASS();\n"
                                                             "}\n"
                                                             "SUITE(b) {\n"
                                                             "    RUN_TEST(b1);\n"
                                                             "}\n"}};
    const tm_source_t backward[] = {forward[1], forward[0]};
    cbm_test_model_t *m1 = model_of(forward, 2);
    cbm_test_model_t *m2 = model_of(backward, 2);
    ASSERT_NOT_NULL(m1);
    ASSERT_NOT_NULL(m2);
    int n1 = 0;
    int n2 = 0;
    const cbm_test_case_t *c1 = cbm_test_model_cases(m1, &n1);
    const cbm_test_case_t *c2 = cbm_test_model_cases(m2, &n2);
    ASSERT_EQ(n1, 2);
    ASSERT_EQ(n2, 2);
    for (int i = 0; i < n1; i++) {
        ASSERT_STR_EQ(c1[i].name, c2[i].name);
        ASSERT_STR_EQ(c1[i].file, c2[i].file);
    }
    ASSERT_STR_EQ(c1[0].name, "a1");
    const cbm_test_registration_t *r1 = cbm_test_model_registrations(m1, &n1);
    const cbm_test_registration_t *r2 = cbm_test_model_registrations(m2, &n2);
    ASSERT_EQ(n1, n2);
    for (int i = 0; i < n1; i++) {
        ASSERT_STR_EQ(r1[i].test, r2[i].test);
        ASSERT_STR_EQ(r1[i].suite, r2[i].suite);
        ASSERT_TRUE(r1[i].resolved && r2[i].resolved);
    }
    cbm_test_model_free(m1);
    cbm_test_model_free(m2);
    PASS();
}

/* Text that ends in the middle of a comment, a literal or a body must end the
 * scan, not run past the buffer. The text is deliberately not NUL-terminated
 * where the scanner would look. */
TEST(test_model_survives_truncated_sources) {
    static const char *const shapes[] = {
        "TEST(open) {\n    PASS();\n",
        "TEST(",
        "TEST(x",
        "TEST(x)",
        "SUITE(s) { RUN_TEST(",
        "/* never closed",
        "const char *s = \"never closed",
        "char c = '",
        "int half = 1 /",
        "const char *s = \"ends in a backslash\\",
        "#define CONTINUED \\",
        "/* ends in a star *",
        "#if",
        "#ifdef A\nTEST(a) {\n",
        "void suite_",
        "void suite_x(void",
        "}}}}\nTEST(after_stray_braces) {\n}\n",
        "#endif\n#endif\n#else\nTEST(after_stray_directives) {\n}\n",
    };
    for (size_t i = 0; i < sizeof(shapes) / sizeof(shapes[0]); i++) {
        size_t len = strlen(shapes[i]);
        char *exact = malloc(len ? len : 1); /* no terminator: ASan sees an over-read */
        ASSERT_NOT_NULL(exact);
        memcpy(exact, shapes[i], len);
        cbm_test_model_t *m = cbm_test_model_new(cbm_test_conventions_cbm());
        ASSERT_NOT_NULL(m);
        ASSERT_TRUE(cbm_test_model_add_source(m, "tests/test_t.c", exact, len));
        ASSERT_TRUE(cbm_test_model_finish(m));
        free(exact);
        if (strstr(shapes[i], "after_stray_braces")) {
            ASSERT_NOT_NULL(case_named(m, "after_stray_braces"));
        }
        if (strstr(shapes[i], "after_stray_directives")) {
            ASSERT_NOT_NULL(case_named(m, "after_stray_directives"));
        }
        cbm_test_model_free(m);
    }
    PASS();
}

/* ── Part 2: the diff reader ─────────────────────────────────────── */

static cbm_diff_t *diff_of(const char *text) {
    return cbm_diff_parse(text, strlen(text));
}

static const cbm_diff_file_t *diff_file(const cbm_diff_t *d, const char *path) {
    int n = 0;
    const cbm_diff_file_t *files = cbm_diff_files(d, &n);
    for (int i = 0; i < n; i++) {
        if (strcmp(files[i].path, path) == 0) {
            return &files[i];
        }
    }
    return NULL;
}

static int diff_file_count(const cbm_diff_t *d) {
    int n = 0;
    (void)cbm_diff_files(d, &n);
    return n;
}

TEST(test_diff_reads_files_and_hunks) {
    cbm_diff_t *d = diff_of("diff --git a/src/b.c b/src/b.c\n"
                            "index 1111111..2222222 100644\n"
                            "--- a/src/b.c\n"
                            "+++ b/src/b.c\n"
                            "@@ -3 +3 @@ static int helper(void)\n"
                            "-    return 1;\n"
                            "+    return 2;\n"
                            "@@ -10,2 +10,3 @@\n"
                            "-old a\n"
                            "-old b\n"
                            "+new a\n"
                            "+new b\n"
                            "+new c\n"
                            "diff --git a/src/a.c b/src/a.c\n"
                            "index 3333333..4444444 100644\n"
                            "--- a/src/a.c\n"
                            "+++ b/src/a.c\n"
                            "@@ -7,0 +8,2 @@\n"
                            "+added one\n"
                            "+added two\n");
    ASSERT_NOT_NULL(d);
    ASSERT_TRUE(cbm_diff_complete(d));
    int n = 0;
    const cbm_diff_file_t *files = cbm_diff_files(d, &n);
    ASSERT_EQ(n, 2);
    /* Ordered by path, whatever order the text had. */
    ASSERT_STR_EQ(files[0].path, "src/a.c");
    ASSERT_STR_EQ(files[1].path, "src/b.c");
    ASSERT_FALSE(files[1].created || files[1].deleted || files[1].binary);

    ASSERT_EQ(files[1].hunk_count, 2);
    const cbm_diff_hunk_t *h = files[1].hunks;
    ASSERT_EQ(h[0].start, 3);
    ASSERT_EQ(h[0].count, 1);
    ASSERT_EQ(h[0].removed_count, 1);
    ASSERT_EQ(h[0].added_count, 1);
    ASSERT_STR_EQ(h[0].removed[0], "    return 1;");
    ASSERT_STR_EQ(h[0].added[0], "    return 2;");
    ASSERT_EQ(h[1].start, 10);
    ASSERT_EQ(h[1].count, 3);
    ASSERT_EQ(h[1].removed_count, 2);
    ASSERT_EQ(h[1].added_count, 3);
    ASSERT_STR_EQ(h[1].removed[1], "old b");
    ASSERT_STR_EQ(h[1].added[2], "new c");

    ASSERT_EQ(files[0].hunk_count, 1);
    ASSERT_EQ(files[0].hunks[0].start, 8);
    ASSERT_EQ(files[0].hunks[0].count, 2);
    ASSERT_EQ(files[0].hunks[0].removed_count, 0);
    ASSERT_STR_EQ(files[0].hunks[0].added[1], "added two");
    cbm_diff_free(d);
    PASS();
}

/* A hunk that only removes lines has no line of its own on the new side: it
 * names the line it follows. Created and deleted files say so. */
TEST(test_diff_reads_removals_creations_and_deletions) {
    cbm_diff_t *d = diff_of("diff --git a/gone.c b/gone.c\n"
                            "deleted file mode 100644\n"
                            "index 1111111..0000000\n"
                            "--- a/gone.c\n"
                            "+++ /dev/null\n"
                            "@@ -1,2 +0,0 @@\n"
                            "-int gone(void);\n"
                            "-int also_gone(void);\n"
                            "diff --git a/kept.c b/kept.c\n"
                            "index 1111111..2222222 100644\n"
                            "--- a/kept.c\n"
                            "+++ b/kept.c\n"
                            "@@ -5,2 +4,0 @@ int kept(void)\n"
                            "-    removed();\n"
                            "-    removed_too();\n"
                            "diff --git a/new.c b/new.c\n"
                            "new file mode 100644\n"
                            "index 0000000..3333333\n"
                            "--- /dev/null\n"
                            "+++ b/new.c\n"
                            "@@ -0,0 +1 @@\n"
                            "+int fresh(void);\n"
                            "diff --git a/empty.c b/empty.c\n"
                            "new file mode 100644\n"
                            "index 0000000..e69de29\n");
    ASSERT_NOT_NULL(d);
    ASSERT_TRUE(cbm_diff_complete(d));
    ASSERT_EQ(diff_file_count(d), 4);
    const cbm_diff_file_t *gone = diff_file(d, "gone.c");
    const cbm_diff_file_t *kept = diff_file(d, "kept.c");
    const cbm_diff_file_t *fresh = diff_file(d, "new.c");
    const cbm_diff_file_t *empty = diff_file(d, "empty.c");
    ASSERT_NOT_NULL(gone);
    ASSERT_NOT_NULL(kept);
    ASSERT_NOT_NULL(fresh);
    ASSERT_NOT_NULL(empty);
    ASSERT_TRUE(gone->deleted);
    ASSERT_FALSE(gone->created);
    ASSERT_EQ(gone->hunks[0].count, 0);
    ASSERT_EQ(gone->hunks[0].removed_count, 2);
    ASSERT_FALSE(kept->deleted || kept->created);
    ASSERT_EQ(kept->hunk_count, 1);
    ASSERT_EQ(kept->hunks[0].start, 4);
    ASSERT_EQ(kept->hunks[0].count, 0);
    ASSERT_EQ(kept->hunks[0].added_count, 0);
    ASSERT_STR_EQ(kept->hunks[0].removed[0], "    removed();");
    ASSERT_TRUE(fresh->created);
    ASSERT_EQ(fresh->hunks[0].start, 1);
    ASSERT_EQ(fresh->hunks[0].count, 1);
    ASSERT_TRUE(empty->created);
    ASSERT_EQ(empty->hunk_count, 0);
    cbm_diff_free(d);
    PASS();
}

/* The counts in the hunk header decide what a line is, not its look: removed
 * and added text can read exactly like a file header, a hunk header or the
 * start of the next file. */
TEST(test_diff_content_that_looks_like_structure_stays_content) {
    cbm_diff_t *d = diff_of("diff --git a/notes.txt b/notes.txt\n"
                            "index 1111111..2222222 100644\n"
                            "--- a/notes.txt\n"
                            "+++ b/notes.txt\n"
                            "@@ -1,2 +1,4 @@\n"
                            "--- a/looks/like/a/header\n"
                            "-diff --git a/x b/x\n"
                            "+++ b/looks/like/a/header\n"
                            "+@@ -1 +1 @@\n"
                            "+diff --git a/y b/y\n"
                            "+\n"
                            "@@ -9 +11 @@\n"
                            "-last\n"
                            "\\ No newline at end of file\n"
                            "+last line\n"
                            "\\ No newline at end of file\n");
    ASSERT_NOT_NULL(d);
    ASSERT_TRUE(cbm_diff_complete(d));
    ASSERT_EQ(diff_file_count(d), 1);
    const cbm_diff_file_t *f = diff_file(d, "notes.txt");
    ASSERT_NOT_NULL(f);
    ASSERT_EQ(f->hunk_count, 2);
    ASSERT_EQ(f->hunks[0].removed_count, 2);
    ASSERT_EQ(f->hunks[0].added_count, 4);
    ASSERT_STR_EQ(f->hunks[0].removed[0], "-- a/looks/like/a/header");
    ASSERT_STR_EQ(f->hunks[0].removed[1], "diff --git a/x b/x");
    ASSERT_STR_EQ(f->hunks[0].added[0], "++ b/looks/like/a/header");
    ASSERT_STR_EQ(f->hunks[0].added[1], "@@ -1 +1 @@");
    ASSERT_STR_EQ(f->hunks[0].added[3], "");
    /* The "no newline" note is neither an old nor a new line. */
    ASSERT_EQ(f->hunks[1].removed_count, 1);
    ASSERT_EQ(f->hunks[1].added_count, 1);
    ASSERT_STR_EQ(f->hunks[1].added[0], "last line");
    cbm_diff_free(d);
    PASS();
}

TEST(test_diff_reads_binary_mode_only_and_awkward_paths) {
    cbm_diff_t *d = diff_of("diff --git a/assets/logo.png b/assets/logo.png\n"
                            "index 1111111..2222222 100644\n"
                            "Binary files a/assets/logo.png and b/assets/logo.png differ\n"
                            "diff --git a/dir b/with space.c b/dir b/with space.c\n"
                            "index 1111111..2222222 100644\n"
                            "--- a/dir b/with space.c\n"
                            "+++ b/dir b/with space.c\n"
                            "@@ -1 +1 @@\n"
                            "-a\r\n"
                            "+b\r\n"
                            "diff --git a/scripts/run.sh b/scripts/run.sh\n"
                            "old mode 100644\n"
                            "new mode 100755\n");
    ASSERT_NOT_NULL(d);
    ASSERT_TRUE(cbm_diff_complete(d));
    ASSERT_EQ(diff_file_count(d), 3);
    const cbm_diff_file_t *logo = diff_file(d, "assets/logo.png");
    ASSERT_NOT_NULL(logo);
    ASSERT_TRUE(logo->binary);
    ASSERT_EQ(logo->hunk_count, 0);
    /* " b/" inside a path does not split it. */
    const cbm_diff_file_t *spaced = diff_file(d, "dir b/with space.c");
    ASSERT_NOT_NULL(spaced);
    ASSERT_FALSE(spaced->binary);
    /* A carriage return is part of the line, as git printed it. */
    ASSERT_STR_EQ(spaced->hunks[0].added[0], "b\r");
    const cbm_diff_file_t *mode = diff_file(d, "scripts/run.sh");
    ASSERT_NOT_NULL(mode);
    ASSERT_EQ(mode->hunk_count, 0);
    cbm_diff_free(d);

    d = diff_of("");
    ASSERT_NOT_NULL(d);
    ASSERT_TRUE(cbm_diff_complete(d));
    ASSERT_EQ(diff_file_count(d), 0);
    cbm_diff_free(d);
    PASS();
}

/* Whatever the reader cannot account for makes the diff incomplete: the
 * caller then knows that what it holds is not the whole change. The files it
 * did read are still there. */
TEST(test_diff_says_when_it_did_not_read_everything) {
    static const struct {
        const char *why;
        const char *text;
        int files;
    } cases[] = {
        {"a quoted path",
         "diff --git \"a/tab\\there.c\" \"b/tab\\there.c\"\n"
         "index 1111111..2222222 100644\n"
         "--- \"a/tab\\there.c\"\n"
         "+++ \"b/tab\\there.c\"\n"
         "@@ -1 +1 @@\n"
         "-a\n"
         "+b\n"
         "diff --git a/ok.c b/ok.c\n"
         "index 1111111..2222222 100644\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1 +1 @@\n"
         "-a\n"
         "+b\n",
         1},
        {"a hunk shorter than its header",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1,2 +1,2 @@\n"
         "-a\n"
         "+b\n",
         1},
        {"text that ends inside a hunk",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1,3 +1,3 @@\n"
         "-a\n",
         1},
        {"a count the text cannot hold",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1,900000000 +1,900000000 @@\n"
         "-a\n"
         "+b\n",
         1},
        {"a line before any file",
         "warning: something git printed\n"
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1 +1 @@\n"
         "-a\n"
         "+b\n",
         1},
        {"a line after a hunk that is no hunk",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1 +1 @@\n"
         "-a\n"
         "+b\n"
         "+one line too many\n",
         1},
        {"an unknown header line",
         "diff --git a/ok.c b/ok.c\n"
         "similarity index 90%\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n",
         1},
        {"a renamed pair",
         "diff --git a/old.c b/new.c\n"
         "--- a/old.c\n"
         "+++ b/new.c\n",
         0},
        {"a combined diff",
         "diff --cc merged.c\n"
         "index 1111111,2222222..3333333\n",
         0},
        {"a hunk header that is not one",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -x +1 @@\n",
         1},
        {"a hunk header without its end",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1,0 +1,0\n",
         1},
        {"text that ends between the old and the new lines",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1 +1 @@\n"
         "-a\n",
         1},
        {"added lines before the removed ones are done",
         "diff --git a/ok.c b/ok.c\n"
         "--- a/ok.c\n"
         "+++ b/ok.c\n"
         "@@ -1,2 +1 @@\n"
         "-a\n"
         "+b\n"
         "-c\n",
         1},
        {"only a quoted header", "diff --git \"a/tab\\there.c\" \"b/tab\\there.c\"\n", 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        size_t len = strlen(cases[i].text);
        char *exact = malloc(len ? len : 1); /* no terminator: ASan sees an over-read */
        ASSERT_NOT_NULL(exact);
        memcpy(exact, cases[i].text, len);
        cbm_diff_t *d = cbm_diff_parse(exact, len);
        free(exact);
        ASSERT_NOT_NULL(d);
        if (cbm_diff_complete(d) || diff_file_count(d) != cases[i].files) {
            printf("  case \"%s\": complete=%d files=%d\n", cases[i].why, cbm_diff_complete(d),
                   diff_file_count(d));
        }
        ASSERT_FALSE(cbm_diff_complete(d));
        ASSERT_EQ(diff_file_count(d), cases[i].files);
        cbm_diff_free(d);
    }
    PASS();
}

/* A tiny, truncated diff must not reserve memory proportional to a claimed
 * line count. Check retained allocator bytes, not RSS or whether a large
 * virtual allocation happens to succeed on this host. */
TEST(test_diff_rejects_impossible_counts_without_large_allocations) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_diff_t *d = diff_of("diff --git a/ok.c b/ok.c\n"
                            "--- a/ok.c\n"
                            "+++ b/ok.c\n"
                            "@@ -1,1000000 +1,1000000 @@\n"
                            "-a\n"
                            "+b\n");
    ASSERT_NOT_NULL(d);
    bool complete = cbm_diff_complete(d);
    size_t retained = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_diff_free(d);
    ASSERT_FALSE(complete);
    ASSERT_TRUE(retained >= before);
    /* Generous room for parser bookkeeping and allocator rounding; allocating
     * the two claimed pointer arrays would retain at least 8 MiB on 32-bit. */
    ASSERT_TRUE(retained - before < 1024 * 1024);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

typedef struct {
    char text[4096];
    size_t length;
    int hunks;
    int context_lines;
    bool overflow;
} diff_git_output_t;

static void diff_git_capture(const char *line, void *ud) {
    diff_git_output_t *output = ud;
    size_t length = strlen(line);
    if (length + 2 > sizeof(output->text) - output->length) {
        output->overflow = true;
        return;
    }
    memcpy(output->text + output->length, line, length);
    output->length += length;
    output->text[output->length++] = '\n';
    output->text[output->length] = '\0';
    output->hunks += strncmp(line, "@@ ", 3) == 0;
    output->context_lines += line[0] == ' ';
}

/* Split the production flags (literal, space-separated arguments) so this
 * exercises the same pins without a shell or a second copy of their values. */
static bool diff_git_run(const char *root, const char *config, diff_git_output_t *output) {
    char pins[] = CBM_TEST_IMPACT_GIT_PINS;
    char flags[] = CBM_TEST_IMPACT_DIFF_FLAGS;
    const char *git = "git";
#ifdef _WIN32
    git = cbm_find_cli("git", cbm_get_home_dir());
    if (!git) {
        return false;
    }
#endif
    const char *argv[48] = {git, "-C", root, "-c", config};
    size_t count = 5;
    char *groups[] = {pins, flags};
    for (size_t group = 0; group < 2; group++) {
        if (group == 1) {
            argv[count++] = "diff";
        }
        char *part = groups[group];
        while (*part) {
            while (*part == ' ') {
                part++;
            }
            if (!*part) {
                break;
            }
            if (count + 6 >= sizeof(argv) / sizeof(argv[0])) {
                return false;
            }
            argv[count++] = part;
            while (*part && *part != ' ') {
                part++;
            }
            if (*part) {
                *part++ = '\0';
            }
        }
    }
    argv[count++] = "--no-index";
    argv[count++] = "--";
    argv[count++] = "before.c";
    argv[count++] = "after.c";
    argv[count] = NULL;
    cbm_proc_opts_t options = {
        .bin = git,
        .argv = argv,
        .log_file = TH_PATH(root, "diff.log"),
        .on_log_line = diff_git_capture,
        .log_ud = output,
        .quiet_timeout_ms = 10000,
        .delete_log_on_exit = true,
        .strip_git_repo_env = true,
    };
    cbm_proc_result_t result = {0};
    return cbm_subprocess_run(&options, &result) == 0 && result.outcome == CBM_PROC_EXIT_NONZERO &&
           result.exit_code == 1 && !output->overflow;
}

TEST(test_diff_git_flags_ignore_inter_hunk_config) {
    char *root = th_mktempdir("cbm-diff-pins");
    ASSERT_NOT_NULL(root);
    bool written = th_write_file(TH_PATH(root, "before.c"),
                                 "old first\nkeep one\nkeep two\nkeep three\nold last\n") == 0 &&
                   th_write_file(TH_PATH(root, "after.c"),
                                 "new first\nkeep one\nkeep two\nkeep three\nnew last\n") == 0;
    diff_git_output_t baseline = {0};
    diff_git_output_t hostile = {0};
    bool baseline_ok = written && diff_git_run(root, "diff.interHunkContext=0", &baseline);
    bool hostile_ok = written && diff_git_run(root, "diff.interHunkContext=999", &hostile);
    int cleanup = th_rmtree(root);
    ASSERT_TRUE(written);
    ASSERT_TRUE(baseline_ok);
    ASSERT_TRUE(hostile_ok);
    ASSERT_EQ(cleanup, 0);
    ASSERT_EQ(baseline.hunks, 2);
    ASSERT_EQ(baseline.context_lines, 0);
    ASSERT_EQ(hostile.hunks, 2);
    ASSERT_EQ(hostile.context_lines, 0);
    ASSERT_STR_EQ(hostile.text, baseline.text);
    PASS();
}

static cbm_coverage_map_t *coverage_of(const char *functions, const char *tests) {
    return cbm_coverage_map_parse(functions, strlen(functions), tests, strlen(tests));
}

TEST(test_coverage_map_reads_sources_setup_and_incomplete_tests) {
    cbm_coverage_map_t *map = coverage_of(
        "0\tsrc/a.c\twork\n1\tsrc/b.c\twork\n2\t\texternal\n3\tinclude/shared.h\tinline_fn\n",
        "z:other\tcomplete\t\t1\n"
        "a:t:parameter\tcomplete\t\t0 2 3\n"
        "a:missing\tincomplete\tno parent profile\t\n"
        "z:*\tcomplete\t\t\n"
        "a:*\tcomplete\t\t0\n"
        "a:empty\tcomplete\t\t\n");
    ASSERT_NOT_NULL(map);
    int count = 0;
    const cbm_coverage_function_t *functions = cbm_coverage_map_functions(map, &count);
    ASSERT_EQ(count, 4);
    ASSERT_STR_EQ(functions[3].file, "include/shared.h");
    const cbm_coverage_function_t *function =
        cbm_coverage_map_find_function(map, "src/b.c", "work");
    ASSERT_NOT_NULL(function);
    ASSERT_EQ(function->id, 1);
    function = cbm_coverage_map_find_function(map, "", "external");
    ASSERT_NOT_NULL(function);
    ASSERT_EQ(function->id, 2);
    ASSERT_TRUE(cbm_coverage_map_find_function(map, "absent.c", "work") == NULL);
    const cbm_coverage_test_t *rows = cbm_coverage_map_tests(map, &count);
    ASSERT_EQ(count, 6);
    ASSERT_STR_EQ(rows[0].suite, "a");
    ASSERT_STR_EQ(rows[0].name, "*");
    const cbm_coverage_test_t *row = cbm_coverage_map_find_test(map, "a", "t:parameter");
    ASSERT_NOT_NULL(row);
    ASSERT_TRUE(row->complete);
    ASSERT_EQ(row->function_count, 3);
    int changed[] = {1, 3};
    ASSERT_TRUE(cbm_coverage_test_intersects(row, changed, 2));
    ASSERT_FALSE(cbm_coverage_test_intersects(row, changed, 1));
    int unsorted[] = {9, 0, 3};
    ASSERT_TRUE(cbm_coverage_test_intersects(row, unsorted, 3));
    row = cbm_coverage_map_find_test(map, "a", "missing");
    ASSERT_NOT_NULL(row);
    ASSERT_FALSE(row->complete);
    ASSERT_STR_EQ(row->reason, "no parent profile");
    ASSERT_EQ(row->function_count, 0);
    ASSERT_TRUE(cbm_coverage_map_find_test(map, "a", "unknown") == NULL);
    cbm_coverage_map_free(map);
    PASS();
}

TEST(test_coverage_map_reads_exact_lengths_and_crlf) {
    const char functions[] = {'0', '\t', 'x', '\r', 'y', '\t', 'f', '\r', '\n'};
    const char tests[] = {'s', ':', '*', '\t', 'i',  'n', 'c',  'o', 'm',  'p',
                          'l', 'e', 't', 'e',  '\t', 'x', '\t', '0', '\r', '\n'};
    cbm_coverage_map_t *map =
        cbm_coverage_map_parse(functions, sizeof(functions), tests, sizeof(tests));
    ASSERT_NOT_NULL(map);
    ASSERT_NOT_NULL(cbm_coverage_map_find_function(map, "x\ry", "f"));
    const cbm_coverage_test_t *row = cbm_coverage_map_find_test(map, "s", "*");
    ASSERT_NOT_NULL(row);
    ASSERT_FALSE(row->complete);
    ASSERT_STR_EQ(row->reason, "x");
    int id = 0;
    ASSERT_TRUE(cbm_coverage_test_intersects(row, &id, 1));
    cbm_coverage_map_free(map);
    PASS();
}

TEST(test_coverage_map_rejects_inconsistent_artifacts) {
    const char *functions = "0\tsrc/a.c\tf\n1\tsrc/b.c\tg\n";
    const char *valid = "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t1\n";
    const char *bad_functions[] = {
        "1\tsrc/a.c\tf\n2\tsrc/b.c\tg\n",          /* first ID missing */
        "0\tsrc/a.c\tf\n0\tsrc/b.c\tg\n",          /* duplicate ID */
        "0\tsrc/a.c\tf\n2\tsrc/b.c\tg\n",          /* sparse IDs */
        "0\tsrc/a.c\tf\n1\tsrc/a.c\tf\n",          /* duplicate source/name */
        "0\tsrc/a.c\t\n1\tsrc/b.c\tg\n",           /* empty name */
        "4294967296\tsrc/a.c\tf\n1\tsrc/b.c\tg\n", /* integer overflow */
        "0\tsrc/a.c\tf",                           /* truncated record */
        "0\tsrc/a.c\tf\textra\n1\tsrc/b.c\tg\n",   /* extra field */
    };
    for (size_t i = 0; i < sizeof(bad_functions) / sizeof(bad_functions[0]); i++) {
        cbm_coverage_map_t *map = coverage_of(bad_functions[i], valid);
        bool rejected = map == NULL;
        cbm_coverage_map_free(map);
        ASSERT_TRUE(rejected);
    }
    const char *bad_tests[] = {
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t2\n",   /* unknown function */
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t1 1\n", /* duplicate ID */
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t1 0\n", /* reversed IDs */
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t-1\n",
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t999999999999999999999\n",
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t1 \n",
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t 1\n",
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t1x\n",
        "s:*\tcomplete\t\t0\ns:t\tcomplete\t\t1\ns:t\tcomplete\t\t1\n",
        "s:*\tunknown\t\t0 1\n",
        "s:*\tcomplete\tproblem\t0 1\n",
        "s:*\tincomplete\t\t0 1\n",
        "s\tcomplete\t\t0 1\n", /* no suite separator */
        ":*\tcomplete\t\t0 1\n",
        "s:*\tcomplete\t\t0\ns:\tcomplete\t\t1\n",
        "s:t\tcomplete\t\t0 1\n",                  /* setup omitted */
        "s:*\tcomplete\t\t0\n",                    /* orphan function row */
        "s:*\tcomplete\t\t0 1\ns:t\tcomplete\t\t", /* truncated final record */
        "s:*\tcomplete\t\t0 1\textra\n",
    };
    for (size_t i = 0; i < sizeof(bad_tests) / sizeof(bad_tests[0]); i++) {
        cbm_coverage_map_t *map = coverage_of(functions, bad_tests[i]);
        bool rejected = map == NULL;
        cbm_coverage_map_free(map);
        if (!rejected) {
            printf("  malformed coverage case %zu was accepted\n", i);
        }
        ASSERT_TRUE(rejected);
    }
    const char embedded_nul[] = "s:*\tcomplete\t\t0\0 1\n";
    cbm_coverage_map_t *map = cbm_coverage_map_parse(functions, strlen(functions), embedded_nul,
                                                     sizeof(embedded_nul) - 1);
    bool rejected = map == NULL;
    cbm_coverage_map_free(map);
    ASSERT_TRUE(rejected);
    map = coverage_of("0\tsrc/a.c\tf\n1\tsrc/b.c\tg", "s:*\tcomplete\t\t0\n");
    rejected = map == NULL;
    cbm_coverage_map_free(map);
    ASSERT_TRUE(rejected);
    PASS();
}

#define COVERAGE_META_COMMIT "193b0a4d066f6ced193d9b68a673b5d5e19c4a56"
#define COVERAGE_META_SUITES                                                      \
    "[{\"suite\":\"b\",\"tests\":1,\"incomplete\":1,\"exit\":-1,\"wall_s\":0.2}," \
    "{\"suite\":\"a\",\"tests\":1,\"incomplete\":0,\"exit\":0,\"wall_s\":0.1}]"

static cbm_coverage_map_t *coverage_metadata_fixture(void) {
    return coverage_of("0\tsrc/a.c\tf\n1\tsrc/b.c\tg\n",
                       "a:*\tcomplete\t\t0\na:t\tcomplete\t\t1\n"
                       "b:*\tincomplete\tsuite did not exit 0\t\n"
                       "b:t\tincomplete\tsuite did not exit 0\t0\n");
}

static const char *coverage_metadata_json(const char *functions, const char *tests,
                                          const char *incomplete, const char *suites,
                                          const char *extra) {
    static char text[4096];
    snprintf(text, sizeof(text),
             "{\"format\":1,\"commit\":\"" COVERAGE_META_COMMIT "\","
             "\"platform\":\"darwin\",\"llvm_profdata\":\"test LLVM\","
             "\"functions\":%s,\"functions_compiled\":3,\"tests\":%s,\"incomplete\":%s,"
             "\"suites\":%s%s}",
             functions, tests, incomplete, suites, extra);
    return text;
}

static const char *coverage_metadata_replace(const char *from, const char *to) {
    const char *base = coverage_metadata_json("2", "2", "1", COVERAGE_META_SUITES, "");
    const char *position = strstr(base, from);
    static char text[4096];
    if (!position)
        return NULL;
    snprintf(text, sizeof(text), "%.*s%s%s", (int)(position - base), base, to,
             position + strlen(from));
    return text;
}

static cbm_coverage_map_t *coverage_metadata_variant(bool setup_complete, bool test_complete) {
    char text[1024];
    snprintf(text, sizeof(text),
             "a:*\tcomplete\t\t0\na:t\tcomplete\t\t1\n"
             "b:*\t%s\t%s\t\nb:t\t%s\t%s\t0\n",
             setup_complete ? "complete" : "incomplete", setup_complete ? "" : "unreadable setup",
             test_complete ? "complete" : "incomplete", test_complete ? "" : "unreadable profile");
    return coverage_of("0\tsrc/a.c\tf\n1\tsrc/b.c\tg\n", text);
}

TEST(test_coverage_metadata_matches_rows_and_commit) {
    cbm_coverage_map_t *map = coverage_metadata_fixture();
    ASSERT_NOT_NULL(map);
    const char *json = coverage_metadata_json("2", "2", "1", COVERAGE_META_SUITES, "");
    bool matched = cbm_coverage_map_metadata_matches(map, json, strlen(json), COVERAGE_META_COMMIT);
    cbm_coverage_map_free(map);
    ASSERT_TRUE(matched);
    PASS();
}

TEST(test_coverage_metadata_accepts_builder_variations) {
    cbm_coverage_map_t *map = coverage_metadata_fixture();
    ASSERT_NOT_NULL(map);
    const char *long_commit = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
    struct {
        const char *from;
        const char *to;
        const char *commit;
    } cases[] = {
        {"\"functions_compiled\":3,", "", COVERAGE_META_COMMIT},
        {"\"functions_compiled\":3", "\"functions_compiled\":0", COVERAGE_META_COMMIT},
        {"\"exit\":-1", "\"exit\":0", COVERAGE_META_COMMIT},
        {",\"wall_s\":0.2", "", COVERAGE_META_COMMIT},
        {COVERAGE_META_COMMIT, long_commit, long_commit},
    };
    bool accepted = true;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *json = coverage_metadata_replace(cases[i].from, cases[i].to);
        bool matched =
            json && cbm_coverage_map_metadata_matches(map, json, strlen(json), cases[i].commit);
        if (!matched)
            printf("  valid metadata variation %zu was rejected\n", i);
        accepted = accepted && matched;
    }
    cbm_coverage_map_free(map);
    ASSERT_TRUE(accepted);
    map = coverage_metadata_variant(false, true);
    ASSERT_NOT_NULL(map);
    const char *json =
        coverage_metadata_json("2", "2", "0",
                               "[{\"suite\":\"a\",\"tests\":1,\"incomplete\":0,\"exit\":0},"
                               "{\"suite\":\"b\",\"tests\":1,\"incomplete\":0,\"exit\":0}]",
                               "");
    bool matched = cbm_coverage_map_metadata_matches(map, json, strlen(json), COVERAGE_META_COMMIT);
    cbm_coverage_map_free(map);
    ASSERT_TRUE(matched); /* incomplete setup does not count as an incomplete test */
    PASS();
}

TEST(test_coverage_metadata_rejects_inconsistent_artifacts) {
    cbm_coverage_map_t *map = coverage_metadata_fixture();
    ASSERT_NOT_NULL(map);
    struct {
        const char *functions;
        const char *tests;
        const char *incomplete;
        const char *suites;
        const char *extra;
    } cases[] = {
        {"3", "2", "1", COVERAGE_META_SUITES, ""}, /* recovered stale function count */
        {"2", "3", "1", COVERAGE_META_SUITES, ""},
        {"2", "2", "0", COVERAGE_META_SUITES, ""},
        {"2.0", "2", "1", COVERAGE_META_SUITES, ""},
        {"-1", "2", "1", COVERAGE_META_SUITES, ""},
        {"18446744073709551615", "2", "1", COVERAGE_META_SUITES, ""},
        {"\"2\"", "2", "1", COVERAGE_META_SUITES, ""},
        {"2", "2", "1", COVERAGE_META_SUITES, ",\"functions\":2"},
        {"2", "2", "1", COVERAGE_META_SUITES, ",\"commit\":\"" COVERAGE_META_COMMIT "\""},
        {"2", "2", "1", "[]", ""},
        {"2", "2", "1", "null", ""},
        {"2", "2", "1",
         "[{\"suite\":\"a\",\"tests\":1,\"incomplete\":0,\"exit\":0},"
         "{\"suite\":\"a\",\"tests\":1,\"incomplete\":0,\"exit\":0}]",
         ""},
        {"2", "2", "1",
         "[{\"suite\":\"a\",\"tests\":2,\"incomplete\":0,\"exit\":0},"
         "{\"suite\":\"b\",\"tests\":1,\"incomplete\":1,\"exit\":-1}]",
         ""},
        {"2", "2", "1",
         "[{\"suite\":\"a\",\"tests\":1,\"incomplete\":0,\"exit\":7},"
         "{\"suite\":\"b\",\"tests\":1,\"incomplete\":1,\"exit\":-1}]",
         ""},
        {"2", "2", "1",
         "[{\"suite\":\"absent\",\"tests\":1,\"incomplete\":0,\"exit\":0},"
         "{\"suite\":\"b\",\"tests\":1,\"incomplete\":1,\"exit\":-1}]",
         ""},
        {"2", "2", "1",
         "[{\"suite\":\"a\\u0000x\",\"tests\":1,\"incomplete\":0,\"exit\":0},"
         "{\"suite\":\"b\",\"tests\":1,\"incomplete\":1,\"exit\":-1}]",
         ""},
        {"2", "2", "1",
         "[{\"suite\":\"a\",\"tests\":1,\"tests\":1,\"incomplete\":0,\"exit\":0},"
         "{\"suite\":\"b\",\"tests\":1,\"incomplete\":1,\"exit\":-1}]",
         ""},
    };
    bool accepted_any = false;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const char *json =
            coverage_metadata_json(cases[i].functions, cases[i].tests, cases[i].incomplete,
                                   cases[i].suites, cases[i].extra);
        bool matched =
            cbm_coverage_map_metadata_matches(map, json, strlen(json), COVERAGE_META_COMMIT);
        if (matched)
            printf("  inconsistent metadata case %zu was accepted\n", i);
        accepted_any = accepted_any || matched;
    }
    struct {
        const char *from;
        const char *to;
    } replacements[] = {
        {"\"format\":1", "\"format\":2"},
        {"\"platform\":\"darwin\"", "\"platform\":\"\""},
        {"\"llvm_profdata\":\"test LLVM\"", "\"llvm_profdata\":42"},
        {"\"functions_compiled\":3", "\"functions_compiled\":-1"},
        {"\"functions_compiled\":3", "\"functions_compiled\":3.5"},
        {"\"wall_s\":0.2", "\"wall_s\":-0.2"},
    };
    for (size_t i = 0; i < sizeof(replacements) / sizeof(replacements[0]); i++) {
        const char *json = coverage_metadata_replace(replacements[i].from, replacements[i].to);
        ASSERT_NOT_NULL(json);
        bool matched =
            cbm_coverage_map_metadata_matches(map, json, strlen(json), COVERAGE_META_COMMIT);
        if (matched)
            printf("  invalid metadata replacement %zu was accepted\n", i);
        accepted_any = accepted_any || matched;
    }
    const char *bad_commit = "g93b0a4d066f6ced193d9b68a673b5d5e19c4a56";
    const char *changed_commit = coverage_metadata_replace(COVERAGE_META_COMMIT, bad_commit);
    ASSERT_NOT_NULL(changed_commit);
    accepted_any |=
        cbm_coverage_map_metadata_matches(map, changed_commit, strlen(changed_commit), bad_commit);
    const char *json = coverage_metadata_json("2", "2", "1", COVERAGE_META_SUITES, "");
    ASSERT_FALSE(cbm_coverage_map_metadata_matches(map, json, strlen(json),
                                                   "0000000000000000000000000000000000000000"));
    ASSERT_FALSE(cbm_coverage_map_metadata_matches(map, json, strlen(json), NULL));
    ASSERT_FALSE(
        cbm_coverage_map_metadata_matches(map, json, strlen(json) - 1, COVERAGE_META_COMMIT));
    ASSERT_FALSE(cbm_coverage_map_metadata_matches(map, "{}", 2, COVERAGE_META_COMMIT));
    cbm_coverage_map_free(map);
    ASSERT_FALSE(accepted_any);
    map = coverage_metadata_variant(true, false);
    ASSERT_NOT_NULL(map);
    json = coverage_metadata_json("2", "2", "1", COVERAGE_META_SUITES, "");
    bool matched = cbm_coverage_map_metadata_matches(map, json, strlen(json), COVERAGE_META_COMMIT);
    cbm_coverage_map_free(map);
    ASSERT_FALSE(matched); /* failed suite, incomplete test, but complete setup */
    map = coverage_metadata_variant(false, true);
    ASSERT_NOT_NULL(map);
    json = coverage_metadata_json("2", "2", "0",
                                  "[{\"suite\":\"a\",\"tests\":1,\"incomplete\":0,\"exit\":0},"
                                  "{\"suite\":\"b\",\"tests\":1,\"incomplete\":0,\"exit\":-1}]",
                                  "");
    matched = cbm_coverage_map_metadata_matches(map, json, strlen(json), COVERAGE_META_COMMIT);
    cbm_coverage_map_free(map);
    ASSERT_FALSE(matched); /* failed suite, incomplete setup, but complete test */
    PASS();
}

TEST(test_test_config_loads_globs_and_optional_absence) {
    char dir[512], path[600];
    snprintf(dir, sizeof(dir), "%s/cbm-impact-config-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(dir));
    snprintf(path, sizeof(path), "%s/project.json", dir);
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_test_config_t *config = cbm_test_config_load(path, true);
    ASSERT_NOT_NULL(config);
    ASSERT_EQ(strlen(cbm_test_config_digest(config)), 64);
    ASSERT_FALSE(cbm_test_config_is_test_file(config, "tests/example.c"));
    char missing_digest[65];
    snprintf(missing_digest, sizeof(missing_digest), "%s", cbm_test_config_digest(config));
    cbm_test_config_free(config);
    ASSERT_NULL(cbm_test_config_load(path, false));
    ASSERT_EQ(th_write_file(path, "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":["
                                  "\"spec/**\",\"**/*_test.c\"]}}}"),
              0);
    config = cbm_test_config_load(path, false);
    ASSERT_NOT_NULL(config);
    ASSERT_TRUE(cbm_test_config_is_test_file(config, "spec/nested/test.c"));
    ASSERT_TRUE(cbm_test_config_is_test_file(config, "src/parser_test.c"));
    ASSERT_TRUE(cbm_test_config_is_test_file(config, "parser_test.c"));
    ASSERT_FALSE(cbm_test_config_is_test_file(config, "src/parser.c"));
    ASSERT_FALSE(cbm_test_config_is_test_file(config, "other/spec/parser.c"));
    ASSERT_TRUE(strcmp(missing_digest, cbm_test_config_digest(config)) != 0);
    char digest[65];
    snprintf(digest, sizeof(digest), "%s", cbm_test_config_digest(config));
    cbm_test_config_free(config);
    config = cbm_test_config_load(path, false);
    ASSERT_NOT_NULL(config);
    ASSERT_STR_EQ(digest, cbm_test_config_digest(config));
    cbm_test_config_free(config);

    const char *empty[] = {"{}", "{\"extra_extensions\":{\".foo\":\"c\"}}",
                           "{\"test_impact\":{\"version\":1}}",
                           "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[]}}}"};
    for (size_t i = 0; i < sizeof(empty) / sizeof(empty[0]); i++) {
        ASSERT_EQ(th_write_file(path, empty[i]), 0);
        config = cbm_test_config_load(path, false);
        ASSERT_NOT_NULL(config);
        ASSERT_FALSE(cbm_test_config_is_test_file(config, "spec/example.c"));
        cbm_test_config_free(config);
    }
    char *boundary = malloc(65538);
    ASSERT_NOT_NULL(boundary);
    memset(boundary, ' ', 65537);
    boundary[0] = '{';
    boundary[1] = '}';
    boundary[65536] = '\0';
    ASSERT_EQ(th_write_file(path, boundary), 0);
    config = cbm_test_config_load(path, false);
    ASSERT_NOT_NULL(config);
    cbm_test_config_free(config);
    boundary[65536] = ' ';
    boundary[65537] = '\0';
    ASSERT_EQ(th_write_file(path, boundary), 0);
    ASSERT_NULL(cbm_test_config_load(path, false));
    free(boundary);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    th_rmtree(dir);
    PASS();
}

TEST(test_test_config_rejects_malformed_consumed_fields) {
    char dir[512], path[600];
    snprintf(dir, sizeof(dir), "%s/cbm-impact-invalid-config-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(dir));
    snprintf(path, sizeof(path), "%s/project.json", dir);
    const char *bad[] = {
        "",
        "[]",
        "{",
        "{\"test_impact\":null}",
        "{\"test_impact\":{}}",
        "{\"test_impact\":{\"version\":2}}",
        "{\"test_impact\":{\"version\":1.0}}",
        "{\"test_impact\":{\"version\":1,\"version\":1}}",
        "{\"test_impact\":{\"version\":1,\"tests\":[]}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":\"test*\"}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[3]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[\"\"]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[\"!spec/**\"]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[\"#spec/**\"]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[\"spec/**\\nother/**\"]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[\"spec/**\\u0000other/**\"]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":[],\"test_globs\":[]}}}",
    };
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        ASSERT_EQ(th_write_file(path, bad[i]), 0);
        cbm_test_config_t *config = cbm_test_config_load(path, true);
        bool accepted = config != NULL;
        cbm_test_config_free(config);
        if (accepted)
            printf("  invalid config case %zu accepted\n", i);
        ASSERT_FALSE(accepted);
    }
    ASSERT_NULL(cbm_test_config_load(dir, true));
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    th_rmtree(dir);
    PASS();
}

TEST(test_test_config_directory_globs_match_descendants) {
    char dir[512], path[600];
    snprintf(dir, sizeof(dir), "%s/cbm-impact-directory-globs-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(dir));
    snprintf(path, sizeof(path), "%s/project.json", dir);
    ASSERT_EQ(th_write_file(path, "{\"test_impact\":{\"version\":1,\"tests\":{\"test_globs\":["
                                  "\"tests/\",\"/spec/\",\"fixtures\"]}}}"),
              0);
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_test_config_t *config = cbm_test_config_load(path, false);
    ASSERT_NOT_NULL(config);
    ASSERT_TRUE(cbm_test_config_is_test_file(config, "tests/case.c"));
    ASSERT_TRUE(cbm_test_config_is_test_file(config, "nested/tests/case.c"));
    ASSERT_TRUE(cbm_test_config_is_test_file(config, "spec/case.c"));
    ASSERT_TRUE(cbm_test_config_is_test_file(config, "fixtures/data.c"));
    ASSERT_FALSE(cbm_test_config_is_test_file(config, "nested/spec/case.c"));
    ASSERT_FALSE(cbm_test_config_is_test_file(config, "tests"));
    ASSERT_FALSE(cbm_test_config_is_test_file(config, "src/testing.c"));
    cbm_test_config_free(config);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    th_rmtree(dir);
    PASS();
}

/* Rule B: static reach OR observed per-test execution, with uncertainty
 * retained. These fixtures deliberately have disjoint static/coverage hits. */
static const char *selection_source =
    "TEST(stat) {}\nTEST(cov) {}\nTEST(changed) {}\nTEST(quiet) {}\n"
    "SUITE(alpha) { RUN_TEST(stat); RUN_TEST(cov); RUN_TEST(changed); RUN_TEST(quiet); }\n"
    "void main(void) { RUN_SELECTED_SUITE(alpha); }\n";
static const char *selection_functions = "0\tsrc/product.c\tchanged_fn\n1\tsrc/other.c\tother\n";
static const char *selection_rows = "alpha:*\tcomplete\t\t1\nalpha:stat\tcomplete\t\t1\n"
                                    "alpha:cov\tcomplete\t\t0\nalpha:changed\tcomplete\t\t1\n"
                                    "alpha:quiet\tcomplete\t\t1\n";

static cbm_test_model_t *selection_model(const char *source) {
    tm_source_t sources[] = {{"tests/cases.c", source}};
    return model_of(sources, 1);
}

static cbm_coverage_map_t *selection_map(const char *rows) {
    return cbm_coverage_map_parse(selection_functions, strlen(selection_functions), rows,
                                  strlen(rows));
}

static cbm_test_selection_input_t selection_input(cbm_test_model_t *model, cbm_coverage_map_t *map,
                                                  cbm_test_reach_t *reach, int *changed) {
    return (cbm_test_selection_input_t){.model = model,
                                        .coverage = map,
                                        .reach = reach,
                                        .reach_count = 4,
                                        .changed_function_ids = changed,
                                        .changed_function_count = 1,
                                        .has_changes = true,
                                        .diff_complete = true,
                                        .inventory_complete = true,
                                        .static_complete = true,
                                        .coverage_admitted = true,
                                        .coverage_changes_complete = true};
}

static const cbm_test_selected_case_t *selection_case(const cbm_test_selection_t *result,
                                                      const char *suite, const char *test) {
    int count;
    const cbm_test_selected_case_t *rows = cbm_test_selection_cases(result, &count);
    for (int i = 0; i < count; i++)
        if (strcmp(rows[i].suite, suite) == 0 && strcmp(rows[i].test, test) == 0)
            return &rows[i];
    return NULL;
}

static unsigned selection_case_reasons(const cbm_test_selection_t *result, const char *suite,
                                       const char *test) {
    const cbm_test_selected_case_t *row = selection_case(result, suite, test);
    return row ? row->reasons : 0;
}

#define SELECTION_REACH                                        \
    {                                                          \
        {"tests/cases.c", "stat", true, true, false},          \
            {"tests/cases.c", "cov", true, false, false},      \
            {"tests/cases.c", "changed", true, false, true}, { \
            "tests/cases.c", "quiet", true, false, false       \
        }                                                      \
    }

TEST(test_selection_unions_static_coverage_and_changed_tests) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_test_model_t *model = selection_model(selection_source);
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = SELECTION_REACH;
    int changed[] = {0};
    cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
    cbm_test_selection_t *result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(cbm_test_selection_run_all(result), 0);
    int count;
    const cbm_test_selected_case_t *cases = cbm_test_selection_cases(result, &count);
    ASSERT_EQ(count, 3);
    ASSERT_STR_EQ(cases[0].test, "changed");
    ASSERT_EQ(cases[0].reasons, CBM_TEST_SELECT_CHANGED);
    ASSERT_STR_EQ(cases[1].test, "cov");
    ASSERT_EQ(cases[1].reasons, CBM_TEST_SELECT_COVERAGE);
    ASSERT_STR_EQ(cases[2].test, "stat");
    ASSERT_EQ(cases[2].reasons, CBM_TEST_SELECT_STATIC);
    ASSERT_NULL(selection_case(result, "alpha", "quiet"));
    const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
    ASSERT_EQ(count, 1);
    ASSERT_FALSE(suites[0].whole);
    ASSERT_EQ(suites[0].reasons,
              CBM_TEST_SELECT_CHANGED | CBM_TEST_SELECT_COVERAGE | CBM_TEST_SELECT_STATIC);
    cbm_test_selection_free(result);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_selection_retains_missing_and_incomplete_test_evidence) {
    cbm_test_model_t *model = selection_model(selection_source);
    cbm_coverage_map_t *map =
        selection_map("alpha:*\tcomplete\t\t1\nalpha:stat\tcomplete\t\t1\n"
                      "alpha:changed\tcomplete\t\t0\nalpha:quiet\tincomplete\tno profile\t\n");
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = SELECTION_REACH;
    reach[0].mapped = false;
    reach[0].reached = false;
    reach[2].changed = false;
    int changed[] = {0};
    cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
    cbm_test_selection_t *result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(cbm_test_selection_run_all(result), 0);
    ASSERT_EQ(selection_case_reasons(result, "alpha", "stat"), CBM_TEST_SELECT_UNMAPPED);
    ASSERT_EQ(selection_case_reasons(result, "alpha", "cov"), CBM_TEST_SELECT_COVERAGE_UNKNOWN);
    ASSERT_EQ(selection_case_reasons(result, "alpha", "quiet"), CBM_TEST_SELECT_COVERAGE_UNKNOWN);
    ASSERT_EQ(selection_case_reasons(result, "alpha", "changed"), CBM_TEST_SELECT_COVERAGE);
    int count;
    const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
    ASSERT_EQ(count, 1);
    ASSERT_FALSE(suites[0].whole);
    cbm_test_selection_free(result);
    /* A completely absent static row has the same conservative meaning. */
    input.reach = reach + 1;
    input.reach_count = 3;
    result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(selection_case_reasons(result, "alpha", "stat"), CBM_TEST_SELECT_UNMAPPED);
    cbm_test_selection_free(result);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_selection_setup_and_rejected_artifacts_select_whole_suites) {
    cbm_test_model_t *model = selection_model(selection_source);
    ASSERT_NOT_NULL(model);
    const char *maps[] = {
        "alpha:*\tcomplete\t\t0\nalpha:stat\tcomplete\t\t1\n",
        "alpha:*\tincomplete\tlost setup\t0\nalpha:stat\tcomplete\t\t1\n",
        "other:*\tcomplete\t\t0 1\n",
        selection_rows,
    };
    unsigned reasons[] = {CBM_TEST_SELECT_SETUP_HIT, CBM_TEST_SELECT_SETUP_UNKNOWN,
                          CBM_TEST_SELECT_SETUP_UNKNOWN, CBM_TEST_SELECT_ARTIFACT_REJECTED};
    cbm_test_reach_t reach[] = SELECTION_REACH;
    int changed[] = {0};
    for (int i = 0; i < 4; i++) {
        cbm_coverage_map_t *map = selection_map(maps[i]);
        ASSERT_NOT_NULL(map);
        cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
        input.coverage_admitted = i != 3;
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        ASSERT_EQ(cbm_test_selection_run_all(result), 0);
        int count;
        const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
        ASSERT_EQ(count, 1);
        ASSERT_TRUE(suites[0].whole);
        ASSERT_TRUE(suites[0].reasons & reasons[i]);
        cbm_test_selection_cases(result, &count);
        ASSERT_EQ(count, 0);
        cbm_test_selection_free(result);
        cbm_coverage_map_free(map);
    }
    cbm_test_model_free(model);
    PASS();
}

TEST(test_selection_incomplete_global_evidence_never_narrows) {
    cbm_test_model_t *model = selection_model(selection_source);
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = SELECTION_REACH;
    int changed[] = {0};
    unsigned expected[] = {CBM_TEST_SELECT_DIFF_INCOMPLETE, CBM_TEST_SELECT_INVENTORY_UNKNOWN,
                           CBM_TEST_SELECT_STATIC_INCOMPLETE,
                           CBM_TEST_SELECT_CHANGE_IDENTITY_UNKNOWN};
    for (int i = 0; i < 4; i++) {
        cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
        if (i == 0)
            input.diff_complete = false;
        if (i == 1)
            input.inventory_complete = false;
        if (i == 2)
            input.static_complete = false;
        if (i == 3)
            input.coverage_changes_complete = false;
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        ASSERT_EQ(cbm_test_selection_run_all(result), expected[i]);
        int count;
        cbm_test_selection_cases(result, &count);
        ASSERT_EQ(count, 0);
        cbm_test_selection_suites(result, &count);
        ASSERT_EQ(count, 0);
        cbm_test_selection_free(result);
    }
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_selection_inventory_ambiguity_and_conditionals_use_whole_suite) {
    const char *sources[] = {
        "void main(void) { RUN_SELECTED_SUITE(alpha); }\n",
        "SUITE(alpha) { RUN_TEST(unknown); }\n"
        "void main(void) { RUN_SELECTED_SUITE(alpha); }\n",
        "#define REG(name) RUN_TEST(name)\nTEST(stat) {}\nTEST(quiet) {}\n"
        "SUITE(alpha) { REG(stat); RUN_TEST(quiet); }\nvoid main(void) { "
        "RUN_SELECTED_SUITE(alpha); }\n",
        "#ifdef FEATURE\nTEST(stat) {}\n#endif\nSUITE(alpha) { RUN_TEST(stat); }\n"
        "void main(void) { RUN_SELECTED_SUITE(alpha); }\n",
        "TEST(stat) {}\nSUITE(alpha) {\n#ifdef FEATURE\nRUN_TEST(stat);\n#endif\n}\n"
        "void main(void) { RUN_SELECTED_SUITE(alpha); }\n",
    };
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = SELECTION_REACH;
    int changed[] = {0};
    for (int i = 0; i < 5; i++) {
        cbm_test_model_t *model = selection_model(sources[i]);
        ASSERT_NOT_NULL(model);
        cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
        /* No changed evidence for cases absent in these deliberately tiny models. */
        reach[2].changed = false;
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        ASSERT_EQ(cbm_test_selection_run_all(result), 0);
        int count;
        const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
        ASSERT_EQ(count, 1);
        ASSERT_TRUE(suites[0].whole);
        ASSERT_TRUE(suites[0].reasons &
                    (i < 3 ? CBM_TEST_SELECT_INVENTORY_UNKNOWN : CBM_TEST_SELECT_CONDITIONAL));
        cbm_test_selection_cases(result, &count);
        ASSERT_EQ(count, 0);
        cbm_test_selection_free(result);
        cbm_test_model_free(model);
    }
    cbm_coverage_map_free(map);
    PASS();
}

/* Uncertainty is scoped to the file it was found in. A file the model cannot
 * read with certainty, but which defines its own suite, makes that suite
 * uncertain (it can only run whole) and leaves every other suite narrowable.
 * The same uncertainty in the runner's file (the list of runnable suites) or
 * in a file with no suite of its own stays global. cbm has such a file:
 * tests/repro/repro_call_argument_matrix_a.c defines its tests by macro. */
static const char *scoped_main =
    "void main(void) { RUN_SELECTED_SUITE(alpha); RUN_SELECTED_SUITE(beta); }\n";
static const char *scoped_cases = "TEST(stat) {}\nSUITE(alpha) { RUN_TEST(stat); }\n";
static const char *scoped_macro =
    "#define MAKE(n) \\\n    TEST(n) {}\nTEST(other) {}\nSUITE(beta) { RUN_TEST(other); }\n";

TEST(test_model_uncertainty_is_scoped_to_the_suites_of_its_file) {
    tm_source_t scoped[] = {{"tests/cases.c", scoped_cases},
                            {"tests/macro.c", scoped_macro},
                            {"tests/main.c", scoped_main}};
    cbm_test_model_t *model = model_of(scoped, 3);
    ASSERT_NOT_NULL(model);
    ASSERT_FALSE(cbm_test_model_complete(model));
    ASSERT_TRUE(cbm_test_model_narrowable(model));
    int n = 0;
    const cbm_test_suite_t *suites = cbm_test_model_suites(model, &n);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(suites[0].name, "alpha");
    ASSERT_FALSE(suites[0].uncertain);
    ASSERT_STR_EQ(suites[1].name, "beta");
    ASSERT_TRUE(suites[1].uncertain);
    cbm_test_model_free(model);

    /* The runner's file is uncertain: some runnable suite may be missing. */
    tm_source_t runner[] = {{"tests/cases.c", scoped_cases},
                            {"tests/main.c", "int all = 1; \\\n int more = 2;\n"
                                             "void main(void) { RUN_SELECTED_SUITE(alpha); }\n"}};
    model = model_of(runner, 2);
    ASSERT_NOT_NULL(model);
    ASSERT_FALSE(cbm_test_model_narrowable(model));
    cbm_test_model_free(model);

    /* An uncertain file with no suite of its own: nothing to scope it to. */
    tm_source_t loose[] = {{"tests/cases.c", scoped_cases},
                           {"tests/helper.c", "int helper = 1; \\\n int more = 2;\n"},
                           {"tests/main.c", "void main(void) { RUN_SELECTED_SUITE(alpha); }\n"}};
    model = model_of(loose, 3);
    ASSERT_NOT_NULL(model);
    ASSERT_FALSE(cbm_test_model_narrowable(model));
    cbm_test_model_free(model);

    /* No uncertainty at all: complete and narrowable. */
    tm_source_t clean[] = {{"tests/cases.c", scoped_cases},
                           {"tests/main.c", "void main(void) { RUN_SELECTED_SUITE(alpha); }\n"}};
    model = model_of(clean, 2);
    ASSERT_NOT_NULL(model);
    ASSERT_TRUE(cbm_test_model_complete(model));
    ASSERT_TRUE(cbm_test_model_narrowable(model));
    cbm_test_model_free(model);
    PASS();
}

/* A line continuation the lexer reads correctly is no uncertainty: inside a
 * preprocessor directive (LF and CRLF) or a string literal. Only one in plain
 * code, which can splice tokens, makes the file uncertain. Before this rule,
 * any continuation anywhere did, and every cbm test file with a multi-line
 * #define (cli, mcp, daemon_runtime, extraction, test_main.c) could never be
 * narrowed. */
TEST(test_model_only_code_continuations_are_uncertain) {
    static const struct {
        const char *text;
        bool complete;
    } cases[] = {
        {"#define MULTI(x) \\\n    do { x; } while (0)\n"
         "TEST(a) {}\nSUITE(s) { RUN_TEST(a); }\n"
         "void main(void) { RUN_SELECTED_SUITE(s); }\n",
         true},
        {"#define MULTI(x) \\\r\n    (x)\r\n"
         "TEST(a) {}\r\nSUITE(s) { RUN_TEST(a); }\r\n"
         "void main(void) { RUN_SELECTED_SUITE(s); }\r\n",
         true},
        {"static const char *text = \"a\\\nb\";\n"
         "TEST(a) {}\nSUITE(s) { RUN_TEST(a); }\n"
         "void main(void) { RUN_SELECTED_SUITE(s); }\n",
         true},
        {"TE\\\nST(a) {}\nSUITE(s) { RUN_TEST(a); }\n"
         "void main(void) { RUN_SELECTED_SUITE(s); }\n",
         false},
        /* a splice that joins a directive word */
        {"#def\\\nine X 1\nTEST(a) {}\nSUITE(s) { RUN_TEST(a); }\n"
         "void main(void) { RUN_SELECTED_SUITE(s); }\n",
         false},
        /* `*` + splice + `/` closes a block comment, which hides nothing then */
        {"/* note *\\\n/\nTEST(a) {}\n/* end */\nSUITE(s) { RUN_TEST(a); }\n"
         "void main(void) { RUN_SELECTED_SUITE(s); }\n",
         false},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        tm_source_t one[] = {{"tests/one.c", cases[i].text}};
        cbm_test_model_t *model = model_of(one, 1);
        ASSERT_NOT_NULL(model);
        if (cbm_test_model_complete(model) != cases[i].complete) {
            printf("  case %zu: complete %d\n", i, (int)cbm_test_model_complete(model));
        }
        ASSERT_EQ(cbm_test_model_complete(model), cases[i].complete);
        if (cases[i].complete) {
            int n = 0;
            (void)cbm_test_model_registrations(model, &n);
            ASSERT_EQ(n, 1);
        }
        cbm_test_model_free(model);
    }
    PASS();
}

/* The selection runs the uncertain suite whole and still narrows the others. */
TEST(test_selection_scoped_uncertainty_runs_only_its_suite_whole) {
    tm_source_t scoped[] = {{"tests/cases.c", scoped_cases},
                            {"tests/macro.c", scoped_macro},
                            {"tests/main.c", scoped_main}};
    cbm_test_model_t *model = model_of(scoped, 3);
    static const char *rows = "alpha:*\tcomplete\t\t1\nalpha:stat\tcomplete\t\t1\n"
                              "beta:*\tcomplete\t\t1\nbeta:other\tcomplete\t\t0\n";
    cbm_coverage_map_t *map = selection_map(rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = {{"tests/cases.c", "stat", true, true, false}};
    int changed[] = {0};
    cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
    input.reach_count = 1;
    cbm_test_selection_t *result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(cbm_test_selection_run_all(result), 0);
    int count = 0;
    const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(suites[0].name, "alpha");
    ASSERT_FALSE(suites[0].whole);
    ASSERT_STR_EQ(suites[1].name, "beta");
    ASSERT_TRUE(suites[1].whole);
    ASSERT_TRUE(suites[1].reasons & CBM_TEST_SELECT_INVENTORY_UNKNOWN);
    ASSERT_EQ(selection_case_reasons(result, "alpha", "stat"), CBM_TEST_SELECT_STATIC);
    cbm_test_selection_free(result);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    PASS();
}

/* A changed test no suite registers runs everything; a runner suite the
 * model finds no definition for runs whole (its tests are unknown). */
/* The answer follows the selection: a model whose uncertainty is scoped to
 * some suites (narrowable, not complete) carries those suites whole and
 * narrows the rest. Before, the codec demanded a complete model and ran
 * everything whenever any suite was uncertain (on cbm: always). */
TEST(test_result_scoped_uncertainty_is_not_a_global_run_all) {
    tm_source_t scoped[] = {{"tests/cases.c", scoped_cases},
                            {"tests/macro.c", scoped_macro},
                            {"tests/main.c", scoped_main}};
    cbm_test_model_t *model = model_of(scoped, 3);
    static const char *rows = "alpha:*\tcomplete\t\t1\nalpha:stat\tcomplete\t\t1\n"
                              "beta:*\tcomplete\t\t1\nbeta:other\tcomplete\t\t0\n";
    cbm_coverage_map_t *map = selection_map(rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    ASSERT_TRUE(cbm_test_model_narrowable(model));
    ASSERT_FALSE(cbm_test_model_complete(model));
    cbm_test_reach_t reach[] = {{"tests/cases.c", "stat", true, true, false}};
    int changed[] = {0};
    cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
    input.reach_count = 1;
    cbm_test_selection_t *selection = cbm_test_select(&input);
    ASSERT_NOT_NULL(selection);
    char dir[512], path[600];
    snprintf(dir, sizeof(dir), "%s/cbm-impact-result-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(dir));
    snprintf(path, sizeof(path), "%s/absent.json", dir);
    cbm_test_config_t *config = cbm_test_config_load(path, true);
    cbm_test_policy_t *policy = config ? cbm_test_policy_new(config) : NULL;
    ASSERT_NOT_NULL(policy);
    cbm_test_result_receipt_t receipt = {0};
    receipt.object_format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.base.format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.head.format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.merge_base.format = CBM_TEST_RESULT_OBJECT_SHA1;
    receipt.diff_sha256.present = true;
    receipt.name_status_sha256.present = true;
    const char *hex = cbm_test_policy_digest(policy);
    ASSERT_NOT_NULL(hex);
    for (size_t i = 0; i < 32; i++) {
        unsigned v = 0;
        ASSERT_EQ(sscanf(hex + i * 2, "%2x", &v), 1);
        receipt.policy_sha256.bytes[i] = (unsigned char)v;
    }
    receipt.policy_sha256.present = true;
    cbm_test_result_input_t in = {.comparison = CBM_TEST_RESULT_COMPARISON_CHANGED,
                                  .selection = selection,
                                  .model = model,
                                  .policy = policy,
                                  .inventory_complete = true,
                                  .activation_complete = true,
                                  .receipt = &receipt};
    cbm_test_result_limits_t limits = {.max_input_bytes = 1048576,
                                       .max_items = 100000,
                                       .max_alloc_bytes = 16777216,
                                       .max_output_bytes = 1048576};
    cbm_test_result_t *result = NULL;
    ASSERT_EQ(cbm_test_result_build(&in, &limits, NULL, NULL, &result), CBM_TEST_RESULT_OK);
    size_t len = 0;
    const char *json = cbm_test_result_json(result, &len);
    ASSERT_NOT_NULL(json);
    if (!strstr(json, "\"decision\":\"selected\"")) {
        printf("  %.300s\n", json);
    }
    ASSERT_NOT_NULL(strstr(json, "\"decision\":\"selected\""));
    ASSERT_NOT_NULL(strstr(json, "\"run_all_reasons\":[]"));
    cbm_test_result_free(result);
    cbm_test_policy_free(policy);
    cbm_test_config_free(config);
    cbm_test_selection_free(selection);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    th_rmtree(dir);
    PASS();
}

TEST(test_selection_unregistered_changes_run_all_and_undefined_runner_suites_run_whole) {
    const char *sources[] = {
        "TEST(changed) {}\nTEST(stat) {}\nSUITE(alpha) { RUN_TEST(stat); }\n"
        "void main(void) { RUN_SELECTED_SUITE(alpha); }\n",
        "TEST(stat) {}\nSUITE(hidden) { RUN_TEST(stat); }\n"
        "void main(void) { RUN_SELECTED_SUITE(alpha); }\n",
    };
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = SELECTION_REACH;
    int changed[] = {0};
    for (int i = 0; i < 2; i++) {
        cbm_test_model_t *model = selection_model(sources[i]);
        ASSERT_NOT_NULL(model);
        cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
        if (i == 1)
            reach[2].changed = false;
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        if (i == 0) {
            ASSERT_TRUE(cbm_test_selection_run_all(result) & CBM_TEST_SELECT_CHANGED_UNREGISTERED);
        } else {
            ASSERT_EQ(cbm_test_selection_run_all(result), 0);
            int count = 0;
            const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
            ASSERT_EQ(count, 1);
            ASSERT_STR_EQ(suites[0].name, "alpha");
            ASSERT_TRUE(suites[0].whole);
            ASSERT_TRUE(suites[0].reasons & CBM_TEST_SELECT_INVENTORY_UNKNOWN);
        }
        cbm_test_selection_free(result);
        cbm_test_model_free(model);
    }
    cbm_coverage_map_free(map);
    PASS();
}

/* A suite this runner never registers (another runner's, like the repro
 * board's RUN_SUITE list, or one not built into it) is outside its universe:
 * its tests are neither selected nor a reason to run everything, even when
 * one of them changed. On cbm, 49 defined suites are such suites; treating
 * them as unknown made every selection run everything. */
TEST(test_selection_other_runners_suites_are_outside_the_selection) {
    const char *source = "TEST(stat) {}\nTEST(elsewhere) {}\n"
                         "SUITE(alpha) { RUN_TEST(stat); }\n"
                         "SUITE(board) { RUN_TEST(elsewhere); }\n"
                         "void main(void) { RUN_SELECTED_SUITE(alpha); }\n";
    cbm_test_model_t *model = selection_model(source);
    static const char *rows = "alpha:*\tcomplete\t\t1\nalpha:stat\tcomplete\t\t0\n";
    cbm_coverage_map_t *map = selection_map(rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = {{"tests/cases.c", "elsewhere", true, true, true},
                                {"tests/cases.c", "stat", true, true, false}};
    int changed[] = {0};
    cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
    input.reach_count = 2;
    cbm_test_selection_t *result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(cbm_test_selection_run_all(result), 0);
    int count = 0;
    const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(suites[0].name, "alpha");
    ASSERT_FALSE(suites[0].whole);
    ASSERT_EQ(selection_case_reasons(result, "alpha", "stat"),
              CBM_TEST_SELECT_STATIC | CBM_TEST_SELECT_COVERAGE);
    cbm_test_selection_free(result);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_selection_invalid_evidence_cannot_produce_a_partial_result) {
    cbm_test_model_t *model = selection_model(selection_source);
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = SELECTION_REACH;
    cbm_test_reach_t duplicate[] = {reach[0], reach[0]};
    int changed[] = {0};
    for (int i = 0; i < 6; i++) {
        cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
        if (i == 0)
            input.changed_function_count = -1;
        if (i == 1) {
            changed[0] = 2;
        }
        if (i == 2) {
            input.reach = NULL;
        }
        if (i == 3) {
            input.reach = duplicate;
            input.reach_count = 2;
        }
        if (i == 4) {
            reach[0].test = NULL;
        }
        if (i == 5) {
            input.changed_function_ids = NULL;
        }
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        ASSERT_TRUE(cbm_test_selection_run_all(result) & CBM_TEST_SELECT_INVALID_INPUT);
        int count;
        cbm_test_selection_cases(result, &count);
        ASSERT_EQ(count, 0);
        cbm_test_selection_free(result);
        changed[0] = 0;
        reach[0].test = "stat";
    }
    ASSERT_TRUE(cbm_test_selection_run_all(NULL) != 0);
    cbm_test_selection_free(NULL);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_selection_is_deterministic_deduplicated_and_owns_strings) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    tm_source_t sources[] = {
        {"tests/cases.c", "TEST(stat) {}\nSUITE(zeta) { RUN_TEST(stat); RUN_TEST(stat); }\n"
                          "SUITE(alpha) { RUN_TEST(stat); }\n"},
        {"tests/main.c",
         "void main(void) { RUN_SELECTED_SUITE(zeta); RUN_SELECTED_SUITE(alpha); }\n"},
    };
    cbm_coverage_map_t *map = selection_map("zeta:*\tcomplete\t\t1\nzeta:stat\tcomplete\t\t0\n"
                                            "alpha:*\tcomplete\t\t1\nalpha:stat\tcomplete\t\t0\n");
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = {{"tests/cases.c", "stat", true, true, false}};
    int changed[] = {0};
    for (int pass = 0; pass < 2; pass++) {
        cbm_test_model_t *model = model_of(sources, 2);
        ASSERT_NOT_NULL(model);
        cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
        input.reach_count = 1;
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        cbm_test_model_free(model);
        ASSERT_EQ(cbm_test_selection_run_all(result), 0);
        int count;
        const cbm_test_selected_case_t *cases = cbm_test_selection_cases(result, &count);
        ASSERT_EQ(count, 2);
        ASSERT_STR_EQ(cases[0].suite, "alpha");
        ASSERT_STR_EQ(cases[1].suite, "zeta");
        ASSERT_STR_EQ(cases[1].file, "tests/cases.c");
        ASSERT_EQ(cases[0].reasons, CBM_TEST_SELECT_STATIC | CBM_TEST_SELECT_COVERAGE);
        ASSERT_EQ(cases[1].reasons, cases[0].reasons);
        cbm_test_selection_free(result);
        tm_source_t swap = sources[0];
        sources[0] = sources[1];
        sources[1] = swap;
    }
    cbm_coverage_map_free(map);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_selection_proven_empty_diff_is_distinct_from_unresolved_changes) {
    cbm_test_selection_input_t input = {.diff_complete = true, .has_changes = false};
    cbm_test_selection_t *result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(cbm_test_selection_run_all(result), 0);
    int count;
    cbm_test_selection_suites(result, &count);
    ASSERT_EQ(count, 0);
    cbm_test_selection_free(result);
    input.diff_complete = false;
    result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_TRUE(cbm_test_selection_run_all(result) & CBM_TEST_SELECT_DIFF_INCOMPLETE);
    cbm_test_selection_free(result);
    PASS();
}

TEST(test_selection_suite_body_and_rule_triggers_dominate_cases) {
    cbm_test_model_t *model = selection_model(selection_source);
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = SELECTION_REACH;
    reach[0].reached = false;
    reach[2].changed = false;
    int changed[] = {0};
    cbm_test_suite_trigger_t trigger = {.suite = "alpha"};
    cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
    input.changed_function_count = 0;
    input.suite_triggers = &trigger;
    input.suite_trigger_count = 1;
    unsigned reasons[] = {CBM_TEST_SELECT_STATIC, CBM_TEST_SELECT_CHANGED, CBM_TEST_SELECT_RULE};
    for (int i = 0; i < 4; i++) {
        reach[0].reached = i < 3;
        trigger.reached = i == 0;
        trigger.changed = i == 1;
        trigger.rule = i == 2;
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        ASSERT_EQ(cbm_test_selection_run_all(result), 0);
        int count;
        const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
        ASSERT_EQ(count, i == 3 ? 0 : 1);
        if (i < 3) {
            ASSERT_TRUE(suites[0].whole);
            ASSERT_EQ(suites[0].reasons, reasons[i] | CBM_TEST_SELECT_STATIC);
        }
        cbm_test_selection_cases(result, &count);
        ASSERT_EQ(count, 0);
        cbm_test_selection_free(result);
    }
    trigger.suite = "not_in_runner";
    trigger.changed = true;
    cbm_test_selection_t *result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_TRUE(cbm_test_selection_run_all(result) & CBM_TEST_SELECT_INVENTORY_UNKNOWN);
    cbm_test_selection_free(result);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_selection_ambiguous_source_identity_cannot_choose_first_match) {
    tm_source_t sources[] = {
        {"tests/cases.c", "TEST(stat) {}\nSUITE(alpha) { RUN_TEST(stat); }\n"
                          "void main(void) { RUN_SELECTED_SUITE(alpha); }\n"},
        {"tests/other.c", "TEST(stat) {}\nSUITE(alpha) { RUN_TEST(stat); }\n"},
    };
    cbm_test_model_t *model = model_of(sources, 2);
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(model);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = {{"tests/cases.c", "stat", true, false, false},
                                {"tests/other.c", "stat", true, false, false}};
    int changed[] = {0};
    cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
    input.reach_count = 2;
    cbm_test_selection_t *result = cbm_test_select(&input);
    ASSERT_NOT_NULL(result);
    ASSERT_EQ(cbm_test_selection_run_all(result), 0);
    int count;
    const cbm_test_selected_suite_t *suites = cbm_test_selection_suites(result, &count);
    ASSERT_EQ(count, 1);
    ASSERT_TRUE(suites[0].whole);
    ASSERT_TRUE(suites[0].reasons & CBM_TEST_SELECT_INVENTORY_UNKNOWN);
    cbm_test_selection_free(result);
    cbm_coverage_map_free(map);
    cbm_test_model_free(model);
    PASS();
}

#undef SELECTION_REACH

/* Independent byte hashes below are generated by Python hashlib from these
 * literal artifacts; policy timestamps are pinned, never the wall clock. */
static const char *receipt_functions = "0\tsrc/source.c\tone\n1\tsrc/source.c\ttwo\n";
static const char *receipt_tests =
    "alpha:*\tcomplete\t\t1\nalpha:one\tcomplete\t\t0\nalpha:two\tcomplete\t\t1\n";
static const char *receipt_metadata =
    "{\"format\":1,\"commit\":\"1111111111111111111111111111111111111111\",\"functions\":2,"
    "\"tests\":2,\"incomplete\":0,\"platform\":\"fixture\",\"llvm_profdata\":\"fixture\","
    "\"suites\":[{\"suite\":\"alpha\",\"tests\":2,\"incomplete\":0,\"exit\":0}]}\n";
static const char *receipt_bad_metadata =
    "{\"format\":1,\"commit\":\"1111111111111111111111111111111111111111\",\"functions\":2,"
    "\"tests\":3,\"incomplete\":0,\"platform\":\"fixture\",\"llvm_profdata\":\"fixture\","
    "\"suites\":[{\"suite\":\"alpha\",\"tests\":2,\"incomplete\":0,\"exit\":0}]}\n";

static cbm_coverage_receipt_t receipt_fixture(void) {
    return (cbm_coverage_receipt_t){
        .commit = "1111111111111111111111111111111111111111",
        .functions_sha256 = "9fb6ff76736fdda3558510f3732c3af3be254476dc29e479bc1e2aa67db14c91",
        .tests_sha256 = "9301b8969a11a4b758f4d0d7128ba71bc00a90da1b6f89acbf175988abef362c",
        .metadata_sha256 = "ebdeebf656b0eace38a267f808365b6fea7eefb89f9feabd41c70d4d2cf156aa",
        .graph_sha256 = "4444444444444444444444444444444444444444444444444444444444444444",
        .compatibility_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
        .oldest_observation_at = INT64_C(2000000000),
    };
}

static cbm_coverage_receipt_context_t receipt_context(void) {
    return (cbm_coverage_receipt_context_t){
        .source_verified = true,
        .trusted_commit = "1111111111111111111111111111111111111111",
        .ancestor_verified = true,
        .ancestor_commit = "1111111111111111111111111111111111111111",
        .ancestor_merge_base = "2222222222222222222222222222222222222222",
        .merge_base = "2222222222222222222222222222222222222222",
        .graph_commit = "1111111111111111111111111111111111111111",
        .graph_sha256 = "4444444444444444444444444444444444444444444444444444444444444444",
        .compatibility_sha256 = "3333333333333333333333333333333333333333333333333333333333333333",
        .now = INT64_C(2000000000) + 604800,
    };
}

static cbm_coverage_map_t *receipt_map(void) {
    return cbm_coverage_map_parse(receipt_functions, strlen(receipt_functions), receipt_tests,
                                  strlen(receipt_tests));
}

TEST(test_coverage_receipt_accepts_bound_evidence_at_age_boundary) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_coverage_map_t *map = receipt_map();
    ASSERT_NOT_NULL(map);
    cbm_coverage_receipt_t receipt = receipt_fixture();
    cbm_coverage_receipt_context_t context = receipt_context();
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &receipt, &context),
              0);
    context.now = receipt.oldest_observation_at;
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &receipt, &context),
              0);
    cbm_coverage_map_free(map);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_coverage_receipt_requires_bound_source_ancestry_and_graph) {
    cbm_coverage_map_t *map = receipt_map();
    ASSERT_NOT_NULL(map);
    unsigned flags[] = {CBM_COVERAGE_RECEIPT_SOURCE,   CBM_COVERAGE_RECEIPT_ANCESTRY,
                        CBM_COVERAGE_RECEIPT_ANCESTRY, CBM_COVERAGE_RECEIPT_ANCESTRY,
                        CBM_COVERAGE_RECEIPT_COMMIT,   CBM_COVERAGE_RECEIPT_COMMIT,
                        CBM_COVERAGE_RECEIPT_CONTENT,  CBM_COVERAGE_RECEIPT_COMPATIBILITY};
    for (int i = 0; i < 8; i++) {
        cbm_coverage_receipt_t receipt = receipt_fixture();
        cbm_coverage_receipt_context_t context = receipt_context();
        if (i == 0)
            context.source_verified = false;
        if (i == 1)
            context.ancestor_verified = false;
        if (i == 2)
            context.ancestor_commit = context.merge_base;
        if (i == 3)
            context.ancestor_merge_base = context.trusted_commit;
        if (i == 4)
            context.graph_commit = context.merge_base;
        if (i == 5)
            context.trusted_commit = context.merge_base;
        if (i == 6)
            context.graph_sha256 = context.compatibility_sha256;
        if (i == 7)
            context.compatibility_sha256 = context.graph_sha256;
        unsigned reasons = cbm_coverage_map_check_receipt(
            map, receipt_metadata, strlen(receipt_metadata), &receipt, &context);
        ASSERT_TRUE(reasons & flags[i]);
        ASSERT_FALSE(reasons & CBM_COVERAGE_RECEIPT_INVALID);
    }
    cbm_coverage_map_free(map);
    PASS();
}

TEST(test_coverage_receipt_binds_exact_bytes_and_metadata_consistency) {
    cbm_coverage_receipt_t receipt = receipt_fixture();
    cbm_coverage_receipt_context_t context = receipt_context();
    cbm_coverage_map_t *map = receipt_map();
    ASSERT_NOT_NULL(map);
    cbm_coverage_receipt_t changed = receipt;
    changed.functions_sha256 = receipt.graph_sha256;
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &changed, &context),
              CBM_COVERAGE_RECEIPT_CONTENT);
    changed = receipt;
    changed.tests_sha256 = receipt.graph_sha256;
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &changed, &context),
              CBM_COVERAGE_RECEIPT_CONTENT);
    changed = receipt;
    changed.metadata_sha256 = receipt.graph_sha256;
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &changed, &context),
              CBM_COVERAGE_RECEIPT_CONTENT);
    /* Matching byte provenance cannot make inconsistent semantic counts valid. */
    changed = receipt;
    changed.metadata_sha256 = "70944a2f6b8ab0318e9fe086326a28d333c0fe5e0836734f8bc246312f71b212";
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_bad_metadata,
                                             strlen(receipt_bad_metadata), &changed, &context),
              CBM_COVERAGE_RECEIPT_METADATA);
    cbm_coverage_map_free(map);
    const char *modified_functions = "0\tsrc/renamed.c\tone\n1\tsrc/source.c\ttwo\n";
    map = cbm_coverage_map_parse(modified_functions, strlen(modified_functions), receipt_tests,
                                 strlen(receipt_tests));
    ASSERT_NOT_NULL(map);
    ASSERT_TRUE(cbm_coverage_map_metadata_matches(map, receipt_metadata, strlen(receipt_metadata),
                                                  receipt.commit));
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &receipt, &context),
              CBM_COVERAGE_RECEIPT_CONTENT);
    cbm_coverage_map_free(map);
    const char *modified_tests = "alpha:*\tcomplete\t\t1\n"
                                 "alpha:one\tcomplete\t\t0\n"
                                 "alpha:two\tcomplete\t\t0\n";
    map = cbm_coverage_map_parse(receipt_functions, strlen(receipt_functions), modified_tests,
                                 strlen(modified_tests));
    ASSERT_NOT_NULL(map);
    ASSERT_TRUE(cbm_coverage_map_metadata_matches(map, receipt_metadata, strlen(receipt_metadata),
                                                  receipt.commit));
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &receipt, &context),
              CBM_COVERAGE_RECEIPT_CONTENT);
    cbm_coverage_map_free(map);
    PASS();
}

TEST(test_coverage_receipt_rejects_stale_future_and_missing_age) {
    cbm_coverage_map_t *map = receipt_map();
    ASSERT_NOT_NULL(map);
    for (int i = 0; i < 6; i++) {
        cbm_coverage_receipt_t receipt = receipt_fixture();
        cbm_coverage_receipt_context_t context = receipt_context();
        if (i == 0)
            context.now++;
        if (i == 1)
            receipt.oldest_observation_at = context.now + 1;
        if (i == 2)
            receipt.oldest_observation_at = 0;
        if (i == 3)
            receipt.oldest_observation_at = INT64_MIN;
        if (i == 4)
            context.now = INT64_MAX;
        if (i == 5)
            context.now = -1;
        ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                                 &receipt, &context),
                  CBM_COVERAGE_RECEIPT_AGE);
    }
    cbm_coverage_map_free(map);
    PASS();
}

TEST(test_coverage_receipt_missing_or_malformed_evidence_fails_closed) {
    cbm_coverage_map_t *map = receipt_map();
    ASSERT_NOT_NULL(map);
    for (int i = 0; i < 9; i++) {
        cbm_coverage_receipt_t receipt = receipt_fixture();
        cbm_coverage_receipt_context_t context = receipt_context();
        if (i == 0)
            receipt.commit = "HEAD";
        if (i == 1)
            receipt.functions_sha256 = "";
        if (i == 2)
            receipt.tests_sha256 = NULL;
        if (i == 3)
            receipt.metadata_sha256 =
                "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
        if (i == 4)
            context.merge_base = NULL;
        if (i == 5)
            context.ancestor_merge_base = "main";
        if (i == 6)
            context.graph_commit = "";
        if (i == 7)
            context.graph_sha256 = "abcd";
        if (i == 8)
            context.compatibility_sha256 = NULL;
        ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                                 &receipt, &context),
                  CBM_COVERAGE_RECEIPT_INVALID);
    }
    cbm_coverage_receipt_t receipt = receipt_fixture();
    cbm_coverage_receipt_context_t context = receipt_context();
    ASSERT_EQ(cbm_coverage_map_check_receipt(NULL, receipt_metadata, strlen(receipt_metadata),
                                             &receipt, &context),
              CBM_COVERAGE_RECEIPT_INVALID);
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata), NULL,
                                             &context),
              CBM_COVERAGE_RECEIPT_INVALID);
    ASSERT_EQ(cbm_coverage_map_check_receipt(map, receipt_metadata, strlen(receipt_metadata),
                                             &receipt, NULL),
              CBM_COVERAGE_RECEIPT_INVALID);
    cbm_coverage_map_free(map);
    PASS();
}

/* Policies consume the exact query-local snapshot and classify paths before
 * target applicability is known. */
static cbm_test_policy_t *policy_of(const char *body) {
    char dir[512], path[600];
    snprintf(dir, sizeof(dir), "%s/cbm-impact-policy-XXXXXX", cbm_tmpdir());
    if (!cbm_mkdtemp(dir))
        return NULL;
    snprintf(path, sizeof(path), "%s/config.json", dir);
    cbm_test_policy_t *policy = NULL;
    if (th_write_file(path, body) == 0) {
        cbm_test_config_t *config = cbm_test_config_load(path, false);
        if (config)
            policy = cbm_test_policy_new(config);
        cbm_test_config_free(config);
    }
    th_rmtree(dir);
    return policy;
}

static bool policy_matches(const cbm_test_policy_t *policy, const char *path, const char *id) {
    const cbm_test_rule_t *rule = NULL;
    if (!cbm_test_policy_match_path(policy, path, &rule))
        return false;
    return id ? rule && strcmp(rule->id, id) == 0 : rule == NULL;
}

TEST(test_policy_retains_exact_snapshot_and_owns_result) {
    char dir[512], path[600];
    snprintf(dir, sizeof(dir), "%s/cbm-impact-policy-source-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(dir));
    snprintf(path, sizeof(path), "%s/config.json", dir);
    const char *json = " {\"test_impact\":{\"version\":1,\"rules\":[{\"id\":\"mine\","
                       "\"paths\":[\"special/**\"],\"action\":\"ignore\"}]}}\n";
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), len = 99;
    bool absent = false;
    cbm_test_config_t *config = cbm_test_config_load(path, true);
    ASSERT_NOT_NULL(config);
    const char *source = cbm_test_config_source(config, &len, &absent);
    ASSERT_NOT_NULL(source);
    ASSERT_EQ(len, 0);
    ASSERT_TRUE(absent);
    cbm_test_policy_t *missing = cbm_test_policy_new(config);
    ASSERT_NOT_NULL(missing);
    cbm_test_policy_free(missing);
    cbm_test_config_free(config);
    ASSERT_EQ(th_write_file(path, json), 0);
    config = cbm_test_config_load(path, false);
    ASSERT_NOT_NULL(config);
    source = cbm_test_config_source(config, &len, &absent);
    ASSERT_FALSE(absent);
    ASSERT_EQ(len, strlen(json));
    ASSERT_NOT_NULL(source);
    ASSERT_EQ(memcmp(source, json, len), 0);
    char digest[65];
    snprintf(digest, sizeof(digest), "%s", cbm_test_config_digest(config));
    ASSERT_EQ(th_write_file(path, "{}"), 0);
    cbm_test_policy_t *policy = cbm_test_policy_new(config);
    ASSERT_NOT_NULL(policy);
    cbm_test_config_free(config);
    th_rmtree(dir);
    ASSERT_STR_EQ(cbm_test_policy_digest(policy), digest);
    ASSERT_TRUE(policy_matches(policy, "special/file.c", "mine"));
    cbm_test_policy_free(policy);
    len = 99;
    absent = false;
    ASSERT_NULL(cbm_test_config_source(NULL, &len, &absent));
    ASSERT_EQ(len, 0);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_policy_defaults_and_first_match_before_target_resolution) {
    cbm_test_policy_t *policy = policy_of("{}");
    ASSERT_NOT_NULL(policy);
    const char *full[] = {"Makefile.cbm",      "sub/CMakeLists.txt",       "go.mod",
                          "package-lock.json", ".github/workflows/ci.yml", "third_party/lib/x.c",
                          "vendor_lib/x.c",    "tests/conftest.py"};
    for (size_t i = 0; i < sizeof(full) / sizeof(full[0]); i++) {
        const cbm_test_rule_t *rule = NULL;
        ASSERT_TRUE(cbm_test_policy_match_path(policy, full[i], &rule));
        ASSERT_NOT_NULL(rule);
        ASSERT_TRUE(rule->builtin);
        ASSERT_EQ(rule->action, CBM_TEST_RULE_RUN_ALL);
    }
    const cbm_test_rule_t *rule = NULL;
    ASSERT_TRUE(cbm_test_policy_match_path(policy, "docs/guide.md", &rule));
    ASSERT_NOT_NULL(rule);
    ASSERT_EQ(rule->action, CBM_TEST_RULE_IGNORE);
    ASSERT_TRUE(policy_matches(policy, "src/ordinary.c", NULL));
    int count = 0;
    const cbm_test_lane_t *lanes = cbm_test_policy_lanes(policy, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(lanes[0].name, "unit");
    ASSERT_TRUE(lanes[0].default_run);
    ASSERT_TRUE(lanes[0].narrow);
    bool selected = false;
    ASSERT_TRUE(cbm_test_policy_lane_selects_suite(policy, 0, "alpha", false, &selected));
    ASSERT_TRUE(selected);
    ASSERT_TRUE(cbm_test_policy_lane_selects_suite(policy, 0, "alpha", true, &selected));
    ASSERT_FALSE(selected);
    cbm_test_policy_free(policy);
    policy = policy_of(
        "{\"test_impact\":{\"version\":1,\"rules\":["
        "{\"id\":\"first\",\"paths\":[\"docs/**\"],\"action\":\"suites\",\"suites\":[\"absent\"]},"
        "{\"id\":\"later\",\"paths\":[\"**\"],\"action\":\"run_all\"}]}}");
    ASSERT_NOT_NULL(policy);
    ASSERT_TRUE(policy_matches(policy, "docs/guide.md", "first"));
    ASSERT_TRUE(policy_matches(policy, "src/x.c", "later"));
    rule = cbm_test_policy_rules(policy, &count);
    ASSERT_TRUE(count > 2);
    ASSERT_EQ(rule[0].action, CBM_TEST_RULE_SUITES);
    ASSERT_FALSE(rule[0].builtin);
    ASSERT_EQ(rule[0].key_segment, -1);
    ASSERT_STR_EQ(rule[0].suites[0], "absent");
    cbm_test_policy_free(policy);
    PASS();
}

/* A change to the selection's own configuration runs everything, in every
 * repository, before any project rule could ignore it (review M-4). */
/* test_impact.coverage.compatibility_paths: kept in order; strict on keys
 * (an ignored misspelling would make admitting a coverage map easier) and on
 * paths (repository-relative, no escaping segment). */
TEST(test_policy_coverage_compatibility_paths_are_strict) {
    cbm_test_policy_t *policy =
        policy_of("{\"test_impact\":{\"version\":1,\"coverage\":{\"compatibility_paths\":"
                  "[\"tests/test_framework.h\",\"Makefile.cbm\"]}}}");
    ASSERT_NOT_NULL(policy);
    int count = 0;
    const char *const *paths = cbm_test_policy_compatibility_paths(policy, &count);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(paths[0], "tests/test_framework.h");
    ASSERT_STR_EQ(paths[1], "Makefile.cbm");
    cbm_test_policy_free(policy);

    policy = policy_of("{\"test_impact\":{\"version\":1}}");
    ASSERT_NOT_NULL(policy);
    ASSERT_NULL(cbm_test_policy_compatibility_paths(policy, &count));
    ASSERT_EQ(count, 0);
    cbm_test_policy_free(policy);

    static const char *const refused[] = {
        "{\"compatability_paths\":[\"a\"]}",
        "{\"compatibility_paths\":[\"a\"],\"extra\":1}",
        "{\"compatibility_paths\":\"a\"}",
        "{\"compatibility_paths\":[\"/etc/passwd\"]}",
        "{\"compatibility_paths\":[\"../outside\"]}",
        "{\"compatibility_paths\":[\"a/./b\"]}",
        "{\"compatibility_paths\":[\"a//b\"]}",
        "{\"compatibility_paths\":[\"dir/\"]}",
        "{\"compatibility_paths\":[\"a\\\\b\"]}",
        "{\"compatibility_paths\":[\"\"]}",
        "{\"compatibility_paths\":[7]}",
        "[]",
    };
    for (size_t i = 0; i < sizeof(refused) / sizeof(refused[0]); i++) {
        char body[256];
        snprintf(body, sizeof(body), "{\"test_impact\":{\"version\":1,\"coverage\":%s}}",
                 refused[i]);
        cbm_test_policy_t *bad = policy_of(body);
        if (bad) {
            printf("  accepted: %s\n", refused[i]);
        }
        ASSERT_NULL(bad);
    }
    PASS();
}

TEST(test_policy_config_change_runs_all) {
    cbm_test_policy_t *policy =
        policy_of("{\"test_impact\":{\"version\":1,\"rules\":[{\"id\":\"late\","
                  "\"paths\":[\"src/**\"],\"action\":\"ignore\"}]}}");
    ASSERT_NOT_NULL(policy);
    ASSERT_TRUE(policy_matches(policy, ".codebase-memory.json", "builtin:test-impact-config"));
    ASSERT_TRUE(policy_matches(policy, "sub/.codebase-memory.json", "builtin:test-impact-config"));
    const cbm_test_rule_t *rule = NULL;
    ASSERT_TRUE(cbm_test_policy_match_path(policy, ".codebase-memory.json", &rule));
    ASSERT_EQ(rule->action, CBM_TEST_RULE_RUN_ALL);
    cbm_test_policy_free(policy);
    PASS();
}

TEST(test_policy_lanes_and_semantic_perf_membership) {
    cbm_test_policy_t *policy = policy_of(
        "{\"test_impact\":{\"version\":1,"
        "\"lanes\":[{\"name\":\"fast\",\"suites\":[\"alpha*\",\"@perf\"],"
        "\"exclude_suites\":[\"alpha_slow\"],\"signals\":[\"lang\"],\"default\":\"skip\"},"
        "{\"name\":\"all\",\"suites\":[\"*\"],\"default\":\"run\",\"narrow\":true}],"
        "\"rules\":[{\"id\":\"lane\",\"paths\":[\"src/"
        "**\"],\"action\":\"lanes\",\"lanes\":[\"fast\"]},"
        "{\"id\":\"suite\",\"paths\":[\"perf/**\"],\"action\":\"suites\",\"suites\":[\"@perf\"]},"
        "{\"id\":\"refs\",\"paths\":[\"fixtures/"
        "**\"],\"action\":\"referencing_tests\",\"key\":\"path_segment:1\"}]}}");
    ASSERT_NOT_NULL(policy);
    int count = 0;
    const cbm_test_lane_t *lanes = cbm_test_policy_lanes(policy, &count);
    ASSERT_EQ(count, 2);
    ASSERT_FALSE(lanes[0].default_run);
    ASSERT_FALSE(lanes[0].narrow);
    ASSERT_STR_EQ(lanes[0].signals[0], "lang");
    bool selected = false;
    ASSERT_TRUE(cbm_test_policy_lane_selects_suite(policy, 0, "alpha", false, &selected));
    ASSERT_TRUE(selected);
    ASSERT_TRUE(cbm_test_policy_lane_selects_suite(policy, 0, "alpha_slow", true, &selected));
    ASSERT_FALSE(selected);
    ASSERT_TRUE(cbm_test_policy_lane_selects_suite(policy, 0, "bench", true, &selected));
    ASSERT_TRUE(selected);
    ASSERT_TRUE(cbm_test_policy_lane_selects_suite(policy, 0, "bench", false, &selected));
    ASSERT_FALSE(selected);
    ASSERT_TRUE(cbm_test_policy_rule_selects_suite(policy, 1, "bench", true, &selected));
    ASSERT_TRUE(selected);
    ASSERT_TRUE(cbm_test_policy_rule_selects_suite(policy, 1, "@perf", false, &selected));
    ASSERT_FALSE(selected);
    const cbm_test_rule_t *rules = cbm_test_policy_rules(policy, &count);
    ASSERT_STR_EQ(rules[0].lanes[0], "fast");
    ASSERT_EQ(rules[2].key_segment, 1);
    cbm_test_policy_free(policy);
    PASS();
}

TEST(test_policy_globs_braces_escapes_and_directory_boundaries) {
    cbm_test_policy_t *policy = policy_of(
        "{\"test_impact\":{\"version\":1,\"rules\":["
        "{\"id\":\"alt\",\"paths\":[\"/{src,{lib,pkg}}/{a,b}{1,2}.c\"],\"action\":\"ignore\"},"
        "{\"id\":\"literal\",\"paths\":[\"literal/\\\\*.c\",\"literal/"
        "\\\\{a\\\\,b\\\\}.c\",\"literal/{word}.c\",\"literal/[{},].c\"],\"action\":\"ignore\"},"
        "{\"id\":\"dir\",\"paths\":[\"fixtures/\",\"/rooted/\"],\"action\":\"ignore\"},"
        "{\"id\":\"class\",\"paths\":[\"x[.-0]y\",\"**/case?.[ch]\"],\"action\":\"ignore\"}]}}");
    ASSERT_NOT_NULL(policy);
    const char *alternatives[] = {"src/a1.c", "src/b2.c", "lib/b1.c", "pkg/a2.c"};
    for (size_t i = 0; i < sizeof(alternatives) / sizeof(alternatives[0]); i++)
        ASSERT_TRUE(policy_matches(policy, alternatives[i], "alt"));
    ASSERT_TRUE(policy_matches(policy, "other/src/a1.c", NULL));
    ASSERT_TRUE(policy_matches(policy, "literal/*.c", "literal"));
    ASSERT_TRUE(policy_matches(policy, "literal/{a,b}.c", "literal"));
    ASSERT_TRUE(policy_matches(policy, "literal/{word}.c", "literal"));
    ASSERT_TRUE(policy_matches(policy, "literal/,.c", "literal"));
    ASSERT_TRUE(policy_matches(policy, "literal/normal.c", NULL));
    ASSERT_TRUE(policy_matches(policy, "fixtures/a/b.c", "dir"));
    ASSERT_TRUE(policy_matches(policy, "nested/fixtures/a.c", "dir"));
    ASSERT_TRUE(policy_matches(policy, "rooted/a.c", "dir"));
    ASSERT_TRUE(policy_matches(policy, "nested/rooted/a.c", NULL));
    ASSERT_TRUE(policy_matches(policy, "fixtures", NULL));
    ASSERT_TRUE(policy_matches(policy, "x/y", NULL));
    ASSERT_TRUE(policy_matches(policy, "x.y", "class"));
    ASSERT_TRUE(policy_matches(policy, "case1.c", "class"));
    ASSERT_TRUE(policy_matches(policy, "a/b/case2.h", "class"));
    cbm_test_policy_free(policy);
    PASS();
}

TEST(test_policy_rejects_invalid_consumed_fields_without_changing_seed_loader) {
    const char *bad[] = {
        "\"rules\":null",
        "\"rules\":[],\"rules\":[]",
        "\"lanes\":[]",
        "\"lanes\":{}",
        "\"unmapped_files\":\"ignore\"",
        "\"unmapped_files\":\"run_all\",\"unmapped_files\":\"run_all\"",
        "\"rules\":[{\"id\":\"a\",\"id\":\"b\",\"paths\":[\"*\"],\"action\":\"ignore\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"*\"],\"action\":\"unknown\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[],\"action\":\"ignore\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"!x\"],\"action\":\"ignore\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"x{a,b\"],\"action\":\"ignore\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"x}a,b\"],\"action\":\"ignore\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"{,}\"],\"action\":\"ignore\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"*\"],\"action\":\"ignore\",\"suites\":[\"a\"]}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"*\"],\"action\":\"suites\",\"suites\":[]}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"*\"],\"action\":\"lanes\",\"lanes\":[\"undeclared\"]"
        "}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"*\"],\"action\":\"referencing_tests\",\"key\":"
        "\"path_segment:-1\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"*\"],\"action\":\"referencing_tests\",\"key\":"
        "\"path_segment:1x\"}]",
        "\"rules\":[{\"id\":\"a\",\"paths\":[\"*\"],\"action\":\"ignore\"},{\"id\":\"a\",\"paths\":"
        "[\"q\"],\"action\":\"run_all\"}]",
        "\"rules\":[{\"id\":\"builtin:build\",\"paths\":[\"*\"],\"action\":\"ignore\"}]",
        "\"lanes\":[{\"name\":\"a\",\"suites\":[\"*\"]}]",
        "\"lanes\":[{\"name\":\"a\",\"suites\":[\"*\"],\"default\":\"maybe\"}]",
        "\"lanes\":[{\"name\":\"a\",\"suites\":[\"*\"],\"default\":\"run\",\"narrow\":1}]",
        "\"lanes\":[{\"name\":\"a\",\"suites\":[\"*\"],\"default\":\"run\"},{\"name\":\"a\","
        "\"suites\":[\"*\"],\"default\":\"skip\"}]"};
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char json[2048];
        snprintf(json, sizeof(json), "{\"test_impact\":{\"version\":1,%s}}", bad[i]);
        char dir[512], path[600];
        snprintf(dir, sizeof(dir), "%s/cbm-impact-policy-invalid-XXXXXX", cbm_tmpdir());
        ASSERT_NOT_NULL(cbm_mkdtemp(dir));
        snprintf(path, sizeof(path), "%s/config.json", dir);
        ASSERT_EQ(th_write_file(path, json), 0);
        cbm_test_config_t *config = cbm_test_config_load(path, false);
        ASSERT_NOT_NULL(config);
        cbm_test_policy_t *policy = cbm_test_policy_new(config);
        bool accepted = policy != NULL;
        cbm_test_policy_free(policy);
        cbm_test_config_free(config);
        th_rmtree(dir);
        if (accepted)
            printf("  invalid policy %zu accepted\n", i);
        ASSERT_FALSE(accepted);
    }
    cbm_test_policy_t *policy = policy_of("{\"test_impact\":{\"version\":1,\"future\":null,"
                                          "\"rules\":[],\"unmapped_files\":\"run_all\"}}");
    ASSERT_NOT_NULL(policy);
    cbm_test_policy_free(policy);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_policy_expansion_limits_reject_whole_policy) {
    char json[4096], pattern[2048];
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    const char *prefix =
        "{\"test_impact\":{\"version\":1,\"rules\":[{\"id\":\"limit\",\"paths\":[\"";
    const char *suffix = "\"],\"action\":\"ignore\"}]}}";
    /* 8192 expansions; bounded source size must not permit combinatorial work. */
    pattern[0] = '\0';
    for (int i = 0; i < 13; i++)
        strcat(pattern, "{a,b}");
    snprintf(json, sizeof(json), "%s%s%s", prefix, pattern, suffix);
    ASSERT_NULL(policy_of(json));
    /* 1024 alternatives of >300 bytes exceeds expanded-byte cap. */
    memset(pattern, 'x', 300);
    pattern[300] = '\0';
    for (int i = 0; i < 10; i++)
        strcat(pattern, "{a,b}");
    snprintf(json, sizeof(json), "%s%s%s", prefix, pattern, suffix);
    ASSERT_NULL(policy_of(json));
    int n = 0;
    for (int i = 0; i < 17; i++)
        pattern[n++] = '{';
    pattern[n++] = 'x';
    for (int i = 0; i < 17; i++) {
        pattern[n++] = ',';
        pattern[n++] = 'y';
        pattern[n++] = '}';
    }
    pattern[n] = '\0';
    snprintf(json, sizeof(json), "%s%s%s", prefix, pattern, suffix);
    ASSERT_NULL(policy_of(json));
    /* Many adjacent groups with one empty branch are legal within the caps. */
    snprintf(json, sizeof(json), "%sprefix{,a}{,b}%s", prefix, suffix);
    cbm_test_policy_t *policy = policy_of(json);
    ASSERT_NOT_NULL(policy);
    ASSERT_TRUE(policy_matches(policy, "prefix", "limit"));
    ASSERT_TRUE(policy_matches(policy, "prefixab", "limit"));
    cbm_test_policy_free(policy);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_policy_evaluation_errors_clear_outputs_and_never_fall_through) {
    char json[2048], pattern[401];
    memset(pattern, '?', 400);
    pattern[400] = '\0';
    snprintf(
        json, sizeof(json),
        "{\"test_impact\":{\"version\":1,\"rules\":[{\"id\":\"costly\",\"paths\":[\"%s\"],"
        "\"action\":\"run_all\"},{\"id\":\"later\",\"paths\":[\"*\"],\"action\":\"ignore\"}]}}",
        pattern);
    cbm_test_policy_t *policy = policy_of(json);
    ASSERT_NOT_NULL(policy);
    char *path = malloc(65538);
    ASSERT_NOT_NULL(path);
    memset(path, 'a', 65537);
    path[65536] = '\0';
    const cbm_test_rule_t *rule = (const cbm_test_rule_t *)policy;
    ASSERT_FALSE(cbm_test_policy_match_path(policy, path, &rule));
    ASSERT_NULL(rule);
    path[65536] = 'a';
    path[65537] = '\0';
    rule = (const cbm_test_rule_t *)policy;
    ASSERT_FALSE(cbm_test_policy_match_path(policy, path, &rule));
    ASSERT_NULL(rule);
    rule = (const cbm_test_rule_t *)policy;
    ASSERT_FALSE(cbm_test_policy_match_path(NULL, "x", &rule));
    ASSERT_NULL(rule);
    rule = (const cbm_test_rule_t *)policy;
    ASSERT_FALSE(cbm_test_policy_match_path(policy, NULL, &rule));
    ASSERT_NULL(rule);
    bool selected = true;
    ASSERT_FALSE(cbm_test_policy_lane_selects_suite(policy, 99, "a", false, &selected));
    ASSERT_FALSE(selected);
    selected = true;
    ASSERT_FALSE(cbm_test_policy_rule_selects_suite(policy, 0, "a", false, &selected));
    ASSERT_FALSE(selected);
    cbm_test_policy_free(policy);
    snprintf(json, sizeof(json),
             "{\"test_impact\":{\"version\":1,\"lanes\":[{\"name\":\"costly\","
             "\"suites\":[\"*\"],\"exclude_suites\":[\"%s\"],\"default\":\"run\"}],"
             "\"rules\":[{\"id\":\"costly\",\"paths\":[\"src/**\"],"
             "\"action\":\"suites\",\"suites\":[\"%s\"]}]}}",
             pattern, pattern);
    policy = policy_of(json);
    ASSERT_NOT_NULL(policy);
    path[65536] = '\0';
    selected = true;
    ASSERT_FALSE(cbm_test_policy_lane_selects_suite(policy, 0, path, false, &selected));
    ASSERT_FALSE(selected); /* inclusion succeeded before exclusion exhausted */
    selected = true;
    ASSERT_FALSE(cbm_test_policy_rule_selects_suite(policy, 0, path, false, &selected));
    ASSERT_FALSE(selected);
    free(path);
    cbm_test_policy_free(policy);
    PASS();
}

TEST(test_policy_embedded_stars_do_not_cross_or_remove_separators) {
    cbm_test_policy_t *policy = policy_of(
        "{\"test_impact\":{\"version\":1,\"rules\":["
        "{\"id\":\"embedded\",\"paths\":[\"/src/a**b.c\",\"/a**/b\"],\"action\":\"ignore\"},"
        "{\"id\":\"segments\",\"paths\":[\"/lib/**/x.c\",\"/deep/**\"],\"action\":\"run_all\"}]}}");
    ASSERT_NOT_NULL(policy);
    bool nested = policy_matches(policy, "src/a/x/b.c", NULL);
    bool removed = policy_matches(policy, "ab", NULL);
    ASSERT_TRUE(policy_matches(policy, "src/aXXb.c", "embedded"));
    ASSERT_TRUE(policy_matches(policy, "aXX/b", "embedded"));
    ASSERT_TRUE(policy_matches(policy, "lib/x.c", "segments"));
    ASSERT_TRUE(policy_matches(policy, "lib/a/b/x.c", "segments"));
    ASSERT_TRUE(policy_matches(policy, "deep/a/b.c", "segments"));
    cbm_test_policy_free(policy);
    ASSERT_TRUE(nested);
    ASSERT_TRUE(removed);
    PASS();
}

/* The common declaration snapshot is data, not a lossy six-macro adapter. */
static cbm_test_declarations_t *declarations_of(const char *json) {
    return cbm_test_declarations_parse(json, strlen(json), false);
}

TEST(test_declarations_defaults_preserve_absent_and_configured_identity) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_test_declarations_t *missing = cbm_test_declarations_parse(NULL, 0, true);
    ASSERT_NOT_NULL(missing);
    ASSERT_FALSE(cbm_test_declarations_configured(missing));
    ASSERT_STR_EQ(cbm_test_declarations_digest(missing),
                  "f79ea6058b0b9bc5faea14c64282c1dece1b88816fbcbbd021dd88e611078c93");
    int count = 99;
    (void)cbm_test_declarations_items(missing, &count);
    ASSERT_EQ(count, 0);
    for (int i = 0; i < CBM_TEST_PRESET_COUNT; i++) {
        bool enabled = false;
        ASSERT_TRUE(cbm_test_declarations_preset(missing, (cbm_test_preset_t)i, &enabled));
        ASSERT_EQ(enabled, i != CBM_TEST_PRESET_C_CBM);
    }
    cbm_test_declarations_t *empty = declarations_of("{}");
    ASSERT_NOT_NULL(empty);
    ASSERT_FALSE(cbm_test_declarations_configured(empty));
    ASSERT_STR_EQ(cbm_test_declarations_digest(empty),
                  "30532ab60754ce0077e67d2967daf1bbd2047ad2ed8bd1767374f71ebd916f8c");
    cbm_test_declarations_t *configured =
        declarations_of("{ \"test_impact\" : {\"version\":1} }\n");
    ASSERT_NOT_NULL(configured);
    ASSERT_TRUE(cbm_test_declarations_configured(configured));
    ASSERT_STR_EQ(cbm_test_declarations_digest(configured),
                  "928be92116c30e969a1769755e9fd1206501aa39311ea21e202ea9ded955dded");
    cbm_test_declarations_free(missing);
    cbm_test_declarations_free(empty);
    cbm_test_declarations_free(configured);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_declarations_preserve_all_roles_arguments_and_owned_strings) {
    const char *json =
        "{\"test_impact\":{\"version\":1,\"tests\":{"
        "\"presets\":{\"c-cbm\":true,\"pytest\":false},\"conventions\":["
        "{\"language\":\"cpp\",\"role\":\"case\",\"define_macro\":\"SPEC_CASE\",\"name_args\":[1,0,"
        "1],\"runner_id\":\"{suite}/{name}\"},"
        "{\"language\":\"c\",\"role\":\"suite\",\"define_macro\":\"GROUP\",\"name_args\":[0]},"
        "{\"language\":\"c\",\"role\":\"registration\",\"macro\":\"EXEC_CASE\",\"test_arg\":1},"
        "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"EXEC_GROUP\",\"suite_"
        "arg\":2,\"perf_macro\":\"EXEC_PERF\"},"
        "{\"language\":\"custom-lang\",\"role\":\"case\",\"define_macro\":\"CUSTOM\",\"name_args\":"
        "[0]},"
        "{\"language\":\"custom-lang\",\"role\":\"case\",\"define_macro\":\"OPAQUE\",\"name_args\":"
        "[0],\"runner_id\":\"external::{unsupported}/%s\"}]}}}";
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), len = strlen(json);
    char *input = malloc(len);
    ASSERT_NOT_NULL(input);
    memcpy(input, json, len);
    cbm_test_declarations_t *d = cbm_test_declarations_parse(input, len, false);
    memset(input, '?', len);
    free(input);
    ASSERT_NOT_NULL(d);
    int count = 0;
    const cbm_test_declaration_t *items = cbm_test_declarations_items(d, &count);
    ASSERT_EQ(count, 6);
    ASSERT_NOT_NULL(items);
    ASSERT_STR_EQ(items[0].language, "cpp");
    ASSERT_EQ(items[0].role, CBM_TEST_DECL_CASE);
    ASSERT_STR_EQ(items[0].define_macro, "SPEC_CASE");
    ASSERT_EQ(items[0].name_arg_count, 3);
    ASSERT_EQ(items[0].name_args[0], 1);
    ASSERT_EQ(items[0].name_args[1], 0);
    ASSERT_EQ(items[0].name_args[2], 1);
    ASSERT_STR_EQ(items[0].runner_id, "{suite}/{name}");
    ASSERT_NULL(items[0].macro);
    ASSERT_EQ(items[0].test_arg, -1);
    ASSERT_EQ(items[0].suite_arg, -1);
    ASSERT_EQ(items[1].role, CBM_TEST_DECL_SUITE);
    ASSERT_STR_EQ(items[1].define_macro, "GROUP");
    ASSERT_NULL(items[1].runner_id);
    ASSERT_EQ(items[1].name_arg_count, 1);
    ASSERT_EQ(items[2].role, CBM_TEST_DECL_REGISTRATION);
    ASSERT_STR_EQ(items[2].macro, "EXEC_CASE");
    ASSERT_EQ(items[2].test_arg, 1);
    ASSERT_EQ(items[2].suite_arg, -1);
    ASSERT_EQ(items[2].name_arg_count, 0);
    ASSERT_NULL(items[2].define_macro);
    ASSERT_EQ(items[3].role, CBM_TEST_DECL_SUITE_REGISTRATION);
    ASSERT_EQ(items[3].suite_arg, 2);
    ASSERT_EQ(items[3].test_arg, -1);
    ASSERT_STR_EQ(items[3].perf_macro, "EXEC_PERF");
    ASSERT_STR_EQ(items[4].language, "custom-lang");
    ASSERT_NULL(items[4].runner_id);
    ASSERT_STR_EQ(items[5].runner_id, "external::{unsupported}/%s");
    bool enabled = false;
    ASSERT_TRUE(cbm_test_declarations_preset(d, CBM_TEST_PRESET_C_CBM, &enabled));
    ASSERT_TRUE(enabled);
    ASSERT_TRUE(cbm_test_declarations_preset(d, CBM_TEST_PRESET_PYTEST, &enabled));
    ASSERT_FALSE(enabled);
    ASSERT_TRUE(cbm_test_declarations_preset(d, CBM_TEST_PRESET_GTEST, &enabled));
    ASSERT_TRUE(enabled);
    cbm_test_declarations_free(d);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_declarations_independent_snapshots_match_loaded_config_digest) {
    const char *json =
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"go\":false,\"c-cbm\":true},"
        "\"conventions\":[{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"A\",\"name_"
        "args\":[0]}]}}}";
    char dir[512], path[600];
    snprintf(dir, sizeof(dir), "%s/cbm-declarations-XXXXXX", cbm_tmpdir());
    ASSERT_NOT_NULL(cbm_mkdtemp(dir));
    snprintf(path, sizeof(path), "%s/config.json", dir);
    ASSERT_EQ(th_write_file(path, json), 0);
    cbm_test_config_t *config = cbm_test_config_load(path, false);
    ASSERT_NOT_NULL(config);
    size_t len = 0;
    bool absent = false;
    const char *source = cbm_test_config_source(config, &len, &absent);
    cbm_test_declarations_t *first = cbm_test_declarations_parse(source, len, absent);
    ASSERT_NOT_NULL(first);
    ASSERT_STR_EQ(cbm_test_declarations_digest(first), cbm_test_config_digest(config));
    cbm_test_config_free(config);
    th_rmtree(dir);
    cbm_test_declarations_t *second = declarations_of(
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"pytest\":false}}}}");
    ASSERT_NOT_NULL(second);
    bool enabled = false;
    ASSERT_TRUE(cbm_test_declarations_preset(second, CBM_TEST_PRESET_GO, &enabled));
    ASSERT_TRUE(enabled);
    ASSERT_TRUE(cbm_test_declarations_preset(first, CBM_TEST_PRESET_GO, &enabled));
    ASSERT_FALSE(enabled);
    ASSERT_TRUE(cbm_test_declarations_preset(first, CBM_TEST_PRESET_PYTEST, &enabled));
    ASSERT_TRUE(enabled);
    cbm_test_declarations_free(second);
    int count = 0;
    const cbm_test_declaration_t *items = cbm_test_declarations_items(first, &count);
    ASSERT_EQ(count, 1);
    ASSERT_STR_EQ(items[0].define_macro, "A");
    cbm_test_declarations_free(first);
    PASS();
}

TEST(test_declarations_invalid_consumed_structure_fails_without_partial_result) {
    const char *bad[] = {
        "",
        "[]",
        "{",
        "{\"test_impact\":null}",
        "{\"test_impact\":{}}",
        "{\"test_impact\":{\"version\":1.0}}",
        "{\"test_impact\":{\"version\":2}}",
        "{\"test_impact\":{\"version\":1},\"test_impact\":{\"version\":1}}",
        "{\"test_impact\":{\"version\":1,\"version\":1}}",
        "{\"test_impact\":{\"version\":1,\"tests\":null}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{},\"tests\":{}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":null}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[],\"conventions\":[]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":[]}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{},\"presets\":{}}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"go\":0}}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"go\":false,\"go\":true}}}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"unsupported\":true}}}}"};
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        cbm_test_declarations_t *d = declarations_of(bad[i]);
        bool accepted = d != NULL;
        cbm_test_declarations_free(d);
        if (accepted)
            printf("  invalid declarations structure %zu accepted\n", i);
        ASSERT_FALSE(accepted);
    }
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    cbm_test_declarations_t *d =
        declarations_of("{\"test_impact\":{\"version\":1,\"future\":null,\"tests\":{\"future\":42,"
                        "\"conventions\":[],\"presets\":{}}}}");
    ASSERT_NOT_NULL(d);
    cbm_test_declarations_free(d);
    PASS();
}

TEST(test_declarations_invalid_role_payloads_are_not_ignored) {
    const char *bad[] = {
        "null",
        "{}",
        "{\"language\":\"c\",\"role\":\"helper\"}",
        "{\"language\":\"\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[0]}",
        "{\"language\":\"c\\u0000x\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[0]"
        "}",
        "{\"language\":\"c\",\"language\":\"cpp\",\"role\":\"case\",\"define_macro\":\"TEST\","
        "\"name_args\":[0]}",
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"9TEST\",\"name_args\":[0]}",
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[]}",
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[-1]}",
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[1.0]}",
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[2147483648]"
        "}",
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[0],"
        "\"runner_id\":null}",
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[0],\"test_"
        "arg\":0}",
        "{\"language\":\"c\",\"role\":\"suite\",\"define_macro\":\"GROUP\",\"name_args\":[0],"
        "\"runner_id\":\"x\"}",
        "{\"language\":\"c\",\"role\":\"registration\",\"macro\":\"RUN\"}",
        "{\"language\":\"c\",\"role\":\"registration\",\"macro\":\"RUN-TEST\",\"test_arg\":0}",
        "{\"language\":\"c\",\"role\":\"registration\",\"macro\":\"RUN\",\"test_arg\":0,\"suite_"
        "arg\":0}",
        "{\"language\":\"c\",\"role\":\"registration\",\"macro\":\"RUN\",\"test_arg\":0,\"name_"
        "args\":[0]}",
        "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"RUN_SUITE\",\"suite_arg\":"
        "-1}",
        "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"RUN_SUITE\",\"suite_arg\":"
        "0,\"perf_macro\":\"\"}",
        "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"RUN_SUITE\",\"suite_arg\":"
        "0,\"suite_arg\":1}"};
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    for (size_t i = 0; i < sizeof(bad) / sizeof(bad[0]); i++) {
        char json[4096];
        snprintf(json, sizeof(json),
                 "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[{\"language\":\"c\","
                 "\"role\":\"case\",\"define_macro\":\"VALID\",\"name_args\":[0]},%s]}}}",
                 bad[i]);
        cbm_test_declarations_t *d = declarations_of(json);
        bool accepted = d != NULL;
        cbm_test_declarations_free(d);
        if (accepted)
            printf("  invalid declaration payload %zu accepted\n", i);
        ASSERT_FALSE(accepted);
    }
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_declarations_exact_length_limits_and_safe_accessors) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    char *bytes = malloc(65537);
    ASSERT_NOT_NULL(bytes);
    memset(bytes, ' ', 65537);
    bytes[0] = '{';
    bytes[1] = '}';
    cbm_test_declarations_t *d = cbm_test_declarations_parse(bytes, 65536, false);
    ASSERT_NOT_NULL(d);
    cbm_test_declarations_free(d);
    ASSERT_NULL(cbm_test_declarations_parse(bytes, 65537, false));
    bytes[5] = '\0';
    ASSERT_NULL(cbm_test_declarations_parse(bytes, 65536, false));
    free(bytes);
    ASSERT_NULL(cbm_test_declarations_parse(NULL, 1, false));
    ASSERT_NULL(cbm_test_declarations_parse(NULL, 0, false));
    ASSERT_NULL(cbm_test_declarations_parse("{}", 2, true));
    d = cbm_test_declarations_parse("{}trailing", 2, false);
    ASSERT_NOT_NULL(d);
    cbm_test_declarations_free(d);
    ASSERT_NULL(cbm_test_declarations_parse("{}trailing", 10, false));
    bool enabled = true;
    ASSERT_FALSE(cbm_test_declarations_preset(NULL, CBM_TEST_PRESET_GO, &enabled));
    ASSERT_FALSE(enabled);
    d = declarations_of("{}");
    ASSERT_NOT_NULL(d);
    enabled = true;
    ASSERT_FALSE(cbm_test_declarations_preset(d, CBM_TEST_PRESET_COUNT, &enabled));
    ASSERT_FALSE(enabled);
    enabled = true;
    ASSERT_FALSE(cbm_test_declarations_preset(d, (cbm_test_preset_t)-1, &enabled));
    ASSERT_FALSE(enabled);
    cbm_test_declarations_free(d);
    int count = 77;
    ASSERT_NULL(cbm_test_declarations_items(NULL, &count));
    ASSERT_EQ(count, 0);
    ASSERT_FALSE(cbm_test_declarations_configured(NULL));
    ASSERT_STR_EQ(cbm_test_declarations_digest(NULL), "");
    cbm_test_declarations_free(NULL);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_declarations_argument_boundaries_and_order_are_preserved) {
    char json[4096], args[1024];
    size_t used = 0;
    for (int i = 0; i < 64; i++)
        used += (size_t)snprintf(args + used, sizeof(args) - used, "%s%d", i ? "," : "", i);
    snprintf(json, sizeof(json),
             "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[{\"language\":\"c\","
             "\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[%s]}]}}}",
             args);
    cbm_test_declarations_t *d = declarations_of(json);
    ASSERT_NOT_NULL(d);
    int count = 0;
    const cbm_test_declaration_t *items = cbm_test_declarations_items(d, &count);
    ASSERT_EQ(count, 1);
    ASSERT_EQ(items[0].name_arg_count, 64);
    ASSERT_EQ(items[0].name_args[0], 0);
    ASSERT_EQ(items[0].name_args[63], 63);
    cbm_test_declarations_free(d);
    strcat(args, ",64");
    snprintf(json, sizeof(json),
             "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[{\"language\":\"c\","
             "\"role\":\"case\",\"define_macro\":\"TEST\",\"name_args\":[%s]}]}}}",
             args);
    ASSERT_NULL(declarations_of(json));
    d = declarations_of(
        "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[{\"language\":\"c\",\"role\":"
        "\"registration\",\"macro\":\"RUN\",\"test_arg\":2147483647},{\"language\":\"c\",\"role\":"
        "\"registration\",\"macro\":\"RUN\",\"test_arg\":0},{\"language\":\"c\",\"role\":"
        "\"registration\",\"macro\":\"RUN\",\"test_arg\":0}]}}}");
    ASSERT_NOT_NULL(d);
    items = cbm_test_declarations_items(d, &count);
    ASSERT_EQ(count, 3);
    ASSERT_EQ(items[0].test_arg, 2147483647);
    ASSERT_EQ(items[1].test_arg, 0);
    ASSERT_EQ(items[2].test_arg, 0);
    ASSERT_STR_EQ(items[1].macro, items[2].macro);
    cbm_test_declarations_free(d);
    PASS();
}

TEST(test_model_completeness_requires_finished_success_and_stays_uncertain) {
    ASSERT_FALSE(cbm_test_model_complete(NULL));
    cbm_test_model_t *model = cbm_test_model_new(cbm_test_conventions_cbm());
    ASSERT_NOT_NULL(model);
    ASSERT_FALSE(cbm_test_model_complete(model));
    ASSERT_TRUE(cbm_test_model_add_source(model, "empty.c", "", 0));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_TRUE(cbm_test_model_complete(model));
    cbm_test_model_free(model);
    model = cbm_test_model_new(cbm_test_conventions_cbm());
    ASSERT_NOT_NULL(model);
    ASSERT_TRUE(cbm_test_model_add_source(model, "first.c", "/* unfinished", 13));
    ASSERT_TRUE(
        cbm_test_model_add_source(model, "second.c", selection_source, strlen(selection_source)));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_FALSE(cbm_test_model_complete(model));
    ASSERT_EQ(count_cases(model), 4);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_reports_truncated_and_unsupported_syntax) {
    const char *uncertain[] = {"/* unfinished",
                               "const char *s = \"unfinished",
                               "char x = '\\",
                               "TEST(unfinished) {",
                               "SUITE(unfinished) {",
                               "#if FEATURE\nTEST(hidden) {}\n",
                               "#else\n",
                               "#endif\n",
                               "}\n",
                               "void helper(void) {\n",
                               "TEST(,bad) {}\n",
                               "TEST(a,b) {}\n",
                               "TEST(no_body);\n",
                               "SUITE(no_body);\n",
                               "void main(void) { RUN_SELECTED_SUITE(a,b); }\n",
                               "SUITE(alpha) { RUN_TEST(a,b); }\n",
                               "namespace nested { TEST(hidden) {} }\n",
                               "void helper(void) { TEST(hidden) {} }\n",
                               "const char *s = R\"tag(ordinary text)tag\";\n",
                               "const char *s = u8R\"tag(ordinary text)tag\";\n"};
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    for (size_t i = 0; i < sizeof(uncertain) / sizeof(uncertain[0]); i++) {
        tm_source_t sources[] = {{"tests/incomplete.c", uncertain[i]}};
        cbm_test_model_t *model = model_of(sources, 1);
        ASSERT_NOT_NULL(model);
        bool complete = cbm_test_model_complete(model);
        cbm_test_model_free(model);
        if (complete)
            printf("  incomplete source %zu reported complete\n", i);
        ASSERT_FALSE(complete);
    }
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_model_complete_sources_keep_normal_declarations_and_literals) {
    const char *source =
        "/* TEST(fake) {} */\n"
        "void suite_alpha(void);\nvoid helper(void);\n"
        "const char *s = \"TEST(fake) {} and escaped \\\" quotes\";\n"
        "char quote = '\\'';\n"
        "#if FEATURE\nTEST(one) {}\n#else\nTEST(two) {}\n#endif\n"
        "SUITE(alpha) {\n#if FEATURE\nRUN_TEST(one);\n#else\nRUN_TEST(two);\n#endif\n}\n"
        "void main(void) { RUN_SELECTED_SUITE(alpha); }\n";
    cbm_test_model_t *model = selection_model(source);
    ASSERT_NOT_NULL(model);
    ASSERT_TRUE(cbm_test_model_complete(model));
    ASSERT_EQ(count_cases(model), 2);
    ASSERT_EQ(count_suites(model), 1);
    ASSERT_EQ(count_registrations(model), 2);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_conditional_limit_is_explicit_and_deterministic) {
    for (int nesting = 64; nesting <= 65; nesting++) {
        char source[4096];
        size_t used = 0;
        for (int i = 0; i < nesting; i++)
            used += (size_t)snprintf(source + used, sizeof(source) - used, "#if FEATURE\n");
        used += (size_t)snprintf(source + used, sizeof(source) - used, "TEST(one) {}\n");
        for (int i = 0; i < nesting; i++)
            used += (size_t)snprintf(source + used, sizeof(source) - used, "#endif\n");
        cbm_test_model_t *model = selection_model(source);
        ASSERT_NOT_NULL(model);
        bool complete = cbm_test_model_complete(model);
        cbm_test_model_free(model);
        ASSERT_EQ(complete, nesting == 64);
    }
    PASS();
}

TEST(test_selection_cannot_override_partial_model_with_complete_input_flag) {
    const char *suffixes[] = {"/* unfinished", "#if FEATURE\n", "TEST(incomplete) {",
                              "namespace nested { TEST(hidden) {} }\n"};
    cbm_coverage_map_t *map = selection_map(selection_rows);
    ASSERT_NOT_NULL(map);
    cbm_test_reach_t reach[] = {{"tests/cases.c", "stat", true, true, false},
                                {"tests/cases.c", "cov", true, false, false},
                                {"tests/cases.c", "changed", true, false, true},
                                {"tests/cases.c", "quiet", true, false, false}};
    int changed[] = {0};
    for (size_t i = 0; i < sizeof(suffixes) / sizeof(suffixes[0]); i++) {
        char source[2048];
        snprintf(source, sizeof(source), "%s%s", selection_source, suffixes[i]);
        cbm_test_model_t *model = selection_model(source);
        ASSERT_NOT_NULL(model);
        cbm_test_selection_input_t input = selection_input(model, map, reach, changed);
        ASSERT_TRUE(input.inventory_complete);
        cbm_test_selection_t *result = cbm_test_select(&input);
        ASSERT_NOT_NULL(result);
        unsigned flags = cbm_test_selection_run_all(result);
        int count = 99;
        cbm_test_selection_cases(result, &count);
        cbm_test_selection_free(result);
        cbm_test_model_free(model);
        ASSERT_EQ(flags, CBM_TEST_SELECT_INVENTORY_UNKNOWN);
        ASSERT_EQ(count, 0);
    }
    cbm_coverage_map_free(map);
    PASS();
}

TEST(test_model_split_identifiers_cannot_hide_registrations) {
    const char *joins[] = {"\\\n", "\\\r\n"};
    for (size_t i = 0; i < sizeof(joins) / sizeof(joins[0]); i++) {
        char source[512];
        snprintf(source, sizeof(source),
                 "TEST(hidden) {}\nSUITE(alpha) { RUN_TE%sST(hidden); }\n"
                 "void main(void) { RUN_SELECTED_SUITE(alpha); }\n",
                 joins[i]);
        cbm_test_model_t *model = selection_model(source);
        ASSERT_NOT_NULL(model);
        int count = 0;
        cbm_test_model_registrations(model, &count);
        bool safe = !cbm_test_model_complete(model) || count == 1;
        cbm_test_model_free(model);
        ASSERT_TRUE(safe);
    }
    PASS();
}

TEST(test_model_guard_like_conditions_cannot_certify_unconditional_cases) {
    const char *sources[] = {
        ("TEST(one) {}\nTEST(two) {}\nSUITE(alpha) {\n#ifndef SWITCH\n#define SWITCH\n"
         "RUN_TEST(one);\n#else\nRUN_TEST(two);\n#endif\n}\n"),
        "#ifndef GUARD\n#define GUARD\nTEST(one) {}\n#else\nTEST(two) {}\n#endif\n",
        "#ifndef GUARD\n#define GUARD\nTEST(one) {}\n#elif FEATURE\nTEST(two) {}\n#endif\n"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        cbm_test_model_t *model = selection_model(sources[i]);
        ASSERT_NOT_NULL(model);
        bool safe = !cbm_test_model_complete(model);
        if (!safe) {
            int count = 0;
            if (i == 0) {
                const cbm_test_registration_t *items = cbm_test_model_registrations(model, &count);
                safe = count == 2 && items[0].conditional && items[1].conditional;
            } else {
                const cbm_test_case_t *items = cbm_test_model_cases(model, &count);
                safe = count == 2 && items[0].conditional && items[1].conditional;
            }
        }
        cbm_test_model_free(model);
        ASSERT_TRUE(safe);
    }
    PASS();
}

/* `#ifndef X` + `#define X` that does not start the file is the
 * define-if-undefined idiom (`WIN32_LEAN_AND_MEAN` in 23 cbm source files),
 * not an include guard: an ordinary condition whose contents are conditional.
 * It made those files uncertain, and every cbm registry unnarrowable. */
TEST(test_model_define_if_undefined_is_an_ordinary_condition) {
    const char *source = "#include <stdio.h>\n"
                         "#ifndef LEAN\n#define LEAN\n#endif\n"
                         "TEST(plain) {}\n"
                         "#ifndef FEATURE\n#define FEATURE\nTEST(gated) {}\n#endif\n"
                         "SUITE(alpha) { RUN_TEST(plain); RUN_TEST(gated); }\n";
    cbm_test_model_t *model = selection_model(source);
    ASSERT_NOT_NULL(model);
    ASSERT_TRUE(cbm_test_model_complete(model));
    int count = 0;
    const cbm_test_case_t *cases = cbm_test_model_cases(model, &count);
    ASSERT_EQ(count, 2);
    ASSERT_STR_EQ(cases[0].name, "plain");
    ASSERT_FALSE(cases[0].conditional);
    ASSERT_STR_EQ(cases[1].name, "gated");
    ASSERT_TRUE(cases[1].conditional);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_empty_elif_is_not_certified) {
    const char *branches[] = {"#elif\n", "#elif /* no expression */\n", "#elif // empty\n"};
    for (size_t i = 0; i < sizeof(branches) / sizeof(branches[0]); i++) {
        char source[256];
        snprintf(source, sizeof(source), "#if FEATURE\n%s#endif\n", branches[i]);
        cbm_test_model_t *model = selection_model(source);
        ASSERT_NOT_NULL(model);
        bool complete = cbm_test_model_complete(model);
        cbm_test_model_free(model);
        ASSERT_FALSE(complete);
    }
    PASS();
}

TEST(test_model_null_empty_source_is_safe) {
    cbm_test_model_t *model = cbm_test_model_new(cbm_test_conventions_cbm());
    ASSERT_NOT_NULL(model);
    ASSERT_TRUE(cbm_test_model_add_source(model, "empty.c", NULL, 0));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_TRUE(cbm_test_model_complete(model));
    cbm_test_model_free(model);
    PASS();
}

static cbm_test_model_t *descriptor_model(const char *json) {
    cbm_test_declarations_t *d = declarations_of(json);
    if (!d)
        return NULL;
    cbm_test_model_t *model = cbm_test_model_new_declarations(d);
    cbm_test_declarations_free(d);
    return model;
}

static const char *descriptor_config =
    "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":["
    "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CASE_AT\",\"name_args\":[1],\"runner_"
    "id\":\"{suite}:{name}\"},"
    "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"OTHER_CASE\",\"name_args\":[0],"
    "\"runner_id\":\"{suite}:{name}\"},"
    "{\"language\":\"c\",\"role\":\"suite\",\"define_macro\":\"GROUP_AT\",\"name_args\":[1]},"
    "{\"language\":\"c\",\"role\":\"registration\",\"macro\":\"USE_AT\",\"test_arg\":1},"
    "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"ENTER_AT\",\"suite_arg\":1,"
    "\"perf_macro\":\"ENTER_PERF\"}]}}}";

TEST(test_model_descriptors_preserve_owned_records_and_multiple_spellings) {
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    cbm_test_model_t *model = descriptor_model(descriptor_config);
    ASSERT_NOT_NULL(model);
    char source[] =
        "CASE_AT(call(a,b),first) {}\nOTHER_CASE(second) {}\n"
        "GROUP_AT((a,b),alpha) { USE_AT(\"ignore,comma\",first); USE_AT(array[1],second); }\n"
        "void main(void) { ENTER_AT((a,b),alpha); ENTER_PERF(ignored,alpha); }\n";
    ASSERT_TRUE(
        cbm_test_model_add_source_language(model, "tests/custom.c", "c", source, strlen(source)));
    memset(source, 'x', sizeof(source) - 1);
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_TRUE(cbm_test_model_complete(model));
    ASSERT_EQ(cbm_test_model_mapping_status(model, NULL), CBM_TEST_MODEL_MAPPING_OK);
    int n = 0;
    const cbm_test_declaration_t *d = cbm_test_model_declarations(model, &n);
    ASSERT_EQ(n, 5);
    ASSERT_STR_EQ(d[0].define_macro, "CASE_AT");
    ASSERT_EQ(d[0].name_args[0], 1);
    ASSERT_STR_EQ(d[0].runner_id, "{suite}:{name}");
    ASSERT_STR_EQ(d[4].perf_macro, "ENTER_PERF");
    const cbm_test_case_t *cases = cbm_test_model_cases(model, &n);
    ASSERT_EQ(n, 2);
    ASSERT_STR_EQ(cases[0].name, "first");
    ASSERT_STR_EQ(cases[1].name, "second");
    ASSERT_EQ(count_suites(model), 1);
    ASSERT_EQ(count_registrations(model), 2);
    const cbm_test_runner_suite_t *runner = cbm_test_model_runner_suites(model, &n);
    ASSERT_EQ(n, 1);
    ASSERT_STR_EQ(runner[0].name, "alpha");
    ASSERT_TRUE(runner[0].perf);
    cbm_test_model_free(model);
    ASSERT_EQ(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA), before);
    PASS();
}

TEST(test_model_descriptors_apply_only_to_explicit_source_language) {
    const char *json =
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"gtest\":false},\"conventions\":["
        "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"C_CASE\",\"name_args\":[0],"
        "\"runner_id\":\"{suite}:{name}\"},"
        "{\"language\":\"cpp\",\"role\":\"case\",\"define_macro\":\"CPP_CASE\",\"name_args\":[0],"
        "\"runner_id\":\"{suite}:{name}\"},"
        "{\"language\":\"cuda\",\"role\":\"case\",\"define_macro\":\"CUDA_CASE\",\"name_args\":[0],"
        "\"runner_id\":\"{suite}:{name}\"}]}}}";
    cbm_test_model_t *model = descriptor_model(json);
    ASSERT_NOT_NULL(model);
    const char *source = "C_CASE(c_one) {} CPP_CASE(cpp_one) {} CUDA_CASE(cuda_one) {}";
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "one.h", "C", source, strlen(source)));
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "two.h", "C++", source, strlen(source)));
    ASSERT_TRUE(
        cbm_test_model_add_source_language(model, "three.h", "CUDA", source, strlen(source)));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_TRUE(cbm_test_model_complete(model));
    int n = 0;
    const cbm_test_case_t *cases = cbm_test_model_cases(model, &n);
    ASSERT_EQ(n, 3);
    ASSERT_STR_EQ(cases[0].file, "one.h");
    ASSERT_STR_EQ(cases[0].name, "c_one");
    ASSERT_STR_EQ(cases[1].file, "three.h");
    ASSERT_STR_EQ(cases[1].name, "cuda_one");
    ASSERT_STR_EQ(cases[2].file, "two.h");
    ASSERT_STR_EQ(cases[2].name, "cpp_one");
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_descriptors_keep_unrenderable_names_and_templates_explicit) {
    const char *payloads[] = {
        "\"language\":\"c\",\"name_args\":[1,0,1],\"runner_id\":\"{suite}:{name}\"",
        "\"language\":\"c\",\"name_args\":[0],\"runner_id\":\"opaque({unknown})\"",
        "\"language\":\"c\",\"name_args\":[0]",
        "\"language\":\"unknown-lang\",\"name_args\":[0],\"runner_id\":\"{suite}:{name}\""};
    cbm_test_model_mapping_status_t expected[] = {
        CBM_TEST_MODEL_MAPPING_UNSUPPORTED_NAME, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_TEMPLATE,
        CBM_TEST_MODEL_MAPPING_UNSUPPORTED_TEMPLATE, CBM_TEST_MODEL_MAPPING_UNSUPPORTED_LANGUAGE};
    for (size_t i = 0; i < sizeof(payloads) / sizeof(payloads[0]); i++) {
        char json[1024];
        snprintf(json, sizeof(json),
                 "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[{\"role\":\"case\","
                 "\"define_macro\":\"CASE\",%s}]}}}",
                 payloads[i]);
        cbm_test_model_t *model = descriptor_model(json);
        ASSERT_NOT_NULL(model);
        ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/config.c", "c", "", 0));
        cbm_test_model_mapping_issue_t issue = {0};
        ASSERT_EQ(cbm_test_model_mapping_status(model, &issue), expected[i]);
        ASSERT_EQ(issue.reason, expected[i]);
        ASSERT_EQ(issue.declaration_index, 0);
        int n = 0;
        const cbm_test_declaration_t *d = cbm_test_model_declarations(model, &n);
        ASSERT_EQ(n, 1);
        if (i == 0) {
            ASSERT_EQ(d[0].name_arg_count, 3);
            ASSERT_EQ(d[0].name_args[0], 1);
            ASSERT_EQ(d[0].name_args[1], 0);
            ASSERT_EQ(d[0].name_args[2], 1);
        }
        if (i == 1)
            ASSERT_STR_EQ(d[0].runner_id, "opaque({unknown})");
        if (i == 2)
            ASSERT_NULL(d[0].runner_id);
        ASSERT_TRUE(cbm_test_model_finish(model));
        ASSERT_FALSE(cbm_test_model_complete(model));
        cbm_test_model_free(model);
    }
    PASS();
}

TEST(test_model_descriptors_duplicates_are_idempotent_and_conflicts_never_narrow) {
    for (int conflict = 0; conflict <= 1; conflict++)
        for (int reverse = 0; reverse <= 1; reverse++) {
            int a = reverse ? conflict : 0, b = reverse ? 0 : conflict;
            char json[2048];
            snprintf(json, sizeof(json),
                     "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":["
                     "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CASE\",\"name_"
                     "args\":[%d],\"runner_id\":\"{suite}:{name}\"},"
                     "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CASE\",\"name_"
                     "args\":[%d],\"runner_id\":\"{suite}:{name}\"}]}}}",
                     a, b);
            cbm_test_model_t *model = descriptor_model(json);
            ASSERT_NOT_NULL(model);
            const char *source = "CASE(one,two) {}";
            ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/custom.c", "c", source,
                                                           strlen(source)));
            ASSERT_TRUE(cbm_test_model_finish(model));
            ASSERT_EQ(cbm_test_model_complete(model), conflict == 0);
            ASSERT_EQ(cbm_test_model_mapping_status(model, NULL),
                      conflict ? CBM_TEST_MODEL_MAPPING_AMBIGUOUS : CBM_TEST_MODEL_MAPPING_OK);
            int n = 0;
            cbm_test_model_declarations(model, &n);
            ASSERT_EQ(n, 2);
            if (!conflict)
                ASSERT_EQ(count_cases(model), 1);
            cbm_test_model_free(model);
        }
    PASS();
}

TEST(test_model_descriptors_argument_failures_have_owned_source_diagnostics) {
    const char *sources[] = {"CASE_AT(one) {}", "CASE_AT(ignore,make_name()) {}",
                             "CASE_AT(ignore,unterminated"};
    cbm_test_model_mapping_status_t expected[] = {CBM_TEST_MODEL_MAPPING_MISSING_ARGUMENT,
                                                  CBM_TEST_MODEL_MAPPING_UNSUPPORTED_ARGUMENT,
                                                  CBM_TEST_MODEL_MAPPING_MALFORMED_INVOCATION};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        cbm_test_model_t *model = descriptor_model(descriptor_config);
        ASSERT_NOT_NULL(model);
        char file[] = "tests/malformed.c";
        ASSERT_TRUE(
            cbm_test_model_add_source_language(model, file, "c", sources[i], strlen(sources[i])));
        memset(file, 'x', sizeof(file) - 1);
        ASSERT_TRUE(cbm_test_model_finish(model));
        ASSERT_FALSE(cbm_test_model_complete(model));
        cbm_test_model_mapping_issue_t issue = {0};
        ASSERT_EQ(cbm_test_model_mapping_status(model, &issue), expected[i]);
        ASSERT_STR_EQ(issue.file, "tests/malformed.c");
        ASSERT_EQ(issue.line, 1);
        ASSERT_EQ(issue.declaration_index, 0);
        cbm_test_model_free(model);
    }
    PASS();
}

/* A function-like macro name that is not followed by '(' is not an invocation:
 * the preprocessor leaves it alone, so it can define or register nothing. An
 * array or variable that happens to carry a convention's name (cbm itself has
 * `static const char *const TEST[]` in src/pipeline/pass_semantic_edges.c)
 * must not make the whole registry uncertain. An opened '(' that never closes
 * still does. */
TEST(test_model_descriptors_plain_identifier_with_a_macro_name_is_no_invocation) {
    cbm_test_model_t *model = descriptor_model(descriptor_config);
    ASSERT_NOT_NULL(model);
    const char *source = "static const char *const CASE_AT[] = {\"test\", NULL};\n"
                         "int USE_AT = 3;\n"
                         "void *p = &ENTER_AT;\n"
                         "OTHER_CASE(real) {}\n";
    ASSERT_TRUE(
        cbm_test_model_add_source_language(model, "src/not_a_test.c", "c", source, strlen(source)));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_TRUE(cbm_test_model_complete(model));
    ASSERT_EQ(cbm_test_model_mapping_status(model, NULL), CBM_TEST_MODEL_MAPPING_OK);
    ASSERT_EQ(count_cases(model), 1);
    cbm_test_model_free(model);

    model = descriptor_model(descriptor_config);
    ASSERT_NOT_NULL(model);
    const char *open = "CASE_AT(ignore,";
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/open.c", "c", open, strlen(open)));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_FALSE(cbm_test_model_complete(model));
    ASSERT_EQ(cbm_test_model_mapping_status(model, NULL),
              CBM_TEST_MODEL_MAPPING_MALFORMED_INVOCATION);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_descriptors_registration_links_stay_in_their_source_file) {
    cbm_test_model_t *model = descriptor_model(descriptor_config);
    ASSERT_NOT_NULL(model);
    const char *a = "OTHER_CASE(same) {} GROUP_AT(ignore,alpha) { USE_AT(ignore,same); }";
    const char *b = "GROUP_AT(ignore,beta) { USE_AT(ignore,same); }";
    const char *c = "OTHER_CASE(same) {} GROUP_AT(ignore,gamma) { USE_AT(ignore,same); }";
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/b.c", "c", b, strlen(b)));
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/a.c", "c", a, strlen(a)));
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/c.c", "c", c, strlen(c)));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_TRUE(cbm_test_model_complete(model));
    int n = 0;
    const cbm_test_registration_t *regs = cbm_test_model_registrations(model, &n);
    ASSERT_EQ(n, 3);
    ASSERT_EQ(count_cases(model), 2);
    ASSERT_STR_EQ(regs[0].file, "tests/a.c");
    ASSERT_TRUE(regs[0].resolved);
    ASSERT_STR_EQ(regs[1].file, "tests/b.c");
    ASSERT_FALSE(regs[1].resolved);
    ASSERT_STR_EQ(regs[2].file, "tests/c.c");
    ASSERT_TRUE(regs[2].resolved);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_descriptors_presets_are_explicit_and_native_gaps_are_visible) {
    const char *configs[] = {
        "{\"test_impact\":{\"version\":1}}",
        "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"c-cbm\":true}}}}"};
    for (int enabled = 0; enabled <= 1; enabled++) {
        cbm_test_model_t *model = descriptor_model(configs[enabled]);
        ASSERT_NOT_NULL(model);
        ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/cases.c", "c",
                                                       selection_source, strlen(selection_source)));
        ASSERT_TRUE(cbm_test_model_finish(model));
        ASSERT_TRUE(cbm_test_model_complete(model));
        ASSERT_EQ(count_cases(model), enabled ? 4 : 0);
        ASSERT_EQ(count_suites(model), enabled ? 1 : 0);
        cbm_test_model_free(model);
    }
    cbm_test_model_t *model = descriptor_model(configs[0]);
    ASSERT_NOT_NULL(model);
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/test.cpp", "cpp", "", 0));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_FALSE(cbm_test_model_complete(model));
    ASSERT_EQ(cbm_test_model_mapping_status(model, NULL),
              CBM_TEST_MODEL_MAPPING_UNSUPPORTED_PRESET);
    cbm_test_model_free(model);
    model = descriptor_model(configs[0]);
    ASSERT_NOT_NULL(model);
    ASSERT_TRUE(cbm_test_model_add_source_language(model, "ambiguous.h", "unknown", "", 0));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_FALSE(cbm_test_model_complete(model));
    ASSERT_EQ(cbm_test_model_mapping_status(model, NULL),
              CBM_TEST_MODEL_MAPPING_UNSUPPORTED_LANGUAGE);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_descriptors_hidden_configured_macros_cannot_certify_inventory) {
    const char *sources[] = {"#define HIDDEN(name) CASE_AT(ignore,name) {}\n",
                             "#define HIDDEN(name) ENTER_AT(ignore,name)\n",
                             "void helper(void) { CASE_AT(ignore,nested) {} }\n"};
    for (size_t i = 0; i < sizeof(sources) / sizeof(sources[0]); i++) {
        cbm_test_model_t *model = descriptor_model(descriptor_config);
        ASSERT_NOT_NULL(model);
        ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/hidden.c", "c", sources[i],
                                                       strlen(sources[i])));
        ASSERT_TRUE(cbm_test_model_finish(model));
        ASSERT_FALSE(cbm_test_model_complete(model));
        cbm_test_model_free(model);
    }
    PASS();
}

TEST(test_model_descriptors_literals_are_not_identifier_names) {
    const char *arguments[] = {"L\"title\"",      "u8\"title\"", "name \"suffix\"",
                               "\"prefix\" name", "'x'",         "\"a,b\""};
    for (size_t i = 0; i < sizeof(arguments) / sizeof(arguments[0]); i++) {
        cbm_test_model_t *model = descriptor_model(descriptor_config);
        ASSERT_NOT_NULL(model);
        char source[256];
        snprintf(source, sizeof(source), "CASE_AT(ignore,%s) {}", arguments[i]);
        ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/literals.c", "c", source,
                                                       strlen(source)));
        ASSERT_TRUE(cbm_test_model_finish(model));
        ASSERT_FALSE(cbm_test_model_complete(model));
        ASSERT_EQ(cbm_test_model_mapping_status(model, NULL),
                  CBM_TEST_MODEL_MAPPING_UNSUPPORTED_ARGUMENT);
        cbm_test_model_free(model);
    }
    PASS();
}

TEST(test_model_descriptors_large_argument_index_does_not_size_an_allocation) {
    const char *json = "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":["
                       "{\"language\":\"c\",\"role\":\"case\",\"define_macro\":\"CASE\",\"name_"
                       "args\":[2147483647],\"runner_id\":\"{suite}:{name}\"}]}}}";
    cbm_test_model_t *model = descriptor_model(json);
    ASSERT_NOT_NULL(model);
    size_t before = cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA);
    const char *source = "CASE(only) {}";
    ASSERT_TRUE(
        cbm_test_model_add_source_language(model, "tests/index.c", "c", source, strlen(source)));
    ASSERT_TRUE(cbm_test_model_finish(model));
    ASSERT_FALSE(cbm_test_model_complete(model));
    ASSERT_EQ(cbm_test_model_mapping_status(model, NULL), CBM_TEST_MODEL_MAPPING_MISSING_ARGUMENT);
    ASSERT_TRUE(cbm_mem_class_live_bytes(CBM_MEM_CLASS_ARENA) - before < 65536);
    cbm_test_model_free(model);
    PASS();
}

TEST(test_model_descriptors_same_site_perf_conflicts_are_ambiguous) {
    const char *normal = "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"ENTER\","
                         "\"suite_arg\":0,\"perf_macro\":\"PERF\"}";
    const char *perf = "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"OTHER\","
                       "\"suite_arg\":0,\"perf_macro\":\"ENTER\"}";
    const char *same = "{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":\"ENTER\","
                       "\"suite_arg\":0,\"perf_macro\":\"ENTER\"}";
    for (int variant = 0; variant < 4; variant++) {
        char json[2048];
        if (variant == 3) {
            snprintf(
                json, sizeof(json),
                "{\"test_impact\":{\"version\":1,\"tests\":{\"presets\":{\"c-cbm\":true},"
                "\"conventions\":[{\"language\":\"c\",\"role\":\"suite_registration\",\"macro\":"
                "\"OTHER\",\"suite_arg\":0,\"perf_macro\":\"RUN_SELECTED_SUITE\"}]}}}");
        } else if (variant == 2)
            snprintf(json, sizeof(json),
                     "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[%s]}}}", same);
        else
            snprintf(json, sizeof(json),
                     "{\"test_impact\":{\"version\":1,\"tests\":{\"conventions\":[%s,%s]}}}",
                     variant ? perf : normal, variant ? normal : perf);
        cbm_test_model_t *model = descriptor_model(json);
        ASSERT_NOT_NULL(model);
        const char *source = variant == 3 ? "void main(void) { RUN_SELECTED_SUITE(alpha); }"
                                          : "void main(void) { ENTER(alpha); }";
        ASSERT_TRUE(cbm_test_model_add_source_language(model, "tests/runner.c", "c", source,
                                                       strlen(source)));
        ASSERT_TRUE(cbm_test_model_finish(model));
        bool complete = cbm_test_model_complete(model);
        cbm_test_model_mapping_status_t status = cbm_test_model_mapping_status(model, NULL);
        cbm_test_model_free(model);
        ASSERT_FALSE(complete);
        ASSERT_EQ(status, CBM_TEST_MODEL_MAPPING_AMBIGUOUS);
    }
    PASS();
}

SUITE(test_impact) {
    RUN_TEST(test_model_descriptors_same_site_perf_conflicts_are_ambiguous);
    RUN_TEST(test_model_descriptors_preserve_owned_records_and_multiple_spellings);
    RUN_TEST(test_model_descriptors_apply_only_to_explicit_source_language);
    RUN_TEST(test_model_descriptors_keep_unrenderable_names_and_templates_explicit);
    RUN_TEST(test_model_descriptors_duplicates_are_idempotent_and_conflicts_never_narrow);
    RUN_TEST(test_model_descriptors_argument_failures_have_owned_source_diagnostics);
    RUN_TEST(test_model_descriptors_plain_identifier_with_a_macro_name_is_no_invocation);
    RUN_TEST(test_model_descriptors_registration_links_stay_in_their_source_file);
    RUN_TEST(test_model_descriptors_presets_are_explicit_and_native_gaps_are_visible);
    RUN_TEST(test_model_descriptors_hidden_configured_macros_cannot_certify_inventory);
    RUN_TEST(test_model_descriptors_literals_are_not_identifier_names);
    RUN_TEST(test_model_descriptors_large_argument_index_does_not_size_an_allocation);

    RUN_TEST(test_model_completeness_requires_finished_success_and_stays_uncertain);
    RUN_TEST(test_model_reports_truncated_and_unsupported_syntax);
    RUN_TEST(test_model_complete_sources_keep_normal_declarations_and_literals);
    RUN_TEST(test_model_conditional_limit_is_explicit_and_deterministic);
    RUN_TEST(test_selection_cannot_override_partial_model_with_complete_input_flag);
    RUN_TEST(test_model_split_identifiers_cannot_hide_registrations);
    RUN_TEST(test_model_guard_like_conditions_cannot_certify_unconditional_cases);
    RUN_TEST(test_model_define_if_undefined_is_an_ordinary_condition);
    RUN_TEST(test_model_empty_elif_is_not_certified);
    RUN_TEST(test_model_null_empty_source_is_safe);

    RUN_TEST(test_declarations_defaults_preserve_absent_and_configured_identity);
    RUN_TEST(test_declarations_preserve_all_roles_arguments_and_owned_strings);
    RUN_TEST(test_declarations_independent_snapshots_match_loaded_config_digest);
    RUN_TEST(test_declarations_invalid_consumed_structure_fails_without_partial_result);
    RUN_TEST(test_declarations_invalid_role_payloads_are_not_ignored);
    RUN_TEST(test_declarations_exact_length_limits_and_safe_accessors);
    RUN_TEST(test_declarations_argument_boundaries_and_order_are_preserved);

    RUN_TEST(test_policy_embedded_stars_do_not_cross_or_remove_separators);
    RUN_TEST(test_policy_retains_exact_snapshot_and_owns_result);
    RUN_TEST(test_policy_defaults_and_first_match_before_target_resolution);
    RUN_TEST(test_policy_coverage_compatibility_paths_are_strict);
    RUN_TEST(test_policy_config_change_runs_all);
    RUN_TEST(test_policy_lanes_and_semantic_perf_membership);
    RUN_TEST(test_policy_globs_braces_escapes_and_directory_boundaries);
    RUN_TEST(test_policy_rejects_invalid_consumed_fields_without_changing_seed_loader);
    RUN_TEST(test_policy_expansion_limits_reject_whole_policy);
    RUN_TEST(test_policy_evaluation_errors_clear_outputs_and_never_fall_through);

    RUN_TEST(test_coverage_receipt_accepts_bound_evidence_at_age_boundary);
    RUN_TEST(test_coverage_receipt_requires_bound_source_ancestry_and_graph);
    RUN_TEST(test_coverage_receipt_binds_exact_bytes_and_metadata_consistency);
    RUN_TEST(test_coverage_receipt_rejects_stale_future_and_missing_age);
    RUN_TEST(test_coverage_receipt_missing_or_malformed_evidence_fails_closed);

    RUN_TEST(test_selection_suite_body_and_rule_triggers_dominate_cases);
    RUN_TEST(test_selection_ambiguous_source_identity_cannot_choose_first_match);
    RUN_TEST(test_selection_unions_static_coverage_and_changed_tests);
    RUN_TEST(test_selection_retains_missing_and_incomplete_test_evidence);
    RUN_TEST(test_selection_setup_and_rejected_artifacts_select_whole_suites);
    RUN_TEST(test_selection_incomplete_global_evidence_never_narrows);
    RUN_TEST(test_selection_inventory_ambiguity_and_conditionals_use_whole_suite);
    RUN_TEST(test_model_uncertainty_is_scoped_to_the_suites_of_its_file);
    RUN_TEST(test_model_only_code_continuations_are_uncertain);
    RUN_TEST(test_selection_scoped_uncertainty_runs_only_its_suite_whole);
    RUN_TEST(test_result_scoped_uncertainty_is_not_a_global_run_all);
    RUN_TEST(test_selection_unregistered_changes_run_all_and_undefined_runner_suites_run_whole);
    RUN_TEST(test_selection_other_runners_suites_are_outside_the_selection);
    RUN_TEST(test_selection_invalid_evidence_cannot_produce_a_partial_result);
    RUN_TEST(test_selection_is_deterministic_deduplicated_and_owns_strings);
    RUN_TEST(test_selection_proven_empty_diff_is_distinct_from_unresolved_changes);

    RUN_TEST(test_test_config_directory_globs_match_descendants);
    RUN_TEST(test_test_config_loads_globs_and_optional_absence);
    RUN_TEST(test_test_config_rejects_malformed_consumed_fields);
    RUN_TEST(test_coverage_metadata_matches_rows_and_commit);
    RUN_TEST(test_coverage_metadata_accepts_builder_variations);
    RUN_TEST(test_coverage_metadata_rejects_inconsistent_artifacts);
    RUN_TEST(test_coverage_map_reads_sources_setup_and_incomplete_tests);
    RUN_TEST(test_coverage_map_reads_exact_lengths_and_crlf);
    RUN_TEST(test_coverage_map_rejects_inconsistent_artifacts);
    RUN_TEST(test_diff_git_flags_ignore_inter_hunk_config);
    RUN_TEST(test_diff_rejects_impossible_counts_without_large_allocations);
    RUN_TEST(test_diff_reads_files_and_hunks);
    RUN_TEST(test_diff_reads_removals_creations_and_deletions);
    RUN_TEST(test_diff_content_that_looks_like_structure_stays_content);
    RUN_TEST(test_diff_reads_binary_mode_only_and_awkward_paths);
    RUN_TEST(test_diff_says_when_it_did_not_read_everything);
    RUN_TEST(test_model_reads_cases_suites_and_registrations);
    RUN_TEST(test_model_ignores_comments_and_literals);
    RUN_TEST(test_model_reads_suites_written_as_functions);
    RUN_TEST(test_model_marks_what_is_under_a_condition);
    RUN_TEST(test_model_keeps_brace_depth_across_else_branches);
    RUN_TEST(test_model_resolves_registrations_in_their_own_file);
    RUN_TEST(test_model_marks_suites_that_register_through_a_macro);
    RUN_TEST(test_model_reads_the_runner_suites);
    RUN_TEST(test_model_order_is_independent_of_input_order);
    RUN_TEST(test_model_survives_truncated_sources);
}
