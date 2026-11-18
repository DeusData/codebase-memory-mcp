/*
 * test_scope_closure.c -- the unordered closure and lookup of the shared
 * cross-file layer (src/pipeline/scope_closure.h), on scopes built by hand.
 */
#include "test_framework.h"
#include "../src/pipeline/scope_closure.h"

#include <stdio.h>
#include <string.h>

static const cbm_closure_lang_t ROW = {.lang = CBM_LANG_PROTOBUF};
static const cbm_closure_lang_t ROW_QUAL = {.lang = CBM_LANG_PROTOBUF, .qualified_refs = true};
static const cbm_closure_lang_t ROW_PKG = {.lang = CBM_LANG_GO, .package_visible = true};
static const cbm_closure_lang_t ROW_PRELUDE = {.lang = CBM_LANG_SCALA, .prelude_external = true};
static const cbm_closure_lang_t ROW_STRICT = {.lang = CBM_LANG_SCALA, .ambiguity_is_error = true};

static cbm_closure_answer_t sc_ask(cbm_closure_t *s, int file, const char *name, const char *qual,
                                   uint8_t flags) {
    cbm_closure_ref_t ref = {.name = name, .qual = qual, .flags = flags};
    return cbm_closure_lookup(s, file, &ref);
}

/* user includes lib, which defines T: the one T, proven */
TEST(scope_closure_one_definition_through_an_include_is_proven) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "user.proto", &ROW, "u");
    int lib = cbm_closure_add_file(s, "lib.proto", &ROW, "l");
    ASSERT_TRUE(cbm_closure_add_def(s, lib, "T", "l.T", "p.lib.T", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, lib, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    cbm_closure_answer_t a = sc_ask(s, user, "T", NULL, 0);
    ASSERT_EQ(a.verdict, CBM_CLOSURE_PROVEN);
    ASSERT_STR_EQ(a.qn, "p.lib.T");
    ASSERT_EQ(a.file, lib);
    ASSERT_EQ(a.visible, 1);
    ASSERT_TRUE(a.complete);
    ASSERT_EQ(a.via, CBM_CLOSURE_VIA_INCLUDE);
    /* a name nobody defines, closure complete: proven absent (counted only) */
    ASSERT_EQ(sc_ask(s, user, "Nope", NULL, 0).verdict, CBM_CLOSURE_PROVEN_ABSENT);
    /* bound locally: no answer */
    cbm_closure_answer_t local = sc_ask(s, user, "T", NULL, CBM_CLOSURE_REF_LOCAL);
    ASSERT_EQ(local.verdict, CBM_CLOSURE_NO_ANSWER);
    ASSERT_EQ(local.why, CBM_CLOSURE_WHY_LOCAL);
    cbm_closure_free(s);
    PASS();
}

/* a plain include is not passed on; a re-exporting one is, through a cycle */
TEST(scope_closure_reexport_chain_and_cycle) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "user.proto", &ROW, "");
    int mid = cbm_closure_add_file(s, "mid.proto", &ROW, "");
    int deep = cbm_closure_add_file(s, "deep.proto", &ROW, "");
    int a = cbm_closure_add_file(s, "a.proto", &ROW, "");
    int b = cbm_closure_add_file(s, "b.proto", &ROW, "");
    ASSERT_TRUE(cbm_closure_add_def(s, deep, "D", NULL, "q.D", false));
    ASSERT_TRUE(cbm_closure_add_def(s, b, "B", NULL, "q.B", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, mid, false));
    ASSERT_TRUE(cbm_closure_add_include(s, mid, deep, false)); /* plain: not passed on */
    ASSERT_TRUE(cbm_closure_add_include(s, user, a, false));
    ASSERT_TRUE(cbm_closure_add_include(s, a, b, true)); /* public, and back: one component */
    ASSERT_TRUE(cbm_closure_add_include(s, b, a, true));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    ASSERT_EQ(sc_ask(s, user, "D", NULL, 0).verdict, CBM_CLOSURE_PROVEN_ABSENT);
    ASSERT_EQ(sc_ask(s, mid, "D", NULL, 0).verdict, CBM_CLOSURE_PROVEN);
    cbm_closure_answer_t through = sc_ask(s, user, "B", NULL, 0);
    ASSERT_EQ(through.verdict, CBM_CLOSURE_PROVEN);
    ASSERT_STR_EQ(through.qn, "q.B");
    ASSERT_EQ(sc_ask(s, a, "B", NULL, 0).verdict, CBM_CLOSURE_PROVEN);
    cbm_closure_free(s);
    PASS();
}

/* incomplete closures: an unresolved include, an opaque file on the way */
TEST(scope_closure_incomplete_is_possible_or_no_answer) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "user.proto", &ROW, "");
    int lib = cbm_closure_add_file(s, "lib.proto", &ROW, "");
    int dark = cbm_closure_add_file(s, "dark.proto", &ROW, "");
    int user2 = cbm_closure_add_file(s, "user2.proto", &ROW, "");
    ASSERT_TRUE(cbm_closure_add_def(s, lib, "T", NULL, "q.T", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, lib, false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, -1, false)); /* names no repository file */
    ASSERT_TRUE(cbm_closure_add_def(s, dark, "U", NULL, "q.U", false));
    ASSERT_TRUE(cbm_closure_add_opaque(s, dark));
    ASSERT_TRUE(cbm_closure_add_include(s, user2, dark, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    cbm_closure_answer_t t = sc_ask(s, user, "T", NULL, 0);
    ASSERT_EQ(t.verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_STR_EQ(t.qn, "q.T");
    ASSERT_FALSE(t.complete);
    ASSERT_EQ(t.why & CBM_CLOSURE_WHY_UNRESOLVED, CBM_CLOSURE_WHY_UNRESOLVED);
    ASSERT_EQ(sc_ask(s, user, "Gone", NULL, 0).verdict, CBM_CLOSURE_NO_ANSWER);
    cbm_closure_answer_t u = sc_ask(s, user2, "U", NULL, 0);
    ASSERT_EQ(u.verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_EQ(u.why & CBM_CLOSURE_WHY_OPAQUE, CBM_CLOSURE_WHY_OPAQUE);
    cbm_closure_free(s);
    PASS();
}

/* two visible definitions: no answer; a conditional one: possible; one
 * without a node: no answer */
TEST(scope_closure_ambiguous_conditional_and_nodeless) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "user.proto", &ROW, "");
    int x = cbm_closure_add_file(s, "x.proto", &ROW, "");
    int y = cbm_closure_add_file(s, "y.proto", &ROW, "");
    ASSERT_TRUE(cbm_closure_add_def(s, x, "T", NULL, "q.x.T", false));
    ASSERT_TRUE(cbm_closure_add_def(s, y, "T", NULL, "q.y.T", false));
    ASSERT_TRUE(cbm_closure_add_def(s, x, "C", NULL, "q.x.C", true));
    ASSERT_TRUE(cbm_closure_add_def(s, y, "N", NULL, NULL, false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, x, false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, y, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    cbm_closure_answer_t two = sc_ask(s, user, "T", NULL, 0);
    ASSERT_EQ(two.verdict, CBM_CLOSURE_NO_ANSWER);
    ASSERT_EQ(two.visible, 2);
    ASSERT_EQ(two.why & CBM_CLOSURE_WHY_AMBIGUOUS, CBM_CLOSURE_WHY_AMBIGUOUS);
    cbm_closure_answer_t cond = sc_ask(s, user, "C", NULL, 0);
    ASSERT_EQ(cond.verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_EQ(cond.why & CBM_CLOSURE_WHY_CONDITIONAL, CBM_CLOSURE_WHY_CONDITIONAL);
    cbm_closure_answer_t none = sc_ask(s, user, "N", NULL, 0);
    ASSERT_EQ(none.verdict, CBM_CLOSURE_NO_ANSWER);
    ASSERT_EQ(none.why & CBM_CLOSURE_WHY_NO_NODE, CBM_CLOSURE_WHY_NO_NODE);
    cbm_closure_free(s);
    PASS();
}

/* a qualified reference read from the package outward picks one of two */
TEST(scope_closure_qualified_reference) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "a/b/user.proto", &ROW_QUAL, "a.b");
    int common = cbm_closure_add_file(s, "common.proto", &ROW_QUAL, "common");
    int y = cbm_closure_add_file(s, "y.proto", &ROW_QUAL, "y");
    int near = cbm_closure_add_file(s, "a/n.proto", &ROW_QUAL, "a.n");
    ASSERT_TRUE(
        cbm_closure_add_def(s, common, "Status", "common.Status", "q.common.Status", false));
    ASSERT_TRUE(cbm_closure_add_def(s, y, "Status", "y.Status", "q.y.Status", false));
    ASSERT_TRUE(cbm_closure_add_def(s, near, "Kind", "a.n.Kind", "q.a.n.Kind", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, common, false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, y, false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, near, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    cbm_closure_answer_t c = sc_ask(s, user, "Status", "common", 0);
    ASSERT_EQ(c.verdict, CBM_CLOSURE_PROVEN);
    ASSERT_STR_EQ(c.qn, "q.common.Status");
    ASSERT_STR_EQ(sc_ask(s, user, "Status", ".y", 0).qn, "q.y.Status");
    ASSERT_EQ(sc_ask(s, user, "Status", NULL, 0).verdict, CBM_CLOSURE_NO_ANSWER); /* bare: two */
    /* `n.Kind` inside package a.b: a.b.n.Kind, then a.n.Kind */
    ASSERT_STR_EQ(sc_ask(s, user, "Kind", "n", 0).qn, "q.a.n.Kind");
    cbm_closure_free(s);
    PASS();
}

/* rows: package mates, a prelude, ambiguity as an error */
TEST(scope_closure_rows) {
    cbm_closure_t *s = cbm_closure_new();
    int p1 = cbm_closure_add_file(s, "pkg/a.go", &ROW_PKG, "pkg");
    int p2 = cbm_closure_add_file(s, "pkg/b.go", &ROW_PKG, "pkg");
    int other = cbm_closure_add_file(s, "other/c.go", &ROW_PKG, "other");
    ASSERT_TRUE(cbm_closure_add_def(s, p2, "F", NULL, "q.pkg.F", false));
    int pre = cbm_closure_add_file(s, "s/a.scala", &ROW_PRELUDE, "s");
    int pre_lib = cbm_closure_add_file(s, "s/l.scala", &ROW_PRELUDE, "s");
    ASSERT_TRUE(cbm_closure_add_def(s, pre_lib, "G", NULL, "q.s.G", false));
    ASSERT_TRUE(cbm_closure_add_include(s, pre, pre_lib, false));
    int strict = cbm_closure_add_file(s, "t/a.scala", &ROW_STRICT, "t");
    int strict_lib = cbm_closure_add_file(s, "t/l.scala", &ROW_STRICT, "t");
    ASSERT_TRUE(cbm_closure_add_def(s, strict_lib, "H", NULL, "q.t.H", false));
    ASSERT_TRUE(cbm_closure_add_include(s, strict, strict_lib, false));
    ASSERT_TRUE(cbm_closure_add_include(s, strict, -1, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    cbm_closure_answer_t mate = sc_ask(s, p1, "F", NULL, 0);
    ASSERT_EQ(mate.verdict, CBM_CLOSURE_PROVEN);
    ASSERT_EQ(mate.via, CBM_CLOSURE_VIA_PACKAGE);
    ASSERT_EQ(sc_ask(s, other, "F", NULL, 0).verdict, CBM_CLOSURE_PROVEN_ABSENT);
    cbm_closure_answer_t prelude = sc_ask(s, pre, "G", NULL, 0);
    ASSERT_EQ(prelude.verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_EQ(prelude.why & CBM_CLOSURE_WHY_PRELUDE, CBM_CLOSURE_WHY_PRELUDE);
    ASSERT_EQ(sc_ask(s, pre, "Absent", NULL, 0).verdict, CBM_CLOSURE_NO_ANSWER);
    cbm_closure_answer_t one = sc_ask(s, strict, "H", NULL, 0);
    ASSERT_EQ(one.verdict, CBM_CLOSURE_PROVEN);
    ASSERT_FALSE(one.complete);
    cbm_closure_free(s);
    PASS();
}

/* Counted work of a header-heavy shape at n: (fan) one file defining N
 * included by n files, each asking N and one undefined name; (chain) n files
 * each re-exporting the next, the last defining N, each asking N. */
static uint64_t sc_work(int n, bool chain) {
    cbm_closure_t *s = cbm_closure_new();
    if (!s) {
        return 0;
    }
    char rel[32];
    int shared = cbm_closure_add_file(s, "shared.proto", &ROW, "");
    int first = -1;
    for (int i = 0; i < n; i++) {
        snprintf(rel, sizeof(rel), "f%d.proto", i);
        int f = cbm_closure_add_file(s, rel, &ROW, "");
        first = first < 0 ? f : first;
    }
    for (int i = 0; i < n; i++) { /* every file exists before an include names it */
        int f = first + i;
        bool ok = chain ? cbm_closure_add_include(s, f, i + 1 < n ? f + 1 : shared, true)
                        : cbm_closure_add_include(s, f, shared, false);
        if (!ok) {
            cbm_closure_free(s);
            return 0;
        }
    }
    if (!cbm_closure_add_def(s, shared, "N", NULL, "q.N", false) || cbm_closure_seal(s) != 0) {
        cbm_closure_free(s);
        return 0;
    }
    bool right = true;
    for (int i = 0; i < n; i++) {
        right = right && sc_ask(s, first + i, "N", NULL, 0).verdict == CBM_CLOSURE_PROVEN;
        right =
            right && sc_ask(s, first + i, "Unknown", NULL, 0).verdict == CBM_CLOSURE_PROVEN_ABSENT;
    }
    uint64_t w = cbm_closure_work(s);
    cbm_closure_free(s);
    return right ? w : 0;
}

TEST(scope_closure_work_grows_linearly) {
    for (int chain = 0; chain < 2; chain++) {
        uint64_t w1 = sc_work(100, chain);
        uint64_t w4 = sc_work(400, chain);
        uint64_t w16 = sc_work(1600, chain);
        ASSERT_GT(w1, 0);
        ASSERT_GT(w4, 0);
        ASSERT_GT(w16, 0);
        /* at most 4.5 times the work per 4 times the files */
        ASSERT_TRUE(w4 * 2 <= w1 * 9);
        ASSERT_TRUE(w16 * 2 <= w4 * 9);
    }
    PASS();
}

SUITE(scope_closure) {
    RUN_TEST(scope_closure_one_definition_through_an_include_is_proven);
    RUN_TEST(scope_closure_reexport_chain_and_cycle);
    RUN_TEST(scope_closure_incomplete_is_possible_or_no_answer);
    RUN_TEST(scope_closure_ambiguous_conditional_and_nodeless);
    RUN_TEST(scope_closure_qualified_reference);
    RUN_TEST(scope_closure_rows);
    RUN_TEST(scope_closure_work_grows_linearly);
}
