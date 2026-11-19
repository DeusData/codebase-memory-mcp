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

/* `written`: the reference as written, NULL for the bare name. No memo: each
 * answer computed afresh (sc_ask_memo shares one). */
static cbm_closure_answer_t sc_ask(cbm_closure_t *s, int file, const char *name,
                                   const char *written, uint8_t flags) {
    cbm_closure_ref_t ref = {.name = name, .written = written, .flags = flags};
    return cbm_closure_lookup(s, NULL, file, &ref);
}

static cbm_closure_answer_t sc_ask_memo(cbm_closure_t *s, cbm_closure_memo_t *m, int file,
                                        const char *name, const char *written) {
    cbm_closure_ref_t ref = {.name = name, .written = written};
    return cbm_closure_lookup(s, m, file, &ref);
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

/* a plain include is not passed on; a re-exporting one is, along a chain;
 * an import cycle (public or plain) is a file protoc refuses ("File
 * recursively imports itself"): nothing through it or in it is proven */
TEST(scope_closure_reexport_chain_and_cycle) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "user.proto", &ROW, "");
    int mid = cbm_closure_add_file(s, "mid.proto", &ROW, "");
    int deep = cbm_closure_add_file(s, "deep.proto", &ROW, "");
    int user2 = cbm_closure_add_file(s, "user2.proto", &ROW, "");
    int a = cbm_closure_add_file(s, "a.proto", &ROW, "");
    int b = cbm_closure_add_file(s, "b.proto", &ROW, "");
    int pub = cbm_closure_add_file(s, "pub.proto", &ROW, "");
    int x = cbm_closure_add_file(s, "x.proto", &ROW, "");
    int y = cbm_closure_add_file(s, "y.proto", &ROW, "");
    ASSERT_TRUE(cbm_closure_add_def(s, deep, "D", NULL, "q.D", false));
    ASSERT_TRUE(cbm_closure_add_def(s, b, "B", NULL, "q.B", false));
    ASSERT_TRUE(cbm_closure_add_def(s, y, "Y", NULL, "q.Y", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, mid, false));
    ASSERT_TRUE(cbm_closure_add_include(s, mid, deep, false)); /* plain: not passed on */
    ASSERT_TRUE(cbm_closure_add_include(s, user, pub, false));
    ASSERT_TRUE(cbm_closure_add_include(s, pub, deep, true)); /* public: passed on */
    ASSERT_TRUE(cbm_closure_add_include(s, user2, a, false));
    ASSERT_TRUE(cbm_closure_add_include(s, a, b, true)); /* public, and back: a cycle */
    ASSERT_TRUE(cbm_closure_add_include(s, b, a, true));
    ASSERT_TRUE(cbm_closure_add_include(s, x, y, false)); /* plain, and back: a cycle too */
    ASSERT_TRUE(cbm_closure_add_include(s, y, x, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    ASSERT_EQ(sc_ask(s, mid, "D", NULL, 0).verdict, CBM_CLOSURE_PROVEN);
    cbm_closure_answer_t chain = sc_ask(s, user, "D", NULL, 0);
    ASSERT_EQ(chain.verdict, CBM_CLOSURE_PROVEN); /* through pub's re-export, not mid's */
    ASSERT_STR_EQ(chain.qn, "q.D");
    cbm_closure_t *s2 = cbm_closure_new(); /* the plain include alone */
    int u2 = cbm_closure_add_file(s2, "user.proto", &ROW, "");
    int m2 = cbm_closure_add_file(s2, "mid.proto", &ROW, "");
    int d2 = cbm_closure_add_file(s2, "deep.proto", &ROW, "");
    ASSERT_TRUE(cbm_closure_add_def(s2, d2, "D", NULL, "q.D", false));
    ASSERT_TRUE(cbm_closure_add_include(s2, u2, m2, false));
    ASSERT_TRUE(cbm_closure_add_include(s2, m2, d2, false));
    ASSERT_EQ(cbm_closure_seal(s2), 0);
    ASSERT_EQ(sc_ask(s2, u2, "D", NULL, 0).verdict, CBM_CLOSURE_PROVEN_ABSENT);
    cbm_closure_free(s2);
    /* the cycles: possible at most, opaque */
    cbm_closure_answer_t through = sc_ask(s, user2, "B", NULL, 0);
    ASSERT_EQ(through.verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_STR_EQ(through.qn, "q.B");
    ASSERT_EQ(through.why & CBM_CLOSURE_WHY_OPAQUE, CBM_CLOSURE_WHY_OPAQUE);
    ASSERT_EQ(sc_ask(s, a, "B", NULL, 0).verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_EQ(sc_ask(s, x, "Y", NULL, 0).verdict, CBM_CLOSURE_POSSIBLE);
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

/* protoc's scope walk: the innermost scope around the site in which the FIRST
 * component names a symbol decides, the rest must be there, no backtracking;
 * the enclosing message is not known, so a name a message of the file holds
 * is at most possible (protoc takes a bare nested name only inside it) */
TEST(scope_closure_qualified_reference) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "a/b/user.proto", &ROW_QUAL, "a.b");
    int common = cbm_closure_add_file(s, "common.proto", &ROW_QUAL, "common");
    int y = cbm_closure_add_file(s, "y.proto", &ROW_QUAL, "y");
    int near = cbm_closure_add_file(s, "a/n.proto", &ROW_QUAL, "a.n");
    int xc = cbm_closure_add_file(s, "x/y.proto", &ROW_QUAL, "x.common");
    ASSERT_TRUE(
        cbm_closure_add_def(s, common, "Status", "common.Status", "q.common.Status", false));
    ASSERT_TRUE(cbm_closure_add_def(s, y, "Status", "y.Status", "q.y.Status", false));
    ASSERT_TRUE(cbm_closure_add_def(s, near, "Kind", "a.n.Kind", "q.a.n.Kind", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, common, false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, y, false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, near, false));
    ASSERT_TRUE(cbm_closure_add_include(s, xc, common, false));
    /* the user's own messages, top-level and nested */
    ASSERT_TRUE(cbm_closure_add_def(s, user, "Outer", "a.b.Outer", "q.Outer", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "Inner", "a.b.Outer.Inner", "q.Outer.Inner", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "Mid", "a.b.Outer.Mid", "q.Mid", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "Deep", "a.b.Outer.Mid.Deep", "q.Deep", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "One", "a.b.One", "q.One", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "Two", "a.b.Two", "q.Two", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "Twin", "a.b.One.Twin", "q.One.Twin", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "Twin", "a.b.Two.Twin", "q.Two.Twin", false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    cbm_closure_answer_t c = sc_ask(s, user, "Status", "common.Status", 0);
    ASSERT_EQ(c.verdict, CBM_CLOSURE_PROVEN);
    ASSERT_STR_EQ(c.qn, "q.common.Status");
    ASSERT_STR_EQ(sc_ask(s, user, "Status", ".y.Status", 0).qn, "q.y.Status");
    /* bare `Status` inside package a.b reads a.b.Status, a.Status, Status:
     * neither common.Status nor y.Status */
    ASSERT_EQ(sc_ask(s, user, "Status", NULL, 0).verdict, CBM_CLOSURE_PROVEN_ABSENT);
    /* `n.Kind` inside package a.b: `n` first names the package a.n */
    ASSERT_STR_EQ(sc_ask(s, user, "Kind", "n.Kind", 0).qn, "q.a.n.Kind");
    /* `common.Status` inside package x.common: `common` first names x.common,
     * so protoc reads x.common.Status, which is not defined: no answer */
    cbm_closure_answer_t pre = sc_ask(s, xc, "Status", "common.Status", 0);
    ASSERT_TRUE(pre.verdict != CBM_CLOSURE_PROVEN && pre.verdict != CBM_CLOSURE_POSSIBLE);
    /* a message of the file holds the name: only inside it does protoc take
     * it, and the site's message is unknown: possible */
    cbm_closure_answer_t inner = sc_ask(s, user, "Inner", NULL, 0);
    ASSERT_EQ(inner.verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_STR_EQ(inner.qn, "q.Outer.Inner");
    ASSERT_EQ(inner.why & CBM_CLOSURE_WHY_SCOPE, CBM_CLOSURE_WHY_SCOPE);
    cbm_closure_answer_t deep = sc_ask(s, user, "Deep", "Mid.Deep", 0);
    ASSERT_EQ(deep.verdict, CBM_CLOSURE_POSSIBLE);
    ASSERT_STR_EQ(deep.qn, "q.Deep");
    /* from the root, or through a top-level message: proven */
    cbm_closure_answer_t lead = sc_ask(s, user, "Inner", ".a.b.Outer.Inner", 0);
    ASSERT_EQ(lead.verdict, CBM_CLOSURE_PROVEN);
    ASSERT_STR_EQ(sc_ask(s, user, "Inner", "Outer.Inner", 0).qn, "q.Outer.Inner");
    ASSERT_EQ(sc_ask(s, user, "Inner", "Outer.Inner", 0).verdict, CBM_CLOSURE_PROVEN);
    /* `Twin` from inside One is One.Twin, from inside Two is Two.Twin: the
     * enclosing message is not known here, so no answer */
    cbm_closure_answer_t twin = sc_ask(s, user, "Twin", NULL, 0);
    ASSERT_EQ(twin.verdict, CBM_CLOSURE_NO_ANSWER);
    ASSERT_EQ(twin.why & CBM_CLOSURE_WHY_AMBIGUOUS, CBM_CLOSURE_WHY_AMBIGUOUS);
    ASSERT_STR_EQ(sc_ask(s, user, "Twin", "Two.Twin", 0).qn, "q.Two.Twin");
    cbm_closure_free(s);
    PASS();
}

/* a file whose package is unknown (no fact names it) reads every scope as
 * possibly around its sites: more candidates, never fewer */
TEST(scope_closure_unknown_package_reads_every_scope) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "user.proto", &ROW_QUAL, NULL);
    int lib = cbm_closure_add_file(s, "lib.proto", &ROW_QUAL, "l");
    ASSERT_TRUE(cbm_closure_add_def(s, lib, "T", "l.T", "q.l.T", false));
    ASSERT_TRUE(cbm_closure_add_def(s, user, "T", "x.M.T", "q.x.T", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, lib, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    ASSERT_EQ(sc_ask(s, user, "T", NULL, 0).verdict, CBM_CLOSURE_NO_ANSWER); /* l.T and x.M.T */
    ASSERT_STR_EQ(sc_ask(s, user, "T", "l.T", 0).qn, "q.l.T");
    cbm_closure_free(s);
    PASS();
}

/* Answers do not depend on the memo: none, a fresh one, or one filled by
 * other lookups (in any order) give the same. */
TEST(scope_closure_answers_do_not_depend_on_the_memo) {
    cbm_closure_t *s = cbm_closure_new();
    int user = cbm_closure_add_file(s, "user.proto", &ROW_QUAL, "u");
    int pub = cbm_closure_add_file(s, "pub.proto", &ROW_QUAL, "u");
    int lib = cbm_closure_add_file(s, "lib.proto", &ROW_QUAL, "u");
    int far = cbm_closure_add_file(s, "far.proto", &ROW_QUAL, "f");
    ASSERT_TRUE(cbm_closure_add_def(s, lib, "T", "u.T", "q.u.T", false));
    ASSERT_TRUE(cbm_closure_add_def(s, far, "T", "f.T", "q.f.T", false));
    ASSERT_TRUE(cbm_closure_add_def(s, far, "V", "f.V", "q.f.V", false));
    ASSERT_TRUE(cbm_closure_add_include(s, user, pub, false));
    ASSERT_TRUE(cbm_closure_add_include(s, pub, lib, true));
    ASSERT_TRUE(cbm_closure_add_include(s, pub, far, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    const struct {
        int file;
        const char *name;
        const char *written;
    } asks[] = {{user, "T", NULL},  {pub, "T", NULL},   {user, "V", NULL},
                {pub, "V", "f.V"},  {far, "T", NULL},   {user, "T", "u.T"},
                {lib, "T", ".u.T"}, {pub, "T", ".f.T"}, {user, "Nope", NULL}};
    enum { N_ASKS = sizeof(asks) / sizeof(asks[0]) };
    cbm_closure_answer_t fresh[N_ASKS];
    for (int i = 0; i < N_ASKS; i++) {
        fresh[i] = sc_ask(s, asks[i].file, asks[i].name, asks[i].written, 0);
    }
    for (int round = 0; round < 2; round++) {
        cbm_closure_memo_t *m = cbm_closure_memo_new();
        ASSERT_NOT_NULL(m);
        for (int k = 0; k < N_ASKS; k++) {
            int i = round == 0 ? k : N_ASKS - 1 - k; /* forward, then backward */
            cbm_closure_answer_t a = sc_ask_memo(s, m, asks[i].file, asks[i].name, asks[i].written);
            ASSERT_EQ(a.verdict, fresh[i].verdict);
            ASSERT_EQ(a.visible, fresh[i].visible);
            ASSERT_EQ(a.why, fresh[i].why);
            ASSERT_EQ(a.file, fresh[i].file);
            ASSERT_TRUE((a.qn == NULL) == (fresh[i].qn == NULL));
            if (a.qn) {
                ASSERT_STR_EQ(a.qn, fresh[i].qn);
            }
        }
        cbm_closure_memo_free(m);
    }
    ASSERT_STR_EQ(fresh[0].qn, "q.u.T");                    /* through pub's re-export */
    ASSERT_STR_EQ(fresh[1].qn, "q.u.T");                    /* f.T is no bare T in package u */
    ASSERT_EQ(fresh[2].verdict, CBM_CLOSURE_PROVEN_ABSENT); /* far is not passed on */
    ASSERT_STR_EQ(fresh[3].qn, "q.f.V");
    ASSERT_EQ(cbm_closure_file(s, "pub.proto"), pub);
    ASSERT_EQ(cbm_closure_file(s, "nope.proto"), -1);
    ASSERT_EQ(cbm_closure_add_file(s, "late.proto", &ROW_QUAL, ""), -1); /* sealed */
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
 * each re-exporting the next, the last defining N, each asking N. `lookups`:
 * the lookups' work (one memo for all, as one resolver thread), else the
 * build's (files, definitions, includes, components). */
static uint64_t sc_work(int n, bool chain, bool lookups) {
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
    /* one memo, as one resolver thread has */
    cbm_closure_memo_t *m = cbm_closure_memo_new();
    bool right = m != NULL;
    for (int i = 0; right && i < n; i++) {
        right = sc_ask_memo(s, m, first + i, "N", NULL).verdict == CBM_CLOSURE_PROVEN &&
                sc_ask_memo(s, m, first + i, "Unknown", NULL).verdict == CBM_CLOSURE_PROVEN_ABSENT;
    }
    uint64_t w = lookups ? cbm_closure_memo_work(m) : cbm_closure_work(s);
    cbm_closure_memo_free(m);
    cbm_closure_free(s);
    return right ? w : 0;
}

/* Counted lookup work of review 1's three shapes at n (one memo, as one
 * resolver thread): (0, chain) f_i `import public` f_{i+1}, each defines T_i,
 * a top file includes f_0 and asks every T_i; (1, fan) a top file includes n
 * files and asks each one's T_i; (2, hub) n files `package p_i; Status`, a hub
 * re-exports all, n users each include the hub and ask p_i.Status. */
static uint64_t sc_shape_work(int n, int shape) {
    cbm_closure_t *s = cbm_closure_new();
    if (!s) {
        return 0;
    }
    char rel[32];
    char name[32];
    char full[32];
    char pkg[32];
    int top = cbm_closure_add_file(s, "top.proto", &ROW_QUAL, "u");
    int hub = cbm_closure_add_file(s, "hub.proto", &ROW_QUAL, "h");
    int first = -1;
    bool ok = true;
    for (int i = 0; ok && i < n; i++) {
        snprintf(rel, sizeof(rel), "f%d.proto", i);
        snprintf(pkg, sizeof(pkg), "p%d", i);
        int f = cbm_closure_add_file(s, rel, &ROW_QUAL, shape == 2 ? pkg : "u");
        first = first < 0 ? f : first;
        if (shape == 2) {
            snprintf(rel, sizeof(rel), "user%d.proto", i);
            ok = cbm_closure_add_file(s, rel, &ROW_QUAL, "u") >= 0;
        }
    }
    for (int i = 0; ok && i < n; i++) {
        int f = first + i * (shape == 2 ? 2 : 1);
        snprintf(name, sizeof(name), "T%d", i);
        snprintf(full, sizeof(full), shape == 2 ? "p%d.Status" : "u.T%d", i);
        ok = cbm_closure_add_def(s, f, shape == 2 ? "Status" : name, full, full, false);
        if (shape == 0) {
            ok = ok && (i + 1 >= n || cbm_closure_add_include(s, f, f + 1, true));
        } else if (shape == 1) {
            ok = ok && cbm_closure_add_include(s, top, f, false);
        } else {
            ok = ok && cbm_closure_add_include(s, hub, f, true) &&
                 cbm_closure_add_include(s, f + 1, hub, false);
        }
    }
    if (shape == 0) {
        ok = ok && cbm_closure_add_include(s, top, first, false);
    }
    ok = ok && cbm_closure_seal(s) == 0;
    cbm_closure_memo_t *m = ok ? cbm_closure_memo_new() : NULL;
    bool right = m != NULL;
    for (int i = 0; right && i < n; i++) {
        snprintf(name, sizeof(name), "T%d", i);
        snprintf(full, sizeof(full), "p%d.Status", i);
        cbm_closure_answer_t a = shape == 2 ? sc_ask_memo(s, m, first + i * 2 + 1, "Status", full)
                                            : sc_ask_memo(s, m, top, name, NULL);
        right = a.verdict == CBM_CLOSURE_PROVEN;
    }
    uint64_t w = m ? cbm_closure_memo_work(m) : 0;
    cbm_closure_memo_free(m);
    cbm_closure_free(s);
    return right ? w : 0;
}

TEST(scope_closure_review_shapes_grow_linearly) {
    static const char *const names[] = {"chain", "fan", "hub"};
    for (int shape = 0; shape < 3; shape++) {
        uint64_t w1 = sc_shape_work(100, shape);
        uint64_t w4 = sc_shape_work(400, shape);
        uint64_t w16 = sc_shape_work(1600, shape);
        printf("\n  %s lookups: %llu / %llu / %llu at 1x / 4x / 16x", names[shape],
               (unsigned long long)w1, (unsigned long long)w4, (unsigned long long)w16);
        ASSERT_GT(w1, 0);
        ASSERT_GT(w4, 0);
        ASSERT_GT(w16, 0);
        ASSERT_TRUE(w4 * 2 <= w1 * 9);
        ASSERT_TRUE(w16 * 2 <= w4 * 9);
    }
    printf("\n");
    PASS();
}

/* Counted lookup work of review 3's R2 shape at n (one memo): n files
 * `package p_i; message M { enum Type {..} Type t = 1; }`, each asking its
 * own Type. `scoped`: the reference carries its enclosing message (proven
 * p_i.M.Type); otherwise the scope is unknown and the file's own nested Type
 * may decide (possible). */
static uint64_t sc_nested_work(int n, bool scoped) {
    cbm_closure_t *s = cbm_closure_new();
    if (!s) {
        return 0;
    }
    char rel[32];
    char pkg[32];
    char msg[32];
    char full[48];
    int first = -1;
    bool ok = true;
    for (int i = 0; ok && i < n; i++) {
        snprintf(rel, sizeof(rel), "f%d.proto", i);
        snprintf(pkg, sizeof(pkg), "p%d", i);
        snprintf(msg, sizeof(msg), "p%d.M", i);
        snprintf(full, sizeof(full), "p%d.M.Type", i);
        int f = cbm_closure_add_file(s, rel, &ROW_QUAL, pkg);
        first = first < 0 ? f : first;
        ok = f >= 0 && cbm_closure_add_def(s, f, "M", msg, msg, false) &&
             cbm_closure_add_def(s, f, "Type", full, full, false);
    }
    ok = ok && cbm_closure_seal(s) == 0;
    cbm_closure_memo_t *m = ok ? cbm_closure_memo_new() : NULL;
    bool right = m != NULL;
    for (int i = 0; right && i < n; i++) {
        snprintf(msg, sizeof(msg), "p%d.M", i);
        snprintf(full, sizeof(full), "p%d.M.Type", i);
        cbm_closure_ref_t ref = {.name = "Type", .scope = scoped ? msg : NULL};
        cbm_closure_answer_t a = cbm_closure_lookup(s, m, first + i, &ref);
        right = a.verdict == (scoped ? CBM_CLOSURE_PROVEN : CBM_CLOSURE_POSSIBLE) && a.qn &&
                strcmp(a.qn, full) == 0;
    }
    uint64_t w = m ? cbm_closure_memo_work(m) : 0;
    cbm_closure_memo_free(m);
    cbm_closure_free(s);
    return right ? w : 0;
}

TEST(scope_closure_nested_name_in_every_file_grows_linearly) {
    for (int scoped = 0; scoped < 2; scoped++) {
        uint64_t w1 = sc_nested_work(250, scoped);
        uint64_t w4 = sc_nested_work(1000, scoped);
        uint64_t w16 = sc_nested_work(4000, scoped);
        printf("\n  nested %s: %llu / %llu / %llu at 250 / 1,000 / 4,000", scoped ? "scoped" : "unscoped",
               (unsigned long long)w1, (unsigned long long)w4, (unsigned long long)w16);
        ASSERT_GT(w1, 0);
        ASSERT_GT(w4, 0);
        ASSERT_GT(w16, 0);
        ASSERT_TRUE(w4 * 2 <= w1 * 9);
        ASSERT_TRUE(w16 * 2 <= w4 * 9);
    }
    printf("\n");
    PASS();
}

/* protoc on review 3's fx4/scope: `package p; import "t.proto"; message A {
 * enum Type {..} Type t = 1; } message B { Type t = 1; }` with t.proto's
 * `package p; enum Type {..}` gives A's field .p.A.Type and B's .p.Type. The
 * reference's own scope decides innermost first; an outer Type is never the
 * answer where an inner one is around the site. */
TEST(scope_closure_reference_scope_decides_innermost_first) {
    cbm_closure_t *s = cbm_closure_new();
    int a = cbm_closure_add_file(s, "a.proto", &ROW_QUAL, "p");
    int t = cbm_closure_add_file(s, "t.proto", &ROW_QUAL, "p");
    ASSERT_TRUE(cbm_closure_add_def(s, t, "Type", "p.Type", "t.Type", false));
    ASSERT_TRUE(cbm_closure_add_def(s, a, "A", "p.A", "a.A", false));
    ASSERT_TRUE(cbm_closure_add_def(s, a, "Type", "p.A.Type", "a.A.Type", false));
    ASSERT_TRUE(cbm_closure_add_def(s, a, "B", "p.B", "a.B", false));
    ASSERT_TRUE(cbm_closure_add_include(s, a, t, false));
    ASSERT_EQ(cbm_closure_seal(s), 0);
    static const struct {
        const char *scope;
        const char *written;
        const char *want; /* the proven node */
    } cases[] = {
        {"p.A", NULL, "a.A.Type"},     /* A's field: the enclosing message first */
        {"p.B", NULL, "t.Type"},       /* B's field: B holds no Type, the package's */
        {"p", NULL, "t.Type"},         /* outside any message */
        {"p.B", "A.Type", "a.A.Type"}, /* qualified from B: A decides at p */
        {"", "p.Type", "t.Type"},      /* from the root */
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        cbm_closure_ref_t ref = {.name = "Type", .written = cases[i].written, .scope = cases[i].scope};
        cbm_closure_answer_t x = cbm_closure_lookup(s, NULL, a, &ref);
        if (x.verdict != CBM_CLOSURE_PROVEN || !x.qn || strcmp(x.qn, cases[i].want) != 0) {
            printf("\n  scope %s written %s: verdict %d qn %s, want proven %s\n", cases[i].scope,
                   cases[i].written ? cases[i].written : "-", (int)x.verdict, x.qn ? x.qn : "-",
                   cases[i].want);
            cbm_closure_free(s);
            FAIL("the reference's own scope did not decide");
        }
    }
    /* `B.Type` from A: B is found at p and holds no Type, no backtracking */
    cbm_closure_ref_t miss = {.name = "Type", .written = "B.Type", .scope = "p.A"};
    ASSERT_NEQ(cbm_closure_lookup(s, NULL, a, &miss).verdict, CBM_CLOSURE_PROVEN);
    cbm_closure_free(s);
    PASS();
}

TEST(scope_closure_work_grows_linearly) {
    for (int chain = 0; chain < 2; chain++) {
        for (int lookups = 0; lookups < 2; lookups++) { /* the build, then the lookups */
            uint64_t w1 = sc_work(100, chain, lookups);
            uint64_t w4 = sc_work(400, chain, lookups);
            uint64_t w16 = sc_work(1600, chain, lookups);
            printf("\n  %s %s: %llu / %llu / %llu at 1x / 4x / 16x", chain ? "chain" : "fan",
                   lookups ? "lookups" : "build", (unsigned long long)w1, (unsigned long long)w4,
                   (unsigned long long)w16);
            ASSERT_GT(w1, 0);
            ASSERT_GT(w4, 0);
            ASSERT_GT(w16, 0);
            /* at most 4.5 times the work per 4 times the files */
            ASSERT_TRUE(w4 * 2 <= w1 * 9);
            ASSERT_TRUE(w16 * 2 <= w4 * 9);
        }
    }
    printf("\n");
    PASS();
}

SUITE(scope_closure) {
    RUN_TEST(scope_closure_review_shapes_grow_linearly);
    RUN_TEST(scope_closure_nested_name_in_every_file_grows_linearly);
    RUN_TEST(scope_closure_reference_scope_decides_innermost_first);
    RUN_TEST(scope_closure_one_definition_through_an_include_is_proven);
    RUN_TEST(scope_closure_reexport_chain_and_cycle);
    RUN_TEST(scope_closure_incomplete_is_possible_or_no_answer);
    RUN_TEST(scope_closure_ambiguous_conditional_and_nodeless);
    RUN_TEST(scope_closure_qualified_reference);
    RUN_TEST(scope_closure_unknown_package_reads_every_scope);
    RUN_TEST(scope_closure_answers_do_not_depend_on_the_memo);
    RUN_TEST(scope_closure_rows);
    RUN_TEST(scope_closure_work_grows_linearly);
}
