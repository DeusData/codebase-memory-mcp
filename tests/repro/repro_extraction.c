/*
 * repro_extraction.c — Reproduce-first cases for OPEN extraction-quality bugs.
 *
 * Each TEST() asserts the CORRECT behaviour and is RED until the bug is fixed.
 * Keep one TEST() per issue; name it repro_issue<N>_<slug> and lead with a
 * comment naming the issue, the root cause, and expected-vs-actual.
 *
 * Cluster (TIER A, in-process via cbm_extract_file):
 *   #554 — C++ out-of-line method CALLS source = Module, not enclosing Method
 *   (more added per wave: #495 #521 #382 #408 #523 #56 #333)
 */
#include "test_framework.h"
#include "cbm.h"

/* Convenience: extract, return result (caller frees). Mirrors test_extraction.c. */
static CBMFileResult *rx(const char *src, CBMLanguage lang, const char *proj, const char *path) {
    return cbm_extract_file(src, (int)strlen(src), lang, proj, path, 0, NULL, NULL);
}

/* Find the first definition matching label+name (either may be NULL = wildcard). */
static CBMDefinition *find_def(CBMFileResult *r, const char *label, const char *name) {
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (label && (!d->label || strcmp(d->label, label) != 0))
            continue;
        if (name && (!d->name || strcmp(d->name, name) != 0))
            continue;
        return d;
    }
    return NULL;
}

/* ───────────────────────────────────────────────────────────────────
 * #554 — C++ out-of-line method definitions: the CALLS edge source falls
 * back to the Module (file-level) instead of the enclosing Method.
 *
 * Root cause (#621 follow-up to #463/adc8304): for `void Foo::bar() { helper(); }`
 * the inner call's `enclosing_func_qn` drops the CLASS qualifier — it resolves to
 * the bare method name (e.g. "t.m.bar") instead of the method node's full
 * class-qualified QN (e.g. "t.m.Foo.bar"). The pre-existing guard in
 * test_extraction.c only checks `enclosing_func_qn != "t.m"` (module), which a
 * buggy "t.m.bar" PASSES — so it never caught the class-qualifier drop.
 *
 * Strong reproduction: tie the call's enclosing_func_qn to the METHOD DEFINITION's
 * own qualified_name (format-agnostic) AND require the class qualifier be present.
 * Expected: enclosing_func_qn == def(bar).qualified_name, and that QN names "Foo".
 * Actual (buggy): enclosing_func_qn loses "Foo" → mismatch → RED.
 * ─────────────────────────────────────────────────────────────────── */
TEST(repro_issue554_cpp_out_of_line_method_class_qualified) {
    CBMFileResult *r = rx("struct Foo { void bar(); };\n"
                          "int helper(int x) { return x; }\n"
                          "void Foo::bar() { helper(1); }\n",
                          CBM_LANG_CPP, "t", "m.cpp");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* The out-of-line method definition: its qualified_name is the ground truth
     * the inner CALLS edge must point at. */
    CBMDefinition *method = find_def(r, "Method", "bar");
    if (!method)
        method = find_def(r, NULL, "bar"); /* tolerate label variance */
    ASSERT_NOT_NULL(method);
    ASSERT_NOT_NULL(method->qualified_name);

    /* The method node must carry the class qualifier — either embedded in the QN
     * or via parent_class. This is the heart of #554/#621. */
    int qn_has_class = strstr(method->qualified_name, "Foo") != NULL;
    int parent_has_class = method->parent_class && strstr(method->parent_class, "Foo") != NULL;
    ASSERT_TRUE(qn_has_class || parent_has_class);

    /* The helper() call inside Foo::bar must attribute to the method node, i.e.
     * its enclosing_func_qn must EQUAL the method's qualified_name (class included),
     * not the bare method name and not the module. */
    int saw_helper = 0;
    for (int i = 0; i < r->calls.count; i++) {
        if (strcmp(r->calls.items[i].callee_name, "helper") == 0) {
            saw_helper = 1;
            const char *enc = r->calls.items[i].enclosing_func_qn;
            ASSERT_NOT_NULL(enc);
            ASSERT_STR_EQ(enc, method->qualified_name);
            ASSERT_TRUE(strstr(enc, "Foo") != NULL); /* class qualifier preserved */
        }
    }
    ASSERT_TRUE(saw_helper);

    cbm_free_result(r);
    PASS();
}

/* ── Suite ──────────────────────────────────────────────────────── */
SUITE(repro_extraction) {
    RUN_TEST(repro_issue554_cpp_out_of_line_method_class_qualified);
}
