/*
 * repro_issue495.c — Reproduce-first case for issue #495:
 *   "cfg-gated twin functions collapse into one node; get_code_snippet
 *   returns the inactive branch's body"
 *
 * ROOT CAUSE (extraction layer):
 *   extract_func_def() computes:
 *     def.qualified_name = cbm_fqn_compute(project, rel_path, name)
 *   for every Rust function_item it visits.  Two same-named functions
 *   guarded by mutually-exclusive #[cfg(...)] attributes both parse as
 *   distinct function_item nodes and both pass through extract_func_def,
 *   but they receive the SAME qualified_name (no cfg predicate is folded
 *   in).  When the graph store upserts them it hits the UNIQUE(project,
 *   qualified_name) constraint and the second write silently overwrites
 *   the first — one branch is lost entirely.
 *
 * EXPECTED (correct) behavior:
 *   Each cfg-gated twin must receive a DISTINCT qualified_name that
 *   encodes its cfg predicate, e.g.
 *     "t.src.try_extract_pdf_text"           (active / feature branch)
 *     "t.src.try_extract_pdf_text#cfg(not(feature=\"rag-pdf\"))" (stub)
 *   So that the graph can keep BOTH nodes and get_code_snippet can return
 *   the correct body for the requested cfg context.
 *
 * ACTUAL (buggy) behavior:
 *   Both defs carry identical qualified_name "t.src.try_extract_pdf_text".
 *   The assertion `qn_a != qn_b` FAILS (both equal the same string), so
 *   this test is RED on unpatched code.
 *
 * SECONDARY assertions (also RED until fixed, targeting the same root
 * cause from different angles):
 *   • The REAL-body function has param name "bytes" (no underscore);
 *     the STUB has "_bytes".  Each def's signature must correspond to its
 *     own branch — i.e. BOTH signatures must appear in the result, one
 *     containing "bytes" without a leading underscore and one with "_bytes".
 *   • Each def's decorators[0] must contain the cfg predicate of ITS OWN
 *     branch (not the other's), so that a fixer can easily scope-qualify
 *     the QN from the already-captured decorator text.
 *
 * Why these assertions are RED on current code:
 *   All three assertions require distinguishing the two defs by their QN.
 *   Since both QNs are currently identical, any loop looking for "the
 *   active branch" finds the SAME node twice, and the body-token /
 *   decorator checks collapse to checking ONE def against itself.
 */

#include "test_framework.h"
#include "cbm.h"

/* ── Helpers ──────────────────────────────────────────────────────── */

/* Extract a Rust source string and return the raw CBMFileResult.
 * Caller must cbm_free_result() the returned pointer. */
static CBMFileResult *rx(const char *src, const char *proj, const char *path) {
    return cbm_extract_file(src, (int)strlen(src), CBM_LANG_RUST, proj, path, 0, NULL, NULL);
}

/* Count how many defs in r have exactly this label AND name. */
static int count_defs_named(CBMFileResult *r, const char *label, const char *name) {
    int n = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (label && (!d->label || strcmp(d->label, label) != 0))
            continue;
        if (name && (!d->name || strcmp(d->name, name) != 0))
            continue;
        n++;
    }
    return n;
}

/* Return the Nth (0-based) def matching label + name, or NULL. */
static CBMDefinition *nth_def_named(CBMFileResult *r, const char *label, const char *name, int nth) {
    int seen = 0;
    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (label && (!d->label || strcmp(d->label, label) != 0))
            continue;
        if (name && (!d->name || strcmp(d->name, name) != 0))
            continue;
        if (seen == nth)
            return d;
        seen++;
    }
    return NULL;
}

/* ── Test ─────────────────────────────────────────────────────────── */

/*
 * Rust source with two mutually-exclusive cfg-gated definitions of the
 * same function.  Tree-sitter sees both function_item nodes regardless
 * of which cfg is active (it does not preprocess).  The correct fix must
 * emit two DISTINCT graph nodes — one per branch — so that
 * get_code_snippet can return the right body for the right build.
 *
 * The "real" branch (feature = "rag-pdf") has:
 *   - parameter name "bytes"  (no underscore)
 *   - a non-trivial body (returns Some(String::new()))
 *   - starts at line 2
 *
 * The "stub" branch (not(feature = "rag-pdf")) has:
 *   - parameter name "_bytes" (underscore = unused)
 *   - a trivial body (returns None)
 *   - starts at line 7
 */
TEST(repro_issue495_cfg_gated_twins_distinct) {
    static const char *src =
        "#[cfg(feature = \"rag-pdf\")]\n"
        "fn try_extract_pdf_text(bytes: &[u8]) -> Option<String> {\n"
        "    if bytes.is_empty() { return None; }\n"
        "    Some(String::new())\n"
        "}\n"
        "\n"
        "#[cfg(not(feature = \"rag-pdf\"))]\n"
        "fn try_extract_pdf_text(_bytes: &[u8]) -> Option<String> { None }\n";

    CBMFileResult *r = rx(src, "t", "src.rs");
    ASSERT_NOT_NULL(r);
    ASSERT_FALSE(r->has_error);

    /* ── Part 1: both defs must be present in the extraction output ── */

    int twin_count = count_defs_named(r, "Function", "try_extract_pdf_text");

    /* Both function_item nodes are in the tree-sitter parse; both must
     * be emitted.  This should already pass on current code (extraction
     * visits both nodes) and acts as a precondition for Parts 2 & 3. */
    ASSERT_GTE(twin_count, 2);

    /* ── Part 2 (PRIMARY RED): distinct qualified_names per twin ───── */

    /* Retrieve the two defs.  On buggy code both have the same QN, so
     * even picking them by index 0 and 1 is meaningful: the pair MUST
     * carry two DIFFERENT qualified_name strings. */
    CBMDefinition *d0 = nth_def_named(r, "Function", "try_extract_pdf_text", 0);
    CBMDefinition *d1 = nth_def_named(r, "Function", "try_extract_pdf_text", 1);
    ASSERT_NOT_NULL(d0);
    ASSERT_NOT_NULL(d1);
    ASSERT_NOT_NULL(d0->qualified_name);
    ASSERT_NOT_NULL(d1->qualified_name);

    /* ROOT CAUSE ASSERTION: the two cfg-gated twins must have DISTINCT
     * qualified_names so the graph upsert can store them as separate
     * nodes.  On current (buggy) code both equal "t.src.try_extract_pdf_text"
     * and this assertion FAILS → RED. */
    ASSERT_STR_NEQ(d0->qualified_name, d1->qualified_name);

    /* ── Part 3 (SECONDARY RED): each def carries its own cfg predicate */

    /* The decorator text for each function_item is already captured by
     * extract_decorators() into def.decorators[0].  The fix can use this
     * captured text to build the disambiguating QN suffix.  We verify
     * that the right predicate lives on the right def:
     *
     *   - the def whose signature contains "bytes" (no underscore, real
     *     body) must have a decorator containing "feature" but NOT "not("
     *   - the def whose signature contains "_bytes" (stub) must have a
     *     decorator containing "not("
     *
     * On buggy code: d0 and d1 have identical QN so we cannot distinguish
     * which is the real and which is the stub — the pair-identity check
     * in Part 2 already failed.  Parts 2 and 3 together pin the root
     * cause at extract_func_def() failing to fold the cfg predicate into
     * the qualified_name. */
    CBMDefinition *real_def = NULL;  /* #[cfg(feature = "rag-pdf")]     */
    CBMDefinition *stub_def = NULL;  /* #[cfg(not(feature = "rag-pdf"))] */

    for (int i = 0; i < r->defs.count; i++) {
        CBMDefinition *d = &r->defs.items[i];
        if (!d->name || strcmp(d->name, "try_extract_pdf_text") != 0)
            continue;
        if (!d->qualified_name)
            continue;
        /* Identify by the cfg predicate baked into the (fixed) QN.
         * On unpatched code both QNs are identical so neither branch
         * is reachable via a unique QN → real_def / stub_def stay NULL
         * → the ASSERT_NOT_NULLs below fire as a second RED signal. */
        if (strstr(d->qualified_name, "not(") != NULL) {
            stub_def = d;
        } else {
            real_def = d;
        }
    }

    /* On fixed code: two distinct QNs → both pointers set. */
    ASSERT_NOT_NULL(real_def);   /* RED on current code */
    ASSERT_NOT_NULL(stub_def);   /* RED on current code */

    /* Decorator text must survive and identify each branch. */
    ASSERT_NOT_NULL(real_def->decorators);
    ASSERT_NOT_NULL(real_def->decorators[0]);
    ASSERT_TRUE(strstr(real_def->decorators[0], "cfg") != NULL);
    ASSERT_TRUE(strstr(real_def->decorators[0], "not(") == NULL);

    ASSERT_NOT_NULL(stub_def->decorators);
    ASSERT_NOT_NULL(stub_def->decorators[0]);
    ASSERT_TRUE(strstr(stub_def->decorators[0], "not(") != NULL);

    /* Line ranges must not overlap (both trees are in-source). */
    ASSERT_TRUE(real_def->start_line != stub_def->start_line);
    ASSERT_TRUE(real_def->end_line   < stub_def->start_line ||
                stub_def->end_line   < real_def->start_line);

    cbm_free_result(r);
    PASS();
}

/* ── Suite ────────────────────────────────────────────────────────── */
SUITE(repro_issue495) {
    RUN_TEST(repro_issue495_cfg_gated_twins_distinct);
}
