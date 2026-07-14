/*
 * repro_invariant_lsp_rescue.c — QUALITY_ANALYSIS gap #5 / #5a:
 * the LSP rescue cannot recover a bad tree-sitter caller QN because the
 * join key is exact caller-QN string equality.
 *
 * THE BLOCKER (file:func:line):
 *   cbm_pipeline_find_lsp_resolution  (src/pipeline/lsp_resolve.h:48)
 *   joins each LSP-resolved call (CBMResolvedCall) to the tree-sitter call
 *   (CBMCall) with EXACT string equality on the caller QN:
 *
 *       lsp_resolve.h:65:
 *           if (strcmp(rc->caller_qn, call->enclosing_func_qn) != 0)
 *               continue;
 *
 *   Consumed by:
 *     - src/pipeline/pass_calls.c:369 (sequential pipeline,
 *       resolve_single_call → emit_classified_edge)
 *     - src/pipeline/pass_parallel.c:1797 (parallel pipeline)
 *
 *   When tree-sitter's enclosing-func walk FAILS, cbm_enclosing_func_qn
 *   falls back to the MODULE QN, so call->enclosing_func_qn is the module
 *   QN. The C/C++ LSP cross resolver (internal/cbm/lsp/c_lsp.c) builds its
 *   OWN enclosing QN from scope resolution — for an out-of-line method
 *   Foo::bar it produces the real method QN "<proj>.<module>.Foo.bar"
 *   (c_process_function, c_lsp.c:4138-4143) and emits a CBMResolvedCall
 *   with caller_qn = that real method QN, strategy = "lsp_direct" /
 *   "lsp_implicit_this" / "lsp_type_dispatch", confidence 0.95
 *   (c_emit_resolved_call, c_lsp.c:3287-3296). 0.95 is well above
 *   CBM_LSP_CONFIDENCE_FLOOR (0.6f, lsp_resolve.h:36).
 *
 *   So the LSP HAS the correct caller, but the join key on the
 *   tree-sitter side is the MODULE QN. module-QN != real-method-QN, the
 *   strcmp at lsp_resolve.h:65 never matches, find_lsp_resolution returns
 *   NULL, the LSP rescue branch (pass_calls.c:370-385) is skipped, and the
 *   edge falls through to the registry resolver — staying Module-sourced
 *   with a registry strategy. The LSP rescue is silently DISCARDED.
 *
 * FIXTURE RATIONALE (C++ out-of-line method — the #554 family):
 *   A free function helper() and a class Processor with an OUT-OF-LINE
 *   method definition Processor::run that calls helper(v). For the
 *   out-of-line method body, tree-sitter's cbm_find_enclosing_func cannot
 *   walk the call-expression's ancestry back to a node whose type is in
 *   func_kinds_cpp = {"function_definition"} in a way that yields the
 *   class-qualified method QN, so cbm_enclosing_func_qn falls back to the
 *   module QN (issue #554 / extract_defs.c + c_lsp.c dominate the
 *   QUALITY_ANALYSIS Module-sourced-CALLS top-file list). C/C++ has a
 *   cross-file LSP wired up (cbm_pxc_has_cross_lsp, pass_lsp_cross.c:281),
 *   so the LSP DOES resolve the real Processor::run caller. This is the
 *   cleanest fixture where tree-sitter attribution lands on Module but the
 *   LSP resolves the real enclosing function — exactly gap #5a.
 *
 * EXPECTED vs ACTUAL:
 *   EXPECTED (correct, what the fix must produce): the helper() CALLS edge
 *   is sourced at the real callable node Processor::run (label
 *   "Function"/"Method"), via the LSP rescue, and its properties_json
 *   carries the LSP strategy marker (strategy starts with "lsp_") and the
 *   LSP confidence (0.95).
 *   ACTUAL (today, RED): the join discards the LSP result, so the edge is
 *   Module-sourced and its properties carry a registry strategy
 *   (same_module / import_map / ...), never an "lsp_" strategy.
 *
 * This file deliberately complements repro_invariant_calls.c: that file
 * asserts the broad "zero Module-sourced CALLS" invariant; THIS file
 * pins the *mechanism* — that the LSP rescue specifically is the missing
 * recovery, by also asserting the rescued edge preserves the LSP
 * strategy/confidence in its properties_json (gap #5a, second assertion).
 *
 * NOTE: line comments only inside this header (no block comments inside a
 * block comment, per coding rules).
 */

#include "test_framework.h"
#include "repro_harness.h"
#include <store/store.h>

#include <string.h>

/* ── Fixture ────────────────────────────────────────────────────────────── */

/*
 * Out-of-line method Processor::run calls the free function helper().
 * - helper        : free function, definition-style body.
 * - Processor::run: OUT-OF-LINE method definition. tree-sitter's
 *                   enclosing-func walk falls back to the module QN here
 *                   (#554), but the C++ LSP resolves caller = Processor::run.
 * The call we care about is `helper(v)` inside Processor::run.
 */
static const char kCppOutOfLine[] =
    "static int helper(int x) { return x * 2; }\n"
    "\n"
    "class Processor {\n"
    "public:\n"
    "    int run(int v);\n"
    "};\n"
    "\n"
    "int Processor::run(int v) {\n"
    "    return helper(v);\n"
    "}\n";

/* ── Locate the helper() CALLS edge ─────────────────────────────────────── */

/*
 * find_call_edge_to_helper
 *
 * Scan all CALLS edges and return (by out-params) the one whose TARGET node
 * qualified_name ends in ".helper" — that is the `helper(v)` call site inside
 * Processor::run. Copies the source node and the edge's properties_json into
 * caller-owned buffers so the caller can assert after freeing the edge array.
 *
 * Returns 1 if found, 0 otherwise.
 */
static int find_call_edge_to_helper(cbm_store_t *store, const char *project,
                                    cbm_node_t *out_src, char *out_props,
                                    size_t props_cap) {
    cbm_edge_t *edges = NULL;
    int nedges = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &nedges)
            != CBM_STORE_OK) {
        return 0;
    }

    int found = 0;
    for (int i = 0; i < nedges; i++) {
        cbm_node_t tgt;
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &tgt)
                != CBM_STORE_OK) {
            continue;
        }
        const char *tqn = tgt.qualified_name ? tgt.qualified_name : "";
        size_t tlen = strlen(tqn);
        const char *suffix = ".helper";
        size_t slen = strlen(suffix);
        if (tlen < slen || strcmp(tqn + tlen - slen, suffix) != 0) {
            continue;
        }
        /* This is the helper() call edge. Capture its source node + props. */
        if (cbm_store_find_node_by_id(store, edges[i].source_id, out_src)
                == CBM_STORE_OK) {
            const char *props = edges[i].properties_json
                                    ? edges[i].properties_json : "{}";
            snprintf(out_props, props_cap, "%s", props);
            found = 1;
        }
        break;
    }

    cbm_store_free_edges(edges, nedges);
    return found;
}

/* ── #5: rescued edge must be callable-sourced via the LSP caller ───────── */

/*
 * repro_invariant_lsp_rescue_source
 *
 * Expected: RED on current code.
 *
 * The helper() call inside the out-of-line method Processor::run must be
 * sourced at the real callable node (label "Function" or "Method") — the
 * LSP resolves caller = Processor::run, which should rescue the bad
 * tree-sitter Module attribution.
 *
 * Today the join in cbm_pipeline_find_lsp_resolution (lsp_resolve.h:65)
 * requires rc->caller_qn == call->enclosing_func_qn; tree-sitter supplies
 * the MODULE QN, the LSP supplies the real method QN, they never strcmp
 * equal, the LSP rescue is discarded, and the edge stays Module-sourced.
 * So src.label == "Module" → this assertion FAILS (RED), proving the bug.
 */
TEST(repro_invariant_lsp_rescue_source) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, "main.cpp", kCppOutOfLine);
    ASSERT_TRUE(store != NULL);

    cbm_node_t src;
    char props[1024];
    int found = find_call_edge_to_helper(store, lp.project, &src,
                                         props, sizeof(props));

    /* Sanity: the helper() CALLS edge must exist at all, else no signal. */
    ASSERT_TRUE(found == 1);

    const char *lbl = src.label ? src.label : "(null)";

    /*
     * INVARIANT (RED today): the edge is sourced at the real callable
     * (Function/Method), NOT at the Module. The only path that can produce
     * this for an out-of-line method whose tree-sitter enclosing is Module
     * is the LSP rescue — which the exact-QN join discards today.
     */
    ASSERT_TRUE(strcmp(lbl, "Function") == 0 || strcmp(lbl, "Method") == 0);

    rh_cleanup(&lp, store);
    return 0;
}

/* ── #5a: rescued edge must preserve the LSP strategy/confidence ────────── */

/*
 * repro_invariant_lsp_rescue_props
 *
 * Expected: RED on current code.
 *
 * Per QUALITY_ANALYSIS gap #5a, when the LSP rescues a call the emitted
 * edge must record the LSP provenance. pass_calls.c:374-381 copies
 * res.strategy = lsp->strategy and res.confidence = lsp->confidence into
 * the edge, and emit_classified_edge writes them into properties_json as
 *   {"callee":"...","confidence":0.95,"strategy":"lsp_...","candidates":1}
 * (pass_calls.c:336-340). The C++ LSP strategies are all "lsp_"-prefixed
 * (lsp_direct / lsp_implicit_this / lsp_type_dispatch / lsp_virtual_dispatch
 * / lsp_base_dispatch / lsp_smart_ptr_dispatch, c_lsp.c:3390-3658) at
 * confidence 0.95.
 *
 * Today the rescue never fires (join discarded), so the surviving edge is
 * registry-resolved and its strategy is a registry strategy (same_module /
 * import_map / ...), never "lsp_". The substring "\"strategy\":\"lsp_" is
 * therefore ABSENT from properties_json → this assertion FAILS (RED).
 *
 * If a future change emits the rescued edge but with different property
 * keys, update the marker here; the source-label invariant in the test
 * above is the primary, key-independent signal.
 */
TEST(repro_invariant_lsp_rescue_props) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, "main.cpp", kCppOutOfLine);
    ASSERT_TRUE(store != NULL);

    cbm_node_t src;
    char props[1024];
    int found = find_call_edge_to_helper(store, lp.project, &src,
                                         props, sizeof(props));
    ASSERT_TRUE(found == 1);

    /*
     * INVARIANT (RED today): the rescued edge's properties_json carries the
     * LSP strategy marker. We look for a "strategy" value beginning with
     * "lsp_" — the prefix shared by every C/C++ LSP strategy string.
     */
    int has_lsp_strategy = (strstr(props, "\"strategy\":\"lsp_") != NULL);
    ASSERT_TRUE(has_lsp_strategy);

    rh_cleanup(&lp, store);
    return 0;
}

/* ── Suite ──────────────────────────────────────────────────────────────── */

SUITE(repro_invariant_lsp_rescue) {
    RUN_TEST(repro_invariant_lsp_rescue_source);
    RUN_TEST(repro_invariant_lsp_rescue_props);
}
