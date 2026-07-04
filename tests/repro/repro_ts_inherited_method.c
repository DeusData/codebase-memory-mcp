/*
 * repro_ts_inherited_method.c — RED reproduction: TypeScript CROSS-FILE
 * INHERITED method call resolution gap (ts_lsp).
 *
 * THE GAP: a typed call to a method the receiver's class INHERITS from a base
 * class defined in ANOTHER file is never resolved by the TS LSP:
 *
 *   base.ts:     export class Base { greet(): string { ... } }
 *   derived.ts:  import { Base } from "./base";
 *                export class Derived extends Base { ... }
 *                export function callSite(): string {
 *                    const d: Derived = new Derived();
 *                    return d.greet();          <-- inherited, cross-file
 *                }
 *
 * CORRECT behaviour (asserted here, so this test is RED until fixed): a CALLS
 * edge from the caller (callable-sourced, QN contains "callSite") to the BASE
 * method definition (target QN suffix ".Base.greet" — mirroring the
 * java/kotlin/php inherited-dispatch convention: resolution lands on the base
 * class's method def, since Derived declares no `greet` and no such node can
 * exist), carrying a genuine TS-LSP resolution strategy ("lsp_ts_*" in the
 * edge's properties_json, per the ts_emit_resolved_call contract documented in
 * repro_lsp_ts.c).
 *
 * WHY the strategy tag is part of the invariant: before the weak-short-name
 * suppression (PR #840, recovering withdrawn #836/#835), this scenario looked
 * resolved via a unique_name REGISTRY fallback — "greet" is unique in a 2-file
 * fixture, so a short-name guess happened to bind the right node (a false
 * green; in a real repo the same guess binds an arbitrary same-named method).
 * PR #840 flipped the probe lrp_ts_s6_inherited_method
 * (tests/test_lsp_resolution_probe.c) to assert that this weak edge is
 * SUPPRESSED (calls == 0) — correct, but it leaves the underlying resolution
 * gap without any red reproduction. THIS test is that reproduction:
 *   - on pre-#840 code the lucky edge exists but carries NO "lsp_ts" strategy
 *     -> RED (the green was never genuine);
 *   - on post-#840 code the weak edge is suppressed, no edge exists at all
 *     -> RED;
 *   - only genuine ts_lsp cross-file inheritance resolution turns it GREEN.
 *
 * ROOT-CAUSE POINTER (for the eventual fixer): ts_lsp cross-file inheritance
 * resolution — internal/cbm/lsp/ts_lsp.c resolve_member_call/lookup_method
 * only walks methods declared on the receiver's OWN class as registered in the
 * module registry; it does not follow the (correctly extracted) INHERITS edge
 * from Derived to an imported Base to find `greet` there. See PR #836/#840 and
 * the S6 probe lrp_ts_s6_inherited_method for the full analysis. The INHERITS
 * edge and both defs ARE in the graph (asserted below as preconditions), so a
 * red here is the RESOLUTION gap, not an extraction failure.
 *
 * NOTE: line comments only inside this header (no nested block comments, per
 * coding rules).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <string.h>

/* ── Fixture ─────────────────────────────────────────────────────────────── */

static const RFile kTsInherited[] = {
    {"base.ts",
     "export class Base {\n"
     "    greet(): string { return \"hello\"; }\n"
     "}\n"},
    {"derived.ts",
     "import { Base } from \"./base\";\n"
     "\n"
     "export class Derived extends Base {\n"
     "    extra(): number { return 2; }\n"
     "}\n"
     "\n"
     "export function callSite(): string {\n"
     "    const d: Derived = new Derived();\n"
     "    return d.greet();\n"
     "}\n"},
};

/* ── Local store helpers ─────────────────────────────────────────────────── */

/* True if some node with `label` has a QN ending in `suffix`. */
static int node_with_qn_suffix(cbm_store_t *store, const char *project,
                               const char *label, const char *suffix) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) !=
        CBM_STORE_OK)
        return 0;
    int found = 0;
    size_t sl = strlen(suffix);
    for (int i = 0; i < count && !found; i++) {
        const char *qn = nodes[i].qualified_name;
        if (qn) {
            size_t ql = strlen(qn);
            if (ql >= sl && strcmp(qn + ql - sl, suffix) == 0)
                found = 1;
        }
    }
    cbm_store_free_nodes(nodes, count);
    return found;
}

/*
 * The PRIMARY invariant, checked on a SINGLE edge (independent per-edge checks
 * could false-green by combining a strategy-less lucky edge with an unrelated
 * lsp_ts-tagged edge): there exists a CALLS edge whose
 *   - source is callable-sourced (Function/Method) and its QN contains
 *     `caller_substr`;
 *   - target QN ends with `callee_suffix`;
 *   - properties_json carries `strategy_substr` (substring, so any concrete
 *     "lsp_ts_*" tag satisfies "lsp_ts").
 * When `dump` is non-zero every CALLS edge is printed to stderr so a RED run
 * documents exactly what the graph contains instead.
 */
static int lsp_resolved_edge_exists(cbm_store_t *store, const char *project,
                                    const char *caller_substr,
                                    const char *callee_suffix,
                                    const char *strategy_substr, int dump) {
    cbm_edge_t *edges = NULL;
    int n = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS", &edges, &n) !=
        CBM_STORE_OK)
        return 0;
    int found = 0;
    size_t cl = strlen(callee_suffix);
    for (int i = 0; i < n; i++) {
        cbm_node_t src, tgt;
        if (cbm_store_find_node_by_id(store, edges[i].source_id, &src) != CBM_STORE_OK)
            continue;
        if (cbm_store_find_node_by_id(store, edges[i].target_id, &tgt) != CBM_STORE_OK)
            continue;
        if (dump)
            fprintf(stderr, "    [ts-inherited] CALLS %s (%s) -> %s props=%s\n",
                    src.qualified_name ? src.qualified_name : "?",
                    src.label ? src.label : "?",
                    tgt.qualified_name ? tgt.qualified_name : "?",
                    edges[i].properties_json ? edges[i].properties_json : "{}");
        const char *slabel = src.label ? src.label : "";
        if (strcmp(slabel, "Function") != 0 && strcmp(slabel, "Method") != 0)
            continue;
        if (!src.qualified_name || !strstr(src.qualified_name, caller_substr))
            continue;
        const char *tqn = tgt.qualified_name;
        if (!tqn)
            continue;
        size_t tl = strlen(tqn);
        if (tl < cl || strcmp(tqn + tl - cl, callee_suffix) != 0)
            continue;
        if (!edges[i].properties_json ||
            !strstr(edges[i].properties_json, strategy_substr))
            continue;
        found = 1;
    }
    cbm_store_free_edges(edges, n);
    return found;
}

/* ── Tests ───────────────────────────────────────────────────────────────── */

/*
 * Extraction-tier preconditions — expected GREEN on current code. These prove
 * a red in the pipeline test below is the RESOLUTION gap, not a fixture or
 * extraction error: both files parse without has_error, base.ts yields the
 * Method def `greet`, and derived.ts yields the `greet` call site.
 */
TEST(repro_ts_inherited_extraction_preconditions) {
    /* base.ts extracts cleanly and defines Method greet. */
    ASSERT_TRUE(inv_extract_clean(kTsInherited[0].content, CBM_LANG_TYPESCRIPT,
                                  "base.ts"));
    CBMFileResult *rb =
        inv_rx(kTsInherited[0].content, CBM_LANG_TYPESCRIPT, "base.ts");
    ASSERT_NOT_NULL(rb);
    int greet_methods = 0;
    for (int i = 0; i < rb->defs.count; i++) {
        CBMDefinition *d = &rb->defs.items[i];
        if (d->label && strcmp(d->label, "Method") == 0 && d->name &&
            strcmp(d->name, "greet") == 0)
            greet_methods++;
    }
    cbm_free_result(rb);
    ASSERT_EQ(greet_methods, 1);

    /* derived.ts extracts cleanly and contains the greet call site. */
    ASSERT_TRUE(inv_extract_clean(kTsInherited[1].content, CBM_LANG_TYPESCRIPT,
                                  "derived.ts"));
    CBMFileResult *rd =
        inv_rx(kTsInherited[1].content, CBM_LANG_TYPESCRIPT, "derived.ts");
    ASSERT_NOT_NULL(rd);
    int has_greet_call = inv_has_call(rd, "greet");
    cbm_free_result(rd);
    ASSERT_TRUE(has_greet_call);
    PASS();
}

/*
 * THE RED REPRODUCTION — index the 2-file fixture through the full production
 * pipeline and assert the CORRECT outcome: an LSP-resolved CALLS edge from
 * callSite to Base.greet. Store-level preconditions (callee node present,
 * INHERITS extracted) are checked first so the failure is attributable to the
 * missing ts_lsp cross-file inheritance resolution and nothing else.
 * Returns 0 on PASS (gap fixed), non-zero on FAIL (RED = the open gap).
 */
static int run_ts_inherited_pipeline(void) {
    RProj lp;
    cbm_store_t *store = rh_index_files(
        &lp, kTsInherited, (int)(sizeof(kTsInherited) / sizeof(kTsInherited[0])));
    if (!store) {
        printf("  %sFAIL%s %s:%d: index failed (setup, NOT the gap)\n", tf_red(),
               tf_reset(), __FILE__, __LINE__);
        rh_cleanup(&lp, store);
        return 1;
    }

    int rc = 0;

    /* Precondition: the callee def node exists in the graph. */
    if (!node_with_qn_suffix(store, lp.project, "Method", ".Base.greet")) {
        printf("  %sFAIL%s %s:%d: precondition — no Method node with QN suffix "
               "\".Base.greet\" (extraction problem, NOT the resolution gap)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__);
        rc = 1;
    }

    /* Precondition: `Derived extends Base` produced an INHERITS edge (the S6
     * probe diagnostic confirms extraction emits it; the gap is downstream). */
    int inherits = rh_count_edges(store, lp.project, "INHERITS");
    if (inherits < 1) {
        printf("  %sFAIL%s %s:%d: precondition — INHERITS=%d (expected >=1; "
               "extraction problem, NOT the resolution gap)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__, inherits);
        rc = 1;
    }

    /* PRIMARY: the inherited call is resolved BY THE TS LSP — a CALLS edge
     * callSite -> *.Base.greet carrying an "lsp_ts" strategy. A short-name
     * registry fallback edge (no lsp_ts tag) does NOT satisfy this; nor does
     * post-#840 suppression (no edge at all). */
    if (!lsp_resolved_edge_exists(store, lp.project, "callSite", ".Base.greet",
                                  "lsp_ts", 0)) {
        /* Dump what the graph actually contains so the RED row documents it. */
        (void)lsp_resolved_edge_exists(store, lp.project, "callSite",
                                       ".Base.greet", "lsp_ts", 1);
        printf("  %sFAIL%s %s:%d: no lsp_ts-resolved CALLS edge callSite -> "
               "*.Base.greet — TS cross-file INHERITED method call is "
               "UNRESOLVED (ts_lsp inheritance gap, see #836/#840)\n",
               tf_red(), tf_reset(), __FILE__, __LINE__);
        rc = 1;
    }

    rh_cleanup(&lp, store);
    return rc;
}

TEST(repro_ts_inherited_method_call_resolution) {
    return run_ts_inherited_pipeline();
}

/* ── Suite ───────────────────────────────────────────────────────────────── */

SUITE(repro_ts_inherited_method) {
    RUN_TEST(repro_ts_inherited_extraction_preconditions);
    RUN_TEST(repro_ts_inherited_method_call_resolution);
}
