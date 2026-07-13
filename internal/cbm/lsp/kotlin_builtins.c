/*
 * kotlin_builtins.c — Minimal Kotlin universal builtins as real graph nodes.
 *
 * When a method call lands on an unknown-typed receiver and the member is one
 * of the universal kotlin.Any methods (toString / equals / hashCode), the
 * Kotlin LSP resolves it to "kotlin.Any.<member>" and emits the lsp_kt_any
 * strategy (kotlin_lsp.c, kt_emit_resolved). Any is the supertype of every
 * Kotlin reference, so this is the same target the fwcd LSP resolves to.
 *
 * The missing piece is downstream: pass_calls.c only writes a CALLS edge when
 * cbm_pipeline_lsp_target_node() resolves the callee_qn to a graph node
 * (src/pipeline/lsp_resolve.h). There is no "kotlin.Any" node in the graph, so
 * the resolved call is dropped and the strategy never lands on an edge
 * (callable=0).
 *
 * Fix: inject a small, fixed set of kotlin.Any definitions into result->defs
 * during the per-file Kotlin LSP run (cbm_run_kotlin_lsp, which executes inside
 * cbm_extract_file, BEFORE the parallel pipeline mints def nodes from
 * result->defs). The graph therefore gains real "kotlin.Any[.<method>]" nodes
 * that the lsp_kt_any edges target. The QNs here MUST match what kt_emit_resolved
 * emits as callee_qn ("kotlin.Any.<member>").
 *
 * Node minting upserts by QN (cbm_gbuf_upsert_node), so injecting the same
 * builtins per Kotlin file collapses to one node per QN — no duplicates.
 *
 * Self-contained: #included from kotlin_lsp.c only (amalgamation pattern; see
 * lsp_all.c). Not a standalone translation unit. Mirror of py_builtins.c.
 */

/* A single builtin entry to mint as a graph node. */
typedef struct {
    const char *qn;    /* graph QN — MUST equal the kt_emit_resolved callee_qn */
    const char *name;  /* short name (last segment of qn)                      */
    const char *label; /* "Class" | "Method"                                   */
} KtBuiltinNode;

/*
 * Universal kotlin.Any members the LSP falls back to (kt_any_methods in
 * kotlin_lsp.c). The Any class node anchors the three methods.
 */
static const KtBuiltinNode kKtBuiltinNodes[] = {
    {"kotlin.Any", "Any", "Class"},
    {"kotlin.Any.toString", "toString", "Method"},
    {"kotlin.Any.equals", "equals", "Method"},
    {"kotlin.Any.hashCode", "hashCode", "Method"},
};

/*
 * Inject the builtin definitions into result->defs so the pipeline mints them
 * as graph nodes. All fields beyond name/qn/label are left zero/NULL: builtins
 * have no body, so complexity/line-range/etc. are irrelevant, and a synthetic
 * file_path keeps them out of any real source file's def list.
 */
static void kt_builtins_inject_defs(CBMFileResult *result, CBMArena *arena) {
    if (!result || !arena) {
        return;
    }
    const int n = (int)(sizeof(kKtBuiltinNodes) / sizeof(kKtBuiltinNodes[0]));
    for (int i = 0; i < n; i++) {
        const KtBuiltinNode *b = &kKtBuiltinNodes[i];
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = b->name;
        def.qualified_name = b->qn;
        def.label = b->label;
        def.file_path = "<kotlin-builtins>";
        def.start_line = 1;
        def.end_line = 1;
        cbm_defs_push(&result->defs, arena, def);
    }
}
