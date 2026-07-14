/*
 * py_builtins.c — Minimal Python builtins as real graph nodes.
 *
 * The Python LSP type registry already knows the builtins (typeshed-derived
 * generated/python_stdlib_data.c registers builtins.len, builtins.str,
 * builtins.str.upper, builtins.list.append, ...). So a call like len(v) /
 * str(v) / "x".upper() / xs.append(1) ALREADY resolves at the LSP layer and
 * emits the correct strategy (lsp_builtin / lsp_builtin_constructor /
 * lsp_builtin_method / lsp_generic_method) with callee_qn = "builtins.<name>".
 *
 * The missing piece is downstream: pass_calls.c only writes a CALLS edge when
 * cbm_pipeline_lsp_target_node() resolves the callee_qn to a graph node
 * (src/pipeline/lsp_resolve.h). There is no "builtins.len" node in the graph,
 * so the resolved call is dropped and the strategy never lands on an edge.
 *
 * Fix: inject a small, fixed set of builtin definitions into result->defs
 * during the per-file Python LSP run (which executes inside cbm_extract_file,
 * BEFORE the parallel pipeline mints def nodes from result->defs). The graph
 * therefore gains real "builtins.*" nodes that the LSP-emitted edges target.
 * The QNs here MUST match what the typeshed registry emits as callee_qn.
 *
 * Node minting upserts by QN (cbm_gbuf_upsert_node), so injecting the same
 * builtins per Python file collapses to one node per QN — no duplicates.
 *
 * Self-contained: #included from py_lsp.c only (CGo amalgamation pattern;
 * see lsp_all.c). Not a standalone translation unit.
 */

/* A single builtin entry to mint as a graph node. */
typedef struct {
    const char *qn;    /* graph QN — MUST equal the registry callee_qn   */
    const char *name;  /* short name (last segment of qn)                */
    const char *label; /* "Function" | "Class" | "Method"                */
} PyBuiltinNode;

/*
 * Minimal builtins set. Kept deliberately small and aligned with the registry
 * (generated/python_stdlib_data.c):
 *   - free functions  (lsp_builtin):             len, print
 *   - types/ctors     (lsp_builtin_constructor): str, int, list, dict, range
 *   - str methods     (lsp_builtin_method):      upper, lower
 *   - list methods    (lsp_generic_method):      append, pop
 *   - dict methods    (lsp_generic_method):      get
 * Note: str/int/list/dict/range are TYPES in the registry (so X() routes to
 * lsp_builtin_constructor), hence the "Class" label here.
 */
static const PyBuiltinNode kPyBuiltinNodes[] = {
    {"builtins.len", "len", "Function"},
    {"builtins.print", "print", "Function"},

    {"builtins.str", "str", "Class"},
    {"builtins.int", "int", "Class"},
    {"builtins.list", "list", "Class"},
    {"builtins.dict", "dict", "Class"},
    {"builtins.range", "range", "Class"},

    {"builtins.str.upper", "upper", "Method"},
    {"builtins.str.lower", "lower", "Method"},

    {"builtins.list.append", "append", "Method"},
    {"builtins.list.pop", "pop", "Method"},

    {"builtins.dict.get", "get", "Method"},
};

/*
 * Inject the builtin definitions into result->defs so the pipeline mints them
 * as graph nodes. All fields beyond name/qn/label are left zero/NULL: builtins
 * have no body, so complexity/line-range/etc. are irrelevant, and a synthetic
 * file_path keeps them out of any real source file's def list.
 */
static void py_builtins_inject_defs(CBMFileResult *result, CBMArena *arena) {
    if (!result || !arena) {
        return;
    }
    const int n = (int)(sizeof(kPyBuiltinNodes) / sizeof(kPyBuiltinNodes[0]));
    for (int i = 0; i < n; i++) {
        const PyBuiltinNode *b = &kPyBuiltinNodes[i];
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = b->name;
        def.qualified_name = b->qn;
        def.label = b->label;
        def.file_path = "<python-builtins>";
        def.start_line = 1;
        def.end_line = 1;
        cbm_defs_push(&result->defs, arena, def);
    }
}
