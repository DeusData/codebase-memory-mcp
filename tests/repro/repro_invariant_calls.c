/*
 * repro_invariant_calls.c — Source-position-aware CALLS attribution invariant.
 *
 * INVARIANT:
 *   For any project where EVERY call site is located INSIDE a function or
 *   method body (no top-level/module-level calls), EVERY CALLS edge in the
 *   graph must be sourced at a node whose label is "Function" or "Method".
 *   Zero CALLS edges may be sourced at a "Module" node.
 *
 * BASIS (QUALITY_ANALYSIS.md, 2026-06-24):
 *   Graph quality audit over the real codebase-memory-mcp repo showed only
 *   3.69% of CALLS edges are callable-sourced (207/5607). The dominant
 *   failure mode is cbm_enclosing_func_qn returning the module QN when
 *   cbm_find_enclosing_func cannot walk the TSNode ancestry back to a
 *   function node. Root cause: func_kinds_for_lang (helpers.c:644) uses a
 *   hardcoded per-language list that is not always in sync with the actual
 *   grammar node types emitted by each tree-sitter grammar; when no ancestor
 *   type matches the list, cbm_find_enclosing_func returns a null node and
 *   cbm_enclosing_func_qn falls back to the module QN. The LSP rescue path
 *   (pass_lsp_cross.c) cannot compensate because it joins on exact
 *   caller_qn equality — a Module QN from tree-sitter is never equal to a
 *   Function QN from LSP, so the LSP result is silently discarded.
 *
 * EXPECTED per language (based on helpers.c func_kinds_for_lang):
 *   GREEN (callable source expected to work):
 *     Go         — func_kinds_go = {function_declaration, method_declaration}
 *                  Standard grammar; tree-sitter-go is mature; enclosing-func
 *                  walk works reliably. Python/Go confirmed correct in
 *                  QUALITY_ANALYSIS grep validation.
 *     Python     — func_kinds_python = {function_definition}
 *                  Standard grammar; confirmed correct in QUALITY_ANALYSIS.
 *
 *   RED (callable source expected to fall back to Module on current code):
 *     C          — func_kinds_cpp = {function_definition}
 *                  C uses the same list as C++. QUALITY_ANALYSIS top-file
 *                  list is dominated by C files (extract_defs.c: 182 Module-
 *                  sourced CALLS, c_lsp.c: 86). The enclosing-func walk for
 *                  C requires the call-expression's ancestor chain to include
 *                  a function_definition node; C test failures are explicitly
 *                  cited as expected-red in the quality contracts suite.
 *     C++        — same func_kinds as C. Out-of-line method definitions
 *                  (Foo::bar) also lose the class qualifier (see issue #554).
 *                  QUALITY_ANALYSIS explicitly lists C/C++ callable-source
 *                  failures as known-red in the node_creation_probe contract.
 *     TypeScript — func_kinds_js = {function_declaration, method_definition,
 *                  arrow_function, ...}. Method definitions and arrow
 *                  function fields are supported, but class method bodies
 *                  emitted by the TS grammar use "method_definition" — listed
 *                  in func_kinds_js — so TS SHOULD be green for ordinary
 *                  function bodies. HOWEVER, QUALITY_ANALYSIS section 6 lists
 *                  TS in the breadth-suite gap set (ts_lsp.c: 95 Module-
 *                  sourced CALLS in the real graph). This fixture uses a
 *                  plain function calling another, the simplest case; we
 *                  expect GREEN. If TS still fails the test will document it.
 *     Java       — func_kinds_java = {method_declaration, constructor_declaration}
 *                  Java LSP is supported. The real-graph audit shows
 *                  java_lsp.h: 90 Module-sourced CALLS. A plain method
 *                  calling another in the same class should be the simplest
 *                  possible case; we expect GREEN but the audit evidence
 *                  suggests it may be RED.
 *     C#         — func_kinds_csharp = {method_declaration, constructor_declaration}
 *                  Analogous to Java. Similar LSP support. Expected GREEN for
 *                  the minimal case, but marked as potentially RED per breadth
 *                  suite evidence.
 *     Rust       — func_kinds_rust = {function_item}
 *                  Rust LSP is hybrid but cbm_pxc_has_cross_lsp returns false
 *                  for CBM_LANG_RUST (pass_lsp_cross.c:281). The enclosing-
 *                  func walk uses only tree-sitter. Expected RED because
 *                  QUALITY_ANALYSIS section 6 notes Rust in the failing set
 *                  and rust_lsp.h: 102 Module-sourced CALLS appears in the
 *                  top-file list.
 *
 * ASSERTION (per edge):
 *   For every cbm_edge_t e where e.type == "CALLS":
 *     cbm_store_find_node_by_id(store, e.source_id, &src) == CBM_STORE_OK
 *     AND (strcmp(src.label, "Function") == 0 || strcmp(src.label, "Method") == 0)
 *   Equivalently: module_sourced_count == 0.
 *
 * NOTE: inline comments below use line comments only (no block comments
 * inside block comments per coding rules).
 */

#include "test_framework.h"
#include "repro_harness.h"
#include <store/store.h>

#include <string.h>

/* ── Shared runner ──────────────────────────────────────────────────────── */

/*
 * assert_calls_callable_sourced
 *
 * Index `files[0..nfiles)` through the production pipeline, collect all CALLS
 * edges, and assert that each edge's source node has label "Function" or
 * "Method" (never "Module").
 *
 * Returns 0 (PASS) when the invariant holds.
 * Returns 1 (FAIL) when one or more Module-sourced CALLS edges are found.
 *
 * lang_tag is a human-readable string used in failure messages only.
 */
static int assert_calls_callable_sourced(const char *lang_tag,
                                         const RFile *files, int nfiles) {
    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, nfiles);
    if (!store) {
        printf("  %sFAIL%s  [%s] rh_index_files returned NULL\n",
               "\033[31m", "\033[0m", lang_tag);
        return 1;
    }

    cbm_edge_t *edges   = NULL;
    int         nedges  = 0;
    int rc = cbm_store_find_edges_by_type(store, lp.project, "CALLS",
                                          &edges, &nedges);
    if (rc != CBM_STORE_OK) {
        printf("  %sFAIL%s  [%s] cbm_store_find_edges_by_type rc=%d\n",
               "\033[31m", "\033[0m", lang_tag, rc);
        rh_cleanup(&lp, store);
        return 1;
    }

    /*
     * We must find at least one CALLS edge — a fixture with zero calls would
     * trivially satisfy the invariant and give no signal. Treat zero edges as
     * a test-setup problem, not a pass.
     */
    if (nedges == 0) {
        printf("  %sFAIL%s  [%s] no CALLS edges found (fixture problem: "
               "expected >= 1)\n",
               "\033[31m", "\033[0m", lang_tag);
        cbm_store_free_edges(edges, nedges);
        rh_cleanup(&lp, store);
        return 1;
    }

    int module_sourced = 0;
    for (int i = 0; i < nedges; i++) {
        cbm_node_t src;
        if (cbm_store_find_node_by_id(store, edges[i].source_id, &src)
                != CBM_STORE_OK) {
            continue; /* dangling edge — ignore for this invariant */
        }
        const char *lbl = src.label ? src.label : "(null)";
        if (strcmp(lbl, "Function") != 0 && strcmp(lbl, "Method") != 0) {
            module_sourced++;
        }
    }

    cbm_store_free_edges(edges, nedges);
    rh_cleanup(&lp, store);

    if (module_sourced > 0) {
        printf("  %sFAIL%s  [%s] %d/%d CALLS edge(s) sourced at non-callable "
               "node (expected 0 module-sourced)\n",
               "\033[31m", "\033[0m", lang_tag, module_sourced, nedges);
        return 1;
    }
    return 0; /* all edges callable-sourced */
}

/* ── C ──────────────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_c
 *
 * Expected: RED on current code.
 * Root cause: func_kinds_cpp = {"function_definition"} is used for C too.
 * The C files dominate the Module-sourced CALLS list in QUALITY_ANALYSIS
 * (extract_defs.c: 182, c_lsp.c: 86). Even the simplest intra-file call
 * between two C functions falls back to Module sourcing because the
 * cbm_enclosing_func_qn path does not correctly resolve the caller QN and
 * the LSP rescue is blocked by the exact-QN equality join requirement.
 */
TEST(repro_invariant_calls_c) {
    static const char src[] =
        "static int add(int a, int b) { return a + b; }\n"
        "\n"
        "int compute(int x) {\n"
        "    return add(x, 1);\n"
        "}\n";

    static const RFile files[] = {
        { "main.c", src },
    };
    return assert_calls_callable_sourced("C",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── C++ ────────────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_cpp
 *
 * Expected: RED on current code.
 * Shares the same func_kinds as C. Out-of-line method definitions additionally
 * drop the class qualifier (issue #554 / helpers.c cbm_enclosing_func_qn).
 * Uses both a free function and a member method so the test covers both forms.
 */
TEST(repro_invariant_calls_cpp) {
    static const char src[] =
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

    static const RFile files[] = {
        { "main.cpp", src },
    };
    return assert_calls_callable_sourced("C++",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── Go ─────────────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_go
 *
 * Expected: GREEN on current code.
 * func_kinds_go = {function_declaration, method_declaration}.
 * Go grammar is mature; tree-sitter-go is stable. QUALITY_ANALYSIS confirms
 * Python/Go callable attribution as correct via grep validation.
 * This case is a regression guard: if it goes RED a future change has broken
 * Go callable attribution.
 */
TEST(repro_invariant_calls_go) {
    static const char src[] =
        "package main\n"
        "\n"
        "func add(a, b int) int {\n"
        "    return a + b\n"
        "}\n"
        "\n"
        "func compute(x int) int {\n"
        "    return add(x, 1)\n"
        "}\n";

    static const RFile files[] = {
        { "main.go", src },
    };
    return assert_calls_callable_sourced("Go",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── Python ─────────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_python
 *
 * Expected: GREEN on current code.
 * func_kinds_python = {function_definition}.
 * QUALITY_ANALYSIS grep-validated Python callable attribution as correct.
 * Regression guard.
 */
TEST(repro_invariant_calls_python) {
    static const char src[] =
        "def add(a, b):\n"
        "    return a + b\n"
        "\n"
        "def compute(x):\n"
        "    return add(x, 1)\n";

    static const RFile files[] = {
        { "main.py", src },
    };
    return assert_calls_callable_sourced("Python",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── TypeScript ─────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_ts
 *
 * Expected: GREEN for a plain function-calls-function fixture (func_kinds_js
 * includes function_declaration and arrow_function). However QUALITY_ANALYSIS
 * shows ts_lsp.c with 95 Module-sourced CALLS in the real graph, so this may
 * be RED. The test documents whichever state holds currently.
 */
TEST(repro_invariant_calls_ts) {
    static const char src[] =
        "function add(a: number, b: number): number {\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "function compute(x: number): number {\n"
        "    return add(x, 1);\n"
        "}\n";

    static const RFile files[] = {
        { "main.ts", src },
    };
    return assert_calls_callable_sourced("TypeScript",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── Java ───────────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_java
 *
 * Expected: likely RED, possibly GREEN.
 * func_kinds_java = {method_declaration, constructor_declaration}.
 * java_lsp.h shows 90 Module-sourced CALLS in the real graph. The simplest
 * same-class method call is the minimal fixture; if even this fails the
 * attribution gap is comprehensive.
 */
TEST(repro_invariant_calls_java) {
    static const char src[] =
        "public class Calculator {\n"
        "    private int add(int a, int b) {\n"
        "        return a + b;\n"
        "    }\n"
        "\n"
        "    public int compute(int x) {\n"
        "        return add(x, 1);\n"
        "    }\n"
        "}\n";

    static const RFile files[] = {
        { "Calculator.java", src },
    };
    return assert_calls_callable_sourced("Java",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── C# ─────────────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_csharp
 *
 * Expected: likely RED, possibly GREEN.
 * func_kinds_csharp = {method_declaration, constructor_declaration}.
 * Analogous evidence to Java from QUALITY_ANALYSIS breadth suite gaps.
 */
TEST(repro_invariant_calls_csharp) {
    static const char src[] =
        "public class Calculator {\n"
        "    private int Add(int a, int b) {\n"
        "        return a + b;\n"
        "    }\n"
        "\n"
        "    public int Compute(int x) {\n"
        "        return Add(x, 1);\n"
        "    }\n"
        "}\n";

    static const RFile files[] = {
        { "Calculator.cs", src },
    };
    return assert_calls_callable_sourced("C#",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── Rust ───────────────────────────────────────────────────────────────── */

/*
 * repro_invariant_calls_rust
 *
 * Expected: RED on current code.
 * func_kinds_rust = {function_item}.
 * cbm_pxc_has_cross_lsp returns false for CBM_LANG_RUST (pass_lsp_cross.c:281)
 * so the cross-file LSP rescue path never runs for Rust. rust_lsp.h appears
 * with 102 Module-sourced CALLS in the QUALITY_ANALYSIS top-file list.
 * Even a single-file intra-function call will fall back to Module sourcing
 * because the tree-sitter enclosing-func walk alone is insufficient.
 */
TEST(repro_invariant_calls_rust) {
    static const char src[] =
        "fn add(a: i32, b: i32) -> i32 {\n"
        "    a + b\n"
        "}\n"
        "\n"
        "fn compute(x: i32) -> i32 {\n"
        "    add(x, 1)\n"
        "}\n";

    static const RFile files[] = {
        { "main.rs", src },
    };
    return assert_calls_callable_sourced("Rust",
        files, (int)(sizeof(files) / sizeof(files[0])));
}

/* ── Suite ──────────────────────────────────────────────────────────────── */

SUITE(repro_invariant_calls) {
    RUN_TEST(repro_invariant_calls_c);
    RUN_TEST(repro_invariant_calls_cpp);
    RUN_TEST(repro_invariant_calls_go);
    RUN_TEST(repro_invariant_calls_python);
    RUN_TEST(repro_invariant_calls_ts);
    RUN_TEST(repro_invariant_calls_java);
    RUN_TEST(repro_invariant_calls_csharp);
    RUN_TEST(repro_invariant_calls_rust);
}
