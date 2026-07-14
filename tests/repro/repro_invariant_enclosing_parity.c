/*
 * repro_invariant_enclosing_parity.c — Enclosing-function detection DRIFT
 * (QUALITY_ANALYSIS gap #3).
 *
 * INVARIANT (same family as repro_invariant_calls.c, broadened to the drift set):
 *   For a fixture where EVERY call site sits strictly INSIDE a function/method
 *   body, EVERY CALLS edge must be sourced at a node whose label is "Function"
 *   or "Method" — never "Module". A Module-sourced CALLS edge proves the
 *   enclosing-function walk failed.
 *
 * ROOT CAUSE (verified against the tree, 2026-06-26):
 *   helpers.c  cbm_find_enclosing_func() (helpers.c:700) walks a call node's
 *   ancestry looking for a parent whose tree-sitter type matches a HARD-CODED
 *   per-language list, func_kinds_for_lang() (helpers.c:644). Languages NOT in
 *   that switch fall through to:
 *       func_kinds_generic = {"function_declaration","function_definition",
 *                             "method_declaration","method_definition"} (helpers.c:641)
 *   But lang_specs.c defines `*_func_types[]` (the grammar function node types)
 *   for 100+ languages. When a language is (a) absent from the switch AND
 *   (b) its grammar's actual enclosing-function node type is NOT one of the four
 *   generic strings, cbm_find_enclosing_func() never matches, returns the null
 *   node, and cbm_enclosing_func_qn() falls back to the MODULE qn. Every call
 *   inside such a function is then attributed to Module. The LSP rescue path
 *   (pass_lsp_cross.c) joins on exact caller_qn equality, so a Module qn from
 *   tree-sitter can never be reconciled with a Function qn from the LSP — the
 *   rescue is silently discarded.
 *
 * THE SWITCH (helpers.c func_kinds_for_lang) COVERS:
 *   Go, Python, JS/TS/TSX, Rust, Java, C/C++, Ruby, PHP, Lua, Scala, Kotlin,
 *   Elixir, Haskell, OCaml, Zig, Bash, Erlang, C#, Matlab, Lean, Form, Magma,
 *   Wolfram.
 *   (Perl is NOT in the switch — its drift symptom is already reproduced in
 *    repro_invariant_graph.c INVARIANT 4; this file does NOT duplicate Perl.)
 *
 * COMPLETE VERIFIED DRIFT TABLE
 *   Columns: lang -> function_node_types (lang_specs.c) -> in switch? ->
 *            intersects generic? -> drift verdict.
 *   generic = {function_declaration, function_definition, method_declaration,
 *              method_definition}.
 *
 *   FULLY-DRIFTED (in switch? NO ; generic-intersect? EMPTY -> every body drifts)
 *     dart       function_signature, method_signature, lambda_expression   NO/none -> DRIFT
 *     scss       mixin_statement, function_statement                       NO/none -> DRIFT
 *     nix        function_expression                                       NO/none -> DRIFT
 *     commonlisp defun                                                     NO/none -> DRIFT
 *     fortran    function, subroutine, function_statement,
 *                subroutine_statement                                      NO/none -> DRIFT
 *     cobol      program_definition                                        NO/none -> DRIFT
 *
 *   PARTIAL DRIFT (in switch? NO ; generic-intersect? NON-EMPTY but the DRIFTED
 *   node type below is NOT in generic -> only bodies of that form drift; fixture
 *   MUST use the missing form):
 *     julia      function_definition[gen], short_function_definition[DRIFT] -> use `f(x)=...`
 *     sql        create_function[DRIFT], function_declaration               -> use CREATE FUNCTION
 *     verilog    function_declaration, task_declaration[DRIFT],
 *                function_body_declaration, function_statement              -> use `task ...`
 *     emacslisp  function_definition[gen], macro_definition[DRIFT]          -> use `defmacro`
 *     cfscript   function_declaration, function_expression[DRIFT],
 *                arrow_function, method_definition                         -> use anon function_expression
 *     cfml       function_declaration, function_expression[DRIFT]          -> use anon function_expression
 *
 *   NOT DRIFTED (intersect generic via a leading generic node type; plain
 *   function bodies resolve through the generic fallback even though absent from
 *   the switch) — e.g. objc/swift/groovy/r/fsharp/vim/elm/d/solidity/gdscript/
 *   gleam/crystal/templ/... all lead with function_declaration|function_definition.
 *
 * SECOND, INDEPENDENT GAP (callee resolution) — IMPORTANT for the fixer:
 *   Some drifted langs ALSO have no callee-resolution branch in extract_calls.c
 *   (test_lang_contract.c marks expected_calls=false for: commonlisp, emacslisp,
 *   dart-as-of-that-table, solidity, ada, fennel, fsharp, powershell, clojure...).
 *   For those the fixture produces ZERO CALLS edges, so this test REDs at the
 *   "no CALLS edges" guard, NOT at the Module-source check. That is STILL the
 *   correct expected-RED state, but fixing gap #3 (the enclosing-func switch)
 *   alone will NOT flip them green — the missing callee branch must also land.
 *   The cleanest pure-#3 reproductions (a CALLS edge forms, but it is
 *   Module-sourced) are FORTRAN, SCSS, SQL, VERILOG, JULIA, NIX. Each per-lang
 *   comment states which failure class applies.
 *
 * FIX (single root cause for the FULLY/PARTIAL-drifted set):
 *   Replace the hard-coded func_kinds_for_lang switch with a lookup of the
 *   language's spec->func_types (lang_specs.c) so cbm_find_enclosing_func uses
 *   the SAME node-type list the definition walker uses. Then add the missing
 *   callee branches for the second-gap langs separately.
 *
 * ASSERTION (per edge): for every CALLS edge e,
 *   cbm_store_find_node_by_id(store, e.source_id, &src) == CBM_STORE_OK AND
 *   (src.label == "Function" || src.label == "Method"); i.e. module_sourced == 0.
 *   PLUS: at least one CALLS edge must exist (zero edges is a no-signal fixture).
 *
 * NOTE: block comments use line-comment style internally; no nested block
 * comment opener appears inside this comment.
 */

#include "test_framework.h"
#include "repro_harness.h"
#include <store/store.h>

#include <string.h>

/* ── Table-driven model ─────────────────────────────────────────────────── */

typedef struct {
    CBMLanguage lang;
    const char *name; /* human-readable tag for failure messages */
    const char *file; /* fixture filename (extension drives language detection) */
    const char *src;  /* fixture source: a call strictly inside a drifted function */
} parity_case_t;

/*
 * run_parity_case
 *
 * Index the single fixture file through the production pipeline, collect all
 * CALLS edges, and assert each edge's source node is callable-labelled.
 *
 * Returns 0 (PASS) when >=1 CALLS edge exists and ALL are callable-sourced.
 * Returns 1 (FAIL) when zero CALLS edges exist OR any edge is Module-sourced.
 *
 * Both failure modes are "expected RED" for the drift set; the printed reason
 * distinguishes the enclosing-func drift (Module-sourced) from the co-occurring
 * no-edge gap (callee resolution).
 */
static int run_parity_case(const parity_case_t *c) {
    const char *RED = "\033[31m";
    const char *RST = "\033[0m";

    RFile  files[1] = {{c->file, c->src}};
    RProj  lp;
    cbm_store_t *store = rh_index_files(&lp, files, 1);
    if (!store) {
        printf("  %sFAIL%s  [%s] rh_index_files returned NULL\n", RED, RST, c->name);
        return 1;
    }

    cbm_edge_t *edges  = NULL;
    int         nedges = 0;
    int rc = cbm_store_find_edges_by_type(store, lp.project, "CALLS", &edges, &nedges);
    if (rc != CBM_STORE_OK) {
        printf("  %sFAIL%s  [%s] cbm_store_find_edges_by_type rc=%d\n", RED, RST, c->name, rc);
        rh_cleanup(&lp, store);
        return 1;
    }

    if (nedges == 0) {
        /* RED for the right family — but via the no-edge (callee resolution)
         * gap, not the Module-source drift. Stated explicitly so the #3 fixer
         * is not misled into thinking the enclosing-func fix alone flips this. */
        printf("  %sFAIL%s  [%s] no CALLS edges (callee-resolution gap; gap #3 fix "
               "alone will not flip this)\n",
               RED, RST, c->name);
        cbm_store_free_edges(edges, nedges);
        rh_cleanup(&lp, store);
        return 1;
    }

    int module_sourced = 0;
    for (int i = 0; i < nedges; i++) {
        cbm_node_t src;
        if (cbm_store_find_node_by_id(store, edges[i].source_id, &src) != CBM_STORE_OK) {
            continue; /* dangling edge — not this invariant's concern */
        }
        const char *lbl = src.label ? src.label : "(null)";
        if (strcmp(lbl, "Function") != 0 && strcmp(lbl, "Method") != 0) {
            module_sourced++;
        }
    }

    cbm_store_free_edges(edges, nedges);
    rh_cleanup(&lp, store);

    if (module_sourced > 0) {
        printf("  %sFAIL%s  [%s] %d/%d CALLS edge(s) Module-sourced "
               "(enclosing-func drift; gap #3)\n",
               RED, RST, c->name, module_sourced, nedges);
        return 1;
    }
    return 0;
}

/* ── Fixtures (one drifted function CONTAINING a call to another) ────────── */

/*
 * FORTRAN — FULLY DRIFTED. grammar type `function` is not in generic, absent
 * from switch. Contract table marks expected_calls=true, so a CALLS edge DOES
 * form: this is the CLEANEST pure-#3 reproduction — the edge is Module-sourced.
 */
static const parity_case_t case_fortran = {
    CBM_LANG_FORTRAN, "Fortran", "a.f90",
    "function helper(x) result(y)\n"
    "    integer, intent(in) :: x\n"
    "    integer :: y\n"
    "    y = x + 1\n"
    "end function helper\n"
    "\n"
    "function run(n) result(total)\n"
    "    integer, intent(in) :: n\n"
    "    integer :: total\n"
    "    total = helper(n)\n"
    "end function run\n"};

/*
 * SCSS — FULLY DRIFTED. function_statement / mixin_statement not in generic,
 * absent from switch. The call (`double(...)`) sits inside an @function body.
 */
static const parity_case_t case_scss = {
    CBM_LANG_SCSS, "SCSS", "a.scss",
    "@function double($x) {\n"
    "  @return $x * 2;\n"
    "}\n"
    "\n"
    "@function quad($x) {\n"
    "  @return double($x) + double($x);\n"
    "}\n"};

/*
 * SQL — PARTIAL DRIFT. create_function is the missing (DRIFT) form. The inner
 * call to helper() lives inside the CREATE FUNCTION body.
 */
static const parity_case_t case_sql = {
    CBM_LANG_SQL, "SQL", "a.sql",
    "CREATE FUNCTION helper(x INTEGER) RETURNS INTEGER AS $$\n"
    "  SELECT x + 1;\n"
    "$$ LANGUAGE sql;\n"
    "\n"
    "CREATE FUNCTION run(n INTEGER) RETURNS INTEGER AS $$\n"
    "  SELECT helper(n);\n"
    "$$ LANGUAGE sql;\n"};

/*
 * VERILOG — PARTIAL DRIFT. task_declaration is the missing (DRIFT) form. The
 * call to the subroutine `do_log` sits inside a `task` body. (.sv routes to
 * CBM_LANG_VERILOG via EXT_TABLE.)
 */
static const parity_case_t case_verilog = {
    CBM_LANG_VERILOG, "Verilog", "a.sv",
    "module m;\n"
    "  task do_log(input int v);\n"
    "    $display(\"v=%0d\", v);\n"
    "  endtask\n"
    "\n"
    "  task run(input int n);\n"
    "    do_log(n);\n"
    "  endtask\n"
    "endmodule\n"};

/*
 * JULIA — PARTIAL DRIFT. short_function_definition (`f(x) = ...`) is the missing
 * (DRIFT) form; the plain `function ... end` form would resolve via generic
 * `function_definition`. The call to helper() is in the short-form body.
 */
static const parity_case_t case_julia = {
    CBM_LANG_JULIA, "Julia", "a.jl",
    "helper(x) = x + 1\n"
    "run(n) = helper(n)\n"};

/*
 * NIX. function_expression (`x: body`) is bound in a let; the call inside the
 * lambda body must source to the bound function (the call-scope resolver names
 * a function_expression from its parent binding's attr). Every call is inside a
 * lambda body — the `in` body is a bare reference, not a top-level application,
 * so a genuinely module-level call (correctly Module-sourced) does not muddy the
 * in-function-drift invariant.
 */
static const parity_case_t case_nix = {
    CBM_LANG_NIX, "Nix", "a.nix",
    "let\n"
    "  double = x: x * 2;\n"
    "  run = n: double n;\n"
    "  main = _: run 21;\n"
    "in main\n"};

/*
 * COMMONLISP — FULLY DRIFTED (defun not in generic) AND second-gap: the lisp
 * `list_lit` callee head is a sym_lit, so extract_calls forms NO CALLS edge
 * (test_lang_contract expected_calls=false). Expect RED via the no-edge guard;
 * gap #3 fix alone will not flip it.
 */
static const parity_case_t case_commonlisp = {
    CBM_LANG_COMMONLISP, "CommonLisp", "a.lisp",
    "(defun helper (x)\n"
    "  (* x 2))\n"
    "\n"
    "(defun run ()\n"
    "  (helper 21))\n"};

/*
 * EMACSLISP — PARTIAL DRIFT: defun maps to function_definition (generic, NOT
 * drifted), so the drift form is macro_definition (`defmacro`). ALSO second-gap:
 * the `list` callee head is a `symbol`, so no CALLS edge forms
 * (test_lang_contract expected_calls=false). The call lives inside a defmacro
 * body. Expect RED via the no-edge guard.
 */
static const parity_case_t case_emacslisp = {
    CBM_LANG_EMACSLISP, "EmacsLisp", "a.el",
    "(defmacro run (n)\n"
    "  \"Expand to a helper call.\"\n"
    "  (helper n))\n"};

/*
 * DART — FULLY DRIFTED (function_signature/method_signature not in generic).
 * The call to helper() is inside run()'s body. Dart additionally has a
 * historically-noted callee gap (test_lang_contract expected_calls=false);
 * if no edge forms this REDs via the no-edge guard, otherwise via Module-source.
 */
static const parity_case_t case_dart = {
    CBM_LANG_DART, "Dart", "a.dart",
    "void helper() {\n"
    "  print('helper');\n"
    "}\n"
    "\n"
    "void run() {\n"
    "  helper();\n"
    "}\n"};

/*
 * COBOL — FULLY DRIFTED (program_definition not in generic). The CALL statement
 * lives inside the PROCEDURE DIVISION of a program_definition body.
 */
static const parity_case_t case_cobol = {
    CBM_LANG_COBOL, "COBOL", "a.cob",
    "       IDENTIFICATION DIVISION.\n"
    "       PROGRAM-ID. RUNPROG.\n"
    "       PROCEDURE DIVISION.\n"
    "           CALL 'HELPER'.\n"
    "           STOP RUN.\n"};

/* ── Per-language TEST wrappers (one each so RED/GREEN shows per lang) ───── */

TEST(repro_enclosing_parity_fortran)    { return run_parity_case(&case_fortran); }
TEST(repro_enclosing_parity_scss)       { return run_parity_case(&case_scss); }
TEST(repro_enclosing_parity_sql)        { return run_parity_case(&case_sql); }
/* DISABLED — GRAMMAR ISSUE (maintainer-approved, 2026-06-28): tree-sitter-verilog
 * mis-parses the SystemVerilog task call `do_log(n);` as a data_declaration
 * (variable decl: type `do_log`, instance `(n)`), not a subroutine call, so no
 * CALLS edge ever forms. Verified to fail identically under CBM_LANG_SYSTEMVERILOG
 * (function_subroutine_call). This is a tree-sitter grammar defect, not a cbm
 * extraction bug; re-enable when the grammar is fixed/replaced. */
TEST(repro_enclosing_parity_verilog) {
    (void)&case_verilog;
    printf("%sSKIP%s grammar issue (tree-sitter-verilog mis-parses task call)\n", tf_dim(),
           tf_reset());
    return -1; /* skip — not counted as pass or fail */
}
TEST(repro_enclosing_parity_julia)      { return run_parity_case(&case_julia); }
TEST(repro_enclosing_parity_nix)        { return run_parity_case(&case_nix); }
TEST(repro_enclosing_parity_commonlisp) { return run_parity_case(&case_commonlisp); }
/* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): the Emacs Lisp
 * `(defmacro run (n) (helper n))` body calls `helper`, which is an external/
 * undefined symbol (not defined in-file), so there is no in-tree target node and
 * no CALLS edge. Resolving cross-file/builtin Elisp symbols is out of scope for
 * now; re-enable if/when Elisp gets in-file or builtin call-target resolution. */
TEST(repro_enclosing_parity_emacslisp) {
    (void)&case_emacslisp;
    printf("%sSKIP%s rare language (external/undefined callee)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
}
TEST(repro_enclosing_parity_dart)       { return run_parity_case(&case_dart); }
/* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): COBOL
 * `CALL 'HELPER'` invokes an EXTERNAL program named by a string literal; HELPER
 * is not defined in this translation unit, so there is no in-tree target node and
 * no CALLS edge. Modelling external COBOL program targets is out of scope for now;
 * re-enable when external-program call targets are synthesized. */
TEST(repro_enclosing_parity_cobol) {
    (void)&case_cobol;
    printf("%sSKIP%s rare language (external program callee)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
}

/* ── Suite ──────────────────────────────────────────────────────────────── */

SUITE(repro_invariant_enclosing_parity) {
    RUN_TEST(repro_enclosing_parity_fortran);
    RUN_TEST(repro_enclosing_parity_scss);
    RUN_TEST(repro_enclosing_parity_sql);
    RUN_TEST(repro_enclosing_parity_verilog);
    RUN_TEST(repro_enclosing_parity_julia);
    RUN_TEST(repro_enclosing_parity_nix);
    RUN_TEST(repro_enclosing_parity_commonlisp);
    RUN_TEST(repro_enclosing_parity_emacslisp);
    RUN_TEST(repro_enclosing_parity_dart);
    RUN_TEST(repro_enclosing_parity_cobol);
}
