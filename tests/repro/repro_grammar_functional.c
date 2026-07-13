/*
 * repro_grammar_functional.c -- Per-grammar INVARIANT battery for the
 * FUNCTIONAL language family.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs the same battery against a tiny idiomatic fixture for
 * that language (a named function/definition whose body calls another named
 * function). The shared single_file_battery() + pipeline_battery() helpers
 * below are a direct mirror of those in repro_grammar_core.c.
 *
 * Languages covered (13) and the CBM_LANG_* enum each uses:
 *   Haskell      -> CBM_LANG_HASKELL
 *   OCaml        -> CBM_LANG_OCAML
 *   F#           -> CBM_LANG_FSHARP
 *   Elixir       -> CBM_LANG_ELIXIR
 *   Erlang       -> CBM_LANG_ERLANG
 *   Elm          -> CBM_LANG_ELM
 *   Clojure      -> CBM_LANG_CLOJURE
 *   Scheme       -> CBM_LANG_SCHEME
 *   Racket       -> CBM_LANG_RACKET
 *   Common Lisp  -> CBM_LANG_COMMONLISP
 *   Emacs Lisp   -> CBM_LANG_EMACSLISP   (note: not ELISP)
 *   Lean 4       -> CBM_LANG_LEAN
 *   Gleam        -> CBM_LANG_GLEAM
 *
 * BATTERY DIMENSIONS (mirror of repro_grammar_core.c)
 * -----------------------------------------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean   : inv_extract_clean(src,lang,file) == 1
 *   2. labels-valid    : inv_count_bad_labels(r) == 0
 *   3. fqn-wellformed  : inv_count_bad_fqns(r) == 0
 *   4. ranges-valid    : inv_count_bad_ranges(r) == 0
 *   5. defs-present    : inv_count_label(r, expect_label) > 0
 *   6. calls-extracted : inv_has_call(r, callee) == 1
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : module_sourced == 0 AND callable_sourced >= 1
 *   8. no-dangling       : inv_count_dangling_edges(store, project, "CALLS") == 0
 *
 * KNOWN GAPS (the point of this file)
 * -------------------------------------
 * Dimension 6 (calls-extracted) is RED for Elm: the scripting-callee path does
 * not yield a call name for Elm's function_call nodes on current code.
 *
 * Dimension 7 (callable-sourcing) is RED for all functional languages on current
 * code. cbm_enclosing_func_qn falls back to the module QN when
 * cbm_find_enclosing_func cannot match tree-sitter node types to
 * func_kinds_for_lang for the language (the same gap documented in
 * QUALITY_ANALYSIS.md section 6 / enclosing-func drift). Only ~3.69% of CALLS
 * edges are callable-sourced in the real graph; functional languages are not in
 * the known-GREEN set (Go/CUDA/D).
 *
 * RED rows ARE the deliverable: they document extraction gaps and serve as
 * permanent regression guards until the gaps are fixed.
 *
 * Coding rule: inline comments are line comments only (no block comments inside
 * block comments).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* -- Shared single-file battery (dimensions 1-6) --------------------------
 *
 * Runs the six single-file invariants against one fixture. Returns 0 when all
 * pass, 1 otherwise (printing a per-dimension FAIL line). lang_tag is for
 * diagnostics only. expect_label is the def label the fixture is guaranteed to
 * produce (e.g. "Function"); callee is the in-body callee name that must
 * appear in the extracted calls.
 */
static int single_file_battery(const char *lang_tag, const char *src,
                               CBMLanguage lang, const char *file,
                               const char *expect_label,
                               const char *callee) {
    const char *RED = tf_red();
    const char *RST = tf_reset();
    int fails = 0;

    /* 1. extract-clean -- must hold before anything else is meaningful. */
    if (inv_extract_clean(src, lang, file) != 1) {
        printf("  %sFAIL%s  [%s] extract-clean: NULL result or has_error set\n",
               RED, RST, lang_tag);
        return 1; /* nothing else can be trusted */
    }

    CBMFileResult *r = inv_rx(src, lang, file);
    if (!r) {
        printf("  %sFAIL%s  [%s] inv_rx returned NULL after clean extract\n",
               RED, RST, lang_tag);
        return 1;
    }

    /* 2. labels-valid */
    int bad_labels = inv_count_bad_labels(r);
    if (bad_labels != 0) {
        printf("  %sFAIL%s  [%s] labels-valid: %d def(s) with invalid label\n",
               RED, RST, lang_tag, bad_labels);
        fails++;
    }

    /* 3. fqn-wellformed */
    int bad_fqns = inv_count_bad_fqns(r);
    if (bad_fqns != 0) {
        printf("  %sFAIL%s  [%s] fqn-wellformed: %d def(s) with malformed QN\n",
               RED, RST, lang_tag, bad_fqns);
        fails++;
    }

    /* 4. ranges-valid */
    int bad_ranges = inv_count_bad_ranges(r);
    if (bad_ranges != 0) {
        printf("  %sFAIL%s  [%s] ranges-valid: %d def(s) with invalid range\n",
               RED, RST, lang_tag, bad_ranges);
        fails++;
    }

    /* 5. defs-present -- the function/definition the fixture wrote must be extracted. */
    if (expect_label && inv_count_label(r, expect_label) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label);
        fails++;
    }

    /* 6. calls-extracted -- the in-body call must be captured. */
    if (inv_has_call(r, callee) != 1) {
        printf("  %sFAIL%s  [%s] calls-extracted: no call to \"%s\" found"
               " -- known extraction gap\n",
               RED, RST, lang_tag, callee);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* -- Shared full-pipeline battery (dimensions 7-8) ------------------------
 *
 * Indexes the single-file fixture through the production pipeline and asserts
 * callable-sourcing (no Module-sourced in-body CALLS) and no dangling CALLS
 * edges. Returns 0 on PASS, 1 on FAIL. Dimension 7 is RED for all functional
 * languages on current code -- that is the intended signal.
 */
static int pipeline_battery(const char *lang_tag, const char *filename,
                            const char *src) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    RFile files[1];
    files[0].name = filename;
    files[0].content = src;

    RProj lp;
    cbm_store_t *store = rh_index_files(&lp, files, 1);
    if (!store) {
        printf("  %sFAIL%s  [%s] pipeline: rh_index_files returned NULL\n",
               RED, RST, lang_tag);
        return 1;
    }

    int fails = 0;

    /* 7. callable-sourcing -- mod must be 0; we also require >=1 callable-sourced
     * edge so a fixture that produced zero CALLS edges cannot vacuously pass. */
    int module_sourced = 0;
    int callable_sourced = 0;
    inv_count_calls_by_source(store, lp.project, &module_sourced,
                              &callable_sourced);
    if (module_sourced != 0) {
        printf("  %sFAIL%s  [%s] callable-sourcing: %d in-body CALLS sourced at "
               "Module (callable=%d) -- known enclosing-func gap\n",
               RED, RST, lang_tag, module_sourced, callable_sourced);
        fails++;
    } else if (callable_sourced < 1) {
        printf("  %sFAIL%s  [%s] callable-sourcing: 0 CALLS edges (fixture "
               "produced no in-body call edge to attribute)\n",
               RED, RST, lang_tag);
        fails++;
    }

    /* 8. no-dangling -- every CALLS edge endpoint must resolve. */
    int dangling = inv_count_dangling_edges(store, lp.project, "CALLS");
    if (dangling != 0) {
        printf("  %sFAIL%s  [%s] no-dangling: %d dangling CALLS endpoint(s)\n",
               RED, RST, lang_tag, dangling);
        fails++;
    }

    rh_cleanup(&lp, store);
    return fails ? 1 : 0;
}

/* -- Haskell ---------------------------------------------------------------
 * Idiomatic: module header, a helper function, a caller function whose body
 * applies the helper. Haskell function application is juxtaposition: `add x y`
 * inside the body of `compute` is the call. The tree-sitter-haskell grammar
 * emits `function` and `apply` nodes; extract_fp_callee handles `apply`.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (no cross-LSP rescue for Haskell;
 * func_kinds_for_lang drift causes enclosing-func walk to fall back to Module).
 */
TEST(repro_grammar_functional_haskell) {
    static const char src[] =
        "module Calc where\n"
        "\n"
        "add :: Int -> Int -> Int\n"
        "add a b = a + b\n"
        "\n"
        "compute :: Int -> Int\n"
        "compute x = add x 1\n";
    if (single_file_battery("Haskell", src, CBM_LANG_HASKELL, "Calc.hs",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Haskell", "Calc.hs", src);
}

/* -- OCaml -----------------------------------------------------------------
 * Idiomatic: two `let` bindings at module top level; the second binding's body
 * calls the first. OCaml `let f x = expr` is a `value_definition` node;
 * extract_fp_callee handles `application_expression`. Labels: "Function".
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (same enclosing-func gap).
 */
TEST(repro_grammar_functional_ocaml) {
    static const char src[] =
        "let add a b = a + b\n"
        "\n"
        "let compute x = add x 1\n";
    if (single_file_battery("OCaml", src, CBM_LANG_OCAML, "calc.ml",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("OCaml", "calc.ml", src);
}

/* -- F# --------------------------------------------------------------------
 * Idiomatic: two `let` bindings; the second calls the first inside its body.
 * F# `let f x = ...` is a `function_or_value_defn` node (or `value_declaration`
 * depending on grammar version); extract_fsharp_callee handles
 * `application_expression`. Labels: "Function".
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap applies;
 * no dedicated F# cross-LSP rescue).
 */
TEST(repro_grammar_functional_fsharp) {
    static const char src[] =
        "let add a b = a + b\n"
        "\n"
        "let compute x = add x 1\n";
    if (single_file_battery("F#", src, CBM_LANG_FSHARP, "Calc.fs",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("F#", "Calc.fs", src);
}

/* -- Elixir ----------------------------------------------------------------
 * Idiomatic: a module with two `def` clauses; the caller's body invokes the
 * helper. Elixir `def` is extracted as a "call" node by tree-sitter-elixir;
 * extract_calls.c has a special Elixir branch for "call" nodes that extracts
 * the callee. Labels: "Function" (elixir_func_types includes "call").
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap).
 */
TEST(repro_grammar_functional_elixir) {
    static const char src[] =
        "defmodule Calc do\n"
        "  def add(a, b), do: a + b\n"
        "\n"
        "  def compute(x) do\n"
        "    add(x, 1)\n"
        "  end\n"
        "end\n";
    if (single_file_battery("Elixir", src, CBM_LANG_ELIXIR, "calc.ex",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Elixir", "calc.ex", src);
}

/* -- Erlang ----------------------------------------------------------------
 * Idiomatic: a module attribute, an exported function, and a helper function.
 * The exported function's body calls the helper. Erlang function clauses are
 * `function_clause` nodes; extract_erlang_callee handles `call` nodes.
 * Labels: "Function" (erlang_func_types = {"function_clause"}).
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap applies;
 * Erlang is not in the known-GREEN callable-sourcing set).
 */
TEST(repro_grammar_functional_erlang) {
    static const char src[] =
        "-module(calc).\n"
        "-export([compute/1]).\n"
        "\n"
        "add(A, B) -> A + B.\n"
        "\n"
        "compute(X) ->\n"
        "    add(X, 1).\n";
    if (single_file_battery("Erlang", src, CBM_LANG_ERLANG, "calc.erl",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Erlang", "calc.erl", src);
}

/* -- Elm ------------------------------------------------------------------
 * Idiomatic: a module declaration, a helper function, and a caller function
 * whose body applies the helper. Elm `f x = body` is a `value_declaration`
 * node; elm_call_types = {"function_call", "function_call_expr"}. The call
 * extractor reaches extract_scripting_callee for Elm but currently does NOT
 * yield a callee name for Elm's function_call node -- dim 6 is RED.
 * Labels: "Function" (elm_func_types = {"value_declaration", ...}).
 * Expected: dims 1-5 + 8 GREEN, dim 6 RED (calls extraction gap -- this RED
 * assertion documents the gap), dim 7 RED (enclosing-func gap).
 */
TEST(repro_grammar_functional_elm) {
    static const char src[] =
        "module Calc exposing (compute)\n"
        "\n"
        "add : Int -> Int -> Int\n"
        "add a b =\n"
        "    a + b\n"
        "\n"
        "compute : Int -> Int\n"
        "compute x =\n"
        "    add x 1\n";
    if (single_file_battery("Elm", src, CBM_LANG_ELM, "Calc.elm",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Elm", "Calc.elm", src);
}

/* -- Clojure ---------------------------------------------------------------
 * Idiomatic: two `defn` forms; the second's body calls the first. In Clojure
 * both forms are `list_lit` nodes; `extract_lisp_def` labels them "Function".
 * `extract_lisp_callee` extracts the callee from the head of a `list_lit`.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap; Clojure is not
 * in the known-GREEN callable-sourcing set).
 */
TEST(repro_grammar_functional_clojure) {
    static const char src[] =
        "(defn add [a b]\n"
        "  (+ a b))\n"
        "\n"
        "(defn compute [x]\n"
        "  (add x 1))\n";
    if (single_file_battery("Clojure", src, CBM_LANG_CLOJURE, "calc.clj",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Clojure", "calc.clj", src);
}

/* -- Scheme ----------------------------------------------------------------
 * Idiomatic: two `define` forms; the second's body calls the first. In
 * tree-sitter-scheme both forms are `list` nodes; `extract_lisp_def` (triggered
 * by SCHEME in walk_defs) labels them "Function".
 * NOTE: CBM_LANG_SCHEME has func_types = empty_types, so extract_func_def is
 * never triggered; definitions only appear via extract_lisp_def. The callee
 * is extracted by extract_lisp_callee (SCHEME is in the lisp group).
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap -- SCHEME not
 * in func_kinds_for_lang known-GREEN set).
 */
TEST(repro_grammar_functional_scheme) {
    static const char src[] =
        "(define (add a b)\n"
        "  (+ a b))\n"
        "\n"
        "(define (compute x)\n"
        "  (add x 1))\n";
    if (single_file_battery("Scheme", src, CBM_LANG_SCHEME, "calc.scm",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Scheme", "calc.scm", src);
}

/* -- Racket ----------------------------------------------------------------
 * Idiomatic: a `#lang racket` reader directive, two `define` forms; the
 * second's body calls the first. tree-sitter-racket emits `list` nodes;
 * `extract_lisp_def` (triggered by RACKET in walk_defs) labels them "Function".
 * NOTE: CBM_LANG_RACKET has func_types = empty_types, so definitions only
 * appear via extract_lisp_def. extract_lisp_callee handles RACKET.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap -- RACKET not
 * in the known-GREEN callable-sourcing set).
 */
TEST(repro_grammar_functional_racket) {
    static const char src[] =
        "#lang racket\n"
        "\n"
        "(define (add a b)\n"
        "  (+ a b))\n"
        "\n"
        "(define (compute x)\n"
        "  (add x 1))\n";
    if (single_file_battery("Racket", src, CBM_LANG_RACKET, "calc.rkt",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Racket", "calc.rkt", src);
}

/* -- Common Lisp -----------------------------------------------------------
 * Idiomatic: two `defun` forms; the second's body calls the first. In
 * tree-sitter-commonlisp `defun` is the node kind; `commonlisp_func_types =
 * {"defun"}` triggers extract_func_def which labels it "Function".
 * extract_lisp_callee handles COMMONLISP.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap -- COMMONLISP
 * not in the known-GREEN callable-sourcing set).
 */
TEST(repro_grammar_functional_commonlisp) {
    static const char src[] =
        "(defun add (a b)\n"
        "  (+ a b))\n"
        "\n"
        "(defun compute (x)\n"
        "  (add x 1))\n";
    if (single_file_battery("Common Lisp", src, CBM_LANG_COMMONLISP, "calc.lisp",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Common Lisp", "calc.lisp", src);
}

/* -- Emacs Lisp ------------------------------------------------------------
 * Idiomatic: two `defun` forms; the second's body calls the first. In
 * tree-sitter-elisp `defun` is a `list` node with head "defun";
 * `elisp_func_types = {"function_definition", "macro_definition"}` triggers
 * extract_func_def. extract_lisp_callee handles EMACSLISP (in the lisp group).
 * Note: the enum is CBM_LANG_EMACSLISP (not ELISP).
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap -- EMACSLISP
 * not in the known-GREEN callable-sourcing set).
 */
TEST(repro_grammar_functional_emacslisp) {
    static const char src[] =
        "(defun add (a b)\n"
        "  (+ a b))\n"
        "\n"
        "(defun compute (x)\n"
        "  (add x 1))\n";
    if (single_file_battery("Emacs Lisp", src, CBM_LANG_EMACSLISP, "calc.el",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Emacs Lisp", "calc.el", src);
}

/* -- Lean 4 ----------------------------------------------------------------
 * Idiomatic: two `def` declarations; the second's body calls the first.
 * `lean_func_types = {"def", "theorem", "instance", "abbrev"}` triggers
 * extract_func_def which labels the definitions "Function". extract_calls.c
 * has a Lean-specific guard (lean_is_in_type_position) for `apply` nodes.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap -- Lean is not
 * in the known-GREEN callable-sourcing set).
 */
TEST(repro_grammar_functional_lean) {
    static const char src[] =
        "def add (a b : Nat) : Nat := a + b\n"
        "\n"
        "def compute (x : Nat) : Nat :=\n"
        "  add x 1\n";
    if (single_file_battery("Lean", src, CBM_LANG_LEAN, "Calc.lean",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Lean", "Calc.lean", src);
}

/* -- Gleam ----------------------------------------------------------------
 * Idiomatic: two `fn` declarations; the second's body calls the first.
 * `gleam_func_types = {"function", "anonymous_function", "external_function",
 * ...}` triggers extract_func_def which labels them "Function".
 * Call extraction reaches extract_scripting_callee (no gleam-specific branch in
 * extract_callee_lang_specific); gleam_call_types = {"function_call"}.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap -- Gleam not
 * in the known-GREEN callable-sourcing set).
 */
TEST(repro_grammar_functional_gleam) {
    static const char src[] =
        "fn add(a: Int, b: Int) -> Int {\n"
        "  a + b\n"
        "}\n"
        "\n"
        "fn compute(x: Int) -> Int {\n"
        "  add(x, 1)\n"
        "}\n";
    if (single_file_battery("Gleam", src, CBM_LANG_GLEAM, "calc.gleam",
                            "Function", "add") != 0)
        return 1;
    return pipeline_battery("Gleam", "calc.gleam", src);
}

/* -- Suite ---------------------------------------------------------------- */

SUITE(repro_grammar_functional) {
    RUN_TEST(repro_grammar_functional_haskell);
    RUN_TEST(repro_grammar_functional_ocaml);
    RUN_TEST(repro_grammar_functional_fsharp);
    RUN_TEST(repro_grammar_functional_elixir);
    RUN_TEST(repro_grammar_functional_erlang);
    RUN_TEST(repro_grammar_functional_elm);
    RUN_TEST(repro_grammar_functional_clojure);
    RUN_TEST(repro_grammar_functional_scheme);
    RUN_TEST(repro_grammar_functional_racket);
    RUN_TEST(repro_grammar_functional_commonlisp);
    RUN_TEST(repro_grammar_functional_emacslisp);
    RUN_TEST(repro_grammar_functional_lean);
    RUN_TEST(repro_grammar_functional_gleam);
}
