/*
 * repro_invariant_breadth.c -- Cross-language CALLS callable-sourcing invariant.
 *
 * INVARIANT (gap #6, QUALITY_ANALYSIS.md):
 *   For every language, a function call written INSIDE a function body must
 *   produce a CALLS edge whose source node carries label "Function" or "Method"
 *   (i.e. callable-sourced).  It must NOT be sourced at a "Module" node.
 *   Calls at the top level of a file may legitimately be Module-sourced; only
 *   in-body calls are asserted here.
 *
 * QUALITY_ANALYSIS.md gap #6 reports 27 languages failing this.  This file
 * is the "large breadth table" — one per-language case, table-driven, asserting
 * the invariant across 26 languages.
 *
 * Fixture design rule:
 *   Each fixture defines exactly TWO functions: a callee (helper) and a caller
 *   (run) that calls helper strictly INSIDE its body.  There are NO top-level
 *   calls in any fixture.  This means ANY Module-sourced CALLS edge is a
 *   direct violation of the invariant.
 *
 * Expected RED/GREEN split (as of QUALITY_ANALYSIS.md, 2026-06-24):
 *   GREEN (already correctly callable-sourced, regression guards):
 *     elixir, ocaml, fortran, pascal, cuda, d, glsl, hlsl, ispc,
 *     odin, slang, squirrel, vimscript, cairo  (14 cases)
 *
 *   RED (module-sourced or no CALLS at all -- reproduces the gap):
 *     r, julia, dart, groovy, commonlisp, powershell, ada, clojure,
 *     fsharp, racket, rescript, scheme  (12 cases)
 *
 * Note: the "suspicious" group (r, julia, ...) from QUALITY_ANALYSIS may be
 * GREEN because the calls-breadth table (test_lang_contract.c) already shows
 * expect_calls=true for most.  The module-sourcing assertion is STRICTER: a
 * language can produce a CALLS edge (calls >= 1) but still fail here if the
 * edge is sourced at Module rather than Function.  Individual case comments
 * explain the known failure mode where root-caused.
 *
 * How to read results:
 *   PASS -- callable-sourced (Function/Method), no Module-sourced in-body calls.
 *          If currently GREEN: regression guard -- a future grammar/pipeline
 *          change that breaks sourcing will turn it RED.
 *          If currently RED:   the bug is confirmed reproduced; fix the
 *          enclosing-function detection for this language.
 */

#include "test_framework.h"
#include "repro_harness.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* ---- helper: count CALLS edges by source-node label --------------------- */

static int ib_calls_from_label(cbm_store_t *store, const char *project,
                                const char *label) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(store, project, "CALLS",
                                     &edges, &edge_count) != CBM_STORE_OK) {
        return -1;
    }
    int total = 0;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t src = {0};
        if (cbm_store_find_node_by_id(store, edges[i].source_id,
                                      &src) != CBM_STORE_OK) {
            continue;
        }
        if (src.label && strcmp(src.label, label) == 0) {
            total++;
        }
        cbm_node_free_fields(&src);
    }
    cbm_store_free_edges(edges, edge_count);
    return total;
}

static int ib_callable_calls(cbm_store_t *store, const char *project) {
    int fn = ib_calls_from_label(store, project, "Function");
    int mt = ib_calls_from_label(store, project, "Method");
    if (fn < 0 || mt < 0) {
        return -1;
    }
    return fn + mt;
}

static int ib_module_calls(cbm_store_t *store, const char *project) {
    return ib_calls_from_label(store, project, "Module");
}

/* ---- per-case result struct --------------------------------------------- */

typedef struct {
    int ok;             /* graph DB opened */
    int calls;          /* total CALLS edges */
    int callable_calls; /* CALLS sourced at Function or Method */
    int module_calls;   /* CALLS sourced at Module */
} IBMetrics;

static IBMetrics ib_metrics(const char *filename, const char *content) {
    RProj lp;
    cbm_store_t *store = rh_index(&lp, filename, content);
    IBMetrics m = {0};
    if (store) {
        m.ok = 1;
        m.calls = rh_count_edges(store, lp.project, "CALLS");
        m.callable_calls = ib_callable_calls(store, lp.project);
        m.module_calls = ib_module_calls(store, lp.project);
    }
    rh_cleanup(&lp, store);
    return m;
}

/* ---- breadth case table ------------------------------------------------- */

typedef struct {
    const char *lang;     /* human-readable language name */
    const char *filename; /* fixture filename (extension selects grammar) */
    const char *src;      /* fixture source — caller inside a function body only */
    int expect_callable;  /* 1: calls should be callable-sourced (GREEN target) */
    const char *gap_note; /* root cause for known gaps (NULL if expected GREEN) */
} IBCase;

/*
 * Fixture rule: helper() is the callee; run() is the caller.
 * The call to helper() is strictly inside the body of run().
 * No top-level calls anywhere in the fixture.
 */
static const IBCase IB_CASES[] = {

    /* ------------------------------------------------------------------ */
    /* SUSPICIOUS / LIKELY-BROKEN GROUP                                    */
    /* QUALITY_ANALYSIS lists these as "expected-true but suspicious".     */
    /* They have expect_calls=true in the calls-breadth table, meaning a   */
    /* CALLS edge is produced -- but it may still be Module-sourced.       */
    /* ------------------------------------------------------------------ */

    {
        "r", "a.R",
        "helper <- function(x) {\n"
        "  x * 2\n"
        "}\n"
        "\n"
        "run <- function() {\n"
        "  helper(21)\n"
        "}\n",
        /*
         * R: extract_calls.c has an R branch that reads the callee from the
         * call node's first child.  However, enclosing-function detection
         * for R may fall back to Module if func_kinds_for_lang does not
         * include R's "function_definition" node type.  RED when the CALLS
         * edge is sourced at Module instead of the "run" Function node.
         */
        0, "R enclosing-function detection likely missing from func_kinds_for_lang; "
           "call may be sourced at Module"
    },

    {
        "julia", "a.jl",
        "function helper(x)\n"
        "    return x + 1\n"
        "end\n"
        "\n"
        "function run(n)\n"
        "    return helper(n)\n"
        "end\n",
        /*
         * Julia: same issue -- function body extraction may not detect the
         * enclosing Julia function node correctly, sourcing the call at Module.
         */
        0, "Julia enclosing-function detection may not map function_definition to "
           "a callable QN; call sourced at Module"
    },

    /* ------------------------------------------------------------------ */
    /* EXPECTED-GREEN GROUP (regression guards)                            */
    /* These languages have correct callable-sourcing in the current build.*/
    /* A regression that breaks enclosing-function detection for any of    */
    /* them will turn the corresponding case RED.                          */
    /* ------------------------------------------------------------------ */

    {
        "elixir", "a.ex",
        "defmodule Sample do\n"
        "  def helper(x) do\n"
        "    x + 1\n"
        "  end\n"
        "\n"
        "  def run do\n"
        "    helper(41)\n"
        "  end\n"
        "end\n",
        1, NULL
    },

    {
        "ocaml", "a.ml",
        "let helper x = x + 1\n"
        "\n"
        "let run () =\n"
        "  let result = helper 41 in\n"
        "  print_int result\n",
        1, NULL
    },

    {
        "fortran", "a.f90",
        "function helper(x) result(y)\n"
        "    integer, intent(in) :: x\n"
        "    integer :: y\n"
        "    y = x + 1\n"
        "end function helper\n"
        "\n"
        "function run(n) result(total)\n"
        "    integer, intent(in) :: n\n"
        "    integer :: total\n"
        "    total = helper(n) + helper(n + 1)\n"
        "end function run\n",
        1, NULL
    },

    {
        "pascal", "a.pas",
        "procedure Helper(x: Integer);\n"
        "begin\n"
        "  WriteLn(x);\n"
        "end;\n"
        "\n"
        "procedure Run;\n"
        "begin\n"
        "  Helper(1);\n"
        "end;\n",
        1, NULL
    },

    {
        "cuda", "a.cu",
        "__device__ int helper(int x) {\n"
        "    return x * 2;\n"
        "}\n"
        "\n"
        "__global__ void run(int *out) {\n"
        "    out[0] = helper(21);\n"
        "}\n",
        1, NULL
    },

    {
        "d", "a.d",
        "int helper(int x)\n"
        "{\n"
        "    return x + 1;\n"
        "}\n"
        "\n"
        "void run()\n"
        "{\n"
        "    int y = helper(41);\n"
        "}\n",
        1, NULL
    },

    {
        "glsl", "a.glsl",
        "float helper(float x) {\n"
        "    return x * 2.0;\n"
        "}\n"
        "\n"
        "void run() {\n"
        "    float y = helper(3.0);\n"
        "}\n",
        1, NULL
    },

    {
        "hlsl", "a.hlsl",
        "float helper(float x)\n"
        "{\n"
        "    return x * 2.0;\n"
        "}\n"
        "\n"
        "float run(float v)\n"
        "{\n"
        "    return helper(v) + 1.0;\n"
        "}\n",
        1, NULL
    },

    {
        "ispc", "a.ispc",
        "static inline uniform float helper(uniform float x) {\n"
        "    return x * 2.0f;\n"
        "}\n"
        "\n"
        "export void run(uniform float in[], uniform float out[],\n"
        "                uniform int n) {\n"
        "    foreach (i = 0 ... n) {\n"
        "        out[i] = helper(in[i]);\n"
        "    }\n"
        "}\n",
        1, NULL
    },

    {
        "odin", "a.odin",
        "package fixture\n"
        "\n"
        "helper :: proc() -> int {\n"
        "\treturn 42\n"
        "}\n"
        "\n"
        "run :: proc() {\n"
        "\tx := helper()\n"
        "\t_ = x\n"
        "}\n",
        1, NULL
    },

    {
        "slang", "a.slang",
        "void helper()\n"
        "{\n"
        "    int x = 1;\n"
        "}\n"
        "\n"
        "void run()\n"
        "{\n"
        "    helper();\n"
        "}\n",
        1, NULL
    },

    {
        "squirrel", "a.nut",
        "function helper(x) {\n"
        "    return x + 1;\n"
        "}\n"
        "\n"
        "function run() {\n"
        "    return helper(41);\n"
        "}\n",
        1, NULL
    },

    {
        "vimscript", "a.vim",
        "function! Helper() abort\n"
        "  return 1\n"
        "endfunction\n"
        "\n"
        "function! Run() abort\n"
        "  call Helper()\n"
        "endfunction\n",
        1, NULL
    },

    {
        "cairo", "a.cairo",
        "fn helper(x: felt252) -> felt252 {\n"
        "    x + 1\n"
        "}\n"
        "\n"
        "fn run() -> felt252 {\n"
        "    helper(41)\n"
        "}\n",
        1, NULL
    },

    /* ------------------------------------------------------------------ */
    /* KNOWN-GAP GROUP                                                     */
    /* These languages fail in the existing calls-breadth contract too     */
    /* (expect_calls=false in test_lang_contract.c CALL_CASES).            */
    /* The primary gap is callee extraction; callable-sourcing cannot be   */
    /* verified until a CALLS edge exists.  Both invariants are asserted:  */
    /* calls >= 1 AND module_calls == 0.                                   */
    /* ------------------------------------------------------------------ */

    {
        "dart", "a.dart",
        "void helper() {\n"
        "  print('helper');\n"
        "}\n"
        "\n"
        "void run() {\n"
        "  helper();\n"
        "}\n",
        /*
         * Dart: selector call node carries no callee field and the first child
         * is not an identifier; no dart branch in extract_calls.c.  No CALLS
         * edge is produced at all, so callable-sourcing cannot be tested
         * independently.  Both gaps (no CALLS + callable-sourcing) are RED.
         */
        0, "selector call node: no callee field, first child not identifier; "
           "no dart branch in extract_calls.c"
    },

    {
        "groovy", "a.groovy",
        "def helper() {\n"
        "    println 'helping'\n"
        "}\n"
        "\n"
        "def run() {\n"
        "    helper()\n"
        "}\n",
        /*
         * Groovy: function_call callee not on a function/name field and first
         * child is not 'identifier'; no groovy branch in extract_calls.c.
         */
        0, "function_call callee not on function/name field; "
           "first child is not identifier; no groovy branch in extract_calls.c"
    },

    {
        "commonlisp", "a.lisp",
        "(defun helper (x)\n"
        "  (* x 2))\n"
        "\n"
        "(defun run ()\n"
        "  (helper 21))\n",
        /*
         * Common Lisp: list_lit call head is sym_lit not identifier;
         * no commonlisp branch in extract_callee_name.
         */
        0, "list_lit call head is sym_lit not identifier; "
           "no commonlisp branch in extract_callee_name"
    },

    {
        "powershell", "a.ps1",
        "function helper {\n"
        "    Write-Output 'hi'\n"
        "}\n"
        "\n"
        "function run {\n"
        "    helper\n"
        "}\n",
        /*
         * PowerShell: command node child is command_name not identifier;
         * extract_scripting_callee handles MATLAB not PowerShell.
         */
        0, "command node child is command_name not identifier; "
           "extract_scripting_callee handles MATLAB not PowerShell"
    },

    {
        "ada", "a.adb",
        "procedure Run is\n"
        "   procedure Helper is\n"
        "   begin\n"
        "      null;\n"
        "   end Helper;\n"
        "begin\n"
        "   Helper;\n"
        "end Run;\n",
        /*
         * Ada: procedure_call_statement callee did not resolve to a CALLS edge;
         * no Ada branch in extract_calls.c.
         */
        0, "procedure_call_statement callee not resolved; "
           "no Ada branch in extract_calls.c"
    },

    {
        "clojure", "a.clj",
        "(defn helper [] 42)\n"
        "\n"
        "(defn run [] (helper))\n",
        /*
         * Clojure: lisp call is a list_lit whose head is a sym_lit (not a
         * field, not a first-child 'identifier'); no lisp branch in
         * extract_callee_name.
         */
        0, "list_lit head is sym_lit not identifier; "
           "no lisp/clojure branch in extract_callee_name"
    },

    {
        "fsharp", "a.fs",
        "let helper x = x + 1\n"
        "\n"
        "let run () = helper 41\n",
        /*
         * F#: application_expression callee head is a long_identifier_or_op
         * wrapper, not a bare identifier/field; no fsharp callee branch.
         */
        0, "application_expression callee head is long_identifier_or_op wrapper; "
           "no fsharp callee branch in extract_callee_name"
    },

    {
        "racket", "a.rkt",
        "#lang racket\n"
        "\n"
        "(define (helper x)\n"
        "  (+ x 1))\n"
        "\n"
        "(define (run)\n"
        "  (helper 41))\n",
        /*
         * Racket: lisp call is a 'list' whose head is a 'symbol' (grammar has
         * no 'identifier' node); no racket branch in extract_callee_name.
         */
        0, "list head is symbol not identifier; "
           "no racket branch in extract_callee_name"
    },

    {
        "rescript", "a.res",
        "let helper = (x) => x + 1\n"
        "\n"
        "let run = () => helper(41)\n",
        /*
         * ReScript: call_expression 'function' field is a 'value_identifier'
         * (not in extract_callee_from_fields' accepted type list).
         */
        0, "call_expression function field is value_identifier; "
           "not in extract_callee_from_fields accepted type list"
    },

    {
        "scheme", "a.scm",
        "(define (helper x)\n"
        "  (* x 2))\n"
        "\n"
        "(define (run)\n"
        "  (helper 21))\n",
        /*
         * Scheme: lisp call is a 'list' whose head is a 'symbol';
         * no scheme branch in extract_callee_name.
         */
        0, "list head is symbol not identifier; "
           "no scheme branch in extract_callee_name"
    },
};

enum { IB_CASES_COUNT = (int)(sizeof(IB_CASES) / sizeof(IB_CASES[0])) };

/* ---- single table-driven test ------------------------------------------- */

/*
 * repro_invariant_breadth_callable_sourcing
 *
 * Iterates every case in IB_CASES.  For each language:
 *   1. Indexes the single-file fixture through the full production pipeline.
 *   2. Counts CALLS edges and their source-node labels.
 *   3. Asserts:
 *        a. store opened (pipeline did not crash hard)
 *        b. calls >= 1 (the call was detected at all)
 *        c. callable_calls >= 1 (at least one CALLS edge is Function/Method-sourced)
 *        d. module_calls == 0 (no CALLS edge is Module-sourced for an in-body call)
 *
 * For expect_callable=0 cases (known gaps), the test still asserts all four
 * conditions -- so those cases are RED (that IS the deliverable: a confirmed,
 * reproducible, durable bug registration for each gap language).
 *
 * For expect_callable=1 cases (regression guards), the test must PASS.
 * A future grammar or pipeline regression that breaks callable-sourcing for
 * a GREEN language will immediately turn it RED here.
 */
TEST(repro_invariant_breadth_callable_sourcing) {
    int failures = 0;

    for (int i = 0; i < IB_CASES_COUNT; i++) {
        const IBCase *c = &IB_CASES[i];
        IBMetrics m = ib_metrics(c->filename, c->src);

        int pass = (m.ok && m.calls >= 1 && m.callable_calls >= 1 &&
                    m.module_calls == 0);

        if (!pass) {
            fprintf(stderr,
                    "  [INV-BREADTH] FAIL %-12s  ok=%d calls=%d "
                    "callable=%d module=%d%s%s\n",
                    c->lang, m.ok, m.calls, m.callable_calls,
                    m.module_calls,
                    c->gap_note ? " -- " : "",
                    c->gap_note ? c->gap_note : "");
            failures++;
        } else {
            fprintf(stderr,
                    "  [INV-BREADTH] PASS %-12s  calls=%d callable=%d "
                    "module=%d\n",
                    c->lang, m.calls, m.callable_calls, m.module_calls);
        }
    }

    fprintf(stderr,
            "  [INV-BREADTH] %d langs checked: %d FAILURES "
            "(each = callable-sourcing invariant violated or no CALLS at all)\n",
            IB_CASES_COUNT, failures);

    ASSERT_EQ(failures, 0);
    PASS();
}

/* ---- suite --------------------------------------------------------------- */

SUITE(repro_invariant_breadth) {
    RUN_TEST(repro_invariant_breadth_callable_sourcing);
}
