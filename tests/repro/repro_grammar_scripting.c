/*
 * repro_grammar_scripting.c -- Exhaustive per-grammar INVARIANT battery for the
 * SCRIPTING / DYNAMIC language family.
 *
 * Mirror of repro_grammar_core.c (same helpers, same per-language battery, same
 * DRY single-file + pipeline runners). One TEST() per language so per-language
 * RED/GREEN shows on the bug-repro board. Each test runs the SAME battery
 * against a tiny idiomatic fixture for that language (a function/method that
 * CALLS another function strictly inside its body, a class where the language
 * has one idiomatically, and an idiomatic import where the language has one).
 *
 * Languages covered (12) and the CBM_LANG_* enum each uses:
 *   Python      -> CBM_LANG_PYTHON
 *   Ruby        -> CBM_LANG_RUBY
 *   PHP         -> CBM_LANG_PHP
 *   JavaScript  -> CBM_LANG_JAVASCRIPT
 *   TypeScript  -> CBM_LANG_TYPESCRIPT
 *   TSX         -> CBM_LANG_TSX
 *   Lua         -> CBM_LANG_LUA
 *   Perl        -> CBM_LANG_PERL
 *   R           -> CBM_LANG_R
 *   Julia       -> CBM_LANG_JULIA
 *   Groovy      -> CBM_LANG_GROOVY
 *   Dart        -> CBM_LANG_DART
 *
 * BATTERY DIMENSIONS
 * ------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean   : inv_extract_clean(src,lang,file) == 1
 *                        (parser returned a result and did not set has_error;
 *                        a hard crash would not return at all).
 *   2. labels-valid    : inv_count_bad_labels(r) == 0   (every def label is in
 *                        the known label set).
 *   3. fqn-wellformed  : inv_count_bad_fqns(r) == 0      (no empty/".."/leading
 *                        or trailing '.'/whitespace QNs).
 *   4. ranges-valid    : inv_count_bad_ranges(r) == 0    (start_line >= 1 and
 *                        start_line <= end_line for every def).
 *   5. defs-present    : the function/class written in the fixture is extracted
 *                        (inv_count_label for the expected def labels > 0).
 *   6. calls-extracted : inv_has_call(r, "<callee>") == 1 (the in-body call was
 *                        captured).
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : inv_count_calls_by_source(store,project,&mod,&call);
 *                          assert mod == 0 -- every in-body call must be sourced
 *                          at a Function/Method node, NEVER at a Module node.
 *   8. no-dangling       : inv_count_dangling_edges(store,project,"CALLS") == 0
 *                          (every CALLS edge resolves both endpoints).
 *
 * EXPECTED RED/GREEN (dimension 7, callable-sourcing), per QUALITY_ANALYSIS.md
 * (2026-06-24), repro_invariant_calls.c, repro_invariant_breadth.c, and
 * repro_invariant_enclosing_parity.c:
 *   GREEN (callable-sourced; regression guards):
 *     Python     -- func_kinds_python = {function_definition}; grep-validated
 *                   correct in QUALITY_ANALYSIS.
 *     JavaScript -- func_kinds_js = {function_declaration, method_definition,
 *                   arrow_function, ...}; the simplest free-function case is
 *                   expected callable-sourced.
 *     TypeScript -- shares func_kinds_js; simplest free-function case expected
 *                   GREEN (the real-graph ts_lsp gap is for more complex bodies).
 *     TSX        -- shares the TS/JS func_kinds; same expectation as TypeScript.
 *     Lua        -- in the enclosing-func switch (repro_invariant_enclosing_
 *                   parity.c); enclosing detection supported.
 *     Ruby       -- in the enclosing-func switch; method bodies source callably.
 *     PHP        -- in the enclosing-func switch; PHP LSP is hybrid; method/
 *                   function bodies source callably.
 *   RED (module-sourced or no CALLS at all -- reproduces the gap):
 *     Perl       -- NOT in the enclosing-func switch; its enclosing-func drift
 *                   symptom is the documented Perl gap (repro_invariant_graph.c
 *                   INVARIANT 4). The in-body call is sourced at Module.
 *     R          -- "R enclosing-function detection likely missing from
 *                   func_kinds_for_lang; call sourced at Module" (breadth file).
 *     Julia      -- "Julia enclosing-function detection may not map
 *                   function_definition to a callable QN; call sourced at
 *                   Module" (breadth file).
 *     Groovy     -- function_call callee not on a function/name field; no groovy
 *                   branch in extract_calls.c -- likely no in-body CALLS edge,
 *                   so dimension 7 cannot reach >=1 callable-sourced (RED).
 *     Dart       -- selector call node carries no callee field; no dart branch
 *                   in extract_calls.c -- likely no in-body CALLS edge (RED).
 *
 * Dimensions 1-6 and 8 are expected GREEN for these idiomatic fixtures across
 * all 12 languages; dimension 7 is the deliverable RED signal for Perl/R/Julia/
 * Groovy/Dart and the GREEN regression guard for Python/JS/TS/TSX/Lua/Ruby/PHP.
 *
 * Coding rule: inline comments are line comments only (no block comments inside
 * block comments).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* ── Shared single-file battery (dimensions 1-6) ────────────────────────────
 *
 * Runs the six single-file invariants against one fixture. Returns 0 when all
 * pass, 1 otherwise (printing a per-dimension FAIL line). lang_tag is for
 * diagnostics only. expect_label / expect_label2 are def labels the fixture is
 * guaranteed to produce (e.g. "Function" and "Class"); pass NULL for
 * expect_label2 when the language has no class in the fixture. callee is the
 * in-body callee name that must appear in the extracted calls.
 */
static int single_file_battery(const char *lang_tag, const char *src,
                               CBMLanguage lang, const char *file,
                               const char *expect_label,
                               const char *expect_label2, const char *callee) {
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

    /* 5. defs-present -- the function/class the fixture wrote must be extracted. */
    if (expect_label && inv_count_label(r, expect_label) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label);
        fails++;
    }
    if (expect_label2 && inv_count_label(r, expect_label2) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label2);
        fails++;
    }

    /* 6. calls-extracted -- the in-body call must be captured. */
    if (inv_has_call(r, callee) != 1) {
        printf("  %sFAIL%s  [%s] calls-extracted: no call to \"%s\" found\n",
               RED, RST, lang_tag, callee);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Shared full-pipeline battery (dimensions 7-8) ──────────────────────────
 *
 * Indexes the single-file fixture through the production pipeline and asserts
 * callable-sourcing (no Module-sourced in-body CALLS) and no dangling CALLS
 * edges. Returns 0 on PASS, 1 on FAIL. Dimension 7 is RED for the dynamic
 * languages whose enclosing-func detection or call extraction is missing
 * (Perl/R/Julia/Groovy/Dart) -- that is the intended signal.
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

/* ── Python ─────────────────────────────────────────────────────────────────
 * Idiomatic: import, a free function, a class with a method, in-body call.
 * Expected GREEN across the battery including dim 7 (func_kinds_python =
 * {function_definition}; grep-validated correct). Regression guard: if dim 7
 * goes RED, Python callable attribution has broken.
 */
TEST(repro_grammar_scripting_python) {
    static const char src[] =
        "import os\n"
        "\n"
        "def add(a, b):\n"
        "    return a + b\n"
        "\n"
        "class Calc:\n"
        "    def compute(self, x):\n"
        "        return add(x, 1)\n";
    if (single_file_battery("Python", src, CBM_LANG_PYTHON, "calc.py",
                            "Function", "Class", "add") != 0)
        return 1;
    return pipeline_battery("Python", "calc.py", src);
}

/* ── Ruby ────────────────────────────────────────────────────────────────────
 * Idiomatic: require, a class with two methods, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 GREEN (Ruby is in the enclosing-func
 * switch; method bodies source callably). Regression guard.
 */
TEST(repro_grammar_scripting_ruby) {
    static const char src[] =
        "require 'set'\n"
        "\n"
        "class Calculator\n"
        "  def add(a, b)\n"
        "    a + b\n"
        "  end\n"
        "\n"
        "  def compute(x)\n"
        "    add(x, 1)\n"
        "  end\n"
        "end\n";
    if (single_file_battery("Ruby", src, CBM_LANG_RUBY, "calc.rb",
                            "Method", "Class", "add") != 0)
        return 1;
    return pipeline_battery("Ruby", "calc.rb", src);
}

/* ── PHP ──────────────────────────────────────────────────────────────────────
 * Idiomatic: <?php tag, a class with two methods, in-body call via $this.
 * Expected: dims 1-6 + 8 GREEN, dim 7 GREEN (PHP is in the enclosing-func
 * switch; PHP LSP is hybrid). The callee is the same-class method `add`.
 */
TEST(repro_grammar_scripting_php) {
    static const char src[] =
        "<?php\n"
        "\n"
        "class Calculator {\n"
        "    private function add($a, $b) {\n"
        "        return $a + $b;\n"
        "    }\n"
        "\n"
        "    public function compute($x) {\n"
        "        return $this->add($x, 1);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("PHP", src, CBM_LANG_PHP, "Calculator.php",
                            "Method", "Class", "add") != 0)
        return 1;
    return pipeline_battery("PHP", "Calculator.php", src);
}

/* ── JavaScript ───────────────────────────────────────────────────────────────
 * Idiomatic: import, a free function, a class with a method, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 GREEN (func_kinds_js supports
 * function_declaration + method_definition; the simplest free-function call is
 * callable-sourced).
 */
TEST(repro_grammar_scripting_javascript) {
    static const char src[] =
        "import fs from 'fs';\n"
        "\n"
        "function add(a, b) {\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "class Calculator {\n"
        "    compute(x) {\n"
        "        return add(x, 1);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("JavaScript", src, CBM_LANG_JAVASCRIPT, "calc.js",
                            "Function", "Class", "add") != 0)
        return 1;
    return pipeline_battery("JavaScript", "calc.js", src);
}

/* ── TypeScript ───────────────────────────────────────────────────────────────
 * Idiomatic: import, a typed free function, a class with a method, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 GREEN for this simplest case (shares
 * func_kinds_js). The real-graph ts_lsp Module-sourced gap is for more complex
 * bodies; if this still fails the test documents it.
 */
TEST(repro_grammar_scripting_typescript) {
    static const char src[] =
        "import { readFileSync } from 'fs';\n"
        "\n"
        "function add(a: number, b: number): number {\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "class Calculator {\n"
        "    compute(x: number): number {\n"
        "        return add(x, 1);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("TypeScript", src, CBM_LANG_TYPESCRIPT, "calc.ts",
                            "Function", "Class", "add") != 0)
        return 1;
    return pipeline_battery("TypeScript", "calc.ts", src);
}

/* ── TSX ──────────────────────────────────────────────────────────────────────
 * Idiomatic: import, a typed free function, a component class with a method
 * returning JSX, in-body call. Expected: dims 1-6 + 8 GREEN, dim 7 GREEN
 * (shares the TS/JS func_kinds). Uses CBM_LANG_TSX with a .tsx file.
 */
TEST(repro_grammar_scripting_tsx) {
    static const char src[] =
        "import React from 'react';\n"
        "\n"
        "function add(a: number, b: number): number {\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "class Widget extends React.Component {\n"
        "    compute(x: number): number {\n"
        "        return add(x, 1);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("TSX", src, CBM_LANG_TSX, "Widget.tsx",
                            "Function", "Class", "add") != 0)
        return 1;
    return pipeline_battery("TSX", "Widget.tsx", src);
}

/* ── Lua ──────────────────────────────────────────────────────────────────────
 * Idiomatic: require, a local function, a module-style function whose body calls
 * the helper. Lua has no idiomatic class keyword, so no expect_label2.
 * Expected: dims 1-6 + 8 GREEN, dim 7 GREEN (Lua is in the enclosing-func
 * switch; function bodies source callably).
 */
TEST(repro_grammar_scripting_lua) {
    static const char src[] =
        "local math = require('math')\n"
        "\n"
        "local function add(a, b)\n"
        "    return a + b\n"
        "end\n"
        "\n"
        "function compute(x)\n"
        "    return add(x, 1)\n"
        "end\n";
    if (single_file_battery("Lua", src, CBM_LANG_LUA, "calc.lua",
                            "Function", NULL, "add") != 0)
        return 1;
    return pipeline_battery("Lua", "calc.lua", src);
}

/* ── Perl ─────────────────────────────────────────────────────────────────────
 * Idiomatic: use pragma, two subs, the callee called strictly inside the caller
 * sub body. Perl has no idiomatic class in this fixture (no expect_label2).
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (Perl is NOT in the enclosing-func
 * switch; its enclosing-func drift is the documented Perl gap -- the in-body
 * call is sourced at Module). RED dim-7 IS the deliverable.
 */
TEST(repro_grammar_scripting_perl) {
    static const char src[] =
        "use strict;\n"
        "\n"
        "sub add {\n"
        "    my ($a, $b) = @_;\n"
        "    return $a + $b;\n"
        "}\n"
        "\n"
        "sub compute {\n"
        "    my ($x) = @_;\n"
        "    return add($x, 1);\n"
        "}\n";
    if (single_file_battery("Perl", src, CBM_LANG_PERL, "calc.pl",
                            "Function", NULL, "add") != 0)
        return 1;
    return pipeline_battery("Perl", "calc.pl", src);
}

/* ── R ────────────────────────────────────────────────────────────────────────
 * Idiomatic: library() load, two function assignments, the callee called inside
 * the caller's body. R has no idiomatic class in this fixture (no expect_label2).
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED ("R enclosing-function detection
 * likely missing from func_kinds_for_lang; call sourced at Module" per the
 * breadth file). RED dim-7 IS the deliverable.
 */
TEST(repro_grammar_scripting_r) {
    static const char src[] =
        "library(stats)\n"
        "\n"
        "add <- function(a, b) {\n"
        "    a + b\n"
        "}\n"
        "\n"
        "compute <- function(x) {\n"
        "    add(x, 1)\n"
        "}\n";
    if (single_file_battery("R", src, CBM_LANG_R, "calc.R",
                            "Function", NULL, "add") != 0)
        return 1;
    return pipeline_battery("R", "calc.R", src);
}

/* ── Julia ────────────────────────────────────────────────────────────────────
 * Idiomatic: using, two functions, the callee called inside the caller body.
 * Julia structs are idiomatic but methods are free functions, so the fixture
 * asserts on Function only (no expect_label2). Expected: dims 1-6 + 8 GREEN,
 * dim 7 RED ("Julia enclosing-function detection may not map
 * function_definition to a callable QN; call sourced at Module" per breadth
 * file). RED dim-7 IS the deliverable.
 */
TEST(repro_grammar_scripting_julia) {
    static const char src[] =
        "using Printf\n"
        "\n"
        "function add(a, b)\n"
        "    return a + b\n"
        "end\n"
        "\n"
        "function compute(x)\n"
        "    return add(x, 1)\n"
        "end\n";
    if (single_file_battery("Julia", src, CBM_LANG_JULIA, "calc.jl",
                            "Function", NULL, "add") != 0)
        return 1;
    return pipeline_battery("Julia", "calc.jl", src);
}

/* ── Groovy ───────────────────────────────────────────────────────────────────
 * Idiomatic: import, a class with two methods, in-body call.
 * Expected: dims 1-5 + 8 GREEN. Dim 6 (calls-extracted) and dim 7 are RED:
 * "function_call callee not on a function/name field and first child is not
 * 'identifier'; no groovy branch in extract_calls.c" (breadth file), so the
 * in-body call may not be captured and no callable-sourced CALLS edge is
 * produced. RED IS the deliverable. (single_file_battery returns early on the
 * dim-6 miss; pipeline dim-7 likewise fails on 0 callable edges.)
 */
TEST(repro_grammar_scripting_groovy) {
    static const char src[] =
        "import groovy.transform.CompileStatic\n"
        "\n"
        "class Calculator {\n"
        "    int add(int a, int b) {\n"
        "        return a + b\n"
        "    }\n"
        "\n"
        "    int compute(int x) {\n"
        "        return add(x, 1)\n"
        "    }\n"
        "}\n";
    if (single_file_battery("Groovy", src, CBM_LANG_GROOVY, "Calculator.groovy",
                            "Method", "Class", "add") != 0)
        return 1;
    return pipeline_battery("Groovy", "Calculator.groovy", src);
}

/* ── Dart ─────────────────────────────────────────────────────────────────────
 * Idiomatic: import, a class with two methods, in-body call.
 * Expected: dims 1-5 + 8 GREEN. Dim 6 (calls-extracted) and dim 7 are RED:
 * "selector call node carries no callee field and the first child is not an
 * identifier; no dart branch in extract_calls.c" (breadth file), so no in-body
 * CALLS edge is produced. RED IS the deliverable. Uses CBM_LANG_DART.
 */
TEST(repro_grammar_scripting_dart) {
    static const char src[] =
        "import 'dart:math';\n"
        "\n"
        "class Calculator {\n"
        "  int add(int a, int b) {\n"
        "    return a + b;\n"
        "  }\n"
        "\n"
        "  int compute(int x) {\n"
        "    return add(x, 1);\n"
        "  }\n"
        "}\n";
    if (single_file_battery("Dart", src, CBM_LANG_DART, "calc.dart",
                            "Method", "Class", "add") != 0)
        return 1;
    return pipeline_battery("Dart", "calc.dart", src);
}

/* ── Suite ──────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_scripting) {
    RUN_TEST(repro_grammar_scripting_python);
    RUN_TEST(repro_grammar_scripting_ruby);
    RUN_TEST(repro_grammar_scripting_php);
    RUN_TEST(repro_grammar_scripting_javascript);
    RUN_TEST(repro_grammar_scripting_typescript);
    RUN_TEST(repro_grammar_scripting_tsx);
    RUN_TEST(repro_grammar_scripting_lua);
    RUN_TEST(repro_grammar_scripting_perl);
    RUN_TEST(repro_grammar_scripting_r);
    RUN_TEST(repro_grammar_scripting_julia);
    RUN_TEST(repro_grammar_scripting_groovy);
    RUN_TEST(repro_grammar_scripting_dart);
}
