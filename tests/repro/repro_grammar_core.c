/*
 * repro_grammar_core.c -- Exhaustive per-grammar INVARIANT battery for the
 * COMPILED / OOP language family.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs the SAME battery against a tiny idiomatic fixture for
 * that language (a function/method that CALLS another function strictly inside
 * its body, a class/struct where the language has one, and an idiomatic
 * import/include). The shared single-file + pipeline runners keep this DRY.
 *
 * Languages covered (12) and the CBM_LANG_* enum each uses:
 *   C       -> CBM_LANG_C
 *   C++     -> CBM_LANG_CPP
 *   CUDA    -> CBM_LANG_CUDA
 *   Rust    -> CBM_LANG_RUST
 *   Go      -> CBM_LANG_GO
 *   Java    -> CBM_LANG_JAVA
 *   C#      -> CBM_LANG_CSHARP
 *   Kotlin  -> CBM_LANG_KOTLIN
 *   Scala   -> CBM_LANG_SCALA
 *   Swift   -> CBM_LANG_SWIFT
 *   Obj-C   -> CBM_LANG_OBJC
 *   D       -> CBM_LANG_DLANG
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
 * KNOWN GAP (the point of this file): dimension 7 (callable-sourcing) is RED for
 * most of the compiled/OOP languages on current code. Per QUALITY_ANALYSIS.md
 * (2026-06-24) only ~3.69% of CALLS edges in the real graph are callable-sourced;
 * the dominant failure is cbm_enclosing_func_qn falling back to the module QN when
 * cbm_find_enclosing_func cannot walk the TSNode ancestry to a function node
 * (func_kinds_for_lang in helpers.c not matching the grammar's emitted node
 * types), and the LSP rescue cannot compensate because it joins on exact caller_qn
 * equality. So dimensions 1-6 and 8 are expected GREEN for these idiomatic
 * fixtures; dimension 7 is expected RED for C/C++/Rust/Java/C#/Kotlin/Scala/
 * Swift/Obj-C/D and GREEN for Go/CUDA (Go is grep-validated correct; CUDA is a
 * listed GREEN in the breadth table). RED dimension-7 rows ARE the deliverable.
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
 * guaranteed to produce (e.g. "Function" and "Class"/"Struct"); pass NULL for
 * expect_label2 when the language has no class/struct in the fixture. callee is
 * the in-body callee name that must appear in the extracted calls.
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
 * edges. Returns 0 on PASS, 1 on FAIL. Dimension 7 is RED for most compiled/
 * OOP languages on current code -- that is the intended signal.
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

/* ── C ──────────────────────────────────────────────────────────────────────
 * Idiomatic: #include header, two free functions, callee inside the body.
 * C has no class/struct def in this fixture (struct shown but the def set we
 * assert on is the Function). Expected: dims 1-6 + 8 GREEN, dim 7 RED
 * (func_kinds_cpp shared with C; C dominates the Module-sourced CALLS list).
 */
TEST(repro_grammar_core_c) {
    static const char src[] =
        "#include <stdio.h>\n"
        "\n"
        "static int add(int a, int b) {\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "int compute(int x) {\n"
        "    return add(x, 1);\n"
        "}\n";
    if (single_file_battery("C", src, CBM_LANG_C, "main.c",
                            "Function", NULL, "add") != 0)
        return 1;
    return pipeline_battery("C", "main.c", src);
}

/* ── C++ ─────────────────────────────────────────────────────────────────────
 * Idiomatic: #include, a class with a method, a free helper, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (shares func_kinds with C; out-of-
 * line method defs also drop the class qualifier, issue #554).
 */
TEST(repro_grammar_core_cpp) {
    static const char src[] =
        "#include <vector>\n"
        "\n"
        "static int helper(int x) {\n"
        "    return x * 2;\n"
        "}\n"
        "\n"
        "class Processor {\n"
        "public:\n"
        "    int run(int v) {\n"
        "        return helper(v);\n"
        "    }\n"
        "};\n";
    if (single_file_battery("C++", src, CBM_LANG_CPP, "main.cpp",
                            "Method", "Class", "helper") != 0)
        return 1;
    return pipeline_battery("C++", "main.cpp", src);
}

/* ── CUDA ─────────────────────────────────────────────────────────────────────
 * Idiomatic: a __device__ helper called from a __global__ kernel body.
 * Expected GREEN across the battery including dim 7 (CUDA is a listed GREEN in
 * the breadth callable-sourcing table).
 */
TEST(repro_grammar_core_cuda) {
    static const char src[] =
        "__device__ int helper(int x) {\n"
        "    return x * 2;\n"
        "}\n"
        "\n"
        "__global__ void run(int *out) {\n"
        "    out[0] = helper(21);\n"
        "}\n";
    if (single_file_battery("CUDA", src, CBM_LANG_CUDA, "k.cu",
                            "Function", NULL, "helper") != 0)
        return 1;
    return pipeline_battery("CUDA", "k.cu", src);
}

/* ── Rust ─────────────────────────────────────────────────────────────────────
 * Idiomatic: a `use` import, a struct + impl method, a free fn, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (cbm_pxc_has_cross_lsp is false for
 * CBM_LANG_RUST, so the cross-LSP rescue never runs; tree-sitter enclosing-func
 * walk alone falls back to Module).
 */
TEST(repro_grammar_core_rust) {
    static const char src[] =
        "use std::fmt;\n"
        "\n"
        "fn add(a: i32, b: i32) -> i32 {\n"
        "    a + b\n"
        "}\n"
        "\n"
        "struct Calc {\n"
        "    base: i32,\n"
        "}\n"
        "\n"
        "impl Calc {\n"
        "    fn compute(&self, x: i32) -> i32 {\n"
        "        add(self.base, x)\n"
        "    }\n"
        "}\n";
    if (single_file_battery("Rust", src, CBM_LANG_RUST, "lib.rs",
                            "Function", "Struct", "add") != 0)
        return 1;
    return pipeline_battery("Rust", "lib.rs", src);
}

/* ── Go ───────────────────────────────────────────────────────────────────────
 * Idiomatic: package + import, a struct + method, a free func, in-body call.
 * Expected GREEN across the battery including dim 7 (func_kinds_go is in sync
 * with the mature tree-sitter-go grammar; grep-validated correct). Regression
 * guard: if dim 7 goes RED, Go callable attribution has broken.
 */
TEST(repro_grammar_core_go) {
    static const char src[] =
        "package main\n"
        "\n"
        "import \"fmt\"\n"
        "\n"
        "type Calc struct {\n"
        "    base int\n"
        "}\n"
        "\n"
        "func add(a, b int) int {\n"
        "    return a + b\n"
        "}\n"
        "\n"
        "func (c Calc) compute(x int) int {\n"
        "    fmt.Println(\"compute\")\n"
        "    return add(c.base, x)\n"
        "}\n";
    if (single_file_battery("Go", src, CBM_LANG_GO, "main.go",
                            "Function", "Struct", "add") != 0)
        return 1;
    return pipeline_battery("Go", "main.go", src);
}

/* ── Java ──────────────────────────────────────────────────────────────────────
 * Idiomatic: import, a class with two methods, callee inside the caller body.
 * Expected: dims 1-6 + 8 GREEN, dim 7 likely RED (java_lsp shows ~90 Module-
 * sourced CALLS in the real graph; the minimal same-class method call is the
 * simplest possible case and the audit evidence suggests it still falls back).
 */
TEST(repro_grammar_core_java) {
    static const char src[] =
        "import java.util.List;\n"
        "\n"
        "public class Calculator {\n"
        "    private int add(int a, int b) {\n"
        "        return a + b;\n"
        "    }\n"
        "\n"
        "    public int compute(int x) {\n"
        "        return add(x, 1);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("Java", src, CBM_LANG_JAVA, "Calculator.java",
                            "Method", "Class", "add") != 0)
        return 1;
    return pipeline_battery("Java", "Calculator.java", src);
}

/* ── C# ────────────────────────────────────────────────────────────────────────
 * Idiomatic: using directive, a class with two methods, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 likely RED (analogous to Java per the
 * breadth-suite gap evidence).
 */
TEST(repro_grammar_core_csharp) {
    static const char src[] =
        "using System;\n"
        "\n"
        "public class Calculator {\n"
        "    private int Add(int a, int b) {\n"
        "        return a + b;\n"
        "    }\n"
        "\n"
        "    public int Compute(int x) {\n"
        "        return Add(x, 1);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("C#", src, CBM_LANG_CSHARP, "Calculator.cs",
                            "Method", "Class", "Add") != 0)
        return 1;
    return pipeline_battery("C#", "Calculator.cs", src);
}

/* ── Kotlin ────────────────────────────────────────────────────────────────────
 * Idiomatic: import, a class with two methods, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 likely RED (Kotlin LSP is hybrid; the
 * enclosing-func attribution gap applies the same as the other OOP/LSP langs).
 */
TEST(repro_grammar_core_kotlin) {
    static const char src[] =
        "import kotlin.math.max\n"
        "\n"
        "class Calculator {\n"
        "    private fun add(a: Int, b: Int): Int {\n"
        "        return a + b\n"
        "    }\n"
        "\n"
        "    fun compute(x: Int): Int {\n"
        "        return add(x, 1)\n"
        "    }\n"
        "}\n";
    if (single_file_battery("Kotlin", src, CBM_LANG_KOTLIN, "Calc.kt",
                            "Method", "Class", "add") != 0)
        return 1;
    return pipeline_battery("Kotlin", "Calc.kt", src);
}

/* ── Scala ─────────────────────────────────────────────────────────────────────
 * Idiomatic: import, a class with two methods, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 likely RED (same enclosing-func gap;
 * Scala has no dedicated cross-LSP rescue distinguishing it from the working
 * set).
 */
TEST(repro_grammar_core_scala) {
    static const char src[] =
        "import scala.collection.mutable\n"
        "\n"
        "class Calculator {\n"
        "  private def add(a: Int, b: Int): Int = {\n"
        "    a + b\n"
        "  }\n"
        "\n"
        "  def compute(x: Int): Int = {\n"
        "    add(x, 1)\n"
        "  }\n"
        "}\n";
    if (single_file_battery("Scala", src, CBM_LANG_SCALA, "Calc.scala",
                            "Method", "Class", "add") != 0)
        return 1;
    return pipeline_battery("Scala", "Calc.scala", src);
}

/* ── Swift ─────────────────────────────────────────────────────────────────────
 * Idiomatic: import, a struct with two methods, in-body call.
 * Expected: dims 1-6 + 8 GREEN, dim 7 likely RED (same attribution gap for the
 * tree-sitter-swift enclosing-func walk).
 */
TEST(repro_grammar_core_swift) {
    static const char src[] =
        "import Foundation\n"
        "\n"
        "struct Calculator {\n"
        "    func add(_ a: Int, _ b: Int) -> Int {\n"
        "        return a + b\n"
        "    }\n"
        "\n"
        "    func compute(_ x: Int) -> Int {\n"
        "        return add(x, 1)\n"
        "    }\n"
        "}\n";
    if (single_file_battery("Swift", src, CBM_LANG_SWIFT, "Calc.swift",
                            "Method", "Struct", "add") != 0)
        return 1;
    return pipeline_battery("Swift", "Calc.swift", src);
}

/* ── Objective-C ───────────────────────────────────────────────────────────────
 * Idiomatic: #import, an @interface/@implementation class, a free C helper, and
 * the call made strictly inside a method body. Expected: dims 1-6 + 8 GREEN,
 * dim 7 likely RED (Obj-C shares the C/C++ enclosing-func handling).
 */
TEST(repro_grammar_core_objc) {
    static const char src[] =
        "#import <Foundation/Foundation.h>\n"
        "\n"
        "static int helper(int x) {\n"
        "    return x * 2;\n"
        "}\n"
        "\n"
        "@interface Calculator : NSObject\n"
        "- (int)compute:(int)x;\n"
        "@end\n"
        "\n"
        "@implementation Calculator\n"
        "- (int)compute:(int)x {\n"
        "    return helper(x);\n"
        "}\n"
        "@end\n";
    if (single_file_battery("Obj-C", src, CBM_LANG_OBJC, "Calc.m",
                            "Method", NULL, "helper") != 0)
        return 1;
    return pipeline_battery("Obj-C", "Calc.m", src);
}

/* ── D ─────────────────────────────────────────────────────────────────────────
 * Idiomatic: import, a struct + method, a free function, in-body call.
 * Expected GREEN across the battery including dim 7 (D is a listed GREEN in the
 * breadth callable-sourcing table). Uses CBM_LANG_DLANG.
 */
TEST(repro_grammar_core_dlang) {
    static const char src[] =
        "import std.stdio;\n"
        "\n"
        "int add(int a, int b)\n"
        "{\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "struct Calc\n"
        "{\n"
        "    int base;\n"
        "    int compute(int x)\n"
        "    {\n"
        "        return add(base, x);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("D", src, CBM_LANG_DLANG, "calc.d",
                            "Function", "Struct", "add") != 0)
        return 1;
    return pipeline_battery("D", "calc.d", src);
}

/* ── Suite ──────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_core) {
    RUN_TEST(repro_grammar_core_c);
    RUN_TEST(repro_grammar_core_cpp);
    RUN_TEST(repro_grammar_core_cuda);
    RUN_TEST(repro_grammar_core_rust);
    RUN_TEST(repro_grammar_core_go);
    RUN_TEST(repro_grammar_core_java);
    RUN_TEST(repro_grammar_core_csharp);
    RUN_TEST(repro_grammar_core_kotlin);
    RUN_TEST(repro_grammar_core_scala);
    RUN_TEST(repro_grammar_core_swift);
    RUN_TEST(repro_grammar_core_objc);
    RUN_TEST(repro_grammar_core_dlang);
}
