/*
 * repro_grammar_scientific.c -- Exhaustive per-grammar INVARIANT battery for the
 * SCIENTIFIC / SHADER / SMART-CONTRACT language family.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs the SAME battery against a tiny idiomatic fixture for
 * that language: a function (or method) that CALLS another function strictly
 * inside its body. The shared single-file + pipeline runners keep this DRY and
 * identical to repro_grammar_core.c so the families are comparable.
 *
 * Languages covered (15) and the CBM_LANG_* enum each uses (all verified present
 * in internal/cbm/cbm.h -- none missing, none skipped):
 *   GLSL     -> CBM_LANG_GLSL      (shader; reuses C node types)
 *   HLSL     -> CBM_LANG_HLSL      (shader; C++-family node types)
 *   WGSL     -> CBM_LANG_WGSL      (shader; own grammar)
 *   ISPC     -> CBM_LANG_ISPC      (shader/SIMD; C-family node types)
 *   Slang    -> CBM_LANG_SLANG     (shader; C++-family node types)
 *   Cairo    -> CBM_LANG_CAIRO     (smart-contract; Rust-like)
 *   Sway     -> CBM_LANG_SWAY      (smart-contract; Rust-like)
 *   FunC     -> CBM_LANG_FUNC      (smart-contract; TON)
 *   Wolfram  -> CBM_LANG_WOLFRAM   (CAS; assignment-as-definition)
 *   MATLAB   -> CBM_LANG_MATLAB    (numeric)
 *   Magma    -> CBM_LANG_MAGMA     (CAS)
 *   FORM     -> CBM_LANG_FORM      (symbolic; procedure_definition / call_statement)
 *   TLA+     -> CBM_LANG_TLAPLUS   (formal spec; operator_definition)
 *   Agda     -> CBM_LANG_AGDA      (dependently-typed)
 *   Apex     -> CBM_LANG_APEX      (Salesforce; Java-like, methods only)
 *
 * BATTERY DIMENSIONS (identical to repro_grammar_core.c)
 * -----------------------------------------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean   : inv_extract_clean(src,lang,file) == 1
 *   2. labels-valid    : inv_count_bad_labels(r) == 0
 *   3. fqn-wellformed  : inv_count_bad_fqns(r) == 0
 *   4. ranges-valid    : inv_count_bad_ranges(r) == 0
 *   5. defs-present    : the function/method written in the fixture is extracted
 *   6. calls-extracted : inv_has_call(r, "<callee>") == 1 (the in-body call)
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : module_sourced == 0 -- every in-body call sourced at a
 *                          Function/Method node, NEVER at a Module node.
 *   8. no-dangling       : inv_count_dangling_edges(store,project,"CALLS") == 0
 *
 * ROBUSTNESS: each TEST also feeds a deliberately malformed fixture through the
 * single-file extractor and asserts it RETURNS (no crash, NULL-or-result both
 * acceptable). A hard crash would not return at all and would fail the test.
 *
 * KNOWN GAP (the point of this file): these are mostly grammar-only (non-LSP)
 * languages, so dimension 7 (callable-sourcing) is expected RED for the majority
 * via the same cbm_enclosing_func_qn -> Module fallback documented in
 * repro_grammar_core.c (func_kinds_for_lang in helpers.c not matching the
 * grammar's emitted function node types, with no cross-LSP rescue for these
 * langs). Several langs are additionally expected RED at dimension 6
 * (calls-extracted) because their call node type is unusual and the in-body
 * call may not be captured at all: Wolfram (call=apply), FORM
 * (call=call_statement), Agda (call=module_application), MATLAB (command/
 * function_call ambiguity). RED rows ARE the deliverable -- they document the
 * gap honestly per language.
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
 * guaranteed to produce; pass NULL for expect_label2 when the language's
 * class/struct labeling is not asserted. callee is the in-body callee name that
 * must appear in the extracted calls.
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

    /* 5. defs-present -- the function/method the fixture wrote must be extracted. */
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
 * edges. Returns 0 on PASS, 1 on FAIL. Dimension 7 is RED for most grammar-only
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

/* ── Robustness probe ───────────────────────────────────────────────────────
 *
 * Feed a deliberately malformed/truncated fixture through the single-file
 * extractor. The ONLY invariant here is liveness: the call must RETURN (a hard
 * crash would not). NULL or a result are both acceptable; if a result comes
 * back its ranges must still be well-formed (no negative/inverted lines).
 * Returns 0 on PASS (returned + ranges sane), 1 on FAIL.
 */
static int robustness_probe(const char *lang_tag, const char *bad_src,
                            CBMLanguage lang, const char *file) {
    const char *RED = tf_red();
    const char *RST = tf_reset();
    CBMFileResult *r = inv_rx(bad_src, lang, file);
    if (!r) {
        /* Returned cleanly with NULL -- acceptable, no crash. */
        return 0;
    }
    int bad_ranges = inv_count_bad_ranges(r);
    cbm_free_result(r);
    if (bad_ranges != 0) {
        printf("  %sFAIL%s  [%s] robustness: malformed input produced %d def(s) "
               "with invalid range\n",
               RED, RST, lang_tag, bad_ranges);
        return 1;
    }
    return 0;
}

/* ── GLSL ────────────────────────────────────────────────────────────────────
 * Shader; reuses C node types (c_func_types / c_call_types). Idiomatic: a helper
 * function called from inside main(). No class/struct in the fixture (shaders
 * have none). Expected: dims 1-6 + 8 GREEN, dim 7 RED (shares C func_kinds; the
 * C family dominates the Module-sourced CALLS list).
 */
TEST(repro_grammar_scientific_glsl) {
    static const char src[] =
        "#version 450\n"
        "\n"
        "float scale(float x) {\n"
        "    return x * 2.0;\n"
        "}\n"
        "\n"
        "void main() {\n"
        "    float v = scale(0.5);\n"
        "    gl_FragColor = vec4(v);\n"
        "}\n";
    if (single_file_battery("GLSL", src, CBM_LANG_GLSL, "shader.frag",
                            "Function", NULL, "scale") != 0)
        return 1;
    if (robustness_probe("GLSL", "void main() { float v = scale(",
                         CBM_LANG_GLSL, "shader.frag") != 0)
        return 1;
    return pipeline_battery("GLSL", "shader.frag", src);
}

/* ── HLSL ────────────────────────────────────────────────────────────────────
 * Shader; C++-family node types (hlsl_func_types = function_definition,
 * hlsl_call_types = call_expression). Idiomatic: a helper called from a pixel
 * shader entry point. Expected: dims 1-6 + 8 GREEN, dim 7 RED (C++ func_kinds
 * gap). No class/struct asserted (shaders rarely use them idiomatically here).
 */
TEST(repro_grammar_scientific_hlsl) {
    static const char src[] =
        "float scale(float x) {\n"
        "    return x * 2.0;\n"
        "}\n"
        "\n"
        "float4 PSMain(float2 uv : TEXCOORD0) : SV_TARGET {\n"
        "    float v = scale(uv.x);\n"
        "    return float4(v, v, v, 1.0);\n"
        "}\n";
    if (single_file_battery("HLSL", src, CBM_LANG_HLSL, "shader.hlsl",
                            "Function", NULL, "scale") != 0)
        return 1;
    if (robustness_probe("HLSL", "float4 PSMain( { return scale(",
                         CBM_LANG_HLSL, "shader.hlsl") != 0)
        return 1;
    return pipeline_battery("HLSL", "shader.hlsl", src);
}

/* ── WGSL ────────────────────────────────────────────────────────────────────
 * WebGPU shading language; own grammar (wgsl_func_types = function_declaration,
 * wgsl_call_types = type_constructor_or_function_call_expression). Idiomatic: a
 * helper fn called from an @fragment entry point. Expected: dims 1-6 + 8 GREEN,
 * dim 7 RED (grammar-only, enclosing-func walk falls back to Module). The call
 * node type is the unusual WGSL one -- dim 6 is a real risk if helpers.c does
 * not map it.
 */
TEST(repro_grammar_scientific_wgsl) {
    static const char src[] =
        "fn scale(x: f32) -> f32 {\n"
        "    return x * 2.0;\n"
        "}\n"
        "\n"
        "@fragment\n"
        "fn fs_main() -> @location(0) vec4<f32> {\n"
        "    let v = scale(0.5);\n"
        "    return vec4<f32>(v, v, v, 1.0);\n"
        "}\n";
    if (single_file_battery("WGSL", src, CBM_LANG_WGSL, "shader.wgsl",
                            "Function", NULL, "scale") != 0)
        return 1;
    if (robustness_probe("WGSL", "fn fs_main() -> { let v = scale(",
                         CBM_LANG_WGSL, "shader.wgsl") != 0)
        return 1;
    return pipeline_battery("WGSL", "shader.wgsl", src);
}

/* ── ISPC ────────────────────────────────────────────────────────────────────
 * Intel SPMD Program Compiler; C-family node types (ispc_func_types =
 * function_definition, ispc_call_types = call_expression). Idiomatic: an inline
 * helper called from an exported kernel. Expected: dims 1-6 + 8 GREEN, dim 7 RED
 * (shares the C/C++ enclosing-func handling).
 */
TEST(repro_grammar_scientific_ispc) {
    static const char src[] =
        "static inline float scale(float x) {\n"
        "    return x * 2.0f;\n"
        "}\n"
        "\n"
        "export void run(uniform float out[], uniform int n) {\n"
        "    foreach (i = 0 ... n) {\n"
        "        out[i] = scale((float)i);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("ISPC", src, CBM_LANG_ISPC, "kernel.ispc",
                            "Function", NULL, "scale") != 0)
        return 1;
    if (robustness_probe("ISPC", "export void run( { out[0] = scale(",
                         CBM_LANG_ISPC, "kernel.ispc") != 0)
        return 1;
    return pipeline_battery("ISPC", "kernel.ispc", src);
}

/* ── Slang ───────────────────────────────────────────────────────────────────
 * NVIDIA Slang shading language; C++-family node types (slang_func_types =
 * function_definition, slang_call_types = call_expression). Idiomatic: a helper
 * called from a compute entry point. Expected: dims 1-6 + 8 GREEN, dim 7 RED
 * (C++ func_kinds gap, no cross-LSP rescue for Slang).
 */
TEST(repro_grammar_scientific_slang) {
    static const char src[] =
        "float scale(float x) {\n"
        "    return x * 2.0;\n"
        "}\n"
        "\n"
        "[shader(\"compute\")]\n"
        "void csMain(uint3 tid : SV_DispatchThreadID) {\n"
        "    float v = scale(float(tid.x));\n"
        "    outBuf[tid.x] = v;\n"
        "}\n";
    if (single_file_battery("Slang", src, CBM_LANG_SLANG, "shader.slang",
                            "Function", NULL, "scale") != 0)
        return 1;
    if (robustness_probe("Slang", "void csMain( { float v = scale(",
                         CBM_LANG_SLANG, "shader.slang") != 0)
        return 1;
    return pipeline_battery("Slang", "shader.slang", src);
}

/* ── Cairo ───────────────────────────────────────────────────────────────────
 * StarkNet smart-contract language; Rust-like (cairo_func_types =
 * function_definition/function_signature, cairo_call_types = call_expression/
 * call). Idiomatic: a free fn calling another free fn. Expected: dims 1-6 + 8
 * GREEN, dim 7 RED (Rust-shaped enclosing-func walk falls back to Module, no
 * cross-LSP rescue for Cairo).
 */
TEST(repro_grammar_scientific_cairo) {
    static const char src[] =
        "fn add(a: felt252, b: felt252) -> felt252 {\n"
        "    a + b\n"
        "}\n"
        "\n"
        "fn compute(x: felt252) -> felt252 {\n"
        "    add(x, 1)\n"
        "}\n";
    if (single_file_battery("Cairo", src, CBM_LANG_CAIRO, "lib.cairo",
                            "Function", NULL, "add") != 0)
        return 1;
    if (robustness_probe("Cairo", "fn compute(x: felt252) -> { add(",
                         CBM_LANG_CAIRO, "lib.cairo") != 0)
        return 1;
    return pipeline_battery("Cairo", "lib.cairo", src);
}

/* ── Sway ────────────────────────────────────────────────────────────────────
 * Fuel smart-contract language; Rust-like (sway_func_types = function_item,
 * sway_call_types = call_expression). Idiomatic: a free fn calling another.
 * Expected: dims 1-6 + 8 GREEN, dim 7 RED (same Rust-shaped enclosing-func gap).
 */
TEST(repro_grammar_scientific_sway) {
    static const char src[] =
        "fn add(a: u64, b: u64) -> u64 {\n"
        "    a + b\n"
        "}\n"
        "\n"
        "fn compute(x: u64) -> u64 {\n"
        "    add(x, 1)\n"
        "}\n";
    if (single_file_battery("Sway", src, CBM_LANG_SWAY, "main.sw",
                            "Function", NULL, "add") != 0)
        return 1;
    if (robustness_probe("Sway", "fn compute(x: u64) -> { add(",
                         CBM_LANG_SWAY, "main.sw") != 0)
        return 1;
    return pipeline_battery("Sway", "main.sw", src);
}

/* ── FunC ────────────────────────────────────────────────────────────────────
 * TON smart-contract language; (func_func_types = function_definition,
 * func_call_types = method_call). Idiomatic: a function calling another. NOTE
 * the call node type is "method_call" -- if the grammar emits a plain call node
 * for `add(x, 1)` rather than `method_call`, dim 6 (calls-extracted) is a real
 * RED risk. Expected: dims 1-5 GREEN, dim 6 at risk, dim 7 RED, dim 8 GREEN.
 */
TEST(repro_grammar_scientific_func) {
    static const char src[] =
        "int add(int a, int b) {\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "int compute(int x) {\n"
        "    return add(x, 1);\n"
        "}\n";
    if (single_file_battery("FunC", src, CBM_LANG_FUNC, "contract.fc",
                            "Function", NULL, "add") != 0)
        return 1;
    if (robustness_probe("FunC", "int compute(int x) { return add(",
                         CBM_LANG_FUNC, "contract.fc") != 0)
        return 1;
    return pipeline_battery("FunC", "contract.fc", src);
}

/* ── Wolfram ─────────────────────────────────────────────────────────────────
 * Wolfram Language / Mathematica; definitions are assignments (wolfram_func_types
 * = set_delayed/set, wolfram_call_types = apply). Idiomatic: `add` defined with
 * `:=`, then `compute` calls `add`. NOTE the call node type is "apply" -- the
 * in-body `add[x, 1]` must surface as an apply node for dim 6 to pass; this is a
 * real RED risk. Expected: dims 1-5 GREEN, dim 6 at risk, dim 7 RED (assignment-
 * as-def has no function-node ancestry for the enclosing-func walk), dim 8 GREEN.
 */
TEST(repro_grammar_scientific_wolfram) {
    static const char src[] =
        "add[a_, b_] := a + b\n"
        "\n"
        "compute[x_] := add[x, 1]\n";
    if (single_file_battery("Wolfram", src, CBM_LANG_WOLFRAM, "calc.wl",
                            "Function", NULL, "add") != 0)
        return 1;
    if (robustness_probe("Wolfram", "compute[x_] := add[x,",
                         CBM_LANG_WOLFRAM, "calc.wl") != 0)
        return 1;
    return pipeline_battery("Wolfram", "calc.wl", src);
}

/* ── MATLAB ───────────────────────────────────────────────────────────────────
 * Numeric; (matlab_func_types = function_definition, matlab_call_types =
 * function_call/command). Idiomatic: a top-level function `compute` calling a
 * local function `add`. NOTE MATLAB's call/command ambiguity: `add(x, 1)` should
 * be a function_call, but a bare `add x` would parse as a command -- the
 * idiomatic parenthesized form is used here. Expected: dims 1-6 + 8 GREEN, dim 7
 * RED (enclosing-func gap).
 */
TEST(repro_grammar_scientific_matlab) {
    static const char src[] =
        "function r = compute(x)\n"
        "    r = add(x, 1);\n"
        "end\n"
        "\n"
        "function s = add(a, b)\n"
        "    s = a + b;\n"
        "end\n";
    if (single_file_battery("MATLAB", src, CBM_LANG_MATLAB, "calc.m",
                            "Function", NULL, "add") != 0)
        return 1;
    if (robustness_probe("MATLAB", "function r = compute(x)\n  r = add(",
                         CBM_LANG_MATLAB, "calc.m") != 0)
        return 1;
    return pipeline_battery("MATLAB", "calc.m", src);
}

/* ── Magma ────────────────────────────────────────────────────────────────────
 * Computational algebra system; (magma_func_types = function_definition/
 * procedure_definition, magma_call_types = call_expression). Idiomatic: a
 * function `Add` and a function `Compute` that calls it.
 *
 * Fixture correction: the prior `Add := function(a, b) ... end function;`
 * assignment form does NOT parse to a `function_definition` in tree-sitter-magma
 * — `function(a, b)` is read as a `call_expression` named "function" and the
 * trailing `end function;` lands in an ERROR node, so no Function def was minted.
 * The declarative `function Name(...) ... end function;` form (the construct the
 * grammar and magma_func_types target) parses cleanly into `function_definition`
 * with a `name` field. Expected: dims 1-6 + 8 GREEN, dim 7 RED (enclosing-func gap).
 */
TEST(repro_grammar_scientific_magma) {
    static const char src[] =
        "function Add(a, b)\n"
        "    return a + b;\n"
        "end function;\n"
        "\n"
        "function Compute(x)\n"
        "    return Add(x, 1);\n"
        "end function;\n";
    if (single_file_battery("Magma", src, CBM_LANG_MAGMA, "calc.magma",
                            "Function", NULL, "Add") != 0)
        return 1;
    if (robustness_probe("Magma", "function Compute(x)\n  return Add(",
                         CBM_LANG_MAGMA, "calc.magma") != 0)
        return 1;
    return pipeline_battery("Magma", "calc.magma", src);
}

/* ── FORM ─────────────────────────────────────────────────────────────────────
 * Symbolic manipulation system; (form_func_types = procedure_definition,
 * form_call_types = call_statement). Idiomatic: a `#procedure add` definition and
 * a second procedure that `#call add` invokes. NOTE the call node type is
 * "call_statement" matching FORM's `#call` preprocessor directive -- dim 6
 * depends on the grammar emitting that node for `#call add`. Expected: dims 1-5
 * GREEN, dim 6 at risk, dim 7 RED, dim 8 GREEN.
 */
TEST(repro_grammar_scientific_form) {
    static const char src[] =
        "#procedure add(x)\n"
        "    Local r = `x' + 1;\n"
        "#endprocedure\n"
        "\n"
        "#procedure compute(y)\n"
        "    #call add(`y')\n"
        "#endprocedure\n";
    if (single_file_battery("FORM", src, CBM_LANG_FORM, "calc.frm",
                            "Function", NULL, "add") != 0)
        return 1;
    if (robustness_probe("FORM", "#procedure compute(y)\n  #call add(",
                         CBM_LANG_FORM, "calc.frm") != 0)
        return 1;
    return pipeline_battery("FORM", "calc.frm", src);
}

/* ── TLA+ ─────────────────────────────────────────────────────────────────────
 * Formal specification language; (tlaplus_func_types = operator_definition/
 * function_definition, tlaplus_call_types = function_evaluation/call). Idiomatic:
 * an operator `Add` and an operator `Compute` that applies it. The defs surface
 * via operator_definition; the in-body `Add(x, 1)` must surface as a
 * function_evaluation/call node for dim 6. Expected: dims 1-5 GREEN, dim 6 at
 * risk, dim 7 RED, dim 8 GREEN.
 */
TEST(repro_grammar_scientific_tlaplus) {
    static const char src[] =
        "---- MODULE Calc ----\n"
        "Add(a, b) == a + b\n"
        "Compute(x) == Add(x, 1)\n"
        "====\n";
    if (single_file_battery("TLA+", src, CBM_LANG_TLAPLUS, "Calc.tla",
                            "Function", NULL, "Add") != 0)
        return 1;
    if (robustness_probe("TLA+", "---- MODULE Calc ----\nCompute(x) == Add(",
                         CBM_LANG_TLAPLUS, "Calc.tla") != 0)
        return 1;
    return pipeline_battery("TLA+", "Calc.tla", src);
}

/* ── Agda ─────────────────────────────────────────────────────────────────────
 * Dependently-typed language; (agda_func_types = function, agda_call_types =
 * module_application). Idiomatic: a function `add` and a function `compute` that
 * applies it. NOTE the call node type is "module_application" -- a plain function
 * application `add x one` will almost certainly NOT match that node type, so dim
 * 6 (calls-extracted) is a strong RED expectation. Expected: dims 1-5 GREEN, dim
 * 6 RED, dim 7 RED (no callable-sourced edge to attribute -> 0 CALLS), dim 8
 * GREEN (vacuously -- no edges).
 */
TEST(repro_grammar_scientific_agda) {
    static const char src[] =
        "module Calc where\n"
        "\n"
        "open import Agda.Builtin.Nat\n"
        "\n"
        "add : Nat -> Nat -> Nat\n"
        "add a b = a + b\n"
        "\n"
        "compute : Nat -> Nat\n"
        "compute x = add x 1\n";
    if (single_file_battery("Agda", src, CBM_LANG_AGDA, "Calc.agda",
                            "Function", NULL, "add") != 0)
        return 1;
    if (robustness_probe("Agda", "module Calc where\ncompute x = add x",
                         CBM_LANG_AGDA, "Calc.agda") != 0)
        return 1;
    return pipeline_battery("Agda", "Calc.agda", src);
}

/* ── Apex ─────────────────────────────────────────────────────────────────────
 * Salesforce Apex; Java-like, methods-only (apex_func_types = method_declaration/
 * constructor_declaration, apex_class_types = class_declaration, apex_call_types =
 * method_invocation). Idiomatic: a class with two methods, the public one calling
 * the private one in-body. Expected: dims 1-6 + 8 GREEN, dim 7 likely RED
 * (analogous to Java per the breadth-suite gap evidence). Asserts both "Method"
 * and "Class" defs are present.
 */
TEST(repro_grammar_scientific_apex) {
    static const char src[] =
        "public class Calculator {\n"
        "    private Integer add(Integer a, Integer b) {\n"
        "        return a + b;\n"
        "    }\n"
        "\n"
        "    public Integer compute(Integer x) {\n"
        "        return add(x, 1);\n"
        "    }\n"
        "}\n";
    if (single_file_battery("Apex", src, CBM_LANG_APEX, "Calculator.cls",
                            "Method", "Class", "add") != 0)
        return 1;
    if (robustness_probe("Apex", "public class Calculator { Integer compute() { return add(",
                         CBM_LANG_APEX, "Calculator.cls") != 0)
        return 1;
    return pipeline_battery("Apex", "Calculator.cls", src);
}

/* ── Suite ──────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_scientific) {
    RUN_TEST(repro_grammar_scientific_glsl);
    RUN_TEST(repro_grammar_scientific_hlsl);
    RUN_TEST(repro_grammar_scientific_wgsl);
    RUN_TEST(repro_grammar_scientific_ispc);
    RUN_TEST(repro_grammar_scientific_slang);
    RUN_TEST(repro_grammar_scientific_cairo);
    RUN_TEST(repro_grammar_scientific_sway);
    RUN_TEST(repro_grammar_scientific_func);
    RUN_TEST(repro_grammar_scientific_wolfram);
    RUN_TEST(repro_grammar_scientific_matlab);
    RUN_TEST(repro_grammar_scientific_magma);
    RUN_TEST(repro_grammar_scientific_form);
    RUN_TEST(repro_grammar_scientific_tlaplus);
    RUN_TEST(repro_grammar_scientific_agda);
    RUN_TEST(repro_grammar_scientific_apex);
}
