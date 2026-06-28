/*
 * repro_grammar_misc.c -- FINAL per-grammar INVARIANT battery covering the
 * remaining MISCELLANEOUS language family (hardware-description, CFML dialects,
 * niche scripting, structural assembly/linker/tablegen/ledger/IaC). This file
 * completes the all-159-grammar reproduce-first coverage: every CBM_LANG_* now
 * has a per-language RED/GREEN row on the bug-repro board.
 *
 * One TEST() per language so per-language RED/GREEN shows on the board. Each
 * test runs the battery dimension appropriate to what the language's lang_spec
 * actually models (verified against internal/cbm/lang_specs.c and the
 * *_func_types / *_class_types / *_call_types arrays):
 *
 *   CALLABLE family (func_types AND call_types both non-empty) -> FULL battery
 *   (dims 1-8) + robustness:
 *     VERILOG       -> CBM_LANG_VERILOG       (func: function_declaration/task;
 *                                              call: system_tf_call/subroutine_call)
 *     SYSTEMVERILOG -> CBM_LANG_SYSTEMVERILOG (func: function_declaration/task;
 *                                              call: function_subroutine_call)
 *     VHDL          -> CBM_LANG_VHDL          (func: subprogram_declaration/def;
 *                                              call: function_call/procedure_call)
 *     CFML          -> CBM_LANG_CFML          (func: function_declaration;
 *                                              call: call_expression)
 *     CFSCRIPT      -> CBM_LANG_CFSCRIPT      (func: function_declaration; call:
 *                                              js_call_types = call_expression)
 *     RESCRIPT      -> CBM_LANG_RESCRIPT      (func: function; call: call_expression)
 *     SQUIRREL      -> CBM_LANG_SQUIRREL      (func: function_declaration; call:
 *                                              call_expression)
 *     PINE          -> CBM_LANG_PINE          (func: function_declaration_statement;
 *                                              call: call)
 *     TEMPL         -> CBM_LANG_TEMPL         (func: function_declaration/method;
 *                                              call: call_expression)
 *     SQL           -> CBM_LANG_SQL           (func: create_function; call:
 *                                              function_call/invocation/command)
 *
 *   STRUCTURAL family (asm / linker / data / IaC) -> extract-clean +
 *   labels/fqn/ranges valid + defs-present (the entities each should extract) +
 *   robustness; NO call / pipeline dims:
 *     ASSEMBLY      -> CBM_LANG_ASSEMBLY      (func_types = {"label"}; defs are
 *                                              labels routed through the func-def
 *                                              path -> "Function"). defs-present
 *                                              asserts "Function".
 *     LINKERSCRIPT  -> CBM_LANG_LINKERSCRIPT  (only module_types + call_types; no
 *                                              func/class/var defs in spec). NO
 *                                              defs-present assertion -- dims 1-4
 *                                              + robustness only.
 *     TABLEGEN      -> CBM_LANG_TABLEGEN      (func: def/multiclass/defm ->
 *                                              "Function"; class: class -> "Class").
 *                                              defs-present asserts "Function" and
 *                                              "Class". No call_types -> no call dim.
 *     BEANCOUNT     -> CBM_LANG_BEANCOUNT     (only module_types + import_types; no
 *                                              func/class/var/call defs in spec).
 *                                              NO defs-present -- dims 1-4 +
 *                                              robustness only.
 *     BICEP         -> CBM_LANG_BICEP         (func: user_defined_function ->
 *                                              "Function"; class: resource/type/
 *                                              module_declaration -> "Class").
 *                                              defs-present asserts "Class" for the
 *                                              resource declaration. Treated as
 *                                              structural per the family split (no
 *                                              call/pipeline dim asserted).
 *
 * BATTERY DIMENSIONS
 * ------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean    : inv_extract_clean(src,lang,file) == 1
 *                         (parser returned a result and did not set has_error; a
 *                         hard crash would not return at all).
 *   2. labels-valid     : inv_count_bad_labels(r) == 0
 *                         (every extracted def label is in the known label set).
 *   3. fqn-wellformed   : inv_count_bad_fqns(r) == 0
 *                         (no empty / ".." / leading or trailing '.' / whitespace QNs).
 *   4. ranges-valid     : inv_count_bad_ranges(r) == 0
 *                         (start_line >= 1 and start_line <= end_line for every def).
 *   5. defs-present     : at least one def with each expected label is extracted.
 *   6. calls-extracted  : inv_has_call(r, callee) == 1 (the in-body call was
 *                         captured). CALLABLE family only.
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : inv_count_calls_by_source(store,project,&mod,&call);
 *                          assert mod == 0 AND call >= 1 -- every in-body call must
 *                          be sourced at a Function/Method node, NEVER at a Module
 *                          node. CALLABLE family only.
 *   8. no-dangling       : inv_count_dangling_edges(store,project,"CALLS") == 0
 *                          (every CALLS edge resolves both endpoints). CALLABLE
 *                          family only.
 *
 * ROBUSTNESS (every language):
 *   R. extract-on-malformed: the extractor must RETURN (not crash/hang) on a
 *      deliberately truncated/broken version of the fixture. cbm_extract_file may
 *      set has_error but must not return NULL.
 *
 * HONEST RED CONTRACT (the point of this file): dimension 7 (callable-sourcing) is
 * expected RED for the non-LSP callable languages here. None of VERILOG /
 * SYSTEMVERILOG / VHDL / CFML / CFSCRIPT / RESCRIPT / SQUIRREL / PINE / TEMPL / SQL
 * has a dedicated cross-LSP rescue, so attribution depends solely on the
 * tree-sitter enclosing-func walk (cbm_find_enclosing_func + func_kinds_for_lang in
 * helpers.c). When that mapping does not match the grammar's emitted func node
 * types, the in-body call falls back to the Module QN -- exactly the enclosing-func
 * drift documented for the compiled/OOP family in repro_grammar_core.c. Some of
 * these languages may additionally fail dim 6 (calls-extracted) if the grammar's
 * call node carries the callee on a child shape the call-extractor does not read,
 * or even dim 7 vacuously (0 CALLS edges). RED rows here ARE the deliverable: they
 * document the per-language attribution / extraction gaps precisely.
 *
 * Coding rule: inline comments are line comments only (no block comments inside
 * block comments).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* ── Shared single-file battery (dims 1-6) ───────────────────────────────────
 *
 * Runs the base invariants (1-4), the defs-present checks (5) for each non-NULL
 * expected label, and the calls-extracted check (6) when callee is non-NULL.
 * Pass NULL for expect_label2 / callee to skip those dimensions (structural
 * languages pass NULL for callee; languages with no asserted def pass NULL for
 * expect_label). Returns 0 on PASS, 1 on FAIL.
 */
static int misc_single_file_battery(const char *lang_tag, const char *src,
                                    CBMLanguage lang, const char *file,
                                    const char *expect_label,
                                    const char *expect_label2,
                                    const char *callee) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

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

    int fails = 0;

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

    /* 5. defs-present (per non-NULL expected label) */
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

    /* 6. calls-extracted (CALLABLE family only) */
    if (callee && inv_has_call(r, callee) != 1) {
        printf("  %sFAIL%s  [%s] calls-extracted: no call to \"%s\" found\n",
               RED, RST, lang_tag, callee);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Shared full-pipeline battery (dims 7-8) ─────────────────────────────────
 *
 * Indexes the single-file fixture through the production pipeline and asserts
 * callable-sourcing (no Module-sourced in-body CALLS, and >=1 callable-sourced
 * edge so a fixture that produced zero CALLS edges cannot vacuously pass) and no
 * dangling CALLS edges. Dim 7 is expected RED for the non-LSP callable languages
 * here -- that is the intended signal. Returns 0 on PASS, 1 on FAIL.
 */
static int misc_pipeline_battery(const char *lang_tag, const char *filename,
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

    /* 7. callable-sourcing */
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

    /* 8. no-dangling */
    int dangling = inv_count_dangling_edges(store, lp.project, "CALLS");
    if (dangling != 0) {
        printf("  %sFAIL%s  [%s] no-dangling: %d dangling CALLS endpoint(s)\n",
               RED, RST, lang_tag, dangling);
        fails++;
    }

    rh_cleanup(&lp, store);
    return fails ? 1 : 0;
}

/* ── Robustness helper: assert call RETURNS on malformed input ───────────────
 *
 * A truncated version of the fixture is passed through cbm_extract_file.
 * has_error may be set (1) but the call must return non-NULL. If it returns NULL
 * the extractor crashed or aborted on bad input -- that is a RED robustness bug.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int misc_robustness(const char *lang_tag, const char *bad_src,
                           CBMLanguage lang, const char *file) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    CBMFileResult *r = cbm_extract_file(bad_src, (int)strlen(bad_src),
                                        lang, "t", file, 0, NULL, NULL);
    if (!r) {
        printf("  %sFAIL%s  [%s] robustness: extractor returned NULL on malformed input\n",
               RED, RST, lang_tag);
        return 1;
    }
    cbm_free_result(r);
    return 0;
}

/* ── ASSEMBLY (structural) ───────────────────────────────────────────────────
 * Idiomatic x86-64 GAS snippet: a global function label, a local label, and a
 * call to a labelled routine. assembly_func_types = {"label"} so labels are
 * routed through the func-def path and minted as "Function" defs.
 * assembly spec has no call_types -> no calls/pipeline dims.
 *
 * Dims asserted: 1-5 ("Function" for the labels) + R.
 * Expected: dims 1-4 + R GREEN; dim 5 GREEN if label -> "Function" mints (the
 * `add:`/`main:` labels). Dim 5 RED would document that the assembly label
 * def-path does not fire for GAS-style labels.
 */
TEST(repro_grammar_misc_assembly) {
    static const char src[] =
        ".text\n"
        ".globl main\n"
        "add:\n"
        "    addl %esi, %edi\n"
        "    movl %edi, %eax\n"
        "    ret\n"
        "main:\n"
        "    movl $1, %edi\n"
        "    movl $2, %esi\n"
        "    call add\n"
        "    ret\n";
    static const char bad[] = ".globl main\nmain:\n    call ";
    if (misc_single_file_battery("ASSEMBLY", src, CBM_LANG_ASSEMBLY, "f.s",
                                 "Function", NULL, NULL) != 0)
        return 1;
    return misc_robustness("ASSEMBLY", bad, CBM_LANG_ASSEMBLY, "f.s");
}

/* ── BEANCOUNT (structural) ──────────────────────────────────────────────────
 * Idiomatic Beancount ledger: an option directive, an open directive for an
 * account, and a transaction with two postings. The Beancount spec has only
 * beancount_module_types = {"file"} + beancount_import_types; no func/class/var/
 * call types are mapped, so no labelled defs are minted from the grammar tree.
 *
 * Dims asserted: 1-4 + R (no defs-present, no calls/pipeline).
 * Expected GREEN: dims 1-4 + R. extract-clean RED would indicate the Beancount
 * grammar misparses standard directive / transaction syntax.
 */
TEST(repro_grammar_misc_beancount) {
    static const char src[] =
        "option \"title\" \"CBM Ledger\"\n"
        "\n"
        "2026-01-01 open Assets:Cash USD\n"
        "2026-01-01 open Expenses:Food USD\n"
        "\n"
        "2026-06-26 * \"Lunch\" \"Sandwich shop\"\n"
        "  Expenses:Food   12.50 USD\n"
        "  Assets:Cash    -12.50 USD\n";
    static const char bad[] = "2026-06-26 * \"Lunch\"\n  Expenses:Food   12.50";
    if (misc_single_file_battery("BEANCOUNT", src, CBM_LANG_BEANCOUNT,
                                 "main.beancount", NULL, NULL, NULL) != 0)
        return 1;
    return misc_robustness("BEANCOUNT", bad, CBM_LANG_BEANCOUNT,
                           "main.beancount");
}

/* ── BICEP (structural) ──────────────────────────────────────────────────────
 * Idiomatic Azure Bicep: a parameter, a variable, and a resource_declaration.
 * bicep_class_types = {"resource_declaration", "type_declaration",
 * "module_declaration"} -> "Class"; bicep_func_types = {"user_defined_function",
 * "lambda_expression"} -> "Function". The resource declaration is the primary
 * structural entity. call_types exist (call_expression) but Bicep is treated as
 * structural here -- the call/pipeline dims are not asserted.
 *
 * Dims asserted: 1-5 ("Class" for the resource) + R.
 * Expected: dims 1-4 + R GREEN; dim 5 GREEN if resource_declaration -> "Class".
 * Dim 5 RED would document that the Bicep resource def-path does not fire.
 */
TEST(repro_grammar_misc_bicep) {
    static const char src[] =
        "param location string = resourceGroup().location\n"
        "var storageName = 'cbmstore'\n"
        "\n"
        "resource sa 'Microsoft.Storage/storageAccounts@2023-01-01' = {\n"
        "  name: storageName\n"
        "  location: location\n"
        "  sku: {\n"
        "    name: 'Standard_LRS'\n"
        "  }\n"
        "  kind: 'StorageV2'\n"
        "}\n";
    static const char bad[] = "resource sa 'Microsoft.Storage@2023' = {\n  name:";
    if (misc_single_file_battery("BICEP", src, CBM_LANG_BICEP, "main.bicep",
                                 "Class", NULL, NULL) != 0)
        return 1;
    return misc_robustness("BICEP", bad, CBM_LANG_BICEP, "main.bicep");
}

/* ── CFML (callable) ─────────────────────────────────────────────────────────
 * Idiomatic CFML tag-dialect template (.cfm): a cffunction defining `add`, and a
 * second cffunction `compute` that invokes `add()` strictly inside its body.
 * cfml_func_types = {"function_declaration", "function_expression"} -> "Function";
 * cfml_call_types = {"call_expression"} -> call extraction.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the cffunction defs.
 * Dim 6 expected GREEN: call to "add" inside compute.
 * Dim 7 expected GREEN: cf_function_tag is in cfml_func_types and compute_func_qn
 *   resolves its name from the cf_attribute (name="..."), so the add() call inside
 *   compute's cffunction body sources to the compute Function. (Previously the
 *   def-extractor minted a "Function" for cf_function_tag but the scope-tracking
 *   func_types list only had function_declaration/_expression, so the in-body call
 *   mis-sourced to Module: a production sync bug, not a rescue gap -- now fixed.)
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_cfml) {
    static const char src[] =
        "<cffunction name=\"add\" returntype=\"numeric\">\n"
        "  <cfargument name=\"a\" type=\"numeric\">\n"
        "  <cfargument name=\"b\" type=\"numeric\">\n"
        "  <cfreturn arguments.a + arguments.b>\n"
        "</cffunction>\n"
        "\n"
        "<cffunction name=\"compute\" returntype=\"numeric\">\n"
        "  <cfargument name=\"x\" type=\"numeric\">\n"
        "  <cfreturn add(arguments.x, 1)>\n"
        "</cffunction>\n";
    static const char bad[] = "<cffunction name=\"add\">\n  <cfreturn add(";
    if (misc_single_file_battery("CFML", src, CBM_LANG_CFML, "calc.cfm",
                                 "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("CFML", bad, CBM_LANG_CFML, "calc.cfm") != 0)
        return 1;
    return misc_pipeline_battery("CFML", "calc.cfm", src);
}

/* ── CFSCRIPT (callable) ─────────────────────────────────────────────────────
 * Idiomatic CFML script-dialect component (.cfc): a function `add` and a function
 * `compute` that calls `add()` inside its body. cfscript_func_types =
 * {"function_declaration", "function_expression", "arrow_function",
 * "method_definition"} -> "Function"; the CFSCRIPT spec reuses js_call_types
 * (call_expression) for call extraction.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the function defs.
 * Dim 6 expected GREEN: call to "add" inside compute.
 * Dim 7 expected RED: no cross-LSP rescue for CFScript; the enclosing-func walk
 *   may attribute the in-body call at Module.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_cfscript) {
    static const char src[] =
        "component {\n"
        "  function add(a, b) {\n"
        "    return a + b;\n"
        "  }\n"
        "\n"
        "  function compute(x) {\n"
        "    return add(x, 1);\n"
        "  }\n"
        "}\n";
    static const char bad[] = "component {\n  function add(a, b) {\n    return add(";
    if (misc_single_file_battery("CFSCRIPT", src, CBM_LANG_CFSCRIPT, "Calc.cfc",
                                 "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("CFSCRIPT", bad, CBM_LANG_CFSCRIPT, "Calc.cfc") != 0)
        return 1;
    return misc_pipeline_battery("CFSCRIPT", "Calc.cfc", src);
}

/* ── LINKERSCRIPT (structural) ───────────────────────────────────────────────
 * Idiomatic GNU ld linker script: a MEMORY block, an ENTRY directive, and a
 * SECTIONS block. The Linkerscript spec has only linkerscript_module_types =
 * {"source_file"} + linkerscript_call_types = {"call_expression"}; there are NO
 * func_types/class_types/var_types, so no labelled defs are minted. Because
 * func_types is empty there is no Function node to source a call against, so the
 * call/pipeline dims are not asserted (they would vacuously fail dim 7).
 *
 * Dims asserted: 1-4 + R (no defs-present, no calls/pipeline).
 * Expected GREEN: dims 1-4 + R. extract-clean RED would indicate the linker-script
 * grammar misparses standard MEMORY/SECTIONS syntax.
 */
TEST(repro_grammar_misc_linkerscript) {
    static const char src[] =
        "ENTRY(_start)\n"
        "\n"
        "MEMORY\n"
        "{\n"
        "  FLASH (rx) : ORIGIN = 0x08000000, LENGTH = 256K\n"
        "  RAM (rwx)  : ORIGIN = 0x20000000, LENGTH = 64K\n"
        "}\n"
        "\n"
        "SECTIONS\n"
        "{\n"
        "  .text : { *(.text*) } > FLASH\n"
        "  .data : { *(.data*) } > RAM\n"
        "}\n";
    static const char bad[] = "SECTIONS\n{\n  .text : { *(.text*) } > ";
    if (misc_single_file_battery("LINKERSCRIPT", src, CBM_LANG_LINKERSCRIPT,
                                 "link.ld", NULL, NULL, NULL) != 0)
        return 1;
    return misc_robustness("LINKERSCRIPT", bad, CBM_LANG_LINKERSCRIPT, "link.ld");
}

/* ── PINE (callable) ─────────────────────────────────────────────────────────
 * Idiomatic Pine Script v5 indicator: a user function `ema2` defined with
 * function_declaration_statement, and a call to the built-in `plot()` plus an
 * application of `ema2`. pine_func_types = {"function_declaration_statement"} ->
 * "Function"; pine_call_types = {"call"} -> call extraction.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for ema2 and wrap.
 * Dim 6 expected GREEN: call to "ema2" inside wrap.
 * Dim 7 expected GREEN: wrap's body calls the same-file ema2, so a
 *   callable-sourced CALLS edge is emitted from the wrap Function node. The
 *   top-level indicator() call targets a Pine built-in (no same-file def), so it
 *   yields no edge -- no Module-sourced edge remains. (The earlier fixture's only
 *   same-file calls -- out = ema2(...) and plot(out) -- sat at script top level
 *   and were legitimately Module-sourced: a broken fixture, not a prod gap.)
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_pine) {
    static const char src[] =
        "//@version=5\n"
        "indicator(\"CBM EMA\", overlay=true)\n"
        "\n"
        "ema2(src, len) =>\n"
        "    a = src + len\n"
        "    a\n"
        "\n"
        "wrap(src, len) =>\n"
        "    b = ema2(src, len)\n"
        "    b\n";
    static const char bad[] = "//@version=5\nema2(src, len) =>\n    a = ta.ema(";
    if (misc_single_file_battery("PINE", src, CBM_LANG_PINE, "ind.pine",
                                 "Function", NULL, "ema2") != 0)
        return 1;
    if (misc_robustness("PINE", bad, CBM_LANG_PINE, "ind.pine") != 0)
        return 1;
    return misc_pipeline_battery("PINE", "ind.pine", src);
}

/* ── RESCRIPT (callable) ─────────────────────────────────────────────────────
 * Idiomatic ReScript module: a let-bound function `add` and a let-bound function
 * `compute` that calls `add` inside its body. rescript_func_types = {"function"}
 * -> "Function"; rescript_call_types = {"call_expression"} -> call extraction;
 * rescript_class_types = {"module_declaration", "type_declaration"}.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the let-bound functions.
 * Dim 6 expected GREEN: call to "add" inside compute.
 * Dim 7 expected RED: ReScript has no cross-LSP rescue; the enclosing-func walk
 *   for the `function` node may fall back to Module for the in-body call.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_rescript) {
    static const char src[] =
        "let add = (a, b) => a + b\n"
        "\n"
        "let compute = x => {\n"
        "  let result = add(x, 1)\n"
        "  result\n"
        "}\n";
    static const char bad[] = "let compute = x => {\n  let result = add(";
    if (misc_single_file_battery("RESCRIPT", src, CBM_LANG_RESCRIPT, "Calc.res",
                                 "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("RESCRIPT", bad, CBM_LANG_RESCRIPT, "Calc.res") != 0)
        return 1;
    return misc_pipeline_battery("RESCRIPT", "Calc.res", src);
}

/* ── SQL (callable) ──────────────────────────────────────────────────────────
 * Idiomatic PostgreSQL PL/pgSQL: a create_function defining `add`, and a second
 * create_function `compute` whose body invokes `add(...)`. sql_func_types =
 * {"create_function", "function_declaration"} -> "Function"; sql_call_types =
 * {"function_call", "invocation", "command"} -> call extraction.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the create_function defs.
 * Dim 6 expected GREEN: call to "add" inside compute (function_call / invocation).
 * Dim 7 expected RED: SQL has no cross-LSP rescue; calls inside the function body
 *   string may not resolve to the enclosing create_function via the tree-sitter
 *   walk, falling back to Module. Dim 7 may also fail vacuously if the call is not
 *   captured as a CALLS edge. RED documents the gap.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_sql) {
    static const char src[] =
        "CREATE FUNCTION add(a integer, b integer) RETURNS integer AS $$\n"
        "BEGIN\n"
        "  RETURN a + b;\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;\n"
        "\n"
        "CREATE FUNCTION compute(x integer) RETURNS integer AS $$\n"
        "BEGIN\n"
        "  RETURN add(x, 1);\n"
        "END;\n"
        "$$ LANGUAGE plpgsql;\n";
    static const char bad[] = "CREATE FUNCTION add(a integer) RETURNS integer AS $$\nBEGIN\n  RETURN add(";
    if (misc_single_file_battery("SQL", src, CBM_LANG_SQL, "fn.sql",
                                 "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("SQL", bad, CBM_LANG_SQL, "fn.sql") != 0)
        return 1;
    return misc_pipeline_battery("SQL", "fn.sql", src);
}

/* ── SQUIRREL (callable) ─────────────────────────────────────────────────────
 * Idiomatic Squirrel: a free function `add` and a free function `compute` that
 * calls `add()` inside its body. squirrel_func_types = {"function_declaration",
 * "anonymous_function", "lambda_expression"} -> "Function";
 * squirrel_call_types = {"call_expression"} -> call extraction;
 * squirrel_class_types = {"class_declaration", "enum_declaration"} -> "Class".
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the function defs.
 * Dim 6 expected GREEN: call to "add" inside compute.
 * Dim 7 expected RED: Squirrel has no cross-LSP rescue; the enclosing-func walk
 *   for the function_declaration node may fall back to Module for the in-body call.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_squirrel) {
    static const char src[] =
        "function add(a, b) {\n"
        "    return a + b;\n"
        "}\n"
        "\n"
        "function compute(x) {\n"
        "    return add(x, 1);\n"
        "}\n";
    static const char bad[] = "function add(a, b) {\n    return add(";
    if (misc_single_file_battery("SQUIRREL", src, CBM_LANG_SQUIRREL, "calc.nut",
                                 "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("SQUIRREL", bad, CBM_LANG_SQUIRREL, "calc.nut") != 0)
        return 1;
    return misc_pipeline_battery("SQUIRREL", "calc.nut", src);
}

/* ── SYSTEMVERILOG (callable) ────────────────────────────────────────────────
 * Idiomatic SystemVerilog module: a function `add` (function_declaration) and an
 * initial block / always block that invokes `add(...)` and a system task.
 * systemverilog_func_types = {"function_declaration", "task_declaration",
 * "function_body_declaration", "function_statement"} -> "Function";
 * systemverilog_call_types = {"function_subroutine_call", "system_tf_call",
 * "method_call"} -> call extraction; systemverilog_class_types includes
 * module_declaration / class_declaration.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the function `add`.
 * Dim 6 expected GREEN: call to "add" (function_subroutine_call) inside the block.
 * Dim 7 expected RED: SystemVerilog has no cross-LSP rescue; the enclosing-func
 *   walk may attribute the in-body call at Module (or at the enclosing
 *   module/class node, which is not a Function/Method). RED documents the gap.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_systemverilog) {
    static const char src[] =
        "module calc;\n"
        "  function automatic int add(int a, int b);\n"
        "    return a + b;\n"
        "  endfunction\n"
        "\n"
        "  function automatic int compute(int x);\n"
        "    return add(x, 1);\n"
        "  endfunction\n"
        "endmodule\n";
    static const char bad[] = "module calc;\n  function automatic int add(int a);\n    return add(";
    if (misc_single_file_battery("SYSTEMVERILOG", src, CBM_LANG_SYSTEMVERILOG,
                                 "calc.sv", "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("SYSTEMVERILOG", bad, CBM_LANG_SYSTEMVERILOG,
                        "calc.sv") != 0)
        return 1;
    return misc_pipeline_battery("SYSTEMVERILOG", "calc.sv", src);
}

/* ── TABLEGEN (structural) ───────────────────────────────────────────────────
 * Idiomatic LLVM TableGen: a class definition and a def (record) that inherits
 * from it. tablegen_func_types = {"def", "multiclass", "defm"} -> "Function";
 * tablegen_class_types = {"class"} -> "Class". TableGen has no call_types -> no
 * calls/pipeline dims.
 *
 * Dims asserted: 1-5 ("Function" for the def, "Class" for the class) + R.
 * Expected: dims 1-4 + R GREEN; dim 5 GREEN if def -> "Function" and class ->
 * "Class" both mint. Dim 5 RED would document the TableGen def/class path gap.
 */
TEST(repro_grammar_misc_tablegen) {
    static const char src[] =
        "class Instruction {\n"
        "  string Namespace = \"CBM\";\n"
        "  bits<8> Opcode = 0;\n"
        "}\n"
        "\n"
        "def ADD : Instruction {\n"
        "  let Opcode = 1;\n"
        "}\n"
        "\n"
        "def SUB : Instruction {\n"
        "  let Opcode = 2;\n"
        "}\n";
    static const char bad[] = "class Instruction {\n  string Namespace = ";
    if (misc_single_file_battery("TABLEGEN", src, CBM_LANG_TABLEGEN, "instr.td",
                                 "Function", "Class", NULL) != 0)
        return 1;
    return misc_robustness("TABLEGEN", bad, CBM_LANG_TABLEGEN, "instr.td");
}

/* ── TEMPL (callable) ────────────────────────────────────────────────────────
 * Idiomatic templ (a-h/templ) file: a Go helper `greeting` (function_declaration)
 * and a Go function `compute` that calls `greeting(...)` inside its body. The
 * templ spec maps templ_func_types = {"function_declaration", "method_declaration",
 * "method_elem"} -> "Function"; templ_call_types = {"call_expression"} -> call
 * extraction; templ_class_types include component_declaration / type defs.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the Go function defs.
 * Dim 6 expected GREEN: call to "greeting" inside compute.
 * Dim 7 expected RED: templ has no cross-LSP rescue; the enclosing-func walk for
 *   the function_declaration node may fall back to Module for the in-body call.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_templ) {
    static const char src[] =
        "package main\n"
        "\n"
        "func greeting(name string) string {\n"
        "    return \"Hello, \" + name\n"
        "}\n"
        "\n"
        "func compute(name string) string {\n"
        "    return greeting(name)\n"
        "}\n";
    static const char bad[] = "package main\nfunc greeting(name string) string {\n    return greeting(";
    if (misc_single_file_battery("TEMPL", src, CBM_LANG_TEMPL, "page.templ",
                                 "Function", NULL, "greeting") != 0)
        return 1;
    if (misc_robustness("TEMPL", bad, CBM_LANG_TEMPL, "page.templ") != 0)
        return 1;
    return misc_pipeline_battery("TEMPL", "page.templ", src);
}

/* ── VERILOG (callable) ──────────────────────────────────────────────────────
 * Idiomatic Verilog module: a function `add` (function_declaration) and a second
 * function `compute` whose body invokes `add(...)`. verilog_func_types =
 * {"function_declaration", "task_declaration", "function_body_declaration",
 * "function_statement"} -> "Function"; verilog_call_types = {"system_tf_call",
 * "subroutine_call", "function_subroutine_call", "method_call"} -> call
 * extraction; verilog_class_types include module_declaration / class_declaration.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the function `add`.
 * Dim 6 expected GREEN: call to "add" (subroutine_call / function_subroutine_call).
 * Dim 7 expected RED: Verilog has no cross-LSP rescue; the in-body call may be
 *   sourced at Module (or at the non-callable enclosing module_declaration node).
 *   RED documents the attribution gap.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_verilog) {
    static const char src[] =
        "module calc;\n"
        "  function integer add(input integer a, input integer b);\n"
        "    add = a + b;\n"
        "  endfunction\n"
        "\n"
        "  function integer compute(input integer x);\n"
        "    compute = add(x, 1);\n"
        "  endfunction\n"
        "endmodule\n";
    static const char bad[] = "module calc;\n  function integer add(input integer a);\n    add = add(";
    if (misc_single_file_battery("VERILOG", src, CBM_LANG_VERILOG, "calc.v",
                                 "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("VERILOG", bad, CBM_LANG_VERILOG, "calc.v") != 0)
        return 1;
    return misc_pipeline_battery("VERILOG", "calc.v", src);
}

/* ── VHDL (callable) ─────────────────────────────────────────────────────────
 * Idiomatic VHDL package body: a function `add` (subprogram_definition) and a
 * function `compute` whose body calls `add(...)`. vhdl_func_types =
 * {"subprogram_declaration", "subprogram_definition"} -> "Function";
 * vhdl_call_types = {"function_call", "procedure_call_statement",
 * "component_instantiation_statement"} -> call extraction; vhdl_class_types
 * include entity/architecture/package declarations.
 *
 * Dims asserted: 1-8 + R.
 * Dim 5 expected GREEN: "Function" for the subprogram defs.
 * Dim 6 expected GREEN: call to "add" (function_call) inside compute.
 * Dim 7 expected RED: VHDL has no cross-LSP rescue; the enclosing-func walk for
 *   the subprogram_definition node may fall back to Module for the in-body call.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_misc_vhdl) {
    static const char src[] =
        "package body calc is\n"
        "  function add(a : integer; b : integer) return integer is\n"
        "  begin\n"
        "    return a + b;\n"
        "  end function;\n"
        "\n"
        "  function compute(x : integer) return integer is\n"
        "  begin\n"
        "    return add(x, 1);\n"
        "  end function;\n"
        "end package body;\n";
    static const char bad[] = "package body calc is\n  function add(a : integer) return integer is\n  begin\n    return add(";
    if (misc_single_file_battery("VHDL", src, CBM_LANG_VHDL, "calc.vhd",
                                 "Function", NULL, "add") != 0)
        return 1;
    if (misc_robustness("VHDL", bad, CBM_LANG_VHDL, "calc.vhd") != 0)
        return 1;
    return misc_pipeline_battery("VHDL", "calc.vhd", src);
}

/* ── Suite ──────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_misc) {
    RUN_TEST(repro_grammar_misc_assembly);
    RUN_TEST(repro_grammar_misc_beancount);
    RUN_TEST(repro_grammar_misc_bicep);
    RUN_TEST(repro_grammar_misc_cfml);
    RUN_TEST(repro_grammar_misc_cfscript);
    RUN_TEST(repro_grammar_misc_linkerscript);
    RUN_TEST(repro_grammar_misc_pine);
    RUN_TEST(repro_grammar_misc_rescript);
    RUN_TEST(repro_grammar_misc_sql);
    RUN_TEST(repro_grammar_misc_squirrel);
    RUN_TEST(repro_grammar_misc_systemverilog);
    RUN_TEST(repro_grammar_misc_tablegen);
    RUN_TEST(repro_grammar_misc_templ);
    RUN_TEST(repro_grammar_misc_verilog);
    RUN_TEST(repro_grammar_misc_vhdl);
}
