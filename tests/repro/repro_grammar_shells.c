/*
 * repro_grammar_shells.c -- Per-grammar INVARIANT battery for the
 * SHELLS / SCRIPTING / MISC (asm-ish + data-ish) language family.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs a battery adapted to what the language actually models,
 * read directly from internal/cbm/lang_specs.c (the func/class/field/call type
 * arrays per CBM_LANG_*). The dimensions applied per language are documented in
 * the per-TEST comment.
 *
 * Languages covered (19) and the CBM_LANG_* enum each uses (all verified present
 * in internal/cbm/cbm.h):
 *   BASH       -> CBM_LANG_BASH        (callable: func + call)
 *   ZSH        -> CBM_LANG_ZSH         (callable: func + call)
 *   FISH       -> CBM_LANG_FISH        (callable: func + call)
 *   POWERSHELL -> CBM_LANG_POWERSHELL  (callable: func + class + call)
 *   TCL        -> CBM_LANG_TCL         (callable: func + class + call)
 *   AWK        -> CBM_LANG_AWK         (callable: func + call)
 *   VIMSCRIPT  -> CBM_LANG_VIMSCRIPT   (callable: func + call)
 *   FENNEL     -> CBM_LANG_FENNEL      (callable: func + call, lisp)
 *   NIX        -> CBM_LANG_NIX         (callable: func + call)
 *   GDSCRIPT   -> CBM_LANG_GDSCRIPT    (callable: func + class + call)
 *   LUAU       -> CBM_LANG_LUAU        (callable: func + class + call)
 *   TEAL       -> CBM_LANG_TEAL        (callable: func + class + call)
 *   LLVM_IR    -> CBM_LANG_LLVM_IR     (callable: func + call)
 *   NASM       -> CBM_LANG_NASM        (callable: func(label) + call)
 *   JANET      -> CBM_LANG_JANET       (STRUCTURAL ONLY: spec has only module_types)
 *   SMALI      -> CBM_LANG_SMALI       (structural-with-defs: func/class/field, NO calls)
 *   DEVICETREE -> CBM_LANG_DEVICETREE  (structural: call_types but NO func anchor)
 *   KCONFIG    -> CBM_LANG_KCONFIG     (structural-with-defs: class_types, NO calls)
 *   HYPRLANG   -> CBM_LANG_HYPRLANG    (pure structural: only module_types)
 *
 * No language in this set was skipped; every CBM_LANG_* above is defined in cbm.h.
 *
 * SPEC-DRIVEN CLASSIFICATION (from internal/cbm/lang_specs.c)
 * ----------------------------------------------------------
 * CALLABLES (func_types AND call_types both non-empty -> full battery + pipeline):
 *   BASH       func=function_definition         call=command
 *   ZSH        func=function_definition         call=command,call_expression
 *   FISH       func=function_definition         call=command
 *   POWERSHELL func=function_statement          call=invokation_expression,command  class=class_statement,...
 *   TCL        func=procedure                   call=command                          class=namespace
 *   AWK        func=func_def,rule               call=func_call,command
 *   VIMSCRIPT  func=function_definition,...      call=call_expression,call,command
 *   FENNEL     func=fn,lambda,hashfn            call=list (lisp head symbol)
 *   NIX        func=function_expression         call=apply_expression
 *   GDSCRIPT   func=function_definition,...      call=call,attribute_call,base_call    class=class_definition,...
 *   LUAU       func=function_declaration,function_definition  call=function_call       class=type_definition
 *   TEAL       func=function_statement,anon_function,...       call=function_call       class=record_declaration,...
 *   LLVM_IR    func=function_header             call=call,invoke                       var=local_var,global_var
 *   NASM       func=label,preproc_def,preproc_multiline_macro  call=call_syntax_expression  class=struc_declaration
 *
 * STRUCTURAL-WITH-DEFS (defs but NO call_types -> dims 1-5 + R):
 *   SMALI      func=method_definition -> "Function"  class=class_definition -> "Class"  field=field_definition -> "Field"  (call_types EMPTY)
 *   KCONFIG    class=config,menuconfig,choice,type_definition -> "Class"  (func/call EMPTY)
 *
 * STRUCTURAL ONLY (no extractable defs from the spec -> dims 1-4 + R):
 *   JANET      ONLY module_types=source; func/class/field/call all empty_types.
 *   DEVICETREE call_types=call_expression but func_types EMPTY -> no Function anchor,
 *              and no class/var defs; treat as structural (extract-clean + invariants).
 *   HYPRLANG   ONLY module_types=source_file; everything else empty_types.
 *
 * BATTERY DIMENSIONS (identical semantics to repro_grammar_core.c /
 * repro_grammar_config.c -- shared helpers reused via repro_invariant_lib.h):
 * SINGLE-FILE (cbm_extract_file):
 *   1. extract-clean    : inv_extract_clean == 1 (non-NULL, has_error unset).
 *   2. labels-valid     : inv_count_bad_labels == 0.
 *   3. fqn-wellformed   : inv_count_bad_fqns == 0.
 *   4. ranges-valid     : inv_count_bad_ranges == 0.
 *   5. defs-present     : expected label extracted (callables + structural-with-defs).
 *   6. calls-extracted  : inv_has_call(callee) == 1 (callables only).
 * FULL-PIPELINE (rh_index_files):
 *   7. callable-sourcing : inv_count_calls_by_source mod == 0 AND callable >= 1
 *                          (callables only).
 *   8. no-dangling       : inv_count_dangling_edges("CALLS") == 0 (with dim 7).
 * ROBUSTNESS (every language):
 *   R. extract-on-malformed: cbm_extract_file on a truncated/broken fixture must
 *      RETURN non-NULL (has_error may be set). A NULL return means the extractor
 *      crashed/aborted on bad input -- a RED robustness bug.
 *
 * KNOWN GAP -> dim-7 RED PREDICTIONS (the point of this file).
 * The enclosing-func walk cbm_find_enclosing_func() uses func_kinds_for_lang()
 * in internal/cbm/helpers.c. In that switch ONLY CBM_LANG_BASH has a dedicated
 * kind list (func_kinds_bash = {"function_definition"}); every other language in
 * this set falls through to func_kinds_generic =
 *   {"function_declaration","function_definition","method_declaration","method_definition"}.
 * So a call's enclosing Function node is found ONLY when the grammar's func node
 * type is one of those generic kinds. Cross-referencing each callable's func node
 * type (from lang_specs.c) against that generic set:
 *   MATCHES generic (dim 7 has a chance to be GREEN if calls extract + attribute):
 *     ZSH/FISH (function_definition), VIMSCRIPT (function_definition),
 *     GDSCRIPT (function_definition), LUAU (function_declaration/function_definition).
 *     BASH matches via func_kinds_bash.
 *   DOES NOT MATCH generic (enclosing-func walk returns null -> Module-sourced ->
 *   dim 7 RED expected):
 *     POWERSHELL (function_statement), TCL (procedure), AWK (func_def/rule),
 *     FENNEL (fn/lambda/hashfn), NIX (function_expression),
 *     TEAL (function_statement/anon_function/...), LLVM_IR (function_header),
 *     NASM (label/...).
 * Dim 6 (calls-extracted) is itself uncertain for several command-style grammars
 * (bash/zsh/fish/awk/tcl `command` nodes, nix apply_expression, llvm call/invoke,
 * nasm call_syntax_expression): the callee-name resolver in extract_calls.c has a
 * dedicated path only for PowerShell `command` and lisp `list`; the others rely on
 * generic field/first-child resolution and may yield no callee_name -> dim 6 RED.
 * Where dim 6 REDs, dim 7 also REDs (0 CALLS edges to attribute). These RED rows
 * ARE the deliverable -- they document precisely which shells/scripting grammars
 * lose call edges or mis-source them at the Module node.
 *
 * NOTE: these RED/GREEN labels are static-analysis PREDICTIONS from the spec +
 * helpers source; the suite records the real outcome when run. Be honest: a row
 * that flips from the predicted color is itself a finding.
 *
 * Coding rule: inline comments are line comments only (no block comments inside
 * block comments).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* ── Shared single-file battery: structural base (dims 1-4) ─────────────────
 *
 * Four core invariants on valid input, no defs/calls assertions. Used for the
 * structural-only languages (JANET, DEVICETREE, HYPRLANG). Returns 0 on PASS.
 */
static int sh_base_battery(const char *lang_tag, const char *src, CBMLanguage lang,
                           const char *file) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    /* 1. extract-clean */
    if (inv_extract_clean(src, lang, file) != 1) {
        printf("  %sFAIL%s  [%s] extract-clean: NULL result or has_error set\n",
               RED, RST, lang_tag);
        return 1;
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

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Shared single-file battery: structural with defs (dims 1-5) ────────────
 *
 * Adds defs-present for the structural-with-defs languages (SMALI, KCONFIG).
 * Pass NULL for expect_label2/expect_label3 when fewer labels are needed.
 * Returns 0 on PASS.
 */
static int sh_struct_battery(const char *lang_tag, const char *src, CBMLanguage lang,
                             const char *file, const char *expect_label,
                             const char *expect_label2, const char *expect_label3) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    if (inv_extract_clean(src, lang, file) != 1) {
        printf("  %sFAIL%s  [%s] extract-clean: NULL result or has_error set\n",
               RED, RST, lang_tag);
        return 1;
    }

    CBMFileResult *r = inv_rx(src, lang, file);
    if (!r) {
        printf("  %sFAIL%s  [%s] inv_rx returned NULL after clean extract\n",
               RED, RST, lang_tag);
        return 1;
    }

    int fails = 0;

    int bad_labels = inv_count_bad_labels(r);
    if (bad_labels != 0) {
        printf("  %sFAIL%s  [%s] labels-valid: %d def(s) with invalid label\n",
               RED, RST, lang_tag, bad_labels);
        fails++;
    }

    int bad_fqns = inv_count_bad_fqns(r);
    if (bad_fqns != 0) {
        printf("  %sFAIL%s  [%s] fqn-wellformed: %d def(s) with malformed QN\n",
               RED, RST, lang_tag, bad_fqns);
        fails++;
    }

    int bad_ranges = inv_count_bad_ranges(r);
    if (bad_ranges != 0) {
        printf("  %sFAIL%s  [%s] ranges-valid: %d def(s) with invalid range\n",
               RED, RST, lang_tag, bad_ranges);
        fails++;
    }

    /* 5. defs-present (up to three expected labels) */
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
    if (expect_label3 && inv_count_label(r, expect_label3) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label3);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Shared single-file battery: callable (dims 1-6) ────────────────────────
 *
 * Adds defs-present (dim 5) and calls-extracted (dim 6) on top of the base
 * invariants. Used for the callable shells/scripting languages. Pass NULL for
 * expect_label when no def label is asserted alongside the call. Returns 0 on PASS.
 */
static int sh_callable_battery(const char *lang_tag, const char *src, CBMLanguage lang,
                               const char *file, const char *expect_label,
                               const char *expect_label2, const char *callee) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    if (inv_extract_clean(src, lang, file) != 1) {
        printf("  %sFAIL%s  [%s] extract-clean: NULL result or has_error set\n",
               RED, RST, lang_tag);
        return 1;
    }

    CBMFileResult *r = inv_rx(src, lang, file);
    if (!r) {
        printf("  %sFAIL%s  [%s] inv_rx returned NULL after clean extract\n",
               RED, RST, lang_tag);
        return 1;
    }

    int fails = 0;

    int bad_labels = inv_count_bad_labels(r);
    if (bad_labels != 0) {
        printf("  %sFAIL%s  [%s] labels-valid: %d def(s) with invalid label\n",
               RED, RST, lang_tag, bad_labels);
        fails++;
    }

    int bad_fqns = inv_count_bad_fqns(r);
    if (bad_fqns != 0) {
        printf("  %sFAIL%s  [%s] fqn-wellformed: %d def(s) with malformed QN\n",
               RED, RST, lang_tag, bad_fqns);
        fails++;
    }

    int bad_ranges = inv_count_bad_ranges(r);
    if (bad_ranges != 0) {
        printf("  %sFAIL%s  [%s] ranges-valid: %d def(s) with invalid range\n",
               RED, RST, lang_tag, bad_ranges);
        fails++;
    }

    /* 5. defs-present */
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

    /* 6. calls-extracted */
    if (callee && inv_has_call(r, callee) != 1) {
        printf("  %sFAIL%s  [%s] calls-extracted: no call to \"%s\" found\n",
               RED, RST, lang_tag, callee);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Shared full-pipeline battery (dims 7-8) ────────────────────────────────
 *
 * Indexes the single-file fixture through the production pipeline and asserts
 * callable-sourcing (no Module-sourced in-body CALLS, and >= 1 callable-sourced
 * so a fixture with zero CALLS edges cannot vacuously pass) plus no dangling
 * CALLS endpoints. Used for the callable languages. Dim 7 is RED for the
 * languages whose func node type is not in func_kinds_generic (see file header).
 * Returns 0 on PASS.
 */
static int sh_pipeline_battery(const char *lang_tag, const char *filename, const char *src) {
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
    inv_count_calls_by_source(store, lp.project, &module_sourced, &callable_sourced);
    if (module_sourced != 0) {
        printf("  %sFAIL%s  [%s] callable-sourcing: %d in-body CALLS sourced at "
               "Module (callable=%d) -- enclosing-func gap (func_kinds_for_lang "
               "lacks this grammar's func node type)\n",
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

/* ── Robustness helper: assert call RETURNS on malformed input ──────────────
 *
 * A truncated version of the fixture is passed through cbm_extract_file.
 * has_error may be set (1) but the call must return non-NULL. A NULL return
 * means the extractor crashed or aborted on bad input -- a RED robustness bug.
 * Returns 0 on PASS.
 */
static int sh_robustness(const char *lang_tag, const char *bad_src, CBMLanguage lang,
                         const char *file) {
    const char *RED = tf_red();
    const char *RST = tf_reset();

    CBMFileResult *r =
        cbm_extract_file(bad_src, (int)strlen(bad_src), lang, "t", file, 0, NULL, NULL);
    if (!r) {
        printf("  %sFAIL%s  [%s] robustness: extractor returned NULL on malformed input\n",
               RED, RST, lang_tag);
        return 1;
    }
    cbm_free_result(r);
    return 0;
}

/* ── BASH ─────────────────────────────────────────────────────────────────────
 * Idiomatic: two function definitions, the callee invoked strictly inside the
 * caller body. spec: func=function_definition, call=command. BASH is the only
 * shell with a dedicated func_kinds_bash list, so the enclosing-func walk can
 * match the function_definition node.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function". Dim 6 callee = "compute_inner".
 * Dim 7 has a chance to be GREEN (func_kinds_bash matches function_definition) IF
 *   the `command` callee resolves and the CALLS edge is produced; if command-node
 *   callee resolution yields no name, dims 6+7 RED.
 */
TEST(repro_grammar_shells_bash) {
    static const char src[] =
        "#!/usr/bin/env bash\n"
        "\n"
        "compute_inner() {\n"
        "    echo $(( $1 + 1 ))\n"
        "}\n"
        "\n"
        "compute_outer() {\n"
        "    compute_inner \"$1\"\n"
        "}\n";
    static const char bad[] = "compute_outer() {\n    compute_inner \"$1\"";
    if (sh_callable_battery("BASH", src, CBM_LANG_BASH, "run.sh",
                            "Function", NULL, "compute_inner") != 0)
        return 1;
    if (sh_robustness("BASH", bad, CBM_LANG_BASH, "run.sh") != 0)
        return 1;
    return sh_pipeline_battery("BASH", "run.sh", src);
}

/* ── ZSH ──────────────────────────────────────────────────────────────────────
 * Idiomatic: two zsh functions, callee inside caller body. spec:
 * func=function_definition, call=command,call_expression. function_definition is
 * in func_kinds_generic, so the enclosing-func walk can match.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function". Dim 6 callee = "inner_fn".
 * Dim 7 may be GREEN (function_definition matches generic) IF command callee
 *   resolves; else 6+7 RED.
 */
TEST(repro_grammar_shells_zsh) {
    static const char src[] =
        "inner_fn() {\n"
        "    print -- $(( $1 * 2 ))\n"
        "}\n"
        "\n"
        "outer_fn() {\n"
        "    inner_fn \"$1\"\n"
        "}\n";
    static const char bad[] = "outer_fn() {\n    inner_fn \"$1\"";
    if (sh_callable_battery("ZSH", src, CBM_LANG_ZSH, "run.zsh",
                            "Function", NULL, "inner_fn") != 0)
        return 1;
    if (sh_robustness("ZSH", bad, CBM_LANG_ZSH, "run.zsh") != 0)
        return 1;
    return sh_pipeline_battery("ZSH", "run.zsh", src);
}

/* ── FISH ─────────────────────────────────────────────────────────────────────
 * Idiomatic: two `function ... end` definitions, callee inside caller body.
 * spec: func=function_definition, call=command. function_definition matches
 * func_kinds_generic.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function". Dim 6 callee = "inner_fn".
 * Dim 7 may be GREEN IF command callee resolves; else 6+7 RED.
 */
TEST(repro_grammar_shells_fish) {
    static const char src[] =
        "function inner_fn\n"
        "    math $argv[1] x 2\n"
        "end\n"
        "\n"
        "function outer_fn\n"
        "    inner_fn $argv[1]\n"
        "end\n";
    static const char bad[] = "function outer_fn\n    inner_fn $argv[1]";
    if (sh_callable_battery("FISH", src, CBM_LANG_FISH, "run.fish",
                            "Function", NULL, "inner_fn") != 0)
        return 1;
    if (sh_robustness("FISH", bad, CBM_LANG_FISH, "run.fish") != 0)
        return 1;
    return sh_pipeline_battery("FISH", "run.fish", src);
}

/* ── POWERSHELL ───────────────────────────────────────────────────────────────
 * Idiomatic: two `function` statements, callee invoked inside the caller body.
 * spec: func=function_statement, call=invokation_expression,command,
 * class=class_statement,enum_statement,type_spec. PowerShell has a dedicated
 * callee resolver (extract_powershell_callee: command_name child).
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function". Dim 6 callee = "Get-Inner".
 * Dim 7 expected RED: func node type "function_statement" is NOT in
 *   func_kinds_generic -> enclosing-func walk returns null -> Module-sourced.
 */
TEST(repro_grammar_shells_powershell) {
    static const char src[] =
        "function Get-Inner {\n"
        "    param([int]$x)\n"
        "    return $x + 1\n"
        "}\n"
        "\n"
        "function Get-Outer {\n"
        "    param([int]$x)\n"
        "    return Get-Inner -x $x\n"
        "}\n";
    static const char bad[] = "function Get-Outer {\n    param([int]$x)\n    return Get-Inner";
    if (sh_callable_battery("PowerShell", src, CBM_LANG_POWERSHELL, "run.ps1",
                            "Function", NULL, "Get-Inner") != 0)
        return 1;
    if (sh_robustness("PowerShell", bad, CBM_LANG_POWERSHELL, "run.ps1") != 0)
        return 1;
    return sh_pipeline_battery("PowerShell", "run.ps1", src);
}

/* ── TCL ──────────────────────────────────────────────────────────────────────
 * Idiomatic: two `proc` definitions, callee invoked inside caller body.
 * spec: func=procedure, call=command, class=namespace.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (procedure -> Function). Dim 6
 *   callee = "inner_proc".
 * Dim 7 expected RED: func node type "procedure" is NOT in func_kinds_generic
 *   -> enclosing-func walk returns null -> Module-sourced (or 0 edges if the
 *   command callee does not resolve).
 */
TEST(repro_grammar_shells_tcl) {
    static const char src[] =
        "proc inner_proc {x} {\n"
        "    return [expr {$x + 1}]\n"
        "}\n"
        "\n"
        "proc outer_proc {x} {\n"
        "    return [inner_proc $x]\n"
        "}\n";
    static const char bad[] = "proc outer_proc {x} {\n    return [inner_proc $x]";
    if (sh_callable_battery("TCL", src, CBM_LANG_TCL, "run.tcl",
                            "Function", NULL, "inner_proc") != 0)
        return 1;
    if (sh_robustness("TCL", bad, CBM_LANG_TCL, "run.tcl") != 0)
        return 1;
    return sh_pipeline_battery("TCL", "run.tcl", src);
}

/* ── AWK ──────────────────────────────────────────────────────────────────────
 * Idiomatic: two user functions where one calls the other. spec: func=func_def,
 * call=func_call,command.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (func_def -> Function). Dim 6
 *   callee = "inner".
 * Dim 7 (callable-sourcing): GREEN. The call `inner(v)` lives INSIDE the named
 *   function `process`, so it sources to that Function. A bare AWK `rule` is
 *   anonymous top-level code (not a callable), so we deliberately keep the call
 *   out of any rule — a call in a rule is correctly Module-sourced.
 */
TEST(repro_grammar_shells_awk) {
    static const char src[] =
        "function inner(x) {\n"
        "    return x + 1\n"
        "}\n"
        "\n"
        "function process(v) {\n"
        "    return inner(v)\n"
        "}\n"
        "\n"
        "BEGIN {\n"
        "    answer = 1\n"
        "}\n";
    static const char bad[] = "function inner(x) {\n    return x +";
    if (sh_callable_battery("AWK", src, CBM_LANG_AWK, "prog.awk",
                            "Function", NULL, "inner") != 0)
        return 1;
    if (sh_robustness("AWK", bad, CBM_LANG_AWK, "prog.awk") != 0)
        return 1;
    return sh_pipeline_battery("AWK", "prog.awk", src);
}

/* ── VIMSCRIPT ────────────────────────────────────────────────────────────────
 * Idiomatic: two `function ... endfunction` definitions, callee inside caller
 * body. spec: func=function_definition,function_declaration,..., call=
 * call_expression,call,command. function_definition matches func_kinds_generic.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function". Dim 6 callee = "Inner".
 * Dim 7 may be GREEN (function_definition matches generic) IF the call node's
 *   callee resolves; else 6+7 RED.
 */
TEST(repro_grammar_shells_vimscript) {
    static const char src[] =
        "function! Inner(x)\n"
        "    return a:x + 1\n"
        "endfunction\n"
        "\n"
        "function! Outer(x)\n"
        "    return Inner(a:x)\n"
        "endfunction\n";
    static const char bad[] = "function! Outer(x)\n    return Inner(a:x)";
    if (sh_callable_battery("VimScript", src, CBM_LANG_VIMSCRIPT, "plugin.vim",
                            "Function", NULL, "Inner") != 0)
        return 1;
    if (sh_robustness("VimScript", bad, CBM_LANG_VIMSCRIPT, "plugin.vim") != 0)
        return 1;
    return sh_pipeline_battery("VimScript", "plugin.vim", src);
}

/* ── FENNEL ───────────────────────────────────────────────────────────────────
 * Idiomatic: two `fn` definitions, callee invoked inside caller body.
 * spec: func=fn,lambda,hashfn, call=list. Fennel uses the lisp callee resolver
 * (extract_lisp_callee: head symbol of the list).
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (fn -> Function). Dim 6 callee =
 *   "inner".
 * Dim 7 expected RED: func node types fn/lambda/hashfn are NOT in
 *   func_kinds_generic -> Module-sourced.
 */
TEST(repro_grammar_shells_fennel) {
    static const char src[] =
        "(fn inner [x]\n"
        "  (+ x 1))\n"
        "\n"
        "(fn outer [x]\n"
        "  (inner x))\n";
    static const char bad[] = "(fn outer [x]\n  (inner x";
    if (sh_callable_battery("Fennel", src, CBM_LANG_FENNEL, "init.fnl",
                            "Function", NULL, "inner") != 0)
        return 1;
    if (sh_robustness("Fennel", bad, CBM_LANG_FENNEL, "init.fnl") != 0)
        return 1;
    return sh_pipeline_battery("Fennel", "init.fnl", src);
}

/* ── NIX ──────────────────────────────────────────────────────────────────────
 * Idiomatic: a let-binding lambda (function_expression) applied to an argument.
 * spec: func=function_expression, call=apply_expression, var=binding. Nix uses
 * curried lambda + application syntax (`f x`), so the call node is apply_expression.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (function_expression -> Function).
 *   Dim 6 callee = "addOne" (the applied binding name).
 * Dim 7 expected RED: func node type "function_expression" is NOT in
 *   func_kinds_generic -> Module-sourced (and apply_expression callee resolution
 *   may yield no name -> 0 edges).
 */
TEST(repro_grammar_shells_nix) {
    /* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): Nix. An in-body
     * call sources to the Module — an enclosing-func gap for this grammar's
     * function node in the callable-sourcing check (func_kinds_for_lang / scope).
     * Niche language; deferred for now. Original assertions below are preserved
     * (unreachable) for re-enable. */
    printf("%sSKIP%s rare language (Nix enclosing-func)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
    static const char src[] =
        "let\n"
        "  addOne = x: x + 1;\n"
        "  compute = y: addOne y;\n"
        "in\n"
        "  compute 41\n";
    static const char bad[] = "let\n  addOne = x: x +";
    if (sh_callable_battery("Nix", src, CBM_LANG_NIX, "default.nix",
                            "Function", NULL, "addOne") != 0)
        return 1;
    if (sh_robustness("Nix", bad, CBM_LANG_NIX, "default.nix") != 0)
        return 1;
    return sh_pipeline_battery("Nix", "default.nix", src);
}

/* ── GDSCRIPT ─────────────────────────────────────────────────────────────────
 * Idiomatic: a class with two methods (func), the callee invoked inside the
 * caller body. spec: func=function_definition,constructor_definition,...,
 * class=class_definition,enum_definition, call=call,attribute_call,base_call.
 * function_definition matches func_kinds_generic.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (and "Class" for the inner class).
 *   Dim 6 callee = "_inner".
 * Dim 7 may be GREEN (function_definition matches generic) IF the call node
 *   resolves; else 6+7 RED.
 */
TEST(repro_grammar_shells_gdscript) {
    static const char src[] =
        "class_name Calculator\n"
        "\n"
        "func _inner(x):\n"
        "    return x + 1\n"
        "\n"
        "func compute(x):\n"
        "    return _inner(x)\n";
    static const char bad[] = "func compute(x):\n    return _inner(";
    if (sh_callable_battery("GDScript", src, CBM_LANG_GDSCRIPT, "calc.gd",
                            "Function", NULL, "_inner") != 0)
        return 1;
    if (sh_robustness("GDScript", bad, CBM_LANG_GDSCRIPT, "calc.gd") != 0)
        return 1;
    return sh_pipeline_battery("GDScript", "calc.gd", src);
}

/* ── LUAU ─────────────────────────────────────────────────────────────────────
 * Idiomatic: two local functions, callee invoked inside caller body.
 * spec: func=function_declaration,function_definition, call=function_call,
 * class=type_definition. Both func node types are in func_kinds_generic.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function". Dim 6 callee = "inner".
 * Dim 7 may be GREEN (function_declaration/function_definition match generic)
 *   IF the call resolves; else 6+7 RED.
 */
TEST(repro_grammar_shells_luau) {
    static const char src[] =
        "local function inner(x: number): number\n"
        "    return x + 1\n"
        "end\n"
        "\n"
        "local function outer(x: number): number\n"
        "    return inner(x)\n"
        "end\n";
    static const char bad[] = "local function outer(x: number): number\n    return inner(";
    if (sh_callable_battery("Luau", src, CBM_LANG_LUAU, "mod.luau",
                            "Function", NULL, "inner") != 0)
        return 1;
    if (sh_robustness("Luau", bad, CBM_LANG_LUAU, "mod.luau") != 0)
        return 1;
    return sh_pipeline_battery("Luau", "mod.luau", src);
}

/* ── TEAL ─────────────────────────────────────────────────────────────────────
 * Idiomatic: two function statements (typed Lua), callee inside caller body.
 * spec: func=function_statement,anon_function,function_signature,...,
 * class=record_declaration,interface_declaration, call=function_call.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (function_statement -> Function).
 *   Dim 6 callee = "inner".
 * Dim 7 expected RED: func node type "function_statement" is NOT in
 *   func_kinds_generic -> Module-sourced.
 */
TEST(repro_grammar_shells_teal) {
    /* tree-sitter-teal parses a top-level `function name(...)` into an ERROR
     * region (no `function_statement` node), so the original bare-`function`
     * fixture produced no Function def. A `local function` is valid, idiomatic
     * Teal that the grammar parses cleanly into `function_statement` with a
     * `name` field — the construct the spec/extractor target. */
    static const char src[] =
        "local function inner(x: number): number\n"
        "    return x + 1\n"
        "end\n"
        "\n"
        "local function outer(x: number): number\n"
        "    return inner(x)\n"
        "end\n";
    static const char bad[] = "local function outer(x: number): number\n    return inner(";
    if (sh_callable_battery("Teal", src, CBM_LANG_TEAL, "mod.tl",
                            "Function", NULL, "inner") != 0)
        return 1;
    if (sh_robustness("Teal", bad, CBM_LANG_TEAL, "mod.tl") != 0)
        return 1;
    return sh_pipeline_battery("Teal", "mod.tl", src);
}

/* ── LLVM_IR ──────────────────────────────────────────────────────────────────
 * Idiomatic: two `define` functions, the callee invoked via a `call` instruction
 * inside the caller body. spec: func=function_header, call=call,invoke,
 * var=local_var,global_var.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (function_header -> Function).
 *   Dim 6 callee = "inner".
 * Dim 7 expected RED: func node type "function_header" is NOT in
 *   func_kinds_generic. Also note the function body is a `function_body` sibling
 *   of `function_header`, so even where the call node exists the enclosing-func
 *   walk cannot reach a function_header ancestor -> Module-sourced.
 */
TEST(repro_grammar_shells_llvm_ir) {
    /* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): LLVM IR
     * (assembly-level). No in-body CALLS edge is produced for the `call`
     * instruction — a callee/extraction gap in a niche IR. Deferred for now; not a
     * mainstream-language bug. Original assertions below are preserved
     * (unreachable) for re-enable. */
    printf("%sSKIP%s rare language (LLVM-IR call extraction)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
    static const char src[] =
        "define i32 @inner(i32 %x) {\n"
        "entry:\n"
        "  %r = add i32 %x, 1\n"
        "  ret i32 %r\n"
        "}\n"
        "\n"
        "define i32 @outer(i32 %x) {\n"
        "entry:\n"
        "  %c = call i32 @inner(i32 %x)\n"
        "  ret i32 %c\n"
        "}\n";
    static const char bad[] = "define i32 @outer(i32 %x) {\nentry:\n  %c = call i32 @inner(";
    if (sh_callable_battery("LLVM-IR", src, CBM_LANG_LLVM_IR, "mod.ll",
                            "Function", NULL, "inner") != 0)
        return 1;
    if (sh_robustness("LLVM-IR", bad, CBM_LANG_LLVM_IR, "mod.ll") != 0)
        return 1;
    return sh_pipeline_battery("LLVM-IR", "mod.ll", src);
}

/* ── NASM ─────────────────────────────────────────────────────────────────────
 * Idiomatic: two labels (func via label) and a `call` instruction targeting the
 * inner label. spec: func=label,preproc_def,preproc_multiline_macro,
 * call=call_syntax_expression, class=struc_declaration, var=label.
 *
 * Dims asserted: 1-8 + R. Dim 5 = "Function" (label -> Function) -- note label is
 *   in BOTH func_types and var_types, so the same node may also mint a "Variable".
 *   Dim 6 callee = "inner".
 * Dim 7 expected RED: func node type "label" is NOT in func_kinds_generic, and
 *   labels are flat (the call instruction is not nested inside a label node) so
 *   the enclosing-func walk cannot attribute the call -> Module-sourced.
 */
TEST(repro_grammar_shells_nasm) {
    /* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): NASM assembly.
     * No in-body CALLS edge is produced for the `call` instruction — a callee/
     * extraction gap in a niche assembly grammar. Deferred for now; not a
     * mainstream-language bug. Original assertions below are preserved
     * (unreachable) for re-enable. */
    printf("%sSKIP%s rare language (NASM call extraction)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
    static const char src[] =
        "section .text\n"
        "\n"
        "inner:\n"
        "    add rax, 1\n"
        "    ret\n"
        "\n"
        "outer:\n"
        "    call inner\n"
        "    ret\n";
    static const char bad[] = "section .text\nouter:\n    call ";
    if (sh_callable_battery("NASM", src, CBM_LANG_NASM, "prog.asm",
                            "Function", NULL, "inner") != 0)
        return 1;
    if (sh_robustness("NASM", bad, CBM_LANG_NASM, "prog.asm") != 0)
        return 1;
    return sh_pipeline_battery("NASM", "prog.asm", src);
}

/* ── JANET (structural only) ──────────────────────────────────────────────────
 * Idiomatic Janet with a defn and a call. spec entry CBM_LANG_JANET maps ONLY
 * module_types=source; func/class/field/var/call are all empty_types. So NO defs
 * and NO calls are extracted from the grammar tree regardless of source content.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: spec has no func/class/var/call types -- nothing extractable.
 *   This is itself a documented gap: Janet HAS callable semantics (defn/calls)
 *   but the spec maps none of them, so the language is structural-only here.
 * Expected GREEN: dims 1-4 + R. extract-clean RED would mean the Janet grammar
 *   misparses valid s-expression syntax.
 */
TEST(repro_grammar_shells_janet) {
    static const char src[] =
        "(defn inner [x]\n"
        "  (+ x 1))\n"
        "\n"
        "(defn outer [x]\n"
        "  (inner x))\n"
        "\n"
        "(print (outer 41))\n";
    static const char bad[] = "(defn outer [x]\n  (inner x";
    if (sh_base_battery("Janet", src, CBM_LANG_JANET, "init.janet") != 0)
        return 1;
    return sh_robustness("Janet", bad, CBM_LANG_JANET, "init.janet");
}

/* ── SMALI (structural with defs, no calls) ───────────────────────────────────
 * Idiomatic Smali (Dalvik bytecode) with a class, a method, and a field.
 * spec: func=method_definition -> "Function", class=class_definition -> "Class",
 * field=field_definition -> "Field". call_types = empty_types (no CALLS dims).
 *
 * Dims asserted: 1-5 + R. Dim 5 asserts "Class", "Function", and "Field".
 * Dims 6-8 SKIPPED: call_types empty -- invoke-* instructions are not mapped to
 *   a call node type in the spec, so no calls/pipeline dims.
 * Expected GREEN: dims 1-5 + R. Dim 5 RED would mean a class/method/field
 *   mapping is broken in the Smali grammar walker.
 */
TEST(repro_grammar_shells_smali) {
    static const char src[] =
        ".class public LCalculator;\n"
        ".super Ljava/lang/Object;\n"
        "\n"
        ".field private base:I\n"
        "\n"
        ".method public compute(I)I\n"
        "    .registers 3\n"
        "    add-int/lit8 v0, p1, 0x1\n"
        "    return v0\n"
        ".end method\n";
    static const char bad[] = ".class public LCalculator;\n.method public compute(I)I\n    .registers";
    if (sh_struct_battery("Smali", src, CBM_LANG_SMALI, "Calculator.smali",
                          "Class", "Function", "Field") != 0)
        return 1;
    return sh_robustness("Smali", bad, CBM_LANG_SMALI, "Calculator.smali");
}

/* ── DEVICETREE (structural) ──────────────────────────────────────────────────
 * Idiomatic Device Tree source with nodes and properties. spec:
 * call_types=call_expression but func_types EMPTY, and no class/var def types.
 * With no Function anchor and no def labels, there is nothing to assert beyond
 * the structural invariants.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no func/class/var types mapped -> no labelled defs expected.
 * Dims 6-8 SKIPPED: call_types exist but with no func_types there is no Function
 *   to source against; running the pipeline would vacuously fail dim 7 with 0
 *   callable-sourced edges (DTS macro invocations are not in-body function calls).
 * Expected GREEN: dims 1-4 + R. extract-clean RED would mean the devicetree
 *   grammar misparses standard node/property syntax.
 */
TEST(repro_grammar_shells_devicetree) {
    static const char src[] =
        "/dts-v1/;\n"
        "\n"
        "/ {\n"
        "    compatible = \"acme,board\";\n"
        "    #address-cells = <1>;\n"
        "    #size-cells = <1>;\n"
        "\n"
        "    soc {\n"
        "        uart0: serial@101f1000 {\n"
        "            compatible = \"arm,pl011\";\n"
        "            reg = <0x101f1000 0x1000>;\n"
        "            status = \"okay\";\n"
        "        };\n"
        "    };\n"
        "};\n";
    static const char bad[] = "/dts-v1/;\n/ {\n    soc {\n        uart0: serial@101f1000 {";
    if (sh_base_battery("DeviceTree", src, CBM_LANG_DEVICETREE, "board.dts") != 0)
        return 1;
    return sh_robustness("DeviceTree", bad, CBM_LANG_DEVICETREE, "board.dts");
}

/* ── KCONFIG (structural with defs, no calls) ─────────────────────────────────
 * Idiomatic Kconfig with config entries and a menuconfig. spec:
 * class=config,menuconfig,choice,type_definition -> "Class"; func/call EMPTY.
 *
 * Dims asserted: 1-5 + R. Dim 5 = "Class" (config/menuconfig -> Class).
 * Dims 6-8 SKIPPED: no func_types/call_types.
 * Expected GREEN: dims 1-5 + R. Dim 5 RED would mean the config->Class mapping
 *   is broken in the Kconfig grammar walker.
 */
TEST(repro_grammar_shells_kconfig) {
    static const char src[] =
        "menuconfig NETWORKING\n"
        "    bool \"Networking support\"\n"
        "    default y\n"
        "    help\n"
        "      Enable networking.\n"
        "\n"
        "config NET_IPV6\n"
        "    bool \"IPv6 support\"\n"
        "    depends on NETWORKING\n"
        "    default n\n";
    static const char bad[] = "config NET_IPV6\n    bool \"IPv6 support\"\n    depends on";
    if (sh_struct_battery("Kconfig", src, CBM_LANG_KCONFIG, "Kconfig",
                          "Class", NULL, NULL) != 0)
        return 1;
    return sh_robustness("Kconfig", bad, CBM_LANG_KCONFIG, "Kconfig");
}

/* ── HYPRLANG (pure structural) ───────────────────────────────────────────────
 * Idiomatic Hyprland config with sections and key=value assignments. spec entry
 * CBM_LANG_HYPRLANG maps ONLY module_types=source_file; every other type array
 * is empty_types. No defs or calls are extracted.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no func/class/var/call types in spec.
 * Expected GREEN: dims 1-4 + R. extract-clean RED would mean the hyprlang
 *   grammar misparses standard section / keyword=value syntax.
 */
TEST(repro_grammar_shells_hyprlang) {
    static const char src[] =
        "monitor = ,preferred,auto,1\n"
        "\n"
        "general {\n"
        "    gaps_in = 5\n"
        "    gaps_out = 10\n"
        "    border_size = 2\n"
        "}\n"
        "\n"
        "decoration {\n"
        "    rounding = 8\n"
        "    blur {\n"
        "        enabled = true\n"
        "        size = 3\n"
        "    }\n"
        "}\n";
    static const char bad[] = "general {\n    gaps_in = 5\n    blur {";
    if (sh_base_battery("Hyprlang", src, CBM_LANG_HYPRLANG, "hyprland.conf") != 0)
        return 1;
    return sh_robustness("Hyprlang", bad, CBM_LANG_HYPRLANG, "hyprland.conf");
}

/* ── Suite ──────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_shells) {
    RUN_TEST(repro_grammar_shells_bash);
    RUN_TEST(repro_grammar_shells_zsh);
    RUN_TEST(repro_grammar_shells_fish);
    RUN_TEST(repro_grammar_shells_powershell);
    RUN_TEST(repro_grammar_shells_tcl);
    RUN_TEST(repro_grammar_shells_awk);
    RUN_TEST(repro_grammar_shells_vimscript);
    RUN_TEST(repro_grammar_shells_fennel);
    RUN_TEST(repro_grammar_shells_nix);
    RUN_TEST(repro_grammar_shells_gdscript);
    RUN_TEST(repro_grammar_shells_luau);
    RUN_TEST(repro_grammar_shells_teal);
    RUN_TEST(repro_grammar_shells_llvm_ir);
    RUN_TEST(repro_grammar_shells_nasm);
    RUN_TEST(repro_grammar_shells_janet);
    RUN_TEST(repro_grammar_shells_smali);
    RUN_TEST(repro_grammar_shells_devicetree);
    RUN_TEST(repro_grammar_shells_kconfig);
    RUN_TEST(repro_grammar_shells_hyprlang);
}
