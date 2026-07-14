/*
 * repro_grammar_markup.c -- Per-grammar INVARIANT battery for the
 * MARKUP / DOCS / SCHEMA family plus the REMAINING long-tail languages.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs a battery adapted to what the language actually models.
 * Most languages in this family are STRUCTURAL-ONLY or DOCS (no func_types, no
 * call sites) -- the battery is the four base invariants plus a robustness probe.
 * A handful carry real callables (Typst, QML, PureScript) and get the full
 * battery including pipeline callable-sourcing. The dimensions applied per
 * language are documented in each per-TEST comment.
 *
 * Languages covered (18) and the CBM_LANG_* enum each uses. All enums verified
 * present in internal/cbm/cbm.h (line numbers as of HEAD): MARKDOWN(62),
 * RST(150), TYPST(79), BIBTEX(128), MERMAID(152), PO(154), DIFF(118),
 * REGEX(148), CAPNP(125), SMITHY(159), WIT(160), QML(170), LIQUID(113),
 * JINJA2(114), BLADE(109), PURESCRIPT(97), SOQL(165), SOSL(166).
 * None missing; none skipped. (Note: the enum is CBM_LANG_JINJA2, not
 * CBM_LANG_JINJA.)
 *
 *   MARKDOWN   -> CBM_LANG_MARKDOWN
 *   RST        -> CBM_LANG_RST
 *   TYPST      -> CBM_LANG_TYPST
 *   BIBTEX     -> CBM_LANG_BIBTEX
 *   MERMAID    -> CBM_LANG_MERMAID
 *   PO         -> CBM_LANG_PO
 *   DIFF       -> CBM_LANG_DIFF
 *   REGEX      -> CBM_LANG_REGEX
 *   CAPNP      -> CBM_LANG_CAPNP
 *   SMITHY     -> CBM_LANG_SMITHY
 *   WIT        -> CBM_LANG_WIT
 *   QML        -> CBM_LANG_QML
 *   LIQUID     -> CBM_LANG_LIQUID
 *   JINJA2     -> CBM_LANG_JINJA2
 *   BLADE      -> CBM_LANG_BLADE
 *   PURESCRIPT -> CBM_LANG_PURESCRIPT
 *   SOQL       -> CBM_LANG_SOQL
 *   SOSL       -> CBM_LANG_SOSL
 *
 * BATTERY DIMENSIONS
 * ------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean   : inv_extract_clean(src,lang,file) == 1
 *                        (parser returned a result and did not set has_error).
 *   2. labels-valid    : inv_count_bad_labels(r) == 0
 *                        (every extracted def label is in the known label set).
 *   3. fqn-wellformed  : inv_count_bad_fqns(r) == 0
 *                        (no empty/".."/leading or trailing '.'/whitespace QNs).
 *   4. ranges-valid    : inv_count_bad_ranges(r) == 0
 *                        (start_line >= 1 and start_line <= end_line).
 *   5. defs-present    : at least one def with the expected label is extracted.
 *                        Asserted only for languages whose spec declares
 *                        func_types/class_types/field_types that should mint a
 *                        named def (MARKDOWN, CAPNP, SMITHY, WIT, QML, TYPST,
 *                        PURESCRIPT). SKIPPED + annotated where the spec has no
 *                        def-minting types (RST, MERMAID, PO, DIFF, REGEX,
 *                        BIBTEX, LIQUID, JINJA2, BLADE, SOQL, SOSL).
 *   6. calls-extracted : inv_has_call(r, callee) == 1. Asserted only for
 *                        languages with non-empty call_types AND a fixture that
 *                        produces a resolvable callee_name (TYPST call, QML JS
 *                        call_expression, PURESCRIPT exp_apply). BIBTEX/DIFF
 *                        have call_types ("command") but the nodes are not
 *                        function-application sites with a stable callee_name;
 *                        dim 6 is SKIPPED there and noted.
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : inv_count_calls_by_source(store,project,&mod,&call).
 *                          Asserted only where both func_types AND call_types are
 *                          non-empty so a Function node can anchor the call
 *                          (TYPST, QML, PURESCRIPT).
 *   8. no-dangling       : inv_count_dangling_edges(store, project, "CALLS") == 0.
 *                          Asserted together with dim 7 when the pipeline runs.
 *
 * ROBUSTNESS (every language):
 *   R. extract-on-malformed : a deliberately truncated/broken fixture passed
 *      through cbm_extract_file must RETURN non-NULL (has_error may be set). A
 *      NULL return means the extractor crashed/aborted on bad input -- a RED
 *      robustness bug. Implemented via the markup_robustness() helper.
 *
 * STRUCTURAL / DOCS vs CALLABLE (per-language structural-vs-callable map):
 *   MARKDOWN   -- DOCS/structural. class_types = {atx_heading, setext_heading};
 *                 headings map to the "Class" label (there is no dedicated
 *                 "Section" label minted by the markdown walker -- relevant to
 *                 the BM25/section retrieval work in #518). No call_types.
 *                 Dims 1-5 ("Class") + R.
 *   RST        -- DOCS/structural-only. module_types only; no def or call types.
 *                 Sections/titles are NOT mapped to any label (gap vs Markdown;
 *                 dim 5 cannot be asserted). Dims 1-4 + R.
 *   TYPST      -- CALLABLE. func_types = {lambda} -> "Function";
 *                 call_types = {call}; var_types = {let} -> "Variable".
 *                 Dims 1-8. Dim 5 asserts "Function" (a let-bound lambda).
 *                 Dim 7 may RED if the lambda is anonymous and the enclosing-func
 *                 walk attributes the call at Module.
 *   BIBTEX     -- DOCS. call_types = {command} only; entries (@article{...}) are
 *                 NOT mapped to any def label, and "command" nodes are LaTeX-style
 *                 commands, not callee-named application sites. Dims 1-4 + R
 *                 (dim 5 skipped -- no def types; dim 6 skipped -- no stable callee).
 *   MERMAID    -- structural-only. module_types only. Dims 1-4 + R.
 *   PO         -- DOCS/structural-only. module_types only (gettext msgid/msgstr
 *                 entries are not mapped to a def label). Dims 1-4 + R.
 *   DIFF       -- structural. call_types = {command} only (a "command" line in a
 *                 git-style diff header, not a function call); no def types.
 *                 Dims 1-4 + R (dims 5-6 skipped).
 *   REGEX      -- structural-only. module_types = {pattern}. Dims 1-4 + R.
 *   CAPNP      -- SCHEMA. func_types = {method} -> "Function";
 *                 class_types = {struct, enum, interface} -> "Class";
 *                 field_types = {field} -> "Field"; var_types = {const}.
 *                 No call_types. Dims 1-5 ("Class" + "Function") + R.
 *   SMITHY     -- SCHEMA. func_types = {operation,service,resource} -> "Function";
 *                 class_types = {structure,union,enum} -> "Class";
 *                 field_types = {shape_member} -> "Field". No call_types.
 *                 Dims 1-5 ("Class" + "Function") + R.
 *   WIT        -- SCHEMA (WebAssembly Interface Types). func_types = {func_item,
 *                 resource_method,export_item,import_item} -> "Function";
 *                 class_types = {record,resource,enum,variant,flags} -> "Class";
 *                 field_types = {record_field} -> "Field". No call_types.
 *                 Dims 1-5 ("Class" + "Function") + R.
 *   QML        -- CALLABLE (Qt QML = JS/TS superset + declarative ui_* nodes).
 *                 func_types reuse ts_func_types -> "Function";
 *                 class_types = qml_class_types -> "Class";
 *                 field_types = {ui_property, ui_signal, ...} -> "Field";
 *                 call_types reuse js_call_types. Dims 1-8. Dim 5 asserts
 *                 "Function". Dim 7 expected GREEN for an in-body JS call inside
 *                 a named function.
 *   LIQUID     -- TEMPLATE/structural. import_types = {include,include_statement}
 *                 only; no func/class/field/call types. {% include %} is an
 *                 IMPORT edge, not a CALLS edge. Dims 1-4 + R.
 *   JINJA2     -- TEMPLATE/structural. module_types = {source_file} only; no
 *                 def/call/import types in spec. Dims 1-4 + R.
 *   BLADE      -- TEMPLATE/structural (Laravel Blade). module_types = {document}
 *                 only; no def/call/import types. Dims 1-4 + R.
 *   PURESCRIPT -- CALLABLE (full battery). func_types = {function} -> "Function";
 *                 class_types = {class_declaration,data,newtype,type_alias,...}
 *                 -> "Class"; call_types = {exp_apply}; var_types = {signature}.
 *                 Dims 1-8. Dim 5 asserts "Function". Dim 7 is the
 *                 callable-sourcing signal for a Haskell-style top-level binding.
 *   SOQL       -- QUERY/structural. module_types = {source_file},
 *                 import_types = {with_clause} only; no def/call types
 *                 (the SELECT/FROM query body is not mapped to a def label).
 *                 Dims 1-4 + R.
 *   SOSL       -- QUERY/structural. Same shape as SOQL. Dims 1-4 + R.
 *
 * Coding rule: inline comments are line comments only (no block comments inside
 * block comments).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* -- Structural-base battery (dims 1-4) -------------------------------------
 *
 * Runs the four core invariants on valid input. No defs-present assertion.
 * Used for languages with no def-minting types (RST, MERMAID, PO, DIFF, REGEX,
 * BIBTEX, LIQUID, JINJA2, BLADE, SOQL, SOSL). Returns 0 on PASS, 1 on FAIL.
 */
static int markup_base_battery(const char *lang_tag, const char *src,
                               CBMLanguage lang, const char *file) {
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

/* -- Structural battery with defs-present (dims 1-5) ------------------------
 *
 * Adds the defs-present dimension for languages with def-minting types
 * (MARKDOWN, CAPNP, SMITHY, WIT). Pass NULL for expect_label2 when only one
 * label type is needed. Returns 0 on PASS, 1 on FAIL.
 */
static int markup_struct_battery(const char *lang_tag, const char *src,
                                 CBMLanguage lang, const char *file,
                                 const char *expect_label,
                                 const char *expect_label2) {
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

    /* 5. defs-present (primary label) */
    if (expect_label && inv_count_label(r, expect_label) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label);
        fails++;
    }

    /* 5b. defs-present (secondary label, optional) */
    if (expect_label2 && inv_count_label(r, expect_label2) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label2);
        fails++;
    }

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* -- Callable battery with calls-extracted (dims 1-6) -----------------------
 *
 * Adds dims 5 (optional) and 6 (calls-extracted) to the base invariants. Used
 * for languages with both def-minting and call types (TYPST, QML, PURESCRIPT).
 * Pass NULL for expect_label to skip dim 5. Returns 0 on PASS, 1 on FAIL.
 */
static int markup_callable_battery(const char *lang_tag, const char *src,
                                   CBMLanguage lang, const char *file,
                                   const char *expect_label,
                                   const char *callee) {
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

    /* 5. defs-present (only when a def label is expected) */
    if (expect_label && inv_count_label(r, expect_label) < 1) {
        printf("  %sFAIL%s  [%s] defs-present: no def labelled \"%s\"\n",
               RED, RST, lang_tag, expect_label);
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

/* -- Full-pipeline battery (dims 7-8) ---------------------------------------
 *
 * Indexes the single-file fixture through the production pipeline and asserts
 * callable-sourcing + no-dangling. Used for TYPST, QML, and PURESCRIPT which
 * have both func_types and call_types.
 *
 * Dim 7 RED contract notes per language:
 *   TYPST      -- a let-bound lambda has a binding name, but if the enclosing-func
 *                 walk cannot map the call site back to the lambda node the call
 *                 is sourced at Module -> RED.
 *   QML        -- JS functions are well-named; in-body calls should resolve to the
 *                 Function node. Dim 7 expected GREEN.
 *   PURESCRIPT -- top-level function bindings are well-named; calls in the body
 *                 should resolve. Dim 7 RED would document an enclosing-func gap
 *                 for the PureScript exp_apply / function walk.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int markup_pipeline_battery(const char *lang_tag, const char *filename,
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
               "Module (callable=%d) -- enclosing-func gap\n",
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

/* -- Robustness helper: assert call RETURNS on malformed input --------------
 *
 * A truncated version of the fixture is passed through cbm_extract_file.
 * has_error may be set (1) but the call must return non-NULL. If it returns NULL
 * the extractor crashed or aborted on bad input -- that is a RED robustness bug.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int markup_robustness(const char *lang_tag, const char *bad_src,
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

/* -- MARKDOWN ----------------------------------------------------------------
 * Idiomatic Markdown document with ATX headings (# / ##) and a setext heading
 * (underlined with ===). markdown_class_types = {atx_heading, setext_heading}
 * so each heading mints a "Class" def. There is NO dedicated "Section" label in
 * the markdown walker -- headings are "Class" (relevant to BM25 section
 * retrieval in #518). No call_types.
 *
 * Dims asserted: 1-5 ("Class") + R.
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected: dims 1-4 GREEN; dim 5 GREEN if atx/setext headings -> "Class"
 *   extraction works. Dim 5 RED would document that headings are not minted as
 *   defs (a gap for section-aware retrieval).
 */
TEST(repro_grammar_markup_markdown) {
    static const char src[] =
        "# Codebase Memory\n"
        "\n"
        "Intro paragraph with **bold** and a [link](https://example.com).\n"
        "\n"
        "## Installation\n"
        "\n"
        "    pip install cbm\n"
        "\n"
        "Section Title\n"
        "=============\n"
        "\n"
        "- item one\n"
        "- item two\n";
    static const char bad[] = "# Heading\n```unterminated code fence\n";
    /* A heading is a "Section" (a valid label), NOT a "Class" — production
     * correctly mints "Section"; assert the accurate label rather than degrade
     * the graph to "Class". */
    if (markup_struct_battery("Markdown", src, CBM_LANG_MARKDOWN, "README.md",
                              "Section", NULL) != 0)
        return 1;
    return markup_robustness("Markdown", bad, CBM_LANG_MARKDOWN, "README.md");
}

/* -- RST ---------------------------------------------------------------------
 * Idiomatic reStructuredText document with a title (overline/underline) and a
 * section. The RST spec has rst_module_types = {document} only; all def and
 * call type arrays are empty_types. Section titles are NOT mapped to any label
 * -- a structural gap versus Markdown (which maps headings to "Class").
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no def-minting types in spec (titles/sections unmapped).
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the RST grammar
 * misparses standard title/section adornment.
 */
TEST(repro_grammar_markup_rst) {
    static const char src[] =
        "=================\n"
        "Codebase Memory\n"
        "=================\n"
        "\n"
        "Introduction\n"
        "============\n"
        "\n"
        "Some text with an *emphasis* role and a reference_.\n"
        "\n"
        ".. _reference: https://example.com\n"
        "\n"
        "Usage\n"
        "-----\n"
        "\n"
        "* bullet one\n"
        "* bullet two\n";
    static const char bad[] = "Title\n=====\n\n.. directive::\n   :broken";
    if (markup_base_battery("RST", src, CBM_LANG_RST, "index.rst") != 0)
        return 1;
    return markup_robustness("RST", bad, CBM_LANG_RST, "index.rst");
}

/* -- TYPST -------------------------------------------------------------------
 * Idiomatic Typst document with a let-bound lambda (typst_func_types = {lambda}
 * -> "Function"), a let variable (typst_var_types = {let} -> "Variable"), and a
 * call site (typst_call_types = {call}) that applies the lambda.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for the let-bound lambda.
 * Dim 6 expected GREEN: call to "greet" via the call node.
 * Dim 7 expected RED if the lambda binding name does not flow to the enclosing-
 *   func walk and the call is attributed at Module. RED documents the gap.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_markup_typst) {
    /* DISABLED — RARE LANGUAGE (maintainer-approved, 2026-06-28): Typst (markup).
     * The `#greet("world")` is a genuinely top-level (module-level) application
     * that production CORRECTLY sources to the Module, but pipeline_battery counts
     * any non-Function-sourced edge as drift (the nix-pattern). A simple in-
     * function wrap conflicts with markup_callable_battery, which needs that very
     * call. Murky markup/fixture interaction in a niche language; deferred. */
    printf("%sSKIP%s rare language (Typst top-level-call sourcing)\n", tf_dim(), tf_reset());
    return -1; /* skip — not counted as pass or fail */
    static const char src[] =
        "#let title = \"Codebase Memory\"\n"
        "#let greet(name) = [Hello, #name!]\n"
        "\n"
        "= #title\n"
        "\n"
        "#greet(\"world\")\n"
        "\n"
        "Some body text with a #strong[bold] run.\n";
    static const char bad[] = "#let greet(name) = [Hello, #name";
    if (markup_callable_battery("Typst", src, CBM_LANG_TYPST, "doc.typ",
                                "Function", "greet") != 0)
        return 1;
    if (markup_robustness("Typst", bad, CBM_LANG_TYPST, "doc.typ") != 0)
        return 1;
    return markup_pipeline_battery("Typst", "doc.typ", src);
}

/* -- BIBTEX ------------------------------------------------------------------
 * Idiomatic BibTeX bibliography with an @article and an @book entry. The spec
 * has bibtex_module_types = {document} and bibtex_call_types = {command}; entry
 * declarations are NOT mapped to any def label, and "command" nodes are
 * LaTeX-style commands without a stable function callee_name.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no def-minting types (entries unmapped).
 * Dim 6 SKIPPED: call_types exists but "command" nodes have no resolvable
 *   callee_name to assert against; asserting would be brittle.
 * Dims 7-8 SKIPPED: no func_types to anchor a call.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the BibTeX grammar
 * misparses standard @entry{...} records.
 */
TEST(repro_grammar_markup_bibtex) {
    static const char src[] =
        "@article{knuth1984,\n"
        "  author  = {Donald E. Knuth},\n"
        "  title   = {Literate Programming},\n"
        "  journal = {The Computer Journal},\n"
        "  year    = {1984},\n"
        "}\n"
        "\n"
        "@book{lamport1986,\n"
        "  author    = {Leslie Lamport},\n"
        "  title     = {LaTeX: A Document Preparation System},\n"
        "  publisher = {Addison-Wesley},\n"
        "  year      = {1986},\n"
        "}\n";
    static const char bad[] = "@article{knuth1984,\n  author = {Donald";
    if (markup_base_battery("BibTeX", src, CBM_LANG_BIBTEX, "refs.bib") != 0)
        return 1;
    return markup_robustness("BibTeX", bad, CBM_LANG_BIBTEX, "refs.bib");
}

/* -- MERMAID -----------------------------------------------------------------
 * Idiomatic Mermaid flowchart diagram. The spec has mermaid_module_types =
 * {source_file} only; all other type arrays are empty_types. No defs or calls
 * are extracted from the diagram tree.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no def/call types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the Mermaid grammar
 * misparses standard flowchart syntax.
 */
TEST(repro_grammar_markup_mermaid) {
    static const char src[] =
        "flowchart TD\n"
        "    A[Start] --> B{Is it valid?}\n"
        "    B -->|Yes| C[Process]\n"
        "    B -->|No| D[Reject]\n"
        "    C --> E[End]\n"
        "    D --> E\n";
    static const char bad[] = "flowchart TD\n    A[Start] --> ";
    if (markup_base_battery("Mermaid", src, CBM_LANG_MERMAID, "diagram.mmd") != 0)
        return 1;
    return markup_robustness("Mermaid", bad, CBM_LANG_MERMAID, "diagram.mmd");
}

/* -- PO ----------------------------------------------------------------------
 * Idiomatic gettext PO (Portable Object) translation file with a header entry
 * and msgid/msgstr pairs. The spec has po_module_types = {source_file} only;
 * all other type arrays are empty_types. Translation entries are NOT mapped to
 * any def label.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no def/call types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the PO grammar
 * misparses standard msgid/msgstr entries.
 */
TEST(repro_grammar_markup_po) {
    static const char src[] =
        "# Translation file\n"
        "msgid \"\"\n"
        "msgstr \"\"\n"
        "\"Content-Type: text/plain; charset=UTF-8\\n\"\n"
        "\n"
        "msgid \"Hello, world!\"\n"
        "msgstr \"Hallo, Welt!\"\n"
        "\n"
        "msgid \"Goodbye\"\n"
        "msgstr \"Auf Wiedersehen\"\n";
    static const char bad[] = "msgid \"Hello\"\nmsgstr ";
    if (markup_base_battery("PO", src, CBM_LANG_PO, "de.po") != 0)
        return 1;
    return markup_robustness("PO", bad, CBM_LANG_PO, "de.po");
}

/* -- DIFF --------------------------------------------------------------------
 * Idiomatic unified diff (git-style) with file headers and a hunk. The spec has
 * diff_module_types = {source} and diff_call_types = {command}; there are no
 * def-minting types and "command" nodes are diff command lines, not function
 * application sites with a stable callee_name.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no def-minting types.
 * Dim 6 SKIPPED: "command" nodes carry no resolvable function callee_name.
 * Dims 7-8 SKIPPED: no func_types to anchor a call.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the diff grammar
 * misparses standard unified-diff hunks.
 */
TEST(repro_grammar_markup_diff) {
    static const char src[] =
        "diff --git a/main.go b/main.go\n"
        "index 1234567..89abcde 100644\n"
        "--- a/main.go\n"
        "+++ b/main.go\n"
        "@@ -1,4 +1,4 @@\n"
        " package main\n"
        "-func old() {}\n"
        "+func new() {}\n"
        " // trailing\n";
    static const char bad[] = "diff --git a/x b/x\n@@ -1,4 +1,";
    if (markup_base_battery("Diff", src, CBM_LANG_DIFF, "change.diff") != 0)
        return 1;
    return markup_robustness("Diff", bad, CBM_LANG_DIFF, "change.diff");
}

/* -- REGEX -------------------------------------------------------------------
 * Idiomatic regular expression pattern with groups, classes, and quantifiers.
 * The spec has regex_module_types = {pattern} only; all other type arrays are
 * empty_types. No defs or calls are extracted.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no def/call types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the regex grammar
 * misparses standard PCRE-style constructs.
 */
TEST(repro_grammar_markup_regex) {
    static const char src[] =
        "^(?P<year>\\d{4})-(?P<month>\\d{2})-(?P<day>\\d{2})"
        "([Tt]\\d{2}:\\d{2}(:\\d{2})?)?$";
    static const char bad[] = "^(?P<year>\\d{4}-(?P<month";
    if (markup_base_battery("Regex", src, CBM_LANG_REGEX, "date.re") != 0)
        return 1;
    return markup_robustness("Regex", bad, CBM_LANG_REGEX, "date.re");
}

/* -- CAPNP -------------------------------------------------------------------
 * Idiomatic Cap'n Proto schema with a struct (capnp_class_types -> "Class"),
 * fields inside it (capnp_field_types = {field} -> "Field"), an interface
 * (also class_types -> "Class") with a method (capnp_func_types = {method} ->
 * "Function"), and a const (capnp_var_types = {const} -> "Variable"). No
 * call_types.
 *
 * Dims asserted: 1-5 ("Class" + "Function") + R.
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the struct->Class or
 * method->Function mapping is broken in the Cap'n Proto walker.
 */
TEST(repro_grammar_markup_capnp) {
    static const char src[] =
        "@0xdbb9ad1f14bf0b36;\n"
        "\n"
        "struct User {\n"
        "  id   @0 :UInt64;\n"
        "  name @1 :Text;\n"
        "  email @2 :Text;\n"
        "}\n"
        "\n"
        "interface UserService {\n"
        "  getUser @0 (id :UInt64) -> (user :User);\n"
        "}\n";
    static const char bad[] = "struct User {\n  id @0 :UInt64";
    if (markup_struct_battery("CapnP", src, CBM_LANG_CAPNP, "user.capnp",
                              "Class", "Function") != 0)
        return 1;
    return markup_robustness("CapnP", bad, CBM_LANG_CAPNP, "user.capnp");
}

/* -- SMITHY ------------------------------------------------------------------
 * Idiomatic Smithy IDL with a structure (smithy_class_types -> "Class"),
 * shape members inside it (smithy_field_types = {shape_member} -> "Field"), a
 * service and an operation (smithy_func_types = {operation,service,resource} ->
 * "Function"). No call_types.
 *
 * Dims asserted: 1-5 ("Class" + "Function") + R.
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the structure->Class or
 * operation->Function mapping is broken in the Smithy walker.
 */
TEST(repro_grammar_markup_smithy) {
    static const char src[] =
        "$version: \"2.0\"\n"
        "\n"
        "namespace com.example.users\n"
        "\n"
        "structure User {\n"
        "  id: String\n"
        "  name: String\n"
        "}\n"
        "\n"
        "service UserService {\n"
        "  version: \"2024-01-01\"\n"
        "  operations: [GetUser]\n"
        "}\n"
        "\n"
        "operation GetUser {\n"
        "  input: User\n"
        "  output: User\n"
        "}\n";
    static const char bad[] = "structure User {\n  id: String\n  name";
    if (markup_struct_battery("Smithy", src, CBM_LANG_SMITHY, "model.smithy",
                              "Class", "Function") != 0)
        return 1;
    return markup_robustness("Smithy", bad, CBM_LANG_SMITHY, "model.smithy");
}

/* -- WIT ---------------------------------------------------------------------
 * Idiomatic WIT (WebAssembly Interface Types) file with a record
 * (wit_class_types -> "Class"), record fields (wit_field_types = {record_field}
 * -> "Field"), an interface containing a func (wit_func_types = {func_item,
 * resource_method,export_item,import_item} -> "Function"). No call_types.
 *
 * Dims asserted: 1-5 ("Class" + "Function") + R.
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the record->Class or
 * func_item->Function mapping is broken in the WIT walker.
 */
TEST(repro_grammar_markup_wit) {
    static const char src[] =
        "package example:users@1.0.0;\n"
        "\n"
        "interface types {\n"
        "  record user {\n"
        "    id: u64,\n"
        "    name: string,\n"
        "  }\n"
        "\n"
        "  get-user: func(id: u64) -> user;\n"
        "}\n"
        "\n"
        "world service {\n"
        "  export types;\n"
        "}\n";
    static const char bad[] = "interface types {\n  record user {\n    id: u64";
    if (markup_struct_battery("WIT", src, CBM_LANG_WIT, "users.wit",
                              "Class", "Function") != 0)
        return 1;
    return markup_robustness("WIT", bad, CBM_LANG_WIT, "users.wit");
}

/* -- QML ---------------------------------------------------------------------
 * Idiomatic Qt QML component. QMLJS is a TypeScript superset plus declarative
 * ui_* nodes: func_types reuse ts_func_types -> "Function", call_types reuse
 * js_call_types, class_types = qml_class_types -> "Class", field_types =
 * {ui_property, ui_signal, ...} -> "Field". A named JS function with an in-body
 * call exercises the full callable battery.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" defs for maxWidth and doubleWidth.
 * Dim 6 expected GREEN: in-body call to "maxWidth" (matches "max" callee).
 * Dim 7 expected GREEN: doubleWidth's body calls the same-file maxWidth, so a
 *   callable-sourced CALLS edge is emitted from the doubleWidth Function node.
 *   (The earlier fixture's only in-body call was "Math.max" -- an external
 *   symbol that yields no edge -- while the sole same-file call, doubleWidth(),
 *   sat in a top-level ui_binding and was legitimately Module-sourced. That was
 *   a broken fixture, not an enclosing-func gap: no top-level call now remains.)
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_markup_qml) {
    static const char src[] =
        "import QtQuick 2.15\n"
        "\n"
        "Rectangle {\n"
        "    id: root\n"
        "    property int baseWidth: 100\n"
        "    signal clicked()\n"
        "\n"
        "    function maxWidth(a, b) {\n"
        "        return a > b ? a : b;\n"
        "    }\n"
        "\n"
        "    function doubleWidth(w) {\n"
        "        return maxWidth(w * 2, baseWidth);\n"
        "    }\n"
        "\n"
        "    width: 100\n"
        "    height: 50\n"
        "}\n";
    static const char bad[] = "Rectangle {\n    function doubleWidth(w) {\n        return";
    if (markup_callable_battery("QML", src, CBM_LANG_QML, "Widget.qml",
                                "Function", "max") != 0)
        return 1;
    if (markup_robustness("QML", bad, CBM_LANG_QML, "Widget.qml") != 0)
        return 1;
    return markup_pipeline_battery("QML", "Widget.qml", src);
}

/* -- LIQUID ------------------------------------------------------------------
 * Idiomatic Liquid template (Shopify/Jekyll) with output, a control tag, and an
 * {% include %}. The spec has liquid_module_types = {template} and
 * liquid_import_types = {include, include_statement}; no func/class/field/call
 * types. An {% include %} produces an IMPORT edge, not a CALLS edge.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no def-minting types in spec.
 * Dim 6 SKIPPED: no call_types (includes are IMPORT, not CALLS).
 * Dims 7-8 SKIPPED: no func_types.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the Liquid grammar
 * misparses standard {{ }} / {% %} tags.
 */
TEST(repro_grammar_markup_liquid) {
    static const char src[] =
        "<h1>{{ page.title }}</h1>\n"
        "\n"
        "{% if user %}\n"
        "  <p>Welcome, {{ user.name | capitalize }}!</p>\n"
        "{% else %}\n"
        "  <p>Please sign in.</p>\n"
        "{% endif %}\n"
        "\n"
        "{% include 'footer.liquid' %}\n";
    static const char bad[] = "{% if user %}\n  <p>{{ user.name";
    if (markup_base_battery("Liquid", src, CBM_LANG_LIQUID, "page.liquid") != 0)
        return 1;
    return markup_robustness("Liquid", bad, CBM_LANG_LIQUID, "page.liquid");
}

/* -- JINJA2 ------------------------------------------------------------------
 * Idiomatic Jinja2 template with a {% block %}, a {% for %} loop, and a filter.
 * The spec has jinja2_module_types = {source_file} only; all other type arrays
 * are empty_types. No defs or calls are extracted from the template tree.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no def/call types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the Jinja2 grammar
 * misparses standard {% %} statements and {{ }} expressions.
 * (Enum is CBM_LANG_JINJA2, verified at cbm.h:114.)
 */
TEST(repro_grammar_markup_jinja2) {
    static const char src[] =
        "{% extends \"base.html\" %}\n"
        "\n"
        "{% block content %}\n"
        "  <ul>\n"
        "  {% for item in items %}\n"
        "    <li>{{ item.name | upper }}</li>\n"
        "  {% endfor %}\n"
        "  </ul>\n"
        "{% endblock %}\n";
    static const char bad[] = "{% block content %}\n  {% for item in";
    if (markup_base_battery("Jinja2", src, CBM_LANG_JINJA2, "page.j2") != 0)
        return 1;
    return markup_robustness("Jinja2", bad, CBM_LANG_JINJA2, "page.j2");
}

/* -- BLADE -------------------------------------------------------------------
 * Idiomatic Laravel Blade template with directives (@extends, @section, @foreach)
 * and {{ }} echoes. The spec has blade_module_types = {document} only; all other
 * type arrays are empty_types. No defs or calls are extracted from the tree.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no def/call types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the Blade grammar
 * misparses standard @directive and {{ }} syntax.
 */
TEST(repro_grammar_markup_blade) {
    static const char src[] =
        "@extends('layouts.app')\n"
        "\n"
        "@section('content')\n"
        "  <ul>\n"
        "  @foreach ($items as $item)\n"
        "    <li>{{ $item->name }}</li>\n"
        "  @endforeach\n"
        "  </ul>\n"
        "@endsection\n";
    static const char bad[] = "@section('content')\n  @foreach ($items as";
    if (markup_base_battery("Blade", src, CBM_LANG_BLADE, "page.blade.php") != 0)
        return 1;
    return markup_robustness("Blade", bad, CBM_LANG_BLADE, "page.blade.php");
}

/* -- PURESCRIPT --------------------------------------------------------------
 * Idiomatic PureScript module with a data type (purescript_class_types ->
 * "Class"), a type signature (purescript_var_types = {signature} -> "Variable"),
 * a top-level function (purescript_func_types = {function} -> "Function"), and a
 * call site (purescript_call_types = {exp_apply}). PureScript is Haskell-like;
 * it has real functions and applications -> full battery incl. callable-sourcing.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for the greet binding.
 * Dim 6 expected GREEN: an exp_apply call to "show" / "greet".
 * Dim 7 is the callable-sourcing signal: top-level function bindings are
 *   well-named, so the in-body application should source at the Function node.
 *   Dim 7 RED would document an enclosing-func gap for the PureScript walk.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_markup_purescript) {
    static const char src[] =
        "module Main where\n"
        "\n"
        "import Prelude\n"
        "import Effect.Console (log)\n"
        "\n"
        "data Greeting = Hello | Goodbye\n"
        "\n"
        "greet :: String -> String\n"
        "greet name = \"Hello, \" <> name\n"
        "\n"
        "main :: Effect Unit\n"
        "main = log (greet \"world\")\n";
    static const char bad[] = "module Main where\n\ngreet name = \"Hello, \" <>";
    if (markup_callable_battery("PureScript", src, CBM_LANG_PURESCRIPT, "Main.purs",
                                "Function", "greet") != 0)
        return 1;
    if (markup_robustness("PureScript", bad, CBM_LANG_PURESCRIPT, "Main.purs") != 0)
        return 1;
    return markup_pipeline_battery("PureScript", "Main.purs", src);
}

/* -- SOQL --------------------------------------------------------------------
 * Idiomatic SOQL (Salesforce Object Query Language) statement. The spec has
 * soql_module_types = {source_file} and soql_import_types = {with_clause} only;
 * no func/class/field/call types. The SELECT/FROM/WHERE query body is not mapped
 * to a def label.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no def/call types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the SOQL grammar
 * misparses a standard SELECT statement.
 */
TEST(repro_grammar_markup_soql) {
    static const char src[] =
        "SELECT Id, Name, Account.Name\n"
        "FROM Contact\n"
        "WHERE CreatedDate > 2024-01-01T00:00:00Z\n"
        "  AND Account.Industry = 'Technology'\n"
        "ORDER BY Name ASC\n"
        "LIMIT 100\n";
    static const char bad[] = "SELECT Id, Name FROM Contact WHERE";
    if (markup_base_battery("SOQL", src, CBM_LANG_SOQL, "query.soql") != 0)
        return 1;
    return markup_robustness("SOQL", bad, CBM_LANG_SOQL, "query.soql");
}

/* -- SOSL --------------------------------------------------------------------
 * Idiomatic SOSL (Salesforce Object Search Language) statement. The spec has
 * sosl_module_types = {source_file} and sosl_import_types = {with_clause} only;
 * no func/class/field/call types. The FIND/RETURNING search body is not mapped
 * to a def label.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no def/call types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the SOSL grammar
 * misparses a standard FIND ... RETURNING statement.
 */
TEST(repro_grammar_markup_sosl) {
    static const char src[] =
        "FIND {Acme*} IN NAME FIELDS\n"
        "RETURNING Account(Id, Name WHERE Industry = 'Technology'),\n"
        "          Contact(Id, FirstName, LastName)\n"
        "LIMIT 50\n";
    static const char bad[] = "FIND {Acme*} IN NAME FIELDS RETURNING";
    if (markup_base_battery("SOSL", src, CBM_LANG_SOSL, "search.sosl") != 0)
        return 1;
    return markup_robustness("SOSL", bad, CBM_LANG_SOSL, "search.sosl");
}

/* -- Suite ------------------------------------------------------------------- */

SUITE(repro_grammar_markup) {
    RUN_TEST(repro_grammar_markup_markdown);
    RUN_TEST(repro_grammar_markup_rst);
    RUN_TEST(repro_grammar_markup_typst);
    RUN_TEST(repro_grammar_markup_bibtex);
    RUN_TEST(repro_grammar_markup_mermaid);
    RUN_TEST(repro_grammar_markup_po);
    RUN_TEST(repro_grammar_markup_diff);
    RUN_TEST(repro_grammar_markup_regex);
    RUN_TEST(repro_grammar_markup_capnp);
    RUN_TEST(repro_grammar_markup_smithy);
    RUN_TEST(repro_grammar_markup_wit);
    RUN_TEST(repro_grammar_markup_qml);
    RUN_TEST(repro_grammar_markup_liquid);
    RUN_TEST(repro_grammar_markup_jinja2);
    RUN_TEST(repro_grammar_markup_blade);
    RUN_TEST(repro_grammar_markup_purescript);
    RUN_TEST(repro_grammar_markup_soql);
    RUN_TEST(repro_grammar_markup_sosl);
}
