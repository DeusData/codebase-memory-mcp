/*
 * repro_grammar_config.c -- Per-grammar INVARIANT battery for the
 * CONFIG / DATA language family.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs a battery adapted to what the language actually models:
 * most config/data languages are STRUCTURAL-ONLY (no func_types or call_types).
 * The battery dimensions applied per language are documented in the per-TEST
 * comment.
 *
 * Languages covered (16) and the CBM_LANG_* enum each uses (all verified in
 * internal/cbm/cbm.h):
 *   JSON       -> CBM_LANG_JSON
 *   JSON5      -> CBM_LANG_JSON5
 *   YAML       -> CBM_LANG_YAML
 *   TOML       -> CBM_LANG_TOML
 *   INI        -> CBM_LANG_INI
 *   HCL        -> CBM_LANG_HCL
 *   XML        -> CBM_LANG_XML
 *   CSV        -> CBM_LANG_CSV
 *   PROPERTIES -> CBM_LANG_PROPERTIES
 *   DOTENV     -> CBM_LANG_DOTENV
 *   KDL        -> CBM_LANG_KDL
 *   RON        -> CBM_LANG_RON
 *   PKL        -> CBM_LANG_PKL
 *   NICKEL     -> CBM_LANG_NICKEL
 *   JSONNET    -> CBM_LANG_JSONNET
 *   STARLARK   -> CBM_LANG_STARLARK
 *
 * BATTERY DIMENSIONS
 * ------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean    : inv_extract_clean(src,lang,file) == 1
 *                         (parser returned a result and did not set has_error).
 *   2. labels-valid     : inv_count_bad_labels(r) == 0
 *                         (every extracted def label is in the known label set).
 *   3. fqn-wellformed   : inv_count_bad_fqns(r) == 0
 *                         (no empty / ".." / leading or trailing '.' / whitespace QNs).
 *   4. ranges-valid     : inv_count_bad_ranges(r) == 0
 *                         (start_line >= 1 and start_line <= end_line).
 *   5. defs-present     : at least one def with the expected label is extracted.
 *                         SKIPPED for languages whose spec has no func_types,
 *                         class_types, or meaningful var_types that produce
 *                         extractable defs (JSON, JSON5, CSV, KDL, RON, DOTENV).
 *   6. calls-extracted  : inv_has_call(r, callee) == 1.
 *                         Only asserted for languages that have non-empty
 *                         call_types: HCL (function_call), NICKEL (infix_expr),
 *                         JSONNET (functioncall), STARLARK (call).
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : inv_count_calls_by_source(store,project,&mod,&call).
 *                          Only asserted for languages where both func_types AND
 *                          call_types are non-empty: NICKEL, JSONNET, STARLARK, PKL.
 *   8. no-dangling       : inv_count_dangling_edges(store, project, "CALLS") == 0.
 *                          Asserted together with dim 7 when the pipeline is run.
 *
 * ROBUSTNESS (every language):
 *   R. extract-on-malformed: the extractor must RETURN (not crash/hang) on a
 *      deliberately truncated/broken version of the fixture. inv_extract_clean
 *      may return 0 (has_error is fine) but must not return NULL.
 *      Implemented inline at the end of each TEST via cbm_extract_file directly.
 *
 * STRUCTURAL-ONLY LANGUAGES (dims 1-4 + R, no calls/pipeline dims):
 *   JSON       -- var_types = pair -> "Variable"; no func/class types.
 *                 Dims 1-4 + R (dim 5 skipped — pair -> Variable may or may not
 *                 extract; no class_types or func_types to assert).
 *   JSON5      -- same as JSON; spec has only json5_module_types + empty others.
 *                 Dims 1-4 + R.
 *   YAML       -- var_types = block_mapping_pair; no func/class/call types.
 *                 Dims 1-4 + R.
 *   CSV        -- module_types only; nothing structural extracted per-row.
 *                 Dims 1-4 + R.
 *   KDL        -- module_types only; no var/func/class/call types in spec.
 *                 Dims 1-4 + R.
 *   RON        -- module_types only; no func/class/var/call types in spec.
 *                 Dims 1-4 + R.
 *   DOTENV     -- module_types only; no var/func/class/call types in spec
 *                 (key=value nodes are not mapped to any def label).
 *                 Dims 1-4 + R.
 *
 * STRUCTURAL LANGUAGES WITH DEFS (dims 1-5 + R, no call dims):
 *   TOML       -- class_types = table/table_array_element -> "Class";
 *                 var_types = pair -> "Variable". Dims 1-5 ("Class"). No calls.
 *   INI        -- class_types = section -> "Class"; var_types = setting.
 *                 Dims 1-5 ("Class"). No calls.
 *   XML        -- class_types = element -> "Class". Dims 1-5 ("Class"). No calls.
 *   PROPERTIES -- var_types = property -> "Variable". Dims 1-5 ("Variable"). No calls.
 *   PKL        -- func_types = classMethod/objectMethod -> "Function";
 *                 class_types = clazz -> "Class"; var_types = classProperty/objectProperty.
 *                 call_types = empty_types. Dims 1-5 ("Function", "Class"). No call dim.
 *
 * LANGUAGES WITH CALLABLES (dims 1-6 + R, and pipeline dims 7-8 where applicable):
 *   HCL        -- class_types = block -> "Class"; var_types = attribute;
 *                 call_types = function_call. Dims 1-6. No func_types so no pipeline
 *                 dim 7 (calls would be module-sourced with no Function anchor).
 *   NICKEL     -- func_types = fun -> "Function"; call_types = infix_expr.
 *                 Dims 1-8. Dim 7 likely RED: infix_expr nodes represent operator
 *                 application, not named function-call sites; the enclosing-func
 *                 walk may fail to find a parent fun node.
 *   JSONNET    -- func_types = anonymous_function -> "Function";
 *                 call_types = functioncall. Dims 1-8. Dim 7 likely RED:
 *                 anonymous functions have no simple name; the enclosing-func walk
 *                 may attribute calls at Module level.
 *   STARLARK   -- func_types = function_definition/lambda -> "Function";
 *                 call_types = call. Dims 1-8. Dim 7 expected GREEN for def-level
 *                 calls; may be RED if branch walk mis-attributes nested calls.
 *
 * Coding rule: inline comments are line comments only (no block comments inside
 * block comments).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* ── Structural-base battery (dims 1-4) ──────────────────────────────────────
 *
 * Runs the four core invariants on valid input. No defs-present assertion.
 * Used for languages with no func_types/class_types and where var_types are
 * not reliably mapped to a named label (JSON, JSON5, YAML, CSV, KDL, RON, DOTENV).
 * Returns 0 on PASS, 1 on FAIL.
 */
static int config_base_battery(const char *lang_tag, const char *src,
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

/* ── Structural battery with defs-present (dims 1-5) ────────────────────────
 *
 * Adds the defs-present dimension for languages with class_types, func_types,
 * or reliably-labelled var_types (TOML, INI, XML, PROPERTIES, PKL).
 * Pass NULL for expect_label2 when only one label type is needed.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int config_struct_battery(const char *lang_tag, const char *src,
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

/* ── Callable battery with calls-extracted (dims 1-6) ───────────────────────
 *
 * Adds dims 5 (optional) and 6 (calls-extracted) to the base invariants.
 * Pass NULL for expect_label when the language has no func/class def to assert
 * alongside the call (e.g. HCL has class_types=block but call_types are for
 * built-in function calls unrelated to the block defs).
 * Returns 0 on PASS, 1 on FAIL.
 */
static int config_callable_battery(const char *lang_tag, const char *src,
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

/* ── Full-pipeline battery (dims 7-8) ───────────────────────────────────────
 *
 * Indexes the single-file fixture through the production pipeline and asserts
 * callable-sourcing + no-dangling. Used for NICKEL, JSONNET, and STARLARK
 * which all have both func_types and call_types.
 *
 * Dim 7 RED contract notes per language:
 *   NICKEL  -- infix_expr call nodes represent operator application; the
 *              enclosing-func walk may not find a parent "fun" node -> module-sourced.
 *   JSONNET -- anonymous_function has no declared name; the walk may attribute
 *              the functioncall at Module rather than the Function node.
 *   STARLARK -- function_definition is well-named; calls inside a function body
 *              should resolve correctly. Dim 7 may be GREEN for Starlark.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int config_pipeline_battery(const char *lang_tag, const char *filename,
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

/* ── Robustness helper: assert call RETURNS on malformed input ───────────────
 *
 * A truncated version of the fixture is passed through cbm_extract_file.
 * has_error may be set (1) but the call must return non-NULL. If it returns NULL
 * the extractor crashed or aborted on bad input -- that is a RED robustness bug.
 * Returns 0 on PASS, 1 on FAIL.
 */
static int config_robustness(const char *lang_tag, const char *bad_src,
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

/* ── JSON ─────────────────────────────────────────────────────────────────────
 * Idiomatic JSON object with nested structure. The spec has json_module_types =
 * {"document"} and json_var_types = {"pair"}. No func/class/call types.
 * Pairs map to "Variable" but the QN derivation may not produce stable names
 * for all nested pairs; defs-present is skipped to avoid brittle assertions.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: pair -> Variable may extract but QN stability is implementation-
 *   dependent; asserting a specific key name is fragile.
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-4. Robustness should always pass.
 */
TEST(repro_grammar_config_json) {
    static const char src[] =
        "{\n"
        "  \"name\": \"cbm\",\n"
        "  \"version\": \"0.8.1\",\n"
        "  \"description\": \"Codebase memory MCP server\",\n"
        "  \"config\": {\n"
        "    \"port\": 8080,\n"
        "    \"debug\": false,\n"
        "    \"tags\": [\"a\", \"b\"]\n"
        "  }\n"
        "}\n";
    static const char bad[] = "{ \"key\": ";
    if (config_base_battery("JSON", src, CBM_LANG_JSON, "config.json") != 0)
        return 1;
    return config_robustness("JSON", bad, CBM_LANG_JSON, "config.json");
}

/* ── JSON5 ───────────────────────────────────────────────────────────────────
 * Idiomatic JSON5 file with comments and trailing commas (valid JSON5, not
 * valid JSON). The spec has json5_module_types = {"document"} and all other
 * type arrays are empty_types; no defs or calls are extracted.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no func/class/var/call types in spec.
 * Expected GREEN: dims 1-4. RED on dim 1 would indicate the JSON5 grammar
 * incorrectly rejects its own extensions (comments, trailing commas).
 */
TEST(repro_grammar_config_json5) {
    static const char src[] =
        "// JSON5 config with comments\n"
        "{\n"
        "  name: 'cbm',       // unquoted keys + single-quoted values\n"
        "  version: '0.8.1',\n"
        "  features: [\n"
        "    'graph',\n"
        "    'lsp',\n"
        "  ],                 // trailing comma OK\n"
        "  limits: {\n"
        "    maxNodes: 5_000_000,\n"
        "  },\n"
        "}\n";
    static const char bad[] = "{ name: ";
    if (config_base_battery("JSON5", src, CBM_LANG_JSON5, "config.json5") != 0)
        return 1;
    return config_robustness("JSON5", bad, CBM_LANG_JSON5, "config.json5");
}

/* ── YAML ─────────────────────────────────────────────────────────────────────
 * Idiomatic YAML document with scalars, a nested mapping, and a sequence.
 * The spec has yaml_module_types = {"stream"} and yaml_var_types =
 * {"block_mapping_pair"}. No func/class/call types.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: block_mapping_pair -> Variable may extract but defs-present
 *   is skipped for the same stability reasons as JSON pairs.
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. Robustness should pass.
 */
TEST(repro_grammar_config_yaml) {
    static const char src[] =
        "name: cbm\n"
        "version: 0.8.1\n"
        "server:\n"
        "  host: localhost\n"
        "  port: 8080\n"
        "  tls: false\n"
        "languages:\n"
        "  - go\n"
        "  - python\n"
        "  - typescript\n";
    static const char bad[] = "name: cbm\n  - broken: [";
    if (config_base_battery("YAML", src, CBM_LANG_YAML, "config.yaml") != 0)
        return 1;
    return config_robustness("YAML", bad, CBM_LANG_YAML, "config.yaml");
}

/* ── TOML ─────────────────────────────────────────────────────────────────────
 * Idiomatic TOML file with a top-level pair (var_types = pair -> "Variable"),
 * a table header (class_types = table -> "Class"), and a table-array entry
 * (class_types = table_array_element -> "Class"). Defs-present asserts "Class"
 * for the [server] table.
 *
 * Dims asserted: 1-5 + R ("Class" from the [server] table).
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the table->Class mapping
 * is broken in the TOML grammar walker.
 */
TEST(repro_grammar_config_toml) {
    static const char src[] =
        "name = \"cbm\"\n"
        "version = \"0.8.1\"\n"
        "\n"
        "[server]\n"
        "host = \"localhost\"\n"
        "port = 8080\n"
        "tls = false\n"
        "\n"
        "[[language]]\n"
        "name = \"go\"\n"
        "enabled = true\n"
        "\n"
        "[[language]]\n"
        "name = \"python\"\n"
        "enabled = true\n";
    static const char bad[] = "name = \"cbm\"\n[[language\n";
    if (config_struct_battery("TOML", src, CBM_LANG_TOML, "config.toml",
                              "Class", NULL) != 0)
        return 1;
    return config_robustness("TOML", bad, CBM_LANG_TOML, "config.toml");
}

/* ── INI ──────────────────────────────────────────────────────────────────────
 * Idiomatic INI file with two sections (ini_class_types = {"section"} ->
 * "Class") and settings under each (ini_var_types = {"setting"}). Defs-present
 * asserts "Class" for the [database] section.
 *
 * Dims asserted: 1-5 + R ("Class").
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the section->Class mapping
 * is broken.
 */
TEST(repro_grammar_config_ini) {
    static const char src[] =
        "[database]\n"
        "host = localhost\n"
        "port = 5432\n"
        "name = cbm_db\n"
        "user = admin\n"
        "\n"
        "[cache]\n"
        "backend = redis\n"
        "ttl = 300\n"
        "max_size = 1024\n";
    static const char bad[] = "[database\nhost = x\n";
    if (config_struct_battery("INI", src, CBM_LANG_INI, "config.ini",
                              "Class", NULL) != 0)
        return 1;
    return config_robustness("INI", bad, CBM_LANG_INI, "config.ini");
}

/* ── HCL ──────────────────────────────────────────────────────────────────────
 * Idiomatic HCL (Terraform-style) file with a resource block
 * (hcl_class_types = {"block"} -> "Class"), attributes (hcl_var_types =
 * {"attribute"}), and a built-in function call (hcl_call_types =
 * {"function_call"} -> call extraction). The call to "tomap" is a standard
 * HCL built-in. Defs-present is skipped because HCL blocks require a label
 * node (the second string argument like "main") and QN derivation is complex;
 * the call assertion is the primary correctness signal.
 *
 * Dims asserted: 1-4 + 6 + R.
 * Dim 5 SKIPPED: block -> Class extraction and QN formation for labeled blocks
 *   is implementation-dependent; not asserting to avoid brittle tests.
 * Dims 7-8 SKIPPED: hcl_func_types = empty_types so no Function node exists
 *   to source the call against; running the pipeline would vacuously fail dim 7
 *   with 0 callable-sourced edges.
 * Expected: dims 1-4 GREEN; dim 6 likely GREEN (tomap maps to function_call).
 */
TEST(repro_grammar_config_hcl) {
    static const char src[] =
        "resource \"aws_instance\" \"main\" {\n"
        "  ami           = \"ami-0c55b159cbfafe1f0\"\n"
        "  instance_type = \"t2.micro\"\n"
        "\n"
        "  tags = tomap({\n"
        "    Name = \"cbm-server\"\n"
        "    Env  = \"prod\"\n"
        "  })\n"
        "}\n"
        "\n"
        "variable \"region\" {\n"
        "  default = \"us-east-1\"\n"
        "}\n";
    static const char bad[] = "resource \"aws_instance\" \"main\" {\n  ami = ";
    if (config_callable_battery("HCL", src, CBM_LANG_HCL, "main.tf",
                                NULL, "tomap") != 0)
        return 1;
    return config_robustness("HCL", bad, CBM_LANG_HCL, "main.tf");
}

/* ── XML ──────────────────────────────────────────────────────────────────────
 * Idiomatic XML document with a root element and nested child elements
 * (xml_class_types = {"element"} -> "Class"). The <config> root and <server>
 * child are both elements and should both yield "Class" defs.
 *
 * Dims asserted: 1-5 + R ("Class").
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the element->Class mapping
 * is broken in the XML grammar walker.
 */
TEST(repro_grammar_config_xml) {
    static const char src[] =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
        "<config>\n"
        "  <server>\n"
        "    <host>localhost</host>\n"
        "    <port>8080</port>\n"
        "  </server>\n"
        "  <database>\n"
        "    <url>postgres://localhost/cbm</url>\n"
        "    <maxConns>10</maxConns>\n"
        "  </database>\n"
        "</config>\n";
    static const char bad[] = "<config>\n  <server>\n    <host>";
    if (config_struct_battery("XML", src, CBM_LANG_XML, "config.xml",
                              "Class", NULL) != 0)
        return 1;
    return config_robustness("XML", bad, CBM_LANG_XML, "config.xml");
}

/* ── CSV ──────────────────────────────────────────────────────────────────────
 * Idiomatic CSV with a header row and data rows. The spec has csv_module_types
 * = {"document"} only; no func/class/var/call types are mapped. No defs or
 * calls are extracted.
 *
 * Dims asserted: 1-4 + R.
 * Dims 5-8 SKIPPED: no structural types in spec.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the CSV grammar
 * is broken on standard comma-separated input.
 */
TEST(repro_grammar_config_csv) {
    static const char src[] =
        "id,name,language,enabled\n"
        "1,cbm-go,go,true\n"
        "2,cbm-py,python,true\n"
        "3,cbm-ts,typescript,false\n";
    static const char bad[] = "id,name\n1,\"unclosed";
    if (config_base_battery("CSV", src, CBM_LANG_CSV, "data.csv") != 0)
        return 1;
    return config_robustness("CSV", bad, CBM_LANG_CSV, "data.csv");
}

/* ── PROPERTIES ───────────────────────────────────────────────────────────────
 * Idiomatic Java .properties file with key=value pairs
 * (properties_var_types = {"property"} -> "Variable"). Each key=value line
 * mints a "Variable" def; defs-present asserts at least one such def.
 *
 * Dims asserted: 1-5 + R ("Variable").
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate property -> Variable
 * mapping is broken.
 */
TEST(repro_grammar_config_properties) {
    static const char src[] =
        "# Application configuration\n"
        "app.name=cbm\n"
        "app.version=0.8.1\n"
        "server.host=localhost\n"
        "server.port=8080\n"
        "db.url=jdbc:postgresql://localhost/cbm\n"
        "db.pool.size=10\n";
    static const char bad[] = "app.name=cbm\nbroken";
    if (config_struct_battery("PROPERTIES", src, CBM_LANG_PROPERTIES,
                              "app.properties", "Variable", NULL) != 0)
        return 1;
    return config_robustness("PROPERTIES", bad, CBM_LANG_PROPERTIES,
                             "app.properties");
}

/* ── DOTENV ───────────────────────────────────────────────────────────────────
 * Idiomatic .env file with KEY=VALUE assignments. The spec has
 * dotenv_module_types = {"source_file"} only; all other type arrays are
 * empty_types. No defs or calls are extracted from the grammar tree itself
 * (key=value bindings are NOT mapped to any label in the spec).
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no var_types mapped in spec; no labelled defs are expected.
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the dotenv grammar
 * misparses standard KEY=VALUE lines.
 */
TEST(repro_grammar_config_dotenv) {
    static const char src[] =
        "# Database\n"
        "DATABASE_URL=postgres://localhost:5432/cbm\n"
        "DATABASE_POOL_SIZE=10\n"
        "\n"
        "# Server\n"
        "SERVER_HOST=0.0.0.0\n"
        "SERVER_PORT=8080\n"
        "DEBUG=false\n"
        "SECRET_KEY=supersecret\n";
    static const char bad[] = "KEY=value\nBROKEN=\"unclosed";
    if (config_base_battery("DOTENV", src, CBM_LANG_DOTENV, ".env") != 0)
        return 1;
    return config_robustness("DOTENV", bad, CBM_LANG_DOTENV, ".env");
}

/* ── KDL ──────────────────────────────────────────────────────────────────────
 * Idiomatic KDL document with nodes and children. The spec has kdl_module_types
 * = {"document"} only; all other type arrays are empty_types. No defs or calls
 * are extracted from the grammar tree (KDL nodes are not mapped to any label).
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no var/func/class types in spec.
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate the KDL grammar
 * is broken on standard node syntax.
 */
TEST(repro_grammar_config_kdl) {
    static const char src[] =
        "package {\n"
        "  name \"cbm\"\n"
        "  version \"0.8.1\"\n"
        "  description \"Codebase memory MCP server\"\n"
        "}\n"
        "\n"
        "server host=\"localhost\" port=8080 {\n"
        "  tls false\n"
        "  timeout 30\n"
        "}\n"
        "\n"
        "language \"go\" enabled=true\n"
        "language \"python\" enabled=true\n";
    static const char bad[] = "server host=\"localhost\" {\n  tls";
    if (config_base_battery("KDL", src, CBM_LANG_KDL, "config.kdl") != 0)
        return 1;
    return config_robustness("KDL", bad, CBM_LANG_KDL, "config.kdl");
}

/* ── RON ──────────────────────────────────────────────────────────────────────
 * Idiomatic RON (Rusty Object Notation) file with a struct literal. The spec
 * has ron_module_types = {"source_file"} only; all other type arrays are
 * empty_types. No defs or calls are extracted from the grammar tree.
 *
 * Dims asserted: 1-4 + R.
 * Dim 5 SKIPPED: no func/class/var types in spec; struct literals are not
 *   mapped to any def label (RON is a data serialisation format, not a schema).
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-4. RED on dim 1 would indicate the RON grammar
 * misparses valid struct-literal syntax.
 */
TEST(repro_grammar_config_ron) {
    static const char src[] =
        "Config(\n"
        "  name: \"cbm\",\n"
        "  version: (major: 0, minor: 8, patch: 1),\n"
        "  languages: [\n"
        "    Language(name: \"go\", enabled: true),\n"
        "    Language(name: \"python\", enabled: true),\n"
        "  ],\n"
        "  debug: false,\n"
        ")\n";
    static const char bad[] = "Config(\n  name: \"cbm\",\n  broken: [";
    if (config_base_battery("RON", src, CBM_LANG_RON, "config.ron") != 0)
        return 1;
    return config_robustness("RON", bad, CBM_LANG_RON, "config.ron");
}

/* ── PKL ──────────────────────────────────────────────────────────────────────
 * Idiomatic PKL (Apple Pkl) module with a class definition
 * (pkl_class_types = {"clazz"} -> "Class"), a method inside it
 * (pkl_func_types = {"classMethod", "objectMethod"} -> "Function"), and
 * class properties (pkl_var_types = {"classProperty", "objectProperty"}).
 * pkl_call_types = empty_types so no call extraction occurs.
 *
 * Dims asserted: 1-5 + R ("Class" for the class def, "Function" for the method).
 * Dims 6-8 SKIPPED: call_types = empty_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate clazz->Class or
 * classMethod->Function mapping is broken in the PKL grammar walker.
 */
TEST(repro_grammar_config_pkl) {
    static const char src[] =
        "module cbm.Config\n"
        "\n"
        "function makeUrl(host: String, port: Int): String = \"http://\\(host):\\(port)\"\n"
        "\n"
        "class Server {\n"
        "  host: String = \"localhost\"\n"
        "  port: Int = 8080\n"
        "  tls: Boolean = false\n"
        "\n"
        "  function url(): String = \"http://\\(host):\\(port)\"\n"
        "}\n"
        "\n"
        "server = new Server {\n"
        "  host = \"0.0.0.0\"\n"
        "  port = 9000\n"
        "}\n";
    static const char bad[] = "module cbm.Config\nclass Server {\n  host:";
    if (config_struct_battery("PKL", src, CBM_LANG_PKL, "config.pkl",
                              "Class", "Function") != 0)
        return 1;
    return config_robustness("PKL", bad, CBM_LANG_PKL, "config.pkl");
}

/* ── NICKEL ───────────────────────────────────────────────────────────────────
 * Idiomatic Nickel configuration file with a let-binding that defines a
 * function (nickel_func_types = {"fun"} -> "Function") and an application of
 * that function (nickel_call_types = {"infix_expr"}). Nickel uses infix
 * application syntax: `f x` rather than `f(x)`, so the call_types node is
 * infix_expr rather than a traditional call_expression.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for the `fun` binding.
 * Dim 6 expected GREEN: call_expression / infix_expr extraction for the
 *   application site. Note: inv_has_call uses substring match on callee_name;
 *   if the callee_name is left empty for operator-style infix_expr nodes this
 *   dim will RED and document the gap.
 * Dim 7 expected RED: infix_expr nodes may not carry a callee name that matches
 *   the enclosing fun node; the call is likely attributed at Module level.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 *
 * Expected GREEN: dims 1-5. Dims 6-7 are likely RED (call extraction gap for
 * Nickel infix application). Robustness should pass.
 */
TEST(repro_grammar_config_nickel) {
    /* All calls must live INSIDE a function body for callable-sourcing (dim 7):
     * `addPort port 0` is applied inside mkServer's `fun` body, so its CALLS edge
     * sources at the mkServer Function. The output record only REFERENCES mkServer
     * (a bare value, not an application) so there is no Module-level call site. */
    static const char src[] =
        "let addPort = fun base offset => base + offset in\n"
        "let mkServer = fun host port => {\n"
        "  host = host,\n"
        "  port = addPort port 0,\n"
        "  url  = \"http://\" ++ host,\n"
        "} in\n"
        "{\n"
        "  make  = mkServer,\n"
        "  debug = false,\n"
        "}\n";
    static const char bad[] = "let addPort = fun base offset =>";
    if (config_callable_battery("Nickel", src, CBM_LANG_NICKEL, "config.ncl",
                                "Function", "addPort") != 0)
        return 1;
    if (config_robustness("Nickel", bad, CBM_LANG_NICKEL, "config.ncl") != 0)
        return 1;
    return config_pipeline_battery("Nickel", "config.ncl", src);
}

/* ── JSONNET ──────────────────────────────────────────────────────────────────
 * Idiomatic Jsonnet configuration file with a local function binding
 * (jsonnet_func_types = {"anonymous_function"} -> "Function") and a call
 * site (jsonnet_call_types = {"functioncall"}). Jsonnet functions are always
 * anonymous; the def's name comes from the local binding identifier.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for the local anonymous_function binding.
 * Dim 6 expected GREEN: functioncall extraction for the call to makeServer.
 * Dim 7 expected RED: anonymous_function nodes may not resolve to a named
 *   Function node during the enclosing-func walk; calls inside the function
 *   body are likely sourced at Module level.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 *
 * Expected GREEN: dims 1-6. Dims 7 likely RED. Robustness should pass.
 */
TEST(repro_grammar_config_jsonnet) {
    /* All calls must live INSIDE a function body for callable-sourcing (dim 7):
     * `build` applies makeServer within its own body, so the CALLS edge sources at
     * the build Function. The output object only REFERENCES build (a bare value,
     * not a functioncall) so there is no Module-level call site. dim 6 still sees
     * a call to makeServer (now in build's body instead of at top level). */
    static const char src[] =
        "local makeServer(host, port) = {\n"
        "  host: host,\n"
        "  port: port,\n"
        "  url: 'http://' + host + ':' + port,\n"
        "};\n"
        "\n"
        "local build(host) = makeServer(host, 8080);\n"
        "\n"
        "{\n"
        "  server: build,\n"
        "  debug: false,\n"
        "}\n";
    static const char bad[] = "local makeServer(host, port) = {";
    if (config_callable_battery("Jsonnet", src, CBM_LANG_JSONNET, "config.jsonnet",
                                "Function", "makeServer") != 0)
        return 1;
    if (config_robustness("Jsonnet", bad, CBM_LANG_JSONNET, "config.jsonnet") != 0)
        return 1;
    return config_pipeline_battery("Jsonnet", "config.jsonnet", src);
}

/* ── STARLARK ─────────────────────────────────────────────────────────────────
 * Idiomatic Starlark BUILD file with a function definition
 * (starlark_func_types = {"function_definition", "lambda"} -> "Function") and
 * call expressions (starlark_call_types = {"call"}). Starlark is Python-like;
 * function definitions use the `def` keyword. Calls inside the function body
 * and at module level both map to "call" nodes.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for the def statement.
 * Dim 6 expected GREEN: call extraction for the print() or go_binary() call.
 * Dim 7 expected GREEN: Starlark function_definition is a well-named node;
 *   calls inside a function body should be correctly sourced at the Function
 *   node rather than Module. Dim 7 RED would indicate the enclosing-func walk
 *   is broken for Starlark function_definition nodes.
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 *
 * Robustness should pass.
 */
TEST(repro_grammar_config_starlark) {
    /* All calls must live INSIDE a function body for callable-sourcing (dim 7):
     * both calls are inside make_binary's body, so their CALLS edges source at
     * the make_binary Function. The module-level statement only REFERENCES
     * make_binary (a bare name assignment, not a call) so there is no
     * Module-level call site.
     *
     * Callable-sourcing (dim 7) counts CALLS *edges* in the graph, and pass_calls
     * only emits a CALLS edge when the callee resolves to a node in the file
     * (an unresolved external callee yields no edge — pass_calls.c:389). The
     * go_binary(...) call satisfies the dim-6 calls-extracted assertion (the
     * "go_binary" callee string is extracted), but go_binary is an external rule
     * with no def here, so it produces no edge. _base_deps() is defined in this
     * same file, so the in-body call to it resolves to a Function node and gives
     * dim 7 a Function-sourced edge to attribute. */
    static const char src[] =
        "def _base_deps():\n"
        "    return [\"//internal/cbm\"]\n"
        "\n"
        "def make_binary(name, srcs, deps = []):\n"
        "    \"\"\"Wrapper around go_binary for internal defaults.\"\"\"\n"
        "    go_binary(\n"
        "        name = name,\n"
        "        srcs = srcs,\n"
        "        deps = deps + _base_deps(),\n"
        "    )\n"
        "\n"
        "default_rule = make_binary\n";
    static const char bad[] = "def make_binary(name, srcs";
    if (config_callable_battery("Starlark", src, CBM_LANG_STARLARK, "BUILD",
                                "Function", "go_binary") != 0)
        return 1;
    if (config_robustness("Starlark", bad, CBM_LANG_STARLARK, "BUILD") != 0)
        return 1;
    return config_pipeline_battery("Starlark", "BUILD", src);
}

/* ── Suite ───────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_config) {
    RUN_TEST(repro_grammar_config_json);
    RUN_TEST(repro_grammar_config_json5);
    RUN_TEST(repro_grammar_config_yaml);
    RUN_TEST(repro_grammar_config_toml);
    RUN_TEST(repro_grammar_config_ini);
    RUN_TEST(repro_grammar_config_hcl);
    RUN_TEST(repro_grammar_config_xml);
    RUN_TEST(repro_grammar_config_csv);
    RUN_TEST(repro_grammar_config_properties);
    RUN_TEST(repro_grammar_config_dotenv);
    RUN_TEST(repro_grammar_config_kdl);
    RUN_TEST(repro_grammar_config_ron);
    RUN_TEST(repro_grammar_config_pkl);
    RUN_TEST(repro_grammar_config_nickel);
    RUN_TEST(repro_grammar_config_jsonnet);
    RUN_TEST(repro_grammar_config_starlark);
}
