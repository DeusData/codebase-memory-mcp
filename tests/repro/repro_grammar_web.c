/*
 * repro_grammar_web.c -- Per-grammar INVARIANT battery for the
 * WEB / MARKUP / SCHEMA language family.
 *
 * One TEST() per language so per-language RED/GREEN shows on the bug-repro
 * board. Each test runs a battery adapted to what the language actually models:
 * many web/markup/schema languages have NO functions or calls (HTML, CSS, Vue,
 * Svelte, Astro, GraphQL, Prisma, JSDoc, GoTemplate as a pure-template host).
 * The battery dimensions applied per language are documented in the per-TEST
 * comment.
 *
 * Languages covered (12) and the CBM_LANG_* enum each uses (all verified in
 * internal/cbm/cbm.h; none missing, none skipped):
 *   HTML        -> CBM_LANG_HTML
 *   CSS         -> CBM_LANG_CSS
 *   SCSS        -> CBM_LANG_SCSS
 *   Vue         -> CBM_LANG_VUE
 *   Svelte      -> CBM_LANG_SVELTE
 *   Astro       -> CBM_LANG_ASTRO
 *   GraphQL     -> CBM_LANG_GRAPHQL
 *   Protobuf    -> CBM_LANG_PROTOBUF
 *   Thrift      -> CBM_LANG_THRIFT
 *   Prisma      -> CBM_LANG_PRISMA
 *   GoTemplate  -> CBM_LANG_GOTEMPLATE
 *   JSDoc       -> CBM_LANG_JSDOC
 *
 * BATTERY DIMENSIONS
 * ------------------
 * SINGLE-FILE (cbm_extract_file, via inv_rx + inv_count_* helpers):
 *   1. extract-clean   : inv_extract_clean(src,lang,file) == 1
 *                        (parser returned a result and did not set has_error).
 *   2. labels-valid    : inv_count_bad_labels(r) == 0
 *                        (every extracted def label is in the known label set).
 *   3. fqn-wellformed  : inv_count_bad_fqns(r) == 0
 *                        (no empty/".."/leading or trailing '/'/whitespace QNs).
 *   4. ranges-valid    : inv_count_bad_ranges(r) == 0
 *                        (start_line >= 1 and start_line <= end_line).
 *   5. defs-present    : at least one def with the expected label is extracted.
 *                        SKIPPED for languages whose spec has no func_types,
 *                        class_types, or field_types (HTML, CSS, Vue, Svelte,
 *                        Astro, GoTemplate, JSDoc). A SKIP is annotated in the
 *                        per-TEST comment; the dimension is not asserted.
 *   6. calls-extracted : inv_has_call(r, callee) == 1.
 *                        Only asserted for languages that have non-empty
 *                        call_types: CSS (call_expression), SCSS (call_expression,
 *                        include_statement), GoTemplate (function_call /
 *                        template_action). Skipped for all others.
 *
 * FULL-PIPELINE (rh_index_files -> cbm_store_t*, via inv_count_* store helpers):
 *   7. callable-sourcing : inv_count_calls_by_source(store,project,&mod,&call).
 *                          Only asserted when dim 6 is asserted (SCSS, GoTemplate).
 *                          For SCSS: expected RED (mixin_statement is parsed as
 *                          func_types so a "Function" def is extracted, but
 *                          cbm_find_enclosing_func relies on the same node being
 *                          recognised in func_kinds_for_lang; if that mapping is
 *                          absent the call will be sourced at Module).
 *                          For GoTemplate: expected RED (no func_types so no
 *                          Function/Method node exists to source the call).
 *   8. no-dangling       : inv_count_dangling_edges(store, project, "CALLS") == 0.
 *                          Asserted together with dim 7 when the pipeline is run.
 *
 * STRUCTURAL-ONLY LANGUAGES (dims 1-5, no call/pipeline dims):
 *   HTML, VUE, SVELTE, ASTRO  -- only module_types in spec; no defs extracted
 *                                from the host grammar node tree (embedded <script>
 *                                re-parsed by the JS sub-grammar separately).
 *                                Dims 1-4 only (dim 5 skipped -- no def labels).
 *   GRAPHQL                   -- class_types (object_type_definition etc. -> "Class")
 *                                and field_types (field_definition -> "Field");
 *                                no call_types. Dims 1-5 ("Class" + "Field").
 *   PROTOBUF                  -- func_types (rpc -> "Function"), class_types
 *                                (message -> "Class"), field_types (field -> "Field");
 *                                call_types = empty. Dims 1-5 ("Function", "Class").
 *   THRIFT                    -- func_types (function_definition -> "Function"),
 *                                class_types (struct_definition -> "Class"),
 *                                field_types (field -> "Field"); call_types = empty.
 *                                Dims 1-5 ("Function", "Class").
 *   PRISMA                    -- class_types (model_declaration -> "Class"),
 *                                field_types (column_declaration -> "Field");
 *                                no func_types; call_types present (call_expression)
 *                                but only for default-value expressions, not
 *                                first-class callable definitions.
 *                                Dims 1-5 ("Class", "Field").
 *   JSDOC                     -- only module_types; no defs or calls in the tree.
 *                                Dims 1-4 only.
 *
 * LANGUAGES WITH CALLABLES (dims 1-8):
 *   CSS         -- call_types = call_expression (url(), calc(), etc.);
 *                  no func_types so no "Function" def is minted. Dims 1-4 + 6 only
 *                  (no defs-present, no pipeline for CSS-only fixtures since the
 *                  calls have no Function source to attribute to).
 *   SCSS        -- func_types = mixin_statement, function_statement -> "Function";
 *                  call_types = call_expression. Dims 1-8. Dim 7 expected RED.
 *   GOTEMPLATE  -- call_types = function_call, method_call, template_action;
 *                  no func_types. Dims 1-4 + 6 + 7-8 (dim 5 skipped -- no def
 *                  minted). Dims 7-8 expected RED (no Function node to source).
 *
 * Coding rule: inline comments are line comments only (no block comments inside
 * block comments).
 */

#include "test_framework.h"
#include "repro_invariant_lib.h"
#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* ── Structural-only battery (dims 1-4) ─────────────────────────────────────
 *
 * Runs the four base invariants that apply to EVERY language regardless of
 * whether it has callable or structural defs. Returns 0 on PASS, 1 on FAIL.
 * Used for languages whose spec has neither func_types nor class_types
 * (HTML, VUE, SVELTE, ASTRO, JSDoc).
 */
static int structural_base_battery(const char *lang_tag, const char *src,
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

/* ── Schema/structural battery (dims 1-5) ───────────────────────────────────
 *
 * Adds the defs-present dimension to the base battery. Used for GraphQL,
 * Protobuf, Thrift, and Prisma whose specs include class_types and/or
 * func_types. Returns 0 on PASS, 1 on FAIL.
 */
static int schema_battery(const char *lang_tag, const char *src,
                          CBMLanguage lang, const char *file,
                          const char *expect_label, const char *expect_label2) {
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

    cbm_free_result(r);
    return fails ? 1 : 0;
}

/* ── Callable battery (dims 1-6) ────────────────────────────────────────────
 *
 * Adds dims 5 and 6 (defs-present + calls-extracted) to the base invariants.
 * Pass NULL for expect_label when the language has no func/class def to assert
 * (e.g. pure-call languages like CSS). Returns 0 on PASS, 1 on FAIL.
 */
static int callable_battery(const char *lang_tag, const char *src,
                            CBMLanguage lang, const char *file,
                            const char *expect_label, const char *callee) {
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
    if (inv_has_call(r, callee) != 1) {
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
 * callable-sourcing + no-dangling. Returns 0 on PASS, 1 on FAIL. For web
 * languages that reach this path (SCSS, GoTemplate), dim 7 is expected RED:
 * SCSS mixin calls are likely sourced at Module (func_kinds_for_lang mapping
 * absent for mixin_statement); GoTemplate has no func_types so the call is
 * unconditionally Module-sourced. RED rows are the deliverable signal.
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

/* ── HTML ────────────────────────────────────────────────────────────────────
 * Idiomatic minimal document with an element that carries an id attribute.
 * The host grammar spec has only html_module_types; no func/class/field types
 * are declared. Embedded <script> content is re-parsed separately by the JS
 * sub-grammar, not extracted by the HTML grammar node walker.
 *
 * Dims asserted: 1-4 (extract-clean, labels-valid, fqn-wellformed, ranges-valid).
 * Dim 5 SKIPPED: no defs are extracted from the HTML grammar tree itself.
 * Dims 6-8 SKIPPED: no call_types in spec; no pipeline run.
 *
 * Expected GREEN: dims 1-4.
 */
TEST(repro_grammar_web_html) {
    static const char src[] =
        "<!DOCTYPE html>\n"
        "<html lang=\"en\">\n"
        "<head><title>Test</title></head>\n"
        "<body>\n"
        "  <div id=\"main\">\n"
        "    <p class=\"intro\">Hello, world!</p>\n"
        "  </div>\n"
        "</body>\n"
        "</html>\n";
    return structural_base_battery("HTML", src, CBM_LANG_HTML, "index.html");
}

/* ── CSS ─────────────────────────────────────────────────────────────────────
 * Idiomatic stylesheet with a rule block containing a property value that uses
 * url() and calc() call expressions (the only call_types in the CSS spec).
 * No func_types are declared; no "Function" defs are minted.
 *
 * Dims asserted: 1-4 + 6 (calls-extracted).
 * Dim 5 SKIPPED: no func/class/field_types; no defs extracted.
 * Dims 7-8 SKIPPED: no Function/Method node exists to source the call; running
 * the pipeline would vacuously fail dim 7 with 0 callable-sourced edges. The
 * pipeline skip is appropriate -- the gap is at the grammar spec level, not the
 * enclosing-func walker.
 *
 * Expected: dims 1-4 GREEN; dim 6 likely GREEN (url() maps to call_expression
 * in tree-sitter-css). Dim 6 RED would indicate call extraction is broken.
 */
TEST(repro_grammar_web_css) {
    static const char src[] =
        "body {\n"
        "  margin: 0;\n"
        "  background: url(\"bg.png\") no-repeat;\n"
        "  width: calc(100% - 2rem);\n"
        "}\n"
        "\n"
        ".container {\n"
        "  padding: 1rem;\n"
        "}\n";
    return callable_battery("CSS", src, CBM_LANG_CSS, "style.css",
                            NULL, "url");
}

/* ── SCSS ────────────────────────────────────────────────────────────────────
 * Idiomatic SCSS: a @mixin definition (func_types = mixin_statement) and a
 * rule that @includes it (call_types = call_expression via the include).
 * The mixin_statement is in func_types so extract_func_def fires and mints a
 * "Function" def for "flex-center". The @include fires a call_expression.
 *
 * Dims asserted: 1-8 (full battery).
 * Dim 5 expected GREEN: "Function" def for "flex-center" (and "card").
 * Dim 6 expected GREEN: call to "flex-center" via @include.
 * Dim 7 expected GREEN: the @include flex-center sits inside the "card"
 *   mixin_statement body. mixin_statement is in scss_func_types, so
 *   push_boundary_scopes pushes a SCOPE_FUNC for "card" and the in-body call
 *   sources to the "card" Function rather than the Module. (The earlier fixture
 *   put the @include inside a plain rule_set, which is not a callable, so the
 *   call was legitimately Module-sourced -- a broken-fixture, not a prod bug.)
 * Dim 8 expected GREEN: dangling edge check.
 */
TEST(repro_grammar_web_scss) {
    static const char src[] =
        "@mixin flex-center {\n"
        "  display: flex;\n"
        "  justify-content: center;\n"
        "  align-items: center;\n"
        "}\n"
        "\n"
        "@mixin card {\n"
        "  @include flex-center;\n"
        "  background: #fff;\n"
        "}\n";
    if (callable_battery("SCSS", src, CBM_LANG_SCSS, "styles.scss",
                         "Function", "flex-center") != 0)
        return 1;
    return pipeline_battery("SCSS", "styles.scss", src);
}

/* ── Vue ─────────────────────────────────────────────────────────────────────
 * Idiomatic single-file component with <template>, <script>, and <style>
 * blocks. The Vue host grammar spec has only vue_module_types = {"document"};
 * no func/class/field types. Embedded <script> content uses the embedded-
 * imports walker (re-parsed as JS), but that does not affect the SFC host
 * grammar's own def extraction.
 *
 * Dims asserted: 1-4.
 * Dims 5-8 SKIPPED: no defs in host grammar; no call_types; no pipeline.
 * Expected GREEN: dims 1-4.
 */
TEST(repro_grammar_web_vue) {
    static const char src[] =
        "<template>\n"
        "  <div class=\"hello\">\n"
        "    <h1>{{ msg }}</h1>\n"
        "  </div>\n"
        "</template>\n"
        "\n"
        "<script>\n"
        "export default {\n"
        "  props: { msg: String }\n"
        "}\n"
        "</script>\n"
        "\n"
        "<style scoped>\n"
        ".hello { font-size: 1rem; }\n"
        "</style>\n";
    return structural_base_battery("Vue", src, CBM_LANG_VUE, "Hello.vue");
}

/* ── Svelte ──────────────────────────────────────────────────────────────────
 * Idiomatic Svelte component with a <script> block and a template body.
 * The Svelte host grammar spec has only svelte_module_types = {"document"} and
 * svelte_branch_types; no func/class/field or call types. Embedded <script>
 * is re-parsed as JS by the embedded-imports walker.
 *
 * Dims asserted: 1-4.
 * Dims 5-8 SKIPPED.
 * Expected GREEN: dims 1-4.
 */
TEST(repro_grammar_web_svelte) {
    static const char src[] =
        "<script>\n"
        "  let count = 0;\n"
        "  function increment() { count++; }\n"
        "</script>\n"
        "\n"
        "<button on:click={increment}>Clicked {count} times</button>\n";
    return structural_base_battery("Svelte", src, CBM_LANG_SVELTE,
                                   "Counter.svelte");
}

/* ── Astro ───────────────────────────────────────────────────────────────────
 * Idiomatic Astro component with a frontmatter fence (--- block) and a
 * template body. The Astro spec has only astro_module_types = {"document"};
 * the frontmatter_js_block is re-parsed as JS for import extraction but the
 * Astro host grammar tree yields no func/class/field defs itself.
 *
 * Dims asserted: 1-4.
 * Dims 5-8 SKIPPED.
 * Expected GREEN: dims 1-4.
 */
TEST(repro_grammar_web_astro) {
    static const char src[] =
        "---\n"
        "import Header from './Header.astro';\n"
        "const title = 'Hello';\n"
        "---\n"
        "\n"
        "<html>\n"
        "  <head><title>{title}</title></head>\n"
        "  <body>\n"
        "    <Header />\n"
        "    <main><p>Content</p></main>\n"
        "  </body>\n"
        "</html>\n";
    return structural_base_battery("Astro", src, CBM_LANG_ASTRO,
                                   "index.astro");
}

/* ── GraphQL ─────────────────────────────────────────────────────────────────
 * Idiomatic schema with a type (object_type_definition -> "Class") containing
 * fields (field_definition -> "Field"), plus an interface and a query type.
 * graphql_class_types covers object_type_definition so "User" maps to "Class".
 * graphql_field_types covers field_definition so "id"/"name" map to "Field".
 * No call_types in spec; no call extraction.
 *
 * Dims asserted: 1-5 ("Class" + "Field").
 * Dims 6-8 SKIPPED: no call_types.
 * Expected GREEN: dims 1-5 (schema languages with well-formed node types tend
 * to extract cleanly). Dim 5 RED would indicate the type/field mapping broke.
 */
TEST(repro_grammar_web_graphql) {
    static const char src[] =
        "interface Node {\n"
        "  id: ID!\n"
        "}\n"
        "\n"
        "type User implements Node {\n"
        "  id: ID!\n"
        "  name: String!\n"
        "  email: String\n"
        "}\n"
        "\n"
        "type Query {\n"
        "  user(id: ID!): User\n"
        "}\n";
    return schema_battery("GraphQL", src, CBM_LANG_GRAPHQL, "schema.graphql",
                          "Class", "Field");
}

/* ── Protobuf ────────────────────────────────────────────────────────────────
 * Idiomatic proto3 file: an import, a message (protobuf_class_types -> "Class"),
 * fields inside the message (protobuf_field_types -> "Field"), a service
 * (also in class_types -> "Class"), and an rpc declaration
 * (protobuf_func_types = {"rpc"} -> "Function").
 * call_types = empty_types so no call extraction occurs.
 *
 * Dims asserted: 1-5 ("Function" for the rpc, "Class" for the message).
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the rpc->Function or
 * message->Class mapping is broken.
 */
TEST(repro_grammar_web_protobuf) {
    static const char src[] =
        "syntax = \"proto3\";\n"
        "\n"
        "import \"google/protobuf/timestamp.proto\";\n"
        "\n"
        "message User {\n"
        "  uint64 id = 1;\n"
        "  string name = 2;\n"
        "  string email = 3;\n"
        "}\n"
        "\n"
        "service UserService {\n"
        "  rpc GetUser (User) returns (User);\n"
        "}\n";
    return schema_battery("Protobuf", src, CBM_LANG_PROTOBUF, "user.proto",
                          "Function", "Class");
}

/* ── Thrift ──────────────────────────────────────────────────────────────────
 * Idiomatic Thrift IDL: a namespace declaration (mapped via import_types),
 * a struct (thrift_class_types -> "Class"), a field inside it
 * (thrift_field_types -> "Field"), a service, and a function_definition inside
 * the service (thrift_func_types = {"function_definition","service_definition"}
 * -> "Function"). call_types = empty_types; no call extraction.
 *
 * Dims asserted: 1-5 ("Function" for the service function, "Class" for the
 * struct).
 * Dims 6-8 SKIPPED: no call_types in spec.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the Thrift struct->Class
 * or function_definition->Function mapping is broken.
 */
TEST(repro_grammar_web_thrift) {
    static const char src[] =
        "namespace go users\n"
        "\n"
        "struct User {\n"
        "  1: required i64 id,\n"
        "  2: required string name,\n"
        "  3: optional string email,\n"
        "}\n"
        "\n"
        "service UserService {\n"
        "  User GetUser(1: i64 id),\n"
        "  void CreateUser(1: User user),\n"
        "}\n";
    return schema_battery("Thrift", src, CBM_LANG_THRIFT, "user.thrift",
                          "Function", "Class");
}

/* ── Prisma ──────────────────────────────────────────────────────────────────
 * Idiomatic Prisma schema: a datasource block, a generator block, a model
 * (prisma_class_types = {"model_declaration",...} -> "Class"), and field
 * declarations inside it (prisma_field_types = {"column_declaration"} ->
 * "Field"). prisma_call_types = {"call_expression"} covers default-value
 * function calls like now() and autoincrement(); these are extracted as calls
 * but there is no Function node to source them from. No func_types.
 *
 * Dims asserted: 1-5 ("Class" for the model, "Field" for the fields).
 * Dims 6-8 SKIPPED: while call_types exists, the call_expression nodes are
 * default-value fragments, not first-class callable definitions; running the
 * pipeline would produce zero callable-sourced edges and vacuously fail dim 7.
 * Expected GREEN: dims 1-5. Dim 5 RED would indicate the model->Class or
 * column_declaration->Field mapping is broken.
 */
TEST(repro_grammar_web_prisma) {
    static const char src[] =
        "datasource db {\n"
        "  provider = \"postgresql\"\n"
        "  url      = env(\"DATABASE_URL\")\n"
        "}\n"
        "\n"
        "generator client {\n"
        "  provider = \"prisma-client-js\"\n"
        "}\n"
        "\n"
        "model User {\n"
        "  id        Int      @id @default(autoincrement())\n"
        "  name      String\n"
        "  email     String   @unique\n"
        "  createdAt DateTime @default(now())\n"
        "}\n";
    return schema_battery("Prisma", src, CBM_LANG_PRISMA, "schema.prisma",
                          "Class", "Field");
}

/* ── GoTemplate ──────────────────────────────────────────────────────────────
 * Idiomatic Go template: a "greeting" named template whose body calls the
 * built-in printf, and a "page" named template whose body invokes greeting via
 * a {{ template }} action. gotemplate_call_types = {"function_call",
 * "method_call", "template_action"}; gotemplate_module_types = {"template"}.
 * gotemplate_func_types = {"define_action"} so each {{ define "x" }} block mints
 * a "Function" def and pushes a SCOPE_FUNC for call attribution.
 *
 * Dims asserted: 1-4 + 6 + 7-8.
 * Dim 6 expected GREEN: call to "printf" inside the greeting define body.
 * Dim 7 expected GREEN: the {{ template "greeting" }} call inside the page
 *   define body resolves to the same-file greeting Function and sources to the
 *   page Function. (Previously the spec had no func_types -- the def-extractor
 *   minted a "Function" for define_action but the scope-tracking func_types list
 *   was empty, so the call mis-sourced to Module: a production sync bug, now
 *   fixed by adding define_action to gotemplate_func_types + a compute_func_qn
 *   case that strips the quoted template name. The fixture also moved its only
 *   call sites from top level into define bodies.)
 * Dim 8 expected GREEN: no dangling CALLS endpoints.
 */
TEST(repro_grammar_web_gotemplate) {
    static const char src[] =
        "{{ define \"greeting\" }}\n"
        "  {{ $msg := printf \"Welcome to %s\" .Site }}\n"
        "  <h1>{{ $msg }}</h1>\n"
        "{{ end }}\n"
        "\n"
        "{{ define \"page\" }}\n"
        "  {{ template \"greeting\" . }}\n"
        "{{ end }}\n";
    if (callable_battery("GoTemplate", src, CBM_LANG_GOTEMPLATE,
                         "index.tmpl", NULL, "printf") != 0)
        return 1;
    return pipeline_battery("GoTemplate", "index.tmpl", src);
}

/* ── JSDoc ───────────────────────────────────────────────────────────────────
 * Idiomatic JSDoc comment block. The JSDoc spec has only
 * jsdoc_module_types = {"document"}; no func/class/field or call types are
 * declared. No defs or calls are extracted from the JSDoc grammar tree.
 *
 * Dims asserted: 1-4 (extract-clean, labels-valid, fqn-wellformed, ranges-valid).
 * Dims 5-8 SKIPPED: no defs, no calls, no pipeline.
 * Expected GREEN: dims 1-4. extract-clean RED would indicate a parser crash or
 * has_error set on a valid JSDoc block.
 */
TEST(repro_grammar_web_jsdoc) {
    static const char src[] =
        "/**\n"
        " * Adds two numbers together.\n"
        " * @param {number} a - The first operand.\n"
        " * @param {number} b - The second operand.\n"
        " * @returns {number} The sum of a and b.\n"
        " * @example\n"
        " * const result = add(1, 2); // 3\n"
        " */\n";
    return structural_base_battery("JSDoc", src, CBM_LANG_JSDOC, "api.jsdoc");
}

/* ── Suite ──────────────────────────────────────────────────────────────────── */

SUITE(repro_grammar_web) {
    RUN_TEST(repro_grammar_web_html);
    RUN_TEST(repro_grammar_web_css);
    RUN_TEST(repro_grammar_web_scss);
    RUN_TEST(repro_grammar_web_vue);
    RUN_TEST(repro_grammar_web_svelte);
    RUN_TEST(repro_grammar_web_astro);
    RUN_TEST(repro_grammar_web_graphql);
    RUN_TEST(repro_grammar_web_protobuf);
    RUN_TEST(repro_grammar_web_thrift);
    RUN_TEST(repro_grammar_web_prisma);
    RUN_TEST(repro_grammar_web_gotemplate);
    RUN_TEST(repro_grammar_web_jsdoc);
}
