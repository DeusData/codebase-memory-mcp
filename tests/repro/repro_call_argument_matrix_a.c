/*
 * repro_call_argument_matrix_a.c -- all call-capable languages from GO through AGDA.
 *
 * This is the general reference-preservation invariant, not a claim that every
 * fixture passes a function object. Most semantic rows intentionally pass a
 * scalar/value parameter named `handler` to `accept`, while a different function
 * directly invokes the function named `handler`. The identical spelling gets two
 * exact AST roles and proves that only the invoked occurrence is replaced by
 * CALLS. True function-object/callback cases are covered separately by
 * repro_call_argument_usages.c and repro_call_scope_usages.c.
 *
 * Languages whose call metadata describes a DSL without ordinary user routine
 * bodies use paired native argument and bare-reference fixtures. The bare fixture
 * distinguishes missing reference vocabulary from call-boundary suppression.
 * CSS custom properties are an explicit domain exception because their only
 * reference syntax is var(...); that row makes no vocabulary-isolation claim.
 */
#include "test_framework.h"
#include "lang_specs.h"

#include <stdio.h>
#include <string.h>

size_t repro_call_argument_matrix_a_copy_language_ids(CBMLanguage *language_ids, size_t capacity);

typedef enum {
    MATRIX_SEMANTIC,
    MATRIX_NATIVE_SCOPE,
} MatrixMode;

typedef enum {
    NATIVE_PAIRED,
    NATIVE_DOMAIN_EXCEPTION,
} NativeFixtureMode;

typedef struct {
    const char *tag;
    CBMLanguage language;
    const char *filename;
    const char *source;
    MatrixMode mode;
    NativeFixtureMode native_fixture_mode;
    int expect_lexical_local_shadow; /* semantic parameter rows only */
    const char *value_name;
    const char *call_kind;
    int scope_node_count;
    int scope_call_count;
    const char *reference_node_kind;
    const char *reference_node_text;
    int scope_reference_node_count;
    int bare_reference_node_count;
    int bare_call_count;
    const char *reason;
    const char *outer_callee;
    const char *scope_caller;
    const char *scope_value;
    const char *scope_definition;
    const char *outer_definition;
    const char *bare_source;
    const char *bare_filename;
    const char *bare_caller;
    const char *bare_value;
    const char *bare_definition;
    const char *broad_source;
    const char *broad_filename;
    const char *broad_caller;
    const char *broad_value;
    const char *broad_node_kind;
    int broad_node_count;
    const char *broad_callee;
    int broad_call_count;
} MatrixCase;

static int qn_has_terminal_name(const char *qn, const char *name) {
    if (!qn || !name)
        return 0;
    size_t qn_len = strlen(qn);
    size_t name_len = strlen(name);
    if (name_len > qn_len || strcmp(qn + qn_len - name_len, name) != 0)
        return 0;
    if (name_len == qn_len)
        return 1;
    char separator = qn[qn_len - name_len - 1];
    return separator == '.' || separator == ':' || separator == '/' || separator == '#';
}

static const char *short_call_name(const char *name) {
    if (!name)
        return NULL;
    const char *short_name = name;
    for (const char *p = name; *p; p++) {
        if (*p == '.' || *p == ':' || *p == '/' || *p == '#')
            short_name = p + 1;
    }
    return short_name;
}

static int definition_count(const CBMFileResult *result, const char *name) {
    int count = 0;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        int callable = definition->label && (strcmp(definition->label, "Function") == 0 ||
                                             strcmp(definition->label, "Method") == 0);
        if (callable && definition->name && strcmp(definition->name, name) == 0)
            count++;
    }
    return count;
}

static int call_count(const CBMFileResult *result, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        const char *callee = short_call_name(call->callee_name);
        if (callee && strcmp(callee, target) == 0 &&
            (!caller || qn_has_terminal_name(call->enclosing_func_qn, caller))) {
            count++;
        }
    }
    return count;
}

static int usage_within_call_count(const CBMFileResult *result, const char *caller,
                                   const char *value, const char *callee) {
    int count = 0;
    for (int u = 0; u < result->usages.count; u++) {
        const CBMUsage *usage = &result->usages.items[u];
        if (!usage->ref_name || strcmp(usage->ref_name, value) != 0 ||
            !qn_has_terminal_name(usage->enclosing_func_qn, caller) ||
            usage->site_end_byte <= usage->site_start_byte) {
            continue;
        }
        for (int c = 0; c < result->calls.count; c++) {
            const CBMCall *call = &result->calls.items[c];
            const char *call_name = short_call_name(call->callee_name);
            if (!call_name || strcmp(call_name, callee) != 0 ||
                !qn_has_terminal_name(call->enclosing_func_qn, caller) ||
                call->source_origin != usage->source_origin ||
                call->site_end_byte <= call->site_start_byte) {
                continue;
            }
            if (usage->site_start_byte >= call->site_start_byte &&
                usage->site_end_byte <= call->site_end_byte) {
                count++;
            }
        }
    }
    return count;
}

static int usage_count(const CBMFileResult *result, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->ref_name && strcmp(usage->ref_name, target) == 0 &&
            (!caller || qn_has_terminal_name(usage->enclosing_func_qn, caller))) {
            count++;
        }
    }
    return count;
}

static int usage_kind_count(const CBMFileResult *result, const char *caller, const char *target,
                            CBMUsageKind kind) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == kind && usage->ref_name && strcmp(usage->ref_name, target) == 0 &&
            (!caller || qn_has_terminal_name(usage->enclosing_func_qn, caller))) {
            count++;
        }
    }
    return count;
}

static int local_shadow_usage_count(const CBMFileResult *result, const char *caller,
                                    const char *target) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->semantic_reference_blocked &&
            usage->semantic_reference_local_shadow && usage->ref_name &&
            strcmp(usage->ref_name, target) == 0 &&
            (!caller || qn_has_terminal_name(usage->enclosing_func_qn, caller))) {
            count++;
        }
    }
    return count;
}

static int type_list_contains(const char *const *types, const char *expected) {
    if (!types || !expected)
        return 0;
    for (int i = 0; types[i]; i++) {
        if (strcmp(types[i], expected) == 0)
            return 1;
    }
    return 0;
}

static int ast_node_match_count(TSNode node, const char *node_kind, const char *node_text,
                                const char *source) {
    int count = 0;
    if (strcmp(ts_node_type(node), node_kind) == 0) {
        if (!node_text) {
            count = 1;
        } else {
            uint32_t start = ts_node_start_byte(node);
            uint32_t end = ts_node_end_byte(node);
            size_t expected_length = strlen(node_text);
            if (end >= start && (size_t)(end - start) == expected_length &&
                memcmp(source + start, node_text, expected_length) == 0) {
                count = 1;
            }
        }
    }
    uint32_t child_count = ts_node_child_count(node);
    for (uint32_t i = 0; i < child_count; i++)
        count += ast_node_match_count(ts_node_child(node, i), node_kind, node_text, source);
    return count;
}

static int check_ast_contract(const MatrixCase *matrix_case, const char *source, const char *phase,
                              const char *primary_kind, const char *primary_text,
                              int primary_expected, const char *primary_invariant,
                              const char *secondary_kind, const char *secondary_text,
                              int secondary_expected, const char *secondary_invariant) {
    int failures = 0;
    const TSLanguage *language = cbm_ts_language(matrix_case->language);
    TSParser *parser = ts_parser_new();
    if (!language || !parser || !ts_parser_set_language(parser, language)) {
        fprintf(stderr,
                "  [call-matrix-a] lang=%s phase=%s invariant=ast_parser_ready expected=1 "
                "actual=0\n",
                matrix_case->tag, phase);
        if (parser)
            ts_parser_delete(parser);
        return 1;
    }

    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)strlen(source));
    if (!tree) {
        fprintf(stderr,
                "  [call-matrix-a] lang=%s phase=%s invariant=ast_tree expected=non-null "
                "actual=null\n",
                matrix_case->tag, phase);
        ts_parser_delete(parser);
        return 1;
    }

    TSNode root = ts_tree_root_node(tree);
    if (ts_node_has_error(root)) {
        fprintf(stderr,
                "  [call-matrix-a] lang=%s phase=%s invariant=ast_clean_parse expected=1 "
                "actual=0\n",
                matrix_case->tag, phase);
        failures++;
    }
    int primary_actual = ast_node_match_count(root, primary_kind, primary_text, source);
    if (primary_actual != primary_expected) {
        fprintf(stderr,
                "  [call-matrix-a] lang=%s phase=%s invariant=%s kind=%s text=%s expected=%d "
                "actual=%d\n",
                matrix_case->tag, phase, primary_invariant, primary_kind,
                primary_text ? primary_text : "*", primary_expected, primary_actual);
        failures++;
    }
    if (secondary_kind) {
        int secondary_actual = ast_node_match_count(root, secondary_kind, secondary_text, source);
        if (secondary_actual != secondary_expected) {
            fprintf(stderr,
                    "  [call-matrix-a] lang=%s phase=%s invariant=%s kind=%s text=%s expected=%d "
                    "actual=%d\n",
                    matrix_case->tag, phase, secondary_invariant, secondary_kind,
                    secondary_text ? secondary_text : "*", secondary_expected, secondary_actual);
            failures++;
        }
    }
    ts_tree_delete(tree);
    ts_parser_delete(parser);
    return failures;
}

#define CHECK_EXACT(tag, invariant, expression, expected)                                     \
    do {                                                                                      \
        int actual = (expression);                                                            \
        if (actual != (expected)) {                                                           \
            fprintf(stderr, "  [call-matrix-a] lang=%s invariant=%s expected=%d actual=%d\n", \
                    (tag), (invariant), (expected), actual);                                  \
            failures++;                                                                       \
        }                                                                                     \
    } while (0)

static CBMFileResult *extract_matrix_source(const MatrixCase *matrix_case, const char *source,
                                            const char *filename, const char *phase) {
    CBMFileResult *result = cbm_extract_file(source, (int)strlen(source), matrix_case->language,
                                             "repro", filename, 0, NULL, NULL);
    if (!result) {
        fprintf(stderr, "  [call-matrix-a] lang=%s phase=%s invariant=extract_result_null\n",
                matrix_case->tag, phase);
    }
    return result;
}

static int check_reference_fixture(const MatrixCase *matrix_case, const char *source,
                                   const char *filename, const char *caller, const char *value,
                                   const char *outer_callee, const char *definition,
                                   const char *outer_definition, const char *phase,
                                   const char *usage_invariant, const char *not_call_invariant,
                                   const char *shape_callee, int expected_call_count,
                                   const char *total_calls_invariant) {
    CBMFileResult *result = extract_matrix_source(matrix_case, source, filename, phase);
    if (!result)
        return 1;

    int failures = 0;
    if (result->has_error || result->parse_incomplete) {
        fprintf(stderr,
                "  [call-matrix-a] lang=%s phase=%s invariant=valid_fixture expected=clean\n",
                matrix_case->tag, phase);
        failures++;
    }
    if (definition)
        CHECK_EXACT(matrix_case->tag, "native_fixture_definition",
                    definition_count(result, definition), 1);
    if (outer_definition)
        CHECK_EXACT(matrix_case->tag, "native_outer_definition",
                    definition_count(result, outer_definition), 1);
    if (outer_callee) {
        CHECK_EXACT(matrix_case->tag, "native_outer_call", call_count(result, caller, outer_callee),
                    1);
        CHECK_EXACT(matrix_case->tag, "native_outer_callee_not_usage",
                    usage_count(result, caller, outer_callee), 0);
    }
    CHECK_EXACT(matrix_case->tag, usage_invariant, usage_count(result, caller, value), 1);
    CHECK_EXACT(matrix_case->tag, "reference_is_ordinary_usage",
                usage_kind_count(result, caller, value, CBM_USAGE_VALUE), 1);
    CHECK_EXACT(matrix_case->tag, "reference_not_call_reference",
                usage_kind_count(result, caller, value, CBM_USAGE_CALL_REFERENCE), 0);
    CHECK_EXACT(matrix_case->tag, not_call_invariant, call_count(result, caller, value), 0);
    CHECK_EXACT(matrix_case->tag, total_calls_invariant, result->calls.count, expected_call_count);
    if (shape_callee)
        CHECK_EXACT(matrix_case->tag, "broad_expected_callee",
                    call_count(result, caller, shape_callee), expected_call_count);
    cbm_free_result(result);
    return failures;
}

static int check_semantic_case(const MatrixCase *matrix_case) {
    int failures = check_ast_contract(matrix_case, matrix_case->source, "roles-ast",
                                      matrix_case->call_kind, NULL, matrix_case->scope_node_count,
                                      "semantic_primary_call_node_count", NULL, NULL, 0, NULL);
    CBMFileResult *result =
        extract_matrix_source(matrix_case, matrix_case->source, matrix_case->filename, "roles");
    if (!result)
        return failures + 1;

    if (result->has_error || result->parse_incomplete) {
        fprintf(stderr,
                "  [call-matrix-a] lang=%s phase=roles invariant=valid_fixture expected=clean\n",
                matrix_case->tag);
        failures++;
    }

    CHECK_EXACT(matrix_case->tag, "handler_definition", definition_count(result, "handler"), 1);
    CHECK_EXACT(matrix_case->tag, "accept_definition", definition_count(result, "accept"), 1);
    CHECK_EXACT(matrix_case->tag, "argument_definition", definition_count(result, "argument"), 1);
    CHECK_EXACT(matrix_case->tag, "direct_definition", definition_count(result, "direct"), 1);
    CHECK_EXACT(matrix_case->tag, "bare_definition", definition_count(result, "bare"), 1);
    CHECK_EXACT(matrix_case->tag, "registrar_call", call_count(result, "argument", "accept"), 1);
    CHECK_EXACT(matrix_case->tag, "direct_call", call_count(result, "direct", "handler"), 1);
    CHECK_EXACT(matrix_case->tag, "inside_value_usage",
                usage_count(result, "argument", matrix_case->value_name), 1);
    CHECK_EXACT(matrix_case->tag, "inside_value_site_owned_by_registrar_call",
                usage_within_call_count(result, "argument", matrix_case->value_name, "accept"), 1);
    CHECK_EXACT(matrix_case->tag, "inside_value_is_ordinary_usage",
                usage_kind_count(result, "argument", matrix_case->value_name, CBM_USAGE_VALUE), 1);
    CHECK_EXACT(
        matrix_case->tag, "inside_value_not_call_reference",
        usage_kind_count(result, "argument", matrix_case->value_name, CBM_USAGE_CALL_REFERENCE), 0);
    if (matrix_case->expect_lexical_local_shadow) {
        CHECK_EXACT(matrix_case->tag, "inside_value_blocks_callable_promotion",
                    local_shadow_usage_count(result, "argument", matrix_case->value_name), 1);
    }
    CHECK_EXACT(matrix_case->tag, "bare_value_usage",
                usage_count(result, "bare", matrix_case->value_name), 1);
    CHECK_EXACT(matrix_case->tag, "bare_value_is_ordinary_usage",
                usage_kind_count(result, "bare", matrix_case->value_name, CBM_USAGE_VALUE), 1);
    CHECK_EXACT(matrix_case->tag, "bare_value_not_call_reference",
                usage_kind_count(result, "bare", matrix_case->value_name, CBM_USAGE_CALL_REFERENCE),
                0);
    CHECK_EXACT(matrix_case->tag, "direct_callee_not_usage",
                usage_count(result, "direct", "handler"), 0);
    CHECK_EXACT(matrix_case->tag, "argument_not_fabricated_call",
                call_count(result, "argument", "handler"), 0);
    CHECK_EXACT(matrix_case->tag, "semantic_total_calls", result->calls.count,
                matrix_case->scope_call_count);
    cbm_free_result(result);

    if (matrix_case->broad_source) {
        failures += check_ast_contract(
            matrix_case, matrix_case->broad_source, "broad-node", matrix_case->broad_node_kind,
            NULL, matrix_case->broad_node_count, "broad_ast_node_count", NULL, NULL, 0, NULL);
        failures += check_reference_fixture(
            matrix_case, matrix_case->broad_source, matrix_case->broad_filename,
            matrix_case->broad_caller, matrix_case->broad_value, NULL, NULL, NULL, "broad-node",
            "broad_reference_usage", "broad_reference_not_call", matrix_case->broad_callee,
            matrix_case->broad_call_count, "broad_total_calls");
    }
    return failures;
}

static int check_native_scope_case(const MatrixCase *matrix_case) {
    int failures = 0;
    const CBMLangSpec *spec = cbm_lang_spec(matrix_case->language);
    if (!spec || spec->language != matrix_case->language) {
        fprintf(stderr, "  [call-matrix-a] lang=%s invariant=registered_spec_missing\n",
                matrix_case->tag);
        failures++;
    } else if (!type_list_contains(spec->call_node_types, matrix_case->call_kind)) {
        fprintf(stderr,
                "  [call-matrix-a] lang=%s invariant=native_call_kind_missing expected=%s\n",
                matrix_case->tag, matrix_case->call_kind);
        failures++;
    }
    if (!matrix_case->reason || matrix_case->reason[0] == '\0') {
        fprintf(stderr, "  [call-matrix-a] lang=%s invariant=applicability_reason_missing\n",
                matrix_case->tag);
        failures++;
    }
    if (!matrix_case->call_kind || !matrix_case->reference_node_kind ||
        !matrix_case->reference_node_text) {
        fprintf(stderr, "  [call-matrix-a] lang=%s invariant=native_ast_contract_missing\n",
                matrix_case->tag);
        return failures + 1;
    }
    failures += check_ast_contract(
        matrix_case, matrix_case->source, "native-argument", matrix_case->call_kind, NULL,
        matrix_case->scope_node_count, "native_call_node_count", matrix_case->reference_node_kind,
        matrix_case->reference_node_text, matrix_case->scope_reference_node_count,
        "native_argument_reference_shape");
    failures += check_reference_fixture(
        matrix_case, matrix_case->source, matrix_case->filename, matrix_case->scope_caller,
        matrix_case->scope_value, matrix_case->outer_callee, matrix_case->scope_definition,
        matrix_case->outer_definition, "native-argument", "native_argument_usage",
        "native_argument_not_call", NULL, matrix_case->scope_call_count,
        "native_argument_total_calls");

    if (matrix_case->native_fixture_mode == NATIVE_DOMAIN_EXCEPTION)
        return failures;

    if (!matrix_case->bare_source || !matrix_case->bare_filename || !matrix_case->bare_value) {
        fprintf(stderr, "  [call-matrix-a] lang=%s invariant=native_bare_fixture_missing\n",
                matrix_case->tag);
        return failures + 1;
    }
    failures +=
        check_ast_contract(matrix_case, matrix_case->bare_source, "native-bare",
                           matrix_case->call_kind, NULL, 0, "native_bare_call_node_count",
                           matrix_case->reference_node_kind, matrix_case->reference_node_text,
                           matrix_case->bare_reference_node_count, "native_bare_reference_shape");
    failures += check_reference_fixture(
        matrix_case, matrix_case->bare_source, matrix_case->bare_filename, matrix_case->bare_caller,
        matrix_case->bare_value, NULL, matrix_case->bare_definition, NULL, "native-bare",
        "native_bare_usage", "native_bare_not_call", NULL, matrix_case->bare_call_count,
        "native_bare_total_calls");
    return failures;
}

static int check_matrix_case(const MatrixCase *matrix_case) {
    return matrix_case->mode == MATRIX_SEMANTIC ? check_semantic_case(matrix_case)
                                                : check_native_scope_case(matrix_case);
}

#define SEMANTIC_CASE(tag_, lang_, file_, source_, value_, kind_, node_count_, call_count_) \
    static const MatrixCase CASE_##tag_ = {                                                 \
        .tag = #tag_,                                                                       \
        .language = CBM_LANG_##lang_,                                                       \
        .filename = file_,                                                                  \
        .source = source_,                                                                  \
        .mode = MATRIX_SEMANTIC,                                                            \
        .expect_lexical_local_shadow = 1,                                                   \
        .value_name = value_,                                                               \
        .call_kind = kind_,                                                                 \
        .scope_node_count = node_count_,                                                    \
        .scope_call_count = call_count_,                                                    \
    }

#define SEMANTIC_BROAD_CASE(tag_, lang_, file_, source_, value_, kind_, node_count_, call_count_, \
                            broad_src_, broad_file_, broad_caller_, broad_value_, broad_kind_,    \
                            broad_node_count_, broad_callee_, broad_call_count_)                  \
    static const MatrixCase CASE_##tag_ = {                                                       \
        .tag = #tag_,                                                                             \
        .language = CBM_LANG_##lang_,                                                             \
        .filename = file_,                                                                        \
        .source = source_,                                                                        \
        .mode = MATRIX_SEMANTIC,                                                                  \
        .expect_lexical_local_shadow = 1,                                                         \
        .value_name = value_,                                                                     \
        .call_kind = kind_,                                                                       \
        .scope_node_count = node_count_,                                                          \
        .scope_call_count = call_count_,                                                          \
        .broad_source = broad_src_,                                                               \
        .broad_filename = broad_file_,                                                            \
        .broad_caller = broad_caller_,                                                            \
        .broad_value = broad_value_,                                                              \
        .broad_node_kind = broad_kind_,                                                           \
        .broad_node_count = broad_node_count_,                                                    \
        .broad_callee = broad_callee_,                                                            \
        .broad_call_count = broad_call_count_,                                                    \
    }

#define NATIVE_SCOPE_CASE(tag_, lang_, file_, source_, kind_, reason_, callee_, caller_, value_,  \
                          definition_, outer_definition_, bare_source_, bare_file_, bare_caller_, \
                          bare_value_, bare_definition_, node_count_, reference_kind_,            \
                          reference_text_, scope_reference_count_, bare_reference_count_,         \
                          scope_call_count_, bare_call_count_)                                    \
    static const MatrixCase CASE_##tag_ = {                                                       \
        .tag = #tag_,                                                                             \
        .language = CBM_LANG_##lang_,                                                             \
        .filename = file_,                                                                        \
        .source = source_,                                                                        \
        .mode = MATRIX_NATIVE_SCOPE,                                                              \
        .native_fixture_mode = NATIVE_PAIRED,                                                     \
        .call_kind = kind_,                                                                       \
        .scope_node_count = node_count_,                                                          \
        .scope_call_count = scope_call_count_,                                                    \
        .reference_node_kind = reference_kind_,                                                   \
        .reference_node_text = reference_text_,                                                   \
        .scope_reference_node_count = scope_reference_count_,                                     \
        .bare_reference_node_count = bare_reference_count_,                                       \
        .bare_call_count = bare_call_count_,                                                      \
        .reason = reason_,                                                                        \
        .outer_callee = callee_,                                                                  \
        .scope_caller = caller_,                                                                  \
        .scope_value = value_,                                                                    \
        .scope_definition = definition_,                                                          \
        .outer_definition = outer_definition_,                                                    \
        .bare_source = bare_source_,                                                              \
        .bare_filename = bare_file_,                                                              \
        .bare_caller = bare_caller_,                                                              \
        .bare_value = bare_value_,                                                                \
        .bare_definition = bare_definition_,                                                      \
    }

#define NATIVE_DOMAIN_CASE(tag_, lang_, file_, source_, kind_, reason_, callee_, value_,    \
                           node_count_, reference_kind_, reference_text_, reference_count_, \
                           call_count_)                                                     \
    static const MatrixCase CASE_##tag_ = {                                                 \
        .tag = #tag_,                                                                       \
        .language = CBM_LANG_##lang_,                                                       \
        .filename = file_,                                                                  \
        .source = source_,                                                                  \
        .mode = MATRIX_NATIVE_SCOPE,                                                        \
        .native_fixture_mode = NATIVE_DOMAIN_EXCEPTION,                                     \
        .call_kind = kind_,                                                                 \
        .scope_node_count = node_count_,                                                    \
        .scope_call_count = call_count_,                                                    \
        .reference_node_kind = reference_kind_,                                             \
        .reference_node_text = reference_text_,                                             \
        .scope_reference_node_count = reference_count_,                                     \
        .reason = reason_,                                                                  \
        .outer_callee = callee_,                                                            \
        .scope_value = value_,                                                              \
    }

SEMANTIC_CASE(go, GO, "main.go",
              "package sample\n"
              "func handler() int { return 0 }\n"
              "func accept(value int) int { return value }\n"
              "func argument(handler int) int { return accept(handler) }\n"
              "func direct() int { return handler() }\n"
              "func bare(handler int) int { return handler }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(python, PYTHON, "main.py",
              "def handler():\n    return 0\n"
              "def accept(value):\n    return value\n"
              "def argument(handler):\n    return accept(handler)\n"
              "def direct():\n    return handler()\n"
              "def bare(handler):\n    return handler\n",
              "handler", "call", 2, 2);

SEMANTIC_CASE(javascript, JAVASCRIPT, "main.js",
              "function handler() { return 0; }\n"
              "function accept(value) { return value; }\n"
              "function argument(handler) { return accept(handler); }\n"
              "function direct() { return handler(); }\n"
              "function bare(handler) { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(typescript, TYPESCRIPT, "main.ts",
              "function handler(): number { return 0; }\n"
              "function accept(value: number): number { return value; }\n"
              "function argument(handler: number): number { return accept(handler); }\n"
              "function direct(): number { return handler(); }\n"
              "function bare(handler: number): number { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(tsx, TSX, "main.tsx",
              "function handler(): number { return 0; }\n"
              "function accept(value: number): number { return value; }\n"
              "function argument(handler: number): number { return accept(handler); }\n"
              "function direct(): number { return handler(); }\n"
              "function bare(handler: number): number { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(rust, RUST, "main.rs",
              "fn handler() -> i32 { 0 }\n"
              "fn accept(value: i32) -> i32 { value }\n"
              "fn argument(handler: i32) -> i32 { accept(handler) }\n"
              "fn direct() -> i32 { handler() }\n"
              "fn bare(handler: i32) -> i32 { handler }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(java, JAVA, "Sample.java",
              "class Sample {\n"
              "  static int handler() { return 0; }\n"
              "  static int accept(int value) { return value; }\n"
              "  static int argument(int handler) { return accept(handler); }\n"
              "  static int direct() { return handler(); }\n"
              "  static int bare(int handler) { return handler; }\n"
              "}\n",
              "handler", "method_invocation", 2, 2);

SEMANTIC_CASE(cpp, CPP, "main.cpp",
              "int handler() { return 0; }\n"
              "int accept(int value) { return value; }\n"
              "int argument(int handler) { return accept(handler); }\n"
              "int direct() { return handler(); }\n"
              "int bare(int handler) { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(csharp, CSHARP, "Sample.cs",
              "class Sample {\n"
              "  static int handler() { return 0; }\n"
              "  static int accept(int value) { return value; }\n"
              "  static int argument(int handler) { return accept(handler); }\n"
              "  static int direct() { return handler(); }\n"
              "  static int bare(int handler) { return handler; }\n"
              "}\n",
              "handler", "invocation_expression", 2, 2);

SEMANTIC_CASE(php, PHP, "main.php",
              "<?php\n"
              "function handler() { return 0; }\n"
              "function accept($value) { return $value; }\n"
              "function argument($handler) { return accept($handler); }\n"
              "function direct() { return handler(); }\n"
              "function bare($handler) { return $handler; }\n",
              "$handler", "function_call_expression", 2, 2);

SEMANTIC_CASE(lua, LUA, "main.lua",
              "function handler() return 0 end\n"
              "function accept(value) return value end\n"
              "function argument(handler) return accept(handler) end\n"
              "function direct() return handler() end\n"
              "function bare(handler) return handler end\n",
              "handler", "function_call", 2, 2);

SEMANTIC_BROAD_CASE(scala, SCALA, "Main.scala",
                    "object Sample {\n"
                    "  def handler(): Int = 0\n"
                    "  def accept(value: Int): Int = value\n"
                    "  def argument(handler: Int): Int = accept(handler)\n"
                    "  def direct(): Int = handler()\n"
                    "  def bare(handler: Int): Int = handler\n"
                    "}\n",
                    "handler", "call_expression", 2, 2,
                    "object Scope { def scope(watched: Int): Int = watched + 1 }\n", "Scope.scala",
                    "scope", "watched", "infix_expression", 1, "+", 1);

SEMANTIC_CASE(kotlin, KOTLIN, "Main.kt",
              "fun handler(): Int = 0\n"
              "fun accept(value: Int): Int = value\n"
              "fun argument(handler: Int): Int = accept(handler)\n"
              "fun direct(): Int = handler()\n"
              "fun bare(handler: Int): Int = handler\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(ruby, RUBY, "main.rb",
              "def handler; 0; end\n"
              "def accept(value); value; end\n"
              "def argument(handler); accept(handler); end\n"
              "def direct; handler(); end\n"
              "def bare(handler); handler; end\n",
              "handler", "call", 2, 2);

SEMANTIC_CASE(c, C, "main.c",
              "int handler(void) { return 0; }\n"
              "int accept(int value) { return value; }\n"
              "int argument(int handler) { return accept(handler); }\n"
              "int direct(void) { return handler(); }\n"
              "int bare(int handler) { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(bash, BASH, "main.sh",
              "handler() { return 0; }\n"
              "accept() { printf '%s' \"$1\"; }\n"
              "argument() { local handler=$1; accept \"$handler\"; }\n"
              "direct() { handler; }\n"
              "bare() { local handler=$1; printf '%s' \"$handler\"; }\n",
              "handler", "command", 5, 4);

SEMANTIC_CASE(zig, ZIG, "main.zig",
              "fn handler() i32 { return 0; }\n"
              "fn accept(value: i32) i32 { return value; }\n"
              "fn argument(handler: i32) i32 { return accept(handler); }\n"
              "fn direct() i32 { return handler(); }\n"
              "fn bare(handler: i32) i32 { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(elixir, ELIXIR, "main.ex",
              "defmodule Sample do\n"
              "  def handler(), do: 0\n"
              "  def accept(value), do: value\n"
              "  def argument(handler), do: accept(handler)\n"
              "  def direct(), do: handler()\n"
              "  def bare(handler), do: handler\n"
              "end\n",
              "handler", "call", 13, 2);

SEMANTIC_BROAD_CASE(haskell, HASKELL, "Main.hs",
                    "module Main where\n"
                    "handler :: Int -> Int\nhandler value = value\n"
                    "accept :: Int -> Int\naccept value = value\n"
                    "argument :: Int -> Int\nargument handler = accept handler\n"
                    "direct :: Int -> Int\ndirect value = handler value\n"
                    "bare :: Int -> Int\nbare handler = handler\n",
                    "handler", "apply", 2, 2, "module Scope where\nscope watched = watched + 1\n",
                    "Scope.hs", "scope", "watched", "infix", 1, "+", 1);

SEMANTIC_BROAD_CASE(ocaml, OCAML, "main.ml",
                    "let handler value = value\n"
                    "let accept value = value\n"
                    "let argument handler = accept handler\n"
                    "let direct value = handler value\n"
                    "let bare handler = handler\n",
                    "handler", "application_expression", 2, 2, "let scope watched = watched + 1\n",
                    "scope.ml", "scope", "watched", "infix_expression", 1, "+", 1);

SEMANTIC_CASE(objc, OBJC, "main.m",
              "int handler(void) { return 0; }\n"
              "int accept(int value) { return value; }\n"
              "int argument(int handler) { return accept(handler); }\n"
              "int direct(void) { return handler(); }\n"
              "int bare(int handler) { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_BROAD_CASE(swift, SWIFT, "main.swift",
                    "func handler() -> Int { return 0 }\n"
                    "func accept(_ value: Int) -> Int { return value }\n"
                    "func argument(_ handler: Int) -> Int { return accept(handler) }\n"
                    "func direct() -> Int { return handler() }\n"
                    "func bare(_ handler: Int) -> Int { return handler }\n",
                    "handler", "call_expression", 2, 2,
                    "struct Holder { let value: Int }\n"
                    "func scope(_ holder: Holder) -> Int { return holder.value }\n",
                    "Scope.swift", "scope", "holder", "navigation_expression", 1, NULL, 0);

SEMANTIC_CASE(dart, DART, "main.dart",
              "int handler() => 0;\n"
              "int accept(int value) => value;\n"
              "int argument(int handler) => accept(handler);\n"
              "int direct() => handler();\n"
              "int bare(int handler) => handler;\n",
              "handler", "selector", 2, 2);

SEMANTIC_CASE(perl, PERL, "main.pl",
              "sub handler { return 0; }\n"
              "sub accept { my ($value) = @_; return $value; }\n"
              "sub argument { my ($handler) = @_; return accept($handler); }\n"
              "sub direct { return handler(); }\n"
              "sub bare { my ($handler) = @_; return $handler; }\n",
              "$handler", "function_call_expression", 2, 2);

SEMANTIC_CASE(groovy, GROOVY, "Main.groovy",
              "int handler() { return 0 }\n"
              "int accept(int value) { return value }\n"
              "int argument(int handler) { return accept(handler) }\n"
              "int direct() { return handler() }\n"
              "int bare(int handler) { return handler }\n",
              "handler", "function_call", 2, 2);

SEMANTIC_CASE(erlang, ERLANG, "main.erl",
              "-module(main).\n"
              "handler() -> 0.\n"
              "accept(Value) -> Value.\n"
              "argument(Handler) -> accept(Handler).\n"
              "direct() -> handler().\n"
              "bare(Handler) -> Handler.\n",
              "Handler", "call", 2, 2);

SEMANTIC_CASE(r, R, "main.R",
              "handler <- function() { 0 }\n"
              "accept <- function(value) { value }\n"
              "argument <- function(handler) { accept(handler) }\n"
              "direct <- function() { handler() }\n"
              "bare <- function(handler) { handler }\n",
              "handler", "call", 2, 2);

NATIVE_DOMAIN_CASE(css, CSS, "main.css",
                   ":root { --watched: 0; }\n.sample { color: var(--watched); }\n",
                   "call_expression",
                   "CSS custom properties can only be referenced through var(), so no honest "
                   "bare equivalent exists; this checks exact AST/call/usage behavior without "
                   "claiming vocabulary isolation",
                   "var", "--watched", 1, "plain_value", "--watched", 1, 1);

SEMANTIC_CASE(scss, SCSS, "main.scss",
              "@function handler($unused) {\n  @return 0;\n}\n"
              "@function accept($value) {\n  @return $value;\n}\n"
              "@function argument($handler) {\n  @return accept($handler);\n}\n"
              "@function direct($unused) {\n  @return handler(0);\n}\n"
              "@function bare($handler) {\n  @return $handler;\n}\n",
              "$handler", "call_expression", 2, 2);

NATIVE_SCOPE_CASE(hcl, HCL, "main.tf", "locals {\n  watched = [1]\n  result = length(watched)\n}\n",
                  "function_call",
                  "HCL exposes built-in function applications but cannot declare user routines, "
                  "so local-expression references use exact global counts",
                  "length", NULL, "watched", NULL, NULL,
                  "locals {\n  watched = [1]\n  result = watched\n}\n", "bare.tf", NULL, "watched",
                  NULL, 1, "variable_expr", "watched", 1, 1, 1, 0);

/* This known-valid PL/pgSQL shape exercises the grammar-real `invocation` node.
 * The registered `function_call` spelling remains covered as a stale manifest
 * defect; SQL `command` is separately classified NOT_A_CALL. */
NATIVE_SCOPE_CASE(sql, SQL, "argument.sql",
                  "CREATE FUNCTION accept(value integer) RETURNS integer AS $$\n"
                  "BEGIN\n  RETURN value;\nEND;\n$$ LANGUAGE plpgsql;\n"
                  "CREATE FUNCTION argument(handler integer) RETURNS integer AS $$\n"
                  "BEGIN\n  RETURN accept(handler);\nEND;\n$$ LANGUAGE plpgsql;\n",
                  "invocation",
                  "SQL create_function supplies an enclosing callable, so definitions and "
                  "caller-qualified references are exact",
                  "accept", "argument", "handler", "argument", "accept",
                  "CREATE FUNCTION bare(handler integer) RETURNS integer AS $$\n"
                  "BEGIN\n  RETURN handler;\nEND;\n$$ LANGUAGE plpgsql;\n",
                  "bare.sql", "bare", "handler", "bare", 1, "identifier", "handler", 2, 2, 1, 0);

SEMANTIC_CASE(clojure, CLOJURE, "main.clj",
              "(defn handler [] 0)\n"
              "(defn accept [value] value)\n"
              "(defn argument [handler] (accept handler))\n"
              "(defn direct [] (handler))\n"
              "(defn bare [handler] handler)\n",
              "handler", "list_lit", 7, 2);

SEMANTIC_BROAD_CASE(fsharp, FSHARP, "Main.fs",
                    "let handler () = 0\n"
                    "let accept value = value\n"
                    "let argument handler = accept handler\n"
                    "let direct () = handler ()\n"
                    "let bare handler = handler\n",
                    "handler", "application_expression", 2, 2,
                    "type Holder = { value: int }\n"
                    "let scope (watched: Holder) = (watched).value\n",
                    "Scope.fs", "scope", "watched", "dot_expression", 1, NULL, 0);

SEMANTIC_CASE(julia, JULIA, "main.jl",
              "function handler()\n  return 0\nend\n"
              "function accept(value)\n  return value\nend\n"
              "function argument(handler)\n  return accept(handler)\nend\n"
              "function direct()\n  return handler()\nend\n"
              "function bare(handler)\n  return handler\nend\n",
              "handler", "call_expression", 7, 2);

SEMANTIC_CASE(vimscript, VIMSCRIPT, "main.vim",
              "function! handler()\n  return 0\nendfunction\n"
              "function! accept(value)\n  return a:value\nendfunction\n"
              "function! argument(handler)\n  return accept(a:handler)\nendfunction\n"
              "function! direct()\n  return handler()\nendfunction\n"
              "function! bare(handler)\n  return a:handler\nendfunction\n",
              "a:handler", "call_expression", 2, 2);

SEMANTIC_CASE(nix, NIX, "main.nix",
              "let\n"
              "  handler = value: value;\n"
              "  accept = value: value;\n"
              "  argument = handler: accept handler;\n"
              "  direct = value: handler value;\n"
              "  bare = handler: handler;\n"
              "in argument\n",
              "handler", "apply_expression", 2, 2);

SEMANTIC_CASE(commonlisp, COMMONLISP, "main.lisp",
              "(defun handler () 0)\n"
              "(defun accept (value) value)\n"
              "(defun argument (handler) (accept handler))\n"
              "(defun direct () (handler))\n"
              "(defun bare (handler) handler)\n",
              "handler", "list_lit", 12, 2);

SEMANTIC_CASE(elm, ELM, "Main.elm",
              "module Main exposing (..)\n"
              "handler value = value\n"
              "accept value = value\n"
              "argument handler = accept handler\n"
              "direct value = handler value\n"
              "bare handler = handler\n",
              "handler", "function_call_expr", 2, 2);

SEMANTIC_CASE(fortran, FORTRAN, "main.f90",
              "module sample\ncontains\n"
              "  integer function handler()\n    handler = 0\n  end function handler\n"
              "  integer function accept(value)\n    integer :: value\n    accept = value\n"
              "  end function accept\n"
              "  integer function argument(handler)\n    integer :: handler\n"
              "    argument = accept(handler)\n  end function argument\n"
              "  integer function direct()\n    direct = handler()\n  end function direct\n"
              "  integer function bare(handler)\n    integer :: handler\n"
              "    bare = handler\n  end function bare\nend module sample\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(cuda, CUDA, "main.cu",
              "__device__ int handler() { return 0; }\n"
              "__device__ int accept(int value) { return value; }\n"
              "__device__ int argument(int handler) { return accept(handler); }\n"
              "__device__ int direct() { return handler(); }\n"
              "__device__ int bare(int handler) { return handler; }\n",
              "handler", "call_expression", 2, 2);

NATIVE_SCOPE_CASE(cobol, COBOL, "main.cob",
                  "       IDENTIFICATION DIVISION.\n"
                  "       PROGRAM-ID. argument.\n"
                  "       DATA DIVISION.\n"
                  "       WORKING-STORAGE SECTION.\n"
                  "       01 handler PIC 9 VALUE 1.\n"
                  "       PROCEDURE DIVISION.\n"
                  "           CALL \"accept\" USING handler.\n"
                  "           STOP RUN.\n"
                  "       END PROGRAM argument.\n"
                  "       IDENTIFICATION DIVISION.\n"
                  "       PROGRAM-ID. accept.\n"
                  "       PROCEDURE DIVISION.\n"
                  "           STOP RUN.\n"
                  "       END PROGRAM accept.\n",
                  "call_statement",
                  "COBOL programs are callable definitions, so CALL USING and MOVE references "
                  "are caller-qualified to exact program definitions",
                  "accept", "argument", "handler", "argument", "accept",
                  "       IDENTIFICATION DIVISION.\n"
                  "       PROGRAM-ID. bare.\n"
                  "       DATA DIVISION.\n"
                  "       WORKING-STORAGE SECTION.\n"
                  "       01 handler PIC 9 VALUE 1.\n"
                  "       01 result-value PIC 9 VALUE 0.\n"
                  "       PROCEDURE DIVISION.\n"
                  "           MOVE handler TO result-value.\n"
                  "           STOP RUN.\n"
                  "       END PROGRAM bare.\n",
                  "bare.cob", "bare", "handler", "bare", 1, "qualified_word", "handler", 1, 1, 1,
                  0);

SEMANTIC_CASE(verilog, VERILOG, "main.sv",
              "module sample;\n"
              "function integer handler(input integer value); handler = value; endfunction\n"
              "function integer accept(input integer value); accept = value; endfunction\n"
              "function integer argument(input integer handler); argument = accept(handler); "
              "endfunction\n"
              "function integer direct(); direct = handler(0); endfunction\n"
              "function integer bare(input integer handler); bare = handler; endfunction\n"
              "endmodule\n",
              "handler", "function_subroutine_call", 2, 2);

SEMANTIC_CASE(emacslisp, EMACSLISP, "main.el",
              "(defun handler () 0)\n"
              "(defun accept (value) value)\n"
              "(defun argument (handler) (accept handler))\n"
              "(defun direct () (handler))\n"
              "(defun bare (handler) handler)\n",
              "handler", "list", 7, 2);

NATIVE_SCOPE_CASE(makefile, MAKEFILE, "Makefile",
                  "handler := watched\nargument := $(info $(handler))\n", "function_call",
                  "Make functions are macro expansions attached to variables rather than user "
                  "callable bodies, so variable references use exact global counts",
                  "info", NULL, "handler", NULL, NULL, "handler := watched\nbare := $(handler)\n",
                  "Bare.mk", NULL, "handler", NULL, 1, "variable_reference", "$(handler)", 1, 1, 1,
                  0);

SEMANTIC_CASE(cmake, CMAKE, "CMakeLists.txt",
              "function(handler)\nendfunction()\n"
              "function(accept value)\nendfunction()\n"
              "function(argument handler)\n  accept(${handler})\nendfunction()\n"
              "function(direct)\n  handler()\nendfunction()\n"
              "function(bare handler)\n  set(result ${handler})\nendfunction()\n",
              "handler", "normal_command", 3, 3);

NATIVE_SCOPE_CASE(meson, MESON, "meson.build", "watched = 'value'\nmessage(watched)\n",
                  "normal_command",
                  "Meson normal commands call built-ins and provide no named user callable body, "
                  "so assignment references use exact global counts",
                  "message", NULL, "watched", NULL, NULL, "watched = 'value'\nbare = watched\n",
                  "bare.build", NULL, "watched", NULL, 1, "identifier", "watched", 2, 2, 1, 0);

SEMANTIC_CASE(glsl, GLSL, "main.glsl",
              "int handler() { return 0; }\n"
              "int accept(int value) { return value; }\n"
              "int argument(int handler) { return accept(handler); }\n"
              "int direct() { return handler(); }\n"
              "int bare(int handler) { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(matlab, MATLAB, "main.m",
              "function out = handler()\n  out = 0;\nend\n"
              "function out = accept(value)\n  out = value;\nend\n"
              "function out = argument(handler)\n  out = accept(handler);\nend\n"
              "function out = direct()\n  out = handler();\nend\n"
              "function out = bare(handler)\n  out = handler;\nend\n",
              "handler", "function_call", 2, 2);

SEMANTIC_CASE(lean, LEAN, "Main.lean",
              "def handler (value : Nat) : Nat := value\n"
              "def accept (value : Nat) : Nat := value\n"
              "def argument (handler : Nat) : Nat := accept handler\n"
              "def direct (value : Nat) : Nat := handler value\n"
              "def bare (handler : Nat) : Nat := handler\n",
              "handler", "apply", 2, 2);

SEMANTIC_CASE(form, FORM, "main.frm",
              "#procedure handler()\n  id out = 0;\n#endprocedure\n"
              "#procedure accept(value)\n  id out = value;\n#endprocedure\n"
              "#procedure argument(handler)\n  #call accept(handler)\n#endprocedure\n"
              "#procedure direct()\n  #call handler()\n#endprocedure\n"
              "#procedure bare(handler)\n  id out = handler;\n#endprocedure\n",
              "handler", "call_statement", 2, 2);

SEMANTIC_CASE(magma, MAGMA, "main.magma",
              "function handler()\n  return 0;\nend function;\n"
              "function accept(value)\n  return value;\nend function;\n"
              "function argument(handler)\n  return accept(handler);\nend function;\n"
              "function direct()\n  return handler();\nend function;\n"
              "function bare(handler)\n  return handler;\nend function;\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(wolfram, WOLFRAM, "main.wl",
              "handler[] := 0\n"
              "accept[value_] := value\n"
              "argument[handler_] := accept[handler]\n"
              "direct[] := handler[]\n"
              "bare[handler_] := handler\n",
              "handler", "apply", 7, 2);

SEMANTIC_CASE(solidity, SOLIDITY, "Main.sol",
              "pragma solidity ^0.8.0;\ncontract Sample {\n"
              "function handler() internal pure returns (uint) { return 0; }\n"
              "function accept(uint value) internal pure returns (uint) { return value; }\n"
              "function argument(uint handler) internal pure returns (uint) { return "
              "accept(handler); }\n"
              "function direct() internal pure returns (uint) { return handler(); }\n"
              "function bare(uint handler) internal pure returns (uint) { return handler; }\n}\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(typst, TYPST, "main.typ",
              "#let handler() = 0\n"
              "#let accept(value) = value\n"
              "#let argument(handler) = accept(handler)\n"
              "#let direct() = handler()\n"
              "#let bare(handler) = handler\n",
              "handler", "call", 7, 2);

SEMANTIC_CASE(gdscript, GDSCRIPT, "main.gd",
              "func handler():\n  return 0\n"
              "func accept(value):\n  return value\n"
              "func argument(handler):\n  return accept(handler)\n"
              "func direct():\n  return handler()\n"
              "func bare(handler):\n  return handler\n",
              "handler", "call", 2, 2);

SEMANTIC_CASE(gleam, GLEAM, "main.gleam",
              "fn handler() { 0 }\n"
              "fn accept(value: Int) { value }\n"
              "fn argument(handler: Int) { accept(handler) }\n"
              "fn direct() { handler() }\n"
              "fn bare(handler: Int) { handler }\n",
              "handler", "function_call", 2, 2);

SEMANTIC_CASE(powershell, POWERSHELL, "main.ps1",
              "function handler {\n  return 0\n}\n"
              "function accept {\n  param($value)\n  return $value\n}\n"
              "function argument {\n  param($handler)\n  accept $handler\n}\n"
              "function direct {\n  handler\n}\n"
              "function bare {\n  param($handler)\n  return $handler\n}\n",
              "$handler", "command", 2, 2);

SEMANTIC_CASE(pascal, PASCAL, "main.pas",
              "program Sample;\n"
              "function handler: Integer; begin handler := 0; end;\n"
              "function accept(value: Integer): Integer; begin accept := value; end;\n"
              "function argument(handler: Integer): Integer; begin argument := accept(handler); "
              "end;\n"
              "function direct: Integer; begin direct := handler(); end;\n"
              "function bare(handler: Integer): Integer; begin bare := handler; end;\n"
              "begin end.\n",
              "handler", "exprCall", 2, 2);

SEMANTIC_CASE(dlang, DLANG, "main.d",
              "int handler() { return 0; }\n"
              "int accept(int value) { return value; }\n"
              "int argument(int handler) { return accept(handler); }\n"
              "int direct() { return handler(); }\n"
              "int bare(int handler) { return handler; }\n",
              "handler", "call_expression", 2, 2);

SEMANTIC_CASE(scheme, SCHEME, "main.scm",
              "(define (handler) 0)\n"
              "(define (accept value) value)\n"
              "(define (argument handler) (accept handler))\n"
              "(define (direct) (handler))\n"
              "(define (bare handler) handler)\n",
              "handler", "list", 12, 2);

SEMANTIC_CASE(fennel, FENNEL, "main.fnl",
              "(fn handler [] 0)\n"
              "(fn accept [value] value)\n"
              "(fn argument [handler] (accept handler))\n"
              "(fn direct [] (handler))\n"
              "(fn bare [handler] handler)\n",
              "handler", "list", 2, 2);

SEMANTIC_CASE(fish, FISH, "main.fish",
              "function handler\n  return 0\nend\n"
              "function accept --argument value\n  echo $value\nend\n"
              "function argument --argument handler\n  accept $handler\nend\n"
              "function direct\n  handler\nend\n"
              "function bare --argument handler\n  echo $handler\nend\n",
              "handler", "command", 4, 4);

SEMANTIC_BROAD_CASE(awk, AWK, "main.awk",
                    "function handler() { return 0 }\n"
                    "function accept(value) { return value }\n"
                    "function argument(handler) { return accept(handler) }\n"
                    "function direct() { return handler() }\n"
                    "function bare(handler) { return handler }\n",
                    "handler", "func_call", 2, 2, "function scope(watched) { print watched }\n",
                    "Scope.awk", "scope", "watched", "command", 0, NULL, 0);

SEMANTIC_CASE(zsh, ZSH, "main.zsh",
              "handler() { return 0 }\n"
              "accept() { print -- $1 }\n"
              "argument() { local handler=$1; accept $handler }\n"
              "direct() { handler }\n"
              "bare() { local handler=$1; print -- $handler }\n",
              "handler", "command", 5, 4);

SEMANTIC_CASE(tcl, TCL, "main.tcl",
              "proc handler {} { return 0 }\n"
              "proc accept {value} { return $value }\n"
              "proc argument {handler} { return [accept $handler] }\n"
              "proc direct {} { return [handler] }\n"
              "proc bare {handler} { return $handler }\n",
              "$handler", "command", 7, 2);

SEMANTIC_CASE(ada, ADA, "main.adb",
              "package body Sample is\n"
              "  function handler(value : Integer) return Integer is begin return value; end "
              "handler;\n"
              "  function accept(value : Integer) return Integer is begin return value; end "
              "accept;\n"
              "  function argument(handler : Integer) return Integer is begin return "
              "accept(handler); end argument;\n"
              "  function direct return Integer is begin return handler(0); end direct;\n"
              "  function bare(handler : Integer) return Integer is begin return handler; end "
              "bare;\n"
              "end Sample;\n",
              "handler", "function_call", 2, 2);

SEMANTIC_CASE(agda, AGDA, "Main.agda",
              "module Main where\n"
              "open import Agda.Builtin.Nat\n"
              "handler : Nat -> Nat\nhandler value = value\n"
              "accept : Nat -> Nat\naccept value = value\n"
              "argument : Nat -> Nat\nargument handler = accept handler\n"
              "direct : Nat -> Nat\ndirect value = handler value\n"
              "bare : Nat -> Nat\nbare handler = handler\n",
              "handler", "expr", 15, 2);

#define MATRIX_A_CASES(X) \
    X(go)                 \
    X(python)             \
    X(javascript)         \
    X(typescript)         \
    X(tsx)                \
    X(rust)               \
    X(java)               \
    X(cpp)                \
    X(csharp)             \
    X(php)                \
    X(lua)                \
    X(scala)              \
    X(kotlin)             \
    X(ruby)               \
    X(c)                  \
    X(bash)               \
    X(zig)                \
    X(elixir)             \
    X(haskell)            \
    X(ocaml)              \
    X(objc)               \
    X(swift)              \
    X(dart)               \
    X(perl)               \
    X(groovy)             \
    X(erlang)             \
    X(r)                  \
    X(css)                \
    X(scss)               \
    X(hcl)                \
    X(sql)                \
    X(clojure)            \
    X(fsharp)             \
    X(julia)              \
    X(vimscript)          \
    X(nix)                \
    X(commonlisp)         \
    X(elm)                \
    X(fortran)            \
    X(cuda)               \
    X(cobol)              \
    X(verilog)            \
    X(emacslisp)          \
    X(makefile)           \
    X(cmake)              \
    X(meson)              \
    X(glsl)               \
    X(matlab)             \
    X(lean)               \
    X(form)               \
    X(magma)              \
    X(wolfram)            \
    X(solidity)           \
    X(typst)              \
    X(gdscript)           \
    X(gleam)              \
    X(powershell)         \
    X(pascal)             \
    X(dlang)              \
    X(scheme)             \
    X(fennel)             \
    X(fish)               \
    X(awk)                \
    X(zsh)                \
    X(tcl)                \
    X(ada)                \
    X(agda)

#define DEFINE_MATRIX_TEST(tag_)                        \
    TEST(repro_call_argument_matrix_a_##tag_) {         \
        int failures = check_matrix_case(&CASE_##tag_); \
        ASSERT_EQ(failures, 0);                         \
        PASS();                                         \
    }

MATRIX_A_CASES(DEFINE_MATRIX_TEST)

#define MATRIX_CASE_POINTER(tag_) &CASE_##tag_,
static const MatrixCase *const MATRIX_A_CASE_ROWS[] = {MATRIX_A_CASES(MATRIX_CASE_POINTER)};
#undef MATRIX_CASE_POINTER

_Static_assert(sizeof(MATRIX_A_CASE_ROWS) / sizeof(MATRIX_A_CASE_ROWS[0]) == 67,
               "GO..AGDA call-capable matrix must contain exactly 67 language rows");

size_t repro_call_argument_matrix_a_copy_language_ids(CBMLanguage *language_ids, size_t capacity) {
    size_t row_count = sizeof(MATRIX_A_CASE_ROWS) / sizeof(MATRIX_A_CASE_ROWS[0]);
    for (size_t row = 0; language_ids && row < row_count && row < capacity; row++) {
        language_ids[row] = MATRIX_A_CASE_ROWS[row]->language;
    }
    return row_count;
}

SUITE(repro_call_argument_matrix_a) {
#define RUN_MATRIX_TEST(tag_) RUN_TEST(repro_call_argument_matrix_a_##tag_);
    MATRIX_A_CASES(RUN_MATRIX_TEST)
#undef RUN_MATRIX_TEST
}

#undef DEFINE_MATRIX_TEST
#undef MATRIX_A_CASES
#undef NATIVE_DOMAIN_CASE
#undef NATIVE_SCOPE_CASE
#undef SEMANTIC_BROAD_CASE
#undef SEMANTIC_CASE
#undef CHECK_EXACT
