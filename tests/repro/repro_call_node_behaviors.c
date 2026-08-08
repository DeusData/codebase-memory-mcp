/*
 * repro_call_node_behaviors.c — raw CALL/USAGE ownership for overloaded and
 * call-shaped expressions.
 *
 * C++ and CUDA deliberately register several expression containers as call
 * nodes. A container may own one synthetic operator call, one semantic-only
 * implicit-call candidate, or no call at all; none of those roles allows it to erase
 * operand references. Scala and Elixir operator syntax follows the same rule.
 * Puppet resource declarations belong to the DSL extractor and must not be
 * reinterpreted as generic code calls.
 *
 * Every fixture has exactly one target AST node. Every raw count is exact and
 * joined to the exact qualified name of its caller (or exact module QN for the
 * top-level Puppet DSL case), so duplicate emissions and scope drift stay RED.
 */
#include "test_framework.h"
#include "cbm.h"
#include "lang_specs.h"
#include "macro_table.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *tag;
    CBMLanguage language;
    const char *filename;
    const char *source;
    const char *ast_kind;
    const char *reason;
} BehaviorCase;

static int terminal_name_matches(const char *raw, const char *expected) {
    if (!raw || !expected)
        return 0;
    if (strcmp(raw, expected) == 0)
        return 1;

    size_t raw_len = strlen(raw);
    size_t expected_len = strlen(expected);
    if (raw_len < expected_len)
        return 0;
    const char *tail = raw + raw_len - expected_len;
    if (strcmp(tail, expected) != 0)
        return 0;
    if (tail == raw)
        return 1;

    unsigned char boundary = (unsigned char)tail[-1];
    return !isalnum(boundary) && boundary != '_';
}

static void check_exact(const BehaviorCase *test_case, const char *invariant, int actual,
                        int expected, int *failures) {
    if (actual == expected)
        return;
    fprintf(stderr, "  [call-node-behavior] case=%s invariant=%s expected=%d actual=%d reason=%s\n",
            test_case->tag, invariant, expected, actual, test_case->reason);
    (*failures)++;
}

static int ast_kind_count(TSNode node, const char *kind) {
    int count = strcmp(ts_node_type(node), kind) == 0 ? 1 : 0;
    uint32_t child_count = ts_node_child_count(node);
    for (uint32_t i = 0; i < child_count; i++)
        count += ast_kind_count(ts_node_child(node, i), kind);
    return count;
}

static void check_exact_ast_kind(const BehaviorCase *test_case, int *failures) {
    const TSLanguage *language = cbm_ts_language(test_case->language);
    TSParser *parser = ts_parser_new();
    if (!language || !parser || !ts_parser_set_language(parser, language)) {
        fprintf(stderr,
                "  [call-node-behavior] case=%s invariant=ast_parser_ready expected=1 "
                "actual=0 reason=%s\n",
                test_case->tag, test_case->reason);
        (*failures)++;
        if (parser)
            ts_parser_delete(parser);
        return;
    }

    TSTree *tree = ts_parser_parse_string(parser, NULL, test_case->source,
                                          (uint32_t)strlen(test_case->source));
    int count = 0;
    int parse_error = 1;
    if (tree) {
        TSNode root = ts_tree_root_node(tree);
        parse_error = ts_node_has_error(root) ? 1 : 0;
        count = ast_kind_count(root, test_case->ast_kind);
    }
    check_exact(test_case, "ast_clean_parse", parse_error, 0, failures);
    check_exact(test_case, "exact_ast_kind_count", count, 1, failures);
    if (tree)
        ts_tree_delete(tree);
    ts_parser_delete(parser);
}

static CBMFileResult *extract_case_with_macros(const BehaviorCase *test_case,
                                               const CBMMacroTable *macro_table, int *failures) {
    CBMFileResult *result =
        cbm_extract_file_ex(test_case->source, (int)strlen(test_case->source), test_case->language,
                            "repro", test_case->filename, 0, NULL, NULL, macro_table, NULL);
    if (!result) {
        fprintf(stderr,
                "  [call-node-behavior] case=%s invariant=extract_result expected=non-null "
                "reason=%s\n",
                test_case->tag, test_case->reason);
        (*failures)++;
        return NULL;
    }
    check_exact(test_case, "clean_parse", (result->has_error || result->parse_incomplete) ? 1 : 0,
                0, failures);
    return result;
}

static CBMFileResult *extract_case(const BehaviorCase *test_case, int *failures) {
    return extract_case_with_macros(test_case, NULL, failures);
}

static int call_count_exact(const CBMFileResult *result, const char *caller_qn,
                            const char *callee) {
    int count = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (call->callee_name && call->enclosing_func_qn &&
            strcmp(call->callee_name, callee) == 0 &&
            strcmp(call->enclosing_func_qn, caller_qn) == 0)
            count++;
    }
    return count;
}

static int usage_count_exact(const CBMFileResult *result, const char *caller_qn,
                             const char *reference) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->ref_name && usage->enclosing_func_qn &&
            strcmp(usage->ref_name, reference) == 0 &&
            strcmp(usage->enclosing_func_qn, caller_qn) == 0)
            count++;
    }
    return count;
}

static int value_usage_count_exact(const CBMFileResult *result, const char *caller_qn,
                                   const char *reference) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->ref_name && usage->enclosing_func_qn &&
            strcmp(usage->ref_name, reference) == 0 &&
            strcmp(usage->enclosing_func_qn, caller_qn) == 0) {
            count++;
        }
    }
    return count;
}

static int callable_definition_count_exact(const CBMFileResult *result, const char *name,
                                           const char *qualified_name) {
    int count = 0;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        int callable_label = definition->label && (strcmp(definition->label, "Function") == 0 ||
                                                   strcmp(definition->label, "Method") == 0);
        if (callable_label && definition->name && definition->qualified_name &&
            strcmp(definition->name, name) == 0 &&
            strcmp(definition->qualified_name, qualified_name) == 0) {
            count++;
        }
    }
    return count;
}

static int call_count_in_routine(const CBMFileResult *result, const char *caller,
                                 const char *callee) {
    int count = 0;
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (call->callee_name && strcmp(call->callee_name, callee) == 0 &&
            terminal_name_matches(call->enclosing_func_qn, caller))
            count++;
    }
    return count;
}

static int exact_semantic_candidate_count_in_routine(const BehaviorCase *test_case,
                                                     const CBMFileResult *result,
                                                     const char *caller, const char *callee,
                                                     const char *site_text) {
    int count = 0;
    size_t source_len = strlen(test_case->source);
    size_t site_len = strlen(site_text);
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (!call->requires_lsp_resolution || !call->callee_name ||
            strcmp(call->callee_name, callee) != 0 ||
            !terminal_name_matches(call->enclosing_func_qn, caller) ||
            call->site_end_byte <= call->site_start_byte || call->site_end_byte > source_len ||
            (size_t)(call->site_end_byte - call->site_start_byte) != site_len ||
            strncmp(test_case->source + call->site_start_byte, site_text, site_len) != 0) {
            continue;
        }
        count++;
    }
    return count;
}

static int usage_count_in_routine(const CBMFileResult *result, const char *caller,
                                  const char *reference) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->ref_name &&
            strcmp(usage->ref_name, reference) == 0 &&
            terminal_name_matches(usage->enclosing_func_qn, caller))
            count++;
    }
    return count;
}

static int callable_reference_count_in_routine(const CBMFileResult *result, const char *caller,
                                               const char *reference) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == CBM_USAGE_CALL_REFERENCE && usage->ref_name &&
            strcmp(usage->ref_name, reference) == 0 &&
            terminal_name_matches(usage->enclosing_func_qn, caller))
            count++;
    }
    return count;
}

static int semantic_reference_candidate_count_in_routine(const CBMFileResult *result,
                                                         const char *caller,
                                                         const char *reference) {
    int count = 0;
    for (int i = 0; i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->may_be_call_reference && usage->ref_name &&
            strcmp(usage->ref_name, reference) == 0 &&
            terminal_name_matches(usage->enclosing_func_qn, caller)) {
            count++;
        }
    }
    return count;
}

static int resolved_call_count_in_routine(const CBMFileResult *result, const char *caller,
                                          const char *callee) {
    int count = 0;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        const CBMResolvedCall *call = &result->resolved_calls.items[i];
        if (call->kind == CBM_RESOLVED_INVOCATION &&
            terminal_name_matches(call->caller_qn, caller) &&
            terminal_name_matches(call->callee_qn, callee)) {
            count++;
        }
    }
    return count;
}

static int resolved_call_total_in_routine(const CBMFileResult *result, const char *caller) {
    int count = 0;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        if (result->resolved_calls.items[i].kind == CBM_RESOLVED_INVOCATION &&
            terminal_name_matches(result->resolved_calls.items[i].caller_qn, caller)) {
            count++;
        }
    }
    return count;
}

static int resolved_reference_count_in_routine(const CBMFileResult *result, const char *caller,
                                               const char *callee) {
    int count = 0;
    for (int i = 0; i < result->resolved_calls.count; i++) {
        const CBMResolvedCall *reference = &result->resolved_calls.items[i];
        if (reference->kind == CBM_RESOLVED_CALL_REFERENCE &&
            terminal_name_matches(reference->caller_qn, caller) &&
            terminal_name_matches(reference->callee_qn, callee)) {
            count++;
        }
    }
    return count;
}

static void check_caller_definition(const BehaviorCase *test_case, const CBMFileResult *result,
                                    const char *caller, int *failures) {
    int count = 0;
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *definition = &result->defs.items[i];
        int callable_label = definition->label && (strcmp(definition->label, "Function") == 0 ||
                                                   strcmp(definition->label, "Method") == 0);
        if (callable_label && definition->name && strcmp(definition->name, caller) == 0 &&
            terminal_name_matches(definition->qualified_name, caller)) {
            count++;
        }
    }
    check_exact(test_case, "caller_callable_definition", count, 1, failures);
}

static const char *checked_module_qn(const BehaviorCase *test_case, const CBMFileResult *result,
                                     int *failures) {
    if (result->module_qn && result->module_qn[0])
        return result->module_qn;
    fprintf(stderr,
            "  [call-node-behavior] case=%s invariant=module_qn expected=non-empty reason=%s\n",
            test_case->tag, test_case->reason);
    (*failures)++;
    return "<missing-module-qn>";
}

static int begin_routine_case(const BehaviorCase *test_case, CBMFileResult **result_out,
                              int *failures) {
    check_exact_ast_kind(test_case, failures);
    CBMFileResult *result = extract_case(test_case, failures);
    *result_out = result;
    if (!result)
        return 0;
    check_caller_definition(test_case, result, "run", failures);
    return 1;
}

static int finish_case(CBMFileResult *result, int failures) {
    if (result)
        cbm_free_result(result);
    return failures == 0 ? 0 : 1;
}

static int run_binary_behavior(const BehaviorCase *test_case) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_exact(test_case, "operator_call_in_run",
                    call_count_in_routine(result, "run", "operator+"), 1, &failures);
        check_exact(test_case, "lhs_usage_in_run", usage_count_in_routine(result, "run", "lhs"), 1,
                    &failures);
        check_exact(test_case, "rhs_usage_in_run", usage_count_in_routine(result, "run", "rhs"), 1,
                    &failures);
        check_exact(test_case, "lhs_not_call_in_run", call_count_in_routine(result, "run", "lhs"),
                    0, &failures);
        check_exact(test_case, "rhs_not_call_in_run", call_count_in_routine(result, "run", "rhs"),
                    0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

static int run_subscript_behavior(const BehaviorCase *test_case) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_exact(test_case, "operator_call_in_run",
                    call_count_in_routine(result, "run", "operator[]"), 1, &failures);
        check_exact(test_case, "receiver_usage_in_run",
                    usage_count_in_routine(result, "run", "receiver"), 1, &failures);
        check_exact(test_case, "index_usage_in_run", usage_count_in_routine(result, "run", "index"),
                    1, &failures);
        check_exact(test_case, "receiver_not_call_in_run",
                    call_count_in_routine(result, "run", "receiver"), 0, &failures);
        check_exact(test_case, "index_not_call_in_run",
                    call_count_in_routine(result, "run", "index"), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

static int run_unary_behavior(const BehaviorCase *test_case) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_exact(test_case, "operator_call_in_run",
                    call_count_in_routine(result, "run", "operator-"), 1, &failures);
        check_exact(test_case, "operand_usage_in_run",
                    usage_count_in_routine(result, "run", "operand"), 1, &failures);
        check_exact(test_case, "operand_not_call_in_run",
                    call_count_in_routine(result, "run", "operand"), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

static int run_update_behavior(const BehaviorCase *test_case) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_exact(test_case, "operator_call_in_run",
                    call_count_in_routine(result, "run", "operator++"), 1, &failures);
        check_exact(test_case, "operand_usage_in_run",
                    usage_count_in_routine(result, "run", "operand"), 1, &failures);
        check_exact(test_case, "operand_not_call_in_run",
                    call_count_in_routine(result, "run", "operand"), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

/* A delete expression has no textual `~Type` token. The raw call therefore uses
 * a non-textual `~` candidate at the exact expression span. It is semantic-only:
 * without a materialized destructor the LSP emits no target and the graph pass
 * drops the candidate instead of falling back to the operand's spelling. */
static int run_delete_behavior(const BehaviorCase *test_case) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_exact(test_case, "raw_delete_semantic_candidate_in_run",
                    call_count_in_routine(result, "run", "~"), 1, &failures);
        check_exact(test_case, "raw_delete_candidate_exact_site",
                    exact_semantic_candidate_count_in_routine(test_case, result, "run", "~",
                                                              "delete victim"),
                    1, &failures);
        check_exact(test_case, "operand_usage_in_run",
                    usage_count_in_routine(result, "run", "victim"), 1, &failures);
        check_exact(test_case, "operand_not_call_in_run",
                    call_count_in_routine(result, "run", "victim"), 0, &failures);
        check_exact(test_case, "semantic_operator_not_minted_raw",
                    call_count_in_routine(result, "run", "operator delete"), 0, &failures);
        check_exact(test_case, "unmaterialized_destructor_not_resolved",
                    resolved_call_count_in_routine(result, "run", "~Value"), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

static int run_field_behavior(const BehaviorCase *test_case) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_exact(test_case, "receiver_usage_in_run",
                    usage_count_in_routine(result, "run", "receiver"), 1, &failures);
        check_exact(test_case, "member_usage_in_run",
                    usage_count_in_routine(result, "run", "member"), 1, &failures);
        check_exact(test_case, "receiver_not_call_in_run",
                    call_count_in_routine(result, "run", "receiver"), 0, &failures);
        check_exact(test_case, "member_not_call_in_run",
                    call_count_in_routine(result, "run", "member"), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 0, &failures);
    }
    return finish_case(result, failures);
}

static int run_callable_value_behavior(const BehaviorCase *test_case,
                                       int expect_raw_typed_reference,
                                       int expect_semantic_reference) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_caller_definition(test_case, result, "handler", &failures);
        check_exact(test_case, "outer_accept_call_in_run",
                    call_count_in_routine(result, "run", "accept"), 1, &failures);
        check_exact(test_case, "handler_value_usage_in_run",
                    usage_count_in_routine(result, "run", "handler"),
                    expect_raw_typed_reference ? 0 : 1, &failures);
        check_exact(test_case, "handler_callable_reference_in_run",
                    callable_reference_count_in_routine(result, "run", "handler"),
                    expect_raw_typed_reference ? 1 : 0, &failures);
        check_exact(test_case, "handler_semantic_reference_candidate_in_run",
                    semantic_reference_candidate_count_in_routine(result, "run", "handler"),
                    !expect_raw_typed_reference && expect_semantic_reference ? 1 : 0, &failures);
        check_exact(test_case, "handler_value_not_invoked_in_run",
                    call_count_in_routine(result, "run", "handler"), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);

        if (expect_semantic_reference) {
            check_exact(test_case, "lsp_outer_accept_call_in_run",
                        resolved_call_count_in_routine(result, "run", "accept"), 1, &failures);
            check_exact(test_case, "lsp_handler_value_not_invoked_in_run",
                        resolved_call_count_in_routine(result, "run", "handler"), 0, &failures);
            check_exact(test_case, "lsp_handler_callable_reference_in_run",
                        resolved_reference_count_in_routine(result, "run", "handler"), 1,
                        &failures);
            check_exact(test_case, "lsp_total_calls_in_run",
                        resolved_call_total_in_routine(result, "run"), 1, &failures);
        }
    }
    return finish_case(result, failures);
}

static int run_arrow_behavior(const BehaviorCase *test_case) {
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(test_case, &result, &failures)) {
        check_exact(test_case, "operator_arrow_call_in_run",
                    call_count_in_routine(result, "run", "operator->"), 1, &failures);
        check_exact(test_case, "receiver_usage_in_run",
                    usage_count_in_routine(result, "run", "receiver"), 1, &failures);
        check_exact(test_case, "member_usage_in_run",
                    usage_count_in_routine(result, "run", "member"), 1, &failures);
        check_exact(test_case, "receiver_not_call_in_run",
                    call_count_in_routine(result, "run", "receiver"), 0, &failures);
        check_exact(test_case, "member_not_call_in_run",
                    call_count_in_routine(result, "run", "member"), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

static int run_objectscript_call_behavior(const BehaviorCase *test_case, const char *caller_qn,
                                          const char *callee, const char *callee_reference,
                                          const CBMMacroTable *macro_table) {
    int failures = 0;
    CBMFileResult *result = NULL;
    check_exact_ast_kind(test_case, &failures);
    result = extract_case_with_macros(test_case, macro_table, &failures);
    if (result) {
        check_exact(test_case, "exact_caller_definition",
                    callable_definition_count_exact(result, "run", caller_qn), 1, &failures);
        check_exact(test_case, "exact_call_owner_and_target",
                    call_count_exact(result, caller_qn, callee), 1, &failures);
        check_exact(test_case, "argument_usage_in_exact_caller",
                    value_usage_count_exact(result, caller_qn, "watched"), 1, &failures);
        check_exact(test_case, "argument_not_call_in_exact_caller",
                    call_count_exact(result, caller_qn, "watched"), 0, &failures);
        check_exact(test_case, "callee_not_usage_in_exact_caller",
                    value_usage_count_exact(result, caller_qn, callee_reference), 0, &failures);
        check_exact(test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

static const char CPP_BINARY_SOURCE[] = "struct Value {};\n"
                                        "Value operator+(Value lhs, Value rhs) { return lhs; }\n"
                                        "Value run(Value lhs, Value rhs) { return lhs + rhs; }\n";

static const char CPP_SUBSCRIPT_SOURCE[] =
    "struct Buffer {\n"
    "  int storage;\n"
    "  int& operator[](int index) { return storage; }\n"
    "};\n"
    "int run(Buffer& receiver, int index) { return receiver[index]; }\n";

static const char CPP_UNARY_SOURCE[] = "struct Value {};\n"
                                       "Value operator-(Value operand) { return operand; }\n"
                                       "Value run(Value operand) { return -operand; }\n";

static const char CPP_UPDATE_SOURCE[] = "struct Counter {\n"
                                        "  Counter& operator++() { return *this; }\n"
                                        "};\n"
                                        "Counter& run(Counter& operand) { return ++operand; }\n";

static const char CPP_DELETE_SOURCE[] = "struct Value {};\n"
                                        "void run(Value* victim) { delete victim; }\n";

static const char CPP_FIELD_SOURCE[] = "struct Record { int member; };\n"
                                       "int run(Record receiver) { return receiver.member; }\n";

static const char CPP_ARROW_SOURCE[] = "struct Target { int member; };\n"
                                       "struct Proxy {\n"
                                       "  Target* target;\n"
                                       "  Target* operator->() { return target; }\n"
                                       "};\n"
                                       "int run(Proxy receiver) { return receiver->member; }\n";

static const char CPP_DELETE_COLLISION_SOURCE[] = "struct Value { ~Value() {} };\n"
                                                  "void victim() {}\n"
                                                  "void run(Value* victim) { delete victim; }\n";

static const char JAVA_METHOD_REFERENCE_SOURCE[] = "interface Task { void invoke(); }\n"
                                                   "class Sample {\n"
                                                   "  void handler() {}\n"
                                                   "  void accept(Task callback) {}\n"
                                                   "  void run() { accept(this::handler); }\n"
                                                   "}\n";

static const char KOTLIN_CALLABLE_REFERENCE_SOURCE[] = "fun handler() {}\n"
                                                       "fun accept(callback: () -> Unit) {}\n"
                                                       "fun run() { accept(::handler) }\n";

static const char CSHARP_METHOD_GROUP_SOURCE[] = "delegate void Task();\n"
                                                 "class Sample {\n"
                                                 "  static void handler() {}\n"
                                                 "  static void accept(Task callback) {}\n"
                                                 "  static void run() { accept(handler); }\n"
                                                 "}\n";

static const char CPP_FUNCTION_VALUE_SOURCE[] = "void handler() {}\n"
                                                "void accept(void (*callback)()) {}\n"
                                                "void run() { accept(handler); }\n";

static const char RUST_FUNCTION_ITEM_SOURCE[] = "fn handler() {}\n"
                                                "fn accept(_callback: fn()) {}\n"
                                                "fn run() { accept(handler); }\n";

static const char OBJECTSCRIPT_CLASS_METHOD_SOURCE[] =
    "Class Sample.Behavior Extends %RegisteredObject\n"
    "{\n"
    "Method run(watched As %String) As %Status\n"
    "{\n"
    "    Do ##class(Sample.Target).accept(watched)\n"
    "    Quit\n"
    "}\n"
    "}\n";

static const char OBJECTSCRIPT_METHOD_SOURCE[] =
    "Class Sample.Behavior Extends %RegisteredObject\n"
    "{\n"
    "Method run(target As Sample.Target, watched As %String) As %Status\n"
    "{\n"
    "    Do target.accept(watched)\n"
    "    Quit\n"
    "}\n"
    "}\n";

static const char OBJECTSCRIPT_RELATIVE_DOT_METHOD_SOURCE[] =
    "Class Sample.Behavior Extends %RegisteredObject\n"
    "{\n"
    "Method accept(value As %String) As %Status\n"
    "{\n"
    "    Quit\n"
    "}\n"
    "Method run(watched As %String) As %Status\n"
    "{\n"
    "    Do ..accept(watched)\n"
    "    Quit\n"
    "}\n"
    "}\n";

static const char OBJECTSCRIPT_MACRO_SOURCE[] = "Class Sample.Behavior Extends %RegisteredObject\n"
                                                "{\n"
                                                "Method run(watched As %Status) As %Status\n"
                                                "{\n"
                                                "    Quit $$$ISERR(watched)\n"
                                                "}\n"
                                                "}\n";

static const char OBJECTSCRIPT_EXTRINSIC_SOURCE[] = "run(watched)\n"
                                                    "    Quit $$accept(watched)\n"
                                                    "accept(value)\n"
                                                    "    Quit value\n";

static const char OBJECTSCRIPT_ROUTINE_TAG_SOURCE[] = "run(watched)\n"
                                                      "    Do accept(watched)\n"
                                                      "    Quit\n"
                                                      "accept(value)\n"
                                                      "    Quit value\n";

static const char SCALA_INFIX_SOURCE[] = "class Box {\n"
                                         "  def merge(other: Box): Box = this\n"
                                         "}\n"
                                         "object Sample {\n"
                                         "  def run(lhs: Box, rhs: Box): Box = lhs merge rhs\n"
                                         "}\n";

static const char ELIXIR_BINARY_SOURCE[] = "defmodule Sample do\n"
                                           "  def run(watched), do: watched + 1\n"
                                           "end\n";

static const char PUPPET_RESOURCE_SOURCE[] = "$watched = 'ready'\n"
                                             "notify { 'call-node-behavior':\n"
                                             "  message => $watched,\n"
                                             "}\n";

#define DEFINE_CPP_CUDA_BEHAVIOR_TESTS(name, source_value, ast_value, runner, reason_value) \
    TEST(repro_call_node_behavior_cpp_##name) {                                             \
        static const BehaviorCase test_case = {"cpp/" #name, CBM_LANG_CPP, "behavior.cpp",  \
                                               source_value, ast_value,    reason_value};   \
        return runner(&test_case);                                                          \
    }                                                                                       \
    TEST(repro_call_node_behavior_cuda_##name) {                                            \
        static const BehaviorCase test_case = {"cuda/" #name, CBM_LANG_CUDA, "behavior.cu", \
                                               source_value,  ast_value,     reason_value}; \
        return runner(&test_case);                                                          \
    }

DEFINE_CPP_CUDA_BEHAVIOR_TESTS(
    binary, CPP_BINARY_SOURCE, "binary_expression", run_binary_behavior,
    "binary operator call exists, but operands remain independently visible usages")
DEFINE_CPP_CUDA_BEHAVIOR_TESTS(
    subscript, CPP_SUBSCRIPT_SOURCE, "subscript_expression", run_subscript_behavior,
    "subscript operator needs one synthetic call without consuming receiver or index usages")
DEFINE_CPP_CUDA_BEHAVIOR_TESTS(
    unary, CPP_UNARY_SOURCE, "unary_expression", run_unary_behavior,
    "unary overload needs its exact synthetic operator name and operand usage")
DEFINE_CPP_CUDA_BEHAVIOR_TESTS(
    update, CPP_UPDATE_SOURCE, "update_expression", run_update_behavior,
    "update overload needs one operator++ call while retaining its operand usage")
DEFINE_CPP_CUDA_BEHAVIOR_TESTS(
    delete, CPP_DELETE_SOURCE, "delete_expression", run_delete_behavior,
    "delete keeps an exact-span semantic candidate plus the independent operand usage")
DEFINE_CPP_CUDA_BEHAVIOR_TESTS(
    field, CPP_FIELD_SOURCE, "field_expression", run_field_behavior,
    "ordinary member access is a callee wrapper, not a call, and both names are usages")
DEFINE_CPP_CUDA_BEHAVIOR_TESTS(
    overloaded_arrow, CPP_ARROW_SOURCE, "field_expression", run_arrow_behavior,
    "overloaded operator-> is an actual invocation while receiver and member remain usages")

#undef DEFINE_CPP_CUDA_BEHAVIOR_TESTS

TEST(repro_call_node_behavior_objectscript_udl_class_method_call) {
    static const BehaviorCase test_case = {
        "objectscript-udl/class-method-call",
        CBM_LANG_OBJECTSCRIPT_UDL,
        "Behavior.cls",
        OBJECTSCRIPT_CLASS_METHOD_SOURCE,
        "class_method_call",
        "##class(Sample.Target).accept(watched) has one exact class-method callee while its "
        "argument remains a value usage"};
    return run_objectscript_call_behavior(&test_case, "repro.Behavior.Sample.Behavior.run",
                                          "Sample.Target.accept", "accept", NULL);
}

TEST(repro_call_node_behavior_objectscript_udl_method_call) {
    static const BehaviorCase test_case = {
        "objectscript-udl/method-call",
        CBM_LANG_OBJECTSCRIPT_UDL,
        "Behavior.cls",
        OBJECTSCRIPT_METHOD_SOURCE,
        "method_call",
        "a typed ObjectScript receiver resolves one exact instance-method call without "
        "consuming its argument usage"};
    return run_objectscript_call_behavior(&test_case, "repro.Behavior.Sample.Behavior.run",
                                          "Sample.Target.accept", "accept", NULL);
}

TEST(repro_call_node_behavior_objectscript_udl_relative_dot_method) {
    static const BehaviorCase test_case = {
        "objectscript-udl/relative-dot-method",
        CBM_LANG_OBJECTSCRIPT_UDL,
        "Behavior.cls",
        OBJECTSCRIPT_RELATIVE_DOT_METHOD_SOURCE,
        "relative_dot_method",
        "..accept(watched) resolves against the exact enclosing class while watched remains a "
        "value usage"};
    return run_objectscript_call_behavior(&test_case, "repro.Behavior.Sample.Behavior.run",
                                          "repro.Behavior.Sample.Behavior.accept", "accept", NULL);
}

TEST(repro_call_node_behavior_objectscript_udl_macro) {
    static const BehaviorCase test_case = {
        "objectscript-udl/macro",
        CBM_LANG_OBJECTSCRIPT_UDL,
        "Behavior.cls",
        OBJECTSCRIPT_MACRO_SOURCE,
        "macro",
        "the system $$$ISERR(watched) macro maps to %SYSTEM.Status.IsError while watched "
        "remains a value usage"};
    CBMMacroTable *macro_table = calloc(1, sizeof(*macro_table));
    ASSERT_NOT_NULL(macro_table);
    cbm_macro_table_init_system(macro_table);
    int status = run_objectscript_call_behavior(&test_case, "repro.Behavior.Sample.Behavior.run",
                                                "%SYSTEM.Status.IsError", "ISERR", macro_table);
    cbm_macro_table_free(macro_table);
    return status;
}

TEST(repro_call_node_behavior_objectscript_routine_extrinsic_function) {
    static const BehaviorCase test_case = {
        "objectscript-routine/extrinsic-function",
        CBM_LANG_OBJECTSCRIPT_ROUTINE,
        "Behavior.mac",
        OBJECTSCRIPT_EXTRINSIC_SOURCE,
        "extrinsic_function",
        "$$accept(watched) belongs to the exact run tag and retains watched as its argument "
        "usage"};
    return run_objectscript_call_behavior(&test_case, "repro.Behavior.run", "accept", "accept",
                                          NULL);
}

TEST(repro_call_node_behavior_objectscript_routine_tag_call) {
    static const BehaviorCase test_case = {
        "objectscript-routine/routine-tag-call",
        CBM_LANG_OBJECTSCRIPT_ROUTINE,
        "Behavior.mac",
        OBJECTSCRIPT_ROUTINE_TAG_SOURCE,
        "routine_tag_call",
        "Do accept(watched) belongs to the exact run tag and retains watched as its argument "
        "usage"};
    return run_objectscript_call_behavior(&test_case, "repro.Behavior.run", "accept", "accept",
                                          NULL);
}

TEST(repro_call_node_behavior_scala_user_infix) {
    static const BehaviorCase test_case = {
        "scala/user-infix",
        CBM_LANG_SCALA,
        "Behavior.scala",
        SCALA_INFIX_SOURCE,
        "infix_expression",
        "user-defined infix merge is the callee while lhs and rhs remain value usages"};
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(&test_case, &result, &failures)) {
        check_exact(&test_case, "merge_call_in_run", call_count_in_routine(result, "run", "merge"),
                    1, &failures);
        check_exact(&test_case, "lhs_usage_in_run", usage_count_in_routine(result, "run", "lhs"), 1,
                    &failures);
        check_exact(&test_case, "rhs_usage_in_run", usage_count_in_routine(result, "run", "rhs"), 1,
                    &failures);
        check_exact(&test_case, "lhs_not_call_in_run", call_count_in_routine(result, "run", "lhs"),
                    0, &failures);
        check_exact(&test_case, "rhs_not_call_in_run", call_count_in_routine(result, "run", "rhs"),
                    0, &failures);
        check_exact(&test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

TEST(repro_call_node_behavior_elixir_binary_plus) {
    static const BehaviorCase test_case = {
        "elixir/binary-plus",
        CBM_LANG_ELIXIR,
        "behavior.ex",
        ELIXIR_BINARY_SOURCE,
        "binary_operator",
        "binary + belongs to the operator callee, not to its watched operand"};
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(&test_case, &result, &failures)) {
        check_exact(&test_case, "plus_call_in_run", call_count_in_routine(result, "run", "+"), 1,
                    &failures);
        check_exact(&test_case, "operand_usage_in_run",
                    usage_count_in_routine(result, "run", "watched"), 1, &failures);
        check_exact(&test_case, "operand_not_call_in_run",
                    call_count_in_routine(result, "run", "watched"), 0, &failures);
        check_exact(&test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

TEST(repro_call_node_behavior_puppet_resource_ownership) {
    static const BehaviorCase test_case = {
        "puppet/resource",
        CBM_LANG_PUPPET,
        "behavior.pp",
        PUPPET_RESOURCE_SOURCE,
        "resource_declaration",
        "resource declarations are Puppet DSL ownership and must not fabricate generic calls"};
    int failures = 0;
    check_exact_ast_kind(&test_case, &failures);
    CBMFileResult *result = extract_case(&test_case, &failures);
    if (result) {
        const char *module_qn = checked_module_qn(&test_case, result, &failures);
        check_exact(&test_case, "resource_value_usage_in_module",
                    usage_count_exact(result, module_qn, "$watched"), 1, &failures);
        check_exact(&test_case, "variable_not_call_in_module",
                    call_count_exact(result, module_qn, "$watched"), 0, &failures);
        check_exact(&test_case, "total_calls", result->calls.count, 0, &failures);
    }
    return finish_case(result, failures);
}

TEST(repro_call_node_behavior_java_method_reference_value) {
    static const BehaviorCase test_case = {
        "java/method-reference-value",
        CBM_LANG_JAVA,
        "Behavior.java",
        JAVA_METHOD_REFERENCE_SOURCE,
        "method_reference",
        "a Java method reference is a value, not an invocation at the reference site"};
    return run_callable_value_behavior(&test_case, 1, 1);
}

TEST(repro_call_node_behavior_kotlin_callable_reference_value) {
    static const BehaviorCase test_case = {
        "kotlin/callable-reference-value",
        CBM_LANG_KOTLIN,
        "Behavior.kt",
        KOTLIN_CALLABLE_REFERENCE_SOURCE,
        "callable_reference",
        "a Kotlin callable reference is a value, not an invocation at the reference site"};
    return run_callable_value_behavior(&test_case, 0, 1);
}

TEST(repro_call_node_behavior_csharp_method_group_value) {
    static const BehaviorCase test_case = {
        "csharp/method-group-value",
        CBM_LANG_CSHARP,
        "Behavior.cs",
        CSHARP_METHOD_GROUP_SOURCE,
        "invocation_expression",
        "a C# method group passed as an argument is a value, not a handler invocation"};
    return run_callable_value_behavior(&test_case, 0, 1);
}

TEST(repro_call_node_behavior_cpp_function_value) {
    static const BehaviorCase test_case = {
        "cpp/function-value",
        CBM_LANG_CPP,
        "behavior.cpp",
        CPP_FUNCTION_VALUE_SOURCE,
        "call_expression",
        "a C++ function value passed as an argument is referenced but not invoked"};
    return run_callable_value_behavior(&test_case, 0, 1);
}

TEST(repro_call_node_behavior_rust_function_item_value) {
    static const BehaviorCase test_case = {
        "rust/function-item-value",
        CBM_LANG_RUST,
        "behavior.rs",
        RUST_FUNCTION_ITEM_SOURCE,
        "call_expression",
        "a Rust function item passed as an argument is referenced but not invoked"};
    return run_callable_value_behavior(&test_case, 0, 1);
}

TEST(repro_call_node_behavior_cpp_delete_name_collision) {
    static const BehaviorCase test_case = {
        "cpp/delete-name-collision",
        CBM_LANG_CPP,
        "behavior.cpp",
        CPP_DELETE_COLLISION_SOURCE,
        "delete_expression",
        "delete must key the implicit ~Value destructor, not operand text that collides with "
        "victim()"};
    int failures = 0;
    CBMFileResult *result = NULL;
    if (begin_routine_case(&test_case, &result, &failures)) {
        check_caller_definition(&test_case, result, "victim", &failures);
        check_exact(&test_case, "destructor_call_in_run",
                    call_count_in_routine(result, "run", "~Value"), 1, &failures);
        check_exact(&test_case, "destructor_candidate_exact_site",
                    exact_semantic_candidate_count_in_routine(&test_case, result, "run", "~Value",
                                                              "delete victim"),
                    1, &failures);
        check_exact(&test_case, "operand_usage_in_run",
                    usage_count_in_routine(result, "run", "victim"), 1, &failures);
        check_exact(&test_case, "unrelated_victim_not_called_in_run",
                    call_count_in_routine(result, "run", "victim"), 0, &failures);
        check_exact(&test_case, "lsp_destructor_call_in_run",
                    resolved_call_count_in_routine(result, "run", "~Value"), 1, &failures);
        check_exact(&test_case, "lsp_unrelated_victim_not_called_in_run",
                    resolved_call_count_in_routine(result, "run", "victim"), 0, &failures);
        check_exact(&test_case, "lsp_total_calls_in_run",
                    resolved_call_total_in_routine(result, "run"), 1, &failures);
        check_exact(&test_case, "total_calls", result->calls.count, 1, &failures);
    }
    return finish_case(result, failures);
}

enum {
    CPP_BEHAVIOR_COUNT = 9,
    CUDA_BEHAVIOR_COUNT = 7,
    OTHER_BEHAVIOR_COUNT = 7,
    OBJECTSCRIPT_BEHAVIOR_COUNT = 6,
    CALL_NODE_BEHAVIOR_COUNT = CPP_BEHAVIOR_COUNT + CUDA_BEHAVIOR_COUNT + OTHER_BEHAVIOR_COUNT +
                               OBJECTSCRIPT_BEHAVIOR_COUNT,
};

_Static_assert(CALL_NODE_BEHAVIOR_COUNT == 29,
               "call-node behavior suite must retain 23 contracts and add 6 ObjectScript "
               "call-role guards");

SUITE(repro_call_node_behaviors) {
    RUN_TEST(repro_call_node_behavior_cpp_binary);
    RUN_TEST(repro_call_node_behavior_cpp_subscript);
    RUN_TEST(repro_call_node_behavior_cpp_unary);
    RUN_TEST(repro_call_node_behavior_cpp_update);
    RUN_TEST(repro_call_node_behavior_cpp_delete);
    RUN_TEST(repro_call_node_behavior_cpp_field);
    RUN_TEST(repro_call_node_behavior_cpp_overloaded_arrow);
    RUN_TEST(repro_call_node_behavior_cpp_function_value);
    RUN_TEST(repro_call_node_behavior_cpp_delete_name_collision);

    RUN_TEST(repro_call_node_behavior_cuda_binary);
    RUN_TEST(repro_call_node_behavior_cuda_subscript);
    RUN_TEST(repro_call_node_behavior_cuda_unary);
    RUN_TEST(repro_call_node_behavior_cuda_update);
    RUN_TEST(repro_call_node_behavior_cuda_delete);
    RUN_TEST(repro_call_node_behavior_cuda_field);
    RUN_TEST(repro_call_node_behavior_cuda_overloaded_arrow);

    RUN_TEST(repro_call_node_behavior_objectscript_udl_class_method_call);
    RUN_TEST(repro_call_node_behavior_objectscript_udl_method_call);
    RUN_TEST(repro_call_node_behavior_objectscript_udl_relative_dot_method);
    RUN_TEST(repro_call_node_behavior_objectscript_udl_macro);
    RUN_TEST(repro_call_node_behavior_objectscript_routine_extrinsic_function);
    RUN_TEST(repro_call_node_behavior_objectscript_routine_tag_call);

    RUN_TEST(repro_call_node_behavior_scala_user_infix);
    RUN_TEST(repro_call_node_behavior_elixir_binary_plus);
    RUN_TEST(repro_call_node_behavior_puppet_resource_ownership);
    RUN_TEST(repro_call_node_behavior_java_method_reference_value);
    RUN_TEST(repro_call_node_behavior_kotlin_callable_reference_value);
    RUN_TEST(repro_call_node_behavior_csharp_method_group_value);
    RUN_TEST(repro_call_node_behavior_rust_function_item_value);
}
