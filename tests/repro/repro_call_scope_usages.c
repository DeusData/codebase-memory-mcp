/*
 * repro_call_scope_usages.c — exact reference semantics inside call-shaped ASTs.
 *
 * A CALLS edge replaces only the exact invoked callee occurrence. Receivers,
 * computed keys, arguments, and nested callback bodies remain ordinary value
 * references. Several language specs also classify non-invocation containers
 * as call nodes; those containers must neither erase their whole subtree nor
 * fabricate calls from an arbitrary child. Every assertion uses exact counts
 * so duplicate CALLS/USAGE emissions cannot masquerade as a successful fix.
 */
#include "test_framework.h"
#include "cbm.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int qn_ends_with(const char *qn, const char *name) {
    if (!qn || !name)
        return 0;
    const char *last = strrchr(qn, '.');
    return strcmp(last ? last + 1 : qn, name) == 0;
}

static const char *call_short_name(const char *name) {
    if (!name)
        return NULL;
    const char *short_name = strrchr(name, '.');
    short_name = short_name ? short_name + 1 : name;
    const char *scope = strrchr(short_name, ':');
    return scope ? scope + 1 : short_name;
}

static int usage_count(const CBMFileResult *r, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < r->usages.count; i++) {
        const CBMUsage *usage = &r->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->ref_name &&
            strcmp(usage->ref_name, target) == 0 &&
            (!caller || qn_ends_with(usage->enclosing_func_qn, caller))) {
            count++;
        }
    }
    return count;
}

static int call_reference_count(const CBMFileResult *r, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < r->usages.count; i++) {
        const CBMUsage *usage = &r->usages.items[i];
        if (usage->kind == CBM_USAGE_CALL_REFERENCE && usage->ref_name &&
            strcmp(usage->ref_name, target) == 0 &&
            (!caller || qn_ends_with(usage->enclosing_func_qn, caller))) {
            count++;
        }
    }
    return count;
}

static int call_count(const CBMFileResult *r, const char *caller, const char *target) {
    int count = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        const char *callee = call_short_name(call->callee_name);
        if (callee && strcmp(callee, target) == 0 &&
            (!caller || qn_ends_with(call->enclosing_func_qn, caller))) {
            count++;
        }
    }
    return count;
}

static int exact_usage_count(const CBMFileResult *r, const char *caller_qn, const char *target) {
    int count = 0;
    for (int i = 0; i < r->usages.count; i++) {
        const CBMUsage *usage = &r->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->ref_name && usage->enclosing_func_qn &&
            strcmp(usage->ref_name, target) == 0 &&
            strcmp(usage->enclosing_func_qn, caller_qn) == 0) {
            count++;
        }
    }
    return count;
}

static int exact_call_count(const CBMFileResult *r, const char *caller_qn, const char *callee) {
    int count = 0;
    for (int i = 0; i < r->calls.count; i++) {
        const CBMCall *call = &r->calls.items[i];
        if (call->callee_name && call->enclosing_func_qn &&
            strcmp(call->callee_name, callee) == 0 &&
            strcmp(call->enclosing_func_qn, caller_qn) == 0) {
            count++;
        }
    }
    return count;
}

static int exact_definition_count(const CBMFileResult *r, const char *name,
                                  const char *qualified_name) {
    int count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *def = &r->defs.items[i];
        if (def->name && def->qualified_name && strcmp(def->name, name) == 0 &&
            strcmp(def->qualified_name, qualified_name) == 0) {
            count++;
        }
    }
    return count;
}

static int definition_count(const CBMFileResult *r, const char *name) {
    int count = 0;
    for (int i = 0; i < r->defs.count; i++) {
        const CBMDefinition *def = &r->defs.items[i];
        if (def->name && strcmp(def->name, name) == 0)
            count++;
    }
    return count;
}

static CBMFileResult *extract_case(const char *tag, const char *source, CBMLanguage language,
                                   const char *filename) {
    CBMFileResult *r =
        cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL, NULL);
    if (!r)
        fprintf(stderr, "  [call-scope] case=%s invariant=extract_result expected=non-null\n", tag);
    return r;
}

#define CHECK_COUNT(tag, invariant, expression, expected)                                         \
    do {                                                                                          \
        int actual_count = (expression);                                                          \
        if (actual_count != (expected)) {                                                         \
            fprintf(stderr, "  [call-scope] case=%s invariant=%s expected=%d actual=%d\n", (tag), \
                    (invariant), (expected), actual_count);                                       \
            failures++;                                                                           \
        }                                                                                         \
    } while (0)

#define CHECK_CLEAN(tag, result)                                                               \
    do {                                                                                       \
        if ((result)->has_error || (result)->parse_incomplete) {                               \
            fprintf(stderr, "  [call-scope] case=%s invariant=valid_fixture expected=clean\n", \
                    (tag));                                                                    \
            failures++;                                                                        \
        }                                                                                      \
    } while (0)

/* The two textual `foo` occurrences have different roles: the first is the
 * invoked callee, while the second is a function value passed as an argument. */
TEST(repro_call_scope_same_spelling_callee_and_argument) {
    static const char source[] = "function foo() {}\n"
                                 "function run() { foo(foo); }\n";
    CBMFileResult *r = extract_case("same_spelling", source, CBM_LANG_JAVASCRIPT, "main.js");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("same_spelling", r);
    CHECK_COUNT("same_spelling", "foo_definition", definition_count(r, "foo"), 1);
    CHECK_COUNT("same_spelling", "callee_call", call_count(r, "run", "foo"), 1);
    CHECK_COUNT("same_spelling", "argument_usage_only", usage_count(r, "run", "foo"), 1);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Nested calls need independent callee identity. The outer call must not hide
 * either its direct function-value argument or the inner call's value argument. */
TEST(repro_call_scope_nested_calls_retain_each_argument) {
    static const char source[] = "const value = 1;\n"
                                 "function handler(): void {}\n"
                                 "function inner(input: number): number { return input; }\n"
                                 "function outer(cb: () => void, input: number): void {}\n"
                                 "function run(): void { outer(handler, inner(value)); }\n";
    CBMFileResult *r = extract_case("nested_calls", source, CBM_LANG_TYPESCRIPT, "main.ts");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("nested_calls", r);
    CHECK_COUNT("nested_calls", "outer_call", call_count(r, "run", "outer"), 1);
    CHECK_COUNT("nested_calls", "inner_call", call_count(r, "run", "inner"), 1);
    CHECK_COUNT("nested_calls", "handler_argument_usage", usage_count(r, "run", "handler"), 1);
    CHECK_COUNT("nested_calls", "value_argument_usage", usage_count(r, "run", "value"), 1);
    CHECK_COUNT("nested_calls", "outer_not_usage", usage_count(r, "run", "outer"), 0);
    CHECK_COUNT("nested_calls", "inner_not_usage", usage_count(r, "run", "inner"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* A callback body is executable code, not part of the registrar's callee. Its
 * references remain visible even though the arrow function is an argument. */
TEST(repro_call_scope_inline_callback_body_usage) {
    static const char source[] = "const watched = 1;\n"
                                 "function register(callback) {}\n"
                                 "function run() { register(() => { watched; }); }\n";
    CBMFileResult *r = extract_case("inline_callback", source, CBM_LANG_JAVASCRIPT, "main.js");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("inline_callback", r);
    CHECK_COUNT("inline_callback", "registrar_call", call_count(r, "run", "register"), 1);
    CHECK_COUNT("inline_callback", "callback_body_usage", usage_count(r, NULL, "watched"), 1);
    CHECK_COUNT("inline_callback", "registrar_not_usage", usage_count(r, "run", "register"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* For a member call only `method` is the direct callee. The receiver and
 * arguments are values. For a computed call, both receiver and key are values. */
TEST(repro_call_scope_member_and_computed_components) {
    static const char source[] =
        "class Receiver { method(value: number): void {} }\n"
        "const receiver = new Receiver();\n"
        "const key = 'method';\n"
        "const payload = 1;\n"
        "function run(): void { receiver.method(payload); receiver[key](payload); }\n";
    CBMFileResult *r = extract_case("member_computed", source, CBM_LANG_TYPESCRIPT, "main.ts");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("member_computed", r);
    CHECK_COUNT("member_computed", "method_call", call_count(r, "run", "method"), 1);
    CHECK_COUNT("member_computed", "receiver_usages", usage_count(r, "run", "receiver"), 2);
    CHECK_COUNT("member_computed", "computed_key_usage", usage_count(r, "run", "key"), 1);
    CHECK_COUNT("member_computed", "argument_usages", usage_count(r, "run", "payload"), 2);
    CHECK_COUNT("member_computed", "terminal_method_not_usage", usage_count(r, "run", "method"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Constructor and generic-call syntax must not change argument semantics. The
 * type argument belongs to type-reference extraction; both handler expressions
 * are ordinary value usages. */
TEST(repro_call_scope_constructor_and_generic_arguments) {
    static const char source[] =
        "interface Payload {}\n"
        "class Box { constructor(callback: () => void) {} }\n"
        "function handler(): void {}\n"
        "function accept<T>(callback: () => void): void {}\n"
        "function run(): void { new Box(handler); accept<Payload>(handler); }\n";
    CBMFileResult *r = extract_case("constructor_generic", source, CBM_LANG_TYPESCRIPT, "main.ts");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("constructor_generic", r);
    CHECK_COUNT("constructor_generic", "constructor_call", call_count(r, "run", "Box"), 1);
    CHECK_COUNT("constructor_generic", "generic_call", call_count(r, "run", "accept"), 1);
    CHECK_COUNT("constructor_generic", "handler_argument_usages", usage_count(r, "run", "handler"),
                2);
    CHECK_COUNT("constructor_generic", "constructor_not_usage", usage_count(r, "run", "Box"), 0);
    CHECK_COUNT("constructor_generic", "generic_callee_not_usage", usage_count(r, "run", "accept"),
                0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_scope_c_function_pointer_invocation_and_arguments) {
    static const char source[] = "typedef void (*Callback)(void);\n"
                                 "void handler(void) {}\n"
                                 "void invoke(Callback first, Callback second) {}\n"
                                 "Callback shared = 0;\n"
                                 "void run(void) { invoke(shared, handler); shared(); }\n";
    CBMFileResult *r = extract_case("c_function_pointer", source, CBM_LANG_C, "main.c");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("c_function_pointer", r);
    CHECK_COUNT("c_function_pointer", "invoke_call", call_count(r, "run", "invoke"), 1);
    CHECK_COUNT("c_function_pointer", "pointer_call", call_count(r, "run", "shared"), 1);
    CHECK_COUNT("c_function_pointer", "pointer_argument_usage", usage_count(r, "run", "shared"), 1);
    CHECK_COUNT("c_function_pointer", "function_argument_usage", usage_count(r, "run", "handler"),
                1);
    CHECK_COUNT("c_function_pointer", "invoke_not_usage", usage_count(r, "run", "invoke"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_scope_cpp_function_pointer_invocation_and_arguments) {
    static const char source[] = "using Callback = void (*)();\n"
                                 "void handler() {}\n"
                                 "void invoke(Callback first, Callback second) {}\n"
                                 "Callback shared = nullptr;\n"
                                 "void run() { invoke(shared, handler); shared(); }\n";
    CBMFileResult *r = extract_case("cpp_function_pointer", source, CBM_LANG_CPP, "main.cpp");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("cpp_function_pointer", r);
    CHECK_COUNT("cpp_function_pointer", "invoke_call", call_count(r, "run", "invoke"), 1);
    CHECK_COUNT("cpp_function_pointer", "pointer_call", call_count(r, "run", "shared"), 1);
    CHECK_COUNT("cpp_function_pointer", "pointer_argument_usage", usage_count(r, "run", "shared"),
                1);
    CHECK_COUNT("cpp_function_pointer", "function_argument_usage", usage_count(r, "run", "handler"),
                1);
    CHECK_COUNT("cpp_function_pointer", "invoke_not_usage", usage_count(r, "run", "invoke"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_scope_python_keyword_argument) {
    static const char source[] = "def handler():\n    pass\n"
                                 "def accept(callback=None):\n    pass\n"
                                 "def run():\n    accept(callback=handler)\n";
    CBMFileResult *r = extract_case("python_keyword", source, CBM_LANG_PYTHON, "main.py");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("python_keyword", r);
    CHECK_COUNT("python_keyword", "accept_call", call_count(r, "run", "accept"), 1);
    CHECK_COUNT("python_keyword", "keyword_value_usage", usage_count(r, "run", "handler"), 1);
    CHECK_COUNT("python_keyword", "keyword_label_not_usage", usage_count(r, "run", "callback"), 0);
    CHECK_COUNT("python_keyword", "callee_not_usage", usage_count(r, "run", "accept"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Python registers `with_statement` as a call node. The actual manager()
 * invocation is a call, but the with body is not part of that callee. */
TEST(repro_call_scope_python_with_body_usage) {
    static const char source[] = "watched = 1\n"
                                 "def manager():\n    pass\n"
                                 "def run():\n    with manager():\n        watched\n";
    CBMFileResult *r = extract_case("python_with", source, CBM_LANG_PYTHON, "main.py");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("python_with", r);
    CHECK_COUNT("python_with", "run_definition", definition_count(r, "run"), 1);
    CHECK_COUNT("python_with", "manager_call", call_count(r, "run", "manager"), 1);
    CHECK_COUNT("python_with", "with_body_usage", usage_count(r, "run", "watched"), 1);
    CHECK_COUNT("python_with", "manager_not_usage", usage_count(r, "run", "manager"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Rust macro_invocation is call-like, but its token-tree arguments remain
 * value references and must survive the macro callee's scope. */
TEST(repro_call_scope_rust_macro_argument) {
    static const char source[] = "static WATCHED: i32 = 1;\n"
                                 "macro_rules! probe { ($value:expr) => { $value }; }\n"
                                 "fn run() { probe!(WATCHED); }\n";
    CBMFileResult *r = extract_case("rust_macro", source, CBM_LANG_RUST, "main.rs");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("rust_macro", r);
    CHECK_COUNT("rust_macro", "run_definition", definition_count(r, "run"), 1);
    CHECK_COUNT("rust_macro", "macro_call", call_count(r, "run", "probe"), 1);
    CHECK_COUNT("rust_macro", "macro_argument_usage", usage_count(r, "run", "WATCHED"), 1);
    CHECK_COUNT("rust_macro", "macro_callee_not_usage", usage_count(r, "run", "probe"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Elixir binary_operator is registered as a call node, but `watched + 1` is
 * not a call to watched. This uses caller-independent counts because Elixir's
 * callable attribution has a separately documented Module-scope drift. */
TEST(repro_call_scope_elixir_binary_operator_operand) {
    static const char source[] = "defmodule Sample do\n"
                                 "  def run(), do: watched + 1\n"
                                 "end\n";
    CBMFileResult *r = extract_case("elixir_binary", source, CBM_LANG_ELIXIR, "sample.ex");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("elixir_binary", r);
    CHECK_COUNT("elixir_binary", "run_definition", definition_count(r, "run"), 1);
    CHECK_COUNT("elixir_binary", "operator_call", call_count(r, NULL, "+"), 1);
    CHECK_COUNT("elixir_binary", "operand_usage", usage_count(r, NULL, "watched"), 1);
    CHECK_COUNT("elixir_binary", "operand_not_call", call_count(r, NULL, "watched"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Fortran keyword_argument is an argument container, not another invocation.
 * The keyword label is metadata; only its value is a usage. */
TEST(repro_call_scope_fortran_keyword_argument) {
    static const char source[] = "module sample\n"
                                 "  integer :: watched\n"
                                 "contains\n"
                                 "  integer function consume(value)\n"
                                 "    integer, intent(in) :: value\n"
                                 "    consume = value\n"
                                 "  end function consume\n"
                                 "  integer function run()\n"
                                 "    run = consume(value=watched)\n"
                                 "  end function run\n"
                                 "end module sample\n";
    CBMFileResult *r = extract_case("fortran_keyword", source, CBM_LANG_FORTRAN, "main.f90");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("fortran_keyword", r);
    CHECK_COUNT("fortran_keyword", "run_definition", definition_count(r, "run"), 1);
    CHECK_COUNT("fortran_keyword", "consume_call", call_count(r, NULL, "consume"), 1);
    CHECK_COUNT("fortran_keyword", "keyword_value_usage", usage_count(r, "run", "watched"), 1);
    CHECK_COUNT("fortran_keyword", "keyword_label_not_usage", usage_count(r, "run", "value"), 0);
    CHECK_COUNT("fortran_keyword", "keyword_label_not_call", call_count(r, "run", "value"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* A separate gap discovered while building the keyword-argument control:
 * Fortran's `call` statement is registered as call metadata, yet the direct
 * subroutine invocation currently emits no CBMCall. Keep this independent from
 * keyword labels so its future fix cannot hide behind another emitted call. */
TEST(repro_fortran_subroutine_call_is_extracted) {
    static const char source[] = "subroutine consume(value)\n"
                                 "  integer :: value\n"
                                 "end subroutine consume\n"
                                 "subroutine run()\n"
                                 "  call consume(watched)\n"
                                 "end subroutine run\n";
    CBMFileResult *r =
        extract_case("fortran_subroutine_call", source, CBM_LANG_FORTRAN, "main.f90");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("fortran_subroutine_call", r);
    CHECK_COUNT("fortran_subroutine_call", "consume_definition", definition_count(r, "consume"), 1);
    CHECK_COUNT("fortran_subroutine_call", "run_definition", definition_count(r, "run"), 1);
    CHECK_COUNT("fortran_subroutine_call", "total_calls", r->calls.count, 1);
    CHECK_COUNT("fortran_subroutine_call", "consume_call_from_run", call_count(r, "run", "consume"),
                1);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Kotlin navigation_expression also represents ordinary property access. A
 * property read must retain both reference components and emit no call. */
TEST(repro_call_scope_kotlin_property_navigation) {
    static const char source[] = "class Holder(val value: Int)\n"
                                 "val box = Holder(1)\n"
                                 "fun run(): Int = box.value\n";
    CBMFileResult *r = extract_case("kotlin_navigation", source, CBM_LANG_KOTLIN, "Main.kt");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("kotlin_navigation", r);
    CHECK_COUNT("kotlin_navigation", "run_definition", definition_count(r, "run"), 1);
    CHECK_COUNT("kotlin_navigation", "receiver_usage", usage_count(r, "run", "box"), 1);
    CHECK_COUNT("kotlin_navigation", "property_usage", usage_count(r, "run", "value"), 1);
    CHECK_COUNT("kotlin_navigation", "receiver_not_call", call_count(r, "run", "box"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* PHP 8.1 first-class callable syntax evaluates a function reference without
 * invoking it. The pinned grammar exposes the ordinary bare, member,
 * scoped/static, and nullsafe call-expression nodes plus variadic_placeholder;
 * CHECK_CLEAN guards the combinations used below. A real variadic_unpacking
 * argument is still an invocation, so keep it as a positive discriminator. */
TEST(repro_call_scope_php_first_class_callable) {
    static const char source[] = "<?php\n"
                                 "function bare_handler(): void {}\n"
                                 "function foo(...$values): void {}\n"
                                 "function accept(callable $callback): void {}\n"
                                 "class CallbackTarget {\n"
                                 "  public function member_handler(): void {}\n"
                                 "  public function nullable_handler(): void {}\n"
                                 "  public static function static_handler(): void {}\n"
                                 "}\n"
                                 "function run($receiver, $nullable, array $args): void {\n"
                                 "  accept(bare_handler(...));\n"
                                 "  accept($receiver->member_handler(...));\n"
                                 "  accept(CallbackTarget::static_handler(...));\n"
                                 "  accept($nullable?->nullable_handler(...));\n"
                                 "  foo(...$args);\n"
                                 "}\n";
    CBMFileResult *r = extract_case("php_first_class", source, CBM_LANG_PHP, "main.php");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("php_first_class", r);
    CHECK_COUNT("php_first_class", "total_calls", r->calls.count, 5);
    CHECK_COUNT("php_first_class", "accept_calls", call_count(r, "run", "accept"), 4);
    CHECK_COUNT("php_first_class", "bare_reference_usage", usage_count(r, "run", "bare_handler"),
                0);
    CHECK_COUNT("php_first_class", "bare_call_reference",
                call_reference_count(r, "run", "bare_handler"), 1);
    CHECK_COUNT("php_first_class", "bare_reference_not_invoked",
                call_count(r, "run", "bare_handler"), 0);
    CHECK_COUNT("php_first_class", "member_receiver_usage", usage_count(r, "run", "$receiver"), 1);
    CHECK_COUNT("php_first_class", "member_reference_usage",
                usage_count(r, "run", "member_handler"), 0);
    CHECK_COUNT("php_first_class", "member_call_reference",
                call_reference_count(r, "run", "member_handler"), 1);
    CHECK_COUNT("php_first_class", "member_reference_not_invoked",
                call_count(r, "run", "member_handler"), 0);
    CHECK_COUNT("php_first_class", "static_scope_usage", usage_count(r, "run", "CallbackTarget"),
                1);
    CHECK_COUNT("php_first_class", "static_reference_usage",
                usage_count(r, "run", "static_handler"), 0);
    CHECK_COUNT("php_first_class", "static_call_reference",
                call_reference_count(r, "run", "static_handler"), 1);
    CHECK_COUNT("php_first_class", "static_reference_not_invoked",
                call_count(r, "run", "static_handler"), 0);
    CHECK_COUNT("php_first_class", "nullsafe_receiver_usage", usage_count(r, "run", "$nullable"),
                1);
    CHECK_COUNT("php_first_class", "nullsafe_reference_usage",
                usage_count(r, "run", "nullable_handler"), 0);
    CHECK_COUNT("php_first_class", "nullsafe_call_reference",
                call_reference_count(r, "run", "nullable_handler"), 1);
    CHECK_COUNT("php_first_class", "nullsafe_reference_not_invoked",
                call_count(r, "run", "nullable_handler"), 0);
    CHECK_COUNT("php_first_class", "unpacking_argument_usage", usage_count(r, "run", "$args"), 1);
    CHECK_COUNT("php_first_class", "unpacking_is_invocation", call_count(r, "run", "foo"), 1);
    CHECK_COUNT("php_first_class", "invoked_callee_not_usage", usage_count(r, "run", "foo"), 0);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* MAX_SCOPES is currently a fixed 64-entry stack. The 65th nested function is
 * silently discarded, so calls in its body inherit the wrong enclosing QN.
 * The later sibling is an independent pop-recovery control: it must remain
 * attributed to itself rather than inheriting any deep or stale scope. */
TEST(repro_scope_stack_preserves_deepest_function) {
    enum { FUNCTION_DEPTH = 65, SOURCE_CAPACITY = 8192 };
    char *source = calloc(SOURCE_CAPACITY, 1);
    ASSERT_NOT_NULL(source);

    size_t used = (size_t)snprintf(source, SOURCE_CAPACITY, "function target() {}\n");
    for (int depth = 0; depth < FUNCTION_DEPTH && used < SOURCE_CAPACITY; depth++) {
        used +=
            (size_t)snprintf(source + used, SOURCE_CAPACITY - used, "function f%d() {\n", depth);
    }
    used += (size_t)snprintf(source + used, SOURCE_CAPACITY - used, "target();\n");
    for (int depth = 0; depth < FUNCTION_DEPTH && used < SOURCE_CAPACITY; depth++) {
        used += (size_t)snprintf(source + used, SOURCE_CAPACITY - used, "}\n");
    }
    used += (size_t)snprintf(source + used, SOURCE_CAPACITY - used,
                             "function sibling() { target(); }\n");
    ASSERT_TRUE(used < SOURCE_CAPACITY);

    CBMFileResult *r = extract_case("deep_scope", source, CBM_LANG_JAVASCRIPT, "deep.js");
    free(source);
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("deep_scope", r);
    CHECK_COUNT("deep_scope", "deepest_definition", definition_count(r, "f64"), 1);
    CHECK_COUNT("deep_scope", "sibling_definition", definition_count(r, "sibling"), 1);
    CHECK_COUNT("deep_scope", "target_call_total", call_count(r, NULL, "target"), 2);
    CHECK_COUNT("deep_scope", "penultimate_scope_not_inherited", call_count(r, "f63", "target"), 0);
    CHECK_COUNT("deep_scope", "deepest_call_attribution", call_count(r, "f64", "target"), 1);
    CHECK_COUNT("deep_scope", "sibling_pop_recovery_control", call_count(r, "sibling", "target"),
                1);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* Declarations and simple assignment targets are not references. True reads
 * remain usages, including the read side of compound assignments and updates.
 * Distinct names prevent one role from falsely satisfying another assertion. */
TEST(repro_usage_declarations_are_not_references) {
    static const char source[] = "function run(unusedParameter: number,\n"
                                 "             usedParameter: number): number {\n"
                                 "  let assignmentOnly: number;\n"
                                 "  assignmentOnly = 1;\n"
                                 "  let localValue = usedParameter + usedParameter;\n"
                                 "  let compoundValue = 0;\n"
                                 "  compoundValue += 1;\n"
                                 "  let updatedValue = 0;\n"
                                 "  updatedValue++;\n"
                                 "  return localValue + localValue;\n"
                                 "}\n";
    CBMFileResult *r = extract_case("declaration_usage", source, CBM_LANG_TYPESCRIPT, "main.ts");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("declaration_usage", r);
    CHECK_COUNT("declaration_usage", "run_definition", definition_count(r, "run"), 1);
    CHECK_COUNT("declaration_usage", "unused_parameter_not_usage",
                usage_count(r, "run", "unusedParameter"), 0);
    CHECK_COUNT("declaration_usage", "assignment_only_binding_not_usage",
                usage_count(r, "run", "assignmentOnly"), 0);
    CHECK_COUNT("declaration_usage", "used_parameter_reads", usage_count(r, "run", "usedParameter"),
                2);
    CHECK_COUNT("declaration_usage", "local_value_reads", usage_count(r, "run", "localValue"), 2);
    CHECK_COUNT("declaration_usage", "compound_assignment_reads_value",
                usage_count(r, "run", "compoundValue"), 1);
    CHECK_COUNT("declaration_usage", "update_reads_value", usage_count(r, "run", "updatedValue"),
                1);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* ObjectScript's typed receiver map must not change either ownership or
 * CALL/USAGE roles: the method is the exact callee and the parameter passed to
 * it remains one value usage in Sample.Behavior.run. */
TEST(repro_call_scope_objectscript_typed_receiver_exact_owner) {
    static const char source[] =
        "Class Sample.Behavior Extends %RegisteredObject\n"
        "{\n"
        "Method run(target As Sample.Target, watched As %String) As %Status\n"
        "{\n"
        "    Do target.accept(watched)\n"
        "    Quit\n"
        "}\n"
        "}\n";
    static const char caller_qn[] = "repro.Behavior.Sample.Behavior.run";
    static const char callee[] = "Sample.Target.accept";
    CBMFileResult *r = extract_case("objectscript_typed_receiver", source,
                                    CBM_LANG_OBJECTSCRIPT_UDL, "Behavior.cls");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("objectscript_typed_receiver", r);
    CHECK_COUNT("objectscript_typed_receiver", "exact_run_definition",
                exact_definition_count(r, "run", caller_qn), 1);
    CHECK_COUNT("objectscript_typed_receiver", "exact_call_owner_and_target",
                exact_call_count(r, caller_qn, callee), 1);
    CHECK_COUNT("objectscript_typed_receiver", "argument_usage",
                exact_usage_count(r, caller_qn, "watched"), 1);
    CHECK_COUNT("objectscript_typed_receiver", "argument_not_call",
                exact_call_count(r, caller_qn, "watched"), 0);
    CHECK_COUNT("objectscript_typed_receiver", "callee_not_usage",
                exact_usage_count(r, caller_qn, "accept"), 0);
    CHECK_COUNT("objectscript_typed_receiver", "one_call", r->calls.count, 1);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

/* A parameterized ObjectScript routine is represented by a procedure wrapper
 * around its tag. Calls in the body still belong to the tag's Function node,
 * not to the module and not to the following tag. */
TEST(repro_call_scope_objectscript_routine_tag_exact_owner) {
    static const char source[] = "run(watched)\n"
                                 "    Do accept(watched)\n"
                                 "    Quit\n"
                                 "accept(value)\n"
                                 "    Quit value\n";
    static const char caller_qn[] = "repro.Behavior.run";
    CBMFileResult *r = extract_case("objectscript_routine_scope", source,
                                    CBM_LANG_OBJECTSCRIPT_ROUTINE, "Behavior.mac");
    ASSERT_NOT_NULL(r);
    int failures = 0;
    CHECK_CLEAN("objectscript_routine_scope", r);
    CHECK_COUNT("objectscript_routine_scope", "exact_run_definition",
                exact_definition_count(r, "run", caller_qn), 1);
    CHECK_COUNT("objectscript_routine_scope", "exact_call_owner_and_target",
                exact_call_count(r, caller_qn, "accept"), 1);
    CHECK_COUNT("objectscript_routine_scope", "argument_usage",
                exact_usage_count(r, caller_qn, "watched"), 1);
    CHECK_COUNT("objectscript_routine_scope", "argument_not_call",
                exact_call_count(r, caller_qn, "watched"), 0);
    CHECK_COUNT("objectscript_routine_scope", "callee_not_usage",
                exact_usage_count(r, caller_qn, "accept"), 0);
    CHECK_COUNT("objectscript_routine_scope", "one_call", r->calls.count, 1);
    cbm_free_result(r);
    ASSERT_EQ(failures, 0);
    PASS();
}

SUITE(repro_call_scope_usages) {
    RUN_TEST(repro_call_scope_same_spelling_callee_and_argument);
    RUN_TEST(repro_call_scope_nested_calls_retain_each_argument);
    RUN_TEST(repro_call_scope_inline_callback_body_usage);
    RUN_TEST(repro_call_scope_member_and_computed_components);
    RUN_TEST(repro_call_scope_constructor_and_generic_arguments);
    RUN_TEST(repro_call_scope_c_function_pointer_invocation_and_arguments);
    RUN_TEST(repro_call_scope_cpp_function_pointer_invocation_and_arguments);
    RUN_TEST(repro_call_scope_python_keyword_argument);
    RUN_TEST(repro_call_scope_python_with_body_usage);
    RUN_TEST(repro_call_scope_rust_macro_argument);
    RUN_TEST(repro_call_scope_elixir_binary_operator_operand);
    RUN_TEST(repro_call_scope_fortran_keyword_argument);
    RUN_TEST(repro_fortran_subroutine_call_is_extracted);
    RUN_TEST(repro_call_scope_kotlin_property_navigation);
    RUN_TEST(repro_call_scope_php_first_class_callable);
    RUN_TEST(repro_scope_stack_preserves_deepest_function);
    RUN_TEST(repro_usage_declarations_are_not_references);
    RUN_TEST(repro_call_scope_objectscript_typed_receiver_exact_owner);
    RUN_TEST(repro_call_scope_objectscript_routine_tag_exact_owner);
}

#undef CHECK_CLEAN
#undef CHECK_COUNT
