/*
 * repro_reference_precision.c — occurrence- and target-exact reference edges.
 *
 * These are valid-syntax, production-pipeline contracts. Explicit reference
 * syntax earns CALL_REFERENCE only when the semantic pass proves an exact,
 * materialized callable target at that occurrence. Properties, dynamic names,
 * shadowed locals without graph nodes, and unimported definitions must not be
 * promoted by a same-name fallback. Ordinary calls have the same occurrence
 * requirement when two receiver types expose the same method name.
 */
#include "test_framework.h"
#include "repro_harness.h"
#include "../resolver_quality.h"
#include "lsp/rust_lsp.h"
#include "lsp/ts_lsp.h"
#include "pipeline/lsp_resolve.h"

#include <store/store.h>

#include <stdio.h>
#include <string.h>

/* These named gaps retain the original source relationships in the denominator.
 * Supported-contract controls keep their original required assertions. Recovery of any listed gap
 * fails until independently reviewed and promoted to REQUIRED. Every migrated fixture also requires
 * a separate exact-call control through the same full MCP indexing lifecycle. */
typedef struct {
    const char *test;
    RQ_Expected expected;
} RP_QualityGolden;
static const RFile rp_quality_control = {"rp_quality_control.py",
                                         "def rp_quality_target():\n    pass\n"
                                         "def rp_quality_caller():\n    rp_quality_target()\n"};
static const RP_QualityGolden rp_quality_goldens[] = {
    {"repro_kotlin_property_reference_does_not_borrow_same_name_function",
     {.case_id = "kotlin_property_reference_does_not_borrow_same_name_function-1",
      .site = {.file = "Use.kt", .caller = "propertyReference", .start_byte = 80, .end_byte = 87},
      .relation = "USAGE",
      .target = {.file = "Holder.kt",
                 .suffix = "Holder.handler",
                 .label = "Variable",
                 .declaration_line = 1,
                 .declaration_text = "class Holder { val handler: Int = 1 }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_property_reference_does_not_borrow_same_name_function-1",
      .why = "Kotlin property-reference receiver and property-value proof is not implemented by "
             "the direct-method bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_property_reference_does_not_borrow_same_name_function",
     {.case_id = "kotlin_property_reference_does_not_borrow_same_name_function-2",
      .site = {.file = "Use.kt", .caller = "propertyReference", .start_byte = 30, .end_byte = 36},
      .relation = "USAGE",
      .target = {.file = "Holder.kt",
                 .suffix = "Holder",
                 .label = "Class",
                 .declaration_line = 1,
                 .declaration_text = "class Holder { val handler: Int = 1 }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_property_reference_does_not_borrow_same_name_function-2",
      .why = "Kotlin property-reference receiver and property-value proof is not implemented by "
             "the direct-method bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_constructor_reference_requires_concrete_constructor",
     {.case_id = "kotlin_constructor_reference_requires_concrete_constructor-1",
      .site = {.file = "Constructors.kt",
               .caller = "explicitReference",
               .start_byte = 88,
               .end_byte = 96},
      .relation = "CALL_REFERENCE",
      .target = {.file = "Constructors.kt",
                 .suffix = "Explicit.Explicit",
                 .label = "Method",
                 .declaration_line = 1,
                 .declaration_text = "class Explicit { constructor(seed: Int) {} }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_constructor_reference_requires_concrete_constructor-1",
      .why = "Kotlin constructor references and implicit class-value references require a "
             "dedicated audited proof bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_constructor_reference_requires_concrete_constructor",
     {.case_id = "kotlin_constructor_reference_requires_concrete_constructor-2",
      .site = {.file = "Constructors.kt",
               .caller = "implicitReference",
               .start_byte = 125,
               .end_byte = 133},
      .relation = "USAGE",
      .target = {.file = "Constructors.kt",
                 .suffix = "Implicit",
                 .label = "Class",
                 .declaration_line = 2,
                 .declaration_text = "class Implicit"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_constructor_reference_requires_concrete_constructor-2",
      .why = "Kotlin constructor references and implicit class-value references require a "
             "dedicated audited proof bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_php_static_magic_first_class_reference_targets_callstatic",
     {.case_id = "php_static_magic_first_class_reference_targets_callstatic-1",
      .site = {.file = "MagicStatic.php",
               .caller = "staticMagicReference",
               .start_byte = 192,
               .end_byte = 199},
      .relation = "CALL_REFERENCE",
      .target =
          {.file = "MagicStatic.php",
           .suffix = "MagicStatic.__callStatic",
           .label = "Method",
           .declaration_line = 3,
           .declaration_text =
               "  public static function __callStatic(string $name, array $arguments): mixed {"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "php_static_magic_first_class_reference_targets_callstatic-1",
      .why = "PHP magic first-class callable dispatch has no audited callable-reference bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_php_typed_invokable_first_class_reference_targets_invoke",
     {.case_id = "php_typed_invokable_first_class_reference_targets_invoke-1",
      .site = {.file = "Invokable.php",
               .caller = "invokableReference",
               .start_byte = 156,
               .end_byte = 164},
      .relation = "CALL_REFERENCE",
      .target = {.file = "Invokable.php",
                 .suffix = "Invokable.__invoke",
                 .label = "Method",
                 .declaration_line = 3,
                 .declaration_text =
                     "  public function __invoke(int $value): int { return $value; }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "php_typed_invokable_first_class_reference_targets_invoke-1",
      .why =
          "PHP invokable-object first-class callable dispatch requires audited __invoke identity.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_python_callable_alias_invocation_targets_exact_value",
     {.case_id = "python_callable_alias_invocation_targets_exact_value-1",
      .site = {.file = "aliases.py", .caller = "aliasCaller", .start_byte = 176, .end_byte = 186},
      .relation = "CALLS",
      .target = {.file = "aliases.py",
                 .suffix = "aliases.actual",
                 .label = "Function",
                 .declaration_line = 4,
                 .declaration_text = "def actual() -> int:"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "python_callable_alias_invocation_targets_exact_value-1",
      .why = "Callable assignment and indirect invocation require exact target flow through a "
             "local alias; ordinary name lookup must not recover the shadowed declaration.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_python_callable_alias_invocation_targets_exact_value",
     {.case_id = "python_callable_alias_invocation_targets_exact_value-2",
      .site = {.file = "aliases.py", .caller = "aliasCaller", .start_byte = 158, .end_byte = 164},
      .relation = "CALL_REFERENCE",
      .target = {.file = "aliases.py",
                 .suffix = "aliases.actual",
                 .label = "Function",
                 .declaration_line = 4,
                 .declaration_text = "def actual() -> int:"},
      .mode = RQ_REQUIRED}},
    {"repro_rust_callable_alias_invocation_targets_exact_value",
     {.case_id = "rust_callable_alias_invocation_targets_exact_value-1",
      .site = {.file = "aliases.rs", .caller = "aliasCaller", .start_byte = 122, .end_byte = 132},
      .relation = "CALLS",
      .target = {.file = "aliases.rs",
                 .suffix = "aliases.actual",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "fn actual() -> i32 { 2 }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "rust_callable_alias_invocation_targets_exact_value-1",
      .why = "Callable assignment and indirect invocation require exact target flow through a "
             "local alias; ordinary name lookup must not recover the shadowed declaration.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_rust_callable_alias_invocation_targets_exact_value",
     {.case_id = "rust_callable_alias_invocation_targets_exact_value-2",
      .site = {.file = "aliases.rs", .caller = "aliasCaller", .start_byte = 110, .end_byte = 116},
      .relation = "CALL_REFERENCE",
      .target = {.file = "aliases.rs",
                 .suffix = "aliases.actual",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "fn actual() -> i32 { 2 }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "rust_callable_alias_invocation_targets_exact_value-2",
      .why = "Callable assignment and indirect invocation require exact target flow through a "
             "local alias; ordinary name lookup must not recover the shadowed declaration.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_callable_alias_invocation_targets_exact_value",
     {.case_id = "kotlin_callable_alias_invocation_targets_exact_value-1",
      .site = {.file = "Aliases.kt", .caller = "aliasCaller", .start_byte = 106, .end_byte = 116},
      .relation = "CALLS",
      .target = {.file = "Aliases.kt",
                 .suffix = "Aliases.actual",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "fun actual(): Int = 2"},
      .mode = RQ_REQUIRED,
      .gap_id = "kotlin_callable_alias_invocation_targets_exact_value-1",
      .why = "Callable assignment and indirect invocation require exact target flow through a "
             "local alias; ordinary name lookup must not recover the shadowed declaration.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_callable_alias_invocation_targets_exact_value",
     {.case_id = "kotlin_callable_alias_invocation_targets_exact_value-2",
      .site = {.file = "Aliases.kt", .caller = "aliasCaller", .start_byte = 90, .end_byte = 96},
      .relation = "CALL_REFERENCE",
      .target = {.file = "Aliases.kt",
                 .suffix = "Aliases.actual",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "fun actual(): Int = 2"},
      .mode = RQ_REQUIRED,
      .gap_id = "kotlin_callable_alias_invocation_targets_exact_value-2",
      .why = "Callable assignment and indirect invocation require exact target flow through a "
             "local alias; ordinary name lookup must not recover the shadowed declaration.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_callable_alias_invocation_targets_exact_value",
     {.case_id = "csharp_callable_alias_invocation_targets_exact_value-1",
      .site = {.file = "Aliases.cs", .caller = "aliasCaller", .start_byte = 177, .end_byte = 187},
      .relation = "CALLS",
      .target = {.file = "Aliases.cs",
                 .suffix = "Aliases.actual",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static int actual() { return 2; }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_callable_alias_invocation_targets_exact_value-1",
      .why = "Callable assignment and indirect invocation require exact target flow through a "
             "local alias; ordinary name lookup must not recover the shadowed declaration.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_callable_alias_invocation_targets_exact_value",
     {.case_id = "csharp_callable_alias_invocation_targets_exact_value-2",
      .site = {.file = "Aliases.cs", .caller = "aliasCaller", .start_byte = 158, .end_byte = 164},
      .relation = "CALL_REFERENCE",
      .target = {.file = "Aliases.cs",
                 .suffix = "Aliases.actual",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static int actual() { return 2; }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_callable_alias_invocation_targets_exact_value-2",
      .why = "Callable assignment and indirect invocation require exact target flow through a "
             "local alias; ordinary name lookup must not recover the shadowed declaration.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_javascript_exact_function_value_is_call_reference",
     {.case_id = "javascript_exact_function_value_is_call_reference-1",
      .site = {.file = "refs.js", .caller = "argument", .start_byte = 80, .end_byte = 87},
      .relation = "CALL_REFERENCE",
      .target = {.file = "refs.js",
                 .suffix = "refs.handler",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "function handler() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "javascript_exact_function_value_is_call_reference-1",
      .why = "JavaScript script/global binding completeness is unavailable without an explicit "
             "module boundary.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_javascript_exact_function_value_is_call_reference",
     {.case_id = "javascript_exact_function_value_is_call_reference-2",
      .site = {.file = "refs.js", .caller = "argument", .start_byte = 73, .end_byte = 88},
      .relation = "CALLS",
      .target = {.file = "refs.js",
                 .suffix = "refs.accept",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "function accept(callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "javascript_exact_function_value_is_call_reference-2",
      .why = "JavaScript script/global binding completeness is unavailable without an explicit "
             "module boundary.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_exact_function_value_is_call_reference",
     {.case_id = "csharp_exact_function_value_is_call_reference-1",
      .site = {.file = "Refs.cs", .caller = "argument", .start_byte = 129, .end_byte = 136},
      .relation = "CALL_REFERENCE",
      .target = {.file = "Refs.cs",
                 .suffix = "Refs.handler",
                 .label = "Method",
                 .declaration_line = 3,
                 .declaration_text = "  static void handler() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_exact_function_value_is_call_reference-1",
      .why = "Delegate or coderef argument values and callable-parameter applicability require an "
             "audited proof bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_exact_function_value_is_call_reference",
     {.case_id = "csharp_exact_function_value_is_call_reference-2",
      .site = {.file = "Refs.cs", .caller = "argument", .start_byte = 122, .end_byte = 137},
      .relation = "CALLS",
      .target = {.file = "Refs.cs",
                 .suffix = "Refs.accept",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static void accept(Action callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_exact_function_value_is_call_reference-2",
      .why = "Delegate or coderef argument values and callable-parameter applicability require an "
             "audited proof bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_perl_exact_coderef_is_call_reference",
     {.case_id = "perl_exact_coderef_is_call_reference-1",
      .site = {.file = "refs.pl", .caller = "argument", .start_byte = 73, .end_byte = 82},
      .relation = "CALL_REFERENCE",
      .target = {.file = "refs.pl",
                 .suffix = "refs.handler",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "sub handler {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "perl_exact_coderef_is_call_reference-1",
      .why = "Delegate or coderef argument values and callable-parameter applicability require an "
             "audited proof bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_perl_exact_coderef_is_call_reference",
     {.case_id = "perl_exact_coderef_is_call_reference-2",
      .site = {.file = "refs.pl", .caller = "argument", .start_byte = 66, .end_byte = 83},
      .relation = "CALLS",
      .target = {.file = "refs.pl",
                 .suffix = "refs.accept",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "sub accept { my ($callback) = @_; }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "perl_exact_coderef_is_call_reference-2",
      .why = "Delegate or coderef argument values and callable-parameter applicability require an "
             "audited proof bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_rust_direct_scoped_function_value_is_exact_reference",
     {.case_id = "rust_direct_scoped_function_value_is_exact_reference-1",
      .site = {.file = "main.rs", .caller = "scoped_argument", .start_byte = 83, .end_byte = 98},
      .relation = "CALL_REFERENCE",
      .target = {.file = "target.rs",
                 .suffix = "target.handler",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "pub fn handler() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "rust_direct_scoped_function_value_is_exact_reference-1",
      .why = "Rust scoped callable-value extraction proves the target, but shared "
             "callable-reference evidence is not yet bridged.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_rust_direct_scoped_function_value_is_exact_reference",
     {.case_id = "rust_direct_scoped_function_value_is_exact_reference-2",
      .site = {.file = "main.rs", .caller = "scoped_argument", .start_byte = 76, .end_byte = 99},
      .relation = "CALLS",
      .target = {.file = "main.rs",
                 .suffix = "main.accept",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "fn accept(_callback: fn()) {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "rust_direct_scoped_function_value_is_exact_reference-2",
      .why = "Rust scoped callable-value extraction proves the target, but shared "
             "callable-reference evidence is not yet bridged.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_member_method_group_is_exact_reference",
     {.case_id = "csharp_member_method_group_is_exact_reference-1",
      .site = {.file = "MemberReference.cs",
               .caller = "memberArgument",
               .start_byte = 173,
               .end_byte = 180},
      .relation = "CALL_REFERENCE",
      .target = {.file = "MemberReference.cs",
                 .suffix = "Service.handler",
                 .label = "Method",
                 .declaration_line = 2,
                 .declaration_text = "class Service { public void handler() {} }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_member_method_group_is_exact_reference-1",
      .why = "C# callable groups require exact delegate applicability and member/callable "
             "evidence; generic arguments additionally require constructed callable identity.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_member_method_group_is_exact_reference",
     {.case_id = "csharp_member_method_group_is_exact_reference-2",
      .site = {.file = "MemberReference.cs",
               .caller = "memberArgument",
               .start_byte = 164,
               .end_byte = 181},
      .relation = "CALLS",
      .target = {.file = "MemberReference.cs",
                 .suffix = "MemberReference.accept",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static void accept(Action callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_member_method_group_is_exact_reference-2",
      .why = "C# callable groups require exact delegate applicability and member/callable "
             "evidence; generic arguments additionally require constructed callable identity.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_parenthesized_method_group_is_exact_reference",
     {.case_id = "csharp_parenthesized_method_group_is_exact_reference-1",
      .site = {.file = "ParenthesizedReference.cs",
               .caller = "parenthesizedArgument",
               .start_byte = 161,
               .end_byte = 168},
      .relation = "CALL_REFERENCE",
      .target = {.file = "ParenthesizedReference.cs",
                 .suffix = "ParenthesizedReference.handler",
                 .label = "Method",
                 .declaration_line = 3,
                 .declaration_text = "  static void handler() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_parenthesized_method_group_is_exact_reference-1",
      .why = "C# callable groups require exact delegate applicability and member/callable "
             "evidence; generic arguments additionally require constructed callable identity.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_parenthesized_method_group_is_exact_reference",
     {.case_id = "csharp_parenthesized_method_group_is_exact_reference-2",
      .site = {.file = "ParenthesizedReference.cs",
               .caller = "parenthesizedArgument",
               .start_byte = 153,
               .end_byte = 170},
      .relation = "CALLS",
      .target = {.file = "ParenthesizedReference.cs",
                 .suffix = "ParenthesizedReference.accept",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static void accept(Action callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_parenthesized_method_group_is_exact_reference-2",
      .why = "C# callable groups require exact delegate applicability and member/callable "
             "evidence; generic arguments additionally require constructed callable identity.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_generic_method_group_is_exact_reference",
     {.case_id = "csharp_generic_method_group_is_exact_reference-1",
      .site = {.file = "GenericReference.cs",
               .caller = "genericArgument",
               .start_byte = 151,
               .end_byte = 158},
      .relation = "CALL_REFERENCE",
      .target = {.file = "GenericReference.cs",
                 .suffix = "GenericReference.handler",
                 .label = "Method",
                 .declaration_line = 3,
                 .declaration_text = "  static void handler<T>() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_generic_method_group_is_exact_reference-1",
      .why = "C# callable groups require exact delegate applicability and member/callable "
             "evidence; generic arguments additionally require constructed callable identity.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_generic_method_group_is_exact_reference",
     {.case_id = "csharp_generic_method_group_is_exact_reference-2",
      .site = {.file = "GenericReference.cs",
               .caller = "genericArgument",
               .start_byte = 144,
               .end_byte = 164},
      .relation = "CALLS",
      .target = {.file = "GenericReference.cs",
                 .suffix = "GenericReference.accept",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static void accept(Action callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_generic_method_group_is_exact_reference-2",
      .why = "C# callable groups require exact delegate applicability and member/callable "
             "evidence; generic arguments additionally require constructed callable identity.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_python_module_type_alias_shadow_fails_closed",
     {.case_id = "python_module_type_alias_shadow_fails_closed-1",
      .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 109, .end_byte = 125},
      .relation = "CALLS",
      .target = {.file = "use.py",
                 .suffix = "use.accept",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "def accept(fn):"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "python_module_type_alias_shadow_fails_closed-1",
      .why = "Python type_alias_statement is outside the audited binding allowlist; its namespace "
             "effects currently suppress independent direct calls.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_rust_cross_file_exact_function_value_is_call_reference",
     {.case_id = "rust_cross_file_exact_function_value_is_call_reference-1",
      .site = {.file = "main.rs", .caller = "crossArgument", .start_byte = 91, .end_byte = 98},
      .relation = "CALL_REFERENCE",
      .target = {.file = "target.rs",
                 .suffix = "target.handler",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "pub fn handler() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "rust_cross_file_exact_function_value_is_call_reference-1",
      .why = "Cross-file callable references need audited callable-value proof; C# delegate "
             "applicability and assembly visibility also remain unproved.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_rust_cross_file_exact_function_value_is_call_reference",
     {.case_id = "rust_cross_file_exact_function_value_is_call_reference-2",
      .site = {.file = "main.rs", .caller = "crossArgument", .start_byte = 84, .end_byte = 99},
      .relation = "CALLS",
      .target = {.file = "main.rs",
                 .suffix = "main.accept",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "fn accept(_callback: fn()) {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "rust_cross_file_exact_function_value_is_call_reference-2",
      .why = "Cross-file callable references need audited callable-value proof; C# delegate "
             "applicability and assembly visibility also remain unproved.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_cross_file_exact_function_value_is_call_reference",
     {.case_id = "csharp_cross_file_exact_function_value_is_call_reference-1",
      .site = {.file = "Use.cs", .caller = "crossArgument", .start_byte = 127, .end_byte = 134},
      .relation = "CALL_REFERENCE",
      .target = {.file = "Target.cs",
                 .suffix = "Target.handler",
                 .label = "Method",
                 .declaration_line = 1,
                 .declaration_text = "class Target { public static void handler() {} }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_cross_file_exact_function_value_is_call_reference-1",
      .why = "Cross-file callable references need audited callable-value proof; C# delegate "
             "applicability and assembly visibility also remain unproved.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_cross_file_exact_function_value_is_call_reference",
     {.case_id = "csharp_cross_file_exact_function_value_is_call_reference-2",
      .site = {.file = "Use.cs", .caller = "crossArgument", .start_byte = 120, .end_byte = 135},
      .relation = "CALLS",
      .target = {.file = "Use.cs",
                 .suffix = "Use.accept",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static void accept(Action callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_cross_file_exact_function_value_is_call_reference-2",
      .why = "Cross-file callable references need audited callable-value proof; C# delegate "
             "applicability and assembly visibility also remain unproved.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_cross_file_callable_alias_invocation_targets_exact_value",
     {.case_id = "csharp_cross_file_callable_alias_invocation_targets_exact_value-1",
      .site = {.file = "Use.cs", .caller = "crossAliasCaller", .start_byte = 163, .end_byte = 173},
      .relation = "CALLS",
      .target = {.file = "Target.cs",
                 .suffix = "Target.actual",
                 .label = "Method",
                 .declaration_line = 1,
                 .declaration_text = "class Target { public static int actual() { return 2; } }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_cross_file_callable_alias_invocation_targets_exact_value-1",
      .why = "C# imported callable alias flow and assembly visibility are outside the audited "
             "direct-member bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_cross_file_callable_alias_invocation_targets_exact_value",
     {.case_id = "csharp_cross_file_callable_alias_invocation_targets_exact_value-2",
      .site = {.file = "Use.cs", .caller = "crossAliasCaller", .start_byte = 144, .end_byte = 150},
      .relation = "CALL_REFERENCE",
      .target = {.file = "Target.cs",
                 .suffix = "Target.actual",
                 .label = "Method",
                 .declaration_line = 1,
                 .declaration_text = "class Target { public static int actual() { return 2; } }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_cross_file_callable_alias_invocation_targets_exact_value-2",
      .why = "C# imported callable alias flow and assembly visibility are outside the audited "
             "direct-member bridge.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_receiver_bound_alias_invocation_targets_exact_method",
     {.case_id = "kotlin_receiver_bound_alias_invocation_targets_exact_method-1",
      .site = {.file = "ReceiverAlias.kt",
               .caller = "receiverAliasCaller",
               .start_byte = 110,
               .end_byte = 119},
      .relation = "CALLS",
      .target = {.file = "ReceiverAlias.kt",
                 .suffix = NULL,
                 .label = "Method",
                 .declaration_line = 1,
                 .declaration_text = "class Service { fun run(): Int = 2 }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_receiver_bound_alias_invocation_targets_exact_method-1",
      .why = "Kotlin receiver-bound callable aliases require exact receiver and captured-target "
             "flow; implicit constructor identity is not materialized.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_receiver_bound_alias_invocation_targets_exact_method",
     {.case_id = "kotlin_receiver_bound_alias_invocation_targets_exact_method-2",
      .site = {.file = "ReceiverAlias.kt",
               .caller = "receiverAliasCaller",
               .start_byte = 159,
               .end_byte = 169},
      .relation = "CALLS",
      .target = {.file = "ReceiverAlias.kt",
                 .suffix = "Service.run",
                 .label = "Method",
                 .declaration_line = 1,
                 .declaration_text = "class Service { fun run(): Int = 2 }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_receiver_bound_alias_invocation_targets_exact_method-2",
      .why = "Kotlin receiver-bound callable aliases require exact receiver and captured-target "
             "flow; implicit constructor identity is not materialized.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_receiver_bound_alias_invocation_targets_exact_method",
     {.case_id = "kotlin_receiver_bound_alias_invocation_targets_exact_method-3",
      .site = {.file = "ReceiverAlias.kt",
               .caller = "receiverAliasCaller",
               .start_byte = 146,
               .end_byte = 149},
      .relation = "CALL_REFERENCE",
      .target = {.file = "ReceiverAlias.kt",
                 .suffix = "Service.run",
                 .label = "Method",
                 .declaration_line = 1,
                 .declaration_text = "class Service { fun run(): Int = 2 }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_receiver_bound_alias_invocation_targets_exact_method-3",
      .why = "Kotlin receiver-bound callable aliases require exact receiver and captured-target "
             "flow; implicit constructor identity is not materialized.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_unknown_imported_property_is_not_callable_reference",
     {.case_id = "kotlin_unknown_imported_property_is_not_callable_reference-1",
      .site = {.file = "Use.kt",
               .caller = "importedPropertyReference",
               .start_byte = 107,
               .end_byte = 114},
      .relation = "USAGE",
      .target = {.file = "Values.kt",
                 .suffix = "Values.handler",
                 .label = "Variable",
                 .declaration_line = 2,
                 .declaration_text = "val handler: Int = 1"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_unknown_imported_property_is_not_callable_reference-1",
      .why = "Kotlin imported property identities require audited value-binding evidence separate "
             "from callable-reference proof.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_cpp_complex_function_value_stays_usage",
     {.case_id = "cpp_complex_function_value_stays_usage-1",
      .site = {.file = "complex.cpp", .caller = "complex", .start_byte = 127, .end_byte = 161},
      .relation = "CALLS",
      .target = {.file = "complex.cpp",
                 .suffix = "complex.accept",
                 .label = "Function",
                 .declaration_line = 4,
                 .declaration_text = "void accept(Callback callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "cpp_complex_function_value_stays_usage-1",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_cpp_complex_function_value_stays_usage",
     {.case_id = "cpp_complex_function_value_stays_usage-2",
      .site = {.file = "complex.cpp", .caller = "complex", .start_byte = 141, .end_byte = 148},
      .relation = "USAGE",
      .target = {.file = "complex.cpp",
                 .suffix = "complex.handler",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "void handler() {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "cpp_complex_function_value_stays_usage-2",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_cpp_complex_function_value_stays_usage",
     {.case_id = "cpp_complex_function_value_stays_usage-3",
      .site = {.file = "complex.cpp", .caller = "complex", .start_byte = 151, .end_byte = 160},
      .relation = "USAGE",
      .target = {.file = "complex.cpp",
                 .suffix = "complex.alternate",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "void alternate() {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "cpp_complex_function_value_stays_usage-3",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_cuda_complex_function_value_stays_usage",
     {.case_id = "cuda_complex_function_value_stays_usage-1",
      .site = {.file = "complex.cu", .caller = "complex", .start_byte = 127, .end_byte = 161},
      .relation = "CALLS",
      .target = {.file = "complex.cu",
                 .suffix = "complex.accept",
                 .label = "Function",
                 .declaration_line = 4,
                 .declaration_text = "void accept(Callback callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "cuda_complex_function_value_stays_usage-1",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_cuda_complex_function_value_stays_usage",
     {.case_id = "cuda_complex_function_value_stays_usage-2",
      .site = {.file = "complex.cu", .caller = "complex", .start_byte = 141, .end_byte = 148},
      .relation = "USAGE",
      .target = {.file = "complex.cu",
                 .suffix = "complex.handler",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "void handler() {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "cuda_complex_function_value_stays_usage-2",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_cuda_complex_function_value_stays_usage",
     {.case_id = "cuda_complex_function_value_stays_usage-3",
      .site = {.file = "complex.cu", .caller = "complex", .start_byte = 151, .end_byte = 160},
      .relation = "USAGE",
      .target = {.file = "complex.cu",
                 .suffix = "complex.alternate",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "void alternate() {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "cuda_complex_function_value_stays_usage-3",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_complex_function_value_stays_usage",
     {.case_id = "csharp_complex_function_value_stays_usage-1",
      .site = {.file = "Complex.cs", .caller = "complex", .start_byte = 159, .end_byte = 193},
      .relation = "CALLS",
      .target = {.file = "Complex.cs",
                 .suffix = "Refs.accept",
                 .label = "Method",
                 .declaration_line = 5,
                 .declaration_text = "  static void accept(Action callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_complex_function_value_stays_usage-1",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_complex_function_value_stays_usage",
     {.case_id = "csharp_complex_function_value_stays_usage-2",
      .site = {.file = "Complex.cs", .caller = "complex", .start_byte = 173, .end_byte = 180},
      .relation = "USAGE",
      .target = {.file = "Complex.cs",
                 .suffix = "Refs.handler",
                 .label = "Method",
                 .declaration_line = 3,
                 .declaration_text = "  static void handler() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_complex_function_value_stays_usage-2",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_csharp_complex_function_value_stays_usage",
     {.case_id = "csharp_complex_function_value_stays_usage-3",
      .site = {.file = "Complex.cs", .caller = "complex", .start_byte = 183, .end_byte = 192},
      .relation = "USAGE",
      .target = {.file = "Complex.cs",
                 .suffix = "Refs.alternate",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static void alternate() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "csharp_complex_function_value_stays_usage-3",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_java_complex_method_reference_stays_usage",
     {.case_id = "java_complex_method_reference_stays_usage-1",
      .site = {.file = "Refs.java", .caller = "complex", .start_byte = 177, .end_byte = 223},
      .relation = "CALLS",
      .target = {.file = "Refs.java",
                 .suffix = "Refs.accept",
                 .label = "Method",
                 .declaration_line = 5,
                 .declaration_text = "  static void accept(Task callback) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "java_complex_method_reference_stays_usage-1",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_java_complex_method_reference_stays_usage",
     {.case_id = "java_complex_method_reference_stays_usage-2",
      .site = {.file = "Refs.java", .caller = "complex", .start_byte = 197, .end_byte = 204},
      .relation = "USAGE",
      .target = {.file = "Refs.java",
                 .suffix = "Refs.handler",
                 .label = "Method",
                 .declaration_line = 3,
                 .declaration_text = "  static void handler() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "java_complex_method_reference_stays_usage-2",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_java_complex_method_reference_stays_usage",
     {.case_id = "java_complex_method_reference_stays_usage-3",
      .site = {.file = "Refs.java", .caller = "complex", .start_byte = 213, .end_byte = 222},
      .relation = "USAGE",
      .target = {.file = "Refs.java",
                 .suffix = "Refs.alternate",
                 .label = "Method",
                 .declaration_line = 4,
                 .declaration_text = "  static void alternate() {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "java_complex_method_reference_stays_usage-3",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_java_complex_method_reference_stays_usage",
     {.case_id = "java_complex_method_reference_stays_usage-4",
      .site = {.file = "Refs.java", .caller = "complex", .start_byte = 191, .end_byte = 195},
      .relation = "USAGE",
      .target = {.file = "Refs.java",
                 .suffix = "Refs",
                 .label = "Class",
                 .declaration_line = 2,
                 .declaration_text = "class Refs {"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "java_complex_method_reference_stays_usage-4",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_java_complex_method_reference_stays_usage",
     {.case_id = "java_complex_method_reference_stays_usage-5",
      .site = {.file = "Refs.java", .caller = "complex", .start_byte = 207, .end_byte = 211},
      .relation = "USAGE",
      .target = {.file = "Refs.java",
                 .suffix = "Refs",
                 .label = "Class",
                 .declaration_line = 2,
                 .declaration_text = "class Refs {"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "java_complex_method_reference_stays_usage-5",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_complex_callable_reference_stays_usage",
     {.case_id = "kotlin_complex_callable_reference_stays_usage-1",
      .site = {.file = "Refs.kt", .caller = "complex", .start_byte = 101, .end_byte = 145},
      .relation = "CALLS",
      .target = {.file = "Refs.kt",
                 .suffix = "Refs.accept",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "fun accept(callback: () -> Unit) {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "kotlin_complex_callable_reference_stays_usage-1",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_complex_callable_reference_stays_usage",
     {.case_id = "kotlin_complex_callable_reference_stays_usage-2",
      .site = {.file = "Refs.kt", .caller = "complex", .start_byte = 120, .end_byte = 127},
      .relation = "USAGE",
      .target = {.file = "Refs.kt",
                 .suffix = "Refs.handler",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "fun handler() {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "kotlin_complex_callable_reference_stays_usage-2",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_kotlin_complex_callable_reference_stays_usage",
     {.case_id = "kotlin_complex_callable_reference_stays_usage-3",
      .site = {.file = "Refs.kt", .caller = "complex", .start_byte = 135, .end_byte = 144},
      .relation = "USAGE",
      .target = {.file = "Refs.kt",
                 .suffix = "Refs.alternate",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "fun alternate() {}"},
      .mode = RQ_REQUIRED,
      .gap_id = "kotlin_complex_callable_reference_stays_usage-3",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_php_complex_first_class_callable_stays_usage",
     {.case_id = "php_complex_first_class_callable_stays_usage-1",
      .site = {.file = "refs.php", .caller = "complex", .start_byte = 146, .end_byte = 191},
      .relation = "CALLS",
      .target = {.file = "refs.php",
                 .suffix = "refs.accept",
                 .label = "Function",
                 .declaration_line = 4,
                 .declaration_text = "function accept(callable $callback): void {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "php_complex_first_class_callable_stays_usage-1",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_php_complex_first_class_callable_stays_usage",
     {.case_id = "php_complex_first_class_callable_stays_usage-2",
      .site = {.file = "refs.php", .caller = "complex", .start_byte = 161, .end_byte = 168},
      .relation = "USAGE",
      .target = {.file = "refs.php",
                 .suffix = "refs.handler",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "function handler(): void {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "php_complex_first_class_callable_stays_usage-2",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_php_complex_first_class_callable_stays_usage",
     {.case_id = "php_complex_first_class_callable_stays_usage-3",
      .site = {.file = "refs.php", .caller = "complex", .start_byte = 176, .end_byte = 185},
      .relation = "USAGE",
      .target = {.file = "refs.php",
                 .suffix = "refs.alternate",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "function alternate(): void {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "php_complex_first_class_callable_stays_usage-3",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_perl_conditional_coderefs_stay_usage",
     {.case_id = "perl_conditional_coderefs_stay_usage-1",
      .site = {.file = "complex.pl", .caller = "complex", .start_byte = 99, .end_byte = 138},
      .relation = "CALLS",
      .target = {.file = "complex.pl",
                 .suffix = "complex.accept",
                 .label = "Function",
                 .declaration_line = 3,
                 .declaration_text = "sub accept { my ($callback) = @_; }"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "perl_conditional_coderefs_stay_usage-1",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_perl_conditional_coderefs_stay_usage",
     {.case_id = "perl_conditional_coderefs_stay_usage-2",
      .site = {.file = "complex.pl", .caller = "complex", .start_byte = 116, .end_byte = 123},
      .relation = "USAGE",
      .target = {.file = "complex.pl",
                 .suffix = "complex.handler",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "sub handler {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "perl_conditional_coderefs_stay_usage-2",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_perl_conditional_coderefs_stay_usage",
     {.case_id = "perl_conditional_coderefs_stay_usage-3",
      .site = {.file = "complex.pl", .caller = "complex", .start_byte = 128, .end_byte = 137},
      .relation = "USAGE",
      .target = {.file = "complex.pl",
                 .suffix = "complex.alternate",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "sub alternate {}"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "perl_conditional_coderefs_stay_usage-3",
      .why = "Conditional callable values retain genuine target mentions, but complete source "
             "binding, callable-argument applicability and value-reference proof are not yet "
             "audited for this language/form.",
      .tried = "Original full-MCP fixture retained; exact source occurrence and target declaration "
               "reviewed; raw and forbidden assertions retained."}},
    {"repro_python_local_function_shadows_imported_callable",
     {.case_id = "repro_python_local_function_shadows_imported_callable-reference",
      .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 121, .end_byte = 129},
      .relation = "CALL_REFERENCE",
      .target = {.file = "use.py",
                 .suffix = "use.callback",
                 .label = "Function",
                 .declaration_line = 2,
                 .declaration_text = "def callback():"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "python-module-binding-versions",
      .why = "Distinct module bindings and captured aliases need ordered binding versions; the "
             "shared source index conservatively treats repeated same-scope names as ambiguous.",
      .tried = "Original full-pipeline fixture retained and ordered declarations inspected. Exact "
               "source proof rejects repeated same-scope names; the registrar remains required and "
               "wrong targets stay forbidden."}},
    {"repro_python_local_function_shadows_imported_callable",
     {.case_id = "repro_python_local_function_shadows_imported_callable-registrar",
      .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 114, .end_byte = 130},
      .relation = "CALLS",
      .target = {.file = "use.py",
                 .suffix = "use.accept",
                 .label = "Function",
                 .declaration_line = 4,
                 .declaration_text = "def accept(fn):"},
      .mode = RQ_REQUIRED}},
    {"repro_python_alias_capture_precedes_later_import_rebinding",
     {.case_id = "repro_python_alias_capture_precedes_later_import_rebinding-reference",
      .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 138, .end_byte = 143},
      .relation = "CALL_REFERENCE",
      .target = {.file = "use.py",
                 .suffix = "use.callback",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "def callback():"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "python-module-binding-versions",
      .why = "Distinct module bindings and captured aliases need ordered binding versions; the "
             "shared source index conservatively treats repeated same-scope names as ambiguous.",
      .tried = "Original full-pipeline fixture retained and ordered declarations inspected. Exact "
               "source proof rejects repeated same-scope names; the registrar remains required and "
               "wrong targets stay forbidden."}},
    {"repro_python_alias_capture_precedes_later_import_rebinding",
     {.case_id = "repro_python_alias_capture_precedes_later_import_rebinding-registrar",
      .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 131, .end_byte = 144},
      .relation = "CALLS",
      .target = {.file = "use.py",
                 .suffix = "use.accept",
                 .label = "Function",
                 .declaration_line = 5,
                 .declaration_text = "def accept(fn):"},
      .mode = RQ_REQUIRED}},
    {"repro_python_later_import_restores_imported_callable_identity",
     {.case_id = "repro_python_later_import_restores_imported_callable_identity-reference",
      .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 121, .end_byte = 129},
      .relation = "CALL_REFERENCE",
      .target = {.file = "target.py",
                 .suffix = "target.handler",
                 .label = "Function",
                 .declaration_line = 1,
                 .declaration_text = "def handler():"},
      .mode = RQ_KNOWN_GAP,
      .gap_id = "python-module-binding-versions",
      .why = "Distinct module bindings and captured aliases need ordered binding versions; the "
             "shared source index conservatively treats repeated same-scope names as ambiguous.",
      .tried = "Original full-pipeline fixture retained and ordered declarations inspected. Exact "
               "source proof rejects repeated same-scope names; the registrar remains required and "
               "wrong targets stay forbidden."}},
    {"repro_python_later_import_restores_imported_callable_identity",
     {.case_id = "repro_python_later_import_restores_imported_callable_identity-registrar",
      .site = {.file = "use.py", .caller = "crossArgument", .start_byte = 114, .end_byte = 130},
      .relation = "CALLS",
      .target = {.file = "use.py",
                 .suffix = "use.accept",
                 .label = "Function",
                 .declaration_line = 4,
                 .declaration_text = "def accept(fn):"},
      .mode = RQ_REQUIRED}},
};

static cbm_store_t *rp_quality_index(RProj *project, const RFile *files, int count,
                                     const char *quality_case) {
    if (!quality_case)
        return rh_index_files(project, files, count);
    if (count < 1 || count > 8) {
        memset(project, 0, sizeof(*project));
        return NULL;
    }
    RFile inputs[9];
    for (int i = 0; i < count; i++)
        inputs[i] = files[i];
    inputs[count] = rp_quality_control;
    return rh_index_files(project, inputs, count + 1);
}
static int rp_quality_check(cbm_store_t *store, const RProj *project, const RFile *files, int count,
                            const char *quality_case) {
    if (!quality_case)
        return 0;
    ASSERT_TRUE(count > 0 && count <= 8);
    RQ_Source sources[9] = {0};
    for (int i = 0; i < count; i++)
        sources[i] = (RQ_Source){.path = files[i].name, .text = files[i].content};
    sources[count] =
        (RQ_Source){.path = rp_quality_control.name, .text = rp_quality_control.content};
    RQ_Expected expected[16];
    size_t n = 0;
    for (size_t i = 0; i < sizeof(rp_quality_goldens) / sizeof(rp_quality_goldens[0]); i++) {
        if (!strcmp(rp_quality_goldens[i].test, quality_case)) {
            ASSERT_TRUE(n + 1 < sizeof(expected) / sizeof(expected[0]));
            expected[n++] = rp_quality_goldens[i].expected;
        }
    }
    ASSERT_TRUE(n > 0);
    expected[n++] = (RQ_Expected){.case_id = "reference-quality-exact-call-control",
                                  .site = {.file = "rp_quality_control.py",
                                           .caller = "rp_quality_caller",
                                           .needle = "rp_quality_target()",
                                           .occurrence_index = 1},
                                  .relation = "CALLS",
                                  .target = {.file = "rp_quality_control.py",
                                             .suffix = "rp_quality_target",
                                             .label = "Function",
                                             .declaration_line = 1,
                                             .declaration_text = "def rp_quality_target():"},
                                  .mode = RQ_REQUIRED};
    RQ_Report report;
    bool ok =
        rq_check_project_declarations(store, project->project, quality_case, sources,
                                      (size_t)count + 1, expected, n, NULL, 0, &report, stderr);
    ASSERT_EQ(report.genuine, n);
    ASSERT_TRUE(ok);
    return 0;
}

static int rp_qn_ends_with(const char *qn, const char *suffix) {
    if (!qn || !suffix)
        return 0;
    size_t qn_len = strlen(qn);
    size_t suffix_len = strlen(suffix);
    return qn_len >= suffix_len && strcmp(qn + qn_len - suffix_len, suffix) == 0;
}

static int rp_extracts_clean(const char *source, CBMLanguage language, const char *filename) {
    CBMFileResult *result =
        cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL, NULL);
    if (!result)
        return 0;
    int clean = !result->has_error && !result->parse_incomplete;
    cbm_free_result(result);
    return clean;
}

static int rp_node_count(cbm_store_t *store, const char *project, const char *label,
                         const char *name, const char *qn_suffix) {
    cbm_node_t *nodes = NULL;
    int count = 0;
    if (cbm_store_find_nodes_by_label(store, project, label, &nodes, &count) != CBM_STORE_OK)
        return -1;
    int matches = 0;
    for (int i = 0; i < count; i++) {
        if (name && (!nodes[i].name || strcmp(nodes[i].name, name) != 0))
            continue;
        if (qn_suffix && !rp_qn_ends_with(nodes[i].qualified_name, qn_suffix))
            continue;
        matches++;
    }
    cbm_store_free_nodes(nodes, count);
    return matches;
}

static int rp_edge_count(cbm_store_t *store, const char *project, const char *edge_type,
                         const char *source_name, const char *target_label, const char *target_name,
                         const char *target_qn_suffix) {
    cbm_edge_t *edges = NULL;
    int edge_count = 0;
    if (cbm_store_find_edges_by_type(store, project, edge_type, &edges, &edge_count) !=
        CBM_STORE_OK) {
        return -1;
    }

    int matches = 0;
    for (int i = 0; i < edge_count; i++) {
        cbm_node_t source = {0};
        cbm_node_t target = {0};
        int source_ok =
            cbm_store_find_node_by_id(store, edges[i].source_id, &source) == CBM_STORE_OK;
        int target_ok =
            cbm_store_find_node_by_id(store, edges[i].target_id, &target) == CBM_STORE_OK;
        if (source_ok && target_ok &&
            (!source_name || (source.name && strcmp(source.name, source_name) == 0)) &&
            (!target_label || (target.label && strcmp(target.label, target_label) == 0)) &&
            (!target_name || (target.name && strcmp(target.name, target_name) == 0)) &&
            (!target_qn_suffix || rp_qn_ends_with(target.qualified_name, target_qn_suffix))) {
            matches++;
        }
        cbm_node_free_fields(&source);
        cbm_node_free_fields(&target);
    }
    cbm_store_free_edges(edges, edge_count);
    return matches;
}

static int rp_is_constructor_node(const cbm_node_t *node, const char *class_name) {
    if (!node || !node->label || !node->name || !node->qualified_name || !class_name)
        return 0;
    int callable_label = strcmp(node->label, "Constructor") == 0 ||
                         strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0;
    int constructor_name =
        strcmp(node->name, class_name) == 0 || strcmp(node->name, "constructor") == 0;
    char class_segment[128];
    snprintf(class_segment, sizeof(class_segment), ".%s", class_name);
    return callable_label && constructor_name && strstr(node->qualified_name, class_segment);
}

static int rp_constructor_node_count(cbm_store_t *store, const char *project,
                                     const char *class_name) {
    static const char *const labels[] = {"Constructor", "Function", "Method"};
    int matches = 0;
    for (size_t li = 0; li < sizeof(labels) / sizeof(labels[0]); li++) {
        cbm_node_t *nodes = NULL;
        int count = 0;
        if (cbm_store_find_nodes_by_label(store, project, labels[li], &nodes, &count) !=
            CBM_STORE_OK) {
            return -1;
        }
        for (int i = 0; i < count; i++) {
            if (rp_is_constructor_node(&nodes[i], class_name))
                matches++;
        }
        cbm_store_free_nodes(nodes, count);
    }
    return matches;
}

static int rp_raw_usage_count(const CBMFileResult *result, const char *caller, const char *target,
                              CBMUsageKind kind) {
    int count = 0;
    for (int i = 0; result && i < result->usages.count; i++) {
        const CBMUsage *usage = &result->usages.items[i];
        const char *owner = usage->enclosing_func_qn;
        const char *owner_name = owner ? strrchr(owner, '.') : NULL;
        owner_name = owner_name ? owner_name + 1 : owner;
        if (usage->kind == kind && usage->ref_name && owner_name &&
            strcmp(owner_name, caller) == 0 && strcmp(usage->ref_name, target) == 0) {
            count++;
        }
    }
    return count;
}

static int rp_raw_source_callable_def_count(const CBMFileResult *result, const char *source_path) {
    /* Kotlin raw results also contain synthetic kotlin.Any method defs. This
     * helper verifies the fixture's own source declarations, not builtins. */
    int count = 0;
    for (int i = 0; result && i < result->defs.count; i++) {
        const char *file_path = result->defs.items[i].file_path;
        if (!file_path || !source_path || strcmp(file_path, source_path) != 0)
            continue;
        const char *label = result->defs.items[i].label;
        if (label && (strcmp(label, "Function") == 0 || strcmp(label, "Method") == 0 ||
                      strcmp(label, "Constructor") == 0)) {
            count++;
        }
    }
    return count;
}

TEST(repro_kotlin_property_reference_does_not_borrow_same_name_function) {
    static const RFile files[] = {
        {"Holder.kt", "class Holder { val handler: Int = 1 }\n"},
        {"Functions.kt", "fun handler(): Unit {}\n"},
        {"Use.kt", "fun propertyReference(holder: Holder): kotlin.reflect.KProperty0<Int> = "
                   "holder::handler\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_KOTLIN, files[1].name));
    ASSERT_TRUE(rp_extracts_clean(files[2].content, CBM_LANG_KOTLIN, files[2].name));

    const char *quality_case = "repro_kotlin_property_reference_does_not_borrow_same_name_function";
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 3, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin property-reference collision fixture did not produce a graph store");
    }
    int property_node =
        rp_node_count(store, project.project, "Variable", "handler", ".Holder.handler");
    int function_node =
        rp_node_count(store, project.project, "Function", "handler", ".Functions.handler");
    int wrong_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "propertyReference", "Function",
                      "handler", ".Functions.handler");
    int property_promoted =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "propertyReference", "Variable",
                      "handler", ".Holder.handler");
    int quality_rc = rp_quality_check(store, &project, files, 3, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_GTE(property_node, 1);
    ASSERT_GTE(function_node, 1);

    ASSERT_EQ(wrong_reference, 0);
    ASSERT_EQ(property_promoted, 0);
    PASS();
}

TEST(repro_kotlin_constructor_reference_requires_concrete_constructor) {
    /* Inferred return types keep the Class occurrence unique to `::Class`;
     * spelling the class again in an annotation would be a legitimate USAGE. */
    static const RFile files[] = {{
        "Constructors.kt",
        "class Explicit { constructor(seed: Int) {} }\n"
        "class Implicit\n"
        "fun explicitReference() = ::Explicit\n"
        "fun implicitReference() = ::Implicit\n",
    }};
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));

    const char *quality_case = "repro_kotlin_constructor_reference_requires_concrete_constructor";
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin constructor-reference fixture did not produce a graph store");
    }
    int concrete_constructor = rp_constructor_node_count(store, project.project, "Explicit");
    int explicit_class_usage = rp_edge_count(store, project.project, "USAGE", "explicitReference",
                                             "Class", "Explicit", ".Explicit");
    int implicit_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                           "implicitReference", NULL, NULL, NULL);
    int implicit_class_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "implicitReference", "Class",
                      "Implicit", ".Implicit");
    int quality_rc = rp_quality_check(store, &project, files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_GTE(concrete_constructor, 1);

    ASSERT_EQ(explicit_class_usage, 0);
    ASSERT_EQ(implicit_reference, 0);
    ASSERT_EQ(implicit_class_reference, 0);

    PASS();
}

TEST(repro_kotlin_local_reference_never_binds_same_name_top_level) {
    static const RFile files[] = {
        {"Other.kt", "fun handler(): Unit {}\n"},
        {"Use.kt", "fun localReference(): Unit {\n"
                   "  fun handler(): Unit {}\n"
                   "  val callback = ::handler\n"
                   "  callback()\n"
                   "}\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_KOTLIN, files[1].name));

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin local callable-reference fixture did not produce a graph store");
    }
    int top_level_node =
        rp_node_count(store, project.project, "Function", "handler", ".Other.handler");
    int local_node = rp_node_count(store, project.project, "Function", "handler", ".Use.handler");
    int local_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "localReference",
                                        "Function", "handler", ".Use.handler");
    int any_precise_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "localReference", NULL, NULL, NULL);
    int wrong_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "localReference",
                                        "Function", "handler", ".Other.handler");
    int wrong_usage = rp_edge_count(store, project.project, "USAGE", "localReference", "Function",
                                    "handler", ".Other.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(top_level_node, 1);
    ASSERT_EQ(wrong_reference, 0);
    ASSERT_EQ(wrong_usage, 0);
    /* A future local-function node may be referenced precisely. Until such a
     * node exists, the explicit syntax must remain non-precise. */
    ASSERT_TRUE(local_node > 0 ? local_reference == 1 : any_precise_reference == 0);
    PASS();
}

TEST(repro_php_dynamic_first_class_terminal_is_not_precise) {
    static const char source[] =
        "<?php\n"
        "class DynamicTarget { public function concrete(): void {} }\n"
        "function method(): void {}\n"
        "function dynamicReference(DynamicTarget $object, string $method): callable {\n"
        "  return $object->$method(...);\n"
        "}\n";
    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PHP, "repro",
                                          "Dynamic.php", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    int ordinary_terminal = rp_raw_usage_count(raw, "dynamicReference", "$method", CBM_USAGE_VALUE);
    int precise_terminal =
        rp_raw_usage_count(raw, "dynamicReference", "$method", CBM_USAGE_CALL_REFERENCE);
    cbm_free_result(raw);
    ASSERT_EQ(ordinary_terminal, 1);
    ASSERT_EQ(precise_terminal, 0);

    RProj project;
    cbm_store_t *store = rh_index(&project, "Dynamic.php", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP dynamic first-class callable fixture did not produce a graph store");
    }
    int fabricated_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                             "dynamicReference", NULL, NULL, NULL);
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "dynamicReference",
                                        "Function", "method", ".Dynamic.method");
    rh_cleanup(&project, store);

    ASSERT_EQ(fabricated_reference, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_php_static_magic_first_class_reference_targets_callstatic) {
    static const char source[] =
        "<?php\n"
        "class MagicStatic {\n"
        "  public static function __callStatic(string $name, array $arguments): mixed {\n"
        "    return null;\n"
        "  }\n"
        "}\n"
        "function staticMagicReference(): callable { return MagicStatic::missing(...); }\n";
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_PHP, "MagicStatic.php"));

    const char *quality_case = "repro_php_static_magic_first_class_reference_targets_callstatic";
    const RFile quality_files[] = {{"MagicStatic.php", source}};
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, quality_files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP __callStatic reference fixture did not produce a graph store");
    }
    int magic_node = rp_node_count(store, project.project, "Method", "__callStatic",
                                   ".MagicStatic.__callStatic");
    int missing_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                          "staticMagicReference", NULL, "missing", NULL);
    int magic_call = rp_edge_count(store, project.project, "CALLS", "staticMagicReference",
                                   "Method", "__callStatic", ".MagicStatic.__callStatic");
    int magic_usage = rp_edge_count(store, project.project, "USAGE", "staticMagicReference",
                                    "Method", "__callStatic", ".MagicStatic.__callStatic");
    int quality_rc = rp_quality_check(store, &project, quality_files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_GTE(magic_node, 1);

    ASSERT_EQ(missing_reference, 0);
    ASSERT_EQ(magic_call, 0);
    ASSERT_EQ(magic_usage, 0);
    PASS();
}

TEST(repro_php_typed_invokable_first_class_reference_targets_invoke) {
    static const char source[] =
        "<?php\n"
        "class Invokable {\n"
        "  public function __invoke(int $value): int { return $value; }\n"
        "}\n"
        "function invokableReference(Invokable $handler): callable { return $handler(...); }\n";
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_PHP, "Invokable.php"));

    const char *quality_case = "repro_php_typed_invokable_first_class_reference_targets_invoke";
    const RFile quality_files[] = {{"Invokable.php", source}};
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, quality_files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("PHP invokable-object reference fixture did not produce a graph store");
    }
    int invoke_node =
        rp_node_count(store, project.project, "Method", "__invoke", ".Invokable.__invoke");
    int invoke_call = rp_edge_count(store, project.project, "CALLS", "invokableReference", "Method",
                                    "__invoke", ".Invokable.__invoke");
    int invoke_usage = rp_edge_count(store, project.project, "USAGE", "invokableReference",
                                     "Method", "__invoke", ".Invokable.__invoke");
    int quality_rc = rp_quality_check(store, &project, quality_files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_GTE(invoke_node, 1);

    ASSERT_EQ(invoke_call, 0);
    ASSERT_EQ(invoke_usage, 0);
    PASS();
}

TEST(repro_java_same_name_receiver_calls_keep_occurrence_identity) {
    static const char source[] = "class Alpha { void handle() {} }\n"
                                 "class Beta { void handle() {} }\n"
                                 "class Calls {\n"
                                 "  static void callBoth(Alpha alpha, Beta beta) {\n"
                                 "    alpha.handle();\n"
                                 "    beta.handle();\n"
                                 "  }\n"
                                 "}\n";
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_JAVA, "Calls.java"));

    RProj project;
    cbm_store_t *store = rh_index(&project, "Calls.java", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Java same-name receiver-call fixture did not produce a graph store");
    }
    int alpha_node = rp_node_count(store, project.project, "Method", "handle", ".Alpha.handle");
    int beta_node = rp_node_count(store, project.project, "Method", "handle", ".Beta.handle");
    int alpha_call = rp_edge_count(store, project.project, "CALLS", "callBoth", "Method", "handle",
                                   ".Alpha.handle");
    int beta_call = rp_edge_count(store, project.project, "CALLS", "callBoth", "Method", "handle",
                                  ".Beta.handle");
    rh_cleanup(&project, store);

    ASSERT_GTE(alpha_node, 1);
    ASSERT_GTE(beta_node, 1);
    ASSERT_EQ(alpha_call, 1);
    ASSERT_EQ(beta_call, 1);
    PASS();
}

TEST(repro_typescript_function_value_upgrades_only_with_exact_semantic_target) {
    static const RFile files[] = {
        {"target.ts", "export function handler(): void {}\n"},
        {"use.ts", "import { handler } from './target';\n"
                   "function accept(callback: () => void): void {}\n"
                   "function consume(value: number): void {}\n"
                   "export function exactReference(): void { accept(handler); }\n"
                   "export function shadowedValue(handler: number): void { consume(handler); }\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_TYPESCRIPT, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_TYPESCRIPT, files[1].name));

    CBMFileResult *raw =
        cbm_extract_file(files[1].content, (int)strlen(files[1].content), CBM_LANG_TYPESCRIPT,
                         "repro", files[1].name, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_EQ(rp_raw_usage_count(raw, "exactReference", "handler", CBM_USAGE_VALUE), 1);
    ASSERT_EQ(rp_raw_usage_count(raw, "exactReference", "handler", CBM_USAGE_CALL_REFERENCE), 0);
    ASSERT_EQ(rp_raw_usage_count(raw, "shadowedValue", "handler", CBM_USAGE_VALUE), 1);
    cbm_free_result(raw);

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("TypeScript function-value precision fixture did not produce a graph store");
    }
    int function_node =
        rp_node_count(store, project.project, "Function", "handler", ".target.handler");
    int exact_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "exactReference",
                                        "Function", "handler", ".target.handler");
    int exact_usage = rp_edge_count(store, project.project, "USAGE", "exactReference", "Function",
                                    "handler", ".target.handler");
    int exact_call = rp_edge_count(store, project.project, "CALLS", "exactReference", "Function",
                                   "handler", ".target.handler");
    int shadow_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "shadowedValue",
                                         "Function", "handler", ".target.handler");
    int shadow_usage = rp_edge_count(store, project.project, "USAGE", "shadowedValue", "Function",
                                     "handler", ".target.handler");
    int shadow_call = rp_edge_count(store, project.project, "CALLS", "shadowedValue", "Function",
                                    "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(function_node, 1);
    ASSERT_EQ(exact_reference, 1);
    ASSERT_EQ(exact_usage, 0);
    ASSERT_EQ(exact_call, 0);
    ASSERT_EQ(shadow_reference, 0);
    ASSERT_EQ(shadow_usage, 0);
    ASSERT_EQ(shadow_call, 0);
    PASS();
}

TEST(repro_kotlin_empty_cross_filter_does_not_fail_open) {
    static const RFile files[] = {
        {"src/main/kotlin/unrelated/Unrelated.kt", "package unrelated\n"
                                                   "fun handler(): Unit {}\n"},
        {"src/main/kotlin/consumer/Use.kt", "package consumer\n"
                                            "val leakedReference: () -> Unit = ::handler\n"},
    };
    ASSERT_TRUE(rp_extracts_clean(files[0].content, CBM_LANG_KOTLIN, files[0].name));
    ASSERT_TRUE(rp_extracts_clean(files[1].content, CBM_LANG_KOTLIN, files[1].name));

    CBMFileResult *consumer =
        cbm_extract_file(files[1].content, (int)strlen(files[1].content), CBM_LANG_KOTLIN, "repro",
                         files[1].name, 0, NULL, NULL);
    ASSERT_NOT_NULL(consumer);
    int own_callable_defs = rp_raw_source_callable_def_count(consumer, files[1].name);
    cbm_free_result(consumer);
    ASSERT_EQ(own_callable_defs, 0);

    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin empty cross-filter fixture did not produce a graph store");
    }
    int unrelated_node =
        rp_node_count(store, project.project, "Function", "handler", ".Unrelated.handler");
    int leaked_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", NULL, "Function",
                                         "handler", ".Unrelated.handler");
    int leaked_usage = rp_edge_count(store, project.project, "USAGE", NULL, "Function", "handler",
                                     ".Unrelated.handler");
    int leaked_call = rp_edge_count(store, project.project, "CALLS", NULL, "Function", "handler",
                                    ".Unrelated.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(unrelated_node, 1);
    ASSERT_EQ(leaked_reference, 0);
    ASSERT_EQ(leaked_usage, 0);
    ASSERT_EQ(leaked_call, 0);
    PASS();
}

static int rp_assert_callable_alias_graph_quality(const char *filename, const char *source,
                                                  CBMLanguage language, const char *caller,
                                                  const char *callable_label,
                                                  const char *quality_case) {
    ASSERT_TRUE(rp_extracts_clean(source, language, filename));

    const RFile quality_files[] = {{filename, source}};
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, quality_files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("callable-alias fixture did not produce a graph store");
    }
    int shadowed_node = rp_node_count(store, project.project, callable_label, "shadowed", NULL);
    int actual_node = rp_node_count(store, project.project, callable_label, "actual", NULL);
    int wrong_call =
        rp_edge_count(store, project.project, "CALLS", caller, callable_label, "shadowed", NULL);
    int exact_call =
        rp_edge_count(store, project.project, "CALLS", caller, callable_label, "actual", NULL);
    int quality_rc = rp_quality_check(store, &project, quality_files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_GTE(shadowed_node, 1);
    ASSERT_GTE(actual_node, 1);
    ASSERT_EQ(wrong_call, 0);
    if (!quality_case)
        ASSERT_EQ(exact_call, 1);
    return 0;
}

static int rp_assert_callable_alias_graph(const char *filename, const char *source,
                                          CBMLanguage language, const char *caller,
                                          const char *callable_label) {
    return rp_assert_callable_alias_graph_quality(filename, source, language, caller,
                                                  callable_label, NULL);
}

TEST(repro_go_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "package aliases\n"
                                 "func shadowed() int { return 1 }\n"
                                 "func actual() int { return 2 }\n"
                                 "func aliasCaller() int {\n"
                                 "  shadowed := actual\n"
                                 "  return shadowed()\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph("aliases.go", source, CBM_LANG_GO, "aliasCaller",
                                            "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "from typing import Callable\n"
                                 "def shadowed() -> int:\n"
                                 "    return 1\n"
                                 "def actual() -> int:\n"
                                 "    return 2\n"
                                 "def aliasCaller() -> int:\n"
                                 "    shadowed: Callable[[], int] = actual\n"
                                 "    return shadowed()\n";
    int rc = rp_assert_callable_alias_graph_quality(
        "aliases.py", source, CBM_LANG_PYTHON, "aliasCaller", "Function",
        "repro_python_callable_alias_invocation_targets_exact_value");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "fn shadowed() -> i32 { 1 }\n"
                                 "fn actual() -> i32 { 2 }\n"
                                 "fn aliasCaller() -> i32 {\n"
                                 "    let shadowed: fn() -> i32 = actual;\n"
                                 "    shadowed()\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph_quality(
        "aliases.rs", source, CBM_LANG_RUST, "aliasCaller", "Function",
        "repro_rust_callable_alias_invocation_targets_exact_value");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "fun shadowed(): Int = 1\n"
                                 "fun actual(): Int = 2\n"
                                 "fun aliasCaller(): Int {\n"
                                 "  val shadowed = ::actual\n"
                                 "  return shadowed()\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph_quality(
        "Aliases.kt", source, CBM_LANG_KOTLIN, "aliasCaller", "Function",
        "repro_kotlin_callable_alias_invocation_targets_exact_value");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_callable_alias_invocation_targets_exact_value) {
    static const char source[] = "using System;\n"
                                 "class Aliases {\n"
                                 "  static int shadowed() { return 1; }\n"
                                 "  static int actual() { return 2; }\n"
                                 "  static int aliasCaller() {\n"
                                 "    Func<int> shadowed = actual;\n"
                                 "    return shadowed();\n"
                                 "  }\n"
                                 "}\n";
    int rc = rp_assert_callable_alias_graph_quality(
        "Aliases.cs", source, CBM_LANG_CSHARP, "aliasCaller", "Method",
        "repro_csharp_callable_alias_invocation_targets_exact_value");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_exact_function_value_reference_quality(const char *filename,
                                                            const char *source,
                                                            CBMLanguage language,
                                                            const char *callable_label,
                                                            const char *quality_case) {
    ASSERT_TRUE(rp_extracts_clean(source, language, filename));

    const RFile quality_files[] = {{filename, source}};
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, quality_files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("function-value fixture did not produce a graph store");
    }
    int target_node = rp_node_count(store, project.project, callable_label, "handler", NULL);
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "argument",
                                          callable_label, "handler", NULL);
    int ordinary_usage =
        rp_edge_count(store, project.project, "USAGE", "argument", callable_label, "handler", NULL);
    int fabricated_call =
        rp_edge_count(store, project.project, "CALLS", "argument", callable_label, "handler", NULL);
    int quality_rc = rp_quality_check(store, &project, quality_files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_GTE(target_node, 1);
    if (!quality_case)
        ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    return 0;
}

static int rp_assert_exact_function_value_reference(const char *filename, const char *source,
                                                    CBMLanguage language,
                                                    const char *callable_label) {
    return rp_assert_exact_function_value_reference_quality(filename, source, language,
                                                            callable_label, NULL);
}

TEST(repro_go_exact_function_value_is_call_reference) {
    static const char source[] = "package refs\n"
                                 "func handler() {}\n"
                                 "func accept(callback func()) {}\n"
                                 "func argument() { accept(handler) }\n";
    int rc = rp_assert_exact_function_value_reference("refs.go", source, CBM_LANG_GO, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_exact_function_value_is_call_reference) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def argument():\n"
                                 "    accept(handler)\n";
    int rc =
        rp_assert_exact_function_value_reference("refs.py", source, CBM_LANG_PYTHON, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_javascript_exact_function_value_is_call_reference) {
    static const char source[] = "function handler() {}\n"
                                 "function accept(callback) {}\n"
                                 "function argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference_quality(
        "refs.js", source, CBM_LANG_JAVASCRIPT, "Function",
        "repro_javascript_exact_function_value_is_call_reference");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_c_exact_function_value_is_call_reference) {
    static const char source[] = "typedef void (*callback_t)(void);\n"
                                 "void handler(void) {}\n"
                                 "void accept(callback_t callback) {}\n"
                                 "void argument(void) { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.c", source, CBM_LANG_C, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cpp_exact_function_value_is_call_reference) {
    static const char source[] = "using Callback = void (*)();\n"
                                 "void handler() {}\n"
                                 "void accept(Callback callback) {}\n"
                                 "void argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.cpp", source, CBM_LANG_CPP, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_exact_function_value_is_call_reference) {
    static const char source[] = "fn handler() {}\n"
                                 "fn accept(_callback: fn()) {}\n"
                                 "fn argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.rs", source, CBM_LANG_RUST, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_direct_scoped_function_value_is_exact_reference) {
    static const RFile files[] = {
        {"target.rs", "pub fn handler() {}\n"},
        {"decoy.rs", "pub fn handler() {}\n"},
        {"main.rs", "mod target;\n"
                    "mod decoy;\n"
                    "fn accept(_callback: fn()) {}\n"
                    "fn scoped_argument() { accept(target::handler); }\n"},
    };
    const char *source = files[2].content;
    for (size_t i = 0; i < sizeof(files) / sizeof(files[0]); i++) {
        ASSERT_TRUE(rp_extracts_clean(files[i].content, CBM_LANG_RUST, files[i].name));
    }

    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST, "repro",
                                          files[2].name, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);

    CBMRustLSPDef defs[3];
    memset(defs, 0, sizeof(defs));
    defs[0].qualified_name = "repro.target.handler";
    defs[0].short_name = "handler";
    defs[0].label = "Function";
    defs[0].def_module_qn = "repro.target";
    defs[1].qualified_name = "repro.decoy.handler";
    defs[1].short_name = "handler";
    defs[1].label = "Function";
    defs[1].def_module_qn = "repro.decoy";
    defs[2].qualified_name = "repro.main.accept";
    defs[2].short_name = "accept";
    defs[2].label = "Function";
    defs[2].def_module_qn = "repro.main";
    cbm_run_rust_lsp_cross(&raw->arena, source, (int)strlen(source), "repro.main", defs, 3, NULL,
                           NULL, 0, NULL, &raw->resolved_calls);

    const char *site = strstr(source, "target::handler");
    ASSERT_NOT_NULL(site);
    uint32_t start = (uint32_t)(site - source);
    uint32_t end = start + (uint32_t)strlen("target::handler");
    const CBMUsage *site_usage = NULL;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->ref_name &&
            strcmp(usage->ref_name, "target::handler") == 0 && usage->enclosing_func_qn &&
            rp_qn_ends_with(usage->enclosing_func_qn, "scoped_argument") &&
            usage->site_start_byte == start && usage->site_end_byte == end) {
            site_usage = usage;
            break;
        }
    }

    int target_reference_rows = 0;
    int decoy_reference_rows = 0;
    int target_invocation_rows = 0;
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (!resolved->caller_qn || !rp_qn_ends_with(resolved->caller_qn, "scoped_argument") ||
            resolved->site_start_byte != start || resolved->site_end_byte != end ||
            !resolved->callee_qn) {
            continue;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE &&
            rp_qn_ends_with(resolved->callee_qn, ".target.handler")) {
            target_reference_rows++;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE &&
            rp_qn_ends_with(resolved->callee_qn, ".decoy.handler")) {
            decoy_reference_rows++;
        }
        if (resolved->kind == CBM_RESOLVED_INVOCATION &&
            rp_qn_ends_with(resolved->callee_qn, ".target.handler")) {
            target_invocation_rows++;
        }
    }
    const CBMResolvedCall *joined =
        site_usage ? cbm_pipeline_find_lsp_reference(&raw->resolved_calls, site_usage, false)
                   : NULL;
    bool semantic_join_is_exact = joined && joined->kind == CBM_RESOLVED_CALL_REFERENCE &&
                                  joined->callee_qn &&
                                  rp_qn_ends_with(joined->callee_qn, ".target.handler");
    bool has_site_usage = site_usage != NULL;
    bool site_is_reference_candidate = site_usage && site_usage->may_be_call_reference;
    cbm_free_result(raw);

    ASSERT_TRUE(has_site_usage);
    ASSERT_EQ(target_reference_rows, 1);
    ASSERT_EQ(decoy_reference_rows, 0);
    ASSERT_EQ(target_invocation_rows, 0);
    ASSERT_TRUE(semantic_join_is_exact);
    ASSERT_TRUE(site_is_reference_candidate);

    const char *quality_case = "repro_rust_direct_scoped_function_value_is_exact_reference";
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 3, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Rust scoped function-value fixture did not produce a graph store");
    }
    int target_node =
        rp_node_count(store, project.project, "Function", "handler", ".target.handler");
    int decoy_node = rp_node_count(store, project.project, "Function", "handler", ".decoy.handler");
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "scoped_argument",
                                       "Function", "accept", NULL);
    int target_usage = rp_edge_count(store, project.project, "USAGE", "scoped_argument", "Function",
                                     "handler", ".target.handler");
    int target_call = rp_edge_count(store, project.project, "CALLS", "scoped_argument", "Function",
                                    "handler", ".target.handler");
    int decoy_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "scoped_argument",
                                        "Function", "handler", ".decoy.handler");
    int decoy_usage = rp_edge_count(store, project.project, "USAGE", "scoped_argument", "Function",
                                    "handler", ".decoy.handler");
    int decoy_call = rp_edge_count(store, project.project, "CALLS", "scoped_argument", "Function",
                                   "handler", ".decoy.handler");
    int quality_rc = rp_quality_check(store, &project, files, 3, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_EQ(target_node, 1);
    ASSERT_EQ(decoy_node, 1);
    ASSERT_EQ(registrar_call, 1);

    ASSERT_EQ(target_usage, 0);
    ASSERT_EQ(target_call, 0);
    ASSERT_EQ(decoy_reference, 0);
    ASSERT_EQ(decoy_usage, 0);
    ASSERT_EQ(decoy_call, 0);
    PASS();
}

TEST(repro_csharp_exact_function_value_is_call_reference) {
    static const char source[] = "using System;\n"
                                 "class Refs {\n"
                                 "  static void handler() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void argument() { accept(handler); }\n"
                                 "}\n";
    int rc = rp_assert_exact_function_value_reference_quality(
        "Refs.cs", source, CBM_LANG_CSHARP, "Method",
        "repro_csharp_exact_function_value_is_call_reference");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_csharp_callable_value_shape_quality(const char *filename, const char *source,
                                                         const char *caller,
                                                         const char *target_qn_suffix,
                                                         const char *quality_case) {
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_CSHARP, filename));

    const RFile quality_files[] = {{filename, source}};
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, quality_files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("C# callable-value shape fixture did not produce a graph store");
    }
    int target_node = rp_node_count(store, project.project, "Method", "handler", target_qn_suffix);
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", caller,
                                          "Method", "handler", target_qn_suffix);
    int ordinary_usage = rp_edge_count(store, project.project, "USAGE", caller, "Method", "handler",
                                       target_qn_suffix);
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", caller, "Method",
                                        "handler", target_qn_suffix);
    int quality_rc = rp_quality_check(store, &project, quality_files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    if (!quality_case && (target_node != 1 || precise_reference != 1 || ordinary_usage != 0 ||
                          fabricated_call != 0)) {
        fprintf(stderr,
                "  [csharp-reference-shape] caller=%s target=%s node=%d reference=%d "
                "usage=%d call=%d\n",
                caller, target_qn_suffix, target_node, precise_reference, ordinary_usage,
                fabricated_call);
    }

    ASSERT_EQ(target_node, 1);
    if (!quality_case)
        ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    return 0;
}

/* Service.handler is the source-authored callable target. The shared gate
 * still needs audited member/delegate evidence for this exact leaf occurrence. */
TEST(repro_csharp_member_method_group_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class Service { public void handler() {} }\n"
                                 "class MemberReference {\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void memberArgument(Service s) { accept(s.handler); }\n"
                                 "}\n";
    int rc = rp_assert_csharp_callable_value_shape_quality(
        "MemberReference.cs", source, "memberArgument", ".Service.handler",
        "repro_csharp_member_method_group_is_exact_reference");
    if (rc != 0)
        return rc;
    PASS();
}

/* Parentheses do not make a single method group ambiguous.  They must not
 * prevent the exact semantic target from joining the inner raw carrier. */
TEST(repro_csharp_parenthesized_method_group_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class ParenthesizedReference {\n"
                                 "  static void handler() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void parenthesizedArgument() { accept((handler)); }\n"
                                 "}\n";
    int rc = rp_assert_csharp_callable_value_shape_quality(
        "ParenthesizedReference.cs", source, "parenthesizedArgument",
        ".ParenthesizedReference.handler",
        "repro_csharp_parenthesized_method_group_is_exact_reference");
    if (rc != 0)
        return rc;
    PASS();
}

/* An explicitly constructed generic method group is one exact callable
 * value: handler<int> has no overload or inference ambiguity in this fixture. */
TEST(repro_csharp_generic_method_group_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class GenericReference {\n"
                                 "  static void handler<T>() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void genericArgument() { accept(handler<int>); }\n"
                                 "}\n";
    int rc = rp_assert_csharp_callable_value_shape_quality(
        "GenericReference.cs", source, "genericArgument", ".GenericReference.handler",
        "repro_csharp_generic_method_group_is_exact_reference");
    if (rc != 0)
        return rc;
    PASS();
}

/* Both imported static classes contain an equally applicable method group.
 * C# must fail closed: the parser keeps an ordinary value usage, but the
 * semantic pass and graph may not select either target precisely. */
TEST(repro_csharp_ambiguous_using_static_method_group_is_not_precise) {
    static const char source[] = "using System;\n"
                                 "using static A;\n"
                                 "using static B;\n"
                                 "class A { public static void handler() {} }\n"
                                 "class B { public static void handler() {} }\n"
                                 "class AmbiguousStaticReference {\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void ambiguousArgument() { accept(handler); }\n"
                                 "}\n";

    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_CSHARP, "repro",
                                          "AmbiguousStaticReference.cs", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument = strstr(source, "accept(handler)");
    ASSERT_NOT_NULL(argument);
    uint32_t site_start = (uint32_t)(argument - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("handler");
    int ordinary_usage = rp_raw_usage_count(raw, "ambiguousArgument", "handler", CBM_USAGE_VALUE);
    int precise_rows = 0;
    int a_rows = 0;
    int b_rows = 0;
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (resolved->kind != CBM_RESOLVED_CALL_REFERENCE || !resolved->caller_qn ||
            !rp_qn_ends_with(resolved->caller_qn, "ambiguousArgument") ||
            resolved->site_start_byte != site_start || resolved->site_end_byte != site_end) {
            continue;
        }
        precise_rows++;
        if (rp_qn_ends_with(resolved->callee_qn, ".A.handler"))
            a_rows++;
        if (rp_qn_ends_with(resolved->callee_qn, ".B.handler"))
            b_rows++;
    }
    cbm_free_result(raw);

    RProj project;
    cbm_store_t *store = rh_index(&project, "AmbiguousStaticReference.cs", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("ambiguous C# using-static fixture did not produce a graph store");
    }
    int a_node = rp_node_count(store, project.project, "Method", "handler", ".A.handler");
    int b_node = rp_node_count(store, project.project, "Method", "handler", ".B.handler");
    int a_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "ambiguousArgument",
                                    "Method", "handler", ".A.handler");
    int b_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "ambiguousArgument",
                                    "Method", "handler", ".B.handler");
    int a_call = rp_edge_count(store, project.project, "CALLS", "ambiguousArgument", "Method",
                               "handler", ".A.handler");
    int b_call = rp_edge_count(store, project.project, "CALLS", "ambiguousArgument", "Method",
                               "handler", ".B.handler");
    rh_cleanup(&project, store);

    ASSERT_GTE(ordinary_usage, 1);
    ASSERT_EQ(a_node, 1);
    ASSERT_EQ(b_node, 1);
    ASSERT_EQ(precise_rows, 0);
    ASSERT_EQ(a_rows, 0);
    ASSERT_EQ(b_rows, 0);
    ASSERT_EQ(a_reference, 0);
    ASSERT_EQ(b_reference, 0);
    ASSERT_EQ(a_call, 0);
    ASSERT_EQ(b_call, 0);
    PASS();
}

TEST(repro_cuda_exact_function_value_is_call_reference) {
    static const char source[] = "using Callback = void (*)();\n"
                                 "void handler() {}\n"
                                 "void accept(Callback callback) {}\n"
                                 "void argument() { accept(handler); }\n";
    int rc = rp_assert_exact_function_value_reference("refs.cu", source, CBM_LANG_CUDA, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

typedef struct {
    const char *tag;
    const char *filename;
    const char *source;
    CBMLanguage language;
} RPExactReferenceSiteCase;

static int rp_check_exact_function_value_lsp_site(const RPExactReferenceSiteCase *c) {
    CBMFileResult *result = cbm_extract_file(c->source, (int)strlen(c->source), c->language,
                                             "repro", c->filename, 0, NULL, NULL);
    if (!result) {
        fprintf(stderr, "  [reference-site] lang=%s invariant=extract_result\n", c->tag);
        return 1;
    }
    const char *argument = strstr(c->source, "accept(handler)");
    const uint32_t start = argument ? (uint32_t)(argument - c->source) + 7U : 0U;
    const uint32_t end = start + (uint32_t)strlen("handler");
    const bool ts_family = c->language == CBM_LANG_JAVASCRIPT ||
                           c->language == CBM_LANG_TYPESCRIPT || c->language == CBM_LANG_TSX;
    CBMArena cross_arena;
    CBMResolvedCallArray cross_resolved = {0};
    if (ts_family) {
        /* TS/JS value references intentionally wait for the project-wide pass:
         * only that registry proves that the target is materialized. Exercise
         * that production phase while retaining the exact source-site oracle. */
        CBMLSPDef defs[] = {
            {.qualified_name = "repro.refs.handler",
             .short_name = "handler",
             .label = "Function",
             .def_module_qn = "repro.refs",
             .lang = c->language},
            {.qualified_name = "repro.refs.accept",
             .short_name = "accept",
             .label = "Function",
             .def_module_qn = "repro.refs",
             .lang = c->language},
            {.qualified_name = "repro.refs.argument",
             .short_name = "argument",
             .label = "Function",
             .def_module_qn = "repro.refs",
             .lang = c->language},
        };
        cbm_arena_init(&cross_arena);
        cbm_run_ts_lsp_cross(&cross_arena, c->source, (int)strlen(c->source), "repro.refs",
                             c->language == CBM_LANG_JAVASCRIPT, c->language == CBM_LANG_TSX, false,
                             defs, (int)(sizeof(defs) / sizeof(defs[0])), NULL, NULL, 0, NULL,
                             &cross_resolved);
    }
    const CBMResolvedCallArray *resolved_calls =
        ts_family ? &cross_resolved : &result->resolved_calls;
    int exact_references = 0;
    int handler_invocations = 0;
    for (int i = 0; i < resolved_calls->count; i++) {
        const CBMResolvedCall *resolved = &resolved_calls->items[i];
        if (!resolved->caller_qn || !rp_qn_ends_with(resolved->caller_qn, "argument") ||
            !resolved->callee_qn || !rp_qn_ends_with(resolved->callee_qn, "handler")) {
            continue;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE && resolved->site_start_byte == start &&
            resolved->site_end_byte == end) {
            exact_references++;
        }
        if (resolved->kind == CBM_RESOLVED_INVOCATION && resolved->site_start_byte == start &&
            resolved->site_end_byte == end) {
            handler_invocations++;
        }
    }
    int failures = 0;
    if (result->has_error || result->parse_incomplete) {
        fprintf(stderr, "  [reference-site] lang=%s invariant=valid_fixture\n", c->tag);
        failures++;
    }
    if (exact_references != 1) {
        fprintf(stderr,
                "  [reference-site] lang=%s invariant=exact_reference expected=1 actual=%d "
                "site=%u:%u\n",
                c->tag, exact_references, start, end);
        failures++;
    }
    if (handler_invocations != 0) {
        fprintf(stderr, "  [reference-site] lang=%s invariant=argument_not_invocation actual=%d\n",
                c->tag, handler_invocations);
        failures++;
    }
    if (ts_family) {
        cbm_arena_destroy(&cross_arena);
    }
    cbm_free_result(result);
    return failures;
}

TEST(repro_exact_function_value_lsp_sites) {
    static const RPExactReferenceSiteCase cases[] = {
        {"go", "refs.go",
         "package refs\n"
         "func handler() {}\n"
         "func accept(callback func()) {}\n"
         "func argument() { accept(handler) }\n",
         CBM_LANG_GO},
        {"python", "refs.py",
         "def handler():\n    pass\n"
         "def accept(callback):\n    pass\n"
         "def argument():\n    accept(handler)\n",
         CBM_LANG_PYTHON},
        {"javascript", "refs.js",
         "function handler() {}\n"
         "function accept(callback) {}\n"
         "function argument() { accept(handler); }\n",
         CBM_LANG_JAVASCRIPT},
        {"typescript", "refs.ts",
         "function handler(): void {}\n"
         "function accept(callback: () => void): void {}\n"
         "function argument(): void { accept(handler); }\n",
         CBM_LANG_TYPESCRIPT},
        {"tsx", "refs.tsx",
         "function handler(): void {}\n"
         "function accept(callback: () => void): void {}\n"
         "function argument(): void { accept(handler); }\n",
         CBM_LANG_TSX},
        {"c", "refs.c",
         "typedef void (*callback_t)(void);\n"
         "void handler(void) {}\n"
         "void accept(callback_t callback) {}\n"
         "void argument(void) { accept(handler); }\n",
         CBM_LANG_C},
        {"cpp", "refs.cpp",
         "using Callback = void (*)();\n"
         "void handler() {}\n"
         "void accept(Callback callback) {}\n"
         "void argument() { accept(handler); }\n",
         CBM_LANG_CPP},
        {"cuda", "refs.cu",
         "using Callback = void (*)();\n"
         "void handler() {}\n"
         "void accept(Callback callback) {}\n"
         "void argument() { accept(handler); }\n",
         CBM_LANG_CUDA},
        {"rust", "refs.rs",
         "fn handler() {}\n"
         "fn accept(_callback: fn()) {}\n"
         "fn argument() { accept(handler); }\n",
         CBM_LANG_RUST},
        {"csharp", "Refs.cs",
         "using System;\n"
         "class Refs {\n"
         "  static void handler() {}\n"
         "  static void accept(Action callback) {}\n"
         "  static void argument() { accept(handler); }\n"
         "}\n",
         CBM_LANG_CSHARP},
    };
    int failures = 0;
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        failures += rp_check_exact_function_value_lsp_site(&cases[i]);
    }
    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_typescript_exact_bound_method_is_call_reference) {
    static const char source[] =
        "class Service { handler(): void {} }\n"
        "function accept(callback: () => void): void {}\n"
        "function argument(service: Service): void { accept(service.handler); }\n";
    int rc =
        rp_assert_exact_function_value_reference("bound.ts", source, CBM_LANG_TYPESCRIPT, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_tsx_exact_bound_method_is_call_reference) {
    static const char source[] =
        "class Service { handler(): void {} }\n"
        "function accept(callback: () => void): void {}\n"
        "function argument(service: Service): void { accept(service.handler); }\n";
    int rc = rp_assert_exact_function_value_reference("bound.tsx", source, CBM_LANG_TSX, "Method");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_perl_exact_coderef_is_call_reference) {
    static const char source[] = "sub handler {}\n"
                                 "sub accept { my ($callback) = @_; }\n"
                                 "sub argument { accept(\\&handler); }\n";
    int rc = rp_assert_exact_function_value_reference_quality(
        "refs.pl", source, CBM_LANG_PERL, "Function", "repro_perl_exact_coderef_is_call_reference");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_cross_file_exact_function_value_quality(const RFile *files, int file_count,
                                                             CBMLanguage language,
                                                             const char *callable_label,
                                                             const char *quality_case) {
    for (int i = 0; i < file_count; i++) {
        ASSERT_TRUE(rp_extracts_clean(files[i].content, language, files[i].name));
    }
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, file_count, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("cross-file function-value fixture did not produce a graph store");
    }
    int target_node = rp_node_count(store, project.project, callable_label, "handler", NULL);
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                       callable_label, "accept", NULL);
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                          callable_label, "handler", NULL);
    int ordinary_usage = rp_edge_count(store, project.project, "USAGE", "crossArgument",
                                       callable_label, "handler", NULL);
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        callable_label, "handler", NULL);
    int quality_rc = rp_quality_check(store, &project, files, file_count, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    if (!quality_case && (target_node < 1 || registrar_call != 1 || precise_reference != 1 ||
                          ordinary_usage != 0 || fabricated_call != 0)) {
        fprintf(stderr,
                "  [cross-reference] language=%d target=%d registrar=%d reference=%d "
                "usage=%d call=%d\n",
                (int)language, target_node, registrar_call, precise_reference, ordinary_usage,
                fabricated_call);
    }

    ASSERT_GTE(target_node, 1);
    if (!quality_case)
        ASSERT_EQ(registrar_call, 1);
    if (!quality_case)
        ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    return 0;
}

static int rp_assert_cross_file_exact_function_value(const RFile *files, int file_count,
                                                     CBMLanguage language,
                                                     const char *callable_label) {
    return rp_assert_cross_file_exact_function_value_quality(files, file_count, language,
                                                             callable_label, NULL);
}

TEST(repro_go_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.go", "package refs\nfunc handler() {}\n"},
        {"use.go", "package refs\n"
                   "func accept(callback func()) {}\n"
                   "func crossArgument() { accept(handler) }\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value(files, 2, CBM_LANG_GO, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler\n"
                   "def accept(callback):\n    pass\n"
                   "def crossArgument():\n    accept(handler)\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value(files, 2, CBM_LANG_PYTHON, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_aliased_import_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    for (int i = 0; i < 2; i++)
        ASSERT_TRUE(rp_extracts_clean(files[i].content, CBM_LANG_PYTHON, files[i].name));
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python aliased from-import fixture did not produce a graph store");
    }
    int target_node =
        rp_node_count(store, project.project, "Function", "handler", ".target.handler");
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                       "accept", ".use.accept");
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                          "Function", "handler", ".target.handler");
    int ordinary_usage = rp_edge_count(store, project.project, "USAGE", "crossArgument", "Function",
                                       "handler", ".target.handler");
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        "Function", "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(target_node, 1);
    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_python_alias_equal_module_leaf_targets_imported_member) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\ndef target():\n    pass\n"},
        {"use.py", "from target import handler as target\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(target)\n"},
    };
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python alias/module-leaf collision fixture did not produce a graph store");
    }
    int correct_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                          "Function", "handler", ".target.handler");
    int wrong_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "target", ".target.target");
    rh_cleanup(&project, store);

    ASSERT_EQ(correct_reference, 1);
    ASSERT_EQ(wrong_reference, 0);
    PASS();
}

static int rp_assert_python_callback_occurrence_stays_usage(const char *source,
                                                            const char *caller) {
    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "repro",
                                          "use.py", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument_call = NULL;
    const char *scan = source;
    while ((scan = strstr(scan, "accept(callback)")) != NULL) {
        argument_call = scan;
        scan++;
    }
    ASSERT_NOT_NULL(argument_call);
    uint32_t site_start = (uint32_t)(argument_call - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("callback");
    int ordinary_usage = 0;
    int promoted_usage = 0;
    int precise_reference = 0;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (!usage->ref_name || strcmp(usage->ref_name, "callback") != 0 ||
            !usage->enclosing_func_qn || !rp_qn_ends_with(usage->enclosing_func_qn, caller) ||
            usage->site_start_byte != site_start || usage->site_end_byte != site_end)
            continue;
        if (usage->kind == CBM_USAGE_VALUE)
            ordinary_usage++;
        else if (usage->kind == CBM_USAGE_CALL_REFERENCE)
            promoted_usage++;
    }
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE && resolved->caller_qn &&
            rp_qn_ends_with(resolved->caller_qn, caller) &&
            resolved->site_start_byte == site_start && resolved->site_end_byte == site_end) {
            precise_reference++;
        }
    }
    cbm_free_result(raw);

    ASSERT_GTE(ordinary_usage, 1);
    ASSERT_EQ(promoted_usage, 0);
    ASSERT_EQ(precise_reference, 0);
    return 0;
}

static int rp_assert_python_imported_callback_fails_closed_quality(const char *source,
                                                                   const char *quality_case) {
    int raw_rc = rp_assert_python_callback_occurrence_stays_usage(source, "crossArgument");
    if (raw_rc != 0)
        return raw_rc;

    const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", source},
    };
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 2, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python module-binding fixture did not produce a graph store");
    }
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                       "accept", ".use.accept");
    int total_references =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", NULL, NULL, NULL);
    int imported_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "handler", ".target.handler");
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        "Function", "handler", ".target.handler");
    int quality_rc = rp_quality_check(store, &project, files, 2, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    if (!quality_case)
        ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(total_references, 0);
    ASSERT_EQ(imported_reference, 0);
    ASSERT_EQ(fabricated_call, 0);
    return 0;
}

static int rp_assert_python_imported_callback_fails_closed(const char *source) {
    return rp_assert_python_imported_callback_fails_closed_quality(source, NULL);
}

static int rp_assert_python_exact_reference_occurrence(const char *filename, const char *source,
                                                       const char *marker, const char *site_text,
                                                       const char *ref_name) {
    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_PYTHON, "repro",
                                          filename, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);

    const char *marker_start = strstr(source, marker);
    ASSERT_NOT_NULL(marker_start);
    const char *site = strstr(marker_start, site_text);
    ASSERT_TRUE(site && site + strlen(site_text) <= marker_start + strlen(marker));
    uint32_t site_start = (uint32_t)(site - source);
    uint32_t site_end = site_start + (uint32_t)strlen(site_text);

    const CBMUsage *carrier = NULL;
    int carrier_count = 0;
    int target_reference_rows = 0;
    int target_invocation_rows = 0;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (usage->kind == CBM_USAGE_VALUE && usage->ref_name &&
            strcmp(usage->ref_name, ref_name) == 0 && usage->enclosing_func_qn &&
            rp_qn_ends_with(usage->enclosing_func_qn, "argument") &&
            usage->site_start_byte == site_start && usage->site_end_byte == site_end) {
            carrier = usage;
            carrier_count++;
        }
    }
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (!resolved->caller_qn || !rp_qn_ends_with(resolved->caller_qn, "argument") ||
            !resolved->callee_qn || !rp_qn_ends_with(resolved->callee_qn, ".handler") ||
            resolved->site_start_byte != site_start || resolved->site_end_byte != site_end) {
            continue;
        }
        target_reference_rows += resolved->kind == CBM_RESOLVED_CALL_REFERENCE;
        target_invocation_rows += resolved->kind == CBM_RESOLVED_INVOCATION;
    }
    const CBMResolvedCall *joined =
        carrier ? cbm_pipeline_find_lsp_reference(&raw->resolved_calls, carrier, false) : NULL;
    bool carrier_is_candidate = carrier && carrier->may_be_call_reference;
    bool exact_join = joined && joined->kind == CBM_RESOLVED_CALL_REFERENCE && joined->callee_qn &&
                      rp_qn_ends_with(joined->callee_qn, ".handler");
    cbm_free_result(raw);

    ASSERT_EQ(carrier_count, 1);
    ASSERT_TRUE(carrier_is_candidate);
    ASSERT_EQ(target_reference_rows, 1);
    ASSERT_EQ(target_invocation_rows, 0);
    ASSERT_TRUE(exact_join);
    return rp_assert_exact_function_value_reference(filename, source, CBM_LANG_PYTHON, "Function");
}

TEST(repro_python_module_walrus_shadow_fails_closed) {
    static const char source[] = "from target import handler as callback\n"
                                 "(callback := 0)\n"
                                 "def accept(fn):\n    pass\n"
                                 "def crossArgument():\n    accept(callback)\n";
    int rc = rp_assert_python_imported_callback_fails_closed(source);
    if (rc != 0)
        return rc;
    PASS();
}

/* A comprehension assignment expression binds in the containing scope, not
 * the comprehension's implicit scope. */
TEST(repro_python_module_comprehension_walrus_shadow_fails_closed) {
    static const char source[] = "from target import handler as callback\n"
                                 "[(callback := 0) for _ in (0,)]\n"
                                 "def accept(fn):\n    pass\n"
                                 "def crossArgument():\n    accept(callback)\n";
    int rc = rp_assert_python_imported_callback_fails_closed(source);
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_module_type_alias_shadow_fails_closed) {
    static const char source[] = "from target import handler as callback\n"
                                 "type callback = int\n"
                                 "def accept(fn):\n    pass\n"
                                 "def crossArgument():\n    accept(callback)\n";
    int rc = rp_assert_python_imported_callback_fails_closed_quality(
        source, "repro_python_module_type_alias_shadow_fails_closed");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_parenthesized_function_value_is_exact_reference) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def argument():\n"
                                 "    accept((handler))\n";
    /* The occurrence is the outermost parenthesised direct argument, matching
     * what the bound-method form in test_call_reference_contract.c already
     * uses. The span originally written here, "((handler))", is the argument
     * LIST -- one byte wider on each side, matching neither the usage carrier
     * nor the semantic row, and per-argument it would collide for any call
     * with more than one argument. */
    int rc = rp_assert_python_exact_reference_occurrence(
        "parenthesized_reference.py", source, "accept((handler))", "(handler)", "handler");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_local_callable_alias_value_is_exact_reference) {
    /* Both occurrences of handler inside `argument` are proven exact callable
     * values -- the alias use in accept(callback) AND the right-hand side of
     * `callback = handler` (maintainer decision: a proven RHS is a
     * CALL_REFERENCE, not an ordinary USAGE). The graph deduplicates edges by
     * (src, tgt, type), so the two occurrences surface as ONE CALL_REFERENCE
     * edge and zero USAGE edges, which is exactly what the shared helper
     * asserts. */
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def argument():\n"
                                 "    callback = handler\n"
                                 "    accept(callback)\n";
    int rc = rp_assert_python_exact_reference_occurrence(
        "local_alias_reference.py", source, "accept(callback)", "callback", "callback");
    if (rc != 0)
        return rc;
    PASS();
}

/* Raw lexical coverage for this occurrence lives in
 * repro_python_assignment_blocks_earlier_use_for_whole_function. This graph
 * control proves that a semantic row cannot bypass that local-shadow guard. */
TEST(repro_python_read_before_local_assignment_has_no_graph_reference) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def accept(fn):\n"
                                 "    pass\n"
                                 "def argument():\n"
                                 "    accept(handler)\n"
                                 "    handler = 0\n";
    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_PYTHON, "read_before_write.py"));

    RProj project;
    cbm_store_t *store = rh_index(&project, "read_before_write.py", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python read-before-write fixture did not produce a graph store");
    }
    int registrar_call =
        rp_edge_count(store, project.project, "CALLS", "argument", "Function", "accept", NULL);
    int total_references =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "argument", NULL, NULL, NULL);
    int fabricated_call =
        rp_edge_count(store, project.project, "CALLS", "argument", "Function", "handler", NULL);
    rh_cleanup(&project, store);

    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(total_references, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_python_local_function_shadows_imported_callable) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "def callback():\n    pass\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    const char *quality_case = "repro_python_local_function_shadows_imported_callable";
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 2, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python local-function shadow fixture did not produce a graph store");
    }
    int wrong_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "handler", ".target.handler");
    int quality_rc = rp_quality_check(store, &project, files, 2, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_EQ(wrong_reference, 0);
    PASS();
}

TEST(repro_python_alias_capture_precedes_later_import_rebinding) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "def callback():\n    pass\n"
                   "alias = callback\n"
                   "from target import handler as callback\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(alias)\n"},
    };
    const char *quality_case = "repro_python_alias_capture_precedes_later_import_rebinding";
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 2, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python ordered alias-capture fixture did not produce a graph store");
    }
    int wrong_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "handler", ".target.handler");
    int quality_rc = rp_quality_check(store, &project, files, 2, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_EQ(wrong_reference, 0);
    PASS();
}

TEST(repro_python_later_class_clears_function_value_identity) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "def callback():\n    pass\n"
                   "class callback:\n    pass\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    int raw_rc =
        rp_assert_python_callback_occurrence_stays_usage(files[1].content, "crossArgument");
    if (raw_rc != 0)
        return raw_rc;
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python class-rebinding fixture did not produce a graph store");
    }
    int local_function_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "callback", ".use.callback");
    int imported_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(local_function_reference, 0);
    ASSERT_EQ(imported_reference, 0);
    PASS();
}

TEST(repro_python_later_wildcard_import_clears_local_function_proof) {
    static const RFile files[] = {
        {"target.py", "def callback():\n    pass\n"},
        {"use.py", "def callback():\n    pass\n"
                   "from target import *\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    int raw_rc =
        rp_assert_python_callback_occurrence_stays_usage(files[1].content, "crossArgument");
    if (raw_rc != 0)
        return raw_rc;
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python wildcard-rebinding fixture did not produce a graph store");
    }
    int local_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "callback", ".use.callback");
    int imported_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "callback", ".target.callback");
    rh_cleanup(&project, store);

    ASSERT_EQ(local_reference, 0);
    ASSERT_EQ(imported_reference, 0);
    PASS();
}

TEST(repro_python_decorated_local_shadow_fails_closed) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "def replace(fn):\n    return object()\n"
                   "@replace\n"
                   "def callback():\n    pass\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    int raw_rc =
        rp_assert_python_callback_occurrence_stays_usage(files[1].content, "crossArgument");
    if (raw_rc != 0)
        return raw_rc;
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python decorated-shadow fixture did not produce a graph store");
    }
    int local_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "callback", ".use.callback");
    int imported_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(local_reference, 0);
    ASSERT_EQ(imported_reference, 0);
    PASS();
}

TEST(repro_python_module_if_assignment_shadow_fails_closed) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "if True:\n    callback = 0\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    int raw_rc =
        rp_assert_python_callback_occurrence_stays_usage(files[1].content, "crossArgument");
    if (raw_rc != 0)
        return raw_rc;
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python conditional-assignment shadow fixture did not produce a graph store");
    }
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                       "accept", ".use.accept");
    int total_references =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", NULL, NULL, NULL);
    int imported_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "handler", ".target.handler");
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        "Function", "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(total_references, 0);
    ASSERT_EQ(imported_reference, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_python_module_if_definition_shadow_fails_closed) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "if True:\n"
                   "    def callback():\n"
                   "        pass\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    int raw_rc =
        rp_assert_python_callback_occurrence_stays_usage(files[1].content, "crossArgument");
    if (raw_rc != 0)
        return raw_rc;
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python conditional-definition shadow fixture did not produce a graph store");
    }
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                       "accept", ".use.accept");
    int total_references =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", NULL, NULL, NULL);
    int local_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "callback", ".use.callback");
    int imported_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "handler", ".target.handler");
    int local_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                   "callback", ".use.callback");
    int imported_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                      "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(total_references, 0);
    ASSERT_EQ(local_reference, 0);
    ASSERT_EQ(imported_reference, 0);
    ASSERT_EQ(local_call, 0);
    ASSERT_EQ(imported_call, 0);
    PASS();
}

TEST(repro_python_later_import_restores_imported_callable_identity) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "def callback():\n    pass\n"
                   "from target import handler as callback\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    const char *quality_case = "repro_python_later_import_restores_imported_callable_identity";
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 2, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python later-import fixture did not produce a graph store");
    }
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                       "accept", ".use.accept");
    int local_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "callback", ".use.callback");
    int imported_usage = rp_edge_count(store, project.project, "USAGE", "crossArgument", "Function",
                                       "handler", ".target.handler");
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        "Function", "handler", ".target.handler");
    int quality_rc = rp_quality_check(store, &project, files, 2, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(local_reference, 0);
    ASSERT_EQ(imported_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_python_wildcard_then_local_function_restores_exactness) {
    static const RFile files[] = {
        {"target.py", "def callback():\n    pass\n"},
        {"use.py", "from target import *\n"
                   "def callback():\n    pass\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python wildcard-then-definition fixture did not produce a graph store");
    }
    int registrar_call = rp_edge_count(store, project.project, "CALLS", "crossArgument", "Function",
                                       "accept", ".use.accept");
    int total_references =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", NULL, NULL, NULL);
    int local_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                        "Function", "callback", ".use.callback");
    int imported_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument", "Function",
                      "callback", ".target.callback");
    int local_usage = rp_edge_count(store, project.project, "USAGE", "crossArgument", "Function",
                                    "callback", ".use.callback");
    int fabricated_call = rp_edge_count(store, project.project, "CALLS", "crossArgument",
                                        "Function", "callback", ".use.callback");
    rh_cleanup(&project, store);

    ASSERT_EQ(registrar_call, 1);
    ASSERT_EQ(total_references, 1);
    ASSERT_EQ(local_reference, 1);
    ASSERT_EQ(imported_reference, 0);
    ASSERT_EQ(local_usage, 0);
    ASSERT_EQ(fabricated_call, 0);
    PASS();
}

TEST(repro_python_module_compound_binding_matrix_fails_closed) {
    static const char *const cases[] = {
        "from target import handler as callback\n"
        "while flag:\n    callback = 0\n    break\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
        "from target import handler as callback\n"
        "for callback in items:\n    pass\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
        "from target import handler as callback\n"
        "with manager as callback:\n    pass\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
        "from target import handler as callback\n"
        "try:\n    callback = 0\nexcept Exception:\n    pass\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
        "from target import handler as callback\n"
        "match value:\n    case callback:\n        pass\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
        "from target import handler as callback\n"
        "if flag:\n    del callback\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
        "from target import handler as callback\n"
        "if flag:\n    from other import alternate as callback\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
        "from target import handler as callback\n"
        "if flag:\n    from other import *\n"
        "def accept(fn):\n    pass\n"
        "def crossArgument():\n    accept(callback)\n",
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        int rc = rp_assert_python_callback_occurrence_stays_usage(cases[i], "crossArgument");
        if (rc != 0)
            return rc;
    }
    PASS();
}

TEST(repro_python_annotation_only_preserves_imported_callable_identity) {
    static const RFile files[] = {
        {"target.py", "def handler():\n    pass\n"},
        {"use.py", "from target import handler as callback\n"
                   "callback: object\n"
                   "def accept(fn):\n    pass\n"
                   "def crossArgument():\n    accept(callback)\n"},
    };
    RProj project;
    cbm_store_t *store = rh_index_files(&project, files, 2);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Python annotation-only fixture did not produce a graph store");
    }
    int precise_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "crossArgument",
                                          "Function", "handler", ".target.handler");
    int ordinary_usage = rp_edge_count(store, project.project, "USAGE", "crossArgument", "Function",
                                       "handler", ".target.handler");
    rh_cleanup(&project, store);

    ASSERT_EQ(precise_reference, 1);
    ASSERT_EQ(ordinary_usage, 0);
    PASS();
}

TEST(repro_rust_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"target.rs", "pub fn handler() {}\n"},
        {"main.rs", "mod target;\n"
                    "use target::handler;\n"
                    "fn accept(_callback: fn()) {}\n"
                    "fn crossArgument() { accept(handler); }\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value_quality(
        files, 2, CBM_LANG_RUST, "Function",
        "repro_rust_cross_file_exact_function_value_is_call_reference");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_cross_file_exact_function_value_is_call_reference) {
    static const RFile files[] = {
        {"Target.cs", "class Target { public static void handler() {} }\n"},
        {"Use.cs", "using System;\n"
                   "using static Target;\n"
                   "class Use {\n"
                   "  static void accept(Action callback) {}\n"
                   "  static void crossArgument() { accept(handler); }\n"
                   "}\n"},
    };
    int rc = rp_assert_cross_file_exact_function_value_quality(
        files, 2, CBM_LANG_CSHARP, "Method",
        "repro_csharp_cross_file_exact_function_value_is_call_reference");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_cross_file_callable_alias_graph_quality(const RFile *files, int file_count,
                                                             CBMLanguage language,
                                                             const char *callable_label,
                                                             const char *quality_case) {
    for (int i = 0; i < file_count; i++) {
        ASSERT_TRUE(rp_extracts_clean(files[i].content, language, files[i].name));
    }
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, file_count, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("cross-file callable-alias fixture did not produce a graph store");
    }
    int wrong_call = rp_edge_count(store, project.project, "CALLS", "crossAliasCaller",
                                   callable_label, "shadowed", NULL);
    int exact_call = rp_edge_count(store, project.project, "CALLS", "crossAliasCaller",
                                   callable_label, "actual", NULL);
    int quality_rc = rp_quality_check(store, &project, files, file_count, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;
    ASSERT_EQ(wrong_call, 0);
    if (!quality_case)
        ASSERT_EQ(exact_call, 1);
    return 0;
}

static int rp_assert_cross_file_callable_alias_graph(const RFile *files, int file_count,
                                                     CBMLanguage language,
                                                     const char *callable_label) {
    return rp_assert_cross_file_callable_alias_graph_quality(files, file_count, language,
                                                             callable_label, NULL);
}

TEST(repro_csharp_cross_file_callable_alias_invocation_targets_exact_value) {
    static const RFile files[] = {
        {"Target.cs", "class Target { public static int actual() { return 2; } }\n"},
        {"Use.cs", "using System;\n"
                   "using static Target;\n"
                   "class Use {\n"
                   "  static int shadowed() { return 1; }\n"
                   "  static int crossAliasCaller() {\n"
                   "    Func<int> shadowed = actual;\n"
                   "    return shadowed();\n"
                   "  }\n"
                   "}\n"},
    };
    int rc = rp_assert_cross_file_callable_alias_graph_quality(
        files, 2, CBM_LANG_CSHARP, "Method",
        "repro_csharp_cross_file_callable_alias_invocation_targets_exact_value");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_cross_file_callable_alias_invocation_targets_exact_value) {
    static const RFile files[] = {
        {"Target.kt", "package target\nfun actual(): Int = 2\n"},
        {"Use.kt", "package use\n"
                   "import target.actual\n"
                   "fun shadowed(): Int = 1\n"
                   "fun crossAliasCaller(): Int {\n"
                   "  val shadowed = ::actual\n"
                   "  return shadowed()\n"
                   "}\n"},
    };
    int rc = rp_assert_cross_file_callable_alias_graph(files, 2, CBM_LANG_KOTLIN, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

static int rp_assert_alias_argument_exact_join(const char *filename, const char *source,
                                               CBMLanguage language, const char *caller,
                                               const char *target, bool allow_tail_match) {
    CBMFileResult *raw =
        cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument = NULL;
    const char *scan = source;
    while ((scan = strstr(scan, "accept(callback)")) != NULL) {
        argument = scan;
        scan++;
    }
    ASSERT_NOT_NULL(argument);
    uint32_t start = (uint32_t)(argument - source) + 7U;
    uint32_t end = start + (uint32_t)strlen("callback");
    const CBMUsage *site_usage = NULL;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (usage->ref_name && strcmp(usage->ref_name, "callback") == 0 &&
            usage->enclosing_func_qn && rp_qn_ends_with(usage->enclosing_func_qn, caller) &&
            usage->site_start_byte == start && usage->site_end_byte == end) {
            site_usage = usage;
            break;
        }
    }
    const CBMResolvedCall *joined =
        site_usage
            ? cbm_pipeline_find_lsp_reference(&raw->resolved_calls, site_usage, allow_tail_match)
            : NULL;
    bool exact_target = joined && joined->callee_qn && rp_qn_ends_with(joined->callee_qn, target);
    cbm_free_result(raw);
    ASSERT_NOT_NULL(site_usage);
    ASSERT_TRUE(exact_target);
    return 0;
}

TEST(repro_csharp_callable_alias_argument_is_exact_reference) {
    static const char source[] = "using System;\n"
                                 "class AliasArgument {\n"
                                 "  static void handler() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void argument() {\n"
                                 "    Action callback = handler;\n"
                                 "    accept(callback);\n"
                                 "  }\n"
                                 "}\n";
    int rc = rp_assert_alias_argument_exact_join("AliasArgument.cs", source, CBM_LANG_CSHARP,
                                                 "argument", "handler", false);
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_callable_alias_argument_is_exact_reference) {
    static const char source[] = "fun handler(): Unit {}\n"
                                 "fun accept(callback: () -> Unit): Unit {}\n"
                                 "fun argument(): Unit {\n"
                                 "  val callback = ::handler\n"
                                 "  accept(callback)\n"
                                 "}\n";
    int rc = rp_assert_alias_argument_exact_join("AliasArgument.kt", source, CBM_LANG_KOTLIN,
                                                 "argument", "handler", true);
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_receiver_bound_alias_invocation_targets_exact_method) {
    static const char source[] = "class Service { fun run(): Int = 2 }\n"
                                 "fun shadowed(): Int = 1\n"
                                 "fun receiverAliasCaller(): Int {\n"
                                 "  val service = Service()\n"
                                 "  val shadowed = service::run\n"
                                 "  return shadowed()\n"
                                 "}\n";
    const char *quality_case = "repro_kotlin_receiver_bound_alias_invocation_targets_exact_method";
    const RFile quality_files[] = {{"ReceiverAlias.kt", source}};
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, quality_files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin receiver-alias fixture did not produce a graph store");
    }
    int wrong_call = rp_edge_count(store, project.project, "CALLS", "receiverAliasCaller",
                                   "Function", "shadowed", NULL);
    int quality_rc = rp_quality_check(store, &project, quality_files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;
    ASSERT_EQ(wrong_call, 0);

    PASS();
}

TEST(repro_kotlin_unknown_imported_property_is_not_callable_reference) {
    static const RFile files[] = {
        {"Values.kt", "package values\nval handler: Int = 1\n"},
        {"Use.kt", "package consumer\n"
                   "import values.handler\n"
                   "fun importedPropertyReference(): kotlin.reflect.KProperty0<Int> = ::handler\n"},
    };
    const char *quality_case = "repro_kotlin_unknown_imported_property_is_not_callable_reference";
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, files, 2, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("Kotlin imported-property fixture did not produce a graph store");
    }
    int property_node = rp_node_count(store, project.project, "Variable", "handler", NULL);
    int precise_property = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "importedPropertyReference", "Variable", "handler", NULL);
    int quality_rc = rp_quality_check(store, &project, files, 2, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;
    ASSERT_GTE(property_node, 1);
    ASSERT_EQ(precise_property, 0);

    PASS();
}

/* A branch/map expression can name callable definitions without selecting one
 * statically at the argument occurrence.  Those target mentions remain USAGE;
 * only an exact single-target value earns CALL_REFERENCE. */
static int rp_assert_complex_function_values_stay_usage_quality(const char *filename,
                                                                const char *source,
                                                                CBMLanguage language,
                                                                const char *callable_label,
                                                                const char *quality_case) {
    ASSERT_TRUE(rp_extracts_clean(source, language, filename));

    const RFile quality_files[] = {{filename, source}};
    RProj project;
    cbm_store_t *store = rp_quality_index(&project, quality_files, 1, quality_case);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("complex function-value fixture did not produce a graph store");
    }
    int handler_node = rp_node_count(store, project.project, callable_label, "handler", NULL);
    int alternate_node = rp_node_count(store, project.project, callable_label, "alternate", NULL);
    int registrar_call =
        rp_edge_count(store, project.project, "CALLS", "complex", callable_label, "accept", NULL);
    int handler_usage =
        rp_edge_count(store, project.project, "USAGE", "complex", callable_label, "handler", NULL);
    int alternate_usage = rp_edge_count(store, project.project, "USAGE", "complex", callable_label,
                                        "alternate", NULL);
    int handler_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "complex",
                                          callable_label, "handler", NULL);
    int alternate_reference = rp_edge_count(store, project.project, "CALL_REFERENCE", "complex",
                                            callable_label, "alternate", NULL);
    int handler_call =
        rp_edge_count(store, project.project, "CALLS", "complex", callable_label, "handler", NULL);
    int alternate_call = rp_edge_count(store, project.project, "CALLS", "complex", callable_label,
                                       "alternate", NULL);
    int quality_rc = rp_quality_check(store, &project, quality_files, 1, quality_case);
    rh_cleanup(&project, store);
    if (quality_rc != 0)
        return quality_rc;

    if (!quality_case && (handler_node < 1 || alternate_node < 1 || registrar_call != 1 ||
                          handler_usage != 1 || alternate_usage != 1 || handler_reference != 0 ||
                          alternate_reference != 0 || handler_call != 0 || alternate_call != 0)) {
        fprintf(stderr,
                "  [complex-reference] file=%s nodes=%d/%d registrar=%d usage=%d/%d "
                "reference=%d/%d calls=%d/%d\n",
                filename, handler_node, alternate_node, registrar_call, handler_usage,
                alternate_usage, handler_reference, alternate_reference, handler_call,
                alternate_call);
    }

    ASSERT_GTE(handler_node, 1);
    ASSERT_GTE(alternate_node, 1);
    if (!quality_case)
        ASSERT_EQ(registrar_call, 1);
    if (!quality_case)
        ASSERT_EQ(handler_usage, 1);
    if (!quality_case)
        ASSERT_EQ(alternate_usage, 1);
    ASSERT_EQ(handler_reference, 0);
    ASSERT_EQ(alternate_reference, 0);
    ASSERT_EQ(handler_call, 0);
    ASSERT_EQ(alternate_call, 0);
    return 0;
}

static int rp_assert_complex_function_values_stay_usage(const char *filename, const char *source,
                                                        CBMLanguage language,
                                                        const char *callable_label) {
    return rp_assert_complex_function_values_stay_usage_quality(filename, source, language,
                                                                callable_label, NULL);
}

TEST(repro_javascript_complex_function_value_stays_usage) {
    /* Use a module so this contract has an explicit namespace. */
    static const char source[] = "export {};\n"
                                 "function handler() {}\n"
                                 "function alternate() {}\n"
                                 "function accept(callback) {}\n"
                                 "function complex(flag) { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.js", source, CBM_LANG_JAVASCRIPT,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_typescript_complex_function_value_stays_usage) {
    /* Use a module so this contract has an explicit namespace. */
    static const char source[] =
        "export {};\n"
        "function handler(): void {}\n"
        "function alternate(): void {}\n"
        "function accept(callback: () => void): void {}\n"
        "function complex(flag: boolean): void { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.ts", source, CBM_LANG_TYPESCRIPT,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_tsx_complex_function_value_stays_usage) {
    /* Use a module so this contract has an explicit namespace. */
    static const char source[] =
        "export {};\n"
        "function handler(): void {}\n"
        "function alternate(): void {}\n"
        "function accept(callback: () => void): void {}\n"
        "function complex(flag: boolean): void { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.tsx", source, CBM_LANG_TSX,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_go_complex_function_value_stays_usage) {
    static const char source[] =
        "package refs\n"
        "func handler() {}\n"
        "func alternate() {}\n"
        "func accept(callback func()) {}\n"
        "func complex(flag bool) { "
        "accept(map[bool]func(){true: handler, false: alternate}[flag]) }\n";
    int rc =
        rp_assert_complex_function_values_stay_usage("complex.go", source, CBM_LANG_GO, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_complex_function_value_stays_usage) {
    static const char source[] = "def handler():\n"
                                 "    pass\n"
                                 "def alternate():\n"
                                 "    pass\n"
                                 "def accept(callback):\n"
                                 "    pass\n"
                                 "def complex(flag):\n"
                                 "    accept(handler if flag else alternate)\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.py", source, CBM_LANG_PYTHON,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_c_complex_function_value_stays_usage) {
    static const char source[] = "typedef void (*callback_t)(void);\n"
                                 "void handler(void) {}\n"
                                 "void alternate(void) {}\n"
                                 "void accept(callback_t callback) {}\n"
                                 "void complex(int flag) { accept(flag ? handler : alternate); }\n";
    int rc =
        rp_assert_complex_function_values_stay_usage("complex.c", source, CBM_LANG_C, "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cpp_complex_function_value_stays_usage) {
    static const char source[] =
        "using Callback = void (*)();\n"
        "void handler() {}\n"
        "void alternate() {}\n"
        "void accept(Callback callback) {}\n"
        "void complex(bool flag) { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage_quality(
        "complex.cpp", source, CBM_LANG_CPP, "Function",
        "repro_cpp_complex_function_value_stays_usage");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cuda_complex_function_value_stays_usage) {
    static const char source[] =
        "using Callback = void (*)();\n"
        "void handler() {}\n"
        "void alternate() {}\n"
        "void accept(Callback callback) {}\n"
        "void complex(bool flag) { accept(flag ? handler : alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage_quality(
        "complex.cu", source, CBM_LANG_CUDA, "Function",
        "repro_cuda_complex_function_value_stays_usage");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_complex_function_value_stays_usage) {
    static const char source[] = "fn handler() {}\n"
                                 "fn alternate() {}\n"
                                 "fn accept(_callback: fn()) {}\n"
                                 "fn complex(flag: bool) { "
                                 "accept(if flag { handler } else { alternate }); }\n";
    int rc = rp_assert_complex_function_values_stay_usage("complex.rs", source, CBM_LANG_RUST,
                                                          "Function");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_complex_function_value_stays_usage) {
    static const char source[] =
        "using System;\n"
        "class Refs {\n"
        "  static void handler() {}\n"
        "  static void alternate() {}\n"
        "  static void accept(Action callback) {}\n"
        "  static void complex(bool flag) { accept(flag ? handler : alternate); }\n"
        "}\n";
    int rc = rp_assert_complex_function_values_stay_usage_quality(
        "Complex.cs", source, CBM_LANG_CSHARP, "Method",
        "repro_csharp_complex_function_value_stays_usage");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_java_complex_method_reference_stays_usage) {
    static const char source[] = "interface Task { void run(); }\n"
                                 "class Refs {\n"
                                 "  static void handler() {}\n"
                                 "  static void alternate() {}\n"
                                 "  static void accept(Task callback) {}\n"
                                 "  static void complex(boolean flag) { "
                                 "accept(flag ? Refs::handler : Refs::alternate); }\n"
                                 "}\n";
    int rc = rp_assert_complex_function_values_stay_usage_quality(
        "Refs.java", source, CBM_LANG_JAVA, "Method",
        "repro_java_complex_method_reference_stays_usage");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_kotlin_complex_callable_reference_stays_usage) {
    static const char source[] = "fun handler() {}\n"
                                 "fun alternate() {}\n"
                                 "fun accept(callback: () -> Unit) {}\n"
                                 "fun complex(flag: Boolean) { "
                                 "accept(if (flag) ::handler else ::alternate) }\n";
    int rc = rp_assert_complex_function_values_stay_usage_quality(
        "Refs.kt", source, CBM_LANG_KOTLIN, "Function",
        "repro_kotlin_complex_callable_reference_stays_usage");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_php_complex_first_class_callable_stays_usage) {
    static const char source[] = "<?php\n"
                                 "function handler(): void {}\n"
                                 "function alternate(): void {}\n"
                                 "function accept(callable $callback): void {}\n"
                                 "function complex(bool $flag): void { "
                                 "accept($flag ? handler(...) : alternate(...)); }\n";
    int rc = rp_assert_complex_function_values_stay_usage_quality(
        "refs.php", source, CBM_LANG_PHP, "Function",
        "repro_php_complex_first_class_callable_stays_usage");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_perl_conditional_coderefs_stay_usage) {
    static const char source[] = "sub handler {}\n"
                                 "sub alternate {}\n"
                                 "sub accept { my ($callback) = @_; }\n"
                                 "sub complex { my ($flag) = @_; "
                                 "accept($flag ? \\&handler : \\&alternate); }\n";
    int rc = rp_assert_complex_function_values_stay_usage_quality(
        "complex.pl", source, CBM_LANG_PERL, "Function",
        "repro_perl_conditional_coderefs_stay_usage");
    if (rc != 0)
        return rc;
    PASS();
}

/* A function-pointer binding is exact only until an assignment makes its
 * target multi-valued.  Keeping the initializer's target alive here would
 * fabricate a CALL_REFERENCE for the later argument occurrence. */
TEST(repro_c_ambiguous_alias_reassignment_drops_stale_target) {
    static const char source[] = "void actual(void) {}\n"
                                 "void alternate(void) {}\n"
                                 "void accept(void (*callback)(void)) {}\n"
                                 "void staleAliasCaller(int flag) {\n"
                                 "  void (*callback)(void) = actual;\n"
                                 "  callback = flag ? actual : alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";

    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_C, "stale_alias.c"));
    RProj project;
    cbm_store_t *store = rh_index(&project, "stale_alias.c", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("ambiguous C alias fixture did not produce a graph store");
    }
    int actual_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "staleAliasCaller", "Function", "actual", NULL);
    int alternate_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                            "staleAliasCaller", "Function", "alternate", NULL);
    rh_cleanup(&project, store);

    ASSERT_EQ(actual_reference, 0);
    ASSERT_EQ(alternate_reference, 0);
    PASS();
}

/* A write in only one branch cannot become the unconditional post-branch
 * callable identity.  The merge has two possible targets and must fail closed
 * to ordinary USAGE at accept(callback). */
TEST(repro_c_conditional_alias_reassignment_drops_stale_target) {
    static const char source[] = "void actual(void) {}\n"
                                 "void alternate(void) {}\n"
                                 "void accept(void (*callback)(void)) {}\n"
                                 "void conditionalAliasCaller(int flag) {\n"
                                 "  void (*callback)(void) = actual;\n"
                                 "  if (flag) callback = alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";

    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_C, "conditional_alias.c"));
    RProj project;
    cbm_store_t *store = rh_index(&project, "conditional_alias.c", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("conditional C alias fixture did not produce a graph store");
    }
    int actual_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "conditionalAliasCaller", "Function", "actual", NULL);
    int alternate_reference =
        rp_edge_count(store, project.project, "CALL_REFERENCE", "conditionalAliasCaller",
                      "Function", "alternate", NULL);
    rh_cleanup(&project, store);

    ASSERT_EQ(actual_reference, 0);
    ASSERT_EQ(alternate_reference, 0);
    PASS();
}

/* The nearest ordinary lexical binding wins even when an outer function
 * pointer with the same spelling has an exact target. */
TEST(repro_c_noncallable_shadow_blocks_outer_alias_target) {
    static const char source[] = "void actual(void) {}\n"
                                 "void accept(void (*callback)(void)) {}\n"
                                 "void shadowedAliasCaller(void) {\n"
                                 "  void (*callback)(void) = actual;\n"
                                 "  {\n"
                                 "    int callback = 0;\n"
                                 "    accept(callback);\n"
                                 "  }\n"
                                 "}\n";

    ASSERT_TRUE(rp_extracts_clean(source, CBM_LANG_C, "shadowed_alias.c"));
    RProj project;
    cbm_store_t *store = rh_index(&project, "shadowed_alias.c", source);
    if (!store) {
        rh_cleanup(&project, store);
        FAIL("shadowed C alias fixture did not produce a graph store");
    }
    int leaked_reference = rp_edge_count(store, project.project, "CALL_REFERENCE",
                                         "shadowedAliasCaller", "Function", "actual", NULL);
    rh_cleanup(&project, store);

    ASSERT_EQ(leaked_reference, 0);
    PASS();
}

static int rp_assert_conditional_alias_reference_fails_closed(const char *filename,
                                                              const char *source,
                                                              CBMLanguage language,
                                                              const char *caller) {
    CBMFileResult *raw =
        cbm_extract_file(source, (int)strlen(source), language, "repro", filename, 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument_call = NULL;
    const char *scan = source;
    while ((scan = strstr(scan, "accept(callback)")) != NULL) {
        argument_call = scan;
        scan++;
    }
    ASSERT_NOT_NULL(argument_call);
    uint32_t site_start = (uint32_t)(argument_call - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("callback");
    int ordinary_usage = 0;
    int precise_reference = 0;
    int fabricated_invocation = 0;
    for (int i = 0; i < raw->usages.count; i++) {
        const CBMUsage *usage = &raw->usages.items[i];
        if (usage->ref_name && strcmp(usage->ref_name, "callback") == 0 &&
            usage->enclosing_func_qn && rp_qn_ends_with(usage->enclosing_func_qn, caller) &&
            usage->site_start_byte == site_start && usage->site_end_byte == site_end &&
            usage->kind == CBM_USAGE_VALUE) {
            ordinary_usage++;
        }
    }
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (!resolved->caller_qn || !rp_qn_ends_with(resolved->caller_qn, caller) ||
            resolved->site_start_byte != site_start || resolved->site_end_byte != site_end) {
            continue;
        }
        if (resolved->kind == CBM_RESOLVED_CALL_REFERENCE) {
            precise_reference++;
        } else if (resolved->kind == CBM_RESOLVED_INVOCATION) {
            fabricated_invocation++;
        }
    }
    cbm_free_result(raw);

    ASSERT_GTE(ordinary_usage, 1);
    ASSERT_EQ(precise_reference, 0);
    ASSERT_EQ(fabricated_invocation, 0);
    return 0;
}

TEST(repro_cpp_conditional_alias_reference_fails_closed) {
    static const char source[] = "void actual() {}\n"
                                 "void alternate() {}\n"
                                 "void accept(void (*callback)()) {}\n"
                                 "void conditionalAliasCaller(bool flag) {\n"
                                 "  void (*callback)() = actual;\n"
                                 "  if (flag) callback = alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.cpp", source, CBM_LANG_CPP, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_cuda_conditional_alias_reference_fails_closed) {
    static const char source[] = "void actual() {}\n"
                                 "void alternate() {}\n"
                                 "void accept(void (*callback)()) {}\n"
                                 "void conditionalAliasCaller(bool flag) {\n"
                                 "  void (*callback)() = actual;\n"
                                 "  if (flag) callback = alternate;\n"
                                 "  accept(callback);\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.cu", source, CBM_LANG_CUDA, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_go_conditional_alias_reference_fails_closed) {
    static const char source[] = "package refs\n"
                                 "func actual() {}\n"
                                 "func alternate() {}\n"
                                 "func accept(callback func()) {}\n"
                                 "func conditionalAliasCaller(flag bool) {\n"
                                 "  callback := actual\n"
                                 "  if flag { callback = alternate }\n"
                                 "  accept(callback)\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.go", source, CBM_LANG_GO, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_python_conditional_alias_reference_fails_closed) {
    static const char source[] = "def actual():\n    pass\n"
                                 "def alternate():\n    pass\n"
                                 "def accept(callback):\n    pass\n"
                                 "def conditionalAliasCaller(flag):\n"
                                 "    callback = actual\n"
                                 "    if flag:\n        callback = alternate\n"
                                 "    accept(callback)\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.py", source, CBM_LANG_PYTHON, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_conditional_alias_reference_fails_closed) {
    static const char source[] = "fn actual() {}\n"
                                 "fn alternate() {}\n"
                                 "fn accept(callback: fn()) {}\n"
                                 "fn conditionalAliasCaller(flag: bool) {\n"
                                 "    let mut callback: fn() = actual;\n"
                                 "    if flag { callback = alternate; }\n"
                                 "    accept(callback);\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "conditional_alias.rs", source, CBM_LANG_RUST, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_rust_unconditional_alias_reassignment_tracks_new_exact_target) {
    static const char source[] = "fn actual() {}\n"
                                 "fn alternate() {}\n"
                                 "fn accept(callback: fn()) {}\n"
                                 "fn exactReassignmentCaller() {\n"
                                 "    let mut callback: fn() = actual;\n"
                                 "    callback = alternate;\n"
                                 "    accept(callback);\n"
                                 "}\n";

    CBMFileResult *raw = cbm_extract_file(source, (int)strlen(source), CBM_LANG_RUST, "repro",
                                          "exact_reassignment.rs", 0, NULL, NULL);
    ASSERT_NOT_NULL(raw);
    ASSERT_FALSE(raw->has_error || raw->parse_incomplete);
    const char *argument_call = strstr(source, "accept(callback)");
    ASSERT_NOT_NULL(argument_call);
    uint32_t site_start = (uint32_t)(argument_call - source) + 7U;
    uint32_t site_end = site_start + (uint32_t)strlen("callback");
    int stale_reference = 0;
    int exact_reference = 0;
    for (int i = 0; i < raw->resolved_calls.count; i++) {
        const CBMResolvedCall *resolved = &raw->resolved_calls.items[i];
        if (resolved->kind != CBM_RESOLVED_CALL_REFERENCE ||
            resolved->site_start_byte != site_start || resolved->site_end_byte != site_end ||
            !resolved->callee_qn) {
            continue;
        }
        if (rp_qn_ends_with(resolved->callee_qn, "actual")) {
            stale_reference++;
        }
        if (rp_qn_ends_with(resolved->callee_qn, "alternate")) {
            exact_reference++;
        }
    }
    cbm_free_result(raw);

    ASSERT_EQ(stale_reference, 0);
    ASSERT_EQ(exact_reference, 1);
    PASS();
}

TEST(repro_kotlin_conditional_alias_reference_fails_closed) {
    static const char source[] = "fun actual(): Unit {}\n"
                                 "fun alternate(): Unit {}\n"
                                 "fun accept(callback: () -> Unit): Unit {}\n"
                                 "fun conditionalAliasCaller(flag: Boolean): Unit {\n"
                                 "  var callback = ::actual\n"
                                 "  if (flag) callback = ::alternate\n"
                                 "  accept(callback)\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "ConditionalAlias.kt", source, CBM_LANG_KOTLIN, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

TEST(repro_csharp_conditional_alias_reference_fails_closed) {
    static const char source[] = "using System;\n"
                                 "class ConditionalAlias {\n"
                                 "  static void actual() {}\n"
                                 "  static void alternate() {}\n"
                                 "  static void accept(Action callback) {}\n"
                                 "  static void conditionalAliasCaller(bool flag) {\n"
                                 "    Action callback = actual;\n"
                                 "    if (flag) callback = alternate;\n"
                                 "    accept(callback);\n"
                                 "  }\n"
                                 "}\n";
    int rc = rp_assert_conditional_alias_reference_fails_closed(
        "ConditionalAlias.cs", source, CBM_LANG_CSHARP, "conditionalAliasCaller");
    if (rc != 0)
        return rc;
    PASS();
}

/* Branch-gating graph contracts for every frontend that has dedicated exact
 * callable-reference semantics. Complex/ambiguous expressions must retain an
 * ordinary USAGE edge instead of being promoted by a name-only match. */
SUITE(call_reference_language_complex_contract) {
    RUN_TEST(repro_javascript_complex_function_value_stays_usage);
    RUN_TEST(repro_typescript_complex_function_value_stays_usage);
    RUN_TEST(repro_tsx_complex_function_value_stays_usage);
    RUN_TEST(repro_go_complex_function_value_stays_usage);
    RUN_TEST(repro_python_complex_function_value_stays_usage);
    RUN_TEST(repro_c_complex_function_value_stays_usage);
    RUN_TEST(repro_cpp_complex_function_value_stays_usage);
    RUN_TEST(repro_cuda_complex_function_value_stays_usage);
    RUN_TEST(repro_rust_complex_function_value_stays_usage);
    RUN_TEST(repro_csharp_complex_function_value_stays_usage);
    RUN_TEST(repro_java_complex_method_reference_stays_usage);
    RUN_TEST(repro_kotlin_complex_callable_reference_stays_usage);
    RUN_TEST(repro_php_complex_first_class_callable_stays_usage);
    RUN_TEST(repro_perl_conditional_coderefs_stay_usage);
}

SUITE(repro_reference_precision) {
    RUN_TEST(repro_kotlin_property_reference_does_not_borrow_same_name_function);
    RUN_TEST(repro_kotlin_constructor_reference_requires_concrete_constructor);
    RUN_TEST(repro_kotlin_local_reference_never_binds_same_name_top_level);
    RUN_TEST(repro_php_dynamic_first_class_terminal_is_not_precise);
    RUN_TEST(repro_php_static_magic_first_class_reference_targets_callstatic);
    RUN_TEST(repro_php_typed_invokable_first_class_reference_targets_invoke);
    RUN_TEST(repro_java_same_name_receiver_calls_keep_occurrence_identity);
    RUN_TEST(repro_typescript_function_value_upgrades_only_with_exact_semantic_target);
    RUN_TEST(repro_kotlin_empty_cross_filter_does_not_fail_open);
    RUN_TEST(repro_go_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_python_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_rust_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_kotlin_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_csharp_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_go_exact_function_value_is_call_reference);
    RUN_TEST(repro_python_exact_function_value_is_call_reference);
    RUN_TEST(repro_javascript_exact_function_value_is_call_reference);
    RUN_TEST(repro_c_exact_function_value_is_call_reference);
    RUN_TEST(repro_cpp_exact_function_value_is_call_reference);
    RUN_TEST(repro_rust_exact_function_value_is_call_reference);
    RUN_TEST(repro_rust_direct_scoped_function_value_is_exact_reference);
    RUN_TEST(repro_csharp_exact_function_value_is_call_reference);
    RUN_TEST(repro_csharp_member_method_group_is_exact_reference);
    RUN_TEST(repro_csharp_parenthesized_method_group_is_exact_reference);
    RUN_TEST(repro_csharp_generic_method_group_is_exact_reference);
    RUN_TEST(repro_csharp_ambiguous_using_static_method_group_is_not_precise);
    RUN_TEST(repro_cuda_exact_function_value_is_call_reference);
    RUN_TEST(repro_exact_function_value_lsp_sites);
    RUN_TEST(repro_typescript_exact_bound_method_is_call_reference);
    RUN_TEST(repro_tsx_exact_bound_method_is_call_reference);
    RUN_TEST(repro_perl_exact_coderef_is_call_reference);
    RUN_TEST(repro_go_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_python_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_python_aliased_import_function_value_is_call_reference);
    RUN_TEST(repro_python_alias_equal_module_leaf_targets_imported_member);
    RUN_TEST(repro_python_module_walrus_shadow_fails_closed);
    RUN_TEST(repro_python_module_comprehension_walrus_shadow_fails_closed);
    RUN_TEST(repro_python_module_type_alias_shadow_fails_closed);
    RUN_TEST(repro_python_parenthesized_function_value_is_exact_reference);
    RUN_TEST(repro_python_local_callable_alias_value_is_exact_reference);
    RUN_TEST(repro_python_read_before_local_assignment_has_no_graph_reference);
    RUN_TEST(repro_python_local_function_shadows_imported_callable);
    RUN_TEST(repro_python_alias_capture_precedes_later_import_rebinding);
    RUN_TEST(repro_python_later_class_clears_function_value_identity);
    RUN_TEST(repro_python_later_wildcard_import_clears_local_function_proof);
    RUN_TEST(repro_python_decorated_local_shadow_fails_closed);
    RUN_TEST(repro_python_module_if_assignment_shadow_fails_closed);
    RUN_TEST(repro_python_module_if_definition_shadow_fails_closed);
    RUN_TEST(repro_python_later_import_restores_imported_callable_identity);
    RUN_TEST(repro_python_wildcard_then_local_function_restores_exactness);
    RUN_TEST(repro_python_module_compound_binding_matrix_fails_closed);
    RUN_TEST(repro_python_annotation_only_preserves_imported_callable_identity);
    RUN_TEST(repro_rust_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_csharp_cross_file_exact_function_value_is_call_reference);
    RUN_TEST(repro_csharp_cross_file_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_kotlin_cross_file_callable_alias_invocation_targets_exact_value);
    RUN_TEST(repro_csharp_callable_alias_argument_is_exact_reference);
    RUN_TEST(repro_kotlin_callable_alias_argument_is_exact_reference);
    RUN_TEST(repro_kotlin_receiver_bound_alias_invocation_targets_exact_method);
    RUN_TEST(repro_kotlin_unknown_imported_property_is_not_callable_reference);
    RUN_TEST(repro_javascript_complex_function_value_stays_usage);
    RUN_TEST(repro_typescript_complex_function_value_stays_usage);
    RUN_TEST(repro_tsx_complex_function_value_stays_usage);
    RUN_TEST(repro_go_complex_function_value_stays_usage);
    RUN_TEST(repro_python_complex_function_value_stays_usage);
    RUN_TEST(repro_c_complex_function_value_stays_usage);
    RUN_TEST(repro_cpp_complex_function_value_stays_usage);
    RUN_TEST(repro_cuda_complex_function_value_stays_usage);
    RUN_TEST(repro_rust_complex_function_value_stays_usage);
    RUN_TEST(repro_csharp_complex_function_value_stays_usage);
    RUN_TEST(repro_java_complex_method_reference_stays_usage);
    RUN_TEST(repro_kotlin_complex_callable_reference_stays_usage);
    RUN_TEST(repro_php_complex_first_class_callable_stays_usage);
    RUN_TEST(repro_perl_conditional_coderefs_stay_usage);
    RUN_TEST(repro_c_ambiguous_alias_reassignment_drops_stale_target);
    RUN_TEST(repro_c_conditional_alias_reassignment_drops_stale_target);
    RUN_TEST(repro_c_noncallable_shadow_blocks_outer_alias_target);
    RUN_TEST(repro_cpp_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_cuda_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_go_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_python_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_rust_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_rust_unconditional_alias_reassignment_tracks_new_exact_target);
    RUN_TEST(repro_kotlin_conditional_alias_reference_fails_closed);
    RUN_TEST(repro_csharp_conditional_alias_reference_fails_closed);
}
