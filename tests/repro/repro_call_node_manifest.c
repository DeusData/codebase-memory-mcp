/*
 * Exhaustive contract for call-node metadata.
 *
 * This ledger is intentionally literal. Semantic role and extraction ownership
 * are independent review decisions; neither field supplies a default for the
 * other. Every row also states whether it belonged to the historical metadata
 * snapshot and whether it belongs in the primary call-node registry.
 *
 * This metadata contract is not proof of extraction behavior. Changes must also
 * pass the focused repro_call_scope_usages, repro_call_argument_matrix_a,
 * repro_call_argument_matrix_b, repro_lsp_c_cpp, repro_grammar_build, and
 * repro_grammar_shells suites before production metadata is changed.
 * C++/CUDA operator rows require raw-call-to-LSP assertions in repro_lsp_c_cpp;
 * Scala/Elixir direct operators require operand-scope assertions in the call
 * argument suites; Puppet resource ownership remains gated by repro_grammar_build.
 * TLA+ bound_op is historical direct-call metadata. Bounded quantification is
 * not a historical ledger row; repro_call_argument_matrix_b is its separate
 * negative behavior guard.
 */
#include "test_framework.h"
#include "lang_specs.h"

#include <stdbool.h>
#include <stdio.h>
#include <string.h>

typedef enum {
    DIRECT_CALL = 0,
    CONSTRUCTOR_CALL,
    OPERATOR_CALL,
    IMPLICIT_CALL,
    DSL_INVOCATION,
    BUILD_DEPENDENCY,
    CALLEE_WRAPPER,
    ARGUMENT_WRAPPER,
    CONTROL_OR_DOCUMENT_NONCALL,
    CALL_SEMANTIC_ROLE_COUNT
} CallSemanticRole;

typedef enum {
    PRIMARY_EXTRACTOR = 0,
    SYNTHETIC_EXTRACTOR,
    NO_CALL_EDGE,
    CALL_EXTRACTION_OWNER_COUNT
} CallExtractionOwner;

typedef struct {
    CBMLanguage language;
    const char *node_kind;
    CallSemanticRole semantic_role;
    CallExtractionOwner extraction_owner;
    bool historically_registered;
    bool expected_primary_registered;
} CallNodeManifestEntry;

enum {
    EXPECTED_HISTORICAL_CALL_NODE_TOTAL = 219,
    EXPECTED_ACTIVE_PRIMARY_TOTAL = 189,
    EXPECTED_DIRECT_CALLS = 155,
    EXPECTED_CONSTRUCTOR_CALLS = 20,
    EXPECTED_OPERATOR_CALLS = 12,
    EXPECTED_IMPLICIT_CALLS = 2,
    EXPECTED_DSL_INVOCATIONS = 14,
    EXPECTED_BUILD_DEPENDENCIES = 1,
    EXPECTED_CALLEE_WRAPPERS = 8,
    EXPECTED_ARGUMENT_WRAPPERS = 1,
    EXPECTED_CONTROL_OR_DOCUMENT_NONCALLS = 6,
    EXPECTED_PRIMARY_OWNERS = 189,
    EXPECTED_SYNTHETIC_OWNERS = 10,
    EXPECTED_NO_CALL_EDGE_OWNERS = 20,
    EXPECTED_PRIMARY_OPERATOR_CALLS = 4,
    EXPECTED_SYNTHETIC_OPERATOR_CALLS = 8,
};

#define ENTRY(language, kind, role, owner, historical, expected_primary) \
    {language, kind, role, owner, historical, expected_primary}

static const CallNodeManifestEntry CALL_NODE_MANIFEST[] = {
    ENTRY(CBM_LANG_GO, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PYTHON, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PYTHON, "with_statement", CONTROL_OR_DOCUMENT_NONCALL, NO_CALL_EDGE, true,
          false),
    ENTRY(CBM_LANG_JAVASCRIPT, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_JAVASCRIPT, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TYPESCRIPT, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TYPESCRIPT, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TSX, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TSX, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_RUST, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_RUST, "macro_invocation", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_JAVA, "method_invocation", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_JAVA, "object_creation_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_CPP, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CPP, "field_expression", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_CPP, "subscript_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CPP, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CPP, "delete_expression", IMPLICIT_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CPP, "binary_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CPP, "unary_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CPP, "update_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CSHARP, "invocation_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CSHARP, "object_creation_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_PHP, "member_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PHP, "scoped_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PHP, "function_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PHP, "object_creation_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_PHP, "nullsafe_member_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_LUA, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SCALA, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SCALA, "generic_function", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_SCALA, "field_expression", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_SCALA, "infix_expression", OPERATOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SCALA, "instance_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_KOTLIN, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_KOTLIN, "navigation_expression", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_RUBY, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_RUBY, "command_call", DIRECT_CALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_C, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_BASH, "command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ZIG, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ZIG, "builtin_function", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ELIXIR, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ELIXIR, "dot", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_ELIXIR, "binary_operator", OPERATOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_HASKELL, "infix", OPERATOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_HASKELL, "apply", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OCAML, "application_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OCAML, "infix_expression", OPERATOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OCAML, "method_invocation", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OCAML, "module_application", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OCAML, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OBJC, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OBJC, "message_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SWIFT, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SWIFT, "constructor_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_SWIFT, "macro_invocation", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SWIFT, "navigation_expression", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_DART, "selector", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_DART, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PERL, "ambiguous_function_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_PERL, "function_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PERL, "func1op_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PERL, "method_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GROOVY, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GROOVY, "juxt_function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ERLANG, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_R, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CSS, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SCSS, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SCSS, "include_statement", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_HCL, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SQL, "function_call", DIRECT_CALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_SQL, "invocation", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SQL, "command", CONTROL_OR_DOCUMENT_NONCALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_CLOJURE, "list_lit", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FSHARP, "application_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FSHARP, "dot_expression", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_JULIA, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_JULIA, "broadcast_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VIMSCRIPT, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VIMSCRIPT, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VIMSCRIPT, "command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_NIX, "apply_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_COMMONLISP, "list_lit", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ELM, "function_call", DIRECT_CALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_ELM, "function_call_expr", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FORTRAN, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FORTRAN, "keyword_argument", ARGUMENT_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_FORTRAN, "subroutine_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CUDA, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CUDA, "field_expression", CALLEE_WRAPPER, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_CUDA, "subscript_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CUDA, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CUDA, "delete_expression", IMPLICIT_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CUDA, "binary_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CUDA, "unary_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_CUDA, "update_expression", OPERATOR_CALL, SYNTHETIC_EXTRACTOR, true, false),
    ENTRY(CBM_LANG_COBOL, "call_statement", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VERILOG, "system_tf_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VERILOG, "subroutine_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VERILOG, "function_subroutine_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VERILOG, "method_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_EMACSLISP, "list", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MAKEFILE, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MAKEFILE, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MAKEFILE, "shell_function", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CMAKE, "normal_command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MESON, "normal_command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GLSL, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MATLAB, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MATLAB, "command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_LEAN, "apply", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_LEAN, "command", CONTROL_OR_DOCUMENT_NONCALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_FORM, "call_statement", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MAGMA, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_WOLFRAM, "apply", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SOLIDITY, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SOLIDITY, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SOLIDITY, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TYPST, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GDSCRIPT, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GDSCRIPT, "attribute_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GDSCRIPT, "base_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_QML, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_QML, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CFSCRIPT, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CFSCRIPT, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CFML, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GLEAM, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_POWERSHELL, "invokation_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_POWERSHELL, "command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PASCAL, "exprCall", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_DLANG, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_DLANG, "function_call_expression", DIRECT_CALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_DLANG, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SCHEME, "list", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FENNEL, "list", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FISH, "command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_AWK, "func_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_AWK, "command", CONTROL_OR_DOCUMENT_NONCALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_ZSH, "command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ZSH, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TCL, "command", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ADA, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ADA, "procedure_call_statement", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_AGDA, "module_application", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_AGDA, "expr", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_RACKET, "list", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ODIN, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ODIN, "selector_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_RESCRIPT, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PURESCRIPT, "exp_apply", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_NICKEL, "applicative", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CRYSTAL, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CRYSTAL, "command", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CRYSTAL, "implicit_object_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TEAL, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_HARE, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PONY, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_LUAU, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SWAY, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SWAY, "abi_call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_NASM, "call_syntax_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_NASM, "actual_instruction", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_JUST, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_JUST, "dependency", BUILD_DEPENDENCY, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GOTEMPLATE, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GOTEMPLATE, "method_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GOTEMPLATE, "template_action", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TEMPL, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PRISMA, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_DIFF, "command", CONTROL_OR_DOCUMENT_NONCALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_WGSL, "type_constructor_or_function_call_expression", DIRECT_CALL,
          PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_JSONNET, "functioncall", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_BIBTEX, "command", CONTROL_OR_DOCUMENT_NONCALL, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_STARLARK, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_BICEP, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_HLSL, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_HLSL, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VHDL, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VHDL, "procedure_call_statement", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_VHDL, "component_instantiation_statement", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR,
          true, true),
    ENTRY(CBM_LANG_VHDL, "parenthesis_group", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SYSTEMVERILOG, "function_subroutine_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_SYSTEMVERILOG, "system_tf_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SYSTEMVERILOG, "method_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_DEVICETREE, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_LINKERSCRIPT, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_GN, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_BITBAKE, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ISPC, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_ISPC, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CAIRO, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_CAIRO, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MOVE, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SQUIRREL, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FUNC, "method_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_FUNC, "function_application", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PUPPET, "function_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PUPPET, "resource_declaration", DSL_INVOCATION, NO_CALL_EDGE, true, false),
    ENTRY(CBM_LANG_PUPPET, "include_statement", DSL_INVOCATION, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SLANG, "call_expression", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_SLANG, "new_expression", CONSTRUCTOR_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_LLVM_IR, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_LLVM_IR, "invoke", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_LLVM_IR, "instruction_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TLAPLUS, "function_evaluation", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TLAPLUS, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_TLAPLUS, "bound_op", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_APEX, "method_invocation", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_PINE, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_MOJO, "call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OBJECTSCRIPT_UDL, "class_method_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_OBJECTSCRIPT_UDL, "method_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OBJECTSCRIPT_UDL, "relative_dot_method", DIRECT_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_OBJECTSCRIPT_UDL, "macro", DIRECT_CALL, PRIMARY_EXTRACTOR, true, true),
    ENTRY(CBM_LANG_OBJECTSCRIPT_ROUTINE, "extrinsic_function", DIRECT_CALL, PRIMARY_EXTRACTOR, true,
          true),
    ENTRY(CBM_LANG_OBJECTSCRIPT_ROUTINE, "routine_tag_call", DIRECT_CALL, PRIMARY_EXTRACTOR, true,
          true),
};

#undef ENTRY

#define MANIFEST_COUNT (sizeof(CALL_NODE_MANIFEST) / sizeof(CALL_NODE_MANIFEST[0]))

_Static_assert(MANIFEST_COUNT == EXPECTED_HISTORICAL_CALL_NODE_TOTAL,
               "call-node manifest must contain exactly 219 entries");
_Static_assert(EXPECTED_DIRECT_CALLS + EXPECTED_CONSTRUCTOR_CALLS + EXPECTED_OPERATOR_CALLS +
                       EXPECTED_IMPLICIT_CALLS + EXPECTED_DSL_INVOCATIONS +
                       EXPECTED_BUILD_DEPENDENCIES + EXPECTED_CALLEE_WRAPPERS +
                       EXPECTED_ARGUMENT_WRAPPERS + EXPECTED_CONTROL_OR_DOCUMENT_NONCALLS ==
                   EXPECTED_HISTORICAL_CALL_NODE_TOTAL,
               "semantic-role partition must cover the historical manifest");
_Static_assert(EXPECTED_PRIMARY_OWNERS + EXPECTED_SYNTHETIC_OWNERS + EXPECTED_NO_CALL_EDGE_OWNERS ==
                   EXPECTED_HISTORICAL_CALL_NODE_TOTAL,
               "extraction-owner partition must cover the historical manifest");
_Static_assert(EXPECTED_PRIMARY_OWNERS == EXPECTED_ACTIVE_PRIMARY_TOTAL,
               "primary-owner and active-primary totals must agree");
_Static_assert(EXPECTED_PRIMARY_OPERATOR_CALLS + EXPECTED_SYNTHETIC_OPERATOR_CALLS ==
                   EXPECTED_OPERATOR_CALLS,
               "operator calls must have an explicit direct or synthetic owner");

static int manifest_match_count(CBMLanguage language, const char *node_kind) {
    int matches = 0;
    for (size_t i = 0; i < MANIFEST_COUNT; i++) {
        const CallNodeManifestEntry *entry = &CALL_NODE_MANIFEST[i];
        if (entry->language == language && strcmp(entry->node_kind, node_kind) == 0) {
            matches++;
        }
    }
    return matches;
}

static int spec_match_count(const CBMLangSpec *spec, const char *node_kind) {
    int matches = 0;
    if (!spec || !spec->call_node_types) {
        return 0;
    }
    for (int i = 0; spec->call_node_types[i]; i++) {
        if (strcmp(spec->call_node_types[i], node_kind) == 0) {
            matches++;
        }
    }
    return matches;
}

TEST(repro_call_node_manifest_is_unique_and_partitioned) {
    int failures = 0;
    int role_counts[CALL_SEMANTIC_ROLE_COUNT] = {0};
    int owner_counts[CALL_EXTRACTION_OWNER_COUNT] = {0};
    int historical_total = 0;
    int expected_primary_total = 0;
    int primary_operator_calls = 0;
    int synthetic_operator_calls = 0;

    if (MANIFEST_COUNT != EXPECTED_HISTORICAL_CALL_NODE_TOTAL) {
        fprintf(stderr, "  [call-node-manifest] invariant=table_total expected=%d actual=%zu\n",
                EXPECTED_HISTORICAL_CALL_NODE_TOTAL, MANIFEST_COUNT);
        failures++;
    }

    for (size_t i = 0; i < MANIFEST_COUNT; i++) {
        const CallNodeManifestEntry *entry = &CALL_NODE_MANIFEST[i];
        if (entry->language < 0 || entry->language >= CBM_LANG_COUNT ||
            entry->language == CBM_LANG_NIM) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=invalid_language index=%zu language=%d\n", i,
                    (int)entry->language);
            failures++;
        }
        if (!entry->node_kind || !entry->node_kind[0]) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=empty_node_kind index=%zu language=%d\n", i,
                    (int)entry->language);
            failures++;
        }
        if (entry->semantic_role < DIRECT_CALL ||
            entry->semantic_role >= CALL_SEMANTIC_ROLE_COUNT) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=invalid_role index=%zu language=%d role=%d\n",
                    i, (int)entry->language, (int)entry->semantic_role);
            failures++;
        } else {
            role_counts[entry->semantic_role]++;
        }
        if (entry->extraction_owner < PRIMARY_EXTRACTOR ||
            entry->extraction_owner >= CALL_EXTRACTION_OWNER_COUNT) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=invalid_owner index=%zu language=%d "
                    "owner=%d\n",
                    i, (int)entry->language, (int)entry->extraction_owner);
            failures++;
        } else {
            owner_counts[entry->extraction_owner]++;
        }

        if (!entry->historically_registered) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=historical_snapshot_gap index=%zu "
                    "language=%d node=%s\n",
                    i, (int)entry->language, entry->node_kind);
            failures++;
        } else {
            historical_total++;
        }
        if (entry->expected_primary_registered) {
            expected_primary_total++;
        }

        /*
         * Direct operator models are PRIMARY_EXTRACTOR entries. C++/CUDA
         * operator triggers owned by synthetic extraction must not also create
         * generic primary scopes or raw calls.
         */
        if (entry->extraction_owner == SYNTHETIC_EXTRACTOR && entry->expected_primary_registered) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=synthetic_owner_registered_primary "
                    "language=%d node=%s role=%d\n",
                    (int)entry->language, entry->node_kind, (int)entry->semantic_role);
            failures++;
        }
        if (entry->extraction_owner == NO_CALL_EDGE && entry->expected_primary_registered) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=no_edge_owner_registered_primary "
                    "language=%d node=%s role=%d\n",
                    (int)entry->language, entry->node_kind, (int)entry->semantic_role);
            failures++;
        }
        if ((entry->extraction_owner == PRIMARY_EXTRACTOR) != entry->expected_primary_registered) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=primary_owner_membership_mismatch "
                    "language=%d node=%s owner=%d expected_primary=%d\n",
                    (int)entry->language, entry->node_kind, (int)entry->extraction_owner,
                    entry->expected_primary_registered);
            failures++;
        }

        if (entry->semantic_role == OPERATOR_CALL) {
            if (entry->extraction_owner == PRIMARY_EXTRACTOR) {
                primary_operator_calls++;
            } else if (entry->extraction_owner == SYNTHETIC_EXTRACTOR) {
                synthetic_operator_calls++;
            } else {
                fprintf(stderr,
                        "  [call-node-manifest] invariant=operator_without_extractor "
                        "language=%d node=%s\n",
                        (int)entry->language, entry->node_kind);
                failures++;
            }
        }

        for (size_t j = i + 1; j < MANIFEST_COUNT; j++) {
            const CallNodeManifestEntry *other = &CALL_NODE_MANIFEST[j];
            if (entry->language == other->language && entry->node_kind && other->node_kind &&
                strcmp(entry->node_kind, other->node_kind) == 0) {
                fprintf(stderr,
                        "  [call-node-manifest] invariant=duplicate_table_entry language=%d "
                        "node=%s first=%zu second=%zu\n",
                        (int)entry->language, entry->node_kind, i, j);
                failures++;
            }
        }
    }

    if (historical_total != EXPECTED_HISTORICAL_CALL_NODE_TOTAL ||
        expected_primary_total != EXPECTED_ACTIVE_PRIMARY_TOTAL) {
        fprintf(stderr,
                "  [call-node-manifest] invariant=registration_partition historical=%d "
                "expected_primary=%d\n",
                historical_total, expected_primary_total);
        failures++;
    }

    if (role_counts[DIRECT_CALL] != EXPECTED_DIRECT_CALLS ||
        role_counts[CONSTRUCTOR_CALL] != EXPECTED_CONSTRUCTOR_CALLS ||
        role_counts[OPERATOR_CALL] != EXPECTED_OPERATOR_CALLS ||
        role_counts[IMPLICIT_CALL] != EXPECTED_IMPLICIT_CALLS ||
        role_counts[DSL_INVOCATION] != EXPECTED_DSL_INVOCATIONS ||
        role_counts[BUILD_DEPENDENCY] != EXPECTED_BUILD_DEPENDENCIES ||
        role_counts[CALLEE_WRAPPER] != EXPECTED_CALLEE_WRAPPERS ||
        role_counts[ARGUMENT_WRAPPER] != EXPECTED_ARGUMENT_WRAPPERS ||
        role_counts[CONTROL_OR_DOCUMENT_NONCALL] != EXPECTED_CONTROL_OR_DOCUMENT_NONCALLS) {
        fprintf(stderr,
                "  [call-node-manifest] invariant=semantic_role_partition direct=%d "
                "constructor=%d operator=%d implicit=%d dsl=%d build=%d callee_wrapper=%d "
                "argument_wrapper=%d control_or_document=%d\n",
                role_counts[DIRECT_CALL], role_counts[CONSTRUCTOR_CALL], role_counts[OPERATOR_CALL],
                role_counts[IMPLICIT_CALL], role_counts[DSL_INVOCATION],
                role_counts[BUILD_DEPENDENCY], role_counts[CALLEE_WRAPPER],
                role_counts[ARGUMENT_WRAPPER], role_counts[CONTROL_OR_DOCUMENT_NONCALL]);
        failures++;
    }

    if (owner_counts[PRIMARY_EXTRACTOR] != EXPECTED_PRIMARY_OWNERS ||
        owner_counts[SYNTHETIC_EXTRACTOR] != EXPECTED_SYNTHETIC_OWNERS ||
        owner_counts[NO_CALL_EDGE] != EXPECTED_NO_CALL_EDGE_OWNERS) {
        fprintf(stderr,
                "  [call-node-manifest] invariant=extraction_owner_partition primary=%d "
                "synthetic=%d no_call_edge=%d\n",
                owner_counts[PRIMARY_EXTRACTOR], owner_counts[SYNTHETIC_EXTRACTOR],
                owner_counts[NO_CALL_EDGE]);
        failures++;
    }

    if (primary_operator_calls != EXPECTED_PRIMARY_OPERATOR_CALLS ||
        synthetic_operator_calls != EXPECTED_SYNTHETIC_OPERATOR_CALLS) {
        fprintf(stderr,
                "  [call-node-manifest] invariant=operator_owner_partition primary=%d "
                "synthetic=%d\n",
                primary_operator_calls, synthetic_operator_calls);
        failures++;
    }

    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_node_manifest_live_specs_contain_only_expected_primary_entries) {
    int failures = 0;
    int runtime_total = 0;

    for (int value = 0; value < CBM_LANG_COUNT; value++) {
        CBMLanguage language = (CBMLanguage)value;
        const CBMLangSpec *spec = cbm_lang_spec(language);

        /* The registry reproduction owns Nim's stale enum and Studio Export's
         * transform-only, intentionally grammar-free classification. */
        if (language == CBM_LANG_NIM || language == CBM_LANG_OBJECTSCRIPT_EXPORT) {
            continue;
        }
        if (!spec || spec->language != language) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=live_spec_missing language=%d actual=%d\n",
                    value, spec ? (int)spec->language : -1);
            failures++;
            continue;
        }
        if (!spec->call_node_types) {
            continue;
        }

        for (int i = 0; spec->call_node_types[i]; i++) {
            const char *node_kind = spec->call_node_types[i];
            runtime_total++;

            int table_matches = manifest_match_count(language, node_kind);
            if (table_matches != 1) {
                fprintf(stderr,
                        "  [call-node-manifest] invariant=live_to_table language=%d node=%s "
                        "matches=%d\n",
                        value, node_kind, table_matches);
                failures++;
            } else {
                for (size_t manifest_index = 0; manifest_index < MANIFEST_COUNT; manifest_index++) {
                    const CallNodeManifestEntry *entry = &CALL_NODE_MANIFEST[manifest_index];
                    if (entry->language == language && strcmp(entry->node_kind, node_kind) == 0 &&
                        !entry->expected_primary_registered) {
                        fprintf(stderr,
                                "  [call-node-manifest] invariant=unexpected_primary_registration "
                                "language=%d node=%s role=%d owner=%d\n",
                                value, node_kind, (int)entry->semantic_role,
                                (int)entry->extraction_owner);
                        failures++;
                        break;
                    }
                }
            }
            for (int j = 0; j < i; j++) {
                if (strcmp(spec->call_node_types[j], node_kind) == 0) {
                    fprintf(stderr,
                            "  [call-node-manifest] invariant=duplicate_live_kind language=%d "
                            "node=%s first=%d second=%d\n",
                            value, node_kind, j, i);
                    failures++;
                }
            }
        }
    }

    if (runtime_total != EXPECTED_ACTIVE_PRIMARY_TOTAL) {
        fprintf(stderr, "  [call-node-manifest] invariant=runtime_total expected=%d actual=%d\n",
                EXPECTED_ACTIVE_PRIMARY_TOTAL, runtime_total);
        failures++;
    }

    ASSERT_EQ(failures, 0);
    PASS();
}

TEST(repro_call_node_manifest_live_membership_matches_expected_primary_boolean) {
    int failures = 0;

    for (size_t i = 0; i < MANIFEST_COUNT; i++) {
        const CallNodeManifestEntry *entry = &CALL_NODE_MANIFEST[i];
        const CBMLangSpec *spec = cbm_lang_spec(entry->language);
        if (!spec || spec->language != entry->language) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=table_spec_missing index=%zu language=%d "
                    "actual=%d\n",
                    i, (int)entry->language, spec ? (int)spec->language : -1);
            failures++;
            continue;
        }

        int live_matches = spec_match_count(spec, entry->node_kind);
        int expected_matches = entry->expected_primary_registered ? 1 : 0;
        if (live_matches != expected_matches) {
            fprintf(stderr,
                    "  [call-node-manifest] invariant=table_to_live index=%zu language=%d node=%s "
                    "expected=%d matches=%d role=%d owner=%d historical=%d\n",
                    i, (int)entry->language, entry->node_kind, expected_matches, live_matches,
                    (int)entry->semantic_role, (int)entry->extraction_owner,
                    entry->historically_registered);
            failures++;
        }
    }

    ASSERT_EQ(failures, 0);
    PASS();
}

SUITE(repro_call_node_manifest) {
    RUN_TEST(repro_call_node_manifest_is_unique_and_partitioned);
    RUN_TEST(repro_call_node_manifest_live_specs_contain_only_expected_primary_entries);
    RUN_TEST(repro_call_node_manifest_live_membership_matches_expected_primary_boolean);
}
