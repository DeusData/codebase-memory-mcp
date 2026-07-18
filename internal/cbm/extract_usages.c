#include "cbm.h"
#include "helpers.h"
#include "lang_specs.h"
#include "extract_unified.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"
#include "extract_node_stack.h"

enum { LAST_IDX = 1 };
#include <stdint.h> // uint32_t
#include <stdlib.h> // qsort
#include <string.h>
#include <strings.h>
#include <ctype.h>

#if defined(CBM_CALL_REFERENCE_LOOKUP_TEST_API) && CBM_CALL_REFERENCE_LOOKUP_TEST_API
#include <stdatomic.h>

static _Atomic uint64_t g_usage_field_lookup_work = 0;
static _Atomic uint64_t g_usage_slow_parent_fallbacks = 0;

void cbm_usage_field_lookup_test_reset(void) {
    atomic_store_explicit(&g_usage_field_lookup_work, 0, memory_order_relaxed);
    atomic_store_explicit(&g_usage_slow_parent_fallbacks, 0, memory_order_relaxed);
}

uint64_t cbm_usage_field_lookup_test_work(void) {
    return atomic_load_explicit(&g_usage_field_lookup_work, memory_order_relaxed);
}

uint64_t cbm_usage_slow_parent_fallback_test_count(void) {
    return atomic_load_explicit(&g_usage_slow_parent_fallbacks, memory_order_relaxed);
}

static void usage_field_lookup_test_note_work(void) {
    atomic_fetch_add_explicit(&g_usage_field_lookup_work, 1, memory_order_relaxed);
}

static void usage_slow_parent_fallback_test_note(void) {
    atomic_fetch_add_explicit(&g_usage_slow_parent_fallbacks, 1, memory_order_relaxed);
}
#else
static void usage_field_lookup_test_note_work(void) {}
static void usage_slow_parent_fallback_test_note(void) {}
#endif

// Forward declaration
static void walk_usages(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec);
static bool is_direct_argument_value(TSNode node);
static TSNode python_direct_callable_attribute_site(TSNode node);

// Check if a node is inside a call expression (to avoid double-counting as usage)
static bool is_inside_call(TSNode node, const CBMLangSpec *spec) {
    TSNode cur = ts_node_parent(node);
    while (!ts_node_is_null(cur)) {
        if (cbm_kind_in_set(cur, spec->call_node_types)) {
            return true;
        }
        cur = ts_node_parent(cur);
    }
    return false;
}

// Check if a node is inside an import statement
static bool is_inside_import(TSNode node, const CBMLangSpec *spec) {
    bool has_imports = spec->import_node_types && spec->import_node_types[0];
    bool has_from_imports = spec->import_from_types && spec->import_from_types[0];
    if (!has_imports && !has_from_imports) {
        return false;
    }
    TSNode cur = ts_node_parent(node);
    while (!ts_node_is_null(cur)) {
        if ((has_imports && cbm_kind_in_set(cur, spec->import_node_types)) ||
            (has_from_imports && cbm_kind_in_set(cur, spec->import_from_types))) {
            return true;
        }
        cur = ts_node_parent(cur);
    }
    return false;
}

// Is this an identifier-like node that represents a reference?
static bool is_reference_node(TSNode node, CBMLanguage lang) {
    const char *kind = ts_node_type(node);

    /* Python's attribute node and its terminal identifier describe the same
     * source occurrence. For a direct callable-value argument, keep the leaf
     * as the raw carrier (so unresolved cases retain a useful short-name
     * USAGE) and stamp it with the full attribute span below. Receivers remain
     * independent value references. */
    if (lang == CBM_LANG_PYTHON && strcmp(kind, "attribute") == 0 &&
        !ts_node_is_null(python_direct_callable_attribute_site(node))) {
        return false;
    }

    /* A Rust scoped_identifier owns the complete path occurrence. Its nested
     * path/identifier children are grammar structure, not additional value
     * references; keeping them would let a short-name fallback bind a decoy. */
    if (lang == CBM_LANG_RUST &&
        (strcmp(kind, "identifier") == 0 || strcmp(kind, "scoped_identifier") == 0)) {
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "scoped_identifier") == 0) {
            return false;
        }
    }

    /* Some grammars expose the sigil/scope as a named wrapper around a generic
     * identifier. Keep exactly one occurrence: the wrapper carries the source
     * spelling that resolution needs (`$watched`, `a:watched`). */
    if (strcmp(kind, "identifier") == 0) {
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) &&
            ((lang == CBM_LANG_PUPPET && strcmp(ts_node_type(parent), "variable") == 0) ||
             (lang == CBM_LANG_VIMSCRIPT && strcmp(ts_node_type(parent), "argument") == 0))) {
            return false;
        }
    }

    // Common identifier types across languages
    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0 ||
        strcmp(kind, "type_identifier") == 0) {
        return true;
    }

    // Language-specific reference types
    switch (lang) {
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_QML:
    case CBM_LANG_CFSCRIPT:
        return strcmp(kind, "property_identifier") == 0 ||
               strcmp(kind, "private_property_identifier") == 0;
    case CBM_LANG_GO:
        return strcmp(kind, "field_identifier") == 0 || strcmp(kind, "package_identifier") == 0;
    case CBM_LANG_PYTHON:
        return strcmp(kind, "attribute") == 0;
    case CBM_LANG_RUST:
        return strcmp(kind, "field_identifier") == 0 || strcmp(kind, "scoped_identifier") == 0;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
        return strcmp(kind, "field_identifier") == 0;
    case CBM_LANG_PHP:
        return strcmp(kind, "name") == 0 || strcmp(kind, "variable_name") == 0;
    case CBM_LANG_HASKELL:
        return strcmp(kind, "variable") == 0 || strcmp(kind, "constructor") == 0;
    case CBM_LANG_OCAML:
        return strcmp(kind, "value_path") == 0 || strcmp(kind, "constructor_path") == 0;
    case CBM_LANG_ERLANG:
        return strcmp(kind, "atom") == 0 || strcmp(kind, "var") == 0;
    case CBM_LANG_CSS:
        /* Custom-property reads inside `var(...)` are exposed as plain_value;
         * declarations use the separate property_name alias. */
        return strcmp(kind, "plain_value") == 0;
    case CBM_LANG_SCSS:
        /* SCSS value occurrences retain their `$` spelling in variable_value;
         * declaration parameters use the distinct variable_name node. */
        return strcmp(kind, "variable_value") == 0;
    case CBM_LANG_LLVM_IR:
        return strcmp(kind, "local_var") == 0 || strcmp(kind, "global_var") == 0;
    case CBM_LANG_PUPPET:
    case CBM_LANG_POWERSHELL:
        /* Puppet keeps the sigil in a dedicated `variable` node (`$watched`). */
        return strcmp(kind, "variable") == 0;
    case CBM_LANG_BASH:
    case CBM_LANG_FISH:
    case CBM_LANG_ZSH:
        /* Expansion wrappers retain `$`; their named leaf is the bare variable name. */
        return strcmp(kind, "variable_name") == 0;
    case CBM_LANG_PERL:
        /* Perl's named scalar node retains its sigil (`$watched`). */
        return strcmp(kind, "scalar") == 0;
    case CBM_LANG_CLOJURE:
    case CBM_LANG_COMMONLISP:
        return strcmp(kind, "sym_lit") == 0;
    case CBM_LANG_VIMSCRIPT:
        return strcmp(kind, "scoped_identifier") == 0 || strcmp(kind, "argument") == 0;
    case CBM_LANG_ELM:
        return strcmp(kind, "lower_case_identifier") == 0;
    case CBM_LANG_COBOL:
        return strcmp(kind, "qualified_word") == 0;
    case CBM_LANG_EMACSLISP:
    case CBM_LANG_SCHEME:
    case CBM_LANG_FENNEL:
    case CBM_LANG_RACKET:
    case CBM_LANG_LINKERSCRIPT:
        return strcmp(kind, "symbol") == 0;
    case CBM_LANG_MAKEFILE:
        return strcmp(kind, "variable_reference") == 0;
    case CBM_LANG_CMAKE:
        /* `${watched}` contains a named `variable` leaf spanning only `watched`. */
        return strcmp(kind, "variable") == 0;
    case CBM_LANG_WOLFRAM:
        return strcmp(kind, "user_symbol") == 0;
    case CBM_LANG_TYPST:
        return strcmp(kind, "ident") == 0;
    case CBM_LANG_TCL:
        /* Tcl's value spelling includes the sigil, so retain the substitution wrapper. */
        return strcmp(kind, "variable_substitution") == 0;
    case CBM_LANG_TLAPLUS:
        /* Definition-side names/parameters are `identifier`; expression-side
         * values and operator arguments use the distinct `identifier_ref`. */
        return strcmp(kind, "identifier_ref") == 0;
    case CBM_LANG_AGDA:
        /* `_qid` is hidden and aliases its public expression leaf to `qid`. */
        return strcmp(kind, "qid") == 0;
    case CBM_LANG_RESCRIPT:
        return strcmp(kind, "value_identifier") == 0;
    case CBM_LANG_PURESCRIPT:
        return strcmp(kind, "variable") == 0;
    case CBM_LANG_NICKEL:
        return strcmp(kind, "ident") == 0;
    case CBM_LANG_JSONNET:
        return strcmp(kind, "id") == 0;
    case CBM_LANG_CFML:
        return strcmp(kind, "property_identifier") == 0;
    case CBM_LANG_OBJECTSCRIPT_UDL:
    case CBM_LANG_OBJECTSCRIPT_ROUTINE:
        return strcmp(kind, "objectscript_identifier") == 0 ||
               strcmp(kind, "objectscript_identifier_special") == 0;
    default:
        return false;
    }
}

static bool node_contains(TSNode outer, TSNode inner) {
    return !ts_node_is_null(outer) && !ts_node_is_null(inner) &&
           ts_node_start_byte(outer) <= ts_node_start_byte(inner) &&
           ts_node_end_byte(outer) >= ts_node_end_byte(inner);
}

static bool vhdl_forward_callee_wrapper(TSNode node) {
    const char *kind = ts_node_type(node);
    return strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0 ||
           strcmp(kind, "library_function") == 0 || strcmp(kind, "name") == 0 ||
           strcmp(kind, "simple_name") == 0;
}

static TSNode terminal_vhdl_identifier(TSNode node, int remaining_depth) {
    if (ts_node_is_null(node) || remaining_depth < 0) {
        return (TSNode){0};
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = count; i > 0; i--) {
        TSNode found =
            terminal_vhdl_identifier(ts_node_named_child(node, i - 1), remaining_depth - 1);
        if (!ts_node_is_null(found)) {
            return found;
        }
    }
    const char *kind = ts_node_type(node);
    return strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0 ? node
                                                                                     : (TSNode){0};
}

// Dart and VHDL represent some calls as a callee node immediately followed by
// the invocation node. The unified pre-order walk has already visited that
// callee when the call descriptor is created, so suppress only the exact
// language-specific preceding occurrence. Do not generalize this to arbitrary
// identifiers: a sibling selector/parenthesis group is the grammar contract.
static bool is_forward_sibling_callee(CBMLanguage language, TSNode node) {
    if (language == CBM_LANG_DART && strcmp(ts_node_type(node), "identifier") == 0) {
        TSNode next = ts_node_next_named_sibling(node);
        return !ts_node_is_null(next) && strcmp(ts_node_type(next), "selector") == 0;
    }

    if (language != CBM_LANG_VHDL) {
        return false;
    }

    TSNode owner = node;
    for (int depth = 0; depth < 4 && vhdl_forward_callee_wrapper(owner); depth++) {
        TSNode next = ts_node_next_named_sibling(owner);
        if (!ts_node_is_null(next) && strcmp(ts_node_type(next), "parenthesis_group") == 0) {
            TSNode leaf = terminal_vhdl_identifier(owner, 8);
            return !ts_node_is_null(leaf) && ts_node_eq(node, leaf);
        }
        TSNode parent = ts_node_parent(owner);
        if (ts_node_is_null(parent) || !vhdl_forward_callee_wrapper(parent)) {
            break;
        }
        owner = parent;
    }
    return false;
}

static const char *field_name_for_node(TSNode parent, TSNode child) {
    const char *field = NULL;
    TSTreeCursor cursor = ts_tree_cursor_new(parent);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            usage_field_lookup_test_note_work();
            if (ts_node_eq(ts_tree_cursor_current_node(&cursor), child)) {
                field = ts_tree_cursor_current_field_name(&cursor);
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    return field;
}

/* The unified walk already owns the exact cursor path to the current node.
 * Reset one reusable scratch cursor to that path before each occurrence
 * classifier, then walk its parents without restarting a sibling scan at
 * every level. Legacy extraction callers retain the node-based fallback. */
static TSTreeCursor *reset_occurrence_cursor(WalkState *state, TSNode node) {
    if (!state || !state->current_cursor || !state->occurrence_cursor ||
        !ts_node_eq(ts_tree_cursor_current_node(state->current_cursor), node)) {
        return NULL;
    }
    ts_tree_cursor_reset_to(state->occurrence_cursor, state->current_cursor);
    return state->occurrence_cursor;
}

static bool occurrence_parent(TSTreeCursor *cursor, TSNode current, TSNode *parent,
                              const char **field) {
    if (cursor) {
        *field = ts_tree_cursor_current_field_name(cursor);
        usage_field_lookup_test_note_work();
        if (!ts_tree_cursor_goto_parent(cursor)) {
            *parent = (TSNode){0};
            return false;
        }
        *parent = ts_tree_cursor_current_node(cursor);
        return true;
    }

    *parent = ts_node_parent(current);
    if (ts_node_is_null(*parent)) {
        *field = NULL;
        return false;
    }
    *field = field_name_for_node(*parent, current);
    return true;
}

static const char *field_name_for_walk_node(WalkState *state, TSNode parent, TSNode node) {
    if (state && state->current_cursor &&
        ts_node_eq(ts_tree_cursor_current_node(state->current_cursor), node)) {
        usage_field_lookup_test_note_work();
        return ts_tree_cursor_current_field_name(state->current_cursor);
    }
    return field_name_for_node(parent, node);
}

static bool is_value_field(const char *field) {
    return field && (strcmp(field, "value") == 0 || strcmp(field, "right") == 0 ||
                     strcmp(field, "initializer") == 0 || strcmp(field, "default") == 0 ||
                     strcmp(field, "default_value") == 0 || strcmp(field, "body") == 0 ||
                     strcmp(field, "arguments") == 0 || strcmp(field, "condition") == 0 ||
                     strcmp(field, "consequence") == 0 || strcmp(field, "alternative") == 0 ||
                     strcmp(field, "expression") == 0 || strcmp(field, "result") == 0);
}

static bool kind_in_exact_set(const char *kind, const char *const *set) {
    if (!kind || !set) {
        return false;
    }
    for (const char *const *candidate = set; *candidate; candidate++) {
        if (strcmp(kind, *candidate) == 0) {
            return true;
        }
    }
    return false;
}

static bool field_contains_node(TSNode parent, const char *field, TSNode node) {
    TSNode value = ts_node_child_by_field_name(parent, field, (uint32_t)strlen(field));
    return node_contains(value, node);
}

typedef enum {
    CBM_OCCURRENCE_STANDARD = 0,
    CBM_OCCURRENCE_LISP_DEF,
    CBM_OCCURRENCE_COMMONLISP_DEFUN,
    CBM_OCCURRENCE_FENNEL_FN,
    CBM_OCCURRENCE_ELIXIR_DEF,
    CBM_OCCURRENCE_JULIA_FUNCTION,
    CBM_OCCURRENCE_WOLFRAM_SET,
    CBM_OCCURRENCE_TYPST_LET,
    CBM_OCCURRENCE_AGDA_FUNCTION,
    CBM_OCCURRENCE_TLAPLUS_OPERATOR,
    CBM_OCCURRENCE_COBOL_MOVE,
    CBM_OCCURRENCE_HCL_ATTRIBUTE,
    CBM_OCCURRENCE_ELM_VALUE,
    CBM_OCCURRENCE_RESCRIPT_LET,
    CBM_OCCURRENCE_PURESCRIPT_LHS,
    CBM_OCCURRENCE_NICKEL_LET,
    CBM_OCCURRENCE_ERLANG_CLAUSE,
    CBM_OCCURRENCE_NIX_FUNCTION,
    CBM_OCCURRENCE_MATLAB_ARGUMENTS,
    CBM_OCCURRENCE_LEAN_BINDER,
    CBM_OCCURRENCE_PASCAL_PROC,
    CBM_OCCURRENCE_TEAL_FUNCTION,
    CBM_OCCURRENCE_VHDL_INTERFACE,
    CBM_OCCURRENCE_PINE_FUNCTION,
    CBM_OCCURRENCE_LLVM_FUNCTION,
} CBMOccurrencePolicy;

typedef struct {
    const char *const *whole_binding_nodes;
    const char *const *write_nodes;
    CBMOccurrencePolicy policy;
    bool first_named_child_is_write;
} CBMOccurrenceSpec;

/* These are exact grammar roles, not name fragments.  Default/value/body
 * fields are barriers, so an initializer nested below one of these containers
 * remains a read. */
static const char *const common_whole_binding_nodes[] = {"formal_parameter",
                                                         "formal_parameters",
                                                         "parameter",
                                                         "parameters",
                                                         "parameter_list",
                                                         "parameter_declaration",
                                                         "parameter_specification",
                                                         "required_parameter",
                                                         "optional_parameter",
                                                         "default_parameter",
                                                         "typed_parameter",
                                                         "function_value_parameter",
                                                         "function_value_parameters",
                                                         "lambda_parameter",
                                                         "lambda_parameters",
                                                         "function_parameter_declaration",
                                                         "closure_parameters",
                                                         "block_parameters",
                                                         "receiver",
                                                         NULL};

static const char *const field_binding_nodes[] = {"variable_declarator",
                                                  "init_declarator",
                                                  "variable_declaration",
                                                  "const_declaration",
                                                  "lexical_declaration",
                                                  "short_var_declaration",
                                                  "local_variable_declaration",
                                                  "property_declaration",
                                                  "field_declaration",
                                                  "value_declaration",
                                                  "val_definition",
                                                  "var_definition",
                                                  "let_declaration",
                                                  "local_bind",
                                                  "let_binding",
                                                  "data_declaration",
                                                  "net_declaration",
                                                  "object_declaration",
                                                  "number_declaration",
                                                  "typed_binding",
                                                  "variable_assignment",
                                                  NULL};

static const char *const binding_fields[] = {"name",       "pattern", "declarator", "parameter",
                                             "parameters", "left",    "variable",   "variables",
                                             "key",        NULL};

static const char *const sql_binding_nodes[] = {"function_argument", NULL};
static const char *const haskell_binding_nodes[] = {"patterns", NULL};
static const char *const fsharp_binding_nodes[] = {
    "function_declaration_left", "value_declaration_left", "argument_patterns", NULL};
static const char *const crystal_binding_nodes[] = {"param_list", NULL};
static const char *const awk_binding_nodes[] = {"param_list", NULL};
static const char *const teal_binding_nodes[] = {"function_signature", NULL};
static const char *const systemverilog_binding_nodes[] = {"tf_port_item", "tf_port_item1", NULL};
static const char *const rescript_binding_nodes[] = {"formal_parameters", "labeled_parameter",
                                                     "parameter", NULL};
static const char *const purescript_binding_nodes[] = {"bind_pattern", "pattern", "patterns", NULL};
static const char *const nickel_binding_nodes[] = {"pattern_fun", NULL};
static const char *const jsonnet_binding_nodes[] = {"param", NULL};
static const char *const llvm_binding_nodes[] = {"function_header", NULL};
static const char *const linkerscript_write_nodes[] = {"assignment", NULL};
static const char *const meson_write_nodes[] = {"operatorunit", NULL};
static const char *const gn_write_nodes[] = {"assignment_statement", NULL};
static const char *const objectscript_binding_nodes[] = {"argument", "tag_parameter", NULL};
static const char *const objectscript_write_nodes[] = {"set_argument", NULL};

/* Parallel to lang_specs: occurrence semantics evolve without adding fields to
 * the positional CBMLangSpec initializer used by every language. */
static const CBMOccurrenceSpec occurrence_specs[CBM_LANG_COUNT] = {
    [CBM_LANG_SQL] = {sql_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_CLOJURE] = {NULL, NULL, CBM_OCCURRENCE_LISP_DEF, false},
    [CBM_LANG_SCHEME] = {NULL, NULL, CBM_OCCURRENCE_LISP_DEF, false},
    [CBM_LANG_RACKET] = {NULL, NULL, CBM_OCCURRENCE_LISP_DEF, false},
    [CBM_LANG_COMMONLISP] = {NULL, NULL, CBM_OCCURRENCE_COMMONLISP_DEFUN, false},
    [CBM_LANG_FENNEL] = {NULL, NULL, CBM_OCCURRENCE_FENNEL_FN, false},
    [CBM_LANG_ELIXIR] = {NULL, NULL, CBM_OCCURRENCE_ELIXIR_DEF, false},
    [CBM_LANG_JULIA] = {NULL, NULL, CBM_OCCURRENCE_JULIA_FUNCTION, false},
    [CBM_LANG_WOLFRAM] = {NULL, NULL, CBM_OCCURRENCE_WOLFRAM_SET, false},
    [CBM_LANG_TYPST] = {NULL, NULL, CBM_OCCURRENCE_TYPST_LET, false},
    [CBM_LANG_AGDA] = {NULL, NULL, CBM_OCCURRENCE_AGDA_FUNCTION, false},
    [CBM_LANG_TLAPLUS] = {NULL, NULL, CBM_OCCURRENCE_TLAPLUS_OPERATOR, false},
    [CBM_LANG_COBOL] = {NULL, NULL, CBM_OCCURRENCE_COBOL_MOVE, false},
    [CBM_LANG_HCL] = {NULL, NULL, CBM_OCCURRENCE_HCL_ATTRIBUTE, false},
    [CBM_LANG_ELM] = {NULL, NULL, CBM_OCCURRENCE_ELM_VALUE, false},
    [CBM_LANG_RESCRIPT] = {rescript_binding_nodes, NULL, CBM_OCCURRENCE_RESCRIPT_LET, false},
    [CBM_LANG_PURESCRIPT] = {purescript_binding_nodes, NULL, CBM_OCCURRENCE_PURESCRIPT_LHS, false},
    [CBM_LANG_NICKEL] = {nickel_binding_nodes, NULL, CBM_OCCURRENCE_NICKEL_LET, false},
    [CBM_LANG_JSONNET] = {jsonnet_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_HASKELL] = {haskell_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_ERLANG] = {NULL, NULL, CBM_OCCURRENCE_ERLANG_CLAUSE, false},
    [CBM_LANG_FSHARP] = {fsharp_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_NIX] = {NULL, NULL, CBM_OCCURRENCE_NIX_FUNCTION, false},
    [CBM_LANG_MATLAB] = {NULL, NULL, CBM_OCCURRENCE_MATLAB_ARGUMENTS, false},
    [CBM_LANG_LEAN] = {NULL, NULL, CBM_OCCURRENCE_LEAN_BINDER, false},
    [CBM_LANG_PASCAL] = {NULL, NULL, CBM_OCCURRENCE_PASCAL_PROC, false},
    [CBM_LANG_VERILOG] = {systemverilog_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_AWK] = {awk_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_CRYSTAL] = {crystal_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_TEAL] = {teal_binding_nodes, NULL, CBM_OCCURRENCE_TEAL_FUNCTION, false},
    [CBM_LANG_VHDL] = {NULL, NULL, CBM_OCCURRENCE_VHDL_INTERFACE, false},
    [CBM_LANG_SYSTEMVERILOG] = {systemverilog_binding_nodes, NULL, CBM_OCCURRENCE_STANDARD, false},
    [CBM_LANG_PINE] = {NULL, NULL, CBM_OCCURRENCE_PINE_FUNCTION, false},
    [CBM_LANG_PUPPET] = {NULL, NULL, CBM_OCCURRENCE_STANDARD, true},
    [CBM_LANG_LLVM_IR] = {llvm_binding_nodes, NULL, CBM_OCCURRENCE_LLVM_FUNCTION, false},
    [CBM_LANG_MESON] = {NULL, meson_write_nodes, CBM_OCCURRENCE_STANDARD, true},
    [CBM_LANG_GN] = {NULL, gn_write_nodes, CBM_OCCURRENCE_STANDARD, true},
    [CBM_LANG_LINKERSCRIPT] = {NULL, linkerscript_write_nodes, CBM_OCCURRENCE_STANDARD, true},
    [CBM_LANG_OBJECTSCRIPT_UDL] = {objectscript_binding_nodes, objectscript_write_nodes,
                                   CBM_OCCURRENCE_STANDARD, true},
    [CBM_LANG_OBJECTSCRIPT_ROUTINE] = {objectscript_binding_nodes, objectscript_write_nodes,
                                       CBM_OCCURRENCE_STANDARD, true},
};

static bool text_equals(CBMExtractCtx *ctx, TSNode node, const char *expected) {
    char *text = cbm_node_text(ctx->arena, node, ctx->source);
    return text && strcmp(text, expected) == 0;
}

static bool named_child_contains(TSNode parent, uint32_t index, TSNode node) {
    return ts_node_named_child_count(parent) > index &&
           node_contains(ts_node_named_child(parent, index), node);
}

static bool lisp_def_head(const char *text) {
    static const char *const heads[] = {"defn",
                                        "defn-",
                                        "def",
                                        "defmacro",
                                        "defmulti",
                                        "defmethod",
                                        "defprotocol",
                                        "defrecord",
                                        "deftype",
                                        "definterface",
                                        "defonce",
                                        "define",
                                        "define-syntax",
                                        "define-values",
                                        "define-struct",
                                        "define-record-type",
                                        "define/contract",
                                        "struct",
                                        NULL};
    return kind_in_exact_set(text, heads);
}

static bool is_lisp_def_binding(CBMExtractCtx *ctx, TSNode node) {
    for (TSNode form = ts_node_parent(node); !ts_node_is_null(form); form = ts_node_parent(form)) {
        const char *kind = ts_node_type(form);
        if ((strcmp(kind, "list") != 0 && strcmp(kind, "list_lit") != 0) ||
            ts_node_named_child_count(form) < 2) {
            continue;
        }
        TSNode head_node = ts_node_named_child(form, 0);
        char *head = cbm_node_text(ctx->arena, head_node, ctx->source);
        if (!lisp_def_head(head)) {
            continue;
        }
        if (node_contains(head_node, node) || named_child_contains(form, 1, node)) {
            return true;
        }
        if (ctx->language == CBM_LANG_CLOJURE && ts_node_named_child_count(form) > 2 &&
            named_child_contains(form, 2, node)) {
            return true;
        }
        return false;
    }
    return false;
}

static bool is_fennel_fn_binding(CBMExtractCtx *ctx, TSNode node) {
    for (TSNode form = ts_node_parent(node); !ts_node_is_null(form); form = ts_node_parent(form)) {
        if (strcmp(ts_node_type(form), "list") != 0 || ts_node_named_child_count(form) < 2 ||
            !text_equals(ctx, ts_node_named_child(form, 0), "fn")) {
            continue;
        }
        TSNode first = ts_node_named_child(form, 1);
        const char *first_kind = ts_node_type(first);
        bool anonymous = strcmp(first_kind, "sequence") == 0 || strcmp(first_kind, "table") == 0 ||
                         strcmp(first_kind, "vector") == 0;
        if (anonymous) {
            return named_child_contains(form, 0, node) || node_contains(first, node);
        }
        return named_child_contains(form, 0, node) || node_contains(first, node) ||
               (ts_node_named_child_count(form) > 2 && named_child_contains(form, 2, node));
    }
    return false;
}

static bool is_elixir_def_binding(CBMExtractCtx *ctx, TSNode node) {
    for (TSNode form = ts_node_parent(node); !ts_node_is_null(form); form = ts_node_parent(form)) {
        if (strcmp(ts_node_type(form), "call") != 0 || ts_node_named_child_count(form) < 2) {
            continue;
        }
        TSNode head = ts_node_named_child(form, 0);
        if (!text_equals(ctx, head, "def") && !text_equals(ctx, head, "defp") &&
            !text_equals(ctx, head, "defmacro")) {
            continue;
        }
        TSNode arguments = ts_node_child_by_field_name(form, TS_FIELD("arguments"));
        if (ts_node_is_null(arguments) || ts_node_named_child_count(arguments) == 0) {
            arguments = ts_node_named_child(form, 1);
        }
        TSNode signature = ts_node_named_child_count(arguments) > 0
                               ? ts_node_named_child(arguments, 0)
                               : arguments;
        return node_contains(head, node) || node_contains(signature, node);
    }
    return false;
}

static bool is_first_named_part_of(TSNode node, const char *container_kind) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), container_kind) == 0) {
            return named_child_contains(parent, 0, node);
        }
    }
    return false;
}

static bool is_wolfram_lhs(TSNode node) {
    static const char *const set_nodes[] = {
        "set",     "set_top",     "set_delayed",     "set_delayed_top",
        "tag_set", "tag_set_top", "tag_set_delayed", "tag_set_delayed_top",
        "up_set",  "up_set_top",  "up_set_delayed",  "up_set_delayed_top",
        NULL};
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (kind_in_exact_set(ts_node_type(parent), set_nodes)) {
            return named_child_contains(parent, 0, node);
        }
    }
    return false;
}

static bool any_field_contains_node(TSNode parent, const char *field, TSNode node);

static bool is_tlaplus_binding(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (strcmp(kind, "operator_definition") == 0) {
            /* `name:` is the callable declaration, while every repeated
             * `parameter:` field is a function-wide lexical binder. */
            return any_field_contains_node(parent, "parameter", node);
        }
        if (strcmp(kind, "function_definition") == 0) {
            /* `F[x \in S] == ...`: only quantifier_bound.intro binds. The set
             * expression S remains an ordinary identifier_ref usage. */
            for (TSNode bound = ts_node_parent(node);
                 !ts_node_is_null(bound) && !ts_node_eq(bound, parent);
                 bound = ts_node_parent(bound)) {
                if (strcmp(ts_node_type(bound), "quantifier_bound") == 0) {
                    return any_field_contains_node(bound, "intro", node);
                }
            }
            return false;
        }
        if (strcmp(kind, "bounded_quantification") == 0) {
            for (TSNode bound = ts_node_parent(node);
                 !ts_node_is_null(bound) && !ts_node_eq(bound, parent);
                 bound = ts_node_parent(bound)) {
                if (strcmp(ts_node_type(bound), "quantifier_bound") == 0) {
                    return any_field_contains_node(bound, "intro", node);
                }
            }
            return false;
        }
        if (strcmp(kind, "unbounded_quantification") == 0) {
            return any_field_contains_node(parent, "intro", node);
        }
    }
    return false;
}

static bool is_cobol_move_destination(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "move_statement") != 0) {
            continue;
        }
        if (field_contains_node(parent, "destination", node) ||
            field_contains_node(parent, "target", node)) {
            return true;
        }
        uint32_t count = ts_node_named_child_count(parent);
        return count > 1 && named_child_contains(parent, count - 1, node);
    }
    return false;
}

/* Some grammars expose declaration roles as repeated/inherited fields. The
 * public child-by-field helper returns only one match, so inspect every direct
 * child and preserve the exact field role while allowing nested wrappers. */
static bool any_field_contains_node(TSNode parent, const char *field, TSNode node) {
    uint32_t count = ts_node_child_count(parent);
    for (uint32_t i = 0; i < count; i++) {
        const char *child_field = ts_node_field_name_for_child(parent, i);
        if (child_field && strcmp(child_field, field) == 0 &&
            node_contains(ts_node_child(parent, i), node)) {
            return true;
        }
    }
    return false;
}

static bool is_perl_lexical_declaration_binding(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "variable_declaration") == 0) {
            /* Perl permits repeated `variables:` fields for `my ($a, $b)`;
             * inspect all direct field instances, not only the first. */
            return any_field_contains_node(parent, "variables", node);
        }
        if (strcmp(ts_node_type(parent), "assignment_expression") == 0) {
            return false;
        }
    }
    return false;
}

static bool is_cmake_function_parameter(TSNode node) {
    if (strcmp(ts_node_type(node), "unquoted_argument") != 0) {
        return false;
    }
    TSNode argument = ts_node_parent(node);
    TSNode arguments = ts_node_parent(argument);
    TSNode command = ts_node_parent(arguments);
    if (ts_node_is_null(argument) || strcmp(ts_node_type(argument), "argument") != 0 ||
        ts_node_is_null(arguments) || strcmp(ts_node_type(arguments), "argument_list") != 0 ||
        ts_node_is_null(command) ||
        (strcmp(ts_node_type(command), "function_command") != 0 &&
         strcmp(ts_node_type(command), "macro_command") != 0)) {
        return false;
    }
    uint32_t count = ts_node_named_child_count(arguments);
    for (uint32_t i = 1; i < count; i++) {
        if (node_contains(ts_node_named_child(arguments, i), node)) {
            return true;
        }
    }
    return false;
}

static bool fish_argument_marker(CBMExtractCtx *ctx, TSNode node) {
    char *text = cbm_node_text(ctx->arena, node, ctx->source);
    return text && (strcmp(text, "-a") == 0 || strcmp(text, "--argument") == 0 ||
                    strcmp(text, "--argument-names") == 0);
}

static bool is_fish_function_parameter(CBMExtractCtx *ctx, TSNode node, WalkState *state) {
    if (strcmp(ts_node_type(node), "word") != 0) {
        return false;
    }
    TSNode definition = ts_node_parent(node);
    const char *field =
        ts_node_is_null(definition) ? NULL : field_name_for_walk_node(state, definition, node);
    if (!field || strcmp(field, "option") != 0 ||
        strcmp(ts_node_type(definition), "function_definition") != 0) {
        return false;
    }
    for (TSNode previous = ts_node_prev_named_sibling(node); !ts_node_is_null(previous);
         previous = ts_node_prev_named_sibling(previous)) {
        char *text = cbm_node_text(ctx->arena, previous, ctx->source);
        if (!text || text[0] != '-') {
            continue;
        }
        return fish_argument_marker(ctx, previous);
    }
    return false;
}

static bool is_tcl_procedure_parameter(TSNode node) {
    TSNode argument = ts_node_parent(node);
    if (ts_node_is_null(argument) || strcmp(ts_node_type(argument), "argument") != 0 ||
        !any_field_contains_node(argument, "name", node)) {
        return false;
    }
    TSNode arguments = ts_node_parent(argument);
    TSNode procedure = ts_node_parent(arguments);
    return !ts_node_is_null(arguments) && strcmp(ts_node_type(arguments), "arguments") == 0 &&
           !ts_node_is_null(procedure) && strcmp(ts_node_type(procedure), "procedure") == 0 &&
           any_field_contains_node(procedure, "arguments", node);
}

static bool cfml_argument_tag_name_binding(CBMExtractCtx *ctx, TSNode node) {
    if (strcmp(ts_node_type(node), "attribute_value") != 0) {
        return false;
    }
    TSNode quoted = ts_node_parent(node);
    TSNode attribute = ts_node_parent(quoted);
    if (ts_node_is_null(attribute) || strcmp(ts_node_type(attribute), "cf_attribute") != 0) {
        return false;
    }
    TSNode attribute_name = cbm_find_child_by_kind(attribute, "cf_attribute_name");
    char *name_text = cbm_node_text(ctx->arena, attribute_name, ctx->source);
    if (!name_text || strcasecmp(name_text, "name") != 0) {
        return false;
    }
    TSNode tag = ts_node_parent(attribute);
    if (ts_node_is_null(tag) || strcmp(ts_node_type(tag), "cf_selfclose_tag") != 0) {
        return false;
    }
    char *tag_text = cbm_node_text(ctx->arena, tag, ctx->source);
    static const char argument_tag[] = "<cfargument";
    size_t prefix_length = sizeof(argument_tag) - 1U;
    if (!tag_text || strncasecmp(tag_text, argument_tag, prefix_length) != 0 ||
        (tag_text[prefix_length] != '\0' && !isspace((unsigned char)tag_text[prefix_length]) &&
         tag_text[prefix_length] != '>' && tag_text[prefix_length] != '/')) {
        return false;
    }
    for (TSNode parent = ts_node_parent(tag); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "cf_function_tag") == 0) {
            return true;
        }
    }
    return false;
}

static bool is_exact_language_binding(CBMExtractCtx *ctx, TSNode node, WalkState *state) {
    const char *kind = ts_node_type(node);
    switch (ctx->language) {
    case CBM_LANG_OCAML: {
        if (strcmp(kind, "value_pattern") != 0) {
            return false;
        }
        TSNode parameter = ts_node_parent(node);
        return !ts_node_is_null(parameter) && strcmp(ts_node_type(parameter), "parameter") == 0 &&
               field_contains_node(parameter, "pattern", node);
    }
    case CBM_LANG_SCSS: {
        if (strcmp(kind, "variable_name") != 0) {
            return false;
        }
        TSNode parameter = ts_node_parent(node);
        return !ts_node_is_null(parameter) && strcmp(ts_node_type(parameter), "parameter") == 0;
    }
    case CBM_LANG_FORM: {
        if (strcmp(kind, "parameter") != 0) {
            return false;
        }
        TSNode parameters = ts_node_parent(node);
        return !ts_node_is_null(parameters) &&
               strcmp(ts_node_type(parameters), "parameter_list") == 0;
    }
    case CBM_LANG_FUNC: {
        if (strcmp(kind, "parameter") != 0) {
            return false;
        }
        TSNode declaration = ts_node_parent(node);
        return !ts_node_is_null(declaration) &&
               strcmp(ts_node_type(declaration), "parameter_declaration") == 0 &&
               field_contains_node(declaration, "name", node);
    }
    case CBM_LANG_PERL:
        return is_perl_lexical_declaration_binding(node);
    case CBM_LANG_CMAKE:
        return is_cmake_function_parameter(node);
    case CBM_LANG_FISH:
        return is_fish_function_parameter(ctx, node, state);
    case CBM_LANG_TCL:
        return is_tcl_procedure_parameter(node);
    case CBM_LANG_CFML:
        return cfml_argument_tag_name_binding(ctx, node);
    default:
        return false;
    }
}

static bool ancestor_field_binds(TSNode node, const char *container_kind,
                                 const char *const *fields) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), container_kind) != 0) {
            continue;
        }
        for (const char *const *field = fields; *field; field++) {
            if (any_field_contains_node(parent, *field, node)) {
                return true;
            }
        }
        return false;
    }
    return false;
}

static bool is_erlang_clause_binding(TSNode node) {
    static const char *const fields[] = {"args", NULL};
    return ancestor_field_binds(node, "function_clause", fields);
}

static bool is_nix_function_binding(TSNode node) {
    /* tree-sitter-nix names a simple `x: body` binder `universal`; set
     * destructuring uses the distinct `formals` field. */
    static const char *const fields[] = {"universal", "formals", NULL};
    return ancestor_field_binds(node, "function_expression", fields);
}

static bool is_lean_binder_name(TSNode node) {
    static const char *const binder_kinds[] = {"explicit_binder", "implicit_binder",
                                               "instance_binder", NULL};
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (kind_in_exact_set(ts_node_type(parent), binder_kinds)) {
            return any_field_contains_node(parent, "name", node);
        }
    }
    return false;
}

static bool is_pascal_proc_binding(TSNode node) {
    static const char *const fields[] = {"args", NULL};
    return ancestor_field_binds(node, "declProc", fields) ||
           ancestor_field_binds(node, "defProc", fields);
}

static bool is_teal_function_binding(TSNode node) {
    TSNode arguments = {0};
    for (TSNode current = ts_node_parent(node); !ts_node_is_null(current);
         current = ts_node_parent(current)) {
        const char *kind = ts_node_type(current);
        if (ts_node_is_null(arguments) && strcmp(kind, "arguments") == 0) {
            arguments = current;
            continue;
        }
        if (strcmp(kind, "function_signature") == 0) {
            return !ts_node_is_null(arguments) &&
                   any_field_contains_node(current, "arguments", node);
        }
        if (strcmp(kind, "function_statement") == 0) {
            if (ts_node_is_null(arguments)) {
                return false;
            }
            TSNode signature = ts_node_child_by_field_name(current, TS_FIELD("signature"));
            return node_contains(signature, arguments);
        }
    }
    return false;
}

static bool is_commonlisp_defun_binding(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "defun_header") != 0) {
            continue;
        }
        return field_contains_node(parent, "function_name", node) ||
               field_contains_node(parent, "lambda_list", node);
    }
    return false;
}

static bool is_hcl_attribute_binding(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "attribute") == 0) {
            /* HCL's attribute production exposes no fields. The first named
             * child is its key; the expression is the second named child. */
            return named_child_contains(parent, 0, node);
        }
    }
    return false;
}

static bool is_matlab_argument_binding(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (strcmp(kind, "function_arguments") == 0 || strcmp(kind, "lambda_arguments") == 0) {
            return true;
        }
    }
    return false;
}

static bool is_vhdl_interface_binding(TSNode node) {
    static const char *const interface_kinds[] = {
        "interface_constant_declaration", "interface_signal_declaration",
        "interface_variable_declaration", "interface_declaration", NULL};
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (kind_in_exact_set(ts_node_type(parent), interface_kinds)) {
            /* FIELD_COUNT does not name this role; every interface production
             * starts with its identifier_list before mode/type/default. */
            return named_child_contains(parent, 0, node);
        }
    }
    return false;
}

static bool is_pine_function_binding(TSNode node) {
    static const char *const fields[] = {"argument", NULL};
    return ancestor_field_binds(node, "function_declaration_statement", fields);
}

static bool is_policy_binding(CBMExtractCtx *ctx, TSNode node,
                              const CBMOccurrenceSpec *occurrence) {
    switch (occurrence->policy) {
    case CBM_OCCURRENCE_LISP_DEF:
        return is_lisp_def_binding(ctx, node);
    case CBM_OCCURRENCE_COMMONLISP_DEFUN:
        return is_commonlisp_defun_binding(node);
    case CBM_OCCURRENCE_FENNEL_FN:
        return is_fennel_fn_binding(ctx, node);
    case CBM_OCCURRENCE_ELIXIR_DEF:
        return is_elixir_def_binding(ctx, node);
    case CBM_OCCURRENCE_JULIA_FUNCTION:
        return is_first_named_part_of(node, "function_definition");
    case CBM_OCCURRENCE_WOLFRAM_SET:
        return is_wolfram_lhs(node);
    case CBM_OCCURRENCE_TYPST_LET:
        return is_first_named_part_of(node, "let");
    case CBM_OCCURRENCE_AGDA_FUNCTION:
        for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
             parent = ts_node_parent(parent)) {
            if (strcmp(ts_node_type(parent), "lhs") == 0) {
                return true;
            }
        }
        return false;
    case CBM_OCCURRENCE_TLAPLUS_OPERATOR:
        return is_tlaplus_binding(node);
    case CBM_OCCURRENCE_COBOL_MOVE:
        return is_cobol_move_destination(node);
    case CBM_OCCURRENCE_HCL_ATTRIBUTE:
        return is_hcl_attribute_binding(node);
    case CBM_OCCURRENCE_ELM_VALUE:
        return is_first_named_part_of(node, "value_declaration");
    case CBM_OCCURRENCE_RESCRIPT_LET:
        return is_first_named_part_of(node, "let_binding");
    case CBM_OCCURRENCE_PURESCRIPT_LHS:
        return is_first_named_part_of(node, "function");
    case CBM_OCCURRENCE_NICKEL_LET:
        return is_first_named_part_of(node, "let_binding") ||
               is_first_named_part_of(node, "pattern_fun");
    case CBM_OCCURRENCE_ERLANG_CLAUSE:
        return is_erlang_clause_binding(node);
    case CBM_OCCURRENCE_NIX_FUNCTION:
        return is_nix_function_binding(node);
    case CBM_OCCURRENCE_MATLAB_ARGUMENTS:
        return is_matlab_argument_binding(node);
    case CBM_OCCURRENCE_LEAN_BINDER:
        return is_lean_binder_name(node);
    case CBM_OCCURRENCE_PASCAL_PROC:
        return is_pascal_proc_binding(node);
    case CBM_OCCURRENCE_TEAL_FUNCTION:
        return is_teal_function_binding(node);
    case CBM_OCCURRENCE_VHDL_INTERFACE:
        return is_vhdl_interface_binding(node);
    case CBM_OCCURRENCE_PINE_FUNCTION:
        return is_pine_function_binding(node);
    case CBM_OCCURRENCE_LLVM_FUNCTION:
        for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
             parent = ts_node_parent(parent)) {
            if (strcmp(ts_node_type(parent), "function_header") == 0) {
                return field_contains_node(parent, "arguments", node);
            }
        }
        return false;
    case CBM_OCCURRENCE_STANDARD:
    default:
        return false;
    }
}

static bool elixir_binary_operator_binds(CBMExtractCtx *ctx, TSNode node) {
    if (ctx->language != CBM_LANG_ELIXIR || strcmp(ts_node_type(node), "binary_operator") != 0) {
        return false;
    }
    TSNode operator_node = ts_node_child_by_field_name(node, TS_FIELD("operator"));
    return text_equals(ctx, operator_node, "=") || text_equals(ctx, operator_node, "<-") ||
           text_equals(ctx, operator_node, "->") || text_equals(ctx, operator_node, "\\\\");
}

static bool is_binding_occurrence(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                  WalkState *state) {
    const CBMOccurrenceSpec *occurrence = &occurrence_specs[ctx->language];
    if (is_exact_language_binding(ctx, node, state)) {
        return true;
    }
    if (is_policy_binding(ctx, node, occurrence)) {
        return true;
    }

    TSNode current = node;
    TSTreeCursor *cursor = reset_occurrence_cursor(state, node);
    while (!ts_node_is_null(current)) {
        TSNode parent;
        const char *field;
        if (!occurrence_parent(cursor, current, &parent, &field)) {
            break;
        }
        if (is_value_field(field)) {
            return false;
        }
        /* A declaration container binds its name/pattern, not the symbols used
         * by its type annotation.  Both TypeScript (`cfg: Config`) and Go
         * (`cfg Config`) expose that annotation through the exact `type` field;
         * stop before the whole-parameter binding rule can swallow `Config`.
         * The emitted occurrence remains an ordinary USAGE, never a callable
         * reference merely because the target happens to be a type. */
        if (field && strcmp(field, "type") == 0) {
            return false;
        }

        const char *kind = ts_node_type(parent);
        if (kind_in_exact_set(kind, common_whole_binding_nodes) ||
            kind_in_exact_set(kind, occurrence->whole_binding_nodes)) {
            return true;
        }

        bool variable_container =
            spec->variable_node_types && cbm_kind_in_set(parent, spec->variable_node_types);
        if (ctx->language == CBM_LANG_ELIXIR && strcmp(kind, "binary_operator") == 0) {
            variable_container = elixir_binary_operator_binds(ctx, parent);
        }
        bool declared_container =
            kind_in_exact_set(kind, field_binding_nodes) ||
            (spec->function_node_types && cbm_kind_in_set(parent, spec->function_node_types)) ||
            (spec->class_node_types && cbm_kind_in_set(parent, spec->class_node_types)) ||
            (spec->field_node_types && cbm_kind_in_set(parent, spec->field_node_types)) ||
            variable_container;
        if (declared_container) {
            for (const char *const *binding_field = binding_fields; *binding_field;
                 binding_field++) {
                if (field_contains_node(parent, *binding_field, node)) {
                    return true;
                }
            }
        }
        current = parent;
    }
    return false;
}

static bool assignment_reads_target(TSNode assignment) {
    static const char *const read_write_nodes[] = {"augmented_assignment",
                                                   "augmented_assignment_expression",
                                                   "compound_assignment_expr",
                                                   "compound_assignment_expression",
                                                   "operator_assignment",
                                                   "operator_assign",
                                                   "update_exp",
                                                   "postfix_unary_expression",
                                                   "prefix_unary_expression",
                                                   NULL};
    if (kind_in_exact_set(ts_node_type(assignment), read_write_nodes)) {
        return true;
    }
    static const char *const read_write_operators[] = {"+=",
                                                       "-=",
                                                       "*=",
                                                       "/=",
                                                       "%=",
                                                       "&=",
                                                       "|=",
                                                       "^=",
                                                       "<<=",
                                                       ">>=",
                                                       "?"
                                                       "?=",
                                                       "++",
                                                       "--",
                                                       NULL};
    uint32_t count = ts_node_child_count(assignment);
    for (uint32_t i = 0; i < count; i++) {
        if (kind_in_exact_set(ts_node_type(ts_node_child(assignment, i)), read_write_operators)) {
            return true;
        }
    }
    return false;
}

static bool is_write_occurrence(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                WalkState *state) {
    const CBMOccurrenceSpec *occurrence = &occurrence_specs[ctx->language];
    TSNode current = node;
    TSTreeCursor *cursor = reset_occurrence_cursor(state, node);
    while (!ts_node_is_null(current)) {
        TSNode parent;
        const char *field;
        if (!occurrence_parent(cursor, current, &parent, &field)) {
            break;
        }
        if (is_value_field(field)) {
            return false;
        }
        bool assignment =
            (spec->assignment_node_types && cbm_kind_in_set(parent, spec->assignment_node_types)) ||
            kind_in_exact_set(ts_node_type(parent), occurrence->write_nodes);
        if (ctx->language == CBM_LANG_ELIXIR &&
            strcmp(ts_node_type(parent), "binary_operator") == 0) {
            assignment = elixir_binary_operator_binds(ctx, parent);
        }
        if (assignment) {
            if (assignment_reads_target(parent)) {
                return false;
            }
            if (ctx->language == CBM_LANG_LINKERSCRIPT &&
                strcmp(ts_node_type(parent), "assignment") == 0) {
                /* This grammar's assignment production has no field map. Its
                 * first direct child is the lhs, even when that child is the
                 * anonymous location-counter token `.`. */
                return ts_node_child_count(parent) > 0 &&
                       node_contains(ts_node_child(parent, 0), node);
            }
            TSNode left = ts_node_child_by_field_name(parent, TS_FIELD("left"));
            if (node_contains(left, node) || field_contains_node(parent, "target", node) ||
                field_contains_node(parent, "destination", node)) {
                return true;
            }
            return occurrence->first_named_child_is_write && named_child_contains(parent, 0, node);
        }
        current = parent;
    }
    return false;
}

static bool is_argument_container_kind(const char *kind) {
    return kind && (strcmp(kind, "arguments") == 0 || strcmp(kind, "argument_list") == 0 ||
                    strcmp(kind, "value_arguments") == 0);
}

static bool is_labeled_argument_kind(const char *kind) {
    return kind && (strcmp(kind, "keyword_argument") == 0 || strcmp(kind, "named_argument") == 0 ||
                    strcmp(kind, "labeled_argument") == 0);
}

/* A named/labeled argument's key describes the callee parameter; it is not a
 * value read in the caller. Keep this separate from binding detection: the
 * label must neither emit USAGE nor create a caller-local lexical binding. */
static bool is_call_argument_label(TSNode node) {
    usage_slow_parent_fallback_test_note();
    for (TSNode current = node; !ts_node_is_null(current);) {
        TSNode parent = ts_node_parent(current);
        if (ts_node_is_null(parent)) {
            return false;
        }
        const char *parent_kind = ts_node_type(parent);
        if (is_labeled_argument_kind(parent_kind)) {
            return field_contains_node(parent, "name", node) ||
                   field_contains_node(parent, "label", node) ||
                   field_contains_node(parent, "key", node);
        }
        if (is_argument_container_kind(parent_kind)) {
            return false;
        }
        current = parent;
    }
    return false;
}

static bool is_call_argument_label_walk(TSNode node, WalkState *state) {
    TSTreeCursor *cursor = reset_occurrence_cursor(state, node);
    if (!cursor)
        return is_call_argument_label(node);
    TSNode current = node;
    while (!ts_node_is_null(current)) {
        TSNode parent;
        const char *field;
        if (!occurrence_parent(cursor, current, &parent, &field))
            return false;
        const char *parent_kind = ts_node_type(parent);
        if (is_labeled_argument_kind(parent_kind)) {
            return field && (strcmp(field, "name") == 0 || strcmp(field, "label") == 0 ||
                             strcmp(field, "key") == 0);
        }
        if (is_argument_container_kind(parent_kind))
            return false;
        current = parent;
    }
    return false;
}

static bool is_direct_argument_value(TSNode node) {
    usage_slow_parent_fallback_test_note();
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent)) {
        return false;
    }
    if (is_labeled_argument_kind(ts_node_type(parent))) {
        TSNode value = ts_node_child_by_field_name(parent, TS_FIELD("value"));
        return !ts_node_is_null(value) && ts_node_eq(value, node) &&
               is_direct_argument_value(parent);
    }
    TSNode direct_arguments = ts_node_child_by_field_name(parent, TS_FIELD("arguments"));
    if (!ts_node_is_null(direct_arguments) && ts_node_eq(direct_arguments, node)) {
        return true;
    }
    if (is_argument_container_kind(ts_node_type(parent))) {
        return true;
    }
    const char *parent_kind = ts_node_type(parent);
    if (strcmp(parent_kind, "list_expression") == 0) {
        TSNode call = ts_node_parent(parent);
        if (!ts_node_is_null(call)) {
            TSNode arguments = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
            return !ts_node_is_null(arguments) && ts_node_eq(arguments, parent);
        }
        return false;
    }
    if (strcmp(parent_kind, "argument") != 0 && strcmp(parent_kind, "value_argument") != 0) {
        return false;
    }
    TSNode grandparent = ts_node_parent(parent);
    return !ts_node_is_null(grandparent) && is_argument_container_kind(ts_node_type(grandparent));
}

/* Cursor-backed counterpart for the unified walker. `cursor` must currently
 * point at `node`; it is consumed while walking toward the argument owner. */
static bool is_direct_argument_value_cursor(TSNode node, TSTreeCursor *cursor) {
    TSNode current = node;
    while (!ts_node_is_null(current)) {
        TSNode parent;
        const char *field;
        if (!occurrence_parent(cursor, current, &parent, &field))
            return false;
        const char *parent_kind = ts_node_type(parent);
        if (is_labeled_argument_kind(parent_kind)) {
            if (!field || strcmp(field, "value") != 0)
                return false;
            current = parent;
            continue;
        }
        if (field && strcmp(field, "arguments") == 0)
            return true;
        if (is_argument_container_kind(parent_kind))
            return true;
        if (strcmp(parent_kind, "list_expression") == 0) {
            TSNode call;
            const char *list_field;
            return occurrence_parent(cursor, parent, &call, &list_field) && list_field &&
                   strcmp(list_field, "arguments") == 0;
        }
        if (strcmp(parent_kind, "argument") != 0 && strcmp(parent_kind, "value_argument") != 0) {
            return false;
        }
        TSNode grandparent;
        const char *argument_field;
        return occurrence_parent(cursor, parent, &grandparent, &argument_field) &&
               is_argument_container_kind(ts_node_type(grandparent));
    }
    return false;
}

static bool is_direct_argument_value_walk(TSNode node, WalkState *state) {
    TSTreeCursor *cursor = reset_occurrence_cursor(state, node);
    return cursor ? is_direct_argument_value_cursor(node, cursor) : is_direct_argument_value(node);
}

static TSNode python_direct_callable_attribute_site(TSNode node) {
    usage_slow_parent_fallback_test_note();
    if (ts_node_is_null(node) || strcmp(ts_node_type(node), "attribute") != 0) {
        return (TSNode){0};
    }
    TSNode site = node;
    TSNode parent = ts_node_parent(site);
    while (!ts_node_is_null(parent) &&
           strcmp(ts_node_type(parent), "parenthesized_expression") == 0) {
        TSNode inner = ts_node_child_by_field_name(parent, TS_FIELD("expression"));
        if (ts_node_is_null(inner) && ts_node_named_child_count(parent) == 1) {
            inner = ts_node_named_child(parent, 0);
        }
        if (ts_node_is_null(inner) || !ts_node_eq(inner, site)) {
            return (TSNode){0};
        }
        site = parent;
        parent = ts_node_parent(site);
    }
    return is_direct_argument_value(site) ? site : (TSNode){0};
}

static bool language_may_stamp_exact_callable_value_candidate(CBMLanguage language) {
    switch (language) {
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_GO:
    case CBM_LANG_PYTHON:
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_CUDA:
    case CBM_LANG_RUST:
    case CBM_LANG_CSHARP:
    case CBM_LANG_KOTLIN:
        return true;
    default:
        return false;
    }
}

/* Direct identifiers and direct, statically-named TS member values passed as
 * arguments are narrow syntactic candidates only. A language LSP must still
 * prove one target at this exact occurrence before the graph upgrades USAGE to
 * CALL_REFERENCE; unresolved, reassigned, and composite expressions stay USAGE. */
static TSNode csharp_callable_value_site(TSNode node) {
    usage_slow_parent_fallback_test_note();
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "identifier") != 0 && strcmp(kind, "simple_identifier") != 0) {
        return (TSNode){0};
    }

    TSNode site = node;
    TSNode parent = ts_node_parent(site);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "generic_name") == 0) {
        TSNode name = ts_node_child_by_field_name(parent, TS_FIELD("name"));
        if (ts_node_is_null(name) && ts_node_named_child_count(parent) > 0) {
            name = ts_node_named_child(parent, 0);
        }
        if (ts_node_is_null(name) || !ts_node_eq(name, node)) {
            return (TSNode){0};
        }
        site = parent;
        parent = ts_node_parent(site);
    }

    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "member_access_expression") == 0) {
        TSNode member = ts_node_child_by_field_name(parent, TS_FIELD("name"));
        if (ts_node_is_null(member) || !ts_node_eq(member, site)) {
            return (TSNode){0};
        }
        site = parent;
        parent = ts_node_parent(site);
    }

    while (!ts_node_is_null(parent) &&
           strcmp(ts_node_type(parent), "parenthesized_expression") == 0) {
        TSNode inner = ts_node_child_by_field_name(parent, TS_FIELD("expression"));
        if (ts_node_is_null(inner) && ts_node_named_child_count(parent) == 1) {
            inner = ts_node_named_child(parent, 0);
        }
        if (ts_node_is_null(inner) || !ts_node_eq(inner, site)) {
            return (TSNode){0};
        }
        site = parent;
        parent = ts_node_parent(site);
    }

    /* The wrapper chain proves admission, but the semantic row is keyed to the
     * terminal method-name identifier. Keep the raw carrier on that leaf. */
    return is_direct_argument_value(site) ? node : (TSNode){0};
}

static TSNode call_reference_candidate_site(CBMExtractCtx *ctx, TSNode node, const char *name,
                                            WalkState *state) {
    if (!ctx || !name || !language_may_stamp_exact_callable_value_candidate(ctx->language)) {
        return (TSNode){0};
    }
    const char *kind = ts_node_type(node);
    TSTreeCursor *cursor = reset_occurrence_cursor(state, node);
    TSNode parent = {0};
    const char *parent_field = NULL;
    if (!cursor) {
        usage_slow_parent_fallback_test_note();
        parent = ts_node_parent(node);
    } else {
        (void)occurrence_parent(cursor, node, &parent, &parent_field);
    }
    bool ts_family = ctx->language == CBM_LANG_JAVASCRIPT || ctx->language == CBM_LANG_TYPESCRIPT ||
                     ctx->language == CBM_LANG_TSX;
    if (ts_family && strcmp(kind, "property_identifier") == 0 && !ts_node_is_null(parent) &&
        strcmp(ts_node_type(parent), "member_expression") == 0) {
        TSNode property = ts_node_child_by_field_name(parent, TS_FIELD("property"));
        TSNode arguments = {0};
        const char *member_field = NULL;
        if (cursor) {
            (void)occurrence_parent(cursor, parent, &arguments, &member_field);
        } else {
            usage_slow_parent_fallback_test_note();
            arguments = ts_node_parent(parent);
        }
        return !ts_node_is_null(property) && ts_node_eq(property, node) &&
                       !ts_node_is_null(arguments) &&
                       strcmp(ts_node_type(arguments), "arguments") == 0
                   ? node
                   : (TSNode){0};
    }
    if (ctx->language == CBM_LANG_PYTHON && strcmp(kind, "identifier") == 0 &&
        !ts_node_is_null(parent) && strcmp(ts_node_type(parent), "attribute") == 0) {
        TSNode attribute = ts_node_child_by_field_name(parent, TS_FIELD("attribute"));
        return !ts_node_is_null(attribute) && ts_node_eq(attribute, node)
                   ? python_direct_callable_attribute_site(parent)
                   : (TSNode){0};
    }
    if (ctx->language == CBM_LANG_GO && strcmp(kind, "field_identifier") == 0 &&
        !ts_node_is_null(parent) && strcmp(ts_node_type(parent), "selector_expression") == 0) {
        TSNode field = ts_node_child_by_field_name(parent, TS_FIELD("field"));
        return !ts_node_is_null(field) && ts_node_eq(field, node) &&
                       (cursor ? is_direct_argument_value_cursor(parent, cursor)
                               : is_direct_argument_value(parent))
                   ? parent
                   : (TSNode){0};
    }
    if (ctx->language == CBM_LANG_RUST && strcmp(kind, "scoped_identifier") == 0) {
        return is_direct_argument_value_walk(node, state) ? node : (TSNode){0};
    }
    if (ctx->language == CBM_LANG_CSHARP) {
        return csharp_callable_value_site(node);
    }
    return (strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0) &&
                   is_direct_argument_value_walk(node, state)
               ? node
               : (TSNode){0};
}

static char *reference_name(CBMExtractCtx *ctx, TSNode node) {
    char *name = cbm_node_text(ctx->arena, node, ctx->source);
    if (!name || ctx->language != CBM_LANG_MAKEFILE ||
        strcmp(ts_node_type(node), "variable_reference") != 0) {
        return name;
    }

    /* Make exposes `$(watched)` as one named reference node.
     * Normalize only that grammar wrapper; the recorded site still spans the
     * exact source occurrence for occurrence-sensitive resolution. */
    size_t length = strlen(name);
    bool parenthesized = length > 3 && name[0] == '$' && name[1] == '(' && name[length - 1] == ')';
    return parenthesized ? cbm_arena_strndup(ctx->arena, name + 2, length - 3) : name;
}

static bool lexical_identifier_case_insensitive(CBMLanguage language) {
    switch (language) {
    case CBM_LANG_POWERSHELL:
    case CBM_LANG_FORTRAN:
    case CBM_LANG_ADA:
    case CBM_LANG_PASCAL:
    case CBM_LANG_COBOL:
    case CBM_LANG_VHDL:
        return true;
    default:
        return false;
    }
}

static const char *lexical_binding_key(CBMExtractCtx *ctx, WalkState *state, const char *name) {
    if (!ctx || !name) {
        return name;
    }
    /* Preserve language namespaces: Perl sigils and Vim prefixes distinguish
     * different variables. Tcl's grammar is asymmetric (`name` versus
     * `$name`), so unwrap only that language-defined reference sigil. */
    if (ctx->language == CBM_LANG_TCL && name[0] == '$') {
        name++;
    }
    if (!lexical_identifier_case_insensitive(ctx->language)) {
        return name;
    }
    size_t n = strlen(name);
    char *folded = (char *)cbm_arena_alloc(ctx->arena, n + 1U);
    if (!folded) {
        if (state) {
            state->lexical_binding_tracking_failed = true;
        }
        return NULL;
    }
    for (size_t i = 0; i < n; i++) {
        folded[i] = (char)tolower((unsigned char)name[i]);
    }
    folded[n] = '\0';
    return folded;
}

static void stamp_usage_site(CBMExtractCtx *ctx, CBMUsage *usage, TSNode node, const char *name,
                             WalkState *state) {
    TSNode candidate_site = call_reference_candidate_site(ctx, node, name, state);
    TSNode site = ts_node_is_null(candidate_site) ? node : candidate_site;
    usage->site_start_byte = ts_node_start_byte(site);
    usage->site_end_byte = ts_node_end_byte(site);
    usage->may_be_call_reference = !ts_node_is_null(candidate_site);
}

static const CBMLexicalScope *usage_lexical_scope(const WalkState *state, uint32_t id) {
    return state && id > 0 && id <= (uint32_t)state->lexical_scope_count
               ? &state->lexical_scopes[id - 1U]
               : NULL;
}

static uint32_t active_lexical_scope_id(const WalkState *state) {
    if (!state) {
        return 0;
    }
    for (int i = state->scope_top - 1; i >= 0; i--) {
        if (state->scopes[i].lexical_scope_id != 0) {
            return state->scopes[i].lexical_scope_id;
        }
    }
    return state->root_lexical_scope_id;
}

static uint32_t lexical_ancestor_of_kind(const WalkState *state, uint32_t start_id,
                                         bool want_function, bool want_block);

static bool python_default_value_reference(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        /* A nested executable scope inside the default owns its own lookups;
         * only a direct default expression is evaluated in the declaring
         * function's structural parent namespace. */
        if (strcmp(kind, "lambda") == 0 || strcmp(kind, "list_comprehension") == 0 ||
            strcmp(kind, "set_comprehension") == 0 ||
            strcmp(kind, "dictionary_comprehension") == 0 ||
            strcmp(kind, "generator_expression") == 0) {
            return false;
        }
        if ((strcmp(kind, "default_parameter") == 0 ||
             strcmp(kind, "typed_default_parameter") == 0) &&
            (field_contains_node(parent, "value", node) ||
             field_contains_node(parent, "default", node))) {
            return true;
        }
    }
    return false;
}

static uint32_t usage_lexical_scope_id_for_node(CBMExtractCtx *ctx, const WalkState *state,
                                                TSNode node) {
    uint32_t scope_id = active_lexical_scope_id(state);
    if (!ctx || ctx->language != CBM_LANG_PYTHON || !python_default_value_reference(node)) {
        return scope_id;
    }
    uint32_t function_id = lexical_ancestor_of_kind(state, scope_id, true, false);
    const CBMLexicalScope *function_scope = usage_lexical_scope(state, function_id);
    /* Method bodies intentionally skip their class lookup parent, but Python
     * evaluates the method's default expressions while the class namespace is
     * executing. Use structural ownership here, not body lookup ownership. */
    return function_scope ? function_scope->parent_id : scope_id;
}

static uint32_t lexical_ancestor_of_kind(const WalkState *state, uint32_t start_id,
                                         bool want_function, bool want_block) {
    uint32_t id = start_id;
    int remaining = state ? state->lexical_scope_count : 0;
    while (id != 0 && remaining-- > 0) {
        const CBMLexicalScope *scope = usage_lexical_scope(state, id);
        if (!scope) {
            return 0;
        }
        bool function_scope = scope->kind == CBM_LEXICAL_SCOPE_FUNCTION ||
                              scope->kind == CBM_LEXICAL_SCOPE_COMPREHENSION;
        bool block_scope = scope->kind == CBM_LEXICAL_SCOPE_BLOCK;
        if ((want_function && function_scope) || (want_block && block_scope)) {
            return id;
        }
        id = scope->parent_id;
    }
    return 0;
}

static uint32_t python_nearest_namespace(const WalkState *state, uint32_t start_id) {
    uint32_t id = start_id;
    int remaining = state ? state->lexical_scope_count : 0;
    while (id != 0 && remaining-- > 0) {
        const CBMLexicalScope *scope = usage_lexical_scope(state, id);
        if (!scope) {
            return 0;
        }
        if (scope->kind == CBM_LEXICAL_SCOPE_FUNCTION ||
            scope->kind == CBM_LEXICAL_SCOPE_COMPREHENSION ||
            scope->kind == CBM_LEXICAL_SCOPE_CLASS || scope->kind == CBM_LEXICAL_SCOPE_MODULE) {
            return id;
        }
        id = scope->parent_id;
    }
    return 0;
}

static bool ensure_lexical_binding_capacity(WalkState *state) {
    if (!state->lexical_bindings) {
        state->lexical_bindings = state->inline_lexical_bindings;
        state->lexical_binding_capacity = INLINE_LEXICAL_BINDINGS;
        memset(state->inline_lexical_bindings, 0, sizeof(state->inline_lexical_bindings));
    }
    if (state->lexical_binding_count < state->lexical_binding_capacity) {
        return true;
    }
    if (state->lexical_binding_capacity > INT32_MAX / PAIR_LEN) {
        state->lexical_binding_tracking_failed = true;
        return false;
    }
    int new_capacity = state->lexical_binding_capacity * PAIR_LEN;
    CBMLexicalBinding *grown =
        (CBMLexicalBinding *)cbm_arena_alloc(state->arena, (size_t)new_capacity * sizeof(*grown));
    if (!grown) {
        state->lexical_binding_tracking_failed = true;
        return false;
    }
    memcpy(grown, state->lexical_bindings, (size_t)state->lexical_binding_count * sizeof(*grown));
    state->lexical_bindings = grown;
    state->lexical_binding_capacity = new_capacity;
    return true;
}

static bool lexical_ancestor_kind(TSNode node, const char *kind) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), kind) == 0) {
            return true;
        }
    }
    return false;
}

static TSNode declared_function_name_owner(TSNode node, const CBMLangSpec *spec) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (spec->function_node_types && cbm_kind_in_set(parent, spec->function_node_types)) {
            return field_contains_node(parent, "name", node) ? parent : (TSNode){0};
        }
    }
    return (TSNode){0};
}

static TSNode declared_class_name_owner(TSNode node, const CBMLangSpec *spec) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (spec->class_node_types && cbm_kind_in_set(parent, spec->class_node_types)) {
            return field_contains_node(parent, "name", node) ? parent : (TSNode){0};
        }
    }
    return (TSNode){0};
}

static bool ensure_python_directive_capacity(WalkState *state) {
    if (!state->python_directives) {
        state->python_directives = state->inline_python_directives;
        state->python_directive_capacity = INLINE_PYTHON_DIRECTIVES;
        memset(state->inline_python_directives, 0, sizeof(state->inline_python_directives));
    }
    if (state->python_directive_count < state->python_directive_capacity) {
        return true;
    }
    if (state->python_directive_capacity > INT32_MAX / PAIR_LEN) {
        state->lexical_binding_tracking_failed = true;
        return false;
    }
    int new_capacity = state->python_directive_capacity * PAIR_LEN;
    CBMPythonDirective *grown =
        (CBMPythonDirective *)cbm_arena_alloc(state->arena, (size_t)new_capacity * sizeof(*grown));
    if (!grown) {
        state->lexical_binding_tracking_failed = true;
        return false;
    }
    memcpy(grown, state->python_directives, (size_t)state->python_directive_count * sizeof(*grown));
    state->python_directives = grown;
    state->python_directive_capacity = new_capacity;
    return true;
}

static void record_python_directive(WalkState *state, uint32_t function_scope_id, const char *name,
                                    CBMPythonDirectiveKind kind) {
    if (!function_scope_id || !name || !ensure_python_directive_capacity(state)) {
        return;
    }
    for (int i = 0; i < state->python_directive_count; i++) {
        CBMPythonDirective *directive = &state->python_directives[i];
        if (directive->function_scope_id == function_scope_id &&
            strcmp(directive->name, name) == 0) {
            directive->kind = (uint8_t)kind;
            return;
        }
    }
    CBMPythonDirective *directive = &state->python_directives[state->python_directive_count++];
    directive->function_scope_id = function_scope_id;
    directive->name = name;
    directive->kind = (uint8_t)kind;
}

static CBMPythonDirectiveKind python_directive_for(const WalkState *state,
                                                   uint32_t function_scope_id, const char *name) {
    for (int i = state ? state->python_directive_count - 1 : -1; i >= 0; i--) {
        const CBMPythonDirective *directive = &state->python_directives[i];
        if (directive->function_scope_id == function_scope_id && directive->name &&
            strcmp(directive->name, name) == 0) {
            return (CBMPythonDirectiveKind)directive->kind;
        }
    }
    return 0;
}

static uint32_t python_enclosing_function_namespace(const WalkState *state,
                                                    uint32_t inner_function_id) {
    const CBMLexicalScope *inner = usage_lexical_scope(state, inner_function_id);
    uint32_t id = inner ? inner->parent_id : 0;
    int remaining = state ? state->lexical_scope_count : 0;
    while (id != 0 && remaining-- > 0) {
        const CBMLexicalScope *scope = usage_lexical_scope(state, id);
        if (!scope) {
            return 0;
        }
        if (scope->kind == CBM_LEXICAL_SCOPE_FUNCTION ||
            scope->kind == CBM_LEXICAL_SCOPE_COMPREHENSION) {
            return id;
        }
        id = scope->parent_id;
    }
    return 0;
}

static bool binding_is_parameter(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    const CBMOccurrenceSpec *occurrence = &occurrence_specs[ctx->language];
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (kind_in_exact_set(kind, common_whole_binding_nodes) ||
            kind_in_exact_set(kind, occurrence->whole_binding_nodes)) {
            return true;
        }
        if (spec->function_node_types && cbm_kind_in_set(parent, spec->function_node_types)) {
            return field_contains_node(parent, "parameter", node) ||
                   field_contains_node(parent, "parameters", node);
        }
    }
    return false;
}

static bool js_var_binding(TSNode node) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        const char *kind = ts_node_type(parent);
        if (strcmp(kind, "variable_declaration") == 0) {
            return true;
        }
        if (strcmp(kind, "lexical_declaration") == 0) {
            return false;
        }
    }
    return false;
}

static bool import_kind_matches(TSNode node, const CBMLangSpec *spec) {
    return (spec->import_node_types && cbm_kind_in_set(node, spec->import_node_types)) ||
           (spec->import_from_types && cbm_kind_in_set(node, spec->import_from_types));
}

static TSNode nearest_import_ancestor(TSNode node, const CBMLangSpec *spec) {
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (import_kind_matches(parent, spec)) {
            return parent;
        }
    }
    return (TSNode){0};
}

static TSNode first_named_leaf(TSNode node) {
    while (!ts_node_is_null(node) && ts_node_named_child_count(node) > 0) {
        node = ts_node_named_child(node, 0);
    }
    return node;
}

static bool import_alias_contains(TSNode node, TSNode boundary) {
    for (TSNode parent = ts_node_parent(node);
         !ts_node_is_null(parent) && !ts_node_eq(parent, boundary);
         parent = ts_node_parent(parent)) {
        if (field_contains_node(parent, "alias", node)) {
            return true;
        }
    }
    return false;
}

static bool rust_use_list_has_direct_self(TSNode list) {
    uint32_t count = ts_node_named_child_count(list);
    for (uint32_t i = 0; i < count; i++) {
        if (strcmp(ts_node_type(ts_node_named_child(list, i)), "self") == 0) {
            return true;
        }
    }
    return false;
}

/* Import subtrees are excluded from ordinary usage emission, but their local
 * names still participate in lexical lookup. Keep this classifier narrow: it
 * identifies only the binding side, never a module path or imported source
 * name. */
static bool is_import_binding_occurrence(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    TSNode boundary = nearest_import_ancestor(node, spec);
    if (ts_node_is_null(boundary)) {
        return false;
    }
    /* Rust's extern-crate alias is a direct field of the import boundary,
     * unlike the nested alias containers used by Python and ES imports. */
    if (field_contains_node(boundary, "alias", node)) {
        return true;
    }
    if (import_alias_contains(node, boundary)) {
        return true;
    }

    switch (ctx->language) {
    case CBM_LANG_PYTHON: {
        const char *boundary_kind = ts_node_type(boundary);
        if (strcmp(boundary_kind, "future_import_statement") == 0) {
            return false;
        }
        for (TSNode parent = ts_node_parent(node);
             !ts_node_is_null(parent) && !ts_node_eq(parent, boundary);
             parent = ts_node_parent(parent)) {
            /* In an aliased import, only the alias binds. */
            if (strcmp(ts_node_type(parent), "aliased_import") == 0) {
                return false;
            }
        }
        bool from_import = strcmp(boundary_kind, "import_from_statement") == 0;
        if (from_import) {
            if (field_contains_node(boundary, "module_name", node) ||
                field_contains_node(boundary, "module", node)) {
                return false;
            }
            /* Older Python grammar variants omit the module field. Their first
             * named child is still the source module, never a local binding. */
            if (ts_node_named_child_count(boundary) > 1 &&
                node_contains(ts_node_named_child(boundary, 0), node)) {
                return false;
            }
            return strcmp(ts_node_type(node), "identifier") == 0;
        }

        TSNode entry = node;
        TSNode parent = ts_node_parent(entry);
        while (!ts_node_is_null(parent) && !ts_node_eq(parent, boundary)) {
            entry = parent;
            parent = ts_node_parent(parent);
        }
        TSNode first = first_named_leaf(entry);
        return !ts_node_is_null(first) && ts_node_eq(first, node);
    }
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX: {
        if (strcmp(ts_node_type(boundary), "import_statement") != 0) {
            return false;
        }
        for (TSNode parent = ts_node_parent(node);
             !ts_node_is_null(parent) && !ts_node_eq(parent, boundary);
             parent = ts_node_parent(parent)) {
            const char *kind = ts_node_type(parent);
            if (strcmp(kind, "import_specifier") == 0) {
                TSNode alias = ts_node_child_by_field_name(parent, TS_FIELD("alias"));
                if (!ts_node_is_null(alias)) {
                    return false;
                }
                return field_contains_node(parent, "name", node) ||
                       ts_node_eq(first_named_leaf(parent), node);
            }
            if (strcmp(kind, "namespace_import") == 0) {
                return field_contains_node(parent, "name", node) ||
                       ts_node_eq(first_named_leaf(parent), node);
            }
            if (strcmp(kind, "import_clause") == 0) {
                /* A direct identifier is the default import. Named and
                 * namespace imports were handled by their inner containers. */
                return ts_node_eq(first_named_leaf(parent), node);
            }
            if (strcmp(kind, "import_require_clause") == 0) {
                /* TypeScript `import local = require("module")` and
                 * `import local = Namespace.member` put the binding first.
                 * Current grammar variants expose only the source field, so
                 * retain the first-leaf fallback as the exact role check. */
                return field_contains_node(parent, "name", node) ||
                       ts_node_eq(first_named_leaf(parent), node);
            }
        }
        return false;
    }
    case CBM_LANG_RUST: {
        TSNode boundary_alias = ts_node_child_by_field_name(boundary, TS_FIELD("alias"));
        if (!ts_node_is_null(boundary_alias) && field_contains_node(boundary, "name", node)) {
            /* `extern crate source as local` binds only local. */
            return false;
        }
        bool terminal = false;
        for (TSNode parent = ts_node_parent(node);
             !ts_node_is_null(parent) && !ts_node_eq(parent, boundary);
             parent = ts_node_parent(parent)) {
            if (field_contains_node(parent, "path", node)) {
                /* `use foo::{self}` imports the module itself under the final
                 * segment of the prefix (`foo`). An aliased `self as x` is a
                 * use_as_clause child rather than a direct self entry and is
                 * already handled by the alias rule above. */
                if (strcmp(ts_node_type(parent), "scoped_use_list") == 0 &&
                    strcmp(ts_node_type(node), "identifier") == 0) {
                    TSNode path = ts_node_child_by_field_name(parent, TS_FIELD("path"));
                    TSNode list = ts_node_child_by_field_name(parent, TS_FIELD("list"));
                    bool terminal_path = terminal || ts_node_eq(path, node);
                    if (terminal_path && !ts_node_is_null(list) &&
                        rust_use_list_has_direct_self(list)) {
                        return true;
                    }
                }
                return false;
            }
            if (field_contains_node(parent, "name", node)) {
                terminal = true;
            }
        }
        return terminal || strcmp(ts_node_type(node), "identifier") == 0;
    }
    default:
        return false;
    }
}

static uint32_t lexical_import_scope(const WalkState *state, uint32_t start_id) {
    uint32_t id = start_id;
    int remaining = state ? state->lexical_scope_count : 0;
    while (id != 0 && remaining-- > 0) {
        const CBMLexicalScope *scope = usage_lexical_scope(state, id);
        if (!scope) {
            return 0;
        }
        if (scope->kind == CBM_LEXICAL_SCOPE_MODULE || scope->kind == CBM_LEXICAL_SCOPE_FUNCTION ||
            scope->kind == CBM_LEXICAL_SCOPE_COMPREHENSION ||
            scope->kind == CBM_LEXICAL_SCOPE_BLOCK) {
            return id;
        }
        id = scope->parent_id;
    }
    return 0;
}

static void record_lexical_binding(CBMExtractCtx *ctx, WalkState *state, TSNode node,
                                   const CBMLangSpec *spec, const char *raw_name,
                                   bool import_binding) {
    if (!ctx || !state || !raw_name || !raw_name[0] || state->lexical_binding_tracking_failed) {
        return;
    }
    bool parameter = binding_is_parameter(ctx, node, spec);
    if (ctx->language == CBM_LANG_VIMSCRIPT && parameter && !strchr(raw_name, ':')) {
        raw_name = cbm_arena_sprintf(ctx->arena, "a:%s", raw_name);
        if (!raw_name) {
            state->lexical_binding_tracking_failed = true;
            return;
        }
    }
    const char *name = lexical_binding_key(ctx, state, raw_name);
    if (!name || !name[0]) {
        return;
    }

    uint32_t current_id = active_lexical_scope_id(state);
    uint32_t scope_id = 0;
    bool whole_scope = false;
    TSNode function_declaration = declared_function_name_owner(node, spec);
    TSNode class_declaration = declared_class_name_owner(node, spec);

    if (ctx->language == CBM_LANG_PYTHON && (lexical_ancestor_kind(node, "global_statement") ||
                                             lexical_ancestor_kind(node, "nonlocal_statement"))) {
        uint32_t function_id = lexical_ancestor_of_kind(state, current_id, true, false);
        CBMPythonDirectiveKind directive = lexical_ancestor_kind(node, "global_statement")
                                               ? CBM_PYTHON_DIRECTIVE_GLOBAL
                                               : CBM_PYTHON_DIRECTIVE_NONLOCAL;
        record_python_directive(state, function_id, name, directive);
        return;
    }

    if (!ts_node_is_null(function_declaration)) {
        uint32_t function_id = lexical_ancestor_of_kind(state, current_id, true, false);
        const CBMLexicalScope *function_scope = usage_lexical_scope(state, function_id);
        uint32_t parent_id = function_scope ? function_scope->parent_id : 0;
        if (ctx->language == CBM_LANG_PYTHON) {
            scope_id = python_nearest_namespace(state, parent_id);
            const CBMLexicalScope *owner = usage_lexical_scope(state, scope_id);
            if (!owner || (owner->kind != CBM_LEXICAL_SCOPE_FUNCTION &&
                           owner->kind != CBM_LEXICAL_SCOPE_COMPREHENSION)) {
                return;
            }
        } else {
            scope_id = lexical_ancestor_of_kind(state, parent_id, true, true);
        }
        /* Top-level/class callable declarations are themselves semantic
         * targets, not local-value blockers. Nested declarations still bind
         * their enclosing executable scope and must block raw-name fallback. */
        if (scope_id == 0) {
            return;
        }
        whole_scope = true;
    } else if (ctx->language == CBM_LANG_PYTHON && !ts_node_is_null(class_declaration)) {
        uint32_t class_id = python_nearest_namespace(state, current_id);
        const CBMLexicalScope *class_scope = usage_lexical_scope(state, class_id);
        scope_id = python_nearest_namespace(state, class_scope ? class_scope->parent_id : 0);
        const CBMLexicalScope *owner = usage_lexical_scope(state, scope_id);
        if (!owner || (owner->kind != CBM_LEXICAL_SCOPE_FUNCTION &&
                       owner->kind != CBM_LEXICAL_SCOPE_COMPREHENSION)) {
            return;
        }
        whole_scope = true;
    } else if (parameter) {
        scope_id = lexical_ancestor_of_kind(state, current_id, true, false);
        whole_scope = true;
    } else if (ctx->language == CBM_LANG_PYTHON) {
        uint32_t namespace_id = python_nearest_namespace(state, current_id);
        const CBMLexicalScope *namespace_scope = usage_lexical_scope(state, namespace_id);
        uint32_t function_id =
            namespace_scope && (namespace_scope->kind == CBM_LEXICAL_SCOPE_FUNCTION ||
                                namespace_scope->kind == CBM_LEXICAL_SCOPE_COMPREHENSION)
                ? namespace_id
                : lexical_ancestor_of_kind(state, current_id, true, false);
        CBMPythonDirectiveKind directive = python_directive_for(state, function_id, name);
        if (directive == CBM_PYTHON_DIRECTIVE_GLOBAL) {
            scope_id = state->root_lexical_scope_id;
        } else if (directive == CBM_PYTHON_DIRECTIVE_NONLOCAL) {
            scope_id = python_enclosing_function_namespace(state, function_id);
        } else {
            scope_id = namespace_id;
        }
        const CBMLexicalScope *owner = usage_lexical_scope(state, scope_id);
        if (!owner) {
            return;
        }
        whole_scope = directive == 0 && (owner->kind == CBM_LEXICAL_SCOPE_FUNCTION ||
                                         owner->kind == CBM_LEXICAL_SCOPE_COMPREHENSION);
    } else if (ctx->language == CBM_LANG_POWERSHELL) {
        scope_id = lexical_ancestor_of_kind(state, current_id, true, false);
        whole_scope = true;
    } else if (ctx->language == CBM_LANG_JAVASCRIPT || ctx->language == CBM_LANG_TYPESCRIPT ||
               ctx->language == CBM_LANG_TSX) {
        bool is_var = js_var_binding(node);
        scope_id = lexical_ancestor_of_kind(state, current_id, is_var, !is_var);
        if (scope_id == 0) {
            scope_id = lexical_ancestor_of_kind(state, current_id, true, false);
        }
        if (scope_id == 0) {
            scope_id = state->root_lexical_scope_id;
        }
        whole_scope = true; /* var hoisting and let/const TDZ */
    } else if (ctx->language == CBM_LANG_RUST && import_binding) {
        scope_id = lexical_import_scope(state, current_id);
        if (scope_id == 0) {
            scope_id = state->root_lexical_scope_id;
        }
        /* A Rust use/extern-crate declaration is an item whose name is in
         * scope for the complete containing block or module, independent of
         * textual declaration order. */
        whole_scope = true;
    } else {
        scope_id = lexical_ancestor_of_kind(state, current_id, true, true);
        if (scope_id == 0) {
            scope_id = state->root_lexical_scope_id;
        }
    }
    const CBMLexicalScope *scope = usage_lexical_scope(state, scope_id);
    if (!scope || !ensure_lexical_binding_capacity(state)) {
        return;
    }
    CBMLexicalBinding *binding = &state->lexical_bindings[state->lexical_binding_count++];
    binding->scope_id = scope_id;
    binding->active_start = whole_scope ? scope->start_byte : ts_node_end_byte(node);
    /* Zero means "the finalized end of this concrete scope". Split
     * signature/body languages extend the scope after parameters are seen. */
    binding->active_end = 0;
    binding->name = name;
}

static int compare_lexical_bindings(const void *left_ptr, const void *right_ptr) {
    const CBMLexicalBinding *left = (const CBMLexicalBinding *)left_ptr;
    const CBMLexicalBinding *right = (const CBMLexicalBinding *)right_ptr;
    if (left->scope_id != right->scope_id) {
        return left->scope_id < right->scope_id ? -1 : 1;
    }
    int name_order = strcmp(left->name, right->name);
    if (name_order != 0) {
        return name_order;
    }
    if (left->active_start != right->active_start) {
        return left->active_start < right->active_start ? -1 : 1;
    }
    if (left->active_end != right->active_end) {
        return left->active_end < right->active_end ? -1 : 1;
    }
    return 0;
}

static int compare_binding_key(const CBMLexicalBinding *binding, uint32_t scope_id,
                               const char *name) {
    if (binding->scope_id != scope_id) {
        return binding->scope_id < scope_id ? -1 : 1;
    }
    return strcmp(binding->name, name);
}

static int lexical_binding_lower_bound(const WalkState *state, uint32_t scope_id,
                                       const char *name) {
    int low = 0;
    int high = state->lexical_binding_count;
    while (low < high) {
        int middle = low + (high - low) / PAIR_LEN;
        if (compare_binding_key(&state->lexical_bindings[middle], scope_id, name) < 0) {
            low = middle + 1;
        } else {
            high = middle;
        }
    }
    return low;
}

static bool lexical_binding_active_at(const WalkState *state, uint32_t scope_id, const char *name,
                                      uint32_t position) {
    const CBMLexicalScope *scope = usage_lexical_scope(state, scope_id);
    if (!scope) {
        return false;
    }
    int index = lexical_binding_lower_bound(state, scope_id, name);
    while (index < state->lexical_binding_count) {
        const CBMLexicalBinding *binding = &state->lexical_bindings[index++];
        if (binding->scope_id != scope_id || strcmp(binding->name, name) != 0) {
            break;
        }
        uint32_t active_end = binding->active_end ? binding->active_end : scope->end_byte;
        if (binding->active_start <= position && position < active_end) {
            return true;
        }
    }
    return false;
}

static const CBMLexicalScope *lexical_binding_scope_for_usage(const WalkState *state,
                                                              const CBMUsage *usage,
                                                              const char *name) {
    uint32_t scope_id = usage->lexical_scope_id;
    int remaining = state ? state->lexical_scope_count : 0;
    while (scope_id != 0 && remaining-- > 0) {
        const CBMLexicalScope *scope = usage_lexical_scope(state, scope_id);
        if (!scope) {
            return NULL;
        }
        if (lexical_binding_active_at(state, scope_id, name, usage->site_start_byte)) {
            return scope;
        }
        scope_id = scope->lookup_parent_id;
    }
    return NULL;
}

void cbm_finalize_lexical_usages(CBMExtractCtx *ctx, WalkState *state) {
    if (!ctx || !state) {
        return;
    }
    if (state->lexical_binding_count > 1) {
        qsort(state->lexical_bindings, (size_t)state->lexical_binding_count,
              sizeof(*state->lexical_bindings), compare_lexical_bindings);
    }
    for (int i = state->usage_start_index; i < ctx->result->usages.count; i++) {
        CBMUsage *usage = &ctx->result->usages.items[i];
        if (!usage->ref_name || !usage->ref_name[0]) {
            continue;
        }
        const char *name = lexical_binding_key(ctx, state, usage->ref_name);
        if (!name) {
            continue;
        }
        const CBMLexicalScope *binding_scope = lexical_binding_scope_for_usage(state, usage, name);
        if (!binding_scope) {
            continue;
        }
        usage->semantic_reference_blocked = true;
        usage->semantic_reference_local_shadow = binding_scope->kind != CBM_LEXICAL_SCOPE_MODULE;
    }

    /* Allocation failure must never widen textual fallback. Exact semantic
     * occurrence proof still runs first, but every unproven value fails closed
     * until the next clean extraction. */
    if (state->lexical_binding_tracking_failed) {
        for (int i = state->usage_start_index; i < ctx->result->usages.count; i++) {
            CBMUsage *usage = &ctx->result->usages.items[i];
            usage->semantic_reference_blocked = true;
            const CBMLexicalScope *scope = usage_lexical_scope(state, usage->lexical_scope_id);
            usage->semantic_reference_local_shadow =
                scope && scope->kind != CBM_LEXICAL_SCOPE_MODULE;
        }
    }
}

static char *perl_direct_coderef_name(CBMExtractCtx *ctx, TSNode node) {
    if (!ctx || ctx->language != CBM_LANG_PERL ||
        strcmp(ts_node_type(node), "refgen_expression") != 0 || !is_direct_argument_value(node)) {
        return NULL;
    }
    char *text = cbm_node_text(ctx->arena, node, ctx->source);
    const char *amp = text ? strchr(text, '&') : NULL;
    if (!amp) {
        return NULL;
    }
    const char *start = amp + 1;
    while (*start && isspace((unsigned char)*start)) {
        start++;
    }
    const char *end = start;
    while (*end && (isalnum((unsigned char)*end) || *end == '_' || *end == ':')) {
        end++;
    }
    return end > start ? cbm_arena_strndup(ctx->arena, start, (size_t)(end - start)) : NULL;
}

static bool inside_direct_perl_coderef(CBMExtractCtx *ctx, TSNode node) {
    if (!ctx || ctx->language != CBM_LANG_PERL) {
        return false;
    }
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "refgen_expression") == 0) {
            return is_direct_argument_value(parent);
        }
        if (is_argument_container_kind(ts_node_type(parent))) {
            return false;
        }
    }
    return false;
}

static bool emit_direct_perl_coderef_usage(CBMExtractCtx *ctx, TSNode node,
                                           const char *enclosing_func_qn,
                                           uint32_t lexical_scope_id) {
    char *name = perl_direct_coderef_name(ctx, node);
    if (!name || !name[0]) {
        return false;
    }
    CBMUsage usage = {0};
    usage.ref_name = name;
    usage.enclosing_func_qn = enclosing_func_qn;
    usage.kind = CBM_USAGE_CALL_REFERENCE;
    usage.may_be_call_reference = true;
    usage.lexical_scope_id = lexical_scope_id;
    usage.site_start_byte = ts_node_start_byte(node);
    usage.site_end_byte = ts_node_end_byte(node);
    cbm_usages_push(&ctx->result->usages, ctx->arena, usage);
    return true;
}

// Try to emit a usage for a reference node. Returns early if the node should be skipped.
static void try_emit_usage(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    if (emit_direct_perl_coderef_usage(ctx, node, cbm_enclosing_func_qn_cached(ctx, node), 0)) {
        return;
    }
    if (inside_direct_perl_coderef(ctx, node)) {
        return;
    }
    if (!is_reference_node(node, ctx->language)) {
        return;
    }
    if (is_call_argument_label(node)) {
        return;
    }
    if (is_inside_call(node, spec) || is_inside_import(node, spec)) {
        return;
    }
    if (is_binding_occurrence(ctx, node, spec, NULL) ||
        is_write_occurrence(ctx, node, spec, NULL)) {
        return;
    }
    char *name = reference_name(ctx, node);
    if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
        CBMUsage usage = {0};
        usage.ref_name = name;
        usage.enclosing_func_qn = cbm_enclosing_func_qn_cached(ctx, node);
        stamp_usage_site(ctx, &usage, node, name, NULL);
        cbm_usages_push(&ctx->result->usages, ctx->arena, usage);
    }
}

// Iterative usage walker — explicit stack
static void walk_usages(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, 4096);
    ts_nstack_push(&stack, ctx->arena, root);

    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        try_emit_usage(ctx, node, spec);
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - LAST_IDX; i >= 0; i--) {
            ts_nstack_push(&stack, ctx->arena, ts_node_child(node, (uint32_t)i));
        }
    }
}

void cbm_extract_usages(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    walk_usages(ctx, ctx->root, spec);
}

// --- Unified handler: called once per node by the cursor walk ---
// Uses WalkState flags instead of parent-chain walks for O(1) context checks.

void handle_usages(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state) {
    if (emit_direct_perl_coderef_usage(ctx, node, state->enclosing_func_qn,
                                       active_lexical_scope_id(state))) {
        return;
    }
    if (inside_direct_perl_coderef(ctx, node)) {
        return;
    }
    bool reference_node = is_reference_node(node, ctx->language);
    const CBMOccurrenceSpec *occurrence = &occurrence_specs[ctx->language];
    bool possible_binding_leaf =
        ts_node_is_named(node) && ts_node_named_child_count(node) == 0 &&
        (state->inside_import || is_exact_language_binding(ctx, node, state) ||
         is_policy_binding(ctx, node, occurrence));
    if (!reference_node && !possible_binding_leaf) {
        return;
    }

    // An invocation relationship replaces only the exact AST occurrence it
    // consumes. Receivers, qualifiers, computed keys, arguments, and nested
    // bodies remain ordinary value references. Explicit callable syntax keeps
    // that exact occurrence as a typed reference instead of erasing it.
    bool exact_callee =
        (!ts_node_is_null(state->callee_expr) && ts_node_eq(node, state->callee_expr)) ||
        (!ts_node_is_null(state->callee_leaf) && ts_node_eq(node, state->callee_leaf));
    if (exact_callee) {
        if (reference_node && state->invocation_kind == CBM_INVOCATION_CALLABLE_REFERENCE) {
            char *name = reference_name(ctx, node);
            if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
                CBMUsage usage = {0};
                usage.ref_name = name;
                usage.enclosing_func_qn = state->enclosing_func_qn;
                usage.lexical_scope_id = usage_lexical_scope_id_for_node(ctx, state, node);
                /* Kotlin callable-reference syntax is only a precision
                 * candidate. The semantic pass decides whether `::name`
                 * denotes one exact callable; unresolved properties and
                 * ambiguous values must remain ordinary USAGE. */
                if (ctx->language == CBM_LANG_KOTLIN) {
                    usage.kind = CBM_USAGE_VALUE;
                    usage.may_be_call_reference = true;
                } else {
                    usage.kind = CBM_USAGE_CALL_REFERENCE;
                }
                usage.site_start_byte = ts_node_start_byte(node);
                usage.site_end_byte = ts_node_end_byte(node);
                cbm_usages_push(&ctx->result->usages, ctx->arena, usage);
            }
        }
        return;
    }
    if (is_forward_sibling_callee(ctx->language, node)) {
        return;
    }
    if (is_call_argument_label_walk(node, state)) {
        return;
    }
    // Imports do not emit ordinary usages, but their binding occurrence must
    // be recorded before the subtree is skipped.
    if (state->inside_import) {
        if (is_import_binding_occurrence(ctx, node, spec)) {
            char *binding_name = reference_name(ctx, node);
            if (binding_name && binding_name[0] && !cbm_is_keyword(binding_name, ctx->language)) {
                record_lexical_binding(ctx, state, node, spec, binding_name, true);
            }
        }
        return;
    }

    bool python_scope_directive =
        ctx->language == CBM_LANG_PYTHON && (lexical_ancestor_kind(node, "global_statement") ||
                                             lexical_ancestor_kind(node, "nonlocal_statement"));
    if (python_scope_directive || is_binding_occurrence(ctx, node, spec, state)) {
        char *binding_name = reference_name(ctx, node);
        if (binding_name && binding_name[0] && !cbm_is_keyword(binding_name, ctx->language)) {
            record_lexical_binding(ctx, state, node, spec, binding_name, false);
        }
        return;
    }
    if (!reference_node) {
        return;
    }
    if (is_write_occurrence(ctx, node, spec, state)) {
        if (ctx->language == CBM_LANG_OBJECTSCRIPT_UDL ||
            ctx->language == CBM_LANG_OBJECTSCRIPT_ROUTINE) {
            char *binding_name = reference_name(ctx, node);
            if (binding_name && binding_name[0] && !cbm_is_keyword(binding_name, ctx->language)) {
                record_lexical_binding(ctx, state, node, spec, binding_name, false);
            }
        }
        return;
    }

    char *name = reference_name(ctx, node);
    if (name && name[0] && !cbm_is_keyword(name, ctx->language)) {
        CBMUsage usage = {0};
        usage.ref_name = name;
        usage.enclosing_func_qn = state->enclosing_func_qn;
        usage.lexical_scope_id = usage_lexical_scope_id_for_node(ctx, state, node);
        stamp_usage_site(ctx, &usage, node, name, state);
        cbm_usages_push(&ctx->result->usages, ctx->arena, usage);
    }
}
