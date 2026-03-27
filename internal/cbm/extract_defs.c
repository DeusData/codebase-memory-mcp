#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_alloc/strdup/sprintf
#include "helpers.h"
#include "lang_specs.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <stdint.h>          // uint32_t
#include <string.h>
#include <ctype.h>

// Field name lengths for ts_node_child_by_field_name() calls.
#define FIELD_LEN_FUNCTION_NAME 13      // strlen("function_name")
#define FIELD_LEN_FUNCTION_DECL_LEFT 22 // strlen("functionDeclarationLeft")

// Buffer sizes for local arrays (base classes, params, return types).
#define MAX_COMMENT_LEN 500
#define MAX_BASES 16
#define MAX_BASES_MINUS_1 15
#define MAX_PARAMS 32
#define MAX_PARAMS_MINUS_1 31
#define MAX_RETURN_TYPES_MINUS_1 15

// Forward declarations
static void extract_func_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void extract_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void walk_defs(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, int depth);
static void extract_variables(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec);
static void extract_class_variables(CBMExtractCtx *ctx, TSNode class_node, const CBMLangSpec *spec);
static void extract_rust_impl(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void extract_class_methods(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                  const CBMLangSpec *spec);
static void extract_class_fields(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                 const CBMLangSpec *spec);
static void extract_elixir_call(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);

// --- Helpers ---

// Get "name" field from a node
static TSNode func_name_node(TSNode node) {
    return ts_node_child_by_field_name(node, "name", 4);
}

// Resolve the name node for a function, handling language-specific quirks
// NOLINTNEXTLINE(readability-function-cognitive-complexity,misc-no-recursion)
static TSNode resolve_func_name(TSNode node, CBMLanguage lang, const char *source) {
    const char *kind = ts_node_type(node);

    // Haskell: skip signature nodes
    if (lang == CBM_LANG_HASKELL && strcmp(kind, "signature") == 0) {
        TSNode null_node = {0};
        return null_node;
    }

    TSNode name = func_name_node(node);

    // R: function_definition — name on parent binary_operator lhs
    if (lang == CBM_LANG_R && strcmp(kind, "function_definition") == 0) {
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "binary_operator") == 0) {
            // Try field names first
            TSNode lhs = ts_node_child_by_field_name(parent, "left", 4);
            if (ts_node_is_null(lhs)) {
                lhs = ts_node_child_by_field_name(parent, "lhs", 3);
            }
            // R grammar has no field names — first named child is the identifier
            if (ts_node_is_null(lhs) && ts_node_named_child_count(parent) > 0) {
                lhs = ts_node_named_child(parent, 0);
            }
            if (!ts_node_is_null(lhs)) {
                return lhs;
            }
        }
        TSNode null_node = {0};
        return null_node;
    }

    if (!ts_node_is_null(name)) {
        return name;
    }

    // Swift: function_declaration has simple_identifier (no "name" field)
    if (lang == CBM_LANG_SWIFT && strcmp(kind, "function_declaration") == 0) {
        TSNode si = cbm_find_child_by_kind(node, "simple_identifier");
        if (!ts_node_is_null(si)) {
            return si;
        }
    }

    // Lua: anonymous function assignment — check parent assignment_statement or expression_list
    if (lang == CBM_LANG_LUA && strcmp(kind, "function_definition") == 0) {
        // Walk up: function_definition -> expression_list -> assignment_statement
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "expression_list") == 0) {
            parent = ts_node_parent(parent);
        }
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "assignment_statement") == 0) {
            // Left side of assignment: variable_list -> variable
            TSNode vars = ts_node_child_by_field_name(parent, "variables", 9);
            if (ts_node_is_null(vars)) {
                uint32_t n = ts_node_child_count(parent);
                for (uint32_t i = 0; i < n; i++) {
                    TSNode c = ts_node_child(parent, i);
                    if (strcmp(ts_node_type(c), "variable_list") == 0) {
                        vars = c;
                        break;
                    }
                }
            }
            if (!ts_node_is_null(vars) && ts_node_child_count(vars) > 0) {
                return ts_node_child(vars, 0);
            }
        }
    }

    // OCaml: value_definition -> let_binding with pattern field
    if (lang == CBM_LANG_OCAML && strcmp(kind, "value_definition") == 0) {
        TSNode binding = cbm_find_child_by_kind(node, "let_binding");
        if (!ts_node_is_null(binding)) {
            TSNode pattern = ts_node_child_by_field_name(binding, "pattern", 7);
            if (!ts_node_is_null(pattern)) {
                return pattern;
            }
        }
    }

    // SQL: create_function — name in object_reference > identifier
    if (lang == CBM_LANG_SQL && strcmp(kind, "create_function") == 0) {
        TSNode obj_ref = cbm_find_child_by_kind(node, "object_reference");
        if (!ts_node_is_null(obj_ref)) {
            TSNode id = cbm_find_child_by_kind(obj_ref, "identifier");
            if (!ts_node_is_null(id)) {
                return id;
            }
        }
        return cbm_find_child_by_kind(node, "identifier");
    }

    // Zig: test_declaration — name is in string child
    if (lang == CBM_LANG_ZIG && strcmp(kind, "test_declaration") == 0) {
        TSNode str_node = cbm_find_child_by_kind(node, "string");
        if (!ts_node_is_null(str_node)) {
            TSNode content = cbm_find_child_by_kind(str_node, "string_content");
            if (!ts_node_is_null(content)) {
                return content;
            }
        }
    }

    // Arrow function: name on parent variable_declarator
    if (strcmp(kind, "arrow_function") == 0) {
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "variable_declarator") == 0) {
            return ts_node_child_by_field_name(parent, "name", 4);
        }
    }

    // VimScript: function_definition — name is inside function_declaration child
    // Both function_definition and function_declaration have production_id=0 (no field names)
    if (lang == CBM_LANG_VIMSCRIPT && strcmp(kind, "function_definition") == 0) {
        TSNode decl = cbm_find_child_by_kind(node, "function_declaration");
        if (!ts_node_is_null(decl)) {
            // function_declaration: first named child is the identifier/name
            uint32_t nc = ts_node_named_child_count(decl);
            if (nc > 0) {
                return ts_node_named_child(decl, 0);
            }
        }
        // Fallback: first named child of function_definition directly
        {
            uint32_t nc = ts_node_named_child_count(node);
            if (nc > 0) {
                return ts_node_named_child(node, 0);
            }
        }
    }

    // Julia: function_definition — no field names defined (production_id=0)
    // Name is inside first named child: identifier or call_expression(identifier, params)
    if (lang == CBM_LANG_JULIA && strcmp(kind, "function_definition") == 0) {
        TSNode current = node;
        for (int depth = 0; depth < 4; depth++) {
            uint32_t nc = ts_node_named_child_count(current);
            if (nc == 0) {
                break;
            }
            TSNode first = ts_node_named_child(current, 0);
            const char *fk = ts_node_type(first);
            if (strcmp(fk, "identifier") == 0 || strcmp(fk, "operator_identifier") == 0) {
                return first;
            }
            current = first;
        }
        TSNode null_node = {0};
        return null_node;
    }

    // CommonLisp: defun — name via function_name field (defun grammar uses field_function_name)
    if (lang == CBM_LANG_COMMONLISP && strcmp(kind, "defun") == 0) {
        TSNode fn = ts_node_child_by_field_name(node, "function_name", FIELD_LEN_FUNCTION_NAME);
        if (!ts_node_is_null(fn)) {
            return fn;
        }
        // Fallback: traverse defun_header -> first sym_lit child
        TSNode header = cbm_find_child_by_kind(node, "defun_header");
        if (!ts_node_is_null(header)) {
            return cbm_find_child_by_kind(header, "sym_lit");
        }
    }

    // Makefile: rule — name is first word in the targets group
    if (lang == CBM_LANG_MAKEFILE && strcmp(kind, "rule") == 0) {
        TSNode targets = cbm_find_child_by_kind(node, "targets");
        if (!ts_node_is_null(targets)) {
            uint32_t nc = ts_node_named_child_count(targets);
            if (nc > 0) {
                return ts_node_named_child(targets, 0);
            }
        }
        // Fallback: first word child directly on rule
        return cbm_find_child_by_kind(node, "word");
    }

    // Haskell: function — prod_id 151/152 has inherited field_name (handled above),
    // prod_id 77 (no-argument function like `main = ...`) has NO field_name.
    // Fallback: first named child of first child contains the variable name.
    if (lang == CBM_LANG_HASKELL && strcmp(kind, "function") == 0) {
        // func_name_node already returned null — prod_id 77 path
        if (ts_node_named_child_count(node) > 0) {
            TSNode head = ts_node_named_child(node, 0);
            const char *hk = ts_node_type(head);
            // Direct variable (function name without patterns)
            if (strcmp(hk, "variable") == 0 || strcmp(hk, "name") == 0) {
                return head;
            }
            // Function head node: first named child is the variable
            if (ts_node_named_child_count(head) > 0) {
                TSNode v = ts_node_named_child(head, 0);
                const char *vk = ts_node_type(v);
                if (strcmp(vk, "variable") == 0 || strcmp(vk, "name") == 0) {
                    return v;
                }
            }
        }
        TSNode null_node = {0};
        return null_node;
    }

    // Elm: value_declaration — name is inside functionDeclarationLeft child (field 22 chars)
    if (lang == CBM_LANG_ELM && strcmp(kind, "value_declaration") == 0) {
        TSNode fdl = ts_node_child_by_field_name(node, "functionDeclarationLeft",
                                                 FIELD_LEN_FUNCTION_DECL_LEFT);
        if (ts_node_is_null(fdl)) {
            fdl = cbm_find_child_by_kind(node, "function_declaration_left");
        }
        if (!ts_node_is_null(fdl) && ts_node_named_child_count(fdl) > 0) {
            return ts_node_named_child(fdl, 0);
        }
        TSNode null_node = {0};
        return null_node;
    }

    // MATLAB: function_definition — name via "name" field
    if (lang == CBM_LANG_MATLAB && strcmp(kind, "function_definition") == 0) {
        // MATLAB grammar: function [ret] = name(args)
        // The "name" field should be set; fallback to first identifier child
        if (!ts_node_is_null(name)) {
            return name;
        }
        return cbm_find_child_by_kind(node, "identifier");
    }

    // Lean: def/theorem/instance/abbrev — name via declId field
    if (lang == CBM_LANG_LEAN) {
        // Lean grammar uses "declId" field for the name (6 chars)
        TSNode decl_id = ts_node_child_by_field_name(node, "declId", 6);
        if (!ts_node_is_null(decl_id)) {
            // declId contains an identifier
            TSNode id = cbm_find_child_by_kind(decl_id, "ident");
            if (!ts_node_is_null(id)) {
                return id;
            }
            // Fallback: first named child of declId
            if (ts_node_named_child_count(decl_id) > 0) {
                return ts_node_named_child(decl_id, 0);
            }
            return decl_id;
        }
        // Fallback: look for "name" field or first identifier
        if (!ts_node_is_null(name)) {
            return name;
        }
        return cbm_find_child_by_kind(node, "ident");
    }

    // FORM: procedure_definition — name via "name" field (standard)
    // Magma: function/procedure/intrinsic_definition — name via "name" field (standard)
    // Both use standard "name" field which is handled above at line 33

    // Wolfram: set_delayed_top/set_top — LHS is apply(user_symbol("f"), ...) for f[x_] := ...
    if (lang == CBM_LANG_WOLFRAM &&
        (strcmp(kind, "set_delayed_top") == 0 || strcmp(kind, "set_top") == 0 ||
         strcmp(kind, "set_delayed") == 0 || strcmp(kind, "set") == 0)) {
        if (ts_node_named_child_count(node) > 0) {
            TSNode lhs = ts_node_named_child(node, 0);
            if (strcmp(ts_node_type(lhs), "apply") == 0 && ts_node_named_child_count(lhs) > 0) {
                TSNode head = ts_node_named_child(lhs, 0);
                if (strcmp(ts_node_type(head), "user_symbol") == 0) {
                    return head;
                }
            }
        }
        TSNode null_node = {0};
        return null_node;
    }

    // C++/CUDA: template_declaration wraps a function_definition — unwrap and resolve inner
    if ((lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA) &&
        strcmp(kind, "template_declaration") == 0) {
        // Find the inner function_definition or declaration child
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode ch = ts_node_named_child(node, i);
            const char *ck = ts_node_type(ch);
            if (strcmp(ck, "function_definition") == 0 || strcmp(ck, "declaration") == 0) {
                return resolve_func_name(ch, lang, source);
            }
        }
        TSNode null_node = {0};
        return null_node;
    }

    // C/C++/CUDA/GLSL: function_definition — name is inside the declarator chain
    // C grammar: function_definition{declarator:function_declarator{declarator:identifier}}
    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL) &&
        strcmp(kind, "function_definition") == 0) {
        TSNode decl = ts_node_child_by_field_name(node, "declarator", 10);
        for (int depth = 0; depth < 8 && !ts_node_is_null(decl); depth++) {
            const char *dk = ts_node_type(decl);
            if (strcmp(dk, "identifier") == 0) {
                return decl;
            }
            if (strcmp(dk, "field_identifier") == 0) {
                return decl;
            }
            // C++ operator functions: operator+, operator[], operator(), etc.
            if (strcmp(dk, "operator_name") == 0 || strcmp(dk, "operator_cast") == 0) {
                return decl;
            }
            // C++ destructor: ~ClassName
            if (strcmp(dk, "destructor_name") == 0) {
                return decl;
            }
            // C++ qualified name: Namespace::Function or Class::operator+
            if (strcmp(dk, "qualified_identifier") == 0 || strcmp(dk, "scoped_identifier") == 0) {
                // Check for operator_name child first (e.g., Class::operator+)
                TSNode op = cbm_find_child_by_kind(decl, "operator_name");
                if (!ts_node_is_null(op)) {
                    return op;
                }
                op = cbm_find_child_by_kind(decl, "operator_cast");
                if (!ts_node_is_null(op)) {
                    return op;
                }
                op = cbm_find_child_by_kind(decl, "destructor_name");
                if (!ts_node_is_null(op)) {
                    return op;
                }
                TSNode id = cbm_find_child_by_kind(decl, "identifier");
                if (!ts_node_is_null(id)) {
                    return id;
                }
                id = cbm_find_child_by_kind(decl, "field_identifier");
                if (!ts_node_is_null(id)) {
                    return id;
                }
                break;
            }
            // Unwrap pointer_declarator, reference_declarator, function_declarator, etc.
            TSNode inner = ts_node_child_by_field_name(decl, "declarator", 10);
            if (ts_node_is_null(inner) && ts_node_named_child_count(decl) > 0) {
                inner = ts_node_named_child(decl, 0);
            }
            if (ts_node_is_null(inner)) {
                break;
            }
            decl = inner;
        }
        TSNode null_node = {0};
        return null_node;
    }

    TSNode null_node = {0};
    return null_node;
}

// Check for export_statement ancestor (JS/TS/TSX)
static bool is_js_exported(TSNode node) {
    return cbm_has_ancestor_kind(node, "export_statement", 4);
}

// Extract docstring from the node's leading comment
static const char *extract_docstring(CBMArena *a, TSNode node, const char *source,
                                     CBMLanguage lang) {
    // Go: type_spec is inside type_declaration; comment is before type_declaration
    if (lang == CBM_LANG_GO) {
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "type_spec") == 0 || strcmp(kind, "type_alias") == 0) {
            TSNode parent = ts_node_parent(node);
            if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "type_declaration") == 0) {
                TSNode pprev = ts_node_prev_sibling(parent);
                if (!ts_node_is_null(pprev)) {
                    const char *ppk = ts_node_type(pprev);
                    if (strcmp(ppk, "comment") == 0 || strcmp(ppk, "block_comment") == 0 ||
                        strcmp(ppk, "line_comment") == 0) {
                        char *text = cbm_node_text(a, pprev, source);
                        if (text && strlen(text) > MAX_COMMENT_LEN) {
                            text[MAX_COMMENT_LEN] = '\0';
                        }
                        return text;
                    }
                }
            }
        }
    }

    // Check previous sibling for comment
    TSNode prev = ts_node_prev_sibling(node);
    if (!ts_node_is_null(prev)) {
        const char *pk = ts_node_type(prev);
        if (strcmp(pk, "comment") == 0 || strcmp(pk, "block_comment") == 0 ||
            strcmp(pk, "line_comment") == 0) {
            char *text = cbm_node_text(a, prev, source);
            if (text && strlen(text) > MAX_COMMENT_LEN) {
                text[MAX_COMMENT_LEN] = '\0';
            }
            return text;
        }
    }

    // Python: docstring as first child expression_statement -> string inside function body
    if (lang == CBM_LANG_PYTHON) {
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        if (!ts_node_is_null(body) && ts_node_named_child_count(body) > 0) {
            TSNode first = ts_node_named_child(body, 0);
            if (!ts_node_is_null(first) &&
                strcmp(ts_node_type(first), "expression_statement") == 0) {
                if (ts_node_named_child_count(first) > 0) {
                    TSNode str = ts_node_named_child(first, 0);
                    if (!ts_node_is_null(str)) {
                        const char *sk = ts_node_type(str);
                        if (strcmp(sk, "string") == 0 || strcmp(sk, "concatenated_string") == 0) {
                            char *text = cbm_node_text(a, str, source);
                            if (text && strlen(text) > MAX_COMMENT_LEN) {
                                text[MAX_COMMENT_LEN] = '\0';
                            }
                            return text;
                        }
                    }
                }
            }
        }
    }
    return NULL;
}

// Extract decorator names from preceding decorator/annotation nodes
static const char **extract_decorators(CBMArena *a, TSNode node, const char *source,
                                       CBMLanguage lang, const CBMLangSpec *spec) {
    if (!spec->decorator_node_types || !spec->decorator_node_types[0]) {
        return NULL;
    }

    // Count decorators (preceding siblings matching decorator types)
    int count = 0;
    TSNode prev = ts_node_prev_sibling(node);
    while (!ts_node_is_null(prev)) {
        if (cbm_kind_in_set(prev, spec->decorator_node_types)) {
            count++;
        } else {
            break; // stop at first non-decorator
        }
        prev = ts_node_prev_sibling(prev);
    }

    // Java/Kotlin/C#: annotations live inside "modifiers" child, not as preceding siblings
    TSNode modifiers = {0};
    int mod_count = 0;
    if (count == 0 &&
        (lang == CBM_LANG_JAVA || lang == CBM_LANG_KOTLIN || lang == CBM_LANG_CSHARP)) {
        modifiers = ts_node_child_by_field_name(node, "modifiers", 9);
        if (ts_node_is_null(modifiers)) {
            modifiers = cbm_find_child_by_kind(node, "modifiers");
        }
        if (!ts_node_is_null(modifiers)) {
            uint32_t mc = ts_node_child_count(modifiers);
            for (uint32_t mi = 0; mi < mc; mi++) {
                TSNode mchild = ts_node_child(modifiers, mi);
                if (cbm_kind_in_set(mchild, spec->decorator_node_types)) {
                    mod_count++;
                }
            }
        }
    }

    int total = count + mod_count;
    if (total == 0) {
        return NULL;
    }

    const char **result = (const char **)cbm_arena_alloc(a, sizeof(const char *) * (total + 1));
    if (!result) {
        return NULL;
    }

    int idx = 0;
    // Preceding siblings
    prev = ts_node_prev_sibling(node);
    while (!ts_node_is_null(prev) && idx < count) {
        if (cbm_kind_in_set(prev, spec->decorator_node_types)) {
            result[idx++] = cbm_node_text(a, prev, source);
        } else {
            break;
        }
        prev = ts_node_prev_sibling(prev);
    }
    // Modifiers children
    if (!ts_node_is_null(modifiers)) {
        uint32_t mc = ts_node_child_count(modifiers);
        for (uint32_t mi = 0; mi < mc && idx < total; mi++) {
            TSNode mchild = ts_node_child(modifiers, mi);
            if (cbm_kind_in_set(mchild, spec->decorator_node_types)) {
                result[idx++] = cbm_node_text(a, mchild, source);
            }
        }
    }
    result[idx] = NULL;
    return result;
}

// Extract base class names from a class node
static const char **extract_base_classes(CBMArena *a, TSNode node, const char *source,
                                         CBMLanguage lang) {
    (void)lang;
    // Try common field names for superclass lists
    static const char *fields[] = {"superclass",
                                   "superclasses",
                                   "superinterfaces",
                                   "interfaces",
                                   "bases",
                                   "type_inheritance_clause",
                                   "delegation_specifiers",
                                   NULL};

    for (const char **f = fields; *f; f++) {
        TSNode super = ts_node_child_by_field_name(node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(super)) {
            char *text = cbm_node_text(a, super, source);
            if (text && text[0]) {
                const char **result = (const char **)cbm_arena_alloc(a, sizeof(const char *) * 2);
                if (result) {
                    result[0] = text;
                    result[1] = NULL;
                    return result;
                }
            }
        }
    }
    // C# specific: handle base_list node (contains base types separated by commas)
    {
        uint32_t count = ts_node_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_child(node, i);
            if (strcmp(ts_node_type(child), "base_list") == 0) {
                const char *bases[16];
                int base_count = 0;
                uint32_t bnc = ts_node_named_child_count(child);
                for (uint32_t bi = 0; bi < bnc && base_count < MAX_BASES_MINUS_1; bi++) {
                    TSNode bc = ts_node_named_child(child, bi);
                    const char *bk = ts_node_type(bc);
                    // C# base types can be: identifier, generic_name, qualified_name,
                    // or wrapped in a simple_base_type / primary_constructor_base_type
                    char *text = NULL;
                    if (strcmp(bk, "identifier") == 0 || strcmp(bk, "generic_name") == 0 ||
                        strcmp(bk, "qualified_name") == 0) {
                        text = cbm_node_text(a, bc, source);
                    } else {
                        // For wrapper nodes (simple_base_type etc.), extract the first
                        // named child which should be the type identifier
                        TSNode inner = ts_node_named_child(bc, 0);
                        if (!ts_node_is_null(inner)) {
                            text = cbm_node_text(a, inner, source);
                        }
                    }
                    if (text && text[0]) {
                        // Strip generic args for resolution: "List<int>" → "List"
                        char *angle = strchr(text, '<');
                        if (angle) *angle = '\0';
                        bases[base_count++] = text;
                    }
                }
                if (base_count > 0) {
                    const char **result =
                        (const char **)cbm_arena_alloc(a, (base_count + 1) * sizeof(const char *));
                    if (result) {
                        for (int j = 0; j < base_count; j++) {
                            result[j] = bases[j];
                        }
                        result[base_count] = NULL;
                        return result;
                    }
                }
            }
        }
    }

    // C/C++ specific: handle base_class_clause (contains access specifiers + type names)
    {
        uint32_t count2 = ts_node_child_count(node);
        for (uint32_t i = 0; i < count2; i++) {
            TSNode child = ts_node_child(node, i);
            if (strcmp(ts_node_type(child), "base_class_clause") == 0) {
                // Extract type identifiers from base_class_clause, skipping access specifiers
                const char *bases[16];
                int base_count = 0;
                uint32_t bnc = ts_node_named_child_count(child);
                for (uint32_t bi = 0; bi < bnc && base_count < MAX_BASES_MINUS_1; bi++) {
                    TSNode bc = ts_node_named_child(child, bi);
                    const char *bk = ts_node_type(bc);
                    if (strcmp(bk, "access_specifier") == 0) {
                        continue;
                    }
                    // type_identifier, qualified_identifier, scoped_identifier, template_type
                    if (strcmp(bk, "type_identifier") == 0 ||
                        strcmp(bk, "qualified_identifier") == 0 ||
                        strcmp(bk, "scoped_identifier") == 0) {
                        char *text = cbm_node_text(a, bc, source);
                        if (text && text[0]) {
                            bases[base_count++] = text;
                        }
                    } else if (strcmp(bk, "template_type") == 0) {
                        // For template base: extract just the template name (not args)
                        TSNode tname = ts_node_child_by_field_name(bc, "name", 4);
                        if (!ts_node_is_null(tname)) {
                            char *text = cbm_node_text(a, tname, source);
                            if (text && text[0]) {
                                bases[base_count++] = text;
                            }
                        }
                    }
                }
                if (base_count > 0) {
                    const char **result =
                        (const char **)cbm_arena_alloc(a, (base_count + 1) * sizeof(const char *));
                    if (result) {
                        for (int j = 0; j < base_count; j++) {
                            result[j] = bases[j];
                        }
                        result[base_count] = NULL;
                        return result;
                    }
                }
            }
        }
    }

    // Fallback: search for common base class node types as children
    static const char *base_types[] = {"superclass",
                                       "superinterfaces",
                                       "type_inheritance_clause",
                                       "class_heritage",
                                       "delegation_specifiers",
                                       "super_interfaces",
                                       "extends_clause",
                                       "implements_clause",
                                       "argument_list",
                                       "inheritance_specifier",
                                       NULL};
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        for (const char **t = base_types; *t; t++) {
            if (strcmp(ck, *t) == 0) {
                char *text = cbm_node_text(a, child, source);
                if (text && text[0]) {
                    const char **result =
                        (const char **)cbm_arena_alloc(a, sizeof(const char *) * 2);
                    if (result) {
                        result[0] = text;
                        result[1] = NULL;
                        return result;
                    }
                }
            }
        }
    }
    return NULL;
}

// Classify class label from AST node kind
static const char *class_label_for_kind(const char *kind) {
    if (strcmp(kind, "interface_declaration") == 0 || strcmp(kind, "interface_type") == 0 ||
        strcmp(kind, "trait_item") == 0 || strcmp(kind, "trait_definition") == 0 ||
        strcmp(kind, "protocol_declaration") == 0) {
        return "Interface";
    }
    if (strcmp(kind, "enum_specifier") == 0 || strcmp(kind, "enum_declaration") == 0 ||
        strcmp(kind, "enum_item") == 0) {
        return "Enum";
    }
    if (strcmp(kind, "type_alias_declaration") == 0 || strcmp(kind, "type_item") == 0 ||
        strcmp(kind, "type_alias") == 0 || strcmp(kind, "type_definition") == 0) {
        return "Type";
    }
    return "Class";
}

// --- Parameter type extraction ---

// Builtin types we skip (not useful as USES_TYPE targets).
static bool is_builtin_type(const char *name) {
    static const char *builtins[] = {
        "int",       "int8",       "int16",     "int32",   "int64",   "uint",      "uint8",
        "uint16",    "uint32",     "uint64",    "float",   "float32", "float64",   "double",
        "string",    "str",        "bool",      "boolean", "byte",    "rune",      "void",
        "None",      "any",        "interface", "object",  "Object",  "error",     "uintptr",
        "complex64", "complex128", "number",    "bigint",  "symbol",  "undefined", "null",
        "char",      "short",      "long",      "i8",      "i16",     "i32",       "i64",
        "u8",        "u16",        "u32",       "u64",     "f32",     "f64",       "usize",
        "isize",     "self",       "Self",      "cls",     "type",    "Int",       "Int8",
        "Int16",     "Int32",      "Int64",     "UInt",    "UInt8",   "UInt16",    "UInt32",
        "UInt64",    "Float",      "Double",    "String",  "Bool",    "Boolean",   "Byte",
        "Short",     "Long",       "Char",      "Unit",    "Void",    "Any",       "Nothing",
        "Dynamic",   NULL};
    for (const char **b = builtins; *b; b++) {
        if (strcmp(name, *b) == 0) {
            return true;
        }
    }
    return false;
}

// Clean a type name: strip *, &, [], ..., generics
static char *clean_type_name(CBMArena *a, const char *raw) {
    if (!raw || !raw[0]) {
        return NULL;
    }
    const char *s = raw;
    // Skip leading whitespace, ":", "*", "&", "[]", "..."
    while (*s == ' ' || *s == '\t' || *s == ':' || *s == '*' || *s == '&' || *s == '[' ||
           *s == ']' || *s == '.') {
        s++;
    }
    if (!*s) {
        return NULL;
    }
    // Find end: stop at <, [, or whitespace
    size_t len = 0;
    while (s[len] && s[len] != '<' && s[len] != '[' && s[len] != ' ') {
        len++;
    }
    if (len == 0) {
        return NULL;
    }
    char *result = cbm_arena_alloc(a, len + 1);
    memcpy(result, s, len);
    result[len] = '\0';
    return result;
}

// Extract param_names from a parameter list node.
// Returns NULL-terminated arena-allocated array.
static const char **extract_param_names(CBMArena *a, TSNode params, const char *source,
                                        CBMLanguage lang) {
    (void)lang;
    if (ts_node_is_null(params)) {
        return NULL;
    }

    const char *names[32];
    int count = 0;

    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc && count < MAX_PARAMS_MINUS_1; i++) {
        TSNode param = ts_node_child(params, i);
        if (ts_node_is_null(param) || !ts_node_is_named(param)) {
            continue;
        }

        const char *pk = ts_node_type(param);
        char *name_text = NULL;

        // Go: parameter_declaration has "name" field
        if (strcmp(pk, "parameter_declaration") == 0) {
            TSNode nm = ts_node_child_by_field_name(param, "name", 4);
            if (!ts_node_is_null(nm)) {
                name_text = cbm_node_text(a, nm, source);
            }
        }
        // Generic: try "name" field on parameter nodes
        else if (strcmp(pk, "formal_parameter") == 0 || strcmp(pk, "parameter") == 0 ||
                 strcmp(pk, "required_parameter") == 0 || strcmp(pk, "optional_parameter") == 0 ||
                 strcmp(pk, "simple_parameter") == 0 || strcmp(pk, "typed_parameter") == 0) {
            TSNode nm = ts_node_child_by_field_name(param, "name", 4);
            if (ts_node_is_null(nm)) {
                nm = ts_node_child_by_field_name(param, "pattern", 7);
            }
            if (!ts_node_is_null(nm)) {
                if (strcmp(ts_node_type(nm), "identifier") == 0 ||
                    strcmp(ts_node_type(nm), "simple_identifier") == 0) {
                    name_text = cbm_node_text(a, nm, source);
                }
            }
        }

        if (name_text && name_text[0]) {
            names[count++] = name_text;
        }
    }

    if (count == 0) {
        return NULL;
    }

    const char **result = (const char **)cbm_arena_alloc(a, (count + 1) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = names[i];
    }
    result[count] = NULL;
    return result;
}

// Extract return_types from a return type node.
// Parses Go-style multi-return (T1, T2) and single return types.
// Returns NULL-terminated arena-allocated array.
static const char **extract_return_types(CBMArena *a, TSNode rt_node, const char *source,
                                         CBMLanguage lang) {
    (void)lang;
    if (ts_node_is_null(rt_node)) {
        return NULL;
    }

    const char *types[16];
    int count = 0;

    const char *kind = ts_node_type(rt_node);

    // Go: parameter_list as result type means multi-return
    if (strcmp(kind, "parameter_list") == 0) {
        uint32_t nc = ts_node_child_count(rt_node);
        for (uint32_t i = 0; i < nc && count < MAX_RETURN_TYPES_MINUS_1; i++) {
            TSNode child = ts_node_child(rt_node, i);
            if (ts_node_is_null(child) || !ts_node_is_named(child)) {
                continue;
            }
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "parameter_declaration") == 0) {
                // Get the type from the parameter_declaration
                TSNode tn = ts_node_child_by_field_name(child, "type", 4);
                if (!ts_node_is_null(tn)) {
                    char *type_text = cbm_node_text(a, tn, source);
                    if (type_text && type_text[0]) {
                        char *cleaned = clean_type_name(a, type_text);
                        if (cleaned && cleaned[0]) {
                            types[count++] = cleaned;
                        }
                    }
                }
            } else {
                // Bare type in result list
                char *type_text = cbm_node_text(a, child, source);
                if (type_text && type_text[0]) {
                    char *cleaned = clean_type_name(a, type_text);
                    if (cleaned && cleaned[0]) {
                        types[count++] = cleaned;
                    }
                }
            }
        }
    } else {
        // Single return type
        char *type_text = cbm_node_text(a, rt_node, source);
        if (type_text && type_text[0]) {
            char *cleaned = clean_type_name(a, type_text);
            if (cleaned && cleaned[0]) {
                types[count++] = cleaned;
            }
        }
    }

    if (count == 0) {
        return NULL;
    }

    const char **result = (const char **)cbm_arena_alloc(a, (count + 1) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = types[i];
    }
    result[count] = NULL;
    return result;
}

// Extract param_types from a parameter list node.
// Returns NULL-terminated arena-allocated array.
static const char **extract_param_types(CBMArena *a, TSNode params, const char *source,
                                        CBMLanguage lang) {
    if (ts_node_is_null(params)) {
        return NULL;
    }

    // Temporary buffer (max 32 param types)
    const char *types[32];
    int count = 0;

    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc && count < MAX_PARAMS_MINUS_1; i++) {
        TSNode param = ts_node_child(params, i);
        if (ts_node_is_null(param) || !ts_node_is_named(param)) {
            continue;
        }

        const char *pk = ts_node_type(param);
        char *type_text = NULL;

        switch (lang) {
        case CBM_LANG_TYPESCRIPT:
        case CBM_LANG_TSX: {
            // TS: required_parameter/optional_parameter -> type_annotation child
            if (strcmp(pk, "required_parameter") == 0 || strcmp(pk, "optional_parameter") == 0) {
                TSNode ta = cbm_find_child_by_kind(param, "type_annotation");
                if (!ts_node_is_null(ta)) {
                    // type_annotation contains ": Type" — get the type identifier
                    uint32_t tanc = ts_node_named_child_count(ta);
                    for (uint32_t ti = 0; ti < tanc; ti++) {
                        TSNode tch = ts_node_named_child(ta, ti);
                        if (!ts_node_is_null(tch)) {
                            const char *tk = ts_node_type(tch);
                            if (strcmp(tk, "type_identifier") == 0 ||
                                strcmp(tk, "generic_type") == 0 ||
                                strcmp(tk, "predefined_type") == 0) {
                                type_text = cbm_node_text(a, tch, source);
                                break;
                            }
                        }
                    }
                }
            }
            break;
        }
        case CBM_LANG_KOTLIN: {
            // Kotlin: parameter -> "type" field or user_type child
            if (strcmp(pk, "parameter") == 0) {
                TSNode tn = ts_node_child_by_field_name(param, "type", 4);
                if (!ts_node_is_null(tn)) {
                    type_text = cbm_node_text(a, tn, source);
                } else {
                    TSNode ut = cbm_find_child_by_kind(param, "user_type");
                    if (!ts_node_is_null(ut)) {
                        type_text = cbm_node_text(a, ut, source);
                    }
                }
            }
            break;
        }
        case CBM_LANG_SCALA: {
            // Scala: parameter -> type_identifier child
            if (strcmp(pk, "parameter") == 0) {
                TSNode tid = cbm_find_child_by_kind(param, "type_identifier");
                if (!ts_node_is_null(tid)) {
                    type_text = cbm_node_text(a, tid, source);
                }
            }
            break;
        }
        case CBM_LANG_DART: {
            // Dart: formal_parameter -> type_identifier child
            if (strcmp(pk, "formal_parameter") == 0) {
                TSNode tid = cbm_find_child_by_kind(param, "type_identifier");
                if (!ts_node_is_null(tid)) {
                    type_text = cbm_node_text(a, tid, source);
                }
            }
            break;
        }
        case CBM_LANG_GROOVY: {
            // Groovy: parameter -> "type" field
            if (strcmp(pk, "parameter") == 0) {
                TSNode tn = ts_node_child_by_field_name(param, "type", 4);
                if (!ts_node_is_null(tn)) {
                    type_text = cbm_node_text(a, tn, source);
                }
            }
            break;
        }
        case CBM_LANG_OCAML: {
            // OCaml: parameter -> typed_pattern -> "type" field
            if (strcmp(pk, "parameter") == 0) {
                TSNode tp = cbm_find_child_by_kind(param, "typed_pattern");
                if (!ts_node_is_null(tp)) {
                    TSNode tn = ts_node_child_by_field_name(tp, "type", 4);
                    if (!ts_node_is_null(tn)) {
                        type_text = cbm_node_text(a, tn, source);
                    }
                }
            }
            break;
        }
        default: {
            // Generic: formal_parameter, parameter, parameter_declaration,
            // spread_parameter, simple_parameter, variadic_parameter -> "type" field
            if (strcmp(pk, "formal_parameter") == 0 || strcmp(pk, "parameter") == 0 ||
                strcmp(pk, "parameter_declaration") == 0 || strcmp(pk, "spread_parameter") == 0 ||
                strcmp(pk, "simple_parameter") == 0 || strcmp(pk, "variadic_parameter") == 0 ||
                strcmp(pk, "typed_parameter") == 0) {
                TSNode tn = ts_node_child_by_field_name(param, "type", 4);
                if (!ts_node_is_null(tn)) {
                    type_text = cbm_node_text(a, tn, source);
                }
            }
            break;
        }
        }

        if (type_text && type_text[0]) {
            char *cleaned = clean_type_name(a, type_text);
            if (cleaned && cleaned[0] && !is_builtin_type(cleaned)) {
                // Deduplicate
                bool dup = false;
                for (int j = 0; j < count; j++) {
                    if (strcmp(types[j], cleaned) == 0) {
                        dup = true;
                        break;
                    }
                }
                if (!dup) {
                    types[count++] = cleaned;
                }
            }
        }
    }

    if (count == 0) {
        return NULL;
    }

    // Build NULL-terminated array
    const char **result = (const char **)cbm_arena_alloc(a, (count + 1) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = types[i];
    }
    result[count] = NULL;
    return result;
}

// --- Function definition extraction ---

static void extract_func_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;

    TSNode name_node = resolve_func_name(node, ctx->language, ctx->source);
    if (ts_node_is_null(name_node)) {
        return;
    }

    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0] || strcmp(name, "function") == 0) {
        return;
    }

    // For template_declaration, use the inner function_definition for field lookups
    // (parameters, return type, etc. are on the inner node, not the template wrapper)
    TSNode func_node = node;
    if ((ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA) &&
        strcmp(ts_node_type(node), "template_declaration") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode ch = ts_node_named_child(node, i);
            const char *ck = ts_node_type(ch);
            if (strcmp(ck, "function_definition") == 0 || strcmp(ck, "declaration") == 0) {
                func_node = ch;
                break;
            }
        }
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));

    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = "Function";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + 1;
    def.end_line = ts_node_end_point(node).row + 1;
    def.lines = (int)(def.end_line - def.start_line + 1);
    def.is_exported = cbm_is_exported(name, ctx->language);

    // Parameters — use func_node (inner function for templates)
    TSNode params = ts_node_child_by_field_name(func_node, "parameters", 10);
    // C/C++/CUDA/GLSL: parameters live on function_declarator inside declarator chain
    if (ts_node_is_null(params) &&
        (ctx->language == CBM_LANG_C || ctx->language == CBM_LANG_CPP ||
         ctx->language == CBM_LANG_CUDA || ctx->language == CBM_LANG_GLSL)) {
        TSNode decl = ts_node_child_by_field_name(func_node, "declarator", 10);
        for (int d = 0; d < 5 && !ts_node_is_null(decl); d++) {
            params = ts_node_child_by_field_name(decl, "parameters", 10);
            if (!ts_node_is_null(params)) {
                break;
            }
            decl = ts_node_child_by_field_name(decl, "declarator", 10);
        }
    }
    if (!ts_node_is_null(params)) {
        def.signature = cbm_node_text(a, params, ctx->source);
        def.param_names = extract_param_names(a, params, ctx->source, ctx->language);
        def.param_types = extract_param_types(a, params, ctx->source, ctx->language);
    }

    // Return type — use func_node (inner function for templates)
    static const char *rt_fields[] = {"result", "return_type", "type", NULL};
    for (const char **f = rt_fields; *f; f++) {
        TSNode rt = ts_node_child_by_field_name(func_node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(rt)) {
            def.return_type = cbm_node_text(a, rt, ctx->source);
            def.return_types = extract_return_types(a, rt, ctx->source, ctx->language);
            break;
        }
    }

    // C++: trailing return type (auto f() -> Type) — override "auto" with actual type
    if (def.return_type && strcmp(def.return_type, "auto") == 0 &&
        (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA)) {
        TSNode declarator = ts_node_child_by_field_name(func_node, "declarator", 10);
        if (!ts_node_is_null(declarator)) {
            // trailing_return_type is a child of the function_declarator
            uint32_t nc = ts_node_named_child_count(declarator);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode ch = ts_node_named_child(declarator, i);
                if (strcmp(ts_node_type(ch), "trailing_return_type") == 0) {
                    // trailing_return_type contains a type_descriptor as first named child
                    TSNode type_desc =
                        ts_node_named_child_count(ch) > 0 ? ts_node_named_child(ch, 0) : ch;
                    def.return_type = cbm_node_text(a, type_desc, ctx->source);
                    // Also update return_types array (so registration uses trailing type, not
                    // "auto")
                    if (def.return_type && def.return_type[0]) {
                        const char **rt =
                            (const char **)cbm_arena_alloc(a, 2 * sizeof(const char *));
                        if (rt) {
                            rt[0] = def.return_type;
                            rt[1] = NULL;
                            def.return_types = rt;
                        }
                    }
                    break;
                }
            }
        }
    }

    // Receiver (Go methods)
    TSNode recv = ts_node_child_by_field_name(node, "receiver", 8);
    if (!ts_node_is_null(recv)) {
        def.receiver = cbm_node_text(a, recv, ctx->source);
        def.label = "Method";
    }

    // Decorators
    def.decorators = extract_decorators(a, node, ctx->source, ctx->language, spec);

    // Docstring
    def.docstring = extract_docstring(a, node, ctx->source, ctx->language);

    // Complexity
    if (spec->branching_node_types && spec->branching_node_types[0]) {
        def.complexity = cbm_count_branching(node, spec->branching_node_types);
    }

    // JS/TS export detection
    if (ctx->language == CBM_LANG_JAVASCRIPT || ctx->language == CBM_LANG_TYPESCRIPT ||
        ctx->language == CBM_LANG_TSX) {
        if (is_js_exported(node)) {
            def.is_entry_point = true;
        }
    }

    // main is always an entry point
    if (strcmp(name, "main") == 0) {
        def.is_entry_point = true;
    }

    cbm_defs_push(&ctx->result->defs, a, def);
}

// --- Class definition extraction ---

static void extract_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);

    // Config language class extraction (TOML tables, INI sections, XML elements, Markdown headings)
    if (ctx->language == CBM_LANG_TOML &&
        (strcmp(kind, "table") == 0 || strcmp(kind, "table_array_element") == 0)) {
        // TOML table: name from first bare_key/dotted_key/quoted_key child,
        // or from the nested key within a bracket header
        char *name = NULL;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && !name; i++) {
            TSNode child = ts_node_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "bare_key") == 0 || strcmp(ck, "dotted_key") == 0 ||
                strcmp(ck, "quoted_key") == 0 || strcmp(ck, "key") == 0) {
                name = cbm_node_text(a, child, ctx->source);
            }
        }
        if (!name || !name[0]) {
            return;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        def.label = "Class";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + 1;
        def.end_line = ts_node_end_point(node).row + 1;
        def.is_exported = true;
        cbm_defs_push(&ctx->result->defs, a, def);
        return;
    }

    if (ctx->language == CBM_LANG_INI && strcmp(kind, "section") == 0) {
        // INI section: name from section_name child
        char *name = NULL;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && !name; i++) {
            TSNode child = ts_node_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "section_name") == 0) {
                name = cbm_node_text(a, child, ctx->source);
            }
        }
        if (!name || !name[0]) {
            return;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        def.label = "Class";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + 1;
        def.end_line = ts_node_end_point(node).row + 1;
        def.is_exported = true;
        cbm_defs_push(&ctx->result->defs, a, def);
        return;
    }

    if (ctx->language == CBM_LANG_XML && strcmp(kind, "element") == 0) {
        // XML element: name from start_tag > tag_name or self_closing_tag > tag_name
        char *name = NULL;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && !name; i++) {
            TSNode child = ts_node_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "start_tag") == 0 || strcmp(ck, "self_closing_tag") == 0 ||
                strcmp(ck, "STag") == 0 || strcmp(ck, "EmptyElemTag") == 0) {
                // Find Name or tag_name child
                uint32_t tnc = ts_node_child_count(child);
                for (uint32_t j = 0; j < tnc; j++) {
                    TSNode tag = ts_node_child(child, j);
                    const char *tk = ts_node_type(tag);
                    if (strcmp(tk, "tag_name") == 0 || strcmp(tk, "Name") == 0) {
                        name = cbm_node_text(a, tag, ctx->source);
                        break;
                    }
                }
            }
        }
        // Fallback: try "Name" field directly for some XML grammars
        if (!name) {
            TSNode name_child = cbm_find_child_by_kind(node, "Name");
            if (!ts_node_is_null(name_child)) {
                name = cbm_node_text(a, name_child, ctx->source);
            }
        }
        if (!name || !name[0]) {
            return;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        def.label = "Class";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + 1;
        def.end_line = ts_node_end_point(node).row + 1;
        def.is_exported = true;
        cbm_defs_push(&ctx->result->defs, a, def);
        return;
    }

    if (ctx->language == CBM_LANG_MARKDOWN &&
        (strcmp(kind, "atx_heading") == 0 || strcmp(kind, "setext_heading") == 0)) {
        // Markdown heading: extract text content as name, use "Section" label
        // For atx_heading: children are atx_h[1-6]_marker + inline content
        // For setext_heading: children are paragraph + setext_h[12]_underline
        char *name = NULL;
        if (strcmp(kind, "atx_heading") == 0) {
            // Find heading_content or inline child (skip the marker)
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode child = ts_node_child(node, i);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "heading_content") == 0 || strcmp(ck, "inline") == 0) {
                    name = cbm_node_text(a, child, ctx->source);
                    break;
                }
            }
            // Fallback: extract everything after the # marker
            if (!name) {
                char *full = cbm_node_text(a, node, ctx->source);
                if (full) {
                    // Skip leading # and space
                    char *p = full;
                    while (*p == '#') {
                        p++;
                    }
                    while (*p == ' ') {
                        p++;
                    }
                    if (*p) {
                        name = cbm_arena_strdup(a, p);
                    }
                }
            }
        } else {
            // setext_heading: first child is the heading text (paragraph)
            if (ts_node_child_count(node) > 0) {
                TSNode first = ts_node_child(node, 0);
                const char *fk = ts_node_type(first);
                (void)fk; // any child type: extract text
                name = cbm_node_text(a, first, ctx->source);
            }
        }
        if (!name || !name[0]) {
            return;
        }
        // Trim trailing whitespace/newlines
        size_t len = strlen(name);
        while (len > 0 &&
               (name[len - 1] == '\n' || name[len - 1] == '\r' || name[len - 1] == ' ')) {
            name[len - 1] = '\0';
            len--;
        }
        if (!name[0]) {
            return;
        }
        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        def.label = "Section"; // NOT "Class" — avoids polluting class queries
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + 1;
        def.end_line = ts_node_end_point(node).row + 1;
        def.is_exported = true;
        cbm_defs_push(&ctx->result->defs, a, def);
        return;
    }

    // HCL blocks need special handling
    if (ctx->language == CBM_LANG_HCL && strcmp(kind, "block") == 0) {
        // Simple: use first identifier child as name
        TSNode id = cbm_find_child_by_kind(node, "identifier");
        if (ts_node_is_null(id)) {
            return;
        }
        char *name = cbm_node_text(a, id, ctx->source);
        if (!name || !name[0]) {
            return;
        }

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        def.label = "Class";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + 1;
        def.end_line = ts_node_end_point(node).row + 1;
        def.is_exported = true;
        cbm_defs_push(&ctx->result->defs, a, def);
        return;
    }

    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    // ObjC: class name is first identifier child
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_OBJC) {
        name_node = cbm_find_child_by_kind(node, "identifier");
    }
    // Swift: class name is type_identifier child (no "name" field)
    if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_SWIFT) {
        name_node = cbm_find_child_by_kind(node, "type_identifier");
    }
    if (ts_node_is_null(name_node)) {
        return;
    }

    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return;
    }

    // For nested classes, prefix with enclosing class QN (e.g., Outer.Inner)
    const char *class_qn;
    if (ctx->enclosing_class_qn) {
        class_qn = cbm_arena_sprintf(a, "%s.%s", ctx->enclosing_class_qn, name);
    } else {
        class_qn = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    }
    const char *label = class_label_for_kind(kind);

    // Go type_spec: check inner type for interface/struct
    if (strcmp(kind, "type_spec") == 0) {
        TSNode type_inner = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_inner)) {
            const char *inner_kind = ts_node_type(type_inner);
            if (strcmp(inner_kind, "interface_type") == 0) {
                label = "Interface";
            } else if (strcmp(inner_kind, "struct_type") == 0) {
                label = "Class";
            }
        }
    }

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = class_qn;
    def.label = label;
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + 1;
    def.end_line = ts_node_end_point(node).row + 1;
    def.is_exported = cbm_is_exported(name, ctx->language);
    def.base_classes = extract_base_classes(a, node, ctx->source, ctx->language);
    def.decorators = extract_decorators(a, node, ctx->source, ctx->language, spec);
    def.docstring = extract_docstring(a, node, ctx->source, ctx->language);

    cbm_defs_push(&ctx->result->defs, a, def);

    // Extract methods inside the class
    extract_class_methods(ctx, node, class_qn, spec);

    // Extract typed struct/class fields (for cross-file LSP type resolution)
    extract_class_fields(ctx, node, class_qn, spec);

    // Extract class-level variables (field declarations)
    extract_class_variables(ctx, node, spec);
}

// Find the body/members node inside a class node
static TSNode find_class_body(TSNode class_node, CBMLanguage lang) {
    // Try field names first
    static const char *body_fields[] = {"body", "members", "class_body", "declaration_list", NULL};
    for (const char **f = body_fields; *f; f++) {
        TSNode body = ts_node_child_by_field_name(class_node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(body)) {
            return body;
        }
    }
    // Go: type_spec -> type field (interface_type or struct_type)
    if (lang == CBM_LANG_GO) {
        TSNode type_inner = ts_node_child_by_field_name(class_node, "type", 4);
        if (!ts_node_is_null(type_inner)) {
            return type_inner;
        }
    }
    // ObjC: class_implementation/class_interface has no single body node
    // Methods are inside implementation_definition children directly
    if (lang == CBM_LANG_OBJC) {
        return class_node; // iterate children of the class node itself
    }
    // Fallback: search children for known body node types
    static const char *body_types[] = {"class_body",
                                       "interface_body",
                                       "enum_body",
                                       "template_body",
                                       "interface_type",
                                       "struct_type",
                                       "field_declaration_list",
                                       "compound_statement",
                                       "block",
                                       "closure",
                                       "implementation_definition",
                                       NULL};
    uint32_t count = ts_node_child_count(class_node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(class_node, i);
        const char *ck = ts_node_type(child);
        for (const char **t = body_types; *t; t++) {
            if (strcmp(ck, *t) == 0) {
                return child;
            }
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Helper: try to extract method name from a node, with fallbacks
static TSNode resolve_method_name(TSNode child, CBMLanguage lang) {
    TSNode name_node = func_name_node(child);
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }

    const char *ck = ts_node_type(child);

    // C/C++/CUDA: function_definition inside class body — use resolve_func_name which
    // handles the declarator chain including operator_name, destructor_name, etc.
    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL) &&
        strcmp(ck, "function_definition") == 0) {
        return resolve_func_name(child, lang, NULL);
    }

    // Groovy: function_definition uses field_function for the method name (not field_name).
    // e.g., "String greet()" → {field_type:String, field_function:greet, ...}
    // Using the first identifier would pick up the return type instead of the name.
    if (lang == CBM_LANG_GROOVY && strcmp(ck, "function_definition") == 0) {
        TSNode fn = ts_node_child_by_field_name(child, "function", 8);
        if (!ts_node_is_null(fn)) {
            return fn;
        }
        return cbm_find_child_by_kind(child, "identifier");
    }

    // Dart: method_signature > function_signature > name or identifier
    if (lang == CBM_LANG_DART) {
        if (strcmp(ck, "method_signature") == 0) {
            TSNode func_sig = cbm_find_child_by_kind(child, "function_signature");
            if (!ts_node_is_null(func_sig)) {
                name_node = func_name_node(func_sig);
                if (!ts_node_is_null(name_node)) {
                    return name_node;
                }
                return cbm_find_child_by_kind(func_sig, "identifier");
            }
        }
        if (strcmp(ck, "function_signature") == 0) {
            return cbm_find_child_by_kind(child, "identifier");
        }
    }

    // ObjC: method_definition has identifier child (no "name" field)
    if (lang == CBM_LANG_OBJC && strcmp(ck, "method_definition") == 0) {
        return cbm_find_child_by_kind(child, "identifier");
    }

    // Swift: function_declaration has simple_identifier child (no "name" field)
    if (lang == CBM_LANG_SWIFT && strcmp(ck, "function_declaration") == 0) {
        return cbm_find_child_by_kind(child, "simple_identifier");
    }

    // Arrow function: name on parent variable_declarator
    if (strcmp(ck, "arrow_function") == 0) {
        TSNode parent = ts_node_parent(child);
        if (!ts_node_is_null(parent)) {
            const char *pk = ts_node_type(parent);
            if (strcmp(pk, "field_definition") == 0) {
                return ts_node_child_by_field_name(parent, "property", 8);
            }
            if (strcmp(pk, "public_field_definition") == 0 ||
                strcmp(pk, "variable_declarator") == 0) {
                return ts_node_child_by_field_name(parent, "name", 4);
            }
        }
    }

    TSNode null_node = {0};
    return null_node;
}

// Push a single method definition
static void push_method_def(CBMExtractCtx *ctx, TSNode child, const char *class_qn,
                            const CBMLangSpec *spec, TSNode name_node) {
    CBMArena *a = ctx->arena;

    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return;
    }

    const char *method_qn = cbm_arena_sprintf(a, "%s.%s", class_qn, name);

    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = method_qn;
    def.label = "Method";
    def.file_path = ctx->rel_path;
    def.parent_class = class_qn;
    def.start_line = ts_node_start_point(child).row + 1;
    def.end_line = ts_node_end_point(child).row + 1;
    def.lines = (int)(def.end_line - def.start_line + 1);
    def.is_exported = cbm_is_exported(name, ctx->language);

    TSNode params = ts_node_child_by_field_name(child, "parameters", 10);
    if (!ts_node_is_null(params)) {
        def.signature = cbm_node_text(a, params, ctx->source);
        def.param_types = extract_param_types(a, params, ctx->source, ctx->language);
    }

    // Return type (same fields as extract_func_def)
    {
        static const char *rt_fields[] = {"result", "return_type", "type", NULL};
        for (const char **f = rt_fields; *f; f++) {
            TSNode rt = ts_node_child_by_field_name(child, *f, (uint32_t)strlen(*f));
            if (!ts_node_is_null(rt)) {
                def.return_type = cbm_node_text(a, rt, ctx->source);
                break;
            }
        }
    }

    // C++: trailing return type (auto method() -> Type)
    if (def.return_type && strcmp(def.return_type, "auto") == 0 &&
        (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA)) {
        TSNode declarator = ts_node_child_by_field_name(child, "declarator", 10);
        if (!ts_node_is_null(declarator)) {
            uint32_t nc = ts_node_named_child_count(declarator);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode ch = ts_node_named_child(declarator, i);
                if (strcmp(ts_node_type(ch), "trailing_return_type") == 0) {
                    TSNode type_desc =
                        ts_node_named_child_count(ch) > 0 ? ts_node_named_child(ch, 0) : ch;
                    def.return_type = cbm_node_text(a, type_desc, ctx->source);
                    if (def.return_type && def.return_type[0]) {
                        const char **rt =
                            (const char **)cbm_arena_alloc(a, 2 * sizeof(const char *));
                        if (rt) {
                            rt[0] = def.return_type;
                            rt[1] = NULL;
                            def.return_types = rt;
                        }
                    }
                    break;
                }
            }
        }
    }

    def.decorators = extract_decorators(a, child, ctx->source, ctx->language, spec);
    def.docstring = extract_docstring(a, child, ctx->source, ctx->language);

    if (spec->branching_node_types && spec->branching_node_types[0]) {
        def.complexity = cbm_count_branching(child, spec->branching_node_types);
    }

    cbm_defs_push(&ctx->result->defs, a, def);
}

// Extract methods inside a class body
static void extract_class_methods(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                  const CBMLangSpec *spec) {
    TSNode body = find_class_body(class_node, ctx->language);
    if (ts_node_is_null(body)) {
        return;
    }

    uint32_t count = ts_node_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(body, i);
        if (ts_node_is_null(child)) {
            continue;
        }

        // ObjC: class_implementation has implementation_definition children
        // which each contain a method_definition
        if (ctx->language == CBM_LANG_OBJC &&
            strcmp(ts_node_type(child), "implementation_definition") == 0) {
            uint32_t nc = ts_node_child_count(child);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode inner = ts_node_child(child, j);
                if (ts_node_is_null(inner)) {
                    continue;
                }
                if (cbm_kind_in_set(inner, spec->function_node_types)) {
                    TSNode nm = resolve_method_name(inner, ctx->language);
                    if (!ts_node_is_null(nm)) {
                        push_method_def(ctx, inner, class_qn, spec, nm);
                    }
                }
            }
            continue;
        }

        if (!cbm_kind_in_set(child, spec->function_node_types)) {
            continue;
        }

        TSNode name_node = resolve_method_name(child, ctx->language);
        if (ts_node_is_null(name_node)) {
            continue;
        }

        push_method_def(ctx, child, class_qn, spec, name_node);
    }
}

// --- Rust impl block extraction ---

static void extract_rust_impl(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;

    TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
    if (ts_node_is_null(type_node)) {
        return;
    }

    char *type_name = cbm_node_text(a, type_node, ctx->source);
    if (!type_name || !type_name[0]) {
        return;
    }

    // Check for "impl Trait for Struct" pattern
    TSNode trait_node = ts_node_child_by_field_name(node, "trait", 5);
    if (!ts_node_is_null(trait_node)) {
        char *trait_name = cbm_node_text(a, trait_node, ctx->source);
        if (trait_name && trait_name[0]) {
            CBMImplTrait it;
            it.trait_name = trait_name;
            it.struct_name = type_name;
            cbm_impltrait_push(&ctx->result->impl_traits, a, it);
        }
    }

    const char *type_qn = cbm_fqn_compute(a, ctx->project, ctx->rel_path, type_name);

    // Extract methods inside impl body
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (ts_node_is_null(body)) {
        return;
    }

    uint32_t count = ts_node_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(body, i);
        if (ts_node_is_null(child)) {
            continue;
        }
        if (!cbm_kind_in_set(child, spec->function_node_types)) {
            continue;
        }

        TSNode name_node = func_name_node(child);
        if (ts_node_is_null(name_node)) {
            continue;
        }

        char *name = cbm_node_text(a, name_node, ctx->source);
        if (!name || !name[0]) {
            continue;
        }

        const char *method_qn = cbm_arena_sprintf(a, "%s.%s", type_qn, name);

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = method_qn;
        def.label = "Method";
        def.file_path = ctx->rel_path;
        def.parent_class = type_qn;
        def.start_line = ts_node_start_point(child).row + 1;
        def.end_line = ts_node_end_point(child).row + 1;
        def.is_exported = cbm_is_exported(name, ctx->language);

        TSNode params = ts_node_child_by_field_name(child, "parameters", 10);
        if (!ts_node_is_null(params)) {
            def.signature = cbm_node_text(a, params, ctx->source);
            def.param_types = extract_param_types(a, params, ctx->source, ctx->language);
        }

        if (spec->branching_node_types && spec->branching_node_types[0]) {
            def.complexity = cbm_count_branching(child, spec->branching_node_types);
        }

        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

// --- Elixir def/defp/defmodule ---

// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk for nested Elixir modules
static void extract_elixir_call(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;

    // First child is the macro name (def, defp, defmodule, defmacro)
    if (ts_node_child_count(node) == 0) {
        return;
    }
    TSNode callee = ts_node_child(node, 0);
    if (ts_node_is_null(callee)) {
        return;
    }
    char *macro = cbm_node_text(a, callee, ctx->source);
    if (!macro) {
        return;
    }

    if (strcmp(macro, "def") == 0 || strcmp(macro, "defp") == 0 || strcmp(macro, "defmacro") == 0) {
        // Second child is arguments with the function name
        TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
        if (ts_node_is_null(args)) {
            if (ts_node_child_count(node) > 1) {
                args = ts_node_child(node, 1);
            }
        }
        if (ts_node_is_null(args)) {
            return;
        }

        // The function name is the first child of args (a call or identifier)
        TSNode first_arg = ts_node_child(args, 0);
        if (ts_node_is_null(first_arg)) {
            return;
        }

        const char *fk = ts_node_type(first_arg);
        char *name = NULL;
        if (strcmp(fk, "call") == 0 && ts_node_child_count(first_arg) > 0) {
            name = cbm_node_text(a, ts_node_child(first_arg, 0), ctx->source);
            // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        } else if (strcmp(fk, "identifier") == 0) {
            name = cbm_node_text(a, first_arg, ctx->source);
        }
        if (!name || !name[0]) {
            return;
        }

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        def.label = "Function";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + 1;
        def.end_line = ts_node_end_point(node).row + 1;
        // NOLINTNEXTLINE(readability-implicit-bool-conversion)
        def.is_exported = (strcmp(macro, "def") == 0 || strcmp(macro, "defmacro") == 0);
        cbm_defs_push(&ctx->result->defs, a, def);
    } else if (strcmp(macro, "defmodule") == 0) {
        TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
        if (ts_node_is_null(args) && ts_node_child_count(node) > 1) {
            args = ts_node_child(node, 1);
        }
        if (ts_node_is_null(args)) {
            return;
        }

        TSNode name_node = ts_node_child(args, 0);
        if (ts_node_is_null(name_node)) {
            return;
        }

        char *name = cbm_node_text(a, name_node, ctx->source);
        if (!name || !name[0]) {
            return;
        }

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
        def.label = "Class";
        def.file_path = ctx->rel_path;
        def.start_line = ts_node_start_point(node).row + 1;
        def.end_line = ts_node_end_point(node).row + 1;
        def.is_exported = true;
        cbm_defs_push(&ctx->result->defs, a, def);

        // Recurse into do_block for nested def/defp/defmacro
        TSNode do_block = cbm_find_child_by_kind(node, "do_block");
        if (!ts_node_is_null(do_block)) {
            uint32_t dbc = ts_node_child_count(do_block);
            for (uint32_t di = 0; di < dbc; di++) {
                TSNode dchild = ts_node_child(do_block, di);
                if (!ts_node_is_null(dchild) && strcmp(ts_node_type(dchild), "call") == 0) {
                    extract_elixir_call(ctx, dchild, spec);
                }
            }
        }
    }
}

// --- Variable extraction ---

// Helper to push a Variable definition
static void push_var_def(CBMExtractCtx *ctx, const char *name, TSNode node) {
    if (!name || !name[0] || strcmp(name, "_") == 0) {
        return;
    }
    CBMArena *a = ctx->arena;
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = "Variable";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + 1;
    def.end_line = ts_node_end_point(node).row + 1;
    def.is_exported = cbm_is_exported(name, ctx->language);
    cbm_defs_push(&ctx->result->defs, a, def);
}

// Helper: extract name from a declarator chain (C/C++/ObjC)
// declaration > init_declarator > declarator (may be pointer_declarator > identifier)
static const char *extract_c_declarator_name(CBMArena *a, TSNode decl, const char *source) {
    // Try "declarator" field on the declaration
    TSNode declarator = ts_node_child_by_field_name(decl, "declarator", 10);
    if (ts_node_is_null(declarator)) {
        return NULL;
    }

    // Could be init_declarator wrapping the actual declarator
    const char *dk = ts_node_type(declarator);
    if (strcmp(dk, "init_declarator") == 0) {
        declarator = ts_node_child_by_field_name(declarator, "declarator", 10);
        if (ts_node_is_null(declarator)) {
            return NULL;
        }
        dk = ts_node_type(declarator);
    }
    // Unwrap pointer_declarator
    while (strcmp(dk, "pointer_declarator") == 0 || strcmp(dk, "reference_declarator") == 0) {
        declarator = ts_node_child_by_field_name(declarator, "declarator", 10);
        if (ts_node_is_null(declarator)) {
            return NULL;
        }
        dk = ts_node_type(declarator);
    }
    if (strcmp(dk, "identifier") == 0) {
        return cbm_node_text(a, declarator, source);
    }
    return NULL;
}

// Helper: extract name from Java/C# field_declaration (declarator > name)
static const char *extract_java_field_name(CBMArena *a, TSNode field, const char *source) {
    TSNode declarator = ts_node_child_by_field_name(field, "declarator", 10);
    if (ts_node_is_null(declarator)) {
        // Try iterating children for variable_declarator
        uint32_t n = ts_node_named_child_count(field);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(field, i);
            if (strcmp(ts_node_type(child), "variable_declarator") == 0) {
                declarator = child;
                break;
            }
        }
    }
    if (ts_node_is_null(declarator)) {
        return NULL;
    }
    TSNode name = ts_node_child_by_field_name(declarator, "name", 4);
    if (!ts_node_is_null(name)) {
        return cbm_node_text(a, name, source);
    }
    return NULL;
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity,readability-function-size)
static void extract_var_names(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    (void)spec;
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);

    switch (ctx->language) {
    case CBM_LANG_PYTHON: {
        // assignment/augmented_assignment: left = right
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        if (!ts_node_is_null(left) && strcmp(ts_node_type(left), "identifier") == 0) {
            push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_GO: {
        // var_declaration -> var_spec* -> name
        // const_declaration -> const_spec* -> name
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "var_spec") == 0 || strcmp(ck, "const_spec") == 0) {
                TSNode vname = ts_node_child_by_field_name(child, "name", 4);
                if (!ts_node_is_null(vname)) {
                    push_var_def(ctx, cbm_node_text(a, vname, ctx->source), child);
                }
            }
        }
        break;
    }
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX: {
        // lexical_declaration/variable_declaration -> variable_declarator -> name
        // Skip if value is arrow_function or function_expression (extracted as Function)
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (strcmp(ts_node_type(child), "variable_declarator") != 0) {
                continue;
            }
            // Skip function-valued variables
            TSNode value = ts_node_child_by_field_name(child, "value", 5);
            if (!ts_node_is_null(value)) {
                const char *vk = ts_node_type(value);
                if (strcmp(vk, "arrow_function") == 0 || strcmp(vk, "function_expression") == 0 ||
                    strcmp(vk, "generator_function") == 0) {
                    continue;
                }
            }
            TSNode vname = ts_node_child_by_field_name(child, "name", 4);
            if (!ts_node_is_null(vname)) {
                push_var_def(ctx, cbm_node_text(a, vname, ctx->source), child);
            }
        }
        break;
    }
    case CBM_LANG_JAVA: {
        // field_declaration -> variable_declarator -> name
        const char *fname = extract_java_field_name(a, node, ctx->source);
        if (fname) {
            push_var_def(ctx, fname, node);
        }
        break;
    }
    case CBM_LANG_CSHARP: {
        // field_declaration -> variable_declaration -> variable_declarator -> identifier
        const char *fname = extract_java_field_name(a, node, ctx->source);
        if (fname) {
            push_var_def(ctx, fname, node);
        } else {
            // C# local_declaration_statement > variable_declaration > variable_declarator
            uint32_t n = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < n; i++) {
                TSNode child = ts_node_named_child(node, i);
                if (strcmp(ts_node_type(child), "variable_declaration") == 0) {
                    uint32_t nc = ts_node_named_child_count(child);
                    for (uint32_t j = 0; j < nc; j++) {
                        TSNode decl = ts_node_named_child(child, j);
                        if (strcmp(ts_node_type(decl), "variable_declarator") == 0) {
                            TSNode id = ts_node_child_by_field_name(decl, "name", 4);
                            if (ts_node_is_null(id)) {
                                id = cbm_find_child_by_kind(decl, "identifier");
                            }
                            if (!ts_node_is_null(id)) {
                                push_var_def(ctx, cbm_node_text(a, id, ctx->source), decl);
                            }
                        }
                    }
                }
            }
        }
        break;
    }
    case CBM_LANG_CPP:
    case CBM_LANG_C:
    case CBM_LANG_OBJC: {
        // declaration > init_declarator > declarator chain > identifier
        const char *vname = extract_c_declarator_name(a, node, ctx->source);
        if (vname) {
            push_var_def(ctx, vname, node);
        }
        break;
    }
    case CBM_LANG_PHP: {
        // expression_statement > assignment_expression > left (variable_name)
        if (strcmp(kind, "expression_statement") == 0) {
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode inner = ts_node_named_child(node, j);
                if (strcmp(ts_node_type(inner), "assignment_expression") == 0) {
                    TSNode left = ts_node_child_by_field_name(inner, "left", 4);
                    if (!ts_node_is_null(left)) {
                        char *name = cbm_node_text(a, left, ctx->source);
                        // Strip $ prefix for PHP variables
                        if (name && name[0] == '$') {
                            name++;
                        }
                        push_var_def(ctx, name, node);
                    }
                }
            }
        }
        break;
    }
    case CBM_LANG_LUA: {
        // variable_declaration > assignment_statement > variable_list > first var
        // Skip if value is a function_definition (extracted as Function instead)
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "assignment_statement") == 0) {
                // Check if the value is a function_definition
                TSNode expr_list = cbm_find_child_by_kind(child, "expression_list");
                if (!ts_node_is_null(expr_list) && ts_node_named_child_count(expr_list) > 0) {
                    TSNode val = ts_node_named_child(expr_list, 0);
                    if (!ts_node_is_null(val) &&
                        strcmp(ts_node_type(val), "function_definition") == 0) {
                        continue; // Skip: will be extracted as Function
                    }
                }
                TSNode vars = ts_node_child_by_field_name(child, "variables", 9);
                if (ts_node_is_null(vars)) {
                    vars = cbm_find_child_by_kind(child, "variable_list");
                }
                if (!ts_node_is_null(vars) && ts_node_named_child_count(vars) > 0) {
                    TSNode first = ts_node_named_child(vars, 0);
                    if (!ts_node_is_null(first)) {
                        push_var_def(ctx, cbm_node_text(a, first, ctx->source), node);
                    }
                }
            }
        }
        break;
    }
    case CBM_LANG_SCALA: {
        // val_definition/var_definition: "pattern" field contains the name
        TSNode pattern = ts_node_child_by_field_name(node, "pattern", 7);
        if (!ts_node_is_null(pattern)) {
            push_var_def(ctx, cbm_node_text(a, pattern, ctx->source), node);
        } else {
            TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
            if (!ts_node_is_null(name_node)) {
                push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
            }
        }
        break;
    }
    case CBM_LANG_KOTLIN: {
        // property_declaration > variable_declaration > identifier (or simple_identifier)
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (ts_node_is_null(name_node)) {
            name_node = cbm_find_child_by_kind(node, "simple_identifier");
        }
        if (ts_node_is_null(name_node)) {
            name_node = cbm_find_child_by_kind(node, "identifier");
        }
        if (ts_node_is_null(name_node)) {
            // Look deeper: variable_declaration > identifier/simple_identifier
            TSNode var_decl = cbm_find_child_by_kind(node, "variable_declaration");
            if (!ts_node_is_null(var_decl)) {
                name_node = cbm_find_child_by_kind(var_decl, "simple_identifier");
                if (ts_node_is_null(name_node)) {
                    name_node = cbm_find_child_by_kind(var_decl, "identifier");
                }
            }
        }
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_RUBY: {
        // assignment: left field
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        if (!ts_node_is_null(left)) {
            const char *lk = ts_node_type(left);
            if (strcmp(lk, "identifier") == 0 || strcmp(lk, "constant") == 0) {
                push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
            }
        }
        break;
    }
    case CBM_LANG_R: {
        // binary_operator: try "left" then "lhs" field, then first named child
        // Skip if value is function_definition (extracted as Function instead)
        uint32_t rnc = ts_node_named_child_count(node);
        for (uint32_t ri = 0; ri < rnc; ri++) {
            TSNode rch = ts_node_named_child(node, ri);
            if (!ts_node_is_null(rch) && strcmp(ts_node_type(rch), "function_definition") == 0) {
                goto r_skip; // will be extracted as Function
            }
        }
        {
            TSNode left = ts_node_child_by_field_name(node, "left", 4);
            if (ts_node_is_null(left)) {
                left = ts_node_child_by_field_name(node, "lhs", 3);
            }
            if (ts_node_is_null(left) && ts_node_named_child_count(node) > 0) {
                left = ts_node_named_child(node, 0);
            }
            if (!ts_node_is_null(left)) {
                const char *lk = ts_node_type(left);
                if (strcmp(lk, "identifier") == 0 || strcmp(lk, "constant") == 0 ||
                    strcmp(lk, "string") == 0) {
                    push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
                }
            }
        }
    r_skip:
        break;
    }
    case CBM_LANG_RUST: {
        // static_item/const_item: name field
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_PERL: {
        // Perl: variable_declaration > scalar_variable | assignment_expression > left
        // Also: expression_statement > assignment_expression > variable_declaration >
        // scalar_variable
        uint32_t n = ts_node_named_child_count(node);
        bool found = false;
        for (uint32_t i = 0; i < n && !found; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "scalar_variable") == 0 || strcmp(ck, "array_variable") == 0 ||
                strcmp(ck, "hash_variable") == 0 || strcmp(ck, "variable_declarator") == 0 ||
                strcmp(ck, "scalar") == 0 || strcmp(ck, "array") == 0 || strcmp(ck, "hash") == 0) {
                char *name = cbm_node_text(a, child, ctx->source);
                if (name && name[0] == '$') {
                    name++;
                }
                if (name && name[0] == '@') {
                    name++;
                }
                if (name && name[0] == '%') {
                    name++;
                }
                push_var_def(ctx, name, node);
                found = true;
            } else if (strcmp(ck, "assignment_expression") == 0) {
                // LHS may be variable_declaration (my $x = ...) — drill into it
                TSNode left = ts_node_child_by_field_name(child, "left", 4);
                if (ts_node_is_null(left) && ts_node_named_child_count(child) > 0) {
                    left = ts_node_named_child(child, 0);
                }
                if (!ts_node_is_null(left)) {
                    // If left is variable_declaration (my $x), drill into it
                    const char *lk = ts_node_type(left);
                    if (strcmp(lk, "variable_declaration") == 0) {
                        uint32_t lnc = ts_node_named_child_count(left);
                        for (uint32_t li = 0; li < lnc; li++) {
                            TSNode var_node = ts_node_named_child(left, li);
                            const char *vk = ts_node_type(var_node);
                            if (strcmp(vk, "scalar") == 0 || strcmp(vk, "array") == 0 ||
                                strcmp(vk, "hash") == 0 || strcmp(vk, "scalar_variable") == 0 ||
                                strcmp(vk, "array_variable") == 0 ||
                                strcmp(vk, "hash_variable") == 0) {
                                left = var_node;
                                break;
                            }
                        }
                    }
                    char *name = cbm_node_text(a, left, ctx->source);
                    if (name && name[0] == '$') {
                        name++;
                    }
                    if (name && name[0] == '@') {
                        name++;
                    }
                    if (name && name[0] == '%') {
                        name++;
                    }
                    push_var_def(ctx, name, node);
                    found = true;
                }
            }
        }
        break;
    }
    case CBM_LANG_YAML: {
        // block_mapping_pair: key field
        TSNode key = ts_node_child_by_field_name(node, "key", 3);
        if (!ts_node_is_null(key)) {
            push_var_def(ctx, cbm_node_text(a, key, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_TOML: {
        // pair: first child is bare_key/dotted_key/quoted_key
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "bare_key") == 0 || strcmp(ck, "dotted_key") == 0 ||
                strcmp(ck, "quoted_key") == 0 || strcmp(ck, "key") == 0) {
                push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
                break;
            }
        }
        break;
    }
    case CBM_LANG_JSON: {
        // pair: "key" field is a string node, extract unquoted text
        TSNode key_node = ts_node_child_by_field_name(node, "key", 3);
        if (!ts_node_is_null(key_node)) {
            char *raw = cbm_node_text(a, key_node, ctx->source);
            if (raw) {
                // Strip surrounding quotes if present
                size_t rlen = strlen(raw);
                if (rlen >= 2 && raw[0] == '"' && raw[rlen - 1] == '"') {
                    raw[rlen - 1] = '\0';
                    raw++;
                }
                push_var_def(ctx, raw, node);
            }
        }
        break;
    }
    case CBM_LANG_INI: {
        // setting: first non-whitespace child before "=" is the key name
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "setting_name") == 0 || strcmp(ck, "name") == 0) {
                char *name = cbm_node_text(a, child, ctx->source);
                // Trim whitespace
                if (name) {
                    while (*name == ' ' || *name == '\t') {
                        name++;
                    }
                    size_t nlen = strlen(name);
                    while (nlen > 0 && (name[nlen - 1] == ' ' || name[nlen - 1] == '\t')) {
                        name[nlen - 1] = '\0';
                        nlen--;
                    }
                }
                push_var_def(ctx, name, node);
                break;
            }
        }
        // Fallback: try first child text (some INI grammars use different structure)
        if (nc > 0) {
            TSNode first = ts_node_child(node, 0);
            const char *fk = ts_node_type(first);
            if (strcmp(fk, "setting_name") != 0 && strcmp(fk, "name") != 0) {
                // Check if we already pushed (avoid duplicate)
                // Only try if no setting_name was found above
                bool found_name = false;
                for (uint32_t i = 0; i < nc; i++) {
                    const char *ck = ts_node_type(ts_node_child(node, i));
                    if (strcmp(ck, "setting_name") == 0 || strcmp(ck, "name") == 0) {
                        found_name = true;
                        break;
                    }
                }
                if (!found_name) {
                    char *name = cbm_node_text(a, first, ctx->source);
                    if (name) {
                        while (*name == ' ' || *name == '\t') {
                            name++;
                        }
                        size_t nlen = strlen(name);
                        while (nlen > 0 && (name[nlen - 1] == ' ' || name[nlen - 1] == '\t')) {
                            name[nlen - 1] = '\0';
                            nlen--;
                        }
                    }
                    push_var_def(ctx, name, node);
                }
            }
        }
        break;
    }
    case CBM_LANG_ERLANG: {
        // pp_define / record_decl: name from atom/var/macro_lhs child
        if (strcmp(kind, "pp_define") == 0 || strcmp(kind, "record_decl") == 0) {
            uint32_t n = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < n; i++) {
                TSNode child = ts_node_named_child(node, i);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "atom") == 0 || strcmp(ck, "var") == 0 ||
                    strcmp(ck, "macro_lhs") == 0) {
                    push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
                    break;
                }
            }
        }
        break;
    }
    case CBM_LANG_SQL: {
        // create_table/create_view: find identifier/object_reference
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "identifier") == 0 || strcmp(ck, "object_reference") == 0) {
                push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
                break;
            }
        }
        break;
    }
    case CBM_LANG_BASH: {
        // variable_assignment: name = value
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        } else {
            // Fallback: first word before =
            uint32_t n = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < n; i++) {
                TSNode child = ts_node_named_child(node, i);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "variable_name") == 0 || strcmp(ck, "word") == 0) {
                    push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
                    break;
                }
            }
        }
        break;
    }
    case CBM_LANG_SCSS: {
        // declaration: property_name child (SCSS variable like $primary-color: value)
        TSNode prop = ts_node_child_by_field_name(node, "property", 8);
        if (ts_node_is_null(prop)) {
            prop = ts_node_child_by_field_name(node, "name", 4);
        }
        if (ts_node_is_null(prop)) {
            prop = cbm_find_child_by_kind(node, "property_name");
        }
        if (ts_node_is_null(prop)) {
            prop = cbm_find_child_by_kind(node, "variable_name");
        }
        if (!ts_node_is_null(prop)) {
            push_var_def(ctx, cbm_node_text(a, prop, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_GROOVY: {
        // declaration: name field or identifier child
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (ts_node_is_null(name_node)) {
            // Try declarator chain (like C)
            const char *cname = extract_c_declarator_name(a, node, ctx->source);
            if (cname) {
                push_var_def(ctx, cname, node);
                break;
            }
            // Try first identifier
            name_node = cbm_find_child_by_kind(node, "identifier");
        }
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    default: {
        // Try "name" field first, then C-style declarator chain, then first identifier
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
            break;
        }
        // Try C-style declarator chain
        const char *cname = extract_c_declarator_name(a, node, ctx->source);
        if (cname) {
            push_var_def(ctx, cname, node);
            break;
        }
        // Generic: try first identifier child
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (strcmp(ts_node_type(child), "identifier") == 0) {
                push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
                break;
            }
        }
        break;
    }
    }
}

// Recursive variable walker for languages with deeply nested module structure.
// Used by YAML, TOML, INI, JSON (config languages with nested containers).
// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk for nested config structures
static void walk_variables_rec(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                               int depth) {
    if (depth > 8) {
        return; // safety limit
    }
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (ts_node_is_null(child)) {
            continue;
        }
        if (cbm_kind_in_set(child, spec->variable_node_types)) {
            if (cbm_is_module_level(child, ctx->language)) {
                extract_var_names(ctx, child, spec);
            }
        }
        // Always recurse into structural container nodes
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "document") == 0 || strcmp(ck, "block_node") == 0 ||
            strcmp(ck, "block_mapping") == 0 || strcmp(ck, "stream") == 0 ||
            // TOML containers
            strcmp(ck, "table") == 0 || strcmp(ck, "table_array_element") == 0 ||
            // INI containers
            strcmp(ck, "section") == 0 ||
            // JSON/TOML containers (pair can contain nested objects)
            strcmp(ck, "object") == 0 || strcmp(ck, "array") == 0 || strcmp(ck, "pair") == 0 ||
            // XML containers
            strcmp(ck, "element") == 0 || strcmp(ck, "content") == 0) {
            walk_variables_rec(ctx, child, spec, depth + 1);
        }
    }
}

static void extract_variables(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    if (!spec->variable_node_types || !spec->variable_node_types[0]) {
        return;
    }

    // Config languages with nested structure: use recursive walk
    if (ctx->language == CBM_LANG_YAML || ctx->language == CBM_LANG_TOML ||
        ctx->language == CBM_LANG_INI || ctx->language == CBM_LANG_JSON) {
        walk_variables_rec(ctx, root, spec, 0);
        return;
    }

    uint32_t count = ts_node_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(root, i);
        if (ts_node_is_null(child)) {
            continue;
        }

        if (!cbm_is_module_level(child, ctx->language)) {
            continue;
        }

        if (cbm_kind_in_set(child, spec->variable_node_types)) {
            extract_var_names(ctx, child, spec);
            continue;
        }

        // Unwrap wrapper nodes: expression_statement, export_statement, statement
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "expression_statement") == 0 || strcmp(ck, "export_statement") == 0 ||
            strcmp(ck, "statement") == 0) {
            // Check inner named children for variable types
            uint32_t nc = ts_node_named_child_count(child);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode inner = ts_node_named_child(child, j);
                if (cbm_kind_in_set(inner, spec->variable_node_types)) {
                    extract_var_names(ctx, inner, spec);
                }
            }
            // Also check if the wrapper itself is a variable type (e.g., PHP expression_statement)
            if (cbm_kind_in_set(child, spec->variable_node_types)) {
                extract_var_names(ctx, child, spec);
            }
        }
    }
}

// Extract typed struct/class fields for cross-file LSP resolution (C/C++/CUDA/Go/Java/Rust etc.)
// Creates "Field" label definitions with return_type set to the field's type text.
// These are later collected by DefsToLSPDefs to build FieldDefs pipe-separated strings.
static void extract_class_fields(CBMExtractCtx *ctx, TSNode class_node, const char *class_qn,
                                 const CBMLangSpec *spec) {
    if (!spec->field_node_types || !spec->field_node_types[0]) {
        return;
    }

    TSNode body = find_class_body(class_node, ctx->language);
    if (ts_node_is_null(body)) {
        return;
    }

    CBMArena *a = ctx->arena;
    uint32_t count = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(body, i);
        if (!cbm_kind_in_set(child, spec->field_node_types)) {
            continue;
        }

        // Skip function-pointer fields: walk the declarator chain — if any level
        // is function_declarator, this is a function pointer, not a data field.
        {
            TSNode decl = ts_node_child_by_field_name(child, "declarator", 10);
            bool is_func = false;
            for (int depth = 0; depth < 5 && !ts_node_is_null(decl); depth++) {
                const char *dk = ts_node_type(decl);
                if (strcmp(dk, "function_declarator") == 0) {
                    is_func = true;
                    break;
                }
                // Descend through parenthesized_declarator, pointer_declarator, etc.
                TSNode inner = ts_node_child_by_field_name(decl, "declarator", 10);
                if (ts_node_is_null(inner)) {
                    uint32_t nc = ts_node_named_child_count(decl);
                    for (uint32_t k = 0; k < nc; k++) {
                        inner = ts_node_named_child(decl, k);
                        if (!ts_node_is_null(inner)) {
                            break;
                        }
                    }
                }
                decl = inner;
            }
            if (is_func) {
                continue;
            }
        }

        // Extract type from "type" field
        TSNode type_node = ts_node_child_by_field_name(child, "type", 4);
        if (ts_node_is_null(type_node)) {
            continue;
        }
        char *type_text = cbm_node_text(a, type_node, ctx->source);
        if (!type_text || !type_text[0]) {
            continue;
        }

        // Extract field name from "declarator" field (C/C++/CUDA/GLSL)
        // or "name" field (Go, Java, Rust)
        TSNode name_node = ts_node_child_by_field_name(child, "declarator", 10);
        if (ts_node_is_null(name_node)) {
            name_node = ts_node_child_by_field_name(child, "name", 4);
        }
        if (ts_node_is_null(name_node)) {
            continue;
        }

        // For C/C++: declarator might be a pointer_declarator or array_declarator wrapping
        // field_identifier
        const char *nk = ts_node_type(name_node);
        if (strcmp(nk, "pointer_declarator") == 0 || strcmp(nk, "array_declarator") == 0) {
            // Prepend pointer/array to type, get inner identifier
            char *decl_text = cbm_node_text(a, name_node, ctx->source);
            TSNode inner = ts_node_child_by_field_name(name_node, "declarator", 10);
            if (!ts_node_is_null(inner)) {
                name_node = inner;
            } else {
                (void)decl_text;
                continue;
            }
        }

        char *name = cbm_node_text(a, name_node, ctx->source);
        if (!name || !name[0]) {
            continue;
        }

        const char *field_qn = cbm_arena_sprintf(a, "%s.%s", class_qn, name);

        CBMDefinition def;
        memset(&def, 0, sizeof(def));
        def.name = name;
        def.qualified_name = field_qn;
        def.label = "Field";
        def.file_path = ctx->rel_path;
        def.parent_class = class_qn;
        def.return_type = type_text;
        def.start_line = ts_node_start_point(child).row + 1;
        def.end_line = ts_node_end_point(child).row + 1;
        def.is_exported = cbm_is_exported(name, ctx->language);

        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

// Extract class-level variables (field declarations inside class bodies)
static void extract_class_variables(CBMExtractCtx *ctx, TSNode class_node,
                                    const CBMLangSpec *spec) {
    if (!spec->variable_node_types || !spec->variable_node_types[0]) {
        return;
    }

    TSNode body = find_class_body(class_node, ctx->language);
    if (ts_node_is_null(body)) {
        return;
    }

    uint32_t count = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(body, i);
        if (cbm_kind_in_set(child, spec->variable_node_types)) {
            extract_var_names(ctx, child, spec);
        }
    }
}

// --- Module node + main walk ---

// Recursive definition walker using tree-sitter cursor for cache-friendliness
// Search for nested class definitions inside a class body, recursing into wrapper
// nodes (field_declaration, template_declaration) but not into function bodies.
// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk for nested class hierarchies
static void find_nested_classes(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        if (cbm_kind_in_set(child, spec->class_node_types)) {
            // Found a nested class — process it via normal walk_defs
            walk_defs(ctx, child, spec, 0);
        } else {
            const char *ck = ts_node_type(child);
            // Recurse into wrapper nodes that might contain nested classes
            // but NOT into function bodies (function_definition, lambda_expression)
            if (strcmp(ck, "field_declaration") == 0 || strcmp(ck, "template_declaration") == 0 ||
                strcmp(ck, "declaration") == 0) {
                find_nested_classes(ctx, child, spec);
            }
        }
    }
}

/* Max AST recursion depth — prevents stack overflow on pathological files.
 * 256 levels is far beyond any real code nesting; 896+ was observed on
 * generated Linux kernel headers with deeply nested preprocessor output. */
#define CBM_WALK_DEFS_MAX_DEPTH 256

// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk
static void walk_defs(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, int depth) {
    if (depth >= CBM_WALK_DEFS_MAX_DEPTH) {
        return;
    }

    const char *kind = ts_node_type(node);

    // Elixir: all defs are call nodes
    if (ctx->language == CBM_LANG_ELIXIR && strcmp(kind, "call") == 0) {
        extract_elixir_call(ctx, node, spec);
        return;
    }

    // Function types
    if (cbm_kind_in_set(node, spec->function_node_types)) {
        // C++/CUDA: template_declaration may wrap a class, not a function.
        // Check if it contains a class_specifier/struct_specifier — if so, skip
        // function extraction and fall through to class handling.
        bool is_template_class = false;
        if ((ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA) &&
            strcmp(kind, "template_declaration") == 0) {
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                const char *ck = ts_node_type(ts_node_named_child(node, i));
                if (strcmp(ck, "class_specifier") == 0 || strcmp(ck, "struct_specifier") == 0 ||
                    strcmp(ck, "union_specifier") == 0) {
                    is_template_class = true;
                    break;
                }
            }
        }
        if (!is_template_class) {
            extract_func_def(ctx, node, spec);
            // Wolfram: continue recursing — nested set_delayed inside Module/Block are valid defs
            if (ctx->language != CBM_LANG_WOLFRAM) {
                return; // don't recurse into function bodies for nested defs
            }
        }
        // For template classes: fall through to class check or default recursion
    }

    // Rust impl blocks
    if (ctx->language == CBM_LANG_RUST && strcmp(kind, "impl_item") == 0) {
        extract_rust_impl(ctx, node, spec);
        return;
    }

    // Class types
    if (cbm_kind_in_set(node, spec->class_node_types)) {
        const char *saved_enclosing = ctx->enclosing_class_qn;
        extract_class_def(ctx, node, spec);
        // extract_class_def computed class_qn; replicate to set enclosing for nested classes
        {
            TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
            if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_OBJC) {
                name_node = cbm_find_child_by_kind(node, "identifier");
            }
            if (ts_node_is_null(name_node) && ctx->language == CBM_LANG_SWIFT) {
                name_node = cbm_find_child_by_kind(node, "type_identifier");
            }
            if (!ts_node_is_null(name_node)) {
                char *cname = cbm_node_text(ctx->arena, name_node, ctx->source);
                if (cname && cname[0]) {
                    if (saved_enclosing) {
                        ctx->enclosing_class_qn =
                            cbm_arena_sprintf(ctx->arena, "%s.%s", saved_enclosing, cname);
                    } else {
                        ctx->enclosing_class_qn =
                            cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, cname);
                    }
                }
            }
        }
        // Recurse into class body for nested class definitions (C++, Java, C#, Python, etc.)
        // For languages with body containers, use targeted nested class search.
        // For config languages (XML, JSON, etc.) with no body container, generic child walk.
        bool found_body = false;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t ci = 0; ci < nc; ci++) {
            TSNode child = ts_node_child(node, ci);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "field_declaration_list") == 0 || strcmp(ck, "class_body") == 0 ||
                strcmp(ck, "declaration_list") == 0 || strcmp(ck, "body") == 0 ||
                strcmp(ck, "block") == 0 || strcmp(ck, "suite") == 0) {
                find_nested_classes(ctx, child, spec);
                found_body = true;
                break;
            }
        }
        if (!found_body) {
            // Config languages (XML, TOML, JSON, etc.) — children are direct class nodes
            for (uint32_t ci = 0; ci < nc; ci++) {
                walk_defs(ctx, ts_node_child(node, ci), spec, depth + 1);
            }
        }
        ctx->enclosing_class_qn = saved_enclosing;
        return;
    }

    // Recurse into children
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        walk_defs(ctx, ts_node_child(node, i), spec, depth + 1);
    }
}

void cbm_extract_definitions(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec) {
        return;
    }

    CBMArena *a = ctx->arena;

    // Create module node (always first definition)
    CBMDefinition mod;
    memset(&mod, 0, sizeof(mod));
    mod.name = ctx->rel_path; // will be refined by Go layer
    mod.qualified_name = ctx->module_qn;
    mod.label = "Module";
    mod.file_path = ctx->rel_path;
    mod.start_line = 1;
    mod.end_line = ts_node_end_point(ctx->root).row + 1;
    mod.is_exported = true;
    mod.is_test = ctx->result->is_test_file;
    cbm_defs_push(&ctx->result->defs, a, mod);

    // Walk AST for function/class definitions
    walk_defs(ctx, ctx->root, spec, 0);

    // Extract module-level variables
    extract_variables(ctx, ctx->root, spec);
}
