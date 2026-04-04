#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_alloc/strdup/sprintf
#include "helpers.h"
#include "lang_specs.h"
#include "foundation/constants.h"
#include "simhash/minhash.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <stdint.h>          // uint32_t
#include <string.h>
#include <ctype.h>

// Buffer sizes for local arrays (base classes, params, return types).
#define MAX_COMMENT_LEN 500
#define MAX_BASES 16
#define MAX_BASES_MINUS_1 15
#define MAX_PARAMS CBM_SZ_32
#define MAX_PARAMS_MINUS_1 31
#define MAX_RETURN_TYPES 16
#define MAX_RETURN_TYPES_MINUS_1 15

// Tree traversal limits.
enum {
    TEMPLATE_DEPTH_LIMIT = 4,
    DECLARATOR_DEPTH_LIMIT = 8,
    EXPORT_ANCESTOR_DEPTH = 4,
    DECORATOR_SCAN_LIMIT = 3,
    C_RETURN_WALK_DEPTH = 5,
    VAR_RECURSION_LIMIT = 8,
    NESTED_CLASS_STACK_CAP = 128,
};

/* Compute MinHash fingerprint for a function body node and store in def.
 * Sets def->fingerprint (arena-allocated) and def->fingerprint_k on success,
 * leaves them NULL/0 if the body is too short. */
static void compute_fingerprint(CBMExtractCtx *ctx, CBMDefinition *def, TSNode func_node) {
    /* Find the function body child */
    TSNode body = ts_node_child_by_field_name(func_node, TS_FIELD("body"));
    if (ts_node_is_null(body)) {
        body = func_node;
    }
    cbm_minhash_t result;
    if (!cbm_minhash_compute(body, ctx->source, (int)ctx->language, &result)) {
        return; /* Too short or empty — no fingerprint */
    }
    /* Arena-allocate the fingerprint array */
    uint32_t *fp = cbm_arena_alloc(ctx->arena, CBM_MINHASH_K * sizeof(uint32_t));
    if (!fp) {
        return;
    }
    memcpy(fp, result.values, CBM_MINHASH_K * sizeof(uint32_t));
    def->fingerprint = fp;
    def->fingerprint_k = CBM_MINHASH_K;
}

// Tree-sitter row is 0-based; lines are 1-based.

// Null-terminated array allocation: need count + 1 for terminator.
enum { NULL_TERM = 1 };

// String operations.
enum {
    SKIP_CHAR = 1,        // skip one character (dot, quote, prefix)
    PAIR_CHARS = 2,       // pair of delimiters (quotes, parens)
    SECOND_CHILD_IDX = 1, // index of second child
    FIRST_LINE = 1,       // first line number
};

// Return type pair array size.
enum { RT_PAIR_SIZE = 2 };

// Forward declarations
static void extract_func_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void extract_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec);
static void walk_defs(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec, int depth_unused);
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
    return ts_node_child_by_field_name(node, TS_FIELD("name"));
}

// Lua: resolve anonymous function assignment name from parent assignment_statement.
static TSNode resolve_lua_func_name(TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "expression_list") == 0) {
        parent = ts_node_parent(parent);
    }
    if (ts_node_is_null(parent) || strcmp(ts_node_type(parent), "assignment_statement") != 0) {
        TSNode null_node = {0};
        return null_node;
    }
    TSNode vars = ts_node_child_by_field_name(parent, TS_FIELD("variables"));
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
    TSNode null_node = {0};
    return null_node;
}

// Julia: walk named children to find first identifier.
static TSNode resolve_julia_func_name(TSNode node) {
    TSNode current = node;
    for (int depth = 0; depth < TEMPLATE_DEPTH_LIMIT; depth++) {
        if (ts_node_named_child_count(current) == 0) {
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

// OCaml: resolve value_definition name from let_binding→pattern.
static TSNode resolve_ocaml_func_name(TSNode node) {
    TSNode binding = cbm_find_child_by_kind(node, "let_binding");
    if (!ts_node_is_null(binding)) {
        TSNode pattern = ts_node_child_by_field_name(binding, TS_FIELD("pattern"));
        if (!ts_node_is_null(pattern)) {
            return pattern;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// SQL: resolve create_function name from object_reference→identifier or direct identifier.
static TSNode resolve_sql_func_name(TSNode node) {
    TSNode obj_ref = cbm_find_child_by_kind(node, "object_reference");
    if (!ts_node_is_null(obj_ref)) {
        TSNode id = cbm_find_child_by_kind(obj_ref, "identifier");
        if (!ts_node_is_null(id)) {
            return id;
        }
    }
    return cbm_find_child_by_kind(node, "identifier");
}

// Zig: resolve test_declaration name from string→string_content.
static TSNode resolve_zig_test_name(TSNode node) {
    TSNode str_node = cbm_find_child_by_kind(node, "string");
    if (!ts_node_is_null(str_node)) {
        TSNode content = cbm_find_child_by_kind(str_node, "string_content");
        if (!ts_node_is_null(content)) {
            return content;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// VimScript: resolve function_definition name from function_declaration child.
static TSNode resolve_vimscript_func_name(TSNode node) {
    TSNode decl = cbm_find_child_by_kind(node, "function_declaration");
    if (!ts_node_is_null(decl) && ts_node_named_child_count(decl) > 0) {
        return ts_node_named_child(decl, 0);
    }
    if (ts_node_named_child_count(node) > 0) {
        return ts_node_named_child(node, 0);
    }
    TSNode null_node = {0};
    return null_node;
}

// Resolve function name for scripting/niche languages (Lua, OCaml, SQL, Zig, VimScript, Julia).
static TSNode resolve_func_name_scripting(TSNode node, CBMLanguage lang, const char *kind) {
    if (lang == CBM_LANG_LUA && strcmp(kind, "function_definition") == 0) {
        return resolve_lua_func_name(node);
    }
    if (lang == CBM_LANG_OCAML && strcmp(kind, "value_definition") == 0) {
        return resolve_ocaml_func_name(node);
    }
    if (lang == CBM_LANG_SQL && strcmp(kind, "create_function") == 0) {
        return resolve_sql_func_name(node);
    }
    if (lang == CBM_LANG_ZIG && strcmp(kind, "test_declaration") == 0) {
        return resolve_zig_test_name(node);
    }
    if (lang == CBM_LANG_VIMSCRIPT && strcmp(kind, "function_definition") == 0) {
        return resolve_vimscript_func_name(node);
    }
    if (lang == CBM_LANG_JULIA && strcmp(kind, "function_definition") == 0) {
        return resolve_julia_func_name(node);
    }

    TSNode null_node = {0};
    return null_node;
}

// Lean: resolve function name from declId field.
static TSNode resolve_lean_func_name(TSNode node, TSNode name) {
    TSNode decl_id = ts_node_child_by_field_name(node, TS_FIELD("declId"));
    if (!ts_node_is_null(decl_id)) {
        TSNode id = cbm_find_child_by_kind(decl_id, "ident");
        if (!ts_node_is_null(id)) {
            return id;
        }
        if (ts_node_named_child_count(decl_id) > 0) {
            return ts_node_named_child(decl_id, 0);
        }
        return decl_id;
    }
    if (!ts_node_is_null(name)) {
        return name;
    }
    return cbm_find_child_by_kind(node, "ident");
}

// Haskell: resolve function name from first named child (variable/name).
static TSNode resolve_haskell_func_name(TSNode node) {
    if (ts_node_named_child_count(node) > 0) {
        TSNode head = ts_node_named_child(node, 0);
        const char *hk = ts_node_type(head);
        if (strcmp(hk, "variable") == 0 || strcmp(hk, "name") == 0) {
            return head;
        }
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

// CommonLisp: resolve defun name from function_name field or defun_header→sym_lit.
static TSNode resolve_commonlisp_func_name(TSNode node) {
    TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function_name"));
    if (!ts_node_is_null(fn)) {
        return fn;
    }
    TSNode header = cbm_find_child_by_kind(node, "defun_header");
    if (!ts_node_is_null(header)) {
        return cbm_find_child_by_kind(header, "sym_lit");
    }
    TSNode null_node = {0};
    return null_node;
}

// Makefile: resolve rule name from targets child or word fallback.
static TSNode resolve_makefile_func_name(TSNode node) {
    TSNode targets = cbm_find_child_by_kind(node, "targets");
    if (!ts_node_is_null(targets) && ts_node_named_child_count(targets) > 0) {
        return ts_node_named_child(targets, 0);
    }
    return cbm_find_child_by_kind(node, "word");
}

// Elm: resolve value_declaration name from functionDeclarationLeft field.
static TSNode resolve_elm_func_name(TSNode node) {
    TSNode fdl = ts_node_child_by_field_name(node, TS_FIELD("functionDeclarationLeft"));
    if (ts_node_is_null(fdl)) {
        fdl = cbm_find_child_by_kind(node, "function_declaration_left");
    }
    if (!ts_node_is_null(fdl) && ts_node_named_child_count(fdl) > 0) {
        return ts_node_named_child(fdl, 0);
    }
    TSNode null_node = {0};
    return null_node;
}

// Wolfram: resolve set/set_delayed name from LHS apply→user_symbol.
static TSNode resolve_wolfram_func_name(TSNode node) {
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

// Resolve function name for FP/scientific languages.
static TSNode resolve_func_name_fp(TSNode node, CBMLanguage lang, const char *kind, TSNode name) {
    if (lang == CBM_LANG_COMMONLISP && strcmp(kind, "defun") == 0) {
        return resolve_commonlisp_func_name(node);
    }

    if (lang == CBM_LANG_MAKEFILE && strcmp(kind, "rule") == 0) {
        return resolve_makefile_func_name(node);
    }

    if (lang == CBM_LANG_HASKELL && strcmp(kind, "function") == 0) {
        return resolve_haskell_func_name(node);
    }

    if (lang == CBM_LANG_ELM && strcmp(kind, "value_declaration") == 0) {
        return resolve_elm_func_name(node);
    }

    if (lang == CBM_LANG_MATLAB && strcmp(kind, "function_definition") == 0) {
        if (!ts_node_is_null(name)) {
            return name;
        }
        return cbm_find_child_by_kind(node, "identifier");
    }

    if (lang == CBM_LANG_LEAN) {
        return resolve_lean_func_name(node, name);
    }

    if (lang == CBM_LANG_WOLFRAM &&
        (strcmp(kind, "set_delayed_top") == 0 || strcmp(kind, "set_top") == 0 ||
         strcmp(kind, "set_delayed") == 0 || strcmp(kind, "set") == 0)) {
        return resolve_wolfram_func_name(node);
    }

    TSNode null_node = {0};
    return null_node;
}

// Check if a node type is a terminal C declarator name.
static bool is_c_terminal_name(const char *dk) {
    return strcmp(dk, "identifier") == 0 || strcmp(dk, "field_identifier") == 0 ||
           strcmp(dk, "operator_name") == 0 || strcmp(dk, "operator_cast") == 0 ||
           strcmp(dk, "destructor_name") == 0;
}

// Resolve name from a C++ qualified_identifier/scoped_identifier.
static TSNode resolve_qualified_name(TSNode decl) {
    static const char *name_kinds[] = {"operator_name", "operator_cast",    "destructor_name",
                                       "identifier",    "field_identifier", NULL};
    for (const char **k = name_kinds; *k; k++) {
        TSNode found = cbm_find_child_by_kind(decl, *k);
        if (!ts_node_is_null(found)) {
            return found;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Resolve function name from C/C++/CUDA/GLSL declarator chain.
static TSNode resolve_c_declarator_name(TSNode node) {
    TSNode decl = ts_node_child_by_field_name(node, TS_FIELD("declarator"));
    for (int depth = 0; depth < DECLARATOR_DEPTH_LIMIT && !ts_node_is_null(decl); depth++) {
        const char *dk = ts_node_type(decl);
        if (is_c_terminal_name(dk)) {
            return decl;
        }
        if (strcmp(dk, "qualified_identifier") == 0 || strcmp(dk, "scoped_identifier") == 0) {
            return resolve_qualified_name(decl);
        }
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
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

// R: resolve function_definition name from parent binary_operator lhs.
static TSNode resolve_r_func_name(TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "binary_operator") == 0) {
        TSNode lhs = ts_node_child_by_field_name(parent, TS_FIELD("left"));
        if (ts_node_is_null(lhs)) {
            lhs = ts_node_child_by_field_name(parent, TS_FIELD("lhs"));
        }
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

// Forward declaration for mutual recursion.
static TSNode resolve_func_name(TSNode node, CBMLanguage lang);

// C++/CUDA: find inner function/declaration inside template_declaration.
// Returns the inner node (not the resolved name) to break the recursive cycle.
static TSNode resolve_template_inner_node(TSNode node) {
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ch = ts_node_named_child(node, i);
        const char *ck = ts_node_type(ch);
        if (strcmp(ck, "function_definition") == 0 || strcmp(ck, "declaration") == 0) {
            return ch;
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Try arrow_function name via parent variable_declarator (top-level resolution).
static TSNode resolve_toplevel_arrow_name(TSNode node, const char *kind) {
    if (strcmp(kind, "arrow_function") != 0) {
        TSNode null_node = {0};
        return null_node;
    }
    TSNode parent = ts_node_parent(node);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "variable_declarator") == 0) {
        return ts_node_child_by_field_name(parent, TS_FIELD("name"));
    }
    TSNode null_node = {0};
    return null_node;
}

// Try C/C++/CUDA/GLSL function_definition declarator name or template unwrap.
static TSNode resolve_func_name_c_family(TSNode *node_ptr, CBMLanguage lang, const char *kind) {
    if ((lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA) &&
        strcmp(kind, "template_declaration") == 0) {
        TSNode inner = resolve_template_inner_node(*node_ptr);
        if (!ts_node_is_null(inner)) {
            *node_ptr = inner; /* signal caller to retry */
        }
        TSNode null_node = {0};
        return null_node;
    }
    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL) &&
        strcmp(kind, "function_definition") == 0) {
        return resolve_c_declarator_name(*node_ptr);
    }
    TSNode null_node = {0};
    return null_node;
}

// Resolve the name node for a function, handling language-specific quirks.
// Uses a loop to handle template_declaration unwrapping (avoids recursion).
static TSNode resolve_func_name(TSNode node, CBMLanguage lang) {
    enum { MAX_TEMPLATE_DEPTH = 2 };
    for (int tmpl_depth = 0; tmpl_depth < MAX_TEMPLATE_DEPTH; tmpl_depth++) {
        const char *kind = ts_node_type(node);

        if (lang == CBM_LANG_HASKELL && strcmp(kind, "signature") == 0) {
            TSNode null_node = {0};
            return null_node;
        }

        TSNode name = func_name_node(node);

        if (lang == CBM_LANG_R && strcmp(kind, "function_definition") == 0) {
            return resolve_r_func_name(node);
        }

        if (!ts_node_is_null(name)) {
            return name;
        }

        if (lang == CBM_LANG_SWIFT && strcmp(kind, "function_declaration") == 0) {
            TSNode si = cbm_find_child_by_kind(node, "simple_identifier");
            if (!ts_node_is_null(si)) {
                return si;
            }
        }

        {
            TSNode r = resolve_toplevel_arrow_name(node, kind);
            if (!ts_node_is_null(r)) {
                return r;
            }
        }
        {
            TSNode r = resolve_func_name_scripting(node, lang, kind);
            if (!ts_node_is_null(r)) {
                return r;
            }
        }
        {
            TSNode r = resolve_func_name_fp(node, lang, kind, name);
            if (!ts_node_is_null(r)) {
                return r;
            }
        }

        {
            TSNode prev = node;
            TSNode r = resolve_func_name_c_family(&node, lang, kind);
            if (!ts_node_is_null(r)) {
                return r;
            }
            if (!ts_node_eq(prev, node)) {
                continue; /* template unwrapped — retry */
            }
        }

        break;
    } /* end template depth loop */
    TSNode null_node = {0};
    return null_node;
}

// Check for export_statement ancestor (JS/TS/TSX)
static bool is_js_exported(TSNode node) {
    return cbm_has_ancestor_kind(node, "export_statement", EXPORT_ANCESTOR_DEPTH);
}

// Check if a node is a comment node type.
static bool is_comment_node(const char *kind) {
    return (strcmp(kind, "comment") == 0 || strcmp(kind, "block_comment") == 0 ||
            strcmp(kind, "line_comment") == 0);
}

// Extract comment text, truncating to MAX_COMMENT_LEN.
static char *extract_comment_text(CBMArena *a, TSNode node, const char *source) {
    char *text = cbm_node_text(a, node, source);
    if (text && strlen(text) > MAX_COMMENT_LEN) {
        text[MAX_COMMENT_LEN] = '\0';
    }
    return text;
}

// Go-specific: type_spec/type_alias comment is before the parent type_declaration.
static const char *extract_go_type_docstring(CBMArena *a, TSNode node, const char *source) {
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "type_spec") != 0 && strcmp(kind, "type_alias") != 0) {
        return NULL;
    }
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent) || strcmp(ts_node_type(parent), "type_declaration") != 0) {
        return NULL;
    }
    TSNode pprev = ts_node_prev_sibling(parent);
    if (!ts_node_is_null(pprev) && is_comment_node(ts_node_type(pprev))) {
        return extract_comment_text(a, pprev, source);
    }
    return NULL;
}

// Python-specific: docstring as first expression_statement -> string in function body.
static const char *extract_python_docstring(CBMArena *a, TSNode node, const char *source) {
    TSNode body = ts_node_child_by_field_name(node, TS_FIELD("body"));
    if (ts_node_is_null(body) || ts_node_named_child_count(body) == 0) {
        return NULL;
    }
    TSNode first = ts_node_named_child(body, 0);
    if (ts_node_is_null(first) || strcmp(ts_node_type(first), "expression_statement") != 0) {
        return NULL;
    }
    if (ts_node_named_child_count(first) == 0) {
        return NULL;
    }
    TSNode str = ts_node_named_child(first, 0);
    if (ts_node_is_null(str)) {
        return NULL;
    }
    const char *sk = ts_node_type(str);
    if (strcmp(sk, "string") == 0 || strcmp(sk, "concatenated_string") == 0) {
        return extract_comment_text(a, str, source);
    }
    return NULL;
}

// Extract docstring from the node's leading comment.
static const char *extract_docstring(CBMArena *a, TSNode node, const char *source,
                                     CBMLanguage lang) {
    if (lang == CBM_LANG_GO) {
        const char *doc = extract_go_type_docstring(a, node, source);
        if (doc) {
            return doc;
        }
    }

    TSNode prev = ts_node_prev_sibling(node);
    if (!ts_node_is_null(prev) && is_comment_node(ts_node_type(prev))) {
        return extract_comment_text(a, prev, source);
    }

    if (lang == CBM_LANG_PYTHON) {
        return extract_python_docstring(a, node, source);
    }
    return NULL;
}

/* HTTP method names recognized in decorator calls (e.g., @router.post → "POST") */
static const char *decorator_method_name(const char *attr_text) {
    if (!attr_text) {
        return NULL;
    }
    /* Match the last segment after the dot: "router.post" → "post" */
    const char *dot = strrchr(attr_text, '.');
    const char *method = dot ? dot + SKIP_CHAR : attr_text;
    if (strcmp(method, "get") == 0 || strcmp(method, "Get") == 0) {
        return "GET";
    }
    if (strcmp(method, "post") == 0 || strcmp(method, "Post") == 0) {
        return "POST";
    }
    if (strcmp(method, "put") == 0 || strcmp(method, "Put") == 0) {
        return "PUT";
    }
    if (strcmp(method, "delete") == 0 || strcmp(method, "Delete") == 0) {
        return "DELETE";
    }
    if (strcmp(method, "patch") == 0 || strcmp(method, "Patch") == 0) {
        return "PATCH";
    }
    if (strcmp(method, "route") == 0 || strcmp(method, "api_route") == 0) {
        return "ANY";
    }
    return NULL;
}

/* Extract route path + method from a decorator's AST nodes.
 * Works for: @app.route("/path"), @router.post("/path"), @GetMapping("/path"),
 * @app.get("/path", ...), etc.
 *
 * Pure AST approach: walks the decorator node's call children to find:
 * 1. The function/attribute name → infer HTTP method
 * 2. The first string argument → route path */
// Find the arguments node for a decorator call node.
static TSNode find_decorator_args(TSNode call_node) {
    TSNode args = ts_node_child_by_field_name(call_node, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        for (uint32_t ai = 0; ai < ts_node_named_child_count(call_node); ai++) {
            TSNode ac = ts_node_named_child(call_node, ai);
            if (strcmp(ts_node_type(ac), "argument_list") == 0) {
                return ac;
            }
        }
    }
    return args;
}

// Extract route path from decorator arguments (first string that starts with /).
static const char *extract_route_path_from_args(CBMArena *a, TSNode args, const char *source) {
    uint32_t nc = ts_node_named_child_count(args);
    for (uint32_t ai = 0; ai < nc && ai < DECORATOR_SCAN_LIMIT; ai++) {
        TSNode arg = ts_node_named_child(args, ai);
        const char *ak = ts_node_type(arg);
        if (strcmp(ak, "string") != 0 && strcmp(ak, "string_literal") != 0 &&
            strcmp(ak, "interpreted_string_literal") != 0) {
            continue;
        }
        char *path = cbm_node_text(a, arg, source);
        if (path) {
            int plen = (int)strlen(path);
            if (plen >= PAIR_CHARS && (path[0] == '"' || path[0] == '\'')) {
                path = cbm_arena_strndup(a, path + SKIP_CHAR, (size_t)(plen - PAIR_CHARS));
            }
            if (path && path[0] == '/') {
                return path;
            }
        }
    }
    return NULL;
}

// Try to extract a route from a single decorator call node.
// Returns true if a route method was found (even with fallback path "/").
static bool try_route_from_decorator_call(CBMArena *a, TSNode dchild, const char *source,
                                          const char **out_path, const char **out_method) {
    TSNode fn = ts_node_child_by_field_name(dchild, TS_FIELD("function"));
    if (ts_node_is_null(fn)) {
        fn = ts_node_named_child(dchild, 0);
    }
    if (ts_node_is_null(fn)) {
        return false;
    }

    char *fn_text = cbm_node_text(a, fn, source);
    const char *method = decorator_method_name(fn_text);
    if (!method) {
        return false;
    }

    TSNode args = find_decorator_args(dchild);
    if (!ts_node_is_null(args)) {
        const char *path = extract_route_path_from_args(a, args, source);
        if (path) {
            *out_path = path;
            *out_method = method;
            return true;
        }
    }
    *out_path = "/";
    *out_method = method;
    return true;
}

static void extract_route_from_decorators(CBMArena *a, TSNode func_node, const char *source,
                                          const CBMLangSpec *spec, const char **out_path,
                                          const char **out_method) {
    *out_path = NULL;
    *out_method = NULL;

    if (!spec->decorator_node_types || !spec->decorator_node_types[0]) {
        return;
    }

    TSNode prev = ts_node_prev_sibling(func_node);
    while (!ts_node_is_null(prev)) {
        if (!cbm_kind_in_set(prev, spec->decorator_node_types)) {
            break;
        }

        uint32_t dc = ts_node_named_child_count(prev);
        for (uint32_t di = 0; di < dc; di++) {
            TSNode dchild = ts_node_named_child(prev, di);
            if (strcmp(ts_node_type(dchild), "call") != 0) {
                continue;
            }
            if (try_route_from_decorator_call(a, dchild, source, out_path, out_method)) {
                return;
            }
        }
        prev = ts_node_prev_sibling(prev);
    }
}

// Extract decorator names from preceding decorator/annotation nodes
// Count annotations inside a Java/Kotlin/C# "modifiers" node.
static int count_modifier_annotations(TSNode modifiers, const CBMLangSpec *spec) {
    int count = 0;
    uint32_t mc = ts_node_child_count(modifiers);
    for (uint32_t mi = 0; mi < mc; mi++) {
        TSNode mchild = ts_node_child(modifiers, mi);
        if (cbm_kind_in_set(mchild, spec->decorator_node_types)) {
            count++;
        }
    }
    return count;
}

// Find Java/Kotlin/C# modifiers node with annotations.
static TSNode find_jvm_modifiers(TSNode node, CBMLanguage lang) {
    TSNode null_node = {0};
    if (lang != CBM_LANG_JAVA && lang != CBM_LANG_KOTLIN && lang != CBM_LANG_CSHARP) {
        return null_node;
    }
    TSNode modifiers = ts_node_child_by_field_name(node, TS_FIELD("modifiers"));
    if (ts_node_is_null(modifiers)) {
        modifiers = cbm_find_child_by_kind(node, "modifiers");
    }
    return modifiers;
}

// Collect decorator texts from a modifiers node into result array starting at idx.
static int collect_modifier_decorators(CBMArena *a, TSNode modifiers, const char *source,
                                       const CBMLangSpec *spec, const char **result, int idx,
                                       int max) {
    uint32_t mc = ts_node_child_count(modifiers);
    for (uint32_t mi = 0; mi < mc && idx < max; mi++) {
        TSNode mchild = ts_node_child(modifiers, mi);
        if (cbm_kind_in_set(mchild, spec->decorator_node_types)) {
            result[idx++] = cbm_node_text(a, mchild, source);
        }
    }
    return idx;
}

static const char **extract_decorators(CBMArena *a, TSNode node, const char *source,
                                       CBMLanguage lang, const CBMLangSpec *spec) {
    if (!spec->decorator_node_types || !spec->decorator_node_types[0]) {
        return NULL;
    }

    int count = 0;
    TSNode prev = ts_node_prev_sibling(node);
    while (!ts_node_is_null(prev)) {
        if (cbm_kind_in_set(prev, spec->decorator_node_types)) {
            count++;
        } else {
            break;
        }
        prev = ts_node_prev_sibling(prev);
    }

    TSNode modifiers = {0};
    int mod_count = 0;
    if (count == 0) {
        modifiers = find_jvm_modifiers(node, lang);
        if (!ts_node_is_null(modifiers)) {
            mod_count = count_modifier_annotations(modifiers, spec);
        }
    }

    int total = count + mod_count;
    if (total == 0) {
        return NULL;
    }

    const char **result =
        (const char **)cbm_arena_alloc(a, sizeof(const char *) * (total + NULL_TERM));
    if (!result) {
        return NULL;
    }

    int idx = 0;
    prev = ts_node_prev_sibling(node);
    while (!ts_node_is_null(prev) && idx < count) {
        if (cbm_kind_in_set(prev, spec->decorator_node_types)) {
            result[idx++] = cbm_node_text(a, prev, source);
        } else {
            break;
        }
        prev = ts_node_prev_sibling(prev);
    }
    if (!ts_node_is_null(modifiers)) {
        idx = collect_modifier_decorators(a, modifiers, source, spec, result, idx, total);
    }
    result[idx] = NULL;
    return result;
}

// Extract base class name text from a single base_class child node.
static char *extract_cpp_base_text(CBMArena *a, TSNode bc, const char *source) {
    const char *bk = ts_node_type(bc);
    if (strcmp(bk, "access_specifier") == 0) {
        return NULL;
    }
    if (strcmp(bk, "type_identifier") == 0 || strcmp(bk, "qualified_identifier") == 0 ||
        strcmp(bk, "scoped_identifier") == 0) {
        return cbm_node_text(a, bc, source);
    }
    if (strcmp(bk, "template_type") == 0) {
        TSNode tname = ts_node_child_by_field_name(bc, TS_FIELD("name"));
        if (!ts_node_is_null(tname)) {
            return cbm_node_text(a, tname, source);
        }
    }
    return NULL;
}

// Extract base classes from a C++ base_class_clause node.
static const char **extract_cpp_base_classes(CBMArena *a, TSNode clause, const char *source) {
    const char *bases[MAX_BASES];
    int base_count = 0;
    uint32_t bnc = ts_node_named_child_count(clause);
    for (uint32_t bi = 0; bi < bnc && base_count < MAX_BASES_MINUS_1; bi++) {
        char *text = extract_cpp_base_text(a, ts_node_named_child(clause, bi), source);
        if (text && text[0]) {
            bases[base_count++] = text;
        }
    }
    if (base_count > 0) {
        const char **result =
            (const char **)cbm_arena_alloc(a, (base_count + NULL_TERM) * sizeof(const char *));
        if (result) {
            for (int j = 0; j < base_count; j++) {
                result[j] = bases[j];
            }
            result[base_count] = NULL;
            return result;
        }
    }
    return NULL;
}

// Build a single-element NULL-terminated base class array.
static const char **make_single_base(CBMArena *a, const char *text) {
    if (!text || !text[0]) {
        return NULL;
    }
    const char **result = (const char **)cbm_arena_alloc(a, sizeof(const char *) * RT_PAIR_SIZE);
    if (result) {
        result[0] = text;
        result[SKIP_CHAR] = NULL;
    }
    return result;
}

// Search children for a child matching one of the base_types and return its text as single base.
static const char **find_base_from_children(CBMArena *a, TSNode node, const char *source,
                                            const char **base_types) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        for (const char **t = base_types; *t; t++) {
            if (strcmp(ck, *t) == 0) {
                const char **r = make_single_base(a, cbm_node_text(a, child, source));
                if (r) {
                    return r;
                }
            }
        }
    }
    return NULL;
}

// Extract base class names from a class node.
static const char **extract_base_classes(CBMArena *a, TSNode node, const char *source,
                                         CBMLanguage lang) {
    (void)lang;
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
            const char **r = make_single_base(a, cbm_node_text(a, super, source));
            if (r) {
                return r;
            }
        }
    }

    // C/C++: handle base_class_clause
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (strcmp(ts_node_type(child), "base_class_clause") == 0) {
            const char **result = extract_cpp_base_classes(a, child, source);
            if (result) {
                return result;
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
    return find_base_from_children(a, node, source, base_types);
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
    char *result = cbm_arena_alloc(a, len + NULL_TERM);
    memcpy(result, s, len);
    result[len] = '\0';
    return result;
}

// Extract param_names from a parameter list node.
// Returns NULL-terminated arena-allocated array.
// Extract the parameter name from a single parameter AST node.
static char *resolve_param_name(CBMArena *a, TSNode param, const char *source) {
    const char *pk = ts_node_type(param);

    if (strcmp(pk, "parameter_declaration") == 0) {
        TSNode nm = ts_node_child_by_field_name(param, TS_FIELD("name"));
        if (!ts_node_is_null(nm)) {
            return cbm_node_text(a, nm, source);
        }
        return NULL;
    }
    if (strcmp(pk, "identifier") == 0) {
        return cbm_node_text(a, param, source);
    }
    if (strcmp(pk, "formal_parameter") == 0 || strcmp(pk, "parameter") == 0 ||
        strcmp(pk, "required_parameter") == 0 || strcmp(pk, "optional_parameter") == 0 ||
        strcmp(pk, "simple_parameter") == 0 || strcmp(pk, "typed_parameter") == 0 ||
        strcmp(pk, "default_parameter") == 0 || strcmp(pk, "typed_default_parameter") == 0) {
        TSNode nm = ts_node_child_by_field_name(param, TS_FIELD("name"));
        if (ts_node_is_null(nm)) {
            nm = ts_node_child_by_field_name(param, TS_FIELD("pattern"));
        }
        if (!ts_node_is_null(nm)) {
            if (strcmp(ts_node_type(nm), "identifier") == 0 ||
                strcmp(ts_node_type(nm), "simple_identifier") == 0) {
                return cbm_node_text(a, nm, source);
            }
        }
    }
    return NULL;
}

static const char **extract_param_names(CBMArena *a, TSNode params, const char *source,
                                        CBMLanguage lang) {
    (void)lang;
    if (ts_node_is_null(params)) {
        return NULL;
    }

    const char *names[MAX_PARAMS];
    int count = 0;

    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc && count < MAX_PARAMS_MINUS_1; i++) {
        TSNode param = ts_node_child(params, i);
        if (ts_node_is_null(param) || !ts_node_is_named(param)) {
            continue;
        }

        char *name_text = resolve_param_name(a, param, source);

        if (name_text && name_text[0]) {
            names[count++] = name_text;
        }
    }

    if (count == 0) {
        return NULL;
    }

    const char **result =
        (const char **)cbm_arena_alloc(a, (count + NULL_TERM) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = names[i];
    }
    result[count] = NULL;
    return result;
}

// Extract return_types from a return type node.
// Parses Go-style multi-return (T1, T2) and single return types.
// Returns NULL-terminated arena-allocated array.
// Clean a type text and add to types array if valid.
static void add_cleaned_type(CBMArena *a, const char **types, int *count, char *type_text) {
    if (!type_text || !type_text[0]) {
        return;
    }
    char *cleaned = clean_type_name(a, type_text);
    if (cleaned && cleaned[0]) {
        types[(*count)++] = cleaned;
    }
}

// Extract Go multi-return types from a parameter_list result node.
static void extract_go_multi_return(CBMArena *a, TSNode rt_node, const char *source,
                                    const char **types, int *count) {
    uint32_t nc = ts_node_child_count(rt_node);
    for (uint32_t i = 0; i < nc && *count < MAX_RETURN_TYPES_MINUS_1; i++) {
        TSNode child = ts_node_child(rt_node, i);
        if (ts_node_is_null(child) || !ts_node_is_named(child)) {
            continue;
        }
        if (strcmp(ts_node_type(child), "parameter_declaration") == 0) {
            TSNode tn = ts_node_child_by_field_name(child, TS_FIELD("type"));
            if (!ts_node_is_null(tn)) {
                add_cleaned_type(a, types, count, cbm_node_text(a, tn, source));
            }
        } else {
            add_cleaned_type(a, types, count, cbm_node_text(a, child, source));
        }
    }
}

// Build a NULL-terminated arena-allocated string array from a types buffer.
static const char **build_type_array(CBMArena *a, const char **types, int count) {
    if (count == 0) {
        return NULL;
    }
    const char **result =
        (const char **)cbm_arena_alloc(a, (count + NULL_TERM) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = types[i];
    }
    result[count] = NULL;
    return result;
}

static const char **extract_return_types(CBMArena *a, TSNode rt_node, const char *source,
                                         CBMLanguage lang) {
    (void)lang;
    if (ts_node_is_null(rt_node)) {
        return NULL;
    }

    const char *types[MAX_RETURN_TYPES];
    int count = 0;

    if (strcmp(ts_node_type(rt_node), "parameter_list") == 0) {
        extract_go_multi_return(a, rt_node, source, types, &count);
    } else {
        add_cleaned_type(a, types, &count, cbm_node_text(a, rt_node, source));
    }

    return build_type_array(a, types, count);
}

// Extract param_types from a parameter list node.
// Returns NULL-terminated arena-allocated array.
// Extract type text from a TypeScript type_annotation child.
static char *extract_ts_param_type(CBMArena *a, TSNode param, const char *source) {
    TSNode ta = cbm_find_child_by_kind(param, "type_annotation");
    if (ts_node_is_null(ta)) {
        return NULL;
    }
    uint32_t tanc = ts_node_named_child_count(ta);
    for (uint32_t ti = 0; ti < tanc; ti++) {
        TSNode tch = ts_node_named_child(ta, ti);
        if (ts_node_is_null(tch)) {
            continue;
        }
        const char *tk = ts_node_type(tch);
        if (strcmp(tk, "type_identifier") == 0 || strcmp(tk, "generic_type") == 0 ||
            strcmp(tk, "predefined_type") == 0) {
            return cbm_node_text(a, tch, source);
        }
    }
    return NULL;
}

// Check if a param node type is a generic parameter-like node.
static bool is_generic_param_kind(const char *pk) {
    return strcmp(pk, "formal_parameter") == 0 || strcmp(pk, "parameter") == 0 ||
           strcmp(pk, "parameter_declaration") == 0 || strcmp(pk, "spread_parameter") == 0 ||
           strcmp(pk, "simple_parameter") == 0 || strcmp(pk, "variadic_parameter") == 0 ||
           strcmp(pk, "typed_parameter") == 0;
}

// Resolve param type for JVM/misc languages (Kotlin, Scala, Dart, Groovy, OCaml).
static char *resolve_jvm_param_type(CBMArena *a, TSNode param, const char *pk, const char *source,
                                    CBMLanguage lang) {
    if (strcmp(pk, "parameter") != 0 && strcmp(pk, "formal_parameter") != 0) {
        return NULL;
    }
    if (lang == CBM_LANG_KOTLIN) {
        TSNode tn = ts_node_child_by_field_name(param, TS_FIELD("type"));
        if (!ts_node_is_null(tn)) {
            return cbm_node_text(a, tn, source);
        }
        TSNode ut = cbm_find_child_by_kind(param, "user_type");
        return ts_node_is_null(ut) ? NULL : cbm_node_text(a, ut, source);
    }
    if (lang == CBM_LANG_SCALA || lang == CBM_LANG_DART) {
        TSNode tid = cbm_find_child_by_kind(param, "type_identifier");
        return ts_node_is_null(tid) ? NULL : cbm_node_text(a, tid, source);
    }
    if (lang == CBM_LANG_GROOVY) {
        TSNode tn = ts_node_child_by_field_name(param, TS_FIELD("type"));
        return ts_node_is_null(tn) ? NULL : cbm_node_text(a, tn, source);
    }
    if (lang == CBM_LANG_OCAML) {
        TSNode tp = cbm_find_child_by_kind(param, "typed_pattern");
        if (!ts_node_is_null(tp)) {
            TSNode tn = ts_node_child_by_field_name(tp, TS_FIELD("type"));
            if (!ts_node_is_null(tn)) {
                return cbm_node_text(a, tn, source);
            }
        }
        return NULL;
    }
    return NULL;
}

// Resolve parameter type text for a single param node.
static char *resolve_param_type_text(CBMArena *a, TSNode param, const char *source,
                                     CBMLanguage lang) {
    const char *pk = ts_node_type(param);

    if (lang == CBM_LANG_TYPESCRIPT || lang == CBM_LANG_TSX) {
        if (strcmp(pk, "required_parameter") == 0 || strcmp(pk, "optional_parameter") == 0) {
            return extract_ts_param_type(a, param, source);
        }
        return NULL;
    }

    if (lang == CBM_LANG_KOTLIN || lang == CBM_LANG_SCALA || lang == CBM_LANG_DART ||
        lang == CBM_LANG_GROOVY || lang == CBM_LANG_OCAML) {
        return resolve_jvm_param_type(a, param, pk, source, lang);
    }

    // Generic: parameter-like nodes with "type" field
    if (is_generic_param_kind(pk)) {
        TSNode tn = ts_node_child_by_field_name(param, TS_FIELD("type"));
        if (!ts_node_is_null(tn)) {
            return cbm_node_text(a, tn, source);
        }
    }
    return NULL;
}

// Add a cleaned, deduplicated type to the types array.
static void add_dedup_type(CBMArena *a, const char **types, int *count, char *type_text) {
    if (!type_text || !type_text[0]) {
        return;
    }
    char *cleaned = clean_type_name(a, type_text);
    if (!cleaned || !cleaned[0] || is_builtin_type(cleaned)) {
        return;
    }
    for (int j = 0; j < *count; j++) {
        if (strcmp(types[j], cleaned) == 0) {
            return;
        }
    }
    types[(*count)++] = cleaned;
}

static const char **extract_param_types(CBMArena *a, TSNode params, const char *source,
                                        CBMLanguage lang) {
    if (ts_node_is_null(params)) {
        return NULL;
    }

    const char *types[MAX_PARAMS];
    int count = 0;

    uint32_t nc = ts_node_child_count(params);
    for (uint32_t i = 0; i < nc && count < MAX_PARAMS_MINUS_1; i++) {
        TSNode param = ts_node_child(params, i);
        if (ts_node_is_null(param) || !ts_node_is_named(param)) {
            continue;
        }
        add_dedup_type(a, types, &count, resolve_param_type_text(a, param, source, lang));
    }

    if (count == 0) {
        return NULL;
    }

    const char **result =
        (const char **)cbm_arena_alloc(a, (count + NULL_TERM) * sizeof(const char *));
    for (int i = 0; i < count; i++) {
        result[i] = types[i];
    }
    result[count] = NULL;
    return result;
}

// --- Function definition extraction ---

// For C++/CUDA template_declaration, find the inner function_definition or declaration.
static TSNode unwrap_template_inner(TSNode node, CBMLanguage lang) {
    if ((lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA) &&
        strcmp(ts_node_type(node), "template_declaration") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode ch = ts_node_named_child(node, i);
            const char *ck = ts_node_type(ch);
            if (strcmp(ck, "function_definition") == 0 || strcmp(ck, "declaration") == 0) {
                return ch;
            }
        }
    }
    return node;
}

// C/C++/CUDA/GLSL: parameters live on function_declarator inside declarator chain.
static TSNode find_c_params(TSNode func_node) {
    TSNode decl = ts_node_child_by_field_name(func_node, TS_FIELD("declarator"));
    for (int d = 0; d < C_RETURN_WALK_DEPTH && !ts_node_is_null(decl); d++) {
        TSNode params = ts_node_child_by_field_name(decl, TS_FIELD("parameters"));
        if (!ts_node_is_null(params)) {
            return params;
        }
        decl = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
    }
    TSNode null_node = {0};
    return null_node;
}

// C++: resolve trailing return type (auto f() -> Type) on a declarator node.
// Updates def->return_type and def->return_types if trailing type found.
static void resolve_cpp_trailing_return(CBMArena *a, TSNode func_node, const char *source,
                                        CBMDefinition *def) {
    TSNode declarator = ts_node_child_by_field_name(func_node, TS_FIELD("declarator"));
    if (ts_node_is_null(declarator)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(declarator);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode ch = ts_node_named_child(declarator, i);
        if (strcmp(ts_node_type(ch), "trailing_return_type") == 0) {
            TSNode type_desc = ts_node_named_child_count(ch) > 0 ? ts_node_named_child(ch, 0) : ch;
            def->return_type = cbm_node_text(a, type_desc, source);
            if (def->return_type && def->return_type[0]) {
                const char **rt =
                    (const char **)cbm_arena_alloc(a, RT_PAIR_SIZE * sizeof(const char *));
                if (rt) {
                    rt[0] = def->return_type;
                    rt[SKIP_CHAR] = NULL;
                    def->return_types = rt;
                }
            }
            break;
        }
    }
}

static void extract_func_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;

    TSNode name_node = resolve_func_name(node, ctx->language);
    if (ts_node_is_null(name_node)) {
        return;
    }

    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0] || strcmp(name, "function") == 0) {
        return;
    }

    TSNode func_node = unwrap_template_inner(node, ctx->language);

    CBMDefinition def;
    memset(&def, 0, sizeof(def));

    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = "Function";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = cbm_is_exported(name, ctx->language);

    // Parameters — use func_node (inner function for templates)
    TSNode params = ts_node_child_by_field_name(func_node, TS_FIELD("parameters"));
    if (ts_node_is_null(params) &&
        (ctx->language == CBM_LANG_C || ctx->language == CBM_LANG_CPP ||
         ctx->language == CBM_LANG_CUDA || ctx->language == CBM_LANG_GLSL)) {
        params = find_c_params(func_node);
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

    // C++: trailing return type (auto f() -> Type)
    if (def.return_type && strcmp(def.return_type, "auto") == 0 &&
        (ctx->language == CBM_LANG_CPP || ctx->language == CBM_LANG_CUDA)) {
        resolve_cpp_trailing_return(a, func_node, ctx->source, &def);
    }

    // Receiver (Go methods)
    TSNode recv = ts_node_child_by_field_name(node, TS_FIELD("receiver"));
    if (!ts_node_is_null(recv)) {
        def.receiver = cbm_node_text(a, recv, ctx->source);
        def.label = "Method";
    }

    // Decorators + route extraction from decorator AST
    def.decorators = extract_decorators(a, node, ctx->source, ctx->language, spec);
    extract_route_from_decorators(a, node, ctx->source, spec, &def.route_path, &def.route_method);

    // Docstring
    def.docstring = extract_docstring(a, node, ctx->source, ctx->language);

    // Complexity
    if (spec->branching_node_types && spec->branching_node_types[0]) {
        def.complexity = cbm_count_branching(node, spec->branching_node_types);
    }

    // MinHash fingerprint
    compute_fingerprint(ctx, &def, func_node);

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

// Push a simple class definition (used by config language extractors).
static void push_simple_class_def(CBMExtractCtx *ctx, TSNode node, char *name, const char *label) {
    CBMArena *a = ctx->arena;
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = label;
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, a, def);
}

// Find TOML table key name from children.
static char *find_toml_key_name(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "bare_key") == 0 || strcmp(ck, "dotted_key") == 0 ||
            strcmp(ck, "quoted_key") == 0 || strcmp(ck, "key") == 0) {
            return cbm_node_text(a, child, source);
        }
    }
    return NULL;
}

// Extract XML element name from start_tag/self_closing_tag children.
static char *find_xml_element_name(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "start_tag") == 0 || strcmp(ck, "self_closing_tag") == 0 ||
            strcmp(ck, "STag") == 0 || strcmp(ck, "EmptyElemTag") == 0) {
            uint32_t tnc = ts_node_child_count(child);
            for (uint32_t j = 0; j < tnc; j++) {
                TSNode tag = ts_node_child(child, j);
                const char *tk = ts_node_type(tag);
                if (strcmp(tk, "tag_name") == 0 || strcmp(tk, "Name") == 0) {
                    return cbm_node_text(a, tag, source);
                }
            }
        }
    }
    // Fallback: try "Name" field directly for some XML grammars
    TSNode name_child = cbm_find_child_by_kind(node, "Name");
    if (!ts_node_is_null(name_child)) {
        return cbm_node_text(a, name_child, source);
    }
    return NULL;
}

// Extract text from an atx_heading node (# Title).
static char *extract_atx_heading_text(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "heading_content") == 0 || strcmp(ck, "inline") == 0) {
            return cbm_node_text(a, child, source);
        }
    }
    // Fallback: strip leading # and spaces from full text
    char *full = cbm_node_text(a, node, source);
    if (full) {
        char *p = full;
        while (*p == '#') {
            p++;
        }
        while (*p == ' ') {
            p++;
        }
        if (*p) {
            return cbm_arena_strdup(a, p);
        }
    }
    return NULL;
}

// Trim trailing whitespace/newlines from a heading name in-place.
static char *trim_heading_name(char *name) {
    if (!name || !name[0]) {
        return NULL;
    }
    size_t len = strlen(name);
    while (len > 0 && (name[len - SKIP_CHAR] == '\n' || name[len - SKIP_CHAR] == '\r' ||
                       name[len - SKIP_CHAR] == ' ')) {
        name[len - SKIP_CHAR] = '\0';
        len--;
    }
    return (name[0]) ? name : NULL;
}

// Extract Markdown heading name from atx_heading or setext_heading.
static char *extract_markdown_heading_name(CBMArena *a, TSNode node, const char *kind,
                                           const char *source) {
    char *name = NULL;
    if (strcmp(kind, "atx_heading") == 0) {
        name = extract_atx_heading_text(a, node, source);
    } else {
        if (ts_node_child_count(node) > 0) {
            name = cbm_node_text(a, ts_node_child(node, 0), source);
        }
    }
    return trim_heading_name(name);
}

// INI: extract section name from section node.
static char *find_ini_section_name(CBMArena *a, TSNode node, const char *source) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        if (strcmp(ts_node_type(ts_node_child(node, i)), "section_name") == 0) {
            return cbm_node_text(a, ts_node_child(node, i), source);
        }
    }
    return NULL;
}

// HCL: extract block name from identifier child.
static char *find_hcl_block_name(CBMArena *a, TSNode node, const char *source) {
    TSNode id = cbm_find_child_by_kind(node, "identifier");
    if (!ts_node_is_null(id)) {
        return cbm_node_text(a, id, source);
    }
    return NULL;
}

// Handle config language class nodes (TOML, INI, XML, Markdown, HCL).
// Returns true if handled (caller should return early).
static bool extract_config_class_def(CBMExtractCtx *ctx, TSNode node, const char *kind) {
    CBMArena *a = ctx->arena;
    char *name = NULL;
    const char *label = "Class";

    if (ctx->language == CBM_LANG_TOML &&
        (strcmp(kind, "table") == 0 || strcmp(kind, "table_array_element") == 0)) {
        name = find_toml_key_name(a, node, ctx->source);
    } else if (ctx->language == CBM_LANG_INI && strcmp(kind, "section") == 0) {
        name = find_ini_section_name(a, node, ctx->source);
    } else if (ctx->language == CBM_LANG_XML && strcmp(kind, "element") == 0) {
        name = find_xml_element_name(a, node, ctx->source);
    } else if (ctx->language == CBM_LANG_MARKDOWN &&
               (strcmp(kind, "atx_heading") == 0 || strcmp(kind, "setext_heading") == 0)) {
        name = extract_markdown_heading_name(a, node, kind, ctx->source);
        label = "Section";
    } else if (ctx->language == CBM_LANG_HCL && strcmp(kind, "block") == 0) {
        name = find_hcl_block_name(a, node, ctx->source);
    } else {
        return false;
    }

    if (name && name[0]) {
        push_simple_class_def(ctx, node, name, label);
    }
    return true;
}

static void extract_class_def(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);

    if (extract_config_class_def(ctx, node, kind)) {
        return;
    }

    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
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
        TSNode type_inner = ts_node_child_by_field_name(node, TS_FIELD("type"));
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
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
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
        TSNode type_inner = ts_node_child_by_field_name(class_node, TS_FIELD("type"));
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

// Dart: resolve method name from method_signature/function_signature.
static TSNode resolve_dart_method_name(TSNode child, const char *ck) {
    if (strcmp(ck, "method_signature") == 0) {
        TSNode func_sig = cbm_find_child_by_kind(child, "function_signature");
        if (!ts_node_is_null(func_sig)) {
            TSNode name_node = func_name_node(func_sig);
            if (!ts_node_is_null(name_node)) {
                return name_node;
            }
            return cbm_find_child_by_kind(func_sig, "identifier");
        }
    }
    if (strcmp(ck, "function_signature") == 0) {
        return cbm_find_child_by_kind(child, "identifier");
    }
    TSNode null_node = {0};
    return null_node;
}

// Arrow function: name on parent variable_declarator/field_definition.
static TSNode resolve_arrow_func_name(TSNode child) {
    TSNode parent = ts_node_parent(child);
    if (!ts_node_is_null(parent)) {
        const char *pk = ts_node_type(parent);
        if (strcmp(pk, "field_definition") == 0) {
            return ts_node_child_by_field_name(parent, TS_FIELD("property"));
        }
        if (strcmp(pk, "public_field_definition") == 0 || strcmp(pk, "variable_declarator") == 0) {
            return ts_node_child_by_field_name(parent, TS_FIELD("name"));
        }
    }
    TSNode null_node = {0};
    return null_node;
}

// Try to extract method name from a node, with language-specific fallbacks.
static TSNode resolve_method_name(TSNode child, CBMLanguage lang) {
    TSNode name_node = func_name_node(child);
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }

    const char *ck = ts_node_type(child);

    if ((lang == CBM_LANG_C || lang == CBM_LANG_CPP || lang == CBM_LANG_CUDA ||
         lang == CBM_LANG_GLSL) &&
        strcmp(ck, "function_definition") == 0) {
        return resolve_func_name(child, lang);
    }

    if (lang == CBM_LANG_GROOVY && strcmp(ck, "function_definition") == 0) {
        TSNode fn = ts_node_child_by_field_name(child, TS_FIELD("function"));
        if (!ts_node_is_null(fn)) {
            return fn;
        }
        return cbm_find_child_by_kind(child, "identifier");
    }

    if (lang == CBM_LANG_DART) {
        return resolve_dart_method_name(child, ck);
    }

    if (lang == CBM_LANG_OBJC && strcmp(ck, "method_definition") == 0) {
        return cbm_find_child_by_kind(child, "identifier");
    }

    if (lang == CBM_LANG_SWIFT && strcmp(ck, "function_declaration") == 0) {
        return cbm_find_child_by_kind(child, "simple_identifier");
    }

    if (strcmp(ck, "arrow_function") == 0) {
        return resolve_arrow_func_name(child);
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
    def.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
    def.lines = (int)(def.end_line - def.start_line + TS_LINE_OFFSET);
    def.is_exported = cbm_is_exported(name, ctx->language);

    TSNode params = ts_node_child_by_field_name(child, TS_FIELD("parameters"));
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
        resolve_cpp_trailing_return(a, child, ctx->source, &def);
    }

    def.decorators = extract_decorators(a, child, ctx->source, ctx->language, spec);
    extract_route_from_decorators(a, child, ctx->source, spec, &def.route_path, &def.route_method);
    def.docstring = extract_docstring(a, child, ctx->source, ctx->language);

    if (spec->branching_node_types && spec->branching_node_types[0]) {
        def.complexity = cbm_count_branching(child, spec->branching_node_types);
    }

    // MinHash fingerprint
    compute_fingerprint(ctx, &def, child);

    cbm_defs_push(&ctx->result->defs, a, def);
}

// Extract methods from an ObjC implementation_definition node.
static void extract_objc_impl_methods(CBMExtractCtx *ctx, TSNode impl_node, const char *class_qn,
                                      const CBMLangSpec *spec) {
    uint32_t nc = ts_node_child_count(impl_node);
    for (uint32_t j = 0; j < nc; j++) {
        TSNode inner = ts_node_child(impl_node, j);
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

        if (ctx->language == CBM_LANG_OBJC &&
            strcmp(ts_node_type(child), "implementation_definition") == 0) {
            extract_objc_impl_methods(ctx, child, class_qn, spec);
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

    TSNode type_node = ts_node_child_by_field_name(node, TS_FIELD("type"));
    if (ts_node_is_null(type_node)) {
        return;
    }

    char *type_name = cbm_node_text(a, type_node, ctx->source);
    if (!type_name || !type_name[0]) {
        return;
    }

    // Check for "impl Trait for Struct" pattern
    TSNode trait_node = ts_node_child_by_field_name(node, TS_FIELD("trait"));
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
    TSNode body = ts_node_child_by_field_name(node, TS_FIELD("body"));
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
        def.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
        def.is_exported = cbm_is_exported(name, ctx->language);

        TSNode params = ts_node_child_by_field_name(child, TS_FIELD("parameters"));
        if (!ts_node_is_null(params)) {
            def.signature = cbm_node_text(a, params, ctx->source);
            def.param_types = extract_param_types(a, params, ctx->source, ctx->language);
        }

        if (spec->branching_node_types && spec->branching_node_types[0]) {
            def.complexity = cbm_count_branching(child, spec->branching_node_types);
        }

        // MinHash fingerprint
        compute_fingerprint(ctx, &def, child);

        cbm_defs_push(&ctx->result->defs, a, def);
    }
}

// --- Elixir def/defp/defmodule ---

// Get the "arguments" node for an Elixir call, with fallback to second child.
static TSNode elixir_call_args(TSNode node) {
    TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
    if (ts_node_is_null(args) && ts_node_child_count(node) > SECOND_CHILD_IDX) {
        args = ts_node_child(node, SECOND_CHILD_IDX);
    }
    return args;
}

// Handle Elixir def/defp/defmacro — extract function definition.
static void extract_elixir_func_def(CBMExtractCtx *ctx, TSNode node, const char *macro) {
    CBMArena *a = ctx->arena;
    TSNode args = elixir_call_args(node);
    if (ts_node_is_null(args)) {
        return;
    }

    TSNode first_arg = ts_node_child(args, 0);
    if (ts_node_is_null(first_arg)) {
        return;
    }

    const char *fk = ts_node_type(first_arg);
    char *name = NULL;
    if (strcmp(fk, "call") == 0 && ts_node_child_count(first_arg) > 0) {
        name = cbm_node_text(a, ts_node_child(first_arg, 0), ctx->source);
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
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.is_exported = (strcmp(macro, "def") == 0 || strcmp(macro, "defmacro") == 0);
    cbm_defs_push(&ctx->result->defs, a, def);
}

// Emit Class definition for an Elixir defmodule node. Returns do_block or null.
static TSNode emit_elixir_module_class(CBMExtractCtx *ctx, TSNode cur) {
    CBMArena *a = ctx->arena;
    TSNode null_node = {0};
    TSNode args = elixir_call_args(cur);
    if (ts_node_is_null(args)) {
        return null_node;
    }
    TSNode name_node = ts_node_child(args, 0);
    if (ts_node_is_null(name_node)) {
        return null_node;
    }
    char *name = cbm_node_text(a, name_node, ctx->source);
    if (!name || !name[0]) {
        return null_node;
    }
    CBMDefinition def;
    memset(&def, 0, sizeof(def));
    def.name = name;
    def.qualified_name = cbm_fqn_compute(a, ctx->project, ctx->rel_path, name);
    def.label = "Class";
    def.file_path = ctx->rel_path;
    def.start_line = ts_node_start_point(cur).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(cur).row + TS_LINE_OFFSET;
    def.is_exported = true;
    cbm_defs_push(&ctx->result->defs, a, def);
    return cbm_find_child_by_kind(cur, "do_block");
}

// Process Elixir call nodes iteratively — handles defmodule/def/defp/defmacro
// without recursion between extract_elixir_call ↔ extract_elixir_module_def.
#define ELIXIR_STACK_CAP CBM_SZ_64
static void extract_elixir_call(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    (void)spec;
    TSNode stack[ELIXIR_STACK_CAP];
    int top = 0;
    stack[top++] = node;

    while (top > 0) {
        TSNode cur = stack[--top];
        CBMArena *a = ctx->arena;

        if (ts_node_child_count(cur) == 0) {
            continue;
        }
        TSNode callee = ts_node_child(cur, 0);
        if (ts_node_is_null(callee)) {
            continue;
        }
        char *macro = cbm_node_text(a, callee, ctx->source);
        if (!macro) {
            continue;
        }

        if (strcmp(macro, "def") == 0 || strcmp(macro, "defp") == 0 ||
            strcmp(macro, "defmacro") == 0) {
            extract_elixir_func_def(ctx, cur, macro);
        } else if (strcmp(macro, "defmodule") == 0) {
            TSNode do_block = emit_elixir_module_class(ctx, cur);
            if (!ts_node_is_null(do_block)) {
                uint32_t dbc = ts_node_child_count(do_block);
                for (int di = (int)dbc - SKIP_CHAR; di >= 0 && top < ELIXIR_STACK_CAP; di--) {
                    TSNode dchild = ts_node_child(do_block, (uint32_t)di);
                    if (!ts_node_is_null(dchild) && strcmp(ts_node_type(dchild), "call") == 0) {
                        stack[top++] = dchild;
                    }
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
    def.start_line = ts_node_start_point(node).row + TS_LINE_OFFSET;
    def.end_line = ts_node_end_point(node).row + TS_LINE_OFFSET;
    def.is_exported = cbm_is_exported(name, ctx->language);
    cbm_defs_push(&ctx->result->defs, a, def);
}

// Helper: extract name from a declarator chain (C/C++/ObjC)
// declaration > init_declarator > declarator (may be pointer_declarator > identifier)
static const char *extract_c_declarator_name(CBMArena *a, TSNode decl, const char *source) {
    // Try "declarator" field on the declaration
    TSNode declarator = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
    if (ts_node_is_null(declarator)) {
        return NULL;
    }

    // Could be init_declarator wrapping the actual declarator
    const char *dk = ts_node_type(declarator);
    if (strcmp(dk, "init_declarator") == 0) {
        declarator = ts_node_child_by_field_name(declarator, TS_FIELD("declarator"));
        if (ts_node_is_null(declarator)) {
            return NULL;
        }
        dk = ts_node_type(declarator);
    }
    // Unwrap pointer_declarator
    while (strcmp(dk, "pointer_declarator") == 0 || strcmp(dk, "reference_declarator") == 0) {
        declarator = ts_node_child_by_field_name(declarator, TS_FIELD("declarator"));
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
    TSNode declarator = ts_node_child_by_field_name(field, TS_FIELD("declarator"));
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
    TSNode name = ts_node_child_by_field_name(declarator, TS_FIELD("name"));
    if (!ts_node_is_null(name)) {
        return cbm_node_text(a, name, source);
    }
    return NULL;
}

/* ── Variable name extractors by language group ─────────────────── */

// C# variable extraction: handle field_declaration with nested variable_declaration.
static void extract_csharp_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    const char *fname = extract_java_field_name(a, node, ctx->source);
    if (fname) {
        push_var_def(ctx, fname, node);
        return;
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "variable_declaration") != 0) {
            continue;
        }
        uint32_t nc = ts_node_named_child_count(child);
        for (uint32_t j = 0; j < nc; j++) {
            TSNode decl = ts_node_named_child(child, j);
            if (strcmp(ts_node_type(decl), "variable_declarator") == 0) {
                TSNode id = ts_node_child_by_field_name(decl, TS_FIELD("name"));
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

// JS/TS variable extraction: skip function-assigned declarators.
static void extract_js_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "variable_declarator") != 0) {
            continue;
        }
        TSNode value = ts_node_child_by_field_name(child, TS_FIELD("value"));
        if (!ts_node_is_null(value)) {
            const char *vk = ts_node_type(value);
            if (strcmp(vk, "arrow_function") == 0 || strcmp(vk, "function_expression") == 0 ||
                strcmp(vk, "generator_function") == 0) {
                continue;
            }
        }
        TSNode vname = ts_node_child_by_field_name(child, TS_FIELD("name"));
        if (!ts_node_is_null(vname)) {
            push_var_def(ctx, cbm_node_text(a, vname, ctx->source), child);
        }
    }
}

static void extract_vars_mainstream(CBMExtractCtx *ctx, TSNode node, CBMArena *a,
                                    const char *kind) {
    (void)kind;
    switch (ctx->language) {
    case CBM_LANG_PYTHON: {
        TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
        if (!ts_node_is_null(left) && strcmp(ts_node_type(left), "identifier") == 0) {
            push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_GO: {
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "var_spec") == 0 || strcmp(ck, "const_spec") == 0) {
                TSNode vname = ts_node_child_by_field_name(child, TS_FIELD("name"));
                if (!ts_node_is_null(vname)) {
                    push_var_def(ctx, cbm_node_text(a, vname, ctx->source), child);
                }
            }
        }
        break;
    }
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        extract_js_vars(ctx, node, a);
        break;
    case CBM_LANG_JAVA: {
        const char *fname = extract_java_field_name(a, node, ctx->source);
        if (fname) {
            push_var_def(ctx, fname, node);
        }
        break;
    }
    case CBM_LANG_CSHARP:
        extract_csharp_vars(ctx, node, a);
        break;
    case CBM_LANG_CPP:
    case CBM_LANG_C:
    case CBM_LANG_OBJC: {
        const char *vname = extract_c_declarator_name(a, node, ctx->source);
        if (vname) {
            push_var_def(ctx, vname, node);
        }
        break;
    }
    case CBM_LANG_RUST: {
        TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    default:
        break;
    }
}

// Lua variable extraction: handle assignment_statement with function-def filtering.
static void extract_lua_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "assignment_statement") != 0) {
            continue;
        }
        TSNode expr_list = cbm_find_child_by_kind(child, "expression_list");
        if (!ts_node_is_null(expr_list) && ts_node_named_child_count(expr_list) > 0) {
            TSNode val = ts_node_named_child(expr_list, 0);
            if (!ts_node_is_null(val) && strcmp(ts_node_type(val), "function_definition") == 0) {
                continue;
            }
        }
        TSNode vars = ts_node_child_by_field_name(child, TS_FIELD("variables"));
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

// Strip Perl sigil ($, @, %) from name.
static char *strip_perl_sigil(char *name) {
    if (name && (name[0] == '$' || name[0] == '@' || name[0] == '%')) {
        return name + SKIP_CHAR;
    }
    return name;
}

// Check if a node type is a Perl variable type.
static bool is_perl_var_type(const char *ck) {
    return strcmp(ck, "scalar_variable") == 0 || strcmp(ck, "array_variable") == 0 ||
           strcmp(ck, "hash_variable") == 0 || strcmp(ck, "variable_declarator") == 0 ||
           strcmp(ck, "scalar") == 0 || strcmp(ck, "array") == 0 || strcmp(ck, "hash") == 0;
}

// Perl variable extraction: handle direct variable nodes and assignment_expression.
static void extract_perl_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);
        if (is_perl_var_type(ck)) {
            push_var_def(ctx, strip_perl_sigil(cbm_node_text(a, child, ctx->source)), node);
            return;
        }
        if (strcmp(ck, "assignment_expression") != 0) {
            continue;
        }
        TSNode left = ts_node_child_by_field_name(child, TS_FIELD("left"));
        if (ts_node_is_null(left) && ts_node_named_child_count(child) > 0) {
            left = ts_node_named_child(child, 0);
        }
        if (ts_node_is_null(left)) {
            continue;
        }
        if (strcmp(ts_node_type(left), "variable_declaration") == 0) {
            uint32_t lnc = ts_node_named_child_count(left);
            for (uint32_t li = 0; li < lnc; li++) {
                TSNode var_node = ts_node_named_child(left, li);
                if (is_perl_var_type(ts_node_type(var_node))) {
                    left = var_node;
                    break;
                }
            }
        }
        push_var_def(ctx, strip_perl_sigil(cbm_node_text(a, left, ctx->source)), node);
        return;
    }
}

// R variable extraction: skip function-definitions, then extract left/lhs.
static void extract_r_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t rnc = ts_node_named_child_count(node);
    for (uint32_t ri = 0; ri < rnc; ri++) {
        TSNode rch = ts_node_named_child(node, ri);
        if (!ts_node_is_null(rch) && strcmp(ts_node_type(rch), "function_definition") == 0) {
            return;
        }
    }
    TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
    if (ts_node_is_null(left)) {
        left = ts_node_child_by_field_name(node, TS_FIELD("lhs"));
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

// PHP variable extraction from expression_statement.
static void extract_php_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a, const char *kind) {
    if (strcmp(kind, "expression_statement") != 0) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t j = 0; j < nc; j++) {
        TSNode inner = ts_node_named_child(node, j);
        if (strcmp(ts_node_type(inner), "assignment_expression") == 0) {
            TSNode left = ts_node_child_by_field_name(inner, TS_FIELD("left"));
            if (!ts_node_is_null(left)) {
                char *name = cbm_node_text(a, left, ctx->source);
                if (name && name[0] == '$') {
                    name++;
                }
                push_var_def(ctx, name, node);
            }
        }
    }
}

static void extract_vars_dynamic(CBMExtractCtx *ctx, TSNode node, CBMArena *a, const char *kind) {
    switch (ctx->language) {
    case CBM_LANG_PHP:
        extract_php_vars(ctx, node, a, kind);
        break;
    case CBM_LANG_LUA:
        extract_lua_vars(ctx, node, a);
        break;
    case CBM_LANG_RUBY: {
        TSNode left = ts_node_child_by_field_name(node, TS_FIELD("left"));
        if (!ts_node_is_null(left)) {
            const char *lk = ts_node_type(left);
            if (strcmp(lk, "identifier") == 0 || strcmp(lk, "constant") == 0) {
                push_var_def(ctx, cbm_node_text(a, left, ctx->source), node);
            }
        }
        break;
    }
    case CBM_LANG_R:
        extract_r_vars(ctx, node, a);
        break;
    case CBM_LANG_PERL:
        extract_perl_vars(ctx, node, a);
        break;
    default:
        break;
    }
}

// Kotlin variable name resolution: name > simple_identifier > identifier > variable_declaration.
static TSNode resolve_kotlin_var_name(TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }
    name_node = cbm_find_child_by_kind(node, "simple_identifier");
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }
    name_node = cbm_find_child_by_kind(node, "identifier");
    if (!ts_node_is_null(name_node)) {
        return name_node;
    }
    TSNode var_decl = cbm_find_child_by_kind(node, "variable_declaration");
    if (!ts_node_is_null(var_decl)) {
        name_node = cbm_find_child_by_kind(var_decl, "simple_identifier");
        if (!ts_node_is_null(name_node)) {
            return name_node;
        }
        return cbm_find_child_by_kind(var_decl, "identifier");
    }
    TSNode null_node = {0};
    return null_node;
}

static void extract_vars_jvm(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    switch (ctx->language) {
    case CBM_LANG_SCALA: {
        TSNode pattern = ts_node_child_by_field_name(node, TS_FIELD("pattern"));
        if (!ts_node_is_null(pattern)) {
            push_var_def(ctx, cbm_node_text(a, pattern, ctx->source), node);
        } else {
            TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
            if (!ts_node_is_null(name_node)) {
                push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
            }
        }
        break;
    }
    case CBM_LANG_KOTLIN: {
        TSNode name_node = resolve_kotlin_var_name(node);
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_GROOVY: {
        TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (ts_node_is_null(name_node)) {
            const char *cname = extract_c_declarator_name(a, node, ctx->source);
            if (cname) {
                push_var_def(ctx, cname, node);
                break;
            }
            name_node = cbm_find_child_by_kind(node, "identifier");
        }
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        }
        break;
    }
    default:
        break;
    }
}

// Trim leading/trailing whitespace from a name in-place.
static char *trim_whitespace(char *name) {
    if (!name) {
        return name;
    }
    while (*name == ' ' || *name == '\t') {
        name++;
    }
    size_t nlen = strlen(name);
    while (nlen > 0 && (name[nlen - SKIP_CHAR] == ' ' || name[nlen - SKIP_CHAR] == '\t')) {
        name[nlen - SKIP_CHAR] = '\0';
        nlen--;
    }
    return name;
}

// INI variable extraction: find setting_name/name child, with fallback to first child.
static void extract_ini_vars(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "setting_name") == 0 || strcmp(ck, "name") == 0) {
            push_var_def(ctx, trim_whitespace(cbm_node_text(a, child, ctx->source)), node);
            return;
        }
    }
    if (nc > 0) {
        bool found_name = false;
        for (uint32_t i = 0; i < nc; i++) {
            const char *ck = ts_node_type(ts_node_child(node, i));
            if (strcmp(ck, "setting_name") == 0 || strcmp(ck, "name") == 0) {
                found_name = true;
                break;
            }
        }
        if (!found_name) {
            push_var_def(
                ctx, trim_whitespace(cbm_node_text(a, ts_node_child(node, 0), ctx->source)), node);
        }
    }
}

// Find first named child matching one of the given types and push as var def.
static void push_first_matching_child(CBMExtractCtx *ctx, TSNode node, CBMArena *a,
                                      const char **match_types) {
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);
        for (const char **t = match_types; *t; t++) {
            if (strcmp(ck, *t) == 0) {
                push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
                return;
            }
        }
    }
}

// JSON variable extraction: strip quotes from key.
static void extract_json_var(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    TSNode key_node = ts_node_child_by_field_name(node, TS_FIELD("key"));
    if (ts_node_is_null(key_node)) {
        return;
    }
    char *raw = cbm_node_text(a, key_node, ctx->source);
    if (raw) {
        size_t rlen = strlen(raw);
        if (rlen >= PAIR_CHARS && raw[0] == '"' && raw[rlen - SKIP_CHAR] == '"') {
            raw[rlen - SKIP_CHAR] = '\0';
            raw++;
        }
        push_var_def(ctx, raw, node);
    }
}

// SCSS variable extraction: try property > name > property_name > variable_name.
static void extract_scss_var(CBMExtractCtx *ctx, TSNode node, CBMArena *a) {
    TSNode prop = ts_node_child_by_field_name(node, TS_FIELD("property"));
    if (ts_node_is_null(prop)) {
        prop = ts_node_child_by_field_name(node, TS_FIELD("name"));
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
}

static void extract_vars_config(CBMExtractCtx *ctx, TSNode node, CBMArena *a, const char *kind) {
    switch (ctx->language) {
    case CBM_LANG_YAML: {
        TSNode key = ts_node_child_by_field_name(node, TS_FIELD("key"));
        if (!ts_node_is_null(key)) {
            push_var_def(ctx, cbm_node_text(a, key, ctx->source), node);
        }
        break;
    }
    case CBM_LANG_TOML: {
        char *name = find_toml_key_name(a, node, ctx->source);
        if (name) {
            push_var_def(ctx, name, node);
        }
        break;
    }
    case CBM_LANG_JSON:
        extract_json_var(ctx, node, a);
        break;
    case CBM_LANG_INI:
        extract_ini_vars(ctx, node, a);
        break;
    case CBM_LANG_ERLANG: {
        if (strcmp(kind, "pp_define") == 0 || strcmp(kind, "record_decl") == 0) {
            static const char *erlang_var_types[] = {"atom", "var", "macro_lhs", NULL};
            push_first_matching_child(ctx, node, a, erlang_var_types);
        }
        break;
    }
    case CBM_LANG_SQL: {
        static const char *sql_var_types[] = {"identifier", "object_reference", NULL};
        push_first_matching_child(ctx, node, a, sql_var_types);
        break;
    }
    case CBM_LANG_BASH: {
        TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
        if (!ts_node_is_null(name_node)) {
            push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        } else {
            static const char *bash_var_types[] = {"variable_name", "word", NULL};
            push_first_matching_child(ctx, node, a, bash_var_types);
        }
        break;
    }
    case CBM_LANG_SCSS:
        extract_scss_var(ctx, node, a);
        break;
    default:
        break;
    }
}

/* ── Variable name extraction dispatcher ────────────────────────── */

static void extract_var_names(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec) {
    (void)spec;
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);

    switch (ctx->language) {
    /* Mainstream + C-family + Rust */
    case CBM_LANG_PYTHON:
    case CBM_LANG_GO:
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
    case CBM_LANG_JAVA:
    case CBM_LANG_CSHARP:
    case CBM_LANG_CPP:
    case CBM_LANG_C:
    case CBM_LANG_OBJC:
    case CBM_LANG_RUST:
        extract_vars_mainstream(ctx, node, a, kind);
        return;
    /* Dynamic/scripting */
    case CBM_LANG_PHP:
    case CBM_LANG_LUA:
    case CBM_LANG_RUBY:
    case CBM_LANG_R:
    case CBM_LANG_PERL:
        extract_vars_dynamic(ctx, node, a, kind);
        return;
    /* JVM (non-Java) */
    case CBM_LANG_SCALA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_GROOVY:
        extract_vars_jvm(ctx, node, a);
        return;
    /* Config + other */
    case CBM_LANG_YAML:
    case CBM_LANG_TOML:
    case CBM_LANG_JSON:
    case CBM_LANG_INI:
    case CBM_LANG_ERLANG:
    case CBM_LANG_SQL:
    case CBM_LANG_BASH:
    case CBM_LANG_SCSS:
        extract_vars_config(ctx, node, a, kind);
        return;
    default:
        break;
    }

    /* Default fallback: name field → C-declarator → first identifier */
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (!ts_node_is_null(name_node)) {
        push_var_def(ctx, cbm_node_text(a, name_node, ctx->source), node);
        return;
    }
    const char *cname = extract_c_declarator_name(a, node, ctx->source);
    if (cname) {
        push_var_def(ctx, cname, node);
        return;
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(child), "identifier") == 0) {
            push_var_def(ctx, cbm_node_text(a, child, ctx->source), node);
            return;
        }
    }
}

// Iterative variable walker for config languages with nested structure.
// Used by YAML, TOML, INI, JSON.
#define VAR_WALK_STACK_CAP CBM_SZ_256
static void walk_variables_iter(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec) {
    TSNode stack[VAR_WALK_STACK_CAP];
    int top = 0;
    stack[top++] = root;

    while (top > 0) {
        TSNode node = stack[--top];
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_CHAR; i >= 0; i--) {
            TSNode child = ts_node_child(node, (uint32_t)i);
            if (ts_node_is_null(child)) {
                continue;
            }
            if (cbm_kind_in_set(child, spec->variable_node_types)) {
                if (cbm_is_module_level(child, ctx->language)) {
                    extract_var_names(ctx, child, spec);
                }
            }
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "document") == 0 || strcmp(ck, "block_node") == 0 ||
                strcmp(ck, "block_mapping") == 0 || strcmp(ck, "stream") == 0 ||
                strcmp(ck, "table") == 0 || strcmp(ck, "table_array_element") == 0 ||
                strcmp(ck, "section") == 0 || strcmp(ck, "object") == 0 ||
                strcmp(ck, "array") == 0 || strcmp(ck, "pair") == 0 || strcmp(ck, "element") == 0 ||
                strcmp(ck, "content") == 0) {
                if (top < VAR_WALK_STACK_CAP) {
                    stack[top++] = child;
                }
            }
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
        walk_variables_iter(ctx, root, spec);
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
// Check if a field_declaration has a function-pointer declarator chain.
static bool is_func_ptr_field(TSNode field) {
    TSNode decl = ts_node_child_by_field_name(field, TS_FIELD("declarator"));
    for (int depth = 0; depth < C_RETURN_WALK_DEPTH && !ts_node_is_null(decl); depth++) {
        if (strcmp(ts_node_type(decl), "function_declarator") == 0) {
            return true;
        }
        TSNode inner = ts_node_child_by_field_name(decl, TS_FIELD("declarator"));
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
    return false;
}

// Resolve the name node for a field declaration, unwrapping C pointer/array declarators.
static TSNode resolve_field_name_node(TSNode child) {
    TSNode name_node = ts_node_child_by_field_name(child, TS_FIELD("declarator"));
    if (ts_node_is_null(name_node)) {
        name_node = ts_node_child_by_field_name(child, TS_FIELD("name"));
    }
    if (ts_node_is_null(name_node)) {
        TSNode null_node = {0};
        return null_node;
    }
    const char *nk = ts_node_type(name_node);
    if (strcmp(nk, "pointer_declarator") == 0 || strcmp(nk, "array_declarator") == 0) {
        TSNode inner = ts_node_child_by_field_name(name_node, TS_FIELD("declarator"));
        if (!ts_node_is_null(inner)) {
            return inner;
        }
        TSNode null_node = {0};
        return null_node;
    }
    return name_node;
}

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

        if (is_func_ptr_field(child)) {
            continue;
        }

        // Extract type from "type" field
        TSNode type_node = ts_node_child_by_field_name(child, TS_FIELD("type"));
        if (ts_node_is_null(type_node)) {
            continue;
        }
        char *type_text = cbm_node_text(a, type_node, ctx->source);
        if (!type_text || !type_text[0]) {
            continue;
        }

        TSNode name_node = resolve_field_name_node(child);
        if (ts_node_is_null(name_node)) {
            continue;
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
        def.start_line = ts_node_start_point(child).row + TS_LINE_OFFSET;
        def.end_line = ts_node_end_point(child).row + TS_LINE_OFFSET;
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

// Iterative walk_defs — explicit stack with enclosing class context per frame.
typedef struct {
    TSNode node;
    const char *enclosing_class_qn; // saved context for class nesting
} walk_defs_frame_t;

#define CBM_WALK_DEFS_STACK_CAP 4096

// Push nested class nodes from a class body container onto the defs stack.
// Iteratively walks into wrapper nodes (field_declaration, template_declaration).
static void push_nested_class_nodes(TSNode body, const CBMLangSpec *spec, walk_defs_frame_t *stack,
                                    int *top, const char *enclosing_qn) {
    TSNode nc_stack[NESTED_CLASS_STACK_CAP];
    int nc_top = 0;
    nc_stack[nc_top++] = body;

    while (nc_top > 0) {
        TSNode cur = nc_stack[--nc_top];
        uint32_t nc = ts_node_child_count(cur);
        for (int i = (int)nc - SKIP_CHAR; i >= 0; i--) {
            TSNode child = ts_node_child(cur, (uint32_t)i);
            if (cbm_kind_in_set(child, spec->class_node_types)) {
                if (*top < CBM_WALK_DEFS_STACK_CAP) {
                    stack[(*top)++] = (walk_defs_frame_t){child, enclosing_qn};
                }
            } else {
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "field_declaration") == 0 ||
                    strcmp(ck, "template_declaration") == 0 || strcmp(ck, "declaration") == 0) {
                    if (nc_top < NESTED_CLASS_STACK_CAP) {
                        nc_stack[nc_top++] = child;
                    }
                }
            }
        }
    }
}

// Check if a C++/CUDA template_declaration wraps a class/struct/union (not a function).
static bool is_template_class_node(TSNode node, CBMLanguage lang) {
    if ((lang != CBM_LANG_CPP && lang != CBM_LANG_CUDA) ||
        strcmp(ts_node_type(node), "template_declaration") != 0) {
        return false;
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        const char *ck = ts_node_type(ts_node_named_child(node, i));
        if (strcmp(ck, "class_specifier") == 0 || strcmp(ck, "struct_specifier") == 0 ||
            strcmp(ck, "union_specifier") == 0) {
            return true;
        }
    }
    return false;
}

// Compute the enclosing class QN for a class node (for nested class context).
static const char *compute_class_qn(CBMExtractCtx *ctx, TSNode node, const char *saved_enclosing) {
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
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
                return cbm_arena_sprintf(ctx->arena, "%s.%s", saved_enclosing, cname);
            }
            return cbm_fqn_compute(ctx->arena, ctx->project, ctx->rel_path, cname);
        }
    }
    return saved_enclosing;
}

// Push nested class children from a class body container onto the walk stack.
static void push_class_body_children(TSNode node, const CBMLangSpec *spec, walk_defs_frame_t *stack,
                                     int *top, const char *new_enclosing) {
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t ci = 0; ci < nc; ci++) {
        TSNode child = ts_node_child(node, ci);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "field_declaration_list") == 0 || strcmp(ck, "class_body") == 0 ||
            strcmp(ck, "declaration_list") == 0 || strcmp(ck, "body") == 0 ||
            strcmp(ck, "block") == 0 || strcmp(ck, "suite") == 0) {
            push_nested_class_nodes(child, spec, stack, top, new_enclosing);
            return;
        }
    }
    // No body found — push all children directly
    for (int ci = (int)nc - SKIP_CHAR; ci >= 0 && *top < CBM_WALK_DEFS_STACK_CAP; ci--) {
        stack[(*top)++] = (walk_defs_frame_t){ts_node_child(node, (uint32_t)ci), new_enclosing};
    }
}

static void walk_defs(CBMExtractCtx *ctx, TSNode root, const CBMLangSpec *spec, int depth_unused) {
    (void)depth_unused;
    walk_defs_frame_t stack[CBM_WALK_DEFS_STACK_CAP];
    int top = 0;
    stack[top++] = (walk_defs_frame_t){root, ctx->enclosing_class_qn};

    while (top > 0) {
        walk_defs_frame_t frame = stack[--top];
        TSNode node = frame.node;
        ctx->enclosing_class_qn = frame.enclosing_class_qn;
        const char *kind = ts_node_type(node);

        if (ctx->language == CBM_LANG_ELIXIR && strcmp(kind, "call") == 0) {
            extract_elixir_call(ctx, node, spec);
            continue;
        }

        if (cbm_kind_in_set(node, spec->function_node_types)) {
            if (!is_template_class_node(node, ctx->language)) {
                extract_func_def(ctx, node, spec);
                if (ctx->language != CBM_LANG_WOLFRAM) {
                    continue;
                }
            }
        }

        if (ctx->language == CBM_LANG_RUST && strcmp(kind, "impl_item") == 0) {
            extract_rust_impl(ctx, node, spec);
            continue;
        }

        if (cbm_kind_in_set(node, spec->class_node_types)) {
            extract_class_def(ctx, node, spec);
            const char *new_enclosing = compute_class_qn(ctx, node, frame.enclosing_class_qn);
            push_class_body_children(node, spec, stack, &top, new_enclosing);
            continue;
        }

        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_CHAR; i >= 0 && top < CBM_WALK_DEFS_STACK_CAP; i--) {
            stack[top++] =
                (walk_defs_frame_t){ts_node_child(node, (uint32_t)i), frame.enclosing_class_qn};
        }
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
    mod.start_line = FIRST_LINE;
    mod.end_line = ts_node_end_point(ctx->root).row + TS_LINE_OFFSET;
    mod.is_exported = true;
    mod.is_test = ctx->result->is_test_file;
    cbm_defs_push(&ctx->result->defs, a, mod);

    // Walk AST for function/class definitions
    walk_defs(ctx, ctx->root, spec, 0);

    // Extract module-level variables
    extract_variables(ctx, ctx->root, spec);
}
