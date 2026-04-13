#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_strdup/strndup/sprintf
#include "helpers.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"
#include <stdint.h> // uint32_t
#include <string.h>
#include <ctype.h>

/* Local constants for magic number elimination. */
enum {
    USE_PREFIX_LEN = 4, /* strlen("use ") */
    MIN_WOLFRAM_CHILDREN = 2,
    SECOND_IDX = 1,
};

// Field name length for ts_node_child_by_field_name() calls.
#define FIELD_LEN_MODULE_NAME 11 // strlen("module_name")

// Forward declarations
static void parse_go_imports(CBMExtractCtx *ctx);
static void parse_python_imports(CBMExtractCtx *ctx);
static void parse_es_imports(CBMExtractCtx *ctx);
static void parse_java_imports(CBMExtractCtx *ctx);
static void parse_rust_imports(CBMExtractCtx *ctx);
static void parse_c_imports(CBMExtractCtx *ctx);
static void parse_ruby_imports(CBMExtractCtx *ctx);
static void parse_lua_imports(CBMExtractCtx *ctx);
static void parse_generic_imports(CBMExtractCtx *ctx, const char *node_type);
static void parse_wolfram_imports(CBMExtractCtx *ctx);

// Helper: strip quotes from a string literal
static char *strip_quotes(CBMArena *a, const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    if (len >= CBM_QUOTE_PAIR && (s[0] == '"' || s[0] == '\'') && s[len - SKIP_ONE] == s[0]) {
        return cbm_arena_strndup(a, s + SKIP_ONE, len - PAIR_LEN);
    }
    return cbm_arena_strdup(a, s);
}

// Helper: get last path component as local name
static const char *path_last(CBMArena *a, const char *path) {
    if (!path) {
        return NULL;
    }
    const char *last = strrchr(path, '/');
    if (last) {
        return cbm_arena_strdup(a, last + SKIP_ONE);
    }
    last = strrchr(path, '.');
    if (last) {
        return cbm_arena_strdup(a, last + SKIP_ONE);
    }
    return path;
}

// --- Go imports ---
// import_declaration -> import_spec_list -> import_spec -> (name, path)

// Parse a single Go import_spec node.
static void parse_go_import_spec(CBMExtractCtx *ctx, TSNode spec) {
    CBMArena *a = ctx->arena;
    TSNode path_node = ts_node_child_by_field_name(spec, TS_FIELD("path"));
    if (ts_node_is_null(path_node)) {
        return;
    }
    char *path = strip_quotes(a, cbm_node_text(a, path_node, ctx->source));
    if (!path || !path[0]) {
        return;
    }

    TSNode name_node = ts_node_child_by_field_name(spec, TS_FIELD("name"));
    const char *local_name =
        !ts_node_is_null(name_node) ? cbm_node_text(a, name_node, ctx->source) : path_last(a, path);

    CBMImport imp = {.local_name = local_name, .module_path = path};
    cbm_imports_push(&ctx->result->imports, a, imp);
}

static void parse_go_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode decl = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(decl), "import_declaration") != 0) {
            continue;
        }

        uint32_t dc = ts_node_child_count(decl);
        for (uint32_t j = 0; j < dc; j++) {
            TSNode child = ts_node_child(decl, j);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "import_spec") == 0) {
                parse_go_import_spec(ctx, child);
            } else if (strcmp(ck, "import_spec_list") == 0) {
                uint32_t sc = ts_node_child_count(child);
                for (uint32_t k = 0; k < sc; k++) {
                    TSNode spec = ts_node_child(child, k);
                    if (strcmp(ts_node_type(spec), "import_spec") == 0) {
                        parse_go_import_spec(ctx, spec);
                    }
                }
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Python imports ---
// import_statement: import X, import X as Y
// import_from_statement: from X import Y, from X import Y as Z

// Emit a Python aliased_import (import X as Y / from X import Y as Z).
static void emit_py_aliased_import(CBMExtractCtx *ctx, TSNode child, const char *mod_prefix) {
    CBMArena *a = ctx->arena;
    TSNode mod_node = ts_node_child_by_field_name(child, TS_FIELD("name"));
    TSNode alias_node = ts_node_child_by_field_name(child, TS_FIELD("alias"));
    if (ts_node_is_null(mod_node)) {
        return;
    }
    char *name = cbm_node_text(a, mod_node, ctx->source);
    if (!name || !name[0]) {
        return;
    }
    const char *local = !ts_node_is_null(alias_node) ? cbm_node_text(a, alias_node, ctx->source)
                                                     : path_last(a, name);
    const char *full = mod_prefix ? cbm_arena_sprintf(a, "%s.%s", mod_prefix, name) : name;
    CBMImport imp = {.local_name = local, .module_path = full};
    cbm_imports_push(&ctx->result->imports, a, imp);
}

// Process a single Python import_statement node (import X, import X as Y).
static void process_py_import_stmt(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    TSNode name_node = ts_node_child_by_field_name(node, TS_FIELD("name"));
    if (ts_node_is_null(name_node)) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t j = 0; j < nc; j++) {
            TSNode child = ts_node_child(node, j);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "dotted_name") == 0 || strcmp(ck, "identifier") == 0) {
                char *mod = cbm_node_text(a, child, ctx->source);
                if (mod && mod[0]) {
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            } else if (strcmp(ck, "aliased_import") == 0) {
                emit_py_aliased_import(ctx, child, NULL);
            }
        }
    } else {
        char *mod = cbm_node_text(a, name_node, ctx->source);
        if (mod && mod[0]) {
            CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
            cbm_imports_push(&ctx->result->imports, a, imp);
        }
    }
}

// Resolve the module_name node for a Python import_from_statement.
static TSNode resolve_py_module_node(TSNode node) {
    TSNode module_node = ts_node_child_by_field_name(node, "module_name", FIELD_LEN_MODULE_NAME);
    if (ts_node_is_null(module_node)) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t j = 0; j < nc; j++) {
            TSNode c = ts_node_child(node, j);
            if (strcmp(ts_node_type(c), "dotted_name") == 0 ||
                strcmp(ts_node_type(c), "relative_import") == 0) {
                return c;
            }
        }
    }
    return module_node;
}

// Emit a Python import-from name child (identifier/dotted_name).
static void emit_py_import_from_name(CBMExtractCtx *ctx, TSNode child, const char *mod_path) {
    CBMArena *a = ctx->arena;
    char *name = cbm_node_text(a, child, ctx->source);
    if (name && name[0]) {
        const char *full = mod_path ? cbm_arena_sprintf(a, "%s.%s", mod_path, name) : name;
        CBMImport imp = {.local_name = name, .module_path = full};
        cbm_imports_push(&ctx->result->imports, a, imp);
    }
}

// Process a single Python import_from_statement node (from X import Y [as Z]).
static void process_py_import_from(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    TSNode module_node = resolve_py_module_node(node);
    char *mod_path =
        ts_node_is_null(module_node) ? NULL : cbm_node_text(a, module_node, ctx->source);

    uint32_t nc = ts_node_child_count(node);
    for (uint32_t j = 0; j < nc; j++) {
        TSNode child = ts_node_child(node, j);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "identifier") == 0 || strcmp(ck, "dotted_name") == 0) {
            if (!ts_node_is_null(module_node) &&
                ts_node_start_byte(child) == ts_node_start_byte(module_node)) {
                continue;
            }
            emit_py_import_from_name(ctx, child, mod_path);
        } else if (strcmp(ck, "aliased_import") == 0) {
            emit_py_aliased_import(ctx, child, mod_path);
        }
    }
}

static void parse_python_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);

        if (strcmp(kind, "import_statement") == 0) {
            process_py_import_stmt(ctx, node);
        } else if (strcmp(kind, "import_from_statement") == 0) {
            process_py_import_from(ctx, node);
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- ES module imports (JS/TS/TSX) ---
// import X from "Y"; import {A, B} from "Y"; import * as X from "Y"
// const X = require("Y")

// Find the source string node in an ES import_statement.
static TSNode find_es_source_node(TSNode node) {
    TSNode source_node = ts_node_child_by_field_name(node, TS_FIELD("source"));
    if (ts_node_is_null(source_node)) {
        uint32_t nc = ts_node_child_count(node);
        for (int j = (int)nc - SKIP_ONE; j >= 0; j--) {
            TSNode c = ts_node_child(node, (uint32_t)j);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "string") == 0 || strcmp(ck, "string_literal") == 0) {
                return c;
            }
        }
    }
    return source_node;
}

// Process named_imports: import {A, B as C} from "path".
static bool process_named_imports(CBMExtractCtx *ctx, TSNode sub, const char *path) {
    CBMArena *a = ctx->arena;
    bool found = false;
    uint32_t nc2 = ts_node_child_count(sub);
    for (uint32_t m = 0; m < nc2; m++) {
        TSNode imp_spec = ts_node_child(sub, m);
        if (strcmp(ts_node_type(imp_spec), "import_specifier") != 0) {
            continue;
        }
        TSNode local = ts_node_child_by_field_name(imp_spec, TS_FIELD("alias"));
        TSNode orig = ts_node_child_by_field_name(imp_spec, TS_FIELD("name"));
        if (ts_node_is_null(orig) && ts_node_child_count(imp_spec) > 0) {
            orig = ts_node_child(imp_spec, 0);
        }
        if (!ts_node_is_null(orig)) {
            char *local_name = !ts_node_is_null(local) ? cbm_node_text(a, local, ctx->source)
                                                       : cbm_node_text(a, orig, ctx->source);
            CBMImport imp = {.local_name = local_name, .module_path = path};
            cbm_imports_push(&ctx->result->imports, a, imp);
            found = true;
        }
    }
    return found;
}

// Process an import_clause node: default, namespace, and named imports.
static bool process_import_clause(CBMExtractCtx *ctx, TSNode clause, const char *path) {
    CBMArena *a = ctx->arena;
    bool found = false;
    uint32_t cc = ts_node_child_count(clause);
    for (uint32_t k = 0; k < cc; k++) {
        TSNode sub = ts_node_child(clause, k);
        const char *sk = ts_node_type(sub);
        if (strcmp(sk, "identifier") == 0) {
            char *name = cbm_node_text(a, sub, ctx->source);
            CBMImport imp = {.local_name = name, .module_path = path};
            cbm_imports_push(&ctx->result->imports, a, imp);
            found = true;
        } else if (strcmp(sk, "namespace_import") == 0) {
            TSNode as_name = ts_node_child_by_field_name(sub, TS_FIELD("name"));
            if (ts_node_is_null(as_name) && ts_node_child_count(sub) > 0) {
                as_name = ts_node_child(sub, ts_node_child_count(sub) - SKIP_ONE);
            }
            if (!ts_node_is_null(as_name)) {
                char *name = cbm_node_text(a, as_name, ctx->source);
                CBMImport imp = {.local_name = name, .module_path = path};
                cbm_imports_push(&ctx->result->imports, a, imp);
                found = true;
            }
        } else if (strcmp(sk, "named_imports") == 0) {
            if (process_named_imports(ctx, sub, path)) {
                found = true;
            }
        }
    }
    return found;
}

/* Process a single ES import_statement node. Returns true if fully handled. */
static bool process_es_import_statement(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    TSNode source_node = find_es_source_node(node);
    if (ts_node_is_null(source_node)) {
        return false;
    }
    char *path = strip_quotes(a, cbm_node_text(a, source_node, ctx->source));
    if (!path || !path[0]) {
        return false;
    }
    uint32_t nc = ts_node_child_count(node);
    bool found = false;
    for (uint32_t j = 0; j < nc; j++) {
        TSNode child = ts_node_child(node, j);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "identifier") == 0) {
            char *name = cbm_node_text(a, child, ctx->source);
            CBMImport imp = {.local_name = name, .module_path = path};
            cbm_imports_push(&ctx->result->imports, a, imp);
            found = true;
        } else if (strcmp(ck, "import_clause") == 0) {
            if (process_import_clause(ctx, child, path)) {
                found = true;
            }
        }
    }
    if (!found) {
        CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
        cbm_imports_push(&ctx->result->imports, a, imp);
    }
    return true;
}

/* Handle CommonJS `require("path")` call_expression nodes.  The local name
 * is derived from the enclosing variable_declarator / assignment when
 * possible (so `const foo = require('./foo')` emits local_name="foo"),
 * otherwise falls back to the last path component. */
static bool process_commonjs_require(CBMExtractCtx *ctx, TSNode call) {
    CBMArena *a = ctx->arena;
    if (ts_node_child_count(call) < MIN_WOLFRAM_CHILDREN) {
        return false;
    }
    /* Callee must be the identifier "require". */
    TSNode fn = ts_node_child_by_field_name(call, TS_FIELD("function"));
    if (ts_node_is_null(fn)) {
        fn = ts_node_child(call, 0);
    }
    if (ts_node_is_null(fn)) {
        return false;
    }
    const char *fn_kind = ts_node_type(fn);
    if (strcmp(fn_kind, "identifier") != 0) {
        return false;
    }
    char *fn_name = cbm_node_text(a, fn, ctx->source);
    if (!fn_name || strcmp(fn_name, "require") != 0) {
        return false;
    }

    /* First string literal child of the argument list is the module path. */
    TSNode args = ts_node_child_by_field_name(call, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        return false;
    }
    uint32_t argc = ts_node_named_child_count(args);
    char *path = NULL;
    for (uint32_t i = 0; i < argc; i++) {
        TSNode arg = ts_node_named_child(args, i);
        const char *ak = ts_node_type(arg);
        if (strcmp(ak, "string") == 0 || strcmp(ak, "string_literal") == 0 ||
            strcmp(ak, "template_string") == 0) {
            path = strip_quotes(a, cbm_node_text(a, arg, ctx->source));
            break;
        }
    }
    if (!path || !path[0]) {
        return false;
    }

    /* Infer local name from enclosing variable_declarator.  Tree-sitter's JS
     * grammar wraps `const foo = require(..)` as
     *   lexical_declaration → variable_declarator { name: identifier, value: call } */
    const char *local_name = NULL;
    TSNode parent = ts_node_parent(call);
    if (!ts_node_is_null(parent) && strcmp(ts_node_type(parent), "variable_declarator") == 0) {
        TSNode name_node = ts_node_child_by_field_name(parent, TS_FIELD("name"));
        if (!ts_node_is_null(name_node) && strcmp(ts_node_type(name_node), "identifier") == 0) {
            local_name = cbm_node_text(a, name_node, ctx->source);
        }
    }
    if (!local_name) {
        local_name = path_last(a, path);
    }

    CBMImport imp = {.local_name = local_name, .module_path = path};
    cbm_imports_push(&ctx->result->imports, a, imp);
    return true;
}

#define ES_IMPORT_STACK_CAP CBM_SZ_512
static void walk_es_imports(CBMExtractCtx *ctx, TSNode root) {
    TSNode stack[ES_IMPORT_STACK_CAP];
    int top = 0;
    stack[top++] = root;

    while (top > 0) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);
        bool push_children = true;

        if (strcmp(kind, "import_statement") == 0) {
            if (process_es_import_statement(ctx, node)) {
                push_children = false;
            }
        } else if (strcmp(kind, "call_expression") == 0) {
            /* CommonJS require() — only consume the node if we recognized
             * it as a require call; otherwise keep traversing the children. */
            if (process_commonjs_require(ctx, node)) {
                push_children = false;
            }
        }

        if (push_children) {
            uint32_t count = ts_node_child_count(node);
            for (int i = (int)count - SKIP_ONE; i >= 0 && top < ES_IMPORT_STACK_CAP; i--) {
                stack[top++] = ts_node_child(node, (uint32_t)i);
            }
        }
    }
}

static void parse_es_imports(CBMExtractCtx *ctx) {
    walk_es_imports(ctx, ctx->root);
}

// --- Java imports ---
// import_declaration -> scoped_identifier

static void parse_java_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(node), "import_declaration") != 0) {
            continue;
        }

        // Get the full import path (skip "import" and "static" keywords)
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t j = 0; j < nc; j++) {
            TSNode child = ts_node_child(node, j);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "scoped_identifier") == 0 || strcmp(ck, "identifier") == 0) {
                char *path = cbm_node_text(a, child, ctx->source);
                if (path && path[0]) {
                    CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
                break;
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Rust imports ---
// use_declaration -> use_list or scoped_use_list

static void parse_rust_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(node), "use_declaration") != 0) {
            continue;
        }

        char *full = cbm_node_text(a, node, ctx->source);
        if (!full) {
            continue;
        }
        // Strip "use " prefix and trailing ";"
        if (strncmp(full, "use ", USE_PREFIX_LEN) == 0) {
            full += USE_PREFIX_LEN;
        }
        size_t len = strlen(full);
        if (len > 0 && full[len - SKIP_ONE] == ';') {
            full[len - SKIP_ONE] = '\0';
        }

        CBMImport imp = {.local_name = path_last(a, full), .module_path = full};
        cbm_imports_push(&ctx->result->imports, a, imp);
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- C/C++ imports ---
// preproc_include -> path or string_literal

// Find the path node inside a preproc_include/preproc_import node.
static TSNode find_include_path_node(TSNode node) {
    TSNode path_node = ts_node_child_by_field_name(node, TS_FIELD("path"));
    if (ts_node_is_null(path_node)) {
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t j = 0; j < nc; j++) {
            TSNode c = ts_node_child(node, j);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "string_literal") == 0 || strcmp(ck, "system_lib_string") == 0) {
                return c;
            }
        }
    }
    return path_node;
}

// Strip angle brackets from a system include path (<stdio.h> → stdio.h).
static char *strip_angle_brackets(CBMArena *a, char *path) {
    if (path && path[0] == '<') {
        size_t len = strlen(path);
        if (len > SKIP_ONE && path[len - SKIP_ONE] == '>') {
            return cbm_arena_strndup(a, path + SKIP_ONE, len - PAIR_LEN);
        }
    }
    return path;
}

static void parse_c_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "preproc_include") != 0 && strcmp(kind, "preproc_import") != 0) {
            continue;
        }

        TSNode path_node = find_include_path_node(node);
        if (ts_node_is_null(path_node)) {
            continue;
        }

        char *path = strip_quotes(a, cbm_node_text(a, path_node, ctx->source));
        path = strip_angle_brackets(a, path);
        if (!path || !path[0]) {
            continue;
        }

        CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
        cbm_imports_push(&ctx->result->imports, a, imp);
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Ruby imports ---
// call nodes: require("X"), require_relative("X")

// Check if a Ruby call node is a require/require_relative, return method name or NULL.
static const char *ruby_require_method(CBMArena *a, TSNode node, const char *source) {
    TSNode method = ts_node_child_by_field_name(node, TS_FIELD("method"));
    if (ts_node_is_null(method) && ts_node_child_count(node) > 0) {
        method = ts_node_child(node, 0);
    }
    if (ts_node_is_null(method)) {
        return NULL;
    }
    char *name = cbm_node_text(a, method, source);
    if (!name || (strcmp(name, "require") != 0 && strcmp(name, "require_relative") != 0)) {
        return NULL;
    }
    return name;
}

// Extract string argument from a Ruby require/require_relative call.
static char *extract_ruby_require_arg(CBMArena *a, TSNode node, const char *source) {
    TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
    if (ts_node_is_null(args)) {
        if (ts_node_child_count(node) > SECOND_IDX) {
            args = ts_node_child(node, SECOND_IDX);
        }
    }
    if (ts_node_is_null(args)) {
        return NULL;
    }

    uint32_t ac = ts_node_child_count(args);
    for (uint32_t j = 0; j < ac; j++) {
        TSNode c = ts_node_child(args, j);
        const char *ck = ts_node_type(c);
        if (strcmp(ck, "string") == 0 || strcmp(ck, "string_literal") == 0) {
            return strip_quotes(a, cbm_node_text(a, c, source));
        }
    }
    return strip_quotes(a, cbm_node_text(a, args, source));
}

static void parse_ruby_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "call") != 0 && strcmp(kind, "command_call") != 0) {
            continue;
        }

        if (!ruby_require_method(a, node, ctx->source)) {
            continue;
        }

        char *arg_text = extract_ruby_require_arg(a, node, ctx->source);
        if (!arg_text || !arg_text[0]) {
            continue;
        }

        CBMImport imp = {.local_name = path_last(a, arg_text), .module_path = arg_text};
        cbm_imports_push(&ctx->result->imports, a, imp);
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Lua imports ---
// function_call: require("X")

static void parse_lua_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        // Lua: local X = require("Y") → assignment_statement or variable_declaration
        // containing function_call(require, "Y")
        char *text = cbm_node_text(a, node, ctx->source);
        if (!text) {
            continue;
        }
        if (strstr(text, "require") == NULL) {
            continue;
        }

        // Simple extraction: find require("...") pattern in node text
        const char *req = strstr(text, "require");
        if (!req) {
            continue;
        }

        // Find the string argument
        const char *open = strchr(req, '(');
        if (!open) {
            open = strchr(req, '"');
        }
        if (!open) {
            open = strchr(req, '\'');
        }
        if (!open) {
            continue;
        }

        const char *q1 = strchr(open, '"');
        const char *q2 = strchr(open, '\'');
        if (!q1 && !q2) {
            continue;
        }
        const char *start = q1 && (!q2 || q1 < q2) ? q1 : q2;
        char delim = *start;
        start++;
        const char *end = strchr(start, delim);
        if (!end) {
            continue;
        }

        char *mod = cbm_arena_strndup(a, start, (size_t)(end - start));
        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
        cbm_imports_push(&ctx->result->imports, a, imp);
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Generic import parsing for languages with simple import_declaration ---

// Try known field names (path/source/module/name) to extract import path.
static bool try_generic_path_fields(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    static const char *path_fields[] = {"path", "source", "module", "name", NULL};
    for (const char **f = path_fields; *f; f++) {
        TSNode path_node = ts_node_child_by_field_name(node, *f, (uint32_t)strlen(*f));
        if (!ts_node_is_null(path_node)) {
            char *path = strip_quotes(a, cbm_node_text(a, path_node, ctx->source));
            if (path && path[0]) {
                CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                cbm_imports_push(&ctx->result->imports, a, imp);
            }
            return true;
        }
    }
    return false;
}

// Fallback: extract import path from full node text, stripping keyword and semicolon.
static void generic_import_from_text(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    char *text = cbm_node_text(a, node, ctx->source);
    if (!text || !text[0]) {
        return;
    }
    char *space = strchr(text, ' ');
    if (space) {
        text = space + SKIP_ONE;
    }
    size_t len = strlen(text);
    if (len > 0 && text[len - SKIP_ONE] == ';') {
        text[len - SKIP_ONE] = '\0';
    }
    if (text[0]) {
        CBMImport imp = {.local_name = path_last(a, text), .module_path = text};
        cbm_imports_push(&ctx->result->imports, a, imp);
    }
}

static void parse_generic_imports(CBMExtractCtx *ctx, const char *node_type) {
    /* Use TSTreeCursor for O(1)-per-step sibling traversal. */
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(node), node_type) != 0) {
            continue;
        }

        if (!try_generic_path_fields(ctx, node)) {
            generic_import_from_text(ctx, node);
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Wolfram imports ---
// get_top: << "package" (Get["file"])
// apply where first child is builtin_symbol "Needs" with string arg

// Handle Wolfram get_top: << "path" → import.
static void process_wolfram_get_top(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "string") == 0 || strcmp(ck, "user_symbol") == 0) {
            char *text = cbm_node_text(a, child, ctx->source);
            if (text && text[0]) {
                char *path = strip_quotes(a, text);
                CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                cbm_imports_push(&ctx->result->imports, a, imp);
            }
            break;
        }
    }
}

// Handle Wolfram Needs["package`"] — apply where head is builtin_symbol "Needs".
static void process_wolfram_needs(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    if (ts_node_named_child_count(node) < MIN_WOLFRAM_CHILDREN) {
        return;
    }
    TSNode head = ts_node_named_child(node, 0);
    if (strcmp(ts_node_type(head), "builtin_symbol") != 0) {
        return;
    }
    char *name = cbm_node_text(a, head, ctx->source);
    if (!name || strcmp(name, "Needs") != 0) {
        return;
    }
    TSNode arg = ts_node_named_child(node, SECOND_IDX);
    char *text = cbm_node_text(a, arg, ctx->source);
    if (text && text[0]) {
        char *path = strip_quotes(a, text);
        CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
        cbm_imports_push(&ctx->result->imports, a, imp);
    }
}

#define WOLFRAM_IMPORT_STACK_CAP CBM_SZ_512
static void walk_wolfram_imports(CBMExtractCtx *ctx, TSNode root) {
    TSNode stack[WOLFRAM_IMPORT_STACK_CAP];
    int top = 0;
    stack[top++] = root;

    while (top > 0) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);

        if (strcmp(kind, "get_top") == 0) {
            process_wolfram_get_top(ctx, node);
        } else if (strcmp(kind, "apply") == 0) {
            process_wolfram_needs(ctx, node);
        }

        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - SKIP_ONE; i >= 0 && top < WOLFRAM_IMPORT_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

static void parse_wolfram_imports(CBMExtractCtx *ctx) {
    walk_wolfram_imports(ctx, ctx->root);
}

// --- Main dispatch ---

void cbm_extract_imports(CBMExtractCtx *ctx) {
    switch (ctx->language) {
    case CBM_LANG_GO:
        parse_go_imports(ctx);
        break;
    case CBM_LANG_PYTHON:
        parse_python_imports(ctx);
        break;
    case CBM_LANG_JAVASCRIPT:
    case CBM_LANG_TYPESCRIPT:
    case CBM_LANG_TSX:
        parse_es_imports(ctx);
        break;
    case CBM_LANG_JAVA:
        parse_java_imports(ctx);
        break;
    case CBM_LANG_KOTLIN:
        parse_generic_imports(ctx, "import");
        break;
    case CBM_LANG_SCALA:
        parse_generic_imports(ctx, "import_declaration");
        break;
    case CBM_LANG_CSHARP:
        parse_generic_imports(ctx, "using_directive");
        break;
    case CBM_LANG_RUST:
        parse_rust_imports(ctx);
        break;
    case CBM_LANG_C:
    case CBM_LANG_CPP:
    case CBM_LANG_OBJC:
        parse_c_imports(ctx);
        break;
    case CBM_LANG_PHP:
        // PHP uses require/include calls, similar to Ruby
        parse_generic_imports(ctx, "expression_statement");
        break;
    case CBM_LANG_RUBY:
        parse_ruby_imports(ctx);
        break;
    case CBM_LANG_LUA:
        parse_lua_imports(ctx);
        break;
    case CBM_LANG_ELIXIR:
        // Elixir: import/use/alias/require are call nodes
        parse_generic_imports(ctx, "call");
        break;
    case CBM_LANG_BASH:
        // source/. commands
        parse_generic_imports(ctx, "command");
        break;
    case CBM_LANG_ZIG:
        parse_generic_imports(ctx, "builtin_function");
        break;
    case CBM_LANG_ERLANG:
        parse_generic_imports(ctx, "module_attribute");
        break;
    case CBM_LANG_HASKELL:
        parse_generic_imports(ctx, "import");
        break;
    case CBM_LANG_OCAML:
        parse_generic_imports(ctx, "open_module");
        break;
    case CBM_LANG_CSS:
    case CBM_LANG_SCSS:
        parse_generic_imports(ctx, "import_statement");
        break;
    case CBM_LANG_PERL:
        parse_generic_imports(ctx, "use_statement");
        break;
    case CBM_LANG_GROOVY:
        parse_generic_imports(ctx, "groovy_import");
        break;
    case CBM_LANG_SWIFT:
    case CBM_LANG_DART:
        parse_generic_imports(ctx, "import_declaration");
        break;
    case CBM_LANG_LEAN:
        parse_generic_imports(ctx, "import");
        break;
    case CBM_LANG_FORM:
        parse_generic_imports(ctx, "include_directive");
        break;
    case CBM_LANG_MAGMA:
        parse_generic_imports(ctx, "load_statement");
        break;
    case CBM_LANG_WOLFRAM:
        parse_wolfram_imports(ctx);
        break;
    default:
        break;
    }
}
