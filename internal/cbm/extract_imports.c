#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_strdup/strndup/sprintf
#include "helpers.h"
#include "lang_specs.h"      // CBMLangSpec, CBMEmbeddedLangSpec, cbm_lang_spec, cbm_ts_language
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include "foundation/constants.h"
#include "extract_node_stack.h"
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
static void parse_r_imports(CBMExtractCtx *ctx);
static void parse_kotlin_imports(CBMExtractCtx *ctx);
static void parse_dart_imports(CBMExtractCtx *ctx);
static void parse_haskell_imports(CBMExtractCtx *ctx);
static void parse_zig_imports(CBMExtractCtx *ctx);
static void parse_generic_imports(CBMExtractCtx *ctx, const char *node_type);
static void parse_wolfram_imports(CBMExtractCtx *ctx);
static void parse_php_imports(CBMExtractCtx *ctx);
static void parse_csharp_imports(CBMExtractCtx *ctx);
static void parse_spec_imports(CBMExtractCtx *ctx);
static void parse_hare_imports(CBMExtractCtx *ctx);
static void parse_pascal_imports(CBMExtractCtx *ctx);
static void parse_powershell_imports(CBMExtractCtx *ctx);
static void parse_lisp_imports(CBMExtractCtx *ctx);
static void parse_starlark_imports(CBMExtractCtx *ctx);
static void parse_tcl_imports(CBMExtractCtx *ctx);
static void parse_teal_imports(CBMExtractCtx *ctx);
static void parse_zsh_imports(CBMExtractCtx *ctx);
static void parse_css_imports(CBMExtractCtx *ctx);
static void parse_html_imports(CBMExtractCtx *ctx);
static void parse_cmake_imports(CBMExtractCtx *ctx);
static void parse_bitbake_imports(CBMExtractCtx *ctx);
static void parse_kconfig_imports(CBMExtractCtx *ctx);
static void parse_gn_imports(CBMExtractCtx *ctx);
static void parse_just_imports(CBMExtractCtx *ctx);
static void parse_nix_imports(CBMExtractCtx *ctx);
static void parse_jsonnet_imports(CBMExtractCtx *ctx);
static void parse_pkl_imports(CBMExtractCtx *ctx);
static void parse_nickel_imports(CBMExtractCtx *ctx);
static void parse_thrift_imports(CBMExtractCtx *ctx);
static void parse_capnp_imports(CBMExtractCtx *ctx);
static void parse_dlang_imports(CBMExtractCtx *ctx);
static void parse_tablegen_imports(CBMExtractCtx *ctx);
static void parse_crystal_imports(CBMExtractCtx *ctx);
static void parse_fsharp_imports(CBMExtractCtx *ctx);
static void parse_ada_imports(CBMExtractCtx *ctx);
static void parse_elm_imports(CBMExtractCtx *ctx);
static void parse_move_imports(CBMExtractCtx *ctx);
static void parse_smali_imports(CBMExtractCtx *ctx);
static void parse_tlaplus_imports(CBMExtractCtx *ctx);
static void parse_vhdl_imports(CBMExtractCtx *ctx);
static void parse_wit_imports(CBMExtractCtx *ctx);
static void parse_smithy_imports(CBMExtractCtx *ctx);
static void parse_hyprlang_imports(CBMExtractCtx *ctx);

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

// Helper: get last path component as local name.
// Recognizes every separator used across the supported import syntaxes:
//   '/'  (Go / TS / JS paths), '.' (Java / Kotlin / C# / Python dotted names),
//   '::' (Rust / C++ scope), '\\' (PHP namespaces).  The last separator of any
//   kind wins, so "std::collections::HashMap" → "HashMap" and
//   "App\\Http\\Controller" → "Controller".
static const char *path_last(CBMArena *a, const char *path) {
    if (!path) {
        return NULL;
    }
    const char *last_sep = NULL;
    for (const char *p = path; *p; p++) {
        if (*p == '/' || *p == '.' || *p == ':' || *p == '\\') {
            last_sep = p;
        }
    }
    if (last_sep) {
        return cbm_arena_strdup(a, last_sep + SKIP_ONE);
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
    } else if (strcmp(ts_node_type(name_node), "aliased_import") == 0) {
        /* `import util as u` — the import_statement's `name` field points at the
         * aliased_import; extract its real module name (not "util as u"). */
        emit_py_aliased_import(ctx, name_node, NULL);
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
    // `from __future__ import annotations` is a dedicated node type whose first
    // child is the literal `__future__` keyword (an identifier, not a
    // dotted_name).  Emit a single import for `__future__` and return.
    if (strcmp(ts_node_type(node), "future_import_statement") == 0) {
        CBMImport imp = {.local_name = cbm_arena_strdup(a, "__future__"),
                         .module_path = cbm_arena_strdup(a, "__future__")};
        cbm_imports_push(&ctx->result->imports, a, imp);
        return;
    }
    TSNode module_node = resolve_py_module_node(node);
    char *mod_path =
        ts_node_is_null(module_node) ? NULL : cbm_node_text(a, module_node, ctx->source);

    uint32_t nc = ts_node_child_count(node);
    bool emitted = false;
    for (uint32_t j = 0; j < nc; j++) {
        TSNode child = ts_node_child(node, j);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "identifier") == 0 || strcmp(ck, "dotted_name") == 0) {
            if (!ts_node_is_null(module_node) &&
                ts_node_start_byte(child) == ts_node_start_byte(module_node)) {
                continue;
            }
            emit_py_import_from_name(ctx, child, mod_path);
            emitted = true;
        } else if (strcmp(ck, "aliased_import") == 0) {
            emit_py_aliased_import(ctx, child, mod_path);
            emitted = true;
        } else if (strcmp(ck, "wildcard_import") == 0) {
            // `from os.path import *` — the module itself is the import.
            if (mod_path && mod_path[0]) {
                CBMImport imp = {.local_name = path_last(a, mod_path), .module_path = mod_path};
                cbm_imports_push(&ctx->result->imports, a, imp);
                emitted = true;
            }
        }
    }
    // Defensive: a from-import with a module but no recognized name child
    // (grammar variant) still records the module as an import.
    if (!emitted && mod_path && mod_path[0]) {
        CBMImport imp = {.local_name = path_last(a, mod_path), .module_path = mod_path};
        cbm_imports_push(&ctx->result->imports, a, imp);
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
        } else if (strcmp(kind, "import_from_statement") == 0 ||
                   strcmp(kind, "future_import_statement") == 0) {
            // `from __future__ import annotations` is a distinct node type in
            // tree-sitter-python but has the same shape (module + name list).
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

static void walk_es_imports(CBMExtractCtx *ctx, TSNode root) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, root);

    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *kind = ts_node_type(node);
        bool push_children = true;

        if (strcmp(kind, "import_statement") == 0) {
            if (process_es_import_statement(ctx, node)) {
                push_children = false;
            }
        } else if (strcmp(kind, "export_statement") == 0) {
            /* Re-export: `export { x } from './mod'` / `export * from './mod'`.
             * It carries a `source` string just like an import and creates the
             * same module dependency. */
            TSNode src = ts_node_child_by_field_name(node, TS_FIELD("source"));
            if (!ts_node_is_null(src)) {
                char *path = strip_quotes(ctx->arena, cbm_node_text(ctx->arena, src, ctx->source));
                if (path && path[0]) {
                    CBMImport imp = {.local_name = path_last(ctx->arena, path),
                                     .module_path = path};
                    cbm_imports_push(&ctx->result->imports, ctx->arena, imp);
                }
            }
        } else if (strcmp(kind, "call_expression") == 0) {
            /* CommonJS require() — only consume the node if we recognized
             * it as a require call; otherwise keep traversing the children. */
            if (process_commonjs_require(ctx, node)) {
                push_children = false;
            }
        }

        if (push_children) {
            ts_nstack_push_children(&stack, ctx->arena, node);
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

    // Walk for call nodes with "require" or "require_relative"
    // Walk top-level children via O(N) TSTreeCursor traversal
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

// --- R imports: library()/require()/source() + box::use() (#218) ---

// Emit an IMPORTS edge for a module path string (strips a trailing [symbols]
// list and surrounding quotes; uses the last path segment as the local name).
static void r_push_import(CBMExtractCtx *ctx, const char *raw) {
    CBMArena *a = ctx->arena;
    if (!raw || !raw[0]) {
        return;
    }
    char *mod = strip_quotes(a, raw);
    // box::use specs look like "shiny[moduleServer, NS]" or
    // "app/logic/validation[validate_input]" — the module path is the part
    // before the '[' symbol list.
    char *bracket = strchr(mod, '[');
    if (bracket) {
        *bracket = '\0';
    }
    // Trim trailing whitespace left by truncation.
    size_t len = strlen(mod);
    while (len > 0 && (mod[len - SKIP_ONE] == ' ' || mod[len - SKIP_ONE] == '\t' ||
                       mod[len - SKIP_ONE] == '\n')) {
        mod[--len] = '\0';
    }
    if (mod[0] == '\0') {
        return;
    }
    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
    cbm_imports_push(&ctx->result->imports, a, imp);
}

// Recursively scan R for import-producing calls.
static void r_collect_imports(CBMExtractCtx *ctx, TSNode node) { // NOLINT(misc-no-recursion)
    if (strcmp(ts_node_type(node), "call") == 0) {
        TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
        TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
        if (!ts_node_is_null(fn) && !ts_node_is_null(args)) {
            const char *ft = ts_node_type(fn);
            if (strcmp(ft, "namespace_operator") == 0) {
                // box::use(mod[syms], pkg/path[syms], ...) — one IMPORTS per arg.
                TSNode lhs = ts_node_child_by_field_name(fn, TS_FIELD("lhs"));
                TSNode rhs = ts_node_child_by_field_name(fn, TS_FIELD("rhs"));
                char *lt =
                    ts_node_is_null(lhs) ? NULL : cbm_node_text(ctx->arena, lhs, ctx->source);
                char *rt =
                    ts_node_is_null(rhs) ? NULL : cbm_node_text(ctx->arena, rhs, ctx->source);
                if (lt && rt && strcmp(lt, "box") == 0 && strcmp(rt, "use") == 0) {
                    uint32_t na = ts_node_named_child_count(args);
                    for (uint32_t i = 0; i < na; i++) {
                        TSNode arg = ts_node_named_child(args, i);
                        if (strcmp(ts_node_type(arg), "argument") != 0) {
                            continue;
                        }
                        TSNode val = ts_node_child_by_field_name(arg, TS_FIELD("value"));
                        if (!ts_node_is_null(val)) {
                            r_push_import(ctx, cbm_node_text(ctx->arena, val, ctx->source));
                        }
                    }
                }
            } else if (strcmp(ft, "identifier") == 0) {
                // library(pkg) / require(pkg) / requireNamespace("pkg") /
                // loadNamespace("pkg") / source("file.R") — first arg is the module.
                char *fname = cbm_node_text(ctx->arena, fn, ctx->source);
                if (fname &&
                    (strcmp(fname, "library") == 0 || strcmp(fname, "require") == 0 ||
                     strcmp(fname, "requireNamespace") == 0 ||
                     strcmp(fname, "loadNamespace") == 0 || strcmp(fname, "source") == 0)) {
                    uint32_t na = ts_node_named_child_count(args);
                    for (uint32_t i = 0; i < na; i++) {
                        TSNode arg = ts_node_named_child(args, i);
                        if (strcmp(ts_node_type(arg), "argument") != 0) {
                            continue;
                        }
                        TSNode val = ts_node_child_by_field_name(arg, TS_FIELD("value"));
                        if (!ts_node_is_null(val)) {
                            r_push_import(ctx, cbm_node_text(ctx->arena, val, ctx->source));
                        }
                        break; // first positional argument only
                    }
                }
            }
        }
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        r_collect_imports(ctx, ts_node_named_child(node, i));
    }
}

static void parse_r_imports(CBMExtractCtx *ctx) {
    r_collect_imports(ctx, ctx->root);
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
    /* Strip surrounding quotes (Pony `use "util"`, func `#include "utils.fc"`)
     * so the module path is a clean filename the resolver can match. */
    text = strip_quotes(a, text);
    if (text && text[0]) {
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

// --- Kotlin imports ---
// tree-sitter-kotlin nests imports: source_file -> import_list -> import_header*.
// parse_generic_imports only scans the DIRECT children of root, and "import" is
// the keyword token (anon_sym_import), not a statement node — so a generic
// match on "import" finds nothing.  Descend into import_list (and accept a bare
// import_header for grammar variants) and reuse the generic path extractors.
static void extract_one_import_header(CBMExtractCtx *ctx, TSNode header) {
    if (!try_generic_path_fields(ctx, header)) {
        generic_import_from_text(ctx, header);
    }
}

static void parse_kotlin_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "import_header") == 0) {
            extract_one_import_header(ctx, node);
        } else if (strcmp(kind, "import_list") == 0) {
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode child = ts_node_child(node, j);
                if (strcmp(ts_node_type(child), "import_header") == 0) {
                    extract_one_import_header(ctx, child);
                }
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// Find the first descendant node of `type` (DFS, pre-order). Returns true and
// writes *out on the first match. Shared by the Dart/Zig import parsers, whose
// URI/string is nested several levels below the import node.
static bool find_first_descendant_of(TSNode node, const char *type, // NOLINT(misc-no-recursion)
                                     TSNode *out) {
    if (strcmp(ts_node_type(node), type) == 0) {
        *out = node;
        return true;
    }
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        if (find_first_descendant_of(ts_node_named_child(node, i), type, out)) {
            return true;
        }
    }
    return false;
}

// --- Dart imports ---
// tree-sitter-dart wraps each top-level import as `import_or_export`; the URI is
// a `string_literal` nested under library_import -> import_specification ->
// configurable_uri -> uri. The old dispatch matched "import_declaration" (which
// tree-sitter-dart never emits) -> 0 imports. Find the first string_literal under
// each import_or_export and strip its quotes.
static void parse_dart_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(node), "import_or_export") != 0) {
            continue;
        }
        TSNode uri = node;
        if (find_first_descendant_of(node, "string_literal", &uri)) {
            char *path = strip_quotes(a, cbm_node_text(a, uri, ctx->source));
            if (path && path[0]) {
                CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                cbm_imports_push(&ctx->result->imports, a, imp);
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Haskell imports ---
// tree-sitter-haskell nests imports under an `imports` container (and/or lists
// `import` nodes); each `import` carries a `module` field. parse_generic_imports
// only scanned root children for "import", missing the container -> 0 imports.
// Descend into `imports` (and accept a root-level `import`) and reuse the generic
// path extractors, which pick up the "module" field.
static void parse_haskell_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "import") == 0) {
            extract_one_import_header(ctx, node);
        } else if (strcmp(kind, "imports") == 0) {
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode child = ts_node_child(node, j);
                if (strcmp(ts_node_type(child), "import") == 0) {
                    extract_one_import_header(ctx, child);
                }
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Zig imports ---
// Zig imports are `const std = @import("std");` — a `builtin_function` (@import)
// nested inside a variable_declaration, NOT a root child. The old dispatch scanned
// root for "builtin_function" -> 0. DFS the whole tree for builtin_function nodes
// whose text starts with @import/@cImport and take their first string argument.
static void parse_zig_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "builtin_function") == 0) {
            char *bf = cbm_node_text(a, node, ctx->source);
            if (bf && (strncmp(bf, "@import", sizeof("@import") - 1) == 0 ||
                       strncmp(bf, "@cImport", sizeof("@cImport") - 1) == 0)) {
                TSNode str = node;
                if (find_first_descendant_of(node, "string", &str)) {
                    char *path = strip_quotes(a, cbm_node_text(a, str, ctx->source));
                    if (path && path[0]) {
                        CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
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

static void walk_wolfram_imports(CBMExtractCtx *ctx, TSNode root) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, root);

    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *kind = ts_node_type(node);

        if (strcmp(kind, "get_top") == 0) {
            process_wolfram_get_top(ctx, node);
        } else if (strcmp(kind, "apply") == 0) {
            process_wolfram_needs(ctx, node);
        }

        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

static void parse_wolfram_imports(CBMExtractCtx *ctx) {
    walk_wolfram_imports(ctx, ctx->root);
}

// --- PHP imports ---
// tree-sitter-php models `use Foo\Bar;` as a `namespace_use_declaration`
// containing one or more `namespace_use_clause` nodes (each a qualified_name,
// optionally aliased via `as`).  Grouped `use Foo\{A, B};` uses a
// `namespace_use_group` with `namespace_use_group_clause` children.  The bare
// require/include forms remain `expression_statement`s and are still handled by
// the text fallback.  Take the first qualified_name/name descendant of each
// clause as the module path.
static void emit_php_use_clause(CBMExtractCtx *ctx, TSNode clause, const char *group_prefix) {
    CBMArena *a = ctx->arena;
    // The path node is the qualified_name / namespace_name / name child.
    TSNode path_node = clause;
    bool found = false;
    static const char *path_kinds[] = {"qualified_name", "namespace_name", "name", NULL};
    for (const char **k = path_kinds; *k && !found; k++) {
        if (find_first_descendant_of(clause, *k, &path_node)) {
            found = true;
        }
    }
    if (!found) {
        return;
    }
    char *path = cbm_node_text(a, path_node, ctx->source);
    if (!path || !path[0]) {
        return;
    }
    if (group_prefix && group_prefix[0]) {
        path = cbm_arena_sprintf(a, "%s\\%s", group_prefix, path);
    }
    // Alias: an `as` clause provides a trailing identifier (the second name).
    TSNode alias = ts_node_child_by_field_name(clause, TS_FIELD("alias"));
    const char *local =
        !ts_node_is_null(alias) ? cbm_node_text(a, alias, ctx->source) : path_last(a, path);
    CBMImport imp = {.local_name = local, .module_path = path};
    cbm_imports_push(&ctx->result->imports, a, imp);
}

static void emit_php_use_decl(CBMExtractCtx *ctx, TSNode decl) {
    CBMArena *a = ctx->arena;
    // Grouped form: namespace_use_group with a leading prefix qualified_name.
    TSNode group = decl;
    if (find_first_descendant_of(decl, "namespace_use_group", &group)) {
        // The prefix is the qualified_name sibling preceding the group within decl.
        char *prefix = NULL;
        uint32_t dc = ts_node_named_child_count(decl);
        for (uint32_t i = 0; i < dc; i++) {
            TSNode c = ts_node_named_child(decl, i);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "qualified_name") == 0 || strcmp(ck, "namespace_name") == 0 ||
                strcmp(ck, "name") == 0) {
                prefix = cbm_node_text(a, c, ctx->source);
                break;
            }
        }
        uint32_t gc = ts_node_named_child_count(group);
        for (uint32_t i = 0; i < gc; i++) {
            TSNode clause = ts_node_named_child(group, i);
            const char *ck = ts_node_type(clause);
            if (strcmp(ck, "namespace_use_group_clause") == 0 ||
                strcmp(ck, "namespace_use_clause") == 0) {
                emit_php_use_clause(ctx, clause, prefix);
            }
        }
        return;
    }
    // Flat form: one or more namespace_use_clause children.
    uint32_t dc = ts_node_named_child_count(decl);
    bool any = false;
    for (uint32_t i = 0; i < dc; i++) {
        TSNode clause = ts_node_named_child(decl, i);
        if (strcmp(ts_node_type(clause), "namespace_use_clause") == 0) {
            emit_php_use_clause(ctx, clause, NULL);
            any = true;
        }
    }
    if (!any) {
        // Some grammar versions inline the path directly under the declaration.
        emit_php_use_clause(ctx, decl, NULL);
    }
}

static void parse_php_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (strcmp(kind, "namespace_use_declaration") == 0) {
            emit_php_use_decl(ctx, node);
        } else if (strcmp(kind, "expression_statement") == 0) {
            // require / include / require_once / include_once
            if (!try_generic_path_fields(ctx, node)) {
                generic_import_from_text(ctx, node);
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- C# imports ---
// tree-sitter-c-sharp models `using System.Text;` as a `using_directive`.
// The namespace path is a `qualified_name`/`identifier` child.  For an alias
// form `using F = System.IO.File;` the directive has a `name` field holding the
// alias identifier `F` and a separate qualified_name on the right of `=` — the
// generic path-field extractor wrongly returns the alias `F`, so we instead
// take the LAST qualified_name/identifier (the real namespace/type), and use
// the `name` field as local_name when present.
static void parse_csharp_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(node), "using_directive") != 0) {
            continue;
        }
        // Find the right-most qualified_name / identifier (the namespace/type),
        // which is the import target even in the alias form `using F = X;`.
        TSNode path_node = node;
        bool found = false;
        uint32_t nc = ts_node_named_child_count(node);
        for (int i = (int)nc - 1; i >= 0; i--) {
            TSNode c = ts_node_named_child(node, (uint32_t)i);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "qualified_name") == 0 || strcmp(ck, "identifier") == 0 ||
                strcmp(ck, "member_access_expression") == 0 || strcmp(ck, "name") == 0) {
                path_node = c;
                found = true;
                break;
            }
        }
        char *path = found ? cbm_node_text(a, path_node, ctx->source) : NULL;
        if (!path || !path[0]) {
            // Fallback to text stripping (handles `using static X;`).
            if (!try_generic_path_fields(ctx, node)) {
                generic_import_from_text(ctx, node);
            }
            continue;
        }
        // Alias name, if any.
        TSNode alias = ts_node_child_by_field_name(node, TS_FIELD("alias"));
        const char *local =
            !ts_node_is_null(alias) ? cbm_node_text(a, alias, ctx->source) : path_last(a, path);
        CBMImport imp = {.local_name = local, .module_path = path};
        cbm_imports_push(&ctx->result->imports, a, imp);
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Generic spec-driven imports ---
// For grammar-only languages that have no dedicated parser above, consume the
// `import_node_types` declared in the language's CBMLangSpec.  Each root-level
// child whose type matches one of those node types is treated as an import:
// first try the known path fields (path/source/module/name), then fall back to
// stripping the leading keyword + trailing ';' from the node text.  This is the
// same extraction strategy the dedicated `parse_generic_imports` used, but the
// node-type set comes from the spec instead of a hardcoded string, so every
// language with `import_node_types` configured gets imports extracted.
static bool spec_type_matches(const char **types, const char *kind) {
    if (!types) {
        return false;
    }
    for (const char **t = types; *t; t++) {
        if (strcmp(*t, kind) == 0) {
            return true;
        }
    }
    return false;
}

static void parse_spec_imports(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec || !spec->import_node_types) {
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (!spec_type_matches(spec->import_node_types, kind)) {
            continue;
        }
        if (!try_generic_path_fields(ctx, node)) {
            generic_import_from_text(ctx, node);
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Embedded-language imports ---
// Generic walker for host grammars (Svelte, Vue, HTML, Astro, ...) whose AST
// keeps embedded sub-language source as raw_text (or similar) without parsing
// it.  The host's CBMLangSpec.embedded_imports declares which content nodes
// hold which sub-language; we re-parse each match with the embedded grammar
// and run the standard ES import walker over the inner AST.
//
// No grammar symbols are referenced here — the embedded TSLanguage is
// resolved through cbm_ts_language(spec->embedded_language), the same hook
// that the main parser uses.  Adding another host language is a one-line
// declaration in lang_specs.c.

static void embedded_collect_content_nodes(TSNode root, const CBMEmbeddedLangSpec *spec,
                                           TSNode *out, int *out_count, int max_out) {
    /* Iterative DFS so deeply-nested script blocks are still found.  Cap the
     * stack to a sane bound (host grammars do not have million-deep markup
     * trees) — no need to introduce TSNodeStack here. */
    enum { EMBED_STACK_CAP = 1024 };
    TSNode stack[EMBED_STACK_CAP];
    int top = 0;
    stack[top++] = root;
    while (top > 0 && *out_count < max_out) {
        TSNode node = stack[--top];
        const char *kind = ts_node_type(node);
        if (strcmp(kind, spec->script_node_type) == 0) {
            uint32_t cc = ts_node_child_count(node);
            for (uint32_t k = 0; k < cc; k++) {
                TSNode c = ts_node_child(node, k);
                if (strcmp(ts_node_type(c), spec->content_node_type) == 0) {
                    out[(*out_count)++] = c;
                    if (*out_count >= max_out) {
                        return;
                    }
                    break; /* one content node per script element */
                }
            }
            /* Do not descend into <script>'s children — content already taken. */
            continue;
        }
        uint32_t count = ts_node_child_count(node);
        for (int i = (int)count - 1; i >= 0 && top < EMBED_STACK_CAP; i--) {
            stack[top++] = ts_node_child(node, (uint32_t)i);
        }
    }
}

static void parse_embedded_imports(CBMExtractCtx *ctx) {
    const CBMLangSpec *spec = cbm_lang_spec(ctx->language);
    if (!spec || !spec->embedded_imports) {
        return;
    }
    for (const CBMEmbeddedLangSpec *e = spec->embedded_imports; e->script_node_type != NULL; e++) {
        const TSLanguage *embedded_lang = cbm_ts_language(e->embedded_language);
        if (!embedded_lang) {
            continue; /* embedded grammar not linked in — silently skip */
        }
        enum { MAX_EMBEDDED_BLOCKS = 16 };
        TSNode hits[MAX_EMBEDDED_BLOCKS];
        int hit_count = 0;
        embedded_collect_content_nodes(ctx->root, e, hits, &hit_count, MAX_EMBEDDED_BLOCKS);
        if (hit_count == 0) {
            continue;
        }
        TSParser *parser = ts_parser_new();
        if (!parser) {
            continue;
        }
        if (!ts_parser_set_language(parser, embedded_lang)) {
            ts_parser_delete(parser);
            continue;
        }
        for (int i = 0; i < hit_count; i++) {
            uint32_t s = ts_node_start_byte(hits[i]);
            uint32_t end = ts_node_end_byte(hits[i]);
            if (end <= s) {
                continue;
            }
            const char *sub_src = ctx->source + s;
            uint32_t sub_len = end - s;
            TSTree *sub_tree = ts_parser_parse_string(parser, NULL, sub_src, sub_len);
            if (!sub_tree) {
                continue;
            }
            CBMExtractCtx sub_ctx = *ctx;
            sub_ctx.source = sub_src;
            sub_ctx.root = ts_tree_root_node(sub_tree);
            walk_es_imports(&sub_ctx, sub_ctx.root);
            ts_tree_delete(sub_tree);
        }
        ts_parser_delete(parser);
    }
}

// --- Namespace / package declaration capture ---
// Java/Kotlin/C#/PHP put the file's symbols inside a namespace/package whose
// name is NOT reflected in the path-based QN scheme.  Capturing it lets the
// pipeline resolve `using App.Utils` / `import com.example.Foo` to the File
// node(s) that declare that namespace, which the path-derived QN alone cannot.
static void capture_namespace_decl(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    static const char *ns_kinds[] = {"namespace_declaration",             // C#
                                     "file_scoped_namespace_declaration", // C# 10
                                     "package_declaration",               // Java / Kotlin
                                     "package_header",                    // Kotlin
                                     "namespace_definition",              // PHP
                                     NULL};
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (!spec_type_matches(ns_kinds, kind)) {
            continue;
        }
        // The namespace name is the first qualified_name / scoped_identifier /
        // namespace_name / identifier descendant.
        static const char *name_kinds[] = {"qualified_name",
                                           "scoped_identifier",
                                           "namespace_name",
                                           "identifier",
                                           "dotted_name",
                                           "name",
                                           NULL};
        for (const char **nk = name_kinds; *nk; nk++) {
            TSNode nn = node;
            if (find_first_descendant_of(node, *nk, &nn)) {
                char *ns = cbm_node_text(a, nn, ctx->source);
                if (ns && ns[0]) {
                    ctx->result->namespace_name = ns;
                }
                break;
            }
        }
        if (ctx->result->namespace_name) {
            break;
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Hare imports ---
// tree-sitter-hare nests imports: module -> imports -> use_statement* ->
// identifier. The use_statement is not a root child, so a generic scan misses
// it. Descend into the `imports` container and take each use_statement's
// identifier text (a `::`-separated module path).
static void parse_hare_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "use_statement") == 0) {
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode c = ts_node_named_child(node, j);
                const char *ck = ts_node_type(c);
                if (strcmp(ck, "identifier") == 0 || strcmp(ck, "scoped_identifier") == 0) {
                    char *mod = cbm_node_text(a, c, ctx->source);
                    if (mod && mod[0]) {
                        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                    break;
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Pascal imports ---
// tree-sitter-pascal: declUses (uses Foo, Bar, Baz;) is nested under
// unit -> interface (or implementation), NOT a root child, and carries one
// `moduleName` child per imported unit. DFS for declUses nodes and emit one
// import per moduleName.
static void parse_pascal_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "declUses") == 0) {
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode c = ts_node_named_child(node, j);
                if (strcmp(ts_node_type(c), "moduleName") != 0) {
                    continue;
                }
                char *mod = cbm_node_text(a, c, ctx->source);
                if (mod && mod[0]) {
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- PowerShell imports ---
// tree-sitter-powershell models `using module ./M` / `using namespace System.IO`
// as a plain `command` whose command_name is "using" — deeply nested under
// statement_list -> pipeline -> pipeline_chain. The module path is the LAST
// generic_token child of command_elements (after the "module"/"namespace"/
// "assembly" qualifier). DFS for such commands.
static void parse_powershell_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "command") == 0) {
            TSNode name = ts_node_child_by_field_name(node, TS_FIELD("command_name"));
            char *nm = ts_node_is_null(name) ? NULL : cbm_node_text(a, name, ctx->source);
            if (nm && strcmp(nm, "using") == 0) {
                /* Find the last generic_token anywhere under the command — that
                 * is the module path / namespace / assembly being imported. */
                TSNodeStack inner;
                ts_nstack_init(&inner, a, CBM_SZ_512);
                ts_nstack_push(&inner, a, node);
                const char *last_tok = NULL;
                uint32_t last_start = 0;
                while (inner.count > 0) {
                    TSNode c = ts_nstack_pop(&inner);
                    if (strcmp(ts_node_type(c), "generic_token") == 0) {
                        char *t = cbm_node_text(a, c, ctx->source);
                        uint32_t sb = ts_node_start_byte(c);
                        if (t && t[0] && strcmp(t, "module") != 0 && strcmp(t, "namespace") != 0 &&
                            strcmp(t, "assembly") != 0 && (last_tok == NULL || sb > last_start)) {
                            last_tok = t;
                            last_start = sb;
                        }
                    }
                    ts_nstack_push_children(&inner, a, c);
                }
                if (last_tok && last_tok[0]) {
                    CBMImport imp = {.local_name = path_last(a, last_tok),
                                     .module_path = (char *)last_tok};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue; /* commands don't nest imports further */
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Lisp-family imports (Scheme, Racket) ---
// S-expression grammars model (import (lib ...)) / (require mod) / (load "f")
// as a `list` whose first named child is a `symbol` naming the form. Walk
// top-level lists; for import/require/load/use, take the remaining children as
// module references (a symbol, a string, or a nested list whose meaningful
// module symbol we take as-is).
static void lisp_push_module(CBMExtractCtx *ctx, TSNode mod_node) {
    CBMArena *a = ctx->arena;
    const char *mk = ts_node_type(mod_node);
    char *mod = NULL;
    if (strcmp(mk, "symbol") == 0 || strcmp(mk, "string") == 0 || strcmp(mk, "sym_lit") == 0 ||
        strcmp(mk, "str_lit") == 0 || strcmp(mk, "kwd_lit") == 0) {
        mod = strip_quotes(a, cbm_node_text(a, mod_node, ctx->source));
        /* Clojure/Fennel keyword module refs carry a leading ':' (e.g. `:util`,
         * `(require :util)`); strip it so the name matches the sibling file. */
        if (mod && mod[0] == ':') {
            mod = cbm_arena_strdup(a, mod + 1);
        }
    } else if (strcmp(mk, "list") == 0 || strcmp(mk, "list_lit") == 0 ||
               strcmp(mk, "vec_lit") == 0) {
        /* (only-in racket/math pi) → take first symbol after a leading keyword,
         * else join inner symbols with '/'. Simplest robust choice: the whole
         * list text minus the parens. */
        mod = cbm_node_text(a, mod_node, ctx->source);
        if (mod) {
            size_t len = strlen(mod);
            if (len >= 2 && mod[0] == '(' && mod[len - 1] == ')') {
                mod = cbm_arena_strndup(a, mod + 1, len - 2);
            }
        }
    }
    if (mod && mod[0]) {
        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
        cbm_imports_push(&ctx->result->imports, a, imp);
    }
}

/* Strip a leading ':' from a lisp keyword/symbol token (Clojure/Fennel/CL
 * package keywords carry it: `:util` → `util`).  Returns an arena copy. */
static char *lisp_strip_kw(CBMArena *a, const char *s) {
    if (!s) {
        return NULL;
    }
    if (s[0] == ':') {
        s++;
    }
    return cbm_arena_strdup(a, s);
}

/* Record the file's declared namespace/package so the pipeline namespace map
 * can resolve `(:require ns)` / `(:use :pkg)` to the declaring file.  First
 * declaration wins (mirrors capture_namespace_decl). */
static void lisp_set_namespace(CBMExtractCtx *ctx, TSNode name_node) {
    CBMArena *a = ctx->arena;
    char *ns = lisp_strip_kw(a, cbm_node_text(a, name_node, ctx->source));
    if (ns && ns[0] && !ctx->result->namespace_name) {
        ctx->result->namespace_name = ns;
    }
}

/* Clojure `(ns app.core (:require [app.util :as u]) (:use app.io))` and Common
 * Lisp `(defpackage :main (:use :cl :util))` carry their dependency list inside
 * nested keyword clauses (`:require`/`:use`/`:import`).  Walk such a clause and
 * push each referenced module/package as an import.  `clause` is the
 * `(:require ...)` / `(:use ...)` list. */
static void lisp_push_clause_modules(CBMExtractCtx *ctx, TSNode clause) {
    uint32_t cc = ts_node_named_child_count(clause);
    for (uint32_t j = 1; j < cc; j++) { /* skip the leading keyword head */
        TSNode item = ts_node_named_child(clause, j);
        const char *ik = ts_node_type(item);
        /* `[app.util :as u]` — the module is the vector's first symbol. */
        if (strcmp(ik, "vec_lit") == 0 || strcmp(ik, "list_lit") == 0 || strcmp(ik, "list") == 0) {
            uint32_t vc = ts_node_named_child_count(item);
            if (vc > 0) {
                lisp_push_module(ctx, ts_node_named_child(item, 0));
            }
        } else {
            /* bare symbol/keyword: `:util`, `app.io`. */
            lisp_push_module(ctx, item);
        }
    }
}

/* The s-expression head symbol text, or NULL if the list has no symbol head. */
static char *lisp_head_text(CBMExtractCtx *ctx, TSNode list, TSNode *out_head) {
    if (ts_node_named_child_count(list) < 1) {
        return NULL;
    }
    TSNode head = ts_node_named_child(list, 0);
    const char *ht = ts_node_type(head);
    if (strcmp(ht, "symbol") != 0 && strcmp(ht, "sym_lit") != 0) {
        return NULL;
    }
    if (out_head) {
        *out_head = head;
    }
    return cbm_node_text(ctx->arena, head, ctx->source);
}

/* Process one s-expression list as a potential import/namespace form. */
static void lisp_process_list(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    (void)a;
    TSNode head = node;
    char *hn = lisp_head_text(ctx, node, &head);
    if (!hn) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(node);

    /* Clojure namespace form: `(ns app.core (:require ...) (:use ...))`.
     * 2nd child is the namespace symbol; later keyword-headed lists are
     * dependency clauses. */
    if (strcmp(hn, "ns") == 0 && nc >= 2) {
        lisp_set_namespace(ctx, ts_node_named_child(node, 1));
        for (uint32_t j = 2; j < nc; j++) {
            TSNode clause = ts_node_named_child(node, j);
            const char *ck = ts_node_type(clause);
            if (strcmp(ck, "list_lit") == 0 || strcmp(ck, "list") == 0) {
                lisp_push_clause_modules(ctx, clause);
            }
        }
        return;
    }

    /* Common Lisp package form: `(defpackage :main (:use :cl :util) ...)`.
     * 2nd child is the package name keyword; nested `(:use ...)` lists name the
     * imported packages. */
    if (strcmp(hn, "defpackage") == 0 && nc >= 2) {
        lisp_set_namespace(ctx, ts_node_named_child(node, 1));
        for (uint32_t j = 2; j < nc; j++) {
            TSNode clause = ts_node_named_child(node, j);
            const char *ck = ts_node_type(clause);
            if (strcmp(ck, "list_lit") != 0 && strcmp(ck, "list") != 0) {
                continue;
            }
            /* Only `:use` / `:import-from` clauses denote dependencies. */
            char *chn = lisp_head_text(ctx, clause, NULL);
            if (chn && (strcmp(chn, ":use") == 0 || strstr(chn, "import-from"))) {
                lisp_push_clause_modules(ctx, clause);
            } else if (ts_node_named_child_count(clause) > 0) {
                TSNode ch0 = ts_node_named_child(clause, 0);
                const char *c0k = ts_node_type(ch0);
                /* CL `(:use ...)` head is a `kwd_lit`, not a symbol. */
                if (strcmp(c0k, "kwd_lit") == 0) {
                    char *kw = cbm_node_text(ctx->arena, ch0, ctx->source);
                    if (kw && (strstr(kw, "use") || strstr(kw, "import-from"))) {
                        lisp_push_clause_modules(ctx, clause);
                    }
                }
            }
        }
        return;
    }

    /* Common Lisp `(in-package :util)` also declares the file's package. */
    if (strcmp(hn, "in-package") == 0 && nc >= 2) {
        lisp_set_namespace(ctx, ts_node_named_child(node, 1));
        return;
    }

    /* Plain import head: `(require :util)`, `(import ...)`, `(use ...)`. */
    if (strcmp(hn, "import") == 0 || strcmp(hn, "require") == 0 || strcmp(hn, "load") == 0 ||
        strcmp(hn, "use") == 0 || strcmp(hn, "include") == 0) {
        for (uint32_t j = 1; j < nc; j++) {
            TSNode mod_node = ts_node_named_child(node, j);
            const char *mk = ts_node_type(mod_node);
            /* (require 'json) — the quoted datum is a `quote` wrapping a symbol. */
            if ((strcmp(mk, "quote") == 0 || strcmp(mk, "quoting_lit") == 0) &&
                ts_node_named_child_count(mod_node) > 0) {
                lisp_push_module(ctx, ts_node_named_child(mod_node, 0));
            } else {
                lisp_push_module(ctx, mod_node);
            }
        }
    }
}

/* Iteratively walk lisp lists.  Fennel binds requires inside other forms
 * (`(local util (require :util))`), so we must descend into every list, not
 * just root children. Stack-based (not recursive) to avoid deep-nesting stack
 * overflow, matching the other walkers in this file. */
static void parse_lisp_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *nt = ts_node_type(node);
        if (strcmp(nt, "list") == 0 || strcmp(nt, "list_lit") == 0) {
            lisp_process_list(ctx, node);
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Starlark imports ---
// load("//pkg:file.bzl", "sym", ...) — an expression_statement -> call whose
// function identifier is "load"; the first string argument is the module path.
static void parse_starlark_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "call") == 0) {
            TSNode fn = ts_node_child_by_field_name(node, TS_FIELD("function"));
            char *fname = ts_node_is_null(fn) ? NULL : cbm_node_text(a, fn, ctx->source);
            if (fname && strcmp(fname, "load") == 0) {
                TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
                if (!ts_node_is_null(args)) {
                    TSNode str = args;
                    if (find_first_descendant_of(args, "string", &str)) {
                        char *path = strip_quotes(a, cbm_node_text(a, str, ctx->source));
                        /* string node may carry string_start/_content/_end; if the
                         * stripped text still has quotes, fall back to content. */
                        if (path && path[0] && (path[0] == '"' || path[0] == '\'')) {
                            TSNode content = str;
                            if (find_first_descendant_of(str, "string_content", &content)) {
                                path = cbm_node_text(a, content, ctx->source);
                            }
                        }
                        if (path && path[0]) {
                            CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                            cbm_imports_push(&ctx->result->imports, a, imp);
                        }
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Tcl imports ---
// source path / package require name — `command` nodes whose `name` field is
// "source" (file include) or "package" (package require). Take the first
// simple_word argument as the module.
static void parse_tcl_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "command") == 0) {
            TSNode name = ts_node_child_by_field_name(node, TS_FIELD("name"));
            char *nm = ts_node_is_null(name) ? NULL : cbm_node_text(a, name, ctx->source);
            if (nm && strcmp(nm, "source") == 0) {
                TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
                if (!ts_node_is_null(args)) {
                    uint32_t nc = ts_node_named_child_count(args);
                    for (uint32_t j = 0; j < nc; j++) {
                        TSNode c = ts_node_named_child(args, j);
                        char *mod = strip_quotes(a, cbm_node_text(a, c, ctx->source));
                        if (mod && mod[0]) {
                            CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                            cbm_imports_push(&ctx->result->imports, a, imp);
                            break;
                        }
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Teal imports ---
// Teal is a typed Lua dialect: `local m = require("mod")` — a function_call
// whose called_object identifier is "require"; the first string argument is the
// module. DFS for such calls (mirrors the Lua require pattern at AST level).
static void parse_teal_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "function_call") == 0) {
            TSNode callee = ts_node_child_by_field_name(node, "called_object", 13);
            char *cn = ts_node_is_null(callee) ? NULL : cbm_node_text(a, callee, ctx->source);
            if (cn && strcmp(cn, "require") == 0) {
                TSNode args = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
                TSNode str = ts_node_is_null(args) ? node : args;
                if (!ts_node_is_null(args) && find_first_descendant_of(args, "string", &str)) {
                    char *mod = strip_quotes(a, cbm_node_text(a, str, ctx->source));
                    if (mod && mod[0] && (mod[0] == '"' || mod[0] == '\'')) {
                        TSNode content = str;
                        if (find_first_descendant_of(str, "string_content", &content)) {
                            mod = cbm_node_text(a, content, ctx->source);
                        }
                    }
                    if (mod && mod[0]) {
                        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Zsh imports ---
// source file / . file — `command` nodes whose command_name is "source" or ".".
// The argument field carries the sourced path.
static void parse_zsh_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "command") == 0) {
            TSNode name = ts_node_child_by_field_name(node, TS_FIELD("name"));
            char *nm = ts_node_is_null(name) ? NULL : cbm_node_text(a, name, ctx->source);
            if (nm && (strcmp(nm, "source") == 0 || strcmp(nm, ".") == 0)) {
                TSNode arg = ts_node_child_by_field_name(node, TS_FIELD("argument"));
                if (ts_node_is_null(arg)) {
                    arg = ts_node_child_by_field_name(node, TS_FIELD("arguments"));
                }
                if (!ts_node_is_null(arg)) {
                    char *mod = strip_quotes(a, cbm_node_text(a, arg, ctx->source));
                    if (mod && mod[0]) {
                        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- CSS / SCSS imports ---
// CSS @import "x.css" and SCSS @use/@forward 'x' are top-level statements that
// carry a `string_value` (whose `string_content` is the unquoted path). The
// generic text fallback mangled these (kept the surrounding quotes), so the
// resolver couldn't match the target file. Extract the clean string content.
static void css_push_import_from_stmt(CBMExtractCtx *ctx, TSNode stmt) {
    CBMArena *a = ctx->arena;
    TSNode sv = stmt;
    if (!find_first_descendant_of(stmt, "string_value", &sv)) {
        /* CSS @import url("x") uses a `call_expression`/`plain_value`; fall back
         * to the first string node. */
        if (!find_first_descendant_of(stmt, "string_content", &sv)) {
            return;
        }
    }
    /* Prefer the inner string_content (already unquoted) when present. */
    TSNode content = sv;
    char *path = NULL;
    if (find_first_descendant_of(sv, "string_content", &content)) {
        path = cbm_node_text(a, content, ctx->source);
    } else {
        path = strip_quotes(a, cbm_node_text(a, sv, ctx->source));
    }
    if (!path || !path[0]) {
        return;
    }
    CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
    cbm_imports_push(&ctx->result->imports, a, imp);
}

static void parse_css_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *k = ts_node_type(node);
        if (strcmp(k, "import_statement") == 0 || strcmp(k, "use_statement") == 0 ||
            strcmp(k, "forward_statement") == 0 || strcmp(k, "include_statement") == 0) {
            css_push_import_from_stmt(ctx, node);
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- HTML imports ---
// `<script src="app.js">` and `<link href="style.css">` reference sibling
// files. tree-sitter-html exposes these as `start_tag` -> `attribute`
// (attribute_name src/href) -> quoted_attribute_value -> attribute_value. DFS
// for start_tags and emit one import per src/href value. (Embedded <script>
// bodies are still handled separately by parse_embedded_imports.)
static void html_extract_tag_src(CBMExtractCtx *ctx, TSNode tag) {
    CBMArena *a = ctx->arena;
    uint32_t nc = ts_node_named_child_count(tag);
    for (uint32_t j = 0; j < nc; j++) {
        TSNode attr = ts_node_named_child(tag, j);
        if (strcmp(ts_node_type(attr), "attribute") != 0) {
            continue;
        }
        TSNode name = ts_node_named_child_count(attr) > 0 ? ts_node_named_child(attr, 0) : attr;
        char *an = cbm_node_text(a, name, ctx->source);
        if (!an || (strcmp(an, "src") != 0 && strcmp(an, "href") != 0)) {
            continue;
        }
        TSNode val = attr;
        char *path = NULL;
        if (find_first_descendant_of(attr, "attribute_value", &val)) {
            path = cbm_node_text(a, val, ctx->source);
        }
        if (path && path[0]) {
            CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
            cbm_imports_push(&ctx->result->imports, a, imp);
        }
    }
}

static void parse_html_imports(CBMExtractCtx *ctx) {
    /* First run the embedded-language walker (inline <script> JS imports). */
    parse_embedded_imports(ctx);

    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "start_tag") == 0 ||
            strcmp(ts_node_type(node), "self_closing_tag") == 0) {
            html_extract_tag_src(ctx, node);
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// Shared: push an import whose path is the (quote-stripped) text of `node`.
// Strips a leading "./" so relative-import resolution matches the sibling file.
static void push_path_import(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    char *path = strip_quotes(a, cbm_node_text(a, node, ctx->source));
    if (!path || !path[0]) {
        return;
    }
    /* Trim surrounding whitespace (some grammars include a leading space). */
    while (*path == ' ' || *path == '\t') {
        path++;
    }
    size_t len = strlen(path);
    while (len > 0 && (path[len - 1] == ' ' || path[len - 1] == '\t' || path[len - 1] == '\n')) {
        path[--len] = '\0';
    }
    if (!path[0]) {
        return;
    }
    CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
    cbm_imports_push(&ctx->result->imports, a, imp);
}

// Shared: find a node's clean string path — prefers an inner unquoted content
// child (string_content / str_literal / …); else an inner quoted `string`
// (whose quotes push_path_import strips); else the node's own text. Never falls
// back to text that still contains a directive keyword.
static void push_string_descendant_import(CBMExtractCtx *ctx, TSNode node) {
    TSNode hit = node;
    static const char *content_kinds[] = {
        "string_content",  "str_literal",     "slStringLiteralPart",
        "include_path",    "attribute_value", "path_fragment",
        "string_fragment", "literal_content", NULL};
    for (const char **k = content_kinds; *k; k++) {
        if (find_first_descendant_of(node, *k, &hit)) {
            push_path_import(ctx, hit);
            return;
        }
    }
    /* Quoted string nodes (Just/Pkl/Jsonnet variants): strip the quotes. */
    static const char *string_kinds[] = {"string",        "string_value",   "string_literal",
                                         "static_string", "stringConstant", NULL};
    for (const char **k = string_kinds; *k; k++) {
        if (find_first_descendant_of(node, *k, &hit)) {
            push_path_import(ctx, hit);
            return;
        }
    }
    push_path_import(ctx, node);
}

// --- CMake imports: include(path) / add_subdirectory(dir) ---
static void parse_cmake_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "normal_command") == 0) {
            TSNode id = ts_node_named_child_count(node) > 0 ? ts_node_named_child(node, 0) : node;
            char *cmd = cbm_node_text(a, id, ctx->source);
            if (cmd && (strcmp(cmd, "include") == 0 || strcmp(cmd, "add_subdirectory") == 0)) {
                TSNode args = cbm_find_child_by_kind(node, "argument_list");
                if (!ts_node_is_null(args)) {
                    uint32_t nc = ts_node_named_child_count(args);
                    for (uint32_t j = 0; j < nc; j++) {
                        TSNode arg = ts_node_named_child(args, j);
                        push_string_descendant_import(ctx, arg);
                        break; /* first argument is the module/dir */
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- BitBake imports: require/include path ---
static void parse_bitbake_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (strcmp(k, "require_directive") == 0 || strcmp(k, "include_directive") == 0 ||
            strcmp(k, "inherit_directive") == 0) {
            push_string_descendant_import(ctx, node);
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- Meson imports: subdir('name') → descend into name/meson.build ---
// Meson's subdir() includes a child directory's meson.build into the build.
// The grammar models a call as `function_expression` (id + argument_list).
// Find subdir() calls and push the directory name as the module path; the
// pipeline resolver maps "name" → "name/meson.build".
static void parse_meson_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        /* tree-sitter-meson models a call as `normal_command` (identifier +
         * arguments) and string arguments as `string`. */
        if (strcmp(k, "normal_command") == 0) {
            uint32_t nc = ts_node_named_child_count(node);
            if (nc >= 1) {
                TSNode fn = ts_node_named_child(node, 0);
                char *fname = cbm_node_text(a, fn, ctx->source);
                if (fname && strcmp(fname, "subdir") == 0) {
                    TSNode str = node;
                    if (find_first_descendant_of(node, "string", &str)) {
                        char *path = strip_quotes(a, cbm_node_text(a, str, ctx->source));
                        if (path && path[0]) {
                            CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                            cbm_imports_push(&ctx->result->imports, a, imp);
                        }
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Kconfig imports: source "path" ---
static void parse_kconfig_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (ts_node_is_named(node) && strcmp(ts_node_type(node), "source") == 0) {
            push_string_descendant_import(ctx, node);
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- GN imports: import("//path") ---
static void parse_gn_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "import_statement") == 0) {
            push_string_descendant_import(ctx, node);
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- Just imports: import 'path' ---
static void parse_just_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (strcmp(ts_node_type(node), "import") == 0) {
            push_string_descendant_import(ctx, node);
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Nix imports: import ./path ---
static void parse_nix_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "apply_expression") == 0) {
            /* import <path> — the function position is the identifier "import",
             * the argument a path_expression / path_fragment / string. */
            TSNode fn = ts_node_named_child_count(node) > 0 ? ts_node_named_child(node, 0) : node;
            char *fname = cbm_node_text(a, fn, ctx->source);
            if (fname && strcmp(fname, "import") == 0 && ts_node_named_child_count(node) > 1) {
                TSNode arg = ts_node_named_child(node, 1);
                TSNode frag = arg;
                if (find_first_descendant_of(arg, "path_fragment", &frag) ||
                    find_first_descendant_of(arg, "string_content", &frag)) {
                    push_path_import(ctx, frag);
                } else {
                    push_path_import(ctx, arg);
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Jsonnet imports: import 'path' / importstr 'path' ---
static void parse_jsonnet_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (ts_node_is_named(node) && (strcmp(k, "import") == 0 || strcmp(k, "importstr") == 0 ||
                                       strcmp(k, "importbin") == 0)) {
            push_string_descendant_import(ctx, node);
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- Pkl imports: amends/extends/import "path" ---
static void parse_pkl_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (strcmp(k, "extendsOrAmendsClause") == 0 || strcmp(k, "importClause") == 0 ||
            strcmp(k, "importExpr") == 0) {
            push_string_descendant_import(ctx, node);
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- Nickel imports: import "path" ---
static void parse_nickel_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        /* The grammar emits an anonymous `import` token followed by a
         * static_string sibling. Detect any node carrying a static_string whose
         * preceding sibling text is "import". Simpler: scan children for the
         * "import" keyword token and take the next static_string. */
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t j = 0; j + 1 < nc; j++) {
            TSNode c = ts_node_child(node, j);
            char *t = cbm_node_text(a, c, ctx->source);
            if (t && strcmp(t, "import") == 0) {
                for (uint32_t m = j + 1; m < nc; m++) {
                    TSNode s = ts_node_child(node, m);
                    if (strcmp(ts_node_type(s), "static_string") == 0) {
                        push_string_descendant_import(ctx, s);
                        break;
                    }
                }
            }
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Thrift imports: include "path" ---
static void parse_thrift_imports(CBMExtractCtx *ctx) {
    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *k = ts_node_type(node);
        if (strcmp(k, "include_statement") == 0 || strcmp(k, "cpp_include_statement") == 0) {
            push_string_descendant_import(ctx, node);
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Cap'n Proto imports: using X = import "path" ---
static void parse_capnp_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (ts_node_is_named(node) &&
            (strcmp(k, "import_using") == 0 || strcmp(k, "import_path") == 0)) {
            push_string_descendant_import(ctx, node);
            continue; /* don't double-emit from nested import_path */
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- D imports: import module.path; (nested under module_def) ---
static void parse_dlang_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "import_declaration") == 0) {
            TSNode fqn = node;
            if (find_first_descendant_of(node, "module_fqn", &fqn)) {
                char *mod = cbm_node_text(a, fqn, ctx->source);
                if (mod && mod[0]) {
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- TableGen imports: include "path.td" ---
static void parse_tablegen_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (ts_node_is_named(node) &&
            (strcmp(k, "include_directive") == 0 || strcmp(k, "include") == 0)) {
            push_string_descendant_import(ctx, node);
            continue;
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- Crystal imports: require "./path" ---
static void parse_crystal_imports(CBMExtractCtx *ctx) {
    TSNodeStack stack;
    ts_nstack_init(&stack, ctx->arena, CBM_SZ_512);
    ts_nstack_push(&stack, ctx->arena, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (ts_node_is_named(node) && strcmp(ts_node_type(node), "require") == 0) {
            push_string_descendant_import(ctx, node);
            continue;
        }
        ts_nstack_push_children(&stack, ctx->arena, node);
    }
}

// --- F# imports: open Module.Path ---
static void parse_fsharp_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (strcmp(k, "import_decl") == 0 || strcmp(k, "open_expression") == 0) {
            TSNode id = node;
            if (find_first_descendant_of(node, "long_identifier", &id) ||
                find_first_descendant_of(node, "identifier", &id)) {
                char *mod = cbm_node_text(a, id, ctx->source);
                if (mod && mod[0]) {
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Ada imports: with Pkg.Child; ---
static void parse_ada_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "with_clause") == 0) {
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode c = ts_node_named_child(node, j);
                const char *ck = ts_node_type(c);
                if (strcmp(ck, "identifier") == 0 || strcmp(ck, "selected_component") == 0 ||
                    strcmp(ck, "name") == 0) {
                    char *mod = cbm_node_text(a, c, ctx->source);
                    if (mod && mod[0]) {
                        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Elm imports: `import Module.Path [as X] [exposing (..)]` ---
// AST: file -> import_clause(moduleName: upper_case_qid). The module path maps
// to a sibling file (`import Utils` -> Utils.elm), resolved by the pipeline's
// sibling-file resolver.
static void parse_elm_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "import_clause") == 0) {
            TSNode name = node;
            if (find_first_descendant_of(node, "upper_case_qid", &name)) {
                char *mod = cbm_node_text(a, name, ctx->source);
                if (mod && mod[0]) {
                    /* Elm module dots map to directory separators on disk
                     * (Math.Util -> Math/Util.elm); '.'-> '/' so the sibling
                     * resolver builds the right path. */
                    for (char *p = mod; *p; p++) {
                        if (*p == '.') {
                            *p = '/';
                        }
                    }
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Move imports: `use 0xADDR::module [::member];` ---
// AST: use_declaration with `argument: identifier` = the module name. The
// leading hex address parses as an error node but the module identifier is
// captured cleanly via the `argument` field. Resolves to a sibling file.
static void parse_move_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "use_declaration") == 0) {
            TSNode arg = ts_node_child_by_field_name(node, TS_FIELD("argument"));
            char *mod = NULL;
            if (!ts_node_is_null(arg)) {
                mod = cbm_node_text(a, arg, ctx->source);
            } else if (find_first_descendant_of(node, "identifier", &arg)) {
                mod = cbm_node_text(a, arg, ctx->source);
            }
            if (mod && mod[0]) {
                CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                cbm_imports_push(&ctx->result->imports, a, imp);
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Smali imports: `.super Lpkg/Name;` / `.implements Lpkg/Iface;` ---
// AST: super_directive / implements_directive -> class_identifier holding a JVM
// type descriptor `Lpkg/Name;`. Demangle the descriptor (drop leading 'L',
// trailing ';', '/'->'.') so the bare class name resolves to its sibling file.
static char *smali_demangle_descriptor(CBMArena *a, const char *desc) {
    if (!desc || !desc[0]) {
        return NULL;
    }
    const char *start = desc;
    if (start[0] == 'L') {
        start++;
    }
    size_t len = strlen(start);
    if (len > 0 && start[len - 1] == ';') {
        len--;
    }
    char *out = cbm_arena_strndup(a, start, len);
    if (out) {
        for (char *p = out; *p; p++) {
            if (*p == '/') {
                *p = '.';
            }
        }
    }
    return out;
}

static void parse_smali_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (strcmp(k, "super_directive") == 0 || strcmp(k, "implements_directive") == 0) {
            TSNode cid = node;
            if (find_first_descendant_of(node, "class_identifier", &cid)) {
                char *mod = smali_demangle_descriptor(a, cbm_node_text(a, cid, ctx->source));
                if (mod && mod[0]) {
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- TLA+ imports: `EXTENDS Foo, Bar` / `INSTANCE Foo` ---
// AST: extends -> identifier_ref* (one per extended module). Each module maps
// to a sibling `Foo.tla` file.
static void parse_tlaplus_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        const char *k = ts_node_type(node);
        if (strcmp(k, "extends") == 0 || strcmp(k, "instance") == 0) {
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode c = ts_node_named_child(node, j);
                const char *ck = ts_node_type(c);
                if (strcmp(ck, "identifier_ref") == 0 || strcmp(ck, "identifier") == 0) {
                    char *mod = cbm_node_text(a, c, ctx->source);
                    if (mod && mod[0]) {
                        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- VHDL imports: `use work.pkg.all;` ---
// AST: use_clause -> selected_name(library:, package:, ...). The `package`
// field names the imported package, which maps to a sibling file (math_pkg ->
// math_pkg.vhd) declaring `package math_pkg`. `library_clause` (`library work;`)
// names no file and is skipped.
static void parse_vhdl_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "use_clause") == 0) {
            TSNode sel = node;
            if (find_first_descendant_of(node, "selected_name", &sel)) {
                TSNode pkg = ts_node_child_by_field_name(sel, TS_FIELD("package"));
                char *mod = NULL;
                if (!ts_node_is_null(pkg)) {
                    mod = cbm_node_text(a, pkg, ctx->source);
                }
                if (mod && mod[0]) {
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- WIT imports: `use types.{point};` / `use pkg/iface;` ---
// AST: use_item -> use_path(id...). The leading path segment names the imported
// interface/package, mapping to a sibling `types.wit` file.
static void parse_wit_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "use_item") == 0) {
            TSNode path = node;
            if (find_first_descendant_of(node, "use_path", &path)) {
                /* First `id` child of the path is the interface/package name. */
                TSNode idn = path;
                if (find_first_descendant_of(path, "id", &idn)) {
                    char *mod = cbm_node_text(a, idn, ctx->source);
                    if (mod && mod[0]) {
                        CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Smithy imports: `use com.example.common#Shape` ---
// AST: use_statement -> absolute_root_shape_id(namespace, identifier). Emit a
// dotted path `<namespace>.<Shape>` so the pipeline's symbol-name fallback links
// it to the declaring shape (a Class/Struct node) in the namespace's file.
static void parse_smithy_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "use_statement") == 0) {
            TSNode sid = node;
            if (find_first_descendant_of(node, "absolute_root_shape_id", &sid) ||
                find_first_descendant_of(node, "shape_id", &sid)) {
                char *mod = cbm_node_text(a, sid, ctx->source);
                if (mod && mod[0]) {
                    /* `com.example.common#Timestamp` -> dotted path so the last
                     * segment (Timestamp) resolves to the declaring shape. */
                    for (char *p = mod; *p; p++) {
                        if (*p == '#') {
                            *p = '.';
                        }
                    }
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Hyprlang imports: `source = ~/path/to/file.conf` ---
// AST: source -> string. Emit the referenced path; the pipeline's sibling-file
// resolver matches it (by full path, then by basename) to the included file.
static void parse_hyprlang_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;
    TSNodeStack stack;
    ts_nstack_init(&stack, a, CBM_SZ_512);
    ts_nstack_push(&stack, a, ctx->root);
    while (stack.count > 0) {
        TSNode node = ts_nstack_pop(&stack);
        if (strcmp(ts_node_type(node), "source") == 0) {
            TSNode str = node;
            char *path = NULL;
            if (find_first_descendant_of(node, "string", &str)) {
                path = strip_quotes(a, cbm_node_text(a, str, ctx->source));
            } else {
                path = strip_quotes(a, cbm_node_text(a, node, ctx->source));
            }
            if (path && path[0]) {
                CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                cbm_imports_push(&ctx->result->imports, a, imp);
            }
            continue;
        }
        ts_nstack_push_children(&stack, a, node);
    }
}

// --- Main dispatch ---

void cbm_extract_imports(CBMExtractCtx *ctx) {
    switch (ctx->language) {
    case CBM_LANG_JAVA:
    case CBM_LANG_KOTLIN:
    case CBM_LANG_CSHARP:
    case CBM_LANG_PHP:
        capture_namespace_decl(ctx);
        break;
    default:
        break;
    }
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
        parse_kotlin_imports(ctx);
        break;
    case CBM_LANG_SCALA:
        parse_generic_imports(ctx, "import_declaration");
        break;
    case CBM_LANG_CSHARP:
        parse_csharp_imports(ctx);
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
        // PHP `use Foo\Bar;` is a namespace_use_declaration; require/include are
        // expression_statements.  parse_php_imports handles both.
        parse_php_imports(ctx);
        break;
    case CBM_LANG_RUBY:
        parse_ruby_imports(ctx);
        break;
    case CBM_LANG_LUA:
        parse_lua_imports(ctx);
        break;
    case CBM_LANG_R:
        parse_r_imports(ctx);
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
        parse_zig_imports(ctx);
        break;
    case CBM_LANG_ERLANG:
        parse_generic_imports(ctx, "module_attribute");
        break;
    case CBM_LANG_HASKELL:
        parse_haskell_imports(ctx);
        break;
    case CBM_LANG_OCAML:
        parse_generic_imports(ctx, "open_module");
        break;
    case CBM_LANG_CSS:
    case CBM_LANG_SCSS:
        parse_css_imports(ctx);
        break;
    case CBM_LANG_PERL:
        parse_generic_imports(ctx, "use_statement");
        break;
    case CBM_LANG_GROOVY:
        parse_generic_imports(ctx, "groovy_import");
        break;
    case CBM_LANG_SWIFT:
        parse_generic_imports(ctx, "import_declaration");
        break;
    case CBM_LANG_DART:
        parse_dart_imports(ctx);
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
    case CBM_LANG_HARE:
        parse_hare_imports(ctx);
        break;
    case CBM_LANG_PASCAL:
        parse_pascal_imports(ctx);
        break;
    case CBM_LANG_POWERSHELL:
        parse_powershell_imports(ctx);
        break;
    case CBM_LANG_SCHEME:
    case CBM_LANG_RACKET:
    case CBM_LANG_EMACSLISP:
    case CBM_LANG_FENNEL:
    case CBM_LANG_COMMONLISP:
    case CBM_LANG_CLOJURE:
        parse_lisp_imports(ctx);
        break;
    case CBM_LANG_STARLARK:
        parse_starlark_imports(ctx);
        break;
    case CBM_LANG_TCL:
        parse_tcl_imports(ctx);
        break;
    case CBM_LANG_TEAL:
        parse_teal_imports(ctx);
        break;
    case CBM_LANG_ZSH:
        parse_zsh_imports(ctx);
        break;
    case CBM_LANG_CMAKE:
        parse_cmake_imports(ctx);
        break;
    case CBM_LANG_BITBAKE:
        parse_bitbake_imports(ctx);
        break;
    case CBM_LANG_KCONFIG:
        parse_kconfig_imports(ctx);
        break;
    case CBM_LANG_GN:
        parse_gn_imports(ctx);
        break;
    case CBM_LANG_JUST:
        parse_just_imports(ctx);
        break;
    case CBM_LANG_MESON:
        parse_meson_imports(ctx);
        break;
    case CBM_LANG_NIX:
        parse_nix_imports(ctx);
        break;
    case CBM_LANG_JSONNET:
        parse_jsonnet_imports(ctx);
        break;
    case CBM_LANG_PKL:
        parse_pkl_imports(ctx);
        break;
    case CBM_LANG_NICKEL:
        parse_nickel_imports(ctx);
        break;
    case CBM_LANG_THRIFT:
        parse_thrift_imports(ctx);
        break;
    case CBM_LANG_CAPNP:
        parse_capnp_imports(ctx);
        break;
    case CBM_LANG_DLANG:
        parse_dlang_imports(ctx);
        break;
    case CBM_LANG_TABLEGEN:
        parse_tablegen_imports(ctx);
        break;
    case CBM_LANG_CRYSTAL:
        parse_crystal_imports(ctx);
        break;
    case CBM_LANG_FSHARP:
        parse_fsharp_imports(ctx);
        break;
    case CBM_LANG_ADA:
        parse_ada_imports(ctx);
        break;
    case CBM_LANG_ELM:
        parse_elm_imports(ctx);
        break;
    case CBM_LANG_MOVE:
        parse_move_imports(ctx);
        break;
    case CBM_LANG_SMALI:
        parse_smali_imports(ctx);
        break;
    case CBM_LANG_TLAPLUS:
        parse_tlaplus_imports(ctx);
        break;
    case CBM_LANG_VHDL:
        parse_vhdl_imports(ctx);
        break;
    case CBM_LANG_WIT:
        parse_wit_imports(ctx);
        break;
    case CBM_LANG_SMITHY:
        parse_smithy_imports(ctx);
        break;
    case CBM_LANG_HYPRLANG:
        parse_hyprlang_imports(ctx);
        break;
    /* Host languages whose tree-sitter grammar leaves <script> bodies as raw
     * text — re-parse the embedded slice via the embedded-language spec. */
    case CBM_LANG_HTML:
        parse_html_imports(ctx);
        break;
    case CBM_LANG_SVELTE:
    case CBM_LANG_VUE:
    case CBM_LANG_ASTRO:
        parse_embedded_imports(ctx);
        break;
    default:
        // Grammar-only languages with no dedicated parser: consume the
        // import_node_types declared in their CBMLangSpec generically.
        parse_spec_imports(ctx);
        break;
    }
}
