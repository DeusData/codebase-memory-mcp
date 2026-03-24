#include "cbm.h"
#include "arena.h" // CBMArena, cbm_arena_strdup/strndup/sprintf
#include "helpers.h"
#include "tree_sitter/api.h" // TSNode, ts_node_*
#include <stdint.h>          // uint32_t
#include <string.h>
#include <ctype.h>

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
    if (len >= 2 && (s[0] == '"' || s[0] == '\'') && s[len - 1] == s[0]) {
        return cbm_arena_strndup(a, s + 1, len - 2);
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
        return cbm_arena_strdup(a, last + 1);
    }
    last = strrchr(path, '.');
    if (last) {
        return cbm_arena_strdup(a, last + 1);
    }
    return path;
}

// --- Go imports ---
// import_declaration -> import_spec_list -> import_spec -> (name, path)

static void parse_go_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

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
            if (strcmp(ck, "import_spec") != 0 && strcmp(ck, "import_spec_list") != 0) {
                continue;
            }

            if (strcmp(ck, "import_spec") == 0) {
                TSNode path_node = ts_node_child_by_field_name(child, "path", 4);
                if (ts_node_is_null(path_node)) {
                    continue;
                }
                char *path = strip_quotes(a, cbm_node_text(a, path_node, ctx->source));
                if (!path || !path[0]) {
                    continue;
                }

                TSNode name_node = ts_node_child_by_field_name(child, "name", 4);
                const char *local_name;
                if (!ts_node_is_null(name_node)) {
                    local_name = cbm_node_text(a, name_node, ctx->source);
                } else {
                    local_name = path_last(a, path);
                }

                CBMImport imp = {.local_name = local_name, .module_path = path};
                cbm_imports_push(&ctx->result->imports, a, imp);
            } else {
                // import_spec_list: contains multiple import_spec children
                uint32_t sc = ts_node_child_count(child);
                for (uint32_t k = 0; k < sc; k++) {
                    TSNode spec = ts_node_child(child, k);
                    if (strcmp(ts_node_type(spec), "import_spec") != 0) {
                        continue;
                    }

                    TSNode path_node = ts_node_child_by_field_name(spec, "path", 4);
                    if (ts_node_is_null(path_node)) {
                        continue;
                    }
                    char *path = strip_quotes(a, cbm_node_text(a, path_node, ctx->source));
                    if (!path || !path[0]) {
                        continue;
                    }

                    TSNode name_node = ts_node_child_by_field_name(spec, "name", 4);
                    const char *local_name;
                    if (!ts_node_is_null(name_node)) {
                        local_name = cbm_node_text(a, name_node, ctx->source);
                    } else {
                        local_name = path_last(a, path);
                    }

                    CBMImport imp = {.local_name = local_name, .module_path = path};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Python imports ---
// import_statement: import X, import X as Y
// import_from_statement: from X import Y, from X import Y as Z

static void parse_python_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    TSTreeCursor cursor = ts_tree_cursor_new(ctx->root);
    if (!ts_tree_cursor_goto_first_child(&cursor)) {
        ts_tree_cursor_delete(&cursor);
        return;
    }
    do {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);

        if (strcmp(kind, "import_statement") == 0) {
            // import X [as Y]
            TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
            if (ts_node_is_null(name_node)) {
                // Try children: dotted_name nodes
                uint32_t nc = ts_node_child_count(node);
                for (uint32_t j = 0; j < nc; j++) {
                    TSNode child = ts_node_child(node, j);
                    const char *ck = ts_node_type(child);
                    if (strcmp(ck, "dotted_name") == 0 || strcmp(ck, "identifier") == 0) {
                        char *mod = cbm_node_text(a, child, ctx->source);
                        if (mod && mod[0]) {
                            const char *local = path_last(a, mod);
                            CBMImport imp = {.local_name = local, .module_path = mod};
                            cbm_imports_push(&ctx->result->imports, a, imp);
                        }
                    } else if (strcmp(ck, "aliased_import") == 0) {
                        TSNode mod_node = ts_node_child_by_field_name(child, "name", 4);
                        TSNode alias_node = ts_node_child_by_field_name(child, "alias", 5);
                        if (!ts_node_is_null(mod_node)) {
                            char *mod = cbm_node_text(a, mod_node, ctx->source);
                            const char *local = !ts_node_is_null(alias_node)
                                                    ? cbm_node_text(a, alias_node, ctx->source)
                                                    : path_last(a, mod);
                            CBMImport imp = {.local_name = local, .module_path = mod};
                            cbm_imports_push(&ctx->result->imports, a, imp);
                        }
                    }
                }
            } else {
                char *mod = cbm_node_text(a, name_node, ctx->source);
                if (mod && mod[0]) {
                    CBMImport imp = {.local_name = path_last(a, mod), .module_path = mod};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
        } else if (strcmp(kind, "import_from_statement") == 0) {
            // from X import Y [as Z]
            TSNode module_node =
                ts_node_child_by_field_name(node, "module_name", FIELD_LEN_MODULE_NAME);
            if (ts_node_is_null(module_node)) {
                // Try alternative field names
                uint32_t nc = ts_node_child_count(node);
                for (uint32_t j = 0; j < nc; j++) {
                    TSNode c = ts_node_child(node, j);
                    if (strcmp(ts_node_type(c), "dotted_name") == 0 ||
                        strcmp(ts_node_type(c), "relative_import") == 0) {
                        module_node = c;
                        break;
                    }
                }
            }
            // NOLINTBEGIN(readability-implicit-bool-conversion)
            char *mod_path =
                ts_node_is_null(module_node) ? NULL : cbm_node_text(a, module_node, ctx->source);
            // NOLINTEND(readability-implicit-bool-conversion)

            // Find imported names
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode child = ts_node_child(node, j);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "identifier") == 0 || strcmp(ck, "dotted_name") == 0) {
                    // Skip the module name node
                    if (!ts_node_is_null(module_node) &&
                        ts_node_start_byte(child) == ts_node_start_byte(module_node)) {
                        continue;
                    }
                    char *name = cbm_node_text(a, child, ctx->source);
                    if (name && name[0]) {
                        const char *full =
                            mod_path ? cbm_arena_sprintf(a, "%s.%s", mod_path, name) : name;
                        CBMImport imp = {.local_name = name, .module_path = full};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                } else if (strcmp(ck, "aliased_import") == 0) {
                    TSNode name_n = ts_node_child_by_field_name(child, "name", 4);
                    TSNode alias_n = ts_node_child_by_field_name(child, "alias", 5);
                    if (!ts_node_is_null(name_n)) {
                        char *name = cbm_node_text(a, name_n, ctx->source);
                        const char *local = !ts_node_is_null(alias_n)
                                                ? cbm_node_text(a, alias_n, ctx->source)
                                                : name;
                        const char *full =
                            mod_path ? cbm_arena_sprintf(a, "%s.%s", mod_path, name) : name;
                        CBMImport imp = {.local_name = local, .module_path = full};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                    }
                }
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- ES module imports (JS/TS/TSX) ---
// import X from "Y"; import {A, B} from "Y"; import * as X from "Y"
// const X = require("Y")

// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk
static void walk_es_imports(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "import_statement") == 0) {
        TSNode source_node = ts_node_child_by_field_name(node, "source", 6);
        if (ts_node_is_null(source_node)) {
            // Try last string child
            uint32_t nc = ts_node_child_count(node);
            for (int j = (int)nc - 1; j >= 0; j--) {
                TSNode c = ts_node_child(node, (uint32_t)j);
                const char *ck = ts_node_type(c);
                if (strcmp(ck, "string") == 0 || strcmp(ck, "string_literal") == 0) {
                    source_node = c;
                    break;
                }
            }
        }
        if (ts_node_is_null(source_node)) {
            goto recurse;
        }

        char *path = strip_quotes(a, cbm_node_text(a, source_node, ctx->source));
        if (!path || !path[0]) {
            goto recurse;
        }

        // Default import, namespace import, named imports
        uint32_t nc = ts_node_child_count(node);
        bool found = false;
        for (uint32_t j = 0; j < nc; j++) {
            TSNode child = ts_node_child(node, j);
            const char *ck = ts_node_type(child);

            if (strcmp(ck, "identifier") == 0) {
                // Default import: import X from "Y"
                char *name = cbm_node_text(a, child, ctx->source);
                CBMImport imp = {.local_name = name, .module_path = path};
                cbm_imports_push(&ctx->result->imports, a, imp);
                found = true;
            } else if (strcmp(ck, "import_clause") == 0) {
                // Walk into import clause for named/default imports
                uint32_t cc = ts_node_child_count(child);
                for (uint32_t k = 0; k < cc; k++) {
                    TSNode sub = ts_node_child(child, k);
                    const char *sk = ts_node_type(sub);
                    if (strcmp(sk, "identifier") == 0) {
                        char *name = cbm_node_text(a, sub, ctx->source);
                        CBMImport imp = {.local_name = name, .module_path = path};
                        cbm_imports_push(&ctx->result->imports, a, imp);
                        found = true;
                    } else if (strcmp(sk, "namespace_import") == 0) {
                        TSNode as_name = ts_node_child_by_field_name(sub, "name", 4);
                        if (ts_node_is_null(as_name) && ts_node_child_count(sub) > 0) {
                            as_name = ts_node_child(sub, ts_node_child_count(sub) - 1);
                        }
                        if (!ts_node_is_null(as_name)) {
                            char *name = cbm_node_text(a, as_name, ctx->source);
                            CBMImport imp = {.local_name = name, .module_path = path};
                            cbm_imports_push(&ctx->result->imports, a, imp);
                            found = true;
                        }
                    } else if (strcmp(sk, "named_imports") == 0) {
                        uint32_t nc2 = ts_node_child_count(sub);
                        for (uint32_t m = 0; m < nc2; m++) {
                            TSNode imp_spec = ts_node_child(sub, m);
                            if (strcmp(ts_node_type(imp_spec), "import_specifier") == 0) {
                                TSNode local = ts_node_child_by_field_name(imp_spec, "alias", 5);
                                TSNode orig = ts_node_child_by_field_name(imp_spec, "name", 4);
                                if (ts_node_is_null(orig) && ts_node_child_count(imp_spec) > 0) {
                                    orig = ts_node_child(imp_spec, 0);
                                }
                                if (!ts_node_is_null(orig)) {
                                    char *local_name = !ts_node_is_null(local)
                                                           ? cbm_node_text(a, local, ctx->source)
                                                           : cbm_node_text(a, orig, ctx->source);
                                    CBMImport imp = {.local_name = local_name, .module_path = path};
                                    cbm_imports_push(&ctx->result->imports, a, imp);
                                    found = true;
                                }
                            }
                        }
                    }
                }
            }
        }

        if (!found) {
            // Side-effect import: import "Y"
            CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
            cbm_imports_push(&ctx->result->imports, a, imp);
        }
        return;
    }

recurse:;
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        walk_es_imports(ctx, ts_node_child(node, i));
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
        if (strncmp(full, "use ", 4) == 0) {
            full += 4;
        }
        size_t len = strlen(full);
        if (len > 0 && full[len - 1] == ';') {
            full[len - 1] = '\0';
        }

        CBMImport imp = {.local_name = path_last(a, full), .module_path = full};
        cbm_imports_push(&ctx->result->imports, a, imp);
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- C/C++ imports ---
// preproc_include -> path or string_literal

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

        TSNode path_node = ts_node_child_by_field_name(node, "path", 4);
        if (ts_node_is_null(path_node)) {
            // Try system_lib_string or string_literal
            uint32_t nc = ts_node_child_count(node);
            for (uint32_t j = 0; j < nc; j++) {
                TSNode c = ts_node_child(node, j);
                const char *ck = ts_node_type(c);
                if (strcmp(ck, "string_literal") == 0 || strcmp(ck, "system_lib_string") == 0) {
                    path_node = c;
                    break;
                }
            }
        }
        if (ts_node_is_null(path_node)) {
            continue;
        }

        char *path = strip_quotes(a, cbm_node_text(a, path_node, ctx->source));
        // Also strip angle brackets
        if (path && path[0] == '<') {
            size_t len = strlen(path);
            if (len > 1 && path[len - 1] == '>') {
                path = cbm_arena_strndup(a, path + 1, len - 2);
            }
        }
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

static void parse_ruby_imports(CBMExtractCtx *ctx) {
    CBMArena *a = ctx->arena;

    // Walk for call nodes with "require" or "require_relative"
    // Simple: walk top-level children
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

        TSNode method = ts_node_child_by_field_name(node, "method", 6);
        if (ts_node_is_null(method) && ts_node_child_count(node) > 0) {
            method = ts_node_child(node, 0);
        }
        if (ts_node_is_null(method)) {
            continue;
        }

        char *name = cbm_node_text(a, method, ctx->source);
        if (!name || (strcmp(name, "require") != 0 && strcmp(name, "require_relative") != 0)) {
            continue;
        }

        // Get the argument (string literal)
        TSNode args = ts_node_child_by_field_name(node, "arguments", 9);
        if (ts_node_is_null(args)) {
            // Try second child
            if (ts_node_child_count(node) > 1) {
                args = ts_node_child(node, 1);
            }
        }
        if (ts_node_is_null(args)) {
            continue;
        }

        // Find string literal in args
        char *arg_text = NULL;
        uint32_t ac = ts_node_child_count(args);
        for (uint32_t j = 0; j < ac; j++) {
            TSNode c = ts_node_child(args, j);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "string") == 0 || strcmp(ck, "string_literal") == 0) {
                arg_text = strip_quotes(a, cbm_node_text(a, c, ctx->source));
                break;
            }
        }
        if (!arg_text) {
            arg_text = strip_quotes(a, cbm_node_text(a, args, ctx->source));
        }
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

static void parse_generic_imports(CBMExtractCtx *ctx, const char *node_type) {
    CBMArena *a = ctx->arena;

    /* Use TSTreeCursor for O(1)-per-step sibling traversal.
     * ts_node_child(root, i) and ts_node_next_sibling() both call
     * ts_node_child_with_descendant() internally, making naive iteration
     * O(N²) on roots with thousands of children (e.g. generated .d.ts files).
     * ts_tree_cursor_goto_next_sibling() maintains cursor state and is O(1). */
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

        // Try to find a path/source/module field
        static const char *path_fields[] = {"path", "source", "module", "name", NULL};
        for (const char **f = path_fields; *f; f++) {
            TSNode path_node = ts_node_child_by_field_name(node, *f, (uint32_t)strlen(*f));
            if (!ts_node_is_null(path_node)) {
                char *path = strip_quotes(a, cbm_node_text(a, path_node, ctx->source));
                if (path && path[0]) {
                    CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
                break;
            }
        }

        // Fallback: use full node text minus keyword
        if (ctx->result->imports.count == 0) {
            char *text = cbm_node_text(a, node, ctx->source);
            if (text && text[0]) {
                // Strip leading keyword
                char *space = strchr(text, ' ');
                if (space) {
                    text = space + 1;
                }
                // Strip trailing semicolon
                size_t len = strlen(text);
                if (len > 0 && text[len - 1] == ';') {
                    text[len - 1] = '\0';
                }
                if (text[0]) {
                    CBMImport imp = {.local_name = path_last(a, text), .module_path = text};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
        }
    } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

// --- Wolfram imports ---
// get_top: << "package" (Get["file"])
// apply where first child is builtin_symbol "Needs" with string arg

// NOLINTNEXTLINE(misc-no-recursion) — intentional AST tree walk
static void walk_wolfram_imports(CBMExtractCtx *ctx, TSNode node) {
    CBMArena *a = ctx->arena;
    const char *kind = ts_node_type(node);

    // get_top: << "path" → import
    if (strcmp(kind, "get_top") == 0) {
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

    // Needs["package`"] — apply where head is builtin_symbol "Needs"
    if (strcmp(kind, "apply") == 0 && ts_node_named_child_count(node) >= 2) {
        TSNode head = ts_node_named_child(node, 0);
        if (strcmp(ts_node_type(head), "builtin_symbol") == 0) {
            char *name = cbm_node_text(a, head, ctx->source);
            if (name && strcmp(name, "Needs") == 0) {
                // Second named child should be the string argument
                TSNode arg = ts_node_named_child(node, 1);
                char *text = cbm_node_text(a, arg, ctx->source);
                if (text && text[0]) {
                    char *path = strip_quotes(a, text);
                    CBMImport imp = {.local_name = path_last(a, path), .module_path = path};
                    cbm_imports_push(&ctx->result->imports, a, imp);
                }
            }
        }
    }

    // Recurse
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        walk_wolfram_imports(ctx, ts_node_child(node, i));
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
