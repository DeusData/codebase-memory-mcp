/* Vim script binding rules, from `:help user-function`, `:help script-local` and
 * `:help autoload`.
 *
 * The prefix of a function name says where it lives, and nothing can move it:
 *   s:Name, <SID>Name   the script's own functions, one namespace per file;
 *   a#b#c               an autoload function, which only runtime file autoload/a/b.vim defines;
 *   Name, g:Name        one global namespace for the whole editor session.
 * A bare name that starts in lower case is a builtin: `:function` refuses to declare one, so a
 * project declaration can never be what `get(...)` means.
 *
 * A global is read from the file's own declaration when it has one, then from the one project
 * file that declares it. Two files declaring a global leave it open: which one is in effect
 * depends on what was sourced last. A variable holding a Funcref is looked up before a function
 * of that name (`let F = function('X')`, `call F()`), so a capitalized name a file also assigns
 * abstains there. A dict function is selected by the dict's runtime value and stays unknown. */
#include "source_adapter_internal.h"
#include <ctype.h>
#include <stdio.h>

#define VIM_GLOBAL "G"
#define VIM_AUTOLOAD "A"
#define VIM_SCRIPT "S:"
#define VIM_OWN "H:"
#define VIM_ASSIGNED "V:"

typedef struct {
    cbm_source_walk_t *walk;
    const char *script_owner;
    const char *own_owner;
    const char *assigned_owner;
} vim_ctx_t;

static bool vim_script_scope(const char *text) {
    return !strncmp(text, "s:", 2) || !strncmp(text, "<SID>", 5) || !strncmp(text, "<sid>", 5);
}

static void vim_declare_once(vim_ctx_t *ctx, const char *owner, const char *name, const char *qn) {
    cbm_source_decl_t *head = (cbm_source_decl_t *)cbm_source_lookup(ctx->walk, owner, name);
    if (!head) {
        (void)cbm_source_declare(ctx->walk, owner, name, qn, 0, 0);
        return;
    }
    if (!qn || !head->qn || strcmp(qn, head->qn))
        head->flags |= CBM_SOURCE_DECL_F_OPEN;
}

/* a#b#c belongs in autoload/a/b.vim, the only file `:help autoload` reads for it. */
static bool vim_autoload_file(const vim_ctx_t *ctx, const char *name) {
    const char *last = strrchr(name, '#');
    const char *file = ctx->walk->file;
    if (!last || !file)
        return false;
    char path[512];
    int length = snprintf(path, sizeof(path), "autoload/%.*s.vim", (int)(last - name), name);
    if (length <= 0 || (size_t)length >= sizeof(path))
        return false;
    for (char *p = path; *p; p++)
        if (*p == '#')
            *p = '/';
    size_t size = strlen(file);
    if (size < (size_t)length || strcmp(file + size - (size_t)length, path))
        return false;
    return size == (size_t)length || file[size - (size_t)length - 1] == '/';
}

static void vim_collect_function(vim_ctx_t *ctx, TSNode node) {
    cbm_source_walk_t *walk = ctx->walk;
    TSNode declaration = (TSNode){0};
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count && ts_node_is_null(declaration); i++)
        if (cbm_source_is(ts_node_named_child(node, i), "function_declaration"))
            declaration = ts_node_named_child(node, i);
    TSNode name = cbm_source_field(declaration, "name");
    const char *text = cbm_source_text(walk, name);
    if (!text)
        return;
    const char *qn = cbm_source_def_qn_at(walk, text, ts_node_start_point(node).row + 1);
    if (cbm_source_is(name, "scoped_identifier")) {
        const char *bare = cbm_source_text(walk, ts_node_named_child(name, 1));
        if (!bare)
            return;
        if (vim_script_scope(text)) {
            vim_declare_once(ctx, ctx->script_owner, bare, qn);
        } else if (!strncmp(text, "g:", 2)) {
            vim_declare_once(ctx, VIM_GLOBAL, bare, qn);
            vim_declare_once(ctx, ctx->own_owner, bare, qn);
        }
        return;
    }
    if (!cbm_source_is(name, "identifier"))
        return;
    if (strchr(text, '#')) {
        /* The file's own definition is what it reads, wherever the file sits. */
        vim_declare_once(ctx, ctx->own_owner, text, qn);
        if (vim_autoload_file(ctx, text))
            vim_declare_once(ctx, VIM_AUTOLOAD, text, qn);
        return;
    }
    vim_declare_once(ctx, VIM_GLOBAL, text, qn);
    vim_declare_once(ctx, ctx->own_owner, text, qn);
}

/* A capitalized name assigned anywhere in the file can hold a Funcref. */
static void vim_collect_assigned(vim_ctx_t *ctx, TSNode node) {
    if (!ts_node_named_child_count(node))
        return;
    TSNode target = ts_node_named_child(node, 0);
    if (cbm_source_is(target, "scoped_identifier"))
        target = ts_node_named_child(target, 1);
    if (!cbm_source_is(target, "identifier"))
        return;
    const char *text = cbm_source_text(ctx->walk, target);
    if (text && isupper((unsigned char)text[0]))
        vim_declare_once(ctx, ctx->assigned_owner, text, NULL);
}

static void vim_collect_walk(vim_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "function_definition"))
        vim_collect_function(ctx, node);
    else if (cbm_source_is(node, "let_statement") || cbm_source_is(node, "const_statement") ||
             cbm_source_is(node, "for_loop"))
        vim_collect_assigned(ctx, node);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                vim_collect_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static bool vim_ctx_init(vim_ctx_t *ctx, cbm_source_walk_t *walk) {
    *ctx = (vim_ctx_t){.walk = walk};
    ctx->script_owner = cbm_arena_sprintf(walk->arena, VIM_SCRIPT "%s", walk->file);
    ctx->own_owner = cbm_arena_sprintf(walk->arena, VIM_OWN "%s", walk->file);
    ctx->assigned_owner = cbm_arena_sprintf(walk->arena, VIM_ASSIGNED "%s", walk->file);
    if (!ctx->script_owner || !ctx->own_owner || !ctx->assigned_owner) {
        cbm_source_fail(walk, "vim_scope_allocation");
        return false;
    }
    return true;
}

static void vim_collect(cbm_source_walk_t *walk) {
    walk->table = walk->writable;
    vim_ctx_t ctx;
    if (vim_ctx_init(&ctx, walk))
        vim_collect_walk(&ctx, walk->root, 0);
}

static void vim_emit(vim_ctx_t *ctx, TSNode call, const cbm_source_decl_t *decl, const char *domain,
                     const char *absent) {
    if (!decl) {
        cbm_source_emit_node(ctx->walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                             NULL, absent);
        return;
    }
    if (decl->flags & CBM_SOURCE_DECL_F_OPEN) {
        cbm_source_emit_node(ctx->walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                             NULL, "vim_declared_by_more_than_one_file");
        return;
    }
    cbm_resolve_rule_t rule = strcmp(decl->file, ctx->walk->file) ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                                                  : CBM_RESOLVE_RULE_SCOPE_BINDING;
    cbm_source_emit_node(ctx->walk, call, CBM_SOURCE_SITE_CALL, decl->qn, rule, domain,
                         decl->qn ? "vim_namespace_declaration" : "vim_function_has_no_graph_node");
}

static void vim_emit_global(vim_ctx_t *ctx, TSNode call, const char *name) {
    const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, ctx->own_owner, name);
    vim_emit(ctx, call, own ? own : cbm_source_lookup(ctx->walk, VIM_GLOBAL, name), VIM_GLOBAL,
             "vim_global_not_declared_in_project");
}

static void vim_visit_call(vim_ctx_t *ctx, TSNode call) {
    cbm_source_walk_t *walk = ctx->walk;
    TSNode function = cbm_source_field(call, "function");
    const char *text = cbm_source_text(walk, function);
    if (!text)
        return;
    if (cbm_source_is(function, "scoped_identifier")) {
        const char *bare = cbm_source_text(walk, ts_node_named_child(function, 1));
        if (!bare)
            return;
        if (vim_script_scope(text)) {
            vim_emit(ctx, call, cbm_source_lookup(walk, ctx->script_owner, bare), ctx->script_owner,
                     "vim_script_function_not_in_this_file");
        } else if (!strncmp(text, "g:", 2)) {
            vim_emit_global(ctx, call, bare);
        } else {
            cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                                 NULL, "vim_funcref_in_a_variable");
        }
        return;
    }
    if (!cbm_source_is(function, "identifier")) {
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "vim_callee_is_a_runtime_value");
        return;
    }
    if (strchr(text, '#')) {
        const cbm_source_decl_t *own = cbm_source_lookup(walk, ctx->own_owner, text);
        vim_emit(ctx, call, own ? own : cbm_source_lookup(walk, VIM_AUTOLOAD, text), VIM_AUTOLOAD,
                 "vim_autoload_file_not_in_project");
        return;
    }
    if (!isupper((unsigned char)text[0])) {
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "vim_builtin_function");
        return;
    }
    if (cbm_source_lookup(walk, ctx->assigned_owner, text)) {
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "vim_name_also_assigned_a_funcref");
        return;
    }
    vim_emit_global(ctx, call, text);
}

static void vim_resolve_walk(vim_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "call_expression"))
        vim_visit_call(ctx, node);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                vim_resolve_walk(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void vim_resolve(cbm_source_walk_t *walk) {
    vim_ctx_t ctx;
    if (vim_ctx_init(&ctx, walk))
        vim_resolve_walk(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_vim = {.language = CBM_LANG_VIMSCRIPT,
                                                     .collect = vim_collect,
                                                     .resolve = vim_resolve,
                                                     .parse_errors_are_local = true,
                                                     .one_name_one_node = true};
