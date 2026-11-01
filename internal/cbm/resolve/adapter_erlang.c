/* Erlang binding rules, from the reference manual's module and function chapters.
 *
 * A module is named by its -module attribute and functions by name/arity; all
 * clauses of one name/arity are one function. Variables start uppercase, so an
 * atom in call position names a function and nothing local shadows it.
 *
 * `f(X)` selects the module's own f/1, else the f/1 of an -import, and otherwise
 * leaves the project (auto-imported BIF). `m:f(X)` selects m's f/1 and needs it
 * to be exported, by -export or export_all. `?MODULE:f(X)` is the same call on
 * the current module. A variable module or function is runtime dispatch.
 *
 * The graph's QN holds no arity, so f/1 and f/2 are one node: the edge names that
 * function name in that module, which is what the graph can represent. */
#include "source_adapter_internal.h"
#include <stdlib.h>

enum { ERLANG_FLAG_EXPORTED = CBM_SOURCE_DECL_F_PUBLIC };

typedef struct {
    const char *name;
    int arity;
    const char *module;
} erlang_import_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *module;
    bool export_all;
    const char **exports; /* "name/arity" */
    size_t export_count;
    erlang_import_t *imports;
    size_t import_count;
} erlang_ctx_t;

/* The attribute kinds a module header can use. */
static void erlang_header(erlang_ctx_t *ctx) {
    TSNode root = ctx->walk->root;
    uint32_t count = ts_node_named_child_count(root);
    size_t export_capacity = 0, import_capacity = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode form = ts_node_named_child(root, i);
        if (cbm_source_is(form, "module_attribute")) {
            ctx->module = cbm_source_text(ctx->walk, cbm_source_field(form, "name"));
        } else if (cbm_source_is(form, "compile_options_attribute")) {
            const char *text = cbm_source_text(ctx->walk, form);
            if (text && strstr(text, "export_all"))
                ctx->export_all = true;
        } else if (cbm_source_is(form, "export_attribute") ||
                   cbm_source_is(form, "import_attribute")) {
            bool import = cbm_source_is(form, "import_attribute");
            const char *module =
                import ? cbm_source_text(ctx->walk, cbm_source_field(form, "module")) : NULL;
            uint32_t members = ts_node_named_child_count(form);
            for (uint32_t m = 0; m < members; m++) {
                TSNode fa = ts_node_named_child(form, m);
                if (!cbm_source_is(fa, "fa"))
                    continue;
                const char *name = cbm_source_text(ctx->walk, cbm_source_field(fa, "fun"));
                TSNode arity_node = cbm_source_field(fa, "arity");
                const char *arity =
                    cbm_source_text(ctx->walk, cbm_source_field(arity_node, "value"));
                if (!name || !arity)
                    continue;
                if (import) {
                    if (ctx->import_count == import_capacity) {
                        size_t next = import_capacity ? import_capacity * 2 : 16;
                        erlang_import_t *items =
                            cbm_arena_alloc(ctx->walk->arena, next * sizeof(*items));
                        if (!items) {
                            cbm_source_fail(ctx->walk, "erlang_import_allocation");
                            return;
                        }
                        if (ctx->import_count)
                            memcpy(items, ctx->imports, ctx->import_count * sizeof(*items));
                        ctx->imports = items;
                        import_capacity = next;
                    }
                    ctx->imports[ctx->import_count++] =
                        (erlang_import_t){name, atoi(arity), module};
                } else {
                    if (ctx->export_count == export_capacity) {
                        size_t next = export_capacity ? export_capacity * 2 : 32;
                        const char **items =
                            cbm_arena_alloc(ctx->walk->arena, next * sizeof(*items));
                        if (!items) {
                            cbm_source_fail(ctx->walk, "erlang_export_allocation");
                            return;
                        }
                        if (ctx->export_count)
                            memcpy(items, ctx->exports, ctx->export_count * sizeof(*items));
                        ctx->exports = items;
                        export_capacity = next;
                    }
                    ctx->exports[ctx->export_count++] =
                        cbm_arena_sprintf(ctx->walk->arena, "%s/%s", name, arity);
                }
            }
        }
    }
}

static bool erlang_exported(const erlang_ctx_t *ctx, const char *name, int arity) {
    if (ctx->export_all)
        return true;
    size_t length = strlen(name);
    for (size_t i = 0; i < ctx->export_count; i++) {
        const char *entry = ctx->exports[i];
        if (entry && !strncmp(entry, name, length) && entry[length] == '/' &&
            atoi(entry + length + 1) == arity)
            return true;
    }
    return false;
}

static void erlang_collect(cbm_source_walk_t *walk) {
    erlang_ctx_t ctx = {.walk = walk};
    erlang_header(&ctx);
    if (!ctx.module)
        return; /* A header belongs to whichever module includes it. */
    (void)cbm_source_declare(walk, ctx.module, "", NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    TSNode root = walk->root;
    uint32_t count = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode form = ts_node_named_child(root, i);
        if (!cbm_source_is(form, "fun_decl"))
            continue;
        uint32_t clauses = ts_node_named_child_count(form);
        for (uint32_t c = 0; c < clauses; c++) {
            TSNode clause = ts_node_named_child(form, c);
            if (!cbm_source_is(clause, "function_clause"))
                continue;
            const char *name = cbm_source_text(walk, cbm_source_field(clause, "name"));
            TSNode args = cbm_source_field(clause, "args");
            if (!name || ts_node_is_null(args))
                continue;
            int arity = (int)ts_node_named_child_count(args);
            uint32_t line = ts_node_start_point(clause).row + 1;
            cbm_source_decl_t *decl = cbm_source_declare(
                walk, ctx.module, name, cbm_source_def_qn_label(walk, name, line, "Function"),
                erlang_exported(&ctx, name, arity) ? ERLANG_FLAG_EXPORTED : 0, line);
            if (decl) {
                decl->arity_min = arity;
                decl->arity_max = arity;
            }
        }
    }
}

static const char *erlang_select(const erlang_ctx_t *ctx, const char *module, const char *name,
                                 int arity, bool exported_only, const char **reason) {
    const char *selected = NULL;
    *reason = "erlang_function_absent_from_module";
    for (const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, module, name); decl;
         decl = decl->next) {
        if (decl->arity_min != arity) {
            *reason = "erlang_function_arity_absent_from_module";
            continue;
        }
        if (exported_only && !(decl->flags & ERLANG_FLAG_EXPORTED)) {
            *reason = "erlang_function_not_exported";
            continue;
        }
        if (!decl->qn) {
            *reason = "source_declaration_not_materialized";
            return NULL;
        }
        if (selected && strcmp(selected, decl->qn)) {
            *reason = "source_declaration_ambiguous";
            return NULL;
        }
        selected = decl->qn;
    }
    return selected;
}

static void erlang_visit_call(erlang_ctx_t *ctx, TSNode call) {
    TSNode expr = cbm_source_field(call, "expr");
    TSNode args = cbm_source_field(call, "args");
    int arity = ts_node_is_null(args) ? 0 : (int)ts_node_named_child_count(args);
    cbm_source_walk_t *walk = ctx->walk;
    if (cbm_source_is(expr, "atom")) {
        const char *name = cbm_source_text(walk, expr);
        const char *reason = NULL;
        const char *target = name && ctx->module
                                 ? erlang_select(ctx, ctx->module, name, arity, false, &reason)
                                 : NULL;
        if (target) {
            cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, target,
                                 CBM_RESOLVE_RULE_SCOPE_BINDING, ctx->module,
                                 "erlang_local_function_name_and_arity");
            return;
        }
        for (size_t i = 0; name && i < ctx->import_count; i++) {
            const erlang_import_t *import = &ctx->imports[i];
            if (import->arity != arity || strcmp(import->name, name) || !import->module)
                continue;
            target = erlang_select(ctx, import->module, name, arity, true, &reason);
            cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, target,
                                 CBM_RESOLVE_RULE_IMPORT_BINDING, import->module,
                                 target ? "erlang_imported_function_name_and_arity" : reason);
            return;
        }
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "erlang_name_not_in_module_or_imports");
        return;
    }
    if (cbm_source_is(expr, "remote")) {
        TSNode wrapper = cbm_source_field(expr, "module");
        TSNode module_node = cbm_source_field(wrapper, "module");
        TSNode fun = cbm_source_field(expr, "fun");
        const char *module = NULL;
        if (cbm_source_is(module_node, "atom")) {
            module = cbm_source_text(walk, module_node);
        } else if (cbm_source_is(module_node, "macro_call_expr")) {
            const char *macro = cbm_source_text(walk, cbm_source_field(module_node, "name"));
            module = macro && !strcmp(macro, "MODULE") ? ctx->module : NULL;
        }
        if (!module || !cbm_source_is(fun, "atom")) {
            cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                                 NULL, "erlang_dynamic_module_or_function");
            return;
        }
        const char *reason = NULL;
        const char *target =
            erlang_select(ctx, module, cbm_source_text(walk, fun), arity, true, &reason);
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, target,
                             CBM_RESOLVE_RULE_LANGUAGE_BINDING, module,
                             target ? "erlang_remote_exported_function" : reason);
        return;
    }
    cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                         "erlang_fun_value_call");
}

static void erlang_visit(erlang_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "call"))
        erlang_visit_call(ctx, node);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                erlang_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void erlang_resolve(cbm_source_walk_t *walk) {
    erlang_ctx_t ctx = {.walk = walk};
    erlang_header(&ctx);
    erlang_visit(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_erlang = {.language = CBM_LANG_ERLANG,
                                                        .collect = erlang_collect,
                                                        .resolve = erlang_resolve,
                                                        .one_name_one_node = true};
