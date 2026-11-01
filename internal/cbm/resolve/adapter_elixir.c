/* Elixir binding rules, from the language's compile-time name resolution.
 *
 * A module is a global name, written in full wherever it is declared; a nested
 * defmodule concatenates and leaves an alias in its parent. `alias` and `import`
 * are lexical: they hold from their statement to the end of the enclosing block.
 * Functions are identified by name and arity, and a parenthesised `f(x)` is
 * always a function call: variables are invoked as `f.(x)`, so no local can
 * shadow it. An unqualified call selects a function of the enclosing module with
 * that arity, else an imported one; two imports providing it are a compile
 * error, so a second candidate is no decision here. Functions of an enclosing
 * module are not visible in a nested one.
 *
 * Inside `quote` a name belongs to the eventual expansion site, so nothing there
 * is decided. A remote call reaches only `def`/`defmacro`, never `defp`.
 *
 * The graph's Elixir QN holds neither module nor arity, so all clauses and
 * arities of one name in a module are one node. Two modules of a file sharing a
 * name share a node too, and that identity cannot name either: such a target
 * abstains in cbm_source_unique_qn rather than picking one. */
#include "source_adapter_internal.h"
#include <stdlib.h>

typedef struct {
    const char *local;
    const char *module;
} elixir_alias_t;

typedef struct {
    const char *module;
    TSNode only, except;
} elixir_import_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *module; /* NULL at file level */
    elixir_alias_t *aliases;
    size_t alias_count, alias_capacity;
    elixir_import_t *imports;
    size_t import_count, import_capacity;
} elixir_ctx_t;

static TSNode elixir_arguments(TSNode call) {
    /* The grammar gives a call's arguments no field name. */
    uint32_t count = ts_node_named_child_count(call);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(call, i);
        if (cbm_source_is(child, "arguments"))
            return child;
    }
    return (TSNode){0};
}

static TSNode elixir_do_block(TSNode call) {
    uint32_t count = ts_node_named_child_count(call);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(call, i);
        if (cbm_source_is(child, "do_block"))
            return child;
    }
    return (TSNode){0};
}

static const char *elixir_target_name(cbm_source_walk_t *walk, TSNode call) {
    TSNode target = cbm_source_field(call, "target");
    return cbm_source_is(target, "identifier") ? cbm_source_text(walk, target) : NULL;
}

static bool elixir_name_in(const char *name, const char *const *names) {
    for (size_t i = 0; name && names[i]; i++)
        if (!strcmp(name, names[i]))
            return true;
    return false;
}

static bool elixir_definer(const char *name) {
    static const char *const names[] = {"def",      "defp",      "defmacro", "defmacrop",
                                        "defguard", "defguardp", NULL};
    return elixir_name_in(name, names);
}

static bool elixir_public_definer(const char *name) {
    return name && (!strcmp(name, "def") || !strcmp(name, "defmacro") || !strcmp(name, "defguard"));
}

/* A head is `name(args)` or a bare `name`, optionally under `when`. */
static TSNode elixir_head(TSNode definition) {
    TSNode arguments = elixir_arguments(definition);
    TSNode head = ts_node_is_null(arguments) ? (TSNode){0} : ts_node_named_child(arguments, 0);
    if (cbm_source_is(head, "binary_operator") && cbm_source_token(head, "when"))
        head = cbm_source_field(head, "left");
    return head;
}

static const char *elixir_module_of_declaration(elixir_ctx_t *ctx, TSNode call) {
    TSNode arguments = elixir_arguments(call);
    TSNode name = ts_node_is_null(arguments) ? (TSNode){0} : ts_node_named_child(arguments, 0);
    if (!cbm_source_is(name, "alias"))
        return NULL;
    const char *text = cbm_source_text(ctx->walk, name);
    if (!text)
        return NULL;
    return ctx->module ? cbm_arena_sprintf(ctx->walk->arena, "%s.%s", ctx->module, text) : text;
}

static void elixir_declare_function(elixir_ctx_t *ctx, TSNode call, const char *definer) {
    TSNode head = elixir_head(call);
    const char *name = NULL;
    int arity_min = 0, arity_max = 0;
    if (cbm_source_is(head, "identifier")) {
        name = cbm_source_text(ctx->walk, head);
    } else if (cbm_source_is(head, "call") &&
               cbm_source_is(cbm_source_field(head, "target"), "identifier")) {
        name = cbm_source_text(ctx->walk, cbm_source_field(head, "target"));
        TSNode parameters = elixir_arguments(head);
        uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
        for (uint32_t i = 0; i < count; i++) {
            TSNode parameter = ts_node_named_child(parameters, i);
            arity_max++;
            /* `a \\ 1` makes the arity optional at this position. */
            bool defaulted =
                cbm_source_is(parameter, "binary_operator") && cbm_source_token(parameter, "\\\\");
            if (!defaulted)
                arity_min++;
        }
    }
    if (!name || !ctx->module)
        return;
    uint32_t line = ts_node_start_point(call).row + 1;
    cbm_source_decl_t *decl = cbm_source_declare(
        ctx->walk, ctx->module, name, cbm_source_def_qn_label(ctx->walk, name, line, "Function"),
        elixir_public_definer(definer) ? CBM_SOURCE_DECL_F_PUBLIC : CBM_SOURCE_DECL_F_PRIVATE,
        line);
    if (decl) {
        decl->arity_min = arity_min;
        decl->arity_max = arity_max;
    }
}

static void elixir_collect_node(elixir_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "call")) {
            const char *name = elixir_target_name(ctx->walk, child);
            if (name && !strcmp(name, "quote"))
                continue;
            if (name && !strcmp(name, "defmodule")) {
                const char *module = elixir_module_of_declaration(ctx, child);
                if (!module)
                    continue;
                uint32_t line = ts_node_start_point(child).row + 1;
                (void)cbm_source_declare(ctx->walk, module, "", NULL, CBM_SOURCE_DECL_F_TYPE, line);
                const char *saved = ctx->module;
                ctx->module = module;
                TSNode body = elixir_do_block(child);
                if (!ts_node_is_null(body))
                    elixir_collect_node(ctx, body, depth + 1);
                ctx->module = saved;
                continue;
            }
            if (name && elixir_definer(name)) {
                elixir_declare_function(ctx, child, name);
                continue;
            }
            /* Protocol and implementation bodies have their own dispatch, which
             * these rules do not state; their functions are not modelled. */
            if (name && (!strcmp(name, "defprotocol") || !strcmp(name, "defimpl")))
                continue;
        }
        elixir_collect_node(ctx, child, depth + 1);
    }
}

static void elixir_collect(cbm_source_walk_t *walk) {
    elixir_ctx_t ctx = {.walk = walk};
    elixir_collect_node(&ctx, walk->root, 0);
}

static void elixir_add_alias(elixir_ctx_t *ctx, const char *local, const char *module) {
    if (!local || !module)
        return;
    if (ctx->alias_count == ctx->alias_capacity) {
        size_t capacity = ctx->alias_capacity ? ctx->alias_capacity * 2 : 16;
        elixir_alias_t *items = cbm_arena_alloc(ctx->walk->arena, capacity * sizeof(*items));
        if (!items) {
            cbm_source_fail(ctx->walk, "elixir_alias_allocation");
            return;
        }
        if (ctx->alias_count)
            memcpy(items, ctx->aliases, ctx->alias_count * sizeof(*items));
        ctx->aliases = items;
        ctx->alias_capacity = capacity;
    }
    ctx->aliases[ctx->alias_count++] = (elixir_alias_t){local, module};
}

/* The innermost alias of a name wins; a later statement in a block shadows an
 * earlier one, so the table is read from its end. */
static const char *elixir_expand_alias(const elixir_ctx_t *ctx, const char *text) {
    const char *dot = strchr(text, '.');
    size_t length = dot ? (size_t)(dot - text) : strlen(text);
    for (size_t i = ctx->alias_count; i > 0; i--) {
        const elixir_alias_t *alias = &ctx->aliases[i - 1];
        if (strlen(alias->local) == length && !strncmp(alias->local, text, length))
            return dot ? cbm_arena_sprintf(ctx->walk->arena, "%s%s", alias->module, dot)
                       : alias->module;
    }
    return text;
}

static const char *elixir_module_operand(elixir_ctx_t *ctx, TSNode node) {
    if (cbm_source_is(node, "alias"))
        return elixir_expand_alias(ctx, cbm_source_text(ctx->walk, node));
    if (cbm_source_is(node, "identifier")) {
        const char *text = cbm_source_text(ctx->walk, node);
        return text && !strcmp(text, "__MODULE__") ? ctx->module : NULL;
    }
    if (cbm_source_is(node, "dot")) {
        const char *left = elixir_module_operand(ctx, cbm_source_field(node, "left"));
        TSNode right = cbm_source_field(node, "right");
        const char *text = cbm_source_is(right, "alias") ? cbm_source_text(ctx->walk, right) : NULL;
        return left && text ? cbm_arena_sprintf(ctx->walk->arena, "%s.%s", left, text) : NULL;
    }
    return NULL;
}

static const char *elixir_leaf(const char *module) {
    const char *dot = strrchr(module, '.');
    return dot ? dot + 1 : module;
}

static void elixir_note_alias(elixir_ctx_t *ctx, TSNode call) {
    TSNode arguments = elixir_arguments(call);
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    if (!count)
        return;
    TSNode target = ts_node_named_child(arguments, 0);
    /* `alias A.{B, C.D}` is one alias per tuple member. */
    if (cbm_source_is(target, "dot") && cbm_source_is(cbm_source_field(target, "right"), "tuple")) {
        const char *prefix = elixir_module_operand(ctx, cbm_source_field(target, "left"));
        TSNode tuple = cbm_source_field(target, "right");
        uint32_t members = ts_node_named_child_count(tuple);
        for (uint32_t i = 0; prefix && i < members; i++) {
            TSNode member = ts_node_named_child(tuple, i);
            const char *text =
                cbm_source_is(member, "alias") ? cbm_source_text(ctx->walk, member) : NULL;
            if (text)
                elixir_add_alias(ctx, elixir_leaf(text),
                                 cbm_arena_sprintf(ctx->walk->arena, "%s.%s", prefix, text));
        }
        return;
    }
    const char *module = elixir_module_operand(ctx, target);
    if (!module)
        return;
    const char *local = elixir_leaf(module);
    for (uint32_t i = 1; i < count; i++) {
        TSNode keywords = ts_node_named_child(arguments, i);
        if (!cbm_source_is(keywords, "keywords"))
            continue;
        uint32_t pairs = ts_node_named_child_count(keywords);
        for (uint32_t p = 0; p < pairs; p++) {
            TSNode pair = ts_node_named_child(keywords, p);
            const char *key = cbm_source_text(ctx->walk, cbm_source_field(pair, "key"));
            TSNode value = cbm_source_field(pair, "value");
            if (key && !strncmp(key, "as:", 3) && cbm_source_is(value, "alias"))
                local = cbm_source_text(ctx->walk, value);
        }
    }
    elixir_add_alias(ctx, local, module);
}

static void elixir_note_import(elixir_ctx_t *ctx, TSNode call) {
    TSNode arguments = elixir_arguments(call);
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    const char *module =
        count ? elixir_module_operand(ctx, ts_node_named_child(arguments, 0)) : NULL;
    if (!module)
        return;
    elixir_import_t import = {.module = module};
    for (uint32_t i = 1; i < count; i++) {
        TSNode keywords = ts_node_named_child(arguments, i);
        if (!cbm_source_is(keywords, "keywords"))
            continue;
        uint32_t pairs = ts_node_named_child_count(keywords);
        for (uint32_t p = 0; p < pairs; p++) {
            TSNode pair = ts_node_named_child(keywords, p);
            const char *key = cbm_source_text(ctx->walk, cbm_source_field(pair, "key"));
            if (key && !strncmp(key, "only:", 5))
                import.only = cbm_source_field(pair, "value");
            else if (key && !strncmp(key, "except:", 7))
                import.except = cbm_source_field(pair, "value");
        }
    }
    if (ctx->import_count == ctx->import_capacity) {
        size_t capacity = ctx->import_capacity ? ctx->import_capacity * 2 : 8;
        elixir_import_t *items = cbm_arena_alloc(ctx->walk->arena, capacity * sizeof(*items));
        if (!items) {
            cbm_source_fail(ctx->walk, "elixir_import_allocation");
            return;
        }
        if (ctx->import_count)
            memcpy(items, ctx->imports, ctx->import_count * sizeof(*items));
        ctx->imports = items;
        ctx->import_capacity = capacity;
    }
    ctx->imports[ctx->import_count++] = import;
}

/* `only: [a: 1]` or `except: [a: 1]`. `only: :functions` and any computed list
 * leave membership unknown, which is reported as the third state. */
typedef enum { ELIXIR_LIST_NO, ELIXIR_LIST_YES, ELIXIR_LIST_UNKNOWN } elixir_list_t;

static elixir_list_t elixir_filter_has(elixir_ctx_t *ctx, TSNode filter, const char *name,
                                       int arity) {
    if (!cbm_source_is(filter, "list"))
        return ELIXIR_LIST_UNKNOWN;
    uint32_t count = ts_node_named_child_count(filter);
    for (uint32_t i = 0; i < count; i++) {
        TSNode keywords = ts_node_named_child(filter, i);
        if (!cbm_source_is(keywords, "keywords"))
            return ELIXIR_LIST_UNKNOWN;
        uint32_t pairs = ts_node_named_child_count(keywords);
        for (uint32_t p = 0; p < pairs; p++) {
            TSNode pair = ts_node_named_child(keywords, p);
            const char *key = cbm_source_text(ctx->walk, cbm_source_field(pair, "key"));
            const char *value = cbm_source_text(ctx->walk, cbm_source_field(pair, "value"));
            if (!key || !value)
                return ELIXIR_LIST_UNKNOWN;
            size_t length = strlen(name);
            if (!strncmp(key, name, length) && key[length] == ':' && atoi(value) == arity)
                return ELIXIR_LIST_YES;
        }
    }
    return ELIXIR_LIST_NO;
}

/* Every declaration of (module, name) accepting this arity; several QNs mean
 * the name is not one declaration. */
static const char *elixir_select(elixir_ctx_t *ctx, const char *module, const char *name, int arity,
                                 bool public_only, const char **reason) {
    const char *selected = NULL;
    *reason = "elixir_function_absent_from_module";
    for (const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, module, name); decl;
         decl = decl->next) {
        bool fits = arity >= 0 && decl->arity_min >= 0 && arity >= decl->arity_min &&
                    arity <= decl->arity_max;
        if (!fits) {
            *reason = "elixir_function_arity_absent_from_module";
            continue;
        }
        if (public_only && (decl->flags & CBM_SOURCE_DECL_F_PRIVATE)) {
            *reason = "elixir_private_function_from_other_module";
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
    /* One name's node must be owned by one module to denote this function. */
    if (selected) {
        const char *identity = NULL;
        selected = cbm_source_unique_qn(cbm_source_lookup(ctx->walk, module, name), &identity)
                       ? selected
                       : NULL;
        if (!selected) {
            *reason = identity;
            return NULL;
        }
    }
    return selected;
}

static void elixir_emit(elixir_ctx_t *ctx, TSNode node, const char *target, cbm_resolve_rule_t rule,
                        const char *domain, const char *reason) {
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_CALL, target, rule, domain, reason);
}

static void elixir_visit_bare_call(elixir_ctx_t *ctx, TSNode call, const char *name, int arity) {
    if (!ctx->module) {
        elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL, "elixir_call_outside_module");
        return;
    }
    const char *reason = NULL;
    const char *target = elixir_select(ctx, ctx->module, name, arity, false, &reason);
    if (target) {
        elixir_emit(ctx, call, target, CBM_RESOLVE_RULE_SCOPE_BINDING, ctx->module,
                    "elixir_local_function_name_and_arity");
        return;
    }
    if (reason && (!strcmp(reason, "source_declaration_ambiguous") ||
                   !strcmp(reason, "source_declaration_not_materialized") ||
                   !strcmp(reason, "source_graph_identity_shared_by_owners"))) {
        elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL, reason);
        return;
    }
    const char *selected = NULL, *from = NULL;
    for (size_t i = 0; i < ctx->import_count; i++) {
        const elixir_import_t *import = &ctx->imports[i];
        if (!ts_node_is_null(import->only)) {
            elixir_list_t member = elixir_filter_has(ctx, import->only, name, arity);
            if (member == ELIXIR_LIST_UNKNOWN) {
                elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                            "elixir_import_filter_not_literal");
                return;
            }
            if (member == ELIXIR_LIST_NO)
                continue;
        }
        if (!ts_node_is_null(import->except)) {
            elixir_list_t member = elixir_filter_has(ctx, import->except, name, arity);
            if (member == ELIXIR_LIST_UNKNOWN) {
                elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                            "elixir_import_filter_not_literal");
                return;
            }
            if (member == ELIXIR_LIST_YES)
                continue;
        }
        const char *candidate = NULL;
        const char *why = NULL;
        candidate = elixir_select(ctx, import->module, name, arity, true, &why);
        if (!candidate)
            continue;
        if (selected && strcmp(selected, candidate)) {
            elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                        "elixir_two_imports_provide_name");
            return;
        }
        selected = candidate;
        from = import->module;
    }
    if (selected)
        elixir_emit(ctx, call, selected, CBM_RESOLVE_RULE_IMPORT_BINDING, from,
                    "elixir_imported_function_name_and_arity");
    else
        elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                    "elixir_name_not_in_module_or_project_imports");
}

static void elixir_visit_remote_call(elixir_ctx_t *ctx, TSNode call, TSNode dot, int arity) {
    TSNode left = cbm_source_field(dot, "left");
    TSNode right = cbm_source_field(dot, "right");
    if (!cbm_source_is(right, "identifier")) {
        elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL, "elixir_anonymous_function_call");
        return;
    }
    const char *module = elixir_module_operand(ctx, left);
    if (!module) {
        elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL, "elixir_receiver_not_a_module");
        return;
    }
    const char *reason = NULL;
    const char *target = elixir_select(ctx, module, cbm_source_text(ctx->walk, right), arity,
                                       !ctx->module || strcmp(ctx->module, module), &reason);
    bool direct = cbm_source_is(left, "alias") && !strcmp(cbm_source_text(ctx->walk, left), module);
    elixir_emit(ctx, call, target,
                direct ? CBM_RESOLVE_RULE_LANGUAGE_BINDING : CBM_RESOLVE_RULE_IMPORT_BINDING,
                module, target ? "elixir_remote_module_function" : reason);
}

static void elixir_visit(elixir_ctx_t *ctx, TSNode node, bool piped, unsigned depth);

static void elixir_visit_children(elixir_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                elixir_visit(ctx, child, false, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* A do_block and the statements after it end its aliases and imports. */
static void elixir_visit_scoped(elixir_ctx_t *ctx, TSNode node, unsigned depth) {
    size_t aliases = ctx->alias_count, imports = ctx->import_count;
    elixir_visit_children(ctx, node, depth);
    ctx->alias_count = aliases;
    ctx->import_count = imports;
}

static void elixir_visit_call(elixir_ctx_t *ctx, TSNode node, bool piped, unsigned depth) {
    const char *name = elixir_target_name(ctx->walk, node);
    TSNode arguments = elixir_arguments(node);
    TSNode body = elixir_do_block(node);
    int arity = ts_node_is_null(arguments) ? 0 : (int)ts_node_named_child_count(arguments);
    if (piped)
        arity++;
    if (!ts_node_is_null(body))
        arity++; /* A do block is the trailing keyword argument. */
    if (name) {
        if (!strcmp(name, "quote") || !strcmp(name, "defprotocol") || !strcmp(name, "defimpl"))
            return;
        if (!strcmp(name, "defmodule")) {
            const char *module = elixir_module_of_declaration(ctx, node);
            if (!module)
                return;
            /* A nested defmodule leaves an alias of its last segment in the parent. */
            if (ctx->module)
                elixir_add_alias(ctx, cbm_source_text(ctx->walk, ts_node_named_child(arguments, 0)),
                                 module);
            const char *saved = ctx->module;
            size_t imports = ctx->import_count;
            ctx->module = module;
            ctx->import_count = 0;
            if (!ts_node_is_null(body))
                elixir_visit_scoped(ctx, body, depth + 1);
            ctx->module = saved;
            ctx->import_count = imports;
            return;
        }
        if (elixir_definer(name)) {
            /* Heads are patterns and guards; only the body runs at the call. */
            TSNode head_arguments = elixir_arguments(node);
            uint32_t count =
                ts_node_is_null(head_arguments) ? 0 : ts_node_named_child_count(head_arguments);
            for (uint32_t i = 1; i < count; i++)
                elixir_visit_scoped(ctx, ts_node_named_child(head_arguments, i), depth + 1);
            if (!ts_node_is_null(body))
                elixir_visit_scoped(ctx, body, depth + 1);
            return;
        }
        if (!strcmp(name, "alias")) {
            elixir_note_alias(ctx, node);
            return;
        }
        if (!strcmp(name, "require")) {
            /* `require A, as: B` also aliases; a plain require does not. */
            uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
            if (count > 1)
                elixir_note_alias(ctx, node);
            return;
        }
        if (!strcmp(name, "import")) {
            elixir_note_import(ctx, node);
            return;
        }
        if (!strcmp(name, "use"))
            return;
        elixir_visit_bare_call(ctx, node, name, arity);
    } else if (cbm_source_is(cbm_source_field(node, "target"), "dot")) {
        elixir_visit_remote_call(ctx, node, cbm_source_field(node, "target"), arity);
    }
    if (!ts_node_is_null(arguments))
        elixir_visit_children(ctx, arguments, depth);
    if (!ts_node_is_null(body))
        elixir_visit_scoped(ctx, body, depth + 1);
}

static void elixir_visit(elixir_ctx_t *ctx, TSNode node, bool piped, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "call")) {
        elixir_visit_call(ctx, node, piped, depth);
        return;
    }
    /* A module attribute evaluates at compile time and typespecs are not code. */
    if (cbm_source_is(node, "unary_operator") && cbm_source_token(node, "@"))
        return;
    /* `&name/1` and `&Mod.name/1` capture; they do not invoke. */
    if (cbm_source_is(node, "unary_operator") && cbm_source_token(node, "&")) {
        TSNode operand = cbm_source_field(node, "operand");
        if (cbm_source_is(operand, "binary_operator") && cbm_source_token(operand, "/")) {
            TSNode left = cbm_source_field(operand, "left");
            if (cbm_source_is(left, "call"))
                elixir_emit(ctx, left, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                            "elixir_capture_is_not_an_invocation");
            return;
        }
    }
    if (cbm_source_is(node, "binary_operator") && cbm_source_token(node, "|>")) {
        elixir_visit(ctx, cbm_source_field(node, "left"), false, depth + 1);
        elixir_visit(ctx, cbm_source_field(node, "right"), true, depth + 1);
        return;
    }
    if (cbm_source_is(node, "do_block") || cbm_source_is(node, "stab_clause") ||
        cbm_source_is(node, "anonymous_function")) {
        elixir_visit_scoped(ctx, node, depth);
        return;
    }
    elixir_visit_children(ctx, node, depth);
}

static void elixir_resolve(cbm_source_walk_t *walk) {
    elixir_ctx_t ctx = {.walk = walk};
    elixir_visit_children(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_elixir = {.language = CBM_LANG_ELIXIR,
                                                        .collect = elixir_collect,
                                                        .resolve = elixir_resolve,
                                                        .one_name_one_node = true};
