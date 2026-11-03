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
 * `use M` expands M's `__using__`. What that macro returns is known when its last
 * statement is a `quote`: an `import X` or `use Y` standing directly in that quote holds
 * at the use site, and a `def` standing there is a function of the using module, which
 * has no node of its own, so a call of that name abstains.
 *
 * `:name.f(x)` is a call of Erlang module name, so it selects that module's exported
 * f/1 when the project has one. `Proto.f(x)` selects the function a `defprotocol`
 * declares; which `defimpl` runs is dispatch. A `defimpl P, for: T` is module P.T, so
 * its own functions call each other like any module's.
 *
 * `&f/1` and `&Mod.f/1` capture by the same selection, without calling. A module's
 * name wherever it is read is a mention of that module, if the project declares it
 * once. A bare identifier is a variable, and the graph has no node for one; only `x
 * |> f` makes one a call, of f/1.
 *
 * The graph's Elixir QN holds neither module nor arity, so all clauses and
 * arities of one name in a module are one node. Two modules of a file sharing a
 * name share a node too, and that identity cannot name either: such a target
 * abstains in cbm_source_unique_qn rather than picking one. */
#include "source_adapter_internal.h"
#include <stdlib.h>
#include "syntax_index.h"

typedef struct {
    const char *local;
    const char *module;
} elixir_alias_t;

typedef struct {
    const char *module;
    TSNode only, except;
    /* The same filters when the import stands in another file's `__using__`. */
    const char *only_text, *except_text;
    /* Not an import: a function `use` put into this module. */
    const char *injected;
    int injected_min, injected_max;
} elixir_import_t;

/* A statement in a module's `__using__`, kept on that module under this name. */
#define ELIXIR_USING "&using"
enum { ELIXIR_USING_KIND, ELIXIR_USING_ONLY, ELIXIR_USING_EXCEPT, ELIXIR_USING_PARTS };

typedef struct {
    cbm_source_walk_t *walk;
    const char *module; /* NULL at file level */
    const char *scope;  /* the enclosing definition's QN, or NULL */
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

/* `defimpl P, for: T` is module P.T; with no `for:` T is the enclosing module. A list of
 * types is several modules, which one body cannot be. */
static const char *elixir_module_of_impl(elixir_ctx_t *ctx, TSNode call) {
    TSNode arguments = elixir_arguments(call);
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    TSNode protocol = count ? ts_node_named_child(arguments, 0) : (TSNode){0};
    if (!cbm_source_is(protocol, "alias"))
        return NULL;
    const char *name = cbm_source_text(ctx->walk, protocol);
    const char *type = ctx->module;
    for (uint32_t i = 1; i < count; i++) {
        TSNode keywords = ts_node_named_child(arguments, i);
        uint32_t pairs =
            cbm_source_is(keywords, "keywords") ? ts_node_named_child_count(keywords) : 0;
        for (uint32_t p = 0; p < pairs; p++) {
            TSNode pair = ts_node_named_child(keywords, p);
            const char *key = cbm_source_text(ctx->walk, cbm_source_field(pair, "key"));
            if (!key || strncmp(key, "for:", 4))
                continue;
            TSNode value = cbm_source_field(pair, "value");
            type = cbm_source_is(value, "alias") ? cbm_source_text(ctx->walk, value) : NULL;
        }
    }
    return name && type ? cbm_arena_sprintf(ctx->walk->arena, "%s.%s", name, type) : NULL;
}

/* A definition's name and the arities its head accepts. */
static const char *elixir_signature(elixir_ctx_t *ctx, TSNode call, int *fewest, int *most) {
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
    *fewest = arity_min;
    *most = arity_max;
    return name;
}

static void elixir_declare_function(elixir_ctx_t *ctx, TSNode call, const char *definer) {
    int arity_min = 0, arity_max = 0;
    const char *name = elixir_signature(ctx, call, &arity_min, &arity_max);
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

/* `only: [a: 1, b: 2]` as ",a/1,b/2,"; "?" when the list is not written out. */
static const char *elixir_filter_text(elixir_ctx_t *ctx, TSNode filter) {
    CBMArena *arena = ctx->walk->arena;
    if (!cbm_source_is(filter, "list"))
        return "?";
    const char *text = ",";
    uint32_t count = ts_node_named_child_count(filter);
    for (uint32_t i = 0; text && i < count; i++) {
        TSNode keywords = ts_node_named_child(filter, i);
        if (!cbm_source_is(keywords, "keywords"))
            return "?";
        uint32_t pairs = ts_node_named_child_count(keywords);
        for (uint32_t p = 0; text && p < pairs; p++) {
            TSNode pair = ts_node_named_child(keywords, p);
            const char *key = cbm_source_text(ctx->walk, cbm_source_field(pair, "key"));
            const char *value = cbm_source_text(ctx->walk, cbm_source_field(pair, "value"));
            const char *colon = key ? strchr(key, ':') : NULL;
            if (!colon || !value)
                return "?";
            text =
                cbm_arena_sprintf(arena, "%s%.*s/%d,", text, (int)(colon - key), key, atoi(value));
        }
    }
    return text ? text : "?";
}

static cbm_source_decl_t *elixir_declare_using(elixir_ctx_t *ctx, TSNode statement,
                                               const char *value, const char *kind) {
    cbm_source_decl_t *decl =
        cbm_source_declare(ctx->walk, ctx->module, ELIXIR_USING, NULL, CBM_SOURCE_DECL_F_TYPE,
                           ts_node_start_point(statement).row + 1);
    const char **parts =
        decl ? cbm_source_table_alloc(ctx->walk, ELIXIR_USING_PARTS * sizeof(*parts)) : NULL;
    if (!parts)
        return NULL;
    decl->type = cbm_source_table_intern(ctx->walk, value);
    parts[ELIXIR_USING_KIND] = kind;
    decl->bases = parts;
    return decl;
}

static void elixir_collect_using_statement(elixir_ctx_t *ctx, TSNode statement) {
    const char *name =
        cbm_source_is(statement, "call") ? elixir_target_name(ctx->walk, statement) : NULL;
    if (!name)
        return;
    if (elixir_definer(name)) {
        int arity_min = 0, arity_max = 0;
        const char *defined = elixir_signature(ctx, statement, &arity_min, &arity_max);
        cbm_source_decl_t *decl =
            defined ? elixir_declare_using(ctx, statement, defined, "def") : NULL;
        if (decl) {
            decl->arity_min = arity_min;
            decl->arity_max = arity_max;
        }
        return;
    }
    bool import = !strcmp(name, "import");
    if (!import && strcmp(name, "use"))
        return;
    TSNode arguments = elixir_arguments(statement);
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    TSNode first = count ? ts_node_named_child(arguments, 0) : (TSNode){0};
    const char *written = cbm_source_text(ctx->walk, first);
    /* Inside the quote `unquote(__MODULE__)` is the module this macro is in. */
    const char *module = !written                                  ? NULL
                         : cbm_source_is(first, "alias")           ? written
                         : !strcmp(written, "unquote(__MODULE__)") ? ctx->module
                                                                   : NULL;
    cbm_source_decl_t *decl =
        module ? elixir_declare_using(ctx, statement, module, import ? "import" : "use") : NULL;
    if (!decl || !import)
        return;
    const char **parts = (const char **)decl->bases;
    for (uint32_t i = 1; i < count; i++) {
        TSNode keywords = ts_node_named_child(arguments, i);
        uint32_t pairs =
            cbm_source_is(keywords, "keywords") ? ts_node_named_child_count(keywords) : 0;
        for (uint32_t p = 0; p < pairs; p++) {
            TSNode pair = ts_node_named_child(keywords, p);
            const char *key = cbm_source_text(ctx->walk, cbm_source_field(pair, "key"));
            TSNode value = cbm_source_field(pair, "value");
            if (key && !strncmp(key, "only:", 5))
                parts[ELIXIR_USING_ONLY] =
                    cbm_source_table_intern(ctx->walk, elixir_filter_text(ctx, value));
            else if (key && !strncmp(key, "except:", 7))
                parts[ELIXIR_USING_EXCEPT] =
                    cbm_source_table_intern(ctx->walk, elixir_filter_text(ctx, value));
        }
    }
}

/* What `defmacro __using__` returns, when its last statement says so. */
static void elixir_collect_using(elixir_ctx_t *ctx, TSNode definition) {
    int arity_min = 0, arity_max = 0;
    const char *name = elixir_signature(ctx, definition, &arity_min, &arity_max);
    TSNode body = elixir_do_block(definition);
    if (!name || strcmp(name, "__using__") || !ctx->module || ts_node_is_null(body))
        return;
    uint32_t statements = ts_node_named_child_count(body);
    TSNode last = statements ? ts_node_named_child(body, statements - 1) : (TSNode){0};
    const char *returned = cbm_source_is(last, "call") ? elixir_target_name(ctx->walk, last) : NULL;
    if (!returned || strcmp(returned, "quote"))
        return;
    TSNode quoted = elixir_do_block(last);
    if (!ts_node_is_null(quoted)) {
        TSTreeCursor cursor = ts_tree_cursor_new(quoted);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                elixir_collect_using_statement(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        return;
    }
    /* `quote do: import(X)` */
    TSNode arguments = elixir_arguments(last);
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < count; i++) {
        TSNode keywords = ts_node_named_child(arguments, i);
        uint32_t pairs =
            cbm_source_is(keywords, "keywords") ? ts_node_named_child_count(keywords) : 0;
        for (uint32_t p = 0; p < pairs; p++) {
            TSNode pair = ts_node_named_child(keywords, p);
            const char *key = cbm_source_text(ctx->walk, cbm_source_field(pair, "key"));
            if (key && !strncmp(key, "do:", 3))
                elixir_collect_using_statement(ctx, cbm_source_field(pair, "value"));
        }
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
            if (name && (!strcmp(name, "defmodule") || !strcmp(name, "defprotocol"))) {
                const char *module = elixir_module_of_declaration(ctx, child);
                if (!module)
                    continue;
                uint32_t line = ts_node_start_point(child).row + 1;
                TSNode spelled = ts_node_named_child(elixir_arguments(child), 0);
                (void)cbm_source_declare(
                    ctx->walk, module, "",
                    cbm_source_def_qn_label(ctx->walk, cbm_source_text(ctx->walk, spelled), line,
                                            "Class"),
                    CBM_SOURCE_DECL_F_TYPE, line);
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
                if (!strcmp(name, "defmacro"))
                    elixir_collect_using(ctx, child);
                continue;
            }
            if (name && !strcmp(name, "defimpl")) {
                const char *module = elixir_module_of_impl(ctx, child);
                TSNode body = elixir_do_block(child);
                if (!module || ts_node_is_null(body))
                    continue;
                const char *saved = ctx->module;
                ctx->module = module;
                elixir_collect_node(ctx, body, depth + 1);
                ctx->module = saved;
                continue;
            }
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

static void elixir_push_import(elixir_ctx_t *ctx, elixir_import_t import) {
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
    elixir_push_import(ctx, import);
}

/* `use M`: what M's `__using__` is known to return. A `use` in there follows on, and each
 * module is read once, so this costs the modules reachable from M. */
static void elixir_apply_using(elixir_ctx_t *ctx, const char *module, cbm_source_chain_t *chain) {
    if (!cbm_source_chain_first_visit(chain, module))
        return;
    for (const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, module, ELIXIR_USING); decl;
         decl = decl->next) {
        const char *kind = decl->bases[ELIXIR_USING_KIND];
        if (!strcmp(kind, "use")) {
            elixir_apply_using(ctx, decl->type, chain);
        } else if (!strcmp(kind, "def")) {
            elixir_push_import(ctx, (elixir_import_t){.injected = decl->type,
                                                      .injected_min = decl->arity_min,
                                                      .injected_max = decl->arity_max});
        } else {
            elixir_push_import(ctx,
                               (elixir_import_t){.module = decl->type,
                                                 .only_text = decl->bases[ELIXIR_USING_ONLY],
                                                 .except_text = decl->bases[ELIXIR_USING_EXCEPT]});
        }
    }
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

static elixir_list_t elixir_text_filter_has(elixir_ctx_t *ctx, const char *filter, const char *name,
                                            int arity) {
    if (!strcmp(filter, "?"))
        return ELIXIR_LIST_UNKNOWN;
    const char *entry = cbm_arena_sprintf(ctx->walk->arena, ",%s/%d,", name, arity);
    return entry && strstr(filter, entry) ? ELIXIR_LIST_YES : ELIXIR_LIST_NO;
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
        if (import->injected) {
            if (!strcmp(import->injected, name) && arity >= import->injected_min &&
                arity <= import->injected_max) {
                elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                            "elixir_function_put_here_by_use");
                return;
            }
            continue;
        }
        if (import->only_text || import->except_text) {
            elixir_list_t only = import->only_text
                                     ? elixir_text_filter_has(ctx, import->only_text, name, arity)
                                     : ELIXIR_LIST_YES;
            elixir_list_t except =
                import->except_text ? elixir_text_filter_has(ctx, import->except_text, name, arity)
                                    : ELIXIR_LIST_NO;
            if (only == ELIXIR_LIST_UNKNOWN || except == ELIXIR_LIST_UNKNOWN) {
                elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                            "elixir_import_filter_not_literal");
                return;
            }
            if (only == ELIXIR_LIST_NO || except == ELIXIR_LIST_YES)
                continue;
        }
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

/* `:name` read as a module: an Erlang module of that name. */
static const char *elixir_erlang_module(elixir_ctx_t *ctx, TSNode node) {
    if (!cbm_source_is(node, "atom"))
        return NULL;
    const char *text = cbm_source_text(ctx->walk, node);
    return text && text[0] == ':' && text[1] != '"' ? text + 1 : NULL;
}

/* An Erlang module's exported function of this name and arity. */
static const char *elixir_erlang_function(elixir_ctx_t *ctx, const char *module, const char *name,
                                          int arity) {
    const char *selected = NULL;
    for (const cbm_source_decl_t *decl =
             cbm_source_lookup_in(ctx->walk, CBM_LANG_ERLANG, module, name);
         decl; decl = decl->next) {
        if (decl->arity_min != arity || !(decl->flags & CBM_SOURCE_DECL_F_PUBLIC))
            continue;
        if (!decl->qn || (selected && strcmp(selected, decl->qn)))
            return NULL;
        selected = decl->qn;
    }
    return selected;
}

static void elixir_visit_remote_call(elixir_ctx_t *ctx, TSNode call, TSNode dot, int arity) {
    TSNode left = cbm_source_field(dot, "left");
    TSNode right = cbm_source_field(dot, "right");
    if (!cbm_source_is(right, "identifier")) {
        elixir_emit(ctx, call, NULL, CBM_RESOLVE_RULE_NONE, NULL, "elixir_anonymous_function_call");
        return;
    }
    const char *erlang = elixir_erlang_module(ctx, left);
    if (erlang) {
        const char *target =
            elixir_erlang_function(ctx, erlang, cbm_source_text(ctx->walk, right), arity);
        elixir_emit(ctx, call, target, CBM_RESOLVE_RULE_LANGUAGE_BINDING, erlang,
                    target ? "elixir_erlang_module_function" : "elixir_erlang_function_absent");
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

static void elixir_extra_begin(elixir_ctx_t *ctx, TSNode node, const char *name) {
    cbm_source_walk_t *walk = ctx->walk;
    walk->extra_scope = ctx->scope ? ctx->scope : walk->result->module_qn;
    walk->extra_name = name;
    walk->extra_line = ts_node_start_point(node).row + 1;
}

static void elixir_extra_end(elixir_ctx_t *ctx) {
    ctx->walk->extra_scope = NULL;
    ctx->walk->extra_name = NULL;
    ctx->walk->extra_line = 0;
    ctx->walk->extra_composite = false;
}

/* `if f, do: &a/1, else: &b/1` and `f || &a/1` hand over one of several values, so a capture there
 * only mentions its function. Anything else above it makes it one value again: a call's argument,
 * a binding, a list, a closure's body. */
static bool elixir_capture_in_composite(elixir_ctx_t *ctx, TSNode capture) {
    static const char *const branching[] = {"if",   "unless", "case",    "cond",
                                            "with", "try",    "receive", NULL};
    static const char *const within[] = {
        "arguments",    "keywords",    "pair",        "block",       "do_block", "else_block",
        "rescue_block", "catch_block", "after_block", "stab_clause", "body",     NULL};
    for (TSNode parent = ts_node_parent(capture); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (cbm_source_is(parent, "binary_operator"))
            return cbm_source_token(parent, "&&") || cbm_source_token(parent, "||") ||
                   cbm_source_token(parent, "and") || cbm_source_token(parent, "or");
        if (cbm_source_is(parent, "call")) {
            const char *target = cbm_source_text(ctx->walk, cbm_source_field(parent, "target"));
            for (size_t i = 0; target && branching[i]; i++)
                if (!strcmp(target, branching[i]))
                    return true;
            return false;
        }
        bool passes = false;
        for (size_t i = 0; !passes && within[i]; i++)
            passes = cbm_source_is(parent, within[i]);
        if (!passes)
            return false;
    }
    return false;
}

/* A module's name, read anywhere other than its own declaration. */
static void elixir_visit_module_mention(elixir_ctx_t *ctx, TSNode node) {
    const char *text = cbm_source_text(ctx->walk, node);
    const char *module = text ? elixir_expand_alias(ctx, text) : NULL;
    const char *reason = NULL;
    const char *qn =
        module ? cbm_source_unique_qn(cbm_source_lookup(ctx->walk, module, ""), &reason) : NULL;
    if (!qn)
        return;
    elixir_extra_begin(ctx, node, text);
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, qn,
                         CBM_RESOLVE_RULE_LANGUAGE_BINDING, module, "elixir_module_name");
    elixir_extra_end(ctx);
}

/* `&f/1` or `&Mod.f/1`. The grammar spells f as an identifier or a call without arguments. */
static void elixir_visit_capture(elixir_ctx_t *ctx, TSNode site, TSNode left, int arity) {
    const char *target = NULL;
    const char *domain = NULL;
    const char *reason = NULL;
    const char *name = NULL;
    TSNode callee = cbm_source_is(left, "call") ? cbm_source_field(left, "target") : left;
    if (cbm_source_is(callee, "identifier") && ctx->module) {
        name = cbm_source_text(ctx->walk, callee);
        domain = ctx->module;
        target = name ? elixir_select(ctx, ctx->module, name, arity, false, &reason) : NULL;
    } else if (cbm_source_is(callee, "dot") &&
               cbm_source_is(cbm_source_field(callee, "right"), "identifier")) {
        domain = elixir_module_operand(ctx, cbm_source_field(callee, "left"));
        name = cbm_source_text(ctx->walk, cbm_source_field(callee, "right"));
        target = domain && name
                     ? elixir_select(ctx, domain, name, arity,
                                     !ctx->module || strcmp(ctx->module, domain), &reason)
                     : NULL;
    }
    if (!target)
        return;
    elixir_extra_begin(ctx, site, name);
    ctx->walk->extra_composite = elixir_capture_in_composite(ctx, site);
    cbm_source_emit_node(ctx->walk, site, CBM_SOURCE_SITE_REFERENCE, target,
                         CBM_RESOLVE_RULE_LANGUAGE_BINDING, domain, "elixir_function_capture");
    elixir_extra_end(ctx);
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
        if (!strcmp(name, "quote"))
            return;
        bool protocol = !strcmp(name, "defprotocol");
        bool implementation = !strcmp(name, "defimpl");
        if (!strcmp(name, "defmodule") || protocol || implementation) {
            const char *module = implementation ? elixir_module_of_impl(ctx, node)
                                                : elixir_module_of_declaration(ctx, node);
            if (!module)
                return;
            /* A nested defmodule leaves an alias of its last segment in the parent. */
            if (ctx->module && !implementation)
                elixir_add_alias(ctx, cbm_source_text(ctx->walk, ts_node_named_child(arguments, 0)),
                                 module);
            /* Imports are lexical, so a nested module sees those around it, and what it adds
             * ends with it. */
            const char *saved = ctx->module;
            size_t imports = ctx->import_count;
            ctx->module = module;
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
            const char *saved_scope = ctx->scope;
            TSNode head = elixir_head(node);
            TSNode head_name =
                cbm_source_is(head, "call") ? cbm_source_field(head, "target") : head;
            ctx->scope =
                cbm_source_is(head_name, "identifier")
                    ? cbm_source_def_qn_label(ctx->walk, cbm_source_text(ctx->walk, head_name),
                                              ts_node_start_point(node).row + 1, "Function")
                    : NULL;
            for (uint32_t i = 1; i < count; i++)
                elixir_visit_scoped(ctx, ts_node_named_child(head_arguments, i), depth + 1);
            if (!ts_node_is_null(body))
                elixir_visit_scoped(ctx, body, depth + 1);
            ctx->scope = saved_scope;
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
        if (!strcmp(name, "use")) {
            const char *used = !ts_node_is_null(arguments) && ts_node_named_child_count(arguments)
                                   ? elixir_module_operand(ctx, ts_node_named_child(arguments, 0))
                                   : NULL;
            cbm_source_chain_t chain = {0};
            if (used)
                elixir_apply_using(ctx, used, &chain);
            return;
        }
        elixir_visit_bare_call(ctx, node, name, arity);
    } else if (cbm_source_is(cbm_source_field(node, "target"), "dot")) {
        elixir_visit_remote_call(ctx, node, cbm_source_field(node, "target"), arity);
        /* The receiver is read first: `f(x).y` calls f, and `Mod.g(x)` mentions Mod. */
        TSNode receiver = cbm_source_field(cbm_source_field(node, "target"), "left");
        if (!ts_node_is_null(receiver))
            elixir_visit(ctx, receiver, false, depth + 1);
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
    if (cbm_source_is(node, "alias")) {
        elixir_visit_module_mention(ctx, node);
        return;
    }
    if (cbm_source_is(node, "identifier")) {
        const char *name = cbm_source_text(ctx->walk, node);
        if (piped && name) {
            elixir_extra_begin(ctx, node, name);
            elixir_visit_bare_call(ctx, node, name, 1);
            elixir_extra_end(ctx);
            cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, NULL,
                                 CBM_RESOLVE_RULE_NONE, NULL, "elixir_piped_name_is_a_call");
        } else {
            elixir_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                        "elixir_bare_name_is_a_variable");
            cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, NULL,
                                 CBM_RESOLVE_RULE_NONE, NULL, "elixir_variable_has_no_node");
        }
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
            TSNode arity = cbm_source_field(operand, "right");
            const char *digits =
                cbm_source_is(arity, "integer") ? cbm_source_text(ctx->walk, arity) : NULL;
            if (digits)
                elixir_visit_capture(ctx, node, left, atoi(digits));
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
                                                        .one_name_one_node = true,
                                                        .decides_references = true,
                                                        .contains_parse_damage = true};
