/* Haskell binding rules, from the Haskell 2010 report's module chapter.
 *
 * A module is named by its header. An unqualified `f` is first any enclosing
 * local binding (a parameter, where, let, do bind, lambda or case pattern), then
 * a top-level declaration of the module, then what an unqualified import brings
 * in. An import list names exactly what it brings in and `hiding` names what it
 * leaves out. `M.f` searches the imports named or aliased M. The report makes two
 * imports providing one name an error at its use, so a second candidate means no
 * single declaration can be stated here, not that one should be picked.
 *
 * A module's export list decides what other modules can see. Without one, all its
 * top-level declarations are visible. An entry re-exporting another module is not
 * followed, so a name reached only through a re-export abstains.
 *
 * All equations of a function and its signature are one name, and the graph has
 * one node per name in a file.
 *
 * Constructors have a namespace of their own, looked up the same way. The graph has a
 * node for a type, none for a constructor, so `data T = T a` selects T and a
 * constructor named otherwise is known but selects nothing. An export or import entry
 * reaches a type's constructors only when it lists children, as in `T(..)`; a
 * pattern matches a constructor without calling it.
 *
 * A class's methods are top-level names of its module. `status x` selects the method's
 * declaration in the class; which instance runs is dispatch. A signature alone has no node;
 * the file's default or instance bodies of that name are its node, and with none the method
 * is known but selects nothing. `C(..)` and `C(m)` reach methods the same way an entry
 * reaches constructors. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>

#define HS_EXPORT_KEY_PREFIX "&export:"
#define HS_EXPORTS_ALL "&exports_all"
#define HS_EXPORT_CHILDREN_PREFIX "&export_children:"

typedef struct {
    const char *module;
    const char *alias; /* `as` name, or NULL */
    bool qualified;
    bool has_list, hiding;
    TSNode names;
} hs_import_t;

typedef struct hs_local {
    const char *name;
    struct hs_local *next;
} hs_local_t;

typedef struct {
    cbm_source_walk_t *walk;
    bool reference;   /* the name being read is a value, not a call head */
    bool constructor; /* the name being read is a constructor */
    const char *module;
    hs_import_t *imports;
    size_t import_count;
    hs_local_t *locals;
} hs_ctx_t;

static TSNode hs_child(TSNode node, const char *kind) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), kind))
            return ts_node_named_child(node, i);
    return (TSNode){0};
}

static const char *hs_header_module(cbm_source_walk_t *walk) {
    TSNode header = hs_child(walk->root, "header");
    const char *module = cbm_source_text(walk, cbm_source_field(header, "module"));
    /* A file with no header is module Main. */
    return module ? module : "Main";
}

static void hs_collect_exports(cbm_source_walk_t *walk, const char *module) {
    TSNode header = hs_child(walk->root, "header");
    TSNode exports = cbm_source_field(header, "exports");
    if (ts_node_is_null(exports)) {
        (void)cbm_source_declare(walk, module, HS_EXPORTS_ALL, NULL, CBM_SOURCE_DECL_F_VALUE, 1);
        return;
    }
    uint32_t count = ts_node_named_child_count(exports);
    for (uint32_t i = 0; i < count; i++) {
        TSNode entry = ts_node_named_child(exports, i);
        const char *name = cbm_source_text(walk, cbm_source_field(entry, "variable"));
        /* `T(..)` exports T's constructors; a bare `T` keeps them private. */
        TSNode children = cbm_source_field(entry, "children");
        if (!name && !ts_node_is_null(children))
            name = cbm_source_text(walk, hs_child(entry, "name"));
        const char *key =
            name ? cbm_arena_sprintf(walk->arena, "%s%s", HS_EXPORT_KEY_PREFIX, name) : NULL;
        if (key)
            (void)cbm_source_declare(walk, module, key, NULL, CBM_SOURCE_DECL_F_VALUE, 1);
        /* `C(..)` exports every method of C, and `C(m)` exports m. */
        uint32_t listed = ts_node_is_null(children) ? 0 : ts_node_named_child_count(children);
        for (uint32_t c = 0; name && c < listed; c++) {
            TSNode child = ts_node_named_child(children, c);
            const char *child_key =
                cbm_source_is(child, "all_names")
                    ? cbm_arena_sprintf(walk->arena, "%s%s", HS_EXPORT_CHILDREN_PREFIX, name)
                    : cbm_arena_sprintf(walk->arena, "%s%s", HS_EXPORT_KEY_PREFIX,
                                        cbm_source_text(walk, child));
            if (child_key)
                (void)cbm_source_declare(walk, module, child_key, NULL, CBM_SOURCE_DECL_F_VALUE, 1);
        }
    }
}

static void hs_collect_method(cbm_source_walk_t *walk, const char *module, const char *class_name,
                              TSNode name) {
    const char *text = cbm_source_text(walk, name);
    uint32_t line = ts_node_start_point(name).row + 1;
    cbm_source_decl_t *decl =
        text ? cbm_source_declare(walk, module, text, cbm_source_def_qn_of_name(walk, text),
                                  CBM_SOURCE_DECL_F_PUBLIC, line)
             : NULL;
    if (decl)
        decl->type = cbm_source_table_intern(walk, class_name);
}

/* What a class declares a signature for; a default body repeats a name, adding nothing. */
static void hs_collect_class(cbm_source_walk_t *walk, const char *module, TSNode declaration) {
    const char *class_name = cbm_source_text(walk, hs_child(declaration, "name"));
    TSNode body = hs_child(declaration, "class_declarations");
    if (!class_name || ts_node_is_null(body))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode signature = ts_tree_cursor_current_node(&cursor);
            if (!cbm_source_is(signature, "signature"))
                continue;
            TSNode head = ts_node_named_child(signature, 0);
            if (cbm_source_is(head, "variable"))
                hs_collect_method(walk, module, class_name, head);
            uint32_t names =
                cbm_source_is(head, "binding_list") ? ts_node_named_child_count(head) : 0;
            for (uint32_t n = 0; n < names; n++)
                hs_collect_method(walk, module, class_name, ts_node_named_child(head, n));
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* Every constructor is declared, so that one with no node still hides an imported
 * name. Only the constructor sharing its type's name has a node to select. */
static void hs_collect_constructor(cbm_source_walk_t *walk, const char *module, TSNode constructor,
                                   const char *type_name, const char *type_qn, uint32_t line) {
    const char *text = cbm_source_text(walk, constructor);
    if (text)
        (void)cbm_source_declare(walk, module, text,
                                 type_name && !strcmp(text, type_name) ? type_qn : NULL,
                                 CBM_SOURCE_DECL_F_TYPE | CBM_SOURCE_DECL_F_PUBLIC, line);
}

static void hs_collect_constructors(cbm_source_walk_t *walk, const char *module, TSNode node,
                                    const char *type_name, const char *type_qn, uint32_t line,
                                    unsigned depth) {
    if (!cbm_source_step(walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (cbm_source_is(child, "constructor"))
                hs_collect_constructor(walk, module, child, type_name, type_qn, line);
            else if (cbm_source_is(child, "data_constructors") ||
                     cbm_source_is(child, "data_constructor") ||
                     cbm_source_is(child, "newtype_constructor") ||
                     cbm_source_is(child, "prefix") || cbm_source_is(child, "record"))
                hs_collect_constructors(walk, module, child, type_name, type_qn, line, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void hs_collect(cbm_source_walk_t *walk) {
    const char *module = hs_header_module(walk);
    (void)cbm_source_declare(walk, module, "", NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    hs_collect_exports(walk, module);
    TSNode declarations = hs_child(walk->root, "declarations");
    uint32_t count = ts_node_is_null(declarations) ? 0 : ts_node_named_child_count(declarations);
    for (uint32_t i = 0; i < count; i++) {
        TSNode declaration = ts_node_named_child(declarations, i);
        if (cbm_source_is(declaration, "data_type") || cbm_source_is(declaration, "newtype")) {
            const char *type_name = cbm_source_text(walk, cbm_source_field(declaration, "name"));
            uint32_t type_line = ts_node_start_point(declaration).row + 1;
            hs_collect_constructors(walk, module, declaration, type_name,
                                    cbm_source_def_qn_label(walk, type_name, type_line, "Class"),
                                    type_line, 1);
            continue;
        }
        if (cbm_source_is(declaration, "class")) {
            hs_collect_class(walk, module, declaration);
            continue;
        }
        if (!cbm_source_is(declaration, "function") && !cbm_source_is(declaration, "bind") &&
            !cbm_source_is(declaration, "signature"))
            continue;
        TSNode name = cbm_source_field(declaration, "name");
        if (!cbm_source_is(name, "variable"))
            continue; /* A pattern binding names several things at once. */
        const char *text = cbm_source_text(walk, name);
        uint32_t line = ts_node_start_point(declaration).row + 1;
        const char *qn = cbm_source_def_qn(walk, text, line);
        if (text && qn)
            (void)cbm_source_declare(walk, module, text, qn, CBM_SOURCE_DECL_F_PUBLIC, line);
    }
}

/* ── Phase two ─────────────────────────────────────────────────────── */

static void hs_gather_imports(hs_ctx_t *ctx) {
    TSNode imports = hs_child(ctx->walk->root, "imports");
    uint32_t count = ts_node_is_null(imports) ? 0 : ts_node_named_child_count(imports);
    if (!count)
        return;
    ctx->imports = cbm_arena_calloc(ctx->walk->arena, count * sizeof(*ctx->imports));
    if (!ctx->imports) {
        cbm_source_fail(ctx->walk, "haskell_import_allocation");
        return;
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode import = ts_node_named_child(imports, i);
        if (!cbm_source_is(import, "import"))
            continue;
        TSNode names = cbm_source_field(import, "names");
        hs_import_t row = {
            .module = cbm_source_text(ctx->walk, cbm_source_field(import, "module")),
            .alias = cbm_source_text(ctx->walk, cbm_source_field(import, "alias")),
            .qualified = cbm_source_token(import, "qualified"),
            .has_list = !ts_node_is_null(names),
            .hiding = cbm_source_token(import, "hiding"),
            .names = names,
        };
        if (row.module)
            ctx->imports[ctx->import_count++] = row;
    }
}

/* An import entry `C(..)` or `C(m)` naming method m of class C. */
static bool hs_entry_lists_method(hs_ctx_t *ctx, TSNode entry, const char *class_name,
                                  const char *name) {
    TSNode children = cbm_source_field(entry, "children");
    const char *listed = cbm_source_text(ctx->walk, hs_child(entry, "name"));
    if (ts_node_is_null(children) || !class_name || !listed || strcmp(listed, class_name))
        return false;
    uint32_t count = ts_node_named_child_count(children);
    for (uint32_t c = 0; c < count; c++) {
        TSNode child = ts_node_named_child(children, c);
        const char *text = cbm_source_text(ctx->walk, child);
        if (cbm_source_is(child, "all_names") || (text && !strcmp(text, name)))
            return true;
    }
    return false;
}

static bool hs_list_names(hs_ctx_t *ctx, TSNode names, const char *module, const char *name) {
    uint32_t count = ts_node_is_null(names) ? 0 : ts_node_named_child_count(names);
    const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, module, name);
    for (uint32_t i = 0; i < count; i++) {
        TSNode entry = ts_node_named_child(names, i);
        const char *text = cbm_source_text(ctx->walk, cbm_source_field(entry, "variable"));
        if (!text && !ts_node_is_null(cbm_source_field(entry, "children")))
            text = cbm_source_text(ctx->walk, hs_child(entry, "name"));
        if ((text && !strcmp(text, name)) ||
            (decl && hs_entry_lists_method(ctx, entry, decl->type, name)))
            return true;
    }
    return false;
}

/* The module's own declaration of name, when other modules can see it. */
static const char *hs_visible_member(hs_ctx_t *ctx, const char *module, const char *name,
                                     const char **reason) {
    const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, module, name);
    const char *qn = cbm_source_unique_qn(decl, reason);
    if (!qn)
        return NULL;
    char key[256];
    int length = snprintf(key, sizeof(key), "%s%s", HS_EXPORT_KEY_PREFIX, name);
    if (length < 0 || (size_t)length >= sizeof(key))
        return NULL;
    if (cbm_source_lookup(ctx->walk, module, HS_EXPORTS_ALL) ||
        cbm_source_lookup(ctx->walk, module, key))
        return qn;
    if (decl->type) {
        length = snprintf(key, sizeof(key), "%s%s", HS_EXPORT_CHILDREN_PREFIX, decl->type);
        if (length > 0 && (size_t)length < sizeof(key) && cbm_source_lookup(ctx->walk, module, key))
            return qn;
    }
    *reason = "haskell_name_not_in_export_list";
    return NULL;
}

static bool hs_is_local(const hs_ctx_t *ctx, const char *name) {
    for (const hs_local_t *local = ctx->locals; local; local = local->next)
        if (!strcmp(local->name, name))
            return true;
    return false;
}

static void hs_bind(hs_ctx_t *ctx, const char *name) {
    hs_local_t *local = name ? cbm_arena_alloc(ctx->walk->arena, sizeof(*local)) : NULL;
    if (!local)
        return;
    *local = (hs_local_t){name, ctx->locals};
    ctx->locals = local;
}

/* Every variable below a pattern binds it. */
static void hs_bind_pattern(hs_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "variable"))
        hs_bind(ctx, cbm_source_text(ctx->walk, node));
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        hs_bind_pattern(ctx, ts_node_named_child(node, i), depth + 1);
}

static void hs_bind_decls(hs_ctx_t *ctx, TSNode binds) {
    uint32_t count = ts_node_named_child_count(binds);
    for (uint32_t i = 0; i < count; i++) {
        TSNode declaration = ts_node_named_child(binds, i);
        TSNode name = cbm_source_field(declaration, "name");
        if (!ts_node_is_null(name))
            hs_bind(ctx, cbm_source_text(ctx->walk, name));
        TSNode pattern = cbm_source_field(declaration, "pattern");
        if (!ts_node_is_null(pattern))
            hs_bind_pattern(ctx, pattern, 0);
    }
}

static void hs_emit(hs_ctx_t *ctx, TSNode site, const char *target, cbm_resolve_rule_t rule,
                    const char *domain, const char *reason) {
    cbm_source_emit_facts(ctx->walk, ts_node_start_byte(site), ts_node_end_byte(site),
                          ctx->reference ? CBM_SOURCE_SITE_REFERENCE : CBM_SOURCE_SITE_CALL, target,
                          rule, domain, reason,
                          ctx->constructor ? CBM_RESOLVE_FACT_CLASS_CALLABLE : 0);
}

static void hs_resolve_bare(hs_ctx_t *ctx, TSNode site, const char *name) {
    if (hs_is_local(ctx, name)) {
        hs_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "haskell_locally_bound_name");
        return;
    }
    const char *reason = NULL;
    const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, ctx->module, name);
    if (own) {
        const char *qn = cbm_source_unique_qn(own, &reason);
        hs_emit(ctx, site, qn, CBM_RESOLVE_RULE_SCOPE_BINDING, ctx->module,
                qn ? "haskell_top_level_declaration" : reason);
        return;
    }
    const char *selected = NULL, *from = NULL;
    for (size_t i = 0; i < ctx->import_count; i++) {
        const hs_import_t *import = &ctx->imports[i];
        if (import->qualified)
            continue;
        if (import->has_list && !import->hiding &&
            !hs_list_names(ctx, import->names, import->module, name))
            continue;
        if (import->has_list && import->hiding &&
            hs_list_names(ctx, import->names, import->module, name))
            continue;
        const char *why = NULL;
        const char *qn = hs_visible_member(ctx, import->module, name, &why);
        if (!qn)
            continue;
        if (selected && strcmp(selected, qn)) {
            hs_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                    "haskell_two_imports_provide_name");
            return;
        }
        selected = qn;
        from = import->module;
    }
    hs_emit(ctx, site, selected, CBM_RESOLVE_RULE_IMPORT_BINDING, from,
            selected ? "haskell_import_binding" : "haskell_name_not_in_project_imports");
}

static void hs_resolve_qualified(hs_ctx_t *ctx, TSNode site, TSNode qualified) {
    const char *qualifier = cbm_source_text(ctx->walk, cbm_source_field(qualified, "module"));
    /* The module node's span ends in the dot before the name. */
    if (qualifier) {
        size_t length = strlen(qualifier);
        if (length && qualifier[length - 1] == '.')
            qualifier = cbm_arena_strndup(ctx->walk->arena, qualifier, length - 1);
    }
    const char *name = cbm_source_text(ctx->walk, cbm_source_field(qualified, "id"));
    if (!qualifier || !name) {
        hs_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "haskell_qualified_name_unread");
        return;
    }
    const char *selected = NULL, *from = NULL;
    for (size_t i = 0; i < ctx->import_count; i++) {
        const hs_import_t *import = &ctx->imports[i];
        const char *visible_as = import->alias ? import->alias : import->module;
        if (strcmp(visible_as, qualifier))
            continue;
        if (import->has_list && !import->hiding &&
            !hs_list_names(ctx, import->names, import->module, name))
            continue;
        if (import->has_list && import->hiding &&
            hs_list_names(ctx, import->names, import->module, name))
            continue;
        const char *why = NULL;
        const char *qn = hs_visible_member(ctx, import->module, name, &why);
        if (!qn)
            continue;
        if (selected && strcmp(selected, qn)) {
            hs_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                    "haskell_two_imports_provide_qualified_name");
            return;
        }
        selected = qn;
        from = import->module;
    }
    hs_emit(ctx, site, selected, CBM_RESOLVE_RULE_IMPORT_BINDING, from,
            selected ? "haskell_qualified_import_binding" : "haskell_qualifier_not_in_project");
}

/* `f a b` is apply(apply(f, a), b): one call, whose head is the innermost function. */
static TSNode hs_apply_head(TSNode apply) {
    TSNode head = apply;
    while (cbm_source_is(head, "apply"))
        head = cbm_source_field(head, "function");
    return head;
}

static void hs_visit(hs_ctx_t *ctx, TSNode node, unsigned depth);

static void hs_visit_children(hs_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            /* These fields hold names being bound or spelled, not read. */
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (field && (!strcmp(field, "pattern") || !strcmp(field, "patterns")))
                continue;
            if (cbm_source_is(child, "variable") && field && !strcmp(field, "name"))
                continue;
            hs_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void hs_visit(hs_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    /* A name read as a value follows the same lookup as a call head. */
    if (!strcmp(kind, "variable")) {
        const char *name = cbm_source_text(ctx->walk, node);
        ctx->reference = true;
        if (name)
            hs_resolve_bare(ctx, node, name);
        ctx->reference = false;
        return;
    }
    if (!strcmp(kind, "constructor")) {
        const char *name = cbm_source_text(ctx->walk, node);
        ctx->reference = true;
        if (name)
            hs_resolve_bare(ctx, node, name);
        ctx->reference = false;
        return;
    }
    /* A qualified name's parts and a signature's names are not reads, and a declaration
     * of a type spells constructors rather than reading them. */
    if (!strcmp(kind, "qualified") || !strcmp(kind, "signature") || !strcmp(kind, "patterns") ||
        !strcmp(kind, "imports") || !strcmp(kind, "exports") || !strcmp(kind, "header") ||
        !strcmp(kind, "data_type") || !strcmp(kind, "newtype"))
        return;
    /* A do statement's or generator's pattern holds for the statements after it.
     * Leaving it bound until the enclosing `do` ends can only make more names
     * local, which abstains, so it never needs a tighter scope. */
    bool statement_pattern = !ts_node_is_null(cbm_source_field(node, "pattern")) &&
                             (!strcmp(kind, "bind") || !strcmp(kind, "generator"));
    if (statement_pattern) {
        hs_visit_children(ctx, node, depth);
        hs_bind_pattern(ctx, cbm_source_field(node, "pattern"), depth + 1);
        return;
    }
    /* Scoped forms restore the locals of the enclosing expression on leaving. */
    if (!strcmp(kind, "function") || !strcmp(kind, "bind") || !strcmp(kind, "lambda") ||
        !strcmp(kind, "alternative") || !strcmp(kind, "let_in") || !strcmp(kind, "do") ||
        !strcmp(kind, "list_comprehension")) {
        hs_local_t *saved = ctx->locals;
        TSNode patterns = cbm_source_field(node, "patterns");
        if (!ts_node_is_null(patterns))
            hs_bind_pattern(ctx, patterns, depth + 1);
        TSNode pattern = cbm_source_field(node, "pattern");
        if (!ts_node_is_null(pattern))
            hs_bind_pattern(ctx, pattern, depth + 1);
        TSNode binds = cbm_source_field(node, "binds");
        if (!ts_node_is_null(binds))
            hs_bind_decls(ctx, binds);
        hs_visit_children(ctx, node, depth);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "let")) {
        TSNode binds = cbm_source_field(node, "binds");
        if (!ts_node_is_null(binds))
            hs_bind_decls(ctx, binds);
    }
    if (!strcmp(kind, "infix")) {
        /* a `f` b applies f, spelled with backticks. */
        TSNode op = cbm_source_field(node, "operator");
        if (cbm_source_is(op, "infix_id") && ts_node_named_child_count(op) == 1) {
            TSNode head = ts_node_named_child(op, 0);
            if (cbm_source_is(head, "variable"))
                hs_resolve_bare(ctx, node, cbm_source_text(ctx->walk, head));
            else if (cbm_source_is(head, "qualified"))
                hs_resolve_qualified(ctx, node, head);
        }
    }
    if (!strcmp(kind, "apply")) {
        TSNode head = hs_apply_head(node);
        if (cbm_source_is(head, "variable")) {
            hs_resolve_bare(ctx, node, cbm_source_text(ctx->walk, head));
        } else if (cbm_source_is(head, "constructor")) {
            ctx->constructor = true;
            hs_resolve_bare(ctx, node, cbm_source_text(ctx->walk, head));
            ctx->constructor = false;
        } else if (cbm_source_is(head, "qualified")) {
            ctx->constructor = cbm_source_is(cbm_source_field(head, "id"), "constructor");
            hs_resolve_qualified(ctx, node, head);
            ctx->constructor = false;
        } else {
            hs_emit(ctx, node, NULL, CBM_RESOLVE_RULE_NONE, NULL, "haskell_computed_function");
        }
    }
    hs_visit_children(ctx, node, depth);
}

static void hs_resolve(cbm_source_walk_t *walk) {
    hs_ctx_t ctx = {.walk = walk, .module = hs_header_module(walk)};
    hs_gather_imports(&ctx);
    TSNode declarations = hs_child(walk->root, "declarations");
    if (!ts_node_is_null(declarations))
        hs_visit_children(&ctx, declarations, 0);
}

const cbm_source_adapter_t cbm_source_adapter_haskell = {.language = CBM_LANG_HASKELL,
                                                         .collect = hs_collect,
                                                         .resolve = hs_resolve,
                                                         .one_name_one_node = true,
                                                         .decides_references = true,
                                                         .contains_parse_damage = true};
