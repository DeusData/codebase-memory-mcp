/* PHP binding rules, stated against the language manual rather than any name
 * similarity. All of them are source-audited; none consults a score.
 *
 * Names. A class-like name is `namespace\name`, compared case-insensitively.
 * An unqualified class name resolves through the file's `use` aliases and then
 * the current namespace, never to a same-named class elsewhere. An unqualified
 * function name resolves through `use function`, then the current namespace,
 * then the global namespace (the manual's fallback; constants and classes have
 * no such fallback). Variables have their own sigil, so nothing shadows a
 * function or class name.
 *
 * Members. A method call selects the first declaration on the receiver's class
 * chain in PHP's order: the class itself, its traits, its parent, then for an
 * abstract member its interfaces. A class outside the project ends the chain as
 * unknown rather than absent, because it may declare the member. Runtime
 * override is dispatch, which CALLS does not model.
 *
 * Receivers. `$this` is the lexically enclosing class. `self`/`static` name it
 * too, `parent` its parent. A declared parameter, property or return type is
 * enforced at runtime, so it is a fact rather than a hint; PHPDoc is not, so it
 * is never used. A local variable has a type only if every assignment to it
 * gives that same type. References, `global`, `extract`, `eval`, `include` and
 * variable variables end the analysis of what they can touch. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include "syntax_index.h"

enum { PHP_MAX_EXPR_DEPTH = 24 };

typedef enum { PHP_USE_CLASS, PHP_USE_FUNCTION, PHP_USE_CONST } php_use_kind_t;

typedef struct {
    const char *alias;  /* lower-cased, except constants */
    const char *target; /* lower-cased fully qualified name */
    php_use_kind_t kind;
} php_use_t;

typedef struct {
    php_use_t *items;
    size_t count, capacity;
} php_uses_t;

typedef struct {
    const char *canonical; /* lower-cased FQCN; NULL inside an anonymous class */
    const char *parent;
} php_class_t;

typedef struct php_var {
    const char *name;
    const char *declared; /* canonical parameter type */
    const char *type;     /* proven canonical type */
    bool param, blocked, computed, computing;
    uint32_t available_from, escaped_from;
    TSNode *rhs; /* every write's value, in source order */
    int writes, write_capacity;
    struct php_var *next;
} php_var_t;

typedef struct php_scope {
    php_var_t *vars;
    struct php_scope *parent; /* value capture source of an arrow function */
    bool arrow, has_this, dynamic;
} php_scope_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *ns; /* lower-cased, "" for the global namespace */
    php_uses_t uses;
    const php_class_t *cls;
    php_scope_t *scope;
    bool conditional;
} php_ctx_t;

static const char *php_join(php_ctx_t *ctx, const char *left, const char *right) {
    if (!left || !left[0])
        return right;
    return cbm_arena_sprintf(ctx->walk->arena, "%s\\%s", left, right);
}

static const char *php_lower_text(php_ctx_t *ctx, TSNode node) {
    return cbm_source_lower(ctx->walk, cbm_source_text(ctx->walk, node));
}

static void php_add_use(php_ctx_t *ctx, const char *alias, const char *target,
                        php_use_kind_t kind) {
    if (!alias || !target)
        return;
    php_uses_t *uses = &ctx->uses;
    if (uses->count == uses->capacity) {
        size_t capacity = uses->capacity ? uses->capacity * 2 : 16;
        php_use_t *items = cbm_arena_alloc(ctx->walk->arena, capacity * sizeof(*items));
        if (!items) {
            cbm_source_fail(ctx->walk, "php_use_allocation");
            return;
        }
        if (uses->count)
            memcpy(items, uses->items, uses->count * sizeof(*items));
        uses->items = items;
        uses->capacity = capacity;
    }
    uses->items[uses->count++] = (php_use_t){alias, target, kind};
}

static const char *php_find_use(const php_ctx_t *ctx, const char *alias, php_use_kind_t kind) {
    const char *found = NULL;
    for (size_t i = 0; i < ctx->uses.count; i++) {
        const php_use_t *use = &ctx->uses.items[i];
        if (use->kind != kind || strcmp(use->alias, alias))
            continue;
        if (found && strcmp(found, use->target))
            return NULL; /* a compile error in PHP: no single meaning to state */
        found = use->target;
    }
    return found;
}

static void php_note_use_clause(php_ctx_t *ctx, TSNode clause, const char *prefix,
                                php_use_kind_t inherited) {
    php_use_kind_t kind = cbm_source_token(clause, "function") ? PHP_USE_FUNCTION
                          : cbm_source_token(clause, "const")  ? PHP_USE_CONST
                                                               : inherited;
    TSNode alias = cbm_source_field(clause, "alias");
    TSNode name = (TSNode){0};
    uint32_t count = ts_node_named_child_count(clause);
    for (uint32_t i = 0; i < count && ts_node_is_null(name); i++) {
        TSNode child = ts_node_named_child(clause, i);
        if (!ts_node_is_null(alias) && ts_node_eq(child, alias))
            continue;
        if (cbm_source_is(child, "name") || cbm_source_is(child, "qualified_name"))
            name = child;
    }
    const char *text = cbm_source_text(ctx->walk, name);
    if (!text)
        return;
    if (text[0] == '\\')
        text++;
    const char *full = prefix ? php_join(ctx, prefix, text) : text;
    const char *leaf = strrchr(full, '\\');
    leaf = leaf ? leaf + 1 : full;
    const char *local = ts_node_is_null(alias) ? leaf : cbm_source_text(ctx->walk, alias);
    /* Constant names are case-sensitive; the namespace part never reaches a
     * constant lookup here, so only classes and functions fold. */
    php_add_use(ctx, kind == PHP_USE_CONST ? local : cbm_source_lower(ctx->walk, local),
                cbm_source_lower(ctx->walk, full), kind);
}

static void php_note_use(php_ctx_t *ctx, TSNode node) {
    php_use_kind_t kind = cbm_source_token(node, "function") ? PHP_USE_FUNCTION
                          : cbm_source_token(node, "const")  ? PHP_USE_CONST
                                                             : PHP_USE_CLASS;
    const char *prefix = NULL;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "namespace_name") || cbm_source_is(child, "qualified_name")) {
            const char *text = cbm_source_text(ctx->walk, child);
            prefix = text && text[0] == '\\' ? text + 1 : text;
        } else if (cbm_source_is(child, "namespace_use_clause")) {
            php_note_use_clause(ctx, child, NULL, kind);
        } else if (cbm_source_is(child, "namespace_use_group")) {
            uint32_t members = ts_node_named_child_count(child);
            for (uint32_t m = 0; m < members; m++) {
                TSNode member = ts_node_named_child(child, m);
                if (cbm_source_is(member, "namespace_use_clause") ||
                    cbm_source_is(member, "namespace_use_group_clause"))
                    php_note_use_clause(ctx, member, prefix, kind);
            }
        }
    }
}

/* The class-name resolution of the manual's "Name resolution rules". */
static const char *php_class_name(php_ctx_t *ctx, const char *text) {
    if (!text || !text[0])
        return NULL;
    const char *lower = cbm_source_lower(ctx->walk, text);
    if (!lower)
        return NULL;
    if (lower[0] == '\\')
        return lower + 1;
    if (!strncmp(lower, "namespace\\", 10))
        return php_join(ctx, ctx->ns, lower + 10);
    const char *slash = strchr(lower, '\\');
    if (slash) {
        const char *first = cbm_arena_strndup(ctx->walk->arena, lower, (size_t)(slash - lower));
        const char *target = first ? php_find_use(ctx, first, PHP_USE_CLASS) : NULL;
        return target ? php_join(ctx, target, slash + 1) : php_join(ctx, ctx->ns, lower);
    }
    const char *target = php_find_use(ctx, lower, PHP_USE_CLASS);
    return target ? target : php_join(ctx, ctx->ns, lower);
}

static bool php_reserved_type(const char *lower) {
    static const char *const reserved[] = {
        "self", "static", "parent", "array",  "callable", "bool",  "false", "true", "float",
        "int",  "null",   "object", "string", "iterable", "mixed", "never", "void", NULL};
    for (size_t i = 0; reserved[i]; i++)
        if (!strcmp(lower, reserved[i]))
            return true;
    return false;
}

/* Canonical spelling of a declared type: a lower-cased FQCN, "static", or
 * "#name" for a primitive. A nullable single class is that class, because a
 * call on null is an error, not a different target. Other unions have no
 * single declaration to select, so they stay NULL. */
static const char *php_named_type(php_ctx_t *ctx, TSNode named) {
    if (cbm_source_is(named, "primitive_type"))
        return cbm_arena_sprintf(ctx->walk->arena, "#%s", php_lower_text(ctx, named));
    if (!cbm_source_is(named, "named_type"))
        return NULL;
    TSNode inner = ts_node_named_child(named, 0);
    const char *text = cbm_source_text(ctx->walk, inner);
    const char *lower = cbm_source_lower(ctx->walk, text);
    if (!lower)
        return NULL;
    if (!strcmp(lower, "static"))
        return "static";
    if (!strcmp(lower, "self"))
        return ctx->cls ? ctx->cls->canonical : NULL;
    if (!strcmp(lower, "parent"))
        return ctx->cls ? ctx->cls->parent : NULL;
    if (php_reserved_type(lower))
        return cbm_arena_sprintf(ctx->walk->arena, "#%s", lower);
    return php_class_name(ctx, text);
}

static const char *php_type_of(php_ctx_t *ctx, TSNode type) {
    if (ts_node_is_null(type))
        return NULL;
    if (cbm_source_is(type, "optional_type"))
        return php_named_type(ctx, ts_node_named_child(type, 0));
    if (cbm_source_is(type, "union_type")) {
        const char *single = NULL;
        uint32_t count = ts_node_named_child_count(type);
        for (uint32_t i = 0; i < count; i++) {
            const char *member = php_named_type(ctx, ts_node_named_child(type, i));
            if (member && !strcmp(member, "#null"))
                continue;
            if (!member || single)
                return NULL;
            single = member;
        }
        return single;
    }
    return php_named_type(ctx, type);
}

/* ── Phase one: what a file declares ───────────────────────────────── */

static void php_note_params(php_ctx_t *ctx, cbm_source_decl_t *decl, TSNode parameters) {
    if (ts_node_is_null(parameters) || !decl)
        return;
    uint32_t count = ts_node_named_child_count(parameters);
    cbm_source_param_t *params =
        count ? cbm_source_table_alloc(ctx->walk, count * sizeof(*params)) : NULL;
    int used = 0, required = 0;
    bool variadic = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (cbm_source_is(parameter, "comment"))
            continue;
        if (!params)
            return;
        uint32_t flags = 0;
        if (cbm_source_is(parameter, "variadic_parameter")) {
            flags |= CBM_SOURCE_PARAM_VARIADIC;
            variadic = true;
        } else if (!cbm_source_is(parameter, "simple_parameter") &&
                   !cbm_source_is(parameter, "property_promotion_parameter")) {
            return; /* An unmodelled parameter form leaves the signature unknown. */
        }
        if (!ts_node_is_null(cbm_source_field(parameter, "default_value")))
            flags |= CBM_SOURCE_PARAM_OPTIONAL;
        if (!ts_node_is_null(cbm_source_field(parameter, "reference_modifier")) ||
            cbm_source_token(parameter, "&"))
            flags |= CBM_SOURCE_PARAM_BY_REF;
        uint32_t parts = ts_node_named_child_count(parameter);
        for (uint32_t p = 0; p < parts; p++)
            if (cbm_source_is(ts_node_named_child(parameter, p), "reference_modifier"))
                flags |= CBM_SOURCE_PARAM_BY_REF;
        const char *name = cbm_source_text(ctx->walk, cbm_source_field(parameter, "name"));
        const char *type = php_type_of(ctx, cbm_source_field(parameter, "type"));
        params[used++] =
            (cbm_source_param_t){.name = cbm_source_table_intern(ctx->walk, name),
                                 .type = type ? cbm_source_table_intern(ctx->walk, type) : NULL,
                                 .flags = flags};
        if (!(flags & (CBM_SOURCE_PARAM_OPTIONAL | CBM_SOURCE_PARAM_VARIADIC)))
            required = used;
    }
    decl->params = params;
    decl->param_count = used;
    decl->arity_min = required;
    decl->arity_max = variadic ? -1 : used;
}

static uint32_t php_method_flags(php_ctx_t *ctx, TSNode method) {
    uint32_t flags = 0;
    bool visibility = false;
    uint32_t count = ts_node_named_child_count(method);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(method, i);
        if (cbm_source_is(child, "visibility_modifier")) {
            const char *text = php_lower_text(ctx, child);
            visibility = true;
            if (text && !strcmp(text, "private"))
                flags |= CBM_SOURCE_DECL_F_PRIVATE;
            else if (text && !strcmp(text, "protected"))
                flags |= CBM_SOURCE_DECL_F_PROTECTED;
            else
                flags |= CBM_SOURCE_DECL_F_PUBLIC;
        } else if (cbm_source_is(child, "static_modifier")) {
            flags |= CBM_SOURCE_DECL_F_STATIC;
        }
    }
    return visibility ? flags : flags | CBM_SOURCE_DECL_F_PUBLIC;
}

static void php_declare_property(php_ctx_t *ctx, const char *owner, TSNode name, TSNode type,
                                 uint32_t flags) {
    const char *text = cbm_source_text(ctx->walk, name);
    if (!text)
        return;
    cbm_source_decl_t *decl =
        cbm_source_declare(ctx->walk, owner, text, NULL, flags | CBM_SOURCE_DECL_F_VALUE,
                           ts_node_start_point(name).row + 1);
    const char *declared = php_type_of(ctx, type);
    if (decl && declared)
        decl->type = cbm_source_table_intern(ctx->walk, declared);
}

static void php_declare_method(php_ctx_t *ctx, const char *owner, TSNode method) {
    TSNode name = cbm_source_field(method, "name");
    const char *text = cbm_source_text(ctx->walk, name);
    const char *lower = cbm_source_lower(ctx->walk, text);
    if (!lower)
        return;
    uint32_t line = ts_node_start_point(name).row + 1;
    uint32_t flags = php_method_flags(ctx, method);
    if (ts_node_is_null(cbm_source_field(method, "body")))
        flags |= CBM_SOURCE_DECL_F_ABSTRACT;
    cbm_source_decl_t *decl = cbm_source_declare(
        ctx->walk, owner, lower, cbm_source_def_qn(ctx->walk, text, line), flags, line);
    if (!decl)
        return;
    const char *returned = php_type_of(ctx, cbm_source_field(method, "return_type"));
    if (returned)
        decl->type = cbm_source_table_intern(ctx->walk, returned);
    TSNode parameters = cbm_source_field(method, "parameters");
    php_note_params(ctx, decl, parameters);
    /* A promoted constructor parameter also declares the typed property. */
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (cbm_source_is(parameter, "property_promotion_parameter"))
            php_declare_property(ctx, owner, cbm_source_field(parameter, "name"),
                                 cbm_source_field(parameter, "type"), 0);
    }
}

static void php_push_base(php_ctx_t *ctx, cbm_source_names_t *found, const char *entry) {
    if (!cbm_source_names_push(ctx->walk, found, entry))
        cbm_source_fail(ctx->walk, "php_base_allocation");
}

static const char *const *php_bases(php_ctx_t *ctx, TSNode node, const char **parent) {
    cbm_source_names_t found = {0};
    *parent = NULL;
    uint32_t count = ts_node_named_child_count(node);
    bool interface = cbm_source_is(node, "interface_declaration");
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        bool base = cbm_source_is(child, "base_clause");
        bool implements = cbm_source_is(child, "class_interface_clause");
        if (!base && !implements)
            continue;
        uint32_t members = ts_node_named_child_count(child);
        for (uint32_t m = 0; m < members; m++) {
            TSNode member = ts_node_named_child(child, m);
            if (!cbm_source_is(member, "name") && !cbm_source_is(member, "qualified_name"))
                continue;
            const char *canonical = php_class_name(ctx, cbm_source_text(ctx->walk, member));
            if (!canonical)
                continue;
            /* `interface A extends B, C` has interface parents, not a class. */
            bool is_parent = base && !interface;
            if (is_parent)
                *parent = canonical;
            php_push_base(
                ctx, &found,
                cbm_arena_sprintf(ctx->walk->arena, "%c:%s", is_parent ? 'P' : 'I', canonical));
        }
    }
    TSNode body = cbm_source_field(node, "body");
    uint32_t statements = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < statements; i++) {
        TSNode child = ts_node_named_child(body, i);
        if (!cbm_source_is(child, "use_declaration"))
            continue;
        uint32_t members = ts_node_named_child_count(child);
        for (uint32_t m = 0; m < members; m++) {
            TSNode member = ts_node_named_child(child, m);
            if (!cbm_source_is(member, "name") && !cbm_source_is(member, "qualified_name"))
                continue;
            const char *canonical = php_class_name(ctx, cbm_source_text(ctx->walk, member));
            if (canonical)
                php_push_base(ctx, &found, cbm_arena_sprintf(ctx->walk->arena, "T:%s", canonical));
        }
    }
    const char **copy = cbm_source_table_alloc(ctx->walk, (found.count + 1) * sizeof(*copy));
    if (!copy)
        return NULL;
    for (size_t i = 0; i < found.count; i++)
        copy[i] = cbm_source_table_intern(ctx->walk, found.items[i]);
    copy[found.count] = NULL;
    return copy;
}

static bool php_class_like(TSNode node) {
    return cbm_source_is(node, "class_declaration") ||
           cbm_source_is(node, "interface_declaration") ||
           cbm_source_is(node, "trait_declaration") || cbm_source_is(node, "enum_declaration");
}

static void php_declare_class(php_ctx_t *ctx, TSNode node) {
    TSNode name = cbm_source_field(node, "name");
    const char *text = cbm_source_text(ctx->walk, name);
    const char *owner = text ? php_join(ctx, ctx->ns, cbm_source_lower(ctx->walk, text)) : NULL;
    TSNode body = cbm_source_field(node, "body");
    if (!owner || ts_node_is_null(body))
        return;
    uint32_t flags = CBM_SOURCE_DECL_F_TYPE;
    if (ctx->conditional)
        flags |= CBM_SOURCE_DECL_F_CONDITIONAL;
    if (cbm_source_is(node, "interface_declaration"))
        flags |= CBM_SOURCE_DECL_F_INTERFACE;
    if (cbm_source_is(node, "trait_declaration"))
        flags |= CBM_SOURCE_DECL_F_TRAIT;
    if (cbm_source_is(node, "enum_declaration"))
        flags |= CBM_SOURCE_DECL_F_ENUM;
    uint32_t statements = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < statements; i++) {
        TSNode child = ts_node_named_child(body, i);
        if (cbm_source_is(child, "use_declaration")) {
            uint32_t parts = ts_node_named_child_count(child);
            for (uint32_t p = 0; p < parts; p++)
                if (cbm_source_is(ts_node_named_child(child, p), "use_list"))
                    flags |= CBM_SOURCE_DECL_F_ADAPTED;
        }
    }
    uint32_t line = ts_node_start_point(name).row + 1;
    cbm_source_decl_t *decl = cbm_source_declare(
        ctx->walk, owner, "", cbm_source_def_qn(ctx->walk, text, line), flags, line);
    const char *parent = NULL;
    const char *const *bases = php_bases(ctx, node, &parent);
    if (decl)
        decl->bases = bases;
    const php_class_t *saved = ctx->cls;
    php_class_t cls = {.canonical = owner, .parent = parent};
    ctx->cls = &cls;
    for (uint32_t i = 0; i < statements; i++) {
        TSNode child = ts_node_named_child(body, i);
        if (cbm_source_is(child, "method_declaration")) {
            php_declare_method(ctx, owner, child);
        } else if (cbm_source_is(child, "property_declaration")) {
            uint32_t static_flag = php_method_flags(ctx, child) & CBM_SOURCE_DECL_F_STATIC;
            uint32_t parts = ts_node_named_child_count(child);
            for (uint32_t p = 0; p < parts; p++) {
                TSNode element = ts_node_named_child(child, p);
                if (cbm_source_is(element, "property_element"))
                    php_declare_property(ctx, owner, cbm_source_field(element, "name"),
                                         cbm_source_field(child, "type"), static_flag);
            }
        } else if (cbm_source_is(child, "enum_case")) {
            /* An enum case is a value of its enum, spelled with a sigil that no
             * property can take. */
            const char *case_name = cbm_source_text(ctx->walk, cbm_source_field(child, "name"));
            const char *key =
                case_name ? cbm_arena_sprintf(ctx->walk->arena, "@%s", case_name) : NULL;
            cbm_source_decl_t *value =
                key ? cbm_source_declare(ctx->walk, owner, key, NULL, CBM_SOURCE_DECL_F_VALUE,
                                         ts_node_start_point(child).row + 1)
                    : NULL;
            if (value)
                value->type = cbm_source_table_intern(ctx->walk, owner);
        }
    }
    ctx->cls = saved;
}

static void php_declare_function(php_ctx_t *ctx, TSNode node) {
    TSNode name = cbm_source_field(node, "name");
    const char *text = cbm_source_text(ctx->walk, name);
    if (!text)
        return;
    uint32_t line = ts_node_start_point(name).row + 1;
    cbm_source_decl_t *decl =
        cbm_source_declare(ctx->walk, ctx->ns, cbm_source_lower(ctx->walk, text),
                           cbm_source_def_qn(ctx->walk, text, line),
                           ctx->conditional ? CBM_SOURCE_DECL_F_CONDITIONAL : 0, line);
    if (!decl)
        return;
    const char *returned = php_type_of(ctx, cbm_source_field(node, "return_type"));
    if (returned)
        decl->type = cbm_source_table_intern(ctx->walk, returned);
    php_note_params(ctx, decl, cbm_source_field(node, "parameters"));
}

static void php_namespace_enter(php_ctx_t *ctx, TSNode node) {
    TSNode name = cbm_source_field(node, "name");
    ctx->ns = ts_node_is_null(name) ? "" : php_lower_text(ctx, name);
    if (!ctx->ns)
        ctx->ns = "";
    ctx->uses.count = 0;
}

static void php_collect_node(php_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            if (cbm_source_is(child, "namespace_definition")) {
                php_namespace_enter(ctx, child);
                TSNode body = cbm_source_field(child, "body");
                if (!ts_node_is_null(body)) {
                    php_collect_node(ctx, body, depth + 1);
                    ctx->uses.count = 0;
                    ctx->ns = "";
                }
            } else if (cbm_source_is(child, "namespace_use_declaration")) {
                php_note_use(ctx, child);
            } else if (php_class_like(child)) {
                php_declare_class(ctx, child);
                bool saved = ctx->conditional;
                ctx->conditional = true;
                php_collect_node(ctx, cbm_source_field(child, "body"), depth + 1);
                ctx->conditional = saved;
            } else if (cbm_source_is(child, "function_definition")) {
                php_declare_function(ctx, child);
                bool saved = ctx->conditional;
                ctx->conditional = true;
                php_collect_node(ctx, cbm_source_field(child, "body"), depth + 1);
                ctx->conditional = saved;
            } else if (cbm_source_is(child, "anonymous_class")) {
                continue; /* Its members are not reachable by any name. */
            } else {
                /* Anything nested below a statement runs conditionally. */
                bool saved = ctx->conditional;
                bool transparent = cbm_source_is(child, "compound_statement") ||
                                   cbm_source_is(child, "declare_statement");
                ctx->conditional = saved || !transparent;
                php_collect_node(ctx, child, depth + 1);
                ctx->conditional = saved;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void php_collect(cbm_source_walk_t *walk) {
    php_ctx_t ctx = {.walk = walk, .ns = ""};
    php_collect_node(&ctx, walk->root, 0);
}

/* ── Member lookup on the class chain ──────────────────────────────── */

typedef enum { PHP_FOUND, PHP_ABSENT, PHP_OPEN, PHP_AMBIGUOUS } php_lookup_t;

static bool php_same_decl(const cbm_source_decl_t *a, const cbm_source_decl_t *b) {
    return a == b || (a->qn && b->qn && !strcmp(a->qn, b->qn) && !strcmp(a->owner, b->owner));
}

static php_lookup_t php_find_member(const php_ctx_t *ctx, const char *cls, const char *member,
                                    cbm_source_chain_t *chain, const cbm_source_decl_t **out);

static php_lookup_t php_member_on(const php_ctx_t *ctx, const char *cls, const char *member,
                                  cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, cls, "");
    /* Not declared in the project: the class may still declare the member. */
    if (!record)
        return PHP_OPEN;
    if (record->next || (record->flags & CBM_SOURCE_DECL_F_CONDITIONAL))
        return PHP_AMBIGUOUS;
    const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, cls, member);
    if (own) {
        if (own->next && !php_same_decl(own, own->next))
            return PHP_AMBIGUOUS;
        *out = own;
        return PHP_FOUND;
    }
    bool open = false;
    const cbm_source_decl_t *trait = NULL;
    const char *const *bases = record->bases;
    for (size_t i = 0; bases && bases[i]; i++) {
        if (bases[i][0] != 'T')
            continue;
        const cbm_source_decl_t *candidate = NULL;
        php_lookup_t result = php_find_member(ctx, bases[i] + 2, member, chain, &candidate);
        if (result == PHP_AMBIGUOUS)
            return result;
        if (result == PHP_OPEN)
            open = true;
        if (result == PHP_FOUND) {
            if (trait && !php_same_decl(trait, candidate))
                return PHP_AMBIGUOUS;
            trait = candidate;
        }
    }
    if (trait) {
        /* Adaptation rules can rename or exclude the trait's member. */
        if (record->flags & CBM_SOURCE_DECL_F_ADAPTED)
            return PHP_OPEN;
        /* Two traits inserting one method is a fatal error unless the class resolves it, and
         * this class resolves nothing, so a trait outside the project cannot declare it too.
         * An abstract member is the exception: it gives way to the other trait's body. */
        if (open && (trait->flags & CBM_SOURCE_DECL_F_ABSTRACT))
            return PHP_OPEN;
        *out = trait;
        return PHP_FOUND;
    }
    for (size_t i = 0; bases && bases[i]; i++) {
        if (bases[i][0] != 'P')
            continue;
        const cbm_source_decl_t *candidate = NULL;
        php_lookup_t result = php_find_member(ctx, bases[i] + 2, member, chain, &candidate);
        if (result == PHP_AMBIGUOUS)
            return result;
        if (result == PHP_OPEN)
            open = true;
        if (result == PHP_FOUND) {
            /* The trait step above is open: one of them could still declare it. */
            if (open)
                return PHP_OPEN;
            *out = candidate;
            return PHP_FOUND;
        }
    }
    if (open)
        return PHP_OPEN;
    const cbm_source_decl_t *abstract = NULL;
    for (size_t i = 0; bases && bases[i]; i++) {
        if (bases[i][0] != 'I')
            continue;
        const cbm_source_decl_t *candidate = NULL;
        php_lookup_t result = php_find_member(ctx, bases[i] + 2, member, chain, &candidate);
        if (result == PHP_AMBIGUOUS)
            return result;
        if (result == PHP_OPEN)
            open = true;
        if (result == PHP_FOUND) {
            if (abstract && !php_same_decl(abstract, candidate))
                return PHP_AMBIGUOUS;
            abstract = candidate;
        }
    }
    if (open)
        return PHP_OPEN;
    if (abstract) {
        *out = abstract;
        return PHP_FOUND;
    }
    return PHP_ABSENT;
}

static php_lookup_t php_find_member(const php_ctx_t *ctx, const char *cls, const char *member,
                                    cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!cbm_source_chain_enter(ctx->walk, chain, cls))
        return chain->overflow ? PHP_OPEN : PHP_ABSENT;
    php_lookup_t found = php_member_on(ctx, cls, member, chain, out);
    cbm_source_chain_leave(chain);
    return found;
}

static bool php_class_inherits_on(const php_ctx_t *ctx, const char *cls, const char *ancestor,
                                  cbm_source_chain_t *chain) {
    if (!cls || !ancestor || !cbm_source_chain_enter(ctx->walk, chain, cls))
        return false;
    bool inherits = !strcmp(cls, ancestor);
    const cbm_source_decl_t *record = inherits ? NULL : cbm_source_lookup(ctx->walk, cls, "");
    for (size_t i = 0; !inherits && record && record->bases && record->bases[i]; i++)
        inherits = php_class_inherits_on(ctx, record->bases[i] + 2, ancestor, chain);
    cbm_source_chain_leave(chain);
    return inherits;
}

static bool php_class_inherits(const php_ctx_t *ctx, const char *cls, const char *ancestor) {
    cbm_source_chain_t chain = {0};
    return php_class_inherits_on(ctx, cls, ancestor, &chain);
}

static bool php_member_visible(const php_ctx_t *ctx, const cbm_source_decl_t *decl) {
    const char *caller = ctx->cls ? ctx->cls->canonical : NULL;
    if (decl->flags & CBM_SOURCE_DECL_F_PRIVATE) {
        if (!caller)
            return false;
        if (!strcmp(caller, decl->owner))
            return true;
        /* A private trait member is flattened into the class using the trait. */
        const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, decl->owner, "");
        return record && (record->flags & CBM_SOURCE_DECL_F_TRAIT) &&
               php_class_inherits(ctx, caller, decl->owner);
    }
    if (decl->flags & CBM_SOURCE_DECL_F_PROTECTED)
        return caller && (php_class_inherits(ctx, caller, decl->owner) ||
                          php_class_inherits(ctx, decl->owner, caller));
    return true;
}

/* ── Local variable facts ──────────────────────────────────────────── */

static php_var_t *php_var(php_ctx_t *ctx, php_scope_t *scope, const char *name) {
    for (php_var_t *var = scope->vars; var; var = var->next)
        if (!strcmp(var->name, name))
            return var;
    php_var_t *var = cbm_arena_calloc(ctx->walk->arena, sizeof(*var));
    if (!var) {
        cbm_source_fail(ctx->walk, "php_scope_allocation");
        return NULL;
    }
    var->name = name;
    var->escaped_from = UINT32_MAX;
    var->next = scope->vars;
    scope->vars = var;
    return var;
}

static void php_block_var(php_ctx_t *ctx, php_scope_t *scope, TSNode node) {
    if (!cbm_source_is(node, "variable_name"))
        return;
    const char *name = cbm_source_text(ctx->walk, node);
    php_var_t *var = name ? php_var(ctx, scope, name) : NULL;
    if (var)
        var->blocked = true;
}

static void php_block_all_vars(php_ctx_t *ctx, php_scope_t *scope, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    php_block_var(ctx, scope, node);
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        php_block_all_vars(ctx, scope, ts_node_named_child(node, i), depth + 1);
}

/* Functions the manual forbids calling dynamically because they reach into the
 * caller's symbol table, so spelling them is the only way to invoke them. */
static bool php_scope_effect_function(const char *lower) {
    static const char *const names[] = {"extract", "parse_str", "mb_parse_str", "eval", NULL};
    const char *leaf = strrchr(lower, '\\');
    leaf = leaf ? leaf + 1 : lower;
    for (size_t i = 0; names[i]; i++)
        if (!strcmp(leaf, names[i]))
            return true;
    return false;
}

static void php_audit_scope(php_ctx_t *ctx, php_scope_t *scope, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth) || ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);
    /* Own scopes. A closure reaches this one only through its `use` clause. */
    if (!strcmp(kind, "function_definition") || !strcmp(kind, "method_declaration") ||
        php_class_like(node) || !strcmp(kind, "anonymous_class") || !strcmp(kind, "arrow_function"))
        return;
    if (!strcmp(kind, "anonymous_function")) {
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (!cbm_source_is(child, "anonymous_function_use_clause"))
                continue;
            uint32_t uses = ts_node_named_child_count(child);
            for (uint32_t u = 0; u < uses; u++) {
                TSNode use = ts_node_named_child(child, u);
                if (cbm_source_is(use, "by_ref"))
                    php_block_all_vars(ctx, scope, use, depth + 1);
            }
        }
        return;
    }
    if (!strcmp(kind, "assignment_expression")) {
        TSNode left = cbm_source_field(node, "left");
        TSNode right = cbm_source_field(node, "right");
        if (cbm_source_is(left, "variable_name")) {
            const char *name = cbm_source_text(ctx->walk, left);
            php_var_t *var = name ? php_var(ctx, scope, name) : NULL;
            if (var) {
                if (var->writes == var->write_capacity) {
                    int capacity = var->write_capacity ? var->write_capacity * 2 : 4;
                    TSNode *rhs =
                        cbm_arena_alloc(ctx->walk->arena, (size_t)capacity * sizeof(*rhs));
                    if (rhs) {
                        if (var->writes)
                            memcpy(rhs, var->rhs, (size_t)var->writes * sizeof(*rhs));
                        var->rhs = rhs;
                        var->write_capacity = capacity;
                    }
                }
                if (var->writes < var->write_capacity)
                    var->rhs[var->writes++] = right;
                else
                    var->blocked = true;
                uint32_t end = ts_node_end_byte(node);
                if (!var->available_from || end < var->available_from)
                    var->available_from = end;
            }
        } else if (cbm_source_is(left, "dynamic_variable_name")) {
            scope->dynamic = true;
        } else if (!cbm_source_is(left, "member_access_expression") &&
                   !cbm_source_is(left, "nullsafe_member_access_expression") &&
                   !cbm_source_is(left, "subscript_expression") &&
                   !cbm_source_is(left, "scoped_property_access_expression")) {
            /* Destructuring rebinds every variable it names. */
            php_block_all_vars(ctx, scope, left, depth + 1);
        }
        php_audit_scope(ctx, scope, right, depth + 1);
        return;
    }
    if (!strcmp(kind, "reference_assignment_expression")) {
        php_block_all_vars(ctx, scope, cbm_source_field(node, "left"), depth + 1);
        php_block_all_vars(ctx, scope, cbm_source_field(node, "right"), depth + 1);
        return;
    }
    if (!strcmp(kind, "augmented_assignment_expression")) {
        php_block_var(ctx, scope, cbm_source_field(node, "left"));
    } else if (!strcmp(kind, "update_expression")) {
        php_block_var(ctx, scope, cbm_source_field(node, "argument"));
    } else if (!strcmp(kind, "foreach_statement")) {
        /* [iterable, value-or-pair, body]: the middle binds on every iteration. */
        uint32_t count = ts_node_named_child_count(node);
        if (count >= 2)
            php_block_all_vars(ctx, scope, ts_node_named_child(node, 1), depth + 1);
    } else if (!strcmp(kind, "catch_clause")) {
        php_block_var(ctx, scope, cbm_source_field(node, "name"));
    } else if (!strcmp(kind, "global_declaration") ||
               !strcmp(kind, "function_static_declaration") || !strcmp(kind, "unset_statement")) {
        php_block_all_vars(ctx, scope, node, depth + 1);
        return;
    } else if (!strcmp(kind, "dynamic_variable_name") || !strcmp(kind, "include_expression") ||
               !strcmp(kind, "include_once_expression") || !strcmp(kind, "require_expression") ||
               !strcmp(kind, "require_once_expression")) {
        scope->dynamic = true;
    } else if (!strcmp(kind, "function_call_expression")) {
        TSNode function = cbm_source_field(node, "function");
        const char *lower =
            cbm_source_is(function, "name") || cbm_source_is(function, "qualified_name")
                ? php_lower_text(ctx, function)
                : NULL;
        if (lower && php_scope_effect_function(lower))
            scope->dynamic = true;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        php_audit_scope(ctx, scope, ts_node_named_child(node, i), depth + 1);
}

static void php_declare_params(php_ctx_t *ctx, php_scope_t *scope, TSNode parameters) {
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        TSNode name = cbm_source_field(parameter, "name");
        const char *text = cbm_source_text(ctx->walk, name);
        php_var_t *var = text ? php_var(ctx, scope, text) : NULL;
        if (!var)
            continue;
        var->param = true;
        uint32_t parts = ts_node_named_child_count(parameter);
        for (uint32_t p = 0; p < parts; p++)
            if (cbm_source_is(ts_node_named_child(parameter, p), "reference_modifier"))
                var->blocked = true;
        if (cbm_source_token(parameter, "&"))
            var->blocked = true;
        /* A default of null still leaves only the declared class as a target. */
        if (cbm_source_is(parameter, "variadic_parameter"))
            var->blocked = true;
        else
            var->declared = php_type_of(ctx, cbm_source_field(parameter, "type"));
    }
}

/* The function a call's name selects. A name goes through `use function`, then the current
 * namespace, then the global namespace; a qualified one has no fallback. */
static const cbm_source_decl_t *php_function_decl(php_ctx_t *ctx, TSNode function,
                                                  const char **out_owner,
                                                  cbm_resolve_rule_t *out_rule) {
    const char *text = cbm_source_text(ctx->walk, function);
    const char *lower = text ? cbm_source_lower(ctx->walk, text) : NULL;
    const char *owner = NULL, *name = NULL, *fallback_name = NULL;
    *out_rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
    if (!lower) {
        *out_owner = NULL;
        return NULL;
    }
    if (lower[0] == '\\') {
        const char *leaf = strrchr(lower, '\\');
        owner = leaf == lower
                    ? ""
                    : cbm_arena_strndup(ctx->walk->arena, lower + 1, (size_t)(leaf - lower - 1));
        name = leaf + 1;
    } else if (strchr(lower, '\\')) {
        /* Qualified: the first segment goes through class aliases, the rest is
         * namespace, with no global fallback. */
        const char *qualified = !strncmp(lower, "namespace\\", 10)
                                    ? php_join(ctx, ctx->ns, lower + 10)
                                    : php_class_name(ctx, text);
        const char *leaf = qualified ? strrchr(qualified, '\\') : NULL;
        if (leaf) {
            owner = cbm_arena_strndup(ctx->walk->arena, qualified, (size_t)(leaf - qualified));
            name = leaf + 1;
        }
    } else {
        const char *imported = php_find_use(ctx, lower, PHP_USE_FUNCTION);
        if (imported) {
            const char *leaf = strrchr(imported, '\\');
            owner = leaf ? cbm_arena_strndup(ctx->walk->arena, imported, (size_t)(leaf - imported))
                         : "";
            name = leaf ? leaf + 1 : imported;
            *out_rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
        } else {
            owner = ctx->ns;
            name = lower;
            fallback_name = ctx->ns[0] ? lower : NULL;
        }
    }
    const cbm_source_decl_t *decl = name ? cbm_source_lookup(ctx->walk, owner, name) : NULL;
    /* The manual's fallback: an unqualified function not found in the current
     * namespace is looked up in the global namespace. */
    if (!decl && fallback_name) {
        decl = cbm_source_lookup(ctx->walk, "", fallback_name);
        owner = "";
    }
    *out_owner = owner;
    return decl;
}

static const char *php_expr_class(php_ctx_t *ctx, php_scope_t *scope, TSNode node, uint32_t at,
                                  unsigned depth);

static const char *php_var_class(php_ctx_t *ctx, php_scope_t *scope, const char *name, uint32_t at,
                                 unsigned depth) {
    if (!scope)
        return NULL;
    if (!strcmp(name, "$this"))
        return scope->has_this && ctx->cls ? ctx->cls->canonical : NULL;
    if (scope->dynamic)
        return NULL;
    php_var_t *var = NULL;
    for (php_var_t *candidate = scope->vars; candidate; candidate = candidate->next)
        if (!strcmp(candidate->name, name))
            var = candidate;
    if (!var) {
        /* An arrow function reads the enclosing variable by value. */
        return scope->arrow ? php_var_class(ctx, scope->parent, name, at, depth) : NULL;
    }
    if (var->blocked || at >= var->escaped_from)
        return NULL;
    if (!var->computed) {
        if (var->computing)
            return NULL; /* `$x = $x->next()` has no fixed type. */
        var->computing = true;
        const char *type = var->param ? var->declared : NULL;
        bool consistent = !var->param || var->declared;
        for (int w = 0; consistent && w < var->writes; w++) {
            const char *written =
                php_expr_class(ctx, scope, var->rhs[w], ts_node_start_byte(var->rhs[w]), depth + 1);
            consistent = written && (!type || !strcmp(type, written));
            if (consistent)
                type = written;
        }
        var->computing = false;
        var->computed = true;
        var->type = consistent ? type : NULL;
    }
    if (!var->type || (!var->param && at < var->available_from))
        return NULL;
    return var->type;
}

static const char *php_scope_class(php_ctx_t *ctx, php_scope_t *scope, TSNode node, uint32_t at,
                                   unsigned depth, bool *is_relative) {
    *is_relative = false;
    if (cbm_source_is(node, "relative_scope")) {
        const char *text = php_lower_text(ctx, node);
        *is_relative = true;
        if (!ctx->cls || !text)
            return NULL;
        if (!strcmp(text, "parent"))
            return ctx->cls->parent;
        return ctx->cls->canonical; /* self and static */
    }
    if (cbm_source_is(node, "name") || cbm_source_is(node, "qualified_name"))
        return php_class_name(ctx, cbm_source_text(ctx->walk, node));
    return php_expr_class(ctx, scope, node, at, depth);
}

typedef struct {
    const cbm_source_decl_t *decl;
    const char *receiver;
    const char *reason;
} php_member_result_t;

static php_member_result_t php_resolve_member(php_ctx_t *ctx, const char *receiver,
                                              const char *lower) {
    php_member_result_t result = {.receiver = receiver};
    if (!receiver) {
        result.reason = "php_receiver_type_unknown";
        return result;
    }
    if (receiver[0] == '#' || !strcmp(receiver, "static")) {
        result.reason = "php_receiver_not_a_class";
        return result;
    }
    const cbm_source_decl_t *found = NULL;
    cbm_source_chain_t chain = {0};
    switch (php_find_member(ctx, receiver, lower, &chain, &found)) {
    case PHP_FOUND:
        result.decl = found;
        result.reason = "php_member_on_class_chain";
        break;
    case PHP_ABSENT:
        result.reason = "php_member_absent_from_complete_chain";
        break;
    case PHP_AMBIGUOUS:
        result.reason = "php_member_ambiguous";
        break;
    case PHP_OPEN:
        result.reason = "php_member_chain_leaves_project";
        break;
    }
    return result;
}

static const char *php_return_class(const char *declared, const char *receiver) {
    if (!declared)
        return NULL;
    if (!strcmp(declared, "static"))
        return receiver; /* Late static binding returns the receiver's class. */
    return declared;
}

static const char *php_expr_class(php_ctx_t *ctx, php_scope_t *scope, TSNode node, uint32_t at,
                                  unsigned depth) {
    if (depth > PHP_MAX_EXPR_DEPTH || ts_node_is_null(node))
        return NULL;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "variable_name")) {
        const char *name = cbm_source_text(ctx->walk, node);
        return name ? php_var_class(ctx, scope, name, at, depth + 1) : NULL;
    }
    if (!strcmp(kind, "parenthesized_expression") || !strcmp(kind, "clone_expression"))
        return ts_node_named_child_count(node) == 1
                   ? php_expr_class(ctx, scope, ts_node_named_child(node, 0), at, depth + 1)
                   : NULL;
    if (!strcmp(kind, "object_creation_expression")) {
        TSNode name = ts_node_named_child(node, 0);
        bool relative = false;
        if (cbm_source_is(name, "name") && !strcmp(cbm_source_text(ctx->walk, name), "static"))
            return ctx->cls ? ctx->cls->canonical : NULL;
        if (cbm_source_is(name, "name") && !strcmp(cbm_source_text(ctx->walk, name), "self"))
            return ctx->cls ? ctx->cls->canonical : NULL;
        if (cbm_source_is(name, "name") && !strcmp(cbm_source_text(ctx->walk, name), "parent"))
            return ctx->cls ? ctx->cls->parent : NULL;
        if (!cbm_source_is(name, "name") && !cbm_source_is(name, "qualified_name"))
            return NULL;
        return php_scope_class(ctx, scope, name, at, depth + 1, &relative);
    }
    bool member_access = !strcmp(kind, "member_access_expression") ||
                         !strcmp(kind, "nullsafe_member_access_expression");
    if (member_access) {
        TSNode member = cbm_source_field(node, "name");
        if (!cbm_source_is(member, "name"))
            return NULL;
        const char *receiver =
            php_expr_class(ctx, scope, cbm_source_field(node, "object"), at, depth + 1);
        const char *text = cbm_source_text(ctx->walk, member);
        const char *key = text ? cbm_arena_sprintf(ctx->walk->arena, "$%s", text) : NULL;
        php_member_result_t found = php_resolve_member(ctx, receiver, key);
        return found.decl && php_member_visible(ctx, found.decl) && found.decl->type
                   ? php_return_class(found.decl->type, receiver)
                   : NULL;
    }
    if (!strcmp(kind, "scoped_property_access_expression")) {
        bool relative = false;
        const char *receiver =
            php_scope_class(ctx, scope, cbm_source_field(node, "scope"), at, depth + 1, &relative);
        TSNode member = cbm_source_field(node, "name");
        const char *text = cbm_source_text(ctx->walk, member);
        php_member_result_t found = php_resolve_member(ctx, receiver, text);
        return found.decl && (found.decl->flags & CBM_SOURCE_DECL_F_STATIC) &&
                       php_member_visible(ctx, found.decl) && found.decl->type
                   ? php_return_class(found.decl->type, receiver)
                   : NULL;
    }
    if (!strcmp(kind, "class_constant_access_expression")) {
        /* Foo::CASE is an instance of Foo exactly when Foo is an enum and CASE
         * is one of its cases; a plain class constant has no such guarantee. */
        if (ts_node_named_child_count(node) != 2)
            return NULL;
        bool relative = false;
        const char *receiver =
            php_scope_class(ctx, scope, ts_node_named_child(node, 0), at, depth + 1, &relative);
        const char *text = cbm_source_text(ctx->walk, ts_node_named_child(node, 1));
        const char *key = text ? cbm_arena_sprintf(ctx->walk->arena, "@%s", text) : NULL;
        const cbm_source_decl_t *value =
            receiver && key ? cbm_source_lookup(ctx->walk, receiver, key) : NULL;
        return value && !value->next ? value->type : NULL;
    }
    if (!strcmp(kind, "function_call_expression")) {
        /* A declared return type is enforced at runtime, as it is for a method. */
        TSNode function = cbm_source_field(node, "function");
        if (!cbm_source_is(function, "name") && !cbm_source_is(function, "qualified_name"))
            return NULL;
        const char *owner = NULL;
        cbm_resolve_rule_t rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
        const cbm_source_decl_t *decl = php_function_decl(ctx, function, &owner, &rule);
        return decl && !decl->next && !(decl->flags & CBM_SOURCE_DECL_F_CONDITIONAL) ? decl->type
                                                                                     : NULL;
    }
    bool member_call =
        !strcmp(kind, "member_call_expression") || !strcmp(kind, "nullsafe_member_call_expression");
    bool scoped_call = !strcmp(kind, "scoped_call_expression");
    if (member_call || scoped_call) {
        TSNode member = cbm_source_field(node, "name");
        if (!cbm_source_is(member, "name"))
            return NULL;
        const char *lower = php_lower_text(ctx, member);
        bool relative = false;
        const char *receiver =
            member_call
                ? php_expr_class(ctx, scope, cbm_source_field(node, "object"), at, depth + 1)
                : php_scope_class(ctx, scope, cbm_source_field(node, "scope"), at, depth + 1,
                                  &relative);
        php_member_result_t found = php_resolve_member(ctx, receiver, lower);
        return found.decl && php_member_visible(ctx, found.decl)
                   ? php_return_class(found.decl->type, receiver)
                   : NULL;
    }
    return NULL;
}

/* ── Occurrences ───────────────────────────────────────────────────── */

/* A call that cannot bind to this declaration in PHP is an error at runtime,
 * not an edge. Extra positional arguments are legal PHP but select nothing the
 * declaration asked for, so they stay an abstention. */
static bool php_literal_fits(TSNode value, const cbm_source_param_t *param) {
    bool literal_string = cbm_source_is(value, "string") || cbm_source_is(value, "encapsed_string");
    bool literal_number = cbm_source_is(value, "integer") || cbm_source_is(value, "float");
    bool literal = literal_string || literal_number || cbm_source_is(value, "boolean") ||
                   cbm_source_is(value, "null");
    if (!literal)
        return true;
    if (param->flags & CBM_SOURCE_PARAM_BY_REF)
        return false;
    if (!param->type || !strcmp(param->type, "#mixed"))
        return true;
    if (param->type[0] != '#')
        return cbm_source_is(value, "null") && (param->flags & CBM_SOURCE_PARAM_OPTIONAL);
    if (literal_string && (!strcmp(param->type, "#int") || !strcmp(param->type, "#float") ||
                           !strcmp(param->type, "#array")))
        return false;
    if (literal_number && !strcmp(param->type, "#array"))
        return false;
    return true;
}

static bool php_named_argument_known(php_ctx_t *ctx, TSNode label, const cbm_source_decl_t *decl) {
    const char *text = cbm_source_text(ctx->walk, label);
    for (int p = 0; text && p < decl->param_count; p++) {
        const char *param = decl->params[p].name;
        if (param && param[0] == '$' && !strcmp(param + 1, text))
            return true;
    }
    return decl->arity_max < 0; /* A variadic collects unknown names. */
}

static bool php_arguments_fit(php_ctx_t *ctx, TSNode arguments, const cbm_source_decl_t *decl) {
    if (ts_node_is_null(arguments) || !decl || decl->param_count < 0)
        return true;
    int positional = 0, named = 0;
    bool unpacked = false;
    uint32_t count = ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < count; i++) {
        TSNode argument = ts_node_named_child(arguments, i);
        if (cbm_source_is(argument, "comment"))
            continue;
        if (cbm_source_is(argument, "variadic_placeholder"))
            return true; /* First-class callable syntax binds no arguments. */
        if (!cbm_source_is(argument, "argument"))
            return false;
        TSNode label = cbm_source_field(argument, "name");
        TSNode value = ts_node_named_child(argument, ts_node_named_child_count(argument) - 1);
        if (cbm_source_token(argument, "...") || cbm_source_is(value, "variadic_unpacking")) {
            TSNode inner =
                cbm_source_is(value, "variadic_unpacking") ? ts_node_named_child(value, 0) : value;
            if (cbm_source_is(inner, "string") || cbm_source_is(inner, "integer") ||
                cbm_source_is(inner, "boolean") || cbm_source_is(inner, "null"))
                return false;
            unpacked = true;
            continue;
        }
        if (!ts_node_is_null(label)) {
            if (!php_named_argument_known(ctx, label, decl))
                return false;
            named++;
            continue;
        }
        const cbm_source_param_t *param = NULL;
        if (positional < decl->param_count)
            param = &decl->params[positional];
        else if (decl->param_count &&
                 (decl->params[decl->param_count - 1].flags & CBM_SOURCE_PARAM_VARIADIC))
            param = &decl->params[decl->param_count - 1];
        if (!param)
            return false;
        if (!php_literal_fits(value, param))
            return false;
        positional++;
    }
    return unpacked || named || positional >= decl->arity_min;
}

/* An unknown callee could take this variable by reference and rebind it, so
 * facts about it stop at this site. A resolved declaration states it directly. */
static void php_note_argument_effects(php_ctx_t *ctx, php_scope_t *scope, TSNode arguments,
                                      const cbm_source_decl_t *decl) {
    if (!scope || ts_node_is_null(arguments))
        return;
    uint32_t count = ts_node_named_child_count(arguments);
    int position = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode argument = ts_node_named_child(arguments, i);
        if (!cbm_source_is(argument, "argument"))
            continue;
        TSNode value = ts_node_named_child(argument, ts_node_named_child_count(argument) - 1);
        bool may_rebind = true;
        if (decl && decl->param_count >= 0) {
            const cbm_source_param_t *param = position < decl->param_count ? &decl->params[position]
                                              : decl->param_count
                                                  ? &decl->params[decl->param_count - 1]
                                                  : NULL;
            may_rebind = param && (param->flags & CBM_SOURCE_PARAM_BY_REF);
        }
        position++;
        if (!may_rebind || !cbm_source_is(value, "variable_name"))
            continue;
        const char *name = cbm_source_text(ctx->walk, value);
        if (!name || !strcmp(name, "$this"))
            continue;
        php_var_t *var = php_var(ctx, scope, name);
        uint32_t end = ts_node_end_byte(argument);
        if (var && end < var->escaped_from)
            var->escaped_from = end;
    }
}

static void php_visit_function_call(php_ctx_t *ctx, TSNode node) {
    TSNode function = cbm_source_field(node, "function");
    TSNode arguments = cbm_source_field(node, "arguments");
    const char *text = cbm_source_is(function, "name") || cbm_source_is(function, "qualified_name")
                           ? cbm_source_text(ctx->walk, function)
                           : NULL;
    if (!text) {
        php_note_argument_effects(ctx, ctx->scope, arguments, NULL);
        cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                             NULL, "php_dynamic_callee");
        return;
    }
    const char *owner = NULL, *reason = NULL;
    cbm_resolve_rule_t rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
    const cbm_source_decl_t *decl = php_function_decl(ctx, function, &owner, &rule);
    php_note_argument_effects(ctx, ctx->scope, arguments, decl);
    const char *target = cbm_source_unique_qn(decl, &reason);
    if (!decl)
        reason = "php_function_not_declared_in_project";
    if (target && (decl->flags & CBM_SOURCE_DECL_F_CONDITIONAL)) {
        target = NULL;
        reason = "php_function_declaration_conditional";
    }
    if (target && !php_arguments_fit(ctx, arguments, decl)) {
        target = NULL;
        reason = "php_arguments_do_not_fit_declaration";
    }
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_CALL, target, rule,
                         owner && owner[0] ? owner : "php.global_namespace",
                         target ? "php_function_namespace_binding" : reason);
}

static void php_emit_member_call(php_ctx_t *ctx, TSNode node, const php_member_result_t *found,
                                 TSNode arguments, bool static_form, bool relative) {
    const char *reason = found->reason;
    const char *target = NULL;
    const cbm_source_decl_t *decl = found->decl;
    if (decl && (decl->flags & CBM_SOURCE_DECL_F_VALUE)) {
        decl = NULL;
        reason = "php_member_is_not_a_method";
    }
    if (decl && !php_member_visible(ctx, decl)) {
        decl = NULL;
        reason = "php_member_not_visible_from_caller";
    }
    if (decl && static_form && !(decl->flags & CBM_SOURCE_DECL_F_STATIC)) {
        /* parent::m() and Base::m() from an instance method forward $this;
         * from anywhere else this is an error. */
        bool has_this = ctx->scope && ctx->scope->has_this;
        const char *caller = ctx->cls ? ctx->cls->canonical : NULL;
        bool forwards = has_this && caller && found->receiver &&
                        (relative || php_class_inherits(ctx, caller, found->receiver));
        if (!forwards) {
            decl = NULL;
            reason = "php_instance_member_called_statically";
        }
    }
    if (decl && !php_arguments_fit(ctx, arguments, decl)) {
        decl = NULL;
        reason = "php_arguments_do_not_fit_declaration";
    }
    if (decl)
        target = cbm_source_unique_qn(decl, &reason);
    php_note_argument_effects(ctx, ctx->scope, arguments, found->decl);
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_CALL, target,
                         CBM_RESOLVE_RULE_TYPE_MEMBER, found->receiver,
                         target ? "php_declared_receiver_member" : reason);
}

static void php_visit_member_call(php_ctx_t *ctx, TSNode node) {
    TSNode member = cbm_source_field(node, "name");
    TSNode arguments = cbm_source_field(node, "arguments");
    if (!cbm_source_is(member, "name")) {
        php_note_argument_effects(ctx, ctx->scope, arguments, NULL);
        cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                             NULL, "php_dynamic_member_name");
        return;
    }
    uint32_t at = ts_node_start_byte(node);
    const char *receiver = php_expr_class(ctx, ctx->scope, cbm_source_field(node, "object"), at, 0);
    php_member_result_t found = php_resolve_member(ctx, receiver, php_lower_text(ctx, member));
    php_emit_member_call(ctx, node, &found, arguments, false, false);
}

static void php_visit_scoped_call(php_ctx_t *ctx, TSNode node) {
    TSNode member = cbm_source_field(node, "name");
    TSNode arguments = cbm_source_field(node, "arguments");
    if (!cbm_source_is(member, "name")) {
        php_note_argument_effects(ctx, ctx->scope, arguments, NULL);
        cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                             NULL, "php_dynamic_member_name");
        return;
    }
    bool relative = false;
    const char *receiver = php_scope_class(ctx, ctx->scope, cbm_source_field(node, "scope"),
                                           ts_node_start_byte(node), 0, &relative);
    php_member_result_t found = php_resolve_member(ctx, receiver, php_lower_text(ctx, member));
    php_emit_member_call(ctx, node, &found, arguments, true, relative);
}

static void php_visit_new(php_ctx_t *ctx, TSNode node) {
    TSNode name = ts_node_named_child(node, 0);
    uint32_t count = ts_node_named_child_count(node);
    TSNode arguments = (TSNode){0};
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), "arguments"))
            arguments = ts_node_named_child(node, i);
    const char *receiver = php_expr_class(ctx, ctx->scope, node, ts_node_start_byte(node), 0);
    if (!cbm_source_is(name, "name") && !cbm_source_is(name, "qualified_name"))
        receiver = NULL;
    php_member_result_t found = php_resolve_member(ctx, receiver, "__construct");
    /* A class with no constructor runs no code: the class mention is a usage. */
    if (!found.decl && found.reason &&
        !strcmp(found.reason, "php_member_absent_from_complete_chain"))
        found.reason = "php_class_declares_no_constructor";
    php_emit_member_call(ctx, node, &found, arguments, false, false);
}

/* The extractor records a class mention at its `name` token. */
static void php_emit_class_reference(php_ctx_t *ctx, TSNode name) {
    const char *text = cbm_source_text(ctx->walk, name);
    if (!text)
        return;
    const char *lower = cbm_source_lower(ctx->walk, text);
    if (php_reserved_type(lower))
        return;
    bool imported =
        lower[0] != '\\' && !strchr(lower, '\\') && php_find_use(ctx, lower, PHP_USE_CLASS);
    const char *canonical = php_class_name(ctx, text);
    const cbm_source_decl_t *record =
        canonical ? cbm_source_lookup(ctx->walk, canonical, "") : NULL;
    const char *reason = NULL;
    const char *target = cbm_source_unique_qn(record, &reason);
    if (!record)
        reason = imported ? "php_imported_class_not_in_project" : "php_class_not_in_project";
    if (target && (record->flags & CBM_SOURCE_DECL_F_CONDITIONAL)) {
        target = NULL;
        reason = "php_class_declaration_conditional";
    }
    uint32_t start = ts_node_start_byte(name), end = ts_node_end_byte(name);
    cbm_source_emit(ctx->walk, start, end, CBM_SOURCE_SITE_REFERENCE, target,
                    imported ? CBM_RESOLVE_RULE_IMPORT_BINDING : CBM_RESOLVE_RULE_SCOPE_BINDING,
                    canonical, target ? "php_class_name_resolution" : reason);
}

static void php_class_list_references(php_ctx_t *ctx, TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "name") || cbm_source_is(child, "qualified_name"))
            php_emit_class_reference(ctx, child);
    }
}

static void php_visit(php_ctx_t *ctx, TSNode node, unsigned depth);

static void php_visit_children(php_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                php_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void php_visit_function(php_ctx_t *ctx, TSNode node, bool has_this, bool closure,
                               unsigned depth) {
    php_scope_t scope = {.parent = closure ? ctx->scope : NULL,
                         .arrow = cbm_source_is(node, "arrow_function"),
                         .has_this = has_this};
    php_scope_t *saved = ctx->scope;
    TSNode parameters = cbm_source_field(node, "parameters");
    TSNode body = cbm_source_field(node, "body");
    php_declare_params(ctx, &scope, parameters);
    if (closure) {
        /* `use ($x)` copies the value: the outer fact holds at creation. */
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode use = ts_node_named_child(node, i);
            if (!cbm_source_is(use, "anonymous_function_use_clause"))
                continue;
            uint32_t uses = ts_node_named_child_count(use);
            for (uint32_t u = 0; u < uses; u++) {
                TSNode variable = ts_node_named_child(use, u);
                const char *name = cbm_source_is(variable, "variable_name")
                                       ? cbm_source_text(ctx->walk, variable)
                                       : NULL;
                if (!name)
                    continue;
                php_var_t *var = php_var(ctx, &scope, name);
                if (var) {
                    var->param = true;
                    var->declared = php_var_class(ctx, saved, name, ts_node_start_byte(node), 0);
                }
            }
        }
    }
    if (!ts_node_is_null(body))
        php_audit_scope(ctx, &scope, body, depth + 1);
    ctx->scope = &scope;
    if (!ts_node_is_null(parameters))
        php_visit(ctx, parameters, depth + 1);
    TSNode returned = cbm_source_field(node, "return_type");
    if (!ts_node_is_null(returned))
        php_visit(ctx, returned, depth + 1);
    if (!ts_node_is_null(body))
        php_visit(ctx, body, depth + 1);
    ctx->scope = saved;
}

static bool php_static_modifier(TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), "static_modifier"))
            return true;
    return false;
}

static void php_visit_class(php_ctx_t *ctx, TSNode node, unsigned depth) {
    const php_class_t *saved = ctx->cls;
    php_scope_t *saved_scope = ctx->scope;
    php_class_t cls = {0};
    if (php_class_like(node)) {
        const char *text = cbm_source_text(ctx->walk, cbm_source_field(node, "name"));
        const char *lower = cbm_source_lower(ctx->walk, text);
        if (lower)
            cls.canonical = php_join(ctx, ctx->ns, lower);
        const cbm_source_decl_t *record =
            cls.canonical ? cbm_source_lookup(ctx->walk, cls.canonical, "") : NULL;
        for (size_t i = 0; record && record->bases && record->bases[i]; i++)
            if (record->bases[i][0] == 'P')
                cls.parent = record->bases[i] + 2;
    }
    ctx->cls = &cls;
    ctx->scope = NULL;
    php_visit_children(ctx, node, depth);
    ctx->cls = saved;
    ctx->scope = saved_scope;
}

static void php_visit(php_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "namespace_definition")) {
        php_namespace_enter(ctx, node);
        TSNode body = cbm_source_field(node, "body");
        if (!ts_node_is_null(body)) {
            php_visit(ctx, body, depth + 1);
            ctx->uses.count = 0;
            ctx->ns = "";
        }
        return;
    }
    if (!strcmp(kind, "namespace_use_declaration")) {
        php_note_use(ctx, node);
        return;
    }
    if (php_class_like(node) || !strcmp(kind, "anonymous_class")) {
        php_visit_class(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "method_declaration")) {
        php_visit_function(ctx, node, !php_static_modifier(node), false, depth);
        return;
    }
    if (!strcmp(kind, "function_definition")) {
        php_visit_function(ctx, node, false, false, depth);
        return;
    }
    if (!strcmp(kind, "anonymous_function") || !strcmp(kind, "arrow_function")) {
        /* A closure binds $this lexically unless it is declared static. */
        bool bound = ctx->scope && ctx->scope->has_this;
        php_visit_function(ctx, node, bound && !php_static_modifier(node), true, depth);
        return;
    }
    if (!strcmp(kind, "function_call_expression")) {
        php_visit_function_call(ctx, node);
    } else if (!strcmp(kind, "member_call_expression") ||
               !strcmp(kind, "nullsafe_member_call_expression")) {
        php_visit_member_call(ctx, node);
    } else if (!strcmp(kind, "scoped_call_expression")) {
        TSNode scope = cbm_source_field(node, "scope");
        if (cbm_source_is(scope, "name") || cbm_source_is(scope, "qualified_name"))
            php_emit_class_reference(ctx, scope);
        php_visit_scoped_call(ctx, node);
    } else if (!strcmp(kind, "object_creation_expression")) {
        TSNode name = ts_node_named_child(node, 0);
        if (cbm_source_is(name, "name") || cbm_source_is(name, "qualified_name"))
            php_emit_class_reference(ctx, name);
        php_visit_new(ctx, node);
    } else if (!strcmp(kind, "named_type") || !strcmp(kind, "base_clause") ||
               !strcmp(kind, "class_interface_clause") || !strcmp(kind, "use_declaration") ||
               !strcmp(kind, "attribute")) {
        php_class_list_references(ctx, node);
    } else if (!strcmp(kind, "class_constant_access_expression") ||
               !strcmp(kind, "scoped_property_access_expression")) {
        TSNode scope = !strcmp(kind, "class_constant_access_expression")
                           ? ts_node_named_child(node, 0)
                           : cbm_source_field(node, "scope");
        if (cbm_source_is(scope, "name") || cbm_source_is(scope, "qualified_name"))
            php_emit_class_reference(ctx, scope);
    } else if (!strcmp(kind, "binary_expression")) {
        TSNode right = cbm_source_field(node, "right");
        if (cbm_source_token(node, "instanceof") &&
            (cbm_source_is(right, "name") || cbm_source_is(right, "qualified_name")))
            php_emit_class_reference(ctx, right);
    }
    php_visit_children(ctx, node, depth);
}

static void php_resolve(cbm_source_walk_t *walk) {
    php_scope_t file_scope = {0};
    php_ctx_t ctx = {.walk = walk, .ns = "", .scope = &file_scope};
    php_audit_scope(&ctx, &file_scope, walk->root, 0);
    php_visit_children(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_php = {.language = CBM_LANG_PHP,
                                                     .collect = php_collect,
                                                     .resolve = php_resolve,
                                                     .decides_references = true};
