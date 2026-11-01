/* Zig binding rules, from the language reference's container and import chapters.
 *
 * Every file is a struct. `@import("x.zig")` is that file's struct, by a path
 * relative to the importing file. A declaration is visible in its container and
 * in containers nested inside it, and another file reaches it only when it is
 * `pub`. A local or parameter may not shadow a declaration in an enclosing scope:
 * the compiler rejects it, so a name that reaches a container declaration is that
 * declaration. There is no overloading.
 *
 * A member call `x.f()` selects `f` from x's container. Struct members share one
 * namespace, so a field called `f` and a declaration called `f` cannot coexist.
 * x's container is known from the declared type of a parameter, variable or field
 * (through pointers and optionals, which auto-dereference on member access), from
 * a struct literal, or from the declared return type of the function it was
 * initialized by. `@This()` is the enclosing container. Anything computed at
 * comptime, including a generic `Foo(T)` and `anytype`, stays unknown.
 *
 * The graph's Zig QN holds the file but not the struct, so members of two structs
 * in one file sharing a name share a node. That identity cannot name either one,
 * and cbm_source_unique_qn abstains rather than picking. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>

enum { ZIG_MAX_RESOLVE_DEPTH = 16, ZIG_INITIAL_LOCALS = 32 };

/* An encoded path: `F|file|a|b` starts at a file, `N|name|a` at a lexical name,
 * `C|a` at the enclosing container, and `X` is outside the project's sources. */

typedef enum { ZIG_NOTHING, ZIG_CONTAINER, ZIG_FUNCTION } zig_kind_t;

typedef struct {
    zig_kind_t kind;
    const char *container;
    const cbm_source_decl_t *function;
    const char *reason;
} zig_ref_t;

typedef struct {
    const char *name;
    const char *container; /* NULL: a value of unknown type */
} zig_local_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *owner; /* enclosing container */
    zig_local_t *locals;
    size_t local_count, local_capacity;
    unsigned local_struct_depth;
    bool in_function;
} zig_ctx_t;

static const char *zig_normalize_import(zig_ctx_t *ctx, const char *literal) {
    /* A name like "std" or "root" is resolved by the build, not by this path. */
    size_t length = strlen(literal);
    if (length < 5 || strcmp(literal + length - 4, ".zig") || literal[0] == '/')
        return NULL;
    const char *file = ctx->walk->file ? ctx->walk->file : "";
    const char *slash = strrchr(file, '/');
    size_t prefix = slash ? (size_t)(slash - file) : 0;
    char *joined = prefix
                       ? cbm_arena_sprintf(ctx->walk->arena, "%.*s/%s", (int)prefix, file, literal)
                       : cbm_arena_strdup(ctx->walk->arena, literal);
    if (!joined)
        return NULL;
    /* Collapse "." and ".." in place; a path above the root leaves the project. */
    char *out = joined;
    for (const char *read = joined; *read;) {
        const char *end = strchr(read, '/');
        size_t segment = end ? (size_t)(end - read) : strlen(read);
        if (segment == 1 && read[0] == '.') {
            read += end ? 2 : 1;
            continue;
        }
        if (segment == 2 && read[0] == '.' && read[1] == '.') {
            if (out == joined)
                return NULL;
            out--;
            while (out > joined && out[-1] != '/')
                out--;
            read += end ? 3 : 2;
            continue;
        }
        memmove(out, read, segment);
        out += segment;
        read += segment;
        if (*read == '/') {
            *out++ = '/';
            read++;
        }
    }
    *out = '\0';
    return joined;
}

static const char *zig_encode(zig_ctx_t *ctx, TSNode node, unsigned depth) {
    if (depth > ZIG_MAX_RESOLVE_DEPTH || ts_node_is_null(node))
        return NULL;
    cbm_source_walk_t *walk = ctx->walk;
    if (cbm_source_is(node, "identifier")) {
        const char *text = cbm_source_text(walk, node);
        return text ? cbm_arena_sprintf(walk->arena, "N|%s", text) : NULL;
    }
    if (cbm_source_is(node, "field_expression")) {
        const char *object = zig_encode(ctx, cbm_source_field(node, "object"), depth + 1);
        const char *member = cbm_source_text(walk, cbm_source_field(node, "member"));
        return object && member ? cbm_arena_sprintf(walk->arena, "%s|%s", object, member) : NULL;
    }
    if (cbm_source_is(node, "pointer_type") || cbm_source_is(node, "nullable_type")) {
        /* Member access dereferences both; the declaring container is the same. */
        uint32_t count = ts_node_named_child_count(node);
        return count ? zig_encode(ctx, ts_node_named_child(node, count - 1), depth + 1) : NULL;
    }
    if (cbm_source_is(node, "error_union_type"))
        return zig_encode(ctx, cbm_source_field(node, "ok"), depth + 1);
    if (cbm_source_is(node, "builtin_function")) {
        const char *builtin = cbm_source_text(walk, ts_node_named_child(node, 0));
        if (!builtin)
            return NULL;
        if (!strcmp(builtin, "@This"))
            return "C";
        if (strcmp(builtin, "@import"))
            return NULL;
        TSNode arguments = ts_node_named_child(node, 1);
        TSNode string =
            ts_node_is_null(arguments) ? (TSNode){0} : ts_node_named_child(arguments, 0);
        const char *literal =
            cbm_source_is(string, "string") && ts_node_named_child_count(string) == 1
                ? cbm_source_text(walk, ts_node_named_child(string, 0))
                : NULL;
        const char *path = literal ? zig_normalize_import(ctx, literal) : NULL;
        return path ? cbm_arena_sprintf(walk->arena, "F|%s", path) : "X";
    }
    return NULL;
}

static bool zig_container_value(TSNode node) {
    return cbm_source_is(node, "struct_declaration") || cbm_source_is(node, "union_declaration") ||
           cbm_source_is(node, "enum_declaration") || cbm_source_is(node, "opaque_declaration");
}

static const char *zig_nested(zig_ctx_t *ctx, const char *owner, const char *name) {
    return cbm_arena_sprintf(ctx->walk->arena, "%s#%s", owner, name);
}

/* A declaration's name is its first identifier child. */
static TSNode zig_declared_name(TSNode declaration) {
    uint32_t count = ts_node_named_child_count(declaration);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(declaration, i), "identifier"))
            return ts_node_named_child(declaration, i);
    return (TSNode){0};
}

/* The initial value is the last named child that is not the name or the type. */
static TSNode zig_initial_value(TSNode declaration, TSNode name) {
    TSNode type = cbm_source_field(declaration, "type");
    TSNode found = (TSNode){0};
    uint32_t count = ts_node_named_child_count(declaration);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(declaration, i);
        if (ts_node_eq(child, name) || (!ts_node_is_null(type) && ts_node_eq(child, type)))
            continue;
        found = child;
    }
    return found;
}

static void zig_collect_container(zig_ctx_t *ctx, const char *owner, TSNode body, unsigned depth) {
    cbm_source_walk_t *walk = ctx->walk;
    if (!cbm_source_step(walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            uint32_t line = ts_node_start_point(child).row + 1;
            uint32_t visibility = cbm_source_token(child, "pub") ? CBM_SOURCE_DECL_F_PUBLIC : 0;
            if (cbm_source_is(child, "function_declaration")) {
                const char *name = cbm_source_text(walk, cbm_source_field(child, "name"));
                if (!name)
                    continue;
                cbm_source_decl_t *decl = cbm_source_declare(
                    walk, owner, name, cbm_source_def_qn_label(walk, name, line, "Function"),
                    visibility, line);
                const char *returned = zig_encode(ctx, cbm_source_field(child, "type"), 0);
                if (decl && returned)
                    decl->type = cbm_source_table_intern(walk, returned);
            } else if (cbm_source_is(child, "container_field")) {
                const char *name = cbm_source_text(walk, cbm_source_field(child, "name"));
                const char *type = zig_encode(ctx, cbm_source_field(child, "type"), 0);
                const char *key = name ? cbm_arena_sprintf(walk->arena, ".%s", name) : NULL;
                cbm_source_decl_t *decl =
                    key ? cbm_source_declare(walk, owner, key, NULL, CBM_SOURCE_DECL_F_VALUE, line)
                        : NULL;
                if (decl && type)
                    decl->type = cbm_source_table_intern(walk, type);
            } else if (cbm_source_is(child, "using_namespace_declaration")) {
                (void)cbm_source_declare(walk, owner, "", NULL, CBM_SOURCE_DECL_F_OPEN, line);
            } else if (cbm_source_is(child, "variable_declaration")) {
                TSNode name_node = zig_declared_name(child);
                const char *name = cbm_source_text(walk, name_node);
                if (!name)
                    continue;
                TSNode value = zig_initial_value(child, name_node);
                TSNode type = cbm_source_field(child, "type");
                if (zig_container_value(value)) {
                    const char *nested = zig_nested(ctx, owner, name);
                    (void)cbm_source_declare(walk, nested, "", NULL, CBM_SOURCE_DECL_F_TYPE, line);
                    cbm_source_decl_t *decl = cbm_source_declare(
                        walk, owner, name, cbm_source_def_qn(walk, name, line),
                        CBM_SOURCE_DECL_F_TYPE | visibility, line);
                    if (decl)
                        decl->type = cbm_source_table_intern(
                            walk, cbm_arena_sprintf(walk->arena, "O|%s", nested));
                    zig_collect_container(ctx, nested, value, depth + 1);
                    continue;
                }
                cbm_source_decl_t *decl = NULL;
                if (!ts_node_is_null(type)) {
                    /* A typed global is a value of that type, not an alias. */
                    decl = cbm_source_declare(walk, owner, name,
                                              cbm_source_def_qn(walk, name, line),
                                              CBM_SOURCE_DECL_F_VALUE | visibility, line);
                    const char *encoded = zig_encode(ctx, type, 0);
                    if (decl && encoded)
                        decl->type = cbm_source_table_intern(walk, encoded);
                    continue;
                }
                const char *encoded = zig_encode(ctx, value, 0);
                decl = cbm_source_declare(
                    walk, owner, name, cbm_source_def_qn(walk, name, line),
                    (encoded ? CBM_SOURCE_DECL_F_TYPE : CBM_SOURCE_DECL_F_VALUE) | visibility,
                    line);
                if (decl && encoded)
                    decl->type = cbm_source_table_intern(walk, encoded);
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void zig_collect(cbm_source_walk_t *walk) {
    zig_ctx_t ctx = {.walk = walk};
    (void)cbm_source_declare(walk, walk->file, "", NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    zig_collect_container(&ctx, walk->file, walk->root, 0);
}

/* ── Phase two ─────────────────────────────────────────────────────── */

static const char *zig_parent(zig_ctx_t *ctx, const char *owner) {
    const char *hash = strrchr(owner, '#');
    return hash ? cbm_arena_strndup(ctx->walk->arena, owner, (size_t)(hash - owner)) : NULL;
}

static zig_ref_t zig_resolve_encoded(zig_ctx_t *ctx, const char *scope, const char *encoded,
                                     unsigned depth);

/* What (container, name) denotes, from a site in `from`'s file. */
static zig_ref_t zig_member(zig_ctx_t *ctx, const char *container, const char *name,
                            unsigned depth) {
    zig_ref_t ref = {.reason = "zig_member_absent_from_container"};
    const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, container, name);
    if (!decl)
        return ref;
    if (decl->next) {
        ref.reason = "zig_member_not_one_declaration";
        return ref;
    }
    const char *root_end = strchr(container, '#');
    size_t file_length = root_end ? (size_t)(root_end - container) : strlen(container);
    bool same_file = ctx->walk->file && strlen(ctx->walk->file) == file_length &&
                     !strncmp(ctx->walk->file, container, file_length);
    if (!same_file && !(decl->flags & CBM_SOURCE_DECL_F_PUBLIC)) {
        ref.reason = "zig_declaration_not_pub_in_other_file";
        return ref;
    }
    if (decl->flags & CBM_SOURCE_DECL_F_VALUE) {
        ref.reason = "zig_member_is_a_value";
        return ref;
    }
    if (decl->flags & CBM_SOURCE_DECL_F_TYPE)
        return decl->type ? zig_resolve_encoded(ctx, decl->owner, decl->type, depth + 1) : ref;
    ref.kind = ZIG_FUNCTION;
    ref.function = decl;
    ref.reason = "zig_container_function";
    return ref;
}

static zig_ref_t zig_lexical(zig_ctx_t *ctx, const char *scope, const char *name, unsigned depth) {
    for (const char *owner = scope; owner; owner = zig_parent(ctx, owner)) {
        if (cbm_source_lookup(ctx->walk, owner, name))
            return zig_member(ctx, owner, name, depth);
    }
    return (zig_ref_t){.reason = "zig_name_not_declared_in_enclosing_containers"};
}

static zig_ref_t zig_resolve_encoded(zig_ctx_t *ctx, const char *scope, const char *encoded,
                                     unsigned depth) {
    zig_ref_t ref = {.reason = "zig_path_not_resolved"};
    if (!encoded || depth > ZIG_MAX_RESOLVE_DEPTH)
        return ref;
    char *copy = cbm_arena_strdup(ctx->walk->arena, encoded);
    if (!copy)
        return ref;
    char *next = strchr(copy, '|');
    char tag = copy[0];
    if (next)
        *next++ = '\0';
    if (tag == 'X') {
        ref.reason = "zig_import_resolved_by_build_not_path";
        return ref;
    }
    char *segment = next;
    if (tag == 'C') {
        ref.kind = ZIG_CONTAINER;
        ref.container = scope;
    } else {
        if (!segment)
            return ref;
        next = strchr(segment, '|');
        if (next)
            *next++ = '\0';
        if (tag == 'O' || tag == 'F') {
            if (!cbm_source_lookup(ctx->walk, segment, "")) {
                ref.reason = "zig_import_target_not_indexed";
                return ref;
            }
            ref.kind = ZIG_CONTAINER;
            ref.container = segment;
        } else {
            ref = zig_lexical(ctx, scope, segment, depth + 1);
        }
    }
    while (next && *next) {
        if (ref.kind != ZIG_CONTAINER)
            return (zig_ref_t){.reason = ref.reason ? ref.reason : "zig_path_not_resolved"};
        segment = next;
        next = strchr(segment, '|');
        if (next)
            *next++ = '\0';
        ref = zig_member(ctx, ref.container, segment, depth + 1);
    }
    return ref;
}

static void zig_push_local(zig_ctx_t *ctx, const char *name, const char *container) {
    if (!name)
        return;
    if (ctx->local_count == ctx->local_capacity) {
        size_t capacity = ctx->local_capacity ? ctx->local_capacity * 2 : ZIG_INITIAL_LOCALS;
        zig_local_t *items = cbm_arena_alloc(ctx->walk->arena, capacity * sizeof(*items));
        if (!items) {
            cbm_source_fail(ctx->walk, "zig_scope_allocation");
            return;
        }
        if (ctx->local_count)
            memcpy(items, ctx->locals, ctx->local_count * sizeof(*items));
        ctx->locals = items;
        ctx->local_capacity = capacity;
    }
    ctx->locals[ctx->local_count++] = (zig_local_t){name, container};
}

static const zig_local_t *zig_find_local(const zig_ctx_t *ctx, const char *name) {
    for (size_t i = ctx->local_count; i > 0; i--)
        if (!strcmp(ctx->locals[i - 1].name, name))
            return &ctx->locals[i - 1];
    return NULL;
}

/* Which container an expression's value (or type) belongs to, or NULL. */
static const char *zig_expr_container(zig_ctx_t *ctx, TSNode node, unsigned depth) {
    if (depth > ZIG_MAX_RESOLVE_DEPTH || ts_node_is_null(node))
        return NULL;
    cbm_source_walk_t *walk = ctx->walk;
    if (cbm_source_is(node, "identifier")) {
        const char *name = cbm_source_text(walk, node);
        if (!name)
            return NULL;
        const zig_local_t *local = zig_find_local(ctx, name);
        if (local)
            return local->container;
        for (const char *owner = ctx->owner; owner; owner = zig_parent(ctx, owner)) {
            const cbm_source_decl_t *decl = cbm_source_lookup(walk, owner, name);
            if (!decl)
                continue;
            /* A typed global: its value belongs to the declared type's container. */
            if ((decl->flags & CBM_SOURCE_DECL_F_VALUE) && decl->type && !decl->next) {
                zig_ref_t typed = zig_resolve_encoded(ctx, owner, decl->type, depth + 1);
                return typed.kind == ZIG_CONTAINER ? typed.container : NULL;
            }
            zig_ref_t ref = zig_member(ctx, owner, name, depth + 1);
            return ref.kind == ZIG_CONTAINER ? ref.container : NULL;
        }
        return NULL;
    }
    if (cbm_source_is(node, "parenthesized_expression") || cbm_source_is(node, "try_expression") ||
        cbm_source_is(node, "null_coercion_expression")) {
        return ts_node_named_child_count(node)
                   ? zig_expr_container(ctx, ts_node_named_child(node, 0), depth + 1)
                   : NULL;
    }
    if (cbm_source_is(node, "struct_initializer")) {
        const char *encoded = zig_encode(ctx, ts_node_named_child(node, 0), 0);
        zig_ref_t ref =
            encoded ? zig_resolve_encoded(ctx, ctx->owner, encoded, depth + 1) : (zig_ref_t){0};
        return ref.kind == ZIG_CONTAINER ? ref.container : NULL;
    }
    if (cbm_source_is(node, "field_expression")) {
        const char *object = zig_expr_container(ctx, cbm_source_field(node, "object"), depth + 1);
        const char *member = cbm_source_text(walk, cbm_source_field(node, "member"));
        if (!object || !member)
            return NULL;
        const char *key = cbm_arena_sprintf(walk->arena, ".%s", member);
        const cbm_source_decl_t *field = key ? cbm_source_lookup(walk, object, key) : NULL;
        if (field) {
            if (field->next || !field->type)
                return NULL;
            zig_ref_t typed = zig_resolve_encoded(ctx, field->owner, field->type, depth + 1);
            return typed.kind == ZIG_CONTAINER ? typed.container : NULL;
        }
        const cbm_source_decl_t *decl = cbm_source_lookup(walk, object, member);
        if (decl && (decl->flags & CBM_SOURCE_DECL_F_VALUE) && decl->type && !decl->next) {
            zig_ref_t typed = zig_resolve_encoded(ctx, decl->owner, decl->type, depth + 1);
            return typed.kind == ZIG_CONTAINER ? typed.container : NULL;
        }
        zig_ref_t ref = zig_member(ctx, object, member, depth + 1);
        return ref.kind == ZIG_CONTAINER ? ref.container : NULL;
    }
    if (cbm_source_is(node, "call_expression")) {
        /* `Foo(T)` builds a type at comptime; only a declared return type names one. */
        TSNode function = cbm_source_field(node, "function");
        zig_ref_t callee = {0};
        if (cbm_source_is(function, "identifier")) {
            const char *name = cbm_source_text(walk, function);
            if (name && !zig_find_local(ctx, name))
                callee = zig_lexical(ctx, ctx->owner, name, depth + 1);
        } else if (cbm_source_is(function, "field_expression")) {
            const char *object =
                zig_expr_container(ctx, cbm_source_field(function, "object"), depth + 1);
            const char *member = cbm_source_text(walk, cbm_source_field(function, "member"));
            if (object && member)
                callee = zig_member(ctx, object, member, depth + 1);
        }
        if (callee.kind != ZIG_FUNCTION || !callee.function->type)
            return NULL;
        zig_ref_t returned =
            zig_resolve_encoded(ctx, callee.function->owner, callee.function->type, depth + 1);
        return returned.kind == ZIG_CONTAINER ? returned.container : NULL;
    }
    if (cbm_source_is(node, "builtin_function")) {
        const char *encoded = zig_encode(ctx, node, 0);
        zig_ref_t ref =
            encoded ? zig_resolve_encoded(ctx, ctx->owner, encoded, depth + 1) : (zig_ref_t){0};
        return ref.kind == ZIG_CONTAINER ? ref.container : NULL;
    }
    return NULL;
}

static const char *zig_type_container(zig_ctx_t *ctx, TSNode type) {
    const char *encoded = zig_encode(ctx, type, 0);
    if (!encoded)
        return NULL;
    zig_ref_t ref = zig_resolve_encoded(ctx, ctx->owner, encoded, 0);
    return ref.kind == ZIG_CONTAINER ? ref.container : NULL;
}

static void zig_emit_ref(zig_ctx_t *ctx, TSNode call, const zig_ref_t *ref,
                         cbm_resolve_rule_t rule) {
    const char *reason = ref->reason;
    const char *target = NULL;
    if (ref->kind == ZIG_FUNCTION) {
        const char *identity = NULL;
        target = cbm_source_unique_qn(ref->function, &identity);
        if (!target)
            reason = identity;
    } else if (ref->kind == ZIG_CONTAINER) {
        reason = "zig_call_of_a_container_value";
    }
    cbm_source_emit_node(ctx->walk, call, CBM_SOURCE_SITE_CALL, target, rule,
                         ref->function ? ref->function->owner : NULL, reason);
}

static void zig_visit_call(zig_ctx_t *ctx, TSNode call) {
    TSNode function = cbm_source_field(call, "function");
    cbm_source_walk_t *walk = ctx->walk;
    if (ctx->local_struct_depth) {
        cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                             "zig_call_inside_local_container");
        return;
    }
    if (cbm_source_is(function, "identifier")) {
        const char *name = cbm_source_text(walk, function);
        if (!name)
            return;
        if (zig_find_local(ctx, name)) {
            cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                                 NULL, "zig_function_valued_local");
            return;
        }
        zig_ref_t ref = zig_lexical(ctx, ctx->owner, name, 0);
        bool same_file = ref.function && !strcmp(ref.function->file, walk->file);
        zig_emit_ref(ctx, call, &ref,
                     same_file ? CBM_RESOLVE_RULE_SCOPE_BINDING : CBM_RESOLVE_RULE_IMPORT_BINDING);
        return;
    }
    if (cbm_source_is(function, "field_expression")) {
        const char *object = zig_expr_container(ctx, cbm_source_field(function, "object"), 0);
        const char *member = cbm_source_text(walk, cbm_source_field(function, "member"));
        if (!object || !member) {
            cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE,
                                 NULL, "zig_receiver_container_unknown");
            return;
        }
        zig_ref_t ref = zig_member(ctx, object, member, 0);
        zig_emit_ref(ctx, call, &ref, CBM_RESOLVE_RULE_TYPE_MEMBER);
        return;
    }
    cbm_source_emit_node(walk, call, CBM_SOURCE_SITE_CALL, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                         "zig_computed_callee");
}

static void zig_visit(zig_ctx_t *ctx, TSNode node, unsigned depth);

/* A name read as a value or type is the innermost declaration of it: locals, then the
 * enclosing containers outward. Zig has no shadowing of those, and nothing is inherited. */
static void zig_visit_reference(zig_ctx_t *ctx, TSNode node) {
    const char *name = cbm_source_text(ctx->walk, node);
    if (!name || !strcmp(name, "_") || ctx->local_struct_depth || zig_find_local(ctx, name))
        return;
    for (const char *owner = ctx->owner; owner; owner = zig_parent(ctx, owner)) {
        const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, owner, name);
        if (!decl)
            continue;
        const char *identity = NULL;
        const char *target = cbm_source_unique_qn(decl, &identity);
        cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, target,
                             CBM_RESOLVE_RULE_SCOPE_BINDING, owner,
                             target ? "zig_enclosing_container_declaration" : identity);
        return;
    }
}

static void zig_visit_children(zig_ctx_t *ctx, TSNode node, unsigned depth) {
    /* These hold names being bound or spelled, not read. */
    if (cbm_source_is(node, "block_label") || cbm_source_is(node, "break_label") ||
        cbm_source_is(node, "payload"))
        return;
    TSNode declared = cbm_source_is(node, "variable_declaration") ? zig_declared_name(node)
                                                                   : (TSNode){0};
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (cbm_source_is(child, "identifier") &&
                ((field && (!strcmp(field, "member") || !strcmp(field, "name"))) ||
                 (!ts_node_is_null(declared) && ts_node_eq(child, declared))))
                continue;
            zig_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void zig_visit_function(zig_ctx_t *ctx, TSNode node, unsigned depth) {
    size_t saved = ctx->local_count;
    bool saved_function = ctx->in_function;
    ctx->in_function = true;
    /* The grammar gives a function's parameter list no field name. */
    TSNode parameters = (TSNode){0};
    uint32_t parts = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < parts && ts_node_is_null(parameters); i++)
        if (cbm_source_is(ts_node_named_child(node, i), "parameters"))
            parameters = ts_node_named_child(node, i);
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (!cbm_source_is(parameter, "parameter"))
            continue;
        const char *name = cbm_source_text(ctx->walk, cbm_source_field(parameter, "name"));
        TSNode type = cbm_source_field(parameter, "type");
        if (!ts_node_is_null(type))
            zig_visit(ctx, type, depth + 1);
        zig_push_local(ctx, name, zig_type_container(ctx, type));
    }
    TSNode returned = cbm_source_field(node, "type");
    if (!ts_node_is_null(returned))
        zig_visit(ctx, returned, depth + 1);
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(body))
        zig_visit(ctx, body, depth + 1);
    ctx->local_count = saved;
    ctx->in_function = saved_function;
}

static void zig_visit_container_members(zig_ctx_t *ctx, const char *owner, TSNode body,
                                        unsigned depth) {
    const char *saved = ctx->owner;
    size_t locals = ctx->local_count;
    ctx->owner = owner;
    ctx->local_count = 0;
    zig_visit_children(ctx, body, depth);
    ctx->owner = saved;
    ctx->local_count = locals;
}

static void zig_visit(zig_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        zig_visit_reference(ctx, node);
        return;
    }
    if (cbm_source_is(node, "function_declaration")) {
        zig_visit_function(ctx, node, depth);
        return;
    }
    if (cbm_source_is(node, "test_declaration")) {
        size_t saved = ctx->local_count;
        bool saved_function = ctx->in_function;
        ctx->in_function = true;
        zig_visit_children(ctx, node, depth);
        ctx->local_count = saved;
        ctx->in_function = saved_function;
        return;
    }
    if (cbm_source_is(node, "block") || cbm_source_is(node, "if_statement") ||
        cbm_source_is(node, "for_statement") || cbm_source_is(node, "while_statement") ||
        cbm_source_is(node, "if_expression") || cbm_source_is(node, "for_expression") ||
        cbm_source_is(node, "while_expression") || cbm_source_is(node, "switch_case")) {
        size_t saved = ctx->local_count;
        zig_visit_children(ctx, node, depth);
        ctx->local_count = saved;
        return;
    }
    if (cbm_source_is(node, "payload")) {
        /* A capture's type comes from the expression it unwraps: not modelled. */
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (cbm_source_is(child, "identifier"))
                zig_push_local(ctx, cbm_source_text(ctx->walk, child), NULL);
        }
        return;
    }
    if (cbm_source_is(node, "variable_declaration")) {
        TSNode name_node = zig_declared_name(node);
        const char *name = cbm_source_text(ctx->walk, name_node);
        TSNode value = zig_initial_value(node, name_node);
        if (zig_container_value(value)) {
            if (ctx->in_function || ctx->local_struct_depth) {
                /* A struct declared in a function has no stable table entry. */
                ctx->local_struct_depth++;
                zig_visit_children(ctx, value, depth);
                ctx->local_struct_depth--;
                zig_push_local(ctx, name, NULL);
            } else if (name) {
                zig_visit_container_members(ctx, zig_nested(ctx, ctx->owner, name), value, depth);
            }
            return;
        }
        zig_visit_children(ctx, node, depth);
        /* Below a container, this is a local coming into scope after its value. */
        if (ctx->in_function && name && strcmp(name, "_")) {
            TSNode type = cbm_source_field(node, "type");
            zig_push_local(ctx, name,
                           !ts_node_is_null(type) ? zig_type_container(ctx, type)
                                                  : zig_expr_container(ctx, value, 0));
        }
        return;
    }
    if (cbm_source_is(node, "call_expression"))
        zig_visit_call(ctx, node);
    zig_visit_children(ctx, node, depth);
}

static void zig_resolve(cbm_source_walk_t *walk) {
    zig_ctx_t ctx = {.walk = walk, .owner = walk->file};
    zig_visit_children(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_zig = {
    .language = CBM_LANG_ZIG,
    .collect = zig_collect,
    .resolve = zig_resolve,
    .decides_references = true};
