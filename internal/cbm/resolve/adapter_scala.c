/* Scala binding rules, from the specification's chapter on bindings and names.
 *
 * A bare name is looked up from the inside out. Local definitions come first, then the
 * members of the enclosing templates, with what they inherit. Then the compilation
 * unit's own package members, then explicit imports, then wildcard imports, and last
 * the package members in other files, since a package clause from another compilation
 * unit binds with the lowest precedence. A wildcard import from outside the project
 * can therefore shadow such a member, and a template inheriting from outside the
 * project may have any member, so what lies past either is not decided.
 *
 * A class and its companion object are two namespaces. `Foo.m()` reads the object,
 * `new Foo` and `x.m()` with x of type Foo read the class. `Foo(...)` calls the
 * object's apply when one fits; otherwise it constructs the class, as a case class's
 * synthetic apply and Scala 3's universal apply do. The graph has no constructor node,
 * so the selected declaration is the class, and the evidence says it is a class call.
 *
 * `x.m()` selects m on x's static type: a declared parameter, val or return type, or
 * `new`. Implicit conversions add members only where the type has none of that name,
 * so a member found on the chain is the one selected, and an absent one is not guessed.
 * Overloads are told apart by arity; those that share a graph node need no more.
 *
 * A pattern `case e: Foo` and a parameter `(y: Foo) =>` declare a type too.
 *
 * `a op b` is `a.op(b)`, and `a op: b` is `b.op:(a)`. `x.m` with m a method taking no
 * arguments is a call of it, so `x.m.n()` reads n on m's return type.
 *
 * `def f = new Foo(...)` and `val v = new Foo(...)` have type Foo, as has a block ending
 * in that. A type written in another file is read with that file's imports, which the
 * table holds, so a base class or return type imported there is not left unknown there.
 * An explicit import binds its name even when it leaves the project.
 *
 * `new Foo { ... }` opens a scope whose members are its own and those on Foo's chain. They
 * hide every enclosing template, so a Foo outside the project leaves its bare names open.
 *
 * Known gaps, each abstaining: other inferred return types, type members and path-dependent
 * types, generic element types, postfix calls, and calls to Java classes. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>

#define SCALA_RECORD "&template"
#define SCALA_PACKAGE "&package"
#define SCALA_IMPORT "&import"
#define SCALA_TYPE_NAME "type:"
#define SCALA_OBJECT_NAME "object:"

enum { SCALA_MAX_PACKAGES = 8, SCALA_MAX_BASES = 32 };

typedef struct scala_local {
    const char *name;
    const char *type_key;
    struct scala_local *next;
} scala_local_t;

typedef struct scala_import {
    const char *path;   /* dotted, as written */
    const char *member; /* selected name, NULL for a wildcard */
    const char *alias;  /* the name it binds */
    struct scala_import *next;
} scala_import_t;

/* An anonymous class around this point. */
typedef struct scala_anonymous {
    const char *base; /* its template key, NULL when it is outside the project */
    struct scala_anonymous *next;
} scala_anonymous_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *packages[SCALA_MAX_PACKAGES]; /* innermost first */
    int package_count;
    const char *template_key; /* enclosing template, or NULL */
    const char *type_params;
    scala_local_t *locals;
    scala_import_t *imports;       /* the file's */
    scala_import_t *block_imports; /* in the blocks around this point */
    scala_anonymous_t *anonymous;
    TSNode callee;           /* the function of the call being walked */
    bool child_of_block;     /* the node being visited stands directly in a block */
    bool child_of_template;  /* or directly in a template's body */
    CBMHashTable *type_memo; /* (file, owner, name) to template key, for this file's walk */
} scala_ctx_t;

typedef enum {
    SCALA_UNKNOWN,
    SCALA_VALUE,
    SCALA_TEMPLATE, /* an object or a package member reached by name */
    SCALA_CLASS,
    SCALA_PACKAGE_REF,
    SCALA_FUNCTION
} scala_kind_t;

typedef struct {
    scala_kind_t kind;
    const char *key;               /* SCALA_VALUE, SCALA_TEMPLATE, SCALA_CLASS: template key */
    const char *companion;         /* SCALA_CLASS or SCALA_TEMPLATE: the other namespace */
    const char *package;           /* SCALA_PACKAGE_REF */
    const cbm_source_decl_t *decl; /* SCALA_FUNCTION; SCALA_CLASS: the class */
    const cbm_source_decl_t *object_decl;
    const cbm_source_decl_t *value_decl; /* the val or var a name read reaches */
    bool via_import, via_receiver, from_super;
    const char *reason;
} scala_ref_t;

/* ── Syntax ────────────────────────────────────────────────────────── */

static TSNode scala_child_of_kind(TSNode node, const char *kind) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), kind))
            return ts_node_named_child(node, i);
    return (TSNode){0};
}

static bool scala_list_has(const char *list, const char *name) {
    if (!list || !name)
        return false;
    size_t length = strlen(name);
    for (const char *p = list; (p = strstr(p, name)); p += length)
        if (p > list && p[-1] == ',' && p[length] == ',')
            return true;
    return false;
}

static const char *scala_type_params(cbm_source_walk_t *walk, TSNode owner, const char *outer) {
    TSNode params = cbm_source_field(owner, "type_parameters");
    if (ts_node_is_null(params))
        return outer;
    char *joined =
        outer ? cbm_arena_strdup(walk->arena, outer) : cbm_arena_strdup(walk->arena, ",");
    uint32_t count = ts_node_named_child_count(params);
    for (uint32_t i = 0; joined && i < count; i++) {
        TSNode param = ts_node_named_child(params, i);
        TSNode name = cbm_source_field(param, "name");
        if (ts_node_is_null(name))
            name = cbm_source_is(param, "identifier") ? param
                                                      : scala_child_of_kind(param, "identifier");
        const char *text = cbm_source_text(walk, name);
        if (text)
            joined = cbm_arena_sprintf(walk->arena, "%s%s,", joined, text);
    }
    return joined;
}

/* The written name of a type this table could own: `A`, `a.B`, `A[T]`. */
static const char *scala_type_text(cbm_source_walk_t *walk, TSNode node, const char *type_params) {
    if (cbm_source_is(node, "generic_type"))
        return scala_type_text(walk, cbm_source_field(node, "type"), type_params);
    if (cbm_source_is(node, "type_identifier") || cbm_source_is(node, "stable_type_identifier")) {
        const char *text = cbm_source_text(walk, node);
        return text && !scala_list_has(type_params, text) ? text : NULL;
    }
    return NULL;
}

static bool scala_template_kind(TSNode node) {
    return cbm_source_is(node, "class_definition") || cbm_source_is(node, "object_definition") ||
           cbm_source_is(node, "trait_definition") || cbm_source_is(node, "enum_definition");
}

/* The dotted text of a path's identifiers. */
static const char *scala_path(cbm_source_walk_t *walk, TSNode node) {
    const char *joined = NULL;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (!cbm_source_is(part, "identifier"))
            continue;
        const char *text = cbm_source_text(walk, part);
        if (!text)
            return NULL;
        joined = joined ? cbm_arena_sprintf(walk->arena, "%s.%s", joined, text) : text;
    }
    return joined;
}

/* ── Phase one ─────────────────────────────────────────────────────── */

static void scala_note_params(cbm_source_walk_t *walk, TSNode function, cbm_source_decl_t *decl) {
    /* The grammar names this field on a plain def but not on a generic one. */
    TSNode parameters = scala_child_of_kind(function, "parameters");
    decl->arity_min = decl->arity_max = 0;
    if (ts_node_is_null(parameters))
        return;
    uint32_t count = ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode param = ts_node_named_child(parameters, i);
        if (!cbm_source_is(param, "parameter"))
            continue;
        const char *text = cbm_source_text(walk, cbm_source_field(param, "type"));
        decl->arity_max++;
        if (ts_node_is_null(cbm_source_field(param, "default_value")))
            decl->arity_min++;
        if (text && strchr(text, '*'))
            decl->arity_max = -1;
        if (decl->arity_max < 0)
            break;
    }
}

static uint32_t scala_flags(cbm_source_walk_t *walk, TSNode node) {
    TSNode modifiers = scala_child_of_kind(node, "modifiers");
    const char *text = ts_node_is_null(modifiers) ? NULL : cbm_source_text(walk, modifiers);
    /* `private[pkg]` opens the member to that package, which is treated as the project. */
    return text && strstr(text, "private") && !strstr(text, "private[") ? CBM_SOURCE_DECL_F_PRIVATE
                                                                        : 0;
}

static void scala_collect_template(cbm_source_walk_t *walk, const char *package, const char *outer,
                                   TSNode node);

/* The type of a right-hand side that says it: `new Foo(...)`, alone or ending a block. */
static const char *scala_constructed_type(cbm_source_walk_t *walk, TSNode value,
                                          const char *type_params) {
    while (cbm_source_is(value, "block")) {
        uint32_t count = ts_node_named_child_count(value);
        TSNode last = (TSNode){0};
        for (uint32_t i = count; i > 0 && ts_node_is_null(last); i--) {
            TSNode child = ts_node_named_child(value, i - 1);
            if (!cbm_source_is(child, "comment") && !cbm_source_is(child, "block_comment"))
                last = child;
        }
        value = last;
    }
    return cbm_source_is(value, "instance_expression")
               ? scala_type_text(walk, ts_node_named_child(value, 0), type_params)
               : NULL;
}

static void scala_collect_value(cbm_source_walk_t *walk, const char *owner, TSNode node,
                                const char *type_params) {
    TSNode pattern = cbm_source_field(node, "pattern");
    if (!cbm_source_is(pattern, "identifier"))
        return;
    const char *type = scala_type_text(walk, cbm_source_field(node, "type"), type_params);
    if (ts_node_is_null(cbm_source_field(node, "type")))
        type = scala_constructed_type(walk, cbm_source_field(node, "value"), type_params);
    const char *name = cbm_source_text(walk, pattern);
    uint32_t line = ts_node_start_point(pattern).row + 1;
    cbm_source_decl_t *decl =
        cbm_source_declare(walk, owner, name, cbm_source_def_qn(walk, name, line),
                           CBM_SOURCE_DECL_F_VALUE | scala_flags(walk, node), line);
    if (decl && type)
        decl->type = cbm_source_table_intern(walk, type);
}

static void scala_collect_function(cbm_source_walk_t *walk, const char *owner, TSNode node,
                                   const char *type_params) {
    TSNode name = cbm_source_field(node, "name");
    const char *text = cbm_source_text(walk, name);
    if (!text)
        return;
    uint32_t line = ts_node_start_point(name).row + 1;
    cbm_source_decl_t *decl = cbm_source_declare(
        walk, owner, text, cbm_source_def_qn(walk, text, line), scala_flags(walk, node), line);
    if (!decl)
        return;
    scala_note_params(walk, node, decl);
    const char *params = scala_type_params(walk, node, type_params);
    TSNode returned = cbm_source_field(node, "return_type");
    const char *type = ts_node_is_null(returned)
                           ? scala_constructed_type(walk, cbm_source_field(node, "body"), params)
                           : scala_type_text(walk, returned, params);
    if (type)
        decl->type = cbm_source_table_intern(walk, type);
}

static void scala_collect_members(cbm_source_walk_t *walk, const char *package, const char *owner,
                                  TSNode body, const char *type_params) {
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (cbm_source_is(member, "function_definition") ||
            cbm_source_is(member, "function_declaration"))
            scala_collect_function(walk, owner, member, type_params);
        else if (cbm_source_is(member, "val_definition") ||
                 cbm_source_is(member, "var_definition") ||
                 cbm_source_is(member, "val_declaration") ||
                 cbm_source_is(member, "var_declaration"))
            scala_collect_value(walk, owner, member, type_params);
        else if (scala_template_kind(member))
            scala_collect_template(walk, package, owner, member);
    }
}

static void scala_note_bases(cbm_source_walk_t *walk, TSNode node, cbm_source_decl_t *record,
                             const char *type_params) {
    const char *found[SCALA_MAX_BASES];
    size_t used = 0;
    bool open = false;
    TSNode extend = cbm_source_field(node, "extend");
    uint32_t count = ts_node_is_null(extend) ? 0 : ts_node_named_child_count(extend);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(extend, i);
        if (cbm_source_is(child, "arguments"))
            continue;
        const char *text = scala_type_text(walk, child, type_params);
        if (!text || used == SCALA_MAX_BASES)
            open = true;
        else
            found[used++] = text;
    }
    const char **copy = cbm_source_table_alloc(walk, (used + 1) * sizeof(*copy));
    if (!copy)
        return;
    for (size_t i = 0; i < used; i++)
        copy[i] = cbm_source_table_intern(walk, found[i]);
    copy[used] = NULL;
    record->bases = copy;
    if (open)
        record->flags |= CBM_SOURCE_DECL_F_OPEN;
}

static void scala_collect_class_parameters(cbm_source_walk_t *walk, const char *key, TSNode node,
                                           const char *type_params) {
    TSNode parameters = cbm_source_field(node, "class_parameters");
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode param = ts_node_named_child(parameters, i);
        if (!cbm_source_is(param, "class_parameter"))
            continue;
        const char *name = cbm_source_text(walk, cbm_source_field(param, "name"));
        const char *type = scala_type_text(walk, cbm_source_field(param, "type"), type_params);
        uint32_t line = ts_node_start_point(param).row + 1;
        cbm_source_decl_t *decl =
            name ? cbm_source_declare(walk, key, name, cbm_source_def_qn(walk, name, line),
                                      CBM_SOURCE_DECL_F_VALUE, line)
                 : NULL;
        if (decl && type)
            decl->type = cbm_source_table_intern(walk, type);
    }
}

static void scala_collect_template(cbm_source_walk_t *walk, const char *package, const char *outer,
                                   TSNode node) {
    const char *name = cbm_source_text(walk, cbm_source_field(node, "name"));
    if (!name)
        return;
    bool object = cbm_source_is(node, "object_definition");
    const char *key =
        outer ? cbm_arena_sprintf(walk->arena, "%s.%s%s", outer, object ? "$" : "", name)
              : cbm_arena_sprintf(walk->arena, "t:%s#%s%s", package, object ? "$" : "", name);
    const char *owner = outer ? outer : cbm_arena_sprintf(walk->arena, "p:%s", package);
    const char *name_key =
        cbm_arena_sprintf(walk->arena, "%s%s", object ? SCALA_OBJECT_NAME : SCALA_TYPE_NAME, name);
    if (!key || !owner || !name_key)
        return;
    uint32_t line = ts_node_start_point(node).row + 1;
    const char *type_params = scala_type_params(walk, node, NULL);
    cbm_source_decl_t *record =
        cbm_source_declare(walk, key, SCALA_RECORD, NULL, CBM_SOURCE_DECL_F_TYPE, line);
    if (record)
        scala_note_bases(walk, node, record, type_params);
    cbm_source_decl_t *declared =
        cbm_source_declare(walk, owner, name_key, cbm_source_def_qn(walk, name, line),
                           CBM_SOURCE_DECL_F_TYPE | scala_flags(walk, node), line);
    if (declared)
        declared->type = cbm_source_table_intern(walk, key);
    scala_collect_class_parameters(walk, key, node, type_params);
    scala_collect_members(walk, package, key, cbm_source_field(node, "body"), type_params);
}

/* A file's package clauses nest: `package a.b; package c` sees c and a.b. */
static int scala_file_packages(cbm_source_walk_t *walk, const char **out) {
    const char *joined = NULL;
    int used = 0;
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (!cbm_source_is(child, "package_clause"))
            continue;
        const char *path = scala_path(walk, cbm_source_field(child, "name"));
        if (!path || used == SCALA_MAX_PACKAGES)
            break;
        joined = joined ? cbm_arena_sprintf(walk->arena, "%s.%s", joined, path) : path;
        out[used++] = joined;
    }
    /* Innermost first. */
    for (int i = 0; i < used / 2; i++) {
        const char *held = out[i];
        out[i] = out[used - 1 - i];
        out[used - 1 - i] = held;
    }
    if (!used) {
        out[0] = "";
        used = 1;
    }
    return used;
}

static void scala_add_import(scala_ctx_t *ctx, TSNode node, scala_import_t **list);

/* A file's imports stay in the table: a type written in that file is read through them. */
static const char *scala_imports_owner(CBMArena *arena, const char *file) {
    return cbm_arena_sprintf(arena, "i:%s", file);
}

static void scala_collect_imports(cbm_source_walk_t *walk) {
    scala_ctx_t ctx = {.walk = walk};
    scala_import_t *imports = NULL;
    TSTreeCursor cursor = ts_tree_cursor_new(walk->root);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (cbm_source_is(child, "import_declaration"))
                scala_add_import(&ctx, child, &imports);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    const char *owner = imports ? scala_imports_owner(walk->arena, walk->file) : NULL;
    for (const scala_import_t *import = imports; import && owner; import = import->next) {
        cbm_source_decl_t *decl =
            cbm_source_declare(walk, owner, SCALA_IMPORT, NULL, CBM_SOURCE_DECL_F_TYPE, 1);
        const char **parts = cbm_source_table_alloc(walk, 3 * sizeof(*parts));
        if (!decl || !parts)
            return;
        decl->type = cbm_source_table_intern(walk, import->path);
        parts[0] = cbm_source_table_intern(walk, import->member);
        parts[1] = cbm_source_table_intern(walk, import->alias);
        decl->bases = parts;
    }
}

static void scala_collect(cbm_source_walk_t *walk) {
    scala_collect_imports(walk);
    const char *packages[SCALA_MAX_PACKAGES];
    (void)scala_file_packages(walk, packages);
    const char *package = packages[0];
    const char *owner = cbm_arena_sprintf(walk->arena, "p:%s", package);
    if (!owner)
        return;
    (void)cbm_source_declare(walk, owner, SCALA_PACKAGE, NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (scala_template_kind(child))
            scala_collect_template(walk, package, NULL, child);
        else if (cbm_source_is(child, "function_definition"))
            scala_collect_function(walk, owner, child, NULL);
        else if (cbm_source_is(child, "val_definition") || cbm_source_is(child, "var_definition"))
            scala_collect_value(walk, owner, child, NULL);
    }
}

/* ── Phase two: declarations by name ───────────────────────────────── */

static const char *scala_key_of(scala_ctx_t *ctx, const char *prefix, const char *name) {
    return cbm_arena_sprintf(ctx->walk->arena, "%s%s", prefix, name);
}

static const char *scala_visible_decl_key(const cbm_source_decl_t *decl) {
    return decl && !decl->next && decl->type ? decl->type : NULL;
}

/* A type or object name from one owner: a package or a template. */
static scala_ref_t scala_member_names(scala_ctx_t *ctx, const char *owner, const char *name) {
    scala_ref_t ref = {.reason = "scala_name_not_in_project"};
    const char *type_name = scala_key_of(ctx, SCALA_TYPE_NAME, name);
    const char *object_name = scala_key_of(ctx, SCALA_OBJECT_NAME, name);
    const cbm_source_decl_t *type =
        type_name ? cbm_source_lookup(ctx->walk, owner, type_name) : NULL;
    const cbm_source_decl_t *object =
        object_name ? cbm_source_lookup(ctx->walk, owner, object_name) : NULL;
    if (object) {
        ref.kind = SCALA_TEMPLATE;
        ref.key = scala_visible_decl_key(object);
        ref.companion = scala_visible_decl_key(type);
        ref.decl = type && !type->next ? type : NULL;
        ref.object_decl = object->next ? NULL : object;
        ref.reason = "scala_object_name";
        return ref;
    }
    if (type) {
        ref.kind = SCALA_CLASS;
        ref.key = scala_visible_decl_key(type);
        ref.decl = type->next ? NULL : type;
        ref.reason = "scala_class_name";
    }
    return ref;
}

static const char *scala_package_owner(scala_ctx_t *ctx, const char *package) {
    return cbm_arena_sprintf(ctx->walk->arena, "p:%s", package);
}

/* What a dotted import path names: a package, or a template in one. */
static scala_ref_t scala_resolve_path(scala_ctx_t *ctx, const char *path) {
    scala_ref_t ref = {.reason = "scala_import_outside_project"};
    cbm_source_walk_t *walk = ctx->walk;
    /* Absolute first, then relative to the packages this file sees. */
    int attempts = ctx->package_count + 1;
    for (int a = 0; a < attempts; a++) {
        const char *full =
            a == 0 ? path
                   : (ctx->packages[a - 1][0]
                          ? cbm_arena_sprintf(walk->arena, "%s.%s", ctx->packages[a - 1], path)
                          : NULL);
        if (!full)
            continue;
        const char *owner = scala_package_owner(ctx, full);
        if (owner && cbm_source_lookup(walk, owner, SCALA_PACKAGE))
            return (scala_ref_t){.kind = SCALA_PACKAGE_REF, .package = full};
        /* The longest package prefix, then templates down from it. */
        char *prefix = cbm_arena_strdup(walk->arena, full);
        for (char *dot = prefix ? strrchr(prefix, '.') : NULL; dot; dot = strrchr(prefix, '.')) {
            *dot = '\0';
            owner = scala_package_owner(ctx, prefix);
            if (!owner || !cbm_source_lookup(walk, owner, SCALA_PACKAGE))
                continue;
            const char *rest = full + strlen(prefix) + 1;
            scala_ref_t step = {.kind = SCALA_PACKAGE_REF, .package = prefix};
            while (*rest) {
                const char *next = strchr(rest, '.');
                const char *part =
                    next ? cbm_arena_strndup(walk->arena, rest, (size_t)(next - rest)) : rest;
                const char *from = step.kind == SCALA_PACKAGE_REF ? owner : step.key;
                if (!part || !from)
                    return ref;
                step = scala_member_names(ctx, from, part);
                if (step.kind == SCALA_UNKNOWN)
                    return ref;
                rest = next ? next + 1 : "";
            }
            return step;
        }
    }
    return ref;
}

/* A written type name, seen from a declaring owner's packages. */
static const char *scala_lookup_type(scala_ctx_t *ctx, const char *owner, const char *text,
                                     const char *file);

/* ── Phase two: templates ──────────────────────────────────────────── */

typedef enum { SCALA_FOUND, SCALA_ABSENT, SCALA_OPEN, SCALA_AMBIGUOUS } scala_lookup_t;

static bool scala_arity_fits(const cbm_source_decl_t *decl, int arguments) {
    if (arguments < 0 || (decl->flags & CBM_SOURCE_DECL_F_VALUE))
        return true;
    return arguments >= decl->arity_min && (decl->arity_max < 0 || arguments <= decl->arity_max);
}

static int scala_select(const scala_ctx_t *ctx, const cbm_source_decl_t *first, int arguments,
                        const cbm_source_decl_t **out) {
    const cbm_source_decl_t *selected = NULL;
    for (const cbm_source_decl_t *decl = first; decl; decl = decl->next) {
        if ((decl->flags & CBM_SOURCE_DECL_F_PRIVATE) && strcmp(decl->file, ctx->walk->file))
            continue;
        if (!scala_arity_fits(decl, arguments))
            continue;
        if (selected) {
            bool same_node = selected->qn && decl->qn && !strcmp(selected->qn, decl->qn);
            if (!same_node)
                return -1;
            continue;
        }
        selected = decl;
    }
    *out = selected;
    return selected ? 1 : 0;
}

static scala_lookup_t scala_find_member(scala_ctx_t *ctx, const char *key, const char *name,
                                        int arguments, bool bases_only, cbm_source_chain_t *chain,
                                        const cbm_source_decl_t **out) {
    if (!cbm_source_chain_first_visit(chain, key))
        return chain->overflow ? SCALA_OPEN : SCALA_ABSENT;
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, key, SCALA_RECORD);
    if (!record || record->next || (record->flags & CBM_SOURCE_DECL_F_OPEN))
        return record && !bases_only && cbm_source_lookup(ctx->walk, key, name) &&
                       scala_select(ctx, cbm_source_lookup(ctx->walk, key, name), arguments, out) >
                           0
                   ? SCALA_FOUND
                   : SCALA_OPEN;
    if (!bases_only) {
        const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, key, name);
        int selected = own ? scala_select(ctx, own, arguments, out) : 0;
        if (selected < 0)
            return SCALA_AMBIGUOUS;
        if (selected > 0)
            return SCALA_FOUND;
    }
    /* Linearization: the last-mixed trait first, the superclass last. */
    size_t count = 0;
    while (record->bases && record->bases[count])
        count++;
    for (size_t step = count; step > 0; step--) {
        const char *parent = scala_lookup_type(ctx, key, record->bases[step - 1], record->file);
        if (!parent)
            return SCALA_OPEN;
        scala_lookup_t found = scala_find_member(ctx, parent, name, arguments, false, chain, out);
        if (found != SCALA_ABSENT)
            return found;
    }
    return SCALA_ABSENT;
}

static const char *scala_key_package(cbm_source_walk_t *walk, const char *key) {
    if (!strncmp(key, "p:", 2))
        return key + 2;
    const char *hash = strchr(key, '#');
    return hash ? cbm_arena_strndup(walk->arena, key + 2, (size_t)(hash - key - 2)) : "";
}

static const char *scala_class_key_of(const scala_ref_t *ref) {
    if (ref->kind == SCALA_CLASS)
        return ref->key;
    if (ref->kind == SCALA_TEMPLATE)
        return ref->companion;
    return NULL;
}

/* A path written in a file resolves from that file's package. */
static scala_ref_t scala_resolve_path_in(scala_ctx_t *ctx, const char *path, const char *owner,
                                         const char *file) {
    if (!strcmp(file, ctx->walk->file))
        return scala_resolve_path(ctx, path);
    const char *held = ctx->packages[0];
    int count = ctx->package_count;
    ctx->packages[0] = scala_key_package(ctx->walk, owner);
    ctx->package_count = 1;
    scala_ref_t found = scala_resolve_path(ctx, path);
    ctx->packages[0] = held;
    ctx->package_count = count;
    return found;
}

/* A type or object a template inherits. Each template on the chain is read once. */
static scala_ref_t scala_inherited_names(scala_ctx_t *ctx, const char *key, const char *text,
                                         cbm_source_chain_t *chain) {
    scala_ref_t none = {.reason = "scala_name_not_in_project"};
    if (!cbm_source_chain_first_visit(chain, key))
        return none;
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, key, SCALA_RECORD);
    if (!record || record->next)
        return none;
    for (size_t i = 0; record->bases && record->bases[i]; i++) {
        const char *parent = scala_lookup_type(ctx, key, record->bases[i], record->file);
        if (!parent)
            continue;
        scala_ref_t found = scala_member_names(ctx, parent, text);
        if (found.kind == SCALA_UNKNOWN)
            found = scala_inherited_names(ctx, parent, text, chain);
        if (found.kind != SCALA_UNKNOWN)
            return found;
    }
    return none;
}

/* A type name as `file` reads it from `owner`: the templates around it, that file's explicit
 * imports, its package, then its wildcard imports. */
static const char *scala_lookup_type_uncached(scala_ctx_t *ctx, const char *owner, const char *text,
                                              const char *file) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *imports_owner = scala_imports_owner(walk->arena, file);
    const cbm_source_decl_t *imports =
        imports_owner ? cbm_source_lookup(walk, imports_owner, SCALA_IMPORT) : NULL;
    const char *dot = strchr(text, '.');
    if (dot) {
        scala_ref_t path = scala_resolve_path_in(ctx, text, owner, file);
        const char *key = scala_class_key_of(&path);
        if (key)
            return key;
        /* The first segment may be a name that file imported. */
        size_t head = (size_t)(dot - text);
        for (const cbm_source_decl_t *import = imports; import; import = import->next) {
            const char *alias = import->bases[1];
            if (!alias || strlen(alias) != head || strncmp(alias, text, head))
                continue;
            scala_ref_t full = scala_resolve_path_in(
                ctx, cbm_arena_sprintf(walk->arena, "%s.%s%s", import->type, import->bases[0], dot),
                owner, file);
            return scala_class_key_of(&full);
        }
        return NULL;
    }
    /* Templates from inside out, each with what it inherits. */
    char *scope = !strncmp(owner, "t:", 2) ? cbm_arena_strdup(walk->arena, owner) : NULL;
    while (scope) {
        scala_ref_t nested = scala_member_names(ctx, scope, text);
        const char *key = scala_class_key_of(&nested);
        if (!key) {
            cbm_source_chain_t chain = {0};
            scala_ref_t inherited = scala_inherited_names(ctx, scope, text, &chain);
            key = scala_class_key_of(&inherited);
        }
        if (key)
            return key;
        char *up = strrchr(scope, '.');
        char *hash = strchr(scope, '#');
        if (!up || (hash && up < hash))
            break;
        *up = '\0';
    }
    /* An explicit import binds the name, also when it leaves the project. */
    for (const cbm_source_decl_t *import = imports; import; import = import->next) {
        if (!import->bases[0] || strcmp(import->bases[1], text))
            continue;
        scala_ref_t path = scala_resolve_path_in(
            ctx, cbm_arena_sprintf(walk->arena, "%s.%s", import->type, import->bases[0]), owner,
            file);
        return scala_class_key_of(&path);
    }
    const char *package_owner = scala_package_owner(ctx, scala_key_package(walk, owner));
    scala_ref_t own =
        package_owner ? scala_member_names(ctx, package_owner, text) : (scala_ref_t){0};
    const char *key = scala_class_key_of(&own);
    if (key)
        return key;
    for (const cbm_source_decl_t *import = imports; import; import = import->next) {
        if (import->bases[0])
            continue;
        scala_ref_t from = scala_resolve_path_in(ctx, import->type, owner, file);
        const char *inside = from.kind == SCALA_PACKAGE_REF
                                 ? scala_package_owner(ctx, from.package)
                                 : (from.kind == SCALA_TEMPLATE ? from.key : NULL);
        scala_ref_t found = inside ? scala_member_names(ctx, inside, text) : (scala_ref_t){0};
        key = scala_class_key_of(&found);
        if (key)
            return key;
    }
    return NULL;
}

static const char *scala_lookup_type(scala_ctx_t *ctx, const char *owner, const char *text,
                                     const char *file) {
    if (!owner || !text || !file)
        return NULL;
    const char *memo_key =
        ctx->type_memo ? cbm_arena_sprintf(ctx->walk->arena, "%s\x1f%s\x1f%s", file, owner, text)
                       : NULL;
    const char *known = memo_key ? cbm_ht_get(ctx->type_memo, memo_key) : NULL;
    if (known)
        return known[0] ? known : NULL;
    /* A base named through what it inherits itself finds nothing there while it is read. */
    if (memo_key)
        cbm_ht_set(ctx->type_memo, memo_key, (void *)"");
    const char *key = scala_lookup_type_uncached(ctx, owner, text, file);
    if (memo_key)
        cbm_ht_set(ctx->type_memo, memo_key, (void *)(key ? key : ""));
    return key;
}

static scala_ref_t scala_ref_of_member(scala_ctx_t *ctx, const cbm_source_decl_t *decl) {
    scala_ref_t ref = {.reason = "scala_name_not_in_project"};
    if (decl->flags & CBM_SOURCE_DECL_F_VALUE) {
        const char *type =
            decl->type ? scala_lookup_type(ctx, decl->owner, decl->type, decl->file) : NULL;
        ref.kind = type ? SCALA_VALUE : SCALA_UNKNOWN;
        ref.key = type;
        ref.value_decl = decl;
        ref.reason = type ? "scala_call_of_a_value" : "scala_value_of_unknown_type";
        return ref;
    }
    ref.kind = SCALA_FUNCTION;
    ref.decl = decl;
    ref.reason = "scala_declared_function";
    return ref;
}

static scala_ref_t scala_template_member(scala_ctx_t *ctx, const char *key, const char *name,
                                         int arguments, bool bases_only) {
    scala_ref_t ref = {.reason = "scala_member_absent_from_chain"};
    const cbm_source_decl_t *member = NULL;
    cbm_source_chain_t chain = {0};
    switch (scala_find_member(ctx, key, name, arguments, bases_only, &chain, &member)) {
    case SCALA_FOUND:
        ref = scala_ref_of_member(ctx, member);
        ref.via_receiver = true;
        return ref;
    case SCALA_OPEN:
        ref.reason = "scala_member_chain_leaves_project";
        return ref;
    case SCALA_AMBIGUOUS:
        ref.reason = "scala_overload_not_decided";
        return ref;
    case SCALA_ABSENT:
        break;
    }
    /* A type nested in the template, reached through it. */
    scala_ref_t nested = scala_member_names(ctx, key, name);
    return nested.kind != SCALA_UNKNOWN ? nested : ref;
}

/* ── Phase two: names ──────────────────────────────────────────────── */

static scala_ref_t scala_from_import(scala_ctx_t *ctx, const scala_import_t *import,
                                     const char *name, int arguments) {
    scala_ref_t none = {.reason = "scala_name_not_in_project"};
    scala_ref_t from = scala_resolve_path(ctx, import->path);
    const char *member = import->member ? import->member : name;
    if (from.kind == SCALA_PACKAGE_REF) {
        const char *owner = scala_package_owner(ctx, from.package);
        const cbm_source_decl_t *function =
            owner ? cbm_source_lookup(ctx->walk, owner, member) : NULL;
        const cbm_source_decl_t *selected = NULL;
        if (function && scala_select(ctx, function, arguments, &selected) > 0) {
            scala_ref_t ref = scala_ref_of_member(ctx, selected);
            ref.via_import = true;
            return ref;
        }
        scala_ref_t named = owner ? scala_member_names(ctx, owner, member) : none;
        named.via_import = true;
        return named.kind == SCALA_UNKNOWN ? none : named;
    }
    if (from.kind == SCALA_TEMPLATE && from.key) {
        const cbm_source_decl_t *found = NULL;
        cbm_source_chain_t chain = {0};
        if (scala_find_member(ctx, from.key, member, arguments, false, &chain, &found) ==
            SCALA_FOUND) {
            scala_ref_t ref = scala_ref_of_member(ctx, found);
            ref.via_import = true;
            return ref;
        }
        scala_ref_t named = scala_member_names(ctx, from.key, member);
        named.via_import = true;
        return named.kind == SCALA_UNKNOWN ? none : named;
    }
    return none;
}

static bool scala_import_is_outside(scala_ctx_t *ctx, const scala_import_t *import) {
    scala_ref_t from = scala_resolve_path(ctx, import->path);
    return from.kind != SCALA_PACKAGE_REF && from.kind != SCALA_TEMPLATE;
}

/* One precedence level: a binding found here is the answer; two is an error at the use. */
static scala_ref_t scala_imports_level(scala_ctx_t *ctx, const scala_import_t *imports,
                                       const char *name, int arguments, bool wildcards,
                                       bool *outside) {
    scala_ref_t found = {.reason = "scala_name_not_in_project"};
    for (const scala_import_t *import = imports; import; import = import->next) {
        if (wildcards == (import->member != NULL))
            continue;
        if (!wildcards && strcmp(import->alias, name))
            continue;
        if (scala_import_is_outside(ctx, import)) {
            *outside = true;
            continue;
        }
        scala_ref_t next = scala_from_import(ctx, import, name, arguments);
        if (next.kind == SCALA_UNKNOWN)
            continue;
        if (found.kind != SCALA_UNKNOWN && (found.decl != next.decl || found.key != next.key))
            return (scala_ref_t){.reason = "scala_two_imports_provide_name"};
        found = next;
    }
    return found;
}

static scala_ref_t scala_package_function(scala_ctx_t *ctx, const char *owner, const char *name,
                                          int arguments, bool same_unit_only) {
    scala_ref_t none = {.reason = "scala_name_not_in_project"};
    const cbm_source_decl_t *first = cbm_source_lookup(ctx->walk, owner, name);
    const cbm_source_decl_t *selected = NULL;
    if (first && scala_select(ctx, first, arguments, &selected) > 0 &&
        (!same_unit_only || !strcmp(selected->file, ctx->walk->file)))
        return scala_ref_of_member(ctx, selected);
    scala_ref_t named = scala_member_names(ctx, owner, name);
    if (named.kind == SCALA_UNKNOWN)
        return none;
    const cbm_source_decl_t *decl = named.decl ? named.decl : named.object_decl;
    if (same_unit_only && decl && strcmp(decl->file, ctx->walk->file))
        return none;
    if (same_unit_only && !decl)
        return none;
    return named;
}

static scala_ref_t scala_name(scala_ctx_t *ctx, const char *name, int arguments) {
    scala_ref_t ref = {.reason = "scala_name_not_in_project"};
    for (const scala_local_t *local = ctx->locals; local; local = local->next) {
        if (!strcmp(local->name, name)) {
            ref.kind = local->type_key ? SCALA_VALUE : SCALA_UNKNOWN;
            ref.key = local->type_key;
            ref.reason = local->type_key ? "scala_call_of_a_value" : "scala_local_of_unknown_type";
            return ref;
        }
    }
    if (scala_list_has(ctx->type_params, name)) {
        ref.reason = "scala_type_parameter";
        return ref;
    }
    /* An import in a surrounding block binds closer than anything outside the block. */
    bool outside = false;
    scala_ref_t block =
        scala_imports_level(ctx, ctx->block_imports, name, arguments, false, &outside);
    if (block.kind != SCALA_UNKNOWN)
        return block;
    block = scala_imports_level(ctx, ctx->block_imports, name, arguments, true, &outside);
    if (block.kind != SCALA_UNKNOWN)
        return block;
    if (outside) {
        ref.reason = "scala_outside_import_may_provide_name";
        return ref;
    }
    for (const scala_anonymous_t *anonymous = ctx->anonymous; anonymous;
         anonymous = anonymous->next) {
        const cbm_source_decl_t *member = NULL;
        cbm_source_chain_t chain = {0};
        scala_lookup_t found =
            anonymous->base
                ? scala_find_member(ctx, anonymous->base, name, arguments, false, &chain, &member)
                : SCALA_OPEN;
        if (found == SCALA_FOUND)
            return scala_ref_of_member(ctx, member);
        if (found != SCALA_ABSENT) {
            ref.reason = found == SCALA_OPEN ? "scala_inheritance_leaves_project"
                                             : "scala_overload_not_decided";
            return ref;
        }
    }
    /* Enclosing templates, inner to outer. */
    char *scope = ctx->template_key ? cbm_arena_strdup(ctx->walk->arena, ctx->template_key) : NULL;
    while (scope) {
        const cbm_source_decl_t *member = NULL;
        cbm_source_chain_t chain = {0};
        switch (scala_find_member(ctx, scope, name, arguments, false, &chain, &member)) {
        case SCALA_FOUND:
            return scala_ref_of_member(ctx, member);
        case SCALA_AMBIGUOUS:
            ref.reason = "scala_overload_not_decided";
            return ref;
        case SCALA_OPEN:
            ref.reason = "scala_inheritance_leaves_project";
            /* Term names can come from the chain; a template of the unit is still
             * the one written here when the name is a type's. */
            {
                scala_ref_t nested = scala_member_names(ctx, scope, name);
                if (nested.kind != SCALA_UNKNOWN)
                    return nested;
            }
            return ref;
        case SCALA_ABSENT:
            break;
        }
        scala_ref_t nested = scala_member_names(ctx, scope, name);
        if (nested.kind != SCALA_UNKNOWN)
            return nested;
        cbm_source_chain_t names = {0};
        nested = scala_inherited_names(ctx, scope, name, &names);
        if (nested.kind != SCALA_UNKNOWN)
            return nested;
        char *up = strrchr(scope, '.');
        char *hash = strchr(scope, '#');
        if (!up || (hash && up < hash))
            break;
        *up = '\0';
    }
    /* This unit's own package members outrank every import. */
    for (int i = 0; i < ctx->package_count; i++) {
        const char *owner = scala_package_owner(ctx, ctx->packages[i]);
        if (!owner)
            continue;
        scala_ref_t own = scala_package_function(ctx, owner, name, arguments, true);
        if (own.kind != SCALA_UNKNOWN)
            return own;
    }
    bool wildcard_outside = false;
    outside = false;
    scala_ref_t explicit_import =
        scala_imports_level(ctx, ctx->imports, name, arguments, false, &outside);
    if (explicit_import.kind != SCALA_UNKNOWN ||
        !strcmp(explicit_import.reason, "scala_two_imports_provide_name"))
        return explicit_import;
    if (outside) {
        ref.reason = "scala_import_outside_project";
        return ref;
    }
    scala_ref_t wildcard =
        scala_imports_level(ctx, ctx->imports, name, arguments, true, &wildcard_outside);
    if (wildcard.kind != SCALA_UNKNOWN)
        return wildcard;
    for (int i = 0; i < ctx->package_count; i++) {
        const char *owner = scala_package_owner(ctx, ctx->packages[i]);
        if (!owner)
            continue;
        scala_ref_t other = scala_package_function(ctx, owner, name, arguments, false);
        if (other.kind == SCALA_UNKNOWN)
            continue;
        /* An outside wildcard outranks a package member from another file. */
        if (wildcard_outside) {
            ref.reason = "scala_outside_import_may_provide_name";
            return ref;
        }
        return other;
    }
    /* A package's own name, for `pkg.Thing`. */
    scala_ref_t path = scala_resolve_path(ctx, name);
    if (path.kind == SCALA_PACKAGE_REF)
        return path;
    return ref;
}

/* ── Phase two: expressions ────────────────────────────────────────── */

static int scala_argument_count(TSNode call) {
    TSNode arguments = cbm_source_field(call, "arguments");
    if (ts_node_is_null(arguments))
        return -1;
    /* `f { ... }` is one argument, however many statements the block holds. */
    if (!cbm_source_is(arguments, "arguments"))
        return 1;
    return (int)ts_node_named_child_count(arguments);
}

static scala_ref_t scala_call_result(scala_ctx_t *ctx, const scala_ref_t *callee, int arguments);

static scala_ref_t scala_member_of(scala_ctx_t *ctx, const scala_ref_t *target, const char *name,
                                   int arguments) {
    scala_ref_t ref = {.reason = "scala_receiver_type_unknown"};
    if (target->kind == SCALA_PACKAGE_REF) {
        const char *owner = scala_package_owner(ctx, target->package);
        const char *nested = cbm_arena_sprintf(ctx->walk->arena, "%s.%s", target->package, name);
        scala_ref_t found =
            owner ? scala_package_function(ctx, owner, name, arguments, false) : ref;
        if (found.kind != SCALA_UNKNOWN) {
            found.via_import = true;
            return found;
        }
        return nested ? scala_resolve_path(ctx, nested) : ref;
    }
    if (target->kind == SCALA_VALUE && target->key)
        return scala_template_member(ctx, target->key, name, arguments, target->from_super);
    if (target->kind == SCALA_TEMPLATE && target->key)
        return scala_template_member(ctx, target->key, name, arguments, false);
    /* `x.m.n`: a method read without arguments is called. */
    if (target->kind == SCALA_FUNCTION && target->decl && target->decl->arity_min == 0) {
        scala_ref_t value = scala_call_result(ctx, target, 0);
        if (value.kind == SCALA_VALUE && value.key)
            return scala_template_member(ctx, value.key, name, arguments, false);
        if (value.reason)
            ref.reason = value.reason;
        return ref;
    }
    /* Why the receiver is unknown is why this member is. */
    if (target->reason)
        ref.reason = target->reason;
    return ref;
}

static scala_ref_t scala_expr(scala_ctx_t *ctx, TSNode node, int arguments, unsigned depth);

/* After a call: the value it produces. */
static scala_ref_t scala_call_result(scala_ctx_t *ctx, const scala_ref_t *callee, int arguments) {
    scala_ref_t result = {.reason = "scala_return_type_unknown"};
    if (callee->kind == SCALA_FUNCTION) {
        const char *type = callee->decl->type
                               ? scala_lookup_type(ctx, callee->decl->owner, callee->decl->type,
                                                   callee->decl->file)
                               : NULL;
        result.kind = type ? SCALA_VALUE : SCALA_UNKNOWN;
        result.key = type;
        return result;
    }
    if (callee->kind == SCALA_CLASS && callee->key) {
        result.kind = SCALA_VALUE;
        result.key = callee->key;
    } else if (callee->kind == SCALA_TEMPLATE) {
        scala_ref_t apply = callee->key
                                ? scala_template_member(ctx, callee->key, "apply", arguments, false)
                                : result;
        if (apply.kind == SCALA_FUNCTION)
            return scala_call_result(ctx, &apply, arguments);
        if (callee->companion &&
            !strcmp(apply.reason ? apply.reason : "", "scala_member_absent_from_chain")) {
            result.kind = SCALA_VALUE;
            result.key = callee->companion;
        }
    }
    return result;
}

/* `f[T]` in `f[T](x)` is still f. */
static TSNode scala_written_callee(TSNode function) {
    return cbm_source_is(function, "generic_function") ? cbm_source_field(function, "function")
                                                       : function;
}

/* `a op b` selects op on a, and `a op: b` selects it on b. */
static scala_ref_t scala_infix_member(scala_ctx_t *ctx, TSNode node, unsigned depth) {
    const char *name = cbm_source_text(ctx->walk, cbm_source_field(node, "operator"));
    if (!name || !name[0])
        return (scala_ref_t){.reason = "scala_computed_receiver"};
    bool right = name[strlen(name) - 1] == ':';
    TSNode operand = cbm_source_field(node, right ? "right" : "left");
    scala_ref_t target = scala_expr(ctx, operand, -1, depth);
    return scala_member_of(ctx, &target, name, 1);
}

static scala_ref_t scala_expr(scala_ctx_t *ctx, TSNode node, int arguments, unsigned depth) {
    scala_ref_t unknown = {.reason = "scala_computed_receiver"};
    if (ts_node_is_null(node) || depth > 64)
        return unknown;
    if (cbm_source_is(node, "identifier") || cbm_source_is(node, "operator_identifier")) {
        const char *name = cbm_source_text(ctx->walk, node);
        if (!name)
            return unknown;
        if (!strcmp(name, "this") || !strcmp(name, "super")) {
            if (!ctx->template_key)
                return unknown;
            return (scala_ref_t){
                .kind = SCALA_VALUE, .key = ctx->template_key, .from_super = name[0] == 's'};
        }
        return scala_name(ctx, name, arguments);
    }
    if (cbm_source_is(node, "field_expression")) {
        const char *member = cbm_source_text(ctx->walk, cbm_source_field(node, "field"));
        if (!member)
            return unknown;
        scala_ref_t target = scala_expr(ctx, cbm_source_field(node, "value"), -1, depth + 1);
        return scala_member_of(ctx, &target, member, arguments);
    }
    if (cbm_source_is(node, "call_expression")) {
        int count = scala_argument_count(node);
        scala_ref_t callee = scala_expr(
            ctx, scala_written_callee(cbm_source_field(node, "function")), count, depth + 1);
        return scala_call_result(ctx, &callee, count);
    }
    if (cbm_source_is(node, "infix_expression")) {
        scala_ref_t callee = scala_infix_member(ctx, node, depth + 1);
        return scala_call_result(ctx, &callee, 1);
    }
    if (cbm_source_is(node, "instance_expression")) {
        const char *type =
            scala_type_text(ctx->walk, ts_node_named_child(node, 0), ctx->type_params);
        const char *owner =
            ctx->template_key ? ctx->template_key : scala_package_owner(ctx, ctx->packages[0]);
        const char *key = type ? scala_lookup_type(ctx, owner, type, ctx->walk->file) : NULL;
        if (key)
            return (scala_ref_t){.kind = SCALA_VALUE, .key = key};
        return unknown;
    }
    if (cbm_source_is(node, "parenthesized_expression"))
        return scala_expr(ctx, ts_node_named_child(node, 0), -1, depth + 1);
    return unknown;
}

static void scala_bind(scala_ctx_t *ctx, const char *name, const char *type_key) {
    scala_local_t *local = name ? cbm_arena_alloc(ctx->walk->arena, sizeof(*local)) : NULL;
    if (!local)
        return;
    *local = (scala_local_t){name, type_key, ctx->locals};
    ctx->locals = local;
}

static void scala_emit(scala_ctx_t *ctx, TSNode site, const scala_ref_t *ref, int arguments) {
    const char *reason = ref->reason;
    const char *target = NULL;
    uint32_t facts = 0;
    scala_ref_t selected = *ref;
    if (ref->kind == SCALA_TEMPLATE) {
        /* An object called by name: its apply, or the companion class's construction. */
        scala_ref_t apply = ref->key
                                ? scala_template_member(ctx, ref->key, "apply", arguments, false)
                                : (scala_ref_t){0};
        if (apply.kind == SCALA_FUNCTION) {
            selected = apply;
            selected.via_import = ref->via_import;
        } else if (ref->companion && ref->decl &&
                   !strcmp(apply.reason ? apply.reason : "", "scala_member_absent_from_chain")) {
            /* An object whose chain leaves the project may inherit an apply. */
            selected.kind = SCALA_CLASS;
        }
    }
    if ((selected.kind == SCALA_FUNCTION || selected.kind == SCALA_CLASS) && selected.decl) {
        const char *identity = NULL;
        cbm_source_decl_t single = *selected.decl;
        single.next = NULL; /* The selection already told overloads apart. */
        target = cbm_source_unique_qn(&single, &identity);
        if (!target)
            reason = identity;
        if (selected.kind == SCALA_CLASS)
            facts = CBM_RESOLVE_FACT_CLASS_CALLABLE;
    }
    cbm_resolve_rule_t rule = selected.via_receiver ? CBM_RESOLVE_RULE_TYPE_MEMBER
                              : selected.via_import ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                                    : CBM_RESOLVE_RULE_SCOPE_BINDING;
    cbm_source_emit_facts(ctx->walk, ts_node_start_byte(site), ts_node_end_byte(site),
                          CBM_SOURCE_SITE_CALL, target, rule,
                          selected.decl ? selected.decl->owner : NULL, reason, facts);
}

/* A name read as a value or a type follows the same lookup as a call's. A template
 * reached by a type name is the class; by a term, the object. */
static void scala_emit_reference(scala_ctx_t *ctx, TSNode node, const scala_ref_t *ref,
                                 bool type_position) {
    const cbm_source_decl_t *decl = NULL;
    if (ref->kind == SCALA_FUNCTION && !type_position)
        decl = ref->decl;
    else if (ref->kind == SCALA_CLASS)
        decl = ref->decl;
    else if (ref->kind == SCALA_TEMPLATE)
        decl = type_position ? ref->decl : ref->object_decl;
    else if (ref->value_decl && !type_position)
        decl = ref->value_decl;
    if (!decl)
        return;
    const char *identity = NULL;
    cbm_source_decl_t single = *decl;
    single.next = NULL;
    const char *target = cbm_source_unique_qn(&single, &identity);
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, target,
                         ref->via_import     ? CBM_RESOLVE_RULE_IMPORT_BINDING
                         : ref->via_receiver ? CBM_RESOLVE_RULE_TYPE_MEMBER
                                             : CBM_RESOLVE_RULE_SCOPE_BINDING,
                         decl->owner, target ? ref->reason : identity);
}

static void scala_visit_reference(scala_ctx_t *ctx, TSNode node, bool type_position) {
    const char *name = cbm_source_text(ctx->walk, node);
    if (!name || !strcmp(name, "this") || !strcmp(name, "super"))
        return;
    scala_ref_t ref = scala_name(ctx, name, -1);
    scala_emit_reference(ctx, node, &ref, type_position);
}

/* `x.name` read as a value, when it is no call's function. */
static void scala_visit_selection(scala_ctx_t *ctx, TSNode node) {
    TSNode field = cbm_source_field(node, "field");
    const char *name = cbm_source_text(ctx->walk, field);
    if (!name)
        return;
    scala_ref_t target = scala_expr(ctx, cbm_source_field(node, "value"), -1, 0);
    scala_ref_t ref = scala_member_of(ctx, &target, name, -1);
    scala_emit_reference(ctx, field, &ref, false);
}

/* `a.b.Foo` in a type: the path names it, never a Foo in scope here. */
static void scala_visit_type_path(scala_ctx_t *ctx, TSNode node) {
    const char *text = scala_type_text(ctx->walk, node, ctx->type_params);
    uint32_t count = ts_node_named_child_count(node);
    if (!text || !count)
        return;
    scala_ref_t ref = scala_resolve_path(ctx, text);
    /* A mention sits on the last name, a type use on the whole path. */
    scala_emit_reference(ctx, ts_node_named_child(node, count - 1), &ref, true);
    scala_emit_reference(ctx, node, &ref, true);
}

/* ── Phase two: the walk ───────────────────────────────────────────── */

static void scala_visit(scala_ctx_t *ctx, TSNode node, unsigned depth);

static void scala_visit_children(scala_ctx_t *ctx, TSNode node, unsigned depth) {
    bool in_block = cbm_source_is(node, "block");
    bool in_template = cbm_source_is(node, "template_body");
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            /* A field's name and a named argument's are spelled, not read. */
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (cbm_source_is(child, "identifier") && field &&
                (!strcmp(field, "field") || !strcmp(field, "name") || !strcmp(field, "operator") ||
                 (!strcmp(field, "left") && cbm_source_is(node, "assignment_expression"))))
                continue;
            ctx->child_of_block = in_block;
            ctx->child_of_template = in_template;
            scala_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static const char *scala_type_key_here(scala_ctx_t *ctx, TSNode type_node);

static void scala_bind_pattern(scala_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        scala_bind(ctx, cbm_source_text(ctx->walk, node), NULL);
        return;
    }
    /* `case e: Foo` and `(y: Foo) =>` say the type of what they bind. */
    if (cbm_source_is(node, "typed_pattern") || cbm_source_is(node, "binding")) {
        TSNode name = ts_node_named_child(node, 0);
        if (cbm_source_is(name, "identifier")) {
            scala_bind(ctx, cbm_source_text(ctx->walk, name),
                       scala_type_key_here(ctx, ts_node_named_child(node, 1)));
            return;
        }
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        scala_bind_pattern(ctx, ts_node_named_child(node, i), depth + 1);
}

static const char *scala_type_key_here(scala_ctx_t *ctx, TSNode type_node) {
    const char *type = scala_type_text(ctx->walk, type_node, ctx->type_params);
    const char *owner =
        ctx->template_key ? ctx->template_key : scala_package_owner(ctx, ctx->packages[0]);
    return type ? scala_lookup_type(ctx, owner, type, ctx->walk->file) : NULL;
}

/* One parameter list: its types and defaults are read before its names come into scope,
 * which the lists after it, and the body, see. A class's parameters go the same way. */
static void scala_visit_parameter_list(scala_ctx_t *ctx, TSNode list, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(list);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode param = ts_tree_cursor_current_node(&cursor);
            if (!cbm_source_is(param, "parameter") && !cbm_source_is(param, "class_parameter"))
                continue;
            TSNode type = cbm_source_field(param, "type");
            if (!ts_node_is_null(type))
                scala_visit(ctx, type, depth + 1);
            TSNode value = cbm_source_field(param, "default_value");
            if (!ts_node_is_null(value))
                scala_visit(ctx, value, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_reset(&cursor, list);
        (void)ts_tree_cursor_goto_first_child(&cursor);
        do {
            TSNode param = ts_tree_cursor_current_node(&cursor);
            if (cbm_source_is(param, "parameter") || cbm_source_is(param, "class_parameter"))
                scala_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(param, "name")),
                           scala_type_key_here(ctx, cbm_source_field(param, "type")));
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void scala_add_import(scala_ctx_t *ctx, TSNode node, scala_import_t **list) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *path = NULL;
    TSNode selectors = (TSNode){0};
    bool wildcard = false;
    uint32_t count = ts_node_named_child_count(node);
    uint32_t identifiers = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "identifier")) {
            const char *text = cbm_source_text(walk, child);
            identifiers++;
            path = path ? cbm_arena_sprintf(walk->arena, "%s.%s", path, text) : text;
        } else if (cbm_source_is(child, "namespace_selectors")) {
            selectors = child;
        } else if (cbm_source_is(child, "namespace_wildcard")) {
            wildcard = true;
        }
    }
    if (!path)
        return;
    if (wildcard) {
        scala_import_t *import = cbm_arena_calloc(walk->arena, sizeof(*import));
        if (import) {
            *import = (scala_import_t){path, NULL, NULL, *list};
            *list = import;
        }
        return;
    }
    if (!ts_node_is_null(selectors)) {
        uint32_t selected = ts_node_named_child_count(selectors);
        for (uint32_t i = 0; i < selected; i++) {
            TSNode one = ts_node_named_child(selectors, i);
            const char *member = NULL, *alias = NULL;
            if (cbm_source_is(one, "identifier")) {
                member = alias = cbm_source_text(walk, one);
            } else if (cbm_source_is(one, "arrow_renamed_identifier") ||
                       cbm_source_is(one, "as_renamed_identifier")) {
                member = cbm_source_text(walk, cbm_source_field(one, "name"));
                alias = cbm_source_text(walk, cbm_source_field(one, "alias"));
            } else if (cbm_source_is(one, "namespace_wildcard")) {
                scala_import_t *import = cbm_arena_calloc(walk->arena, sizeof(*import));
                if (import) {
                    *import = (scala_import_t){path, NULL, NULL, *list};
                    *list = import;
                }
                continue;
            }
            if (!member || !alias || !strcmp(alias, "_"))
                continue;
            scala_import_t *import = cbm_arena_calloc(walk->arena, sizeof(*import));
            if (import) {
                *import = (scala_import_t){path, member, alias, *list};
                *list = import;
            }
        }
        return;
    }
    /* `import a.b.C`: the last segment is the member. */
    const char *dot = identifiers > 1 ? strrchr(path, '.') : NULL;
    scala_import_t *import = cbm_arena_calloc(walk->arena, sizeof(*import));
    if (import && dot) {
        const char *prefix = cbm_arena_strndup(walk->arena, path, (size_t)(dot - path));
        *import = (scala_import_t){prefix, dot + 1, dot + 1, *list};
        *list = import;
    }
}

static void scala_visit_function(scala_ctx_t *ctx, TSNode node, unsigned depth, bool local) {
    if (local)
        scala_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(node, "name")), NULL);
    scala_local_t *saved = ctx->locals;
    scala_import_t *saved_imports = ctx->block_imports;
    const char *saved_params = ctx->type_params;
    ctx->type_params = scala_type_params(ctx->walk, node, ctx->type_params);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode list = ts_tree_cursor_current_node(&cursor);
            if (cbm_source_is(list, "parameters"))
                scala_visit_parameter_list(ctx, list, depth);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    TSNode returned = cbm_source_field(node, "return_type");
    if (!ts_node_is_null(returned))
        scala_visit(ctx, returned, depth + 1);
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(body))
        scala_visit(ctx, body, depth + 1);
    ctx->locals = saved;
    ctx->block_imports = saved_imports;
    ctx->type_params = saved_params;
}

static void scala_visit_template(scala_ctx_t *ctx, TSNode node, unsigned depth) {
    const char *name = cbm_source_text(ctx->walk, cbm_source_field(node, "name"));
    if (!name)
        return;
    bool object = cbm_source_is(node, "object_definition");
    const char *saved = ctx->template_key;
    const char *saved_params = ctx->type_params;
    scala_local_t *saved_locals = ctx->locals;
    scala_import_t *saved_imports = ctx->block_imports;
    ctx->type_params = scala_type_params(ctx->walk, node, NULL);
    /* A class's parameters and its parents' arguments are read outside its members. */
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode part = ts_tree_cursor_current_node(&cursor);
            if (cbm_source_is(part, "class_parameters"))
                scala_visit_parameter_list(ctx, part, depth);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    TSNode extend = cbm_source_field(node, "extend");
    if (!ts_node_is_null(extend))
        scala_visit_children(ctx, extend, depth + 1);
    ctx->template_key =
        saved ? cbm_arena_sprintf(ctx->walk->arena, "%s.%s%s", saved, object ? "$" : "", name)
              : cbm_arena_sprintf(ctx->walk->arena, "t:%s#%s%s", ctx->packages[0],
                                  object ? "$" : "", name);
    ctx->locals = NULL;
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(body))
        scala_visit_children(ctx, body, depth + 1);
    ctx->template_key = saved;
    ctx->type_params = saved_params;
    ctx->locals = saved_locals;
    ctx->block_imports = saved_imports;
}

static void scala_visit(scala_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "identifier") || !strcmp(kind, "type_identifier")) {
        scala_visit_reference(ctx, node, kind[0] == 't');
        return;
    }
    if (!strcmp(kind, "package_clause") || !strcmp(kind, "modifiers") ||
        !strcmp(kind, "annotation"))
        return;
    if (scala_template_kind(node)) {
        scala_visit_template(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "function_definition")) {
        /* A member's name is declared in the template; a local one binds here. */
        scala_visit_function(ctx, node, depth, ctx->child_of_block);
        return;
    }
    if (!strcmp(kind, "val_definition") || !strcmp(kind, "var_definition")) {
        bool in_block = ctx->child_of_block;
        TSNode value = cbm_source_field(node, "value");
        TSNode pattern = cbm_source_field(node, "pattern");
        TSNode type = cbm_source_field(node, "type");
        const char *key = ts_node_is_null(type) ? NULL : scala_type_key_here(ctx, type);
        if (ts_node_is_null(type) && !ts_node_is_null(value)) {
            scala_ref_t inferred = scala_expr(ctx, value, -1, 0);
            if (inferred.kind == SCALA_VALUE)
                key = inferred.key;
        }
        if (!ts_node_is_null(value))
            scala_visit(ctx, value, depth + 1);
        if (ctx->template_key && !in_block)
            return; /* A template's own member, declared in the table. */
        if (cbm_source_is(pattern, "identifier"))
            scala_bind(ctx, cbm_source_text(ctx->walk, pattern), key);
        else
            scala_bind_pattern(ctx, pattern, depth + 1);
        return;
    }
    if (!strcmp(kind, "call_expression")) {
        TSNode function = scala_written_callee(cbm_source_field(node, "function"));
        if (cbm_source_is(function, "identifier") || cbm_source_is(function, "field_expression")) {
            int count = scala_argument_count(node);
            scala_ref_t ref = scala_expr(ctx, function, count, 0);
            scala_emit(ctx, node, &ref, count);
        }
        TSNode saved = ctx->callee;
        ctx->callee = function;
        scala_visit_children(ctx, node, depth);
        ctx->callee = saved;
        return;
    }
    if (!strcmp(kind, "infix_expression")) {
        scala_ref_t ref = scala_infix_member(ctx, node, 0);
        scala_emit(ctx, node, &ref, 1);
        scala_visit_children(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "field_expression") &&
        (ts_node_is_null(ctx->callee) || !ts_node_eq(node, ctx->callee))) {
        scala_visit_selection(ctx, node);
        scala_visit_children(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "stable_type_identifier")) {
        scala_visit_type_path(ctx, node);
        return;
    }
    if (!strcmp(kind, "instance_expression")) {
        /* `new Foo(...)` constructs the class, whatever the term namespace holds. */
        const char *type =
            scala_type_text(ctx->walk, ts_node_named_child(node, 0), ctx->type_params);
        if (type) {
            scala_ref_t ref =
                strchr(type, '.') ? scala_resolve_path(ctx, type) : scala_name(ctx, type, -1);
            if (ref.kind == SCALA_TEMPLATE && ref.decl)
                ref.kind = SCALA_CLASS;
            if (ref.kind != SCALA_CLASS)
                ref = (scala_ref_t){.reason = "scala_class_not_in_project"};
            scala_emit(ctx, node, &ref, -1);
        }
        TSNode body = scala_child_of_kind(node, "template_body");
        if (ts_node_is_null(body)) {
            scala_visit_children(ctx, node, depth);
            return;
        }
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode part = ts_tree_cursor_current_node(&cursor);
                if (ts_node_is_named(part) && !ts_node_eq(part, body))
                    scala_visit(ctx, part, depth + 1);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        scala_anonymous_t anonymous = {.next = ctx->anonymous};
        anonymous.base = scala_type_key_here(ctx, ts_node_named_child(node, 0));
        scala_local_t *saved = ctx->locals;
        ctx->anonymous = &anonymous;
        /* Its own members are in no table, and hide what is around it. */
        cursor = ts_tree_cursor_new(body);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode member = ts_tree_cursor_current_node(&cursor);
                if (cbm_source_is(member, "function_definition"))
                    scala_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(member, "name")),
                               NULL);
                else if (cbm_source_is(member, "val_definition") ||
                         cbm_source_is(member, "var_definition"))
                    scala_bind_pattern(ctx, cbm_source_field(member, "pattern"), depth + 1);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
        scala_visit_children(ctx, body, depth + 1);
        ctx->anonymous = anonymous.next;
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "lambda_expression")) {
        scala_local_t *saved = ctx->locals;
        TSNode parameters = cbm_source_field(node, "parameters");
        if (!ts_node_is_null(parameters))
            scala_bind_pattern(ctx, parameters, depth + 1);
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (!ts_node_eq(child, parameters))
                scala_visit(ctx, child, depth + 1);
        }
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "case_clause")) {
        scala_local_t *saved = ctx->locals;
        scala_bind_pattern(ctx, cbm_source_field(node, "pattern"), depth + 1);
        TSNode guard = scala_child_of_kind(node, "guard");
        if (!ts_node_is_null(guard))
            scala_visit(ctx, guard, depth + 1);
        TSNode body = cbm_source_field(node, "body");
        if (!ts_node_is_null(body))
            scala_visit(ctx, body, depth + 1);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "for_expression")) {
        scala_local_t *saved = ctx->locals;
        TSNode enumerators = scala_child_of_kind(node, "enumerators");
        uint32_t count = ts_node_is_null(enumerators) ? 0 : ts_node_named_child_count(enumerators);
        for (uint32_t i = 0; i < count; i++) {
            TSNode enumerator = ts_node_named_child(enumerators, i);
            uint32_t parts = ts_node_named_child_count(enumerator);
            /* The generator reads what is outside; its guards see what it binds. */
            if (parts > 1)
                scala_visit(ctx, ts_node_named_child(enumerator, 1), depth + 1);
            if (parts > 0)
                scala_bind_pattern(ctx, ts_node_named_child(enumerator, 0), depth + 1);
            for (uint32_t p = 2; p < parts; p++)
                scala_visit(ctx, ts_node_named_child(enumerator, p), depth + 1);
        }
        TSNode body = cbm_source_field(node, "body");
        if (!ts_node_is_null(body))
            scala_visit(ctx, body, depth + 1);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "import_declaration")) {
        if (ctx->child_of_block || ctx->child_of_template)
            scala_add_import(ctx, node, &ctx->block_imports);
        return;
    }
    if (!strcmp(kind, "block")) {
        scala_local_t *saved = ctx->locals;
        scala_import_t *saved_imports = ctx->block_imports;
        scala_visit_children(ctx, node, depth);
        ctx->locals = saved;
        ctx->block_imports = saved_imports;
        return;
    }
    scala_visit_children(ctx, node, depth);
}

static void scala_resolve(cbm_source_walk_t *walk) {
    scala_ctx_t ctx = {.walk = walk, .type_memo = cbm_ht_create(64)};
    ctx.package_count = scala_file_packages(walk, ctx.packages);
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (cbm_source_is(child, "import_declaration"))
            scala_add_import(&ctx, child, &ctx.imports);
    }
    scala_visit_children(&ctx, walk->root, 0);
    cbm_ht_free(ctx.type_memo);
}

const cbm_source_adapter_t cbm_source_adapter_scala = {.language = CBM_LANG_SCALA,
                                                       .collect = scala_collect,
                                                       .resolve = scala_resolve,
                                                       .one_name_one_node = true,
                                                       .decides_references = true,
                                                       .contains_parse_damage = true};
