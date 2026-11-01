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
 * Known gaps, each abstaining: inferred return types, type members and path-dependent
 * types, generic element types, infix and postfix calls, and calls to Java classes. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>

#define SCALA_RECORD "&template"
#define SCALA_PACKAGE "&package"
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

typedef struct {
    cbm_source_walk_t *walk;
    const char *packages[SCALA_MAX_PACKAGES]; /* innermost first */
    int package_count;
    const char *template_key; /* enclosing template, or NULL */
    const char *type_params;
    scala_local_t *locals;
    scala_import_t *imports;       /* the file's */
    scala_import_t *block_imports; /* in the blocks around this point */
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
    TSNode parameters = cbm_source_field(function, "parameters");
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

static void scala_collect_value(cbm_source_walk_t *walk, const char *owner, TSNode node,
                                const char *type_params) {
    TSNode pattern = cbm_source_field(node, "pattern");
    if (!cbm_source_is(pattern, "identifier"))
        return;
    const char *type = scala_type_text(walk, cbm_source_field(node, "type"), type_params);
    if (ts_node_is_null(cbm_source_field(node, "type"))) {
        TSNode value = cbm_source_field(node, "value");
        /* `val x = new Foo(...)`: the type it constructs. */
        if (cbm_source_is(value, "instance_expression"))
            type = scala_type_text(walk, ts_node_named_child(value, 0), type_params);
    }
    cbm_source_decl_t *decl = cbm_source_declare(walk, owner, cbm_source_text(walk, pattern), NULL,
                                                 CBM_SOURCE_DECL_F_VALUE | scala_flags(walk, node),
                                                 ts_node_start_point(node).row + 1);
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
    const char *type = scala_type_text(walk, cbm_source_field(node, "return_type"),
                                       scala_type_params(walk, node, type_params));
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
        cbm_source_decl_t *decl =
            name ? cbm_source_declare(walk, key, name, NULL, CBM_SOURCE_DECL_F_VALUE,
                                      ts_node_start_point(param).row + 1)
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

static void scala_collect(cbm_source_walk_t *walk) {
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
static const char *scala_lookup_type(scala_ctx_t *ctx, const char *owner, const char *text);

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
        const char *parent = scala_lookup_type(ctx, key, record->bases[step - 1]);
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

/* Imports a declaring file made are not in this table; the current file's are. For a
 * type named in another file, its own package and the enclosing templates decide, and
 * a name that only that file's import would reach stays unknown. */
static const char *scala_lookup_type(scala_ctx_t *ctx, const char *owner, const char *text) {
    if (!owner || !text)
        return NULL;
    cbm_source_walk_t *walk = ctx->walk;
    const char *dot = strrchr(text, '.');
    if (dot) {
        scala_ref_t path = scala_resolve_path(ctx, text);
        return scala_class_key_of(&path);
    }
    /* Templates from inside out. */
    char *scope = !strncmp(owner, "t:", 2) ? cbm_arena_strdup(walk->arena, owner) : NULL;
    while (scope) {
        scala_ref_t nested = scala_member_names(ctx, scope, text);
        const char *key = scala_class_key_of(&nested);
        if (key)
            return key;
        char *up = strrchr(scope, '.');
        char *hash = strchr(scope, '#');
        if (!up || (hash && up < hash))
            break;
        *up = '\0';
    }
    const char *package = scala_key_package(walk, owner);
    /* From this file: its imports come before other files of the package. */
    bool here = false;
    for (int i = 0; i < ctx->package_count; i++)
        here |= !strcmp(ctx->packages[i], package);
    if (here) {
        for (const scala_import_t *import = ctx->imports; import; import = import->next) {
            if (!import->member || strcmp(import->alias, text))
                continue;
            scala_ref_t path = scala_resolve_path(
                ctx, cbm_arena_sprintf(walk->arena, "%s.%s", import->path, import->member));
            const char *key = scala_class_key_of(&path);
            if (key)
                return key;
        }
    }
    const char *package_owner = scala_package_owner(ctx, package);
    scala_ref_t own =
        package_owner ? scala_member_names(ctx, package_owner, text) : (scala_ref_t){0};
    const char *key = scala_class_key_of(&own);
    if (key)
        return key;
    if (here) {
        for (const scala_import_t *import = ctx->imports; import; import = import->next) {
            if (import->member)
                continue;
            scala_ref_t from = scala_resolve_path(ctx, import->path);
            if (from.kind != SCALA_PACKAGE_REF)
                continue;
            scala_ref_t found =
                scala_member_names(ctx, scala_package_owner(ctx, from.package), text);
            key = scala_class_key_of(&found);
            if (key)
                return key;
        }
    }
    return NULL;
}

static scala_ref_t scala_ref_of_member(scala_ctx_t *ctx, const cbm_source_decl_t *decl) {
    scala_ref_t ref = {.reason = "scala_name_not_in_project"};
    if (decl->flags & CBM_SOURCE_DECL_F_VALUE) {
        const char *type = decl->type ? scala_lookup_type(ctx, decl->owner, decl->type) : NULL;
        ref.kind = type ? SCALA_VALUE : SCALA_UNKNOWN;
        ref.key = type;
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
    return ref;
}

static scala_ref_t scala_expr(scala_ctx_t *ctx, TSNode node, int arguments, unsigned depth);

/* After a call: the value it produces. */
static scala_ref_t scala_call_result(scala_ctx_t *ctx, const scala_ref_t *callee, int arguments) {
    scala_ref_t result = {.reason = "scala_return_type_unknown"};
    if (callee->kind == SCALA_FUNCTION) {
        const char *type = callee->decl->type
                               ? scala_lookup_type(ctx, callee->decl->owner, callee->decl->type)
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
        scala_ref_t callee = scala_expr(ctx, cbm_source_field(node, "function"), count, depth + 1);
        return scala_call_result(ctx, &callee, count);
    }
    if (cbm_source_is(node, "instance_expression")) {
        const char *type =
            scala_type_text(ctx->walk, ts_node_named_child(node, 0), ctx->type_params);
        const char *owner =
            ctx->template_key ? ctx->template_key : scala_package_owner(ctx, ctx->packages[0]);
        const char *key = type ? scala_lookup_type(ctx, owner, type) : NULL;
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
static void scala_visit_reference(scala_ctx_t *ctx, TSNode node, bool type_position) {
    const char *name = cbm_source_text(ctx->walk, node);
    if (!name || !strcmp(name, "this") || !strcmp(name, "super"))
        return;
    scala_ref_t ref = scala_name(ctx, name, -1);
    const cbm_source_decl_t *decl = NULL;
    if (ref.kind == SCALA_FUNCTION && !type_position)
        decl = ref.decl;
    else if (ref.kind == SCALA_CLASS)
        decl = ref.decl;
    else if (ref.kind == SCALA_TEMPLATE)
        decl = type_position ? ref.decl : ref.object_decl;
    if (!decl)
        return;
    const char *identity = NULL;
    cbm_source_decl_t single = *decl;
    single.next = NULL;
    const char *target = cbm_source_unique_qn(&single, &identity);
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, target,
                         ref.via_import ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                        : CBM_RESOLVE_RULE_SCOPE_BINDING,
                         decl->owner, target ? ref.reason : identity);
}

/* ── Phase two: the walk ───────────────────────────────────────────── */

static void scala_visit(scala_ctx_t *ctx, TSNode node, unsigned depth);

static void scala_visit_children(scala_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            /* A field's name and a named argument's are spelled, not read. */
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (cbm_source_is(child, "identifier") && field &&
                (!strcmp(field, "field") || !strcmp(field, "left") || !strcmp(field, "name")))
                continue;
            scala_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static void scala_bind_pattern(scala_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        scala_bind(ctx, cbm_source_text(ctx->walk, node), NULL);
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        scala_bind_pattern(ctx, ts_node_named_child(node, i), depth + 1);
}

static const char *scala_type_key_here(scala_ctx_t *ctx, TSNode type_node) {
    const char *type = scala_type_text(ctx->walk, type_node, ctx->type_params);
    const char *owner =
        ctx->template_key ? ctx->template_key : scala_package_owner(ctx, ctx->packages[0]);
    return type ? scala_lookup_type(ctx, owner, type) : NULL;
}

static void scala_bind_parameters(scala_ctx_t *ctx, TSNode function) {
    uint32_t count = ts_node_named_child_count(function);
    for (uint32_t i = 0; i < count; i++) {
        TSNode list = ts_node_named_child(function, i);
        if (!cbm_source_is(list, "parameters"))
            continue;
        uint32_t params = ts_node_named_child_count(list);
        for (uint32_t p = 0; p < params; p++) {
            TSNode param = ts_node_named_child(list, p);
            if (!cbm_source_is(param, "parameter"))
                continue;
            scala_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(param, "name")),
                       scala_type_key_here(ctx, cbm_source_field(param, "type")));
        }
    }
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
    uint32_t parts = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < parts; i++) {
        TSNode list = ts_node_named_child(node, i);
        uint32_t params = cbm_source_is(list, "parameters") ? ts_node_named_child_count(list) : 0;
        for (uint32_t p = 0; p < params; p++) {
            TSNode type = cbm_source_field(ts_node_named_child(list, p), "type");
            if (!ts_node_is_null(type))
                scala_visit(ctx, type, depth + 1);
        }
    }
    TSNode returned = cbm_source_field(node, "return_type");
    if (!ts_node_is_null(returned))
        scala_visit(ctx, returned, depth + 1);
    scala_bind_parameters(ctx, node);
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
    ctx->template_key =
        saved ? cbm_arena_sprintf(ctx->walk->arena, "%s.%s%s", saved, object ? "$" : "", name)
              : cbm_arena_sprintf(ctx->walk->arena, "t:%s#%s%s", ctx->packages[0],
                                  object ? "$" : "", name);
    ctx->type_params = scala_type_params(ctx->walk, node, NULL);
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
        TSNode parent_is_body = ts_node_parent(node);
        scala_visit_function(ctx, node, depth, cbm_source_is(parent_is_body, "block"));
        return;
    }
    if (!strcmp(kind, "val_definition") || !strcmp(kind, "var_definition")) {
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
        if (ctx->template_key && !cbm_source_is(ts_node_parent(node), "block"))
            return; /* A template's own member, declared in the table. */
        if (cbm_source_is(pattern, "identifier"))
            scala_bind(ctx, cbm_source_text(ctx->walk, pattern), key);
        else
            scala_bind_pattern(ctx, pattern, depth + 1);
        return;
    }
    if (!strcmp(kind, "call_expression")) {
        TSNode function = cbm_source_field(node, "function");
        if (cbm_source_is(function, "identifier") || cbm_source_is(function, "field_expression")) {
            int count = scala_argument_count(node);
            scala_ref_t ref = scala_expr(ctx, function, count, 0);
            scala_emit(ctx, node, &ref, count);
        }
        scala_visit_children(ctx, node, depth);
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
        scala_visit_children(ctx, node, depth);
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
        TSNode enumerators = cbm_source_field(node, "enumerators");
        uint32_t count = ts_node_is_null(enumerators) ? 0 : ts_node_named_child_count(enumerators);
        for (uint32_t i = 0; i < count; i++) {
            TSNode enumerator = ts_node_named_child(enumerators, i);
            uint32_t parts = ts_node_named_child_count(enumerator);
            for (uint32_t p = 1; p < parts; p++)
                scala_visit(ctx, ts_node_named_child(enumerator, p), depth + 1);
            if (parts > 0)
                scala_bind_pattern(ctx, ts_node_named_child(enumerator, 0), depth + 1);
        }
        TSNode body = cbm_source_field(node, "body");
        if (!ts_node_is_null(body))
            scala_visit(ctx, body, depth + 1);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "import_declaration")) {
        if (cbm_source_is(ts_node_parent(node), "block") ||
            cbm_source_is(ts_node_parent(node), "template_body"))
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
    scala_ctx_t ctx = {.walk = walk};
    ctx.package_count = scala_file_packages(walk, ctx.packages);
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (cbm_source_is(child, "import_declaration"))
            scala_add_import(&ctx, child, &ctx.imports);
    }
    scala_visit_children(&ctx, walk->root, 0);
}

const cbm_source_adapter_t cbm_source_adapter_scala = {.language = CBM_LANG_SCALA,
                                                       .collect = scala_collect,
                                                       .resolve = scala_resolve,
                                                       .one_name_one_node = true,
                                                       .decides_references = true};
