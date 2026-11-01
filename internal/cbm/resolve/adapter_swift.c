/* Swift binding rules.
 *
 * Every file of a build target sees the target's internal declarations without an
 * import, so which files share a module is a build fact. SwiftPM states it: each
 * target in Package.swift owns a directory. A file under no known target has no known
 * module, and a declaration in another such file is not visible as far as this table
 * can prove, so only its own file decides for it.
 *
 * An unqualified name is looked up in the local scopes, then in the members of the
 * enclosing types from innermost out, with their extensions and what they inherit,
 * then in the module, then in the imported modules' public declarations (internal
 * ones too under @testable). The module shadows its imports. A type whose inheritance
 * leaves the project may have any member, so nothing beyond its own members is
 * decided for code inside it.
 *
 * `x.m()` selects m on x's static type: a declared parameter, property or local, a
 * constructor call, or a declared return type. A class's own member wins over an
 * inherited one. Overloads are told apart by argument labels and arity as the compiler
 * does first; overloads that share labels share one graph node, so the choice between
 * them does not change the edge.
 *
 * `Foo(...)` calls an initializer. The graph has no node for one, so the selected
 * declaration is the type, and the evidence says it is a class call.
 *
 * Known gaps, each abstaining: Xcode projects' module membership, implicit member
 * expressions (`.foo()`), element types of collections and generics, optional
 * chaining through a function result's generic type, key paths, macros, and results
 * of overload resolution that depend on argument types. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>

#define SWIFT_TYPE_RECORD "&type"
#define SWIFT_IMPORTS "&imports"
#define SWIFT_TYPE_NAME "type:"
#define SWIFT_DIR_KEY "@cbm:raw:swift-dir:"
#define SWIFT_TARGET_KEY "@cbm:raw:swift-target:"
#define SWIFT_AMBIGUOUS_PREFIX "@cbm:"

enum { SWIFT_MAX_LABELS = 32, SWIFT_MAX_BASES = 32 };

typedef struct {
    const char *labels[SWIFT_MAX_LABELS]; /* NULL: an unlabelled argument */
    int count;
    int trailing;
    bool too_many;
} swift_args_t;

typedef struct swift_local {
    const char *name;
    const char *type_key; /* NULL: unknown type */
    struct swift_local *next;
} swift_local_t;

typedef struct {
    const char *module; /* directory of the target */
    bool testable;
} swift_import_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *module;      /* this file's module, "" when unknown */
    const char *type_key;    /* enclosing type, or NULL */
    const char *type_params; /* ",T,U," in scope, or NULL */
    swift_local_t *locals;
    bool in_function;
    swift_import_t *imports;
    size_t import_count;
    CBMHashTable *module_imports;
} swift_ctx_t;

typedef enum { SWIFT_UNKNOWN, SWIFT_VALUE, SWIFT_TYPE, SWIFT_MODULE, SWIFT_FUNCTION } swift_kind_t;

typedef struct {
    swift_kind_t kind;
    const char *type_key;          /* SWIFT_VALUE, SWIFT_TYPE */
    const char *module;            /* SWIFT_MODULE */
    const cbm_source_decl_t *decl; /* SWIFT_FUNCTION, SWIFT_TYPE */
    bool via_import, via_receiver, from_super;
    const char *reason;
} swift_ref_t;

/* ── Modules ───────────────────────────────────────────────────────── */

static const char *swift_package_value(cbm_source_walk_t *walk, const char *prefix,
                                       const char *name) {
    const cbm_source_table_t *table = walk->table ? walk->table : walk->writable;
    const char *key = cbm_arena_sprintf(walk->arena, "%s%s", prefix, name);
    const char *value = key ? cbm_source_table_package(table, key) : NULL;
    return value && strncmp(value, SWIFT_AMBIGUOUS_PREFIX, strlen(SWIFT_AMBIGUOUS_PREFIX)) ? value
                                                                                           : NULL;
}

/* The directory of the target that owns the file, or "" when no target does. */
static const char *swift_module_of(cbm_source_walk_t *walk, const char *file) {
    const char *slash = file ? strrchr(file, '/') : NULL;
    char *dir = slash ? cbm_arena_strndup(walk->arena, file, (size_t)(slash - file)) : NULL;
    while (dir) {
        if (swift_package_value(walk, SWIFT_DIR_KEY, dir))
            return dir;
        char *up = strrchr(dir, '/');
        if (!up)
            break;
        *up = '\0';
    }
    return "";
}

static const char *swift_module_owner(cbm_source_walk_t *walk, const char *module) {
    return cbm_arena_sprintf(walk->arena, "m:%s", module);
}

static const char *swift_nested_key(cbm_source_walk_t *walk, const char *module, const char *outer,
                                    const char *name) {
    return outer ? cbm_arena_sprintf(walk->arena, "%s.%s", outer, name)
                 : cbm_arena_sprintf(walk->arena, "t:%s#%s", module, name);
}

/* ── Syntax ────────────────────────────────────────────────────────── */

static TSNode swift_child_of_kind(TSNode node, const char *kind) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), kind))
            return ts_node_named_child(node, i);
    return (TSNode){0};
}

/* The written name of a type this table could own: `A.B`, `A<T>`, `A?`, `some A`. */
static const char *swift_type_text(cbm_source_walk_t *walk, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 6)
        return NULL;
    if (cbm_source_is(node, "user_type")) {
        const char *joined = NULL;
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode part = ts_node_named_child(node, i);
            if (!cbm_source_is(part, "type_identifier"))
                continue;
            const char *text = cbm_source_text(walk, part);
            if (!text)
                return NULL;
            joined = joined ? cbm_arena_sprintf(walk->arena, "%s.%s", joined, text) : text;
        }
        return joined;
    }
    if (cbm_source_is(node, "type_annotation"))
        return swift_type_text(walk, cbm_source_field(node, "name"), depth + 1);
    if (cbm_source_is(node, "optional_type"))
        return swift_type_text(walk, cbm_source_field(node, "wrapped"), depth + 1);
    if (cbm_source_is(node, "opaque_type") || cbm_source_is(node, "existential_type"))
        return swift_type_text(walk, ts_node_named_child(node, 0), depth + 1);
    return NULL;
}

static bool swift_type_kind(TSNode node) {
    return cbm_source_is(node, "user_type") || cbm_source_is(node, "optional_type") ||
           cbm_source_is(node, "opaque_type") || cbm_source_is(node, "existential_type") ||
           cbm_source_is(node, "array_type") || cbm_source_is(node, "dictionary_type") ||
           cbm_source_is(node, "function_type") || cbm_source_is(node, "tuple_type");
}

/* A function's return type is the last type-kind child before its body. */
static const char *swift_return_type(cbm_source_walk_t *walk, TSNode function) {
    const char *type = NULL;
    uint32_t count = ts_node_named_child_count(function);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(function, i);
        if (swift_type_kind(child))
            type = swift_type_text(walk, child, 0);
    }
    return type;
}

static const char *swift_type_params(cbm_source_walk_t *walk, TSNode owner, const char *outer) {
    TSNode params = swift_child_of_kind(owner, "type_parameters");
    if (ts_node_is_null(params))
        return outer;
    char *joined =
        outer ? cbm_arena_strdup(walk->arena, outer) : cbm_arena_strdup(walk->arena, ",");
    uint32_t count = ts_node_named_child_count(params);
    for (uint32_t i = 0; joined && i < count; i++) {
        TSNode id = swift_child_of_kind(ts_node_named_child(params, i), "type_identifier");
        const char *text = ts_node_is_null(id) ? NULL : cbm_source_text(walk, id);
        if (text)
            joined = cbm_arena_sprintf(walk->arena, "%s%s,", joined, text);
    }
    return joined;
}

static bool swift_list_has(const char *list, const char *name) {
    if (!list || !name)
        return false;
    size_t length = strlen(name);
    for (const char *p = list; (p = strstr(p, name)); p += length)
        if (p > list && p[-1] == ',' && p[length] == ',')
            return true;
    return false;
}

static bool swift_type_param_text(const char *type_params, const char *type) {
    if (!type)
        return false;
    const char *dot = strchr(type, '.');
    if (!dot)
        return swift_list_has(type_params, type);
    char head[128];
    size_t length = (size_t)(dot - type);
    if (length >= sizeof(head))
        return true;
    memcpy(head, type, length);
    head[length] = '\0';
    return swift_list_has(type_params, head);
}

/* Access flags, read from the declaration's modifiers. */
static uint32_t swift_modifier_flags(cbm_source_walk_t *walk, TSNode declaration) {
    uint32_t flags = 0;
    TSNode modifiers = swift_child_of_kind(declaration, "modifiers");
    uint32_t count = ts_node_is_null(modifiers) ? 0 : ts_node_named_child_count(modifiers);
    for (uint32_t i = 0; i < count; i++) {
        TSNode modifier = ts_node_named_child(modifiers, i);
        const char *text = cbm_source_text(walk, modifier);
        if (!text)
            continue;
        if (cbm_source_is(modifier, "visibility_modifier")) {
            if (!strncmp(text, "private", 7) || !strncmp(text, "fileprivate", 11))
                flags |= CBM_SOURCE_DECL_F_PRIVATE;
            else if (!strncmp(text, "public", 6) || !strncmp(text, "open", 4))
                flags |= CBM_SOURCE_DECL_F_PUBLIC;
        } else if (!strcmp(text, "static") || !strcmp(text, "class")) {
            flags |= CBM_SOURCE_DECL_F_STATIC;
        }
    }
    return flags;
}

/* ── Phase one ─────────────────────────────────────────────────────── */

static void swift_note_params(cbm_source_walk_t *walk, TSNode function, cbm_source_decl_t *decl) {
    uint32_t count = ts_node_named_child_count(function);
    int total = 0;
    for (uint32_t i = 0; i < count; i++)
        total += cbm_source_is(ts_node_named_child(function, i), "parameter");
    cbm_source_param_t *params =
        total ? cbm_source_table_alloc(walk, (size_t)total * sizeof(*params)) : NULL;
    if (total && !params) {
        decl->param_count = -1;
        return;
    }
    int used = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(function, i);
        if (!cbm_source_is(child, "parameter"))
            continue;
        TSNode external = cbm_source_field(child, "external_name");
        const char *label = cbm_source_text(
            walk, ts_node_is_null(external) ? cbm_source_field(child, "name") : external);
        cbm_source_param_t *param = &params[used++];
        *param = (cbm_source_param_t){.name = label ? cbm_source_table_intern(walk, label) : ""};
        /* A sibling carrying the default_value field, right after, makes it optional. */
        const char *field =
            i + 1 < count ? ts_node_field_name_for_named_child(function, i + 1) : NULL;
        if (field && !strcmp(field, "default_value"))
            param->flags |= CBM_SOURCE_PARAM_OPTIONAL;
        const char *text = cbm_source_text(walk, child);
        if (text && strstr(text, "..."))
            param->flags |= CBM_SOURCE_PARAM_VARIADIC;
    }
    decl->params = params;
    decl->param_count = used;
}

static void swift_collect_function(cbm_source_walk_t *walk, const char *owner, TSNode node,
                                   const char *type_params) {
    TSNode name = cbm_source_field(node, "name");
    const char *text =
        cbm_source_is(name, "simple_identifier") ? cbm_source_text(walk, name) : NULL;
    if (!text)
        return;
    uint32_t line = ts_node_start_point(name).row + 1;
    cbm_source_decl_t *decl =
        cbm_source_declare(walk, owner, text, cbm_source_def_qn(walk, text, line),
                           swift_modifier_flags(walk, node), line);
    if (!decl)
        return;
    swift_note_params(walk, node, decl);
    const char *params = swift_type_params(walk, node, type_params);
    const char *type = swift_return_type(walk, node);
    if (type && !swift_type_param_text(params, type))
        decl->type = cbm_source_table_intern(walk, type);
}

static void swift_collect_pattern_names(cbm_source_walk_t *walk, const char *owner, TSNode pattern,
                                        const char *type, uint32_t flags, unsigned depth) {
    if (ts_node_is_null(pattern) || depth > 6)
        return;
    if (cbm_source_is(pattern, "simple_identifier")) {
        const char *text = cbm_source_text(walk, pattern);
        size_t length = text ? strlen(text) : 0;
        /* A backticked name is the same name at its uses. */
        if (length > 2 && text[0] == '`' && text[length - 1] == '`')
            text = cbm_arena_strndup(walk->arena, text + 1, length - 2);
        cbm_source_decl_t *decl =
            text ? cbm_source_declare(walk, owner, text, NULL, flags | CBM_SOURCE_DECL_F_VALUE,
                                      ts_node_start_point(pattern).row + 1)
                 : NULL;
        if (decl && type)
            decl->type = cbm_source_table_intern(walk, type);
        return;
    }
    uint32_t count = ts_node_named_child_count(pattern);
    for (uint32_t i = 0; i < count; i++)
        swift_collect_pattern_names(walk, owner, ts_node_named_child(pattern, i), type, flags,
                                    depth + 1);
}

static void swift_collect_property(cbm_source_walk_t *walk, const char *owner, TSNode node,
                                   const char *type_params) {
    TSNode annotation = swift_child_of_kind(node, "type_annotation");
    const char *type = NULL;
    if (!ts_node_is_null(annotation)) {
        type = swift_type_text(walk, annotation, 0);
    } else {
        /* `let x = Foo(...)`: the name of the possible type it constructs. */
        TSNode value = cbm_source_field(node, "value");
        if (cbm_source_is(value, "call_expression") &&
            cbm_source_is(ts_node_named_child(value, 0), "simple_identifier"))
            type = cbm_source_text(walk, ts_node_named_child(value, 0));
    }
    if (swift_type_param_text(type_params, type))
        type = NULL;
    uint32_t flags = swift_modifier_flags(walk, node);
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "pattern"))
            swift_collect_pattern_names(walk, owner, child, type, flags, 0);
    }
}

static TSNode swift_body_of(TSNode node) {
    TSNode body = cbm_source_field(node, "body");
    return body;
}

static void swift_note_bases(cbm_source_walk_t *walk, TSNode node, cbm_source_decl_t *record) {
    const char *found[SWIFT_MAX_BASES];
    size_t used = 0;
    bool open = false;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!cbm_source_is(child, "inheritance_specifier"))
            continue;
        const char *text = swift_type_text(walk, cbm_source_field(child, "inherits_from"), 0);
        if (!text || used == SWIFT_MAX_BASES)
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

static void swift_collect_type(cbm_source_walk_t *walk, const char *module, const char *outer,
                               TSNode node);

static void swift_collect_members(cbm_source_walk_t *walk, const char *module, const char *owner,
                                  TSNode body, const char *type_params) {
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (cbm_source_is(member, "function_declaration") ||
            cbm_source_is(member, "protocol_function_declaration"))
            swift_collect_function(walk, owner, member, type_params);
        else if (cbm_source_is(member, "property_declaration") ||
                 cbm_source_is(member, "protocol_property_declaration"))
            swift_collect_property(walk, owner, member, type_params);
        else if (cbm_source_is(member, "class_declaration") ||
                 cbm_source_is(member, "protocol_declaration"))
            swift_collect_type(walk, module, owner, member);
        else if (cbm_source_is(member, "enum_entry")) {
            /* A case is a member like any other; `.a` of this enum's own type. */
            uint32_t names = ts_node_named_child_count(member);
            for (uint32_t n = 0; n < names; n++) {
                TSNode name = ts_node_named_child(member, n);
                if (cbm_source_is(name, "simple_identifier"))
                    swift_collect_pattern_names(walk, owner, name, NULL, 0, 0);
            }
        }
    }
}

static void swift_collect_type(cbm_source_walk_t *walk, const char *module, const char *outer,
                               TSNode node) {
    TSNode name = cbm_source_field(node, "name");
    bool extension = cbm_source_is(name, "user_type");
    const char *text = extension ? swift_type_text(walk, name, 0) : cbm_source_text(walk, name);
    if (!text)
        return;
    const char *owner_of_name = outer ? outer : swift_module_owner(walk, module);
    const char *key = swift_nested_key(walk, module, outer, text);
    if (!key || !owner_of_name)
        return;
    uint32_t line = ts_node_start_point(node).row + 1;
    cbm_source_decl_t *record =
        cbm_source_declare(walk, key, SWIFT_TYPE_RECORD, NULL, CBM_SOURCE_DECL_F_TYPE, line);
    if (record) {
        swift_note_bases(walk, node, record);
        if (extension)
            record->flags |= CBM_SOURCE_DECL_F_ADAPTED;
    }
    if (!extension) {
        const char *name_key = cbm_arena_sprintf(walk->arena, "%s%s", SWIFT_TYPE_NAME, text);
        cbm_source_decl_t *declared =
            name_key ? cbm_source_declare(
                           walk, owner_of_name, name_key, cbm_source_def_qn(walk, text, line),
                           CBM_SOURCE_DECL_F_TYPE | swift_modifier_flags(walk, node), line)
                     : NULL;
        if (declared)
            declared->type = cbm_source_table_intern(walk, key);
    }
    swift_collect_members(walk, module, key, swift_body_of(node),
                          swift_type_params(walk, node, NULL));
}

static void swift_collect(cbm_source_walk_t *walk) {
    const char *module = swift_module_of(walk, walk->file);
    const char *owner = swift_module_owner(walk, module);
    if (!owner)
        return;
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (cbm_source_is(child, "function_declaration")) {
            swift_collect_function(walk, owner, child, NULL);
        } else if (cbm_source_is(child, "property_declaration")) {
            swift_collect_property(walk, owner, child, NULL);
        } else if (cbm_source_is(child, "class_declaration") ||
                   cbm_source_is(child, "protocol_declaration")) {
            swift_collect_type(walk, module, NULL, child);
        } else if (cbm_source_is(child, "import_declaration")) {
            TSNode path = swift_child_of_kind(child, "identifier");
            const char *imported = cbm_source_text(walk, ts_node_named_child(path, 0));
            cbm_source_decl_t *decl =
                imported
                    ? cbm_source_declare(walk, owner, SWIFT_IMPORTS, NULL, CBM_SOURCE_DECL_F_VALUE,
                                         ts_node_start_point(child).row + 1)
                    : NULL;
            if (decl) {
                decl->type = cbm_source_table_intern(walk, imported);
                const char *text = cbm_source_text(walk, child);
                if (text && strstr(text, "@testable"))
                    decl->flags |= CBM_SOURCE_DECL_F_OPEN;
            }
        }
    }
}

/* ── Phase two: visibility and selection ───────────────────────────── */

static bool swift_visible(const swift_ctx_t *ctx, const cbm_source_decl_t *decl, bool testable,
                          bool same_module) {
    if ((decl->flags & CBM_SOURCE_DECL_F_PRIVATE) && strcmp(decl->file, ctx->walk->file))
        return false;
    if (same_module)
        return ctx->module[0] || !strcmp(decl->file, ctx->walk->file);
    return testable || (decl->flags & CBM_SOURCE_DECL_F_PUBLIC);
}

static bool swift_args_fit(const cbm_source_decl_t *decl, const swift_args_t *args) {
    if (!args || args->too_many || decl->param_count < 0)
        return true;
    int next = 0, trailing = args->trailing;
    for (int p = 0; p < decl->param_count; p++) {
        const cbm_source_param_t *param = &decl->params[p];
        bool unlabelled = !strcmp(param->name, "_");
        bool matches =
            next < args->count &&
            (unlabelled ? args->labels[next] == NULL
                        : args->labels[next] && !strcmp(args->labels[next], param->name));
        if (matches) {
            next++;
            if (param->flags & CBM_SOURCE_PARAM_VARIADIC)
                while (next < args->count && !args->labels[next])
                    next++;
            continue;
        }
        if (next == args->count && trailing > 0) {
            trailing--;
            continue;
        }
        if (param->flags & (CBM_SOURCE_PARAM_OPTIONAL | CBM_SOURCE_PARAM_VARIADIC))
            continue;
        return false;
    }
    return next == args->count && trailing == 0;
}

/* The one graph node these declarations select for these arguments. 0: none fits. */
static int swift_select(const swift_ctx_t *ctx, const cbm_source_decl_t *first,
                        const swift_args_t *args, bool testable, bool same_module,
                        const cbm_source_decl_t **out) {
    const cbm_source_decl_t *selected = NULL;
    for (const cbm_source_decl_t *decl = first; decl; decl = decl->next) {
        if (!swift_visible(ctx, decl, testable, same_module) || !swift_args_fit(decl, args))
            continue;
        if (selected) {
            bool same_node = selected->qn && decl->qn && !strcmp(selected->qn, decl->qn);
            bool both_values = (selected->flags & CBM_SOURCE_DECL_F_VALUE) &&
                               (decl->flags & CBM_SOURCE_DECL_F_VALUE);
            if (!same_node && !both_values)
                return -1;
            continue;
        }
        selected = decl;
    }
    *out = selected;
    return selected ? 1 : 0;
}

/* Same-named private types in other files are not candidates here. */
static const cbm_source_decl_t *swift_visible_type(const swift_ctx_t *ctx,
                                                   const cbm_source_decl_t *first, bool testable,
                                                   bool same_module) {
    const cbm_source_decl_t *selected = NULL;
    for (const cbm_source_decl_t *decl = first; decl; decl = decl->next) {
        if (!swift_visible(ctx, decl, testable, same_module))
            continue;
        if (selected)
            return NULL;
        selected = decl;
    }
    return selected;
}

static const swift_import_t *swift_imports_of(swift_ctx_t *ctx, const char *module, size_t *count) {
    /* Each module's imports are read once per file. */
    typedef struct {
        swift_import_t *items;
        size_t count;
    } cached_t;
    cached_t *cached = cbm_ht_get(ctx->module_imports, module);
    if (!cached) {
        cached = cbm_arena_calloc(ctx->walk->arena, sizeof(*cached));
        char *key = cbm_arena_strdup(ctx->walk->arena, module);
        const char *owner = swift_module_owner(ctx->walk, module);
        if (!cached || !key || !owner) {
            cbm_source_fail(ctx->walk, "swift_import_allocation");
            *count = 0;
            return NULL;
        }
        size_t total = 0;
        for (const cbm_source_decl_t *d = cbm_source_lookup(ctx->walk, owner, SWIFT_IMPORTS); d;
             d = d->next)
            total++;
        cached->items =
            total ? cbm_arena_calloc(ctx->walk->arena, total * sizeof(*cached->items)) : NULL;
        for (const cbm_source_decl_t *d = cbm_source_lookup(ctx->walk, owner, SWIFT_IMPORTS);
             cached->items && d; d = d->next) {
            const char *dir =
                d->type ? swift_package_value(ctx->walk, SWIFT_TARGET_KEY, d->type) : NULL;
            bool seen = false;
            for (size_t i = 0; dir && i < cached->count; i++)
                seen |= !strcmp(cached->items[i].module, dir);
            if (dir && !seen)
                cached->items[cached->count++] =
                    (swift_import_t){dir, (d->flags & CBM_SOURCE_DECL_F_OPEN) != 0};
        }
        cbm_ht_set(ctx->module_imports, key, cached);
    }
    *count = cached->count;
    return cached->items;
}

static const char *swift_module_from_key(cbm_source_walk_t *walk, const char *owner) {
    if (!strncmp(owner, "m:", 2))
        return owner + 2;
    const char *hash = strchr(owner, '#');
    return hash ? cbm_arena_strndup(walk->arena, owner + 2, (size_t)(hash - owner - 2)) : "";
}

/* A written type name, seen from a declaration's owner. */
static const char *swift_lookup_type(swift_ctx_t *ctx, const char *owner, const char *text) {
    if (!owner || !text)
        return NULL;
    cbm_source_walk_t *walk = ctx->walk;
    const char *dot = strchr(text, '.');
    const char *head = dot ? cbm_arena_strndup(walk->arena, text, (size_t)(dot - text)) : text;
    const char *name_key =
        head ? cbm_arena_sprintf(walk->arena, "%s%s", SWIFT_TYPE_NAME, head) : NULL;
    if (!name_key)
        return NULL;
    if (!strcmp(head, "Self") && !strncmp(owner, "t:", 2))
        return dot ? NULL : owner;
    const char *module = swift_module_from_key(walk, owner);
    const cbm_source_decl_t *found = NULL;
    /* Nested types from inside out, then the module, then its imports. */
    char *scope = !strncmp(owner, "t:", 2) ? cbm_arena_strdup(walk->arena, owner) : NULL;
    while (scope && !found) {
        found = cbm_source_lookup(walk, scope, name_key);
        char *up = strrchr(scope, '.');
        char *hash = strchr(scope, '#');
        if (!up || (hash && up < hash))
            break;
        *up = '\0';
    }
    if (!found)
        found = cbm_source_lookup(walk, swift_module_owner(walk, module), name_key);
    if (!found) {
        size_t count = 0;
        const swift_import_t *imports = swift_imports_of(ctx, module, &count);
        for (size_t i = 0; i < count; i++) {
            const cbm_source_decl_t *imported =
                cbm_source_lookup(walk, swift_module_owner(walk, imports[i].module), name_key);
            if (imported && found && imported != found)
                return NULL;
            if (imported)
                found = imported;
        }
    }
    if (found)
        found = swift_visible_type(ctx, found, true, true);
    if (!found || !found->type)
        return NULL;
    const char *key = found->type;
    while (dot && key) {
        text = dot + 1;
        dot = strchr(text, '.');
        const char *part = dot ? cbm_arena_strndup(walk->arena, text, (size_t)(dot - text)) : text;
        const char *nested =
            part ? cbm_arena_sprintf(walk->arena, "%s%s", SWIFT_TYPE_NAME, part) : NULL;
        const cbm_source_decl_t *inner = nested ? cbm_source_lookup(walk, key, nested) : NULL;
        key = inner && !inner->next ? inner->type : NULL;
    }
    return key;
}

/* Standard protocols that add a known, closed set of members. */
static bool swift_closed_base(const char *base, const char *name) {
    static const char *const closed[] = {"Equatable",
                                         "Hashable",
                                         "Comparable",
                                         "Codable",
                                         "Encodable",
                                         "Decodable",
                                         "Sendable",
                                         "Error",
                                         "Identifiable",
                                         "CaseIterable",
                                         "CustomStringConvertible",
                                         "CustomDebugStringConvertible",
                                         "AnyObject"};
    static const char *const members[] = {
        "hash", "encode",   "description", "debugDescription",
        "id",   "allCases", "hashValue",   "localizedDescription"};
    bool known = false;
    for (size_t i = 0; i < sizeof(closed) / sizeof(closed[0]); i++)
        known |= !strcmp(base, closed[i]);
    if (!known)
        return false;
    for (size_t i = 0; i < sizeof(members) / sizeof(members[0]); i++)
        if (!strcmp(name, members[i]))
            return false;
    return true;
}

typedef enum { SWIFT_FOUND, SWIFT_ABSENT, SWIFT_OPEN, SWIFT_AMBIGUOUS } swift_lookup_t;

static swift_lookup_t swift_find_member(swift_ctx_t *ctx, const char *type_key, const char *name,
                                        const swift_args_t *args, bool bases_only,
                                        cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!cbm_source_chain_first_visit(chain, type_key))
        return chain->overflow ? SWIFT_OPEN : SWIFT_ABSENT;
    const char *module = swift_module_from_key(ctx->walk, type_key);
    const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, type_key, SWIFT_TYPE_RECORD);
    if (!records)
        return SWIFT_OPEN;
    if (!bases_only) {
        const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, type_key, name);
        bool same_module = !strcmp(module, ctx->module);
        bool testable = false;
        for (size_t i = 0; !same_module && i < ctx->import_count; i++)
            if (!strcmp(ctx->imports[i].module, module))
                testable |= ctx->imports[i].testable;
        int selected = own ? swift_select(ctx, own, args, testable, same_module, out) : 0;
        if (selected < 0)
            return SWIFT_AMBIGUOUS;
        if (selected > 0)
            return SWIFT_FOUND;
        /* With no known module, another file's member is neither seen nor ruled out. */
        if (own && !module[0])
            return SWIFT_OPEN;
    }
    bool declared = false;
    bool open = false;
    swift_lookup_t result = SWIFT_ABSENT;
    /* An extension of a type outside the project has no declaration here. */
    const char *base_owner = type_key;
    for (const cbm_source_decl_t *record = records; record; record = record->next) {
        declared |= !(record->flags & CBM_SOURCE_DECL_F_ADAPTED);
        open |= (record->flags & CBM_SOURCE_DECL_F_OPEN) != 0;
        for (const char *const *base = record->bases; base && *base; base++) {
            const char *parent = swift_lookup_type(ctx, base_owner, *base);
            if (!parent) {
                open |= !swift_closed_base(*base, name);
                continue;
            }
            const cbm_source_decl_t *found = NULL;
            swift_lookup_t step = swift_find_member(ctx, parent, name, args, false, chain, &found);
            if (step == SWIFT_FOUND) {
                if (result == SWIFT_FOUND && *out != found &&
                    (!(*out)->qn || !found->qn || strcmp((*out)->qn, found->qn)))
                    return SWIFT_AMBIGUOUS;
                *out = found;
                result = SWIFT_FOUND;
            } else if (step == SWIFT_AMBIGUOUS) {
                return step;
            } else if (step == SWIFT_OPEN) {
                open = true;
            }
        }
    }
    if (result == SWIFT_FOUND)
        return open ? SWIFT_OPEN : result;
    return open || !declared ? SWIFT_OPEN : SWIFT_ABSENT;
}

static const char *swift_declared_type(swift_ctx_t *ctx, const cbm_source_decl_t *decl) {
    return decl->type ? swift_lookup_type(ctx, decl->owner, decl->type) : NULL;
}

static swift_ref_t swift_ref_of_decl(swift_ctx_t *ctx, const cbm_source_decl_t *decl) {
    swift_ref_t ref = {.reason = "swift_name_not_in_project"};
    if (decl->flags & CBM_SOURCE_DECL_F_VALUE) {
        const char *type = swift_declared_type(ctx, decl);
        ref.kind = type ? SWIFT_VALUE : SWIFT_UNKNOWN;
        ref.type_key = type;
        ref.reason = type ? "swift_call_of_a_value" : "swift_value_of_unknown_type";
        return ref;
    }
    ref.kind = SWIFT_FUNCTION;
    ref.decl = decl;
    ref.reason = "swift_declared_function";
    return ref;
}

static swift_ref_t swift_ref_of_type(const cbm_source_decl_t *decl) {
    return (swift_ref_t){
        .kind = SWIFT_TYPE, .type_key = decl->type, .decl = decl, .reason = "swift_type_name"};
}

/* A name at module level, in this file's module or the modules it imports. */
static swift_ref_t swift_module_level(swift_ctx_t *ctx, const char *name,
                                      const swift_args_t *args) {
    cbm_source_walk_t *walk = ctx->walk;
    swift_ref_t ref = {.reason = "swift_name_not_in_project"};
    const char *owner = swift_module_owner(walk, ctx->module);
    const char *type_key = cbm_arena_sprintf(walk->arena, "%s%s", SWIFT_TYPE_NAME, name);
    if (!owner || !type_key)
        return ref;
    const cbm_source_decl_t *own = cbm_source_lookup(walk, owner, name);
    const cbm_source_decl_t *selected = NULL;
    int count = own ? swift_select(ctx, own, args, true, true, &selected) : 0;
    if (count < 0) {
        ref.reason = "swift_overload_not_decided";
        return ref;
    }
    if (count > 0)
        return swift_ref_of_decl(ctx, selected);
    if (own && !ctx->module[0]) {
        ref.reason = "swift_build_module_membership_required";
        return ref;
    }
    const cbm_source_decl_t *type = cbm_source_lookup(walk, owner, type_key);
    if (type) {
        const cbm_source_decl_t *visible = swift_visible_type(ctx, type, true, true);
        if (!visible) {
            ref.reason = "swift_build_module_membership_required";
            return ref;
        }
        return swift_ref_of_type(visible);
    }
    for (size_t i = 0; i < ctx->import_count; i++) {
        const char *other = swift_module_owner(walk, ctx->imports[i].module);
        const cbm_source_decl_t *function = cbm_source_lookup(walk, other, name);
        const cbm_source_decl_t *found = NULL;
        int fits = function
                       ? swift_select(ctx, function, args, ctx->imports[i].testable, false, &found)
                       : 0;
        if (fits < 0) {
            ref.reason = "swift_overload_not_decided";
            return ref;
        }
        const cbm_source_decl_t *imported_type = cbm_source_lookup(walk, other, type_key);
        if (imported_type)
            imported_type = swift_visible_type(ctx, imported_type, ctx->imports[i].testable, false);
        swift_ref_t next = {.reason = "swift_name_not_in_project"};
        if (fits > 0) {
            next = swift_ref_of_decl(ctx, found);
        } else if (imported_type) {
            next = swift_ref_of_type(imported_type);
        } else {
            continue;
        }
        next.via_import = true;
        if (ref.via_import && ref.decl != next.decl) {
            ref = (swift_ref_t){.reason = "swift_two_imports_provide_name"};
            return ref;
        }
        ref = next;
    }
    return ref;
}

static const swift_local_t *swift_find_local(const swift_ctx_t *ctx, const char *name) {
    for (const swift_local_t *local = ctx->locals; local; local = local->next)
        if (!strcmp(local->name, name))
            return local;
    return NULL;
}

static swift_ref_t swift_name(swift_ctx_t *ctx, const char *name, const swift_args_t *args) {
    swift_ref_t ref = {.reason = "swift_name_not_in_project"};
    const swift_local_t *local = swift_find_local(ctx, name);
    if (local) {
        ref.kind = local->type_key ? SWIFT_VALUE : SWIFT_UNKNOWN;
        ref.type_key = local->type_key;
        ref.reason = local->type_key ? "swift_call_of_a_value" : "swift_local_of_unknown_type";
        return ref;
    }
    if (swift_list_has(ctx->type_params, name)) {
        ref.reason = "swift_type_parameter";
        return ref;
    }
    /* The enclosing types, innermost first. */
    char *scope = ctx->type_key ? cbm_arena_strdup(ctx->walk->arena, ctx->type_key) : NULL;
    while (scope) {
        const cbm_source_decl_t *member = NULL;
        cbm_source_chain_t chain = {0};
        switch (swift_find_member(ctx, scope, name, args, false, &chain, &member)) {
        case SWIFT_FOUND:
            return swift_ref_of_decl(ctx, member);
        case SWIFT_AMBIGUOUS:
            ref.reason = "swift_overload_not_decided";
            return ref;
        case SWIFT_OPEN: {
            /* Its own type name still reaches an initializer. */
            ref.reason = "swift_inheritance_leaves_project";
            const cbm_source_decl_t *nested = NULL;
            const char *name_key =
                cbm_arena_sprintf(ctx->walk->arena, "%s%s", SWIFT_TYPE_NAME, name);
            nested = name_key ? cbm_source_lookup(ctx->walk, scope, name_key) : NULL;
            if (nested)
                return swift_ref_of_type(nested);
            /* A superclass nesting a type of this very name is the rare case the
             * compiler itself warns about; the module's types are still types. */
            swift_ref_t level = swift_module_level(ctx, name, args);
            return level.kind == SWIFT_TYPE ? level : ref;
        }
        case SWIFT_ABSENT:
            break;
        }
        const char *name_key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", SWIFT_TYPE_NAME, name);
        const cbm_source_decl_t *nested =
            name_key ? cbm_source_lookup(ctx->walk, scope, name_key) : NULL;
        if (nested)
            return swift_ref_of_type(nested);
        char *up = strrchr(scope, '.');
        char *hash = strchr(scope, '#');
        if (!up || (hash && up < hash))
            break;
        *up = '\0';
    }
    ref = swift_module_level(ctx, name, args);
    if (ref.kind != SWIFT_UNKNOWN)
        return ref;
    /* `Alamofire.foo()`: a module's own name. */
    const char *dir = swift_package_value(ctx->walk, SWIFT_TARGET_KEY, name);
    for (size_t i = 0; dir && i < ctx->import_count; i++) {
        if (!strcmp(ctx->imports[i].module, dir)) {
            ref = (swift_ref_t){.kind = SWIFT_MODULE, .module = dir, .via_import = true};
            return ref;
        }
    }
    return ref;
}

static swift_ref_t swift_member(swift_ctx_t *ctx, const swift_ref_t *receiver, const char *name,
                                const swift_args_t *args) {
    swift_ref_t ref = {.reason = "swift_receiver_type_unknown"};
    if (receiver->kind == SWIFT_MODULE) {
        swift_ctx_t scoped = *ctx;
        scoped.module = receiver->module;
        const cbm_source_decl_t *found = NULL;
        const char *owner = swift_module_owner(ctx->walk, receiver->module);
        const cbm_source_decl_t *first = owner ? cbm_source_lookup(ctx->walk, owner, name) : NULL;
        bool testable = false;
        for (size_t i = 0; i < ctx->import_count; i++)
            if (!strcmp(ctx->imports[i].module, receiver->module))
                testable |= ctx->imports[i].testable;
        if (first && swift_select(ctx, first, args, testable, false, &found) > 0) {
            ref = swift_ref_of_decl(ctx, found);
            ref.via_import = true;
            return ref;
        }
        const char *type_key =
            owner ? cbm_arena_sprintf(ctx->walk->arena, "%s%s", SWIFT_TYPE_NAME, name) : NULL;
        const cbm_source_decl_t *type =
            type_key ? cbm_source_lookup(ctx->walk, owner, type_key) : NULL;
        if (type) {
            ref = swift_ref_of_type(type);
            ref.via_import = true;
            return ref;
        }
        ref.reason = "swift_module_member_not_in_project";
        return ref;
    }
    if ((receiver->kind != SWIFT_VALUE && receiver->kind != SWIFT_TYPE) || !receiver->type_key)
        return ref;
    if (receiver->kind == SWIFT_TYPE) {
        if (!strcmp(name, "init")) {
            ref = *receiver;
            ref.reason = "swift_explicit_init";
            return ref;
        }
        const char *name_key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", SWIFT_TYPE_NAME, name);
        const cbm_source_decl_t *nested =
            name_key ? cbm_source_lookup(ctx->walk, receiver->type_key, name_key) : NULL;
        if (nested)
            return swift_ref_of_type(nested);
    }
    const cbm_source_decl_t *member = NULL;
    cbm_source_chain_t chain = {0};
    switch (swift_find_member(ctx, receiver->type_key, name, args, receiver->from_super, &chain,
                              &member)) {
    case SWIFT_FOUND:
        ref = swift_ref_of_decl(ctx, member);
        ref.via_receiver = true;
        return ref;
    case SWIFT_OPEN:
        ref.reason = "swift_member_chain_leaves_project";
        return ref;
    case SWIFT_AMBIGUOUS:
        ref.reason = "swift_overload_not_decided";
        return ref;
    case SWIFT_ABSENT:
        break;
    }
    ref.reason = "swift_member_absent_from_chain";
    return ref;
}

/* ── Phase two: expressions ────────────────────────────────────────── */

static void swift_read_args(swift_ctx_t *ctx, TSNode call, swift_args_t *args) {
    *args = (swift_args_t){0};
    TSNode suffix = swift_child_of_kind(call, "call_suffix");
    uint32_t count = ts_node_is_null(suffix) ? 0 : ts_node_named_child_count(suffix);
    for (uint32_t i = 0; i < count; i++) {
        TSNode part = ts_node_named_child(suffix, i);
        if (cbm_source_is(part, "lambda_literal") || cbm_source_is(part, "annotated_lambda")) {
            args->trailing++;
        } else if (cbm_source_is(part, "value_arguments")) {
            uint32_t values = ts_node_named_child_count(part);
            for (uint32_t v = 0; v < values; v++) {
                TSNode value = ts_node_named_child(part, v);
                if (!cbm_source_is(value, "value_argument"))
                    continue;
                if (args->count == SWIFT_MAX_LABELS) {
                    args->too_many = true;
                    return;
                }
                TSNode label = cbm_source_field(value, "name");
                args->labels[args->count++] =
                    ts_node_is_null(label) ? NULL : cbm_source_text(ctx->walk, label);
            }
        }
    }
}

static swift_ref_t swift_expr(swift_ctx_t *ctx, TSNode node, const swift_args_t *args,
                              unsigned depth) {
    swift_ref_t unknown = {.reason = "swift_computed_receiver"};
    if (ts_node_is_null(node) || depth > 64)
        return unknown;
    if (cbm_source_is(node, "simple_identifier")) {
        const char *name = cbm_source_text(ctx->walk, node);
        return name ? swift_name(ctx, name, args) : unknown;
    }
    if (cbm_source_is(node, "self_expression")) {
        if (!ctx->type_key)
            return unknown;
        return (swift_ref_t){.kind = SWIFT_VALUE, .type_key = ctx->type_key};
    }
    if (cbm_source_is(node, "super_expression")) {
        if (!ctx->type_key)
            return unknown;
        return (swift_ref_t){.kind = SWIFT_VALUE, .type_key = ctx->type_key, .from_super = true};
    }
    if (cbm_source_is(node, "navigation_expression")) {
        TSNode suffix = cbm_source_field(node, "suffix");
        const char *member = cbm_source_text(ctx->walk, cbm_source_field(suffix, "suffix"));
        if (!member)
            return unknown;
        swift_ref_t target = swift_expr(ctx, cbm_source_field(node, "target"), NULL, depth + 1);
        return swift_member(ctx, &target, member, args);
    }
    if (cbm_source_is(node, "call_expression")) {
        swift_args_t call_args;
        swift_read_args(ctx, node, &call_args);
        swift_ref_t callee = swift_expr(ctx, ts_node_named_child(node, 0), &call_args, depth + 1);
        swift_ref_t result = {.reason = "swift_return_type_unknown"};
        if (callee.kind == SWIFT_TYPE && callee.type_key) {
            result.kind = SWIFT_VALUE;
            result.type_key = callee.type_key;
        } else if (callee.kind == SWIFT_FUNCTION) {
            const char *type = swift_declared_type(ctx, callee.decl);
            result.kind = type ? SWIFT_VALUE : SWIFT_UNKNOWN;
            result.type_key = type;
        }
        return result;
    }
    if (cbm_source_is(node, "postfix_expression"))
        return swift_expr(ctx, cbm_source_field(node, "target"), NULL, depth + 1);
    if (cbm_source_is(node, "try_expression"))
        return swift_expr(ctx, cbm_source_field(node, "expr"), NULL, depth + 1);
    if (cbm_source_is(node, "await_expression"))
        return swift_expr(ctx, cbm_source_field(node, "expr"), NULL, depth + 1);
    return unknown;
}

static void swift_bind(swift_ctx_t *ctx, const char *name, const char *type_key) {
    swift_local_t *local = name ? cbm_arena_alloc(ctx->walk->arena, sizeof(*local)) : NULL;
    if (!local)
        return;
    *local = (swift_local_t){name, type_key, ctx->locals};
    ctx->locals = local;
}

static void swift_emit_call(swift_ctx_t *ctx, TSNode call, const swift_ref_t *ref) {
    const char *reason = ref->reason;
    const char *target = NULL;
    cbm_resolve_rule_t rule = ref->via_receiver ? CBM_RESOLVE_RULE_TYPE_MEMBER
                              : ref->via_import ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                                : CBM_RESOLVE_RULE_SCOPE_BINDING;
    if ((ref->kind == SWIFT_FUNCTION || ref->kind == SWIFT_TYPE) && ref->decl) {
        const char *identity = NULL;
        cbm_source_decl_t single = *ref->decl;
        single.next = NULL; /* The selection already told overloads apart. */
        target = cbm_source_unique_qn(&single, &identity);
        if (!target)
            reason = identity;
    }
    cbm_source_emit_facts(ctx->walk, ts_node_start_byte(call), ts_node_end_byte(call),
                          CBM_SOURCE_SITE_CALL, target, rule, ref->decl ? ref->decl->owner : NULL,
                          reason, ref->kind == SWIFT_TYPE ? CBM_RESOLVE_FACT_CLASS_CALLABLE : 0);
}

/* ── Phase two: the walk ───────────────────────────────────────────── */

static void swift_visit(swift_ctx_t *ctx, TSNode node, unsigned depth);
static void swift_bind_pattern(swift_ctx_t *ctx, TSNode node, unsigned depth);

static TSNode swift_cursor_named(TSTreeCursor *cursor, bool *more, const char **field) {
    while (*more) {
        TSNode node = ts_tree_cursor_current_node(cursor);
        const char *name = ts_tree_cursor_current_field_name(cursor);
        *more = ts_tree_cursor_goto_next_sibling(cursor);
        if (ts_node_is_named(node)) {
            *field = name;
            return node;
        }
    }
    *field = NULL;
    return (TSNode){0};
}

/* Children in order. `let x = value` and `if let x = value` keep the bound name and
 * its value as siblings, so a binding takes effect once its value has been read. */
static void swift_visit_children(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    const char *field = NULL;
    const char *pending = NULL;
    TSNode child = swift_cursor_named(&cursor, &more, &field);
    while (!ts_node_is_null(child)) {
        bool bound = field && !strcmp(field, "bound_identifier");
        if (bound) {
            if (pending)
                swift_bind(ctx, pending, NULL);
            pending = cbm_source_text(ctx->walk, child);
        } else if (pending && field && !strcmp(field, "condition")) {
            swift_ref_t value = swift_expr(ctx, child, NULL, 0);
            swift_visit(ctx, child, depth + 1);
            swift_bind(ctx, pending, value.kind == SWIFT_VALUE ? value.type_key : NULL);
            pending = NULL;
        } else {
            if (pending) {
                /* `if let x` with no value rebinds x, keeping what it was. */
                swift_ref_t value = swift_name(ctx, pending, NULL);
                swift_bind(ctx, pending, value.kind == SWIFT_VALUE ? value.type_key : NULL);
                pending = NULL;
            }
            swift_visit(ctx, child, depth + 1);
        }
        child = swift_cursor_named(&cursor, &more, &field);
    }
    if (pending)
        swift_bind(ctx, pending, NULL);
    ts_tree_cursor_delete(&cursor);
}

static void swift_bind_params(swift_ctx_t *ctx, TSNode function) {
    uint32_t count = ts_node_named_child_count(function);
    for (uint32_t i = 0; i < count; i++) {
        TSNode param = ts_node_named_child(function, i);
        if (!cbm_source_is(param, "parameter"))
            continue;
        const char *name = NULL, *type = NULL;
        bool function_typed = false;
        uint32_t parts = ts_node_named_child_count(param);
        for (uint32_t p = 0; p < parts; p++) {
            TSNode part = ts_node_named_child(param, p);
            if (cbm_source_is(part, "simple_identifier")) {
                const char *field = ts_node_field_name_for_named_child(param, p);
                if (field && !strcmp(field, "name"))
                    name = cbm_source_text(ctx->walk, part);
            } else if (swift_type_kind(part)) {
                type = swift_type_text(ctx->walk, part, 0);
                function_typed = cbm_source_is(part, "function_type");
            }
        }
        const char *key = NULL;
        if (type && !function_typed && !swift_type_param_text(ctx->type_params, type))
            key = swift_lookup_type(
                ctx, ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module),
                type);
        swift_bind(ctx, name, key);
    }
}

static void swift_visit_function(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    swift_local_t *saved = ctx->locals;
    const char *saved_params = ctx->type_params;
    bool saved_function = ctx->in_function;
    ctx->type_params = swift_type_params(ctx->walk, node, ctx->type_params);
    swift_bind_params(ctx, node);
    ctx->in_function = true;
    /* Parameter defaults run in the enclosing type; visit everything but the header. */
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "parameter") || cbm_source_is(child, "modifiers") ||
            swift_type_kind(child) || cbm_source_is(child, "simple_identifier") ||
            cbm_source_is(child, "type_parameters") || cbm_source_is(child, "type_constraints"))
            continue;
        swift_visit(ctx, child, depth + 1);
    }
    ctx->in_function = saved_function;
    ctx->type_params = saved_params;
    ctx->locals = saved;
}

static void swift_visit_type(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    TSNode name = cbm_source_field(node, "name");
    const char *text = cbm_source_is(name, "user_type") ? swift_type_text(ctx->walk, name, 0)
                                                        : cbm_source_text(ctx->walk, name);
    if (!text)
        return;
    const char *saved = ctx->type_key;
    const char *saved_params = ctx->type_params;
    swift_local_t *saved_locals = ctx->locals;
    bool saved_function = ctx->in_function;
    ctx->type_key = swift_nested_key(ctx->walk, ctx->module, saved, text);
    ctx->type_params = swift_type_params(ctx->walk, node, NULL);
    ctx->locals = NULL;
    ctx->in_function = false;
    TSNode body = swift_body_of(node);
    if (!ts_node_is_null(body))
        swift_visit_children(ctx, body, depth + 1);
    ctx->type_key = saved;
    ctx->type_params = saved_params;
    ctx->locals = saved_locals;
    ctx->in_function = saved_function;
}

static void swift_visit_property(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!ctx->in_function) {
        swift_visit_children(ctx, node, depth);
        return;
    }
    TSNode value = cbm_source_field(node, "value");
    TSNode annotation = swift_child_of_kind(node, "type_annotation");
    const char *type =
        ts_node_is_null(annotation) ? NULL : swift_type_text(ctx->walk, annotation, 0);
    const char *key = NULL;
    if (type && !swift_type_param_text(ctx->type_params, type))
        key = swift_lookup_type(
            ctx, ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module), type);
    if (ts_node_is_null(annotation) && !ts_node_is_null(value)) {
        swift_ref_t inferred = swift_expr(ctx, value, NULL, 0);
        if (inferred.kind == SWIFT_VALUE)
            key = inferred.type_key;
    }
    if (!ts_node_is_null(value))
        swift_visit(ctx, value, depth + 1);
    /* A local is bound after its value: it does not see itself. */
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (!cbm_source_is(part, "pattern"))
            continue;
        uint32_t names = ts_node_named_child_count(part);
        if (names == 1 && cbm_source_is(ts_node_named_child(part, 0), "simple_identifier")) {
            swift_bind(ctx, cbm_source_text(ctx->walk, ts_node_named_child(part, 0)), key);
        } else {
            swift_bind_pattern(ctx, part, depth + 1);
        }
    }
    /* Computed locals and observers still have bodies to walk. */
    for (uint32_t i = 0; i < count; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (cbm_source_is(part, "computed_property") || cbm_source_is(part, "willset_didset_block"))
            swift_visit(ctx, part, depth + 1);
    }
}

static void swift_visit_call(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    swift_args_t args;
    swift_read_args(ctx, node, &args);
    TSNode callee = ts_node_named_child(node, 0);
    if (cbm_source_is(callee, "simple_identifier") ||
        cbm_source_is(callee, "navigation_expression")) {
        swift_ref_t ref = swift_expr(ctx, callee, &args, 0);
        swift_emit_call(ctx, node, &ref);
    }
    swift_visit_children(ctx, node, depth);
}

/* Every name in a pattern binds here. One that was really a constant compared
 * against can only make a later use abstain. */
static void swift_bind_pattern(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "simple_identifier")) {
        swift_bind(ctx, cbm_source_text(ctx->walk, node), NULL);
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        swift_bind_pattern(ctx, ts_node_named_child(node, i), depth + 1);
}

static void swift_visit(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "class_declaration") || !strcmp(kind, "protocol_declaration")) {
        swift_visit_type(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "function_declaration") || !strcmp(kind, "init_declaration") ||
        !strcmp(kind, "deinit_declaration") || !strcmp(kind, "subscript_declaration")) {
        if (ctx->in_function)
            swift_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(node, "name")), NULL);
        swift_visit_function(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "property_declaration")) {
        swift_visit_property(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "call_expression")) {
        swift_visit_call(ctx, node, depth);
        return;
    }
    if (!strcmp(kind, "computed_property") || !strcmp(kind, "computed_getter") ||
        !strcmp(kind, "computed_setter") || !strcmp(kind, "willset_clause") ||
        !strcmp(kind, "didset_clause")) {
        swift_local_t *saved = ctx->locals;
        bool saved_function = ctx->in_function;
        ctx->in_function = true;
        swift_bind(ctx, "newValue", NULL);
        swift_bind(ctx, "oldValue", NULL);
        swift_visit_children(ctx, node, depth);
        ctx->locals = saved;
        ctx->in_function = saved_function;
        return;
    }
    if (!strcmp(kind, "lambda_literal")) {
        swift_local_t *saved = ctx->locals;
        bool saved_function = ctx->in_function;
        ctx->in_function = true;
        swift_visit_children(ctx, node, depth);
        ctx->locals = saved;
        ctx->in_function = saved_function;
        return;
    }
    if (!strcmp(kind, "pattern")) {
        swift_bind_pattern(ctx, node, depth + 1);
        return;
    }
    if (!strcmp(kind, "lambda_parameter") || !strcmp(kind, "capture_list_item")) {
        swift_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(node, "name")), NULL);
        return;
    }
    if (!strcmp(kind, "catch_block")) {
        swift_local_t *saved = ctx->locals;
        swift_bind(ctx, "error", NULL);
        swift_visit_children(ctx, node, depth);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "statements") || !strcmp(kind, "if_statement") ||
        !strcmp(kind, "for_statement") || !strcmp(kind, "while_statement") ||
        !strcmp(kind, "repeat_while_statement") || !strcmp(kind, "do_statement") ||
        !strcmp(kind, "switch_entry") || !strcmp(kind, "function_body")) {
        swift_local_t *saved = ctx->locals;
        swift_visit_children(ctx, node, depth);
        ctx->locals = saved;
        return;
    }
    swift_visit_children(ctx, node, depth);
}

static void swift_resolve(cbm_source_walk_t *walk) {
    swift_ctx_t ctx = {.walk = walk, .module = swift_module_of(walk, walk->file)};
    ctx.module_imports = cbm_ht_create(8);
    if (!ctx.module_imports) {
        cbm_source_fail(walk, "swift_import_allocation");
        return;
    }
    /* This file's own imports decide what it sees of other modules. */
    uint32_t count = ts_node_named_child_count(walk->root), total = 0;
    for (uint32_t i = 0; i < count; i++)
        total += cbm_source_is(ts_node_named_child(walk->root, i), "import_declaration");
    ctx.imports = total ? cbm_arena_calloc(walk->arena, total * sizeof(*ctx.imports)) : NULL;
    for (uint32_t i = 0; ctx.imports && i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (!cbm_source_is(child, "import_declaration"))
            continue;
        TSNode path = swift_child_of_kind(child, "identifier");
        const char *imported = cbm_source_text(walk, ts_node_named_child(path, 0));
        const char *dir = imported ? swift_package_value(walk, SWIFT_TARGET_KEY, imported) : NULL;
        const char *text = cbm_source_text(walk, child);
        if (dir)
            ctx.imports[ctx.import_count++] =
                (swift_import_t){dir, text && strstr(text, "@testable")};
    }
    swift_visit_children(&ctx, walk->root, 0);
    cbm_ht_free(ctx.module_imports);
}

const cbm_source_adapter_t cbm_source_adapter_swift = {.language = CBM_LANG_SWIFT,
                                                       .collect = swift_collect,
                                                       .resolve = swift_resolve,
                                                       .one_name_one_node = true};
