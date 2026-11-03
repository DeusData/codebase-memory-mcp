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
 * does first. Overloads that share labels usually share one graph node, so the choice
 * does not change the edge. Across files or modules they do not, and the compiler's next
 * step decides: a candidate whose parameter type cannot take the argument is out, and one
 * that takes it exactly beats one that needs a conversion to an existential.
 *
 * `.foo` is a member of the type its context expects: a written annotation, a declared
 * return type, the other side of `==` or `=`, the subject of a switch, or the parameter
 * of the one function a call selected.
 *
 * `Foo(...)` calls an initializer. The graph has no node for one, so the selected
 * declaration is the type, and the evidence says it is a class call.
 *
 * A closure passed to a function takes its parameters' types from the function type that
 * parameter declares; a `typealias` is followed to what it names.
 *
 * `extension URLRequest` adds members to a type declared outside the project. Those members
 * are known and everything else about the type is not, so a member the extension does not
 * declare stays unknown rather than absent.
 *
 * Known gaps, each abstaining: Xcode projects' module membership, element types of
 * collections and generics, a key path with no written root, macros, and overloads only a
 * generic constraint decides. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include "stats.h"
#include "syntax_index.h"

#define SWIFT_TYPE_RECORD "&type"
#define SWIFT_IMPORTS "&imports"
#define SWIFT_TYPE_NAME "type:"
#define SWIFT_ALIAS_NAME "alias:" /* type: what the alias names, as written */
#define SWIFT_CLOSURE_MARK '('    /* a written type that is a function: its parameters' types */
#define SWIFT_CLOSURE_SEPARATOR ','
#define SWIFT_INITIALIZER_READ '=' /* a property's type is that of the member `=A.b` reads */
#define SWIFT_INITIALIZER_CALL '@' /* a property's type is what the member `@A.b` returns */
#define SWIFT_DIR_KEY "@cbm:raw:swift-dir:"
#define SWIFT_TARGET_KEY "@cbm:raw:swift-target:"
#define SWIFT_AMBIGUOUS_PREFIX "@cbm:"

enum { SWIFT_MAX_LABELS = 32, SWIFT_MAX_BASES = 32, SWIFT_MAX_INITIALIZER_CHAIN = 64 };
/* A type record of a struct: with no initializer anywhere it has one for its stored properties. */
enum { SWIFT_RECORD_STRUCT = 1u << 16 };
/* A parameter whose written type is a function: a trailing closure goes to one of these first.
 * One written `some P`: `.foo` can reach a static member of P's extensions, which an existential
 * parameter's `.foo` cannot. */
enum { SWIFT_PARAM_FUNCTION = 1u << 8, SWIFT_PARAM_OPAQUE = 1u << 9 };

typedef struct {
    const char *labels[SWIFT_MAX_LABELS]; /* NULL: an unlabelled argument */
    TSNode values[SWIFT_MAX_LABELS];      /* the argument expressions */
    TSNode first_trailing;                /* the first trailing closure */
    int count;
    int trailing;
    bool too_many;
} swift_args_t;

typedef struct swift_local {
    const char *name;
    const char *type_key; /* NULL: unknown type */
    bool outside;         /* a type the project does not declare */
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
    TSNode callee; /* the callee of the call being walked: its member is called, not read */
    const char *pattern_type; /* the type a pattern's names have, once it has written one */
    const char *return_key;   /* the enclosing function's declared return type */
    const char *subject_key;  /* the type a `case .foo:` of the enclosing switch is a member of */
    CBMHashTable *expected;   /* start byte of a `.foo` -> the type its context expects */
    CBMHashTable *argument_refs; /* start and end of an argument -> what it denotes */
    CBMHashTable *closures;      /* start and end of a closure -> its parameters' written types */
    const struct swift_closure *closure; /* the closure being walked, if a signature typed it */
    int closure_index;                   /* its next parameter */
    int alias_depth;                     /* aliases followed to reach this lookup */
    int initializer_depth;               /* property initializers followed to reach it */
    CBMHashTable *file_private;          /* this file's private top-level type names, or NULL */
    bool imports_testing;                /* `#require` is Swift Testing's */
} swift_ctx_t;

typedef struct swift_closure {
    const char *owner; /* where the signature was written, for its names */
    const char *types; /* `(A,B,`; an empty entry is a type this table does not name */
} swift_closure_t;

typedef enum { SWIFT_UNKNOWN, SWIFT_VALUE, SWIFT_TYPE, SWIFT_MODULE, SWIFT_FUNCTION } swift_kind_t;

typedef struct {
    swift_kind_t kind;
    const char *type_key;           /* SWIFT_VALUE, SWIFT_TYPE */
    const char *module;             /* SWIFT_MODULE */
    const cbm_source_decl_t *decl;  /* SWIFT_FUNCTION, SWIFT_TYPE */
    const cbm_source_decl_t *value; /* a value's own declaration, whatever its type */
    bool via_import, via_receiver, from_super;
    bool outside; /* a value of a type the project does not declare */
    const char *reason;
    const char *common_result; /* overloads not told apart, each returning this type */
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

/* A private type is its file's own, and another file of the module may declare the name again.
 * Its extensions in that file are the same type. */
static const char *swift_top_level_key(cbm_source_walk_t *walk, const char *module,
                                       CBMHashTable *file_private, const char *name) {
    const char *dot = strchr(name, '.');
    const char *head = dot ? cbm_arena_strndup(walk->arena, name, (size_t)(dot - name)) : name;
    if (head && file_private && cbm_ht_get(file_private, head))
        return cbm_arena_sprintf(walk->arena, "t:%s#%s@%s%s", module, head, walk->file,
                                 dot ? dot : "");
    return swift_nested_key(walk, module, NULL, name);
}

/* ── Syntax ────────────────────────────────────────────────────────── */

static TSNode swift_child_of_kind(TSNode node, const char *kind) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), kind))
            return ts_node_named_child(node, i);
    return (TSNode){0};
}

static bool swift_type_kind(TSNode node);
static bool swift_type_param_text(const char *type_params, const char *type);

/* What one entry of a tuple type holds, past its label and modifiers. */
static TSNode swift_tuple_item_type(TSNode item) {
    uint32_t count = ts_node_is_null(item) ? 0 : ts_node_named_child_count(item);
    for (uint32_t i = 0; i < count; i++)
        if (swift_type_kind(ts_node_named_child(item, i)))
            return ts_node_named_child(item, i);
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
    /* `(any P)?` is how an optional existential has to be written. */
    if (cbm_source_is(node, "tuple_type") && ts_node_named_child_count(node) == 1)
        return swift_type_text(walk, swift_tuple_item_type(ts_node_named_child(node, 0)),
                               depth + 1);
    return NULL;
}

/* A function type's parameters as `(A,B,`. NULL when the type is not a function type. */
static const char *swift_closure_signature(cbm_source_walk_t *walk, TSNode type,
                                           const char *type_params) {
    if (cbm_source_is(type, "optional_type")) {
        TSNode wrapped = cbm_source_field(type, "wrapped");
        type = cbm_source_is(wrapped, "tuple_type") && ts_node_named_child_count(wrapped) == 1
                   ? swift_tuple_item_type(ts_node_named_child(wrapped, 0))
                   : wrapped;
    }
    if (!cbm_source_is(type, "function_type"))
        return NULL;
    const char *joined = cbm_arena_sprintf(walk->arena, "%c", SWIFT_CLOSURE_MARK);
    TSNode params = cbm_source_field(type, "params");
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    for (uint32_t i = 0; joined && i < count; i++) {
        const char *text =
            swift_type_text(walk, swift_tuple_item_type(ts_node_named_child(params, i)), 0);
        if (text && swift_type_param_text(type_params, text))
            text = NULL;
        joined = cbm_arena_sprintf(walk->arena, "%s%s%c", joined, text ? text : "",
                                   SWIFT_CLOSURE_SEPARATOR);
    }
    return joined;
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

/* A member with no access of its own takes what its extension or protocol states. */
static uint32_t swift_member_flags(cbm_source_walk_t *walk, TSNode declaration,
                                   uint32_t inherited) {
    uint32_t flags = swift_modifier_flags(walk, declaration);
    bool stated = (flags & (CBM_SOURCE_DECL_F_PUBLIC | CBM_SOURCE_DECL_F_PRIVATE)) != 0;
    return stated ? flags : flags | inherited;
}

/* ── Phase one ─────────────────────────────────────────────────────── */

static const char *swift_param_type(cbm_source_walk_t *walk, TSNode parameter,
                                    const char *type_params) {
    uint32_t parts = ts_node_named_child_count(parameter);
    for (uint32_t p = 0; p < parts; p++) {
        TSNode part = ts_node_named_child(parameter, p);
        if (swift_type_kind(part)) {
            const char *type = swift_type_text(walk, part, 0);
            return type && !swift_type_param_text(type_params, type) ? type : NULL;
        }
    }
    return NULL;
}

static bool swift_param_is_function(TSNode parameter) {
    uint32_t parts = ts_node_named_child_count(parameter);
    for (uint32_t p = 0; p < parts; p++) {
        TSNode part = ts_node_named_child(parameter, p);
        if (!swift_type_kind(part))
            continue;
        if (cbm_source_is(part, "optional_type")) {
            TSNode wrapped = cbm_source_field(part, "wrapped");
            part = cbm_source_is(wrapped, "tuple_type") && ts_node_named_child_count(wrapped) == 1
                       ? cbm_source_field(ts_node_named_child(wrapped, 0), "element")
                       : wrapped;
        }
        return cbm_source_is(part, "function_type");
    }
    return false;
}

static void swift_note_params(cbm_source_walk_t *walk, TSNode function, cbm_source_decl_t *decl,
                              const char *type_params) {
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
    /* A default value's field sits right after its parameter. `= nil` is an unnamed token, so
     * only a walk of every child sees it. */
    TSTreeCursor cursor = ts_tree_cursor_new(function);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    while (more) {
        TSNode child = ts_tree_cursor_current_node(&cursor);
        const char *field = ts_tree_cursor_current_field_name(&cursor);
        if (field && !strcmp(field, "default_value") && used > 0) {
            params[used - 1].flags |= CBM_SOURCE_PARAM_OPTIONAL;
        } else if (cbm_source_is(child, "parameter")) {
            TSNode external = cbm_source_field(child, "external_name");
            const char *label = cbm_source_text(
                walk, ts_node_is_null(external) ? cbm_source_field(child, "name") : external);
            cbm_source_param_t *param = &params[used++];
            *param =
                (cbm_source_param_t){.name = label ? cbm_source_table_intern(walk, label) : ""};
            const char *written = swift_param_type(walk, child, type_params);
            if (written)
                param->type = cbm_source_table_intern(walk, written);
            if (swift_param_is_function(child)) {
                param->flags |= SWIFT_PARAM_FUNCTION;
                uint32_t parts = ts_node_named_child_count(child);
                for (uint32_t t = 0; t < parts; t++) {
                    const char *signature =
                        swift_closure_signature(walk, ts_node_named_child(child, t), type_params);
                    if (signature)
                        param->type = cbm_source_table_intern(walk, signature);
                }
            }
            if (!ts_node_is_null(swift_child_of_kind(child, "opaque_type")))
                param->flags |= SWIFT_PARAM_OPAQUE;
            const char *text = cbm_source_text(walk, child);
            if (text && strstr(text, "..."))
                param->flags |= CBM_SOURCE_PARAM_VARIADIC;
        }
        more = ts_tree_cursor_goto_next_sibling(&cursor);
    }
    ts_tree_cursor_delete(&cursor);
    decl->params = params;
    decl->param_count = used;
}

/* `case c(reason: R, _ x: Int = 1)`: a label, the name after `_`, a type, maybe a default. */
static void swift_note_case_params(cbm_source_walk_t *walk, TSNode list, cbm_source_decl_t *decl,
                                   const char *type_params) {
    uint32_t count = ts_node_named_child_count(list);
    int total = 0;
    for (uint32_t i = 0; i < count; i++)
        total += swift_type_kind(ts_node_named_child(list, i));
    cbm_source_param_t *params =
        total ? cbm_source_table_alloc(walk, (size_t)total * sizeof(*params)) : NULL;
    if (!params)
        return;
    int used = 0;
    const char *label = NULL;
    bool unlabelled = false, past_type = false;
    TSTreeCursor cursor = ts_tree_cursor_new(list);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    while (more) {
        TSNode child = ts_tree_cursor_current_node(&cursor);
        more = ts_tree_cursor_goto_next_sibling(&cursor);
        if (cbm_source_is(child, "=")) {
            if (used > 0)
                params[used - 1].flags |= CBM_SOURCE_PARAM_OPTIONAL;
            past_type = true;
        } else if (cbm_source_is(child, ",")) {
            past_type = false;
        } else if (past_type) {
            continue;
        } else if (cbm_source_is(child, "wildcard_pattern")) {
            unlabelled = true;
        } else if (cbm_source_is(child, "simple_identifier")) {
            label = label ? label : cbm_source_text(walk, child);
        } else if (swift_type_kind(child) && used < total) {
            const char *written = swift_type_text(walk, child, 0);
            cbm_source_param_t *param = &params[used++];
            *param = (cbm_source_param_t){
                .name = label && !unlabelled ? cbm_source_table_intern(walk, label) : "_"};
            if (written && !swift_type_param_text(type_params, written))
                param->type = cbm_source_table_intern(walk, written);
            if (cbm_source_is(child, "function_type"))
                param->flags |= SWIFT_PARAM_FUNCTION;
            label = NULL;
            unlabelled = false;
            past_type = true;
        }
    }
    ts_tree_cursor_delete(&cursor);
    decl->params = params;
    decl->param_count = used;
}

static void swift_collect_function(cbm_source_walk_t *walk, const char *owner, TSNode node,
                                   const char *type_params, uint32_t inherited) {
    TSNode name = cbm_source_field(node, "name");
    const char *text =
        cbm_source_is(name, "simple_identifier") ? cbm_source_text(walk, name) : NULL;
    if (!text)
        return;
    uint32_t line = ts_node_start_point(name).row + 1;
    /* A property of this name may share the line; the function is what is declared here. */
    const char *qn = cbm_source_def_qn_label(walk, text, line, "Method");
    if (!qn)
        qn = cbm_source_def_qn_label(walk, text, line, "Function");
    cbm_source_decl_t *decl =
        cbm_source_declare(walk, owner, text, qn, swift_member_flags(walk, node, inherited), line);
    if (!decl)
        return;
    const char *params = swift_type_params(walk, node, type_params);
    swift_note_params(walk, node, decl, params);
    const char *type = swift_return_type(walk, node);
    if (type && !swift_type_param_text(params, type))
        decl->type = cbm_source_table_intern(walk, type);
}

/* `self.init(...)` and `super.init(...)` select an initializer like any member. `Foo(...)` stays
 * a call of the type, which is what names the instance being made. */
static void swift_collect_init(cbm_source_walk_t *walk, const char *owner, TSNode node,
                               const char *type_params, uint32_t inherited) {
    uint32_t line = ts_node_start_point(node).row + 1;
    cbm_source_decl_t *decl = cbm_source_declare(
        walk, owner, "init", cbm_source_def_qn_label(walk, "init", line, "Method"),
        swift_member_flags(walk, node, inherited), line);
    if (decl)
        swift_note_params(walk, node, decl, swift_type_params(walk, node, type_params));
}

static cbm_source_decl_t *swift_declare_value(cbm_source_walk_t *walk, const char *owner,
                                              TSNode name, const char *type, uint32_t flags) {
    const char *text = cbm_source_text(walk, name);
    size_t length = text ? strlen(text) : 0;
    /* A backticked name is the same name at its uses. */
    if (length > 2 && text[0] == '`' && text[length - 1] == '`')
        text = cbm_arena_strndup(walk->arena, text + 1, length - 2);
    uint32_t line = ts_node_start_point(name).row + 1;
    const char *qn = text ? cbm_source_def_qn_label(walk, text, line, "Variable") : NULL;
    cbm_source_decl_t *decl =
        text ? cbm_source_declare(walk, owner, text, qn, flags | CBM_SOURCE_DECL_F_VALUE, line)
             : NULL;
    if (decl && type)
        decl->type = cbm_source_table_intern(walk, type);
    return decl;
}

static void swift_collect_pattern_names(cbm_source_walk_t *walk, const char *owner, TSNode pattern,
                                        const char *type, uint32_t flags, unsigned depth) {
    if (ts_node_is_null(pattern) || depth > 6)
        return;
    if (cbm_source_is(pattern, "simple_identifier")) {
        (void)swift_declare_value(walk, owner, pattern, type, flags);
        return;
    }
    uint32_t count = ts_node_named_child_count(pattern);
    for (uint32_t i = 0; i < count; i++)
        swift_collect_pattern_names(walk, owner, ts_node_named_child(pattern, i), type, flags,
                                    depth + 1);
}

/* What a property's initializer says of its type when none is written. `Foo(...)` and
 * `Foo<T>(...)` name the possible type they construct. `A.b` and `A.b(...)` leave it to b's
 * declaration, which phase two reads. */
static const char *swift_initializer_text(cbm_source_walk_t *walk, TSNode value) {
    if (cbm_source_is(value, "constructor_expression"))
        return swift_type_text(walk, cbm_source_field(value, "constructed_type"), 0);
    bool called = cbm_source_is(value, "call_expression");
    TSNode callee = called ? ts_node_named_child(value, 0) : value;
    if (called && cbm_source_is(callee, "simple_identifier"))
        return cbm_source_text(walk, callee);
    if (!cbm_source_is(callee, "navigation_expression"))
        return NULL;
    TSNode target = cbm_source_field(callee, "target");
    const char *member =
        cbm_source_text(walk, cbm_source_field(cbm_source_field(callee, "suffix"), "suffix"));
    const char *head =
        cbm_source_is(target, "simple_identifier") ? cbm_source_text(walk, target) : NULL;
    return head && member
               ? cbm_arena_sprintf(walk->arena, "%c%s.%s",
                                   called ? SWIFT_INITIALIZER_CALL : SWIFT_INITIALIZER_READ, head,
                                   member)
               : NULL;
}

static bool swift_initializer_marked(const char *type) {
    return type && (type[0] == SWIFT_INITIALIZER_READ || type[0] == SWIFT_INITIALIZER_CALL);
}

static void swift_collect_property(cbm_source_walk_t *walk, const char *owner, TSNode node,
                                   const char *type_params, uint32_t inherited) {
    TSNode annotation = swift_child_of_kind(node, "type_annotation");
    const char *type = ts_node_is_null(annotation)
                           ? swift_initializer_text(walk, cbm_source_field(node, "value"))
                           : swift_type_text(walk, annotation, 0);
    if (swift_type_param_text(type_params, swift_initializer_marked(type) ? type + 1 : type))
        type = NULL;
    uint32_t flags = swift_member_flags(walk, node, inherited);
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
                               TSNode node, CBMHashTable *file_private);

/* An alias's name is a type name in its scope and hides what lies outside it. Which type it
 * means is not recorded, so a value of it has no known members. */
static void swift_collect_alias(cbm_source_walk_t *walk, const char *owner, TSNode node) {
    const char *text = cbm_source_text(walk, cbm_source_field(node, "name"));
    const char *key = text ? cbm_arena_sprintf(walk->arena, "%s%s", SWIFT_TYPE_NAME, text) : NULL;
    uint32_t line = ts_node_start_point(node).row + 1;
    if (key)
        (void)cbm_source_declare(walk, owner, key, NULL, CBM_SOURCE_DECL_F_TYPE, line);
    /* The last type child is what a `typealias` names; an associatedtype's is only a bound. */
    const char *names = NULL;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; cbm_source_is(node, "typealias_declaration") && i < count; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (!swift_type_kind(part))
            continue;
        const char *params = swift_type_params(walk, node, NULL);
        names = swift_closure_signature(walk, part, params);
        if (!names) {
            names = swift_type_text(walk, part, 0);
            names = names && !swift_type_param_text(params, names) ? names : NULL;
        }
    }
    const char *alias_key =
        text && names ? cbm_arena_sprintf(walk->arena, "%s%s", SWIFT_ALIAS_NAME, text) : NULL;
    cbm_source_decl_t *alias =
        alias_key ? cbm_source_declare(walk, owner, alias_key, NULL, CBM_SOURCE_DECL_F_VALUE, line)
                  : NULL;
    if (alias)
        alias->type = cbm_source_table_intern(walk, names);
}

static void swift_collect_members(cbm_source_walk_t *walk, const char *module, const char *owner,
                                  TSNode body, const char *type_params, uint32_t inherited,
                                  uint32_t case_access) {
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (cbm_source_is(member, "function_declaration") ||
            cbm_source_is(member, "protocol_function_declaration"))
            swift_collect_function(walk, owner, member, type_params, inherited);
        else if (cbm_source_is(member, "init_declaration"))
            swift_collect_init(walk, owner, member, type_params, inherited);
        else if (cbm_source_is(member, "property_declaration") ||
                 cbm_source_is(member, "protocol_property_declaration"))
            swift_collect_property(walk, owner, member, type_params, inherited);
        else if (cbm_source_is(member, "class_declaration") ||
                 cbm_source_is(member, "protocol_declaration"))
            swift_collect_type(walk, module, owner, member, NULL);
        else if (cbm_source_is(member, "typealias_declaration") ||
                 cbm_source_is(member, "associatedtype_declaration"))
            swift_collect_alias(walk, owner, member);
        else if (cbm_source_is(member, "enum_entry")) {
            /* A case is a member like any other; `.a` of this enum's own type. Its payload is
             * what a call of it takes. */
            uint32_t names = ts_node_named_child_count(member);
            cbm_source_decl_t *last = NULL;
            for (uint32_t n = 0; n < names; n++) {
                TSNode name = ts_node_named_child(member, n);
                if (cbm_source_is(name, "simple_identifier"))
                    last = swift_declare_value(walk, owner, name, "Self",
                                               CBM_SOURCE_DECL_F_STATIC | case_access);
                else if (cbm_source_is(name, "enum_type_parameters") && last)
                    swift_note_case_params(walk, name, last, type_params);
            }
        }
    }
}

static void swift_collect_type(cbm_source_walk_t *walk, const char *module, const char *outer,
                               TSNode node, CBMHashTable *file_private) {
    TSNode name = cbm_source_field(node, "name");
    bool extension = cbm_source_is(name, "user_type");
    const char *text = extension ? swift_type_text(walk, name, 0) : cbm_source_text(walk, name);
    if (!text)
        return;
    const char *owner_of_name = outer ? outer : swift_module_owner(walk, module);
    const char *key = outer ? swift_nested_key(walk, module, outer, text)
                            : swift_top_level_key(walk, module, file_private, text);
    if (!key || !owner_of_name)
        return;
    uint32_t line = ts_node_start_point(node).row + 1;
    cbm_source_decl_t *record =
        cbm_source_declare(walk, key, SWIFT_TYPE_RECORD, NULL, CBM_SOURCE_DECL_F_TYPE, line);
    if (record) {
        swift_note_bases(walk, node, record);
        if (extension)
            record->flags |= CBM_SOURCE_DECL_F_ADAPTED;
        if (cbm_source_is(node, "protocol_declaration"))
            record->flags |= CBM_SOURCE_DECL_F_INTERFACE;
        if (cbm_source_token(node, "struct"))
            record->flags |= SWIFT_RECORD_STRUCT;
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
    /* An extension's or protocol's access is its members' too, and an enum's its cases'. */
    uint32_t access =
        swift_modifier_flags(walk, node) & (CBM_SOURCE_DECL_F_PUBLIC | CBM_SOURCE_DECL_F_PRIVATE);
    swift_collect_members(
        walk, module, key, swift_body_of(node), swift_type_params(walk, node, NULL),
        extension || cbm_source_is(node, "protocol_declaration") ? access : 0, access);
}

/* The names this file's top-level private types take. NULL: it declares none. */
static CBMHashTable *swift_file_private_types(cbm_source_walk_t *walk) {
    CBMHashTable *names = NULL;
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (!cbm_source_is(child, "class_declaration") &&
            !cbm_source_is(child, "protocol_declaration"))
            continue;
        TSNode name = cbm_source_field(child, "name");
        if (cbm_source_is(name, "user_type") ||
            !(swift_modifier_flags(walk, child) & CBM_SOURCE_DECL_F_PRIVATE))
            continue;
        const char *text = cbm_source_text(walk, name);
        if (text && !names)
            names = cbm_ht_create(8);
        if (text && names)
            cbm_ht_set(names, text, (void *)text);
    }
    return names;
}

static void swift_collect(cbm_source_walk_t *walk) {
    const char *module = swift_module_of(walk, walk->file);
    const char *owner = swift_module_owner(walk, module);
    if (!owner)
        return;
    CBMHashTable *file_private = swift_file_private_types(walk);
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (cbm_source_is(child, "function_declaration")) {
            swift_collect_function(walk, owner, child, NULL, 0);
        } else if (cbm_source_is(child, "property_declaration")) {
            swift_collect_property(walk, owner, child, NULL, 0);
        } else if (cbm_source_is(child, "class_declaration") ||
                   cbm_source_is(child, "protocol_declaration")) {
            swift_collect_type(walk, module, NULL, child, file_private);
        } else if (cbm_source_is(child, "typealias_declaration")) {
            swift_collect_alias(walk, owner, child);
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
    if (file_private)
        cbm_ht_free(file_private);
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

/* Matches written arguments to parameters in order. `skip_to_function`: a trailing closure
 * passes over a defaulted parameter whose written type is not a function, as the compiler's
 * forward scan does. A type an alias names cannot be told from a function here, so a caller
 * also tries without it. */
static bool swift_args_fit_as(const cbm_source_decl_t *decl, const swift_args_t *args,
                              bool skip_to_function) {
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
        bool defaulted = (param->flags & CBM_SOURCE_PARAM_OPTIONAL) != 0;
        bool passes = skip_to_function && defaulted && !(param->flags & SWIFT_PARAM_FUNCTION);
        if (next == args->count && trailing > 0 && !passes) {
            trailing--;
            continue;
        }
        if (param->flags & (CBM_SOURCE_PARAM_OPTIONAL | CBM_SOURCE_PARAM_VARIADIC))
            continue;
        return false;
    }
    return next == args->count && trailing == 0;
}

static bool swift_args_fit(const cbm_source_decl_t *decl, const swift_args_t *args) {
    if (!args || args->too_many || decl->param_count < 0)
        return true;
    return swift_args_fit_as(decl, args, true) || swift_args_fit_as(decl, args, false);
}

typedef struct {
    const cbm_source_decl_t *first;
    bool testable, same_module;
} swift_overloads_t;

static bool swift_same_node(const cbm_source_decl_t *a, const cbm_source_decl_t *b) {
    return a->qn && b->qn && !strcmp(a->qn, b->qn);
}

static bool swift_args_excluded(swift_ctx_t *ctx, const cbm_source_decl_t *decl,
                                const swift_args_t *args, int *exact);

/* Candidates that share labels and differ in graph node: what their parameter types decide. */
static int swift_settle(swift_ctx_t *ctx, const swift_overloads_t *sets, size_t set_count,
                        const swift_args_t *args, const cbm_source_decl_t **out) {
    if (!args || args->too_many)
        return -1;
    const cbm_source_decl_t *survivor = NULL, *best = NULL;
    bool several = false, tied = false;
    int best_exact = 0;
    for (size_t s = 0; s < set_count; s++) {
        for (const cbm_source_decl_t *decl = sets[s].first; decl; decl = decl->next) {
            if (!swift_visible(ctx, decl, sets[s].testable, sets[s].same_module) ||
                !swift_args_fit(decl, args) || (decl->flags & CBM_SOURCE_DECL_F_VALUE))
                continue;
            int exact = 0;
            if (swift_args_excluded(ctx, decl, args, &exact))
                continue;
            several |= survivor && !swift_same_node(survivor, decl);
            if (!survivor)
                survivor = decl;
            if (exact > best_exact) {
                best = decl;
                best_exact = exact;
                tied = false;
            } else if (exact && exact == best_exact && !swift_same_node(best, decl)) {
                tied = true;
            }
        }
    }
    /* Nothing takes these arguments in code that compiles, so the analysis missed something. */
    if (!survivor)
        return -1;
    if (several && (!best || tied))
        return -1;
    *out = several ? best : survivor;
    return 1;
}

/* The one graph node these declarations select for these arguments. 0: none fits. */
static int swift_select(swift_ctx_t *ctx, const cbm_source_decl_t *first, const swift_args_t *args,
                        bool testable, bool same_module, const cbm_source_decl_t **out) {
    const cbm_source_decl_t *selected = NULL;
    bool split = false;
    /* `T.method(.post)` beside `var method: M`: what is called is the function. */
    bool function_fits = false;
    for (const cbm_source_decl_t *decl = first; args && decl && !function_fits; decl = decl->next)
        function_fits = !(decl->flags & CBM_SOURCE_DECL_F_VALUE) &&
                        swift_visible(ctx, decl, testable, same_module) &&
                        swift_args_fit(decl, args);
    /* `var m` beside `func m(x: Int)`, named with no call: what is read is the value. */
    bool value_fits = false;
    for (const cbm_source_decl_t *decl = first; !args && decl && !value_fits; decl = decl->next)
        value_fits = (decl->flags & CBM_SOURCE_DECL_F_VALUE) &&
                     swift_visible(ctx, decl, testable, same_module);
    for (const cbm_source_decl_t *decl = first; decl; decl = decl->next) {
        if (!swift_visible(ctx, decl, testable, same_module) || !swift_args_fit(decl, args))
            continue;
        if (function_fits && (decl->flags & CBM_SOURCE_DECL_F_VALUE))
            continue;
        if (value_fits && !(decl->flags & CBM_SOURCE_DECL_F_VALUE))
            continue;
        if (selected) {
            bool both_values = (selected->flags & CBM_SOURCE_DECL_F_VALUE) &&
                               (decl->flags & CBM_SOURCE_DECL_F_VALUE);
            /* One name twice can only be two branches of `#if`. Which one a build reads is
             * unknown, so the name has a type only when both branches give it the same one. */
            bool same_type = (!selected->type && !decl->type) ||
                             (selected->type && decl->type && !strcmp(selected->type, decl->type));
            if (both_values && !same_type)
                return -1;
            split |= !both_values && !swift_same_node(selected, decl);
            continue;
        }
        selected = decl;
    }
    if (split) {
        swift_overloads_t set = {first, testable, same_module};
        return swift_settle(ctx, &set, 1, args, out);
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

/* Who declares a type name, seen from a declaration's owner: nested types from inside out, then
 * the module, then its imports. NULL with *conflict false: nothing in the project does. */
static const cbm_source_decl_t *swift_find_type_name(swift_ctx_t *ctx, const char *owner,
                                                     const char *head, bool *conflict) {
    cbm_source_walk_t *walk = ctx->walk;
    *conflict = false;
    const char *name_key =
        head ? cbm_arena_sprintf(walk->arena, "%s%s", SWIFT_TYPE_NAME, head) : NULL;
    if (!name_key) {
        *conflict = true;
        return NULL;
    }
    const char *module = swift_module_from_key(walk, owner);
    const cbm_source_decl_t *found = NULL;
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
            if (imported && found && imported != found) {
                *conflict = true;
                return NULL;
            }
            if (imported)
                found = imported;
        }
    }
    return found;
}

/* A written type no project declaration can be: its head names nothing here. */
static bool swift_written_outside(swift_ctx_t *ctx, const char *owner, const char *text) {
    if (!owner || !text || swift_initializer_marked(text))
        return false;
    const char *dot = strchr(text, '.');
    const char *head = dot ? cbm_arena_strndup(ctx->walk->arena, text, (size_t)(dot - text)) : text;
    if (!head || !strcmp(head, "Self") || swift_list_has(ctx->type_params, head))
        return false;
    bool conflict = false;
    return !swift_find_type_name(ctx, owner, head, &conflict) && !conflict;
}

static const char *swift_lookup_type(swift_ctx_t *ctx, const char *owner, const char *text);

/* A type no project declaration names, which an extension in this module or an imported one
 * adds to. NULL: nothing here extends it. */
static const char *swift_extended_type(swift_ctx_t *ctx, const char *owner, const char *head) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *module = swift_module_from_key(walk, owner);
    const char *key = swift_nested_key(walk, module, NULL, head);
    if (key && cbm_source_lookup(walk, key, SWIFT_TYPE_RECORD))
        return key;
    size_t count = 0;
    const swift_import_t *imports = swift_imports_of(ctx, module, &count);
    for (size_t i = 0; i < count; i++) {
        key = swift_nested_key(walk, imports[i].module, NULL, head);
        if (key && cbm_source_lookup(walk, key, SWIFT_TYPE_RECORD))
            return key;
    }
    return NULL;
}

/* What an alias declared by `found` names, as written. */
static const char *swift_alias_target(swift_ctx_t *ctx, const cbm_source_decl_t *found,
                                      const char *name) {
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", SWIFT_ALIAS_NAME, name);
    const cbm_source_decl_t *alias = key ? cbm_source_lookup(ctx->walk, found->owner, key) : NULL;
    return alias && !alias->next ? alias->type : NULL;
}

/* An alias names one type or another alias, and code that compiles has no cycle of them. */
static const char *swift_lookup_alias(swift_ctx_t *ctx, const char *owner, const char *written) {
    ctx->alias_depth++;
    const char *key = swift_lookup_type(ctx, owner, written);
    ctx->alias_depth--;
    return key;
}

/* A written type name, seen from a declaration's owner. */
static const char *swift_lookup_type(swift_ctx_t *ctx, const char *owner, const char *text) {
    if (!owner || !text)
        return NULL;
    cbm_source_walk_t *walk = ctx->walk;
    const char *dot = strchr(text, '.');
    const char *head = dot ? cbm_arena_strndup(walk->arena, text, (size_t)(dot - text)) : text;
    if (!head)
        return NULL;
    if (!strcmp(head, "Self") && !strncmp(owner, "t:", 2))
        return dot ? NULL : owner;
    bool conflict = false;
    const cbm_source_decl_t *found = swift_find_type_name(ctx, owner, head, &conflict);
    if (found)
        found = swift_visible_type(ctx, found, true, true);
    if (found && !found->type && !dot) {
        const char *written = swift_alias_target(ctx, found, head);
        return written && written[0] != SWIFT_CLOSURE_MARK && ctx->alias_depth < 8
                   ? swift_lookup_alias(ctx, found->owner, written)
                   : NULL;
    }
    if (!found && !conflict && !dot)
        return swift_extended_type(ctx, owner, head);
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

/* `extension Session` in another module is keyed by that module, so the name the extension
 * spells decides which type it adds to. The file's own module may add to an imported type. */
static const char *swift_type_in_this_module(swift_ctx_t *ctx, const char *type_key) {
    const char *hash = strchr(type_key, '#');
    if (!hash || !strncmp(type_key + 2, ctx->module, (size_t)(hash - type_key - 2)))
        return NULL;
    return swift_nested_key(ctx->walk, ctx->module, NULL, hash + 1);
}

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
        /* This module's extension of an imported type competes with what the type declares. */
        const char *here = !same_module ? swift_type_in_this_module(ctx, type_key) : NULL;
        const cbm_source_decl_t *added = here ? cbm_source_lookup(ctx->walk, here, name) : NULL;
        if (added) {
            const cbm_source_decl_t *found = NULL;
            int fits = swift_select(ctx, added, args, true, true, &found);
            bool values = fits > 0 && selected > 0 && (found->flags & CBM_SOURCE_DECL_F_VALUE) &&
                          ((*out)->flags & CBM_SOURCE_DECL_F_VALUE);
            if (values) {
                /* A property of one name and type here shadows the imported one. Two types are
                 * for the expected type to tell apart, which this lookup does not have. */
                bool same_type = (*out)->type && found->type && !strcmp((*out)->type, found->type);
                selected = same_type ? 1 : -1;
                if (same_type)
                    *out = found;
            } else if (fits != 0 && selected != 0) {
                swift_overloads_t sets[] = {{own, testable, same_module}, {added, true, true}};
                selected = swift_settle(ctx, sets, 2, args, out);
            } else if (fits != 0) {
                selected = fits;
                *out = found;
            }
        }
        if (selected < 0)
            return SWIFT_AMBIGUOUS;
        if (selected > 0)
            return SWIFT_FOUND;
        /* With no known module, another file's member is neither seen nor ruled out. */
        if (own && !module[0])
            return SWIFT_OPEN;
    }
    /* Only extensions here: what the extension spells is declared elsewhere, or outside. */
    bool extension_only = true;
    for (const cbm_source_decl_t *record = records; record && extension_only; record = record->next)
        extension_only = (record->flags & CBM_SOURCE_DECL_F_ADAPTED) != 0;
    bool declared = false;
    if (extension_only && !bases_only) {
        const char *hash = strchr(type_key, '#');
        const char *owner = swift_module_owner(ctx->walk, module);
        const char *spelled = hash && owner ? swift_lookup_type(ctx, owner, hash + 1) : NULL;
        if (spelled && strcmp(spelled, type_key) && strncmp(spelled, type_key, 2) == 0) {
            const cbm_source_decl_t *spelled_records =
                cbm_source_lookup(ctx->walk, spelled, SWIFT_TYPE_RECORD);
            swift_lookup_t step = swift_find_member(ctx, spelled, name, args, false, chain, out);
            if (step != SWIFT_ABSENT)
                return step;
            declared = spelled_records && !(spelled_records->flags & CBM_SOURCE_DECL_F_ADAPTED);
        }
        /* The same outside type, extended by a module this file imports. */
        for (size_t i = 0; hash && i < ctx->import_count; i++) {
            const char *there = swift_nested_key(ctx->walk, ctx->imports[i].module, NULL, hash + 1);
            if (!there || !strcmp(there, type_key) ||
                !cbm_source_lookup(ctx->walk, there, SWIFT_TYPE_RECORD))
                continue;
            const cbm_source_decl_t *found = NULL;
            swift_lookup_t step = swift_find_member(ctx, there, name, args, false, chain, &found);
            if (step == SWIFT_FOUND) {
                *out = found;
                return step;
            }
        }
    }
    bool open = false;
    swift_lookup_t result = SWIFT_ABSENT;
    /* An extension of a type outside the project has no declaration here. */
    const char *base_owner = type_key;
    for (const cbm_source_decl_t *record = records; record; record = record->next) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        declared |= !(record->flags & CBM_SOURCE_DECL_F_ADAPTED);
        open |= (record->flags & CBM_SOURCE_DECL_F_OPEN) != 0;
        /* Past the chain's bound every further base answers open, and the answer is open now. */
        if (open && chain->overflow)
            break;
        for (const char *const *base = record->bases; base && *base; base++) {
            const char *parent = swift_lookup_type(ctx, base_owner, *base);
            if (!parent) {
                open |= !swift_closed_base(*base, name);
                continue;
            }
            const cbm_source_decl_t *found = NULL;
            swift_lookup_t step = swift_find_member(ctx, parent, name, args, false, chain, &found);
            /* `extension JSONDecoder: DataDecoder {}`: a type outside the project meets the
             * protocol with members of its own, which hide what the protocol names. */
            if (step == SWIFT_FOUND && extension_only && !declared) {
                open = true;
            } else if (step == SWIFT_FOUND) {
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

static swift_ref_t swift_member(swift_ctx_t *ctx, const swift_ref_t *receiver, const char *name,
                                const swift_args_t *args);
static const char *swift_declared_type(swift_ctx_t *ctx, const cbm_source_decl_t *decl);

/* Overloads in one file share a node, so selecting the node does not say which of them runs. */
static bool swift_overloads_return_differently(swift_ctx_t *ctx, const cbm_source_decl_t *decl,
                                               const swift_args_t *args) {
    for (const cbm_source_decl_t *other = cbm_source_lookup(ctx->walk, decl->owner, decl->name);
         other; other = other->next) {
        if (other == decl || !swift_same_node(other, decl) || !swift_args_fit(other, args))
            continue;
        if ((!other->type) != (!decl->type) ||
            (other->type && decl->type && strcmp(other->type, decl->type)))
            return true;
    }
    return false;
}

/* `let x = A.b` is what b is, and `let x = A.b()` what b returns. A type's property needs a type
 * to be inferred from, so code that compiles has no cycle of these. */
static const char *swift_initializer_type(swift_ctx_t *ctx, const cbm_source_decl_t *decl) {
    const char *text = decl->type + 1;
    const char *dot = strrchr(text, '.');
    if (!dot || ctx->initializer_depth >= SWIFT_MAX_INITIALIZER_CHAIN)
        return NULL;
    const char *head = cbm_arena_strndup(ctx->walk->arena, text, (size_t)(dot - text));
    const char *key = head ? swift_lookup_type(ctx, decl->owner, head) : NULL;
    if (!key)
        return NULL;
    bool called = decl->type[0] == SWIFT_INITIALIZER_CALL;
    swift_ref_t receiver = {.kind = SWIFT_TYPE, .type_key = key};
    ctx->initializer_depth++;
    swift_ref_t found = swift_member(ctx, &receiver, dot + 1, NULL);
    const char *type = NULL;
    if (found.kind == SWIFT_VALUE)
        type = called ? NULL : found.type_key;
    else if (found.kind == SWIFT_FUNCTION && called &&
             !swift_overloads_return_differently(ctx, found.decl, NULL))
        type = swift_declared_type(ctx, found.decl);
    else if (found.kind == SWIFT_TYPE && called)
        type = found.type_key;
    ctx->initializer_depth--;
    return type;
}

static const char *swift_declared_type(swift_ctx_t *ctx, const cbm_source_decl_t *decl) {
    if (swift_initializer_marked(decl->type))
        return swift_initializer_type(ctx, decl);
    return decl->type ? swift_lookup_type(ctx, decl->owner, decl->type) : NULL;
}

static swift_ref_t swift_ref_of_decl(swift_ctx_t *ctx, const cbm_source_decl_t *decl) {
    swift_ref_t ref = {.reason = "swift_name_not_in_project"};
    if (decl->flags & CBM_SOURCE_DECL_F_VALUE) {
        const char *type = swift_declared_type(ctx, decl);
        ref.kind = type ? SWIFT_VALUE : SWIFT_UNKNOWN;
        ref.type_key = type;
        ref.value = decl;
        ref.outside = !type && swift_written_outside(ctx, decl->owner, decl->type);
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

/* What a call returns when the overloads that take its arguments cannot be told apart and each
 * returns the same type. The call itself stays undecided; what follows it does not. */
static const char *swift_common_result(swift_ctx_t *ctx, const char *type_key, const char *name,
                                       const swift_args_t *args) {
    if (!args || args->too_many)
        return NULL;
    const char *common = NULL;
    const char *module = swift_module_from_key(ctx->walk, type_key);
    const char *keys[] = {
        type_key, strcmp(module, ctx->module) ? swift_type_in_this_module(ctx, type_key) : NULL};
    for (size_t k = 0; k < sizeof(keys) / sizeof(keys[0]); k++) {
        for (const cbm_source_decl_t *decl = keys[k] ? cbm_source_lookup(ctx->walk, keys[k], name)
                                                     : NULL;
             decl; decl = decl->next) {
            if (!swift_visible(ctx, decl, true, true) || !swift_args_fit(decl, args))
                continue;
            const char *type =
                (decl->flags & CBM_SOURCE_DECL_F_VALUE) ? NULL : swift_declared_type(ctx, decl);
            if (!type || (common && strcmp(common, type)))
                return NULL;
            common = type;
        }
    }
    return common;
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

/* The name of an outside type that an extension here adds to. It has no declaration to be
 * the target of an edge, but its members are known. */
static swift_ref_t swift_extended_name(swift_ctx_t *ctx, const char *name) {
    const char *key = swift_extended_type(
        ctx, ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module), name);
    return key ? (swift_ref_t){.kind = SWIFT_TYPE, .type_key = key, .reason = "swift_outside_type"}
               : (swift_ref_t){.reason = "swift_name_not_in_project"};
}

static swift_ref_t swift_name(swift_ctx_t *ctx, const char *name, const swift_args_t *args) {
    swift_ref_t ref = {.reason = "swift_name_not_in_project"};
    const swift_local_t *local = swift_find_local(ctx, name);
    if (local) {
        ref.kind = local->type_key ? SWIFT_VALUE : SWIFT_UNKNOWN;
        ref.type_key = local->type_key;
        ref.outside = local->outside;
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
            if (level.kind == SWIFT_TYPE)
                return level;
            swift_ref_t extended = swift_extended_name(ctx, name);
            return extended.kind == SWIFT_TYPE ? extended : ref;
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
    swift_ref_t extended = swift_extended_name(ctx, name);
    if (extended.kind == SWIFT_TYPE)
        return extended;
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
        /* A type's name reaches its static members and cases, not an instance property. What it
         * names is then something this table does not hold, like a case of an outside enum. */
        if (receiver->kind == SWIFT_TYPE && (member->flags & CBM_SOURCE_DECL_F_VALUE) &&
            !(member->flags & CBM_SOURCE_DECL_F_STATIC)) {
            ref.reason = "swift_instance_member_through_a_type";
            return ref;
        }
        ref = swift_ref_of_decl(ctx, member);
        ref.via_receiver = true;
        return ref;
    case SWIFT_OPEN:
        ref.reason = "swift_member_chain_leaves_project";
        return ref;
    case SWIFT_AMBIGUOUS:
        ref.reason = "swift_overload_not_decided";
        ref.common_result = swift_common_result(ctx, receiver->type_key, name, args);
        return ref;
    case SWIFT_ABSENT:
        break;
    }
    ref.reason = "swift_member_absent_from_chain";
    return ref;
}

/* `.foo` names a static member, a case or a nested type of what the context states. */
static swift_ref_t swift_implicit_member(swift_ctx_t *ctx, const char *type_key, const char *name,
                                         const swift_args_t *args) {
    swift_ref_t context = {.kind = SWIFT_TYPE, .type_key = type_key};
    swift_ref_t ref = swift_member(ctx, &context, name, args);
    if (ref.kind == SWIFT_FUNCTION && !(ref.decl->flags & CBM_SOURCE_DECL_F_STATIC))
        return (swift_ref_t){.reason = "swift_instance_member_through_a_type"};
    return ref;
}

/* ── Phase two: expressions ────────────────────────────────────────── */

static void swift_read_args(swift_ctx_t *ctx, TSNode call, swift_args_t *args) {
    *args = (swift_args_t){0};
    TSNode suffix = swift_child_of_kind(call, "call_suffix");
    if (ts_node_is_null(suffix))
        suffix = swift_child_of_kind(call, "constructor_suffix");
    uint32_t count = ts_node_is_null(suffix) ? 0 : ts_node_named_child_count(suffix);
    for (uint32_t i = 0; i < count; i++) {
        TSNode part = ts_node_named_child(suffix, i);
        if (cbm_source_is(part, "lambda_literal") || cbm_source_is(part, "annotated_lambda")) {
            if (!args->trailing)
                args->first_trailing = part;
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
                args->values[args->count] = cbm_source_field(value, "value");
                args->labels[args->count++] =
                    ts_node_is_null(label) ? NULL : cbm_source_text(ctx->walk, label);
            }
        }
    }
}

/* The `.foo` an expression starts from, through a call or a chain on it: `.a(1).b`. */
static TSNode swift_implicit_head(TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 64)
        return (TSNode){0};
    if (cbm_source_is(node, "prefix_expression"))
        return cbm_source_token(node, ".") ? node : (TSNode){0};
    if (cbm_source_is(node, "call_expression"))
        return swift_implicit_head(ts_node_named_child(node, 0), depth + 1);
    if (cbm_source_is(node, "navigation_expression"))
        return swift_implicit_head(cbm_source_field(node, "target"), depth + 1);
    return (TSNode){0};
}

static const char *swift_byte_key(swift_ctx_t *ctx, TSNode node) {
    return cbm_arena_sprintf(ctx->walk->arena, "%u:%u", ts_node_start_byte(node),
                             ts_node_end_byte(node));
}

static const char *swift_expected_at(swift_ctx_t *ctx, TSNode head) {
    const char *key = swift_byte_key(ctx, head);
    return key ? cbm_ht_get(ctx->expected, key) : NULL;
}

/* Remembers what the context expects of a value, if the value starts from `.foo`. */
static void swift_note_expected(swift_ctx_t *ctx, TSNode value, const char *type_key) {
    TSNode head = type_key ? swift_implicit_head(value, 0) : (TSNode){0};
    const char *key = ts_node_is_null(head) ? NULL : swift_byte_key(ctx, head);
    if (key)
        cbm_ht_set(ctx->expected, key, (void *)type_key);
}

/* The type a written type names, seen from where the walk is. */
static const char *swift_key_of_written(swift_ctx_t *ctx, const char *type) {
    if (!type || swift_type_param_text(ctx->type_params, type))
        return NULL;
    return swift_lookup_type(
        ctx, ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module), type);
}

static swift_ref_t swift_expr_type_name(swift_ctx_t *ctx, TSNode node) {
    const char *name = cbm_source_text(ctx->walk, node);
    return name ? swift_name(ctx, name, NULL) : (swift_ref_t){.reason = "swift_computed_receiver"};
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
        } else if (callee.kind == SWIFT_VALUE && callee.value && callee.value->param_count > 0) {
            /* A case with its payload is a value of its enum. */
            result.kind = SWIFT_VALUE;
            result.type_key = callee.type_key;
        } else if (callee.common_result) {
            result.kind = SWIFT_VALUE;
            result.type_key = callee.common_result;
        } else if (callee.kind == SWIFT_FUNCTION) {
            bool split = swift_overloads_return_differently(ctx, callee.decl, &call_args);
            const char *type = split ? NULL : swift_declared_type(ctx, callee.decl);
            result.kind = type ? SWIFT_VALUE : SWIFT_UNKNOWN;
            result.type_key = type;
            result.outside = !split && !type &&
                             swift_written_outside(ctx, callee.decl->owner, callee.decl->type);
        } else if (cbm_source_is(ts_node_named_child(node, 0), "simple_identifier") &&
                   callee.kind == SWIFT_UNKNOWN && !callee.outside) {
            /* `URLRequest(url: u)`: a type's name, and no declaration here has it. */
            const char *name = cbm_source_text(ctx->walk, ts_node_named_child(node, 0));
            bool written_like_a_type = name && name[0] >= 'A' && name[0] <= 'Z';
            result.outside =
                written_like_a_type && !swift_find_local(ctx, name) &&
                swift_written_outside(
                    ctx, ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module),
                    name);
        }
        return result;
    }
    if (cbm_source_is(node, "macro_invocation")) {
        /* Swift Testing's `try #require(x)` is x, or the test stops there. */
        const char *macro =
            cbm_source_text(ctx->walk, swift_child_of_kind(node, "simple_identifier"));
        if (!ctx->imports_testing || !macro || strcmp(macro, "require"))
            return unknown;
        swift_args_t macro_args;
        swift_read_args(ctx, node, &macro_args);
        return macro_args.count > 0 ? swift_expr(ctx, macro_args.values[0], NULL, depth + 1)
                                    : unknown;
    }
    if (cbm_source_is(node, "constructor_expression")) {
        /* `Box<Int>()`: the generic form of a call of a type. */
        const char *written =
            swift_type_text(ctx->walk, cbm_source_field(node, "constructed_type"), 0);
        const char *key = swift_key_of_written(ctx, written);
        if (key)
            return (swift_ref_t){.kind = SWIFT_VALUE, .type_key = key};
        unknown.outside = swift_written_outside(
            ctx, ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module),
            written);
        return unknown;
    }
    if (cbm_source_is(node, "prefix_expression") && cbm_source_token(node, ".")) {
        /* `.foo`: a member of what the context expects. Optional's own cases are not. */
        const char *member = cbm_source_text(ctx->walk, cbm_source_field(node, "target"));
        const char *type = member ? swift_expected_at(ctx, node) : NULL;
        if (!type || !strcmp(member, "none") || !strcmp(member, "some"))
            return unknown;
        return swift_implicit_member(ctx, type, member, args);
    }
    if (cbm_source_is(node, "key_path_expression")) {
        /* `\\Foo.bar` names a member of instances of Foo. `\\.bar` depends on the context. */
        TSNode root = ts_node_named_child(node, 0);
        if (!cbm_source_is(root, "type_identifier"))
            return unknown;
        swift_ref_t type = swift_expr_type_name(ctx, root);
        return type.kind == SWIFT_TYPE && type.type_key
                   ? (swift_ref_t){.kind = SWIFT_VALUE, .type_key = type.type_key}
                   : unknown;
    }
    if (cbm_source_is(node, "as_expression")) {
        /* The form `x as? T` is T? and `x as! T` and `x as T` are T; members select the same. */
        const char *type = swift_type_text(ctx->walk, cbm_source_field(node, "name"), 0);
        const char *key = swift_key_of_written(ctx, type);
        return key ? (swift_ref_t){.kind = SWIFT_VALUE, .type_key = key} : unknown;
    }
    if (cbm_source_is(node, "tuple_expression") && ts_node_named_child_count(node) == 1)
        return swift_expr(ctx, cbm_source_field(node, "value"), NULL, depth + 1);
    if (cbm_source_is(node, "ternary_expression") ||
        cbm_source_is(node, "nil_coalescing_expression")) {
        /* Both arms name the type: one arm alone could be a subclass of the common type. */
        bool ternary = cbm_source_is(node, "ternary_expression");
        swift_ref_t left =
            swift_expr(ctx, cbm_source_field(node, ternary ? "if_true" : "value"), NULL, depth + 1);
        swift_ref_t right = swift_expr(ctx, cbm_source_field(node, ternary ? "if_false" : "if_nil"),
                                       NULL, depth + 1);
        return left.kind == SWIFT_VALUE && right.kind == SWIFT_VALUE && left.type_key &&
                       right.type_key && !strcmp(left.type_key, right.type_key)
                   ? (swift_ref_t){.kind = SWIFT_VALUE, .type_key = left.type_key}
                   : unknown;
    }
    if (cbm_source_is(node, "postfix_expression"))
        return swift_expr(ctx, cbm_source_field(node, "target"), NULL, depth + 1);
    if (cbm_source_is(node, "try_expression"))
        return swift_expr(ctx, cbm_source_field(node, "expr"), NULL, depth + 1);
    if (cbm_source_is(node, "await_expression"))
        return swift_expr(ctx, cbm_source_field(node, "expr"), NULL, depth + 1);
    return unknown;
}

static void swift_bind_as(swift_ctx_t *ctx, const char *name, const char *type_key, bool outside) {
    swift_local_t *local = name ? cbm_arena_alloc(ctx->walk->arena, sizeof(*local)) : NULL;
    if (!local)
        return;
    *local = (swift_local_t){name, type_key, outside, ctx->locals};
    ctx->locals = local;
}

static void swift_bind(swift_ctx_t *ctx, const char *name, const char *type_key) {
    swift_bind_as(ctx, name, type_key, false);
}

/* What an argument denotes. Each argument is worked out once, so nested calls that all have
 * overloads stay linear. */
static const swift_ref_t *swift_argument_ref(swift_ctx_t *ctx, TSNode value) {
    const char *key = swift_byte_key(ctx, value);
    swift_ref_t *held = key ? cbm_ht_get(ctx->argument_refs, key) : NULL;
    if (held)
        return held;
    held = cbm_arena_alloc(ctx->walk->arena, sizeof(*held));
    if (!held)
        return NULL;
    /* Until it is worked out: an argument that reaches itself denotes nothing known. */
    *held = (swift_ref_t){.reason = "swift_computed_receiver"};
    if (key)
        cbm_ht_set(ctx->argument_refs, key, held);
    *held = swift_expr(ctx, value, NULL, 0);
    return held;
}

static bool swift_inherits(swift_ctx_t *ctx, const char *type_key, const char *ancestor,
                           cbm_source_chain_t *chain) {
    if (!strcmp(type_key, ancestor))
        return true;
    if (!cbm_source_chain_first_visit(chain, type_key))
        return false;
    for (const cbm_source_decl_t *record =
             cbm_source_lookup(ctx->walk, type_key, SWIFT_TYPE_RECORD);
         record; record = record->next) {
        for (const char *const *base = record->bases; base && *base; base++) {
            const char *parent = swift_lookup_type(ctx, type_key, *base);
            if (parent && swift_inherits(ctx, parent, ancestor, chain))
                return true;
        }
    }
    return false;
}

/* Whether a type can take a literal: it conforms to the protocol, or to one this table cannot
 * read. The standard's closed protocols do not refine the literal ones. */
static bool swift_takes_literal(swift_ctx_t *ctx, const char *type_key, const char *protocol,
                                cbm_source_chain_t *chain) {
    if (!cbm_source_chain_first_visit(chain, type_key))
        return chain->overflow;
    const cbm_source_decl_t *records = cbm_source_lookup(ctx->walk, type_key, SWIFT_TYPE_RECORD);
    if (!records)
        return true;
    for (const cbm_source_decl_t *record = records; record; record = record->next) {
        if (record->flags & CBM_SOURCE_DECL_F_OPEN)
            return true;
        for (const char *const *base = record->bases; base && *base; base++) {
            if (!strcmp(*base, protocol))
                return true;
            const char *parent = swift_lookup_type(ctx, type_key, *base);
            if (parent ? swift_takes_literal(ctx, parent, protocol, chain)
                       : !swift_closed_base(*base, ""))
                return true;
        }
    }
    return false;
}

typedef enum { SWIFT_ARG_ANY, SWIFT_ARG_EXACT, SWIFT_ARG_NO } swift_arg_fit_t;

/* Only what the compiler would refuse counts as a refusal; the rest stays possible. */
static swift_arg_fit_t swift_argument_against(swift_ctx_t *ctx, TSNode value,
                                              const cbm_source_decl_t *decl,
                                              const cbm_source_param_t *param) {
    if (ts_node_is_null(value) || !param->type)
        return SWIFT_ARG_ANY;
    const char *wanted = swift_lookup_type(ctx, decl->owner, param->type);
    if (!wanted)
        return SWIFT_ARG_ANY;
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, wanted, SWIFT_TYPE_RECORD);
    bool protocol = record && (record->flags & CBM_SOURCE_DECL_F_INTERFACE);
    TSNode head = swift_implicit_head(value, 0);
    if (!ts_node_is_null(head)) {
        const char *member = cbm_source_text(ctx->walk, cbm_source_field(head, "target"));
        if (!member || !strcmp(member, "init"))
            return SWIFT_ARG_ANY;
        /* Only a concrete type, or a generic stand-in for one, has static members to offer. */
        if (protocol && !(param->flags & SWIFT_PARAM_OPAQUE))
            return SWIFT_ARG_NO;
        const char *name_key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", SWIFT_TYPE_NAME, member);
        if (name_key && cbm_source_lookup(ctx->walk, wanted, name_key))
            return SWIFT_ARG_ANY;
        const cbm_source_decl_t *found = NULL;
        cbm_source_chain_t chain = {0};
        /* Only the name counts here: which overload of it a call picks comes later. */
        swift_lookup_t step = swift_find_member(ctx, wanted, member, NULL, false, &chain, &found);
        return step == SWIFT_ABSENT ? SWIFT_ARG_NO : SWIFT_ARG_ANY;
    }
    const char *literal = cbm_source_is(value, "line_string_literal") ||
                                  cbm_source_is(value, "multi_line_string_literal") ||
                                  cbm_source_is(value, "raw_string_literal")
                              ? "ExpressibleByStringLiteral"
                          : cbm_source_is(value, "integer_literal") ? "ExpressibleByIntegerLiteral"
                          : cbm_source_is(value, "boolean_literal") ? "ExpressibleByBooleanLiteral"
                                                                    : NULL;
    if (literal) {
        cbm_source_chain_t chain = {0};
        return protocol || swift_takes_literal(ctx, wanted, literal, &chain) ? SWIFT_ARG_ANY
                                                                             : SWIFT_ARG_NO;
    }
    const swift_ref_t *ref = swift_argument_ref(ctx, value);
    if (ref && ref->kind == SWIFT_VALUE && ref->type_key) {
        if (!strcmp(ref->type_key, wanted))
            return SWIFT_ARG_EXACT;
        if (protocol)
            return SWIFT_ARG_ANY;
        cbm_source_chain_t chain = {0};
        return swift_inherits(ctx, ref->type_key, wanted, &chain) || chain.overflow ? SWIFT_ARG_ANY
                                                                                    : SWIFT_ARG_NO;
    }
    /* A type outside the project cannot be, or inherit from, one declared in it. */
    return ref && ref->outside && !protocol ? SWIFT_ARG_NO : SWIFT_ARG_ANY;
}

/* Walks a fitting candidate's parameters the way swift_args_fit does. */
static bool swift_args_excluded(swift_ctx_t *ctx, const cbm_source_decl_t *decl,
                                const swift_args_t *args, int *exact) {
    int next = 0;
    for (int p = 0; decl->param_count > 0 && p < decl->param_count && next < args->count; p++) {
        const cbm_source_param_t *param = &decl->params[p];
        bool unlabelled = !strcmp(param->name, "_");
        bool matches = unlabelled ? args->labels[next] == NULL
                                  : args->labels[next] && !strcmp(args->labels[next], param->name);
        if (!matches)
            continue;
        swift_arg_fit_t fit = swift_argument_against(ctx, args->values[next], decl, param);
        if (fit == SWIFT_ARG_NO)
            return true;
        *exact += fit == SWIFT_ARG_EXACT;
        next++;
        if (param->flags & CBM_SOURCE_PARAM_VARIADIC)
            while (next < args->count && !args->labels[next])
                next++;
    }
    return false;
}

/* A function type a parameter declares, directly or through an alias. */
static const char *swift_param_signature(swift_ctx_t *ctx, const cbm_source_decl_t *decl,
                                         const cbm_source_param_t *param) {
    if (!param->type)
        return NULL;
    if (param->type[0] == SWIFT_CLOSURE_MARK)
        return param->type;
    bool conflict = false;
    const cbm_source_decl_t *found = swift_find_type_name(ctx, decl->owner, param->type, &conflict);
    const char *written =
        found && !found->type ? swift_alias_target(ctx, found, param->type) : NULL;
    return written && written[0] == SWIFT_CLOSURE_MARK ? written : NULL;
}

static void swift_note_closure(swift_ctx_t *ctx, TSNode closure, const char *owner,
                               const char *signature) {
    const char *key = swift_byte_key(ctx, closure);
    swift_closure_t *held =
        key && signature ? cbm_arena_alloc(ctx->walk->arena, sizeof(*held)) : NULL;
    if (!held)
        return;
    *held = (swift_closure_t){owner, signature};
    cbm_ht_set(ctx->closures, key, held);
}

/* What the selected function's parameters say of its arguments: what a `.foo` is a member of,
 * and what a closure's parameters are. */
static void swift_note_argument_types(swift_ctx_t *ctx, const cbm_source_decl_t *decl,
                                      const swift_args_t *args) {
    if (!decl || args->too_many || decl->param_count <= 0)
        return;
    int next = 0;
    bool trailing_open = args->trailing > 0;
    for (int p = 0; p < decl->param_count; p++) {
        const cbm_source_param_t *param = &decl->params[p];
        bool unlabelled = !strcmp(param->name, "_");
        bool matches =
            next < args->count &&
            (unlabelled ? args->labels[next] == NULL
                        : args->labels[next] && !strcmp(args->labels[next], param->name));
        const char *signature = swift_param_signature(ctx, decl, param);
        if (matches) {
            TSNode value = args->values[next];
            if (signature && cbm_source_is(value, "lambda_literal"))
                swift_note_closure(ctx, value, decl->owner, signature);
            else if (param->type && !signature)
                swift_note_expected(ctx, value, swift_lookup_type(ctx, decl->owner, param->type));
            next++;
        } else if (trailing_open && next == args->count && signature) {
            swift_note_closure(ctx, args->first_trailing, decl->owner, signature);
            trailing_open = false;
        }
    }
}

/* What a call of a type says of its arguments: the initializer that takes them, or for a struct
 * with no initializer anywhere the stored property each label names. */
static void swift_note_init_arguments(swift_ctx_t *ctx, const char *type_key,
                                      const swift_args_t *args) {
    if (args->too_many)
        return;
    cbm_source_walk_t *walk = ctx->walk;
    if (cbm_source_lookup(walk, type_key, "init")) {
        const cbm_source_decl_t *selected = NULL;
        cbm_source_chain_t chain = {0};
        if (swift_find_member(ctx, type_key, "init", args, false, &chain, &selected) == SWIFT_FOUND)
            swift_note_argument_types(ctx, selected, args);
        return;
    }
    bool is_struct = false;
    for (const cbm_source_decl_t *record = cbm_source_lookup(walk, type_key, SWIFT_TYPE_RECORD);
         record; record = record->next)
        is_struct |= (record->flags & SWIFT_RECORD_STRUCT) != 0;
    if (!is_struct) {
        const cbm_source_decl_t *inherited = NULL;
        cbm_source_chain_t chain = {0};
        if (swift_find_member(ctx, type_key, "init", args, false, &chain, &inherited) ==
            SWIFT_FOUND)
            swift_note_argument_types(ctx, inherited, args);
        return;
    }
    for (int i = 0; i < args->count; i++) {
        const cbm_source_decl_t *stored =
            args->labels[i] ? cbm_source_lookup(walk, type_key, args->labels[i]) : NULL;
        if (stored && !stored->next && (stored->flags & CBM_SOURCE_DECL_F_VALUE))
            swift_note_expected(ctx, args->values[i], swift_declared_type(ctx, stored));
    }
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

/* A method name read without a call is left out: lookup merges it with inherited properties
 * and overloads, and only the expected type picks among them. */
static void swift_emit_reference(swift_ctx_t *ctx, TSNode node, const swift_ref_t *ref) {
    const cbm_source_decl_t *decl = ref->value && ref->value->qn ? ref->value
                                    : ref->kind == SWIFT_TYPE    ? ref->decl
                                                                 : NULL;
    if (!decl)
        return;
    const char *identity = NULL;
    cbm_source_decl_t single = *decl;
    single.next = NULL;
    const char *target = cbm_source_unique_qn(&single, &identity);
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, target,
                         ref->via_receiver ? CBM_RESOLVE_RULE_TYPE_MEMBER
                         : ref->via_import ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                           : CBM_RESOLVE_RULE_SCOPE_BINDING,
                         decl->owner, target ? ref->reason : identity);
}

/* A name read as a value or a type follows the same lookup as a call's. */
static void swift_visit_reference(swift_ctx_t *ctx, TSNode node) {
    const char *name = cbm_source_text(ctx->walk, node);
    if (!name)
        return;
    swift_ref_t ref = swift_name(ctx, name, NULL);
    swift_emit_reference(ctx, node, &ref);
}

/* `x.y` read: y on x's static type, reported at the spelling of y. */
static void swift_visit_member_read(swift_ctx_t *ctx, TSNode node) {
    TSNode member = cbm_source_field(cbm_source_field(node, "suffix"), "suffix");
    if (!cbm_source_is(member, "simple_identifier"))
        return;
    swift_ref_t ref = swift_expr(ctx, node, NULL, 0);
    swift_emit_reference(ctx, member, &ref);
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
    bool case_form =
        (cbm_source_is(node, "if_statement") || cbm_source_is(node, "guard_statement")) &&
        cbm_source_token(node, "case");
    bool seen_case_value = false;
    TSNode child = swift_cursor_named(&cursor, &more, &field);
    while (!ts_node_is_null(child)) {
        bool bound = field && !strcmp(field, "bound_identifier");
        /* A tuple's or a tuple type's label is spelled, not read. */
        bool label = field && !strcmp(field, "name") && cbm_source_is(child, "simple_identifier");
        /* `if case .b = z` keeps the case's name as a plain condition. */
        if (label) {
            /* Nothing to read. */
        } else if (case_form && cbm_source_is(child, "simple_identifier") && !seen_case_value) {
            seen_case_value = true;
        } else if (bound) {
            if (pending)
                swift_bind(ctx, pending, NULL);
            pending = cbm_source_text(ctx->walk, child);
        } else if (pending && field && !strcmp(field, "condition")) {
            swift_ref_t value = swift_expr(ctx, child, NULL, 0);
            swift_visit(ctx, child, depth + 1);
            swift_bind_as(ctx, pending, value.kind == SWIFT_VALUE ? value.type_key : NULL,
                          value.outside);
            pending = NULL;
        } else {
            if (pending) {
                /* `if let x` with no value rebinds x, keeping what it was. */
                swift_ref_t value = swift_name(ctx, pending, NULL);
                swift_bind_as(ctx, pending, value.kind == SWIFT_VALUE ? value.type_key : NULL,
                              value.outside);
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
        const char *owner =
            ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module);
        swift_bind_as(ctx, name, function_typed ? NULL : swift_key_of_written(ctx, type),
                      !function_typed && swift_written_outside(ctx, owner, type));
    }
}

/* A body of one expression returns it. */
static void swift_note_implicit_return(swift_ctx_t *ctx, TSNode body) {
    if (ts_node_is_null(body))
        return;
    TSNode statements =
        cbm_source_is(body, "statements") ? body : swift_child_of_kind(body, "statements");
    if (!ts_node_is_null(statements) && ts_node_named_child_count(statements) == 1)
        swift_note_expected(ctx, ts_node_named_child(statements, 0), ctx->return_key);
}

static void swift_visit_function(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    swift_local_t *saved = ctx->locals;
    const char *saved_params = ctx->type_params;
    bool saved_function = ctx->in_function;
    const char *saved_return = ctx->return_key;
    ctx->type_params = swift_type_params(ctx->walk, node, ctx->type_params);
    swift_bind_params(ctx, node);
    ctx->in_function = true;
    ctx->return_key = swift_key_of_written(ctx, swift_return_type(ctx->walk, node));
    swift_note_implicit_return(ctx, cbm_source_field(node, "body"));
    /* Parameter defaults run in the enclosing type; visit everything but the header. */
    uint32_t count = ts_node_named_child_count(node);
    const char *last_param_type = NULL;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *child_field = ts_node_field_name_for_named_child(node, i);
        if (child_field && !strcmp(child_field, "default_value"))
            swift_note_expected(ctx, child, swift_key_of_written(ctx, last_param_type));
        if (cbm_source_is(child, "parameter"))
            last_param_type = swift_param_type(ctx->walk, child, ctx->type_params);
        if (cbm_source_is(child, "parameter")) {
            uint32_t parts = ts_node_named_child_count(child);
            for (uint32_t p = 0; p < parts; p++)
                if (swift_type_kind(ts_node_named_child(child, p)))
                    swift_visit(ctx, ts_node_named_child(child, p), depth + 1);
            continue;
        }
        if (cbm_source_is(child, "modifiers") || cbm_source_is(child, "simple_identifier") ||
            cbm_source_is(child, "type_parameters") || cbm_source_is(child, "type_constraints"))
            continue;
        swift_visit(ctx, child, depth + 1);
    }
    ctx->in_function = saved_function;
    ctx->type_params = saved_params;
    ctx->locals = saved;
    ctx->return_key = saved_return;
}

static void swift_visit_type(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    TSNode name = cbm_source_field(node, "name");
    const char *text = cbm_source_is(name, "user_type") ? swift_type_text(ctx->walk, name, 0)
                                                        : cbm_source_text(ctx->walk, name);
    if (!text)
        return;
    /* A type declared in a body hides what the same name means outside it. */
    if (ctx->in_function)
        swift_bind(ctx, text, NULL);
    /* `extension Foo` names Foo, in the scope around it. */
    if (cbm_source_is(name, "user_type"))
        swift_visit(ctx, name, depth + 1);
    const char *saved = ctx->type_key;
    const char *saved_params = ctx->type_params;
    swift_local_t *saved_locals = ctx->locals;
    bool saved_function = ctx->in_function;
    ctx->type_key = saved ? swift_nested_key(ctx->walk, ctx->module, saved, text)
                          : swift_top_level_key(ctx->walk, ctx->module, ctx->file_private, text);
    ctx->type_params = swift_type_params(ctx->walk, node, NULL);
    ctx->locals = NULL;
    ctx->in_function = false;
    /* What the header names: bases, and the bounds and constraints of its parameters. */
    uint32_t parts = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < parts; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (cbm_source_is(part, "inheritance_specifier") ||
            cbm_source_is(part, "type_parameters") || cbm_source_is(part, "type_constraints"))
            swift_visit(ctx, part, depth + 1);
    }
    TSNode body = swift_body_of(node);
    if (!ts_node_is_null(body))
        swift_visit_children(ctx, body, depth + 1);
    ctx->type_key = saved;
    ctx->type_params = saved_params;
    ctx->locals = saved_locals;
    ctx->in_function = saved_function;
}

static void swift_visit_property(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    TSNode written = swift_child_of_kind(node, "type_annotation");
    const char *written_key =
        ts_node_is_null(written)
            ? NULL
            : swift_key_of_written(ctx, swift_type_text(ctx->walk, written, 0));
    swift_note_expected(ctx, cbm_source_field(node, "value"), written_key);
    /* `var x: T { .foo }`: the type also says what its body returns. */
    TSNode computed = swift_child_of_kind(node, "computed_property");
    if (!ts_node_is_null(computed)) {
        const char *saved_return = ctx->return_key;
        ctx->return_key = written_key;
        swift_note_implicit_return(ctx, computed);
        ctx->return_key = saved_return;
    }
    if (!ctx->in_function) {
        /* A member's name is in the table; binding it here would hide its type. */
        uint32_t members = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < members; i++) {
            TSNode part = ts_node_named_child(node, i);
            if (!cbm_source_is(part, "pattern") && !cbm_source_is(part, "modifiers"))
                swift_visit(ctx, part, depth + 1);
        }
        return;
    }
    TSNode value = cbm_source_field(node, "value");
    TSNode annotation = swift_child_of_kind(node, "type_annotation");
    const char *type =
        ts_node_is_null(annotation) ? NULL : swift_type_text(ctx->walk, annotation, 0);
    const char *key = swift_key_of_written(ctx, type);
    bool outside = swift_written_outside(
        ctx, ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module), type);
    if (ts_node_is_null(annotation) && !ts_node_is_null(value)) {
        swift_ref_t inferred = swift_expr(ctx, value, NULL, 0);
        if (inferred.kind == SWIFT_VALUE)
            key = inferred.type_key;
        outside = inferred.outside;
    }
    if (!ts_node_is_null(annotation))
        swift_visit(ctx, annotation, depth + 1);
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
            swift_bind_as(ctx, cbm_source_text(ctx->walk, ts_node_named_child(part, 0)), key,
                          outside);
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
    bool implicit = cbm_source_is(callee, "prefix_expression") && cbm_source_token(callee, ".");
    if (cbm_source_is(callee, "simple_identifier") ||
        cbm_source_is(callee, "navigation_expression") || implicit) {
        swift_ref_t ref = swift_expr(ctx, callee, &args, 0);
        swift_emit_call(ctx, node, &ref);
        if (ref.kind == SWIFT_FUNCTION)
            swift_note_argument_types(ctx, ref.decl, &args);
        else if (ref.kind == SWIFT_TYPE && ref.type_key)
            swift_note_init_arguments(ctx, ref.type_key, &args);
        else if (ref.value)
            swift_note_argument_types(ctx, ref.value, &args);
        /* A value called, an enum case with its payload or a closure property, is read. */
        TSNode spelled = implicit ? cbm_source_field(callee, "target")
                         : cbm_source_is(callee, "navigation_expression")
                             ? cbm_source_field(cbm_source_field(callee, "suffix"), "suffix")
                             : (TSNode){0};
        if (ref.value && cbm_source_is(spelled, "simple_identifier"))
            swift_emit_reference(ctx, spelled, &ref);
    }
    ctx->callee = callee;
    swift_visit_children(ctx, node, depth);
}

/* `Box<Int>(...)` names its type as a written type: one segment per nested type. */
static void swift_visit_constructor(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    swift_args_t args;
    swift_read_args(ctx, node, &args);
    TSNode type = cbm_source_field(node, "constructed_type");
    swift_ref_t ref = {.reason = "swift_computed_receiver"};
    if (cbm_source_is(type, "user_type")) {
        bool first = true;
        uint32_t parts = ts_node_named_child_count(type);
        for (uint32_t i = 0; i < parts; i++) {
            TSNode part = ts_node_named_child(type, i);
            const char *name =
                cbm_source_is(part, "type_identifier") ? cbm_source_text(ctx->walk, part) : NULL;
            if (!name)
                continue;
            ref = first ? swift_name(ctx, name, NULL) : swift_member(ctx, &ref, name, NULL);
            first = false;
        }
        swift_emit_call(ctx, node, &ref);
        if (ref.kind == SWIFT_TYPE && ref.type_key)
            swift_note_init_arguments(ctx, ref.type_key, &args);
    }
    swift_visit_children(ctx, node, depth);
}

/* Every name in a pattern binds here. One that was really a constant compared
 * against can only make a later use abstain. */
static void swift_bind_pattern(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "simple_identifier")) {
        swift_bind(ctx, cbm_source_text(ctx->walk, node), ctx->pattern_type);
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    /* `case .foo(let x):` names a member of the subject's type and binds only what is inside. */
    if (cbm_source_is(node, "pattern") && cbm_source_token(node, ".")) {
        TSNode spelled = swift_child_of_kind(node, "simple_identifier");
        const char *member = cbm_source_text(ctx->walk, spelled);
        if (member && ctx->subject_key && strcmp(member, "none") && strcmp(member, "some")) {
            swift_ref_t ref = swift_implicit_member(ctx, ctx->subject_key, member, NULL);
            swift_emit_reference(ctx, spelled, &ref);
        }
        for (uint32_t i = 0; i < count; i++)
            if (!ts_node_eq(ts_node_named_child(node, i), spelled))
                swift_bind_pattern(ctx, ts_node_named_child(node, i), depth + 1);
        return;
    }
    /* `catch let e as Failure` and `case let w as V:` name their value's type, and
     * `case is U:` names one too. */
    const char *saved = ctx->pattern_type;
    for (uint32_t i = 0; cbm_source_is(node, "pattern") && i < count; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (swift_type_kind(part)) {
            swift_visit(ctx, part, depth + 1);
            ctx->pattern_type = swift_key_of_written(ctx, swift_type_text(ctx->walk, part, 0));
        }
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (!swift_type_kind(part))
            swift_bind_pattern(ctx, part, depth + 1);
    }
    ctx->pattern_type = saved;
}

/* The type a closure's signature gives its nth parameter. */
static void swift_bind_closure_param(swift_ctx_t *ctx, const char *name, int index) {
    const char *written = NULL;
    const char *entry = ctx->closure ? ctx->closure->types + 1 : NULL;
    for (int i = 0; entry; i++) {
        const char *comma = strchr(entry, SWIFT_CLOSURE_SEPARATOR);
        if (!comma)
            break;
        if (i == index) {
            written = comma > entry
                          ? cbm_arena_strndup(ctx->walk->arena, entry, (size_t)(comma - entry))
                          : NULL;
            break;
        }
        entry = comma + 1;
    }
    swift_bind_as(ctx, name, written ? swift_lookup_type(ctx, ctx->closure->owner, written) : NULL,
                  written && swift_written_outside(ctx, ctx->closure->owner, written));
}

/* `$0` always means this closure's own first parameter, typed or not. */
static void swift_bind_shorthands(swift_ctx_t *ctx) {
    enum { SWIFT_SHORTHANDS = 4 };
    int count = SWIFT_SHORTHANDS;
    for (const char *p = ctx->closure ? ctx->closure->types : ""; *p; p++)
        count += *p == SWIFT_CLOSURE_SEPARATOR;
    for (int i = 0; i < count; i++)
        swift_bind_closure_param(ctx, cbm_arena_sprintf(ctx->walk->arena, "$%d", i), i);
}

static void swift_visit(swift_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "simple_identifier") || !strcmp(kind, "type_identifier")) {
        swift_visit_reference(ctx, node);
        return;
    }
    /* Spelled names, not reads. */
    if (!strcmp(kind, "navigation_suffix") || !strcmp(kind, "value_argument_label") ||
        !strcmp(kind, "attribute") || !strcmp(kind, "modifiers") ||
        !strcmp(kind, "import_declaration"))
        return;
    /* `.foo` is a member of the type the context expects, never a name in scope. */
    if (!strcmp(kind, "prefix_expression") && cbm_source_token(node, ".")) {
        if (!ts_node_eq(node, ctx->callee)) {
            swift_ref_t ref = swift_expr(ctx, node, NULL, 0);
            TSNode spelled = cbm_source_field(node, "target");
            if (cbm_source_is(spelled, "simple_identifier"))
                swift_emit_reference(ctx, spelled, &ref);
        }
        uint32_t parts = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < parts; i++)
            if (!cbm_source_is(ts_node_named_child(node, i), "simple_identifier"))
                swift_visit(ctx, ts_node_named_child(node, i), depth + 1);
        return;
    }
    /* `return .foo`, `x = .foo`, `x == .foo`: what the context expects. */
    if (!strcmp(kind, "control_transfer_statement") &&
        !ts_node_is_null(cbm_source_field(node, "result")))
        swift_note_expected(ctx, cbm_source_field(node, "result"), ctx->return_key);
    if (!strcmp(kind, "assignment")) {
        TSNode target = cbm_source_field(node, "target");
        TSNode result = cbm_source_field(node, "result");
        if (!ts_node_is_null(swift_implicit_head(result, 0)) &&
            ts_node_named_child_count(target) == 1) {
            swift_ref_t left = swift_expr(ctx, ts_node_named_child(target, 0), NULL, 0);
            swift_note_expected(ctx, result, left.kind == SWIFT_VALUE ? left.type_key : NULL);
        }
    }
    if (!strcmp(kind, "equality_expression")) {
        TSNode left = cbm_source_field(node, "lhs"), right = cbm_source_field(node, "rhs");
        bool on_right = !ts_node_is_null(swift_implicit_head(right, 0));
        if (on_right || !ts_node_is_null(swift_implicit_head(left, 0))) {
            swift_ref_t other = swift_expr(ctx, on_right ? left : right, NULL, 0);
            swift_note_expected(ctx, on_right ? right : left,
                                other.kind == SWIFT_VALUE ? other.type_key : NULL);
        }
    }
    if (!strcmp(kind, "switch_statement")) {
        const char *saved_subject = ctx->subject_key;
        swift_ref_t subject = swift_expr(ctx, cbm_source_field(node, "expr"), NULL, 0);
        ctx->subject_key = subject.kind == SWIFT_VALUE ? subject.type_key : NULL;
        swift_visit_children(ctx, node, depth);
        ctx->subject_key = saved_subject;
        return;
    }
    if (!strcmp(kind, "navigation_expression") && !ts_node_eq(node, ctx->callee))
        swift_visit_member_read(ctx, node);
    if (!strcmp(kind, "class_declaration") || !strcmp(kind, "protocol_declaration")) {
        swift_visit_type(ctx, node, depth);
        return;
    }
    /* A case's name and its associated values' labels are declarations; their types are read. */
    if (!strcmp(kind, "enum_entry") || !strcmp(kind, "enum_type_parameters") ||
        !strcmp(kind, "typealias_declaration") || !strcmp(kind, "associatedtype_declaration")) {
        uint32_t parts = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < parts; i++) {
            TSNode part = ts_node_named_child(node, i);
            if (swift_type_kind(part) || cbm_source_is(part, "enum_type_parameters"))
                swift_visit(ctx, part, depth + 1);
        }
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
    if (!strcmp(kind, "constructor_expression")) {
        swift_visit_constructor(ctx, node, depth);
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
        const swift_closure_t *saved_closure = ctx->closure;
        int saved_index = ctx->closure_index;
        const char *key = swift_byte_key(ctx, node);
        ctx->closure = key ? cbm_ht_get(ctx->closures, key) : NULL;
        ctx->closure_index = 0;
        ctx->in_function = true;
        swift_bind_shorthands(ctx);
        swift_visit_children(ctx, node, depth);
        ctx->locals = saved;
        ctx->in_function = saved_function;
        ctx->closure = saved_closure;
        ctx->closure_index = saved_index;
        return;
    }
    if (!strcmp(kind, "pattern")) {
        swift_bind_pattern(ctx, node, depth + 1);
        return;
    }
    if (!strcmp(kind, "capture_list_item")) {
        swift_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(node, "name")), NULL);
        return;
    }
    if (!strcmp(kind, "lambda_parameter")) {
        /* `{ (f: Foo) in`: written; `{ f in`: what the signature says of this position. */
        const char *written = NULL;
        uint32_t parts = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < parts; i++)
            if (swift_type_kind(ts_node_named_child(node, i)))
                written = swift_type_text(ctx->walk, ts_node_named_child(node, i), 0);
        const char *owner =
            ctx->type_key ? ctx->type_key : swift_module_owner(ctx->walk, ctx->module);
        if (written) {
            swift_bind_as(ctx, cbm_source_text(ctx->walk, cbm_source_field(node, "name")),
                          swift_key_of_written(ctx, written),
                          swift_written_outside(ctx, owner, written));
        } else {
            swift_bind_closure_param(ctx,
                                     cbm_source_text(ctx->walk, cbm_source_field(node, "name")),
                                     ctx->closure_index);
        }
        ctx->closure_index++;
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
    ctx.expected = cbm_ht_create(32);
    ctx.argument_refs = cbm_ht_create(32);
    ctx.closures = cbm_ht_create(16);
    if (!ctx.module_imports || !ctx.expected || !ctx.argument_refs || !ctx.closures) {
        cbm_ht_free(ctx.module_imports);
        cbm_ht_free(ctx.expected);
        cbm_ht_free(ctx.argument_refs);
        cbm_ht_free(ctx.closures);
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
        else if (imported && !strcmp(imported, "Testing"))
            ctx.imports_testing = true;
    }
    ctx.file_private = swift_file_private_types(walk);
    swift_visit_children(&ctx, walk->root, 0);
    if (ctx.file_private)
        cbm_ht_free(ctx.file_private);
    cbm_ht_free(ctx.module_imports);
    cbm_ht_free(ctx.expected);
    cbm_ht_free(ctx.argument_refs);
    cbm_ht_free(ctx.closures);
}

const cbm_source_adapter_t cbm_source_adapter_swift = {.language = CBM_LANG_SWIFT,
                                                       .collect = swift_collect,
                                                       .resolve = swift_resolve,
                                                       .one_name_one_node = true,
                                                       .decides_references = true};
