/* Dart binding rules, from the language specification's scoping chapter.
 *
 * Dart is lexically scoped. A bare name is looked up in the enclosing blocks, then
 * the members declared in the enclosing class, then the library's top-level
 * declarations (including its parts), then its imports. Only when none of those
 * has it is it an implicit `this.name`, resolved on the superclass chain, so a
 * library function beats an inherited method. An unprefixed import brings in a
 * library's public names and those of what it exports, filtered by show and hide;
 * two project libraries providing one name are an error at its use, so a second
 * candidate decides nothing. A prefix `p.` searches only imports with that prefix.
 *
 * An import from outside the project can provide any name. It cannot collide with
 * a project import in code that compiles, but it does win over an inherited member,
 * so an inherited bare name is decided only when no such import can provide it.
 *
 * `x.m()` selects m on x's static type: a parameter, variable or field with a
 * declared type, or one initialized by a constructor call or by a member with a
 * declared return type. The chain is the class, its mixins from last to first, its
 * superclass and its interfaces. A class outside the project ends the chain as
 * unknown, not absent. Override at runtime is dispatch and not selected here.
 *
 * `Foo(...)` constructs Foo. The graph has no node for a constructor, so the
 * selected declaration is the class, and the evidence says it is a class call.
 *
 * These rules decide an occurrence before the older direct-import rules in
 * binding.c; where these abstain, those keep their own stricter proofs.
 *
 * Known gaps, each abstaining: `part of a.b;` by library name (the file falls back
 * to the older rules), element types of collections and generics, and conditional
 * imports. Calls deep in a selector chain and in cascades have no extraction carrier,
 * so a decided one is reported as an extra site. */
#include "source_adapter_internal.h"
#include <stdio.h>
#include <stdlib.h>
#include "stats.h"
#include "syntax_index.h"

#define DART_LIBRARY_RECORD "&library"
#define DART_CLASS_RECORD "&class"
#define DART_CLASS_PARAMS "&params" /* type: the class's own type parameters, `,T,U,` */
#define DART_IMPORTS "&imports"
#define DART_EXPORTS "&exports"
#define DART_EXPORTERS "&exporters" /* under a library: who exports it; bases: them, show, hide */
#define DART_NAMES "&names"         /* owner of a row per library declaring a top-level name */
#define DART_CTOR_PREFIX "ctor:"
#define DART_CLASS_PREFIX "class:"
#define DART_DIRECTIVE_SEPARATOR '\x1e'
#define DART_TYPE_ARGS_SEPARATOR '\x1f' /* base's name, then its type arguments */
#define DART_CLASS_PARAM '$'            /* a declared type that is a parameter of its class */

typedef struct {
    const char *library; /* project path; NULL when the library is not visible to this table */
    const char *prefix;
    const char *show, *hide;
    bool conditional; /* its library depends on the build's configuration */
} dart_directive_t;

/* What a question about a name needs of a library's imports, without reading them all. */
typedef struct dart_import_row {
    size_t index;
    struct dart_import_row *next;
} dart_import_row_t;

typedef struct {
    dart_directive_t *imports;
    size_t import_count;
    dart_import_row_t *conditional; /* whose library the build's configuration picks */
    dart_import_row_t *open;        /* from outside the project, under a filter */
    bool open_unfiltered;           /* one from outside the project passes every name */
} dart_library_t;

typedef struct dart_local {
    const char *name;
    const char *class_key; /* NULL: a value of unknown type */
    struct dart_local *next;
} dart_local_t;

typedef struct {
    const cbm_source_decl_t *decl;
    const char *reason;
} dart_memo_t;

typedef struct {
    cbm_source_walk_t *walk;
    const char *library;
    const char *class_key;   /* enclosing class, or NULL */
    const char *type_params; /* ",T,U," in scope, or NULL */
    const char *scope;       /* the enclosing definition's QN, or NULL */
    dart_local_t *locals;
    CBMHashTable *libraries;
    CBMHashTable *import_memo;
    CBMHashTable *imports_of; /* importer and imported library: that import's rows */
    CBMHashTable *prefixes;   /* this library's import prefixes */
} dart_ctx_t;

typedef enum { DART_NOTHING, DART_FUNCTION, DART_CLASS, DART_INSTANCE, DART_LIBRARY } dart_kind_t;

/* What an expression prefix denotes while walking its selectors. */
typedef struct {
    dart_kind_t kind;
    const char *class_key;          /* DART_CLASS, DART_INSTANCE */
    const cbm_source_decl_t *decl;  /* DART_FUNCTION, DART_CLASS */
    const cbm_source_decl_t *value; /* a value's own declaration, whatever its type */
    const char *prefix;             /* DART_LIBRARY */
    const char *receiver;           /* the class a member was selected on */
    bool via_import;
    bool via_receiver;
    bool from_super; /* `super`: its lookup starts past the class's own members */
    const char *reason;
} dart_ref_t;

/* ── URIs ──────────────────────────────────────────────────────────── */

static const char *dart_path_join(cbm_source_walk_t *walk, const char *directory,
                                  const char *relative) {
    char *joined = directory && directory[0]
                       ? cbm_arena_sprintf(walk->arena, "%s/%s", directory, relative)
                       : cbm_arena_strdup(walk->arena, relative);
    if (!joined)
        return NULL;
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
                return NULL; /* Above the repository root. */
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

static const char *dart_directory(cbm_source_walk_t *walk, const char *file) {
    const char *slash = file ? strrchr(file, '/') : NULL;
    return slash ? cbm_arena_strndup(walk->arena, file, (size_t)(slash - file)) : "";
}

/* A project path for a URI, or NULL for dart: and packages outside the project. */
static const char *dart_uri_library(cbm_source_walk_t *walk, const char *base_file,
                                    const char *uri) {
    if (!uri || !uri[0] || !strncmp(uri, "dart:", 5))
        return NULL;
    if (!strncmp(uri, "package:", 8)) {
        const char *rest = uri + 8;
        const char *slash = strchr(rest, '/');
        if (!slash)
            return NULL;
        char *name = cbm_arena_strndup(walk->arena, rest, (size_t)(slash - rest));
        const cbm_source_table_t *table = walk->table ? walk->table : walk->writable;
        const char *lib = name ? cbm_source_table_dart_package(table, base_file, name) : NULL;
        return lib ? dart_path_join(walk, lib, slash + 1) : NULL;
    }
    if (strchr(uri, ':') || uri[0] == '/')
        return NULL;
    return dart_path_join(walk, dart_directory(walk, base_file), uri);
}

static const char *dart_string(cbm_source_walk_t *walk, TSNode literal) {
    const char *text = cbm_source_text(walk, literal);
    size_t length = text ? strlen(text) : 0;
    if (length < 3 || (text[0] != '\'' && text[0] != '"') || text[length - 1] != text[0] ||
        strchr(text, '$') || strchr(text, '\\'))
        return NULL;
    return cbm_arena_strndup(walk->arena, text + 1, length - 2);
}

static TSNode dart_find(TSNode node, const char *kind, unsigned depth) {
    if (depth > 8 || ts_node_is_null(node))
        return (TSNode){0};
    if (cbm_source_is(node, kind))
        return node;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode found = dart_find(ts_node_named_child(node, i), kind, depth + 1);
        if (!ts_node_is_null(found))
            return found;
    }
    return (TSNode){0};
}

static TSNode dart_cursor_named(TSTreeCursor *cursor, bool *more) {
    while (*more) {
        TSNode node = ts_tree_cursor_current_node(cursor);
        *more = ts_tree_cursor_goto_next_sibling(cursor);
        if (ts_node_is_named(node))
            return node;
    }
    return (TSNode){0};
}

/* ── Phase one ─────────────────────────────────────────────────────── */

static const char *dart_list(cbm_source_walk_t *walk, TSNode node, const char *kind,
                             const char *outer) {
    char *joined =
        outer ? cbm_arena_strdup(walk->arena, outer) : cbm_arena_strdup(walk->arena, ",");
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; joined && i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!cbm_source_is(child, kind))
            continue;
        /* `T extends Base` is a type parameter named T. */
        const char *name = cbm_source_is(child, "type_parameter")
                               ? cbm_source_text(walk, ts_node_named_child(child, 0))
                               : cbm_source_text(walk, child);
        if (name)
            joined = cbm_arena_sprintf(walk->arena, "%s%s,", joined, name);
    }
    return joined;
}

static bool dart_list_has(const char *list, const char *name) {
    if (!list || !list[0])
        return false;
    size_t length = strlen(name);
    for (const char *p = list; (p = strstr(p, name)); p += length)
        if (p > list && p[-1] == ',' && p[length] == ',')
            return true;
    return false;
}

/* The type parameters a generic declaration brings into scope. */
static const char *dart_type_params(cbm_source_walk_t *walk, TSNode owner, const char *outer) {
    uint32_t count = ts_node_named_child_count(owner);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(owner, i);
        if (cbm_source_is(child, "type_parameters"))
            return dart_list(walk, child, "type_parameter", outer);
        if (cbm_source_is(child, "formal_parameter_list"))
            break;
    }
    return outer;
}

/* The class a declaration's type names. A type parameter or function type is not one. */
static const char *dart_type_text(cbm_source_walk_t *walk, TSNode holder, const char *type_params) {
    const char *type = NULL;
    uint32_t count = ts_node_named_child_count(holder);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(holder, i);
        if (cbm_source_is(child, "function_type") || cbm_source_is(child, "record_type"))
            return NULL;
        if (!type && cbm_source_is(child, "type_identifier"))
            type = cbm_source_text(walk, child);
    }
    return type && !dart_list_has(type_params, type) ? type : NULL;
}

/* A member's declared type. A parameter of its class stays a parameter, for a subclass that
 * fixes it to say what it means; one the member declares itself means nothing outside. */
static const char *dart_member_type_text(cbm_source_walk_t *walk, TSNode holder,
                                         const char *class_params, const char *own_params) {
    const char *type = dart_type_text(walk, holder, own_params);
    if (type && dart_list_has(class_params, type))
        return cbm_arena_sprintf(walk->arena, "%c%s", DART_CLASS_PARAM, type);
    return type;
}

static void dart_note_directive(cbm_source_walk_t *walk, const char *library, TSNode directive,
                                const char *key) {
    /* A conditional or unreadable URI is kept as an open library, not dropped: it
     * can still provide names, and its prefix is still a name. */
    const char *uri = !ts_node_is_null(dart_find(directive, "configuration_uri", 0))
                          ? ""
                          : dart_string(walk, dart_find(directive, "string_literal", 0));
    const char *prefix = "", *show = "", *hide = "";
    TSNode holder = dart_find(directive, "import_specification", 0);
    if (ts_node_is_null(holder))
        holder = directive;
    uint32_t count = ts_node_named_child_count(holder);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(holder, i);
        if (cbm_source_is(child, "identifier")) {
            const char *text = cbm_source_text(walk, child);
            prefix = text ? text : "";
        } else if (cbm_source_is(child, "combinator")) {
            const char *names = dart_list(walk, child, "identifier", NULL);
            if (cbm_source_token(child, "show"))
                show = names ? names : "";
            else
                hide = names ? names : "";
        }
    }
    cbm_source_decl_t *decl = cbm_source_declare(walk, library, key, NULL, CBM_SOURCE_DECL_F_VALUE,
                                                 ts_node_start_point(directive).row + 1);
    const char *encoded =
        cbm_arena_sprintf(walk->arena, "%s%c%s%c%s%c%s", uri ? uri : "", DART_DIRECTIVE_SEPARATOR,
                          prefix, DART_DIRECTIVE_SEPARATOR, show, DART_DIRECTIVE_SEPARATOR, hide);
    if (decl && encoded)
        decl->type = cbm_source_table_intern(walk, encoded);
    if (strcmp(key, DART_EXPORTS))
        return;
    const char *target = dart_uri_library(walk, walk->file, uri);
    cbm_source_decl_t *edge =
        target ? cbm_source_declare(walk, target, DART_EXPORTERS, NULL, CBM_SOURCE_DECL_F_VALUE,
                                    ts_node_start_point(directive).row + 1)
               : NULL;
    const char **bases = edge ? cbm_source_table_alloc(walk, 4 * sizeof(*bases)) : NULL;
    if (!bases)
        return;
    bases[0] = cbm_source_table_intern(walk, library);
    bases[1] = cbm_source_table_intern(walk, show);
    bases[2] = cbm_source_table_intern(walk, hide);
    edge->bases = bases;
}

/* A declaration of this file. One at its top level is also listed under its name, for a walk
 * through exports to start from. */
static cbm_source_decl_t *dart_declare(cbm_source_walk_t *walk, const char *owner, const char *name,
                                       const char *qn, uint32_t flags, uint32_t line) {
    cbm_source_decl_t *decl = cbm_source_declare(walk, owner, name, qn, flags, line);
    if (decl && owner == walk->top_owner) {
        cbm_source_decl_t *row =
            cbm_source_declare(walk, DART_NAMES, name, NULL, CBM_SOURCE_DECL_F_VALUE, line);
        if (row)
            row->type = decl->owner;
    }
    return decl;
}

static void dart_declare_function(cbm_source_walk_t *walk, const char *owner, TSNode signature,
                                  const char *type_params) {
    TSNode name = cbm_source_field(signature, "name");
    const char *text = cbm_source_text(walk, name);
    if (!text)
        return;
    uint32_t line = ts_node_start_point(name).row + 1;
    cbm_source_decl_t *decl =
        dart_declare(walk, owner, text, cbm_source_def_qn(walk, text, line), 0, line);
    const char *type = dart_member_type_text(walk, signature, type_params,
                                             dart_type_params(walk, signature, NULL));
    if (decl && type)
        decl->type = cbm_source_table_intern(walk, type);
}

static void dart_declare_value(cbm_source_walk_t *walk, const char *owner, const char *name,
                               const char *type, uint32_t line) {
    /* An enum constant or a top-level variable has a node, and an accessor has its Method; a
     * plain field has none. */
    const char *qn = name ? cbm_source_def_qn_label(walk, name, line, "Variable") : NULL;
    if (!qn && name)
        qn = cbm_source_def_qn_label(walk, name, line, "Method");
    cbm_source_decl_t *decl =
        name ? dart_declare(walk, owner, name, qn, CBM_SOURCE_DECL_F_VALUE, line) : NULL;
    if (decl && type)
        decl->type = cbm_source_table_intern(walk, type);
}

/* `x = Foo(...)` with nothing else: the name of the possible class it constructs. */
static const char *dart_initializer_type(cbm_source_walk_t *walk, TSNode entry) {
    if (ts_node_named_child_count(entry) != 3)
        return NULL;
    TSNode head = ts_node_named_child(entry, 1), call = ts_node_named_child(entry, 2);
    return cbm_source_is(head, "identifier") && cbm_source_is(call, "selector") &&
                   cbm_source_is(ts_node_named_child(call, 0), "argument_part")
               ? cbm_source_text(walk, head)
               : NULL;
}

/* `type` NULL with `typed` set: a declared type the table cannot name, which an
 * initializer's type must not replace. */
static void dart_collect_names(cbm_source_walk_t *walk, const char *owner, TSNode list,
                               const char *type, bool typed) {
    uint32_t count = ts_node_named_child_count(list);
    for (uint32_t i = 0; i < count; i++) {
        TSNode entry = ts_node_named_child(list, i);
        TSNode name = cbm_source_is(entry, "identifier") ? entry : ts_node_named_child(entry, 0);
        if (!cbm_source_is(name, "identifier"))
            continue;
        dart_declare_value(walk, owner, cbm_source_text(walk, name),
                           typed ? type : dart_initializer_type(walk, entry),
                           ts_node_start_point(entry).row + 1);
    }
}

static void dart_collect_ctor(cbm_source_walk_t *walk, const char *owner, TSNode signature) {
    /* `Foo.named` is two identifiers; the plain constructor is one. */
    const char *named = NULL;
    uint32_t parts = ts_node_named_child_count(signature), identifiers = 0;
    for (uint32_t p = 0; p < parts; p++) {
        TSNode part = ts_node_named_child(signature, p);
        if (cbm_source_is(part, "identifier") && ++identifiers == 2)
            named = cbm_source_text(walk, part);
    }
    const char *key = cbm_arena_sprintf(walk->arena, "%s%s", DART_CTOR_PREFIX, named ? named : "");
    if (key)
        (void)cbm_source_declare(walk, owner, key, NULL, CBM_SOURCE_DECL_F_VALUE,
                                 ts_node_start_point(signature).row + 1);
}

static bool dart_type_written(TSNode declaration) {
    uint32_t count = ts_node_named_child_count(declaration);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(declaration, i);
        if (cbm_source_is(child, "type_identifier") || cbm_source_is(child, "function_type") ||
            cbm_source_is(child, "record_type"))
            return true;
    }
    return false;
}

static void dart_collect_declaration(cbm_source_walk_t *walk, const char *owner, TSNode declaration,
                                     const char *type_params) {
    const char *type = dart_member_type_text(walk, declaration, type_params, NULL);
    bool typed = dart_type_written(declaration);
    uint32_t count = ts_node_named_child_count(declaration);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(declaration, i);
        if (cbm_source_is(child, "initialized_identifier_list") ||
            cbm_source_is(child, "static_final_declaration_list")) {
            dart_collect_names(walk, owner, child, type, typed);
        } else if (cbm_source_is(child, "function_signature")) {
            dart_declare_function(walk, owner, child, type_params);
        } else if (cbm_source_is(child, "getter_signature") ||
                   cbm_source_is(child, "setter_signature")) {
            dart_declare_value(walk, owner, cbm_source_text(walk, cbm_source_field(child, "name")),
                               dart_member_type_text(walk, child, type_params, NULL),
                               ts_node_start_point(child).row + 1);
        } else if (cbm_source_is(child, "constructor_signature") ||
                   cbm_source_is(child, "constant_constructor_signature") ||
                   cbm_source_is(child, "factory_constructor_signature") ||
                   cbm_source_is(child, "redirecting_factory_constructor_signature")) {
            dart_collect_ctor(walk, owner, child);
        } else if (cbm_source_is(child, "method_signature")) {
            dart_collect_declaration(walk, owner, child, type_params);
        }
    }
}

/* A base's type arguments, in order. An argument with no plain name counts as a place. */
static const char *dart_base_args(cbm_source_walk_t *walk, TSNode type) {
    TSNode list = ts_node_next_named_sibling(type);
    if (!cbm_source_is(list, "type_arguments"))
        return "";
    const char *joined = "";
    uint32_t count = ts_node_named_child_count(list);
    for (uint32_t i = 0; joined && i < count; i++) {
        TSNode arg = ts_node_named_child(list, i);
        /* `A<B>` keeps B's list beside it, and `A?` its mark. */
        if (cbm_source_is(arg, "type_arguments") || cbm_source_is(arg, "nullable_type"))
            continue;
        const char *text = cbm_source_is(arg, "type_identifier") ? cbm_source_text(walk, arg) : "?";
        joined = cbm_arena_sprintf(walk->arena, "%s%s,", joined, text ? text : "?");
    }
    return joined ? joined : "";
}

static const char *dart_base_entry(cbm_source_walk_t *walk, char tag, TSNode type) {
    const char *text = cbm_source_text(walk, type);
    return text ? cbm_arena_sprintf(walk->arena, "%c:%s%c%s", tag, text, DART_TYPE_ARGS_SEPARATOR,
                                    dart_base_args(walk, type))
                : NULL;
}

/* Each base is recorded with its role, since the roles are searched in order. */
static void dart_note_bases(cbm_source_walk_t *walk, TSNode node, cbm_source_decl_t *record) {
    cbm_source_names_t found = {0};
    bool open = false;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        char tag = cbm_source_is(child, "superclass")   ? 'S'
                   : cbm_source_is(child, "interfaces") ? 'I'
                   : cbm_source_is(child, "mixins")     ? 'M'
                                                        : 0;
        /* `mixin M on Base` and `extension E on Base` see Base's members as their own. */
        if (cbm_source_is(child, "type_identifier")) {
            const char *entry = dart_base_entry(walk, 'S', child);
            if (entry)
                open |= !cbm_source_names_push(walk, &found, entry);
            continue;
        }
        if (!tag)
            continue;
        uint32_t parts = ts_node_named_child_count(child);
        for (uint32_t p = 0; p < parts; p++) {
            TSNode part = ts_node_named_child(child, p);
            bool nested = cbm_source_is(part, "mixins");
            uint32_t names = nested ? ts_node_named_child_count(part) : 1;
            for (uint32_t m = 0; m < names; m++) {
                TSNode type = nested ? ts_node_named_child(part, m) : part;
                if (!cbm_source_is(type, "type_identifier"))
                    continue;
                const char *entry = dart_base_entry(walk, nested ? 'M' : tag, type);
                if (entry)
                    open |= !cbm_source_names_push(walk, &found, entry);
            }
        }
    }
    const char **copy = cbm_source_table_alloc(walk, (found.count + 1) * sizeof(*copy));
    if (!copy)
        return;
    for (size_t i = 0; i < found.count; i++)
        copy[i] = found.items[i] ? cbm_source_table_intern(walk, found.items[i]) : NULL;
    copy[found.count] = NULL;
    record->bases = copy;
    if (open)
        record->flags |= CBM_SOURCE_DECL_F_OPEN;
}

static TSNode dart_class_body(TSNode node) {
    TSNode body = cbm_source_field(node, "body");
    if (!ts_node_is_null(body))
        return body;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "class_body") || cbm_source_is(child, "extension_body") ||
            cbm_source_is(child, "enum_body"))
            return child;
    }
    return (TSNode){0};
}

static const char *dart_class_name(cbm_source_walk_t *walk, TSNode node) {
    TSNode name = cbm_source_field(node, "name");
    if (ts_node_is_null(name)) {
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count && ts_node_is_null(name); i++)
            if (cbm_source_is(ts_node_named_child(node, i), "identifier"))
                name = ts_node_named_child(node, i);
    }
    return cbm_source_text(walk, name);
}

static void dart_collect_class(cbm_source_walk_t *walk, const char *library, TSNode node) {
    const char *name = dart_class_name(walk, node);
    if (!name)
        return;
    uint32_t line = ts_node_start_point(node).row + 1;
    const char *key = cbm_arena_sprintf(walk->arena, "%s%s", DART_CLASS_PREFIX, name);
    const char *class_key = cbm_arena_sprintf(walk->arena, "%s#%s", library, name);
    if (!key || !class_key)
        return;
    /* The library holds the class's name and QN, for a constructor call; the record
     * under the class's own key holds what its members need. */
    cbm_source_decl_t *in_library = dart_declare(
        walk, library, key, cbm_source_def_qn(walk, name, line), CBM_SOURCE_DECL_F_TYPE, line);
    if (in_library)
        in_library->type = cbm_source_table_intern(walk, class_key);
    cbm_source_decl_t *record =
        cbm_source_declare(walk, class_key, DART_CLASS_RECORD, NULL, CBM_SOURCE_DECL_F_TYPE, line);
    const char *type_params = dart_type_params(walk, node, NULL);
    if (record) {
        record->type = cbm_source_table_intern(walk, library);
        dart_note_bases(walk, node, record);
    }
    if (type_params) {
        cbm_source_decl_t *params = cbm_source_declare(walk, class_key, DART_CLASS_PARAMS, NULL,
                                                       CBM_SOURCE_DECL_F_VALUE, line);
        if (params)
            params->type = cbm_source_table_intern(walk, type_params);
    }
    TSNode body = dart_class_body(node);
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (cbm_source_is(member, "method_signature") || cbm_source_is(member, "declaration"))
            dart_collect_declaration(walk, class_key, member, type_params);
        else if (cbm_source_is(member, "enum_constant"))
            dart_declare_value(walk, class_key,
                               cbm_source_text(walk, cbm_source_field(member, "name")), name,
                               ts_node_start_point(member).row + 1);
    }
    TSNode representation = cbm_source_field(node, "representation");
    if (!ts_node_is_null(representation))
        dart_declare_value(walk, class_key,
                           cbm_source_text(walk, cbm_source_field(representation, "name")),
                           cbm_source_text(walk, cbm_source_field(representation, "type")), line);
}

/* A part's declarations and imports are its library's. */
static const char *dart_library_of_file(cbm_source_walk_t *walk, bool *named_part) {
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (!cbm_source_is(child, "part_of_directive"))
            continue;
        const char *uri = dart_string(walk, dart_find(child, "string_literal", 0));
        if (uri)
            return dart_path_join(walk, dart_directory(walk, walk->file), uri);
        if (named_part)
            *named_part = true;
        return walk->file;
    }
    return walk->file;
}

static bool dart_class_kind(TSNode node) {
    return cbm_source_is(node, "class_definition") || cbm_source_is(node, "mixin_declaration") ||
           cbm_source_is(node, "enum_declaration") ||
           cbm_source_is(node, "extension_declaration") ||
           cbm_source_is(node, "extension_type_declaration");
}

static void dart_collect(cbm_source_walk_t *walk) {
    const char *library = dart_library_of_file(walk, NULL);
    if (!library)
        return;
    (void)cbm_source_declare(walk, library, DART_LIBRARY_RECORD, NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    walk->top_owner = library;
    const char *pending = NULL;
    bool typed = false;
    uint32_t count = ts_node_named_child_count(walk->root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(walk->root, i);
        if (cbm_source_is(child, "import_or_export")) {
            TSNode inner = ts_node_named_child(child, 0);
            dart_note_directive(walk, library, inner,
                                cbm_source_is(inner, "library_export") ? DART_EXPORTS
                                                                       : DART_IMPORTS);
        } else if (cbm_source_is(child, "function_signature")) {
            dart_declare_function(walk, library, child, NULL);
        } else if (cbm_source_is(child, "getter_signature") ||
                   cbm_source_is(child, "setter_signature")) {
            dart_declare_value(
                walk, library, cbm_source_text(walk, cbm_source_field(child, "name")),
                dart_type_text(walk, child, NULL), ts_node_start_point(child).row + 1);
        } else if (dart_class_kind(child)) {
            dart_collect_class(walk, library, child);
        } else if (cbm_source_is(child, "type_alias")) {
            /* A typedef's target is not a class this table can walk. */
            const char *name = NULL;
            uint32_t parts = ts_node_named_child_count(child);
            for (uint32_t p = 0; p < parts && !name; p++)
                if (cbm_source_is(ts_node_named_child(child, p), "type_identifier"))
                    name = cbm_source_text(walk, ts_node_named_child(child, p));
            const char *key =
                name ? cbm_arena_sprintf(walk->arena, "%s%s", DART_CLASS_PREFIX, name) : NULL;
            if (key)
                (void)dart_declare(walk, library, key, NULL,
                                   CBM_SOURCE_DECL_F_TYPE | CBM_SOURCE_DECL_F_OPEN,
                                   ts_node_start_point(child).row + 1);
        } else if (cbm_source_is(child, "type_identifier")) {
            /* Top-level variables keep type and names as flat siblings. */
            pending = cbm_source_text(walk, child);
            typed = true;
        } else if (cbm_source_is(child, "function_type") || cbm_source_is(child, "record_type")) {
            pending = NULL;
            typed = true;
        } else if (cbm_source_is(child, "initialized_identifier_list") ||
                   cbm_source_is(child, "static_final_declaration_list")) {
            dart_collect_names(walk, library, child, pending, typed);
            pending = NULL;
            typed = false;
        } else if (cbm_source_is(child, "declaration")) {
            dart_collect_declaration(walk, library, child, NULL);
        }
    }
}

/* ── Phase two: libraries ──────────────────────────────────────────── */

static bool dart_filter_passes(const char *show, const char *hide, const char *name) {
    if (show && show[0] && !dart_list_has(show, name))
        return false;
    return !dart_list_has(hide, name);
}

/* False for a directive this table holds in another form, which says nothing. */
static bool dart_read_directive(dart_ctx_t *ctx, const cbm_source_decl_t *d,
                                dart_directive_t *out) {
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
    char *copy = d->type ? cbm_arena_strdup(ctx->walk->arena, d->type) : NULL;
    char *parts[4] = {0};
    int used = 0;
    for (char *p = copy; p && used < 4;) {
        parts[used++] = p;
        char *mark = strchr(p, DART_DIRECTIVE_SEPARATOR);
        if (mark)
            *mark = '\0';
        p = mark ? mark + 1 : NULL;
    }
    if (used != 4)
        return false;
    const char *path = dart_uri_library(ctx->walk, d->file, parts[0]);
    if (path && !cbm_source_lookup(ctx->walk, path, DART_LIBRARY_RECORD))
        path = NULL;
    *out =
        (dart_directive_t){path, parts[1][0] ? parts[1] : NULL, parts[2], parts[3], !parts[0][0]};
    return true;
}

static void dart_read_directives(dart_ctx_t *ctx, const char *library, const char *key,
                                 dart_directive_t **out, size_t *out_count) {
    size_t count = 0;
    for (const cbm_source_decl_t *d = cbm_source_lookup(ctx->walk, library, key); d; d = d->next)
        count++;
    dart_directive_t *items =
        count ? cbm_arena_calloc(ctx->walk->arena, count * sizeof(*items)) : NULL;
    *out = items;
    *out_count = 0;
    if (!items)
        return;
    for (const cbm_source_decl_t *d = cbm_source_lookup(ctx->walk, library, key); d; d = d->next)
        if (dart_read_directive(ctx, d, &items[*out_count]))
            (*out_count)++;
}

static dart_import_row_t *dart_import_row(dart_ctx_t *ctx, size_t index, dart_import_row_t *next) {
    dart_import_row_t *row = cbm_arena_alloc(ctx->walk->arena, sizeof(*row));
    if (!row) {
        cbm_source_fail(ctx->walk, "dart_library_allocation");
        return next;
    }
    *row = (dart_import_row_t){index, next};
    return row;
}

static const char *dart_import_key(dart_ctx_t *ctx, const char *importer, const char *imported) {
    return cbm_arena_sprintf(ctx->walk->arena, "%s%c%s", importer, DART_DIRECTIVE_SEPARATOR,
                             imported);
}

static void dart_index_imports(dart_ctx_t *ctx, const char *path, dart_library_t *library) {
    bool own = !strcmp(path, ctx->library);
    for (size_t i = 0; i < library->import_count; i++) {
        const dart_directive_t *import = &library->imports[i];
        if (own && import->prefix)
            cbm_ht_set(ctx->prefixes, import->prefix, (void *)import->prefix);
        if (import->conditional)
            library->conditional = dart_import_row(ctx, i, library->conditional);
        if (import->library) {
            const char *key = dart_import_key(ctx, path, import->library);
            if (key)
                cbm_ht_set(ctx->imports_of, key,
                           dart_import_row(ctx, i, cbm_ht_get(ctx->imports_of, key)));
        } else if (!import->prefix) {
            if ((!import->show || !import->show[0]) && (!import->hide || !import->hide[0]))
                library->open_unfiltered = true;
            else
                library->open = dart_import_row(ctx, i, library->open);
        }
    }
}

static dart_library_t *dart_library(dart_ctx_t *ctx, const char *path) {
    dart_library_t *library = cbm_ht_get(ctx->libraries, path);
    if (library)
        return library;
    library = cbm_arena_calloc(ctx->walk->arena, sizeof(*library));
    char *key = cbm_arena_strdup(ctx->walk->arena, path);
    if (!library || !key) {
        cbm_source_fail(ctx->walk, "dart_library_allocation");
        return NULL;
    }
    dart_read_directives(ctx, path, DART_IMPORTS, &library->imports, &library->import_count);
    dart_index_imports(ctx, key, library);
    cbm_ht_set(ctx->libraries, key, library);
    return library;
}

/* Every library that reaches `library` through exports that pass this name, each once. A library
 * declaring the key itself answers with its own, so no walk goes on through it. This reads who
 * exports `library`, directly or not, and no barrel's whole export list, and it recurses nowhere.
 * True when `visit` said to stop. */
typedef bool (*dart_up_fn)(const char *library, void *user);

static bool dart_walk_up(dart_ctx_t *ctx, const char *library, const char *key, const char *name,
                         cbm_source_chain_t *seen, dart_up_fn visit, void *user, bool *limited) {
    enum { DART_WALK_INLINE = 32 };
    const char *held[DART_WALK_INLINE];
    const char **stack = held;
    size_t depth = 0, capacity = DART_WALK_INLINE;
    for (const char *at = library; at; at = depth ? stack[--depth] : NULL) {
        for (const cbm_source_decl_t *edge = cbm_source_lookup(ctx->walk, at, DART_EXPORTERS); edge;
             edge = edge->next) {
            cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
            const char *const *bases = edge->bases;
            if (!bases || !dart_filter_passes(bases[1], bases[2], name))
                continue;
            if (!cbm_source_chain_first_visit(ctx->walk, seen, bases[0])) {
                if (seen->overflow) {
                    *limited = true;
                    return false;
                }
                continue;
            }
            if (visit(bases[0], user))
                return true;
            if (cbm_source_lookup(ctx->walk, bases[0], key))
                continue;
            if (depth == capacity) {
                const char **grown =
                    cbm_arena_alloc(ctx->walk->arena, capacity * 2 * sizeof(*grown));
                if (!grown) {
                    *limited = true;
                    return false;
                }
                memcpy(grown, stack, depth * sizeof(*grown));
                stack = grown;
                capacity *= 2;
            }
            stack[depth++] = bases[0];
        }
    }
    return false;
}

static bool dart_is_library(const char *library, void *user) {
    return !strcmp(library, user);
}

/* A library's public name, including what it re-exports. A barrel can list the whole project and
 * every file can import it, so this starts from who declares the name: only they can answer. */
static const cbm_source_decl_t *dart_library_public(dart_ctx_t *ctx, const char *path,
                                                    const char *key, const char *name,
                                                    bool *conflict, bool *limited) {
    if (!path)
        return NULL;
    const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, path, key);
    if (own || !cbm_source_lookup(ctx->walk, path, DART_EXPORTS))
        return own;
    const cbm_source_decl_t *found = NULL;
    for (const cbm_source_decl_t *row = cbm_source_lookup(ctx->walk, DART_NAMES, key); row;
         row = row->next) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        const cbm_source_decl_t *declared = cbm_source_lookup(ctx->walk, row->type, key);
        if (declared == found)
            continue;
        cbm_source_chain_t seen = {0};
        (void)cbm_source_chain_first_visit(ctx->walk, &seen, row->type);
        if (!dart_walk_up(ctx, row->type, key, name, &seen, dart_is_library, (void *)path, limited))
            continue;
        if (found) {
            *conflict = true;
            return found;
        }
        found = declared;
    }
    return found;
}

/* The declaration a name reaches through one library's imports, with this prefix. */
typedef struct {
    dart_ctx_t *ctx;
    const char *path;
    dart_import_row_t *rows;
    size_t count;
} dart_answering_t;

/* This library's imports of a library that can answer. */
static bool dart_note_answering(const char *imported, void *user) {
    dart_answering_t *found = user;
    const char *key = dart_import_key(found->ctx, found->path, imported);
    const dart_import_row_t *row = key ? cbm_ht_get(found->ctx->imports_of, key) : NULL;
    for (; row; row = row->next) {
        found->rows = dart_import_row(found->ctx, row->index, found->rows);
        found->count++;
    }
    return false;
}

static int dart_compare_index(const void *left, const void *right) {
    size_t a = *(const size_t *)left, b = *(const size_t *)right;
    return a < b ? -1 : a > b;
}

/* In written order, the imports that can change the answer for this name: those whose library, or
 * something it exports, declares it, and those whose library the build picks. Every other import
 * adds nothing, wherever it stands. False: the walk could not finish, so ask them all. */
static bool dart_imports_answering(dart_ctx_t *ctx, const char *path, const dart_library_t *library,
                                   const char *key, const char *name, size_t **out, size_t *count) {
    dart_answering_t found = {.ctx = ctx, .path = path};
    cbm_source_chain_t seen = {0};
    bool limited = false;
    for (const cbm_source_decl_t *row = cbm_source_lookup(ctx->walk, DART_NAMES, key);
         row && !limited; row = row->next) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        limited |= !cbm_source_chain_first_visit(ctx->walk, &seen, row->type) && seen.overflow;
        (void)dart_note_answering(row->type, &found);
        (void)dart_walk_up(ctx, row->type, key, name, &seen, dart_note_answering, &found, &limited);
    }
    if (limited)
        return false;
    for (const dart_import_row_t *row = library->conditional; row; row = row->next) {
        found.rows = dart_import_row(ctx, row->index, found.rows);
        found.count++;
    }
    size_t *items =
        found.count ? cbm_arena_alloc(ctx->walk->arena, found.count * sizeof(*items)) : NULL;
    if (found.count && !items)
        return false;
    size_t used = 0;
    for (const dart_import_row_t *row = found.rows; row; row = row->next)
        items[used++] = row->index;
    if (used > 1)
        qsort(items, used, sizeof(*items), dart_compare_index);
    *out = items;
    *count = used;
    return true;
}

static const cbm_source_decl_t *dart_import_search(dart_ctx_t *ctx, const char *path,
                                                   const char *prefix, const char *key,
                                                   const char *name, const char **reason) {
    *reason = NULL;
    if (name[0] == '_')
        return NULL; /* Private to its library. */
    const dart_library_t *library = dart_library(ctx, path);
    if (!library)
        return NULL;
    size_t *answering = NULL, count = library->import_count;
    bool some = dart_imports_answering(ctx, path, library, key, name, &answering, &count);
    if (!some)
        count = library->import_count;
    const cbm_source_decl_t *selected = NULL;
    size_t last = SIZE_MAX;
    for (size_t step = 0; step < count; step++) {
        size_t i = some ? answering[step] : step;
        /* An import that declares a name and reaches it through an export comes up twice. */
        if (i == last)
            continue;
        last = i;
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        const dart_directive_t *import = &library->imports[i];
        bool same_prefix = (!prefix && !import->prefix) ||
                           (prefix && import->prefix && !strcmp(prefix, import->prefix));
        if (!same_prefix || !dart_filter_passes(import->show, import->hide, name))
            continue;
        /* Which file this import reads is a build fact. It can be a project file, so
         * unlike a package outside the project it can collide with another import. */
        if (import->conditional) {
            *reason = "dart_conditional_import_may_provide_name";
            return NULL;
        }
        bool conflict = false, limited = false;
        const cbm_source_decl_t *found =
            dart_library_public(ctx, import->library, key, name, &conflict, &limited);
        if (conflict) {
            *reason = "dart_export_conflict";
            return NULL;
        }
        if (limited && !found) {
            *reason = "dart_export_graph_not_walked";
            return NULL;
        }
        if (found && selected && found != selected) {
            *reason = "dart_two_imports_provide_name";
            return NULL;
        }
        if (found)
            selected = found;
    }
    return selected;
}

static const cbm_source_decl_t *dart_import_lookup(dart_ctx_t *ctx, const char *path,
                                                   const char *prefix, const char *key,
                                                   const char *name, const char **reason) {
    if (strcmp(path, ctx->library))
        return dart_import_search(ctx, path, prefix, key, name, reason);
    const char *memo_key = cbm_arena_sprintf(ctx->walk->arena, "%s%c%s", prefix ? prefix : "",
                                             DART_DIRECTIVE_SEPARATOR, key);
    dart_memo_t *memo = memo_key ? cbm_ht_get(ctx->import_memo, memo_key) : NULL;
    if (!memo) {
        memo = cbm_arena_alloc(ctx->walk->arena, sizeof(*memo));
        if (!memo) {
            cbm_source_fail(ctx->walk, "dart_import_memo_allocation");
            *reason = "dart_import_memo_allocation";
            return NULL;
        }
        memo->decl = dart_import_search(ctx, path, prefix, key, name, &memo->reason);
        if (memo_key)
            cbm_ht_set(ctx->import_memo, memo_key, memo);
    }
    *reason = memo->reason;
    return memo->decl;
}

/* An import outside the project with no show list that rules this name out. */
static bool dart_open_import_can_provide(dart_ctx_t *ctx, const char *name) {
    const dart_library_t *library = dart_library(ctx, ctx->library);
    if (library && library->open_unfiltered)
        return true;
    for (const dart_import_row_t *row = library ? library->open : NULL; row; row = row->next) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        const dart_directive_t *import = &library->imports[row->index];
        if (dart_filter_passes(import->show, import->hide, name))
            return true;
    }
    /* dart:core is imported implicitly, and has these functions. */
    return !strcmp(name, "print") || !strcmp(name, "identical") ||
           !strcmp(name, "identityHashCode");
}

/* ── Phase two: names, classes and members ─────────────────────────── */

static const char *dart_class_key_of(const cbm_source_decl_t *decl) {
    return decl && !decl->next && (decl->flags & CBM_SOURCE_DECL_F_TYPE) &&
                   !(decl->flags & CBM_SOURCE_DECL_F_OPEN)
               ? decl->type
               : NULL;
}

/* A class name as seen from a library. */
static const cbm_source_decl_t *dart_type_decl(dart_ctx_t *ctx, const char *path, const char *name,
                                               const char **reason, bool *via_import) {
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", DART_CLASS_PREFIX, name);
    if (!key)
        return NULL;
    /* A library's own function of that name would shadow the class. */
    if (cbm_source_lookup(ctx->walk, path, name))
        return NULL;
    const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, path, key);
    if (own)
        return own;
    if (via_import)
        *via_import = true;
    return dart_import_lookup(ctx, path, NULL, key, name, reason);
}

static const char *dart_type_class(dart_ctx_t *ctx, const char *path, const char *name) {
    const char *reason = NULL;
    return name ? dart_class_key_of(dart_type_decl(ctx, path, name, &reason, NULL)) : NULL;
}

typedef enum { DART_FOUND, DART_ABSENT, DART_OPEN, DART_AMBIGUOUS } dart_lookup_t;

static const char *dart_base_name(dart_ctx_t *ctx, const char *base) {
    const char *mark = strchr(base + 2, DART_TYPE_ARGS_SEPARATOR);
    return mark ? cbm_arena_strndup(ctx->walk->arena, base + 2, (size_t)(mark - (base + 2)))
                : base + 2;
}

static dart_lookup_t dart_find_member(dart_ctx_t *ctx, const char *class_key, const char *name,
                                      cbm_source_chain_t *chain, const cbm_source_decl_t **out);

static dart_lookup_t dart_search_bases(dart_ctx_t *ctx, const cbm_source_decl_t *record, char role,
                                       bool reverse, const char *name, cbm_source_chain_t *chain,
                                       const cbm_source_decl_t **out) {
    const char *const *bases = record->bases;
    size_t count = 0;
    while (bases && bases[count])
        count++;
    for (size_t step = 0; step < count; step++) {
        const char *base = bases[reverse ? count - 1 - step : step];
        if (base[0] != role)
            continue;
        const char *parent = dart_type_class(ctx, record->type, dart_base_name(ctx, base));
        if (!parent)
            return DART_OPEN;
        dart_lookup_t found = dart_find_member(ctx, parent, name, chain, out);
        if (found != DART_ABSENT)
            return found;
    }
    return DART_ABSENT;
}

/* The class's own, then the last mixin back to the first, then the superclass,
 * then the interfaces. */
static dart_lookup_t dart_member_on(dart_ctx_t *ctx, const char *class_key, const char *name,
                                    cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, class_key, DART_CLASS_RECORD);
    if (!record || record->next || (record->flags & CBM_SOURCE_DECL_F_OPEN))
        return DART_OPEN;
    const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, class_key, name);
    /* `_x` of another library is another name, whichever class chain leads to it. */
    if (own && name[0] == '_' && record->type && strcmp(record->type, ctx->library))
        own = NULL;
    if (own) {
        if (own->next && !(own->flags & CBM_SOURCE_DECL_F_VALUE))
            return DART_AMBIGUOUS;
        *out = own;
        return DART_FOUND;
    }
    dart_lookup_t found = dart_search_bases(ctx, record, 'M', true, name, chain, out);
    if (found == DART_ABSENT)
        found = dart_search_bases(ctx, record, 'S', false, name, chain, out);
    if (found == DART_ABSENT)
        found = dart_search_bases(ctx, record, 'I', false, name, chain, out);
    return found;
}

static dart_lookup_t dart_find_member(dart_ctx_t *ctx, const char *class_key, const char *name,
                                      cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!cbm_source_chain_enter(ctx->walk, chain, class_key))
        return chain->overflow ? DART_OPEN : DART_ABSENT;
    dart_lookup_t found = dart_member_on(ctx, class_key, name, chain, out);
    cbm_source_chain_leave(chain);
    return found;
}

/* `super.m`: what the class inherits, in the same order, without its own members. */
static dart_lookup_t dart_find_past_own(dart_ctx_t *ctx, const char *class_key, const char *name,
                                        cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (!cbm_source_chain_first_visit(ctx->walk, chain, class_key))
        return DART_ABSENT;
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, class_key, DART_CLASS_RECORD);
    if (!record || record->next || (record->flags & CBM_SOURCE_DECL_F_OPEN))
        return DART_OPEN;
    dart_lookup_t found = dart_search_bases(ctx, record, 'M', true, name, chain, out);
    if (found == DART_ABSENT)
        found = dart_search_bases(ctx, record, 'S', false, name, chain, out);
    if (found == DART_ABSENT)
        found = dart_search_bases(ctx, record, 'I', false, name, chain, out);
    return found;
}

static const char *dart_decl_library(dart_ctx_t *ctx, const cbm_source_decl_t *decl) {
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, decl->owner, DART_CLASS_RECORD);
    return record && record->type ? record->type : decl->owner;
}

static const char *dart_declared_class(dart_ctx_t *ctx, const cbm_source_decl_t *decl) {
    return decl && decl->type && decl->type[0] != DART_CLASS_PARAM
               ? dart_type_class(ctx, dart_decl_library(ctx, decl), decl->type)
               : NULL;
}

/* The nth comma-separated entry of `A,B,`, or NULL. */
static const char *dart_nth(dart_ctx_t *ctx, const char *list, int index) {
    for (int i = 0; list && *list; i++) {
        const char *comma = strchr(list, ',');
        if (!comma)
            return NULL;
        if (i == index)
            return cbm_arena_strndup(ctx->walk->arena, list, (size_t)(comma - list));
        list = comma + 1;
    }
    return NULL;
}

/* Where a class declares this type parameter, or -1. */
static int dart_param_index(dart_ctx_t *ctx, const char *cls, const char *name) {
    const cbm_source_decl_t *params = cbm_source_lookup(ctx->walk, cls, DART_CLASS_PARAMS);
    /* `,T,U,`: past the leading comma, in the same form dart_nth reads. */
    const char *list = params && params->type ? params->type + 1 : NULL;
    for (int i = 0;; i++) {
        const char *entry = dart_nth(ctx, list, i);
        if (!entry)
            return -1;
        if (!strcmp(entry, name))
            return i;
    }
}

/* What parameter `param` of `owner` means seen from `cls`, walking the extends and with clauses
 * that fix it. A class key, a parameter of `cls` as "$T", or NULL. The chain bounds the walk: a
 * class appears once. */
static const char *dart_type_arg_above(dart_ctx_t *ctx, const char *cls, const char *owner,
                                       const char *param, cbm_source_chain_t *chain);

static const char *dart_type_arg(dart_ctx_t *ctx, const char *cls, const char *owner,
                                 const char *param, cbm_source_chain_t *chain) {
    if (!strcmp(cls, owner))
        return cbm_arena_sprintf(ctx->walk->arena, "%c%s", DART_CLASS_PARAM, param);
    if (!cbm_source_chain_enter(ctx->walk, chain, cls))
        return NULL;
    const char *held = dart_type_arg_above(ctx, cls, owner, param, chain);
    cbm_source_chain_leave(chain);
    return held;
}

static const char *dart_type_arg_above(dart_ctx_t *ctx, const char *cls, const char *owner,
                                       const char *param, cbm_source_chain_t *chain) {
    const cbm_source_decl_t *record = cbm_source_lookup(ctx->walk, cls, DART_CLASS_RECORD);
    if (!record || record->next)
        return NULL;
    for (const char *const *base = record->bases; base && *base; base++) {
        const char *parent = dart_type_class(ctx, record->type, dart_base_name(ctx, *base));
        if (!parent)
            continue;
        const char *seen = dart_type_arg(ctx, parent, owner, param, chain);
        if (!seen)
            continue;
        if (seen[0] != DART_CLASS_PARAM)
            return seen;
        const char *mark = strchr(*base, DART_TYPE_ARGS_SEPARATOR);
        int index = dart_param_index(ctx, parent, seen + 1);
        const char *arg = mark && index >= 0 ? dart_nth(ctx, mark + 1, index) : NULL;
        if (!arg || !strcmp(arg, "?"))
            return NULL;
        if (dart_param_index(ctx, cls, arg) >= 0)
            return cbm_arena_sprintf(ctx->walk->arena, "%c%s", DART_CLASS_PARAM, arg);
        return dart_type_class(ctx, record->type, arg);
    }
    return NULL;
}

/* A member's declared class, as selected on this receiver. */
static const char *dart_member_class(dart_ctx_t *ctx, const cbm_source_decl_t *decl,
                                     const char *receiver) {
    if (!decl || !decl->type || decl->type[0] != DART_CLASS_PARAM)
        return dart_declared_class(ctx, decl);
    if (!receiver)
        return NULL;
    cbm_source_chain_t chain = {0};
    const char *seen = dart_type_arg(ctx, receiver, decl->owner, decl->type + 1, &chain);
    return seen && seen[0] != DART_CLASS_PARAM ? seen : NULL;
}

static dart_ref_t dart_ref_on(dart_ctx_t *ctx, const cbm_source_decl_t *decl,
                              const char *receiver) {
    dart_ref_t ref = {.reason = "dart_name_not_declared_in_project", .receiver = receiver};
    if (!decl)
        return ref;
    if (decl->flags & CBM_SOURCE_DECL_F_VALUE) {
        const char *cls = dart_member_class(ctx, decl, receiver);
        ref.kind = cls ? DART_INSTANCE : DART_NOTHING;
        ref.class_key = cls;
        ref.value = decl;
        ref.reason = cls ? "dart_call_of_a_value" : "dart_value_of_unknown_type";
        return ref;
    }
    if (decl->next) {
        ref.reason = "dart_name_not_one_declaration";
        return ref;
    }
    ref.kind = DART_FUNCTION;
    ref.decl = decl;
    ref.reason = "dart_declared_function";
    return ref;
}

static dart_ref_t dart_ref_of_decl(dart_ctx_t *ctx, const cbm_source_decl_t *decl) {
    return dart_ref_on(ctx, decl, NULL);
}

static dart_ref_t dart_ref_of_class(const cbm_source_decl_t *decl) {
    return (dart_ref_t){.kind = DART_CLASS,
                        .class_key = dart_class_key_of(decl),
                        .decl = decl,
                        .reason = "dart_class_name"};
}

static const dart_local_t *dart_find_local(const dart_ctx_t *ctx, const char *name) {
    for (const dart_local_t *local = ctx->locals; local; local = local->next)
        if (!strcmp(local->name, name))
            return local;
    return NULL;
}

/* Nothing lexical: only now is it this.name on the superclass chain. */
static dart_ref_t dart_implicit_this(dart_ctx_t *ctx, const char *name) {
    dart_ref_t ref = {.reason = "dart_name_not_declared_in_project"};
    if (!ctx->class_key)
        return ref;
    const cbm_source_decl_t *inherited = NULL;
    cbm_source_chain_t chain = {0};
    switch (dart_find_member(ctx, ctx->class_key, name, &chain, &inherited)) {
    case DART_FOUND:
        if (dart_open_import_can_provide(ctx, name)) {
            ref.reason = "dart_outside_import_may_provide_name";
            return ref;
        }
        return dart_ref_on(ctx, inherited, ctx->class_key);
    case DART_OPEN:
        ref.reason = "dart_superclass_chain_leaves_project";
        return ref;
    case DART_AMBIGUOUS:
        ref.reason = "dart_name_not_one_declaration";
        return ref;
    case DART_ABSENT:
        break;
    }
    return ref;
}

/* The lexical lookup of a bare name, then the implicit this. */
static dart_ref_t dart_name(dart_ctx_t *ctx, const char *name) {
    dart_ref_t ref = {.reason = "dart_name_not_declared_in_project"};
    const dart_local_t *local = dart_find_local(ctx, name);
    if (local) {
        ref.kind = local->class_key ? DART_INSTANCE : DART_NOTHING;
        ref.class_key = local->class_key;
        ref.reason = local->class_key ? "dart_call_of_a_value" : "dart_local_of_unknown_type";
        return ref;
    }
    if (dart_list_has(ctx->type_params, name)) {
        ref.reason = "dart_type_parameter";
        return ref;
    }
    if (ctx->class_key) {
        const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, ctx->class_key, name);
        if (own)
            return dart_ref_on(ctx, own, ctx->class_key);
    }
    const cbm_source_decl_t *top = cbm_source_lookup(ctx->walk, ctx->library, name);
    if (top)
        return dart_ref_of_decl(ctx, top);
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", DART_CLASS_PREFIX, name);
    if (!key)
        return ref;
    const cbm_source_decl_t *cls = cbm_source_lookup(ctx->walk, ctx->library, key);
    if (cls)
        return dart_ref_of_class(cls);
    /* Imports may name a member or a class. */
    const char *reason = NULL;
    const cbm_source_decl_t *imported =
        dart_import_lookup(ctx, ctx->library, NULL, name, name, &reason);
    if (reason) {
        ref.reason = reason;
        return ref;
    }
    if (imported) {
        ref = dart_ref_of_decl(ctx, imported);
        ref.via_import = true;
        return ref;
    }
    cls = dart_import_lookup(ctx, ctx->library, NULL, key, name, &reason);
    if (reason) {
        ref.reason = reason;
        return ref;
    }
    if (cls) {
        ref = dart_ref_of_class(cls);
        ref.via_import = true;
        return ref;
    }
    /* A prefix is its own kind of name. */
    (void)dart_library(ctx, ctx->library);
    const char *prefix = cbm_ht_get(ctx->prefixes, name);
    if (prefix) {
        ref.kind = DART_LIBRARY;
        ref.prefix = prefix;
        ref.via_import = true;
        return ref;
    }
    return dart_implicit_this(ctx, name);
}

static dart_ref_t dart_prefixed_member(dart_ctx_t *ctx, const char *prefix, const char *name) {
    dart_ref_t ref = {.reason = "dart_prefixed_name_not_in_project"};
    const char *reason = NULL;
    const cbm_source_decl_t *member =
        dart_import_lookup(ctx, ctx->library, prefix, name, name, &reason);
    if (member) {
        ref = dart_ref_of_decl(ctx, member);
        ref.via_import = true;
        return ref;
    }
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s%s", DART_CLASS_PREFIX, name);
    const cbm_source_decl_t *cls =
        !reason && key ? dart_import_lookup(ctx, ctx->library, prefix, key, name, &reason) : NULL;
    if (cls) {
        ref = dart_ref_of_class(cls);
        ref.via_import = true;
        return ref;
    }
    if (reason)
        ref.reason = reason;
    return ref;
}

static dart_ref_t dart_member(dart_ctx_t *ctx, const dart_ref_t *receiver, const char *name) {
    if (receiver->kind == DART_LIBRARY)
        return dart_prefixed_member(ctx, receiver->prefix, name);
    dart_ref_t ref = {.reason = "dart_receiver_type_unknown"};
    if ((receiver->kind != DART_INSTANCE && receiver->kind != DART_CLASS) || !receiver->class_key)
        return ref;
    /* A class name reaches its named constructors and static members. */
    if (receiver->kind == DART_CLASS) {
        const char *ctor = cbm_arena_sprintf(ctx->walk->arena, "%s%s", DART_CTOR_PREFIX, name);
        if (ctor && cbm_source_lookup(ctx->walk, receiver->class_key, ctor)) {
            ref = *receiver;
            ref.reason = "dart_named_constructor";
            return ref;
        }
    }
    const cbm_source_decl_t *member = NULL;
    cbm_source_chain_t chain = {0};
    dart_lookup_t lookup = receiver->from_super
                               ? dart_find_past_own(ctx, receiver->class_key, name, &chain, &member)
                               : dart_find_member(ctx, receiver->class_key, name, &chain, &member);
    switch (lookup) {
    case DART_FOUND:
        ref = dart_ref_on(ctx, member, receiver->class_key);
        ref.via_receiver = true;
        return ref;
    case DART_OPEN:
        ref.reason = "dart_member_chain_leaves_project";
        return ref;
    case DART_AMBIGUOUS:
        ref.reason = "dart_member_not_one_declaration";
        return ref;
    case DART_ABSENT:
        break;
    }
    ref.reason = "dart_member_absent_from_chain";
    return ref;
}

/* A value invoked like a function runs its class's `call` method. */
static dart_ref_t dart_invoked(dart_ctx_t *ctx, const dart_ref_t *callee) {
    if (callee->kind != DART_INSTANCE || !callee->class_key)
        return *callee;
    dart_ref_t found = dart_member(ctx, callee, "call");
    return found.kind == DART_FUNCTION ? found : *callee;
}

/* ── Phase two: the walk ───────────────────────────────────────────── */

static void dart_bind(dart_ctx_t *ctx, const char *name, const char *class_key) {
    dart_local_t *local = name ? cbm_arena_alloc(ctx->walk->arena, sizeof(*local)) : NULL;
    if (!local)
        return;
    *local = (dart_local_t){name, class_key, ctx->locals};
    ctx->locals = local;
}

static void dart_bind_formal(dart_ctx_t *ctx, TSNode node) {
    TSNode name = cbm_source_field(node, "name");
    bool function_typed = false;
    uint32_t parts = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < parts; i++) {
        TSNode part = ts_node_named_child(node, i);
        if (cbm_source_is(part, "formal_parameter_list")) {
            function_typed = true;
        } else if (cbm_source_is(part, "identifier") && ts_node_is_null(name)) {
            name = part;
        } else if (cbm_source_is(part, "constructor_param") ||
                   cbm_source_is(part, "super_formal_parameter")) {
            /* `this.x` takes the field's declared type; `super.x` is not modelled. */
            const char *text = cbm_source_text(ctx->walk, dart_find(part, "identifier", 7));
            const cbm_source_decl_t *field =
                text && ctx->class_key && cbm_source_is(part, "constructor_param")
                    ? cbm_source_lookup(ctx->walk, ctx->class_key, text)
                    : NULL;
            dart_bind(ctx, text, field ? dart_declared_class(ctx, field) : NULL);
            return;
        }
    }
    const char *type = function_typed ? NULL : dart_type_text(ctx->walk, node, ctx->type_params);
    dart_bind(ctx, cbm_source_text(ctx->walk, name),
              type ? dart_type_class(ctx, ctx->library, type) : NULL);
}

static bool dart_parameters_holder(TSNode node) {
    return cbm_source_is(node, "formal_parameter_list") ||
           cbm_source_is(node, "optional_formal_parameters") ||
           cbm_source_is(node, "method_signature") || cbm_source_is(node, "function_signature") ||
           cbm_source_is(node, "setter_signature") || cbm_source_is(node, "operator_signature") ||
           cbm_source_is(node, "constructor_signature") ||
           cbm_source_is(node, "constant_constructor_signature") ||
           cbm_source_is(node, "factory_constructor_signature") ||
           cbm_source_is(node, "redirecting_factory_constructor_signature");
}

static void dart_bind_parameters(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "formal_parameter"))
            dart_bind_formal(ctx, child);
        else if (dart_parameters_holder(child))
            dart_bind_parameters(ctx, child, depth + 1);
    }
}

static bool dart_selector_is_call(TSNode selector) {
    return cbm_source_is(selector, "selector") &&
           cbm_source_is(ts_node_named_child(selector, 0), "argument_part");
}

static TSNode dart_selector_member_node(TSNode selector) {
    TSNode first =
        cbm_source_is(selector, "selector") ? ts_node_named_child(selector, 0) : selector;
    if (!cbm_source_is(first, "unconditional_assignable_selector") &&
        !cbm_source_is(first, "conditional_assignable_selector"))
        return (TSNode){0};
    uint32_t count = ts_node_named_child_count(first);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(first, i), "identifier"))
            return ts_node_named_child(first, i);
    return (TSNode){0};
}

static const char *dart_selector_member(dart_ctx_t *ctx, TSNode selector) {
    return cbm_source_text(ctx->walk, dart_selector_member_node(selector));
}

static void dart_emit_call(dart_ctx_t *ctx, uint32_t start, uint32_t end, const dart_ref_t *ref) {
    const char *reason = ref->reason;
    const char *target = NULL;
    cbm_resolve_rule_t rule = ref->via_receiver ? CBM_RESOLVE_RULE_TYPE_MEMBER
                              : ref->via_import ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                                : CBM_RESOLVE_RULE_SCOPE_BINDING;
    if (ref->kind == DART_FUNCTION || ref->kind == DART_CLASS) {
        const char *identity = NULL;
        target = cbm_source_unique_qn(ref->decl, &identity);
        if (!target)
            reason = identity;
    }
    cbm_source_emit_facts(ctx->walk, start, end, CBM_SOURCE_SITE_CALL, target, rule,
                          ref->decl ? ref->decl->owner : NULL, reason,
                          ref->kind == DART_CLASS ? CBM_RESOLVE_FACT_CLASS_CALLABLE : 0);
}

static void dart_emit_extra(dart_ctx_t *ctx, uint32_t start, uint32_t end, const char *name,
                            const dart_ref_t *ref) {
    if (!ctx->scope)
        return;
    ctx->walk->extra_scope = ctx->scope;
    ctx->walk->extra_name = name;
    dart_emit_call(ctx, start, end, ref);
    ctx->walk->extra_scope = NULL;
    ctx->walk->extra_name = NULL;
}

static void dart_visit(dart_ctx_t *ctx, TSNode node, unsigned depth);

/* After a call: what the callee's declared return type says the value is. */
static dart_ref_t dart_result_of(dart_ctx_t *ctx, const dart_ref_t *callee) {
    dart_ref_t ref = {.reason = "dart_return_type_unknown"};
    if (callee->kind == DART_CLASS && callee->class_key) {
        ref.kind = DART_INSTANCE;
        ref.class_key = callee->class_key;
    } else if (callee->kind == DART_FUNCTION) {
        const char *cls = dart_member_class(ctx, callee->decl, callee->receiver);
        ref.kind = cls ? DART_INSTANCE : DART_NOTHING;
        ref.class_key = cls;
    }
    return ref;
}

/* A name read as a value follows the same lookup as a call's. */
static void dart_emit_reference(dart_ctx_t *ctx, TSNode node, const dart_ref_t *ref) {
    /* A plain field has no node to be the target of a read. */
    const cbm_source_decl_t *decl = ref->value && ref->value->qn ? ref->value : ref->decl;
    if (!decl || (decl == ref->decl && ref->kind != DART_FUNCTION && ref->kind != DART_CLASS))
        return;
    const char *identity = NULL;
    const char *target = cbm_source_unique_qn(decl, &identity);
    cbm_source_emit_node(ctx->walk, node, CBM_SOURCE_SITE_REFERENCE, target,
                         ref->via_receiver ? CBM_RESOLVE_RULE_TYPE_MEMBER
                         : ref->via_import ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                           : CBM_RESOLVE_RULE_SCOPE_BINDING,
                         decl->owner, target ? ref->reason : identity);
}

static void dart_visit_type_reference(dart_ctx_t *ctx, TSNode node) {
    const char *name = cbm_source_text(ctx->walk, node);
    if (!name || dart_list_has(ctx->type_params, name))
        return;
    const char *reason = NULL;
    bool via_import = false;
    const cbm_source_decl_t *cls = dart_type_decl(ctx, ctx->library, name, &reason, &via_import);
    if (!cls || cls->next)
        return;
    dart_ref_t ref = dart_ref_of_class(cls);
    ref.via_import = via_import;
    dart_emit_reference(ctx, node, &ref);
}

/* Dart keeps an expression's selectors as siblings of its head. This walks one
 * such sequence left to right, tracking what each prefix denotes. */
static dart_ref_t dart_visit_sequence(dart_ctx_t *ctx, TSNode node, unsigned depth,
                                      bool resolve_every_name) {
    dart_ref_t current = {.reason = "dart_computed_receiver"};
    TSNode previous = (TSNode){0};
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    TSNode child = dart_cursor_named(&cursor, &more);
    while (!ts_node_is_null(child)) {
        TSNode next = dart_cursor_named(&cursor, &more);
        if (cbm_source_is(child, "selector")) {
            if (dart_selector_is_call(child)) {
                dart_ref_t invoked = dart_invoked(ctx, &current);
                if (cbm_source_is(previous, "identifier"))
                    dart_emit_call(ctx, ts_node_start_byte(previous), ts_node_end_byte(child),
                                   &invoked);
                dart_visit(ctx, child, depth + 1);
                current = dart_result_of(ctx, &invoked);
            } else if (ts_node_named_child_count(child) > 0) {
                const char *member = dart_selector_member(ctx, child);
                dart_ref_t found = member ? dart_member(ctx, &current, member)
                                          : (dart_ref_t){.reason = "dart_computed_member"};
                if (member && dart_selector_is_call(next)) {
                    if (cbm_source_is(previous, "identifier"))
                        dart_emit_call(ctx, ts_node_start_byte(previous), ts_node_end_byte(child),
                                       &found);
                    else
                        dart_emit_extra(ctx, ts_node_start_byte(child), ts_node_end_byte(next),
                                        member, &found);
                } else if (member) {
                    dart_emit_reference(ctx, dart_selector_member_node(child), &found);
                }
                dart_visit(ctx, child, depth + 1);
                current = found;
            } /* `x!` leaves x's type. */
        } else if (cbm_source_is(child, "unconditional_assignable_selector") &&
                   cbm_source_is(previous, "super")) {
            /* `super.m` keeps its selector beside the keyword, with no `selector` around it. */
            const char *member = dart_selector_member(ctx, child);
            dart_ref_t found = member ? dart_member(ctx, &current, member)
                                      : (dart_ref_t){.reason = "dart_computed_member"};
            if (member && dart_selector_is_call(next))
                dart_emit_extra(ctx, ts_node_start_byte(previous), ts_node_end_byte(next), member,
                                &found);
            else if (member)
                dart_emit_reference(ctx, dart_selector_member_node(child), &found);
            current = found;
        } else if (cbm_source_is(child, "cascade_section")) {
            /* `x..a()..b()`: each section selects on x, and the value stays x. */
            TSNode selector = dart_find(child, "cascade_selector", 7);
            const char *member = cbm_source_text(ctx->walk, dart_find(selector, "identifier", 7));
            bool call = !ts_node_is_null(dart_find(child, "argument_part", 7));
            if (member && call && ts_node_named_child_count(selector) == 1) {
                dart_ref_t found = dart_member(ctx, &current, member);
                dart_emit_extra(ctx, ts_node_start_byte(child), ts_node_end_byte(child), member,
                                &found);
            }
            dart_visit(ctx, child, depth + 1);
        } else if (cbm_source_is(child, "initialized_identifier")) {
            /* `var a = 1, b = 2`: another name of this declaration, typed by itself. */
            dart_ref_t value = dart_visit_sequence(ctx, child, depth + 1, false);
            dart_bind(ctx, cbm_source_text(ctx->walk, ts_node_named_child(child, 0)),
                      value.kind == DART_INSTANCE ? value.class_key : NULL);
        } else {
            current = (dart_ref_t){.reason = "dart_computed_receiver"};
            if (cbm_source_is(child, "identifier")) {
                /* A declaration's own name is not a read of anything. */
                bool declared =
                    ts_node_eq(child, cbm_source_field(node, "name")) ||
                    cbm_source_is(node, "label") ||
                    (cbm_source_is(node, "initialized_identifier") && ts_node_is_null(previous));
                const char *name = declared ? NULL : cbm_source_text(ctx->walk, child);
                if (name) {
                    current = dart_name(ctx, name);
                    /* The head of `Status.ready` reads Status; `f()` is a call instead. */
                    if (!dart_selector_is_call(next))
                        dart_emit_reference(ctx, child, &current);
                }
            } else if (cbm_source_is(child, "this")) {
                if (ctx->class_key)
                    current = (dart_ref_t){.kind = DART_INSTANCE, .class_key = ctx->class_key};
            } else if (cbm_source_is(child, "super")) {
                if (ctx->class_key)
                    current = (dart_ref_t){
                        .kind = DART_INSTANCE, .class_key = ctx->class_key, .from_super = true};
            } else if (cbm_source_is(child, "type_identifier")) {
                dart_visit_type_reference(ctx, child);
            } else {
                dart_visit(ctx, child, depth + 1);
            }
        }
        previous = child;
        child = next;
    }
    ts_tree_cursor_delete(&cursor);
    return current;
}

static void dart_visit_signature_types(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (cbm_source_is(child, "type_identifier"))
            dart_visit_type_reference(ctx, child);
        else if (cbm_source_is(child, "type_arguments") || dart_parameters_holder(child) ||
                 cbm_source_is(child, "formal_parameter") ||
                 cbm_source_is(child, "getter_signature"))
            dart_visit_signature_types(ctx, child, depth + 1);
    }
}

/* `{this.tab = Tab.one, Item? item}`: a default stands beside its parameter, not inside it. It
 * is read in the scope around the signature, so this runs before the parameters are bound. */
static void dart_visit_parameter_defaults(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "optional_formal_parameters")) {
        (void)dart_visit_sequence(ctx, node, depth, false);
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (TSNode child = dart_cursor_named(&cursor, &more); !ts_node_is_null(child);
         child = dart_cursor_named(&cursor, &more))
        if (dart_parameters_holder(child))
            dart_visit_parameter_defaults(ctx, child, depth + 1);
    ts_tree_cursor_delete(&cursor);
}

/* Everything a class's header names: its bases, its mixins, what a type parameter is
 * bounded by, and what a mixin or an extension is `on`. */
static void dart_visit_type_tree(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "type_identifier")) {
        dart_visit_type_reference(ctx, node);
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (TSNode child = dart_cursor_named(&cursor, &more); !ts_node_is_null(child);
         child = dart_cursor_named(&cursor, &more))
        dart_visit_type_tree(ctx, child, depth + 1);
    ts_tree_cursor_delete(&cursor);
}

static void dart_visit_class_header(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (TSNode child = dart_cursor_named(&cursor, &more); !ts_node_is_null(child);
         child = dart_cursor_named(&cursor, &more)) {
        if (cbm_source_is(child, "superclass") || cbm_source_is(child, "interfaces") ||
            cbm_source_is(child, "mixins") || cbm_source_is(child, "type_parameters") ||
            cbm_source_is(child, "type_arguments") || cbm_source_is(child, "type_identifier"))
            dart_visit_type_tree(ctx, child, depth + 1);
    }
    ts_tree_cursor_delete(&cursor);
}

static void dart_visit_function_body(dart_ctx_t *ctx, TSNode signature, TSNode body,
                                     unsigned depth) {
    dart_local_t *saved = ctx->locals;
    const char *saved_params = ctx->type_params;
    const char *saved_scope = ctx->scope;
    /* A named definition is the scope; a closure stays in the one around it. */
    TSNode named = cbm_source_is(signature, "method_signature") ? ts_node_named_child(signature, 0)
                                                                : signature;
    TSNode definition = cbm_source_field(named, "name");
    if (!ts_node_is_null(definition) && !cbm_source_is(signature, "formal_parameter_list")) {
        const char *text = cbm_source_text(ctx->walk, definition);
        ctx->scope =
            text ? cbm_source_def_qn(ctx->walk, text, ts_node_start_point(definition).row + 1)
                 : NULL;
    }
    if (!ts_node_is_null(signature)) {
        dart_visit_parameter_defaults(ctx, signature, depth + 1);
        if (cbm_source_is(signature, "method_signature") && ts_node_named_child_count(signature))
            ctx->type_params =
                dart_type_params(ctx->walk, ts_node_named_child(signature, 0), ctx->type_params);
        ctx->type_params = dart_type_params(ctx->walk, signature, ctx->type_params);
        /* A closure's signature is the parameter list itself. */
        if (cbm_source_is(signature, "formal_parameter_list")) {
            uint32_t count = ts_node_named_child_count(signature);
            for (uint32_t i = 0; i < count; i++) {
                TSNode formal = ts_node_named_child(signature, i);
                if (cbm_source_is(formal, "formal_parameter"))
                    dart_bind_formal(ctx, formal);
                else if (dart_parameters_holder(formal))
                    dart_bind_parameters(ctx, formal, depth + 1);
            }
        } else {
            dart_bind_parameters(ctx, signature, depth + 1);
        }
    }
    /* A signature's types are read in its own type-parameter scope. */
    if (!ts_node_is_null(signature) && !cbm_source_is(signature, "formal_parameter_list")) {
        dart_visit_signature_types(ctx, signature, depth + 1);
        /* `C(x) : y = x, super(x) {`: an initializer list runs with the parameters in scope. */
        uint32_t parts = ts_node_named_child_count(signature);
        for (uint32_t i = 0; i < parts; i++)
            if (cbm_source_is(ts_node_named_child(signature, i), "initializers"))
                dart_visit(ctx, ts_node_named_child(signature, i), depth + 1);
    }
    if (!ts_node_is_null(body))
        dart_visit(ctx, body, depth + 1);
    ctx->locals = saved;
    ctx->type_params = saved_params;
    ctx->scope = saved_scope;
}

/* A declaration's signature is followed by its sibling body. */
static void dart_visit_members(dart_ctx_t *ctx, TSNode body, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    TSNode member = dart_cursor_named(&cursor, &more);
    while (!ts_node_is_null(member)) {
        TSNode next = dart_cursor_named(&cursor, &more);
        bool declaration = cbm_source_is(member, "declaration");
        bool signature = cbm_source_is(member, "method_signature") ||
                         cbm_source_is(member, "function_signature") ||
                         cbm_source_is(member, "getter_signature") ||
                         cbm_source_is(member, "setter_signature");
        bool has_body = cbm_source_is(next, "function_body");
        if (signature && has_body) {
            dart_visit_function_body(ctx, member, next, depth);
        } else if (declaration) {
            /* A constructor's initializers and body run with its parameters in scope. */
            dart_local_t *saved = ctx->locals;
            dart_visit_parameter_defaults(ctx, member, depth + 1);
            dart_bind_parameters(ctx, member, depth + 1);
            dart_visit(ctx, member, depth + 1);
            if (has_body)
                dart_visit(ctx, next, depth + 1);
            ctx->locals = saved;
        } else {
            dart_visit(ctx, member, depth + 1);
        }
        member = (signature || declaration) && has_body ? dart_cursor_named(&cursor, &more) : next;
    }
    ts_tree_cursor_delete(&cursor);
}

/* Every identifier a pattern holds binds a name here; a field label among them can
 * only make a later use abstain. */
static void dart_bind_pattern(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "identifier")) {
        dart_bind(ctx, cbm_source_text(ctx->walk, node), NULL);
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        dart_bind_pattern(ctx, ts_node_named_child(node, i), depth + 1);
}

static void dart_visit_local_definition(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    /* `final x = Store();` holds type, name and value as siblings. */
    const char *type = dart_type_text(ctx->walk, node, ctx->type_params);
    bool typed = dart_type_written(node);
    const char *declared = type ? dart_type_class(ctx, ctx->library, type) : NULL;
    dart_ref_t value = dart_visit_sequence(ctx, node, depth, true);
    const char *cls = declared;
    if (!typed && value.kind == DART_INSTANCE)
        cls = value.class_key;
    dart_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(node, "name")), cls);
}

static void dart_visit_new(dart_ctx_t *ctx, TSNode node) {
    /* `new Foo(...)` or `new Foo.named(...)`; a prefixed form is not modelled. */
    TSNode head = ts_node_named_child(node, 0), second = ts_node_named_child(node, 1);
    const char *name = cbm_source_text(ctx->walk, head);
    if (!name || !cbm_source_is(head, "type_identifier") ||
        cbm_source_is(second, "type_identifier"))
        return;
    const char *reason = NULL;
    bool via_import = false;
    const cbm_source_decl_t *cls = dart_type_decl(ctx, ctx->library, name, &reason, &via_import);
    dart_ref_t ref = {.reason = reason ? reason : "dart_name_not_declared_in_project"};
    if (cls) {
        ref = dart_ref_of_class(cls);
        ref.via_import = via_import;
    }
    dart_emit_call(ctx, ts_node_start_byte(node), ts_node_end_byte(node), &ref);
}

static void dart_visit(dart_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    if (dart_class_kind(node)) {
        const char *name = dart_class_name(ctx->walk, node);
        const char *saved = ctx->class_key;
        const char *saved_params = ctx->type_params;
        ctx->class_key =
            name ? cbm_arena_sprintf(ctx->walk->arena, "%s#%s", ctx->library, name) : NULL;
        ctx->type_params = dart_type_params(ctx->walk, node, NULL);
        dart_visit_class_header(ctx, node, depth + 1);
        TSNode body = dart_class_body(node);
        if (!ts_node_is_null(body))
            dart_visit_members(ctx, body, depth + 1);
        ctx->class_key = saved;
        ctx->type_params = saved_params;
        return;
    }
    if (!strcmp(kind, "block") || !strcmp(kind, "for_statement") || !strcmp(kind, "for_element") ||
        !strcmp(kind, "try_statement") || !strcmp(kind, "if_statement") ||
        !strcmp(kind, "if_element") || !strcmp(kind, "switch_statement_case") ||
        !strcmp(kind, "switch_expression_case")) {
        dart_local_t *saved = ctx->locals;
        (void)dart_visit_sequence(ctx, node, depth, false);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "function_expression")) {
        dart_visit_function_body(ctx, cbm_source_field(node, "parameters"),
                                 cbm_source_field(node, "body"), depth);
        return;
    }
    if (!strcmp(kind, "lambda_expression")) {
        TSNode signature = cbm_source_field(node, "parameters");
        dart_bind(ctx, cbm_source_text(ctx->walk, cbm_source_field(signature, "name")), NULL);
        dart_visit_function_body(ctx, signature, cbm_source_field(node, "body"), depth);
        return;
    }
    if (!strcmp(kind, "initialized_variable_definition")) {
        dart_visit_local_definition(ctx, node, depth);
        return;
    }
    /* A member's name after a dot is spelled, not read, and so is a constructor's. */
    if (!strcmp(kind, "unconditional_assignable_selector") ||
        !strcmp(kind, "conditional_assignable_selector") || !strcmp(kind, "cascade_selector"))
        return;
    /* Parameters declare; their defaults are read on the way into their signature. */
    if (!strcmp(kind, "formal_parameter_list") || !strcmp(kind, "formal_parameter"))
        return;
    /* `final Tab x = ...;` at the top of a file keeps its type beside the names. */
    if (!strcmp(kind, "type_identifier")) {
        dart_visit_type_reference(ctx, node);
        return;
    }
    if (!strcmp(kind, "constructor_signature") || !strcmp(kind, "constant_constructor_signature") ||
        !strcmp(kind, "factory_constructor_signature") ||
        !strcmp(kind, "redirecting_factory_constructor_signature")) {
        dart_visit_signature_types(ctx, node, depth + 1);
        return;
    }
    if (!strcmp(kind, "assignment_expression")) {
        /* A name assigned here is a variable from here on, whatever else it could be. */
        TSNode left = cbm_source_field(node, "left");
        TSNode target =
            cbm_source_is(left, "assignable_expression") && ts_node_named_child_count(left) == 1
                ? ts_node_named_child(left, 0)
                : (TSNode){0};
        (void)dart_visit_sequence(ctx, node, depth, false);
        if (cbm_source_is(target, "identifier"))
            dart_bind(ctx, cbm_source_text(ctx->walk, target), NULL);
        return;
    }
    if (!strcmp(kind, "pattern_variable_declaration")) {
        /* The value first, in the scope before these names exist. */
        uint32_t count = ts_node_named_child_count(node);
        (void)dart_visit_sequence(ctx, node, depth, false);
        for (uint32_t i = 0; i < count; i++) {
            TSNode part = ts_node_named_child(node, i);
            if (strstr(ts_node_type(part), "pattern"))
                dart_bind_pattern(ctx, part, depth + 1);
        }
        return;
    }
    if (!strcmp(kind, "qualified")) {
        /* `case Status.ready:` spells the member; it is not a bare name in scope. */
        const char *reason = NULL;
        bool via_import = false;
        TSNode head = ts_node_named_child(node, 0), member = ts_node_named_child(node, 1);
        const char *name =
            cbm_source_is(head, "type_identifier") ? cbm_source_text(ctx->walk, head) : NULL;
        const cbm_source_decl_t *cls =
            name && !dart_list_has(ctx->type_params, name)
                ? dart_type_decl(ctx, ctx->library, name, &reason, &via_import)
                : NULL;
        if (cls && !cls->next) {
            dart_ref_t ref = dart_ref_of_class(cls);
            ref.via_import = via_import;
            dart_emit_reference(ctx, head, &ref);
            const char *text =
                cbm_source_is(member, "identifier") ? cbm_source_text(ctx->walk, member) : NULL;
            if (text) {
                dart_ref_t found = dart_member(ctx, &ref, text);
                dart_emit_reference(ctx, member, &found);
            }
        }
        return;
    }
    if (!strcmp(kind, "variable_pattern")) {
        /* `case Sub s:` is a declared type and a name of it. */
        TSNode type = ts_node_named_child(node, 0);
        uint32_t parts = ts_node_named_child_count(node);
        if (cbm_source_is(type, "type_identifier") && parts == 2 &&
            cbm_source_is(ts_node_named_child(node, 1), "identifier")) {
            dart_visit_type_reference(ctx, type);
            const char *name = cbm_source_text(ctx->walk, type);
            dart_bind(ctx, cbm_source_text(ctx->walk, ts_node_named_child(node, 1)),
                      name && !dart_list_has(ctx->type_params, name)
                          ? dart_type_class(ctx, ctx->library, name)
                          : NULL);
            return;
        }
        dart_bind_pattern(ctx, node, depth + 1);
        return;
    }
    if (!strcmp(kind, "catch_parameters")) {
        dart_bind_pattern(ctx, node, depth + 1);
        return;
    }
    if (!strcmp(kind, "for_loop_parts")) {
        TSNode name = cbm_source_field(node, "name");
        if (!ts_node_is_null(name)) {
            /* The element type is read only when declared. */
            const char *type = dart_type_text(ctx->walk, node, ctx->type_params);
            dart_bind(ctx, cbm_source_text(ctx->walk, name),
                      type ? dart_type_class(ctx, ctx->library, type) : NULL);
        }
    }
    if (!strcmp(kind, "new_expression"))
        dart_visit_new(ctx, node);
    (void)dart_visit_sequence(ctx, node, depth, false);
}

static void dart_resolve(cbm_source_walk_t *walk) {
    bool named_part = false;
    dart_ctx_t ctx = {.walk = walk, .library = dart_library_of_file(walk, &named_part)};
    if (named_part) {
        cbm_source_fail(walk, "dart_part_of_library_name");
        return;
    }
    if (!ctx.library) {
        cbm_source_fail(walk, "dart_part_above_root");
        return;
    }
    ctx.libraries = cbm_ht_create(16);
    ctx.import_memo = cbm_ht_create(64);
    ctx.imports_of = cbm_ht_create(64);
    ctx.prefixes = cbm_ht_create(16);
    if (ctx.libraries && ctx.import_memo && ctx.imports_of && ctx.prefixes)
        dart_visit_members(&ctx, walk->root, 0);
    else
        cbm_source_fail(walk, "dart_library_allocation");
    cbm_ht_free(ctx.libraries);
    cbm_ht_free(ctx.import_memo);
    cbm_ht_free(ctx.imports_of);
    cbm_ht_free(ctx.prefixes);
}

/* The one shared name Dart has is a getter and a setter, which are one node here. */
const cbm_source_adapter_t cbm_source_adapter_dart = {.language = CBM_LANG_DART,
                                                      .collect = dart_collect,
                                                      .resolve = dart_resolve,
                                                      .one_name_one_node = true,
                                                      .before_binding_rules = true,
                                                      .decides_references = true};
