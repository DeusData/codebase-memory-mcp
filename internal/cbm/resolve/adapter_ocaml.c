/* OCaml binding rules, from the manual's module language chapter.
 *
 * Scope is sequential. A `let` is visible to what follows it, and to itself only
 * with `rec`. A local let, parameter, fun or match pattern shadows anything outer.
 * `open M` brings M's names in from that point, shadowing earlier ones, so an open
 * of a module whose contents are not known leaves every earlier name uncertain.
 * Two top-level lets of one name are two declarations with one graph identity,
 * which the resolver's target check refuses.
 *
 * A file foo.ml is the compilation unit Foo. Within one directory a build's modules
 * see each other by name, unless something in scope defines a module of that name.
 * `M.f` therefore looks through the modules opens in scope bring in and local module
 * aliases, then at the unit beside this file, then at a library of that name.
 *
 * A dune file is a fact about this: `(library (name foo))` makes foo.ml in that
 * directory the module Foo everywhere in the project, or with no foo.ml makes each unit
 * there a member of Foo; `(include_subdirs unqualified)` makes the whole subtree one
 * directory; `qualified` lets a unit see the directories above it and reads a
 * subdirectory as a module.
 *
 * `include M` gives a unit what M has from that line on, and a later binding of one
 * name replaces an earlier one. M can be a path, a literal `struct`, or an application
 * of a functor, whose result has what the functor's own `struct` has. A functor's
 * parameter has what its module type says and no more: a name there has no declaration
 * to point at, but what is not there cannot come from it. A unit is closed when
 * everything it includes is known; a module outside the project is not, and an open of
 * such a unit leaves any name uncertain. `module X = Y` reads Y where it is written,
 * before X exists, under the opens and includes of the `struct` around it. A unit is not
 * in scope in its own body, so `include List` in list.ml is some other List. A functor
 * argument's own names are its own; a first class module is not followed.
 *
 * A unit's interface says what it shows: a name a closed foo.mli does not list cannot be
 * reached from outside, whatever foo.ml has, and a unit whose interface is closed is closed
 * whatever foo.ml includes. The declarations are still the .ml's.
 *
 * The standard library's modules are known by name, and so is which modules each holds.
 * Its values are not listed, so a name read through one abstains. A module type's name
 * is kept apart from a module's by a `^`, which no identifier can begin with.
 *
 * `M.(e)` and `let open M in e` hold M open for e alone. */
#include "source_adapter_internal.h"
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>

#define OCAML_OPEN_ENDED "&open_ended"
#define OCAML_MODULE_PREFIX "&module:"
#define OCAML_DECL_RECURSIVE (1u << 20) /* `module rec`: in scope in its own body */
/* A functor's parameter: its module type is read where the functor is written, whose line is
 * in arity_min, because the name itself is in scope from the functor's first line. */
#define OCAML_DECL_PARAMETER (1u << 21)
#define OCAML_INCLUDE "&include"
/* A directory read as a module: its units are its members. */
#define OCAML_DIRECTORY_PREFIX "&dir:"
/* Under `include_subdirs unqualified`, the units of a whole subtree by module name. */
#define OCAML_SUBTREE_PREFIX "&subtree:"
#define OCAML_LIBRARY_KEY "@cbm:raw:ocaml-lib:"
#define OCAML_DIR_KEY "@cbm:raw:ocaml-dir:"
#define OCAML_SUBDIRS_KEY "@cbm:raw:ocaml-subdirs:"
#define OCAML_BLOCKED_PREFIX "@cbm:"
#define OCAML_STDLIB_PREFIX "&stdlib:"

enum { OCAML_MAX_OPENS = 32 };

typedef struct ocaml_name {
    const char *name;
    struct ocaml_name *next;
} ocaml_name_t;

typedef struct ocaml_open {
    const char *unit; /* a project unit's key, NULL when the module is not known */
    uint32_t at, line;
    bool include;
    struct ocaml_open *next;
} ocaml_open_t;

typedef struct {
    cbm_source_walk_t *walk;
    bool reference;     /* the name being read is a value, not an application */
    uint32_t site_line; /* the occurrence being decided */
    const char *unit;
    ocaml_name_t *locals;
    ocaml_open_t *opens;
    /* This file's answers. Every site reads the same few opens, each with a closure of
     * includes behind it, so each (unit, name) is worked out once per file. */
    CBMHashTable *closed_units;
    CBMHashTable *member_modules;
    CBMHashTable *member_values;
    CBMHashTable *decl_paths;
    bool in_functor_argument; /* a `let` here binds a name of the argument's own */
} ocaml_ctx_t;

typedef enum { OCAML_FOUND, OCAML_ABSENT, OCAML_UNKNOWN } ocaml_lookup_t;

/* src/foo/bar.ml is unit key src/foo/bar; its module name is Bar. */
static const char *ocaml_unit_key(cbm_source_walk_t *walk) {
    const char *file = walk->file ? walk->file : "";
    const char *dot = strrchr(file, '.');
    return dot ? cbm_arena_strndup(walk->arena, file, (size_t)(dot - file)) : file;
}

static const char *ocaml_manifest(cbm_source_walk_t *walk, const char *prefix, const char *key) {
    const cbm_source_table_t *table = walk->table ? walk->table : walk->writable;
    char name[640];
    int length = snprintf(name, sizeof(name), "%s%s", prefix, key);
    const char *value =
        length > 0 && (size_t)length < sizeof(name) ? cbm_source_table_package(table, name) : NULL;
    return value && strncmp(value, OCAML_BLOCKED_PREFIX, strlen(OCAML_BLOCKED_PREFIX)) ? value
                                                                                       : NULL;
}

/* The directory holding a unit key: "." at the root, so a manifest can name it. */
static const char *ocaml_directory_of(cbm_source_walk_t *walk, const char *unit) {
    const char *hash = strchr(unit, '#');
    size_t length = hash ? (size_t)(hash - unit) : strlen(unit);
    const char *slash = NULL;
    for (size_t i = 0; i < length; i++)
        if (unit[i] == '/')
            slash = unit + i;
    return slash ? cbm_arena_strndup(walk->arena, unit, (size_t)(slash - unit)) : ".";
}

static const char *ocaml_parent_directory(cbm_source_walk_t *walk, const char *directory) {
    const char *slash = strrchr(directory, '/');
    if (slash)
        return cbm_arena_strndup(walk->arena, directory, (size_t)(slash - directory));
    return strcmp(directory, ".") ? "." : NULL;
}

static const char *ocaml_lowered(cbm_source_walk_t *walk, const char *module) {
    char *copy = cbm_arena_strdup(walk->arena, module);
    if (copy)
        copy[0] = (char)tolower((unsigned char)copy[0]);
    return copy;
}

/* A unit of this directory, by its file or, under `unqualified`, anywhere below it. */
static const char *ocaml_directory_member(const ocaml_ctx_t *ctx, const char *directory,
                                          const char *lowered) {
    const char *key = strcmp(directory, ".")
                          ? cbm_arena_sprintf(ctx->walk->arena, "%s/%s", directory, lowered)
                          : lowered;
    if (key && cbm_source_lookup(ctx->walk, key, ""))
        return key;
    const char *subtree =
        cbm_arena_sprintf(ctx->walk->arena, "%s%s", OCAML_SUBTREE_PREFIX, directory);
    const cbm_source_decl_t *found =
        subtree ? cbm_source_lookup(ctx->walk, subtree, lowered) : NULL;
    return found && !found->next ? found->type : NULL;
}

/* The module a name denotes from a unit's place in the tree, with nothing in scope. */
static const char *ocaml_sibling_unit(const ocaml_ctx_t *ctx, const char *from,
                                      const char *module) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *lowered = ocaml_lowered(walk, module);
    if (!lowered)
        return NULL;
    for (const char *directory = ocaml_directory_of(walk, from); directory;
         directory = ocaml_parent_directory(walk, directory)) {
        const char *unit = ocaml_directory_member(ctx, directory, lowered);
        if (unit)
            return unit;
        const char *subdirs = ocaml_manifest(walk, OCAML_SUBDIRS_KEY, directory);
        /* `qualified` reads a subdirectory as a module, and looks up through its parents
         * as far as the directory that declares it. Everything else stops here. */
        if (subdirs && !strcmp(subdirs, "qualified")) {
            const char *below = strcmp(directory, ".")
                                    ? cbm_arena_sprintf(walk->arena, "%s/%s", directory, lowered)
                                    : lowered;
            if (below &&
                cbm_source_lookup(
                    walk, cbm_arena_sprintf(walk->arena, "%s%s", OCAML_DIRECTORY_PREFIX, below),
                    ""))
                return cbm_arena_sprintf(walk->arena, "%s%s", OCAML_DIRECTORY_PREFIX, below);
            break;
        }
        /* Below an include_subdirs the units are still that library's. */
        const char *parent = ocaml_parent_directory(walk, directory);
        if (!parent || !ocaml_manifest(walk, OCAML_SUBDIRS_KEY, parent))
            break;
    }
    return NULL;
}

/* A library's module: its main unit, or with none its directory, whose units are then
 * the members. A library can name one outside what the project built only by its name. */
static const char *ocaml_library_module(const ocaml_ctx_t *ctx, const char *module) {
    cbm_source_walk_t *walk = ctx->walk;
    const char *lowered = ocaml_lowered(walk, module);
    const char *directory = lowered ? ocaml_manifest(walk, OCAML_LIBRARY_KEY, lowered) : NULL;
    if (!directory)
        return NULL;
    const char *main_unit = ocaml_directory_member(ctx, directory, lowered);
    return main_unit ? main_unit
                     : cbm_arena_sprintf(walk->arena, "%s%s", OCAML_DIRECTORY_PREFIX, directory);
}

static bool ocaml_is_directory_module(const char *container) {
    return !strncmp(container, OCAML_DIRECTORY_PREFIX, strlen(OCAML_DIRECTORY_PREFIX));
}

/* From the manual's chapter on the standard library, sorted for bsearch. A module that holds
 * modules is followed by them; a functor's result holds none. Those with holdings this does
 * not need (Bigarray, Ephemeron, Obj) are left out, and so stay unknown. */
static const char *const OCAML_STDLIB_MODULES[] = {
    "Arg",
    "Array",
    "ArrayLabels",
    "Atomic",
    "Bool",
    "Buffer",
    "Bytes",
    "BytesLabels",
    "Callback",
    "Char",
    "Complex",
    "Condition",
    "Digest",
    "Digest.BLAKE128",
    "Digest.BLAKE256",
    "Digest.BLAKE512",
    "Digest.MD5",
    "Domain",
    "Domain.DLS",
    "Dynarray",
    "Effect",
    "Effect.Deep",
    "Effect.Shallow",
    "Either",
    "Filename",
    "Float",
    "Float.Array",
    "Float.ArrayLabels",
    "Format",
    "Fun",
    "Gc",
    "Gc.Memprof",
    "Hashtbl",
    "Hashtbl.Make",
    "Hashtbl.MakeSeeded",
    "In_channel",
    "Int",
    "Int32",
    "Int64",
    "Lazy",
    "Lexing",
    "List",
    "ListLabels",
    "Map",
    "Map.Make",
    "Marshal",
    "MoreLabels",
    "MoreLabels.Hashtbl",
    "MoreLabels.Hashtbl.Make",
    "MoreLabels.Hashtbl.MakeSeeded",
    "MoreLabels.Map",
    "MoreLabels.Map.Make",
    "MoreLabels.Set",
    "MoreLabels.Set.Make",
    "Mutex",
    "Nativeint",
    "Oo",
    "Option",
    "Out_channel",
    "Parsing",
    "Printexc",
    "Printexc.Slot",
    "Printf",
    "Queue",
    "Random",
    "Random.State",
    "Result",
    "Scanf",
    "Scanf.Scanning",
    "Semaphore",
    "Semaphore.Binary",
    "Semaphore.Counting",
    "Seq",
    "Set",
    "Set.Make",
    "Stack",
    "StdLabels",
    "StdLabels.Array",
    "StdLabels.Bytes",
    "StdLabels.List",
    "StdLabels.String",
    "String",
    "StringLabels",
    "Sys",
    "Sys.Immediate64",
    "Type",
    "Type.Id",
    "Uchar",
    "Unit",
    "Weak",
    "Weak.Make",
};

static int ocaml_stdlib_compare(const void *key, const void *entry) {
    return strcmp((const char *)key, *(const char *const *)entry);
}

static bool ocaml_is_stdlib(const char *container) {
    return !strncmp(container, OCAML_STDLIB_PREFIX, strlen(OCAML_STDLIB_PREFIX));
}

/* A module of the standard library by its path below Stdlib, or NULL. */
static const char *ocaml_stdlib_module(const ocaml_ctx_t *ctx, const char *path) {
    bool known = bsearch(path, OCAML_STDLIB_MODULES,
                         sizeof(OCAML_STDLIB_MODULES) / sizeof(OCAML_STDLIB_MODULES[0]),
                         sizeof(OCAML_STDLIB_MODULES[0]), ocaml_stdlib_compare) != NULL;
    return known ? cbm_arena_sprintf(ctx->walk->arena, "%s%s", OCAML_STDLIB_PREFIX, path) : NULL;
}

/* `from`, a module inside it, or its interface, is this unit. */
static bool ocaml_is_own_unit(const char *unit, const char *from) {
    size_t length = strlen(unit);
    return !strncmp(unit, from, length) &&
           (from[length] == '\0' || from[length] == '#' || from[length] == '.');
}

/* The module a name denotes by place alone. A unit is not in scope in its own body. */
static const char *ocaml_unit_by_place(const ocaml_ctx_t *ctx, const char *from,
                                       const char *module) {
    const char *unit = ocaml_sibling_unit(ctx, from, module);
    if (!unit || ocaml_is_own_unit(unit, from))
        unit = ocaml_library_module(ctx, module);
    if (unit && !ocaml_is_own_unit(unit, from))
        return unit;
    return !strcmp(module, "Stdlib") ? OCAML_STDLIB_PREFIX : ocaml_stdlib_module(ctx, module);
}

static TSNode ocaml_child(TSNode node, const char *kind) {
    if (ts_node_is_null(node))
        return (TSNode){0};
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (cbm_source_is(ts_node_named_child(node, i), kind))
            return ts_node_named_child(node, i);
    return (TSNode){0};
}

/* ── Phase one ─────────────────────────────────────────────────────── */

static void ocaml_collect_binding(cbm_source_walk_t *walk, const char *owner, TSNode binding) {
    TSNode pattern = cbm_source_field(binding, "pattern");
    if (!cbm_source_is(pattern, "value_name")) {
        /* A destructuring let binds values, and those shadow functions by name. */
        return;
    }
    const char *name = cbm_source_text(walk, pattern);
    if (name) {
        uint32_t line = ts_node_start_point(binding).row + 1;
        (void)cbm_source_declare(walk, owner, name, cbm_source_def_qn(walk, name, line), 0, line);
    }
}

static void ocaml_collect_structure(cbm_source_walk_t *walk, const char *owner, TSNode structure,
                                    const char **opens, size_t *open_count, unsigned depth);

/* A module path is read where it is written, under the opens in force there. Each is kept
 * as "line:path", or "line+path" for an include, because a binding after an open comes before
 * it. */
static const char *const *ocaml_opens_copy(cbm_source_walk_t *walk, const char *const *opens,
                                           size_t count) {
    if (!count)
        return NULL;
    const char **copy = cbm_source_table_alloc(walk, (count + 1) * sizeof(*copy));
    for (size_t o = 0; copy && o < count; o++)
        copy[o] = cbm_source_table_intern(walk, opens[o]);
    return copy;
}

/* What is open at this point of a struct. Past what can be kept, the last one reads as a
 * module no one knows, and so does an empty path. */
static void ocaml_push_open(cbm_source_walk_t *walk, const char **opens, size_t *count,
                            uint32_t line, const char *path, bool include) {
    const char *entry =
        cbm_arena_sprintf(walk->arena, "%u%c%s", line, include ? '+' : ':', path ? path : "");
    if (!entry)
        return;
    if (*count < OCAML_MAX_OPENS)
        opens[(*count)++] = entry;
    else
        opens[OCAML_MAX_OPENS - 1] = cbm_arena_sprintf(walk->arena, "%u:", line);
}

/* A literal struct inlined here: its opens end with it, and what it includes stays. */
static void ocaml_collect_inlined(cbm_source_walk_t *walk, const char *owner, TSNode structure,
                                  const char **opens, size_t *open_count, unsigned depth) {
    const char *inner[OCAML_MAX_OPENS];
    size_t inner_count = *open_count;
    memcpy(inner, opens, inner_count * sizeof(*inner));
    ocaml_collect_structure(walk, owner, structure, inner, &inner_count, depth + 1);
    for (size_t i = *open_count; i < inner_count; i++) {
        char *rest = NULL;
        unsigned long line = strtoul(inner[i], &rest, 10);
        if (rest && *rest == '+')
            ocaml_push_open(walk, opens, open_count, (uint32_t)line, rest + 1, true);
    }
}

/* The path a module expression reads: M, M of `(M : S)`, F of `F (A) (B)`. */
static TSNode ocaml_module_read(TSNode body) {
    while (cbm_source_is(body, "typed_module_expression") ||
           cbm_source_is(body, "module_application") ||
           cbm_source_is(body, "parenthesized_module_expression"))
        body = ts_node_named_child_count(body) ? ts_node_named_child(body, 0) : (TSNode){0};
    return cbm_source_is(body, "module_path") ? body : (TSNode){0};
}

/* `include M`: what it adds is known only through M, and is in scope below it. */
static void ocaml_collect_include(cbm_source_walk_t *walk, const char *owner, TSNode item,
                                  const char **outer_opens, size_t *outer_count, unsigned depth) {
    uint32_t line = ts_node_start_point(item).row + 1;
    TSNode body = ts_node_named_child_count(item) ? ts_node_named_child(item, 0) : (TSNode){0};
    if (cbm_source_is(body, "typed_module_expression") && ts_node_named_child_count(body))
        body = ts_node_named_child(body, 0);
    if (cbm_source_is(body, "structure")) {
        ocaml_collect_inlined(walk, owner, body, outer_opens, outer_count, depth);
        return;
    }
    const char *path = cbm_source_text(walk, ocaml_module_read(body));
    if (path) {
        cbm_source_decl_t *decl =
            cbm_source_declare(walk, owner, OCAML_INCLUDE, NULL, CBM_SOURCE_DECL_F_VALUE, line);
        if (decl) {
            decl->type = cbm_source_table_intern(walk, path);
            decl->bases = ocaml_opens_copy(walk, outer_opens, *outer_count);
        }
    } else {
        (void)cbm_source_declare(walk, owner, OCAML_OPEN_ENDED, NULL, CBM_SOURCE_DECL_F_OPEN, line);
    }
    ocaml_push_open(walk, outer_opens, outer_count, line, path, true);
}

/* `A.S` as a module type: the last name is kept apart from a module's. */
static const char *ocaml_module_type_path(cbm_source_walk_t *walk, TSNode path) {
    const char *text = cbm_source_text(walk, path);
    const char *dot = text ? strrchr(text, '.') : NULL;
    if (!text)
        return NULL;
    return dot ? cbm_arena_sprintf(walk->arena, "%.*s.^%s", (int)(dot - text), text, dot + 1)
               : cbm_arena_sprintf(walk->arena, "^%s", text);
}

/* `(S with type t = int)` promises what S does. */
static TSNode ocaml_module_type_read(TSNode type) {
    while (cbm_source_is(type, "module_type_constraint") ||
           cbm_source_is(type, "parenthesized_module_type"))
        type = ts_node_named_child_count(type) ? ts_node_named_child(type, 0) : (TSNode){0};
    return type;
}

static void ocaml_collect_signature(cbm_source_walk_t *walk, const char *owner, TSNode signature,
                                    const char *const *outer_opens, size_t outer_count,
                                    unsigned depth);

/* What a module type says goes on the declaration: its own scope for a `sig`, a path for a
 * name. Anything else leaves the type NULL, which is a module no one can read. */
static void ocaml_declare_module_type(cbm_source_walk_t *walk, cbm_source_decl_t *decl,
                                      const char *scope, TSNode type, const char *const *opens,
                                      size_t open_count, unsigned depth) {
    if (!decl)
        return;
    type = ocaml_module_type_read(type);
    if (cbm_source_is(type, "signature") && scope) {
        decl->type = cbm_source_table_intern(walk, scope);
        (void)cbm_source_declare(walk, scope, "", NULL, CBM_SOURCE_DECL_F_TYPE, 0);
        ocaml_collect_signature(walk, scope, type, opens, open_count, depth + 1);
    } else if (cbm_source_is(type, "module_type_path")) {
        decl->flags |= CBM_SOURCE_DECL_F_VALUE;
        decl->type = cbm_source_table_intern(walk, ocaml_module_type_path(walk, type));
        decl->bases = ocaml_opens_copy(walk, opens, open_count);
    }
}

static void ocaml_collect_module_type(cbm_source_walk_t *walk, const char *owner, TSNode item,
                                      const char *const *opens, size_t open_count, unsigned depth) {
    const char *name = cbm_source_text(walk, ocaml_child(item, "module_type_name"));
    uint32_t parts = ts_node_named_child_count(item);
    if (!name)
        return;
    cbm_source_decl_t *decl = cbm_source_declare(
        walk, owner, cbm_arena_sprintf(walk->arena, "%s^%s", OCAML_MODULE_PREFIX, name), NULL,
        CBM_SOURCE_DECL_F_TYPE, ts_node_start_point(item).row + 1);
    if (parts > 1)
        ocaml_declare_module_type(walk, decl, cbm_arena_sprintf(walk->arena, "%s#^%s", owner, name),
                                  ts_node_named_child(item, parts - 1), opens, open_count, depth);
}

/* A signature's names have no declaration behind them. Knowing them shows what cannot come
 * from there. */
static void ocaml_collect_signature(cbm_source_walk_t *walk, const char *owner, TSNode signature,
                                    const char *const *outer_opens, size_t outer_count,
                                    unsigned depth) {
    if (!cbm_source_step(walk, depth))
        return;
    const char *opens[OCAML_MAX_OPENS];
    size_t open_count = outer_count;
    if (outer_count)
        memcpy(opens, outer_opens, outer_count * sizeof(*opens));
    TSTreeCursor cursor = ts_tree_cursor_new(signature);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode item = ts_tree_cursor_current_node(&cursor);
        uint32_t line = ts_node_start_point(item).row + 1;
        if (cbm_source_is(item, "value_specification") || cbm_source_is(item, "external")) {
            const char *name = cbm_source_text(walk, ocaml_child(item, "value_name"));
            if (name)
                (void)cbm_source_declare(walk, owner, name, NULL, 0, line);
        } else if (cbm_source_is(item, "module_definition")) {
            TSNode binding = ocaml_child(item, "module_binding");
            const char *name = cbm_source_text(walk, ocaml_child(binding, "module_name"));
            uint32_t parts = ts_node_is_null(binding) ? 0 : ts_node_named_child_count(binding);
            if (!name || parts < 2)
                continue;
            TSNode body = ts_node_named_child(binding, parts - 1);
            cbm_source_decl_t *decl = cbm_source_declare(
                walk, owner, cbm_arena_sprintf(walk->arena, "%s%s", OCAML_MODULE_PREFIX, name),
                NULL, CBM_SOURCE_DECL_F_TYPE, line);
            if (cbm_source_is(body, "module_path") && decl) {
                decl->flags |= CBM_SOURCE_DECL_F_VALUE;
                decl->type = cbm_source_table_intern(walk, cbm_source_text(walk, body));
                decl->bases = ocaml_opens_copy(walk, opens, open_count);
            } else {
                ocaml_declare_module_type(walk, decl,
                                          cbm_arena_sprintf(walk->arena, "%s#%s", owner, name),
                                          body, opens, open_count, depth);
            }
        } else if (cbm_source_is(item, "module_type_definition")) {
            ocaml_collect_module_type(walk, owner, item, opens, open_count, depth);
        } else if (cbm_source_is(item, "include_module_type")) {
            TSNode type = ts_node_named_child_count(item)
                              ? ocaml_module_type_read(ts_node_named_child(item, 0))
                              : (TSNode){0};
            const char *path =
                cbm_source_is(type, "module_type_path") ? ocaml_module_type_path(walk, type) : NULL;
            /* `include module type of M`, with or without `struct include M end`, has what M
             * has. */
            TSNode of = cbm_source_is(type, "module_type_of") && ts_node_named_child_count(type)
                            ? ts_node_named_child(type, 0)
                            : (TSNode){0};
            if (cbm_source_is(of, "structure")) {
                ocaml_collect_inlined(walk, owner, of, opens, &open_count, depth);
                continue;
            }
            if (!ts_node_is_null(of))
                path = cbm_source_text(walk, ocaml_module_read(of));
            if (path) {
                cbm_source_decl_t *decl = cbm_source_declare(walk, owner, OCAML_INCLUDE, NULL,
                                                             CBM_SOURCE_DECL_F_VALUE, line);
                if (decl) {
                    decl->type = cbm_source_table_intern(walk, path);
                    decl->bases = ocaml_opens_copy(walk, opens, open_count);
                }
            } else {
                (void)cbm_source_declare(walk, owner, OCAML_OPEN_ENDED, NULL,
                                         CBM_SOURCE_DECL_F_OPEN, line);
            }
            ocaml_push_open(walk, opens, &open_count, line, path, true);
        } else if (cbm_source_is(item, "open_module")) {
            ocaml_push_open(
                walk, opens, &open_count, line,
                cbm_source_text(walk, ocaml_module_read(cbm_source_field(item, "module"))), false);
        }
    }
    ts_tree_cursor_delete(&cursor);
}

/* `module Make (X : S) = struct`: inside, X is a module with what S says, whatever else goes
 * by that name. */
static void ocaml_collect_parameters(cbm_source_walk_t *walk, const char *nested, TSNode binding,
                                     const char *const *opens, size_t open_count, uint32_t line,
                                     unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(binding);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode child = ts_tree_cursor_current_node(&cursor);
        if (!cbm_source_is(child, "module_parameter"))
            continue;
        const char *name = cbm_source_text(walk, ocaml_child(child, "module_name"));
        uint32_t parts = ts_node_named_child_count(child);
        if (!name || parts < 2)
            continue;
        cbm_source_decl_t *decl = cbm_source_declare(
            walk, nested, cbm_arena_sprintf(walk->arena, "%s%s", OCAML_MODULE_PREFIX, name), NULL,
            CBM_SOURCE_DECL_F_TYPE | OCAML_DECL_PARAMETER, 0);
        if (decl)
            decl->arity_min = (int)line;
        ocaml_declare_module_type(walk, decl,
                                  cbm_arena_sprintf(walk->arena, "%s#^(%s", nested, name),
                                  ts_node_named_child(child, parts - 1), opens, open_count, depth);
    }
    ts_tree_cursor_delete(&cursor);
}

static void ocaml_collect_structure(cbm_source_walk_t *walk, const char *owner, TSNode structure,
                                    const char **opens, size_t *open_count, unsigned depth) {
    if (!cbm_source_step(walk, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(structure);
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode item = ts_tree_cursor_current_node(&cursor);
        if (!ts_node_is_named(item))
            continue;
        if (cbm_source_is(item, "value_definition")) {
            uint32_t parts = ts_node_named_child_count(item);
            for (uint32_t p = 0; p < parts; p++)
                if (cbm_source_is(ts_node_named_child(item, p), "let_binding"))
                    ocaml_collect_binding(walk, owner, ts_node_named_child(item, p));
        } else if (cbm_source_is(item, "external")) {
            const char *name = cbm_source_text(walk, ocaml_child(item, "value_name"));
            if (name) {
                uint32_t line = ts_node_start_point(item).row + 1;
                (void)cbm_source_declare(walk, owner, name, cbm_source_def_qn(walk, name, line), 0,
                                         line);
            }
        } else if (cbm_source_is(item, "module_definition")) {
            TSNode binding = ocaml_child(item, "module_binding");
            const char *name = cbm_source_text(walk, ocaml_child(binding, "module_name"));
            TSNode body = cbm_source_field(binding, "body");
            if (!name)
                continue;
            const char *key = cbm_arena_sprintf(walk->arena, "%s%s", OCAML_MODULE_PREFIX, name);
            cbm_source_decl_t *decl = cbm_source_declare(
                walk, owner, key, NULL,
                CBM_SOURCE_DECL_F_TYPE | (cbm_source_token(item, "rec") ? OCAML_DECL_RECURSIVE : 0),
                ts_node_start_point(item).row + 1);
            if (cbm_source_is(body, "structure")) {
                const char *nested = cbm_arena_sprintf(walk->arena, "%s#%s", owner, name);
                if (decl)
                    decl->type = cbm_source_table_intern(walk, nested);
                (void)cbm_source_declare(walk, nested, "", NULL, CBM_SOURCE_DECL_F_TYPE, 0);
                ocaml_collect_parameters(walk, nested, binding, opens, *open_count,
                                         ts_node_start_point(item).row + 1, depth);
                /* What is open here stays open inside the struct. */
                const char *inner_opens[OCAML_MAX_OPENS];
                size_t inner_count = *open_count;
                memcpy(inner_opens, opens, inner_count * sizeof(*inner_opens));
                ocaml_collect_structure(walk, nested, body, inner_opens, &inner_count, depth + 1);
            } else if (decl && !ts_node_is_null(ocaml_module_read(body))) {
                /* `module C = Config` or `module C = Make (T)`: stored as the spelling, read
                 * where it is written: under the opens of this struct, and before C exists. */
                decl->flags |= CBM_SOURCE_DECL_F_VALUE;
                decl->type =
                    cbm_source_table_intern(walk, cbm_source_text(walk, ocaml_module_read(body)));
                decl->bases = ocaml_opens_copy(walk, opens, *open_count);
            }
        } else if (cbm_source_is(item, "module_type_definition")) {
            ocaml_collect_module_type(walk, owner, item, opens, *open_count, depth);
        } else if (cbm_source_is(item, "include_module")) {
            ocaml_collect_include(walk, owner, item, opens, open_count, depth);
        } else if (cbm_source_is(item, "open_module")) {
            TSNode opened = cbm_source_field(item, "module");
            if (cbm_source_is(opened, "structure")) {
                /* `open struct`: what it has is in scope below, as an include's is. */
                ocaml_collect_inlined(walk, owner, opened, opens, open_count, depth);
            } else {
                ocaml_push_open(walk, opens, open_count, ts_node_start_point(item).row + 1,
                                cbm_source_text(walk, ocaml_module_read(opened)), false);
            }
        }
    }
    ts_tree_cursor_delete(&cursor);
}

/* Where this file stands among the modules of its build. */
static void ocaml_collect_place(cbm_source_walk_t *walk, const char *unit) {
    const char *leaf = strrchr(unit, '/');
    leaf = leaf ? leaf + 1 : unit;
    char lowered[256];
    if (strlen(leaf) >= sizeof(lowered))
        return;
    strcpy(lowered, leaf);
    lowered[0] = (char)tolower((unsigned char)lowered[0]);
    uint32_t line = 1;
    const char *directory = ocaml_directory_of(walk, unit);
    for (const char *up = directory; up; up = ocaml_parent_directory(walk, up)) {
        const char *subdirs = ocaml_manifest(walk, OCAML_SUBDIRS_KEY, up);
        if (subdirs && !strcmp(subdirs, "unqualified")) {
            cbm_source_decl_t *decl = cbm_source_declare(
                walk, cbm_arena_sprintf(walk->arena, "%s%s", OCAML_SUBTREE_PREFIX, up), lowered,
                NULL, CBM_SOURCE_DECL_F_VALUE, line);
            if (decl)
                decl->type = cbm_source_table_intern(walk, unit);
        }
        if (subdirs && !strcmp(subdirs, "qualified")) {
            /* Each directory on the way down from here is a module of its parent. */
            for (const char *group = directory; group && strcmp(group, up);
                 group = ocaml_parent_directory(walk, group))
                (void)cbm_source_declare(
                    walk, cbm_arena_sprintf(walk->arena, "%s%s", OCAML_DIRECTORY_PREFIX, group), "",
                    NULL, CBM_SOURCE_DECL_F_TYPE, line);
        }
        if (ocaml_manifest(walk, OCAML_DIR_KEY, up))
            break;
    }
}

/* foo.mli is the signature of unit foo, kept under its own name. The grammar here reads
 * implementations, where `val` is an error, so the file is read inside `sig ... end`, which
 * is what it is. Rows stay right because the opening adds none. One that still does not
 * parse says nothing. */
static void ocaml_collect_interface(cbm_source_walk_t *walk, const char *unit) {
    static const char opening[] = "module type I = sig ", closing[] = "\nend\n";
    const char *interface = cbm_arena_sprintf(walk->arena, "%s.mli", unit);
    if (!interface)
        return;
    (void)cbm_source_declare(walk, interface, "", NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    size_t length = strlen(opening) + walk->source_len + strlen(closing);
    char *text = cbm_arena_alloc(walk->arena, length + 1);
    TSParser *parser = ts_parser_new();
    TSTree *tree = NULL;
    if (text && parser && ts_parser_set_language(parser, ts_node_language(walk->root))) {
        snprintf(text, length + 1, "%s%.*s%s", opening, (int)walk->source_len, walk->source,
                 closing);
        tree = ts_parser_parse_string(parser, NULL, text, (uint32_t)length);
    }
    TSNode root = tree ? ts_tree_root_node(tree) : (TSNode){0};
    TSNode signature = ts_node_is_null(root) || ts_node_has_error(root)
                           ? (TSNode){0}
                           : ocaml_child(ocaml_child(root, "module_type_definition"), "signature");
    if (cbm_source_is(signature, "signature")) {
        const char *source = walk->source;
        size_t source_len = walk->source_len;
        walk->source = text;
        walk->source_len = length;
        ocaml_collect_signature(walk, interface, signature, NULL, 0, 0);
        walk->source = source;
        walk->source_len = source_len;
    } else {
        (void)cbm_source_declare(walk, interface, OCAML_OPEN_ENDED, NULL, CBM_SOURCE_DECL_F_OPEN,
                                 1);
    }
    if (tree)
        ts_tree_delete(tree);
    if (parser)
        ts_parser_delete(parser);
}

static void ocaml_collect(cbm_source_walk_t *walk) {
    const char *unit = ocaml_unit_key(walk);
    (void)cbm_source_declare(walk, unit, "", NULL, CBM_SOURCE_DECL_F_TYPE, 1);
    /* A unit's members and its place are the .ml's. */
    size_t length = strlen(walk->file);
    if (length > 4 && !strcmp(walk->file + length - 4, ".mli")) {
        ocaml_collect_interface(walk, unit);
        return;
    }
    ocaml_collect_place(walk, unit);
    const char *opens[OCAML_MAX_OPENS];
    size_t open_count = 0;
    ocaml_collect_structure(walk, unit, walk->root, opens, &open_count, 0);
}

/* An interface is what the implementation grammar cannot read, and has its own way in. */
static void ocaml_collect_unusable(cbm_source_walk_t *walk) {
    size_t length = strlen(walk->file);
    if (length > 4 && !strcmp(walk->file + length - 4, ".mli"))
        ocaml_collect(walk);
    else
        walk->failure = "ocaml_unreadable";
}

/* ── Phase two ─────────────────────────────────────────────────────── */

static bool ocaml_is_local(const ocaml_ctx_t *ctx, const char *name) {
    for (const ocaml_name_t *local = ctx->locals; local; local = local->next)
        if (!strcmp(local->name, name))
            return true;
    return false;
}

static void ocaml_bind(ocaml_ctx_t *ctx, const char *name) {
    ocaml_name_t *local = name ? cbm_arena_alloc(ctx->walk->arena, sizeof(*local)) : NULL;
    if (!local)
        return;
    *local = (ocaml_name_t){name, ctx->locals};
    ctx->locals = local;
}

/* Every name a pattern introduces. */
static void ocaml_bind_pattern(ocaml_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    if (cbm_source_is(node, "value_pattern") || cbm_source_is(node, "value_name"))
        ocaml_bind(ctx, cbm_source_text(ctx->walk, node));
    /* `{ a; M.b }` binds a and b; `{ a = c }` binds only c. */
    if (cbm_source_is(node, "field_pattern") && ts_node_named_child_count(node) == 1) {
        TSNode path = ts_node_named_child(node, 0);
        ocaml_bind(ctx, cbm_source_text(ctx->walk, ocaml_child(path, "field_name")));
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                ocaml_bind_pattern(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

static const char *ocaml_decl_path(ocaml_ctx_t *ctx, const cbm_source_decl_t *decl, unsigned depth);
static bool ocaml_unit_closed(ocaml_ctx_t *ctx, const char *unit);

/* The latest binding of a module name in one structure that is in scope before a line.
 * UINT32_MAX reads a finished structure, where every binding has happened. */
static const cbm_source_decl_t *ocaml_module_binding(const ocaml_ctx_t *ctx, const char *scope,
                                                     const char *module, uint32_t before_line) {
    char key[320];
    int length = snprintf(key, sizeof(key), "%s%s", OCAML_MODULE_PREFIX, module);
    if (length < 0 || (size_t)length >= sizeof(key))
        return NULL;
    const cbm_source_decl_t *latest = NULL;
    for (const cbm_source_decl_t *decl = cbm_source_lookup(ctx->walk, scope, key); decl;
         decl = decl->next)
        if (decl->line < before_line && (!latest || decl->line >= latest->line))
            latest = decl;
    return latest;
}

/* `within#head` holds `unit`: a site in unit stands in the body of that very binding, where
 * the name, unless it is `rec`, still means what it meant before the binding. */
static bool ocaml_in_own_body(const cbm_source_decl_t *binding, const char *unit,
                              const char *within, const char *head) {
    size_t scope = strlen(within), name = strlen(head);
    return binding && !(binding->flags & OCAML_DECL_RECURSIVE) && !strncmp(unit, within, scope) &&
           unit[scope] == '#' && !strncmp(unit + scope + 1, head, name) &&
           (unit[scope + 1 + name] == '\0' || unit[scope + 1 + name] == '#');
}

/* What a module binding denotes. A binding with nothing readable behind it is NULL. */
static const char *ocaml_binding_target(ocaml_ctx_t *ctx, const cbm_source_decl_t *decl,
                                        unsigned depth) {
    if (!decl->type)
        return NULL;
    return decl->flags & CBM_SOURCE_DECL_F_VALUE ? ocaml_decl_path(ctx, decl, depth + 1)
                                                 : decl->type;
}

/* A module inside a module: its own binding, or one of what it includes. */
static const char *ocaml_member_module_uncached(ocaml_ctx_t *ctx, const char *container,
                                                const char *module, unsigned depth);

static const char *ocaml_member_module(ocaml_ctx_t *ctx, const char *container, const char *module,
                                       unsigned depth) {
    if (depth > 32)
        return NULL;
    char key[640];
    int length = snprintf(key, sizeof(key), "%s\x1f%s", container, module);
    bool keyed = length > 0 && (size_t)length < sizeof(key) && ctx->member_modules;
    const char *known = keyed ? cbm_ht_get(ctx->member_modules, key) : NULL;
    if (known)
        return known[0] ? known : NULL;
    /* Asked again while working it out, the answer is no. */
    const char *stored = keyed ? cbm_arena_strdup(ctx->walk->arena, key) : NULL;
    if (stored)
        cbm_ht_set(ctx->member_modules, stored, (void *)"");
    const char *found = ocaml_member_module_uncached(ctx, container, module, depth);
    if (stored)
        cbm_ht_set(ctx->member_modules, stored, (void *)(found ? found : ""));
    return found;
}

/* A unit's interface, when it has one. */
static const char *ocaml_interface_of(const ocaml_ctx_t *ctx, const char *unit) {
    const char *key = cbm_arena_sprintf(ctx->walk->arena, "%s.mli", unit);
    return key && cbm_source_lookup(ctx->walk, key, "") ? key : NULL;
}

static const char *ocaml_member_module_uncached(ocaml_ctx_t *ctx, const char *container,
                                                const char *module, unsigned depth) {
    if (ocaml_is_stdlib(container)) {
        const char *parent = container + strlen(OCAML_STDLIB_PREFIX);
        const char *path =
            *parent ? cbm_arena_sprintf(ctx->walk->arena, "%s.%s", parent, module) : module;
        return path ? ocaml_stdlib_module(ctx, path) : NULL;
    }
    if (ocaml_is_directory_module(container)) {
        const char *directory = container + strlen(OCAML_DIRECTORY_PREFIX);
        const char *lowered = ocaml_lowered(ctx->walk, module);
        const char *unit = lowered ? ocaml_directory_member(ctx, directory, lowered) : NULL;
        if (unit)
            return unit;
        const char *group = lowered ? cbm_arena_sprintf(ctx->walk->arena, "%s%s/%s",
                                                        OCAML_DIRECTORY_PREFIX, directory, lowered)
                                    : NULL;
        return group && cbm_source_lookup(ctx->walk, group, "") ? group : NULL;
    }
    /* What an interface lists can have a module type no one reads: it is still listed. */
    const char *interface = ocaml_interface_of(ctx, container);
    if (interface && !ocaml_module_binding(ctx, interface, module, UINT32_MAX) &&
        !ocaml_member_module(ctx, interface, module, depth + 1) &&
        ocaml_unit_closed(ctx, interface))
        return NULL;
    const cbm_source_decl_t *own = ocaml_module_binding(ctx, container, module, UINT32_MAX);
    const cbm_source_decl_t *include = cbm_source_lookup(ctx->walk, container, OCAML_INCLUDE);
    /* The latest include after the binding replaces it, if it has the module at all. */
    const char *found = NULL;
    uint32_t found_line = 0;
    for (; include; include = include->next) {
        if (own && include->line < own->line)
            continue;
        const char *included = ocaml_decl_path(ctx, include, depth + 1);
        const char *inner = included ? ocaml_member_module(ctx, included, module, depth + 1) : NULL;
        if (inner && include->line >= found_line) {
            found = inner;
            found_line = include->line;
        }
    }
    return found ? found : own ? ocaml_binding_target(ctx, own, depth + 1) : NULL;
}

/* The opens above a path, read in order, each under the ones before it. */
typedef struct {
    const char *unit; /* NULL: a module no one can read */
    uint32_t line;
} ocaml_read_open_t;

/* `A.B.C` read from a structure: the latest of its opens and enclosing bindings, then the
 * unit's place, then libraries. An open that could hold A and cannot be read leaves it
 * unknown. */
static const char *ocaml_resolve_path(ocaml_ctx_t *ctx, const char *scope, const char *path,
                                      uint32_t before_line, const ocaml_read_open_t *opens,
                                      size_t open_count, unsigned depth) {
    if (depth > 32 || !path || !*path)
        return NULL;
    const char *dot = strchr(path, '.');
    const char *head = dot ? cbm_arena_strndup(ctx->walk->arena, path, (size_t)(dot - path)) : path;
    const cbm_source_decl_t *binding = NULL;
    for (const char *within = scope; !binding && within;) {
        binding = ocaml_module_binding(ctx, within, head, before_line);
        if (ocaml_in_own_body(binding, scope, within, head))
            binding = NULL;
        const char *hash = strrchr(within, '#');
        within = hash ? cbm_arena_strndup(ctx->walk->arena, within, (size_t)(hash - within)) : NULL;
    }
    const char *container = NULL;
    for (size_t i = open_count; !container && i > 0; i--) {
        if (binding && opens[i - 1].line < binding->line)
            break;
        if (!opens[i - 1].unit)
            return NULL;
        container = ocaml_member_module(ctx, opens[i - 1].unit, head, depth + 1);
        if (!container && !ocaml_unit_closed(ctx, opens[i - 1].unit))
            return NULL;
    }
    if (!container && binding) {
        container = ocaml_binding_target(ctx, binding, depth + 1);
        if (!container)
            return NULL;
    }
    if (!container)
        container = ocaml_unit_by_place(ctx, scope, head);
    while (container && dot) {
        const char *rest = dot + 1;
        dot = strchr(rest, '.');
        const char *part =
            dot ? cbm_arena_strndup(ctx->walk->arena, rest, (size_t)(dot - rest)) : rest;
        container = part ? ocaml_member_module(ctx, container, part, depth + 1) : NULL;
    }
    return container;
}

/* What a declaration's path denotes. Worked out once per file; a path that needs itself
 * has no answer. */
static const char *ocaml_decl_path(ocaml_ctx_t *ctx, const cbm_source_decl_t *decl,
                                   unsigned depth) {
    char key[32];
    snprintf(key, sizeof(key), "%p", (const void *)decl);
    const char *known = cbm_ht_get(ctx->decl_paths, key);
    if (known)
        return known[0] ? known : NULL;
    const char *stored = cbm_arena_strdup(ctx->walk->arena, key);
    if (stored)
        cbm_ht_set(ctx->decl_paths, stored, (void *)"");
    const char *scope = decl->owner;
    uint32_t line = decl->line;
    if (decl->flags & OCAML_DECL_PARAMETER) {
        const char *hash = strrchr(scope, '#');
        scope = hash ? cbm_arena_strndup(ctx->walk->arena, scope, (size_t)(hash - scope)) : scope;
        line = (uint32_t)decl->arity_min;
    }
    size_t count = 0;
    while (decl->bases && decl->bases[count])
        count++;
    ocaml_read_open_t *opens =
        count ? cbm_arena_alloc(ctx->walk->arena, count * sizeof(*opens)) : NULL;
    size_t kept = 0;
    for (size_t i = 0; opens && i < count; i++) {
        char *rest = NULL;
        unsigned long open_line = strtoul(decl->bases[i], &rest, 10);
        opens[i].line = (uint32_t)open_line;
        opens[i].unit = ocaml_resolve_path(ctx, scope, rest && *rest ? rest + 1 : NULL,
                                           (uint32_t)open_line, opens, i, depth + 1);
        kept = i + 1;
    }
    const char *found = ocaml_resolve_path(ctx, scope, decl->type, line, opens, kept, depth);
    if (stored)
        cbm_ht_set(ctx->decl_paths, stored, (void *)(found ? found : ""));
    return found;
}

/* Everything a unit brings in is known. Answers are kept for this file: a closure is
 * shared by every open of it, and each unit is read once per file. */
static bool ocaml_unit_closed_walk(ocaml_ctx_t *ctx, const char *unit, cbm_source_chain_t *chain) {
    if (ocaml_is_directory_module(unit) || ocaml_is_stdlib(unit))
        return true;
    if (!cbm_source_chain_first_visit(chain, unit))
        return !chain->overflow;
    /* An interface can only hide, so either side being closed closes the unit. */
    const char *interface = ocaml_interface_of(ctx, unit);
    if (interface && ocaml_unit_closed(ctx, interface))
        return true;
    if (cbm_source_lookup(ctx->walk, unit, OCAML_OPEN_ENDED))
        return false;
    for (const cbm_source_decl_t *include = cbm_source_lookup(ctx->walk, unit, OCAML_INCLUDE);
         include; include = include->next) {
        const char *included = ocaml_decl_path(ctx, include, 0);
        if (!included || !ocaml_unit_closed_walk(ctx, included, chain))
            return false;
    }
    return true;
}

static bool ocaml_unit_closed(ocaml_ctx_t *ctx, const char *unit) {
    if (!unit)
        return false;
    const char *memo = ctx->closed_units ? cbm_ht_get(ctx->closed_units, unit) : NULL;
    if (memo)
        return memo[0] == 'y';
    /* Asked again while working it out, the answer is no. */
    const char *key = cbm_arena_strdup(ctx->walk->arena, unit);
    if (ctx->closed_units && key)
        cbm_ht_set(ctx->closed_units, key, "n");
    cbm_source_chain_t chain = {0};
    bool closed = ocaml_unit_closed_walk(ctx, unit, &chain);
    if (ctx->closed_units && key)
        cbm_ht_set(ctx->closed_units, key, closed ? "y" : "n");
    return closed;
}

/* A value of a closed or open-ended unit: the latest binding of it the unit makes. */
static ocaml_lookup_t ocaml_unit_value(ocaml_ctx_t *ctx, const char *unit, const char *name,
                                       cbm_source_chain_t *chain, const cbm_source_decl_t **out) {
    if (ocaml_is_stdlib(unit))
        return OCAML_UNKNOWN;
    if (ocaml_is_directory_module(unit))
        return OCAML_ABSENT;
    if (!cbm_source_chain_first_visit(chain, unit))
        return chain->overflow ? OCAML_UNKNOWN : OCAML_ABSENT;
    const char *interface = ocaml_interface_of(ctx, unit);
    if (interface) {
        const cbm_source_decl_t *listed = NULL;
        if (ocaml_unit_value(ctx, interface, name, chain, &listed) == OCAML_ABSENT)
            return OCAML_ABSENT;
    }
    const cbm_source_decl_t *own = cbm_source_lookup(ctx->walk, unit, name);
    uint32_t own_line = 0;
    for (const cbm_source_decl_t *decl = own; decl; decl = decl->next)
        if (decl->line >= own_line)
            own_line = decl->line;
    const cbm_source_decl_t *best = own;
    uint32_t best_line = own_line, unknown_line = 0;
    bool unknown = false;
    for (const cbm_source_decl_t *include = cbm_source_lookup(ctx->walk, unit, OCAML_INCLUDE);
         include; include = include->next) {
        if (include->line < best_line)
            continue;
        const char *included = ocaml_decl_path(ctx, include, 0);
        const cbm_source_decl_t *inner = NULL;
        ocaml_lookup_t found =
            included ? ocaml_unit_value(ctx, included, name, chain, &inner) : OCAML_UNKNOWN;
        if (found == OCAML_UNKNOWN) {
            unknown = true;
            if (include->line > unknown_line)
                unknown_line = include->line;
        } else if (found == OCAML_FOUND) {
            best = inner;
            best_line = include->line;
        }
    }
    /* `include (val m)` can replace what stands above it. */
    for (const cbm_source_decl_t *ended = cbm_source_lookup(ctx->walk, unit, OCAML_OPEN_ENDED);
         ended; ended = ended->next) {
        unknown = true;
        if (ended->line > unknown_line)
            unknown_line = ended->line;
    }
    /* What is unknown matters only below the latest binding known. */
    if (unknown && (!best || unknown_line >= best_line))
        return OCAML_UNKNOWN;
    if (!best)
        return OCAML_ABSENT;
    *out = best;
    return OCAML_FOUND;
}

typedef struct {
    ocaml_lookup_t found;
    const cbm_source_decl_t *decl;
} ocaml_value_memo_t;

static ocaml_lookup_t ocaml_value_in(ocaml_ctx_t *ctx, const char *unit, const char *name,
                                     const cbm_source_decl_t **out) {
    char key[640];
    int length = snprintf(key, sizeof(key), "%s\x1f%s", unit, name);
    bool keyed = length > 0 && (size_t)length < sizeof(key) && ctx->member_values;
    const ocaml_value_memo_t *known = keyed ? cbm_ht_get(ctx->member_values, key) : NULL;
    if (known) {
        *out = known->decl;
        return known->found;
    }
    cbm_source_chain_t chain = {0};
    const cbm_source_decl_t *decl = NULL;
    ocaml_lookup_t found = ocaml_unit_value(ctx, unit, name, &chain, &decl);
    ocaml_value_memo_t *memo = keyed ? cbm_arena_alloc(ctx->walk->arena, sizeof(*memo)) : NULL;
    if (memo) {
        *memo = (ocaml_value_memo_t){found, decl};
        cbm_ht_set(ctx->member_values, cbm_arena_strdup(ctx->walk->arena, key), memo);
    }
    *out = decl;
    return found;
}

/* A module name at a site: an open since any binding of it can bring in another. */
typedef enum { OCAML_MODULE_FOUND, OCAML_MODULE_ABSENT, OCAML_MODULE_UNCERTAIN } ocaml_module_t;

static ocaml_module_t ocaml_module_at_site(ocaml_ctx_t *ctx, const char *path, uint32_t at,
                                           const char **out) {
    const char *dot = strchr(path, '.');
    const char *head = dot ? cbm_arena_strndup(ctx->walk->arena, path, (size_t)(dot - path)) : path;
    if (!head)
        return OCAML_MODULE_ABSENT;
    const cbm_source_decl_t *binding = NULL;
    for (const char *within = ctx->unit; !binding && within;) {
        binding = ocaml_module_binding(ctx, within, head, ctx->site_line);
        if (ocaml_in_own_body(binding, ctx->unit, within, head))
            binding = NULL;
        const char *hash = strrchr(within, '#');
        within = hash ? cbm_arena_strndup(ctx->walk->arena, within, (size_t)(hash - within)) : NULL;
    }
    const char *container = NULL;
    for (const ocaml_open_t *open = ctx->opens; !container && open; open = open->next) {
        if (open->at >= at)
            continue;
        if (binding && open->line < binding->line)
            break;
        if (!ocaml_unit_closed(ctx, open->unit))
            return OCAML_MODULE_UNCERTAIN;
        container = ocaml_member_module(ctx, open->unit, head, 0);
    }
    if (!container && binding)
        container = ocaml_binding_target(ctx, binding, 0);
    else if (!container)
        container = ocaml_unit_by_place(ctx, ctx->unit, head);
    while (container && dot) {
        const char *rest = dot + 1;
        dot = strchr(rest, '.');
        const char *part =
            dot ? cbm_arena_strndup(ctx->walk->arena, rest, (size_t)(dot - rest)) : rest;
        container = part ? ocaml_member_module(ctx, container, part, 0) : NULL;
    }
    *out = container;
    return container ? OCAML_MODULE_FOUND : OCAML_MODULE_ABSENT;
}

/* A literal struct has ended: its opens end with it, and what it includes stays. */
static ocaml_open_t *ocaml_keep_includes(ocaml_open_t *open, ocaml_open_t *saved) {
    if (open == saved)
        return saved;
    ocaml_open_t *below = ocaml_keep_includes(open->next, saved);
    if (!open->include)
        return below;
    open->next = below;
    return open;
}

static void ocaml_note_open(ocaml_ctx_t *ctx, TSNode item, TSNode path) {
    const char *text = cbm_source_text(ctx->walk, path);
    ocaml_open_t *open = cbm_arena_alloc(ctx->walk->arena, sizeof(*open));
    if (!open)
        return;
    const char *unit = NULL;
    ctx->site_line = ts_node_start_point(item).row + 1;
    if (text &&
        ocaml_module_at_site(ctx, text, ts_node_start_byte(item), &unit) != OCAML_MODULE_FOUND)
        unit = NULL;
    *open = (ocaml_open_t){.unit = unit,
                           .at = ts_node_start_byte(item),
                           .line = ts_node_start_point(item).row + 1,
                           .include = cbm_source_is(item, "include_module"),
                           .next = ctx->opens};
    ctx->opens = open;
}

static void ocaml_emit(ocaml_ctx_t *ctx, TSNode site, const char *target, cbm_resolve_rule_t rule,
                       const char *domain, const char *reason) {
    cbm_source_emit_node(ctx->walk, site,
                         ctx->reference ? CBM_SOURCE_SITE_REFERENCE : CBM_SOURCE_SITE_CALL, target,
                         rule, domain, reason);
}

static void ocaml_visit_application(ocaml_ctx_t *ctx, TSNode site, TSNode function, uint32_t at) {
    ctx->site_line = ts_node_start_point(site).row + 1;
    if (!cbm_source_is(function, "value_path")) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_computed_function");
        return;
    }
    TSNode path = ocaml_child(function, "module_path");
    TSNode leaf = ocaml_child(function, "value_name");
    const char *name = cbm_source_text(ctx->walk, leaf);
    if (!name) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_operator_value");
        return;
    }
    const char *reason = NULL;
    if (ts_node_is_null(path)) {
        if (ocaml_is_local(ctx, name)) {
            ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_locally_bound_name");
            return;
        }
        /* A nested structure sees the names of those around it. */
        const cbm_source_decl_t *decl = NULL;
        const char *scope = ctx->unit;
        while (scope && !(decl = cbm_source_lookup(ctx->walk, scope, name))) {
            const char *hash = strrchr(scope, '#');
            scope =
                hash ? cbm_arena_strndup(ctx->walk->arena, scope, (size_t)(hash - scope)) : NULL;
        }
        if (decl) {
            uint32_t line = ts_node_start_point(site).row + 1;
            const char *qn = decl->line <= line ? cbm_source_unique_qn(decl, &reason) : NULL;
            if (!qn) {
                ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                           reason ? reason : "ocaml_let_after_its_use");
                return;
            }
            /* Any open since the declaration could have replaced the name. */
            for (const ocaml_open_t *open = ctx->opens; open; open = open->next) {
                const cbm_source_decl_t *replacing = NULL;
                if (open->at < at && open->line > decl->line &&
                    (!ocaml_unit_closed(ctx, open->unit) ||
                     ocaml_value_in(ctx, open->unit, name, &replacing) != OCAML_ABSENT)) {
                    ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                               "ocaml_open_may_replace_name");
                    return;
                }
            }
            ocaml_emit(ctx, site, qn, CBM_RESOLVE_RULE_SCOPE_BINDING, scope,
                       "ocaml_sequential_let");
            return;
        }
        /* The latest open that declares the name, with nothing unknown after it. */
        for (const ocaml_open_t *open = ctx->opens; open; open = open->next) {
            if (open->at >= at)
                continue;
            if (!ocaml_unit_closed(ctx, open->unit)) {
                ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                           "ocaml_open_of_unknown_module");
                return;
            }
            const cbm_source_decl_t *opened = NULL;
            ocaml_lookup_t from_open = ocaml_value_in(ctx, open->unit, name, &opened);
            if (from_open == OCAML_UNKNOWN) {
                ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                           "ocaml_open_of_unknown_module");
                return;
            }
            if (from_open == OCAML_FOUND) {
                const char *qn = cbm_source_unique_qn(opened, &reason);
                ocaml_emit(ctx, site, qn, CBM_RESOLVE_RULE_IMPORT_BINDING, open->unit,
                           qn ? "ocaml_name_from_open" : reason);
                return;
            }
        }
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_name_not_in_scope");
        return;
    }
    const char *module = cbm_source_text(ctx->walk, path);
    const char *unit = NULL;
    /* An open-ended open could define a module of this name. */
    ocaml_module_t where =
        module ? ocaml_module_at_site(ctx, module, at, &unit) : OCAML_MODULE_ABSENT;
    if (where == OCAML_MODULE_UNCERTAIN) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_open_may_define_module");
        return;
    }
    if (where == OCAML_MODULE_ABSENT) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL, "ocaml_module_not_in_project");
        return;
    }
    const cbm_source_decl_t *decl = NULL;
    ocaml_lookup_t found = ocaml_value_in(ctx, unit, name, &decl);
    if (found == OCAML_UNKNOWN) {
        ocaml_emit(ctx, site, NULL, CBM_RESOLVE_RULE_NONE, NULL,
                   ocaml_is_stdlib(unit) ? "ocaml_standard_library_value"
                                         : "ocaml_module_is_open_ended");
        return;
    }
    const char *qn = found == OCAML_FOUND ? cbm_source_unique_qn(decl, &reason) : NULL;
    if (!qn && !reason)
        reason = "source_declaration_absent";
    ocaml_emit(ctx, site, qn, CBM_RESOLVE_RULE_IMPORT_BINDING, unit,
               qn ? "ocaml_compilation_unit_member" : reason);
}

static void ocaml_visit(ocaml_ctx_t *ctx, TSNode node, unsigned depth);

static void ocaml_visit_children(ocaml_ctx_t *ctx, TSNode node, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                ocaml_visit(ctx, child, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* Parameters and the body of one binding, in a scope of their own. */
static void ocaml_visit_binding(ocaml_ctx_t *ctx, TSNode binding, unsigned depth) {
    ocaml_name_t *saved = ctx->locals;
    uint32_t count = ts_node_named_child_count(binding);
    TSNode pattern = cbm_source_field(binding, "pattern");
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(binding, i);
        if (cbm_source_is(child, "parameter"))
            ocaml_bind_pattern(ctx, child, depth + 1);
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(binding, i);
        if (!ts_node_eq(child, pattern) && !cbm_source_is(child, "parameter"))
            ocaml_visit(ctx, child, depth + 1);
    }
    ctx->locals = saved;
}

static void ocaml_visit_value_definition(ocaml_ctx_t *ctx, TSNode node, bool local,
                                         unsigned depth) {
    bool recursive = cbm_source_token(node, "rec");
    uint32_t count = ts_node_named_child_count(node);
    if (local && recursive) {
        for (uint32_t i = 0; i < count; i++) {
            TSNode binding = ts_node_named_child(node, i);
            if (cbm_source_is(binding, "let_binding"))
                ocaml_bind_pattern(ctx, cbm_source_field(binding, "pattern"), depth + 1);
        }
    }
    for (uint32_t i = 0; i < count; i++) {
        TSNode binding = ts_node_named_child(node, i);
        if (cbm_source_is(binding, "let_binding"))
            ocaml_visit_binding(ctx, binding, depth + 1);
    }
    if (local && !recursive) {
        for (uint32_t i = 0; i < count; i++) {
            TSNode binding = ts_node_named_child(node, i);
            if (cbm_source_is(binding, "let_binding"))
                ocaml_bind_pattern(ctx, cbm_source_field(binding, "pattern"), depth + 1);
        }
    }
}

static void ocaml_visit(ocaml_ctx_t *ctx, TSNode node, unsigned depth) {
    if (!cbm_source_step(ctx->walk, depth))
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "value_path")) {
        /* A name read as a value follows the same lookup as an application's head. */
        ctx->reference = true;
        ocaml_visit_application(ctx, node, node, ts_node_start_byte(node));
        ctx->reference = false;
        return;
    }
    if (!strcmp(kind, "open_module")) {
        TSNode opened = cbm_source_field(node, "module");
        if (cbm_source_is(opened, "structure")) {
            ocaml_open_t *saved_opens = ctx->opens;
            ocaml_visit_children(ctx, opened, depth);
            ctx->opens = ocaml_keep_includes(ctx->opens, saved_opens);
        } else {
            ocaml_note_open(ctx, node, ocaml_module_read(opened));
        }
        return;
    }
    /* What an include adds is in scope after it, as an open's is, and may replace a name. A
     * literal struct is walked. */
    if (!strcmp(kind, "include_module")) {
        TSNode body = ts_node_named_child_count(node) ? ts_node_named_child(node, 0) : (TSNode){0};
        if (cbm_source_is(body, "typed_module_expression") && ts_node_named_child_count(body))
            body = ts_node_named_child(body, 0);
        ocaml_open_t *saved_opens = ctx->opens;
        bool literal = cbm_source_is(body, "structure");
        if (!literal)
            ocaml_note_open(ctx, node, ocaml_module_read(body));
        ocaml_visit_children(ctx, node, depth);
        if (literal)
            ctx->opens = ocaml_keep_includes(ctx->opens, saved_opens);
        return;
    }
    /* `F (struct ... end)`: what the argument binds is its own, and ends with it. */
    if (!strcmp(kind, "module_application")) {
        ocaml_name_t *saved = ctx->locals;
        ocaml_open_t *saved_opens = ctx->opens;
        bool saved_argument = ctx->in_functor_argument;
        ctx->in_functor_argument = true;
        ocaml_visit_children(ctx, node, depth);
        ctx->in_functor_argument = saved_argument;
        ctx->locals = saved;
        ctx->opens = saved_opens;
        return;
    }
    /* `M.(e)` and `let open M in e`: M is open for e and no further. */
    if (!strcmp(kind, "local_open_expression") || !strcmp(kind, "let_open_expression")) {
        ocaml_open_t *saved_opens = ctx->opens;
        if (!strcmp(kind, "local_open_expression"))
            ocaml_note_open(ctx, node, ocaml_child(node, "module_path"));
        ocaml_visit_children(ctx, node, depth);
        ctx->opens = saved_opens;
        return;
    }
    if (!strcmp(kind, "value_definition")) {
        ocaml_visit_value_definition(ctx, node, ctx->in_functor_argument, depth);
        return;
    }
    if (!strcmp(kind, "let_expression")) {
        ocaml_name_t *saved = ctx->locals;
        ocaml_open_t *saved_opens = ctx->opens;
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (cbm_source_is(child, "value_definition"))
                ocaml_visit_value_definition(ctx, child, true, depth);
            else
                ocaml_visit(ctx, child, depth + 1);
        }
        ctx->locals = saved;
        ctx->opens = saved_opens;
        return;
    }
    if (!strcmp(kind, "fun_expression") || !strcmp(kind, "match_case") ||
        !strcmp(kind, "for_expression") || !strcmp(kind, "function_expression")) {
        ocaml_name_t *saved = ctx->locals;
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (cbm_source_is(child, "parameter") || cbm_source_is(child, "value_pattern"))
                ocaml_bind_pattern(ctx, child, depth + 1);
        }
        TSNode pattern = cbm_source_field(node, "pattern");
        if (!ts_node_is_null(pattern))
            ocaml_bind_pattern(ctx, pattern, depth + 1);
        ocaml_visit_children(ctx, node, depth);
        ctx->locals = saved;
        return;
    }
    if (!strcmp(kind, "module_definition")) {
        /* A nested structure has names and opens of its own, kept under its key. */
        TSNode binding = ocaml_child(node, "module_binding");
        TSNode body = cbm_source_field(binding, "body");
        if (cbm_source_is(body, "structure")) {
            const char *name = cbm_source_text(ctx->walk, ocaml_child(binding, "module_name"));
            const char *saved = ctx->unit;
            ocaml_open_t *saved_opens = ctx->opens;
            if (name)
                ctx->unit = cbm_arena_sprintf(ctx->walk->arena, "%s#%s", saved, name);
            ocaml_visit_children(ctx, body, depth);
            ctx->unit = saved;
            ctx->opens = saved_opens;
        } else if (!ts_node_is_null(body)) {
            ocaml_visit(ctx, body, depth + 1);
        }
        return;
    }
    if (!strcmp(kind, "application_expression"))
        ocaml_visit_application(ctx, node, cbm_source_field(node, "function"),
                                ts_node_start_byte(node));
    ocaml_visit_children(ctx, node, depth);
}

static void ocaml_resolve(cbm_source_walk_t *walk) {
    ocaml_ctx_t ctx = {.walk = walk,
                       .unit = ocaml_unit_key(walk),
                       .closed_units = cbm_ht_create(16),
                       .member_modules = cbm_ht_create(64),
                       .member_values = cbm_ht_create(64),
                       .decl_paths = cbm_ht_create(16)};
    ocaml_visit_children(&ctx, walk->root, 0);
    cbm_ht_free(ctx.closed_units);
    cbm_ht_free(ctx.member_modules);
    cbm_ht_free(ctx.member_values);
    cbm_ht_free(ctx.decl_paths);
}

const cbm_source_adapter_t cbm_source_adapter_ocaml = {.language = CBM_LANG_OCAML,
                                                       .collect = ocaml_collect,
                                                       .collect_unusable = ocaml_collect_unusable,
                                                       .resolve = ocaml_resolve,
                                                       .decides_references = true,
                                                       .contains_parse_damage = true};
