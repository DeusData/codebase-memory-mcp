#include "oo_internal.h"
#include "proof_bridge.h"
#include "../resolve/policy.h"
#include "../resolve/snapshot.h"
#include "foundation/mem_core.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* A hierarchy deeper or wider than this is reported as ambiguous/incomplete,
 * never truncated into a uniqueness claim. */
enum {
    OO_MAX_HIERARCHY = 128,
    OO_MAX_CANDIDATES = 64,
    OO_MAX_DEPTH = 24,
    OO_KEY_MAX = 1024,
    OO_ARG_SLOTS = 32
};

bool cbm_oo_language(CBMLanguage language) {
    return language == CBM_LANG_JAVA || language == CBM_LANG_KOTLIN || language == CBM_LANG_CSHARP;
}

/* ── Index ──────────────────────────────────────────────────────────── */

cbm_oo_index_t *cbm_oo_index_new(CBMArena *arena) {
    cbm_oo_index_t *ix = cbm_arena_calloc(arena, sizeof(*ix));
    if (!ix)
        return NULL;
    ix->arena = arena;
    ix->types = cbm_ht_create(1024);
    ix->methods = cbm_ht_create(4096);
    ix->fields = cbm_ht_create(4096);
    ix->functions = cbm_ht_create(1024);
    ix->props = cbm_ht_create(1024);
    ix->ext_static = cbm_ht_create(256);
    ix->files = cbm_ht_create(256);
    if (!ix->types || !ix->methods || !ix->fields || !ix->functions || !ix->props ||
        !ix->ext_static || !ix->files) {
        cbm_oo_index_free(ix);
        return NULL;
    }
    return ix;
}

void cbm_oo_index_free(cbm_oo_index_t *ix) {
    if (!ix)
        return;
    cbm_ht_free(ix->types);
    cbm_ht_free(ix->methods);
    cbm_ht_free(ix->fields);
    cbm_ht_free(ix->functions);
    cbm_ht_free(ix->props);
    cbm_ht_free(ix->ext_static);
    cbm_ht_free(ix->files);
    ix->types = ix->methods = ix->fields = ix->functions = ix->props = ix->ext_static = NULL;
    ix->files = NULL;
}

static const char *oo_key(char *buffer, size_t capacity, const char *a, const char *b) {
    int length = snprintf(buffer, capacity, "%s\x1f%s", a ? a : "", b ? b : "");
    return length > 0 && (size_t)length < capacity ? buffer : NULL;
}

static const char *file_rel_or(const char *rel) {
    return rel ? rel : "";
}

/* Gradle and Maven: `src/test/...`, `src/internalClusterTest/...`, any source set named *Test. */
static bool oo_jvm_test_source(const char *rel) {
    for (const char *at = strstr(rel, "src/"); at; at = strstr(at + 1, "src/")) {
        if (at != rel && at[-1] != '/')
            continue;
        const char *set = at + 4;
        size_t length = strcspn(set, "/");
        if (set[length] != '/' || length < 4)
            continue;
        if (!strncmp(set + length - 4, "test", 4) || !strncmp(set + length - 4, "Test", 4))
            return true;
    }
    return false;
}

/* .NET: a test project, by its own directory: `<lib>/tests`, `<name>.Tests`. A helper library under
 * a tests directory (dotnet/runtime's Common/tests/TestUtilities) is read by every test project. */
static bool oo_csharp_test_project(const char *project) {
    if (!project)
        return false;
    const char *slash = strrchr(project, '/');
    const char *at = slash ? slash + 1 : project;
    size_t length = strlen(at);
    return !strcmp(at, "tests") || !strcmp(at, "test") ||
           (length > 6 && !strcmp(at + length - 6, ".Tests")) ||
           (length > 5 && !strcmp(at + length - 5, ".Test"));
}

/* Test sources are the leaves of a build: another project's code does not read them, unless it
 * compiles the very file. So a free name never reaches a function or an extension method there from
 * outside: `items.Concat(x)` in one test project does not mean another test project's helper in an
 * enclosing namespace. A type named outright still binds, since a shared source file reads it. */
static bool oo_file_outside_tests(const cbm_oo_index_t *ix, const oo_file_t *declared,
                                  const oo_file_t *reader) {
    if (!declared || !declared->test_source || !reader || !declared->project)
        return false;
    const oo_file_t *indexed = reader->project ? reader : cbm_ht_get(ix->files, reader->rel);
    return indexed && indexed->project && strcmp(indexed->project, declared->project);
}

const oo_file_t *oo_index_file(cbm_oo_index_t *ix, CBMLanguage lang, const char *rel,
                               const char *package, oo_import_t *imports, int import_count,
                               bool has_error) {
    oo_file_t *file = cbm_arena_calloc(ix->arena, sizeof(*file));
    if (!file) {
        ix->failed = true;
        return NULL;
    }
    *file =
        (oo_file_t){.rel = cbm_arena_strdup(ix->arena, rel ? rel : ""),
                    .package = cbm_arena_strdup(ix->arena, package ? package : ""),
                    .language = lang,
                    .imports = imports,
                    .import_count = import_count,
                    .has_error = has_error,
                    .reference = ix->reference_files,
                    .test_source = lang == CBM_LANG_CSHARP ? oo_csharp_test_project(ix->project)
                                                           : oo_jvm_test_source(file_rel_or(rel)),
                    .project = ix->project};
    if (ix->project)
        cbm_ht_set(ix->files, file->rel, file);
    return file;
}

void cbm_oo_index_set_project(cbm_oo_index_t *ix, const char *project_dir) {
    if (ix)
        ix->project = project_dir ? cbm_arena_strdup(ix->arena, project_dir) : NULL;
}

void cbm_oo_index_begin_reference_files(cbm_oo_index_t *ix) {
    if (ix)
        ix->reference_files = true;
}

bool oo_type_is_implemented_elsewhere(const oo_type_t *type, const oo_file_t *file) {
    return file && file->reference && type && type->file && !type->file->reference;
}

/* One more declaration of a name. The first keeps its members under the plain qualified name, and
 * the index entry for the name becomes a holder of the declarations with no members of its own.
 * Each later one is its own type, keyed by its file, so its members and nested types stay apart. */
static oo_type_t *oo_index_variant(cbm_oo_index_t *ix, oo_type_t *prior, const char *qn,
                                   const char *name, const oo_file_t *f, const char *ns,
                                   const oo_type_t *owner, const char *graph_qn, uint32_t flags) {
    oo_type_t *holder = prior;
    if (!(prior->flags & OO_F_AMBIGUOUS)) {
        holder = cbm_arena_calloc(ix->arena, sizeof(*holder));
        if (!holder) {
            ix->failed = true;
            return prior;
        }
        *holder = *prior;
        holder->flags |= OO_F_AMBIGUOUS;
        holder->graph_qn = NULL;
        holder->graph_count = 0;
        holder->variants = prior;
        holder->next_in_index = NULL;
        cbm_ht_set(ix->types, prior->qn, holder);
        ix->has_variants = true;
    }
    /* Twice in one file, as in two `#if` branches, is one graph node. */
    for (const oo_type_t *v = holder->variants; v; v = v->next_variant)
        if (v->file == f)
            return (oo_type_t *)v;
    const char *key = cbm_arena_sprintf(ix->arena, "%s\x1e%d", qn, ++ix->variant_serial);
    oo_type_t *variant = cbm_arena_calloc(ix->arena, sizeof(*variant));
    const char *short_name = cbm_arena_strdup(ix->arena, name);
    if (!variant || !key || !short_name) {
        ix->failed = true;
        return holder;
    }
    *variant = (oo_type_t){.qn = key,
                           .name = short_name,
                           .graph_qn = graph_qn ? cbm_arena_strdup(ix->arena, graph_qn) : NULL,
                           .graph_count = graph_qn ? 1 : 0,
                           .file = f,
                           .ns = ns ? cbm_arena_strdup(ix->arena, ns) : "",
                           .owner = owner,
                           .flags = flags,
                           .next_in_index = ix->all_types,
                           .next_variant = holder->variants};
    holder->variants = variant;
    ix->all_types = variant;
    cbm_ht_set(ix->types, key, variant);
    return variant;
}

/* The file named after a type (`Foo.cs` for `Foo` and `Foo<T>`). */
static bool oo_file_named_after(const char *rel, const char *name) {
    const char *slash = rel ? strrchr(rel, '/') : NULL;
    const char *base = slash ? slash + 1 : rel;
    size_t length = strcspn(name, "`");
    return base && !strncmp(base, name, length) && !strcmp(base + length, ".cs");
}

/* A partial type is one type with a node per file. A mention of the type needs one of them, and
 * every one is a declaration of it, so this picks by a pure function of the set: the file named
 * after the type, then the smallest path. Its members keep their own nodes. */
static void oo_partial_representative(cbm_oo_index_t *ix, oo_type_t *type, const oo_file_t *f,
                                      const char *graph_qn) {
    type->graph_count++;
    if (!graph_qn || !f)
        return;
    bool named = oo_file_named_after(f->rel, type->name);
    bool held = type->graph_rel && oo_file_named_after(type->graph_rel, type->name);
    if (type->graph_qn && (held != named ? held : strcmp(type->graph_rel, f->rel) <= 0))
        return;
    type->graph_qn = cbm_arena_strdup(ix->arena, graph_qn);
    type->graph_rel = f->rel;
}

oo_type_t *oo_index_type(cbm_oo_index_t *ix, const char *qn, const char *name, const oo_file_t *f,
                         const char *ns, const oo_type_t *owner, const char *graph_qn,
                         uint32_t flags) {
    if (!qn || !name)
        return NULL;
    oo_type_t *prior = cbm_ht_get(ix->types, qn);
    if (prior) {
        if (oo_type_is_implemented_elsewhere(prior, f))
            return prior;
        /* C# partial declarations are one type with several graph nodes. Any
         * other repeated qualified name is two types the index cannot tell
         * apart (expect/actual, multi-release sources): never choose one. */
        if (!(prior->flags & OO_F_AMBIGUOUS) && (prior->flags & OO_F_PARTIAL) &&
            (flags & OO_F_PARTIAL)) {
            oo_partial_representative(ix, prior, f, graph_qn);
            return prior;
        }
        return oo_index_variant(ix, prior, qn, name, f, ns, owner, graph_qn, flags);
    }
    oo_type_t *type = cbm_arena_calloc(ix->arena, sizeof(*type));
    const char *key = cbm_arena_strdup(ix->arena, qn);
    const char *short_name = cbm_arena_strdup(ix->arena, name);
    if (!type || !key || !short_name) {
        ix->failed = true;
        return NULL;
    }
    *type = (oo_type_t){.qn = key,
                        .name = short_name,
                        .graph_qn = graph_qn ? cbm_arena_strdup(ix->arena, graph_qn) : NULL,
                        .graph_count = graph_qn ? 1 : 0,
                        .graph_rel = f ? f->rel : NULL,
                        .file = f,
                        .ns = ns ? cbm_arena_strdup(ix->arena, ns) : "",
                        .owner = owner,
                        .flags = flags,
                        .next_in_index = ix->all_types};
    ix->all_types = type;
    cbm_ht_set(ix->types, key, type);
    return type;
}

void oo_type_add_super(cbm_oo_index_t *ix, oo_type_t *type, const oo_typeref_t *ref,
                       const oo_file_t *f, const char *ns) {
    if (!type || !ref)
        return;
    oo_super_t *grown =
        cbm_arena_alloc(ix->arena, (size_t)(type->super_count + 1) * sizeof(*grown));
    if (!grown) {
        ix->failed = true;
        return;
    }
    if (type->super_count)
        memcpy(grown, type->supers, (size_t)type->super_count * sizeof(*grown));
    grown[type->super_count] =
        (oo_super_t){.ref = ref, .file = f, .ns = ns ? cbm_arena_strdup(ix->arena, ns) : ""};
    type->supers = grown;
    type->super_count++;
}

static void oo_list_add(cbm_oo_index_t *ix, CBMHashTable *table, const char *a, const char *b,
                        void *entry, void **next_slot) {
    char buffer[OO_KEY_MAX];
    const char *key = oo_key(buffer, sizeof(buffer), a, b);
    if (!key) {
        ix->failed = true;
        return;
    }
    void *prior = cbm_ht_get(table, key);
    *next_slot = prior;
    const char *stored = prior ? cbm_ht_get_key(table, key) : cbm_arena_strdup(ix->arena, key);
    if (!stored) {
        ix->failed = true;
        return;
    }
    cbm_ht_set(table, stored, entry);
}

void oo_index_method(cbm_oo_index_t *ix, const oo_type_t *owner, oo_callable_t *callable) {
    oo_list_add(ix, ix->methods, owner->qn, callable->name, callable, (void **)&callable->next);
}
void oo_index_field(cbm_oo_index_t *ix, const oo_type_t *owner, oo_field_t *field) {
    oo_list_add(ix, ix->fields, owner->qn, field->name, field, (void **)&field->next);
}
void oo_index_function(cbm_oo_index_t *ix, const char *ns, oo_callable_t *callable) {
    oo_list_add(ix, ix->functions, ns, callable->name, callable, (void **)&callable->next);
}
void oo_index_prop(cbm_oo_index_t *ix, const char *ns, oo_field_t *field) {
    oo_list_add(ix, ix->props, ns, field->name, field, (void **)&field->next);
}
void oo_index_ext_static(cbm_oo_index_t *ix, const char *ns, oo_callable_t *callable) {
    oo_callable_t *copy = cbm_arena_alloc(ix->arena, sizeof(*copy));
    if (!copy) {
        ix->failed = true;
        return;
    }
    *copy = *callable;
    oo_list_add(ix, ix->ext_static, ns, copy->name, copy, (void **)&copy->next);
}

const oo_type_t *oo_lookup_type(const cbm_oo_index_t *ix, const char *qn) {
    return qn ? cbm_ht_get(ix->types, qn) : NULL;
}

const oo_type_t *oo_lookup_declared(const cbm_oo_index_t *ix, const char *qn, const char *rel) {
    const oo_type_t *type = oo_lookup_type(ix, qn);
    if (!type || !(type->flags & OO_F_AMBIGUOUS) || !rel)
        return type;
    for (const oo_type_t *v = type->variants; v; v = v->next_variant)
        if (v->file && !strcmp(v->file->rel, rel))
            return v;
    return type;
}

/* The declaration of a several-claimed name this file means, or NULL: its own file's, then the only
 * one in its project (C# CS0436, and javac's and kotlinc's own sources: a project's declaration
 * shadows one it could only read from elsewhere). Linear in that name's declarations. */
static const oo_type_t *oo_variant_for(const cbm_oo_index_t *ix, const oo_type_t *holder,
                                       const oo_file_t *file) {
    if (!holder || !holder->variants || !file)
        return NULL;
    const oo_file_t *indexed = file->project ? file : cbm_ht_get(ix->files, file->rel);
    const char *project = indexed ? indexed->project : NULL;
    const oo_type_t *in_project = NULL;
    int count = 0;
    for (const oo_type_t *v = holder->variants; v; v = v->next_variant) {
        if (!v->file)
            continue;
        if (!strcmp(v->file->rel, file->rel))
            return v;
        if (project && v->file->project && !strcmp(project, v->file->project)) {
            in_project = v;
            count++;
        }
    }
    return count == 1 ? in_project : NULL;
}

const oo_type_t *oo_lookup_visible(const cbm_oo_index_t *ix, const char *qn,
                                   const oo_file_t *file) {
    const oo_type_t *exact = oo_lookup_type(ix, qn);
    if (!qn || !ix->has_variants)
        return exact;
    if (exact && (exact->flags & OO_F_AMBIGUOUS)) {
        const oo_type_t *variant = oo_variant_for(ix, exact, file);
        return variant ? variant : exact;
    }
    /* A dotted name through a several-claimed outer type. Bounded by its dots. */
    char prefix[OO_KEY_MAX];
    size_t length = strlen(qn);
    if (length >= sizeof(prefix))
        return exact;
    memcpy(prefix, qn, length + 1);
    for (char *dot = strchr(prefix, '.'); dot; dot = strchr(dot + 1, '.')) {
        *dot = '\0';
        const oo_type_t *outer = cbm_ht_get(ix->types, prefix);
        *dot = '.';
        if (!outer || !(outer->flags & OO_F_AMBIGUOUS))
            continue;
        const oo_type_t *type = oo_variant_for(ix, outer, file);
        if (!type)
            return outer;
        const char *rest = qn + (dot - prefix) + 1;
        while (type && *rest) {
            const char *next = strchr(rest, '.');
            size_t part = next ? (size_t)(next - rest) : strlen(rest);
            char segment[256];
            if (part >= sizeof(segment))
                return NULL;
            memcpy(segment, rest, part);
            segment[part] = '\0';
            type = oo_find_nested(ix, type, segment);
            rest = next ? next + 1 : rest + part;
        }
        return type;
    }
    return exact;
}

static void *oo_list_get(const CBMHashTable *table, const char *a, const char *b) {
    char buffer[OO_KEY_MAX];
    const char *key = oo_key(buffer, sizeof(buffer), a, b);
    return key ? cbm_ht_get(table, key) : NULL;
}

const oo_callable_t *oo_lookup_methods(const cbm_oo_index_t *ix, const char *qn, const char *name) {
    return oo_list_get(ix->methods, qn, name);
}
const oo_field_t *oo_lookup_fields(const cbm_oo_index_t *ix, const char *qn, const char *name) {
    return oo_list_get(ix->fields, qn, name);
}
const oo_callable_t *oo_lookup_functions(const cbm_oo_index_t *ix, const char *ns,
                                         const char *name) {
    return oo_list_get(ix->functions, ns, name);
}
const oo_field_t *oo_lookup_props(const cbm_oo_index_t *ix, const char *ns, const char *name) {
    return oo_list_get(ix->props, ns, name);
}
const oo_callable_t *oo_lookup_ext_static(const cbm_oo_index_t *ix, const char *ns,
                                          const char *name) {
    return oo_list_get(ix->ext_static, ns, name);
}

/* ── Definitions ────────────────────────────────────────────────────── */

static char oo_def_class(const char *label) {
    if (!label)
        return 'F';
    if (!strcmp(label, "Method") || !strcmp(label, "Function") || !strcmp(label, "Constructor"))
        return 'C';
    return cbm_label_is_type_like(label) ? 'T' : 'F';
}

/* A spilled result goes back to the store as soon as its file is harvested, so a qualified
 * name the index keeps is copied into the index. A definition found here is valid only
 * during the harvest. */
bool oo_defs_init(oo_defs_t *defs, CBMArena *arena, const CBMFileResult *file) {
    cbm_arena_init(&defs->scratch);
    defs->by_line = cbm_ht_create((uint32_t)file->defs.count + 8);
    if (!defs->by_line) {
        oo_defs_free(defs);
        return false;
    }
    for (int i = 0; i < file->defs.count; i++) {
        const CBMDefinition *def = &file->defs.items[i];
        if (!def->name || !def->qualified_name)
            continue;
        const char *key = cbm_arena_sprintf(&defs->scratch, "%c%u:%s", oo_def_class(def->label),
                                            def->start_line, def->name);
        if (!key) {
            oo_defs_free(defs);
            return false;
        }
        /* First writer wins: overloads on one line share a qualified name. */
        if (cbm_ht_get(defs->by_line, key))
            continue;
        CBMDefinition *copy = cbm_arena_alloc(&defs->scratch, sizeof(*copy));
        const char *qualified_name = cbm_arena_strdup(arena, def->qualified_name);
        if (!copy || !qualified_name) {
            oo_defs_free(defs);
            return false;
        }
        *copy = *def;
        copy->qualified_name = qualified_name;
        cbm_ht_set(defs->by_line, key, copy);
    }
    return true;
}

void oo_defs_free(oo_defs_t *defs) {
    cbm_ht_free(defs->by_line);
    defs->by_line = NULL;
    cbm_arena_destroy(&defs->scratch);
}

const CBMDefinition *oo_defs_find(const oo_defs_t *defs, char kind, uint32_t line,
                                  const char *name) {
    char buffer[OO_KEY_MAX];
    int length = snprintf(buffer, sizeof(buffer), "%c%u:%s", kind, line, name ? name : "");
    return length > 0 && (size_t)length < sizeof(buffer) ? cbm_ht_get(defs->by_line, buffer) : NULL;
}

/* ── Hierarchy ──────────────────────────────────────────────────────── */

static bool oo_broken_node(TSNode node) {
    return ts_node_is_error(node) || ts_node_is_missing(node);
}

bool oo_members_broken(TSNode declaration) {
    uint32_t count = ts_node_child_count(declaration);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(declaration, i);
        if (oo_broken_node(child))
            return true;
        const char *kind = ts_node_type(child);
        if (!strstr(kind, "body") && !strstr(kind, "declaration_list"))
            continue;
        uint32_t members = ts_node_child_count(child);
        for (uint32_t m = 0; m < members; m++)
            if (oo_broken_node(ts_node_child(child, m)))
                return true;
    }
    return false;
}

const oo_type_t *oo_unalias(const cbm_oo_index_t *ix, const oo_type_t *type) {
    for (int hops = 0; type && type->alias_of && hops < 16; hops++) {
        oo_scope_t scope = oo_scope_of_type(type);
        scope.owner = type->owner;
        oo_name_t name = oo_resolve_name(ix, &scope, type->alias_of->name);
        if (name.status != OO_NAME_TYPE)
            return NULL;
        type = name.type;
    }
    return type && !type->alias_of ? type : NULL;
}

bool oo_is_subtype(const cbm_oo_index_t *ix, const oo_type_t *type, const oo_type_t *super) {
    const oo_type_t *queue[OO_MAX_HIERARCHY];
    int head = 0, tail = 0;
    if (!type || !super)
        return false;
    queue[tail++] = type;
    while (head < tail) {
        const oo_type_t *current = queue[head++];
        if (current == super)
            return true;
        for (int i = 0; i < current->super_count && tail < OO_MAX_HIERARCHY; i++) {
            const oo_type_t *next = current->supers[i].resolved;
            if (!next)
                continue;
            bool seen = false;
            for (int j = 0; j < tail && !seen; j++)
                seen = queue[j] == next;
            if (!seen)
                queue[tail++] = next;
        }
    }
    (void)ix;
    return false;
}

bool oo_hierarchy_complete(const cbm_oo_index_t *ix, const oo_type_t *type) {
    const oo_type_t *queue[OO_MAX_HIERARCHY];
    int head = 0, tail = 0;
    (void)ix;
    if (!type)
        return false;
    queue[tail++] = type;
    while (head < tail) {
        const oo_type_t *current = queue[head++];
        if (!current->supers_complete || (current->flags & OO_F_INCOMPLETE))
            return false;
        for (int i = 0; i < current->super_count; i++) {
            const oo_type_t *next = current->supers[i].resolved;
            if (!next)
                continue; /* supers_complete says it is a closed external. */
            bool seen = false;
            for (int j = 0; j < tail && !seen; j++)
                seen = queue[j] == next;
            if (!seen) {
                if (tail == OO_MAX_HIERARCHY)
                    return false;
                queue[tail++] = next;
            }
        }
    }
    return true;
}

const oo_type_t *oo_find_nested(const cbm_oo_index_t *ix, const oo_type_t *type, const char *name) {
    const oo_type_t *queue[OO_MAX_HIERARCHY];
    int head = 0, tail = 0;
    if (!type || !name)
        return NULL;
    queue[tail++] = type;
    while (head < tail) {
        const oo_type_t *current = queue[head++];
        char buffer[OO_KEY_MAX];
        int length = snprintf(buffer, sizeof(buffer), "%s.%s", current->qn, name);
        if (length > 0 && (size_t)length < sizeof(buffer)) {
            const oo_type_t *nested = cbm_ht_get(ix->types, buffer);
            if (nested)
                return nested;
        }
        for (int i = 0; i < current->super_count && tail < OO_MAX_HIERARCHY; i++) {
            const oo_type_t *next = current->supers[i].resolved;
            if (!next)
                continue;
            bool seen = false;
            for (int j = 0; j < tail && !seen; j++)
                seen = queue[j] == next;
            if (!seen)
                queue[tail++] = next;
        }
    }
    return NULL;
}

/* ── Names ──────────────────────────────────────────────────────────── */

static oo_name_t oo_name_type(const cbm_oo_index_t *ix, const oo_file_t *file,
                              const oo_type_t *type) {
    if (!type)
        return (oo_name_t){.status = OO_NAME_UNKNOWN};
    if (type->flags & OO_F_AMBIGUOUS) {
        type = oo_variant_for(ix, type, file);
        if (!type)
            return (oo_name_t){.status = OO_NAME_AMBIGUOUS};
    }
    return (oo_name_t){.status = OO_NAME_TYPE, .type = type};
}

static const oo_type_t *oo_qualified(const cbm_oo_index_t *ix, const char *prefix,
                                     const char *name) {
    char buffer[OO_KEY_MAX];
    int length = prefix && prefix[0] ? snprintf(buffer, sizeof(buffer), "%s.%s", prefix, name)
                                     : snprintf(buffer, sizeof(buffer), "%s", name);
    return length > 0 && (size_t)length < sizeof(buffer) ? cbm_ht_get(ix->types, buffer) : NULL;
}

static bool oo_builtin_name(const char *name) {
    static const char *const names[] = {"int",   "long",   "short",   "byte", "char",
                                        "float", "double", "boolean", "void", NULL};
    for (int i = 0; names[i]; i++)
        if (!strcmp(name, names[i]))
            return true;
    return false;
}

/* Type parameters and the enclosing-type chain outrank every import. */
static bool oo_lexical_name(const cbm_oo_index_t *ix, const oo_scope_t *scope, const char *segment,
                            oo_name_t *out) {
    for (int i = 0; i < scope->tparam_count; i++) {
        if (!strcmp(scope->tparams[i].name, segment)) {
            *out = (oo_name_t){.status = OO_NAME_TPARAM, .tparam = &scope->tparams[i]};
            return true;
        }
    }
    for (const oo_type_t *type = scope->owner; type; type = type->owner) {
        for (int i = 0; i < type->tparam_count; i++) {
            if (!strcmp(type->tparams[i].name, segment)) {
                *out = (oo_name_t){.status = OO_NAME_TPARAM, .tparam = &type->tparams[i]};
                return true;
            }
        }
        if (!strcmp(type->name, segment)) {
            /* Inside its own body a type's name is this declaration. */
            *out = (oo_name_t){.status = OO_NAME_TYPE, .type = type};
            if (type->flags & OO_F_AMBIGUOUS)
                *out = oo_name_type(ix, scope->file, type);
            return true;
        }
        const oo_type_t *nested = oo_find_nested(ix, type, segment);
        if (nested) {
            *out = oo_name_type(ix, scope->file, nested);
            return true;
        }
    }
    return false;
}

static oo_name_t oo_star_name(const cbm_oo_index_t *ix, const oo_file_t *file, const char *segment,
                              const char *only_ns) {
    const oo_type_t *found = NULL;
    for (int i = 0; i < file->import_count; i++) {
        const oo_import_t *import = &file->imports[i];
        if (!import->star)
            continue;
        if (only_ns && strcmp(import->ns ? import->ns : "", only_ns))
            continue;
        /* A static import brings the type's nested types in as well (JLS 7.5.4, C# 14.5.4). */
        const oo_type_t *imported =
            import->statik ? oo_lookup_visible(ix, import->path, file) : NULL;
        const oo_type_t *candidate = !import->statik ? oo_qualified(ix, import->path, segment)
                                     : imported && !(imported->flags & OO_F_AMBIGUOUS)
                                         ? oo_find_nested(ix, imported, segment)
                                         : NULL;
        if (!candidate)
            continue;
        if (found && found != candidate)
            return (oo_name_t){.status = OO_NAME_AMBIGUOUS};
        found = candidate;
    }
    return oo_name_type(ix, file, found);
}

static oo_name_t oo_jvm_simple(const cbm_oo_index_t *ix, const oo_scope_t *scope,
                               const char *segment) {
    const oo_file_t *file = scope->file;
    /* JLS 6.4.1 / Kotlin spec: explicit import shadows the package, which
     * shadows on-demand imports. */
    for (int i = 0; i < file->import_count; i++) {
        const oo_import_t *import = &file->imports[i];
        if (import->star || !import->local || strcmp(import->local, segment))
            continue;
        const oo_type_t *type = oo_lookup_visible(ix, import->path, file);
        if (type) {
            oo_name_t name = oo_name_type(ix, file, type);
            name.via_import = true;
            return name;
        }
        return (oo_name_t){.status = OO_NAME_EXTERNAL, .external = import->path};
    }
    const oo_type_t *own = oo_qualified(ix, file->package, segment);
    if (own)
        return oo_name_type(ix, file, own);
    oo_name_t star = oo_star_name(ix, file, segment, NULL);
    if (star.status != OO_NAME_UNKNOWN) {
        star.via_import = true;
        return star;
    }
    return (oo_name_t){.status = OO_NAME_EXTERNAL, .external = segment};
}

/* C# §7.8: each enclosing namespace's members, then that same namespace
 * declaration's using aliases and using namespaces, innermost first. */
static oo_name_t oo_cs_simple(const cbm_oo_index_t *ix, const oo_scope_t *scope,
                              const char *segment) {
    char prefix[OO_KEY_MAX];
    int length = snprintf(prefix, sizeof(prefix), "%s", scope->ns ? scope->ns : "");
    if (length < 0 || (size_t)length >= sizeof(prefix))
        return (oo_name_t){.status = OO_NAME_UNKNOWN};
    for (;;) {
        const oo_type_t *member = oo_qualified(ix, prefix, segment);
        if (member)
            return oo_name_type(ix, scope->file, member);
        const oo_file_t *file = scope->file;
        for (int i = 0; i < file->import_count; i++) {
            const oo_import_t *import = &file->imports[i];
            if (import->star || !import->local || strcmp(import->local, segment) ||
                strcmp(import->ns ? import->ns : "", prefix))
                continue;
            const oo_type_t *type = oo_lookup_visible(ix, import->path, file);
            if (type) {
                oo_name_t name = oo_name_type(ix, file, type);
                name.via_import = true;
                return name;
            }
            return (oo_name_t){.status = OO_NAME_EXTERNAL, .external = import->path};
        }
        oo_name_t star = oo_star_name(ix, file, segment, prefix);
        if (star.status != OO_NAME_UNKNOWN) {
            star.via_import = true;
            return star;
        }
        if (!prefix[0])
            break;
        char *dot = strrchr(prefix, '.');
        if (dot)
            *dot = '\0';
        else
            prefix[0] = '\0';
    }
    return (oo_name_t){.status = OO_NAME_EXTERNAL, .external = segment};
}

oo_name_t oo_resolve_name(const cbm_oo_index_t *ix, const oo_scope_t *scope, const char *name) {
    if (!name || !name[0] || !scope || !scope->file)
        return (oo_name_t){.status = OO_NAME_UNKNOWN};
    const char *dot = strchr(name, '.');
    char first[256];
    size_t length = dot ? (size_t)(dot - name) : strlen(name);
    if (length >= sizeof(first))
        return (oo_name_t){.status = OO_NAME_UNKNOWN};
    memcpy(first, name, length);
    first[length] = '\0';
    if (!dot && oo_builtin_name(first))
        return (oo_name_t){.status = OO_NAME_EXTERNAL, .external = name};
    oo_name_t head = {0};
    if (!oo_lexical_name(ix, scope, first, &head))
        head = scope->file->language == CBM_LANG_CSHARP ? oo_cs_simple(ix, scope, first)
                                                        : oo_jvm_simple(ix, scope, first);
    if (!dot) {
        /* `first` dies with this frame; `name` holds the same text. */
        if (head.status == OO_NAME_EXTERNAL && head.external == first)
            head.external = name;
        return head;
    }
    if (head.status == OO_NAME_TYPE) {
        const oo_type_t *type = head.type;
        const char *rest = dot + 1;
        while (rest && *rest) {
            const char *next = strchr(rest, '.');
            size_t part = next ? (size_t)(next - rest) : strlen(rest);
            char segment[256];
            if (part >= sizeof(segment))
                return (oo_name_t){.status = OO_NAME_UNKNOWN};
            memcpy(segment, rest, part);
            segment[part] = '\0';
            type = oo_find_nested(ix, type, segment);
            if (!type)
                return (oo_name_t){.status = OO_NAME_UNKNOWN};
            rest = next ? next + 1 : NULL;
        }
        return oo_name_type(ix, scope->file, type);
    }
    if (head.status == OO_NAME_AMBIGUOUS)
        return head;
    /* Not a type in scope: the spelling can only be fully qualified. */
    const oo_type_t *exact = oo_lookup_visible(ix, name, scope->file);
    if (exact)
        return oo_name_type(ix, scope->file, exact);
    return (oo_name_t){.status = OO_NAME_EXTERNAL, .external = name};
}

/* ── Values ─────────────────────────────────────────────────────────── */

oo_value_t oo_value_unknown(void) {
    return (oo_value_t){0};
}

bool oo_value_known(const oo_value_t *value) {
    return value && (value->type || value->external || value->fn_ref);
}

oo_value_t oo_value_of_type(const oo_type_t *type) {
    return (oo_value_t){.type = type};
}

oo_value_t oo_value_external(const char *name) {
    return (oo_value_t){.external = name};
}

oo_scope_t oo_scope_of_type(const oo_type_t *type) {
    return (oo_scope_t){.file = type->file, .ns = type->ns, .owner = type};
}

oo_scope_t oo_scope_of_callable(const oo_callable_t *callable) {
    return (oo_scope_t){.file = callable->file,
                        .ns = callable->ns,
                        .owner = callable->owner,
                        .tparams = callable->tparams,
                        .tparam_count = callable->tparam_count};
}

oo_scope_t oo_scope_of_field(const oo_field_t *field) {
    return (oo_scope_t){.file = field->file,
                        .ns = field->ns,
                        .owner = field->owner,
                        .tparams = field->tparams,
                        .tparam_count = field->tparam_count};
}

oo_subst_t *oo_subst_of(oo_walk_t *w, const oo_value_t *value) {
    if (!value || !value->type || !value->type->tparam_count || !value->arg_count)
        return NULL;
    oo_subst_t *subst = cbm_arena_alloc(w->arena, sizeof(*subst));
    if (!subst)
        return NULL;
    *subst = (oo_subst_t){.tparams = value->type->tparams,
                          .count = value->type->tparam_count,
                          .args = value->args,
                          .arg_count = value->arg_count};
    return subst;
}

static const oo_value_t *oo_subst_lookup(const oo_subst_t *subst, const oo_tparam_t *tparam) {
    for (; subst; subst = subst->next) {
        if (tparam >= subst->tparams && tparam < subst->tparams + subst->count) {
            int index = (int)(tparam - subst->tparams);
            return index < subst->arg_count ? &subst->args[index] : NULL;
        }
    }
    return NULL;
}

oo_value_t oo_value_from_ref(oo_walk_t *w, const oo_typeref_t *ref, const oo_scope_t *scope,
                             const oo_subst_t *subst) {
    if (!ref || !scope || w->depth >= OO_MAX_DEPTH)
        return oo_value_unknown();
    if (ref->function) {
        oo_value_t value = {.fn_ref = ref};
        w->depth++;
        if (ref->fn_receiver) {
            oo_value_t *receiver = cbm_arena_alloc(w->arena, sizeof(*receiver));
            if (receiver) {
                *receiver = oo_value_from_ref(w, ref->fn_receiver, scope, subst);
                value.fn_receiver = receiver;
            }
        }
        if (ref->fn_return) {
            oo_value_t *result = cbm_arena_alloc(w->arena, sizeof(*result));
            if (result) {
                *result = oo_value_from_ref(w, ref->fn_return, scope, subst);
                value.fn_return = result;
            }
        }
        w->depth--;
        return value;
    }
    if (!ref->name && ref->lower) {
        oo_value_t value = oo_value_unknown();
        oo_value_t *lower = cbm_arena_alloc(w->arena, sizeof(*lower));
        if (lower) {
            w->depth++;
            *lower = oo_value_from_ref(w, ref->lower, scope, subst);
            w->depth--;
            value.lower = lower;
        }
        return value;
    }
    if (!ref->name)
        return oo_value_unknown();
    oo_name_t name = oo_resolve_name(w->ix, scope, ref->name);
    oo_value_t value = oo_value_unknown();
    w->depth++;
    switch (name.status) {
    case OO_NAME_TPARAM: {
        const oo_value_t *bound = oo_subst_lookup(subst, name.tparam);
        if (bound && (oo_value_known(bound) || bound->lower))
            value = *bound;
        else if (name.tparam->bound)
            value = oo_value_from_ref(w, name.tparam->bound, scope, subst);
        break;
    }
    case OO_NAME_TYPE: {
        const oo_type_t *type = name.type;
        if (type->alias_of) {
            oo_scope_t alias_scope = oo_scope_of_type(type);
            value = oo_value_from_ref(w, type->alias_of, &alias_scope, NULL);
            break;
        }
        value.type = type;
        break;
    }
    case OO_NAME_EXTERNAL:
        value.external = name.external;
        break;
    default:
        break;
    }
    if ((value.type || value.external) && ref->args && !value.arg_count) {
        int count = 0;
        while (count < OO_ARG_SLOTS && ref->args[count])
            count++;
        oo_value_t *args = count ? cbm_arena_alloc(w->arena, (size_t)count * sizeof(*args)) : NULL;
        if (args) {
            for (int i = 0; i < count; i++)
                args[i] = oo_value_from_ref(w, ref->args[i], scope, subst);
            value.args = args;
            value.arg_count = count;
        }
    }
    w->depth--;
    value.dims += ref->dims;
    return value;
}

bool oo_names_tparam(const oo_walk_t *w, const char *name) {
    return name && oo_resolve_name(w->ix, &w->scope, name).status == OO_NAME_TPARAM;
}

bool oo_type_shaped(const char *name) {
    if (!name || name[0] < 'A' || name[0] > 'Z')
        return false;
    for (const char *p = name; *p; p++)
        if (*p >= 'a' && *p <= 'z')
            return true;
    return false;
}

/* JDK, Kotlin and .NET supertypes small enough to list. A class naming only
 * these still has a known member set, because the interface members it must
 * implement are declared in its own body. */
static const struct {
    const char *type;
    const char *members;
} OO_CLOSED_SUPERS[] = {
    {"AutoCloseable", "close"},
    {"Closeable", "close"},
    {"Comparable", "compareTo"},
    {"Serializable", ""},
    {"Cloneable", "clone"},
    {"Runnable", "run"},
    {"Callable", "call"},
    {"Iterable", "iterator forEach spliterator"},
    {"Iterator", "hasNext next remove forEachRemaining"},
    {"MutableIterator", "hasNext next remove"},
    {"Supplier", "get"},
    {"Consumer", "accept andThen"},
    {"BiConsumer", "accept andThen"},
    {"Function", "apply andThen compose"},
    {"BiFunction", "apply andThen"},
    {"Predicate", "test and or negate"},
    {"Comparator", "compare reversed thenComparing"},
    {"Any", "toString equals hashCode"},
    {"Object", "toString equals hashCode getClass notify notifyAll wait clone finalize"},
    {"Throwable", "getMessage getCause getLocalizedMessage printStackTrace getStackTrace "
                  "setStackTrace initCause fillInStackTrace addSuppressed getSuppressed "
                  "message cause stackTraceToString"},
    {"Exception", "getMessage getCause getLocalizedMessage printStackTrace getStackTrace "
                  "setStackTrace initCause fillInStackTrace addSuppressed getSuppressed "
                  "message cause stackTraceToString"},
    {"RuntimeException", "getMessage getCause getLocalizedMessage printStackTrace getStackTrace "
                         "setStackTrace initCause fillInStackTrace addSuppressed getSuppressed "
                         "message cause stackTraceToString"},
    {"IDisposable", "Dispose"},
    {"IAsyncDisposable", "DisposeAsync"},
    {"ICloneable", "Clone"},
    {"IComparable", "CompareTo"},
    {"IEquatable", "Equals"},
    {"IEnumerable", "GetEnumerator"},
    {"IEnumerator", "Current MoveNext Reset Dispose"},
};

static bool oo_word_in(const char *words, const char *word) {
    size_t length = strlen(word);
    for (const char *p = words; (p = strstr(p, word)) != NULL; p += length) {
        bool start = p == words || p[-1] == ' ';
        bool end = p[length] == ' ' || p[length] == '\0';
        if (start && end)
            return true;
    }
    return false;
}

/* The leaf of a spelled name, without its arity. */
static const char *oo_plain_leaf(const char *name, char *buffer, size_t capacity) {
    const char *dot = name ? strrchr(name, '.') : NULL;
    const char *leaf = dot ? dot + 1 : name;
    if (!leaf)
        return NULL;
    size_t length = strcspn(leaf, "`");
    if (length >= capacity)
        return NULL;
    memcpy(buffer, leaf, length);
    buffer[length] = '\0';
    return buffer;
}

static bool oo_closed_super(const char *name) {
    char leaf[64];
    if (!oo_plain_leaf(name, leaf, sizeof(leaf)))
        return false;
    for (size_t i = 0; i < sizeof(OO_CLOSED_SUPERS) / sizeof(OO_CLOSED_SUPERS[0]); i++)
        if (!strcmp(OO_CLOSED_SUPERS[i].type, leaf))
            return true;
    return false;
}

/* Does an enclosing type have this member from outside the project? Every class
 * has the root type's members; a listed supertype brings its own. Such a member
 * has no declaration to bind, and beats a free function of the name. */
bool oo_closed_member(const oo_walk_t *w, const char *name) {
    static const char *const ROOT_MEMBERS =
        "toString equals hashCode getClass notify notifyAll wait finalize clone "
        "ToString Equals GetHashCode GetType MemberwiseClone ReferenceEquals Finalize";
    if (!name)
        return false;
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        if (!r->value.type)
            continue;
        if (oo_word_in(ROOT_MEMBERS, name))
            return true;
        const oo_type_t *queue[OO_MAX_HIERARCHY];
        int head = 0, tail = 0;
        queue[tail++] = r->value.type;
        while (head < tail) {
            const oo_type_t *current = queue[head++];
            for (int i = 0; i < current->super_count; i++) {
                const oo_super_t *super = &current->supers[i];
                const oo_type_t *next = super->resolved;
                if (!next) {
                    const char *spelled = super->ref ? super->ref->name : NULL;
                    const char *dot = spelled ? strrchr(spelled, '.') : NULL;
                    const char *leaf = dot ? dot + 1 : spelled;
                    for (size_t t = 0;
                         leaf && t < sizeof(OO_CLOSED_SUPERS) / sizeof(OO_CLOSED_SUPERS[0]); t++)
                        if (!strcmp(OO_CLOSED_SUPERS[t].type, leaf) &&
                            oo_word_in(OO_CLOSED_SUPERS[t].members, name))
                            return true;
                    continue;
                }
                bool seen = false;
                for (int j = 0; j < tail && !seen; j++)
                    seen = queue[j] == next;
                if (!seen && tail < OO_MAX_HIERARCHY)
                    queue[tail++] = next;
            }
        }
    }
    return false;
}

static bool oo_name_in(const char *leaf, const char *const *names) {
    for (int i = 0; names[i]; i++)
        if (!strcmp(leaf, names[i]))
            return true;
    return false;
}

static bool oo_same_value(const oo_value_t *a, const oo_value_t *b);

static const char *const OO_ONE_ARG_COLLECTIONS[] = {"Iterable",
                                                     "MutableIterable",
                                                     "Collection",
                                                     "MutableCollection",
                                                     "List",
                                                     "MutableList",
                                                     "ArrayList",
                                                     "LinkedList",
                                                     "Set",
                                                     "MutableSet",
                                                     "HashSet",
                                                     "LinkedHashSet",
                                                     "TreeSet",
                                                     "SortedSet",
                                                     "NavigableSet",
                                                     "Queue",
                                                     "Deque",
                                                     "ArrayDeque",
                                                     "Vector",
                                                     "Sequence",
                                                     "Flow",
                                                     "Array",
                                                     "Iterator",
                                                     "ListIterator",
                                                     "MutableIterator",
                                                     "Stream",
                                                     "Optional",
                                                     "IEnumerable",
                                                     "ICollection",
                                                     "IList",
                                                     "IReadOnlyList",
                                                     "IReadOnlyCollection",
                                                     "ISet",
                                                     "Stack",
                                                     "IEnumerator",
                                                     "Span",
                                                     "ReadOnlySpan",
                                                     "Memory",
                                                     "ReadOnlyMemory",
                                                     NULL};

static const char *const OO_MAPS[] = {"Map",
                                      "MutableMap",
                                      "HashMap",
                                      "LinkedHashMap",
                                      "TreeMap",
                                      "SortedMap",
                                      "NavigableMap",
                                      "ConcurrentHashMap",
                                      "ConcurrentMap",
                                      "Dictionary",
                                      "IDictionary",
                                      "IReadOnlyDictionary",
                                      NULL};

oo_value_t oo_element_of(oo_walk_t *w, const oo_value_t *value) {
    if (!value)
        return oo_value_unknown();
    if (value->dims > 0) {
        oo_value_t element = *value;
        element.dims--;
        element.classref = false;
        return element;
    }
    char leaf[64];
    if (value->external && value->arg_count == 1 &&
        oo_plain_leaf(value->external, leaf, sizeof(leaf)) &&
        oo_name_in(leaf, OO_ONE_ARG_COLLECTIONS)) {
        /* `? super X` yields nothing known; `? extends X` already reads as X. */
        return value->args[0];
    }
    /* Kotlin and C# iterate a map's entries. Java does not compile this. */
    if (value->external && value->arg_count == 2 &&
        oo_plain_leaf(value->external, leaf, sizeof(leaf)) && oo_name_in(leaf, OO_MAPS)) {
        oo_value_t entry = oo_value_external("Entry");
        entry.args = value->args;
        entry.arg_count = 2;
        return entry;
    }
    if (!value->type)
        return oo_value_unknown();
    /* A project type's own Iterable<X> supertype, with its arguments substituted. */
    const oo_type_t *visited[32];
    int seen = 0;
    oo_value_t current = *value;
    for (int hops = 0; hops < 32 && current.type; hops++) {
        bool repeat = false;
        for (int i = 0; i < seen; i++)
            repeat = repeat || visited[i] == current.type;
        if (repeat)
            break;
        visited[seen++] = current.type;
        oo_subst_t *subst = oo_subst_of(w, &current);
        const oo_type_t *type = current.type;
        oo_value_t next = oo_value_unknown();
        for (int i = 0; i < type->super_count; i++) {
            const oo_super_t *super = &type->supers[i];
            oo_scope_t scope = {.file = super->file, .ns = super->ns, .owner = type};
            oo_value_t candidate = oo_value_from_ref(w, super->ref, &scope, subst);
            if (candidate.external && candidate.arg_count == 1 &&
                oo_plain_leaf(candidate.external, leaf, sizeof(leaf)) &&
                oo_name_in(leaf, OO_ONE_ARG_COLLECTIONS))
                return candidate.args[0];
            if (candidate.type && !next.type)
                next = candidate;
        }
        current = next;
    }
    return oo_value_unknown();
}

oo_value_t oo_external_member_return(oo_walk_t *w, const oo_value_t *value, const char *name) {
    char leaf[64];
    if (!value || !name || !value->external || value->dims ||
        !oo_plain_leaf(value->external, leaf, sizeof(leaf)))
        return oo_value_unknown();
    static const char *const elements[] = {"get",
                                           "first",
                                           "last",
                                           "single",
                                           "firstOrNull",
                                           "lastOrNull",
                                           "singleOrNull",
                                           "getOrNull",
                                           "next",
                                           "peek",
                                           "poll",
                                           "pop",
                                           "element",
                                           "removeFirst",
                                           "removeLast",
                                           "getFirst",
                                           "getLast",
                                           "orElse",
                                           "orElseThrow",
                                           "orElseGet",
                                           "orNull",
                                           "getOrThrow",
                                           "Current",
                                           "First",
                                           "Last",
                                           "Single",
                                           "FirstOrDefault",
                                           "LastOrDefault",
                                           "SingleOrDefault",
                                           "ElementAt",
                                           NULL};
    static const char *const same[] = {
        "iterator",   "listIterator", "stream",    "filter",         "sorted",   "distinct",
        "limit",      "skip",         "toList",    "toMutableList",  "toSet",    "asSequence",
        "asIterable", "reversed",     "subList",   "take",           "drop",     "filterNotNull",
        "peek",       "takeWhile",    "dropWhile", "parallelStream", "parallel", "sequential",
        "Where",      "ToList",       "ToArray",   "AsEnumerable",   NULL};
    static const char *const optional_of[] = {"findFirst", "findAny", "min", "max", NULL};
    if (value->arg_count == 1 && oo_name_in(leaf, OO_ONE_ARG_COLLECTIONS)) {
        if (oo_name_in(name, elements))
            return value->args[0];
        if (oo_name_in(name, same)) {
            oo_value_t copy = *value;
            copy.classref = false;
            return copy;
        }
        if (oo_name_in(name, optional_of) && !strcmp(leaf, "Stream")) {
            oo_value_t optional = *value;
            optional.classref = false;
            optional.external = "Optional";
            return optional;
        }
    }
    /* A map entry, or a pair. */
    if (value->arg_count == 2 && (!strcmp(leaf, "Entry") || !strcmp(leaf, "MutableEntry") ||
                                  !strcmp(leaf, "KeyValuePair") || !strcmp(leaf, "Pair"))) {
        static const char *const keys[] = {"getKey", "key", "Key", "first", NULL};
        static const char *const vals[] = {"getValue", "value", "Value", "second", NULL};
        if (oo_name_in(name, keys))
            return value->args[0];
        if (oo_name_in(name, vals))
            return value->args[1];
    }
    static const char *const map_values[] = {"get",       "getValue", "getOrDefault",    "getOrPut",
                                             "getOrElse", "remove",   "computeIfAbsent", NULL};
    if (value->arg_count == 2 && oo_name_in(leaf, OO_MAPS) && oo_name_in(name, map_values))
        return value->args[1];
    if (value->arg_count == 2 && oo_name_in(leaf, OO_MAPS)) {
        /* The map's views: what iterating them, or their stream, yields. */
        oo_value_t view = oo_value_unknown();
        if (!strcmp(name, "keySet")) {
            view = oo_value_external("Set");
            view.args = &value->args[0];
            view.arg_count = 1;
        } else if (!strcmp(name, "values")) {
            view = oo_value_external("Collection");
            view.args = &value->args[1];
            view.arg_count = 1;
        } else if (!strcmp(name, "entrySet")) {
            oo_value_t *entry = cbm_arena_alloc(w->arena, sizeof(*entry));
            if (entry) {
                *entry = oo_value_external("Entry");
                entry->args = value->args;
                entry->arg_count = 2;
                view = oo_value_external("Set");
                view.args = entry;
                view.arg_count = 1;
            }
        }
        return view;
    }
    return oo_value_unknown();
}

oo_value_t oo_external_with_element(oo_walk_t *w, const oo_value_t *value, oo_value_t element) {
    oo_value_t *slot = cbm_arena_alloc(w->arena, sizeof(*slot));
    if (!slot || !value->external || !oo_value_known(&element))
        return oo_value_unknown();
    *slot = element;
    slot->classref = false;
    oo_value_t copy = *value;
    copy.classref = false;
    copy.args = slot;
    copy.arg_count = 1;
    return copy;
}

oo_value_t oo_jdk_factory_result(oo_walk_t *w, const oo_value_t *receiver, const char *name,
                                 const oo_value_t *args, int argc) {
    char leaf[64];
    if (!receiver->classref || !receiver->external || argc < 1 || !args ||
        strncmp(receiver->external, "java.util.", 10) != 0 ||
        !oo_plain_leaf(receiver->external, leaf, sizeof(leaf)))
        return oo_value_unknown();
    static const struct {
        const char *owner, *factory, *result;
        bool copies; /* The argument is a collection whose element carries over. */
    } FACTORIES[] = {{"Optional", "of", "Optional", false},
                     {"Optional", "ofNullable", "Optional", false},
                     {"List", "of", "List", false},
                     {"Set", "of", "Set", false},
                     {"List", "copyOf", "List", true},
                     {"Set", "copyOf", "Set", true},
                     {"Stream", "of", "Stream", false},
                     {"Arrays", "asList", "List", false},
                     {"Arrays", "stream", "Stream", false},
                     {"Collections", "singletonList", "List", false},
                     {"Collections", "singleton", "Set", false},
                     {"Collections", "unmodifiableList", "List", true},
                     {"Collections", "unmodifiableSet", "Set", true},
                     {"Collections", "unmodifiableCollection", "Collection", true}};
    for (size_t i = 0; i < sizeof(FACTORIES) / sizeof(FACTORIES[0]); i++) {
        if (strcmp(FACTORIES[i].owner, leaf) || strcmp(FACTORIES[i].factory, name))
            continue;
        oo_value_t element = args[0];
        if (FACTORIES[i].copies) {
            element = oo_element_of(w, &args[0]);
        } else if (argc == 1 && element.dims > 0) {
            element.dims--;
        } else {
            for (int k = 1; k < argc; k++)
                if (!oo_same_value(&element, &args[k]))
                    return oo_value_unknown();
        }
        oo_value_t made = oo_value_external(FACTORIES[i].result);
        return oo_external_with_element(w, &made, element);
    }
    return oo_value_unknown();
}

int oo_stdlib_lambda_parameters(oo_walk_t *w, const oo_value_t *receiver, const char *name,
                                int position, int argc, oo_value_t *out, int capacity) {
    static const char *const each[] = {
        /* Java: Iterable, Collection, Stream, Optional. */
        "forEach", "forEachOrdered", "removeIf", "filter", "map", "mapToInt", "mapToLong",
        "mapToDouble", "mapToObj", "flatMap", "anyMatch", "allMatch", "noneMatch", "peek",
        "takeWhile", "dropWhile", "ifPresent", "ifPresentOrElse",
        /* C#: List and Enumerable. */
        "ForEach", "Exists", "Find", "FindAll", "FindIndex", "FindLast", "FindLastIndex",
        "RemoveAll", "TrueForAll", "Where", "Select", "SelectMany", "Any", "All", "First",
        "FirstOrDefault", "Last", "LastOrDefault", "Single", "SingleOrDefault", "Count",
        "LongCount", "OrderBy", "OrderByDescending", "ThenBy", "ThenByDescending", "GroupBy",
        "ToDictionary", "ToLookup", "Sum", "Min", "Max", "Average", "SkipWhile", "TakeWhile",
        "DistinctBy", "MinBy", "MaxBy", "Sort", NULL};
    static const char *const pair[] = {"sorted", "sort", "min", "max", NULL};
    if (!receiver || capacity < 2 || position < 0)
        return -1;
    char leaf[64];
    if (receiver->external && receiver->arg_count == 2 &&
        oo_plain_leaf(receiver->external, leaf, sizeof(leaf)) && !strcmp(leaf, "Map")) {
        if ((!strcmp(name, "forEach") || !strcmp(name, "replaceAll")) && position == 0) {
            out[0] = receiver->args[0];
            out[1] = receiver->args[1];
            return 2;
        }
        if (!strcmp(name, "computeIfAbsent") && position == 1) {
            out[0] = receiver->args[0];
            return 1;
        }
        if ((!strcmp(name, "computeIfPresent") || !strcmp(name, "compute")) && position == 1) {
            out[0] = receiver->args[0];
            out[1] = receiver->args[1];
            return 2;
        }
        if (!strcmp(name, "merge") && position == 2) {
            out[0] = receiver->args[1];
            out[1] = receiver->args[1];
            return 2;
        }
        return -1;
    }
    oo_value_t element = oo_element_of(w, receiver);
    if (!oo_value_known(&element))
        return -1;
    if (oo_name_in(name, pair) && argc == 1 && position == 0) {
        out[0] = element;
        out[1] = element;
        return 2;
    }
    if (oo_name_in(name, each) && position == 0) {
        out[0] = element;
        return 1;
    }
    return -1;
}

/* ── Member selection ───────────────────────────────────────────────── */

typedef struct {
    const oo_type_t *type;
    oo_value_t value;
    oo_subst_t *subst;
} oo_step_t;

typedef struct {
    oo_step_t steps[OO_MAX_HIERARCHY];
    int head, tail;
    oo_walk_t *w;
    bool truncated;
} oo_hierarchy_t;

static void oo_hierarchy_add(oo_hierarchy_t *h, oo_value_t value) {
    const oo_type_t *type = value.type;
    if (!type)
        return;
    for (int i = 0; i < h->tail; i++)
        if (h->steps[i].type == type)
            return;
    if (h->tail == OO_MAX_HIERARCHY) {
        h->truncated = true;
        return;
    }
    h->steps[h->tail++] =
        (oo_step_t){.type = type, .value = value, .subst = oo_subst_of(h->w, &value)};
}

static void oo_hierarchy_expand(oo_hierarchy_t *h, const oo_step_t *step) {
    const oo_type_t *type = step->type;
    for (int i = 0; i < type->super_count; i++) {
        const oo_super_t *super = &type->supers[i];
        if (!super->resolved)
            continue;
        oo_scope_t scope = {.file = super->file, .ns = super->ns, .owner = type};
        oo_value_t value = oo_value_from_ref(h->w, super->ref, &scope, step->subst);
        if (value.type != super->resolved)
            value = oo_value_of_type(super->resolved);
        oo_hierarchy_add(h, value);
    }
}

static const oo_type_t *oo_outermost(const oo_type_t *type) {
    while (type && type->owner)
        type = type->owner;
    return type;
}

/* A private member is not inherited, and is visible only inside its own
 * top-level declaration (JLS 6.6.1, C# §7.5). */
static bool oo_private_visible(const oo_walk_t *w, const oo_type_t *owner) {
    if (!w->scope.owner)
        return false;
    return oo_outermost(w->scope.owner) == oo_outermost(owner);
}

bool oo_callable_visible(const oo_walk_t *w, const oo_callable_t *callable) {
    if (callable->file && callable->file->test_source && w->f &&
        oo_file_outside_tests(w->ix, callable->file, w->f))
        return false;
    if (!(callable->flags & OO_F_PRIVATE))
        return true;
    if (callable->owner)
        return oo_private_visible(w, callable->owner);
    /* Kotlin file-private top-level declaration. */
    return callable->file && !strcmp(callable->file->rel, w->rel_path);
}

static bool oo_arity_fits(const oo_callable_t *callable, int argc) {
    if (argc < 0)
        return true;
    if (argc < callable->min_args)
        return false;
    return callable->max_args < 0 || argc <= callable->max_args;
}

static const char *oo_leaf(const char *name) {
    const char *dot = name ? strrchr(name, '.') : NULL;
    return dot ? dot + 1 : name;
}

static bool oo_ref_is_tparam(const oo_callable_t *callable, const oo_typeref_t *ref) {
    if (!ref || !ref->name)
        return false;
    oo_scope_t scope = oo_scope_of_callable(callable);
    for (int i = 0; i < scope.tparam_count; i++)
        if (!strcmp(scope.tparams[i].name, ref->name))
            return true;
    for (const oo_type_t *type = scope.owner; type; type = type->owner)
        for (int i = 0; i < type->tparam_count; i++)
            if (!strcmp(type->tparams[i].name, ref->name))
                return true;
    return false;
}

/* `later` is declared further up the hierarchy than `earlier`. Equal parameter
 * spellings override; so does a parameter the supertype leaves generic, which
 * the subtype instantiated. Anything else is a real overload. */
static bool oo_overrides(const oo_callable_t *earlier, const oo_callable_t *later) {
    if (earlier->param_count != later->param_count)
        return false;
    for (int i = 0; i < earlier->param_count; i++) {
        const oo_typeref_t *a = earlier->params ? earlier->params[i] : NULL;
        const oo_typeref_t *b = later->params ? later->params[i] : NULL;
        if (!a || !b || !a->name || !b->name) {
            if (a != b && !(earlier->flags & OO_F_OVERRIDE))
                return false;
            continue;
        }
        if (oo_ref_is_tparam(later, b))
            continue;
        if (a->dims != b->dims || strcmp(oo_leaf(a->name), oo_leaf(b->name)))
            return false;
    }
    return true;
}

typedef enum {
    OO_SCALAR_NONE,
    OO_SCALAR_INTEGER,
    OO_SCALAR_REAL,
    OO_SCALAR_BOOL,
    OO_SCALAR_CHAR,
    OO_SCALAR_STRING
} oo_scalar_t;

static oo_scalar_t oo_scalar_of(const char *leaf) {
    static const char *const integers[] = {
        "int",     "long",  "short", "byte",  "sbyte",  "uint",   "ulong",  "ushort", "nint",
        "nuint",   "Int",   "Long",  "Short", "Byte",   "UInt",   "ULong",  "UShort", "UByte",
        "Integer", "Int16", "Int32", "Int64", "UInt16", "UInt32", "UInt64", "SByte",  NULL};
    static const char *const reals[] = {"float",  "double",  "decimal", "Float",
                                        "Double", "Decimal", "Single",  NULL};
    static const char *const bools[] = {"boolean", "bool", "Boolean", NULL};
    static const char *const chars[] = {"char", "Char", "Character", NULL};
    static const char *const strings[] = {"String", "string", NULL};
    if (oo_name_in(leaf, integers))
        return OO_SCALAR_INTEGER;
    if (oo_name_in(leaf, reals))
        return OO_SCALAR_REAL;
    if (oo_name_in(leaf, bools))
        return OO_SCALAR_BOOL;
    if (oo_name_in(leaf, chars))
        return OO_SCALAR_CHAR;
    return oo_name_in(leaf, strings) ? OO_SCALAR_STRING : OO_SCALAR_NONE;
}

/* A scalar value cannot reach a scalar parameter of another family. Java and C#
 * widen integers and chars into numbers; Kotlin widens nothing. Inside the integers
 * only a long into something narrower is certain, because a small literal fits all. */
static bool oo_scalar_mismatch(CBMLanguage language, const char *parameter,
                               const oo_value_t *value) {
    const char *argument = value->external;
    const char *parameter_leaf = oo_leaf(parameter), *argument_leaf = oo_leaf(argument);
    oo_scalar_t to = oo_scalar_of(parameter_leaf), from = oo_scalar_of(argument_leaf);
    /* The root type does not convert to a scalar without a cast, in any of the three. */
    static const char *const roots[] = {"object", "Object", "Any", NULL};
    if (to != OO_SCALAR_NONE && oo_name_in(argument_leaf, roots))
        return true;
    if (to == OO_SCALAR_NONE || from == OO_SCALAR_NONE)
        return false;
    if (to == from) {
        /* Kotlin widens nothing: an Int variable is not a Long. A literal adapts. */
        if (language == CBM_LANG_KOTLIN && to == OO_SCALAR_INTEGER && !value->small_literal &&
            strcmp(parameter_leaf, argument_leaf))
            return true;
        static const char *const narrow[] = {"int",   "short", "byte",    "Int",   "Short", "Byte",
                                             "sbyte", "Int32", "Integer", "Int16", "SByte", NULL};
        static const char *const wide[] = {"long", "Long", "Int64", NULL};
        return to == OO_SCALAR_INTEGER && oo_name_in(argument_leaf, wide) &&
               oo_name_in(parameter_leaf, narrow);
    }
    if (language != CBM_LANG_KOTLIN && (from == OO_SCALAR_INTEGER || from == OO_SCALAR_CHAR) &&
        (to == OO_SCALAR_INTEGER || to == OO_SCALAR_REAL))
        return false;
    return true;
}

int oo_parameter_named(const oo_callable_t *callable, const char *name) {
    for (int p = 0; name && callable->param_names && p < callable->param_count; p++)
        if (callable->param_names[p] && !strcmp(callable->param_names[p], name))
            return p;
    return -1;
}

/* The parameter this argument lands on: the one it names (C# 12.6.2.2), or else its position's.
 * -1 for a name the declaration does not have. */
static int oo_parameter_of(const oo_callable_t *callable, const oo_value_t *args, int i) {
    return args[i].argument_name ? oo_parameter_named(callable, args[i].argument_name) : i;
}

/* NO only when both sides are known to differ; an unknown keeps the candidate
 * so that it continues to compete. */
/* C# 8.2.3: object, ValueType and Enum stand above every type of their kind, unspelled. */
static bool oo_csharp_root_accepts(const oo_type_t *root, const oo_type_t *type) {
    if (!strcmp(root->qn, "System.Object"))
        return true;
    if (!strcmp(root->qn, "System.ValueType"))
        return (type->flags & (OO_F_VALUE | OO_F_ENUM)) != 0;
    return !strcmp(root->qn, "System.Enum") && (type->flags & OO_F_ENUM);
}

/* C#: `T[]` reaches `Span<T>` and its kin, and no other element. */
static bool oo_span_of_another_element(const oo_value_t *parameter, const oo_value_t *argument) {
    static const char *const SPANS[] = {"Span`1",           "ReadOnlySpan`1", "Memory`1",
                                        "ReadOnlyMemory`1", "Span",           "ReadOnlySpan",
                                        "Memory",           "ReadOnlyMemory", NULL};
    if (argument->dims != 1 || parameter->arg_count != 1 || parameter->dims)
        return false;
    const char *leaf = parameter->type ? parameter->type->name : oo_leaf(parameter->external);
    if (!leaf || !oo_name_in(leaf, SPANS))
        return false;
    const oo_value_t *element = &parameter->args[0];
    if (element->dims)
        return false;
    if (element->type && argument->type)
        return element->type != argument->type;
    return element->external && argument->external &&
           strcmp(oo_leaf(element->external), oo_leaf(argument->external)) != 0;
}

static bool oo_argument_excludes(oo_walk_t *w, const oo_callable_t *callable,
                                 const oo_subst_t *subst, const oo_value_t *args, int argc) {
    oo_scope_t scope = oo_scope_of_callable(callable);
    for (int i = 0; i < argc && i < OO_ARG_SLOTS; i++) {
        int index = i < callable->param_count ? i : callable->param_count - 1;
        /* A Kotlin closing function literal lands on the last parameter, defaults between. */
        if (args[i].function_literal && i == argc - 1 && callable->file &&
            callable->file->language == CBM_LANG_KOTLIN)
            index = callable->param_count - 1;
        /* A name this declaration does not have rules it out. */
        if (args[i].argument_name) {
            index = oo_parameter_of(callable, args, i);
            if (index < 0)
                return true;
        }
        if (index < 0 || !callable->params || !callable->params[index])
            continue;
        if (args[i].by_ref != callable->params[index]->by_ref &&
            !callable->params[index]->ref_optional)
            return true;
        if (callable->max_args < 0 && index == callable->param_count - 1)
            continue; /* Variadic element versus array conversion is not modelled. */
        /* Past a vararg, position no longer says which parameter an argument lands on. */
        if (callable->vararg_position && i >= callable->vararg_position - 1 &&
            !args[i].function_literal)
            continue;
        if (oo_ref_is_tparam(callable, callable->params[index]))
            continue;
        oo_value_t parameter = oo_value_from_ref(w, callable->params[index], &scope, subst);
        const oo_value_t *argument = &args[i];
        if (argument->function_literal) {
            /* A literal converts to a function type or an interface, nothing else. In C#
             * it converts to a delegate, or to the expression tree of one. */
            bool csharp = callable->file && callable->file->language == CBM_LANG_CSHARP;
            if (parameter.type &&
                !(parameter.type->flags & (csharp ? OO_F_DELEGATE : OO_F_INTERFACE)) &&
                !(csharp && !strcmp(parameter.type->name, "Expression`1")))
                return true;
            char collection[64];
            if (parameter.external &&
                (oo_builtin_name(oo_leaf(parameter.external)) ||
                 !strcmp(oo_leaf(parameter.external), "String") ||
                 !strcmp(oo_leaf(parameter.external), "string") ||
                 (oo_plain_leaf(parameter.external, collection, sizeof(collection)) &&
                  (oo_name_in(collection, OO_ONE_ARG_COLLECTIONS) ||
                   oo_name_in(collection, OO_MAPS)))))
                return true;
            /* `x => ...` does not reach a delegate of two parameters. */
            if (argument->lambda_parameters) {
                oo_value_t shape[OO_ARG_SLOTS];
                int fits = parameter.type
                               ? oo_functional_parameters(w, &parameter, shape, OO_ARG_SLOTS)
                               : -1;
                if (fits >= 0 && fits != argument->lambda_parameters - 1)
                    return true;
                if (parameter.external && csharp) {
                    const char *leaf = oo_leaf(parameter.external);
                    int arity = parameter.arg_count;
                    if ((!strncmp(leaf, "Func`", 5) &&
                         arity - 1 != argument->lambda_parameters - 1) ||
                        (!strncmp(leaf, "Action`", 7) && arity != argument->lambda_parameters - 1))
                        return true;
                }
            }
            continue;
        }
        if (callable->file && callable->file->language == CBM_LANG_CSHARP &&
            oo_span_of_another_element(&parameter, argument))
            return true;
        /* Array is final: a project class is never one. */
        if (parameter.external && !strcmp(oo_leaf(parameter.external), "Array") && argument->type &&
            oo_hierarchy_complete(w->ix, argument->type))
            return true;
        if (parameter.fn_ref) {
            /* A value whose whole hierarchy is known is not a function. */
            if (argument->type && oo_hierarchy_complete(w->ix, argument->type))
                return true;
            if (argument->external && !argument->fn_ref &&
                (oo_builtin_name(oo_leaf(argument->external)) ||
                 !strcmp(oo_leaf(argument->external), "String")))
                return true;
            continue;
        }
        /* C# 10.2: a user-defined implicit conversion is declared on either side, and an
         * array reaches the generic collection interfaces. Neither is on the type's hierarchy. */
        bool csharp = callable->file && callable->file->language == CBM_LANG_CSHARP;
        if (parameter.type && argument->type) {
            if (parameter.dims != argument->dims ||
                !oo_is_subtype(w->ix, argument->type, parameter.type)) {
                bool converts =
                    csharp &&
                    (oo_csharp_root_accepts(parameter.type, argument->type) ||
                     (parameter.type->flags & OO_F_CONVERTS) ||
                     (argument->type->flags & OO_F_CONVERTS) ||
                     (argument->dims > parameter.dims && (parameter.type->flags & OO_F_INTERFACE)));
                /* An unresolved supertype could still make it assignable. */
                if (!converts && oo_hierarchy_complete(w->ix, argument->type))
                    return true;
            }
        } else if (parameter.type && argument->external && !argument->fn_ref) {
            /* A literal or JDK value versus a project parameter: only a primitive
             * or String literal is certainly not that class. C#: an integer reaches an
             * enum as a literal 0, and a project declaring the scalars themselves (a core
             * library) has them implement its own interfaces. */
            bool converts =
                csharp && ((parameter.type->flags & (OO_F_CONVERTS | OO_F_INTERFACE | OO_F_ENUM)) ||
                           (argument->dims && (!strcmp(parameter.type->qn, "System.Array") ||
                                               !strcmp(parameter.type->qn, "System.Object"))));
            if (!converts && oo_scalar_of(oo_leaf(argument->external)) != OO_SCALAR_NONE)
                return true;
        } else if (parameter.external && argument->external) {
            if (parameter.dims != argument->dims) {
                /* An array is not its element, and an element is not an array. */
                if (oo_scalar_of(oo_leaf(parameter.external)) != OO_SCALAR_NONE &&
                    oo_scalar_of(oo_leaf(argument->external)) != OO_SCALAR_NONE)
                    return true;
            } else if (!parameter.dims &&
                       oo_scalar_mismatch(callable->file ? callable->file->language : CBM_LANG_JAVA,
                                          parameter.external, argument)) {
                return true;
            }
        } else if (parameter.external && argument->type && oo_builtin_name(parameter.external) &&
                   !(csharp && (argument->type->flags & OO_F_CONVERTS))) {
            return true;
        }
    }
    return false;
}

static bool oo_same_external(const oo_value_t *a, const oo_value_t *b) {
    if (!a->external || !b->external || strcmp(oo_leaf(a->external), oo_leaf(b->external)) ||
        a->dims != b->dims || a->arg_count != b->arg_count)
        return false;
    for (int i = 0; i < a->arg_count; i++) {
        const oo_value_t *x = &a->args[i], *y = &b->args[i];
        bool same = x->type ? x->type == y->type && x->dims == y->dims : oo_same_external(x, y);
        if (!same)
            return false;
    }
    return true;
}

/* Every argument's type is known and identical to its parameter's. Identity is the best
 * conversion in Java (15.12.2.5), C# (12.6.4.5) and Kotlin (11.6) alike. */
static bool oo_arguments_exact(oo_walk_t *w, const oo_callable_t *callable, const oo_subst_t *subst,
                               const oo_value_t *args, int argc) {
    if (argc > callable->param_count || callable->max_args < 0 || !callable->params)
        return false;
    oo_scope_t scope = oo_scope_of_callable(callable);
    for (int i = 0; i < argc; i++) {
        const oo_value_t *argument = &args[i];
        int index = oo_parameter_of(callable, args, i);
        if (index < 0 || !callable->params[index] ||
            oo_ref_is_tparam(callable, callable->params[index]))
            return false;
        oo_value_t parameter = oo_value_from_ref(w, callable->params[index], &scope, subst);
        bool same = argument->type
                        ? argument->type == parameter.type && argument->dims == parameter.dims
                        : oo_same_external(argument, &parameter);
        if (!same)
            return false;
    }
    return true;
}

/* Which arguments have their own type as this declaration's parameter, one bit each. */
static uint32_t oo_identical_arguments(oo_walk_t *w, const oo_callable_t *callable,
                                       const oo_subst_t *subst, const oo_value_t *args, int argc) {
    enum { MASK_BITS = 32 };
    uint32_t mask = 0;
    if (!callable->params)
        return 0;
    oo_scope_t scope = oo_scope_of_callable(callable);
    for (int i = 0; i < argc && i < callable->param_count && i < MASK_BITS; i++) {
        int index = oo_parameter_of(callable, args, i);
        const oo_typeref_t *ref = index < 0 ? NULL : callable->params[index];
        if (!ref || (callable->max_args < 0 && index == callable->param_count - 1))
            continue;
        /* C# infers T as the argument's own type: its conversion is an identity. */
        if (oo_ref_is_tparam(callable, ref)) {
            if (oo_value_known(&args[i]) && callable->file &&
                callable->file->language == CBM_LANG_CSHARP)
                mask |= 1u << i;
            continue;
        }
        oo_value_t parameter = oo_value_from_ref(w, ref, &scope, subst);
        if (args[i].type ? args[i].type == parameter.type && args[i].dims == parameter.dims
                         : oo_same_external(&args[i], &parameter))
            mask |= 1u << i;
    }
    return mask;
}

typedef struct {
    const oo_callable_t *callable;
    oo_subst_t *subst;
} oo_candidate_t;

static bool oo_same_graph_node(const oo_callable_t *a, const oo_callable_t *b) {
    return a->graph_qn && b->graph_qn && !strcmp(a->graph_qn, b->graph_qn);
}

/* Every argument certainly reaches this declaration's parameter: known, and of its type, a subtype,
 * a scalar it widens to, or an array of a span's element. Not excluded is not the same thing: an
 * argument from outside the project could still fail to convert to a project's parameter. */
static bool oo_arguments_certainly_fit(oo_walk_t *w, const oo_callable_t *callable,
                                       const oo_subst_t *subst, const oo_value_t *args, int argc) {
    static const char *const SPANS[] = {"Span`1",           "ReadOnlySpan`1", "Memory`1",
                                        "ReadOnlyMemory`1", "Span",           "ReadOnlySpan",
                                        "Memory",           "ReadOnlyMemory", NULL};
    static const char *const ROOTS[] = {"object", "Object", NULL};
    if (argc > callable->param_count || !callable->params || callable->max_args < 0)
        return false;
    oo_scope_t scope = oo_scope_of_callable(callable);
    for (int i = 0; i < argc; i++) {
        const oo_value_t *argument = &args[i];
        int index = oo_parameter_of(callable, args, i);
        const oo_typeref_t *ref = index < 0 ? NULL : callable->params[index];
        if (!ref)
            return false;
        if (argument->function_literal || argument->default_literal ||
            oo_ref_is_tparam(callable, ref))
            continue;
        oo_value_t parameter = oo_value_from_ref(w, ref, &scope, subst);
        const char *to = parameter.type ? parameter.type->name : oo_leaf(parameter.external);
        if (!to)
            return false;
        if (argument->null_literal) {
            if (oo_scalar_of(to) != OO_SCALAR_NONE && oo_scalar_of(to) != OO_SCALAR_STRING)
                return false;
            continue;
        }
        if (!oo_value_known(argument))
            return false;
        if (oo_name_in(to, ROOTS) && !parameter.dims)
            continue;
        bool fits = false;
        if (argument->type && parameter.type)
            fits = argument->dims == parameter.dims &&
                   oo_is_subtype(w->ix, argument->type, parameter.type);
        else if (argument->external && parameter.external && argument->dims == parameter.dims)
            fits = !strcmp(oo_leaf(argument->external), to) ||
                   (oo_scalar_of(to) != OO_SCALAR_NONE &&
                    oo_scalar_of(oo_leaf(argument->external)) != OO_SCALAR_NONE);
        /* An array reaches a span; a span of another element is excluded already. */
        if (!fits && argument->dims == 1 && oo_name_in(to, SPANS))
            fits = true;
        if (!fits)
            return false;
    }
    return true;
}

/* An identical parameter is a better conversion than any other. A candidate with every identity
 * any rival has, and one more, cannot lose an argument to it, and each rival loses one to it. The
 * call compiles, so it has a best member, and no rival can be that. */
static int oo_best_by_identity(oo_walk_t *w, const oo_candidate_t *candidates, int count,
                               const oo_value_t *args, int argc) {
    uint32_t masks[OO_MAX_CANDIDATES];
    for (int i = 0; i < count; i++)
        masks[i] =
            oo_identical_arguments(w, candidates[i].callable, candidates[i].subst, args, argc);
    for (int i = 0; i < count; i++) {
        bool beats_all =
            masks[i] != 0 &&
            oo_arguments_certainly_fit(w, candidates[i].callable, candidates[i].subst, args, argc);
        for (int j = 0; j < count && beats_all; j++)
            if (i != j && !oo_same_graph_node(candidates[i].callable, candidates[j].callable))
                beats_all = !(masks[j] & ~masks[i]) && masks[j] != masks[i];
        if (beats_all)
            return i;
    }
    return -1;
}

/* C# 12.8.10.2: a method declared in a base type leaves the set when an applicable one in a more
 * derived type stands. */
static int oo_keep_most_derived(oo_walk_t *w, oo_candidate_t *candidates, int count,
                                const oo_value_t *args, int argc) {
    bool removed[OO_MAX_CANDIDATES] = {false};
    for (int j = 0; j < count; j++) {
        if (!oo_arguments_certainly_fit(w, candidates[j].callable, candidates[j].subst, args, argc))
            continue;
        for (int i = 0; i < count; i++)
            removed[i] =
                removed[i] || (candidates[i].callable->owner != candidates[j].callable->owner &&
                               oo_is_subtype(w->ix, candidates[j].callable->owner,
                                             candidates[i].callable->owner));
    }
    int kept = 0;
    for (int i = 0; i < count; i++)
        if (!removed[i])
            candidates[kept++] = candidates[i];
    return kept;
}

static oo_select_t oo_select_finish(oo_walk_t *w, oo_candidate_t *candidates, int count, int argc,
                                    const oo_value_t *args) {
    oo_select_t out = {.status = OO_SEL_UNIQUE};
    for (int i = 1; i < count; i++) {
        if (!candidates[0].callable->graph_qn || !candidates[i].callable->graph_qn ||
            strcmp(candidates[0].callable->graph_qn, candidates[i].callable->graph_qn)) {
            out.status = OO_SEL_AMBIGUOUS;
            break;
        }
    }
    /* Even a single candidate must fit: a project namesake of the wrong signature
     * is not the target of a call whose real target lies outside the project. */
    if (args && argc >= 0) {
        int kept = 0;
        for (int i = 0; i < count; i++)
            if (!oo_argument_excludes(w, candidates[i].callable, candidates[i].subst, args, argc))
                candidates[kept++] = candidates[i];
        count = kept;
        if (!count)
            return (oo_select_t){.status = OO_SEL_MISMATCH};
        out.status = OO_SEL_UNIQUE;
        for (int i = 1; i < count; i++) {
            if (!candidates[0].callable->graph_qn || !candidates[i].callable->graph_qn ||
                strcmp(candidates[0].callable->graph_qn, candidates[i].callable->graph_qn)) {
                out.status = OO_SEL_AMBIGUOUS;
                break;
            }
        }
    }
    bool csharp =
        candidates[0].callable->file && candidates[0].callable->file->language == CBM_LANG_CSHARP;
    if (out.status == OO_SEL_AMBIGUOUS && args && argc >= 0 && csharp) {
        count = oo_keep_most_derived(w, candidates, count, args, argc);
        out.status = OO_SEL_UNIQUE;
        for (int i = 1; i < count; i++)
            if (!oo_same_graph_node(candidates[0].callable, candidates[i].callable))
                out.status = OO_SEL_AMBIGUOUS;
    }
    if (out.status == OO_SEL_AMBIGUOUS && args && argc > 0) {
        int exact = -1, matches = 0;
        for (int i = 0; i < count; i++) {
            if (oo_arguments_exact(w, candidates[i].callable, candidates[i].subst, args, argc)) {
                exact = i;
                matches++;
            }
        }
        bool one_node = matches > 1;
        for (int i = 0; i < count && one_node; i++)
            if (oo_arguments_exact(w, candidates[i].callable, candidates[i].subst, args, argc) &&
                (!candidates[i].callable->graph_qn || !candidates[exact].callable->graph_qn ||
                 strcmp(candidates[i].callable->graph_qn, candidates[exact].callable->graph_qn)))
                one_node = false;
        if (matches == 1 || one_node) {
            candidates[0] = candidates[exact];
            count = 1;
            out.status = OO_SEL_UNIQUE;
        }
    }
    if (out.status == OO_SEL_AMBIGUOUS && args && argc > 0) {
        int best = oo_best_by_identity(w, candidates, count, args, argc);
        if (best >= 0) {
            candidates[0] = candidates[best];
            count = 1;
            out.status = OO_SEL_UNIQUE;
        }
    }
    if (out.status == OO_SEL_UNIQUE) {
        out.callable = candidates[0].callable;
        out.subst = candidates[0].subst;
        out.overload_set = count > 1;
    }
    return out;
}

static oo_select_t oo_select_in_hierarchy(oo_walk_t *w, const oo_value_t *receiver,
                                          const char *name, int argc, const oo_value_t *args,
                                          bool static_only, oo_extension_accepts_fn accepts,
                                          const oo_value_t *extended) {
    if (!receiver || !receiver->type || !name || receiver->dims)
        return (oo_select_t){.status = OO_SEL_NONE};
    oo_hierarchy_t walk, *h = &walk;
    h->head = h->tail = 0;
    h->w = w;
    h->truncated = false;
    oo_hierarchy_add(h, *receiver);
    oo_candidate_t candidates[OO_MAX_CANDIDATES];
    int count = 0;
    bool seen = false;
    while (h->head < h->tail) {
        oo_step_t step = h->steps[h->head++];
        /* C# 12.5.1: the base types of a class or struct are its base classes. An interface's
         * members are not among its members. */
        if ((step.type->flags & OO_F_INTERFACE) && !(receiver->type->flags & OO_F_INTERFACE) &&
            step.type->file && step.type->file->language == CBM_LANG_CSHARP)
            continue;
        for (const oo_callable_t *c = oo_lookup_methods(w->ix, step.type->qn, name); c;
             c = c->next) {
            if (c->flags & OO_F_CTOR)
                continue;
            /* A member extension is applicable only to its declared receiver. */
            if (accepts ? !c->receiver || !accepts(w, c, extended) : c->receiver != NULL)
                continue;
            seen = true;
            if ((c->flags & OO_F_PRIVATE) && !oo_private_visible(w, step.type))
                continue;
            if (static_only && !(c->flags & OO_F_STATIC))
                continue;
            if (!oo_arity_fits(c, argc))
                continue;
            bool overridden = false;
            for (int i = 0; i < count && !overridden; i++)
                overridden = candidates[i].callable->owner != c->owner &&
                             oo_overrides(candidates[i].callable, c);
            if (overridden)
                continue;
            if (count == OO_MAX_CANDIDATES)
                return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
            candidates[count++] = (oo_candidate_t){.callable = c, .subst = step.subst};
        }
        oo_hierarchy_expand(h, &step);
    }
    if (count)
        return oo_select_finish(w, candidates, count, argc, args);
    if (seen)
        return (oo_select_t){.status = OO_SEL_MISMATCH};
    return (oo_select_t){.status = !h->truncated && oo_hierarchy_complete(w->ix, receiver->type)
                                       ? OO_SEL_NONE
                                       : OO_SEL_INCOMPLETE};
}

oo_select_t oo_select_method(oo_walk_t *w, const oo_value_t *receiver, const char *name, int argc,
                             const oo_value_t *args, bool static_only) {
    return oo_select_in_hierarchy(w, receiver, name, argc, args, static_only, NULL, NULL);
}

oo_select_t oo_select_member_extension(oo_walk_t *w, const oo_value_t *dispatch,
                                       const oo_value_t *extended, const char *name, int argc,
                                       const oo_value_t *args, oo_extension_accepts_fn accepts) {
    return oo_select_in_hierarchy(w, dispatch, name, argc, args, false, accepts, extended);
}

oo_select_t oo_select_constructor(oo_walk_t *w, const oo_value_t *receiver, int argc,
                                  const oo_value_t *args) {
    if (!receiver || !receiver->type)
        return (oo_select_t){.status = OO_SEL_NONE};
    oo_candidate_t candidates[OO_MAX_CANDIDATES];
    int count = 0;
    bool seen = false;
    oo_subst_t *subst = oo_subst_of(w, receiver);
    /* Constructors are not inherited; only the class itself is searched. */
    for (const oo_callable_t *c =
             oo_lookup_methods(w->ix, receiver->type->qn, receiver->type->name);
         c; c = c->next) {
        if (!(c->flags & OO_F_CTOR))
            continue;
        seen = true;
        if (!oo_arity_fits(c, argc))
            continue;
        if (count == OO_MAX_CANDIDATES)
            return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
        candidates[count++] = (oo_candidate_t){.callable = c, .subst = subst};
    }
    if (count)
        return oo_select_finish(w, candidates, count, argc, args);
    return (oo_select_t){.status = seen ? OO_SEL_MISMATCH : OO_SEL_NONE};
}

bool oo_has_method_named(oo_walk_t *w, const oo_value_t *receiver, const char *name) {
    if (!receiver || !receiver->type || !name)
        return false;
    const oo_type_t *queue[OO_MAX_HIERARCHY];
    int head = 0, tail = 0;
    queue[tail++] = receiver->type;
    while (head < tail) {
        const oo_type_t *type = queue[head++];
        for (const oo_callable_t *c = oo_lookup_methods(w->ix, type->qn, name); c; c = c->next) {
            if ((c->flags & OO_F_CTOR) || c->receiver)
                continue;
            if ((c->flags & OO_F_PRIVATE) && !oo_private_visible(w, type))
                continue;
            return true;
        }
        for (int i = 0; i < type->super_count && tail < OO_MAX_HIERARCHY; i++) {
            const oo_type_t *next = type->supers[i].resolved;
            if (!next)
                continue;
            bool seen = false;
            for (int j = 0; j < tail && !seen; j++)
                seen = queue[j] == next;
            if (!seen)
                queue[tail++] = next;
        }
    }
    return false;
}

const oo_field_t *oo_find_field(oo_walk_t *w, const oo_value_t *receiver, const char *name,
                                oo_subst_t **subst, bool *incomplete) {
    if (incomplete)
        *incomplete = false;
    if (subst)
        *subst = NULL;
    if (!receiver || !receiver->type || !name || receiver->dims)
        return NULL;
    oo_hierarchy_t walk, *h = &walk;
    h->head = h->tail = 0;
    h->w = w;
    h->truncated = false;
    oo_hierarchy_add(h, *receiver);
    while (h->head < h->tail) {
        oo_step_t step = h->steps[h->head++];
        const oo_field_t *found = NULL;
        bool conflict = false;
        for (const oo_field_t *f = oo_lookup_fields(w->ix, step.type->qn, name); f; f = f->next) {
            if ((f->flags & OO_F_PRIVATE) && !oo_private_visible(w, step.type))
                continue;
            if (found && (!found->graph_qn || !f->graph_qn || strcmp(found->graph_qn, f->graph_qn)))
                conflict = true;
            found = f;
        }
        if (conflict) {
            if (incomplete)
                *incomplete = true;
            return NULL;
        }
        if (found) {
            if (subst)
                *subst = step.subst;
            return found;
        }
        oo_hierarchy_expand(h, &step);
    }
    if (incomplete)
        *incomplete = h->truncated || !oo_hierarchy_complete(w->ix, receiver->type);
    return NULL;
}

/* JLS 9.8: an interface with one abstract method, its own or inherited, counting a redeclaration
 * once and leaving out one a subinterface implemented. An external supertype cannot add a second:
 * the lambda would not compile. The walk is bounded by OO_MAX_HIERARCHY. */
int oo_functional_parameters(oo_walk_t *w, const oo_value_t *function, oo_value_t *out,
                             int capacity) {
    if (!function || !function->type || function->dims ||
        !(function->type->flags & (OO_F_INTERFACE | OO_F_DELEGATE)))
        return -1;
    oo_hierarchy_t walk, *h = &walk;
    h->head = h->tail = 0;
    h->w = w;
    h->truncated = false;
    oo_hierarchy_add(h, *function);
    const oo_callable_t *only = NULL;
    oo_subst_t *only_subst = NULL;
    while (h->head < h->tail) {
        oo_step_t step = h->steps[h->head++];
        if (step.type->abstract_count > 1)
            return -1;
        const oo_callable_t *declared = step.type->abstract_method;
        if (declared) {
            bool implemented = false;
            for (int i = 0; i + 1 < h->head && !implemented; i++)
                for (const oo_callable_t *m =
                         oo_lookup_methods(w->ix, h->steps[i].type->qn, declared->name);
                     m && !implemented; m = m->next)
                    implemented = m != h->steps[i].type->abstract_method;
            if (!implemented && only && strcmp(only->name, declared->name))
                return -1;
            if (!implemented && !only) {
                only = declared;
                only_subst = step.subst;
            }
        }
        oo_hierarchy_expand(h, &step);
    }
    if (!only || h->truncated || only->param_count > capacity ||
        (only->param_count && !only->params))
        return -1;
    oo_scope_t scope = oo_scope_of_callable(only);
    for (int i = 0; i < only->param_count; i++) {
        oo_value_t value = oo_value_from_ref(w, only->params[i], &scope, only_subst);
        out[i] = !oo_value_known(&value) && value.lower ? *value.lower : value;
    }
    return only->param_count;
}

oo_select_t oo_select_from_list(oo_walk_t *w, const oo_callable_t *list, int argc,
                                const oo_value_t *args, const oo_value_t *receiver) {
    oo_candidate_t candidates[OO_MAX_CANDIDATES];
    int count = 0;
    bool seen = false;
    for (const oo_callable_t *c = list; c; c = c->next) {
        if (!oo_callable_visible(w, c))
            continue;
        /* A free name lookup sees only plain functions; extension applicability
         * is checked by the caller against the receiver's hierarchy. */
        if (!receiver && c->receiver)
            continue;
        seen = true;
        if (!oo_arity_fits(c, argc))
            continue;
        if (count == OO_MAX_CANDIDATES)
            return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
        candidates[count++] = (oo_candidate_t){.callable = c};
    }
    if (count)
        return oo_select_finish(w, candidates, count, argc, args);
    return (oo_select_t){.status = seen ? OO_SEL_MISMATCH : OO_SEL_NONE};
}

/* What one parameter says about one type parameter, given the value passed to it. */
static oo_value_t oo_tparam_from(oo_walk_t *w, const oo_typeref_t *ref, const char *tparam,
                                 const oo_value_t *value) {
    if (!ref || !ref->name || !value)
        return oo_value_unknown();
    if (!strcmp(ref->name, tparam)) {
        if (ref->dims > value->dims)
            return oo_value_unknown();
        oo_value_t element = *value;
        element.dims -= ref->dims;
        element.classref = false;
        return element;
    }
    if (!ref->args || ref->dims)
        return oo_value_unknown();
    int count = 0;
    while (ref->args[count])
        count++;
    for (int k = 0; k < count; k++) {
        const oo_typeref_t *arg = ref->args[k];
        const oo_typeref_t *written = arg && arg->lower ? arg->lower : arg;
        if (!written || !written->name || strcmp(written->name, tparam))
            continue;
        /* The same generic type: positions match. Anything else only through a known element. */
        const char *declared = oo_leaf(ref->name);
        const char *actual = value->type ? value->type->name : oo_leaf(value->external);
        size_t stem = strcspn(declared, "`");
        if (actual && value->arg_count == count && !strncmp(declared, actual, stem) &&
            (actual[stem] == 0 || actual[stem] == 0x60))
            return value->args[k];
        if (count == 1)
            return oo_element_of(w, value);
    }
    return oo_value_unknown();
}

static bool oo_same_value(const oo_value_t *a, const oo_value_t *b) {
    return a->dims == b->dims && a->type == b->type &&
           (a->type || (a->external && b->external && !strcmp(a->external, b->external)));
}

bool oo_ref_unsettled(const oo_callable_t *callee, const oo_typeref_t *ref,
                      const oo_subst_t *subst) {
    if (!callee || !ref || !ref->name)
        return false;
    for (int i = 0; i < callee->tparam_count; i++) {
        if (strcmp(callee->tparams[i].name, ref->name))
            continue;
        const oo_value_t *bound = oo_subst_lookup(subst, &callee->tparams[i]);
        return !bound || !oo_value_known(bound);
    }
    return false;
}

const oo_subst_t *oo_infer_call_subst(oo_walk_t *w, const oo_callable_t *callee,
                                      const oo_subst_t *subst, const oo_value_t *receiver,
                                      const oo_value_t *args, int argc) {
    if (!callee || !callee->tparam_count)
        return subst;
    oo_value_t *values = cbm_arena_calloc(w->arena, (size_t)callee->tparam_count * sizeof(*values));
    oo_subst_t *inferred = values ? cbm_arena_alloc(w->arena, sizeof(*inferred)) : NULL;
    if (!inferred)
        return subst;
    bool any = false;
    for (int t = 0; t < callee->tparam_count; t++) {
        const char *name = callee->tparams[t].name;
        bool conflict = false;
        oo_value_t found = oo_value_unknown();
        for (int i = -1; i < argc && i < callee->param_count && !conflict; i++) {
            int index = i < 0 ? i : oo_parameter_of(callee, args, i);
            if (i >= 0 && index < 0)
                continue;
            const oo_typeref_t *ref = i < 0            ? callee->receiver
                                      : callee->params ? callee->params[index]
                                                       : NULL;
            const oo_value_t *value = i < 0 ? receiver : &args[i];
            oo_value_t one = oo_tparam_from(w, ref, name, value);
            if (!oo_value_known(&one))
                continue;
            conflict = oo_value_known(&found) && !oo_same_value(&found, &one);
            found = one;
        }
        if (!conflict && oo_value_known(&found)) {
            values[t] = found;
            any = true;
        }
    }
    if (!any)
        return subst;
    *inferred = (oo_subst_t){.tparams = callee->tparams,
                             .count = callee->tparam_count,
                             .args = values,
                             .arg_count = callee->tparam_count,
                             .next = (oo_subst_t *)subst};
    return inferred;
}

oo_value_t oo_kotlin_eval_summary(oo_walk_t *w, const oo_exprsum_t *summary,
                                  const oo_scope_t *scope, const oo_subst_t *subst);

oo_value_t oo_callable_return(oo_walk_t *w, const oo_callable_t *callable,
                              const oo_subst_t *subst) {
    if (!callable)
        return oo_value_unknown();
    oo_scope_t scope = oo_scope_of_callable(callable);
    if (callable->ret)
        return oo_value_from_ref(w, callable->ret, &scope, subst);
    if (callable->ret_expr)
        return oo_kotlin_eval_summary(w, callable->ret_expr, &scope, subst);
    return oo_value_unknown();
}

/* ── Seal ───────────────────────────────────────────────────────────── */

static void oo_resolve_supers(cbm_oo_index_t *ix) {
    /* Two passes: a super may name a type nested in another type's supertype,
     * which is visible only once that type's own supers are resolved. */
    for (int pass = 0; pass < 2; pass++) {
        for (oo_type_t *type = ix->all_types; type; type = type->next_in_index) {
            bool complete = true;
            for (int i = 0; i < type->super_count; i++) {
                oo_super_t *super = &type->supers[i];
                if (super->resolved)
                    continue;
                /* A supertype spelling never sees the type's own nested or
                 * parameter names through itself as a supertype. The owner is
                 * the lexical context of the declaration. */
                oo_scope_t scope = {.file = super->file, .ns = super->ns, .owner = type->owner};
                oo_name_t name = oo_resolve_name(ix, &scope, super->ref->name);
                if (name.status == OO_NAME_TYPE) {
                    const oo_type_t *target = oo_unalias(ix, name.type);
                    if (target && target != type) {
                        super->resolved = target;
                        continue;
                    }
                }
                super->external = name.status == OO_NAME_EXTERNAL;
                if (!super->external || !oo_closed_super(super->ref->name))
                    complete = false;
            }
            type->supers_complete = complete;
        }
    }
}

void cbm_oo_index_seal(cbm_oo_index_t *ix) {
    if (!ix || ix->sealed)
        return;
    oo_resolve_supers(ix);
    ix->sealed = true;
}

/* ── Walk ───────────────────────────────────────────────────────────── */

static void oo_carrier_add(oo_walk_t *w, CBMHashTable *table, uint32_t start, uint32_t end,
                           const char *caller, const char *spelling, bool may_reference) {
    if (end <= start || !caller || !spelling)
        return;
    char buffer[32];
    int length = snprintf(buffer, sizeof(buffer), "%u:%u", start, end);
    if (length <= 0 || (size_t)length >= sizeof(buffer))
        return;
    oo_carrier_t *carrier = cbm_arena_alloc(w->arena, sizeof(*carrier));
    if (!carrier)
        return;
    oo_carrier_t *prior = cbm_ht_get(table, buffer);
    *carrier = (oo_carrier_t){.caller = caller,
                              .spelling = spelling,
                              .next = prior,
                              .may_be_call_reference = may_reference};
    const char *key = prior ? cbm_ht_get_key(table, buffer) : cbm_arena_strdup(w->arena, buffer);
    if (key)
        cbm_ht_set(table, key, carrier);
}

bool oo_walk_init(oo_walk_t *w, const cbm_oo_index_t *ix, CBMArena *arena, const char *source,
                  int source_len, const CBMFileResult *file, CBMResolvedCallArray *out) {
    memset(w, 0, sizeof(*w));
    w->ix = ix;
    w->arena = arena;
    w->source = source;
    w->length = (size_t)source_len;
    w->file = file;
    w->out = out;
    cbm_resolve_types_init(&w->types, arena, 0);
    cbm_resolve_proof_pool_init_project(&w->types.proofs, arena);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, file->module_qn);
    if (!cbm_lsp_proof_context(&w->types, &proof_source, file->module_qn))
        return false;
    w->calls = cbm_ht_create((uint32_t)file->calls.count + 8);
    w->usages = cbm_ht_create((uint32_t)file->usages.count + 8);
    w->rws = cbm_ht_create((uint32_t)file->rw.count + 8);
    w->type_refs = cbm_ht_create((uint32_t)file->type_refs.count + 8);
    if (!w->calls || !w->usages || !w->rws || !w->type_refs)
        return false;
    for (int i = 0; i < file->calls.count; i++) {
        const CBMCall *call = &file->calls.items[i];
        if (call->source_origin == CBM_SOURCE_ORIGIN_RAW)
            oo_carrier_add(w, w->calls, call->site_start_byte, call->site_end_byte,
                           call->enclosing_func_qn, call->callee_name, false);
    }
    for (int i = 0; i < file->usages.count; i++) {
        const CBMUsage *usage = &file->usages.items[i];
        if (usage->source_origin == CBM_SOURCE_ORIGIN_RAW)
            oo_carrier_add(w, w->usages, usage->site_start_byte, usage->site_end_byte,
                           usage->enclosing_func_qn, usage->ref_name, usage->may_be_call_reference);
    }
    for (int i = 0; i < file->rw.count; i++) {
        const CBMReadWrite *rw = &file->rw.items[i];
        if (rw->source_origin == CBM_SOURCE_ORIGIN_RAW)
            oo_carrier_add(w, w->rws, rw->site_start_byte, rw->site_end_byte, rw->enclosing_func_qn,
                           rw->var_name, rw->is_write);
    }
    for (int i = 0; i < file->type_refs.count; i++) {
        const CBMTypeRef *type = &file->type_refs.items[i];
        if (type->site_is_exact && type->source_origin == CBM_SOURCE_ORIGIN_RAW)
            oo_carrier_add(w, w->type_refs, type->site_start_byte, type->site_end_byte,
                           type->enclosing_func_qn, type->type_name, false);
    }
    return true;
}

void oo_walk_dispose(oo_walk_t *w) {
    cbm_ht_free(w->calls);
    cbm_ht_free(w->usages);
    cbm_ht_free(w->rws);
    cbm_ht_free(w->type_refs);
    w->calls = w->usages = w->rws = w->type_refs = NULL;
}

bool oo_budget(oo_walk_t *w) {
    /* Each source node is visited a bounded number of times. Exhaustion
     * discards the file's staged rows; it never publishes a partial walk. */
    if (++w->visits > 64u * 1024u * 1024u)
        w->failed = true;
    return !w->failed;
}

void oo_frame_push(oo_walk_t *w, oo_frame_t *frame) {
    frame->locals = NULL;
    frame->parent = w->frame;
    w->frame = frame;
}

void oo_frame_pop(oo_walk_t *w) {
    if (w->frame)
        w->frame = w->frame->parent;
}

void oo_bind(oo_walk_t *w, const char *name, oo_value_t value) {
    if (!name || !w->frame)
        return;
    oo_local_t *local = cbm_arena_alloc(w->arena, sizeof(*local));
    if (!local) {
        w->failed = true;
        return;
    }
    *local = (oo_local_t){.name = name, .value = value, .next = w->frame->locals};
    w->frame->locals = local;
}

const oo_local_t *oo_lookup_local(const oo_walk_t *w, const char *name) {
    if (!name)
        return NULL;
    for (const oo_frame_t *frame = w->frame; frame; frame = frame->parent)
        for (const oo_local_t *local = frame->locals; local; local = local->next)
            if (!strcmp(local->name, name))
                return local;
    return NULL;
}

const char *oo_text(const oo_walk_t *w, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= w->length
               ? cbm_arena_strndup(w->arena, w->source + start, end - start)
               : NULL;
}

oo_receiver_t *oo_receiver_push(oo_walk_t *w, oo_value_t value, bool static_context) {
    oo_receiver_t *receiver = cbm_arena_calloc(w->arena, sizeof(*receiver));
    if (!receiver) {
        w->failed = true;
        return NULL;
    }
    receiver->value = value;
    receiver->static_context = static_context;
    receiver->parent = w->receiver;
    w->receiver = receiver;
    return receiver;
}

void oo_receiver_pop(oo_walk_t *w) {
    if (w->receiver)
        w->receiver = w->receiver->parent;
}

/* ── Rows ───────────────────────────────────────────────────────────── */

static const oo_carrier_t *oo_carrier_at(const CBMHashTable *table, TSNode site) {
    char buffer[32];
    int length =
        snprintf(buffer, sizeof(buffer), "%u:%u", ts_node_start_byte(site), ts_node_end_byte(site));
    return length > 0 && (size_t)length < sizeof(buffer) ? cbm_ht_get(table, buffer) : NULL;
}

static void oo_emit_row(oo_walk_t *w, const oo_carrier_t *carrier, TSNode site, const char *target,
                        CBMResolvedKind kind, const char *relation, cbm_resolve_rule_t rule,
                        uint32_t extra_facts, const char *decision) {
    uint32_t required = cbm_resolve_rule_required_facts(rule);
    if (!required || !target || !target[0] || !carrier) {
        return;
    }
    uint32_t start = ts_node_start_byte(site), end = ts_node_end_byte(site);
    cbm_resolve_proof_pool_t *pool = &w->types.proofs;
    cbm_resolve_proof_source_t source = w->types.proof_source;
    const char *scope = carrier->caller;
    if (w->dir_module_scope && w->file_qn && !strcmp(scope, w->dir_module_scope))
        scope = w->file_qn;
    source.scope = scope;
    cbm_resolve_proof_key_t key = {.lookup_domain = scope,
                                   .lookup_key = carrier->spelling,
                                   .binding_identity = target,
                                   .start_byte = start,
                                   .end_byte = end,
                                   .origin = source.source_origin,
                                   .detail = decision};
    cbm_resolve_result_t result = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                   .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                   .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                   .rule = rule,
                                   .facts = required | extra_facts,
                                   .target = target,
                                   .candidate_count = 1,
                                   .reason = decision};
    /* The language rule's decision, as one leaf: a project holds millions. */
    const cbm_resolve_proof_t *semantic =
        cbm_resolve_proof_leaf(pool, CBM_PROOF_ADAPTER_DOMAIN, &source, &key, &result);
    if (!semantic)
        return;
    /* The exact AST role at this site, established where the walker stands. */
    cbm_resolve_proof_key_t role = {.lookup_domain = scope,
                                    .lookup_key = carrier->spelling,
                                    .binding_identity = target,
                                    .start_byte = start,
                                    .end_byte = end,
                                    .origin = source.source_origin,
                                    .detail = relation};
    cbm_resolve_result_t audited = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                    .facts = CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                    .reason = "audited_ast_occurrence_role"};
    const cbm_resolve_proof_t *premises[] = {
        semantic, cbm_resolve_proof_leaf(pool, CBM_PROOF_SOURCE_FACT, &source, &role, &audited)};
    result.facts |= CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    result.proof = cbm_resolve_proof_derive(pool, CBM_PROOF_NAMED_JOIN, &source, &role, &result,
                                            premises, 2, true);
    if (!result.proof || result.proof->value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return;
    CBMResolvedCall row = {.caller_qn = carrier->caller,
                           .callee_qn = target,
                           .strategy = "oo_static_selection",
                           .confidence = 1.0f,
                           .kind = kind,
                           .site_start_byte = start,
                           .site_end_byte = end,
                           .source_origin = CBM_SOURCE_ORIGIN_RAW,
                           .evidence = {.analysis = result.analysis,
                                        .rule = result.rule,
                                        .facts = result.facts,
                                        .proof = result.proof}};
    cbm_resolvedcall_push(w->out, w->arena, row);
}

void oo_emit_call(oo_walk_t *w, TSNode site, const char *target, cbm_resolve_rule_t rule,
                  uint32_t extra_facts, const char *decision) {
    for (const oo_carrier_t *c = oo_carrier_at(w->calls, site); c; c = c->next)
        oo_emit_row(w, c, site, target, CBM_RESOLVED_INVOCATION, "CALLS", rule, extra_facts,
                    decision);
}

void oo_emit_usage(oo_walk_t *w, TSNode site, const char *target, CBMResolvedKind kind,
                   cbm_resolve_rule_t rule, const char *decision) {
    const char *relation = kind == CBM_RESOLVED_CALL_REFERENCE   ? "CALL_REFERENCE"
                           : kind == CBM_RESOLVED_TYPE_REFERENCE ? "USES_TYPE"
                                                                 : "USAGE";
    for (const oo_carrier_t *c = oo_carrier_at(w->usages, site); c; c = c->next)
        oo_emit_row(w, c, site, target, kind, relation, rule, 0, decision);
}

void oo_emit_type_ref(oo_walk_t *w, TSNode site, const char *target, const char *decision) {
    for (const oo_carrier_t *c = oo_carrier_at(w->type_refs, site); c; c = c->next)
        oo_emit_row(w, c, site, target, CBM_RESOLVED_TYPE_REFERENCE, "USES_TYPE",
                    CBM_RESOLVE_RULE_DECLARED_TYPE, 0, decision);
}

void oo_emit_rw(oo_walk_t *w, TSNode site, const char *target, const char *decision) {
    for (const oo_carrier_t *c = oo_carrier_at(w->rws, site); c; c = c->next)
        oo_emit_row(w, c, site, target, CBM_RESOLVED_FIELD_REFERENCE,
                    c->may_be_call_reference ? "WRITES" : "READS", CBM_RESOLVE_RULE_TYPE_MEMBER, 0,
                    decision);
}

/* ── Entry points ───────────────────────────────────────────────────── */

static const TSLanguage *oo_ts_language(CBMLanguage language) {
    extern const TSLanguage *tree_sitter_java(void);
    extern const TSLanguage *tree_sitter_kotlin(void);
    extern const TSLanguage *tree_sitter_c_sharp(void);
    return language == CBM_LANG_JAVA     ? tree_sitter_java()
           : language == CBM_LANG_KOTLIN ? tree_sitter_kotlin()
                                         : tree_sitter_c_sharp();
}

static TSTree *oo_parse(CBMLanguage language, const char *source, int source_len) {
    TSParser *parser = ts_parser_new();
    if (!parser)
        return NULL;
    TSTree *tree = NULL;
    if (ts_parser_set_language(parser, oo_ts_language(language)))
        tree = cbm_parse_source_repairing(parser, language, source, (uint32_t)source_len,
                                          (TSParseOptions){0});
    ts_parser_delete(parser);
    return tree;
}

bool cbm_oo_index_add_file(cbm_oo_index_t *ix, CBMLanguage language, const char *rel_path,
                           const CBMFileResult *file, const char *source, int source_len) {
    if (!ix || ix->sealed || !file || !source || source_len <= 0 || !cbm_oo_language(language))
        return false;
    TSTree *owned = NULL;
    TSTree *tree = file->cached_tree && file->cached_lang == language ? file->cached_tree : NULL;
    if (!tree) {
        owned = oo_parse(language, source, source_len);
        tree = owned;
    }
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    bool ok = language == CBM_LANG_JAVA
                  ? oo_java_harvest(ix, rel_path, file, source, (size_t)source_len, root)
              : language == CBM_LANG_KOTLIN
                  ? oo_kotlin_harvest(ix, rel_path, file, source, (size_t)source_len, root)
                  : oo_csharp_harvest(ix, rel_path, file, source, (size_t)source_len, root);
    if (owned)
        ts_tree_delete(owned);
    return ok;
}

void cbm_oo_resolve_file(const cbm_oo_index_t *ix, CBMArena *arena, CBMLanguage language,
                         const char *rel_path, const char *file_qn, const char *source,
                         int source_len, TSTree *tree, const CBMFileResult *file,
                         CBMResolvedCallArray *out) {
    if (!ix || !ix->sealed || ix->failed || !arena || !source || source_len <= 0 || !file || !out ||
        !cbm_oo_language(language))
        return;
    TSTree *owned = NULL;
    if (!tree) {
        owned = oo_parse(language, source, source_len);
        tree = owned;
    }
    if (!tree)
        return;
    /* Staged: only a walk that finished within its budget publishes rows. */
    CBMResolvedCallArray staged = {0};
    oo_walk_t walk;
    if (oo_walk_init(&walk, ix, arena, source, source_len, file, &staged)) {
        walk.rel_path = rel_path ? rel_path : "";
        /* A directory-module language's class-level occurrence carries the
         * package's module QN; its source node is this file (#787). */
        walk.dir_module_scope =
            language == CBM_LANG_JAVA && file->module_qn ? file->module_qn : NULL;
        walk.file_qn = file_qn;
        TSNode root = ts_tree_root_node(tree);
        if (language == CBM_LANG_JAVA)
            oo_java_resolve(&walk, root);
        else if (language == CBM_LANG_KOTLIN)
            oo_kotlin_resolve(&walk, root);
        else
            oo_csharp_resolve(&walk, root);
        if (!walk.failed)
            for (int i = 0; i < staged.count; i++)
                cbm_resolvedcall_push(out, arena, staged.items[i]);
    }
    oo_walk_dispose(&walk);
    if (owned)
        ts_tree_delete(owned);
}
