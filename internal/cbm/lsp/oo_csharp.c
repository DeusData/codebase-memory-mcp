#include "oo_internal.h"
#include <stdio.h>
#include <string.h>

/* C# adapter. Names follow §7.8: each enclosing namespace's members, then the
 * using directives written in that namespace declaration, innermost first.
 * Member lookup follows §12.5: the innermost enclosing type with a member of
 * the name is the one searched, then `using static`. Extension methods apply
 * only after instance lookup found nothing applicable.
 *
 * `Foo` and `Foo<T>` are different types, so a generic type's identity carries
 * its arity. A partial class is one type with several graph nodes: its members
 * keep their own nodes, and the type itself has no single node to name. */

typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length;
} cs_text_t;

static bool cs_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}

static TSNode cs_field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

static TSNode cs_child(TSNode node, const char *kind) {
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (cs_is(child, kind))
            return child;
    }
    return (TSNode){0};
}

static const char *cs_text(const cs_text_t *t, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= t->length
               ? cbm_arena_strndup(t->arena, t->source + start, end - start)
               : NULL;
}

/* A verbatim identifier's @ is not part of its name. */
static const char *cs_name(const cs_text_t *t, TSNode node) {
    const char *text = cs_text(t, node);
    return text && text[0] == '@' ? text + 1 : text;
}

static const char *cs_join(CBMArena *arena, const char *prefix, const char *name) {
    return prefix && prefix[0] ? cbm_arena_sprintf(arena, "%s.%s", prefix, name)
                               : cbm_arena_strdup(arena, name);
}

static int cs_type_args_count(TSNode list) {
    return ts_node_is_null(list) ? 0 : (int)ts_node_named_child_count(list);
}

/* A declared or spelled type's identity spelling: arity after a backtick. */
static const char *cs_arity_name(CBMArena *arena, const char *name, int arity) {
    return arity > 0 ? cbm_arena_sprintf(arena, "%s`%d", name, arity) : name;
}

static const char *cs_type_spelling(const cs_text_t *t, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    if (cs_is(node, "identifier"))
        return cs_name(t, node);
    if (cs_is(node, "generic_name")) {
        TSNode list = cs_child(node, "type_argument_list");
        return cs_arity_name(t->arena, cs_name(t, cs_child(node, "identifier")),
                             cs_type_args_count(list));
    }
    if (cs_is(node, "qualified_name")) {
        const char *qualifier = cs_type_spelling(t, cs_field(node, "qualifier"));
        const char *name = cs_type_spelling(t, cs_field(node, "name"));
        return qualifier && name ? cbm_arena_sprintf(t->arena, "%s.%s", qualifier, name) : NULL;
    }
    if (cs_is(node, "alias_qualified_name")) {
        /* `global::X`: the namespace root; other aliases are not modelled. */
        const char *alias = cs_text(t, cs_field(node, "alias"));
        return alias && !strcmp(alias, "global") ? cs_type_spelling(t, cs_field(node, "name"))
                                                 : NULL;
    }
    if (cs_is(node, "predefined_type"))
        return cs_text(t, node);
    return NULL;
}

static const oo_typeref_t *cs_typeref(const cs_text_t *t, TSNode node);

static const oo_typeref_t **cs_type_args(const cs_text_t *t, TSNode list) {
    int count = cs_type_args_count(list);
    if (!count)
        return NULL;
    const oo_typeref_t **args = cbm_arena_calloc(t->arena, ((size_t)count + 1) * sizeof(*args));
    if (!args)
        return NULL;
    for (int i = 0; i < count; i++) {
        args[i] = cs_typeref(t, ts_node_named_child(list, (uint32_t)i));
        if (!args[i])
            args[i] = cbm_arena_calloc(t->arena, sizeof(oo_typeref_t));
    }
    return args;
}

static const oo_typeref_t *cs_typeref(const cs_text_t *t, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    oo_typeref_t *ref = cbm_arena_calloc(t->arena, sizeof(*ref));
    if (!ref)
        return NULL;
    if (cs_is(node, "nullable_type") || cs_is(node, "ref_type") || cs_is(node, "scoped_type")) {
        return cs_typeref(t, cs_field(node, "type"));
    }
    if (cs_is(node, "array_type")) {
        const oo_typeref_t *element = cs_typeref(t, cs_field(node, "type"));
        if (element)
            *ref = *element;
        ref->dims++;
        return ref;
    }
    ref->name = cs_type_spelling(t, node);
    /* The trailing generic segment's arguments. */
    TSNode last = cs_is(node, "qualified_name") ? cs_field(node, "name") : node;
    if (cs_is(last, "generic_name"))
        ref->args = cs_type_args(t, cs_child(last, "type_argument_list"));
    return ref;
}

static oo_tparam_t *cs_tparams(const cs_text_t *t, TSNode owner, int *out_count) {
    *out_count = 0;
    TSNode list = cs_field(owner, "type_parameters");
    if (ts_node_is_null(list))
        list = cs_child(owner, "type_parameter_list");
    uint32_t count = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
    if (!count)
        return NULL;
    oo_tparam_t *items = cbm_arena_calloc(t->arena, count * sizeof(*items));
    if (!items)
        return NULL;
    int n = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(list, i);
        if (!cs_is(parameter, "type_parameter"))
            continue;
        items[n].name = cs_name(t, cs_field(parameter, "name"));
        if (items[n].name)
            n++;
    }
    /* `where T : Base`: a class or interface constraint reads as the bound. */
    uint32_t children = ts_node_named_child_count(owner);
    for (uint32_t i = 0; i < children; i++) {
        TSNode clause = ts_node_named_child(owner, i);
        if (!cs_is(clause, "type_parameter_constraints_clause"))
            continue;
        const char *name = cs_name(t, ts_node_named_child(clause, 0));
        uint32_t parts = ts_node_named_child_count(clause);
        for (uint32_t p = 1; p < parts; p++) {
            TSNode constraint = ts_node_named_child(clause, p);
            if (!cs_is(constraint, "type_parameter_constraint"))
                continue;
            TSNode type = cs_field(constraint, "type");
            if (ts_node_is_null(type) && ts_node_named_child_count(constraint))
                type = ts_node_named_child(constraint, 0);
            for (int k = 0; name && !ts_node_is_null(type) && k < n; k++)
                if (!strcmp(items[k].name, name) && !items[k].bound)
                    items[k].bound = cs_typeref(t, type);
        }
    }
    *out_count = n;
    return items;
}

enum {
    CS_MOD_STATIC = 1,
    CS_MOD_PUBLIC = 2,
    CS_MOD_PRIVATE = 4,
    CS_MOD_PARTIAL = 8,
    CS_MOD_OVERRIDE = 16,
    CS_MOD_ACCESS = 32,
    CS_MOD_ABSTRACT = 64
};

static unsigned cs_modifiers(const cs_text_t *t, TSNode node) {
    unsigned flags = 0;
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!cs_is(child, "modifier"))
            continue;
        const char *text = cs_text(t, child);
        if (!text)
            continue;
        if (!strcmp(text, "static"))
            flags |= CS_MOD_STATIC;
        else if (!strcmp(text, "partial"))
            flags |= CS_MOD_PARTIAL;
        else if (!strcmp(text, "override"))
            flags |= CS_MOD_OVERRIDE;
        else if (!strcmp(text, "abstract"))
            flags |= CS_MOD_ABSTRACT;
        else if (!strcmp(text, "private"))
            flags |= CS_MOD_PRIVATE | CS_MOD_ACCESS;
        else if (!strcmp(text, "public"))
            flags |= CS_MOD_PUBLIC | CS_MOD_ACCESS;
        else if (!strcmp(text, "protected") || !strcmp(text, "internal"))
            flags |= CS_MOD_ACCESS;
    }
    return flags;
}

static uint32_t cs_member_flags(unsigned modifiers, const oo_type_t *owner) {
    uint32_t flags = 0;
    if (modifiers & CS_MOD_STATIC)
        flags |= OO_F_STATIC;
    if (modifiers & CS_MOD_OVERRIDE)
        flags |= OO_F_OVERRIDE;
    /* A class or struct member with no access modifier is private (§7.5.2). */
    if ((modifiers & CS_MOD_PRIVATE) ||
        (!(modifiers & CS_MOD_ACCESS) && owner && !(owner->flags & OO_F_INTERFACE)))
        flags |= OO_F_PRIVATE;
    return flags;
}

/* ── Harvest ────────────────────────────────────────────────────────── */

typedef struct {
    cbm_oo_index_t *ix;
    cs_text_t text;
    oo_defs_t defs;
    const oo_file_t *file;
} ch_t;

static uint32_t cs_line(TSNode node) {
    return ts_node_start_point(node).row + 1;
}

static void ch_parameters(const ch_t *h, oo_callable_t *callable, TSNode list, bool *extension) {
    uint32_t count = ts_node_is_null(list) ? 0 : ts_node_child_count(list);
    const oo_typeref_t **refs =
        count ? cbm_arena_calloc(h->text.arena, count * sizeof(*refs)) : NULL;
    int total = 0, optional = 0;
    bool variadic = false;
    if (extension)
        *extension = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(list, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "params")) {
            variadic = true;
        } else if (!strcmp(kind, "parameter")) {
            TSNode type = cs_field(child, "type");
            if (refs)
                refs[total] = cs_typeref(&h->text, type);
            /* A named child after the name is its default value. */
            TSNode name = cs_field(child, "name");
            uint32_t parts = ts_node_named_child_count(child);
            for (uint32_t p = 0; p < parts; p++) {
                TSNode part = ts_node_named_child(child, p);
                if (ts_node_start_byte(part) > ts_node_end_byte(name)) {
                    optional++;
                    break;
                }
            }
            if (total == 0 && extension) {
                for (uint32_t p = 0; p < parts; p++) {
                    TSNode part = ts_node_named_child(child, p);
                    const char *text = cs_is(part, "modifier") ? cs_text(&h->text, part) : NULL;
                    if (text && !strcmp(text, "this"))
                        *extension = true;
                }
            }
            total++;
        } else if (variadic && ts_node_is_named(child) && !strcmp(kind, "array_type") && refs) {
            /* `params T[] rest` sits directly in the list. */
            const oo_typeref_t *element = cs_typeref(&h->text, cs_field(child, "type"));
            refs[total++] = element;
        }
    }
    callable->params = refs;
    callable->param_count = total;
    callable->min_args = total - optional - (variadic ? 1 : 0);
    if (callable->min_args < 0)
        callable->min_args = 0;
    callable->max_args = variadic ? -1 : total;
}

static const char *ch_graph(const ch_t *h, TSNode node, const char *name) {
    const CBMDefinition *def = oo_defs_find(&h->defs, 'C', cs_line(node), name);
    if (!def)
        def = oo_defs_find(&h->defs, 'F', cs_line(node), name);
    return def ? def->qualified_name : NULL;
}

static void ch_method(ch_t *h, oo_type_t *type, TSNode node) {
    bool ctor = cs_is(node, "constructor_declaration");
    const char *spelled = cs_name(&h->text, cs_field(node, "name"));
    if (!spelled)
        return;
    oo_callable_t *callable = cbm_arena_calloc(h->text.arena, sizeof(*callable));
    if (!callable)
        return;
    unsigned modifiers = cs_modifiers(&h->text, node);
    /* A constructor is stored under the type's own identity spelling. */
    callable->name = ctor ? type->name : spelled;
    callable->graph_qn = ch_graph(h, node, spelled);
    callable->owner = type;
    callable->file = h->file;
    callable->ns = type->ns;
    callable->flags = cs_member_flags(modifiers, type) | (ctor ? OO_F_CTOR : 0);
    /* Own arity keeps a generic method's type parameters out of the type's. */
    callable->tparams = cs_tparams(&h->text, node, &callable->tparam_count);
    bool extension = false;
    ch_parameters(h, callable, cs_field(node, "parameters"), &extension);
    if (!ctor)
        callable->ret = cs_typeref(&h->text, cs_field(node, "returns"));
    oo_index_method(h->ix, type, callable);
    if (extension && callable->param_count > 0) {
        oo_callable_t reduced = *callable;
        reduced.receiver = callable->params ? callable->params[0] : NULL;
        reduced.params = callable->params ? callable->params + 1 : NULL;
        reduced.param_count = callable->param_count - 1;
        reduced.min_args = callable->min_args > 0 ? callable->min_args - 1 : 0;
        reduced.max_args = callable->max_args < 0 ? -1 : callable->max_args - 1;
        reduced.next = NULL;
        oo_index_ext_static(h->ix, type->ns, &reduced);
    }
}

static void ch_field(ch_t *h, oo_type_t *type, const char *name, TSNode node, TSNode type_node,
                     unsigned modifiers, uint32_t forced) {
    if (!name)
        return;
    oo_field_t *field = cbm_arena_calloc(h->text.arena, sizeof(*field));
    if (!field)
        return;
    *field = (oo_field_t){.name = name,
                          .graph_qn = ch_graph(h, node, name),
                          .owner = type,
                          .file = h->file,
                          .ns = type->ns,
                          .flags = cs_member_flags(modifiers, type) | forced,
                          .ref = cs_typeref(&h->text, type_node)};
    oo_index_field(h->ix, type, field);
}

static void ch_type(ch_t *h, TSNode node, oo_type_t *owner, const char *ns);

static void ch_members(ch_t *h, oo_type_t *type, TSNode body, const char *ns) {
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        const char *kind = ts_node_type(member);
        if (!strcmp(kind, "method_declaration") || !strcmp(kind, "constructor_declaration")) {
            ch_method(h, type, member);
        } else if (!strcmp(kind, "property_declaration")) {
            ch_field(h, type, cs_name(&h->text, cs_field(member, "name")), member,
                     cs_field(member, "type"), cs_modifiers(&h->text, member), 0);
        } else if (!strcmp(kind, "field_declaration") || !strcmp(kind, "event_field_declaration")) {
            TSNode declaration = cs_child(member, "variable_declaration");
            TSNode field_type = cs_field(declaration, "type");
            unsigned modifiers = cs_modifiers(&h->text, member);
            const char *text = cs_text(&h->text, member);
            /* A const is implicitly static. */
            uint32_t forced = text && strstr(text, "const ") ? OO_F_STATIC : 0;
            uint32_t parts =
                ts_node_is_null(declaration) ? 0 : ts_node_named_child_count(declaration);
            for (uint32_t p = 0; p < parts; p++) {
                TSNode declarator = ts_node_named_child(declaration, p);
                if (cs_is(declarator, "variable_declarator"))
                    ch_field(h, type, cs_name(&h->text, cs_field(declarator, "name")), declarator,
                             field_type, modifiers, forced);
            }
        } else if (!strcmp(kind, "enum_member_declaration")) {
            const char *name = cs_name(&h->text, cs_field(member, "name"));
            oo_field_t *field = name ? cbm_arena_calloc(h->text.arena, sizeof(*field)) : NULL;
            oo_typeref_t *ref = cbm_arena_calloc(h->text.arena, sizeof(*ref));
            if (!field || !ref)
                continue;
            ref->name = type->name;
            *field = (oo_field_t){.name = name,
                                  .graph_qn = ch_graph(h, member, name),
                                  .owner = type,
                                  .file = h->file,
                                  .ns = type->ns,
                                  .flags = OO_F_STATIC,
                                  .ref = ref};
            oo_index_field(h->ix, type, field);
        } else if (!strcmp(kind, "class_declaration") || !strcmp(kind, "struct_declaration") ||
                   !strcmp(kind, "interface_declaration") || !strcmp(kind, "enum_declaration") ||
                   !strcmp(kind, "record_declaration") ||
                   !strcmp(kind, "record_struct_declaration") ||
                   !strcmp(kind, "delegate_declaration")) {
            ch_type(h, member, type, ns);
        }
    }
}

static void ch_type(ch_t *h, TSNode node, oo_type_t *owner, const char *ns) {
    const char *name = cs_name(&h->text, cs_field(node, "name"));
    if (!name)
        return;
    int arity = cs_type_args_count(cs_child(node, "type_parameter_list"));
    const char *identity = cs_arity_name(h->text.arena, name, arity);
    const char *qn =
        owner ? cs_join(h->text.arena, owner->qn, identity) : cs_join(h->text.arena, ns, identity);
    if (!qn)
        return;
    unsigned modifiers = cs_modifiers(&h->text, node);
    uint32_t flags = 0;
    if (modifiers & CS_MOD_PARTIAL)
        flags |= OO_F_PARTIAL;
    if (modifiers & CS_MOD_STATIC)
        flags |= OO_F_STATIC;
    if (cs_is(node, "interface_declaration"))
        flags |= OO_F_INTERFACE;
    if (cs_is(node, "enum_declaration"))
        flags |= OO_F_ENUM;
    if (ts_node_has_error(node))
        flags |= OO_F_INCOMPLETE;
    const CBMDefinition *def = oo_defs_find(&h->defs, 'T', cs_line(node), name);
    oo_type_t *type = oo_index_type(h->ix, qn, identity, h->file, ns, owner,
                                    def ? def->qualified_name : NULL, flags);
    if (!type)
        return;
    if (!type->tparams)
        type->tparams = cs_tparams(&h->text, node, &type->tparam_count);
    TSNode bases = cs_child(node, "base_list");
    uint32_t parts = ts_node_is_null(bases) ? 0 : ts_node_named_child_count(bases);
    for (uint32_t i = 0; i < parts; i++) {
        TSNode base = ts_node_named_child(bases, i);
        /* `: Base(args)` in a primary constructor: the name is what matters. */
        if (cs_is(base, "primary_constructor_base_type") && ts_node_named_child_count(base))
            base = ts_node_named_child(base, 0);
        if (cs_is(base, "argument_list"))
            continue;
        oo_type_add_super(h->ix, type, cs_typeref(&h->text, base), h->file, ns);
    }
    /* A primary constructor: its parameters; a record's are also properties. */
    TSNode primary = cs_child(node, "parameter_list");
    if (!ts_node_is_null(primary)) {
        oo_callable_t *callable = cbm_arena_calloc(h->text.arena, sizeof(*callable));
        if (callable) {
            callable->name = identity;
            callable->owner = type;
            callable->file = h->file;
            callable->ns = ns;
            callable->flags = OO_F_CTOR;
            ch_parameters(h, callable, primary, NULL);
            oo_index_method(h->ix, type, callable);
        }
        if (cs_is(node, "record_declaration") || cs_is(node, "record_struct_declaration")) {
            uint32_t count = ts_node_named_child_count(primary);
            for (uint32_t i = 0; i < count; i++) {
                TSNode parameter = ts_node_named_child(primary, i);
                if (cs_is(parameter, "parameter"))
                    ch_field(h, type, cs_name(&h->text, cs_field(parameter, "name")), parameter,
                             cs_field(parameter, "type"), CS_MOD_PUBLIC | CS_MOD_ACCESS, 0);
            }
        }
    }
    TSNode body = cs_field(node, "body");
    ch_members(h, type, body, ns);
}

/* Using directives anywhere, each tagged with its enclosing namespace. */
typedef struct {
    oo_import_t *items;
    int count, capacity;
} cs_imports_t;

static void cs_add_using(const cs_text_t *t, cs_imports_t *list, TSNode node, const char *ns) {
    if (list->count == list->capacity) {
        int capacity = list->capacity ? list->capacity * 2 : 16;
        oo_import_t *grown = cbm_arena_alloc(t->arena, (size_t)capacity * sizeof(*grown));
        if (!grown)
            return;
        if (list->count)
            memcpy(grown, list->items, (size_t)list->count * sizeof(*grown));
        list->items = grown;
        list->capacity = capacity;
    }
    TSNode alias = cs_field(node, "name");
    bool statik = !ts_node_is_null(cs_child(node, "static"));
    TSNode path = {0};
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!ts_node_is_null(alias) && ts_node_eq(child, alias))
            continue;
        path = child;
    }
    const char *spelling = cs_type_spelling(t, path);
    if (!spelling)
        return;
    const char *local = ts_node_is_null(alias) ? NULL : cs_name(t, alias);
    list->items[list->count++] =
        (oo_import_t){.local = local, .path = spelling, .ns = ns, .star = !local, .statik = statik};
}

/* Visit one namespace's items. A file-scoped namespace renames the rest. */
static void cs_scan_usings(const cs_text_t *t, cs_imports_t *list, TSNode parent, const char *ns) {
    uint32_t count = ts_node_is_null(parent) ? 0 : ts_node_named_child_count(parent);
    const char *current = ns;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(parent, i);
        if (cs_is(child, "using_directive")) {
            cs_add_using(t, list, child, current);
        } else if (cs_is(child, "file_scoped_namespace_declaration")) {
            const char *name = cs_type_spelling(t, cs_field(child, "name"));
            current = name ? cs_join(t->arena, current, name) : current;
            /* Newer grammars nest the file's items inside the declaration. */
            cs_scan_usings(t, list, child, current);
        } else if (cs_is(child, "namespace_declaration")) {
            const char *name = cs_type_spelling(t, cs_field(child, "name"));
            cs_scan_usings(t, list, cs_field(child, "body"),
                           name ? cs_join(t->arena, current, name) : current);
        }
    }
}

static void ch_namespace_items(ch_t *h, TSNode parent, const char *ns) {
    uint32_t count = ts_node_is_null(parent) ? 0 : ts_node_named_child_count(parent);
    const char *current = ns;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(parent, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "file_scoped_namespace_declaration")) {
            const char *name = cs_type_spelling(&h->text, cs_field(child, "name"));
            current = name ? cs_join(h->text.arena, current, name) : current;
            ch_namespace_items(h, child, current);
        } else if (!strcmp(kind, "namespace_declaration")) {
            const char *name = cs_type_spelling(&h->text, cs_field(child, "name"));
            ch_namespace_items(h, cs_field(child, "body"),
                               name ? cs_join(h->text.arena, current, name) : current);
        } else if (!strcmp(kind, "class_declaration") || !strcmp(kind, "struct_declaration") ||
                   !strcmp(kind, "interface_declaration") || !strcmp(kind, "enum_declaration") ||
                   !strcmp(kind, "record_declaration") ||
                   !strcmp(kind, "record_struct_declaration") ||
                   !strcmp(kind, "delegate_declaration")) {
            ch_type(h, child, NULL, current);
        }
    }
}

bool oo_csharp_harvest(cbm_oo_index_t *ix, const char *rel, const CBMFileResult *file,
                       const char *source, size_t length, TSNode root) {
    ch_t h = {.ix = ix, .text = {.arena = ix->arena, .source = source, .length = length}};
    if (!oo_defs_init(&h.defs, ix->arena, file))
        return false;
    cs_imports_t imports = {0};
    cs_scan_usings(&h.text, &imports, root, "");
    h.file = oo_index_file(ix, CBM_LANG_CSHARP, rel, "", imports.items, imports.count,
                           ts_node_has_error(root));
    if (h.file)
        ch_namespace_items(&h, root, "");
    oo_defs_free(&h.defs);
    return h.file != NULL;
}

/* ── Walk ───────────────────────────────────────────────────────────── */

typedef struct {
    oo_walk_t *w;
    cs_text_t text;
    int depth;
} cw_t;

static oo_value_t cw_expr(cw_t *c, TSNode node);
static void cw_members(cw_t *c, TSNode body, const oo_type_t *type);
static void cw_type(cw_t *c, TSNode node, const oo_type_t *owner);

static void cw_children(cw_t *c, TSNode node) {
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        (void)cw_expr(c, ts_node_named_child(node, i));
}

static oo_value_t cw_ref(cw_t *c, const oo_typeref_t *ref) {
    return ref ? oo_value_from_ref(c->w, ref, &c->w->scope, NULL) : oo_value_unknown();
}

static void cw_type_mentions(cw_t *c, TSNode node, unsigned depth) {
    oo_walk_t *w = c->w;
    if (ts_node_is_null(node) || depth > 32)
        return;
    if (cs_is(node, "identifier") || cs_is(node, "qualified_name") || cs_is(node, "generic_name")) {
        oo_value_t value = cw_ref(c, cs_typeref(&c->text, node));
        if (value.type && value.type->graph_qn) {
            oo_emit_type_ref(w, node, value.type->graph_qn, "csharp-type-name-lookup");
            oo_emit_usage(w, node, value.type->graph_qn, CBM_RESOLVED_TYPE_REFERENCE,
                          CBM_RESOLVE_RULE_DECLARED_TYPE, "csharp-type-name-lookup");
        }
        TSNode list = cs_is(node, "generic_name") ? cs_child(node, "type_argument_list")
                      : cs_is(node, "qualified_name")
                          ? cs_child(cs_field(node, "name"), "type_argument_list")
                          : (TSNode){0};
        uint32_t count = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
        for (uint32_t i = 0; i < count; i++)
            cw_type_mentions(c, ts_node_named_child(list, i), depth + 1);
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        cw_type_mentions(c, ts_node_named_child(node, i), depth + 1);
}

static oo_value_t cw_declared(cw_t *c, TSNode type, oo_value_t initial) {
    if (cs_is(type, "implicit_type")) {
        initial.classref = false;
        return initial;
    }
    cw_type_mentions(c, type, 0);
    return cw_ref(c, cs_typeref(&c->text, type));
}

/* Target-typed `new()`: the declared type names the class. */
static void cw_implicit_creation(cw_t *c, TSNode node, const oo_value_t *target);

static void cw_variable_declaration(cw_t *c, TSNode node) {
    TSNode type = cs_field(node, "type");
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode declarator = ts_node_named_child(node, i);
        if (!cs_is(declarator, "variable_declarator"))
            continue;
        const char *name = cs_name(&c->text, cs_field(declarator, "name"));
        uint32_t parts = ts_node_named_child_count(declarator);
        TSNode initializer = {0};
        for (uint32_t p = 0; p < parts; p++) {
            TSNode part = ts_node_named_child(declarator, p);
            if (!ts_node_eq(part, cs_field(declarator, "name")) &&
                !cs_is(part, "bracketed_argument_list"))
                initializer = part;
        }
        oo_value_t initial = oo_value_unknown();
        bool implicit = cs_is(initializer, "implicit_object_creation_expression");
        if (!ts_node_is_null(initializer) && !implicit)
            initial = cw_expr(c, initializer);
        oo_value_t value = cw_declared(c, type, initial);
        if (implicit)
            cw_implicit_creation(c, initializer, &value);
        oo_bind(c->w, name, value);
    }
}

static void cw_bind_parameters(cw_t *c, TSNode list) {
    uint32_t count = ts_node_is_null(list) ? 0 : ts_node_child_count(list);
    TSNode pending_type = {0};
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(list, i);
        if (cs_is(child, "parameter")) {
            TSNode type = cs_field(child, "type");
            const char *name = cs_name(&c->text, cs_field(child, "name"));
            cw_type_mentions(c, type, 0);
            oo_bind(c->w, name,
                    ts_node_is_null(type) ? oo_value_unknown()
                                          : cw_ref(c, cs_typeref(&c->text, type)));
            /* A default value is an expression of the enclosing scope. */
            uint32_t parts = ts_node_named_child_count(child);
            TSNode name_node = cs_field(child, "name");
            for (uint32_t p = 0; p < parts; p++) {
                TSNode part = ts_node_named_child(child, p);
                if (!ts_node_is_null(name_node) &&
                    ts_node_start_byte(part) > ts_node_end_byte(name_node))
                    (void)cw_expr(c, part);
            }
        } else if (cs_is(child, "array_type")) {
            pending_type = child;
        } else if (cs_is(child, "identifier")) {
            /* `params T[] rest` directly in the list. */
            oo_bind(c->w, cs_name(&c->text, child),
                    ts_node_is_null(pending_type) ? oo_value_unknown()
                                                  : cw_ref(c, cs_typeref(&c->text, pending_type)));
        }
    }
}

static TSNode cw_argument_value(TSNode argument) {
    uint32_t parts = ts_node_named_child_count(argument);
    return parts ? ts_node_named_child(argument, parts - 1) : (TSNode){0};
}

static const oo_value_t *cw_arguments(cw_t *c, TSNode list, int *out_count) {
    uint32_t children = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
    int count = 0;
    for (uint32_t i = 0; i < children; i++)
        if (cs_is(ts_node_named_child(list, i), "argument"))
            count++;
    *out_count = count;
    oo_value_t *values =
        count ? cbm_arena_calloc(c->w->arena, (size_t)count * sizeof(*values)) : NULL;
    int at = 0;
    for (uint32_t i = 0; i < children; i++) {
        TSNode argument = ts_node_named_child(list, i);
        if (!cs_is(argument, "argument"))
            continue;
        oo_value_t value = cw_expr(c, cw_argument_value(argument));
        if (values)
            values[at] = value;
        at++;
    }
    return values;
}

static bool cw_external_accepts(const char *declared, const oo_value_t *value) {
    if (!value->external)
        return false;
    const char *a = strrchr(declared, '.');
    a = a ? a + 1 : declared;
    const char *b = strrchr(value->external, '.');
    b = b ? b + 1 : value->external;
    return !strcmp(a, b);
}

static bool cw_extension_accepts(oo_walk_t *w, const oo_callable_t *candidate,
                                 const oo_value_t *value) {
    const oo_typeref_t *ref = candidate->receiver;
    if (!ref || !ref->name)
        return false;
    oo_scope_t scope = oo_scope_of_callable(candidate);
    oo_name_t name = oo_resolve_name(w->ix, &scope, ref->name);
    if (name.status == OO_NAME_TPARAM) {
        if (!name.tparam->bound)
            return value->type || value->external;
        oo_value_t bound = oo_value_from_ref(w, name.tparam->bound, &scope, NULL);
        return bound.type && value->type && oo_is_subtype(w->ix, value->type, bound.type);
    }
    oo_value_t receiver = oo_value_from_ref(w, ref, &scope, NULL);
    if (receiver.dims != value->dims)
        return false;
    if (receiver.type)
        return value->type && oo_is_subtype(w->ix, value->type, receiver.type);
    return receiver.external && cw_external_accepts(receiver.external, value);
}

/* Extension methods: the innermost namespace, then its usings, outward. */
static oo_select_t cw_select_extension(cw_t *c, const oo_value_t *value, const char *name, int argc,
                                       const oo_value_t *args) {
    oo_walk_t *w = c->w;
    char prefix[512];
    int length = snprintf(prefix, sizeof(prefix), "%s", w->scope.ns ? w->scope.ns : "");
    if (length < 0 || (size_t)length >= sizeof(prefix))
        return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
    for (;;) {
        oo_callable_t *head = NULL;
        for (const oo_callable_t *e = oo_lookup_ext_static(w->ix, prefix, name); e; e = e->next) {
            if (!cw_extension_accepts(w, e, value))
                continue;
            oo_callable_t *copy = cbm_arena_alloc(w->arena, sizeof(*copy));
            if (!copy)
                return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
            *copy = *e;
            copy->next = head;
            head = copy;
        }
        const oo_file_t *file = w->f;
        for (int i = 0; i < file->import_count; i++) {
            const oo_import_t *import = &file->imports[i];
            if (!import->star || import->statik || strcmp(import->ns ? import->ns : "", prefix))
                continue;
            for (const oo_callable_t *e = oo_lookup_ext_static(w->ix, import->path, name); e;
                 e = e->next) {
                if (!cw_extension_accepts(w, e, value))
                    continue;
                oo_callable_t *copy = cbm_arena_alloc(w->arena, sizeof(*copy));
                if (!copy)
                    return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
                *copy = *e;
                copy->next = head;
                head = copy;
            }
        }
        if (head) {
            oo_select_t selected = oo_select_from_list(w, head, argc, args, value);
            if (selected.status == OO_SEL_UNIQUE || selected.status == OO_SEL_AMBIGUOUS)
                return selected;
        }
        if (!prefix[0])
            break;
        char *dot = strrchr(prefix, '.');
        if (dot)
            *dot = '\0';
        else
            prefix[0] = '\0';
    }
    return (oo_select_t){.status = OO_SEL_NONE};
}

static oo_value_t cw_emit_selected(cw_t *c, TSNode site, const oo_select_t *selected,
                                   const char *decision) {
    oo_walk_t *w = c->w;
    if (selected->status != OO_SEL_UNIQUE || !selected->callable)
        return oo_value_unknown();
    if (selected->callable->graph_qn)
        oo_emit_call(w, site, selected->callable->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                     CBM_RESOLVE_FACT_RECEIVER_TYPED, decision);
    return oo_callable_return(w, selected->callable, selected->subst);
}

static oo_value_t cw_member_call(cw_t *c, TSNode site, const oo_value_t *receiver, const char *name,
                                 int argc, const oo_value_t *args) {
    oo_walk_t *w = c->w;
    if (receiver->type) {
        oo_select_t member = oo_select_method(w, receiver, name, argc, args, receiver->classref);
        if (member.status == OO_SEL_UNIQUE)
            return cw_emit_selected(c, site, &member,
                                    receiver->classref ? "csharp-static-member"
                                                       : "csharp-declared-receiver-member");
        if (member.status == OO_SEL_AMBIGUOUS)
            return oo_value_unknown();
    }
    if (receiver->classref || (!receiver->type && !receiver->external))
        return oo_value_unknown();
    oo_select_t extension = cw_select_extension(c, receiver, name, argc, args);
    return cw_emit_selected(c, site, &extension, "csharp-extension-method");
}

static oo_value_t cw_bare_call(cw_t *c, TSNode site, const char *name, int argc,
                               const oo_value_t *args) {
    oo_walk_t *w = c->w;
    /* A local function or a delegate-typed local hides every member. */
    const oo_local_t *local = oo_lookup_local(w, name);
    if (local)
        return oo_value_unknown();
    bool complete = true;
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        if (r->unknown) {
            complete = false;
            break;
        }
        if (!r->value.type)
            continue;
        if (oo_has_method_named(w, &r->value, name)) {
            oo_select_t selected = oo_select_method(w, &r->value, name, argc, args, false);
            return cw_emit_selected(c, site, &selected, "csharp-enclosing-type-member");
        }
        if (!oo_hierarchy_complete(w->ix, r->value.type))
            complete = false;
    }
    if (!complete)
        return oo_value_unknown();
    /* `using static`: every imported type's members compete. */
    const oo_type_t *found = NULL;
    for (int i = 0; i < w->f->import_count; i++) {
        const oo_import_t *import = &w->f->imports[i];
        if (!import->statik)
            continue;
        oo_scope_t scope = {.file = w->f, .ns = import->ns};
        oo_name_t type_name = oo_resolve_name(w->ix, &scope, import->path);
        /* A type outside the index could declare the same name. */
        if (type_name.status != OO_NAME_TYPE)
            return oo_value_unknown();
        oo_value_t receiver = oo_value_of_type(type_name.type);
        if (!oo_has_method_named(w, &receiver, name))
            continue;
        if (found && found != type_name.type)
            return oo_value_unknown();
        found = type_name.type;
    }
    if (!found)
        return oo_value_unknown();
    oo_value_t receiver = oo_value_of_type(found);
    oo_select_t selected = oo_select_method(w, &receiver, name, argc, args, true);
    return cw_emit_selected(c, site, &selected, "csharp-using-static");
}

static const char *cw_generic_or_name(cw_t *c, TSNode node) {
    if (cs_is(node, "generic_name"))
        return cs_name(&c->text, cs_child(node, "identifier"));
    return cs_name(&c->text, node);
}

static oo_value_t cw_this(const oo_walk_t *w) {
    const oo_receiver_t *r = w->receiver;
    if (!r || r->unknown)
        return oo_value_unknown();
    oo_value_t value = r->value;
    value.classref = false;
    return value;
}

/* `base.M()`: the direct supertypes of the enclosing type. */
static oo_value_t cw_base_call(cw_t *c, TSNode site, const char *name, int argc,
                               const oo_value_t *args) {
    oo_walk_t *w = c->w;
    oo_value_t self = cw_this(w);
    if (!self.type)
        return oo_value_unknown();
    const oo_type_t *only = NULL;
    for (int i = 0; i < self.type->super_count; i++) {
        const oo_type_t *candidate = self.type->supers[i].resolved;
        if (!candidate)
            continue;
        oo_value_t receiver = oo_value_of_type(candidate);
        if (!oo_has_method_named(w, &receiver, name))
            continue;
        if (only && only != candidate)
            return oo_value_unknown();
        only = candidate;
    }
    if (!only)
        return oo_value_unknown();
    oo_value_t receiver = oo_value_of_type(only);
    oo_select_t selected = oo_select_method(w, &receiver, name, argc, args, false);
    return cw_emit_selected(c, site, &selected, "csharp-base-member");
}

static oo_value_t cw_invocation(cw_t *c, TSNode node) {
    TSNode function = cs_field(node, "function");
    TSNode arguments = cs_field(node, "arguments");
    int argc = 0;
    if (cs_is(function, "identifier") || cs_is(function, "generic_name")) {
        const char *name = cw_generic_or_name(c, function);
        const oo_value_t *args = cw_arguments(c, arguments, &argc);
        return name ? cw_bare_call(c, node, name, argc, args) : oo_value_unknown();
    }
    if (cs_is(function, "member_access_expression")) {
        TSNode object = cs_field(function, "expression");
        const char *name = cw_generic_or_name(c, cs_field(function, "name"));
        if (cs_is(object, "base")) {
            const oo_value_t *args = cw_arguments(c, arguments, &argc);
            return name ? cw_base_call(c, node, name, argc, args) : oo_value_unknown();
        }
        oo_value_t receiver = cw_expr(c, object);
        const oo_value_t *args = cw_arguments(c, arguments, &argc);
        return name ? cw_member_call(c, node, &receiver, name, argc, args) : oo_value_unknown();
    }
    if (cs_is(function, "conditional_access_expression")) {
        oo_value_t receiver = cw_expr(c, cs_field(function, "condition"));
        receiver.classref = false;
        TSNode binding = cs_child(function, "member_binding_expression");
        const char *name = cw_generic_or_name(c, cs_field(binding, "name"));
        const oo_value_t *args = cw_arguments(c, arguments, &argc);
        return name ? cw_member_call(c, node, &receiver, name, argc, args) : oo_value_unknown();
    }
    (void)cw_expr(c, function);
    (void)cw_arguments(c, arguments, &argc);
    return oo_value_unknown();
}

static void cw_emit_field(cw_t *c, TSNode site, const oo_field_t *field) {
    if (!field->graph_qn)
        return;
    oo_emit_usage(c->w, site, field->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                  CBM_RESOLVE_RULE_TYPE_MEMBER, "csharp-member-lookup");
    oo_emit_rw(c->w, site, field->graph_qn, "csharp-member-lookup");
}

static oo_value_t cw_identifier(cw_t *c, TSNode node) {
    oo_walk_t *w = c->w;
    const char *name = cs_name(&c->text, node);
    if (!name)
        return oo_value_unknown();
    const oo_local_t *local = oo_lookup_local(w, name);
    if (local)
        return local->value;
    bool obscurable = false;
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        if (r->unknown) {
            obscurable = true;
            break;
        }
        if (!r->value.type)
            continue;
        oo_subst_t *subst = NULL;
        bool incomplete = false;
        const oo_field_t *field = oo_find_field(w, &r->value, name, &subst, &incomplete);
        if (field) {
            cw_emit_field(c, node, field);
            oo_scope_t scope = oo_scope_of_field(field);
            oo_value_t value = oo_value_from_ref(w, field->ref, &scope, subst);
            /* §12.8.7.2 Color Color: a member whose type is spelled like its
             * own name also reads as that type, so static members stay callable. */
            if (value.type && !strcmp(value.type->name, name))
                value.classref = false;
            return value;
        }
        if (incomplete) {
            obscurable = true;
            break;
        }
    }
    if (obscurable && !oo_type_shaped(name))
        return oo_value_unknown();
    oo_typeref_t ref = {.name = name};
    oo_value_t value = cw_ref(c, &ref);
    if (value.type || value.external)
        value.classref = true;
    if (value.type && value.type->graph_qn)
        oo_emit_usage(w, node, value.type->graph_qn, CBM_RESOLVED_TYPE_REFERENCE,
                      CBM_RESOLVE_RULE_DECLARED_TYPE, "csharp-type-name-lookup");
    return value;
}

static oo_value_t cw_member_access(cw_t *c, TSNode node) {
    oo_walk_t *w = c->w;
    TSNode object = cs_field(node, "expression");
    TSNode name_node = cs_field(node, "name");
    const char *name = cw_generic_or_name(c, name_node);
    /* A dotted chain can be a namespace-qualified type. */
    const char *spelled = cs_text(&c->text, node);
    if (spelled &&
        strspn(spelled, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.") ==
            strlen(spelled)) {
        size_t head_length = strcspn(spelled, ".");
        char *head = cbm_arena_strndup(w->arena, spelled, head_length);
        bool variable = head && oo_lookup_local(w, head);
        for (const oo_receiver_t *r = w->receiver; r && !variable; r = r->parent) {
            oo_subst_t *subst = NULL;
            bool incomplete = false;
            variable = !r->unknown && r->value.type &&
                       oo_find_field(w, &r->value, head, &subst, &incomplete);
        }
        if (!variable) {
            oo_typeref_t ref = {.name = spelled};
            oo_name_t type_name = oo_resolve_name(w->ix, &w->scope, spelled);
            if (type_name.status == OO_NAME_TYPE) {
                oo_value_t value = oo_value_of_type(oo_unalias(w->ix, type_name.type));
                (void)ref;
                value.classref = true;
                return value;
            }
        }
    }
    oo_value_t receiver = cs_is(object, "this")   ? cw_this(w)
                          : cs_is(object, "base") ? oo_value_unknown()
                                                  : cw_expr(c, object);
    if (cs_is(object, "base")) {
        oo_value_t self = cw_this(w);
        if (self.type && self.type->super_count && self.type->supers[0].resolved)
            receiver = oo_value_of_type(self.type->supers[0].resolved);
    }
    if (!name || !receiver.type)
        return oo_value_unknown();
    oo_subst_t *subst = NULL;
    bool incomplete = false;
    const oo_field_t *field = oo_find_field(w, &receiver, name, &subst, &incomplete);
    if (field) {
        cw_emit_field(c, name_node, field);
        oo_scope_t scope = oo_scope_of_field(field);
        return oo_value_from_ref(w, field->ref, &scope, subst);
    }
    if (receiver.classref) {
        const char *identity = cs_type_spelling(&c->text, name_node);
        const oo_type_t *nested = identity ? oo_find_nested(w->ix, receiver.type, identity) : NULL;
        if (nested && !(nested->flags & OO_F_AMBIGUOUS)) {
            oo_value_t value = oo_value_of_type(nested);
            value.classref = true;
            return value;
        }
    }
    return oo_value_unknown();
}

static void cw_emit_constructor(cw_t *c, TSNode site, const oo_value_t *value, int argc,
                                const oo_value_t *args) {
    oo_walk_t *w = c->w;
    oo_select_t selected = oo_select_constructor(w, value, argc, args);
    if (selected.status == OO_SEL_UNIQUE && selected.callable->graph_qn)
        oo_emit_call(w, site, selected.callable->graph_qn, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                     CBM_RESOLVE_FACT_LANGUAGE_BOUND, "csharp-constructor-selection");
    else if ((selected.status == OO_SEL_NONE ||
              (selected.status == OO_SEL_UNIQUE && !selected.callable->graph_qn)) &&
             value->type && value->type->graph_qn && !(value->type->flags & OO_F_INTERFACE))
        oo_emit_call(w, site, value->type->graph_qn, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                     CBM_RESOLVE_FACT_LANGUAGE_BOUND | CBM_RESOLVE_FACT_CLASS_CALLABLE,
                     "csharp-implicit-constructor");
}

/* `{ X = 1 }`: the names are members of the created type, not of this scope. */
static void cw_initializer(cw_t *c, TSNode initializer, const oo_value_t *created) {
    oo_walk_t *w = c->w;
    uint32_t count = ts_node_is_null(initializer) ? 0 : ts_node_named_child_count(initializer);
    for (uint32_t i = 0; i < count; i++) {
        TSNode entry = ts_node_named_child(initializer, i);
        if (cs_is(entry, "assignment_expression")) {
            TSNode left = cs_field(entry, "left");
            (void)cw_expr(c, cs_field(entry, "right"));
            const char *name = cs_is(left, "identifier") ? cs_name(&c->text, left) : NULL;
            if (name && created->type) {
                oo_subst_t *subst = NULL;
                bool incomplete = false;
                const oo_field_t *field = oo_find_field(w, created, name, &subst, &incomplete);
                if (field)
                    cw_emit_field(c, left, field);
            }
        } else {
            (void)cw_expr(c, entry);
        }
    }
}

static oo_value_t cw_object_creation(cw_t *c, TSNode node) {
    TSNode type = cs_field(node, "type");
    cw_type_mentions(c, type, 0);
    oo_value_t value = cw_ref(c, cs_typeref(&c->text, type));
    int argc = 0;
    const oo_value_t *args = cw_arguments(c, cs_field(node, "arguments"), &argc);
    if (value.type)
        cw_emit_constructor(c, node, &value, argc, args);
    TSNode initializer = cs_field(node, "initializer");
    if (ts_node_is_null(initializer))
        initializer = cs_child(node, "initializer_expression");
    cw_initializer(c, initializer, &value);
    return value;
}

static void cw_implicit_creation(cw_t *c, TSNode node, const oo_value_t *target) {
    int argc = 0;
    const oo_value_t *args = cw_arguments(c, cs_child(node, "argument_list"), &argc);
    if (target->type)
        cw_emit_constructor(c, node, target, argc, args);
    cw_initializer(c, cs_child(node, "initializer_expression"), target);
}

/* Binders of a pattern, wherever the pattern nests. */
static oo_value_t cw_declaration_pattern(cw_t *c, TSNode node) {
    oo_walk_t *w = c->w;
    TSNode type = cs_field(node, "type");
    const char *name = cs_name(&c->text, cs_field(node, "name"));
    cw_type_mentions(c, type, 0);
    oo_bind(w, name, cw_ref(c, cs_typeref(&c->text, type)));
    return oo_value_unknown();
}

static oo_value_t cw_foreach(cw_t *c, TSNode node) {
    oo_walk_t *w = c->w;
    oo_value_t iterable = cw_expr(c, cs_field(node, "right"));
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    TSNode left = cs_field(node, "left");
    TSNode type = cs_field(node, "type");
    oo_value_t element = oo_value_unknown();
    if (iterable.dims > 0) {
        element = iterable;
        element.dims--;
        element.classref = false;
    } else if (iterable.external && iterable.arg_count == 1) {
        static const char *const known[] = {
            "IEnumerable", "ICollection", "IList", "List",  "IReadOnlyList", "IReadOnlyCollection",
            "HashSet",     "ISet",        "Queue", "Stack", "LinkedList",    NULL};
        const char *leaf = strrchr(iterable.external, '.');
        leaf = leaf ? leaf + 1 : iterable.external;
        for (int i = 0; known[i]; i++) {
            size_t length = strlen(known[i]);
            if (!strncmp(leaf, known[i], length) && leaf[length] == '`')
                element = iterable.args[0];
        }
    }
    if (cs_is(left, "identifier")) {
        oo_value_t value =
            ts_node_is_null(type) ? oo_value_unknown() : cw_declared(c, type, element);
        oo_bind(w, cs_name(&c->text, left), value);
    } else {
        /* Deconstruction: every binder shadows, with an unknown type. */
        (void)cw_expr(c, left);
    }
    (void)cw_expr(c, cs_field(node, "body"));
    oo_frame_pop(w);
    return oo_value_unknown();
}

/* Query clauses bind range variables the rest of the query reads. */
static void cw_bind_query_names(cw_t *c, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 16)
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "from_clause") || !strcmp(kind, "let_clause") ||
        !strcmp(kind, "join_clause") || !strcmp(kind, "join_into_clause") ||
        !strcmp(kind, "query_continuation")) {
        TSNode name = cs_field(node, "name");
        if (ts_node_is_null(name)) {
            uint32_t parts = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < parts; i++) {
                TSNode part = ts_node_named_child(node, i);
                if (cs_is(part, "identifier")) {
                    name = part;
                    break;
                }
            }
        }
        if (!ts_node_is_null(name))
            oo_bind(c->w, cs_name(&c->text, name), oo_value_unknown());
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        cw_bind_query_names(c, ts_node_named_child(node, i), depth + 1);
}

static oo_value_t cw_block_like(cw_t *c, TSNode node) {
    oo_frame_t frame;
    oo_frame_push(c->w, &frame);
    cw_children(c, node);
    oo_frame_pop(c->w);
    return oo_value_unknown();
}

static oo_value_t cw_lambda(cw_t *c, TSNode node) {
    oo_walk_t *w = c->w;
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    TSNode parameters = cs_field(node, "parameters");
    if (cs_is(parameters, "implicit_parameter") || cs_is(parameters, "identifier"))
        oo_bind(w, cs_name(&c->text, parameters), oo_value_unknown());
    else
        cw_bind_parameters(c, parameters);
    (void)cw_expr(c, cs_field(node, "body"));
    oo_frame_pop(w);
    return oo_value_unknown();
}

static oo_value_t cw_expr_kind(cw_t *c, TSNode node, const char *kind) {
    oo_walk_t *w = c->w;
    if (!strcmp(kind, "invocation_expression"))
        return cw_invocation(c, node);
    if (!strcmp(kind, "identifier"))
        return cw_identifier(c, node);
    if (!strcmp(kind, "member_access_expression"))
        return cw_member_access(c, node);
    if (!strcmp(kind, "object_creation_expression"))
        return cw_object_creation(c, node);
    if (!strcmp(kind, "implicit_object_creation_expression")) {
        oo_value_t none = oo_value_unknown();
        cw_implicit_creation(c, node, &none);
        return none;
    }
    if (!strcmp(kind, "variable_declaration")) {
        cw_variable_declaration(c, node);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "block") || !strcmp(kind, "switch_section") ||
        !strcmp(kind, "switch_expression_arm") || !strcmp(kind, "for_statement") ||
        !strcmp(kind, "using_statement") || !strcmp(kind, "fixed_statement") ||
        !strcmp(kind, "lock_statement") || !strcmp(kind, "while_statement") ||
        !strcmp(kind, "do_statement") || !strcmp(kind, "checked_statement") ||
        !strcmp(kind, "unsafe_statement"))
        return cw_block_like(c, node);
    if (!strcmp(kind, "this"))
        return cw_this(w);
    if (!strcmp(kind, "foreach_statement"))
        return cw_foreach(c, node);
    if (!strcmp(kind, "declaration_pattern"))
        return cw_declaration_pattern(c, node);
    if (!strcmp(kind, "var_pattern")) {
        uint32_t parts = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < parts; i++) {
            TSNode part = ts_node_named_child(node, i);
            if (cs_is(part, "identifier"))
                oo_bind(w, cs_name(&c->text, part), oo_value_unknown());
        }
        return oo_value_unknown();
    }
    if (!strcmp(kind, "recursive_pattern") || !strcmp(kind, "positional_pattern_clause") ||
        !strcmp(kind, "property_pattern_clause") || !strcmp(kind, "subpattern")) {
        /* A trailing designation binds; a member name inside is not a value. */
        uint32_t parts = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < parts; i++) {
            TSNode part = ts_node_named_child(node, i);
            if (cs_is(part, "identifier") && !strcmp(kind, "recursive_pattern"))
                oo_bind(w, cs_name(&c->text, part), oo_value_unknown());
            else if (!cs_is(part, "identifier") && !cs_is(part, "name_colon") &&
                     !cs_is(part, "expression_colon"))
                (void)cw_expr(c, part);
        }
        return oo_value_unknown();
    }
    if (!strcmp(kind, "declaration_expression")) {
        TSNode type = cs_field(node, "type");
        const char *name = cs_name(&c->text, cs_field(node, "name"));
        oo_bind(w, name, cw_declared(c, type, oo_value_unknown()));
        return oo_value_unknown();
    }
    if (!strcmp(kind, "is_pattern_expression")) {
        (void)cw_expr(c, cs_field(node, "expression"));
        (void)cw_expr(c, cs_field(node, "pattern"));
        return oo_value_unknown();
    }
    if (!strcmp(kind, "catch_clause")) {
        oo_frame_t frame;
        oo_frame_push(w, &frame);
        TSNode declaration = cs_child(node, "catch_declaration");
        TSNode type = cs_field(declaration, "type");
        if (!ts_node_is_null(type)) {
            cw_type_mentions(c, type, 0);
            oo_bind(w, cs_name(&c->text, cs_field(declaration, "name")),
                    cw_ref(c, cs_typeref(&c->text, type)));
        }
        (void)cw_expr(c, cs_child(node, "catch_filter_clause"));
        (void)cw_expr(c, cs_field(node, "body"));
        oo_frame_pop(w);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "lambda_expression") || !strcmp(kind, "anonymous_method_expression"))
        return cw_lambda(c, node);
    if (!strcmp(kind, "local_function_statement")) {
        oo_bind(w, cs_name(&c->text, cs_field(node, "name")), oo_value_unknown());
        oo_scope_t saved = w->scope;
        w->scope.tparams = cs_tparams(&c->text, node, &w->scope.tparam_count);
        oo_frame_t frame;
        oo_frame_push(w, &frame);
        cw_bind_parameters(c, cs_field(node, "parameters"));
        (void)cw_expr(c, cs_field(node, "body"));
        oo_frame_pop(w);
        w->scope = saved;
        return oo_value_unknown();
    }
    if (!strcmp(kind, "query_expression")) {
        oo_frame_t frame;
        oo_frame_push(w, &frame);
        cw_bind_query_names(c, node, 0);
        cw_children(c, node);
        oo_frame_pop(w);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return cw_expr(c, ts_node_named_child(node, 0));
    if (!strcmp(kind, "cast_expression")) {
        (void)cw_expr(c, cs_field(node, "value"));
        TSNode type = cs_field(node, "type");
        cw_type_mentions(c, type, 0);
        return cw_ref(c, cs_typeref(&c->text, type));
    }
    if (!strcmp(kind, "as_expression")) {
        (void)cw_expr(c, cs_field(node, "left"));
        TSNode type = cs_field(node, "right");
        cw_type_mentions(c, type, 0);
        return cw_ref(c, cs_typeref(&c->text, type));
    }
    if (!strcmp(kind, "postfix_unary_expression") && ts_node_named_child_count(node) == 1) {
        const char *text = cs_text(&c->text, node);
        size_t length = text ? strlen(text) : 0;
        oo_value_t value = cw_expr(c, ts_node_named_child(node, 0));
        return length && text[length - 1] == '!' ? value : oo_value_unknown();
    }
    if (!strcmp(kind, "await_expression")) {
        oo_value_t value = oo_value_unknown();
        uint32_t parts = ts_node_named_child_count(node);
        if (parts)
            value = cw_expr(c, ts_node_named_child(node, parts - 1));
        /* Task<T> and ValueTask<T> await to T; anything else stays unknown. */
        if (value.external && value.arg_count == 1) {
            const char *leaf = strrchr(value.external, '.');
            leaf = leaf ? leaf + 1 : value.external;
            if (!strcmp(leaf, "Task`1") || !strcmp(leaf, "ValueTask`1"))
                return value.args[0];
        }
        return oo_value_unknown();
    }
    if (!strcmp(kind, "conditional_expression")) {
        (void)cw_expr(c, cs_field(node, "condition"));
        TSNode first_node = cs_field(node, "consequence");
        TSNode second_node = cs_field(node, "alternative");
        oo_value_t first = cw_expr(c, first_node);
        oo_value_t second = cw_expr(c, second_node);
        if (cs_is(second_node, "null_literal"))
            return first;
        if (cs_is(first_node, "null_literal"))
            return second;
        return first.type && first.type == second.type ? first : oo_value_unknown();
    }
    if (!strcmp(kind, "binary_expression")) {
        oo_value_t left = cw_expr(c, cs_field(node, "left"));
        oo_value_t right = cw_expr(c, cs_field(node, "right"));
        const char *op = cs_text(&c->text, cs_field(node, "operator"));
        if (op && !strcmp(op, "??"))
            return left.type && (left.type == right.type || !oo_value_known(&right)) ? left : right;
        return oo_value_unknown();
    }
    if (!strcmp(kind, "assignment_expression")) {
        oo_value_t left = cw_expr(c, cs_field(node, "left"));
        (void)cw_expr(c, cs_field(node, "right"));
        return left;
    }
    if (!strcmp(kind, "element_access_expression")) {
        oo_value_t array = cw_expr(c, cs_field(node, "expression"));
        (void)cw_expr(c, cs_field(node, "subscript"));
        if (array.dims > 0) {
            array.dims--;
            array.classref = false;
            return array;
        }
        return oo_value_unknown();
    }
    if (!strcmp(kind, "conditional_access_expression")) {
        oo_value_t receiver = cw_expr(c, cs_field(node, "condition"));
        receiver.classref = false;
        TSNode binding = cs_child(node, "member_binding_expression");
        TSNode name_node = cs_field(binding, "name");
        const char *name = cw_generic_or_name(c, name_node);
        if (receiver.type && name) {
            oo_subst_t *subst = NULL;
            bool incomplete = false;
            const oo_field_t *field = oo_find_field(w, &receiver, name, &subst, &incomplete);
            if (field) {
                cw_emit_field(c, name_node, field);
                oo_scope_t scope = oo_scope_of_field(field);
                return oo_value_from_ref(w, field->ref, &scope, subst);
            }
        }
        return oo_value_unknown();
    }
    if (!strcmp(kind, "array_creation_expression")) {
        TSNode type = cs_field(node, "type");
        cw_type_mentions(c, type, 0);
        cw_children(c, cs_child(type, "array_rank_specifier"));
        (void)cw_expr(c, cs_child(node, "initializer_expression"));
        return cw_ref(c, cs_typeref(&c->text, type));
    }
    if (!strcmp(kind, "typeof_expression") || !strcmp(kind, "sizeof_expression") ||
        !strcmp(kind, "default_expression")) {
        cw_type_mentions(c, cs_field(node, "type"), 0);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "string_literal") || !strcmp(kind, "verbatim_string_literal") ||
        !strcmp(kind, "raw_string_literal") || !strcmp(kind, "interpolated_string_expression")) {
        if (kind[0] == 'i')
            cw_children(c, node);
        return oo_value_external("string");
    }
    if (!strcmp(kind, "integer_literal"))
        return oo_value_external("int");
    if (!strcmp(kind, "boolean_literal"))
        return oo_value_external("bool");
    if (!strcmp(kind, "real_literal"))
        return oo_value_external("double");
    if (!strcmp(kind, "character_literal"))
        return oo_value_external("char");
    if (!strcmp(kind, "null_literal") || !strcmp(kind, "comment") ||
        !strcmp(kind, "attribute_list") || !strcmp(kind, "discard") ||
        !strcmp(kind, "predefined_type"))
        return oo_value_unknown();
    /* A generic name in expression position: a type used as a receiver. */
    if (!strcmp(kind, "generic_name") || !strcmp(kind, "qualified_name")) {
        cw_type_mentions(c, node, 0);
        oo_value_t value = cw_ref(c, cs_typeref(&c->text, node));
        if (value.type || value.external)
            value.classref = true;
        return value;
    }
    if (!strcmp(kind, "class_declaration") || !strcmp(kind, "struct_declaration")) {
        cw_type(c, node, w->scope.owner);
        return oo_value_unknown();
    }
    cw_children(c, node);
    return oo_value_unknown();
}

enum { CW_MAX_AST_DEPTH = 384 };

static oo_value_t cw_expr(cw_t *c, TSNode node) {
    if (ts_node_is_null(node) || !oo_budget(c->w) || c->depth >= CW_MAX_AST_DEPTH)
        return oo_value_unknown();
    c->depth++;
    oo_value_t value = cw_expr_kind(c, node, ts_node_type(node));
    c->depth--;
    return value;
}

static void cw_method(cw_t *c, TSNode node) {
    oo_walk_t *w = c->w;
    oo_scope_t saved = w->scope;
    w->scope.tparams = cs_tparams(&c->text, node, &w->scope.tparam_count);
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    cw_type_mentions(c, cs_field(node, "returns"), 0);
    cw_type_mentions(c, cs_field(node, "type"), 0);
    cw_bind_parameters(c, cs_field(node, "parameters"));
    TSNode initializer = cs_child(node, "constructor_initializer");
    if (!ts_node_is_null(initializer))
        cw_children(c, initializer);
    (void)cw_expr(c, cs_field(node, "body"));
    oo_frame_pop(w);
    w->scope = saved;
}

static void cw_members(cw_t *c, TSNode body, const oo_type_t *type) {
    oo_walk_t *w = c->w;
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        const char *kind = ts_node_type(member);
        if (!strcmp(kind, "method_declaration") || !strcmp(kind, "constructor_declaration") ||
            !strcmp(kind, "destructor_declaration") || !strcmp(kind, "operator_declaration") ||
            !strcmp(kind, "conversion_operator_declaration")) {
            cw_method(c, member);
        } else if (!strcmp(kind, "property_declaration") || !strcmp(kind, "indexer_declaration") ||
                   !strcmp(kind, "event_declaration")) {
            oo_frame_t frame;
            oo_frame_push(w, &frame);
            cw_type_mentions(c, cs_field(member, "type"), 0);
            /* An indexer's parameters; `value` inside its setter. */
            TSNode parameters = cs_field(member, "parameters");
            if (!ts_node_is_null(parameters))
                cw_bind_parameters(c, parameters);
            oo_bind(w, "value", oo_value_unknown());
            (void)cw_expr(c, cs_field(member, "accessors"));
            (void)cw_expr(c, cs_field(member, "value"));
            oo_frame_pop(w);
        } else if (!strcmp(kind, "field_declaration") || !strcmp(kind, "event_field_declaration")) {
            /* Initializers read enclosing members; the field names aren't locals. */
            TSNode declaration = cs_child(member, "variable_declaration");
            TSNode field_type = cs_field(declaration, "type");
            cw_type_mentions(c, field_type, 0);
            oo_value_t declared = cw_ref(c, cs_typeref(&c->text, field_type));
            uint32_t parts =
                ts_node_is_null(declaration) ? 0 : ts_node_named_child_count(declaration);
            for (uint32_t p = 0; p < parts; p++) {
                TSNode declarator = ts_node_named_child(declaration, p);
                if (!cs_is(declarator, "variable_declarator"))
                    continue;
                uint32_t pieces = ts_node_named_child_count(declarator);
                for (uint32_t q = 0; q < pieces; q++) {
                    TSNode piece = ts_node_named_child(declarator, q);
                    if (ts_node_eq(piece, cs_field(declarator, "name")))
                        continue;
                    if (cs_is(piece, "implicit_object_creation_expression"))
                        cw_implicit_creation(c, piece, &declared);
                    else
                        (void)cw_expr(c, piece);
                }
            }
        } else if (!strcmp(kind, "class_declaration") || !strcmp(kind, "struct_declaration") ||
                   !strcmp(kind, "interface_declaration") || !strcmp(kind, "enum_declaration") ||
                   !strcmp(kind, "record_declaration") ||
                   !strcmp(kind, "record_struct_declaration")) {
            cw_type(c, member, type);
        } else if (!strcmp(kind, "enum_member_declaration")) {
            uint32_t parts = ts_node_named_child_count(member);
            for (uint32_t p = 0; p < parts; p++) {
                TSNode part = ts_node_named_child(member, p);
                if (!ts_node_eq(part, cs_field(member, "name")) && !cs_is(part, "attribute_list"))
                    (void)cw_expr(c, part);
            }
        }
    }
}

static void cw_type(cw_t *c, TSNode node, const oo_type_t *owner) {
    oo_walk_t *w = c->w;
    const char *name = cs_name(&c->text, cs_field(node, "name"));
    if (!name)
        return;
    int arity = cs_type_args_count(cs_child(node, "type_parameter_list"));
    const char *identity = cs_arity_name(w->arena, name, arity);
    const char *qn =
        owner ? cs_join(w->arena, owner->qn, identity) : cs_join(w->arena, w->scope.ns, identity);
    const oo_type_t *type = qn ? oo_lookup_type(w->ix, qn) : NULL;
    oo_scope_t saved = w->scope;
    if (type)
        w->scope.owner = type;
    w->scope.tparams = NULL;
    w->scope.tparam_count = 0;
    bool usable = type && !(type->flags & OO_F_AMBIGUOUS);
    oo_receiver_t *receiver =
        oo_receiver_push(w, usable ? oo_value_of_type(type) : oo_value_unknown(), false);
    if (receiver) {
        receiver->unknown = !usable;
        oo_frame_t frame;
        oo_frame_push(w, &frame);
        TSNode bases = cs_child(node, "base_list");
        uint32_t parts = ts_node_is_null(bases) ? 0 : ts_node_named_child_count(bases);
        for (uint32_t i = 0; i < parts; i++) {
            TSNode base = ts_node_named_child(bases, i);
            if (cs_is(base, "argument_list")) {
                int argc = 0;
                (void)cw_arguments(c, base, &argc);
            } else {
                cw_type_mentions(c, base, 0);
            }
        }
        /* Primary-constructor parameters are in scope for the whole body. */
        TSNode primary = cs_child(node, "parameter_list");
        if (!ts_node_is_null(primary))
            cw_bind_parameters(c, primary);
        cw_members(c, cs_field(node, "body"), usable ? type : NULL);
        oo_frame_pop(w);
        oo_receiver_pop(w);
    }
    w->scope = saved;
}

static void cw_namespace_items(cw_t *c, TSNode parent) {
    oo_walk_t *w = c->w;
    uint32_t count = ts_node_is_null(parent) ? 0 : ts_node_named_child_count(parent);
    const char *entry_ns = w->scope.ns;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(parent, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "file_scoped_namespace_declaration")) {
            const char *name = cs_type_spelling(&c->text, cs_field(child, "name"));
            if (name)
                w->scope.ns = cs_join(w->arena, w->scope.ns, name);
            cw_namespace_items(c, child);
        } else if (!strcmp(kind, "namespace_declaration")) {
            const char *saved = w->scope.ns;
            const char *name = cs_type_spelling(&c->text, cs_field(child, "name"));
            if (name)
                w->scope.ns = cs_join(w->arena, saved, name);
            cw_namespace_items(c, cs_field(child, "body"));
            w->scope.ns = saved;
        } else if (!strcmp(kind, "class_declaration") || !strcmp(kind, "struct_declaration") ||
                   !strcmp(kind, "interface_declaration") || !strcmp(kind, "enum_declaration") ||
                   !strcmp(kind, "record_declaration") ||
                   !strcmp(kind, "record_struct_declaration")) {
            cw_type(c, child, NULL);
        } else if (!strcmp(kind, "global_statement")) {
            /* Top-level statements: no enclosing type at all. */
            oo_frame_t frame;
            oo_frame_push(w, &frame);
            cw_children(c, child);
            oo_frame_pop(w);
        }
    }
    /* A file-scoped namespace ends with the file, a block one with its body. */
    w->scope.ns = entry_ns;
}

void oo_csharp_resolve(oo_walk_t *w, TSNode root) {
    cw_t c = {.w = w, .text = {.arena = w->arena, .source = w->source, .length = w->length}};
    cs_imports_t imports = {0};
    cs_scan_usings(&c.text, &imports, root, "");
    oo_file_t file = {.rel = w->rel_path,
                      .package = "",
                      .language = CBM_LANG_CSHARP,
                      .imports = imports.items,
                      .import_count = imports.count,
                      .has_error = ts_node_has_error(root)};
    w->f = &file;
    w->scope = (oo_scope_t){.file = &file, .ns = ""};
    cw_namespace_items(&c, root);
    w->f = NULL;
}
