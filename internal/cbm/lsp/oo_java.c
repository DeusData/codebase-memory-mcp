#include "oo_internal.h"
#include <stdio.h>
#include <string.h>

/* Java adapter. Name rules are JLS 6.4.1 and 6.5, method lookup JLS 15.12:
 * the innermost class with a member of the name is the only class searched,
 * single-static imports shadow on-demand ones, and a class with an unknown
 * supertype cannot prove that it lacks a member. */

static bool j_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}

static TSNode j_field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

static TSNode j_child(TSNode node, const char *kind) {
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (j_is(child, kind))
            return child;
    }
    return (TSNode){0};
}

typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length;
} j_text_t;

static const char *j_text(const j_text_t *t, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= t->length
               ? cbm_arena_strndup(t->arena, t->source + start, end - start)
               : NULL;
}

/* A dotted name's spelling without whitespace, annotations or type arguments. */
static const char *j_compact(const j_text_t *t, TSNode node) {
    const char *text = j_text(t, node);
    if (!text)
        return NULL;
    size_t length = strlen(text);
    char *out = cbm_arena_alloc(t->arena, length + 1);
    if (!out)
        return NULL;
    size_t n = 0;
    int depth = 0;
    for (size_t i = 0; i < length; i++) {
        char c = text[i];
        if (c == '<')
            depth++;
        else if (c == '>')
            depth--;
        else if (!depth && c != ' ' && c != '\t' && c != '\n' && c != '\r')
            out[n++] = c;
    }
    out[n] = '\0';
    /* An annotated segment (`a.@B c`) is not a plain name. */
    return strchr(out, '@') ? NULL : out;
}

static int j_dimensions(TSNode node) {
    int dims = 0;
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        if (!strcmp(ts_node_type(ts_node_child(node, i)), "["))
            dims++;
    return dims;
}

static const oo_typeref_t *j_typeref(const j_text_t *t, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    oo_typeref_t *ref = cbm_arena_calloc(t->arena, sizeof(*ref));
    if (!ref)
        return NULL;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "type_identifier") || !strcmp(kind, "scoped_type_identifier") ||
        !strcmp(kind, "integral_type") || !strcmp(kind, "floating_point_type") ||
        !strcmp(kind, "boolean_type") || !strcmp(kind, "void_type")) {
        ref->name = j_compact(t, node);
    } else if (!strcmp(kind, "generic_type")) {
        ref->name = j_compact(t, ts_node_named_child(node, 0));
        TSNode arguments = j_child(node, "type_arguments");
        uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
        if (count) {
            const oo_typeref_t **args = cbm_arena_calloc(t->arena, (count + 1) * sizeof(*args));
            if (args) {
                for (uint32_t i = 0; i < count; i++)
                    args[i] = j_typeref(t, ts_node_named_child(arguments, i));
                /* An unsupported argument keeps its position as an empty ref. */
                for (uint32_t i = 0; i < count; i++)
                    if (!args[i])
                        args[i] = cbm_arena_calloc(t->arena, sizeof(oo_typeref_t));
                ref->args = args;
            }
        }
    } else if (!strcmp(kind, "array_type")) {
        const oo_typeref_t *element = j_typeref(t, j_field(node, "element"));
        if (element)
            *ref = *element;
        ref->dims += j_dimensions(j_field(node, "dimensions"));
    } else if (!strcmp(kind, "annotated_type")) {
        uint32_t count = ts_node_named_child_count(node);
        return count ? j_typeref(t, ts_node_named_child(node, count - 1)) : ref;
    } else if (!strcmp(kind, "wildcard")) {
        const char *text = j_text(t, node);
        /* `? extends X` reads as X; `? super X` and `?` read as nothing. */
        if (text && strstr(text, "extends")) {
            uint32_t count = ts_node_named_child_count(node);
            return count ? j_typeref(t, ts_node_named_child(node, count - 1)) : ref;
        }
    }
    return ref;
}

static oo_tparam_t *j_tparams(const j_text_t *t, TSNode parameters, int *out_count) {
    *out_count = 0;
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    if (!count)
        return NULL;
    oo_tparam_t *items = cbm_arena_calloc(t->arena, count * sizeof(*items));
    if (!items)
        return NULL;
    int n = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (!j_is(parameter, "type_parameter"))
            continue;
        TSNode name = {0};
        TSNode bound = {0};
        uint32_t children = ts_node_named_child_count(parameter);
        for (uint32_t c = 0; c < children; c++) {
            TSNode child = ts_node_named_child(parameter, c);
            if (j_is(child, "type_identifier") || j_is(child, "identifier"))
                name = child;
            else if (j_is(child, "type_bound") && ts_node_named_child_count(child))
                bound = ts_node_named_child(child, 0);
        }
        items[n].name = j_text(t, name);
        items[n].bound = ts_node_is_null(bound) ? NULL : j_typeref(t, bound);
        if (items[n].name)
            n++;
    }
    *out_count = n;
    return items;
}

static uint32_t j_modifiers(TSNode node) {
    uint32_t flags = 0;
    TSNode modifiers = j_child(node, "modifiers");
    uint32_t count = ts_node_is_null(modifiers) ? 0 : ts_node_child_count(modifiers);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(modifiers, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "private"))
            flags |= OO_F_PRIVATE;
        else if (!strcmp(kind, "static"))
            flags |= OO_F_STATIC;
        else if (!strcmp(kind, "abstract"))
            flags |= OO_F_ABSTRACT;
    }
    return flags;
}

static const char *j_join(CBMArena *arena, const char *prefix, const char *name) {
    return prefix && prefix[0] ? cbm_arena_sprintf(arena, "%s.%s", prefix, name)
                               : cbm_arena_strdup(arena, name);
}

/* ── Harvest ────────────────────────────────────────────────────────── */

typedef struct {
    cbm_oo_index_t *ix;
    j_text_t text;
    oo_defs_t defs;
    const oo_file_t *file;
} jh_t;

static uint32_t j_line(TSNode node) {
    return ts_node_start_point(node).row + 1;
}

static const char *jh_graph(const jh_t *h, char kind, TSNode node, const char *name) {
    const CBMDefinition *def = oo_defs_find(&h->defs, kind, j_line(node), name);
    return def ? def->qualified_name : NULL;
}

static void jh_parameters(const jh_t *h, oo_callable_t *callable, TSNode parameters) {
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    const oo_typeref_t **refs =
        count ? cbm_arena_calloc(h->text.arena, count * sizeof(*refs)) : NULL;
    int n = 0;
    bool variadic = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (j_is(parameter, "formal_parameter")) {
            const oo_typeref_t *ref = j_typeref(&h->text, j_field(parameter, "type"));
            int extra = j_dimensions(j_field(parameter, "dimensions"));
            if (ref && extra) {
                oo_typeref_t *copy = cbm_arena_alloc(h->text.arena, sizeof(*copy));
                if (copy) {
                    *copy = *ref;
                    copy->dims += extra;
                    ref = copy;
                }
            }
            if (refs)
                refs[n] = ref;
            n++;
        } else if (j_is(parameter, "spread_parameter")) {
            TSNode type = {0};
            uint32_t children = ts_node_named_child_count(parameter);
            for (uint32_t c = 0; c < children; c++) {
                TSNode child = ts_node_named_child(parameter, c);
                if (!j_is(child, "modifiers") && !j_is(child, "variable_declarator")) {
                    type = child;
                    break;
                }
            }
            if (refs)
                refs[n] = j_typeref(&h->text, type);
            n++;
            variadic = true;
        }
    }
    callable->params = refs;
    callable->param_count = n;
    callable->min_args = variadic ? n - 1 : n;
    callable->max_args = variadic ? -1 : n;
}

static oo_callable_t *jh_callable(const jh_t *h, const oo_type_t *owner, const char *name,
                                  const char *graph, uint32_t flags) {
    oo_callable_t *callable = cbm_arena_calloc(h->text.arena, sizeof(*callable));
    if (!callable)
        return NULL;
    callable->name = name;
    callable->graph_qn = graph;
    callable->owner = owner;
    callable->file = h->file;
    callable->ns = owner->ns;
    callable->flags = flags;
    return callable;
}

static void jh_member_fields(const jh_t *h, const oo_type_t *owner, TSNode node, uint32_t forced) {
    const oo_typeref_t *type = j_typeref(&h->text, j_field(node, "type"));
    uint32_t flags = j_modifiers(node) | forced;
    uint32_t children = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < children; i++) {
        TSNode declarator = ts_node_named_child(node, i);
        if (!j_is(declarator, "variable_declarator"))
            continue;
        TSNode name_node = j_field(declarator, "name");
        const char *name = j_text(&h->text, name_node);
        if (!name)
            continue;
        oo_field_t *field = cbm_arena_calloc(h->text.arena, sizeof(*field));
        if (!field)
            continue;
        const oo_typeref_t *ref = type;
        int extra = j_dimensions(j_field(declarator, "dimensions"));
        if (ref && extra) {
            oo_typeref_t *copy = cbm_arena_alloc(h->text.arena, sizeof(*copy));
            if (copy) {
                *copy = *ref;
                copy->dims += extra;
                ref = copy;
            }
        }
        *field = (oo_field_t){.name = name,
                              .graph_qn = jh_graph(h, 'F', node, name),
                              .owner = owner,
                              .file = h->file,
                              .ns = owner->ns,
                              .flags = flags,
                              .ref = ref};
        if (!field->graph_qn)
            field->graph_qn = jh_graph(h, 'F', declarator, name);
        oo_index_field(h->ix, owner, field);
    }
}

static void jh_type(jh_t *h, TSNode node, const oo_type_t *owner);

static void jh_body(jh_t *h, oo_type_t *type, TSNode body, bool is_interface);

static void jh_enum_constants(jh_t *h, oo_type_t *type, TSNode body) {
    uint32_t children = ts_node_named_child_count(body);
    oo_typeref_t *ref = cbm_arena_calloc(h->text.arena, sizeof(*ref));
    if (ref)
        ref->name = type->name;
    for (uint32_t i = 0; i < children; i++) {
        TSNode child = ts_node_named_child(body, i);
        if (j_is(child, "enum_constant")) {
            const char *name = j_text(&h->text, j_field(child, "name"));
            if (!name)
                continue;
            oo_field_t *field = cbm_arena_calloc(h->text.arena, sizeof(*field));
            if (!field)
                continue;
            *field = (oo_field_t){.name = name,
                                  .graph_qn = jh_graph(h, 'F', child, name),
                                  .owner = type,
                                  .file = h->file,
                                  .ns = type->ns,
                                  .flags = OO_F_STATIC,
                                  .ref = ref};
            oo_index_field(h->ix, type, field);
        } else if (j_is(child, "enum_body_declarations")) {
            jh_body(h, type, child, false);
        }
    }
}

static void jh_record_components(jh_t *h, oo_type_t *type, TSNode parameters) {
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        TSNode name_node = j_field(parameter, "name");
        const oo_typeref_t *ref = j_typeref(&h->text, j_field(parameter, "type"));
        if (j_is(parameter, "spread_parameter")) {
            TSNode declarator = j_child(parameter, "variable_declarator");
            name_node = j_field(declarator, "name");
            uint32_t children = ts_node_named_child_count(parameter);
            for (uint32_t c = 0; c < children; c++) {
                TSNode child = ts_node_named_child(parameter, c);
                if (!j_is(child, "modifiers") && !j_is(child, "variable_declarator")) {
                    const oo_typeref_t *element = j_typeref(&h->text, child);
                    oo_typeref_t *copy = cbm_arena_alloc(h->text.arena, sizeof(*copy));
                    if (element && copy) {
                        *copy = *element;
                        copy->dims++;
                        ref = copy;
                    }
                    break;
                }
            }
        }
        const char *name = j_text(&h->text, name_node);
        if (!name)
            continue;
        /* The component's implicit field, reachable as x inside the record. */
        oo_field_t *field = cbm_arena_calloc(h->text.arena, sizeof(*field));
        if (field) {
            *field = (oo_field_t){.name = name,
                                  .owner = type,
                                  .file = h->file,
                                  .ns = type->ns,
                                  .flags = OO_F_PRIVATE,
                                  .ref = ref};
            oo_index_field(h->ix, type, field);
        }
        /* The generated accessor, when extraction materialized one. A hand
         * written accessor has its own method_declaration in the body. */
        const CBMDefinition *def = oo_defs_find(&h->defs, 'C', j_line(parameter), name);
        if (!def || !def->extra_props)
            continue;
        oo_callable_t *accessor = jh_callable(h, type, name, def->qualified_name, OO_F_GENERATED);
        if (accessor) {
            accessor->ret = ref;
            oo_index_method(h->ix, type, accessor);
        }
    }
}

static void jh_method(jh_t *h, oo_type_t *type, TSNode node, bool is_interface) {
    bool ctor =
        j_is(node, "constructor_declaration") || j_is(node, "compact_constructor_declaration");
    const char *name = j_text(&h->text, j_field(node, "name"));
    if (!name)
        return;
    uint32_t flags = j_modifiers(node);
    if (ctor)
        flags |= OO_F_CTOR;
    TSNode modifiers = j_child(node, "modifiers");
    const char *modifier_text = ts_node_is_null(modifiers) ? NULL : j_text(&h->text, modifiers);
    if (modifier_text && strstr(modifier_text, "@Override"))
        flags |= OO_F_OVERRIDE;
    oo_callable_t *callable = jh_callable(h, type, name, jh_graph(h, 'C', node, name), flags);
    if (!callable)
        return;
    if (j_is(node, "compact_constructor_declaration")) {
        /* Its parameters are the record header's. */
        TSNode record = ts_node_parent(ts_node_parent(node));
        jh_parameters(h, callable, j_field(record, "parameters"));
    } else {
        jh_parameters(h, callable, j_field(node, "parameters"));
    }
    if (!ctor) {
        const oo_typeref_t *ret = j_typeref(&h->text, j_field(node, "type"));
        int extra = j_dimensions(j_field(node, "dimensions"));
        if (ret && extra) {
            oo_typeref_t *copy = cbm_arena_alloc(h->text.arena, sizeof(*copy));
            if (copy) {
                *copy = *ret;
                copy->dims += extra;
                ret = copy;
            }
        }
        callable->ret = ret;
    }
    callable->tparams =
        j_tparams(&h->text, j_field(node, "type_parameters"), &callable->tparam_count);
    oo_index_method(h->ix, type, callable);
}

static bool j_type_decl(TSNode node) {
    return j_is(node, "class_declaration") || j_is(node, "interface_declaration") ||
           j_is(node, "enum_declaration") || j_is(node, "record_declaration") ||
           j_is(node, "annotation_type_declaration");
}

static void jh_body(jh_t *h, oo_type_t *type, TSNode body, bool is_interface) {
    uint32_t children = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < children; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (j_is(member, "method_declaration") || j_is(member, "constructor_declaration") ||
            j_is(member, "compact_constructor_declaration")) {
            jh_method(h, type, member, is_interface);
        } else if (j_is(member, "annotation_type_element_declaration")) {
            const char *name = j_text(&h->text, j_field(member, "name"));
            oo_callable_t *callable =
                name ? jh_callable(h, type, name, jh_graph(h, 'C', member, name), 0) : NULL;
            if (callable) {
                callable->ret = j_typeref(&h->text, j_field(member, "type"));
                oo_index_method(h->ix, type, callable);
            }
        } else if (j_is(member, "field_declaration") || j_is(member, "constant_declaration")) {
            /* Interface constants are implicitly static; nested types below too. */
            jh_member_fields(h, type, member, is_interface ? OO_F_STATIC : 0);
        } else if (j_type_decl(member)) {
            jh_type(h, member, type);
        }
    }
}

static void jh_supers(jh_t *h, oo_type_t *type, TSNode node) {
    TSNode superclass = j_field(node, "superclass");
    if (!ts_node_is_null(superclass) && ts_node_named_child_count(superclass)) {
        oo_type_add_super(h->ix, type, j_typeref(&h->text, ts_node_named_child(superclass, 0)),
                          h->file, type->ns);
    }
    TSNode lists[2] = {j_field(node, "interfaces"), j_child(node, "extends_interfaces")};
    for (int l = 0; l < 2; l++) {
        TSNode list = j_child(lists[l], "type_list");
        uint32_t count = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
        for (uint32_t i = 0; i < count; i++)
            oo_type_add_super(h->ix, type, j_typeref(&h->text, ts_node_named_child(list, i)),
                              h->file, type->ns);
    }
}

static void jh_type(jh_t *h, TSNode node, const oo_type_t *owner) {
    const char *name = j_text(&h->text, j_field(node, "name"));
    if (!name)
        return;
    const char *qn = owner ? j_join(h->text.arena, owner->qn, name)
                           : j_join(h->text.arena, h->file->package, name);
    if (!qn)
        return;
    uint32_t flags = j_modifiers(node);
    bool is_interface =
        j_is(node, "interface_declaration") || j_is(node, "annotation_type_declaration");
    if (is_interface)
        flags |= OO_F_INTERFACE;
    if (j_is(node, "enum_declaration"))
        flags |= OO_F_ENUM;
    if (oo_members_broken(node))
        flags |= OO_F_INCOMPLETE;
    oo_type_t *type =
        oo_index_type(h->ix, qn, name, h->file, "", owner, jh_graph(h, 'T', node, name), flags);
    if (!type)
        return;
    type->tparams = j_tparams(&h->text, j_field(node, "type_parameters"), &type->tparam_count);
    jh_supers(h, type, node);
    TSNode body = j_field(node, "body");
    if (j_is(node, "enum_declaration"))
        jh_enum_constants(h, type, body);
    else
        jh_body(h, type, body, is_interface);
    if (j_is(node, "record_declaration"))
        jh_record_components(h, type, j_field(node, "parameters"));
}

static void j_read_imports(const j_text_t *t, TSNode root, oo_import_t **out, int *out_count,
                           const char **package) {
    uint32_t children = ts_node_named_child_count(root);
    int capacity = 0;
    for (uint32_t i = 0; i < children; i++)
        if (j_is(ts_node_named_child(root, i), "import_declaration"))
            capacity++;
    oo_import_t *imports =
        capacity ? cbm_arena_calloc(t->arena, (size_t)capacity * sizeof(*imports)) : NULL;
    int count = 0;
    *package = "";
    for (uint32_t i = 0; i < children; i++) {
        TSNode child = ts_node_named_child(root, i);
        if (j_is(child, "package_declaration")) {
            uint32_t parts = ts_node_named_child_count(child);
            for (uint32_t p = 0; p < parts; p++) {
                TSNode part = ts_node_named_child(child, p);
                if (j_is(part, "scoped_identifier") || j_is(part, "identifier")) {
                    const char *text = j_compact(t, part);
                    if (text)
                        *package = text;
                }
            }
        } else if (j_is(child, "import_declaration") && imports) {
            TSNode path = {0};
            bool star = false, statik = false;
            uint32_t tokens = ts_node_child_count(child);
            for (uint32_t c = 0; c < tokens; c++) {
                TSNode token = ts_node_child(child, c);
                const char *kind = ts_node_type(token);
                if (!strcmp(kind, "static"))
                    statik = true;
                else if (!strcmp(kind, "asterisk"))
                    star = true;
                else if (!strcmp(kind, "scoped_identifier") || !strcmp(kind, "identifier"))
                    path = token;
            }
            const char *text = j_compact(t, path);
            if (!text)
                continue;
            const char *dot = strrchr(text, '.');
            imports[count++] = (oo_import_t){.local = star ? NULL : (dot ? dot + 1 : text),
                                             .path = text,
                                             .star = star,
                                             .statik = statik};
        }
    }
    *out = imports;
    *out_count = count;
}

bool oo_java_harvest(cbm_oo_index_t *ix, const char *rel, const CBMFileResult *file,
                     const char *source, size_t length, TSNode root) {
    jh_t h = {.ix = ix, .text = {.arena = ix->arena, .source = source, .length = length}};
    if (!oo_defs_init(&h.defs, ix->arena, file))
        return false;
    oo_import_t *imports = NULL;
    int import_count = 0;
    const char *package = "";
    j_read_imports(&h.text, root, &imports, &import_count, &package);
    h.file = oo_index_file(ix, CBM_LANG_JAVA, rel, package, imports, import_count,
                           ts_node_has_error(root));
    if (h.file) {
        uint32_t children = ts_node_named_child_count(root);
        for (uint32_t i = 0; i < children; i++) {
            TSNode child = ts_node_named_child(root, i);
            if (j_type_decl(child))
                jh_type(&h, child, NULL);
        }
    }
    oo_defs_free(&h.defs);
    return h.file != NULL;
}

/* ── Walk ───────────────────────────────────────────────────────────── */

typedef struct {
    oo_walk_t *w;
    j_text_t text;
    int depth;
} jw_t;

static oo_value_t jw_expr(jw_t *j, TSNode node);
static void jw_class_body(jw_t *j, TSNode body, const oo_type_t *type);
static void jw_type_decl(jw_t *j, TSNode node, const oo_type_t *owner);

static void jw_children(jw_t *j, TSNode node) {
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        (void)jw_expr(j, ts_node_named_child(node, i));
}

static oo_value_t jw_ref(jw_t *j, const oo_typeref_t *ref) {
    oo_walk_t *w = j->w;
    if (!ref)
        return oo_value_unknown();
    if (ref->name && !ref->dims) {
        /* A local class has no index identity: its name reads as unknown and
         * can never fall through to a same-named project class. */
        char first[256];
        const char *dot = strchr(ref->name, '.');
        size_t length = dot ? (size_t)(dot - ref->name) : strlen(ref->name);
        if (length < sizeof(first)) {
            memcpy(first, ref->name, length);
            first[length] = '\0';
            const oo_local_t *local =
                oo_lookup_local(w, cbm_arena_sprintf(w->arena, "#type:%s", first));
            if (local)
                return oo_value_unknown();
        }
    }
    return oo_value_from_ref(w, ref, &w->scope, NULL);
}

static const oo_typeref_t *jw_type_at(jw_t *j, TSNode node) {
    return j_typeref(&j->text, node);
}

static void jw_emit_type_mentions(jw_t *j, TSNode node, unsigned depth);

/* Every type mention in a type expression: the identifier leaves at their own
 * spans, resolved in the current scope. */
static void jw_type_leaf(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    const char *text = j_compact(&j->text, node);
    if (!text)
        return;
    oo_typeref_t ref = {.name = text};
    oo_value_t value = jw_ref(j, &ref);
    if (value.type && value.type->graph_qn)
        oo_emit_type_ref(w, node, value.type->graph_qn, "java-type-name-lookup");
}

static void jw_emit_type_mentions(jw_t *j, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 32)
        return;
    if (j_is(node, "type_identifier") || j_is(node, "scoped_type_identifier")) {
        jw_type_leaf(j, node);
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        jw_emit_type_mentions(j, ts_node_named_child(node, i), depth + 1);
}

static void jw_bind_declared(jw_t *j, const char *name, TSNode type, oo_value_t initial, int dims) {
    oo_walk_t *w = j->w;
    oo_value_t value = oo_value_unknown();
    const char *spelling = j_is(type, "type_identifier") ? j_text(&j->text, type) : NULL;
    if (spelling && !strcmp(spelling, "var")) {
        value = initial;
        value.classref = false;
    } else {
        value = jw_ref(j, jw_type_at(j, type));
        jw_emit_type_mentions(j, type, 0);
    }
    value.dims += dims;
    oo_bind(w, name, value);
}

static void jw_parameters(jw_t *j, TSNode parameters) {
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (j_is(parameter, "formal_parameter")) {
            jw_bind_declared(j, j_text(&j->text, j_field(parameter, "name")),
                             j_field(parameter, "type"), oo_value_unknown(),
                             j_dimensions(j_field(parameter, "dimensions")));
        } else if (j_is(parameter, "spread_parameter")) {
            TSNode declarator = j_child(parameter, "variable_declarator");
            const char *name = j_text(&j->text, j_field(declarator, "name"));
            TSNode type = {0};
            uint32_t children = ts_node_named_child_count(parameter);
            for (uint32_t c = 0; c < children; c++) {
                TSNode child = ts_node_named_child(parameter, c);
                if (!j_is(child, "modifiers") && !j_is(child, "variable_declarator")) {
                    type = child;
                    break;
                }
            }
            jw_bind_declared(j, name, type, oo_value_unknown(), 1);
        } else if (j_is(parameter, "identifier")) {
            oo_bind(j->w, j_text(&j->text, parameter), oo_value_unknown());
        }
    }
}

static int jw_argument_count(TSNode arguments) {
    int count = 0;
    uint32_t children = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < children; i++) {
        const char *kind = ts_node_type(ts_node_named_child(arguments, i));
        if (strcmp(kind, "line_comment") && strcmp(kind, "block_comment"))
            count++;
    }
    return count;
}

/* Arguments are ordinary expressions at this site: evaluate each exactly once. */
static const oo_value_t *jw_arguments(jw_t *j, TSNode arguments, int *out_count) {
    int count = jw_argument_count(arguments);
    *out_count = count;
    oo_value_t *values =
        count ? cbm_arena_calloc(j->w->arena, (size_t)count * sizeof(*values)) : NULL;
    int at = 0;
    uint32_t children = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < children; i++) {
        TSNode child = ts_node_named_child(arguments, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "line_comment") || !strcmp(kind, "block_comment"))
            continue;
        oo_value_t value = jw_expr(j, child);
        if (values)
            values[at] = value;
        at++;
    }
    return values;
}

static oo_value_t jw_select_emit(jw_t *j, TSNode site, const oo_value_t *receiver, const char *name,
                                 int argc, const oo_value_t *args, bool static_only,
                                 const char *decision) {
    oo_walk_t *w = j->w;
    oo_select_t selected = oo_select_method(w, receiver, name, argc, args, static_only);
    if (selected.status != OO_SEL_UNIQUE || !selected.callable)
        return oo_value_unknown();
    if (selected.callable->graph_qn)
        oo_emit_call(w, site, selected.callable->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                     CBM_RESOLVE_FACT_RECEIVER_TYPED, decision);
    return oo_callable_return(w, selected.callable, selected.subst);
}

static bool jw_receiver_owns(const oo_receiver_t *receiver, const char *name) {
    for (int i = 0; i < receiver->own_count; i++)
        if (!strcmp(receiver->own_names[i], name))
            return true;
    return false;
}

static oo_value_t jw_static_import_call(jw_t *j, TSNode site, const char *name, int argc,
                                        const oo_value_t *args, bool chain_complete) {
    oo_walk_t *w = j->w;
    const oo_file_t *file = w->f;
    /* A single-static import shadows every on-demand one (JLS 6.4.1). */
    for (int i = 0; i < file->import_count; i++) {
        const oo_import_t *import = &file->imports[i];
        if (!import->statik || import->star || !import->local || strcmp(import->local, name))
            continue;
        const char *dot = strrchr(import->path, '.');
        const char *owner_qn =
            dot ? cbm_arena_strndup(w->arena, import->path, (size_t)(dot - import->path)) : NULL;
        const oo_type_t *owner = owner_qn ? oo_lookup_type(w->ix, owner_qn) : NULL;
        if (!owner || (owner->flags & OO_F_AMBIGUOUS))
            return oo_value_unknown();
        oo_value_t receiver = oo_value_of_type(owner);
        return jw_select_emit(j, site, &receiver, name, argc, args, true,
                              "java-single-static-import");
    }
    if (!chain_complete)
        return oo_value_unknown();
    const oo_type_t *found = NULL;
    for (int i = 0; i < file->import_count; i++) {
        const oo_import_t *import = &file->imports[i];
        if (!import->statik || !import->star)
            continue;
        const oo_type_t *owner = oo_lookup_type(w->ix, import->path);
        /* Members of an unindexed class take part in the same overload set. */
        if (!owner || (owner->flags & OO_F_AMBIGUOUS))
            return oo_value_unknown();
        oo_value_t receiver = oo_value_of_type(owner);
        if (!oo_has_method_named(w, &receiver, name))
            continue;
        if (found && found != owner)
            return oo_value_unknown();
        found = owner;
    }
    if (!found)
        return oo_value_unknown();
    oo_value_t receiver = oo_value_of_type(found);
    return jw_select_emit(j, site, &receiver, name, argc, args, true,
                          "java-static-import-on-demand");
}

static oo_value_t jw_bare_call(jw_t *j, TSNode site, const char *name, int argc,
                               const oo_value_t *args) {
    oo_walk_t *w = j->w;
    /* JLS 15.12.1: the innermost enclosing class with a member of this name is
     * the one class searched. */
    bool complete = true;
    for (const oo_receiver_t *r = w->receiver; r && complete; r = r->parent) {
        if (r->anonymous && jw_receiver_owns(r, name))
            return oo_value_unknown();
        if (r->unknown) {
            complete = false;
            break;
        }
        if (!r->value.type)
            continue;
        if (oo_has_method_named(w, &r->value, name))
            return jw_select_emit(j, site, &r->value, name, argc, args, false,
                                  "java-enclosing-class-member");
        /* An unknown supertype could declare the name, hiding every outer class. */
        if (!oo_hierarchy_complete(w->ix, r->value.type))
            complete = false;
    }
    /* A single-static import spells out this very name: a shadowing inherited
     * member would make that import useless, so it is taken even when a
     * supertype is unindexed. */
    return jw_static_import_call(j, site, name, argc, args, complete && !oo_closed_member(name));
}

/* The innermost `this`, or `Outer.this`. */
static oo_value_t jw_this_value(const oo_walk_t *w) {
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        if (r->unknown)
            return oo_value_unknown();
        return r->value;
    }
    return oo_value_unknown();
}

/* A field reached at this exact token: its USAGE and READS/WRITES carriers. */
static oo_value_t jw_field_value(jw_t *j, TSNode site, const oo_field_t *field,
                                 const oo_subst_t *subst) {
    oo_walk_t *w = j->w;
    if (field->graph_qn) {
        oo_emit_usage(w, site, field->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                      CBM_RESOLVE_RULE_TYPE_MEMBER, "java-field-lookup");
        oo_emit_rw(w, site, field->graph_qn, "java-field-lookup");
    }
    oo_scope_t scope = oo_scope_of_field(field);
    return oo_value_from_ref(w, field->ref, &scope, subst);
}

static oo_value_t jw_identifier(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    const char *name = j_text(&j->text, node);
    if (!name)
        return oo_value_unknown();
    const oo_local_t *local = oo_lookup_local(w, name);
    if (local)
        return local->value;
    /* JLS 6.5.2: a variable in scope first; a field of any enclosing class. */
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
        if (field)
            return jw_field_value(j, node, field, subst);
        if (incomplete) {
            obscurable = true;
            break;
        }
    }
    /* An unindexed supertype cannot list its fields, so a variable could in
     * principle obscure this name. A field spelled like a type breaks JLS 6.1's
     * naming convention, so only a name of that shape may still read as a type,
     * and only one that resolves exactly by import or scope. */
    if (obscurable && !oo_type_shaped(name))
        return oo_value_unknown();
    /* Static field imports. */
    for (int i = 0; i < w->f->import_count; i++) {
        const oo_import_t *import = &w->f->imports[i];
        if (!import->statik)
            continue;
        if (!import->star && (!import->local || strcmp(import->local, name)))
            continue;
        const char *owner_qn = import->star
                                   ? import->path
                                   : cbm_arena_strndup(w->arena, import->path,
                                                       strlen(import->path) - strlen(name) - 1);
        const oo_type_t *owner = owner_qn ? oo_lookup_type(w->ix, owner_qn) : NULL;
        if (!owner)
            continue;
        oo_value_t receiver = oo_value_of_type(owner);
        oo_subst_t *subst = NULL;
        bool incomplete = false;
        const oo_field_t *field = oo_find_field(w, &receiver, name, &subst, &incomplete);
        if (field)
            return jw_field_value(j, node, field, subst);
    }
    /* A type name: the receiver of a static member. */
    oo_typeref_t ref = {.name = name};
    oo_value_t value = jw_ref(j, &ref);
    if (value.type || value.external)
        value.classref = true;
    if (value.type && value.type->graph_qn)
        oo_emit_usage(w, node, value.type->graph_qn, CBM_RESOLVED_TYPE_REFERENCE,
                      CBM_RESOLVE_RULE_DECLARED_TYPE, "java-type-name-lookup");
    return value;
}

static oo_value_t jw_field_access(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    TSNode object = j_field(node, "object");
    TSNode field_node = j_field(node, "field");
    if (j_is(field_node, "this")) {
        /* Outer.this: the named enclosing class. */
        oo_value_t outer = jw_expr(j, object);
        outer.classref = false;
        return outer;
    }
    if (j_is(field_node, "super")) {
        /* Iface.super: that named direct supertype. */
        oo_value_t outer = jw_expr(j, object);
        outer.classref = true;
        return outer;
    }
    const char *name = j_text(&j->text, field_node);
    /* A dotted identifier chain can be a package-qualified type. */
    if (j_is(object, "field_access") || j_is(object, "identifier")) {
        const char *text = j_compact(&j->text, node);
        if (text) {
            size_t length = strcspn(text, ".");
            char *head = cbm_arena_strndup(w->arena, text, length);
            bool variable = head && oo_lookup_local(w, head);
            if (!variable) {
                for (const oo_receiver_t *r = w->receiver; r && !variable; r = r->parent) {
                    oo_subst_t *subst = NULL;
                    bool incomplete = false;
                    variable =
                        r->unknown ||
                        (r->value.type &&
                         (oo_find_field(w, &r->value, head, &subst, &incomplete) || incomplete));
                }
            }
            const oo_type_t *exact = variable ? NULL : oo_lookup_type(w->ix, text);
            if (exact && !(exact->flags & OO_F_AMBIGUOUS)) {
                oo_value_t value = oo_value_of_type(exact);
                value.classref = true;
                return value;
            }
        }
    }
    oo_value_t receiver = jw_expr(j, object);
    if (!receiver.type || receiver.dims)
        return oo_value_unknown();
    oo_subst_t *subst = NULL;
    bool incomplete = false;
    const oo_field_t *field = oo_find_field(w, &receiver, name, &subst, &incomplete);
    if (field)
        return jw_field_value(j, field_node, field, subst);
    if (receiver.classref) {
        const oo_type_t *nested = oo_find_nested(w->ix, receiver.type, name);
        if (nested && !(nested->flags & OO_F_AMBIGUOUS)) {
            oo_value_t value = oo_value_of_type(nested);
            value.classref = true;
            return value;
        }
    }
    return oo_value_unknown();
}

static oo_value_t jw_method_invocation(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    TSNode object = j_field(node, "object");
    TSNode name_node = j_field(node, "name");
    const char *name = j_text(&j->text, name_node);
    TSNode arguments = j_field(node, "arguments");
    if (!name) {
        jw_children(j, node);
        return oo_value_unknown();
    }
    if (ts_node_is_null(object)) {
        int argc = 0;
        const oo_value_t *args = jw_arguments(j, arguments, &argc);
        return jw_bare_call(j, node, name, argc, args);
    }
    if (j_is(object, "super")) {
        int argc = 0;
        const oo_value_t *args = jw_arguments(j, arguments, &argc);
        /* super.m() selects from the direct supertypes of the enclosing class. */
        const oo_value_t self = jw_this_value(w);
        if (!self.type)
            return oo_value_unknown();
        const oo_type_t *only = NULL;
        for (int i = 0; i < self.type->super_count; i++) {
            const oo_type_t *candidate = self.type->supers[i].resolved;
            if (!candidate)
                return oo_value_unknown();
            oo_value_t receiver = oo_value_of_type(candidate);
            if (!oo_has_method_named(w, &receiver, name))
                continue;
            if (only && only != candidate)
                return oo_value_unknown();
            only = candidate;
        }
        oo_value_t receiver = oo_value_of_type(only);
        return only ? jw_select_emit(j, node, &receiver, name, argc, args, false,
                                     "java-super-member")
                    : oo_value_unknown();
    }
    oo_value_t receiver = jw_expr(j, object);
    if (j_is(object, "field_access") && j_is(j_field(object, "field"), "super"))
        receiver.classref = false;
    int argc = 0;
    const oo_value_t *args = jw_arguments(j, arguments, &argc);
    if (!receiver.type)
        return oo_external_member_return(w, &receiver, name);
    return jw_select_emit(j, node, &receiver, name, argc, args, receiver.classref,
                          receiver.classref ? "java-static-member"
                                            : "java-declared-receiver-member");
}

static void jw_anonymous_body(jw_t *j, TSNode body, oo_value_t super_value) {
    oo_walk_t *w = j->w;
    oo_receiver_t *receiver = oo_receiver_push(w, super_value, false);
    if (!receiver)
        return;
    receiver->anonymous = true;
    uint32_t count = ts_node_named_child_count(body);
    const char **names = count ? cbm_arena_calloc(w->arena, count * sizeof(*names)) : NULL;
    int own = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (j_is(member, "method_declaration") && names) {
            const char *name = j_text(&j->text, j_field(member, "name"));
            if (name)
                names[own++] = name;
        }
    }
    receiver->own_names = names;
    receiver->own_count = own;
    /* An unresolved supertype means an unknowable member set. */
    if (!super_value.type)
        receiver->unknown = true;
    jw_class_body(j, body, super_value.type);
    oo_receiver_pop(w);
}

static oo_value_t jw_object_creation(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    TSNode type_node = j_field(node, "type");
    TSNode arguments = j_field(node, "arguments");
    TSNode outer = {0};
    uint32_t named = ts_node_named_child_count(node);
    if (named && !ts_node_eq(ts_node_named_child(node, 0), type_node)) {
        TSNode first = ts_node_named_child(node, 0);
        if (!j_is(first, "type_arguments"))
            outer = first;
    }
    oo_value_t outer_value = ts_node_is_null(outer) ? oo_value_unknown() : jw_expr(j, outer);
    oo_value_t value = oo_value_unknown();
    if (!ts_node_is_null(outer)) {
        /* `outer.new Inner()` names a member class of outer's type. */
        const char *inner = j_compact(&j->text, type_node);
        const oo_type_t *nested =
            outer_value.type && inner ? oo_find_nested(w->ix, outer_value.type, inner) : NULL;
        if (nested && !(nested->flags & OO_F_AMBIGUOUS))
            value = oo_value_of_type(nested);
    } else {
        value = jw_ref(j, jw_type_at(j, type_node));
        jw_emit_type_mentions(j, type_node, 0);
    }
    int argc = 0;
    const oo_value_t *args = jw_arguments(j, arguments, &argc);
    TSNode body = j_child(node, "class_body");
    if (value.type && !ts_node_is_null(body) && (value.type->flags & OO_F_INTERFACE)) {
        /* An anonymous implementation of an interface has only Object's constructor. */
    } else if (value.type) {
        oo_select_t selected = oo_select_constructor(w, &value, argc, args);
        if (selected.status == OO_SEL_UNIQUE && selected.callable->graph_qn)
            oo_emit_call(w, node, selected.callable->graph_qn, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                         CBM_RESOLVE_FACT_LANGUAGE_BOUND, "java-constructor-selection");
        else if (selected.status == OO_SEL_NONE && value.type->graph_qn &&
                 !(value.type->flags & OO_F_INTERFACE))
            oo_emit_call(w, node, value.type->graph_qn, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                         CBM_RESOLVE_FACT_LANGUAGE_BOUND | CBM_RESOLVE_FACT_CLASS_CALLABLE,
                         "java-implicit-default-constructor");
    }
    if (!ts_node_is_null(body))
        jw_anonymous_body(j, body, value);
    return value;
}

static oo_value_t jw_local_variable(jw_t *j, TSNode node) {
    TSNode type = j_field(node, "type");
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode declarator = ts_node_named_child(node, i);
        if (!j_is(declarator, "variable_declarator"))
            continue;
        TSNode value_node = j_field(declarator, "value");
        oo_value_t initial =
            ts_node_is_null(value_node) ? oo_value_unknown() : jw_expr(j, value_node);
        jw_bind_declared(j, j_text(&j->text, j_field(declarator, "name")), type, initial,
                         j_dimensions(j_field(declarator, "dimensions")));
    }
    return oo_value_unknown();
}

/* Pattern variables can outlive their test by flow scoping, in a negated
 * guard. Nothing here tracks definite matching, so after such a statement the
 * name is left shadowed and unknown: never a stale type, never the field. */
static void jw_collect_pattern_names(jw_t *j, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 64)
        return;
    if (j_is(node, "instanceof_expression")) {
        TSNode name = j_field(node, "name");
        if (!ts_node_is_null(name))
            oo_bind(j->w, j_text(&j->text, name), oo_value_unknown());
    } else if (j_is(node, "type_pattern") || j_is(node, "record_pattern_component")) {
        uint32_t count = ts_node_named_child_count(node);
        if (count) {
            TSNode last = ts_node_named_child(node, count - 1);
            if (j_is(last, "identifier"))
                oo_bind(j->w, j_text(&j->text, last), oo_value_unknown());
        }
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        jw_collect_pattern_names(j, ts_node_named_child(node, i), depth + 1);
}

static oo_value_t jw_block_like(jw_t *j, TSNode node) {
    oo_frame_t frame;
    oo_frame_push(j->w, &frame);
    jw_children(j, node);
    oo_frame_pop(j->w);
    return oo_value_unknown();
}

static oo_value_t jw_conditional_statement(jw_t *j, TSNode node) {
    oo_frame_t frame;
    oo_frame_push(j->w, &frame);
    jw_children(j, node);
    oo_frame_pop(j->w);
    jw_collect_pattern_names(j, j_field(node, "condition"), 0);
    return oo_value_unknown();
}

static oo_value_t jw_lambda(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    TSNode parameters = j_field(node, "parameters");
    if (j_is(parameters, "identifier"))
        oo_bind(w, j_text(&j->text, parameters), oo_value_unknown());
    else
        jw_parameters(j, parameters);
    (void)jw_expr(j, j_field(node, "body"));
    oo_frame_pop(w);
    return oo_value_unknown();
}

static oo_value_t jw_enhanced_for(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    oo_frame_t frame;
    oo_value_t iterable = jw_expr(j, j_field(node, "value"));
    oo_frame_push(w, &frame);
    jw_bind_declared(j, j_text(&j->text, j_field(node, "name")), j_field(node, "type"),
                     oo_element_of(w, &iterable), j_dimensions(j_field(node, "dimensions")));
    (void)jw_expr(j, j_field(node, "body"));
    oo_frame_pop(w);
    return oo_value_unknown();
}

static oo_value_t jw_catch(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    TSNode parameter = j_child(node, "catch_formal_parameter");
    TSNode types = j_child(parameter, "catch_type");
    const char *name = j_text(&j->text, j_field(parameter, "name"));
    /* A multi-catch variable has the least upper bound, which is not computed. */
    if (!ts_node_is_null(types) && ts_node_named_child_count(types) == 1)
        jw_bind_declared(j, name, ts_node_named_child(types, 0), oo_value_unknown(), 0);
    else {
        jw_emit_type_mentions(j, types, 0);
        oo_bind(w, name, oo_value_unknown());
    }
    (void)jw_expr(j, j_field(node, "body"));
    oo_frame_pop(w);
    return oo_value_unknown();
}

static oo_value_t jw_literal(const char *kind) {
    if (!strcmp(kind, "string_literal"))
        return oo_value_external("String");
    if (!strcmp(kind, "decimal_integer_literal") || !strcmp(kind, "hex_integer_literal") ||
        !strcmp(kind, "octal_integer_literal") || !strcmp(kind, "binary_integer_literal"))
        return oo_value_external("int");
    if (!strcmp(kind, "decimal_floating_point_literal") ||
        !strcmp(kind, "hex_floating_point_literal"))
        return oo_value_external("double");
    if (!strcmp(kind, "true") || !strcmp(kind, "false"))
        return oo_value_external("boolean");
    if (!strcmp(kind, "character_literal"))
        return oo_value_external("char");
    return oo_value_unknown();
}

/* `x::m` names a declaration without calling it. The occurrence is its name. */
static oo_value_t jw_method_reference(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    uint32_t count = ts_node_named_child_count(node);
    TSNode object = count ? ts_node_named_child(node, 0) : (TSNode){0};
    TSNode member = count > 1 ? ts_node_named_child(node, count - 1) : (TSNode){0};
    oo_value_t receiver = oo_value_unknown();
    if (j_is(object, "this") || j_is(object, "super"))
        receiver = jw_this_value(w);
    else if (j_is(object, "type_identifier") || j_is(object, "scoped_type_identifier") ||
             j_is(object, "generic_type")) {
        receiver = jw_ref(j, jw_type_at(j, object));
        receiver.classref = true;
    } else
        receiver = jw_expr(j, object);
    if (!receiver.type || !j_is(member, "identifier"))
        return oo_value_unknown();
    const char *name = j_text(&j->text, member);
    /* No arity is known here. Every declaration of the name competes. */
    oo_select_t selected = oo_select_method(w, &receiver, name, -1, NULL, false);
    if (selected.status == OO_SEL_UNIQUE && selected.callable->graph_qn)
        oo_emit_usage(w, member, selected.callable->graph_qn, CBM_RESOLVED_CALL_REFERENCE,
                      CBM_RESOLVE_RULE_TYPE_MEMBER, "java-method-reference");
    return oo_value_unknown();
}

static oo_value_t jw_type_pattern(jw_t *j, TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    TSNode type = {0}, name = {0};
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (j_is(child, "identifier"))
            name = child;
        else if (j_is(child, "record_pattern"))
            (void)jw_expr(j, child);
        else if (!j_is(child, "modifiers"))
            type = child;
    }
    if (!ts_node_is_null(name))
        jw_bind_declared(j, j_text(&j->text, name), type, oo_value_unknown(), 0);
    return oo_value_unknown();
}

/* A class declared in a method has no index entry; it reads like an anonymous
 * class, and its name shadows every other type of that spelling. */
static void jw_local_class(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    const char *name = j_text(&j->text, j_field(node, "name"));
    if (name)
        oo_bind(w, cbm_arena_sprintf(w->arena, "#type:%s", name), oo_value_unknown());
    TSNode superclass = j_field(node, "superclass");
    TSNode interfaces = j_field(node, "interfaces");
    oo_value_t super_value = oo_value_unknown();
    bool knowable =
        ts_node_is_null(interfaces) && ts_node_is_null(j_child(node, "extends_interfaces"));
    if (!ts_node_is_null(superclass) && ts_node_named_child_count(superclass)) {
        super_value = jw_ref(j, jw_type_at(j, ts_node_named_child(superclass, 0)));
        knowable = knowable && super_value.type;
    }
    TSNode body = j_field(node, "body");
    oo_receiver_t *receiver = oo_receiver_push(w, super_value, false);
    if (!receiver)
        return;
    receiver->anonymous = true;
    receiver->unknown = !knowable;
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    const char **names = count ? cbm_arena_calloc(w->arena, count * sizeof(*names)) : NULL;
    int own = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (j_is(member, "method_declaration") && names) {
            const char *member_name = j_text(&j->text, j_field(member, "name"));
            if (member_name)
                names[own++] = member_name;
        }
    }
    receiver->own_names = names;
    receiver->own_count = own;
    jw_class_body(j, body, super_value.type);
    oo_receiver_pop(w);
}

static oo_value_t jw_expr_kind(jw_t *j, TSNode node, const char *kind) {
    oo_walk_t *w = j->w;
    switch (kind[0]) {
    case 'i':
        if (!strcmp(kind, "identifier"))
            return jw_identifier(j, node);
        if (!strcmp(kind, "if_statement"))
            return jw_conditional_statement(j, node);
        if (!strcmp(kind, "instanceof_expression")) {
            (void)jw_expr(j, j_field(node, "left"));
            TSNode right = j_field(node, "right");
            TSNode name = j_field(node, "name");
            if (!ts_node_is_null(name))
                jw_bind_declared(j, j_text(&j->text, name), right, oo_value_unknown(), 0);
            else if (j_is(right, "record_pattern"))
                (void)jw_expr(j, right);
            else
                jw_emit_type_mentions(j, right, 0);
            TSNode pattern = j_field(node, "pattern");
            if (!ts_node_is_null(pattern))
                (void)jw_expr(j, pattern);
            return oo_value_unknown();
        }
        if (!strcmp(kind, "interface_declaration")) {
            jw_local_class(j, node);
            return oo_value_unknown();
        }
        break;
    case 'm':
        if (!strcmp(kind, "method_invocation"))
            return jw_method_invocation(j, node);
        if (!strcmp(kind, "method_reference"))
            return jw_method_reference(j, node);
        if (!strcmp(kind, "marker_annotation"))
            return oo_value_unknown();
        break;
    case 'f':
        if (!strcmp(kind, "field_access"))
            return jw_field_access(j, node);
        if (!strcmp(kind, "for_statement"))
            return jw_block_like(j, node);
        break;
    case 'o':
        if (!strcmp(kind, "object_creation_expression"))
            return jw_object_creation(j, node);
        break;
    case 't':
        if (!strcmp(kind, "this"))
            return jw_this_value(w);
        if (!strcmp(kind, "ternary_expression")) {
            (void)jw_expr(j, j_field(node, "condition"));
            TSNode first_node = j_field(node, "consequence");
            TSNode second_node = j_field(node, "alternative");
            oo_value_t first = jw_expr(j, first_node);
            oo_value_t second = jw_expr(j, second_node);
            if (j_is(second_node, "null_literal"))
                return first;
            if (j_is(first_node, "null_literal"))
                return second;
            return first.type && first.type == second.type ? first : oo_value_unknown();
        }
        if (!strcmp(kind, "try_with_resources_statement"))
            return jw_block_like(j, node);
        if (!strcmp(kind, "type_pattern"))
            return jw_type_pattern(j, node);
        if (!strcmp(kind, "true"))
            return jw_literal(kind);
        break;
    case 'p':
        if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
            return jw_expr(j, ts_node_named_child(node, 0));
        break;
    case 'c':
        if (!strcmp(kind, "cast_expression")) {
            (void)jw_expr(j, j_field(node, "value"));
            TSNode type = j_field(node, "type");
            jw_emit_type_mentions(j, type, 0);
            return jw_ref(j, jw_type_at(j, type));
        }
        if (!strcmp(kind, "catch_clause"))
            return jw_catch(j, node);
        if (!strcmp(kind, "class_declaration")) {
            jw_local_class(j, node);
            return oo_value_unknown();
        }
        if (!strcmp(kind, "constructor_body"))
            return jw_block_like(j, node);
        if (!strcmp(kind, "character_literal"))
            return jw_literal(kind);
        break;
    case 'a':
        if (!strcmp(kind, "assignment_expression")) {
            oo_value_t left = jw_expr(j, j_field(node, "left"));
            (void)jw_expr(j, j_field(node, "right"));
            return left;
        }
        if (!strcmp(kind, "array_access")) {
            oo_value_t array = jw_expr(j, j_field(node, "array"));
            (void)jw_expr(j, j_field(node, "index"));
            if (array.dims > 0) {
                array.dims--;
                array.classref = false;
                return array;
            }
            return oo_value_unknown();
        }
        if (!strcmp(kind, "array_creation_expression")) {
            TSNode type = j_field(node, "type");
            jw_emit_type_mentions(j, type, 0);
            oo_value_t value = jw_ref(j, jw_type_at(j, type));
            uint32_t count = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < count; i++) {
                TSNode child = ts_node_named_child(node, i);
                if (j_is(child, "dimensions_expr")) {
                    value.dims++;
                    jw_children(j, child);
                } else if (j_is(child, "dimensions")) {
                    value.dims += j_dimensions(child);
                } else if (j_is(child, "array_initializer")) {
                    (void)jw_expr(j, child);
                }
            }
            return value;
        }
        if (!strcmp(kind, "annotation") || !strcmp(kind, "annotation_type_declaration"))
            return oo_value_unknown();
        break;
    case 'b':
        if (!strcmp(kind, "block"))
            return jw_block_like(j, node);
        break;
    case 'd':
        if (!strcmp(kind, "do_statement"))
            return jw_conditional_statement(j, node);
        if (!strcmp(kind, "decimal_integer_literal") ||
            !strcmp(kind, "decimal_floating_point_literal"))
            return jw_literal(kind);
        break;
    case 'e':
        if (!strcmp(kind, "enhanced_for_statement"))
            return jw_enhanced_for(j, node);
        if (!strcmp(kind, "enum_declaration")) {
            jw_local_class(j, node);
            return oo_value_unknown();
        }
        break;
    case 'l':
        if (!strcmp(kind, "local_variable_declaration"))
            return jw_local_variable(j, node);
        if (!strcmp(kind, "lambda_expression"))
            return jw_lambda(j, node);
        if (!strcmp(kind, "line_comment"))
            return oo_value_unknown();
        break;
    case 'r':
        if (!strcmp(kind, "resource")) {
            TSNode name = j_field(node, "name");
            if (ts_node_is_null(name))
                return jw_expr(j, ts_node_named_child(node, 0));
            oo_value_t initial = jw_expr(j, j_field(node, "value"));
            jw_bind_declared(j, j_text(&j->text, name), j_field(node, "type"), initial, 0);
            return oo_value_unknown();
        }
        if (!strcmp(kind, "record_declaration")) {
            jw_local_class(j, node);
            return oo_value_unknown();
        }
        if (!strcmp(kind, "record_pattern_component"))
            return jw_type_pattern(j, node);
        break;
    case 's':
        if (!strcmp(kind, "string_literal"))
            return jw_literal(kind);
        if (!strcmp(kind, "switch_block") || !strcmp(kind, "switch_rule") ||
            !strcmp(kind, "switch_block_statement_group") ||
            !strcmp(kind, "synchronized_statement"))
            return jw_block_like(j, node);
        if (!strcmp(kind, "scoped_type_identifier"))
            return oo_value_unknown();
        break;
    case 'w':
        if (!strcmp(kind, "while_statement"))
            return jw_conditional_statement(j, node);
        break;
    case 'h':
        if (!strcmp(kind, "hex_integer_literal") || !strcmp(kind, "hex_floating_point_literal"))
            return jw_literal(kind);
        break;
    default:
        break;
    }
    if (!strcmp(kind, "false") || !strcmp(kind, "octal_integer_literal") ||
        !strcmp(kind, "binary_integer_literal"))
        return jw_literal(kind);
    if (!strcmp(kind, "null_literal") || !strcmp(kind, "block_comment"))
        return oo_value_unknown();
    if (!strcmp(kind, "explicit_constructor_invocation")) {
        /* super(...) and this(...) only need their arguments walked. */
        (void)jw_expr(j, j_field(node, "object"));
        int argc = 0;
        (void)jw_arguments(j, j_field(node, "arguments"), &argc);
        return oo_value_unknown();
    }
    jw_children(j, node);
    return oo_value_unknown();
}

enum { JW_MAX_AST_DEPTH = 384 };

static oo_value_t jw_expr(jw_t *j, TSNode node) {
    if (ts_node_is_null(node) || !oo_budget(j->w))
        return oo_value_unknown();
    /* A pathological nesting depth stops descending: nothing below is resolved,
     * which only loses edges. */
    if (j->depth >= JW_MAX_AST_DEPTH)
        return oo_value_unknown();
    j->depth++;
    oo_value_t value = jw_expr_kind(j, node, ts_node_type(node));
    j->depth--;
    return value;
}

static void jw_method(jw_t *j, TSNode node) {
    oo_walk_t *w = j->w;
    oo_scope_t saved = w->scope;
    w->scope.tparams =
        j_tparams(&j->text, j_field(node, "type_parameters"), &w->scope.tparam_count);
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    jw_emit_type_mentions(j, j_field(node, "type"), 0);
    if (j_is(node, "compact_constructor_declaration")) {
        /* The header's components are this constructor's parameters. */
        TSNode record = ts_node_parent(ts_node_parent(node));
        jw_parameters(j, j_field(record, "parameters"));
    } else {
        jw_parameters(j, j_field(node, "parameters"));
    }
    (void)jw_expr(j, j_field(node, "body"));
    oo_frame_pop(w);
    w->scope = saved;
}

static void jw_field_declaration(jw_t *j, TSNode node) {
    jw_emit_type_mentions(j, j_field(node, "type"), 0);
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode declarator = ts_node_named_child(node, i);
        if (j_is(declarator, "variable_declarator"))
            (void)jw_expr(j, j_field(declarator, "value"));
    }
}

static void jw_class_body(jw_t *j, TSNode body, const oo_type_t *type) {
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (j_is(member, "method_declaration") || j_is(member, "constructor_declaration") ||
            j_is(member, "compact_constructor_declaration")) {
            jw_method(j, member);
        } else if (j_is(member, "field_declaration") || j_is(member, "constant_declaration")) {
            jw_field_declaration(j, member);
        } else if (j_is(member, "static_initializer") || j_is(member, "block")) {
            oo_frame_t frame;
            oo_frame_push(j->w, &frame);
            jw_children(j, member);
            oo_frame_pop(j->w);
        } else if (j_is(member, "enum_constant")) {
            int argc = 0;
            (void)jw_arguments(j, j_field(member, "arguments"), &argc);
            TSNode constant_body = j_field(member, "body");
            if (!ts_node_is_null(constant_body))
                jw_anonymous_body(j, constant_body, oo_value_of_type(type));
        } else if (j_is(member, "enum_body_declarations")) {
            jw_class_body(j, member, type);
        } else if (j_type_decl(member)) {
            jw_type_decl(j, member, type);
        }
    }
}

static void jw_type_decl(jw_t *j, TSNode node, const oo_type_t *owner) {
    oo_walk_t *w = j->w;
    const char *name = j_text(&j->text, j_field(node, "name"));
    if (!name)
        return;
    const char *qn =
        owner ? j_join(w->arena, owner->qn, name) : j_join(w->arena, w->f->package, name);
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
        jw_emit_type_mentions(j, j_field(node, "superclass"), 0);
        jw_emit_type_mentions(j, j_field(node, "interfaces"), 0);
        jw_emit_type_mentions(j, j_child(node, "extends_interfaces"), 0);
        jw_class_body(j, j_field(node, "body"), usable ? type : NULL);
        oo_receiver_pop(w);
    }
    w->scope = saved;
}

void oo_java_resolve(oo_walk_t *w, TSNode root) {
    jw_t j = {.w = w, .text = {.arena = w->arena, .source = w->source, .length = w->length}};
    const char *package = "";
    oo_import_t *imports = NULL;
    int import_count = 0;
    j_read_imports(&j.text, root, &imports, &import_count, &package);
    oo_file_t file = {.rel = "",
                      .package = package,
                      .language = CBM_LANG_JAVA,
                      .imports = imports,
                      .import_count = import_count,
                      .has_error = ts_node_has_error(root)};
    w->f = &file;
    w->scope = (oo_scope_t){.file = &file, .ns = ""};
    uint32_t count = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(root, i);
        if (j_type_decl(child))
            jw_type_decl(&j, child, NULL);
    }
    w->f = NULL;
}
