#include "oo_internal.h"
#include <stdio.h>
#include <string.h>

/* Kotlin adapter. Overload resolution follows the Kotlin specification's scope
 * levels: local declarations, then members of the implicit receivers from the
 * innermost out, then explicit imports, the package, star imports, and finally
 * the default imports (which hold no project declaration). A member beats an
 * extension of the same name when it is applicable.
 *
 * A lambda whose expected type cannot be read may have an implicit receiver.
 * Its member set is unknown, so a bare name inside abstains. */

typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length;
} kt_text_t;

static bool kt_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}

static TSNode kt_child(TSNode node, const char *kind) {
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (kt_is(child, kind))
            return child;
    }
    return (TSNode){0};
}

static const char *kt_text(const kt_text_t *t, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= t->length
               ? cbm_arena_strndup(t->arena, t->source + start, end - start)
               : NULL;
}

/* Backticks are part of the source spelling, never of the name. */
static const char *kt_name(const kt_text_t *t, TSNode node) {
    const char *text = kt_text(t, node);
    if (!text)
        return NULL;
    size_t length = strlen(text);
    if (length >= 2 && text[0] == '`' && text[length - 1] == '`')
        return cbm_arena_strndup(t->arena, text + 1, length - 2);
    return text;
}

static const char *kt_compact(const kt_text_t *t, TSNode node) {
    const char *text = kt_text(t, node);
    if (!text)
        return NULL;
    size_t length = strlen(text);
    char *out = cbm_arena_alloc(t->arena, length + 1);
    if (!out)
        return NULL;
    size_t n = 0;
    for (size_t i = 0; i < length; i++)
        if (text[i] != ' ' && text[i] != '\t' && text[i] != '\n' && text[i] != '\r' &&
            text[i] != '`')
            out[n++] = text[i];
    out[n] = '\0';
    return out;
}

static const char *kt_join(CBMArena *arena, const char *prefix, const char *name) {
    return prefix && prefix[0] ? cbm_arena_sprintf(arena, "%s.%s", prefix, name)
                               : cbm_arena_strdup(arena, name);
}

static const oo_typeref_t *kt_typeref(const kt_text_t *t, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "nullable_type") || !strcmp(kind, "non_nullable_type") ||
        !strcmp(kind, "parenthesized_type") || !strcmp(kind, "receiver_type") ||
        !strcmp(kind, "type_projection") || !strcmp(kind, "not_nullable_type")) {
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (!kt_is(child, "type_modifiers") && !kt_is(child, "variance_modifier") &&
                !kt_is(child, "type_projection_modifiers") && !kt_is(child, "quest"))
                return kt_typeref(t, child);
        }
        return cbm_arena_calloc(t->arena, sizeof(oo_typeref_t));
    }
    oo_typeref_t *ref = cbm_arena_calloc(t->arena, sizeof(*ref));
    if (!ref)
        return NULL;
    if (!strcmp(kind, "user_type")) {
        /* Segments joined by dots; the type arguments of the last segment. */
        char buffer[512];
        size_t used = 0;
        TSNode arguments = {0};
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (kt_is(child, "type_identifier")) {
                const char *segment = kt_name(t, child);
                size_t length = segment ? strlen(segment) : 0;
                if (!length || used + length + 2 >= sizeof(buffer))
                    return ref;
                if (used)
                    buffer[used++] = '.';
                memcpy(buffer + used, segment, length);
                used += length;
                arguments = (TSNode){0};
            } else if (kt_is(child, "type_arguments")) {
                arguments = child;
            }
        }
        buffer[used] = '\0';
        ref->name = used ? cbm_arena_strdup(t->arena, buffer) : NULL;
        uint32_t arg_count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
        if (arg_count) {
            const oo_typeref_t **args = cbm_arena_calloc(t->arena, (arg_count + 1) * sizeof(*args));
            if (args) {
                for (uint32_t i = 0; i < arg_count; i++) {
                    args[i] = kt_typeref(t, ts_node_named_child(arguments, i));
                    if (!args[i])
                        args[i] = cbm_arena_calloc(t->arena, sizeof(oo_typeref_t));
                }
                ref->args = args;
            }
        }
    } else if (!strcmp(kind, "function_type")) {
        ref->function = true;
        TSNode receiver = ts_node_child_by_field_name(node, "receiver", 8);
        if (!ts_node_is_null(receiver))
            ref->fn_receiver = kt_typeref(t, receiver);
        uint32_t count = ts_node_named_child_count(node);
        TSNode parameters = kt_child(node, "function_type_parameters");
        for (uint32_t i = count; i > 0; i--) {
            TSNode child = ts_node_named_child(node, i - 1);
            if (!kt_is(child, "function_type_parameters") && !kt_is(child, "receiver_type")) {
                ref->fn_return = kt_typeref(t, child);
                break;
            }
        }
        uint32_t params = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
        if (params) {
            const oo_typeref_t **args = cbm_arena_calloc(t->arena, (params + 1) * sizeof(*args));
            if (args) {
                int n = 0;
                for (uint32_t i = 0; i < params; i++) {
                    TSNode child = ts_node_named_child(parameters, i);
                    /* A named parameter: `(id: Int) -> Unit`. */
                    if (kt_is(child, "parameter")) {
                        uint32_t parts = ts_node_named_child_count(child);
                        child = parts ? ts_node_named_child(child, parts - 1) : child;
                    }
                    args[n] = kt_typeref(t, child);
                    if (!args[n])
                        args[n] = cbm_arena_calloc(t->arena, sizeof(oo_typeref_t));
                    n++;
                }
                ref->args = args;
            }
        }
    }
    return ref;
}

static oo_tparam_t *kt_tparams(const kt_text_t *t, TSNode owner, int *out_count) {
    *out_count = 0;
    TSNode parameters = kt_child(owner, "type_parameters");
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    if (!count)
        return NULL;
    oo_tparam_t *items = cbm_arena_calloc(t->arena, count * sizeof(*items));
    if (!items)
        return NULL;
    int n = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (!kt_is(parameter, "type_parameter"))
            continue;
        TSNode name = kt_child(parameter, "type_identifier");
        items[n].name = kt_name(t, name);
        uint32_t children = ts_node_named_child_count(parameter);
        for (uint32_t c = 0; c < children; c++) {
            TSNode child = ts_node_named_child(parameter, c);
            if (!kt_is(child, "type_identifier") && !kt_is(child, "type_parameter_modifiers"))
                items[n].bound = kt_typeref(t, child);
        }
        /* `where T : X`: the constraint reads like a bound of T. */
        if (items[n].name)
            n++;
    }
    TSNode constraints = kt_child(owner, "type_constraints");
    uint32_t constraint_count =
        ts_node_is_null(constraints) ? 0 : ts_node_named_child_count(constraints);
    for (uint32_t i = 0; i < constraint_count; i++) {
        TSNode constraint = ts_node_named_child(constraints, i);
        if (!kt_is(constraint, "type_constraint"))
            continue;
        const char *name = kt_name(t, kt_child(constraint, "type_identifier"));
        uint32_t parts = ts_node_named_child_count(constraint);
        TSNode bound = parts ? ts_node_named_child(constraint, parts - 1) : (TSNode){0};
        for (int k = 0; name && k < n; k++)
            if (!strcmp(items[k].name, name) && !items[k].bound)
                items[k].bound = kt_typeref(t, bound);
    }
    *out_count = n;
    return items;
}

enum {
    KT_MOD_PRIVATE = 1,
    KT_MOD_OVERRIDE = 2,
    KT_MOD_INNER = 4,
    KT_MOD_ABSTRACT = 8,
    KT_MOD_DATA = 16
};

static unsigned kt_modifiers(const kt_text_t *t, TSNode node) {
    unsigned flags = 0;
    TSNode modifiers = kt_child(node, "modifiers");
    uint32_t count = ts_node_is_null(modifiers) ? 0 : ts_node_named_child_count(modifiers);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(modifiers, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "annotation"))
            continue;
        const char *text = kt_text(t, child);
        if (!text)
            continue;
        if (!strcmp(text, "private"))
            flags |= KT_MOD_PRIVATE;
        else if (!strcmp(text, "override"))
            flags |= KT_MOD_OVERRIDE;
        else if (!strcmp(text, "inner"))
            flags |= KT_MOD_INNER;
        else if (!strcmp(text, "abstract"))
            flags |= KT_MOD_ABSTRACT;
        else if (!strcmp(text, "data"))
            flags |= KT_MOD_DATA;
    }
    return flags;
}

/* The direct named child that is the declared type: the one following ':'. */
static TSNode kt_after_colon(TSNode node, const char *stop_a, const char *stop_b) {
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_child_count(node);
    bool seen = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (!ts_node_is_named(child)) {
            if (!strcmp(ts_node_type(child), ":"))
                seen = true;
            continue;
        }
        if (kt_is(child, stop_a) || (stop_b && kt_is(child, stop_b)))
            break;
        /* `suspend` and annotations stand in front of the type itself. */
        if (seen && !kt_is(child, "type_modifiers"))
            return child;
    }
    return (TSNode){0};
}

/* ── Expression summaries ───────────────────────────────────────────── */

static int kt_call_argc(TSNode suffix);

static bool kt_sum_steps(const kt_text_t *t, TSNode node, oo_sumstep_t *steps, int *count,
                         int capacity, unsigned depth) {
    if (ts_node_is_null(node) || depth > 16 || *count >= capacity)
        return false;
    if (kt_is(node, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return kt_sum_steps(t, ts_node_named_child(node, 0), steps, count, capacity, depth + 1);
    if (kt_is(node, "postfix_expression") && ts_node_named_child_count(node) == 1) {
        const char *text = kt_text(t, node);
        size_t length = text ? strlen(text) : 0;
        /* `x!!` keeps x's type; `x++` is not a chain. */
        if (length < 2 || strcmp(text + length - 2, "!!"))
            return false;
        return kt_sum_steps(t, ts_node_named_child(node, 0), steps, count, capacity, depth + 1);
    }
    if (kt_is(node, "simple_identifier")) {
        const char *name = kt_name(t, node);
        if (!name)
            return false;
        steps[(*count)++] = (oo_sumstep_t){.name = name, .argc = -1, .kind = OO_SUM_NAME};
        return true;
    }
    if (kt_is(node, "navigation_expression")) {
        TSNode suffix = kt_child(node, "navigation_suffix");
        const char *member = kt_name(t, kt_child(suffix, "simple_identifier"));
        if (!member || !ts_node_named_child_count(node) ||
            !kt_sum_steps(t, ts_node_named_child(node, 0), steps, count, capacity, depth + 1) ||
            *count >= capacity)
            return false;
        steps[(*count)++] = (oo_sumstep_t){.name = member, .argc = -1, .kind = OO_SUM_MEMBER};
        return true;
    }
    if (kt_is(node, "call_expression")) {
        TSNode callee = ts_node_named_child(node, 0);
        TSNode suffix = kt_child(node, "call_suffix");
        if (ts_node_is_null(suffix) || !kt_sum_steps(t, callee, steps, count, capacity, depth + 1))
            return false;
        steps[*count - 1].argc = kt_call_argc(suffix);
        TSNode type_args = kt_child(suffix, "type_arguments");
        uint32_t arg_count = ts_node_is_null(type_args) ? 0 : ts_node_named_child_count(type_args);
        if (arg_count) {
            const oo_typeref_t **args = cbm_arena_calloc(t->arena, (arg_count + 1) * sizeof(*args));
            if (args) {
                for (uint32_t i = 0; i < arg_count; i++) {
                    args[i] = kt_typeref(t, ts_node_named_child(type_args, i));
                    if (!args[i])
                        args[i] = cbm_arena_calloc(t->arena, sizeof(oo_typeref_t));
                }
                steps[*count - 1].type_args = args;
            }
        }
        return true;
    }
    return false;
}

static const oo_exprsum_t *kt_summary(const kt_text_t *t, TSNode node) {
    oo_sumstep_t steps[12];
    int count = 0;
    if (!kt_sum_steps(t, node, steps, &count, 12, 0) || !count)
        return NULL;
    oo_exprsum_t *summary = cbm_arena_alloc(t->arena, sizeof(*summary));
    oo_sumstep_t *copy = cbm_arena_alloc(t->arena, (size_t)count * sizeof(*copy));
    if (!summary || !copy)
        return NULL;
    memcpy(copy, steps, (size_t)count * sizeof(*copy));
    summary->steps = copy;
    summary->count = count;
    return summary;
}

/* ── Harvest ────────────────────────────────────────────────────────── */

typedef struct {
    cbm_oo_index_t *ix;
    kt_text_t text;
    oo_defs_t defs;
    const oo_file_t *file;
} kh_t;

static uint32_t kt_line(TSNode node) {
    return ts_node_start_point(node).row + 1;
}

static void kh_parameters(const kh_t *h, oo_callable_t *callable, TSNode parameters) {
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_child_count(parameters);
    const oo_typeref_t **refs =
        count ? cbm_arena_calloc(h->text.arena, count * sizeof(*refs)) : NULL;
    const char **names = count ? cbm_arena_calloc(h->text.arena, count * sizeof(*names)) : NULL;
    int total = 0, optional = 0;
    bool vararg = false, pending_vararg = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(parameters, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "parameter_modifiers")) {
            const char *text = kt_text(&h->text, child);
            pending_vararg = text && strstr(text, "vararg");
        } else if (!strcmp(kind, "parameter") || !strcmp(kind, "class_parameter")) {
            uint32_t parts = ts_node_named_child_count(child);
            TSNode type = kt_after_colon(child, "=", NULL);
            if (ts_node_is_null(type) && parts)
                type = ts_node_named_child(child, parts - 1);
            if (refs)
                refs[total] = kt_typeref(&h->text, type);
            if (names)
                names[total] = kt_name(&h->text, kt_child(child, "simple_identifier"));
            total++;
            if (pending_vararg && !vararg)
                callable->vararg_position = total;
            vararg |= pending_vararg;
            pending_vararg = false;
            /* A class parameter keeps its default inside its own node. */
            uint32_t tokens = ts_node_child_count(child);
            for (uint32_t k = 0; k < tokens; k++)
                if (!strcmp(ts_node_type(ts_node_child(child, k)), "="))
                    optional++;
        } else if (!strcmp(kind, "=")) {
            optional++;
        }
    }
    callable->params = refs;
    callable->param_names = names;
    callable->param_count = total;
    callable->min_args = total - optional - (vararg ? 1 : 0);
    if (callable->min_args < 0)
        callable->min_args = 0;
    callable->max_args = vararg ? -1 : total;
}

static void kh_function(kh_t *h, TSNode node, oo_type_t *owner) {
    const char *name = kt_name(&h->text, kt_child(node, "simple_identifier"));
    if (!name)
        return;
    oo_callable_t *callable = cbm_arena_calloc(h->text.arena, sizeof(*callable));
    if (!callable)
        return;
    const CBMDefinition *def = oo_defs_find(&h->defs, 'C', kt_line(node), name);
    unsigned modifiers = kt_modifiers(&h->text, node);
    callable->name = name;
    callable->graph_qn = def ? def->qualified_name : NULL;
    callable->owner = owner;
    callable->file = h->file;
    callable->ns = owner ? owner->ns : h->file->package;
    if (modifiers & KT_MOD_PRIVATE)
        callable->flags |= OO_F_PRIVATE;
    if (modifiers & KT_MOD_OVERRIDE)
        callable->flags |= OO_F_OVERRIDE;
    if (modifiers & KT_MOD_ABSTRACT)
        callable->flags |= OO_F_ABSTRACT;
    callable->tparams = kt_tparams(&h->text, node, &callable->tparam_count);
    TSNode receiver = ts_node_child_by_field_name(node, "receiver", 8);
    if (!ts_node_is_null(receiver))
        callable->receiver = kt_typeref(&h->text, receiver);
    TSNode parameters = kt_child(node, "function_value_parameters");
    kh_parameters(h, callable, parameters);
    /* The return annotation follows the parameter list. */
    uint32_t count = ts_node_child_count(node);
    bool past = false, colon = false;
    TSNode body = kt_child(node, "function_body");
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (ts_node_eq(child, parameters)) {
            past = true;
        } else if (past && !strcmp(ts_node_type(child), ":")) {
            colon = true;
        } else if (past && colon && ts_node_is_named(child) && !kt_is(child, "function_body") &&
                   !kt_is(child, "type_constraints")) {
            callable->ret = kt_typeref(&h->text, child);
            break;
        }
    }
    if (!callable->ret && !ts_node_is_null(body)) {
        const char *text = kt_text(&h->text, body);
        if (text && text[0] == '=' && ts_node_named_child_count(body) == 1)
            callable->ret_expr = kt_summary(&h->text, ts_node_named_child(body, 0));
    }
    if (owner)
        oo_index_method(h->ix, owner, callable);
    else
        oo_index_function(h->ix, h->file->package, callable);
}

static void kh_property(kh_t *h, TSNode node, oo_type_t *owner) {
    TSNode variable = kt_child(node, "variable_declaration");
    if (ts_node_is_null(variable))
        return; /* A destructuring property has no single declared name. */
    const char *name = kt_name(&h->text, kt_child(variable, "simple_identifier"));
    if (!name)
        return;
    oo_field_t *field = cbm_arena_calloc(h->text.arena, sizeof(*field));
    if (!field)
        return;
    const CBMDefinition *def = oo_defs_find(&h->defs, 'F', kt_line(node), name);
    if (!def)
        def = oo_defs_find(&h->defs, 'F', kt_line(variable), name);
    unsigned modifiers = kt_modifiers(&h->text, node);
    field->name = name;
    field->graph_qn = def ? def->qualified_name : NULL;
    field->owner = owner;
    field->file = h->file;
    field->ns = owner ? owner->ns : h->file->package;
    if (modifiers & KT_MOD_PRIVATE)
        field->flags |= OO_F_PRIVATE;
    field->tparams = kt_tparams(&h->text, node, &field->tparam_count);
    TSNode receiver = ts_node_child_by_field_name(node, "receiver", 8);
    if (!ts_node_is_null(receiver))
        field->receiver = kt_typeref(&h->text, receiver);
    TSNode declared = kt_after_colon(variable, "=", NULL);
    if (!ts_node_is_null(declared)) {
        field->ref = kt_typeref(&h->text, declared);
    } else if (ts_node_is_null(kt_child(node, "property_delegate"))) {
        /* The initializer, the named child after the variable. */
        uint32_t count = ts_node_named_child_count(node);
        bool past = false;
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (ts_node_eq(child, variable)) {
                past = true;
                continue;
            }
            if (past && !kt_is(child, "getter") && !kt_is(child, "setter")) {
                field->init = kt_summary(&h->text, child);
                break;
            }
        }
    }
    if (owner)
        oo_index_field(h->ix, owner, field);
    else
        oo_index_prop(h->ix, h->file->package, field);
}

static void kh_type(kh_t *h, TSNode node, oo_type_t *owner);

static void kh_body(kh_t *h, oo_type_t *type, TSNode body) {
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        if (kt_is(member, "function_declaration")) {
            kh_function(h, member, type);
        } else if (kt_is(member, "property_declaration")) {
            kh_property(h, member, type);
        } else if (kt_is(member, "class_declaration") || kt_is(member, "object_declaration") ||
                   kt_is(member, "companion_object")) {
            kh_type(h, member, type);
        } else if (kt_is(member, "secondary_constructor")) {
            oo_callable_t *callable = cbm_arena_calloc(h->text.arena, sizeof(*callable));
            if (!callable)
                continue;
            const CBMDefinition *def = oo_defs_find(&h->defs, 'C', kt_line(member), type->name);
            callable->name = type->name;
            callable->graph_qn = def ? def->qualified_name : NULL;
            callable->owner = type;
            callable->file = h->file;
            callable->ns = type->ns;
            callable->flags = OO_F_CTOR;
            kh_parameters(h, callable, kt_child(member, "function_value_parameters"));
            oo_index_method(h->ix, type, callable);
        } else if (kt_is(member, "enum_entry")) {
            const char *name = kt_name(&h->text, kt_child(member, "simple_identifier"));
            oo_field_t *field = name ? cbm_arena_calloc(h->text.arena, sizeof(*field)) : NULL;
            oo_typeref_t *ref = cbm_arena_calloc(h->text.arena, sizeof(*ref));
            if (!field || !ref)
                continue;
            ref->name = type->name;
            const CBMDefinition *def = oo_defs_find(&h->defs, 'F', kt_line(member), name);
            *field = (oo_field_t){.name = name,
                                  .graph_qn = def ? def->qualified_name : NULL,
                                  .owner = type,
                                  .file = h->file,
                                  .ns = type->ns,
                                  .flags = OO_F_STATIC,
                                  .ref = ref};
            oo_index_field(h->ix, type, field);
        }
    }
}

static void kh_primary_constructor(kh_t *h, oo_type_t *type, TSNode primary) {
    oo_callable_t *callable = cbm_arena_calloc(h->text.arena, sizeof(*callable));
    if (!callable)
        return;
    callable->name = type->name;
    callable->owner = type;
    callable->file = h->file;
    callable->ns = type->ns;
    callable->flags = OO_F_CTOR;
    kh_parameters(h, callable, primary);
    oo_index_method(h->ix, type, callable);
    type->primary = callable;
    uint32_t count = ts_node_named_child_count(primary);
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(primary, i);
        if (!kt_is(parameter, "class_parameter") ||
            ts_node_is_null(kt_child(parameter, "binding_pattern_kind")))
            continue;
        const char *name = kt_name(&h->text, kt_child(parameter, "simple_identifier"));
        oo_field_t *field = name ? cbm_arena_calloc(h->text.arena, sizeof(*field)) : NULL;
        if (!field)
            continue;
        unsigned modifiers = kt_modifiers(&h->text, parameter);
        const CBMDefinition *def = oo_defs_find(&h->defs, 'F', kt_line(parameter), name);
        *field = (oo_field_t){.name = name,
                              .graph_qn = def ? def->qualified_name : NULL,
                              .owner = type,
                              .file = h->file,
                              .ns = type->ns,
                              .flags = (modifiers & KT_MOD_PRIVATE) ? OO_F_PRIVATE : 0u,
                              .ref = kt_typeref(&h->text, kt_after_colon(parameter, "=", NULL))};
        oo_index_field(h->ix, type, field);
    }
}

static void kh_supers(kh_t *h, oo_type_t *type, TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!kt_is(child, "delegation_specifier"))
            continue;
        TSNode constructor = kt_child(child, "constructor_invocation");
        TSNode user = !ts_node_is_null(constructor) ? kt_child(constructor, "user_type")
                                                    : kt_child(child, "user_type");
        if (!ts_node_is_null(user))
            oo_type_add_super(h->ix, type, kt_typeref(&h->text, user), h->file, type->ns);
        else {
            /* `by` delegation and other forms: the supertype set is not known. */
            TSNode explicit_delegation = kt_child(child, "explicit_delegation");
            TSNode inner = kt_child(explicit_delegation, "user_type");
            oo_typeref_t *unknown = cbm_arena_calloc(h->text.arena, sizeof(*unknown));
            oo_type_add_super(h->ix, type,
                              !ts_node_is_null(inner) ? kt_typeref(&h->text, inner) : unknown,
                              h->file, type->ns);
        }
    }
}

static void kh_type(kh_t *h, TSNode node, oo_type_t *owner) {
    bool companion = kt_is(node, "companion_object");
    TSNode name_node = kt_child(node, "type_identifier");
    const char *name = ts_node_is_null(name_node) ? (companion ? "Companion" : NULL)
                                                  : kt_name(&h->text, name_node);
    if (!name)
        return;
    const char *qn = owner ? kt_join(h->text.arena, owner->qn, name)
                           : kt_join(h->text.arena, h->file->package, name);
    if (!qn)
        return;
    uint32_t flags = 0;
    if (kt_is(node, "object_declaration"))
        flags |= OO_F_OBJECT;
    if (companion)
        flags |= OO_F_OBJECT | OO_F_COMPANION;
    if (kt_is(node, "class_declaration")) {
        if (!ts_node_is_null(kt_child(node, "interface")))
            flags |= OO_F_INTERFACE;
        if (!ts_node_is_null(kt_child(node, "enum")))
            flags |= OO_F_ENUM;
    }
    unsigned modifiers = kt_modifiers(&h->text, node);
    if (modifiers & KT_MOD_PRIVATE)
        flags |= OO_F_PRIVATE;
    if (modifiers & KT_MOD_INNER)
        flags |= OO_F_INNER;
    if (modifiers & KT_MOD_DATA)
        flags |= OO_F_DATA;
    if (oo_members_broken(node))
        flags |= OO_F_INCOMPLETE;
    const CBMDefinition *def = oo_defs_find(&h->defs, 'T', kt_line(node), name);
    oo_type_t *type = oo_index_type(h->ix, qn, name, h->file, h->file->package, owner,
                                    def ? def->qualified_name : NULL, flags);
    if (!type)
        return;
    type->tparams = kt_tparams(&h->text, node, &type->tparam_count);
    if (companion && owner)
        owner->companion = type;
    kh_supers(h, type, node);
    TSNode primary = kt_child(node, "primary_constructor");
    if (!ts_node_is_null(primary))
        kh_primary_constructor(h, type, primary);
    TSNode body = kt_child(node, "class_body");
    if (ts_node_is_null(body))
        body = kt_child(node, "enum_class_body");
    kh_body(h, type, body);
}

static void kh_top_level(kh_t *h, TSNode parent) {
    uint32_t count = ts_node_named_child_count(parent);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(parent, i);
        if (kt_is(child, "class_declaration") || kt_is(child, "object_declaration")) {
            kh_type(h, child, NULL);
        } else if (kt_is(child, "function_declaration")) {
            kh_function(h, child, NULL);
        } else if (kt_is(child, "property_declaration")) {
            kh_property(h, child, NULL);
        } else if (kt_is(child, "type_alias")) {
            const char *name = kt_name(&h->text, kt_child(child, "type_identifier"));
            uint32_t parts = ts_node_named_child_count(child);
            const char *qn = name ? kt_join(h->text.arena, h->file->package, name) : NULL;
            if (!qn || !parts)
                continue;
            oo_type_t *alias =
                oo_index_type(h->ix, qn, name, h->file, h->file->package, NULL, NULL, 0);
            if (alias) {
                alias->tparams = kt_tparams(&h->text, child, &alias->tparam_count);
                alias->alias_of = kt_typeref(&h->text, ts_node_named_child(child, parts - 1));
                alias->supers_complete = true;
            }
        }
    }
}

static void kt_read_imports(const kt_text_t *t, TSNode root, oo_import_t **out, int *out_count,
                            const char **package) {
    *package = "";
    *out = NULL;
    *out_count = 0;
    TSNode header = kt_child(root, "package_header");
    if (!ts_node_is_null(header)) {
        const char *name = kt_compact(t, kt_child(header, "identifier"));
        if (name)
            *package = name;
    }
    TSNode list = kt_child(root, "import_list");
    uint32_t count = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
    if (!count)
        return;
    oo_import_t *imports = cbm_arena_calloc(t->arena, count * sizeof(*imports));
    if (!imports)
        return;
    int n = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode import = ts_node_named_child(list, i);
        if (!kt_is(import, "import_header"))
            continue;
        const char *path = kt_compact(t, kt_child(import, "identifier"));
        if (!path)
            continue;
        bool star = !ts_node_is_null(kt_child(import, "wildcard_import"));
        TSNode alias = kt_child(import, "import_alias");
        const char *local = NULL;
        if (!star) {
            local = !ts_node_is_null(alias) ? kt_name(t, kt_child(alias, "type_identifier")) : NULL;
            if (!local) {
                const char *dot = strrchr(path, '.');
                local = dot ? dot + 1 : path;
            }
        }
        imports[n++] = (oo_import_t){.local = local, .path = path, .star = star};
    }
    *out = imports;
    *out_count = n;
}

bool oo_kotlin_harvest(cbm_oo_index_t *ix, const char *rel, const CBMFileResult *file,
                       const char *source, size_t length, TSNode root) {
    kh_t h = {.ix = ix, .text = {.arena = ix->arena, .source = source, .length = length}};
    if (!oo_defs_init(&h.defs, ix->arena, file))
        return false;
    oo_import_t *imports = NULL;
    int import_count = 0;
    const char *package = "";
    kt_read_imports(&h.text, root, &imports, &import_count, &package);
    h.file = oo_index_file(ix, CBM_LANG_KOTLIN, rel, package, imports, import_count,
                           ts_node_has_error(root));
    if (h.file)
        kh_top_level(&h, root);
    oo_defs_free(&h.defs);
    return h.file != NULL;
}

/* ── Selection independent of an AST position ───────────────────────── */

/* The levels of a free name below the implicit receivers. */
enum { KT_LEVEL_EXPLICIT = 0, KT_LEVEL_PACKAGE, KT_LEVEL_STAR, KT_LEVEL_COUNT };

typedef struct {
    const oo_callable_t *items[64];
    int count;
    bool overflow;
} kt_list_t;

static void kt_list_collect(kt_list_t *list, const oo_callable_t *head) {
    for (const oo_callable_t *c = head; c; c = c->next) {
        if (list->count == 64) {
            list->overflow = true;
            return;
        }
        list->items[list->count++] = c;
    }
}

/* Candidates of a free name at one import level, in the scope's file. */
static void kt_level_functions(oo_walk_t *w, const oo_scope_t *scope, const char *name, int level,
                               kt_list_t *out) {
    const oo_file_t *file = scope->file;
    if (level == KT_LEVEL_PACKAGE) {
        kt_list_collect(out, oo_lookup_functions(w->ix, file->package, name));
        return;
    }
    if (level == KT_LEVEL_STAR) {
        for (int i = 0; i < file->import_count; i++) {
            const oo_import_t *import = &file->imports[i];
            if (import->star)
                kt_list_collect(out, oo_lookup_functions(w->ix, import->path, name));
        }
        return;
    }
    for (int i = oo_import_after(file, name, -1); i >= 0; i = oo_import_after(file, name, i)) {
        const oo_import_t *import = &file->imports[i];
        if (import->star || !import->local || strcmp(import->local, name))
            continue;
        /* The imported name may be an alias: the leaf of the path is the
         * declared name. `import a.b.foo` names package a.b's foo, or a member
         * of object a.b. */
        const char *dot = strrchr(import->path, '.');
        const char *leaf = dot ? dot + 1 : import->path;
        const char *prefix =
            dot ? cbm_arena_strndup(w->arena, import->path, (size_t)(dot - import->path)) : "";
        if (!prefix)
            continue;
        kt_list_collect(out, oo_lookup_functions(w->ix, prefix, leaf));
        const oo_type_t *owner = oo_lookup_type(w->ix, prefix);
        if (owner)
            kt_list_collect(out, oo_lookup_methods(w->ix, owner->qn, leaf));
    }
}

/* `import kotlin.test.assertEquals`: the name is bound, outside the project. It
 * shadows every lower level, so a project namesake there is not a candidate. */
static bool kt_explicit_external(oo_walk_t *w, const oo_scope_t *scope, const char *name) {
    const oo_file_t *file = scope->file;
    for (int i = oo_import_after(file, name, -1); i >= 0; i = oo_import_after(file, name, i)) {
        const oo_import_t *import = &file->imports[i];
        if (import->star || !import->local || strcmp(import->local, name))
            continue;
        const char *dot = strrchr(import->path, '.');
        const char *leaf = dot ? dot + 1 : import->path;
        const char *prefix =
            dot ? cbm_arena_strndup(w->arena, import->path, (size_t)(dot - import->path)) : "";
        if (!prefix)
            return true;
        const oo_type_t *owner = oo_lookup_type(w->ix, prefix);
        if (!oo_lookup_functions(w->ix, prefix, leaf) &&
            !(owner && oo_lookup_methods(w->ix, owner->qn, leaf)) &&
            !oo_lookup_type(w->ix, import->path) && !oo_lookup_props(w->ix, prefix, leaf))
            return true;
    }
    return false;
}

/* The standard library's collection and text subtyping, for extension receivers. */
static bool kt_external_conforms(const char *actual, const char *declared) {
    static const struct {
        const char *actual, *supertypes;
    } TABLE[] = {
        {"List", " Collection Iterable "},
        {"Set", " Collection Iterable "},
        {"Collection", " Iterable "},
        {"MutableList", " List Collection MutableCollection Iterable MutableIterable "},
        {"MutableSet", " Set Collection MutableCollection Iterable MutableIterable "},
        {"MutableCollection", " Collection Iterable MutableIterable "},
        {"ArrayList", " MutableList List Collection MutableCollection Iterable "},
        {"LinkedHashSet", " MutableSet Set Collection MutableCollection Iterable "},
        {"HashSet", " MutableSet Set Collection MutableCollection Iterable "},
        {"MutableMap", " Map "},
        {"HashMap", " MutableMap Map "},
        {"LinkedHashMap", " MutableMap Map HashMap "},
        {"String", " CharSequence Comparable "},
        {"Int", " Number Comparable "},
        {"Long", " Number Comparable "},
        {"Double", " Number Comparable "},
        {"Float", " Number Comparable "},
        {"Short", " Number Comparable "},
        {"Byte", " Number Comparable "},
    };
    char word[64];
    int written = snprintf(word, sizeof(word), " %s ", declared);
    if (written <= 0 || (size_t)written >= sizeof(word))
        return false;
    for (size_t i = 0; i < sizeof(TABLE) / sizeof(TABLE[0]); i++)
        if (!strcmp(TABLE[i].actual, actual))
            return strstr(TABLE[i].supertypes, word) != NULL;
    return false;
}

/* Does an extension's declared receiver accept the value? */
static bool kt_ref_accepts(oo_walk_t *w, const oo_typeref_t *ref, oo_scope_t scope,
                           const oo_value_t *value) {
    if (!ref || !value || ref->function)
        return false;
    if (!ref->name)
        return false;
    oo_name_t name = oo_resolve_name(w->ix, &scope, ref->name);
    if (name.status == OO_NAME_TPARAM) {
        if (!name.tparam->bound)
            return value->type || value->external;
        oo_value_t bound = oo_value_from_ref(w, name.tparam->bound, &scope, NULL);
        if (bound.type)
            return value->type && oo_is_subtype(w->ix, value->type, bound.type);
        if (bound.external && !strcmp(bound.external, "Any"))
            return value->type || value->external;
        return bound.external && value->external && !strcmp(bound.external, value->external);
    }
    oo_value_t receiver = oo_value_from_ref(w, ref, &scope, NULL);
    if (receiver.type)
        return value->type && oo_is_subtype(w->ix, value->type, receiver.type);
    if (receiver.external) {
        const char *declared = strrchr(receiver.external, '.');
        declared = declared ? declared + 1 : receiver.external;
        if (!strcmp(declared, "Any"))
            return value->type || value->external;
        /* A project collection reaches Iterable through its declared supertypes. */
        if (!value->external) {
            oo_value_t element =
                !strcmp(declared, "Iterable") ? oo_element_of(w, value) : oo_value_unknown();
            return oo_value_known(&element);
        }
        const char *actual = strrchr(value->external, '.');
        actual = actual ? actual + 1 : value->external;
        return !strcmp(declared, actual) || kt_external_conforms(actual, declared);
    }
    return false;
}

static bool kt_receiver_accepts(oo_walk_t *w, const oo_callable_t *callable,
                                const oo_value_t *value) {
    return kt_ref_accepts(w, callable->receiver, oo_scope_of_callable(callable), value);
}

static bool kt_hidden(const oo_value_t *value, const char *name) {
    if (value->hidden)
        for (int i = 0; i < value->hidden->count; i++)
            if (!strcmp(value->hidden->names[i], name))
                return true;
    return false;
}

/* What an anonymous object's own property holds, seen from outside. Unknown for a function. */
static oo_value_t kt_hidden_value(const oo_value_t *value, const char *name) {
    if (value->hidden && value->hidden->values)
        for (int i = 0; i < value->hidden->count; i++)
            if (!strcmp(value->hidden->names[i], name))
                return value->hidden->values[i];
    return oo_value_unknown();
}

static oo_select_t kt_select_list(oo_walk_t *w, const kt_list_t *list, int argc,
                                  const oo_value_t *args, const oo_value_t *receiver,
                                  bool extensions_only) {
    /* Rebuilt as a chain of private copies: the shared lists are immutable. */
    oo_callable_t *head = NULL;
    for (int i = list->count - 1; i >= 0; i--) {
        const oo_callable_t *c = list->items[i];
        if (extensions_only) {
            if (!c->receiver || !kt_receiver_accepts(w, c, receiver))
                continue;
        } else if (c->receiver) {
            continue;
        }
        oo_callable_t *copy = cbm_arena_alloc(w->arena, sizeof(*copy));
        if (!copy)
            return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
        *copy = *c;
        copy->next = head;
        head = copy;
    }
    return oo_select_from_list(w, head, argc, args, extensions_only ? receiver : NULL);
}

/* An extension on this value, at the first level that offers an applicable one. */
static oo_select_t kt_select_extension(oo_walk_t *w, const oo_scope_t *scope,
                                       const oo_value_t *value, const char *name, int argc,
                                       const oo_value_t *args) {
    if (kt_explicit_external(w, scope, name))
        return (oo_select_t){.status = OO_SEL_NONE};
    for (int level = 0; level < KT_LEVEL_COUNT; level++) {
        kt_list_t list = {.count = 0};
        kt_level_functions(w, scope, name, level, &list);
        if (list.overflow)
            return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
        if (!list.count)
            continue;
        oo_select_t selected = kt_select_list(w, &list, argc, args, value, true);
        if (selected.status == OO_SEL_UNIQUE || selected.status == OO_SEL_AMBIGUOUS)
            return selected;
    }
    return (oo_select_t){.status = OO_SEL_NONE};
}

/* Kotlin spec 11.3: a member extension of an implicit receiver, innermost first. */
static oo_select_t kt_implicit_member_extension(oo_walk_t *w, const oo_value_t *extended,
                                                const char *name, int argc,
                                                const oo_value_t *args) {
    bool unknown = false;
    bool no_outer_instance = false;
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        bool blocked =
            no_outer_instance && !(r->value.type && (r->value.type->flags & OO_F_OBJECT));
        no_outer_instance = no_outer_instance || r->static_context;
        if (blocked)
            continue;
        if (r->unknown) {
            unknown = true;
            continue;
        }
        if (!r->value.type || kt_hidden(&r->value, name))
            continue;
        oo_select_t selected = oo_select_member_extension(w, &r->value, extended, name, argc, args,
                                                          kt_receiver_accepts);
        if (selected.status == OO_SEL_UNIQUE || selected.status == OO_SEL_AMBIGUOUS)
            return unknown ? (oo_select_t){.status = OO_SEL_AMBIGUOUS} : selected;
    }
    return (oo_select_t){.status = OO_SEL_NONE};
}

/* The same for a declaration read out of context: its owners are its receivers. */
static oo_select_t kt_owner_member_extension(oo_walk_t *w, const oo_scope_t *scope,
                                             const oo_value_t *extended, const char *name, int argc,
                                             const oo_value_t *args) {
    for (const oo_type_t *type = scope->owner; type; type = type->owner) {
        oo_value_t dispatch = oo_value_of_type(type);
        oo_select_t selected = oo_select_member_extension(w, &dispatch, extended, name, argc, args,
                                                          kt_receiver_accepts);
        if (selected.status == OO_SEL_UNIQUE || selected.status == OO_SEL_AMBIGUOUS)
            return selected;
    }
    return (oo_select_t){.status = OO_SEL_NONE};
}

/* A member call on a value, or a member of a type named by the value. `walking`
 * says the walk's implicit receivers are the ones in scope at this site. */
static oo_select_t kt_select_member_at(oo_walk_t *w, const oo_scope_t *scope,
                                       const oo_value_t *value, const char *name, int argc,
                                       const oo_value_t *args, bool walking) {
    if (kt_hidden(value, name))
        return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
    oo_value_t receiver = *value;
    if (value->classref && value->type) {
        const oo_type_t *type = value->type;
        if (type->file && type->file->language == CBM_LANG_JAVA)
            return oo_select_method(w, value, name, argc, args, true);
        if (!(type->flags & OO_F_OBJECT)) {
            /* `Foo.bar()` on a class is its companion's member. */
            if (!type->companion)
                return (oo_select_t){.status = OO_SEL_NONE};
            receiver = oo_value_of_type(type->companion);
        }
        receiver.classref = false;
    }
    oo_select_t member = receiver.type ? oo_select_method(w, &receiver, name, argc, args, false)
                                       : (oo_select_t){.status = OO_SEL_NONE};
    if (member.status == OO_SEL_UNIQUE || member.status == OO_SEL_AMBIGUOUS)
        return member;
    /* Every value has Any's members, and a member beats any extension. */
    if (!strcmp(name, "toString") || !strcmp(name, "equals") || !strcmp(name, "hashCode"))
        return (oo_select_t){.status = OO_SEL_NONE};
    oo_select_t member_extension =
        walking ? kt_implicit_member_extension(w, &receiver, name, argc, args)
                : kt_owner_member_extension(w, scope, &receiver, name, argc, args);
    if (member_extension.status == OO_SEL_UNIQUE || member_extension.status == OO_SEL_AMBIGUOUS)
        return member_extension;
    oo_select_t extension = kt_select_extension(w, scope, &receiver, name, argc, args);
    if (extension.status == OO_SEL_UNIQUE || extension.status == OO_SEL_AMBIGUOUS)
        return extension;
    return member.status == OO_SEL_INCOMPLETE ? member : (oo_select_t){.status = OO_SEL_NONE};
}

static oo_select_t kt_select_member(oo_walk_t *w, const oo_scope_t *scope, const oo_value_t *value,
                                    const char *name, int argc, const oo_value_t *args) {
    return kt_select_member_at(w, scope, value, name, argc, args, true);
}

/* A property of a value, member first and then a visible extension property. */
static const oo_field_t *kt_find_property(oo_walk_t *w, const oo_value_t *value, const char *name,
                                          oo_subst_t **subst) {
    oo_value_t receiver = *value;
    if (value->classref && value->type && !(value->type->flags & OO_F_OBJECT)) {
        /* Enum entries are the class's own; other names are its companion's. */
        bool incomplete = false;
        const oo_field_t *own = oo_find_field(w, value, name, subst, &incomplete);
        if (own && (own->flags & OO_F_STATIC))
            return own;
        if (!value->type->companion)
            return NULL;
        receiver = oo_value_of_type(value->type->companion);
    }
    bool incomplete = false;
    return oo_find_field(w, &receiver, name, subst, &incomplete);
}

static oo_value_t kt_field_type(oo_walk_t *w, const oo_field_t *field, const oo_subst_t *subst);

/* A top-level property of the scope's file: explicit import, package, star. */
static const oo_field_t *kt_free_property(oo_walk_t *w, const oo_scope_t *scope, const char *name) {
    const oo_file_t *file = scope->file;
    for (int i = oo_import_after(file, name, -1); i >= 0; i = oo_import_after(file, name, i)) {
        const oo_import_t *import = &file->imports[i];
        if (import->star || !import->local || strcmp(import->local, name))
            continue;
        const char *dot = strrchr(import->path, '.');
        const char *leaf = dot ? dot + 1 : import->path;
        const char *prefix =
            dot ? cbm_arena_strndup(w->arena, import->path, (size_t)(dot - import->path)) : "";
        if (!prefix)
            return NULL;
        for (const oo_field_t *f = oo_lookup_props(w->ix, prefix, leaf); f; f = f->next)
            if (!f->receiver)
                return f->next ? NULL : f;
        const oo_type_t *owner = oo_lookup_type(w->ix, prefix);
        const oo_field_t *member = owner ? oo_lookup_fields(w->ix, owner->qn, leaf) : NULL;
        return member && !member->next ? member : NULL;
    }
    const oo_field_t *found = NULL;
    for (const oo_field_t *f = oo_lookup_props(w->ix, file->package, name); f; f = f->next) {
        if (f->receiver || ((f->flags & OO_F_PRIVATE) && strcmp(f->file->rel, file->rel)))
            continue;
        if (found)
            return NULL;
        found = f;
    }
    if (found)
        return found;
    for (int i = 0; i < file->import_count; i++) {
        const oo_import_t *import = &file->imports[i];
        if (!import->star)
            continue;
        for (const oo_field_t *f = oo_lookup_props(w->ix, import->path, name); f; f = f->next) {
            if (f->receiver || (f->flags & OO_F_PRIVATE))
                continue;
            if (found && found != f)
                return NULL;
            found = f;
        }
    }
    return found;
}

static bool kt_property_accepts(oo_walk_t *w, const oo_field_t *field, const oo_value_t *value) {
    return kt_ref_accepts(w, field->receiver, oo_scope_of_field(field), value);
}

/* An extension property on a value: the first import level that offers one
 * decides, as for functions. */
static const oo_field_t *kt_extension_property(oo_walk_t *w, const oo_scope_t *scope,
                                               const oo_value_t *value, const char *name) {
    const oo_file_t *file = scope->file;
    const oo_field_t *found = NULL;
    for (int i = oo_import_after(file, name, -1); i >= 0; i = oo_import_after(file, name, i)) {
        const oo_import_t *import = &file->imports[i];
        if (import->star || !import->local || strcmp(import->local, name))
            continue;
        const char *dot = strrchr(import->path, '.');
        const char *leaf = dot ? dot + 1 : import->path;
        const char *prefix =
            dot ? cbm_arena_strndup(w->arena, import->path, (size_t)(dot - import->path)) : "";
        if (!prefix)
            return NULL;
        for (const oo_field_t *f = oo_lookup_props(w->ix, prefix, leaf); f; f = f->next) {
            if (!f->receiver || !kt_property_accepts(w, f, value))
                continue;
            if (found && found != f)
                return NULL;
            found = f;
        }
    }
    if (found)
        return found;
    for (const oo_field_t *f = oo_lookup_props(w->ix, file->package, name); f; f = f->next) {
        if (!f->receiver || ((f->flags & OO_F_PRIVATE) && strcmp(f->file->rel, file->rel)) ||
            !kt_property_accepts(w, f, value))
            continue;
        if (found)
            return NULL;
        found = f;
    }
    if (found)
        return found;
    for (int i = 0; i < file->import_count; i++) {
        const oo_import_t *import = &file->imports[i];
        if (!import->star)
            continue;
        for (const oo_field_t *f = oo_lookup_props(w->ix, import->path, name); f; f = f->next) {
            if (!f->receiver || (f->flags & OO_F_PRIVATE) || !kt_property_accepts(w, f, value))
                continue;
            if (found && found != f)
                return NULL;
            found = f;
        }
    }
    return found;
}

/* A bare name in a declaration's summary: members of the enclosing types, then
 * free functions and types of its file. Summaries carry no local bindings. */
static oo_value_t kt_summary_first(oo_walk_t *w, const oo_sumstep_t *step, const oo_scope_t *scope,
                                   const oo_subst_t *subst) {
    for (const oo_type_t *type = scope->owner; type; type = type->owner) {
        oo_value_t receiver = oo_value_of_type(type);
        for (int pass = 0; pass < 2; pass++) {
            if (step->argc >= 0) {
                oo_select_t selected =
                    oo_select_method(w, &receiver, step->name, step->argc, NULL, false);
                if (selected.status == OO_SEL_UNIQUE)
                    return oo_callable_return(w, selected.callable,
                                              selected.subst ? selected.subst : subst);
                if (selected.status == OO_SEL_AMBIGUOUS || selected.status == OO_SEL_MISMATCH)
                    return oo_value_unknown();
            } else {
                oo_subst_t *field_subst = NULL;
                bool incomplete = false;
                const oo_field_t *field =
                    oo_find_field(w, &receiver, step->name, &field_subst, &incomplete);
                if (field)
                    return kt_field_type(w, field, field_subst ? field_subst : subst);
                if (incomplete)
                    return oo_value_unknown();
            }
            /* `val at = datetime("at")` on an extension of the enclosing type. */
            for (int level = 0; step->argc >= 0 && level < KT_LEVEL_COUNT; level++) {
                kt_list_t list = {.count = 0};
                kt_level_functions(w, scope, step->name, level, &list);
                if (list.overflow)
                    return oo_value_unknown();
                oo_select_t extension = kt_select_list(w, &list, step->argc, NULL, &receiver, true);
                if (extension.status == OO_SEL_UNIQUE)
                    return oo_callable_return(w, extension.callable, extension.subst);
                if (extension.status == OO_SEL_AMBIGUOUS)
                    return oo_value_unknown();
            }
            if (pass == 0 && type->companion)
                receiver = oo_value_of_type(type->companion);
            else
                break;
        }
        if (!oo_hierarchy_complete(w->ix, type))
            return oo_value_unknown();
    }
    if (step->argc >= 0) {
        for (int level = 0; level < KT_LEVEL_COUNT; level++) {
            kt_list_t list = {.count = 0};
            kt_level_functions(w, scope, step->name, level, &list);
            if (list.overflow)
                return oo_value_unknown();
            oo_select_t selected = kt_select_list(w, &list, step->argc, NULL, NULL, false);
            if (selected.status == OO_SEL_UNIQUE)
                return oo_callable_return(w, selected.callable, NULL);
            if (selected.status == OO_SEL_AMBIGUOUS)
                return oo_value_unknown();
            /* Constructors live at the classifier's own level. */
        }
    } else {
        const oo_field_t *field = kt_free_property(w, scope, step->name);
        if (field)
            return kt_field_type(w, field, NULL);
    }
    oo_typeref_t ref = {.name = step->name, .args = step->type_args};
    oo_value_t value = oo_value_from_ref(w, &ref, scope, subst);
    if (value.type && step->argc < 0)
        value.classref = true;
    if (value.type)
        return value;
    /* An external constructor spelled like a type keeps its name and arguments. */
    return step->argc >= 0 && oo_type_shaped(step->name) ? value : oo_value_unknown();
}

oo_value_t oo_kotlin_eval_summary(oo_walk_t *w, const oo_exprsum_t *summary,
                                  const oo_scope_t *scope, const oo_subst_t *subst) {
    if (!summary || !summary->count || w->depth >= 12)
        return oo_value_unknown();
    w->depth++;
    oo_value_t value = kt_summary_first(w, &summary->steps[0], scope, subst);
    for (int i = 1; i < summary->count && oo_value_known(&value); i++) {
        const oo_sumstep_t *step = &summary->steps[i];
        if (step->argc >= 0) {
            oo_select_t selected =
                kt_select_member_at(w, scope, &value, step->name, step->argc, NULL, false);
            if (selected.status != OO_SEL_UNIQUE) {
                value = oo_value_unknown();
                break;
            }
            value = oo_callable_return(w, selected.callable, selected.subst);
        } else {
            oo_subst_t *field_subst = NULL;
            const oo_field_t *field = kt_find_property(w, &value, step->name, &field_subst);
            if (!field) {
                value = oo_value_unknown();
                break;
            }
            value = kt_field_type(w, field, field_subst);
        }
    }
    w->depth--;
    return value;
}

static oo_value_t kt_field_type(oo_walk_t *w, const oo_field_t *field, const oo_subst_t *subst) {
    oo_scope_t scope = oo_scope_of_field(field);
    if (field->ref)
        return oo_value_from_ref(w, field->ref, &scope, subst);
    if (field->init)
        return oo_kotlin_eval_summary(w, field->init, &scope, subst);
    return oo_value_unknown();
}

/* ── Walk ───────────────────────────────────────────────────────────── */

typedef struct kw_args kw_args_t;

typedef struct {
    oo_walk_t *w;
    kt_text_t text;
    int depth;
    kw_args_t *free_args;
    oo_hidden_t *object_members; /* The anonymous object whose members are being walked. */
} kw_t;

static oo_value_t kw_expr(kw_t *k, TSNode node);
static void kw_class_like(kw_t *k, TSNode node, const oo_type_t *owner);
static void kw_function(kw_t *k, TSNode node);
static void kw_members(kw_t *k, TSNode body, const oo_type_t *owner);

static void kw_children(kw_t *k, TSNode node) {
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        (void)kw_expr(k, ts_node_named_child(node, i));
}

static oo_value_t kw_ref(kw_t *k, const oo_typeref_t *ref) {
    oo_walk_t *w = k->w;
    if (ref && ref->name) {
        char first[256];
        const char *dot = strchr(ref->name, '.');
        size_t length = dot ? (size_t)(dot - ref->name) : strlen(ref->name);
        if (length < sizeof(first)) {
            memcpy(first, ref->name, length);
            first[length] = '\0';
            if (oo_lookup_local(w, cbm_arena_sprintf(w->arena, "#type:%s", first)))
                return oo_value_unknown();
        }
    }
    return ref ? oo_value_from_ref(w, ref, &w->scope, NULL) : oo_value_unknown();
}

static void kw_type_mentions(kw_t *k, TSNode node, unsigned depth);

/* `@InternalApi`, `@OptIn(InternalApi::class)`, in a declaration's modifiers. */
static void kw_annotations(kw_t *k, TSNode owner) {
    TSNode modifiers = kt_child(owner, "modifiers");
    uint32_t count = ts_node_is_null(modifiers) ? 0 : ts_node_named_child_count(modifiers);
    for (uint32_t i = 0; i < count; i++) {
        TSNode annotation = ts_node_named_child(modifiers, i);
        if (!kt_is(annotation, "annotation"))
            continue;
        uint32_t parts = ts_node_named_child_count(annotation);
        for (uint32_t p = 0; p < parts; p++) {
            TSNode part = ts_node_named_child(annotation, p);
            if (kt_is(part, "user_type")) {
                kw_type_mentions(k, part, 0);
            } else if (kt_is(part, "constructor_invocation")) {
                kw_type_mentions(k, kt_child(part, "user_type"), 0);
                kw_children(k, kt_child(part, "value_arguments"));
            }
        }
    }
}

/* Every type mention inside an annotation, at the identifier's own span. */
static void kw_type_mentions(kw_t *k, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 32)
        return;
    if (kt_is(node, "user_type")) {
        oo_walk_t *w = k->w;
        uint32_t count = ts_node_named_child_count(node);
        /* The whole spelled path selects the leaf type. */
        const oo_typeref_t *ref = kt_typeref(&k->text, node);
        oo_value_t value = kw_ref(k, ref);
        TSNode last = {0};
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (kt_is(child, "type_identifier"))
                last = child;
            else
                kw_type_mentions(k, child, depth + 1);
        }
        if (value.type && value.type->graph_qn && !ts_node_is_null(last) &&
            !(ref && oo_names_tparam(w, ref->name))) {
            oo_emit_type_ref(w, last, value.type->graph_qn, "kotlin-type-name-lookup");
            oo_emit_usage(w, last, value.type->graph_qn, CBM_RESOLVED_TYPE_REFERENCE,
                          CBM_RESOLVE_RULE_DECLARED_TYPE, "kotlin-type-name-lookup");
        }
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        kw_type_mentions(k, ts_node_named_child(node, i), depth + 1);
}

static int kt_call_argc(TSNode suffix) {
    TSNode arguments = kt_child(suffix, "value_arguments");
    int count = 0;
    uint32_t children = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < children; i++)
        if (kt_is(ts_node_named_child(arguments, i), "value_argument"))
            count++;
    /* A trailing lambda is one more argument. */
    uint32_t parts = ts_node_named_child_count(suffix);
    for (uint32_t i = 0; i < parts; i++)
        if (kt_is(ts_node_named_child(suffix, i), "annotated_lambda"))
            count++;
    return count;
}

/* The expected function type of a lambda argument, when the callee is known. */
typedef struct {
    const oo_callable_t *callee;
    const oo_subst_t *subst;
    /* A scope function's receiver and `it` when the callee is the standard library. */
    oo_value_t self;
    bool has_self;
    oo_value_t it;
    bool has_it;
    bool no_receiver;
    oo_value_t result;
    bool result_is_self;
    bool result_is_lambda;
    bool result_is_same;
    bool result_is_element;
} kw_lambda_ctx_t;

/* Standard library higher-order functions whose lambda takes no receiver. A
 * project declaration of the name wins before these are ever consulted. */
static bool kt_stdlib_plain_lambda(const char *name) {
    static const char *const names[] = {"let",
                                        "also",
                                        "takeIf",
                                        "takeUnless",
                                        "map",
                                        "mapNotNull",
                                        "mapIndexed",
                                        "flatMap",
                                        "filter",
                                        "filterNot",
                                        "filterNotNull",
                                        "forEach",
                                        "forEachIndexed",
                                        "any",
                                        "all",
                                        "none",
                                        "count",
                                        "first",
                                        "firstOrNull",
                                        "last",
                                        "lastOrNull",
                                        "single",
                                        "singleOrNull",
                                        "find",
                                        "findLast",
                                        "sortedBy",
                                        "sortedByDescending",
                                        "groupBy",
                                        "associate",
                                        "associateBy",
                                        "associateWith",
                                        "partition",
                                        "fold",
                                        "reduce",
                                        "sumOf",
                                        "maxBy",
                                        "minBy",
                                        "maxByOrNull",
                                        "minByOrNull",
                                        "distinctBy",
                                        "onEach",
                                        "use",
                                        "repeat",
                                        "lazy",
                                        "require",
                                        "check",
                                        "requireNotNull",
                                        "checkNotNull",
                                        "error",
                                        "synchronized",
                                        "getOrPut",
                                        "getOrElse",
                                        "computeIfAbsent",
                                        "ifEmpty",
                                        "ifBlank",
                                        "assertFails",
                                        "assertFailsWith",
                                        "assertThrows",
                                        "assertDoesNotThrow",
                                        "thread",
                                        "measureTimeMillis",
                                        "measureNanoTime",
                                        "sortedWith",
                                        "compareBy",
                                        "zip",
                                        "takeWhile",
                                        "dropWhile",
                                        "flatten",
                                        "indexOfFirst",
                                        "indexOfLast",
                                        "removeAll",
                                        "retainAll",
                                        "replaceAll",
                                        "mapValues",
                                        "mapKeys",
                                        "joinToString",
                                        "orElse",
                                        "orElseGet",
                                        "orElseThrow",
                                        NULL};
    for (int i = 0; names[i]; i++)
        if (!strcmp(names[i], name))
            return true;
    return false;
}

/* Lambda facts of a standard library function on a known receiver. */
static void kt_stdlib_lambda(oo_walk_t *w, const char *name, const oo_value_t *receiver,
                             kw_lambda_ctx_t *ctx) {
    if (!strcmp(name, "let") || !strcmp(name, "also") || !strcmp(name, "takeIf") ||
        !strcmp(name, "takeUnless") || !strcmp(name, "use")) {
        ctx->no_receiver = true;
        if (receiver && oo_value_known(receiver)) {
            ctx->it = *receiver;
            ctx->it.classref = false;
            ctx->has_it = true;
        }
        ctx->result_is_lambda = !strcmp(name, "let") || !strcmp(name, "use");
        ctx->result_is_self = !strcmp(name, "also") || !strncmp(name, "take", 4);
        return;
    }
    if ((!strcmp(name, "apply") || !strcmp(name, "run")) && receiver && oo_value_known(receiver)) {
        ctx->self = *receiver;
        ctx->self.classref = false;
        ctx->has_self = true;
        ctx->result_is_self = !strcmp(name, "apply");
        ctx->result_is_lambda = !strcmp(name, "run");
        return;
    }
    if (kt_stdlib_plain_lambda(name)) {
        ctx->no_receiver = true;
        if (receiver) {
            oo_value_t element = oo_element_of(w, receiver);
            if (oo_value_known(&element) && strcmp(name, "fold") && strcmp(name, "reduce") &&
                strcmp(name, "forEachIndexed") && strcmp(name, "mapIndexed")) {
                ctx->it = element;
                ctx->has_it = true;
            }
            if (!strcmp(name, "filter") || !strcmp(name, "filterNot") ||
                !strcmp(name, "sortedBy") || !strcmp(name, "sortedByDescending") ||
                !strcmp(name, "distinctBy") || !strcmp(name, "onEach") ||
                !strcmp(name, "takeWhile"))
                ctx->result_is_same = true;
            if (!strcmp(name, "first") || !strcmp(name, "last") || !strcmp(name, "single") ||
                !strcmp(name, "firstOrNull") || !strcmp(name, "lastOrNull") ||
                !strcmp(name, "singleOrNull") || !strcmp(name, "find"))
                ctx->result_is_element = true;
        }
    }
}

/* `componentN` of a destructured value (Kotlin spec 8.4): a pair, a triple or a map entry from the
 * standard library, a list's element, a data class's primary constructor, or the operator itself.
 */
static oo_value_t kt_component(oo_walk_t *w, const oo_value_t *value, int position) {
    if (!value || value->dims)
        return oo_value_unknown();
    if (value->external) {
        const char *dot = strrchr(value->external, '.');
        const char *leaf = dot ? dot + 1 : value->external;
        bool tuple = (!strcmp(leaf, "Pair") && value->arg_count == 2) ||
                     (!strcmp(leaf, "Triple") && value->arg_count == 3) ||
                     (!strcmp(leaf, "Entry") && value->arg_count == 2) ||
                     (!strcmp(leaf, "MutableEntry") && value->arg_count == 2);
        if (tuple)
            return position < value->arg_count ? value->args[position] : oo_value_unknown();
        return oo_element_of(w, value);
    }
    if (!value->type)
        return oo_value_unknown();
    oo_subst_t *subst = oo_subst_of(w, value);
    char method[32];
    snprintf(method, sizeof(method), "component%d", position + 1);
    oo_select_t selected = oo_select_method(w, value, method, 0, NULL, false);
    if (selected.status == OO_SEL_UNIQUE)
        return oo_callable_return(w, selected.callable, selected.subst);
    const oo_callable_t *primary = value->type->primary;
    if ((value->type->flags & OO_F_DATA) && primary && primary->params &&
        position < primary->param_count && primary->params[position]) {
        oo_scope_t scope = oo_scope_of_callable(primary);
        return oo_value_from_ref(w, primary->params[position], &scope, subst);
    }
    return oo_value_unknown();
}

static void kw_bind_destructured(kw_t *k, TSNode node, const oo_value_t *source) {
    uint32_t count = ts_node_named_child_count(node);
    int position = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (kt_is(child, "variable_declaration")) {
            const char *name = kt_name(&k->text, kt_child(child, "simple_identifier"));
            TSNode declared = kt_after_colon(child, "=", NULL);
            oo_bind(k->w, name,
                    !ts_node_is_null(declared) ? kw_ref(k, kt_typeref(&k->text, declared))
                                               : kt_component(k->w, source, position));
            position++;
        }
    }
}

/* Walk a lambda under its expected function type. The value is its last line. */
static oo_value_t kw_lambda(kw_t *k, TSNode lambda, const oo_value_t *expected,
                            const kw_lambda_ctx_t *ctx) {
    oo_walk_t *w = k->w;
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    bool pushed = false;
    const oo_value_t *function = expected && expected->fn_ref ? expected : NULL;
    bool settled = function && !(ctx && oo_ref_unsettled(ctx->callee, function->fn_ref->fn_receiver,
                                                         ctx->subst));
    if (settled && function->fn_receiver && oo_value_known(function->fn_receiver)) {
        pushed = oo_receiver_push(w, *function->fn_receiver, false) != NULL;
    } else if (function && function->fn_ref->fn_receiver) {
        /* A receiver exists whose type could not be read. */
        oo_receiver_t *receiver = oo_receiver_push(w, oo_value_unknown(), false);
        if (receiver) {
            receiver->unknown = true;
            pushed = true;
        }
    } else if (ctx && ctx->has_self) {
        pushed = oo_receiver_push(w, ctx->self, false) != NULL;
    } else if (!function && !(ctx && ctx->no_receiver)) {
        /* Nothing says this lambda lacks a receiver. */
        oo_receiver_t *receiver = oo_receiver_push(w, oo_value_unknown(), false);
        if (receiver) {
            receiver->unknown = true;
            pushed = true;
        }
    }
    /* Parameters. Every binder shadows, with or without a type. */
    TSNode parameters = kt_child(lambda, "lambda_parameters");
    oo_value_t first_parameter = oo_value_unknown();
    const oo_typeref_t *const *fn_params =
        function && function->fn_ref->args ? function->fn_ref->args : NULL;
    if (fn_params && fn_params[0]) {
        /* Function-type parameters are read in the declaring scope; the value
         * carries its already resolved arguments only for the return type. */
        if (ctx && ctx->callee) {
            oo_scope_t scope = oo_scope_of_callable(ctx->callee);
            first_parameter = oo_value_from_ref(w, fn_params[0], &scope, ctx->subst);
        }
    } else if (ctx && ctx->has_it) {
        first_parameter = ctx->it;
    }
    if (!ts_node_is_null(parameters)) {
        uint32_t count = ts_node_named_child_count(parameters);
        int index = 0;
        for (uint32_t i = 0; i < count; i++) {
            TSNode parameter = ts_node_named_child(parameters, i);
            if (kt_is(parameter, "variable_declaration")) {
                const char *name = kt_name(&k->text, kt_child(parameter, "simple_identifier"));
                TSNode declared = kt_after_colon(parameter, "=", NULL);
                oo_value_t value = oo_value_unknown();
                if (!ts_node_is_null(declared))
                    value = kw_ref(k, kt_typeref(&k->text, declared));
                else if (index == 0)
                    value = first_parameter;
                else if (fn_params && ctx && ctx->callee && fn_params[0]) {
                    int at = 0;
                    while (at < index && fn_params[at])
                        at++;
                    if (fn_params[at]) {
                        oo_scope_t scope = oo_scope_of_callable(ctx->callee);
                        value = oo_value_from_ref(w, fn_params[at], &scope, ctx->subst);
                    }
                }
                oo_bind(w, name, value);
                index++;
            } else if (kt_is(parameter, "multi_variable_declaration")) {
                kw_bind_destructured(k, parameter, index == 0 ? &first_parameter : NULL);
                index++;
            }
        }
    } else {
        oo_bind(w, "it", first_parameter);
    }
    oo_value_t last = oo_value_unknown();
    TSNode statements = kt_child(lambda, "statements");
    uint32_t count = ts_node_is_null(statements) ? 0 : ts_node_named_child_count(statements);
    for (uint32_t i = 0; i < count; i++)
        last = kw_expr(k, ts_node_named_child(statements, i));
    if (pushed)
        oo_receiver_pop(w);
    oo_frame_pop(w);
    return last;
}

/* Value arguments and trailing lambdas, walked once each. A lambda's expected
 * type is read from the selected declaration, so lambdas wait for selection. */
struct kw_args {
    TSNode nodes[32];
    oo_value_t values[32];
    bool lambda[32];
    const char *names[32]; /* A named argument lands on that parameter, not at its position. */
    int count;
    bool overflow;
    kw_args_t *next_free;
};

static kw_args_t *kw_args_take(kw_t *k) {
    kw_args_t *args = k->free_args;
    if (args)
        k->free_args = args->next_free;
    else
        args = cbm_arena_alloc(k->w->arena, sizeof(*args));
    return args;
}

static void kw_args_give(kw_t *k, kw_args_t *args) {
    if (args) {
        args->next_free = k->free_args;
        k->free_args = args;
    }
}

static TSNode kw_lambda_of(TSNode argument) {
    TSNode node = argument;
    if (kt_is(node, "value_argument")) {
        uint32_t parts = ts_node_named_child_count(node);
        node = parts ? ts_node_named_child(node, parts - 1) : (TSNode){0};
    }
    if (kt_is(node, "annotated_lambda"))
        node = kt_child(node, "lambda_literal");
    return kt_is(node, "lambda_literal") ? node : (TSNode){0};
}

static void kw_collect_args(kw_t *k, TSNode suffix, kw_args_t *args) {
    args->count = 0;
    args->overflow = false;
    if (ts_node_is_null(suffix))
        return;
    TSNode arguments = kt_child(suffix, "value_arguments");
    uint32_t children = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < children; i++) {
        TSNode argument = ts_node_named_child(arguments, i);
        if (!kt_is(argument, "value_argument"))
            continue;
        if (args->count == 32) {
            args->overflow = true;
            (void)kw_expr(k, argument);
            continue;
        }
        TSNode lambda = kw_lambda_of(argument);
        uint32_t parts = ts_node_named_child_count(argument);
        bool named = parts >= 2 && kt_is(ts_node_named_child(argument, 0), "simple_identifier");
        args->nodes[args->count] = argument;
        args->lambda[args->count] = !ts_node_is_null(lambda);
        args->names[args->count] =
            named ? kt_name(&k->text, ts_node_named_child(argument, 0)) : NULL;
        args->values[args->count] = oo_value_unknown();
        args->values[args->count].function_literal = !ts_node_is_null(lambda) && !named;
        if (ts_node_is_null(lambda)) {
            TSNode value = parts ? ts_node_named_child(argument, parts - 1) : (TSNode){0};
            oo_value_t read = kw_expr(k, value);
            if (!named)
                args->values[args->count] = read;
        }
        args->count++;
    }
    uint32_t parts = ts_node_named_child_count(suffix);
    for (uint32_t i = 0; i < parts; i++) {
        TSNode child = ts_node_named_child(suffix, i);
        if (!kt_is(child, "annotated_lambda") || args->count == 32)
            continue;
        args->nodes[args->count] = child;
        args->lambda[args->count] = true;
        args->names[args->count] = NULL;
        args->values[args->count] = oo_value_unknown();
        args->values[args->count].function_literal = true;
        args->count++;
    }
}

/* The parameter a positional or trailing argument lands on. */
static const oo_typeref_t *kw_param_for(const oo_callable_t *callee, int index, int total,
                                        bool trailing, const char *named) {
    if (!callee || !callee->params || !callee->param_count)
        return NULL;
    if (named) {
        for (int i = 0; callee->param_names && i < callee->param_count; i++)
            if (callee->param_names[i] && !strcmp(callee->param_names[i], named))
                return callee->params[i];
        return NULL;
    }
    if (trailing && index == total - 1)
        return callee->params[callee->param_count - 1];
    return index < callee->param_count
               ? callee->params[index]
               : (callee->max_args < 0 ? callee->params[callee->param_count - 1] : NULL);
}

static oo_value_t kw_finish_lambdas(kw_t *k, const kw_args_t *args, const oo_callable_t *callee,
                                    const oo_subst_t *subst, const kw_lambda_ctx_t *base) {
    oo_walk_t *w = k->w;
    oo_value_t last = oo_value_unknown();
    for (int i = 0; i < args->count; i++) {
        if (!args->lambda[i])
            continue;
        TSNode lambda = kw_lambda_of(args->nodes[i]);
        if (ts_node_is_null(lambda))
            continue;
        kw_lambda_ctx_t ctx = base ? *base : (kw_lambda_ctx_t){0};
        ctx.callee = callee;
        ctx.subst = subst;
        oo_value_t expected = oo_value_unknown();
        bool trailing = kt_is(args->nodes[i], "annotated_lambda");
        const oo_typeref_t *ref = kw_param_for(callee, i, args->count, trailing, args->names[i]);
        if (ref && callee) {
            oo_scope_t scope = oo_scope_of_callable(callee);
            expected = oo_value_from_ref(w, ref, &scope, subst);
            /* A generic function parameter (`T`) gives no shape: no receiver. */
            if (!expected.fn_ref)
                ctx.no_receiver = true;
        }
        last = kw_lambda(k, lambda, &expected, &ctx);
    }
    return last;
}

/* Any declaration the implicit receivers offer for a bare call. */
typedef struct {
    oo_select_t selected;
    bool stop;
    bool unknown_receiver;
    bool extension;
} kw_implicit_t;

/* An extension applicable to one implicit receiver: member extensions, then the
 * import levels. Under an unknown closer receiver only an explicit import, which
 * spells out this very name, is still believable. */
static oo_select_t kw_extension_on_implicit(oo_walk_t *w, const oo_value_t *value, const char *name,
                                            int argc, const oo_value_t *args, bool explicit_only) {
    if (!explicit_only) {
        oo_select_t member = kt_implicit_member_extension(w, value, name, argc, args);
        if (member.status == OO_SEL_UNIQUE || member.status == OO_SEL_AMBIGUOUS)
            return member;
    }
    if (kt_explicit_external(w, &w->scope, name))
        return (oo_select_t){.status = OO_SEL_NONE};
    int levels = explicit_only ? KT_LEVEL_EXPLICIT + 1 : KT_LEVEL_COUNT;
    for (int level = 0; level < levels; level++) {
        kt_list_t list = {.count = 0};
        kt_level_functions(w, &w->scope, name, level, &list);
        if (list.overflow)
            return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
        if (!list.count)
            continue;
        oo_select_t extension = kt_select_list(w, &list, argc, args, value, true);
        if (extension.status == OO_SEL_UNIQUE || extension.status == OO_SEL_AMBIGUOUS)
            return extension;
    }
    return (oo_select_t){.status = OO_SEL_NONE};
}

static kw_implicit_t kw_implicit_member(kw_t *k, TSNode name_site, const char *name, int argc,
                                        const oo_value_t *args) {
    oo_walk_t *w = k->w;
    kw_implicit_t out = {0};
    bool no_outer_instance = false;
    bool own_property = false;
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        bool blocked =
            no_outer_instance && !(r->value.type && (r->value.type->flags & OO_F_OBJECT));
        no_outer_instance = no_outer_instance || r->static_context;
        if (blocked)
            continue;
        for (int i = 0; i < r->own_count; i++) {
            if (strcmp(r->own_names[i], name))
                continue;
            if (!r->own_function || r->own_function[i]) {
                out.stop = true;
                return out;
            }
            own_property = true;
        }
        if (r->unknown) {
            out.unknown_receiver = true;
            continue;
        }
        if (!r->value.type)
            continue;
        if (kt_hidden(&r->value, name)) {
            out.stop = true;
            return out;
        }
        oo_select_t selected = oo_select_method(w, &r->value, name, argc, args, false);
        if (selected.status == OO_SEL_UNIQUE || selected.status == OO_SEL_AMBIGUOUS) {
            /* A closer unknown receiver could still declare the name. */
            if (out.unknown_receiver) {
                out.stop = true;
                return out;
            }
            out.selected = selected;
            out.stop = true;
            return out;
        }
        /* `f()` on a member property is its invoke, not a free function f. */
        oo_subst_t *field_subst = NULL;
        bool field_incomplete = false;
        const oo_field_t *property =
            oo_find_field(w, &r->value, name, &field_subst, &field_incomplete);
        if (property) {
            out.stop = true;
            if (!out.unknown_receiver) {
                oo_value_t value = kt_field_type(w, property, field_subst);
                if (value.type && argc >= 0)
                    out.selected = kt_select_member(w, &w->scope, &value, "invoke", argc, args);
                if (out.selected.status != OO_SEL_UNIQUE)
                    out.selected = (oo_select_t){.status = OO_SEL_NONE};
                else if (property->graph_qn)
                    oo_emit_usage(w, name_site, property->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                                  CBM_RESOLVE_RULE_TYPE_MEMBER,
                                  "kotlin-implicit-receiver-property");
            }
            return out;
        }
        if (selected.status == OO_SEL_INCOMPLETE)
            out.unknown_receiver = true;
        /* Kotlin spec 11.3: extensions on this receiver come before the next
         * receiver's members. */
        oo_select_t extension =
            oo_closed_member(w, name)
                ? (oo_select_t){.status = OO_SEL_NONE}
                : kw_extension_on_implicit(w, &r->value, name, argc, args, out.unknown_receiver);
        if (extension.status == OO_SEL_UNIQUE || extension.status == OO_SEL_AMBIGUOUS) {
            out.selected = extension;
            out.extension = true;
            out.stop = true;
            return out;
        }
    }
    /* No function of the name anywhere: the call is the own property's invoke. */
    out.stop = out.stop || own_property;
    return out;
}

/* Top-level functions that take no receiver, and constructors. */
static oo_select_t kw_free_call(kw_t *k, const char *name, int argc, const oo_value_t *args,
                                bool *constructor, oo_value_t *type_value, bool explicit_only) {
    oo_walk_t *w = k->w;
    *constructor = false;
    *type_value = oo_value_unknown();
    oo_typeref_t ref = {.name = name};
    oo_name_t type_name = oo_resolve_name(w->ix, &w->scope, name);
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        const oo_type_t *nested =
            r->object_super && r->value.type ? oo_find_nested(w->ix, r->value.type, name) : NULL;
        if (nested && !(nested->flags & OO_F_AMBIGUOUS)) {
            type_name = (oo_name_t){.status = OO_NAME_TYPE, .type = nested};
            break;
        }
    }
    bool shadowed_type = oo_lookup_local(w, cbm_arena_sprintf(w->arena, "#type:%s", name)) != NULL;
    if (shadowed_type)
        return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
    if (kt_explicit_external(w, &w->scope, name)) {
        /* Only its own classifier could still be meant, and that was this import. */
        return (oo_select_t){.status = OO_SEL_NONE};
    }
    int levels = explicit_only ? KT_LEVEL_EXPLICIT + 1 : KT_LEVEL_COUNT;
    for (int level = 0; level < levels; level++) {
        kt_list_t list = {.count = 0};
        kt_level_functions(w, &w->scope, name, level, &list);
        if (list.overflow)
            return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
        if (!list.count)
            continue;
        oo_select_t plain = kt_select_list(w, &list, argc, args, NULL, false);
        if (plain.status == OO_SEL_UNIQUE || plain.status == OO_SEL_AMBIGUOUS)
            return plain;
    }
    (void)ref;
    if (type_name.status == OO_NAME_TYPE && (!explicit_only || type_name.via_import)) {
        const oo_type_t *type = oo_unalias(w->ix, type_name.type);
        if (!type)
            return (oo_select_t){.status = OO_SEL_AMBIGUOUS};
        *type_value = oo_value_of_type(type);
        if (type->flags & OO_F_OBJECT)
            return oo_select_method(w, type_value, "invoke", argc, args, false);
        oo_select_t selected = oo_select_constructor(w, type_value, argc, args);
        if (selected.status != OO_SEL_UNIQUE && selected.status != OO_SEL_AMBIGUOUS &&
            type->companion) {
            oo_value_t companion = oo_value_of_type(type->companion);
            oo_select_t invoke = oo_select_method(w, &companion, "invoke", argc, args, false);
            if (invoke.status == OO_SEL_UNIQUE || invoke.status == OO_SEL_AMBIGUOUS)
                return invoke;
        }
        *constructor = true;
        return selected;
    }
    return (oo_select_t){.status = OO_SEL_NONE};
}

static const oo_value_t *kw_arg_values(const kw_args_t *args) {
    return args->overflow ? NULL : args->values;
}

static void kw_emit_constructor(kw_t *k, TSNode site, const oo_select_t *selected,
                                const oo_value_t *type_value) {
    oo_walk_t *w = k->w;
    if (selected->status == OO_SEL_UNIQUE && selected->callable->graph_qn) {
        oo_emit_call(w, site, selected->callable->graph_qn, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                     CBM_RESOLVE_FACT_LANGUAGE_BOUND, "kotlin-constructor-selection");
    } else if ((selected->status == OO_SEL_UNIQUE || selected->status == OO_SEL_NONE) &&
               type_value->type && type_value->type->graph_qn &&
               !(type_value->type->flags & OO_F_INTERFACE)) {
        /* A primary constructor has no node of its own: the class stands for it. */
        oo_emit_call(w, site, type_value->type->graph_qn, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                     CBM_RESOLVE_FACT_LANGUAGE_BOUND | CBM_RESOLVE_FACT_CLASS_CALLABLE,
                     "kotlin-class-constructor");
    }
}

static const oo_typeref_t **kw_type_args(kw_t *k, TSNode suffix) {
    TSNode type_args = kt_child(suffix, "type_arguments");
    uint32_t count = ts_node_is_null(type_args) ? 0 : ts_node_named_child_count(type_args);
    if (!count)
        return NULL;
    const oo_typeref_t **args = cbm_arena_calloc(k->w->arena, (count + 1) * sizeof(*args));
    if (!args)
        return NULL;
    for (uint32_t i = 0; i < count; i++) {
        args[i] = kt_typeref(&k->text, ts_node_named_child(type_args, i));
        if (!args[i])
            args[i] = cbm_arena_calloc(k->w->arena, sizeof(oo_typeref_t));
    }
    return args;
}

/* The standard collection factories: their element is the explicit type
 * argument, or the first argument's type. A project function of the name was
 * selected before this is consulted. */
static oo_value_t kt_factory_value(kw_t *k, const char *name, TSNode suffix,
                                   const kw_args_t *args) {
    static const char *const factories[] = {
        "listOf",    "mutableListOf", "arrayListOf", "listOfNotNull", "setOf", "mutableSetOf",
        "hashSetOf", "linkedSetOf",   "sequenceOf",  "arrayOf",       NULL};
    bool known = false;
    for (int i = 0; factories[i]; i++)
        known = known || !strcmp(factories[i], name);
    if (!known)
        return oo_value_unknown();
    oo_value_t element = args->count && !args->lambda[0] ? args->values[0] : oo_value_unknown();
    const oo_typeref_t **type_args = kw_type_args(k, suffix);
    if (type_args && type_args[0] && !type_args[1])
        element = kw_ref(k, type_args[0]);
    element.classref = false;
    oo_value_t *slot = cbm_arena_alloc(k->w->arena, sizeof(*slot));
    if (!slot || !oo_value_known(&element))
        return oo_value_unknown();
    *slot = element;
    oo_value_t value = oo_value_external("List");
    value.args = slot;
    value.arg_count = 1;
    return value;
}

static oo_value_t kw_bare_call_with(kw_t *k, TSNode node, TSNode callee, TSNode suffix,
                                    kw_args_t *args) {
    oo_walk_t *w = k->w;
    const char *name = kt_name(&k->text, callee);
    if (!args || !name) {
        kw_children(k, node);
        return oo_value_unknown();
    }
    kw_collect_args(k, suffix, args);
    int argc = args->overflow ? -1 : args->count;
    const oo_value_t *values = kw_arg_values(args);
    /* A local binding of the name: a local function, or an invocable value.
     * Neither has a declaration this walk can name. */
    const oo_local_t *local = oo_lookup_local(w, name);
    if (local) {
        oo_select_t invoke = local->value.type ? kt_select_member(w, &w->scope, &local->value,
                                                                  "invoke", argc, values)
                                               : (oo_select_t){.status = OO_SEL_NONE};
        if (invoke.status == OO_SEL_UNIQUE) {
            if (invoke.callable->graph_qn)
                oo_emit_call(w, node, invoke.callable->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                             CBM_RESOLVE_FACT_RECEIVER_TYPED, "kotlin-local-invoke");
            kw_lambda_ctx_t ctx = {0};
            (void)kw_finish_lambdas(k, args, invoke.callable, invoke.subst, &ctx);
            return oo_callable_return(w, invoke.callable, invoke.subst);
        }
        kw_lambda_ctx_t ctx = {.no_receiver = true};
        (void)kw_finish_lambdas(k, args, NULL, NULL, &ctx);
        return local->value.fn_return ? *local->value.fn_return : oo_value_unknown();
    }
    kw_implicit_t implicit = kw_implicit_member(k, callee, name, argc, values);
    const oo_callable_t *target = NULL;
    const oo_subst_t *subst = NULL;
    oo_value_t result = oo_value_unknown();
    bool constructor = false;
    oo_value_t type_value = oo_value_unknown();
    if (implicit.selected.status == OO_SEL_UNIQUE) {
        target = implicit.selected.callable;
        subst = implicit.selected.subst;
        if (target->graph_qn)
            oo_emit_call(w, node, target->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                         CBM_RESOLVE_FACT_RECEIVER_TYPED,
                         implicit.extension ? "kotlin-implicit-receiver-extension"
                                            : "kotlin-implicit-receiver-member");
        result = oo_callable_return(w, target, subst);
    } else if (!implicit.stop && !oo_closed_member(w, name)) {
        /* An unlistable implicit receiver could hide a free function. An explicit
         * import spells out this very name, and would be useless if it did. */
        oo_select_t selected = kw_free_call(k, name, argc, values, &constructor, &type_value,
                                            implicit.unknown_receiver);
        if (constructor) {
            kw_emit_constructor(k, node, &selected, &type_value);
            result = type_value;
            const oo_typeref_t **type_args = kw_type_args(k, suffix);
            if (type_args && type_value.type) {
                oo_typeref_t ref = {.name = name, .args = type_args};
                oo_value_t typed = kw_ref(k, &ref);
                if (typed.type == type_value.type)
                    result = typed;
            }
            if (selected.status == OO_SEL_UNIQUE)
                target = selected.callable;
        } else if (selected.status == OO_SEL_UNIQUE) {
            target = selected.callable;
            if (target->graph_qn) {
                oo_emit_call(w, node, target->graph_qn, CBM_RESOLVE_RULE_IMPORT_BINDING,
                             CBM_RESOLVE_FACT_IMPORT_BOUND, "kotlin-free-function-lookup");
            }
            result = oo_callable_return(w, target, NULL);
        }
    }
    if (!target && !constructor && !implicit.stop && implicit.unknown_receiver &&
        oo_type_shaped(name)) {
        /* Members are named in lower case by convention: a type-shaped name is
         * not plausibly a member of the unknown receiver. Only an exact
         * classifier lookup may then read it as a constructor. */
        oo_name_t type_name = oo_resolve_name(w->ix, &w->scope, name);
        if (type_name.status == OO_NAME_TYPE) {
            const oo_type_t *type = oo_unalias(w->ix, type_name.type);
            if (type) {
                type_value = oo_value_of_type(type);
                oo_select_t selected = oo_select_constructor(w, &type_value, argc, values);
                kw_emit_constructor(k, node, &selected, &type_value);
                result = type_value;
                if (selected.status == OO_SEL_UNIQUE)
                    target = selected.callable;
            }
        }
    }
    /* Lambdas run under the selected declaration's parameter types. */
    kw_lambda_ctx_t ctx = {0};
    if (!target) {
        if (!strcmp(name, "with") && args->count == 2 && !args->lambda[0] && args->lambda[1]) {
            ctx.self = args->values[0];
            ctx.has_self = oo_value_known(&ctx.self);
            ctx.result_is_lambda = true;
        } else if (!strcmp(name, "run")) {
            ctx.no_receiver = true;
            ctx.result_is_lambda = true;
        } else {
            kt_stdlib_lambda(w, name, NULL, &ctx);
        }
    }
    oo_value_t lambda_value = kw_finish_lambdas(k, args, target, subst, &ctx);
    if (!target && ctx.result_is_lambda)
        return lambda_value;
    if (!target && !constructor && !oo_value_known(&result))
        return kt_factory_value(k, name, suffix, args);
    return result;
}

static oo_value_t kw_bare_call(kw_t *k, TSNode node, TSNode callee, TSNode suffix) {
    kw_args_t *args = kw_args_take(k);
    oo_value_t value = kw_bare_call_with(k, node, callee, suffix, args);
    kw_args_give(k, args);
    return value;
}

/* The result of a standard collection transform. */
static oo_value_t kt_list_of(oo_walk_t *w, oo_value_t element) {
    oo_value_t *slot = cbm_arena_alloc(w->arena, sizeof(*slot));
    if (!slot || !oo_value_known(&element))
        return oo_value_unknown();
    *slot = element;
    slot->classref = false;
    oo_value_t list = oo_value_external("List");
    list.args = slot;
    list.arg_count = 1;
    return list;
}

static oo_value_t kw_member_call_with(kw_t *k, TSNode node, TSNode navigation, TSNode suffix,
                                      kw_args_t *args) {
    oo_walk_t *w = k->w;
    uint32_t parts = ts_node_named_child_count(navigation);
    TSNode receiver_node = parts ? ts_node_named_child(navigation, 0) : (TSNode){0};
    TSNode member = kt_child(navigation, "navigation_suffix");
    TSNode name_node = kt_child(member, "simple_identifier");
    const char *name = kt_name(&k->text, name_node);
    oo_value_t receiver = oo_value_unknown();
    bool super_call = kt_is(receiver_node, "super_expression");
    if (!super_call)
        receiver = kw_expr(k, receiver_node);
    if (!args || !name) {
        if (args)
            kw_collect_args(k, suffix, args);
        return oo_value_unknown();
    }
    kw_collect_args(k, suffix, args);
    int argc = args->overflow ? -1 : args->count;
    const oo_value_t *values = kw_arg_values(args);
    const oo_callable_t *target = NULL;
    const oo_subst_t *subst = NULL;
    oo_value_t result = oo_value_unknown();
    if (super_call) {
        /* super.m() or super<T>.m(): the enclosing class's direct supertypes. */
        for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
            if (r->unknown || !r->value.type || r->extension)
                continue;
            const oo_type_t *only = NULL;
            bool conflict = false;
            for (int i = 0; i < r->value.type->super_count; i++) {
                const oo_type_t *candidate = r->value.type->supers[i].resolved;
                if (!candidate)
                    continue;
                oo_value_t value = oo_value_of_type(candidate);
                if (!oo_has_method_named(w, &value, name))
                    continue;
                if (only && only != candidate)
                    conflict = true;
                only = candidate;
            }
            if (only && !conflict) {
                oo_value_t value = oo_value_of_type(only);
                oo_select_t selected = oo_select_method(w, &value, name, argc, values, false);
                if (selected.status == OO_SEL_UNIQUE) {
                    target = selected.callable;
                    subst = selected.subst;
                    if (target->graph_qn)
                        oo_emit_call(w, node, target->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                                     CBM_RESOLVE_FACT_RECEIVER_TYPED, "kotlin-super-member");
                    result = oo_callable_return(w, target, subst);
                }
            }
            break;
        }
    } else if (receiver.classref && receiver.type && !(receiver.type->flags & OO_F_OBJECT) &&
               oo_find_nested(w->ix, receiver.type, name)) {
        /* `Outer.Inner()` constructs a nested class. */
        const oo_type_t *nested = oo_find_nested(w->ix, receiver.type, name);
        if (!(nested->flags & OO_F_AMBIGUOUS)) {
            oo_value_t type_value = oo_value_of_type(nested);
            oo_select_t selected = oo_select_constructor(w, &type_value, argc, values);
            kw_emit_constructor(k, node, &selected, &type_value);
            if (selected.status == OO_SEL_UNIQUE)
                target = selected.callable;
            result = type_value;
        }
    } else if (receiver.type || receiver.external) {
        oo_select_t selected = kt_select_member(w, &w->scope, &receiver, name, argc, values);
        if (selected.status == OO_SEL_UNIQUE) {
            target = selected.callable;
            subst = selected.subst;
            if (target->graph_qn)
                oo_emit_call(w, node, target->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                             CBM_RESOLVE_FACT_RECEIVER_TYPED, "kotlin-receiver-member");
            result = oo_callable_return(w, target, subst);
        }
    }
    kw_lambda_ctx_t ctx = {0};
    if (!target)
        kt_stdlib_lambda(w, name, &receiver, &ctx);
    /* `Users.insert { ... }`: the receiver settles a `T.() -> Unit`'s T. */
    const oo_subst_t *lambda_subst =
        target && !args->overflow
            ? oo_infer_call_subst(w, target, subst, &receiver, kw_arg_values(args), args->count)
            : subst;
    oo_value_t lambda_value = kw_finish_lambdas(k, args, target, lambda_subst, &ctx);
    if (!target) {
        if (ctx.result_is_lambda)
            return lambda_value;
        if (ctx.result_is_self) {
            oo_value_t same = receiver;
            same.classref = false;
            return same;
        }
        if (ctx.result_is_same) {
            oo_value_t same = receiver;
            same.classref = false;
            return receiver.type ? kt_list_of(w, oo_element_of(w, &receiver)) : same;
        }
        if (ctx.result_is_element)
            return oo_element_of(w, &receiver);
        /* `map` and friends collect what the lambda produced. */
        if (args->count && args->lambda[args->count - 1] && oo_value_known(&lambda_value) &&
            (!strcmp(name, "map") || !strcmp(name, "mapNotNull") || !strcmp(name, "mapIndexed")))
            return kt_list_of(w, lambda_value);
        /* Standard accessors on a project type that is itself a collection. */
        if (receiver.type && (!strcmp(name, "first") || !strcmp(name, "last") ||
                              !strcmp(name, "single") || !strcmp(name, "firstOrNull") ||
                              !strcmp(name, "lastOrNull") || !strcmp(name, "singleOrNull")))
            return oo_element_of(w, &receiver);
        if (receiver.type &&
            (!strcmp(name, "toList") || !strcmp(name, "toMutableList") || !strcmp(name, "toSet") ||
             !strcmp(name, "asSequence") || !strcmp(name, "asIterable") ||
             !strcmp(name, "reversed") || !strcmp(name, "take") || !strcmp(name, "drop") ||
             !strcmp(name, "distinct") || !strcmp(name, "filterNotNull")))
            return kt_list_of(w, oo_element_of(w, &receiver));
        if (receiver.external)
            return oo_external_member_return(w, &receiver, name);
    }
    return result;
}

static oo_value_t kw_member_call(kw_t *k, TSNode node, TSNode navigation, TSNode suffix) {
    kw_args_t *args = kw_args_take(k);
    oo_value_t value = kw_member_call_with(k, node, navigation, suffix, args);
    kw_args_give(k, args);
    return value;
}

/* An object expression: its body is a class body the grammar sometimes reads
 * as a trailing lambda. */
static oo_value_t kw_object_body(kw_t *k, TSNode object, TSNode members) {
    oo_walk_t *w = k->w;
    uint32_t count = ts_node_named_child_count(object);
    int pushed = 0;
    oo_value_t only_super = oo_value_unknown();
    int supers = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(object, i);
        if (!kt_is(child, "delegation_specifier"))
            continue;
        TSNode constructor = kt_child(child, "constructor_invocation");
        TSNode user = !ts_node_is_null(constructor) ? kt_child(constructor, "user_type")
                                                    : kt_child(child, "user_type");
        kw_type_mentions(k, user, 0);
        if (!ts_node_is_null(constructor))
            kw_children(k, kt_child(constructor, "value_arguments"));
        oo_value_t value = kw_ref(k, kt_typeref(&k->text, user));
        only_super = value;
        supers++;
        oo_receiver_t *receiver = oo_receiver_push(w, value, false);
        if (receiver) {
            receiver->unknown = !value.type;
            receiver->object_super = true;
            pushed++;
        }
    }
    /* The object's own members hide everything else and have no index entry. */
    oo_hidden_t *hidden = cbm_arena_calloc(w->arena, sizeof(*hidden));
    oo_receiver_t *own = oo_receiver_push(w, oo_value_unknown(), false);
    if (own) {
        pushed++;
        own->anonymous = true;
        uint32_t members_count = ts_node_is_null(members) ? 0 : ts_node_named_child_count(members);
        const char **names =
            members_count ? cbm_arena_calloc(w->arena, members_count * sizeof(*names)) : NULL;
        bool *functions =
            members_count ? cbm_arena_calloc(w->arena, members_count * sizeof(*functions)) : NULL;
        oo_value_t *values =
            members_count ? cbm_arena_calloc(w->arena, members_count * sizeof(*values)) : NULL;
        int n = 0;
        for (uint32_t i = 0; i < members_count; i++) {
            TSNode member = ts_node_named_child(members, i);
            const char *name = NULL;
            bool function = kt_is(member, "function_declaration");
            if (function)
                name = kt_name(&k->text, kt_child(member, "simple_identifier"));
            else if (kt_is(member, "property_declaration"))
                name = kt_name(&k->text, kt_child(kt_child(member, "variable_declaration"),
                                                  "simple_identifier"));
            if (name && names && functions) {
                functions[n] = function;
                names[n++] = name;
            }
        }
        own->own_names = names;
        own->own_count = n;
        own->own_function = functions;
        own->own_values = values;
        if (hidden) {
            hidden->names = names;
            hidden->count = n;
            hidden->values = values;
        }
    }
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    oo_hidden_t *outer_members = k->object_members;
    k->object_members = hidden;
    kw_members(k, members, NULL);
    k->object_members = outer_members;
    oo_frame_pop(w);
    for (int i = 0; i < pushed; i++)
        oo_receiver_pop(w);
    /* Reached from outside, it is a value of its one supertype, minus what it
     * declares itself. Several supertypes are an intersection this does not model. */
    if (supers != 1 || !hidden || !only_super.type)
        return oo_value_unknown();
    only_super.hidden = hidden;
    return only_super;
}

static oo_value_t kw_call(kw_t *k, TSNode node) {
    uint32_t parts = ts_node_named_child_count(node);
    TSNode callee = parts ? ts_node_named_child(node, 0) : (TSNode){0};
    TSNode suffix = kt_child(node, "call_suffix");
    if (kt_is(callee, "object_literal")) {
        /* The grammar's reading of `object : T(...) { members }`. */
        TSNode lambda = kt_child(kt_child(suffix, "annotated_lambda"), "lambda_literal");
        return kw_object_body(k, callee, kt_child(lambda, "statements"));
    }
    if (ts_node_is_null(suffix)) {
        kw_children(k, node);
        return oo_value_unknown();
    }
    if (kt_is(callee, "simple_identifier"))
        return kw_bare_call(k, node, callee, suffix);
    if (kt_is(callee, "navigation_expression"))
        return kw_member_call(k, node, callee, suffix);
    /* The callee is some other expression: an invocation of its value. */
    oo_value_t value = kw_expr(k, callee);
    kw_args_t *args = kw_args_take(k);
    if (args) {
        kw_collect_args(k, suffix, args);
        kw_lambda_ctx_t ctx = {.no_receiver = true};
        (void)kw_finish_lambdas(k, args, NULL, NULL, &ctx);
        kw_args_give(k, args);
    }
    return value.fn_return ? *value.fn_return : oo_value_unknown();
}

/* A name reached with no call: a variable, member, property, or classifier. */
static oo_value_t kw_identifier(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    const char *name = kt_name(&k->text, node);
    if (!name)
        return oo_value_unknown();
    const oo_local_t *local = oo_lookup_local(w, name);
    if (local)
        return local->value;
    bool unknown = false;
    bool no_outer_instance = false;
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        bool blocked =
            no_outer_instance && !(r->value.type && (r->value.type->flags & OO_F_OBJECT));
        no_outer_instance = no_outer_instance || r->static_context;
        if (blocked)
            continue;
        for (int i = 0; i < r->own_count; i++)
            if (!strcmp(r->own_names[i], name))
                return r->own_values ? r->own_values[i] : oo_value_unknown();
        if (r->unknown) {
            unknown = true;
            continue;
        }
        if (!r->value.type)
            continue;
        /* An anonymous object's own member, which has no node. */
        if (kt_hidden(&r->value, name))
            return kt_hidden_value(&r->value, name);
        oo_subst_t *subst = NULL;
        bool incomplete = false;
        const oo_field_t *field = oo_find_field(w, &r->value, name, &subst, &incomplete);
        if (field && !unknown) {
            if (field->graph_qn) {
                oo_emit_usage(w, node, field->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                              CBM_RESOLVE_RULE_TYPE_MEMBER, "kotlin-implicit-receiver-property");
                oo_emit_rw(w, node, field->graph_qn, "kotlin-implicit-receiver-property");
            }
            return kt_field_type(w, field, subst);
        }
        if (field || incomplete)
            unknown = true;
        if (!unknown) {
            const oo_field_t *extension = kt_extension_property(w, &w->scope, &r->value, name);
            if (extension) {
                if (extension->graph_qn) {
                    oo_emit_usage(w, node, extension->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                                  CBM_RESOLVE_RULE_IMPORT_BINDING,
                                  "kotlin-implicit-receiver-extension-property");
                    oo_emit_rw(w, node, extension->graph_qn,
                               "kotlin-implicit-receiver-extension-property");
                }
                return kt_field_type(w, extension, NULL);
            }
        }
    }
    if (!unknown) {
        const oo_field_t *field = kt_free_property(w, &w->scope, name);
        if (field) {
            if (field->graph_qn)
                oo_emit_usage(w, node, field->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                              CBM_RESOLVE_RULE_IMPORT_BINDING, "kotlin-free-property-lookup");
            return kt_field_type(w, field, NULL);
        }
    } else if (!oo_type_shaped(name)) {
        return oo_value_unknown();
    }
    oo_typeref_t ref = {.name = name};
    oo_value_t value = kw_ref(k, &ref);
    if (value.type) {
        value.classref = true;
        if (value.type->graph_qn)
            oo_emit_usage(w, node, value.type->graph_qn, CBM_RESOLVED_TYPE_REFERENCE,
                          CBM_RESOLVE_RULE_DECLARED_TYPE, "kotlin-type-name-lookup");
        return value;
    }
    if (value.external)
        value.classref = true;
    return value;
}

static oo_value_t kw_navigation(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    uint32_t parts = ts_node_named_child_count(node);
    TSNode receiver_node = parts ? ts_node_named_child(node, 0) : (TSNode){0};
    TSNode suffix = kt_child(node, "navigation_suffix");
    TSNode name_node = kt_child(suffix, "simple_identifier");
    const char *name = kt_name(&k->text, name_node);
    /* A dotted chain can be a package-qualified classifier. */
    const char *text = kt_compact(&k->text, node);
    if (text && strspn(text, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.") ==
                    strlen(text)) {
        size_t head_length = strcspn(text, ".");
        char *head = cbm_arena_strndup(w->arena, text, head_length);
        const oo_type_t *exact =
            head && !oo_lookup_local(w, head) ? oo_lookup_visible(w->ix, text, w->f) : NULL;
        if (exact && !(exact->flags & OO_F_AMBIGUOUS)) {
            bool shadow = false;
            for (const oo_receiver_t *r = w->receiver; r && !shadow; r = r->parent)
                shadow = r->unknown;
            if (!shadow) {
                oo_value_t value = oo_value_of_type(exact);
                value.classref = true;
                return value;
            }
        }
    }
    oo_value_t receiver = kw_expr(k, receiver_node);
    if (name && kt_hidden(&receiver, name))
        return kt_hidden_value(&receiver, name);
    if (!name || (!receiver.type && !receiver.external))
        return oo_value_unknown();
    if (receiver.type) {
        oo_subst_t *subst = NULL;
        const oo_field_t *field = kt_find_property(w, &receiver, name, &subst);
        if (field) {
            if (field->graph_qn) {
                oo_emit_usage(w, name_node, field->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                              CBM_RESOLVE_RULE_TYPE_MEMBER, "kotlin-receiver-property");
                oo_emit_rw(w, name_node, field->graph_qn, "kotlin-receiver-property");
            }
            return kt_field_type(w, field, subst);
        }
        if (receiver.classref) {
            const oo_type_t *nested = oo_find_nested(w->ix, receiver.type, name);
            if (nested && !(nested->flags & OO_F_AMBIGUOUS)) {
                oo_value_t value = oo_value_of_type(nested);
                value.classref = true;
                if (value.type->graph_qn)
                    oo_emit_usage(w, name_node, value.type->graph_qn, CBM_RESOLVED_TYPE_REFERENCE,
                                  CBM_RESOLVE_RULE_DECLARED_TYPE, "kotlin-nested-type-name");
                return value;
            }
            /* `Foo.Companion`. */
            if (receiver.type->companion && !strcmp(receiver.type->companion->name, name)) {
                oo_value_t value = oo_value_of_type(receiver.type->companion);
                value.classref = true;
                return value;
            }
        }
    }
    if (!receiver.classref) {
        const oo_field_t *extension = kt_extension_property(w, &w->scope, &receiver, name);
        if (extension) {
            if (extension->graph_qn) {
                oo_emit_usage(w, name_node, extension->graph_qn, CBM_RESOLVED_VALUE_REFERENCE,
                              CBM_RESOLVE_RULE_IMPORT_BINDING, "kotlin-extension-property");
                oo_emit_rw(w, name_node, extension->graph_qn, "kotlin-extension-property");
            }
            return kt_field_type(w, extension, NULL);
        }
    }
    /* `entry.value`, `pair.first`. */
    return receiver.external ? oo_external_member_return(w, &receiver, name) : oo_value_unknown();
}

/* The operator method a desugared expression calls, at the expression's span. */
static oo_value_t kw_operator(kw_t *k, TSNode site, const char *method, const oo_value_t *receiver,
                              const oo_value_t *args, int argc) {
    oo_walk_t *w = k->w;
    if (!receiver->type && !receiver->external)
        return oo_value_unknown();
    oo_value_t value = *receiver;
    value.classref = false;
    oo_select_t selected = kt_select_member(w, &w->scope, &value, method, argc, args);
    if (selected.status != OO_SEL_UNIQUE)
        return oo_value_unknown();
    if (selected.callable->graph_qn)
        oo_emit_call(w, site, selected.callable->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                     CBM_RESOLVE_FACT_RECEIVER_TYPED, "kotlin-operator-convention");
    return oo_callable_return(w, selected.callable, selected.subst);
}

static const char *kt_binary_method(const kw_t *k, TSNode node) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        if (ts_node_is_named(child))
            continue;
        const char *op = ts_node_type(child);
        if (!strcmp(op, "+"))
            return "plus";
        if (!strcmp(op, "-"))
            return "minus";
        if (!strcmp(op, "*"))
            return "times";
        if (!strcmp(op, "/"))
            return "div";
        if (!strcmp(op, "%"))
            return "rem";
        if (!strcmp(op, ".."))
            return "rangeTo";
        if (!strcmp(op, "..<"))
            return "rangeUntil";
        if (!strcmp(op, "<") || !strcmp(op, ">") || !strcmp(op, "<=") || !strcmp(op, ">="))
            return "compareTo";
        if (!strcmp(op, "==") || !strcmp(op, "!="))
            return "equals";
    }
    (void)k;
    return NULL;
}

static oo_value_t kw_binary(kw_t *k, TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    if (count < 2) {
        kw_children(k, node);
        return oo_value_unknown();
    }
    oo_value_t left = kw_expr(k, ts_node_named_child(node, 0));
    oo_value_t right = kw_expr(k, ts_node_named_child(node, count - 1));
    const char *method = kt_binary_method(k, node);
    if (!method)
        return oo_value_unknown();
    oo_value_t args[1] = {right};
    oo_value_t value = kw_operator(k, node, method, &left, args, 1);
    /* compareTo and equals yield Boolean through the comparison itself. */
    if (!strcmp(method, "compareTo") || !strcmp(method, "equals"))
        return oo_value_external("Boolean");
    return value;
}

static oo_value_t kw_check(kw_t *k, TSNode node) {
    /* `a in b` is b.contains(a); `a is T` tests a type. */
    uint32_t count = ts_node_named_child_count(node);
    bool in = !ts_node_is_null(kt_child(node, "in")) || !ts_node_is_null(kt_child(node, "!in"));
    if (count < 2) {
        kw_children(k, node);
        return oo_value_external("Boolean");
    }
    oo_value_t left = kw_expr(k, ts_node_named_child(node, 0));
    TSNode right_node = ts_node_named_child(node, count - 1);
    if (in) {
        oo_value_t right = kw_expr(k, right_node);
        oo_value_t args[1] = {left};
        (void)kw_operator(k, node, "contains", &right, args, 1);
    } else {
        kw_type_mentions(k, right_node, 0);
    }
    return oo_value_external("Boolean");
}

static oo_value_t kw_infix(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    uint32_t count = ts_node_named_child_count(node);
    if (count != 3) {
        kw_children(k, node);
        return oo_value_unknown();
    }
    oo_value_t left = kw_expr(k, ts_node_named_child(node, 0));
    TSNode name_node = ts_node_named_child(node, 1);
    const char *name = kt_name(&k->text, name_node);
    oo_value_t right = kw_expr(k, ts_node_named_child(node, 2));
    if (!name || (!left.type && !left.external))
        return oo_value_unknown();
    oo_value_t args[1] = {right};
    left.classref = false;
    oo_select_t selected = kt_select_member(w, &w->scope, &left, name, 1, args);
    if (selected.status != OO_SEL_UNIQUE)
        return oo_value_unknown();
    if (selected.callable->graph_qn)
        oo_emit_call(w, node, selected.callable->graph_qn, CBM_RESOLVE_RULE_TYPE_MEMBER,
                     CBM_RESOLVE_FACT_RECEIVER_TYPED, "kotlin-infix-call");
    return oo_callable_return(w, selected.callable, selected.subst);
}

static oo_value_t kw_indexing(kw_t *k, TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    TSNode receiver_node = count ? ts_node_named_child(node, 0) : (TSNode){0};
    oo_value_t receiver = kw_expr(k, receiver_node);
    TSNode suffix = kt_child(node, "indexing_suffix");
    int argc = 0;
    oo_value_t args[8];
    uint32_t parts = ts_node_is_null(suffix) ? 0 : ts_node_named_child_count(suffix);
    for (uint32_t i = 0; i < parts; i++) {
        oo_value_t value = kw_expr(k, ts_node_named_child(suffix, i));
        if (argc < 8)
            args[argc] = value;
        argc++;
    }
    if (argc > 8)
        return oo_value_unknown();
    oo_value_t value = kw_operator(k, node, "get", &receiver, args, argc);
    if (oo_value_known(&value))
        return value;
    if (receiver.dims)
        return oo_element_of(k->w, &receiver);
    return oo_external_member_return(k->w, &receiver, "get");
}

/* Type narrowing that the language itself applies to a stable local. */
static void kw_narrow_from(kw_t *k, TSNode condition) {
    if (kt_is(condition, "conjunction_expression")) {
        uint32_t count = ts_node_named_child_count(condition);
        for (uint32_t i = 0; i < count; i++)
            kw_narrow_from(k, ts_node_named_child(condition, i));
        return;
    }
    if (kt_is(condition, "parenthesized_expression") && ts_node_named_child_count(condition) == 1) {
        kw_narrow_from(k, ts_node_named_child(condition, 0));
        return;
    }
    if (!kt_is(condition, "check_expression") || !ts_node_is_null(kt_child(condition, "!is")))
        return;
    uint32_t count = ts_node_named_child_count(condition);
    if (count != 2 || ts_node_is_null(kt_child(condition, "is")))
        return;
    TSNode subject = ts_node_named_child(condition, 0);
    if (!kt_is(subject, "simple_identifier"))
        return;
    const char *name = kt_name(&k->text, subject);
    /* Only a binding this walk owns can be proved stable within its branch. */
    const oo_local_t *local = oo_lookup_local(k->w, name);
    if (!local)
        return;
    oo_value_t value = kw_ref(k, kt_typeref(&k->text, ts_node_named_child(condition, 1)));
    if (value.type)
        oo_bind(k->w, name, value);
}

static oo_value_t kw_if(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    TSNode condition = ts_node_child_by_field_name(node, "condition", 9);
    (void)kw_expr(k, condition);
    TSNode consequence = ts_node_child_by_field_name(node, "consequence", 11);
    TSNode alternative = ts_node_child_by_field_name(node, "alternative", 11);
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    kw_narrow_from(k, condition);
    oo_value_t first = kw_expr(k, consequence);
    oo_frame_pop(w);
    oo_frame_push(w, &frame);
    oo_value_t second = kw_expr(k, alternative);
    oo_frame_pop(w);
    return first.type && first.type == second.type ? first : oo_value_unknown();
}

static oo_value_t kw_when(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    oo_frame_t outer;
    oo_frame_push(w, &outer);
    TSNode subject = kt_child(node, "when_subject");
    const char *subject_name = NULL;
    if (!ts_node_is_null(subject)) {
        TSNode variable = kt_child(subject, "variable_declaration");
        uint32_t parts = ts_node_named_child_count(subject);
        TSNode value_node = parts ? ts_node_named_child(subject, parts - 1) : (TSNode){0};
        oo_value_t value = kw_expr(k, value_node);
        if (!ts_node_is_null(variable)) {
            subject_name = kt_name(&k->text, kt_child(variable, "simple_identifier"));
            TSNode declared = kt_after_colon(variable, "=", NULL);
            oo_bind(w, subject_name,
                    ts_node_is_null(declared) ? value : kw_ref(k, kt_typeref(&k->text, declared)));
        } else if (kt_is(value_node, "simple_identifier")) {
            const char *name = kt_name(&k->text, value_node);
            if (oo_lookup_local(w, name))
                subject_name = name;
        }
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode entry = ts_node_named_child(node, i);
        if (!kt_is(entry, "when_entry"))
            continue;
        oo_frame_t branch;
        oo_frame_push(w, &branch);
        uint32_t parts = ts_node_named_child_count(entry);
        for (uint32_t p = 0; p < parts; p++) {
            TSNode part = ts_node_named_child(entry, p);
            if (kt_is(part, "when_condition")) {
                TSNode test = kt_child(part, "type_test");
                if (!ts_node_is_null(test)) {
                    uint32_t test_parts = ts_node_named_child_count(test);
                    TSNode type =
                        test_parts ? ts_node_named_child(test, test_parts - 1) : (TSNode){0};
                    kw_type_mentions(k, type, 0);
                    /* `is T` on a subject narrows it inside a single-condition entry. */
                    if (subject_name && parts == 2) {
                        const char *text = kt_text(&k->text, test);
                        if (text && strncmp(text, "!is", 3)) {
                            oo_value_t value = kw_ref(k, kt_typeref(&k->text, type));
                            if (value.type)
                                oo_bind(w, subject_name, value);
                        }
                    }
                } else {
                    kw_children(k, part);
                }
            } else {
                (void)kw_expr(k, part);
            }
        }
        oo_frame_pop(w);
    }
    oo_frame_pop(w);
    return oo_value_unknown();
}

/* A local declaration. `val x = e` takes e's type; an annotation wins. */
static oo_value_t kw_property(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    TSNode variable = kt_child(node, "variable_declaration");
    TSNode multi = kt_child(node, "multi_variable_declaration");
    TSNode delegate = kt_child(node, "property_delegate");
    oo_value_t initial = oo_value_unknown();
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (kt_is(child, "variable_declaration") || kt_is(child, "multi_variable_declaration") ||
            kt_is(child, "modifiers") || kt_is(child, "binding_pattern_kind") ||
            kt_is(child, "type_parameters") || kt_is(child, "receiver_type") ||
            kt_is(child, "type_constraints"))
            continue;
        if (kt_is(child, "getter") || kt_is(child, "setter")) {
            kw_children(k, child);
            continue;
        }
        oo_value_t value = kw_expr(k, child);
        if (!kt_is(child, "property_delegate"))
            initial = value;
    }
    if (!ts_node_is_null(multi)) {
        kw_bind_destructured(k, multi, &initial);
    } else if (!ts_node_is_null(variable)) {
        const char *name = kt_name(&k->text, kt_child(variable, "simple_identifier"));
        TSNode declared = kt_after_colon(variable, "=", NULL);
        oo_value_t value = oo_value_unknown();
        if (!ts_node_is_null(declared)) {
            kw_type_mentions(k, declared, 0);
            value = kw_ref(k, kt_typeref(&k->text, declared));
        } else if (ts_node_is_null(delegate)) {
            value = initial;
            value.classref = false;
        }
        oo_bind(w, name, value);
    }
    return oo_value_unknown();
}

static oo_value_t kw_for(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    oo_frame_t frame;
    TSNode variable = kt_child(node, "variable_declaration");
    TSNode multi = kt_child(node, "multi_variable_declaration");
    TSNode body = kt_child(node, "control_structure_body");
    uint32_t count = ts_node_named_child_count(node);
    oo_value_t iterable = oo_value_unknown();
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!kt_is(child, "variable_declaration") && !kt_is(child, "multi_variable_declaration") &&
            !kt_is(child, "control_structure_body") && !kt_is(child, "annotation"))
            iterable = kw_expr(k, child);
    }
    oo_frame_push(w, &frame);
    if (!ts_node_is_null(multi)) {
        oo_value_t element = oo_element_of(w, &iterable);
        kw_bind_destructured(k, multi, &element);
    } else if (!ts_node_is_null(variable)) {
        const char *name = kt_name(&k->text, kt_child(variable, "simple_identifier"));
        TSNode declared = kt_after_colon(variable, "=", NULL);
        oo_bind(w, name,
                ts_node_is_null(declared) ? oo_element_of(w, &iterable)
                                          : kw_ref(k, kt_typeref(&k->text, declared)));
    }
    (void)kw_expr(k, body);
    oo_frame_pop(w);
    return oo_value_unknown();
}

static oo_value_t kw_this(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    TSNode label = kt_child(node, "type_identifier");
    const char *wanted = ts_node_is_null(label) ? NULL : kt_name(&k->text, label);
    for (const oo_receiver_t *r = w->receiver; r; r = r->parent) {
        if (r->anonymous)
            return oo_value_unknown();
        if (!wanted) {
            if (r->unknown)
                return oo_value_unknown();
            oo_value_t value = r->value;
            value.classref = false;
            return value;
        }
        if (!r->unknown && r->value.type && !strcmp(r->value.type->name, wanted)) {
            oo_value_t value = r->value;
            value.classref = false;
            return value;
        }
    }
    return oo_value_unknown();
}

/* `if (c) ::a else ::b` selects at run time and `"k" to ::a` builds a larger value, so the
 * leaf is not the one callable this occurrence hands over. It stays an ordinary reference
 * to what it names, as a Java ternary's alternatives do. */
static bool kw_reference_is_composite_leaf(TSNode reference) {
    for (TSNode parent = ts_node_parent(reference); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (kt_is(parent, "if_expression") || kt_is(parent, "when_expression") ||
            kt_is(parent, "elvis_expression") || kt_is(parent, "infix_expression"))
            return true;
        if (kt_is(parent, "value_arguments") || kt_is(parent, "assignment") ||
            kt_is(parent, "property_declaration") || kt_is(parent, "statements"))
            return false;
    }
    return false;
}

static oo_value_t kw_callable_reference(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    TSNode member = kt_child(node, "simple_identifier");
    TSNode receiver_node = kt_child(node, "type_identifier");
    uint32_t parts = ts_node_named_child_count(node);
    if (ts_node_is_null(member)) {
        kw_children(k, node);
        return oo_value_unknown();
    }
    const char *name = kt_name(&k->text, member);
    if (!name)
        return oo_value_unknown();
    const oo_callable_t *target = NULL;
    if (parts == 1) {
        /* `::name`: every declaration of the name competes. */
        kw_implicit_t implicit = kw_implicit_member(k, member, name, -1, NULL);
        if (implicit.selected.status == OO_SEL_UNIQUE && !implicit.selected.overload_set)
            target = implicit.selected.callable;
        else if (!implicit.stop && !implicit.unknown_receiver && !oo_lookup_local(w, name)) {
            bool constructor = false;
            oo_value_t type_value = oo_value_unknown();
            oo_select_t selected =
                kw_free_call(k, name, -1, NULL, &constructor, &type_value, false);
            /* Overloads share a node, but this spelling picks none of them. */
            if (selected.status == OO_SEL_UNIQUE && !constructor && !selected.overload_set)
                target = selected.callable;
        }
    } else if (!ts_node_is_null(receiver_node)) {
        /* `x::m` or `Type::m`: the grammar spells both as a type identifier. */
        const char *receiver_name = kt_name(&k->text, receiver_node);
        const oo_local_t *local = receiver_name ? oo_lookup_local(w, receiver_name) : NULL;
        oo_value_t receiver = oo_value_unknown();
        if (local) {
            receiver = local->value;
        } else if (receiver_name) {
            oo_typeref_t ref = {.name = receiver_name};
            receiver = kw_ref(k, &ref);
        }
        receiver.classref = false;
        if (receiver.type) {
            oo_select_t selected = oo_select_method(w, &receiver, name, -1, NULL, false);
            if (selected.status == OO_SEL_UNIQUE)
                target = selected.callable;
        }
    }
    if (target && target->graph_qn)
        oo_emit_usage(w, member, target->graph_qn,
                      kw_reference_is_composite_leaf(node) ? CBM_RESOLVED_VALUE_REFERENCE
                                                           : CBM_RESOLVED_CALL_REFERENCE,
                      CBM_RESOLVE_RULE_TYPE_MEMBER, "kotlin-callable-reference");
    return oo_value_unknown();
}

static bool kw_navigation_chain(TSNode target, uint32_t parts) {
    if (parts < 2)
        return false;
    for (uint32_t p = 1; p < parts; p++)
        if (!kt_is(ts_node_named_child(target, p), "navigation_suffix"))
            return false;
    return true;
}

static oo_value_t kw_literal(kw_t *k, TSNode node, const char *kind) {
    if (!strcmp(kind, "string_literal") || !strcmp(kind, "line_string_literal") ||
        !strcmp(kind, "multi_line_string_literal"))
        return oo_value_external("String");
    if (!strcmp(kind, "integer_literal") || !strcmp(kind, "hex_literal") ||
        !strcmp(kind, "bin_literal")) {
        /* A decimal literal past Int's range has type Long (Kotlin spec 2.3.1). */
        const char *digits = kt_compact(&k->text, node);
        bool beyond =
            digits && !strcmp(kind, "integer_literal") &&
            (strlen(digits) > 10 || (strlen(digits) == 10 && strcmp(digits, "2147483647") > 0));
        oo_value_t value = oo_value_external(beyond ? "Long" : "Int");
        value.small_literal = !beyond;
        return value;
    }
    if (!strcmp(kind, "long_literal"))
        return oo_value_external("Long");
    if (!strcmp(kind, "real_literal"))
        return oo_value_external("Double");
    if (!strcmp(kind, "boolean_literal"))
        return oo_value_external("Boolean");
    if (!strcmp(kind, "character_literal"))
        return oo_value_external("Char");
    return oo_value_unknown();
}

static oo_value_t kw_block(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    oo_value_t last = oo_value_unknown();
    uint32_t count = ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        last = kw_expr(k, ts_node_named_child(node, i));
    oo_frame_pop(w);
    return last;
}

static oo_value_t kw_expr_kind(kw_t *k, TSNode node, const char *kind) {
    oo_walk_t *w = k->w;
    if (!strcmp(kind, "call_expression"))
        return kw_call(k, node);
    if (!strcmp(kind, "simple_identifier"))
        return kw_identifier(k, node);
    if (!strcmp(kind, "navigation_expression"))
        return kw_navigation(k, node);
    if (!strcmp(kind, "property_declaration"))
        return kw_property(k, node);
    if (!strcmp(kind, "statements") || !strcmp(kind, "control_structure_body") ||
        !strcmp(kind, "function_body") || !strcmp(kind, "finally_block"))
        return kw_block(k, node);
    if (!strcmp(kind, "additive_expression") || !strcmp(kind, "multiplicative_expression") ||
        !strcmp(kind, "comparison_expression") || !strcmp(kind, "equality_expression") ||
        !strcmp(kind, "range_expression"))
        return kw_binary(k, node);
    if (!strcmp(kind, "check_expression"))
        return kw_check(k, node);
    if (!strcmp(kind, "infix_expression"))
        return kw_infix(k, node);
    if (!strcmp(kind, "indexing_expression"))
        return kw_indexing(k, node);
    if (!strcmp(kind, "if_expression"))
        return kw_if(k, node);
    if (!strcmp(kind, "when_expression"))
        return kw_when(k, node);
    if (!strcmp(kind, "for_statement"))
        return kw_for(k, node);
    if (!strcmp(kind, "this_expression"))
        return kw_this(k, node);
    if (!strcmp(kind, "callable_reference"))
        return kw_callable_reference(k, node);
    if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return kw_expr(k, ts_node_named_child(node, 0));
    if (!strcmp(kind, "as_expression")) {
        uint32_t count = ts_node_named_child_count(node);
        if (count == 2) {
            (void)kw_expr(k, ts_node_named_child(node, 0));
            TSNode type = ts_node_named_child(node, 1);
            kw_type_mentions(k, type, 0);
            return kw_ref(k, kt_typeref(&k->text, type));
        }
    }
    if (!strcmp(kind, "elvis_expression")) {
        uint32_t count = ts_node_named_child_count(node);
        if (count == 2) {
            oo_value_t left = kw_expr(k, ts_node_named_child(node, 0));
            TSNode right_node = ts_node_named_child(node, 1);
            oo_value_t right = kw_expr(k, right_node);
            /* `x ?: return` and `x ?: throw` leave x's type. */
            if (kt_is(right_node, "jump_expression"))
                return left;
            return left.type && left.type == right.type ? left : oo_value_unknown();
        }
    }
    if (!strcmp(kind, "postfix_expression")) {
        uint32_t count = ts_node_named_child_count(node);
        const char *text = kt_text(&k->text, node);
        size_t length = text ? strlen(text) : 0;
        if (count == 1 && length >= 2 && !strcmp(text + length - 2, "!!"))
            return kw_expr(k, ts_node_named_child(node, 0));
        if (count == 1 && length >= 2) {
            oo_value_t value = kw_expr(k, ts_node_named_child(node, 0));
            const char *method = !strcmp(text + length - 2, "++")   ? "inc"
                                 : !strcmp(text + length - 2, "--") ? "dec"
                                                                    : NULL;
            if (method)
                (void)kw_operator(k, node, method, &value, NULL, 0);
            return value;
        }
    }
    if (!strcmp(kind, "prefix_expression")) {
        uint32_t count = ts_node_named_child_count(node);
        if (count >= 1) {
            oo_value_t value = kw_expr(k, ts_node_named_child(node, count - 1));
            uint32_t tokens = ts_node_child_count(node);
            const char *op = tokens ? ts_node_type(ts_node_child(node, 0)) : "";
            const char *method = !strcmp(op, "-")    ? "unaryMinus"
                                 : !strcmp(op, "+")  ? "unaryPlus"
                                 : !strcmp(op, "!")  ? "not"
                                 : !strcmp(op, "++") ? "inc"
                                 : !strcmp(op, "--") ? "dec"
                                                     : NULL;
            if (method) {
                oo_value_t result = kw_operator(k, node, method, &value, NULL, 0);
                return oo_value_known(&result) ? result : value;
            }
            return value;
        }
    }
    if (!strcmp(kind, "assignment")) {
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (kt_is(child, "directly_assignable_expression")) {
                /* The target's receiver chain and the written name. */
                uint32_t parts = ts_node_named_child_count(child);
                if (parts == 1) {
                    (void)kw_expr(k, ts_node_named_child(child, 0));
                } else if (kw_navigation_chain(child, parts)) {
                    /* The grammar flattens `a.b.c`: it reads a and b, and writes c. */
                    oo_value_t receiver = kw_expr(k, ts_node_named_child(child, 0));
                    for (uint32_t p = 1; p < parts; p++) {
                        TSNode name_node =
                            kt_child(ts_node_named_child(child, p), "simple_identifier");
                        const char *name = kt_name(&k->text, name_node);
                        oo_subst_t *subst = NULL;
                        const oo_field_t *field = receiver.type && name
                                                      ? kt_find_property(w, &receiver, name, &subst)
                                                      : NULL;
                        if (field && field->graph_qn) {
                            oo_emit_usage(w, name_node, field->graph_qn,
                                          CBM_RESOLVED_VALUE_REFERENCE,
                                          CBM_RESOLVE_RULE_TYPE_MEMBER, "kotlin-receiver-property");
                            if (p == parts - 1)
                                oo_emit_rw(w, name_node, field->graph_qn,
                                           "kotlin-receiver-property");
                        }
                        receiver = field ? kt_field_type(w, field, subst) : oo_value_unknown();
                    }
                } else {
                    kw_children(k, child);
                }
            } else {
                (void)kw_expr(k, child);
            }
        }
        return oo_value_unknown();
    }
    if (!strcmp(kind, "try_expression")) {
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (kt_is(child, "statements")) {
                (void)kw_block(k, child);
            } else if (kt_is(child, "catch_block")) {
                oo_frame_t frame;
                oo_frame_push(w, &frame);
                const char *name = kt_name(&k->text, kt_child(child, "simple_identifier"));
                TSNode type = kt_after_colon(child, "statements", NULL);
                kw_type_mentions(k, type, 0);
                oo_bind(w, name, kw_ref(k, kt_typeref(&k->text, type)));
                TSNode statements = kt_child(child, "statements");
                if (!ts_node_is_null(statements))
                    (void)kw_block(k, statements);
                oo_frame_pop(w);
            } else {
                (void)kw_expr(k, child);
            }
        }
        return oo_value_unknown();
    }
    if (!strcmp(kind, "lambda_literal")) {
        /* Not an argument of a known call: nothing says it lacks a receiver,
         * unless it is the value of an annotated property. */
        return kw_lambda(k, node, NULL, NULL);
    }
    if (!strcmp(kind, "annotated_lambda")) {
        TSNode lambda = kt_child(node, "lambda_literal");
        return kw_lambda(k, lambda, NULL, NULL);
    }
    if (!strcmp(kind, "anonymous_function")) {
        oo_frame_t frame;
        oo_frame_push(w, &frame);
        uint32_t count = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < count; i++) {
            TSNode child = ts_node_named_child(node, i);
            if (kt_is(child, "function_value_parameters")) {
                uint32_t parameters = ts_node_named_child_count(child);
                for (uint32_t p = 0; p < parameters; p++) {
                    TSNode parameter = ts_node_named_child(child, p);
                    if (kt_is(parameter, "parameter")) {
                        const char *name =
                            kt_name(&k->text, kt_child(parameter, "simple_identifier"));
                        oo_bind(
                            w, name,
                            kw_ref(k, kt_typeref(&k->text, kt_after_colon(parameter, "=", NULL))));
                    }
                }
            } else if (kt_is(child, "function_body")) {
                (void)kw_expr(k, child);
            }
        }
        oo_frame_pop(w);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "object_literal"))
        return kw_object_body(k, node, kt_child(node, "class_body"));
    if (!strcmp(kind, "function_declaration")) {
        /* A local function shadows every other callable of the name. */
        const char *name = kt_name(&k->text, kt_child(node, "simple_identifier"));
        oo_bind(w, name, oo_value_unknown());
        kw_function(k, node);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "class_declaration") || !strcmp(kind, "object_declaration")) {
        const char *name = kt_name(&k->text, kt_child(node, "type_identifier"));
        if (name)
            oo_bind(w, cbm_arena_sprintf(w->arena, "#type:%s", name), oo_value_unknown());
        oo_receiver_t *receiver = oo_receiver_push(w, oo_value_unknown(), false);
        if (receiver)
            receiver->unknown = true;
        TSNode body = kt_child(node, "class_body");
        kw_members(k, body, NULL);
        if (receiver)
            oo_receiver_pop(w);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "jump_expression")) {
        kw_children(k, node);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "string_literal")) {
        kw_children(k, node);
        return oo_value_external("String");
    }
    if (!strcmp(kind, "user_type") || !strcmp(kind, "nullable_type") ||
        !strcmp(kind, "function_type")) {
        kw_type_mentions(k, node, 0);
        return oo_value_unknown();
    }
    if (!strcmp(kind, "annotation") || !strcmp(kind, "modifiers") ||
        !strcmp(kind, "line_comment") || !strcmp(kind, "multiline_comment") ||
        !strcmp(kind, "label") || !strcmp(kind, "string_content"))
        return oo_value_unknown();
    oo_value_t literal = kw_literal(k, node, kind);
    if (oo_value_known(&literal))
        return literal;
    kw_children(k, node);
    return oo_value_unknown();
}

enum { KW_MAX_AST_DEPTH = 384 };

static oo_value_t kw_expr(kw_t *k, TSNode node) {
    if (ts_node_is_null(node) || !oo_budget(k->w) || k->depth >= KW_MAX_AST_DEPTH)
        return oo_value_unknown();
    k->depth++;
    oo_value_t value = kw_expr_kind(k, node, ts_node_type(node));
    k->depth--;
    return value;
}

static void kw_bind_parameters(kw_t *k, TSNode parameters) {
    uint32_t count = ts_node_is_null(parameters) ? 0 : ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (!kt_is(parameter, "parameter") && !kt_is(parameter, "class_parameter")) {
            /* Default values are expressions of the enclosing scope. */
            if (!kt_is(parameter, "parameter_modifiers"))
                (void)kw_expr(k, parameter);
            continue;
        }
        const char *name = kt_name(&k->text, kt_child(parameter, "simple_identifier"));
        TSNode type = kt_after_colon(parameter, "=", NULL);
        kw_type_mentions(k, type, 0);
        oo_bind(k->w, name, kw_ref(k, kt_typeref(&k->text, type)));
    }
}

static void kw_function(kw_t *k, TSNode node) {
    oo_walk_t *w = k->w;
    oo_scope_t saved = w->scope;
    w->scope.tparams = kt_tparams(&k->text, node, &w->scope.tparam_count);
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    TSNode receiver_node = ts_node_child_by_field_name(node, "receiver", 8);
    bool pushed = false;
    if (!ts_node_is_null(receiver_node)) {
        /* An extension: its receiver is the innermost implicit receiver. */
        kw_type_mentions(k, receiver_node, 0);
        oo_value_t value = kw_ref(k, kt_typeref(&k->text, receiver_node));
        oo_receiver_t *receiver = oo_receiver_push(w, value, false);
        if (receiver) {
            receiver->unknown = !value.type;
            receiver->extension = true;
            pushed = true;
        }
    }
    kw_bind_parameters(k, kt_child(node, "function_value_parameters"));
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (kt_is(child, "user_type") || kt_is(child, "nullable_type") ||
            kt_is(child, "function_type"))
            kw_type_mentions(k, child, 0);
    }
    TSNode body = kt_child(node, "function_body");
    if (!ts_node_is_null(body))
        (void)kw_expr(k, body);
    if (pushed)
        oo_receiver_pop(w);
    oo_frame_pop(w);
    w->scope = saved;
}

static void kw_members(kw_t *k, TSNode body, const oo_type_t *owner) {
    oo_walk_t *w = k->w;
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode member = ts_node_named_child(body, i);
        const char *kind = ts_node_type(member);
        if (!strcmp(kind, "function_declaration")) {
            kw_annotations(k, member);
            kw_function(k, member);
        } else if (!strcmp(kind, "property_declaration")) {
            kw_annotations(k, member);
            /* A member property: initializer, accessors, annotation. */
            oo_frame_t frame;
            oo_frame_push(w, &frame);
            TSNode receiver_node = ts_node_child_by_field_name(member, "receiver", 8);
            bool pushed = false;
            oo_scope_t saved = w->scope;
            w->scope.tparams = kt_tparams(&k->text, member, &w->scope.tparam_count);
            if (!ts_node_is_null(receiver_node)) {
                oo_value_t value = kw_ref(k, kt_typeref(&k->text, receiver_node));
                oo_receiver_t *receiver = oo_receiver_push(w, value, false);
                if (receiver) {
                    receiver->unknown = !value.type;
                    pushed = true;
                }
            }
            TSNode variable = kt_child(member, "variable_declaration");
            TSNode declared = kt_after_colon(variable, "=", NULL);
            kw_type_mentions(k, declared, 0);
            oo_value_t held = oo_value_unknown();
            uint32_t parts = ts_node_named_child_count(member);
            for (uint32_t p = 0; p < parts; p++) {
                TSNode part = ts_node_named_child(member, p);
                if (kt_is(part, "variable_declaration") ||
                    kt_is(part, "multi_variable_declaration") || kt_is(part, "modifiers") ||
                    kt_is(part, "binding_pattern_kind") || kt_is(part, "type_parameters") ||
                    kt_is(part, "receiver_type") || kt_is(part, "type_constraints"))
                    continue;
                if (kt_is(part, "getter") || kt_is(part, "setter")) {
                    oo_frame_t accessor;
                    oo_frame_push(w, &accessor);
                    /* A setter's parameter name shadows inside the accessor. */
                    TSNode parameter = kt_child(part, "parameter_with_optional_type");
                    if (!ts_node_is_null(parameter))
                        oo_bind(w, kt_name(&k->text, kt_child(parameter, "simple_identifier")),
                                oo_value_unknown());
                    TSNode simple = kt_child(part, "simple_identifier");
                    if (!ts_node_is_null(simple))
                        oo_bind(w, kt_name(&k->text, simple), oo_value_unknown());
                    oo_bind(w, "field", oo_value_unknown());
                    TSNode accessor_body = kt_child(part, "function_body");
                    if (!ts_node_is_null(accessor_body))
                        (void)kw_expr(k, accessor_body);
                    oo_frame_pop(w);
                    continue;
                }
                if (kt_is(part, "property_delegate")) {
                    kw_children(k, part);
                    continue;
                }
                oo_value_t value = kw_expr(k, part);
                if (!ts_node_eq(part, declared))
                    held = value;
            }
            oo_hidden_t *members = k->object_members;
            if (members && members->values) {
                if (!ts_node_is_null(declared))
                    held = kw_ref(k, kt_typeref(&k->text, declared));
                held.classref = false;
                const char *held_name = kt_name(&k->text, kt_child(variable, "simple_identifier"));
                for (int m = 0; held_name && m < members->count; m++)
                    if (!strcmp(members->names[m], held_name))
                        members->values[m] = held;
            }
            if (pushed)
                oo_receiver_pop(w);
            w->scope = saved;
            oo_frame_pop(w);
        } else if (!strcmp(kind, "class_declaration") || !strcmp(kind, "object_declaration") ||
                   !strcmp(kind, "companion_object")) {
            oo_hidden_t *outer_members = k->object_members;
            k->object_members = NULL;
            kw_class_like(k, member, owner);
            k->object_members = outer_members;
        } else if (!strcmp(kind, "anonymous_initializer")) {
            (void)kw_block(k, kt_child(member, "statements"));
        } else if (!strcmp(kind, "secondary_constructor")) {
            oo_frame_t frame;
            oo_frame_push(w, &frame);
            kw_bind_parameters(k, kt_child(member, "function_value_parameters"));
            TSNode delegation = kt_child(member, "constructor_delegation_call");
            if (!ts_node_is_null(delegation))
                kw_children(k, delegation);
            TSNode statements = kt_child(member, "statements");
            if (!ts_node_is_null(statements))
                (void)kw_block(k, statements);
            oo_frame_pop(w);
        } else if (!strcmp(kind, "enum_entry")) {
            kw_children(k, kt_child(member, "value_arguments"));
            TSNode entry_body = kt_child(member, "class_body");
            if (!ts_node_is_null(entry_body)) {
                /* The entry's body is an anonymous subclass of the enum. */
                oo_receiver_t *own = oo_receiver_push(w, oo_value_unknown(), false);
                if (own) {
                    own->anonymous = true;
                    uint32_t entries = ts_node_named_child_count(entry_body);
                    const char **names =
                        entries ? cbm_arena_calloc(w->arena, entries * sizeof(*names)) : NULL;
                    int n = 0;
                    for (uint32_t e = 0; e < entries; e++) {
                        TSNode entry_member = ts_node_named_child(entry_body, e);
                        if (kt_is(entry_member, "function_declaration") && names) {
                            const char *entry_name =
                                kt_name(&k->text, kt_child(entry_member, "simple_identifier"));
                            if (entry_name)
                                names[n++] = entry_name;
                        }
                    }
                    own->own_names = names;
                    own->own_count = n;
                }
                kw_members(k, entry_body, NULL);
                if (own)
                    oo_receiver_pop(w);
            }
        }
    }
}

static void kw_class_like(kw_t *k, TSNode node, const oo_type_t *owner) {
    oo_walk_t *w = k->w;
    bool companion = kt_is(node, "companion_object");
    TSNode name_node = kt_child(node, "type_identifier");
    const char *name = ts_node_is_null(name_node) ? (companion ? "Companion" : NULL)
                                                  : kt_name(&k->text, name_node);
    if (!name)
        return;
    const char *qn =
        owner ? kt_join(w->arena, owner->qn, name) : kt_join(w->arena, w->f->package, name);
    const oo_type_t *type = qn ? oo_lookup_declared(w->ix, qn, w->rel_path) : NULL;
    /* A class's annotations read the scope around it. */
    kw_annotations(k, node);
    bool usable = type && !(type->flags & OO_F_AMBIGUOUS);
    oo_scope_t saved = w->scope;
    if (type)
        w->scope.owner = type;
    w->scope.tparams = NULL;
    w->scope.tparam_count = 0;
    /* Inside a class its companion's members are implicit receivers too; a
     * nested non-inner class still sees the outer class's companion. */
    int pushed = 0;
    if (usable && type->companion) {
        if (oo_receiver_push(w, oo_value_of_type(type->companion), false))
            pushed++;
    }
    /* Only an inner class holds an outer instance. */
    oo_receiver_t *receiver =
        oo_receiver_push(w, usable ? oo_value_of_type(type) : oo_value_unknown(),
                         !(usable && (type->flags & OO_F_INNER)));
    if (receiver) {
        pushed++;
        receiver->unknown = !usable;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (kt_is(child, "delegation_specifier")) {
            kw_type_mentions(k, kt_child(child, "user_type"), 0);
            TSNode constructor = kt_child(child, "constructor_invocation");
            if (!ts_node_is_null(constructor)) {
                kw_type_mentions(k, kt_child(constructor, "user_type"), 0);
                kw_children(k, kt_child(constructor, "value_arguments"));
            }
        } else if (kt_is(child, "primary_constructor")) {
            /* Constructor parameters are in scope for member initializers. */
            kw_bind_parameters(k, child);
        }
    }
    TSNode body = kt_child(node, "class_body");
    if (ts_node_is_null(body))
        body = kt_child(node, "enum_class_body");
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    /* Parameters of a primary constructor without val/var. */
    TSNode primary = kt_child(node, "primary_constructor");
    uint32_t parameters = ts_node_is_null(primary) ? 0 : ts_node_named_child_count(primary);
    for (uint32_t i = 0; i < parameters; i++) {
        TSNode parameter = ts_node_named_child(primary, i);
        if (kt_is(parameter, "class_parameter"))
            oo_bind(w, kt_name(&k->text, kt_child(parameter, "simple_identifier")),
                    kw_ref(k, kt_typeref(&k->text, kt_after_colon(parameter, "=", NULL))));
    }
    kw_members(k, body, usable ? type : NULL);
    oo_frame_pop(w);
    for (int i = 0; i < pushed; i++)
        oo_receiver_pop(w);
    w->scope = saved;
}

/* Top level: declarations, and anything the grammar could only wrap in ERROR. */
static void kw_top_level(kw_t *k, TSNode parent, bool in_error) {
    uint32_t count = ts_node_named_child_count(parent);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(parent, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "ERROR")) {
            /* Members of a class this parse lost: their receiver is unknown. */
            oo_receiver_t *receiver = oo_receiver_push(k->w, oo_value_unknown(), false);
            if (receiver)
                receiver->unknown = true;
            kw_top_level(k, child, true);
            if (receiver)
                oo_receiver_pop(k->w);
        } else if (!strcmp(kind, "class_declaration") || !strcmp(kind, "object_declaration")) {
            kw_class_like(k, child, NULL);
        } else if (!strcmp(kind, "function_declaration")) {
            kw_annotations(k, child);
            kw_function(k, child);
        } else if (!strcmp(kind, "property_declaration") ||
                   !strcmp(kind, "anonymous_initializer") ||
                   !strcmp(kind, "secondary_constructor") || !strcmp(kind, "companion_object")) {
            /* kw_members is the one walker of a property's parts. */
            if (in_error) {
                oo_frame_t frame;
                oo_frame_push(k->w, &frame);
                kw_children(k, child);
                oo_frame_pop(k->w);
            } else {
                oo_frame_t frame;
                oo_frame_push(k->w, &frame);
                TSNode variable = kt_child(child, "variable_declaration");
                kw_type_mentions(k, kt_after_colon(variable, "=", NULL), 0);
                uint32_t parts = ts_node_named_child_count(child);
                for (uint32_t p = 0; p < parts; p++) {
                    TSNode part = ts_node_named_child(child, p);
                    if (kt_is(part, "variable_declaration") || kt_is(part, "modifiers") ||
                        kt_is(part, "binding_pattern_kind") ||
                        kt_is(part, "multi_variable_declaration") ||
                        kt_is(part, "type_parameters") || kt_is(part, "receiver_type") ||
                        kt_is(part, "type_constraints"))
                        continue;
                    if (kt_is(part, "getter") || kt_is(part, "setter")) {
                        TSNode accessor_body = kt_child(part, "function_body");
                        if (!ts_node_is_null(accessor_body))
                            (void)kw_expr(k, accessor_body);
                        continue;
                    }
                    if (kt_is(part, "property_delegate")) {
                        kw_children(k, part);
                        continue;
                    }
                    (void)kw_expr(k, part);
                }
                oo_frame_pop(k->w);
            }
        }
    }
}

void oo_kotlin_resolve(oo_walk_t *w, TSNode root) {
    kw_t k = {.w = w, .text = {.arena = w->arena, .source = w->source, .length = w->length}};
    const char *package = "";
    oo_import_t *imports = NULL;
    int import_count = 0;
    kt_read_imports(&k.text, root, &imports, &import_count, &package);
    oo_file_t *file = cbm_arena_alloc(w->arena, sizeof(*file));
    if (!file)
        return;
    *file = (oo_file_t){.rel = w->rel_path,
                        .package = package,
                        .language = CBM_LANG_KOTLIN,
                        .imports = imports,
                        .import_count = import_count,
                        .has_error = ts_node_has_error(root)};
    oo_file_chain_imports(w->arena, file);
    w->f = file;
    w->scope = (oo_scope_t){.file = file, .ns = package};
    oo_frame_t frame;
    oo_frame_push(w, &frame);
    kw_top_level(&k, root, false);
    oo_frame_pop(w);
    w->f = NULL;
}
