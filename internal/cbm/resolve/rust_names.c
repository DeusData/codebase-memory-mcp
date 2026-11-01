/* Rust crate graph, module tree and path resolution. See rust_names.h.
 *
 * The rules here are Rust's own:
 * - A module's own items and named imports cannot collide in one namespace, so
 *   finding one settles the lookup whatever macros or globs the module also has.
 * - Globs only fill names the module does not otherwise bind. A program that
 *   compiles cannot use a name two globs bind to different items, so one project
 *   provider is the answer. A module-level macro can still add a shadowing item,
 *   so a name found only through a glob is withheld there.
 * - cfg-gated twins are variants of one declaration; twins naming different
 *   declarations stay ambiguous.
 */
#include "rust_names_internal.h"
#include "../helpers.h"
#include "../lang_specs.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum { RS_MAX_MODULE_DEPTH = 64, RS_MAX_SEGMENTS = 64 };

bool rs_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && strcmp(ts_node_type(node), kind) == 0;
}

TSNode rs_field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

const char *rs_text(rs_builder_t *b, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= b->length
               ? cbm_arena_strndup(b->arena, b->source + start, end - start)
               : NULL;
}

/* ── Type syntax ───────────────────────────────────────────────────── */

static rs_tsyn_t *rs_tsyn_new(rs_builder_t *b, rs_tsyn_kind_t kind) {
    rs_tsyn_t *syn = cbm_arena_calloc(b->arena, sizeof(*syn));
    if (syn)
        syn->kind = kind;
    return syn;
}

static void rs_path_push(rs_builder_t *b, rs_tsyn_t *syn, const char *seg) {
    const char **segs = cbm_arena_alloc(b->arena, (size_t)(syn->nseg + 1) * sizeof(*segs));
    if (!segs)
        return;
    if (syn->nseg)
        memcpy(segs, syn->segs, (size_t)syn->nseg * sizeof(*segs));
    segs[syn->nseg++] = seg;
    syn->segs = segs;
}

/* Flattens identifier | scoped_identifier | crate | self | super | generic path
 * prefixes into segments. Returns false for a path this model cannot spell. */
static bool rs_path_flatten(rs_builder_t *b, TSNode node, rs_tsyn_t *out, unsigned depth) {
    if (ts_node_is_null(node) || depth > RS_MAX_SEGMENTS)
        return false;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "scoped_identifier") || !strcmp(kind, "scoped_type_identifier")) {
        TSNode path = rs_field(node, "path");
        if (ts_node_is_null(path))
            out->absolute = true;
        else if (!rs_path_flatten(b, path, out, depth + 1))
            return false;
        const char *name = rs_text(b, rs_field(node, "name"));
        if (!name)
            return false;
        rs_path_push(b, out, name);
        return true;
    }
    if (!strcmp(kind, "generic_type") || !strcmp(kind, "generic_type_with_turbofish"))
        return rs_path_flatten(b, rs_field(node, "type"), out, depth + 1);
    if (!strcmp(kind, "identifier") || !strcmp(kind, "type_identifier") || !strcmp(kind, "crate") ||
        !strcmp(kind, "self") || !strcmp(kind, "super") || !strcmp(kind, "primitive_type")) {
        const char *name = rs_text(b, node);
        if (!name)
            return false;
        rs_path_push(b, out, name);
        return true;
    }
    return false;
}

static void rs_args_from(rs_builder_t *b, rs_tsyn_t *syn, TSNode list) {
    uint32_t count = ts_node_named_child_count(list);
    if (!count)
        return;
    rs_tsyn_t **args = cbm_arena_calloc(b->arena, count * sizeof(*args));
    if (!args)
        return;
    int n = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode arg = ts_node_named_child(list, i);
        const char *kind = ts_node_type(arg);
        if (!strcmp(kind, "type_binding")) {
            const char *name = rs_text(b, rs_field(arg, "name"));
            if (name && !strcmp(name, "Item"))
                syn->item_binding = rs_tsyn_from(b, rs_field(arg, "type"));
            continue;
        }
        /* Lifetimes and const arguments occupy no type-parameter position. */
        if (!strcmp(kind, "lifetime") || !strcmp(kind, "integer_literal") ||
            !strcmp(kind, "block") || !strcmp(kind, "line_comment") ||
            !strcmp(kind, "block_comment"))
            continue;
        args[n++] = rs_tsyn_from(b, arg);
    }
    syn->args = args;
    syn->nargs = n;
}

static void rs_bounds_from(rs_builder_t *b, TSNode node, rs_tsyn_t **out, int *count, int cap) {
    if (ts_node_is_null(node) || *count >= cap)
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "bounded_type") || !strcmp(kind, "trait_bounds")) {
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++)
            rs_bounds_from(b, ts_node_named_child(node, i), out, count, cap);
        return;
    }
    if (!strcmp(kind, "higher_ranked_trait_bound")) {
        rs_bounds_from(b, rs_field(node, "type"), out, count, cap);
        return;
    }
    /* `dyn A + 'a` parses as a bound list around a trait object. */
    if (!strcmp(kind, "dynamic_type") || !strcmp(kind, "abstract_type")) {
        rs_bounds_from(b, rs_field(node, "trait"), out, count, cap);
        return;
    }
    if (!strcmp(kind, "lifetime") || !strcmp(kind, "removed_trait_bound") ||
        !strcmp(kind, "use_bounds"))
        return;
    out[(*count)++] = rs_tsyn_from(b, node);
}

rs_tsyn_t *rs_tsyn_from(rs_builder_t *b, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    const char *kind = ts_node_type(node);
    rs_tsyn_t *syn = NULL;
    if (!strcmp(kind, "primitive_type")) {
        syn = rs_tsyn_new(b, RS_TS_PRIM);
        if (syn)
            syn->prim = rs_text(b, node);
    } else if (!strcmp(kind, "type_identifier") || !strcmp(kind, "scoped_type_identifier") ||
               !strcmp(kind, "identifier") || !strcmp(kind, "scoped_identifier")) {
        syn = rs_tsyn_new(b, RS_TS_PATH);
        if (syn && !rs_path_flatten(b, node, syn, 0))
            syn->kind = RS_TS_UNKNOWN;
    } else if (!strcmp(kind, "generic_type") || !strcmp(kind, "generic_type_with_turbofish")) {
        syn = rs_tsyn_new(b, RS_TS_PATH);
        if (syn && !rs_path_flatten(b, node, syn, 0))
            syn->kind = RS_TS_UNKNOWN;
        else if (syn) {
            TSNode args = rs_field(node, "type_arguments");
            if (!ts_node_is_null(args))
                rs_args_from(b, syn, args);
        }
    } else if (!strcmp(kind, "reference_type")) {
        syn = rs_tsyn_new(b, RS_TS_REF);
        if (syn)
            syn->inner = rs_tsyn_from(b, rs_field(node, "type"));
    } else if (!strcmp(kind, "unit_type")) {
        syn = rs_tsyn_new(b, RS_TS_UNIT);
    } else if (!strcmp(kind, "never_type")) {
        syn = rs_tsyn_new(b, RS_TS_NEVER);
    } else if (!strcmp(kind, "tuple_type")) {
        syn = rs_tsyn_new(b, RS_TS_TUPLE);
        if (syn) {
            uint32_t count = ts_node_named_child_count(node);
            syn->args = count ? cbm_arena_calloc(b->arena, count * sizeof(*syn->args)) : NULL;
            for (uint32_t i = 0; syn->args && i < count; i++)
                syn->args[syn->nargs++] = rs_tsyn_from(b, ts_node_named_child(node, i));
        }
    } else if (!strcmp(kind, "array_type")) {
        syn = rs_tsyn_new(b, RS_TS_ARRAY);
        if (syn)
            syn->inner = rs_tsyn_from(b, rs_field(node, "element"));
    } else if (!strcmp(kind, "dynamic_type") || !strcmp(kind, "abstract_type")) {
        syn = rs_tsyn_new(b, RS_TS_DYN);
        if (syn) {
            enum { CAP = 16 };
            syn->args = cbm_arena_calloc(b->arena, CAP * sizeof(*syn->args));
            if (syn->args)
                rs_bounds_from(b, rs_field(node, "trait"), syn->args, &syn->nargs, CAP);
        }
    } else if (!strcmp(kind, "function_type")) {
        syn = rs_tsyn_new(b, RS_TS_FN);
        TSNode params = rs_field(node, "parameters");
        uint32_t count = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
        if (syn && count) {
            syn->args = cbm_arena_calloc(b->arena, count * sizeof(*syn->args));
            for (uint32_t i = 0; syn->args && i < count; i++) {
                TSNode param = ts_node_named_child(params, i);
                if (rs_is(param, "line_comment") || rs_is(param, "block_comment"))
                    continue;
                syn->args[syn->nargs++] =
                    rs_tsyn_from(b, rs_is(param, "parameter") ? rs_field(param, "type") : param);
            }
        }
        if (syn) {
            TSNode ret = rs_field(node, "return_type");
            syn->inner = ts_node_is_null(ret) ? NULL : rs_tsyn_from(b, ret);
        }
    } else if (!strcmp(kind, "bounded_type") && ts_node_named_child_count(node) > 0) {
        /* `(dyn A + 'a)` and `impl A + B` spelled through a bound list. */
        syn = rs_tsyn_new(b, RS_TS_DYN);
        if (syn) {
            enum { CAP = 16 };
            syn->args = cbm_arena_calloc(b->arena, CAP * sizeof(*syn->args));
            if (syn->args)
                rs_bounds_from(b, node, syn->args, &syn->nargs, CAP);
        }
    } else {
        syn = rs_tsyn_new(b, RS_TS_UNKNOWN);
    }
    if (syn)
        syn->text = rs_text(b, node);
    return syn;
}

/* Generic parameters plus where-clause bounds on a parameter name. */
rs_generics_t *rs_generics_from(rs_builder_t *b, TSNode owner) {
    TSNode params = rs_field(owner, "type_parameters");
    TSNode where = (TSNode){0};
    for (uint32_t i = 0; i < ts_node_named_child_count(owner); i++) {
        TSNode child = ts_node_named_child(owner, i);
        if (rs_is(child, "where_clause"))
            where = child;
    }
    if (ts_node_is_null(params) && ts_node_is_null(where))
        return NULL;
    uint32_t cap = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    if (!ts_node_is_null(where))
        cap += ts_node_named_child_count(where);
    if (!cap)
        return NULL;
    rs_generics_t *g = cbm_arena_calloc(b->arena, sizeof(*g));
    if (!g)
        return NULL;
    g->names = cbm_arena_calloc(b->arena, cap * sizeof(*g->names));
    g->bounds = cbm_arena_calloc(b->arena, cap * sizeof(*g->bounds));
    g->nbounds = cbm_arena_calloc(b->arena, cap * sizeof(*g->nbounds));
    if (!g->names || !g->bounds || !g->nbounds)
        return NULL;
    enum { CAP = 16 };
    for (uint32_t i = 0; !ts_node_is_null(params) && i < ts_node_named_child_count(params); i++) {
        TSNode param = ts_node_named_child(params, i);
        if (!rs_is(param, "type_parameter") && !rs_is(param, "const_parameter"))
            continue;
        const char *name = rs_text(b, rs_field(param, "name"));
        if (!name)
            continue;
        g->names[g->count] = name;
        g->bounds[g->count] = cbm_arena_calloc(b->arena, CAP * sizeof(rs_tsyn_t *));
        if (g->bounds[g->count])
            rs_bounds_from(b, rs_field(param, "bounds"), g->bounds[g->count], &g->nbounds[g->count],
                           CAP);
        g->count++;
    }
    for (uint32_t i = 0; !ts_node_is_null(where) && i < ts_node_named_child_count(where); i++) {
        TSNode pred = ts_node_named_child(where, i);
        if (!rs_is(pred, "where_predicate"))
            continue;
        const char *left = rs_text(b, rs_field(pred, "left"));
        for (int g_i = 0; left && g_i < g->count; g_i++) {
            if (strcmp(g->names[g_i], left) == 0 && g->bounds[g_i])
                rs_bounds_from(b, rs_field(pred, "bounds"), g->bounds[g_i], &g->nbounds[g_i], CAP);
        }
    }
    return g->count ? g : NULL;
}

/* ── Definitions: graph identity by line and name ──────────────────── */

static int rs_def_compare(const void *left, const void *right) {
    const rs_def_t *a = left, *b = right;
    if (a->line != b->line)
        return a->line < b->line ? -1 : 1;
    return strcmp(a->name, b->name);
}

const rs_def_t *rs_def_find(const rs_file_t *file, uint32_t line, const char *name,
                            const char *const *labels) {
    if (!file || !file->ndefs || !name)
        return NULL;
    rs_def_t key = {.line = line, .name = name};
    const rs_def_t *hit =
        bsearch(&key, file->defs, (size_t)file->ndefs, sizeof(*file->defs), rs_def_compare);
    if (!hit)
        return NULL;
    while (hit > file->defs && rs_def_compare(hit - 1, &key) == 0)
        hit--;
    for (; hit < file->defs + file->ndefs && rs_def_compare(hit, &key) == 0; hit++) {
        for (const char *const *label = labels; label && *label; label++)
            if (hit->label && strcmp(hit->label, *label) == 0)
                return hit;
    }
    return NULL;
}

static const char *const RS_LABELS_FN[] = {"Function", "Method", NULL};
static const char *const RS_LABELS_STRUCT[] = {"Struct", "Class", "Union", NULL};
static const char *const RS_LABELS_ENUM[] = {"Enum", "Class", NULL};
static const char *const RS_LABELS_TRAIT[] = {"Interface", "Trait", "Class", NULL};
static const char *const RS_LABELS_TYPE[] = {"Type", "TypeAlias", "Class", NULL};
static const char *const RS_LABELS_VAR[] = {"Variable", "Constant", NULL};
static const char *const RS_LABELS_FIELD[] = {"Field", NULL};

/* ── Items ─────────────────────────────────────────────────────────── */

static uint32_t rs_line(TSNode node) {
    return ts_node_start_point(node).row + 1;
}

static rs_vis_t rs_vis_of(rs_builder_t *b, TSNode node) {
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!rs_is(child, "visibility_modifier"))
            continue;
        const char *text = rs_text(b, child);
        if (!text)
            return RS_VIS_PRIVATE;
        if (!strcmp(text, "pub"))
            return RS_VIS_PUB;
        if (strstr(text, "self"))
            return RS_VIS_PRIVATE;
        if (strstr(text, "super"))
            return RS_VIS_SUPER;
        return RS_VIS_CRATE; /* pub(crate), crate, pub(in path) within the crate */
    }
    return RS_VIS_PRIVATE;
}

static void rs_attach_def(rs_builder_t *b, TSNode node, const char *name, const char *const *labels,
                          const char **qn, const char **label, bool *conflict) {
    const rs_def_t *def = rs_def_find(b->file, rs_line(node), name, labels);
    *qn = def ? def->qn : NULL;
    *label = def ? def->label : NULL;
    /* A block-scoped item sharing a file-level QN is not representable. */
    if (conflict)
        *conflict = b->block_scope && def && def->dup_qn;
}

static rs_item_t *rs_item_add(rs_builder_t *b, rs_mod_t *mod, const char *name, unsigned ns,
                              rs_item_kind_t kind, rs_vis_t vis) {
    rs_item_t *item = name ? cbm_arena_calloc(b->arena, sizeof(*item)) : NULL;
    if (!item)
        return NULL;
    item->name = name;
    item->ns = ns;
    item->kind = kind;
    item->vis = vis;
    item->owner = mod;
    item->next = mod->item_list;
    mod->item_list = item;
    mod->nitems++;
    return item;
}

static bool rs_self_param_of(rs_builder_t *b, TSNode param, rs_self_kind_t *kind) {
    if (rs_is(param, "self_parameter")) {
        const char *text = rs_text(b, param);
        bool reference = text && strchr(text, '&');
        bool mutable_self = text && strstr(text, "mut ");
        *kind = reference ? (mutable_self ? RS_SELF_MUT : RS_SELF_REF) : RS_SELF_VALUE;
        return true;
    }
    if (rs_is(param, "parameter") && rs_is(rs_field(param, "pattern"), "self")) {
        *kind = RS_SELF_VALUE;
        return true;
    }
    return false;
}

static rs_fn_t *rs_fn_from(rs_builder_t *b, rs_mod_t *mod, TSNode node) {
    rs_fn_t *fn = cbm_arena_calloc(b->arena, sizeof(*fn));
    const char *name = fn ? rs_text(b, rs_field(node, "name")) : NULL;
    if (!name)
        return NULL;
    fn->name = name;
    fn->owner = mod;
    fn->vis = rs_vis_of(b, node);
    fn->generics = rs_generics_from(b, node);
    rs_attach_def(b, node, name, RS_LABELS_FN, &fn->qn, &fn->label, &fn->qn_conflict);
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        if (rs_is(child, "function_modifiers")) {
            const char *text = rs_text(b, child);
            fn->is_async = text && strstr(text, "async");
        }
    }
    TSNode params = rs_field(node, "parameters");
    uint32_t count = ts_node_is_null(params) ? 0 : ts_node_named_child_count(params);
    fn->params = count ? cbm_arena_calloc(b->arena, count * sizeof(*fn->params)) : NULL;
    for (uint32_t i = 0; i < count; i++) {
        TSNode param = ts_node_named_child(params, i);
        rs_self_kind_t self_kind;
        if (rs_self_param_of(b, param, &self_kind)) {
            fn->self_kind = self_kind;
            continue;
        }
        if (rs_is(param, "parameter") && fn->params)
            fn->params[fn->nparams++] = rs_tsyn_from(b, rs_field(param, "type"));
    }
    TSNode ret = rs_field(node, "return_type");
    fn->ret = ts_node_is_null(ret) ? NULL : rs_tsyn_from(b, ret);
    return fn;
}

static void rs_fields_from(rs_builder_t *b, TSNode body, rs_field_t **out, bool *tuple) {
    if (ts_node_is_null(body))
        return;
    rs_field_t *head = NULL, **tail = &head;
    if (rs_is(body, "ordered_field_declaration_list")) {
        *tuple = true;
        int position = 0;
        TSTreeCursor cursor = ts_tree_cursor_new(body);
        bool more = ts_tree_cursor_goto_first_child(&cursor);
        for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (!field || strcmp(field, "type") != 0)
                continue;
            rs_field_t *entry = cbm_arena_calloc(b->arena, sizeof(*entry));
            if (!entry)
                break;
            entry->name = cbm_arena_sprintf(b->arena, "%d", position++);
            entry->type = rs_tsyn_from(b, ts_tree_cursor_current_node(&cursor));
            *tail = entry;
            tail = &entry->next;
        }
        ts_tree_cursor_delete(&cursor);
    } else {
        rs_children_t children = rs_children(body);
        TSNode child;
        while (rs_children_next(&children, &child)) {
            if (!rs_is(child, "field_declaration"))
                continue;
            rs_field_t *entry = cbm_arena_calloc(b->arena, sizeof(*entry));
            const char *name = entry ? rs_text(b, rs_field(child, "name")) : NULL;
            if (!name) {
                rs_children_close(&children);
                break;
            }
            entry->name = name;
            entry->vis = rs_vis_of(b, child);
            entry->type = rs_tsyn_from(b, rs_field(child, "type"));
            rs_attach_def(b, child, name, RS_LABELS_FIELD, &entry->qn, &entry->label, NULL);
            *tail = entry;
            tail = &entry->next;
        }
    }
    *out = head;
}

static void rs_collect_enum(rs_builder_t *b, rs_item_t *item, TSNode body) {
    rs_variant_t **tail = &item->variants;
    rs_children_t children = rs_children(body);
    TSNode child;
    while (rs_children_next(&children, &child)) {
        if (!rs_is(child, "enum_variant"))
            continue;
        rs_variant_t *variant = cbm_arena_calloc(b->arena, sizeof(*variant));
        const char *name = variant ? rs_text(b, rs_field(child, "name")) : NULL;
        if (!name) {
            rs_children_close(&children);
            return;
        }
        variant->name = name;
        rs_fields_from(b, rs_field(child, "body"), &variant->fields, &variant->tuple);
        *tail = variant;
        tail = &variant->next;
    }
}

static void rs_collect_trait(rs_builder_t *b, rs_mod_t *mod, rs_item_t *item, TSNode node) {
    enum { CAP = 16 };
    item->supers = cbm_arena_calloc(b->arena, CAP * sizeof(*item->supers));
    if (item->supers)
        rs_bounds_from(b, rs_field(node, "bounds"), item->supers, &item->nsupers, CAP);
    rs_children_t children = rs_children(rs_field(node, "body"));
    TSNode child;
    while (rs_children_next(&children, &child)) {
        if (!rs_is(child, "function_item") && !rs_is(child, "function_signature_item"))
            continue;
        rs_fn_t *fn = rs_fn_from(b, mod, child);
        if (!fn)
            continue;
        fn->trait = item;
        fn->vis = RS_VIS_PUB; /* a trait's members share the trait's visibility */
        fn->next = item->methods;
        item->methods = fn;
    }
}

static void rs_collect_impl(rs_builder_t *b, rs_mod_t *mod, TSNode node) {
    rs_impl_t *impl = cbm_arena_calloc(b->arena, sizeof(*impl));
    if (!impl)
        return;
    impl->owner = mod;
    impl->start = ts_node_start_byte(node);
    impl->generics = rs_generics_from(b, node);
    impl->self_syn = rs_tsyn_from(b, rs_field(node, "type"));
    TSNode trait = rs_field(node, "trait");
    impl->trait_syn = ts_node_is_null(trait) ? NULL : rs_tsyn_from(b, trait);
    TSNode body = rs_field(node, "body");
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    impl->assoc_names =
        count ? cbm_arena_calloc(b->arena, count * sizeof(*impl->assoc_names)) : NULL;
    impl->assoc_types =
        count ? cbm_arena_calloc(b->arena, count * sizeof(*impl->assoc_types)) : NULL;
    rs_children_t children = rs_children(body);
    TSNode child;
    while (rs_children_next(&children, &child)) {
        if (rs_is(child, "function_item")) {
            rs_fn_t *fn = rs_fn_from(b, mod, child);
            if (fn) {
                fn->impl = impl;
                fn->next = impl->methods;
                impl->methods = fn;
            }
        } else if (rs_is(child, "type_item") && impl->assoc_names) {
            const char *name = rs_text(b, rs_field(child, "name"));
            if (name) {
                impl->assoc_names[impl->nassoc] = name;
                impl->assoc_types[impl->nassoc++] = rs_tsyn_from(b, rs_field(child, "type"));
            }
        } else if (rs_is(child, "macro_invocation")) {
            impl->has_macro = true;
        }
    }
    impl->next = mod->impls;
    mod->impls = impl;
}

static void rs_use_add(rs_builder_t *b, rs_mod_t *mod, const char *const *segs, int nseg,
                       bool absolute, const char *local, bool glob, rs_vis_t vis, bool has_cfg,
                       TSNode site) {
    rs_use_t *use = cbm_arena_calloc(b->arena, sizeof(*use));
    const char **copy = cbm_arena_alloc(b->arena, (size_t)(nseg ? nseg : 1) * sizeof(*copy));
    if (!use || !copy)
        return;
    if (nseg)
        memcpy(copy, segs, (size_t)nseg * sizeof(*copy));
    use->segs = copy;
    use->nseg = nseg;
    use->absolute = absolute;
    use->local = local;
    use->glob = glob;
    use->vis = vis;
    use->has_cfg = has_cfg;
    use->start = ts_node_start_byte(site);
    use->end = ts_node_end_byte(site);
    if (glob) {
        use->next = mod->globs;
        mod->globs = use;
    } else {
        use->next = mod->uses;
        mod->uses = use;
    }
}

/* One binding per leaf of a use tree: groups, renames, `self`, nesting, globs. */
static void rs_use_tree(rs_builder_t *b, rs_mod_t *mod, TSNode node, const char **prefix,
                        int nprefix, bool absolute, rs_vis_t vis, bool has_cfg, unsigned depth) {
    if (ts_node_is_null(node) || depth > RS_MAX_SEGMENTS)
        return;
    const char *kind = ts_node_type(node);
    const char *segs[RS_MAX_SEGMENTS];
    if (nprefix > 0)
        memcpy(segs, prefix, (size_t)nprefix * sizeof(*segs));
    rs_tsyn_t path = {0};
    if (!strcmp(kind, "use_list")) {
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++)
            rs_use_tree(b, mod, ts_node_named_child(node, i), segs, nprefix, absolute, vis, has_cfg,
                        depth + 1);
        return;
    }
    if (!strcmp(kind, "scoped_use_list")) {
        TSNode head = rs_field(node, "path");
        if (ts_node_is_null(head))
            absolute = absolute || nprefix == 0;
        else if (!rs_path_flatten(b, head, &path, 0))
            return;
        if (nprefix + path.nseg >= RS_MAX_SEGMENTS)
            return;
        for (int i = 0; i < path.nseg; i++)
            segs[nprefix + i] = path.segs[i];
        rs_use_tree(b, mod, rs_field(node, "list"), segs, nprefix + path.nseg,
                    absolute || path.absolute, vis, has_cfg, depth + 1);
        return;
    }
    if (!strcmp(kind, "use_wildcard")) {
        TSNode head = ts_node_named_child_count(node) ? ts_node_named_child(node, 0) : (TSNode){0};
        if (!ts_node_is_null(head) && !rs_path_flatten(b, head, &path, 0))
            return;
        if (nprefix + path.nseg >= RS_MAX_SEGMENTS)
            return;
        for (int i = 0; i < path.nseg; i++)
            segs[nprefix + i] = path.segs[i];
        rs_use_add(b, mod, segs, nprefix + path.nseg, absolute || path.absolute, NULL, true, vis,
                   has_cfg, node);
        return;
    }
    const char *alias = NULL;
    TSNode target = node;
    if (!strcmp(kind, "use_as_clause")) {
        target = rs_field(node, "path");
        alias = rs_text(b, rs_field(node, "alias"));
    }
    /* `a::{self}` and `a::{self as b}` bind module a itself. */
    bool self_leaf = rs_is(target, "self") && nprefix > 0;
    if (!self_leaf && !rs_path_flatten(b, target, &path, 0))
        return;
    int total = nprefix + (self_leaf ? 0 : path.nseg);
    if (total >= RS_MAX_SEGMENTS || total <= 0)
        return;
    for (int i = 0; !self_leaf && i < path.nseg; i++)
        segs[nprefix + i] = path.segs[i];
    const char *local = alias ? alias : segs[total - 1];
    rs_use_add(b, mod, segs, total, absolute || path.absolute, local, false, vis, has_cfg, node);
}

static const char *rs_string_literal(rs_builder_t *b, const char *text) {
    const char *open = text ? strchr(text, '"') : NULL;
    const char *close = open ? strchr(open + 1, '"') : NULL;
    return close ? cbm_arena_strndup(b->arena, open + 1, (size_t)(close - open - 1)) : NULL;
}

static void rs_collect_mod(rs_builder_t *b, rs_mod_t *mod, TSNode node, const char *path_attr,
                           bool has_cfg) {
    const char *name = rs_text(b, rs_field(node, "name"));
    rs_item_t *item = rs_item_add(b, mod, name, RS_NS_TYPE, RS_ITEM_MOD, rs_vis_of(b, node));
    if (!item)
        return;
    item->path_attr = path_attr;
    item->has_cfg = has_cfg;
    TSNode body = rs_field(node, "body");
    if (ts_node_is_null(body))
        return;
    rs_mod_t *child = cbm_arena_calloc(b->arena, sizeof(*child));
    if (!child)
        return;
    child->name = name;
    child->parent = mod;
    child->file = b->file;
    child->is_inline = true;
    child->start = ts_node_start_byte(node);
    child->end = ts_node_end_byte(node);
    child->next_child = mod->children;
    mod->children = child;
    item->inline_mod = child;
    rs_collect_items(b, child, body);
}

static void rs_collect_struct(rs_builder_t *b, rs_item_t *item, TSNode node) {
    item->generics = rs_generics_from(b, node);
    TSNode body = rs_field(node, "body");
    rs_fields_from(b, body, &item->fields, &item->tuple_struct);
    item->unit_struct = ts_node_is_null(body);
    if (item->tuple_struct || item->unit_struct)
        item->ns |= RS_NS_VALUE;
}

static void rs_collect_one(rs_builder_t *b, rs_mod_t *mod, TSNode node, const char *path_attr,
                           bool macro_export, bool has_cfg, bool derives_conversion) {
    const char *kind = ts_node_type(node);
    rs_vis_t vis = rs_vis_of(b, node);
    const char *name = rs_text(b, rs_field(node, "name"));
    rs_item_t *item = NULL;
    if (!strcmp(kind, "function_item") || !strcmp(kind, "function_signature_item")) {
        item = rs_item_add(b, mod, name, RS_NS_VALUE, RS_ITEM_FN, vis);
        if (item) {
            item->fn = rs_fn_from(b, mod, node);
            if (item->fn) {
                item->qn = item->fn->qn;
                item->label = item->fn->label;
                item->qn_conflict = item->fn->qn_conflict;
            }
        }
    } else if (!strcmp(kind, "struct_item") || !strcmp(kind, "union_item")) {
        item = rs_item_add(b, mod, name, RS_NS_TYPE,
                           !strcmp(kind, "union_item") ? RS_ITEM_UNION : RS_ITEM_STRUCT, vis);
        if (item) {
            rs_attach_def(b, node, name, RS_LABELS_STRUCT, &item->qn, &item->label,
                          &item->qn_conflict);
            rs_collect_struct(b, item, node);
        }
    } else if (!strcmp(kind, "enum_item")) {
        item = rs_item_add(b, mod, name, RS_NS_TYPE, RS_ITEM_ENUM, vis);
        if (item) {
            rs_attach_def(b, node, name, RS_LABELS_ENUM, &item->qn, &item->label,
                          &item->qn_conflict);
            item->generics = rs_generics_from(b, node);
            rs_collect_enum(b, item, rs_field(node, "body"));
        }
    } else if (!strcmp(kind, "trait_item")) {
        item = rs_item_add(b, mod, name, RS_NS_TYPE, RS_ITEM_TRAIT, vis);
        if (item) {
            rs_attach_def(b, node, name, RS_LABELS_TRAIT, &item->qn, &item->label,
                          &item->qn_conflict);
            item->generics = rs_generics_from(b, node);
            rs_collect_trait(b, mod, item, node);
        }
    } else if (!strcmp(kind, "type_item")) {
        item = rs_item_add(b, mod, name, RS_NS_TYPE, RS_ITEM_TYPE, vis);
        if (item) {
            rs_attach_def(b, node, name, RS_LABELS_TYPE, &item->qn, &item->label,
                          &item->qn_conflict);
            item->generics = rs_generics_from(b, node);
            item->type = rs_tsyn_from(b, rs_field(node, "type"));
        }
    } else if (!strcmp(kind, "const_item") || !strcmp(kind, "static_item")) {
        item = rs_item_add(b, mod, name, RS_NS_VALUE,
                           !strcmp(kind, "const_item") ? RS_ITEM_CONST : RS_ITEM_STATIC, vis);
        if (item) {
            rs_attach_def(b, node, name, RS_LABELS_VAR, &item->qn, &item->label,
                          &item->qn_conflict);
            item->type = rs_tsyn_from(b, rs_field(node, "type"));
        }
    } else if (!strcmp(kind, "macro_definition")) {
        item = rs_item_add(b, mod, name, RS_NS_MACRO, RS_ITEM_MACRO,
                           macro_export ? RS_VIS_PUB : RS_VIS_CRATE);
        if (item && macro_export)
            item->unit_struct = true; /* exported at the crate root; see rs_inst_build */
    } else if (!strcmp(kind, "mod_item")) {
        rs_collect_mod(b, mod, node, path_attr, has_cfg);
        return;
    } else if (!strcmp(kind, "impl_item")) {
        rs_collect_impl(b, mod, node);
        return;
    } else if (!strcmp(kind, "use_declaration")) {
        rs_use_tree(b, mod, rs_field(node, "argument"), NULL, 0, false, vis, has_cfg, 0);
        return;
    } else if (!strcmp(kind, "extern_crate_declaration")) {
        const char *crate_name = name;
        const char *alias = rs_text(b, rs_field(node, "alias"));
        if (crate_name) {
            const char *segs[1] = {crate_name};
            rs_use_add(b, mod, segs, 1, true, alias ? alias : crate_name, false, vis, has_cfg,
                       node);
            if (mod->uses)
                mod->uses->extern_crate = true;
        }
        return;
    } else if (!strcmp(kind, "foreign_mod_item")) {
        rs_collect_items(b, mod, rs_field(node, "body"));
        return;
    } else if (!strcmp(kind, "macro_invocation")) {
        mod->has_macro_items = true;
        return;
    } else if (!strcmp(kind, "expression_statement")) {
        if (ts_node_named_child_count(node) &&
            rs_is(ts_node_named_child(node, 0), "macro_invocation"))
            mod->has_macro_items = true;
        return;
    }
    if (item) {
        item->has_cfg = has_cfg;
        item->derives_conversion = derives_conversion;
    }
}

/* derive(Error) with #[from], or derive_more's From, adds impls source does not show. */
static bool rs_derive_adds_conversion(const char *text) {
    const char *derive = text ? strstr(text, "derive") : NULL;
    return derive && (strstr(derive, "Error") || strstr(derive, "From") || strstr(derive, "Into"));
}

void rs_collect_items(rs_builder_t *b, rs_mod_t *mod, TSNode container) {
    if (ts_node_is_null(container))
        return;
    const char *path_attr = NULL;
    bool macro_export = false, has_cfg = false, derives_conversion = false;
    rs_children_t children = rs_children(container);
    TSNode node;
    while (rs_children_next(&children, &node)) {
        if (rs_is(node, "line_comment") || rs_is(node, "block_comment"))
            continue;
        if (rs_is(node, "attribute_item")) {
            const char *text = rs_text(b, node);
            if (text && strstr(text, "macro_export"))
                macro_export = true;
            else if (text && (strstr(text, "cfg(") || strstr(text, "cfg (")))
                has_cfg = true;
            else if (text && !strncmp(text, "#[path", 6))
                path_attr = rs_string_literal(b, text);
            derives_conversion |= rs_derive_adds_conversion(text);
            continue;
        }
        rs_collect_one(b, mod, node, path_attr, macro_export, has_cfg, derives_conversion);
        path_attr = NULL;
        macro_export = false;
        has_cfg = false;
        derives_conversion = false;
    }
}

static int rs_item_compare(const void *left, const void *right) {
    const rs_item_t *a = *(rs_item_t *const *)left, *b = *(rs_item_t *const *)right;
    return strcmp(a->name, b->name);
}

void rs_mod_seal(CBMArena *arena, rs_mod_t *mod) {
    if (mod->nitems) {
        mod->items = cbm_arena_alloc(arena, (size_t)mod->nitems * sizeof(*mod->items));
        int n = 0;
        for (rs_item_t *item = mod->item_list; mod->items && item; item = item->next)
            mod->items[n++] = item;
        if (mod->items)
            qsort(mod->items, (size_t)n, sizeof(*mod->items), rs_item_compare);
        else
            mod->nitems = 0;
    }
    for (rs_mod_t *child = mod->children; child; child = child->next_child)
        rs_mod_seal(arena, child);
}

/* ── Project and files ─────────────────────────────────────────────── */

typedef struct rs_dep {
    const char *key;  /* the extern name with - already folded to _ */
    const char *path; /* relative to the declaring manifest, or NULL */
    bool workspace;   /* resolved through [workspace.dependencies] */
    bool renamed;     /* `package = "real"`: the key, not the lib, is the crate name */
    struct rs_dep *next;
} rs_dep_t;

typedef struct rs_target {
    rs_crate_kind_t kind;
    const char *name;
    const char *path; /* relative to the package directory, or NULL for the default */
    struct rs_target *next;
} rs_target_t;

struct rs_package {
    const char *rel;
    const char *dir;
    const char *name;
    const char *lib_name;
    const char *lib_path;
    const char *build_path;
    bool has_package;
    bool edition_2015;
    bool edition_inherited;
    bool auto_bins, auto_tests, auto_benches, auto_examples;
    const char *workspace_edition;
    rs_dep_t *deps;
    rs_dep_t *workspace_deps;
    rs_target_t *targets;
    rs_crate_t *lib_crate;
    struct rs_package *next;
};

cbm_rust_project_t *cbm_rust_project_new(void) {
    cbm_rust_project_t *project = cbm_alloc(CBM_MEM_CLASS_SEMANTIC, sizeof(*project));
    if (!project)
        return NULL;
    memset(project, 0, sizeof(*project));
    cbm_arena_init(&project->arena);
    return project;
}

void cbm_rust_project_free(cbm_rust_project_t *project) {
    if (!project)
        return;
    cbm_arena_destroy(&project->arena);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, project);
}

static int rs_file_compare(const void *left, const void *right) {
    const rs_file_t *a = *(rs_file_t *const *)left, *b = *(rs_file_t *const *)right;
    return strcmp(a->rel, b->rel);
}

rs_file_t *rs_file_find(const cbm_rust_project_t *project, const char *rel) {
    if (!project || !rel || !project->nfiles)
        return NULL;
    rs_file_t key = {.rel = rel};
    const rs_file_t *needle = &key;
    rs_file_t **hit = bsearch(&needle, project->files, (size_t)project->nfiles,
                              sizeof(*project->files), rs_file_compare);
    return hit ? *hit : NULL;
}

/* A qualified name that two different scopes of one file both claim cannot name
 * either declaration faithfully. cfg twins and repeated impl methods share one
 * scope and stay one identity, as the graph treats them. */
typedef struct {
    const rs_mod_t *owner;
    int claims;
    int fn_claims; /* a field sharing a method's name is no signature twin */
    bool conflict;
} rs_qn_claim_t;

static rs_qn_claim_t *rs_claim(CBMArena *arena, CBMHashTable *claims, const char *qn,
                               const rs_mod_t *owner) {
    if (!qn)
        return NULL;
    rs_qn_claim_t *claim = cbm_ht_get(claims, qn);
    if (!claim) {
        claim = cbm_arena_calloc(arena, sizeof(*claim));
        if (!claim)
            return NULL;
        claim->owner = owner;
        cbm_ht_set(claims, qn, claim);
    }
    claim->claims++;
    claim->conflict |= claim->owner != owner;
    return claim;
}

static void rs_claim_fn(CBMArena *arena, CBMHashTable *claims, const char *qn,
                        const rs_mod_t *owner) {
    rs_qn_claim_t *claim = rs_claim(arena, claims, qn, owner);
    if (claim)
        claim->fn_claims++;
}

static void rs_claim_fns(CBMArena *arena, CBMHashTable *claims, rs_fn_t *fn,
                         const rs_mod_t *owner) {
    for (; fn; fn = fn->next)
        rs_claim_fn(arena, claims, fn->qn, owner);
}

static void rs_claim_mod(CBMArena *arena, CBMHashTable *claims, rs_mod_t *mod) {
    for (rs_item_t *item = mod->item_list; item; item = item->next) {
        if (item->kind == RS_ITEM_FN && item->fn)
            rs_claim_fn(arena, claims, item->fn->qn, mod);
        else
            rs_claim(arena, claims, item->qn, mod);
        for (rs_field_t *field = item->fields; field; field = field->next)
            rs_claim(arena, claims, field->qn, mod);
        rs_claim_fns(arena, claims, item->methods, mod);
    }
    for (rs_impl_t *impl = mod->impls; impl; impl = impl->next)
        rs_claim_fns(arena, claims, impl->methods, mod);
    for (rs_mod_t *child = mod->children; child; child = child->next_child)
        rs_claim_mod(arena, claims, child);
}

static bool rs_claim_lost(CBMHashTable *claims, CBMHashTable *def_counts, const char *qn) {
    if (!qn)
        return false;
    const rs_qn_claim_t *claim = cbm_ht_get(claims, qn);
    const int *defs = cbm_ht_get(def_counts, qn);
    /* More definitions than module-level claims: a block-scoped namesake. */
    return claim && (claim->conflict || (defs && *defs > claim->claims));
}

static bool rs_claim_shared(CBMHashTable *claims, const char *qn) {
    const rs_qn_claim_t *claim = qn ? cbm_ht_get(claims, qn) : NULL;
    return claim && claim->fn_claims > 1;
}

static void rs_mark_fns(CBMHashTable *claims, CBMHashTable *counts, rs_fn_t *fn) {
    for (; fn; fn = fn->next) {
        fn->qn_conflict = rs_claim_lost(claims, counts, fn->qn);
        fn->qn_twin = rs_claim_shared(claims, fn->qn);
    }
}

static void rs_mark_mod(CBMHashTable *claims, CBMHashTable *counts, rs_mod_t *mod) {
    for (rs_item_t *item = mod->item_list; item; item = item->next) {
        if (item->kind == RS_ITEM_FN && item->fn) {
            item->fn->qn_conflict = rs_claim_lost(claims, counts, item->fn->qn);
            item->fn->qn_twin = rs_claim_shared(claims, item->fn->qn);
        }
        item->qn_conflict = rs_claim_lost(claims, counts, item->qn);
        rs_mark_fns(claims, counts, item->methods);
    }
    for (rs_impl_t *impl = mod->impls; impl; impl = impl->next)
        rs_mark_fns(claims, counts, impl->methods);
    for (rs_mod_t *child = mod->children; child; child = child->next_child)
        rs_mark_mod(claims, counts, child);
}

static bool rs_file_defs(cbm_rust_project_t *project, rs_file_t *file, const CBMFileResult *result,
                         CBMHashTable *counts) {
    int count = result ? result->defs.count : 0;
    file->defs =
        count ? cbm_arena_calloc(&project->arena, (size_t)count * sizeof(*file->defs)) : NULL;
    if (count && !file->defs)
        return false;
    for (int i = 0; i < count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        if (!def->name || !def->qualified_name || !def->label || def->start_line == 0)
            continue;
        rs_def_t *entry = &file->defs[file->ndefs++];
        entry->line = (uint32_t)def->start_line;
        entry->name = cbm_arena_strdup(&project->arena, def->name);
        entry->qn = cbm_arena_strdup(&project->arena, def->qualified_name);
        entry->label = cbm_arena_strdup(&project->arena, def->label);
        if (!entry->name || !entry->qn || !entry->label)
            return false;
        int *seen = cbm_ht_get(counts, entry->qn);
        if (!seen) {
            seen = cbm_arena_calloc(&project->arena, sizeof(*seen));
            if (!seen)
                return false;
            cbm_ht_set(counts, entry->qn, seen);
        }
        (*seen)++;
    }
    for (int i = 0; i < file->ndefs; i++) {
        const int *seen = cbm_ht_get(counts, file->defs[i].qn);
        file->defs[i].dup_qn = seen && *seen > 1;
    }
    if (file->ndefs > 1)
        qsort(file->defs, (size_t)file->ndefs, sizeof(*file->defs), rs_def_compare);
    return true;
}

bool cbm_rust_project_add_file(cbm_rust_project_t *project, const char *rel,
                               const CBMFileResult *result, const char *source, size_t length,
                               TSNode root) {
    if (!project || project->sealed || !rel || !source)
        return false;
    rs_file_t *file = cbm_arena_calloc(&project->arena, sizeof(*file));
    if (!file)
        return false;
    file->rel = cbm_arena_strdup(&project->arena, rel);
    if (project->nfiles == project->cap_files) {
        int cap = project->cap_files ? project->cap_files * 2 : 64;
        rs_file_t **files = cbm_arena_alloc(&project->arena, (size_t)cap * sizeof(*files));
        if (!files)
            return false;
        if (project->nfiles)
            memcpy(files, project->files, (size_t)project->nfiles * sizeof(*files));
        project->files = files;
        project->cap_files = cap;
    }
    project->files[project->nfiles++] = file;
    rs_mod_t *top = cbm_arena_calloc(&project->arena, sizeof(*top));
    if (!top || !file->rel)
        return false;
    top->file = file;
    top->end = (uint32_t)length;
    file->root = top;
    CBMHashTable *counts = cbm_ht_create(32);
    CBMHashTable *claims = cbm_ht_create(32);
    bool ok = counts && claims && rs_file_defs(project, file, result, counts);
    if (ok) {
        rs_builder_t builder = {
            .arena = &project->arena, .source = source, .length = length, .file = file};
        rs_collect_items(&builder, top, root);
        rs_claim_mod(&project->arena, claims, top);
        rs_mark_mod(claims, counts, top);
        rs_mod_seal(&project->arena, top);
    }
    cbm_ht_free(counts);
    cbm_ht_free(claims);
    return ok;
}

/* ── Cargo manifests ───────────────────────────────────────────────── */

static const char *rs_folded(CBMArena *arena, const char *name) {
    char *copy = name ? cbm_arena_strdup(arena, name) : NULL;
    for (char *at = copy; at && *at; at++)
        if (*at == '-')
            *at = '_';
    return copy;
}

static const char *rs_dirname(CBMArena *arena, const char *rel) {
    const char *slash = strrchr(rel, '/');
    return slash ? cbm_arena_strndup(arena, rel, (size_t)(slash - rel)) : "";
}

static const char *rs_join(CBMArena *arena, const char *dir, const char *rel) {
    /* Normalizes ./ and ../ so a manifest's `path = "../x"` names a real directory. */
    const char *joined = dir && dir[0] ? cbm_arena_sprintf(arena, "%s/%s", dir, rel) : rel;
    if (!joined)
        return NULL;
    char *out = cbm_arena_alloc(arena, strlen(joined) + 1);
    if (!out)
        return NULL;
    size_t length = 0;
    for (const char *part = joined; part && *part;) {
        const char *end = strchr(part, '/');
        size_t size = end ? (size_t)(end - part) : strlen(part);
        if (size == 2 && part[0] == '.' && part[1] == '.') {
            while (length && out[length - 1] != '/')
                length--;
            if (length)
                length--;
        } else if (size && !(size == 1 && part[0] == '.')) {
            if (length)
                out[length++] = '/';
            memcpy(out + length, part, size);
            length += size;
        }
        part = end ? end + 1 : NULL;
    }
    out[length] = '\0';
    return out;
}

static char *rs_quoted(CBMArena *arena, const char *value) {
    while (value && *value && isspace((unsigned char)*value))
        value++;
    if (!value || (*value != '"' && *value != '\''))
        return NULL;
    char quote = *value++;
    const char *end = strchr(value, quote);
    return end ? cbm_arena_strndup(arena, value, (size_t)(end - value)) : NULL;
}

/* Value of `key` inside a one-line inline table, or NULL. */
static const char *rs_inline_value(const char *table, const char *key) {
    size_t size = strlen(key);
    for (const char *at = table; at && (at = strstr(at, key)) != NULL; at += size) {
        bool start =
            at == table || !(isalnum((unsigned char)at[-1]) || at[-1] == '_' || at[-1] == '-');
        const char *after = at + size;
        while (*after == ' ' || *after == '\t')
            after++;
        if (start && *after == '=') {
            after++;
            while (*after == ' ' || *after == '\t')
                after++;
            return after;
        }
    }
    return NULL;
}

static void rs_dep_note(CBMArena *arena, rs_dep_t **list, const char *key, const char *value) {
    if (!key || !value)
        return;
    const char *folded = rs_folded(arena, key);
    rs_dep_t *dep = NULL;
    for (rs_dep_t *at = *list; at; at = at->next)
        if (!strcmp(at->key, folded))
            dep = at;
    if (!dep) {
        dep = cbm_arena_calloc(arena, sizeof(*dep));
        if (!dep)
            return;
        dep->key = folded;
        dep->next = *list;
        *list = dep;
    }
    const char *path = rs_inline_value(value, "path");
    if (path)
        dep->path = rs_quoted(arena, path);
    const char *workspace = rs_inline_value(value, "workspace");
    if (workspace && !strncmp(workspace, "true", 4))
        dep->workspace = true;
    if (rs_inline_value(value, "package"))
        dep->renamed = true;
}

typedef enum {
    RS_SEC_OTHER = 0,
    RS_SEC_PACKAGE,
    RS_SEC_LIB,
    RS_SEC_TARGET,
    RS_SEC_DEPS,
    RS_SEC_WS_DEPS,
    RS_SEC_WS_PACKAGE
} rs_section_t;

static rs_section_t rs_section_of(CBMArena *arena, const char *header, rs_target_t **target,
                                  rs_package_t *package, const char **dep_table) {
    *dep_table = NULL;
    static const struct {
        const char *name;
        rs_crate_kind_t kind;
    } targets[] = {{"[[bin]]", RS_CRATE_BIN},
                   {"[[test]]", RS_CRATE_AUX},
                   {"[[bench]]", RS_CRATE_AUX},
                   {"[[example]]", RS_CRATE_AUX}};
    for (size_t i = 0; i < sizeof(targets) / sizeof(targets[0]); i++) {
        if (!strncmp(header, targets[i].name, strlen(targets[i].name))) {
            rs_target_t *entry = cbm_arena_calloc(arena, sizeof(*entry));
            if (!entry)
                return RS_SEC_OTHER;
            entry->kind = targets[i].kind;
            entry->next = package->targets;
            package->targets = entry;
            *target = entry;
            return RS_SEC_TARGET;
        }
    }
    if (!strncmp(header, "[package]", 9))
        return RS_SEC_PACKAGE;
    if (!strncmp(header, "[lib]", 5))
        return RS_SEC_LIB;
    if (!strncmp(header, "[workspace.package]", 19))
        return RS_SEC_WS_PACKAGE;
    const char *end = strchr(header, ']');
    /* [dependencies], [dev-dependencies], [build-dependencies], their
     * [target.'cfg'.*] forms, and the [x.dependencies.name] table spelling. */
    const char *found = NULL;
    for (const char *at = header; (at = strstr(at, "dependencies")) != NULL; at += 12)
        found = at;
    if (!found || !end || found > end)
        return RS_SEC_OTHER;
    bool workspace = !strncmp(header, "[workspace.", 11);
    const char *after = found + 12;
    if (*after == '.')
        *dep_table = cbm_arena_strndup(arena, after + 1, (size_t)(end - after - 1));
    else if (*after != ']')
        return RS_SEC_OTHER;
    return workspace ? RS_SEC_WS_DEPS : RS_SEC_DEPS;
}

static char *rs_strip_comment(char *line) {
    char quote = 0;
    for (char *at = line; *at; at++) {
        if (quote) {
            if (*at == quote)
                quote = 0;
        } else if (*at == '"' || *at == '\'') {
            quote = *at;
        } else if (*at == '#') {
            *at = '\0';
            break;
        }
    }
    return line;
}

static void rs_manifest_line(CBMArena *arena, rs_package_t *package, rs_section_t section,
                             rs_target_t *target, const char *dep_table, const char *key,
                             const char *value) {
    switch (section) {
    case RS_SEC_PACKAGE:
        if (!strcmp(key, "name"))
            package->name = rs_quoted(arena, value);
        else if (!strcmp(key, "edition"))
            package->edition_2015 = value && strstr(value, "2015");
        else if (!strcmp(key, "edition.workspace"))
            package->edition_inherited = true;
        else if (!strcmp(key, "build"))
            package->build_path = rs_quoted(arena, value);
        else if (!strcmp(key, "autobins"))
            package->auto_bins = strncmp(value, "false", 5) != 0;
        else if (!strcmp(key, "autotests"))
            package->auto_tests = strncmp(value, "false", 5) != 0;
        else if (!strcmp(key, "autobenches"))
            package->auto_benches = strncmp(value, "false", 5) != 0;
        else if (!strcmp(key, "autoexamples"))
            package->auto_examples = strncmp(value, "false", 5) != 0;
        break;
    case RS_SEC_LIB:
        if (!strcmp(key, "name"))
            package->lib_name = rs_quoted(arena, value);
        else if (!strcmp(key, "path"))
            package->lib_path = rs_quoted(arena, value);
        break;
    case RS_SEC_TARGET:
        if (!strcmp(key, "name"))
            target->name = rs_quoted(arena, value);
        else if (!strcmp(key, "path"))
            target->path = rs_quoted(arena, value);
        break;
    case RS_SEC_WS_PACKAGE:
        if (!strcmp(key, "edition"))
            package->workspace_edition = rs_quoted(arena, value);
        break;
    case RS_SEC_DEPS:
    case RS_SEC_WS_DEPS: {
        rs_dep_t **list = section == RS_SEC_DEPS ? &package->deps : &package->workspace_deps;
        if (dep_table) {
            /* [dependencies.foo]: key = value */
            rs_dep_note(arena, list, dep_table, cbm_arena_sprintf(arena, "%s = %s", key, value));
        } else {
            const char *dot = strchr(key, '.');
            if (dot) { /* foo.workspace = true, foo.path = "x" */
                const char *name = cbm_arena_strndup(arena, key, (size_t)(dot - key));
                rs_dep_note(arena, list, name, cbm_arena_sprintf(arena, "%s = %s", dot + 1, value));
            } else {
                rs_dep_note(arena, list, key, value);
            }
        }
        break;
    }
    default:
        break;
    }
}

bool cbm_rust_project_add_manifest(cbm_rust_project_t *project, const char *rel, const char *text,
                                   size_t length) {
    if (!project || project->sealed || !rel || !text)
        return false;
    CBMArena *arena = &project->arena;
    rs_package_t *package = cbm_arena_calloc(arena, sizeof(*package));
    char *copy = cbm_arena_strndup(arena, text, length);
    if (!package || !copy)
        return false;
    package->rel = cbm_arena_strdup(arena, rel);
    package->dir = rs_dirname(arena, rel);
    package->auto_bins = package->auto_tests = package->auto_benches = package->auto_examples =
        true;
    package->edition_2015 = true; /* Cargo's default when no edition is written */
    package->next = project->packages;
    project->packages = package;
    rs_section_t section = RS_SEC_OTHER;
    rs_target_t *target = NULL;
    const char *dep_table = NULL;
    for (char *line = copy; line && *line;) {
        char *next = strchr(line, '\n');
        if (next)
            *next++ = '\0';
        rs_strip_comment(line);
        while (isspace((unsigned char)*line))
            line++;
        if (*line == '[') {
            section = rs_section_of(arena, line, &target, package, &dep_table);
            package->has_package |= section == RS_SEC_PACKAGE;
        } else if (*line) {
            char *equal = strchr(line, '=');
            if (equal) {
                *equal = '\0';
                char *end = equal - 1;
                while (end >= line && isspace((unsigned char)*end))
                    *end-- = '\0';
                char *value = equal + 1;
                while (isspace((unsigned char)*value))
                    value++;
                if (*line == '"' || *line == '\'') {
                    line++;
                    size_t size = strlen(line);
                    if (size)
                        line[size - 1] = '\0';
                }
                rs_manifest_line(arena, package, section, target, dep_table, line, value);
            }
        }
        line = next;
    }
    return package->rel != NULL;
}

/* ── Module tree ───────────────────────────────────────────────────── */

static rs_inst_t *rs_inst_build(cbm_rust_project_t *project, rs_crate_t *crate, rs_mod_t *syn,
                                rs_inst_t *parent, const char *dir, unsigned depth);

static void rs_inst_note(CBMArena *arena, rs_mod_t *syn, rs_inst_t *inst) {
    rs_inst_t **list = cbm_arena_alloc(arena, (size_t)(syn->ninstances + 1) * sizeof(*list));
    if (!list)
        return;
    if (syn->ninstances)
        memcpy(list, syn->instances, (size_t)syn->ninstances * sizeof(*list));
    list[syn->ninstances++] = inst;
    syn->instances = list;
}

static bool rs_inst_chain_has(const rs_inst_t *inst, const rs_mod_t *syn) {
    for (; inst; inst = inst->parent)
        if (inst->syn == syn)
            return true;
    return false;
}

static void rs_child_add(CBMArena *arena, rs_inst_t *inst, const char *name, rs_inst_t *child) {
    rs_child_t *entry = cbm_arena_calloc(arena, sizeof(*entry));
    if (!entry)
        return;
    entry->name = name;
    entry->inst = child;
    entry->missing_file = child == NULL;
    entry->next = inst->children;
    inst->children = entry;
}

static void rs_note_macro_export(CBMArena *arena, rs_crate_t *crate, rs_item_t *item) {
    if (crate->nmacro_exports == crate->cap_macro_exports) {
        int cap = crate->cap_macro_exports ? crate->cap_macro_exports * 2 : 8;
        rs_item_t **list = cbm_arena_alloc(arena, (size_t)cap * sizeof(*list));
        if (!list)
            return;
        if (crate->nmacro_exports)
            memcpy(list, crate->macro_exports, (size_t)crate->nmacro_exports * sizeof(*list));
        crate->macro_exports = list;
        crate->cap_macro_exports = cap;
    }
    crate->macro_exports[crate->nmacro_exports++] = item;
}

static void rs_inst_children(cbm_rust_project_t *project, rs_inst_t *inst, unsigned depth) {
    CBMArena *arena = &project->arena;
    rs_mod_t *syn = inst->syn;
    for (rs_item_t *item = syn->item_list; item; item = item->next) {
        if (item->kind == RS_ITEM_MACRO && item->unit_struct)
            rs_note_macro_export(arena, inst->crate, item);
        if (item->kind != RS_ITEM_MOD)
            continue;
        const char *child_dir = rs_join(arena, inst->dir, item->name);
        if (item->inline_mod) {
            if (item->path_attr)
                child_dir = rs_join(arena, inst->dir, item->path_attr);
            rs_inst_t *child =
                rs_inst_build(project, inst->crate, item->inline_mod, inst, child_dir, depth + 1);
            rs_child_add(arena, inst, item->name, child);
            continue;
        }
        rs_file_t *file = NULL;
        if (item->path_attr) {
            const char *base = syn->is_inline ? inst->dir : rs_dirname(arena, syn->file->rel);
            const char *rel = rs_join(arena, base, item->path_attr);
            file = rel ? rs_file_find(project, rel) : NULL;
            child_dir = rel ? rs_dirname(arena, rel) : child_dir;
        } else {
            const char *flat = cbm_arena_sprintf(arena, "%s.rs", child_dir);
            const char *nested = cbm_arena_sprintf(arena, "%s/mod.rs", child_dir);
            rs_file_t *first = flat ? rs_file_find(project, flat) : NULL;
            rs_file_t *second = nested ? rs_file_find(project, nested) : NULL;
            /* Both present is rustc error E0761: no file is the module. */
            file = first && second ? NULL : first ? first : second;
        }
        rs_inst_t *child =
            file && !rs_inst_chain_has(inst, file->root)
                ? rs_inst_build(project, inst->crate, file->root, inst, child_dir, depth + 1)
                : NULL;
        rs_child_add(arena, inst, item->name, child);
    }
}

static rs_inst_t *rs_inst_build(cbm_rust_project_t *project, rs_crate_t *crate, rs_mod_t *syn,
                                rs_inst_t *parent, const char *dir, unsigned depth) {
    if (depth > RS_MAX_MODULE_DEPTH)
        return NULL;
    rs_inst_t *inst = cbm_arena_calloc(&project->arena, sizeof(*inst));
    if (!inst)
        return NULL;
    inst->crate = crate;
    inst->syn = syn;
    inst->parent = parent;
    inst->dir = dir;
    rs_inst_note(&project->arena, syn, inst);
    rs_inst_children(project, inst, depth);
    return inst;
}

static rs_crate_t *rs_crate_add(cbm_rust_project_t *project, rs_package_t *package,
                                const char *name, const char *root_rel, rs_crate_kind_t kind) {
    rs_file_t *file = rs_file_find(project, root_rel);
    if (!file)
        return NULL;
    rs_crate_t *crate = cbm_arena_calloc(&project->arena, sizeof(*crate));
    if (!crate)
        return NULL;
    crate->name = name;
    crate->root_rel = root_rel;
    crate->kind = kind;
    crate->package = package;
    crate->edition_2015 = package ? package->edition_2015 : false;
    crate->extern_prelude_known = package != NULL;
    crate->next = project->crates;
    project->crates = crate;
    return crate;
}

static void rs_crate_root(cbm_rust_project_t *project, rs_crate_t *crate) {
    rs_file_t *file = rs_file_find(project, crate->root_rel);
    if (file)
        crate->root = rs_inst_build(project, crate, file->root, NULL,
                                    rs_dirname(&project->arena, crate->root_rel), 0);
}

static bool rs_under(const char *rel, const char *dir, const char **rest) {
    size_t size = strlen(dir);
    if (!size) {
        *rest = rel;
        return true;
    }
    if (strncmp(rel, dir, size) != 0 || rel[size] != '/')
        return false;
    *rest = rel + size + 1;
    return true;
}

/* src/bin/x.rs, src/bin/x/main.rs and their tests/benches/examples forms. */
static const char *rs_auto_target(CBMArena *arena, const char *rest, const char *prefix) {
    size_t size = strlen(prefix);
    if (strncmp(rest, prefix, size) != 0)
        return NULL;
    rest += size;
    const char *slash = strchr(rest, '/');
    if (!slash) {
        size_t length = strlen(rest);
        return length > 3 && !strcmp(rest + length - 3, ".rs")
                   ? cbm_arena_strndup(arena, rest, length - 3)
                   : NULL;
    }
    return !strcmp(slash, "/main.rs") ? cbm_arena_strndup(arena, rest, (size_t)(slash - rest))
                                      : NULL;
}

static const rs_package_t *rs_owning_package(const cbm_rust_project_t *project, const char *rel) {
    const rs_package_t *best = NULL;
    size_t best_size = 0;
    for (const rs_package_t *package = project->packages; package; package = package->next) {
        const char *rest;
        if (!package->has_package || !rs_under(rel, package->dir, &rest))
            continue;
        size_t size = strlen(package->dir);
        if (!best || size > best_size) {
            best = package;
            best_size = size;
        }
    }
    return best;
}

static bool rs_target_listed(const rs_package_t *package, const char *rel, CBMArena *arena) {
    for (const rs_target_t *target = package->targets; target; target = target->next)
        if (target->path) {
            const char *listed = rs_join(arena, package->dir, target->path);
            if (listed && !strcmp(listed, rel))
                return true;
        }
    return false;
}

static void rs_package_crates(cbm_rust_project_t *project, rs_package_t *package) {
    CBMArena *arena = &project->arena;
    const char *folded = rs_folded(arena, package->name);
    const char *lib_rel =
        rs_join(arena, package->dir, package->lib_path ? package->lib_path : "src/lib.rs");
    const char *lib_name = package->lib_name ? rs_folded(arena, package->lib_name) : folded;
    package->lib_name = lib_name;
    if (lib_rel)
        package->lib_crate = rs_crate_add(project, package, lib_name, lib_rel, RS_CRATE_LIB);
    for (rs_target_t *target = package->targets; target; target = target->next) {
        const char *path = target->path;
        const char *rel = path ? rs_join(arena, package->dir, path) : NULL;
        if (!rel && target->name) {
            /* A target without path uses the conventional location. */
            static const char *const dirs[] = {"src/bin", "tests", "benches", "examples"};
            for (size_t i = 0; !rel && i < sizeof(dirs) / sizeof(dirs[0]); i++) {
                const char *guess = cbm_arena_sprintf(arena, "%s/%s.rs", dirs[i], target->name);
                const char *full = guess ? rs_join(arena, package->dir, guess) : NULL;
                if (full && rs_file_find(project, full))
                    rel = full;
            }
        }
        if (rel)
            (void)rs_crate_add(project, package,
                               rs_folded(arena, target->name ? target->name : folded), rel,
                               target->kind);
    }
    const char *main_rel = rs_join(arena, package->dir, "src/main.rs");
    if (package->auto_bins && main_rel && !rs_target_listed(package, main_rel, arena))
        (void)rs_crate_add(project, package, folded, main_rel, RS_CRATE_BIN);
    const char *build_rel =
        rs_join(arena, package->dir, package->build_path ? package->build_path : "build.rs");
    if (build_rel)
        (void)rs_crate_add(project, package, "build_script_build", build_rel, RS_CRATE_AUX);
}

/* src/bin, tests, benches and examples of whichever package owns each file. */
static void rs_auto_crates(cbm_rust_project_t *project) {
    CBMArena *arena = &project->arena;
    for (int i = 0; i < project->nfiles; i++) {
        const char *rel = project->files[i]->rel, *rest = NULL;
        rs_package_t *package = (rs_package_t *)rs_owning_package(project, rel);
        if (!package || !package->name || !rs_under(rel, package->dir, &rest))
            continue;
        const char *name = NULL;
        rs_crate_kind_t kind = RS_CRATE_AUX;
        if (package->auto_bins && (name = rs_auto_target(arena, rest, "src/bin/")) != NULL)
            kind = RS_CRATE_BIN;
        else if (!(package->auto_tests && (name = rs_auto_target(arena, rest, "tests/"))) &&
                 !(package->auto_benches && (name = rs_auto_target(arena, rest, "benches/"))) &&
                 !(package->auto_examples && (name = rs_auto_target(arena, rest, "examples/"))))
            continue;
        if (!rs_target_listed(package, rel, arena))
            (void)rs_crate_add(project, package, rs_folded(arena, name), rel, kind);
    }
}

static const rs_package_t *rs_package_at(const cbm_rust_project_t *project, const char *dir) {
    for (const rs_package_t *package = project->packages; package; package = package->next)
        if (package->has_package && package->dir && !strcmp(package->dir, dir))
            return package;
    return NULL;
}

/* The nearest ancestor manifest listing this key in [workspace.dependencies]. */
static const rs_dep_t *rs_workspace_dep(const cbm_rust_project_t *project, const char *start,
                                        const char *key, const rs_package_t **owner) {
    const rs_package_t *best = NULL;
    const rs_dep_t *found = NULL;
    size_t best_size = 0;
    for (const rs_package_t *package = project->packages; package; package = package->next) {
        const char *rest;
        if (!rs_under(start, package->dir, &rest) && strcmp(start, package->dir) != 0)
            continue;
        for (const rs_dep_t *dep = package->workspace_deps; dep; dep = dep->next) {
            size_t size = strlen(package->dir);
            if (!strcmp(dep->key, key) && (!best || size >= best_size)) {
                best = package;
                found = dep;
                best_size = size;
            }
        }
    }
    *owner = best;
    return found;
}

static void rs_extern_add(CBMArena *arena, rs_crate_t *crate, const char *name,
                          rs_crate_t *target) {
    rs_extern_t *entry = cbm_arena_calloc(arena, sizeof(*entry));
    if (!entry)
        return;
    entry->name = name;
    entry->crate = target;
    entry->next = crate->externs;
    crate->externs = entry;
}

static void rs_wire_externs(cbm_rust_project_t *project) {
    CBMArena *arena = &project->arena;
    for (rs_crate_t *crate = project->crates; crate; crate = crate->next) {
        rs_package_t *package = crate->package;
        if (!package)
            continue;
        if (crate->kind != RS_CRATE_LIB && package->lib_crate)
            rs_extern_add(arena, crate, package->lib_name, package->lib_crate);
        for (rs_dep_t *dep = package->deps; dep; dep = dep->next) {
            const char *path = dep->path;
            const char *base = package->dir;
            bool renamed = dep->renamed;
            if (dep->workspace) {
                const rs_package_t *owner = NULL;
                const rs_dep_t *shared = rs_workspace_dep(project, package->dir, dep->key, &owner);
                path = shared ? shared->path : NULL;
                base = owner ? owner->dir : "";
                renamed = renamed || (shared && shared->renamed);
            }
            const char *dir = path ? rs_join(arena, base, path) : NULL;
            const rs_package_t *target = dir ? rs_package_at(project, dir) : NULL;
            /* Code names a dependency by its [lib] name unless Cargo.toml renamed it. */
            const char *name = target && !renamed && target->lib_name ? target->lib_name : dep->key;
            rs_extern_add(arena, crate, name, target ? target->lib_crate : NULL);
        }
    }
}

static void rs_inherit_editions(cbm_rust_project_t *project) {
    for (rs_package_t *package = project->packages; package; package = package->next) {
        if (!package->edition_inherited)
            continue;
        const char *edition = NULL;
        size_t best = 0;
        for (const rs_package_t *owner = project->packages; owner; owner = owner->next) {
            const char *rest;
            if (owner->workspace_edition && rs_under(package->dir, owner->dir, &rest) &&
                (!edition || strlen(owner->dir) >= best)) {
                edition = owner->workspace_edition;
                best = strlen(owner->dir);
            }
        }
        package->edition_2015 = edition && strstr(edition, "2015");
    }
}

static void rs_settle_mod(rs_query_t *q, rs_mod_t *mod) {
    rs_inst_t *inst = mod->ninstances ? mod->instances[0] : NULL;
    for (rs_impl_t *impl = mod->impls; inst && impl; impl = impl->next)
        rs_settle_impl(q, impl, inst);
    for (rs_mod_t *child = mod->children; child; child = child->next_child)
        rs_settle_mod(q, child);
}

bool cbm_rust_project_seal(cbm_rust_project_t *project) {
    if (!project || project->sealed)
        return false;
    CBMArena *arena = &project->arena;
    if (project->nfiles > 1)
        qsort(project->files, (size_t)project->nfiles, sizeof(*project->files), rs_file_compare);
    bool any_manifest = false;
    rs_inherit_editions(project);
    for (rs_package_t *package = project->packages; package; package = package->next) {
        any_manifest = any_manifest || package->has_package;
        if (package->has_package && package->name)
            rs_package_crates(project, package);
    }
    rs_auto_crates(project);
    if (!any_manifest) {
        /* Without a manifest an entry file is an explicit rustc root. */
        for (int i = 0; i < project->nfiles; i++) {
            const char *rel = project->files[i]->rel;
            const char *leaf = strrchr(rel, '/');
            leaf = leaf ? leaf + 1 : rel;
            if (!strcmp(leaf, "main.rs") || !strcmp(leaf, "lib.rs"))
                (void)rs_crate_add(project, NULL, "crate", rel, RS_CRATE_AUX);
        }
    }
    rs_wire_externs(project);
    for (rs_crate_t *crate = project->crates; crate; crate = crate->next)
        rs_crate_root(project, crate);
    /* A file no crate root reaches still has true same-module names. */
    for (int i = 0; i < project->nfiles; i++) {
        rs_file_t *file = project->files[i];
        if (file->root->ninstances)
            continue;
        rs_crate_t *crate = cbm_arena_calloc(arena, sizeof(*crate));
        if (!crate)
            return false;
        crate->name = "orphan";
        crate->root_rel = file->rel;
        crate->orphan = true;
        crate->root =
            rs_inst_build(project, crate, file->root, NULL, rs_dirname(arena, file->rel), 0);
        if (!crate->root)
            return false;
    }
    /* Seal only now: impl settlement reads the finished module tree. */
    project->sealed = true;
    rs_query_t q = {.project = project, .arena = arena};
    for (int i = 0; i < project->nfiles; i++)
        rs_settle_mod(&q, project->files[i]->root);
    return true;
}

/* ── Lookup ────────────────────────────────────────────────────────── */

rs_inst_t *rs_module_of(rs_inst_t *inst) {
    while (inst && inst->syn && inst->syn->is_block && inst->parent)
        inst = inst->parent;
    return inst;
}

rs_inst_t *rs_inst_for(const rs_mod_t *syn, const rs_crate_t *crate) {
    if (!syn || !syn->ninstances)
        return NULL;
    for (int i = 0; crate && i < syn->ninstances; i++)
        if (syn->instances[i]->crate == crate)
            return syn->instances[i];
    return syn->instances[0];
}

rs_mod_t *rs_mod_at(rs_mod_t *mod, uint32_t start, uint32_t end) {
    for (rs_mod_t *child = mod->children; child; child = child->next_child)
        if (start >= child->start && end <= child->end)
            return rs_mod_at(child, start, end);
    return mod;
}

bool rs_visible(rs_vis_t vis, const rs_mod_t *owner, const rs_inst_t *from) {
    /* A block's own items can only be named from inside that block. */
    if (vis == RS_VIS_PUB || !from || (owner && owner->is_block))
        return true;
    if (vis == RS_VIS_CRATE) {
        for (int i = 0; owner && i < owner->ninstances; i++)
            if (owner->instances[i]->crate == from->crate)
                return true;
        return owner && owner->is_block;
    }
    const rs_mod_t *scope = owner;
    if (vis == RS_VIS_SUPER && scope) {
        if (scope->parent) {
            scope = scope->parent;
        } else {
            /* A file root's parent is whichever module declared the file. */
            for (const rs_inst_t *at = from; at; at = at->parent)
                for (const rs_child_t *child = at->children; child; child = child->next)
                    if (child->inst && child->inst->syn == owner)
                        return true;
            return false;
        }
    }
    for (const rs_inst_t *at = from; at; at = at->parent)
        if (at->syn == scope)
            return true;
    return false;
}

static rs_ent_t rs_ent(rs_ent_kind_t kind, rs_via_t via) {
    return (rs_ent_t){.kind = kind, .via = via};
}

static rs_ent_t rs_ent_item(rs_item_t *item, rs_via_t via) {
    return (rs_ent_t){.kind = RS_ENT_ITEM, .via = via, .item = item};
}

static rs_ent_t rs_ent_unknown(const char *reason) {
    return (rs_ent_t){.kind = RS_ENT_UNKNOWN, .reason = reason};
}

static bool rs_ent_found(const rs_ent_t *ent) {
    return ent->kind != RS_ENT_NONE;
}

static bool rs_ent_same(const rs_ent_t *a, const rs_ent_t *b) {
    if (a->kind != b->kind)
        return false;
    switch (a->kind) {
    case RS_ENT_MODULE:
        return a->module == b->module ||
               (a->module && b->module && a->module->syn == b->module->syn);
    case RS_ENT_ITEM:
        return a->item == b->item ||
               (a->item->qn && b->item->qn && !strcmp(a->item->qn, b->item->qn));
    case RS_ENT_VARIANT:
        return a->variant == b->variant;
    case RS_ENT_EXTERNAL:
    case RS_ENT_PRELUDE:
        return !strcmp(a->external_path ? a->external_path : "",
                       b->external_path ? b->external_path : "");
    default:
        return true;
    }
}

static const rs_child_t *rs_child_of(const rs_inst_t *inst, const char *name, bool *ambiguous) {
    const rs_child_t *found = NULL;
    for (const rs_child_t *child = inst->children; child; child = child->next) {
        if (strcmp(child->name, name) != 0)
            continue;
        if (found && found->inst != child->inst && !(found->missing_file && child->missing_file)) {
            /* cfg twins: `#[cfg(a)] mod x; #[cfg(not(a))] #[path] mod x;` */
            if (found->missing_file) {
                found = child;
                continue;
            }
            if (!child->missing_file && ambiguous)
                *ambiguous = true;
            continue;
        }
        found = found ? found : child;
    }
    return found;
}

static rs_ent_t rs_module_item_ent(rs_inst_t *inst, const rs_item_t *item, rs_via_t via) {
    bool ambiguous = false;
    const rs_child_t *child = rs_child_of(inst, item->name, &ambiguous);
    if (ambiguous)
        return rs_ent(RS_ENT_AMBIGUOUS, via);
    if (!child || child->missing_file || !child->inst)
        return rs_ent_unknown("rust_module_file_not_indexed");
    return (rs_ent_t){.kind = RS_ENT_MODULE, .via = via, .module = child->inst};
}

static int rs_items_lower_bound(const rs_mod_t *mod, const char *name) {
    int low = 0, high = mod->nitems;
    while (low < high) {
        int mid = low + (high - low) / 2;
        if (strcmp(mod->items[mid]->name, name) < 0)
            low = mid + 1;
        else
            high = mid;
    }
    return low;
}

/* Step 1: the module's own items. */
static rs_ent_t rs_local_items(rs_inst_t *inst, const char *name, unsigned ns, rs_inst_t *from) {
    const rs_mod_t *mod = inst->syn;
    rs_ent_t found = rs_ent(RS_ENT_NONE, RS_VIA_NONE);
    bool hidden = false;
    for (int i = rs_items_lower_bound(mod, name); i < mod->nitems; i++) {
        rs_item_t *item = mod->items[i];
        if (strcmp(item->name, name) != 0)
            break;
        if (!(item->ns & ns))
            continue;
        if (!rs_visible(item->vis, mod, from)) {
            hidden = true;
            continue;
        }
        rs_ent_t next = item->kind == RS_ITEM_MOD ? rs_module_item_ent(inst, item, RS_VIA_LOCAL)
                                                  : rs_ent_item(item, RS_VIA_LOCAL);
        if (rs_ent_found(&found) && !rs_ent_same(&found, &next))
            return rs_ent(RS_ENT_AMBIGUOUS, RS_VIA_LOCAL);
        if (!rs_ent_found(&found))
            found = next;
    }
    if (!rs_ent_found(&found) && hidden)
        return rs_ent(RS_ENT_NOT_VISIBLE, RS_VIA_LOCAL);
    return found;
}

#define RS_MACRO_DECLARED_ITEM "rust_macro_declared_item"

static rs_ent_t rs_use_target(rs_query_t *q, rs_inst_t *inst, const rs_use_t *use, unsigned ns) {
    rs_inst_t *last = NULL;
    int consumed = 0;
    rs_ent_t ent = rs_resolve_path(q, inst, NULL, 0, use->segs, use->nseg, use->absolute, ns, true,
                                   &last, &consumed);
    /* A use path names a whole item; members of a type cannot be imported. */
    if (rs_ent_found(&ent) && consumed < use->nseg && ent.kind != RS_ENT_EXTERNAL &&
        ent.kind != RS_ENT_UNKNOWN && ent.kind != RS_ENT_PRELUDE)
        return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
    /* The path stops in a module whose macros can declare the name: the import
     * is real, only its target is unseen. */
    if (ent.kind == RS_ENT_NONE && last && last->syn->has_macro_items)
        return rs_ent_unknown(RS_MACRO_DECLARED_ITEM);
    return ent;
}

/* Step 2: named imports. A local name bound twice under cfg stays one binding
 * only when both leaves reach one declaration. */
static rs_ent_t rs_named_uses(rs_query_t *q, rs_inst_t *inst, const char *name, unsigned ns,
                              rs_inst_t *from) {
    rs_ent_t found = rs_ent(RS_ENT_NONE, RS_VIA_NONE);
    bool hidden = false, found_cfg = false;
    for (const rs_use_t *use = inst->syn->uses; use; use = use->next) {
        if (!use->local || strcmp(use->local, name) != 0)
            continue;
        if (!rs_visible(use->vis, inst->syn, from)) {
            hidden = true;
            continue;
        }
        rs_ent_t next = rs_use_target(q, inst, use, ns);
        if (!rs_ent_found(&next))
            continue;
        next.via = RS_VIA_NAMED;
        if (rs_ent_found(&found) && !rs_ent_same(&found, &next)) {
            /* An unknown path cannot settle a name a known path also binds. */
            if (found.kind == RS_ENT_UNKNOWN) {
                found = next;
                found_cfg = use->has_cfg;
                continue;
            }
            if (next.kind == RS_ENT_UNKNOWN)
                continue;
            /* One name twice in one namespace does not compile, so an external path
             * beside a project one binds another namespace: `use thiserror::Error`
             * is the derive beside `use crate::Error`. Under cfg they can be two. */
            if (!found_cfg && !use->has_cfg &&
                (found.kind == RS_ENT_EXTERNAL) != (next.kind == RS_ENT_EXTERNAL)) {
                if (found.kind == RS_ENT_EXTERNAL)
                    found = next;
                continue;
            }
            return rs_ent(RS_ENT_AMBIGUOUS, RS_VIA_NAMED);
        }
        if (!rs_ent_found(&found)) {
            found = next;
            found_cfg = use->has_cfg;
        }
    }
    if (!rs_ent_found(&found) && hidden)
        return rs_ent(RS_ENT_NOT_VISIBLE, RS_VIA_NAMED);
    return found;
}

static rs_ent_t rs_variant_ent(rs_item_t *item, const char *name, unsigned ns) {
    if (!(ns & (RS_NS_VALUE | RS_NS_TYPE)))
        return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
    for (rs_variant_t *variant = item->variants; variant; variant = variant->next)
        if (!strcmp(variant->name, name))
            return (rs_ent_t){.kind = RS_ENT_VARIANT, .item = item, .variant = variant};
    return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
}

/* Step 3: globs. A name two globs bind differently is unusable in a program
 * that compiles, so such a use is outside the supported domain. */
static rs_ent_t rs_glob_uses(rs_query_t *q, rs_inst_t *inst, const char *name, unsigned ns,
                             rs_inst_t *from) {
    rs_ent_t found = rs_ent(RS_ENT_NONE, RS_VIA_NONE);
    bool open_domain = false;
    for (const rs_use_t *use = inst->syn->globs; use; use = use->next) {
        if (!rs_visible(use->vis, inst->syn, from))
            continue;
        rs_inst_t *last = NULL;
        int consumed = 0;
        rs_ent_t target = rs_resolve_path(q, inst, NULL, 0, use->segs, use->nseg, use->absolute,
                                          RS_NS_TYPE, true, &last, &consumed);
        rs_ent_t next = rs_ent(RS_ENT_NONE, RS_VIA_NONE);
        if (target.kind == RS_ENT_MODULE && consumed == use->nseg)
            next = rs_lookup_ns(q, target.module, name, ns, inst);
        else if (target.kind == RS_ENT_ITEM && target.item->kind == RS_ITEM_ENUM &&
                 consumed == use->nseg)
            next = rs_variant_ent(target.item, name, ns);
        else
            open_domain = true; /* external or unresolved: not enumerable */
        if (next.kind == RS_ENT_NOT_VISIBLE)
            continue;
        if (next.kind == RS_ENT_UNKNOWN) {
            open_domain = true;
            continue;
        }
        if (!rs_ent_found(&next))
            continue;
        next.via = RS_VIA_GLOB;
        if (rs_ent_found(&found) && !rs_ent_same(&found, &next))
            return rs_ent(RS_ENT_AMBIGUOUS, RS_VIA_GLOB);
        if (!rs_ent_found(&found))
            found = next;
    }
    if (rs_ent_found(&found)) {
        if (inst->syn->has_macro_items)
            return rs_ent_unknown("rust_glob_name_macro_may_shadow");
        return found;
    }
    return open_domain ? rs_ent_unknown("rust_glob_domain_open") : found;
}

static rs_ent_t rs_lookup_steps(rs_query_t *q, rs_inst_t *inst, const char *name, unsigned ns,
                                rs_inst_t *from) {
    rs_ent_t ent = rs_local_items(inst, name, ns, from);
    if (rs_ent_found(&ent))
        return ent;
    ent = rs_named_uses(q, inst, name, ns, from);
    if (rs_ent_found(&ent))
        return ent;
    /* #[macro_export] macros live at the crate root. */
    if ((ns & RS_NS_MACRO) && inst->crate && inst == inst->crate->root) {
        for (int i = 0; i < inst->crate->nmacro_exports; i++) {
            rs_item_t *item = inst->crate->macro_exports[i];
            if (!strcmp(item->name, name))
                return rs_ent_item(item, RS_VIA_LOCAL);
        }
    }
    return rs_glob_uses(q, inst, name, ns, from);
}

rs_ent_t rs_lookup_ns(rs_query_t *q, rs_inst_t *inst, const char *name, unsigned ns,
                      rs_inst_t *from) {
    if (!inst || !inst->syn || !name)
        return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
    for (const rs_visit_t *visit = q->stack; visit; visit = visit->up) {
        if (visit->inst == inst && visit->ns == ns && !strcmp(visit->name, name)) {
            q->cycle_hit = true;
            return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
        }
    }
    const char *key = NULL;
    if (q->memo) {
        key = cbm_arena_sprintf(q->arena, "%p|%p|%u|%s", (const void *)inst, (const void *)from, ns,
                                name);
        const rs_ent_t *memo = key ? cbm_ht_get(q->memo, key) : NULL;
        if (memo)
            return *memo;
    }
    rs_visit_t visit = {.inst = inst, .name = name, .ns = ns, .up = q->stack};
    const rs_visit_t *saved = q->stack;
    bool saved_cycle = q->cycle_hit;
    q->stack = &visit;
    q->cycle_hit = false;
    rs_ent_t ent = rs_lookup_steps(q, inst, name, ns, from);
    bool cyclic = q->cycle_hit;
    q->stack = saved;
    q->cycle_hit = saved_cycle || cyclic;
    /* A result reached through an open cycle depends on its caller. */
    if (key && !cyclic) {
        rs_ent_t *stored = cbm_arena_alloc(q->arena, sizeof(*stored));
        if (stored) {
            *stored = ent;
            cbm_ht_set(q->memo, key, stored);
        }
    }
    return ent;
}

static bool rs_listed(const char *name, const char *const *names) {
    for (; *names; names++)
        if (!strcmp(*names, name))
            return true;
    return false;
}

static const char *rs_prelude_type(const char *name) {
    static const struct {
        const char *name, *path;
    } types[] = {{"Option", "std::option::Option"},
                 {"Result", "std::result::Result"},
                 {"Vec", "std::vec::Vec"},
                 {"String", "std::string::String"},
                 {"Box", "std::boxed::Box"},
                 {"ToString", "std::string::ToString"},
                 {"ToOwned", "std::borrow::ToOwned"},
                 {"Clone", "std::clone::Clone"},
                 {"Copy", "std::marker::Copy"},
                 {"Send", "std::marker::Send"},
                 {"Sync", "std::marker::Sync"},
                 {"Sized", "std::marker::Sized"},
                 {"Unpin", "std::marker::Unpin"},
                 {"Drop", "std::ops::Drop"},
                 {"Fn", "std::ops::Fn"},
                 {"FnMut", "std::ops::FnMut"},
                 {"FnOnce", "std::ops::FnOnce"},
                 {"AsRef", "std::convert::AsRef"},
                 {"AsMut", "std::convert::AsMut"},
                 {"Into", "std::convert::Into"},
                 {"From", "std::convert::From"},
                 {"TryFrom", "std::convert::TryFrom"},
                 {"TryInto", "std::convert::TryInto"},
                 {"Default", "std::default::Default"},
                 {"Iterator", "std::iter::Iterator"},
                 {"Extend", "std::iter::Extend"},
                 {"IntoIterator", "std::iter::IntoIterator"},
                 {"FromIterator", "std::iter::FromIterator"},
                 {"DoubleEndedIterator", "std::iter::DoubleEndedIterator"},
                 {"ExactSizeIterator", "std::iter::ExactSizeIterator"},
                 {"Eq", "std::cmp::Eq"},
                 {"PartialEq", "std::cmp::PartialEq"},
                 {"Ord", "std::cmp::Ord"},
                 {"PartialOrd", "std::cmp::PartialOrd"}};
    for (size_t i = 0; i < sizeof(types) / sizeof(types[0]); i++)
        if (!strcmp(types[i].name, name))
            return types[i].path;
    return NULL;
}

static rs_ent_t rs_extern_lookup(rs_query_t *q, const rs_crate_t *crate, const char *name) {
    static const char *const toolchain[] = {"std", "core", "alloc", "proc_macro", "test", NULL};
    (void)q;
    if (crate) {
        for (const rs_extern_t *entry = crate->externs; entry; entry = entry->next) {
            if (strcmp(entry->name, name) != 0)
                continue;
            if (entry->crate && entry->crate->root)
                return (rs_ent_t){
                    .kind = RS_ENT_MODULE, .via = RS_VIA_PATH, .module = entry->crate->root};
            return (rs_ent_t){
                .kind = RS_ENT_EXTERNAL, .via = RS_VIA_PATH, .external_path = entry->name};
        }
    }
    if (rs_listed(name, toolchain))
        return (rs_ent_t){.kind = RS_ENT_EXTERNAL, .via = RS_VIA_PATH, .external_path = name};
    return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
}

rs_ent_t rs_lookup_lexical(rs_query_t *q, rs_inst_t *inst, rs_inst_t *const *blocks, int nblocks,
                           const char *name, unsigned ns) {
    for (int i = nblocks - 1; i >= 0; i--) {
        const rs_mod_t *block = blocks[i]->syn;
        for (int m = 0; m < block->nmacro_items; m++)
            if (!strcmp(block->macro_items[m], name))
                return rs_ent_unknown("rust_name_may_be_macro_item");
        rs_ent_t ent = rs_lookup_ns(q, blocks[i], name, ns, blocks[i]);
        if (rs_ent_found(&ent))
            return ent;
    }
    rs_inst_t *module = rs_module_of(inst);
    rs_ent_t ent = rs_lookup_ns(q, module, name, ns, module);
    /* An external glob that cannot be enumerated still does not make a declared
     * dependency or prelude name unknown; neither ever yields a project edge. */
    bool open_glob =
        ent.kind == RS_ENT_UNKNOWN && ent.reason && !strcmp(ent.reason, "rust_glob_domain_open");
    if (rs_ent_found(&ent) && !open_glob)
        return ent;
    if (ns & RS_NS_TYPE) {
        rs_ent_t extern_ent = rs_extern_lookup(q, module ? module->crate : NULL, name);
        if (rs_ent_found(&extern_ent))
            return extern_ent;
        const char *path = rs_prelude_type(name);
        if (path)
            return (rs_ent_t){.kind = RS_ENT_PRELUDE, .via = RS_VIA_PATH, .external_path = path};
        static const char *const builtins[] = {"bool", "char", "str",   "i8",  "i16", "i32",
                                               "i64",  "i128", "isize", "u8",  "u16", "u32",
                                               "u64",  "u128", "usize", "f32", "f64", NULL};
        if (rs_listed(name, builtins))
            return (rs_ent_t){.kind = RS_ENT_BUILTIN, .via = RS_VIA_PATH, .external_path = name};
    }
    if (ns & RS_NS_VALUE) {
        static const struct {
            const char *name, *path;
        } values[] = {{"Some", "std::option::Option::Some"},
                      {"None", "std::option::Option::None"},
                      {"Ok", "std::result::Result::Ok"},
                      {"Err", "std::result::Result::Err"},
                      {"drop", "std::mem::drop"}};
        for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++)
            if (!strcmp(values[i].name, name))
                return (rs_ent_t){
                    .kind = RS_ENT_PRELUDE, .via = RS_VIA_PATH, .external_path = values[i].path};
    }
    if (open_glob)
        return ent;
    /* No crate root reaches this file, so its dependency names are not known. */
    if (module && module->crate && module->crate->orphan)
        return rs_ent_unknown("rust_crate_root_unknown");
    return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
}

static rs_ent_t rs_path_head(rs_query_t *q, rs_inst_t *inst, rs_inst_t *const *blocks, int nblocks,
                             const char *const *segs, int nseg, bool absolute, unsigned ns,
                             bool is_use, int *used) {
    rs_inst_t *module = rs_module_of(inst);
    const rs_crate_t *crate = module ? module->crate : NULL;
    const char *head = segs[0];
    unsigned head_ns = nseg == 1 ? ns : RS_NS_TYPE;
    *used = 1;
    if (absolute) {
        /* ::a is crate-rooted before the 2018 edition, an extern crate after it. */
        if (crate && crate->edition_2015 && crate->root) {
            rs_ent_t rooted = rs_lookup_ns(q, crate->root, head, head_ns, module);
            if (rs_ent_found(&rooted))
                return rooted;
        }
        return rs_extern_lookup(q, crate, head);
    }
    if (!strcmp(head, "crate") || !strcmp(head, "$crate")) {
        if (!crate || crate->orphan || !crate->root)
            return rs_ent_unknown("rust_crate_root_unknown");
        return (rs_ent_t){.kind = RS_ENT_MODULE, .via = RS_VIA_PATH, .module = crate->root};
    }
    if (!strcmp(head, "self"))
        return (rs_ent_t){.kind = RS_ENT_MODULE, .via = RS_VIA_PATH, .module = module};
    if (!strcmp(head, "super")) {
        rs_inst_t *at = module;
        int i = 0;
        while (i < nseg && !strcmp(segs[i], "super")) {
            if (!at || !at->parent)
                return rs_ent_unknown(crate && crate->orphan ? "rust_parent_module_unknown"
                                                             : "rust_super_above_root");
            at = rs_module_of(at->parent);
            i++;
        }
        *used = i;
        return (rs_ent_t){.kind = RS_ENT_MODULE, .via = RS_VIA_PATH, .module = at};
    }
    if (is_use && crate && crate->edition_2015 && crate->root) {
        rs_ent_t ent = rs_lookup_ns(q, crate->root, head, head_ns, module);
        if (rs_ent_found(&ent))
            return ent;
        return rs_extern_lookup(q, crate, head);
    }
    return rs_lookup_lexical(q, inst, blocks, nblocks, head, head_ns);
}

rs_ent_t rs_resolve_path(rs_query_t *q, rs_inst_t *inst, rs_inst_t *const *blocks, int nblocks,
                         const char *const *segs, int nseg, bool absolute, unsigned ns, bool is_use,
                         rs_inst_t **last_module, int *consumed) {
    if (last_module)
        *last_module = NULL;
    if (consumed)
        *consumed = 0;
    if (!inst || nseg <= 0 || !segs)
        return rs_ent(RS_ENT_NONE, RS_VIA_NONE);
    int at = 0;
    rs_ent_t ent = rs_path_head(q, inst, blocks, nblocks, segs, nseg, absolute, ns, is_use, &at);
    rs_inst_t *from = rs_module_of(inst);
    for (;;) {
        if (ent.kind == RS_ENT_MODULE && last_module)
            *last_module = ent.module;
        if (ent.kind == RS_ENT_EXTERNAL || ent.kind == RS_ENT_PRELUDE) {
            /* The rest of the path stays outside the project. */
            const char *path = ent.external_path;
            for (int i = at; path && i < nseg; i++)
                path = cbm_arena_sprintf(q->arena, "%s::%s", path, segs[i]);
            ent.external_path = path;
            if (consumed)
                *consumed = nseg;
            return ent;
        }
        if (ent.kind == RS_ENT_BUILTIN) {
            if (consumed)
                *consumed = nseg;
            return ent;
        }
        if (at >= nseg || !rs_ent_found(&ent) || ent.kind == RS_ENT_AMBIGUOUS ||
            ent.kind == RS_ENT_UNKNOWN || ent.kind == RS_ENT_NOT_VISIBLE ||
            ent.kind == RS_ENT_VARIANT)
            break;
        if (ent.kind == RS_ENT_ITEM) {
            if (ent.item->kind == RS_ITEM_ENUM) {
                rs_ent_t variant = rs_variant_ent(ent.item, segs[at], RS_NS_VALUE);
                if (variant.kind == RS_ENT_VARIANT) {
                    ent = variant;
                    at++;
                    continue;
                }
            }
            break; /* the remaining segments name members of this type */
        }
        unsigned next_ns = at == nseg - 1 ? ns : RS_NS_TYPE;
        rs_via_t via = ent.via;
        ent = rs_lookup_ns(q, ent.module, segs[at], next_ns, from);
        at++;
        (void)via;
        if (ent.via != RS_VIA_NONE)
            ent.via = RS_VIA_PATH;
    }
    if (consumed)
        *consumed = at;
    return ent;
}

/* ── Import facts ──────────────────────────────────────────────────── */

typedef struct {
    const char **segs;
    int nseg;
    bool absolute, glob;
} rs_spelled_t;

static bool rs_spelled_from(CBMArena *arena, const char *path, rs_spelled_t *out) {
    memset(out, 0, sizeof(*out));
    if (!path)
        return false;
    if (!strncmp(path, "::", 2)) {
        out->absolute = true;
        path += 2;
    }
    const char *segs[RS_MAX_SEGMENTS];
    int n = 0;
    while (path && *path && n < RS_MAX_SEGMENTS) {
        const char *end = strstr(path, "::");
        size_t size = end ? (size_t)(end - path) : strlen(path);
        if (size == 1 && *path == '*') {
            out->glob = true;
            break;
        }
        segs[n] = cbm_arena_strndup(arena, path, size);
        if (!segs[n])
            return false;
        n++;
        path = end ? end + 2 : NULL;
    }
    out->segs = cbm_arena_alloc(arena, (size_t)(n ? n : 1) * sizeof(*out->segs));
    if (!out->segs)
        return false;
    if (n)
        memcpy(out->segs, segs, (size_t)n * sizeof(*segs));
    out->nseg = n;
    return true;
}

static cbm_rust_import_t rs_import_in(rs_query_t *q, rs_inst_t *inst, const rs_spelled_t *path) {
    cbm_rust_import_t out = {.reason = "rust_use_path_unresolved"};
    static const unsigned spaces[] = {RS_NS_TYPE, RS_NS_VALUE, RS_NS_MACRO};
    rs_inst_t *macro_module = NULL;
    bool hidden = false;
    for (size_t i = 0; i < sizeof(spaces) / sizeof(spaces[0]); i++) {
        rs_inst_t *last = NULL;
        int consumed = 0;
        rs_ent_t ent = rs_resolve_path(q, inst, NULL, 0, path->segs, path->nseg, path->absolute,
                                       path->glob ? RS_NS_TYPE : spaces[i], true, &last, &consumed);
        if (rs_ent_found(&ent) && consumed < path->nseg && ent.kind != RS_ENT_EXTERNAL &&
            ent.kind != RS_ENT_PRELUDE && ent.kind != RS_ENT_UNKNOWN)
            ent = rs_ent(RS_ENT_NONE, RS_VIA_NONE);
        const rs_mod_t *owner = last ? last->syn : inst->syn;
        switch (ent.kind) {
        case RS_ENT_NONE:
            /* The path stopped inside a module whose macros can declare the rest:
             * `make_error_codes!` in error.rs defines Code. It names that file. */
            if (last && last->syn->has_macro_items)
                macro_module = last;
            continue;
        case RS_ENT_MODULE:
            out.dependency_file = ent.module->syn->file->rel;
            out.reason = "declared_rust_module_path";
            return out;
        case RS_ENT_ITEM:
            if (path->glob && ent.item->kind != RS_ITEM_ENUM) {
                out.reason = "rust_glob_of_non_module";
                return out;
            }
            out.dependency_file = owner->file->rel;
            if (ent.item->qn && !ent.item->qn_conflict && !path->glob)
                out.member_qn = ent.item->qn;
            out.reason = "declared_rust_module_path";
            return out;
        case RS_ENT_VARIANT:
            out.dependency_file = owner->file->rel;
            out.reason = "declared_rust_module_path";
            return out;
        case RS_ENT_EXTERNAL:
        case RS_ENT_PRELUDE:
        case RS_ENT_BUILTIN:
            out.external = true;
            out.reason = "rust_external_crate_import";
            return out;
        case RS_ENT_AMBIGUOUS:
            out.ambiguous = true;
            out.reason = "rust_use_path_ambiguous";
            return out;
        case RS_ENT_NOT_VISIBLE:
            /* A use takes every namespace it may see: a private `mod path` does
             * not stop `pub fn path` from being imported. */
            hidden = true;
            continue;
        case RS_ENT_UNKNOWN:
            if (ent.reason && !strcmp(ent.reason, RS_MACRO_DECLARED_ITEM)) {
                macro_module = last ? last : inst;
                continue;
            }
            out.reason = ent.reason ? ent.reason : "rust_use_path_unknown";
            return out;
        }
    }
    if (macro_module) {
        out.dependency_file = macro_module->syn->file->rel;
        out.reason = "declared_rust_module_path";
    } else if (hidden) {
        out.reason = "rust_use_path_not_visible";
    }
    return out;
}

static cbm_rust_import_t rs_module_decl_in(const rs_inst_t *inst, const char *name) {
    cbm_rust_import_t out = {.reason = "rust_module_file_not_indexed"};
    bool ambiguous = false;
    const rs_child_t *child = rs_child_of(inst, name, &ambiguous);
    if (ambiguous) {
        out.ambiguous = true;
        out.reason = "rust_module_file_ambiguous";
    } else if (child && child->inst) {
        out.dependency_file = child->inst->syn->file->rel;
        out.reason = "declared_rust_module_path";
    }
    return out;
}

cbm_rust_import_t cbm_rust_project_import(const cbm_rust_project_t *project, const char *rel,
                                          const CBMImport *import) {
    cbm_rust_import_t out = {.reason = "rust_project_model_missing"};
    const rs_file_t *file = project ? rs_file_find(project, rel) : NULL;
    if (!file || !import || !import->module_path)
        return out;
    const rs_mod_t *mod = rs_mod_at(file->root, import->site_start_byte, import->site_end_byte);
    CBMArena scratch;
    cbm_arena_init_lazy(&scratch, 4096);
    rs_query_t q = {.project = project, .arena = &scratch};
    rs_spelled_t path;
    bool parsed = rs_spelled_from(&scratch, import->module_path, &path);
    bool first = true;
    for (int i = 0; parsed && i < mod->ninstances; i++) {
        cbm_rust_import_t one = import->kind == CBM_IMPORT_KIND_MODULE_DECLARATION
                                    ? rs_module_decl_in(mod->instances[i], import->module_path)
                                    : rs_import_in(&q, mod->instances[i], &path);
        if (first) {
            out = one;
            first = false;
            continue;
        }
        /* One file in two crates: an instance that cannot see the path does
         * not outvote one that can; two different answers are not one binding. */
        if (!one.dependency_file && !one.external && !one.ambiguous)
            continue;
        if (!out.dependency_file && !out.external && !out.ambiguous) {
            out = one;
            continue;
        }
        bool same_file = (!one.dependency_file && !out.dependency_file) ||
                         (one.dependency_file && out.dependency_file &&
                          !strcmp(one.dependency_file, out.dependency_file));
        bool same_member =
            (!one.member_qn && !out.member_qn) ||
            (one.member_qn && out.member_qn && !strcmp(one.member_qn, out.member_qn));
        if (one.external != out.external || !same_file || !same_member) {
            out = (cbm_rust_import_t){.ambiguous = true, .reason = "rust_crate_instances_disagree"};
            break;
        }
    }
    cbm_arena_destroy(&scratch);
    return out;
}
