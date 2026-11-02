/* Rust occurrence walker: lexical scopes, receiver types and member selection,
 * producing one fact per reference site. See rust_names.h.
 *
 * Rust is statically typed and has no overloading, so a type once established
 * for a binding holds for that binding's whole life, assignment included; only
 * shadowing makes a new binding. Method selection follows the language: inherent
 * methods before trait methods, trait methods only when the trait is in scope,
 * autoderef through references, smart pointers and Deref impls. Standard library
 * signatures used for return types are the published ones, keyed by full path, so
 * a project type named Vec is never mistaken for std::vec::Vec.
 */
#include "rust_names_internal.h"
#include "../lang_specs.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "syntax_index.h"
#include "stats.h"
#include "foundation/hash_table.h"

enum {
    RS_MAX_WALK_DEPTH = 256,
    RS_MAX_BLOCKS = 64,
    RS_MAX_DEREF = 8,
    RS_MAX_LOWER = 24,
    RS_MAX_GENERIC_BINDINGS = 16
};

typedef enum {
    RT_UNKNOWN = 0,
    RT_PRIM,
    RT_ITEM,
    RT_EXT,
    RT_REF,
    RT_TUPLE,
    RT_ARRAY,
    RT_UNIT,
    RT_PARAM, /* generic parameter, impl Trait or dyn Trait: known by its traits */
    RT_FUTURE,
    RT_ITER, /* a std iterator yielding inner */
    RT_NEVER
} rs_type_kind_t;

struct rs_type {
    rs_type_kind_t kind;
    const char *name; /* primitive name, external path or parameter name */
    rs_item_t *item;
    const rs_type_t **args;
    int nargs;
    const rs_type_t *inner;
    rs_item_t **traits;
    int ntraits;
    bool projected; /* what project() returned: fields read through the projection */
    /* RT_PARAM: its bounds as written, the external ones included. */
    rs_tsyn_t *const *bounds;
    int nbounds;
};

static const rs_type_t RS_UNKNOWN_TYPE = {.kind = RT_UNKNOWN};
/* A raw pointer has no autoderef: only an explicit `*p` reaches what it points at. */
static const char RS_RAW_POINTER[] = "std::ptr::raw";
static const rs_type_t RS_UNIT_TYPE = {.kind = RT_UNIT};

enum { RS_LOCALS_INITIAL = 32 };

typedef struct rs_binding {
    const char *name;
    const rs_type_t *type;
    /* `let x;`: the first assignment settles the type. */
    bool deferred;
    struct rs_binding *next;
    struct rs_binding *shadowed; /* the binding of this name it hides, until its scope ends */
    unsigned depth;              /* of its scope */
} rs_binding_t;

typedef struct rs_scope {
    struct rs_scope *parent;
    rs_binding_t *bindings;
    bool fn_boundary;      /* a nested fn item cannot see the locals outside it */
    unsigned depth;        /* outermost scope is 1 */
    unsigned visible_from; /* the depth of the innermost fn boundary */
} rs_scope_t;

typedef struct rs_genv {
    const rs_generics_t *generics;
    const struct rs_genv *up;
} rs_genv_t;

typedef struct {
    rs_query_t *q;
    rs_inst_t *inst;
    const rs_type_t *self_ty;
    const rs_impl_t *impl;
    const rs_genv_t *gens;
    const char *const *subst_names;
    const rs_type_t *const *subst_types;
    int nsubst;
    rs_inst_t *const *blocks;
    int nblocks;
    int depth;
} rs_env_t;

typedef struct {
    const cbm_rust_project_t *project;
    CBMArena *arena;
    rs_builder_t b;
    rs_query_t q;
    rs_inst_t *inst;
    rs_scope_t *scope;
    CBMHashTable *locals; /* name -> its innermost live binding; a body can bind thousands */
    const rs_genv_t *gens;
    const rs_type_t *self_ty;
    const rs_impl_t *impl;
    rs_inst_t *blocks[RS_MAX_BLOCKS];
    int nblocks;
    cbm_rust_site_t *sites;
    size_t nsites, cap_sites;
    unsigned depth;
    bool failed;
    /* Added to node offsets while walking a re-parsed macro argument list. */
    int64_t site_delta;
    TSParser *parser;
    /* The element of the last `.collect()` walked, and where that call ends. */
    const rs_type_t *collected;
    uint32_t collected_end;
    int64_t collected_delta;
    /* The first argument's type, when the call being selected has walked it. */
    const rs_type_t *call_arg0;
    /* Types the enclosing call gives a closure argument's parameters. */
    const rs_type_t *closure_params[2];
    int nclosure_params;
    const rs_type_t *last_closure_ret;
} rs_walk_t;

struct cbm_rust_file_analysis {
    cbm_rust_site_t *sites;
    size_t nsites;
};

/* ── Types ─────────────────────────────────────────────────────────── */

static rs_type_t *rs_type_new(CBMArena *arena, rs_type_kind_t kind) {
    rs_type_t *type = cbm_arena_calloc(arena, sizeof(*type));
    if (type)
        type->kind = kind;
    return type;
}

static const rs_type_t *rs_ref_of(CBMArena *arena, const rs_type_t *inner) {
    rs_type_t *type = rs_type_new(arena, RT_REF);
    if (!type)
        return &RS_UNKNOWN_TYPE;
    type->inner = inner;
    return type;
}

static const rs_type_t *rs_peel(const rs_type_t *type) {
    while (type && type->kind == RT_REF && type->inner)
        type = type->inner;
    return type ? type : &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_ext_type(CBMArena *arena, const char *path, const rs_type_t *a,
                                    const rs_type_t *b) {
    rs_type_t *type = rs_type_new(arena, RT_EXT);
    if (!type)
        return &RS_UNKNOWN_TYPE;
    type->name = path;
    int count = (a ? 1 : 0) + (b ? 1 : 0);
    if (count) {
        type->args = cbm_arena_alloc(arena, (size_t)count * sizeof(*type->args));
        if (type->args) {
            if (a)
                type->args[type->nargs++] = a;
            if (b)
                type->args[type->nargs++] = b;
        }
    }
    return type;
}

static const rs_type_t *rs_iter_of(CBMArena *arena, const rs_type_t *element) {
    rs_type_t *type = rs_type_new(arena, RT_ITER);
    if (!type)
        return &RS_UNKNOWN_TYPE;
    type->inner = element ? element : &RS_UNKNOWN_TYPE;
    return type;
}

static const char *rs_normal_ext_path(CBMArena *arena, const char *path) {
    if (!path)
        return NULL;
    if (!strncmp(path, "core::", 6))
        return cbm_arena_sprintf(arena, "std::%s", path + 6);
    if (!strncmp(path, "alloc::", 7))
        return cbm_arena_sprintf(arena, "std::%s", path + 7);
    return path;
}

static bool rs_ext_is(const rs_type_t *type, const char *path) {
    return type && type->kind == RT_EXT && type->name && !strcmp(type->name, path);
}

/* A crate's `Result<T>` alias keeps the success type first: heed::Result, io::Result. */
static bool rs_ext_is_result(const rs_type_t *type) {
    if (!type || type->kind != RT_EXT || !type->name)
        return false;
    size_t length = strlen(type->name);
    return length >= 6 && !strcmp(type->name + length - 6, "Result") &&
           (length == 6 || type->name[length - 7] == ':');
}

static const rs_type_t *rs_arg(const rs_type_t *type, int index) {
    return type && index < type->nargs && type->args[index] ? type->args[index] : &RS_UNKNOWN_TYPE;
}

/* ── Lowering type syntax in a scope ───────────────────────────────── */

static const rs_type_t *rs_lower(rs_env_t *env, const rs_tsyn_t *syn);

static const rs_generics_t *rs_genv_find(const rs_genv_t *gens, const char *name, int *index) {
    for (; gens; gens = gens->up) {
        for (int i = 0; gens->generics && i < gens->generics->count; i++) {
            if (!strcmp(gens->generics->names[i], name)) {
                *index = i;
                return gens->generics;
            }
        }
    }
    return NULL;
}

static rs_item_t *rs_trait_of_syn(rs_env_t *env, const rs_tsyn_t *syn) {
    if (!syn || syn->kind != RS_TS_PATH)
        return NULL;
    int consumed = 0;
    rs_ent_t ent = rs_resolve_path(env->q, env->inst, env->blocks, env->nblocks, syn->segs,
                                   syn->nseg, syn->absolute, RS_NS_TYPE, false, NULL, &consumed);
    return ent.kind == RS_ENT_ITEM && consumed == syn->nseg && ent.item->kind == RS_ITEM_TRAIT
               ? ent.item
               : NULL;
}

static const char *rs_tsyn_leaf(const rs_tsyn_t *syn) {
    return syn && syn->kind == RS_TS_PATH && syn->nseg ? syn->segs[syn->nseg - 1] : NULL;
}

static bool rs_iterator_trait_name(const char *name) {
    static const char *const names[] = {"Iterator",
                                        "IntoIterator",
                                        "DoubleEndedIterator",
                                        "ExactSizeIterator",
                                        "ParallelIterator",
                                        "IndexedParallelIterator",
                                        NULL};
    for (const char *const *at = names; name && *at; at++)
        if (!strcmp(name, *at))
            return true;
    return false;
}

static const rs_type_t *rs_param_type(rs_env_t *env, const char *name, rs_tsyn_t *const *bounds,
                                      int nbounds) {
    /* `impl Iterator<Item = T>` yields T whatever concrete iterator it hides. */
    for (int i = 0; i < nbounds; i++) {
        if (bounds[i] && bounds[i]->item_binding &&
            rs_iterator_trait_name(rs_tsyn_leaf(bounds[i]))) {
            rs_type_t *iter = rs_type_new(env->q->arena, RT_ITER);
            if (iter) {
                iter->inner = rs_lower(env, bounds[i]->item_binding);
                return iter;
            }
        }
    }
    rs_type_t *type = rs_type_new(env->q->arena, RT_PARAM);
    if (!type)
        return &RS_UNKNOWN_TYPE;
    type->name = name;
    type->bounds = bounds;
    type->nbounds = nbounds;
    if (nbounds > 0) {
        type->traits = cbm_arena_calloc(env->q->arena, (size_t)nbounds * sizeof(*type->traits));
        for (int i = 0; type->traits && i < nbounds; i++) {
            rs_item_t *trait = rs_trait_of_syn(env, bounds[i]);
            if (trait)
                type->traits[type->ntraits++] = trait;
        }
    }
    return type;
}

static const rs_type_t *rs_lower_item(rs_env_t *env, rs_item_t *item, const rs_tsyn_t *syn) {
    rs_query_t *q = env->q;
    switch (item->kind) {
    case RS_ITEM_STRUCT:
    case RS_ITEM_ENUM:
    case RS_ITEM_UNION: {
        rs_type_t *type = rs_type_new(q->arena, RT_ITEM);
        if (!type)
            return &RS_UNKNOWN_TYPE;
        type->item = item;
        if (syn && syn->nargs) {
            type->args = cbm_arena_calloc(q->arena, (size_t)syn->nargs * sizeof(*type->args));
            for (int i = 0; type->args && i < syn->nargs; i++)
                type->args[type->nargs++] = rs_lower(env, syn->args[i]);
        }
        return type;
    }
    case RS_ITEM_TRAIT: {
        /* A bare trait name in type position is a trait object. */
        rs_type_t *type = rs_type_new(q->arena, RT_PARAM);
        if (!type)
            return &RS_UNKNOWN_TYPE;
        type->name = item->name;
        type->traits = cbm_arena_alloc(q->arena, sizeof(*type->traits));
        if (type->traits) {
            type->traits[0] = item;
            type->ntraits = 1;
        }
        return type;
    }
    case RS_ITEM_TYPE: {
        if (!item->type)
            return &RS_UNKNOWN_TYPE;
        int count = item->generics ? item->generics->count : 0;
        const rs_type_t **types =
            count ? cbm_arena_calloc(q->arena, (size_t)count * sizeof(*types)) : NULL;
        for (int i = 0; types && syn && i < count && i < syn->nargs; i++)
            types[i] = rs_lower(env, syn->args[i]);
        for (int i = 0; types && i < count; i++)
            if (!types[i])
                types[i] = &RS_UNKNOWN_TYPE;
        rs_env_t inner = {.q = q,
                          .inst = rs_inst_for(item->owner, env->inst ? env->inst->crate : NULL),
                          .self_ty = NULL,
                          .subst_names = count ? (const char *const *)item->generics->names : NULL,
                          .subst_types = types,
                          .nsubst = types ? count : 0,
                          .depth = env->depth + 1};
        return inner.inst ? rs_lower(&inner, item->type) : &RS_UNKNOWN_TYPE;
    }
    default:
        return &RS_UNKNOWN_TYPE;
    }
}

static const rs_type_t *rs_lower_path(rs_env_t *env, const rs_tsyn_t *syn) {
    if (!syn->nseg)
        return &RS_UNKNOWN_TYPE;
    const char *head = syn->segs[0];
    if (syn->nseg == 1 && !syn->absolute) {
        for (int i = 0; i < env->nsubst; i++)
            if (!strcmp(env->subst_names[i], head) && env->subst_types[i])
                return env->subst_types[i];
        if (!strcmp(head, "Self"))
            return env->self_ty ? env->self_ty : &RS_UNKNOWN_TYPE;
        int index = 0;
        const rs_generics_t *generics = rs_genv_find(env->gens, head, &index);
        if (generics)
            return rs_param_type(env, head, generics->bounds[index], generics->nbounds[index]);
    }
    if (syn->nseg == 2 && !strcmp(head, "Self") && env->impl) {
        for (int i = 0; i < env->impl->nassoc; i++) {
            if (!strcmp(env->impl->assoc_names[i], syn->segs[1])) {
                rs_env_t inner = *env;
                inner.depth++;
                return rs_lower(&inner, env->impl->assoc_types[i]);
            }
        }
        return &RS_UNKNOWN_TYPE;
    }
    int consumed = 0;
    rs_ent_t ent = rs_resolve_path(env->q, env->inst, env->blocks, env->nblocks, syn->segs,
                                   syn->nseg, syn->absolute, RS_NS_TYPE, false, NULL, &consumed);
    if ((ent.kind == RS_ENT_EXTERNAL || ent.kind == RS_ENT_PRELUDE) && ent.external_path) {
        rs_type_t *type = rs_type_new(env->q->arena, RT_EXT);
        if (!type)
            return &RS_UNKNOWN_TYPE;
        type->name = rs_normal_ext_path(env->q->arena, ent.external_path);
        if (syn->nargs) {
            type->args = cbm_arena_calloc(env->q->arena, (size_t)syn->nargs * sizeof(*type->args));
            for (int i = 0; type->args && i < syn->nargs; i++)
                type->args[type->nargs++] = rs_lower(env, syn->args[i]);
        }
        return type;
    }
    if (ent.kind == RS_ENT_BUILTIN && ent.external_path) {
        rs_type_t *type = rs_type_new(env->q->arena, RT_PRIM);
        if (type)
            type->name = ent.external_path;
        return type ? type : &RS_UNKNOWN_TYPE;
    }
    if (ent.kind == RS_ENT_ITEM && consumed == syn->nseg)
        return rs_lower_item(env, ent.item, syn);
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_lower(rs_env_t *env, const rs_tsyn_t *syn) {
    if (!syn || env->depth > RS_MAX_LOWER || !env->inst)
        return &RS_UNKNOWN_TYPE;
    CBMArena *arena = env->q->arena;
    switch (syn->kind) {
    case RS_TS_PATH:
        return rs_lower_path(env, syn);
    case RS_TS_PRIM: {
        /* Primitive names can be shadowed by a type of the same name. */
        const char *segs[1] = {syn->prim};
        int consumed = 0;
        rs_ent_t ent = rs_resolve_path(env->q, env->inst, env->blocks, env->nblocks, segs, 1, false,
                                       RS_NS_TYPE, false, NULL, &consumed);
        if (ent.kind == RS_ENT_ITEM)
            return rs_lower_item(env, ent.item, NULL);
        rs_type_t *type = rs_type_new(arena, RT_PRIM);
        if (type)
            type->name = syn->prim;
        return type ? type : &RS_UNKNOWN_TYPE;
    }
    case RS_TS_PTR:
        return rs_ext_type(env->q->arena, RS_RAW_POINTER, rs_lower(env, syn->inner), NULL);
    case RS_TS_REF:
        return rs_ref_of(arena, rs_lower(env, syn->inner));
    case RS_TS_UNIT:
        return &RS_UNIT_TYPE;
    case RS_TS_NEVER: {
        rs_type_t *type = rs_type_new(arena, RT_NEVER);
        return type ? type : &RS_UNKNOWN_TYPE;
    }
    case RS_TS_TUPLE: {
        rs_type_t *type = rs_type_new(arena, RT_TUPLE);
        if (!type)
            return &RS_UNKNOWN_TYPE;
        type->args =
            syn->nargs ? cbm_arena_calloc(arena, (size_t)syn->nargs * sizeof(*type->args)) : NULL;
        for (int i = 0; type->args && i < syn->nargs; i++)
            type->args[type->nargs++] = rs_lower(env, syn->args[i]);
        return type;
    }
    case RS_TS_ARRAY: {
        rs_type_t *type = rs_type_new(arena, RT_ARRAY);
        if (!type)
            return &RS_UNKNOWN_TYPE;
        type->inner = rs_lower(env, syn->inner);
        return type;
    }
    case RS_TS_DYN:
        return rs_param_type(env, "dyn", syn->args, syn->nargs);
    case RS_TS_FN:
    case RS_TS_UNKNOWN:
        return &RS_UNKNOWN_TYPE;
    }
    return &RS_UNKNOWN_TYPE;
}

/* ── Impl settlement, run once at seal ─────────────────────────────── */

static bool rs_generics_has(const rs_generics_t *generics, const char *name) {
    for (int i = 0; generics && i < generics->count; i++)
        if (!strcmp(generics->names[i], name))
            return true;
    return false;
}

static void rs_attach_impl(rs_item_t *item, rs_impl_t *impl) {
    impl->next_self = item->impls;
    item->impls = impl;
}

void rs_settle_impl(rs_query_t *q, rs_impl_t *impl, rs_inst_t *inst) {
    cbm_rust_project_t *project = (cbm_rust_project_t *)q->project;
    const rs_tsyn_t *self_syn = impl->self_syn;
    while (self_syn && self_syn->kind == RS_TS_REF)
        self_syn = self_syn->inner;
    rs_env_t env = {.q = q, .inst = inst};
    if (impl->trait_syn && impl->trait_syn->kind == RS_TS_PATH) {
        int consumed = 0;
        rs_ent_t ent =
            rs_resolve_path(q, inst, NULL, 0, impl->trait_syn->segs, impl->trait_syn->nseg,
                            impl->trait_syn->absolute, RS_NS_TYPE, false, NULL, &consumed);
        if (ent.kind == RS_ENT_ITEM && ent.item->kind == RS_ITEM_TRAIT &&
            consumed == impl->trait_syn->nseg)
            impl->trait_item = ent.item;
        else
            impl->trait_external = true;
    }
    if (!self_syn)
        return;
    if (self_syn->kind == RS_TS_DYN && self_syn->nargs) {
        rs_item_t *trait = rs_trait_of_syn(&env, self_syn->args[0]);
        if (trait)
            rs_attach_impl(trait, impl);
        return;
    }
    /* `impl Tr for &str`, `impl Tr for u32`: the type is std's, the trait this project's. */
    if (self_syn->kind == RS_TS_PRIM && self_syn->prim) {
        impl->ext_path = self_syn->prim;
        if (!project->ext_impls)
            project->ext_impls = cbm_ht_create(16);
        if (project->ext_impls) {
            impl->next_self = cbm_ht_get(project->ext_impls, impl->ext_path);
            cbm_ht_set(project->ext_impls, impl->ext_path, impl);
        }
        return;
    }
    if (self_syn->kind != RS_TS_PATH || !self_syn->nseg)
        return;
    if (self_syn->nseg == 1 && rs_generics_has(impl->generics, self_syn->segs[0])) {
        impl->next_self = project->blanket_impls;
        project->blanket_impls = impl;
        return;
    }
    int consumed = 0;
    rs_ent_t ent = rs_resolve_path(q, inst, NULL, 0, self_syn->segs, self_syn->nseg,
                                   self_syn->absolute, RS_NS_TYPE, false, NULL, &consumed);
    if (ent.kind == RS_ENT_ITEM && consumed == self_syn->nseg) {
        rs_item_t *item = ent.item;
        if (item->kind == RS_ITEM_TYPE) {
            const rs_type_t *alias = rs_peel(rs_lower_item(&env, item, NULL));
            item = alias->kind == RT_ITEM ? alias->item : NULL;
        }
        if (item && (item->kind == RS_ITEM_STRUCT || item->kind == RS_ITEM_ENUM ||
                     item->kind == RS_ITEM_UNION || item->kind == RS_ITEM_TRAIT))
            rs_attach_impl(item, impl);
    } else if (ent.kind == RS_ENT_EXTERNAL || ent.kind == RS_ENT_PRELUDE ||
               ent.kind == RS_ENT_BUILTIN) {
        impl->ext_path = rs_normal_ext_path(q->arena, ent.external_path);
        if (!project->ext_impls)
            project->ext_impls = cbm_ht_create(16);
        if (project->ext_impls && impl->ext_path) {
            impl->next_self = cbm_ht_get(project->ext_impls, impl->ext_path);
            cbm_ht_set(project->ext_impls, impl->ext_path, impl);
        }
    }
}

/* ── Sites ─────────────────────────────────────────────────────────── */

static void rs_site_push(rs_walk_t *w, cbm_rust_site_t site) {
    if (w->nsites == w->cap_sites) {
        size_t cap = w->cap_sites ? w->cap_sites * 2 : 256;
        cbm_rust_site_t *sites = cbm_arena_alloc(w->arena, cap * sizeof(*sites));
        if (!sites) {
            w->failed = true;
            return;
        }
        if (w->nsites)
            memcpy(sites, w->sites, w->nsites * sizeof(*sites));
        w->sites = sites;
        w->cap_sites = cap;
    }
    w->sites[w->nsites++] = site;
}

static uint32_t rs_rule_facts(cbm_resolve_rule_t rule) {
    uint32_t facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                     CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
    switch (rule) {
    case CBM_RESOLVE_RULE_SCOPE_BINDING:
        return facts | CBM_RESOLVE_FACT_SCOPE_BOUND;
    case CBM_RESOLVE_RULE_IMPORT_BINDING:
        return facts | CBM_RESOLVE_FACT_IMPORT_BOUND;
    case CBM_RESOLVE_RULE_TYPE_MEMBER:
        return facts | CBM_RESOLVE_FACT_RECEIVER_TYPED;
    case CBM_RESOLVE_RULE_MODULE_PATH:
        return facts | CBM_RESOLVE_FACT_PATH_BOUND;
    case CBM_RESOLVE_RULE_DECLARED_TYPE:
        return facts | CBM_RESOLVE_FACT_TYPE_BOUND;
    default:
        return 0;
    }
}

/* A resolved selection: which declaration, by which rule, and what it evaluates to. */
typedef struct {
    const char *target;
    const char *label;
    bool qn_conflict;
    cbm_resolve_rule_t rule;
    const char *reason;
    cbm_resolve_binding_t binding;
    cbm_resolve_domain_t domain;
    cbm_resolve_analysis_t analysis;
    const rs_fn_t *fn;
    const rs_type_t *self_ty;
    const rs_type_t *value_type;
    rs_item_t *ctor;
    rs_item_t *variant_enum;
    rs_variant_t *variant;
    const char *external_path;
    const rs_type_t *assoc_return;
} rs_sel_t;

static rs_sel_t rs_sel_open(const char *reason) {
    return (rs_sel_t){.reason = reason, .analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN};
}

/* The search finished and leaves the project, or names a binding with no node. */
static rs_sel_t rs_sel_settled(const char *reason, cbm_resolve_domain_t domain) {
    return (rs_sel_t){
        .reason = reason, .domain = domain, .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
}

static rs_sel_t rs_sel_target(const char *target, const char *label, bool conflict,
                              cbm_resolve_rule_t rule, const char *reason) {
    return (rs_sel_t){.target = target,
                      .label = label,
                      .qn_conflict = conflict,
                      .rule = rule,
                      .reason = reason,
                      .binding = CBM_RESOLVE_BINDING_UNIQUE,
                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
}

static cbm_resolve_rule_t rs_via_rule(rs_via_t via) {
    return via == RS_VIA_LOCAL                         ? CBM_RESOLVE_RULE_SCOPE_BINDING
           : via == RS_VIA_NAMED || via == RS_VIA_GLOB ? CBM_RESOLVE_RULE_IMPORT_BINDING
                                                       : CBM_RESOLVE_RULE_MODULE_PATH;
}

static const char *rs_via_reason(rs_via_t via) {
    return via == RS_VIA_LOCAL   ? "rust_same_module_item"
           : via == RS_VIA_NAMED ? "rust_named_import"
           : via == RS_VIA_GLOB  ? "rust_glob_import"
                                 : "rust_module_path";
}

static void rs_emit(rs_walk_t *w, cbm_rust_site_kind_t kind, uint32_t start, uint32_t end,
                    const rs_sel_t *sel) {
    cbm_rust_site_t site = {.start = (uint32_t)((int64_t)start + w->site_delta),
                            .end = (uint32_t)((int64_t)end + w->site_delta),
                            .kind = kind,
                            .target = sel->target,
                            .label = sel->label,
                            .rule = sel->rule,
                            /* A tuple struct's name is its constructor function. */
                            .facts = sel->target ? rs_rule_facts(sel->rule) |
                                                       (kind == CBM_RUST_SITE_CALL && sel->ctor
                                                            ? CBM_RESOLVE_FACT_CLASS_CALLABLE
                                                            : 0)
                                                 : 0,
                            .binding = sel->binding,
                            .domain = sel->domain,
                            .analysis = sel->analysis,
                            .reason = sel->reason};
    if (sel->target && sel->qn_conflict) {
        site.target = NULL;
        site.facts = 0;
        site.binding = CBM_RESOLVE_BINDING_UNRESOLVED;
        site.domain = CBM_RESOLVE_DOMAIN_UNKNOWN;
        site.reason = "rust_target_qn_collision";
    }
    rs_site_push(w, site);
}

/* ── Scopes ────────────────────────────────────────────────────────── */

static void rs_scope_push(rs_walk_t *w, rs_scope_t *scope, bool fn_boundary) {
    const rs_scope_t *parent = w->scope;
    unsigned depth = parent ? parent->depth + 1 : 1;
    *scope = (rs_scope_t){.parent = w->scope,
                          .fn_boundary = fn_boundary,
                          .depth = depth,
                          .visible_from = fn_boundary || !parent ? depth : parent->visible_from};
    w->scope = scope;
}

static void rs_scope_pop(rs_walk_t *w) {
    /* Newest first, so a name bound twice here ends at what the scope hid. */
    for (rs_binding_t *binding = w->scope->bindings; binding; binding = binding->next) {
        if (binding->shadowed)
            cbm_ht_set(w->locals, binding->name, binding->shadowed);
        else
            cbm_ht_delete(w->locals, binding->name);
    }
    w->scope = w->scope->parent;
}

static void rs_bind(rs_walk_t *w, const char *name, const rs_type_t *type) {
    if (!name || !w->scope)
        return;
    if (!w->locals)
        w->locals = cbm_ht_create(RS_LOCALS_INITIAL);
    rs_binding_t *binding = cbm_arena_alloc(w->arena, sizeof(*binding));
    if (!binding || !w->locals) {
        w->failed = true;
        return;
    }
    binding->name = name;
    binding->type = type ? type : &RS_UNKNOWN_TYPE;
    binding->deferred = false;
    binding->next = w->scope->bindings;
    binding->shadowed = cbm_ht_get(w->locals, name);
    binding->depth = w->scope->depth;
    w->scope->bindings = binding;
    cbm_ht_set(w->locals, name, binding);
}

/* A binding always goes to the innermost scope, so the newest live one of a name is the
 * one a walk out through the scopes would meet first. */
static rs_binding_t *rs_local(const rs_walk_t *w, const char *name) {
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
    if (!w->scope || !w->locals)
        return NULL;
    rs_binding_t *binding = cbm_ht_get(w->locals, name);
    return binding && binding->depth >= w->scope->visible_from ? binding : NULL;
}

static rs_env_t rs_env_here(rs_walk_t *w) {
    return (rs_env_t){.q = &w->q,
                      .inst = w->inst,
                      .self_ty = w->self_ty,
                      .impl = w->impl,
                      .gens = w->gens,
                      .blocks = w->blocks,
                      .nblocks = w->nblocks};
}

/* ── Member selection ──────────────────────────────────────────────── */

static bool rs_inst_binds_trait(rs_walk_t *w, rs_inst_t *inst, const rs_item_t *trait) {
    const rs_mod_t *syn = inst->syn;
    if (trait->owner == syn)
        return true;
    for (const rs_use_t *use = syn->uses; use; use = use->next) {
        if (!use->nseg || strcmp(use->segs[use->nseg - 1], trait->name) != 0)
            continue;
        int consumed = 0;
        rs_ent_t ent = rs_resolve_path(&w->q, inst, NULL, 0, use->segs, use->nseg, use->absolute,
                                       RS_NS_TYPE, true, NULL, &consumed);
        if (ent.kind == RS_ENT_ITEM && ent.item == trait)
            return true;
    }
    for (const rs_use_t *use = syn->globs; use; use = use->next) {
        int consumed = 0;
        rs_ent_t target = rs_resolve_path(&w->q, inst, NULL, 0, use->segs, use->nseg, use->absolute,
                                          RS_NS_TYPE, true, NULL, &consumed);
        if (target.kind != RS_ENT_MODULE || consumed != use->nseg)
            continue;
        rs_ent_t ent = rs_lookup_ns(&w->q, target.module, trait->name, RS_NS_TYPE, inst);
        if (ent.kind == RS_ENT_ITEM && ent.item == trait)
            return true;
    }
    return false;
}

static bool rs_trait_in_scope(rs_walk_t *w, const rs_item_t *trait) {
    /* The compiler puts the trait being implemented in scope inside its own impl, written by
     * path or not: `impl lib::Config for Mine { fn url(&self) { self.inner.url() } }`. */
    if (w->impl && w->impl->trait_item == trait)
        return true;
    for (int i = w->nblocks - 1; i >= 0; i--)
        if (rs_inst_binds_trait(w, w->blocks[i], trait))
            return true;
    rs_inst_t *module = rs_module_of(w->inst);
    return module && rs_inst_binds_trait(w, module, trait);
}

typedef struct {
    const rs_fn_t *fn;
    const rs_type_t *self_ty;
    const char *reason;
    bool ambiguous, hidden, stop;
} rs_pick_t;

static const rs_type_t *rs_impl_assoc_type(rs_walk_t *w, const rs_impl_t *impl,
                                           const rs_type_t *self_ty, const rs_tsyn_t *syn);

/* One declared parameter against one known argument type: 1 same, 0 different,
 * -1 when either side does not say. */
static int rs_type_matches(const rs_type_t *declared, const rs_type_t *actual) {
    declared = rs_peel(declared);
    actual = rs_peel(actual);
    if (declared->kind == RT_UNKNOWN || actual->kind == RT_UNKNOWN || declared->kind == RT_PARAM ||
        actual->kind == RT_PARAM)
        return -1;
    if (declared->kind != actual->kind)
        return 0;
    switch (declared->kind) {
    case RT_ITEM:
        return declared->item == actual->item || (declared->item->qn && actual->item->qn &&
                                                  !strcmp(declared->item->qn, actual->item->qn));
    case RT_PRIM:
    case RT_EXT:
        return declared->name && actual->name && !strcmp(declared->name, actual->name);
    default:
        return -1;
    }
}

/* The first parameter of a conversion impl method, read in that impl's scope. */
static const rs_type_t *rs_first_param(rs_walk_t *w, const rs_fn_t *fn, const rs_type_t *self_ty) {
    if (!fn->nparams || !fn->impl)
        return &RS_UNKNOWN_TYPE;
    return rs_impl_assoc_type(w, fn->impl, self_ty, fn->params[0]);
}

/* A trait with type arguments has one impl per argument type: `From<A>` and
 * `From<B>` are chosen by the value passed. */
static bool rs_impl_is_generic_trait(const rs_impl_t *impl) {
    return impl && impl->trait_syn && impl->trait_syn->kind == RS_TS_PATH &&
           impl->trait_syn->nargs > 0;
}

static const rs_fn_t *rs_fn_in(const rs_fn_t *list, const char *name, bool need_self) {
    for (const rs_fn_t *fn = list; fn; fn = fn->next)
        if (!strcmp(fn->name, name) && (!need_self || fn->self_kind != RS_SELF_NONE))
            return fn;
    return NULL;
}

/* Two same-named fns in one scope share one QN (cfg twins, From<A> and From<B>)
 * and are one graph identity. Different QNs are different declarations. */
static bool rs_same_fn(const rs_fn_t *a, const rs_fn_t *b) {
    return a == b || (a->qn && b->qn && !strcmp(a->qn, b->qn));
}

/* Inherent methods first, then trait methods of traits in scope. */
static void rs_pick_on_item(rs_walk_t *w, const rs_item_t *item, const rs_type_t *self_ty,
                            const char *name, bool need_self, rs_pick_t *pick) {
    const rs_fn_t *inherent = NULL;
    const rs_fn_t *trait_fn = NULL;
    bool trait_ambiguous = false, out_of_scope = false, generic_trait = false;
    rs_inst_t *from = w->inst;
    for (const rs_impl_t *impl = item->impls; impl; impl = impl->next_self) {
        if (!impl->trait_syn) {
            const rs_fn_t *fn = rs_fn_in(impl->methods, name, need_self);
            if (!fn)
                continue;
            if (inherent && !rs_same_fn(inherent, fn)) {
                pick->ambiguous = true;
                pick->reason = "rust_inherent_methods_ambiguous";
                pick->stop = true;
                return;
            }
            inherent = fn;
            continue;
        }
        const rs_fn_t *fn = rs_fn_in(impl->methods, name, need_self);
        if (!fn && impl->trait_item)
            fn = rs_fn_in(impl->trait_item->methods, name, need_self);
        if (!fn)
            continue;
        if (impl->trait_item && !rs_trait_in_scope(w, impl->trait_item)) {
            out_of_scope = true;
            continue;
        }
        if (rs_impl_is_generic_trait(impl)) {
            generic_trait = true;
            /* The value passed selects among From<A>, From<B>. */
            if (w->call_arg0 && rs_type_matches(rs_first_param(w, fn, self_ty), w->call_arg0) == 0)
                continue;
        }
        if (trait_fn && !rs_same_fn(trait_fn, fn))
            trait_ambiguous = true;
        trait_fn = fn;
    }
    if (!inherent && generic_trait && item->derives_conversion &&
        (!trait_fn || !w->call_arg0 ||
         rs_type_matches(rs_first_param(w, trait_fn, self_ty), w->call_arg0) != 1)) {
        pick->reason = "rust_conversion_may_be_derived";
        return;
    }
    if (inherent) {
        if (!rs_visible(inherent->vis, inherent->owner, from)) {
            pick->hidden = true;
            pick->stop = true;
            pick->reason = "rust_method_not_visible";
            return;
        }
        pick->fn = inherent;
        pick->self_ty = self_ty;
        pick->reason = "rust_inherent_method";
        return;
    }
    /* The macro may have written an inherent method of this name, which would come first. */
    if (item->macro_declared && (trait_fn || trait_ambiguous)) {
        pick->stop = true;
        pick->reason = "rust_macro_declared_item";
        return;
    }
    if (trait_ambiguous) {
        pick->ambiguous = true;
        pick->stop = true;
        pick->reason = "rust_trait_methods_ambiguous";
        return;
    }
    if (trait_fn) {
        pick->fn = trait_fn;
        pick->self_ty = self_ty;
        pick->reason = "rust_trait_method";
        return;
    }
    if (out_of_scope)
        pick->reason = "rust_trait_not_in_scope";
}

static const rs_fn_t *rs_trait_member(rs_walk_t *w, const rs_item_t *trait, const char *name,
                                      bool need_self, int depth) {
    const rs_fn_t *fn = rs_fn_in(trait->methods, name, need_self);
    if (fn || depth > RS_MAX_DEREF)
        return fn;
    rs_env_t env = rs_env_here(w);
    env.inst = rs_inst_for(trait->owner, w->inst ? w->inst->crate : NULL);
    for (int i = 0; env.inst && i < trait->nsupers; i++) {
        const rs_item_t *super = rs_trait_of_syn(&env, trait->supers[i]);
        fn = super ? rs_trait_member(w, super, name, need_self, depth + 1) : NULL;
        if (fn)
            return fn;
    }
    return NULL;
}

static bool rs_trait_in_scope(rs_walk_t *w, const rs_item_t *trait);
static const char *rs_bound_last_segment(const rs_tsyn_t *syn) {
    return syn && syn->kind == RS_TS_PATH && syn->nseg ? syn->segs[syn->nseg - 1] : NULL;
}

/* `impl<St: Stream> StreamExt for St` serves `s: S` where `S: Stream`, whatever Stream is. Every
 * bound the impl states must be stated on the parameter: nothing else is known about it. */
static bool rs_param_meets_blanket(const rs_type_t *type, const rs_impl_t *impl) {
    const rs_generics_t *generics = impl->generics;
    const char *param =
        impl->self_syn && impl->self_syn->kind == RS_TS_PATH && impl->self_syn->nseg == 1
            ? impl->self_syn->segs[0]
            : NULL;
    bool some_bound = false;
    for (int i = 0; param && generics && i < generics->count; i++) {
        if (strcmp(generics->names[i], param) != 0)
            continue;
        for (int b = 0; b < generics->nbounds[i]; b++) {
            const char *wanted = rs_bound_last_segment(generics->bounds[i][b]);
            bool stated = false;
            for (int have = 0; wanted && !stated && have < type->nbounds; have++) {
                const char *written = rs_bound_last_segment(type->bounds[have]);
                stated = written && !strcmp(written, wanted);
            }
            if (!stated)
                return false;
            some_bound = true;
        }
    }
    return some_bound;
}

static void rs_pick_on_param(rs_walk_t *w, const rs_type_t *type, const char *name, bool need_self,
                             rs_pick_t *pick) {
    const rs_fn_t *found = NULL;
    for (int i = 0; i < type->ntraits; i++) {
        const rs_fn_t *fn = rs_trait_member(w, type->traits[i], name, need_self, 0);
        if (!fn)
            continue;
        if (found && !rs_same_fn(found, fn)) {
            pick->ambiguous = true;
            pick->stop = true;
            pick->reason = "rust_bound_methods_ambiguous";
            return;
        }
        found = fn;
    }
    if (found) {
        pick->fn = found;
        pick->self_ty = type;
        pick->reason = "rust_trait_bound_method";
        return;
    }
    for (const rs_impl_t *impl = w->project->blanket_impls; impl; impl = impl->next_self) {
        const rs_fn_t *fn = rs_fn_in(impl->methods, name, need_self);
        if (!fn && impl->trait_item)
            fn = rs_fn_in(impl->trait_item->methods, name, need_self);
        if (!fn || !impl->trait_item || !rs_trait_in_scope(w, impl->trait_item) ||
            !rs_param_meets_blanket(type, impl))
            continue;
        if (found && !rs_same_fn(found, fn)) {
            pick->ambiguous = true;
            pick->stop = true;
            pick->reason = "rust_trait_methods_ambiguous";
            return;
        }
        found = fn;
    }
    if (found) {
        pick->fn = found;
        pick->self_ty = type;
        pick->reason = "rust_blanket_impl_method";
    }
}

static bool rs_deref_wrapper(const rs_type_t *type) {
    static const char *const wrappers[] = {"std::boxed::Box",
                                           "std::rc::Rc",
                                           "std::sync::Arc",
                                           "std::cell::Ref",
                                           "std::cell::RefMut",
                                           "std::sync::MutexGuard",
                                           "std::sync::RwLockReadGuard",
                                           "std::sync::RwLockWriteGuard",
                                           "std::borrow::Cow",
                                           "std::sync::LazyLock",
                                           "std::cell::LazyCell",
                                           "std::pin::Pin",
                                           "std::mem::ManuallyDrop",
                                           "once_cell::sync::Lazy",
                                           "once_cell::unsync::Lazy",
                                           "actix_web::web::Data",
                                           "actix_web::web::Json",
                                           "actix_web::web::Path",
                                           "actix_web::web::Query",
                                           "actix_web::web::ReqData",
                                           "tokio::sync::MutexGuard",
                                           "tokio::sync::RwLockReadGuard",
                                           "tokio::sync::RwLockWriteGuard",
                                           "parking_lot::MutexGuard",
                                           "parking_lot::RwLockReadGuard",
                                           "parking_lot::RwLockWriteGuard",
                                           NULL};
    for (const char *const *at = wrappers; *at; at++)
        if (rs_ext_is(type, *at))
            return true;
    return false;
}

/* Methods the wrapper answers itself; everything else autoderefs to its content. */
static bool rs_wrapper_own_method(const char *name) {
    static const char *const own[] = {
        "clone",    "as_ref",    "as_mut",  "borrow",     "borrow_mut",  "into",       "try_into",
        "to_owned", "eq",        "ne",      "cmp",        "partial_cmp", "hash",       "fmt",
        "deref",    "deref_mut", "type_id", "into_owned", "to_mut",      "into_inner", NULL};
    for (const char *const *at = own; *at; at++)
        if (!strcmp(name, *at))
            return true;
    return false;
}

static void rs_unify(const rs_tsyn_t *syn, const rs_type_t *type, const rs_generics_t *generics,
                     const rs_type_t **out) {
    if (!syn || !type || !generics)
        return;
    if (syn->kind == RS_TS_REF) {
        rs_unify(syn->inner, type->kind == RT_REF ? type->inner : type, generics, out);
        return;
    }
    if (syn->kind != RS_TS_PATH)
        return;
    if (syn->nseg == 1) {
        for (int i = 0; i < generics->count; i++) {
            if (!strcmp(generics->names[i], syn->segs[0])) {
                if (!out[i])
                    out[i] = type;
                return;
            }
        }
    }
    const rs_type_t *peeled = rs_peel(type);
    for (int i = 0; i < syn->nargs && i < peeled->nargs; i++)
        rs_unify(syn->args[i], peeled->args[i], generics, out);
}

/* What a declaration's generic parameters mean at one use. A parameter no argument
 * fixed is unknown there: an outside caller knows the concrete type exists, not which
 * type it is, so its bounds must not stand in for it. */
typedef struct {
    const char **names;
    const rs_type_t **types;
    int count;
} rs_bindings_t;

static rs_bindings_t rs_bindings_for(rs_walk_t *w, const rs_generics_t *owner,
                                     const rs_generics_t *inner, const rs_tsyn_t *self_syn,
                                     const rs_type_t *self_ty) {
    rs_bindings_t out = {0};
    int owned = owner ? owner->count : 0, own_inner = inner ? inner->count : 0;
    int total = owned + own_inner;
    if (!total)
        return out;
    out.names = cbm_arena_alloc(w->arena, (size_t)total * sizeof(*out.names));
    out.types = cbm_arena_calloc(w->arena, (size_t)total * sizeof(*out.types));
    const rs_type_t **from_self =
        owned ? cbm_arena_calloc(w->arena, (size_t)owned * sizeof(*from_self)) : NULL;
    if (!out.names || !out.types || (owned && !from_self)) {
        w->failed = true;
        out.count = 0;
        return out;
    }
    if (self_syn && self_ty && from_self)
        rs_unify(self_syn, self_ty, owner, from_self);
    /* The innermost declaration shadows, and lookup takes the first match. */
    for (int i = 0; i < own_inner; i++) {
        out.names[out.count] = inner->names[i];
        out.types[out.count++] = &RS_UNKNOWN_TYPE;
    }
    for (int i = 0; i < owned; i++) {
        out.names[out.count] = owner->names[i];
        out.types[out.count++] = from_self && from_self[i] ? from_self[i] : &RS_UNKNOWN_TYPE;
    }
    return out;
}

/* A type named by an impl's own `type X = ...`, with the impl's generic parameters
 * bound from the receiver: `impl<D> Deref for Guard<D> { type Target = D; }`. */
static const rs_type_t *rs_impl_assoc_type(rs_walk_t *w, const rs_impl_t *impl,
                                           const rs_type_t *self_ty, const rs_tsyn_t *syn) {
    rs_bindings_t bound = rs_bindings_for(w, impl->generics, NULL, impl->self_syn, self_ty);
    rs_env_t env = {.q = &w->q,
                    .inst = rs_inst_for(impl->owner, w->inst ? w->inst->crate : NULL),
                    .self_ty = self_ty,
                    .impl = impl,
                    .subst_names = bound.names,
                    .subst_types = bound.types,
                    .nsubst = bound.count};
    return rs_lower(&env, syn);
}

/* A project Deref impl continues the method probe at its Target. */
static const rs_type_t *rs_item_deref(rs_walk_t *w, const rs_type_t *self_ty) {
    const rs_item_t *item = self_ty->item;
    for (const rs_impl_t *impl = item->impls; impl; impl = impl->next_self) {
        if (!impl->trait_syn || impl->trait_syn->kind != RS_TS_PATH || !impl->trait_syn->nseg ||
            strcmp(impl->trait_syn->segs[impl->trait_syn->nseg - 1], "Deref") != 0)
            continue;
        for (int i = 0; i < impl->nassoc; i++) {
            if (strcmp(impl->assoc_names[i], "Target") != 0)
                continue;
            return rs_impl_assoc_type(w, impl, self_ty, impl->assoc_types[i]);
        }
    }
    return NULL;
}

static void rs_pick_ext_impls(rs_walk_t *w, const rs_type_t *type, const char *name, bool need_self,
                              rs_pick_t *pick) {
    const char *path = type->name;
    if (!path)
        return;
    const rs_fn_t *found = NULL;
    const cbm_rust_project_t *project = w->project;
    if (!project->ext_impls)
        return;
    /* A crate's Result<T> alias is std's Result, so impls on Result serve it. */
    static const char std_result[] = "std::result::Result";
    const char *const owners[] = {
        path, rs_ext_is_result(type) && strcmp(path, std_result) ? std_result : NULL};
    /* Only those declaring this method: one without it has nothing to say about this call. */
    bool chained = project->ext_by_row != NULL;
    for (size_t round = 0; round < 2 && owners[round]; round++) {
        int row = chained
                      ? cbm_name_rows_lookup(&project->ext_rows, rs_ext_scope(owners[round]), name)
                      : -1;
        const rs_impl_t *impl = !chained   ? cbm_ht_get(project->ext_impls, owners[round])
                                : row >= 0 ? project->ext_by_row[row]
                                           : NULL;
        while (impl) {
            const rs_impl_t *listed = impl;
            if (chained) {
                row = project->ext_rows.next[row];
                impl = row >= 0 ? project->ext_by_row[row] : NULL;
            } else {
                impl = impl->next_self;
            }
            cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
            /* Two paths may share one scope. */
            if (strcmp(listed->ext_path, owners[round]) != 0)
                continue;
            const rs_fn_t *fn = rs_fn_in(listed->methods, name, need_self);
            if (!fn && listed->trait_item)
                fn = rs_fn_in(listed->trait_item->methods, name, need_self);
            if (!fn || (listed->trait_item && !rs_trait_in_scope(w, listed->trait_item)))
                continue;
            /* This impl's own body shows the external type has an inherent method of this name. */
            if (fn->delegates_to_own_name) {
                pick->stop = true;
                pick->reason = "rust_external_receiver_method";
                return;
            }
            if (found && !rs_same_fn(found, fn)) {
                pick->ambiguous = true;
                pick->stop = true;
                pick->reason = "rust_trait_methods_ambiguous";
                return;
            }
            found = fn;
        }
    }
    if (found) {
        pick->fn = found;
        pick->self_ty = type;
        pick->reason = "rust_trait_method_on_external_type";
    }
}

static bool rs_name_in(const char *name, const char *const *names);

/* Bounds a type can meet with no impl written for it in the project: auto traits, derives, and
 * traits their own crate implements blanket-wide. */
static bool rs_bound_needs_no_impl(const char *name) {
    static const char *const names[] = {
        "Send",         "Sync",    "Sized",     "Unpin",       "Copy",    "Clone",
        "Debug",        "Default", "Eq",        "PartialEq",   "Ord",     "PartialOrd",
        "Hash",         "Any",     "From",      "Into",        "TryFrom", "TryInto",
        "AsRef",        "AsMut",   "Borrow",    "BorrowMut",   "ToOwned", "ToString",
        "IntoIterator", "Display", "Serialize", "Deserialize", NULL};
    return rs_name_in(name, names);
}

/* `impl<T: Bound> Tr for T` serves any type meeting Bound. A project bound is checked against the
 * type's impls. So is an external one when the type is the project's: the orphan rule puts every
 * impl for it in its own crate, unless the trait's crate implements it blanket-wide. */
static bool rs_item_meets_bounds(rs_walk_t *w, const rs_item_t *item, const rs_impl_t *impl) {
    if (item->macro_declared)
        return false;
    const rs_generics_t *generics = impl->generics;
    const char *param =
        impl->self_syn && impl->self_syn->kind == RS_TS_PATH && impl->self_syn->nseg == 1
            ? impl->self_syn->segs[0]
            : NULL;
    rs_env_t env = rs_env_here(w);
    env.inst = rs_inst_for(impl->owner, w->inst ? w->inst->crate : NULL);
    env.blocks = NULL;
    env.nblocks = 0;
    for (int i = 0; param && generics && i < generics->count; i++) {
        if (strcmp(generics->names[i], param) != 0)
            continue;
        for (int b = 0; b < generics->nbounds[i]; b++) {
            const rs_tsyn_t *syn = generics->bounds[i][b];
            const rs_item_t *bound = rs_trait_of_syn(&env, syn);
            const char *spelled = !bound && syn && syn->kind == RS_TS_PATH && syn->nseg
                                      ? syn->segs[syn->nseg - 1]
                                      : NULL;
            if (!bound && (!spelled || rs_bound_needs_no_impl(spelled)))
                continue;
            bool met = false;
            for (const rs_impl_t *have = item->impls; have && !met; have = have->next_self) {
                const rs_tsyn_t *written = have->trait_syn;
                met = bound ? have->trait_item == bound
                            : written && written->kind == RS_TS_PATH && written->nseg &&
                                  !strcmp(written->segs[written->nseg - 1], spelled);
            }
            if (!met)
                return false;
        }
    }
    return true;
}

static void rs_pick_blanket(rs_walk_t *w, const rs_item_t *item, const rs_type_t *self_ty,
                            const char *name, bool need_self, rs_pick_t *pick) {
    const rs_fn_t *found = NULL;
    for (const rs_impl_t *impl = w->project->blanket_impls; impl; impl = impl->next_self) {
        const rs_fn_t *fn = rs_fn_in(impl->methods, name, need_self);
        if (!fn && impl->trait_item)
            fn = rs_fn_in(impl->trait_item->methods, name, need_self);
        if (!fn || !impl->trait_item || !rs_trait_in_scope(w, impl->trait_item) ||
            !rs_item_meets_bounds(w, item, impl))
            continue;
        if (found && !rs_same_fn(found, fn)) {
            pick->ambiguous = true;
            pick->stop = true;
            pick->reason = "rust_trait_methods_ambiguous";
            return;
        }
        found = fn;
    }
    if (found) {
        pick->fn = found;
        pick->self_ty = self_ty;
        pick->reason = "rust_blanket_impl_method";
    }
}

/* Autoderef probe for `value.name()`. */
static rs_pick_t rs_find_method(rs_walk_t *w, const rs_type_t *receiver, const char *name) {
    rs_pick_t pick = {.reason = "rust_receiver_type_unknown"};
    const rs_type_t *type = receiver;
    for (int step = 0; type && step < RS_MAX_DEREF; step++) {
        type = rs_peel(type);
        switch (type->kind) {
        case RT_ITEM: {
            rs_pick_on_item(w, type->item, type, name, true, &pick);
            if (!pick.fn && !pick.stop)
                rs_pick_blanket(w, type->item, type, name, true, &pick);
            if (pick.fn || pick.stop)
                return pick;
            const rs_type_t *target = rs_item_deref(w, type);
            if (!target) {
                if (!pick.reason || strcmp(pick.reason, "rust_trait_not_in_scope"))
                    pick.reason = "rust_method_not_in_project";
                return pick;
            }
            type = target;
            continue;
        }
        case RT_PARAM:
            rs_pick_on_param(w, type, name, true, &pick);
            if (!pick.fn && !pick.stop)
                pick.reason = "rust_method_not_in_known_bounds";
            return pick;
        case RT_EXT:
            /* `self: Pin<&mut Self>` makes a Pin<&mut W> receiver mean Self = W at the first
             * probing step. `impl<P> Tr for Pin<P>` would need Pin<&mut Pin<P>>. */
            if (!rs_ext_is(type, "std::pin::Pin"))
                rs_pick_ext_impls(w, type, name, true, &pick);
            if (pick.fn || pick.stop)
                return pick;
            if (rs_deref_wrapper(type) && type->nargs && !rs_wrapper_own_method(name)) {
                type = type->args[0];
                continue;
            }
            pick.reason = "rust_external_receiver_method";
            return pick;
        case RT_PRIM:
            rs_pick_ext_impls(w, type, name, true, &pick);
            if (!pick.fn && !pick.stop)
                pick.reason = "rust_external_receiver_method";
            return pick;
        case RT_TUPLE:
        case RT_ARRAY:
        case RT_UNIT:
        case RT_ITER:
            pick.reason = "rust_external_receiver_method";
            return pick;
        case RT_FUTURE:
            pick.reason = "rust_future_receiver_method";
            return pick;
        default:
            pick.reason = "rust_receiver_type_unknown";
            return pick;
        }
    }
    return pick;
}

/* `Type::name` lives on that type itself: no autoderef, and fns without self count. */
static rs_pick_t rs_find_assoc(rs_walk_t *w, const rs_type_t *type, const char *name) {
    rs_pick_t pick = {.reason = "rust_assoc_item_not_in_project"};
    type = rs_peel(type);
    if (type->kind == RT_ITEM)
        rs_pick_on_item(w, type->item, type, name, false, &pick);
    else if (type->kind == RT_PARAM)
        rs_pick_on_param(w, type, name, false, &pick);
    else if (type->kind == RT_EXT || type->kind == RT_PRIM)
        rs_pick_ext_impls(w, type, name, false, &pick);
    else if (type->kind == RT_UNKNOWN)
        pick.reason = "rust_receiver_type_unknown";
    return pick;
}

typedef struct {
    const rs_field_t *field;
    const rs_type_t *owner;
} rs_field_pick_t;

static rs_field_pick_t rs_find_field(rs_walk_t *w, const rs_type_t *receiver, const char *name) {
    const rs_type_t *type = receiver;
    for (int step = 0; type && step < RS_MAX_DEREF; step++) {
        type = rs_peel(type);
        if (type->kind == RT_ITEM) {
            for (const rs_field_t *field = type->item->fields; field; field = field->next) {
                if (!strcmp(field->name, name) &&
                    rs_visible(field->vis, type->item->owner, w->inst))
                    return (rs_field_pick_t){.field = field, .owner = type};
                if (!strcmp(field->name, name) && type->item->tuple_struct)
                    return (rs_field_pick_t){.field = field, .owner = type};
            }
            type = rs_item_deref(w, type);
            continue;
        }
        if (type->kind == RT_EXT && rs_deref_wrapper(type) && type->nargs) {
            type = type->args[0];
            continue;
        }
        break;
    }
    return (rs_field_pick_t){0};
}

/* The declared type of a member, read in its declaring module with this
 * receiver's generic arguments substituted. */
static const rs_type_t *rs_field_type(rs_walk_t *w, const rs_field_pick_t *pick) {
    const rs_item_t *item = pick->owner->item;
    int count = item->generics ? item->generics->count : 0;
    const rs_type_t **types =
        count ? cbm_arena_calloc(w->arena, (size_t)count * sizeof(*types)) : NULL;
    for (int i = 0; types && i < count; i++)
        types[i] = i < pick->owner->nargs && pick->owner->args[i] ? pick->owner->args[i]
                                                                  : &RS_UNKNOWN_TYPE;
    rs_env_t env = {.q = &w->q,
                    .inst = rs_inst_for(item->owner, w->inst ? w->inst->crate : NULL),
                    .self_ty = pick->owner,
                    .subst_names = count ? (const char *const *)item->generics->names : NULL,
                    .subst_types = types,
                    .nsubst = types ? count : 0};
    const rs_type_t *declared = rs_lower(&env, pick->field->type);
    return pick->owner->projected && pick->field->pinned
               ? rs_ext_type(w->arena, "std::pin::Pin", declared, NULL)
               : declared;
}

/* pin-project generates project() and project_ref() on a struct with a #[pin] field. No source
 * declares them, so the call has no target, but what they return reads the struct's own fields. */
static const rs_type_t *rs_pin_projection(rs_walk_t *w, const rs_type_t *receiver,
                                          const char *name) {
    if (strcmp(name, "project") != 0 && strcmp(name, "project_ref") != 0)
        return NULL;
    const rs_type_t *type = receiver;
    for (int step = 0; type && step < RS_MAX_DEREF; step++) {
        type = rs_peel(type);
        if (type->kind == RT_ITEM) {
            if (!type->item->pin_projected)
                return NULL;
            rs_type_t *projected = rs_type_new(w->arena, RT_ITEM);
            if (!projected)
                return NULL;
            *projected = *type;
            projected->projected = true;
            return projected;
        }
        if (type->kind != RT_EXT || !rs_deref_wrapper(type) || !type->nargs)
            return NULL;
        type = type->args[0];
    }
    return NULL;
}

/* The values passed fix generic parameters the receiver left open:
 * `fn new(inner: T) -> Self` called with an A gives Wrapper<A>. */
static void rs_bind_from_args(const rs_fn_t *fn, const rs_generics_t *owner,
                              const rs_type_t **types, int nargs, int offset,
                              rs_bindings_t *bound) {
    int inner = fn->generics ? fn->generics->count : 0;
    for (int i = 0; i < fn->nparams && i + offset < nargs; i++) {
        const rs_type_t *actual = types[i + offset];
        if (!actual || actual->kind == RT_UNKNOWN)
            continue;
        const rs_type_t *found[RS_MAX_GENERIC_BINDINGS] = {NULL};
        if (fn->generics && inner <= RS_MAX_GENERIC_BINDINGS) {
            rs_unify(fn->params[i], actual, fn->generics, found);
            for (int g = 0; g < inner; g++)
                if (found[g] && bound->types[g]->kind == RT_UNKNOWN)
                    bound->types[g] = found[g];
        }
        int owned = owner ? owner->count : 0;
        const rs_type_t *found_owner[RS_MAX_GENERIC_BINDINGS] = {NULL};
        if (owner && owned <= RS_MAX_GENERIC_BINDINGS) {
            rs_unify(fn->params[i], actual, owner, found_owner);
            for (int g = 0; g < owned; g++)
                if (found_owner[g] && bound->types[inner + g]->kind == RT_UNKNOWN)
                    bound->types[inner + g] = found_owner[g];
        }
    }
}

static const rs_type_t *rs_fn_return_for(rs_walk_t *w, const rs_fn_t *fn, const rs_type_t *self_ty,
                                         const rs_type_t **types, int nargs, int offset) {
    if (!fn)
        return &RS_UNKNOWN_TYPE;
    /* Twins share a node, not a signature: only `Self` is the same across them, and whatever
     * every one of them writes alike. */
    if (fn->qn_twin && !fn->twin_return_agrees && fn->ret &&
        !(fn->ret->kind == RS_TS_PATH && fn->ret->nseg == 1 && !strcmp(fn->ret->segs[0], "Self")))
        return &RS_UNKNOWN_TYPE;
    const rs_impl_t *impl = fn->impl;
    const rs_generics_t *owner = impl ? impl->generics : (fn->trait ? fn->trait->generics : NULL);
    rs_bindings_t bound =
        rs_bindings_for(w, owner, fn->generics, impl ? impl->self_syn : NULL, self_ty);
    if (types && bound.count)
        rs_bind_from_args(fn, owner, types, nargs, offset, &bound);
    rs_env_t env = {.q = &w->q,
                    .inst = rs_inst_for(fn->owner, w->inst ? w->inst->crate : NULL),
                    .self_ty = self_ty,
                    .impl = impl,
                    .subst_names = bound.names,
                    .subst_types = bound.types,
                    .nsubst = bound.count};
    /* `Self` in a generic impl carries what the arguments just fixed. */
    if (impl && self_ty && rs_peel(self_ty)->kind == RT_ITEM && !rs_peel(self_ty)->nargs && owner &&
        owner->count)
        env.self_ty = rs_lower(&env, impl->self_syn);
    const rs_type_t *result = fn->ret ? rs_lower(&env, fn->ret) : &RS_UNIT_TYPE;
    if (fn->is_async) {
        rs_type_t *future = rs_type_new(w->arena, RT_FUTURE);
        if (future) {
            future->inner = result;
            return future;
        }
    }
    return result;
}

/* ── Standard library signatures ───────────────────────────────────── */

static bool rs_name_in(const char *name, const char *const *names) {
    for (; *names; names++)
        if (!strcmp(name, *names))
            return true;
    return false;
}

static const rs_type_t *rs_option_of(CBMArena *arena, const rs_type_t *inner) {
    return rs_ext_type(arena, "std::option::Option", inner, NULL);
}

static const rs_type_t *rs_result_of(CBMArena *arena, const rs_type_t *ok, const rs_type_t *err) {
    return rs_ext_type(arena, "std::result::Result", ok, err ? err : &RS_UNKNOWN_TYPE);
}

/* Containers whose signatures for the methods read here are std's: FxHashMap is an
 * alias of std's type, and hashbrown, indexmap and bumpalo mirror that API. */
static bool rs_ext_in(const rs_type_t *type, const char *const *paths) {
    if (!type || type->kind != RT_EXT || !type->name)
        return false;
    return rs_name_in(type->name, paths);
}

static bool rs_is_map(const rs_type_t *type) {
    static const char *const maps[] = {"std::collections::HashMap",
                                       "std::collections::hash_map::HashMap",
                                       "std::collections::BTreeMap",
                                       "std::collections::btree_map::BTreeMap",
                                       "hashbrown::HashMap",
                                       "hashbrown::hash_map::HashMap",
                                       "indexmap::IndexMap",
                                       "indexmap::map::IndexMap",
                                       "fxhash::FxHashMap",
                                       "rustc_hash::FxHashMap",
                                       NULL};
    return rs_ext_in(type, maps);
}

static bool rs_is_set(const rs_type_t *type) {
    static const char *const sets[] = {"std::collections::HashSet",
                                       "std::collections::hash_set::HashSet",
                                       "std::collections::BTreeSet",
                                       "std::collections::btree_set::BTreeSet",
                                       "hashbrown::HashSet",
                                       "hashbrown::hash_set::HashSet",
                                       "indexmap::IndexSet",
                                       "indexmap::set::IndexSet",
                                       "fxhash::FxHashSet",
                                       "rustc_hash::FxHashSet",
                                       NULL};
    return rs_ext_in(type, sets);
}

static bool rs_is_seq(const rs_type_t *type) {
    static const char *const seqs[] = {"std::vec::Vec", "std::collections::VecDeque",
                                       "bumpalo::collections::Vec",
                                       "bumpalo::collections::vec::Vec", NULL};
    return type->kind == RT_ARRAY || rs_ext_in(type, seqs);
}

static const rs_type_t *rs_seq_element(const rs_type_t *type) {
    return type->kind == RT_ARRAY ? (type->inner ? type->inner : &RS_UNKNOWN_TYPE)
                                  : rs_arg(type, 0);
}

/* FromIterator: `_` in the spelled collection is the iterator's element. A map takes
 * (K, V); Result<C, E> and Option<C> take an item of that wrapper and collect C. */
static const rs_type_t *rs_collect_into(CBMArena *arena, const rs_type_t *into,
                                        const rs_type_t *element, int depth) {
    if (!into || !element || into->kind != RT_EXT || element->kind == RT_UNKNOWN || depth > 2)
        return into;
    if (rs_is_seq(into) || rs_is_set(into))
        return rs_arg(into, 0)->kind == RT_UNKNOWN ? rs_ext_type(arena, into->name, element, NULL)
                                                   : into;
    if (rs_is_map(into) && element->kind == RT_TUPLE && element->nargs == 2) {
        const rs_type_t *key = rs_arg(into, 0), *value = rs_arg(into, 1);
        return rs_ext_type(arena, into->name, key->kind == RT_UNKNOWN ? element->args[0] : key,
                           value->kind == RT_UNKNOWN ? element->args[1] : value);
    }
    bool option =
        rs_ext_is(into, "std::option::Option") && rs_ext_is(element, "std::option::Option");
    if (option || (rs_ext_is_result(into) && rs_ext_is_result(element))) {
        const rs_type_t *inner =
            rs_collect_into(arena, rs_arg(into, 0), rs_arg(element, 0), depth + 1);
        return rs_ext_type(arena, into->name, inner, into->nargs > 1 ? into->args[1] : NULL);
    }
    return into;
}

/* Parameter types the callee gives a closure passed to it. */
static int rs_closure_params_for(rs_walk_t *w, const rs_type_t *receiver, const char *name,
                                 const rs_type_t **out) {
    const rs_type_t *type = rs_peel(receiver);
    CBMArena *arena = w->arena;
    static const char *const by_value[] = {
        "map",       "and_then", "for_each",    "filter_map", "flat_map",    "any",          "all",
        "map_while", "find_map", "is_some_and", "map_or",     "map_or_else", "try_for_each", NULL};
    static const char *const by_ref[] = {"filter",  "find",       "take_while", "skip_while",
                                         "inspect", "is_none_or", "partition",  NULL};
    if (rs_ext_is(type, "std::option::Option")) {
        if (rs_name_in(name, by_value)) {
            out[0] = rs_arg(type, 0);
            return 1;
        }
        if (rs_name_in(name, by_ref)) {
            out[0] = rs_ref_of(arena, rs_arg(type, 0));
            return 1;
        }
        return 0;
    }
    if (rs_ext_is_result(type)) {
        if (!strcmp(name, "map") || !strcmp(name, "and_then") || !strcmp(name, "is_ok_and")) {
            out[0] = rs_arg(type, 0);
            return 1;
        }
        if (!strcmp(name, "map_err") || !strcmp(name, "or_else") ||
            !strcmp(name, "unwrap_or_else")) {
            out[0] = rs_arg(type, 1);
            return 1;
        }
        return 0;
    }
    if (type->kind == RT_ITER) {
        static const char *const by_ref_key[] = {"max_by_key", "min_by_key", NULL};
        static const char *const two_refs[] = {"max_by", "min_by", NULL};
        if (rs_name_in(name, by_value) || !strcmp(name, "position")) {
            out[0] = type->inner;
            return 1;
        }
        if (rs_name_in(name, by_ref) || rs_name_in(name, by_ref_key)) {
            out[0] = rs_ref_of(arena, type->inner);
            return 1;
        }
        if (rs_name_in(name, two_refs)) {
            out[0] = out[1] = rs_ref_of(arena, type->inner);
            return 2;
        }
        if (!strcmp(name, "fold") || !strcmp(name, "try_fold")) {
            out[0] = &RS_UNKNOWN_TYPE;
            out[1] = type->inner;
            return 2;
        }
        if (!strcmp(name, "reduce")) {
            out[0] = out[1] = type->inner;
            return 2;
        }
        return 0;
    }
    if (rs_is_seq(type)) {
        static const char *const element_ref[] = {
            "sort_by_key",      "sort_by_cached_key", "sort_unstable_by_key", "retain",
            "binary_search_by", "partition_point",    "binary_search_by_key", NULL};
        static const char *const two_refs[] = {"sort_by", "sort_unstable_by", NULL};
        static const char *const element_mut[] = {"retain_mut", "dedup_by_key", NULL};
        const rs_type_t *reference = rs_ref_of(arena, rs_seq_element(type));
        if (rs_name_in(name, element_ref) || rs_name_in(name, element_mut)) {
            out[0] = reference;
            return 1;
        }
        if (rs_name_in(name, two_refs) || !strcmp(name, "dedup_by")) {
            out[0] = out[1] = reference;
            return 2;
        }
    }
    return 0;
}

#define RS_MAP_ENTRY "std::collections::hash_map::Entry"
#define RS_MAP_OCCUPIED "std::collections::hash_map::OccupiedEntry"
#define RS_MAP_VACANT "std::collections::hash_map::VacantEntry"

static const rs_type_t *rs_pair_of(CBMArena *arena, const rs_type_t *a, const rs_type_t *b) {
    rs_type_t *pair = rs_type_new(arena, RT_TUPLE);
    if (!pair)
        return &RS_UNKNOWN_TYPE;
    pair->args = cbm_arena_alloc(arena, 2 * sizeof(*pair->args));
    if (pair->args) {
        pair->args[0] = a;
        pair->args[1] = b;
        pair->nargs = 2;
    }
    return pair;
}

static const rs_type_t *rs_element_of(rs_walk_t *w, const rs_type_t *iterable);

static const rs_type_t *rs_iter_method(rs_walk_t *w, const rs_type_t *type, const char *name,
                                       const rs_type_t *closure_ret) {
    CBMArena *arena = w->arena;
    const rs_type_t *element = type->inner;
    static const char *const same[] = {"filter",   "rev",        "skip",       "take",    "chain",
                                       "peekable", "take_while", "skip_while", "inspect", "fuse",
                                       "step_by",  "by_ref",     "into_iter",  "iter",    NULL};
    if (rs_name_in(name, same))
        return type;
    static const char *const option_element[] = {
        "next",  "last",       "nth",        "min",    "max",    "find",   "next_back", "peek",
        "rfind", "min_by_key", "max_by_key", "min_by", "max_by", "reduce", NULL};
    if (rs_name_in(name, option_element))
        return rs_option_of(arena, element);
    if (!strcmp(name, "map"))
        return rs_iter_of(arena, closure_ret);
    if (!strcmp(name, "flat_map")) {
        const rs_type_t *inner = closure_ret ? rs_peel(closure_ret) : &RS_UNKNOWN_TYPE;
        if (inner->kind == RT_ITER)
            return inner;
        return rs_iter_of(arena, rs_is_seq(inner) ? rs_seq_element(inner) : &RS_UNKNOWN_TYPE);
    }
    if (!strcmp(name, "filter_map") || !strcmp(name, "map_while"))
        return rs_iter_of(arena,
                          closure_ret && rs_ext_is(rs_peel(closure_ret), "std::option::Option")
                              ? rs_arg(rs_peel(closure_ret), 0)
                              : &RS_UNKNOWN_TYPE);
    if (!strcmp(name, "cloned") || !strcmp(name, "copied"))
        return rs_iter_of(arena, rs_peel(element));
    if (!strcmp(name, "enumerate"))
        return rs_iter_of(arena, rs_pair_of(arena, &RS_UNKNOWN_TYPE, element));
    if (!strcmp(name, "flatten"))
        return rs_iter_of(arena, rs_element_of(w, element));
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_option_method(rs_walk_t *w, const rs_type_t *type, const char *name,
                                         const rs_type_t *closure_ret) {
    CBMArena *arena = w->arena;
    const rs_type_t *inner = rs_arg(type, 0);
    static const char *const unwrapping[] = {
        "unwrap",           "expect", "unwrap_or", "unwrap_or_else", "unwrap_or_default",
        "unwrap_unchecked", NULL};
    if (rs_name_in(name, unwrapping))
        return inner;
    static const char *const same[] = {"clone",  "take",    "or",  "or_else",
                                       "filter", "replace", "xor", NULL};
    if (rs_name_in(name, same))
        return type;
    if (!strcmp(name, "as_ref") || !strcmp(name, "as_mut") || !strcmp(name, "as_deref") ||
        !strcmp(name, "as_deref_mut"))
        return rs_option_of(arena, rs_ref_of(arena, inner));
    if (!strcmp(name, "cloned") || !strcmp(name, "copied"))
        return rs_option_of(arena, rs_peel(inner));
    if (!strcmp(name, "map"))
        return rs_option_of(arena, closure_ret ? closure_ret : &RS_UNKNOWN_TYPE);
    if (!strcmp(name, "and_then"))
        return closure_ret ? closure_ret : &RS_UNKNOWN_TYPE;
    if (!strcmp(name, "ok_or") || !strcmp(name, "ok_or_else"))
        return rs_result_of(arena, inner, NULL);
    if (!strcmp(name, "iter") || !strcmp(name, "iter_mut"))
        return rs_iter_of(arena, rs_ref_of(arena, inner));
    if (!strcmp(name, "into_iter"))
        return rs_iter_of(arena, inner);
    if (!strcmp(name, "get_or_insert_with") || !strcmp(name, "insert") ||
        !strcmp(name, "get_or_insert"))
        return rs_ref_of(arena, inner);
    if (!strcmp(name, "flatten"))
        return rs_ext_is(rs_peel(inner), "std::option::Option") ? rs_peel(inner) : &RS_UNKNOWN_TYPE;
    /* Option<Result<T, E>> becomes Result<Option<T>, E>. */
    if (!strcmp(name, "transpose") && rs_ext_is_result(rs_peel(inner)))
        return rs_result_of(arena, rs_option_of(arena, rs_arg(rs_peel(inner), 0)),
                            rs_arg(rs_peel(inner), 1));
    if ((!strcmp(name, "map_or") || !strcmp(name, "map_or_else")) && closure_ret)
        return closure_ret;
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_result_method(rs_walk_t *w, const rs_type_t *type, const char *name,
                                         const rs_type_t *closure_ret) {
    CBMArena *arena = w->arena;
    const rs_type_t *ok = rs_arg(type, 0), *err = rs_arg(type, 1);
    static const char *const unwrapping[] = {
        "unwrap", "expect", "unwrap_or", "unwrap_or_else", "unwrap_or_default", NULL};
    if (rs_name_in(name, unwrapping))
        return ok;
    if (!strcmp(name, "unwrap_err") || !strcmp(name, "expect_err"))
        return err;
    if (!strcmp(name, "ok"))
        return rs_option_of(arena, ok);
    if (!strcmp(name, "err"))
        return rs_option_of(arena, err);
    if (!strcmp(name, "as_ref") || !strcmp(name, "as_mut"))
        return rs_result_of(arena, rs_ref_of(arena, ok), rs_ref_of(arena, err));
    if (!strcmp(name, "map_err") || !strcmp(name, "or_else"))
        return rs_result_of(arena, ok, NULL);
    if (!strcmp(name, "map"))
        return rs_result_of(arena, closure_ret ? closure_ret : &RS_UNKNOWN_TYPE, err);
    if (!strcmp(name, "and_then"))
        return closure_ret ? closure_ret : &RS_UNKNOWN_TYPE;
    if (!strcmp(name, "clone"))
        return type;
    /* Result<Option<T>, E> becomes Option<Result<T, E>>. */
    if (!strcmp(name, "transpose") && rs_ext_is(rs_peel(ok), "std::option::Option"))
        return rs_option_of(arena, rs_result_of(arena, rs_arg(rs_peel(ok), 0), err));
    if ((!strcmp(name, "map_or") || !strcmp(name, "map_or_else")) && closure_ret)
        return closure_ret;
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_seq_method(rs_walk_t *w, const rs_type_t *type, const char *name) {
    CBMArena *arena = w->arena;
    const rs_type_t *element = rs_seq_element(type);
    if (!strcmp(name, "iter") || !strcmp(name, "iter_mut") || !strcmp(name, "par_iter") ||
        !strcmp(name, "par_iter_mut"))
        return rs_iter_of(arena, rs_ref_of(arena, element));
    if (!strcmp(name, "into_iter") || !strcmp(name, "drain") || !strcmp(name, "into_par_iter"))
        return rs_iter_of(arena, element);
    static const char *const by_ref[] = {"first", "last", "first_mut", "last_mut", "get", "get_mut",
                                         "front", "back", "front_mut", "back_mut", NULL};
    if (rs_name_in(name, by_ref))
        return rs_option_of(arena, rs_ref_of(arena, element));
    static const char *const by_value[] = {"pop", "pop_front", "pop_back", NULL};
    if (rs_name_in(name, by_value))
        return rs_option_of(arena, element);
    if (!strcmp(name, "remove") || !strcmp(name, "swap_remove"))
        return element;
    if (!strcmp(name, "clone") || !strcmp(name, "to_vec") || !strcmp(name, "to_owned"))
        return type->kind == RT_ARRAY ? rs_ext_type(arena, "std::vec::Vec", element, NULL) : type;
    if (!strcmp(name, "split_off"))
        return type;
    static const char *const slices[] = {"as_slice", "as_mut_slice", NULL};
    static const char *const slice_runs[] = {
        "windows", "chunks", "chunks_exact", "chunks_mut", "rchunks", "split", NULL};
    if (rs_name_in(name, slices) || rs_name_in(name, slice_runs)) {
        rs_type_t *slice = rs_type_new(arena, RT_ARRAY);
        if (!slice)
            return &RS_UNKNOWN_TYPE;
        slice->inner = element;
        const rs_type_t *reference = rs_ref_of(arena, slice);
        return rs_name_in(name, slices) ? reference : rs_iter_of(arena, reference);
    }
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_map_method(rs_walk_t *w, const rs_type_t *type, const char *name) {
    CBMArena *arena = w->arena;
    const rs_type_t *key = rs_arg(type, 0), *value = rs_arg(type, 1);
    if (!strcmp(name, "get") || !strcmp(name, "get_mut"))
        return rs_option_of(arena, rs_ref_of(arena, value));
    if (!strcmp(name, "remove") || !strcmp(name, "insert"))
        return rs_option_of(arena, value);
    if (!strcmp(name, "values") || !strcmp(name, "values_mut"))
        return rs_iter_of(arena, rs_ref_of(arena, value));
    if (!strcmp(name, "into_values"))
        return rs_iter_of(arena, value);
    if (!strcmp(name, "keys"))
        return rs_iter_of(arena, rs_ref_of(arena, key));
    if (!strcmp(name, "into_keys"))
        return rs_iter_of(arena, key);
    if (!strcmp(name, "clone"))
        return type;
    const rs_type_t *ref_pair = rs_pair_of(arena, rs_ref_of(arena, key), rs_ref_of(arena, value));
    if (!strcmp(name, "iter") || !strcmp(name, "iter_mut"))
        return rs_iter_of(arena, ref_pair);
    if (!strcmp(name, "into_iter") || !strcmp(name, "drain"))
        return rs_iter_of(arena, rs_pair_of(arena, key, value));
    if (!strcmp(name, "entry"))
        return rs_ext_type(arena, RS_MAP_ENTRY, key, value);
    static const char *const ref_pairs[] = {"first_key_value", "last_key_value", "get_key_value",
                                            NULL};
    if (rs_name_in(name, ref_pairs))
        return rs_option_of(arena, ref_pair);
    static const char *const owned_pairs[] = {"pop_first", "pop_last", "remove_entry", NULL};
    if (rs_name_in(name, owned_pairs))
        return rs_option_of(arena, rs_pair_of(arena, key, value));
    return &RS_UNKNOWN_TYPE;
}

/* The map entry API, whichever map handed it out. */
static const rs_type_t *rs_entry_method(CBMArena *arena, const rs_type_t *type, const char *name) {
    const rs_type_t *key = rs_arg(type, 0), *value = rs_arg(type, 1);
    if (!strcmp(name, "key"))
        return rs_ref_of(arena, key);
    if (rs_ext_is(type, RS_MAP_ENTRY)) {
        static const char *const inserting[] = {"or_default", "or_insert", "or_insert_with",
                                                "or_insert_with_key", NULL};
        if (rs_name_in(name, inserting))
            return rs_ref_of(arena, value);
        return !strcmp(name, "and_modify") ? type : &RS_UNKNOWN_TYPE;
    }
    if (rs_ext_is(type, RS_MAP_VACANT))
        return !strcmp(name, "insert") ? rs_ref_of(arena, value) : &RS_UNKNOWN_TYPE;
    static const char *const borrowing[] = {"get", "get_mut", "into_mut", NULL};
    if (rs_name_in(name, borrowing))
        return rs_ref_of(arena, value);
    return !strcmp(name, "insert") || !strcmp(name, "remove") ? value : &RS_UNKNOWN_TYPE;
}

static bool rs_anyhow_context_in_scope(rs_walk_t *w) {
    rs_ent_t ent = rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, "Context", RS_NS_TYPE);
    return ent.kind == RS_ENT_EXTERNAL && ent.external_path &&
           !strcmp(ent.external_path, "anyhow::Context");
}

/* Return type of a standard method on a known receiver; the probe already
 * found nothing on the project side. */
static const rs_type_t *rs_ext_method_ret(rs_walk_t *w, const rs_type_t *receiver, const char *name,
                                          const rs_type_t *closure_ret) {
    CBMArena *arena = w->arena;
    const rs_type_t *type = receiver;
    for (int step = 0; type && step < RS_MAX_DEREF; step++) {
        type = rs_peel(type);
        if (type->kind == RT_ITER)
            return rs_iter_method(w, type, name, closure_ret);
        /* anyhow::Context turns Option<T> and Result<T, E> into anyhow::Result<T>. */
        if ((!strcmp(name, "context") || !strcmp(name, "with_context")) &&
            (rs_ext_is(type, "std::option::Option") || rs_ext_is_result(type)) &&
            rs_anyhow_context_in_scope(w))
            return rs_result_of(arena, rs_arg(type, 0),
                                rs_ext_type(arena, "anyhow::Error", NULL, NULL));
        if (rs_ext_is(type, "std::ptr::NonNull")) {
            if (!strcmp(name, "as_ref") || !strcmp(name, "as_mut"))
                return rs_ref_of(arena, rs_arg(type, 0));
            if (!strcmp(name, "as_ptr"))
                return rs_ext_type(arena, RS_RAW_POINTER, rs_arg(type, 0), NULL);
            return &RS_UNKNOWN_TYPE;
        }
        if (rs_ext_is(type, "std::option::Option"))
            return rs_option_method(w, type, name, closure_ret);
        if (rs_ext_is_result(type))
            return rs_result_method(w, type, name, closure_ret);
        if (rs_is_seq(type))
            return rs_seq_method(w, type, name);
        if (rs_is_map(type))
            return rs_map_method(w, type, name);
        if (rs_ext_is(type, RS_MAP_ENTRY) || rs_ext_is(type, RS_MAP_OCCUPIED) ||
            rs_ext_is(type, RS_MAP_VACANT))
            return rs_entry_method(arena, type, name);
        if (rs_is_set(type)) {
            if (!strcmp(name, "iter"))
                return rs_iter_of(arena, rs_ref_of(arena, rs_arg(type, 0)));
            if (!strcmp(name, "into_iter") || !strcmp(name, "drain"))
                return rs_iter_of(arena, rs_arg(type, 0));
            if (!strcmp(name, "clone"))
                return type;
            static const char *const borrowed[] = {"get", "first", "last", NULL};
            if (rs_name_in(name, borrowed))
                return rs_option_of(arena, rs_ref_of(arena, rs_arg(type, 0)));
            static const char *const owned[] = {"take", "pop_first", "pop_last", NULL};
            if (rs_name_in(name, owned))
                return rs_option_of(arena, rs_arg(type, 0));
            return &RS_UNKNOWN_TYPE;
        }
        if (rs_ext_is(type, "std::cell::RefCell")) {
            if (!strcmp(name, "borrow"))
                return rs_ext_type(arena, "std::cell::Ref", rs_arg(type, 0), NULL);
            if (!strcmp(name, "borrow_mut"))
                return rs_ext_type(arena, "std::cell::RefMut", rs_arg(type, 0), NULL);
            return !strcmp(name, "into_inner") ? rs_arg(type, 0) : &RS_UNKNOWN_TYPE;
        }
        if (rs_ext_is(type, "std::sync::Mutex")) {
            if (!strcmp(name, "lock"))
                return rs_result_of(
                    arena, rs_ext_type(arena, "std::sync::MutexGuard", rs_arg(type, 0), NULL),
                    NULL);
            return &RS_UNKNOWN_TYPE;
        }
        if (rs_ext_is(type, "std::sync::RwLock")) {
            if (!strcmp(name, "read"))
                return rs_result_of(
                    arena, rs_ext_type(arena, "std::sync::RwLockReadGuard", rs_arg(type, 0), NULL),
                    NULL);
            if (!strcmp(name, "write"))
                return rs_result_of(
                    arena, rs_ext_type(arena, "std::sync::RwLockWriteGuard", rs_arg(type, 0), NULL),
                    NULL);
            return &RS_UNKNOWN_TYPE;
        }
        if (rs_deref_wrapper(type)) {
            if (!strcmp(name, "clone"))
                return type;
            if (!strcmp(name, "as_ref") || !strcmp(name, "as_mut") || !strcmp(name, "borrow"))
                return rs_ref_of(arena, rs_arg(type, 0));
            /* What it wraps, or for Data an Arc of that, which reads the same. */
            if (!strcmp(name, "into_inner"))
                return rs_arg(type, 0);
            type = type->nargs ? type->args[0] : NULL;
            continue;
        }
        break;
    }
    /* Clone::clone returns Self whichever impl serves it. */
    if (!strcmp(name, "clone") || !strcmp(name, "to_owned"))
        return rs_peel(receiver);
    return &RS_UNKNOWN_TYPE;
}

/* Constructors of std wrappers carry their argument type. */
static const rs_type_t *rs_ext_path_call(rs_walk_t *w, const char *path, const rs_type_t *first) {
    CBMArena *arena = w->arena;
    if (!path)
        return &RS_UNKNOWN_TYPE;
    const char *const normal = rs_normal_ext_path(arena, path);
    if (!normal)
        return &RS_UNKNOWN_TYPE;
    static const struct {
        const char *ctor, *type;
    } wrappers[] = {{"std::boxed::Box::new", "std::boxed::Box"},
                    {"std::rc::Rc::new", "std::rc::Rc"},
                    {"std::sync::Arc::new", "std::sync::Arc"},
                    {"std::cell::RefCell::new", "std::cell::RefCell"},
                    {"std::sync::Mutex::new", "std::sync::Mutex"},
                    {"std::sync::RwLock::new", "std::sync::RwLock"},
                    {"std::option::Option::Some", "std::option::Option"}};
    for (size_t i = 0; i < sizeof(wrappers) / sizeof(wrappers[0]); i++)
        if (!strcmp(normal, wrappers[i].ctor))
            return rs_ext_type(arena, wrappers[i].type, first ? first : &RS_UNKNOWN_TYPE, NULL);
    if (!strcmp(normal, "std::result::Result::Ok"))
        return rs_result_of(arena, first ? first : &RS_UNKNOWN_TYPE, NULL);
    if (!strcmp(normal, "std::result::Result::Err"))
        return rs_result_of(arena, &RS_UNKNOWN_TYPE, first);
    /* Rc::clone(&x) and Arc::clone(&x) copy the pointer they are given. */
    if ((!strcmp(normal, "std::rc::Rc::clone") || !strcmp(normal, "std::sync::Arc::clone")) &&
        first)
        return rs_peel(first);
    if (!strcmp(normal, "std::vec::Vec::new") || !strcmp(normal, "std::vec::Vec::with_capacity"))
        return rs_ext_type(arena, "std::vec::Vec", &RS_UNKNOWN_TYPE, NULL);
    if (!strcmp(normal, "std::string::String::new") || !strcmp(normal, "std::string::String::from"))
        return rs_ext_type(arena, "std::string::String", NULL, NULL);
    return &RS_UNKNOWN_TYPE;
}

/* ── Names ─────────────────────────────────────────────────────────── */

static const rs_type_t *rs_walk_expr(rs_walk_t *w, TSNode node);
static void rs_walk_type(rs_walk_t *w, TSNode node);
static void rs_walk_items(rs_walk_t *w, TSNode container);
static void rs_walk_fn(rs_walk_t *w, TSNode node);
static void rs_walk_impl(rs_walk_t *w, TSNode node);
static void rs_walk_trait(rs_walk_t *w, TSNode node);
static void rs_bind_pattern(rs_walk_t *w, TSNode node, const rs_type_t *type);

static const rs_type_t *rs_item_value_type(rs_walk_t *w, rs_item_t *item) {
    rs_env_t env = {.q = &w->q, .inst = rs_inst_for(item->owner, w->inst ? w->inst->crate : NULL)};
    switch (item->kind) {
    case RS_ITEM_CONST:
    case RS_ITEM_STATIC:
        return item->type ? rs_lower(&env, item->type) : &RS_UNKNOWN_TYPE;
    case RS_ITEM_STRUCT:
    case RS_ITEM_UNION:
    case RS_ITEM_ENUM:
        return rs_lower_item(&env, item, NULL);
    default:
        return &RS_UNKNOWN_TYPE;
    }
}

static rs_sel_t rs_sel_from_item(rs_walk_t *w, rs_item_t *item, rs_via_t via) {
    cbm_resolve_rule_t rule = rs_via_rule(via);
    const char *reason = rs_via_reason(via);
    const char *qn = item->kind == RS_ITEM_FN && item->fn ? item->fn->qn : item->qn;
    const char *label = item->kind == RS_ITEM_FN && item->fn ? item->fn->label : item->label;
    bool conflict =
        item->kind == RS_ITEM_FN && item->fn ? item->fn->qn_conflict : item->qn_conflict;
    rs_sel_t sel;
    if (qn)
        sel = rs_sel_target(qn, label, conflict, rule, reason);
    else
        sel = rs_sel_settled("rust_declaration_not_materialized", CBM_RESOLVE_DOMAIN_PROJECT);
    if (item->kind == RS_ITEM_FN)
        sel.fn = item->fn;
    else if (item->kind == RS_ITEM_STRUCT && (item->tuple_struct || item->unit_struct))
        sel.ctor = item;
    sel.value_type = rs_item_value_type(w, item);
    return sel;
}

static rs_sel_t rs_sel_from_variant(rs_walk_t *w, rs_item_t *item, rs_variant_t *variant,
                                    rs_via_t via) {
    rs_sel_t sel = rs_sel_from_item(w, item, via);
    sel.variant_enum = item;
    sel.variant = variant;
    return sel;
}

static rs_sel_t rs_sel_from_ent(rs_walk_t *w, const rs_ent_t *ent) {
    switch (ent->kind) {
    case RS_ENT_ITEM:
        return rs_sel_from_item(w, ent->item, ent->via);
    case RS_ENT_VARIANT:
        return rs_sel_from_variant(w, ent->item, ent->variant, ent->via);
    case RS_ENT_MODULE:
        return rs_sel_settled("rust_module_reference", CBM_RESOLVE_DOMAIN_PROJECT);
    case RS_ENT_EXTERNAL:
    case RS_ENT_PRELUDE:
    case RS_ENT_BUILTIN: {
        rs_sel_t sel = rs_sel_settled("rust_external_name", CBM_RESOLVE_DOMAIN_EXTERNAL);
        sel.external_path = ent->external_path;
        return sel;
    }
    case RS_ENT_AMBIGUOUS: {
        rs_sel_t sel = rs_sel_settled("rust_name_ambiguous", CBM_RESOLVE_DOMAIN_UNKNOWN);
        sel.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        return sel;
    }
    case RS_ENT_NOT_VISIBLE:
        return rs_sel_settled("rust_name_not_visible", CBM_RESOLVE_DOMAIN_UNKNOWN);
    case RS_ENT_UNKNOWN:
        return rs_sel_open(ent->reason ? ent->reason : "rust_name_unknown");
    case RS_ENT_NONE:
        break;
    }
    return rs_sel_open("rust_name_not_found");
}

static bool rs_is_self_type(const char *name) {
    return name && !strcmp(name, "Self");
}

/* A method or associated function selected on a known type. */
static rs_sel_t rs_sel_from_pick(const rs_pick_t *pick) {
    if (pick->fn && pick->fn->qn) {
        rs_sel_t sel = rs_sel_target(pick->fn->qn, pick->fn->label, pick->fn->qn_conflict,
                                     CBM_RESOLVE_RULE_TYPE_MEMBER, pick->reason);
        sel.fn = pick->fn;
        sel.self_ty = pick->self_ty;
        return sel;
    }
    if (pick->fn)
        return rs_sel_settled("rust_declaration_not_materialized", CBM_RESOLVE_DOMAIN_PROJECT);
    if (pick->ambiguous) {
        rs_sel_t sel = rs_sel_settled(pick->reason, CBM_RESOLVE_DOMAIN_UNKNOWN);
        sel.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        return sel;
    }
    bool external = pick->reason && !strcmp(pick->reason, "rust_external_receiver_method");
    if (external)
        return rs_sel_settled(pick->reason, CBM_RESOLVE_DOMAIN_EXTERNAL);
    return rs_sel_open(pick->reason ? pick->reason : "rust_member_unresolved");
}

/* The type a path head names, when the head is Self or a generic parameter:
 * neither is a module-tree name. */
static const rs_type_t *rs_head_type(rs_walk_t *w, const char *head) {
    if (rs_is_self_type(head))
        return w->self_ty;
    int index = 0;
    const rs_generics_t *generics = rs_genv_find(w->gens, head, &index);
    if (!generics)
        return NULL;
    rs_env_t env = rs_env_here(w);
    return rs_param_type(&env, head, generics->bounds[index], generics->nbounds[index]);
}

/* The signature of a std trait's associated function fixes its result whichever
 * impl serves it, derived ones included. */
static const rs_type_t *rs_std_assoc_return(rs_walk_t *w, const rs_type_t *type,
                                            const char *member) {
    const rs_type_t *self_ty = rs_peel(type);
    if (!strcmp(member, "default") || !strcmp(member, "from") || !strcmp(member, "from_iter") ||
        !strcmp(member, "clone"))
        return self_ty;
    if (!strcmp(member, "try_from") || !strcmp(member, "from_str"))
        return rs_result_of(w->arena, self_ty, NULL);
    return NULL;
}

/* `Type::member` on an established type. A member with no node of its own, such
 * as an enum variant or associated constant, still names its type at this site. */
/* Whether a type written in this impl names one of its parameters: nothing here binds it. */
static bool rs_mentions_generic(const rs_tsyn_t *syn, const rs_generics_t *generics) {
    if (!syn || !generics)
        return false;
    if (syn->kind == RS_TS_PATH && syn->nseg >= 1)
        for (int i = 0; i < generics->count; i++)
            if (!strcmp(generics->names[i], syn->segs[0]))
                return true;
    for (int i = 0; i < syn->nargs; i++)
        if (rs_mentions_generic(syn->args[i], generics))
            return true;
    return rs_mentions_generic(syn->inner, generics);
}

/* `Ready::ALL`, `D::DATABASE`: an associated const's value has its declared type. An inherent one
 * is searched before a trait's, as for a method. A second candidate at the same rank answers
 * only if it writes the same type. */
static const rs_type_t *rs_assoc_const_type(rs_walk_t *w, const rs_type_t *type, const char *name) {
    const rs_type_t *peeled = rs_peel(type);
    const rs_assoc_const_t *found = NULL;
    const rs_impl_t *owner_impl = NULL;
    rs_mod_t *owner = NULL;
    if (peeled->kind == RT_ITEM) {
        for (int inherent = 1; inherent >= 0 && !found; inherent--)
            for (const rs_impl_t *impl = peeled->item->impls; impl; impl = impl->next_self) {
                if ((impl->trait_syn == NULL) != (inherent == 1))
                    continue;
                const rs_assoc_const_t *hit =
                    rs_const_named(impl->sorted_consts, impl->nconsts, name);
                if (!hit)
                    continue;
                if (found && !rs_tsyn_same(found->type, hit->type))
                    return &RS_UNKNOWN_TYPE;
                found = hit;
                owner_impl = impl;
                owner = impl->owner;
            }
    } else if (peeled->kind == RT_PARAM) {
        for (int i = 0; i < peeled->ntraits; i++) {
            const rs_assoc_const_t *hit = peeled->traits[i]
                                              ? rs_const_named(peeled->traits[i]->sorted_consts,
                                                               peeled->traits[i]->nconsts, name)
                                              : NULL;
            if (!hit)
                continue;
            if (found)
                return &RS_UNKNOWN_TYPE;
            found = hit;
            owner = peeled->traits[i]->owner;
        }
    }
    if (!found || !owner || (owner_impl && rs_mentions_generic(found->type, owner_impl->generics)))
        return &RS_UNKNOWN_TYPE;
    rs_env_t env = {.q = &w->q,
                    .inst = rs_inst_for(owner, w->inst ? w->inst->crate : NULL),
                    .self_ty = peeled,
                    .impl = owner_impl};
    return rs_lower(&env, found->type);
}

static rs_sel_t rs_member_of_type(rs_walk_t *w, const rs_type_t *type, const char *member) {
    const rs_type_t *peeled = rs_peel(type);
    if (peeled->kind == RT_ITEM && peeled->item->kind == RS_ITEM_ENUM) {
        for (rs_variant_t *variant = peeled->item->variants; variant; variant = variant->next)
            if (!strcmp(variant->name, member))
                return rs_sel_from_variant(w, peeled->item, variant, RS_VIA_PATH);
    }
    rs_pick_t pick = rs_find_assoc(w, type, member);
    rs_sel_t sel = rs_sel_from_pick(&pick);
    if (sel.target || pick.ambiguous)
        return sel;
    if (peeled->kind == RT_ITEM && peeled->item->qn) {
        sel = rs_sel_target(peeled->item->qn, peeled->item->label, peeled->item->qn_conflict,
                            CBM_RESOLVE_RULE_MODULE_PATH, "rust_assoc_member_of_type");
        sel.value_type = rs_assoc_const_type(w, type, member);
        sel.fn = NULL;
        sel.assoc_return = rs_std_assoc_return(w, type, member);
        return sel;
    }
    if (peeled->kind == RT_EXT || peeled->kind == RT_PRIM) {
        sel = rs_sel_settled("rust_external_name", CBM_RESOLVE_DOMAIN_EXTERNAL);
        sel.assoc_return = rs_std_assoc_return(w, type, member);
        return sel;
    }
    if (peeled->kind == RT_PARAM)
        sel.value_type = rs_assoc_const_type(w, type, member);
    return sel;
}

static rs_sel_t rs_sel_from_type_item(rs_walk_t *w, rs_item_t *item, const char *member) {
    if (item->kind == RS_ITEM_TRAIT) {
        const rs_fn_t *fn = rs_trait_member(w, item, member, false, 0);
        if (fn && fn->qn) {
            rs_sel_t sel = rs_sel_target(fn->qn, fn->label, fn->qn_conflict,
                                         CBM_RESOLVE_RULE_TYPE_MEMBER, "rust_trait_path_method");
            sel.fn = fn;
            return sel;
        }
        return rs_sel_open("rust_trait_member_not_found");
    }
    rs_env_t env = {.q = &w->q, .inst = rs_inst_for(item->owner, w->inst ? w->inst->crate : NULL)};
    const rs_type_t *type = rs_lower_item(&env, item, NULL);
    return rs_member_of_type(w, type, member);
}

/* A path in value position: module items, enum variants, members of a type. */
static rs_sel_t rs_value_path(rs_walk_t *w, const rs_tsyn_t *path) {
    if (!path->nseg)
        return rs_sel_open("rust_path_unsupported");
    rs_inst_t *module = w->inst;
    if (path->nseg >= 2 && !path->absolute) {
        const rs_type_t *head = rs_head_type(w, path->segs[0]);
        if (head && path->nseg == 2)
            return rs_member_of_type(w, head, path->segs[1]);
        if (head)
            return rs_sel_open("rust_assoc_path_too_deep");
    }
    int consumed = 0;
    rs_ent_t ent = rs_resolve_path(&w->q, module, w->blocks, w->nblocks, path->segs, path->nseg,
                                   path->absolute, RS_NS_VALUE, false, NULL, &consumed);
    if (ent.kind == RS_ENT_NONE && path->nseg >= 2) {
        /* The last segment may be a type used as a value head; retry as a type. */
        ent = rs_resolve_path(&w->q, module, w->blocks, w->nblocks, path->segs, path->nseg,
                              path->absolute, RS_NS_TYPE, false, NULL, &consumed);
    }
    if (ent.kind == RS_ENT_ITEM && consumed == path->nseg - 1)
        return rs_sel_from_type_item(w, ent.item, path->segs[path->nseg - 1]);
    if (ent.kind == RS_ENT_ITEM && consumed < path->nseg)
        return rs_sel_open("rust_assoc_path_too_deep");
    /* `ext::Type::name()`: a project trait implemented for that external type can supply it. */
    if ((ent.kind == RS_ENT_EXTERNAL || ent.kind == RS_ENT_BUILTIN) && path->nseg >= 2 &&
        w->project->ext_impls) {
        rs_tsyn_t owner = *path;
        owner.nseg--;
        rs_env_t env = rs_env_here(w);
        const rs_type_t *type = rs_peel(rs_lower(&env, &owner));
        if (type->kind == RT_EXT || type->kind == RT_PRIM) {
            rs_pick_t pick = rs_find_assoc(w, type, path->segs[path->nseg - 1]);
            if (pick.fn || pick.ambiguous)
                return rs_sel_from_pick(&pick);
        }
    }
    if (ent.kind == RS_ENT_ITEM && consumed == path->nseg && ent.item->kind == RS_ITEM_TYPE) {
        /* An alias of a tuple or unit struct can stand as its constructor. */
        rs_env_t env = {.q = &w->q, .inst = rs_inst_for(ent.item->owner, w->inst->crate)};
        const rs_type_t *alias = rs_peel(rs_lower_item(&env, ent.item, NULL));
        if (alias->kind == RT_ITEM)
            return rs_sel_from_item(w, alias->item, RS_VIA_PATH);
    }
    return rs_sel_from_ent(w, &ent);
}

static rs_sel_t rs_value_name(rs_walk_t *w, const char *name, const rs_type_t **local_type,
                              bool *is_local) {
    *is_local = false;
    if (rs_is_self_type(name) && w->self_ty) {
        const rs_type_t *peeled = rs_peel(w->self_ty);
        if (peeled->kind == RT_ITEM) {
            rs_sel_t sel = rs_sel_from_item(w, peeled->item, RS_VIA_LOCAL);
            sel.value_type = w->self_ty;
            return sel;
        }
        return rs_sel_open("rust_self_type_unknown");
    }
    const rs_binding_t *binding = rs_local(w, name);
    if (binding) {
        *is_local = true;
        *local_type = binding->type;
        return rs_sel_settled("rust_local_binding", CBM_RESOLVE_DOMAIN_UNKNOWN);
    }
    rs_ent_t ent = rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name, RS_NS_VALUE);
    return rs_sel_from_ent(w, &ent);
}

static rs_tsyn_t *rs_path_of(rs_walk_t *w, TSNode node) {
    /* Segment flattening is shared; a path with `<T as Tr>` has no plain spelling. */
    rs_tsyn_t *path = rs_tsyn_from(&w->b, node);
    return path && path->kind == RS_TS_PATH ? path : NULL;
}

/* ── Types in source position ──────────────────────────────────────── */

static void rs_type_name_site(rs_walk_t *w, TSNode site, const char *const *segs, int nseg,
                              bool absolute) {
    uint32_t start = ts_node_start_byte(site), end = ts_node_end_byte(site);
    if (nseg == 1 && !absolute) {
        /* `Vec<_>` asks the compiler to infer: the placeholder names nothing. */
        if (!strcmp(segs[0], "_")) {
            rs_sel_t sel = rs_sel_settled("rust_inferred_type", CBM_RESOLVE_DOMAIN_UNKNOWN);
            rs_emit(w, CBM_RUST_SITE_REF, start, end, &sel);
            return;
        }
        if (rs_is_self_type(segs[0])) {
            const rs_type_t *self_ty = w->self_ty ? rs_peel(w->self_ty) : NULL;
            rs_sel_t sel = self_ty && self_ty->kind == RT_ITEM
                               ? rs_sel_from_item(w, self_ty->item, RS_VIA_LOCAL)
                               : rs_sel_open("rust_self_type_unknown");
            rs_emit(w, CBM_RUST_SITE_REF, start, end, &sel);
            return;
        }
        int index = 0;
        if (rs_genv_find(w->gens, segs[0], &index)) {
            rs_sel_t sel = rs_sel_settled("rust_generic_parameter", CBM_RESOLVE_DOMAIN_UNKNOWN);
            rs_emit(w, CBM_RUST_SITE_REF, start, end, &sel);
            return;
        }
    }
    /* `Self::Output`, `D::Error`: a member of a trait or impl, which has no graph node. */
    int param_index = 0;
    if (nseg == 2 && !absolute &&
        (rs_is_self_type(segs[0]) || rs_genv_find(w->gens, segs[0], &param_index))) {
        rs_sel_t sel = rs_sel_settled("rust_associated_type", CBM_RESOLVE_DOMAIN_UNKNOWN);
        rs_emit(w, CBM_RUST_SITE_REF, start, end, &sel);
        return;
    }
    int consumed = 0;
    rs_ent_t ent = rs_resolve_path(&w->q, w->inst, w->blocks, w->nblocks, segs, nseg, absolute,
                                   RS_NS_TYPE, false, NULL, &consumed);
    /* Only a type can have members; any other stop keeps its own reason. */
    rs_sel_t sel = consumed == nseg || ent.kind != RS_ENT_ITEM
                       ? rs_sel_from_ent(w, &ent)
                       : rs_sel_open("rust_assoc_type_path");
    rs_emit(w, CBM_RUST_SITE_REF, start, end, &sel);
}

static void rs_walk_type_path(rs_walk_t *w, TSNode node) {
    rs_tsyn_t *path = rs_tsyn_from(&w->b, node);
    if (!path || path->kind != RS_TS_PATH)
        return;
    TSNode name = rs_field(node, "name");
    TSNode head = rs_field(node, "path");
    rs_type_name_site(w, ts_node_is_null(name) ? node : name, path->segs, path->nseg,
                      path->absolute);
    /* A whole-path annotation is its own site too: `db_stats: index_scheduler::IndexStats`. */
    if (!ts_node_is_null(name))
        rs_type_name_site(w, node, path->segs, path->nseg, path->absolute);
    /* The qualifier is its own reference site: `fmt` in fmt::Formatter. */
    if (!ts_node_is_null(head) && path->nseg > 1) {
        if (rs_is(head, "generic_type"))
            rs_walk_type(w, head);
        else if (rs_is(head, "identifier") || rs_is(head, "scoped_identifier"))
            rs_type_name_site(w, head, path->segs, path->nseg - 1, path->absolute);
    }
}

static void rs_walk_type(rs_walk_t *w, TSNode node) {
    if (ts_node_is_null(node) || !ts_node_is_named(node))
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "type_identifier")) {
        const char *name = rs_text(&w->b, node);
        if (name) {
            const char *segs[1] = {name};
            rs_type_name_site(w, node, segs, 1, false);
        }
        return;
    }
    if (!strcmp(kind, "scoped_type_identifier")) {
        rs_walk_type_path(w, node);
        return;
    }
    if (!strcmp(kind, "primitive_type") || !strcmp(kind, "lifetime") ||
        !strcmp(kind, "line_comment") || !strcmp(kind, "block_comment"))
        return;
    if (!strcmp(kind, "type_binding")) {
        /* `Item = T`: Item names a member of the bound trait, not a scope name. */
        rs_walk_type(w, rs_field(node, "type"));
        return;
    }
    if (!strcmp(kind, "type_parameter") || !strcmp(kind, "const_parameter")) {
        rs_walk_type(w, rs_field(node, "bounds"));
        rs_walk_type(w, rs_field(node, "default_type"));
        rs_walk_type(w, rs_field(node, "type"));
        return;
    }
    if (w->depth > RS_MAX_WALK_DEPTH)
        return;
    w->depth++;
    rs_children_t children = rs_children(node);
    TSNode child;
    while (rs_children_next(&children, &child))
        rs_walk_type(w, child);
    w->depth--;
}

/* A struct literal or pattern head `a::b::Name<T>`: its leaf carries the selected
 * declaration, its qualifier is a reference site of its own. */
static void rs_emit_type_head(rs_walk_t *w, TSNode head, const rs_tsyn_t *path,
                              const rs_sel_t *sel) {
    if (ts_node_is_null(head))
        return;
    TSNode site = head;
    if (rs_is(head, "generic_type") || rs_is(head, "generic_type_with_turbofish")) {
        TSNode args = rs_field(head, "type_arguments");
        if (!ts_node_is_null(args))
            rs_walk_type(w, args);
        TSNode inner = rs_field(head, "type");
        if (!ts_node_is_null(inner))
            site = inner;
    }
    if (rs_is(site, "scoped_type_identifier") || rs_is(site, "scoped_identifier")) {
        TSNode qualifier = rs_field(site, "path");
        if ((rs_is(qualifier, "identifier") || rs_is(qualifier, "scoped_identifier")) && path &&
            path->nseg > 1)
            rs_type_name_site(w, qualifier, path->segs, path->nseg - 1, path->absolute);
        else if (rs_is(qualifier, "generic_type"))
            rs_walk_type(w, qualifier);
        TSNode name = rs_field(site, "name");
        if (!ts_node_is_null(name))
            site = name;
    }
    rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(site), ts_node_end_byte(site), sel);
}

/* ── Patterns ──────────────────────────────────────────────────────── */

/* A pattern's owner type keeps the scrutinee's generic arguments when they name it.
 * Arena-held: a field's lowered type may be `Self`, and so point back at it. */
static const rs_type_t *rs_owner_type(rs_walk_t *w, const rs_item_t *owner,
                                      const rs_type_t *scrutinee) {
    rs_type_t *type = rs_type_new(w->arena, RT_ITEM);
    if (!type)
        return &RS_UNKNOWN_TYPE;
    type->item = (rs_item_t *)owner;
    if (scrutinee->kind == RT_ITEM && scrutinee->item == owner) {
        type->args = scrutinee->args;
        type->nargs = scrutinee->nargs;
    }
    return type;
}

/* Pattern payloads of Some/Ok/Err and of project variants and tuple structs. */
static const rs_type_t *rs_payload_type(rs_walk_t *w, const rs_sel_t *sel,
                                        const rs_type_t *scrutinee, int position) {
    const rs_type_t *peeled = rs_peel(scrutinee);
    if (sel->external_path) {
        const char *path = sel->external_path;
        if (!strcmp(path, "std::option::Option::Some") && rs_ext_is(peeled, "std::option::Option"))
            return rs_arg(peeled, 0);
        if (!strcmp(path, "std::result::Result::Ok") && rs_ext_is_result(peeled))
            return rs_arg(peeled, 0);
        if (!strcmp(path, "std::result::Result::Err") && rs_ext_is_result(peeled))
            return rs_arg(peeled, 1);
        /* `Entry::Occupied(e)`, under whichever path this map's Entry is named. */
        if (rs_ext_is(peeled, RS_MAP_ENTRY)) {
            size_t length = strlen(path);
            static const char occupied[] = "Entry::Occupied", vacant[] = "Entry::Vacant";
            if (length >= sizeof(occupied) - 1 &&
                !strcmp(path + length - (sizeof(occupied) - 1), occupied))
                return rs_ext_type(w->arena, RS_MAP_OCCUPIED, rs_arg(peeled, 0), rs_arg(peeled, 1));
            if (length >= sizeof(vacant) - 1 &&
                !strcmp(path + length - (sizeof(vacant) - 1), vacant))
                return rs_ext_type(w->arena, RS_MAP_VACANT, rs_arg(peeled, 0), rs_arg(peeled, 1));
        }
        return &RS_UNKNOWN_TYPE;
    }
    const rs_item_t *owner = sel->variant_enum ? sel->variant_enum : sel->ctor;
    const rs_field_t *field = sel->variant ? sel->variant->fields : owner ? owner->fields : NULL;
    for (int i = 0; field && i < position; i++)
        field = field->next;
    if (!field || !owner)
        return &RS_UNKNOWN_TYPE;
    rs_field_pick_t pick = {.field = field, .owner = rs_owner_type(w, owner, peeled)};
    return rs_field_type(w, &pick);
}

static void rs_bind_tuple_struct(rs_walk_t *w, TSNode node, const rs_type_t *type) {
    TSNode head = rs_field(node, "type");
    rs_sel_t sel = rs_sel_open("rust_pattern_path_unsupported");
    if (rs_is(head, "identifier")) {
        const char *name = rs_text(&w->b, head);
        const rs_type_t *ignored = NULL;
        bool is_local = false;
        /* A constructor pattern's head is never a local variable. */
        rs_ent_t ent =
            rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name ? name : "", RS_NS_VALUE);
        sel = rs_is_self_type(name) ? rs_value_name(w, name, &ignored, &is_local)
                                    : rs_sel_from_ent(w, &ent);
    } else if (!ts_node_is_null(head)) {
        rs_tsyn_t *path = rs_path_of(w, head);
        if (path)
            sel = rs_value_path(w, path);
    }
    if (!ts_node_is_null(head))
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(head), ts_node_end_byte(head), &sel);
    int position = 0;
    bool typed = true;
    for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
        TSNode child = ts_node_child(node, i);
        /* `_` is an anonymous token and it holds a position: `Foo(_, v)`. */
        if (!ts_node_is_named(child)) {
            position += rs_is(child, "_") ? 1 : 0;
            continue;
        }
        if (ts_node_eq(child, head))
            continue;
        if (rs_is(child, "remaining_field_pattern")) {
            typed = false; /* positions after `..` count from the end */
            continue;
        }
        rs_bind_pattern(w, child,
                        typed ? rs_payload_type(w, &sel, type, position) : &RS_UNKNOWN_TYPE);
        position++;
    }
}

static const rs_field_t *rs_fields_of_sel(const rs_sel_t *sel) {
    if (sel->variant)
        return sel->variant->fields;
    return sel->ctor ? sel->ctor->fields : NULL;
}

/* `Self::Variant { .. }`: Self is the impl's own type, so the variant is its member. */
static bool rs_sel_from_self_path(rs_walk_t *w, const rs_tsyn_t *path, rs_sel_t *sel) {
    if (path->nseg != 2 || !rs_is_self_type(path->segs[0]) || !w->self_ty)
        return false;
    const rs_type_t *self_ty = rs_peel(w->self_ty);
    if (self_ty->kind != RT_ITEM)
        return false;
    *sel = rs_sel_from_type_item(w, self_ty->item, path->segs[1]);
    return true;
}

static void rs_bind_struct_pattern(rs_walk_t *w, TSNode node, const rs_type_t *type) {
    TSNode head = rs_field(node, "type");
    rs_tsyn_t *path = ts_node_is_null(head) ? NULL : rs_path_of(w, head);
    rs_sel_t sel = rs_sel_open("rust_pattern_path_unsupported");
    const rs_item_t *owner = NULL;
    const rs_field_t *fields = NULL;
    if (path) {
        const rs_type_t *self_ty = path->nseg == 1 ? rs_head_type(w, path->segs[0]) : NULL;
        if (self_ty && rs_peel(self_ty)->kind == RT_ITEM) {
            owner = rs_peel(self_ty)->item;
            fields = owner->fields;
            sel = rs_sel_from_item(w, (rs_item_t *)owner, RS_VIA_LOCAL);
        } else {
            if (!rs_sel_from_self_path(w, path, &sel)) {
                int consumed = 0;
                rs_ent_t ent =
                    rs_resolve_path(&w->q, w->inst, w->blocks, w->nblocks, path->segs, path->nseg,
                                    path->absolute, RS_NS_TYPE, false, NULL, &consumed);
                if (ent.kind == RS_ENT_ITEM && consumed == path->nseg - 1) {
                    sel = rs_sel_from_type_item(w, ent.item, path->segs[path->nseg - 1]);
                } else if (consumed == path->nseg) {
                    sel = rs_sel_from_ent(w, &ent);
                    if (ent.kind == RS_ENT_ITEM) {
                        owner = ent.item;
                        fields = ent.item->fields;
                    }
                } else if (ent.kind == RS_ENT_UNKNOWN || ent.kind == RS_ENT_AMBIGUOUS) {
                    /* The head's own outcome says more than "unsupported". */
                    sel = rs_sel_from_ent(w, &ent);
                }
            }
            if (sel.variant_enum) {
                owner = sel.variant_enum;
                fields = rs_fields_of_sel(&sel);
            }
        }
    }
    rs_emit_type_head(w, head, path, &sel);
    const rs_type_t *peeled = rs_peel(type);
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!rs_is(child, "field_pattern"))
            continue;
        TSNode shorthand = rs_field(child, "shorthand");
        TSNode name_node = !ts_node_is_null(shorthand) ? shorthand : rs_field(child, "name");
        const char *name = rs_text(&w->b, name_node);
        const rs_field_t *field = NULL;
        for (const rs_field_t *at = fields; name && at; at = at->next)
            if (!strcmp(at->name, name))
                field = at;
        const rs_type_t *field_type = &RS_UNKNOWN_TYPE;
        if (field && owner) {
            rs_field_pick_t pick = {.field = field, .owner = rs_owner_type(w, owner, peeled)};
            field_type = rs_field_type(w, &pick);
            if (field->qn) {
                rs_sel_t site = rs_sel_target(field->qn, field->label, false,
                                              CBM_RESOLVE_RULE_TYPE_MEMBER, "rust_struct_field");
                rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(name_node),
                        ts_node_end_byte(name_node), &site);
            }
        }
        if (!ts_node_is_null(shorthand)) {
            rs_bind(w, name, field_type);
        } else {
            TSNode pattern = rs_field(child, "pattern");
            if (!ts_node_is_null(pattern))
                rs_bind_pattern(w, pattern, field_type);
            else
                rs_bind(w, name, field_type);
        }
    }
}

/* An identifier pattern names a constant, unit struct or unit variant when one
 * is in scope; otherwise it declares a new variable. That is Rust's own rule. */
static bool rs_identifier_pattern_refers(rs_walk_t *w, TSNode node, const char *name) {
    rs_ent_t ent = rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name, RS_NS_VALUE);
    bool refers = ent.kind == RS_ENT_VARIANT ||
                  (ent.kind == RS_ENT_PRELUDE && ent.external_path &&
                   !strcmp(ent.external_path, "std::option::Option::None")) ||
                  (ent.kind == RS_ENT_ITEM &&
                   (ent.item->kind == RS_ITEM_CONST || ent.item->kind == RS_ITEM_STATIC ||
                    (ent.item->kind == RS_ITEM_STRUCT && ent.item->unit_struct)));
    if (refers) {
        rs_sel_t sel = rs_sel_from_ent(w, &ent);
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(node), ts_node_end_byte(node), &sel);
    }
    return refers;
}

static void rs_bind_pattern(rs_walk_t *w, TSNode node, const rs_type_t *type) {
    if (ts_node_is_null(node) || w->depth > RS_MAX_WALK_DEPTH)
        return;
    const char *kind = ts_node_type(node);
    w->depth++;
    if (!strcmp(kind, "identifier")) {
        const char *name = rs_text(&w->b, node);
        if (name && !rs_identifier_pattern_refers(w, node, name))
            rs_bind(w, name, type);
    } else if (!strcmp(kind, "mut_pattern") || !strcmp(kind, "ref_pattern") ||
               !strcmp(kind, "reference_pattern") || !strcmp(kind, "generic_pattern")) {
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
            TSNode child = ts_node_named_child(node, i);
            if (!rs_is(child, "mutable_specifier"))
                rs_bind_pattern(w, child,
                                !strcmp(kind, "reference_pattern") ? rs_peel(type) : type);
        }
    } else if (!strcmp(kind, "captured_pattern")) {
        uint32_t count = ts_node_named_child_count(node);
        if (count > 0)
            rs_bind(w, rs_text(&w->b, ts_node_named_child(node, 0)), type);
        if (count > 1)
            rs_bind_pattern(w, ts_node_named_child(node, count - 1), type);
    } else if (!strcmp(kind, "tuple_pattern")) {
        const rs_type_t *peeled = rs_peel(type);
        int position = 0;
        bool typed = peeled->kind == RT_TUPLE;
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            /* `_` is an anonymous token and it holds a position: `(_, v)`. */
            if (!ts_node_is_named(child)) {
                position += rs_is(child, "_") ? 1 : 0;
                continue;
            }
            if (rs_is(child, "remaining_field_pattern")) {
                typed = false;
                continue;
            }
            rs_bind_pattern(w, child, typed ? rs_arg(peeled, position) : &RS_UNKNOWN_TYPE);
            position++;
        }
    } else if (!strcmp(kind, "tuple_struct_pattern")) {
        rs_bind_tuple_struct(w, node, type);
    } else if (!strcmp(kind, "struct_pattern")) {
        rs_bind_struct_pattern(w, node, type);
    } else if (!strcmp(kind, "or_pattern")) {
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++)
            rs_bind_pattern(w, ts_node_named_child(node, i), type);
    } else if (!strcmp(kind, "slice_pattern")) {
        const rs_type_t *peeled = rs_peel(type);
        const rs_type_t *element = rs_is_seq(peeled) ? rs_seq_element(peeled) : &RS_UNKNOWN_TYPE;
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++)
            rs_bind_pattern(w, ts_node_named_child(node, i), element);
    } else if (!strcmp(kind, "scoped_identifier")) {
        rs_tsyn_t *path = rs_path_of(w, node);
        rs_sel_t sel = path ? rs_value_path(w, path) : rs_sel_open("rust_pattern_path_unsupported");
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(node), ts_node_end_byte(node), &sel);
    } else if (!strcmp(kind, "match_pattern")) {
        if (ts_node_named_child_count(node))
            rs_bind_pattern(w, ts_node_named_child(node, 0), type);
    }
    w->depth--;
}

/* ── Expressions ───────────────────────────────────────────────────── */

/* Rust has no overloading, so arguments never choose a target. A known primitive
 * mismatch or wrong arity still proves this candidate is not the callee of a
 * program that compiles, so the site abstains rather than asserting it. */
static bool rs_integer_literal_fits(const char *text, const char *param, bool negative) {
    unsigned long long value = 0;
    const char *at = text;
    bool digit = false;
    if (at[0] == '0' && (at[1] == 'x' || at[1] == 'b' || at[1] == 'o'))
        return true;
    for (; (*at >= '0' && *at <= '9') || *at == '_'; at++) {
        if (*at == '_')
            continue;
        if (value > (~0ULL - (unsigned)(*at - '0')) / 10)
            return true;
        value = value * 10 + (unsigned)(*at - '0');
        digit = true;
    }
    if (!digit)
        return true;
    if (*at && strcmp(at, param) != 0)
        return false; /* explicit suffix naming another type */
    static const struct {
        const char *name;
        unsigned bits;
        bool is_signed;
    } ranges[] = {{"i8", 8, true},  {"i16", 16, true},  {"i32", 32, true},  {"i64", 64, true},
                  {"u8", 8, false}, {"u16", 16, false}, {"u32", 32, false}, {"u64", 64, false}};
    for (size_t i = 0; i < sizeof(ranges) / sizeof(ranges[0]); i++) {
        if (strcmp(ranges[i].name, param) != 0)
            continue;
        if (negative && !ranges[i].is_signed)
            return false;
        unsigned long long limit =
            ranges[i].bits == 64
                ? (ranges[i].is_signed ? (1ULL << 63) - (negative ? 0 : 1) : ~0ULL)
                : (ranges[i].is_signed ? (1ULL << (ranges[i].bits - 1)) - (negative ? 0 : 1)
                                       : (1ULL << ranges[i].bits) - 1);
        return value <= limit;
    }
    return !strncmp(param, "i", 1) || !strncmp(param, "u", 1);
}

static bool rs_arg_conflicts(rs_walk_t *w, TSNode arg, const rs_type_t *arg_type,
                             const rs_type_t *param) {
    const rs_type_t *declared = param;
    if (!declared || declared->kind != RT_PRIM || !declared->name)
        return false;
    TSNode value = arg;
    bool negative = false;
    while (rs_is(value, "parenthesized_expression") && ts_node_named_child_count(value) == 1)
        value = ts_node_named_child(value, 0);
    if (rs_is(value, "unary_expression") && ts_node_named_child_count(value) == 1) {
        const char *text = rs_text(&w->b, value);
        if (text && text[0] == '-') {
            negative = true;
            value = ts_node_named_child(value, 0);
        }
    }
    if (rs_is(value, "integer_literal")) {
        const char *text = rs_text(&w->b, value);
        if (!strcmp(declared->name, "f32") || !strcmp(declared->name, "f64") ||
            !strcmp(declared->name, "bool") || !strcmp(declared->name, "char") ||
            !strcmp(declared->name, "str"))
            return true;
        return text && !rs_integer_literal_fits(text, declared->name, negative);
    }
    if (rs_is(value, "boolean_literal"))
        return strcmp(declared->name, "bool") != 0;
    if (arg_type && arg_type->kind == RT_PRIM && arg_type->name)
        return strcmp(arg_type->name, declared->name) != 0;
    return false;
}

static int rs_argument_nodes(TSNode arguments, TSNode *out, int cap) {
    int count = 0;
    for (uint32_t i = 0;
         !ts_node_is_null(arguments) && i < ts_node_named_child_count(arguments) && count < cap;
         i++) {
        TSNode child = ts_node_named_child(arguments, i);
        if (rs_is(child, "line_comment") || rs_is(child, "block_comment") ||
            rs_is(child, "attribute_item"))
            continue;
        out[count++] = child;
    }
    return count;
}

typedef struct {
    const rs_type_t *first;
    const rs_type_t *closure_ret;
    int count;
} rs_args_t;

enum { RS_MAX_ARGS = 32 };

/* A closure passed to `fn f(g: impl Fn(&mut X))` or `fn f<G: Fn(&mut X)>(g: G)`
 * receives exactly the declared parameter types. */
static const rs_tsyn_t *rs_fn_bound_of(const rs_fn_t *fn, const rs_tsyn_t *param) {
    if (!param)
        return NULL;
    if (param->kind == RS_TS_FN)
        return param;
    if (param->kind == RS_TS_REF)
        return rs_fn_bound_of(fn, param->inner);
    if (param->kind == RS_TS_DYN) {
        for (int i = 0; i < param->nargs; i++)
            if (param->args[i] && param->args[i]->kind == RS_TS_FN)
                return param->args[i];
        return NULL;
    }
    if (param->kind != RS_TS_PATH || param->nseg != 1)
        return NULL;
    const rs_generics_t *lists[2] = {fn->generics, fn->impl ? fn->impl->generics : NULL};
    for (int l = 0; l < 2; l++) {
        const rs_generics_t *generics = lists[l];
        for (int i = 0; generics && i < generics->count; i++) {
            if (strcmp(generics->names[i], param->segs[0]) != 0)
                continue;
            for (int b = 0; b < generics->nbounds[i]; b++)
                if (generics->bounds[i][b] && generics->bounds[i][b]->kind == RS_TS_FN)
                    return generics->bounds[i][b];
        }
    }
    return NULL;
}

static int rs_closure_params_from_fn(rs_walk_t *w, const rs_fn_t *fn, const rs_type_t *self_ty,
                                     int position, const rs_type_t **out) {
    if (!fn || position < 0 || position >= fn->nparams)
        return 0;
    const rs_tsyn_t *bound = rs_fn_bound_of(fn, fn->params[position]);
    if (!bound)
        return 0;
    rs_bindings_t bindings = rs_bindings_for(w, fn->impl ? fn->impl->generics : NULL, fn->generics,
                                             fn->impl ? fn->impl->self_syn : NULL, self_ty);
    rs_env_t env = {.q = &w->q,
                    .inst = rs_inst_for(fn->owner, w->inst ? w->inst->crate : NULL),
                    .self_ty = self_ty,
                    .impl = fn->impl,
                    .subst_names = bindings.names,
                    .subst_types = bindings.types,
                    .nsubst = bindings.count};
    int n = 0;
    for (int i = 0; i < bound->nargs && n < 2; i++)
        out[n++] = rs_lower(&env, bound->args[i]);
    return n;
}

static rs_args_t rs_walk_arguments(rs_walk_t *w, TSNode arguments, const rs_type_t **types,
                                   TSNode *nodes, int closure_count,
                                   const rs_type_t *const *closure_params, const rs_fn_t *callee,
                                   const rs_type_t *self_ty, int offset) {
    rs_args_t out = {0};
    out.count = rs_argument_nodes(arguments, nodes, RS_MAX_ARGS);
    for (int i = 0; i < out.count; i++) {
        const rs_type_t *declared[2] = {NULL, NULL};
        /* `&|x| ...` is passed to a parameter declared `&impl Fn(..)`. */
        bool closure = rs_is(nodes[i], "closure_expression") ||
                       (rs_is(nodes[i], "reference_expression") &&
                        rs_is(rs_field(nodes[i], "value"), "closure_expression"));
        int declared_count =
            closure ? rs_closure_params_from_fn(w, callee, self_ty, i - offset, declared) : 0;
        const rs_type_t *const *given = declared_count ? declared : closure_params;
        w->nclosure_params = declared_count ? declared_count : closure_count;
        for (int p = 0; given && p < w->nclosure_params && p < 2; p++)
            w->closure_params[p] = given[p];
        w->last_closure_ret = NULL;
        const rs_type_t *type = rs_walk_expr(w, nodes[i]);
        w->nclosure_params = 0;
        types[i] = type;
        if (i == 0)
            out.first = type;
        if (rs_is(nodes[i], "closure_expression") && w->last_closure_ret)
            out.closure_ret = w->last_closure_ret;
    }
    return out;
}

/* Declared parameter types read in the callee's own scope. */
static bool rs_call_conflicts(rs_walk_t *w, const rs_fn_t *fn, const rs_type_t *self_ty,
                              TSNode *nodes, const rs_type_t **types, int count,
                              bool method_syntax) {
    /* Twins may differ in signature; only a sole declaration can conflict. */
    if (!fn || fn->qn_twin)
        return false;
    int expected = fn->nparams;
    int offset = 0;
    if (!method_syntax && fn->self_kind != RS_SELF_NONE)
        offset = 1; /* Type::method(receiver, ...) passes self first */
    if (count != expected + offset)
        return true;
    rs_genv_t impl_gens = {.generics = fn->impl ? fn->impl->generics
                                                : (fn->trait ? fn->trait->generics : NULL)};
    rs_genv_t fn_gens = {.generics = fn->generics, .up = &impl_gens};
    rs_env_t env = {.q = &w->q,
                    .inst = rs_inst_for(fn->owner, w->inst ? w->inst->crate : NULL),
                    .self_ty = self_ty,
                    .impl = fn->impl,
                    .gens = &fn_gens};
    for (int i = 0; i < expected && i + offset < count; i++) {
        const rs_type_t *param = rs_lower(&env, fn->params[i]);
        if (rs_arg_conflicts(w, nodes[i + offset], types[i + offset], param))
            return true;
    }
    return false;
}

static const rs_type_t *rs_ctor_result(rs_walk_t *w, const rs_sel_t *sel) {
    if (sel->variant_enum || sel->ctor)
        return sel->value_type ? sel->value_type : &RS_UNKNOWN_TYPE;
    (void)w;
    return &RS_UNKNOWN_TYPE;
}

/* `<Foo as Tr>::bar`: Foo's own impl of Tr answers, or Tr's default body;
 * `<T>::bar` is a member of T. */
static rs_sel_t rs_qualified_member(rs_walk_t *w, TSNode head) {
    TSNode bracket = rs_field(head, "path");
    TSNode qualified =
        ts_node_named_child_count(bracket) ? ts_node_named_child(bracket, 0) : (TSNode){0};
    const char *member = rs_text(&w->b, rs_field(head, "name"));
    if (ts_node_is_null(qualified) || !member)
        return rs_sel_open("rust_path_unsupported");
    rs_env_t env = rs_env_here(w);
    if (!rs_is(qualified, "qualified_type")) {
        rs_walk_type(w, qualified);
        return rs_member_of_type(w, rs_lower(&env, rs_tsyn_from(&w->b, qualified)), member);
    }
    TSNode type_node = rs_field(qualified, "type"), alias = rs_field(qualified, "alias");
    rs_walk_type(w, type_node);
    rs_walk_type(w, alias);
    const rs_type_t *type = rs_peel(rs_lower(&env, rs_tsyn_from(&w->b, type_node)));
    const rs_item_t *trait = rs_trait_of_syn(&env, rs_tsyn_from(&w->b, alias));
    if (!trait)
        return rs_sel_settled("rust_external_name", CBM_RESOLVE_DOMAIN_EXTERNAL);
    const rs_fn_t *fn = NULL;
    if (type->kind == RT_ITEM)
        for (const rs_impl_t *impl = type->item->impls; impl && !fn; impl = impl->next_self)
            if (impl->trait_item == trait)
                fn = rs_fn_in(impl->methods, member, false);
    fn = fn ? fn : rs_trait_member(w, trait, member, false, 0);
    if (!fn || !fn->qn)
        return rs_sel_open("rust_trait_member_not_found");
    rs_sel_t sel = rs_sel_target(fn->qn, fn->label, fn->qn_conflict, CBM_RESOLVE_RULE_TYPE_MEMBER,
                                 "rust_qualified_trait_member");
    sel.fn = fn;
    sel.self_ty = type;
    return sel;
}

static rs_sel_t rs_select_callee(rs_walk_t *w, TSNode head) {
    const rs_type_t *local_type = NULL;
    bool is_local = false;
    if (rs_is(head, "scoped_identifier") && rs_is(rs_field(head, "path"), "bracketed_type"))
        return rs_qualified_member(w, head);
    if (rs_is(head, "identifier") || rs_is(head, "self")) {
        const char *name = rs_text(&w->b, head);
        return name ? rs_value_name(w, name, &local_type, &is_local)
                    : rs_sel_open("rust_name_missing");
    }
    rs_tsyn_t *path = rs_path_of(w, head);
    return path ? rs_value_path(w, path) : rs_sel_open("rust_path_unsupported");
}

/* Types spelled inside a path: `Foo::<Bar>::new`, `helper::<Bar>()`. */
static void rs_walk_path_generics(rs_walk_t *w, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth > 16)
        return;
    if (rs_is(node, "generic_type") || rs_is(node, "generic_function")) {
        TSNode args = rs_field(node, "type_arguments");
        if (!ts_node_is_null(args))
            rs_walk_type(w, args);
    }
    if (rs_is(node, "scoped_identifier") || rs_is(node, "scoped_type_identifier"))
        rs_walk_path_generics(w, rs_field(node, "path"), depth + 1);
    else if (rs_is(node, "generic_function"))
        rs_walk_path_generics(w, rs_field(node, "function"), depth + 1);
}

static const rs_type_t *rs_walk_path_call(rs_walk_t *w, TSNode call, TSNode callee,
                                          TSNode arguments) {
    TSNode head = callee;
    rs_walk_path_generics(w, callee, 0);
    if (rs_is(head, "generic_function"))
        head = rs_field(head, "function");
    w->call_arg0 = NULL;
    rs_sel_t sel = rs_select_callee(w, head);
    const rs_type_t *types[RS_MAX_ARGS];
    TSNode nodes[RS_MAX_ARGS];
    int offset = sel.fn && sel.fn->self_kind != RS_SELF_NONE ? 1 : 0;
    rs_args_t args =
        rs_walk_arguments(w, arguments, types, nodes, 0, NULL, sel.fn, sel.self_ty, offset);
    /* From<A> and From<B> differ only by the value passed. */
    if (args.first && rs_is(head, "scoped_identifier")) {
        w->call_arg0 = args.first;
        sel = rs_select_callee(w, head);
        w->call_arg0 = NULL;
    }
    /* The inner path of `f::<T>()` is its own reference site, and inside macro
     * arguments extraction records a callee as a plain token. */
    if (rs_is(callee, "generic_function") || w->site_delta != 0)
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(head), ts_node_end_byte(head), &sel);
    const rs_type_t *assoc_return = sel.assoc_return;
    if (sel.target && !sel.fn && !sel.ctor && !sel.variant_enum) {
        /* A declaration that is not callable: a derived member, or a named struct. */
        sel = rs_sel_settled("rust_derived_member_not_materialized", CBM_RESOLVE_DOMAIN_PROJECT);
    } else if (sel.variant_enum) {
        rs_item_t *owner = sel.variant_enum;
        rs_variant_t *variant = sel.variant;
        const rs_type_t *value = sel.value_type;
        /* A variant has no graph node of its own, so a call cannot name it exactly. */
        sel = rs_sel_settled("rust_enum_variant_not_materialized", CBM_RESOLVE_DOMAIN_PROJECT);
        sel.variant_enum = owner;
        sel.variant = variant;
        sel.value_type = value;
    } else if (sel.fn &&
               rs_call_conflicts(w, sel.fn, sel.self_ty, nodes, types, args.count, false)) {
        const rs_fn_t *fn = sel.fn;
        const rs_type_t *self_ty = sel.self_ty;
        sel = rs_sel_settled("rust_argument_conflicts_with_signature", CBM_RESOLVE_DOMAIN_UNKNOWN);
        sel.fn = fn;
        sel.self_ty = self_ty;
    }
    rs_emit(w, CBM_RUST_SITE_CALL, ts_node_start_byte(call), ts_node_end_byte(call), &sel);
    if (assoc_return)
        return assoc_return;
    if (sel.fn)
        return rs_fn_return_for(w, sel.fn, sel.self_ty, types, args.count, offset);
    if (sel.external_path)
        return rs_ext_path_call(w, sel.external_path, args.first);
    return rs_ctor_result(w, &sel);
}

static const rs_type_t *rs_walk_method_call(rs_walk_t *w, TSNode call, TSNode callee,
                                            TSNode arguments) {
    TSNode access = callee;
    if (rs_is(access, "generic_function"))
        access = rs_field(access, "function");
    TSNode value = rs_field(access, "value"), member = rs_field(access, "field");
    const rs_type_t *receiver = rs_walk_expr(w, value);
    const char *name = rs_text(&w->b, member);
    if (!name) {
        rs_sel_t sel = rs_sel_open("rust_name_missing");
        rs_emit(w, CBM_RUST_SITE_CALL, ts_node_start_byte(call), ts_node_end_byte(call), &sel);
        return &RS_UNKNOWN_TYPE;
    }
    const rs_type_t *closure_params[2] = {NULL, NULL};
    int closure_count = rs_closure_params_for(w, receiver, name, closure_params);
    const rs_type_t *types[RS_MAX_ARGS];
    TSNode nodes[RS_MAX_ARGS];
    TSNode turbofish =
        rs_is(callee, "generic_function") ? rs_field(callee, "type_arguments") : (TSNode){0};
    if (!ts_node_is_null(turbofish))
        rs_walk_type(w, turbofish);
    w->call_arg0 = NULL;
    rs_pick_t pick = rs_find_method(w, receiver, name);
    rs_args_t args = rs_walk_arguments(w, arguments, types, nodes, closure_count, closure_params,
                                       pick.fn, pick.self_ty, 0);
    /* Trait impls differing only by a type argument are chosen by the value passed. */
    if (args.first && (pick.ambiguous || !pick.fn)) {
        w->call_arg0 = args.first;
        pick = rs_find_method(w, receiver, name);
        w->call_arg0 = NULL;
    }
    if (!pick.fn && !pick.ambiguous) {
        const rs_type_t *projection = rs_pin_projection(w, receiver, name);
        if (projection) {
            rs_sel_t generated = rs_sel_settled("rust_pin_projection", CBM_RESOLVE_DOMAIN_EXTERNAL);
            rs_emit(w, CBM_RUST_SITE_CALL, ts_node_start_byte(call), ts_node_end_byte(call),
                    &generated);
            return projection;
        }
    }
    rs_sel_t sel = rs_sel_from_pick(&pick);
    if (pick.fn && rs_call_conflicts(w, pick.fn, pick.self_ty, nodes, types, args.count, true)) {
        rs_sel_t blocked =
            rs_sel_settled("rust_argument_conflicts_with_signature", CBM_RESOLVE_DOMAIN_UNKNOWN);
        blocked.fn = sel.fn;
        blocked.self_ty = sel.self_ty;
        sel = blocked;
    }
    rs_emit(w, CBM_RUST_SITE_CALL, ts_node_start_byte(call), ts_node_end_byte(call), &sel);
    /* The call is the use of this method. Inside a macro the extractor also records its
     * name as a plain token; that token is not a second use. */
    if (!ts_node_is_null(member)) {
        rs_sel_t name_token = rs_sel_settled("rust_call_member_name", CBM_RESOLVE_DOMAIN_UNKNOWN);
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(member), ts_node_end_byte(member),
                &name_token);
    }
    if (pick.fn)
        return rs_fn_return_for(w, pick.fn, pick.self_ty, types, args.count, 0);
    /* `.collect::<Vec<_>>()` and `.parse::<T>()` spell their result. */
    if (!ts_node_is_null(turbofish)) {
        TSNode first =
            ts_node_named_child_count(turbofish) ? ts_node_named_child(turbofish, 0) : (TSNode){0};
        if (!ts_node_is_null(first)) {
            rs_env_t env = rs_env_here(w);
            const rs_type_t *spelled = rs_lower(&env, rs_tsyn_from(&w->b, first));
            if (!strcmp(name, "collect"))
                return rs_collect_into(w->arena, spelled, rs_element_of(w, receiver), 0);
            if (!strcmp(name, "sum") || !strcmp(name, "product"))
                return spelled;
            if (!strcmp(name, "parse"))
                return rs_result_of(w->arena, spelled, NULL);
        }
    } else if (!strcmp(name, "collect")) {
        /* `let all: Vec<_> = it.collect();`: the let fills the placeholder. */
        w->collected = rs_element_of(w, receiver);
        w->collected_end = ts_node_end_byte(call);
        w->collected_delta = w->site_delta;
    }
    /* zip pairs this element with the argument's. */
    if (!strcmp(name, "zip") && args.first) {
        const rs_type_t *peeled = rs_peel(receiver);
        if (peeled->kind == RT_ITER)
            return rs_iter_of(w->arena,
                              rs_pair_of(w->arena, peeled->inner, rs_element_of(w, args.first)));
        if (rs_ext_is(peeled, "std::option::Option"))
            return rs_option_of(
                w->arena, rs_pair_of(w->arena, rs_arg(peeled, 0), rs_arg(rs_peel(args.first), 0)));
    }
    return rs_ext_method_ret(w, receiver, name, args.closure_ret);
}

static const rs_type_t *rs_walk_call(rs_walk_t *w, TSNode node) {
    TSNode callee = rs_field(node, "function"), arguments = rs_field(node, "arguments");
    TSNode head = rs_is(callee, "generic_function") ? rs_field(callee, "function") : callee;
    if (rs_is(head, "field_expression"))
        return rs_walk_method_call(w, node, callee, arguments);
    if (rs_is(head, "identifier") || rs_is(head, "scoped_identifier"))
        return rs_walk_path_call(w, node, callee, arguments);
    if (!ts_node_is_null(callee))
        (void)rs_walk_expr(w, callee);
    const rs_type_t *types[RS_MAX_ARGS];
    TSNode nodes[RS_MAX_ARGS];
    (void)rs_walk_arguments(w, arguments, types, nodes, 0, NULL, NULL, NULL, 0);
    rs_sel_t sel = rs_sel_open("rust_callee_expression_unresolved");
    rs_emit(w, CBM_RUST_SITE_CALL, ts_node_start_byte(node), ts_node_end_byte(node), &sel);
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_field(rs_walk_t *w, TSNode node) {
    const rs_type_t *receiver = rs_walk_expr(w, rs_field(node, "value"));
    TSNode member = rs_field(node, "field");
    const char *name = rs_text(&w->b, member);
    if (!name)
        return &RS_UNKNOWN_TYPE;
    const rs_type_t *peeled = rs_peel(receiver);
    if (peeled->kind == RT_TUPLE && rs_is(member, "integer_literal")) {
        int index = atoi(name);
        return rs_arg(peeled, index);
    }
    /* `.0` on an external newtype is its own field, not a field of what it derefs to. */
    if (peeled->kind == RT_EXT && rs_is(member, "integer_literal")) {
        bool wraps_first = rs_ext_is(peeled, "actix_web::web::Json") ||
                           rs_ext_is(peeled, "actix_web::web::Path") ||
                           rs_ext_is(peeled, "actix_web::web::Query");
        return wraps_first && !strcmp(name, "0") ? rs_arg(peeled, 0) : &RS_UNKNOWN_TYPE;
    }
    rs_field_pick_t pick = rs_find_field(w, receiver, name);
    if (pick.field) {
        rs_sel_t sel;
        if (pick.field->qn)
            sel = rs_sel_target(pick.field->qn, pick.field->label, false,
                                CBM_RESOLVE_RULE_TYPE_MEMBER, "rust_struct_field");
        else
            sel = rs_sel_settled("rust_declaration_not_materialized", CBM_RESOLVE_DOMAIN_PROJECT);
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(member), ts_node_end_byte(member), &sel);
        return rs_field_type(w, &pick);
    }
    rs_sel_t sel = peeled->kind == RT_UNKNOWN || peeled->kind == RT_PARAM
                       ? rs_sel_open("rust_receiver_type_unknown")
                   : peeled->kind == RT_EXT
                       ? rs_sel_settled("rust_external_receiver_field", CBM_RESOLVE_DOMAIN_EXTERNAL)
                       : rs_sel_open("rust_field_not_found");
    rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(member), ts_node_end_byte(member), &sel);
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_struct_expr(rs_walk_t *w, TSNode node) {
    TSNode head = rs_field(node, "name");
    rs_tsyn_t *path = ts_node_is_null(head) ? NULL : rs_path_of(w, head);
    const rs_type_t *result = &RS_UNKNOWN_TYPE;
    const rs_field_t *fields = NULL;
    const rs_item_t *owner = NULL;
    rs_sel_t sel = rs_sel_open("rust_struct_path_unsupported");
    if (path) {
        const rs_type_t *head_type = path->nseg == 1 ? rs_head_type(w, path->segs[0]) : NULL;
        if (head_type && rs_peel(head_type)->kind == RT_ITEM) {
            result = head_type;
            owner = rs_peel(head_type)->item;
            fields = owner->fields;
            sel = rs_sel_from_item(w, (rs_item_t *)owner, RS_VIA_LOCAL);
        } else {
            if (rs_sel_from_self_path(w, path, &sel)) {
                result = sel.value_type ? sel.value_type : &RS_UNKNOWN_TYPE;
            } else {
                int consumed = 0;
                rs_env_t env = rs_env_here(w);
                rs_ent_t ent =
                    rs_resolve_path(&w->q, w->inst, w->blocks, w->nblocks, path->segs, path->nseg,
                                    path->absolute, RS_NS_TYPE, false, NULL, &consumed);
                if (ent.kind == RS_ENT_ITEM && consumed == path->nseg - 1) {
                    sel = rs_sel_from_type_item(w, ent.item, path->segs[path->nseg - 1]);
                    result = sel.value_type ? sel.value_type : &RS_UNKNOWN_TYPE;
                } else if (consumed == path->nseg) {
                    sel = rs_sel_from_ent(w, &ent);
                    if (ent.kind == RS_ENT_ITEM) {
                        result = rs_lower(&env, path);
                        const rs_type_t *peeled = rs_peel(result);
                        owner = peeled->kind == RT_ITEM ? peeled->item : ent.item;
                        fields = owner->fields;
                    } else if (ent.kind == RS_ENT_VARIANT) {
                        result = sel.value_type ? sel.value_type : &RS_UNKNOWN_TYPE;
                    }
                } else if (ent.kind == RS_ENT_UNKNOWN || ent.kind == RS_ENT_AMBIGUOUS) {
                    sel = rs_sel_from_ent(w, &ent);
                }
            }
            if (sel.variant_enum) {
                owner = sel.variant_enum;
                fields = rs_fields_of_sel(&sel);
                result = sel.value_type ? sel.value_type : &RS_UNKNOWN_TYPE;
            }
        }
    }
    rs_emit_type_head(w, head, path, &sel);
    rs_children_t initializers = rs_children(rs_field(node, "body"));
    TSNode child;
    while (rs_children_next(&initializers, &child)) {
        if (rs_is(child, "shorthand_field_initializer")) {
            /* `Foo { x }`: the field x and the local x. */
            if (ts_node_named_child_count(child))
                (void)rs_walk_expr(w, ts_node_named_child(child, 0));
        } else if (rs_is(child, "field_initializer")) {
            TSNode field = rs_field(child, "field");
            const char *name = rs_text(&w->b, field);
            for (const rs_field_t *at = fields; name && at; at = at->next) {
                if (strcmp(at->name, name) != 0)
                    continue;
                if (at->qn) {
                    rs_sel_t site =
                        rs_sel_target(at->qn, at->label, false, CBM_RESOLVE_RULE_TYPE_MEMBER,
                                      "rust_struct_field");
                    rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(field),
                            ts_node_end_byte(field), &site);
                }
                break;
            }
            (void)rs_walk_expr(w, rs_field(child, "value"));
        } else if (rs_is(child, "base_field_initializer")) {
            if (ts_node_named_child_count(child))
                (void)rs_walk_expr(w, ts_node_named_child(child, 0));
        }
    }
    return result;
}

static const rs_type_t *rs_element_of(rs_walk_t *w, const rs_type_t *iterable) {
    bool by_ref = iterable && iterable->kind == RT_REF;
    const rs_type_t *type = rs_peel(iterable);
    CBMArena *arena = w->arena;
    const rs_type_t *element = NULL;
    if (type->kind == RT_ITER)
        return type->inner;
    /* Box<I> is an Iterator when I is: Box<dyn Iterator<Item = T>>. */
    if (rs_ext_is(type, "std::boxed::Box") && rs_peel(rs_arg(type, 0))->kind == RT_ITER)
        return rs_peel(rs_arg(type, 0))->inner;
    if (rs_is_seq(type))
        element = rs_seq_element(type);
    else if (rs_is_set(type) || rs_ext_is(type, "std::option::Option") || rs_ext_is_result(type))
        element = rs_arg(type, 0);
    else if (rs_is_map(type)) {
        const rs_type_t *pair = rs_iter_of(arena, NULL);
        rs_type_t *tuple = rs_type_new(arena, RT_TUPLE);
        if (!tuple)
            return pair;
        tuple->args = cbm_arena_alloc(arena, 2 * sizeof(*tuple->args));
        if (tuple->args) {
            tuple->args[0] = by_ref ? rs_ref_of(arena, rs_arg(type, 0)) : rs_arg(type, 0);
            tuple->args[1] = by_ref ? rs_ref_of(arena, rs_arg(type, 1)) : rs_arg(type, 1);
            tuple->nargs = 2;
        }
        return tuple;
    } else if (type->kind == RT_ITEM) {
        /* A project iterator names its element in `type Item = X`. */
        for (const rs_impl_t *impl = type->item->impls; impl; impl = impl->next_self) {
            if (!rs_iterator_trait_name(rs_tsyn_leaf(impl->trait_syn)))
                continue;
            for (int i = 0; i < impl->nassoc; i++) {
                if (strcmp(impl->assoc_names[i], "Item") != 0)
                    continue;
                return rs_impl_assoc_type(w, impl, type, impl->assoc_types[i]);
            }
        }
    }
    if (!element)
        return &RS_UNKNOWN_TYPE;
    return by_ref ? rs_ref_of(arena, element) : element;
}

static const rs_type_t *rs_walk_block(rs_walk_t *w, TSNode node);

static void rs_walk_condition(rs_walk_t *w, TSNode node) {
    if (ts_node_is_null(node))
        return;
    if (rs_is(node, "let_condition")) {
        const rs_type_t *value = rs_walk_expr(w, rs_field(node, "value"));
        rs_bind_pattern(w, rs_field(node, "pattern"), value);
        return;
    }
    if (rs_is(node, "let_chain")) {
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++)
            rs_walk_condition(w, ts_node_named_child(node, i));
        return;
    }
    (void)rs_walk_expr(w, node);
}

static const rs_type_t *rs_first_known(const rs_type_t *a, const rs_type_t *b) {
    if (a && a->kind != RT_UNKNOWN && a->kind != RT_NEVER && a->kind != RT_UNIT)
        return a;
    return b ? b : a ? a : &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_if(rs_walk_t *w, TSNode node) {
    rs_scope_t scope;
    rs_scope_push(w, &scope, false);
    rs_walk_condition(w, rs_field(node, "condition"));
    const rs_type_t *then_type = rs_walk_expr(w, rs_field(node, "consequence"));
    rs_scope_pop(w);
    TSNode alternative = rs_field(node, "alternative");
    const rs_type_t *else_type = NULL;
    if (!ts_node_is_null(alternative) && ts_node_named_child_count(alternative))
        else_type = rs_walk_expr(w, ts_node_named_child(alternative, 0));
    return rs_first_known(then_type, else_type);
}

static const rs_type_t *rs_walk_match(rs_walk_t *w, TSNode node) {
    const rs_type_t *value = rs_walk_expr(w, rs_field(node, "value"));
    const rs_type_t *result = NULL;
    rs_children_t arms = rs_children(rs_field(node, "body"));
    TSNode arm;
    while (rs_children_next(&arms, &arm)) {
        if (!rs_is(arm, "match_arm"))
            continue;
        rs_scope_t scope;
        rs_scope_push(w, &scope, false);
        TSNode pattern = rs_field(arm, "pattern");
        rs_bind_pattern(w, pattern, value);
        TSNode guard = rs_field(pattern, "condition");
        if (!ts_node_is_null(guard))
            rs_walk_condition(w, guard);
        const rs_type_t *arm_type = rs_walk_expr(w, rs_field(arm, "value"));
        rs_scope_pop(w);
        result = rs_first_known(result, arm_type);
    }
    return result ? result : &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_for(rs_walk_t *w, TSNode node) {
    const rs_type_t *iterable = rs_walk_expr(w, rs_field(node, "value"));
    rs_scope_t scope;
    rs_scope_push(w, &scope, false);
    rs_bind_pattern(w, rs_field(node, "pattern"), rs_element_of(w, iterable));
    (void)rs_walk_expr(w, rs_field(node, "body"));
    rs_scope_pop(w);
    return &RS_UNIT_TYPE;
}

static const rs_type_t *rs_walk_closure(rs_walk_t *w, TSNode node) {
    int given = w->nclosure_params;
    const rs_type_t *params[2] = {w->closure_params[0], w->closure_params[1]};
    w->nclosure_params = 0;
    rs_scope_t scope;
    rs_scope_push(w, &scope, false);
    TSNode list = rs_field(node, "parameters");
    int position = 0;
    for (uint32_t i = 0; !ts_node_is_null(list) && i < ts_node_child_count(list); i++) {
        TSNode param = ts_node_child(list, i);
        /* `_` is an anonymous token and it holds a position: `|_, v|`. */
        if (!ts_node_is_named(param)) {
            position += rs_is(param, "_") ? 1 : 0;
            continue;
        }
        const rs_type_t *fallback =
            position < given && position < 2 ? params[position] : &RS_UNKNOWN_TYPE;
        if (rs_is(param, "parameter")) {
            TSNode annotation = rs_field(param, "type");
            const rs_type_t *declared = fallback;
            if (!ts_node_is_null(annotation)) {
                rs_walk_type(w, annotation);
                rs_env_t env = rs_env_here(w);
                declared = rs_lower(&env, rs_tsyn_from(&w->b, annotation));
            }
            rs_bind_pattern(w, rs_field(param, "pattern"), declared);
        } else {
            rs_bind_pattern(w, param, fallback);
        }
        position++;
    }
    const rs_type_t *body = rs_walk_expr(w, rs_field(node, "body"));
    rs_scope_pop(w);
    w->last_closure_ret = body;
    return &RS_UNKNOWN_TYPE;
}

/* Macros whose arguments are ordinary expressions of the caller. */
static bool rs_expression_macro(const char *name) {
    static const char *const macros[] = {"println",
                                         "print",
                                         "eprintln",
                                         "eprint",
                                         "format",
                                         "write",
                                         "writeln",
                                         "panic",
                                         "assert",
                                         "assert_eq",
                                         "assert_ne",
                                         "debug_assert",
                                         "debug_assert_eq",
                                         "debug_assert_ne",
                                         "vec",
                                         "dbg",
                                         "unreachable",
                                         "todo",
                                         "unimplemented",
                                         "format_args",
                                         NULL};
    return rs_name_in(name, macros);
}

/* An identifier token in an expression macro argument. A token after `.` is a
 * member of an expression this walk has not typed; `name =` is a named format
 * argument; `name:` a struct field. Neither is a scope name. */
static void rs_macro_token(rs_walk_t *w, TSNode token, const char *before, const char *after,
                           bool heads_only) {
    const char *name = rs_text(&w->b, token);
    if (!name)
        return;
    bool member = before && (!strcmp(before, ".") || !strcmp(before, "::"));
    bool label = after && (!strcmp(after, ":") || !strcmp(after, "="));
    if (member || label)
        return;
    if (after && !strcmp(after, "::")) {
        const char *segs[1] = {name};
        rs_type_name_site(w, token, segs, 1, false);
        return;
    }
    if (heads_only)
        return;
    const rs_type_t *local_type = NULL;
    bool is_local = false;
    rs_sel_t sel = rs_value_name(w, name, &local_type, &is_local);
    rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(token), ts_node_end_byte(token), &sel);
}

static void rs_walk_macro_tokens(rs_walk_t *w, TSNode tree, bool expression, bool heads_only) {
    if (w->depth > RS_MAX_WALK_DEPTH)
        return;
    w->depth++;
    TSTreeCursor cursor = ts_tree_cursor_new(tree);
    const char *previous = NULL, *pending_before = NULL;
    TSNode pending = (TSNode){0};
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode child = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(child);
        if (!ts_node_is_null(pending)) {
            rs_macro_token(w, pending, pending_before, kind, heads_only);
            pending = (TSNode){0};
        }
        if (!strcmp(kind, "token_tree")) {
            rs_walk_macro_tokens(w, child, expression, heads_only);
        } else if (expression && !strcmp(kind, "identifier")) {
            pending = child;
            pending_before = previous;
        }
        previous = kind;
    }
    if (!ts_node_is_null(pending))
        rs_macro_token(w, pending, pending_before, NULL, heads_only);
    ts_tree_cursor_delete(&cursor);
    w->depth--;
}

static TSNode rs_macro_tree(TSNode invocation) {
    for (uint32_t i = 0; i < ts_node_named_child_count(invocation); i++) {
        TSNode child = ts_node_named_child(invocation, i);
        if (rs_is(child, "token_tree"))
            return child;
    }
    return (TSNode){0};
}

/* A project macro of the same name is not the standard one. */
/* log's and tracing's levels and anyhow's and eyre's errors take expressions of the caller, by
 * their documented contract. Only under those crates' own paths: anything else named debug! is
 * opaque. */
static bool rs_external_expression_macro(rs_walk_t *w, const char *name) {
    static const char *const levels[] = {"trace", "debug", "info", "warn", "error", NULL};
    static const char *const errors[] = {"bail", "ensure", "anyhow", "eyre", "format_err", NULL};
    const char *path = name;
    if (!strstr(name, "::")) {
        rs_ent_t ent = rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name, RS_NS_MACRO);
        if (ent.kind != RS_ENT_EXTERNAL || !ent.external_path)
            return false;
        path = ent.external_path;
    }
    const char *tail = strstr(path, "::");
    if (!tail || strstr(tail + 2, "::"))
        return false;
    size_t crate = (size_t)(tail - path);
    if ((crate == 3 && !strncmp(path, "log", 3)) || (crate == 7 && !strncmp(path, "tracing", 7)))
        return rs_name_in(tail + 2, levels);
    if ((crate == 6 && !strncmp(path, "anyhow", 6)) || (crate == 4 && !strncmp(path, "eyre", 4)))
        return rs_name_in(tail + 2, errors);
    return false;
}

static bool rs_std_expression_macro(rs_walk_t *w, const char *name) {
    if (!name)
        return false;
    if (!rs_expression_macro(name))
        return rs_external_expression_macro(w, name);
    rs_ent_t ent = rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name, RS_NS_MACRO);
    return ent.kind != RS_ENT_ITEM;
}

/* `matches!(x, Kind::One | Kind::Two(_))`: the second argument is a pattern. A path head in
 * it names a type or module of the caller's scope; a bare name in it is a new binding. */
static bool rs_std_pattern_macro(rs_walk_t *w, const char *name) {
    if (!name || (strcmp(name, "matches") != 0 && strcmp(name, "assert_matches") != 0 &&
                  strcmp(name, "debug_assert_matches") != 0))
        return false;
    rs_ent_t ent = rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name, RS_NS_MACRO);
    return ent.kind != RS_ENT_ITEM;
}

/* The arguments of an expression macro are ordinary expressions of the caller,
 * so they get the walk any expression gets. Re-parsed in a wrapper, with sites
 * mapped back to this file. Named format arguments bind only inside the macro. */
static bool rs_walk_macro_arguments(rs_walk_t *w, TSNode tree, const char *name,
                                    const rs_type_t **element) {
    uint32_t start = ts_node_start_byte(tree), end = ts_node_end_byte(tree);
    if (end < start + 2 || end > w->b.length)
        return false;
    if (!w->parser) {
        w->parser = ts_parser_new();
        if (!w->parser || !ts_parser_set_language(w->parser, cbm_ts_language(CBM_LANG_RUST)))
            return false;
    }
    bool sequence = !strcmp(name, "vec");
    static const char call_prefix[] = "fn __m(){__m(";
    static const char list_prefix[] = "fn __m(){[";
    const char *prefix = sequence ? list_prefix : call_prefix;
    const char *suffix = sequence ? "];}" : ");}";
    size_t prefix_len = strlen(prefix);
    size_t inner = end - start - 2;
    size_t total = prefix_len + inner + 3;
    char *buffer = cbm_arena_alloc(w->arena, total + 1);
    if (!buffer)
        return false;
    memcpy(buffer, prefix, prefix_len);
    memcpy(buffer + prefix_len, w->b.source + start + 1, inner);
    memcpy(buffer + prefix_len + inner, suffix, 3);
    buffer[total] = '\0';
    TSTree *tree_copy = cbm_parse_source(w->parser, buffer, (uint32_t)total, (TSParseOptions){0});
    if (!tree_copy)
        return false;
    TSNode root = ts_tree_root_node(tree_copy);
    TSNode fn = ts_node_named_child_count(root) == 1 ? ts_node_named_child(root, 0) : (TSNode){0};
    TSNode body = rs_field(fn, "body");
    TSNode statement = !ts_node_is_null(body) && ts_node_named_child_count(body)
                           ? ts_node_named_child(body, 0)
                           : (TSNode){0};
    TSNode holder = rs_is(statement, "expression_statement") && ts_node_named_child_count(statement)
                        ? ts_node_named_child(statement, 0)
                        : statement;
    TSNode list = sequence ? holder : rs_field(holder, "arguments");
    if (ts_node_has_error(root) || ts_node_is_null(list)) {
        ts_tree_delete(tree_copy);
        return false;
    }
    const char *saved_source = w->b.source;
    size_t saved_length = w->b.length;
    int64_t saved_delta = w->site_delta;
    w->b.source = buffer;
    w->b.length = total;
    w->site_delta = saved_delta + (int64_t)(start + 1) - (int64_t)prefix_len;
    bool first = true;
    rs_children_t args = rs_children(list);
    TSNode arg;
    while (rs_children_next(&args, &arg)) {
        const rs_type_t *type;
        if (rs_is(arg, "assignment_expression") && rs_is(rs_field(arg, "left"), "identifier"))
            type = rs_walk_expr(w, rs_field(arg, "right"));
        else
            type = rs_walk_expr(w, arg);
        if (first && element)
            *element = type;
        first = false;
    }
    w->b.source = saved_source;
    w->b.length = saved_length;
    w->site_delta = saved_delta;
    ts_tree_delete(tree_copy);
    return true;
}

static const rs_type_t *rs_walk_macro(rs_walk_t *w, TSNode node) {
    TSNode head = rs_field(node, "macro");
    const char *name = rs_text(&w->b, head);
    bool expression = rs_std_expression_macro(w, name);
    TSNode tree = rs_macro_tree(node);
    const rs_type_t *element = NULL;
    if (!ts_node_is_null(tree)) {
        bool pattern = !expression && rs_std_pattern_macro(w, name);
        if (pattern)
            rs_walk_macro_tokens(w, tree, true, true);
        else if (!expression || !rs_walk_macro_arguments(w, tree, name, &element))
            rs_walk_macro_tokens(w, tree, expression, false);
    }
    rs_sel_t sel = rs_sel_settled("rust_macro_invocation", CBM_RESOLVE_DOMAIN_UNKNOWN);
    rs_emit(w, CBM_RUST_SITE_CALL, ts_node_start_byte(node), ts_node_end_byte(node), &sel);
    if (name && !strcmp(name, "format"))
        return rs_ext_type(w->arena, "std::string::String", NULL, NULL);
    if (name && !strcmp(name, "vec"))
        return rs_ext_type(w->arena, "std::vec::Vec", element ? element : &RS_UNKNOWN_TYPE, NULL);
    return &RS_UNKNOWN_TYPE;
}

/* These parentheses are an expression macro's, in the view of the file: what the macro yields is
 * whatever its definition makes of the argument. */
static bool rs_is_expression_macro(const rs_walk_t *w, TSNode node) {
    const rs_file_t *file = w->b.file;
    if (!file || !file->nexpression_macros || w->site_delta != 0 ||
        (!rs_is(node, "parenthesized_expression") && !rs_is(node, "tuple_expression") &&
         !rs_is(node, "unit_expression")))
        return false;
    uint32_t start = ts_node_start_byte(node);
    int low = 0, high = file->nexpression_macros;
    while (low < high) {
        int mid = low + (high - low) / 2;
        if (file->expression_macros[mid] < start)
            low = mid + 1;
        else if (file->expression_macros[mid] > start)
            high = mid;
        else
            return true;
    }
    return false;
}

static const rs_type_t *rs_walk_children(rs_walk_t *w, TSNode node) {
    rs_children_t children = rs_children(node);
    TSNode child;
    while (rs_children_next(&children, &child))
        (void)rs_walk_expr(w, child);
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_index(rs_walk_t *w, TSNode node) {
    uint32_t count = ts_node_named_child_count(node);
    const rs_type_t *base =
        count > 0 ? rs_walk_expr(w, ts_node_named_child(node, 0)) : &RS_UNKNOWN_TYPE;
    TSNode index = count > 1 ? ts_node_named_child(node, 1) : (TSNode){0};
    if (!ts_node_is_null(index))
        (void)rs_walk_expr(w, index);
    const rs_type_t *peeled = rs_peel(base);
    while (rs_deref_wrapper(peeled) && peeled->nargs)
        peeled = rs_peel(peeled->args[0]);
    if (rs_is_map(peeled))
        return rs_arg(peeled, 1);
    if (rs_is_seq(peeled)) {
        if (rs_is(index, "range_expression")) {
            rs_type_t *slice = rs_type_new(w->arena, RT_ARRAY);
            if (slice)
                slice->inner = rs_seq_element(peeled);
            return slice ? slice : &RS_UNKNOWN_TYPE;
        }
        return rs_seq_element(peeled);
    }
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_deref(rs_walk_t *w, TSNode node) {
    const rs_type_t *inner = ts_node_named_child_count(node)
                                 ? rs_walk_expr(w, ts_node_named_child(node, 0))
                                 : &RS_UNKNOWN_TYPE;
    const char *text = rs_text(&w->b, node);
    if (!text || text[0] != '*')
        return text && text[0] == '-' ? inner : &RS_UNKNOWN_TYPE;
    if (inner->kind == RT_REF)
        return inner->inner ? inner->inner : &RS_UNKNOWN_TYPE;
    if ((rs_deref_wrapper(inner) || rs_ext_is(inner, RS_RAW_POINTER)) && inner->nargs)
        return inner->args[0];
    if (inner->kind == RT_ITEM) {
        const rs_type_t *target = rs_item_deref(w, inner);
        return target ? target : &RS_UNKNOWN_TYPE;
    }
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_literal_type(rs_walk_t *w, const char *name) {
    rs_type_t *type = rs_type_new(w->arena, RT_PRIM);
    if (type)
        type->name = name;
    return type ? type : &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_integer(rs_walk_t *w, TSNode node) {
    const char *text = rs_text(&w->b, node);
    static const char *const suffixes[] = {"i8",  "i16", "i32", "i64",  "i128",  "isize", "u8",
                                           "u16", "u32", "u64", "u128", "usize", NULL};
    for (const char *const *suffix = suffixes; text && *suffix; suffix++) {
        size_t size = strlen(*suffix), length = strlen(text);
        if (length > size && !strcmp(text + length - size, *suffix))
            return rs_literal_type(w, *suffix);
    }
    return &RS_UNKNOWN_TYPE;
}

static const rs_type_t *rs_walk_expr_inner(rs_walk_t *w, TSNode node, const char *kind) {
    if (!strcmp(kind, "call_expression"))
        return rs_walk_call(w, node);
    if (!strcmp(kind, "field_expression"))
        return rs_walk_field(w, node);
    if (!strcmp(kind, "identifier")) {
        const char *name = rs_text(&w->b, node);
        const rs_type_t *local_type = &RS_UNKNOWN_TYPE;
        bool is_local = false;
        rs_sel_t sel = name ? rs_value_name(w, name, &local_type, &is_local)
                            : rs_sel_open("rust_name_missing");
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(node), ts_node_end_byte(node), &sel);
        if (is_local)
            return local_type;
        return sel.value_type ? sel.value_type : &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "self")) {
        const rs_binding_t *binding = rs_local(w, "self");
        return binding ? binding->type : &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "scoped_identifier")) {
        rs_sel_t sel = rs_select_callee(w, node);
        rs_emit(w, CBM_RUST_SITE_REF, ts_node_start_byte(node), ts_node_end_byte(node), &sel);
        return sel.value_type ? sel.value_type : &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "block"))
        return rs_walk_block(w, node);
    if (!strcmp(kind, "unsafe_block") || !strcmp(kind, "async_block") ||
        !strcmp(kind, "const_block") || !strcmp(kind, "gen_block") || !strcmp(kind, "try_block")) {
        const rs_type_t *inner = &RS_UNKNOWN_TYPE;
        rs_children_t children = rs_children(node);
        TSNode child;
        while (rs_children_next(&children, &child))
            inner = rs_walk_expr(w, child);
        return !strcmp(kind, "unsafe_block") || !strcmp(kind, "const_block") ? inner
                                                                             : &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "reference_expression")) {
        const rs_type_t *inner = rs_walk_expr(w, rs_field(node, "value"));
        return rs_ref_of(w->arena, inner);
    }
    if (rs_is_expression_macro(w, node)) {
        /* `name = value` among format arguments binds only inside the macro. */
        rs_children_t arguments = rs_children(node);
        TSNode argument;
        while (rs_children_next(&arguments, &argument))
            (void)rs_walk_expr(w, rs_is(argument, "assignment_expression") &&
                                          rs_is(rs_field(argument, "left"), "identifier")
                                      ? rs_field(argument, "right")
                                      : argument);
        return &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "parenthesized_expression"))
        return ts_node_named_child_count(node) ? rs_walk_expr(w, ts_node_named_child(node, 0))
                                               : &RS_UNKNOWN_TYPE;
    if (!strcmp(kind, "unary_expression"))
        return rs_walk_deref(w, node);
    if (!strcmp(kind, "try_expression")) {
        const rs_type_t *inner = ts_node_named_child_count(node)
                                     ? rs_peel(rs_walk_expr(w, ts_node_named_child(node, 0)))
                                     : &RS_UNKNOWN_TYPE;
        if (rs_ext_is(inner, "std::option::Option") || rs_ext_is_result(inner))
            return rs_arg(inner, 0);
        return &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "await_expression")) {
        const rs_type_t *inner = ts_node_named_child_count(node)
                                     ? rs_walk_expr(w, ts_node_named_child(node, 0))
                                     : &RS_UNKNOWN_TYPE;
        return inner->kind == RT_FUTURE ? inner->inner : &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "type_cast_expression")) {
        (void)rs_walk_expr(w, rs_field(node, "value"));
        TSNode annotation = rs_field(node, "type");
        rs_walk_type(w, annotation);
        rs_env_t env = rs_env_here(w);
        return rs_lower(&env, rs_tsyn_from(&w->b, annotation));
    }
    if (!strcmp(kind, "struct_expression"))
        return rs_walk_struct_expr(w, node);
    if (!strcmp(kind, "if_expression"))
        return rs_walk_if(w, node);
    if (!strcmp(kind, "match_expression"))
        return rs_walk_match(w, node);
    if (!strcmp(kind, "for_expression"))
        return rs_walk_for(w, node);
    if (!strcmp(kind, "while_expression")) {
        rs_scope_t scope;
        rs_scope_push(w, &scope, false);
        rs_walk_condition(w, rs_field(node, "condition"));
        (void)rs_walk_expr(w, rs_field(node, "body"));
        rs_scope_pop(w);
        return &RS_UNIT_TYPE;
    }
    if (!strcmp(kind, "closure_expression"))
        return rs_walk_closure(w, node);
    if (!strcmp(kind, "macro_invocation"))
        return rs_walk_macro(w, node);
    if (!strcmp(kind, "index_expression"))
        return rs_walk_index(w, node);
    if (!strcmp(kind, "integer_literal"))
        return rs_walk_integer(w, node);
    if (!strcmp(kind, "boolean_literal"))
        return rs_literal_type(w, "bool");
    if (!strcmp(kind, "string_literal") || !strcmp(kind, "raw_string_literal"))
        return rs_ref_of(w->arena, rs_literal_type(w, "str"));
    if (!strcmp(kind, "char_literal") || !strcmp(kind, "float_literal") ||
        !strcmp(kind, "line_comment") || !strcmp(kind, "block_comment") || !strcmp(kind, "label") ||
        !strcmp(kind, "lifetime"))
        return &RS_UNKNOWN_TYPE;
    if (!strcmp(kind, "tuple_expression")) {
        rs_type_t *tuple = rs_type_new(w->arena, RT_TUPLE);
        uint32_t count = ts_node_named_child_count(node);
        if (tuple && count)
            tuple->args = cbm_arena_alloc(w->arena, count * sizeof(*tuple->args));
        rs_children_t children = rs_children(node);
        TSNode child;
        while (rs_children_next(&children, &child)) {
            const rs_type_t *element = rs_walk_expr(w, child);
            if (tuple && tuple->args)
                tuple->args[tuple->nargs++] = element;
        }
        return tuple ? tuple : &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "array_expression")) {
        rs_type_t *array = rs_type_new(w->arena, RT_ARRAY);
        const rs_type_t *element = NULL;
        rs_children_t children = rs_children(node);
        TSNode child;
        while (rs_children_next(&children, &child)) {
            const rs_type_t *type = rs_walk_expr(w, child);
            if (!element)
                element = type;
        }
        element = element ? element : &RS_UNKNOWN_TYPE;
        if (array)
            array->inner = element;
        return array ? array : &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "assignment_expression") || !strcmp(kind, "compound_assignment_expr")) {
        const rs_type_t *assigned = rs_walk_expr(w, rs_field(node, "right"));
        TSNode left = rs_field(node, "left");
        (void)rs_walk_expr(w, left);
        rs_binding_t *binding =
            rs_is(left, "identifier") ? rs_local(w, rs_text(&w->b, left)) : NULL;
        if (binding && binding->deferred && assigned->kind != RT_UNKNOWN) {
            binding->type = assigned;
            binding->deferred = false;
        }
        return &RS_UNIT_TYPE;
    }
    if (!strcmp(kind, "binary_expression")) {
        (void)rs_walk_expr(w, rs_field(node, "left"));
        (void)rs_walk_expr(w, rs_field(node, "right"));
        return &RS_UNKNOWN_TYPE;
    }
    if (!strcmp(kind, "generic_function")) {
        /* `foo::<T>` as a value. */
        TSNode args = rs_field(node, "type_arguments");
        if (!ts_node_is_null(args))
            rs_walk_type(w, args);
        return rs_walk_expr(w, rs_field(node, "function"));
    }
    if (!strcmp(kind, "return_expression") || !strcmp(kind, "break_expression")) {
        (void)rs_walk_children(w, node);
        rs_type_t *never = rs_type_new(w->arena, RT_NEVER);
        return never ? never : &RS_UNKNOWN_TYPE;
    }
    return rs_walk_children(w, node);
}

static const rs_type_t *rs_walk_expr(rs_walk_t *w, TSNode node) {
    if (ts_node_is_null(node) || !ts_node_is_named(node) || w->failed)
        return &RS_UNKNOWN_TYPE;
    if (w->depth > RS_MAX_WALK_DEPTH)
        return &RS_UNKNOWN_TYPE;
    w->depth++;
    const rs_type_t *type = rs_walk_expr_inner(w, node, ts_node_type(node));
    w->depth--;
    return type ? type : &RS_UNKNOWN_TYPE;
}

static bool rs_is_item_kind(const char *kind) {
    static const char *const items[] = {"function_item",    "function_signature_item",
                                        "struct_item",      "enum_item",
                                        "union_item",       "trait_item",
                                        "type_item",        "const_item",
                                        "static_item",      "impl_item",
                                        "mod_item",         "use_declaration",
                                        "macro_definition", "extern_crate_declaration",
                                        "foreign_mod_item", NULL};
    return rs_name_in(kind, items);
}

/* `enum X`, `fn y` in a macro's arguments: the expansion likely declares that item. */
static void rs_note_macro_items(rs_walk_t *w, TSNode tree, const char ***out, int *count,
                                int *capacity, unsigned depth) {
    if (depth > RS_MAX_WALK_DEPTH)
        return;
    static const char *const keywords[] = {"enum", "struct", "fn",    "const", "static",
                                           "type", "trait",  "union", "mod",   NULL};
    TSTreeCursor cursor = ts_tree_cursor_new(tree);
    const char *before = NULL;
    bool more = ts_tree_cursor_goto_first_child(&cursor);
    for (; more; more = ts_tree_cursor_goto_next_sibling(&cursor)) {
        TSNode child = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "token_tree")) {
            rs_note_macro_items(w, child, out, count, capacity, depth + 1);
        } else if (!strcmp(kind, "identifier") && before && rs_name_in(before, keywords)) {
            if (*count == *capacity) {
                int grown = *capacity ? *capacity * 2 : 4;
                const char **list = cbm_arena_alloc(w->arena, (size_t)grown * sizeof(*list));
                if (!list)
                    break;
                if (*count)
                    memcpy(list, *out, (size_t)*count * sizeof(*list));
                *out = list;
                *capacity = grown;
            }
            (*out)[(*count)++] = rs_text(&w->b, child);
        }
        before = kind;
    }
    ts_tree_cursor_delete(&cursor);
}

static TSNode rs_statement_macro(TSNode child) {
    if (rs_is(child, "macro_invocation"))
        return child;
    if (rs_is(child, "expression_statement") && ts_node_named_child_count(child) &&
        rs_is(ts_node_named_child(child, 0), "macro_invocation"))
        return ts_node_named_child(child, 0);
    return (TSNode){0};
}

/* Items and uses of a block form a scope for the whole block, wherever they
 * stand in it. Impl blocks attach only to items declared in this function. */
static rs_inst_t *rs_block_scope(rs_walk_t *w, TSNode block) {
    bool any = false;
    const char **macro_items = NULL;
    int nmacro_items = 0, cap_macro_items = 0;
    rs_children_t statements = rs_children(block);
    TSNode child;
    while (rs_children_next(&statements, &child)) {
        any = any || rs_is_item_kind(ts_node_type(child));
        TSNode invocation = rs_statement_macro(child);
        if (!ts_node_is_null(invocation)) {
            TSNode tree = rs_macro_tree(invocation);
            if (!ts_node_is_null(tree))
                rs_note_macro_items(w, tree, &macro_items, &nmacro_items, &cap_macro_items, 0);
        }
    }
    if (!any && !nmacro_items)
        return NULL;
    rs_mod_t *mod = cbm_arena_calloc(w->arena, sizeof(*mod));
    rs_inst_t *inst = cbm_arena_calloc(w->arena, sizeof(*inst));
    if (!mod || !inst) {
        w->failed = true;
        return NULL;
    }
    mod->file = w->b.file;
    mod->is_block = true;
    mod->macro_items = macro_items;
    mod->nmacro_items = nmacro_items;
    mod->parent = w->inst->syn;
    mod->start = ts_node_start_byte(block);
    mod->end = ts_node_end_byte(block);
    rs_builder_t b = w->b;
    b.block_scope = true;
    /* Lines in a re-parsed macro argument are not this file's: no graph identity. */
    if (w->site_delta != 0)
        b.file = NULL;
    rs_collect_items(&b, mod, block);
    rs_mod_seal(w->arena, mod);
    inst->crate = w->inst->crate;
    inst->syn = mod;
    inst->parent = w->inst;
    inst->dir = w->inst->dir;
    mod->instances = cbm_arena_alloc(w->arena, sizeof(*mod->instances));
    if (mod->instances) {
        mod->instances[0] = inst;
        mod->ninstances = 1;
    }
    /* Impl blocks of block-local types settle against this scope. */
    for (rs_impl_t *impl = mod->impls; impl; impl = impl->next) {
        rs_tsyn_t *self_syn = impl->self_syn;
        while (self_syn && self_syn->kind == RS_TS_REF)
            self_syn = self_syn->inner;
        if (!self_syn || self_syn->kind != RS_TS_PATH || self_syn->nseg != 1)
            continue;
        rs_ent_t ent = rs_lookup_ns(&w->q, inst, self_syn->segs[0], RS_NS_TYPE, inst);
        if (ent.kind == RS_ENT_ITEM && ent.item->owner == mod) {
            if (impl->trait_syn && impl->trait_syn->kind == RS_TS_PATH) {
                rs_inst_t *blocks[1] = {inst};
                int consumed = 0;
                rs_ent_t trait = rs_resolve_path(&w->q, inst, blocks, 1, impl->trait_syn->segs,
                                                 impl->trait_syn->nseg, impl->trait_syn->absolute,
                                                 RS_NS_TYPE, false, NULL, &consumed);
                if (trait.kind == RS_ENT_ITEM && trait.item->kind == RS_ITEM_TRAIT)
                    impl->trait_item = trait.item;
                else
                    impl->trait_external = true;
            }
            rs_attach_impl(ent.item, impl);
        }
    }
    return inst;
}

static void rs_walk_let(rs_walk_t *w, TSNode node, bool has_cfg) {
    TSNode annotation = rs_field(node, "type");
    TSNode value = rs_field(node, "value");
    const rs_type_t *type = NULL;
    if (!ts_node_is_null(annotation)) {
        rs_walk_type(w, annotation);
        rs_env_t env = rs_env_here(w);
        type = rs_lower(&env, rs_tsyn_from(&w->b, annotation));
    }
    if (!ts_node_is_null(value)) {
        w->collected = NULL;
        const rs_type_t *value_type = rs_walk_expr(w, value);
        if (!type || type->kind == RT_UNKNOWN)
            type = value_type;
        else if (w->collected && w->collected_end == ts_node_end_byte(value) &&
                 w->collected_delta == w->site_delta)
            type = rs_collect_into(w->arena, type, w->collected, 0);
    }
    TSNode alternative = rs_field(node, "alternative");
    if (!ts_node_is_null(alternative))
        (void)rs_walk_expr(w, alternative);
    /* Two cfg-gated lets of one name may differ in type. */
    TSNode pattern = rs_field(node, "pattern");
    rs_bind_pattern(w, pattern, has_cfg ? &RS_UNKNOWN_TYPE : type);
    if (!type && !has_cfg && rs_is(pattern, "identifier") && w->scope && w->scope->bindings)
        w->scope->bindings->deferred = true;
}

static void rs_walk_block_item(rs_walk_t *w, TSNode node, const char *kind) {
    if (!strcmp(kind, "function_item")) {
        rs_walk_fn(w, node);
    } else if (!strcmp(kind, "impl_item")) {
        rs_walk_impl(w, node);
    } else if (!strcmp(kind, "trait_item")) {
        rs_walk_trait(w, node);
    } else if (!strcmp(kind, "struct_item") || !strcmp(kind, "enum_item") ||
               !strcmp(kind, "union_item") || !strcmp(kind, "type_item") ||
               !strcmp(kind, "const_item") || !strcmp(kind, "static_item")) {
        rs_walk_type(w, rs_field(node, "type"));
        TSNode body = rs_field(node, "body");
        if (!ts_node_is_null(body))
            rs_walk_type(w, body);
        TSNode value = rs_field(node, "value");
        if (!ts_node_is_null(value))
            (void)rs_walk_expr(w, value);
    }
}

static const rs_type_t *rs_walk_block(rs_walk_t *w, TSNode node) {
    rs_scope_t scope;
    rs_scope_push(w, &scope, false);
    rs_inst_t *block = rs_block_scope(w, node);
    bool pushed = block && w->nblocks < RS_MAX_BLOCKS;
    if (pushed)
        w->blocks[w->nblocks++] = block;
    const rs_type_t *tail = &RS_UNIT_TYPE;
    bool has_cfg = false;
    rs_children_t statements = rs_children(node);
    TSNode child;
    while (rs_children_next(&statements, &child)) {
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "line_comment") || !strcmp(kind, "block_comment"))
            continue;
        if (!strcmp(kind, "attribute_item")) {
            const char *text = rs_text(&w->b, child);
            has_cfg = has_cfg || (text && strstr(text, "cfg"));
            continue;
        }
        tail = &RS_UNIT_TYPE;
        if (!strcmp(kind, "let_declaration")) {
            rs_walk_let(w, child, has_cfg);
        } else if (!strcmp(kind, "expression_statement")) {
            if (ts_node_named_child_count(child))
                (void)rs_walk_expr(w, ts_node_named_child(child, 0));
        } else if (rs_is_item_kind(kind)) {
            rs_walk_block_item(w, child, kind);
        } else {
            tail = rs_walk_expr(w, child);
        }
        has_cfg = false;
    }
    if (pushed)
        w->nblocks--;
    rs_scope_pop(w);
    return tail;
}

/* ── Items ─────────────────────────────────────────────────────────── */

static void rs_push_generics(rs_walk_t *w, TSNode owner) {
    rs_genv_t *gens = cbm_arena_alloc(w->arena, sizeof(*gens));
    if (!gens) {
        w->failed = true;
        return;
    }
    gens->generics = rs_generics_from(&w->b, owner);
    gens->up = w->gens;
    w->gens = gens;
}

static void rs_walk_generics(rs_walk_t *w, TSNode owner) {
    TSNode params = rs_field(owner, "type_parameters");
    if (!ts_node_is_null(params))
        rs_walk_type(w, params);
    for (uint32_t i = 0; i < ts_node_named_child_count(owner); i++) {
        TSNode child = ts_node_named_child(owner, i);
        if (rs_is(child, "where_clause"))
            rs_walk_type(w, child);
    }
}

static void rs_walk_fn(rs_walk_t *w, TSNode node) {
    const rs_genv_t *saved_gens = w->gens;
    rs_push_generics(w, node);
    rs_scope_t scope;
    rs_scope_push(w, &scope, true);
    rs_walk_generics(w, node);
    TSNode params = rs_field(node, "parameters");
    for (uint32_t i = 0; !ts_node_is_null(params) && i < ts_node_named_child_count(params); i++) {
        TSNode param = ts_node_named_child(params, i);
        if (rs_is(param, "self_parameter")) {
            const char *text = rs_text(&w->b, param);
            bool reference = text && strchr(text, '&');
            const rs_type_t *self_ty = w->self_ty ? w->self_ty : &RS_UNKNOWN_TYPE;
            rs_bind(w, "self", reference ? rs_ref_of(w->arena, self_ty) : self_ty);
        } else if (rs_is(param, "parameter")) {
            TSNode annotation = rs_field(param, "type");
            rs_walk_type(w, annotation);
            rs_env_t env = rs_env_here(w);
            const rs_type_t *type = rs_lower(&env, rs_tsyn_from(&w->b, annotation));
            TSNode pattern = rs_field(param, "pattern");
            if (rs_is(pattern, "self"))
                rs_bind(w, "self", type);
            else
                rs_bind_pattern(w, pattern, type);
        }
    }
    TSNode ret = rs_field(node, "return_type");
    if (!ts_node_is_null(ret))
        rs_walk_type(w, ret);
    TSNode body = rs_field(node, "body");
    if (!ts_node_is_null(body))
        (void)rs_walk_expr(w, body);
    rs_scope_pop(w);
    w->gens = saved_gens;
}

static void rs_walk_impl_body(rs_walk_t *w, TSNode body) {
    rs_children_t members = rs_children(body);
    TSNode child;
    while (rs_children_next(&members, &child)) {
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "function_item") || !strcmp(kind, "function_signature_item")) {
            rs_walk_fn(w, child);
        } else if (!strcmp(kind, "const_item")) {
            rs_walk_type(w, rs_field(child, "type"));
            TSNode value = rs_field(child, "value");
            if (!ts_node_is_null(value))
                (void)rs_walk_expr(w, value);
        } else if (!strcmp(kind, "type_item") || !strcmp(kind, "associated_type")) {
            rs_walk_type(w, rs_field(child, "type"));
            rs_walk_type(w, rs_field(child, "bounds"));
        }
    }
}

/* The impl at this byte with this self type, first in list order. */
static const rs_impl_t *rs_impl_at(const rs_mod_t *mod, uint32_t start, const char *text) {
    if (!mod->impl_rows || !text)
        return NULL;
    int low = 0, high = mod->nimpl_rows;
    while (low < high) {
        int mid = low + (high - low) / 2;
        if (mod->impl_rows[mid].impl->start < start)
            low = mid + 1;
        else
            high = mid;
    }
    for (; low < mod->nimpl_rows && mod->impl_rows[low].impl->start == start; low++) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        const rs_impl_t *at = mod->impl_rows[low].impl;
        if (at->self_syn && at->self_syn->text && !strcmp(at->self_syn->text, text))
            return at;
    }
    return NULL;
}

static void rs_walk_impl(rs_walk_t *w, TSNode node) {
    const rs_genv_t *saved_gens = w->gens;
    rs_push_generics(w, node);
    const rs_type_t *saved_self = w->self_ty;
    const rs_impl_t *saved_impl = w->impl;
    rs_walk_generics(w, node);
    TSNode type = rs_field(node, "type"), trait = rs_field(node, "trait");
    rs_walk_type(w, type);
    if (!ts_node_is_null(trait))
        rs_walk_type(w, trait);
    rs_tsyn_t *self_syn = rs_tsyn_from(&w->b, type);
    rs_env_t env = rs_env_here(w);
    env.self_ty = NULL;
    w->self_ty = rs_lower(&env, self_syn);
    /* The impl's own `type Target = X` answers Self::Target inside it. */
    rs_mod_t *owner = rs_mod_at(w->b.file->root, ts_node_start_byte(node), ts_node_end_byte(node));
    uint32_t start = ts_node_start_byte(node);
    const rs_impl_t *found = owner && self_syn ? rs_impl_at(owner, start, self_syn->text) : NULL;
    for (const rs_impl_t *at = owner && !found ? owner->impls : NULL; at && self_syn;
         at = at->next) {
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
        if (at->self_syn && at->self_syn->text && self_syn->text &&
            !strcmp(at->self_syn->text, self_syn->text) && at->start == start) {
            found = at;
            break;
        }
    }
    w->impl = found;
    rs_walk_impl_body(w, rs_field(node, "body"));
    w->self_ty = saved_self;
    w->impl = saved_impl;
    w->gens = saved_gens;
}

static void rs_walk_trait(rs_walk_t *w, TSNode node) {
    const rs_genv_t *saved_gens = w->gens;
    rs_push_generics(w, node);
    const rs_type_t *saved_self = w->self_ty;
    const rs_impl_t *saved_impl = w->impl;
    w->impl = NULL;
    rs_walk_generics(w, node);
    rs_walk_type(w, rs_field(node, "bounds"));
    const char *name = rs_text(&w->b, rs_field(node, "name"));
    rs_ent_t ent =
        rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name ? name : "", RS_NS_TYPE);
    rs_type_t *self_ty = rs_type_new(w->arena, RT_PARAM);
    if (self_ty) {
        self_ty->name = "Self";
        if (ent.kind == RS_ENT_ITEM && ent.item->kind == RS_ITEM_TRAIT) {
            self_ty->traits = cbm_arena_alloc(w->arena, sizeof(*self_ty->traits));
            if (self_ty->traits) {
                self_ty->traits[0] = ent.item;
                self_ty->ntraits = 1;
            }
        }
    }
    w->self_ty = self_ty;
    rs_walk_impl_body(w, rs_field(node, "body"));
    w->self_ty = saved_self;
    w->impl = saved_impl;
    w->gens = saved_gens;
}

static void rs_walk_data_item(rs_walk_t *w, TSNode node) {
    const rs_genv_t *saved_gens = w->gens;
    rs_push_generics(w, node);
    const rs_type_t *saved_self = w->self_ty;
    rs_walk_generics(w, node);
    /* Inside a struct or enum definition, Self names that type. */
    const char *name = rs_text(&w->b, rs_field(node, "name"));
    rs_ent_t ent =
        rs_lookup_lexical(&w->q, w->inst, w->blocks, w->nblocks, name ? name : "", RS_NS_TYPE);
    if (ent.kind == RS_ENT_ITEM) {
        rs_env_t env = rs_env_here(w);
        w->self_ty = rs_lower_item(&env, ent.item, NULL);
    }
    TSNode body = rs_field(node, "body");
    if (rs_is(body, "enum_variant_list")) {
        rs_children_t variants = rs_children(body);
        TSNode variant;
        while (rs_children_next(&variants, &variant)) {
            TSNode payload = rs_field(variant, "body");
            if (!ts_node_is_null(payload))
                rs_walk_type(w, payload);
        }
    } else if (!ts_node_is_null(body)) {
        rs_walk_type(w, body);
    }
    rs_walk_type(w, rs_field(node, "type"));
    w->self_ty = saved_self;
    w->gens = saved_gens;
}

static void rs_walk_inline_mod(rs_walk_t *w, TSNode node) {
    const char *name = rs_text(&w->b, rs_field(node, "name"));
    TSNode body = rs_field(node, "body");
    if (!name || ts_node_is_null(body))
        return;
    /* Inline modules were instantiated with their parent. Same-named cfg twins
     * are matched by their span. */
    uint32_t start = ts_node_start_byte(node);
    int row = -1;
    for (const rs_child_t *child = rs_child_first(w->inst, name, &row); child;
         child = rs_child_after(w->inst, child, &row)) {
        if (!child->inst || strcmp(child->name, name) != 0 || !child->inst->syn->is_inline ||
            child->inst->syn->start != start)
            continue;
        rs_inst_t *saved = w->inst;
        const rs_type_t *saved_self = w->self_ty;
        const rs_genv_t *saved_gens = w->gens;
        w->inst = child->inst;
        w->self_ty = NULL;
        w->gens = NULL;
        rs_walk_items(w, body);
        w->inst = saved;
        w->self_ty = saved_self;
        w->gens = saved_gens;
        return;
    }
}

static void rs_walk_items(rs_walk_t *w, TSNode container) {
    rs_children_t items = rs_children(container);
    TSNode node;
    while (rs_children_next(&items, &node)) {
        if (w->failed) {
            rs_children_close(&items);
            return;
        }
        const char *kind = ts_node_type(node);
        if (!strcmp(kind, "function_item") || !strcmp(kind, "function_signature_item")) {
            rs_walk_fn(w, node);
        } else if (!strcmp(kind, "impl_item")) {
            rs_walk_impl(w, node);
        } else if (!strcmp(kind, "trait_item")) {
            rs_walk_trait(w, node);
        } else if (!strcmp(kind, "struct_item") || !strcmp(kind, "enum_item") ||
                   !strcmp(kind, "union_item") || !strcmp(kind, "type_item")) {
            rs_walk_data_item(w, node);
        } else if (!strcmp(kind, "const_item") || !strcmp(kind, "static_item")) {
            rs_walk_type(w, rs_field(node, "type"));
            TSNode value = rs_field(node, "value");
            if (!ts_node_is_null(value))
                (void)rs_walk_expr(w, value);
        } else if (!strcmp(kind, "mod_item")) {
            rs_walk_inline_mod(w, node);
        } else if (!strcmp(kind, "foreign_mod_item")) {
            rs_walk_items(w, rs_field(node, "body"));
        } else if (!strcmp(kind, "macro_invocation")) {
            (void)rs_walk_macro(w, node);
        } else if (!strcmp(kind, "expression_statement") && ts_node_named_child_count(node)) {
            /* `make_things! { .. };` at module level. */
            (void)rs_walk_expr(w, ts_node_named_child(node, 0));
        }
    }
}

/* ── File analysis ─────────────────────────────────────────────────── */

static int rs_site_compare(const void *left, const void *right) {
    const cbm_rust_site_t *a = left, *b = right;
    if (a->start != b->start)
        return a->start < b->start ? -1 : 1;
    if (a->end != b->end)
        return a->end < b->end ? -1 : 1;
    if (a->kind != b->kind)
        return a->kind < b->kind ? -1 : 1;
    return 0;
}

static bool rs_site_settled_target(const cbm_rust_site_t *site) {
    return site->target != NULL;
}

/* An instance that could not see a name does not outvote one that could. Two
 * different declarations at one site are no single binding. */
static void rs_site_merge(cbm_rust_site_t *into, const cbm_rust_site_t *other) {
    if (into->binding == CBM_RESOLVE_BINDING_AMBIGUOUS)
        return;
    if (other->binding == CBM_RESOLVE_BINDING_AMBIGUOUS) {
        *into = *other;
        return;
    }
    if (!rs_site_settled_target(into)) {
        if (rs_site_settled_target(other))
            *into = *other;
        return;
    }
    if (rs_site_settled_target(other) && strcmp(into->target, other->target) != 0) {
        into->target = NULL;
        into->label = NULL;
        into->facts = 0;
        into->binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
        into->domain = CBM_RESOLVE_DOMAIN_UNKNOWN;
        into->reason = "rust_crate_instances_disagree";
    }
}

cbm_rust_file_analysis_t *cbm_rust_analyze_file(const cbm_rust_project_t *project, CBMArena *arena,
                                                const char *rel, const char *source, size_t length,
                                                TSNode root) {
    cbm_rust_file_analysis_t *analysis = arena ? cbm_arena_calloc(arena, sizeof(*analysis)) : NULL;
    if (!analysis)
        return NULL;
    rs_file_t *file = project ? rs_file_find(project, rel) : NULL;
    if (!file || !file->root->ninstances)
        return analysis;
    CBMHashTable *memo = cbm_ht_create(256);
    if (!memo)
        return NULL;
    cbm_rust_site_t *all = NULL;
    size_t total = 0, capacity = 0;
    bool ok = true;
    for (int i = 0; ok && i < file->root->ninstances; i++) {
        rs_walk_t w;
        memset(&w, 0, sizeof(w));
        w.project = project;
        w.arena = arena;
        w.b = (rs_builder_t){.arena = arena, .source = source, .length = length, .file = file};
        w.q = (rs_query_t){.project = project, .arena = arena, .memo = memo};
        w.inst = file->root->instances[i];
        cbm_syntax_index_t syntax;
        cbm_syntax_index_begin(&syntax, root.tree);
        rs_walk_items(&w, root);
        cbm_syntax_index_end(&syntax);
        cbm_ht_free(w.locals);
        if (w.parser)
            ts_parser_delete(w.parser);
        ok = !w.failed;
        if (ok && w.nsites) {
            if (total + w.nsites > capacity) {
                size_t grown = (total + w.nsites) * 2;
                cbm_rust_site_t *sites = cbm_arena_alloc(arena, grown * sizeof(*sites));
                ok = sites != NULL;
                if (ok) {
                    if (total)
                        memcpy(sites, all, total * sizeof(*sites));
                    all = sites;
                    capacity = grown;
                }
            }
            if (ok) {
                memcpy(all + total, w.sites, w.nsites * sizeof(*all));
                total += w.nsites;
            }
        }
    }
    cbm_ht_free(memo);
    if (!ok)
        return NULL;
    if (total > 1)
        qsort(all, total, sizeof(*all), rs_site_compare);
    size_t unique = 0;
    for (size_t i = 0; i < total; i++) {
        if (unique && rs_site_compare(&all[unique - 1], &all[i]) == 0)
            rs_site_merge(&all[unique - 1], &all[i]);
        else
            all[unique++] = all[i];
    }
    analysis->sites = all;
    analysis->nsites = unique;
    return analysis;
}

const cbm_rust_site_t *cbm_rust_site_find(const cbm_rust_file_analysis_t *analysis,
                                          cbm_rust_site_kind_t kind, uint32_t start, uint32_t end) {
    if (!analysis || !analysis->nsites)
        return NULL;
    cbm_rust_site_t key = {.start = start, .end = end, .kind = kind};
    return bsearch(&key, analysis->sites, analysis->nsites, sizeof(*analysis->sites),
                   rs_site_compare);
}
