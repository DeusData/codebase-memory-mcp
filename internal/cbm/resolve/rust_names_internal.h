#ifndef CBM_RESOLVE_RUST_NAMES_INTERNAL_H
#define CBM_RESOLVE_RUST_NAMES_INTERNAL_H

#include "../lsp/lsp_name_rows.h"
#include "stats.h"
#include "rust_names.h"
#include "../arena.h"
#include "foundation/hash_table.h"

/* Rust keeps three separate namespaces: one name can be a module, a function
 * and a macro in one scope without conflict. */
enum { RS_NS_TYPE = 1u << 0, RS_NS_VALUE = 1u << 1, RS_NS_MACRO = 1u << 2 };

typedef enum {
    RS_ITEM_MOD = 0,
    RS_ITEM_FN,
    RS_ITEM_STRUCT,
    RS_ITEM_ENUM,
    RS_ITEM_UNION,
    RS_ITEM_TRAIT,
    RS_ITEM_TYPE,
    RS_ITEM_CONST,
    RS_ITEM_STATIC,
    RS_ITEM_MACRO
} rs_item_kind_t;

typedef enum { RS_VIS_PRIVATE = 0, RS_VIS_PUB, RS_VIS_CRATE, RS_VIS_SUPER } rs_vis_t;

typedef struct rs_mod rs_mod_t;
typedef struct rs_item rs_item_t;
typedef struct rs_inst rs_inst_t;
typedef struct rs_crate rs_crate_t;
typedef struct rs_file rs_file_t;
typedef struct rs_impl rs_impl_t;
typedef struct rs_fn rs_fn_t;
typedef struct rs_tsyn rs_tsyn_t;
typedef struct rs_type rs_type_t;

/* Type syntax, kept independent of the parse tree so another file's signature
 * can be read after that file's tree is gone. */
typedef enum {
    RS_TS_UNKNOWN = 0,
    RS_TS_PATH,
    RS_TS_PRIM,
    RS_TS_REF,
    RS_TS_PTR, /* *const T and *mut T */
    RS_TS_TUPLE,
    RS_TS_ARRAY,
    RS_TS_UNIT,
    RS_TS_DYN, /* dyn A + B and impl A + B: a value known only by its traits */
    RS_TS_NEVER,
    RS_TS_FN /* Fn(A, B) -> R as a bound: args are parameters, inner the result */
} rs_tsyn_kind_t;

struct rs_tsyn {
    rs_tsyn_kind_t kind;
    const char **segs; /* path segments, including crate/self/super */
    int nseg;
    bool absolute;    /* ::a::b */
    const char *prim; /* RS_TS_PRIM */
    rs_tsyn_t **args; /* generic arguments, tuple elements or dyn bounds */
    int nargs;
    rs_tsyn_t *inner;        /* RS_TS_REF, RS_TS_ARRAY, RS_TS_FN result */
    rs_tsyn_t *item_binding; /* the T of Iterator<Item = T> */
    const char *text;        /* source spelling, to compare cfg twins */
};

typedef struct {
    const char **names;
    rs_tsyn_t ***bounds; /* per name: trait bounds */
    int *nbounds;
    int count;
} rs_generics_t;

typedef struct rs_field {
    const char *name;
    rs_tsyn_t *type;
    const char *qn;
    const char *label;
    rs_vis_t vis;
    bool pinned; /* #[pin]: the macro's projection gives Pin<&mut F> */
    struct rs_field *next;
} rs_field_t;

typedef struct rs_variant {
    const char *name;
    rs_field_t *fields;
    bool tuple;
    struct rs_variant *next;
} rs_variant_t;

typedef enum { RS_SELF_NONE = 0, RS_SELF_VALUE, RS_SELF_REF, RS_SELF_MUT } rs_self_kind_t;

struct rs_fn {
    const char *name;
    const char *qn;
    const char *label;
    bool qn_conflict;
    /* Another fn of this file shares the QN (cfg twins, From<A> and From<B>): one graph
     * identity, but its signature is not this fn's alone. */
    bool qn_twin;
    /* Every twin writes the same return type, and none implements a trait: `cfg_rt! { fn current()
     * -> Handle }` beside `cfg_not_rt! { fn current() -> Handle }`. */
    bool twin_return_agrees;
    /* A trait impl's `fn m(&self) { self.m() }`. That is no recursion: the self type has an
     * inherent m, which a method call prefers. */
    bool delegates_to_own_name;
    rs_vis_t vis;
    rs_self_kind_t self_kind;
    bool is_async;
    rs_generics_t *generics;
    rs_tsyn_t **params; /* parameter types, excluding self */
    int nparams;
    rs_tsyn_t *ret; /* NULL means unit */
    rs_mod_t *owner;
    rs_impl_t *impl;  /* owning impl block */
    rs_item_t *trait; /* owning trait */
    rs_fn_t *next;    /* next in impl, trait or item chain */
};

/* `const ALL: Ready = ...` in an impl or a trait. */
typedef struct rs_assoc_const {
    const char *name;
    rs_tsyn_t *type;
    struct rs_assoc_const *next;
} rs_assoc_const_t;

struct rs_impl {
    rs_generics_t *generics;
    rs_tsyn_t *self_syn;
    rs_tsyn_t *trait_syn; /* NULL for an inherent impl */
    rs_mod_t *owner;
    rs_fn_t *methods;
    rs_assoc_const_t *consts;
    const rs_assoc_const_t **sorted_consts; /* by name, after sealing the module */
    int nconsts;
    /* name -> type pairs: `type Target = X;` */
    const char **assoc_names;
    rs_tsyn_t **assoc_types;
    int nassoc;
    bool has_macro; /* macro-generated members are not enumerable */
    uint32_t start;
    /* settled at seal */
    rs_item_t *trait_item;
    bool trait_external;
    const char *ext_path;      /* self type outside the project */
    struct rs_impl *next;      /* next in module */
    struct rs_impl *next_self; /* next impl with the same self item */
};

/* The scope of a path's rows in ext_rows. Two paths may share one; a reader compares paths. */
static inline const void *rs_ext_scope(const char *path) {
    uint64_t hash = UINT64_C(0xCBF29CE484222325);
    for (const char *at = path; *at; at++)
        hash = (hash ^ (unsigned char)*at) * UINT64_C(0x100000001B3);
    return (const void *)(uintptr_t)hash;
}

typedef struct rs_impl_row {
    const struct rs_impl *impl;
    int position;
} rs_impl_row_t;

struct rs_item {
    rs_assoc_const_t *consts; /* a trait's */
    const rs_assoc_const_t **sorted_consts;
    int nconsts;
    const char *name;
    unsigned ns;
    rs_item_kind_t kind;
    rs_vis_t vis;
    rs_mod_t *owner;
    const char *qn;
    const char *label;
    bool qn_conflict;
    bool has_cfg;
    /* #[derive(thiserror::Error)] with #[from], derive_more::From: conversions
     * exist that no impl block in the source shows. */
    bool derives_conversion;
    /* Known only from `impl X` beside a macro: no fields, no node, and its members are not all
     * written where this model can read them. */
    bool macro_declared;
    rs_item_t *next; /* next in the owner module, sorted later */
    rs_generics_t *generics;
    /* MOD */
    rs_mod_t *inline_mod;
    const char *path_attr;
    /* STRUCT/UNION */
    rs_field_t *fields;
    bool tuple_struct, unit_struct;
    bool pin_projected; /* a #[pin] field: pin-project gave it project() and project_ref() */
    /* ENUM */
    rs_variant_t *variants;
    /* TRAIT */
    rs_fn_t *methods;
    rs_tsyn_t **supers;
    int nsupers;
    /* FN */
    rs_fn_t *fn;
    /* TYPE alias, CONST, STATIC */
    rs_tsyn_t *type;
    /* settled at seal: every impl whose self type is this item */
    rs_impl_t *impls;
};

typedef struct rs_use {
    const char *local; /* "_" and NULL (glob) bind no name */
    const char **segs;
    int nseg;
    bool absolute;
    bool glob;
    bool extern_crate; /* extern crate name [as local] */
    bool has_cfg;
    rs_vis_t vis;
    uint32_t start, end;
    struct rs_use *next;
} rs_use_t;

/* One syntactic module: a file's top level, an inline `mod x { }`, or the
 * item scope of a block. The same file reached from two crate roots has one
 * syntactic module and two instances. */
struct rs_mod {
    const char *name;
    rs_mod_t *parent; /* lexical parent within the file */
    rs_file_t *file;
    bool is_inline;
    bool is_block;
    uint32_t start, end;
    rs_item_t *item_list;
    rs_item_t **items; /* sorted by name after sealing the module */
    int nitems;
    rs_use_t *uses;  /* named leaves */
    rs_use_t *globs; /* glob leaves */
    rs_impl_t *impls;
    /* A long impls list by start byte, then list position: each impl walked looks itself up. */
    const struct rs_impl_row *impl_rows;
    int nimpl_rows;
    rs_mod_t *children; /* inline children */
    rs_mod_t *next_child;
    /* A module-level macro may expand to items that shadow a glob import, so
     * a name found only through a glob is not settled here. */
    bool has_macro_items;
    /* Names a statement macro's arguments declare (`enum X`, `fn y`): the
     * expansion's item is in scope for the whole block and hides outer names. */
    const char **macro_items;
    int nmacro_items;
    rs_inst_t **instances;
    int ninstances;
};

/* One pass over a node's named children. ts_node_named_child(i) restarts from the
 * first child, which turns a wide node (a flat file, a 100k-element array) quadratic. */
typedef struct {
    TSTreeCursor cursor;
    bool started, open;
} rs_children_t;

static inline rs_children_t rs_children(TSNode parent) {
    rs_children_t it = {.open = !ts_node_is_null(parent)};
    if (it.open)
        it.cursor = ts_tree_cursor_new(parent);
    return it;
}

/* Closes itself at the end; an early exit from the loop calls rs_children_close. */
static inline bool rs_children_next(rs_children_t *it, TSNode *child) {
    while (it->open) {
        bool moved = it->started ? ts_tree_cursor_goto_next_sibling(&it->cursor)
                                 : ts_tree_cursor_goto_first_child(&it->cursor);
        it->started = true;
        if (!moved)
            break;
        TSNode node = ts_tree_cursor_current_node(&it->cursor);
        if (ts_node_is_named(node)) {
            *child = node;
            return true;
        }
    }
    if (it->open) {
        ts_tree_cursor_delete(&it->cursor);
        it->open = false;
    }
    return false;
}

static inline void rs_children_close(rs_children_t *it) {
    if (it->open) {
        ts_tree_cursor_delete(&it->cursor);
        it->open = false;
    }
}

/* Graph identity of one extracted definition. */
typedef struct {
    uint32_t line;
    const char *name;
    const char *qn;
    const char *label;
    bool dup_qn; /* another definition of this file shares the qualified name */
} rs_def_t;

struct rs_file {
    const char *rel;
    rs_mod_t *root;
    rs_def_t *defs; /* sorted by line, then name */
    int ndefs;
    /* Where each expression macro's `(` stands, sorted: its value is not its argument's. */
    const uint32_t *expression_macros;
    int nexpression_macros;
};

typedef struct rs_child {
    const char *name;
    rs_inst_t *inst;
    bool missing_file;
    struct rs_child *next;
} rs_child_t;

/* A syntactic module at one place in one crate's module tree. */
struct rs_inst {
    rs_crate_t *crate;
    rs_mod_t *syn;
    rs_inst_t *parent;
    const char *dir; /* where this module's `mod x;` files live */
    rs_child_t *children;
    /* A long list, by name, in list order. A flat crate root can hold thousands of modules. */
    const rs_child_t **child_by_row;
    CBMNameRows child_rows;
};

/* The children to look at for a name, in list order: those of that name in a long list,
 * all of them in a short one, so a caller still compares names. */
static inline const rs_child_t *rs_child_first(const rs_inst_t *inst, const char *name, int *row) {
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
    if (!inst->child_by_row)
        return inst->children;
    *row = cbm_name_rows_lookup(&inst->child_rows, NULL, name);
    return *row >= 0 ? inst->child_by_row[*row] : NULL;
}
static inline const rs_child_t *rs_child_after(const rs_inst_t *inst, const rs_child_t *child,
                                               int *row) {
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
    if (!inst->child_by_row)
        return child->next;
    *row = inst->child_rows.next[*row];
    return *row >= 0 ? inst->child_by_row[*row] : NULL;
}

typedef struct rs_extern {
    const char *name;
    rs_crate_t *crate; /* NULL: declared dependency outside the project */
    struct rs_extern *next;
} rs_extern_t;

typedef enum { RS_CRATE_LIB = 0, RS_CRATE_BIN, RS_CRATE_AUX } rs_crate_kind_t;

typedef struct rs_package rs_package_t;

struct rs_crate {
    const char *name;
    const char *root_rel;
    rs_crate_kind_t kind;
    rs_package_t *package;
    rs_inst_t *root;
    rs_extern_t *externs;
    bool edition_2015;
    bool extern_prelude_known; /* a manifest listed its dependencies */
    bool orphan;               /* reached by no crate root: crate:: and super:: are unknown */
    /* #[macro_export] items live at the crate root whatever module declares them. */
    rs_item_t **macro_exports;
    int nmacro_exports, cap_macro_exports;
    struct rs_crate *next;
};

/* Internal view of the project. */
struct cbm_rust_project {
    CBMArena arena;
    rs_file_t **files; /* sorted by rel */
    int nfiles, cap_files;
    rs_package_t *packages;
    rs_crate_t *crates;
    CBMHashTable *ext_impls;  /* normalized path -> impls on that type outside the project */
    /* A long ext_impls by path and method, in each path's order: each std method call asks. */
    const rs_impl_t **ext_by_row;
    CBMNameRows ext_rows;
    rs_impl_t *blanket_impls; /* impl<T> Trait for T */
    CBMHashTable *patches;    /* manifest dir, separator, name -> [patch.crates-io] entry */
    bool sealed;
};

/* ── Entities, the result of a name lookup ─────────────────────────── */

typedef enum {
    RS_ENT_NONE = 0,
    RS_ENT_MODULE,
    RS_ENT_ITEM,
    RS_ENT_VARIANT,
    RS_ENT_EXTERNAL,
    RS_ENT_PRELUDE, /* std prelude: Some, Vec, drop... */
    RS_ENT_BUILTIN, /* bool, i32, str */
    RS_ENT_AMBIGUOUS,
    RS_ENT_UNKNOWN,
    RS_ENT_NOT_VISIBLE
} rs_ent_kind_t;

typedef enum { RS_VIA_NONE = 0, RS_VIA_LOCAL, RS_VIA_NAMED, RS_VIA_GLOB, RS_VIA_PATH } rs_via_t;

typedef struct {
    rs_ent_kind_t kind;
    rs_via_t via;
    rs_inst_t *module;
    rs_item_t *item;
    rs_variant_t *variant;
    const char *external_path;
    const char *reason;
} rs_ent_t;

typedef struct rs_visit {
    const rs_inst_t *inst;
    const char *name;
    unsigned ns;
    const struct rs_visit *up;
} rs_visit_t;

/* Per-thread lookup state over the immutable project. */
typedef struct {
    const cbm_rust_project_t *project;
    CBMArena *arena;
    CBMHashTable *memo; /* NULL: no memoization */
    const rs_visit_t *stack;
    bool cycle_hit;
} rs_query_t;

/* The longest resolvable prefix. *consumed < nseg means the remaining segments
 * name members of the returned type. *last_module is the deepest module named. */
rs_ent_t rs_resolve_path(rs_query_t *q, rs_inst_t *inst, rs_inst_t *const *blocks, int nblocks,
                         const char *const *segs, int nseg, bool absolute, unsigned ns, bool is_use,
                         rs_inst_t **last_module, int *consumed);
rs_ent_t rs_lookup_ns(rs_query_t *q, rs_inst_t *inst, const char *name, unsigned ns,
                      rs_inst_t *from);
rs_ent_t rs_lookup_lexical(rs_query_t *q, rs_inst_t *inst, rs_inst_t *const *blocks, int nblocks,
                           const char *name, unsigned ns);
bool rs_visible(rs_vis_t vis, const rs_mod_t *owner, const rs_inst_t *from);
rs_inst_t *rs_inst_for(const rs_mod_t *syn, const rs_crate_t *crate);
rs_inst_t *rs_module_of(rs_inst_t *inst);
rs_file_t *rs_file_find(const cbm_rust_project_t *project, const char *rel);
rs_mod_t *rs_mod_at(rs_mod_t *mod, uint32_t start, uint32_t end);
const rs_def_t *rs_def_find(const rs_file_t *file, uint32_t line, const char *name,
                            const char *const *labels);

/* Shared with the occurrence walker, which meets block-scoped items. */
typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length;
    rs_file_t *file;
    bool block_scope; /* collecting a block's items: no module-level claims ran */
    /* Sorted [start, end) pairs: items a passthrough macro wraps, which it wraps to add #[cfg]. */
    const uint32_t *wrapped;
    int nwrapped;
} rs_builder_t;

void rs_collect_items(rs_builder_t *b, rs_mod_t *mod, TSNode container);
void rs_mod_seal(CBMArena *arena, rs_mod_t *mod);
rs_tsyn_t *rs_tsyn_from(rs_builder_t *b, TSNode node);
rs_generics_t *rs_generics_from(rs_builder_t *b, TSNode owner);
const char *rs_text(rs_builder_t *b, TSNode node);
/* A sealed impl's or trait's const of this name: generated tables can hold thousands. */
const rs_assoc_const_t *rs_const_named(const rs_assoc_const_t *const *sorted, int count,
                                       const char *name);
/* The same type as written, both known: never true of syntax this model does not read. */
bool rs_tsyn_same(const rs_tsyn_t *a, const rs_tsyn_t *b);
TSNode rs_field(TSNode node, const char *name);
bool rs_is(TSNode node, const char *kind);
void rs_settle_impl(rs_query_t *q, rs_impl_t *impl, rs_inst_t *inst);

#endif
