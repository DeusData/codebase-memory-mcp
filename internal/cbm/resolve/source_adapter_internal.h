#ifndef CBM_RESOLVE_SOURCE_ADAPTER_INTERNAL_H
#define CBM_RESOLVE_SOURCE_ADAPTER_INTERNAL_H

#include "source_adapter.h"
#include "foundation/hash_table.h"
#include <string.h>

/* Declaration facts held in the project table. Every string is owned by the
 * table's arena. owner and name use the language's own identity rules, so a
 * case-insensitive language folds them before they get here. */
enum {
    CBM_SOURCE_DECL_F_PUBLIC = 1u << 0,
    CBM_SOURCE_DECL_F_PRIVATE = 1u << 1,
    CBM_SOURCE_DECL_F_PROTECTED = 1u << 2,
    CBM_SOURCE_DECL_F_STATIC = 1u << 3,
    CBM_SOURCE_DECL_F_CONDITIONAL = 1u << 4, /* existence depends on runtime control flow */
    CBM_SOURCE_DECL_F_TYPE = 1u << 5,        /* class-like owner record */
    CBM_SOURCE_DECL_F_INTERFACE = 1u << 6,
    CBM_SOURCE_DECL_F_TRAIT = 1u << 7,
    CBM_SOURCE_DECL_F_ENUM = 1u << 8,
    CBM_SOURCE_DECL_F_ADAPTED = 1u << 9,   /* trait adaptation rules are present */
    CBM_SOURCE_DECL_F_OPEN = 1u << 10,     /* the declaration domain has an unmodelled effect */
    CBM_SOURCE_DECL_F_VALUE = 1u << 11,    /* a non-callable member, such as a property */
    CBM_SOURCE_DECL_F_ABSTRACT = 1u << 12, /* declared without a body */
    CBM_SOURCE_DECL_F_QN_SHARED = 1u << 13 /* another owner's declaration has this QN */
};

/* Language-neutral parameter facts, for the languages that can state them. */
enum {
    CBM_SOURCE_PARAM_OPTIONAL = 1u << 0,
    CBM_SOURCE_PARAM_VARIADIC = 1u << 1,
    CBM_SOURCE_PARAM_BY_REF = 1u << 2
};

typedef struct {
    const char *name;
    const char *type; /* adapter-defined canonical type spelling, or NULL */
    uint32_t flags;
} cbm_source_param_t;

typedef struct cbm_source_decl {
    const char *owner;
    const char *name;
    const char *qn;           /* exact extracted graph QN; NULL means not materialized */
    const char *file;         /* declaring repository-relative path */
    const char *type;         /* adapter-defined declared value or return type, or NULL */
    const char *const *bases; /* NULL-terminated adapter-defined parent names */
    const cbm_source_param_t *params;
    int param_count;          /* -1 when the signature was not modelled */
    int arity_min, arity_max; /* arity_max -1 means variadic; both -1 means unknown */
    uint32_t flags;
    uint32_t line;
    struct cbm_source_decl *next;
    struct cbm_source_decl *last; /* on a chain's head: its end, so appending stays constant */
} cbm_source_decl_t;

typedef struct {
    uint32_t start, end;
    cbm_source_site_kind_t kind;
    cbm_source_outcome_t outcome;
} cbm_source_site_t;

typedef struct cbm_source_walk {
    CBMArena *arena;
    const CBMFileResult *result;
    CBMLanguage language;
    const char *file;
    const char *source;
    size_t source_len;
    cbm_source_table_t *writable;    /* phase one only */
    const cbm_source_table_t *table; /* phase two only */
    cbm_source_unit_t *unit;         /* phase two only */
    TSNode root;
    uint64_t visits;
    const char *failure;
    CBMHashTable *def_index; /* lazily built, owned by the walk */
    /* Set around an emit for a site extraction has no carrier for. */
    const char *extra_scope;
    const char *extra_name;
    uint32_t extra_line;
} cbm_source_walk_t;

typedef void (*cbm_source_pass_fn)(cbm_source_walk_t *walk);
typedef const char *(*cbm_source_base_fn)(const cbm_source_table_t *table,
                                          const cbm_source_decl_t *decl, size_t offset);
typedef struct {
    CBMLanguage language;
    cbm_source_pass_fn collect;
    cbm_source_pass_fn resolve;
    cbm_source_base_fn base; /* optional: a language whose inheritance only its rules can read */
    bool decides_references;
    bool one_name_one_node;
    bool before_binding_rules;
    /* A grammar's parse error stays where it is: see source_damage_scan. */
    bool contains_parse_damage;
    /* optional: collects a file whose tree is unusable, when the adapter has another way to
     * read it. It sets walk->failure when it has none. */
    cbm_source_pass_fn collect_unusable;
} cbm_source_adapter_t;

/* Per-language rule tables. A row here is what makes a language supported. */
extern const cbm_source_adapter_t cbm_source_adapter_php;
extern const cbm_source_adapter_t cbm_source_adapter_elixir;
extern const cbm_source_adapter_t cbm_source_adapter_erlang;
extern const cbm_source_adapter_t cbm_source_adapter_zig;
extern const cbm_source_adapter_t cbm_source_adapter_perl;
extern const cbm_source_adapter_t cbm_source_adapter_ruby;
extern const cbm_source_adapter_t cbm_source_adapter_haskell;
extern const cbm_source_adapter_t cbm_source_adapter_ocaml;
extern const cbm_source_adapter_t cbm_source_adapter_dart;
extern const cbm_source_adapter_t cbm_source_adapter_swift;
extern const cbm_source_adapter_t cbm_source_adapter_scala;

/* One walk of a class chain. A class appears once on a linearized chain, so a
 * revisit adds nothing: a cycle or a diamond cannot make the walk exponential. */
enum { CBM_SOURCE_CHAIN_MAX = 128 };
typedef struct {
    const char *seen[CBM_SOURCE_CHAIN_MAX];
    int count;
    bool overflow;
} cbm_source_chain_t;

static inline bool cbm_source_chain_first_visit(cbm_source_chain_t *chain, const char *owner) {
    for (int i = 0; i < chain->count; i++)
        if (!strcmp(chain->seen[i], owner))
            return false;
    if (chain->count == CBM_SOURCE_CHAIN_MAX) {
        chain->overflow = true;
        return false;
    }
    chain->seen[chain->count++] = owner;
    return true;
}

/* Syntax helpers shared by the language files; none of them decides anything. */
static inline bool cbm_source_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && strcmp(ts_node_type(node), kind) == 0;
}
static inline TSNode cbm_source_field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}
/* A grammar can tag a parenthesised list's `(` with the field too; this returns
 * the first named child carrying the field. */
TSNode cbm_source_named_field(TSNode node, const char *name);
const char *cbm_source_text(cbm_source_walk_t *walk, TSNode node);
const char *cbm_source_lower(cbm_source_walk_t *walk, const char *text);
/* True while the file's walk stays inside its analysis budget. */
bool cbm_source_step(cbm_source_walk_t *walk, unsigned depth);
void cbm_source_fail(cbm_source_walk_t *walk, const char *reason);
bool cbm_source_token(TSNode node, const char *token);

/* Join one declaration to the definition extraction actually produced for it.
 * The line must fall inside that definition's span. NULL means not materialized. */
const char *cbm_source_def_qn(cbm_source_walk_t *walk, const char *name, uint32_t line);
/* The one QN this file's definitions of a name share, wherever they stand, or NULL. */
const char *cbm_source_def_qn_of_name(cbm_source_walk_t *walk, const char *name);
const char *cbm_source_def_qn_label(cbm_source_walk_t *walk, const char *name, uint32_t line,
                                    const char *label);

/* Phase one. Copies everything into the table; returns NULL on allocation failure. */
cbm_source_decl_t *cbm_source_declare(cbm_source_walk_t *walk, const char *owner, const char *name,
                                      const char *qn, uint32_t flags, uint32_t line);
const char *cbm_source_table_intern(cbm_source_walk_t *walk, const char *text);
void *cbm_source_table_alloc(cbm_source_walk_t *walk, size_t size);

/* Phase two. Every declaration of (owner, name) in declaration order, across
 * the whole project, for this language. */
const cbm_source_decl_t *cbm_source_lookup(const cbm_source_walk_t *walk, const char *owner,
                                           const char *name);
/* The same, in another language's declarations, for a call one language's rules state
 * across to another: Elixir's `:mod.f()` names an Erlang module. */
const cbm_source_decl_t *cbm_source_lookup_in(const cbm_source_walk_t *walk, CBMLanguage language,
                                              const char *owner, const char *name);
const char *cbm_source_table_package(const cbm_source_table_t *table, const char *name);

/* Phase two. Later rows for the same exact site replace earlier ones. */
void cbm_source_emit(cbm_source_walk_t *walk, uint32_t start, uint32_t end,
                     cbm_source_site_kind_t kind, const char *target, cbm_resolve_rule_t rule,
                     const char *domain, const char *reason);
void cbm_source_emit_facts(cbm_source_walk_t *walk, uint32_t start, uint32_t end,
                           cbm_source_site_kind_t kind, const char *target, cbm_resolve_rule_t rule,
                           const char *domain, const char *reason, uint32_t facts);
static inline void cbm_source_emit_node(cbm_source_walk_t *walk, TSNode node,
                                        cbm_source_site_kind_t kind, const char *target,
                                        cbm_resolve_rule_t rule, const char *domain,
                                        const char *reason) {
    cbm_source_emit(walk, ts_node_start_byte(node), ts_node_end_byte(node), kind, target, rule,
                    domain, reason);
}
/* A single declaration's QN when it alone is selected; NULL with a reason when
 * several or a non-materialized declaration make the choice not exact. */
const char *cbm_source_unique_qn(const cbm_source_decl_t *first, const char **reason);

#endif
