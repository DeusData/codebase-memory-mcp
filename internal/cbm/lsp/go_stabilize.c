#include "go_stabilize.h"
#include "../helpers.h"
#include "../resolve/policy.h"
#include "foundation/hash_table.h"
#include "foundation/mem_core.h"
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

extern const TSLanguage *tree_sitter_go(void);

const char cbm_go_type_conversion[] = "go_type_conversion";

typedef struct go_support {
    const char *key;
    const cbm_resolve_proof_t *audit;
    struct go_support *next;
} go_support_t;
typedef struct go_domain {
    const char *key;
    go_support_t *first;
    size_t count;
    struct go_domain *next;
} go_domain_t;
typedef struct go_site {
    const char *key, *file, *scope, *spelling, *site_key;
    uint32_t start, end;
    cbm_resolve_rule_t rule;
    bool carrier_exact;
    const cbm_resolve_proof_source_t *source;
    cbm_resolve_result_t result;
    struct go_site *next;
} go_site_t;
/* One selectable name of a named type: a method declaration or a field annotation. */
typedef struct go_member {
    const char *name, *key, *declaration, *module;
    const cbm_resolve_proof_t *proof;
    bool field, requires_pointer, function_value;
    struct go_member *next;
} go_member_t;
typedef struct go_embed {
    const char *qn; /* NULL: outside the project, or not a plain named type. */
    bool pointer, foreign;
    struct go_embed *next;
} go_embed_t;
/* What Go's selector rule needs: the names declared at depth zero and what is embedded.
 * open means the name set cannot be enumerated, so nothing at or below it is proved. */
typedef struct go_type_info {
    const char *qn;
    const char *underlying; /* `type A B`: B, when it is written as a plain named type. */
    go_member_t *members;
    go_embed_t *embeds;
    bool declared, open, interface;
    struct go_type_info *next;
} go_type_info_t;
/* A reference token and the named fact that selects its declaration, NULL for a token that
 * means a local. Sorted per file. */
typedef struct {
    uint32_t start, end;
    const char *key;
    cbm_resolve_rule_t rule;
} go_ref_t;
typedef struct {
    go_ref_t *items;
    size_t count;
} go_file_refs_t;
struct cbm_go_stabilization {
    CBMArena *arena;
    cbm_resolve_proof_pool_t *proofs;
    cbm_resolve_solver_t *solver;
    CBMHashTable *domains, *registered, *names, *sites, *types, *file_refs, *packages;
    go_site_t *first_site;
    go_domain_t *first_domain;
    go_type_info_t *first_type;
    bool failed, sealed;
};
typedef struct go_local {
    const char *name, *fact, *declared_fact;
    const char *local_type; /* `type T struct{...}` inside a function: T's own fact. */
    TSNode signature;       /* The function type or literal a function value was declared with. */
    bool type_shadow, call_blocked, escaped, nonlocal;
    bool channel; /* Declared as one: ranging over it yields elements, not indexes. */
    struct go_local *next;
} go_local_t;
typedef struct go_frame {
    CBMHashTable *names;
    go_local_t *locals;
    struct go_frame *parent;
} go_frame_t;
typedef struct {
    cbm_go_stabilization_t *shared;
    const CBMFileResult *result;
    const char *source, *module, *scope;
    cbm_resolve_proof_source_t context;
    const cbm_resolve_proof_source_t *site_source;
    CBMHashTable *imports, *import_targets, *definitions, *calls;
    CBMArena call_arena;
    go_ref_t *refs;
    size_t ref_count, ref_capacity;
    go_frame_t *frame;
    TSNode const_type, const_value; /* What a bare const_spec of this list repeats. */
    unsigned depth;
} go_collect_t;

/* Every value source cannot type: an import outside the project, a universe or literal type, a
 * local type. Nothing can be selected from it, so it costs no fact. It is never registered,
 * so anything depending on it stays UNKNOWN. */
static const char GO_UNKNOWN[] = "go/unknown";

/* Lives until this file's collection ends. What must outlive it is copied where it is kept. */
static const char *text(go_collect_t *c, TSNode node) {
    return ts_node_is_null(node) ? NULL : cbm_node_text(&c->call_arena, node, c->source);
}
static uint32_t named_count(TSNode node) {
    return ts_node_is_null(node) ? 0 : ts_node_named_child_count(node);
}
static TSNode named_child(TSNode node, uint32_t index) {
    return ts_node_is_null(node) ? (TSNode){0} : ts_node_named_child(node, index);
}
static TSNode field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}
static bool kind(TSNode node, const char *name) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), name);
}
static const char *site_key(CBMArena *arena, const char *file, uint32_t start, uint32_t end,
                            const char *role) {
    return cbm_arena_sprintf(arena, "go/%s:%zu:%s:%u:%u", role, strlen(file), file, start, end);
}
static const char *node_key(go_collect_t *c, TSNode node, const char *role) {
    return site_key(&c->call_arena, c->context.file, ts_node_start_byte(node),
                    ts_node_end_byte(node), role);
}
static const cbm_resolve_proof_t *audit(go_collect_t *c, TSNode node, const char *key,
                                        const char *detail) {
    cbm_resolve_proof_source_t source = c->context;
    source.scope = c->scope ? c->scope : c->module;
    cbm_resolve_proof_key_t fact = {.lookup_domain = c->module,
                                    .lookup_key = key,
                                    .declaration_identity = key,
                                    .start_byte = ts_node_start_byte(node),
                                    .end_byte = ts_node_end_byte(node),
                                    .lookup_closed = true,
                                    .detail = detail};
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    return cbm_resolve_proof_leaf(c->shared->proofs, CBM_PROOF_SOURCE_FACT, &source, &fact,
                                  &result);
}
/* A fact is shared by every site asking the same question. True the first time, and *key
 * becomes the stable copy either way. */
static bool remember(go_collect_t *c, const char **key) {
    if (!*key) {
        c->shared->failed = true;
        return false;
    }
    const char *known = cbm_ht_get(c->shared->registered, *key);
    if (known) {
        *key = known;
        return false;
    }
    const char *copy = cbm_arena_strdup(c->shared->arena, *key);
    if (!copy) {
        c->shared->failed = true;
        return false;
    }
    cbm_ht_set(c->shared->registered, copy, (void *)copy);
    c->shared->failed |= cbm_ht_get(c->shared->registered, copy) != copy;
    *key = copy;
    return !c->shared->failed;
}
/* The one stable spelling of a domain name, which many sites name and none registers. */
static const char *stable(go_collect_t *c, const char *name) {
    if (!name) {
        c->shared->failed = true;
        return GO_UNKNOWN;
    }
    const char *known = cbm_ht_get(c->shared->names, name);
    if (known)
        return known;
    const char *copy = cbm_arena_strdup(c->shared->arena, name);
    if (!copy) {
        c->shared->failed = true;
        return GO_UNKNOWN;
    }
    cbm_ht_set(c->shared->names, copy, (void *)copy);
    c->shared->failed |= cbm_ht_get(c->shared->names, copy) != copy;
    return copy;
}
static void check(go_collect_t *c, bool ok) {
    c->shared->failed |= !ok;
}
static go_local_t *local_binding(go_collect_t *c, const char *name) {
    if (!name)
        return NULL;
    for (go_frame_t *frame = c->frame; frame; frame = frame->parent) {
        go_local_t *item = frame->names ? cbm_ht_get(frame->names, name) : NULL;
        if (item)
            return item;
    }
    return NULL;
}
static const char *local(go_collect_t *c, const char *name) {
    go_local_t *item = local_binding(c, name);
    return item ? item->fact : NULL;
}
static bool push_frame(go_collect_t *c) {
    go_frame_t *frame = cbm_arena_calloc(&c->call_arena, sizeof(*frame));
    if (!frame) {
        c->shared->failed = true;
        return false;
    }
    frame->parent = c->frame;
    c->frame = frame;
    return true;
}
static void pop_frame(go_collect_t *c) {
    go_frame_t *frame = c->frame;
    c->frame = frame->parent;
    if (frame->names)
        cbm_ht_free(frame->names);
}
static void put(go_collect_t *c, CBMHashTable *table, const char *key, void *value) {
    if (!table || !key || !value) {
        c->shared->failed = true;
        return;
    }
    cbm_ht_set(table, key, value);
    c->shared->failed |= cbm_ht_get(table, key) != value;
}
/* A Go variable's static type is fixed where the variable is declared. Assignment cannot
 * change it, in any branch, closure or through any pointer, so its facts are not
 * flow-sensitive: `x := New(); if c { x = nil }; x.M()` selects by New's declared result.
 * CALLS names the statically selected declaration, not what runs. `:=` re-using a name
 * already declared in the same frame is such an assignment. */
static void note_reference(go_collect_t *, TSNode, const char *, cbm_resolve_rule_t);
static void bind_at(go_collect_t *c, TSNode name, TSNode site, const char *dependency,
                    bool declaration) {
    const char *spelling = text(c, name);
    if (!spelling || !strcmp(spelling, "_"))
        return;
    if (!c->frame) {
        c->shared->failed = true;
        return;
    }
    go_local_t *item = declaration && c->frame->names ? cbm_ht_get(c->frame->names, spelling)
                       : declaration                  ? NULL
                                                      : local_binding(c, spelling);
    if (item || !declaration) {
        if (item)
            note_reference(c, name, NULL, CBM_RESOLVE_RULE_NONE);
        return;
    }
    item = cbm_arena_calloc(&c->call_arena, sizeof(*item));
    if (!c->frame->names)
        c->frame->names = cbm_ht_create(8);
    if (!item || !c->frame->names) {
        c->shared->failed = true;
        return;
    }
    item->name = spelling;
    item->next = c->frame->locals;
    c->frame->locals = item;
    put(c, c->frame->names, spelling, item);
    note_reference(c, name, NULL, CBM_RESOLVE_RULE_NONE);
    if (!dependency || dependency == GO_UNKNOWN) {
        item->fact = GO_UNKNOWN;
        return;
    }
    const char *key = node_key(c, site, "version");
    if (remember(c, &key))
        check(c, cbm_resolve_solver_type(c->shared->solver, key, dependency, CBM_TYPE_NAMED,
                                         ts_node_start_byte(name) + 1,
                                         audit(c, name, key, "immutable_lexical_binding_version")));
    item->fact = key;
}
static void bind(go_collect_t *c, TSNode name, const char *dependency, bool declaration) {
    bind_at(c, name, name, dependency, declaration);
}
/* A captured or address-taken local may hold any callable afterwards. Its type is fixed,
 * but calling through it no longer names one declaration. */
static void escape_visible(go_collect_t *c) {
    for (go_frame_t *frame = c->frame; frame; frame = frame->parent)
        for (go_local_t *item = frame->locals; item; item = item->next)
            item->call_blocked = true;
}
static void escape_effects(go_collect_t *c, TSNode node, unsigned depth) {
    if (ts_node_is_null(node))
        return;
    if (depth >= 64 || kind(node, "func_literal")) {
        escape_visible(c);
        return;
    }
    const char *operator_text =
        kind(node, "unary_expression") ? text(c, field(node, "operator")) : NULL;
    if (operator_text && !strcmp(operator_text, "&")) {
        TSNode operand = field(node, "operand");
        go_local_t *item = kind(operand, "identifier") ? local_binding(c, text(c, operand)) : NULL;
        if (item)
            item->call_blocked = true;
    }
    for (uint32_t i = 0; i < named_count(node); i++)
        escape_effects(c, named_child(node, i), depth + 1);
}

static void shadow(go_collect_t *c, TSNode name, bool type) {
    bind(c, name, NULL, true);
    go_local_t *item = local_binding(c, text(c, name));
    if (item) {
        item->type_shadow = type;
        item->call_blocked = true;
    }
}

static bool exported_name(const char *name) {
    return name && name[0] >= 'A' && name[0] <= 'Z';
}
/* A lower-case name can only be selected inside its declaring package. */
static const char *member_prefix(CBMArena *arena, const char *module, const char *name,
                                 const char *exported, const char *hidden) {
    return exported_name(name) ? exported : cbm_arena_sprintf(arena, "%s%s:", hidden, module);
}
/* A universe name, unless the package redeclares it, which then merely abstains. Its members
 * are never project declarations. */
static bool universe_type(const char *name) {
    static const char *const names[] = {
        "any",     "bool",  "byte",   "comparable", "complex64", "complex128", "error", "float32",
        "float64", "int",   "int8",   "int16",      "int32",     "int64",      "rune",  "string",
        "uint",    "uint8", "uint16", "uint32",     "uint64",    "uintptr"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++)
        if (!strcmp(name, names[i]))
            return true;
    return false;
}
static const char *imported_target(go_collect_t *c, const char *package) {
    return package && !local_binding(c, package) ? cbm_ht_get(c->import_targets, package) : NULL;
}
/* pointer-of or slice-of one fact, the same fact wherever it is written. */
static const char *wrapped(go_collect_t *c, TSNode node, const char *inner, CBMTypeKind wrapper) {
    if (!inner)
        return NULL;
    if (inner == GO_UNKNOWN)
        return GO_UNKNOWN;
    const char *key = cbm_arena_sprintf(&c->call_arena, "go/%s-of:%s",
                                        wrapper == CBM_TYPE_POINTER ? "pointer" : "slice", inner);
    if (remember(c, &key))
        check(c, cbm_resolve_solver_type(c->shared->solver, key, inner, wrapper, 0,
                                         audit(c, node, key, "source_type_annotation")));
    return key;
}
static const char *type_fact(go_collect_t *, TSNode);
static unsigned result_names(TSNode);
/* The one value a function type or literal returns. Several results have no single type. */
static const char *function_result(go_collect_t *c, TSNode node) {
    TSNode result = field(node, "result");
    if (ts_node_is_null(result))
        return GO_UNKNOWN;
    if (!kind(result, "parameter_list"))
        return type_fact(c, result);
    unsigned total = 0;
    for (uint32_t i = 0; i < named_count(result); i++)
        total += result_names(named_child(result, i));
    return total == 1 ? type_fact(c, field(named_child(result, 0), "type")) : GO_UNKNOWN;
}
static const char *type_fact_inner(go_collect_t *, TSNode);
static void struct_members(go_collect_t *, const char *, TSNode, const cbm_resolve_proof_t *, bool);
static go_type_info_t *type_info(cbm_go_stabilization_t *, const char *);
/* A table test's `[]struct{ in *T }{...}` and a field's `[]struct{ X T }`: the literal is its own
 * type, its fields are written right there, and it has no methods and no graph node. A fact per
 * occurrence, so two identical literals are two facts with the same answers. */
static const char *anonymous_struct(go_collect_t *c, TSNode node) {
    const char *key = node_key(c, node, "anonymous-struct");
    if (!remember(c, &key))
        return key;
    const char *qn = cbm_arena_sprintf(&c->call_arena, "%s.%s", c->module, key);
    const cbm_resolve_proof_t *proof = audit(c, node, key, "source_type_annotation");
    go_type_info_t *info = type_info(c->shared, qn);
    if (!info) {
        c->shared->failed = true;
        return GO_UNKNOWN;
    }
    info->declared = true;
    check(c, cbm_resolve_solver_leaf(
                 c->shared->solver, key, CBM_SOLVER_KNOWN,
                 (cbm_resolve_solver_value_t){.target = cbm_arena_strdup(c->shared->arena, qn),
                                              .fact_kind = CBM_SOLVER_FACT_TYPE,
                                              .type = cbm_type_named(c->shared->arena, qn),
                                              .declaration_identity = key},
                 proof));
    struct_members(c, qn, node, proof, true);
    return key;
}
static const char *type_fact(go_collect_t *c, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    if (c->depth >= 64)
        return GO_UNKNOWN;
    c->depth++;
    const char *result = type_fact_inner(c, node);
    c->depth--;
    return result;
}
static const char *type_fact_inner(go_collect_t *c, TSNode node) {
    if (kind(node, "pointer_type") && named_count(node) == 1)
        return wrapped(c, node, type_fact(c, named_child(node, 0)), CBM_TYPE_POINTER);
    if (kind(node, "parenthesized_type") && named_count(node) == 1)
        return type_fact(c, named_child(node, 0));
    /* An instantiation selects members of the generic declaration itself. */
    if (kind(node, "generic_type"))
        return type_fact(c, field(node, "type"));
    if (kind(node, "slice_type") || kind(node, "array_type") ||
        kind(node, "implicit_length_array_type"))
        return wrapped(c, node, type_fact(c, field(node, "element")), CBM_TYPE_SLICE);
    /* Only the element is ever consumed: none of these has selectable members, so they share
     * the wrapper that says "ranging or indexing this yields that". */
    if (kind(node, "map_type") || kind(node, "channel_type"))
        return wrapped(c, node, type_fact(c, field(node, "value")), CBM_TYPE_SLICE);
    /* So does calling a function value: Go has no way to range or index one that returns. */
    if (kind(node, "function_type"))
        return wrapped(c, node, function_result(c, node), CBM_TYPE_SLICE);
    if (kind(node, "struct_type"))
        return anonymous_struct(c, node);
    if (kind(node, "type_identifier")) {
        const char *name = text(c, node);
        const go_local_t *scoped = local_binding(c, name);
        if (scoped && scoped->local_type)
            return scoped->local_type;
        if (!name || scoped || universe_type(name))
            return GO_UNKNOWN;
        return stable(c, cbm_arena_sprintf(&c->call_arena, "go/type:%s.%s", c->module, name));
    }
    if (kind(node, "qualified_type")) {
        const char *package = text(c, field(node, "package"));
        const char *member = text(c, field(node, "name"));
        if (!imported_target(c, package) || !exported_name(member))
            return GO_UNKNOWN;
        const char *key =
            cbm_arena_sprintf(&c->call_arena, "go/qualified-type:%zu:%s:%s.%s",
                              strlen(c->context.file), c->context.file, package, member);
        if (remember(c, &key))
            check(c, cbm_resolve_solver_select(
                         c->shared->solver, key, cbm_ht_get(c->imports, package),
                         "go/type:", cbm_arena_sprintf(&c->call_arena, ".%s", member),
                         audit(c, node, key, "source_type_annotation")));
        return key;
    }
    return GO_UNKNOWN;
}
static const char *expression(go_collect_t *, TSNode);
static const char *imported(go_collect_t *c, TSNode node) {
    return kind(node, "identifier") && !local_binding(c, text(c, node))
               ? cbm_ht_get(c->imports, text(c, node))
               : NULL;
}
/* A name another package selects through its import. */
static const char *qualified_fact(go_collect_t *c, TSNode node, const char *package,
                                  const char *name, const char *prefix, const char *detail) {
    if (!imported_target(c, package) || !exported_name(name))
        return GO_UNKNOWN;
    const char *key = cbm_arena_sprintf(&c->call_arena, "go/qualified-%s%zu:%s:%s.%s", prefix,
                                        strlen(c->context.file), c->context.file, package, name);
    if (remember(c, &key))
        check(c, cbm_resolve_solver_select(c->shared->solver, key, cbm_ht_get(c->imports, package),
                                           cbm_arena_sprintf(&c->call_arena, "go/%s", prefix),
                                           cbm_arena_sprintf(&c->call_arena, ".%s", name),
                                           audit(c, node, key, detail)));
    return key;
}
/* A member selected from a typed value, once per value and name. */
static const char *selected_fact(go_collect_t *c, TSNode node, const char *selector,
                                 const char *name, const char *exported, const char *hidden,
                                 const char *detail) {
    if (!selector || selector == GO_UNKNOWN)
        return GO_UNKNOWN;
    const char *prefix = member_prefix(&c->call_arena, c->module, name, exported, hidden);
    const char *key =
        cbm_arena_sprintf(&c->call_arena, "go/selected-from:%s%s.%s", prefix, selector, name);
    if (remember(c, &key))
        check(c, cbm_resolve_solver_select_type(c->shared->solver, key, selector, prefix,
                                                cbm_arena_sprintf(&c->call_arena, ".%s", name),
                                                audit(c, node, key, detail)));
    return key;
}
static const char *callable(go_collect_t *c, TSNode node, cbm_resolve_rule_t *rule) {
    *rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
    if (kind(node, "identifier")) {
        const char *name = text(c, node);
        go_local_t *binding = local_binding(c, name);
        if (binding) {
            if (!binding->call_blocked)
                return NULL; /* Existing audited callable-value assignment subset. */
            if (binding->fact == GO_UNKNOWN)
                return GO_UNKNOWN;
            const char *key =
                cbm_arena_sprintf(&c->call_arena, "go/callable-from:%s", binding->fact);
            if (remember(c, &key))
                check(c, cbm_resolve_solver_copy(c->shared->solver, key, binding->fact,
                                                 audit(c, node, key, "source_callable_binding")));
            return key;
        }
        return stable(c, cbm_arena_sprintf(&c->call_arena, "go/binding:%s.%s", c->module, name));
    }
    if (kind(node, "parenthesized_expression") && named_count(node) == 1)
        return callable(c, named_child(node, 0), rule);
    /* Name[T]() instantiates the generic declaration it names. Indexing a local or a field is
     * a function value out of a container, with no declaration to select. */
    if (kind(node, "index_expression")) {
        TSNode named = field(node, "operand");
        bool declared = kind(named, "identifier") ? !local_binding(c, text(c, named))
                                                  : kind(named, "selector_expression") &&
                                                        imported(c, field(named, "operand"));
        return declared ? callable(c, named, rule) : NULL;
    }
    if (kind(node, "type_instantiation_expression")) {
        TSNode type = field(node, "type");
        if (kind(type, "qualified_type")) {
            *rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
            return qualified_fact(c, node, text(c, field(type, "package")),
                                  text(c, field(type, "name")),
                                  "binding:", "source_callable_binding");
        }
        const char *name = kind(type, "type_identifier") ? text(c, type) : NULL;
        return name && !local_binding(c, name)
                   ? stable(c,
                            cbm_arena_sprintf(&c->call_arena, "go/binding:%s.%s", c->module, name))
                   : NULL;
    }
    if (!kind(node, "selector_expression"))
        return NULL;
    TSNode operand = field(node, "operand"), member = field(node, "field");
    const char *member_name = text(c, member);
    if (!member_name)
        return NULL;
    if (imported(c, operand)) {
        *rule = CBM_RESOLVE_RULE_IMPORT_BINDING;
        return qualified_fact(c, node, text(c, operand), member_name,
                              "binding:", "source_callable_binding");
    }
    *rule = CBM_RESOLVE_RULE_TYPE_MEMBER;
    /* Only a call result or literal is unaddressable, and only those cannot use a
     * pointer-receiver method. Every other operand is a variable, a field of one,
     * an element or a dereference. */
    bool addressable = !kind(operand, "call_expression") && !kind(operand, "composite_literal") &&
                       !kind(operand, "type_conversion_expression");
    return selected_fact(c, node, expression(c, operand), member_name,
                         addressable ? "go/addressable-member:" : "go/member:",
                         addressable ? "go/private-addressable-member:" : "go/private-member:",
                         "source_callable_binding");
}
/* The extractor owns occurrence coordinates and spelling. In particular, a
 * chained selector's canonical name is not its literal source substring. */
static const char *carrier_key(go_collect_t *c, uint32_t start, uint32_t end, const char *scope) {
    if (!scope) {
        c->shared->failed = true;
        return NULL;
    }
    const char *key =
        cbm_arena_sprintf(&c->call_arena, "%u:%u:%zu:%s", start, end, strlen(scope), scope);
    c->shared->failed |= key == NULL;
    return key;
}
static void record_call_site(go_collect_t *c, TSNode node, const char *binding,
                             cbm_resolve_rule_t rule) {
    const char *site = site_key(c->shared->arena, c->context.file, ts_node_start_byte(node),
                                ts_node_end_byte(node), "site");
    if (!site || cbm_ht_get(c->shared->sites, site))
        return;
    const char *key = carrier_key(c, ts_node_start_byte(node), ts_node_end_byte(node), c->scope);
    const void *entry = key ? cbm_ht_get(c->calls, key) : NULL;
    const CBMCall *call = entry == c ? NULL : entry;
    go_site_t *item = cbm_arena_calloc(c->shared->arena, sizeof(*item));
    if (!item) {
        c->shared->failed = true;
        return;
    }
    *item = (go_site_t){.key = binding == GO_UNKNOWN ? NULL : binding,
                        .file = c->context.file,
                        .scope = c->scope,
                        .site_key = site,
                        .spelling = call && binding != GO_UNKNOWN
                                        ? cbm_arena_strdup(c->shared->arena, call->callee_name)
                                        : NULL,
                        .start = ts_node_start_byte(node),
                        .end = ts_node_end_byte(node),
                        .rule = rule,
                        .carrier_exact = call != NULL,
                        .source = c->site_source,
                        .next = c->shared->first_site};
    if (call && binding != GO_UNKNOWN && !item->spelling)
        c->shared->failed = true;
    c->shared->first_site = item;
    put(c, c->shared->sites, site, item);
}
/* The element a slice, array, map or channel yields, or what a pointer points to. */
static const char *element_of(go_collect_t *c, TSNode node, const char *container) {
    if (!container)
        return NULL;
    if (container == GO_UNKNOWN)
        return GO_UNKNOWN;
    const char *key = cbm_arena_sprintf(&c->call_arena, "go/element-from:%s", container);
    if (remember(c, &key))
        check(c, cbm_resolve_solver_element(c->shared->solver, key, container, "go/element:",
                                            audit(c, node, key, "source_element_type")));
    return key;
}
static const char *selected_field(go_collect_t *c, TSNode node) {
    TSNode operand = field(node, "operand");
    const char *name = text(c, field(node, "field"));
    if (!name)
        return NULL;
    if (imported(c, operand))
        return qualified_fact(c, node, text(c, operand), name,
                              "var:", "source_package_variable_selector");
    return selected_fact(c, node, expression(c, operand), name,
                         "go/field:", "go/private-field:", "source_field_selector");
}
/* (*T)(x) and (*pkg.T)(x) convert. The grammar reads them as calling a parenthesized dereference,
 * but only a type is ever registered under go/type:, so a real dereference of a package-level
 * function pointer stays unknown. A local is never a type here. */
static const char *pointer_conversion(go_collect_t *c, TSNode function) {
    TSNode inner = function;
    while (kind(inner, "parenthesized_expression") && named_count(inner) == 1)
        inner = named_child(inner, 0);
    if (!kind(inner, "unary_expression"))
        return NULL;
    const char *operator_text = text(c, field(inner, "operator"));
    if (!operator_text || strcmp(operator_text, "*"))
        return NULL;
    TSNode operand = field(inner, "operand");
    const char *named = NULL;
    if (kind(operand, "identifier")) {
        const char *name = text(c, operand);
        if (name && !local_binding(c, name) && !universe_type(name))
            named = stable(c, cbm_arena_sprintf(&c->call_arena, "go/type:%s.%s", c->module, name));
    } else if (kind(operand, "selector_expression") && imported(c, field(operand, "operand"))) {
        named = qualified_fact(c, operand, text(c, field(operand, "operand")),
                               text(c, field(operand, "field")), "type:", "source_type_annotation");
    }
    return named ? wrapped(c, inner, named, CBM_TYPE_POINTER) : NULL;
}
/* new and make are universe names unless this scope declares its own. */
static const char *builtin_call(go_collect_t *c, TSNode node, TSNode function, bool *handled) {
    *handled = false;
    const char *name = kind(function, "identifier") ? text(c, function) : NULL;
    TSNode arguments = field(node, "arguments");
    if (!name || local_binding(c, name) || !named_count(arguments))
        return NULL;
    TSNode first = named_child(arguments, 0);
    if (!strcmp(name, "new")) {
        *handled = true;
        /* Since Go 1.26 new(T(x)) and new(T{}) point at the value of an expression. */
        bool value = kind(first, "call_expression") || kind(first, "composite_literal");
        return wrapped(c, node, value ? expression(c, first) : type_fact(c, first),
                       CBM_TYPE_POINTER);
    }
    if (!strcmp(name, "make")) {
        *handled = true;
        return type_fact(c, first);
    }
    return NULL;
}
static const char *errors_as_type(go_collect_t *, TSNode);
static bool std_call(go_collect_t *, TSNode, const char *, const char *);
static const char *expression_inner(go_collect_t *c, TSNode node) {
    if (kind(node, "identifier")) {
        const char *name = text(c, node);
        const char *fact = local(c, name);
        return fact ? fact
                    : stable(c, cbm_arena_sprintf(&c->call_arena, "go/var:%s.%s", c->module, name));
    }
    if (kind(node, "parenthesized_expression") && named_count(node) == 1)
        return expression(c, named_child(node, 0));
    if (kind(node, "composite_literal"))
        return type_fact(c, field(node, "type"));
    if (kind(node, "func_literal"))
        return wrapped(c, node, function_result(c, node), CBM_TYPE_SLICE);
    if (kind(node, "type_conversion_expression")) {
        const char *found = errors_as_type(c, node);
        return found ? found : type_fact(c, field(node, "type"));
    }
    if (kind(node, "type_assertion_expression"))
        return type_fact(c, field(node, "type"));
    if (kind(node, "selector_expression"))
        return selected_field(c, node);
    if (kind(node, "index_expression"))
        return element_of(c, node, expression(c, field(node, "operand")));
    if (kind(node, "slice_expression"))
        return expression(c, field(node, "operand"));
    if (kind(node, "unary_expression")) {
        const char *operator_text = text(c, field(node, "operator"));
        const char *inner = expression(c, field(node, "operand"));
        if (operator_text && !strcmp(operator_text, "&"))
            return wrapped(c, node, inner, CBM_TYPE_POINTER);
        if (operator_text && (!strcmp(operator_text, "*") || !strcmp(operator_text, "<-")))
            return element_of(c, node, inner);
        return GO_UNKNOWN;
    }
    if (kind(node, "call_expression")) {
        TSNode function = field(node, "function");
        const char *converted = pointer_conversion(c, function);
        if (converted)
            return converted;
        bool handled = false;
        const char *builtin = builtin_call(c, node, function, &handled);
        if (handled)
            return builtin;
        /* `var Default = func() *T { ... }()`: the literal is called where it is written. */
        if (kind(function, "func_literal"))
            return function_result(c, function);
        if (std_call(c, node, "slices", "Clone") && named_count(field(node, "arguments")))
            return expression(c, named_child(field(node, "arguments"), 0));
        cbm_resolve_rule_t rule;
        const char *binding = callable(c, function, &rule);
        if (binding)
            record_call_site(c, node, binding, rule);
        /* A local is a function value whatever else is known of the call: only those are
         * callable, so what it yields is its declared result. */
        go_local_t *value =
            kind(function, "identifier") ? local_binding(c, text(c, function)) : NULL;
        if (value && value->local_type)
            return value->local_type; /* T(x) of a local type T. */
        if (value)
            return element_of(c, node, value->fact);
        if (!binding)
            return NULL;
        if (binding == GO_UNKNOWN)
            return GO_UNKNOWN;
        const char *result = cbm_arena_sprintf(&c->call_arena, "go/result-from:%s", binding);
        if (remember(c, &result))
            check(c, cbm_resolve_solver_select(c->shared->solver, result, binding, "go/return:", "",
                                               audit(c, node, result, "resolved_callable_return")));
        return result;
    }
    return GO_UNKNOWN;
}
static const char *expression(go_collect_t *c, TSNode node) {
    if (ts_node_is_null(node) || c->depth >= 64)
        return NULL;
    c->depth++;
    const char *result = expression_inner(c, node);
    c->depth--;
    return result;
}
/* Whether `package.member` names this member of this standard library package: the spelling alone
 * says nothing, a file can import anything under that name. */
static bool std_member(go_collect_t *c, const char *package, const char *member, const char *path,
                       const char *wanted) {
    if (!package || !member || strcmp(member, wanted) != 0 || local_binding(c, package))
        return false;
    for (int i = 0; i < c->result->imports.count; i++) {
        const CBMImport *import = &c->result->imports.items[i];
        if (import->local_name && !strcmp(import->local_name, package))
            return import->module_path && !strcmp(import->module_path, path);
    }
    return false;
}
static bool std_call(go_collect_t *c, TSNode call, const char *path, const char *wanted) {
    TSNode function = field(call, "function");
    if (!kind(call, "call_expression") || !kind(function, "selector_expression"))
        return false;
    TSNode operand = field(function, "operand");
    return kind(operand, "identifier") &&
           std_member(c, text(c, operand), text(c, field(function, "field")), path, wanted);
}
/* `v, ok := errors.AsType[T](err)`, which the grammar reads as a conversion to a generic type. */
static const char *errors_as_type(go_collect_t *c, TSNode conversion) {
    TSNode type = field(conversion, "type");
    TSNode named = kind(type, "generic_type") ? field(type, "type") : (TSNode){0};
    TSNode arguments = field(type, "type_arguments");
    if (!kind(named, "qualified_type") || named_count(arguments) != 1 ||
        !std_member(c, text(c, field(named, "package")), text(c, field(named, "name")), "errors",
                    "AsType"))
        return NULL;
    TSNode element = named_child(arguments, 0);
    return type_fact(c, kind(element, "type_elem") ? named_child(element, 0) : element);
}
/* The index-th result a function type or literal declares. */
static const char *declared_result(go_collect_t *c, TSNode function, unsigned index) {
    TSNode result = field(function, "result");
    if (ts_node_is_null(result))
        return GO_UNKNOWN;
    if (!kind(result, "parameter_list"))
        return index == 0 ? type_fact(c, result) : GO_UNKNOWN;
    unsigned first = 0;
    for (uint32_t i = 0; i < named_count(result); i++) {
        TSNode parameter = named_child(result, i);
        first += result_names(parameter);
        if (index < first)
            return type_fact(c, field(parameter, "type"));
    }
    return GO_UNKNOWN;
}
/* The value `a, b := right` gives its index-th name. */
static const char *tuple_element(go_collect_t *c, TSNode node, unsigned index) {
    while (kind(node, "parenthesized_expression") && named_count(node) == 1)
        node = named_child(node, 0);
    if (kind(node, "call_expression")) {
        TSNode function = field(node, "function");
        if (kind(function, "func_literal"))
            return declared_result(c, function, index);
        /* A local holds a function value of one static type: written where it was declared,
         * or a named function type, whose results are registered under that name. */
        const go_local_t *value =
            kind(function, "identifier") ? local_binding(c, text(c, function)) : NULL;
        if (value && !ts_node_is_null(value->signature))
            return declared_result(c, value->signature, index);
        if (value) {
            if (!value->fact || value->fact == GO_UNKNOWN)
                return GO_UNKNOWN;
            const char *key =
                cbm_arena_sprintf(&c->call_arena, "go/result-from#%u:%s", index, value->fact);
            if (remember(c, &key))
                check(c, cbm_resolve_solver_select(
                             c->shared->solver, key, value->fact,
                             cbm_arena_sprintf(&c->call_arena, "go/return#%u:", index), "",
                             audit(c, node, key, "resolved_callable_tuple_return")));
            return key;
        }
        cbm_resolve_rule_t rule;
        const char *binding = callable(c, function, &rule);
        if (!binding)
            return NULL;
        if (binding == GO_UNKNOWN)
            return GO_UNKNOWN;
        const char *key = cbm_arena_sprintf(&c->call_arena, "go/result-from#%u:%s", index, binding);
        if (remember(c, &key))
            check(c, cbm_resolve_solver_select(
                         c->shared->solver, key, binding,
                         cbm_arena_sprintf(&c->call_arena, "go/return#%u:", index), "",
                         audit(c, node, key, "resolved_callable_tuple_return")));
        return key;
    }
    /* The comma-ok forms: the second name is a plain bool. */
    return index == 0 ? expression(c, node) : NULL;
}
static void note_reference(go_collect_t *c, TSNode token, const char *key,
                           cbm_resolve_rule_t rule) {
    if (c->ref_count == c->ref_capacity) {
        size_t capacity = c->ref_capacity ? c->ref_capacity * 2 : 64;
        go_ref_t *grown = cbm_realloc(CBM_MEM_CLASS_SEMANTIC, c->refs, capacity * sizeof(*grown));
        if (!grown) {
            c->shared->failed = true;
            return;
        }
        c->refs = grown;
        c->ref_capacity = capacity;
    }
    c->refs[c->ref_count++] = (go_ref_t){.start = ts_node_start_byte(token),
                                         .end = ts_node_end_byte(token),
                                         .key = key,
                                         .rule = rule};
}
/* A name outside every local frame can only mean the package-scope declaration: Go has no
 * other scope between them, and a file's imports may not redeclare a package-level name. */
static void reference_package_name(go_collect_t *c, TSNode token) {
    const char *name = text(c, token);
    if (!name || !strcmp(name, "_"))
        return;
    if (local_binding(c, name)) {
        note_reference(c, token, NULL, CBM_RESOLVE_RULE_NONE);
        return;
    }
    note_reference(c, token,
                   stable(c, cbm_arena_sprintf(&c->call_arena, "go/decl:%s.%s", c->module, name)),
                   CBM_RESOLVE_RULE_SCOPE_BINDING);
}
/* Every named type written in a type expression, however deeply wrapped. */
static void reference_types(go_collect_t *c, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth >= 64)
        return;
    if (kind(node, "type_identifier")) {
        reference_package_name(c, node);
        return;
    }
    if (kind(node, "qualified_type")) {
        TSNode name = field(node, "name");
        const char *key = qualified_fact(c, name, text(c, field(node, "package")), text(c, name),
                                         "decl:", "source_qualified_reference");
        if (key != GO_UNKNOWN) {
            /* USES_TYPE reads the whole qualified spelling, USAGE the member token. */
            note_reference(c, name, key, CBM_RESOLVE_RULE_IMPORT_BINDING);
            note_reference(c, node, key, CBM_RESOLVE_RULE_IMPORT_BINDING);
        }
        return;
    }
    for (uint32_t i = 0; i < named_count(node); i++)
        reference_types(c, named_child(node, i), depth + 1);
}
static void reference_selected(go_collect_t *c, TSNode node) {
    TSNode operand = field(node, "operand"), member = field(node, "field");
    const char *name = text(c, member);
    if (!name)
        return;
    if (imported(c, operand)) {
        const char *key = qualified_fact(c, member, text(c, operand), name,
                                         "decl:", "source_qualified_reference");
        if (key != GO_UNKNOWN)
            note_reference(c, member, key, CBM_RESOLVE_RULE_IMPORT_BINDING);
        return;
    }
    const char *key =
        selected_fact(c, member, expression(c, operand), name,
                      "go/selected:", "go/private-selected:", "source_member_reference");
    if (key != GO_UNKNOWN)
        note_reference(c, member, key, CBM_RESOLVE_RULE_TYPE_MEMBER);
}
static void statement(go_collect_t *, TSNode, unsigned);
/* Only a parameter list declares names. A bare result such as `*pkg.T` has no parameters, and
 * the `name` field of its qualified type is the member it selects, which must not become a
 * local shadowing a package-level T for the whole body. */
static void bind_parameters(go_collect_t *c, TSNode parameters) {
    if (ts_node_is_null(parameters) || !kind(parameters, "parameter_list"))
        return;
    for (uint32_t i = 0; i < named_count(parameters); i++) {
        TSNode parameter = named_child(parameters, i);
        const char *fact = type_fact(c, field(parameter, "type"));
        /* `xs ...T` is a []T inside the function. */
        if (kind(parameter, "variadic_parameter_declaration"))
            fact = wrapped(c, parameter, fact, CBM_TYPE_SLICE);
        reference_types(c, field(parameter, "type"), 0);
        for (uint32_t child = 0; child < ts_node_child_count(parameter); child++) {
            const char *role = ts_node_field_name_for_child(parameter, child);
            if (role && !strcmp(role, "name")) {
                TSNode name = ts_node_child(parameter, child);
                bind(c, name, fact, true);
                go_local_t *binding = local_binding(c, text(c, name));
                if (binding) {
                    binding->call_blocked = true;
                    binding->declared_fact = fact;
                    binding->channel = kind(field(parameter, "type"), "channel_type");
                    if (kind(field(parameter, "type"), "function_type"))
                        binding->signature = field(parameter, "type");
                }
            }
        }
    }
}
static void walk_calls(go_collect_t *, TSNode, unsigned);
/* `T{Name: v}`: for a named literal type the bare key is one of its fields, and a named map
 * simply has no such field to select. In a map literal it is an ordinary expression. */
static void literal_element(go_collect_t *c, TSNode type, TSNode element, unsigned depth) {
    if (!kind(element, "keyed_element")) {
        walk_calls(c, element, depth);
        return;
    }
    TSNode key = field(element, "key");
    TSNode inner =
        kind(key, "literal_element") && named_count(key) == 1 ? named_child(key, 0) : key;
    const char *name = kind(inner, "identifier") ? text(c, inner) : NULL;
    if (!name || kind(type, "map_type")) {
        walk_calls(c, key, depth);
    } else if (kind(type, "type_identifier") || kind(type, "qualified_type") ||
               kind(type, "generic_type")) {
        const char *reference =
            selected_fact(c, inner, type_fact(c, type), name,
                          "go/selected:", "go/private-selected:", "source_literal_field_key");
        if (reference != GO_UNKNOWN)
            note_reference(c, inner, reference, CBM_RESOLVE_RULE_TYPE_MEMBER);
    }
    walk_calls(c, field(element, "value"), depth);
}
static void walk_calls(go_collect_t *c, TSNode node, unsigned depth) {
    if (depth == 0)
        escape_effects(c, node, 0);
    if (ts_node_is_null(node) || depth >= 64)
        return;
    if (kind(node, "func_literal")) {
        if (!push_frame(c))
            return;
        reference_types(c, field(node, "result"), 0);
        bind_parameters(c, field(node, "parameters"));
        bind_parameters(c, field(node, "result"));
        statement(c, field(node, "body"), depth);
        pop_frame(c);
        return;
    }
    if (kind(node, "identifier")) {
        reference_package_name(c, node);
        return;
    }
    if (kind(node, "qualified_type") || kind(node, "type_identifier") ||
        kind(node, "pointer_type") || kind(node, "slice_type") || kind(node, "array_type") ||
        kind(node, "implicit_length_array_type") || kind(node, "map_type") ||
        kind(node, "channel_type") || kind(node, "generic_type") || kind(node, "function_type") ||
        kind(node, "struct_type") || kind(node, "interface_type")) {
        reference_types(c, node, 0);
        return;
    }
    if (kind(node, "composite_literal")) {
        TSNode type = field(node, "type"), body = field(node, "body");
        reference_types(c, type, 0);
        for (uint32_t i = 0; i < named_count(body); i++)
            literal_element(c, type, named_child(body, i), depth + 1);
        return;
    }
    if (kind(node, "keyed_element") || kind(node, "literal_value")) {
        /* A nested literal with an elided type: a bare key is a field or a value, and nothing
         * here says which. */
        if (kind(node, "literal_value")) {
            for (uint32_t i = 0; i < named_count(node); i++)
                literal_element(c, (TSNode){0}, named_child(node, i), depth + 1);
        } else {
            literal_element(c, (TSNode){0}, node, depth + 1);
        }
        return;
    }
    if (kind(node, "selector_expression")) {
        reference_selected(c, node);
        if (!imported(c, field(node, "operand")))
            walk_calls(c, field(node, "operand"), depth + 1);
        return;
    }
    if (kind(node, "call_expression")) {
        (void)expression(c, node);
        /* The callee occurrence itself is the CALLS site. */
        TSNode function = field(node, "function");
        if (kind(function, "selector_expression")) {
            if (!imported(c, field(function, "operand")))
                walk_calls(c, field(function, "operand"), depth + 1);
        } else if (!kind(function, "identifier")) {
            walk_calls(c, function, depth + 1);
        }
        walk_calls(c, field(node, "type_arguments"), depth + 1);
        walk_calls(c, field(node, "arguments"), depth + 1);
        return;
    }
    uint32_t count = named_count(node);
    for (uint32_t i = 0; i < count; i++)
        walk_calls(c, named_child(node, i), depth + 1);
}
static bool has_token(TSNode node, const char *token) {
    for (uint32_t i = 0; i < ts_node_child_count(node); i++)
        if (!strcmp(ts_node_type(ts_node_child(node, i)), token))
            return true;
    return false;
}
static TSNode list_item(TSNode list, uint32_t index) {
    return kind(list, "expression_list") ? named_child(list, index)
                                         : (index == 0 ? list : (TSNode){0});
}
static uint32_t list_count(TSNode list) {
    return ts_node_is_null(list) ? 0 : kind(list, "expression_list") ? named_count(list) : 1;
}
static bool makes_channel(go_collect_t *c, TSNode value) {
    TSNode function = field(value, "function");
    if (!kind(value, "call_expression") || !kind(function, "identifier"))
        return false;
    const char *name = text(c, function);
    return name && !strcmp(name, "make") && !local_binding(c, name) &&
           kind(named_child(field(value, "arguments"), 0), "channel_type");
}
static void note_signature(go_collect_t *c, TSNode name, TSNode signature) {
    go_local_t *item = local_binding(c, text(c, name));
    if (item)
        item->signature = signature;
}
static void assignment(go_collect_t *c, TSNode node, bool declaration) {
    TSNode left = field(node, "left"), right = field(node, "right");
    uint32_t names = list_count(left), values = list_count(right);
    escape_effects(c, left, 0);
    walk_calls(c, right, 0);
    if (!declaration)
        walk_calls(c, left, 0);
    for (uint32_t i = 0; i < names; i++) {
        TSNode name = list_item(left, i);
        if (!kind(name, "identifier"))
            continue;
        const char *fact = names == values ? expression(c, list_item(right, i))
                           : values == 1   ? tuple_element(c, list_item(right, 0), i)
                                           : NULL;
        bind(c, name, fact, declaration);
        if (declaration && names == values && kind(list_item(right, i), "func_literal"))
            note_signature(c, name, list_item(right, i));
        if (declaration && names == values && makes_channel(c, list_item(right, i))) {
            go_local_t *item = local_binding(c, text(c, name));
            if (item)
                item->channel = true;
        }
    }
}
static void range_clause(go_collect_t *c, TSNode node) {
    TSNode left = field(node, "left"), right = field(node, "right");
    walk_calls(c, right, 0);
    bool declaration = has_token(node, ":=");
    uint32_t names = list_count(left);
    /* A channel has no index: its only variable is the element. */
    const go_local_t *source = kind(right, "identifier") ? local_binding(c, text(c, right)) : NULL;
    bool received = names == 1 && source && source->channel;
    /* slices.Backward(xs), slices.All(xs) and maps.All(m) iterate what they were given. */
    TSNode iterated =
        names > 1 && (std_call(c, right, "slices", "Backward") ||
                      std_call(c, right, "slices", "All") || std_call(c, right, "maps", "All"))
            ? named_child(field(right, "arguments"), 0)
            : right;
    const char *container = names > 1 || received ? expression(c, iterated) : NULL;
    for (uint32_t i = 0; i < names; i++) {
        TSNode name = list_item(left, i);
        if (kind(name, "identifier"))
            bind(c, name, i == 1 || received ? element_of(c, right, container) : NULL, declaration);
    }
}
/* Every statement of one switch or select arm, in that arm's own frame. */
static void case_body(go_collect_t *c, TSNode node, unsigned depth) {
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(node, i);
        const char *role = ts_node_field_name_for_child(node, i);
        if (ts_node_is_named(child) && !role)
            statement(c, child, depth + 1);
    }
}
static void type_case(go_collect_t *c, TSNode clause, TSNode alias, unsigned depth) {
    if (!push_frame(c))
        return;
    uint32_t types = 0;
    TSNode only = {0};
    for (uint32_t i = 0; i < ts_node_child_count(clause); i++) {
        const char *role = ts_node_field_name_for_child(clause, i);
        if (role && !strcmp(role, "type")) {
            only = ts_node_child(clause, i);
            reference_types(c, only, 0);
            types++;
        }
    }
    /* A clause listing one type gives the alias that type; any other keeps the operand's. One
     * spelled alias binds once per clause, so its version belongs to the clause. */
    if (!ts_node_is_null(alias))
        for (uint32_t i = 0; i < list_count(alias); i++)
            bind_at(c, list_item(alias, i), clause, types == 1 ? type_fact(c, only) : NULL, true);
    case_body(c, clause, depth);
    pop_frame(c);
}
static void switch_statement(go_collect_t *c, TSNode node, unsigned depth, bool typed) {
    if (!push_frame(c))
        return;
    statement(c, field(node, "initializer"), depth + 1);
    walk_calls(c, field(node, "value"), 0);
    TSNode alias = typed ? field(node, "alias") : (TSNode){0};
    for (uint32_t i = 0; i < named_count(node); i++) {
        TSNode clause = named_child(node, i);
        if (kind(clause, "type_case") || (typed && kind(clause, "default_case"))) {
            type_case(c, clause, alias, depth);
        } else if (kind(clause, "expression_case") || kind(clause, "default_case") ||
                   kind(clause, "communication_case")) {
            if (!push_frame(c))
                break;
            walk_calls(c, field(clause, "value"), 0);
            statement(c, field(clause, "communication"), depth + 1);
            case_body(c, clause, depth);
            pop_frame(c);
        }
    }
    pop_frame(c);
}
/* Inside a parenthesized const list a spec without values repeats the last one that has them,
 * type included: `const (A T = iota; B; C)` makes all three T. */
static bool const_spec_repeats(go_collect_t *c, TSNode spec, TSNode *annotation, TSNode *value) {
    if (!kind(spec, "const_spec"))
        return false;
    if (ts_node_is_null(*value)) {
        *annotation = c->const_type;
        *value = c->const_value;
        return true;
    }
    c->const_type = *annotation;
    c->const_value = *value;
    return false;
}
static void statement(go_collect_t *c, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || depth >= 64)
        return;
    if (kind(node, "block") || kind(node, "statement_list")) {
        bool frame = kind(node, "block");
        if (frame && !push_frame(c))
            return;
        uint32_t count = named_count(node);
        for (uint32_t i = 0; i < count; i++)
            statement(c, named_child(node, i), depth + 1);
        if (frame)
            pop_frame(c);
        return;
    }
    if (kind(node, "type_declaration")) {
        for (uint32_t i = 0; i < named_count(node); i++) {
            TSNode declaration = named_child(node, i);
            TSNode name = field(declaration, "name");
            if (ts_node_is_null(name))
                continue;
            shadow(c, name, true);
            /* No method can be declared inside a function, so what a local struct selects is
             * written right here. `type T U` drops U's methods, and is left unknown, as a local
             * alias is. */
            TSNode declared = field(declaration, "type");
            bool renamed = kind(declaration, "type_alias") || kind(declared, "type_identifier") ||
                           kind(declared, "qualified_type") || kind(declared, "generic_type");
            go_local_t *item = local_binding(c, text(c, name));
            reference_types(c, declared, 0);
            if (item && !renamed)
                item->local_type = type_fact(c, declared);
        }
        return;
    }
    if (kind(node, "short_var_declaration")) {
        assignment(c, node, true);
        return;
    }
    if (kind(node, "assignment_statement")) {
        assignment(c, node, false);
        return;
    }
    if (kind(node, "var_declaration") || kind(node, "const_declaration") ||
        kind(node, "var_spec_list")) {
        c->const_type = c->const_value = (TSNode){0};
        for (uint32_t i = 0; i < named_count(node); i++)
            statement(c, named_child(node, i), depth + 1);
        return;
    }
    if (kind(node, "var_spec") || kind(node, "const_spec")) {
        TSNode annotation = field(node, "type"), value = field(node, "value");
        bool repeated = const_spec_repeats(c, node, &annotation, &value);
        if (!repeated) {
            walk_calls(c, value, 0);
            reference_types(c, annotation, 0);
        }
        const char *annotated = !ts_node_is_null(annotation) ? type_fact(c, annotation) : NULL;
        unsigned names = 0;
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            const char *role = ts_node_field_name_for_child(node, i);
            names += role && !strcmp(role, "name");
        }
        unsigned index = 0;
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            const char *role = ts_node_field_name_for_child(node, i);
            if (role && !strcmp(role, "name")) {
                TSNode name = ts_node_child(node, i);
                /* `var a, b = f()` takes f's results by position, as `a, b := f()` does. */
                const char *fact =
                    !ts_node_is_null(annotation) ? annotated
                    : names == list_count(value) ? expression(c, list_item(value, index))
                    : list_count(value) == 1     ? tuple_element(c, list_item(value, 0), index)
                                                 : NULL;
                TSNode initial = names == list_count(value) ? list_item(value, index) : (TSNode){0};
                index++;
                bind(c, name, fact, true);
                go_local_t *item = local_binding(c, text(c, name));
                if (item && !ts_node_is_null(annotation))
                    item->declared_fact = annotated;
                if (item && kind(annotation, "channel_type"))
                    item->channel = true;
                if (kind(annotation, "function_type"))
                    note_signature(c, name, annotation);
                else if (ts_node_is_null(annotation) && kind(initial, "func_literal"))
                    note_signature(c, name, initial);
            }
        }
        return;
    }
    if (kind(node, "if_statement")) {
        if (!push_frame(c))
            return;
        statement(c, field(node, "initializer"), depth + 1);
        walk_calls(c, field(node, "condition"), 0);
        statement(c, field(node, "consequence"), depth + 1);
        statement(c, field(node, "alternative"), depth + 1);
        pop_frame(c);
        return;
    }
    if (kind(node, "for_statement")) {
        if (!push_frame(c))
            return;
        for (uint32_t i = 0; i < named_count(node); i++) {
            TSNode part = named_child(node, i);
            if (kind(part, "for_clause")) {
                statement(c, field(part, "initializer"), depth + 1);
                walk_calls(c, field(part, "condition"), 0);
                statement(c, field(part, "update"), depth + 1);
            } else if (kind(part, "range_clause")) {
                range_clause(c, part);
            } else if (kind(part, "block")) {
                statement(c, part, depth + 1);
            } else {
                walk_calls(c, part, 0);
            }
        }
        pop_frame(c);
        return;
    }
    if (kind(node, "expression_switch_statement") || kind(node, "select_statement")) {
        switch_statement(c, node, depth, false);
        return;
    }
    if (kind(node, "type_switch_statement")) {
        switch_statement(c, node, depth, true);
        return;
    }
    if (kind(node, "labeled_statement")) {
        for (uint32_t i = 0; i < named_count(node); i++)
            if (!kind(named_child(node, i), "label_name"))
                statement(c, named_child(node, i), depth + 1);
        return;
    }
    if (kind(node, "receive_statement")) {
        TSNode left = field(node, "left"), right = field(node, "right");
        walk_calls(c, right, 0);
        bool declaration = has_token(node, ":=");
        for (uint32_t i = 0; i < list_count(left); i++) {
            TSNode name = list_item(left, i);
            if (kind(name, "identifier"))
                bind(c, name, i == 0 ? expression(c, right) : NULL, declaration);
        }
        return;
    }
    /* expression_statement, return, go, defer, send, inc, dec: only calls remain to visit. */
    walk_calls(c, node, 0);
}

static bool contribute_domain(cbm_go_stabilization_t *state, const char *domain, const char *key,
                              const cbm_resolve_proof_t *proof) {
    go_domain_t *entry = cbm_ht_get(state->domains, domain);
    if (!entry) {
        entry = cbm_arena_calloc(state->arena, sizeof(*entry));
        const char *copy = cbm_arena_strdup(state->arena, domain);
        if (!entry || !copy)
            return false;
        entry->key = copy;
        cbm_ht_set(state->domains, copy, entry);
        if (cbm_ht_get(state->domains, copy) != entry)
            return false;
        entry->next = state->first_domain;
        state->first_domain = entry;
    }
    go_support_t *support = cbm_arena_alloc(state->arena, sizeof(*support));
    if (!support)
        return false;
    *support = (go_support_t){key, proof, entry->first};
    entry->first = support;
    entry->count++;
    return true;
}
static bool contribute(go_collect_t *c, const char *domain, const char *key,
                       const cbm_resolve_proof_t *proof) {
    return contribute_domain(c->shared, domain, key, proof);
}
static go_type_info_t *type_info(cbm_go_stabilization_t *state, const char *qn) {
    go_type_info_t *info = cbm_ht_get(state->types, qn);
    if (info)
        return info;
    info = cbm_arena_calloc(state->arena, sizeof(*info));
    const char *copy = cbm_arena_strdup(state->arena, qn);
    if (!info || !copy) {
        state->failed = true;
        return NULL;
    }
    info->qn = copy;
    info->next = state->first_type;
    state->first_type = info;
    cbm_ht_set(state->types, copy, info);
    state->failed |= cbm_ht_get(state->types, copy) != info;
    return info;
}
/* Publish one member of a type under every selector domain that may reach it. */
static void publish_member(cbm_go_stabilization_t *state, const char *qn, const go_member_t *member,
                           bool requires_pointer) {
    const CBMType *named = cbm_type_named(state->arena, qn);
    const CBMType *pointer = cbm_type_pointer(state->arena, named);
    for (int indirect = 0; indirect < 2; indirect++) {
        const char *identity =
            cbm_resolve_solver_type_identity(state->arena, indirect ? pointer : named);
        if (member->declaration) {
            /* What the selected name denotes, as opposed to the value's type. */
            const char *selected = member_prefix(state->arena, member->module, member->name,
                                                 "go/selected:", "go/private-selected:");
            const char *domain =
                identity && selected
                    ? cbm_arena_sprintf(state->arena, "%s%s.%s", selected, identity, member->name)
                    : NULL;
            state->failed |=
                !domain || !contribute_domain(state, domain, member->declaration, member->proof);
        }
        if (member->field) {
            /* A function with several results has no one value type, but is still selected. */
            if (member->key != GO_UNKNOWN) {
                const char *prefix = member_prefix(state->arena, member->module, member->name,
                                                   "go/field:", "go/private-field:");
                const char *domain =
                    identity && prefix
                        ? cbm_arena_sprintf(state->arena, "%s%s.%s", prefix, identity, member->name)
                        : NULL;
                state->failed |=
                    !domain || !contribute_domain(state, domain, member->key, member->proof);
            }
            if (!member->function_value || !member->declaration)
                continue;
        }
        /* x.f() selects a function-typed field exactly as it selects a method. Its target is the
         * field, which is no callable declaration, so only the result's type comes of this. */
        const char *selected_key = member->field ? member->declaration : member->key;
        for (int addressable = 0; addressable < 2; addressable++) {
            if (requires_pointer && !indirect && !addressable)
                continue;
            const char *prefix = member_prefix(
                state->arena, member->module, member->name,
                addressable ? "go/addressable-member:" : "go/member:",
                addressable ? "go/private-addressable-member:" : "go/private-member:");
            const char *domain =
                identity && prefix
                    ? cbm_arena_sprintf(state->arena, "%s%s.%s", prefix, identity, member->name)
                    : NULL;
            state->failed |=
                !domain || !contribute_domain(state, domain, selected_key, member->proof);
        }
    }
}
static void add_member(go_collect_t *c, const char *qn, const char *name, const char *key,
                       const char *declaration, const cbm_resolve_proof_t *proof, bool is_field,
                       bool requires_pointer, bool function_value) {
    go_type_info_t *info = type_info(c->shared, qn);
    go_member_t *member = cbm_arena_calloc(c->shared->arena, sizeof(*member));
    const char *copy = name ? cbm_arena_strdup(c->shared->arena, name) : NULL;
    if (!info || !member || !copy) {
        c->shared->failed = true;
        return;
    }
    *member = (go_member_t){.name = copy,
                            .key = key ? key : GO_UNKNOWN,
                            .declaration = declaration,
                            .module = c->module,
                            .proof = proof,
                            .field = is_field,
                            .requires_pointer = requires_pointer,
                            .function_value = function_value,
                            .next = info->members};
    info->members = member;
    publish_member(c->shared, qn, member, requires_pointer);
}
/* The declaration a type expression names, as far as source alone can say. */
static const char *named_type_qn(go_collect_t *c, TSNode node) {
    if (kind(node, "generic_type"))
        node = field(node, "type");
    if (kind(node, "type_identifier")) {
        const char *name = text(c, node);
        return name && !local_binding(c, name)
                   ? cbm_arena_sprintf(c->shared->arena, "%s.%s", c->module, name)
                   : NULL;
    }
    if (!kind(node, "qualified_type"))
        return NULL;
    const char *package = text(c, field(node, "package")), *name = text(c, field(node, "name"));
    const char *target = package ? cbm_ht_get(c->import_targets, package) : NULL;
    return target && name ? cbm_arena_sprintf(c->shared->arena, "%s.%s", target, name) : NULL;
}
static void add_embed(go_collect_t *c, const char *owner, TSNode type) {
    bool pointer = kind(type, "pointer_type");
    if (pointer)
        type = named_child(type, 0);
    go_type_info_t *info = type_info(c->shared, owner);
    go_embed_t *embed = cbm_arena_calloc(c->shared->arena, sizeof(*embed));
    if (!info || !embed) {
        c->shared->failed = true;
        return;
    }
    /* Written `pkg.T` is another package by syntax. A bare name may be this package's, in a
     * file that was not collected. */
    bool foreign = kind(kind(type, "generic_type") ? field(type, "type") : type, "qualified_type");
    *embed = (go_embed_t){
        .qn = named_type_qn(c, type), .pointer = pointer, .foreign = foreign, .next = info->embeds};
    info->embeds = embed;
}
static void member_domains(go_collect_t *c, TSNode node, const char *name, const char *key,
                           const cbm_resolve_proof_t *proof) {
    TSNode receiver = field(node, "receiver");
    TSNode parameter = named_child(receiver, 0);
    TSNode receiver_type = field(parameter, "type");
    bool requires_pointer = kind(receiver_type, "pointer_type");
    if (requires_pointer)
        receiver_type = named_child(receiver_type, 0);
    if (kind(receiver_type, "generic_type"))
        receiver_type = field(receiver_type, "type");
    if (!kind(receiver_type, "type_identifier"))
        return;
    const char *qn = cbm_arena_sprintf(&c->call_arena, "%s.%s", c->module, text(c, receiver_type));
    add_member(c, qn, name, key, key, proof, false, requires_pointer, false);
}

static const CBMDefinition *definition(go_collect_t *c, TSNode node, TSNode name) {
    const char *key = cbm_arena_sprintf(&c->call_arena, "%u:%s", ts_node_start_point(node).row + 1,
                                        text(c, name));
    const void *value = key ? cbm_ht_get(c->definitions, key) : NULL;
    return value == c ? NULL : value;
}
static unsigned result_names(TSNode parameter) {
    unsigned names = 0;
    for (uint32_t child = 0; child < ts_node_child_count(parameter); child++) {
        const char *role = ts_node_field_name_for_child(parameter, child);
        names += role && !strcmp(role, "name");
    }
    return names ? names : 1;
}
/* A result list becomes one fact when it names one value, else one fact per position. */
static void declared_returns(go_collect_t *c, TSNode node, const char *qn,
                             const cbm_resolve_proof_t *proof) {
    TSNode returns = field(node, "result");
    if (ts_node_is_null(returns))
        return;
    const char *single = NULL;
    if (kind(returns, "parameter_list")) {
        unsigned index = 0, total = 0;
        for (uint32_t i = 0; i < named_count(returns); i++)
            total += result_names(named_child(returns, i));
        for (uint32_t i = 0; i < named_count(returns); i++) {
            TSNode parameter = named_child(returns, i);
            const char *fact = type_fact(c, field(parameter, "type"));
            for (unsigned n = 0; n < result_names(parameter); n++, index++) {
                if (total == 1)
                    single = fact;
                else if (fact && fact != GO_UNKNOWN)
                    check(c, contribute(
                                 c, cbm_arena_sprintf(&c->call_arena, "go/return#%u:%s", index, qn),
                                 fact, proof));
            }
        }
    } else {
        single = type_fact(c, returns);
    }
    if (single && single != GO_UNKNOWN)
        check(c,
              contribute(c, cbm_arena_sprintf(&c->call_arena, "go/return:%s", qn), single, proof));
}
static void shadow_type_parameters(go_collect_t *c, TSNode node) {
    TSNode generics = field(node, "type_parameters");
    for (uint32_t i = 0; i < named_count(generics); i++) {
        TSNode parameter = named_child(generics, i);
        for (uint32_t child = 0; child < ts_node_child_count(parameter); child++) {
            const char *role = ts_node_field_name_for_child(parameter, child);
            if (role && !strcmp(role, "name"))
                shadow(c, ts_node_child(parameter, child), true);
        }
    }
}
/* The fact naming one extracted declaration, or NULL when extraction has none. */
static const char *declared_target(go_collect_t *c, TSNode name, const char *target,
                                   const cbm_resolve_proof_t *proof) {
    const char *key = node_key(c, name, "declared-name");
    if (remember(c, &key))
        check(c, cbm_resolve_solver_leaf(
                     c->shared->solver, key, CBM_SOLVER_KNOWN,
                     (cbm_resolve_solver_value_t){.target = target,
                                                  .fact_kind = CBM_SOLVER_FACT_BINDING,
                                                  .declaration_identity = key},
                     proof));
    return key;
}
static const char *declared_name(go_collect_t *c, TSNode node, TSNode name,
                                 const cbm_resolve_proof_t *proof) {
    const CBMDefinition *def = definition(c, node, name);
    return def && def->qualified_name ? declared_target(c, name, def->qualified_name, proof) : NULL;
}
static void declared_returns(go_collect_t *c, TSNode node, const char *qn,
                             const cbm_resolve_proof_t *proof);
/* `func() (*T, error)`: several results, so no one value type, but each has its own. */
static bool several_results(TSNode function_type) {
    TSNode result = field(function_type, "result");
    if (ts_node_is_null(result) || !kind(result, "parameter_list"))
        return false;
    unsigned total = 0;
    for (uint32_t i = 0; i < named_count(result); i++)
        total += result_names(named_child(result, i));
    return total > 1;
}
static void struct_members(go_collect_t *c, const char *qn, TSNode declared,
                           const cbm_resolve_proof_t *proof, bool anonymous) {
    TSNode list = {0};
    for (uint32_t i = 0; i < named_count(declared); i++)
        if (kind(named_child(declared, i), "field_declaration_list"))
            list = named_child(declared, i);
    for (uint32_t i = 0; i < named_count(list); i++) {
        TSNode member = named_child(list, i);
        if (!kind(member, "field_declaration"))
            continue;
        TSNode type = field(member, "type");
        const char *fact = type_fact(c, type);
        bool named = false;
        for (uint32_t child = 0; child < ts_node_child_count(member); child++) {
            const char *role = ts_node_field_name_for_child(member, child);
            if (role && !strcmp(role, "name")) {
                named = true;
                TSNode name = ts_node_child(member, child);
                const char *declaration = declared_name(c, member, name, proof);
                const CBMDefinition *def = definition(c, member, name);
                const char *field_qn = def ? def->qualified_name : NULL;
                /* Selecting one only ever yields its result's type: `func() int` has none. */
                bool yields =
                    kind(type, "function_type") && (fact != GO_UNKNOWN || several_results(type));
                /* A field of a struct literal has no graph node. It still has a declared result:
                 * `tt.setup()`. A name nothing can emit, so that only the type comes of this. */
                if (yields && !def && anonymous) {
                    field_qn = cbm_arena_sprintf(c->shared->arena, "%s.%s", qn, text(c, name));
                    declaration = field_qn ? declared_target(c, name, field_qn, proof) : NULL;
                }
                bool function_value = yields && declaration && field_qn;
                add_member(c, qn, text(c, name), fact, declaration, proof, true, false,
                           function_value);
                if (function_value)
                    declared_returns(c, type, field_qn, proof);
            }
        }
        if (named)
            continue;
        /* An embedded field is selectable under its base type's own name. */
        TSNode base = kind(type, "pointer_type") ? named_child(type, 0) : type;
        if (kind(base, "generic_type"))
            base = field(base, "type");
        TSNode spelled = kind(base, "qualified_type") ? field(base, "name") : base;
        add_member(c, qn, text(c, spelled), fact, declared_name(c, member, spelled, proof), proof,
                   true, false, false);
        add_embed(c, qn, type);
    }
}
static void interface_members(go_collect_t *c, const char *qn, TSNode declared,
                              const cbm_resolve_proof_t *proof) {
    for (uint32_t i = 0; i < named_count(declared); i++) {
        TSNode member = named_child(declared, i);
        if (kind(member, "type_elem")) {
            if (named_count(member) == 1)
                add_embed(c, qn, named_child(member, 0));
            else
                type_info(c->shared, qn)->open = true;
            continue;
        }
        if (!kind(member, "method_elem"))
            continue;
        TSNode name = field(member, "name");
        const CBMDefinition *def = definition(c, member, name);
        const char *name_text = text(c, name);
        const char *target = def && def->qualified_name
                                 ? def->qualified_name
                                 : cbm_arena_sprintf(&c->call_arena, "%s.%s", qn, name_text);
        const char *key = node_key(c, member, "declaration");
        if (!remember(c, &key))
            continue;
        check(c, cbm_resolve_solver_leaf(
                     c->shared->solver, key, CBM_SOLVER_KNOWN,
                     (cbm_resolve_solver_value_t){.target = target,
                                                  .fact_kind = CBM_SOLVER_FACT_BINDING,
                                                  .declaration_identity = key},
                     proof));
        add_member(c, qn, name_text, key, key, proof, false, false, false);
        declared_returns(c, member, target, proof);
    }
}
static void type_declaration(go_collect_t *c, TSNode node, const CBMDefinition *def) {
    TSNode name = field(node, "name");
    const char *name_text = text(c, name);
    const char *key = node_key(c, node, "declaration");
    if (!remember(c, &key))
        return;
    const cbm_resolve_proof_t *proof = audit(c, node, key, "source_declaration_identity");
    const char *domain = cbm_arena_sprintf(&c->call_arena, "go/type:%s.%s", c->module, name_text);
    const char *qn = cbm_arena_sprintf(&c->call_arena, "%s.%s", c->module, name_text);
    TSNode declared = field(node, "type");
    bool alias = kind(node, "type_alias");
    if (!push_frame(c))
        return;
    shadow_type_parameters(c, node);
    reference_types(c, declared, 0);
    const char *named_declaration = declared_name(c, node, name, proof);
    if (named_declaration)
        check(c, contribute(c, cbm_arena_sprintf(&c->call_arena, "go/decl:%s", qn),
                            named_declaration, proof));
    if (alias) {
        const char *aliased = type_fact(c, declared);
        go_type_info_t *info = type_info(c->shared, qn);
        if (info)
            info->open = true; /* Its members are the aliased declaration's. */
        if (!aliased || aliased == GO_UNKNOWN) {
            pop_frame(c);
            return;
        }
        check(c,
              cbm_resolve_solver_type(c->shared->solver, key, aliased, CBM_TYPE_NAMED, 0, proof));
    } else {
        /* Every defined type has its own nominal identity and method set, whatever its
         * underlying type is. `type bitmap []uint64` selects methods exactly as a struct. */
        const CBMType *named = cbm_type_named(&c->call_arena, qn);
        check(c, cbm_resolve_solver_leaf(
                     c->shared->solver, key, CBM_SOLVER_KNOWN,
                     (cbm_resolve_solver_value_t){.target = def ? def->qualified_name : qn,
                                                  .fact_kind = CBM_SOLVER_FACT_TYPE,
                                                  .type = named,
                                                  .declaration_identity = key},
                     proof));
        go_type_info_t *info = type_info(c->shared, qn);
        if (info) {
            info->declared = true;
            info->interface = kind(declared, "interface_type");
            /* A universe type has no fields or methods to keep, except error's method. */
            const char *written = kind(declared, "type_identifier") ? text(c, declared) : NULL;
            if ((kind(declared, "type_identifier") || kind(declared, "qualified_type") ||
                 kind(declared, "generic_type")) &&
                (!written || !universe_type(written) || !strcmp(written, "error")))
                info->underlying = named_type_qn(c, declared);
        }
        if (kind(declared, "struct_type"))
            struct_members(c, qn, declared, proof, false);
        else if (kind(declared, "interface_type"))
            interface_members(c, qn, declared, proof);
        const char *element = NULL;
        if (kind(declared, "slice_type") || kind(declared, "array_type") ||
            kind(declared, "implicit_length_array_type"))
            element = type_fact(c, field(declared, "element"));
        else if (kind(declared, "map_type") || kind(declared, "channel_type"))
            element = type_fact(c, field(declared, "value"));
        /* Calling a value of `type F func() R` yields R, as for a function type written out.
         * F(x) still converts to F, so one result cannot live under go/return:F. */
        else if (kind(declared, "function_type")) {
            element = function_result(c, declared);
            if (several_results(declared))
                declared_returns(c, declared, def ? def->qualified_name : qn, proof);
        }
        const char *identity = element && element != GO_UNKNOWN
                                   ? cbm_resolve_solver_type_identity(&c->call_arena, named)
                                   : NULL;
        if (identity)
            check(c, contribute(c, cbm_arena_sprintf(&c->call_arena, "go/element:%s", identity),
                                element, proof));
        /* T(x) converts to T: the name is callable syntax whose result has type T. */
        check(c, contribute(c, cbm_arena_sprintf(&c->call_arena, "go/binding:%s", qn), key, proof));
        check(c, contribute(c, cbm_arena_sprintf(&c->call_arena, "go/return:%s", qn), key, proof));
    }
    pop_frame(c);
    check(c, contribute(c, domain, key, proof));
}
static void declaration(go_collect_t *c, TSNode node) {
    TSNode name = field(node, "name");
    bool type = kind(node, "type_spec") || kind(node, "type_alias");
    const CBMDefinition *def = definition(c, node, name);
    if (type) {
        type_declaration(c, node, def);
        return;
    }
    if (!def || !def->qualified_name)
        return;
    const char *name_text = text(c, name);
    const char *key = node_key(c, node, "declaration");
    if (!remember(c, &key))
        return;
    const cbm_resolve_proof_t *proof = audit(c, node, key, "source_declaration_identity");
    const char *domain =
        cbm_arena_sprintf(&c->call_arena, "go/binding:%s.%s", c->module, name_text);
    check(c,
          cbm_resolve_solver_leaf(c->shared->solver, key, CBM_SOLVER_KNOWN,
                                  (cbm_resolve_solver_value_t){.target = def->qualified_name,
                                                               .fact_kind = CBM_SOLVER_FACT_BINDING,
                                                               .declaration_identity = key},
                                  proof));
    if (kind(node, "method_declaration")) {
        member_domains(c, node, name_text, key, proof);
    } else {
        check(c, contribute(c, domain, key, proof));
        check(c, contribute(
                     c, cbm_arena_sprintf(&c->call_arena, "go/decl:%s.%s", c->module, name_text),
                     key, proof));
    }
    c->scope = cbm_arena_strdup(c->shared->arena, def->qualified_name);
    if (!push_frame(c))
        return;
    shadow_type_parameters(c, node);
    TSNode receiver_type = field(named_child(field(node, "receiver"), 0), "type");
    if (kind(receiver_type, "pointer_type"))
        receiver_type = named_child(receiver_type, 0);
    TSNode receiver_arguments = field(receiver_type, "type_arguments");
    for (uint32_t i = 0; i < named_count(receiver_arguments); i++) {
        TSNode argument = named_child(receiver_arguments, i);
        /* Grammar type_elem wraps each receiver type-parameter identifier. */
        while (!kind(argument, "type_identifier") && named_count(argument) == 1)
            argument = named_child(argument, 0);
        if (kind(argument, "type_identifier"))
            shadow(c, argument, true);
    }
    declared_returns(c, node, def->qualified_name, proof);
    reference_types(c, field(node, "result"), 0);
    bind_parameters(c, field(node, "parameters"));
    bind_parameters(c, field(node, "receiver"));
    bind_parameters(c, field(node, "result"));
    statement(c, field(node, "body"), 0);
    c->scope = NULL;
    pop_frame(c);
}
/* `var Default = New()` and `var Default *T`: a package variable's type is declared once,
 * and its initializer's calls belong to this file at package level. */
static void package_values(go_collect_t *c, TSNode node) {
    if (kind(node, "var_declaration") || kind(node, "const_declaration") ||
        kind(node, "var_spec_list")) {
        c->const_type = c->const_value = (TSNode){0};
        for (uint32_t i = 0; i < named_count(node); i++)
            package_values(c, named_child(node, i));
        return;
    }
    if (!kind(node, "var_spec") && !kind(node, "const_spec"))
        return;
    TSNode annotation = field(node, "type"), value = field(node, "value");
    bool repeated = const_spec_repeats(c, node, &annotation, &value);
    c->scope = c->module;
    if (!repeated) {
        walk_calls(c, value, 0);
        reference_types(c, annotation, 0);
    }
    unsigned names = 0;
    for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
        const char *role = ts_node_field_name_for_child(node, i);
        names += role && !strcmp(role, "name");
    }
    unsigned index = 0;
    for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
        const char *role = ts_node_field_name_for_child(node, i);
        if (!role || strcmp(role, "name"))
            continue;
        TSNode name_node = ts_node_child(node, i);
        const char *name = text(c, name_node);
        const cbm_resolve_proof_t *declared =
            audit(c, name_node, node_key(c, name_node, "package-value"), "source_package_value");
        const char *named_declaration = declared_name(c, name_node, name_node, declared);
        if (named_declaration && name && strcmp(name, "_"))
            check(c,
                  contribute(c, cbm_arena_sprintf(&c->call_arena, "go/decl:%s.%s", c->module, name),
                             named_declaration, declared));
        const char *fact = !ts_node_is_null(annotation) ? type_fact(c, annotation)
                           : names == list_count(value) ? expression(c, list_item(value, index))
                           : list_count(value) == 1 ? tuple_element(c, list_item(value, 0), index)
                                                    : NULL;
        index++;
        if (fact && fact != GO_UNKNOWN && name && strcmp(name, "_"))
            check(c,
                  contribute(c, cbm_arena_sprintf(&c->call_arena, "go/var:%s.%s", c->module, name),
                             fact, declared));
    }
    c->scope = NULL;
}
cbm_go_stabilization_t *cbm_go_stabilization_new(CBMArena *arena, cbm_resolve_proof_pool_t *proofs,
                                                 cbm_resolve_solver_t *solver) {
    cbm_go_stabilization_t *state = cbm_arena_calloc(arena, sizeof(*state));
    if (!state)
        return NULL;
    state->arena = arena;
    state->proofs = proofs;
    state->solver = solver;
    state->domains = cbm_ht_create(128);
    state->registered = cbm_ht_create(128);
    state->names = cbm_ht_create(128);
    state->sites = cbm_ht_create(128);
    state->types = cbm_ht_create(128);
    state->file_refs = cbm_ht_create(128);
    state->packages = cbm_ht_create(128);
    if (!state->domains || !state->registered || !state->names || !state->sites || !state->types ||
        !state->file_refs || !state->packages) {
        cbm_go_stabilization_dispose(state);
        return NULL;
    }
    return state;
}
void cbm_go_stabilization_dispose(cbm_go_stabilization_t *state) {
    if (!state)
        return;
    cbm_ht_free(state->domains);
    cbm_ht_free(state->registered);
    cbm_ht_free(state->names);
    cbm_ht_free(state->sites);
    cbm_ht_free(state->types);
    cbm_ht_free(state->file_refs);
    cbm_ht_free(state->packages);
}
/* The package clause is the file's first token: only comments and space precede it. */
static bool package_clause(const char *source, size_t length, const char **name, size_t *size) {
    size_t at = 0;
    while (at < length) {
        unsigned char ch = (unsigned char)source[at];
        if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') {
            at++;
        } else if (at + 1 < length && ch == '/' && source[at + 1] == '/') {
            const char *end = memchr(source + at, '\n', length - at);
            at = end ? (size_t)(end - source) + 1 : length;
        } else if (at + 1 < length && ch == '/' && source[at + 1] == '*') {
            size_t close = at + 2;
            while (close + 1 < length && !(source[close] == '*' && source[close + 1] == '/'))
                close++;
            at = close + 2;
        } else {
            break;
        }
    }
    if (at + 8 > length || strncmp(source + at, "package", 7) ||
        (source[at + 7] != ' ' && source[at + 7] != '\t'))
        return false;
    at += 8;
    while (at < length && (source[at] == ' ' || source[at] == '\t'))
        at++;
    size_t end = at;
    while (end < length && (source[end] == '_' || (source[end] >= '0' && source[end] <= '9') ||
                            (source[end] >= 'a' && source[end] <= 'z') ||
                            (source[end] >= 'A' && source[end] <= 'Z')))
        end++;
    *name = source + at;
    *size = end - at;
    return end > at;
}
/* `package main` and an external test package may share a directory with the package an
 * import means. Two other names there leave it unknown, keeping the path's last segment. */
bool cbm_go_stabilization_note_package(cbm_go_stabilization_t *state, const char *module,
                                       const char *source, size_t length) {
    const char *name = NULL;
    size_t size = 0;
    if (!state || !module || !source)
        return false;
    if (!package_clause(source, length, &name, &size))
        return true;
    if ((size == 4 && !strncmp(name, "main", 4)) ||
        (size > 5 && !strncmp(name + size - 5, "_test", 5)))
        return true;
    const char *known = cbm_ht_get(state->packages, module);
    if (known) {
        if (*known && (strlen(known) != size || strncmp(known, name, size)))
            cbm_ht_set(state->packages, cbm_arena_strdup(state->arena, module),
                       (void *)cbm_arena_strdup(state->arena, ""));
        return true;
    }
    const char *copy = cbm_arena_strndup(state->arena, name, size);
    const char *key = cbm_arena_strdup(state->arena, module);
    if (!copy || !key)
        return false;
    cbm_ht_set(state->packages, key, (void *)copy);
    return cbm_ht_get(state->packages, key) == copy;
}
/* The name this import makes visible in the file. An alias is that name. Without one it is
 * the declared name of the package imported, which is not always the path's last segment;
 * an explicit alias equal to that segment is indistinguishable here and also abstains. */
static const char *import_binding_name(const cbm_go_stabilization_t *state, const CBMImport *import,
                                       const cbm_resolve_import_fact_t *fact) {
    const char *name = import->local_name;
    if (!fact || !fact->closed || !cbm_resolve_admit(&fact->dependency) || !import->module_path)
        return name;
    const char *slash = strrchr(import->module_path, '/');
    const char *last = slash ? slash + 1 : import->module_path;
    if (strcmp(name, last))
        return name;
    const char *declared = cbm_ht_get(state->packages, fact->dependency.target);
    return declared && *declared ? declared : name;
}
static int compare_ref(const void *left, const void *right) {
    const go_ref_t *a = left, *b = right;
    if (a->start != b->start)
        return a->start < b->start ? -1 : 1;
    if (a->end != b->end)
        return a->end < b->end ? -1 : 1;
    return 0;
}
static void publish_references(go_collect_t *c) {
    if (!c->ref_count)
        return;
    qsort(c->refs, c->ref_count, sizeof(*c->refs), compare_ref);
    /* A signature's named result is visited twice. Equal entries keep their order unspecified
     * by qsort, so one token keeps one answer: its key if every visit agrees, else none. */
    size_t kept = 0;
    for (size_t i = 0; i < c->ref_count; i++) {
        go_ref_t *last = kept ? &c->refs[kept - 1] : NULL;
        if (last && !compare_ref(last, &c->refs[i])) {
            if (!last->key || !c->refs[i].key || strcmp(last->key, c->refs[i].key))
                last->key = NULL;
            continue;
        }
        c->refs[kept++] = c->refs[i];
    }
    c->ref_count = kept;
    go_file_refs_t *table = cbm_arena_calloc(c->shared->arena, sizeof(*table));
    go_ref_t *items = cbm_arena_alloc(c->shared->arena, c->ref_count * sizeof(*items));
    if (!table || !items) {
        c->shared->failed = true;
        return;
    }
    memcpy(items, c->refs, c->ref_count * sizeof(*items));
    *table = (go_file_refs_t){items, c->ref_count};
    put(c, c->shared->file_refs, c->context.file, table);
}
bool cbm_go_stabilization_collect(cbm_go_stabilization_t *state, const CBMFileResult *result,
                                  const char *source, size_t length,
                                  const cbm_resolve_proof_source_t *context, const char *module,
                                  const cbm_resolve_import_snapshot_t *imports) {
    if (!state || state->sealed || state->failed || !result || result->imports.count < 0 ||
        result->calls.count < 0 || (result->calls.count && !result->calls.items) || !source ||
        length > UINT32_MAX || !context || !context->file || !context->project || !module)
        return false;
    if (context->source_origin != CBM_SOURCE_ORIGIN_RAW || !result->source_file ||
        strcmp(result->source_file, context->file) || !result->source_project ||
        strcmp(result->source_project, context->project) || !context->content_identity ||
        strcmp(result->source_content_id[CBM_SOURCE_ORIGIN_RAW], context->content_identity) ||
        !context->configuration_identity || !result->source_configuration_id[0] ||
        strcmp(result->source_configuration_id, context->configuration_identity) ||
        !cbm_result_source_matches(result, source, length, CBM_SOURCE_ORIGIN_RAW))
        return false;
    TSParser *parser = ts_parser_new();
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_go());
    TSTree *tree = cbm_parse_source_repairing(parser, CBM_LANG_GO, source, (uint32_t)length,
                                              (TSParseOptions){0});
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    go_collect_t c = {.shared = state,
                      .result = result,
                      .source = source,
                      .module = cbm_arena_strdup(state->arena, module),
                      .context = *context};
    c.context.file = cbm_arena_strdup(state->arena, context->file);
    cbm_arena_init(&c.call_arena);
    cbm_resolve_proof_source_t *site_source = cbm_arena_alloc(state->arena, sizeof(*site_source));
    if (site_source) {
        *site_source = (cbm_resolve_proof_source_t){
            .project = cbm_arena_strdup(state->arena, context->project),
            .file = c.context.file,
            .module = c.module,
            .content_identity = cbm_arena_strdup(state->arena, context->content_identity),
            .configuration_identity =
                cbm_arena_strdup(state->arena, context->configuration_identity),
            .source_origin = context->source_origin};
        c.site_source = site_source;
    } else {
        state->failed = true;
    }
    c.imports = cbm_ht_create(16);
    c.import_targets = cbm_ht_create(16);
    c.definitions = cbm_ht_create(64);
    c.calls = cbm_ht_create((uint32_t)result->calls.count);
    if (!c.imports || !c.import_targets || !c.definitions || !c.calls) {
        cbm_ht_free(c.imports);
        cbm_ht_free(c.import_targets);
        cbm_ht_free(c.definitions);
        cbm_ht_free(c.calls);
        cbm_arena_destroy(&c.call_arena);
        ts_tree_delete(tree);
        return false;
    }
    /* One index construction per file; duplicate carriers never borrow the
     * first spelling/scope. The sentinel is local and never escapes collection. */
    for (int i = 0; i < result->calls.count; i++) {
        const CBMCall *call = &result->calls.items[i];
        if (call->source_origin != CBM_SOURCE_ORIGIN_RAW || !call->enclosing_func_qn ||
            !call->callee_name || !*call->callee_name ||
            call->site_end_byte <= call->site_start_byte || call->site_end_byte > length)
            continue;
        const char *key =
            carrier_key(&c, call->site_start_byte, call->site_end_byte, call->enclosing_func_qn);
        put(&c, c.calls, key, key && cbm_ht_get(c.calls, key) ? &c : (void *)call);
    }
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *def = &result->defs.items[i];
        if (!def->name)
            continue;
        const char *key = cbm_arena_sprintf(&c.call_arena, "%d:%s", def->start_line, def->name);
        put(&c, c.definitions, key, cbm_ht_get(c.definitions, key) ? &c : (void *)def);
    }
    CBMHashTable *import_counts = cbm_ht_create(16);
    if (!import_counts)
        state->failed = true;
    bool snapshot =
        imports && imports->complete && (!imports->count || imports->items) &&
        imports->count == (size_t)result->imports.count && imports->source.file && context->file &&
        !strcmp(imports->source.file, context->file) && imports->source.project &&
        context->project && !strcmp(imports->source.project, context->project) &&
        imports->source.source_origin == CBM_SOURCE_ORIGIN_RAW &&
        imports->source.content_identity && context->content_identity &&
        !strcmp(imports->source.content_identity, context->content_identity) &&
        imports->source.configuration_identity && context->configuration_identity &&
        !strcmp(imports->source.configuration_identity, context->configuration_identity);
    /* `v1 "k8s.io/api/core/v1"` beside `"example/repo/v1"` of package repo: two names, however
     * alike their paths end. */
    for (int i = 0; import_counts && i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (!import->local_name)
            continue;
        const char *bound =
            import_binding_name(state, import, snapshot ? &imports->items[i] : NULL);
        put(&c, import_counts, bound, cbm_ht_get(import_counts, bound) ? &c : (void *)import);
    }
    for (int i = 0; import_counts && i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        const char *name = import->local_name;
        if (!name || !*name || !strcmp(name, "_") || !strcmp(name, "."))
            continue;
        const cbm_resolve_import_fact_t *fact = snapshot ? &imports->items[i] : NULL;
        const char *bound = import_binding_name(state, import, fact);
        const char *key = cbm_arena_sprintf(state->arena, "go/import:%zu:%s:%s",
                                            strlen(context->file), context->file, bound);
        if (cbm_ht_get(c.imports, bound))
            continue;
        put(&c, c.imports, bound, (void *)key);
        bool unique = cbm_ht_get(import_counts, bound) != &c;
        bool known = unique && fact && fact->closed && fact->local_name &&
                     !strcmp(fact->local_name, name) && fact->module_path && import->module_path &&
                     !strcmp(fact->module_path, import->module_path) &&
                     fact->start_byte == import->site_start_byte &&
                     fact->end_byte == import->site_end_byte && fact->end_byte > fact->start_byte &&
                     fact->source_origin == CBM_SOURCE_ORIGIN_RAW &&
                     cbm_resolve_admit(&fact->dependency) && fact->dependency.proof;
        const cbm_resolve_proof_t *proof = audit(&c, root, key, "normalized_source_import_binding");
        if (known && unique)
            put(&c, c.import_targets, bound,
                (void *)cbm_arena_strdup(state->arena, fact->dependency.target));
        if (known) {
            const cbm_resolve_proof_view_t *path = cbm_resolve_proof_view(fact->dependency.proof);
            known = path && path->predecessors_closed && cbm_resolve_admit(&path->outcome) &&
                    !strcmp(path->outcome.target, fact->dependency.target) &&
                    path->key.start_byte == fact->start_byte &&
                    path->key.end_byte == fact->end_byte && path->source.file &&
                    !strcmp(path->source.file, context->file) && path->source.content_identity &&
                    !strcmp(path->source.content_identity, context->content_identity) &&
                    path->source.configuration_identity &&
                    !strcmp(path->source.configuration_identity, context->configuration_identity);
            if (known) {
                const cbm_resolve_proof_t *premises[] = {proof, fact->dependency.proof};
                cbm_resolve_proof_key_t binding = {.lookup_domain = module,
                                                   .lookup_key = bound,
                                                   .binding_identity = key,
                                                   .lookup_closed = true,
                                                   .detail = "normalized_source_import_binding"};
                proof = cbm_resolve_proof_derive(state->proofs, CBM_PROOF_IMPORT_BINDING, context,
                                                 &binding, &fact->dependency, premises, 2, true);
            }
        }
        check(&c, cbm_resolve_solver_leaf(
                      state->solver, key,
                      !unique ? CBM_SOLVER_AMBIGUOUS
                      : known ? CBM_SOLVER_KNOWN
                              : CBM_SOLVER_UNKNOWN,
                      (cbm_resolve_solver_value_t){.target = known ? fact->dependency.target : NULL,
                                                   .module = true},
                      proof));
    }
    cbm_ht_free(import_counts);
    for (uint32_t i = 0; i < named_count(root); i++) {
        TSNode child = named_child(root, i);
        /* A syntax error is confined to its own top-level declaration: nothing else in the
         * file depends on it, and what it declares is simply unknown to the rest. Grammar
         * gaps such as Go 1.26's new(expr) otherwise cost a whole file. */
        if (ts_node_has_error(child))
            continue;
        if (kind(child, "type_declaration")) {
            for (uint32_t n = 0; n < named_count(child); n++)
                declaration(&c, named_child(child, n));
        } else if (kind(child, "function_declaration") || kind(child, "method_declaration")) {
            declaration(&c, child);
        } else if (kind(child, "var_declaration") || kind(child, "const_declaration")) {
            package_values(&c, child);
        }
    }
    publish_references(&c);
    cbm_free(CBM_MEM_CLASS_SEMANTIC, c.refs);
    cbm_ht_free(c.imports);
    cbm_ht_free(c.import_targets);
    cbm_ht_free(c.definitions);
    cbm_ht_free(c.calls);
    cbm_arena_destroy(&c.call_arena);
    ts_tree_delete(tree);
    return !state->failed;
}
static void register_domain(cbm_go_stabilization_t *state, const go_domain_t *domain) {
    const char **dependencies =
        cbm_arena_alloc(state->arena, domain->count * sizeof(*dependencies));
    if (!dependencies) {
        state->failed = true;
        return;
    }
    size_t n = 0;
    const go_support_t *first = domain->first;
    for (const go_support_t *support = domain->first; support; support = support->next) {
        dependencies[n++] = support->key;
        if (strcmp(support->key, first->key) < 0)
            first = support;
    }
    state->failed |=
        !cbm_resolve_solver_join(state->solver, domain->key, dependencies, n, first->audit);
}
/* Go's selector rule for embedded fields: the shallowest depth holding the name decides,
 * and more than one holder at that depth makes the selector illegal. A type whose names
 * cannot be enumerated (an embedded type outside the project, or an alias) might hold the
 * name at its own depth, so no name at or below that depth is proved. */
typedef struct go_level_item {
    const go_type_info_t *type;
    bool pointer;
    struct go_level_item *next;
} go_level_item_t;
typedef struct {
    const go_member_t *member;
    bool pointer;
    unsigned holders;
} go_candidate_t;
static void promote_embedded(cbm_go_stabilization_t *state, const go_type_info_t *owner) {
    CBMHashTable *settled = cbm_ht_create(32), *visited = cbm_ht_create(32);
    if (!settled || !visited) {
        state->failed = true;
        cbm_ht_free(settled);
        cbm_ht_free(visited);
        return;
    }
    for (const go_member_t *member = owner->members; member; member = member->next)
        cbm_ht_set(settled, member->name, (void *)member);
    cbm_ht_set(visited, owner->qn, (void *)owner);
    go_level_item_t *level = NULL;
    /* open: nothing at or below is proved. hidden: an embedded type from another package has
     * unknown names, but non-exported names of different packages are different identifiers
     * and that package cannot import this one back, so only exported names are in doubt. */
    bool open = owner->open, hidden = false;
    for (const go_embed_t *embed = owner->embeds; embed; embed = embed->next) {
        const go_type_info_t *type = embed->qn ? cbm_ht_get(state->types, embed->qn) : NULL;
        if (!type || !type->declared || type->open) {
            hidden |= embed->foreign;
            open |= !embed->foreign;
            continue;
        }
        go_level_item_t *item = cbm_arena_alloc(state->arena, sizeof(*item));
        if (!item) {
            state->failed = true;
            break;
        }
        *item = (go_level_item_t){type, embed->pointer, level};
        level = item;
    }
    while (level && !open && !state->failed) {
        CBMHashTable *candidates = cbm_ht_create(32);
        if (!candidates) {
            state->failed = true;
            break;
        }
        go_level_item_t *next = NULL;
        bool hidden_here = hidden;
        for (const go_level_item_t *item = level; item; item = item->next) {
            for (const go_member_t *member = item->type->members; member; member = member->next) {
                go_candidate_t *found = cbm_ht_get(candidates, member->name);
                if (found) {
                    found->holders++;
                    continue;
                }
                found = cbm_arena_calloc(state->arena, sizeof(*found));
                if (!found) {
                    state->failed = true;
                    break;
                }
                *found = (go_candidate_t){member, item->pointer, 1};
                cbm_ht_set(candidates, member->name, found);
            }
            for (const go_embed_t *embed = item->type->embeds; embed; embed = embed->next) {
                const go_type_info_t *type = embed->qn ? cbm_ht_get(state->types, embed->qn) : NULL;
                if (type && cbm_ht_get(visited, type->qn))
                    continue;
                if (!type || !type->declared || type->open) {
                    hidden |= embed->foreign;
                    open |= !embed->foreign;
                    continue;
                }
                go_level_item_t *deeper = cbm_arena_alloc(state->arena, sizeof(*deeper));
                if (!deeper) {
                    state->failed = true;
                    break;
                }
                *deeper = (go_level_item_t){type, item->pointer || embed->pointer, next};
                next = deeper;
            }
        }
        for (const go_level_item_t *item = level; item; item = item->next) {
            for (const go_member_t *member = item->type->members; member; member = member->next) {
                const go_candidate_t *found = cbm_ht_get(candidates, member->name);
                if (!found || found->member != member)
                    continue;
                if (found->holders == 1 && !cbm_ht_get(settled, member->name) &&
                    !(hidden_here && exported_name(member->name)))
                    publish_member(state, owner->qn, member,
                                   member->requires_pointer && !found->pointer);
                cbm_ht_set(settled, member->name, (void *)member);
            }
            cbm_ht_set(visited, item->type->qn, (void *)item->type);
        }
        cbm_ht_free(candidates);
        level = next;
    }
    cbm_ht_free(settled);
    cbm_ht_free(visited);
}
/* `type A B` declares a new type: it does not inherit the methods bound to B, but the method set
 * of an interface and the fields of a struct remain, as do a struct's embedded fields and so
 * what they promote. A B whose names cannot be enumerated leaves A with its own methods. */
static void inherit_underlying(cbm_go_stabilization_t *state, go_type_info_t *owner) {
    const go_type_info_t *base = cbm_ht_get(state->types, owner->underlying);
    const go_type_info_t *hare = base;
    while (base && base->underlying) {
        base = cbm_ht_get(state->types, base->underlying);
        for (int step = 0; step < 2 && hare; step++)
            hare = hare->underlying ? cbm_ht_get(state->types, hare->underlying) : NULL;
        if (base && base == hare)
            return; /* A cycle is invalid Go and has no underlying type. */
    }
    if (!base || !base->declared || base->open) {
        owner->open = true;
        return;
    }
    for (const go_member_t *member = base->members; member; member = member->next) {
        if (!base->interface && !member->field)
            continue;
        go_member_t *copy = cbm_arena_alloc(state->arena, sizeof(*copy));
        if (!copy) {
            state->failed = true;
            return;
        }
        *copy = *member;
        copy->next = owner->members;
        owner->members = copy;
        publish_member(state, owner->qn, copy, copy->requires_pointer);
    }
    for (const go_embed_t *embed = base->embeds; embed; embed = embed->next) {
        go_embed_t *copy = cbm_arena_alloc(state->arena, sizeof(*copy));
        if (!copy) {
            state->failed = true;
            return;
        }
        *copy = *embed;
        copy->next = owner->embeds;
        owner->embeds = copy;
    }
}
bool cbm_go_stabilization_register(cbm_go_stabilization_t *state) {
    if (!state || state->failed)
        return false;
    for (go_type_info_t *type = state->first_type; type; type = type->next)
        if (type->underlying)
            inherit_underlying(state, type);
    for (const go_type_info_t *type = state->first_type; type; type = type->next)
        if (type->embeds)
            promote_embedded(state, type);
    for (const go_domain_t *domain = state->first_domain; domain; domain = domain->next)
        register_domain(state, domain);
    return !state->failed;
}
bool cbm_go_stabilization_seal(cbm_go_stabilization_t *state) {
    if (!state || state->failed || state->sealed)
        return false;
    state->sealed = true;
    for (go_site_t *site = state->first_site; site; site = site->next) {
        if (!site->key) {
            site->result = (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                                  .reason = "shared_binding_type_fixed_point"};
            continue;
        }
        if (!site->carrier_exact) {
            site->result =
                (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                       .reason = "shared_call_carrier_missing_or_ambiguous"};
            continue;
        }
        cbm_resolve_solver_result_t binding;
        cbm_resolve_solver_get(state->solver, site->key, &binding);
        cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                       .reason = "shared_binding_type_fixed_point"};
        /* T(x) has the syntax of a call and selects the type's declaration. Calling a local
         * function value also reaches a type, the one it returns, but names no type here. */
        bool conversion = binding.status == CBM_SOLVER_KNOWN &&
                          binding.value.fact_kind == CBM_SOLVER_FACT_TYPE &&
                          strncmp(site->key, "go/callable-from:", 17) != 0;
        const char *role = conversion ? "USES_TYPE" : "CALLS";
        switch (binding.status) {
        case CBM_SOLVER_KNOWN:
            result = (cbm_resolve_result_t){
                .binding = CBM_RESOLVE_BINDING_UNIQUE,
                .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                .rule = site->rule,
                .facts =
                    cbm_resolve_rule_required_facts(site->rule) | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                .target = binding.value.target,
                .candidate_count = 1,
                .reason = conversion ? cbm_go_type_conversion : "shared_binding_type_fixed_point"};
            break;
        case CBM_SOLVER_AMBIGUOUS:
            result.binding = CBM_RESOLVE_BINDING_AMBIGUOUS;
            result.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE;
            result.candidate_count = 2;
            break;
        case CBM_SOLVER_UNSUPPORTED:
            result.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED;
            break;
        case CBM_SOLVER_LIMITED:
            result.analysis = CBM_RESOLVE_ANALYSIS_LIMITED;
            break;
        case CBM_SOLVER_FAILED:
            result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
            break;
        default:
            break;
        }
        /* Only an answer is ever admitted or reported as a competitor, so only an answer
         * pays for a proof. Unknown stays visible in the occurrence's own row. */
        if (binding.proof &&
            (binding.status == CBM_SOLVER_KNOWN || binding.status == CBM_SOLVER_AMBIGUOUS)) {
            cbm_resolve_proof_source_t source = *site->source;
            source.scope = site->scope;
            cbm_resolve_proof_key_t fact = {.lookup_domain = source.module,
                                            .lookup_key = site->site_key,
                                            .declaration_identity = site->site_key,
                                            .start_byte = site->start,
                                            .end_byte = site->end,
                                            .lookup_closed = true,
                                            .detail = role};
            cbm_resolve_result_t observed = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
            const cbm_resolve_proof_t *audited = cbm_resolve_proof_leaf(
                state->proofs, CBM_PROOF_SOURCE_FACT, &source, &fact, &observed);
            cbm_resolve_proof_key_t key = {.lookup_domain = site->scope,
                                           .lookup_key = site->spelling,
                                           .binding_identity = result.target,
                                           .detail = role,
                                           .start_byte = site->start,
                                           .end_byte = site->end,
                                           .lookup_closed = binding.predecessors_closed};
            const cbm_resolve_proof_t *premises[] = {audited, binding.proof};
            result.proof = audited ? cbm_resolve_proof_derive(state->proofs, CBM_PROOF_TYPE_BIND,
                                                              &source, &key, &result, premises, 2,
                                                              binding.predecessors_closed)
                                   : NULL;
            if (!result.proof)
                return false;
        }
        site->result = result;
    }
    return true;
}
bool cbm_go_stabilization_call(const cbm_go_stabilization_t *state, const char *file,
                               const char *scope, uint32_t start, uint32_t end,
                               cbm_resolve_result_t *out) {
    if (!state || !state->sealed || !file || !scope)
        return false;
    /* Query keys live only for this call; shared tables and proof pools are read-only. */
    char key[4096];
    int n = snprintf(key, sizeof(key), "go/site:%zu:%s:%u:%u", strlen(file), file, start, end);
    if (n < 0 || (size_t)n >= sizeof(key)) {
        *out = (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_LIMITED,
                                      .reason = "shared_occurrence_identity_limit"};
        return true;
    }
    const go_site_t *site = cbm_ht_get(state->sites, key);
    if (!site || !site->scope || strcmp(site->scope, scope))
        return false;
    *out = site->result;
    return true;
}
bool cbm_go_stabilization_reference(const cbm_go_stabilization_t *state, const char *file,
                                    uint32_t start, uint32_t end, cbm_resolve_result_t *out) {
    if (!state || !state->sealed || !file || !out)
        return false;
    const go_file_refs_t *table = cbm_ht_get(state->file_refs, file);
    if (!table)
        return false;
    go_ref_t query = {.start = start, .end = end};
    const go_ref_t *item =
        bsearch(&query, table->items, table->count, sizeof(*table->items), compare_ref);
    if (!item)
        return false;
    if (!item->key) {
        *out = (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .reason = "go_local_binding"};
        return true;
    }
    cbm_resolve_solver_result_t binding;
    cbm_resolve_solver_get(state->solver, item->key, &binding);
    if (binding.status == CBM_SOLVER_AMBIGUOUS) {
        *out = (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .candidate_count = 2,
                                      .reason = "shared_binding_type_fixed_point"};
        return true;
    }
    if (binding.status != CBM_SOLVER_KNOWN || !binding.value.target || !binding.proof ||
        !binding.predecessors_closed)
        return false;
    *out = (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = item->rule,
                                  .facts = cbm_resolve_rule_required_facts(item->rule) |
                                           CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                  .target = binding.value.target,
                                  .candidate_count = 1,
                                  .reason = "shared_binding_type_fixed_point",
                                  .proof = binding.proof};
    return true;
}
bool cbm_go_stabilization_summary_key(const char *key) {
    return key && (!strncmp(key, "go/type:", 8) || !strncmp(key, "go/binding:", 11) ||
                   !strncmp(key, "go/decl:", 8));
}
