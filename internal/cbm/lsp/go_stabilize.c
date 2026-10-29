#include "go_stabilize.h"
#include "../helpers.h"
#include "../resolve/policy.h"
#include "foundation/hash_table.h"
#include <limits.h>
#include <stdio.h>
#include <string.h>

extern const TSLanguage *tree_sitter_go(void);

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
    const char *key, *file, *scope, *spelling;
    uint32_t start, end;
    cbm_resolve_rule_t rule;
    bool carrier_exact;
    const cbm_resolve_proof_t *audit;
    cbm_resolve_result_t result;
    struct go_site *next;
} go_site_t;
struct cbm_go_stabilization {
    CBMArena *arena;
    cbm_resolve_proof_pool_t *proofs;
    cbm_resolve_solver_t *solver;
    CBMHashTable *domains, *registered, *sites;
    go_site_t *first_site;
    go_domain_t *first_domain;
    bool failed, sealed;
};
typedef struct go_local {
    const char *name, *fact, *declared_fact;
    bool type_shadow, call_blocked, escaped, nonlocal;
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
    CBMHashTable *imports, *definitions, *calls;
    CBMArena call_arena;
    go_frame_t *frame;
    unsigned depth;
} go_collect_t;

static const char *text(go_collect_t *c, TSNode node) {
    return ts_node_is_null(node) ? NULL : cbm_node_text(c->shared->arena, node, c->source);
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
    return site_key(c->shared->arena, c->context.file, ts_node_start_byte(node),
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
static bool remember(go_collect_t *c, const char *key) {
    if (!key) {
        c->shared->failed = true;
        return false;
    }
    if (cbm_ht_get(c->shared->registered, key))
        return false;
    cbm_ht_set(c->shared->registered, key, c->shared);
    c->shared->failed |= cbm_ht_get(c->shared->registered, key) != c->shared;
    return !c->shared->failed;
}
static void check(go_collect_t *c, bool ok) {
    c->shared->failed |= !ok;
}
static const char *unknown(go_collect_t *c, TSNode node, const char *role) {
    const char *key = node_key(c, node, role);
    if (remember(c, key))
        check(c, cbm_resolve_solver_leaf(c->shared->solver, key, CBM_SOLVER_UNSUPPORTED,
                                         (cbm_resolve_solver_value_t){0},
                                         audit(c, node, key, "unsupported_source_transfer")));
    return key;
}
static go_local_t *local_binding(go_collect_t *c, const char *name) {
    if (!name)
        return NULL;
    for (go_frame_t *frame = c->frame; frame; frame = frame->parent) {
        go_local_t *item = cbm_ht_get(frame->names, name);
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
    go_frame_t *frame = cbm_arena_calloc(c->shared->arena, sizeof(*frame));
    if (!frame || !(frame->names = cbm_ht_create(16))) {
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
static void bind(go_collect_t *c, TSNode name, const char *dependency, bool declaration) {
    const char *spelling = text(c, name);
    if (!spelling || !strcmp(spelling, "_"))
        return;
    if (!c->frame) {
        c->shared->failed = true;
        return;
    }
    go_local_t *item =
        declaration ? cbm_ht_get(c->frame->names, spelling) : local_binding(c, spelling);
    /* A blocker for an assignment to a package variable is not a declaration
     * in this lexical frame. A later := may introduce a real local. */
    if (declaration && item && item->nonlocal)
        item = NULL;
    if (!item) {
        item = cbm_arena_calloc(c->shared->arena, sizeof(*item));
        if (!item) {
            c->shared->failed = true;
            return;
        }
        item->name = spelling;
        item->nonlocal = !declaration;
        item->call_blocked = item->nonlocal;
        item->next = c->frame->locals;
        c->frame->locals = item;
        put(c, c->frame->names, spelling, item);
    }
    if (item->nonlocal)
        dependency = NULL;
    else if (item->escaped)
        dependency = item->declared_fact;
    if (!dependency)
        dependency = unknown(c, name, "unknown-write");
    const char *key = node_key(c, name, "version");
    if (remember(c, key))
        check(c, cbm_resolve_solver_type(c->shared->solver, key, dependency, CBM_TYPE_NAMED,
                                         ts_node_start_byte(name) + 1,
                                         audit(c, name, key, "immutable_lexical_binding_version")));
    item->fact = key;
}
static void escape_binding(go_collect_t *c, go_local_t *item, TSNode node) {
    item->escaped = true;
    item->call_blocked = true;
    item->fact = item->declared_fact ? item->declared_fact : unknown(c, node, "escaped-binding");
}
static void escape_visible(go_collect_t *c, TSNode node) {
    for (go_frame_t *frame = c->frame; frame; frame = frame->parent)
        for (go_local_t *item = frame->locals; item; item = item->next)
            escape_binding(c, item, node);
}
/* Effects outlive the expression that creates an alias or closure. Until a
 * precise capture/effect model exists, a closure may capture every visible
 * local. Only a source annotation can preserve its static concrete identity. */
static void escape_effects(go_collect_t *c, TSNode node, unsigned depth) {
    if (ts_node_is_null(node))
        return;
    if (depth >= 64 || kind(node, "func_literal")) {
        escape_visible(c, node);
        return;
    }
    const char *operator_text =
        kind(node, "unary_expression") ? text(c, field(node, "operator")) : NULL;
    if (operator_text && !strcmp(operator_text, "&")) {
        TSNode operand = field(node, "operand");
        go_local_t *item = kind(operand, "identifier") ? local_binding(c, text(c, operand)) : NULL;
        if (item)
            escape_binding(c, item, node);
        else
            escape_visible(c, node);
    }
    for (uint32_t i = 0; i < named_count(node); i++)
        escape_effects(c, named_child(node, i), depth + 1);
}

static void barrier(go_collect_t *c, TSNode node) {
    const char *key = unknown(c, node, "flow-barrier");
    for (go_frame_t *frame = c->frame; frame; frame = frame->parent)
        for (go_local_t *item = frame->locals; item; item = item->next)
            item->fact = item->declared_fact ? item->declared_fact : key;
}
static void shadow(go_collect_t *c, TSNode name, bool type) {
    bind(c, name, NULL, true);
    go_local_t *item = local_binding(c, text(c, name));
    if (item) {
        item->type_shadow = type;
        item->call_blocked = true;
    }
}

static const char *type_fact_inner(go_collect_t *, TSNode);
static const char *type_fact(go_collect_t *c, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    if (c->depth >= 64)
        return unknown(c, node, "annotation-depth-limit");
    c->depth++;
    const char *result = type_fact_inner(c, node);
    c->depth--;
    return result;
}
static const char *type_fact_inner(go_collect_t *c, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    const char *key = node_key(c, node, "annotation");
    if (!remember(c, key))
        return key;
    const cbm_resolve_proof_t *proof = audit(c, node, key, "source_type_annotation");
    const char *dependency = NULL;
    CBMTypeKind wrapper = CBM_TYPE_NAMED;
    if (kind(node, "pointer_type") && named_count(node) == 1) {
        dependency = type_fact(c, named_child(node, 0));
        wrapper = CBM_TYPE_POINTER;
    } else if (kind(node, "type_identifier") && !local_binding(c, text(c, node))) {
        dependency = cbm_arena_sprintf(c->shared->arena, "go/type:%s.%s", c->module, text(c, node));
    } else if (kind(node, "qualified_type")) {
        const char *name = text(c, field(node, "package"));
        const char *member = text(c, field(node, "name"));
        const char *import = name && !local_binding(c, name) ? cbm_ht_get(c->imports, name) : NULL;
        if (import && member && member[0] >= 'A' && member[0] <= 'Z') {
            check(c, cbm_resolve_solver_select(
                         c->shared->solver, key, import,
                         "go/type:", cbm_arena_sprintf(c->shared->arena, ".%s", member), proof));
            return key;
        }
    }
    if (dependency)
        check(c, cbm_resolve_solver_type(c->shared->solver, key, dependency, wrapper, 0, proof));
    else
        check(c, cbm_resolve_solver_leaf(c->shared->solver, key, CBM_SOLVER_UNSUPPORTED,
                                         (cbm_resolve_solver_value_t){0}, proof));
    return key;
}
static const char *expression(go_collect_t *, TSNode);
static const char *callable(go_collect_t *c, TSNode node, cbm_resolve_rule_t *rule) {
    const char *key = node_key(c, node, "callable");
    const cbm_resolve_proof_t *proof = audit(c, node, key, "source_callable_binding");
    *rule = CBM_RESOLVE_RULE_SCOPE_BINDING;
    if (kind(node, "identifier")) {
        const char *name = text(c, node);
        go_local_t *binding = local_binding(c, name);
        if (binding) {
            if (!binding->call_blocked)
                return NULL; /* Existing audited callable-value assignment subset. */
            if (remember(c, key))
                check(c, cbm_resolve_solver_copy(c->shared->solver, key, binding->fact, proof));
            return key;
        }
        if (remember(c, key))
            check(c, cbm_resolve_solver_copy(
                         c->shared->solver, key,
                         cbm_arena_sprintf(c->shared->arena, "go/binding:%s.%s", c->module, name),
                         proof));
        return key;
    }
    if (!kind(node, "selector_expression"))
        return NULL;
    TSNode operand = field(node, "operand"), member = field(node, "field");
    const char *member_name = text(c, member);
    if (!member_name)
        return NULL;
    const char *import = kind(operand, "identifier") && !local(c, text(c, operand))
                             ? cbm_ht_get(c->imports, text(c, operand))
                             : NULL;
    const char *selector = import ? import : expression(c, operand);
    if (!selector)
        return NULL;
    *rule = import ? CBM_RESOLVE_RULE_IMPORT_BINDING : CBM_RESOLVE_RULE_TYPE_MEMBER;
    if (remember(c, key)) {
        if (import && !(member_name[0] >= 'A' && member_name[0] <= 'Z'))
            check(c, cbm_resolve_solver_leaf(c->shared->solver, key, CBM_SOLVER_UNSUPPORTED,
                                             (cbm_resolve_solver_value_t){0}, proof));
        else if (import)
            check(c, cbm_resolve_solver_select(
                         c->shared->solver, key, selector, "go/binding:",
                         cbm_arena_sprintf(c->shared->arena, ".%s", member_name), proof));
        else {
            bool addressable = kind(operand, "identifier") && local(c, text(c, operand));
            const char *prefix =
                member_name[0] >= 'A' && member_name[0] <= 'Z'
                    ? (addressable ? "go/addressable-member:" : "go/member:")
                    : cbm_arena_sprintf(c->shared->arena, "go/private-%smember:%s:",
                                        addressable ? "addressable-" : "", c->module);
            check(c, cbm_resolve_solver_select_type(
                         c->shared->solver, key, selector, prefix,
                         cbm_arena_sprintf(c->shared->arena, ".%s", member_name), proof));
        }
    }
    return key;
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
    const char *site = node_key(c, node, "site");
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
    *item =
        (go_site_t){.key = binding,
                    .file = c->context.file,
                    .scope = c->scope,
                    .spelling = call ? cbm_arena_strdup(c->shared->arena, call->callee_name) : NULL,
                    .start = ts_node_start_byte(node),
                    .end = ts_node_end_byte(node),
                    .rule = rule,
                    .carrier_exact = call != NULL,
                    .audit = audit(c, node, site, "CALLS"),
                    .next = c->shared->first_site};
    if (!item->audit || (call && !item->spelling))
        c->shared->failed = true;
    c->shared->first_site = item;
    put(c, c->shared->sites, site, item);
}
static const char *expression(go_collect_t *c, TSNode node) {
    if (ts_node_is_null(node) || c->depth++ >= 64) {
        if (c->depth)
            c->depth--;
        return NULL;
    }
    const char *result = NULL;
    if (kind(node, "identifier"))
        result = local(c, text(c, node));
    else if (kind(node, "parenthesized_expression") && named_count(node) == 1)
        result = expression(c, named_child(node, 0));
    else if (kind(node, "composite_literal"))
        result = type_fact(c, field(node, "type"));
    else if (kind(node, "call_expression")) {
        TSNode function = field(node, "function");
        cbm_resolve_rule_t rule;
        const char *binding = callable(c, function, &rule);
        if (binding) {
            result = node_key(c, node, "return");
            if (remember(c, result))
                check(c, cbm_resolve_solver_select(
                             c->shared->solver, result, binding, "go/return:", "",
                             audit(c, node, result, "resolved_callable_return")));
            record_call_site(c, node, binding, rule);
        }
    }
    c->depth--;
    return result;
}
static void walk_calls(go_collect_t *c, TSNode node, unsigned depth) {
    if (depth == 0)
        escape_effects(c, node, 0);
    if (ts_node_is_null(node))
        return;
    if (depth >= 64) {
        barrier(c, node);
        return;
    }
    if (kind(node, "func_literal")) {
        escape_visible(c, node);
        return;
    }
    if (kind(node, "call_expression"))
        (void)expression(c, node);
    uint32_t count = named_count(node);
    for (uint32_t i = 0; i < count; i++)
        walk_calls(c, named_child(node, i), depth + 1);
}
static void statement(go_collect_t *c, TSNode node, unsigned depth) {
    if (ts_node_is_null(node))
        return;
    if (depth >= 64) {
        barrier(c, node);
        return;
    }
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
            if (!ts_node_is_null(name))
                shadow(c, name, true);
        }
        return;
    }
    if (kind(node, "short_var_declaration") || kind(node, "assignment_statement")) {
        TSNode left = field(node, "left"), right = field(node, "right");
        uint32_t count = named_count(left);
        escape_effects(c, left, 0);
        walk_calls(c, right, 0);
        if (count == 1 && named_count(right) == 1 && kind(named_child(left, 0), "identifier")) {
            bind(c, named_child(left, 0), expression(c, named_child(right, 0)),
                 kind(node, "short_var_declaration"));
        } else {
            bool indirect = false;
            for (uint32_t i = 0; i < count; i++) {
                if (kind(named_child(left, i), "identifier"))
                    bind(c, named_child(left, i), NULL, kind(node, "short_var_declaration"));
                else
                    indirect = true;
            }
            if (indirect)
                barrier(c, node);
        }
        return;
    }
    if (kind(node, "var_declaration") || kind(node, "const_declaration") ||
        kind(node, "var_spec_list")) {
        for (uint32_t i = 0; i < named_count(node); i++)
            statement(c, named_child(node, i), depth + 1);
        return;
    }
    if (kind(node, "var_spec") || kind(node, "const_spec")) {
        TSNode annotation = field(node, "type"), value = field(node, "value");
        walk_calls(c, value, 0);
        const char *fact = !ts_node_is_null(annotation) ? type_fact(c, annotation)
                           : named_count(value) == 1    ? expression(c, named_child(value, 0))
                                                        : NULL;
        unsigned names = 0;
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            const char *role = ts_node_field_name_for_child(node, i);
            names += role && !strcmp(role, "name");
        }
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            const char *role = ts_node_field_name_for_child(node, i);
            if (role && !strcmp(role, "name")) {
                TSNode name = ts_node_child(node, i);
                bind(c, name, !ts_node_is_null(annotation) || names == 1 ? fact : NULL, true);
                go_local_t *item = local_binding(c, text(c, name));
                if (item && !ts_node_is_null(annotation))
                    item->declared_fact = fact;
            }
        }
        return;
    }
    if (kind(node, "expression_statement") || kind(node, "return_statement")) {
        walk_calls(c, node, 0);
        return;
    }
    /* The finite straight-line adapter never guesses across a branch, loop,
     * defer/go, captured mutation or unsupported write. Separate audited hybrid
     * subsets may handle their internal sites; subsequent bindings are unknown. */
    escape_effects(c, node, 0);
    barrier(c, node);
}

static bool contribute(go_collect_t *c, const char *domain, const char *key,
                       const cbm_resolve_proof_t *proof) {
    go_domain_t *entry = cbm_ht_get(c->shared->domains, domain);
    if (!entry) {
        entry = cbm_arena_calloc(c->shared->arena, sizeof(*entry));
        if (!entry)
            return false;
        entry->key = domain;
        put(c, c->shared->domains, domain, entry);
        entry->next = c->shared->first_domain;
        c->shared->first_domain = entry;
    }
    go_support_t *support = cbm_arena_alloc(c->shared->arena, sizeof(*support));
    if (!support)
        return false;
    *support = (go_support_t){key, proof, entry->first};
    entry->first = support;
    entry->count++;
    return true;
}
static void member_domains(go_collect_t *c, TSNode node, const char *name, const char *key,
                           const cbm_resolve_proof_t *proof) {
    TSNode receiver = field(node, "receiver");
    TSNode parameter = named_child(receiver, 0);
    TSNode receiver_type = field(parameter, "type");
    bool requires_pointer = kind(receiver_type, "pointer_type");
    if (requires_pointer)
        receiver_type = named_child(receiver_type, 0);
    if (!kind(receiver_type, "type_identifier"))
        return;
    const char *qn =
        cbm_arena_sprintf(c->shared->arena, "%s.%s", c->module, text(c, receiver_type));
    const CBMType *named = cbm_type_named(c->shared->arena, qn);
    const CBMType *pointer = cbm_type_pointer(c->shared->arena, named);
    for (int addressable = 0; addressable < 2; addressable++) {
        for (int indirect = 0; indirect < 2; indirect++) {
            if (requires_pointer && !indirect && !addressable)
                continue;
            const char *identity =
                cbm_resolve_solver_type_identity(c->shared->arena, indirect ? pointer : named);
            const char *prefix =
                name[0] >= 'A' && name[0] <= 'Z'
                    ? (addressable ? "go/addressable-member:" : "go/member:")
                    : cbm_arena_sprintf(c->shared->arena, "go/private-%smember:%s:",
                                        addressable ? "addressable-" : "", c->module);
            const char *domain = identity && prefix ? cbm_arena_sprintf(c->shared->arena, "%s%s.%s",
                                                                        prefix, identity, name)
                                                    : NULL;
            check(c, domain && contribute(c, domain, key, proof));
        }
    }
}

static const CBMDefinition *definition(go_collect_t *c, TSNode node, TSNode name) {
    const char *key = cbm_arena_sprintf(c->shared->arena, "%u:%s",
                                        ts_node_start_point(node).row + 1, text(c, name));
    const void *value = key ? cbm_ht_get(c->definitions, key) : NULL;
    return value == c ? NULL : value;
}
static void declaration(go_collect_t *c, TSNode node) {
    TSNode name = field(node, "name");
    const CBMDefinition *def = definition(c, node, name);
    if (!def || !def->qualified_name)
        return;
    bool type = kind(node, "type_spec") || kind(node, "type_alias");
    const char *name_text = text(c, name);
    const char *key = node_key(c, node, "declaration");
    const cbm_resolve_proof_t *proof = audit(c, node, key, "source_declaration_identity");
    const char *domain = cbm_arena_sprintf(c->shared->arena, "go/%s:%s.%s",
                                           type ? "type" : "binding", c->module, name_text);
    if (type) {
        TSNode declared = field(node, "type");
        bool alias = kind(node, "type_alias");
        if (alias)
            check(c,
                  cbm_resolve_solver_copy(c->shared->solver, key, type_fact(c, declared), proof));
        else {
            bool concrete =
                kind(declared, "struct_type") && ts_node_is_null(field(node, "type_parameters"));
            const char *qn = cbm_arena_sprintf(c->shared->arena, "%s.%s", c->module, name_text);
            check(c,
                  cbm_resolve_solver_leaf(
                      c->shared->solver, key, concrete ? CBM_SOLVER_KNOWN : CBM_SOLVER_UNSUPPORTED,
                      (cbm_resolve_solver_value_t){.target = qn,
                                                   .fact_kind = CBM_SOLVER_FACT_TYPE,
                                                   .type = cbm_type_named(c->shared->arena, qn),
                                                   .declaration_identity = key},
                      proof));
        }
        check(c, contribute(c, domain, key, proof));
        return;
    }
    check(c,
          cbm_resolve_solver_leaf(c->shared->solver, key, CBM_SOLVER_KNOWN,
                                  (cbm_resolve_solver_value_t){.target = def->qualified_name,
                                                               .fact_kind = CBM_SOLVER_FACT_BINDING,
                                                               .declaration_identity = key},
                                  proof));
    if (kind(node, "method_declaration"))
        member_domains(c, node, name_text, key, proof);
    else
        check(c, contribute(c, domain, key, proof));
    c->scope = cbm_arena_strdup(c->shared->arena, def->qualified_name);
    if (!push_frame(c))
        return;
    TSNode generics = field(node, "type_parameters");
    for (uint32_t i = 0; i < named_count(generics); i++) {
        TSNode parameter = named_child(generics, i);
        for (uint32_t child = 0; child < ts_node_child_count(parameter); child++) {
            const char *role = ts_node_field_name_for_child(parameter, child);
            if (role && !strcmp(role, "name"))
                shadow(c, ts_node_child(parameter, child), true);
        }
    }
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
    TSNode returns = field(node, "result");
    const char *return_key = node_key(c, node, "declared-return");
    const char *return_type =
        ts_node_is_null(field(node, "type_parameters")) ? type_fact(c, returns) : NULL;
    if (return_type)
        check(c, cbm_resolve_solver_type(c->shared->solver, return_key, return_type, CBM_TYPE_NAMED,
                                         0, proof));
    else
        check(c, cbm_resolve_solver_leaf(c->shared->solver, return_key, CBM_SOLVER_UNSUPPORTED,
                                         (cbm_resolve_solver_value_t){0}, proof));
    check(c, contribute(c, cbm_arena_sprintf(c->shared->arena, "go/return:%s", def->qualified_name),
                        return_key, proof));
    const char *parameter_groups[] = {"parameters", "receiver", "result"};
    for (size_t group = 0; group < 3; group++) {
        TSNode parameters = field(node, parameter_groups[group]);
        for (uint32_t i = 0; i < named_count(parameters); i++) {
            TSNode parameter = named_child(parameters, i);
            const char *fact = type_fact(c, field(parameter, "type"));
            for (uint32_t child = 0; child < ts_node_child_count(parameter); child++) {
                const char *role = ts_node_field_name_for_child(parameter, child);
                if (role && !strcmp(role, "name")) {
                    TSNode parameter_name = ts_node_child(parameter, child);
                    bind(c, parameter_name, fact, true);
                    go_local_t *binding = local_binding(c, text(c, parameter_name));
                    if (binding) {
                        binding->call_blocked = true;
                        binding->declared_fact = fact;
                    }
                }
            }
        }
    }
    statement(c, field(node, "body"), 0);
    c->scope = NULL;
    pop_frame(c);
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
    state->sites = cbm_ht_create(128);
    if (!state->domains || !state->registered || !state->sites) {
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
    cbm_ht_free(state->sites);
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
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    if (ts_node_has_error(root)) {
        ts_tree_delete(tree);
        return true;
    }
    go_collect_t c = {.shared = state,
                      .result = result,
                      .source = source,
                      .module = cbm_arena_strdup(state->arena, module),
                      .context = *context};
    c.context.file = cbm_arena_strdup(state->arena, context->file);
    cbm_arena_init(&c.call_arena);
    c.imports = cbm_ht_create(16);
    c.definitions = cbm_ht_create(64);
    c.calls = cbm_ht_create((uint32_t)result->calls.count);
    if (!c.imports || !c.definitions || !c.calls) {
        cbm_ht_free(c.imports);
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
        const char *key = cbm_arena_sprintf(state->arena, "%d:%s", def->start_line, def->name);
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
    for (int i = 0; import_counts && i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        if (import->local_name)
            put(&c, import_counts, import->local_name,
                cbm_ht_get(import_counts, import->local_name) ? &c : (void *)import);
    }
    for (int i = 0; import_counts && i < result->imports.count; i++) {
        const CBMImport *import = &result->imports.items[i];
        const char *name = import->local_name;
        if (!name || !*name || !strcmp(name, "_") || !strcmp(name, "."))
            continue;
        const char *key = cbm_arena_sprintf(state->arena, "go/import:%zu:%s:%s",
                                            strlen(context->file), context->file, name);
        if (cbm_ht_get(c.imports, name))
            continue;
        put(&c, c.imports, name, (void *)key);
        const cbm_resolve_import_fact_t *fact = snapshot ? &imports->items[i] : NULL;
        bool unique = cbm_ht_get(import_counts, name) != &c;
        bool known = unique && fact && fact->closed && fact->local_name &&
                     !strcmp(fact->local_name, name) && fact->module_path && import->module_path &&
                     !strcmp(fact->module_path, import->module_path) &&
                     fact->start_byte == import->site_start_byte &&
                     fact->end_byte == import->site_end_byte && fact->end_byte > fact->start_byte &&
                     fact->source_origin == CBM_SOURCE_ORIGIN_RAW &&
                     cbm_resolve_admit(&fact->dependency) && fact->dependency.proof;
        const cbm_resolve_proof_t *proof = audit(&c, root, key, "normalized_source_import_binding");
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
                                                   .lookup_key = name,
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
        if (kind(child, "type_declaration")) {
            for (uint32_t n = 0; n < named_count(child); n++)
                declaration(&c, named_child(child, n));
        } else if (kind(child, "function_declaration") || kind(child, "method_declaration"))
            declaration(&c, child);
    }
    cbm_ht_free(c.imports);
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
bool cbm_go_stabilization_register(cbm_go_stabilization_t *state) {
    if (!state || state->failed)
        return false;
    for (const go_domain_t *domain = state->first_domain; domain; domain = domain->next)
        register_domain(state, domain);
    return !state->failed;
}
bool cbm_go_stabilization_seal(cbm_go_stabilization_t *state) {
    if (!state || state->failed || state->sealed)
        return false;
    state->sealed = true;
    for (go_site_t *site = state->first_site; site; site = site->next) {
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
        switch (binding.status) {
        case CBM_SOLVER_KNOWN:
            result = (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                            .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                            .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                            .rule = site->rule,
                                            .facts = cbm_resolve_rule_required_facts(site->rule) |
                                                     CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                            .target = binding.value.target,
                                            .candidate_count = 1,
                                            .reason = "shared_binding_type_fixed_point"};
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
        const cbm_resolve_proof_view_t *source = cbm_resolve_proof_view(site->audit);
        if (binding.proof) {
            cbm_resolve_proof_key_t key = {.lookup_domain = site->scope,
                                           .lookup_key = site->spelling,
                                           .binding_identity = result.target,
                                           .detail = "CALLS",
                                           .start_byte = site->start,
                                           .end_byte = site->end,
                                           .lookup_closed = binding.predecessors_closed};
            const cbm_resolve_proof_t *premises[] = {site->audit, binding.proof};
            result.proof =
                cbm_resolve_proof_derive(state->proofs, CBM_PROOF_TYPE_BIND, &source->source, &key,
                                         &result, premises, 2, binding.predecessors_closed);
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
