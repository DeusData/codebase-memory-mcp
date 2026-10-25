#include "proof_bridge.h"
#include "ts_proof.h"
#include "../helpers.h"
#include "../resolve/policy.h"
#include <stdlib.h>
#include <string.h>

enum { TS_PROOF_DEPTH = 512, TS_PROOF_WORK = 1000000, TS_PROOF_LOOKUP = 16384 };

typedef struct {
    const char *name;
    TSNode declaration;
    uint32_t start, end;
    bool write;
} ts_proof_binding_t;

struct CBMTSProofAudit {
    ts_proof_binding_t *bindings;
    size_t count, capacity;
    bool complete;
};

static bool kind(TSNode node, const char *name) {
    return !ts_node_is_null(node) && strcmp(ts_node_type(node), name) == 0;
}

static TSNode field(TSNode node, const char *name) {
    if (ts_node_is_null(node))
        return (TSNode){0};
    return ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}

static const char *text(TSLSPContext *ctx, TSNode node) {
    return ts_node_is_null(node) ? NULL : cbm_node_text(ctx->arena, node, ctx->source);
}

static bool function(TSNode node) {
    return kind(node, "function_declaration") || kind(node, "function_expression") ||
           kind(node, "arrow_function") || kind(node, "method_definition") ||
           kind(node, "generator_function") || kind(node, "generator_function_declaration");
}

static TSNode binding_scope(TSNode node, bool function_scope) {
    TSNode parent = ts_node_parent(node);
    while (!ts_node_is_null(parent)) {
        if (function(parent) || kind(parent, "program") ||
            (!function_scope && (kind(parent, "statement_block") || kind(parent, "for_statement") ||
                                 kind(parent, "for_in_statement"))))
            return parent;
        parent = ts_node_parent(parent);
    }
    return node;
}

static void add_binding(TSLSPContext *ctx, TSNode declaration, TSNode name, TSNode scope,
                        bool write) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    if (!kind(name, "identifier") && !kind(name, "type_identifier")) {
        audit->complete = false;
        return;
    }
    if (audit->count == audit->capacity) {
        size_t capacity = audit->capacity ? audit->capacity * 2 : 32;
        ts_proof_binding_t *items = cbm_arena_alloc(ctx->arena, capacity * sizeof(*items));
        if (!items) {
            audit->complete = false;
            return;
        }
        if (audit->count)
            memcpy(items, audit->bindings, audit->count * sizeof(*items));
        audit->bindings = items;
        audit->capacity = capacity;
    }
    const char *name_text = text(ctx, name);
    if (!name_text) {
        audit->complete = false;
        return;
    }
    audit->bindings[audit->count++] =
        (ts_proof_binding_t){name_text, declaration, write ? 0 : ts_node_start_byte(scope),
                             write ? (uint32_t)ctx->source_len : ts_node_end_byte(scope), write};
}

static void audit_node(TSLSPContext *ctx, TSNode node) {
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    if (ts_node_is_missing(node) || kind(node, "ERROR") || kind(node, "with_statement") ||
        kind(node, "decorator") || kind(node, "object_pattern") || kind(node, "array_pattern") ||
        kind(node, "rest_pattern") || kind(node, "catch_clause") || kind(node, "type_parameter") ||
        kind(node, "internal_module")) {
        audit->complete = false;
    } else if (kind(node, "variable_declarator")) {
        bool hoisted = kind(ts_node_parent(node), "variable_declaration");
        add_binding(ctx, node, field(node, "name"), binding_scope(node, hoisted), false);
    } else if (kind(node, "required_parameter") || kind(node, "optional_parameter")) {
        TSNode name = field(node, "pattern");
        if (ts_node_is_null(name))
            name = field(node, "name");
        add_binding(ctx, node, name, binding_scope(node, true), false);
    } else if (kind(node, "identifier") && kind(ts_node_parent(node), "formal_parameters")) {
        add_binding(ctx, node, node, binding_scope(node, true), false);
    } else if (kind(node, "class_declaration") || kind(node, "interface_declaration") ||
               kind(node, "function_declaration") || kind(node, "type_alias_declaration") ||
               kind(node, "enum_declaration")) {
        add_binding(ctx, node, field(node, "name"), binding_scope(node, false), false);
    } else if (kind(node, "function_expression") || kind(node, "generator_function")) {
        TSNode name = field(node, "name");
        if (!ts_node_is_null(name))
            add_binding(ctx, node, name, node, false);
    } else if (kind(node, "arrow_function")) {
        TSNode parameter = field(node, "parameter");
        if (!ts_node_is_null(parameter))
            add_binding(ctx, parameter, parameter, node, false);
    } else if (kind(node, "assignment_expression") ||
               kind(node, "augmented_assignment_expression") || kind(node, "update_expression")) {
        TSNode target = field(node, "left");
        if (ts_node_is_null(target))
            target = field(node, "argument");
        /* A write in a closure or branch can execute out of AST order. Record
         * it across the whole file, including before the textual assignment. */
        if (kind(target, "member_expression") && !ctx->js_mode)
            return;
        add_binding(ctx, node, target, node, true);
    } else if (kind(node, "call_expression")) {
        TSNode callee = field(node, "function");
        if (kind(callee, "identifier")) {
            const char *name = text(ctx, callee);
            if (!name || strcmp(name, "eval") == 0 || strcmp(name, "Function") == 0)
                audit->complete = false;
        }
    }
}

static int binding_compare(const void *left, const void *right) {
    const ts_proof_binding_t *a = left, *b = right;
    int order = strcmp(a->name, b->name);
    if (order)
        return order;
    if (a->start != b->start)
        return a->start < b->start ? -1 : 1;
    return a->end == b->end ? 0 : (a->end < b->end ? -1 : 1);
}

void cbm_ts_proof_prepare(TSLSPContext *ctx, TSNode root) {
    if (!ctx || !ctx->arena || ts_node_is_null(root))
        return;
    cbm_resolve_proof_source_t source_context = ctx->proven_types.proof_source;
    cbm_resolve_types_init(&ctx->proven_types, ctx->arena, 0);
    (void)cbm_lsp_proof_context(&ctx->proven_types, &source_context, ctx->module_qn);
    ctx->proof_audit = cbm_arena_calloc(ctx->arena, sizeof(*ctx->proof_audit));
    if (!ctx->proof_audit)
        return;
    ctx->proof_audit->complete = !ts_node_has_error(root);
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    uint32_t work = 0, depth = 0;
    bool walking = true;
    while (walking && ctx->proof_audit->complete) {
        if (++work > TS_PROOF_WORK || depth > TS_PROOF_DEPTH) {
            ctx->proof_audit->complete = false;
            break;
        }
        audit_node(ctx, ts_tree_cursor_current_node(&cursor));
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            depth++;
            continue;
        }
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor)) {
                walking = false;
                break;
            }
            depth--;
        }
    }
    ts_tree_cursor_delete(&cursor);
    if (ctx->proof_audit->count > 1)
        qsort(ctx->proof_audit->bindings, ctx->proof_audit->count,
              sizeof(*ctx->proof_audit->bindings), binding_compare);
}

static const ts_proof_binding_t *find_binding(TSLSPContext *ctx, const char *name, uint32_t at,
                                              bool *occupied) {
    if (occupied)
        *occupied = false;
    struct CBMTSProofAudit *audit = ctx->proof_audit;
    size_t low = 0, high = audit->count;
    while (low < high) {
        size_t mid = low + (high - low) / 2;
        if (strcmp(audit->bindings[mid].name, name) < 0)
            low = mid + 1;
        else
            high = mid;
    }
    const ts_proof_binding_t *selected = NULL;
    bool duplicate = false;
    uint32_t work = 0;
    for (; low < audit->count && strcmp(audit->bindings[low].name, name) == 0; low++) {
        if (++work > TS_PROOF_LOOKUP) {
            if (occupied)
                *occupied = true;
            return NULL;
        }
        const ts_proof_binding_t *entry = &audit->bindings[low];
        if (entry->write) {
            if (occupied)
                *occupied = true;
            return NULL;
        }
        if (at < entry->start || at >= entry->end)
            continue;
        if (occupied)
            *occupied = true;
        if (!selected || entry->end - entry->start < selected->end - selected->start) {
            selected = entry;
            duplicate = false;
        } else if (entry->end - entry->start == selected->end - selected->start) {
            duplicate = true;
        }
    }
    return duplicate ? NULL : selected;
}

static uint64_t qn_hash(const char *name) {
    uint64_t hash = UINT64_C(14695981039346656037);
    for (const unsigned char *p = (const unsigned char *)name; *p; p++) {
        hash ^= *p;
        hash *= UINT64_C(1099511628211);
    }
    return hash;
}

static const CBMRegisteredType *exact_type(TSLSPContext *ctx, const char *qn) {
    const CBMRegisteredType *selected = NULL;
    uint32_t work = 0, links = 0;
    for (const CBMTypeRegistry *reg = ctx->registry; reg; reg = reg->fallback) {
        if (++links > 64 || reg->type_count != reg->type_qn_entry_count)
            return NULL;
        if (reg->type_count == 0)
            continue;
        if (!reg->type_qn_buckets || reg->type_qn_bucket_count <= 0 || !reg->type_qn_entries)
            return NULL;
        int slot = (int)(qn_hash(qn) & (uint64_t)(reg->type_qn_bucket_count - 1));
        for (int at = reg->type_qn_buckets[slot]; at >= 0;) {
            if (++work > TS_PROOF_LOOKUP || at >= reg->type_qn_entry_count)
                return NULL;
            const CBMRegistryHashEntry *entry = &reg->type_qn_entries[at];
            if (entry->payload_index < 0 || entry->payload_index >= reg->type_count)
                return NULL;
            const CBMRegisteredType *candidate = &reg->types[entry->payload_index];
            if (candidate->qualified_name && strcmp(candidate->qualified_name, qn) == 0) {
                if (!candidate->project_source)
                    return NULL;
                if (selected && selected != candidate &&
                    (!selected->declaration_identity || !candidate->declaration_identity ||
                     strcmp(selected->declaration_identity, candidate->declaration_identity) != 0))
                    return NULL;
                selected = candidate;
            }
            at = entry->next_index;
        }
    }
    return selected;
}

static const CBMRegisteredType *named_type(TSLSPContext *ctx, const char *name, uint32_t at) {
    bool occupied = false;
    const ts_proof_binding_t *local = find_binding(ctx, name, at, &occupied);
    if (occupied && !local)
        return NULL;
    if (local) {
        if (!kind(local->declaration, "class_declaration") &&
            !kind(local->declaration, "interface_declaration"))
            return NULL;
        /* Nested classes need their canonical lexical-owner identity first. */
        TSNode parent = ts_node_parent(local->declaration);
        if (kind(parent, "export_statement"))
            parent = ts_node_parent(parent);
        if (!kind(parent, "program"))
            return NULL;
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name);
        return qn ? exact_type(ctx, qn) : NULL;
    }
    const char *target = NULL;
    for (int i = 0; i < ctx->import_count; i++) {
        const char *key = ctx->import_local_names[i];
        const char *value = ctx->import_module_qns[i];
        if (!key || strcmp(key, name) != 0)
            continue;
        if (!value || !*value || (target && strcmp(target, value) != 0))
            return NULL;
        target = value;
    }
    if (!target)
        return NULL;
    const CBMRegisteredType *direct = exact_type(ctx, target);
    const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", target, name);
    const CBMRegisteredType *member = qn ? exact_type(ctx, qn) : NULL;
    if (direct && member)
        return NULL;
    const CBMRegisteredType *result = direct ? direct : member;
    return result && result->export_binding_proven ? result : NULL;
}

static const CBMRegisteredType *receiver_type(TSLSPContext *ctx, TSNode receiver, uint32_t at) {
    if (kind(receiver, "this")) {
        TSNode owner = ts_node_parent(receiver);
        while (!ts_node_is_null(owner) && !kind(owner, "class_declaration")) {
            if (function(owner) && !kind(owner, "method_definition") &&
                !kind(owner, "arrow_function"))
                return NULL;
            owner = ts_node_parent(owner);
        }
        const char *name = text(ctx, field(owner, "name"));
        return name ? named_type(ctx, name, at) : NULL;
    }
    if (!kind(receiver, "identifier"))
        return NULL;
    const char *name = text(ctx, receiver);
    const ts_proof_binding_t *binding = name ? find_binding(ctx, name, at, NULL) : NULL;
    if (!binding || ts_node_start_byte(binding->declaration) > at)
        return NULL;
    TSNode annotation = field(binding->declaration, "type");
    if (kind(annotation, "type_annotation"))
        annotation = ts_node_named_child(annotation, 0);
    if (kind(annotation, "type_identifier")) {
        const char *type_name = text(ctx, annotation);
        return type_name ? named_type(ctx, type_name, at) : NULL;
    }
    if (!ctx->js_mode && ts_node_is_null(annotation)) {
        TSNode value = field(binding->declaration, "value");
        TSNode constructor = field(value, "constructor");
        if (kind(value, "new_expression") && kind(constructor, "identifier")) {
            const char *type_name = text(ctx, constructor);
            return type_name ? named_type(ctx, type_name, at) : NULL;
        }
    }
    return NULL;
}

static cbm_resolve_method_candidate_t classify_method(const cbm_resolve_method_query_t *query,
                                                      const CBMTypeRegistry *owner,
                                                      const CBMRegisteredFunc *candidate,
                                                      void *context) {
    (void)query;
    (void)owner;
    TSLSPContext *ctx = context;
    const char *target = candidate->qualified_name;
    if (target && candidate->sig && candidate->sig[0])
        target = cbm_arena_sprintf(ctx->arena, "%s%s", target, candidate->sig);
    return (cbm_resolve_method_candidate_t){
        .exposed = CBM_RESOLVE_METHOD_YES,
        .applicable = CBM_RESOLVE_METHOD_YES,
        .domain = (candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) ? CBM_RESOLVE_DOMAIN_PROJECT
                                                                    : CBM_RESOLVE_DOMAIN_EXTERNAL,
        .graph_target = target,
        .graph_target_exact = target != NULL,
        .declaration_identity = candidate->declaration_identity,
        .declaration_identity_exact = candidate->declaration_identity != NULL};
}

cbm_resolve_method_result_t cbm_ts_proof_method(TSLSPContext *ctx, TSNode receiver,
                                                const char *name, TSNode occurrence) {
    cbm_resolve_method_result_t missing = {.result = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                                      .reason = "ts_receiver_binding_unproven"}};
    if (!ctx || !ctx->proof_audit || !ctx->proof_audit->complete || !name ||
        ts_node_is_null(occurrence))
        return missing;
    uint32_t at = ts_node_start_byte(occurrence);
    const CBMRegisteredType *owner = receiver_type(ctx, receiver, at);
    if (!owner || (owner->embedded_types && owner->embedded_types[0]))
        return missing;
    const char *binding = text(ctx, receiver);
    const CBMType *observed = binding ? cbm_scope_lookup(ctx->current_scope, binding) : NULL;
    if (!observed || observed->kind != CBM_TYPE_NAMED || !observed->data.named.qualified_name ||
        strcmp(observed->data.named.qualified_name, owner->qualified_name) != 0)
        return missing;
    cbm_resolve_result_t evidence = cbm_lsp_proof_type_lookup(
        &ctx->proven_types, binding, receiver, "typescript-source-receiver-type",
        owner->qualified_name, owner->declaration_identity,
        owner->graph_qualified_name ? owner->graph_qualified_name : owner->qualified_name,
        owner->source_flags,
        "complete-binding-audit;exact-annotation-or-constructor;observed-type-agrees");
    if (!cbm_resolve_types_observe(&ctx->proven_types, ctx->current_scope, binding, at, observed,
                                   CBM_RESOLVE_TYPE_OBSERVED_BINDING, &evidence))
        return missing;
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&ctx->proven_types, ctx->current_scope, binding, at);
    cbm_resolve_method_query_t query = {.proof_pool = &ctx->proven_types.proofs,
                                        .proof_source = &ctx->proven_types.proof_source,
                                        .lookup_domain = owner->qualified_name,
                                        .registry = ctx->registry,
                                        .receiver = &fact,
                                        .receiver_qn = owner->qualified_name,
                                        .method_name = name,
                                        .argument_count = -1,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                         CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                        .classify = classify_method,
                                        .context = ctx};
    return cbm_resolve_method(&query);
}
