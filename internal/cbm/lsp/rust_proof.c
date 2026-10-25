#include "proof_bridge.h"
#include "rust_proof.h"
#include "../callable_sig.h"
#include "../resolve/binding.h"
#include "../resolve/symbols.h"
#include "../resolve/policy.h"
#include "foundation/hash_table.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *qn;
    uint32_t line, flags;
} rsp_decl_t;
typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length, visits;
    bool failed;
    rsp_decl_t *defs;
    size_t count, capacity;
} rsp_source_t;
static bool rsp_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}
static TSNode rsp_field(TSNode node, const char *field) {
    return ts_node_is_null(node)
               ? (TSNode){0}
               : ts_node_child_by_field_name(node, field, (uint32_t)strlen(field));
}
static const char *rsp_text(rsp_source_t *source, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= source->length
               ? cbm_arena_strndup(source->arena, source->source + start, end - start)
               : NULL;
}
static bool rsp_budget(rsp_source_t *source, unsigned depth) {
    if (++source->visits > 1000000 || depth > 128)
        source->failed = true;
    return !source->failed;
}
static void rsp_add(rsp_source_t *source, const char *qn, TSNode node, uint32_t flags) {
    if (!qn) {
        source->failed = true;
        return;
    }
    if (source->count == source->capacity) {
        size_t capacity = source->capacity ? source->capacity * 2 : 32;
        rsp_decl_t *items = cbm_arena_alloc(source->arena, capacity * sizeof(*items));
        if (!items) {
            source->failed = true;
            return;
        }
        if (source->count)
            memcpy(items, source->defs, source->count * sizeof(*items));
        source->defs = items;
        source->capacity = capacity;
    }
    source->defs[source->count++] = (rsp_decl_t){qn, ts_node_start_point(node).row + 1, flags};
}
static bool rsp_public(rsp_source_t *source, TSNode node) {
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        if (rsp_is(child, "visibility_modifier")) {
            const char *text = rsp_text(source, child);
            return text && !strcmp(text, "pub");
        }
    }
    return false;
}
static bool rsp_child(TSNode node, const char *kind) {
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++)
        if (rsp_is(ts_node_named_child(node, i), kind))
            return true;
    return false;
}
static void rsp_class_facts(rsp_source_t *source, TSNode node, const char *module) {
    if (!rsp_is(node, "struct_item") || !rsp_public(source, node) ||
        rsp_child(node, "type_parameters") || rsp_child(node, "where_clause"))
        return;
    const char *name = rsp_text(source, rsp_field(node, "name"));
    if (name)
        rsp_add(source, cbm_arena_sprintf(source->arena, "%s.%s", module, name), node,
                CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_TOP_LEVEL |
                    CBM_SOURCE_CLASS_DOMAIN_COMPLETE);
}
static void rsp_impl_facts(rsp_source_t *source, TSNode node, const char *module) {
    TSNode type = rsp_field(node, "type"), body = rsp_field(node, "body");
    if (!rsp_is(type, "type_identifier") || !ts_node_is_null(rsp_field(node, "trait")) ||
        rsp_child(node, "type_parameters") || rsp_child(node, "where_clause") ||
        ts_node_is_null(body))
        return;
    /* Conditional/procedural attributes change which declarations exist. */
    if (rsp_child(body, "attribute_item") || rsp_child(body, "macro_invocation"))
        return;
    const char *owner = rsp_text(source, type);
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (owner && ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode method = ts_tree_cursor_current_node(&cursor);
            if (!rsp_is(method, "function_item") || !rsp_public(source, method) ||
                rsp_child(method, "type_parameters") || rsp_child(method, "where_clause") ||
                rsp_child(method, "function_modifiers"))
                continue;
            TSNode parameters = rsp_field(method, "parameters");
            if (ts_node_named_child_count(parameters) != 1)
                continue;
            TSNode self = ts_node_named_child(parameters, 0);
            const char *receiver = rsp_text(source, self);
            /* This adapter handles the first exact &T receiver candidate only.
             * &mut self, owned receivers, Deref and trait lookup need separate proofs. */
            if (!rsp_is(self, "self_parameter") || !receiver || strcmp(receiver, "&self"))
                continue;
            const char *name = rsp_text(source, rsp_field(method, "name"));
            if (name)
                rsp_add(source, cbm_arena_sprintf(source->arena, "%s.%s.%s", module, owner, name),
                        method,
                        CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC |
                            CBM_SOURCE_DECL_FIXED_ARITY);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static int rsp_compare(const void *left, const void *right) {
    const rsp_decl_t *a = left, *b = right;
    int order = strcmp(a->qn, b->qn);
    return order ? order : (a->line > b->line) - (a->line < b->line);
}
bool cbm_rust_collect_source_facts(CBMArena *arena, const char *source, int length,
                                   const char *module, CBMLSPDef *defs, int count) {
    if (count < 0 || (count && !defs))
        return false;
    for (int i = 0; i < count; i++)
        defs[i].source_flags = 0;
    if (!arena || !source || length <= 0 || !module)
        return false;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_rust(void);
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_rust());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    rsp_source_t facts = {.arena = arena,
                          .source = source,
                          .length = (size_t)length,
                          .failed = ts_node_has_error(root)};
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    if (!facts.failed && ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (rsp_is(child, "struct_item"))
                rsp_class_facts(&facts, child, module);
            else if (rsp_is(child, "impl_item"))
                rsp_impl_facts(&facts, child, module);
            else if (rsp_is(child, "attribute_item") || rsp_is(child, "inner_attribute_item") ||
                     rsp_is(child, "macro_invocation"))
                facts.failed = true;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    ts_tree_delete(tree);
    if (facts.failed)
        return false;
    if (!facts.count)
        return true;
    qsort(facts.defs, facts.count, sizeof(*facts.defs), rsp_compare);
    for (int i = 0; i < count; i++) {
        CBMLSPDef *def = &defs[i];
        const char *qn =
            def->graph_qualified_name ? def->graph_qualified_name : def->qualified_name;
        if (!qn || !def->source_start_line)
            continue;
        rsp_decl_t key = {.qn = qn, .line = def->source_start_line};
        const rsp_decl_t *found =
            bsearch(&key, facts.defs, facts.count, sizeof(*facts.defs), rsp_compare);
        if (found)
            def->source_flags = found->flags;
    }
    return true;
}

typedef struct {
    const char *target;
    bool ambiguous;
} rsp_name_t;
typedef struct {
    rsp_source_t source;
    const CBMTypeRegistry *registry;
    const CBMFileResult *file;
    CBMResolvedCallArray *out;
    cbm_resolve_binding_index_t *bindings;
    CBMResolveTypeMap types;
    CBMHashTable *imports, *callers;
} rsp_context_t;
static bool rsp_put(rsp_context_t *context, CBMHashTable *table, const char *name,
                    const char *target) {
    if (!name)
        return false;
    rsp_name_t *old = cbm_ht_get(table, name);
    if (old) {
        old->ambiguous = true;
        return true;
    }
    rsp_name_t *value = cbm_arena_calloc(context->source.arena, sizeof(*value));
    const char *key = cbm_arena_strdup(context->source.arena, name);
    if (!value || !key)
        return false;
    value->target = target;
    cbm_ht_set(table, key, value);
    return cbm_ht_get(table, key) == value;
}
static const CBMRegisteredType *rsp_type(rsp_context_t *context, const char *name) {
    if (!name || !cbm_resolve_binding_import(context->bindings, name))
        return NULL;
    const rsp_name_t *binding = cbm_ht_get(context->imports, name);
    if (!binding || binding->ambiguous || !binding->target || !binding->target[0])
        return NULL;
    CBMTypeQNIter iterator;
    cbm_registry_types_with_qn(context->registry, binding->target, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    const uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    while ((candidate = cbm_type_qn_next(&iterator, NULL))) {
        if (!candidate->project_source || !candidate->declaration_identity ||
            !candidate->graph_qualified_name || (candidate->source_flags & required) != required)
            return NULL;
        if (chosen && (!chosen->declaration_identity ||
                       strcmp(chosen->declaration_identity, candidate->declaration_identity) ||
                       strcmp(chosen->graph_qualified_name, candidate->graph_qualified_name)))
            return NULL;
        chosen = candidate;
    }
    return iterator.complete ? chosen : NULL;
}
static cbm_resolve_result_t rsp_type_evidence(rsp_context_t *context, const CBMRegisteredType *type,
                                              TSNode site) {
    if (!type)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED};
    return cbm_lsp_proof_type_lookup(
        &context->types, rsp_text(&context->source, site), site, "rust_proof-type-name-domain",
        type->qualified_name, type->declaration_identity,
        type->graph_qualified_name ? type->graph_qualified_name : type->qualified_name,
        type->source_flags, "rust_proof-complete-source-type-lookup");
}
static const char *rsp_occurrence_spelling(rsp_context_t *context, TSNode site,
                                           CBMResolvedKind kind) {
    TSNode expression = site;
    if (kind == CBM_RESOLVED_INVOCATION)
        expression = rsp_field(site, "function");
    if (rsp_is(expression, "field_expression")) {
        const char *receiver = rsp_text(&context->source, rsp_field(expression, "value"));
        const char *member = rsp_text(&context->source, rsp_field(expression, "field"));
        return receiver && member
                   ? cbm_arena_sprintf(context->source.arena, "%s.%s", receiver, member)
                   : NULL;
    }
    return rsp_text(&context->source, expression);
}
static void rsp_emit(rsp_context_t *context, const char *caller, TSNode site, CBMResolvedKind kind,
                     cbm_resolve_result_t proof) {
    if (!caller || !cbm_resolve_admit(&proof))
        return;
    const char *relation = kind == CBM_RESOLVED_INVOCATION       ? "CALLS"
                           : kind == CBM_RESOLVED_CALL_REFERENCE ? "CALL_REFERENCE"
                           : kind == CBM_RESOLVED_TYPE_REFERENCE ? "USES_TYPE"
                                                                 : "USAGE";
    const char *spelling = rsp_occurrence_spelling(context, site, kind);
    proof = cbm_lsp_proof_occurrence(&context->types, proof, site, caller, spelling, relation);
    if (!cbm_resolve_admit(&proof))
        return;
    CBMResolvedCall row = {.caller_qn = caller,
                           .callee_qn = proof.target,
                           .strategy = "rust_source_binding",
                           .confidence = 1.0f,
                           .kind = kind,
                           .site_start_byte = ts_node_start_byte(site),
                           .site_end_byte = ts_node_end_byte(site),
                           .source_origin = CBM_SOURCE_ORIGIN_RAW,
                           .evidence = {.analysis = proof.analysis,
                                        .rule = proof.rule,
                                        .facts = proof.facts,
                                        .proof = proof.proof}};
    cbm_resolvedcall_push(context->out, context->source.arena, row);
}
static cbm_resolve_method_candidate_t rsp_candidate(const cbm_resolve_method_query_t *query,
                                                    const CBMTypeRegistry *owner,
                                                    const CBMRegisteredFunc *candidate,
                                                    void *opaque) {
    (void)query;
    (void)owner;
    (void)opaque;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    const uint32_t required =
        CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_FIXED_ARITY;
    if ((candidate->source_flags & required) != required ||
        !(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) ||
        (candidate->flags & (CBM_FUNC_FLAG_RUST_TRAIT_IMPL | CBM_FUNC_FLAG_RUST_ABSTRACT)) ||
        candidate->impl_trait_qn)
        return proof;
    proof.applicable = CBM_RESOLVE_METHOD_YES;
    proof.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    proof.graph_target = candidate->graph_qualified_name;
    proof.graph_target_exact = proof.graph_target != NULL;
    proof.declaration_identity = candidate->declaration_identity;
    proof.declaration_identity_exact = proof.declaration_identity != NULL;
    return proof;
}
static cbm_resolve_result_t rsp_member(rsp_context_t *context, TSNode attribute, CBMScope *scope) {
    cbm_resolve_result_t missing = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                    .reason = "rust_member_unproven"};
    TSNode receiver = rsp_field(attribute, "value"), member = rsp_field(attribute, "field");
    if (!rsp_is(receiver, "identifier") || !rsp_is(member, "field_identifier"))
        return missing;
    const char *variable = rsp_text(&context->source, receiver),
               *name = rsp_text(&context->source, member);
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&context->types, scope, variable, ts_node_start_byte(attribute));
    if (fact.status != CBM_RESOLVE_TYPE_KNOWN || !fact.type || fact.type->kind != CBM_TYPE_NAMED)
        return missing;
    cbm_resolve_method_query_t query = {.proof_pool = &context->types.proofs,
                                        .proof_source = &context->types.proof_source,
                                        .lookup_domain = fact.type->data.named.qualified_name,
                                        .registry = context->registry,
                                        .receiver = &fact,
                                        .receiver_qn = fact.type->data.named.qualified_name,
                                        .method_name = name,
                                        .argument_count = 0,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                         CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                        .classify = rsp_candidate};
    return cbm_resolve_method(&query).result;
}
static bool rsp_read_syntax(const char *kind) {
    static const char *const kinds[] = {"block",
                                        "expression_statement",
                                        "empty_statement",
                                        "return_expression",
                                        "call_expression",
                                        "field_expression",
                                        "identifier",
                                        "field_identifier",
                                        "arguments",
                                        "integer_literal",
                                        "float_literal",
                                        "boolean_literal",
                                        "string_literal",
                                        "string_content",
                                        "escape_sequence",
                                        "line_comment",
                                        "block_comment",
                                        "let_declaration",
                                        "assignment_expression",
                                        "binary_expression",
                                        "unary_expression",
                                        "parenthesized_expression",
                                        NULL};
    for (size_t i = 0; kinds[i]; i++)
        if (!strcmp(kinds[i], kind))
            return true;
    return false;
}
static void rsp_audit(rsp_context_t *context, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || !ts_node_is_named(node) || !rsp_budget(&context->source, depth))
        return;
    if (!rsp_read_syntax(ts_node_type(node))) {
        context->source.failed = true;
        return;
    }
    if (rsp_is(node, "let_declaration") && (!rsp_is(rsp_field(node, "pattern"), "identifier") ||
                                            !ts_node_is_null(rsp_field(node, "type")))) {
        context->source.failed = true;
        return;
    }
    if (rsp_is(node, "assignment_expression") && !rsp_is(rsp_field(node, "left"), "identifier")) {
        context->source.failed = true;
        return;
    }
    if (rsp_is(node, "call_expression")) {
        TSNode callee = rsp_field(node, "function"), args = rsp_field(node, "arguments");
        /* Unsupported calls and effects do not donate receiver facts. */
        if (!rsp_is(callee, "field_expression") || ts_node_is_null(args) ||
            ts_node_named_child_count(args)) {
            context->source.failed = true;
            return;
        }
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            rsp_audit(context, ts_tree_cursor_current_node(&cursor), depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void rsp_expression(rsp_context_t *context, TSNode node, const char *caller, CBMScope *scope,
                           unsigned depth) {
    if (!rsp_budget(&context->source, depth))
        return;
    if (rsp_is(node, "call_expression")) {
        TSNode callee = rsp_field(node, "function");
        if (rsp_is(callee, "field_expression"))
            rsp_emit(context, caller, node, CBM_RESOLVED_INVOCATION,
                     rsp_member(context, callee, scope));
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            rsp_expression(context, ts_tree_cursor_current_node(&cursor), caller, scope, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void rsp_statements(rsp_context_t *context, TSNode body, const char *caller,
                           CBMScope *scope) {
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode node = ts_tree_cursor_current_node(&cursor);
            TSNode expression =
                rsp_is(node, "expression_statement") && ts_node_named_child_count(node) == 1
                    ? ts_node_named_child(node, 0)
                    : node;
            bool declaration = rsp_is(expression, "let_declaration");
            if (declaration || rsp_is(expression, "assignment_expression")) {
                TSNode left = rsp_field(expression, declaration ? "pattern" : "left"),
                       right = rsp_field(expression, declaration ? "value" : "right");
                const char *name = rsp_text(&context->source, left);
                rsp_expression(context, right, caller, scope, 0);
                if (rsp_is(left, "identifier") && rsp_is(right, "identifier"))
                    (void)cbm_resolve_types_copy(&context->types, scope, name, scope,
                                                 rsp_text(&context->source, right),
                                                 ts_node_end_byte(expression), declaration);
                else if (name)
                    (void)cbm_resolve_types_invalidate(&context->types, scope, name,
                                                       ts_node_end_byte(expression),
                                                       CBM_RESOLVE_TYPE_UNKNOWN);
            } else
                rsp_expression(context, node, caller, scope, 0);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void rsp_function(rsp_context_t *context, TSNode node) {
    const char *name = rsp_text(&context->source, rsp_field(node, "name"));
    const char *key = name ? cbm_arena_sprintf(context->source.arena, "%u:%s",
                                               ts_node_start_point(node).row + 1, name)
                           : NULL;
    const rsp_name_t *caller = key ? cbm_ht_get(context->callers, key) : NULL;
    TSNode body = rsp_field(node, "body"), parameters = rsp_field(node, "parameters");
    if (rsp_child(node, "type_parameters") || rsp_child(node, "where_clause") ||
        rsp_child(node, "function_modifiers") || !caller || caller->ambiguous || !caller->target ||
        ts_node_is_null(body) || ts_node_is_null(parameters))
        return;
    rsp_audit(context, body, 0);
    bool failed = context->source.failed;
    context->source.failed = false;
    if (failed)
        return;
    CBMScope *scope = cbm_scope_push(context->source.arena, NULL);
    if (!scope)
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(parameters);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode parameter = ts_tree_cursor_current_node(&cursor);
            if (!rsp_is(parameter, "parameter"))
                continue;
            TSNode name_node = rsp_field(parameter, "pattern"),
                   annotation = rsp_field(parameter, "type");
            if (!rsp_is(name_node, "identifier") || ts_node_is_null(annotation) ||
                !rsp_is(annotation, "reference_type") || ts_node_named_child_count(annotation) != 1)
                continue;
            TSNode type_node = rsp_field(annotation, "type");
            if (!rsp_is(type_node, "type_identifier"))
                continue;
            const char *variable = rsp_text(&context->source, name_node);
            const CBMRegisteredType *type =
                rsp_type(context, rsp_text(&context->source, type_node));
            if (!variable || !type)
                continue;
            cbm_resolve_result_t proof = rsp_type_evidence(context, type, type_node);
            if (cbm_resolve_types_declare(
                    &context->types, scope, variable, ts_node_start_byte(parameter),
                    cbm_type_named(context->source.arena, type->qualified_name),
                    CBM_RESOLVE_TYPE_ANNOTATION, &proof))
                rsp_emit(context, caller->target, type_node, CBM_RESOLVED_TYPE_REFERENCE, proof);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    rsp_statements(context, body, caller->target, scope);
}
void cbm_rust_proof_resolve(CBMArena *arena, const char *source, int length,
                            const CBMTypeRegistry *registry, const CBMFileResult *file,
                            const char **import_names, const char **import_qns, int import_count,
                            CBMResolvedCallArray *out) {
    if (!arena || !source || length <= 0 || !registry || !file || !out || file->has_error ||
        file->parse_incomplete || file->parse_unusable)
        return;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_rust(void);
    if (!parser)
        return;
    ts_parser_set_language(parser, tree_sitter_rust());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return;
    TSNode root = ts_tree_root_node(tree);
    rsp_context_t context = {.source = {.arena = arena,
                                        .source = source,
                                        .length = (size_t)length,
                                        .failed = ts_node_has_error(root)},
                             .registry = registry,
                             .file = file,
                             .out = out};
    context.bindings =
        cbm_resolve_binding_build(arena, file, CBM_LANG_RUST, source, (size_t)length);
    context.imports = cbm_ht_create(16);
    context.callers = cbm_ht_create(16);
    cbm_resolve_types_init(&context.types, arena, 0);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, file->module_qn);
    if (!cbm_lsp_proof_context(&context.types, &proof_source, file->module_qn))
        context.source.failed = true;
    int first = out->count;
    if (context.imports && context.callers && context.bindings) {
        for (int i = 0; i < import_count; i++)
            if (import_names && import_names[i] && import_qns && import_qns[i])
                if (!rsp_put(&context, context.imports, import_names[i], import_qns[i]))
                    context.source.failed = true;
        for (int i = 0; i < file->defs.count; i++) {
            const CBMDefinition *def = &file->defs.items[i];
            if (!def->name || !def->qualified_name || !def->label || strcmp(def->label, "Function"))
                continue;
            const char *key = cbm_arena_sprintf(arena, "%u:%s", def->start_line, def->name);
            if (!key || !rsp_put(&context, context.callers, key, def->qualified_name))
                context.source.failed = true;
        }
        TSTreeCursor cursor = ts_tree_cursor_new(root);
        if (!context.source.failed && ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (rsp_is(child, "function_item"))
                    rsp_function(&context, child);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
    }
    if (context.source.failed)
        out->count = first;
    cbm_ht_free(context.imports);
    cbm_ht_free(context.callers);
    ts_tree_delete(tree);
}
