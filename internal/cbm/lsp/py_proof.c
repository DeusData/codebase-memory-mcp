#include "proof_bridge.h"
#include "py_proof.h"
#include "../callable_sig.h"
#include "../resolve/binding.h"
#include "../resolve/symbols.h"
#include "../resolve/policy.h"
#include "foundation/hash_table.h"
#include <stdlib.h>
#include <string.h>
#include "../resolve/syntax_index.h"

typedef struct {
    const char *qn;
    uint32_t line, flags;
} pyp_decl_t;
typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length, visits;
    bool failed, exhausted;
    pyp_decl_t *defs;
    size_t count, capacity;
} pyp_source_t;
/* Package file identity retains .__init__, while Python declarations live
 * in the package namespace. Keep proof keys in the same symbol scope as the
 * extractor and registry; an allocation failure cannot fall back to file QNs. */
static const char *pyp_symbol_scope(CBMArena *arena, const char *module) {
    if (!module)
        return NULL;
    size_t length = cbm_fqn_symbol_scope_len(module);
    return module[length] ? cbm_arena_strndup(arena, module, length) : module;
}
static bool pyp_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}
static TSNode pyp_field(TSNode node, const char *field) {
    return ts_node_is_null(node)
               ? (TSNode){0}
               : ts_node_child_by_field_name(node, field, (uint32_t)strlen(field));
}
static const char *pyp_text(pyp_source_t *source, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= source->length
               ? cbm_arena_strndup(source->arena, source->source + start, end - start)
               : NULL;
}
static bool pyp_budget(pyp_source_t *source, unsigned depth) {
    if (depth > 128) {
        source->failed = true;
        source->exhausted = true;
    }
    return !source->failed;
}
static void pyp_add(pyp_source_t *source, const char *qn, TSNode node, uint32_t flags) {
    if (!qn) {
        source->failed = true;
        return;
    }
    if (source->count == source->capacity) {
        size_t capacity = source->capacity ? source->capacity * 2 : 32;
        pyp_decl_t *items = cbm_arena_alloc(source->arena, capacity * sizeof(*items));
        if (!items) {
            source->failed = true;
            return;
        }
        if (source->count)
            memcpy(items, source->defs, source->count * sizeof(*items));
        source->defs = items;
        source->capacity = capacity;
    }
    source->defs[source->count++] = (pyp_decl_t){qn, ts_node_start_point(node).row + 1, flags};
}
/* A declared instance method can be shadowed by an instance write. Until
 * per-method effect summaries are available, a class with a possible member
 * mutation or an unmodelled call cannot establish a complete member domain. */
static bool pyp_members_stable(pyp_source_t *source, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || !ts_node_is_named(node))
        return true;
    if (!pyp_budget(source, depth) || pyp_is(node, "call") || pyp_is(node, "delete_statement") ||
        pyp_is(node, "global_statement") || pyp_is(node, "nonlocal_statement"))
        return false;
    if ((pyp_is(node, "assignment") || pyp_is(node, "augmented_assignment")) &&
        !pyp_is(pyp_field(node, "left"), "identifier"))
        return false;
    bool stable = true;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            if (!pyp_members_stable(source, ts_tree_cursor_current_node(&cursor), depth + 1)) {
                stable = false;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return stable;
}

static void pyp_class_facts(pyp_source_t *source, TSNode node, const char *module) {
    const char *name = pyp_text(source, pyp_field(node, "name"));
    const char *qn = name ? cbm_arena_sprintf(source->arena, "%s.%s", module, name) : NULL;
    TSNode body = pyp_field(node, "body"), bases = pyp_field(node, "superclasses");
    if (!qn || ts_node_is_null(body))
        return;
    bool complete = ts_node_is_null(bases) || ts_node_named_child_count(bases) == 0;
    bool value_complete = complete;
    bool default_constructor = true;
    size_t first = source->count;
    pyp_add(source, qn, node,
            CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_TOP_LEVEL);
    CBMHashTable *names = cbm_ht_create(16);
    if (!names) {
        source->failed = true;
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            bool decorated = pyp_is(child, "decorated_definition");
            if (decorated) {
                /* Decorator output is still bound by this declaration, but
                 * its callable identity is not the original function. */
                complete = false;
                if (!pyp_members_stable(source, child, 0))
                    value_complete = false;
                child = pyp_field(child, "definition");
            }
            if (pyp_is(child, "function_definition")) {
                const char *method = pyp_text(source, pyp_field(child, "name"));
                if (!method || cbm_ht_get(names, method) || !strcmp(method, "__getattribute__") ||
                    !strcmp(method, "__getattr__") || !strcmp(method, "__new__") ||
                    !strcmp(method, "__setattr__") || !strcmp(method, "__delattr__")) {
                    complete = false;
                    value_complete = false;
                    continue;
                }
                cbm_ht_set(names, method, (void *)1);
                if (!cbm_ht_get(names, method)) {
                    source->failed = true;
                    break;
                }
                if (!pyp_members_stable(source, pyp_field(child, "body"), 0)) {
                    complete = false;
                    value_complete = false;
                }
                /* Defaults and annotations are evaluated while defining the
                 * class too. Unknown calls cannot establish a value domain. */
                if (!pyp_members_stable(source, child, 0))
                    value_complete = false;
                TSNode parameters = pyp_field(child, "parameters");
                uint32_t flags = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC;
                /* The first supported method form has only its ordinary bound
                 * receiver. Decorated and richer signatures remain unproved. */
                if (!decorated && !ts_node_is_null(parameters) &&
                    ts_node_named_child_count(parameters) == 1 &&
                    pyp_is(ts_node_named_child(parameters, 0), "identifier"))
                    flags |= CBM_SOURCE_DECL_FIXED_ARITY;
                if (!strcmp(method, "__init__") && !(flags & CBM_SOURCE_DECL_FIXED_ARITY))
                    default_constructor = false;
                pyp_add(source, cbm_arena_sprintf(source->arena, "%s.%s", qn, method), child,
                        flags);
            } else if (decorated || (ts_node_is_named(child) && !pyp_is(child, "pass_statement") &&
                                     !pyp_is(child, "comment"))) {
                complete = false;
                value_complete = false;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    cbm_ht_free(names);
    if (complete && first < source->count)
        source->defs[first].flags |= CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    if (value_complete && first < source->count)
        source->defs[first].flags |= CBM_SOURCE_CLASS_VALUE_DOMAIN_COMPLETE;
    if (complete && default_constructor && first < source->count)
        source->defs[first].flags |= CBM_SOURCE_CLASS_ZERO_ARGUMENT_CONSTRUCTOR;
}
static int pyp_compare(const void *left, const void *right) {
    const pyp_decl_t *a = left, *b = right;
    int order = strcmp(a->qn, b->qn);
    return order ? order : (a->line > b->line) - (a->line < b->line);
}
bool cbm_py_collect_source_facts(CBMArena *arena, const char *source, int length,
                                 const char *module, CBMLSPDef *defs, int count) {
    if (count < 0 || (count && !defs))
        return false;
    for (int i = 0; i < count; i++)
        defs[i].source_flags = 0;
    if (!arena || !source || length <= 0 || !module)
        return false;
    module = pyp_symbol_scope(arena, module);
    if (!module)
        return false;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_python(void);
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_python());
    TSTree *tree = cbm_parse_plain(parser, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    pyp_source_t facts = {.arena = arena,
                          .source = source,
                          .length = (size_t)length,
                          .failed = ts_node_has_error(root)};
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    if (!facts.failed && ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (pyp_is(child, "class_definition"))
                pyp_class_facts(&facts, child, module);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    ts_tree_delete(tree);
    if (facts.failed)
        return false;
    if (!facts.count)
        return true;
    qsort(facts.defs, facts.count, sizeof(*facts.defs), pyp_compare);
    for (int i = 0; i < count; i++) {
        CBMLSPDef *def = &defs[i];
        const char *qn =
            def->graph_qualified_name ? def->graph_qualified_name : def->qualified_name;
        if (!qn || !def->source_start_line)
            continue;
        pyp_decl_t key = {.qn = qn, .line = def->source_start_line};
        const pyp_decl_t *found =
            bsearch(&key, facts.defs, facts.count, sizeof(*facts.defs), pyp_compare);
        if (found)
            def->source_flags = found->flags;
    }
    return true;
}

typedef struct {
    const char *target;
    bool ambiguous;
} pyp_name_t;
typedef struct {
    pyp_source_t source;
    const CBMTypeRegistry *registry;
    const CBMFileResult *file;
    const char *symbol_scope;
    CBMResolvedCallArray *out;
    cbm_resolve_binding_index_t *bindings;
    CBMResolveTypeMap types;
    CBMHashTable *imports, *callers, *effects;
} pyp_context_t;
static bool pyp_put(pyp_context_t *context, CBMHashTable *table, const char *name,
                    const char *target) {
    if (!name)
        return false;
    pyp_name_t *old = cbm_ht_get(table, name);
    if (old) {
        old->ambiguous = true;
        return true;
    }
    pyp_name_t *value = cbm_arena_calloc(context->source.arena, sizeof(*value));
    const char *key = cbm_arena_strdup(context->source.arena, name);
    if (!value || !key)
        return false;
    value->target = target;
    cbm_ht_set(table, key, value);
    return cbm_ht_get(table, key) == value;
}
static const CBMRegisteredType *pyp_registered_type(pyp_context_t *context, const char *target,
                                                    bool imported, uint32_t domain) {
    CBMTypeQNIter iterator;
    cbm_registry_types_with_qn(context->registry, target, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    const uint32_t required = CBM_SOURCE_DECL_KNOWN | domain;
    while ((candidate = cbm_type_qn_next(&iterator, NULL))) {
        if (!candidate->project_source || (imported && !candidate->export_binding_proven) ||
            !candidate->declaration_identity || !candidate->graph_qualified_name ||
            (candidate->source_flags & required) != required)
            return NULL;
        if (chosen && (strcmp(chosen->declaration_identity, candidate->declaration_identity) ||
                       strcmp(chosen->graph_qualified_name, candidate->graph_qualified_name)))
            return NULL;
        chosen = candidate;
    }
    return iterator.complete ? chosen : NULL;
}
static const CBMRegisteredType *pyp_type(pyp_context_t *context, const char *name, TSNode site,
                                         uint32_t domain) {
    if (!name || ts_node_is_null(site))
        return NULL;
    cbm_resolve_result_t local =
        cbm_resolve_binding_local(context->bindings, name, ts_node_start_byte(site),
                                  ts_node_end_byte(site), CBM_SOURCE_ORIGIN_RAW);
    bool imported = !cbm_resolve_admit(&local);
    const char *target = imported ? NULL : local.target;
    if (imported) {
        if (!cbm_resolve_binding_import(context->bindings, name))
            return NULL;
        const pyp_name_t *binding = cbm_ht_get(context->imports, name);
        if (!binding || binding->ambiguous || !binding->target || !binding->target[0])
            return NULL;
        target = binding->target;
        cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                      .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                      .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .rule = CBM_RESOLVE_RULE_IMPORT_BINDING,
                                      .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                               CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                               CBM_RESOLVE_FACT_IMPORT_BOUND,
                                      .target = target,
                                      .candidate_count = 1};
        proof = cbm_resolve_binding_validate(context->bindings, name, ts_node_start_byte(site),
                                             ts_node_end_byte(site), CBM_SOURCE_ORIGIN_RAW, proof);
        if (!cbm_resolve_admit(&proof))
            return NULL;
    }
    return pyp_registered_type(context, target, imported, domain);
}
static cbm_resolve_result_t pyp_type_evidence(pyp_context_t *context, const CBMRegisteredType *type,
                                              TSNode site) {
    if (!type)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED};
    return cbm_lsp_proof_type_lookup(
        &context->types, pyp_text(&context->source, site), site, "py_proof-type-name-domain",
        type->qualified_name, type->declaration_identity,
        type->graph_qualified_name ? type->graph_qualified_name : type->qualified_name,
        type->source_flags, "py_proof-complete-source-type-lookup");
}

static const CBMRegisteredType *pyp_constructor(pyp_context_t *context, TSNode node) {
    if (!pyp_is(node, "call"))
        return NULL;
    TSNode name = pyp_field(node, "function"), args = pyp_field(node, "arguments");
    if (!pyp_is(name, "identifier") || ts_node_is_null(args) || ts_node_named_child_count(args))
        return NULL;
    const char *spelling = pyp_text(&context->source, name);
    const CBMRegisteredType *type =
        pyp_type(context, spelling, node, CBM_SOURCE_CLASS_DOMAIN_COMPLETE);
    if (!type || !(type->source_flags & CBM_SOURCE_CLASS_ZERO_ARGUMENT_CONSTRUCTOR))
        return NULL;
    return type;
}
static const char *pyp_occurrence_spelling(pyp_context_t *context, TSNode site,
                                           CBMResolvedKind kind) {
    TSNode expression = site;
    if (kind == CBM_RESOLVED_INVOCATION)
        expression = pyp_field(site, "function");
    for (unsigned depth = 0; depth < 128 && pyp_is(expression, "parenthesized_expression");
         depth++) {
        if (ts_node_named_child_count(expression) != 1)
            return NULL;
        expression = ts_node_named_child(expression, 0);
    }
    if (pyp_is(expression, "attribute")) {
        const char *receiver = pyp_text(&context->source, pyp_field(expression, "object"));
        const char *member = pyp_text(&context->source, pyp_field(expression, "attribute"));
        /* The direct argument carrier retains the terminal identifier spelling,
         * while its occurrence spans the full attribute and any parentheses.
         * Determine that role from the AST; other member reads keep the full
         * attribute spelling, as does an invocation. */
        if (kind != CBM_RESOLVED_INVOCATION) {
            TSNode parent = ts_node_parent(site);
            if (pyp_is(parent, "keyword_argument") && ts_node_eq(pyp_field(parent, "value"), site))
                parent = ts_node_parent(parent);
            if (pyp_is(parent, "argument_list"))
                return member;
        }
        return receiver && member
                   ? cbm_arena_sprintf(context->source.arena, "%s.%s", receiver, member)
                   : NULL;
    }
    return pyp_text(&context->source, expression);
}
static void pyp_emit(pyp_context_t *context, const char *caller, TSNode site, CBMResolvedKind kind,
                     cbm_resolve_result_t proof) {
    if (!caller || !cbm_resolve_admit(&proof))
        return;
    const char *relation = kind == CBM_RESOLVED_INVOCATION       ? "CALLS"
                           : kind == CBM_RESOLVED_CALL_REFERENCE ? "CALL_REFERENCE"
                           : kind == CBM_RESOLVED_TYPE_REFERENCE ? "USES_TYPE"
                                                                 : "USAGE";
    const char *spelling = pyp_occurrence_spelling(context, site, kind);
    proof = cbm_lsp_proof_occurrence(&context->types, proof, site, caller, spelling, relation);
    if (!cbm_resolve_admit(&proof))
        return;
    CBMResolvedCall row = {.caller_qn = caller,
                           .callee_qn = proof.target,
                           .strategy = "python_source_binding",
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
static cbm_resolve_method_candidate_t pyp_candidate(const cbm_resolve_method_query_t *query,
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
        (candidate->flags &
         (CBM_FUNC_FLAG_PROPERTY | CBM_FUNC_FLAG_CLASSMETHOD | CBM_FUNC_FLAG_STATICMETHOD)) ||
        candidate->decorator_qns)
        return proof;
    proof.applicable = CBM_RESOLVE_METHOD_YES;
    proof.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    proof.graph_target = candidate->graph_qualified_name;
    proof.graph_target_exact = proof.graph_target != NULL;
    proof.declaration_identity = candidate->declaration_identity;
    proof.declaration_identity_exact = proof.declaration_identity != NULL;
    return proof;
}
static cbm_resolve_method_candidate_t pyp_value_candidate(const cbm_resolve_method_query_t *query,
                                                          const CBMTypeRegistry *owner,
                                                          const CBMRegisteredFunc *candidate,
                                                          void *opaque) {
    (void)query;
    (void)owner;
    (void)opaque;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    const uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC;
    if ((candidate->source_flags & required) != required ||
        !(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) ||
        (candidate->flags & CBM_FUNC_FLAG_AMBIGUOUS_BINDING) || !candidate->decorator_qns ||
        !candidate->decorator_qns[0])
        return proof;
    proof.applicable = CBM_RESOLVE_METHOD_YES;
    proof.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    proof.graph_target = candidate->graph_qualified_name;
    proof.graph_target_exact = proof.graph_target != NULL;
    proof.declaration_identity = candidate->declaration_identity;
    proof.declaration_identity_exact = proof.declaration_identity != NULL;
    return proof;
}
static cbm_resolve_result_t pyp_member(pyp_context_t *context, TSNode attribute, CBMScope *scope,
                                       bool value_binding) {
    cbm_resolve_result_t missing = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                    .reason = "python_member_unproven"};
    TSNode receiver = pyp_field(attribute, "object"), member = pyp_field(attribute, "attribute");
    if (!pyp_is(receiver, "identifier") || !pyp_is(member, "identifier"))
        return missing;
    const char *variable = pyp_text(&context->source, receiver),
               *name = pyp_text(&context->source, member);
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&context->types, scope, variable, ts_node_start_byte(attribute));
    if (fact.status != CBM_RESOLVE_TYPE_KNOWN || !fact.type || fact.type->kind != CBM_TYPE_NAMED)
        return missing;
    uint32_t domain =
        value_binding ? CBM_SOURCE_CLASS_VALUE_DOMAIN_COMPLETE : CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    if (!pyp_registered_type(context, fact.type->data.named.qualified_name, false, domain))
        return missing;
    cbm_resolve_method_query_t query = {
        .proof_pool = &context->types.proofs,
        .proof_source = &context->types.proof_source,
        .lookup_domain = fact.type->data.named.qualified_name,
        .registry = context->registry,
        .receiver = &fact,
        .receiver_qn = fact.type->data.named.qualified_name,
        .method_name = name,
        .argument_count = 0,
        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT | CBM_RESOLVE_METHOD_NAME_IDENTITY,
        .classify = value_binding ? pyp_value_candidate : pyp_candidate};
    return cbm_resolve_method(&query).result;
}
static bool pyp_read_syntax(const char *kind) {
    static const char *const kinds[] = {"block",
                                        "expression_statement",
                                        "return_statement",
                                        "pass_statement",
                                        "call",
                                        "attribute",
                                        "identifier",
                                        "argument_list",
                                        "integer",
                                        "float",
                                        "string",
                                        "string_start",
                                        "string_content",
                                        "string_end",
                                        "true",
                                        "false",
                                        "none",
                                        "comment",
                                        "assignment",
                                        "binary_operator",
                                        "unary_operator",
                                        "parenthesized_expression",
                                        NULL};
    for (size_t i = 0; kinds[i]; i++)
        if (!strcmp(kinds[i], kind))
            return true;
    return false;
}
static void pyp_audit(pyp_context_t *context, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || !ts_node_is_named(node) || !pyp_budget(&context->source, depth))
        return;
    if (!pyp_read_syntax(ts_node_type(node))) {
        context->source.failed = true;
        return;
    }
    if (pyp_is(node, "assignment") && !pyp_is(pyp_field(node, "left"), "identifier")) {
        context->source.failed = true;
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            pyp_audit(context, ts_tree_cursor_current_node(&cursor), depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
typedef struct {
    unsigned parameters;
    bool primitive_arguments;
} pyp_effect_t;
static bool pyp_readonly_body(pyp_context_t *context, TSNode node, CBMHashTable *parameters,
                              bool *primitive, unsigned depth) {
    if (ts_node_is_null(node) || !ts_node_is_named(node))
        return true;
    if (!pyp_budget(&context->source, depth))
        return false;
    if (pyp_is(node, "identifier")) {
        const char *name = pyp_text(&context->source, node);
        return name && cbm_ht_get(parameters, name);
    }
    if (pyp_is(node, "binary_operator") || pyp_is(node, "unary_operator") ||
        pyp_is(node, "comparison_operator"))
        *primitive = true;
    else if (!pyp_is(node, "block") && !pyp_is(node, "return_statement") &&
             !pyp_is(node, "pass_statement") && !pyp_is(node, "comment") &&
             !pyp_is(node, "integer") && !pyp_is(node, "float") && !pyp_is(node, "true") &&
             !pyp_is(node, "false") && !pyp_is(node, "none") &&
             !pyp_is(node, "parenthesized_expression"))
        return false;
    bool safe = true;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            if (!pyp_readonly_body(context, ts_tree_cursor_current_node(&cursor), parameters,
                                   primitive, depth + 1)) {
                safe = false;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return safe;
}
static void pyp_record_effect(pyp_context_t *context, TSNode function) {
    TSNode params = pyp_field(function, "parameters"), body = pyp_field(function, "body");
    const char *name = pyp_text(&context->source, pyp_field(function, "name"));
    const char *qn =
        name ? cbm_arena_sprintf(context->source.arena, "%s.%s", context->symbol_scope, name)
             : NULL;
    if (!qn || ts_node_is_null(params) || ts_node_is_null(body))
        return;
    CBMHashTable *names = cbm_ht_create(8);
    if (!names) {
        context->source.failed = true;
        return;
    }
    bool valid = true, primitive = false;
    unsigned count = ts_node_named_child_count(params);
    for (unsigned i = 0; i < count; i++) {
        TSNode parameter = ts_node_named_child(params, i);
        const char *text =
            pyp_is(parameter, "identifier") ? pyp_text(&context->source, parameter) : NULL;
        if (!text || cbm_ht_get(names, text)) {
            valid = false;
            break;
        }
        cbm_ht_set(names, text, (void *)1);
        if (!cbm_ht_get(names, text)) {
            valid = false;
            break;
        }
    }
    if (valid && pyp_readonly_body(context, body, names, &primitive, 0)) {
        pyp_effect_t *effect = cbm_arena_alloc(context->source.arena, sizeof(*effect));
        if (effect) {
            *effect = (pyp_effect_t){count, primitive};
            cbm_ht_set(context->effects, qn, effect);
        }
    }
    cbm_ht_free(names);
}
static bool pyp_readonly_call(pyp_context_t *context, TSNode call) {
    TSNode callee = pyp_field(call, "function"), args = pyp_field(call, "arguments");
    if (!pyp_is(callee, "identifier") || ts_node_is_null(args))
        return false;
    const char *name = pyp_text(&context->source, callee);
    cbm_resolve_result_t binding =
        cbm_resolve_binding_local(context->bindings, name, ts_node_start_byte(call),
                                  ts_node_end_byte(call), CBM_SOURCE_ORIGIN_RAW);
    const pyp_effect_t *effect =
        cbm_resolve_admit(&binding) ? cbm_ht_get(context->effects, binding.target) : NULL;
    if (!effect || effect->parameters != ts_node_named_child_count(args))
        return false;
    for (unsigned i = 0; i < effect->parameters; i++) {
        TSNode arg = ts_node_named_child(args, i);
        if (pyp_is(arg, "keyword_argument") || pyp_is(arg, "list_splat") ||
            pyp_is(arg, "dictionary_splat"))
            return false;
        if (effect->primitive_arguments && !pyp_is(arg, "integer") && !pyp_is(arg, "float") &&
            !pyp_is(arg, "true") && !pyp_is(arg, "false") && !pyp_is(arg, "none"))
            return false;
    }
    return true;
}
/* Extraction keys a parenthesized direct argument to its outer wrapper.
 * Parentheses preserve the value identity, but composite expressions do not.
 * Keep this bounded by the same nesting limit as the source audit. */
static TSNode pyp_reference_site(TSNode attribute) {
    TSNode site = attribute, parent = ts_node_parent(site);
    for (unsigned depth = 0; depth < 128 && pyp_is(parent, "parenthesized_expression"); depth++) {
        if (ts_node_named_child_count(parent) != 1 ||
            !ts_node_eq(ts_node_named_child(parent, 0), site))
            return attribute;
        site = parent;
        parent = ts_node_parent(site);
    }
    if (pyp_is(parent, "keyword_argument") && ts_node_eq(pyp_field(parent, "value"), site))
        parent = ts_node_parent(parent);
    return pyp_is(parent, "argument_list") ? site : attribute;
}
static void pyp_expression(pyp_context_t *context, TSNode node, const char *caller, CBMScope *scope,
                           unsigned depth) {
    if (!pyp_budget(&context->source, depth))
        return;
    if (pyp_is(node, "call")) {
        TSNode callee = pyp_field(node, "function"), arguments = pyp_field(node, "arguments");
        cbm_resolve_result_t proof = {0};
        if (pyp_is(callee, "attribute") && !ts_node_is_null(arguments) &&
            !ts_node_named_child_count(arguments)) {
            proof = pyp_member(context, callee, scope, false);
            pyp_emit(context, caller, node, CBM_RESOLVED_INVOCATION, proof);
        }
        /* Attribute lookup of the callee precedes its arguments and can run
         * an unproved descriptor, unlike a plain lexical callee lookup. */
        if (pyp_is(callee, "attribute") && !cbm_resolve_admit(&proof))
            cbm_resolve_types_invalidate_visible(&context->types, scope, ts_node_end_byte(callee),
                                                 CBM_RESOLVE_TYPE_UNSUPPORTED);
        /* Argument expressions are evaluated before the callee's effects. */
        if (!ts_node_is_null(arguments))
            pyp_expression(context, arguments, caller, scope, depth + 1);
        if (!cbm_resolve_admit(&proof) && !pyp_constructor(context, node) &&
            !pyp_readonly_call(context, node))
            cbm_resolve_types_invalidate_visible(&context->types, scope, ts_node_end_byte(node),
                                                 CBM_RESOLVE_TYPE_UNSUPPORTED);
        return;
    }
    if (pyp_is(node, "attribute")) {
        cbm_resolve_result_t proof = pyp_member(context, node, scope, false);
        if (cbm_resolve_admit(&proof)) {
            pyp_emit(context, caller, pyp_reference_site(node), CBM_RESOLVED_CALL_REFERENCE, proof);
        } else {
            proof = pyp_member(context, node, scope, true);
            pyp_emit(context, caller, pyp_reference_site(node), CBM_RESOLVED_VALUE_REFERENCE,
                     proof);
            /* A property or unproved attribute can execute code on read.
             * An exact binding at this occurrence does not establish the
             * effects needed to preserve later receiver facts. */
            cbm_resolve_types_invalidate_visible(&context->types, scope, ts_node_end_byte(node),
                                                 CBM_RESOLVE_TYPE_UNSUPPORTED);
        }
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            pyp_expression(context, ts_tree_cursor_current_node(&cursor), caller, scope, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void pyp_statement(pyp_context_t *context, TSNode node, const char *caller,
                          CBMScope *scope) {
    if (pyp_is(node, "expression_statement") && ts_node_named_child_count(node) == 1) {
        TSNode expression = ts_node_named_child(node, 0);
        if (pyp_is(expression, "assignment")) {
            TSNode left = pyp_field(expression, "left"), right = pyp_field(expression, "right");
            const char *name = pyp_text(&context->source, left);
            pyp_expression(context, right, caller, scope, 0);
            const CBMRegisteredType *constructed = pyp_constructor(context, right);
            if (constructed) {
                cbm_resolve_result_t proof =
                    pyp_type_evidence(context, constructed, pyp_field(right, "function"));
                (void)cbm_resolve_types_declare(
                    &context->types, scope, name, ts_node_end_byte(expression),
                    cbm_type_named(context->source.arena, constructed->qualified_name),
                    CBM_RESOLVE_TYPE_CONSTRUCTOR, &proof);
            } else if (pyp_is(right, "identifier")) {
                const char *source = pyp_text(&context->source, right);
                (void)cbm_resolve_types_copy(&context->types, scope, name, scope, source,
                                             ts_node_end_byte(expression), true);
            } else {
                (void)cbm_resolve_types_declare(&context->types, scope, name,
                                                ts_node_end_byte(expression), NULL,
                                                CBM_RESOLVE_TYPE_NO_EVIDENCE, NULL);
            }
            return;
        }
    }
    pyp_expression(context, node, caller, scope, 0);
}
static void pyp_statements(pyp_context_t *context, TSNode body, const char *caller,
                           CBMScope *scope) {
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            pyp_statement(context, ts_tree_cursor_current_node(&cursor), caller, scope);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void pyp_function(pyp_context_t *context, TSNode node, const char *expected_caller) {
    const char *name = pyp_text(&context->source, pyp_field(node, "name"));
    const char *key = name ? cbm_arena_sprintf(context->source.arena, "%u:%s",
                                               ts_node_start_point(node).row + 1, name)
                           : NULL;
    const pyp_name_t *caller = key ? cbm_ht_get(context->callers, key) : NULL;
    TSNode body = pyp_field(node, "body"), parameters = pyp_field(node, "parameters");
    if (!caller || caller->ambiguous || !caller->target || !expected_caller ||
        strcmp(caller->target, expected_caller) || ts_node_is_null(body) ||
        ts_node_is_null(parameters))
        return;
    pyp_audit(context, body, 0);
    bool failed = context->source.failed;
    if (context->source.exhausted)
        return;
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
            if (!pyp_is(parameter, "typed_parameter"))
                continue;
            TSNode name_node = ts_node_named_child(parameter, 0),
                   annotation = pyp_field(parameter, "type");
            if (!pyp_is(name_node, "identifier") || ts_node_is_null(annotation) ||
                ts_node_named_child_count(annotation) != 1)
                continue;
            TSNode type_node = ts_node_named_child(annotation, 0);
            if (!pyp_is(type_node, "identifier"))
                continue;
            const char *variable = pyp_text(&context->source, name_node);
            const CBMRegisteredType *type =
                pyp_type(context, pyp_text(&context->source, type_node), type_node,
                         CBM_SOURCE_CLASS_VALUE_DOMAIN_COMPLETE);
            if (!variable || !type)
                continue;
            cbm_resolve_result_t proof = pyp_type_evidence(context, type, type_node);
            if (cbm_resolve_types_declare(
                    &context->types, scope, variable, ts_node_start_byte(parameter),
                    cbm_type_named(context->source.arena, type->qualified_name),
                    CBM_RESOLVE_TYPE_ANNOTATION, &proof))
                pyp_emit(context, caller->target, type_node, CBM_RESOLVED_TYPE_REFERENCE, proof);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    pyp_statements(context, body, caller->target, scope);
}
/* Build the expected caller from the actual declaration nesting as well as
 * the extractor map. A line/name collision or mismatched owner must not move
 * a valid receiver proof into another callable's scope. Decorated declarations
 * and functions nested inside functions remain outside this source subset. */
static void pyp_callers(pyp_context_t *context, TSNode body, const char *owner, unsigned depth) {
    if (ts_node_is_null(body) || !owner || !pyp_budget(&context->source, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode node = ts_tree_cursor_current_node(&cursor);
            bool function = pyp_is(node, "function_definition");
            if (!function && !pyp_is(node, "class_definition"))
                continue;
            TSNode name_node = pyp_field(node, "name");
            if (!pyp_is(name_node, "identifier"))
                continue;
            const char *name = pyp_text(&context->source, name_node);
            const char *qn =
                name ? cbm_arena_sprintf(context->source.arena, "%s.%s", owner, name) : NULL;
            if (!qn) {
                context->source.failed = true;
                break;
            }
            if (function)
                pyp_function(context, node, qn);
            else
                pyp_callers(context, pyp_field(node, "body"), qn, depth + 1);
            if (context->source.failed)
                break;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

/* Module statements execute in file-module scope, including __init__. Class
 * and function bodies are analyzed separately under their declaration scopes.
 * Imports, definitions and unsupported statements can have runtime effects;
 * they invalidate existing receiver facts without executing their child bodies
 * as module expressions. A later explicit construction can establish new facts. */
static void pyp_module(pyp_context_t *context, TSNode root) {
    CBMScope *scope = cbm_scope_push(context->source.arena, NULL);
    if (!scope || !context->file->module_qn)
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode node = ts_tree_cursor_current_node(&cursor);
            pyp_audit(context, node, 0);
            bool unsupported = context->source.failed;
            if (context->source.exhausted)
                break;
            context->source.failed = false;
            if (unsupported) {
                cbm_resolve_types_invalidate_visible(&context->types, scope, ts_node_end_byte(node),
                                                     CBM_RESOLVE_TYPE_UNSUPPORTED);
                continue;
            }
            pyp_statement(context, node, context->file->module_qn, scope);
            if (context->source.failed)
                break;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

void cbm_py_proof_resolve(CBMArena *arena, const char *source, int length,
                          const CBMTypeRegistry *registry, const CBMFileResult *file,
                          const char **import_names, const char **import_qns, int import_count,
                          CBMResolvedCallArray *out) {
    if (!arena || !source || length <= 0 || !registry || !file || !out || file->has_error ||
        file->parse_incomplete || file->parse_unusable)
        return;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_python(void);
    if (!parser)
        return;
    ts_parser_set_language(parser, tree_sitter_python());
    TSTree *tree = cbm_parse_plain(parser, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return;
    TSNode root = ts_tree_root_node(tree);
    /* Append through a private descriptor; publish only after complete analysis.
     * Arena growth preserves the original buffer and its committed prefix. */
    CBMResolvedCallArray staged = *out;
    cbm_syntax_index_t syntax;
    cbm_syntax_index_begin(&syntax, tree);
    pyp_context_t context = {.source = {.arena = arena,
                                        .source = source,
                                        .length = (size_t)length,
                                        .failed = ts_node_has_error(root)},
                             .registry = registry,
                             .file = file,
                             .symbol_scope = pyp_symbol_scope(arena, file->module_qn),
                             .out = &staged};
    context.bindings =
        cbm_resolve_binding_build(arena, file, CBM_LANG_PYTHON, source, (size_t)length);
    context.imports = cbm_ht_create(16);
    context.callers = cbm_ht_create(16);
    context.effects = cbm_ht_create(16);
    cbm_resolve_types_init(&context.types, arena, 0);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, file->module_qn);
    if (!cbm_lsp_proof_context(&context.types, &proof_source, file->module_qn))
        context.source.failed = true;
    if (context.imports && context.callers && context.effects && context.bindings &&
        context.symbol_scope) {
        for (int i = 0; i < import_count; i++)
            if (import_names && import_names[i] && import_qns && import_qns[i])
                if (!pyp_put(&context, context.imports, import_names[i], import_qns[i]))
                    context.source.failed = true;
        for (int i = 0; i < file->defs.count; i++) {
            const CBMDefinition *def = &file->defs.items[i];
            if (!def->name || !def->qualified_name || !def->label ||
                (strcmp(def->label, "Function") && strcmp(def->label, "Method")))
                continue;
            const char *key = cbm_arena_sprintf(arena, "%u:%s", def->start_line, def->name);
            if (!key || !pyp_put(&context, context.callers, key, def->qualified_name))
                context.source.failed = true;
        }
        TSTreeCursor effects = ts_tree_cursor_new(root);
        if (!context.source.failed && ts_tree_cursor_goto_first_child(&effects))
            do {
                TSNode child = ts_tree_cursor_current_node(&effects);
                if (pyp_is(child, "function_definition"))
                    pyp_record_effect(&context, child);
            } while (ts_tree_cursor_goto_next_sibling(&effects));
        ts_tree_cursor_delete(&effects);
        if (!context.source.failed)
            pyp_callers(&context, root, context.symbol_scope, 0);
        if (!context.source.failed)
            pyp_module(&context, root);
    }
    if (!context.source.failed)
        *out = staged;
    cbm_ht_free(context.imports);
    cbm_ht_free(context.callers);
    cbm_ht_free(context.effects);
    cbm_syntax_index_end(&syntax);
    ts_tree_delete(tree);
}
