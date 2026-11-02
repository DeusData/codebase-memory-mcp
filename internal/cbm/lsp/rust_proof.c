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
    const CBMType *signature;
} rsp_decl_t;
typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length, visits;
    bool failed, primitive_domain;
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
static void rsp_add(rsp_source_t *source, const char *qn, TSNode node, uint32_t flags,
                    const CBMType *signature) {
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
    source->defs[source->count++] =
        (rsp_decl_t){qn, ts_node_start_point(node).row + 1, flags, signature};
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
static bool rsp_primitive_name(const char *name) {
    static const char *const names[] = {"bool", "char",  "i8",  "i16", "i32", "i64",
                                        "i128", "isize", "u8",  "u16", "u32", "u64",
                                        "u128", "usize", "f32", "f64"};
    for (size_t i = 0; name && i < sizeof(names) / sizeof(names[0]); i++)
        if (!strcmp(name, names[i]))
            return true;
    return false;
}

/* Rust primitive names can be shadowed in the type namespace. This bounded
 * adapter abstains when an import/type declaration can replace one; it never
 * promotes a primitive-looking token without checking that source namespace. */
static bool rsp_primitive_domain(rsp_source_t *source, TSNode root) {
    for (uint32_t i = 0; i < ts_node_named_child_count(root); i++) {
        TSNode child = ts_node_named_child(root, i);
        if (rsp_is(child, "use_declaration") || rsp_is(child, "extern_crate_declaration")) {
            const char *text = rsp_text(source, child);
            if (!text || strchr(text, '*'))
                return false;
            for (const char *at = text; at && *at;) {
                if ((*at >= 'a' && *at <= 'z') || (*at >= 'A' && *at <= 'Z') || *at == '_') {
                    const char *start = at++;
                    while ((*at >= 'a' && *at <= 'z') || (*at >= 'A' && *at <= 'Z') ||
                           (*at >= '0' && *at <= '9') || *at == '_')
                        at++;
                    size_t length = (size_t)(at - start);
                    char word[8];
                    if (length < sizeof(word)) {
                        memcpy(word, start, length);
                        word[length] = '\0';
                        if (rsp_primitive_name(word))
                            return false;
                    }
                } else
                    at++;
            }
        } else if (rsp_is(child, "attribute_item") || rsp_is(child, "inner_attribute_item") ||
                   rsp_is(child, "macro_invocation") ||
                   ((rsp_is(child, "type_item") || rsp_is(child, "struct_item") ||
                     rsp_is(child, "enum_item") || rsp_is(child, "trait_item") ||
                     rsp_is(child, "union_item") || rsp_is(child, "mod_item")) &&
                    rsp_primitive_name(rsp_text(source, rsp_field(child, "name")))))
            return false;
    }
    return true;
}

static const CBMType *rsp_primitive(rsp_source_t *source, TSNode node) {
    if (!source->primitive_domain || !rsp_is(node, "primitive_type"))
        return NULL;
    const char *name = rsp_text(source, node);
    return rsp_primitive_name(name) ? cbm_type_builtin(source->arena, name) : NULL;
}

static const CBMType *rsp_signature(rsp_source_t *source, TSNode parameters) {
    if (ts_node_is_null(parameters) || !ts_node_named_child_count(parameters))
        return NULL;
    TSNode self = ts_node_named_child(parameters, 0);
    const char *receiver = rsp_text(source, self);
    if (!rsp_is(self, "self_parameter") || !receiver || strcmp(receiver, "&self"))
        return NULL;
    const CBMType *types[65] = {0};
    size_t count = 0;
    uint32_t children = ts_node_named_child_count(parameters);
    for (uint32_t i = 1; i < children; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (rsp_is(parameter, "line_comment") || rsp_is(parameter, "block_comment"))
            continue;
        if (!rsp_is(parameter, "parameter") || ts_node_named_child_count(parameter) != 2 ||
            !rsp_is(rsp_field(parameter, "pattern"), "identifier") || count == 64)
            return NULL;
        const CBMType *type = rsp_primitive(source, rsp_field(parameter, "type"));
        if (!type)
            return NULL;
        types[count++] = type;
    }
    const CBMType **params = cbm_arena_alloc(source->arena, (count + 1) * sizeof(*params));
    if (!params)
        return NULL;
    memcpy(params, types, (count + 1) * sizeof(*params));
    return cbm_type_func(source->arena, NULL, params, NULL);
}

static void rsp_class_facts(rsp_source_t *source, TSNode node, const char *module) {
    if (!rsp_is(node, "struct_item") || rsp_child(node, "type_parameters") ||
        rsp_child(node, "where_clause"))
        return;
    const char *name = rsp_text(source, rsp_field(node, "name"));
    if (name)
        rsp_add(source, cbm_arena_sprintf(source->arena, "%s.%s", module, name), node,
                CBM_SOURCE_DECL_KNOWN | (rsp_public(source, node) ? CBM_SOURCE_DECL_PUBLIC : 0) |
                    CBM_SOURCE_DECL_TOP_LEVEL | CBM_SOURCE_CLASS_DOMAIN_COMPLETE,
                NULL);
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
            if (!rsp_is(method, "function_item") || rsp_child(method, "type_parameters") ||
                rsp_child(method, "where_clause") || rsp_child(method, "function_modifiers"))
                continue;
            const CBMType *signature = rsp_signature(source, rsp_field(method, "parameters"));
            if (!signature)
                continue;
            const char *name = rsp_text(source, rsp_field(method, "name"));
            if (name)
                rsp_add(source, cbm_arena_sprintf(source->arena, "%s.%s.%s", module, owner, name),
                        method,
                        CBM_SOURCE_DECL_KNOWN |
                            (rsp_public(source, method) ? CBM_SOURCE_DECL_PUBLIC : 0) |
                            CBM_SOURCE_DECL_FIXED_ARITY,
                        signature);
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
    for (int i = 0; i < count; i++) {
        defs[i].source_flags = 0;
        defs[i].source_signature = NULL;
    }
    if (!arena || !source || length <= 0 || !module)
        return false;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_rust(void);
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_rust());
    TSTree *tree = cbm_parse_source_repairing(parser, CBM_LANG_RUST, source, (uint32_t)length,
                                              (TSParseOptions){0});
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    rsp_source_t facts = {.arena = arena,
                          .source = source,
                          .length = (size_t)length,
                          .failed = ts_node_has_error(root)};
    facts.primitive_domain = rsp_primitive_domain(&facts, root);
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
        if (found) {
            def->source_flags = found->flags;
            def->source_signature = found->signature;
        }
    }
    return true;
}

typedef struct {
    const char *target;
    const cbm_resolve_proof_t *import_proof;
    bool ambiguous;
} rsp_name_t;
typedef struct {
    rsp_source_t source;
    const CBMTypeRegistry *registry;
    const CBMFileResult *file;
    CBMResolvedCallArray *out;
    cbm_resolve_binding_index_t *bindings;
    CBMResolveTypeMap types;
    CBMHashTable *imports, *callers, *local_types;
    bool require_import_proofs;
    const CBMRegisteredType *enclosing_type;
    cbm_resolve_result_t enclosing_type_proof;
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
static bool rsp_local_target(rsp_context_t *context, const char *qn) {
    const char *module = context->file->module_qn;
    size_t length = module ? strlen(module) : 0;
    return length && qn && !strncmp(qn, module, length) && qn[length] == '.';
}

static const CBMRegisteredType *rsp_type(rsp_context_t *context, const char *name) {
    if (!name)
        return NULL;
    const rsp_name_t *local = cbm_ht_get(context->local_types, name);
    const rsp_name_t *import = cbm_ht_get(context->imports, name);
    const rsp_name_t *binding = local ? local : import;
    if ((local && import) || (!local && !cbm_resolve_binding_import(context->bindings, name)) ||
        !binding || binding->ambiguous || !binding->target || !binding->target[0] ||
        (!local && context->require_import_proofs && !binding->import_proof))
        return NULL;
    if (local) {
        const char *module = context->file->module_qn;
        size_t length = module ? strlen(module) : 0;
        if (!rsp_local_target(context, binding->target) ||
            strcmp(binding->target + length + 1, name))
            return NULL;
    }
    CBMTypeQNIter iterator;
    cbm_registry_types_with_qn(context->registry, binding->target, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    const uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    while ((candidate = cbm_type_qn_next(&iterator, NULL))) {
        if (!candidate->project_source || !candidate->declaration_identity ||
            !candidate->graph_qualified_name || (candidate->source_flags & required) != required ||
            (!local && !(candidate->source_flags & CBM_SOURCE_DECL_PUBLIC)))
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
    const char *spelling = rsp_text(&context->source, site);
    const rsp_name_t *import = spelling ? cbm_ht_get(context->imports, spelling) : NULL;
    if (import && context->require_import_proofs && (import->ambiguous || !import->import_proof))
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                      .reason = "rust_import_predecessor_missing"};
    cbm_resolve_result_t result = cbm_lsp_proof_type_lookup(
        &context->types, spelling, site, "rust_proof-type-name-domain", type->qualified_name,
        type->declaration_identity,
        type->graph_qualified_name ? type->graph_qualified_name : type->qualified_name,
        type->source_flags, "rust_proof-complete-source-type-lookup");
    if (!import || !context->require_import_proofs || !cbm_resolve_admit(&result))
        return result;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(result.proof);
    if (!view)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_FAILED};
    const cbm_resolve_proof_t *premises[] = {result.proof, import->import_proof};
    const cbm_resolve_proof_t *proof =
        cbm_resolve_proof_derive(&context->types.proofs, CBM_PROOF_TYPE_BIND, &view->source,
                                 &view->key, &result, premises, 2, true);
    view = cbm_resolve_proof_view(proof);
    if (!view)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_FAILED,
                                      .reason = "rust_import_predecessor_join_failed"};
    result = view->outcome;
    result.proof = proof;
    return result;
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
static cbm_resolve_result_t rsp_primitive_evidence(rsp_context_t *context, const CBMType *type,
                                                   TSNode site, const char *decision) {
    if (!type || type->kind != CBM_TYPE_BUILTIN || ts_node_is_null(site))
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED};
    cbm_resolve_result_t result = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_TYPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = type->data.builtin.name,
        .candidate_count = 1,
        .reason = decision};
    cbm_resolve_proof_key_t key = {.lookup_domain = "rust-primitive-source-domain",
                                   .lookup_key = rsp_text(&context->source, site),
                                   .type_identity = type->data.builtin.name,
                                   .start_byte = ts_node_start_byte(site),
                                   .end_byte = ts_node_end_byte(site),
                                   .detail = decision};
    result.proof = cbm_resolve_proof_leaf(&context->types.proofs, CBM_PROOF_SOURCE_FACT,
                                          &context->types.proof_source, &key, &result);
    if (!result.proof)
        result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
    return result;
}

static cbm_resolve_method_candidate_t rsp_candidate(const cbm_resolve_method_query_t *query,
                                                    const CBMTypeRegistry *owner,
                                                    const CBMRegisteredFunc *candidate,
                                                    void *opaque) {
    (void)query;
    (void)owner;
    rsp_context_t *context = opaque;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    const uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_FIXED_ARITY;
    bool accessible = (candidate->source_flags & CBM_SOURCE_DECL_PUBLIC) ||
                      rsp_local_target(context, candidate->graph_qualified_name);
    if ((candidate->source_flags & required) != required || !accessible ||
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
/* Literal expectations are applied only after a unique method name and its
 * source signature are established. Unsuffixed integers use that exact
 * contextual integer type; explicit suffixes never undergo widening. */
static const CBMType *rsp_integer(rsp_context_t *context, TSNode node, const CBMType *expected,
                                  bool negative) {
    const char *text = rsp_text(&context->source, node);
    if (!context->source.primitive_domain || !text || !*text || !expected ||
        expected->kind != CBM_TYPE_BUILTIN || !expected->data.builtin.name)
        return NULL;
    uint64_t value = 0;
    const unsigned char *at = (const unsigned char *)text;
    bool digit = false;
    while ((*at >= '0' && *at <= '9') || *at == '_') {
        if (*at != '_') {
            if (value > (UINT64_MAX - (*at - '0')) / 10)
                return NULL;
            value = value * 10 + (*at - '0');
            digit = true;
        }
        at++;
    }
    const char *name = *at ? (const char *)at : expected->data.builtin.name;
    if (!digit || !name || strcmp(name, expected->data.builtin.name))
        return NULL;
    bool signed_type = name[0] == 'i';
    if ((!signed_type && name[0] != 'u') || (negative && !signed_type))
        return NULL;
    unsigned bits = !strcmp(name + 1, "8")     ? 8
                    : !strcmp(name + 1, "16")  ? 16
                    : !strcmp(name + 1, "32")  ? 32
                    : !strcmp(name + 1, "64")  ? 64
                    : !strcmp(name + 1, "128") ? 128
                                               : 0;
    if (!bits)
        return NULL; /* pointer-sized literal ranges require target metadata */
    uint64_t limit = UINT64_MAX;
    if (bits < 64)
        limit = signed_type ? (UINT64_C(1) << (bits - 1)) - !negative : (UINT64_C(1) << bits) - 1;
    else if (bits == 64 && signed_type)
        limit = UINT64_C(9223372036854775807) + (negative ? 1 : 0);
    return value <= limit ? expected : NULL;
}

static const CBMType *rsp_argument_type(rsp_context_t *context, CBMScope *scope, TSNode node,
                                        const CBMType *expected, cbm_resolve_result_t *proof,
                                        unsigned depth) {
    if (ts_node_is_null(node) || depth > 32)
        return NULL;
    if (rsp_is(node, "identifier")) {
        cbm_resolve_type_fact_t fact = cbm_resolve_types_lookup(
            &context->types, scope, rsp_text(&context->source, node), ts_node_start_byte(node));
        if (cbm_resolve_types_fact_status(&fact) != CBM_RESOLVE_TYPE_KNOWN || !fact.type ||
            fact.type->kind != CBM_TYPE_BUILTIN)
            return NULL;
        *proof = fact.evidence;
        return fact.type;
    }
    if (rsp_is(node, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return rsp_argument_type(context, scope, ts_node_named_child(node, 0), expected, proof,
                                 depth + 1);
    const CBMType *type = NULL;
    if (rsp_is(node, "integer_literal"))
        type = rsp_integer(context, node, expected, false);
    else if (rsp_is(node, "unary_expression") && ts_node_named_child_count(node) == 1) {
        TSNode inner = ts_node_named_child(node, 0);
        const char *text = rsp_text(&context->source, node);
        if (text && text[0] == '-' && rsp_is(inner, "integer_literal"))
            type = rsp_integer(context, inner, expected, true);
    } else if (rsp_is(node, "boolean_literal") && context->source.primitive_domain)
        type = cbm_type_builtin(context->source.arena, "bool");
    if (type)
        *proof =
            rsp_primitive_evidence(context, type, node, "rust-source-contextual-primitive-literal");
    return type;
}

static cbm_resolve_result_t rsp_arguments(rsp_context_t *context, CBMScope *scope, TSNode args,
                                          cbm_resolve_method_result_t selected) {
    cbm_resolve_result_t result = selected.result;
    if (!cbm_resolve_admit(&result) || !selected.method)
        return result;
    const CBMType *sig = selected.method->source_signature;
    const CBMType *const *params =
        sig && sig->kind == CBM_TYPE_FUNC ? sig->data.func.param_types : NULL;
    size_t count = 0;
    for (uint32_t i = 0; i < ts_node_named_child_count(args); i++) {
        TSNode argument = ts_node_named_child(args, i);
        if (rsp_is(argument, "line_comment") || rsp_is(argument, "block_comment"))
            continue;
        if (!params || count == 64 || !params[count])
            goto unsupported;
        cbm_resolve_result_t evidence = {0};
        const CBMType *type =
            rsp_argument_type(context, scope, argument, params[count], &evidence, 0);
        if (!type || type->kind != CBM_TYPE_BUILTIN || params[count]->kind != CBM_TYPE_BUILTIN ||
            !type->data.builtin.name || !params[count]->data.builtin.name ||
            strcmp(type->data.builtin.name, params[count]->data.builtin.name) ||
            !cbm_resolve_admit(&evidence) || !evidence.proof)
            goto unsupported;
        result = cbm_lsp_proof_rule(&context->types, result, argument,
                                    selected.method->qualified_name, selected.method->short_name,
                                    "rust-source-exact-primitive-argument", evidence.proof);
        if (!cbm_resolve_admit(&result))
            return result;
        count++;
    }
    if (!params || params[count] || count != (size_t)selected.method->min_params)
        goto unsupported;
    return result;
unsupported:
    return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                  .reason = "rust_argument_applicability_unproven"};
}

static cbm_resolve_result_t rsp_member(rsp_context_t *context, TSNode call, CBMScope *scope) {
    cbm_resolve_result_t missing = {.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                    .reason = "rust_member_unproven"};
    TSNode attribute = rsp_field(call, "function"), args = rsp_field(call, "arguments");
    TSNode receiver = rsp_field(attribute, "value"), member = rsp_field(attribute, "field");
    if ((!rsp_is(receiver, "identifier") && !rsp_is(receiver, "self")) ||
        !rsp_is(member, "field_identifier") || ts_node_is_null(args))
        return missing;
    const char *variable = rsp_text(&context->source, receiver),
               *name = rsp_text(&context->source, member);
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&context->types, scope, variable, ts_node_start_byte(attribute));
    if (cbm_resolve_types_fact_status(&fact) != CBM_RESOLVE_TYPE_KNOWN || !fact.type ||
        fact.type->kind != CBM_TYPE_NAMED)
        return missing;
    cbm_resolve_method_query_t query = {.proof_pool = &context->types.proofs,
                                        .proof_source = &context->types.proof_source,
                                        .lookup_domain = fact.type->data.named.qualified_name,
                                        .registry = context->registry,
                                        .receiver = &fact,
                                        .receiver_qn = fact.type->data.named.qualified_name,
                                        .method_name = name,
                                        .argument_count = -1,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                         CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                        .classify = rsp_candidate,
                                        .context = context};
    return rsp_arguments(context, scope, args, cbm_resolve_method(&query));
}
static bool rsp_read_syntax(const char *kind) {
    static const char *const kinds[] = {"block",
                                        "expression_statement",
                                        "empty_statement",
                                        "return_expression",
                                        "call_expression",
                                        "field_expression",
                                        "identifier",
                                        "self",
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
static bool rsp_argument_syntax(TSNode node, unsigned depth) {
    if (depth > 32)
        return false;
    if (rsp_is(node, "identifier") || rsp_is(node, "integer_literal") ||
        rsp_is(node, "boolean_literal") || rsp_is(node, "float_literal") ||
        rsp_is(node, "string_literal") || rsp_is(node, "line_comment") ||
        rsp_is(node, "block_comment"))
        return true;
    return (rsp_is(node, "parenthesized_expression") || rsp_is(node, "unary_expression")) &&
           ts_node_named_child_count(node) == 1 &&
           rsp_argument_syntax(ts_node_named_child(node, 0), depth + 1);
}
static void rsp_audit(rsp_context_t *context, TSNode node, unsigned depth) {
    if (ts_node_is_null(node) || !ts_node_is_named(node) || !rsp_budget(&context->source, depth))
        return;
    if (!rsp_read_syntax(ts_node_type(node)) || (depth && rsp_is(node, "block"))) {
        context->source.failed = true;
        return;
    }
    if (rsp_is(node, "let_declaration") && (!rsp_is(rsp_field(node, "pattern"), "identifier") ||
                                            !ts_node_is_null(rsp_field(node, "type")))) {
        context->source.failed = true;
        return;
    }
    if (rsp_is(node, "assignment_expression") &&
        (!rsp_is(rsp_field(node, "left"), "identifier") ||
         (!rsp_is(ts_node_parent(node), "expression_statement") &&
          !rsp_is(ts_node_parent(node), "block")))) {
        context->source.failed = true;
        return;
    }
    if (rsp_is(node, "call_expression")) {
        TSNode callee = rsp_field(node, "function"), args = rsp_field(node, "arguments");
        /* Unsupported calls and effects do not donate receiver facts. */
        if (!rsp_is(callee, "field_expression") || ts_node_is_null(args)) {
            context->source.failed = true;
            return;
        }
        for (uint32_t i = 0; i < ts_node_named_child_count(args); i++)
            if (!rsp_argument_syntax(ts_node_named_child(args, i), 0)) {
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
    /* `let x;` has no value to walk. */
    if (ts_node_is_null(node) || !rsp_budget(&context->source, depth))
        return;
    if (rsp_is(node, "call_expression")) {
        TSNode callee = rsp_field(node, "function");
        if (rsp_is(callee, "field_expression"))
            rsp_emit(context, caller, node, CBM_RESOLVED_INVOCATION,
                     rsp_member(context, node, scope));
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
            if (rsp_is(parameter, "self_parameter") && context->enclosing_type) {
                const char *text = rsp_text(&context->source, parameter);
                if (text && !strcmp(text, "&self")) {
                    const CBMRegisteredType *type = context->enclosing_type;
                    cbm_resolve_result_t proof = cbm_lsp_proof_rule(
                        &context->types, context->enclosing_type_proof, parameter,
                        type->qualified_name, "self", "rust-lexical-impl-shared-self", NULL);
                    (void)cbm_resolve_types_declare(
                        &context->types, scope, "self", ts_node_start_byte(parameter),
                        cbm_type_named(context->source.arena, type->qualified_name),
                        CBM_RESOLVE_TYPE_ANNOTATION, &proof);
                }
                continue;
            }
            if (!rsp_is(parameter, "parameter"))
                continue;
            TSNode name_node = rsp_field(parameter, "pattern"),
                   annotation = rsp_field(parameter, "type");
            if (!rsp_is(name_node, "identifier") || ts_node_is_null(annotation) ||
                ts_node_named_child_count(parameter) != 2)
                continue;
            const CBMType *primitive = rsp_primitive(&context->source, annotation);
            if (primitive) {
                cbm_resolve_result_t proof = rsp_primitive_evidence(
                    context, primitive, annotation, "rust-source-primitive-annotation");
                (void)cbm_resolve_types_declare(
                    &context->types, scope, rsp_text(&context->source, name_node),
                    ts_node_start_byte(parameter), primitive, CBM_RESOLVE_TYPE_ANNOTATION, &proof);
                continue;
            }
            if (!rsp_is(annotation, "reference_type") || ts_node_named_child_count(annotation) != 1)
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
static void rust_proof_resolve(CBMArena *arena, const char *source, int length,
                               const CBMTypeRegistry *registry, const CBMFileResult *file,
                               const char **import_names, const char **import_qns, int import_count,
                               CBMResolvedCallArray *out,
                               const cbm_resolve_import_snapshot_t *imports,
                               bool require_import_proofs) {
    if (!arena || !source || length <= 0 || !registry || !file || !out || file->has_error ||
        file->parse_incomplete || file->parse_unusable)
        return;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_rust(void);
    if (!parser)
        return;
    ts_parser_set_language(parser, tree_sitter_rust());
    TSTree *tree = cbm_parse_source_repairing(parser, CBM_LANG_RUST, source, (uint32_t)length,
                                              (TSParseOptions){0});
    ts_parser_delete(parser);
    if (!tree)
        return;
    TSNode root = ts_tree_root_node(tree);
    /* Append through a private descriptor; publish only after complete analysis.
     * Arena growth preserves the original buffer and its committed prefix. */
    CBMResolvedCallArray staged = *out;
    rsp_context_t context = {.source = {.arena = arena,
                                        .source = source,
                                        .length = (size_t)length,
                                        .failed = ts_node_has_error(root)},
                             .registry = registry,
                             .file = file,
                             .out = &staged,
                             .require_import_proofs = require_import_proofs};
    context.bindings =
        cbm_resolve_binding_build(arena, file, CBM_LANG_RUST, source, (size_t)length);
    context.source.primitive_domain = rsp_primitive_domain(&context.source, root);
    if (rsp_child(root, "attribute_item") || rsp_child(root, "inner_attribute_item") ||
        rsp_child(root, "macro_invocation"))
        context.source.failed = true;
    context.imports = cbm_ht_create(16);
    context.callers = cbm_ht_create(16);
    context.local_types = cbm_ht_create(16);
    cbm_resolve_types_init(&context.types, arena, 0);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, file->module_qn);
    if (!cbm_resolve_import_source_matches(imports, &proof_source))
        imports = NULL;
    if (!cbm_lsp_proof_context(&context.types, &proof_source, file->module_qn))
        context.source.failed = true;
    if (context.imports && context.callers && context.local_types && context.bindings) {
        for (int i = 0; i < import_count; i++)
            if (import_names && import_names[i] && import_qns && import_qns[i])
                if (!rsp_put(&context, context.imports, import_names[i], import_qns[i]))
                    context.source.failed = true;
        for (size_t i = 0; imports && i < imports->count; i++) {
            const cbm_resolve_import_fact_t *fact = &imports->items[i];
            rsp_name_t *binding =
                fact->local_name ? cbm_ht_get(context.imports, fact->local_name) : NULL;
            if (binding && !binding->ambiguous && binding->target &&
                cbm_resolve_admit(&fact->dependency) && cbm_resolve_admit(&fact->binding) &&
                fact->dependency.proof && fact->binding.proof &&
                !strcmp(binding->target, fact->binding.target))
                binding->import_proof = fact->binding.proof;
        }
        for (int i = 0; i < file->defs.count; i++) {
            const CBMDefinition *def = &file->defs.items[i];
            if (!def->name || !def->qualified_name || !def->label)
                continue;
            if (!strcmp(def->label, "Struct")) {
                if (!rsp_put(&context, context.local_types, def->name, def->qualified_name))
                    context.source.failed = true;
                continue;
            }
            if (strcmp(def->label, "Function") && strcmp(def->label, "Method"))
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
                else if (rsp_is(child, "impl_item") && ts_node_is_null(rsp_field(child, "trait")) &&
                         rsp_is(rsp_field(child, "type"), "type_identifier") &&
                         !rsp_child(child, "type_parameters") &&
                         !rsp_child(child, "where_clause")) {
                    TSNode body = rsp_field(child, "body");
                    if (rsp_child(body, "attribute_item") || rsp_child(body, "macro_invocation"))
                        continue;
                    TSNode type_site = rsp_field(child, "type");
                    context.enclosing_type =
                        rsp_type(&context, rsp_text(&context.source, type_site));
                    context.enclosing_type_proof =
                        rsp_type_evidence(&context, context.enclosing_type, type_site);
                    for (uint32_t j = 0; j < ts_node_named_child_count(body); j++) {
                        TSNode method = ts_node_named_child(body, j);
                        if (rsp_is(method, "function_item"))
                            rsp_function(&context, method);
                    }
                    context.enclosing_type = NULL;
                }
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
    }
    if (!context.source.failed)
        *out = staged;
    cbm_ht_free(context.imports);
    cbm_ht_free(context.callers);
    cbm_ht_free(context.local_types);
    ts_tree_delete(tree);
}

void cbm_rust_proof_resolve(CBMArena *arena, const char *source, int length,
                            const CBMTypeRegistry *registry, const CBMFileResult *file,
                            const char **names, const char **targets, int count,
                            CBMResolvedCallArray *out) {
    rust_proof_resolve(arena, source, length, registry, file, names, targets, count, out, NULL,
                       false);
}

void cbm_rust_proof_resolve_with_imports(CBMArena *arena, const char *source, int length,
                                         const CBMTypeRegistry *registry, const CBMFileResult *file,
                                         const char **names, const char **targets, int count,
                                         CBMResolvedCallArray *out,
                                         const cbm_resolve_import_snapshot_t *imports) {
    rust_proof_resolve(arena, source, length, registry, file, names, targets, count, out, imports,
                       true);
}
