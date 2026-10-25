#include "kotlin_proof.h"
#include "../callable_sig.h"
#include "../resolve/symbols.h"
#include "../resolve/policy.h"
#include "foundation/hash_table.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *qn;
    uint32_t line, flags;
    int arity;
} kt_source_decl_t;
typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length, visits;
    bool failed;
    kt_source_decl_t *declarations;
    size_t count, capacity;
    const char *package;
} kt_proof_source_t;
static bool kt_proof_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}
static TSNode kt_proof_child(TSNode node, const char *kind) {
    if (ts_node_is_null(node))
        return (TSNode){0};
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    TSNode found = {0};
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (kt_proof_is(child, kind)) {
                found = child;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return found;
}
static const char *kt_proof_text(kt_proof_source_t *source, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= source->length
               ? cbm_arena_strndup(source->arena, source->source + start, end - start)
               : NULL;
}
static bool kt_proof_budget(kt_proof_source_t *source, unsigned depth) {
    if (++source->visits > 1000000 || depth > 128)
        source->failed = true;
    return !source->failed;
}
static const char *kt_proof_join(kt_proof_source_t *source, const char *prefix, const char *name) {
    if (!name) {
        source->failed = true;
        return NULL;
    }
    const char *qn =
        prefix && prefix[0] ? cbm_arena_sprintf(source->arena, "%s.%s", prefix, name) : name;
    if (!qn)
        source->failed = true;
    return qn;
}
static uint32_t kt_proof_modifiers(kt_proof_source_t *source, TSNode node) {
    uint32_t flags = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC;
    TSNode modifiers = kt_proof_child(node, "modifiers");
    if (!ts_node_is_null(modifiers)) {
        TSTreeCursor cursor = ts_tree_cursor_new(modifiers);
        if (ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (kt_proof_is(child, "visibility_modifier")) {
                    const char *text = kt_proof_text(source, child);
                    if (!text || strcmp(text, "public"))
                        flags &= ~CBM_SOURCE_DECL_PUBLIC;
                }
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
    }
    return flags;
}
static void kt_proof_add(kt_proof_source_t *source, const char *qn, TSNode node, uint32_t flags,
                         int arity) {
    if (!qn) {
        source->failed = true;
        return;
    }
    if (source->count == source->capacity) {
        size_t capacity = source->capacity ? source->capacity * 2 : 32;
        kt_source_decl_t *items = cbm_arena_alloc(source->arena, capacity * sizeof(*items));
        if (!items) {
            source->failed = true;
            return;
        }
        if (source->count)
            memcpy(items, source->declarations, source->count * sizeof(*items));
        source->declarations = items;
        source->capacity = capacity;
    }
    source->declarations[source->count++] =
        (kt_source_decl_t){qn, ts_node_start_point(node).row + 1, flags, arity};
}
static void kt_proof_function_fact(kt_proof_source_t *source, TSNode node, const char *parent) {
    TSNode name_node = kt_proof_child(node, "simple_identifier");
    const char *name = kt_proof_text(source, name_node);
    const char *qn = kt_proof_join(source, parent, name);
    uint32_t flags = kt_proof_modifiers(source, node);
    TSNode parameters = kt_proof_child(node, "function_value_parameters");
    int arity = ts_node_is_null(parameters) ? -1 : 0;
    if (!ts_node_is_null(kt_proof_child(node, "type_parameters")))
        flags = 0;
    /* A receiver type before the function name declares an extension. Its
     * lookup priority and applicability belong to a separate proof domain. */
    TSTreeCursor header = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&header))
        do {
            TSNode child = ts_tree_cursor_current_node(&header);
            if (ts_node_is_named(child) &&
                ts_node_start_byte(child) < ts_node_start_byte(name_node) &&
                !kt_proof_is(child, "modifiers"))
                flags = 0;
        } while (ts_tree_cursor_goto_next_sibling(&header));
    ts_tree_cursor_delete(&header);
    if (!ts_node_is_null(parameters)) {
        TSTreeCursor cursor = ts_tree_cursor_new(parameters);
        if (ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (!ts_node_is_named(child))
                    continue;
                if (kt_proof_is(child, "parameter"))
                    arity++;
                else {
                    arity = -1;
                    break;
                }
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
    }
    kt_proof_add(source, qn, node, flags, arity);
}
static void kt_proof_class_fact(kt_proof_source_t *source, TSNode node, const char *parent,
                                unsigned depth) {
    if (!kt_proof_budget(source, depth))
        return;
    const char *name = kt_proof_text(source, kt_proof_child(node, "type_identifier"));
    const char *qn = kt_proof_join(source, parent, name);
    if (!qn)
        return;
    bool complete = ts_node_is_null(kt_proof_child(node, "type_parameters")) &&
                    ts_node_is_null(kt_proof_child(node, "delegation_specifier")) &&
                    ts_node_is_null(kt_proof_child(node, "delegation_specifiers"));
    TSTreeCursor declaration = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&declaration))
        do {
            TSNode child = ts_tree_cursor_current_node(&declaration);
            if (ts_node_is_named(child) && !kt_proof_is(child, "type_identifier") &&
                !kt_proof_is(child, "class_body") && !kt_proof_is(child, "modifiers"))
                complete = false;
        } while (ts_tree_cursor_goto_next_sibling(&declaration));
    ts_tree_cursor_delete(&declaration);
    uint32_t flags = kt_proof_modifiers(source, node);
    if (depth == 0)
        flags |= CBM_SOURCE_DECL_TOP_LEVEL;
    size_t type_at = source->count;
    kt_proof_add(source, qn, node, flags, -1);
    TSNode body = kt_proof_child(node, "class_body");
    if (!ts_node_is_null(body)) {
        TSTreeCursor cursor = ts_tree_cursor_new(body);
        if (ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (kt_proof_is(child, "function_declaration"))
                    kt_proof_function_fact(source, child, qn);
                else if (ts_node_is_named(child) && !kt_proof_is(child, "line_comment") &&
                         !kt_proof_is(child, "multiline_comment"))
                    complete = false;
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
    }
    if (complete && type_at < source->count)
        source->declarations[type_at].flags |= CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
}
static const char *kt_proof_package(kt_proof_source_t *source, TSNode root) {
    TSNode header = kt_proof_child(root, "package_header");
    if (ts_node_is_null(header))
        return "";
    const char *name = kt_proof_text(source, kt_proof_child(header, "identifier"));
    if (!name)
        source->failed = true;
    return name;
}
static int kt_proof_compare(const void *left, const void *right) {
    const kt_source_decl_t *a = left, *b = right;
    int order = strcmp(a->qn, b->qn);
    return order ? order : (a->line > b->line) - (a->line < b->line);
}
bool cbm_kotlin_collect_source_facts(CBMArena *arena, const char *source, int length,
                                     const char *module, CBMLSPDef *defs, int count) {
    if (count < 0 || (count && !defs))
        return false;
    for (int i = 0; i < count; i++)
        defs[i].source_flags = 0;
    if (!arena || !source || length <= 0 || !module)
        return false;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_kotlin(void);
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_kotlin());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    kt_proof_source_t facts = {.arena = arena,
                               .source = source,
                               .length = (size_t)length,
                               .failed = ts_node_has_error(root)};
    const char *package = kt_proof_package(&facts, root);
    const char *prefix = package && package[0] ? package : module;
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    if (!facts.failed && ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (kt_proof_is(child, "class_declaration"))
                kt_proof_class_fact(&facts, child, prefix, 0);
            else if (kt_proof_is(child, "function_declaration")) {
                size_t first = facts.count;
                kt_proof_function_fact(&facts, child, prefix);
                if (first < facts.count && facts.declarations[first].flags)
                    facts.declarations[first].flags |= CBM_SOURCE_DECL_TOP_LEVEL;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    ts_tree_delete(tree);
    if (facts.failed)
        return false;
    if (!facts.count)
        return true;
    qsort(facts.declarations, facts.count, sizeof(*facts.declarations), kt_proof_compare);
    for (int i = 0; i < count; i++) {
        CBMLSPDef *def = &defs[i];
        if (!def->qualified_name || !def->source_start_line)
            continue;
        size_t base_length = cbm_qn_callable_base_len_named(def->qualified_name, def->short_name);
        const char *base = def->qualified_name[base_length]
                               ? cbm_arena_strndup(arena, def->qualified_name, base_length)
                               : def->qualified_name;
        if (!base)
            return false;
        kt_source_decl_t key = {.qn = base, .line = def->source_start_line};
        const kt_source_decl_t *found = bsearch(&key, facts.declarations, facts.count,
                                                sizeof(*facts.declarations), kt_proof_compare);
        if (!found)
            continue;
        size_t offset = (size_t)(found - facts.declarations);
        if ((offset && !kt_proof_compare(found - 1, &key)) ||
            (offset + 1 < facts.count && !kt_proof_compare(found + 1, &key)))
            continue;
        def->source_flags = found->flags;
        if (found->arity >= 0 && found->arity == def->signature_param_count)
            def->source_flags |= CBM_SOURCE_DECL_FIXED_ARITY;
        def->namespace_name = package;
    }
    return true;
}

typedef struct {
    const char *target;
    bool ambiguous;
} kt_proof_name_t;
typedef struct {
    kt_proof_source_t source;
    const CBMTypeRegistry *registry;
    const CBMFileResult *file;
    CBMResolvedCallArray *out;
    CBMResolveTypeMap types;
    CBMHashTable *imports, *callers, *local_types;
    bool wildcard, bare_calls;
} kt_proof_context_t;
static bool kt_proof_put(kt_proof_context_t *context, CBMHashTable *table, const char *name,
                         const char *target) {
    if (!name)
        return false;
    kt_proof_name_t *prior = cbm_ht_get(table, name);
    if (prior) {
        prior->ambiguous = true;
        return true;
    }
    kt_proof_name_t *entry = cbm_arena_calloc(context->source.arena, sizeof(*entry));
    const char *key = cbm_arena_strdup(context->source.arena, name);
    if (!entry || !key)
        return false;
    entry->target = target;
    cbm_ht_set(table, key, entry);
    return cbm_ht_get(table, key) == entry;
}
static bool kt_proof_prepare(kt_proof_context_t *context, TSNode root) {
    context->source.package = kt_proof_package(&context->source, root);
    for (int i = 0; i < context->file->defs.count; i++) {
        const CBMDefinition *def = &context->file->defs.items[i];
        if (!def->name || !def->qualified_name || !def->label)
            continue;
        if (!strcmp(def->label, "Function") || !strcmp(def->label, "Method")) {
            const char *key =
                cbm_arena_sprintf(context->source.arena, "%u:%s", def->start_line, def->name);
            if (!key || !kt_proof_put(context, context->callers, key, def->qualified_name))
                return false;
        } else if (cbm_label_is_type_like(def->label)) {
            if (!kt_proof_put(context, context->local_types, def->name, def->qualified_name))
                return false;
        }
    }
    TSNode imports = kt_proof_child(root, "import_list");
    if (ts_node_is_null(imports))
        return true;
    TSTreeCursor cursor = ts_tree_cursor_new(imports);
    bool ok = true;
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode header = ts_tree_cursor_current_node(&cursor);
            if (!kt_proof_is(header, "import_header"))
                continue;
            const char *text = kt_proof_text(&context->source, header);
            if (!text) {
                ok = false;
                break;
            }
            if (strchr(text, '*')) {
                context->wildcard = true;
                continue;
            }
            const char *target =
                kt_proof_text(&context->source, kt_proof_child(header, "identifier"));
            TSNode alias = kt_proof_child(header, "import_alias");
            const char *name = NULL;
            if (!ts_node_is_null(alias))
                name = kt_proof_text(&context->source, kt_proof_child(alias, "type_identifier"));
            else if (target) {
                name = strrchr(target, '.');
                name = name ? name + 1 : target;
            }
            if (!target || !kt_proof_put(context, context->imports, name, target)) {
                ok = false;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return ok;
}
static bool kt_proof_same_type(const CBMRegisteredType *a, const CBMRegisteredType *b) {
    return a && b && a->declaration_identity && b->declaration_identity &&
           a->graph_qualified_name && b->graph_qualified_name &&
           !strcmp(a->declaration_identity, b->declaration_identity) &&
           !strcmp(a->graph_qualified_name, b->graph_qualified_name);
}
static const CBMRegisteredType *kt_proof_namespace_type(kt_proof_context_t *context,
                                                        const char *package, const char *name) {
    CBMNamespaceTypeIter iterator;
    cbm_registry_types_in_namespace(context->registry, package, name, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    while ((candidate = cbm_namespace_type_next(&iterator, NULL))) {
        uint32_t required =
            CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
        if (!candidate->project_source || !candidate->declaration_identity ||
            (candidate->source_flags & required) != required ||
            (chosen && !kt_proof_same_type(chosen, candidate)))
            return NULL;
        chosen = candidate;
    }
    return iterator.complete ? chosen : NULL;
}
static const CBMRegisteredType *kt_proof_type_name(kt_proof_context_t *context, const char *name) {
    if (!name || !name[0] || strpbrk(name, "<>?()[] `"))
        return NULL;
    const char *target = name;
    const char *dot = strrchr(name, '.');
    if (!dot) {
        /* Nested/local type declarations need their own lexical domain. */
        if (cbm_ht_get(context->local_types, name))
            return NULL;
        const kt_proof_name_t *import = cbm_ht_get(context->imports, name);
        if (import) {
            if (import->ambiguous || !import->target)
                return NULL;
            target = import->target;
            dot = strrchr(target, '.');
        } else
            return kt_proof_namespace_type(context, context->source.package, name);
    }
    const char *package =
        dot ? cbm_arena_strndup(context->source.arena, target, (size_t)(dot - target)) : "";
    return package ? kt_proof_namespace_type(context, package, dot ? dot + 1 : target) : NULL;
}
static cbm_resolve_result_t kt_proof_type_evidence(const CBMRegisteredType *type) {
    return (cbm_resolve_result_t){
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_TYPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = type->graph_qualified_name ? type->graph_qualified_name : type->qualified_name,
        .candidate_count = 1,
        .reason = "kotlin_source_declared_type"};
}
static void kt_proof_emit(kt_proof_context_t *context, const char *caller, TSNode site,
                          CBMResolvedKind kind, cbm_resolve_result_t proof) {
    if (!caller || !cbm_resolve_admit(&proof))
        return;
    CBMResolvedCall row = {.caller_qn = caller,
                           .callee_qn = proof.target,
                           .strategy = "kotlin_source_binding",
                           .confidence = 1.0f,
                           .kind = kind,
                           .site_start_byte = ts_node_start_byte(site),
                           .site_end_byte = ts_node_end_byte(site),
                           .source_origin = CBM_SOURCE_ORIGIN_RAW,
                           .evidence = {proof.analysis, proof.rule, proof.facts}};
    cbm_resolvedcall_push(context->out, context->source.arena, row);
}
static bool kt_proof_read_syntax(const char *kind) {
    static const char *const allowed[] = {"function_body",
                                          "statements",
                                          "call_expression",
                                          "navigation_expression",
                                          "navigation_suffix",
                                          "simple_identifier",
                                          "call_suffix",
                                          "value_arguments",
                                          "value_argument",
                                          "integer_literal",
                                          "real_literal",
                                          "boolean_literal",
                                          "null_literal",
                                          "string_literal",
                                          "string_content",
                                          "line_comment",
                                          "multiline_comment",
                                          "jump_expression",
                                          "if_expression",
                                          "control_structure_body",
                                          "comparison_expression",
                                          "equality_expression",
                                          "additive_expression",
                                          "multiplicative_expression",
                                          "parenthesized_expression",
                                          "assignment",
                                          "property_declaration",
                                          "variable_declaration",
                                          "user_type",
                                          "type_identifier",
                                          "modifiers",
                                          "visibility_modifier",
                                          NULL};
    for (size_t i = 0; allowed[i]; i++)
        if (!strcmp(kind, allowed[i]))
            return true;
    return false;
}
static void kt_proof_body_audit(kt_proof_context_t *context, TSNode node, CBMHashTable *blocked,
                                bool binding, unsigned depth) {
    if (ts_node_is_null(node) || !ts_node_is_named(node) ||
        !kt_proof_budget(&context->source, depth))
        return;
    if (!kt_proof_read_syntax(ts_node_type(node))) {
        context->source.failed = true;
        return;
    }
    binding =
        binding || kt_proof_is(node, "property_declaration") || kt_proof_is(node, "assignment");
    if (binding && kt_proof_is(node, "simple_identifier")) {
        const char *name = kt_proof_text(&context->source, node);
        if (!name) {
            context->source.failed = true;
            return;
        }
        cbm_ht_set(blocked, name, (void *)1);
        if (!cbm_ht_get(blocked, name))
            context->source.failed = true;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            kt_proof_body_audit(context, ts_tree_cursor_current_node(&cursor), blocked, binding,
                                depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static cbm_resolve_method_candidate_t kt_proof_candidate(const cbm_resolve_method_query_t *query,
                                                         const CBMTypeRegistry *owner,
                                                         const CBMRegisteredFunc *candidate,
                                                         void *context) {
    (void)query;
    (void)owner;
    (void)context;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    uint32_t required =
        CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_FIXED_ARITY;
    if ((candidate->source_flags & required) != required || candidate->min_params != 0 ||
        candidate->type_param_names || !(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE))
        return proof;
    proof.applicable = CBM_RESOLVE_METHOD_YES;
    proof.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    proof.graph_target = candidate->graph_qualified_name ? candidate->graph_qualified_name
                                                         : candidate->qualified_name;
    proof.graph_target_exact = proof.graph_target != NULL;
    proof.declaration_identity = candidate->declaration_identity;
    proof.declaration_identity_exact = candidate->declaration_identity != NULL;
    return proof;
}
static void kt_proof_call(kt_proof_context_t *context, TSNode node, const char *caller,
                          CBMScope *scope, CBMHashTable *bound, CBMHashTable *blocked) {
    TSNode suffix = kt_proof_child(node, "call_suffix"),
           args = kt_proof_child(suffix, "value_arguments");
    if (ts_node_is_null(args) || ts_node_named_child_count(args) ||
        ts_node_named_child_count(suffix) != 1)
        return;
    TSNode target = ts_node_named_child(node, 0);
    cbm_resolve_method_result_t resolved = {0};
    uint32_t domain = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                      CBM_RESOLVE_METHOD_OCCURRENCE_EXACT | CBM_RESOLVE_METHOD_NAME_IDENTITY;
    if (kt_proof_is(target, "navigation_expression")) {
        TSNode receiver = ts_node_named_child(target, 0),
               member = kt_proof_child(target, "navigation_suffix");
        TSNode name_node = kt_proof_child(member, "simple_identifier");
        if (!kt_proof_is(receiver, "simple_identifier") || ts_node_named_child_count(target) != 2 ||
            ts_node_is_null(name_node))
            return;
        const char *variable = kt_proof_text(&context->source, receiver);
        const char *name = kt_proof_text(&context->source, name_node);
        cbm_resolve_type_fact_t fact =
            cbm_resolve_types_lookup(&context->types, scope, variable, ts_node_start_byte(node));
        if (fact.status != CBM_RESOLVE_TYPE_KNOWN || !fact.type ||
            fact.type->kind != CBM_TYPE_NAMED)
            return;
        cbm_resolve_method_query_t query = {.registry = context->registry,
                                            .receiver = &fact,
                                            .receiver_qn = fact.type->data.named.qualified_name,
                                            .method_name = name,
                                            .argument_count = 0,
                                            .adapter_facts = domain,
                                            .classify = kt_proof_candidate};
        resolved = cbm_resolve_method(&query);
    } else if (kt_proof_is(target, "simple_identifier")) {
        const char *name = kt_proof_text(&context->source, target);
        if (!context->bare_calls || !name || cbm_ht_get(bound, name) || cbm_ht_get(blocked, name) ||
            cbm_ht_get(context->local_types, name) || context->wildcard)
            return;
        const kt_proof_name_t *import = cbm_ht_get(context->imports, name);
        if (import && (import->ambiguous || !import->target))
            return;
        const char *qn = import ? import->target
                                : kt_proof_join(&context->source, context->source.package, name);
        cbm_resolve_function_query_t query = {
            .registry = context->registry,
            .function_qn = qn,
            .function_binding = {CBM_RESOLVE_ANALYSIS_COMPLETE, CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                                 CBM_RESOLVE_FACT_LANGUAGE_BOUND |
                                     CBM_RESOLVE_FACT_OCCURRENCE_EXACT},
            .argument_count = 0,
            .adapter_facts = domain,
            .classify = kt_proof_candidate};
        resolved = cbm_resolve_function(&query);
    }
    kt_proof_emit(context, caller, node, CBM_RESOLVED_INVOCATION, resolved.result);
}
static void kt_proof_body_calls(kt_proof_context_t *context, TSNode node, const char *caller,
                                CBMScope *scope, CBMHashTable *bound, CBMHashTable *blocked,
                                unsigned depth) {
    if (!kt_proof_budget(&context->source, depth))
        return;
    if (kt_proof_is(node, "call_expression"))
        kt_proof_call(context, node, caller, scope, bound, blocked);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            kt_proof_body_calls(context, ts_tree_cursor_current_node(&cursor), caller, scope, bound,
                                blocked, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void kt_proof_function(kt_proof_context_t *context, TSNode node, bool top_level) {
    const char *name = kt_proof_text(&context->source, kt_proof_child(node, "simple_identifier"));
    const char *key = name ? cbm_arena_sprintf(context->source.arena, "%u:%s",
                                               ts_node_start_point(node).row + 1, name)
                           : NULL;
    const kt_proof_name_t *caller = key ? cbm_ht_get(context->callers, key) : NULL;
    TSNode body = kt_proof_child(node, "function_body"),
           params = kt_proof_child(node, "function_value_parameters");
    if (!caller || caller->ambiguous || ts_node_is_null(body) || ts_node_is_null(params) ||
        !ts_node_is_null(kt_proof_child(node, "type_parameters")))
        return;
    /* Extension receivers introduce an implicit scope whose members can
     * shadow imported names and parameter type spellings. */
    TSNode function_name = kt_proof_child(node, "simple_identifier");
    TSTreeCursor header = ts_tree_cursor_new(node);
    bool ordinary = true;
    if (ts_tree_cursor_goto_first_child(&header))
        do {
            TSNode child = ts_tree_cursor_current_node(&header);
            if (ts_node_is_named(child) &&
                ts_node_start_byte(child) < ts_node_start_byte(function_name) &&
                !kt_proof_is(child, "modifiers"))
                ordinary = false;
        } while (ts_tree_cursor_goto_next_sibling(&header));
    ts_tree_cursor_delete(&header);
    if (!ordinary)
        return;
    context->bare_calls = top_level;
    int first = context->out->count;
    CBMHashTable *blocked = cbm_ht_create(16), *bound = cbm_ht_create(16);
    if (!blocked || !bound) {
        cbm_ht_free(blocked);
        cbm_ht_free(bound);
        return;
    }
    kt_proof_body_audit(context, body, blocked, false, 0);
    bool failed = context->source.failed;
    context->source.failed = false;
    CBMScope *scope = cbm_scope_push(context->source.arena, NULL);
    if (!failed && scope) {
        TSTreeCursor cursor = ts_tree_cursor_new(params);
        if (ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode parameter = ts_tree_cursor_current_node(&cursor);
                if (!kt_proof_is(parameter, "parameter"))
                    continue;
                const char *variable =
                    kt_proof_text(&context->source, kt_proof_child(parameter, "simple_identifier"));
                if (!variable) {
                    failed = true;
                    break;
                }
                if (cbm_ht_get(bound, variable)) {
                    failed = true;
                    break;
                }
                cbm_ht_set(bound, variable, (void *)1);
                if (!cbm_ht_get(bound, variable)) {
                    failed = true;
                    break;
                }
                if (cbm_ht_get(blocked, variable))
                    continue;
                TSNode type_node = kt_proof_child(parameter, "user_type");
                const char *spelling = kt_proof_text(&context->source, type_node);
                const CBMRegisteredType *type = kt_proof_type_name(context, spelling);
                if (!type)
                    continue;
                cbm_resolve_result_t proof = kt_proof_type_evidence(type);
                if (cbm_resolve_types_declare(
                        &context->types, scope, variable, ts_node_start_byte(parameter),
                        cbm_type_named(context->source.arena, type->qualified_name),
                        CBM_RESOLVE_TYPE_ANNOTATION, &proof))
                    kt_proof_emit(context, caller->target, type_node, CBM_RESOLVED_TYPE_REFERENCE,
                                  proof);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
        if (!failed)
            kt_proof_body_calls(context, body, caller->target, scope, bound, blocked, 0);
    }
    if (failed)
        context->out->count = first;
    cbm_ht_free(blocked);
    cbm_ht_free(bound);
}
static void kt_proof_walk(kt_proof_context_t *context, TSNode node, unsigned depth) {
    if (!kt_proof_budget(&context->source, depth))
        return;
    if (kt_proof_is(node, "function_declaration")) {
        kt_proof_function(context, node, depth == 1);
        return;
    }
    if (!kt_proof_is(node, "source_file") && !kt_proof_is(node, "class_body") &&
        !kt_proof_is(node, "class_declaration"))
        return;
    if (kt_proof_is(node, "class_declaration") &&
        !ts_node_is_null(kt_proof_child(node, "type_parameters")))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            kt_proof_walk(context, ts_tree_cursor_current_node(&cursor), depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
void cbm_kotlin_proof_resolve(CBMArena *arena, const char *source, int length, TSNode root,
                              const CBMTypeRegistry *registry, const CBMFileResult *file,
                              CBMResolvedCallArray *out) {
    if (!arena || !source || length <= 0 || !registry || !file || !out || ts_node_is_null(root) ||
        ts_node_has_error(root) || file->has_error || file->parse_incomplete ||
        file->parse_unusable)
        return;
    kt_proof_context_t context = {
        .source = {.arena = arena, .source = source, .length = (size_t)length},
        .registry = registry,
        .file = file,
        .out = out};
    context.imports = cbm_ht_create(32);
    context.callers = cbm_ht_create(32);
    context.local_types = cbm_ht_create(32);
    cbm_resolve_types_init(&context.types, arena, 0);
    int first = out->count;
    if (context.imports && context.callers && context.local_types &&
        kt_proof_prepare(&context, root))
        kt_proof_walk(&context, root, 0);
    if (context.source.failed)
        out->count = first;
    cbm_ht_free(context.imports);
    cbm_ht_free(context.callers);
    cbm_ht_free(context.local_types);
}
