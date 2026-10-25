#include "proof_bridge.h"
#include "cs_proof.h"
#include "../callable_sig.h"
#include "../resolve/symbols.h"
#include "../resolve/policy.h"
#include "foundation/hash_table.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *qn, *namespace;
    uint32_t line, flags;
    int arity;
} cs_source_decl_t;
typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length, visits;
    bool failed;
    cs_source_decl_t *declarations;
    size_t count, capacity;
} cs_proof_source_t;
static bool csp_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}
static TSNode csp_field(TSNode node, const char *field) {
    return ts_node_is_null(node)
               ? (TSNode){0}
               : ts_node_child_by_field_name(node, field, (uint32_t)strlen(field));
}
static bool csp_child_kind(TSNode node, const char *kind) {
    if (ts_node_is_null(node))
        return false;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool found = false;
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            if (csp_is(ts_tree_cursor_current_node(&cursor), kind)) {
                found = true;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return found;
}
static const char *csp_text(cs_proof_source_t *source, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= source->length
               ? cbm_arena_strndup(source->arena, source->source + start, end - start)
               : NULL;
}
static bool csp_budget(cs_proof_source_t *source, unsigned depth) {
    if (++source->visits > 1000000 || depth > 128)
        source->failed = true;
    return !source->failed;
}
static const char *csp_join(cs_proof_source_t *source, const char *prefix, const char *name) {
    const char *result =
        name
            ? (prefix && prefix[0] ? cbm_arena_sprintf(source->arena, "%s.%s", prefix, name) : name)
            : NULL;
    if (!result)
        source->failed = true;
    return result;
}
static bool csp_modifier(cs_proof_source_t *source, TSNode node, const char *wanted) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    bool found = false;
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (csp_is(child, "modifier")) {
                const char *text = csp_text(source, child);
                if (!text)
                    source->failed = true;
                else if (!strcmp(text, wanted))
                    found = true;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return found;
}
static uint32_t csp_modifiers(cs_proof_source_t *source, TSNode node, bool interface_member) {
    uint32_t flags = CBM_SOURCE_DECL_KNOWN;
    if (interface_member || csp_modifier(source, node, "public"))
        flags |= CBM_SOURCE_DECL_PUBLIC;
    if (csp_modifier(source, node, "static"))
        flags |= CBM_SOURCE_DECL_STATIC;
    if (csp_modifier(source, node, "partial"))
        flags = 0;
    return flags;
}
static int csp_arity(TSNode method) {
    TSNode parameters = csp_field(method, "parameters");
    if (ts_node_is_null(parameters))
        return -1;
    TSTreeCursor cursor = ts_tree_cursor_new(parameters);
    int count = 0;
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child))
                continue;
            if (!csp_is(child, "parameter")) {
                count = -1;
                break;
            }
            count++;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return count;
}
static void csp_add(cs_proof_source_t *source, const char *qn, const char *namespace, TSNode node,
                    uint32_t flags, int arity) {
    if (!qn) {
        source->failed = true;
        return;
    }
    if (source->count == source->capacity) {
        size_t capacity = source->capacity ? source->capacity * 2 : 32;
        cs_source_decl_t *items = cbm_arena_alloc(source->arena, capacity * sizeof(*items));
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
        (cs_source_decl_t){qn, namespace, ts_node_start_point(node).row + 1, flags, arity};
}
static bool csp_simple_class(cs_proof_source_t *source, TSNode node) {
    if (csp_modifier(source, node, "partial"))
        return false;
    bool complete = true;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    TSNode name = csp_field(node, "name"), body = csp_field(node, "body");
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child) && !ts_node_eq(child, name) && !ts_node_eq(child, body) &&
                !csp_is(child, "modifier"))
                complete = false;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    if (ts_node_is_null(body))
        return false;
    cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child) && !csp_is(child, "method_declaration") &&
                !csp_is(child, "comment"))
                complete = false;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return complete;
}
static void csp_class_facts(cs_proof_source_t *source, TSNode node, const char *module,
                            const char *namespace) {
    const char *name = csp_text(source, csp_field(node, "name"));
    const char *qn = csp_join(source, module, name);
    bool complete = csp_simple_class(source, node);
    uint32_t flags = csp_modifiers(source, node, false) | CBM_SOURCE_DECL_TOP_LEVEL;
    if (complete)
        flags |= CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    csp_add(source, qn, namespace, node, flags, -1);
    TSNode body = csp_field(node, "body");
    if (ts_node_is_null(body))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!csp_is(child, "method_declaration"))
                continue;
            const char *method = csp_text(source, csp_field(child, "name"));
            uint32_t method_flags =
                csp_modifiers(source, child, csp_is(node, "interface_declaration"));
            if (csp_child_kind(child, "type_parameter_list"))
                method_flags = 0;
            csp_add(source, csp_join(source, qn, method), namespace, child, method_flags,
                    csp_arity(child));
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void csp_declarations(cs_proof_source_t *source, TSNode node, const char *module,
                             const char *namespace, unsigned depth) {
    if (!csp_budget(source, depth))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (csp_is(child, "namespace_declaration")) {
                const char *ns =
                    csp_join(source, namespace, csp_text(source, csp_field(child, "name")));
                TSNode body = csp_field(child, "body");
                if (ns && !ts_node_is_null(body))
                    csp_declarations(source, body, module, ns, depth + 1);
            } else if (csp_is(child, "file_scoped_namespace_declaration")) {
                namespace = csp_join(source, namespace, csp_text(source, csp_field(child, "name")));
            } else if (csp_is(child, "class_declaration") ||
                       csp_is(child, "interface_declaration") ||
                       csp_is(child, "struct_declaration")) {
                csp_class_facts(source, child, module, namespace);
            } else if (strncmp(ts_node_type(child), "preproc_", 8) == 0)
                source->failed = true;
        } while (!source->failed && ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static int csp_compare(const void *left, const void *right) {
    const cs_source_decl_t *a = left, *b = right;
    int order = strcmp(a->qn, b->qn);
    return order ? order : (a->line > b->line) - (a->line < b->line);
}
bool cbm_cs_collect_source_facts(CBMArena *arena, const char *source, int length,
                                 const char *module, CBMLSPDef *defs, int count) {
    if (count < 0 || (count && !defs))
        return false;
    for (int i = 0; i < count; i++)
        defs[i].source_flags = 0;
    if (!arena || !source || length <= 0 || !module)
        return false;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_c_sharp(void);
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_c_sharp());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    cs_proof_source_t facts = {.arena = arena,
                               .source = source,
                               .length = (size_t)length,
                               .failed = ts_node_has_error(root)};
    if (!facts.failed)
        csp_declarations(&facts, root, module, "", 0);
    ts_tree_delete(tree);
    if (facts.failed)
        return false;
    if (!facts.count)
        return true;
    qsort(facts.declarations, facts.count, sizeof(*facts.declarations), csp_compare);
    for (int i = 0; i < count; i++) {
        CBMLSPDef *def = &defs[i];
        const char *qn =
            def->graph_qualified_name ? def->graph_qualified_name : def->qualified_name;
        if (!qn || !def->source_start_line)
            continue;
        size_t length = cbm_qn_callable_base_len_named(qn, def->short_name);
        const char *base = qn[length] ? cbm_arena_strndup(arena, qn, length) : qn;
        if (!base)
            return false;
        cs_source_decl_t key = {.qn = base, .line = def->source_start_line};
        const cs_source_decl_t *found = bsearch(&key, facts.declarations, facts.count,
                                                sizeof(*facts.declarations), csp_compare);
        if (!found)
            continue;
        size_t offset = (size_t)(found - facts.declarations);
        if ((offset && !csp_compare(found - 1, &key)) ||
            (offset + 1 < facts.count && !csp_compare(found + 1, &key)))
            continue;
        def->source_flags = found->flags;
        def->namespace_name = found->namespace;
        if (found->arity >= 0 && found->arity == def->signature_param_count)
            def->source_flags |= CBM_SOURCE_DECL_FIXED_ARITY;
    }
    return true;
}

typedef struct csp_scope {
    struct csp_scope *parent, *allocated_next;
    const char *namespace;
    CBMHashTable *aliases;
    const char **namespaces;
    size_t count, capacity;
    bool unsupported;
} csp_scope_t;
typedef struct {
    const char *target;
    bool ambiguous;
} csp_name_t;
typedef struct {
    cs_proof_source_t source;
    const CBMTypeRegistry *registry;
    const CBMFileResult *file;
    CBMResolvedCallArray *out;
    CBMResolveTypeMap types;
    CBMHashTable *callers, *local_types;
    csp_scope_t *allocated;
    bool static_call;
} csp_context_t;
static bool csp_put(csp_context_t *context, CBMHashTable *table, const char *name,
                    const char *target) {
    if (!name)
        return false;
    csp_name_t *prior = cbm_ht_get(table, name);
    if (prior) {
        prior->ambiguous = true;
        return true;
    }
    csp_name_t *entry = cbm_arena_calloc(context->source.arena, sizeof(*entry));
    const char *key = cbm_arena_strdup(context->source.arena, name);
    if (!entry || !key)
        return false;
    entry->target = target;
    cbm_ht_set(table, key, entry);
    return cbm_ht_get(table, key) == entry;
}
static csp_scope_t *csp_scope(csp_context_t *context, csp_scope_t *parent, const char *namespace) {
    csp_scope_t *scope = cbm_arena_calloc(context->source.arena, sizeof(*scope));
    if (!scope)
        return NULL;
    scope->aliases = cbm_ht_create(8);
    if (!scope->aliases)
        return NULL;
    scope->parent = parent;
    scope->namespace = namespace;
    scope->allocated_next = context->allocated;
    context->allocated = scope;
    return scope;
}
static void csp_using(csp_context_t *context, csp_scope_t *scope, TSNode node) {
    const char *alias = csp_text(&context->source, csp_field(node, "name"));
    TSNode target = {0};
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (csp_is(child, "static") || csp_is(child, "global"))
                scope->unsupported = true;
            if (ts_node_is_named(child) && !ts_node_eq(child, csp_field(node, "name")))
                target = child;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    const char *text = csp_text(&context->source, target);
    if (!text || (!csp_is(target, "qualified_name") && !csp_is(target, "identifier"))) {
        scope->unsupported = true;
        return;
    }
    if (alias) {
        if (!csp_put(context, scope->aliases, alias, text))
            context->source.failed = true;
        return;
    }
    if (scope->count == scope->capacity) {
        size_t capacity = scope->capacity ? scope->capacity * 2 : 8;
        const char **namespaces =
            cbm_arena_alloc(context->source.arena, capacity * sizeof(*namespaces));
        if (!namespaces) {
            context->source.failed = true;
            return;
        }
        if (scope->count)
            memcpy(namespaces, scope->namespaces, scope->count * sizeof(*namespaces));
        scope->namespaces = namespaces;
        scope->capacity = capacity;
    }
    scope->namespaces[scope->count++] = text;
}
static bool csp_same_type(const CBMRegisteredType *a, const CBMRegisteredType *b) {
    return a && b && a->declaration_identity && b->declaration_identity &&
           a->graph_qualified_name && b->graph_qualified_name &&
           !strcmp(a->declaration_identity, b->declaration_identity) &&
           !strcmp(a->graph_qualified_name, b->graph_qualified_name);
}
static const CBMRegisteredType *csp_namespace_type(csp_context_t *context, const char *namespace,
                                                   const char *name, bool *complete) {
    CBMNamespaceTypeIter iterator;
    cbm_registry_types_in_namespace(context->registry, namespace, name, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    uint32_t required =
        CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    while ((candidate = cbm_namespace_type_next(&iterator, NULL))) {
        if (!candidate->project_source || !candidate->declaration_identity ||
            (candidate->source_flags & required) != required ||
            (chosen && !csp_same_type(chosen, candidate))) {
            *complete = false;
            return NULL;
        }
        chosen = candidate;
    }
    if (!iterator.complete)
        *complete = false;
    return chosen;
}
static const CBMRegisteredType *csp_qualified_type(csp_context_t *context, const char *name,
                                                   bool *complete) {
    const char *dot = strrchr(name, '.');
    const char *namespace =
        dot ? cbm_arena_strndup(context->source.arena, name, (size_t)(dot - name)) : "";
    if (!namespace) {
        *complete = false;
        return NULL;
    }
    return csp_namespace_type(context, namespace, dot ? dot + 1 : name, complete);
}
static const CBMRegisteredType *csp_type(csp_context_t *context, csp_scope_t *scope,
                                         const char *name) {
    if (!name || !name[0] || strpbrk(name, "<>?[]()@ :"))
        return NULL;
    for (csp_scope_t *s = scope; s; s = s->parent)
        if (s->unsupported)
            return NULL;
    const char *dot = strchr(name, '.');
    const char *first =
        dot ? cbm_arena_strndup(context->source.arena, name, (size_t)(dot - name)) : name;
    if (!first || cbm_ht_get(context->local_types, first))
        return NULL;
    for (csp_scope_t *s = scope; s; s = s->parent) {
        const csp_name_t *alias = cbm_ht_get(s->aliases, first);
        if (alias) {
            if (alias->ambiguous || !alias->target)
                return NULL;
            const char *full =
                dot ? csp_join(&context->source, alias->target, dot + 1) : alias->target;
            bool complete = true;
            /* Relative alias targets can bind an enclosing namespace first.
             * Reject that unmodelled case rather than assuming global::. */
            if (s->namespace && s->namespace[0])
                return NULL;
            const CBMRegisteredType *result =
                full ? csp_qualified_type(context, full, &complete) : NULL;
            return complete ? result : NULL;
        }
    }
    bool complete = true;
    if (dot) {
        /* Fully qualified type spellings inside namespaces need prefix lookup;
         * only global-namespace occurrences are admitted here. */
        if (scope->namespace && scope->namespace[0])
            return NULL;
        const CBMRegisteredType *result = csp_qualified_type(context, name, &complete);
        return complete ? result : NULL;
    }
    for (csp_scope_t *s = scope; s; s = s->parent) {
        const CBMRegisteredType *chosen =
            csp_namespace_type(context, s->namespace, name, &complete);
        if (!complete)
            return NULL;
        if (chosen)
            return chosen;
        for (size_t i = 0; i < s->count; i++) {
            const CBMRegisteredType *candidate =
                csp_namespace_type(context, s->namespaces[i], name, &complete);
            if (!complete || (chosen && candidate && !csp_same_type(chosen, candidate)))
                return NULL;
            if (candidate)
                chosen = candidate;
        }
        if (chosen)
            return chosen;
    }
    return NULL;
}
static cbm_resolve_result_t csp_type_evidence(csp_context_t *context, const CBMRegisteredType *type,
                                              TSNode site) {
    if (!type)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED};
    return cbm_lsp_proof_type_lookup(
        &context->types, csp_text(&context->source, site), site, "cs_proof-type-name-domain",
        type->qualified_name, type->declaration_identity,
        type->graph_qualified_name ? type->graph_qualified_name : type->qualified_name,
        type->source_flags, "cs_proof-complete-source-type-lookup");
}
static const char *csp_occurrence_spelling(csp_context_t *context, TSNode site,
                                           CBMResolvedKind kind) {
    TSNode expression = site;
    if (kind == CBM_RESOLVED_INVOCATION)
        expression = csp_field(site, "function");
    if (csp_is(expression, "member_access_expression")) {
        const char *receiver = csp_text(&context->source, csp_field(expression, "expression"));
        const char *member = csp_text(&context->source, csp_field(expression, "name"));
        return receiver && member
                   ? cbm_arena_sprintf(context->source.arena, "%s.%s", receiver, member)
                   : NULL;
    }
    return csp_text(&context->source, expression);
}
static void csp_emit(csp_context_t *context, const char *caller, TSNode site, CBMResolvedKind kind,
                     cbm_resolve_result_t proof) {
    if (!caller || !cbm_resolve_admit(&proof))
        return;
    const char *relation = kind == CBM_RESOLVED_INVOCATION       ? "CALLS"
                           : kind == CBM_RESOLVED_CALL_REFERENCE ? "CALL_REFERENCE"
                           : kind == CBM_RESOLVED_TYPE_REFERENCE ? "USES_TYPE"
                                                                 : "USAGE";
    const char *spelling = csp_occurrence_spelling(context, site, kind);
    proof = cbm_lsp_proof_occurrence(&context->types, proof, site, caller, spelling, relation);
    if (!cbm_resolve_admit(&proof))
        return;
    CBMResolvedCall row = {.caller_qn = caller,
                           .callee_qn = proof.target,
                           .strategy = "csharp_source_binding",
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
static cbm_resolve_method_candidate_t csp_candidate(const cbm_resolve_method_query_t *query,
                                                    const CBMTypeRegistry *owner,
                                                    const CBMRegisteredFunc *candidate,
                                                    void *opaque) {
    (void)query;
    (void)owner;
    csp_context_t *context = opaque;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    uint32_t required =
        CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_FIXED_ARITY;
    bool is_static = (candidate->source_flags & CBM_SOURCE_DECL_STATIC) != 0;
    if ((candidate->source_flags & required) != required || is_static != context->static_call ||
        candidate->min_params != 0 || candidate->type_param_names ||
        !(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE))
        return proof;
    proof.applicable = CBM_RESOLVE_METHOD_YES;
    proof.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    proof.graph_target = candidate->graph_qualified_name;
    proof.graph_target_exact = proof.graph_target != NULL;
    proof.declaration_identity = candidate->declaration_identity;
    proof.declaration_identity_exact = proof.declaration_identity != NULL;
    return proof;
}
static bool csp_read_syntax(const char *kind) {
    static const char *const kinds[] = {"block",
                                        "expression_statement",
                                        "return_statement",
                                        "invocation_expression",
                                        "member_access_expression",
                                        "identifier",
                                        "argument_list",
                                        "argument",
                                        "integer_literal",
                                        "real_literal",
                                        "string_literal",
                                        "string_literal_content",
                                        "boolean_literal",
                                        "null_literal",
                                        "comment",
                                        "if_statement",
                                        "binary_expression",
                                        "parenthesized_expression",
                                        "assignment_expression",
                                        "local_declaration_statement",
                                        "variable_declaration",
                                        "variable_declarator",
                                        "implicit_type",
                                        "predefined_type",
                                        "qualified_name",
                                        NULL};
    for (size_t i = 0; kinds[i]; i++)
        if (!strcmp(kinds[i], kind))
            return true;
    return false;
}
static void csp_audit(csp_context_t *context, TSNode node, CBMHashTable *blocked, bool binding,
                      unsigned depth) {
    if (ts_node_is_null(node) || !ts_node_is_named(node) || !csp_budget(&context->source, depth))
        return;
    if (!csp_read_syntax(ts_node_type(node))) {
        context->source.failed = true;
        return;
    }
    binding = binding || csp_is(node, "assignment_expression") ||
              csp_is(node, "local_declaration_statement") || csp_is(node, "argument");
    if (binding && csp_is(node, "identifier")) {
        const char *name = csp_text(&context->source, node);
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
            csp_audit(context, ts_tree_cursor_current_node(&cursor), blocked, binding, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void csp_call(csp_context_t *context, csp_scope_t *names, TSNode node, const char *caller,
                     CBMScope *scope, CBMHashTable *bound, CBMHashTable *blocked) {
    TSNode function = csp_field(node, "function"), arguments = csp_field(node, "arguments");
    if (!csp_is(function, "member_access_expression") || ts_node_is_null(arguments) ||
        ts_node_named_child_count(arguments))
        return;
    TSNode receiver = csp_field(function, "expression"), member = csp_field(function, "name");
    if (!csp_is(receiver, "identifier") || !csp_is(member, "identifier"))
        return;
    const char *variable = csp_text(&context->source, receiver),
               *name = csp_text(&context->source, member);
    if (!variable || !name || cbm_ht_get(blocked, variable))
        return;
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&context->types, scope, variable, ts_node_start_byte(node));
    const CBMRegisteredType *static_type = NULL;
    context->static_call = false;
    if (fact.status != CBM_RESOLVE_TYPE_KNOWN) {
        if (cbm_ht_get(bound, variable))
            return;
        static_type = csp_type(context, names, variable);
        if (!static_type)
            return;
        cbm_resolve_result_t evidence = csp_type_evidence(context, static_type, receiver);
        fact = (cbm_resolve_type_fact_t){
            .status = CBM_RESOLVE_TYPE_KNOWN,
            .origin = CBM_RESOLVE_TYPE_ANNOTATION,
            .proof_pool = &context->types.proofs,
            .type = cbm_type_named(context->source.arena, static_type->qualified_name),
            .evidence = evidence};
        context->static_call = true;
    }
    if (!fact.type || fact.type->kind != CBM_TYPE_NAMED)
        return;
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
                                        .classify = csp_candidate,
                                        .context = context};
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    csp_emit(context, caller, node, CBM_RESOLVED_INVOCATION, result.result);
    if (static_type)
        csp_emit(context, caller, receiver, CBM_RESOLVED_TYPE_REFERENCE,
                 csp_type_evidence(context, static_type, receiver));
}
static void csp_calls(csp_context_t *context, csp_scope_t *names, TSNode node, const char *caller,
                      CBMScope *scope, CBMHashTable *bound, CBMHashTable *blocked, unsigned depth) {
    if (!csp_budget(&context->source, depth))
        return;
    if (csp_is(node, "invocation_expression"))
        csp_call(context, names, node, caller, scope, bound, blocked);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            csp_calls(context, names, ts_tree_cursor_current_node(&cursor), caller, scope, bound,
                      blocked, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void csp_function(csp_context_t *context, csp_scope_t *names, TSNode node) {
    if (csp_child_kind(node, "type_parameter_list"))
        return;
    const char *name = csp_text(&context->source, csp_field(node, "name"));
    const char *key = name ? cbm_arena_sprintf(context->source.arena, "%u:%s",
                                               ts_node_start_point(node).row + 1, name)
                           : NULL;
    const csp_name_t *caller = key ? cbm_ht_get(context->callers, key) : NULL;
    TSNode body = csp_field(node, "body"), parameters = csp_field(node, "parameters");
    if (!caller || caller->ambiguous || !caller->target || ts_node_is_null(body) ||
        ts_node_is_null(parameters))
        return;
    CBMHashTable *bound = cbm_ht_create(16), *blocked = cbm_ht_create(16);
    if (!bound || !blocked) {
        cbm_ht_free(bound);
        cbm_ht_free(blocked);
        return;
    }
    csp_audit(context, body, blocked, false, 0);
    bool failed = context->source.failed;
    context->source.failed = false;
    CBMScope *scope = cbm_scope_push(context->source.arena, NULL);
    int first = context->out->count;
    if (!failed && scope) {
        TSTreeCursor cursor = ts_tree_cursor_new(parameters);
        if (ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode parameter = ts_tree_cursor_current_node(&cursor);
                if (!csp_is(parameter, "parameter"))
                    continue;
                TSNode type_node = csp_field(parameter, "type"),
                       name_node = csp_field(parameter, "name");
                const char *variable = csp_text(&context->source, name_node);
                if (!variable || cbm_ht_get(bound, variable)) {
                    failed = true;
                    break;
                }
                cbm_ht_set(bound, variable, (void *)1);
                if (!cbm_ht_get(bound, variable)) {
                    failed = true;
                    break;
                }
                /* ref/out/in/attributes/defaults need a separate binding model. */
                if (ts_node_named_child_count(parameter) != 2 || cbm_ht_get(blocked, variable))
                    continue;
                if (!csp_is(type_node, "identifier") && !csp_is(type_node, "qualified_name"))
                    continue;
                const CBMRegisteredType *type =
                    csp_type(context, names, csp_text(&context->source, type_node));
                if (!type)
                    continue;
                cbm_resolve_result_t proof = csp_type_evidence(context, type, type_node);
                if (cbm_resolve_types_declare(
                        &context->types, scope, variable, ts_node_start_byte(parameter),
                        cbm_type_named(context->source.arena, type->qualified_name),
                        CBM_RESOLVE_TYPE_ANNOTATION, &proof))
                    csp_emit(context, caller->target, type_node, CBM_RESOLVED_TYPE_REFERENCE,
                             proof);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
        if (!failed)
            csp_calls(context, names, body, caller->target, scope, bound, blocked, 0);
    }
    if (failed)
        context->out->count = first;
    cbm_ht_free(bound);
    cbm_ht_free(blocked);
}
static void csp_walk(csp_context_t *context, TSNode node, csp_scope_t *scope, unsigned depth) {
    if (!csp_budget(&context->source, depth))
        return;
    /* Imports bind throughout their containing namespace, independent of
     * declaration order. Build that scope before resolving method bodies. */
    csp_scope_t *file_scope = NULL, *import_scope = scope;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (csp_is(child, "using_directive"))
                csp_using(context, import_scope, child);
            else if (csp_is(child, "file_scoped_namespace_declaration")) {
                const char *ns = csp_join(&context->source, scope->namespace,
                                          csp_text(&context->source, csp_field(child, "name")));
                if (file_scope || !ns || !(file_scope = csp_scope(context, scope, ns))) {
                    context->source.failed = true;
                    break;
                }
                import_scope = file_scope;
            } else if (strncmp(ts_node_type(child), "preproc_", 8) == 0)
                scope->unsupported = true;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_reset(&cursor, node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (csp_is(child, "namespace_declaration")) {
                const char *ns = csp_join(&context->source, scope->namespace,
                                          csp_text(&context->source, csp_field(child, "name")));
                csp_scope_t *nested = ns ? csp_scope(context, scope, ns) : NULL;
                TSNode body = csp_field(child, "body");
                if (nested && !ts_node_is_null(body))
                    csp_walk(context, body, nested, depth + 1);
            } else if (csp_is(child, "file_scoped_namespace_declaration")) {
                if (!file_scope) {
                    context->source.failed = true;
                    break;
                }
                scope = file_scope;
            } else if (csp_is(child, "class_declaration") ||
                       csp_is(child, "interface_declaration") ||
                       csp_is(child, "struct_declaration")) {
                if (csp_simple_class(&context->source, child))
                    csp_walk(context, csp_field(child, "body"), scope, depth + 1);
            } else if (csp_is(child, "method_declaration"))
                csp_function(context, scope, child);
        } while (!context->source.failed && ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
void cbm_cs_proof_resolve(CBMArena *arena, const char *source, int length, TSNode root,
                          const CBMTypeRegistry *registry, const CBMFileResult *file,
                          CBMResolvedCallArray *out) {
    if (!arena || !source || length <= 0 || !registry || !file || !out || ts_node_is_null(root) ||
        ts_node_has_error(root) || file->has_error || file->parse_incomplete ||
        file->parse_unusable)
        return;
    csp_context_t context = {.source = {.arena = arena, .source = source, .length = (size_t)length},
                             .registry = registry,
                             .file = file,
                             .out = out};
    context.callers = cbm_ht_create(32);
    context.local_types = cbm_ht_create(32);
    cbm_resolve_types_init(&context.types, arena, 0);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, file->module_qn);
    if (!cbm_lsp_proof_context(&context.types, &proof_source, file->module_qn))
        context.source.failed = true;
    int first = out->count;
    csp_scope_t *scope = csp_scope(&context, NULL, "");
    if (context.callers && context.local_types && scope) {
        for (int i = 0; i < file->defs.count; i++) {
            const CBMDefinition *def = &file->defs.items[i];
            if (!def->name || !def->qualified_name || !def->label)
                continue;
            if (!strcmp(def->label, "Method") || !strcmp(def->label, "Function")) {
                const char *key = cbm_arena_sprintf(arena, "%u:%s", def->start_line, def->name);
                if (!key || !csp_put(&context, context.callers, key, def->qualified_name)) {
                    context.source.failed = true;
                    break;
                }
            } else if (cbm_label_is_type_like(def->label)) {
                if (!csp_put(&context, context.local_types, def->name, def->qualified_name)) {
                    context.source.failed = true;
                    break;
                }
            }
        }
        if (!context.source.failed)
            csp_walk(&context, root, scope, 0);
    }
    if (context.source.failed)
        out->count = first;
    for (csp_scope_t *s = context.allocated; s; s = s->allocated_next)
        cbm_ht_free(s->aliases);
    cbm_ht_free(context.callers);
    cbm_ht_free(context.local_types);
}
