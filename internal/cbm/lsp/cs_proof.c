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
    const CBMType *signature;
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
/* Predefined numeric/boolean spellings are lexical type identities. Named,
 * nullable, array, generic and by-reference parameters remain unsupported. */
static const CBMType *csp_primitive(cs_proof_source_t *source, TSNode node) {
    if (!csp_is(node, "predefined_type"))
        return NULL;
    const char *name = csp_text(source, node);
    static const char *const names[] = {"bool",  "sbyte",  "byte",    "short", "ushort",
                                        "int",   "uint",   "long",    "ulong", "char",
                                        "float", "double", "decimal", "void"};
    for (size_t i = 0; name && i < sizeof(names) / sizeof(names[0]); i++)
        if (!strcmp(name, names[i]))
            return cbm_type_builtin(source->arena, name);
    return NULL;
}

static const CBMType *csp_signature(cs_proof_source_t *source, TSNode method) {
    TSNode parameters = csp_field(method, "parameters");
    if (ts_node_is_null(parameters))
        return NULL;
    const CBMType *types[65] = {0};
    size_t count = 0;
    uint32_t children = ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < children; i++) {
        TSNode parameter = ts_node_named_child(parameters, i);
        if (csp_is(parameter, "comment"))
            continue;
        if (!csp_is(parameter, "parameter") || ts_node_named_child_count(parameter) != 2 ||
            count == 64)
            return NULL;
        /* ref/out/in/params are tokens in this grammar, not necessarily named
         * children; reject every non-type/name token explicitly. */
        uint32_t parts = ts_node_child_count(parameter);
        for (uint32_t j = 0; j < parts; j++) {
            TSNode part = ts_node_child(parameter, j);
            if (!ts_node_eq(part, csp_field(parameter, "type")) &&
                !ts_node_eq(part, csp_field(parameter, "name")))
                return NULL;
        }
        const CBMType *type = csp_primitive(source, csp_field(parameter, "type"));
        if (!type || !strcmp(type->data.builtin.name, "void"))
            return NULL;
        types[count++] = type;
    }
    const CBMType **params = cbm_arena_alloc(source->arena, (count + 1) * sizeof(*params));
    if (!params)
        return NULL;
    memcpy(params, types, (count + 1) * sizeof(*params));
    return cbm_type_func(source->arena, NULL, params, NULL);
}
static void csp_add(cs_proof_source_t *source, const char *qn, const char *namespace, TSNode node,
                    uint32_t flags, int arity, const CBMType *signature) {
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
    source->declarations[source->count++] = (cs_source_decl_t){
        qn, namespace, ts_node_start_point(node).row + 1, flags, arity, signature};
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
    csp_add(source, qn, namespace, node, flags, -1, NULL);
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
                    csp_arity(child), csp_signature(source, child));
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
    for (int i = 0; i < count; i++) {
        defs[i].source_flags = 0;
        defs[i].source_signature = NULL;
    }
    if (!arena || !source || length <= 0 || !module)
        return false;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_c_sharp(void);
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_c_sharp());
    TSTree *tree = cbm_parse_plain(parser, source, (uint32_t)length);
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
        size_t base_length = cbm_qn_callable_base_len_named(qn, def->short_name);
        const char *base = qn[base_length] ? cbm_arena_strndup(arena, qn, base_length) : qn;
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
        if (found->arity >= 0 && found->arity == def->signature_param_count) {
            def->source_flags |= CBM_SOURCE_DECL_FIXED_ARITY;
            def->source_signature = found->signature;
        }
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
    const CBMRegisteredType *enclosing_type;
    bool static_context, static_call, implicit_call, same_class;
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
/* `this` and implicit receivers bind to the exact lexical class, even when
 * that class is internal or its method private. Namespace equality alone never
 * grants cross-file internal/assembly accessibility. */
static const CBMRegisteredType *csp_lexical_type(csp_context_t *context, csp_scope_t *scope,
                                                 TSNode node) {
    const char *name = csp_text(&context->source, csp_field(node, "name"));
    const char *qn = csp_join(&context->source, context->file->module_qn, name);
    if (!name || !qn)
        return NULL;
    CBMNamespaceTypeIter iterator;
    cbm_registry_types_in_namespace(context->registry, scope->namespace, name, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    while ((candidate = cbm_namespace_type_next(&iterator, NULL))) {
        if (!candidate->project_source || !candidate->graph_qualified_name ||
            strcmp(candidate->graph_qualified_name, qn) || !candidate->declaration_identity ||
            (candidate->source_flags & required) != required ||
            (chosen && !csp_same_type(chosen, candidate)))
            return NULL;
        chosen = candidate;
    }
    return iterator.complete ? chosen : NULL;
}

static cbm_resolve_result_t csp_primitive_evidence(csp_context_t *context, const CBMType *type,
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
    cbm_resolve_proof_key_t key = {.lookup_domain = "csharp-predefined-type-syntax",
                                   .lookup_key = csp_text(&context->source, site),
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
    uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_FIXED_ARITY;
    bool is_static = (candidate->source_flags & CBM_SOURCE_DECL_STATIC) != 0;
    bool accessible = context->same_class || (candidate->source_flags & CBM_SOURCE_DECL_PUBLIC);
    bool receiver_valid = context->implicit_call && !context->static_context
                              ? true
                              : is_static == context->static_call;
    if ((candidate->source_flags & required) != required || !accessible || !receiver_valid ||
        candidate->min_params < 0 || candidate->type_param_names ||
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
                                        "this",
                                        "this_expression",
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
              csp_is(node, "local_declaration_statement");
    if (csp_is(node, "argument")) {
        uint32_t children = ts_node_child_count(node);
        for (uint32_t i = 0; i < children; i++) {
            TSNode child = ts_node_child(node, i);
            if (csp_is(child, "ref") || csp_is(child, "out") || csp_is(child, "in"))
                binding = true;
        }
    }
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
static const CBMType *csp_argument_type(csp_context_t *context, CBMScope *scope,
                                        CBMHashTable *blocked, TSNode node,
                                        cbm_resolve_result_t *proof, unsigned depth) {
    if (ts_node_is_null(node) || depth > 32)
        return NULL;
    if (csp_is(node, "identifier")) {
        const char *name = csp_text(&context->source, node);
        if (!name || cbm_ht_get(blocked, name))
            return NULL;
        cbm_resolve_type_fact_t fact =
            cbm_resolve_types_lookup(&context->types, scope, name, ts_node_start_byte(node));
        if (cbm_resolve_types_fact_status(&fact) != CBM_RESOLVE_TYPE_KNOWN || !fact.type ||
            fact.type->kind != CBM_TYPE_BUILTIN)
            return NULL;
        *proof = fact.evidence;
        return fact.type;
    }
    if (csp_is(node, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return csp_argument_type(context, scope, blocked, ts_node_named_child(node, 0), proof,
                                 depth + 1);
    const char *type_name = NULL, *text = csp_text(&context->source, node);
    if (csp_is(node, "boolean_literal") && text &&
        (!strcmp(text, "true") || !strcmp(text, "false")))
        type_name = "bool";
    else if (csp_is(node, "integer_literal") && text && *text) {
        /* Decimal int/long literals only. Hex/binary/unsigned and boundary
         * negation need their own literal typing rule; never guess int. */
        uint64_t value = 0;
        bool digit = false;
        const unsigned char *at = (const unsigned char *)text;
        for (; *at && *at != 'l' && *at != 'L'; at++) {
            if (*at == '_')
                continue;
            if (*at < '0' || *at > '9' ||
                value > (UINT64_C(9223372036854775807) - (*at - '0')) / 10)
                return NULL;
            value = value * 10 + (*at - '0');
            digit = true;
        }
        bool is_long = *at == 'l' || *at == 'L';
        if (!digit || (is_long ? at[1] != 0 : *at != 0) ||
            (!is_long && value > UINT64_C(2147483647)))
            return NULL;
        type_name = is_long ? "long" : "int";
    }
    if (!type_name)
        return NULL;
    const CBMType *type = cbm_type_builtin(context->source.arena, type_name);
    *proof = csp_primitive_evidence(context, type, node, "csharp-source-primitive-literal");
    return type;
}

static bool csp_primitive_conversion(const CBMType *from, const CBMType *to) {
    if (!from || !to || from->kind != CBM_TYPE_BUILTIN || to->kind != CBM_TYPE_BUILTIN)
        return false;
    const char *a = from->data.builtin.name, *b = to->data.builtin.name;
    if (!a || !b || !strcmp(a, "void") || !strcmp(b, "void"))
        return false;
    if (!strcmp(a, b))
        return true;
    /* C# implicit numeric conversions only; no constant narrowing, boxing,
     * user conversions, dynamic binding or overload ranking. */
    static const struct {
        const char *from, *to;
    } conversions[] = {{"sbyte", "short,int,long,float,double,decimal"},
                       {"byte", "short,ushort,int,uint,long,ulong,float,double,decimal"},
                       {"short", "int,long,float,double,decimal"},
                       {"ushort", "int,uint,long,ulong,float,double,decimal"},
                       {"int", "long,float,double,decimal"},
                       {"uint", "long,ulong,float,double,decimal"},
                       {"long", "float,double,decimal"},
                       {"ulong", "float,double,decimal"},
                       {"char", "ushort,int,uint,long,ulong,float,double,decimal"},
                       {"float", "double"}};
    for (size_t i = 0; i < sizeof(conversions) / sizeof(conversions[0]); i++) {
        if (strcmp(a, conversions[i].from))
            continue;
        const char *at = conversions[i].to;
        while (*at) {
            const char *comma = strchr(at, ',');
            size_t length = comma ? (size_t)(comma - at) : strlen(at);
            if (strlen(b) == length && !memcmp(at, b, length))
                return true;
            at += length;
            if (*at)
                at++;
        }
    }
    return false;
}

static cbm_resolve_result_t csp_arguments(csp_context_t *context, CBMScope *scope,
                                          CBMHashTable *blocked, TSNode arguments,
                                          cbm_resolve_method_result_t selected) {
    cbm_resolve_result_t result = selected.result;
    if (!cbm_resolve_admit(&result) || !selected.method)
        return result;
    const CBMType *signature = selected.method->source_signature;
    const CBMType *const *parameters =
        signature && signature->kind == CBM_TYPE_FUNC ? signature->data.func.param_types : NULL;
    int count = 0;
    uint32_t children = ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < children; i++) {
        TSNode argument = ts_node_named_child(arguments, i);
        if (csp_is(argument, "comment"))
            continue;
        if (!csp_is(argument, "argument") || ts_node_child_count(argument) != 1 || count >= 64 ||
            !parameters || !parameters[count])
            goto unsupported;
        cbm_resolve_result_t evidence = {0};
        const CBMType *type =
            csp_argument_type(context, scope, blocked, ts_node_child(argument, 0), &evidence, 0);
        if (!csp_primitive_conversion(type, parameters[count]) || !cbm_resolve_admit(&evidence) ||
            !evidence.proof)
            goto unsupported;
        result =
            cbm_lsp_proof_rule(&context->types, result, argument, selected.method->qualified_name,
                               selected.method->short_name,
                               "csharp-source-primitive-invocation-conversion", evidence.proof);
        if (!cbm_resolve_admit(&result))
            return result;
        count++;
    }
    if (count != selected.method->min_params || (count && parameters[count]))
        goto unsupported;
    return result;
unsupported:
    return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED,
                                  .reason = "csharp_argument_applicability_unproven"};
}

static void csp_call(csp_context_t *context, csp_scope_t *names, TSNode node, const char *caller,
                     CBMScope *scope, CBMHashTable *bound, CBMHashTable *blocked) {
    TSNode function = csp_field(node, "function"), arguments = csp_field(node, "arguments");
    if (ts_node_is_null(arguments))
        return;
    bool implicit = csp_is(function, "identifier");
    TSNode receiver = implicit ? (TSNode){0} : csp_field(function, "expression");
    TSNode member = implicit ? function : csp_field(function, "name");
    bool self = csp_is(receiver, "this") || csp_is(receiver, "this_expression");
    if ((!implicit && !csp_is(function, "member_access_expression")) ||
        (!implicit && !self && !csp_is(receiver, "identifier")) || !csp_is(member, "identifier"))
        return;
    const char *name = csp_text(&context->source, member);
    if (!name)
        return;
    static const char *const object_names[] = {"Equals",   "GetHashCode",     "GetType",
                                               "ToString", "ReferenceEquals", "MemberwiseClone",
                                               "Finalize"};
    for (size_t i = 0; i < sizeof(object_names) / sizeof(object_names[0]); i++)
        if (!strcmp(name, object_names[i]))
            return;
    cbm_resolve_type_fact_t fact = {0};
    const CBMRegisteredType *static_type = NULL;
    context->implicit_call = implicit;
    context->static_call = false;
    if (implicit || self) {
        const CBMRegisteredType *type = context->enclosing_type;
        if (!type || (self && context->static_context) ||
            (implicit && (cbm_ht_get(bound, name) || cbm_ht_get(blocked, name))))
            return;
        context->static_call = context->static_context;
        fact = (cbm_resolve_type_fact_t){
            .status = CBM_RESOLVE_TYPE_KNOWN,
            .origin = CBM_RESOLVE_TYPE_ANNOTATION,
            .proof_pool = &context->types.proofs,
            .type = cbm_type_named(context->source.arena, type->qualified_name),
            .evidence = cbm_lsp_proof_type_lookup(
                &context->types, implicit ? name : "this", implicit ? member : receiver,
                "csharp-lexical-enclosing-class", type->qualified_name, type->declaration_identity,
                type->graph_qualified_name, type->source_flags, "csharp-source-lexical-receiver")};
    } else {
        const char *variable = csp_text(&context->source, receiver);
        if (!variable || cbm_ht_get(blocked, variable))
            return;
        fact = cbm_resolve_types_lookup(&context->types, scope, variable, ts_node_start_byte(node));
        if (fact.status != CBM_RESOLVE_TYPE_KNOWN) {
            if (cbm_ht_get(bound, variable))
                return;
            static_type = csp_type(context, names, variable);
            if (!static_type)
                return;
            fact = (cbm_resolve_type_fact_t){
                .status = CBM_RESOLVE_TYPE_KNOWN,
                .origin = CBM_RESOLVE_TYPE_ANNOTATION,
                .proof_pool = &context->types.proofs,
                .type = cbm_type_named(context->source.arena, static_type->qualified_name),
                .evidence = csp_type_evidence(context, static_type, receiver)};
            context->static_call = true;
        }
    }
    if (cbm_resolve_types_fact_status(&fact) != CBM_RESOLVE_TYPE_KNOWN || !fact.type ||
        fact.type->kind != CBM_TYPE_NAMED)
        return;
    context->same_class =
        context->enclosing_type &&
        !strcmp(context->enclosing_type->qualified_name, fact.type->data.named.qualified_name);
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
                                        .classify = csp_candidate,
                                        .context = context};
    cbm_resolve_method_result_t selected = cbm_resolve_method(&query);
    cbm_resolve_result_t result = csp_arguments(context, scope, blocked, arguments, selected);
    csp_emit(context, caller, node, CBM_RESOLVED_INVOCATION, result);
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
    context->static_context = csp_modifier(&context->source, node, "static");
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
                const CBMType *primitive = csp_primitive(&context->source, type_node);
                if (primitive && strcmp(primitive->data.builtin.name, "void")) {
                    /* Parameter modifiers are tokens; a by-reference parameter
                     * must not lend a stable local value fact. */
                    if (ts_node_child_count(parameter) != 2)
                        continue;
                    cbm_resolve_result_t proof = csp_primitive_evidence(
                        context, primitive, type_node, "csharp-source-primitive-annotation");
                    (void)cbm_resolve_types_declare(&context->types, scope, variable,
                                                    ts_node_start_byte(parameter), primitive,
                                                    CBM_RESOLVE_TYPE_ANNOTATION, &proof);
                    continue;
                }
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
                if (csp_simple_class(&context->source, child)) {
                    const CBMRegisteredType *previous = context->enclosing_type;
                    context->enclosing_type = csp_lexical_type(context, scope, child);
                    csp_walk(context, csp_field(child, "body"), scope, depth + 1);
                    context->enclosing_type = previous;
                }
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
    /* Append through a private descriptor; publish only after complete analysis.
     * Arena growth preserves the original buffer and its committed prefix. */
    CBMResolvedCallArray staged = *out;
    csp_context_t context = {.source = {.arena = arena, .source = source, .length = (size_t)length},
                             .registry = registry,
                             .file = file,
                             .out = &staged};
    context.callers = cbm_ht_create(32);
    context.local_types = cbm_ht_create(32);
    cbm_resolve_types_init(&context.types, arena, 0);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, file->module_qn);
    if (!cbm_lsp_proof_context(&context.types, &proof_source, file->module_qn))
        context.source.failed = true;
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
    if (!context.source.failed)
        *out = staged;
    for (csp_scope_t *s = context.allocated; s; s = s->allocated_next)
        cbm_ht_free(s->aliases);
    cbm_ht_free(context.callers);
    cbm_ht_free(context.local_types);
}
