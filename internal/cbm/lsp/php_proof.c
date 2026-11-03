#include "php_proof.h"
#include "../callable_sig.h"
#include "../resolve/policy.h"
#include "../resolve/symbols.h"
#include "foundation/hash_table.h"
#include "proof_bridge.h"
#include <ctype.h>
#include <stdlib.h>
#include <string.h>

/* Exact declaration facts and occurrence facts share source coordinates, but
 * have independent lifetimes: declarations outlive per-file resolver arenas. */
typedef struct {
    const char *qn;
    uint32_t line, flags;
    int arity;
} php_declaration_t;

typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length;
    php_declaration_t *items;
    size_t count, capacity, visits;
    bool failed;
} php_source_t;

static bool php_proof_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}
static TSNode php_proof_field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}
static const char *php_proof_text(php_source_t *source, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= source->length
               ? cbm_arena_strndup(source->arena, source->source + start, end - start)
               : NULL;
}
static bool php_proof_budget(php_source_t *source, unsigned depth) {
    if (depth > 128)
        source->failed = true;
    return !source->failed;
}
static uint32_t php_proof_modifiers(php_source_t *source, TSNode node) {
    uint32_t flags = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (php_proof_is(child, "visibility_modifier")) {
                const char *visibility = php_proof_text(source, child);
                if (visibility && !strcmp(visibility, "private"))
                    flags = (flags & ~CBM_SOURCE_DECL_PUBLIC) | CBM_SOURCE_DECL_PRIVATE;
                else if (visibility && !strcmp(visibility, "protected"))
                    flags = (flags & ~CBM_SOURCE_DECL_PUBLIC) | CBM_SOURCE_DECL_PROTECTED;
            } else if (php_proof_is(child, "static_modifier"))
                flags |= CBM_SOURCE_DECL_STATIC;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return flags;
}
static int php_proof_arity(php_source_t *source, TSNode node, bool *untyped_value) {
    *untyped_value = true;
    TSNode parameters = php_proof_field(node, "parameters");
    if (ts_node_is_null(parameters))
        return -1;
    CBMHashTable *names = cbm_ht_create(16);
    if (!names) {
        source->failed = true;
        return -1;
    }
    int count = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(parameters);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child) || php_proof_is(child, "comment"))
                continue;
            if (!php_proof_is(child, "simple_parameter") ||
                !ts_node_is_null(php_proof_field(child, "default_value"))) {
                count = -1;
                break;
            }
            TSNode variable = php_proof_field(child, "name");
            const char *name = php_proof_text(source, variable);
            if (!php_proof_is(variable, "variable_name") || !name || !strcmp(name, "$this") ||
                cbm_ht_get(names, name)) {
                count = -1;
                break;
            }
            cbm_ht_set(names, name, (void *)1);
            if (!cbm_ht_get(names, name)) {
                source->failed = true;
                count = -1;
                break;
            }
            if (!ts_node_is_null(php_proof_field(child, "type")))
                *untyped_value = false;
            uint32_t children = ts_node_named_child_count(child);
            for (uint32_t i = 0; i < children; i++) {
                TSNode part = ts_node_named_child(child, i);
                if (!php_proof_is(part, "variable_name") && !php_proof_is(part, "comment"))
                    *untyped_value = false;
            }
            if (++count > 64) {
                count = -1;
                break;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    cbm_ht_free(names);
    return count;
}
static void php_proof_append(php_source_t *source, const char *qn, TSNode node, uint32_t flags,
                             int arity) {
    if (!qn) {
        source->failed = true;
        return;
    }
    if (source->count == source->capacity) {
        size_t capacity = source->capacity ? source->capacity * 2 : 32;
        php_declaration_t *items = cbm_arena_alloc(source->arena, capacity * sizeof(*items));
        if (!items) {
            source->failed = true;
            return;
        }
        if (source->count)
            memcpy(items, source->items, source->count * sizeof(*items));
        source->items = items;
        source->capacity = capacity;
    }
    source->items[source->count++] =
        (php_declaration_t){qn, ts_node_start_point(node).row + 1, flags, arity};
}
static const char *php_proof_lower(php_source_t *source, TSNode node) {
    char *name = (char *)php_proof_text(source, node);
    if (name)
        for (char *p = name; *p; p++)
            *p = (char)tolower((unsigned char)*p);
    return name;
}
static void php_proof_class(php_source_t *source, TSNode node, const char *module) {
    const char *name = php_proof_text(source, php_proof_field(node, "name"));
    const char *qn = name ? cbm_arena_sprintf(source->arena, "%s.%s", module, name) : NULL;
    TSNode body = php_proof_field(node, "body");
    if (!qn || ts_node_is_null(body)) {
        source->failed = true;
        return;
    }
    CBMHashTable *methods = cbm_ht_create(16);
    if (!methods) {
        source->failed = true;
        return;
    }
    bool complete = true;
    size_t first = source->count;
    php_proof_append(source, qn, node,
                     CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_TOP_LEVEL,
                     -1);
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (php_proof_is(child, "method_declaration")) {
                TSNode method_name = php_proof_field(child, "name");
                const char *lower = php_proof_lower(source, method_name);
                if (!lower || cbm_ht_get(methods, lower)) {
                    complete = false;
                    continue;
                }
                cbm_ht_set(methods, lower, (void *)1);
                if (!cbm_ht_get(methods, lower)) {
                    source->failed = true;
                    break;
                }
                const char *method = php_proof_text(source, method_name);
                const char *target =
                    method ? cbm_arena_sprintf(source->arena, "%s.%s", qn, method) : NULL;
                bool untyped_value = false;
                int arity = php_proof_arity(source, child, &untyped_value);
                uint32_t flags = php_proof_modifiers(source, child);
                if (arity >= 0 && untyped_value)
                    flags |= CBM_SOURCE_DECL_UNTYPED_VALUE_PARAMETERS;
                php_proof_append(source, target, child, flags, arity);
            } else if (ts_node_is_named(child) && !php_proof_is(child, "comment") &&
                       !php_proof_is(child, "property_declaration") &&
                       !php_proof_is(child, "const_declaration"))
                complete = false;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    cbm_ht_free(methods);
    if (complete && first < source->count)
        source->items[first].flags |= CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    else
        for (size_t i = first; i < source->count; i++)
            source->items[i].flags = 0;
}
static void php_proof_declarations(php_source_t *source, TSNode node, const char *module,
                                   unsigned depth) {
    if (!php_proof_budget(source, depth))
        return;
    if (php_proof_is(node, "class_declaration") || php_proof_is(node, "interface_declaration")) {
        php_proof_class(source, node, module);
        return;
    }
    if (!php_proof_is(node, "program") && !php_proof_is(node, "namespace_definition") &&
        !php_proof_is(node, "compound_statement"))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            php_proof_declarations(source, ts_tree_cursor_current_node(&cursor), module, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static int php_proof_compare(const void *left, const void *right) {
    const php_declaration_t *a = left, *b = right;
    int comparison = strcmp(a->qn, b->qn);
    return comparison ? comparison : (a->line > b->line) - (a->line < b->line);
}
bool cbm_php_collect_source_facts(CBMArena *arena, const char *source, int source_len,
                                  const char *module_qn, CBMLSPDef *defs, int count) {
    if (count < 0 || (count && !defs))
        return false;
    for (int i = 0; i < count; i++)
        defs[i].source_flags = 0;
    if (!arena || !source || source_len <= 0 || !module_qn)
        return false;
    TSParser *parser = ts_parser_new();
    extern const TSLanguage *tree_sitter_php_only(void);
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_php_only());
    TSTree *tree = cbm_parse_plain(parser, source, (uint32_t)source_len);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    TSNode root = ts_tree_root_node(tree);
    php_source_t facts = {.arena = arena,
                          .source = source,
                          .length = (size_t)source_len,
                          .failed = ts_node_has_error(root)};
    if (!facts.failed)
        php_proof_declarations(&facts, root, module_qn, 0);
    ts_tree_delete(tree);
    if (facts.failed)
        return false;
    if (!facts.count)
        return true;
    qsort(facts.items, facts.count, sizeof(*facts.items), php_proof_compare);
    for (int i = 0; i < count; i++) {
        CBMLSPDef *def = &defs[i];
        const char *qn =
            def->graph_qualified_name ? def->graph_qualified_name : def->qualified_name;
        if (!qn || !def->source_start_line)
            continue;
        php_declaration_t key = {.qn = qn, .line = def->source_start_line};
        const php_declaration_t *found =
            bsearch(&key, facts.items, facts.count, sizeof(*facts.items), php_proof_compare);
        if (found) {
            def->source_flags = found->flags & ~CBM_SOURCE_DECL_UNTYPED_VALUE_PARAMETERS;
            if (found->arity >= 0 && found->arity == def->signature_param_count)
                def->source_flags |= CBM_SOURCE_DECL_FIXED_ARITY |
                                     (found->flags & CBM_SOURCE_DECL_UNTYPED_VALUE_PARAMETERS);
        }
    }
    return true;
}

typedef struct {
    const CBMLSPDef *definition;
    const cbm_resolve_proof_t *import_proof;
    bool ambiguous;
} php_proof_type_t;
typedef struct {
    php_source_t source;
    const char *module;
    const CBMTypeRegistry *registry;
    CBMHashTable *types, *file_types, *imports, *classes;
    const CBMLSPDef *lexical_class;
    TSNode lexical_class_name;
    CBMResolvedCallArray *out;
    CBMResolveTypeMap type_map;
    bool static_call, lexical_this_call;
} php_proof_context_t;

static bool php_proof_type_put(php_proof_context_t *context, CBMHashTable *table, const char *key,
                               const CBMLSPDef *definition) {
    php_proof_type_t *old = cbm_ht_get(table, key);
    if (old) {
        old->ambiguous = true;
        return true;
    }
    php_proof_type_t *entry = cbm_arena_calloc(context->source.arena, sizeof(*entry));
    const char *copy = cbm_arena_strdup(context->source.arena, key);
    if (!entry || !copy)
        return false;
    entry->definition = definition;
    cbm_ht_set(table, copy, entry);
    return cbm_ht_get(table, copy) == entry;
}
static bool php_proof_indexes(php_proof_context_t *context, const CBMPHPImportBindings *bindings,
                              const CBMLSPDef *defs, int count) {
    for (int i = 0; i < count; i++) {
        const CBMLSPDef *def = &defs[i];
        if (!def->qualified_name || !def->short_name || !def->label ||
            (strcmp(def->label, "Class") && strcmp(def->label, "Interface")))
            continue;
        if (!php_proof_type_put(context, context->types, def->qualified_name, def))
            return false;
        if (!strcmp(def->label, "Class")) {
            const char *graph =
                def->graph_qualified_name ? def->graph_qualified_name : def->qualified_name;
            if (!php_proof_type_put(context, context->classes, graph, def))
                return false;
        }
        if (def->source_file_qn) {
            const char *key = cbm_arena_sprintf(context->source.arena, "%s\x1f%s",
                                                def->source_file_qn, def->short_name);
            if (!key || !php_proof_type_put(context, context->file_types, key, def))
                return false;
        }
    }
    if (!bindings || !bindings->imports)
        return true;
    const char *const *targets = bindings->targets ? bindings->targets : bindings->proven_targets;
    if (!targets)
        return true;
    for (int i = 0; i < bindings->imports->count; i++) {
        const CBMImport *import = &bindings->imports->items[i];
        if (import->kind != CBM_IMPORT_KIND_DEFAULT || !import->local_name)
            continue;
        const CBMLSPDef *definition = NULL;
        const char *target = targets[i];
        if (target && import->module_path) {
            const char *leaf = strrchr(import->module_path, '\\');
            leaf = leaf ? leaf + 1 : import->module_path;
            size_t length = strlen(target), suffix = strlen(".__file__");
            const php_proof_type_t *type = NULL;
            if (length > suffix && !strcmp(target + length - suffix, ".__file__")) {
                const char *key =
                    cbm_arena_sprintf(context->source.arena, "%s\x1f%s", target, leaf);
                if (key)
                    type = cbm_ht_get(context->file_types, key);
            } else
                type = cbm_ht_get(context->types, target);
            if (type && !type->ambiguous)
                definition = type->definition;
        }
        if (!php_proof_type_put(context, context->imports, import->local_name, definition))
            return false;
        php_proof_type_t *entry = cbm_ht_get(context->imports, import->local_name);
        if (entry && !entry->ambiguous && bindings->facts &&
            bindings->facts->count == (size_t)bindings->imports->count)
            entry->import_proof = bindings->facts->items[i].dependency.proof;
    }
    return true;
}
static const CBMLSPDef *php_proof_type(php_proof_context_t *context, const char *name) {
    if (!name || !name[0] || strchr(name, '\\'))
        return NULL;
    const php_proof_type_t *entry = cbm_ht_get(context->imports, name);
    /* Local type names additionally need namespace-scope evidence. A file
     * module prefix alone cannot establish PHP's class namespace binding. */
    const CBMLSPDef *definition = entry && !entry->ambiguous ? entry->definition : NULL;
    uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    return definition && definition->declaration_identity &&
                   (definition->source_flags & required) == required
               ? definition
               : NULL;
}
static cbm_resolve_result_t php_proof_type_evidence(php_proof_context_t *context,
                                                    const CBMLSPDef *definition, TSNode site) {
    if (!definition)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED};
    const char *spelling = php_proof_text(&context->source, site);
    const php_proof_type_t *import = spelling ? cbm_ht_get(context->imports, spelling) : NULL;
    if (import && (import->ambiguous || import->definition != definition || !import->import_proof))
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNKNOWN,
                                      .reason = "php_import_predecessor_missing"};
    cbm_resolve_result_t result = cbm_lsp_proof_type_lookup(
        &context->type_map, spelling, site, "php_proof-type-name-domain",
        definition->qualified_name, definition->declaration_identity,
        definition->graph_qualified_name ? definition->graph_qualified_name
                                         : definition->qualified_name,
        definition->source_flags, "php_proof-complete-source-type-lookup");
    if (!import || !cbm_resolve_admit(&result))
        return result;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(result.proof);
    const cbm_resolve_proof_t *dependency =
        cbm_resolve_proof_clone(&context->type_map.proofs, import->import_proof);
    if (!view || !dependency)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_FAILED,
                                      .reason = "php_import_predecessor_clone_failed"};
    const cbm_resolve_proof_t *premises[] = {result.proof, dependency};
    result.proof = cbm_resolve_proof_derive(&context->type_map.proofs, CBM_PROOF_TYPE_BIND,
                                            &view->source, &view->key, &result, premises, 2, true);
    if (!result.proof) {
        result.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
        result.facts = 0;
    }
    return result;
}
static const char *php_proof_occurrence_spelling(php_proof_context_t *context, TSNode site,
                                                 CBMResolvedKind kind) {
    TSNode expression = site;
    if (kind == CBM_RESOLVED_INVOCATION) {
        TSNode member = php_proof_field(site, "name");
        const char *name = php_proof_text(&context->source, member);
        /* Static call extraction records the terminal function name. */
        if (php_proof_is(site, "scoped_call_expression"))
            return name;
        const char *receiver = php_proof_text(&context->source, php_proof_field(site, "object"));
        return receiver && name ? cbm_arena_sprintf(context->source.arena, "%s.%s", receiver, name)
                                : NULL;
    }
    return php_proof_text(&context->source, expression);
}
static void php_proof_emit(php_proof_context_t *context, const char *caller, TSNode site,
                           CBMResolvedKind kind, cbm_resolve_result_t proof) {
    if (!caller || !cbm_resolve_admit(&proof))
        return;
    const char *relation = kind == CBM_RESOLVED_INVOCATION       ? "CALLS"
                           : kind == CBM_RESOLVED_CALL_REFERENCE ? "CALL_REFERENCE"
                           : kind == CBM_RESOLVED_TYPE_REFERENCE ? "USES_TYPE"
                                                                 : "USAGE";
    const char *spelling = php_proof_occurrence_spelling(context, site, kind);
    proof = cbm_lsp_proof_occurrence(&context->type_map, proof, site, caller, spelling, relation);
    if (!cbm_resolve_admit(&proof))
        return;
    CBMResolvedCall row = {.caller_qn = caller,
                           .callee_qn = proof.target,
                           .strategy = "php_source_binding",
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
static bool php_proof_read_syntax(const char *kind) {
    static const char *const allowed[] = {"compound_statement",
                                          "expression_statement",
                                          "return_statement",
                                          "member_call_expression",
                                          "scoped_call_expression",
                                          "nullsafe_member_call_expression",
                                          "function_call_expression",
                                          "variable_name",
                                          "name",
                                          "qualified_name",
                                          "namespace_name",
                                          "arguments",
                                          "argument",
                                          "integer",
                                          "float",
                                          "string",
                                          "string_content",
                                          "boolean",
                                          "null",
                                          "assignment_expression",
                                          "object_creation_expression",
                                          "binary_expression",
                                          "unary_op_expression",
                                          "parenthesized_expression",
                                          "if_statement",
                                          "else_clause",
                                          "else_if_clause",
                                          "echo_statement",
                                          "comment",
                                          "throw_expression",
                                          "throw_statement",
                                          "empty_statement",
                                          NULL};
    for (size_t i = 0; allowed[i]; i++)
        if (!strcmp(kind, allowed[i]))
            return true;
    return false;
}
static void php_proof_audit_body(php_proof_context_t *context, TSNode node, CBMHashTable *blocked,
                                 bool write, unsigned depth) {
    php_source_t *source = &context->source;
    if (ts_node_is_null(node) || !php_proof_budget(source, depth) || !ts_node_is_named(node))
        return;
    const char *kind = ts_node_type(node);
    if (!php_proof_read_syntax(kind)) {
        source->failed = true;
        return;
    }
    /* Calls that can alter the caller's symbol table, including aliases of
     * builtins, require an effect model. No ordinary type inference proves it. */
    if (php_proof_is(node, "function_call_expression")) {
        source->failed = true;
        return;
    }
    if (php_proof_is(node, "variable_name") && write) {
        const char *name = php_proof_text(source, node);
        if (!name) {
            source->failed = true;
            return;
        }
        cbm_ht_set(blocked, name, (void *)1);
        if (!cbm_ht_get(blocked, name))
            source->failed = true;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            bool child_write =
                write || php_proof_is(node, "arguments") ||
                (php_proof_is(node, "assignment_expression") && field && !strcmp(field, "left"));
            php_proof_audit_body(context, ts_tree_cursor_current_node(&cursor), blocked,
                                 child_write, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static cbm_resolve_method_candidate_t php_proof_method_candidate(
    const cbm_resolve_method_query_t *query, const CBMTypeRegistry *owner,
    // WHY: The shared classify callback type requires a mutable void * context.
    // cppcheck-suppress constParameterCallback
    const CBMRegisteredFunc *candidate, void *opaque) {
    (void)owner;
    const php_proof_context_t *context = opaque;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_FIXED_ARITY;
    bool lexical_access = context->lexical_this_call && context->lexical_class &&
                          candidate->receiver_type &&
                          !strcmp(candidate->receiver_type, context->lexical_class->qualified_name);
    bool accessible = (candidate->source_flags & CBM_SOURCE_DECL_PUBLIC) || lexical_access;
    if ((candidate->source_flags & required) != required || !accessible ||
        !(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) ||
        candidate->min_params != query->argument_count ||
        (query->argument_count &&
         !(candidate->source_flags & CBM_SOURCE_DECL_UNTYPED_VALUE_PARAMETERS)) ||
        (context->static_call && !(candidate->source_flags & CBM_SOURCE_DECL_STATIC)))
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
/* An untyped by-value parameter accepts these values without type coercion or
 * caller-variable reference binding. Nested calls, named/unpacked arguments,
 * interpolation and variable variables remain outside this audited subset. */
static bool php_proof_argument(TSNode argument) {
    if (!php_proof_is(argument, "argument") ||
        !ts_node_is_null(php_proof_field(argument, "name")) ||
        ts_node_named_child_count(argument) != 1)
        return false;
    TSNode value = ts_node_named_child(argument, 0);
    if (php_proof_is(value, "variable_name"))
        return ts_node_named_child_count(value) == 1 &&
               php_proof_is(ts_node_named_child(value, 0), "name");
    if (php_proof_is(value, "integer") || php_proof_is(value, "float") ||
        php_proof_is(value, "boolean") || php_proof_is(value, "null"))
        return true;
    if (!php_proof_is(value, "string"))
        return false;
    uint32_t count = ts_node_named_child_count(value);
    for (uint32_t i = 0; i < count; i++)
        if (!php_proof_is(ts_node_named_child(value, i), "string_content"))
            return false;
    return true;
}
static int php_proof_arguments(TSNode arguments) {
    if (ts_node_is_null(arguments))
        return -1;
    int count = 0;
    uint32_t children = ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < children; i++) {
        TSNode child = ts_node_named_child(arguments, i);
        if (php_proof_is(child, "comment"))
            continue;
        if (!php_proof_argument(child) || ++count > 64)
            return -1;
    }
    return count;
}
static void php_proof_call(php_proof_context_t *context, TSNode call, const char *caller,
                           CBMScope *scope) {
    bool static_call = php_proof_is(call, "scoped_call_expression");
    TSNode receiver = php_proof_field(call, static_call ? "scope" : "object");
    TSNode method = php_proof_field(call, "name"), args = php_proof_field(call, "arguments");
    int argument_count = php_proof_arguments(args);
    if (!php_proof_is(method, "name") || argument_count < 0)
        return;
    const char *name = php_proof_text(&context->source, method);
    cbm_resolve_type_fact_t fact = {0};
    if (static_call && php_proof_is(receiver, "name")) {
        const CBMLSPDef *type = php_proof_type(context, php_proof_text(&context->source, receiver));
        if (!type)
            return;
        fact = (cbm_resolve_type_fact_t){
            .type = cbm_type_named(context->source.arena, type->qualified_name),
            .status = CBM_RESOLVE_TYPE_KNOWN,
            .origin = CBM_RESOLVE_TYPE_ANNOTATION,
            .proof_pool = &context->type_map.proofs,
            .evidence = php_proof_type_evidence(context, type, receiver)};
        php_proof_emit(context, caller, receiver, CBM_RESOLVED_TYPE_REFERENCE, fact.evidence);
    } else if (!static_call && php_proof_is(receiver, "variable_name")) {
        const char *variable = php_proof_text(&context->source, receiver);
        if (variable)
            fact = cbm_resolve_types_lookup(&context->type_map, scope, variable,
                                            ts_node_start_byte(call));
    }
    if (fact.status != CBM_RESOLVE_TYPE_KNOWN || !fact.type || fact.type->kind != CBM_TYPE_NAMED)
        return;
    context->static_call = static_call;
    const char *receiver_text = php_proof_text(&context->source, receiver);
    context->lexical_this_call = !static_call && receiver_text && !strcmp(receiver_text, "$this");
    cbm_resolve_method_query_t query = {.proof_pool = &context->type_map.proofs,
                                        .proof_source = &context->type_map.proof_source,
                                        .lookup_domain = fact.type->data.named.qualified_name,
                                        .registry = context->registry,
                                        .receiver = &fact,
                                        .receiver_qn = fact.type->data.named.qualified_name,
                                        .method_name = name,
                                        .argument_count = argument_count,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                         CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                        .classify = php_proof_method_candidate,
                                        .context = context};
    cbm_resolve_method_result_t result = cbm_resolve_method(&query);
    if (argument_count && cbm_resolve_admit(&result.result))
        result.result =
            cbm_lsp_proof_rule(&context->type_map, result.result, args, query.lookup_domain, name,
                               "php-fixed-untyped-by-value-positional-arguments", NULL);
    php_proof_emit(context, caller, call, CBM_RESOLVED_INVOCATION, result.result);
}
static void php_proof_calls(php_proof_context_t *context, TSNode node, const char *caller,
                            CBMScope *scope, unsigned depth) {
    if (!php_proof_budget(&context->source, depth))
        return;
    if (php_proof_is(node, "member_call_expression") ||
        php_proof_is(node, "scoped_call_expression") ||
        php_proof_is(node, "nullsafe_member_call_expression"))
        php_proof_call(context, node, caller, scope);
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            php_proof_calls(context, ts_tree_cursor_current_node(&cursor), caller, scope,
                            depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void php_proof_function(php_proof_context_t *context, TSNode node, const char *parent) {
    const char *name = php_proof_text(&context->source, php_proof_field(node, "name"));
    const char *caller =
        name ? cbm_arena_sprintf(context->source.arena, "%s.%s", parent, name) : NULL;
    TSNode body = php_proof_field(node, "body"), parameters = php_proof_field(node, "parameters");
    if (!caller || ts_node_is_null(body) || ts_node_is_null(parameters))
        return;
    CBMHashTable *blocked = cbm_ht_create(16);
    if (!blocked) {
        context->source.failed = true;
        return;
    }
    php_proof_audit_body(context, body, blocked, false, 0);
    bool failed = context->source.failed;
    /* Unsupported function syntax is local; a budget/allocation failure is
     * also conservative because this function contributes no proof rows. */
    context->source.failed = false;
    if (failed) {
        cbm_ht_free(blocked);
        return;
    }
    CBMScope *scope = cbm_scope_push(context->source.arena, NULL);
    if (!scope) {
        cbm_ht_free(blocked);
        return;
    }
    bool this_parameter = false;
    uint32_t parameter_count = ts_node_named_child_count(parameters);
    for (uint32_t i = 0; i < parameter_count; i++) {
        const char *parameter = php_proof_text(
            &context->source, php_proof_field(ts_node_named_child(parameters, i), "name"));
        if (parameter && !strcmp(parameter, "$this"))
            this_parameter = true;
    }
    if (context->lexical_class && php_proof_is(node, "method_declaration") &&
        !(php_proof_modifiers(&context->source, node) & CBM_SOURCE_DECL_STATIC) &&
        !this_parameter && !cbm_ht_get(blocked, "$this")) {
        const CBMLSPDef *definition = context->lexical_class;
        cbm_resolve_result_t proof = cbm_lsp_proof_type_lookup(
            &context->type_map, "$this", context->lexical_class_name,
            "php-lexical-enclosing-class-domain", definition->qualified_name,
            definition->declaration_identity,
            definition->graph_qualified_name ? definition->graph_qualified_name
                                             : definition->qualified_name,
            definition->source_flags, "php-exact-enclosing-instance-class");
        const CBMType *type = cbm_type_named(context->source.arena, definition->qualified_name);
        cbm_resolve_types_declare(&context->type_map, scope, "$this", ts_node_start_byte(node),
                                  type, CBM_RESOLVE_TYPE_ANNOTATION, &proof);
    }
    TSTreeCursor cursor = ts_tree_cursor_new(parameters);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode parameter = ts_tree_cursor_current_node(&cursor);
            if (!php_proof_is(parameter, "simple_parameter"))
                continue;
            TSNode type_node = php_proof_field(parameter, "type"),
                   variable_node = php_proof_field(parameter, "name");
            const char *variable = php_proof_text(&context->source, variable_node);
            if (!variable || cbm_ht_get(blocked, variable) ||
                !php_proof_is(type_node, "named_type"))
                continue;
            bool reference = false;
            uint32_t n = ts_node_named_child_count(parameter);
            for (uint32_t i = 0; i < n; i++)
                if (php_proof_is(ts_node_named_child(parameter, i), "reference_modifier"))
                    reference = true;
            const char *spelling = php_proof_text(&context->source, type_node);
            const CBMLSPDef *definition = reference ? NULL : php_proof_type(context, spelling);
            if (!definition)
                continue;
            cbm_resolve_result_t proof = php_proof_type_evidence(context, definition, type_node);
            const CBMType *type = cbm_type_named(context->source.arena, definition->qualified_name);
            if (!cbm_resolve_types_declare(&context->type_map, scope, variable,
                                           ts_node_start_byte(parameter), type,
                                           CBM_RESOLVE_TYPE_ANNOTATION, &proof))
                continue;
            php_proof_emit(context, caller, type_node, CBM_RESOLVED_TYPE_REFERENCE, proof);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    cbm_ht_free(blocked);
    php_proof_calls(context, body, caller, scope, 0);
}
static const CBMLSPDef *php_proof_lexical_class(php_proof_context_t *context, TSNode node,
                                                const char *graph) {
    if (!php_proof_is(node, "class_declaration"))
        return NULL;
    /* Lexical $this does not borrow a namespace binding. Its declaration must
     * be this exact class, with no base, trait, attribute or interface domain. */
    uint32_t children = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < children; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!php_proof_is(child, "name") && !php_proof_is(child, "declaration_list") &&
            !php_proof_is(child, "final_modifier") && !php_proof_is(child, "comment"))
            return NULL;
    }
    const php_proof_type_t *entry = cbm_ht_get(context->classes, graph);
    const CBMLSPDef *definition = entry && !entry->ambiguous ? entry->definition : NULL;
    uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_CLASS_DOMAIN_COMPLETE;
    if (!definition || !definition->declaration_identity ||
        definition->source_start_line != ts_node_start_point(node).row + 1 ||
        (definition->source_flags & required) != required)
        return NULL;
    return definition;
}
static void php_proof_walk(php_proof_context_t *context, TSNode node, const char *parent,
                           unsigned depth) {
    if (!php_proof_budget(&context->source, depth))
        return;
    if (php_proof_is(node, "function_definition") || php_proof_is(node, "method_declaration")) {
        php_proof_function(context, node, parent);
        return;
    }
    const CBMLSPDef *previous_class = context->lexical_class;
    TSNode previous_class_name = context->lexical_class_name;
    bool class_node =
        php_proof_is(node, "class_declaration") || php_proof_is(node, "interface_declaration");
    if (class_node) {
        const char *name = php_proof_text(&context->source, php_proof_field(node, "name"));
        parent =
            name ? cbm_arena_sprintf(context->source.arena, "%s.%s", context->module, name) : NULL;
        if (!parent)
            return;
        context->lexical_class = php_proof_lexical_class(context, node, parent);
        context->lexical_class_name = php_proof_field(node, "name");
    } else if (!php_proof_is(node, "program") && !php_proof_is(node, "namespace_definition") &&
               !php_proof_is(node, "compound_statement") && !php_proof_is(node, "declaration_list"))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            php_proof_walk(context, ts_tree_cursor_current_node(&cursor), parent, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    context->lexical_class = previous_class;
    context->lexical_class_name = previous_class_name;
}
void cbm_php_proof_resolve(CBMArena *arena, const char *source, int source_len,
                           const char *module_qn, TSNode root, const CBMTypeRegistry *registry,
                           const CBMPHPImportBindings *bindings, const CBMLSPDef *defs, int count,
                           CBMResolvedCallArray *out) {
    cbm_php_proof_resolve_with_source(arena, source, source_len, module_qn, root, registry,
                                      bindings, defs, count, out, NULL);
}

void cbm_php_proof_resolve_with_source(CBMArena *arena, const char *source, int source_len,
                                       const char *module_qn, TSNode root,
                                       const CBMTypeRegistry *registry,
                                       const CBMPHPImportBindings *bindings, const CBMLSPDef *defs,
                                       int count, CBMResolvedCallArray *out,
                                       const cbm_resolve_proof_source_t *source_context) {
    if (!arena || !source || source_len <= 0 || !module_qn || !registry || !out ||
        ts_node_is_null(root) || ts_node_has_error(root))
        return;
    /* The per-file import snapshot has no namespace-block key yet. Refuse
     * to project one block's aliases into another lexical namespace. */
    TSTreeCursor namespace_cursor = ts_tree_cursor_new(root);
    bool namespace_complete = true;
    if (ts_tree_cursor_goto_first_child(&namespace_cursor)) {
        unsigned namespaces = 0;
        do {
            TSNode child = ts_tree_cursor_current_node(&namespace_cursor);
            if (php_proof_is(child, "namespace_definition") &&
                (++namespaces > 1 || !ts_node_is_null(php_proof_field(child, "body"))))
                namespace_complete = false;
        } while (ts_tree_cursor_goto_next_sibling(&namespace_cursor));
    }
    ts_tree_cursor_delete(&namespace_cursor);
    if (!namespace_complete)
        return;
    /* Append through a private descriptor; publish only after complete analysis.
     * Arena growth preserves the original buffer and its committed prefix. */
    CBMResolvedCallArray staged = *out;
    php_proof_context_t context = {
        .source = {.arena = arena, .source = source, .length = (size_t)source_len},
        .module = module_qn,
        .registry = registry,
        .out = &staged};
    context.types = cbm_ht_create(64);
    context.file_types = cbm_ht_create(64);
    context.imports = cbm_ht_create(16);
    context.classes = cbm_ht_create(64);
    cbm_resolve_types_init(&context.type_map, arena, 0);
    bool context_ok = source_context
                          ? cbm_lsp_proof_context(&context.type_map, source_context, module_qn)
                          : cbm_lsp_proof_standalone_context(&context.type_map, source, source_len,
                                                             module_qn, CBM_SOURCE_ORIGIN_RAW);
    if (!context_ok)
        context.source.failed = true;
    if (context.types && context.file_types && context.imports && context.classes &&
        php_proof_indexes(&context, bindings, defs, count))
        php_proof_walk(&context, root, module_qn, 0);
    if (!context.source.failed)
        *out = staged;
    cbm_ht_free(context.types);
    cbm_ht_free(context.file_types);
    cbm_ht_free(context.imports);
    cbm_ht_free(context.classes);
}
