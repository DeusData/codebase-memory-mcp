#include "proof_bridge.h"
#include "kotlin_proof.h"
#include "../callable_sig.h"
#include "../resolve/policy.h"
#include "../resolve/symbols.h"
#include "foundation/hash_table.h"
#include <stdlib.h>
#include <string.h>

typedef struct {
    const char *qn;
    uint32_t line, flags;
    int arity;
    const char **parameter_types;
    const CBMType *return_type;
    const CBMType *signature;
} kt_source_decl_t;
typedef struct {
    CBMArena *arena;
    const char *source;
    size_t length, visits;
    bool failed;
    kt_source_decl_t *declarations;
    size_t count, capacity;
    const char *package;
    bool imports;
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
    source->declarations[source->count++] = (kt_source_decl_t){
        .qn = qn, .line = ts_node_start_point(node).row + 1, .flags = flags, .arity = arity};
}
static const CBMType *kt_source_type(CBMArena *arena, const char *text, bool imports) {
    if (!text)
        return cbm_type_unknown();
    size_t length = strlen(text);
    char *compact = cbm_arena_alloc(arena, length + 1);
    if (!compact)
        return cbm_type_unknown();
    size_t count = 0;
    for (size_t i = 0; i < length; i++)
        if (text[i] != ' ' && text[i] != '\t' && text[i] != '\r' && text[i] != '\n')
            compact[count++] = text[i];
    compact[count] = '\0';
    bool function = !strncmp(compact, "()->", 4);
    const char *name = function ? compact + 4 : compact;
    bool qualified = !strncmp(name, "kotlin.", 7);
    const char *short_name = qualified ? name + 7 : name;
    static const char *const builtin[] = {"Int",     "Long", "Short",  "Byte", "Float", "Double",
                                          "Boolean", "Char", "String", "Unit", NULL};
    bool supported = false;
    for (size_t i = 0; builtin[i]; i++)
        if (!strcmp(short_name, builtin[i]))
            supported = true;
    if (!supported || (imports && !qualified))
        return cbm_type_unknown();
    const CBMType *type = cbm_type_named(arena, name);
    if (!function)
        return type;
    const CBMType **returns = cbm_arena_calloc(arena, 2 * sizeof(*returns));
    if (!returns)
        return cbm_type_unknown();
    returns[0] = type;
    return cbm_type_func(arena, NULL, NULL, returns);
}
static void kt_proof_function_fact(kt_proof_source_t *source, TSNode node, const char *parent) {
    TSNode name_node = kt_proof_child(node, "simple_identifier");
    const char *name = kt_proof_text(source, name_node);
    const char *qn = kt_proof_join(source, parent, name);
    uint32_t flags = kt_proof_modifiers(source, node);
    TSNode modifiers = kt_proof_child(node, "modifiers");
    for (uint32_t i = 0; !ts_node_is_null(modifiers) && i < ts_node_named_child_count(modifiers);
         i++)
        if (!kt_proof_is(ts_node_named_child(modifiers, i), "visibility_modifier"))
            flags = 0; /* Suspend/expect/annotation rules need separate proofs. */
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
    size_t index = source->count;
    kt_proof_add(source, qn, node, flags, arity);
    if (index >= source->count || arity < 0)
        return;
    const char **types = cbm_arena_calloc(source->arena, ((size_t)arity + 1) * sizeof(*types));
    if (!types) {
        source->failed = true;
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(parameters);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        int at = 0;
        do {
            TSNode parameter = ts_tree_cursor_current_node(&cursor);
            if (!kt_proof_is(parameter, "parameter"))
                continue;
            TSNode type = {0};
            uint32_t children = ts_node_named_child_count(parameter);
            for (uint32_t i = 0; i < children; i++) {
                TSNode child = ts_node_named_child(parameter, i);
                if (kt_proof_is(child, "simple_identifier"))
                    continue;
                if (!ts_node_is_null(type)) {
                    source->declarations[index].arity = -1;
                    break;
                }
                type = child;
            }
            types[at++] = kt_proof_text(source, type);
            if (!types[at - 1])
                source->declarations[index].arity = -1;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
    source->declarations[index].parameter_types = types;
    /* A block body with no return annotation has the language-defined Unit
     * return type. Expression bodies require their own inference proof. */
    TSNode body = kt_proof_child(node, "function_body");
    const char *body_text = kt_proof_text(source, body);
    bool annotated = false;
    const CBMType *return_type = NULL;
    TSTreeCursor declaration = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&declaration))
        do {
            TSNode child = ts_tree_cursor_current_node(&declaration);
            if (ts_node_is_named(child) &&
                ts_node_start_byte(child) > ts_node_end_byte(parameters) &&
                !kt_proof_is(child, "function_body")) {
                annotated = true;
                return_type =
                    kt_source_type(source->arena, kt_proof_text(source, child), source->imports);
            }
        } while (ts_tree_cursor_goto_next_sibling(&declaration));
    ts_tree_cursor_delete(&declaration);
    if (!annotated && body_text && body_text[0] == '{')
        return_type = cbm_type_named(source->arena, "kotlin.Unit");
    source->declarations[index].return_type = return_type;
    const CBMType **parameters_types =
        cbm_arena_calloc(source->arena, ((size_t)arity + 1) * sizeof(*parameters_types));
    const CBMType **returns = cbm_arena_calloc(source->arena, 2 * sizeof(*returns));
    if (!parameters_types || !returns) {
        source->failed = true;
        return;
    }
    for (int i = 0; i < arity; i++)
        parameters_types[i] = kt_source_type(source->arena, types[i], source->imports);
    returns[0] = return_type;
    source->declarations[index].signature =
        cbm_type_func(source->arena, NULL, parameters_types, returns);
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
    for (int i = 0; i < count; i++) {
        defs[i].source_flags = 0;
        defs[i].source_signature = NULL;
        defs[i].source_return_type = NULL;
    }
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
    TSNode imports = kt_proof_child(root, "import_list");
    facts.imports = !ts_node_is_null(imports) && ts_node_named_child_count(imports) != 0;
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
                if (first < facts.count)
                    facts.declarations[first].flags |= CBM_SOURCE_DECL_TOP_LEVEL;
            } else if (kt_proof_is(child, "type_alias") ||
                       kt_proof_is(child, "object_declaration")) {
                const char *name = kt_proof_text(&facts, kt_proof_child(child, "type_identifier"));
                if (name)
                    kt_proof_add(&facts, kt_proof_join(&facts, prefix, name), child,
                                 CBM_SOURCE_DECL_TOP_LEVEL, -1);
            } else if (kt_proof_is(child, "property_declaration")) {
                TSNode variable = kt_proof_child(child, "variable_declaration");
                const char *name =
                    kt_proof_text(&facts, kt_proof_child(variable, "simple_identifier"));
                if (name)
                    kt_proof_add(&facts, kt_proof_join(&facts, prefix, name), child,
                                 kt_proof_modifiers(&facts, child) | CBM_SOURCE_DECL_TOP_LEVEL, -1);
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
        if (found->arity >= 0 && found->arity == def->signature_param_count) {
            def->source_flags |= CBM_SOURCE_DECL_FIXED_ARITY;
        }
        def->source_return_type = found->return_type;
        def->source_signature = found->signature;
        def->namespace_name = package;
    }
    return true;
}

typedef struct {
    const char *target;
    bool ambiguous;
} kt_proof_name_t;
typedef struct {
    cbm_resolve_method_result_t identity;
    uint32_t available_after;
} kt_callable_fact_t;
typedef struct {
    kt_proof_source_t source;
    const CBMTypeRegistry *registry;
    const CBMFileResult *file;
    CBMResolvedCallArray *out;
    CBMResolveTypeMap types;
    CBMHashTable *imports, *callers, *local_types, *top_callables, *properties;
    CBMHashTable *value_types, *local_callables, *assignments, *top_types;
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
    TSNode imports = kt_proof_child(root, "import_list");
    context->source.imports = !ts_node_is_null(imports) && ts_node_named_child_count(imports) != 0;
    uint32_t children = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < children; i++) {
        TSNode child = ts_node_named_child(root, i);
        if (!kt_proof_is(child, "class_declaration"))
            continue;
        const char *name =
            kt_proof_text(&context->source, kt_proof_child(child, "type_identifier"));
        const char *key = name ? cbm_arena_sprintf(context->source.arena, "%u:%s",
                                                   ts_node_start_point(child).row + 1, name)
                               : NULL;
        if (!key)
            return false;
        cbm_ht_set(context->top_types, key, (void *)1);
        if (!cbm_ht_get(context->top_types, key))
            return false;
    }
    for (int i = 0; i < context->file->defs.count; i++) {
        const CBMDefinition *def = &context->file->defs.items[i];
        if (!def->name || !def->qualified_name || !def->label)
            continue;
        if (!strcmp(def->label, "Function") || !strcmp(def->label, "Method")) {
            const char *key =
                cbm_arena_sprintf(context->source.arena, "%u:%s", def->start_line, def->name);
            if (!key || !kt_proof_put(context, context->callers, key, def->qualified_name))
                return false;
        } else if (!strcmp(def->label, "Variable") || !strcmp(def->label, "Constant")) {
            const char *key =
                cbm_arena_sprintf(context->source.arena, "%u:%s", def->start_line, def->name);
            if (!key || !kt_proof_put(context, context->properties, key, def->qualified_name))
                return false;
        } else if (cbm_label_is_type_like(def->label)) {
            const char *key =
                cbm_arena_sprintf(context->source.arena, "%u:%s", def->start_line, def->name);
            if (!key ||
                (!cbm_ht_get(context->top_types, key) &&
                 !kt_proof_put(context, context->local_types, def->name, def->qualified_name)))
                return false;
        }
    }
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
static cbm_resolve_result_t kt_proof_type_evidence(kt_proof_context_t *context,
                                                   const CBMRegisteredType *type, TSNode site) {
    if (!type)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED};
    return cbm_lsp_proof_type_lookup(
        &context->types, kt_proof_text(&context->source, site), site,
        "kotlin_proof-type-name-domain", type->qualified_name, type->declaration_identity,
        type->graph_qualified_name ? type->graph_qualified_name : type->qualified_name,
        type->source_flags, "kotlin_proof-complete-source-type-lookup");
}
static const char *kt_proof_occurrence_spelling(kt_proof_context_t *context, TSNode site,
                                                CBMResolvedKind kind) {
    TSNode expression = site;
    if (kind == CBM_RESOLVED_INVOCATION)
        expression = ts_node_named_child(site, 0);
    if (kt_proof_is(expression, "navigation_expression")) {
        TSNode receiver = ts_node_named_child(expression, 0);
        TSNode suffix = kt_proof_child(expression, "navigation_suffix");
        TSNode member = kt_proof_child(suffix, "simple_identifier");
        const char *left = kt_proof_text(&context->source, receiver);
        const char *right = kt_proof_text(&context->source, member);
        return left && right ? cbm_arena_sprintf(context->source.arena, "%s.%s", left, right)
                             : NULL;
    }
    return kt_proof_text(&context->source, expression);
}
static void kt_proof_emit(kt_proof_context_t *context, const char *caller, TSNode site,
                          CBMResolvedKind kind, cbm_resolve_result_t proof) {
    if (!caller || !cbm_resolve_admit(&proof))
        return;
    const char *relation = kind == CBM_RESOLVED_INVOCATION       ? "CALLS"
                           : kind == CBM_RESOLVED_CALL_REFERENCE ? "CALL_REFERENCE"
                           : kind == CBM_RESOLVED_TYPE_REFERENCE ? "USES_TYPE"
                                                                 : "USAGE";
    const char *spelling = kt_proof_occurrence_spelling(context, site, kind);
    proof = cbm_lsp_proof_occurrence(&context->types, proof, site, caller, spelling, relation);
    if (!cbm_resolve_admit(&proof))
        return;
    CBMResolvedCall row = {.caller_qn = caller,
                           .callee_qn = proof.target,
                           .strategy = "kotlin_source_binding",
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
                                          "conjunction_expression",
                                          "disjunction_expression",
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
                                          "callable_reference",
                                          "function_type",
                                          "function_type_parameters",
                                          "binding_pattern_kind",
                                          "directly_assignable_expression",
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
    /* Only binding targets shadow names; an initializer/reference RHS is a
     * read. Writes invalidate facts throughout this conservative body scope. */
    bool property = kt_proof_is(node, "property_declaration");
    bool assignment = kt_proof_is(node, "assignment");
    TSNode target = property     ? kt_proof_child(node, "variable_declaration")
                    : assignment ? ts_node_named_child(node, 0)
                                 : (TSNode){0};
    if (property)
        target = kt_proof_child(target, "simple_identifier");
    while (kt_proof_is(target, "directly_assignable_expression") &&
           ts_node_named_child_count(target) == 1)
        target = ts_node_named_child(target, 0);
    if (kt_proof_is(target, "simple_identifier")) {
        const char *name = kt_proof_text(&context->source, target);
        if (!name) {
            context->source.failed = true;
            return;
        }
        if (cbm_ht_get(blocked, name))
            cbm_ht_set(context->assignments, name, (void *)1);
        cbm_ht_set(blocked, name, (void *)1);
        if (assignment)
            cbm_ht_set(context->assignments, name, (void *)1);
        if (!cbm_ht_get(blocked, name))
            context->source.failed = true;
    } else if (assignment) {
        /* Compound/indirect targets need an effect-aware flow model. */
        context->source.failed = true;
        return;
    }
    (void)binding;
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
    if ((candidate->source_flags & required) != required || candidate->min_params < 0 ||
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
/* Only these source spellings have a proved built-in meaning in this first
 * applicability bridge. Type aliases, nullable types and generics stay unknown.
 */
static const CBMType *kt_proof_scalar_type(CBMArena *arena, const char *text) {
    if (!text)
        return cbm_type_unknown();
    const char *name = !strncmp(text, "kotlin.", 7) ? text + 7 : text;
    static const char *const names[] = {"Int",     "Long", "Short",  "Byte", "Float", "Double",
                                        "Boolean", "Char", "String", "Unit", NULL};
    for (size_t i = 0; names[i]; i++)
        if (!strcmp(name, names[i]))
            return cbm_type_named(arena, cbm_arena_sprintf(arena, "kotlin.%s", name));
    return cbm_type_unknown();
}
static bool kt_proof_namespace_has_type(kt_proof_context_t *context, const char *package,
                                        const char *name) {
    if (!package)
        return true;
    CBMNamespaceTypeIter iterator;
    cbm_registry_types_in_namespace(context->registry, package, name, &iterator);
    if (cbm_namespace_type_next(&iterator, NULL))
        return true;
    return !iterator.complete;
}
static const CBMType *kt_proof_canonical_type(kt_proof_context_t *context, const CBMType *type,
                                              const char *package, bool top_level, unsigned depth) {
    if (!type || depth > 8)
        return cbm_type_unknown();
    if (type->kind == CBM_TYPE_NAMED) {
        const char *name = type->data.named.qualified_name;
        if (!name)
            return cbm_type_unknown();
        bool qualified = !strncmp(name, "kotlin.", 7);
        if ((!qualified && (!top_level || kt_proof_namespace_has_type(context, package, name))) ||
            kt_proof_namespace_has_type(context, "kotlin", qualified ? name + 7 : name))
            return cbm_type_unknown();
        return kt_proof_scalar_type(context->source.arena, name);
    }
    if (type->kind != CBM_TYPE_FUNC)
        return cbm_type_unknown();
    const CBMType *const *params = type->data.func.param_types;
    unsigned count = 0;
    while (count < 64 && params && params[count])
        count++;
    if (count == 64)
        return cbm_type_unknown();
    const CBMType **canonical =
        cbm_arena_calloc(context->source.arena, ((size_t)count + 1) * sizeof(*canonical));
    const CBMType **returns = cbm_arena_calloc(context->source.arena, 2 * sizeof(*returns));
    if (!canonical || !returns)
        return cbm_type_unknown();
    for (unsigned i = 0; i < count; i++)
        canonical[i] = kt_proof_canonical_type(context, params[i], package, top_level, depth + 1);
    if (type->data.func.return_types && type->data.func.return_types[0] &&
        !type->data.func.return_types[1])
        returns[0] = kt_proof_canonical_type(context, type->data.func.return_types[0], package,
                                             top_level, depth + 1);
    return cbm_type_func(context->source.arena, NULL, canonical, returns);
}
static const CBMType *kt_proof_signature(kt_proof_context_t *context,
                                         const CBMRegisteredFunc *function) {
    if (!function || function->min_params < 0 || function->min_params >= 64 ||
        !(function->source_flags & CBM_SOURCE_DECL_FIXED_ARITY))
        return cbm_type_unknown();
    const CBMType *signature =
        kt_proof_canonical_type(context, function->source_signature, function->source_namespace,
                                (function->source_flags & CBM_SOURCE_DECL_TOP_LEVEL) != 0, 0);
    if (!signature || signature->kind != CBM_TYPE_FUNC)
        return cbm_type_unknown();
    /* The shared factory can return a FUNC after an array allocation fails.
     * A missing parameter array must not turn a nonzero-arity declaration
     * into a zero-argument callable value. */
    const CBMType *const *params = signature->data.func.param_types;
    unsigned count = 0;
    while (count < 64 && params && params[count])
        count++;
    return count == (unsigned)function->min_params ? signature : cbm_type_unknown();
}
static cbm_resolve_method_result_t kt_proof_function_identity(kt_proof_context_t *context,
                                                              const char *name, CBMHashTable *bound,
                                                              CBMHashTable *blocked) {
    cbm_resolve_method_result_t none = {0};
    if (!context->bare_calls || !name || (bound && cbm_ht_get(bound, name)) ||
        (blocked && cbm_ht_get(blocked, name)) || cbm_ht_get(context->local_types, name) ||
        context->wildcard)
        return none;
    const kt_proof_name_t *import = cbm_ht_get(context->imports, name);
    if (import && (import->ambiguous || !import->target))
        return none;
    const char *qn =
        import ? import->target : kt_proof_join(&context->source, context->source.package, name);
    const char *dot = qn ? strrchr(qn, '.') : NULL;
    const char *package =
        dot ? cbm_arena_strndup(context->source.arena, qn, (size_t)(dot - qn)) : "";
    if (!qn || kt_proof_namespace_has_type(context, package, dot ? dot + 1 : qn))
        return none;
    cbm_resolve_function_query_t query = {
        .proof_pool = &context->types.proofs,
        .proof_source = &context->types.proof_source,
        .lookup_domain = qn,
        .registry = context->registry,
        .function_qn = qn,
        .function_binding = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                             .rule = CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                             .facts = CBM_RESOLVE_FACT_LANGUAGE_BOUND |
                                      CBM_RESOLVE_FACT_OCCURRENCE_EXACT},
        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT | CBM_RESOLVE_METHOD_NAME_IDENTITY,
        .classify = kt_proof_candidate};
    return cbm_resolve_function(&query);
}
static TSNode kt_proof_unwrap(TSNode node) {
    while (kt_proof_is(node, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        node = ts_node_named_child(node, 0);
    return node;
}
static cbm_resolve_method_result_t kt_proof_reference(kt_proof_context_t *context, TSNode node,
                                                      CBMHashTable *bound, CBMHashTable *blocked) {
    node = kt_proof_unwrap(node);
    if (!kt_proof_is(node, "callable_reference") || ts_node_named_child_count(node) != 1)
        return (cbm_resolve_method_result_t){0};
    TSNode name = ts_node_named_child(node, 0);
    return kt_proof_function_identity(context, kt_proof_text(&context->source, name), bound,
                                      blocked);
}
static const kt_callable_fact_t *kt_proof_callable(kt_proof_context_t *context, const char *name,
                                                   uint32_t at, CBMHashTable *bound,
                                                   CBMHashTable *blocked) {
    if (!name || !context->bare_calls || (bound && cbm_ht_get(bound, name)) ||
        (context->assignments && cbm_ht_get(context->assignments, name)))
        return NULL;
    const kt_callable_fact_t *fact =
        context->local_callables ? cbm_ht_get(context->local_callables, name) : NULL;
    if (fact)
        return fact->available_after <= at ? fact : NULL;
    if (blocked && cbm_ht_get(blocked, name))
        return NULL;
    return cbm_ht_get(context->top_callables, name);
}
static bool kt_proof_equal_type(const CBMType *left, const CBMType *right, unsigned depth) {
    if (!left || !right || depth > 8 || left->kind != right->kind)
        return false;
    if (left->kind == CBM_TYPE_NAMED)
        return left->data.named.qualified_name && right->data.named.qualified_name &&
               !strcmp(left->data.named.qualified_name, right->data.named.qualified_name);
    if (left->kind != CBM_TYPE_FUNC)
        return false;
    const CBMType *const *a = left->data.func.param_types, *const *b = right->data.func.param_types;
    for (unsigned i = 0; i < 64; i++) {
        const CBMType *x = a ? a[i] : NULL, *y = b ? b[i] : NULL;
        if (!x || !y) {
            if (x != y)
                return false;
            a = left->data.func.return_types;
            b = right->data.func.return_types;
            return a && b && a[0] && b[0] && !a[1] && !b[1] &&
                   kt_proof_equal_type(a[0], b[0], depth + 1);
        }
        if (!kt_proof_equal_type(x, y, depth + 1))
            return false;
    }
    return false;
}
static const CBMType *kt_proof_argument_type(kt_proof_context_t *context, TSNode node,
                                             CBMHashTable *bound, CBMHashTable *blocked) {
    node = kt_proof_unwrap(node);
    if (kt_proof_is(node, "integer_literal")) {
        const char *text = kt_proof_text(&context->source, node);
        if (!text || !text[0])
            return cbm_type_unknown();
        /* Decimal Int literals only. Suffixes, radix forms and overflow need
         * their own literal conversion rule, never a guessed Int. */
        uint64_t value = 0;
        for (const char *p = text; *p; p++) {
            if (*p < '0' || *p > '9' || value > (UINT64_C(2147483647) - (*p - '0')) / 10)
                return cbm_type_unknown();
            value = value * 10 + (unsigned)(*p - '0');
        }
        return kt_proof_scalar_type(context->source.arena, "Int");
    }
    if (kt_proof_is(node, "boolean_literal"))
        return kt_proof_scalar_type(context->source.arena, "Boolean");
    if (kt_proof_is(node, "string_literal"))
        return kt_proof_scalar_type(context->source.arena, "String");
    if (kt_proof_is(node, "simple_identifier")) {
        const char *name = kt_proof_text(&context->source, node);
        const kt_callable_fact_t *fact =
            kt_proof_callable(context, name, ts_node_start_byte(node), bound, blocked);
        if (fact)
            return kt_proof_signature(context, fact->identity.function);
        if (name && !(blocked && cbm_ht_get(blocked, name))) {
            const CBMType *type = cbm_ht_get(context->value_types, name);
            if (type)
                return type;
        }
    }
    cbm_resolve_method_result_t reference = kt_proof_reference(context, node, bound, blocked);
    return cbm_resolve_admit(&reference.result) && reference.function
               ? kt_proof_signature(context, reference.function)
               : cbm_type_unknown();
}
static bool kt_proof_arguments(kt_proof_context_t *context, TSNode args,
                               const CBMRegisteredFunc *target, CBMHashTable *bound,
                               CBMHashTable *blocked) {
    const CBMType *signature = kt_proof_signature(context, target);
    if (!target || !signature || signature->kind != CBM_TYPE_FUNC || target->min_params < 0 ||
        !(target->source_flags & CBM_SOURCE_DECL_FIXED_ARITY) ||
        (uint32_t)target->min_params != ts_node_named_child_count(args))
        return false;
    const CBMType *const *parameters = signature->data.func.param_types;
    for (int i = 0; i < target->min_params; i++) {
        TSNode argument = ts_node_named_child(args, (uint32_t)i);
        if (!kt_proof_is(argument, "value_argument") || ts_node_named_child_count(argument) != 1 ||
            !parameters || !parameters[i])
            return false;
        /* A spread marker is unnamed, so named-child count alone is not a
         * positional-argument proof. */
        const char *text = kt_proof_text(&context->source, argument);
        if (!text || text[0] == '*')
            return false;
        const CBMType *type =
            kt_proof_argument_type(context, ts_node_named_child(argument, 0), bound, blocked);
        if (!kt_proof_equal_type(type, parameters[i], 0))
            return false;
    }
    return !parameters || !parameters[target->min_params];
}
static bool kt_proof_property(kt_proof_context_t *context, TSNode node, const char **name,
                              TSNode *initializer, TSNode *annotation) {
    TSNode variable = kt_proof_child(node, "variable_declaration");
    TSNode identifier = kt_proof_child(variable, "simple_identifier");
    if (ts_node_is_null(identifier) || !ts_node_is_null(kt_proof_child(node, "modifiers")))
        return false;
    *annotation = kt_proof_child(variable, "function_type");
    uint32_t children = ts_node_named_child_count(variable);
    if (children != 1 && (children != 2 || ts_node_is_null(*annotation)))
        return false;
    bool immutable = false;
    *initializer = (TSNode){0};
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!strcmp(ts_node_type(child), "val"))
                immutable = true;
            if (kt_proof_is(child, "binding_pattern_kind")) {
                const char *binding = kt_proof_text(&context->source, child);
                immutable = binding && !strcmp(binding, "val");
                continue;
            }
            if (!ts_node_is_named(child) || kt_proof_is(child, "variable_declaration") ||
                kt_proof_is(child, "modifiers"))
                continue;
            if (!ts_node_is_null(*initializer)) {
                immutable = false;
                break;
            }
            *initializer = child;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    *name = kt_proof_text(&context->source, identifier);
    return immutable && *name && !ts_node_is_null(*initializer);
}
static cbm_resolve_method_candidate_t kt_proof_property_candidate(
    const cbm_resolve_method_query_t *query, const CBMTypeRegistry *owner,
    // WHY: The shared classify callback type requires a mutable void * context.
    // cppcheck-suppress constParameterCallback
    const CBMRegisteredFunc *candidate, void *opaque) {
    (void)query;
    (void)owner;
    const char *graph = opaque;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    const char *target = candidate->graph_qualified_name ? candidate->graph_qualified_name
                                                         : candidate->qualified_name;
    uint32_t required = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_TOP_LEVEL;
    if ((candidate->source_flags & required) != required ||
        !(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) || candidate->signature || !target ||
        !graph || strcmp(target, graph))
        return proof;
    proof.applicable = CBM_RESOLVE_METHOD_YES;
    proof.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    proof.graph_target = target;
    proof.graph_target_exact = true;
    proof.declaration_identity = candidate->declaration_identity;
    proof.declaration_identity_exact = candidate->declaration_identity != NULL;
    return proof;
}
static bool kt_proof_top_property(kt_proof_context_t *context, TSNode node, const char *name) {
    const char *key =
        cbm_arena_sprintf(context->source.arena, "%u:%s", ts_node_start_point(node).row + 1, name);
    const kt_proof_name_t *property = key ? cbm_ht_get(context->properties, key) : NULL;
    if (!property || property->ambiguous || !property->target)
        return false;
    const char *qn = kt_proof_join(&context->source, context->source.package, name);
    cbm_resolve_function_query_t query = {
        .proof_pool = &context->types.proofs,
        .proof_source = &context->types.proof_source,
        .lookup_domain = qn,
        .registry = context->registry,
        .function_qn = qn,
        .function_binding = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                             .rule = CBM_RESOLVE_RULE_LANGUAGE_BINDING,
                             .facts = CBM_RESOLVE_FACT_LANGUAGE_BOUND |
                                      CBM_RESOLVE_FACT_OCCURRENCE_EXACT},
        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT | CBM_RESOLVE_METHOD_NAME_IDENTITY,
        .classify = kt_proof_property_candidate,
        .context = (void *)property->target};
    cbm_resolve_method_result_t property_identity = cbm_resolve_function(&query);
    return cbm_resolve_admit(&property_identity.result);
}
static void kt_proof_bind_callable(kt_proof_context_t *context, TSNode node, bool top_level,
                                   CBMHashTable *bound, CBMHashTable *blocked) {
    const char *name = NULL;
    TSNode initializer, annotation;
    if (!kt_proof_property(context, node, &name, &initializer, &annotation) ||
        (bound && cbm_ht_get(bound, name)) ||
        (context->assignments && cbm_ht_get(context->assignments, name)) ||
        (top_level && !kt_proof_top_property(context, node, name)))
        return;
    if (!top_level) {
        TSNode parent = ts_node_parent(node), grandparent = ts_node_parent(parent);
        if (!kt_proof_is(parent, "statements") || !kt_proof_is(grandparent, "function_body"))
            return;
    }
    cbm_resolve_method_result_t identity = kt_proof_reference(context, initializer, bound, blocked);
    if (!cbm_resolve_admit(&identity.result))
        return;
    if (!ts_node_is_null(annotation)) {
        /* An annotation constrains the value; it cannot donate an identity.
         * Only the supported source type and the independently proved target
         * signature may establish that this immutable binding preserves it. */
        const CBMType *source_type =
            kt_source_type(context->source.arena, kt_proof_text(&context->source, annotation),
                           context->source.imports);
        const CBMType *declared = kt_proof_canonical_type(
            context, source_type, context->source.package, top_level || context->bare_calls, 0);
        if (!kt_proof_equal_type(declared, kt_proof_signature(context, identity.function), 0))
            return;
    }
    kt_callable_fact_t *fact = cbm_arena_calloc(context->source.arena, sizeof(*fact));
    if (!fact) {
        context->source.failed = true;
        return;
    }
    identity.result.rule = CBM_RESOLVE_RULE_CALLABLE_BINDING;
    identity.result.facts |= CBM_RESOLVE_FACT_CALLABLE_TYPED;
    identity.result = cbm_lsp_proof_rule(
        &context->types, identity.result, node, "kotlin-immutable-callable-binding", name,
        "immutable-val;exact-reference;annotation-compatible;complete-shadow-write-audit", NULL);
    if (!cbm_resolve_admit(&identity.result))
        return;
    fact->identity = identity;
    fact->available_after = top_level ? 0 : ts_node_end_byte(node);
    CBMHashTable *table = top_level ? context->top_callables : context->local_callables;
    cbm_ht_set(table, name, fact);
    if (cbm_ht_get(table, name) != fact)
        context->source.failed = true;
}
static void kt_proof_top_callables(kt_proof_context_t *context, TSNode root) {
    context->bare_calls = true;
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (kt_proof_is(child, "property_declaration"))
                kt_proof_bind_callable(context, child, true, NULL, NULL);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
static void kt_proof_call(kt_proof_context_t *context, TSNode node, const char *caller,
                          CBMScope *scope, CBMHashTable *bound, CBMHashTable *blocked) {
    TSNode suffix = kt_proof_child(node, "call_suffix"),
           args = kt_proof_child(suffix, "value_arguments");
    if (ts_node_is_null(args) || ts_node_named_child_count(suffix) != 1)
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
        cbm_resolve_method_query_t query = {.proof_pool = &context->types.proofs,
                                            .proof_source = &context->types.proof_source,
                                            .lookup_domain = fact.type->data.named.qualified_name,
                                            .registry = context->registry,
                                            .receiver = &fact,
                                            .receiver_qn = fact.type->data.named.qualified_name,
                                            .method_name = name,
                                            .argument_count = 0,
                                            .adapter_facts = domain,
                                            .classify = kt_proof_candidate};
        resolved = cbm_resolve_method(&query);
    } else if (kt_proof_is(target, "simple_identifier")) {
        const char *name = kt_proof_text(&context->source, target);
        const kt_callable_fact_t *fact =
            kt_proof_callable(context, name, ts_node_start_byte(node), bound, blocked);
        resolved =
            fact ? fact->identity : kt_proof_function_identity(context, name, bound, blocked);
        if (fact) {
            resolved.result.rule = CBM_RESOLVE_RULE_CALLABLE_BINDING;
            resolved.result.facts |= CBM_RESOLVE_FACT_CALLABLE_TYPED;
        }
    }
    /* Name identity is settled over all declarations before applicability;
     * argument types never select a convenient overload. */
    if (cbm_resolve_admit(&resolved.result) &&
        kt_proof_arguments(context, args, resolved.function, bound, blocked)) {
        resolved.result = cbm_lsp_proof_rule(
            &context->types, resolved.result, args, "kotlin-fixed-positional-arguments",
            kt_proof_text(&context->source, args),
            "unique-name-before-applicability;fixed-arity;source-types-compatible", NULL);
        kt_proof_emit(context, caller, node, CBM_RESOLVED_INVOCATION, resolved.result);
    }
}

static void kt_proof_body_calls(kt_proof_context_t *context, TSNode node, const char *caller,
                                CBMScope *scope, CBMHashTable *bound, CBMHashTable *blocked,
                                unsigned depth) {
    if (!kt_proof_budget(&context->source, depth))
        return;
    if (kt_proof_is(node, "property_declaration"))
        kt_proof_bind_callable(context, node, false, bound, blocked);
    if (kt_proof_is(node, "call_expression"))
        kt_proof_call(context, node, caller, scope, bound, blocked);
    if (kt_proof_is(node, "callable_reference")) {
        cbm_resolve_method_result_t reference = kt_proof_reference(context, node, bound, blocked);
        /* Extraction records the identifier leaf of explicit ::name syntax,
         * so semantic proof must use that same exact occurrence span. */
        kt_proof_emit(context, caller, ts_node_named_child(node, 0), CBM_RESOLVED_CALL_REFERENCE,
                      reference.result);
    }
    if (kt_proof_is(node, "value_argument") && ts_node_named_child_count(node) == 1) {
        TSNode value = kt_proof_unwrap(ts_node_named_child(node, 0));
        if (kt_proof_is(value, "simple_identifier")) {
            const char *name = kt_proof_text(&context->source, value);
            const kt_callable_fact_t *fact =
                kt_proof_callable(context, name, ts_node_start_byte(value), bound, blocked);
            if (fact) {
                cbm_resolve_result_t proof = fact->identity.result;
                proof.rule = CBM_RESOLVE_RULE_CALLABLE_BINDING;
                proof.facts |= CBM_RESOLVE_FACT_CALLABLE_TYPED;
                kt_proof_emit(context, caller, value, CBM_RESOLVED_CALL_REFERENCE, proof);
            }
        }
    }
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
    context->assignments = cbm_ht_create(16);
    context->local_callables = cbm_ht_create(16);
    context->value_types = cbm_ht_create(16);
    if (!blocked || !bound || !context->assignments || !context->local_callables ||
        !context->value_types) {
        cbm_ht_free(blocked);
        cbm_ht_free(bound);
        cbm_ht_free(context->assignments);
        cbm_ht_free(context->local_callables);
        cbm_ht_free(context->value_types);
        context->assignments = context->local_callables = context->value_types = NULL;
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
                if (ts_node_is_null(type_node))
                    type_node = kt_proof_child(parameter, "function_type");
                const char *spelling = kt_proof_text(&context->source, type_node);
                /* A direct built-in scalar spelling has its own exact shadow
                 * checks below. Unrelated function imports do not change it.
                 * Function-type components still require separate lookup. */
                bool imported_components =
                    context->source.imports && !kt_proof_is(type_node, "user_type");
                const CBMType *source_type =
                    kt_source_type(context->source.arena, spelling, imported_components);
                /* Imports and local type names can shadow implicit Kotlin
                 * built-ins; only a complete unshadowed type domain is used. */
                if (spelling && (context->wildcard || cbm_ht_get(context->imports, spelling) ||
                                 cbm_ht_get(context->local_types, spelling)))
                    source_type = cbm_type_unknown();
                const CBMType *value_type = kt_proof_canonical_type(
                    context, source_type, context->source.package, top_level, 0);
                if (!cbm_type_is_unknown(value_type))
                    cbm_ht_set(context->value_types, variable, (void *)value_type);
                const CBMRegisteredType *type = kt_proof_type_name(context, spelling);
                if (!type)
                    continue;
                cbm_resolve_result_t proof = kt_proof_type_evidence(context, type, type_node);
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
    cbm_ht_free(context->assignments);
    cbm_ht_free(context->local_callables);
    cbm_ht_free(context->value_types);
    context->assignments = context->local_callables = context->value_types = NULL;
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
    /* Append through a private descriptor; publish only after complete analysis.
     * Arena growth preserves the original buffer and its committed prefix. */
    CBMResolvedCallArray staged = *out;
    kt_proof_context_t context = {
        .source = {.arena = arena, .source = source, .length = (size_t)length},
        .registry = registry,
        .file = file,
        .out = &staged};
    context.imports = cbm_ht_create(32);
    context.callers = cbm_ht_create(32);
    context.local_types = cbm_ht_create(32);
    context.top_callables = cbm_ht_create(32);
    context.properties = cbm_ht_create(32);
    context.top_types = cbm_ht_create(32);
    cbm_resolve_types_init(&context.types, arena, 0);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, file->module_qn);
    if (!cbm_lsp_proof_context(&context.types, &proof_source, file->module_qn))
        context.source.failed = true;
    if (context.imports && context.callers && context.local_types && context.top_callables &&
        context.properties && context.top_types && kt_proof_prepare(&context, root)) {
        kt_proof_top_callables(&context, root);
        kt_proof_walk(&context, root, 0);
    }
    if (!context.source.failed)
        *out = staged;
    cbm_ht_free(context.imports);
    cbm_ht_free(context.callers);
    cbm_ht_free(context.local_types);
    cbm_ht_free(context.top_callables);
    cbm_ht_free(context.properties);
    cbm_ht_free(context.top_types);
}
