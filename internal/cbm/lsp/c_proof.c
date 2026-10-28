#include "c_proof.h"
#include "c_lsp.h"
#include <ctype.h>
#include "../callable_sig.h"
#include "foundation/hash_table.h"
#include <string.h>

typedef struct {
    CBMLSPDef *definition;
    bool duplicate;
} csp_entry_t;
typedef struct {
    CBMArena *arena;
    const char *source;
    uint32_t length, visited;
    CBMHashTable *definitions;
    bool failed;
    bool extern_c;
    bool preprocessor_unknown;
    const char *module;
} csp_context_t;

static bool csp_is(TSNode node, const char *kind) {
    return !ts_node_is_null(node) && !strcmp(ts_node_type(node), kind);
}
static TSNode csp_field(TSNode node, const char *name) {
    return ts_node_is_null(node) ? (TSNode){0}
                                 : ts_node_child_by_field_name(node, name, (uint32_t)strlen(name));
}
static const char *csp_text(csp_context_t *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    uint32_t start = ts_node_start_byte(node), end = ts_node_end_byte(node);
    return end > start && end <= ctx->length
               ? cbm_arena_strndup(ctx->arena, ctx->source + start, end - start)
               : NULL;
}
static bool csp_budget(csp_context_t *ctx, unsigned depth) {
    if (++ctx->visited > 1000000 || depth > 128)
        ctx->failed = true;
    return !ctx->failed;
}
static csp_entry_t *csp_entry(csp_context_t *ctx, const char *qn, uint32_t line) {
    const char *key = qn ? cbm_arena_sprintf(ctx->arena, "%u:%s", line, qn) : NULL;
    if (!key) {
        ctx->failed = true;
        return NULL;
    }
    return cbm_ht_get(ctx->definitions, key);
}
static void csp_mark(csp_context_t *ctx, TSNode node, const char *qn, uint32_t flags, int arity) {
    csp_entry_t *entry = csp_entry(ctx, qn, ts_node_start_point(node).row + 1);
    if (!entry || entry->duplicate)
        return;
    entry->definition->source_flags = flags;
    if (arity >= 0 && entry->definition->signature_param_count == arity)
        entry->definition->source_flags |= CBM_SOURCE_DECL_FIXED_ARITY;
}
static TSNode csp_function_declarator(TSNode node) {
    TSNode decl = csp_field(node, "declarator");
    for (unsigned depth = 0; depth < 32 && !ts_node_is_null(decl); depth++) {
        if (csp_is(decl, "function_declarator"))
            return decl;
        if (!csp_is(decl, "pointer_declarator") && !csp_is(decl, "reference_declarator"))
            break;
        decl = csp_field(decl, "declarator");
    }
    return (TSNode){0};
}
static int csp_arity(csp_context_t *ctx, TSNode declarator) {
    TSNode params = csp_field(declarator, "parameters");
    if (ts_node_is_null(params))
        return -1;
    int count = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(params);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child) || csp_is(child, "comment"))
                continue;
            if (!csp_budget(ctx, 0) || !csp_is(child, "parameter_declaration")) {
                count = -1;
                break;
            }
            if (ts_node_is_null(csp_field(child, "declarator"))) {
                const char *type = csp_text(ctx, csp_field(child, "type"));
                if (type && !strcmp(type, "void") && ts_node_named_child_count(params) == 1)
                    continue;
            }
            count++;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return count;
}
/* Raw directives can rename either a callable or a builtin type. The adapter
 * does not borrow authority from a different preprocessor configuration. */
bool cbm_c_preprocessor_domain_unsupported(TSNode root) {
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    uint32_t visited = 0;
    bool unsupported = false;
    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (++visited > 1000000 || !strncmp(ts_node_type(node), "preproc_", 8)) {
            unsupported = true;
            break;
        }
        if (ts_tree_cursor_goto_first_child(&cursor))
            continue;
        while (!ts_tree_cursor_goto_next_sibling(&cursor)) {
            if (!ts_tree_cursor_goto_parent(&cursor))
                goto done;
        }
    }
done:
    ts_tree_cursor_delete(&cursor);
    return unsupported;
}

/* Keep source spelling conservative: equivalent but differently spelled builtin
 * types can miss, while typedefs, auto and user-defined names never qualify. */
const CBMType *cbm_c_builtin_annotation(CBMArena *arena, const char *source, TSNode type) {
    if (!arena || !source || ts_node_is_null(type) || ts_node_has_error(type) ||
        (!csp_is(type, "primitive_type") && !csp_is(type, "sized_type_specifier")))
        return NULL;
    uint32_t start = ts_node_start_byte(type), end = ts_node_end_byte(type);
    if (end <= start || end - start > 64)
        return NULL;
    char text[65];
    size_t used = 0;
    bool space = false;
    for (uint32_t i = start; i < end; i++) {
        if (isspace((unsigned char)source[i])) {
            space = used != 0;
        } else {
            if (space)
                text[used++] = ' ';
            space = false;
            text[used++] = source[i];
        }
    }
    text[used] = '\0';
    static const char *const supported[] = {"void",
                                            "bool",
                                            "_Bool",
                                            "char",
                                            "signed char",
                                            "unsigned char",
                                            "short",
                                            "short int",
                                            "signed short",
                                            "signed short int",
                                            "unsigned short",
                                            "unsigned short int",
                                            "int",
                                            "signed",
                                            "signed int",
                                            "unsigned",
                                            "unsigned int",
                                            "long",
                                            "long int",
                                            "signed long",
                                            "signed long int",
                                            "unsigned long",
                                            "unsigned long int",
                                            "long long",
                                            "long long int",
                                            "signed long long",
                                            "signed long long int",
                                            "unsigned long long",
                                            "unsigned long long int",
                                            "float",
                                            "double",
                                            "long double",
                                            "wchar_t",
                                            "char8_t",
                                            "char16_t",
                                            "char32_t"};
    for (size_t i = 0; i < sizeof(supported) / sizeof(supported[0]); i++)
        if (!strcmp(text, supported[i]))
            return cbm_type_builtin(arena, cbm_arena_strdup(arena, text));
    return NULL;
}

const CBMType *cbm_c_free_signature(CBMArena *arena, const char *source, TSNode node,
                                    bool cpp_mode) {
    TSNode decl = csp_field(node, "declarator"), type = csp_field(node, "type");
    if (!csp_is(decl, "function_declarator") ||
        !csp_is(csp_field(decl, "declarator"), "identifier") || ts_node_has_error(node))
        return NULL;
    const CBMType *result = cbm_c_builtin_annotation(arena, source, type);
    if (!result)
        return NULL;
    uint32_t children = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < children; i++) {
        TSNode child = ts_node_named_child(node, i);
        if (ts_node_eq(child, type) || ts_node_eq(child, decl) ||
            csp_is(child, "compound_statement") || csp_is(child, "comment"))
            continue;
        if (csp_is(child, "storage_class_specifier")) {
            uint32_t start = ts_node_start_byte(child), length = ts_node_end_byte(child) - start;
            if ((length == 6 && !memcmp(source + start, "extern", 6)) ||
                (length == 6 && !memcmp(source + start, "static", 6)))
                continue;
        }
        return NULL;
    }
    TSNode params = csp_field(decl, "parameters");
    uint32_t count = ts_node_named_child_count(params);
    if (ts_node_is_null(params) || (!count && !cpp_mode) || count > 64 ||
        ts_node_named_child_count(decl) != 2)
        return NULL;
    const CBMType **types = cbm_arena_calloc(arena, (count + 1) * sizeof(*types));
    const CBMType **returns = cbm_arena_calloc(arena, 2 * sizeof(*returns));
    if (!types || !returns)
        return NULL;
    uint32_t used = 0;
    for (uint32_t i = 0; i < count; i++) {
        TSNode param = ts_node_named_child(params, i);
        if (csp_is(param, "comment"))
            continue;
        TSNode named = csp_field(param, "declarator");
        const CBMType *param_type =
            cbm_c_builtin_annotation(arena, source, csp_field(param, "type"));
        if (!csp_is(param, "parameter_declaration") || !param_type ||
            (!ts_node_is_null(named) && !csp_is(named, "identifier")) ||
            ts_node_named_child_count(param) != (ts_node_is_null(named) ? 1u : 2u))
            return NULL;
        if (!strcmp(param_type->data.builtin.name, "void")) {
            if (count != 1 || !ts_node_is_null(named))
                return NULL;
        } else {
            types[used++] = param_type;
        }
    }
    returns[0] = result;
    return cbm_type_func(arena, NULL, types, returns);
}

static void csp_free_function(csp_context_t *ctx, TSNode node, const char *owner) {
    TSNode decl = csp_function_declarator(node), name_node = csp_field(decl, "declarator");
    if (!csp_is(name_node, "identifier"))
        return;
    const char *name = csp_text(ctx, name_node);
    const char *qn = name ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name) : NULL;
    csp_entry_t *entry = csp_entry(ctx, qn, ts_node_start_point(node).row + 1);
    if (!entry || entry->duplicate)
        return;
    CBMLSPDef *def = entry->definition;
    bool cpp = def->lang == CBM_LANG_CPP || def->lang == CBM_LANG_CUDA;
    def->source_flags = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_TOP_LEVEL;
    if (csp_is(node, "function_definition"))
        def->source_flags |= CBM_SOURCE_FUNCTION_DEFINITION;
    if (!cpp || ctx->extern_c)
        def->source_flags |= CBM_SOURCE_DECL_EXTERN_C;
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *text = csp_is(child, "storage_class_specifier") ? csp_text(ctx, child) : NULL;
        if (text && !strcmp(text, "static"))
            def->source_flags |= CBM_SOURCE_DECL_STATIC;
    }
    size_t module_len = strlen(ctx->module);
    def->namespace_name = !strcmp(owner, ctx->module) ? ""
                          : !strncmp(owner, ctx->module, module_len) && owner[module_len] == '.'
                              ? owner + module_len + 1
                              : NULL;
    def->source_signature =
        ctx->preprocessor_unknown ? NULL : cbm_c_free_signature(ctx->arena, ctx->source, node, cpp);
    if (def->source_signature)
        def->source_flags |= CBM_SOURCE_DECL_FIXED_ARITY;
}

static void csp_method(csp_context_t *ctx, TSNode node, const char *owner, bool is_public) {
    TSNode decl = csp_function_declarator(node), name_node = csp_field(decl, "declarator");
    if (!csp_is(name_node, "field_identifier") && !csp_is(name_node, "identifier"))
        return; /* Pointer fields, operators and constructors need their own contracts. */
    const char *name = csp_text(ctx, name_node);
    const char *qn = name ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name) : NULL;
    uint32_t flags =
        CBM_SOURCE_DECL_KNOWN | (is_public ? CBM_SOURCE_DECL_PUBLIC : CBM_SOURCE_DECL_PRIVATE);
    csp_mark(ctx, node, qn, flags, csp_arity(ctx, decl));
}
static void csp_class(csp_context_t *ctx, TSNode node, const char *parent) {
    TSNode body = csp_field(node, "body"), name_node = csp_field(node, "name");
    if (ts_node_is_null(body) || !csp_is(name_node, "type_identifier"))
        return;
    const char *name = csp_text(ctx, name_node);
    const char *qn = name ? cbm_arena_sprintf(ctx->arena, "%s.%s", parent, name) : NULL;
    if (!qn) {
        ctx->failed = true;
        return;
    }
    bool complete = true, is_public = csp_is(node, "struct_specifier");
    TSTreeCursor header = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&header))
        do {
            TSNode child = ts_tree_cursor_current_node(&header);
            if (csp_is(child, "base_class_clause") || csp_is(child, "attribute_specifier") ||
                csp_is(child, "ms_declspec_modifier"))
                complete = false;
        } while (ts_tree_cursor_goto_next_sibling(&header));
    ts_tree_cursor_delete(&header);
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!csp_budget(ctx, 0)) {
                complete = false;
                break;
            }
            if (!ts_node_is_named(child) || csp_is(child, "comment"))
                continue;
            if (csp_is(child, "access_specifier")) {
                const char *access = csp_text(ctx, child);
                is_public = access && !strcmp(access, "public");
            } else if (csp_is(child, "function_definition") || csp_is(child, "field_declaration") ||
                       csp_is(child, "declaration")) {
                csp_method(ctx, child, qn, is_public);
            } else {
                complete = false;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    csp_mark(ctx, node, qn,
             CBM_SOURCE_DECL_KNOWN | (complete ? CBM_SOURCE_CLASS_DOMAIN_COMPLETE : 0), -1);
}
static void csp_walk(csp_context_t *ctx, TSNode node, const char *owner, unsigned depth) {
    if (!csp_budget(ctx, depth))
        return;
    if (csp_is(node, "linkage_specification")) {
        const char *value = csp_text(ctx, csp_field(node, "value"));
        TSNode body = csp_field(node, "body");
        if (value && !strcmp(value, "\"C\"") && !ts_node_is_null(body)) {
            bool saved = ctx->extern_c;
            ctx->extern_c = true;
            csp_walk(ctx, body, owner, depth + 1);
            ctx->extern_c = saved;
        }
        return;
    }
    if (csp_is(node, "function_definition") || csp_is(node, "declaration")) {
        csp_free_function(ctx, node, owner);
        if (csp_is(node, "function_definition"))
            return;
    }
    if (csp_is(node, "class_specifier") || csp_is(node, "struct_specifier")) {
        csp_class(ctx, node, owner);
        return;
    }
    if (csp_is(node, "namespace_definition")) {
        const char *name = csp_text(ctx, csp_field(node, "name"));
        TSNode body = csp_field(node, "body");
        if (name && !strstr(name, "::") && !ts_node_is_null(body)) {
            const char *qualified = cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name);
            if (qualified)
                csp_walk(ctx, body, qualified, depth + 1);
            else
                ctx->failed = true;
        }
        return;
    }
    if (!csp_is(node, "translation_unit") && !csp_is(node, "declaration_list") &&
        !csp_is(node, "declaration"))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                csp_walk(ctx, child, owner, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}
bool cbm_c_collect_source_facts(CBMArena *arena, const char *source, int length, const char *module,
                                CBMLSPDef *defs, int count) {
    if (count < 0 || (count && !defs))
        return false;
    for (int i = 0; i < count; i++) {
        defs[i].source_flags = 0;
        defs[i].source_signature = NULL;
    }
    if (!arena || !source || length <= 0 || !module)
        return false;
    extern const TSLanguage *tree_sitter_cpp(void);
    TSParser *parser = ts_parser_new();
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_cpp());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    csp_context_t ctx = {.arena = arena,
                         .source = source,
                         .length = (uint32_t)length,
                         .module = module,
                         .definitions = cbm_ht_create(32)};
    ctx.failed = !ctx.definitions || ts_node_has_error(ts_tree_root_node(tree));
    ctx.preprocessor_unknown = cbm_c_preprocessor_domain_unsupported(ts_tree_root_node(tree));
    for (int i = 0; !ctx.failed && i < count; i++) {
        CBMLSPDef *def = &defs[i];
        const char *qn =
            def->graph_qualified_name ? def->graph_qualified_name : def->qualified_name;
        if (!qn || !def->source_start_line || !def->short_name)
            continue;
        size_t base = cbm_qn_callable_base_len_named(qn, def->short_name);
        const char *key =
            cbm_arena_sprintf(arena, "%u:%.*s", def->source_start_line, (int)base, qn);
        csp_entry_t *entry = key ? cbm_ht_get(ctx.definitions, key) : NULL;
        if (entry) {
            entry->duplicate = true;
            continue;
        }
        entry = cbm_arena_calloc(arena, sizeof(*entry));
        if (!key || !entry) {
            ctx.failed = true;
            break;
        }
        entry->definition = def;
        cbm_ht_set(ctx.definitions, key, entry);
        if (cbm_ht_get(ctx.definitions, key) != entry)
            ctx.failed = true;
    }
    if (!ctx.failed)
        csp_walk(&ctx, ts_tree_root_node(tree), module, 0);
    if (ctx.failed)
        for (int i = 0; i < count; i++)
            defs[i].source_flags = 0;
    cbm_ht_free(ctx.definitions);
    ts_tree_delete(tree);
    return !ctx.failed;
}
