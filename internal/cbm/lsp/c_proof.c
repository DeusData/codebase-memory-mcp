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
    bool conditional;
    /* C has one linkage for every name. A plain .h is parsed by the C++ grammar
     * like everything here, but its language is its includer's. */
    bool c_language;
    /* `#ifdef __cplusplus extern "C" { #endif`: the brace the grammar cannot pair. */
    bool file_extern_c;
    const char *module;
    CBMCFileFacts *facts;
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
    entry->definition->source_arity = arity >= 0 ? arity + 1 : 0;
}
static TSNode csp_function_declarator(TSNode node) {
    TSNode decl = csp_field(node, "declarator");
    for (unsigned depth = 0; depth < 32 && !ts_node_is_null(decl); depth++) {
        if (csp_is(decl, "function_declarator"))
            return decl;
        if (csp_is(decl, "reference_declarator")) {
            /* `T &f()`: the grammar gives this declarator's inner part no field name. */
            uint32_t count = ts_node_named_child_count(decl);
            decl = count ? ts_node_named_child(decl, count - 1) : (TSNode){0};
            continue;
        }
        if (!csp_is(decl, "pointer_declarator"))
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
/* A signature compares builtin spellings, so only a macro that respells a
 * builtin type word can falsify one. Conditionals and ordinary macros change
 * which declarations exist, which the declaration facts record per name. */
static bool csp_builtin_word(const char *text, uint32_t length) {
    static const char *const words[] = {"void",     "bool",    "_Bool",   "char",     "short",
                                        "int",      "long",    "float",   "double",   "signed",
                                        "unsigned", "wchar_t", "char8_t", "char16_t", "char32_t"};
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++)
        if (strlen(words[i]) == length && !memcmp(text, words[i], length))
            return true;
    return false;
}

bool cbm_c_preprocessor_domain_unsupported(TSNode root, const char *source) {
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    uint32_t visited = 0;
    bool unsupported = false;
    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        if (++visited > 1000000) {
            unsupported = true;
            break;
        }
        if (!strcmp(kind, "preproc_def") || !strcmp(kind, "preproc_function_def")) {
            TSNode name = ts_node_child_by_field_name(node, "name", 4);
            if (!ts_node_is_null(name) &&
                csp_builtin_word(source + ts_node_start_byte(name),
                                 ts_node_end_byte(name) - ts_node_start_byte(name))) {
                unsupported = true;
                break;
            }
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

static CBMCDecl *csp_add_decl(csp_context_t *ctx, CBMCDeclKind kind, const char *name,
                              uint32_t flags) {
    CBMCFileFacts *facts = ctx->facts;
    if (!facts || !name)
        return NULL;
    if (facts->decl_count == facts->decl_cap) {
        int cap = facts->decl_cap ? facts->decl_cap * 2 : 16;
        CBMCDecl *next = cbm_arena_alloc(ctx->arena, (size_t)cap * sizeof(*next));
        if (!next) {
            ctx->failed = true;
            return NULL;
        }
        if (facts->decl_count)
            memcpy(next, facts->decls, (size_t)facts->decl_count * sizeof(*next));
        facts->decls = next;
        facts->decl_cap = cap;
    }
    CBMCDecl *decl = &facts->decls[facts->decl_count++];
    *decl = (CBMCDecl){.file = facts->file_qn ? facts->file_qn : ctx->module,
                       .name = name,
                       .arity = -1,
                       .kind = kind,
                       .flags = flags | (ctx->conditional ? CBM_C_DECL_CONDITIONAL : 0)};
    return decl;
}

static bool csp_storage_is(csp_context_t *ctx, TSNode node, const char *storage) {
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        const char *text = csp_is(child, "storage_class_specifier") ? csp_text(ctx, child) : NULL;
        if (text && !strcmp(text, storage))
            return true;
    }
    return false;
}

/* Name relative to the file: a namespace is part of a C++ linkage key, the file
 * module never is. */
static const char *csp_relative(csp_context_t *ctx, const char *owner, const char *name) {
    size_t module_len = strlen(ctx->module);
    if (!strcmp(owner, ctx->module))
        return name;
    if (!strncmp(owner, ctx->module, module_len) && owner[module_len] == '.')
        return cbm_arena_sprintf(ctx->arena, "%s.%s", owner + module_len + 1, name);
    return NULL;
}

/* A parameter list ending in `...` is not a fixed count, and `()` in C is not a
 * prototype at all: it says nothing of the parameters. */
static int csp_fixed_arity(csp_context_t *ctx, TSNode decl) {
    TSNode params = csp_field(decl, "parameters");
    if (ts_node_is_null(params) || !ts_node_named_child_count(params))
        return -1;
    uint32_t count = ts_node_child_count(params);
    for (uint32_t i = 0; i < count; i++)
        if (csp_is(ts_node_child(params, i), "variadic_parameter") ||
            csp_is(ts_node_child(params, i), "..."))
            return -1;
    return csp_arity(ctx, decl);
}

static void csp_note_prototype(csp_context_t *ctx, TSNode node, TSNode decl, const char *owner,
                               const char *name) {
    bool is_static = csp_storage_is(ctx, node, "static");
    uint32_t flags = (is_static ? 0 : CBM_C_DECL_EXTERNAL) |
                     (ctx->extern_c || ctx->file_extern_c ? CBM_C_DECL_C_LINKAGE : 0);
    CBMCDecl *fact = csp_add_decl(ctx, CBM_C_DECL_PROTOTYPE, csp_relative(ctx, owner, name), flags);
    if (!fact)
        return;
    fact->arity = csp_fixed_arity(ctx, decl);
    fact->signature = ctx->preprocessor_unknown
                          ? NULL
                          : cbm_c_free_signature(ctx->arena, ctx->source, node, !ctx->c_language);
}

/* `struct Tag`, `Tag *`, `const Tag`: the name a member access needs. */
static const char *csp_base_type_name(csp_context_t *ctx, TSNode type) {
    if (csp_is(type, "struct_specifier") || csp_is(type, "union_specifier") ||
        csp_is(type, "class_specifier"))
        type = csp_field(type, "name");
    return csp_is(type, "type_identifier") ? csp_text(ctx, type) : NULL;
}

/* Enumerators sit in file scope whichever declaration wrote them, with no linkage. */
static void csp_note_enumerators(csp_context_t *ctx, TSNode type, const char *owner) {
    if (!ctx->facts || !csp_is(type, "enum_specifier") || strcmp(owner, ctx->module))
        return;
    TSNode list = csp_field(type, "body");
    uint32_t count = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
    for (uint32_t i = 0; i < count; i++) {
        TSNode item = ts_node_named_child(list, i);
        const char *name =
            csp_is(item, "enumerator") ? csp_text(ctx, csp_field(item, "name")) : NULL;
        CBMCDecl *fact = name ? csp_add_decl(ctx, CBM_C_DECL_VALUE, name, 0) : NULL;
        if (fact)
            fact->target = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module, name);
    }
}

/* Objects, not functions: `extern struct redisServer server;` has no graph node
 * of its own, and a file-scope `static` or enumerator has no external linkage. */
static void csp_note_values(csp_context_t *ctx, TSNode node, const char *owner) {
    if (!ctx->facts || strcmp(owner, ctx->module))
        return;
    csp_note_enumerators(ctx, csp_field(node, "type"), owner);
    bool is_extern = csp_storage_is(ctx, node, "extern");
    bool is_static = csp_storage_is(ctx, node, "static");
    const char *type_name = csp_base_type_name(ctx, csp_field(node, "type"));
    /* `EXTERN buf_T *curbuf;` reads as type EXTERN with buf_T stranded in an ERROR:
     * a storage macro in front of the real type. */
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        if (csp_is(child, "ERROR") && ts_node_named_child_count(child) == 1 &&
            csp_is(ts_node_named_child(child, 0), "identifier"))
            type_name = csp_text(ctx, ts_node_named_child(child, 0));
    }
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (!field || strcmp(field, "declarator"))
                continue;
            TSNode declarator = ts_tree_cursor_current_node(&cursor);
            bool initialized = csp_is(declarator, "init_declarator");
            for (unsigned depth = 0; depth < 16 && !ts_node_is_null(declarator); depth++) {
                if (csp_is(declarator, "function_declarator"))
                    break;
                if (csp_is(declarator, "identifier")) {
                    const char *name = csp_text(ctx, declarator);
                    CBMCDecl *fact =
                        is_extern && !initialized
                            ? csp_add_decl(ctx, CBM_C_DECL_EXTERN_VALUE, name, CBM_C_DECL_EXTERNAL)
                            : csp_add_decl(ctx, CBM_C_DECL_VALUE, name,
                                           is_static ? 0 : CBM_C_DECL_EXTERNAL);
                    if (fact) {
                        fact->type_name = type_name;
                        fact->target = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module, name);
                    }
                    break;
                }
                declarator = csp_field(declarator, "declarator");
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

static void csp_note_macro(csp_context_t *ctx, TSNode node) {
    TSNode name = csp_field(node, "name");
    if (!csp_is(name, "identifier"))
        return;
    const char *text = csp_text(ctx, name);
    if (text && !(ctx->facts && ctx->facts->guard && !strcmp(ctx->facts->guard, text)))
        (void)csp_add_decl(ctx, CBM_C_DECL_MACRO, text, 0);
}

static void csp_free_function(csp_context_t *ctx, TSNode node, const char *owner) {
    TSNode decl = csp_function_declarator(node), name_node = csp_field(decl, "declarator");
    if (!csp_is(name_node, "identifier")) {
        if (csp_is(node, "declaration"))
            csp_note_values(ctx, node, owner);
        return;
    }
    const char *name = csp_text(ctx, name_node);
    const char *qn = name ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name) : NULL;
    if (csp_is(node, "declaration") && name && !ts_node_has_error(decl))
        csp_note_prototype(ctx, node, decl, owner, name);
    csp_entry_t *entry = csp_entry(ctx, qn, ts_node_start_point(node).row + 1);
    /* A macro over a builtin type word (`#define int double`) falsifies what the
     * signature below would say, so the definition's linkage facts stay unknown. */
    if (!entry || entry->duplicate || ctx->preprocessor_unknown)
        return;
    CBMLSPDef *def = entry->definition;
    bool cpp = def->lang == CBM_LANG_CPP || def->lang == CBM_LANG_CUDA;
    def->source_flags = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_TOP_LEVEL;
    if (ctx->conditional)
        def->source_flags |= CBM_SOURCE_DECL_CONDITIONAL;
    if (csp_is(node, "function_definition"))
        def->source_flags |= CBM_SOURCE_FUNCTION_DEFINITION;
    if (!cpp || ctx->extern_c || ctx->file_extern_c)
        def->source_flags |= CBM_SOURCE_DECL_EXTERN_C;
    if (csp_storage_is(ctx, node, "static"))
        def->source_flags |= CBM_SOURCE_DECL_STATIC;
    int arity = csp_fixed_arity(ctx, decl);
    def->source_arity = arity >= 0 ? arity + 1 : 0;
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
/* A struct inside a preprocessor branch stays unknown: unlike a function, whose
 * callers cannot compile without it, a branch is how a project swaps one layout
 * for another, and member lookup cannot tell which one the includer gets. */
static void csp_class(csp_context_t *ctx, TSNode node, const char *parent,
                      const char *typedef_name) {
    TSNode body = csp_field(node, "body"), name_node = csp_field(node, "name");
    if (ts_node_is_null(body) || (!csp_is(name_node, "type_identifier") && !typedef_name) ||
        ctx->conditional)
        return;
    const char *name =
        csp_is(name_node, "type_identifier") ? csp_text(ctx, name_node) : typedef_name;
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
static bool csp_preprocessor_branch(TSNode node) {
    return csp_is(node, "preproc_if") || csp_is(node, "preproc_ifdef") ||
           csp_is(node, "preproc_else") || csp_is(node, "preproc_elif") ||
           csp_is(node, "preproc_elifdef");
}

static void csp_walk(csp_context_t *ctx, TSNode node, const char *owner, unsigned depth);

static void csp_walk_children(csp_context_t *ctx, TSNode node, const char *owner, unsigned depth) {
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                csp_walk(ctx, child, owner, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

/* The only code a guard's condition can hide is a header the caller suppressed
 * on purpose: declarations inside one exist in every ordinary configuration.
 * Any other branch records that its declarations are conditional. */
static TSNode csp_include_guard(csp_context_t *ctx, TSNode root) {
    TSNode guard = {0};
    uint32_t count = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(root, i);
        if (csp_is(child, "comment"))
            continue;
        if (!ts_node_is_null(guard) || !csp_is(child, "preproc_ifdef"))
            return (TSNode){0};
        guard = child;
    }
    if (ts_node_is_null(guard))
        return guard;
    TSNode keyword = ts_node_child(guard, 0);
    TSNode name = csp_field(guard, "name");
    if (ts_node_is_null(keyword) || !csp_is(name, "identifier") ||
        ts_node_end_byte(keyword) - ts_node_start_byte(keyword) != 7 ||
        memcmp(ctx->source + ts_node_start_byte(keyword), "#ifndef", 7) ||
        !ts_node_is_null(csp_field(guard, "alternative")))
        return (TSNode){0};
    uint32_t kids = ts_node_named_child_count(guard);
    for (uint32_t i = 0; i < kids; i++) {
        TSNode child = ts_node_named_child(guard, i);
        if (csp_is(child, "comment") || ts_node_eq(child, name))
            continue;
        const char *defined =
            csp_is(child, "preproc_def") ? csp_text(ctx, csp_field(child, "name")) : NULL;
        const char *guarded = csp_text(ctx, name);
        return defined && guarded && !strcmp(defined, guarded) ? guard : (TSNode){0};
    }
    return (TSNode){0};
}

static void csp_walk(csp_context_t *ctx, TSNode node, const char *owner, unsigned depth) {
    if (!csp_budget(ctx, depth))
        return;
    if (ctx->facts && (csp_is(node, "namespace_definition") || csp_is(node, "class_specifier") ||
                       csp_is(node, "template_declaration")))
        ctx->facts->cpp_syntax = true;
    if (csp_is(node, "preproc_def") || csp_is(node, "preproc_function_def")) {
        csp_note_macro(ctx, node);
        return;
    }
    if (csp_preprocessor_branch(node)) {
        bool saved = ctx->conditional;
        ctx->conditional = true;
        csp_walk_children(ctx, node, owner, depth);
        ctx->conditional = saved;
        return;
    }
    /* Error recovery around a directive still leaves its neighbours as complete
     * nodes. Each is judged by itself, and one with an error inside is skipped. */
    if (csp_is(node, "ERROR")) {
        csp_walk_children(ctx, node, owner, depth);
        return;
    }
    if (csp_is(node, "enum_specifier")) {
        csp_note_enumerators(ctx, node, owner);
        return;
    }
    if (csp_is(node, "type_definition")) {
        TSNode type = csp_field(node, "type"), declarator = csp_field(node, "declarator");
        csp_note_enumerators(ctx, type, owner);
        /* An error among the members does not change which struct this is. */
        if (csp_is(type, "class_specifier") || csp_is(type, "struct_specifier") ||
            csp_is(type, "union_specifier"))
            csp_class(ctx, type, owner,
                      csp_is(declarator, "type_identifier") ? csp_text(ctx, declarator) : NULL);
        return;
    }
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
        /* A definition with an error in its body still has this signature. */
        TSNode declarator = csp_field(node, "declarator"), type = csp_field(node, "type");
        if ((ts_node_is_null(declarator) || !ts_node_has_error(declarator)) &&
            (ts_node_is_null(type) || !ts_node_has_error(type)))
            csp_free_function(ctx, node, owner);
        if (csp_is(node, "function_definition"))
            return;
    }
    if (csp_is(node, "class_specifier") || csp_is(node, "struct_specifier") ||
        csp_is(node, "union_specifier")) {
        csp_class(ctx, node, owner, NULL);
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
    csp_walk_children(ctx, node, owner, depth);
}

bool cbm_c_collect_source_facts(CBMArena *arena, const char *source, int length, const char *module,
                                CBMLSPDef *defs, int count) {
    return cbm_c_collect_file_facts(arena, source, length, module, CBM_LANG_CPP, defs, count, NULL);
}

bool cbm_c_collect_file_facts(CBMArena *arena, const char *source, int length, const char *module,
                              CBMLanguage language, CBMLSPDef *defs, int count,
                              CBMCFileFacts *facts) {
    if (count < 0 || (count && !defs))
        return false;
    for (int i = 0; i < count; i++) {
        defs[i].source_flags = 0;
        defs[i].source_signature = NULL;
    }
    if (!arena || !source || length <= 0 || !module)
        return false;
    extern const TSLanguage *tree_sitter_cpp(void);
    extern const TSLanguage *tree_sitter_c(void);
    TSParser *parser = ts_parser_new();
    if (!parser)
        return false;
    /* C source can name a variable `new` or `class`, and the C++ grammar then
     * loses the declaration it sits in. */
    ts_parser_set_language(parser, language == CBM_LANG_C ? tree_sitter_c() : tree_sitter_cpp());
    TSTree *tree = cbm_parse_c_source(parser, source, (uint32_t)length, (TSParseOptions){0});
    ts_parser_delete(parser);
    if (!tree)
        return false;
    csp_context_t ctx = {.arena = arena,
                         .source = source,
                         .length = (uint32_t)length,
                         .module = module,
                         .facts = facts,
                         .c_language = language == CBM_LANG_C,
                         .definitions = cbm_ht_create(32)};
    ctx.failed = !ctx.definitions;
    ctx.preprocessor_unknown =
        cbm_c_preprocessor_domain_unsupported(ts_tree_root_node(tree), source);
    ctx.file_extern_c = strstr(source, "__cplusplus") && strstr(source, "extern \"C\"");
    if (facts && !facts->module_qn)
        facts->module_qn = module;
    for (int i = 0; !ctx.failed && i < count; i++) {
        CBMLSPDef *def = &defs[i];
        const char *qn =
            def->graph_qualified_name ? def->graph_qualified_name : def->qualified_name;
        /* `static struct config {...} config;` holds a Class and a Variable of one name
         * on one line, and only the Class has source facts to receive. */
        if (!qn || !def->source_start_line || !def->short_name ||
            (def->label && !strcmp(def->label, "Variable")))
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
    if (!ctx.failed) {
        TSNode root = ts_tree_root_node(tree);
        TSNode guard = csp_include_guard(&ctx, root);
        if (facts && !ts_node_is_null(guard))
            facts->guard = csp_text(&ctx, csp_field(guard, "name"));
        if (ts_node_is_null(guard))
            csp_walk(&ctx, root, module, 0);
        else
            csp_walk_children(&ctx, guard, module, 0);
    }
    if (ctx.failed)
        for (int i = 0; i < count; i++)
            defs[i].source_flags = 0;
    cbm_ht_free(ctx.definitions);
    ts_tree_delete(tree);
    return !ctx.failed;
}

static bool csp_append_file(CBMArena *arena, const char ***items, int *count, int *cap,
                            const char *file) {
    if (*count == *cap) {
        int next_cap = *cap ? *cap * 2 : 8;
        const char **next = cbm_arena_alloc(arena, (size_t)next_cap * sizeof(*next));
        if (!next)
            return false;
        if (*count)
            memcpy(next, *items, (size_t)*count * sizeof(*next));
        *items = next;
        *cap = next_cap;
    }
    (*items)[(*count)++] = file;
    return true;
}

bool cbm_c_facts_add_include(CBMArena *arena, CBMCFileFacts *facts, const char *file) {
    return csp_append_file(arena, &facts->includes, &facts->include_count, &facts->include_cap,
                           file);
}

bool cbm_c_facts_add_method(CBMArena *arena, CBMCFileFacts *facts, const char *name,
                            const char *target, const char *owner, int arity) {
    csp_context_t ctx = {.arena = arena, .facts = facts, .module = facts->module_qn};
    CBMCDecl *fact = csp_add_decl(&ctx, CBM_C_DECL_METHOD, name, 0);
    if (!fact)
        return false;
    fact->target = target;
    fact->type_name = owner;
    fact->arity = arity;
    return true;
}

bool cbm_c_facts_add_generated(CBMArena *arena, CBMCFileFacts *facts, const char *file) {
    return csp_append_file(arena, &facts->generated_for, &facts->generated_count,
                           &facts->generated_cap, file);
}

typedef struct CDeclFileEntry {
    const CBMCFileFacts *facts;
    struct CDeclFileEntry *next;
} CDeclFileEntry;

struct CBMCDeclIndex {
    CBMCDeclLink **names;
    CDeclFileEntry **files;
    uint32_t name_mask, file_mask;
};

static uint32_t csp_hash(const char *text) {
    uint32_t hash = 2166136261u;
    for (const unsigned char *p = (const unsigned char *)text; *p; p++)
        hash = (hash ^ *p) * 16777619u;
    return hash;
}

static uint32_t csp_bucket_mask(size_t items) {
    uint32_t size = 16;
    while (size < items * 2 && size < (1u << 28))
        size <<= 1;
    return size - 1;
}

static const char *csp_file_key(const CBMCFileFacts *facts) {
    return facts->file_qn ? facts->file_qn : facts->module_qn;
}

const CBMCDeclIndex *cbm_c_decl_index_build(CBMArena *arena, const CBMLSPDef *defs, int count) {
    size_t files = 0, decls = 0;
    for (int i = 0; i < count; i++) {
        if (!defs[i].c_file_facts)
            continue;
        files++;
        decls += (size_t)defs[i].c_file_facts->decl_count;
    }
    if (!files)
        return NULL;
    CBMCDeclIndex *index = cbm_arena_calloc(arena, sizeof(*index));
    if (!index)
        return NULL;
    index->name_mask = csp_bucket_mask(decls);
    index->file_mask = csp_bucket_mask(files);
    index->names = cbm_arena_calloc(arena, (size_t)(index->name_mask + 1) * sizeof(*index->names));
    index->files = cbm_arena_calloc(arena, (size_t)(index->file_mask + 1) * sizeof(*index->files));
    if (!index->names || !index->files)
        return NULL;
    for (int i = 0; i < count; i++) {
        const CBMCFileFacts *facts = defs[i].c_file_facts;
        if (!facts || !facts->module_qn)
            continue;
        CDeclFileEntry *entry = cbm_arena_alloc(arena, sizeof(*entry));
        if (!entry)
            return NULL;
        uint32_t slot = csp_hash(csp_file_key(facts)) & index->file_mask;
        *entry = (CDeclFileEntry){.facts = facts, .next = index->files[slot]};
        index->files[slot] = entry;
        for (int d = 0; d < facts->decl_count; d++) {
            const CBMCDecl *decl = &facts->decls[d];
            uint32_t bucket = csp_hash(decl->name) & index->name_mask;
            CBMCDeclLink *link = cbm_arena_alloc(arena, sizeof(*link));
            if (!link)
                return NULL;
            *link = (CBMCDeclLink){.decl = decl, .next = index->names[bucket]};
            index->names[bucket] = link;
            /* `ns.f` is also reachable by the name a call writes. */
            const char *leaf = decl->kind == CBM_C_DECL_PROTOTYPE ? strrchr(decl->name, '.') : NULL;
            if (leaf) {
                bucket = csp_hash(leaf + 1) & index->name_mask;
                link = cbm_arena_alloc(arena, sizeof(*link));
                if (!link)
                    return NULL;
                *link = (CBMCDeclLink){.decl = decl, .next = index->names[bucket]};
                index->names[bucket] = link;
            }
        }
    }
    return index;
}

const CBMCDeclLink *cbm_c_decl_lookup(const CBMCDeclIndex *index, const char *name) {
    return index && name ? index->names[csp_hash(name) & index->name_mask] : NULL;
}

const CBMCFileFacts *cbm_c_file_facts(const CBMCDeclIndex *index, const char *file) {
    if (!index || !file)
        return NULL;
    for (const CDeclFileEntry *e = index->files[csp_hash(file) & index->file_mask]; e; e = e->next)
        if (!strcmp(csp_file_key(e->facts), file))
            return e->facts;
    return NULL;
}
