#include "c_proof.h"
#include "c_lsp.h"
#include <ctype.h>
#include "../callable_sig.h"
#include "../helpers.h"
#include "foundation/hash_table.h"
#include "../resolve/syntax_index.h"
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
    /* Struct QNs this file defines more than once: its preprocessor branches swap layouts. */
    CBMHashTable *swapped_layouts;
    bool failed;
    bool extern_c;
    bool preprocessor_unknown;
    bool conditional;
    uint32_t branch_group, branch;
    const char *needs_macro;
    int template_depth;
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
    if (depth > 128)
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
    bool recovered = false;
    for (unsigned depth = 0; depth < 32 && !ts_node_is_null(decl); depth++) {
        if (csp_is(decl, "function_declarator"))
            return decl;
        if (csp_is(decl, "reference_declarator")) {
            /* `T &f()`: the grammar gives this declarator's inner part no field name. */
            uint32_t count = ts_node_named_child_count(decl);
            decl = count ? ts_node_named_child(decl, count - 1) : (TSNode){0};
            continue;
        }
        /* `API RetT *\nname(...)`: the grammar reads RetT as a scope with its `::` missing. */
        if (csp_is(decl, "qualified_identifier") && cbm_c_qualifier_is_recovered(decl)) {
            decl = csp_field(decl, "name");
            recovered = true;
            continue;
        }
        if (!csp_is(decl, "pointer_declarator") &&
            !(recovered && csp_is(decl, "pointer_type_declarator")))
            break;
        decl = csp_field(decl, "declarator");
    }
    return (TSNode){0};
}

/* The written name of the function a declarator declares. Three recovered shapes put it where
 * no well-formed declaration would, each only valid if a word in front is a macro:
 * `API RetT\nname(...)` reads RetT as a scope with its `::` missing, `API RetT name(...)` leaves
 * name in an ERROR, and `LUA_API int (lua_gettop) (lua_State *L)` reads as a function `int`
 * taking one parameter of type lua_gettop, itself returning a function, which C does not have. */
static TSNode csp_function_name(TSNode declarator) {
    TSNode real = cbm_c_recovered_func_name(declarator);
    if (!ts_node_is_null(real))
        return real;
    TSNode name = csp_field(declarator, "declarator");
    if (csp_is(name, "qualified_identifier") && cbm_c_qualifier_is_recovered(name))
        return csp_field(name, "name");
    if (csp_is(name, "function_declarator")) {
        TSNode wrapped = cbm_c_macro_wrapped_name_node(declarator);
        TSNode inner = csp_field(name, "declarator");
        return csp_is(inner, "identifier") ? wrapped : (TSNode){0};
    }
    while (csp_is(name, "parenthesized_declarator"))
        name = ts_node_named_child(name, 0);
    return name;
}

/* A declaration's parameters do not change what it names: `dictEntry *dictAddRaw(dict *d, void
 * *key __stored_key, dictEntry **existing);` has a macro no grammar can place among them. */
static bool csp_declares_readable_function(TSNode node) {
    TSNode declarator = csp_function_declarator(node);
    if (ts_node_is_null(declarator))
        return false;
    TSNode name = csp_function_name(declarator);
    return !ts_node_is_null(name) && !ts_node_has_error(name) && !ts_node_is_missing(name) &&
           (csp_is(name, "identifier") || csp_is(name, "type_identifier"));
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
    bool unsupported = false;
    /* One pass over the file, so its size decides nothing. */
    for (;;) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
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
                       .flags = flags | (ctx->conditional ? CBM_C_DECL_CONDITIONAL : 0),
                       .branch_group = ctx->branch_group,
                       .branch = ctx->branch};
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
    fact->arity =
        ts_node_has_error(csp_field(decl, "parameters")) ? -1 : csp_fixed_arity(ctx, decl);
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

/* ── Function-like macros ─────────────────────────────────────────────
 * A token walk over the replacement text, once per definition: linear in that text. */

enum { CSP_MACRO_PARAMS = 32, CSP_MACRO_NESTING = 32 };

typedef struct {
    const char *text;
    uint32_t length;
    bool identifier;
} csp_token_t;

static bool csp_append_file(CBMArena *arena, const char ***items, int *count, int *cap,
                            const char *file);

/* The next token after `at`, skipping space, comments and literals. A literal is a token too: it
 * stops `T` and `x` reading as adjacent across it. Punctuators of interest are kept whole. */
static uint32_t csp_macro_token(const char *text, uint32_t length, uint32_t at, csp_token_t *out) {
    while (at < length) {
        unsigned char ch = (unsigned char)text[at];
        if (isspace(ch) || (ch == '\\' && at + 1 < length && text[at + 1] == '\n')) {
            at++;
        } else if (ch == '/' && at + 1 < length && text[at + 1] == '/') {
            while (at < length && text[at] != '\n')
                at++;
        } else if (ch == '/' && at + 1 < length && text[at + 1] == '*') {
            at += 2;
            while (at + 1 < length && !(text[at] == '*' && text[at + 1] == '/'))
                at++;
            at = at + 2 < length ? at + 2 : length;
        } else {
            break;
        }
    }
    if (at >= length) {
        *out = (csp_token_t){0};
        return length;
    }
    uint32_t start = at;
    unsigned char ch = (unsigned char)text[at];
    if (isalpha(ch) || ch == '_') {
        while (at < length && (isalnum((unsigned char)text[at]) || text[at] == '_'))
            at++;
        *out = (csp_token_t){text + start, at - start, true};
        return at;
    }
    if (isdigit(ch)) {
        while (at < length &&
               (isalnum((unsigned char)text[at]) || text[at] == '_' || text[at] == '.'))
            at++;
    } else if (ch == '"' || ch == '\'') {
        at++;
        while (at < length && (unsigned char)text[at] != ch) {
            at += text[at] == '\\' ? 2 : 1;
        }
        at = at < length ? at + 1 : length;
    } else if (at + 1 < length &&
               ((ch == '#' && text[at + 1] == '#') || (ch == '-' && text[at + 1] == '>'))) {
        at += 2;
    } else {
        at++;
    }
    *out = (csp_token_t){text + start, at - start, false};
    return at;
}

static bool csp_token_is(const csp_token_t *token, const char *text) {
    size_t length = strlen(text);
    return token->text && token->length == length && !memcmp(token->text, text, length);
}

/* A word that can stand before an expression, so that `return x` is no declaration of x. */
static bool csp_token_precedes_expression(const csp_token_t *token) {
    static const char *const words[] = {"return",   "sizeof",  "case",   "else",
                                        "do",       "goto",    "typeof", "__typeof__",
                                        "_Alignof", "alignof", "defined"};
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++)
        if (csp_token_is(token, words[i]))
            return true;
    return false;
}

typedef struct {
    const char *names[CSP_MACRO_PARAMS];
    int count;
    bool variadic;
    bool overflowed;
} csp_macro_params_t;

static void csp_macro_params(csp_context_t *ctx, TSNode list, csp_macro_params_t *out) {
    TSTreeCursor cursor = ts_tree_cursor_new(list);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            const char *kind = ts_node_type(child);
            if (!strcmp(kind, "...")) {
                /* `(a, ...)` names the rest __VA_ARGS__, GNU `(a, rest...)` names it rest. */
                if (out->count == CSP_MACRO_PARAMS)
                    out->overflowed = true;
                else if (!out->variadic) {
                    out->names[out->count++] = "__VA_ARGS__";
                    out->variadic = true;
                }
            } else if (!strcmp(kind, "identifier")) {
                const char *text = csp_text(ctx, child);
                if (out->count == CSP_MACRO_PARAMS)
                    out->overflowed = true;
                else if (text)
                    out->names[out->count++] = text;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

static int csp_macro_param_index(const csp_macro_params_t *params, const csp_token_t *token) {
    for (int i = 0; i < params->count; i++)
        if (csp_token_is(token, params->names[i]))
            return i;
    return -1;
}

static bool csp_append_handoff(CBMArena *arena, CBMCMacroHandoff **items, int *count, int *cap,
                               CBMCMacroHandoff item) {
    if (*count == *cap) {
        int next_cap = *cap ? *cap * 2 : 4;
        CBMCMacroHandoff *next = cbm_arena_alloc(arena, (size_t)next_cap * sizeof(*next));
        if (!next)
            return false;
        if (*count)
            memcpy(next, *items, (size_t)*count * sizeof(*next));
        *items = next;
        *cap = next_cap;
    }
    (*items)[(*count)++] = item;
    return true;
}

/* A name a type stands in front of, followed by what can follow a declarator. */
static bool csp_token_ends_declarator(const csp_token_t *token) {
    return csp_token_is(token, "=") || csp_token_is(token, ";") || csp_token_is(token, ",") ||
           csp_token_is(token, "[") || csp_token_is(token, ":");
}

/* Words of the language, which name no declaration. Type words matter as much as statements:
 * they appear in every cast. */
static bool csp_token_is_keyword(const csp_token_t *token) {
    static const char *const words[] = {
        "if",         "else",         "for",      "while",    "do",          "switch",   "case",
        "default",    "return",       "break",    "continue", "goto",        "sizeof",   "typeof",
        "__typeof__", "defined",      "struct",   "union",    "enum",        "const",    "volatile",
        "static",     "extern",       "register", "inline",   "void",        "char",     "short",
        "int",        "long",         "float",    "double",   "signed",      "unsigned", "bool",
        "_Bool",      "true",         "false",    "NULL",     "__VA_ARGS__", "__FILE__", "__LINE__",
        "__func__",   "__FUNCTION__", "_Alignof", "alignof",  "nullptr",     "this"};
    for (size_t i = 0; i < sizeof(words) / sizeof(words[0]); i++)
        if (csp_token_is(token, words[i]))
            return true;
    return false;
}

/* A word whose parentheses hold a condition, and so hand nothing to a callee. */
static bool csp_token_is_statement_word(const csp_token_t *token) {
    return csp_token_is(token, "if") || csp_token_is(token, "while") ||
           csp_token_is(token, "for") || csp_token_is(token, "switch");
}

static int csp_string_compare(const void *left, const void *right) {
    return strcmp(*(const char *const *)left, *(const char *const *)right);
}

/* Each name once, in a stable order. */
static int csp_unique_names(const char **names, int count) {
    if (count < 2)
        return count;
    qsort(names, (size_t)count, sizeof(*names), csp_string_compare);
    int unique = 1;
    for (int i = 1; i < count; i++)
        if (strcmp(names[i], names[unique - 1]))
            names[unique++] = names[i];
    return unique;
}

static bool csp_append_paste(CBMArena *arena, CBMCMacroPaste **items, int *count, int *cap,
                             CBMCMacroPaste item) {
    if (*count == *cap) {
        int next_cap = *cap ? *cap * 2 : 2;
        CBMCMacroPaste *next = cbm_arena_alloc(arena, (size_t)next_cap * sizeof(*next));
        if (!next)
            return false;
        if (*count)
            memcpy(next, *items, (size_t)*count * sizeof(*next));
        *items = next;
        *cap = next_cap;
    }
    (*items)[(*count)++] = item;
    return true;
}

/* What a macro's replacement says, kept when it says anything. `#define REG_X 0x10` names nothing,
 * and a tree of register headers holds millions of those. */
static bool csp_append_name_argument(CBMArena *arena, CBMCMacroNameArgument **items, int *count,
                                     int *cap, CBMCMacroNameArgument item) {
    if (*count == *cap) {
        int next_cap = *cap ? *cap * 2 : 2;
        CBMCMacroNameArgument *next = cbm_arena_alloc(arena, (size_t)next_cap * sizeof(*next));
        if (!next)
            return false;
        if (*count)
            memcpy(next, *items, (size_t)*count * sizeof(*next));
        *items = next;
        *cap = next_cap;
    }
    (*items)[(*count)++] = item;
    return true;
}

static bool csp_append_member(CBMArena *arena, CBMCMacroMember **items, int *count, int *cap,
                              CBMCMacroMember item) {
    if (*count == *cap) {
        int next_cap = *cap ? *cap * 2 : 2;
        CBMCMacroMember *next = cbm_arena_alloc(arena, (size_t)next_cap * sizeof(*next));
        if (!next)
            return false;
        if (*count)
            memcpy(next, *items, (size_t)*count * sizeof(*next));
        *items = next;
        *cap = next_cap;
    }
    (*items)[(*count)++] = item;
    return true;
}

/* The end of the `(` at `open`, or 0 when it does not close. Comments and literals have been left
 * to the caller: a replacement with a string in its type part reads as no type at all. */
static uint32_t csp_macro_close(const char *text, uint32_t length, uint32_t open) {
    int depth = 0;
    for (uint32_t at = open; at < length; at++) {
        if (text[at] == '"' || text[at] == '\'') {
            char quote = text[at];
            for (at++; at < length && text[at] != quote; at++)
                if (text[at] == '\\')
                    at++;
        } else if (text[at] == '(') {
            depth++;
        } else if (text[at] == ')' && --depth == 0) {
            return at + 1;
        }
    }
    return 0;
}

static uint32_t csp_macro_skip_space(const char *text, uint32_t length, uint32_t at) {
    while (at < length) {
        if (text[at] == ' ' || text[at] == '\t' || text[at] == '\r' || text[at] == '\n' ||
            (text[at] == '\\' && at + 1 < length && text[at + 1] == '\n')) {
            at++;
        } else if (text[at] == '/' && at + 1 < length && text[at + 1] == '*') {
            for (at += 2; at + 1 < length && !(text[at] == '*' && text[at + 1] == '/'); at++)
                ;
            at = at + 2 <= length ? at + 2 : length;
        } else if (text[at] == '/' && at + 1 < length && text[at + 1] == '/') {
            return length;
        } else {
            break;
        }
    }
    return at;
}

/* `((T *)operand)`. The parentheses around it have to close at its end: bare `(T *)p` used as
 * `X->y` would cast p->y instead. The operand may be followed by nothing that takes the value out
 * of the pointer type, which rules out comparisons, `?:` and `,`, and by no `+` or `-`, which would
 * need the other operand read. A value the replacement's own parameter names as the type is not
 * written. */
static void csp_macro_cast(csp_context_t *ctx, CBMCMacroShape *shape, const char *text,
                           uint32_t length, const csp_macro_params_t *params) {
    uint32_t begin = csp_macro_skip_space(text, length, 0), end = length;
    /* The end, past trailing space and comments. */
    for (uint32_t at = begin; at < length;) {
        uint32_t next = csp_macro_skip_space(text, length, at);
        if (next >= length)
            break;
        at = next + 1;
        end = at;
    }
    bool wrapped = false;
    while (begin < end && text[begin] == '(' && csp_macro_close(text, end, begin) == end) {
        wrapped = true;
        begin = csp_macro_skip_space(text, end, begin + 1);
        end--;
        while (end > begin &&
               (text[end - 1] == ' ' || text[end - 1] == '\t' || text[end - 1] == '\r' ||
                text[end - 1] == '\n' || text[end - 1] == '\\'))
            end--;
    }
    if (!wrapped || begin >= end || text[begin] != '(')
        return;
    uint32_t close = csp_macro_close(text, end, begin);
    if (!close || close >= end)
        return;
    /* The type part: qualifiers and keywords, one name, `*`s. */
    const char *word = NULL;
    size_t word_length = 0;
    uint32_t pointers = 0;
    int paste_param = -1;
    for (uint32_t at = csp_macro_skip_space(text, close - 1, begin + 1); at < close - 1;
         at = csp_macro_skip_space(text, close - 1, at)) {
        if (text[at] == '*') {
            pointers++;
            at++;
            continue;
        }
        if (!isalpha((unsigned char)text[at]) && text[at] != '_')
            return;
        uint32_t start = at;
        while (at < close - 1 && (isalnum((unsigned char)text[at]) || text[at] == '_'))
            at++;
        size_t size = at - start;
        static const char *const skipped[] = {"const", "volatile", "struct", "union", "enum"};
        bool keyword = false;
        for (size_t k = 0; k < sizeof(skipped) / sizeof(skipped[0]); k++)
            keyword |= strlen(skipped[k]) == size && !memcmp(text + start, skipped[k], size);
        if (keyword)
            continue;
        if (word || pointers)
            return;
        word = text + start;
        word_length = size;
        if (at + 1 < close - 1 && text[at] == '#' && text[at + 1] == '#') {
            at += 2;
            uint32_t param_start = at;
            while (at < close - 1 && (isalnum((unsigned char)text[at]) || text[at] == '_'))
                at++;
            for (int i = 0; i < params->count; i++)
                if (strlen(params->names[i]) == at - param_start &&
                    !memcmp(params->names[i], text + param_start, at - param_start))
                    paste_param = i;
            if (paste_param < 0)
                return;
        }
    }
    if (!word || pointers > 8)
        return;
    if (paste_param < 0)
        for (int i = 0; i < params->count; i++)
            if (strlen(params->names[i]) == word_length &&
                !memcmp(params->names[i], word, word_length))
                return;
    /* The operand. */
    uint32_t operand = csp_macro_skip_space(text, end, close);
    if (operand >= end || text[operand] == '.' || text[operand] == '[' ||
        (text[operand] == '-' && operand + 1 < end && text[operand + 1] == '>'))
        return;
    int depth = 0;
    for (uint32_t at = operand; at < end; at++) {
        char ch = text[at];
        if (ch == '"' || ch == '\'') {
            for (at++; at < end && text[at] != ch; at++)
                if (text[at] == '\\')
                    at++;
        } else if (ch == '(' || ch == '[') {
            depth++;
        } else if (ch == ')' || ch == ']') {
            depth--;
        } else if (depth == 0 && at > operand && strchr("+*/%&|^<>=!?:,", ch)) {
            return;
        } else if (depth == 0 && ch == '-' && !(at + 1 < end && text[at + 1] == '>')) {
            return;
        }
    }
    shape->cast_type = cbm_arena_strndup(ctx->arena, word, word_length);
    shape->cast_pointers = (uint8_t)pointers;
    shape->cast_pastes = paste_param >= 0;
    shape->cast_param = paste_param >= 0 ? (uint8_t)paste_param : 0;
}

static const CBMCMacroShape *csp_macro_shape_keep(csp_context_t *ctx, const CBMCMacroShape *shape) {
    if (shape->object_like && !shape->call_count && !shape->value_count && !shape->cast_type)
        return NULL;
    CBMCMacroShape *kept = cbm_arena_alloc(ctx->arena, sizeof(*kept));
    if (!kept) {
        ctx->failed = true;
        return NULL;
    }
    *kept = *shape;
    return kept;
}

static const CBMCMacroShape *csp_macro_shape(csp_context_t *ctx, TSNode node) {
    TSNode list = csp_field(node, "parameters");
    CBMCMacroShape local;
    memset(&local, 0, sizeof(local));
    CBMCMacroShape *shape = &local;
    csp_macro_params_t params;
    memset(&params, 0, sizeof(params));
    shape->object_like = ts_node_is_null(list);
    if (!shape->object_like)
        csp_macro_params(ctx, list, &params);
    /* Past the limit a parameter's name would read as a name of the file's. */
    if (params.overflowed)
        return NULL;
    shape->param_count = (uint8_t)params.count;
    shape->variadic = params.variadic;
    TSNode value = csp_field(node, "value");
    if (ts_node_is_null(value))
        return csp_macro_shape_keep(ctx, shape);
    uint32_t begin = ts_node_start_byte(value), end = ts_node_end_byte(value);
    if (end <= begin || end > ctx->length)
        return csp_macro_shape_keep(ctx, shape);
    const char *text = ctx->source + begin;
    uint32_t length = end - begin;
    csp_macro_cast(ctx, shape, text, length, &params);
    if (shape->object_like) {
        /* Up to a comment or the end of the line: names joined by single dots, nothing else. */
        uint32_t last = 0;
        bool word = false, path = true;
        while (last < length && path) {
            char ch = text[last];
            if (ch == '/' || ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n')
                break;
            if (isalpha((unsigned char)ch) || ch == '_' || (word && isdigit((unsigned char)ch)))
                word = true;
            else if (ch == '.' && word)
                word = false;
            else
                path = false;
            last++;
        }
        uint32_t tail = last;
        while (tail < length && (text[tail] == ' ' || text[tail] == '\t' || text[tail] == '\r' ||
                                 text[tail] == '\n'))
            tail++;
        bool comment = tail + 1 < length && text[tail] == '/' &&
                       (text[tail + 1] == '/' || text[tail + 1] == '*');
        if (path && word && (tail == length || comment))
            shape->field_path = cbm_arena_strndup(ctx->arena, text, last);
    }

    /* Which enclosing `name(` each open parenthesis belongs to, and its argument so far. */
    struct {
        const char *callee;
        uint32_t callee_length;
        int position;
    } nesting[CSP_MACRO_NESTING];
    int depth = 0, overflow = 0;
    uint32_t seen = 0, tainted = 0, declared = 0;
    CBMCMacroHandoff *handoffs = NULL;
    int handoff_count = 0, handoff_cap = 0;
    const char *declared_types[CSP_MACRO_PARAMS] = {0};
    uint8_t declared_pointers[CSP_MACRO_PARAMS] = {0};
    const char **locals = NULL;
    int local_count = 0, local_cap = 0;
    const char **calls = NULL, **values = NULL;
    int call_count = 0, call_cap = 0, value_count = 0, value_cap = 0;
    CBMCMacroPaste *pastes = NULL;
    int paste_count = 0, paste_cap = 0;
    CBMCMacroMember *members = NULL;
    int member_count = 0, member_cap = 0;
    CBMCMacroNameArgument *name_arguments = NULL;
    int name_argument_count = 0, name_argument_cap = 0;
    const char **objects = NULL;
    int object_count = 0, object_cap = 0;
    /* `a::b` so far, as written, while the next word finishes the name. */
    const char *qualifier = NULL;
    /* Inside `__attribute__((format(printf, 1, 2)))` the words name no declaration, and
     * `_Generic` selects one of its branches by a type this does not know. */
    int attribute_depth = -1;

    /* Three tokens of history before the current one, and one after it. */
    csp_token_t back1 = {0}, back2 = {0}, back3 = {0}, current, next;
    uint32_t at = csp_macro_token(text, length, 0, &current);
    while (current.text) {
        at = csp_macro_token(text, length, at, &next);
        if (csp_token_is(&current, "(")) {
            if (depth < CSP_MACRO_NESTING) {
                bool named = back1.identifier && !csp_token_precedes_expression(&back1) &&
                             !csp_token_is_statement_word(&back1);
                nesting[depth].callee = named ? back1.text : NULL;
                nesting[depth].callee_length = named ? back1.length : 0;
                nesting[depth].position = 0;
                depth++;
            } else {
                overflow++;
            }
        } else if (csp_token_is(&current, ")")) {
            if (overflow)
                overflow--;
            else if (depth)
                depth--;
            if (attribute_depth >= 0 && depth <= attribute_depth)
                attribute_depth = -1;
        } else if (csp_token_is(&current, ",") && depth && !overflow) {
            nesting[depth - 1].position++;
        } else if (current.identifier) {
            int param = csp_macro_param_index(&params, &current);
            bool after_type = back1.identifier && !csp_token_precedes_expression(&back1);
            /* `T *x =`, and with the parentheses macros put round a parameter `T *(x) =`. */
            bool parenthesized = csp_token_is(&back1, "(") && csp_token_is(&back2, "*");
            bool after_star = csp_token_is(&back1, "*") || parenthesized;
            const csp_token_t *before_star = parenthesized ? &back3 : &back2;
            csp_token_t following = next;
            if (parenthesized && csp_token_is(&following, ")"))
                (void)csp_macro_token(text, length, at, &following);
            bool star_declares = after_star && before_star->identifier &&
                                 !csp_token_precedes_expression(before_star) &&
                                 csp_token_ends_declarator(&following);
            if (param >= 0) {
                seen |= 1u << param;
                if (csp_token_is(&back1, "#") || csp_token_is(&back1, "##") ||
                    csp_token_is(&next, "##") || csp_token_is(&back1, ".") ||
                    csp_token_is(&back1, "->") ||
                    (csp_token_is(&back1, "*") && csp_token_is(&back2, "*"))) {
                    tainted |= 1u << param;
                } else if (after_type || star_declares) {
                    tainted |= 1u << param;
                    if (!(declared & (1u << param))) {
                        declared |= 1u << param;
                        const csp_token_t *type = after_type ? &back1 : before_star;
                        declared_types[param] =
                            cbm_arena_strndup(ctx->arena, type->text, type->length);
                        declared_pointers[param] = after_type ? 0 : 1;
                    }
                }
                /* `(l)->len`, `L->ci->top`: the members named through this parameter. */
                if (!csp_token_is(&back1, ".") && !csp_token_is(&back1, "->") &&
                    !csp_token_is(&back1, "#") && !csp_token_is(&back1, "##")) {
                    csp_token_t step = next, word;
                    uint32_t from = at;
                    /* `(l)->len` wraps the parameter. `rawgco2u(o)->uv` calls something with it. */
                    if (csp_token_is(&next, ")") && csp_token_is(&back1, "(") &&
                        (!back2.identifier || csp_token_precedes_expression(&back2)))
                        from = csp_macro_token(text, length, from, &step);
                    const char *path = NULL;
                    while (csp_token_is(&step, "->") || csp_token_is(&step, ".")) {
                        from = csp_macro_token(text, length, from, &word);
                        if (!word.identifier || csp_macro_param_index(&params, &word) >= 0)
                            break;
                        from = csp_macro_token(text, length, from, &step);
                        if (csp_token_is(&step, "##"))
                            break;
                        const char *piece = cbm_arena_strndup(ctx->arena, word.text, word.length);
                        path = path && piece ? cbm_arena_sprintf(ctx->arena, "%s.%s", path, piece)
                                             : piece;
                        if (!path) {
                            ctx->failed = true;
                            return NULL;
                        }
                    }
                    if (path && !csp_append_member(ctx->arena, &members, &member_count, &member_cap,
                                                   (CBMCMacroMember){path, (uint8_t)param})) {
                        ctx->failed = true;
                        return NULL;
                    }
                }
                /* `RM_ ## name` and `name ## _t`: exactly one word around the parameter. */
                bool pasted_before =
                    csp_token_is(&back1, "##") && back2.identifier && !csp_token_is(&back3, "##");
                bool pasted_after = csp_token_is(&next, "##") && !csp_token_is(&back1, "##");
                if (pasted_before || pasted_after) {
                    csp_token_t word = back2;
                    if (pasted_after) {
                        (void)csp_macro_token(text, length, at, &word);
                        csp_token_t beyond;
                        uint32_t past = word.text ? (uint32_t)(word.text + word.length - text) : at;
                        (void)csp_macro_token(text, length, past, &beyond);
                        if (!word.identifier || csp_macro_param_index(&params, &word) >= 0 ||
                            csp_token_is(&beyond, "##"))
                            word.text = NULL;
                    }
                    const char *piece =
                        word.text ? cbm_arena_strndup(ctx->arena, word.text, word.length) : NULL;
                    if (piece && !csp_append_paste(ctx->arena, &pastes, &paste_count, &paste_cap,
                                                   (CBMCMacroPaste){pasted_before ? piece : "",
                                                                    pasted_after ? piece : "",
                                                                    (uint8_t)param})) {
                        ctx->failed = true;
                        return NULL;
                    }
                }
                for (int level = 0; level < depth; level++) {
                    if (!nesting[level].callee || nesting[level].position >= CSP_MACRO_PARAMS)
                        continue;
                    const char *callee = cbm_arena_strndup(ctx->arena, nesting[level].callee,
                                                           nesting[level].callee_length);
                    if (!callee ||
                        !csp_append_handoff(ctx->arena, &handoffs, &handoff_count, &handoff_cap,
                                            (CBMCMacroHandoff){callee, (uint8_t)param,
                                                               (uint8_t)nesting[level].position})) {
                        ctx->failed = true;
                        return NULL;
                    }
                }
            } else if (after_type || star_declares ||
                       /* `int f(void *counter)` written in a replacement. `a * b)` reads the same
                        * and stays unbound for it, which costs a value and proves nothing false. */
                       (after_star && before_star->identifier && csp_token_is(&next, ")") &&
                        !csp_token_precedes_expression(before_star))) {
                const char *declared_name =
                    cbm_arena_strndup(ctx->arena, current.text, current.length);
                if (!declared_name || !csp_append_file(ctx->arena, &locals, &local_count,
                                                       &local_cap, declared_name)) {
                    ctx->failed = true;
                    return NULL;
                }
            } else if (csp_token_is(&current, "__attribute__") ||
                       csp_token_is(&current, "__declspec") || csp_token_is(&current, "_Pragma") ||
                       csp_token_is(&current, "__pragma") || csp_token_is(&current, "__asm__") ||
                       csp_token_is(&current, "asm") || csp_token_is(&current, "_Generic") ||
                       csp_token_is(&current, "__builtin_choose_expr")) {
                if (attribute_depth < 0)
                    attribute_depth = depth;
            } else if (attribute_depth < 0 && !csp_token_is_keyword(&current) &&
                       !csp_token_is(&back1, ".") && !csp_token_is(&back1, "->") &&
                       !csp_token_is(&back1, "#") && !csp_token_is(&back1, "##") &&
                       !csp_token_is(&next, "##") && !csp_token_is(&back1, "goto") &&
                       /* `done:` opens a statement as a label. */
                       !(csp_token_is(&next, ":") && !(at < length && text[at] == ':') &&
                         (!back1.text || csp_token_is(&back1, ";") || csp_token_is(&back1, "{") ||
                          csp_token_is(&back1, "}")))) {
                bool scoped = csp_token_is(&back1, ":") && csp_token_is(&back2, ":");
                bool qualifies = csp_token_is(&next, ":") && at < length && text[at] == ':';
                /* `T<int>::f` and `f()::g` name a scope this cannot read. A lone `::f` is global.
                 */
                bool unreadable =
                    scoped && (csp_token_is(&back3, ">") || csp_token_is(&back3, ")") ||
                               (back3.identifier && !qualifier));
                const char *word = cbm_arena_strndup(ctx->arena, current.text, current.length);
                if (word && scoped)
                    word =
                        cbm_arena_sprintf(ctx->arena, "%s::%s", qualifier ? qualifier : "", word);
                qualifier = qualifies && !unreadable ? word : NULL;
                if (qualifies || unreadable) {
                    back3 = back2;
                    back2 = back1;
                    back1 = current;
                    current = next;
                    continue;
                }
                bool called = csp_token_is(&next, "(");
                if (word && (csp_token_is(&next, ".") || csp_token_is(&next, "->")) &&
                    !csp_append_file(ctx->arena, &objects, &object_count, &object_cap, word)) {
                    ctx->failed = true;
                    return NULL;
                }
                /* A bare word handed to a callee means what that callee makes of it. */
                if (word && !overflow && depth > 0 &&
                    (csp_token_is(&back1, "(") || csp_token_is(&back1, ",")) &&
                    (csp_token_is(&next, ",") || csp_token_is(&next, ")")) &&
                    nesting[depth - 1].callee && nesting[depth - 1].position < CSP_MACRO_PARAMS) {
                    const char *callee = cbm_arena_strndup(ctx->arena, nesting[depth - 1].callee,
                                                           nesting[depth - 1].callee_length);
                    if (!callee ||
                        !csp_append_name_argument(
                            ctx->arena, &name_arguments, &name_argument_count, &name_argument_cap,
                            (CBMCMacroNameArgument){word, callee,
                                                    (uint8_t)nesting[depth - 1].position})) {
                        ctx->failed = true;
                        return NULL;
                    }
                }
                if (!word || !csp_append_file(ctx->arena, called ? &calls : &values,
                                              called ? &call_count : &value_count,
                                              called ? &call_cap : &value_cap, word)) {
                    ctx->failed = true;
                    return NULL;
                }
            }
        }
        back3 = back2;
        back2 = back1;
        back1 = current;
        current = next;
    }
    shape->plain = seen & ~tainted;
    shape->declared = declared;
    shape->handoffs = handoffs;
    shape->handoff_count = handoff_count;
    shape->locals = locals;
    shape->local_count = local_count;
    shape->calls = calls;
    shape->call_count = csp_unique_names(calls, call_count);
    shape->values = values;
    shape->value_count = csp_unique_names(values, value_count);
    shape->pastes = pastes;
    shape->paste_count = paste_count;
    shape->members = members;
    shape->member_count = member_count;
    shape->name_arguments = name_arguments;
    shape->name_argument_count = name_argument_count;
    shape->objects = objects;
    shape->object_count = csp_unique_names(objects, object_count);
    if (declared) {
        const char **types = cbm_arena_calloc(ctx->arena, CSP_MACRO_PARAMS * sizeof(*types));
        uint8_t *pointers = cbm_arena_calloc(ctx->arena, CSP_MACRO_PARAMS);
        if (!types || !pointers) {
            ctx->failed = true;
            return NULL;
        }
        memcpy(types, declared_types, sizeof(declared_types));
        memcpy(pointers, declared_pointers, sizeof(declared_pointers));
        shape->declared_types = types;
        shape->declared_pointers = pointers;
    }
    return csp_macro_shape_keep(ctx, shape);
}

static void csp_note_macro(csp_context_t *ctx, TSNode node) {
    TSNode name = csp_field(node, "name");
    if (!csp_is(name, "identifier"))
        return;
    const char *text = csp_text(ctx, name);
    if (!text || (ctx->facts && ctx->facts->guard && !strcmp(ctx->facts->guard, text)))
        return;
    CBMCDecl *fact = csp_add_decl(ctx, CBM_C_DECL_MACRO, text, 0);
    if (fact) {
        fact->macro = csp_macro_shape(ctx, node);
        fact->needs_macro = ctx->needs_macro;
    }
}

static void csp_note_quoted_include(csp_context_t *ctx, TSNode node) {
    TSNode path = csp_field(node, "path");
    if (!ctx->facts || !csp_is(path, "string_literal"))
        return;
    const char *text = csp_text(ctx, path);
    size_t length = text ? strlen(text) : 0;
    const char *spelled = length >= 3 ? cbm_arena_strndup(ctx->arena, text + 1, length - 2) : NULL;
    if (spelled && !csp_append_file(ctx->arena, &ctx->facts->quoted_includes,
                                    &ctx->facts->quoted_include_count,
                                    &ctx->facts->quoted_include_cap, spelled))
        ctx->failed = true;
}

static bool csp_text_starts_with(const char *at, const char *end, const char *word) {
    size_t length = strlen(word);
    return (size_t)(end - at) >= length && !memcmp(at, word, length);
}

/* A `#define` inside a function body stays defined to the end of the file: malloc_vsnprintf's
 * APPEND_C, do_sub's ADJUST_SUB_FIRSTLNUM. Most bodies hold none, and the text says so. */
bool cbm_c_text_defines_macro(const char *text, uint32_t length) {
    const char *at = text, *end = text + length;
    while (at < end && (at = memchr(at, '#', (size_t)(end - at)))) {
        const char *word = at + 1;
        while (word < end && (*word == ' ' || *word == '\t'))
            word++;
        if (csp_text_starts_with(word, end, "define"))
            return true;
        at = word;
    }
    return false;
}

static void csp_note_body_macros(csp_context_t *ctx, TSNode body) {
    uint32_t begin = ts_node_start_byte(body), end = ts_node_end_byte(body);
    if (end > ctx->length || !cbm_c_text_defines_macro(ctx->source + begin, end - begin))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    bool more = true;
    while (more) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        bool macro = csp_is(node, "preproc_def") || csp_is(node, "preproc_function_def");
        if (macro)
            csp_note_macro(ctx, node);
        if (!macro && ts_tree_cursor_goto_first_child(&cursor))
            continue;
        while (!(more = ts_tree_cursor_goto_next_sibling(&cursor)))
            if (!ts_tree_cursor_goto_parent(&cursor))
                break;
    }
    ts_tree_cursor_delete(&cursor);
}

static void csp_free_function(csp_context_t *ctx, TSNode node, const char *owner) {
    TSNode function_body = csp_field(node, "body");
    if (!ts_node_is_null(function_body))
        csp_note_body_macros(ctx, function_body);
    TSNode decl = csp_function_declarator(node);
    TSNode name_node = ts_node_is_null(decl) ? decl : csp_function_name(decl);
    if (!csp_declares_readable_function(node)) {
        if (csp_is(node, "declaration"))
            csp_note_values(ctx, node, owner);
        return;
    }
    const char *name = csp_text(ctx, name_node);
    const char *qn = name ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name) : NULL;
    if (csp_is(node, "declaration") && name)
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

static void csp_method(csp_context_t *ctx, TSNode node, const char *owner, uint32_t access) {
    TSNode decl = csp_function_declarator(node), name_node = csp_field(decl, "declarator");
    if (!csp_is(name_node, "field_identifier") && !csp_is(name_node, "identifier"))
        return; /* Pointer fields, operators and constructors need their own contracts. */
    const char *name = csp_text(ctx, name_node);
    const char *qn = name ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name) : NULL;
    csp_mark(ctx, node, qn, CBM_SOURCE_DECL_KNOWN | access, csp_arity(ctx, decl));
}
/* A name a class body declares. Many have no graph node: a member function declared and
 * not defined here, a pure virtual, a nested type, an enumerator, a using-declaration.
 * Inside the class such a name still hides anything of that name outside it. */
static CBMCDecl *csp_member_name(csp_context_t *ctx, const char *klass, TSNode name,
                                 uint32_t access) {
    if (ts_node_is_null(name))
        return NULL;
    const char *text = csp_text(ctx, name);
    const char *scope_end = text ? strrchr(text, ':') : NULL;
    CBMCDecl *fact = csp_add_decl(ctx, CBM_C_DECL_MEMBER_NAME, scope_end ? scope_end + 1 : text,
                                  access == CBM_SOURCE_DECL_PUBLIC ? 0
                                  : access == CBM_SOURCE_DECL_PROTECTED
                                      ? CBM_C_DECL_NOT_PUBLIC | CBM_C_DECL_PROTECTED
                                      : CBM_C_DECL_NOT_PUBLIC);
    if (fact)
        fact->type_name = klass;
    return fact;
}

static void csp_note_member(csp_context_t *ctx, TSNode node, const char *klass,
                            const char *klass_qn, uint32_t access) {
    if (csp_is(node, "template_declaration")) {
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode part = ts_tree_cursor_current_node(&cursor);
                if (ts_node_is_named(part))
                    csp_note_member(ctx, part, klass, klass_qn, access);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
        return;
    }
    if (csp_is(node, "using_declaration")) {
        uint32_t count = ts_node_named_child_count(node);
        if (count)
            csp_member_name(ctx, klass, ts_node_named_child(node, count - 1), access);
        return;
    }
    if (csp_is(node, "alias_declaration")) {
        csp_member_name(ctx, klass, csp_field(node, "name"), access);
        return;
    }
    if (csp_is(node, "type_definition")) {
        csp_member_name(ctx, klass, csp_field(node, "declarator"), access);
        return;
    }
    TSNode function = csp_function_declarator(node);
    if (!ts_node_is_null(function)) {
        TSNode name = csp_field(function, "declarator");
        if (csp_is(name, "field_identifier") || csp_is(name, "identifier"))
            csp_member_name(ctx, klass, name, access);
        return;
    }
    TSNode type = csp_field(node, "type");
    if (ts_node_is_null(type) ||
        (!csp_is(type, "struct_specifier") && !csp_is(type, "class_specifier") &&
         !csp_is(type, "union_specifier") && !csp_is(type, "enum_specifier")))
        return;
    csp_member_name(ctx, klass, csp_field(type, "name"), access);
    if (!csp_is(type, "enum_specifier"))
        return;
    /* `enum class` keeps its enumerators inside the enum. An unscoped enum puts them in the
     * class, and the graph names them there. */
    bool scoped = false;
    TSTreeCursor head = ts_tree_cursor_new(type);
    if (ts_tree_cursor_goto_first_child(&head))
        do {
            const char *word = ts_node_type(ts_tree_cursor_current_node(&head));
            scoped |= !strcmp(word, "class") || !strcmp(word, "struct");
        } while (ts_tree_cursor_goto_next_sibling(&head));
    ts_tree_cursor_delete(&head);
    TSNode body = csp_field(type, "body");
    uint32_t count = ts_node_is_null(body) ? 0 : ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode enumerator = ts_node_named_child(body, i);
        if (!csp_is(enumerator, "enumerator"))
            continue;
        CBMCDecl *fact = csp_member_name(ctx, klass, csp_field(enumerator, "name"), access);
        if (fact && !scoped && klass_qn)
            fact->target = cbm_arena_sprintf(ctx->arena, "%s.%s", klass_qn, fact->name);
    }
}

/* The name a template parameter declares: `class T`, `class T = int`, `class... T`,
 * `template <class> class T`. A value parameter names no type. */
static TSNode csp_template_parameter_name(TSNode parameter) {
    TSNode name = csp_field(parameter, "name");
    if (csp_is(name, "type_identifier"))
        return name;
    TSNode null_node = {0};
    if (csp_is(parameter, "parameter_declaration") ||
        csp_is(parameter, "optional_parameter_declaration") ||
        csp_is(parameter, "variadic_parameter_declaration"))
        return null_node;
    uint32_t count = ts_node_named_child_count(parameter);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(parameter, i);
        if (csp_is(child, "type_identifier"))
            return child;
    }
    return null_node;
}

/* Every template around this class: a nested class reads its outer's parameters too. */
static void csp_note_template_parameters(csp_context_t *ctx, TSNode klass, const char *name) {
    for (TSNode up = ts_node_parent(klass); !ts_node_is_null(up); up = ts_node_parent(up)) {
        if (!csp_is(up, "template_declaration"))
            continue;
        TSNode list = csp_field(up, "parameters");
        uint32_t count = ts_node_is_null(list) ? 0 : ts_node_named_child_count(list);
        for (uint32_t i = 0; i < count; i++) {
            TSNode parameter = csp_template_parameter_name(ts_node_named_child(list, i));
            const char *text = ts_node_is_null(parameter) ? NULL : csp_text(ctx, parameter);
            CBMCDecl *fact =
                text ? csp_add_decl(ctx, CBM_C_DECL_TEMPLATE_PARAMETER, text, 0) : NULL;
            if (fact)
                fact->type_name = name;
        }
    }
}

/* Two branches of a file defining one struct is how a project swaps one layout for
 * another, and member lookup cannot tell which one the includer gets, so that struct stays
 * unknown. A struct a file defines once has that layout in every configuration that has
 * the struct at all, whatever branch holds it: `#ifdef __cplusplus extern "C" {` holds a
 * whole header, and `#ifdef USE_OPENSSL` a whole file. */
static void csp_class(csp_context_t *ctx, TSNode node, const char *parent,
                      const char *typedef_name);

/* `union vval_union { ... } vval;` defines a type where it declares a member. Extraction
 * names it under the outer class or directly under the outer's scope depending on the form,
 * and only the name it used has an entry, so the other call finds nothing. */
static void csp_nested_class(csp_context_t *ctx, TSNode member, const char *klass,
                             const char *outer, const char *parent);

/* `union { ... } data;` has no tag, so extraction names it for its owner and the member. With no
 * declarator at all its members belong to the class around it, and so do classes it holds. */
static void csp_unnamed_member_class(csp_context_t *ctx, TSNode member, TSNode type,
                                     const char *klass, const char *outer, const char *parent) {
    bool declared = false;
    TSTreeCursor cursor = ts_tree_cursor_new(member);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (!field || strcmp(field, "declarator"))
                continue;
            declared = true;
            TSNode name = ts_tree_cursor_current_node(&cursor);
            while (!ts_node_is_null(name) && !csp_is(name, "field_identifier") &&
                   !csp_is(name, "identifier")) {
                TSNode inner = csp_field(name, "declarator");
                name = !ts_node_is_null(inner)           ? inner
                       : ts_node_named_child_count(name) ? ts_node_named_child(name, 0)
                                                         : (TSNode){0};
            }
            const char *text = ts_node_is_null(name) ? NULL : csp_text(ctx, name);
            const char *named = text ? cbm_arena_sprintf(ctx->arena, "%s$%s", klass, text) : NULL;
            if (named)
                csp_class(ctx, type, parent, named);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    if (declared)
        return;
    TSTreeCursor inner = ts_tree_cursor_new(csp_field(type, "body"));
    if (ts_tree_cursor_goto_first_child(&inner))
        do {
            TSNode part = ts_tree_cursor_current_node(&inner);
            if (csp_is(part, "field_declaration"))
                csp_nested_class(ctx, part, klass, outer, parent);
        } while (ts_tree_cursor_goto_next_sibling(&inner));
    ts_tree_cursor_delete(&inner);
}

static void csp_nested_class(csp_context_t *ctx, TSNode member, const char *klass,
                             const char *outer, const char *parent) {
    TSNode type = csp_field(member, "type");
    if ((!csp_is(type, "struct_specifier") && !csp_is(type, "class_specifier") &&
         !csp_is(type, "union_specifier")) ||
        ts_node_is_null(csp_field(type, "body")))
        return;
    if (ts_node_is_null(csp_field(type, "name"))) {
        csp_unnamed_member_class(ctx, member, type, klass, outer, parent);
        return;
    }
    csp_class(ctx, type, outer, NULL);
    csp_class(ctx, type, parent, NULL);
}

static void csp_class(csp_context_t *ctx, TSNode node, const char *parent,
                      const char *typedef_name) {
    TSNode body = csp_field(node, "body"), name_node = csp_field(node, "name");
    /* `template <> struct Traits<int> {...}` replaces the primary's members for that argument. */
    if (csp_is(name_node, "template_type")) {
        const char *specialized = csp_text(ctx, csp_field(name_node, "name"));
        if (specialized)
            (void)csp_add_decl(ctx, CBM_C_DECL_SPECIALIZATION, specialized, 0);
    }
    if (ts_node_is_null(body) || (!csp_is(name_node, "type_identifier") && !typedef_name))
        return;
    const char *name =
        csp_is(name_node, "type_identifier") ? csp_text(ctx, name_node) : typedef_name;
    const char *qn = name ? cbm_arena_sprintf(ctx->arena, "%s.%s", parent, name) : NULL;
    if (!qn) {
        ctx->failed = true;
        return;
    }
    if (ctx->template_depth) {
        (void)csp_add_decl(ctx, CBM_C_DECL_CLASS_TEMPLATE, name, 0);
        csp_note_template_parameters(ctx, node, name);
    }
    if (ctx->conditional && cbm_ht_get(ctx->swapped_layouts, qn))
        return;
    bool complete = true, body_complete = true;
    uint32_t access =
        csp_is(node, "class_specifier") ? CBM_SOURCE_DECL_PRIVATE : CBM_SOURCE_DECL_PUBLIC;
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
                body_complete = false;
                break;
            }
            if (!ts_node_is_named(child) || csp_is(child, "comment"))
                continue;
            /* neovim writes `#define w_p_list w_onebuf_opt.wo_list` beside the member it names, and
             * from there to the end of the file it is a macro like any other. */
            if (csp_is(child, "preproc_def") || csp_is(child, "preproc_function_def"))
                csp_note_macro(ctx, child);
            if (csp_is(child, "access_specifier")) {
                const char *written = csp_text(ctx, child);
                access = written && !strcmp(written, "public")      ? CBM_SOURCE_DECL_PUBLIC
                         : written && !strcmp(written, "protected") ? CBM_SOURCE_DECL_PROTECTED
                                                                    : CBM_SOURCE_DECL_PRIVATE;
            } else if (csp_is(child, "function_definition") || csp_is(child, "field_declaration") ||
                       csp_is(child, "declaration")) {
                csp_method(ctx, child, qn, access);
                csp_note_member(ctx, child, name, qn, access);
                csp_nested_class(ctx, child, name, qn, parent);
            } else {
                complete = false;
                if (csp_is(child, "template_declaration") || csp_is(child, "using_declaration") ||
                    csp_is(child, "alias_declaration") || csp_is(child, "type_definition"))
                    csp_note_member(ctx, child, name, qn, access);
                else if (!csp_is(child, "friend_declaration") &&
                         !csp_is(child, "static_assert_declaration"))
                    body_complete = false;
            }
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    csp_mark(ctx, node, qn,
             CBM_SOURCE_DECL_KNOWN | (complete ? CBM_SOURCE_CLASS_DOMAIN_COMPLETE : 0) |
                 (body_complete ? CBM_SOURCE_CLASS_BODY_COMPLETE : 0),
             -1);
}
/* An #ifdef that splits a function's braces leaves `bool f(int x)` and its statements loose in
 * an ERROR. Definition extraction recovers that function from the expanded source, so it has an
 * entry here to find: the signature standing in the ERROR says how it is linked. */
static void csp_note_stranded_definitions(csp_context_t *ctx, TSNode error, const char *owner) {
    TSTreeCursor cursor = ts_tree_cursor_new(error);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        bool is_static = false;
        uint32_t first_line = 0;
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(child) || csp_is(child, "comment"))
                continue;
            if (csp_is(child, "storage_class_specifier")) {
                const char *word = csp_text(ctx, child);
                is_static |= word && !strcmp(word, "static");
                continue;
            } else if (csp_is(child, "primitive_type") || csp_is(child, "type_identifier") ||
                       csp_is(child, "sized_type_specifier") || csp_is(child, "type_qualifier") ||
                       csp_is(child, "struct_specifier")) {
                if (!first_line)
                    first_line = ts_node_start_point(child).row + 1;
                continue;
            } else if (csp_is(child, "function_declarator") ||
                       csp_is(child, "pointer_declarator")) {
                TSNode decl = child;
                while (csp_is(decl, "pointer_declarator"))
                    decl = csp_field(decl, "declarator");
                TSNode name = csp_is(decl, "function_declarator") ? csp_field(decl, "declarator")
                                                                  : (TSNode){0};
                const char *text = csp_is(name, "identifier") ? csp_text(ctx, name) : NULL;
                const char *qn = text ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, text) : NULL;
                uint32_t line = first_line ? first_line : ts_node_start_point(child).row + 1;
                csp_entry_t *entry = qn ? csp_entry(ctx, qn, line) : NULL;
                if (entry && !entry->duplicate && !ctx->preprocessor_unknown) {
                    CBMLSPDef *def = entry->definition;
                    bool cpp = def->lang == CBM_LANG_CPP || def->lang == CBM_LANG_CUDA;
                    def->source_flags = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_TOP_LEVEL |
                                        CBM_SOURCE_FUNCTION_DEFINITION;
                    if (!cpp || ctx->extern_c || ctx->file_extern_c)
                        def->source_flags |= CBM_SOURCE_DECL_EXTERN_C;
                    if (is_static)
                        def->source_flags |= CBM_SOURCE_DECL_STATIC;
                    def->namespace_name = "";
                }
            }
            is_static = false;
            first_line = 0;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
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

/* The macro this branch's whole condition asks to be defined: `#ifdef X`, `#if defined(X)`. */
static const char *csp_branch_needs_macro(csp_context_t *ctx, TSNode branch) {
    if (csp_is(branch, "preproc_ifdef")) {
        uint32_t start = ts_node_start_byte(branch);
        TSNode name = csp_field(branch, "name");
        if (ts_node_is_null(name) || start + 7 > ctx->length ||
            !memcmp(ctx->source + start, "#ifndef", 7))
            return NULL;
        return csp_text(ctx, name);
    }
    if (csp_is(branch, "preproc_if")) {
        TSNode condition = csp_field(branch, "condition");
        if (csp_is(condition, "preproc_defined") && ts_node_named_child_count(condition) == 1)
            return csp_text(ctx, ts_node_named_child(condition, 0));
    }
    return NULL;
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
    if (csp_is(node, "preproc_include")) {
        csp_note_quoted_include(ctx, node);
        return;
    }
    if (csp_preprocessor_branch(node)) {
        bool saved = ctx->conditional;
        uint32_t saved_group = ctx->branch_group, saved_branch = ctx->branch;
        const char *saved_needs = ctx->needs_macro;
        ctx->needs_macro = csp_branch_needs_macro(ctx, node);
        ctx->conditional = true;
        /* The alternatives of a conditional are nested inside its first branch's node. */
        ctx->branch = ts_node_start_byte(node) + 1;
        if (csp_is(node, "preproc_if") || csp_is(node, "preproc_ifdef"))
            ctx->branch_group = ctx->branch;
        csp_walk_children(ctx, node, owner, depth);
        ctx->conditional = saved;
        ctx->branch_group = saved_group;
        ctx->branch = saved_branch;
        ctx->needs_macro = saved_needs;
        return;
    }
    /* Error recovery around a directive still leaves its neighbours as complete
     * nodes. Each is judged by itself, and one with an error inside is skipped. */
    if (csp_is(node, "ERROR")) {
        csp_note_stranded_definitions(ctx, node, owner);
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
        if (csp_is(declarator, "type_identifier") && !ts_node_is_null(csp_field(type, "body")) &&
            ts_node_is_null(csp_field(type, "name")))
            (void)csp_add_decl(ctx, CBM_C_DECL_UNNAMED_TYPEDEF, csp_text(ctx, declarator), 0);
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
    /* Lua declares its API as `LUA_API int (lua_gettop) (lua_State *L);`. With the macro left out
     * the C++ grammar reads a functional cast being called, and file scope holds no expressions:
     * this is a prototype of the parenthesized name. */
    if (csp_is(node, "expression_statement")) {
        TSNode outer =
            ts_node_named_child_count(node) == 1 ? ts_node_named_child(node, 0) : (TSNode){0};
        TSNode inner =
            csp_is(outer, "call_expression") ? csp_field(outer, "function") : (TSNode){0};
        TSNode list =
            csp_is(inner, "call_expression") ? csp_field(inner, "arguments") : (TSNode){0};
        TSNode written = !ts_node_is_null(list) && ts_node_named_child_count(list) == 1
                             ? ts_node_named_child(list, 0)
                             : (TSNode){0};
        const char *name = csp_is(written, "identifier") ? csp_text(ctx, written) : NULL;
        const char *relative = name ? csp_relative(ctx, owner, name) : NULL;
        if (relative && ctx->facts)
            (void)csp_add_decl(
                ctx, CBM_C_DECL_PROTOTYPE, relative,
                CBM_C_DECL_EXTERNAL |
                    (ctx->extern_c || ctx->file_extern_c ? CBM_C_DECL_C_LINKAGE : 0));
        return;
    }
    /* What a template declares has the same name and linkage whatever its arguments. */
    if (csp_is(node, "template_declaration")) {
        ctx->template_depth++;
        csp_walk_children(ctx, node, owner, depth);
        ctx->template_depth--;
        return;
    }
    if (csp_is(node, "function_definition") || csp_is(node, "declaration")) {
        /* A definition with an error in its body still has this signature. */
        TSNode declarator = csp_field(node, "declarator"), type = csp_field(node, "type");
        if ((ts_node_is_null(declarator) || !ts_node_has_error(declarator) ||
             csp_declares_readable_function(node)) &&
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
        /* An anonymous namespace adds no segment to what it declares, and those are seen around it.
         */
        if (ts_node_is_null(csp_field(node, "name")) && !ts_node_is_null(body)) {
            csp_walk(ctx, body, owner, depth + 1);
            return;
        }
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
                         .definitions = cbm_ht_create(32),
                         .swapped_layouts = cbm_ht_create(8)};
    ctx.failed = !ctx.definitions || !ctx.swapped_layouts;
    ctx.preprocessor_unknown =
        cbm_c_preprocessor_domain_unsupported(ts_tree_root_node(tree), source);
    ctx.file_extern_c = strstr(source, "__cplusplus") && strstr(source, "extern \"C\"");
    if (facts && !facts->module_qn)
        facts->module_qn = module;
    CBMHashTable *seen_layouts = cbm_ht_create(8);
    ctx.failed |= !seen_layouts;
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
        if (def->label && !strcmp(def->label, "Class")) {
            const char *layout = cbm_arena_strndup(arena, qn, base);
            if (!layout)
                ctx.failed = true;
            else if (cbm_ht_get(seen_layouts, layout))
                cbm_ht_set(ctx.swapped_layouts, layout, entry);
            else
                cbm_ht_set(seen_layouts, layout, entry);
        }
    }
    cbm_ht_free(seen_layouts);
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
    cbm_ht_free(ctx.swapped_layouts);
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
    const CBMCFileFacts *includer; /* the first file seen including this one */
    bool several_includers;
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
        CDeclFileEntry *entry = cbm_arena_calloc(arena, sizeof(*entry));
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
    /* One pass over every include edge, after every file has its entry. */
    for (int i = 0; i < count; i++) {
        const CBMCFileFacts *facts = defs[i].c_file_facts;
        for (int n = 0; facts && facts->module_qn && n < facts->include_count; n++) {
            uint32_t slot = csp_hash(facts->includes[n]) & index->file_mask;
            for (CDeclFileEntry *e = index->files[slot]; e; e = e->next) {
                if (strcmp(csp_file_key(e->facts), facts->includes[n]))
                    continue;
                e->several_includers |= e->includer && e->includer != facts;
                e->includer = facts;
                break;
            }
        }
    }
    return index;
}

const CBMCFileFacts *cbm_c_sole_includer(const CBMCDeclIndex *index, const char *file) {
    if (!index || !file)
        return NULL;
    for (const CDeclFileEntry *e = index->files[csp_hash(file) & index->file_mask]; e; e = e->next)
        if (!strcmp(csp_file_key(e->facts), file))
            return e->several_includers ? NULL : e->includer;
    return NULL;
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
