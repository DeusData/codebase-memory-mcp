#include "c_lsp.h"
#include "lsp_node_iter.h"
#include "../helpers.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>

/* Safe kind accessor — returns CBM_TYPE_UNKNOWN for NULL types.
 * Prevents SEGV in c_eval_expr_type_inner on unusual C++ AST shapes. */
static inline CBMTypeKind safe_kind(const CBMType *t) {
    return t ? t->kind : CBM_TYPE_UNKNOWN;
}

// Forward declarations
static void c_resolve_calls_in_node(CLSPContext *ctx, TSNode node);
static void c_emit_resolved_call(CLSPContext *ctx, const char *callee_qn, const char *strategy,
                                 float confidence);
static void c_emit_unresolved_call(CLSPContext *ctx, const char *expr_text, const char *reason);
static const CBMType *c_lookup_field_type(CLSPContext *ctx, const char *type_qn,
                                          const char *field_name, int depth);
static void c_process_function(CLSPContext *ctx, TSNode func_node);
static void c_process_namespace(CLSPContext *ctx, TSNode ns_node);
static void c_process_class(CLSPContext *ctx, TSNode class_node);
static void c_process_body_child(CLSPContext *ctx, TSNode child);
static const char *type_to_qn(const CBMType *t);
const CBMType *c_simplify_type(CLSPContext *ctx, const CBMType *t, bool unwrap_pointer);
const CBMType *c_eval_expr_type(CLSPContext *ctx, TSNode node);

// External tree-sitter language functions (defined in grammar_*.c)
extern const TSLanguage *tree_sitter_c(void);
extern const TSLanguage *tree_sitter_cpp(void);

// --- Smart pointer names for deref ---
// Checks suffix: "std.shared_ptr", "test.main.std.shared_ptr" both match
static bool is_smart_ptr(const char *name) {
    if (!name)
        return false;
    // Extract short name after last dot (or use full name)
    const char *short_name = strrchr(name, '.');
    short_name = short_name ? short_name + 1 : name;
    // Check if the short name is a smart pointer type AND the QN contains "std"
    if (strcmp(short_name, "unique_ptr") == 0 || strcmp(short_name, "shared_ptr") == 0 ||
        strcmp(short_name, "weak_ptr") == 0 || strcmp(short_name, "auto_ptr") == 0 ||
        strcmp(short_name, "scoped_ptr") == 0) {
        return strstr(name, "std") != NULL || strstr(name, "boost") != NULL;
    }
    return false;
}

// --- Helper: get node text ---
static char *c_node_text(CLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

// --- Initialization ---

void c_lsp_init(CLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                const CBMTypeRegistry *registry, const char *module_qn, bool cpp_mode,
                CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(CLSPContext));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->module_qn = module_qn;
    ctx->cpp_mode = cpp_mode;
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL);

    const char *debug_env = getenv("CBM_LSP_DEBUG");
    ctx->debug = (debug_env && debug_env[0]);
}

void c_lsp_add_include(CLSPContext *ctx, const char *header_path, const char *ns_qn) {
    if (ctx->include_count % 32 == 0) {
        int new_cap = ctx->include_count + 32;
        const char **new_paths =
            (const char **)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(const char *));
        const char **new_qns =
            (const char **)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(const char *));
        if (!new_paths || !new_qns)
            return;
        if (ctx->include_paths && ctx->include_count > 0) {
            memcpy(new_paths, ctx->include_paths, ctx->include_count * sizeof(const char *));
            memcpy(new_qns, ctx->include_ns_qns, ctx->include_count * sizeof(const char *));
        }
        ctx->include_paths = new_paths;
        ctx->include_ns_qns = new_qns;
    }
    ctx->include_paths[ctx->include_count] = cbm_arena_strdup(ctx->arena, header_path);
    ctx->include_ns_qns[ctx->include_count] = cbm_arena_strdup(ctx->arena, ns_qn);
    ctx->include_count++;
}

// --- Helper: add using namespace ---
static void c_add_using_namespace(CLSPContext *ctx, const char *ns_qn) {
    if (ctx->using_ns_count >= ctx->using_ns_cap) {
        int new_cap = ctx->using_ns_cap == 0 ? 16 : ctx->using_ns_cap * 2;
        const char **new_arr =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        if (!new_arr)
            return;
        if (ctx->using_namespaces && ctx->using_ns_count > 0)
            memcpy(new_arr, ctx->using_namespaces, ctx->using_ns_count * sizeof(const char *));
        ctx->using_namespaces = new_arr;
        ctx->using_ns_cap = new_cap;
    }
    ctx->using_namespaces[ctx->using_ns_count++] = cbm_arena_strdup(ctx->arena, ns_qn);
}

// --- Helper: add using declaration ---
static void c_add_using_decl(CLSPContext *ctx, const char *short_name, const char *full_qn) {
    if (ctx->using_decl_count >= ctx->using_decl_cap) {
        int new_cap = ctx->using_decl_cap == 0 ? 16 : ctx->using_decl_cap * 2;
        const char **new_names =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        const char **new_qns =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        if (!new_names || !new_qns)
            return;
        if (ctx->using_decl_names && ctx->using_decl_count > 0) {
            memcpy(new_names, ctx->using_decl_names, ctx->using_decl_count * sizeof(const char *));
            memcpy(new_qns, ctx->using_decl_qns, ctx->using_decl_count * sizeof(const char *));
        }
        ctx->using_decl_names = new_names;
        ctx->using_decl_qns = new_qns;
        ctx->using_decl_cap = new_cap;
    }
    ctx->using_decl_names[ctx->using_decl_count] = cbm_arena_strdup(ctx->arena, short_name);
    ctx->using_decl_qns[ctx->using_decl_count] = cbm_arena_strdup(ctx->arena, full_qn);
    ctx->using_decl_count++;
}

// --- Helper: add namespace alias ---
static void c_add_ns_alias(CLSPContext *ctx, const char *alias, const char *full_qn) {
    if (ctx->ns_alias_count >= ctx->ns_alias_cap) {
        int new_cap = ctx->ns_alias_cap == 0 ? 8 : ctx->ns_alias_cap * 2;
        const char **new_names =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        const char **new_qns =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        if (!new_names || !new_qns)
            return;
        if (ctx->ns_alias_names && ctx->ns_alias_count > 0) {
            memcpy(new_names, ctx->ns_alias_names, ctx->ns_alias_count * sizeof(const char *));
            memcpy(new_qns, ctx->ns_alias_qns, ctx->ns_alias_count * sizeof(const char *));
        }
        ctx->ns_alias_names = new_names;
        ctx->ns_alias_qns = new_qns;
        ctx->ns_alias_cap = new_cap;
    }
    ctx->ns_alias_names[ctx->ns_alias_count] = cbm_arena_strdup(ctx->arena, alias);
    ctx->ns_alias_qns[ctx->ns_alias_count] = cbm_arena_strdup(ctx->arena, full_qn);
    ctx->ns_alias_count++;
}

// --- Helper: track function pointer target ---
static void c_add_fp_target(CLSPContext *ctx, const char *var_name, const char *target_qn) {
    if (ctx->fp_count >= ctx->fp_cap) {
        int new_cap = ctx->fp_cap == 0 ? 16 : ctx->fp_cap * 2;
        const char **new_names =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        const char **new_qns =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        if (!new_names || !new_qns)
            return;
        if (ctx->fp_var_names && ctx->fp_count > 0) {
            memcpy(new_names, ctx->fp_var_names, ctx->fp_count * sizeof(const char *));
            memcpy(new_qns, ctx->fp_target_qns, ctx->fp_count * sizeof(const char *));
        }
        ctx->fp_var_names = new_names;
        ctx->fp_target_qns = new_qns;
        ctx->fp_cap = new_cap;
    }
    ctx->fp_var_names[ctx->fp_count] = cbm_arena_strdup(ctx->arena, var_name);
    ctx->fp_target_qns[ctx->fp_count] = cbm_arena_strdup(ctx->arena, target_qn);
    ctx->fp_count++;
}

// Look up function pointer target
static const char *c_lookup_fp_target(CLSPContext *ctx, const char *var_name) {
    for (int i = ctx->fp_count - 1; i >= 0; i--) {
        if (strcmp(ctx->fp_var_names[i], var_name) == 0)
            return ctx->fp_target_qns[i];
    }
    return NULL;
}

// --- Helper: extract function name from DLL/dynamic resolver call ---
// Heuristic: if an expression is a call (possibly cast-wrapped) with a string
// literal argument, the string is likely an external function name being resolved
// dynamically (GetProcAddress, dlsym, or custom resolver).
// Returns the function name string (without quotes), or NULL.
// Sets *out_has_cast to true if the expression was wrapped in a cast.
static const char *c_extract_dll_resolve_name(CLSPContext *ctx, TSNode expr, bool *out_has_cast) {
    if (ts_node_is_null(expr))
        return NULL;
    *out_has_cast = false;

    // Unwrap cast expressions to find inner call
    TSNode inner = expr;
    const char *ik = ts_node_type(inner);
    for (int unwrap_depth = 0; unwrap_depth < 8; unwrap_depth++) {
        // Standard cast nodes: (Type)expr, static_cast<T>(expr) etc.
        if (strcmp(ik, "cast_expression") == 0 || strcmp(ik, "static_cast_expression") == 0 ||
            strcmp(ik, "reinterpret_cast_expression") == 0 ||
            strcmp(ik, "dynamic_cast_expression") == 0 ||
            strcmp(ik, "const_cast_expression") == 0) {
            *out_has_cast = true;
            uint32_t nc = ts_node_named_child_count(inner);
            if (nc == 0)
                return NULL;
            inner = ts_node_named_child(inner, nc - 1);
            if (ts_node_is_null(inner))
                return NULL;
            ik = ts_node_type(inner);
            continue;
        }
        // C++ named casts may parse as call_expression with template_function:
        // static_cast<T>(expr) → call_expression(template_function("static_cast",<T>), (expr))
        if (strcmp(ik, "call_expression") == 0) {
            TSNode fn = ts_node_child_by_field_name(inner, "function", 8);
            if (!ts_node_is_null(fn)) {
                char *fname = NULL;
                const char *fk = ts_node_type(fn);
                if (strcmp(fk, "template_function") == 0) {
                    TSNode nn = ts_node_child_by_field_name(fn, "name", 4);
                    if (!ts_node_is_null(nn))
                        fname = c_node_text(ctx, nn);
                } else if (strcmp(fk, "identifier") == 0) {
                    fname = c_node_text(ctx, fn);
                }
                if (fname &&
                    (strcmp(fname, "static_cast") == 0 || strcmp(fname, "reinterpret_cast") == 0 ||
                     strcmp(fname, "dynamic_cast") == 0 || strcmp(fname, "const_cast") == 0)) {
                    *out_has_cast = true;
                    TSNode cargs = ts_node_child_by_field_name(inner, "arguments", 9);
                    if (!ts_node_is_null(cargs) && ts_node_named_child_count(cargs) > 0) {
                        inner = ts_node_named_child(cargs, 0);
                        if (ts_node_is_null(inner))
                            return NULL;
                        ik = ts_node_type(inner);
                        continue;
                    }
                    return NULL;
                }
            }
        }
        break;
    }

    // Must be a call_expression
    if (strcmp(ik, "call_expression") != 0)
        return NULL;

    // Scan call arguments for a string literal
    TSNode args = ts_node_child_by_field_name(inner, "arguments", 9);
    if (ts_node_is_null(args))
        return NULL;

    uint32_t anc = ts_node_named_child_count(args);
    for (uint32_t i = 0; i < anc; i++) {
        TSNode arg = ts_node_named_child(args, i);
        if (ts_node_is_null(arg))
            continue;
        const char *ak = ts_node_type(arg);
        if (strcmp(ak, "string_literal") != 0 && strcmp(ak, "raw_string_literal") != 0)
            continue;
        char *text = c_node_text(ctx, arg);
        if (!text)
            continue;
        size_t len = strlen(text);
        if (len < 2 || text[0] != '"' || text[len - 1] != '"')
            continue;
        char *name = cbm_arena_strndup(ctx->arena, text + 1, len - 2);
        if (!name || !name[0])
            continue;
        // Validate: function names are identifiers (no spaces, path separators, dots)
        bool valid = true;
        for (const char *p = name; *p; p++) {
            if (*p == ' ' || *p == '/' || *p == '\\' || *p == '.') {
                valid = false;
                break;
            }
        }
        if (valid)
            return name;
    }
    return NULL;
}

// --- Helper: pending template calls (member calls on TYPE_PARAM inside templates) ---
static void c_add_pending_template_call(CLSPContext *ctx, const char *func_qn,
                                        const char *type_param, const char *method_name,
                                        int arg_count) {
    if (!func_qn || !type_param || !method_name)
        return;
    if (ctx->pending_tc_count >= ctx->pending_tc_cap) {
        int new_cap = ctx->pending_tc_cap == 0 ? 16 : ctx->pending_tc_cap * 2;
        size_t sz = new_cap * sizeof(ctx->pending_template_calls[0]);
        void *new_arr = cbm_arena_alloc(ctx->arena, sz);
        if (!new_arr)
            return;
        if (ctx->pending_template_calls && ctx->pending_tc_count > 0)
            memcpy(new_arr, ctx->pending_template_calls,
                   ctx->pending_tc_count * sizeof(ctx->pending_template_calls[0]));
        ctx->pending_template_calls = new_arr;
        ctx->pending_tc_cap = new_cap;
    }
    const char *func_qn_copy = cbm_arena_strdup(ctx->arena, func_qn);
    const char *type_param_copy = cbm_arena_strdup(ctx->arena, type_param);
    const char *method_name_copy = cbm_arena_strdup(ctx->arena, method_name);
    if (!func_qn_copy || !type_param_copy || !method_name_copy)
        return;

    int i = ctx->pending_tc_count++;
    ctx->pending_template_calls[i].func_qn = func_qn_copy;
    ctx->pending_template_calls[i].type_param = type_param_copy;
    ctx->pending_template_calls[i].method_name = method_name_copy;
    ctx->pending_template_calls[i].arg_count = arg_count;
}

// Resolve pending template calls for a function being called with known arg types.
// Deduces type params from call-site args and resolves stored method calls.
static void c_resolve_pending_template_calls(CLSPContext *ctx, const CBMRegisteredFunc *callee,
                                             const CBMType **call_arg_types, int call_arg_count) {
    if (!callee || !callee->type_param_names || !call_arg_types)
        return;

    // Build type param → concrete type mapping from call-site arguments
    const char **tpn = callee->type_param_names;
    const CBMType *param_map[8] = {0};
    int tpn_count = 0;
    while (tpn[tpn_count] && tpn_count < 8)
        tpn_count++;

    // Match call arg types against function param types to deduce type params.
    // The call site may contain more arguments than the parsed function signature
    // knows about (invalid code, macros, variadic calls, or parser recovery).  The
    // signature arrays are NULL-terminated, so never index past the sentinel.
    if (callee->signature && callee->signature->kind == CBM_TYPE_FUNC &&
        callee->signature->data.func.param_types) {
        int formal_count = 0;
        while (callee->signature->data.func.param_types[formal_count])
            formal_count++;
        int limit = call_arg_count < formal_count ? call_arg_count : formal_count;
        for (int i = 0; i < limit; i++) {
            const CBMType *formal = callee->signature->data.func.param_types[i];
            if (!formal || !call_arg_types[i])
                continue;
            // Unwrap references/pointers
            while (formal &&
                   (formal->kind == CBM_TYPE_REFERENCE || formal->kind == CBM_TYPE_RVALUE_REF ||
                    formal->kind == CBM_TYPE_POINTER)) {
                formal = (formal->kind == CBM_TYPE_POINTER) ? formal->data.pointer.elem
                                                            : formal->data.reference.elem;
            }
            if (formal && formal->kind == CBM_TYPE_TYPE_PARAM) {
                for (int j = 0; j < tpn_count; j++) {
                    if (strcmp(tpn[j], formal->data.type_param.name) == 0) {
                        const CBMType *arg = call_arg_types[i];
                        arg = c_simplify_type(ctx, arg, false);
                        param_map[j] = arg;
                    }
                }
            }
        }
    }

    // Resolve pending calls for this function — emit with template func as caller
    const char *saved_func_qn = ctx->enclosing_func_qn;
    ctx->enclosing_func_qn = callee->qualified_name;
    for (int i = 0; i < ctx->pending_tc_count; i++) {
        if (!ctx->pending_template_calls[i].func_qn || !ctx->pending_template_calls[i].type_param ||
            !ctx->pending_template_calls[i].method_name)
            continue;
        if (strcmp(ctx->pending_template_calls[i].func_qn, callee->qualified_name) != 0)
            continue;
        const char *tp = ctx->pending_template_calls[i].type_param;
        const char *method = ctx->pending_template_calls[i].method_name;

        // Find which type param this is
        for (int j = 0; j < tpn_count; j++) {
            if (strcmp(tpn[j], tp) != 0 || !param_map[j])
                continue;
            const char *concrete_qn = type_to_qn(param_map[j]);
            if (!concrete_qn)
                continue;
            // Look up the method on the concrete type
            const CBMRegisteredFunc *m = c_lookup_member(ctx, concrete_qn, method);
            if (m) {
                c_emit_resolved_call(ctx, m->qualified_name, "lsp_template_instantiation", 0.90f);
            }
            break;
        }
    }
    ctx->enclosing_func_qn = saved_func_qn;
}

// --- Helper: extract call argument types for overload scoring ---
static const CBMType **c_extract_call_arg_types(CLSPContext *ctx, TSNode call_node,
                                                int *out_count) {
    TSNode args = ts_node_child_by_field_name(call_node, "arguments", 9);
    if (ts_node_is_null(args)) {
        *out_count = 0;
        return NULL;
    }
    uint32_t nc = ts_node_named_child_count(args);
    if (nc == 0) {
        *out_count = 0;
        return NULL;
    }
    const CBMType **types =
        (const CBMType **)cbm_arena_alloc(ctx->arena, (nc + 1) * sizeof(const CBMType *));
    if (!types) {
        *out_count = 0;
        return NULL;
    }
    int count = 0;
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(args, i);
        if (!ts_node_is_null(child)) {
            types[count++] = c_eval_expr_type(ctx, child);
        }
    }
    types[count] = NULL;
    *out_count = count;
    return types;
}

// --- Helper: parse template parameter defaults ---
static void c_parse_template_params(CLSPContext *ctx, TSNode template_decl) {
    TSNode params = ts_node_child_by_field_name(template_decl, "parameters", 10);
    if (ts_node_is_null(params))
        return;

    ctx->template_param_count = 0;
    uint32_t nc = ts_node_named_child_count(params);
    if (nc == 0)
        return;

    const char **names =
        (const char **)cbm_arena_alloc(ctx->arena, (nc + 1) * sizeof(const char *));
    const CBMType **defaults =
        (const CBMType **)cbm_arena_alloc(ctx->arena, (nc + 1) * sizeof(const CBMType *));
    if (!names || !defaults)
        return;

    int idx = 0;
    for (uint32_t i = 0; i < nc && idx < 16; i++) {
        TSNode param = ts_node_named_child(params, i);
        if (ts_node_is_null(param))
            continue;
        const char *pk = ts_node_type(param);

        // type_parameter_declaration: template<class T = int>
        // optional_type_parameter_declaration: template<class T = Default>
        if (strcmp(pk, "type_parameter_declaration") == 0 ||
            strcmp(pk, "optional_type_parameter_declaration") == 0) {
            // Get parameter name
            TSNode name_node = ts_node_child_by_field_name(param, "name", 4);
            if (ts_node_is_null(name_node)) {
                // Some grammars put the identifier as a direct child
                for (uint32_t j = 0; j < ts_node_named_child_count(param); j++) {
                    TSNode ch = ts_node_named_child(param, j);
                    if (strcmp(ts_node_type(ch), "type_identifier") == 0 ||
                        strcmp(ts_node_type(ch), "identifier") == 0) {
                        name_node = ch;
                        break;
                    }
                }
            }

            char *pname = NULL;
            if (!ts_node_is_null(name_node))
                pname = c_node_text(ctx, name_node);
            if (!pname)
                pname = cbm_arena_sprintf(ctx->arena, "T%d", idx);

            names[idx] = pname;

            // Get default type
            TSNode default_node = ts_node_child_by_field_name(param, "default_type", 12);
            if (ts_node_is_null(default_node)) {
                // Try "default_value" field
                default_node = ts_node_child_by_field_name(param, "default", 7);
            }
            if (!ts_node_is_null(default_node)) {
                defaults[idx] = c_parse_type_node(ctx, default_node);
            } else {
                defaults[idx] = NULL;
            }
            idx++;
        }
    }

    names[idx] = NULL;
    defaults[idx] = NULL;
    ctx->template_param_names = names;
    ctx->template_param_defaults = defaults;
    ctx->template_param_count = idx;
}

// Resolve a template type parameter using defaults from current template scope
static const CBMType *c_resolve_template_param(CLSPContext *ctx, const char *param_name) {
    if (!param_name || !ctx->template_param_names)
        return NULL;
    for (int i = 0; i < ctx->template_param_count; i++) {
        if (ctx->template_param_names[i] && strcmp(ctx->template_param_names[i], param_name) == 0) {
            return ctx->template_param_defaults[i]; // may be NULL if no default
        }
    }
    return NULL;
}

// --- Helper: resolve namespace alias ---
static const char *c_resolve_ns_alias(CLSPContext *ctx, const char *name) {
    for (int i = 0; i < ctx->ns_alias_count; i++) {
        if (strcmp(ctx->ns_alias_names[i], name) == 0)
            return ctx->ns_alias_qns[i];
    }
    return NULL;
}

// --- C/C++ builtin check ---
static bool is_c_builtin_type(const char *name) {
    static const char *builtins[] = {
        "int",     "char",     "short",    "long",      "float",    "double",    "void",
        "bool",    "size_t",   "ssize_t",  "ptrdiff_t", "intptr_t", "uintptr_t", "int8_t",
        "int16_t", "int32_t",  "int64_t",  "uint8_t",   "uint16_t", "uint32_t",  "uint64_t",
        "wchar_t", "char16_t", "char32_t", "char8_t",   "unsigned", "signed",    NULL};
    for (const char **b = builtins; *b; b++) {
        if (strcmp(name, *b) == 0)
            return true;
    }
    return false;
}

static bool is_c_builtin_func(const char *name) {
    // C stdlib functions are registered in the registry, not hardcoded here.
    // But we skip certain compiler builtins that should not generate CALLS edges.
    static const char *skip[] = {"__builtin_expect",
                                 "__builtin_unreachable",
                                 "__builtin_offsetof",
                                 "__builtin_va_start",
                                 "__builtin_va_end",
                                 "__builtin_va_arg",
                                 "sizeof",
                                 "alignof",
                                 "_Alignof",
                                 "typeof",
                                 "decltype",
                                 "static_assert",
                                 "_Static_assert",
                                 NULL};
    for (const char **b = skip; *b; b++) {
        if (strcmp(name, *b) == 0)
            return true;
    }
    return false;
}

// --- Qualified name construction ---

// Build a qualified name from a namespace-qualified identifier.
// Converts "::" to "." for our QN format.
// Inline ABI namespaces to strip during QN construction.
// Only ABI-internal segments, NOT __detail (contains internal types).
static bool is_inline_abi_ns(const char *seg, size_t seg_len) {
    return (seg_len == 3 && memcmp(seg, "__1", 3) == 0) ||
           (seg_len == 7 && memcmp(seg, "__cxx11", 7) == 0) ||
           (seg_len == 9 && memcmp(seg, "__gnu_cxx", 9) == 0);
}

static const char *c_build_qn(CLSPContext *ctx, const char *text) {
    if (!text)
        return NULL;
    // Replace :: with . and strip inline ABI namespaces
    size_t len = strlen(text);
    char *buf = (char *)cbm_arena_alloc(ctx->arena, len + 1);
    if (!buf)
        return text;
    size_t j = 0;
    for (size_t i = 0; i < len; i++) {
        if (text[i] == ':' && i + 1 < len && text[i + 1] == ':') {
            // Peek at next segment to check for inline ABI namespace
            size_t seg_start = i + 2;
            size_t seg_end = seg_start;
            while (seg_end < len && text[seg_end] != ':')
                seg_end++;
            size_t seg_len = seg_end - seg_start;
            if (seg_len > 0 && is_inline_abi_ns(text + seg_start, seg_len) && seg_end + 1 < len &&
                text[seg_end] == ':' && text[seg_end + 1] == ':') {
                // Skip ABI ns + its trailing ::
                i = seg_end + 1; // loop increments to seg_end+2 = after "::"
                // Don't emit a dot — we're removing this segment
                continue;
            }
            buf[j++] = '.';
            i++; // skip second ':'
        } else {
            buf[j++] = text[i];
        }
    }
    buf[j] = '\0';
    return buf;
}

// --- Name lookup (C++ multi-scope resolution) ---

// Look up a name following C++ name resolution order:
// 1. Current scope chain
// 2. Using declarations
// 3. Enclosing class (implicit this)
// 4. Using namespaces
// 5. Current namespace
// 6. Module scope
static const char *c_resolve_name(CLSPContext *ctx, const char *name) {
    if (!name)
        return NULL;

    // 1. Scope lookup
    const CBMType *t = cbm_scope_lookup(ctx->current_scope, name);
    if (!cbm_type_is_unknown(t))
        return NULL; // found in scope, not a function

    // 2. Using declarations
    for (int i = 0; i < ctx->using_decl_count; i++) {
        if (strcmp(ctx->using_decl_names[i], name) == 0)
            return ctx->using_decl_qns[i];
    }

    // 3. Enclosing class (implicit this)
    if (ctx->enclosing_class_qn) {
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_method(ctx->registry, ctx->enclosing_class_qn, name);
        if (f)
            return f->qualified_name;
    }

    // 4. Using namespaces
    for (int i = 0; i < ctx->using_ns_count; i++) {
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->using_namespaces[i], name);
        if (f)
            return f->qualified_name;
    }

    // 5. Current namespace + parent namespaces
    if (ctx->current_namespace) {
        const char *ns = ctx->current_namespace;
        while (ns && ns[0]) {
            const CBMRegisteredFunc *f = cbm_registry_lookup_symbol(ctx->registry, ns, name);
            if (f)
                return f->qualified_name;
            // Walk up: "a.b.c" -> "a.b" -> "a"
            const char *dot = strrchr(ns, '.');
            if (!dot)
                break;
            char *parent = (char *)cbm_arena_alloc(ctx->arena, (size_t)(dot - ns) + 1);
            if (!parent)
                break;
            memcpy(parent, ns, (size_t)(dot - ns));
            parent[dot - ns] = '\0';
            ns = parent;
        }
    }

    // 6. Module scope
    if (ctx->module_qn) {
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, name);
        if (f)
            return f->qualified_name;
    }

    // 7. Global scope (no prefix — C stdlib functions)
    const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, name);
    if (f)
        return f->qualified_name;

    return NULL;
}

// --- ADL (Argument-Dependent Lookup) ---
// Extract the namespace from a qualified name: "std.vector" → "std", "boost.asio.ip" → "boost.asio"
static const char *extract_namespace_from_qn(CBMArena *arena, const char *qn) {
    if (!qn)
        return NULL;
    const char *dot = strrchr(qn, '.');
    if (!dot || dot == qn)
        return NULL;
    size_t len = (size_t)(dot - qn);
    char *ns = (char *)cbm_arena_alloc(arena, len + 1);
    if (!ns)
        return NULL;
    memcpy(ns, qn, len);
    ns[len] = '\0';
    return ns;
}

// ADL: resolve unqualified function call by searching namespaces of argument types.
// E.g., swap(a, b) where a is std::string → look up std::swap.
static const char *c_adl_resolve(CLSPContext *ctx, const char *name, TSNode call_node) {
    if (!ctx->cpp_mode || !name)
        return NULL;

    TSNode args = ts_node_child_by_field_name(call_node, "arguments", 9);
    if (ts_node_is_null(args))
        return NULL;

    uint32_t nc = ts_node_named_child_count(args);
    if (nc == 0)
        return NULL;

    // Collect unique namespace QNs from argument types (max 8 to bound work)
    const char *namespaces[8];
    int ns_count = 0;

    for (uint32_t i = 0; i < nc && ns_count < 8; i++) {
        TSNode arg = ts_node_named_child(args, i);
        if (ts_node_is_null(arg))
            continue;

        const CBMType *arg_type = c_eval_expr_type(ctx, arg);
        const char *qn = type_to_qn(c_simplify_type(ctx, arg_type, false));
        if (!qn)
            continue;

        const char *ns = extract_namespace_from_qn(ctx->arena, qn);
        if (!ns)
            continue;

        // Deduplicate
        bool dup = false;
        for (int j = 0; j < ns_count; j++) {
            if (strcmp(namespaces[j], ns) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup)
            namespaces[ns_count++] = ns;
    }

    // Try each namespace
    for (int i = 0; i < ns_count; i++) {
        const CBMRegisteredFunc *f = cbm_registry_lookup_symbol(ctx->registry, namespaces[i], name);
        if (f)
            return f->qualified_name;
    }

    return NULL;
}

// Resolve a name to a function QN via registry only (skips scope check).
// Used for function pointer target resolution where the name IS in scope
// as a function definition but we need the registry QN, not NULL.
static const char *c_resolve_name_to_func_qn(CLSPContext *ctx, const char *name) {
    if (!name)
        return NULL;

    // Using declarations
    for (int i = 0; i < ctx->using_decl_count; i++) {
        if (strcmp(ctx->using_decl_names[i], name) == 0)
            return ctx->using_decl_qns[i];
    }

    // Enclosing class
    if (ctx->enclosing_class_qn) {
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_method(ctx->registry, ctx->enclosing_class_qn, name);
        if (f)
            return f->qualified_name;
    }

    // Using namespaces
    for (int i = 0; i < ctx->using_ns_count; i++) {
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->using_namespaces[i], name);
        if (f)
            return f->qualified_name;
    }

    // Current namespace + parents
    if (ctx->current_namespace) {
        const char *ns = ctx->current_namespace;
        while (ns && ns[0]) {
            const CBMRegisteredFunc *f = cbm_registry_lookup_symbol(ctx->registry, ns, name);
            if (f)
                return f->qualified_name;
            const char *dot = strrchr(ns, '.');
            if (!dot)
                break;
            char *parent = (char *)cbm_arena_alloc(ctx->arena, (size_t)(dot - ns) + 1);
            if (!parent)
                break;
            memcpy(parent, ns, (size_t)(dot - ns));
            parent[dot - ns] = '\0';
            ns = parent;
        }
    }

    // Module scope
    if (ctx->module_qn) {
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->module_qn, name);
        if (f)
            return f->qualified_name;
    }

    // Global scope
    const CBMRegisteredFunc *gf = cbm_registry_lookup_func(ctx->registry, name);
    if (gf)
        return gf->qualified_name;

    return NULL;
}

// Resolve a name to a type (for identifiers used as types)
static const CBMType *c_resolve_name_to_type(CLSPContext *ctx, const char *name) {
    if (!name)
        return cbm_type_unknown();

    // Builtin types
    if (is_c_builtin_type(name))
        return cbm_type_builtin(ctx->arena, name);

    // Scope lookup: typedef/using aliases are bound here.
    // If the name resolves to an alias, follow it to the underlying type.
    {
        const CBMType *scoped = cbm_scope_lookup(ctx->current_scope, name);
        if (scoped && scoped->kind == CBM_TYPE_ALIAS) {
            const CBMType *resolved = cbm_type_resolve_alias(scoped);
            if (resolved && !cbm_type_is_unknown(resolved))
                return resolved;
        }
    }

    // Template type parameters (T, U, etc.)
    if (ctx->in_template && ctx->template_param_names) {
        for (int i = 0; i < ctx->template_param_count; i++) {
            if (ctx->template_param_names[i] && strcmp(ctx->template_param_names[i], name) == 0) {
                // Return default if available, otherwise TYPE_PARAM
                if (ctx->template_param_defaults && ctx->template_param_defaults[i])
                    return ctx->template_param_defaults[i];
                return cbm_type_type_param(ctx->arena, name);
            }
        }
    }

    // Using declarations
    for (int i = 0; i < ctx->using_decl_count; i++) {
        if (strcmp(ctx->using_decl_names[i], name) == 0) {
            const CBMRegisteredType *rt =
                cbm_registry_lookup_type(ctx->registry, ctx->using_decl_qns[i]);
            if (rt)
                return cbm_type_named(ctx->arena, rt->qualified_name);
        }
    }

    // Enclosing class (nested types: Factory::Product)
    if (ctx->enclosing_class_qn) {
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->enclosing_class_qn, name);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, qn);
        if (rt)
            return cbm_type_named(ctx->arena, qn);
    }

    // Current namespace
    if (ctx->current_namespace) {
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace, name);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, qn);
        if (rt)
            return cbm_type_named(ctx->arena, qn);
    }

    // Module scope
    if (ctx->module_qn) {
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, qn);
        if (rt)
            return cbm_type_named(ctx->arena, qn);
    }

    // Using namespaces
    for (int i = 0; i < ctx->using_ns_count; i++) {
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->using_namespaces[i], name);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, qn);
        if (rt)
            return cbm_type_named(ctx->arena, qn);
    }

    // Try std:: prefix (very common)
    if (ctx->cpp_mode) {
        const char *std_qn = cbm_arena_sprintf(ctx->arena, "std.%s", name);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, std_qn);
        if (rt)
            return cbm_type_named(ctx->arena, std_qn);
    }

    // Unresolved — return named with module prefix
    if (ctx->module_qn) {
        return cbm_type_named(ctx->arena,
                              cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, name));
    }
    return cbm_type_named(ctx->arena, name);
}

// ============================================================================
// c_parse_type_node: AST type node -> CBMType
// ============================================================================

const CBMType *c_parse_type_node(CLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();
    const char *kind = ts_node_type(node);

    // primitive_type: int, double, char, void, bool
    if (strcmp(kind, "primitive_type") == 0) {
        char *name = c_node_text(ctx, node);
        return name ? cbm_type_builtin(ctx->arena, name) : cbm_type_unknown();
    }

    // sized_type_specifier: unsigned int, long long, etc.
    if (strcmp(kind, "sized_type_specifier") == 0) {
        char *name = c_node_text(ctx, node);
        return name ? cbm_type_builtin(ctx->arena, name) : cbm_type_unknown();
    }

    // type_identifier: Foo, MyClass
    if (strcmp(kind, "type_identifier") == 0) {
        char *name = c_node_text(ctx, node);
        if (!name)
            return cbm_type_unknown();
        return c_resolve_name_to_type(ctx, name);
    }

    // qualified_identifier: std::vector, ns::Foo
    if (strcmp(kind, "qualified_identifier") == 0 || strcmp(kind, "scoped_type_identifier") == 0) {
        // Check if name field is a template_type (e.g., std::vector<Widget>)
        // If so, build scoped QN prefix and delegate to template_type handler
        TSNode name_child = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(name_child) &&
            strcmp(ts_node_type(name_child), "template_type") == 0) {
            // Build scope prefix from scope chain
            TSNode scope_node = ts_node_child_by_field_name(node, "scope", 5);
            char *scope_text = NULL;
            if (!ts_node_is_null(scope_node)) {
                scope_text = c_node_text(ctx, scope_node);
                if (scope_text) {
                    const char *alias_qn = c_resolve_ns_alias(ctx, scope_text);
                    if (alias_qn)
                        scope_text = (char *)alias_qn;
                }
            }
            // Parse the template_type's own name
            TSNode tmpl_name = ts_node_child_by_field_name(name_child, "name", 4);
            TSNode tmpl_args = ts_node_child_by_field_name(name_child, "arguments", 9);
            char *tmpl_name_text = NULL;
            if (!ts_node_is_null(tmpl_name))
                tmpl_name_text = c_node_text(ctx, tmpl_name);
            if (!tmpl_name_text)
                return cbm_type_unknown();

            // Build full QN: scope.template_name
            const char *template_qn;
            if (scope_text) {
                const char *scoped =
                    cbm_arena_sprintf(ctx->arena, "%s::%s", scope_text, tmpl_name_text);
                template_qn = c_build_qn(ctx, scoped);
            } else {
                template_qn = c_build_qn(ctx, tmpl_name_text);
            }

            // Resolve against registry
            const CBMRegisteredType *trt = cbm_registry_lookup_type(ctx->registry, template_qn);
            if (!trt && ctx->module_qn) {
                const char *mod_qn =
                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, template_qn);
                trt = cbm_registry_lookup_type(ctx->registry, mod_qn);
                if (trt)
                    template_qn = mod_qn;
            }

            // Parse template arguments
            const CBMType *targs[16] = {
                NULL}; /* zero-fill: cbm_type_substitute requires NULL-terminated args
                          (uninitialized tail bound T to stack garbage -> corrupt type graph,
                          bitcoin serialize.h) */
            int targ_count = 0;
            if (!ts_node_is_null(tmpl_args)) {
                uint32_t nc = ts_node_named_child_count(tmpl_args);
                for (uint32_t i = 0; i < nc && targ_count < 15; i++) {
                    TSNode arg = ts_node_named_child(tmpl_args, i);
                    if (ts_node_is_null(arg))
                        continue;
                    const char *ak = ts_node_type(arg);
                    if (strcmp(ak, "type_descriptor") == 0) {
                        if (ts_node_named_child_count(arg) > 0)
                            targs[targ_count++] =
                                c_parse_type_node(ctx, ts_node_named_child(arg, 0));
                    } else {
                        targs[targ_count++] = c_parse_type_node(ctx, arg);
                    }
                }
            }

            if (targ_count > 0)
                return cbm_type_template(ctx->arena, template_qn, targs, targ_count);
            return cbm_type_named(ctx->arena, template_qn);
        }

        char *text = c_node_text(ctx, node);
        if (!text)
            return cbm_type_unknown();

        // Check for namespace alias at start
        TSNode scope_node = ts_node_child_by_field_name(node, "scope", 5);
        if (!ts_node_is_null(scope_node)) {
            char *scope_text = c_node_text(ctx, scope_node);
            if (scope_text) {
                const char *alias_qn = c_resolve_ns_alias(ctx, scope_text);
                if (alias_qn) {
                    if (!ts_node_is_null(name_child)) {
                        char *name = c_node_text(ctx, name_child);
                        if (name)
                            text = (char *)cbm_arena_sprintf(ctx->arena, "%s::%s", alias_qn, name);
                    }
                }
            }
        }

        const char *qn = c_build_qn(ctx, text);
        // Check registry
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, qn);
        if (rt)
            return cbm_type_named(ctx->arena, qn);
        return cbm_type_named(ctx->arena, qn);
    }

    // template_type: vector<int>, map<K,V>
    if (strcmp(kind, "template_type") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode args_node = ts_node_child_by_field_name(node, "arguments", 9);

        char *name_text = NULL;
        if (!ts_node_is_null(name_node)) {
            name_text = c_node_text(ctx, name_node);
        }
        if (!name_text)
            return cbm_type_unknown();

        const char *template_qn = c_build_qn(ctx, name_text);

        // Try resolving short name to full QN
        const CBMRegisteredType *trt = cbm_registry_lookup_type(ctx->registry, template_qn);
        if (!trt && ctx->module_qn) {
            // Try with module prefix
            const char *mod_qn =
                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, template_qn);
            trt = cbm_registry_lookup_type(ctx->registry, mod_qn);
            if (trt)
                template_qn = mod_qn;
        }
        if (!trt && ctx->current_namespace) {
            // Try with current namespace prefix (e.g., inside namespace std, "shared_ptr" →
            // "std.shared_ptr")
            const char *ns_qn =
                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace, template_qn);
            trt = cbm_registry_lookup_type(ctx->registry, ns_qn);
            if (trt)
                template_qn = ns_qn;
            // Also try module + namespace
            if (!trt && ctx->module_qn) {
                const char *full_qn = cbm_arena_sprintf(ctx->arena, "%s.%s.%s", ctx->module_qn,
                                                        ctx->current_namespace, template_qn);
                trt = cbm_registry_lookup_type(ctx->registry, full_qn);
                if (trt)
                    template_qn = full_qn;
            }
        }
        if (!trt) {
            // Try with std:: prefix
            const char *std_qn = cbm_arena_sprintf(ctx->arena, "std.%s", template_qn);
            trt = cbm_registry_lookup_type(ctx->registry, std_qn);
            if (trt)
                template_qn = std_qn;
        }

        // Parse template arguments
        const CBMType *targs[16] = {
            NULL}; /* zero-fill: cbm_type_substitute requires NULL-terminated args (uninitialized
                      tail bound T to stack garbage -> corrupt type graph, bitcoin serialize.h) */
        int targ_count = 0;
        if (!ts_node_is_null(args_node)) {
            uint32_t nc = ts_node_named_child_count(args_node);
            for (uint32_t i = 0; i < nc && targ_count < 15; i++) {
                TSNode arg = ts_node_named_child(args_node, i);
                if (ts_node_is_null(arg))
                    continue;
                const char *ak = ts_node_type(arg);
                if (strcmp(ak, "type_descriptor") == 0) {
                    // type_descriptor wraps a type node
                    if (ts_node_named_child_count(arg) > 0) {
                        targs[targ_count++] = c_parse_type_node(ctx, ts_node_named_child(arg, 0));
                    }
                } else {
                    targs[targ_count++] = c_parse_type_node(ctx, arg);
                }
            }
        }

        if (targ_count > 0) {
            return cbm_type_template(ctx->arena, template_qn, targs, targ_count);
        }
        return cbm_type_named(ctx->arena, template_qn);
    }

    // pointer_declarator / pointer (*) in type context
    if (strcmp(kind, "pointer_declarator") == 0 || strcmp(kind, "pointer_type") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            return cbm_type_pointer(ctx->arena,
                                    c_parse_type_node(ctx, ts_node_named_child(node, 0)));
        }
        return cbm_type_pointer(ctx->arena, cbm_type_unknown());
    }

    // reference_declarator / reference_type (&)
    if (strcmp(kind, "reference_declarator") == 0 ||
        strcmp(kind, "abstract_reference_declarator") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            return cbm_type_reference(ctx->arena,
                                      c_parse_type_node(ctx, ts_node_named_child(node, 0)));
        }
        return cbm_type_reference(ctx->arena, cbm_type_unknown());
    }

    // auto keyword
    if (strcmp(kind, "auto") == 0 || strcmp(kind, "placeholder_type_specifier") == 0) {
        return cbm_type_unknown(); // will be resolved from initializer
    }

    // decltype
    if (strcmp(kind, "decltype") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            return c_eval_expr_type(ctx, ts_node_named_child(node, 0));
        }
        return cbm_type_unknown();
    }

    // const/volatile qualifier — strip and recurse
    if (strcmp(kind, "type_qualifier") == 0) {
        return cbm_type_unknown(); // qualifier itself has no type
    }

    // array_declarator: int[10]
    if (strcmp(kind, "array_declarator") == 0 || strcmp(kind, "array_type") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            return cbm_type_slice(ctx->arena, c_parse_type_node(ctx, ts_node_named_child(node, 0)));
        }
        return cbm_type_unknown();
    }

    // function_declarator: int (*)(int)
    if (strcmp(kind, "function_declarator") == 0 ||
        strcmp(kind, "abstract_function_declarator") == 0) {
        return cbm_type_func(ctx->arena, NULL, NULL, NULL);
    }

    // struct_specifier / class_specifier (inline struct/class type)
    if (strcmp(kind, "struct_specifier") == 0 || strcmp(kind, "class_specifier") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(name_node)) {
            char *name = c_node_text(ctx, name_node);
            if (name)
                return c_resolve_name_to_type(ctx, name);
        }
        // Anonymous struct
        CBMType *t = (CBMType *)cbm_arena_alloc(ctx->arena, sizeof(CBMType));
        memset(t, 0, sizeof(CBMType));
        t->kind = CBM_TYPE_STRUCT;
        return t;
    }

    // enum_specifier
    if (strcmp(kind, "enum_specifier") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(name_node)) {
            char *name = c_node_text(ctx, name_node);
            if (name)
                return c_resolve_name_to_type(ctx, name);
        }
        return cbm_type_builtin(ctx->arena, "int");
    }

    // type_descriptor: wraps a type with optional qualifiers
    if (strcmp(kind, "type_descriptor") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        // Find the actual type node (skip qualifiers)
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "type_qualifier") != 0) {
                return c_parse_type_node(ctx, child);
            }
        }
        return cbm_type_unknown();
    }

    // parenthesized_type: (T)
    if (strcmp(kind, "parenthesized_type") == 0 && ts_node_named_child_count(node) > 0) {
        return c_parse_type_node(ctx, ts_node_named_child(node, 0));
    }

    // dependent_type: in template scope
    if (strcmp(kind, "dependent_type") == 0 || strcmp(kind, "dependent_name") == 0) {
        char *text = c_node_text(ctx, node);
        if (text)
            return cbm_type_named(ctx->arena, c_build_qn(ctx, text));
        return cbm_type_unknown();
    }

    return cbm_type_unknown();
}

// ============================================================================
// c_simplify_type: multi-step type unwrapping (clangd simplifyType)
// ============================================================================

const CBMType *c_simplify_type(CLSPContext *ctx, const CBMType *t, bool unwrap_pointer) {
    for (int i = 0; i < 64 && t; i++) {
        // Resolve aliases
        if (t->kind == CBM_TYPE_ALIAS) {
            if (t->data.alias.underlying) {
                t = t->data.alias.underlying;
                continue;
            }
            break;
        }

        // Resolve template type parameters using defaults
        if (t->kind == CBM_TYPE_TYPE_PARAM && ctx->in_template) {
            const CBMType *resolved = c_resolve_template_param(ctx, t->data.type_param.name);
            if (resolved) {
                t = resolved;
                continue;
            }
            break; // no default available
        }

        // Unwrap references
        if (t->kind == CBM_TYPE_REFERENCE || t->kind == CBM_TYPE_RVALUE_REF) {
            t = t->data.reference.elem;
            continue;
        }

        if (!unwrap_pointer)
            break;

        // Unwrap pointer
        if (t->kind == CBM_TYPE_POINTER) {
            t = t->data.pointer.elem;
            break;
        }

        // Smart pointer: extract first template arg
        if (t->kind == CBM_TYPE_TEMPLATE && is_smart_ptr(t->data.template_type.template_name)) {
            if (t->data.template_type.arg_count > 0 && t->data.template_type.template_args) {
                t = t->data.template_type.template_args[0];
                break;
            }
        }

        // Named type with operator->
        if (t->kind == CBM_TYPE_NAMED) {
            const CBMRegisteredFunc *op = cbm_registry_lookup_method(
                ctx->registry, t->data.named.qualified_name, "operator->");
            if (op && op->signature && op->signature->kind == CBM_TYPE_FUNC &&
                op->signature->data.func.return_types && op->signature->data.func.return_types[0]) {
                t = op->signature->data.func.return_types[0];
                // deref the pointer return
                if (t->kind == CBM_TYPE_POINTER)
                    t = t->data.pointer.elem;
                break;
            }
        }

        // Template type with operator-> (e.g., optional<Widget>::operator->)
        if (t->kind == CBM_TYPE_TEMPLATE) {
            const char *tqn = t->data.template_type.template_name;
            const CBMRegisteredFunc *op = c_lookup_member(ctx, tqn, "operator->");
            if (op && op->signature && op->signature->kind == CBM_TYPE_FUNC &&
                op->signature->data.func.return_types && op->signature->data.func.return_types[0]) {
                const CBMType *ret = op->signature->data.func.return_types[0];
                // Substitute template params using receiver's template args
                if (ret->kind == CBM_TYPE_TYPE_PARAM || ret->kind == CBM_TYPE_POINTER ||
                    ret->kind == CBM_TYPE_REFERENCE) {
                    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, tqn);
                    if (!rt && ctx->module_qn) {
                        rt = cbm_registry_lookup_type(
                            ctx->registry,
                            cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, tqn));
                    }
                    if (rt && rt->type_param_names) {
                        ret = cbm_type_substitute(ctx->arena, ret, rt->type_param_names,
                                                  t->data.template_type.template_args);
                    } else {
                        const char *fb[] = {"T", "K", "V", NULL};
                        const CBMType *fa[3] = {NULL};
                        int na = t->data.template_type.arg_count;
                        if (na > 0)
                            fa[0] = t->data.template_type.template_args[0];
                        if (na > 1)
                            fa[1] = t->data.template_type.template_args[1];
                        if (na > 2)
                            fa[2] = t->data.template_type.template_args[2];
                        ret = cbm_type_substitute(ctx->arena, ret, fb, fa);
                    }
                }
                t = ret;
                if (t->kind == CBM_TYPE_POINTER)
                    t = t->data.pointer.elem;
                break;
            }
            // Fallback: known smart pointer types — use first template arg
            if (is_smart_ptr(tqn) && t->data.template_type.arg_count > 0 &&
                t->data.template_type.template_args[0]) {
                t = t->data.template_type.template_args[0];
                break;
            }
        }

        break;
    }
    return t ? t : cbm_type_unknown();
}

// Get the QN string from a type (for member lookup)
static const char *type_to_qn(const CBMType *t) {
    if (!t)
        return NULL;
    switch (t->kind) {
    case CBM_TYPE_NAMED:
        return t->data.named.qualified_name;
    case CBM_TYPE_TEMPLATE:
        return t->data.template_type.template_name;
    default:
        return NULL;
    }
}

// ============================================================================
// c_eval_expr_type: recursive expression type evaluator
// ============================================================================

static const CBMType *c_eval_expr_type_inner(CLSPContext *ctx, TSNode node);

#define C_EVAL_DEPTH_LIMIT 256
#define C_EVAL_MAX_STEPS_PER_FILE 10000

const CBMType *c_eval_expr_type(CLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();
    /* Expression type evaluation is best-effort. Some recovery-mode C++ ASTs
     * can repeatedly drive member/type lookup without increasing recursion
     * depth. Keep a generous per-file work budget so pathological expressions
     * degrade to unknown instead of hanging repository indexing. */
    if (ctx->eval_depth > C_EVAL_DEPTH_LIMIT || ctx->eval_steps++ > C_EVAL_MAX_STEPS_PER_FILE) {
        if (ctx->debug && ctx->eval_steps == C_EVAL_MAX_STEPS_PER_FILE + 2) {
            fprintf(stderr, "  [clsp] expression eval step budget exhausted; returning unknown\n");
        }
        return cbm_type_unknown();
    }
    ctx->eval_depth++;
    const CBMType *result = c_eval_expr_type_inner(ctx, node);
    ctx->eval_depth--;
    return result ? result : cbm_type_unknown();
}

static const CBMType *c_eval_expr_type_inner(CLSPContext *ctx, TSNode node) {
    const char *kind = ts_node_type(node);

    // --- identifier: scope lookup ---
    if (strcmp(kind, "identifier") == 0) {
        char *name = c_node_text(ctx, node);
        if (!name)
            return cbm_type_unknown();

        // Scope lookup
        const CBMType *t = cbm_scope_lookup(ctx->current_scope, name);
        if (!cbm_type_is_unknown(t))
            return t;

        // Check if it's a registered function (before type check — functions
        // return FUNC type which lets call_expression extract return types)
        const char *fqn = c_resolve_name(ctx, name);
        if (fqn) {
            const CBMRegisteredFunc *f = cbm_registry_lookup_func(ctx->registry, fqn);
            if (f && f->signature)
                return f->signature;
        }

        // Check if it's a type name (for constructor calls / casts)
        const CBMType *type = c_resolve_name_to_type(ctx, name);
        if (type && type->kind == CBM_TYPE_NAMED) {
            // Could be a constructor call — return as type
            return type;
        }

        return cbm_type_unknown();
    }

    // --- this ---
    if (strcmp(kind, "this") == 0) {
        if (ctx->enclosing_class_qn) {
            return cbm_type_pointer(ctx->arena,
                                    cbm_type_named(ctx->arena, ctx->enclosing_class_qn));
        }
        return cbm_type_unknown();
    }

    // --- field_expression: a.b or a->b ---
    if (strcmp(kind, "field_expression") == 0) {
        TSNode arg_node = ts_node_child_by_field_name(node, "argument", 8);
        TSNode field_node = ts_node_child_by_field_name(node, "field", 5);
        if (ts_node_is_null(arg_node) || ts_node_is_null(field_node))
            return cbm_type_unknown();

        char *field_name = c_node_text(ctx, field_node);
        if (!field_name)
            return cbm_type_unknown();

        const CBMType *obj_type = c_eval_expr_type(ctx, arg_node);
        if (cbm_type_is_unknown(obj_type))
            return cbm_type_unknown();

        // Determine if this is . or -> access
        // Check for "->" operator in node text
        bool is_arrow = false;
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char *op = c_node_text(ctx, child);
                if (op && strcmp(op, "->") == 0) {
                    is_arrow = true;
                    break;
                }
            }
        }

        // Simplify type for member lookup
        const CBMType *base;
        if (is_arrow) {
            base = c_simplify_type(ctx, obj_type, true); // unwrap pointer/smart ptr
        } else {
            base = c_simplify_type(ctx, obj_type, false); // just unwrap refs/aliases
        }

        const char *type_qn = type_to_qn(base);
        if (!type_qn)
            return cbm_type_unknown();

        // Look up method
        const CBMRegisteredFunc *method = c_lookup_member(ctx, type_qn, field_name);
        if (method && method->signature)
            return method->signature;

        // Look up field
        const CBMType *field_type = c_lookup_field_type(ctx, type_qn, field_name, 0);
        if (field_type && !cbm_type_is_unknown(field_type)) {
            // Template field substitution: if field type is TYPE_PARAM (or NAMED matching
            // a template param name) and receiver is a TEMPLATE type, substitute using
            // receiver's template args.
            // e.g., pair<int, Foo>.second where second has type V → substitute V=Foo
            bool is_tparam = (field_type->kind == CBM_TYPE_TYPE_PARAM);
            // Also check NAMED types that match template param names (extract_defs
            // may register template fields as NAMED("V") rather than TYPE_PARAM("V"))
            if (!is_tparam && field_type->kind == CBM_TYPE_NAMED &&
                base->kind == CBM_TYPE_TEMPLATE) {
                const CBMRegisteredType *trt = cbm_registry_lookup_type(ctx->registry, type_qn);
                if (!trt && ctx->module_qn) {
                    trt = cbm_registry_lookup_type(
                        ctx->registry,
                        cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn));
                }
                if (trt && trt->type_param_names) {
                    const char *fname_qn = field_type->data.named.qualified_name;
                    // Extract short name from QN (after last dot)
                    const char *short_fn = strrchr(fname_qn, '.');
                    short_fn = short_fn ? short_fn + 1 : fname_qn;
                    for (int tpi = 0; trt->type_param_names[tpi]; tpi++) {
                        if (strcmp(trt->type_param_names[tpi], short_fn) == 0) {
                            is_tparam = true;
                            // Convert to TYPE_PARAM for substitution
                            field_type = cbm_type_type_param(ctx->arena, short_fn);
                            break;
                        }
                    }
                }
            }
            if (is_tparam && base->kind == CBM_TYPE_TEMPLATE &&
                base->data.template_type.template_args) {
                const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
                if (!rt && ctx->module_qn) {
                    rt = cbm_registry_lookup_type(
                        ctx->registry,
                        cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn));
                }
                if (rt && rt->type_param_names) {
                    field_type = cbm_type_substitute(ctx->arena, field_type, rt->type_param_names,
                                                     base->data.template_type.template_args);
                } else {
                    // Fallback: positional T, K, V
                    const char *params[] = {"T", "K", "V", "T1", "T2", NULL};
                    const CBMType *args[5] = {NULL};
                    int nargs = base->data.template_type.arg_count;
                    if (nargs > 0)
                        args[0] = base->data.template_type.template_args[0];
                    if (nargs > 0)
                        args[1] = base->data.template_type.template_args[0];
                    if (nargs > 1)
                        args[2] = base->data.template_type.template_args[1];
                    if (nargs > 0)
                        args[3] = base->data.template_type.template_args[0];
                    if (nargs > 1)
                        args[4] = base->data.template_type.template_args[1];
                    field_type = cbm_type_substitute(ctx->arena, field_type, params, args);
                }
            }
            return field_type;
        }

        return cbm_type_unknown();
    }

    // --- qualified_identifier / scoped_identifier: ns::Class::method or Enum::Value ---
    if (strcmp(kind, "qualified_identifier") == 0 || strcmp(kind, "scoped_identifier") == 0) {
        char *text = c_node_text(ctx, node);
        if (!text)
            return cbm_type_unknown();

        // Check if name child is template_function: ns::func<T> → strip template args
        TSNode qi_name_child = ts_node_child_by_field_name(node, "name", 4);
        bool qi_has_tmpl_args = false;
        TSNode qi_tmpl_args = (TSNode){0};
        if (!ts_node_is_null(qi_name_child) &&
            strcmp(ts_node_type(qi_name_child), "template_function") == 0) {
            qi_has_tmpl_args = true;
            qi_tmpl_args = ts_node_child_by_field_name(qi_name_child, "arguments", 9);
            // Build scope::bare_name without template args
            TSNode scope_node = ts_node_child_by_field_name(node, "scope", 5);
            TSNode tmpl_bare = ts_node_child_by_field_name(qi_name_child, "name", 4);
            char *scope_text = !ts_node_is_null(scope_node) ? c_node_text(ctx, scope_node) : NULL;
            char *bare_name = !ts_node_is_null(tmpl_bare) ? c_node_text(ctx, tmpl_bare) : NULL;
            if (bare_name) {
                text = scope_text
                           ? (char *)cbm_arena_sprintf(ctx->arena, "%s::%s", scope_text, bare_name)
                           : bare_name;
            }
        }

        const char *qn = c_build_qn(ctx, text);

        // Try as function: module-prefixed first (shadows stdlib stubs), then bare QN
        const CBMRegisteredFunc *f = NULL;
        if (ctx->module_qn) {
            f = cbm_registry_lookup_func(
                ctx->registry, cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, qn));
        }
        if (!f)
            f = cbm_registry_lookup_func(ctx->registry, qn);
        // Namespace fallback: extract_defs may omit namespace from QN.
        // "utils.create_logger" → try just "create_logger" with module prefix
        if (!f) {
            const char *ns_dot = strrchr(qn, '.');
            if (ns_dot && ctx->module_qn) {
                const char *bare = ns_dot + 1;
                f = cbm_registry_lookup_func(
                    ctx->registry, cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, bare));
                if (!f)
                    f = cbm_registry_lookup_func(ctx->registry, bare);
            }
        }
        if (f && f->signature) {
            // If template args present, substitute into return type
            if (qi_has_tmpl_args && !ts_node_is_null(qi_tmpl_args) &&
                f->signature->kind == CBM_TYPE_FUNC && f->signature->data.func.return_types &&
                f->signature->data.func.return_types[0]) {
                const CBMType *base_ret = f->signature->data.func.return_types[0];
                const CBMType *targs[16] = {
                    NULL}; /* zero-fill: cbm_type_substitute requires NULL-terminated args
                              (uninitialized tail bound T to stack garbage -> corrupt type graph,
                              bitcoin serialize.h) */
                int targ_count = 0;
                uint32_t tnc = ts_node_named_child_count(qi_tmpl_args);
                for (uint32_t ti = 0; ti < tnc && targ_count < 15; ti++) {
                    TSNode targ = ts_node_named_child(qi_tmpl_args, ti);
                    if (ts_node_is_null(targ))
                        continue;
                    const char *tak = ts_node_type(targ);
                    if (strcmp(tak, "type_descriptor") == 0) {
                        if (ts_node_named_child_count(targ) > 0)
                            targs[targ_count++] =
                                c_parse_type_node(ctx, ts_node_named_child(targ, 0));
                    } else {
                        targs[targ_count++] = c_parse_type_node(ctx, targ);
                    }
                }
                if (targ_count > 0) {
                    const char **tpn = f->type_param_names;
                    if (tpn) {
                        base_ret = cbm_type_substitute(ctx->arena, base_ret, tpn, targs);
                    } else {
                        const char *fallback[] = {"T", "U", "V", "W", NULL};
                        base_ret = cbm_type_substitute(ctx->arena, base_ret, fallback, targs);
                    }
                    // If return type is still NAMED after substitution (no TYPE_PARAM was
                    // present), wrap as TEMPLATE with explicit args so downstream code
                    // (e.g., smart pointer dereference) can extract inner types.
                    // e.g., make_shared<Widget> returns NAMED("std.shared_ptr") →
                    //        TEMPLATE("std.shared_ptr", [Widget])
                    if (base_ret->kind == CBM_TYPE_NAMED) {
                        const CBMType **final_targs = (const CBMType **)cbm_arena_alloc(
                            ctx->arena, (targ_count + 1) * sizeof(const CBMType *));
                        for (int i = 0; i < targ_count; i++)
                            final_targs[i] = targs[i];
                        final_targs[targ_count] = NULL;
                        base_ret =
                            cbm_type_template(ctx->arena, base_ret->data.named.qualified_name,
                                              final_targs, targ_count);
                    }
                }
                const CBMType **rets =
                    (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(const CBMType *));
                rets[0] = base_ret;
                rets[1] = NULL;
                return cbm_type_func(ctx->arena, NULL, NULL, rets);
            }
            return f->signature;
        }

        // Try as type (bare QN then module-prefixed)
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, qn);
        if (rt)
            return cbm_type_named(ctx->arena, qn);
        if (!rt && ctx->module_qn) {
            const char *mod_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, qn);
            rt = cbm_registry_lookup_type(ctx->registry, mod_qn);
            if (rt)
                return cbm_type_named(ctx->arena, mod_qn);
        }

        // Try as enum member: Enum::Value → look up Enum type, return int/enum type
        const char *dot = strrchr(qn, '.');
        if (dot) {
            size_t prefix_len = (size_t)(dot - qn);
            char *enum_qn = (char *)cbm_arena_alloc(ctx->arena, prefix_len + 1);
            if (enum_qn) {
                memcpy(enum_qn, qn, prefix_len);
                enum_qn[prefix_len] = '\0';
                const CBMRegisteredType *enum_rt = cbm_registry_lookup_type(ctx->registry, enum_qn);
                if (enum_rt) {
                    return cbm_type_named(ctx->arena, enum_qn);
                }
                // Try with module prefix
                if (ctx->module_qn) {
                    const char *fqn_enum =
                        cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, enum_qn);
                    enum_rt = cbm_registry_lookup_type(ctx->registry, fqn_enum);
                    if (enum_rt)
                        return cbm_type_named(ctx->arena, fqn_enum);
                }
            }

            // Template heuristic: T::member where T is a template param with default
            if (ctx->in_template && dot) {
                size_t prefix_len2 = (size_t)(dot - qn);
                char *scope_name = (char *)cbm_arena_alloc(ctx->arena, prefix_len2 + 1);
                if (scope_name) {
                    memcpy(scope_name, qn, prefix_len2);
                    scope_name[prefix_len2] = '\0';
                    const CBMType *default_type = c_resolve_template_param(ctx, scope_name);
                    if (default_type) {
                        default_type = c_simplify_type(ctx, default_type, false);
                        const char *default_qn = type_to_qn(default_type);
                        if (default_qn) {
                            // Try as method: DefaultType::member
                            const char *member = dot + 1;
                            const CBMRegisteredFunc *mf =
                                cbm_registry_lookup_method(ctx->registry, default_qn, member);
                            if (mf && mf->signature)
                                return mf->signature;
                            // Try as static function
                            const char *dep_qn =
                                cbm_arena_sprintf(ctx->arena, "%s.%s", default_qn, member);
                            const CBMRegisteredFunc *sf =
                                cbm_registry_lookup_func(ctx->registry, dep_qn);
                            if (sf && sf->signature)
                                return sf->signature;
                        }
                    }
                }
            }
        }

        return cbm_type_unknown();
    }

    // --- template_function: make_shared<Widget>, make_unique<Foo> ---
    // Returns a FUNC type whose return type is TEMPLATE(base_return, [explicit_args]).
    if (strcmp(kind, "template_function") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode args_node = ts_node_child_by_field_name(node, "arguments", 9);
        if (ts_node_is_null(name_node))
            return cbm_type_unknown();

        char *name = c_node_text(ctx, name_node);
        if (!name)
            return cbm_type_unknown();

        // Look up the function — handle both simple and qualified names
        const CBMRegisteredFunc *f = NULL;
        const char *nk = ts_node_type(name_node);
        if (strcmp(nk, "qualified_identifier") == 0 || strcmp(nk, "scoped_identifier") == 0) {
            // Qualified: std::make_shared → std.make_shared
            const char *qn = c_build_qn(ctx, name);
            f = cbm_registry_lookup_func(ctx->registry, qn);
            if (!f && ctx->module_qn) {
                const char *mod_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, qn);
                f = cbm_registry_lookup_func(ctx->registry, mod_qn);
            }
        } else {
            const char *fqn = c_resolve_name(ctx, name);
            if (fqn)
                f = cbm_registry_lookup_func(ctx->registry, fqn);
        }
        if (!f || !f->signature)
            return cbm_type_unknown();

        // Get the base return type from the function signature
        const CBMType *base_ret = NULL;
        if (f->signature->kind == CBM_TYPE_FUNC && f->signature->data.func.return_types &&
            f->signature->data.func.return_types[0]) {
            base_ret = f->signature->data.func.return_types[0];
        }
        if (!base_ret)
            return f->signature;

        // Parse explicit template arguments from <...>
        const CBMType *targs[16] = {
            NULL}; /* zero-fill: cbm_type_substitute requires NULL-terminated args (uninitialized
                      tail bound T to stack garbage -> corrupt type graph, bitcoin serialize.h) */
        int targ_count = 0;
        if (!ts_node_is_null(args_node)) {
            uint32_t nc = ts_node_named_child_count(args_node);
            for (uint32_t i = 0; i < nc && targ_count < 15; i++) {
                TSNode arg = ts_node_named_child(args_node, i);
                if (ts_node_is_null(arg))
                    continue;
                const char *ak = ts_node_type(arg);
                if (strcmp(ak, "type_descriptor") == 0) {
                    if (ts_node_named_child_count(arg) > 0)
                        targs[targ_count++] = c_parse_type_node(ctx, ts_node_named_child(arg, 0));
                } else {
                    targs[targ_count++] = c_parse_type_node(ctx, arg);
                }
            }
        }

        // Substitute explicit template args into return type
        if (targ_count > 0) {
            const char **tpn = f->type_param_names;
            if (tpn) {
                // Use registered type param names (e.g., ["T", NULL])
                base_ret = cbm_type_substitute(ctx->arena, base_ret, tpn, targs);
            } else {
                // Fallback: positional T, U, V
                const char *fallback_names[] = {"T", "U", "V", "W", NULL};
                base_ret = cbm_type_substitute(ctx->arena, base_ret, fallback_names, targs);
            }
        }

        // Wrap return type in FUNC signature
        const CBMType **rets = cbm_arena_alloc(ctx->arena, 2 * sizeof(CBMType *));
        rets[0] = base_ret;
        rets[1] = NULL;
        return cbm_type_func(ctx->arena, NULL, NULL, rets);
    }

    // --- call_expression: f(args) ---
    if (strcmp(kind, "call_expression") == 0) {
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        if (ts_node_is_null(func_node))
            return cbm_type_unknown();

        // Special-case std::move/std::forward: return operand's type (rvalue ref)
        {
            const char *fk = ts_node_type(func_node);
            char *fname = NULL;
            if (strcmp(fk, "qualified_identifier") == 0 || strcmp(fk, "scoped_identifier") == 0 ||
                strcmp(fk, "identifier") == 0) {
                fname = c_node_text(ctx, func_node);
            } else if (strcmp(fk, "template_function") == 0) {
                TSNode name_node = ts_node_child_by_field_name(func_node, "name", 4);
                if (!ts_node_is_null(name_node))
                    fname = c_node_text(ctx, name_node);
            }
            if (fname && (strcmp(fname, "std::move") == 0 || strcmp(fname, "move") == 0 ||
                          strcmp(fname, "std::forward") == 0 || strcmp(fname, "forward") == 0)) {
                TSNode call_args = ts_node_child_by_field_name(node, "arguments", 9);
                if (!ts_node_is_null(call_args) && ts_node_named_child_count(call_args) > 0) {
                    TSNode first_arg = ts_node_named_child(call_args, 0);
                    if (!ts_node_is_null(first_arg)) {
                        const CBMType *arg_type = c_eval_expr_type(ctx, first_arg);
                        // Unwrap to base type — move/forward preserves the type
                        arg_type = c_simplify_type(ctx, arg_type, false);
                        if (!cbm_type_is_unknown(arg_type))
                            return arg_type;
                    }
                }
            }
        }

        const CBMType *func_type = c_eval_expr_type(ctx, func_node);
        if (!func_type)
            return cbm_type_unknown();
        // FUNC type -> return its return type
        if (func_type->kind == CBM_TYPE_FUNC && func_type->data.func.return_types &&
            func_type->data.func.return_types[0]) {
            const CBMType *ret = func_type->data.func.return_types[0];

            // Template param substitution: try receiver's template args (method calls)
            // then try TAD from call-site argument types (free function calls)
            bool needs_subst = (ret->kind == CBM_TYPE_TYPE_PARAM || ret->kind == CBM_TYPE_NAMED ||
                                ret->kind == CBM_TYPE_POINTER || ret->kind == CBM_TYPE_REFERENCE ||
                                ret->kind == CBM_TYPE_RVALUE_REF ||
                                ret->kind == CBM_TYPE_TEMPLATE || ret->kind == CBM_TYPE_SLICE);

            if (needs_subst) {
                bool substituted = false;

                // Strategy 1: method call on templated receiver — use receiver's template args
                if (strcmp(ts_node_type(func_node), "field_expression") == 0) {
                    TSNode arg_node = ts_node_child_by_field_name(func_node, "argument", 8);
                    if (!ts_node_is_null(arg_node)) {
                        const CBMType *obj_type = c_eval_expr_type(ctx, arg_node);
                        // Use false to preserve TEMPLATE type — we need template args
                        // for substitution, not the pointed-to type from operator->
                        obj_type = c_simplify_type(ctx, obj_type, false);
                        if (obj_type && obj_type->kind == CBM_TYPE_TEMPLATE &&
                            obj_type->data.template_type.template_args) {
                            // Use registered type_param_names if available
                            TSNode field_node = ts_node_child_by_field_name(func_node, "field", 5);
                            char *method_name =
                                !ts_node_is_null(field_node) ? c_node_text(ctx, field_node) : NULL;
                            const char *recv_qn = type_to_qn(obj_type);
                            const CBMRegisteredFunc *mf =
                                (recv_qn && method_name)
                                    ? c_lookup_member(ctx, recv_qn, method_name)
                                    : NULL;
                            const char **tpn = mf ? mf->type_param_names : NULL;

                            // Try class template param names from registered type
                            if (!tpn && recv_qn) {
                                const CBMRegisteredType *rrt =
                                    cbm_registry_lookup_type(ctx->registry, recv_qn);
                                if (!rrt && ctx->module_qn) {
                                    rrt = cbm_registry_lookup_type(
                                        ctx->registry, cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                                         ctx->module_qn, recv_qn));
                                }
                                if (rrt && rrt->type_param_names)
                                    tpn = rrt->type_param_names;
                            }

                            // Build substitution from receiver template args
                            int nargs = obj_type->data.template_type.arg_count;
                            if (tpn) {
                                // Use actual type param names from class/function registration
                                ret =
                                    cbm_type_substitute(ctx->arena, ret, tpn,
                                                        obj_type->data.template_type.template_args);
                                substituted = true;
                            } else {
                                // Fallback: positional params T, K, V, T1, T2
                                const char *params[] = {"T", "K", "V", "T1", "T2", NULL};
                                const CBMType *pargs[5] = {NULL};
                                if (nargs > 0)
                                    pargs[0] = obj_type->data.template_type.template_args[0];
                                if (nargs > 0)
                                    pargs[1] = obj_type->data.template_type.template_args[0];
                                if (nargs > 1)
                                    pargs[2] = obj_type->data.template_type.template_args[1];
                                if (nargs > 0)
                                    pargs[3] = obj_type->data.template_type.template_args[0];
                                if (nargs > 1)
                                    pargs[4] = obj_type->data.template_type.template_args[1];
                                ret = cbm_type_substitute(ctx->arena, ret, params, pargs);
                                substituted = true;
                            }
                        }
                    }
                }

                // Strategy 2: TAD — deduce template params from call-site argument types
                if (!substituted) {
                    // Find the registered function to get its type_param_names and param_types
                    const CBMRegisteredFunc *rf = NULL;
                    const char *fn_type = ts_node_type(func_node);
                    if (strcmp(fn_type, "identifier") == 0) {
                        char *fname = c_node_text(ctx, func_node);
                        if (fname) {
                            const char *fqn = c_resolve_name(ctx, fname);
                            if (fqn)
                                rf = cbm_registry_lookup_func(ctx->registry, fqn);
                        }
                    } else if (strcmp(fn_type, "qualified_identifier") == 0 ||
                               strcmp(fn_type, "scoped_identifier") == 0) {
                        char *fname = c_node_text(ctx, func_node);
                        if (fname) {
                            const char *qn = c_build_qn(ctx, fname);
                            rf = cbm_registry_lookup_func(ctx->registry, qn);
                        }
                    } else if (strcmp(fn_type, "template_function") == 0) {
                        TSNode name_node = ts_node_child_by_field_name(func_node, "name", 4);
                        if (!ts_node_is_null(name_node)) {
                            char *fname = c_node_text(ctx, name_node);
                            if (fname) {
                                const char *nk = ts_node_type(name_node);
                                if (strcmp(nk, "qualified_identifier") == 0 ||
                                    strcmp(nk, "scoped_identifier") == 0) {
                                    const char *qn = c_build_qn(ctx, fname);
                                    rf = cbm_registry_lookup_func(ctx->registry, qn);
                                    if (!rf && ctx->module_qn) {
                                        rf = cbm_registry_lookup_func(
                                            ctx->registry, cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                                             ctx->module_qn, qn));
                                    }
                                } else {
                                    const char *fqn = c_resolve_name(ctx, fname);
                                    if (fqn)
                                        rf = cbm_registry_lookup_func(ctx->registry, fqn);
                                }
                            }
                        }
                    }

                    if (rf && rf->type_param_names && rf->signature &&
                        rf->signature->kind == CBM_TYPE_FUNC &&
                        rf->signature->data.func.param_types) {
                        // Deduce: match call-site arg types against param types
                        // to bind type_param_names
                        int tp_count = 0;
                        while (rf->type_param_names[tp_count])
                            tp_count++;
                        if (tp_count > 0 && tp_count <= 8) {
                            const CBMType *deduced[8] = {NULL};
                            TSNode call_args = ts_node_child_by_field_name(node, "arguments", 9);
                            int pi = 0;
                            if (!ts_node_is_null(call_args)) {
                                uint32_t anc = ts_node_named_child_count(call_args);
                                for (uint32_t ai = 0;
                                     ai < anc && rf->signature->data.func.param_types[pi]; ai++) {
                                    TSNode carg = ts_node_named_child(call_args, ai);
                                    if (ts_node_is_null(carg))
                                        continue;
                                    const CBMType *arg_t = c_eval_expr_type(ctx, carg);
                                    if (cbm_type_is_unknown(arg_t)) {
                                        pi++;
                                        continue;
                                    }
                                    // Unwrap ref/ptr from param type
                                    const CBMType *param_t =
                                        rf->signature->data.func.param_types[pi];
                                    while (param_t && (param_t->kind == CBM_TYPE_REFERENCE ||
                                                       param_t->kind == CBM_TYPE_RVALUE_REF ||
                                                       param_t->kind == CBM_TYPE_POINTER)) {
                                        if (param_t->kind == CBM_TYPE_POINTER)
                                            param_t = param_t->data.pointer.elem;
                                        else
                                            param_t = param_t->data.reference.elem;
                                    }
                                    // If param type is TYPE_PARAM, bind it
                                    if (param_t && param_t->kind == CBM_TYPE_TYPE_PARAM) {
                                        for (int ti = 0; ti < tp_count; ti++) {
                                            if (strcmp(rf->type_param_names[ti],
                                                       param_t->data.type_param.name) == 0) {
                                                // Unwrap ref/ptr from actual arg too
                                                const CBMType *unwrapped = arg_t;
                                                while (unwrapped &&
                                                       (unwrapped->kind == CBM_TYPE_REFERENCE ||
                                                        unwrapped->kind == CBM_TYPE_RVALUE_REF ||
                                                        unwrapped->kind == CBM_TYPE_POINTER)) {
                                                    if (unwrapped->kind == CBM_TYPE_POINTER)
                                                        unwrapped = unwrapped->data.pointer.elem;
                                                    else
                                                        unwrapped = unwrapped->data.reference.elem;
                                                }
                                                if (!deduced[ti])
                                                    deduced[ti] = unwrapped;
                                                break;
                                            }
                                        }
                                    }
                                    pi++;
                                }
                            }
                            // Apply deduced substitutions
                            bool any_deduced = false;
                            for (int ti = 0; ti < tp_count; ti++) {
                                if (deduced[ti]) {
                                    any_deduced = true;
                                    break;
                                }
                            }
                            if (any_deduced) {
                                const CBMType *substituted_ret = cbm_type_substitute(
                                    ctx->arena, ret, rf->type_param_names, deduced);
                                if (substituted_ret)
                                    ret = substituted_ret;
                            }
                        }
                    }
                }
            }

            // Unwrap references in return type
            if (!ret)
                return cbm_type_unknown();
            if (ret->kind == CBM_TYPE_REFERENCE || ret->kind == CBM_TYPE_RVALUE_REF)
                ret = ret->data.reference.elem;
            return ret;
        }

        // Constructor call: Type(args) — if func_node resolves to a named type
        if (func_type->kind == CBM_TYPE_NAMED) {
            return func_type;
        }

        return cbm_type_unknown();
    }

    // --- new_expression: new Foo(args) ---
    if (strcmp(kind, "new_expression") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node)) {
            return cbm_type_pointer(ctx->arena, c_parse_type_node(ctx, type_node));
        }
        // Try first named child
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "type_identifier") == 0 || strcmp(ck, "template_type") == 0 ||
                strcmp(ck, "scoped_type_identifier") == 0 || strcmp(ck, "primitive_type") == 0) {
                return cbm_type_pointer(ctx->arena, c_parse_type_node(ctx, child));
            }
        }
        return cbm_type_pointer(ctx->arena, cbm_type_unknown());
    }

    // --- delete_expression ---
    if (strcmp(kind, "delete_expression") == 0) {
        return cbm_type_unknown(); // void
    }

    // --- subscript_expression: a[i] ---
    if (strcmp(kind, "subscript_expression") == 0) {
        TSNode arg_node = ts_node_child_by_field_name(node, "argument", 8);
        if (ts_node_is_null(arg_node))
            return cbm_type_unknown();
        const CBMType *arr_type = c_eval_expr_type(ctx, arg_node);
        if (!arr_type)
            return cbm_type_unknown();

        // Array/slice: return element
        if (arr_type->kind == CBM_TYPE_SLICE)
            return arr_type->data.slice.elem;
        if (arr_type->kind == CBM_TYPE_POINTER)
            return arr_type->data.pointer.elem;

        // Template with operator[]: return via method lookup
        const char *qn = type_to_qn(c_simplify_type(ctx, arr_type, false));
        if (qn) {
            const CBMRegisteredFunc *op =
                cbm_registry_lookup_method(ctx->registry, qn, "operator[]");
            if (op && op->signature && op->signature->kind == CBM_TYPE_FUNC &&
                op->signature->data.func.return_types && op->signature->data.func.return_types[0]) {
                const CBMType *ret = op->signature->data.func.return_types[0];
                if (ret->kind == CBM_TYPE_REFERENCE)
                    ret = ret->data.reference.elem;

                // Substitute template params
                if (ret->kind == CBM_TYPE_TYPE_PARAM && arr_type->kind == CBM_TYPE_TEMPLATE) {
                    const char *params[] = {"T", "K", "V", NULL};
                    const CBMType *args[3] = {NULL};
                    int nargs = arr_type->data.template_type.arg_count;
                    if (nargs > 0)
                        args[0] = arr_type->data.template_type.template_args[0];
                    if (nargs > 0)
                        args[1] = arr_type->data.template_type.template_args[0];
                    if (nargs > 1)
                        args[2] = arr_type->data.template_type.template_args[1];
                    ret = cbm_type_substitute(ctx->arena, ret, params, args);
                }
                return ret;
            }
        }
        return cbm_type_unknown();
    }

    // --- cast expressions ---
    if (strcmp(kind, "cast_expression") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node))
            return c_parse_type_node(ctx, type_node);
        // Fallback: first named child might be type_descriptor
        if (ts_node_named_child_count(node) > 0) {
            TSNode first = ts_node_named_child(node, 0);
            if (strcmp(ts_node_type(first), "type_descriptor") == 0)
                return c_parse_type_node(ctx, first);
        }
        return cbm_type_unknown();
    }
    if (strcmp(kind, "static_cast_expression") == 0 ||
        strcmp(kind, "dynamic_cast_expression") == 0 ||
        strcmp(kind, "reinterpret_cast_expression") == 0 ||
        strcmp(kind, "const_cast_expression") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node))
            return c_parse_type_node(ctx, type_node);
        return cbm_type_unknown();
    }

    // --- unary_expression: *p, &x, !x, -x ---
    if (strcmp(kind, "unary_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "argument", 8);
        if (ts_node_is_null(operand)) {
            // Try "operand" field name
            operand = ts_node_child_by_field_name(node, "operand", 7);
        }
        if (ts_node_is_null(operand))
            return cbm_type_unknown();

        // Get operator from first non-named child
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char *op = c_node_text(ctx, child);
                if (!op)
                    continue;
                if (strcmp(op, "*") == 0) {
                    const CBMType *inner = c_eval_expr_type(ctx, operand);
                    inner = c_simplify_type(ctx, inner, false);
                    // TEMPLATE with operator*(): look up and substitute return type
                    if (inner && inner->kind == CBM_TYPE_TEMPLATE) {
                        const char *tqn = inner->data.template_type.template_name;
                        const CBMRegisteredFunc *opf = c_lookup_member(ctx, tqn, "operator*");
                        if (opf && opf->signature && opf->signature->kind == CBM_TYPE_FUNC &&
                            opf->signature->data.func.return_types &&
                            opf->signature->data.func.return_types[0]) {
                            const CBMType *ret = opf->signature->data.func.return_types[0];
                            const CBMRegisteredType *rt =
                                cbm_registry_lookup_type(ctx->registry, tqn);
                            if (!rt && ctx->module_qn) {
                                rt = cbm_registry_lookup_type(
                                    ctx->registry,
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, tqn));
                            }
                            if (rt && rt->type_param_names) {
                                ret = cbm_type_substitute(ctx->arena, ret, rt->type_param_names,
                                                          inner->data.template_type.template_args);
                            } else {
                                const char *fb[] = {"T", "K", "V", NULL};
                                const CBMType *fa[3] = {NULL};
                                int na = inner->data.template_type.arg_count;
                                if (na > 0)
                                    fa[0] = inner->data.template_type.template_args[0];
                                if (na > 1)
                                    fa[1] = inner->data.template_type.template_args[1];
                                if (na > 2)
                                    fa[2] = inner->data.template_type.template_args[2];
                                ret = cbm_type_substitute(ctx->arena, ret, fb, fa);
                            }
                            if (ret->kind == CBM_TYPE_REFERENCE)
                                ret = ret->data.reference.elem;
                            return ret;
                        }
                    }
                    return cbm_type_deref(inner);
                }
                if (strcmp(op, "&") == 0) {
                    return cbm_type_pointer(ctx->arena, c_eval_expr_type(ctx, operand));
                }
                if (strcmp(op, "!") == 0 || strcmp(op, "~") == 0) {
                    return c_eval_expr_type(ctx, operand);
                }
                if (strcmp(op, "-") == 0 || strcmp(op, "+") == 0) {
                    return c_eval_expr_type(ctx, operand);
                }
                break;
            }
        }
        return cbm_type_unknown();
    }

    // --- pointer_expression: *p or &x (C tree-sitter uses this instead of unary_expression) ---
    if (strcmp(kind, "pointer_expression") == 0) {
        TSNode arg = ts_node_child_by_field_name(node, "argument", 8);
        if (ts_node_is_null(arg))
            return cbm_type_unknown();
        // Check operator
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char *op = c_node_text(ctx, child);
                if (op && strcmp(op, "*") == 0) {
                    const CBMType *inner = c_eval_expr_type(ctx, arg);
                    inner = c_simplify_type(ctx, inner, false);
                    if (inner && inner->kind == CBM_TYPE_TEMPLATE) {
                        const char *tqn = inner->data.template_type.template_name;
                        const CBMRegisteredFunc *opf = c_lookup_member(ctx, tqn, "operator*");
                        if (opf && opf->signature && opf->signature->kind == CBM_TYPE_FUNC &&
                            opf->signature->data.func.return_types &&
                            opf->signature->data.func.return_types[0]) {
                            const CBMType *ret = opf->signature->data.func.return_types[0];
                            const CBMRegisteredType *rt =
                                cbm_registry_lookup_type(ctx->registry, tqn);
                            if (!rt && ctx->module_qn) {
                                rt = cbm_registry_lookup_type(
                                    ctx->registry,
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, tqn));
                            }
                            if (rt && rt->type_param_names) {
                                ret = cbm_type_substitute(ctx->arena, ret, rt->type_param_names,
                                                          inner->data.template_type.template_args);
                            } else {
                                const char *fb[] = {"T", "K", "V", NULL};
                                const CBMType *fa[3] = {NULL};
                                int na = inner->data.template_type.arg_count;
                                if (na > 0)
                                    fa[0] = inner->data.template_type.template_args[0];
                                if (na > 1)
                                    fa[1] = inner->data.template_type.template_args[1];
                                if (na > 2)
                                    fa[2] = inner->data.template_type.template_args[2];
                                ret = cbm_type_substitute(ctx->arena, ret, fb, fa);
                            }
                            if (ret->kind == CBM_TYPE_REFERENCE)
                                ret = ret->data.reference.elem;
                            return ret;
                        }
                    }
                    return cbm_type_deref(inner);
                }
                if (op && strcmp(op, "&") == 0)
                    return cbm_type_pointer(ctx->arena, c_eval_expr_type(ctx, arg));
                break;
            }
        }
        return cbm_type_unknown();
    }

    // --- update_expression: ++i, i++ ---
    if (strcmp(kind, "update_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "argument", 8);
        if (!ts_node_is_null(operand))
            return c_eval_expr_type(ctx, operand);
        return cbm_type_unknown();
    }

    // --- binary_expression ---
    if (strcmp(kind, "binary_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        // Check operator for comparison (returns bool)
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char *op = c_node_text(ctx, child);
                if (op && (strcmp(op, "==") == 0 || strcmp(op, "!=") == 0 || strcmp(op, "<") == 0 ||
                           strcmp(op, ">") == 0 || strcmp(op, "<=") == 0 || strcmp(op, ">=") == 0 ||
                           strcmp(op, "&&") == 0 || strcmp(op, "||") == 0)) {
                    return cbm_type_builtin(ctx->arena, "bool");
                }
                break;
            }
        }
        if (!ts_node_is_null(left))
            return c_eval_expr_type(ctx, left);
        return cbm_type_unknown();
    }

    // --- conditional_expression: a ? b : c ---
    if (strcmp(kind, "conditional_expression") == 0) {
        TSNode consequence = ts_node_child_by_field_name(node, "consequence", 11);
        if (!ts_node_is_null(consequence))
            return c_eval_expr_type(ctx, consequence);
        return cbm_type_unknown();
    }

    // --- parenthesized_expression: (expr) ---
    if (strcmp(kind, "parenthesized_expression") == 0 && ts_node_named_child_count(node) > 0) {
        return c_eval_expr_type(ctx, ts_node_named_child(node, 0));
    }

    // --- comma_expression ---
    if (strcmp(kind, "comma_expression") == 0) {
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(right))
            return c_eval_expr_type(ctx, right);
        return cbm_type_unknown();
    }

    // --- sizeof / alignof ---
    if (strcmp(kind, "sizeof_expression") == 0 || strcmp(kind, "alignof_expression") == 0) {
        return cbm_type_builtin(ctx->arena, "size_t");
    }

    // --- lambda_expression ---
    if (strcmp(kind, "lambda_expression") == 0) {
        const CBMType *ret_type = NULL;

        // Phase A: trailing return type on declarator (e.g., [](int x) -> Widget { ... })
        TSNode declarator = ts_node_child_by_field_name(node, "declarator", 10);
        if (!ts_node_is_null(declarator)) {
            uint32_t dnc = ts_node_named_child_count(declarator);
            for (uint32_t di = 0; di < dnc; di++) {
                TSNode ch = ts_node_named_child(declarator, di);
                if (strcmp(ts_node_type(ch), "trailing_return_type") == 0) {
                    TSNode type_desc =
                        ts_node_named_child_count(ch) > 0 ? ts_node_named_child(ch, 0) : ch;
                    ret_type = c_parse_type_node(ctx, type_desc);
                    break;
                }
            }
        }

        // Phase B: infer from body's immediate return statements (no descent into nested lambdas)
        if (!ret_type || cbm_type_is_unknown(ret_type)) {
            TSNode body = ts_node_child_by_field_name(node, "body", 4);
            if (!ts_node_is_null(body)) {
                uint32_t bnc = ts_node_named_child_count(body);
                for (uint32_t bi = 0; bi < bnc; bi++) {
                    TSNode stmt = ts_node_named_child(body, bi);
                    if (ts_node_is_null(stmt))
                        continue;
                    const char *sk = ts_node_type(stmt);
                    // Skip nested lambdas, loops, etc. — only immediate returns
                    if (strcmp(sk, "return_statement") == 0) {
                        if (ts_node_named_child_count(stmt) > 0) {
                            TSNode ret_expr = ts_node_named_child(stmt, 0);
                            ret_type = c_eval_expr_type(ctx, ret_expr);
                        }
                        break; // first return only
                    }
                }
            }
        }

        if (ret_type && !cbm_type_is_unknown(ret_type)) {
            const CBMType **ret_types =
                (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(const CBMType *));
            if (ret_types) {
                ret_types[0] = ret_type;
                ret_types[1] = NULL;
                return cbm_type_func(ctx->arena, NULL, NULL, ret_types);
            }
        }
        return cbm_type_func(ctx->arena, NULL, NULL, NULL);
    }

    // --- co_await_expression: co_await expr → evaluate operand, return await_resume type ---
    if (strcmp(kind, "co_await_expression") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            TSNode operand = ts_node_named_child(node, 0);
            const CBMType *op_type = c_eval_expr_type(ctx, operand);
            // co_await returns the result of operator co_await or the awaitable's await_resume
            // Heuristic: if operand is a template type (e.g., Task<T>), return first template arg
            op_type = c_simplify_type(ctx, op_type, false);
            if (op_type && op_type->kind == CBM_TYPE_TEMPLATE &&
                op_type->data.template_type.arg_count > 0) {
                return op_type->data.template_type.template_args[0];
            }
            // Fallback: check for await_resume method
            const char *op_qn = type_to_qn(op_type);
            if (op_qn) {
                const CBMRegisteredFunc *ar = c_lookup_member(ctx, op_qn, "await_resume");
                if (ar && ar->signature && ar->signature->kind == CBM_TYPE_FUNC &&
                    ar->signature->data.func.return_types &&
                    ar->signature->data.func.return_types[0]) {
                    return ar->signature->data.func.return_types[0];
                }
            }
        }
        return cbm_type_unknown();
    }

    // --- fold_expression: (args op ...) → return type of operand ---
    if (strcmp(kind, "fold_expression") == 0) {
        // Fold expressions apply a binary operator across a parameter pack.
        // The result type is the type of the operator applied to the pack elements.
        // Heuristic: return the type of the first named child that isn't an operator.
        uint32_t fnc = ts_node_named_child_count(node);
        for (uint32_t fi = 0; fi < fnc; fi++) {
            TSNode child = ts_node_named_child(node, fi);
            if (!ts_node_is_null(child)) {
                const CBMType *ct = c_eval_expr_type(ctx, child);
                if (!cbm_type_is_unknown(ct))
                    return ct;
            }
        }
        return cbm_type_unknown();
    }

    // --- requires_expression: requires(T x) { x.method(); } → bool ---
    if (strcmp(kind, "requires_expression") == 0) {
        return cbm_type_builtin(ctx->arena, "bool");
    }

    // --- generic_expression: _Generic(expr, type1: val1, type2: val2, ...) ---
    if (strcmp(kind, "generic_expression") == 0) {
        // C11 _Generic: evaluate the controlling expression's type,
        // then match against the association list.
        // Heuristic: return the type of the first non-default association value.
        uint32_t gnc = ts_node_named_child_count(node);
        for (uint32_t gi = 1; gi < gnc; gi++) {
            TSNode assoc = ts_node_named_child(node, gi);
            if (!ts_node_is_null(assoc)) {
                // Each association may have type + value children
                uint32_t anc = ts_node_named_child_count(assoc);
                if (anc > 0) {
                    TSNode val = ts_node_named_child(assoc, anc - 1);
                    const CBMType *vt = c_eval_expr_type(ctx, val);
                    if (!cbm_type_is_unknown(vt))
                        return vt;
                }
            }
        }
        return cbm_type_unknown();
    }

    // --- Literals ---
    if (strcmp(kind, "number_literal") == 0) {
        char *text = c_node_text(ctx, node);
        if (text) {
            // Check for float suffix or decimal point
            for (const char *p = text; *p; p++) {
                if (*p == '.' || *p == 'f' || *p == 'F' || *p == 'e' || *p == 'E')
                    return cbm_type_builtin(ctx->arena, "double");
            }
        }
        return cbm_type_builtin(ctx->arena, "int");
    }
    if (strcmp(kind, "string_literal") == 0 || strcmp(kind, "concatenated_string") == 0 ||
        strcmp(kind, "raw_string_literal") == 0) {
        // C and C++ string literals are const char*, not std::string
        // (std::string requires explicit construction or "hello"s suffix)
        return cbm_type_pointer(ctx->arena, cbm_type_builtin(ctx->arena, "char"));
    }
    if (strcmp(kind, "char_literal") == 0) {
        return cbm_type_builtin(ctx->arena, "char");
    }
    if (strcmp(kind, "true") == 0 || strcmp(kind, "false") == 0) {
        return cbm_type_builtin(ctx->arena, "bool");
    }
    if (strcmp(kind, "null") == 0 || strcmp(kind, "nullptr") == 0) {
        return cbm_type_pointer(ctx->arena, cbm_type_unknown());
    }

    // --- compound_literal_expression: (Type){...} ---
    if (strcmp(kind, "compound_literal_expression") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node))
            return c_parse_type_node(ctx, type_node);
        return cbm_type_unknown();
    }

    // --- assignment_expression: a = b (returns LHS type) ---
    if (strcmp(kind, "assignment_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        if (!ts_node_is_null(left))
            return c_eval_expr_type(ctx, left);
        return cbm_type_unknown();
    }

    // --- initializer_list: {1, 2, 3} ---
    if (strcmp(kind, "initializer_list") == 0) {
        return cbm_type_unknown(); // context-dependent
    }

    return cbm_type_unknown();
}

// ============================================================================
// c_lookup_member: method/field lookup with base class traversal
// ============================================================================

static const CBMRegisteredFunc *c_lookup_member_depth(CLSPContext *ctx, const char *type_qn,
                                                      const char *member_name, int depth) {
    if (!type_qn || !member_name)
        return NULL;
    if (depth > CBM_LSP_MAX_LOOKUP_DEPTH)
        return NULL;

    // Direct method lookup
    const CBMRegisteredFunc *f = cbm_registry_lookup_method(ctx->registry, type_qn, member_name);
    if (f)
        return f;

    // Try module-prefixed QN (e.g., "Container" -> "test.main.Container")
    if (ctx->module_qn) {
        const char *prefixed = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn);
        f = cbm_registry_lookup_method(ctx->registry, prefixed, member_name);
        if (f)
            return f;
    }

    // Scope-based alias: using Vec = std::vector<T>;
    // Vec is in scope as ALIAS → follow to underlying type's QN
    {
        const CBMType *scoped = cbm_scope_lookup(ctx->current_scope, type_qn);
        if (scoped && scoped->kind == CBM_TYPE_ALIAS) {
            const CBMType *underlying = cbm_type_resolve_alias(scoped);
            if (underlying && !cbm_type_is_unknown(underlying)) {
                const char *alias_target_qn = type_to_qn(underlying);
                if (alias_target_qn) {
                    f = c_lookup_member_depth(ctx, alias_target_qn, member_name, depth + 1);
                    if (f)
                        return f;
                }
            }
        }
    }

    // Check registered type for alias and base classes
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt && ctx->module_qn) {
        rt = cbm_registry_lookup_type(
            ctx->registry, cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn));
    }
    if (rt) {
        // Alias chain
        if (rt->alias_of) {
            f = c_lookup_member_depth(ctx, rt->alias_of, member_name, depth + 1);
            if (f)
                return f;
        }

        // Base classes (embedded_types stores base class QNs)
        if (rt->embedded_types) {
            for (int i = 0; rt->embedded_types[i]; i++) {
                f = c_lookup_member_depth(ctx, rt->embedded_types[i], member_name, depth + 1);
                if (f)
                    return f;
            }
        }
    }

    return NULL;
}

const CBMRegisteredFunc *c_lookup_member(CLSPContext *ctx, const char *type_qn,
                                         const char *member_name) {
    return c_lookup_member_depth(ctx, type_qn, member_name, 0);
}

// Field type lookup
static const CBMType *c_lookup_field_type(CLSPContext *ctx, const char *type_qn,
                                          const char *field_name, int depth) {
    if (!type_qn || !field_name || depth > 5)
        return NULL;

    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt && ctx->module_qn) {
        rt = cbm_registry_lookup_type(
            ctx->registry, cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn));
    }
    if (!rt)
        return NULL;

    if (rt->alias_of)
        return c_lookup_field_type(ctx, rt->alias_of, field_name, depth + 1);

    if (rt->field_names) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], field_name) == 0 && rt->field_types &&
                rt->field_types[i])
                return rt->field_types[i];
        }
    }

    // Base classes
    if (rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            const CBMType *f =
                c_lookup_field_type(ctx, rt->embedded_types[i], field_name, depth + 1);
            if (f)
                return f;
        }
    }
    return NULL;
}

// ============================================================================
// c_process_statement: bind variables from statements
// ============================================================================

// Parse a declaration to extract type and declarators
static const CBMType *c_parse_declaration_type(CLSPContext *ctx, TSNode decl_node) {
    // Look for type node in declaration children
    uint32_t nc = ts_node_named_child_count(decl_node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(decl_node, i);
        const char *ck = ts_node_type(child);

        // Skip declarators and non-type specifiers
        if (strcmp(ck, "init_declarator") == 0 || strcmp(ck, "identifier") == 0 ||
            strcmp(ck, "pointer_declarator") == 0 || strcmp(ck, "reference_declarator") == 0 ||
            strcmp(ck, "array_declarator") == 0 || strcmp(ck, "function_declarator") == 0 ||
            strcmp(ck, "storage_class_specifier") == 0 || strcmp(ck, "type_qualifier") == 0 ||
            strcmp(ck, "virtual") == 0 || strcmp(ck, "explicit") == 0 ||
            strcmp(ck, "virtual_function_specifier") == 0 || strcmp(ck, "access_specifier") == 0 ||
            strcmp(ck, "friend") == 0 || strcmp(ck, "comment") == 0)
            continue;

        // Found a type node
        return c_parse_type_node(ctx, child);
    }
    return cbm_type_unknown();
}

void c_process_statement(CLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    // declaration: Type var = expr; or Type var1, var2;
    if (strcmp(kind, "declaration") == 0) {
        const CBMType *base_type = c_parse_declaration_type(ctx, node);
        bool has_auto = false;

        // Check if type is auto/decltype
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "placeholder_type_specifier") == 0 || strcmp(ck, "auto") == 0) {
                has_auto = true;
                break;
            }
            if (strcmp(ck, "decltype") == 0) {
                base_type = c_parse_type_node(ctx, child);
                break;
            }
        }

        // Process each declarator
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);

            if (strcmp(ck, "init_declarator") == 0) {
                TSNode decl = ts_node_child_by_field_name(child, "declarator", 10);
                TSNode value = ts_node_child_by_field_name(child, "value", 5);

                const CBMType *var_type = base_type;

                // For auto, infer from initializer
                if (has_auto && !ts_node_is_null(value)) {
                    var_type = c_eval_expr_type(ctx, value);
                }

                // Get variable name from declarator
                if (!ts_node_is_null(decl)) {
                    const char *dk = ts_node_type(decl);
                    char *var_name = NULL;

                    if (strcmp(dk, "identifier") == 0) {
                        var_name = c_node_text(ctx, decl);
                    } else if (strcmp(dk, "pointer_declarator") == 0) {
                        // Count pointer depth and find identifier
                        int ptr_depth = 0;
                        TSNode inner = decl;
                        while (!ts_node_is_null(inner) &&
                               strcmp(ts_node_type(inner), "pointer_declarator") == 0) {
                            ptr_depth++;
                            uint32_t dnc = ts_node_named_child_count(inner);
                            inner = dnc > 0 ? ts_node_named_child(inner, dnc - 1) : (TSNode){0};
                        }
                        if (!ts_node_is_null(inner) &&
                            strcmp(ts_node_type(inner), "identifier") == 0)
                            var_name = c_node_text(ctx, inner);
                        // For auto*, the deduced type already includes pointer depth
                        // (e.g., auto* w = new Widget() → eval gives Widget*)
                        if (!has_auto) {
                            for (int d = 0; d < ptr_depth; d++)
                                var_type = cbm_type_pointer(ctx->arena, var_type);
                        }
                    } else if (strcmp(dk, "reference_declarator") == 0) {
                        if (ts_node_named_child_count(decl) > 0) {
                            TSNode inner = ts_node_named_child(decl, 0);
                            const char *ik = ts_node_type(inner);
                            if (strcmp(ik, "identifier") == 0) {
                                var_name = c_node_text(ctx, inner);
                            } else if (strcmp(ik, "structured_binding_declarator") == 0) {
                                // const auto& [a, b] = expr; — delegate to structured binding
                                const CBMType *rhs_type = cbm_type_unknown();
                                if (!ts_node_is_null(value)) {
                                    rhs_type = c_eval_expr_type(ctx, value);
                                    rhs_type = c_simplify_type(ctx, rhs_type, false);
                                }
                                uint32_t bnc = ts_node_named_child_count(inner);
                                int binding_idx = 0;
                                for (uint32_t bi = 0; bi < bnc; bi++) {
                                    TSNode bn = ts_node_named_child(inner, bi);
                                    if (strcmp(ts_node_type(bn), "identifier") == 0) {
                                        char *bname = c_node_text(ctx, bn);
                                        if (!bname)
                                            continue;
                                        const CBMType *elem_type = cbm_type_unknown();
                                        if (rhs_type && rhs_type->kind == CBM_TYPE_TEMPLATE &&
                                            rhs_type->data.template_type.template_args) {
                                            int nta = rhs_type->data.template_type.arg_count;
                                            if (binding_idx < nta)
                                                elem_type = rhs_type->data.template_type
                                                                .template_args[binding_idx];
                                        }
                                        if (cbm_type_is_unknown(elem_type) && rhs_type &&
                                            rhs_type->kind == CBM_TYPE_SLICE)
                                            elem_type = rhs_type->data.slice.elem;
                                        if (cbm_type_is_unknown(elem_type) && rhs_type) {
                                            const char *rhs_qn = type_to_qn(rhs_type);
                                            const CBMRegisteredType *rt =
                                                rhs_qn ? cbm_registry_lookup_type(ctx->registry,
                                                                                  rhs_qn)
                                                       : NULL;
                                            if (!rt && rhs_qn && ctx->module_qn)
                                                rt = cbm_registry_lookup_type(
                                                    ctx->registry,
                                                    cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                                      ctx->module_qn, rhs_qn));
                                            if (rt && rt->field_types &&
                                                rt->field_types[binding_idx])
                                                elem_type = rt->field_types[binding_idx];
                                        }
                                        cbm_scope_bind(ctx->current_scope, bname, elem_type);
                                        binding_idx++;
                                    }
                                }
                                continue;
                            }
                        }
                        // For auto&, the deduced type is already correct
                        if (!has_auto)
                            var_type = cbm_type_reference(ctx->arena, var_type);
                    } else if (strcmp(dk, "function_declarator") == 0) {
                        // Function pointer: int (*fp)(int) = &target_func;
                        // Walk into function_declarator → parenthesized_declarator →
                        // pointer_declarator → identifier
                        TSNode inner = ts_node_child_by_field_name(decl, "declarator", 10);
                        while (!ts_node_is_null(inner)) {
                            const char *ik = ts_node_type(inner);
                            if (strcmp(ik, "identifier") == 0) {
                                var_name = c_node_text(ctx, inner);
                                break;
                            } else if (strcmp(ik, "parenthesized_declarator") == 0 ||
                                       strcmp(ik, "pointer_declarator") == 0) {
                                // Walk deeper: get last named child
                                uint32_t nc = ts_node_named_child_count(inner);
                                inner = nc > 0 ? ts_node_named_child(inner, nc - 1) : (TSNode){0};
                            } else {
                                break;
                            }
                        }
                        // Mark as pointer type (function pointer decays to pointer)
                        var_type = cbm_type_pointer(ctx->arena, var_type);
                    } else if (strcmp(dk, "array_declarator") == 0) {
                        // Type var[N]; — extract name and wrap in slice
                        TSNode inner = ts_node_child_by_field_name(decl, "declarator", 10);
                        if (!ts_node_is_null(inner) &&
                            strcmp(ts_node_type(inner), "identifier") == 0) {
                            var_name = c_node_text(ctx, inner);
                        }
                        var_type = cbm_type_slice(ctx->arena, var_type);
                    } else if (strcmp(dk, "structured_binding_declarator") == 0) {
                        // auto [a, b] = expr;
                        // Try to decompose pair/tuple from initializer type
                        const CBMType *rhs_type = cbm_type_unknown();
                        if (!ts_node_is_null(value)) {
                            rhs_type = c_eval_expr_type(ctx, value);
                            rhs_type = c_simplify_type(ctx, rhs_type, false);
                        }
                        uint32_t bnc = ts_node_named_child_count(decl);
                        int binding_idx = 0;
                        for (uint32_t bi = 0; bi < bnc; bi++) {
                            TSNode bn = ts_node_named_child(decl, bi);
                            if (strcmp(ts_node_type(bn), "identifier") == 0) {
                                char *bname = c_node_text(ctx, bn);
                                if (!bname)
                                    continue;
                                const CBMType *elem_type = cbm_type_unknown();
                                // Decompose std::pair<T1,T2> → first=T1, second=T2
                                if (rhs_type && rhs_type->kind == CBM_TYPE_TEMPLATE &&
                                    rhs_type->data.template_type.template_args) {
                                    int nta = rhs_type->data.template_type.arg_count;
                                    if (binding_idx < nta) {
                                        elem_type =
                                            rhs_type->data.template_type.template_args[binding_idx];
                                    }
                                }
                                // Decompose array (SLICE) → each binding gets element type
                                if (cbm_type_is_unknown(elem_type) && rhs_type &&
                                    rhs_type->kind == CBM_TYPE_SLICE) {
                                    elem_type = rhs_type->data.slice.elem;
                                }
                                // Decompose struct fields
                                if (cbm_type_is_unknown(elem_type) && rhs_type) {
                                    const char *rhs_qn = type_to_qn(rhs_type);
                                    const CBMRegisteredType *rt =
                                        rhs_qn ? cbm_registry_lookup_type(ctx->registry, rhs_qn)
                                               : NULL;
                                    if (!rt && rhs_qn && ctx->module_qn)
                                        rt = cbm_registry_lookup_type(
                                            ctx->registry,
                                            cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn,
                                                              rhs_qn));
                                    if (rt && rt->field_types && rt->field_types[binding_idx]) {
                                        elem_type = rt->field_types[binding_idx];
                                    }
                                }
                                cbm_scope_bind(ctx->current_scope, bname, elem_type);
                                binding_idx++;
                            }
                        }
                        continue;
                    }

                    if (var_name && var_name[0] && strcmp(var_name, "_") != 0) {
                        cbm_scope_bind(ctx->current_scope, var_name, var_type);

                        // Track function pointer targets: fp = &foo or fp = foo
                        // Accept pointer, func, or named types (typedef function pointers
                        // like fn_t appear as CBM_TYPE_NAMED, not FUNC/POINTER).
                        if (var_type && (var_type->kind == CBM_TYPE_FUNC ||
                                         var_type->kind == CBM_TYPE_POINTER ||
                                         var_type->kind == CBM_TYPE_NAMED)) {
                            if (!ts_node_is_null(value)) {
                                const char *vk = ts_node_type(value);
                                // pointer_expression with & operator: &foo
                                if (strcmp(vk, "pointer_expression") == 0 ||
                                    strcmp(vk, "unary_expression") == 0) {
                                    uint32_t vnc = ts_node_named_child_count(value);
                                    for (uint32_t vi = 0; vi < vnc; vi++) {
                                        TSNode vch = ts_node_named_child(value, vi);
                                        if (strcmp(ts_node_type(vch), "identifier") == 0) {
                                            char *target_name = c_node_text(ctx, vch);
                                            if (target_name) {
                                                const char *target_qn =
                                                    c_resolve_name_to_func_qn(ctx, target_name);
                                                if (target_qn) {
                                                    c_add_fp_target(ctx, var_name, target_qn);
                                                }
                                            }
                                            break;
                                        }
                                    }
                                }
                                // Direct identifier: fp = foo (function decays to pointer)
                                else if (strcmp(vk, "identifier") == 0) {
                                    char *target_name = c_node_text(ctx, value);
                                    if (target_name) {
                                        const char *target_qn =
                                            c_resolve_name_to_func_qn(ctx, target_name);
                                        if (target_qn) {
                                            c_add_fp_target(ctx, var_name, target_qn);
                                        }
                                    }
                                }
                            }
                        }

                        // Fallback: if RHS is an identifier resolving to a known function,
                        // track as fp target even when var_type is unknown (typedef func ptrs).
                        if (!c_lookup_fp_target(ctx, var_name) && !ts_node_is_null(value)) {
                            const char *vk = ts_node_type(value);
                            if (strcmp(vk, "identifier") == 0) {
                                char *target_name = c_node_text(ctx, value);
                                if (target_name) {
                                    const char *target_qn =
                                        c_resolve_name_to_func_qn(ctx, target_name);
                                    if (target_qn) {
                                        c_add_fp_target(ctx, var_name, target_qn);
                                    }
                                }
                            }
                        }

                        // DLL/dynamic resolver heuristic: fp = (FuncType)Resolve("FuncName")
                        // If RHS is a call (possibly cast-wrapped) with a string literal arg,
                        // and variable has function-pointer-like type or RHS has a cast,
                        // treat the string as an external function name.
                        if (!c_lookup_fp_target(ctx, var_name) && !ts_node_is_null(value)) {
                            bool has_cast = false;
                            const char *dll_func =
                                c_extract_dll_resolve_name(ctx, value, &has_cast);
                            if (dll_func) {
                                bool is_fp_type = var_type && (var_type->kind == CBM_TYPE_FUNC ||
                                                               var_type->kind == CBM_TYPE_POINTER);
                                if (is_fp_type || has_cast) {
                                    const char *target_qn =
                                        cbm_arena_sprintf(ctx->arena, "external.%s", dll_func);
                                    c_add_fp_target(ctx, var_name, target_qn);
                                }
                            }
                        }
                    }
                }
            } else if (strcmp(ck, "identifier") == 0) {
                // Bare declaration without initializer: Type var;
                char *var_name = c_node_text(ctx, child);
                if (var_name && var_name[0]) {
                    cbm_scope_bind(ctx->current_scope, var_name, base_type);
                }
            } else if (strcmp(ck, "pointer_declarator") == 0) {
                // Type *var; without initializer
                int ptr_depth = 0;
                TSNode inner = child;
                while (!ts_node_is_null(inner) &&
                       strcmp(ts_node_type(inner), "pointer_declarator") == 0) {
                    ptr_depth++;
                    uint32_t dnc = ts_node_named_child_count(inner);
                    inner = dnc > 0 ? ts_node_named_child(inner, dnc - 1) : (TSNode){0};
                }
                if (!ts_node_is_null(inner) && strcmp(ts_node_type(inner), "identifier") == 0) {
                    char *var_name = c_node_text(ctx, inner);
                    if (var_name) {
                        const CBMType *vt = base_type;
                        for (int d = 0; d < ptr_depth; d++)
                            vt = cbm_type_pointer(ctx->arena, vt);
                        cbm_scope_bind(ctx->current_scope, var_name, vt);
                    }
                }
            } else if (strcmp(ck, "array_declarator") == 0) {
                // Type var[N]; without initializer
                TSNode inner = ts_node_child_by_field_name(child, "declarator", 10);
                if (!ts_node_is_null(inner) && strcmp(ts_node_type(inner), "identifier") == 0) {
                    char *var_name = c_node_text(ctx, inner);
                    if (var_name) {
                        cbm_scope_bind(ctx->current_scope, var_name,
                                       cbm_type_slice(ctx->arena, base_type));
                    }
                }
            }
        }
        return;
    }

    // using_declaration: using namespace std; or using std::cout;
    if (strcmp(kind, "using_declaration") == 0) {
        char *text = c_node_text(ctx, node);
        if (!text)
            return;

        // "using namespace XXX;"
        if (strstr(text, "namespace")) {
            // Extract namespace name after "namespace"
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode child = ts_node_named_child(node, i);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "identifier") == 0 || strcmp(ck, "qualified_identifier") == 0 ||
                    strcmp(ck, "scoped_identifier") == 0) {
                    char *ns_name = c_node_text(ctx, child);
                    if (ns_name)
                        c_add_using_namespace(ctx, c_build_qn(ctx, ns_name));
                }
            }
        } else if (strstr(text, "enum")) {
            // "using enum MyEnum;" — import enum members into scope
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode child = ts_node_named_child(node, i);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "identifier") == 0 || strcmp(ck, "qualified_identifier") == 0 ||
                    strcmp(ck, "scoped_identifier") == 0 || strcmp(ck, "type_identifier") == 0) {
                    char *enum_text = c_node_text(ctx, child);
                    if (enum_text) {
                        const char *enum_qn = c_build_qn(ctx, enum_text);
                        // Try to find the enum type in registry, import its members
                        const CBMRegisteredType *et =
                            cbm_registry_lookup_type(ctx->registry, enum_qn);
                        if (!et && ctx->module_qn) {
                            enum_qn =
                                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, enum_qn);
                            et = cbm_registry_lookup_type(ctx->registry, enum_qn);
                        }
                        if (!et && ctx->current_namespace) {
                            enum_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace,
                                                        enum_text);
                            et = cbm_registry_lookup_type(ctx->registry, enum_qn);
                        }
                        // Even if enum type not found, add as using namespace for name lookup
                        c_add_using_namespace(ctx, enum_qn);
                    }
                }
            }
        } else {
            // "using std::cout;" — specific declaration
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode child = ts_node_named_child(node, i);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "qualified_identifier") == 0 ||
                    strcmp(ck, "scoped_identifier") == 0) {
                    char *full_text = c_node_text(ctx, child);
                    if (full_text) {
                        const char *qn = c_build_qn(ctx, full_text);
                        // Extract short name (last component)
                        const char *short_name = strrchr(qn, '.');
                        short_name = short_name ? short_name + 1 : qn;
                        c_add_using_decl(ctx, short_name, qn);
                    }
                }
            }
        }
        return;
    }

    // alias_declaration: using Foo = Bar;
    if (strcmp(kind, "alias_declaration") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(name_node) && !ts_node_is_null(type_node)) {
            char *alias_name = c_node_text(ctx, name_node);
            const CBMType *target = c_parse_type_node(ctx, type_node);
            if (alias_name && target) {
                // Build alias QN
                const char *alias_qn =
                    ctx->current_namespace
                        ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace, alias_name)
                        : cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, alias_name);
                cbm_scope_bind(ctx->current_scope, alias_name,
                               cbm_type_alias(ctx->arena, alias_qn, target));
            }
        }
        return;
    }

    // type_definition: typedef Bar Foo; or typedef struct X Y;
    if (strcmp(kind, "type_definition") == 0) {
        // Get the type being aliased and the alias name(s).
        // Tree-sitter produces: type_definition { type_spec alias_name }
        // For "typedef RealWidget Widget": { type_identifier("RealWidget")
        // type_identifier("Widget") } For "typedef struct Foo Bar": { struct_specifier
        // type_identifier("Bar") } The first type_identifier may be the source type (when no
        // specifier precedes it).
        const CBMType *target = cbm_type_unknown();
        uint32_t nc = ts_node_named_child_count(node);
        bool found_type = false;
        int first_type_id_idx = -1;
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (!found_type && strcmp(ck, "type_identifier") != 0 &&
                strcmp(ck, "pointer_declarator") != 0 && strcmp(ck, "identifier") != 0) {
                target = c_parse_type_node(ctx, child);
                found_type = true;
            } else if (!found_type && strcmp(ck, "type_identifier") == 0 && first_type_id_idx < 0) {
                // First type_identifier without a preceding specifier — this IS the source type.
                // Mark it and parse; the NEXT type_identifier will be the alias name.
                first_type_id_idx = (int)i;
                target = c_parse_type_node(ctx, child);
                found_type = true;
            } else if (found_type && strcmp(ck, "type_identifier") == 0) {
                char *alias_name = c_node_text(ctx, child);
                if (alias_name) {
                    const char *alias_qn =
                        ctx->current_namespace
                            ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace,
                                                alias_name)
                            : cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, alias_name);
                    cbm_scope_bind(ctx->current_scope, alias_name,
                                   cbm_type_alias(ctx->arena, alias_qn, target));
                }
            }
        }
        return;
    }

    // namespace_alias_definition: namespace fs = std::filesystem;
    if (strcmp(kind, "namespace_alias_definition") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode value_node = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value_node)) {
            // Try last named child
            uint32_t nc = ts_node_named_child_count(node);
            if (nc >= 2) {
                name_node = ts_node_named_child(node, 0);
                value_node = ts_node_named_child(node, nc - 1);
            }
        }
        if (!ts_node_is_null(name_node) && !ts_node_is_null(value_node)) {
            char *alias = c_node_text(ctx, name_node);
            char *target = c_node_text(ctx, value_node);
            if (alias && target) {
                c_add_ns_alias(ctx, alias, c_build_qn(ctx, target));
            }
        }
        return;
    }

    // for_range_loop: for (auto& x : container) { ... }
    if (strcmp(kind, "for_range_loop") == 0) {
        TSNode decl = ts_node_child_by_field_name(node, "declarator", 10);
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);

        const CBMType *elem_type = cbm_type_unknown();
        if (!ts_node_is_null(right)) {
            const CBMType *container_type = c_eval_expr_type(ctx, right);
            // Deduce element type: container's begin()->operator* or element type
            if (container_type) {
                if (container_type->kind == CBM_TYPE_SLICE) {
                    elem_type = container_type->data.slice.elem;
                } else if (container_type->kind == CBM_TYPE_TEMPLATE &&
                           container_type->data.template_type.arg_count > 0) {
                    // Map containers: element is pair<K,V> not just K
                    const char *tname = container_type->data.template_type.template_name;
                    if (tname && container_type->data.template_type.arg_count >= 2 &&
                        (strstr(tname, "map") || strstr(tname, "Map"))) {
                        const CBMType *pair_args[3];
                        pair_args[0] = container_type->data.template_type.template_args[0];
                        pair_args[1] = container_type->data.template_type.template_args[1];
                        pair_args[2] = NULL;
                        elem_type = cbm_type_template(ctx->arena, "std.pair", pair_args, 2);
                    } else {
                        elem_type = container_type->data.template_type.template_args[0];
                    }
                }
                // Iterator protocol fallback: begin() -> iter -> operator*() -> elem
                if (cbm_type_is_unknown(elem_type) && container_type->kind == CBM_TYPE_NAMED) {
                    const char *cqn = container_type->data.named.qualified_name;
                    const CBMRegisteredFunc *begin_fn = c_lookup_member(ctx, cqn, "begin");
                    if (begin_fn && begin_fn->signature &&
                        begin_fn->signature->kind == CBM_TYPE_FUNC &&
                        begin_fn->signature->data.func.return_types &&
                        begin_fn->signature->data.func.return_types[0]) {
                        const CBMType *iter_type = begin_fn->signature->data.func.return_types[0];
                        const char *iter_qn = type_to_qn(iter_type);
                        if (iter_qn) {
                            const CBMRegisteredFunc *deref =
                                c_lookup_member(ctx, iter_qn, "operator*");
                            if (deref && deref->signature &&
                                deref->signature->kind == CBM_TYPE_FUNC &&
                                deref->signature->data.func.return_types &&
                                deref->signature->data.func.return_types[0]) {
                                elem_type = deref->signature->data.func.return_types[0];
                            }
                        }
                    }
                }
            }
        }

        // If explicit type, use that
        if (!ts_node_is_null(type_node)) {
            const char *tk = ts_node_type(type_node);
            if (strcmp(tk, "placeholder_type_specifier") != 0 && strcmp(tk, "auto") != 0) {
                elem_type = c_parse_type_node(ctx, type_node);
            }
        }

        // Bind declarator
        if (!ts_node_is_null(decl)) {
            // Unwrap reference_declarator to get inner declarator
            TSNode bind_target = decl;
            const char *dk = ts_node_type(bind_target);
            if (strcmp(dk, "reference_declarator") == 0 &&
                ts_node_named_child_count(bind_target) > 0) {
                bind_target = ts_node_named_child(bind_target, 0);
                dk = ts_node_type(bind_target);
            }

            if (strcmp(dk, "identifier") == 0) {
                char *var_name = c_node_text(ctx, bind_target);
                if (var_name)
                    cbm_scope_bind(ctx->current_scope, var_name, elem_type);
            } else if (strcmp(dk, "structured_binding_declarator") == 0) {
                // for (auto& [k, v] : container) — decompose elem_type
                uint32_t bnc = ts_node_named_child_count(bind_target);
                int binding_idx = 0;
                for (uint32_t bi = 0; bi < bnc; bi++) {
                    TSNode bn = ts_node_named_child(bind_target, bi);
                    if (strcmp(ts_node_type(bn), "identifier") == 0) {
                        char *bname = c_node_text(ctx, bn);
                        if (!bname)
                            continue;
                        const CBMType *bt = cbm_type_unknown();
                        // TEMPLATE decomposition (e.g., pair<K,V>)
                        if (elem_type && elem_type->kind == CBM_TYPE_TEMPLATE &&
                            elem_type->data.template_type.template_args &&
                            binding_idx < elem_type->data.template_type.arg_count) {
                            bt = elem_type->data.template_type.template_args[binding_idx];
                        }
                        // SLICE decomposition
                        if (cbm_type_is_unknown(bt) && elem_type &&
                            elem_type->kind == CBM_TYPE_SLICE) {
                            bt = elem_type->data.slice.elem;
                        }
                        // Struct field decomposition
                        if (cbm_type_is_unknown(bt) && elem_type) {
                            const char *eq = type_to_qn(elem_type);
                            const CBMRegisteredType *rt =
                                eq ? cbm_registry_lookup_type(ctx->registry, eq) : NULL;
                            if (!rt && eq && ctx->module_qn)
                                rt = cbm_registry_lookup_type(
                                    ctx->registry,
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, eq));
                            if (rt && rt->field_types && rt->field_types[binding_idx])
                                bt = rt->field_types[binding_idx];
                        }
                        cbm_scope_bind(ctx->current_scope, bname, bt);
                        binding_idx++;
                    }
                }
            }
        }
        return;
    }

    // parameter_declaration: bind param
    if (strcmp(kind, "parameter_declaration") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        TSNode decl = ts_node_child_by_field_name(node, "declarator", 10);
        if (ts_node_is_null(type_node))
            return;

        const CBMType *param_type = c_parse_type_node(ctx, type_node);

        if (!ts_node_is_null(decl)) {
            const char *dk = ts_node_type(decl);
            if (strcmp(dk, "identifier") == 0) {
                char *name = c_node_text(ctx, decl);
                if (name)
                    cbm_scope_bind(ctx->current_scope, name, param_type);
            } else if (strcmp(dk, "pointer_declarator") == 0) {
                int ptr_depth = 0;
                TSNode inner = decl;
                while (!ts_node_is_null(inner) &&
                       strcmp(ts_node_type(inner), "pointer_declarator") == 0) {
                    ptr_depth++;
                    uint32_t dnc = ts_node_named_child_count(inner);
                    inner = dnc > 0 ? ts_node_named_child(inner, dnc - 1) : (TSNode){0};
                }
                if (!ts_node_is_null(inner) && strcmp(ts_node_type(inner), "identifier") == 0) {
                    char *name = c_node_text(ctx, inner);
                    if (name) {
                        const CBMType *vt = param_type;
                        for (int d = 0; d < ptr_depth; d++)
                            vt = cbm_type_pointer(ctx->arena, vt);
                        cbm_scope_bind(ctx->current_scope, name, vt);
                    }
                }
            } else if (strcmp(dk, "reference_declarator") == 0) {
                if (ts_node_named_child_count(decl) > 0) {
                    TSNode inner = ts_node_named_child(decl, 0);
                    if (strcmp(ts_node_type(inner), "identifier") == 0) {
                        char *name = c_node_text(ctx, inner);
                        if (name)
                            cbm_scope_bind(ctx->current_scope, name,
                                           cbm_type_reference(ctx->arena, param_type));
                    }
                }
            }
        }
        return;
    }
}

// ============================================================================
// Emit helpers
// ============================================================================

static void c_emit_resolved_call(CLSPContext *ctx, const char *callee_qn, const char *strategy,
                                 float confidence) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    rc.reason = NULL;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

static void c_emit_unresolved_call(CLSPContext *ctx, const char *expr_text, const char *reason) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

// ============================================================================
// resolve_calls_in_node: walk AST and resolve calls
// ============================================================================

static void c_resolve_calls_in_node_inner(CLSPContext *ctx, TSNode node);

#define C_LSP_MAX_WALK_DEPTH 512

/* Depth-guarded entry: the AST walk recurses per nesting level and crashed
 * with a stack overflow on deeply nested real-world C++ (bitcoin, SIGSEGV in
 * cbm_type_substitute under hundreds of recursive c_resolve_calls_in_node
 * frames via c_adl_resolve). Past the cap the subtree is skipped — its calls
 * stay unresolved, which is graceful degradation, not a crash. */
static void c_resolve_calls_in_node(CLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= C_LSP_MAX_WALK_DEPTH)
        return;
    ctx->walk_depth++;
    c_resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}

static void c_resolve_calls_in_node_inner(CLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    // Process statements for scope building
    c_process_statement(ctx, node);

    // --- Resolve call expressions ---
    if (strcmp(kind, "call_expression") == 0) {
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        if (!ts_node_is_null(func_node)) {
            const char *fk = ts_node_type(func_node);

            // field_expression: obj.method() or ptr->method()
            if (strcmp(fk, "field_expression") == 0) {
                TSNode arg_node = ts_node_child_by_field_name(func_node, "argument", 8);
                TSNode field_node = ts_node_child_by_field_name(func_node, "field", 5);
                if (!ts_node_is_null(arg_node) && !ts_node_is_null(field_node)) {
                    char *field_name = c_node_text(ctx, field_node);
                    if (field_name) {
                        const CBMType *obj_type = c_eval_expr_type(ctx, arg_node);

                        // Determine if arrow or dot
                        bool is_arrow = false;
                        uint32_t fnc = ts_node_child_count(func_node);
                        for (uint32_t fi = 0; fi < fnc; fi++) {
                            TSNode ch = ts_node_child(func_node, fi);
                            if (!ts_node_is_named(ch)) {
                                char *op = c_node_text(ctx, ch);
                                if (op && strcmp(op, "->") == 0) {
                                    is_arrow = true;
                                    break;
                                }
                            }
                        }

                        const CBMType *base = is_arrow ? c_simplify_type(ctx, obj_type, true)
                                                       : c_simplify_type(ctx, obj_type, false);

                        const char *type_qn = type_to_qn(base);
                        if (type_qn) {
                            int arg_count = 0;
                            const CBMType **arg_types =
                                c_extract_call_arg_types(ctx, node, &arg_count);
                            if (ctx->debug)
                                fprintf(stderr,
                                        "  [clsp] member call: type_qn=%s field=%s args=%d\n",
                                        type_qn, field_name, arg_count);
                            // Use type-aware overload scoring
                            const CBMRegisteredFunc *method = cbm_registry_lookup_method_by_types(
                                ctx->registry, type_qn, field_name, arg_types, arg_count);
                            // Fall back to c_lookup_member for base class traversal
                            if (!method)
                                method = c_lookup_member(ctx, type_qn, field_name);
                            if (ctx->debug)
                                fprintf(stderr, "  [clsp] member call result: %s\n",
                                        method ? method->qualified_name : "NULL");
                            if (method) {
                                const char *strategy = "lsp_type_dispatch";
                                // Check if resolved through base class — prefer derived override
                                if (method->receiver_type &&
                                    strcmp(method->receiver_type, type_qn) != 0) {
                                    const CBMRegisteredFunc *override_m =
                                        cbm_registry_lookup_method(ctx->registry, type_qn,
                                                                   field_name);
                                    if (override_m) {
                                        method = override_m;
                                        strategy = "lsp_virtual_dispatch";
                                    } else {
                                        strategy = "lsp_base_dispatch";
                                    }
                                }
                                // Check if through smart pointer
                                if (is_arrow && obj_type->kind == CBM_TYPE_TEMPLATE &&
                                    is_smart_ptr(obj_type->data.template_type.template_name))
                                    strategy = "lsp_smart_ptr_dispatch";
                                c_emit_resolved_call(ctx, method->qualified_name, strategy, 0.95f);
                                goto recurse;
                            }
                        }

                        // TYPE_PARAM receiver: store as pending template call
                        if (base && base->kind == CBM_TYPE_TYPE_PARAM && ctx->enclosing_func_qn &&
                            ctx->in_template) {
                            int ac = 0;
                            c_extract_call_arg_types(ctx, node, &ac);
                            c_add_pending_template_call(ctx, ctx->enclosing_func_qn,
                                                        base->data.type_param.name, field_name, ac);
                        }

                        // Unresolved
                        if (cbm_type_is_unknown(obj_type)) {
                            char *arg_text = c_node_text(ctx, arg_node);
                            c_emit_unresolved_call(ctx,
                                                   cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                                     arg_text ? arg_text : "?",
                                                                     field_name),
                                                   "unknown_receiver_type");
                        } else if (type_qn) {
                            c_emit_unresolved_call(
                                ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, field_name),
                                "method_not_found");
                        }
                    }
                }
                goto recurse;
            }

            // qualified_identifier: ns::func() or Class::static_method()
            // Also handles ns::func<T>() where name child is template_function
            if (strcmp(fk, "qualified_identifier") == 0 || strcmp(fk, "scoped_identifier") == 0) {
                char *text = c_node_text(ctx, func_node);
                if (text) {
                    const char *qn = c_build_qn(ctx, text);
                    // Strip template args from last component only:
                    // std.get<Widget> → std.get, but Registry<int>.init stays unchanged
                    const char *angle = strchr(qn, '<');
                    if (angle) {
                        // Only strip if no '.' follows the '<' (i.e., template is in the last
                        // segment)
                        const char *dot_after = strchr(angle, '.');
                        if (!dot_after) {
                            size_t strip_len = (size_t)(angle - qn);
                            char *stripped = (char *)cbm_arena_alloc(ctx->arena, strip_len + 1);
                            memcpy(stripped, qn, strip_len);
                            stripped[strip_len] = '\0';
                            qn = stripped;
                        } else {
                            // Strip <...> segment but keep what follows: Registry<int>.init →
                            // Registry.init
                            const char *close = strchr(angle, '>');
                            if (close && close < dot_after) {
                                size_t prefix_len = (size_t)(angle - qn);
                                size_t suffix_len = strlen(dot_after);
                                char *stripped = (char *)cbm_arena_alloc(
                                    ctx->arena, prefix_len + suffix_len + 1);
                                memcpy(stripped, qn, prefix_len);
                                memcpy(stripped + prefix_len, dot_after, suffix_len);
                                stripped[prefix_len + suffix_len] = '\0';
                                qn = stripped;
                            }
                        }
                    }
                    int arg_count = 0;
                    const CBMType **arg_types = c_extract_call_arg_types(ctx, node, &arg_count);
                    // Module-prefixed first (shadows stdlib stubs), then bare QN
                    const CBMRegisteredFunc *f = NULL;
                    if (ctx->module_qn) {
                        f = cbm_registry_lookup_func(
                            ctx->registry,
                            cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, qn));
                    }
                    if (!f)
                        f = cbm_registry_lookup_func(ctx->registry, qn);
                    if (f) {
                        c_emit_resolved_call(ctx, f->qualified_name, "lsp_scoped", 0.95f);
                        goto recurse;
                    }
                    // Try as method (Class::method) with type-aware overload resolution
                    const char *dot = strrchr(qn, '.');
                    if (dot) {
                        size_t prefix_len = dot - qn;
                        char *class_qn = (char *)cbm_arena_alloc(ctx->arena, prefix_len + 1);
                        memcpy(class_qn, qn, prefix_len);
                        class_qn[prefix_len] = '\0';
                        const CBMRegisteredFunc *m = cbm_registry_lookup_method_by_types(
                            ctx->registry, class_qn, dot + 1, arg_types, arg_count);
                        // Try with module prefix
                        if (!m && ctx->module_qn) {
                            const char *mod_class =
                                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, class_qn);
                            m = cbm_registry_lookup_method_by_types(ctx->registry, mod_class,
                                                                    dot + 1, arg_types, arg_count);
                        }
                        if (m) {
                            c_emit_resolved_call(ctx, m->qualified_name, "lsp_scoped", 0.95f);
                            goto recurse;
                        }
                    }
                    // Namespace fallback: extract_defs may omit namespace from QN.
                    // "utils.create_logger" → try "create_logger" and "mod.create_logger"
                    if (dot && ctx->module_qn) {
                        const char *bare_name = dot + 1;
                        const CBMRegisteredFunc *nf =
                            cbm_registry_lookup_func(ctx->registry, bare_name);
                        if (!nf) {
                            nf = cbm_registry_lookup_func(
                                ctx->registry,
                                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, bare_name));
                        }
                        if (nf) {
                            c_emit_resolved_call(ctx, nf->qualified_name, "lsp_scoped", 0.90f);
                            goto recurse;
                        }
                    }
                    c_emit_unresolved_call(ctx, qn, "scoped_not_in_registry");
                }
                goto recurse;
            }

            // template_function: func<T>(args) or ns::func<T>(args)
            if (strcmp(fk, "template_function") == 0) {
                TSNode name_node = ts_node_child_by_field_name(func_node, "name", 4);
                if (!ts_node_is_null(name_node)) {
                    char *name = c_node_text(ctx, name_node);
                    if (name) {
                        const char *nk = ts_node_type(name_node);
                        const CBMRegisteredFunc *f = NULL;
                        if (strcmp(nk, "qualified_identifier") == 0 ||
                            strcmp(nk, "scoped_identifier") == 0) {
                            const char *qn = c_build_qn(ctx, name);
                            f = cbm_registry_lookup_func(ctx->registry, qn);
                            if (!f && ctx->module_qn) {
                                const char *mod_qn =
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, qn);
                                f = cbm_registry_lookup_func(ctx->registry, mod_qn);
                            }
                            // Try as Class::method
                            if (!f) {
                                const char *dot = strrchr(qn, '.');
                                if (dot) {
                                    size_t plen = dot - qn;
                                    char *cls = (char *)cbm_arena_alloc(ctx->arena, plen + 1);
                                    memcpy(cls, qn, plen);
                                    cls[plen] = '\0';
                                    const CBMRegisteredFunc *m =
                                        cbm_registry_lookup_method(ctx->registry, cls, dot + 1);
                                    if (!m && ctx->module_qn) {
                                        const char *mc = cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                                           ctx->module_qn, cls);
                                        m = cbm_registry_lookup_method(ctx->registry, mc, dot + 1);
                                    }
                                    if (m)
                                        f = m;
                                }
                            }
                        } else {
                            const char *fqn = c_resolve_name(ctx, name);
                            if (fqn)
                                f = cbm_registry_lookup_func(ctx->registry, fqn);
                        }
                        if (f) {
                            c_emit_resolved_call(ctx, f->qualified_name, "lsp_template", 0.95f);
                            // Resolve pending template calls at this call site
                            if (ctx->pending_tc_count > 0 && f->type_param_names) {
                                int ac = 0;
                                const CBMType **at = c_extract_call_arg_types(ctx, node, &ac);
                                if (at)
                                    c_resolve_pending_template_calls(ctx, f, at, ac);
                            }
                        } else {
                            c_emit_unresolved_call(ctx, c_build_qn(ctx, name),
                                                   "template_not_in_registry");
                        }
                    }
                }
                goto recurse;
            }

            // Direct identifier call: func()
            if (strcmp(fk, "identifier") == 0) {
                char *name = c_node_text(ctx, func_node);
                if (name && !is_c_builtin_func(name)) {
                    // Check function pointer target map FIRST (before scope type check)
                    // because C function pointer declarators like int (*fp)(int)
                    // parse as pointer(int) in scope, not as FUNC type.
                    const char *fp_target = c_lookup_fp_target(ctx, name);
                    if (fp_target) {
                        // Distinguish DLL/dynamic resolution from static fp targets
                        bool is_dll = (strncmp(fp_target, "external.", 9) == 0);
                        c_emit_resolved_call(ctx, fp_target,
                                             is_dll ? "lsp_dll_resolve" : "lsp_func_ptr",
                                             is_dll ? 0.80f : 0.85f);
                        goto recurse;
                    }

                    // Check if it's a variable with callable type
                    const CBMType *var_type = cbm_scope_lookup(ctx->current_scope, name);
                    if (!cbm_type_is_unknown(var_type)) {
                        if (var_type->kind == CBM_TYPE_FUNC) {
                            // Function type variable without tracked target — can't resolve
                            goto recurse;
                        }
                        // Functor call: variable with operator()
                        const CBMType *base = c_simplify_type(ctx, var_type, false);
                        const char *type_qn = type_to_qn(base);
                        if (type_qn) {
                            const CBMRegisteredFunc *op =
                                c_lookup_member(ctx, type_qn, "operator()");
                            if (op) {
                                c_emit_resolved_call(ctx, op->qualified_name, "lsp_operator",
                                                     0.90f);
                                goto recurse;
                            }
                        }
                    }

                    // Check if it's a type name (constructor call)
                    const CBMType *type = c_resolve_name_to_type(ctx, name);
                    if (type && type->kind == CBM_TYPE_NAMED) {
                        const char *type_qn_str = type->data.named.qualified_name;
                        const CBMRegisteredType *rt =
                            cbm_registry_lookup_type(ctx->registry, type_qn_str);
                        if (rt && !rt->is_interface) {
                            // Constructor call
                            const char *ctor_qn =
                                cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn_str, name);
                            c_emit_resolved_call(ctx, ctor_qn, "lsp_constructor", 0.95f);
                            goto recurse;
                        }
                    }

                    // Regular function call
                    const char *fqn = c_resolve_name(ctx, name);
                    if (fqn) {
                        // Check if this is implicit 'this' call
                        const char *strategy = "lsp_direct";
                        if (ctx->enclosing_class_qn) {
                            const CBMRegisteredFunc *m = cbm_registry_lookup_method(
                                ctx->registry, ctx->enclosing_class_qn, name);
                            if (m && strcmp(fqn, m->qualified_name) == 0) {
                                strategy = "lsp_implicit_this";
                            }
                        }
                        c_emit_resolved_call(ctx, fqn, strategy, 0.95f);
                        // Resolve pending template calls at this call site
                        if (ctx->pending_tc_count > 0) {
                            const CBMRegisteredFunc *called =
                                cbm_registry_lookup_func(ctx->registry, fqn);
                            if (called && called->type_param_names) {
                                int ac = 0;
                                const CBMType **at = c_extract_call_arg_types(ctx, node, &ac);
                                if (at)
                                    c_resolve_pending_template_calls(ctx, called, at, ac);
                            }
                        }
                    } else {
                        // ADL: search namespaces of argument types
                        const char *adl_qn = c_adl_resolve(ctx, name, node);
                        if (adl_qn) {
                            c_emit_resolved_call(ctx, adl_qn, "lsp_adl", 0.90f);
                        } else {
                            c_emit_unresolved_call(ctx, name, "function_not_in_registry");
                        }
                    }
                }
                goto recurse;
            }
        }
    }

    // --- Constructor calls from declarations ---
    if (strcmp(kind, "declaration") == 0 && ctx->cpp_mode) {
        // Check for Foo x(args) or Foo x{args} patterns
        uint32_t nc = ts_node_named_child_count(node);
        const CBMType *decl_type = cbm_type_unknown();
        bool has_type = false;
        for (uint32_t i = 0; i < nc; i++) {
            TSNode child = ts_node_named_child(node, i);
            const char *ck = ts_node_type(child);
            if (!has_type &&
                (strcmp(ck, "type_identifier") == 0 || strcmp(ck, "scoped_type_identifier") == 0 ||
                 strcmp(ck, "template_type") == 0)) {
                decl_type = c_parse_type_node(ctx, child);
                has_type = true;
            }
            if (has_type && strcmp(ck, "init_declarator") == 0) {
                // Check if value is argument_list (constructor with parens)
                TSNode value = ts_node_child_by_field_name(child, "value", 5);
                if (!ts_node_is_null(value)) {
                    const char *vk = ts_node_type(value);
                    if (strcmp(vk, "argument_list") == 0 || strcmp(vk, "initializer_list") == 0) {
                        const char *type_qn = type_to_qn(decl_type);
                        if (type_qn) {
                            const char *short_name = strrchr(type_qn, '.');
                            short_name = short_name ? short_name + 1 : type_qn;
                            const char *last_colon = strrchr(type_qn, ':');
                            if (last_colon)
                                short_name = last_colon + 1;
                            const char *ctor_qn =
                                cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, short_name);
                            c_emit_resolved_call(ctx, ctor_qn, "lsp_constructor", 0.90f);
                        }
                    }
                }
            }
        }
    }

    // --- new_expression: emit constructor ---
    if (strcmp(kind, "new_expression") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (ts_node_is_null(type_node)) {
            uint32_t nc = ts_node_named_child_count(node);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode child = ts_node_named_child(node, i);
                const char *ck = ts_node_type(child);
                if (strcmp(ck, "type_identifier") == 0 || strcmp(ck, "template_type") == 0 ||
                    strcmp(ck, "scoped_type_identifier") == 0) {
                    type_node = child;
                    break;
                }
            }
        }
        if (!ts_node_is_null(type_node)) {
            const CBMType *t = c_parse_type_node(ctx, type_node);
            const char *type_qn = type_to_qn(t);
            if (type_qn) {
                const char *short_name = strrchr(type_qn, '.');
                short_name = short_name ? short_name + 1 : type_qn;
                const char *ctor_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, short_name);
                c_emit_resolved_call(ctx, ctor_qn, "lsp_constructor", 0.95f);
            }
        }
    }

    // --- delete_expression: emit destructor ---
    if (strcmp(kind, "delete_expression") == 0 && ctx->cpp_mode) {
        TSNode operand = ts_node_child_by_field_name(node, "argument", 8);
        if (ts_node_is_null(operand)) {
            if (ts_node_named_child_count(node) > 0)
                operand = ts_node_named_child(node, 0);
        }
        if (!ts_node_is_null(operand)) {
            const CBMType *ptr_type = c_eval_expr_type(ctx, operand);
            const CBMType *base = c_simplify_type(ctx, ptr_type, true);
            const char *type_qn = type_to_qn(base);
            if (type_qn) {
                const char *short_name = strrchr(type_qn, '.');
                short_name = short_name ? short_name + 1 : type_qn;
                const char *dtor_qn = cbm_arena_sprintf(ctx->arena, "%s.~%s", type_qn, short_name);
                c_emit_resolved_call(ctx, dtor_qn, "lsp_destructor", 0.90f);
            }
        }
    }

    // --- Operator calls (C++ only) ---
    if (ctx->cpp_mode && strcmp(kind, "binary_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        if (!ts_node_is_null(left)) {
            const CBMType *lhs_type = c_eval_expr_type(ctx, left);
            const CBMType *base = c_simplify_type(ctx, lhs_type, false);
            if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base)) {
                const char *type_qn = type_to_qn(base);
                if (type_qn) {
                    // Extract operator
                    for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
                        TSNode child = ts_node_child(node, i);
                        if (!ts_node_is_named(child)) {
                            char *op = c_node_text(ctx, child);
                            if (op) {
                                const char *op_name =
                                    cbm_arena_sprintf(ctx->arena, "operator%s", op);
                                const CBMRegisteredFunc *m = c_lookup_member(ctx, type_qn, op_name);
                                if (m) {
                                    c_emit_resolved_call(ctx, m->qualified_name, "lsp_operator",
                                                         0.90f);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    // --- Compound assignment operator calls (C++ only): a += b, a -= b, etc. ---
    if (ctx->cpp_mode && strcmp(kind, "assignment_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        if (!ts_node_is_null(left)) {
            const CBMType *lhs_type = c_eval_expr_type(ctx, left);
            const CBMType *base = c_simplify_type(ctx, lhs_type, false);
            if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base)) {
                const char *type_qn = type_to_qn(base);
                if (type_qn) {
                    // Extract operator (+=, -=, *=, /=, etc.)
                    for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
                        TSNode child = ts_node_child(node, i);
                        if (!ts_node_is_named(child)) {
                            char *op = c_node_text(ctx, child);
                            if (op && strlen(op) >= 2 && op[strlen(op) - 1] == '=') {
                                // Compound assignment: +=, -=, *=, /=, %=, <<=, >>=, &=, |=, ^=
                                const char *op_name =
                                    cbm_arena_sprintf(ctx->arena, "operator%s", op);
                                const CBMRegisteredFunc *m = c_lookup_member(ctx, type_qn, op_name);
                                if (m) {
                                    c_emit_resolved_call(ctx, m->qualified_name, "lsp_operator",
                                                         0.90f);
                                }
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    // --- subscript_expression operator[] ---
    if (ctx->cpp_mode && strcmp(kind, "subscript_expression") == 0) {
        TSNode arg_node = ts_node_child_by_field_name(node, "argument", 8);
        if (!ts_node_is_null(arg_node)) {
            const CBMType *arr_type = c_eval_expr_type(ctx, arg_node);
            const CBMType *base = c_simplify_type(ctx, arr_type, false);
            if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base) &&
                base->kind != CBM_TYPE_POINTER && base->kind != CBM_TYPE_SLICE) {
                const char *type_qn = type_to_qn(base);
                if (type_qn) {
                    const CBMRegisteredFunc *m = c_lookup_member(ctx, type_qn, "operator[]");
                    if (m) {
                        c_emit_resolved_call(ctx, m->qualified_name, "lsp_operator", 0.90f);
                    }
                }
            }
        }
    }

    // --- unary_expression/pointer_expression: operator* / operator++ / operator-- on custom types
    // ---
    if (ctx->cpp_mode &&
        (strcmp(kind, "unary_expression") == 0 || strcmp(kind, "pointer_expression") == 0 ||
         strcmp(kind, "update_expression") == 0)) {
        TSNode operand = ts_node_child_by_field_name(node, "argument", 8);
        if (ts_node_is_null(operand))
            operand = ts_node_child_by_field_name(node, "operand", 7);
        if (!ts_node_is_null(operand)) {
            const CBMType *op_type = c_eval_expr_type(ctx, operand);
            const CBMType *base = c_simplify_type(ctx, op_type, false);
            if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base) &&
                base->kind != CBM_TYPE_POINTER) {
                const char *type_qn = type_to_qn(base);
                if (type_qn) {
                    // Determine which operator
                    const char *op_name = NULL;
                    for (uint32_t ui = 0; ui < ts_node_child_count(node); ui++) {
                        TSNode ch = ts_node_child(node, ui);
                        if (!ts_node_is_named(ch)) {
                            char *op = c_node_text(ctx, ch);
                            if (!op)
                                continue;
                            if (strcmp(op, "*") == 0)
                                op_name = "operator*";
                            else if (strcmp(op, "++") == 0)
                                op_name = "operator++";
                            else if (strcmp(op, "--") == 0)
                                op_name = "operator--";
                            else if (strcmp(op, "!") == 0)
                                op_name = "operator!";
                            break;
                        }
                    }
                    if (op_name) {
                        const CBMRegisteredFunc *m = c_lookup_member(ctx, type_qn, op_name);
                        if (m) {
                            c_emit_resolved_call(ctx, m->qualified_name, "lsp_operator", 0.90f);
                        }
                    }
                }
            }
        }
    }

    // --- Copy/move constructor: Foo a = expr; where expr has Foo type ---
    if (ctx->cpp_mode && strcmp(kind, "declaration") == 0) {
        const CBMType *decl_type = c_parse_declaration_type(ctx, node);
        if (decl_type && decl_type->kind == CBM_TYPE_NAMED) {
            uint32_t dnc = ts_node_named_child_count(node);
            for (uint32_t di = 0; di < dnc; di++) {
                TSNode dchild = ts_node_named_child(node, di);
                if (strcmp(ts_node_type(dchild), "init_declarator") == 0) {
                    TSNode val = ts_node_child_by_field_name(dchild, "value", 5);
                    if (!ts_node_is_null(val)) {
                        const char *vk = ts_node_type(val);
                        // Skip if it's argument_list (already handled as constructor above)
                        // or initializer_list (also handled)
                        if (strcmp(vk, "argument_list") != 0 &&
                            strcmp(vk, "initializer_list") != 0) {
                            const CBMType *val_type = c_eval_expr_type(ctx, val);
                            const CBMType *val_base = c_simplify_type(ctx, val_type, false);
                            // If assigning an object of the same type -> copy/move constructor
                            const char *type_qn = type_to_qn(decl_type);
                            const char *val_qn = type_to_qn(val_base);
                            if (type_qn && val_qn && strcmp(type_qn, val_qn) == 0) {
                                const char *short_name = strrchr(type_qn, '.');
                                short_name = short_name ? short_name + 1 : type_qn;
                                const char *ctor_qn =
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, short_name);
                                c_emit_resolved_call(ctx, ctor_qn, "lsp_copy_constructor", 0.85f);
                            }
                        }
                    }
                }
            }
        }
    }

    // --- Conversion operator: implicit bool/int conversion in conditions ---
    if (ctx->cpp_mode &&
        (strcmp(kind, "if_statement") == 0 || strcmp(kind, "while_statement") == 0 ||
         strcmp(kind, "do_statement") == 0)) {
        TSNode cond = ts_node_child_by_field_name(node, "condition", 9);
        if (!ts_node_is_null(cond)) {
            // If condition is a single expression of a custom type with operator bool
            const CBMType *cond_type = c_eval_expr_type(ctx, cond);
            const CBMType *base = c_simplify_type(ctx, cond_type, false);
            if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base) &&
                base->kind != CBM_TYPE_POINTER) {
                const char *type_qn = type_to_qn(base);
                if (type_qn) {
                    const CBMRegisteredFunc *m = c_lookup_member(ctx, type_qn, "operator bool");
                    if (m) {
                        c_emit_resolved_call(ctx, m->qualified_name, "lsp_conversion", 0.85f);
                    }
                }
            }
        }
    }

    // --- throw_statement: throw MyError("msg") → emit constructor call ---
    if (strcmp(kind, "throw_statement") == 0) {
        uint32_t tnc = ts_node_named_child_count(node);
        if (tnc > 0) {
            TSNode thrown = ts_node_named_child(node, 0);
            if (!ts_node_is_null(thrown)) {
                // If the thrown expression is a call_expression (constructor),
                // it will be resolved by the normal call_expression handler during recursion.
                // Just let the recursion handle it — no special emission needed.
            }
        }
        // throw; (rethrow) — no calls, just skip
    }

    // --- co_yield_statement: co_yield expr → resolve inner expression calls ---
    if (strcmp(kind, "co_yield_statement") == 0) {
        // co_yield's operand is processed by normal recursion into children
    }

    // --- co_return_statement: co_return expr → resolve inner expression calls ---
    if (strcmp(kind, "co_return_statement") == 0) {
        // co_return's operand is processed by normal recursion into children
    }

    // --- Designated initializer: {.field = value} ---
    if (strcmp(kind, "initializer_pair") == 0 || strcmp(kind, "field_designator") == 0) {
        // These appear inside initializer_list for aggregate init
        // We don't emit CALLS for field access, but we could track which struct is being
        // initialized This is primarily for completeness — no CALLS edge needed for field
        // designators
    }

recurse:;
    // Push scope for blocks and control structures
    bool push_scope =
        (strcmp(kind, "compound_statement") == 0 || strcmp(kind, "if_statement") == 0 ||
         strcmp(kind, "for_statement") == 0 || strcmp(kind, "for_range_loop") == 0 ||
         strcmp(kind, "while_statement") == 0 || strcmp(kind, "do_statement") == 0 ||
         strcmp(kind, "switch_statement") == 0 || strcmp(kind, "catch_clause") == 0 ||
         strcmp(kind, "lambda_expression") == 0);

    if (push_scope) {
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    }

    // Process catch clause parameter
    if (strcmp(kind, "catch_clause") == 0) {
        TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
        if (!ts_node_is_null(params)) {
            uint32_t pnc = ts_node_named_child_count(params);
            for (uint32_t pi = 0; pi < pnc; pi++) {
                c_process_statement(ctx, ts_node_named_child(params, pi));
            }
        }
    }

    // Process lambda init-captures: [captured = expr] binds 'captured' in lambda scope
    if (strcmp(kind, "lambda_expression") == 0) {
        uint32_t lnc = ts_node_named_child_count(node);
        for (uint32_t li = 0; li < lnc; li++) {
            TSNode child = ts_node_named_child(node, li);
            if (ts_node_is_null(child))
                continue;
            if (strcmp(ts_node_type(child), "lambda_capture_specifier") == 0) {
                uint32_t cnc = ts_node_named_child_count(child);
                for (uint32_t ci = 0; ci < cnc; ci++) {
                    TSNode cap = ts_node_named_child(child, ci);
                    if (ts_node_is_null(cap))
                        continue;
                    const char *ck = ts_node_type(cap);
                    if (strcmp(ck, "lambda_capture_initializer") == 0) {
                        // Init-capture: name = value
                        // Try field-based access first (name field = 4 chars)
                        TSNode name_node = ts_node_child_by_field_name(cap, "name", 4);
                        TSNode val_node = ts_node_child_by_field_name(cap, "value", 5);
                        // Fallback: positional — first named child is name, second is value
                        if (ts_node_is_null(name_node) && ts_node_named_child_count(cap) >= 2) {
                            name_node = ts_node_named_child(cap, 0);
                            val_node = ts_node_named_child(cap, 1);
                        }
                        if (!ts_node_is_null(name_node) && !ts_node_is_null(val_node)) {
                            char *cap_name = c_node_text(ctx, name_node);
                            const CBMType *cap_type = c_eval_expr_type(ctx, val_node);
                            if (cap_name && !cbm_type_is_unknown(cap_type)) {
                                cbm_scope_bind(ctx->current_scope, cap_name, cap_type);
                            }
                        }
                    }
                }
                break; // only one capture specifier per lambda
            }
        }
    }

    // Recurse into children via a cursor (O(n)); ts_node_child(node,i) is O(i)
    // in tree-sitter → O(n²) on a wide node.
    {
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                c_resolve_calls_in_node(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }

    if (push_scope) {
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
    }
}

// ============================================================================
// Process function: set enclosing QN, bind params, walk body
// ============================================================================

static void c_process_function(CLSPContext *ctx, TSNode func_node) {
    TSNode decl = ts_node_child_by_field_name(func_node, "declarator", 10);
    if (ts_node_is_null(decl))
        return;

    // Extract function name from declarator chain
    char *func_name = NULL;
    const char *saved_class_qn = ctx->enclosing_class_qn;
    TSNode params_node = (TSNode){0};

    // Navigate declarator to find name and parameters
    TSNode cur = decl;
    for (int depth = 0; depth < 10 && !ts_node_is_null(cur); depth++) {
        const char *dk = ts_node_type(cur);

        if (strcmp(dk, "function_declarator") == 0) {
            TSNode fdecl = ts_node_child_by_field_name(cur, "declarator", 10);
            params_node = ts_node_child_by_field_name(cur, "parameters", 10);
            cur = fdecl;
            continue;
        }
        if (strcmp(dk, "pointer_declarator") == 0 || strcmp(dk, "reference_declarator") == 0) {
            if (ts_node_named_child_count(cur) > 0)
                cur = ts_node_named_child(cur, ts_node_named_child_count(cur) - 1);
            else
                break;
            continue;
        }
        if (strcmp(dk, "qualified_identifier") == 0 || strcmp(dk, "scoped_identifier") == 0) {
            func_name = c_node_text(ctx, cur);
            // Check if this is a method (has Class:: prefix)
            TSNode scope_node = ts_node_child_by_field_name(cur, "scope", 5);
            if (!ts_node_is_null(scope_node)) {
                char *scope_text = c_node_text(ctx, scope_node);
                if (scope_text) {
                    const char *scope_qn = c_build_qn(ctx, scope_text);
                    // Try as a type for enclosing class
                    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, scope_qn);
                    if (rt) {
                        ctx->enclosing_class_qn = scope_qn;
                    } else if (ctx->module_qn) {
                        const char *fqn =
                            cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, scope_qn);
                        rt = cbm_registry_lookup_type(ctx->registry, fqn);
                        if (rt)
                            ctx->enclosing_class_qn = fqn;
                    }
                }
            }
            break;
        }
        if (strcmp(dk, "identifier") == 0) {
            func_name = c_node_text(ctx, cur);
            break;
        }
        if (strcmp(dk, "field_identifier") == 0) {
            func_name = c_node_text(ctx, cur);
            break;
        }
        // Destructor
        if (strcmp(dk, "destructor_name") == 0) {
            func_name = c_node_text(ctx, cur);
            break;
        }
        break;
    }

    if (!func_name || !func_name[0])
        return;

    // Build enclosing function QN
    const char *func_qn = c_build_qn(ctx, func_name);
    if (ctx->module_qn && !strchr(func_qn, '.')) {
        func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, func_qn);
    }
    ctx->enclosing_func_qn = func_qn;

    // If inside a template, attach type_param_names to the registered function
    // so pending template calls can be resolved at call sites. NEVER do this
    // against the shared Tier-2 cross registry: resolve workers walk files
    // concurrently, so the write races other readers AND stores a pointer to
    // THIS worker's per-file arena into shared state — once that arena is
    // recycled the registry holds dangling memory (intermittent SIGSEGV
    // indexing bitcoin). Cross-phase template deduction then relies on the
    // positional fallback, which is graceful degradation.
    if (ctx->in_template && ctx->template_param_count > 0 && !ctx->registry_shared) {
        // Find the registered function and set type_param_names
        for (int ri = 0; ri < ((CBMTypeRegistry *)ctx->registry)->func_count; ri++) {
            CBMRegisteredFunc *rf = &((CBMTypeRegistry *)ctx->registry)->funcs[ri];
            if (strcmp(rf->qualified_name, func_qn) == 0 && !rf->type_param_names) {
                const char **tpn = (const char **)cbm_arena_alloc(
                    ctx->arena, (ctx->template_param_count + 1) * sizeof(const char *));
                for (int ti = 0; ti < ctx->template_param_count; ti++)
                    tpn[ti] = ctx->template_param_names[ti];
                tpn[ctx->template_param_count] = NULL;
                rf->type_param_names = tpn;
                break;
            }
        }
    }

    // Push function scope
    CBMScope *saved_scope = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

    // Bind 'this' if in a method
    if (ctx->enclosing_class_qn) {
        cbm_scope_bind(
            ctx->current_scope, "this",
            cbm_type_pointer(ctx->arena, cbm_type_named(ctx->arena, ctx->enclosing_class_qn)));
    }

    // Bind parameters and count defaults for min_params
    int total_params = 0, defaulted_params = 0;
    if (!ts_node_is_null(params_node)) {
        uint32_t pnc = ts_node_named_child_count(params_node);
        for (uint32_t i = 0; i < pnc; i++) {
            TSNode param = ts_node_named_child(params_node, i);
            if (!ts_node_is_null(param)) {
                const char *pk = ts_node_type(param);
                if (strcmp(pk, "parameter_declaration") == 0 ||
                    strcmp(pk, "optional_parameter_declaration") == 0) {
                    total_params++;
                    TSNode dv = ts_node_child_by_field_name(param, "default_value", 13);
                    if (!ts_node_is_null(dv))
                        defaulted_params++;
                }
                c_process_statement(ctx, param);
            }
        }
    }
    // Set min_params on the registered function (for default-arg overload matching)
    if (total_params > 0 && defaulted_params > 0) {
        for (int ri = 0; ri < ((CBMTypeRegistry *)ctx->registry)->func_count; ri++) {
            CBMRegisteredFunc *rf = &((CBMTypeRegistry *)ctx->registry)->funcs[ri];
            if (strcmp(rf->qualified_name, func_qn) == 0 && rf->min_params < 0) {
                rf->min_params = total_params - defaulted_params;
                break;
            }
        }
    }

    // Walk function body
    TSNode body = ts_node_child_by_field_name(func_node, "body", 4);
    if (!ts_node_is_null(body)) {
        c_resolve_calls_in_node(ctx, body);
    }

    // Restore
    ctx->current_scope = saved_scope;
    ctx->enclosing_class_qn = saved_class_qn;
}

// ============================================================================
// Process namespace
// ============================================================================

// Process a top-level or nested declaration within a namespace/class body,
// handling template_declaration wrapping.
static void c_process_body_child(CLSPContext *ctx, TSNode child) {
    if (ts_node_is_null(child))
        return;
    const char *ck = ts_node_type(child);

    if (strcmp(ck, "function_definition") == 0) {
        c_process_function(ctx, child);
    } else if (strcmp(ck, "namespace_definition") == 0) {
        c_process_namespace(ctx, child);
    } else if (strcmp(ck, "class_specifier") == 0 || strcmp(ck, "struct_specifier") == 0) {
        c_process_class(ctx, child);
    } else if (strcmp(ck, "declaration") == 0) {
        // declaration may contain class/struct specifier: class Foo { ... };
        // Extract and process the class before resolving calls.
        bool has_class = false;
        uint32_t dnc = ts_node_named_child_count(child);
        for (uint32_t di = 0; di < dnc; di++) {
            TSNode dch = ts_node_named_child(child, di);
            const char *dk = ts_node_type(dch);
            if (strcmp(dk, "class_specifier") == 0 || strcmp(dk, "struct_specifier") == 0) {
                c_process_class(ctx, dch);
                has_class = true;
            }
            // Register bare function declarations: ReturnType func_name(params);
            // Unwrap reference_declarator/pointer_declarator to find function_declarator
            {
                TSNode func_decl_node = dch;
                bool is_ref_ret = false;
                bool is_ptr_ret = false;
                if (strcmp(dk, "reference_declarator") == 0 && ts_node_named_child_count(dch) > 0) {
                    TSNode inner = ts_node_named_child(dch, 0);
                    if (strcmp(ts_node_type(inner), "function_declarator") == 0) {
                        func_decl_node = inner;
                        is_ref_ret = true;
                    }
                } else if (strcmp(dk, "pointer_declarator") == 0 &&
                           ts_node_named_child_count(dch) > 0) {
                    TSNode inner = ts_node_named_child(dch, ts_node_named_child_count(dch) - 1);
                    if (strcmp(ts_node_type(inner), "function_declarator") == 0) {
                        func_decl_node = inner;
                        is_ptr_ret = true;
                    }
                }
                if (strcmp(ts_node_type(func_decl_node), "function_declarator") == 0) {
                    TSNode fn_name = ts_node_child_by_field_name(func_decl_node, "declarator", 10);
                    if (!ts_node_is_null(fn_name)) {
                        char *fname = c_node_text(ctx, fn_name);
                        if (fname && fname[0]) {
                            // Parse return type from declaration's type specifier
                            const CBMType *ret_type = NULL;
                            for (uint32_t ri = 0; ri < dnc; ri++) {
                                TSNode rch = ts_node_named_child(child, ri);
                                const char *rk = ts_node_type(rch);
                                if (strcmp(rk, "function_declarator") != 0 &&
                                    strcmp(rk, "identifier") != 0 &&
                                    strcmp(rk, "pointer_declarator") != 0 &&
                                    strcmp(rk, "reference_declarator") != 0) {
                                    ret_type = c_parse_type_node(ctx, rch);
                                    if (!cbm_type_is_unknown(ret_type))
                                        break;
                                }
                            }
                            if (ret_type && !cbm_type_is_unknown(ret_type)) {
                                if (is_ref_ret)
                                    ret_type = cbm_type_reference(ctx->arena, ret_type);
                                if (is_ptr_ret)
                                    ret_type = cbm_type_pointer(ctx->arena, ret_type);
                                const char *func_qn = c_build_qn(ctx, fname);
                                if (ctx->module_qn && !strchr(func_qn, '.'))
                                    func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn,
                                                                func_qn);
                                // Use namespace if available
                                if (ctx->current_namespace &&
                                    !strstr(func_qn, ctx->current_namespace))
                                    func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                                ctx->current_namespace, fname);
                                // Only register if not already registered
                                if (!cbm_registry_lookup_func(ctx->registry, func_qn)) {
                                    const CBMType **rets = (const CBMType **)cbm_arena_alloc(
                                        ctx->arena, 2 * sizeof(const CBMType *));
                                    rets[0] = ret_type;
                                    rets[1] = NULL;
                                    CBMRegisteredFunc rf = {0};
                                    rf.qualified_name = func_qn;
                                    rf.short_name = fname;
                                    rf.signature = cbm_type_func(ctx->arena, NULL, NULL, rets);
                                    // Copy template params if in template context
                                    if (ctx->in_template && ctx->template_param_count > 0) {
                                        const char **tpn = (const char **)cbm_arena_alloc(
                                            ctx->arena,
                                            (ctx->template_param_count + 1) * sizeof(char *));
                                        for (int tp = 0; tp < ctx->template_param_count; tp++)
                                            tpn[tp] = ctx->template_param_names[tp];
                                        tpn[ctx->template_param_count] = NULL;
                                        rf.type_param_names = tpn;
                                    }
                                    cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, rf);
                                }
                            }
                        }
                    }
                }
            }
        }
        if (!has_class) {
            c_resolve_calls_in_node(ctx, child);
        }
    } else if (strcmp(ck, "concept_definition") == 0) {
        // C++20 concept: concept Sortable = requires(T a) { a.sort(); };
        // No CALLS edges from concept definitions — they are constraints, not runtime calls.
        // Skip processing to avoid false positive CALLS from requires-expressions.
        return;
    } else if (strcmp(ck, "template_declaration") == 0) {
        // Save template state, parse params for defaults, set flag
        bool saved_template = ctx->in_template;
        const char **saved_tpn = ctx->template_param_names;
        const CBMType **saved_tpd = ctx->template_param_defaults;
        int saved_tpc = ctx->template_param_count;

        ctx->in_template = true;
        c_parse_template_params(ctx, child);

        uint32_t tnc = ts_node_named_child_count(child);
        for (uint32_t ti = 0; ti < tnc; ti++) {
            TSNode inner = ts_node_named_child(child, ti);
            c_process_body_child(ctx, inner);
        }

        ctx->in_template = saved_template;
        ctx->template_param_names = saved_tpn;
        ctx->template_param_defaults = saved_tpd;
        ctx->template_param_count = saved_tpc;
    } else {
        c_resolve_calls_in_node(ctx, child);
    }
}

static void c_process_namespace(CLSPContext *ctx, TSNode ns_node) {
    const char *saved_ns = ctx->current_namespace;

    TSNode name_node = ts_node_child_by_field_name(ns_node, "name", 4);
    if (!ts_node_is_null(name_node)) {
        char *ns_name = c_node_text(ctx, name_node);
        if (ns_name) {
            if (saved_ns) {
                ctx->current_namespace = cbm_arena_sprintf(ctx->arena, "%s.%s", saved_ns, ns_name);
            } else if (ctx->module_qn) {
                ctx->current_namespace =
                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, ns_name);
            } else {
                ctx->current_namespace = ns_name;
            }
        }
    }

    TSNode body = ts_node_child_by_field_name(ns_node, "body", 4);
    if (!ts_node_is_null(body)) {
        // Cursor walk (O(n)); namespace bodies can be very wide.
        TSTreeCursor cursor = ts_tree_cursor_new(body);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                c_process_body_child(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }

    ctx->current_namespace = saved_ns;
}

// ============================================================================
// Process class/struct: process method bodies
// ============================================================================

static void c_process_class(CLSPContext *ctx, TSNode class_node) {
    const char *saved_class = ctx->enclosing_class_qn;
    const char **saved_tpn = ctx->template_param_names;
    const CBMType **saved_tpd = ctx->template_param_defaults;
    int saved_tpc = ctx->template_param_count;

    char *class_name = NULL;
    TSNode name_node = ts_node_child_by_field_name(class_node, "name", 4);
    if (!ts_node_is_null(name_node)) {
        class_name = c_node_text(ctx, name_node);
        if (class_name) {
            const char *class_qn;
            if (ctx->current_namespace) {
                class_qn =
                    cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace, class_name);
            } else if (ctx->module_qn) {
                class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, class_name);
            } else {
                class_qn = class_name;
            }
            ctx->enclosing_class_qn = class_qn;

            // Store template param names on the registered type (for substitution)
            if (ctx->in_template && ctx->template_param_names && ctx->template_param_count > 0) {
                CBMRegisteredType *rt = NULL;
                for (int ri = 0; ri < ((CBMTypeRegistry *)ctx->registry)->type_count; ri++) {
                    if (strcmp(((CBMTypeRegistry *)ctx->registry)->types[ri].qualified_name,
                               class_qn) == 0) {
                        rt = &((CBMTypeRegistry *)ctx->registry)->types[ri];
                        break;
                    }
                }
                if (rt && !rt->type_param_names) {
                    const char **tpn = (const char **)cbm_arena_alloc(
                        ctx->arena, (ctx->template_param_count + 1) * sizeof(const char *));
                    for (int pi = 0; pi < ctx->template_param_count; pi++)
                        tpn[pi] = ctx->template_param_names[pi];
                    tpn[ctx->template_param_count] = NULL;
                    rt->type_param_names = tpn;
                }
            }
        }
    }

    // CRTP detection: class Derived : Base<Derived>
    // When a base class template argument matches the class name,
    // bind that template parameter to the derived class type.
    if (class_name && ctx->enclosing_class_qn) {
        // Walk base_class_clause children looking for template base classes
        uint32_t cnc = ts_node_named_child_count(class_node);
        for (uint32_t ci = 0; ci < cnc; ci++) {
            TSNode child = ts_node_named_child(class_node, ci);
            if (ts_node_is_null(child))
                continue;
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "base_class_clause") != 0)
                continue;
            // Iterate base specifiers
            uint32_t bnc = ts_node_named_child_count(child);
            for (uint32_t bi = 0; bi < bnc; bi++) {
                TSNode base_spec = ts_node_named_child(child, bi);
                if (ts_node_is_null(base_spec))
                    continue;
                // Look for template_type as base: Base<Derived>
                TSNode type_node = base_spec;
                if (strcmp(ts_node_type(base_spec), "access_specifier") == 0)
                    continue;
                // The base type might be directly a template_type or wrapped
                if (strcmp(ts_node_type(type_node), "template_type") != 0) {
                    // Try walking children for template_type
                    uint32_t snc = ts_node_named_child_count(type_node);
                    bool found = false;
                    for (uint32_t si = 0; si < snc; si++) {
                        TSNode sc = ts_node_named_child(type_node, si);
                        if (strcmp(ts_node_type(sc), "template_type") == 0) {
                            type_node = sc;
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                        continue;
                }
                // type_node is template_type: check arguments for CRTP
                TSNode targs = ts_node_child_by_field_name(type_node, "arguments", 9);
                if (ts_node_is_null(targs))
                    continue;
                // Check each template argument for CRTP pattern
                uint32_t tanc = ts_node_named_child_count(targs);
                for (uint32_t ti = 0; ti < tanc; ti++) {
                    TSNode ta = ts_node_named_child(targs, ti);
                    if (ts_node_is_null(ta))
                        continue;
                    TSNode ta_inner = ta;
                    // type_descriptor wraps the actual type
                    if (strcmp(ts_node_type(ta), "type_descriptor") == 0 &&
                        ts_node_named_child_count(ta) > 0) {
                        ta_inner = ts_node_named_child(ta, 0);
                    }
                    char *arg_text = c_node_text(ctx, ta_inner);
                    if (arg_text && strcmp(arg_text, class_name) == 0) {
                        // CRTP detected! Map template param at position ti to derived class
                        // For now, bind using positional names (T0, T1, ...)
                        // Also check existing template params
                        if (ctx->template_param_names) {
                            for (int pi = 0; pi < ctx->template_param_count; pi++) {
                                if (ctx->template_param_names[pi] && ctx->template_param_defaults &&
                                    !ctx->template_param_defaults[pi]) {
                                    // Bind unbound template param to derived class
                                    // Only bind the first unbound one at position ti
                                    if (pi == (int)ti) {
                                        ctx->template_param_defaults[pi] =
                                            cbm_type_named(ctx->arena, ctx->enclosing_class_qn);
                                        break;
                                    }
                                }
                            }
                        }
                        break; // found CRTP for this base
                    }
                }
            }
            break; // only one base_class_clause
        }
    }

    TSNode body = ts_node_child_by_field_name(class_node, "body", 4);
    if (!ts_node_is_null(body)) {
        // Pre-pass: register method declarations (no body) as methods in registry.
        // This allows template return type substitution for methods like T& value();
        if (ctx->enclosing_class_qn) {
            uint32_t bkn = 0;
            TSNode *bkids = cbm_lsp_collect_children(ctx->arena, body, &bkn);
            for (uint32_t i = 0; i < bkn; i++) {
                TSNode child = bkids[i];
                if (!ts_node_is_named(child))
                    continue;
                const char *ck = ts_node_type(child);
                // field_declaration, declaration, or function_definition = method
                if (strcmp(ck, "field_declaration") != 0 && strcmp(ck, "declaration") != 0 &&
                    strcmp(ck, "function_definition") != 0)
                    continue;
                // Parse return type from type specifier
                const CBMType *ret_type = c_parse_declaration_type(ctx, child);
                // Walk declarator chain to find function_declarator + method name
                uint32_t dnc = ts_node_named_child_count(child);
                for (uint32_t di = 0; di < dnc; di++) {
                    TSNode decl = ts_node_named_child(child, di);
                    // Navigate through reference_declarator, pointer_declarator to
                    // function_declarator
                    TSNode cur = decl;
                    const CBMType *actual_ret = ret_type;
                    for (int depth = 0; depth < 5 && !ts_node_is_null(cur); depth++) {
                        const char *dk = ts_node_type(cur);
                        if (strcmp(dk, "function_declarator") == 0) {
                            // Found method declaration — extract name
                            TSNode name_node = ts_node_child_by_field_name(cur, "declarator", 10);
                            if (ts_node_is_null(name_node))
                                break;
                            char *method_name = NULL;
                            const char *nk = ts_node_type(name_node);
                            if (strcmp(nk, "field_identifier") == 0 ||
                                strcmp(nk, "identifier") == 0) {
                                method_name = c_node_text(ctx, name_node);
                            } else if (strcmp(nk, "destructor_name") == 0 ||
                                       strcmp(nk, "operator_name") == 0) {
                                method_name = c_node_text(ctx, name_node);
                            }
                            if (!method_name || !method_name[0])
                                break;
                            const char *method_qn = cbm_arena_sprintf(
                                ctx->arena, "%s.%s", ctx->enclosing_class_qn, method_name);
                            // Check if already registered — if so, upgrade return type
                            // if pre-pass has a more specific type (e.g., TEMPLATE vs NAMED)
                            // Check both full QN and short QN (extract_defs may use shorter)
                            const CBMRegisteredFunc *existing = cbm_registry_lookup_method(
                                ctx->registry, ctx->enclosing_class_qn, method_name);
                            if (!existing && ctx->module_qn) {
                                // Try without module prefix: "test.main.std.X" -> "std.X"
                                const char *class_qn = ctx->enclosing_class_qn;
                                size_t mod_len = strlen(ctx->module_qn);
                                if (strncmp(class_qn, ctx->module_qn, mod_len) == 0 &&
                                    class_qn[mod_len] == '.') {
                                    existing = cbm_registry_lookup_method(
                                        ctx->registry, class_qn + mod_len + 1, method_name);
                                }
                            }
                            if (existing) {
                                // Upgrade: if existing return type is NAMED but we have
                                // TEMPLATE/TYPE_PARAM/POINTER(TYPE_PARAM), update in-place
                                bool should_upgrade = false;
                                if (existing->signature &&
                                    existing->signature->kind == CBM_TYPE_FUNC &&
                                    existing->signature->data.func.return_types &&
                                    existing->signature->data.func.return_types[0]) {
                                    int ek = existing->signature->data.func.return_types[0]->kind;
                                    int nk2 = actual_ret ? actual_ret->kind : CBM_TYPE_UNKNOWN;
                                    // Upgrade if existing is NAMED and new is more specific
                                    if (ek == CBM_TYPE_NAMED &&
                                        (nk2 == CBM_TYPE_TEMPLATE || nk2 == CBM_TYPE_TYPE_PARAM ||
                                         nk2 == CBM_TYPE_POINTER || nk2 == CBM_TYPE_REFERENCE))
                                        should_upgrade = true;
                                }
                                if (should_upgrade) {
                                    // Update existing entry's signature return type
                                    const CBMType **new_rets = (const CBMType **)cbm_arena_alloc(
                                        ctx->arena, 2 * sizeof(const CBMType *));
                                    new_rets[0] = actual_ret;
                                    new_rets[1] = NULL;
                                    CBMRegisteredFunc *mut = (CBMRegisteredFunc *)existing;
                                    mut->signature =
                                        cbm_type_func(ctx->arena, NULL, NULL, new_rets);
                                }
                                break;
                            }
                            // Build return type with ref/ptr wrapping
                            const CBMType **rets = (const CBMType **)cbm_arena_alloc(
                                ctx->arena, 2 * sizeof(const CBMType *));
                            rets[0] = actual_ret;
                            rets[1] = NULL;
                            CBMRegisteredFunc rf;
                            memset(&rf, 0, sizeof(rf));
                            rf.qualified_name = method_qn;
                            rf.short_name = method_name;
                            rf.receiver_type = ctx->enclosing_class_qn;
                            rf.signature = cbm_type_func(ctx->arena, NULL, NULL, rets);
                            rf.min_params = -1;
                            cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, rf);
                            break;
                        } else if (strcmp(dk, "reference_declarator") == 0) {
                            actual_ret = cbm_type_reference(ctx->arena, actual_ret);
                            cur = ts_node_named_child_count(cur) > 0 ? ts_node_named_child(cur, 0)
                                                                     : (TSNode){0};
                        } else if (strcmp(dk, "pointer_declarator") == 0) {
                            actual_ret = cbm_type_pointer(ctx->arena, actual_ret);
                            cur = ts_node_named_child_count(cur) > 0 ? ts_node_named_child(cur, 0)
                                                                     : (TSNode){0};
                        } else {
                            break;
                        }
                    }
                }
            }
        }

        uint32_t bkn = 0;
        TSNode *bkids = cbm_lsp_collect_children(ctx->arena, body, &bkn);
        for (uint32_t i = 0; i < bkn; i++) {
            TSNode child = bkids[i];
            if (!ts_node_is_named(child))
                continue;
            c_process_body_child(ctx, child);
        }
    }

    ctx->enclosing_class_qn = saved_class;
    ctx->template_param_names = saved_tpn;
    ctx->template_param_defaults = saved_tpd;
    ctx->template_param_count = saved_tpc;
}

// Need forward declaration since c_process_namespace and c_process_class reference each other
// Already defined above via forward declaration pattern — both reference c_resolve_calls_in_node

// ============================================================================
// Process file: top-level walk
// ============================================================================

__attribute__((no_sanitize("address"))) void c_lsp_process_file(CLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;

    // Collect top-level children once (O(n)); both passes reuse the array.
    // Indexing ts_node_child(root,i) per iteration is O(i) → O(n²) on a wide root.
    uint32_t kn = 0;
    TSNode *kids = cbm_lsp_collect_children(ctx->arena, root, &kn);
    TSNode child; // Hoisted: prevents ASan stack-use-after-scope between passes
    TSNode inner;

    // Pass 1: process using declarations and global variables
    for (uint32_t i = 0; i < kn; i++) {
        child = kids[i];
        const char *ck = ts_node_type(child);

        if (strcmp(ck, "using_declaration") == 0 || strcmp(ck, "alias_declaration") == 0 ||
            strcmp(ck, "type_definition") == 0 || strcmp(ck, "namespace_alias_definition") == 0) {
            c_process_statement(ctx, child);
        } else if (strcmp(ck, "declaration") == 0) {
            c_process_statement(ctx, child);
        } else if (strcmp(ck, "template_declaration") == 0) {
            // template<class T> using Vec = std::vector<T>;
            // Unwrap template_declaration to process inner alias/typedef
            uint32_t tnc = ts_node_named_child_count(child);
            for (uint32_t ti = 0; ti < tnc; ti++) {
                inner = ts_node_named_child(child, ti);
                const char *ik = ts_node_type(inner);
                if (strcmp(ik, "alias_declaration") == 0 || strcmp(ik, "type_definition") == 0) {
                    c_process_statement(ctx, inner);
                }
            }
        }
    }

    // Pass 2: process functions, namespaces, classes, templates
    for (uint32_t i = 0; i < kn; i++) {
        child = kids[i];
        c_process_body_child(ctx, child);
    }
}

// ============================================================================
// Parse C/C++ return type text into CBMType (for cross-file defs)
// ============================================================================

static const CBMType *c_parse_return_type_text(CBMArena *a, const char *text,
                                               const char *module_qn) {
    if (!text || !text[0])
        return cbm_type_unknown();

    // Skip const/volatile qualifiers
    while (strncmp(text, "const ", 6) == 0)
        text += 6;
    while (strncmp(text, "volatile ", 9) == 0)
        text += 9;

    // Pointer
    size_t len = strlen(text);
    if (len > 0 && text[len - 1] == '*') {
        char *inner = cbm_arena_strndup(a, text, len - 1);
        // Trim trailing space
        size_t ilen = strlen(inner);
        while (ilen > 0 && inner[ilen - 1] == ' ')
            inner[--ilen] = '\0';
        return cbm_type_pointer(a, c_parse_return_type_text(a, inner, module_qn));
    }

    // Reference
    if (len > 0 && text[len - 1] == '&') {
        char *inner = cbm_arena_strndup(a, text, len - 1);
        size_t ilen = strlen(inner);
        while (ilen > 0 && inner[ilen - 1] == ' ')
            inner[--ilen] = '\0';
        // Check for rvalue ref
        if (ilen > 0 && inner[ilen - 1] == '&') {
            inner[ilen - 1] = '\0';
            while (ilen > 1 && inner[ilen - 2] == ' ')
                inner[--ilen - 1] = '\0';
            return cbm_type_rvalue_ref(a, c_parse_return_type_text(a, inner, module_qn));
        }
        return cbm_type_reference(a, c_parse_return_type_text(a, inner, module_qn));
    }

    // decltype(expr) — extract the type from the expression
    if (strncmp(text, "decltype(", 9) == 0 && len > 10 && text[len - 1] == ')') {
        char *inner = cbm_arena_strndup(a, text + 9, len - 10);
        size_t ilen = strlen(inner);
        // decltype(auto) — auto deduction, can't resolve statically
        if (strcmp(inner, "auto") == 0)
            return cbm_type_unknown();
        // decltype(TypeName()) — constructor call → type is TypeName
        if (ilen >= 2 && inner[ilen - 1] == ')' && inner[ilen - 2] == '(') {
            inner[ilen - 2] = '\0';
            return c_parse_return_type_text(a, inner, module_qn);
        }
        // decltype(expr) — try to interpret as type name (covers decltype(x) where x is a type)
        return c_parse_return_type_text(a, inner, module_qn);
    }

    // Builtins
    if (is_c_builtin_type(text))
        return cbm_type_builtin(a, text);

    // Template type parameters: only T, U, V, K, W (common conventions) or T1-T9.
    // Do NOT treat all single uppercase letters as type params — A, B, C, etc.
    // are legitimate class names (especially in C++ code with short names).
    if (len == 1 &&
        (text[0] == 'T' || text[0] == 'U' || text[0] == 'V' || text[0] == 'K' || text[0] == 'W')) {
        return cbm_type_type_param(a, text);
    }
    if (len == 2 && text[0] == 'T' && text[1] >= '1' && text[1] <= '9') {
        return cbm_type_type_param(a, text);
    }

    // Template type: Name<Arg1, Arg2, ...>
    {
        const char *angle = strchr(text, '<');
        if (angle && len > 0 && text[len - 1] == '>') {
            // Extract template name
            size_t name_len = (size_t)(angle - text);
            char *tmpl_name = cbm_arena_strndup(a, text, name_len);
            while (name_len > 0 && tmpl_name[name_len - 1] == ' ')
                tmpl_name[--name_len] = '\0';
            // Replace :: with .
            for (char *p = tmpl_name; *p; p++) {
                if (p[0] == ':' && p[1] == ':') {
                    p[0] = '.';
                    memmove(p + 1, p + 2, strlen(p + 2) + 1);
                }
            }

            // Extract args between < and >
            const char *args_start = angle + 1;
            size_t args_len = (size_t)((text + len - 1) - args_start);
            char *args_text = cbm_arena_strndup(a, args_start, args_len);

            // Split by comma at nesting depth 0
            const CBMType *arg_types[16];
            int arg_count = 0;
            int depth = 0;
            const char *arg_begin = args_text;
            for (const char *p = args_text;; p++) {
                if (*p == '<')
                    depth++;
                else if (*p == '>')
                    depth--;
                if ((*p == ',' && depth == 0) || *p == '\0') {
                    if (arg_count < 16) {
                        size_t alen = (size_t)(p - arg_begin);
                        char *arg = cbm_arena_strndup(a, arg_begin, alen);
                        // Trim whitespace
                        while (*arg == ' ')
                            arg++;
                        size_t al = strlen(arg);
                        while (al > 0 && arg[al - 1] == ' ')
                            arg[--al] = '\0';
                        if (arg[0]) {
                            arg_types[arg_count++] = c_parse_return_type_text(a, arg, module_qn);
                        }
                    }
                    if (*p == '\0')
                        break;
                    arg_begin = p + 1;
                }
            }

            // Qualify with module if unqualified
            const char *qname = tmpl_name;
            if (!strchr(tmpl_name, '.') && module_qn && module_qn[0]) {
                qname = cbm_arena_sprintf(a, "%s.%s", module_qn, tmpl_name);
            }
            return cbm_type_template(a, qname, arg_types, arg_count);
        }
    }

    // Qualified name (has ::)
    if (strstr(text, "::")) {
        // Replace :: with .
        char *qn = cbm_arena_strdup(a, text);
        for (char *p = qn; *p; p++) {
            if (p[0] == ':' && p[1] == ':') {
                p[0] = '.';
                memmove(p + 1, p + 2, strlen(p + 2) + 1);
            }
        }
        return cbm_type_named(a, qn);
    }

    // Named type: qualify with module
    if (module_qn && module_qn[0]) {
        return cbm_type_named(a, cbm_arena_sprintf(a, "%s.%s", module_qn, text));
    }
    return cbm_type_named(a, text);
}

// ============================================================================
// Entry point: single-file LSP
// ============================================================================

void cbm_run_c_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                   TSNode root, bool cpp_mode) {

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    // Register stdlib
    cbm_c_stdlib_register(&reg, arena);
    if (cpp_mode)
        cbm_cpp_stdlib_register(&reg, arena);

    const char *module_qn = result->module_qn;

    // Register file's own definitions
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name)
            continue;

        if (d->label && (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Type") == 0)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->name;

            // Base classes
            if (d->base_classes) {
                int bc_count = 0;
                while (d->base_classes[bc_count])
                    bc_count++;
                if (bc_count > 0) {
                    const char **embedded = (const char **)cbm_arena_alloc(
                        arena, (bc_count + 1) * sizeof(const char *));
                    for (int j = 0; j < bc_count; j++) {
                        const char *bc = d->base_classes[j];
                        // Check if already qualified
                        if (strchr(bc, '.') || strstr(bc, "::")) {
                            embedded[j] = bc;
                        } else {
                            embedded[j] = cbm_arena_sprintf(arena, "%s.%s", module_qn, bc);
                        }
                    }
                    embedded[bc_count] = NULL;
                    rt.embedded_types = embedded;
                }
            }
            cbm_registry_add_type(&reg, rt);
        }

        // Field definitions → populate field_names/field_types on parent type
        if (d->label && strcmp(d->label, "Field") == 0 && d->parent_class && d->return_type) {
            // Find or add the parent type in registry
            CBMRegisteredType *parent_rt = NULL;
            for (int ri = 0; ri < reg.type_count; ri++) {
                if (strcmp(reg.types[ri].qualified_name, d->parent_class) == 0) {
                    parent_rt = &reg.types[ri];
                    break;
                }
            }
            if (parent_rt) {
                // Count existing fields
                int existing = 0;
                if (parent_rt->field_names) {
                    while (parent_rt->field_names[existing])
                        existing++;
                }
                // Add this field
                const char **new_fnames =
                    (const char **)cbm_arena_alloc(arena, (existing + 2) * sizeof(const char *));
                const CBMType **new_ftypes = (const CBMType **)cbm_arena_alloc(
                    arena, (existing + 2) * sizeof(const CBMType *));
                if (new_fnames && new_ftypes) {
                    for (int j = 0; j < existing; j++) {
                        new_fnames[j] = parent_rt->field_names[j];
                        new_ftypes[j] = parent_rt->field_types ? parent_rt->field_types[j] : NULL;
                    }
                    new_fnames[existing] = d->name;
                    new_ftypes[existing] =
                        c_parse_return_type_text(arena, d->return_type, module_qn);
                    new_fnames[existing + 1] = NULL;
                    new_ftypes[existing + 1] = NULL;
                    parent_rt->field_names = new_fnames;
                    parent_rt->field_types = new_ftypes;
                }
            }
        }

        if (d->label && (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0)) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.min_params = -1;
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;

            // Build return type — prefer return_type (raw text) over return_types
            // (cleaned by clean_type_name which strips template args like <Service>).
            const CBMType **ret_types = NULL;
            if (d->return_type && d->return_type[0]) {
                ret_types = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                ret_types[0] = c_parse_return_type_text(arena, d->return_type, module_qn);
                ret_types[1] = NULL;
            } else if (d->return_types) {
                int count = 0;
                while (d->return_types[count])
                    count++;
                if (count > 0) {
                    ret_types = (const CBMType **)cbm_arena_alloc(
                        arena, (count + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < count; j++)
                        ret_types[j] =
                            c_parse_return_type_text(arena, d->return_types[j], module_qn);
                    ret_types[count] = NULL;
                }
            }

            // Build param types
            const CBMType **param_types_arr = NULL;
            if (d->param_types) {
                int count = 0;
                while (d->param_types[count])
                    count++;
                if (count > 0) {
                    param_types_arr = (const CBMType **)cbm_arena_alloc(
                        arena, (count + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < count; j++)
                        param_types_arr[j] =
                            c_parse_return_type_text(arena, d->param_types[j], module_qn);
                    param_types_arr[count] = NULL;
                }
            }

            rf.signature = cbm_type_func(arena, d->param_names, param_types_arr, ret_types);

            // Method receiver
            if (strcmp(d->label, "Method") == 0 && d->parent_class) {
                rf.receiver_type = d->parent_class;
                // Auto-create type if needed
                if (!cbm_registry_lookup_type(&reg, d->parent_class)) {
                    CBMRegisteredType auto_type;
                    memset(&auto_type, 0, sizeof(auto_type));
                    auto_type.qualified_name = d->parent_class;
                    // Extract short name
                    const char *dot = strrchr(d->parent_class, '.');
                    auto_type.short_name = dot ? dot + 1 : d->parent_class;
                    cbm_registry_add_type(&reg, auto_type);
                }
            }

            cbm_registry_add_func(&reg, rf);
        }
    }

    // Initialize context and run
    CLSPContext ctx;
    c_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, cpp_mode, &result->resolved_calls);

    c_lsp_process_file(&ctx, root);
}

// ============================================================================
// Entry point: cross-file LSP
// ============================================================================

/* Register one batch of CBMLSPDef[] into a registry. Shared by the
 * per-file cross-LSP path and the Tier 2 pre-built registry builder.
 * Reads field/return/embedded info from the def strings (def-driven —
 * no per-file AST mutation), so the same defs always yield the same
 * registry entries regardless of which file is being processed. */
static void c_register_lsp_defs(CBMArena *arena, CBMTypeRegistry *reg, const char *module_qn,
                                CBMLSPDef *defs, int def_count) {
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name)
            continue;

        if (d->label && (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Type") == 0 ||
                         strcmp(d->label, "Interface") == 0)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name; /* borrowed — d outlives this call */
            rt.short_name = d->short_name;
            rt.is_interface = d->is_interface;

            // Embedded/base types
            if (d->embedded_types) {
                // Parse "|"-separated list
                const char *src = d->embedded_types;
                const char *embeds[32];
                int embed_count = 0;
                while (*src && embed_count < 31) {
                    const char *sep = strchr(src, '|');
                    if (sep) {
                        embeds[embed_count++] = cbm_arena_strndup(arena, src, sep - src);
                        src = sep + 1;
                    } else {
                        embeds[embed_count++] = cbm_arena_strdup(arena, src);
                        break;
                    }
                }
                if (embed_count > 0) {
                    const char **arr = (const char **)cbm_arena_alloc(
                        arena, (embed_count + 1) * sizeof(const char *));
                    for (int j = 0; j < embed_count; j++)
                        arr[j] = embeds[j];
                    arr[embed_count] = NULL;
                    rt.embedded_types = arr;
                }
            }

            // Field defs
            if (d->field_defs) {
                const char *fsrc = d->field_defs;
                const char *fnames[64];
                const CBMType *ftypes[64];
                int fcount = 0;
                while (*fsrc && fcount < 63) {
                    const char *sep = strchr(fsrc, '|');
                    const char *end = sep ? sep : fsrc + strlen(fsrc);
                    char *pair = cbm_arena_strndup(arena, fsrc, end - fsrc);
                    char *colon = strchr(pair, ':');
                    if (colon) {
                        *colon = '\0';
                        fnames[fcount] = pair;
                        ftypes[fcount] = c_parse_return_type_text(
                            arena, colon + 1, d->def_module_qn ? d->def_module_qn : module_qn);
                        fcount++;
                    }
                    if (!sep)
                        break;
                    fsrc = sep + 1;
                }
                if (fcount > 0) {
                    const char **fnarr =
                        (const char **)cbm_arena_alloc(arena, (fcount + 1) * sizeof(const char *));
                    const CBMType **ftarr = (const CBMType **)cbm_arena_alloc(
                        arena, (fcount + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < fcount; j++) {
                        fnarr[j] = fnames[j];
                        ftarr[j] = ftypes[j];
                    }
                    fnarr[fcount] = NULL;
                    ftarr[fcount] = NULL;
                    rt.field_names = fnarr;
                    rt.field_types = ftarr;
                }
            }

            cbm_registry_add_type(reg, rt);
        }

        if (d->label && (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0)) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.min_params = -1;
            rf.qualified_name = d->qualified_name; /* borrowed */
            rf.short_name = d->short_name;

            const char *def_module = d->def_module_qn ? d->def_module_qn : module_qn;

            // Return types
            if (d->return_types) {
                // Parse "|"-separated
                const char *rsrc = d->return_types;
                const CBMType *rets[16];
                int rcount = 0;
                while (*rsrc && rcount < 15) {
                    const char *sep = strchr(rsrc, '|');
                    const char *end = sep ? sep : rsrc + strlen(rsrc);
                    char *rt_text = cbm_arena_strndup(arena, rsrc, end - rsrc);
                    rets[rcount++] = c_parse_return_type_text(arena, rt_text, def_module);
                    if (!sep)
                        break;
                    rsrc = sep + 1;
                }
                if (rcount > 0) {
                    const CBMType **rarr = (const CBMType **)cbm_arena_alloc(
                        arena, (rcount + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < rcount; j++)
                        rarr[j] = rets[j];
                    rarr[rcount] = NULL;
                    rf.signature = cbm_type_func(arena, NULL, NULL, rarr);
                }
            }
            if (!rf.signature)
                rf.signature = cbm_type_func(arena, NULL, NULL, NULL);

            if (d->receiver_type) {
                rf.receiver_type = d->receiver_type; /* borrowed */
            }

            cbm_registry_add_func(reg, rf);
        }
    }
}

/* Tier 2: build a project-wide C/C++/CUDA registry ONCE from all defs.
 * Registers both C and C++ stdlibs (C is a subset; harmless overlap)
 * and all C-family defs. Shared READ-ONLY across resolve workers.
 * Def-driven (no AST field collection) so produces identical entries
 * to the per-file build — zero quality loss. */
CBMTypeRegistry *cbm_c_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count) {
    if (!arena)
        return NULL;
    CBMTypeRegistry *reg = (CBMTypeRegistry *)cbm_arena_alloc(arena, sizeof(*reg));
    if (!reg)
        return NULL;
    cbm_registry_init(reg, arena);
    cbm_c_stdlib_register(reg, arena);
    cbm_cpp_stdlib_register(reg, arena);
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (d->lang != CBM_LANG_C && d->lang != CBM_LANG_CPP && d->lang != CBM_LANG_CUDA) {
            continue;
        }
        c_register_lsp_defs(arena, reg, "", d, 1);
    }
    cbm_registry_finalize(reg);
    return reg;
}

void cbm_run_c_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                       const char *module_qn, bool cpp_mode, CBMTypeRegistry *reg,
                                       const char **include_paths, const char **include_ns_qns,
                                       int include_count, TSTree *cached_tree,
                                       CBMResolvedCallArray *out) {
    if (!source || source_len == 0 || !out || !reg)
        return;

    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        const TSLanguage *ts_lang = cpp_mode ? tree_sitter_cpp() : tree_sitter_c();
        ts_parser_set_language(parser, ts_lang);
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        ts_parser_delete(parser);
        owns_tree = true;
        if (!tree)
            return;
    }
    TSNode root = ts_tree_root_node(tree);

    CLSPContext ctx;
    c_lsp_init(&ctx, arena, source, source_len, reg, module_qn, cpp_mode, out);
    ctx.registry_shared = true; /* Tier-2 shared registry: read-only, see flag doc */
    for (int i = 0; i < include_count; i++) {
        c_lsp_add_include(&ctx, include_paths[i], include_ns_qns[i]);
    }
    c_lsp_process_file(&ctx, root);

    if (owns_tree) {
        ts_tree_delete(tree);
    }
}

void cbm_run_c_lsp_cross(CBMArena *arena, const char *source, int source_len, const char *module_qn,
                         bool cpp_mode, CBMLSPDef *defs, int def_count, const char **include_paths,
                         const char **include_ns_qns, int include_count, TSTree *cached_tree,
                         CBMResolvedCallArray *out) {

    if (!source || source_len == 0 || !out)
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    // Register stdlib
    cbm_c_stdlib_register(&reg, arena);
    if (cpp_mode)
        cbm_cpp_stdlib_register(&reg, arena);

    // Register all defs (shared helper — def-driven)
    c_register_lsp_defs(arena, &reg, module_qn, defs, def_count);

    // Use cached tree if available, otherwise parse fresh
    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        const TSLanguage *ts_lang = cpp_mode ? tree_sitter_cpp() : tree_sitter_c();
        ts_parser_set_language(parser, ts_lang);
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        ts_parser_delete(parser);
        owns_tree = true;
        if (!tree)
            return;
    }

    TSNode root = ts_tree_root_node(tree);

    // Finalize registry — O(1) lookups. See go_lsp.c "3c. Finalize"
    // comment for the rationale (linear-scan fallback otherwise).
    cbm_registry_finalize(&reg);

    // Initialize context and run
    CLSPContext ctx;
    c_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, cpp_mode, out);

    // Add include mappings
    for (int i = 0; i < include_count; i++) {
        c_lsp_add_include(&ctx, include_paths[i], include_ns_qns[i]);
    }

    c_lsp_process_file(&ctx, root);

    if (owns_tree) {
        ts_tree_delete(tree);
    }
}

// --- Batch cross-file LSP ---

void cbm_batch_c_lsp_cross(CBMArena *arena, CBMBatchCLSPFile *files, int file_count,
                           CBMResolvedCallArray *out) {
    if (!files || file_count <= 0 || !out)
        return;

    for (int f = 0; f < file_count; f++) {
        CBMBatchCLSPFile *file = &files[f];
        memset(&out[f], 0, sizeof(CBMResolvedCallArray));

        if (!file->source || file->source_len <= 0 || file->def_count <= 0)
            continue;

        // Per-file arena: registry + temp data freed after each file
        CBMArena file_arena;
        cbm_arena_init(&file_arena);

        CBMResolvedCallArray file_out;
        memset(&file_out, 0, sizeof(file_out));

        // Delegate to existing per-file function
        cbm_run_c_lsp_cross(&file_arena, file->source, file->source_len, file->module_qn,
                            file->cpp_mode, file->defs, file->def_count, file->include_paths,
                            file->include_ns_qns, file->include_count, file->cached_tree,
                            &file_out);

        // Copy results to output arena (must outlive per-file arena)
        if (file_out.count > 0) {
            out[f].count = file_out.count;
            out[f].items =
                (CBMResolvedCall *)cbm_arena_alloc(arena, file_out.count * sizeof(CBMResolvedCall));
            for (int j = 0; j < file_out.count; j++) {
                CBMResolvedCall *src = &file_out.items[j];
                CBMResolvedCall *dst = &out[f].items[j];
                dst->caller_qn = src->caller_qn ? cbm_arena_strdup(arena, src->caller_qn) : NULL;
                dst->callee_qn = src->callee_qn ? cbm_arena_strdup(arena, src->callee_qn) : NULL;
                dst->strategy = src->strategy ? cbm_arena_strdup(arena, src->strategy) : NULL;
                dst->confidence = src->confidence;
                dst->reason = src->reason ? cbm_arena_strdup(arena, src->reason) : NULL;
            }
        }

        cbm_arena_destroy(&file_arena);
    }
}
