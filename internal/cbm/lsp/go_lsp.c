#include "go_lsp.h"
#include "lsp_node_iter.h"
#include "../helpers.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

// Forward declarations
static void resolve_calls_in_node_inner(GoLSPContext* ctx, TSNode node);

/* Depth-guarded entry for the AST call-resolution walk. The walk recurses once
 * per nesting level; a deeply-nested or cyclic file can overflow the native
 * stack (SIGSEGV) and take down the whole index. Past the cap the subtree is
 * skipped — its calls stay unresolved, which is graceful degradation, not a
 * crash. The cap is CBM_LSP_MAX_WALK_DEPTH, env-overridable via the same name.
 * The walk_depth-- runs after the inner returns, so early returns in the body
 * never leak the counter. */
static void resolve_calls_in_node(GoLSPContext* ctx, TSNode node) {
    if (ctx->walk_depth >= cbm_lsp_max_walk_depth())
        return;
    ctx->walk_depth++;
    resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}
static void emit_resolved_call(GoLSPContext* ctx, const char* callee_qn, const char* strategy, float confidence);
static const CBMType* go_lookup_field(GoLSPContext* ctx, const char* type_qn, const char* field_name, int depth);
static void extract_type_params_from_ast(CBMArena* arena, CBMTypeRegistry* reg,
    TSNode root, const char* source, const char* module_qn);

// --- Initialization ---

void go_lsp_init(GoLSPContext* ctx, CBMArena* arena, const char* source, int source_len,
    const CBMTypeRegistry* registry, const char* package_qn, CBMResolvedCallArray* out) {
    memset(ctx, 0, sizeof(GoLSPContext));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->package_qn = package_qn;
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL); // root scope

    {
        const char* debug_env = getenv("CBM_LSP_DEBUG");
        ctx->debug = (debug_env && debug_env[0]);
    }
}

void go_lsp_add_import(GoLSPContext* ctx, const char* local_name, const char* pkg_qn) {
    // Store in parallel arrays (arena-allocated, grow by doubling)
    if (ctx->import_count % 32 == 0) {
        int new_cap = ctx->import_count + 32;
        const char** new_names = (const char**)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(const char*));
        const char** new_qns = (const char**)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(const char*));
        if (!new_names || !new_qns) return;
        if (ctx->import_local_names && ctx->import_count > 0) {
            memcpy(new_names, ctx->import_local_names, ctx->import_count * sizeof(const char*));
            memcpy(new_qns, ctx->import_package_qns, ctx->import_count * sizeof(const char*));
        }
        ctx->import_local_names = new_names;
        ctx->import_package_qns = new_qns;
    }
    ctx->import_local_names[ctx->import_count] = cbm_arena_strdup(ctx->arena, local_name);
    ctx->import_package_qns[ctx->import_count] = cbm_arena_strdup(ctx->arena, pkg_qn);
    ctx->import_count++;
}

// --- Helper: get node text ---

static char* lsp_node_text(GoLSPContext* ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

// --- Helper: resolve import alias to package QN ---

static const char* resolve_import(GoLSPContext* ctx, const char* local_name) {
    for (int i = 0; i < ctx->import_count; i++) {
        if (strcmp(ctx->import_local_names[i], local_name) == 0) {
            return ctx->import_package_qns[i];
        }
    }
    return NULL;
}

// --- Helper: check if name is a Go builtin ---

static bool is_go_builtin_func(const char* name) {
    static const char* builtins[] = {
        "make", "new", "append", "len", "cap", "delete",
        "close", "copy", "panic", "recover", "print", "println",
        "complex", "real", "imag", "min", "max", "clear",
        NULL
    };
    for (const char** b = builtins; *b; b++) {
        if (strcmp(name, *b) == 0) return true;
    }
    return false;
}

// --- Helper: check if name is a Go builtin type ---

static const CBMType* resolve_builtin_type(GoLSPContext* ctx, const char* name) {
    static const char* builtin_types[] = {
        "int", "int8", "int16", "int32", "int64",
        "uint", "uint8", "uint16", "uint32", "uint64",
        "float32", "float64", "complex64", "complex128",
        "string", "bool", "byte", "rune", "error",
        "uintptr", "any",
        NULL
    };
    for (const char** b = builtin_types; *b; b++) {
        if (strcmp(name, *b) == 0) {
            return cbm_type_builtin(ctx->arena, name);
        }
    }
    return NULL;
}

// --- go_parse_type_node: AST type node -> CBMType ---

const CBMType* go_parse_type_node(GoLSPContext* ctx, TSNode node) {
    if (ts_node_is_null(node)) return cbm_type_unknown();

    const char* kind = ts_node_type(node);

    // type_identifier: simple named type
    if (strcmp(kind, "type_identifier") == 0) {
        char* name = lsp_node_text(ctx, node);
        if (!name) return cbm_type_unknown();
        const CBMType* builtin = resolve_builtin_type(ctx, name);
        if (builtin) return builtin;
        // Resolve as local type: package_qn.TypeName
        return cbm_type_named(ctx->arena,
            cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, name));
    }

    // qualified_type: pkg.Type
    if (strcmp(kind, "qualified_type") == 0) {
        TSNode pkg_node = ts_node_child_by_field_name(node, "package", 7);
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(pkg_node) && !ts_node_is_null(name_node)) {
            char* pkg = lsp_node_text(ctx, pkg_node);
            char* name = lsp_node_text(ctx, name_node);
            const char* pkg_qn = resolve_import(ctx, pkg);
            if (pkg_qn) {
                return cbm_type_named(ctx->arena,
                    cbm_arena_sprintf(ctx->arena, "%s.%s", pkg_qn, name));
            }
        }
        return cbm_type_unknown();
    }

    // pointer_type: *T
    if (strcmp(kind, "pointer_type") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        if (nc > 0) {
            return cbm_type_pointer(ctx->arena,
                go_parse_type_node(ctx, ts_node_named_child(node, nc - 1)));
        }
        return cbm_type_unknown();
    }

    // slice_type: []T
    if (strcmp(kind, "slice_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0) {
            elem = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        }
        return cbm_type_slice(ctx->arena, go_parse_type_node(ctx, elem));
    }

    // array_type: [N]T — treat as slice for our purposes
    if (strcmp(kind, "array_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0) {
            elem = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        }
        return cbm_type_slice(ctx->arena, go_parse_type_node(ctx, elem));
    }

    // map_type: map[K]V
    if (strcmp(kind, "map_type") == 0) {
        TSNode key = ts_node_child_by_field_name(node, "key", 3);
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        return cbm_type_map(ctx->arena,
            go_parse_type_node(ctx, key),
            go_parse_type_node(ctx, value));
    }

    // channel_type: chan T
    if (strcmp(kind, "channel_type") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value) && ts_node_named_child_count(node) > 0) {
            value = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        }
        // Determine direction from text
        char* text = lsp_node_text(ctx, node);
        int dir = 0;
        if (text) {
            if (strncmp(text, "chan<-", 6) == 0 || strncmp(text, "chan <-", 7) == 0) dir = 1;
            else if (strncmp(text, "<-chan", 6) == 0 || strncmp(text, "<- chan", 7) == 0) dir = 2;
        }
        return cbm_type_channel(ctx->arena, go_parse_type_node(ctx, value), dir);
    }

    // function_type: func(...)...
    if (strcmp(kind, "function_type") == 0) {
        return cbm_type_func(ctx->arena, NULL, NULL, NULL); // simplified
    }

    // interface_type
    if (strcmp(kind, "interface_type") == 0) {
        CBMType* t = (CBMType*)cbm_arena_alloc(ctx->arena, sizeof(CBMType));
        memset(t, 0, sizeof(CBMType));
        t->kind = CBM_TYPE_INTERFACE;
        return t;
    }

    // struct_type
    if (strcmp(kind, "struct_type") == 0) {
        CBMType* t = (CBMType*)cbm_arena_alloc(ctx->arena, sizeof(CBMType));
        memset(t, 0, sizeof(CBMType));
        t->kind = CBM_TYPE_STRUCT;
        return t;
    }

    // parenthesized_type: (T)
    if (strcmp(kind, "parenthesized_type") == 0 && ts_node_named_child_count(node) > 0) {
        return go_parse_type_node(ctx, ts_node_named_child(node, 0));
    }

    // type_elem: wrapper in type_arguments, unwrap to inner type
    if (strcmp(kind, "type_elem") == 0 && ts_node_named_child_count(node) > 0) {
        return go_parse_type_node(ctx, ts_node_named_child(node, 0));
    }

    // generic_type: Type[T1, T2] — return as named without generic args for now
    if (strcmp(kind, "generic_type") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node)) {
            return go_parse_type_node(ctx, type_node);
        }
    }

    // parameter_list used as result type (multi-return)
    if (strcmp(kind, "parameter_list") == 0) {
        int count = 0;
        const CBMType* elems[16];
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && count < 16; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
            const char* ck = ts_node_type(child);
            if (strcmp(ck, "parameter_declaration") == 0) {
                TSNode tn = ts_node_child_by_field_name(child, "type", 4);
                if (!ts_node_is_null(tn)) {
                    elems[count++] = go_parse_type_node(ctx, tn);
                }
            } else {
                elems[count++] = go_parse_type_node(ctx, child);
            }
        }
        if (count == 1) return elems[0];
        if (count > 1) return cbm_type_tuple(ctx->arena, elems, count);
    }

    return cbm_type_unknown();
}

// --- Implicit generics: type unification ---

// Unify a parameter type pattern (containing TYPE_PARAM) against a concrete argument type.
// Fills inferred[i] with the concrete type bound to type_param_names[i].
static void go_unify_type(const CBMType* param_type, const CBMType* arg_type,
    const char** type_param_names, const CBMType** inferred, int param_count) {
    if (!param_type || !arg_type || cbm_type_is_unknown(arg_type)) return;

    if (param_type->kind == CBM_TYPE_TYPE_PARAM) {
        for (int i = 0; i < param_count; i++) {
            if (strcmp(param_type->data.type_param.name, type_param_names[i]) == 0) {
                if (!inferred[i]) inferred[i] = arg_type;
                break;
            }
        }
        return;
    }

    // Structural matching — recurse into composite types
    if (param_type->kind == CBM_TYPE_SLICE && arg_type->kind == CBM_TYPE_SLICE) {
        go_unify_type(param_type->data.slice.elem, arg_type->data.slice.elem,
            type_param_names, inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_POINTER && arg_type->kind == CBM_TYPE_POINTER) {
        go_unify_type(param_type->data.pointer.elem, arg_type->data.pointer.elem,
            type_param_names, inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_MAP && arg_type->kind == CBM_TYPE_MAP) {
        go_unify_type(param_type->data.map.key, arg_type->data.map.key,
            type_param_names, inferred, param_count);
        go_unify_type(param_type->data.map.value, arg_type->data.map.value,
            type_param_names, inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_CHANNEL && arg_type->kind == CBM_TYPE_CHANNEL) {
        go_unify_type(param_type->data.channel.elem, arg_type->data.channel.elem,
            type_param_names, inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_FUNC && arg_type->kind == CBM_TYPE_FUNC) {
        // Match param types
        if (param_type->data.func.param_types && arg_type->data.func.param_types) {
            for (int i = 0; param_type->data.func.param_types[i] && arg_type->data.func.param_types[i]; i++) {
                go_unify_type(param_type->data.func.param_types[i], arg_type->data.func.param_types[i],
                    type_param_names, inferred, param_count);
            }
        }
        // Match return types
        if (param_type->data.func.return_types && arg_type->data.func.return_types) {
            for (int i = 0; param_type->data.func.return_types[i] && arg_type->data.func.return_types[i]; i++) {
                go_unify_type(param_type->data.func.return_types[i], arg_type->data.func.return_types[i],
                    type_param_names, inferred, param_count);
            }
        }
    }
}

// --- go_eval_expr_type: recursive expression type evaluator ---

const CBMType* go_eval_expr_type(GoLSPContext* ctx, TSNode node) {
    if (ts_node_is_null(node)) return cbm_type_unknown();

    const char* kind = ts_node_type(node);

    // --- Identifier: scope lookup ---
    if (strcmp(kind, "identifier") == 0) {
        char* name = lsp_node_text(ctx, node);
        if (!name) return cbm_type_unknown();

        // Check scope first
        const CBMType* t = cbm_scope_lookup(ctx->current_scope, name);
        if (!cbm_type_is_unknown(t)) return t;

        // Check if it's a package-level function
        const CBMRegisteredFunc* f = cbm_registry_lookup_symbol(ctx->registry, ctx->package_qn, name);
        if (f && f->signature) return f->signature;

        // Check if it's a builtin type (for type conversions like string(x), int(x))
        const CBMType* bt = resolve_builtin_type(ctx, name);
        if (bt) return cbm_type_named(ctx->arena, name);

        // Check if it's a registered type (for type conversions like MyType(x))
        const char* type_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, name);
        const CBMRegisteredType* rt = cbm_registry_lookup_type(ctx->registry, type_qn);
        if (rt) return cbm_type_named(ctx->arena, type_qn);

        return cbm_type_unknown();
    }

    // --- Selector expression: a.B ---
    if (strcmp(kind, "selector_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
        TSNode field = ts_node_child_by_field_name(node, "field", 5);
        if (ts_node_is_null(operand) || ts_node_is_null(field)) return cbm_type_unknown();

        char* field_name = lsp_node_text(ctx, field);
        if (!field_name) return cbm_type_unknown();

        // Check if operand is an import alias (pkg.Symbol)
        if (strcmp(ts_node_type(operand), "identifier") == 0) {
            char* pkg_name = lsp_node_text(ctx, operand);
            if (pkg_name) {
                const char* pkg_qn = resolve_import(ctx, pkg_name);
                if (pkg_qn) {
                    // Look up pkg.Symbol as a function or type
                    const CBMRegisteredFunc* f = cbm_registry_lookup_symbol(ctx->registry, pkg_qn, field_name);
                    if (f && f->signature) return f->signature;
                    // Check if it's a type
                    char* type_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", pkg_qn, field_name);
                    const CBMRegisteredType* rt = cbm_registry_lookup_type(ctx->registry, type_qn);
                    if (rt) return cbm_type_named(ctx->arena, type_qn);
                    return cbm_type_unknown();
                }
            }
        }

        // Evaluate operand type
        const CBMType* recv_type = go_eval_expr_type(ctx, operand);
        if (cbm_type_is_unknown(recv_type)) return cbm_type_unknown();

        // Auto-deref pointers for method calls
        const CBMType* base_type = recv_type;
        if (base_type->kind == CBM_TYPE_POINTER) {
            base_type = cbm_type_deref(base_type);
        }

        if (base_type->kind == CBM_TYPE_NAMED) {
            const char* type_qn = base_type->data.named.qualified_name;

            // Look up method/field (methods recurse through embeddings)
            const CBMRegisteredFunc* method = go_lookup_field_or_method(ctx, type_qn, field_name);
            if (method && method->signature) return method->signature;

            // Check struct fields (with embedded field promotion)
            const CBMType* field_type = go_lookup_field(ctx, type_qn, field_name, 0);
            if (field_type && !cbm_type_is_unknown(field_type)) return field_type;
        }

        return cbm_type_unknown();
    }

    // --- Call expression: f(...) ---
    if (strcmp(kind, "call_expression") == 0) {
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        TSNode args_node = ts_node_child_by_field_name(node, "arguments", 9);
        if (ts_node_is_null(func_node)) return cbm_type_unknown();

        // Check for builtin calls
        if (strcmp(ts_node_type(func_node), "identifier") == 0) {
            char* name = lsp_node_text(ctx, func_node);
            if (name && is_go_builtin_func(name)) {
                return go_eval_builtin_call(ctx, name, args_node);
            }
        }

        // Evaluate function type
        const CBMType* func_type = go_eval_expr_type(ctx, func_node);

        // If it's a FUNC type, return its return type
        if (func_type && func_type->kind == CBM_TYPE_FUNC &&
            func_type->data.func.return_types && func_type->data.func.return_types[0]) {

            // Check for explicit type arguments: call_expression has type_arguments field
            // Go tree-sitter: Func[T1, T2](args) → call_expression { function, type_arguments, arguments }
            TSNode targs_node = ts_node_child_by_field_name(node, "type_arguments", 14);
            if (!ts_node_is_null(targs_node)) {
                // Look up the registered function to get type_param_names
                const CBMRegisteredFunc* rfunc = NULL;
                char* func_name = lsp_node_text(ctx, func_node);
                if (func_name) {
                    const char* func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s",
                        ctx->package_qn, func_name);
                    rfunc = cbm_registry_lookup_func(ctx->registry, func_qn);
                    // If not found as local, try via import (selector_expression: pkg.Func)
                    if (!rfunc && strcmp(ts_node_type(func_node), "selector_expression") == 0) {
                        TSNode operand = ts_node_child_by_field_name(func_node, "operand", 7);
                        TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
                        if (!ts_node_is_null(operand) && !ts_node_is_null(field)) {
                            char* pkg = lsp_node_text(ctx, operand);
                            char* fn = lsp_node_text(ctx, field);
                            if (pkg && fn) {
                                const char* pkg_qn = resolve_import(ctx, pkg);
                                if (pkg_qn) {
                                    rfunc = cbm_registry_lookup_symbol(ctx->registry, pkg_qn, fn);
                                }
                            }
                        }
                    }
                }

                if (rfunc && rfunc->type_param_names) {
                    // Parse type arguments from AST
                    const CBMType* type_args[16];
                    int targ_count = 0;
                    uint32_t ta_nc = ts_node_child_count(targs_node);
                    for (uint32_t ti = 0; ti < ta_nc && targ_count < 15; ti++) {
                        TSNode targ = ts_node_child(targs_node, ti);
                        if (ts_node_is_null(targ) || !ts_node_is_named(targ)) continue;
                        type_args[targ_count++] = go_parse_type_node(ctx, targ);
                    }

                    // Count type params
                    int param_count = 0;
                    while (rfunc->type_param_names[param_count]) param_count++;

                    if (targ_count > 0 && targ_count == param_count) {
                        const CBMType** targ_arr = (const CBMType**)cbm_arena_alloc(
                            ctx->arena, (targ_count + 1) * sizeof(const CBMType*));
                        for (int ti = 0; ti < targ_count; ti++) targ_arr[ti] = type_args[ti];
                        targ_arr[targ_count] = NULL;

                        // Substitute type params in return type(s)
                        int ret_count = 0;
                        while (func_type->data.func.return_types[ret_count]) ret_count++;

                        if (ret_count == 1) {
                            return cbm_type_substitute(ctx->arena,
                                func_type->data.func.return_types[0],
                                rfunc->type_param_names, targ_arr);
                        }
                        // Multi-return: substitute all
                        const CBMType** new_rets = (const CBMType**)cbm_arena_alloc(
                            ctx->arena, (ret_count + 1) * sizeof(const CBMType*));
                        for (int ri = 0; ri < ret_count; ri++) {
                            new_rets[ri] = cbm_type_substitute(ctx->arena,
                                func_type->data.func.return_types[ri],
                                rfunc->type_param_names, targ_arr);
                        }
                        new_rets[ret_count] = NULL;
                        return cbm_type_tuple(ctx->arena, new_rets, ret_count);
                    }
                }
            }

            // Implicit generics: infer type args from argument types
            if (ts_node_is_null(targs_node)) {
                // Look up registered function to check for type_param_names
                const CBMRegisteredFunc* rfunc = NULL;
                char* func_name = lsp_node_text(ctx, func_node);
                if (func_name) {
                    const char* func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s",
                        ctx->package_qn, func_name);
                    rfunc = cbm_registry_lookup_func(ctx->registry, func_qn);
                    if (!rfunc && strcmp(ts_node_type(func_node), "selector_expression") == 0) {
                        TSNode operand = ts_node_child_by_field_name(func_node, "operand", 7);
                        TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
                        if (!ts_node_is_null(operand) && !ts_node_is_null(field)) {
                            char* pkg = lsp_node_text(ctx, operand);
                            char* fn = lsp_node_text(ctx, field);
                            if (pkg && fn) {
                                const char* pkg_qn = resolve_import(ctx, pkg);
                                if (pkg_qn)
                                    rfunc = cbm_registry_lookup_symbol(ctx->registry, pkg_qn, fn);
                            }
                        }
                    }
                }

                if (rfunc && rfunc->type_param_names && func_type->data.func.param_types) {
                    int tpc = 0;
                    while (rfunc->type_param_names[tpc]) tpc++;

                    // Check if any return type uses a type param
                    bool has_type_param = false;
                    int ret_count = 0;
                    while (func_type->data.func.return_types[ret_count]) ret_count++;
                    for (int ri = 0; ri < ret_count && !has_type_param; ri++) {
                        // Quick check: walk return type tree for TYPE_PARAM nodes
                        const CBMType* rt = func_type->data.func.return_types[ri];
                        if (rt->kind == CBM_TYPE_TYPE_PARAM) has_type_param = true;
                        else if (rt->kind == CBM_TYPE_SLICE && rt->data.slice.elem &&
                                 rt->data.slice.elem->kind == CBM_TYPE_TYPE_PARAM) has_type_param = true;
                        else if (rt->kind == CBM_TYPE_POINTER && rt->data.pointer.elem &&
                                 rt->data.pointer.elem->kind == CBM_TYPE_TYPE_PARAM) has_type_param = true;
                        else if (rt->kind == CBM_TYPE_MAP) {
                            if (rt->data.map.key && rt->data.map.key->kind == CBM_TYPE_TYPE_PARAM) has_type_param = true;
                            if (rt->data.map.value && rt->data.map.value->kind == CBM_TYPE_TYPE_PARAM) has_type_param = true;
                        }
                    }

                    if (has_type_param && tpc > 0 && tpc <= 16) {
                        // Evaluate argument types and unify
                        const CBMType* inferred[16] = {0};

                        if (!ts_node_is_null(args_node)) {
                            uint32_t argc = ts_node_named_child_count(args_node);
                            int pi = 0;
                            for (uint32_t ai = 0; ai < argc && func_type->data.func.param_types[pi]; ai++) {
                                TSNode arg = ts_node_named_child(args_node, ai);
                                if (ts_node_is_null(arg)) continue;
                                const CBMType* arg_type = go_eval_expr_type(ctx, arg);
                                go_unify_type(func_type->data.func.param_types[pi],
                                    arg_type, rfunc->type_param_names, inferred, tpc);
                                pi++;
                            }
                        }

                        // Check if all type params were inferred
                        bool all_inferred = true;
                        for (int i = 0; i < tpc; i++) {
                            if (!inferred[i]) { all_inferred = false; break; }
                        }

                        if (all_inferred) {
                            const CBMType** targ_arr = (const CBMType**)cbm_arena_alloc(
                                ctx->arena, (tpc + 1) * sizeof(const CBMType*));
                            for (int i = 0; i < tpc; i++) targ_arr[i] = inferred[i];
                            targ_arr[tpc] = NULL;

                            if (ret_count == 1) {
                                return cbm_type_substitute(ctx->arena,
                                    func_type->data.func.return_types[0],
                                    rfunc->type_param_names, targ_arr);
                            }
                            const CBMType** new_rets = (const CBMType**)cbm_arena_alloc(
                                ctx->arena, (ret_count + 1) * sizeof(const CBMType*));
                            for (int ri = 0; ri < ret_count; ri++) {
                                new_rets[ri] = cbm_type_substitute(ctx->arena,
                                    func_type->data.func.return_types[ri],
                                    rfunc->type_param_names, targ_arr);
                            }
                            new_rets[ret_count] = NULL;
                            return cbm_type_tuple(ctx->arena, new_rets, ret_count);
                        }
                    }
                }
            }

            // No type arguments or no substitution needed — return as-is
            if (!func_type->data.func.return_types[1]) {
                return func_type->data.func.return_types[0];
            }
            int count = 0;
            while (func_type->data.func.return_types[count]) count++;
            if (count > 1) {
                return cbm_type_tuple(ctx->arena, func_type->data.func.return_types, count);
            }
        }

        // Type conversion: Type(expr) — if func_node resolves to a named type
        if (func_type && func_type->kind == CBM_TYPE_NAMED) {
            return func_type;
        }

        // Type conversion with composite type syntax: []byte(s), map[K]V(x), etc.
        // The function node is a type node (slice_type, map_type, etc.)
        {
            const char* fk = ts_node_type(func_node);
            if (strcmp(fk, "slice_type") == 0 || strcmp(fk, "array_type") == 0 ||
                strcmp(fk, "map_type") == 0 || strcmp(fk, "pointer_type") == 0 ||
                strcmp(fk, "channel_type") == 0) {
                return go_parse_type_node(ctx, func_node);
            }
        }

        return cbm_type_unknown();
    }

    // --- Composite literal: Type{...} ---
    if (strcmp(kind, "composite_literal") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node)) {
            return go_parse_type_node(ctx, type_node);
        }
        return cbm_type_unknown();
    }

    // --- Unary expression: &x, *x, <-ch, !x ---
    if (strcmp(kind, "unary_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
        if (ts_node_is_null(operand)) return cbm_type_unknown();

        // Get operator
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char* op = lsp_node_text(ctx, child);
                if (!op) continue;
                if (strcmp(op, "&") == 0) {
                    return cbm_type_pointer(ctx->arena, go_eval_expr_type(ctx, operand));
                }
                if (strcmp(op, "*") == 0) {
                    return cbm_type_deref(go_eval_expr_type(ctx, operand));
                }
                if (strcmp(op, "<-") == 0) {
                    const CBMType* ch_type = go_eval_expr_type(ctx, operand);
                    if (ch_type && ch_type->kind == CBM_TYPE_CHANNEL) {
                        return ch_type->data.channel.elem;
                    }
                    return cbm_type_unknown();
                }
                if (strcmp(op, "!") == 0) {
                    return cbm_type_builtin(ctx->arena, "bool");
                }
                break;
            }
        }
        return cbm_type_unknown();
    }

    // --- Index expression: a[i] ---
    if (strcmp(kind, "index_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
        if (ts_node_is_null(operand)) return cbm_type_unknown();
        const CBMType* op_type = go_eval_expr_type(ctx, operand);
        if (!op_type) return cbm_type_unknown();
        if (op_type->kind == CBM_TYPE_MAP) return op_type->data.map.value;
        if (op_type->kind == CBM_TYPE_SLICE) return op_type->data.slice.elem;
        return cbm_type_unknown();
    }

    // --- Type assertion: x.(Type) ---
    if (strcmp(kind, "type_assertion_expression") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node)) {
            return go_parse_type_node(ctx, type_node);
        }
        return cbm_type_unknown();
    }

    // --- Parenthesized expression: (x) ---
    if (strcmp(kind, "parenthesized_expression") == 0 && ts_node_named_child_count(node) > 0) {
        return go_eval_expr_type(ctx, ts_node_named_child(node, 0));
    }

    // --- Binary expression ---
    if (strcmp(kind, "binary_expression") == 0) {
        // For comparisons, return bool
        // For arithmetic, return left operand type
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char* op = lsp_node_text(ctx, child);
                if (op && (strcmp(op, "==") == 0 || strcmp(op, "!=") == 0 ||
                           strcmp(op, "<") == 0 || strcmp(op, ">") == 0 ||
                           strcmp(op, "<=") == 0 || strcmp(op, ">=") == 0 ||
                           strcmp(op, "&&") == 0 || strcmp(op, "||") == 0)) {
                    return cbm_type_builtin(ctx->arena, "bool");
                }
                break;
            }
        }
        if (!ts_node_is_null(left)) return go_eval_expr_type(ctx, left);
        return cbm_type_unknown();
    }

    // --- Slice expression: a[low:high] ---
    if (strcmp(kind, "slice_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
        if (!ts_node_is_null(operand)) return go_eval_expr_type(ctx, operand);
        return cbm_type_unknown();
    }

    // --- Literals ---
    if (strcmp(kind, "interpreted_string_literal") == 0 ||
        strcmp(kind, "raw_string_literal") == 0) {
        return cbm_type_builtin(ctx->arena, "string");
    }
    if (strcmp(kind, "int_literal") == 0) {
        return cbm_type_builtin(ctx->arena, "int");
    }
    if (strcmp(kind, "float_literal") == 0) {
        return cbm_type_builtin(ctx->arena, "float64");
    }
    if (strcmp(kind, "true") == 0 || strcmp(kind, "false") == 0) {
        return cbm_type_builtin(ctx->arena, "bool");
    }
    if (strcmp(kind, "nil") == 0) {
        return cbm_type_unknown(); // nil has no concrete type
    }

    // --- Func literal (closure) ---
    if (strcmp(kind, "func_literal") == 0) {
        // Process the closure body to resolve calls with captured scope
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        if (!ts_node_is_null(body)) {
            // Push child scope (inherits all outer bindings via parent chain)
            CBMScope* saved = ctx->current_scope;
            ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

            // Bind closure parameters
            TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
            if (!ts_node_is_null(params)) {
                uint32_t nc = ts_node_child_count(params);
                for (uint32_t i = 0; i < nc; i++) {
                    TSNode param = ts_node_child(params, i);
                    if (ts_node_is_null(param) || !ts_node_is_named(param)) continue;
                    if (strcmp(ts_node_type(param), "parameter_declaration") != 0) continue;
                    TSNode type_node = ts_node_child_by_field_name(param, "type", 4);
                    const CBMType* pt = go_parse_type_node(ctx, type_node);
                    uint32_t pnc = ts_node_child_count(param);
                    for (uint32_t j = 0; j < pnc; j++) {
                        TSNode ch = ts_node_child(param, j);
                        if (!ts_node_is_null(ch) && ts_node_is_named(ch) &&
                            strcmp(ts_node_type(ch), "identifier") == 0) {
                            char* pname = lsp_node_text(ctx, ch);
                            if (pname && strcmp(pname, "_") != 0)
                                cbm_scope_bind(ctx->current_scope, pname, pt);
                        }
                    }
                }
            }

            // Walk closure body to resolve calls
            resolve_calls_in_node(ctx, body);

            ctx->current_scope = saved;
        }

        // Build full FUNC type with param/return types from AST
        const CBMType* pt_arr[16];
        int pt_count = 0;
        TSNode params2 = ts_node_child_by_field_name(node, "parameters", 10);
        if (!ts_node_is_null(params2)) {
            uint32_t nc2 = ts_node_child_count(params2);
            for (uint32_t i = 0; i < nc2 && pt_count < 15; i++) {
                TSNode p = ts_node_child(params2, i);
                if (ts_node_is_null(p) || !ts_node_is_named(p)) continue;
                if (strcmp(ts_node_type(p), "parameter_declaration") != 0) continue;
                TSNode pt = ts_node_child_by_field_name(p, "type", 4);
                if (!ts_node_is_null(pt))
                    pt_arr[pt_count++] = go_parse_type_node(ctx, pt);
            }
        }
        pt_arr[pt_count] = NULL;

        const CBMType* rt_arr[16];
        int rt_count = 0;
        TSNode result = ts_node_child_by_field_name(node, "result", 6);
        if (!ts_node_is_null(result)) {
            if (strcmp(ts_node_type(result), "parameter_list") == 0) {
                uint32_t rnc = ts_node_child_count(result);
                for (uint32_t i = 0; i < rnc && rt_count < 15; i++) {
                    TSNode rc = ts_node_child(result, i);
                    if (ts_node_is_null(rc) || !ts_node_is_named(rc)) continue;
                    TSNode rt = ts_node_child_by_field_name(rc, "type", 4);
                    if (ts_node_is_null(rt)) rt = rc;
                    rt_arr[rt_count++] = go_parse_type_node(ctx, rt);
                }
            } else {
                rt_arr[rt_count++] = go_parse_type_node(ctx, result);
            }
        }
        rt_arr[rt_count] = NULL;

        return cbm_type_func(ctx->arena, NULL,
            pt_count > 0 ? (const CBMType**)pt_arr : NULL,
            rt_count > 0 ? (const CBMType**)rt_arr : NULL);
    }

    return cbm_type_unknown();
}

// --- go_eval_builtin_call ---

const CBMType* go_eval_builtin_call(GoLSPContext* ctx, const char* name, TSNode args) {
    // make(Type, ...) -> Type
    if (strcmp(name, "make") == 0 && !ts_node_is_null(args)) {
        uint32_t nc = ts_node_named_child_count(args);
        if (nc > 0) {
            TSNode first_arg = ts_node_named_child(args, 0);
            return go_parse_type_node(ctx, first_arg);
        }
    }

    // new(Type) -> *Type
    if (strcmp(name, "new") == 0 && !ts_node_is_null(args)) {
        uint32_t nc = ts_node_named_child_count(args);
        if (nc > 0) {
            TSNode first_arg = ts_node_named_child(args, 0);
            return cbm_type_pointer(ctx->arena, go_parse_type_node(ctx, first_arg));
        }
    }

    // append(slice, ...) -> same slice type
    if (strcmp(name, "append") == 0 && !ts_node_is_null(args)) {
        uint32_t nc = ts_node_named_child_count(args);
        if (nc > 0) {
            return go_eval_expr_type(ctx, ts_node_named_child(args, 0));
        }
    }

    // len, cap -> int
    if (strcmp(name, "len") == 0 || strcmp(name, "cap") == 0) {
        return cbm_type_builtin(ctx->arena, "int");
    }

    // delete -> void (no return)
    if (strcmp(name, "delete") == 0) {
        return cbm_type_unknown();
    }

    return cbm_type_unknown();
}

// --- go_lookup_field: struct field lookup with embedding recursion ---

static const CBMType* go_lookup_field(GoLSPContext* ctx,
    const char* type_qn, const char* field_name, int depth) {
    if (!type_qn || !field_name || depth > 5) return NULL;

    const CBMRegisteredType* rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt) return NULL;

    // Follow alias chain
    if (rt->alias_of) return go_lookup_field(ctx, rt->alias_of, field_name, depth + 1);

    // Direct field lookup
    if (rt->field_names) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], field_name) == 0 && rt->field_types && rt->field_types[i]) {
                return rt->field_types[i];
            }
        }
    }

    // Promoted fields from embedded types
    if (rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            const CBMType* f = go_lookup_field(ctx, rt->embedded_types[i], field_name, depth + 1);
            if (f) return f;
        }
    }

    return NULL;
}

// --- go_lookup_field_or_method: method sets + embedding ---

static const CBMRegisteredFunc* go_lookup_field_or_method_depth(GoLSPContext* ctx,
    const char* type_qn, const char* member_name, int depth) {
    if (!type_qn || !member_name) return NULL;
    if (depth > CBM_LSP_MAX_LOOKUP_DEPTH) return NULL;

    // Direct method lookup
    const CBMRegisteredFunc* f = cbm_registry_lookup_method(ctx->registry, type_qn, member_name);
    if (f) return f;

    const CBMRegisteredType* rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (rt) {
        // Follow type alias chain
        if (rt->alias_of) {
            f = go_lookup_field_or_method_depth(ctx, rt->alias_of, member_name, depth + 1);
            if (f) return f;
        }

        // Check embedded types (promoted methods)
        if (rt->embedded_types) {
            for (int i = 0; rt->embedded_types[i]; i++) {
                f = go_lookup_field_or_method_depth(ctx, rt->embedded_types[i], member_name, depth + 1);
                if (f) return f;
            }
        }
    }

    return NULL;
}

const CBMRegisteredFunc* go_lookup_field_or_method(GoLSPContext* ctx,
    const char* type_qn, const char* member_name) {
    return go_lookup_field_or_method_depth(ctx, type_qn, member_name, 0);
}

// --- go_process_statement: bind variables from statements ---

void go_process_statement(GoLSPContext* ctx, TSNode node) {
    if (ts_node_is_null(node)) return;
    const char* kind = ts_node_type(node);

    // short_var_declaration: a, b := expr
    if (strcmp(kind, "short_var_declaration") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (ts_node_is_null(left) || ts_node_is_null(right)) return;

        const CBMType* rhs_type = NULL;

        // Check if RHS is an expression_list (multiple values)
        if (strcmp(ts_node_type(right), "expression_list") == 0) {
            uint32_t rhs_count = ts_node_named_child_count(right);
            if (rhs_count == 1) {
                // Single expression that might return a tuple (multi-return)
                rhs_type = go_eval_expr_type(ctx, ts_node_named_child(right, 0));
            }
        } else {
            rhs_type = go_eval_expr_type(ctx, right);
        }

        // Bind left-hand side variables
        if (strcmp(ts_node_type(left), "expression_list") == 0) {
            uint32_t lhs_count = ts_node_named_child_count(left);
            for (uint32_t i = 0; i < lhs_count; i++) {
                TSNode lhs_var = ts_node_named_child(left, i);
                if (strcmp(ts_node_type(lhs_var), "identifier") != 0) continue;
                char* var_name = lsp_node_text(ctx, lhs_var);
                if (!var_name || strcmp(var_name, "_") == 0) continue;

                const CBMType* var_type = cbm_type_unknown();
                if (rhs_type) {
                    if (rhs_type->kind == CBM_TYPE_TUPLE && (int)i < rhs_type->data.tuple.count) {
                        var_type = rhs_type->data.tuple.elems[i];
                    } else if (i == 0) {
                        var_type = rhs_type;
                    }
                }
                cbm_scope_bind(ctx->current_scope, var_name, var_type);
            }
        } else if (strcmp(ts_node_type(left), "identifier") == 0) {
            char* var_name = lsp_node_text(ctx, left);
            if (var_name && strcmp(var_name, "_") != 0 && rhs_type) {
                cbm_scope_bind(ctx->current_scope, var_name, rhs_type);
            }
        }
        return;
    }

    // var_spec: var x Type = expr  OR  var a, b, c Type = expr
    if (strcmp(kind, "var_spec") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        TSNode value_node = ts_node_child_by_field_name(node, "value", 5);

        const CBMType* var_type = cbm_type_unknown();
        if (!ts_node_is_null(type_node)) {
            var_type = go_parse_type_node(ctx, type_node);
        } else if (!ts_node_is_null(value_node)) {
            if (strcmp(ts_node_type(value_node), "expression_list") == 0 &&
                ts_node_named_child_count(value_node) > 0) {
                var_type = go_eval_expr_type(ctx, ts_node_named_child(value_node, 0));
            } else {
                var_type = go_eval_expr_type(ctx, value_node);
            }
        }

        // Bind all name identifiers (handles: var a, b, c int)
        uint32_t vnc = ts_node_child_count(node);
        for (uint32_t vi = 0; vi < vnc; vi++) {
            TSNode ch = ts_node_child(node, vi);
            if (ts_node_is_null(ch) || !ts_node_is_named(ch)) continue;
            if (strcmp(ts_node_type(ch), "identifier") == 0) {
                char* var_name = lsp_node_text(ctx, ch);
                if (var_name && strcmp(var_name, "_") != 0) {
                    cbm_scope_bind(ctx->current_scope, var_name, var_type);
                }
            }
        }
        return;
    }

    // const_spec: const x Type = expr  OR  const x = expr
    if (strcmp(kind, "const_spec") == 0) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        TSNode value_node = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(name_node)) return;

        const CBMType* const_type = cbm_type_unknown();
        if (!ts_node_is_null(type_node)) {
            const_type = go_parse_type_node(ctx, type_node);
        } else if (!ts_node_is_null(value_node)) {
            // For expression_list values, evaluate first element
            if (strcmp(ts_node_type(value_node), "expression_list") == 0 &&
                ts_node_named_child_count(value_node) > 0) {
                const_type = go_eval_expr_type(ctx, ts_node_named_child(value_node, 0));
            } else {
                const_type = go_eval_expr_type(ctx, value_node);
            }
        }

        if (strcmp(ts_node_type(name_node), "identifier") == 0) {
            char* name = lsp_node_text(ctx, name_node);
            if (name && strcmp(name, "_") != 0)
                cbm_scope_bind(ctx->current_scope, name, const_type);
        }
        return;
    }

    // range_clause: for k, v := range container
    if (strcmp(kind, "range_clause") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (ts_node_is_null(right)) return;

        const CBMType* container_type = go_eval_expr_type(ctx, right);

        // Determine key and value types based on container type
        const CBMType* key_type = cbm_type_unknown();
        const CBMType* val_type = cbm_type_unknown();

        if (container_type) {
            switch (container_type->kind) {
            case CBM_TYPE_SLICE:
                key_type = cbm_type_builtin(ctx->arena, "int");
                val_type = container_type->data.slice.elem;
                break;
            case CBM_TYPE_MAP:
                key_type = container_type->data.map.key;
                val_type = container_type->data.map.value;
                break;
            case CBM_TYPE_CHANNEL:
                val_type = container_type->data.channel.elem;
                break;
            default:
                if (container_type->kind == CBM_TYPE_BUILTIN &&
                    strcmp(container_type->data.builtin.name, "string") == 0) {
                    key_type = cbm_type_builtin(ctx->arena, "int");
                    val_type = cbm_type_builtin(ctx->arena, "rune");
                }
                break;
            }
        }

        if (!ts_node_is_null(left) && strcmp(ts_node_type(left), "expression_list") == 0) {
            uint32_t lhs_count = ts_node_named_child_count(left);
            for (uint32_t i = 0; i < lhs_count; i++) {
                TSNode var_node = ts_node_named_child(left, i);
                if (strcmp(ts_node_type(var_node), "identifier") != 0) continue;
                char* var_name = lsp_node_text(ctx, var_node);
                if (!var_name || strcmp(var_name, "_") == 0) continue;
                cbm_scope_bind(ctx->current_scope, var_name, i == 0 ? key_type : val_type);
            }
        }
        return;
    }

    // type_switch_statement is handled in resolve_calls_in_node, not here
    // (needs per-case scope narrowing, not just variable binding)
}

// --- Emit a resolved call ---

static void emit_resolved_call(GoLSPContext* ctx, const char* callee_qn, const char* strategy, float confidence) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn) return;

    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    rc.reason = NULL;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

// Emit a diagnostic for an unresolved call (confidence 0.0).
static void emit_unresolved_call(GoLSPContext* ctx, const char* expr_text, const char* reason) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn) return;

    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

// --- Walk call expressions and resolve them ---

static void resolve_calls_in_node_inner(GoLSPContext* ctx, TSNode node) {
    if (ts_node_is_null(node)) return;
    const char* kind = ts_node_type(node);

    // Process statements to build scope
    go_process_statement(ctx, node);

    // Resolve call expressions
    if (strcmp(kind, "call_expression") == 0) {
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        if (!ts_node_is_null(func_node)) {
            const char* fk = ts_node_type(func_node);

            // selector_expression: obj.Method() or pkg.Func()
            if (strcmp(fk, "selector_expression") == 0) {
                TSNode operand = ts_node_child_by_field_name(func_node, "operand", 7);
                TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
                if (!ts_node_is_null(operand) && !ts_node_is_null(field)) {
                    char* field_name = lsp_node_text(ctx, field);

                    // Check if operand is a package import
                    if (strcmp(ts_node_type(operand), "identifier") == 0) {
                        char* pkg_name = lsp_node_text(ctx, operand);
                        if (pkg_name) {
                            const char* pkg_qn = resolve_import(ctx, pkg_name);
                            if (pkg_qn && field_name) {
                                const CBMRegisteredFunc* f = cbm_registry_lookup_symbol(ctx->registry, pkg_qn, field_name);
                                if (f) {
                                    emit_resolved_call(ctx, f->qualified_name, "lsp_direct", 0.95f);
                                    goto recurse;
                                }
                                // Package found but symbol not in registry
                                emit_unresolved_call(ctx,
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", pkg_name, field_name),
                                    "symbol_not_in_registry");
                                goto recurse;
                            }
                        }
                    }

                    // Type-based method dispatch
                    if (field_name) {
                        const CBMType* recv_type = go_eval_expr_type(ctx, operand);
                        const CBMType* base = recv_type;
                        if (base && base->kind == CBM_TYPE_POINTER) base = cbm_type_deref(base);

                        if (base && base->kind == CBM_TYPE_NAMED) {
                            const CBMRegisteredFunc* method = go_lookup_field_or_method(ctx,
                                base->data.named.qualified_name, field_name);
                            if (method) {
                                const char* strategy = "lsp_type_dispatch";
                                if (method->receiver_type &&
                                    strcmp(method->receiver_type, base->data.named.qualified_name) != 0) {
                                    strategy = "lsp_embed_dispatch";
                                }
                                emit_resolved_call(ctx, method->qualified_name, strategy, 0.95f);
                                goto recurse;
                            }
                        }

                        // Interface dispatch: NAMED type that is an interface, or bare INTERFACE type
                        if (base && field_name) {
                            bool is_iface = (base->kind == CBM_TYPE_INTERFACE);
                            const char* iface_qn = NULL;
                            if (!is_iface && base->kind == CBM_TYPE_NAMED) {
                                const CBMRegisteredType* rt = cbm_registry_lookup_type(ctx->registry,
                                    base->data.named.qualified_name);
                                if (rt && rt->is_interface) {
                                    is_iface = true;
                                    iface_qn = base->data.named.qualified_name;
                                }
                            }
                            if (is_iface) {
                                // Try interface satisfaction: find concrete types implementing this interface
                                const CBMRegisteredType* iface_rt = iface_qn ?
                                    cbm_registry_lookup_type(ctx->registry, iface_qn) : NULL;
                                if (iface_rt && iface_rt->method_names && iface_rt->method_names[0]) {
                                    // Count interface methods
                                    int iface_mcount = 0;
                                    while (iface_rt->method_names[iface_mcount]) iface_mcount++;

                                    // Scan all registered types for satisfaction
                                    const char* sole_impl_qn = NULL;
                                    int impl_count = 0;
                                    // Skip stdlib types when interface is from a project package
                                    bool iface_is_project = iface_qn && strchr(iface_qn, '/') != NULL;
                                    for (int ti = 0; ti < ctx->registry->type_count && impl_count < 2; ti++) {
                                        const CBMRegisteredType* cand = &ctx->registry->types[ti];
                                        if (cand->is_interface) continue;
                                        if (!cand->qualified_name) continue;
                                        if (cand->alias_of) continue;
                                        // For project interfaces, skip stdlib candidates (no '/' in QN)
                                        if (iface_is_project && !strchr(cand->qualified_name, '/')) continue;

                                        // Check if candidate has all interface methods
                                        bool satisfies = true;
                                        for (int mi = 0; mi < iface_mcount; mi++) {
                                            if (!cbm_registry_lookup_method(ctx->registry,
                                                    cand->qualified_name, iface_rt->method_names[mi])) {
                                                satisfies = false;
                                                break;
                                            }
                                        }
                                        if (satisfies) {
                                            sole_impl_qn = cand->qualified_name;
                                            impl_count++;
                                        }
                                    }

                                    if (impl_count == 1 && sole_impl_qn) {
                                        // Single implementer: resolve to concrete method
                                        const CBMRegisteredFunc* concrete_method =
                                            cbm_registry_lookup_method(ctx->registry, sole_impl_qn, field_name);
                                        if (concrete_method) {
                                            // Sole-implementer interface dispatch is an unambiguous
                                            // resolution (exactly one concrete method); rank it at least
                                            // as high as a direct type dispatch (0.95) so the concrete
                                            // `Type.method` wins over the interface-method type_dispatch
                                            // for the same call site.
                                            emit_resolved_call(ctx, concrete_method->qualified_name,
                                                "lsp_interface_resolve", 0.95f);
                                            goto recurse;
                                        }
                                    }
                                }

                                // Fallback: generic interface dispatch
                                emit_resolved_call(ctx,
                                    cbm_arena_sprintf(ctx->arena, "%s.%s",
                                        iface_qn ? iface_qn : "interface", field_name),
                                    "lsp_interface_dispatch", 0.85f);
                                goto recurse;
                            }
                        }

                        // Type resolved to NAMED but neither method nor interface matched
                        if (base && base->kind == CBM_TYPE_NAMED) {
                            emit_unresolved_call(ctx,
                                cbm_arena_sprintf(ctx->arena, "%s.%s",
                                    base->data.named.qualified_name, field_name),
                                "method_not_found");
                        } else if (cbm_type_is_unknown(recv_type)) {
                            char* operand_text = lsp_node_text(ctx, operand);
                            emit_unresolved_call(ctx,
                                cbm_arena_sprintf(ctx->arena, "%s.%s",
                                    operand_text ? operand_text : "?", field_name),
                                "unknown_receiver_type");
                        }
                    }
                }
            }

            // Direct function call: FuncName()
            if (strcmp(fk, "identifier") == 0) {
                char* name = lsp_node_text(ctx, func_node);
                if (name && !is_go_builtin_func(name)) {
                    // Package-local function
                    const CBMRegisteredFunc* f = cbm_registry_lookup_symbol(ctx->registry, ctx->package_qn, name);
                    if (f) {
                        emit_resolved_call(ctx, f->qualified_name, "lsp_direct", 0.95f);
                    } else {
                        emit_unresolved_call(ctx, name, "function_not_in_registry");
                    }
                }
            }
        }
    }

recurse:;
    // Push scope for blocks and statements that introduce variables
    bool push_scope = (strcmp(kind, "block") == 0 ||
                       strcmp(kind, "if_statement") == 0 ||
                       strcmp(kind, "for_statement") == 0 ||
                       strcmp(kind, "expression_switch_statement") == 0);
    if (push_scope) {
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    }

    // Process initializer field for if/for/switch statements.
    // Go patterns: if err := f(); ..., for i := 0; ..., switch v := x; v { ... }
    if (strcmp(kind, "if_statement") == 0 ||
        strcmp(kind, "for_statement") == 0 ||
        strcmp(kind, "expression_switch_statement") == 0) {
        TSNode init = ts_node_child_by_field_name(node, "initializer", 11);
        if (!ts_node_is_null(init)) {
            go_process_statement(ctx, init);
        }
    }

    // Process for_statement range_clause before recursing into body
    if (strcmp(kind, "for_statement") == 0) {
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_null(child) && ts_node_is_named(child) &&
                strcmp(ts_node_type(child), "range_clause") == 0) {
                go_process_statement(ctx, child);
                break;
            }
        }
    }

    // Type switch: switch a := expr.(type) { case *T: a.Method() }
    // Go tree-sitter structure: type_switch_statement has flat children:
    //   expression_list (contains var name "a")
    //   identifier (operand "animal")
    //   type_case (case clause, may repeat)
    if (strcmp(kind, "type_switch_statement") == 0) {
        const char* switch_var = NULL;
        // Pass 1: find switch variable and operand type
        uint32_t nc2 = ts_node_child_count(node);
        bool found_assign = false;
        for (uint32_t i = 0; i < nc2; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child)) continue;
            const char* ck = ts_node_type(child);

            // expression_list before := is the LHS (variable name)
            if (!found_assign && strcmp(ck, "expression_list") == 0) {
                TSNode var_node = ts_node_named_child(child, 0);
                if (!ts_node_is_null(var_node) && strcmp(ts_node_type(var_node), "identifier") == 0)
                    switch_var = lsp_node_text(ctx, var_node);
            }
            // := operator marks the assignment
            if (!ts_node_is_named(child)) {
                char* tok = lsp_node_text(ctx, child);
                if (tok && strcmp(tok, ":=") == 0) found_assign = true;
            }
            // identifier after := is the operand being type-switched
            if (found_assign && strcmp(ck, "identifier") == 0) {
                (void)go_eval_expr_type(ctx, child);
            }
        }

        // Pass 2: process each type_case with narrowed scope
        for (uint32_t i = 0; i < nc2; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
            if (strcmp(ts_node_type(child), "type_case") != 0) continue;

            CBMScope* saved = ctx->current_scope;
            ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

            // Find case type and bind switch variable
            uint32_t cc_count = ts_node_child_count(child);
            for (uint32_t j = 0; j < cc_count; j++) {
                TSNode cc_child = ts_node_child(child, j);
                if (ts_node_is_null(cc_child) || !ts_node_is_named(cc_child)) continue;
                const char* cc_kind = ts_node_type(cc_child);
                if (strcmp(cc_kind, "type_identifier") == 0 ||
                    strcmp(cc_kind, "qualified_type") == 0 ||
                    strcmp(cc_kind, "pointer_type") == 0 ||
                    strcmp(cc_kind, "slice_type") == 0) {
                    if (switch_var) {
                        cbm_scope_bind(ctx->current_scope, switch_var,
                            go_parse_type_node(ctx, cc_child));
                    }
                    break;
                }
            }

            // Recurse into case body statements
            for (uint32_t j = 0; j < cc_count; j++) {
                TSNode cc_child = ts_node_child(child, j);
                if (ts_node_is_null(cc_child) || !ts_node_is_named(cc_child)) continue;
                const char* cc_kind = ts_node_type(cc_child);
                // Skip type nodes, process everything else (expression_statement, etc.)
                if (strcmp(cc_kind, "type_identifier") == 0 ||
                    strcmp(cc_kind, "qualified_type") == 0 ||
                    strcmp(cc_kind, "pointer_type") == 0 ||
                    strcmp(cc_kind, "slice_type") == 0) continue;
                resolve_calls_in_node(ctx, cc_child);
            }

            ctx->current_scope = saved;
        }

        if (push_scope) ctx->current_scope = cbm_scope_pop(ctx->current_scope);
        return;
    }

    // Select statement: each communication_case gets its own scope
    // case msg := <-ch:  →  receive_statement has left (vars) + unary_expression (<-ch)
    if (strcmp(kind, "select_statement") == 0) {
        uint32_t nc3 = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc3; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
            const char* ck3 = ts_node_type(child);

            if (strcmp(ck3, "communication_case") == 0 || strcmp(ck3, "default_case") == 0) {
                CBMScope* saved = ctx->current_scope;
                ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

                // Process children: receive_statement binds vars, then recurse body
                uint32_t cc_count = ts_node_child_count(child);
                for (uint32_t j = 0; j < cc_count; j++) {
                    TSNode cc_child = ts_node_child(child, j);
                    if (ts_node_is_null(cc_child) || !ts_node_is_named(cc_child)) continue;
                    const char* cc_kind = ts_node_type(cc_child);

                    if (strcmp(cc_kind, "receive_statement") == 0) {
                        // receive_statement: left := <-right
                        // tree-sitter Go: right field is the channel expression (after <-)
                        // The receive value type is the channel's element type
                        TSNode left = ts_node_child_by_field_name(cc_child, "left", 4);
                        TSNode right = ts_node_child_by_field_name(cc_child, "right", 5);
                        if (!ts_node_is_null(right)) {
                            const CBMType* right_type = go_eval_expr_type(ctx, right);
                            // right_type might be a channel (if right is the channel)
                            // or already the elem type (if right is a <-ch unary expr)
                            const CBMType* recv_type = right_type;
                            if (right_type && right_type->kind == CBM_TYPE_CHANNEL) {
                                recv_type = right_type->data.channel.elem;
                            }
                            if (!ts_node_is_null(left)) {
                                if (strcmp(ts_node_type(left), "expression_list") == 0) {
                                    uint32_t lhs_count = ts_node_named_child_count(left);
                                    for (uint32_t k = 0; k < lhs_count; k++) {
                                        TSNode var_node = ts_node_named_child(left, k);
                                        if (strcmp(ts_node_type(var_node), "identifier") != 0) continue;
                                        char* var_name = lsp_node_text(ctx, var_node);
                                        if (!var_name || strcmp(var_name, "_") == 0) continue;
                                        if (k == 0) {
                                            cbm_scope_bind(ctx->current_scope, var_name, recv_type);
                                        } else {
                                            // second var is the ok bool
                                            cbm_scope_bind(ctx->current_scope, var_name,
                                                cbm_type_builtin(ctx->arena, "bool"));
                                        }
                                    }
                                } else if (strcmp(ts_node_type(left), "identifier") == 0) {
                                    char* var_name = lsp_node_text(ctx, left);
                                    if (var_name && strcmp(var_name, "_") != 0) {
                                        cbm_scope_bind(ctx->current_scope, var_name, recv_type);
                                    }
                                }
                            }
                        }
                    } else {
                        // Body statements
                        resolve_calls_in_node(ctx, cc_child);
                    }
                }

                ctx->current_scope = saved;
            }
        }
        if (push_scope) ctx->current_scope = cbm_scope_pop(ctx->current_scope);
        return;
    }

    // Recurse into children via a cursor (O(n)); ts_node_child(node,i) is O(i)
    // → O(n²) on a wide node.
    {
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                resolve_calls_in_node(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }

    if (push_scope) {
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
    }
}

// --- Process a single function body ---

static void process_function(GoLSPContext* ctx, TSNode func_node) {
    // Set enclosing function QN
    TSNode name_node = ts_node_child_by_field_name(func_node, "name", 4);
    if (ts_node_is_null(name_node)) return;

    char* func_name = lsp_node_text(ctx, name_node);
    if (!func_name || !func_name[0]) return;

    // For methods, the enclosing-function QN must include the receiver type
    // (package.Type.Method), matching how the textual extractor and the
    // registry qualify the method. Building it as package.Method (no receiver)
    // here made the LSP-resolved call's caller_qn disagree with the textual
    // call's enclosing_func_qn, so cbm_pipeline_find_lsp_resolution never
    // joined them — every call inside a method body silently lost its
    // type-aware LSP strategy. Derive the bare receiver type name the same way
    // the receiver binding below does.
    char* recv_type_name = NULL;
    {
        TSNode recv0 = ts_node_child_by_field_name(func_node, "receiver", 8);
        if (!ts_node_is_null(recv0)) {
            uint32_t rnc0 = ts_node_child_count(recv0);
            for (uint32_t i = 0; i < rnc0 && !recv_type_name; i++) {
                TSNode rp = ts_node_child(recv0, i);
                if (ts_node_is_null(rp) || !ts_node_is_named(rp)) continue;
                if (strcmp(ts_node_type(rp), "parameter_declaration") != 0) continue;
                TSNode rtype = ts_node_child_by_field_name(rp, "type", 4);
                if (ts_node_is_null(rtype)) continue;
                // Unwrap a pointer receiver (*Type) to the bare type identifier.
                const char* rtk = ts_node_type(rtype);
                if (strcmp(rtk, "pointer_type") == 0 && ts_node_named_child_count(rtype) > 0) {
                    rtype = ts_node_named_child(rtype, 0);
                }
                char* tn = lsp_node_text(ctx, rtype);
                if (tn && tn[0]) recv_type_name = tn;
            }
        }
    }

    if (recv_type_name) {
        ctx->enclosing_func_qn =
            cbm_arena_sprintf(ctx->arena, "%s.%s.%s", ctx->package_qn, recv_type_name, func_name);
    } else {
        ctx->enclosing_func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, func_name);
    }

    // Push function scope
    CBMScope* saved_scope = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

    // Bind parameters into scope (including variadic)
    TSNode params = ts_node_child_by_field_name(func_node, "parameters", 10);
    if (!ts_node_is_null(params)) {
        uint32_t nc = ts_node_child_count(params);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode param = ts_node_child(params, i);
            if (ts_node_is_null(param) || !ts_node_is_named(param)) continue;
            const char* pk = ts_node_type(param);

            bool is_variadic = (strcmp(pk, "variadic_parameter_declaration") == 0);
            if (!is_variadic && strcmp(pk, "parameter_declaration") != 0) continue;

            // Get type
            TSNode type_node = ts_node_child_by_field_name(param, "type", 4);
            const CBMType* param_type = go_parse_type_node(ctx, type_node);

            // Variadic: ...T is []T in the function body
            if (is_variadic && param_type) {
                param_type = cbm_type_slice(ctx->arena, param_type);
            }

            // Get name(s) — Go allows multiple names per declaration: a, b int
            uint32_t pnc = ts_node_child_count(param);
            for (uint32_t j = 0; j < pnc; j++) {
                TSNode child = ts_node_child(param, j);
                if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
                if (strcmp(ts_node_type(child), "identifier") == 0) {
                    char* pname = lsp_node_text(ctx, child);
                    if (pname && strcmp(pname, "_") != 0) {
                        cbm_scope_bind(ctx->current_scope, pname, param_type);
                    }
                }
            }
        }
    }

    // Bind named return values into scope
    TSNode result_node = ts_node_child_by_field_name(func_node, "result", 6);
    if (!ts_node_is_null(result_node)) {
        // result can be a parameter_list (named returns) or a simple type
        const char* rk = ts_node_type(result_node);
        if (strcmp(rk, "parameter_list") == 0) {
            uint32_t rnc = ts_node_child_count(result_node);
            for (uint32_t i = 0; i < rnc; i++) {
                TSNode rparam = ts_node_child(result_node, i);
                if (ts_node_is_null(rparam) || !ts_node_is_named(rparam)) continue;
                if (strcmp(ts_node_type(rparam), "parameter_declaration") != 0) continue;

                TSNode rtype = ts_node_child_by_field_name(rparam, "type", 4);
                const CBMType* ret_type = go_parse_type_node(ctx, rtype);

                uint32_t rpnc = ts_node_child_count(rparam);
                for (uint32_t j = 0; j < rpnc; j++) {
                    TSNode child = ts_node_child(rparam, j);
                    if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
                    if (strcmp(ts_node_type(child), "identifier") == 0) {
                        char* rname = lsp_node_text(ctx, child);
                        if (rname && strcmp(rname, "_") != 0) {
                            cbm_scope_bind(ctx->current_scope, rname, ret_type);
                        }
                    }
                }
            }
        }
    }

    // Bind receiver for methods
    TSNode recv = ts_node_child_by_field_name(func_node, "receiver", 8);
    if (!ts_node_is_null(recv)) {
        // receiver is a parameter_list with one parameter_declaration
        uint32_t rnc = ts_node_child_count(recv);
        for (uint32_t i = 0; i < rnc; i++) {
            TSNode rp = ts_node_child(recv, i);
            if (ts_node_is_null(rp) || !ts_node_is_named(rp)) continue;
            if (strcmp(ts_node_type(rp), "parameter_declaration") != 0) continue;

            TSNode rtype = ts_node_child_by_field_name(rp, "type", 4);
            const CBMType* recv_type = go_parse_type_node(ctx, rtype);

            // Find receiver name
            uint32_t rpnc = ts_node_child_count(rp);
            for (uint32_t j = 0; j < rpnc; j++) {
                TSNode rc = ts_node_child(rp, j);
                if (!ts_node_is_null(rc) && ts_node_is_named(rc) &&
                    strcmp(ts_node_type(rc), "identifier") == 0) {
                    char* rname = lsp_node_text(ctx, rc);
                    if (rname && strcmp(rname, "_") != 0) {
                        cbm_scope_bind(ctx->current_scope, rname, recv_type);
                    }
                    break;
                }
            }
        }
    }

    // Walk function body
    TSNode body = ts_node_child_by_field_name(func_node, "body", 4);
    if (!ts_node_is_null(body)) {
        resolve_calls_in_node(ctx, body);
    }

    // Restore scope
    ctx->current_scope = saved_scope;
}

// --- Process entire file ---

void go_lsp_process_file(GoLSPContext* ctx, TSNode root) {
    if (ts_node_is_null(root)) return;

    // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
    uint32_t kn = 0;
    TSNode* kids = cbm_lsp_collect_children(ctx->arena, root, &kn);

    // Pass 1: Bind package-level var/const declarations into root scope.
    // Must happen before processing functions so all globals are visible.
    for (uint32_t i = 0; i < kn; i++) {
        TSNode child = kids[i];
        const char* kind = ts_node_type(child);

        if (strcmp(kind, "var_declaration") == 0 || strcmp(kind, "const_declaration") == 0) {
            uint32_t vnc = ts_node_child_count(child);
            for (uint32_t j = 0; j < vnc; j++) {
                TSNode spec = ts_node_child(child, j);
                if (ts_node_is_null(spec) || !ts_node_is_named(spec)) continue;
                go_process_statement(ctx, spec);
            }
        }
    }

    // Pass 2: Process function/method bodies
    for (uint32_t i = 0; i < kn; i++) {
        TSNode child = kids[i];
        const char* kind = ts_node_type(child);

        if (strcmp(kind, "function_declaration") == 0 ||
            strcmp(kind, "method_declaration") == 0) {
            process_function(ctx, child);
        }
    }
}

// --- Helper: parse Go return type text into CBMType ---

const CBMType* cbm_parse_return_type_text(CBMArena* a, const char* text, const char* module_qn) {
    if (!text || !text[0]) return cbm_type_unknown();

    // Pointer: *Foo
    if (text[0] == '*') {
        return cbm_type_pointer(a, cbm_parse_return_type_text(a, text + 1, module_qn));
    }

    // Slice: []Foo
    if (text[0] == '[' && text[1] == ']') {
        return cbm_type_slice(a, cbm_parse_return_type_text(a, text + 2, module_qn));
    }

    // Builtin types
    static const char* builtins[] = {
        "int","int8","int16","int32","int64",
        "uint","uint8","uint16","uint32","uint64",
        "float32","float64","string","bool","byte","rune","error",
        "any","uintptr",
        NULL
    };
    for (const char** b = builtins; *b; b++) {
        if (strcmp(text, *b) == 0) return cbm_type_builtin(a, text);
    }

    // Named type: assume local to module
    return cbm_type_named(a, cbm_arena_sprintf(a, "%s.%s", module_qn, text));
}

// --- Entry point: build registry from file defs + run LSP ---

void cbm_run_go_lsp(CBMArena* arena, CBMFileResult* result,
    const char* source, int source_len, TSNode root) {

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    // Register Go stdlib types/functions
    cbm_go_stdlib_register(&reg, arena);

    const char* module_qn = result->module_qn;

    // Phase 1: Register all types and functions from the file's own definitions
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition* d = &result->defs.items[i];
        if (!d->qualified_name || !d->name) continue;

        // Register every type-like container (Class/Struct/Type/Interface/Enum/
        // Trait). Struct included so a Go `type T struct {...}` (now labelled
        // "Struct") is registered as a type and its methods/embedding resolve.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->name;
            rt.is_interface = d->label && strcmp(d->label, "Interface") == 0;

            // Populate embedded_types from base_classes (Go struct embedding)
            if (d->base_classes) {
                int bc_count = 0;
                while (d->base_classes[bc_count]) bc_count++;
                if (bc_count > 0) {
                    const char** embedded = (const char**)cbm_arena_alloc(arena, (bc_count + 1) * sizeof(const char*));
                    for (int j = 0; j < bc_count; j++) {
                        const char* bc = d->base_classes[j];
                        // Strip pointer prefix for embedded *Type
                        while (bc[0] == '*') bc++;
                        // Qualify the embedded type name
                        embedded[j] = cbm_arena_sprintf(arena, "%s.%s", module_qn, bc);
                    }
                    embedded[bc_count] = NULL;
                    rt.embedded_types = embedded;
                }
            }

            cbm_registry_add_type(&reg, rt);
        }

        // Register Function/Method nodes
        if (d->label && (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0)) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;

            // Build FUNC type from return_types
            const CBMType** ret_types = NULL;
            if (d->return_types) {
                int count = 0;
                while (d->return_types[count]) count++;
                if (count > 0) {
                    ret_types = (const CBMType**)cbm_arena_alloc(arena, (count + 1) * sizeof(const CBMType*));
                    for (int j = 0; j < count; j++) {
                        ret_types[j] = cbm_parse_return_type_text(arena, d->return_types[j], module_qn);
                    }
                    ret_types[count] = NULL;
                }
            } else if (d->return_type && d->return_type[0]) {
                // Fallback: single return_type string
                ret_types = (const CBMType**)cbm_arena_alloc(arena, 2 * sizeof(const CBMType*));
                ret_types[0] = cbm_parse_return_type_text(arena, d->return_type, module_qn);
                ret_types[1] = NULL;
            }

            // Build param types from param_types array
            const CBMType** param_types_arr = NULL;
            if (d->param_types) {
                int count = 0;
                while (d->param_types[count]) count++;
                if (count > 0) {
                    param_types_arr = (const CBMType**)cbm_arena_alloc(arena, (count + 1) * sizeof(const CBMType*));
                    for (int j = 0; j < count; j++) {
                        param_types_arr[j] = cbm_parse_return_type_text(arena, d->param_types[j], module_qn);
                    }
                    param_types_arr[count] = NULL;
                }
            }

            rf.signature = cbm_type_func(arena, d->param_names, param_types_arr, ret_types);

            // For methods, extract receiver type from receiver text
            if (strcmp(d->label, "Method") == 0 && d->receiver && d->receiver[0]) {
                // Receiver format: "(name *Type)" or "(name Type)"
                // Extract type name: skip parens, skip first identifier, strip *
                const char* r = d->receiver;
                while (*r == '(' || *r == ' ') r++;
                // Skip receiver name
                while (*r && *r != ' ' && *r != '*') r++;
                while (*r == ' ' || *r == '*') r++;
                // Now r points to the type name
                const char* end = r;
                while (*end && *end != ')' && *end != ' ') end++;
                if (end > r) {
                    char* type_name = cbm_arena_strndup(arena, r, end - r);
                    rf.receiver_type = cbm_arena_sprintf(arena, "%s.%s", module_qn, type_name);

                    // Also register this method under the type
                    const CBMRegisteredType* existing = cbm_registry_lookup_type(&reg, rf.receiver_type);
                    if (!existing) {
                        // Auto-create the type entry
                        CBMRegisteredType auto_type;
                        memset(&auto_type, 0, sizeof(auto_type));
                        auto_type.qualified_name = rf.receiver_type;
                        auto_type.short_name = type_name;
                        cbm_registry_add_type(&reg, auto_type);
                    }
                }
            }

            cbm_registry_add_func(&reg, rf);
        }
    }

    // Phase 1b: Scan AST for struct definitions to populate embedded_types
    {
        // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
        uint32_t rkn = 0;
        TSNode* rkids = cbm_lsp_collect_children(arena, root, &rkn);
        for (uint32_t i = 0; i < rkn; i++) {
            TSNode top = rkids[i];
            const char* tk = ts_node_type(top);
            // type_declaration contains type_spec children
            if (strcmp(tk, "type_declaration") != 0) continue;
            uint32_t td_nc = ts_node_child_count(top);
            for (uint32_t j = 0; j < td_nc; j++) {
                TSNode spec = ts_node_child(top, j);
                if (ts_node_is_null(spec) || !ts_node_is_named(spec)) continue;
                const char* spec_kind = ts_node_type(spec);

                // type_alias: type Foo = Bar
                if (strcmp(spec_kind, "type_alias") == 0) {
                    TSNode alias_name = ts_node_child_by_field_name(spec, "name", 4);
                    TSNode alias_type = ts_node_child_by_field_name(spec, "type", 4);
                    if (!ts_node_is_null(alias_name) && !ts_node_is_null(alias_type)) {
                        char* aname = cbm_node_text(arena, alias_name, source);
                        char* atarget = cbm_node_text(arena, alias_type, source);
                        if (aname && aname[0] && atarget && atarget[0]) {
                            const char* alias_type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, aname);
                            const char* alias_target_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, atarget);
                            bool found_a = false;
                            for (int ti = 0; ti < reg.type_count; ti++) {
                                if (reg.types[ti].qualified_name &&
                                    strcmp(reg.types[ti].qualified_name, alias_type_qn) == 0) {
                                    reg.types[ti].alias_of = alias_target_qn;
                                    found_a = true;
                                    break;
                                }
                            }
                            if (!found_a) {
                                CBMRegisteredType alias_rt;
                                memset(&alias_rt, 0, sizeof(alias_rt));
                                alias_rt.qualified_name = alias_type_qn;
                                alias_rt.short_name = aname;
                                alias_rt.alias_of = alias_target_qn;
                                cbm_registry_add_type(&reg, alias_rt);
                            }
                        }
                    }
                    continue;
                }

                if (strcmp(spec_kind, "type_spec") != 0) continue;

                TSNode name_node = ts_node_child_by_field_name(spec, "name", 4);
                TSNode type_node = ts_node_child_by_field_name(spec, "type", 4);
                if (ts_node_is_null(name_node) || ts_node_is_null(type_node)) continue;

                char* type_name = cbm_node_text(arena,name_node, source);
                if (!type_name || !type_name[0]) continue;
                const char* type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, type_name);

                // Interface type: extract method names for satisfaction checking
                if (strcmp(ts_node_type(type_node), "interface_type") == 0) {
                    const char* iface_methods[64];
                    int iface_method_count = 0;
                    uint32_t inl_nc = ts_node_named_child_count(type_node);
                    for (uint32_t k = 0; k < inl_nc && iface_method_count < 63; k++) {
                        TSNode child = ts_node_named_child(type_node, k);
                        if (ts_node_is_null(child)) continue;
                        const char* ck = ts_node_type(child);
                        // method_spec: MethodName(params) returns
                        if (strcmp(ck, "method_spec") == 0 || strcmp(ck, "method_elem") == 0) {
                            TSNode mname = ts_node_child_by_field_name(child, "name", 4);
                            if (!ts_node_is_null(mname)) {
                                char* mn = cbm_node_text(arena, mname, source);
                                if (mn && mn[0]) {
                                    iface_methods[iface_method_count++] = mn;
                                }
                            }
                        }
                    }
                    if (iface_method_count > 0) {
                        for (int ti = 0; ti < reg.type_count; ti++) {
                            if (!reg.types[ti].qualified_name ||
                                strcmp(reg.types[ti].qualified_name, type_qn) != 0) continue;
                            const char** names = (const char**)cbm_arena_alloc(arena,
                                (iface_method_count + 1) * sizeof(const char*));
                            for (int mi = 0; mi < iface_method_count; mi++) {
                                names[mi] = iface_methods[mi];
                            }
                            names[iface_method_count] = NULL;
                            reg.types[ti].method_names = names;
                            break;
                        }
                    }
                    continue;
                }

                if (strcmp(ts_node_type(type_node), "struct_type") != 0) continue;

                // Find field_declaration_list inside struct_type
                TSNode field_list = ts_node_child_by_field_name(type_node, "body", 4);
                if (ts_node_is_null(field_list)) {
                    // Some grammars use first named child
                    if (ts_node_named_child_count(type_node) > 0)
                        field_list = ts_node_named_child(type_node, 0);
                }
                if (ts_node_is_null(field_list)) continue;

                // Scan field_declarations for embeds and named fields
                const char* embeds[16];
                int embed_count = 0;
                const char* fld_names[64];
                const CBMType* fld_types[64];
                int fld_count = 0;

                // Create a temporary LSP context for parsing field types
                GoLSPContext tmp_ctx;
                memset(&tmp_ctx, 0, sizeof(tmp_ctx));
                tmp_ctx.arena = arena;
                tmp_ctx.source = source;
                tmp_ctx.source_len = (int)strlen(source);
                tmp_ctx.registry = &reg;
                tmp_ctx.package_qn = module_qn;

                uint32_t fl_nc = ts_node_child_count(field_list);
                for (uint32_t k = 0; k < fl_nc; k++) {
                    TSNode field = ts_node_child(field_list, k);
                    if (ts_node_is_null(field) || !ts_node_is_named(field)) continue;
                    if (strcmp(ts_node_type(field), "field_declaration") != 0) continue;

                    TSNode fname = ts_node_child_by_field_name(field, "name", 4);
                    TSNode ftype = ts_node_child_by_field_name(field, "type", 4);

                    if (ts_node_is_null(fname) && !ts_node_is_null(ftype)) {
                        // Embedded field: has type but no name
                        if (embed_count < 15) {
                            char* embed_text = cbm_node_text(arena, ftype, source);
                            if (embed_text && embed_text[0]) {
                                const char* et = embed_text;
                                while (*et == '*') et++;
                                embeds[embed_count++] = cbm_arena_sprintf(arena, "%s.%s", module_qn, et);
                            }
                        }
                    } else if (!ts_node_is_null(fname) && !ts_node_is_null(ftype) && fld_count < 63) {
                        // Named field: name + type
                        char* fn = cbm_node_text(arena, fname, source);
                        if (fn && fn[0]) {
                            fld_names[fld_count] = fn;
                            fld_types[fld_count] = go_parse_type_node(&tmp_ctx, ftype);
                            fld_count++;
                        }
                    }
                }

                // Find the registered type and update it
                for (int ti = 0; ti < reg.type_count; ti++) {
                    if (!reg.types[ti].qualified_name ||
                        strcmp(reg.types[ti].qualified_name, type_qn) != 0) continue;

                    if (embed_count > 0) {
                        const char** arr = (const char**)cbm_arena_alloc(arena,
                            (embed_count + 1) * sizeof(const char*));
                        for (int ei = 0; ei < embed_count; ei++) arr[ei] = embeds[ei];
                        arr[embed_count] = NULL;
                        reg.types[ti].embedded_types = arr;
                    }
                    if (fld_count > 0) {
                        const char** names = (const char**)cbm_arena_alloc(arena,
                            (fld_count + 1) * sizeof(const char*));
                        const CBMType** types = (const CBMType**)cbm_arena_alloc(arena,
                            (fld_count + 1) * sizeof(const CBMType*));
                        for (int fi = 0; fi < fld_count; fi++) {
                            names[fi] = fld_names[fi];
                            types[fi] = fld_types[fi];
                        }
                        names[fld_count] = NULL;
                        types[fld_count] = NULL;
                        reg.types[ti].field_names = names;
                        reg.types[ti].field_types = types;
                    }
                    break;
                }
            }
        }
    }

    // Phase 1c: Extract type parameters from generic function declarations
    extract_type_params_from_ast(arena, &reg, root, source, module_qn);

    // Phase 2: Build LSP context and run
    GoLSPContext lsp_ctx;
    go_lsp_init(&lsp_ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);

    // Add imports
    for (int i = 0; i < result->imports.count; i++) {
        CBMImport* imp = &result->imports.items[i];
        if (imp->local_name && imp->module_path) {
            go_lsp_add_import(&lsp_ctx, imp->local_name, imp->module_path);
        }
    }

    // Process the file
    go_lsp_process_file(&lsp_ctx, root);
}

// --- Cross-file LSP: parse source, build registry from defs, run LSP ---

// Helper: split "|"-separated string into array of CBMType*.
static const CBMType** split_pipe_types(CBMArena* a, const char* text, const char* module_qn) {
    if (!text || !text[0]) return NULL;

    // Count separators
    int count = 1;
    for (const char* p = text; *p; p++) {
        if (*p == '|') count++;
    }

    const CBMType** arr = (const CBMType**)cbm_arena_alloc(a, (count + 1) * sizeof(const CBMType*));
    if (!arr) return NULL;

    // Split and parse each type
    char* buf = cbm_arena_strdup(a, text);
    int idx = 0;
    char* start = buf;
    for (char* p = buf; ; p++) {
        if (*p == '|' || *p == '\0') {
            char save = *p;
            *p = '\0';
            if (start[0]) {
                arr[idx++] = cbm_parse_return_type_text(a, start, module_qn);
            }
            if (save == '\0') break;
            start = p + 1;
        }
    }
    arr[idx] = NULL;
    return idx > 0 ? arr : NULL;
}

// Helper: split "|"-separated string into array of const char*.
static const char** split_pipe_strings(CBMArena* a, const char* text) {
    if (!text || !text[0]) return NULL;

    int count = 1;
    for (const char* p = text; *p; p++) {
        if (*p == '|') count++;
    }

    const char** arr = (const char**)cbm_arena_alloc(a, (count + 1) * sizeof(const char*));
    if (!arr) return NULL;

    char* buf = cbm_arena_strdup(a, text);
    int idx = 0;
    char* start = buf;
    for (char* p = buf; ; p++) {
        if (*p == '|' || *p == '\0') {
            char save = *p;
            *p = '\0';
            if (start[0]) {
                arr[idx++] = cbm_arena_strdup(a, start);
            }
            if (save == '\0') break;
            start = p + 1;
        }
    }
    arr[idx] = NULL;
    return idx > 0 ? arr : NULL;
}

// Helper: parse "|"-separated "name:type" field definitions and populate a registered type.
// Format: "Binder:Binder|Name:string|Count:int"
// type_text is resolved relative to def_module_qn.
static void parse_field_defs_into_type(CBMArena* arena, CBMTypeRegistry* reg,
    const char* type_qn, const char* field_defs, const char* def_module_qn) {
    if (!field_defs || !field_defs[0] || !type_qn) return;

    // Count fields
    int count = 1;
    for (const char* p = field_defs; *p; p++) {
        if (*p == '|') count++;
    }
    if (count > 63) count = 63;

    const char** names = (const char**)cbm_arena_alloc(arena, (count + 1) * sizeof(const char*));
    const CBMType** types = (const CBMType**)cbm_arena_alloc(arena, (count + 1) * sizeof(const CBMType*));
    if (!names || !types) return;

    char* buf = cbm_arena_strdup(arena, field_defs);
    int idx = 0;
    char* start = buf;
    for (char* p = buf; ; p++) {
        if (*p == '|' || *p == '\0') {
            char save = *p;
            *p = '\0';
            // Parse "name:type"
            char* colon = NULL;
            for (char* q = start; *q; q++) {
                if (*q == ':') { colon = q; break; }
            }
            if (colon && idx < count) {
                *colon = '\0';
                names[idx] = cbm_arena_strdup(arena, start);
                types[idx] = cbm_parse_return_type_text(arena, colon + 1, def_module_qn);
                idx++;
            }
            if (save == '\0') break;
            start = p + 1;
        }
    }
    names[idx] = NULL;
    types[idx] = NULL;

    if (idx > 0) {
        // Find the registered type and update field info
        for (int ti = 0; ti < reg->type_count; ti++) {
            if (reg->types[ti].qualified_name &&
                strcmp(reg->types[ti].qualified_name, type_qn) == 0) {
                reg->types[ti].field_names = names;
                reg->types[ti].field_types = types;
                break;
            }
        }
    }
}

// --- Phase 1c: Extract type parameters from generic function declarations ---

// Helper: parse a tree-sitter type AST node with type param awareness.
// Like go_parse_type_node but checks type_identifier against type_param_names first,
// and works without a full GoLSPContext (uses raw arena/source/module_qn).
static const CBMType* parse_type_node_with_params(CBMArena* arena, TSNode node,
    const char* source, const char* module_qn, const char** type_param_names) {
    if (ts_node_is_null(node)) return cbm_type_unknown();

    const char* kind = ts_node_type(node);

    // type_identifier: check type params first, then resolve as named type
    if (strcmp(kind, "type_identifier") == 0) {
        char* name = cbm_node_text(arena, node, source);
        if (!name) return cbm_type_unknown();
        // Check type param names
        if (type_param_names) {
            for (int i = 0; type_param_names[i]; i++) {
                if (strcmp(name, type_param_names[i]) == 0) {
                    return cbm_type_type_param(arena, name);
                }
            }
        }
        // Builtin types
        if (strcmp(name, "int") == 0 || strcmp(name, "string") == 0 ||
            strcmp(name, "bool") == 0 || strcmp(name, "float64") == 0 ||
            strcmp(name, "float32") == 0 || strcmp(name, "byte") == 0 ||
            strcmp(name, "rune") == 0 || strcmp(name, "error") == 0 ||
            strcmp(name, "any") == 0 || strcmp(name, "int8") == 0 ||
            strcmp(name, "int16") == 0 || strcmp(name, "int32") == 0 ||
            strcmp(name, "int64") == 0 || strcmp(name, "uint") == 0 ||
            strcmp(name, "uint8") == 0 || strcmp(name, "uint16") == 0 ||
            strcmp(name, "uint32") == 0 || strcmp(name, "uint64") == 0 ||
            strcmp(name, "uintptr") == 0) {
            return cbm_type_builtin(arena, name);
        }
        return cbm_type_named(arena, cbm_arena_sprintf(arena, "%s.%s", module_qn, name));
    }

    // type_elem: wrapper in type_arguments
    if (strcmp(kind, "type_elem") == 0 && ts_node_named_child_count(node) > 0) {
        return parse_type_node_with_params(arena, ts_node_named_child(node, 0),
            source, module_qn, type_param_names);
    }

    // qualified_type: pkg.Type
    if (strcmp(kind, "qualified_type") == 0) {
        TSNode pkg_node = ts_node_child_by_field_name(node, "package", 7);
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(pkg_node) && !ts_node_is_null(name_node)) {
            char* pkg = cbm_node_text(arena, pkg_node, source);
            char* name = cbm_node_text(arena, name_node, source);
            if (pkg && name) {
                return cbm_type_named(arena, cbm_arena_sprintf(arena, "%s.%s", pkg, name));
            }
        }
        return cbm_type_unknown();
    }

    // pointer_type: *T
    if (strcmp(kind, "pointer_type") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        if (nc > 0)
            return cbm_type_pointer(arena, parse_type_node_with_params(arena,
                ts_node_named_child(node, nc - 1), source, module_qn, type_param_names));
        return cbm_type_unknown();
    }

    // slice_type: []T
    if (strcmp(kind, "slice_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0)
            elem = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        return cbm_type_slice(arena, parse_type_node_with_params(arena,
            elem, source, module_qn, type_param_names));
    }

    // array_type: [N]T
    if (strcmp(kind, "array_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0)
            elem = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        return cbm_type_slice(arena, parse_type_node_with_params(arena,
            elem, source, module_qn, type_param_names));
    }

    // map_type: map[K]V
    if (strcmp(kind, "map_type") == 0) {
        TSNode key = ts_node_child_by_field_name(node, "key", 3);
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        return cbm_type_map(arena,
            parse_type_node_with_params(arena, key, source, module_qn, type_param_names),
            parse_type_node_with_params(arena, value, source, module_qn, type_param_names));
    }

    // channel_type: chan T
    if (strcmp(kind, "channel_type") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value) && ts_node_named_child_count(node) > 0)
            value = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        char* text = cbm_node_text(arena, node, source);
        int dir = 0;
        if (text) {
            if (strncmp(text, "chan<-", 6) == 0 || strncmp(text, "chan <-", 7) == 0) dir = 1;
            else if (strncmp(text, "<-chan", 6) == 0 || strncmp(text, "<- chan", 7) == 0) dir = 2;
        }
        return cbm_type_channel(arena, parse_type_node_with_params(arena,
            value, source, module_qn, type_param_names), dir);
    }

    // function_type: func(T) R — full parsing with type param awareness
    if (strcmp(kind, "function_type") == 0) {
        TSNode params_node = ts_node_child_by_field_name(node, "parameters", 10);
        TSNode result_node = ts_node_child_by_field_name(node, "result", 6);

        const CBMType* param_types_arr[16];
        int pc = 0;
        if (!ts_node_is_null(params_node)) {
            uint32_t pnc = ts_node_child_count(params_node);
            for (uint32_t i = 0; i < pnc && pc < 15; i++) {
                TSNode child = ts_node_child(params_node, i);
                if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
                if (strcmp(ts_node_type(child), "parameter_declaration") == 0) {
                    TSNode pt = ts_node_child_by_field_name(child, "type", 4);
                    if (!ts_node_is_null(pt))
                        param_types_arr[pc++] = parse_type_node_with_params(arena,
                            pt, source, module_qn, type_param_names);
                }
            }
        }
        param_types_arr[pc] = NULL;

        const CBMType* ret_types_arr[16];
        int rc = 0;
        if (!ts_node_is_null(result_node)) {
            if (strcmp(ts_node_type(result_node), "parameter_list") == 0) {
                uint32_t rnc = ts_node_child_count(result_node);
                for (uint32_t i = 0; i < rnc && rc < 15; i++) {
                    TSNode child = ts_node_child(result_node, i);
                    if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
                    TSNode rt = ts_node_child_by_field_name(child, "type", 4);
                    if (ts_node_is_null(rt)) rt = child;
                    ret_types_arr[rc++] = parse_type_node_with_params(arena,
                        rt, source, module_qn, type_param_names);
                }
            } else {
                ret_types_arr[rc++] = parse_type_node_with_params(arena,
                    result_node, source, module_qn, type_param_names);
            }
        }
        ret_types_arr[rc] = NULL;

        const CBMType** pt = pc > 0 ? (const CBMType**)param_types_arr : NULL;
        const CBMType** rt = rc > 0 ? (const CBMType**)ret_types_arr : NULL;
        return cbm_type_func(arena, NULL, pt, rt);
    }

    // interface_type
    if (strcmp(kind, "interface_type") == 0) {
        CBMType* t = (CBMType*)cbm_arena_alloc(arena, sizeof(CBMType));
        memset(t, 0, sizeof(CBMType));
        t->kind = CBM_TYPE_INTERFACE;
        return t;
    }

    // parenthesized_type: (T)
    if (strcmp(kind, "parenthesized_type") == 0 && ts_node_named_child_count(node) > 0) {
        return parse_type_node_with_params(arena, ts_node_named_child(node, 0),
            source, module_qn, type_param_names);
    }

    // generic_type: Type[T1, T2]
    if (strcmp(kind, "generic_type") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node))
            return parse_type_node_with_params(arena, type_node, source, module_qn, type_param_names);
    }

    return cbm_type_unknown();
}

// Scan AST for generic function declarations and set type_param_names + re-parse
// return types on matching registered functions.
static void extract_type_params_from_ast(CBMArena* arena, CBMTypeRegistry* reg,
    TSNode root, const char* source, const char* module_qn) {

    // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
    uint32_t rkn = 0;
    TSNode* rkids = cbm_lsp_collect_children(arena, root, &rkn);
    for (uint32_t i = 0; i < rkn; i++) {
        TSNode top = rkids[i];
        if (strcmp(ts_node_type(top), "function_declaration") != 0) continue;

        // Check for type_parameter_list (field name: "type_parameters", 15 chars)
        TSNode tp_list = ts_node_child_by_field_name(top, "type_parameters", 15);
        if (ts_node_is_null(tp_list)) continue;

        // Get function name
        TSNode name_node = ts_node_child_by_field_name(top, "name", 4);
        if (ts_node_is_null(name_node)) continue;
        char* func_name = cbm_node_text(arena, name_node, source);
        if (!func_name || !func_name[0]) continue;

        // Extract type param names from type_parameter_declaration children
        const char* params[16];
        int param_count = 0;
        uint32_t tp_nc = ts_node_child_count(tp_list);
        for (uint32_t j = 0; j < tp_nc && param_count < 15; j++) {
            TSNode child = ts_node_child(tp_list, j);
            if (ts_node_is_null(child) || !ts_node_is_named(child)) continue;
            if (strcmp(ts_node_type(child), "type_parameter_declaration") != 0) continue;
            TSNode pname = ts_node_child_by_field_name(child, "name", 4);
            if (ts_node_is_null(pname)) {
                // Fallback: first type_identifier child
                uint32_t cc = ts_node_child_count(child);
                for (uint32_t k = 0; k < cc; k++) {
                    TSNode c = ts_node_child(child, k);
                    if (!ts_node_is_null(c) && ts_node_is_named(c) &&
                        strcmp(ts_node_type(c), "type_identifier") == 0) {
                        pname = c;
                        break;
                    }
                }
            }
            if (!ts_node_is_null(pname)) {
                char* pn = cbm_node_text(arena, pname, source);
                if (pn && pn[0]) {
                    params[param_count++] = cbm_arena_strdup(arena, pn);
                }
            }
        }
        if (param_count == 0) continue;
        params[param_count] = NULL;

        // Build arena-allocated type_param_names array
        const char** tp_names = (const char**)cbm_arena_alloc(arena, (param_count + 1) * sizeof(const char*));
        for (int j = 0; j <= param_count; j++) tp_names[j] = params[j];

        // Find the matching registered function and set type_param_names
        const char* func_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, func_name);
        for (int fi = 0; fi < reg->func_count; fi++) {
            if (reg->funcs[fi].qualified_name &&
                strcmp(reg->funcs[fi].qualified_name, func_qn) == 0) {
                reg->funcs[fi].type_param_names = tp_names;

                // Re-parse return types with type param awareness
                if (reg->funcs[fi].signature && reg->funcs[fi].signature->kind == CBM_TYPE_FUNC &&
                    reg->funcs[fi].signature->data.func.return_types) {
                    const CBMType** old_rets = reg->funcs[fi].signature->data.func.return_types;
                    int ret_count = 0;
                    while (old_rets[ret_count]) ret_count++;

                    // Check if any return type is a NAMED that matches a type param
                    bool needs_reparse = false;
                    for (int ri = 0; ri < ret_count && !needs_reparse; ri++) {
                        const CBMType* check = old_rets[ri];
                        // Unwrap slice/pointer to get inner named type
                        if (check->kind == CBM_TYPE_SLICE && check->data.slice.elem)
                            check = check->data.slice.elem;
                        else if (check->kind == CBM_TYPE_POINTER && check->data.pointer.elem)
                            check = check->data.pointer.elem;
                        if (check->kind == CBM_TYPE_NAMED) {
                            const char* qn = check->data.named.qualified_name;
                            const char* dot = strrchr(qn, '.');
                            const char* short_name = dot ? dot + 1 : qn;
                            for (int pi = 0; pi < param_count; pi++) {
                                if (strcmp(short_name, tp_names[pi]) == 0) {
                                    needs_reparse = true;
                                    break;
                                }
                            }
                        }
                    }

                    if (needs_reparse) {
                        TSNode result_node = ts_node_child_by_field_name(top, "result", 6);
                        if (!ts_node_is_null(result_node)) {
                            const CBMType** new_rets = (const CBMType**)cbm_arena_alloc(arena,
                                (ret_count + 1) * sizeof(const CBMType*));

                            if (strcmp(ts_node_type(result_node), "parameter_list") == 0) {
                                int idx = 0;
                                uint32_t rnc = ts_node_child_count(result_node);
                                for (uint32_t ri = 0; ri < rnc && idx < ret_count; ri++) {
                                    TSNode rchild = ts_node_child(result_node, ri);
                                    if (ts_node_is_null(rchild) || !ts_node_is_named(rchild)) continue;
                                    TSNode rtype = ts_node_child_by_field_name(rchild, "type", 4);
                                    if (ts_node_is_null(rtype)) rtype = rchild;
                                    new_rets[idx++] = parse_type_node_with_params(arena,
                                        rtype, source, module_qn, tp_names);
                                }
                                new_rets[idx] = NULL;
                            } else {
                                new_rets[0] = parse_type_node_with_params(arena,
                                    result_node, source, module_qn, tp_names);
                                new_rets[1] = NULL;
                            }

                            CBMType* new_sig = (CBMType*)cbm_arena_alloc(arena, sizeof(CBMType));
                            *new_sig = *(CBMType*)reg->funcs[fi].signature;
                            new_sig->data.func.return_types = new_rets;
                            reg->funcs[fi].signature = new_sig;
                        }
                    }
                }

                // Also re-parse param types with TYPE_PARAM awareness (for implicit generics)
                if (reg->funcs[fi].signature && reg->funcs[fi].signature->kind == CBM_TYPE_FUNC &&
                    reg->funcs[fi].signature->data.func.param_types) {
                    const CBMType** old_params = reg->funcs[fi].signature->data.func.param_types;
                    int pc = 0;
                    while (old_params[pc]) pc++;
                    // Re-parse from AST parameter list
                    TSNode params_node = ts_node_child_by_field_name(top, "parameters", 10);
                    if (!ts_node_is_null(params_node)) {
                        const CBMType** new_params = (const CBMType**)cbm_arena_alloc(arena,
                            (pc + 1) * sizeof(const CBMType*));
                        int idx = 0;
                        uint32_t pnc = ts_node_child_count(params_node);
                        for (uint32_t pi = 0; pi < pnc && idx < pc; pi++) {
                            TSNode pchild = ts_node_child(params_node, pi);
                            if (ts_node_is_null(pchild) || !ts_node_is_named(pchild)) continue;
                            if (strcmp(ts_node_type(pchild), "parameter_declaration") != 0) continue;
                            TSNode ptype = ts_node_child_by_field_name(pchild, "type", 4);
                            if (ts_node_is_null(ptype)) continue;
                            new_params[idx++] = parse_type_node_with_params(arena,
                                ptype, source, module_qn, tp_names);
                        }
                        new_params[idx] = NULL;
                        if (idx > 0) {
                            CBMType* sig = (CBMType*)reg->funcs[fi].signature;
                            if (sig != reg->funcs[fi].signature) {
                                // Already rebuilt — update in place
                                ((CBMType*)reg->funcs[fi].signature)->data.func.param_types = new_params;
                            } else {
                                CBMType* new_sig = (CBMType*)cbm_arena_alloc(arena, sizeof(CBMType));
                                *new_sig = *sig;
                                new_sig->data.func.param_types = new_params;
                                reg->funcs[fi].signature = new_sig;
                            }
                        }
                    }
                }
                break;
            }
        }
    }
}

// Forward: tree-sitter Go language (from lang_specs.c)
extern const TSLanguage* tree_sitter_go(void);

void cbm_run_go_lsp_cross(
    CBMArena* arena,
    const char* source, int source_len,
    const char* module_qn,
    CBMLSPDef* defs, int def_count,
    const char** import_names, const char** import_qns, int import_count,
    TSTree* cached_tree,
    CBMResolvedCallArray* out)
{
    if (!source || source_len <= 0) return;

    // 1. Use cached tree if available, otherwise parse fresh
    TSParser* parser = NULL;
    TSTree* tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser) return;
        ts_parser_set_language(parser, tree_sitter_go());
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        owns_tree = true;
        if (!tree) { ts_parser_delete(parser); return; }
    }
    TSNode root = ts_tree_root_node(tree);

    // 2. Build registry
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_go_stdlib_register(&reg, arena);

    // Register all defs (file-local + cross-file).
    // Perf: borrow strings from defs[] directly — they live in the
    // caller's arena which outlives this call, so cbm_arena_strdup is
    // wasted work. On kubernetes (~110k defs × 11k files × 5 strdups
    // ~= 6B mallocs) this alone saves ~90s of resolve wall time.
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef* d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label) continue;

        const char* def_mod = d->def_module_qn ? d->def_module_qn : module_qn;

        // Every type-like container (Type/Class/Struct/Interface/Enum/Trait).
        // Struct included so Go structs (now labelled "Struct") register as types.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;  // borrowed
            rt.short_name = d->short_name;          // borrowed
            rt.is_interface = d->is_interface || strcmp(d->label, "Interface") == 0;
            rt.embedded_types = split_pipe_strings(arena, d->embedded_types);

            // Set method_names for interfaces from "|"-separated string
            if (rt.is_interface && d->method_names_str && d->method_names_str[0]) {
                rt.method_names = split_pipe_strings(arena, d->method_names_str);
            }

            cbm_registry_add_type(&reg, rt);

            // Populate struct field types from field_defs
            if (d->field_defs && d->field_defs[0]) {
                parse_field_defs_into_type(arena, &reg, rt.qualified_name, d->field_defs, def_mod);
            }
        }

        // Function/Method
        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;  // borrowed
            rf.short_name = d->short_name;          // borrowed

            // Build FUNC type from return_types text
            const CBMType** ret_types = split_pipe_types(arena, d->return_types, def_mod);
            rf.signature = cbm_type_func(arena, NULL, NULL, ret_types);

            // Method receiver
            if (strcmp(d->label, "Method") == 0 && d->receiver_type && d->receiver_type[0]) {
                rf.receiver_type = d->receiver_type;  // borrowed
                // Auto-create type entry if not exists
                if (!cbm_registry_lookup_type(&reg, rf.receiver_type)) {
                    CBMRegisteredType auto_type;
                    memset(&auto_type, 0, sizeof(auto_type));
                    auto_type.qualified_name = rf.receiver_type;
                    const char* dot = strrchr(d->receiver_type, '.');
                    auto_type.short_name = dot ? dot + 1 : rf.receiver_type;  // borrowed substring
                    cbm_registry_add_type(&reg, auto_type);
                }
            }

            cbm_registry_add_func(&reg, rf);
        }
    }

    // 3. Phase 1b: Scan AST for struct definitions to populate embedded_types
    {
        // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
        uint32_t rkn = 0;
        TSNode* rkids = cbm_lsp_collect_children(arena, root, &rkn);
        for (uint32_t i = 0; i < rkn; i++) {
            TSNode top = rkids[i];
            if (strcmp(ts_node_type(top), "type_declaration") != 0) continue;
            uint32_t td_nc = ts_node_child_count(top);
            for (uint32_t j = 0; j < td_nc; j++) {
                TSNode spec = ts_node_child(top, j);
                if (ts_node_is_null(spec) || !ts_node_is_named(spec)) continue;
                const char* spec_kind2 = ts_node_type(spec);

                // type_alias: type Foo = Bar
                if (strcmp(spec_kind2, "type_alias") == 0) {
                    TSNode alias_name = ts_node_child_by_field_name(spec, "name", 4);
                    TSNode alias_type = ts_node_child_by_field_name(spec, "type", 4);
                    if (!ts_node_is_null(alias_name) && !ts_node_is_null(alias_type)) {
                        char* aname = cbm_node_text(arena, alias_name, source);
                        char* atarget = cbm_node_text(arena, alias_type, source);
                        if (aname && aname[0] && atarget && atarget[0]) {
                            const char* alias_type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, aname);
                            const char* alias_target_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, atarget);
                            bool found_a = false;
                            for (int ti = 0; ti < reg.type_count; ti++) {
                                if (reg.types[ti].qualified_name &&
                                    strcmp(reg.types[ti].qualified_name, alias_type_qn) == 0) {
                                    reg.types[ti].alias_of = alias_target_qn;
                                    found_a = true;
                                    break;
                                }
                            }
                            if (!found_a) {
                                CBMRegisteredType alias_rt;
                                memset(&alias_rt, 0, sizeof(alias_rt));
                                alias_rt.qualified_name = alias_type_qn;
                                alias_rt.short_name = aname;
                                alias_rt.alias_of = alias_target_qn;
                                cbm_registry_add_type(&reg, alias_rt);
                            }
                        }
                    }
                    continue;
                }

                if (strcmp(spec_kind2, "type_spec") != 0) continue;

                TSNode name_node = ts_node_child_by_field_name(spec, "name", 4);
                TSNode type_node = ts_node_child_by_field_name(spec, "type", 4);
                if (ts_node_is_null(name_node) || ts_node_is_null(type_node)) continue;
                if (strcmp(ts_node_type(type_node), "struct_type") != 0) continue;

                char* type_name = cbm_node_text(arena, name_node, source);
                if (!type_name || !type_name[0]) continue;
                const char* type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, type_name);

                TSNode field_list = ts_node_child_by_field_name(type_node, "body", 4);
                if (ts_node_is_null(field_list)) {
                    if (ts_node_named_child_count(type_node) > 0)
                        field_list = ts_node_named_child(type_node, 0);
                }
                if (ts_node_is_null(field_list)) continue;

                const char* embeds[16];
                int embed_count = 0;
                const char* fld_names[64];
                const CBMType* fld_types[64];
                int fld_count = 0;

                GoLSPContext tmp_ctx;
                memset(&tmp_ctx, 0, sizeof(tmp_ctx));
                tmp_ctx.arena = arena;
                tmp_ctx.source = source;
                tmp_ctx.source_len = source_len;
                tmp_ctx.registry = &reg;
                tmp_ctx.package_qn = module_qn;
                tmp_ctx.import_local_names = import_names;
                tmp_ctx.import_package_qns = import_qns;
                tmp_ctx.import_count = import_count;

                uint32_t fl_nc = ts_node_child_count(field_list);
                for (uint32_t k = 0; k < fl_nc; k++) {
                    TSNode field = ts_node_child(field_list, k);
                    if (ts_node_is_null(field) || !ts_node_is_named(field)) continue;
                    if (strcmp(ts_node_type(field), "field_declaration") != 0) continue;
                    TSNode fname = ts_node_child_by_field_name(field, "name", 4);
                    TSNode ftype = ts_node_child_by_field_name(field, "type", 4);
                    if (ts_node_is_null(fname) && !ts_node_is_null(ftype)) {
                        if (embed_count < 15) {
                            char* embed_text = cbm_node_text(arena, ftype, source);
                            if (embed_text && embed_text[0]) {
                                const char* et = embed_text;
                                while (*et == '*') et++;
                                embeds[embed_count++] = cbm_arena_sprintf(arena, "%s.%s", module_qn, et);
                            }
                        }
                    } else if (!ts_node_is_null(fname) && !ts_node_is_null(ftype) && fld_count < 63) {
                        char* fn = cbm_node_text(arena, fname, source);
                        if (fn && fn[0]) {
                            fld_names[fld_count] = fn;
                            fld_types[fld_count] = go_parse_type_node(&tmp_ctx, ftype);
                            fld_count++;
                        }
                    }
                }

                for (int ti = 0; ti < reg.type_count; ti++) {
                    if (!reg.types[ti].qualified_name ||
                        strcmp(reg.types[ti].qualified_name, type_qn) != 0) continue;

                    if (embed_count > 0) {
                        const char** arr = (const char**)cbm_arena_alloc(arena,
                            (embed_count + 1) * sizeof(const char*));
                        for (int ei = 0; ei < embed_count; ei++) arr[ei] = embeds[ei];
                        arr[embed_count] = NULL;
                        reg.types[ti].embedded_types = arr;
                    }
                    if (fld_count > 0) {
                        const char** names = (const char**)cbm_arena_alloc(arena,
                            (fld_count + 1) * sizeof(const char*));
                        const CBMType** types = (const CBMType**)cbm_arena_alloc(arena,
                            (fld_count + 1) * sizeof(const CBMType*));
                        for (int fi = 0; fi < fld_count; fi++) {
                            names[fi] = fld_names[fi];
                            types[fi] = fld_types[fi];
                        }
                        names[fld_count] = NULL;
                        types[fld_count] = NULL;
                        reg.types[ti].field_names = names;
                        reg.types[ti].field_types = types;
                    }
                    break;
                }
            }
        }
    }

    // 3b. Phase 1c: Extract type params from generic function declarations
    extract_type_params_from_ast(arena, &reg, root, source, module_qn);

    // 3c. Finalize registry — builds hash buckets for O(1) lookups in
    // cbm_registry_lookup_method / lookup_type. Without this, every
    // lookup falls through to the O(N) linear scan in type_registry.c,
    // turning the resolution pass below into an O(N·C·F) cost per file
    // (kubernetes: ~64B strcmps observed = 35min). Must come AFTER all
    // mutations (Phase 1b/1c above) and BEFORE the LSP context init.
    cbm_registry_finalize(&reg);

    // 4. Build LSP context and run
    GoLSPContext ctx;
    go_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, out);

    for (int i = 0; i < import_count; i++) {
        if (import_names[i] && import_qns[i]) {
            go_lsp_add_import(&ctx, import_names[i], import_qns[i]);
        }
    }

    go_lsp_process_file(&ctx, root);

    // 5. Cleanup — only free if we allocated
    if (owns_tree) {
        ts_tree_delete(tree);
        if (parser) ts_parser_delete(parser);
    }
}

/* ── Tier 2: pre-built per-language registry (gopls package summary pattern) ──
 *
 * cbm_go_build_cross_registry runs the registry build ONCE per project (in
 * pipeline.c, before the parallel resolve workers fire). The returned
 * registry is finalized (O(1) lookups) and shared READ-ONLY across all
 * workers in the resolve phase.
 *
 * cbm_run_go_lsp_cross_with_registry is the per-file entrypoint used by
 * the resolve worker. It SKIPS the registry build (uses the pre-built reg)
 * AND skips Phase 1b/1c AST scans (those mutate the registry, which would
 * race with other workers reading the shared reg). Accuracy trade-off:
 * file-local type aliases and struct embeddings discovered ONLY by Phase
 * 1b (and not already in defs[] embedded_types/field_defs strings) are
 * missed. In practice the per-file LSP during extract already captures
 * those via the def strings. */
CBMTypeRegistry* cbm_go_build_cross_registry(
    CBMArena* arena, CBMLSPDef* defs, int def_count) {
    if (!arena) return NULL;
    CBMTypeRegistry* reg = (CBMTypeRegistry*)cbm_arena_alloc(arena, sizeof(*reg));
    if (!reg) return NULL;
    cbm_registry_init(reg, arena);
    cbm_go_stdlib_register(reg, arena);

    for (int i = 0; i < def_count; i++) {
        CBMLSPDef* d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label) continue;
        /* Filter to Go defs only — the all_defs[] array is mixed-language. */
        if (d->lang != CBM_LANG_GO) continue;
        /* In the pre-built path every def carries its own def_module_qn
         * (set by cbm_pxc_collect_all_defs). There is no caller module to
         * fall back to — this registry is project-wide, not per-file. */
        const char* def_mod = d->def_module_qn ? d->def_module_qn : "";

        // Every type-like container (Type/Class/Struct/Interface/Enum/Trait).
        // Struct included so Go structs (now labelled "Struct") register as types.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name; /* borrowed */
            rt.short_name = d->short_name;
            rt.is_interface = d->is_interface || strcmp(d->label, "Interface") == 0;
            rt.embedded_types = split_pipe_strings(arena, d->embedded_types);
            if (rt.is_interface && d->method_names_str && d->method_names_str[0]) {
                rt.method_names = split_pipe_strings(arena, d->method_names_str);
            }
            cbm_registry_add_type(reg, rt);
            if (d->field_defs && d->field_defs[0]) {
                parse_field_defs_into_type(arena, reg, rt.qualified_name,
                                           d->field_defs, def_mod);
            }
        }

        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name; /* borrowed */
            rf.short_name = d->short_name;
            const CBMType** ret_types = split_pipe_types(arena, d->return_types, def_mod);
            rf.signature = cbm_type_func(arena, NULL, NULL, ret_types);
            if (strcmp(d->label, "Method") == 0 && d->receiver_type && d->receiver_type[0]) {
                rf.receiver_type = d->receiver_type;
                if (!cbm_registry_lookup_type(reg, rf.receiver_type)) {
                    CBMRegisteredType auto_type;
                    memset(&auto_type, 0, sizeof(auto_type));
                    auto_type.qualified_name = rf.receiver_type;
                    const char* dot = strrchr(d->receiver_type, '.');
                    auto_type.short_name = dot ? dot + 1 : rf.receiver_type;
                    cbm_registry_add_type(reg, auto_type);
                }
            }
            cbm_registry_add_func(reg, rf);
        }
    }

    cbm_registry_finalize(reg);
    reg->read_only = true; /* seal: shared Tier-2 registry is read-only during resolve */
    return reg;
}

void cbm_run_go_lsp_cross_with_registry(
    CBMArena* arena,
    const char* source, int source_len,
    const char* module_qn,
    CBMTypeRegistry* reg,
    const char** import_names, const char** import_qns, int import_count,
    TSTree* cached_tree,
    CBMResolvedCallArray* out) {
    if (!source || source_len <= 0 || !reg) return;

    TSParser* parser = NULL;
    TSTree* tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser) return;
        ts_parser_set_language(parser, tree_sitter_go());
        tree = ts_parser_parse_string(parser, NULL, source, source_len);
        owns_tree = true;
        if (!tree) { ts_parser_delete(parser); return; }
    }
    TSNode root = ts_tree_root_node(tree);

    /* Phase 1b/1c (per-file AST mutations on registry) DELIBERATELY SKIPPED —
     * shared registry must stay immutable across parallel workers. */

    GoLSPContext ctx;
    go_lsp_init(&ctx, arena, source, source_len, reg, module_qn, out);
    for (int i = 0; i < import_count; i++) {
        if (import_names[i] && import_qns[i]) {
            go_lsp_add_import(&ctx, import_names[i], import_qns[i]);
        }
    }
    go_lsp_process_file(&ctx, root);

    if (owns_tree) {
        ts_tree_delete(tree);
        if (parser) ts_parser_delete(parser);
    }
}

/* ── Tier 3: AST-walk-free metadata-driven cross-file resolver ────
 *
 * KEY INSIGHT: the per-file LSP during extract ALREADY emits one
 * CBMResolvedCall per call site, with strategy="lsp_unresolved" for
 * calls it couldn't resolve. For those unresolved entries:
 *
 *   * "symbol_not_in_registry" (go_lsp.c:1142) → callee_qn is
 *     "pkg_name.field_name" (literal local import alias)
 *   * "method_not_found" (go_lsp.c:1242) → callee_qn is
 *     "<FullyQualifiedReceiverTypeQN>.<MethodName>" — receiver type
 *     was ALREADY inferred by per-file LSP, just the method wasn't
 *     in the per-file registry (cross-file)
 *   * "function_not_in_registry" (go_lsp.c:1266) → callee_qn is
 *     the bare function name (likely a same-package symbol we
 *     missed, or a cross-file package function used unqualified)
 *   * "unknown_receiver_type" (go_lsp.c:1248) → per-file LSP
 *     couldn't infer the receiver type. Cross-LSP can't either
 *     without re-walking — leave unresolved.
 *
 * So cross-LSP work reduces to:
 *   for each unresolved entry → look up callee_qn (or pkg.x via
 *   import map) in the global registry → emit resolved version
 *   on top. NO TREE-SITTER PARSE. NO AST WALK. Just hash lookups.
 *
 * This is what the user meant: walk the AST ONCE (during extract),
 * capture everything cross-LSP needs as metadata, and let cross-LSP
 * be a pure lookup pass. */
int cbm_go_fast_resolve_qualified_calls(
    CBMFileResult* result,
    CBMTypeRegistry* reg,
    const char** import_names, const char** import_qns, int import_count) {
    if (!result || !reg) return 0;
    /* Snapshot the count: we append to resolved_calls inside the loop,
     * but only want to scan the original unresolved entries. */
    int initial_count = result->resolved_calls.count;
    int newly_resolved = 0;

    for (int i = 0; i < initial_count; i++) {
        const CBMResolvedCall* uc = &result->resolved_calls.items[i];
        if (!uc->strategy || !uc->callee_qn || !uc->caller_qn) continue;
        if (strcmp(uc->strategy, "lsp_unresolved") != 0) continue;

        /* Case 1: callee_qn is already a fully-qualified type/pkg
         * symbol that per-file LSP couldn't find in its local registry
         * but the global registry has. (method_not_found with NAMED
         * receiver, or any other case where callee_qn looks like a
         * full QN). Direct lookup. */
        const CBMRegisteredFunc* f = cbm_registry_lookup_func(reg, uc->callee_qn);

        /* Case 2: callee_qn is "local_pkg_alias.suffix" — the alias
         * needs translating through the file's import map to get the
         * real module QN. */
        if (!f) {
            const char* dot = strchr(uc->callee_qn, '.');
            if (dot) {
                size_t prefix_len = (size_t)(dot - uc->callee_qn);
                if (prefix_len > 0 && prefix_len < 256) {
                    char prefix[256];
                    memcpy(prefix, uc->callee_qn, prefix_len);
                    prefix[prefix_len] = '\0';
                    const char* suffix = dot + 1;
                    for (int j = 0; j < import_count; j++) {
                        if (import_names[j] && import_qns[j] &&
                            strcmp(prefix, import_names[j]) == 0) {
                            char fq[1024];
                            int n = snprintf(fq, sizeof(fq), "%s.%s",
                                             import_qns[j], suffix);
                            if (n > 0 && n < (int)sizeof(fq)) {
                                f = cbm_registry_lookup_func(reg, fq);
                            }
                            break;
                        }
                    }
                }
            }
        }

        if (!f) continue;

        /* Emit a resolved entry. cbm_pipeline_find_lsp_resolution
         * picks the highest-confidence match, so the unresolved entry
         * stays (harmless duplicate) but our resolved entry wins. */
        CBMResolvedCall rc;
        rc.caller_qn = uc->caller_qn;
        rc.callee_qn = f->qualified_name; /* borrowed from pipeline arena */
        rc.strategy = "lsp_strategy_cross_file";
        rc.confidence = 0.92f;
        rc.reason = NULL;
        cbm_resolvedcall_push(&result->resolved_calls, &result->arena, rc);
        newly_resolved++;
    }

    /* Return value mirrors the old contract (count of items still
     * needing the slow path) but is always 0 now — caller should
     * unconditionally skip the slow path for Go. */
    (void)newly_resolved;
    return 0;
}

// --- Batch cross-file LSP ---

void cbm_batch_go_lsp_cross(
    CBMArena* arena,
    CBMBatchGoLSPFile* files, int file_count,
    CBMResolvedCallArray* out)
{
    if (!files || file_count <= 0 || !out) return;

    for (int f = 0; f < file_count; f++) {
        CBMBatchGoLSPFile* file = &files[f];
        memset(&out[f], 0, sizeof(CBMResolvedCallArray));

        if (!file->source || file->source_len <= 0 || file->def_count <= 0) continue;

        // Per-file arena: registry + temp data freed after each file
        CBMArena file_arena;
        cbm_arena_init(&file_arena);

        CBMResolvedCallArray file_out;
        memset(&file_out, 0, sizeof(file_out));

        // Delegate to existing per-file function
        cbm_run_go_lsp_cross(
            &file_arena,
            file->source, file->source_len,
            file->module_qn,
            file->defs, file->def_count,
            file->import_names, file->import_qns, file->import_count,
            file->cached_tree,
            &file_out);

        // Copy results to output arena (must outlive per-file arena)
        if (file_out.count > 0) {
            out[f].count = file_out.count;
            out[f].items = (CBMResolvedCall*)cbm_arena_alloc(arena,
                file_out.count * sizeof(CBMResolvedCall));
            for (int j = 0; j < file_out.count; j++) {
                CBMResolvedCall* src = &file_out.items[j];
                CBMResolvedCall* dst = &out[f].items[j];
                dst->caller_qn = src->caller_qn ? cbm_arena_strdup(arena, src->caller_qn) : NULL;
                dst->callee_qn = src->callee_qn ? cbm_arena_strdup(arena, src->callee_qn) : NULL;
                dst->strategy  = src->strategy  ? cbm_arena_strdup(arena, src->strategy)  : NULL;
                dst->confidence = src->confidence;
                dst->reason    = src->reason    ? cbm_arena_strdup(arena, src->reason)    : NULL;
            }
        }

        cbm_arena_destroy(&file_arena);
    }
}
