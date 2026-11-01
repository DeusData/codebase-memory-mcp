#include "proof_bridge.h"
#include "go_lsp.h"
#include "lsp_node_iter.h"
#include "../../../src/foundation/hash_table.h"
#include "../helpers.h"
#include "../callable_sig.h"
#include "../resolve/symbols.h"
#include "../resolve/policy.h"
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdatomic.h>

// Forward declarations
static void resolve_calls_in_node_inner(GoLSPContext *ctx, TSNode node);

/* Depth-guarded entry for the AST call-resolution walk. The walk recurses once
 * per nesting level; a deeply-nested or cyclic file can overflow the native
 * stack (SIGSEGV) and take down the whole index. Past the cap the subtree is
 * skipped — its calls stay unresolved, which is graceful degradation, not a
 * crash. The cap is CBM_LSP_MAX_WALK_DEPTH, env-overridable via the same name.
 * The walk_depth-- runs after the inner returns, so early returns in the body
 * never leak the counter. */
static void resolve_calls_in_node(GoLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= cbm_lsp_max_walk_depth())
        return;
    ctx->walk_depth++;
    resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}
static void emit_resolved_call(GoLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence, TSNode site);
static const char *go_exact_callable_target(GoLSPContext *ctx, TSNode node);
static void go_bind_declared(GoLSPContext *ctx, const char *name, const CBMType *type,
                             TSNode written, uint32_t at);
static const CBMType *go_lookup_field(GoLSPContext *ctx, const char *type_qn,
                                      const char *field_name, int depth);
static void extract_type_params_from_ast(CBMArena *arena, CBMTypeRegistry *reg, TSNode root,
                                         const char *source, const char *module_qn);

// --- Initialization ---

void go_lsp_init(GoLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                 const CBMTypeRegistry *registry, const char *package_qn,
                 CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(GoLSPContext));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->package_qn = package_qn;
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL); // root scope
    cbm_resolve_types_init(&ctx->proven_types, arena, 0);
    (void)cbm_lsp_proof_standalone_context(&ctx->proven_types, source, source_len, package_qn,
                                           CBM_SOURCE_ORIGIN_RAW);

    {
        const char *debug_env = getenv("CBM_LSP_DEBUG");
        ctx->debug = (debug_env && debug_env[0]);
    }
}

void go_lsp_add_import(GoLSPContext *ctx, const char *local_name, const char *pkg_qn) {
    // Store in parallel arrays (arena-allocated, grow by doubling)
    if (ctx->import_count % 32 == 0) {
        int new_cap = ctx->import_count + 32;
        const char **new_names =
            (const char **)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(const char *));
        const char **new_qns =
            (const char **)cbm_arena_alloc(ctx->arena, (new_cap + 1) * sizeof(const char *));
        if (!new_names || !new_qns)
            return;
        if (ctx->import_local_names && ctx->import_count > 0) {
            memcpy(new_names, ctx->import_local_names, ctx->import_count * sizeof(const char *));
            memcpy(new_qns, ctx->import_package_qns, ctx->import_count * sizeof(const char *));
        }
        ctx->import_local_names = new_names;
        ctx->import_package_qns = new_qns;
    }
    ctx->import_local_names[ctx->import_count] = cbm_arena_strdup(ctx->arena, local_name);
    ctx->import_package_qns[ctx->import_count] = cbm_arena_strdup(ctx->arena, pkg_qn);
    ctx->import_count++;
}

// --- Helper: get node text ---

static char *lsp_node_text(GoLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

// --- Helper: resolve import alias to package QN ---

static const char *resolve_import(GoLSPContext *ctx, const char *local_name) {
    if (!local_name || cbm_scope_contains(ctx->current_scope, local_name))
        return NULL;
    const char *target = NULL;
    for (int i = 0; i < ctx->import_count; i++) {
        if (strcmp(ctx->import_local_names[i], local_name) == 0) {
            if (target && strcmp(target, ctx->import_package_qns[i]) != 0)
                return NULL;
            target = ctx->import_package_qns[i];
        }
    }
    return target;
}

// --- Helper: check if name is a Go builtin ---

static bool is_go_builtin_func(const char *name) {
    static const char *builtins[] = {"make",   "new",     "append",  "len",   "cap",
                                     "delete", "close",   "copy",    "panic", "recover",
                                     "print",  "println", "complex", "real",  "imag",
                                     "min",    "max",     "clear",   NULL};
    for (const char **b = builtins; *b; b++) {
        if (strcmp(name, *b) == 0)
            return true;
    }
    return false;
}

// --- Helper: check if name is a Go builtin type ---

static const CBMType *resolve_builtin_type(GoLSPContext *ctx, const char *name) {
    static const char *builtin_types[] = {
        "int",    "int8",   "int16",   "int32",   "int64",     "uint",       "uint8",  "uint16",
        "uint32", "uint64", "float32", "float64", "complex64", "complex128", "string", "bool",
        "byte",   "rune",   "error",   "uintptr", "any",       NULL};
    for (const char **b = builtin_types; *b; b++) {
        if (strcmp(name, *b) == 0) {
            return cbm_type_builtin(ctx->arena, name);
        }
    }
    return NULL;
}

// --- go_parse_type_node: AST type node -> CBMType ---

const CBMType *go_parse_type_node(GoLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();

    const char *kind = ts_node_type(node);

    // type_identifier: simple named type
    if (strcmp(kind, "type_identifier") == 0) {
        char *name = lsp_node_text(ctx, node);
        if (!name)
            return cbm_type_unknown();
        const CBMType *builtin = resolve_builtin_type(ctx, name);
        if (builtin)
            return builtin;
        // Resolve as local type: package_qn.TypeName
        return cbm_type_named(ctx->arena,
                              cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, name));
    }

    // qualified_type: pkg.Type
    if (strcmp(kind, "qualified_type") == 0) {
        TSNode pkg_node = ts_node_child_by_field_name(node, "package", 7);
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(pkg_node) && !ts_node_is_null(name_node)) {
            char *pkg = lsp_node_text(ctx, pkg_node);
            char *name = lsp_node_text(ctx, name_node);
            const char *pkg_qn = resolve_import(ctx, pkg);
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
        return cbm_type_map(ctx->arena, go_parse_type_node(ctx, key),
                            go_parse_type_node(ctx, value));
    }

    // channel_type: chan T
    if (strcmp(kind, "channel_type") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value) && ts_node_named_child_count(node) > 0) {
            value = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        }
        // Determine direction from text
        char *text = lsp_node_text(ctx, node);
        int dir = 0;
        if (text) {
            if (strncmp(text, "chan<-", 6) == 0 || strncmp(text, "chan <-", 7) == 0)
                dir = 1;
            else if (strncmp(text, "<-chan", 6) == 0 || strncmp(text, "<- chan", 7) == 0)
                dir = 2;
        }
        return cbm_type_channel(ctx->arena, go_parse_type_node(ctx, value), dir);
    }

    // function_type: func(...)...
    if (strcmp(kind, "function_type") == 0) {
        return cbm_type_func(ctx->arena, NULL, NULL, NULL); // simplified
    }

    // interface_type
    if (strcmp(kind, "interface_type") == 0) {
        CBMType *t = (CBMType *)cbm_arena_alloc(ctx->arena, sizeof(CBMType));
        memset(t, 0, sizeof(CBMType));
        t->kind = CBM_TYPE_INTERFACE;
        return t;
    }

    // struct_type
    if (strcmp(kind, "struct_type") == 0) {
        CBMType *t = (CBMType *)cbm_arena_alloc(ctx->arena, sizeof(CBMType));
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
        const CBMType *elems[16];
        uint32_t nc = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc && count < 16; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child) || !ts_node_is_named(child))
                continue;
            const char *ck = ts_node_type(child);
            if (strcmp(ck, "parameter_declaration") == 0) {
                TSNode tn = ts_node_child_by_field_name(child, "type", 4);
                if (!ts_node_is_null(tn)) {
                    elems[count++] = go_parse_type_node(ctx, tn);
                }
            } else {
                elems[count++] = go_parse_type_node(ctx, child);
            }
        }
        if (count == 1)
            return elems[0];
        if (count > 1)
            return cbm_type_tuple(ctx->arena, elems, count);
    }

    return cbm_type_unknown();
}

// --- Implicit generics: type unification ---

// Unify a parameter type pattern (containing TYPE_PARAM) against a concrete argument type.
// Fills inferred[i] with the concrete type bound to type_param_names[i].
static void go_unify_type(const CBMType *param_type, const CBMType *arg_type,
                          const char **type_param_names, const CBMType **inferred,
                          int param_count) {
    if (!param_type || !arg_type || cbm_type_is_unknown(arg_type))
        return;

    if (param_type->kind == CBM_TYPE_TYPE_PARAM) {
        for (int i = 0; i < param_count; i++) {
            if (strcmp(param_type->data.type_param.name, type_param_names[i]) == 0) {
                if (!inferred[i])
                    inferred[i] = arg_type;
                break;
            }
        }
        return;
    }

    // Structural matching — recurse into composite types
    if (param_type->kind == CBM_TYPE_SLICE && arg_type->kind == CBM_TYPE_SLICE) {
        go_unify_type(param_type->data.slice.elem, arg_type->data.slice.elem, type_param_names,
                      inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_POINTER && arg_type->kind == CBM_TYPE_POINTER) {
        go_unify_type(param_type->data.pointer.elem, arg_type->data.pointer.elem, type_param_names,
                      inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_MAP && arg_type->kind == CBM_TYPE_MAP) {
        go_unify_type(param_type->data.map.key, arg_type->data.map.key, type_param_names, inferred,
                      param_count);
        go_unify_type(param_type->data.map.value, arg_type->data.map.value, type_param_names,
                      inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_CHANNEL && arg_type->kind == CBM_TYPE_CHANNEL) {
        go_unify_type(param_type->data.channel.elem, arg_type->data.channel.elem, type_param_names,
                      inferred, param_count);
    }
    if (param_type->kind == CBM_TYPE_FUNC && arg_type->kind == CBM_TYPE_FUNC) {
        // Match param types
        if (param_type->data.func.param_types && arg_type->data.func.param_types) {
            for (int i = 0;
                 param_type->data.func.param_types[i] && arg_type->data.func.param_types[i]; i++) {
                go_unify_type(param_type->data.func.param_types[i],
                              arg_type->data.func.param_types[i], type_param_names, inferred,
                              param_count);
            }
        }
        // Match return types
        if (param_type->data.func.return_types && arg_type->data.func.return_types) {
            for (int i = 0;
                 param_type->data.func.return_types[i] && arg_type->data.func.return_types[i];
                 i++) {
                go_unify_type(param_type->data.func.return_types[i],
                              arg_type->data.func.return_types[i], type_param_names, inferred,
                              param_count);
            }
        }
    }
}

// --- go_eval_expr_type: recursive expression type evaluator ---

const CBMType *go_eval_expr_type(GoLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();

    const char *kind = ts_node_type(node);

    // --- Identifier: scope lookup ---
    if (strcmp(kind, "identifier") == 0) {
        char *name = lsp_node_text(ctx, node);
        if (!name)
            return cbm_type_unknown();

        // Check scope first
        const CBMType *t = cbm_scope_lookup(ctx->current_scope, name);
        if (cbm_scope_contains(ctx->current_scope, name))
            return t ? t : cbm_type_unknown();

        // Check if it's a package-level function
        const CBMRegisteredFunc *f =
            cbm_registry_lookup_symbol(ctx->registry, ctx->package_qn, name);
        if (f && f->signature)
            return f->signature;

        // Check if it's a builtin type (for type conversions like string(x), int(x))
        const CBMType *bt = resolve_builtin_type(ctx, name);
        if (bt)
            return cbm_type_named(ctx->arena, name);

        // Check if it's a registered type (for type conversions like MyType(x))
        const char *type_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, name);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
        if (rt)
            return cbm_type_named(ctx->arena, type_qn);

        return cbm_type_unknown();
    }

    // --- Selector expression: a.B ---
    if (strcmp(kind, "selector_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
        TSNode field = ts_node_child_by_field_name(node, "field", 5);
        if (ts_node_is_null(operand) || ts_node_is_null(field))
            return cbm_type_unknown();

        char *field_name = lsp_node_text(ctx, field);
        if (!field_name)
            return cbm_type_unknown();

        // Check if operand is an import alias (pkg.Symbol)
        if (strcmp(ts_node_type(operand), "identifier") == 0) {
            char *pkg_name = lsp_node_text(ctx, operand);
            if (pkg_name) {
                const char *pkg_qn = resolve_import(ctx, pkg_name);
                if (pkg_qn) {
                    // Look up pkg.Symbol as a function or type
                    const CBMRegisteredFunc *f =
                        cbm_registry_lookup_symbol(ctx->registry, pkg_qn, field_name);
                    if (f && f->signature)
                        return f->signature;
                    // Check if it's a type
                    char *type_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", pkg_qn, field_name);
                    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
                    if (rt)
                        return cbm_type_named(ctx->arena, type_qn);
                    return cbm_type_unknown();
                }
            }
        }

        // Evaluate operand type
        const CBMType *recv_type = go_eval_expr_type(ctx, operand);
        if (cbm_type_is_unknown(recv_type))
            return cbm_type_unknown();

        // Auto-deref pointers for method calls
        const CBMType *base_type = recv_type;
        if (base_type->kind == CBM_TYPE_POINTER) {
            base_type = cbm_type_deref(base_type);
        }

        if (base_type->kind == CBM_TYPE_NAMED) {
            const char *type_qn = base_type->data.named.qualified_name;

            // Look up method/field (methods recurse through embeddings)
            const CBMRegisteredFunc *method = go_lookup_field_or_method(ctx, type_qn, field_name);
            if (method && method->signature)
                return method->signature;

            // Check struct fields (with embedded field promotion)
            const CBMType *field_type = go_lookup_field(ctx, type_qn, field_name, 0);
            if (field_type && !cbm_type_is_unknown(field_type))
                return field_type;
        }

        return cbm_type_unknown();
    }

    // --- Call expression: f(...) ---
    if (strcmp(kind, "call_expression") == 0) {
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        TSNode args_node = ts_node_child_by_field_name(node, "arguments", 9);
        if (ts_node_is_null(func_node))
            return cbm_type_unknown();

        // Check for builtin calls
        if (strcmp(ts_node_type(func_node), "identifier") == 0) {
            char *name = lsp_node_text(ctx, func_node);
            if (name && is_go_builtin_func(name)) {
                return go_eval_builtin_call(ctx, name, args_node);
            }
        }

        // Evaluate function type
        const CBMType *func_type = go_eval_expr_type(ctx, func_node);

        // If it's a FUNC type, return its return type
        if (func_type && func_type->kind == CBM_TYPE_FUNC && func_type->data.func.return_types &&
            func_type->data.func.return_types[0]) {

            // Check for explicit type arguments: call_expression has type_arguments field
            // Go tree-sitter: Func[T1, T2](args) → call_expression { function, type_arguments,
            // arguments }
            TSNode targs_node = ts_node_child_by_field_name(node, "type_arguments", 14);
            if (!ts_node_is_null(targs_node)) {
                // Look up the registered function to get type_param_names
                const CBMRegisteredFunc *rfunc = NULL;
                char *func_name = lsp_node_text(ctx, func_node);
                if (func_name) {
                    const char *func_qn =
                        cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, func_name);
                    rfunc = cbm_registry_lookup_func(ctx->registry, func_qn);
                    // If not found as local, try via import (selector_expression: pkg.Func)
                    if (!rfunc && strcmp(ts_node_type(func_node), "selector_expression") == 0) {
                        TSNode operand = ts_node_child_by_field_name(func_node, "operand", 7);
                        TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
                        if (!ts_node_is_null(operand) && !ts_node_is_null(field)) {
                            char *pkg = lsp_node_text(ctx, operand);
                            char *fn = lsp_node_text(ctx, field);
                            if (pkg && fn) {
                                const char *pkg_qn = resolve_import(ctx, pkg);
                                if (pkg_qn) {
                                    rfunc = cbm_registry_lookup_symbol(ctx->registry, pkg_qn, fn);
                                }
                            }
                        }
                    }
                }

                if (rfunc && rfunc->type_param_names) {
                    // Parse type arguments from AST
                    const CBMType *type_args[16];
                    int targ_count = 0;
                    uint32_t ta_nc = ts_node_child_count(targs_node);
                    for (uint32_t ti = 0; ti < ta_nc && targ_count < 15; ti++) {
                        TSNode targ = ts_node_child(targs_node, ti);
                        if (ts_node_is_null(targ) || !ts_node_is_named(targ))
                            continue;
                        type_args[targ_count++] = go_parse_type_node(ctx, targ);
                    }

                    // Count type params
                    int param_count = 0;
                    while (rfunc->type_param_names[param_count])
                        param_count++;

                    if (targ_count > 0 && targ_count == param_count) {
                        const CBMType **targ_arr = (const CBMType **)cbm_arena_alloc(
                            ctx->arena, (targ_count + 1) * sizeof(const CBMType *));
                        for (int ti = 0; ti < targ_count; ti++)
                            targ_arr[ti] = type_args[ti];
                        targ_arr[targ_count] = NULL;

                        // Substitute type params in return type(s)
                        int ret_count = 0;
                        while (func_type->data.func.return_types[ret_count])
                            ret_count++;

                        if (ret_count == 1) {
                            return cbm_type_substitute(ctx->arena,
                                                       func_type->data.func.return_types[0],
                                                       rfunc->type_param_names, targ_arr);
                        }
                        // Multi-return: substitute all
                        const CBMType **new_rets = (const CBMType **)cbm_arena_alloc(
                            ctx->arena, (ret_count + 1) * sizeof(const CBMType *));
                        for (int ri = 0; ri < ret_count; ri++) {
                            new_rets[ri] = cbm_type_substitute(
                                ctx->arena, func_type->data.func.return_types[ri],
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
                const CBMRegisteredFunc *rfunc = NULL;
                char *func_name = lsp_node_text(ctx, func_node);
                if (func_name) {
                    const char *func_qn =
                        cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, func_name);
                    rfunc = cbm_registry_lookup_func(ctx->registry, func_qn);
                    if (!rfunc && strcmp(ts_node_type(func_node), "selector_expression") == 0) {
                        TSNode operand = ts_node_child_by_field_name(func_node, "operand", 7);
                        TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
                        if (!ts_node_is_null(operand) && !ts_node_is_null(field)) {
                            char *pkg = lsp_node_text(ctx, operand);
                            char *fn = lsp_node_text(ctx, field);
                            if (pkg && fn) {
                                const char *pkg_qn = resolve_import(ctx, pkg);
                                if (pkg_qn)
                                    rfunc = cbm_registry_lookup_symbol(ctx->registry, pkg_qn, fn);
                            }
                        }
                    }
                }

                if (rfunc && rfunc->type_param_names && func_type->data.func.param_types) {
                    int tpc = 0;
                    while (rfunc->type_param_names[tpc])
                        tpc++;

                    // Check if any return type uses a type param
                    bool has_type_param = false;
                    int ret_count = 0;
                    while (func_type->data.func.return_types[ret_count])
                        ret_count++;
                    for (int ri = 0; ri < ret_count && !has_type_param; ri++) {
                        // Quick check: walk return type tree for TYPE_PARAM nodes
                        const CBMType *rt = func_type->data.func.return_types[ri];
                        if (rt->kind == CBM_TYPE_TYPE_PARAM)
                            has_type_param = true;
                        else if (rt->kind == CBM_TYPE_SLICE && rt->data.slice.elem &&
                                 rt->data.slice.elem->kind == CBM_TYPE_TYPE_PARAM)
                            has_type_param = true;
                        else if (rt->kind == CBM_TYPE_POINTER && rt->data.pointer.elem &&
                                 rt->data.pointer.elem->kind == CBM_TYPE_TYPE_PARAM)
                            has_type_param = true;
                        else if (rt->kind == CBM_TYPE_MAP) {
                            if (rt->data.map.key && rt->data.map.key->kind == CBM_TYPE_TYPE_PARAM)
                                has_type_param = true;
                            if (rt->data.map.value &&
                                rt->data.map.value->kind == CBM_TYPE_TYPE_PARAM)
                                has_type_param = true;
                        }
                    }

                    if (has_type_param && tpc > 0 && tpc <= 16) {
                        // Evaluate argument types and unify
                        const CBMType *inferred[16] = {0};

                        if (!ts_node_is_null(args_node)) {
                            uint32_t argc = ts_node_named_child_count(args_node);
                            int pi = 0;
                            for (uint32_t ai = 0; ai < argc && func_type->data.func.param_types[pi];
                                 ai++) {
                                TSNode arg = ts_node_named_child(args_node, ai);
                                if (ts_node_is_null(arg))
                                    continue;
                                const CBMType *arg_type = go_eval_expr_type(ctx, arg);
                                go_unify_type(func_type->data.func.param_types[pi], arg_type,
                                              rfunc->type_param_names, inferred, tpc);
                                pi++;
                            }
                        }

                        // Check if all type params were inferred
                        bool all_inferred = true;
                        for (int i = 0; i < tpc; i++) {
                            if (!inferred[i]) {
                                all_inferred = false;
                                break;
                            }
                        }

                        if (all_inferred) {
                            const CBMType **targ_arr = (const CBMType **)cbm_arena_alloc(
                                ctx->arena, (tpc + 1) * sizeof(const CBMType *));
                            for (int i = 0; i < tpc; i++)
                                targ_arr[i] = inferred[i];
                            targ_arr[tpc] = NULL;

                            if (ret_count == 1) {
                                return cbm_type_substitute(ctx->arena,
                                                           func_type->data.func.return_types[0],
                                                           rfunc->type_param_names, targ_arr);
                            }
                            const CBMType **new_rets = (const CBMType **)cbm_arena_alloc(
                                ctx->arena, (ret_count + 1) * sizeof(const CBMType *));
                            for (int ri = 0; ri < ret_count; ri++) {
                                new_rets[ri] = cbm_type_substitute(
                                    ctx->arena, func_type->data.func.return_types[ri],
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
            while (func_type->data.func.return_types[count])
                count++;
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
            const char *fk = ts_node_type(func_node);
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
        if (ts_node_is_null(operand))
            return cbm_type_unknown();

        // Get operator
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char *op = lsp_node_text(ctx, child);
                if (!op)
                    continue;
                if (strcmp(op, "&") == 0) {
                    return cbm_type_pointer(ctx->arena, go_eval_expr_type(ctx, operand));
                }
                if (strcmp(op, "*") == 0) {
                    return cbm_type_deref(go_eval_expr_type(ctx, operand));
                }
                if (strcmp(op, "<-") == 0) {
                    const CBMType *ch_type = go_eval_expr_type(ctx, operand);
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
        if (ts_node_is_null(operand))
            return cbm_type_unknown();
        const CBMType *op_type = go_eval_expr_type(ctx, operand);
        if (!op_type)
            return cbm_type_unknown();
        if (op_type->kind == CBM_TYPE_MAP)
            return op_type->data.map.value;
        if (op_type->kind == CBM_TYPE_SLICE)
            return op_type->data.slice.elem;
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
                char *op = lsp_node_text(ctx, child);
                if (op && (strcmp(op, "==") == 0 || strcmp(op, "!=") == 0 || strcmp(op, "<") == 0 ||
                           strcmp(op, ">") == 0 || strcmp(op, "<=") == 0 || strcmp(op, ">=") == 0 ||
                           strcmp(op, "&&") == 0 || strcmp(op, "||") == 0)) {
                    return cbm_type_builtin(ctx->arena, "bool");
                }
                break;
            }
        }
        if (!ts_node_is_null(left))
            return go_eval_expr_type(ctx, left);
        return cbm_type_unknown();
    }

    // --- Slice expression: a[low:high] ---
    if (strcmp(kind, "slice_expression") == 0) {
        TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
        if (!ts_node_is_null(operand))
            return go_eval_expr_type(ctx, operand);
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
            CBMScope *saved = ctx->current_scope;
            ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

            // Bind closure parameters
            TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
            if (!ts_node_is_null(params)) {
                uint32_t nc = ts_node_child_count(params);
                for (uint32_t i = 0; i < nc; i++) {
                    TSNode param = ts_node_child(params, i);
                    if (ts_node_is_null(param) || !ts_node_is_named(param))
                        continue;
                    if (strcmp(ts_node_type(param), "parameter_declaration") != 0)
                        continue;
                    TSNode type_node = ts_node_child_by_field_name(param, "type", 4);
                    const CBMType *pt = go_parse_type_node(ctx, type_node);
                    uint32_t pnc = ts_node_child_count(param);
                    for (uint32_t j = 0; j < pnc; j++) {
                        TSNode ch = ts_node_child(param, j);
                        if (!ts_node_is_null(ch) && ts_node_is_named(ch) &&
                            strcmp(ts_node_type(ch), "identifier") == 0) {
                            char *pname = lsp_node_text(ctx, ch);
                            if (pname && strcmp(pname, "_") != 0)
                                go_bind_declared(ctx, pname, pt, type_node,
                                                 ts_node_end_byte(param));
                        }
                    }
                }
            }

            // Walk closure body to resolve calls
            resolve_calls_in_node(ctx, body);

            ctx->current_scope = saved;
        }

        // Build full FUNC type with param/return types from AST
        const CBMType *pt_arr[16];
        int pt_count = 0;
        TSNode params2 = ts_node_child_by_field_name(node, "parameters", 10);
        if (!ts_node_is_null(params2)) {
            uint32_t nc2 = ts_node_child_count(params2);
            for (uint32_t i = 0; i < nc2 && pt_count < 15; i++) {
                TSNode p = ts_node_child(params2, i);
                if (ts_node_is_null(p) || !ts_node_is_named(p))
                    continue;
                if (strcmp(ts_node_type(p), "parameter_declaration") != 0)
                    continue;
                TSNode pt = ts_node_child_by_field_name(p, "type", 4);
                if (!ts_node_is_null(pt))
                    pt_arr[pt_count++] = go_parse_type_node(ctx, pt);
            }
        }
        pt_arr[pt_count] = NULL;

        const CBMType *rt_arr[16];
        int rt_count = 0;
        TSNode result = ts_node_child_by_field_name(node, "result", 6);
        if (!ts_node_is_null(result)) {
            if (strcmp(ts_node_type(result), "parameter_list") == 0) {
                uint32_t rnc = ts_node_child_count(result);
                for (uint32_t i = 0; i < rnc && rt_count < 15; i++) {
                    TSNode rc = ts_node_child(result, i);
                    if (ts_node_is_null(rc) || !ts_node_is_named(rc))
                        continue;
                    TSNode rt = ts_node_child_by_field_name(rc, "type", 4);
                    if (ts_node_is_null(rt))
                        rt = rc;
                    rt_arr[rt_count++] = go_parse_type_node(ctx, rt);
                }
            } else {
                rt_arr[rt_count++] = go_parse_type_node(ctx, result);
            }
        }
        rt_arr[rt_count] = NULL;

        return cbm_type_func(ctx->arena, NULL, pt_count > 0 ? (const CBMType **)pt_arr : NULL,
                             rt_count > 0 ? (const CBMType **)rt_arr : NULL);
    }

    return cbm_type_unknown();
}

// --- go_eval_builtin_call ---

const CBMType *go_eval_builtin_call(GoLSPContext *ctx, const char *name, TSNode args) {
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

// --- Import-alias re-qualification ------------------------------------
//
// parse_field_defs_into_type qualifies struct field type texts as
// "<def_module>.<text>". When the author wrote the text through an import
// alias ("Svc:svc.Svc" in module test.main), that yields a QN
// ("test.main.svc.Svc") that exists nowhere in the project-wide registry —
// the real QN is "<import_qn>.Svc" and only the calling file's import map
// can say so. On an exact-QN miss this rewrites the alias segment through
// the file's imports; returns NULL when no alias segment is involved.

static const char *go_requalify_via_imports(GoLSPContext *ctx, const char *type_qn) {
    if (!ctx || !type_qn || !type_qn[0] || ctx->import_count <= 0)
        return NULL;
    for (int j = 0; j < ctx->import_count; j++) {
        const char *alias = ctx->import_local_names[j];
        const char *alias_qn = ctx->import_package_qns[j];
        if (!alias || !alias[0] || !alias_qn || strchr(alias, '.'))
            continue;
        size_t alias_len = strlen(alias);
        /* Last occurrence of a "<dot>alias<dot>" segment in type_qn. */
        const char *hit = NULL;
        for (const char *p = type_qn;;) {
            p = strstr(p, ".");
            if (!p)
                break;
            p++;
            if (strncmp(p, alias, alias_len) == 0 && p[alias_len] == '.') {
                hit = p;
                p += alias_len;
            }
        }
        if (hit) {
            const char *rest = hit + alias_len + 1; /* past "<alias>." */
            return cbm_arena_sprintf(ctx->arena, "%s.%s", alias_qn, rest);
        }
    }
    return NULL;
}

static const CBMType *go_lookup_field(GoLSPContext *ctx, const char *type_qn,
                                      const char *field_name, int depth) {
    if (!type_qn || !field_name || depth > 5)
        return NULL;

    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt && depth == 0) {
        /* Import-alias re-qualification: field texts from cross-package defs
         * may embed an alias segment only this file's import map resolves. */
        const char *alt_qn = go_requalify_via_imports(ctx, type_qn);
        if (alt_qn)
            rt = cbm_registry_lookup_type(ctx->registry, alt_qn);
    }
    if (!rt)
        return NULL;

    // Follow alias chain
    if (rt->alias_of)
        return go_lookup_field(ctx, rt->alias_of, field_name, depth + 1);

    // Direct field lookup
    if (rt->field_names) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], field_name) == 0 && rt->field_types &&
                rt->field_types[i]) {
                return rt->field_types[i];
            }
        }
    }

    // Promoted fields from embedded types
    if (rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            const CBMType *f = go_lookup_field(ctx, rt->embedded_types[i], field_name, depth + 1);
            if (f)
                return f;
        }
    }

    return NULL;
}

// --- go_lookup_field_or_method: method sets + embedding ---

static const CBMRegisteredFunc *go_lookup_field_or_method_depth(GoLSPContext *ctx,
                                                                const char *type_qn,
                                                                const char *member_name,
                                                                int depth) {
    if (!type_qn || !member_name)
        return NULL;
    if (depth > CBM_LSP_MAX_LOOKUP_DEPTH)
        return NULL;

    // Direct method lookup
    const CBMRegisteredFunc *f = cbm_registry_lookup_method(ctx->registry, type_qn, member_name);
    if (f)
        return f;

    /* Import-alias re-qualification fallback: NAMED receivers built from
     * cross-package field type texts can carry a "<module>.svc.Svc" QN;
     * retry the method set on the import-resolved QN (see
     * go_requalify_via_imports). */
    if (depth == 0) {
        const char *alt_qn = go_requalify_via_imports(ctx, type_qn);
        if (alt_qn) {
            f = go_lookup_field_or_method_depth(ctx, alt_qn, member_name, depth + 1);
            if (f)
                return f;
        }
    }

    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (rt) {
        // Follow type alias chain
        if (rt->alias_of) {
            f = go_lookup_field_or_method_depth(ctx, rt->alias_of, member_name, depth + 1);
            if (f)
                return f;
        }

        // Check embedded types (promoted methods)
        if (rt->embedded_types) {
            for (int i = 0; rt->embedded_types[i]; i++) {
                f = go_lookup_field_or_method_depth(ctx, rt->embedded_types[i], member_name,
                                                    depth + 1);
                if (f)
                    return f;
            }
        }
    }

    return NULL;
}

const CBMRegisteredFunc *go_lookup_field_or_method(GoLSPContext *ctx, const char *type_qn,
                                                   const char *member_name) {
    return go_lookup_field_or_method_depth(ctx, type_qn, member_name, 0);
}

static const CBMType *go_named_base(const CBMType *type) {
    if (type && type->kind == CBM_TYPE_POINTER)
        type = cbm_type_deref(type);
    return type && type->kind == CBM_TYPE_NAMED ? type : NULL;
}

/* Written types establish their package through the language namespace. A
 * named result from expression inference alone is not sufficient evidence. */
static const CBMRegisteredType *go_proven_type(GoLSPContext *ctx, const char *qn) {
    CBMTypeQNIter iterator;
    cbm_registry_types_with_qn(ctx->registry, qn, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    while ((candidate = cbm_type_qn_next(&iterator, NULL))) {
        if (!candidate->project_source)
            return NULL;
        if (chosen && (!chosen->declaration_identity || !candidate->declaration_identity ||
                       strcmp(chosen->declaration_identity, candidate->declaration_identity) ||
                       !chosen->graph_qualified_name || !candidate->graph_qualified_name ||
                       strcmp(chosen->graph_qualified_name, candidate->graph_qualified_name)))
            return NULL;
        chosen = candidate;
    }
    return iterator.complete ? chosen : NULL;
}

static const char *go_declared_type_qn(GoLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return NULL;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "pointer_type") && ts_node_named_child_count(node) == 1)
        return go_declared_type_qn(ctx, ts_node_named_child(node, 0));
    const char *qn = NULL;
    if (!strcmp(kind, "type_identifier")) {
        char *name = lsp_node_text(ctx, node);
        if (!name || cbm_scope_contains(ctx->current_scope, name))
            return NULL;
        qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, name);
    } else if (!strcmp(kind, "qualified_type")) {
        char *local = lsp_node_text(ctx, ts_node_child_by_field_name(node, "package", 7));
        char *name = lsp_node_text(ctx, ts_node_child_by_field_name(node, "name", 4));
        const char *package = local ? resolve_import(ctx, local) : NULL;
        if (!package || !name || name[0] < 'A' || name[0] > 'Z')
            return NULL;
        qn = cbm_arena_sprintf(ctx->arena, "%s.%s", package, name);
    }
    const CBMRegisteredType *type = qn ? go_proven_type(ctx, qn) : NULL;
    return type ? type->qualified_name : NULL;
}

static cbm_resolve_result_t go_type_proof(GoLSPContext *ctx, const char *qn, TSNode site) {
    const CBMRegisteredType *type = qn ? go_proven_type(ctx, qn) : NULL;
    if (!type)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED};
    return cbm_lsp_proof_type_lookup(&ctx->proven_types, lsp_node_text(ctx, site), site,
                                     "go-package-type-domain", type->qualified_name,
                                     type->declaration_identity, type->graph_qualified_name,
                                     type->source_flags, "go-exact-package-type-lookup");
}

static void go_observe_declared_type(GoLSPContext *ctx, const char *name, const CBMType *type,
                                     TSNode written_type, uint32_t at) {
    const char *qn = go_declared_type_qn(ctx, written_type);
    const CBMType *base = go_named_base(type);
    if (!qn || !base || strcmp(qn, base->data.named.qualified_name))
        return;
    cbm_resolve_result_t proof = go_type_proof(ctx, qn, written_type);
    cbm_resolve_types_observe(&ctx->proven_types, ctx->current_scope, name, at, type,
                              CBM_RESOLVE_TYPE_ANNOTATION, &proof);
}

static cbm_resolve_result_t go_callable_proof(GoLSPContext *ctx, TSNode node);
static cbm_resolve_type_fact_t go_return_fact(GoLSPContext *ctx, TSNode node);
static cbm_resolve_type_fact_t go_receiver_fact_inner(GoLSPContext *ctx, TSNode node);
static cbm_resolve_type_fact_t go_receiver_fact(GoLSPContext *ctx, TSNode node) {
    if (ctx->proof_depth >= 64)
        return (cbm_resolve_type_fact_t){.status = CBM_RESOLVE_TYPE_LIMITED};
    ctx->proof_depth++;
    cbm_resolve_type_fact_t fact = go_receiver_fact_inner(ctx, node);
    ctx->proof_depth--;
    return fact;
}

static cbm_resolve_type_fact_t go_receiver_fact_inner(GoLSPContext *ctx, TSNode node) {
    cbm_resolve_type_fact_t unknown = {0};
    if (ctx->binding_failed || ts_node_is_null(node))
        return unknown;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return go_receiver_fact(ctx, ts_node_named_child(node, 0));
    if (!strcmp(kind, "identifier")) {
        char *name = lsp_node_text(ctx, node);
        cbm_resolve_type_fact_t fact = cbm_resolve_types_lookup(
            &ctx->proven_types, ctx->current_scope, name, ts_node_start_byte(node));
        const CBMType *base = go_named_base(fact.type);
        if (fact.status != CBM_RESOLVE_TYPE_KNOWN || !base)
            return unknown;
        /* Go selectors automatically dereference a pointer receiver. The
         * named owner remains the exact declaration carried by the fact. */
        fact.type = base;
        fact.binding = NULL; /* checked pointer-to-owner adaptation above */
        return fact;
    }
    if (!strcmp(kind, "unary_expression") && ctx->source[ts_node_start_byte(node)] == '&')
        return go_receiver_fact(ctx, ts_node_child_by_field_name(node, "operand", 7));
    if (!strcmp(kind, "call_expression"))
        return go_return_fact(ctx, node);
    if (!strcmp(kind, "composite_literal")) {
        TSNode written = ts_node_child_by_field_name(node, "type", 4);
        const char *qn = go_declared_type_qn(ctx, written);
        if (qn) {
            cbm_resolve_type_fact_t fact = {.type = cbm_type_named(ctx->arena, qn),
                                            .status = CBM_RESOLVE_TYPE_KNOWN,
                                            .origin = CBM_RESOLVE_TYPE_CONSTRUCTOR,
                                            .proof_pool = &ctx->proven_types.proofs,
                                            .evidence = go_type_proof(ctx, qn, written)};
            return fact;
        }
    }
    return unknown;
}

static cbm_resolve_method_candidate_t go_classify_callable(const cbm_resolve_method_query_t *query,
                                                           const CBMTypeRegistry *owner,
                                                           const CBMRegisteredFunc *candidate,
                                                           void *context) {
    (void)query;
    (void)owner;
    GoLSPContext *ctx = context;
    const char *target =
        candidate->sig && candidate->sig[0]
            ? cbm_arena_sprintf(ctx->arena, "%s%s", candidate->qualified_name, candidate->sig)
            : candidate->qualified_name;
    return (cbm_resolve_method_candidate_t){
        .exposed = CBM_RESOLVE_METHOD_YES,
        .applicable = CBM_RESOLVE_METHOD_YES,
        .graph_target = target,
        .graph_target_exact = target != NULL,
        .declaration_identity = candidate->declaration_identity,
        .declaration_identity_exact = candidate->declaration_identity != NULL,
        .domain = (candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) ? CBM_RESOLVE_DOMAIN_PROJECT
                                                                    : CBM_RESOLVE_DOMAIN_EXTERNAL};
}

static cbm_resolve_result_t go_scoped_candidate(GoLSPContext *ctx, const char *qn,
                                                cbm_resolve_rule_t rule, uint32_t facts,
                                                const cbm_resolve_proof_t *source_proof,
                                                TSNode site) {
    cbm_resolve_evidence_t binding = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .rule = rule,
                                      .facts = facts | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                      .proof = source_proof};
    if (!binding.proof) {
        cbm_resolve_proof_key_t key = {
            .lookup_domain = "go-source-name-binding",
            .lookup_key = qn,
            .binding_identity = qn,
            .start_byte = ts_node_is_null(site) ? 0 : ts_node_start_byte(site),
            .end_byte = ts_node_is_null(site) ? 0 : ts_node_end_byte(site),
            .detail = "audited-scope-import-or-callable-binding"};
        cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .rule = rule,
                                      .facts = binding.facts,
                                      .reason = key.detail};
        binding.proof = cbm_resolve_proof_leaf(&ctx->proven_types.proofs, CBM_PROOF_ADAPTER_DOMAIN,
                                               &ctx->proven_types.proof_source, &key, &audit);
    }
    cbm_resolve_function_query_t query = {
        .proof_pool = &ctx->proven_types.proofs,
        .proof_source = &ctx->proven_types.proof_source,
        .lookup_domain = qn,
        .registry = ctx->registry,
        .function_qn = qn,
        .function_binding = binding,
        .argument_count = -1,
        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT | CBM_RESOLVE_METHOD_NAME_IDENTITY,
        .classify = go_classify_callable,
        .context = ctx};
    cbm_resolve_function_result_t resolved = cbm_resolve_function(&query);
    if (cbm_resolve_admit(&resolved.result))
        return resolved.result;
    /* Keep a source-bound pending candidate for the shared cross-file pass.
     * It is not an admitted target until that pass proves a unique registry
     * declaration; arbitrary unresolved strings never acquire these facts. */
    if (resolved.result.binding != CBM_RESOLVE_BINDING_AMBIGUOUS &&
        !cbm_registry_lookup_func(ctx->registry, qn)) {
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_LIMITED,
                                      .rule = rule,
                                      .facts = binding.facts,
                                      .target = qn,
                                      .reason = "declaration_not_in_registry",
                                      .proof = binding.proof};
    }
    return resolved.result;
}

static cbm_resolve_result_t go_bound_callable(GoLSPContext *ctx, const char *target, TSNode site) {
    const CBMRegisteredFunc *candidate = cbm_registry_lookup_func(ctx->registry, target);
    if (!candidate || !candidate->receiver_type)
        return go_scoped_candidate(ctx, target, CBM_RESOLVE_RULE_CALLABLE_BINDING,
                                   CBM_RESOLVE_FACT_CALLABLE_TYPED, NULL, site);
    /* The binding was installed only from go_exact_callable_target. A bound
     * method value captures the selected declaration; it need not guess its
     * receiver again when copied or invoked through the alias. */
    cbm_resolve_type_fact_t receiver = {
        .type = cbm_type_named(ctx->arena, candidate->receiver_type),
        .status = CBM_RESOLVE_TYPE_KNOWN,
        .origin = CBM_RESOLVE_TYPE_OBSERVED_BINDING,
        .proof_pool = &ctx->proven_types.proofs,
        .evidence = go_type_proof(ctx, candidate->receiver_type, site)};
    cbm_resolve_method_query_t query = {.proof_pool = &ctx->proven_types.proofs,
                                        .proof_source = &ctx->proven_types.proof_source,
                                        .lookup_domain = candidate->receiver_type,
                                        .registry = ctx->registry,
                                        .receiver = &receiver,
                                        .receiver_qn = candidate->receiver_type,
                                        .method_name = candidate->short_name,
                                        .argument_count = -1,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                         CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                        .classify = go_classify_callable,
                                        .context = ctx};
    cbm_resolve_result_t result = cbm_resolve_method(&query).result;
    if (cbm_resolve_admit(&result)) {
        result.rule = CBM_RESOLVE_RULE_CALLABLE_BINDING;
        result.facts |= CBM_RESOLVE_FACT_CALLABLE_TYPED;
        result = cbm_lsp_proof_rule(&ctx->proven_types, result, site, "go-lexical-callable-binding",
                                    lsp_node_text(ctx, site),
                                    "nearest-binding-retains-source-proven-method-identity", NULL);
    }
    return result;
}

static cbm_resolve_result_t go_callable_proof(GoLSPContext *ctx, TSNode node) {
    cbm_resolve_result_t none = {0};
    if (ctx->binding_failed || ts_node_is_null(node))
        return none;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return go_callable_proof(ctx, ts_node_named_child(node, 0));
    if (!strcmp(kind, "identifier")) {
        char *name = lsp_node_text(ctx, node);
        if (!name)
            return none;
        const CBMVarBinding *binding = cbm_scope_lookup_binding(ctx->current_scope, name);
        if (binding) {
            return binding->callable_qn ? go_bound_callable(ctx, binding->callable_qn, node) : none;
        }
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, name);
        return go_scoped_candidate(ctx, qn, CBM_RESOLVE_RULE_SCOPE_BINDING,
                                   CBM_RESOLVE_FACT_SCOPE_BOUND, NULL, node);
    }
    if (strcmp(kind, "selector_expression"))
        return none;
    TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
    TSNode field = ts_node_child_by_field_name(node, "field", 5);
    if (ts_node_is_null(operand) || ts_node_is_null(field))
        return none;
    char *member = lsp_node_text(ctx, field);
    if (!member)
        return none;
    if (!strcmp(ts_node_type(operand), "identifier")) {
        const char *package = resolve_import(ctx, lsp_node_text(ctx, operand));
        if (package) {
            if (member[0] < 'A' || member[0] > 'Z')
                return none;
            return go_scoped_candidate(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", package, member),
                                       CBM_RESOLVE_RULE_IMPORT_BINDING,
                                       CBM_RESOLVE_FACT_IMPORT_BOUND, NULL, node);
        }
    }
    cbm_resolve_type_fact_t fact = go_receiver_fact(ctx, operand);
    if (fact.status != CBM_RESOLVE_TYPE_KNOWN)
        return none;
    const char *owner = fact.type->data.named.qualified_name;
    const CBMRegisteredType *type = cbm_registry_lookup_type(ctx->registry, owner);
    if (!type || type->alias_of)
        return none;
    const char *last = strrchr(owner, '.');
    size_t package_len = last ? (size_t)(last - owner) : 0;
    if ((strlen(ctx->package_qn) != package_len || strncmp(owner, ctx->package_qn, package_len)) &&
        (member[0] < 'A' || member[0] > 'Z'))
        return none;
    /* A direct Go method shadows promoted members. Without a direct method,
     * embedded selector depth/ambiguity is not yet a complete lookup. */
    if (!cbm_registry_lookup_method(ctx->registry, owner, member)) {
        if (type->embedded_types && type->embedded_types[0])
            return none;
        return (cbm_resolve_result_t){
            .analysis = CBM_RESOLVE_ANALYSIS_LIMITED,
            .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
            .facts = CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
            .target = cbm_arena_sprintf(ctx->arena, "%s.%s", owner, member),
            .reason = "method_not_found",
            .proof = fact.evidence.proof};
    }
    cbm_resolve_method_query_t query = {.proof_pool = &ctx->proven_types.proofs,
                                        .proof_source = &ctx->proven_types.proof_source,
                                        .lookup_domain = owner,
                                        .registry = ctx->registry,
                                        .receiver = &fact,
                                        .receiver_qn = owner,
                                        .method_name = member,
                                        .argument_count = -1,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                         CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                        .classify = go_classify_callable,
                                        .context = ctx};
    return cbm_resolve_method(&query).result;
}

/* A resolved callable is necessary but not sufficient: the return annotation
 * must itself come from a supported declaration and resolve to one type. */
static cbm_resolve_type_fact_t go_return_fact(GoLSPContext *ctx, TSNode node) {
    cbm_resolve_type_fact_t unknown = {0};
    TSNode callable = ts_node_child_by_field_name(node, "function", 8);
    cbm_resolve_result_t call = go_callable_proof(ctx, callable);
    if (!cbm_resolve_admit(&call))
        return unknown;
    const char *base =
        cbm_arena_strndup(ctx->arena, call.target, cbm_qn_callable_base_len(call.target));
    cbm_resolve_function_query_t query = {
        .proof_pool = &ctx->proven_types.proofs,
        .proof_source = &ctx->proven_types.proof_source,
        .lookup_domain = base,
        .registry = ctx->registry,
        .function_qn = base,
        .function_binding = {.analysis = call.analysis,
                             .rule = call.rule,
                             .facts = call.facts,
                             .proof = call.proof},
        .argument_count = -1,
        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT | CBM_RESOLVE_METHOD_NAME_IDENTITY,
        .classify = go_classify_callable,
        .context = ctx};
    cbm_resolve_function_result_t selected = cbm_resolve_function(&query);
    const CBMRegisteredFunc *function = selected.method;
    if (!cbm_resolve_admit(&selected.result) || !function ||
        strcmp(call.target, selected.result.target) || !function->source_return_type)
        return unknown;
    const CBMType *type = go_named_base(function->source_return_type);
    const CBMRegisteredType *declaration =
        type ? go_proven_type(ctx, type->data.named.qualified_name) : NULL;
    if (!declaration || declaration->alias_of)
        return unknown;
    cbm_resolve_result_t returned = go_type_proof(ctx, declaration->qualified_name, node);
    returned = cbm_lsp_proof_rule(
        &ctx->proven_types, returned, node, "go-source-return-type", function->declaration_identity,
        "unique-callable;source-return-annotation;exact-package-type", selected.result.proof);
    return (cbm_resolve_type_fact_t){
        .type = type,
        .status = cbm_resolve_admit(&returned) ? CBM_RESOLVE_TYPE_KNOWN : CBM_RESOLVE_TYPE_UNKNOWN,
        .origin = CBM_RESOLVE_TYPE_RETURN,
        .proof_pool = &ctx->proven_types.proofs,
        .evidence = returned};
}

/* Resolve only source forms that prove one callable identity.  This is
 * intentionally narrower than go_eval_expr_type: conditional expressions,
 * indexed dispatch tables, interfaces with several implementers, and unknown
 * locals remain ordinary values. */
static const char *go_exact_callable_target(GoLSPContext *ctx, TSNode node) {
    cbm_resolve_result_t proof = go_callable_proof(ctx, node);
    return cbm_resolve_admit(&proof) ? proof.target : NULL;
}

static void go_scope_bind_value(GoLSPContext *ctx, const char *name, const CBMType *type,
                                const char *callable_qn) {
    bool bound = callable_qn
                     ? cbm_scope_bind_callable_checked(ctx->current_scope, name, type, callable_qn)
                     : cbm_scope_bind_checked(ctx->current_scope, name, type);
    if (!bound)
        ctx->binding_failed = true;
}

static void go_observe_value_type(GoLSPContext *ctx, const char *name, const CBMType *type,
                                  TSNode value, uint32_t at) {
    cbm_resolve_type_fact_t fact = go_receiver_fact(ctx, value);
    const CBMType *base = go_named_base(type);
    if (fact.status == CBM_RESOLVE_TYPE_KNOWN && base &&
        !strcmp(base->data.named.qualified_name, fact.type->data.named.qualified_name)) {
        cbm_resolve_types_observe(&ctx->proven_types, ctx->current_scope, name, at, type,
                                  CBM_RESOLVE_TYPE_ASSIGNMENT, &fact.evidence);
    }
}

static void go_bind_declared(GoLSPContext *ctx, const char *name, const CBMType *type,
                             TSNode written, uint32_t at) {
    go_scope_bind_value(ctx, name, type, NULL);
    go_observe_declared_type(ctx, name, type, written, at);
}

// --- go_process_statement: bind variables from statements ---

void go_process_statement(GoLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    if (!strcmp(kind, "type_spec") || !strcmp(kind, "type_alias")) {
        TSNode local_name = ts_node_child_by_field_name(node, "name", 4);
        char *name = ts_node_is_null(local_name) ? NULL : lsp_node_text(ctx, local_name);
        if (name)
            go_scope_bind_value(ctx, name, cbm_type_unknown(), NULL);
        return;
    }

    // short_var_declaration: a, b := expr
    if (strcmp(kind, "short_var_declaration") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (ts_node_is_null(left) || ts_node_is_null(right))
            return;

        const CBMType *rhs_type = NULL;
        TSNode single_rhs = {0};

        // Check if RHS is an expression_list (multiple values)
        if (strcmp(ts_node_type(right), "expression_list") == 0) {
            uint32_t rhs_count = ts_node_named_child_count(right);
            if (rhs_count == 1) {
                // Single expression that might return a tuple (multi-return)
                single_rhs = ts_node_named_child(right, 0);
                rhs_type = go_eval_expr_type(ctx, single_rhs);
            }
        } else {
            single_rhs = right;
            rhs_type = go_eval_expr_type(ctx, right);
        }
        const char *rhs_callable =
            ts_node_is_null(single_rhs) ? NULL : go_exact_callable_target(ctx, single_rhs);

        // Bind left-hand side variables
        if (strcmp(ts_node_type(left), "expression_list") == 0) {
            uint32_t lhs_count = ts_node_named_child_count(left);
            for (uint32_t i = 0; i < lhs_count; i++) {
                TSNode lhs_var = ts_node_named_child(left, i);
                if (strcmp(ts_node_type(lhs_var), "identifier") != 0)
                    continue;
                char *var_name = lsp_node_text(ctx, lhs_var);
                if (!var_name || strcmp(var_name, "_") == 0)
                    continue;

                const CBMType *var_type = cbm_type_unknown();
                if (rhs_type) {
                    if (rhs_type->kind == CBM_TYPE_TUPLE && (int)i < rhs_type->data.tuple.count) {
                        var_type = rhs_type->data.tuple.elems[i];
                    } else if (i == 0) {
                        var_type = rhs_type;
                    }
                }
                go_scope_bind_value(ctx, var_name, var_type, i == 0 ? rhs_callable : NULL);
                if (i == 0)
                    go_observe_value_type(ctx, var_name, var_type, single_rhs,
                                          ts_node_end_byte(node));
            }
        } else if (strcmp(ts_node_type(left), "identifier") == 0) {
            char *var_name = lsp_node_text(ctx, left);
            if (var_name && strcmp(var_name, "_") != 0 && rhs_type) {
                go_scope_bind_value(ctx, var_name, rhs_type, rhs_callable);
                go_observe_value_type(ctx, var_name, rhs_type, single_rhs, ts_node_end_byte(node));
            }
        }
        return;
    }

    /* Reassignment must update (or clear) callable identity just like :=.
     * Without this, `alias := target; alias = dynamic; alias()` would retain a
     * stale exact target and fabricate a call. */
    if (strcmp(kind, "assignment_statement") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (ts_node_is_null(left) || ts_node_is_null(right))
            return;
        uint32_t lhs_count = strcmp(ts_node_type(left), "expression_list") == 0
                                 ? ts_node_named_child_count(left)
                                 : 1;
        uint32_t rhs_count = strcmp(ts_node_type(right), "expression_list") == 0
                                 ? ts_node_named_child_count(right)
                                 : 1;
        for (uint32_t i = 0; i < lhs_count; i++) {
            TSNode lhs = lhs_count == 1 && strcmp(ts_node_type(left), "expression_list") != 0
                             ? left
                             : ts_node_named_child(left, i);
            if (ts_node_is_null(lhs) || strcmp(ts_node_type(lhs), "identifier") != 0)
                continue;
            char *name = lsp_node_text(ctx, lhs);
            if (!name || strcmp(name, "_") == 0)
                continue;
            TSNode rhs = {0};
            if (rhs_count == lhs_count) {
                rhs = rhs_count == 1 && strcmp(ts_node_type(right), "expression_list") != 0
                          ? right
                          : ts_node_named_child(right, i);
            }
            const CBMType *type =
                ts_node_is_null(rhs) ? cbm_type_unknown() : go_eval_expr_type(ctx, rhs);
            const char *callable = ts_node_is_null(rhs) ? NULL : go_exact_callable_target(ctx, rhs);
            /* Assignment updates the nearest existing frame. Binding into a
             * temporary block would leave a stale outer callable behind. */
            CBMScope *saved = ctx->current_scope;
            while (ctx->current_scope && !cbm_scope_lookup_local(ctx->current_scope, name))
                ctx->current_scope = ctx->current_scope->parent;
            if (ctx->current_scope) {
                go_scope_bind_value(ctx, name, type, lhs_count == 1 ? callable : NULL);
                cbm_resolve_types_observe(&ctx->proven_types, ctx->current_scope, name,
                                          ts_node_end_byte(node), type,
                                          CBM_RESOLVE_TYPE_NO_EVIDENCE, NULL);
            }
            ctx->current_scope = saved;
        }
        return;
    }

    // var_spec: var x Type = expr  OR  var a, b, c Type = expr
    if (strcmp(kind, "var_spec") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        TSNode value_node = ts_node_child_by_field_name(node, "value", 5);

        const CBMType *var_type = cbm_type_unknown();
        TSNode callable_value = value_node;
        if (!ts_node_is_null(value_node) &&
            strcmp(ts_node_type(value_node), "expression_list") == 0) {
            callable_value = ts_node_named_child_count(value_node) == 1
                                 ? ts_node_named_child(value_node, 0)
                                 : (TSNode){0};
        }
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
        const char *callable_qn =
            ts_node_is_null(callable_value) ? NULL : go_exact_callable_target(ctx, callable_value);

        // Bind all name identifiers (handles: var a, b, c int)
        uint32_t vnc = ts_node_child_count(node);
        for (uint32_t vi = 0; vi < vnc; vi++) {
            TSNode ch = ts_node_child(node, vi);
            if (ts_node_is_null(ch) || !ts_node_is_named(ch))
                continue;
            if (strcmp(ts_node_type(ch), "identifier") == 0) {
                char *var_name = lsp_node_text(ctx, ch);
                if (var_name && strcmp(var_name, "_") != 0) {
                    go_scope_bind_value(ctx, var_name, var_type, callable_qn);
                    if (!ts_node_is_null(type_node))
                        go_observe_declared_type(ctx, var_name, var_type, type_node,
                                                 ts_node_end_byte(node));
                    else
                        go_observe_value_type(ctx, var_name, var_type, callable_value,
                                              ts_node_end_byte(node));
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
        if (ts_node_is_null(name_node))
            return;

        const CBMType *const_type = cbm_type_unknown();
        TSNode callable_value = value_node;
        if (!ts_node_is_null(value_node) &&
            strcmp(ts_node_type(value_node), "expression_list") == 0) {
            callable_value = ts_node_named_child_count(value_node) == 1
                                 ? ts_node_named_child(value_node, 0)
                                 : (TSNode){0};
        }
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
        const char *callable_qn =
            ts_node_is_null(callable_value) ? NULL : go_exact_callable_target(ctx, callable_value);

        if (strcmp(ts_node_type(name_node), "identifier") == 0) {
            char *name = lsp_node_text(ctx, name_node);
            if (name && strcmp(name, "_") != 0)
                go_scope_bind_value(ctx, name, const_type, callable_qn);
        }
        return;
    }

    // range_clause: for k, v := range container
    if (strcmp(kind, "range_clause") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (ts_node_is_null(right))
            return;

        const CBMType *container_type = go_eval_expr_type(ctx, right);

        // Determine key and value types based on container type
        const CBMType *key_type = cbm_type_unknown();
        const CBMType *val_type = cbm_type_unknown();

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
                if (strcmp(ts_node_type(var_node), "identifier") != 0)
                    continue;
                char *var_name = lsp_node_text(ctx, var_node);
                if (!var_name || strcmp(var_name, "_") == 0)
                    continue;
                go_scope_bind_value(ctx, var_name, i == 0 ? key_type : val_type, NULL);
            }
        }
        return;
    }

    // type_switch_statement is handled in resolve_calls_in_node, not here
    // (needs per-case scope narrowing, not just variable binding)
}

// --- Emit a resolved call/reference ---

static void go_emit_resolved_kind(GoLSPContext *ctx, const char *callee_qn, const char *strategy,
                                  float confidence, const char *source_name, CBMResolvedKind kind,
                                  TSNode site) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;

    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    /* When a callable alias has a different textual leaf, retain that source
     * name for the exact-site join. */
    rc.reason = source_name;
    rc.kind = kind;
    if (!ts_node_is_null(site)) {
        rc.site_start_byte = ts_node_start_byte(site);
        rc.site_end_byte = ts_node_end_byte(site);
    }
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* Match the extraction carrier's bounded selector-chain normalization while
 * preserving the complete call-expression span as the occurrence identity. */
static const char *go_occurrence_call_spelling(GoLSPContext *ctx, TSNode call) {
    TSNode function = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(function))
        return NULL;
    if (strcmp(ts_node_type(function), "selector_expression"))
        return lsp_node_text(ctx, function);
    TSNode operand = ts_node_child_by_field_name(function, "operand", 7);
    TSNode field = ts_node_child_by_field_name(function, "field", 5);
    if (ts_node_is_null(operand) || ts_node_is_null(field) ||
        strcmp(ts_node_type(operand), "call_expression"))
        return lsp_node_text(ctx, function);
    const char *method = lsp_node_text(ctx, field);
    for (int depth = 0; depth < 4; depth++) {
        TSNode inner = ts_node_child_by_field_name(operand, "function", 8);
        if (ts_node_is_null(inner))
            break;
        if (!strcmp(ts_node_type(inner), "selector_expression")) {
            TSNode next = ts_node_child_by_field_name(inner, "operand", 7);
            if (!ts_node_is_null(next) && !strcmp(ts_node_type(next), "call_expression")) {
                operand = next;
                continue;
            }
        }
        const char *base = lsp_node_text(ctx, inner);
        return base && method ? cbm_arena_sprintf(ctx->arena, "%s.%s", base, method) : method;
    }
    return method;
}

/* A pending target is not a complete semantic result. Capture only its
 * already-proven source/receiver facts and the actual AST occurrence so the
 * metadata-only pass can later combine them with an independently resolved
 * declaration. The row itself remains LIMITED. */
static const cbm_resolve_proof_t *go_pending_occurrence(GoLSPContext *ctx,
                                                        const cbm_resolve_result_t *candidate,
                                                        TSNode site, const char *spelling,
                                                        const char *relation) {
    const cbm_resolve_proof_view_t *prior = cbm_resolve_proof_view(candidate->proof);
    if (!prior || prior->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        !prior->predecessors_closed || !candidate->target || !spelling || !spelling[0] ||
        !ctx->enclosing_func_qn || ts_node_is_null(site))
        return NULL;
    cbm_resolve_proof_source_t source = ctx->proven_types.proof_source;
    source.scope = ctx->enclosing_func_qn;
    cbm_resolve_proof_key_t key = {.lookup_domain = source.scope,
                                   .lookup_key = spelling,
                                   .binding_identity = candidate->target,
                                   .start_byte = ts_node_start_byte(site),
                                   .end_byte = ts_node_end_byte(site),
                                   .origin = source.source_origin,
                                   .flags = candidate->facts,
                                   .detail = relation};
    /* Retain the producer's pending metadata separately from the reusable
     * semantic root: a method candidate carries a declared receiver proof,
     * whose rule and facts intentionally differ from the pending call. */
    cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = candidate->rule,
                                  .facts = CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                                  .reason = "go_pending_ast_occurrence"};
    const cbm_resolve_proof_t *premises[] = {
        candidate->proof, cbm_resolve_proof_leaf(&ctx->proven_types.proofs, CBM_PROOF_SOURCE_FACT,
                                                 &source, &key, &audit)};
    return cbm_resolve_proof_derive(&ctx->proven_types.proofs, CBM_PROOF_NAMED_JOIN, &source, &key,
                                    &prior->outcome, premises, 2, true);
}

static bool go_occurrence_source_equal(const cbm_resolve_proof_source_t *a,
                                       const cbm_resolve_proof_source_t *b) {
    const char *left[] = {
        a->project, a->file, a->module, a->scope, a->content_identity, a->configuration_identity};
    const char *right[] = {
        b->project, b->file, b->module, b->scope, b->content_identity, b->configuration_identity};
    for (size_t i = 0; i < sizeof(left) / sizeof(left[0]); i++)
        if ((left[i] == NULL) != (right[i] == NULL) || (left[i] && strcmp(left[i], right[i])))
            return false;
    return a->source_origin == b->source_origin;
}

/* Both coordinates and roles must come from the earlier AST capture. A legacy
 * row's numeric span or strategy is never sufficient occurrence authority. */
static const cbm_resolve_proof_view_t *go_pending_site(const CBMResolvedCall *row) {
    const cbm_resolve_proof_view_t *capture = cbm_resolve_proof_view(row->evidence.proof);
    if (!capture || capture->kind != CBM_PROOF_NAMED_JOIN || !capture->predecessors_closed ||
        capture->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE || capture->premise_count != 2 ||
        !capture->source.scope || !capture->key.lookup_domain || !capture->key.lookup_key ||
        !capture->key.lookup_key[0] || !capture->key.binding_identity || !capture->key.detail ||
        strcmp(capture->source.scope, row->caller_qn) ||
        strcmp(capture->key.lookup_domain, row->caller_qn) ||
        strcmp(capture->key.binding_identity, row->callee_qn) ||
        capture->key.start_byte != row->site_start_byte ||
        capture->key.end_byte != row->site_end_byte ||
        capture->key.origin != (uint32_t)row->source_origin ||
        capture->key.flags != row->evidence.facts)
        return NULL;
    const char *relation = row->kind == CBM_RESOLVED_INVOCATION ? "CALLS" : "CALL_REFERENCE";
    if (strcmp(capture->key.detail, relation) &&
        !(row->kind == CBM_RESOLVED_CALL_REFERENCE && !strcmp(capture->key.detail, "USAGE")))
        return NULL;
    /* Premises are structurally sorted, so locate the captured AST leaf. */
    for (size_t i = 0; i < capture->premise_count; i++) {
        const cbm_resolve_proof_view_t *leaf = cbm_resolve_proof_view(capture->premises[i]);
        if (leaf && leaf->kind == CBM_PROOF_SOURCE_FACT && leaf->outcome.reason &&
            !strcmp(leaf->outcome.reason, "go_pending_ast_occurrence") &&
            leaf->outcome.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
            leaf->outcome.rule == row->evidence.rule &&
            (leaf->outcome.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT) &&
            go_occurrence_source_equal(&leaf->source, &capture->source) &&
            leaf->key.binding_identity &&
            !strcmp(leaf->key.binding_identity, capture->key.binding_identity) &&
            leaf->key.lookup_domain &&
            !strcmp(leaf->key.lookup_domain, capture->key.lookup_domain) && leaf->key.lookup_key &&
            !strcmp(leaf->key.lookup_key, capture->key.lookup_key) && leaf->key.detail &&
            !strcmp(leaf->key.detail, capture->key.detail) &&
            leaf->key.start_byte == capture->key.start_byte &&
            leaf->key.end_byte == capture->key.end_byte &&
            leaf->key.origin == capture->key.origin && leaf->key.flags == capture->key.flags)
            return capture;
    }
    return NULL;
}

static bool go_emit_proven(GoLSPContext *ctx, const cbm_resolve_result_t *proof,
                           const char *source_name, CBMResolvedKind kind, TSNode site) {
    bool accepted = cbm_resolve_admit(proof);
    if (!proof->target || (!accepted && proof->analysis != CBM_RESOLVE_ANALYSIS_LIMITED) ||
        !ctx->resolved_calls || !ctx->enclosing_func_qn || ts_node_is_null(site))
        return false;
    const char *relation = kind == CBM_RESOLVED_INVOCATION ? "CALLS" : "CALL_REFERENCE";
    if (kind == CBM_RESOLVED_CALL_REFERENCE) {
        TSNode parent = ts_node_parent(site);
        const char *parent_kind = ts_node_is_null(parent) ? "" : ts_node_type(parent);
        if (!strcmp(parent_kind, "literal_element") || !strcmp(parent_kind, "keyed_element") ||
            !strcmp(parent_kind, "binary_expression") || !strcmp(parent_kind, "unary_expression"))
            relation = "USAGE";
    }
    if (kind == CBM_RESOLVED_INVOCATION) {
        source_name = go_occurrence_call_spelling(ctx, site);
    } else if (kind == CBM_RESOLVED_CALL_REFERENCE) {
        for (int depth = 0; depth < 32 && !strcmp(ts_node_type(site), "parenthesized_expression") &&
                            ts_node_named_child_count(site) == 1;
             depth++)
            site = ts_node_named_child(site, 0);
        TSNode spelling = site;
        if (!strcmp(ts_node_type(site), "selector_expression")) {
            TSNode leaf = ts_node_child_by_field_name(site, "field", 5);
            TSNode parent = ts_node_parent(site);
            if (!ts_node_is_null(leaf)) {
                spelling = leaf;
                /* A direct selector argument keeps the complete selector
                 * occurrence but the member leaf spelling in extraction.
                 * Assignment and composite reads retain their leaf site. */
                if (ts_node_is_null(parent) || strcmp(ts_node_type(parent), "argument_list"))
                    site = leaf;
            }
        }
        source_name = lsp_node_text(ctx, spelling);
    }
    cbm_resolve_result_t occurrence = *proof;
    if (accepted) {
        occurrence = cbm_lsp_proof_occurrence(&ctx->proven_types, occurrence, site,
                                              ctx->enclosing_func_qn, source_name, relation);
        if (!cbm_resolve_admit(&occurrence))
            return false;
    } else {
        occurrence.proof = go_pending_occurrence(ctx, proof, site, source_name, relation);
        if (!occurrence.proof)
            return false;
    }
    proof = &occurrence;
    const char *strategy = proof->rule == CBM_RESOLVE_RULE_TYPE_MEMBER        ? "lsp_type_dispatch"
                           : proof->rule == CBM_RESOLVE_RULE_CALLABLE_BINDING ? "lsp_callable_alias"
                                                                              : "lsp_direct";
    const char *leaf = strrchr(proof->target, '.');
    leaf = leaf ? leaf + 1 : proof->target;
    const char *reason = source_name && strcmp(source_name, leaf) ? source_name : NULL;
    if (!accepted) {
        reason = proof->rule == CBM_RESOLVE_RULE_TYPE_MEMBER      ? "method_not_found"
                 : proof->rule == CBM_RESOLVE_RULE_IMPORT_BINDING ? "symbol_not_in_registry"
                                                                  : "function_not_in_registry";
    }
    CBMResolvedCall row = {.caller_qn = ctx->enclosing_func_qn,
                           .callee_qn = proof->target,
                           .strategy = accepted ? strategy : "lsp_unresolved",
                           .confidence = accepted ? 0.97f : 0.0f,
                           .reason = reason,
                           .kind = kind,
                           .site_start_byte = ts_node_start_byte(site),
                           .site_end_byte = ts_node_end_byte(site),
                           .source_origin = CBM_SOURCE_ORIGIN_RAW,
                           .evidence = {.analysis = proof->analysis,
                                        .rule = proof->rule,
                                        .facts = proof->facts,
                                        .proof = proof->proof}};
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, row);
    return true;
}

static void emit_resolved_call(GoLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence, TSNode site) {
    go_emit_resolved_kind(ctx, callee_qn, strategy, confidence, NULL, CBM_RESOLVED_INVOCATION,
                          site);
}

static void emit_resolved_alias_call(GoLSPContext *ctx, const char *callee_qn,
                                     const char *source_name, TSNode site) {
    go_emit_resolved_kind(ctx, callee_qn, "lsp_callable_alias", 0.97f, source_name,
                          CBM_RESOLVED_INVOCATION, site);
}

static void emit_unresolved_reference(GoLSPContext *ctx, const char *source_name, TSNode site) {
    if (!ctx || !ctx->resolved_calls || !ctx->enclosing_func_qn || !source_name ||
        ts_node_is_null(site)) {
        return;
    }
    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = source_name;
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = "callable_value_not_in_registry";
    rc.kind = CBM_RESOLVED_CALL_REFERENCE;
    rc.site_start_byte = ts_node_start_byte(site);
    rc.site_end_byte = ts_node_end_byte(site);
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

// Emit a diagnostic for an unresolved call (confidence 0.0).
static void emit_unresolved_call(GoLSPContext *ctx, const char *expr_text, const char *reason,
                                 TSNode site) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn)
        return;

    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    rc.kind = CBM_RESOLVED_INVOCATION;
    if (!ts_node_is_null(site)) {
        rc.site_start_byte = ts_node_start_byte(site);
        rc.site_end_byte = ts_node_end_byte(site);
    }
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* Only direct argument expressions are candidates. Descending into a
 * conditional/map/index expression would promote its constituent names even
 * though the runtime callable is not statically unique. */
static void go_resolve_value_references_at(GoLSPContext *ctx, TSNode call) {
    TSNode args = ts_node_child_by_field_name(call, "arguments", 9);
    if (ts_node_is_null(args))
        return;
    uint32_t count = ts_node_named_child_count(args);
    for (uint32_t i = 0; i < count; i++) {
        TSNode arg = ts_node_named_child(args, i);
        const char *kind = ts_node_type(arg);
        if (strcmp(kind, "identifier") != 0 && strcmp(kind, "selector_expression") != 0 &&
            strcmp(kind, "parenthesized_expression") != 0)
            continue;
        cbm_resolve_result_t proof = go_callable_proof(ctx, arg);
        char *source_name = lsp_node_text(ctx, arg);
        if (go_emit_proven(ctx, &proof, source_name, CBM_RESOLVED_CALL_REFERENCE, arg)) {
            continue;
        } else if (strcmp(kind, "identifier") == 0 && source_name &&
                   !cbm_scope_contains(ctx->current_scope, source_name)) {
            /* The per-file registry cannot see an unqualified function from
             * another file in the same Go package. Preserve the exact source
             * occurrence for the metadata-only project pass. */
            emit_unresolved_reference(ctx, source_name, arg);
        }
    }
}

/* Assignments evaluate every RHS before any LHS binding changes. Capture only
 * direct callable values here; the existing proof lookup rejects unknown
 * locals, indexed expressions, and ambiguous receivers. */
static void go_resolve_assignment_references_at(GoLSPContext *ctx, TSNode statement) {
    const char *kind = ts_node_type(statement);
    const char *field =
        !strcmp(kind, "short_var_declaration") || !strcmp(kind, "assignment_statement") ? "right"
        : !strcmp(kind, "var_spec")                                                     ? "value"
                                                                                        : NULL;
    if (!field)
        return;
    TSNode values = ts_node_child_by_field_name(statement, field, (uint32_t)strlen(field));
    if (ts_node_is_null(values))
        return;
    if (strcmp(ts_node_type(values), "expression_list")) {
        cbm_resolve_result_t proof = go_callable_proof(ctx, values);
        (void)go_emit_proven(ctx, &proof, NULL, CBM_RESOLVED_CALL_REFERENCE, values);
        return;
    }
    TSTreeCursor cursor = ts_tree_cursor_new(values);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode value = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(value))
                continue;
            cbm_resolve_result_t proof = go_callable_proof(ctx, value);
            (void)go_emit_proven(ctx, &proof, NULL, CBM_RESOLVED_CALL_REFERENCE, value);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

/* Any assignment that is guarded by a branch/loop makes the post-construct
 * callable identity a join of at least two paths. Clear the outer exact
 * identity before walking the construct and after each guarded branch.
 * Assignments update the nearest binding, so a known target written inside
 * a branch is valid there but must not escape to a sibling or the join. */
static void go_invalidate_control_flow_aliases(GoLSPContext *ctx, TSNode node, int depth) {
    if (!ctx || ts_node_is_null(node))
        return;
    if (depth > 64) {
        ctx->binding_failed = true;
        return;
    }
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "func_literal") == 0)
        return;
    if (strcmp(kind, "assignment_statement") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        if (!ts_node_is_null(left)) {
            uint32_t count = strcmp(ts_node_type(left), "expression_list") == 0
                                 ? ts_node_named_child_count(left)
                                 : 1;
            for (uint32_t i = 0; i < count; i++) {
                TSNode target = count == 1 && strcmp(ts_node_type(left), "expression_list") != 0
                                    ? left
                                    : ts_node_named_child(left, i);
                if (ts_node_is_null(target) || strcmp(ts_node_type(target), "identifier") != 0) {
                    continue;
                }
                char *name = lsp_node_text(ctx, target);
                if (name && cbm_scope_lookup_callable(ctx->current_scope, name)) {
                    cbm_scope_update_callable(ctx->current_scope, name, NULL);
                }
            }
        }
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++) {
        go_invalidate_control_flow_aliases(ctx, ts_node_named_child(node, i), depth + 1);
    }
}

/* The generated external registry records interface membership explicitly,
 * although it omits separate function rows for those signatures. This exact
 * member list supports a declaration diagnostic; it does not establish project
 * source or runtime implementation identity and carries no emission proof. */
static const char *go_external_interface_member(GoLSPContext *ctx, const char *interface_qn,
                                                const char *member) {
    const CBMRegisteredType *type =
        interface_qn ? cbm_registry_lookup_type(ctx->registry, interface_qn) : NULL;
    if (!type || type->project_source || !type->is_interface || !type->method_names || !member)
        return NULL;
    for (size_t i = 0; type->method_names[i]; i++)
        if (!strcmp(type->method_names[i], member))
            return cbm_arena_sprintf(ctx->arena, "%s.%s", type->qualified_name, member);
    return NULL;
}

// --- Walk call expressions and resolve them ---

static void resolve_calls_in_node_inner(GoLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    // Capture RHS values before statements update their lexical bindings.
    go_resolve_assignment_references_at(ctx, node);
    go_process_statement(ctx, node);

    bool control_flow = strcmp(kind, "if_statement") == 0 || strcmp(kind, "for_statement") == 0 ||
                        strcmp(kind, "expression_switch_statement") == 0 ||
                        strcmp(kind, "type_switch_statement") == 0 ||
                        strcmp(kind, "select_statement") == 0;
    if (control_flow)
        go_invalidate_control_flow_aliases(ctx, node, 0);

    /* Preserve exact declaration identities for leaves inside composite
     * values. The extraction role keeps these USAGE rather than claiming
     * the complete expression is one statically selected callable. */
    if (!strcmp(kind, "identifier") || !strcmp(kind, "selector_expression")) {
        TSNode parent = ts_node_parent(node);
        const char *parent_kind = ts_node_is_null(parent) ? "" : ts_node_type(parent);
        if (!strcmp(parent_kind, "literal_element") || !strcmp(parent_kind, "keyed_element") ||
            !strcmp(parent_kind, "binary_expression") || !strcmp(parent_kind, "unary_expression")) {
            cbm_resolve_result_t proof = go_callable_proof(ctx, node);
            (void)go_emit_proven(ctx, &proof, lsp_node_text(ctx, node), CBM_RESOLVED_CALL_REFERENCE,
                                 node);
        }
    }

    // Resolve call expressions
    if (strcmp(kind, "call_expression") == 0) {
        go_resolve_value_references_at(ctx, node);
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        if (!ts_node_is_null(func_node)) {
            const char *fk = ts_node_type(func_node);
            char *source_name = lsp_node_text(ctx, func_node);
            cbm_resolve_result_t proof = go_callable_proof(ctx, func_node);
            if (!(source_name && !strcmp(fk, "identifier") && is_go_builtin_func(source_name)) &&
                go_emit_proven(ctx, &proof, source_name, CBM_RESOLVED_INVOCATION, node))
                goto recurse;

            // selector_expression: obj.Method() or pkg.Func()
            if (strcmp(fk, "selector_expression") == 0) {
                TSNode operand = ts_node_child_by_field_name(func_node, "operand", 7);
                TSNode field = ts_node_child_by_field_name(func_node, "field", 5);
                if (!ts_node_is_null(operand) && !ts_node_is_null(field)) {
                    char *field_name = lsp_node_text(ctx, field);

                    // Check if operand is a package import
                    if (strcmp(ts_node_type(operand), "identifier") == 0) {
                        char *pkg_name = lsp_node_text(ctx, operand);
                        if (pkg_name) {
                            const char *pkg_qn = resolve_import(ctx, pkg_name);
                            if (pkg_qn && field_name) {
                                const CBMRegisteredFunc *f =
                                    cbm_registry_lookup_symbol(ctx->registry, pkg_qn, field_name);
                                if (f) {
                                    emit_resolved_call(ctx, f->qualified_name, "lsp_direct", 0.95f,
                                                       node);
                                    goto recurse;
                                }
                                // Package found but symbol not in registry
                                emit_unresolved_call(
                                    ctx,
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", pkg_name, field_name),
                                    "symbol_not_in_registry", node);
                                goto recurse;
                            }
                        }
                    }

                    // Type-based method dispatch
                    if (field_name) {
                        const CBMType *recv_type = go_eval_expr_type(ctx, operand);
                        const CBMType *base = recv_type;
                        if (base && base->kind == CBM_TYPE_POINTER)
                            base = cbm_type_deref(base);

                        if (base && base->kind == CBM_TYPE_NAMED) {
                            const char *recv_qn = base->data.named.qualified_name;
                            const CBMRegisteredType *receiver_type =
                                cbm_registry_lookup_type(ctx->registry, recv_qn);
                            const char *alt_qn = NULL;
                            /* Re-qualify NAMED receivers that embed an import
                             * alias segment (cross-package field type texts) —
                             * the real type only exists under the import QN. */
                            if (!receiver_type) {
                                alt_qn = go_requalify_via_imports(ctx, recv_qn);
                                if (alt_qn) {
                                    receiver_type = cbm_registry_lookup_type(ctx->registry, alt_qn);
                                    if (receiver_type)
                                        recv_qn = alt_qn;
                                }
                            }
                            /* Registered interface receivers must reach the
                             * interface-resolution branch below. Their semantic
                             * method registrations are signatures, not concrete
                             * dispatch targets. */
                            if (!receiver_type || !receiver_type->is_interface) {
                                const CBMRegisteredFunc *method =
                                    go_lookup_field_or_method(ctx, recv_qn, field_name);
                                if (method) {
                                    const char *strategy = "lsp_type_dispatch";
                                    if (method->receiver_type &&
                                        strcmp(method->receiver_type, recv_qn) != 0) {
                                        strategy = "lsp_embed_dispatch";
                                    }
                                    emit_resolved_call(ctx, method->qualified_name, strategy, 0.95f,
                                                       node);
                                    goto recurse;
                                }
                            }
                        }

                        // Interface dispatch: NAMED type that is an interface, or bare INTERFACE
                        // type
                        if (base && field_name) {
                            bool is_iface = (base->kind == CBM_TYPE_INTERFACE);
                            const char *iface_qn = NULL;
                            if (!is_iface && base->kind == CBM_TYPE_NAMED) {
                                const CBMRegisteredType *rt = cbm_registry_lookup_type(
                                    ctx->registry, base->data.named.qualified_name);
                                if (!rt) {
                                    const char *alt_qn = go_requalify_via_imports(
                                        ctx, base->data.named.qualified_name);
                                    if (alt_qn)
                                        rt = cbm_registry_lookup_type(ctx->registry, alt_qn);
                                }
                                if (rt && rt->is_interface) {
                                    is_iface = true;
                                    iface_qn = rt->qualified_name;
                                }
                            }
                            if (is_iface) {
                                /* CALLS denotes the statically selected declaration.
                                 * The number of indexed implementations cannot prove
                                 * the dynamic receiver used at runtime. */
                                const CBMRegisteredFunc *method =
                                    iface_qn ? cbm_registry_lookup_method(ctx->registry, iface_qn,
                                                                          field_name)
                                             : NULL;
                                const char *external =
                                    method
                                        ? NULL
                                        : go_external_interface_member(ctx, iface_qn, field_name);
                                if (method)
                                    emit_resolved_call(ctx, method->qualified_name,
                                                       "lsp_interface_dispatch", 0.95f, node);
                                else if (external)
                                    emit_resolved_call(ctx, external, "lsp_interface_dispatch",
                                                       0.95f, node);
                                else
                                    emit_unresolved_call(ctx, field_name,
                                                         "interface_declaration_not_found", node);
                                goto recurse;
                            }
                        }

                        // Type resolved to NAMED but neither method nor interface matched
                        if (base && base->kind == CBM_TYPE_NAMED) {
                            emit_unresolved_call(ctx,
                                                 cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                                   base->data.named.qualified_name,
                                                                   field_name),
                                                 "method_not_found", node);
                        } else if (cbm_type_is_unknown(recv_type)) {
                            char *operand_text = lsp_node_text(ctx, operand);
                            emit_unresolved_call(
                                ctx,
                                cbm_arena_sprintf(ctx->arena, "%s.%s",
                                                  operand_text ? operand_text : "?", field_name),
                                "unknown_receiver_type", node);
                        }
                    }
                }
            }

            // Direct function call: FuncName()
            if (strcmp(fk, "identifier") == 0) {
                char *name = lsp_node_text(ctx, func_node);
                if (name && !is_go_builtin_func(name)) {
                    if (cbm_scope_contains(ctx->current_scope, name)) {
                        const char *alias_target =
                            cbm_scope_lookup_callable(ctx->current_scope, name);
                        if (alias_target) {
                            emit_resolved_alias_call(ctx, alias_target, name, node);
                        } else {
                            emit_unresolved_call(ctx, name, "lexical_value_not_exact_callable",
                                                 node);
                        }
                        goto recurse;
                    }
                    // Package-local function
                    const CBMRegisteredFunc *f =
                        cbm_registry_lookup_symbol(ctx->registry, ctx->package_qn, name);
                    if (f) {
                        emit_resolved_call(ctx, f->qualified_name, "lsp_direct", 0.95f, node);
                    } else {
                        emit_unresolved_call(ctx, name, "function_not_in_registry", node);
                    }
                }
            }
        }
    }

recurse:;
    // Push scope for blocks and statements that introduce variables
    bool push_scope =
        (strcmp(kind, "block") == 0 || strcmp(kind, "if_statement") == 0 ||
         strcmp(kind, "for_statement") == 0 || strcmp(kind, "expression_switch_statement") == 0);
    if (push_scope) {
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    }

    // Process initializer field for if/for/switch statements.
    // Go patterns: if err := f(); ..., for i := 0; ..., switch v := x; v { ... }
    if (strcmp(kind, "if_statement") == 0 || strcmp(kind, "for_statement") == 0 ||
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
        const char *switch_var = NULL;
        // Pass 1: find switch variable and operand type
        uint32_t nc2 = ts_node_child_count(node);
        bool found_assign = false;
        for (uint32_t i = 0; i < nc2; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child))
                continue;
            const char *ck = ts_node_type(child);

            // expression_list before := is the LHS (variable name)
            if (!found_assign && strcmp(ck, "expression_list") == 0) {
                TSNode var_node = ts_node_named_child(child, 0);
                if (!ts_node_is_null(var_node) && strcmp(ts_node_type(var_node), "identifier") == 0)
                    switch_var = lsp_node_text(ctx, var_node);
            }
            // := operator marks the assignment
            if (!ts_node_is_named(child)) {
                char *tok = lsp_node_text(ctx, child);
                if (tok && strcmp(tok, ":=") == 0)
                    found_assign = true;
            }
            // identifier after := is the operand being type-switched
            if (found_assign && strcmp(ck, "identifier") == 0) {
                (void)go_eval_expr_type(ctx, child);
            }
        }

        // Pass 2: process each type_case with narrowed scope
        for (uint32_t i = 0; i < nc2; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child) || !ts_node_is_named(child))
                continue;
            if (strcmp(ts_node_type(child), "type_case") != 0)
                continue;

            CBMScope *saved = ctx->current_scope;
            ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

            // Find case type and bind switch variable
            uint32_t cc_count = ts_node_child_count(child);
            for (uint32_t j = 0; j < cc_count; j++) {
                TSNode cc_child = ts_node_child(child, j);
                if (ts_node_is_null(cc_child) || !ts_node_is_named(cc_child))
                    continue;
                const char *cc_kind = ts_node_type(cc_child);
                if (strcmp(cc_kind, "type_identifier") == 0 ||
                    strcmp(cc_kind, "qualified_type") == 0 ||
                    strcmp(cc_kind, "pointer_type") == 0 || strcmp(cc_kind, "slice_type") == 0) {
                    if (switch_var) {
                        go_scope_bind_value(ctx, switch_var, go_parse_type_node(ctx, cc_child),
                                            NULL);
                    }
                    break;
                }
            }

            // Recurse into case body statements
            for (uint32_t j = 0; j < cc_count; j++) {
                TSNode cc_child = ts_node_child(child, j);
                if (ts_node_is_null(cc_child) || !ts_node_is_named(cc_child))
                    continue;
                const char *cc_kind = ts_node_type(cc_child);
                // Skip type nodes, process everything else (expression_statement, etc.)
                if (strcmp(cc_kind, "type_identifier") == 0 ||
                    strcmp(cc_kind, "qualified_type") == 0 ||
                    strcmp(cc_kind, "pointer_type") == 0 || strcmp(cc_kind, "slice_type") == 0)
                    continue;
                resolve_calls_in_node(ctx, cc_child);
            }

            ctx->current_scope = saved;
            go_invalidate_control_flow_aliases(ctx, child, 0);
        }

        if (push_scope)
            ctx->current_scope = cbm_scope_pop(ctx->current_scope);
        return;
    }

    // Select statement: each communication_case gets its own scope
    // case msg := <-ch:  →  receive_statement has left (vars) + unary_expression (<-ch)
    if (strcmp(kind, "select_statement") == 0) {
        uint32_t nc3 = ts_node_child_count(node);
        for (uint32_t i = 0; i < nc3; i++) {
            TSNode child = ts_node_child(node, i);
            if (ts_node_is_null(child) || !ts_node_is_named(child))
                continue;
            const char *ck3 = ts_node_type(child);

            if (strcmp(ck3, "communication_case") == 0 || strcmp(ck3, "default_case") == 0) {
                CBMScope *saved = ctx->current_scope;
                ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

                // Process children: receive_statement binds vars, then recurse body
                uint32_t cc_count = ts_node_child_count(child);
                for (uint32_t j = 0; j < cc_count; j++) {
                    TSNode cc_child = ts_node_child(child, j);
                    if (ts_node_is_null(cc_child) || !ts_node_is_named(cc_child))
                        continue;
                    const char *cc_kind = ts_node_type(cc_child);

                    if (strcmp(cc_kind, "receive_statement") == 0) {
                        // receive_statement: left := <-right
                        // tree-sitter Go: right field is the channel expression (after <-)
                        // The receive value type is the channel's element type
                        TSNode left = ts_node_child_by_field_name(cc_child, "left", 4);
                        TSNode right = ts_node_child_by_field_name(cc_child, "right", 5);
                        if (!ts_node_is_null(right)) {
                            const CBMType *right_type = go_eval_expr_type(ctx, right);
                            // right_type might be a channel (if right is the channel)
                            // or already the elem type (if right is a <-ch unary expr)
                            const CBMType *recv_type = right_type;
                            if (right_type && right_type->kind == CBM_TYPE_CHANNEL) {
                                recv_type = right_type->data.channel.elem;
                            }
                            if (!ts_node_is_null(left)) {
                                if (strcmp(ts_node_type(left), "expression_list") == 0) {
                                    uint32_t lhs_count = ts_node_named_child_count(left);
                                    for (uint32_t k = 0; k < lhs_count; k++) {
                                        TSNode var_node = ts_node_named_child(left, k);
                                        if (strcmp(ts_node_type(var_node), "identifier") != 0)
                                            continue;
                                        char *var_name = lsp_node_text(ctx, var_node);
                                        if (!var_name || strcmp(var_name, "_") == 0)
                                            continue;
                                        if (k == 0) {
                                            go_scope_bind_value(ctx, var_name, recv_type, NULL);
                                        } else {
                                            // second var is the ok bool
                                            go_scope_bind_value(
                                                ctx, var_name, cbm_type_builtin(ctx->arena, "bool"),
                                                NULL);
                                        }
                                    }
                                } else if (strcmp(ts_node_type(left), "identifier") == 0) {
                                    char *var_name = lsp_node_text(ctx, left);
                                    if (var_name && strcmp(var_name, "_") != 0) {
                                        go_scope_bind_value(ctx, var_name, recv_type, NULL);
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
                go_invalidate_control_flow_aliases(ctx, child, 0);
            }
        }
        if (push_scope)
            ctx->current_scope = cbm_scope_pop(ctx->current_scope);
        return;
    }

    // Recurse into children via a cursor (O(n)); ts_node_child(node,i) is O(i)
    // → O(n²) on a wide node.
    // The cursor is this depth's pooled one (cbm_cursor_acquire): the recursion
    // below runs one level deeper and never touches it.
    {
        cbm_cursor_lease_t lease;
        TSTreeCursor *cursor = cbm_cursor_acquire(&lease, ctx->walk_depth, node);
        if (ts_tree_cursor_goto_first_child(cursor)) {
            do {
                TSNode child = ts_tree_cursor_current_node(cursor);
                resolve_calls_in_node(ctx, child);
                const char *child_kind = ts_node_type(child);
                if (control_flow &&
                    (!strcmp(child_kind, "block") || !strcmp(child_kind, "if_statement") ||
                     !strcmp(child_kind, "expression_case") ||
                     !strcmp(child_kind, "default_case") || !strcmp(child_kind, "for_clause")))
                    go_invalidate_control_flow_aliases(ctx, child, 0);
            } while (ts_tree_cursor_goto_next_sibling(cursor));
        }
        cbm_cursor_release(&lease);
    }

    if (push_scope) {
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
    }
}

// --- Process a single function body ---

static void go_bind_type_parameters(GoLSPContext *ctx, TSNode func_node) {
    /* Type parameters occupy the lexical type namespace. Until constraint
     * members carry their own declaration proof, do not borrow a package
     * type with the same spelling. */
    TSNode type_parameters = ts_node_child_by_field_name(func_node, "type_parameters", 15);
    if (ts_node_is_null(type_parameters))
        return;
    TSTreeCursor type_cursor = ts_tree_cursor_new(type_parameters);
    if (ts_tree_cursor_goto_first_child(&type_cursor))
        do {
            TSNode parameter = ts_tree_cursor_current_node(&type_cursor);
            TSTreeCursor name_cursor = ts_tree_cursor_new(parameter);
            if (ts_tree_cursor_goto_first_child(&name_cursor))
                do {
                    TSNode name = ts_tree_cursor_current_node(&name_cursor);
                    const char *field = ts_tree_cursor_current_field_name(&name_cursor);
                    if (field && !strcmp(field, "name") &&
                        !strcmp(ts_node_type(name), "identifier")) {
                        char *text = lsp_node_text(ctx, name);
                        if (text)
                            go_scope_bind_value(ctx, text, cbm_type_unknown(), NULL);
                    }
                } while (ts_tree_cursor_goto_next_sibling(&name_cursor));
            ts_tree_cursor_delete(&name_cursor);
        } while (ts_tree_cursor_goto_next_sibling(&type_cursor));
    ts_tree_cursor_delete(&type_cursor);
}

static void process_function(GoLSPContext *ctx, TSNode func_node) {
    // Set enclosing function QN
    TSNode name_node = ts_node_child_by_field_name(func_node, "name", 4);
    if (ts_node_is_null(name_node))
        return;

    char *func_name = lsp_node_text(ctx, name_node);
    if (!func_name || !func_name[0])
        return;

    // Enclosing-function QN must be EXACTLY the QN the textual call events
    // (extract_unified.c compute_func_qn) and the defs pass produce, or the
    // caller-QN join in cbm_pipeline_find_lsp_resolution breaks and the
    // LSP-resolved call silently falls back to the registry short-name
    // resolver. Since #1909 receiver-qualified Go method QNs, that shared
    // form is package_qn.ReceiverType.method for methods (the ONE-formula
    // contract: cbm_go_receiver_type_name) and package_qn.func for functions.
    // A package may hold any number of init() functions, so each one is
    // package_qn.init#<file>:<N> (#1910, cbm_go_init_qn — the same formula).
    ctx->enclosing_func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->package_qn, func_name);
    if (strcmp(ts_node_type(func_node), "method_declaration") == 0) {
        TSNode recv = ts_node_child_by_field_name(func_node, "receiver", 8);
        if (!ts_node_is_null(recv)) {
            char *recv_type = cbm_go_receiver_type_name(ctx->arena, recv, ctx->source);
            if (recv_type && recv_type[0]) {
                ctx->enclosing_func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s.%s", ctx->package_qn,
                                                           recv_type, func_name);
            }
        }
    } else if (strcmp(func_name, "init") == 0) {
        ctx->enclosing_func_qn = cbm_go_init_qn(ctx->arena, ctx->enclosing_func_qn, ctx->rel_path,
                                                func_node, ctx->source);
    }

    // Push function scope
    CBMScope *saved_scope = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);

    go_bind_type_parameters(ctx, func_node);

    // Bind parameters into scope (including variadic)
    TSNode params = ts_node_child_by_field_name(func_node, "parameters", 10);
    if (!ts_node_is_null(params)) {
        uint32_t nc = ts_node_child_count(params);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode param = ts_node_child(params, i);
            if (ts_node_is_null(param) || !ts_node_is_named(param))
                continue;
            const char *pk = ts_node_type(param);

            bool is_variadic = (strcmp(pk, "variadic_parameter_declaration") == 0);
            if (!is_variadic && strcmp(pk, "parameter_declaration") != 0)
                continue;

            // Get type
            TSNode type_node = ts_node_child_by_field_name(param, "type", 4);
            const CBMType *param_type = go_parse_type_node(ctx, type_node);

            // Variadic: ...T is []T in the function body
            if (is_variadic && param_type) {
                param_type = cbm_type_slice(ctx->arena, param_type);
            }

            // Get name(s) — Go allows multiple names per declaration: a, b int
            uint32_t pnc = ts_node_child_count(param);
            for (uint32_t j = 0; j < pnc; j++) {
                TSNode child = ts_node_child(param, j);
                if (ts_node_is_null(child) || !ts_node_is_named(child))
                    continue;
                if (strcmp(ts_node_type(child), "identifier") == 0) {
                    char *pname = lsp_node_text(ctx, child);
                    if (pname && strcmp(pname, "_") != 0) {
                        go_bind_declared(ctx, pname, param_type, type_node,
                                         ts_node_end_byte(param));
                    }
                }
            }
        }
    }

    // Bind named return values into scope
    TSNode result_node = ts_node_child_by_field_name(func_node, "result", 6);
    if (!ts_node_is_null(result_node)) {
        // result can be a parameter_list (named returns) or a simple type
        const char *rk = ts_node_type(result_node);
        if (strcmp(rk, "parameter_list") == 0) {
            uint32_t rnc = ts_node_child_count(result_node);
            for (uint32_t i = 0; i < rnc; i++) {
                TSNode rparam = ts_node_child(result_node, i);
                if (ts_node_is_null(rparam) || !ts_node_is_named(rparam))
                    continue;
                if (strcmp(ts_node_type(rparam), "parameter_declaration") != 0)
                    continue;

                TSNode rtype = ts_node_child_by_field_name(rparam, "type", 4);
                const CBMType *ret_type = go_parse_type_node(ctx, rtype);

                uint32_t rpnc = ts_node_child_count(rparam);
                for (uint32_t j = 0; j < rpnc; j++) {
                    TSNode child = ts_node_child(rparam, j);
                    if (ts_node_is_null(child) || !ts_node_is_named(child))
                        continue;
                    if (strcmp(ts_node_type(child), "identifier") == 0) {
                        char *rname = lsp_node_text(ctx, child);
                        if (rname && strcmp(rname, "_") != 0) {
                            go_bind_declared(ctx, rname, ret_type, rtype, ts_node_end_byte(rparam));
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
            if (ts_node_is_null(rp) || !ts_node_is_named(rp))
                continue;
            if (strcmp(ts_node_type(rp), "parameter_declaration") != 0)
                continue;

            TSNode rtype = ts_node_child_by_field_name(rp, "type", 4);
            const CBMType *recv_type = go_parse_type_node(ctx, rtype);

            // Find receiver name
            uint32_t rpnc = ts_node_child_count(rp);
            for (uint32_t j = 0; j < rpnc; j++) {
                TSNode rc = ts_node_child(rp, j);
                if (!ts_node_is_null(rc) && ts_node_is_named(rc) &&
                    strcmp(ts_node_type(rc), "identifier") == 0) {
                    char *rname = lsp_node_text(ctx, rc);
                    if (rname && strcmp(rname, "_") != 0) {
                        go_bind_declared(ctx, rname, recv_type, rtype, ts_node_end_byte(rp));
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

void go_lsp_process_file(GoLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;

    // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
    uint32_t kn = 0;
    TSNode *kids = cbm_lsp_collect_children(ctx->arena, root, &kn);

    // Pass 1: Bind package-level var/const declarations into root scope.
    // Must happen before processing functions so all globals are visible.
    for (uint32_t i = 0; i < kn; i++) {
        TSNode child = kids[i];
        const char *kind = ts_node_type(child);

        if (strcmp(kind, "var_declaration") == 0 || strcmp(kind, "const_declaration") == 0) {
            uint32_t vnc = ts_node_child_count(child);
            for (uint32_t j = 0; j < vnc; j++) {
                TSNode spec = ts_node_child(child, j);
                if (ts_node_is_null(spec) || !ts_node_is_named(spec))
                    continue;
                go_process_statement(ctx, spec);
            }
        }
    }

    // Pass 2: Process function/method bodies
    for (uint32_t i = 0; i < kn; i++) {
        TSNode child = kids[i];
        const char *kind = ts_node_type(child);

        if (strcmp(kind, "function_declaration") == 0 || strcmp(kind, "method_declaration") == 0) {
            process_function(ctx, child);
        }
    }
}

// --- Helper: parse Go return type text into CBMType ---

const CBMType *cbm_parse_return_type_text(CBMArena *a, const char *text, const char *module_qn) {
    if (!text || !text[0])
        return cbm_type_unknown();

    // Pointer: *Foo
    if (text[0] == '*') {
        return cbm_type_pointer(a, cbm_parse_return_type_text(a, text + 1, module_qn));
    }

    // Slice: []Foo
    if (text[0] == '[' && text[1] == ']') {
        return cbm_type_slice(a, cbm_parse_return_type_text(a, text + 2, module_qn));
    }

    // Builtin types
    static const char *builtins[] = {"int",     "int8",    "int16",  "int32",   "int64",
                                     "uint",    "uint8",   "uint16", "uint32",  "uint64",
                                     "float32", "float64", "string", "bool",    "byte",
                                     "rune",    "error",   "any",    "uintptr", NULL};
    for (const char **b = builtins; *b; b++) {
        if (strcmp(text, *b) == 0)
            return cbm_type_builtin(a, text);
    }

    // Named type: assume local to module
    return cbm_type_named(a, cbm_arena_sprintf(a, "%s.%s", module_qn, text));
}

typedef struct {
    const char *module_qn;
} CBMGoLSPSignatureParserContext;

static const CBMType *cbm_go_lsp_parse_signature_param_text(CBMArena *arena, const char *text,
                                                            void *parser_ctx) {
    const CBMGoLSPSignatureParserContext *ctx = (const CBMGoLSPSignatureParserContext *)parser_ctx;
    const char *module_qn = ctx && ctx->module_qn ? ctx->module_qn : "";
    return cbm_parse_return_type_text(arena, text, module_qn);
}

// --- Shared Go stdlib registry ---

/* The Go stdlib (2,328 functions, 321 types) registered ONCE per process into
 * a finalized, sealed registry that every per-file registry chains to as its
 * fallback -- the overlay contract of type_registry.h: lookups and the chain
 * iterators reach it, refinements copy into the head, nothing writes it.
 * Registered into each file's registry instead, it was 13.4 k re-registrations
 * on the Go corpus, 2.5 GB of copies, and every lookup of the unfinalized
 * per-file registry scanned the stdlib linearly (waste sanitizer, 2026-09-17).
 * Threads racing the first build each build a copy and the first to publish
 * wins; the others free theirs -- no thread waits. NULL on allocation
 * failure: the caller registers the stdlib into its own registry as before. */
typedef struct {
    CBMArena arena;
    CBMTypeRegistry reg;
} go_stdlib_holder_t;

static _Atomic(go_stdlib_holder_t *) g_go_stdlib_shared;

static const CBMTypeRegistry *go_stdlib_shared(void) {
    go_stdlib_holder_t *ready = atomic_load_explicit(&g_go_stdlib_shared, memory_order_acquire);
    if (ready)
        return &ready->reg;
    go_stdlib_holder_t *built = cbm_calloc(CBM_MEM_CLASS_OTHER, sizeof(*built));
    if (!built)
        return NULL;
    cbm_arena_init(&built->arena);
    if (built->arena.nblocks == 0) {
        cbm_free(CBM_MEM_CLASS_OTHER, built);
        return NULL;
    }
    cbm_registry_init(&built->reg, &built->arena);
    cbm_go_stdlib_register(&built->reg, &built->arena);
    cbm_registry_finalize(&built->reg);
    built->reg.read_only = true;
    go_stdlib_holder_t *expected = NULL;
    if (atomic_compare_exchange_strong_explicit(&g_go_stdlib_shared, &expected, built,
                                                memory_order_acq_rel, memory_order_acquire)) {
        return &built->reg;
    }
    cbm_arena_destroy(&built->arena);
    cbm_free(CBM_MEM_CLASS_OTHER, built);
    return &expected->reg;
}

/* A per-file registry sees the stdlib: chained to the shared copy, or
 * registered into it when the shared copy cannot be built. */
static void go_registry_with_stdlib(CBMTypeRegistry *reg, CBMArena *arena) {
    const CBMTypeRegistry *shared = go_stdlib_shared();
    if (shared) {
        reg->fallback = shared;
    } else {
        cbm_go_stdlib_register(reg, arena);
    }
}

/* The first return rule supports one local named type, optionally a pointer,
 * on a non-generic free function. Richer annotations remain unknown. */
static CBMHashTable *go_source_returns(CBMArena *arena, TSNode root, const char *source,
                                       const char *module) {
    if (ts_node_has_error(root))
        return NULL;
    CBMHashTable *facts = cbm_ht_create(32);
    if (!facts)
        return NULL;
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode function = ts_tree_cursor_current_node(&cursor);
            if (strcmp(ts_node_type(function), "function_declaration") ||
                !ts_node_is_null(ts_node_child_by_field_name(function, "type_parameters", 15)))
                continue;
            TSNode name = ts_node_child_by_field_name(function, "name", 4);
            TSNode result = ts_node_child_by_field_name(function, "result", 6);
            if (ts_node_is_null(name) || ts_node_is_null(result))
                continue;
            bool pointer = !strcmp(ts_node_type(result), "pointer_type");
            if (pointer && ts_node_named_child_count(result) == 1)
                result = ts_node_named_child(result, 0);
            if (strcmp(ts_node_type(result), "type_identifier"))
                continue;
            const char *type_name = cbm_node_text(arena, result, source);
            const char *qn =
                type_name ? cbm_arena_sprintf(arena, "%s.%s", module, type_name) : NULL;
            const CBMType *type = qn ? cbm_type_named(arena, qn) : NULL;
            if (type && pointer)
                type = cbm_type_pointer(arena, type);
            const char *function_name = cbm_node_text(arena, name, source);
            const char *key =
                function_name
                    ? cbm_arena_sprintf(arena, "%u:%s", ts_node_start_point(function).row + 1,
                                        function_name)
                    : NULL;
            if (key && type)
                cbm_ht_set(facts, key, (void *)type);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return facts;
}

bool cbm_go_collect_source_facts(CBMArena *arena, const char *source, int length,
                                 const char *module, CBMLSPDef *defs, int count) {
    for (int i = 0; i < count; i++)
        defs[i].source_return_type = NULL;
    if (!source || length <= 0 || !module)
        return false;
    extern const TSLanguage *tree_sitter_go(void);
    TSParser *parser = ts_parser_new();
    if (!parser)
        return false;
    ts_parser_set_language(parser, tree_sitter_go());
    TSTree *tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)length);
    ts_parser_delete(parser);
    if (!tree)
        return false;
    CBMHashTable *facts = go_source_returns(arena, ts_tree_root_node(tree), source, module);
    for (int i = 0; facts && i < count; i++) {
        if (!defs[i].label || strcmp(defs[i].label, "Function") || !defs[i].short_name)
            continue;
        const char *key =
            cbm_arena_sprintf(arena, "%u:%s", defs[i].source_start_line, defs[i].short_name);
        if (key)
            defs[i].source_return_type = cbm_ht_get(facts, key);
    }
    bool ok = facts != NULL;
    cbm_ht_free(facts);
    ts_tree_delete(tree);
    return ok;
}

// --- Entry point: build registry from file defs + run LSP ---

void cbm_run_go_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                    TSNode root, const char *rel_path) {

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    // Go stdlib types/functions: the shared sealed registry, chained
    go_registry_with_stdlib(&reg, arena);

    const char *module_qn = result->module_qn;
    CBMHashTable *return_facts = go_source_returns(arena, root, source, module_qn);

    // Phase 1: Register all types and functions from the file's own definitions
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name)
            continue;

        // Register every type-like container (Class/Struct/Type/Interface/Enum/
        // Trait). Struct included so a Go `type T struct {...}` (now labelled
        // "Struct") is registered as a type and its methods/embedding resolve.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->name;
            rt.project_source = true;
            rt.graph_qualified_name = d->qualified_name;
            rt.declaration_identity = cbm_arena_sprintf(
                arena, "%s:%d", d->file_path ? d->file_path : result->module_qn, i);
            rt.is_interface = d->label && strcmp(d->label, "Interface") == 0;

            // Populate embedded_types from base_classes (Go struct embedding)
            if (d->base_classes) {
                int bc_count = 0;
                while (d->base_classes[bc_count])
                    bc_count++;
                if (bc_count > 0) {
                    const char **embedded = (const char **)cbm_arena_alloc(
                        arena, (bc_count + 1) * sizeof(const char *));
                    for (int j = 0; j < bc_count; j++) {
                        const char *bc = d->base_classes[j];
                        // Strip pointer prefix for embedded *Type
                        while (bc[0] == '*')
                            bc++;
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
            rf.flags |= CBM_FUNC_FLAG_PROJECT_SOURCE;
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;
            /* Preserve the same source declaration identity used for types.
             * A QN alone is not a declaration witness for the shared resolver. */
            rf.declaration_identity = cbm_arena_sprintf(
                arena, "%s:%d", d->file_path ? d->file_path : result->module_qn, i);
            const char *return_key = cbm_arena_sprintf(arena, "%u:%s", d->start_line, d->name);
            if (!strcmp(d->label, "Function") && return_facts && return_key)
                rf.source_return_type = cbm_ht_get(return_facts, return_key);

            // Build FUNC type from return_types
            const CBMType **ret_types = NULL;
            if (d->return_types) {
                int count = 0;
                while (d->return_types[count])
                    count++;
                if (count > 0) {
                    ret_types = (const CBMType **)cbm_arena_alloc(
                        arena, (count + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < count; j++) {
                        ret_types[j] =
                            cbm_parse_return_type_text(arena, d->return_types[j], module_qn);
                    }
                    ret_types[count] = NULL;
                }
            } else if (d->return_type && d->return_type[0]) {
                // Fallback: single return_type string
                ret_types = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(const CBMType *));
                ret_types[0] = cbm_parse_return_type_text(arena, d->return_type, module_qn);
                ret_types[1] = NULL;
            }

            /* The occurrence-preserving carrier is authoritative whenever it
             * is present, including a non-NULL carrier with an explicit zero
             * count. Its slots deliberately have no names because legacy name
             * extraction is not guaranteed to have the same multiplicity. */
            const CBMType **param_types_arr = NULL;
            const char **param_names_arr = NULL;
            bool has_ordered_params =
                d->signature_param_types != NULL || d->signature_param_count > 0;
            CBMGoLSPSignatureParserContext param_parser_ctx = {module_qn};
            if (has_ordered_params) {
                param_types_arr = cbm_type_materialize_signature_params(
                    arena, d->signature_param_types, d->signature_param_count,
                    cbm_go_lsp_parse_signature_param_text, &param_parser_ctx);
            } else if (d->param_types) {
                param_names_arr = d->param_names;
                int count = 0;
                while (d->param_types[count])
                    count++;
                if (count > 0) {
                    param_types_arr = (const CBMType **)cbm_arena_alloc(
                        arena, (count + 1) * sizeof(const CBMType *));
                    for (int j = 0; j < count; j++) {
                        param_types_arr[j] =
                            cbm_parse_return_type_text(arena, d->param_types[j], module_qn);
                    }
                    param_types_arr[count] = NULL;
                }
            } else {
                param_names_arr = d->param_names;
            }

            rf.signature = cbm_type_func(arena, param_names_arr, param_types_arr, ret_types);

            // Prefer the extractor's semantic receiver QN. Raw receiver text is
            // retained only as a compatibility fallback for hand-built defs;
            // reparsing it alone loses valid unnamed receivers such as `(Right)`.
            if (strcmp(d->label, "Method") == 0) {
                if (d->parent_class && d->parent_class[0]) {
                    rf.receiver_type = d->parent_class;
                } else if (d->receiver && d->receiver[0]) {
                    // Legacy named receiver formats: "(name *Type)" / "(name Type)".
                    const char *r = d->receiver;
                    while (*r == '(' || *r == ' ')
                        r++;
                    while (*r && *r != ' ' && *r != '*')
                        r++;
                    while (*r == ' ' || *r == '*')
                        r++;
                    const char *end = r;
                    while (*end && *end != ')' && *end != ' ')
                        end++;
                    if (end > r) {
                        char *type_name = cbm_arena_strndup(arena, r, (size_t)(end - r));
                        rf.receiver_type = cbm_arena_sprintf(arena, "%s.%s", module_qn, type_name);
                    }
                }

                if (rf.receiver_type && !cbm_registry_lookup_type(&reg, rf.receiver_type)) {
                    const char *dot = strrchr(rf.receiver_type, '.');
                    CBMRegisteredType auto_type;
                    memset(&auto_type, 0, sizeof(auto_type));
                    auto_type.qualified_name = rf.receiver_type;
                    auto_type.short_name = dot ? dot + 1 : rf.receiver_type;
                    cbm_registry_add_type(&reg, auto_type);
                }
            }

            cbm_registry_add_func(&reg, rf);
        }
    }

    // Phase 1b: Scan AST for struct definitions to populate embedded_types
    {
        // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
        uint32_t rkn = 0;
        TSNode *rkids = cbm_lsp_collect_children(arena, root, &rkn);
        for (uint32_t i = 0; i < rkn; i++) {
            TSNode top = rkids[i];
            const char *tk = ts_node_type(top);
            // type_declaration contains type_spec children
            if (strcmp(tk, "type_declaration") != 0)
                continue;
            uint32_t td_nc = ts_node_child_count(top);
            for (uint32_t j = 0; j < td_nc; j++) {
                TSNode spec = ts_node_child(top, j);
                if (ts_node_is_null(spec) || !ts_node_is_named(spec))
                    continue;
                const char *spec_kind = ts_node_type(spec);

                // type_alias: type Foo = Bar
                if (strcmp(spec_kind, "type_alias") == 0) {
                    TSNode alias_name = ts_node_child_by_field_name(spec, "name", 4);
                    TSNode alias_type = ts_node_child_by_field_name(spec, "type", 4);
                    if (!ts_node_is_null(alias_name) && !ts_node_is_null(alias_type)) {
                        char *aname = cbm_node_text(arena, alias_name, source);
                        char *atarget = cbm_node_text(arena, alias_type, source);
                        if (aname && aname[0] && atarget && atarget[0]) {
                            const char *alias_type_qn =
                                cbm_arena_sprintf(arena, "%s.%s", module_qn, aname);
                            const char *alias_target_qn =
                                cbm_arena_sprintf(arena, "%s.%s", module_qn, atarget);
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

                if (strcmp(spec_kind, "type_spec") != 0)
                    continue;

                TSNode name_node = ts_node_child_by_field_name(spec, "name", 4);
                TSNode type_node = ts_node_child_by_field_name(spec, "type", 4);
                if (ts_node_is_null(name_node) || ts_node_is_null(type_node))
                    continue;

                char *type_name = cbm_node_text(arena, name_node, source);
                if (!type_name || !type_name[0])
                    continue;
                const char *type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, type_name);

                // Interface type: extract method names for satisfaction checking
                if (strcmp(ts_node_type(type_node), "interface_type") == 0) {
                    const char *iface_methods[64];
                    int iface_method_count = 0;
                    uint32_t inl_nc = ts_node_named_child_count(type_node);
                    for (uint32_t k = 0; k < inl_nc && iface_method_count < 63; k++) {
                        TSNode child = ts_node_named_child(type_node, k);
                        if (ts_node_is_null(child))
                            continue;
                        const char *ck = ts_node_type(child);
                        // method_spec: MethodName(params) returns
                        if (strcmp(ck, "method_spec") == 0 || strcmp(ck, "method_elem") == 0) {
                            TSNode mname = ts_node_child_by_field_name(child, "name", 4);
                            if (!ts_node_is_null(mname)) {
                                char *mn = cbm_node_text(arena, mname, source);
                                if (mn && mn[0]) {
                                    iface_methods[iface_method_count++] = mn;
                                }
                            }
                        }
                    }
                    if (iface_method_count > 0) {
                        for (int ti = 0; ti < reg.type_count; ti++) {
                            if (!reg.types[ti].qualified_name ||
                                strcmp(reg.types[ti].qualified_name, type_qn) != 0)
                                continue;
                            const char **names = (const char **)cbm_arena_alloc(
                                arena, (iface_method_count + 1) * sizeof(const char *));
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

                if (strcmp(ts_node_type(type_node), "struct_type") != 0)
                    continue;

                // Find field_declaration_list inside struct_type
                TSNode field_list = ts_node_child_by_field_name(type_node, "body", 4);
                if (ts_node_is_null(field_list)) {
                    // Some grammars use first named child
                    if (ts_node_named_child_count(type_node) > 0)
                        field_list = ts_node_named_child(type_node, 0);
                }
                if (ts_node_is_null(field_list))
                    continue;

                // Scan field_declarations for embeds and named fields
                const char *embeds[16];
                int embed_count = 0;
                const char *fld_names[64];
                const CBMType *fld_types[64];
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
                    if (ts_node_is_null(field) || !ts_node_is_named(field))
                        continue;
                    if (strcmp(ts_node_type(field), "field_declaration") != 0)
                        continue;

                    TSNode fname = ts_node_child_by_field_name(field, "name", 4);
                    TSNode ftype = ts_node_child_by_field_name(field, "type", 4);

                    if (ts_node_is_null(fname) && !ts_node_is_null(ftype)) {
                        // Embedded field: has type but no name
                        if (embed_count < 15) {
                            char *embed_text = cbm_node_text(arena, ftype, source);
                            if (embed_text && embed_text[0]) {
                                const char *et = embed_text;
                                while (*et == '*')
                                    et++;
                                embeds[embed_count++] =
                                    cbm_arena_sprintf(arena, "%s.%s", module_qn, et);
                            }
                        }
                    } else if (!ts_node_is_null(fname) && !ts_node_is_null(ftype) &&
                               fld_count < 63) {
                        // Named field: name + type
                        char *fn = cbm_node_text(arena, fname, source);
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
                        strcmp(reg.types[ti].qualified_name, type_qn) != 0)
                        continue;

                    if (embed_count > 0) {
                        const char **arr = (const char **)cbm_arena_alloc(
                            arena, (embed_count + 1) * sizeof(const char *));
                        for (int ei = 0; ei < embed_count; ei++)
                            arr[ei] = embeds[ei];
                        arr[embed_count] = NULL;
                        reg.types[ti].embedded_types = arr;
                    }
                    if (fld_count > 0) {
                        const char **names = (const char **)cbm_arena_alloc(
                            arena, (fld_count + 1) * sizeof(const char *));
                        const CBMType **types = (const CBMType **)cbm_arena_alloc(
                            arena, (fld_count + 1) * sizeof(const CBMType *));
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

    cbm_ht_free(return_facts);

    // Phase 1c: Extract type parameters from generic function declarations
    extract_type_params_from_ast(arena, &reg, root, source, module_qn);
    /* Shared lookup requires indexes covering every registered declaration.
     * Registration is now complete; finalize once before the read-only walk. */
    cbm_registry_finalize(&reg);

    // Phase 2: Build LSP context and run
    GoLSPContext lsp_ctx;
    go_lsp_init(&lsp_ctx, arena, source, source_len, &reg, module_qn, &result->resolved_calls);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(result, CBM_SOURCE_ORIGIN_RAW, module_qn);
    (void)cbm_lsp_proof_context(&lsp_ctx.proven_types, &proof_source, module_qn);
    lsp_ctx.rel_path = rel_path;

    // Add imports
    for (int i = 0; i < result->imports.count; i++) {
        CBMImport *imp = &result->imports.items[i];
        if (imp->local_name && imp->module_path) {
            go_lsp_add_import(&lsp_ctx, imp->local_name, imp->module_path);
        }
    }

    // Process the file
    go_lsp_process_file(&lsp_ctx, root);
}

// --- Cross-file LSP: parse source, build registry from defs, run LSP ---

// Helper: split "|"-separated string into array of CBMType*.
static const CBMType **split_pipe_types(CBMArena *a, const char *text, const char *module_qn) {
    if (!text || !text[0])
        return NULL;

    // Count separators
    int count = 1;
    for (const char *p = text; *p; p++) {
        if (*p == '|')
            count++;
    }

    const CBMType **arr =
        (const CBMType **)cbm_arena_alloc(a, (count + 1) * sizeof(const CBMType *));
    if (!arr)
        return NULL;

    // Split and parse each type
    char *buf = cbm_arena_strdup(a, text);
    int idx = 0;
    char *start = buf;
    for (char *p = buf;; p++) {
        if (*p == '|' || *p == '\0') {
            char save = *p;
            *p = '\0';
            if (start[0]) {
                arr[idx++] = cbm_parse_return_type_text(a, start, module_qn);
            }
            if (save == '\0')
                break;
            start = p + 1;
        }
    }
    arr[idx] = NULL;
    return idx > 0 ? arr : NULL;
}

// Helper: split "|"-separated string into array of const char*.
static const char **split_pipe_strings(CBMArena *a, const char *text) {
    if (!text || !text[0])
        return NULL;

    int count = 1;
    for (const char *p = text; *p; p++) {
        if (*p == '|')
            count++;
    }

    const char **arr = (const char **)cbm_arena_alloc(a, (count + 1) * sizeof(const char *));
    if (!arr)
        return NULL;

    char *buf = cbm_arena_strdup(a, text);
    int idx = 0;
    char *start = buf;
    for (char *p = buf;; p++) {
        if (*p == '|' || *p == '\0') {
            char save = *p;
            *p = '\0';
            if (start[0]) {
                arr[idx++] = cbm_arena_strdup(a, start);
            }
            if (save == '\0')
                break;
            start = p + 1;
        }
    }
    arr[idx] = NULL;
    return idx > 0 ? arr : NULL;
}

/* QN of the type an identity alias `type A = B` names, for the def-driven
 * cross-file registries (#2033). The per-file AST scan sets alias_of only for
 * the file it walks, so an alias declared in another file of the package
 * reaches a call only through here. Only a bare identifier is qualified (B
 * declared in A's package); a qualified, pointer, composite or generic target
 * stays NULL rather than guessed. */
static const char *go_alias_target_qn(CBMArena *arena, const char *target,
                                      const char *def_module_qn) {
    if (!target || !target[0] || !def_module_qn || !def_module_qn[0]) {
        return NULL;
    }
    for (const unsigned char *p = (const unsigned char *)target; *p; p++) {
        bool ident = (*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                     (*p >= '0' && *p <= '9') || *p == '_' || *p >= 0x80;
        if (!ident) {
            return NULL;
        }
    }
    return cbm_arena_sprintf(arena, "%s.%s", def_module_qn, target);
}

// Helper: parse "|"-separated "name:type" field definitions and populate a registered type.
// Format: "Binder:Binder|Name:string|Count:int"
// type_text is resolved relative to def_module_qn.
/* Parse "name:type|name:type" into NULL-terminated name/type arrays. False when
 * nothing parsed (or on allocation failure): the type keeps no field info. */
static bool parse_field_defs(CBMArena *arena, const char *field_defs, const char *def_module_qn,
                             const char ***names_out, const CBMType ***types_out) {

    // Count fields
    int count = 1;
    for (const char *p = field_defs; *p; p++) {
        if (*p == '|')
            count++;
    }
    if (count > 63)
        count = 63;

    const char **names = (const char **)cbm_arena_alloc(arena, (count + 1) * sizeof(const char *));
    const CBMType **types =
        (const CBMType **)cbm_arena_alloc(arena, (count + 1) * sizeof(const CBMType *));
    if (!names || !types)
        return false;

    char *buf = cbm_arena_strdup(arena, field_defs);
    int idx = 0;
    char *start = buf;
    for (char *p = buf;; p++) {
        if (*p == '|' || *p == '\0') {
            char save = *p;
            *p = '\0';
            // Parse "name:type"
            char *colon = NULL;
            for (char *q = start; *q; q++) {
                if (*q == ':') {
                    colon = q;
                    break;
                }
            }
            if (colon && idx < count) {
                *colon = '\0';
                names[idx] = cbm_arena_strdup(arena, start);
                types[idx] = cbm_parse_return_type_text(arena, colon + 1, def_module_qn);
                idx++;
            }
            if (save == '\0')
                break;
            start = p + 1;
        }
    }
    names[idx] = NULL;
    types[idx] = NULL;
    *names_out = names;
    *types_out = types;
    return idx > 0;
}

static void parse_field_defs_into_type(CBMArena *arena, CBMTypeRegistry *reg, const char *type_qn,
                                       const char *field_defs, const char *def_module_qn) {
    if (!field_defs || !field_defs[0] || !type_qn)
        return;
    const char **names = NULL;
    const CBMType **types = NULL;
    if (parse_field_defs(arena, field_defs, def_module_qn, &names, &types)) {
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
static const CBMType *parse_type_node_with_params(CBMArena *arena, TSNode node, const char *source,
                                                  const char *module_qn,
                                                  const char **type_param_names) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();

    const char *kind = ts_node_type(node);

    // type_identifier: check type params first, then resolve as named type
    if (strcmp(kind, "type_identifier") == 0) {
        char *name = cbm_node_text(arena, node, source);
        if (!name)
            return cbm_type_unknown();
        // Check type param names
        if (type_param_names) {
            for (int i = 0; type_param_names[i]; i++) {
                if (strcmp(name, type_param_names[i]) == 0) {
                    return cbm_type_type_param(arena, name);
                }
            }
        }
        // Builtin types
        if (strcmp(name, "int") == 0 || strcmp(name, "string") == 0 || strcmp(name, "bool") == 0 ||
            strcmp(name, "float64") == 0 || strcmp(name, "float32") == 0 ||
            strcmp(name, "byte") == 0 || strcmp(name, "rune") == 0 || strcmp(name, "error") == 0 ||
            strcmp(name, "any") == 0 || strcmp(name, "int8") == 0 || strcmp(name, "int16") == 0 ||
            strcmp(name, "int32") == 0 || strcmp(name, "int64") == 0 || strcmp(name, "uint") == 0 ||
            strcmp(name, "uint8") == 0 || strcmp(name, "uint16") == 0 ||
            strcmp(name, "uint32") == 0 || strcmp(name, "uint64") == 0 ||
            strcmp(name, "uintptr") == 0) {
            return cbm_type_builtin(arena, name);
        }
        return cbm_type_named(arena, cbm_arena_sprintf(arena, "%s.%s", module_qn, name));
    }

    // type_elem: wrapper in type_arguments
    if (strcmp(kind, "type_elem") == 0 && ts_node_named_child_count(node) > 0) {
        return parse_type_node_with_params(arena, ts_node_named_child(node, 0), source, module_qn,
                                           type_param_names);
    }

    // qualified_type: pkg.Type
    if (strcmp(kind, "qualified_type") == 0) {
        TSNode pkg_node = ts_node_child_by_field_name(node, "package", 7);
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        if (!ts_node_is_null(pkg_node) && !ts_node_is_null(name_node)) {
            char *pkg = cbm_node_text(arena, pkg_node, source);
            char *name = cbm_node_text(arena, name_node, source);
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
            return cbm_type_pointer(
                arena, parse_type_node_with_params(arena, ts_node_named_child(node, nc - 1), source,
                                                   module_qn, type_param_names));
        return cbm_type_unknown();
    }

    // slice_type: []T
    if (strcmp(kind, "slice_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0)
            elem = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        return cbm_type_slice(
            arena, parse_type_node_with_params(arena, elem, source, module_qn, type_param_names));
    }

    // array_type: [N]T
    if (strcmp(kind, "array_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0)
            elem = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        return cbm_type_slice(
            arena, parse_type_node_with_params(arena, elem, source, module_qn, type_param_names));
    }

    // map_type: map[K]V
    if (strcmp(kind, "map_type") == 0) {
        TSNode key = ts_node_child_by_field_name(node, "key", 3);
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        return cbm_type_map(
            arena, parse_type_node_with_params(arena, key, source, module_qn, type_param_names),
            parse_type_node_with_params(arena, value, source, module_qn, type_param_names));
    }

    // channel_type: chan T
    if (strcmp(kind, "channel_type") == 0) {
        TSNode value = ts_node_child_by_field_name(node, "value", 5);
        if (ts_node_is_null(value) && ts_node_named_child_count(node) > 0)
            value = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        char *text = cbm_node_text(arena, node, source);
        int dir = 0;
        if (text) {
            if (strncmp(text, "chan<-", 6) == 0 || strncmp(text, "chan <-", 7) == 0)
                dir = 1;
            else if (strncmp(text, "<-chan", 6) == 0 || strncmp(text, "<- chan", 7) == 0)
                dir = 2;
        }
        return cbm_type_channel(
            arena, parse_type_node_with_params(arena, value, source, module_qn, type_param_names),
            dir);
    }

    // function_type: func(T) R — full parsing with type param awareness
    if (strcmp(kind, "function_type") == 0) {
        TSNode params_node = ts_node_child_by_field_name(node, "parameters", 10);
        TSNode result_node = ts_node_child_by_field_name(node, "result", 6);

        const CBMType *param_types_arr[16];
        int pc = 0;
        if (!ts_node_is_null(params_node)) {
            uint32_t pnc = ts_node_child_count(params_node);
            for (uint32_t i = 0; i < pnc && pc < 15; i++) {
                TSNode child = ts_node_child(params_node, i);
                if (ts_node_is_null(child) || !ts_node_is_named(child))
                    continue;
                if (strcmp(ts_node_type(child), "parameter_declaration") == 0) {
                    TSNode pt = ts_node_child_by_field_name(child, "type", 4);
                    if (!ts_node_is_null(pt))
                        param_types_arr[pc++] = parse_type_node_with_params(
                            arena, pt, source, module_qn, type_param_names);
                }
            }
        }
        param_types_arr[pc] = NULL;

        const CBMType *ret_types_arr[16];
        int rc = 0;
        if (!ts_node_is_null(result_node)) {
            if (strcmp(ts_node_type(result_node), "parameter_list") == 0) {
                uint32_t rnc = ts_node_child_count(result_node);
                for (uint32_t i = 0; i < rnc && rc < 15; i++) {
                    TSNode child = ts_node_child(result_node, i);
                    if (ts_node_is_null(child) || !ts_node_is_named(child))
                        continue;
                    TSNode rt = ts_node_child_by_field_name(child, "type", 4);
                    if (ts_node_is_null(rt))
                        rt = child;
                    ret_types_arr[rc++] =
                        parse_type_node_with_params(arena, rt, source, module_qn, type_param_names);
                }
            } else {
                ret_types_arr[rc++] = parse_type_node_with_params(arena, result_node, source,
                                                                  module_qn, type_param_names);
            }
        }
        ret_types_arr[rc] = NULL;

        const CBMType **pt = pc > 0 ? (const CBMType **)param_types_arr : NULL;
        const CBMType **rt = rc > 0 ? (const CBMType **)ret_types_arr : NULL;
        return cbm_type_func(arena, NULL, pt, rt);
    }

    // interface_type
    if (strcmp(kind, "interface_type") == 0) {
        CBMType *t = (CBMType *)cbm_arena_alloc(arena, sizeof(CBMType));
        memset(t, 0, sizeof(CBMType));
        t->kind = CBM_TYPE_INTERFACE;
        return t;
    }

    // parenthesized_type: (T)
    if (strcmp(kind, "parenthesized_type") == 0 && ts_node_named_child_count(node) > 0) {
        return parse_type_node_with_params(arena, ts_node_named_child(node, 0), source, module_qn,
                                           type_param_names);
    }

    // generic_type: Type[T1, T2]
    if (strcmp(kind, "generic_type") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node))
            return parse_type_node_with_params(arena, type_node, source, module_qn,
                                               type_param_names);
    }

    return cbm_type_unknown();
}

/* Go permits one AST parameter declaration to introduce several call
 * positions: `first, second T`. Count direct names without exposing or
 * rewriting the legacy public param_names projection. */
static int cbm_go_lsp_signature_param_multiplicity(TSNode param) {
    const char *kind = ts_node_type(param);
    if (strcmp(kind, "parameter_declaration") != 0 &&
        strcmp(kind, "variadic_parameter_declaration") != 0) {
        return 1;
    }

    int field_names = 0;
    int identifier_names = 0;
    uint32_t child_count = ts_node_child_count(param);
    for (uint32_t i = 0; i < child_count; i++) {
        TSNode child = ts_node_child(param, i);
        if (ts_node_is_null(child) || !ts_node_is_named(child))
            continue;
        const char *field = ts_node_field_name_for_child(param, i);
        if (field && strcmp(field, "name") == 0)
            field_names++;
        if (strcmp(ts_node_type(child), "identifier") == 0)
            identifier_names++;
    }

    int names = field_names > 0 ? field_names : identifier_names;
    return names > 0 ? names : 1;
}

static const CBMType *cbm_go_lsp_keep_known_refinement(const CBMType *candidate,
                                                       const CBMType *existing) {
    if (cbm_type_is_unknown(candidate) && !cbm_type_is_unknown(existing)) {
        return existing;
    }
    return candidate ? candidate : cbm_type_unknown();
}

// Scan AST for generic function declarations and set type_param_names + re-parse
// return types on matching registered functions.
static void extract_type_params_from_ast(CBMArena *arena, CBMTypeRegistry *reg, TSNode root,
                                         const char *source, const char *module_qn) {

    // Collect top-level children once (O(n)); ts_node_child(root,i) is O(i) → O(n²).
    uint32_t rkn = 0;
    TSNode *rkids = cbm_lsp_collect_children(arena, root, &rkn);
    for (uint32_t i = 0; i < rkn; i++) {
        TSNode top = rkids[i];
        if (strcmp(ts_node_type(top), "function_declaration") != 0)
            continue;

        // Check for type_parameter_list (field name: "type_parameters", 15 chars)
        TSNode tp_list = ts_node_child_by_field_name(top, "type_parameters", 15);
        if (ts_node_is_null(tp_list))
            continue;

        // Get function name
        TSNode name_node = ts_node_child_by_field_name(top, "name", 4);
        if (ts_node_is_null(name_node))
            continue;
        char *func_name = cbm_node_text(arena, name_node, source);
        if (!func_name || !func_name[0])
            continue;

        // Extract type param names from type_parameter_declaration children
        const char *params[16];
        int param_count = 0;
        uint32_t tp_nc = ts_node_child_count(tp_list);
        for (uint32_t j = 0; j < tp_nc && param_count < 15; j++) {
            TSNode child = ts_node_child(tp_list, j);
            if (ts_node_is_null(child) || !ts_node_is_named(child))
                continue;
            if (strcmp(ts_node_type(child), "type_parameter_declaration") != 0)
                continue;
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
                char *pn = cbm_node_text(arena, pname, source);
                if (pn && pn[0]) {
                    params[param_count++] = cbm_arena_strdup(arena, pn);
                }
            }
        }
        if (param_count == 0)
            continue;
        params[param_count] = NULL;

        // Build arena-allocated type_param_names array
        const char **tp_names =
            (const char **)cbm_arena_alloc(arena, (param_count + 1) * sizeof(const char *));
        for (int j = 0; j <= param_count; j++)
            tp_names[j] = params[j];

        // Find the matching registered function and set type_param_names
        const char *func_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, func_name);
        for (int fi = 0; fi < reg->func_count; fi++) {
            if (reg->funcs[fi].qualified_name &&
                strcmp(reg->funcs[fi].qualified_name, func_qn) == 0) {
                reg->funcs[fi].type_param_names = tp_names;

                // Re-parse return types with type param awareness
                if (reg->funcs[fi].signature && reg->funcs[fi].signature->kind == CBM_TYPE_FUNC &&
                    reg->funcs[fi].signature->data.func.return_types) {
                    const CBMType **old_rets = reg->funcs[fi].signature->data.func.return_types;
                    int ret_count = 0;
                    while (old_rets[ret_count])
                        ret_count++;

                    // Check if any return type is a NAMED that matches a type param
                    bool needs_reparse = false;
                    for (int ri = 0; ri < ret_count && !needs_reparse; ri++) {
                        const CBMType *check = old_rets[ri];
                        // Unwrap slice/pointer to get inner named type
                        if (check->kind == CBM_TYPE_SLICE && check->data.slice.elem)
                            check = check->data.slice.elem;
                        else if (check->kind == CBM_TYPE_POINTER && check->data.pointer.elem)
                            check = check->data.pointer.elem;
                        if (check->kind == CBM_TYPE_NAMED) {
                            const char *qn = check->data.named.qualified_name;
                            const char *dot = strrchr(qn, '.');
                            const char *short_name = dot ? dot + 1 : qn;
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
                            const CBMType **new_rets = (const CBMType **)cbm_arena_alloc(
                                arena, (ret_count + 1) * sizeof(const CBMType *));

                            if (strcmp(ts_node_type(result_node), "parameter_list") == 0) {
                                int idx = 0;
                                uint32_t rnc = ts_node_child_count(result_node);
                                for (uint32_t ri = 0; ri < rnc && idx < ret_count; ri++) {
                                    TSNode rchild = ts_node_child(result_node, ri);
                                    if (ts_node_is_null(rchild) || !ts_node_is_named(rchild))
                                        continue;
                                    TSNode rtype = ts_node_child_by_field_name(rchild, "type", 4);
                                    if (ts_node_is_null(rtype))
                                        rtype = rchild;
                                    const CBMType *parsed = parse_type_node_with_params(
                                        arena, rtype, source, module_qn, tp_names);
                                    new_rets[idx] =
                                        cbm_go_lsp_keep_known_refinement(parsed, old_rets[idx]);
                                    idx++;
                                }
                                while (idx < ret_count) {
                                    new_rets[idx] = old_rets[idx];
                                    idx++;
                                }
                                new_rets[ret_count] = NULL;
                            } else {
                                const CBMType *parsed = parse_type_node_with_params(
                                    arena, result_node, source, module_qn, tp_names);
                                new_rets[0] = cbm_go_lsp_keep_known_refinement(parsed, old_rets[0]);
                                for (int idx = 1; idx < ret_count; idx++) {
                                    new_rets[idx] = old_rets[idx];
                                }
                                new_rets[ret_count] = NULL;
                            }

                            const CBMType *new_sig = cbm_type_func_replace_returns(
                                arena, reg->funcs[fi].signature, new_rets);
                            if (new_sig && new_sig->kind == CBM_TYPE_FUNC) {
                                reg->funcs[fi].signature = new_sig;
                            }
                        }
                    }
                }

                // Also re-parse param types with TYPE_PARAM awareness (for implicit generics)
                if (reg->funcs[fi].signature && reg->funcs[fi].signature->kind == CBM_TYPE_FUNC &&
                    reg->funcs[fi].signature->data.func.param_types) {
                    const CBMType **old_params = reg->funcs[fi].signature->data.func.param_types;
                    int pc = 0;
                    while (old_params[pc])
                        pc++;
                    // Re-parse from AST parameter list
                    TSNode params_node = ts_node_child_by_field_name(top, "parameters", 10);
                    if (!ts_node_is_null(params_node)) {
                        const CBMType **new_params = (const CBMType **)cbm_arena_alloc(
                            arena, (pc + 1) * sizeof(const CBMType *));
                        int idx = 0;
                        bool overflow = false;
                        uint32_t pnc = ts_node_child_count(params_node);
                        for (uint32_t pi = 0; pi < pnc && !overflow; pi++) {
                            TSNode pchild = ts_node_child(params_node, pi);
                            if (ts_node_is_null(pchild) || !ts_node_is_named(pchild))
                                continue;
                            const char *param_kind = ts_node_type(pchild);
                            if (strcmp(param_kind, "parameter_declaration") != 0 &&
                                strcmp(param_kind, "variadic_parameter_declaration") != 0) {
                                continue;
                            }
                            TSNode ptype = ts_node_child_by_field_name(pchild, "type", 4);
                            const CBMType *parsed =
                                ts_node_is_null(ptype)
                                    ? cbm_type_unknown()
                                    : parse_type_node_with_params(arena, ptype, source, module_qn,
                                                                  tp_names);
                            int multiplicity = cbm_go_lsp_signature_param_multiplicity(pchild);
                            for (int occurrence = 0; occurrence < multiplicity; occurrence++) {
                                if (idx >= pc) {
                                    overflow = true;
                                    break;
                                }
                                new_params[idx] =
                                    cbm_go_lsp_keep_known_refinement(parsed, old_params[idx]);
                                idx++;
                            }
                        }
                        new_params[idx] = NULL;
                        if (!overflow && idx == pc) {
                            const CBMType *old_sig = reg->funcs[fi].signature;
                            const CBMType *new_sig =
                                cbm_type_func(arena, old_sig->data.func.param_names, new_params,
                                              old_sig->data.func.return_types);
                            if (new_sig && new_sig->kind == CBM_TYPE_FUNC) {
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
extern const TSLanguage *tree_sitter_go(void);

void cbm_run_go_lsp_cross(CBMArena *arena, const char *source, int source_len,
                          const char *module_qn, const char *rel_path, CBMLSPDef *defs,
                          int def_count, const char **import_names, const char **import_qns,
                          int import_count, TSTree *cached_tree, CBMResolvedCallArray *out) {
    cbm_run_go_lsp_cross_with_source(arena, source, source_len, module_qn, rel_path, defs,
                                     def_count, import_names, import_qns, import_count, cached_tree,
                                     out, NULL);
}

void cbm_run_go_lsp_cross_with_source(CBMArena *arena, const char *source, int source_len,
                                      const char *module_qn, const char *rel_path, CBMLSPDef *defs,
                                      int def_count, const char **import_names,
                                      const char **import_qns, int import_count,
                                      TSTree *cached_tree, CBMResolvedCallArray *out,
                                      const cbm_resolve_proof_source_t *source_context) {
    if (!source || source_len <= 0)
        return;

    // 1. Use cached tree if available, otherwise parse fresh
    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        ts_parser_set_language(parser, tree_sitter_go());
        tree = cbm_parse_source(parser, source, (uint32_t)source_len, (TSParseOptions){0});
        owns_tree = true;
        if (!tree) {
            ts_parser_delete(parser);
            return;
        }
    }
    TSNode root = ts_tree_root_node(tree);

    // 2. Build registry (the stdlib is the shared sealed registry, chained)
    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    go_registry_with_stdlib(&reg, arena);

    // Register all defs (file-local + cross-file).
    // Perf: borrow strings from defs[] directly — they live in the
    // caller's arena which outlives this call, so cbm_arena_strdup is
    // wasted work. On kubernetes (~110k defs × 11k files × 5 strdups
    // ~= 6B mallocs) this alone saves ~90s of resolve wall time.
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;

        const char *def_mod = d->def_module_qn ? d->def_module_qn : module_qn;

        // Every type-like container (Type/Class/Struct/Interface/Enum/Trait).
        // Struct included so Go structs (now labelled "Struct") register as types.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name; // borrowed
            rt.project_source = true;
            rt.graph_qualified_name =
                d->graph_qualified_name ? d->graph_qualified_name : d->qualified_name;
            rt.declaration_identity = d->declaration_identity;
            rt.short_name = d->short_name; // borrowed
            rt.is_interface = d->is_interface || strcmp(d->label, "Interface") == 0;
            rt.embedded_types = split_pipe_strings(arena, d->embedded_types);

            // Set method_names for interfaces from "|"-separated string
            if (rt.is_interface && d->method_names_str && d->method_names_str[0]) {
                rt.method_names = split_pipe_strings(arena, d->method_names_str);
            }
            rt.alias_of = go_alias_target_qn(arena, d->alias_target, def_mod);

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
            rf.flags |= CBM_FUNC_FLAG_PROJECT_SOURCE;
            rf.sig = d->sig;
            rf.declaration_identity = d->declaration_identity;
            rf.source_return_type = d->source_return_type;
            rf.qualified_name = d->qualified_name; // borrowed
            rf.short_name = d->short_name;         // borrowed

            // Build FUNC type from return_types text
            const CBMType **ret_types = split_pipe_types(arena, d->return_types, def_mod);
            CBMGoLSPSignatureParserContext param_parser_ctx = {def_mod};
            const CBMType **param_types = cbm_type_materialize_signature_params(
                arena, d->signature_param_types, d->signature_param_count,
                cbm_go_lsp_parse_signature_param_text, &param_parser_ctx);
            rf.signature = cbm_type_func(arena, NULL, param_types, ret_types);

            // Method receiver
            if (strcmp(d->label, "Method") == 0 && d->receiver_type && d->receiver_type[0]) {
                rf.receiver_type = d->receiver_type; // borrowed
                // Auto-create type entry if not exists
                if (!cbm_registry_lookup_type(&reg, rf.receiver_type)) {
                    CBMRegisteredType auto_type;
                    memset(&auto_type, 0, sizeof(auto_type));
                    auto_type.qualified_name = rf.receiver_type;
                    const char *dot = strrchr(d->receiver_type, '.');
                    auto_type.short_name = dot ? dot + 1 : rf.receiver_type; // borrowed substring
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
        TSNode *rkids = cbm_lsp_collect_children(arena, root, &rkn);
        for (uint32_t i = 0; i < rkn; i++) {
            TSNode top = rkids[i];
            if (strcmp(ts_node_type(top), "type_declaration") != 0)
                continue;
            uint32_t td_nc = ts_node_child_count(top);
            for (uint32_t j = 0; j < td_nc; j++) {
                TSNode spec = ts_node_child(top, j);
                if (ts_node_is_null(spec) || !ts_node_is_named(spec))
                    continue;
                const char *spec_kind2 = ts_node_type(spec);

                // type_alias: type Foo = Bar
                if (strcmp(spec_kind2, "type_alias") == 0) {
                    TSNode alias_name = ts_node_child_by_field_name(spec, "name", 4);
                    TSNode alias_type = ts_node_child_by_field_name(spec, "type", 4);
                    if (!ts_node_is_null(alias_name) && !ts_node_is_null(alias_type)) {
                        char *aname = cbm_node_text(arena, alias_name, source);
                        char *atarget = cbm_node_text(arena, alias_type, source);
                        if (aname && aname[0] && atarget && atarget[0]) {
                            const char *alias_type_qn =
                                cbm_arena_sprintf(arena, "%s.%s", module_qn, aname);
                            const char *alias_target_qn =
                                cbm_arena_sprintf(arena, "%s.%s", module_qn, atarget);
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

                if (strcmp(spec_kind2, "type_spec") != 0)
                    continue;

                TSNode name_node = ts_node_child_by_field_name(spec, "name", 4);
                TSNode type_node = ts_node_child_by_field_name(spec, "type", 4);
                if (ts_node_is_null(name_node) || ts_node_is_null(type_node))
                    continue;
                if (strcmp(ts_node_type(type_node), "struct_type") != 0)
                    continue;

                char *type_name = cbm_node_text(arena, name_node, source);
                if (!type_name || !type_name[0])
                    continue;
                const char *type_qn = cbm_arena_sprintf(arena, "%s.%s", module_qn, type_name);

                TSNode field_list = ts_node_child_by_field_name(type_node, "body", 4);
                if (ts_node_is_null(field_list)) {
                    if (ts_node_named_child_count(type_node) > 0)
                        field_list = ts_node_named_child(type_node, 0);
                }
                if (ts_node_is_null(field_list))
                    continue;

                const char *embeds[16];
                int embed_count = 0;
                const char *fld_names[64];
                const CBMType *fld_types[64];
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
                    if (ts_node_is_null(field) || !ts_node_is_named(field))
                        continue;
                    if (strcmp(ts_node_type(field), "field_declaration") != 0)
                        continue;
                    TSNode fname = ts_node_child_by_field_name(field, "name", 4);
                    TSNode ftype = ts_node_child_by_field_name(field, "type", 4);
                    if (ts_node_is_null(fname) && !ts_node_is_null(ftype)) {
                        if (embed_count < 15) {
                            char *embed_text = cbm_node_text(arena, ftype, source);
                            if (embed_text && embed_text[0]) {
                                const char *et = embed_text;
                                while (*et == '*')
                                    et++;
                                embeds[embed_count++] =
                                    cbm_arena_sprintf(arena, "%s.%s", module_qn, et);
                            }
                        }
                    } else if (!ts_node_is_null(fname) && !ts_node_is_null(ftype) &&
                               fld_count < 63) {
                        char *fn = cbm_node_text(arena, fname, source);
                        if (fn && fn[0]) {
                            fld_names[fld_count] = fn;
                            fld_types[fld_count] = go_parse_type_node(&tmp_ctx, ftype);
                            fld_count++;
                        }
                    }
                }

                for (int ti = 0; ti < reg.type_count; ti++) {
                    if (!reg.types[ti].qualified_name ||
                        strcmp(reg.types[ti].qualified_name, type_qn) != 0)
                        continue;

                    if (embed_count > 0) {
                        const char **arr = (const char **)cbm_arena_alloc(
                            arena, (embed_count + 1) * sizeof(const char *));
                        for (int ei = 0; ei < embed_count; ei++)
                            arr[ei] = embeds[ei];
                        arr[embed_count] = NULL;
                        reg.types[ti].embedded_types = arr;
                    }
                    if (fld_count > 0) {
                        const char **names = (const char **)cbm_arena_alloc(
                            arena, (fld_count + 1) * sizeof(const char *));
                        const CBMType **types = (const CBMType **)cbm_arena_alloc(
                            arena, (fld_count + 1) * sizeof(const CBMType *));
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
    if (source_context)
        (void)cbm_lsp_proof_context(&ctx.proven_types, source_context, module_qn);
    ctx.rel_path = rel_path;

    for (int i = 0; i < import_count; i++) {
        if (import_names[i] && import_qns[i]) {
            go_lsp_add_import(&ctx, import_names[i], import_qns[i]);
        }
    }

    go_lsp_process_file(&ctx, root);

    // 5. Cleanup — only free if we allocated
    if (owns_tree) {
        ts_tree_delete(tree);
        if (parser)
            ts_parser_delete(parser);
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
CBMTypeRegistry *cbm_go_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count) {
    if (!arena)
        return NULL;
    CBMTypeRegistry *reg = (CBMTypeRegistry *)cbm_arena_alloc(arena, sizeof(*reg));
    if (!reg)
        return NULL;
    cbm_registry_init(reg, arena);
    cbm_go_stdlib_register(reg, arena);

    /* Qualified name -> FIRST type index while the registry is built: it is
     * not finalized until the end, so its own lookup scans every type, and the
     * receiver check below plus the field-definition lookup made the build
     * O(types x (methods + structs)) -- 4.4 s single-threaded on the Go corpus,
     * the scaling lane's parse_field_defs_into_type x2.9 (waste sanitizer,
     * 2026-09-17). First-match semantics are kept exactly: a receiver's
     * placeholder type added before its struct still receives the fields.
     * NULL on allocation failure: the linear paths below run as before. */
    CBMHashTable *first_type = cbm_ht_create((uint32_t)(def_count / 4 + 1024));
    int noted_types = 0;
#define GO_NOTE_NEW_TYPES()                                                        \
    do {                                                                           \
        for (; first_type && noted_types < reg->type_count; noted_types++) {       \
            const char *tqn = reg->types[noted_types].qualified_name;              \
            if (tqn && !cbm_ht_has(first_type, tqn)) {                             \
                cbm_ht_set(first_type, tqn, (void *)(uintptr_t)(noted_types + 1)); \
            }                                                                      \
        }                                                                          \
    } while (0)
    GO_NOTE_NEW_TYPES();

    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;
        /* Filter to Go defs only — the all_defs[] array is mixed-language. */
        if (d->lang != CBM_LANG_GO)
            continue;
        /* In the pre-built path every def carries its own def_module_qn
         * (set by cbm_pxc_collect_all_defs). There is no caller module to
         * fall back to — this registry is project-wide, not per-file. */
        const char *def_mod = d->def_module_qn ? d->def_module_qn : "";

        // Every type-like container (Type/Class/Struct/Interface/Enum/Trait).
        // Struct included so Go structs (now labelled "Struct") register as types.
        if (cbm_label_is_type_like(d->label)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name; /* borrowed */
            rt.project_source = true;
            rt.graph_qualified_name =
                d->graph_qualified_name ? d->graph_qualified_name : d->qualified_name;
            rt.declaration_identity = d->declaration_identity;
            rt.short_name = d->short_name;
            rt.is_interface = d->is_interface || strcmp(d->label, "Interface") == 0;
            rt.embedded_types = split_pipe_strings(arena, d->embedded_types);
            if (rt.is_interface && d->method_names_str && d->method_names_str[0]) {
                rt.method_names = split_pipe_strings(arena, d->method_names_str);
            }
            rt.alias_of = go_alias_target_qn(arena, d->alias_target, def_mod);
            cbm_registry_add_type(reg, rt);
            GO_NOTE_NEW_TYPES();
            if (d->field_defs && d->field_defs[0]) {
                if (first_type) {
                    const char **names = NULL;
                    const CBMType **types = NULL;
                    uintptr_t at = (uintptr_t)cbm_ht_get(first_type, rt.qualified_name);
                    if (parse_field_defs(arena, d->field_defs, def_mod, &names, &types) && at) {
                        reg->types[at - 1].field_names = names;
                        reg->types[at - 1].field_types = types;
                    }
                } else {
                    parse_field_defs_into_type(arena, reg, rt.qualified_name, d->field_defs,
                                               def_mod);
                }
            }
        }

        if (strcmp(d->label, "Function") == 0 || strcmp(d->label, "Method") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.flags |= CBM_FUNC_FLAG_PROJECT_SOURCE;
            rf.sig = d->sig;
            rf.declaration_identity = d->declaration_identity;
            rf.source_return_type = d->source_return_type;
            rf.qualified_name = d->qualified_name; /* borrowed */
            rf.short_name = d->short_name;
            const CBMType **ret_types = split_pipe_types(arena, d->return_types, def_mod);
            CBMGoLSPSignatureParserContext param_parser_ctx = {def_mod};
            const CBMType **param_types = cbm_type_materialize_signature_params(
                arena, d->signature_param_types, d->signature_param_count,
                cbm_go_lsp_parse_signature_param_text, &param_parser_ctx);
            rf.signature = cbm_type_func(arena, NULL, param_types, ret_types);
            if (strcmp(d->label, "Method") == 0 && d->receiver_type && d->receiver_type[0]) {
                rf.receiver_type = d->receiver_type;
                bool known = first_type ? cbm_ht_has(first_type, rf.receiver_type)
                                        : cbm_registry_lookup_type(reg, rf.receiver_type) != NULL;
                if (!known) {
                    CBMRegisteredType auto_type;
                    memset(&auto_type, 0, sizeof(auto_type));
                    auto_type.qualified_name = rf.receiver_type;
                    const char *dot = strrchr(d->receiver_type, '.');
                    auto_type.short_name = dot ? dot + 1 : rf.receiver_type;
                    cbm_registry_add_type(reg, auto_type);
                    GO_NOTE_NEW_TYPES();
                }
            }
            cbm_registry_add_func(reg, rf);
        }
    }
#undef GO_NOTE_NEW_TYPES
    cbm_ht_free(first_type);

    cbm_registry_finalize(reg);
    reg->read_only = true; /* seal: shared Tier-2 registry is read-only during resolve */
    return reg;
}

void cbm_run_go_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                        const char *module_qn, const char *rel_path,
                                        CBMTypeRegistry *reg, const char **import_names,
                                        const char **import_qns, int import_count,
                                        TSTree *cached_tree, CBMResolvedCallArray *out) {
    cbm_run_go_lsp_cross_with_registry_with_source(arena, source, source_len, module_qn, rel_path,
                                                   reg, import_names, import_qns, import_count,
                                                   cached_tree, out, NULL);
}

void cbm_run_go_lsp_cross_with_registry_with_source(
    CBMArena *arena, const char *source, int source_len, const char *module_qn,
    const char *rel_path, CBMTypeRegistry *reg, const char **import_names, const char **import_qns,
    int import_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    const cbm_resolve_proof_source_t *source_context) {
    if (!source || source_len <= 0 || !reg)
        return;

    TSParser *parser = NULL;
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return;
        ts_parser_set_language(parser, tree_sitter_go());
        tree = cbm_parse_source(parser, source, (uint32_t)source_len, (TSParseOptions){0});
        owns_tree = true;
        if (!tree) {
            ts_parser_delete(parser);
            return;
        }
    }
    TSNode root = ts_tree_root_node(tree);

    /* Phase 1b/1c (per-file AST mutations on registry) DELIBERATELY SKIPPED —
     * shared registry must stay immutable across parallel workers. */

    GoLSPContext ctx;
    go_lsp_init(&ctx, arena, source, source_len, reg, module_qn, out);
    if (source_context)
        (void)cbm_lsp_proof_context(&ctx.proven_types, source_context, module_qn);
    ctx.rel_path = rel_path;
    for (int i = 0; i < import_count; i++) {
        if (import_names[i] && import_qns[i]) {
            go_lsp_add_import(&ctx, import_names[i], import_qns[i]);
        }
    }
    go_lsp_process_file(&ctx, root);

    if (owns_tree) {
        ts_tree_delete(tree);
        if (parser)
            ts_parser_delete(parser);
    }
}

/* Complete only canonical candidates whose source occurrence already proved
 * its Go scope/import/receiver binding. Text, strategy and confidence do not
 * establish that binding. This preserves the metadata-only cross-file path. */
int cbm_go_fast_resolve_qualified_calls(CBMFileResult *result, CBMTypeRegistry *reg,
                                        const char **import_names, const char **import_qns,
                                        int import_count) {
    (void)import_names;
    (void)import_qns;
    (void)import_count;
    if (!result || !reg)
        return 0;
    GoLSPContext ctx = {.arena = &result->arena, .registry = reg, .package_qn = result->module_qn};
    cbm_resolve_types_init(&ctx.proven_types, &result->arena, 0);
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(result, CBM_SOURCE_ORIGIN_RAW, result->module_qn);
    (void)cbm_lsp_proof_context(&ctx.proven_types, &proof_source, result->module_qn);
    int initial_count = result->resolved_calls.count;
    for (int i = 0; i < initial_count; i++) {
        CBMResolvedCall source = result->resolved_calls.items[i];
        if (!source.callee_qn || !source.caller_qn ||
            source.site_start_byte >= source.site_end_byte ||
            source.evidence.analysis != CBM_RESOLVE_ANALYSIS_LIMITED ||
            !(source.evidence.facts & CBM_RESOLVE_FACT_OCCURRENCE_EXACT) || !source.evidence.proof)
            continue;
        const cbm_resolve_proof_view_t *occurrence = go_pending_site(&source);
        if (!occurrence)
            continue;
        cbm_resolve_result_t proof = {0};
        if (source.evidence.rule == CBM_RESOLVE_RULE_TYPE_MEMBER &&
            (source.evidence.facts & CBM_RESOLVE_FACT_RECEIVER_TYPED)) {
            const char *dot = strrchr(source.callee_qn, '.');
            if (!dot || dot == source.callee_qn || !dot[1])
                continue;
            const char *owner = cbm_arena_strndup(&result->arena, source.callee_qn,
                                                  (size_t)(dot - source.callee_qn));
            if (!owner || !cbm_registry_lookup_type(reg, owner))
                continue;
            const cbm_resolve_proof_view_t *prior = cbm_resolve_proof_view(source.evidence.proof);
            if (!prior || !cbm_resolve_admit(&prior->outcome) ||
                !(prior->outcome.facts & CBM_RESOLVE_FACT_TYPE_BOUND) || !prior->outcome.target ||
                strcmp(prior->outcome.target, owner))
                continue;
            cbm_resolve_type_fact_t receiver = {.type = cbm_type_named(&result->arena, owner),
                                                .status = CBM_RESOLVE_TYPE_KNOWN,
                                                .origin = CBM_RESOLVE_TYPE_OBSERVED_BINDING,
                                                .proof_pool = &ctx.proven_types.proofs,
                                                .evidence = prior->outcome};
            receiver.evidence.proof = source.evidence.proof;
            cbm_resolve_method_query_t query = {.proof_pool = &ctx.proven_types.proofs,
                                                .proof_source = &ctx.proven_types.proof_source,
                                                .lookup_domain = owner,
                                                .registry = reg,
                                                .receiver = &receiver,
                                                .receiver_qn = owner,
                                                .method_name = dot + 1,
                                                .argument_count = -1,
                                                .adapter_facts =
                                                    CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                    CBM_RESOLVE_METHOD_OCCURRENCE_EXACT |
                                                    CBM_RESOLVE_METHOD_NAME_IDENTITY,
                                                .classify = go_classify_callable,
                                                .context = &ctx};
            proof = cbm_resolve_method(&query).result;
        } else if ((source.evidence.rule == CBM_RESOLVE_RULE_SCOPE_BINDING &&
                    (source.evidence.facts & CBM_RESOLVE_FACT_SCOPE_BOUND)) ||
                   (source.evidence.rule == CBM_RESOLVE_RULE_IMPORT_BINDING &&
                    (source.evidence.facts & CBM_RESOLVE_FACT_IMPORT_BOUND)) ||
                   (source.evidence.rule == CBM_RESOLVE_RULE_CALLABLE_BINDING &&
                    (source.evidence.facts & CBM_RESOLVE_FACT_CALLABLE_TYPED))) {
            proof = go_scoped_candidate(&ctx, source.callee_qn, source.evidence.rule,
                                        source.evidence.facts, source.evidence.proof, (TSNode){0});
        }
        if (!cbm_resolve_admit(&proof))
            continue;
        /* Reuse the source producer's occurrence, never manufacture a new one
         * from the metadata row. Target identity comes from the fresh lookup. */
        cbm_resolve_proof_key_t key = occurrence->key;
        key.binding_identity = proof.target;
        const cbm_resolve_proof_t *premises[] = {proof.proof, source.evidence.proof};
        proof.proof =
            cbm_resolve_proof_derive(&ctx.proven_types.proofs, CBM_PROOF_NAMED_JOIN,
                                     &occurrence->source, &key, &proof, premises, 2, true);
        const cbm_resolve_proof_view_t *completed = cbm_resolve_proof_view(proof.proof);
        if (!completed || completed->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
            !cbm_resolve_admit(&completed->outcome))
            continue;
        source.callee_qn = proof.target;
        source.strategy = "lsp_strategy_cross_file";
        source.confidence = 0.97f;
        source.reason = occurrence->key.lookup_key;
        source.evidence = cbm_lsp_proof_evidence(proof);
        cbm_resolvedcall_push(&result->resolved_calls, &result->arena, source);
    }
    return 0;
}

// --- Batch cross-file LSP ---

void cbm_batch_go_lsp_cross(CBMArena *arena, CBMBatchGoLSPFile *files, int file_count,
                            CBMResolvedCallArray *out) {
    if (!files || file_count <= 0 || !out)
        return;

    for (int f = 0; f < file_count; f++) {
        CBMBatchGoLSPFile *file = &files[f];
        memset(&out[f], 0, sizeof(CBMResolvedCallArray));

        if (!file->source || file->source_len <= 0 || file->def_count <= 0)
            continue;

        // Per-file arena: registry + temp data freed after each file
        CBMArena file_arena;
        cbm_arena_init(&file_arena);

        cbm_resolve_proof_pool_t copied_proofs;
        cbm_resolve_proof_pool_init(&copied_proofs, arena, 0, 0, 0);
        CBMResolvedCallArray file_out;
        memset(&file_out, 0, sizeof(file_out));

        // Delegate to existing per-file function
        cbm_run_go_lsp_cross(&file_arena, file->source, file->source_len, file->module_qn,
                             file->rel_path, file->defs, file->def_count, file->import_names,
                             file->import_qns, file->import_count, file->cached_tree, &file_out);

        // Copy results to output arena (must outlive per-file arena)
        if (file_out.count > 0) {
            out[f].count = file_out.count;
            out[f].items =
                (CBMResolvedCall *)cbm_arena_alloc(arena, file_out.count * sizeof(CBMResolvedCall));
            for (int j = 0; j < file_out.count; j++) {
                CBMResolvedCall *src = &file_out.items[j];
                CBMResolvedCall *dst = &out[f].items[j];
                memset(dst, 0, sizeof(*dst));
                dst->caller_qn = src->caller_qn ? cbm_arena_strdup(arena, src->caller_qn) : NULL;
                dst->callee_qn = src->callee_qn ? cbm_arena_strdup(arena, src->callee_qn) : NULL;
                dst->strategy = src->strategy ? cbm_arena_strdup(arena, src->strategy) : NULL;
                dst->confidence = src->confidence;
                dst->reason = src->reason ? cbm_arena_strdup(arena, src->reason) : NULL;
                dst->kind = src->kind;
                dst->site_start_byte = src->site_start_byte;
                dst->site_end_byte = src->site_end_byte;
                dst->source_origin = src->source_origin;
                dst->evidence = cbm_lsp_proof_copy(&copied_proofs, src->evidence);
            }
        }

        cbm_arena_destroy(&file_arena);
    }
}
