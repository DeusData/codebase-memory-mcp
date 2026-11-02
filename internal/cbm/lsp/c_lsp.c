#include "proof_bridge.h"
#include "../helpers.h"
#include "../resolve/policy.h"
#include "../resolve/symbols.h"
#include "c_lsp.h"
#include "c_proof.h"
#include "lsp_node_iter.h"
#include "lsp_work.h"
#include <ctype.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Safe kind accessor — returns CBM_TYPE_UNKNOWN for NULL types.
 * Prevents SEGV in c_eval_expr_type_inner on unusual C++ AST shapes. */
static inline CBMTypeKind safe_kind(const CBMType *t) {
    return t ? t->kind : CBM_TYPE_UNKNOWN;
}

#ifdef CBM_ENABLE_TEST_SEAMS
static _Thread_local int c_test_memo_allocations_left = -1;
static _Thread_local bool c_test_depth_failure;
void cbm_c_lsp_test_depth_fail(bool enabled) {
    c_test_depth_failure = enabled;
}
static _Thread_local uint32_t c_test_walks;
uint32_t cbm_c_lsp_test_walks_take(void) {
    uint32_t walks = c_test_walks;
    c_test_walks = 0;
    return walks;
}
void cbm_c_lsp_test_memo_fail_after(int successful_allocations) {
    c_test_memo_allocations_left = successful_allocations;
}
#endif

static void c_memo_test_stage(CLSPContext *ctx, const char *stage) {
#ifdef CBM_ENABLE_TEST_SEAMS
    c_test_walks++;
    const char *depth_stage = getenv("CBM_TEST_C_LSP_DEPTH_FAIL_STAGE");
    if (depth_stage && strcmp(depth_stage, stage) == 0)
        ctx->eval_depth = 257;
    const char *fail_stage = getenv("CBM_TEST_C_LSP_MEMO_FAIL_STAGE");
    if (fail_stage && strcmp(fail_stage, stage) == 0)
        ctx->test_memo_allocations_left = 0;
#else
    (void)ctx;
    (void)stage;
#endif
}

void cbm_c_lsp_record_failure(CBMFileResult *result, CBMLSPStatus status) {
    result->lsp_failed = true;
    result->has_error = true;
    result->lsp_skipped = true;
    /* Static lifetime keeps the reason available even when the arena is full.
     * Result compaction/spilling copies strings, including non-arena strings. */
    result->error_msg =
        status == CBM_LSP_DEPTH_EXCEEDED ? CBM_C_LSP_DEPTH_ERROR : CBM_C_LSP_MEMO_ERROR;
}

// Forward declarations
static void c_resolve_calls_in_node(CLSPContext *ctx, TSNode node);
static void c_emit_resolved_call(CLSPContext *ctx, const char *callee_qn, const char *strategy,
                                 float confidence);
static void c_emit_unresolved_call(CLSPContext *ctx, const char *expr_text, const char *reason);
static bool c_type_is_parameter_of(CLSPContext *ctx, const char *klass, const CBMType *written);
static const CBMType *c_lookup_field_type(CLSPContext *ctx, const char *type_qn,
                                          const char *field_name, int depth);
static const CBMRegisteredType *c_resolve_field_owner_type(CLSPContext *ctx, const char *type_qn);
static void c_process_function(CLSPContext *ctx, TSNode func_node);
static bool c_process_configured_test_body(CLSPContext *ctx, TSNode scope);
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

/* Proof lookup uses only the source declaration boundary. Inference may
 * refine or append entries later without changing this view. Shared sealed
 * registries are borrowed; only mutable per-file descriptors are copied. */
static const CBMTypeRegistry *c_proof_registry_snapshot(CLSPContext *ctx,
                                                        const CBMTypeRegistry *source, int depth) {
    if (!source || depth > CBM_LSP_MAX_LOOKUP_DEPTH)
        return NULL;
    if (source->read_only)
        return source;
    CBMTypeRegistry *copy = cbm_arena_alloc(ctx->arena, sizeof(*copy));
    if (!copy)
        return NULL;
    *copy = *source;
    if (source->func_count) {
        copy->funcs =
            cbm_arena_alloc(ctx->arena, (size_t)source->func_count * sizeof(*copy->funcs));
        if (!copy->funcs)
            return NULL;
        memcpy(copy->funcs, source->funcs, (size_t)source->func_count * sizeof(*copy->funcs));
    }
    if (source->fallback) {
        copy->fallback = c_proof_registry_snapshot(ctx, source->fallback, depth + 1);
        if (!copy->fallback)
            return NULL;
    }
    /* NAME_IDENTITY never consumes mutable inferred signature/type pointers.
     * All descriptor fields used by the classifier remain in this snapshot. */
    if (copy->func_qn_entry_count != copy->func_count ||
        (copy->func_count && !copy->func_qn_buckets)) {
        copy->arena = ctx->arena;
        cbm_registry_finalize(copy);
    }
    copy->read_only = true;
    return copy;
}

static void c_scope_bind(CLSPContext *ctx, const char *name, const CBMType *type) {
    if (!cbm_scope_bind_checked(ctx->current_scope, name, type))
        ctx->binding_failed = true;
}

// --- Initialization ---

void c_lsp_init(CLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                const CBMTypeRegistry *registry, const char *module_qn, bool cpp_mode,
                CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(CLSPContext));
#ifdef CBM_ENABLE_TEST_SEAMS
    ctx->test_memo_allocations_left = c_test_memo_allocations_left;
    if (c_test_depth_failure)
        ctx->eval_depth = 257;
#endif
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->registry_head = (CBMTypeRegistry *)registry;
    ctx->module_qn = module_qn;
    ctx->module_qn_len = module_qn ? strlen(module_qn) : 0;
    ctx->enclosing_func_qn = module_qn;
    ctx->cpp_mode = cpp_mode;
    ctx->resolved_calls = out;
    ctx->source_origin = CBM_SOURCE_ORIGIN_RAW;
    ctx->current_scope = cbm_scope_push(arena, NULL);
    /* One type fact per declaration and one proof per distinct occurrence fact, so
     * the totals follow the file's own size. A cap here does not bound anything;
     * it withholds every later edge of a large file, at a point that even moves
     * with the checkout path's length, because every hashed byte counts as work. */
    cbm_resolve_types_init(&ctx->field_types, arena, UINT64_MAX);
    cbm_resolve_proof_pool_init_project(&ctx->field_types.proofs, arena);
    (void)cbm_lsp_proof_standalone_context(&ctx->field_types, source, source_len, module_qn,
                                           CBM_SOURCE_ORIGIN_RAW);

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
        if (!new_paths || !new_qns) {
            ctx->binding_failed = true;
            return;
        }
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

/* Parentheses preserve one expression's storage identity. Ignore comments,
 * never peel casts/comma expressions, and fail closed at the finite bound. */
static TSNode c_fp_unwrap_parentheses(CLSPContext *ctx, TSNode node) {
    for (unsigned depth = 0;
         !ts_node_is_null(node) && !strcmp(ts_node_type(node), "parenthesized_expression");
         depth++) {
        /* Only this expression's reading is lost. The rest of the file still has
         * its own scopes, which a latch here would withhold from every later site. */
        if (depth == 64 || ts_node_has_error(node)) {
            return (TSNode){0};
        }
        TSNode expression = {0};
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        bool valid = true;
        if (ts_tree_cursor_goto_first_child(&cursor))
            do {
                TSNode child = ts_tree_cursor_current_node(&cursor);
                if (!ts_node_is_named(child) || !strcmp(ts_node_type(child), "comment"))
                    continue;
                if (!ts_node_is_null(expression)) {
                    valid = false;
                    break;
                }
                expression = child;
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        ts_tree_cursor_delete(&cursor);
        if (!valid || ts_node_is_null(expression)) {
            ctx->binding_failed = true;
            return (TSNode){0};
        }
        node = expression;
    }
    return node;
}

/* Follow only declaration wrappers, preserving whether storage is an alias.
 * The same exact leaf is used when finalizing an initializer after its RHS. */
static TSNode c_fp_declarator_name(CLSPContext *ctx, TSNode node, bool *reference) {
    for (unsigned depth = 0; !ts_node_is_null(node); depth++) {
        if (depth == 64) {
            ctx->binding_failed = true;
            return (TSNode){0};
        }
        const char *kind = ts_node_type(node);
        if (!strcmp(kind, "identifier"))
            return node;
        bool is_reference = !strcmp(kind, "reference_declarator");
        if (reference && is_reference)
            *reference = true;
        if (strcmp(kind, "function_declarator") && strcmp(kind, "pointer_declarator") &&
            strcmp(kind, "parenthesized_declarator") && !is_reference)
            return (TSNode){0};
        TSNode inner = ts_node_child_by_field_name(node, "declarator", 10);
        if (ts_node_is_null(inner)) {
            TSTreeCursor cursor = ts_tree_cursor_new(node);
            bool single = true;
            if (ts_tree_cursor_goto_first_child(&cursor))
                do {
                    TSNode child = ts_tree_cursor_current_node(&cursor);
                    if (!ts_node_is_named(child) || !strcmp(ts_node_type(child), "comment"))
                        continue;
                    if (!ts_node_is_null(inner)) {
                        single = false;
                        break;
                    }
                    inner = child;
                } while (ts_tree_cursor_goto_next_sibling(&cursor));
            ts_tree_cursor_delete(&cursor);
            if (!single)
                return (TSNode){0};
        }
        node = inner;
    }
    return (TSNode){0};
}

// --- Helper: track function pointer target ---
static void c_add_fp_target(CLSPContext *ctx, const char *var_name, const char *target_qn,
                            const cbm_resolve_proof_t *origin) {
    if (ctx->fp_count >= ctx->fp_cap) {
        int new_cap = ctx->fp_cap == 0 ? 16 : ctx->fp_cap * 2;
        const char **new_names =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        const char **new_qns =
            (const char **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const char *));
        const CBMScope **new_scopes =
            (const CBMScope **)cbm_arena_alloc(ctx->arena, new_cap * sizeof(const CBMScope *));
        bool *new_proven = cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*new_proven));
        bool *new_escaped = cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*new_escaped));
        const cbm_resolve_proof_t **new_proofs =
            cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*new_proofs));
        if (!new_names || !new_qns || !new_scopes || !new_proven || !new_escaped || !new_proofs) {
            ctx->binding_failed = true;
            return;
        }
        if (ctx->fp_var_names && ctx->fp_count > 0) {
            memcpy(new_names, ctx->fp_var_names, ctx->fp_count * sizeof(const char *));
            memcpy(new_qns, ctx->fp_target_qns, ctx->fp_count * sizeof(const char *));
            memcpy(new_scopes, ctx->fp_binding_scopes, ctx->fp_count * sizeof(const CBMScope *));
            memcpy(new_proven, ctx->fp_target_proven, (size_t)ctx->fp_count * sizeof(*new_proven));
            memcpy(new_escaped, ctx->fp_storage_escaped,
                   (size_t)ctx->fp_count * sizeof(*new_escaped));
            memcpy(new_proofs, ctx->fp_target_proofs, (size_t)ctx->fp_count * sizeof(*new_proofs));
        }
        ctx->fp_var_names = new_names;
        ctx->fp_target_qns = new_qns;
        ctx->fp_binding_scopes = new_scopes;
        ctx->fp_target_proven = new_proven;
        ctx->fp_storage_escaped = new_escaped;
        ctx->fp_target_proofs = new_proofs;
        ctx->fp_cap = new_cap;
    }
    ctx->fp_var_names[ctx->fp_count] = cbm_arena_strdup(ctx->arena, var_name);
    ctx->fp_target_qns[ctx->fp_count] = target_qn ? cbm_arena_strdup(ctx->arena, target_qn) : NULL;
    ctx->fp_binding_scopes[ctx->fp_count] = ctx->current_scope;
    ctx->fp_target_proven[ctx->fp_count] = origin && target_qn;
    ctx->fp_storage_escaped[ctx->fp_count] = false;
    ctx->fp_target_proofs[ctx->fp_count] = origin;
    ctx->fp_count++;
}

static bool c_scope_declares_name(const CBMScope *scope, const char *var_name) {
    if (!scope || !var_name) {
        return false;
    }
    return cbm_scope_lookup_local(scope, var_name) != NULL;
}

/* Return the exact fp-state entry for the nearest lexical declaration. An
 * ordinary declaration in an intervening scope blocks an outer fp target even
 * though it has no fp-state entry of its own. */
static int c_find_fp_binding_index(CLSPContext *ctx, const char *var_name) {
    if (!ctx || !var_name) {
        return -1;
    }
    for (int i = ctx->fp_count - 1; i >= 0; i--) {
        if (!ctx->fp_var_names[i] || strcmp(ctx->fp_var_names[i], var_name) != 0) {
            continue;
        }
        const CBMScope *binding_scope = ctx->fp_binding_scopes[i];
        for (const CBMScope *scope = ctx->current_scope; scope; scope = scope->parent) {
            if (scope == binding_scope) {
                return i;
            }
            if (c_scope_declares_name(scope, var_name)) {
                return -1;
            }
        }
    }
    return -1;
}

// Look up an exact target; NULL also represents a present but ambiguous
// binding.
static const char *c_lookup_fp_target(CLSPContext *ctx, const char *var_name) {
    int index = c_find_fp_binding_index(ctx, var_name);
    return index >= 0 ? ctx->fp_target_qns[index] : NULL;
}

// Reassignment replaces the whole proof-bearing version. NULL clears both
// target and provenance; a later use cannot reconstruct authority from a name.
// An escaped binding remains unknown even after an exact-looking assignment:
// aliases or deferred writes can still change the same storage afterward.
static void c_update_fp_target(CLSPContext *ctx, const char *var_name,
                               const cbm_resolve_proof_t *origin) {
    if (!ctx || !var_name)
        return;
    int index = c_find_fp_binding_index(ctx, var_name);
    if (index >= 0) {
        if (ctx->fp_storage_escaped[index])
            origin = NULL;
        const char *target_qn = origin ? origin->value.outcome.target : NULL;
        ctx->fp_target_qns[index] = target_qn ? cbm_arena_strdup(ctx->arena, target_qn) : NULL;
        ctx->fp_target_proven[index] = origin && target_qn;
        ctx->fp_target_proofs[index] = origin;
    }
}

/* This is a property of the nearest lexical storage binding, not its current
 * value version. Shadow declarations have independent state; popping their
 * scope leaves any outer binding's escape state intact. */
static void c_escape_fp_storage(CLSPContext *ctx, const char *var_name) {
    int index = c_find_fp_binding_index(ctx, var_name);
    if (index < 0)
        return;
    ctx->fp_storage_escaped[index] = true;
    ctx->fp_target_qns[index] = NULL;
    ctx->fp_target_proven[index] = false;
    ctx->fp_target_proofs[index] = NULL;
}

// --- Helper: extract function name from DLL/dynamic resolver call ---
// Heuristic: if an expression is a call (possibly cast-wrapped) with a string
// literal argument, the string is likely an external function name being
// resolved dynamically (GetProcAddress, dlsym, or custom resolver). Returns the
// function name string (without quotes), or NULL. Sets *out_has_cast to true if
// the expression was wrapped in a cast.
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
        // static_cast<T>(expr) →
        // call_expression(template_function("static_cast",<T>), (expr))
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
        // Validate: function names are identifiers (no spaces, path separators,
        // dots)
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

// --- Helper: pending template calls (member calls on TYPE_PARAM inside
// templates) ---
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

// Resolve pending template calls for a function being called with known arg
// types. Deduces type params from call-site args and resolves stored method
// calls.
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
    // knows about (invalid code, macros, variadic calls, or parser recovery). The
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
// Extract the namespace from a qualified name: "std.vector" → "std",
// "boost.asio.ip" → "boost.asio"
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

// ADL: resolve unqualified function call by searching namespaces of argument
// types. E.g., swap(a, b) where a is std::string → look up std::swap.
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

    // Try each namespace, then the module-prefixed form of it. An argument type
    // written as `ns::Data` evaluates to the namespace QN `ns`, but the function
    // is registered under the module-qualified `<module>.ns.serialize`; without
    // the module-prefixed retry the namespace-scoped overload is never found.
    for (int i = 0; i < ns_count; i++) {
        const CBMRegisteredFunc *f = cbm_registry_lookup_symbol(ctx->registry, namespaces[i], name);
        if (!f && ctx->module_qn) {
            const char *prefixed =
                cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, namespaces[i]);
            f = cbm_registry_lookup_symbol(ctx->registry, prefixed, name);
        }
        if (f)
            return f->qualified_name;
    }

    return NULL;
}

/* Visibility is a property of the source occurrence, not of a registry's
 * globally unique short name. This small index covers nondependent free
 * declarations and literal includes; unmodelled lookup branches abstain. */
typedef struct CProofDeclaration {
    const char *qn;
    uint32_t at;
    TSNode site;
    const CBMType *signature;
    bool external, c_linkage;
    bool no_prototype; /* C `int f();` says nothing of the parameters */
} CProofDeclaration;

typedef struct CProofBuiltinBinding {
    const CBMVarBinding *binding;
    const CBMType *type;
} CProofBuiltinBinding;

typedef struct CProofInclude {
    const char *path;
    const char *module;
    const char *file_qn; /* the included File, when the import facts name one */
    uint32_t at;
    const cbm_resolve_proof_t *binding_proof;
} CProofInclude;

typedef struct CProofUsing {
    const char *target;
    const char *owner;
    const CBMScope *scope;
    uint32_t at;
    bool namespace;
    bool supported;
} CProofUsing;

static cbm_resolve_result_t c_proof_missing(const char *reason) {
    return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_UNSUPPORTED, .reason = reason};
}

/* One file this translation unit includes, directly or through other files. Its key
 * is the File QN, since a header and its source share a module QN; a caller with no
 * import facts has only the include table's module to go on. */
typedef struct CTUModule {
    const char *module;
    uint32_t at; /* where the direct include that reaches it stands in this file */
    const cbm_resolve_proof_t *via;
    bool exports_only; /* reached through a header generated from this source file */
    /* In view only because the one includer of this file had it: this file's own includes and
     * declarations answer first. A sibling alternative under another #if may stand here too. */
    bool from_includer;
} CTUModule;

static uint32_t c_tu_hash(const char *text, size_t length) {
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < length; i++)
        hash = (hash ^ (unsigned char)text[i]) * 16777619u;
    return hash;
}

static CTUModule *c_tu_find(CLSPContext *ctx, const char *module, size_t length) {
    if (!ctx->tu_cap)
        return NULL;
    uint32_t mask = ctx->tu_cap - 1;
    for (uint32_t slot = c_tu_hash(module, length) & mask;; slot = (slot + 1) & mask) {
        CTUModule *entry = &ctx->tu_modules[slot];
        if (!entry->module)
            return NULL;
        if (!strncmp(entry->module, module, length) && !entry->module[length])
            return entry;
    }
}

static bool c_tu_add(CLSPContext *ctx, const char *module, uint32_t at,
                     const cbm_resolve_proof_t *via, bool exports_only) {
    if ((ctx->tu_count + 1) * 10 >= ctx->tu_cap * 7) {
        uint32_t cap = ctx->tu_cap ? ctx->tu_cap * 2 : 64;
        CTUModule *next = cbm_arena_calloc(ctx->arena, (size_t)cap * sizeof(*next));
        if (!next) {
            ctx->binding_failed = true;
            return false;
        }
        for (uint32_t i = 0; i < ctx->tu_cap; i++) {
            const CTUModule *old = &ctx->tu_modules[i];
            if (!old->module)
                continue;
            uint32_t slot = c_tu_hash(old->module, strlen(old->module)) & (cap - 1);
            while (next[slot].module)
                slot = (slot + 1) & (cap - 1);
            next[slot] = *old;
        }
        ctx->tu_modules = next;
        ctx->tu_cap = cap;
    }
    uint32_t mask = ctx->tu_cap - 1;
    uint32_t slot = c_tu_hash(module, strlen(module)) & mask;
    while (ctx->tu_modules[slot].module)
        slot = (slot + 1) & mask;
    ctx->tu_modules[slot] = (CTUModule){.module = module,
                                        .at = at,
                                        .via = via,
                                        .exports_only = exports_only,
                                        .from_includer = ctx->tu_adding_includer_context};
    ctx->tu_count++;
    return true;
}

/* The unit's entry for a declaration held in this file, or with this module prefix. */
static const CTUModule *c_tu_for_declaration(CLSPContext *ctx, const char *file_qn, const char *qn,
                                             size_t module_len) {
    return file_qn ? c_tu_find(ctx, file_qn, strlen(file_qn)) : c_tu_find(ctx, qn, module_len);
}

static const char *c_own_file_key(const CLSPContext *ctx) {
    return ctx->own_file_qn ? ctx->own_file_qn : ctx->module_qn;
}

static bool c_is_own_declaration(const CLSPContext *ctx, const char *file_qn, const char *qn,
                                 size_t module_len) {
    if (file_qn && ctx->own_file_qn)
        return !strcmp(file_qn, ctx->own_file_qn);
    return ctx->module_qn && module_len == ctx->module_qn_len &&
           !strncmp(qn, ctx->module_qn, module_len);
}

/* The included file holding this declaration QN, visible at this point in this file.
 * Without file identities a QN may nest inside its module (`m.Outer.Inner`), so try
 * each prefix. */
static const CTUModule *c_tu_owner(CLSPContext *ctx, const char *qn, uint32_t at) {
    CBMTypeQNIter iterator;
    cbm_registry_types_with_qn(ctx->proof_registry, qn, &iterator);
    const CBMRegisteredType *candidate;
    while ((candidate = cbm_type_qn_next(&iterator, NULL))) {
        if (!candidate->source_file_qn)
            continue;
        const CTUModule *entry =
            c_tu_find(ctx, candidate->source_file_qn, strlen(candidate->source_file_qn));
        if (entry && !entry->exports_only && entry->at < at)
            return entry;
    }
    for (const char *dot = strrchr(qn, '.'); dot; dot--) {
        if (*dot == '.') {
            const CTUModule *entry = c_tu_find(ctx, qn, (size_t)(dot - qn));
            if (entry && !entry->exports_only && entry->at < at)
                return entry;
        }
        if (dot == qn)
            break;
    }
    return NULL;
}

/* Preserve every namespace segment. In particular, inline ABI namespaces
 * cannot be removed from a declaration identity. */
static const char *c_proof_name(CLSPContext *ctx, const char *text) {
    if (!text || !*text)
        return NULL;
    if (text[0] == ':' && text[1] == ':')
        text += 2;
    char *out = cbm_arena_alloc(ctx->arena, strlen(text) + 1);
    if (!out) {
        ctx->binding_failed = true;
        return NULL;
    }
    size_t length = 0;
    bool start = true;
    for (size_t i = 0; text[i]; i++) {
        unsigned char ch = (unsigned char)text[i];
        if (ch == ':' && text[i + 1] == ':' && !start) {
            out[length++] = '.';
            i++;
            start = true;
        } else if ((start && (isalpha(ch) || ch == '_')) ||
                   (!start && (isalnum(ch) || ch == '_'))) {
            out[length++] = (char)ch;
            start = false;
        } else {
            return NULL;
        }
    }
    if (start)
        return NULL;
    out[length] = '\0';
    return out;
}

static void c_proof_add_declaration(CLSPContext *ctx, const char *qn, uint32_t at) {
    if (ctx->proof_declaration_count == ctx->proof_declaration_cap) {
        int cap = ctx->proof_declaration_cap ? ctx->proof_declaration_cap * 2 : 32;
        CProofDeclaration *next = cbm_arena_alloc(ctx->arena, (size_t)cap * sizeof(*next));
        if (!next) {
            ctx->binding_failed = true;
            return;
        }
        if (ctx->proof_declaration_count)
            memcpy(next, ctx->proof_declarations,
                   (size_t)ctx->proof_declaration_count * sizeof(*next));
        ctx->proof_declarations = next;
        ctx->proof_declaration_cap = cap;
    }
    ctx->proof_declarations[ctx->proof_declaration_count++] =
        (CProofDeclaration){.qn = qn, .at = at};
}

static int c_proof_declaration_compare(const void *left, const void *right) {
    const CProofDeclaration *a = left, *b = right;
    int order = strcmp(a->qn, b->qn);
    return order ? order : (a->at > b->at) - (a->at < b->at);
}

static bool c_proof_declared(CLSPContext *ctx, const char *qn, uint32_t at) {
    int lo = 0, hi = ctx->proof_declaration_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (strcmp(ctx->proof_declarations[mid].qn, qn) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo < ctx->proof_declaration_count && strcmp(ctx->proof_declarations[lo].qn, qn) == 0 &&
           ctx->proof_declarations[lo].at < at;
}

static int c_proof_include_compare(const void *left, const void *right) {
    const CProofInclude *a = left, *b = right;
    int order = strcmp(a->path, b->path);
    return order ? order : strcmp(a->module, b->module);
}

static int c_proof_offset_compare(const void *left, const void *right) {
    uint32_t a = *(const uint32_t *)left, b = *(const uint32_t *)right;
    return (a > b) - (a < b);
}

static void c_proof_add_unknown_include(CLSPContext *ctx, uint32_t at) {
    if (ctx->proof_unknown_include_count == ctx->proof_unknown_include_cap) {
        int cap = ctx->proof_unknown_include_cap ? ctx->proof_unknown_include_cap * 2 : 8;
        uint32_t *next = cbm_arena_alloc(ctx->arena, (size_t)cap * sizeof(*next));
        if (!next) {
            ctx->binding_failed = true;
            return;
        }
        if (ctx->proof_unknown_include_count)
            memcpy(next, ctx->proof_unknown_includes,
                   (size_t)ctx->proof_unknown_include_count * sizeof(*next));
        ctx->proof_unknown_includes = next;
        ctx->proof_unknown_include_cap = cap;
    }
    ctx->proof_unknown_includes[ctx->proof_unknown_include_count++] = at;
}

static void c_proof_include_site(CLSPContext *ctx, TSNode node) {
    TSNode path = ts_node_child_by_field_name(node, "path", 4);
    if (ts_node_is_null(path))
        return;
    char *text = c_node_text(ctx, path);
    size_t len = text ? strlen(text) : 0;
    if (len < 3 ||
        !((text[0] == '"' && text[len - 1] == '"') || (text[0] == '<' && text[len - 1] == '>')))
        return;
    bool quoted = text[0] == '"';
    text[len - 1] = '\0';
    text++;
    int lo = 0, hi = ctx->include_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (strcmp(ctx->proof_includes[mid].path, text) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    bool known = false;
    for (int i = lo; i < ctx->include_count && !strcmp(ctx->proof_includes[i].path, text); i++) {
        if (ts_node_start_byte(node) < ctx->proof_includes[i].at)
            ctx->proof_includes[i].at = ts_node_start_byte(node);
        known |= ctx->proof_includes[i].file_qn ||
                 (ctx->proof_includes[i].module[0] && ctx->c_decls &&
                  cbm_c_file_facts(ctx->c_decls, ctx->proof_includes[i].module));
    }
    if (quoted && !known)
        c_proof_add_unknown_include(ctx, ts_node_start_byte(node));
}

static TSNode c_proof_function_name(TSNode declaration) {
    TSNode cursor = declaration;
    bool recovered = false;
    for (int depth = 0; depth < 32 && !ts_node_is_null(cursor); depth++) {
        const char *kind = ts_node_type(cursor);
        if (!strcmp(kind, "function_declarator")) {
            TSNode real = cbm_c_recovered_func_name(cursor);
            if (!ts_node_is_null(real))
                return real;
            TSNode name = ts_node_child_by_field_name(cursor, "declarator", 10);
            const char *nk = ts_node_is_null(name) ? "" : ts_node_type(name);
            /* `API RetT\nname(...)`: the grammar reads RetT as a scope with its `::` missing. */
            if (!strcmp(nk, "qualified_identifier") && cbm_c_qualifier_is_recovered(name))
                return ts_node_child_by_field_name(name, "name", 4);
            if (recovered && !strcmp(nk, "type_identifier"))
                return name;
            /* A parenthesized pointer declarator is a variable, not a function. */
            return !strcmp(nk, "identifier") || !strcmp(nk, "qualified_identifier") ||
                           !strcmp(nk, "scoped_identifier")
                       ? name
                       : (TSNode){0};
        }
        /* `API RetT *\nname(...)` holds the rest of its declarator on the name side. */
        if (!strcmp(kind, "qualified_identifier") && cbm_c_qualifier_is_recovered(cursor)) {
            cursor = ts_node_child_by_field_name(cursor, "name", 4);
            recovered = true;
            continue;
        }
        if (recovered && !strcmp(kind, "pointer_type_declarator")) {
            cursor = ts_node_child_by_field_name(cursor, "declarator", 10);
            continue;
        }
        if (strcmp(kind, "pointer_declarator") && strcmp(kind, "reference_declarator") &&
            strcmp(kind, "function_definition"))
            break;
        cursor = ts_node_child_by_field_name(cursor, "declarator", 10);
        if (ts_node_is_null(cursor) && !strcmp(kind, "reference_declarator") &&
            ts_node_named_child_count(declaration))
            cursor = ts_node_named_child(declaration, 0);
    }
    return (TSNode){0};
}

static bool c_proof_internal_declaration(CLSPContext *ctx, TSNode node) {
    for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
        TSNode child = ts_node_named_child(node, i);
        if (!strcmp(ts_node_type(child), "storage_class_specifier")) {
            const char *text = c_node_text(ctx, child);
            if (text && !strcmp(text, "static"))
                return true;
        }
    }
    return false;
}

static bool c_proof_c_linkage(CLSPContext *ctx, TSNode node) {
    if (!ctx->cpp_mode)
        return true;
    for (TSNode parent = ts_node_parent(node); !ts_node_is_null(parent);
         parent = ts_node_parent(parent)) {
        if (strcmp(ts_node_type(parent), "linkage_specification"))
            continue;
        TSNode value = ts_node_child_by_field_name(parent, "value", 5);
        const char *text = ts_node_is_null(value) ? NULL : c_node_text(ctx, value);
        return text && !strcmp(text, "\"C\"");
    }
    return false;
}

/* A macro defined in a function body is in force from there to the end of the file. */
static void c_proof_collect_body_macros(CLSPContext *ctx, TSNode body) {
    uint32_t begin = ts_node_start_byte(body), end = ts_node_end_byte(body);
    if (!cbm_c_text_defines_macro(ctx->source + begin, end - begin))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    bool more = true;
    while (more) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        const char *kind = ts_node_type(node);
        bool macro = !strcmp(kind, "preproc_def") || !strcmp(kind, "preproc_function_def");
        if (macro) {
            TSNode name = ts_node_child_by_field_name(node, "name", 4);
            const char *text = ts_node_is_null(name) ? NULL : c_node_text(ctx, name);
            const char *key = text ? cbm_arena_sprintf(ctx->arena, "$macro:%s", text) : NULL;
            if (key)
                c_proof_add_declaration(ctx, key, ts_node_end_byte(name));
        }
        if (!macro && ts_tree_cursor_goto_first_child(&cursor))
            continue;
        while (!(more = ts_tree_cursor_goto_next_sibling(&cursor)))
            if (!ts_tree_cursor_goto_parent(&cursor))
                break;
    }
    ts_tree_cursor_delete(&cursor);
}

static void c_proof_collect(CLSPContext *ctx, TSNode node, const char *owner, int depth) {
    /* An #elif chain nests a level per branch. Past this depth declarations go
     * uncollected, so their uses abstain; nothing else in the file is affected. */
    if (depth > 512)
        return;
    const char *kind = ts_node_type(node);
    if (!strcmp(kind, "struct_specifier") || !strcmp(kind, "class_specifier") ||
        !strcmp(kind, "union_specifier") || !strcmp(kind, "enum_specifier")) {
        TSNode name = ts_node_child_by_field_name(node, "name", 4);
        const char *text = ts_node_is_null(name) ? NULL : c_node_text(ctx, name);
        const char *qn = text ? cbm_arena_sprintf(ctx->arena, "$type:%s.%s", owner, text) : NULL;
        if (qn)
            c_proof_add_declaration(ctx, qn, ts_node_end_byte(name));
        /* A class declared inside a class is declared in this file too. */
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        if (!qn || !ctx->cpp_mode || ts_node_is_null(body) || !strcmp(kind, "enum_specifier"))
            return;
        const char *inner_owner = qn + strlen("$type:");
        TSTreeCursor members = ts_tree_cursor_new(body);
        if (ts_tree_cursor_goto_first_child(&members))
            do {
                TSNode member = ts_tree_cursor_current_node(&members);
                const char *member_kind = ts_node_type(member);
                TSNode type = strcmp(member_kind, "field_declaration")
                                  ? (TSNode){0}
                                  : ts_node_child_by_field_name(member, "type", 4);
                if (!ts_node_is_null(type))
                    c_proof_collect(ctx, type, inner_owner, depth + 1);
                else if (!strcmp(member_kind, "template_declaration"))
                    c_proof_collect(ctx, member, inner_owner, depth + 1);
            } while (ts_tree_cursor_goto_next_sibling(&members));
        ts_tree_cursor_delete(&members);
        return;
    }
    if (!strcmp(kind, "preproc_include")) {
        c_proof_include_site(ctx, node);
        return;
    }
    if (!strcmp(kind, "preproc_def") || !strcmp(kind, "preproc_function_def")) {
        TSNode name = ts_node_child_by_field_name(node, "name", 4);
        const char *text = ts_node_is_null(name) ? NULL : c_node_text(ctx, name);
        const char *key = text ? cbm_arena_sprintf(ctx->arena, "$macro:%s", text) : NULL;
        if (key)
            c_proof_add_declaration(ctx, key, ts_node_end_byte(name));
        return;
    }
    if (!strcmp(kind, "preproc_call")) {
        TSNode directive = ts_node_child_by_field_name(node, "directive", 9);
        TSNode argument = ts_node_child_by_field_name(node, "argument", 8);
        const char *word = ts_node_is_null(directive) ? NULL : c_node_text(ctx, directive);
        if (word && !strcmp(word, "#undef") && !ts_node_is_null(argument)) {
            const char *text = c_node_text(ctx, argument);
            size_t length = text ? strcspn(text, " \t\r\n/") : 0;
            const char *key =
                length ? cbm_arena_sprintf(ctx->arena, "$undef:%.*s", (int)length, text) : NULL;
            if (key)
                c_proof_add_declaration(ctx, key, ts_node_end_byte(argument));
        }
        return;
    }
    if (!strcmp(kind, "type_definition")) {
        TSNode type = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type))
            c_proof_collect(ctx, type, owner, depth + 1);
        TSNode alias = ts_node_child_by_field_name(node, "declarator", 10);
        for (int hop = 0; hop < 8 && !ts_node_is_null(alias); hop++) {
            if (!strcmp(ts_node_type(alias), "type_identifier")) {
                const char *text = c_node_text(ctx, alias);
                const char *qn =
                    text ? cbm_arena_sprintf(ctx->arena, "$type:%s.%s", owner, text) : NULL;
                if (qn)
                    c_proof_add_declaration(ctx, qn, ts_node_end_byte(alias));
                /* `typedef struct {...} Name;` gives no tag of that name. */
                if (text && !ts_node_is_null(type) &&
                    ts_node_is_null(ts_node_child_by_field_name(type, "name", 4)) &&
                    !ts_node_is_null(ts_node_child_by_field_name(type, "body", 4))) {
                    const char *key = cbm_arena_sprintf(ctx->arena, "$no-tag:%s", text);
                    if (key)
                        c_proof_add_declaration(ctx, key, ts_node_end_byte(alias));
                }
                break;
            }
            alias = ts_node_child_by_field_name(alias, "declarator", 10);
        }
        return;
    }
    if (!strcmp(kind, "function_definition") || !strcmp(kind, "declaration")) {
        TSNode function_body = ts_node_child_by_field_name(node, "body", 4);
        if (!ts_node_is_null(function_body))
            c_proof_collect_body_macros(ctx, function_body);
        uint32_t count = 0;
        TSNode *kids = cbm_lsp_collect_children(ctx->arena, node, &count);
        if (!kids && count) {
            ctx->binding_failed = true;
            return;
        }
        for (uint32_t i = 0; i < count; i++) {
            const char *child_kind = ts_node_type(kids[i]);
            if (!strcmp(child_kind, "struct_specifier") || !strcmp(child_kind, "class_specifier") ||
                !strcmp(child_kind, "union_specifier") || !strcmp(child_kind, "enum_specifier")) {
                c_proof_collect(ctx, kids[i], owner, depth + 1);
                continue;
            }
            TSNode name_node = c_proof_function_name(kids[i]);
            if (ts_node_is_null(name_node))
                continue;
            const char *name = c_proof_name(ctx, c_node_text(ctx, name_node));
            if (name && !strchr(name, '.')) {
                const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name);
                if (qn) {
                    int before = ctx->proof_declaration_count;
                    c_proof_add_declaration(ctx, qn, ts_node_end_byte(name_node));
                    if (ctx->proof_declaration_count > before && !strcmp(kind, "declaration")) {
                        CProofDeclaration *decl = &ctx->proof_declarations[before];
                        decl->site = node;
                        decl->signature =
                            cbm_c_free_signature(ctx->arena, ctx->source, node, ctx->cpp_mode);
                        decl->external = !c_proof_internal_declaration(ctx, node);
                        decl->c_linkage = c_proof_c_linkage(ctx, node);
                        TSNode list = ts_node_parent(name_node);
                        list = ts_node_is_null(list)
                                   ? list
                                   : ts_node_child_by_field_name(list, "parameters", 10);
                        decl->no_prototype = !ctx->cpp_mode && !ts_node_is_null(list) &&
                                             !ts_node_named_child_count(list);
                    }
                } else {
                    ctx->binding_failed = true;
                }
            }
        }
        return;
    }
    if (!strcmp(kind, "namespace_definition")) {
        TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
        TSNode body = ts_node_child_by_field_name(node, "body", 4);
        /* What an anonymous namespace declares is seen in the file around it, with no segment of
         * its own in the name. LLVM's .cpp files keep most of their classes there. */
        if (ts_node_is_null(name_node) && !ts_node_is_null(body) && !ts_node_is_missing(body)) {
            c_proof_collect(ctx, body, owner, depth + 1);
            return;
        }
        /* Missing or malformed named namespaces cannot establish an exact declaration owner. */
        if (ts_node_is_null(name_node) || ts_node_is_missing(name_node) ||
            ts_node_has_error(name_node) || ts_node_is_null(body) || ts_node_is_missing(body))
            return;
        const char *name = c_proof_name(ctx, c_node_text(ctx, name_node));
        if (!name)
            return;
        const char *qn = cbm_arena_sprintf(ctx->arena, "%s.%s", owner, name);
        if (!qn) {
            ctx->binding_failed = true;
            return;
        }
        c_proof_collect(ctx, body, qn, depth + 1);
        return;
    }
    /* `bool f(int x)` loose in an ERROR, its braces split by an #ifdef, still declares f here. */
    if (!strcmp(kind, "function_declarator")) {
        TSNode name = ts_node_child_by_field_name(node, "declarator", 10);
        TSNode parent = ts_node_parent(node);
        if (!ts_node_is_null(name) && !strcmp(ts_node_type(name), "identifier") &&
            !ts_node_is_null(parent) && !strcmp(ts_node_type(parent), "ERROR")) {
            const char *text = c_node_text(ctx, name);
            const char *qn = text ? cbm_arena_sprintf(ctx->arena, "%s.%s", owner, text) : NULL;
            if (qn)
                c_proof_add_declaration(ctx, qn, ts_node_end_byte(name));
        }
        return;
    }
    /* A branch of conditional compilation declares its names in the configurations
     * that compile it, and code using them compiles only in those. Error recovery
     * around a directive leaves complete declarations inside the ERROR. Templates,
     * classes and local prototypes need additional domain proofs. */
    if (strcmp(kind, "translation_unit") && strcmp(kind, "declaration_list") &&
        strcmp(kind, "linkage_specification") && strcmp(kind, "preproc_if") &&
        strcmp(kind, "preproc_ifdef") && strcmp(kind, "preproc_else") &&
        strcmp(kind, "preproc_elif") && strcmp(kind, "preproc_elifdef") && strcmp(kind, "ERROR") &&
        strcmp(kind, "template_declaration"))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor)) {
        do {
            TSNode child = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(child))
                c_proof_collect(ctx, child, owner, depth + 1);
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    }
    ts_tree_cursor_delete(&cursor);
}

/* Configured test macros have a separately audited name/body identity. The
 * C grammar can mark their declaration wrapper as erroneous even when the
 * body is an ordinary compound statement. Only that authenticated wrapper
 * may be excluded from the syntax check; errors inside the body still fail. */
static bool c_proof_configured_wrapper(CLSPContext *ctx, TSNode node) {
    const CBMFileResult *owners = ctx->test_definition_owners;
    if (!owners || !owners->has_test_definition_owners)
        return false;
    TSNode name, body;
    if (cbm_test_definition_owner_qn(owners, node) &&
        cbm_test_definition_candidate(node, owners->test_owner_language, &name, &body))
        return !ts_node_is_null(body) && !ts_node_has_error(body);
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "ERROR") && strcmp(kind, "expression_statement"))
        return false;
    TSNode next = ts_node_next_named_sibling(node);
    while (!ts_node_is_null(next) && !strcmp(ts_node_type(next), "comment"))
        next = ts_node_next_named_sibling(next);
    if (!cbm_test_definition_owner_qn(owners, next) ||
        !cbm_test_definition_candidate(next, owners->test_owner_language, &name, &body) ||
        ts_node_is_null(body) || ts_node_has_error(body))
        return false;
    return ts_node_start_byte(name) >= ts_node_start_byte(node) &&
           ts_node_end_byte(name) <= ts_node_end_byte(node);
}

/* The proof boundary is the first syntax error no configured wrapper accounts for:
 * sites before it are audited complete and keep their proofs, sites at or after it
 * do not. Only the spine of error-bearing nodes is descended, with a cursor rather
 * than recursion, so how deeply the error is nested is not a property the answer can
 * depend on. Real C nests that deep: an #ifdef inside an else-if chain puts redis
 * src/defrag.c past 16 levels, and treating that as a failure of the whole file
 * withheld every proof in it. */
static uint32_t c_proof_first_error(CLSPContext *ctx, TSNode root);
static uint32_t c_proof_first_declaring_error(CLSPContext *ctx, TSNode body);

static void c_proof_audit_syntax(CLSPContext *ctx, TSNode root) {
    ctx->proof_syntax_limit = c_proof_first_error(ctx, root);
}

/* A function's own scopes are read from its own subtree. A file-scope syntax error
 * elsewhere, nearly always an #ifdef that splits a statement, says nothing about
 * them: tree-sitter resumes at the next complete top-level item, and a file-scope
 * declaration lost in the error can only hide a name no proof would then find.
 * Within a function nothing after its first error is read. Configured test owners
 * keep the whole file's first error, since they name their bodies through the
 * surrounding text. */
static void c_proof_enter_function(CLSPContext *ctx, TSNode func_node) {
    if (ctx->test_definition_owners && ctx->test_definition_owners->has_test_definition_owners)
        return;
    /* Attribute macros between the parameters and the brace put the error in the
     * header, ahead of a perfectly readable body. */
    TSNode body = ts_node_child_by_field_name(func_node, "body", 4);
    ctx->proof_syntax_limit = !ts_node_is_null(body) && ts_node_has_error(body)
                                  ? c_proof_first_declaring_error(ctx, body)
                                  : ts_node_end_byte(func_node);
}

/* What a syntax error can do to a later site in the same function is hide a local
 * declaration, and then a global of that name would seem to be in reach. An error
 * holding no type and no pair of adjacent names cannot be a declaration: it is an
 * #ifdef inside an expression, or a stray token, and what follows reads normally. */
static bool c_proof_error_may_declare(TSNode error) {
    uint32_t count = ts_node_named_child_count(error);
    bool previous_identifier = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(error, i);
        const char *kind = ts_node_type(child);
        if (!strcmp(kind, "comment"))
            continue;
        if (!strcmp(kind, "primitive_type") || !strcmp(kind, "type_identifier") ||
            !strcmp(kind, "sized_type_specifier") || !strcmp(kind, "struct_specifier") ||
            !strcmp(kind, "union_specifier") || !strcmp(kind, "enum_specifier") ||
            !strcmp(kind, "declaration") || !strcmp(kind, "storage_class_specifier") ||
            !strcmp(kind, "type_qualifier") || !strcmp(kind, "init_declarator") ||
            !strcmp(kind, "pointer_declarator") || !strcmp(kind, "array_declarator"))
            return true;
        bool identifier = !strcmp(kind, "identifier");
        if (identifier && previous_identifier)
            return true;
        previous_identifier = identifier;
    }
    return false;
}

/* A file-scope item is read from its own subtree too: `{ 'Z', nv_Zet }` in a table means what it
 * means whatever an #ifdef did to some function further up the file. */
static uint32_t c_proof_enter_file_item(CLSPContext *ctx, TSNode item);

static uint32_t c_proof_first_declaring_error(CLSPContext *ctx, TSNode body) {
    uint32_t limit = ts_node_end_byte(body);
    TSTreeCursor cursor = ts_tree_cursor_new(body);
    bool walking = true;
    while (walking) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        if (ts_node_has_error(node) && !c_proof_configured_wrapper(ctx, node)) {
            if (ts_node_is_error(node)) {
                if (c_proof_error_may_declare(node)) {
                    limit = ts_node_start_byte(node);
                    break;
                }
            } else if (ts_tree_cursor_goto_first_child(&cursor)) {
                continue;
            }
        }
        while (walking && !ts_tree_cursor_goto_next_sibling(&cursor))
            walking = ts_tree_cursor_goto_parent(&cursor);
    }
    ts_tree_cursor_delete(&cursor);
    return limit;
}

static uint32_t c_proof_enter_file_item(CLSPContext *ctx, TSNode item) {
    uint32_t saved = ctx->proof_syntax_limit;
    if (!ctx->test_definition_owners || !ctx->test_definition_owners->has_test_definition_owners)
        ctx->proof_syntax_limit = c_proof_first_declaring_error(ctx, item);
    return saved;
}

static uint32_t c_proof_first_error(CLSPContext *ctx, TSNode root) {
    uint32_t limit = ts_node_end_byte(root);
    TSTreeCursor cursor = ts_tree_cursor_new(root);
    bool walking = true;
    while (walking) {
        TSNode node = ts_tree_cursor_current_node(&cursor);
        bool faulty = ts_node_has_error(node) || ts_node_is_missing(node);
        if (faulty && !c_proof_configured_wrapper(ctx, node)) {
            if (ts_node_is_error(node) || ts_node_is_missing(node)) {
                limit = ts_node_start_byte(node);
                break;
            }
            if (ts_tree_cursor_goto_first_child(&cursor))
                continue;
        }
        while (walking && !ts_tree_cursor_goto_next_sibling(&cursor))
            walking = ts_tree_cursor_goto_parent(&cursor);
    }
    ts_tree_cursor_delete(&cursor);
    return limit;
}

static int c_proof_include_site_compare(const void *left, const void *right) {
    const CProofInclude *a = left, *b = right;
    if (a->at != b->at)
        return a->at < b->at ? -1 : 1;
    int order = strcmp(a->path, b->path);
    return order ? order : strcmp(a->module, b->module);
}

/* Add this file and whatever it includes to the unit, seen from the top of this file. */
static bool c_tu_add_closure(CLSPContext *ctx, const char *file, const cbm_resolve_proof_t *via) {
    const char *own_key = c_own_file_key(ctx);
    size_t cap = 64, head = 0, tail = 0;
    const char **queue = cbm_arena_alloc(ctx->arena, cap * sizeof(*queue));
    if (!queue)
        return false;
    if (!c_tu_find(ctx, file, strlen(file)) && strcmp(file, own_key)) {
        if (!c_tu_add(ctx, file, 0, via, false))
            return false;
    }
    queue[tail++] = file;
    while (head < tail) {
        const CBMCFileFacts *facts = cbm_c_file_facts(ctx->c_decls, queue[head++]);
        for (int n = 0; facts && n < facts->include_count; n++) {
            const char *next = facts->includes[n];
            if (c_tu_find(ctx, next, strlen(next)) || !strcmp(next, own_key))
                continue;
            if (!c_tu_add(ctx, next, 0, via, false))
                return false;
            if (tail == cap) {
                const char **grown = cbm_arena_alloc(ctx->arena, cap * 2 * sizeof(*grown));
                if (!grown)
                    return false;
                memcpy(grown, queue, tail * sizeof(*grown));
                queue = grown;
                cap *= 2;
            }
            queue[tail++] = next;
        }
    }
    return true;
}

/* `ae.c` includes `ae_epoll.c`, which includes nothing and uses aeEventLoop. A header, or a source
 * file with no include of its own, is compiled only through the one file that includes it, so
 * what that includer had in view ahead of the `#include` was in view for this file too. A source
 * file with includes of its own is another matter: Lua's lbaselib.c also sits in one amalgamation
 * and is compiled alone. Each hop goes up one file, and a file seen twice ends it. */
static void c_proof_walk_includers(CLSPContext *ctx, const char *current) {
    const cbm_resolve_proof_t *via = NULL;
    for (uint32_t hop = 0; hop < 64; hop++) {
        const CBMCFileFacts *includer = cbm_c_sole_includer(ctx->c_decls, current);
        const char *key =
            includer ? (includer->file_qn ? includer->file_qn : includer->module_qn) : NULL;
        if (!key || !strcmp(key, c_own_file_key(ctx)))
            return;
        if (!via) {
            cbm_resolve_proof_key_t proof_key = {.lookup_domain = "c-sole-includer",
                                                 .lookup_key = key,
                                                 .binding_identity = key,
                                                 .detail = "the-one-file-that-includes-this-one"};
            cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                          .reason = "the-one-file-that-includes-this-one"};
            via = cbm_resolve_proof_leaf(&ctx->field_types.proofs, CBM_PROOF_ADAPTER_DOMAIN,
                                         &ctx->field_types.proof_source, &proof_key, &audit);
            if (!via) {
                ctx->binding_failed = true;
                return;
            }
        }
        /* Only the includes standing before this file's own in the includer. */
        if (!c_tu_find(ctx, key, strlen(key)) && !c_tu_add(ctx, key, 0, via, false)) {
            ctx->binding_failed = true;
            return;
        }
        for (int n = 0; n < includer->include_count; n++) {
            if (!strcmp(includer->includes[n], current))
                break;
            if (!c_tu_add_closure(ctx, includer->includes[n], via)) {
                ctx->binding_failed = true;
                return;
            }
        }
        current = key;
    }
}

static void c_proof_add_includer_context(CLSPContext *ctx) {
    const char *current = c_own_file_key(ctx);
    if (!ctx->c_decls || !current)
        return;
    const CBMCFileFacts *own_facts = cbm_c_file_facts(ctx->c_decls, current);
    if (!own_facts || (own_facts->source_file && own_facts->include_count > 0))
        return;
    ctx->tu_adding_includer_context = true;
    c_proof_walk_includers(ctx, current);
    ctx->tu_adding_includer_context = false;
}

/* Everything this file sees through its includes, each at the point the direct
 * include that reaches it stands. A branch makes its includes conditional, not
 * absent: a name found through one was still declared there in the configurations
 * that compile the use. A caller defining a header's guard first keeps it out. */
static void c_proof_build_translation_unit(CLSPContext *ctx) {
    if (!ctx->include_count || !ctx->proof_includes)
        return;
    CProofInclude *direct =
        cbm_arena_alloc(ctx->arena, (size_t)ctx->include_count * sizeof(*direct));
    if (!direct) {
        ctx->binding_failed = true;
        return;
    }
    memcpy(direct, ctx->proof_includes, (size_t)ctx->include_count * sizeof(*direct));
    qsort(direct, (size_t)ctx->include_count, sizeof(*direct), c_proof_include_site_compare);
    size_t queue_cap = 64, head = 0, tail = 0;
    const char **queue = cbm_arena_alloc(ctx->arena, queue_cap * sizeof(*queue));
    if (!queue) {
        ctx->binding_failed = true;
        return;
    }
    for (int i = 0; i < ctx->include_count; i++) {
        const CProofInclude *include = &direct[i];
        if (include->at == UINT32_MAX || !include->module[0])
            continue;
        const char *direct_key = include->file_qn ? include->file_qn : include->module;
        const char *own_key = c_own_file_key(ctx);
        const CBMCFileFacts *facts = cbm_c_file_facts(ctx->c_decls, direct_key);
        if (facts && facts->guard) {
            const char *key = cbm_arena_sprintf(ctx->arena, "$macro:%s", facts->guard);
            if (key && c_proof_declared(ctx, key, include->at))
                continue;
        }
        if (c_tu_find(ctx, direct_key, strlen(direct_key)) || !strcmp(direct_key, own_key))
            continue;
        if (!c_tu_add(ctx, direct_key, include->at, include->binding_proof, false))
            return;
        head = tail = 0;
        queue[tail++] = direct_key;
        while (head < tail) {
            facts = cbm_c_file_facts(ctx->c_decls, queue[head++]);
            if (!facts)
                continue;
            if (facts->has_unknown_include)
                c_proof_add_unknown_include(ctx, include->at);
            for (int g = 0; g < facts->generated_count; g++) {
                const char *source = facts->generated_for[g];
                if (!c_tu_find(ctx, source, strlen(source)) && strcmp(source, own_key) &&
                    !c_tu_add(ctx, source, include->at, include->binding_proof, true))
                    return;
            }
            for (int n = 0; n < facts->include_count; n++) {
                const char *next = facts->includes[n];
                if (c_tu_find(ctx, next, strlen(next)) || !strcmp(next, own_key))
                    continue;
                if (!c_tu_add(ctx, next, include->at, include->binding_proof, false))
                    return;
                if (tail == queue_cap) {
                    /* Consumed entries are dead: reuse their room before growing. */
                    size_t live = tail - head;
                    size_t cap = live * 2 >= queue_cap ? queue_cap * 2 : queue_cap;
                    const char **grown = cap == queue_cap
                                             ? queue
                                             : cbm_arena_alloc(ctx->arena, cap * sizeof(*grown));
                    if (!grown) {
                        ctx->binding_failed = true;
                        return;
                    }
                    memmove(grown, queue + head, live * sizeof(*grown));
                    queue = grown;
                    queue_cap = cap;
                    head = 0;
                    tail = live;
                }
                queue[tail++] = next;
            }
        }
    }
}

static void c_proof_prepare(CLSPContext *ctx, TSNode root) {
    ctx->proof_registry = c_proof_registry_snapshot(ctx, ctx->registry, 0);
    ctx->proof_preprocessor_unknown = cbm_c_preprocessor_domain_unsupported(root, ctx->source);
    for (const CBMTypeRegistry *link = ctx->registry; link && !ctx->c_decls; link = link->fallback)
        ctx->c_decls = link->c_decls;
    ctx->own_file_qn = ctx->import_facts ? ctx->import_facts->source.scope : NULL;
    const CBMCFileFacts *own_facts = cbm_c_file_facts(ctx->c_decls, c_own_file_key(ctx));
    /* A .h with no C++ syntax of its own reads as C: no overloads, no argument-
     * dependent lookup, and C linkage whichever language includes it. */
    ctx->plain_header = own_facts && own_facts->plain_header && !own_facts->cpp_syntax;
    c_proof_audit_syntax(ctx, root);
    if (!ctx->proof_registry || !ctx->module_qn || ctx->binding_failed) {
        ctx->binding_failed = true;
        return;
    }
    if (ctx->include_count) {
        ctx->proof_includes =
            cbm_arena_alloc(ctx->arena, (size_t)ctx->include_count * sizeof(*ctx->proof_includes));
        if (!ctx->proof_includes) {
            ctx->binding_failed = true;
            return;
        }
        CBMHashTable *facts = ctx->import_facts ? cbm_ht_create(16) : NULL;
        if (ctx->import_facts && !facts) {
            ctx->binding_failed = true;
            return;
        }
        for (size_t i = 0; ctx->import_facts && i < ctx->import_facts->count; i++) {
            const cbm_resolve_import_fact_t *fact = &ctx->import_facts->items[i];
            if (fact->module_path && !cbm_ht_get(facts, fact->module_path)) {
                cbm_ht_set(facts, fact->module_path, (void *)fact);
                if (cbm_ht_get(facts, fact->module_path) != fact)
                    ctx->binding_failed = true;
            }
        }
        for (int i = 0; i < ctx->include_count; i++) {
            if (!ctx->include_paths[i] || !ctx->include_ns_qns[i]) {
                ctx->binding_failed = true;
                cbm_ht_free(facts);
                return;
            }
            CProofInclude include = {
                .path = ctx->include_paths[i], .module = ctx->include_ns_qns[i], .at = UINT32_MAX};
            const cbm_resolve_import_fact_t *fact = facts ? cbm_ht_get(facts, include.path) : NULL;
            if (fact && cbm_resolve_admit(&fact->dependency) && cbm_resolve_admit(&fact->binding) &&
                fact->dependency.proof && fact->binding.proof &&
                !strcmp(fact->binding.target, include.module)) {
                include.binding_proof = fact->binding.proof;
                include.file_qn = fact->dependency.target;
            }
            ctx->proof_includes[i] = include;
        }
        cbm_ht_free(facts);
        qsort(ctx->proof_includes, (size_t)ctx->include_count, sizeof(*ctx->proof_includes),
              c_proof_include_compare);
    }
    c_proof_collect(ctx, root, ctx->module_qn, 0);
    if (ctx->proof_declaration_count)
        qsort(ctx->proof_declarations, (size_t)ctx->proof_declaration_count,
              sizeof(*ctx->proof_declarations), c_proof_declaration_compare);
    c_proof_build_translation_unit(ctx);
    c_proof_add_includer_context(ctx);
    if (ctx->proof_unknown_include_count > 1)
        qsort(ctx->proof_unknown_includes, (size_t)ctx->proof_unknown_include_count,
              sizeof(*ctx->proof_unknown_includes), c_proof_offset_compare);
}

static void c_proof_record_using(CLSPContext *ctx, TSNode node) {
    char *text = c_node_text(ctx, node);
    if (!text || strstr(text, "enum"))
        return;
    uint32_t count = ts_node_named_child_count(node);
    TSNode name_node = count ? ts_node_named_child(node, count - 1) : (TSNode){0};
    if (ts_node_is_null(name_node))
        return;
    char *name = c_node_text(ctx, name_node);
    const char *target = c_proof_name(ctx, name);
    if (ctx->proof_using_count == ctx->proof_using_cap) {
        int cap = ctx->proof_using_cap ? ctx->proof_using_cap * 2 : 16;
        CProofUsing *next = cbm_arena_alloc(ctx->arena, (size_t)cap * sizeof(*next));
        if (!next) {
            ctx->binding_failed = true;
            return;
        }
        if (ctx->proof_using_count)
            memcpy(next, ctx->proof_usings, (size_t)ctx->proof_using_count * sizeof(*next));
        ctx->proof_usings = next;
        ctx->proof_using_cap = cap;
    }
    ctx->proof_usings[ctx->proof_using_count++] = (CProofUsing){
        .target = target,
        .owner = ctx->current_namespace ? ctx->current_namespace : ctx->module_qn,
        .scope = ctx->current_scope,
        .at = ts_node_start_byte(node),
        .namespace = strstr(text, "namespace") != NULL,
        .supported = target && (!ctx->current_namespace || (name[0] == ':' && name[1] == ':'))};
}

static cbm_resolve_method_candidate_t c_proof_classify(const cbm_resolve_method_query_t *query,
                                                       const CBMTypeRegistry *owner,
                                                       const CBMRegisteredFunc *candidate,
                                                       void *context) {
    (void)query;
    (void)owner;
    CLSPContext *ctx = context;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    if (!(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE))
        return proof;
    proof.applicable = CBM_RESOLVE_METHOD_YES;
    proof.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    proof.graph_target =
        candidate->sig && candidate->sig[0]
            ? cbm_arena_sprintf(ctx->arena, "%s%s", candidate->qualified_name, candidate->sig)
            : candidate->qualified_name;
    proof.graph_target_exact = proof.graph_target != NULL;
    proof.declaration_identity = candidate->declaration_identity;
    /* The #if branches of one file define a function more than once, and the graph
     * holds them as one node. One file and one graph identity is one declaration. */
    if (candidate->source_file_qn && proof.graph_target)
        proof.declaration_identity =
            cbm_arena_sprintf(ctx->arena, "%s|%s", candidate->source_file_qn, proof.graph_target);
    proof.declaration_identity_exact = proof.declaration_identity != NULL;
    return proof;
}

static cbm_resolve_result_t c_proof_query(CLSPContext *ctx, const char *qn,
                                          cbm_resolve_rule_t rule) {
    if (!qn || ctx->binding_failed)
        return c_proof_missing("c_binding_incomplete");
    uint32_t fact = rule == CBM_RESOLVE_RULE_CALLABLE_BINDING ? CBM_RESOLVE_FACT_CALLABLE_TYPED
                    : rule == CBM_RESOLVE_RULE_IMPORT_BINDING ? CBM_RESOLVE_FACT_IMPORT_BOUND
                                                              : CBM_RESOLVE_FACT_SCOPE_BOUND;
    cbm_resolve_function_query_t query = {
        .proof_pool = &ctx->field_types.proofs,
        .proof_source = &ctx->field_types.proof_source,
        .lookup_domain = qn,
        .registry = ctx->proof_registry,
        .function_qn = qn,
        .function_binding = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                             .rule = rule,
                             .facts = fact | CBM_RESOLVE_FACT_OCCURRENCE_EXACT},
        .argument_count = -1,
        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT | CBM_RESOLVE_METHOD_NAME_IDENTITY,
        .classify = c_proof_classify,
        .context = ctx};
    return cbm_resolve_function(&query).result;
}

static cbm_resolve_result_t c_proof_import_join(CLSPContext *ctx, cbm_resolve_result_t result,
                                                const cbm_resolve_proof_t *dependency,
                                                cbm_resolve_proof_kind_t kind) {
    if (!ctx->require_import_proofs)
        return result;
    if (!dependency)
        return c_proof_missing("c_source_import_predecessor_missing");
    if (!cbm_resolve_admit(&result))
        return result;
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(result.proof);
    if (!view)
        return c_proof_missing("c_source_import_result_missing");
    const cbm_resolve_proof_t *premises[] = {result.proof, dependency};
    const cbm_resolve_proof_t *proof = cbm_resolve_proof_derive(
        &ctx->field_types.proofs, kind, &view->source, &view->key, &result, premises, 2, true);
    view = cbm_resolve_proof_view(proof);
    if (!view)
        return c_proof_missing("c_source_import_join_failed");
    result = view->outcome;
    result.proof = proof;
    return result;
}

static cbm_resolve_result_t c_proof_type_import(CLSPContext *ctx, const char *qn, uint32_t at,
                                                cbm_resolve_result_t result) {
    const char *key = cbm_arena_sprintf(ctx->arena, "$type:%s", qn);
    if (!ctx->require_import_proofs || (key && c_proof_declared(ctx, key, at)))
        return result;
    const CTUModule *owner = c_tu_owner(ctx, qn, at);
    if (owner)
        return c_proof_import_join(ctx, result, owner->via, CBM_PROOF_TYPE_BIND);
    return c_proof_missing("c_type_import_predecessor_missing");
}

static bool c_proof_merge(cbm_resolve_result_t *result, cbm_resolve_result_t next) {
    if (next.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        next.binding == CBM_RESOLVE_BINDING_AMBIGUOUS) {
        *result = next;
        return false;
    }
    if (next.binding == CBM_RESOLVE_BINDING_UNRESOLVED)
        return true;
    if (result->binding == CBM_RESOLVE_BINDING_UNIQUE && strcmp(result->target, next.target)) {
        *result = (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                         .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                         .candidate_count = 2,
                                         .reason = "c_visible_declarations_conflict"};
        return false;
    }
    *result = next;
    return true;
}

/* The generic type comparator intentionally compares FUNC values by identity.
 * These separately parsed source signatures have a much narrower audited shape:
 * one builtin return and at most 64 builtin parameters, with no variadic/default
 * or user-defined types. Compare that shape explicitly without broadening the
 * equality contract used by inference elsewhere. */
static bool c_proof_signature_equal(const CBMType *left, const CBMType *right) {
    if (!left || !right || left->kind != CBM_TYPE_FUNC || right->kind != CBM_TYPE_FUNC)
        return false;
    const CBMType **lr = left->data.func.return_types;
    const CBMType **rr = right->data.func.return_types;
    const CBMType **lp = left->data.func.param_types;
    const CBMType **rp = right->data.func.param_types;
    if (!lr || !rr || !lr[0] || !rr[0] || lr[1] || rr[1] || lr[0]->kind != CBM_TYPE_BUILTIN ||
        rr[0]->kind != CBM_TYPE_BUILTIN || !cbm_type_equal(lr[0], rr[0]) || !lp || !rp)
        return false;
    for (unsigned i = 0; i <= 64; i++) {
        if (!lp[i] || !rp[i])
            return lp[i] == NULL && rp[i] == NULL;
        if (i == 64 || lp[i]->kind != CBM_TYPE_BUILTIN || rp[i]->kind != CBM_TYPE_BUILTIN ||
            !cbm_type_equal(lp[i], rp[i]))
            return false;
    }
    return false;
}

/* A visible prototype establishes a linkage key, never a globally unique
 * short-name guess. Every project candidate in that indexed bucket must have
 * audited linkage facts; incompatible, duplicate and unknown definitions close
 * the domain without a target. Static definitions belong to other TUs. */
/* Does a translation unit rooted at this file include that one? A definition whose
 * own file sees the prototype was compiled against it. */
static bool c_proof_file_reaches(CLSPContext *ctx, const char *from, const char *to) {
    if (!strcmp(from, to))
        return true;
    enum { REACH_LIMIT = 4096 };
    const char **seen = cbm_arena_alloc(ctx->arena, REACH_LIMIT * sizeof(*seen));
    if (!seen)
        return false;
    int head = 0, tail = 0;
    seen[tail++] = from;
    while (head < tail) {
        const CBMCFileFacts *facts = cbm_c_file_facts(ctx->c_decls, seen[head++]);
        for (int n = 0; facts && n < facts->include_count; n++) {
            const char *next = facts->includes[n];
            if (!strcmp(next, to))
                return true;
            bool known = false;
            for (int k = 0; k < tail && !known; k++)
                known = !strcmp(seen[k], next);
            if (!known && tail < REACH_LIMIT)
                seen[tail++] = next;
        }
    }
    return false;
}

static const char *c_proof_func_file(CLSPContext *ctx, const CBMRegisteredFunc *candidate,
                                     const char *relative) {
    if (candidate->source_file_qn)
        return candidate->source_file_qn;
    size_t qn = strlen(candidate->qualified_name), rel = strlen(relative);
    return qn > rel + 1 ? cbm_arena_strndup(ctx->arena, candidate->qualified_name, qn - rel - 1)
                        : NULL;
}

static cbm_resolve_result_t c_proof_declaration_lookup(CLSPContext *ctx, const char *spelling,
                                                       TSNode site, const char *target,
                                                       const char *decision, bool imported);

/* `declaring` names the file holding the visible prototype, or NULL when that is
 * this file. Several definitions of one external name are several programs in one
 * repository. The one whose own file includes this prototype was compiled against
 * it, which is what makes it the definition of this declaration. */
static cbm_resolve_result_t c_proof_external_definition(CLSPContext *ctx, const char *relative,
                                                        const CBMType *signature, int arity,
                                                        bool c_linkage, bool any_linkage,
                                                        const char *declaring) {
    if (ctx->proof_preprocessor_unknown)
        return c_proof_missing("c_external_preprocessor_domain_unproven");
    const char *leaf = strrchr(relative, '.');
    const char *name = leaf ? leaf + 1 : relative;
    size_t namespace_len = leaf ? (size_t)(leaf - relative) : 0;
    const CBMRegisteredFunc *chosen = NULL;
    const CBMRegisteredFunc *compiled_against = NULL;
    int definitions = 0, compiled_count = 0;
    CBMFreeFuncIter iterator;
    cbm_registry_free_funcs_by_short_name_chain(ctx->proof_registry, name, &iterator);
    int index;
    while ((index = cbm_free_func_iter_next(&iterator)) >= 0) {
        const CBMRegisteredFunc *candidate = &iterator.reg->funcs[index];
        if (!(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) || candidate->receiver_type ||
            !candidate->short_name || strcmp(candidate->short_name, name))
            continue;
        uint32_t known = CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_TOP_LEVEL;
        if ((candidate->source_flags & known) != known || !candidate->source_namespace)
            return c_proof_missing("c_external_linkage_domain_incomplete");
        if (candidate->source_flags & CBM_SOURCE_DECL_STATIC)
            continue;
        bool candidate_c = (candidate->source_flags & CBM_SOURCE_DECL_EXTERN_C) != 0;
        if (!any_linkage && candidate_c != c_linkage)
            continue;
        if (!candidate_c && (strlen(candidate->source_namespace) != namespace_len ||
                             strncmp(candidate->source_namespace, relative, namespace_len)))
            continue;
        /* C identifies an external name by spelling. Signatures are compared to
         * reject what a compiler would reject, as far as each is understood:
         * builtin types exactly, and otherwise the parameter count. */
        if (signature && candidate->source_signature) {
            if (!c_proof_signature_equal(signature, candidate->source_signature))
                return c_proof_missing("c_external_signature_conflict");
        } else if (arity >= 0 && candidate->source_arity && candidate->source_arity != arity + 1) {
            return c_proof_missing("c_external_signature_conflict");
        }
        if (!(candidate->source_flags & CBM_SOURCE_FUNCTION_DEFINITION))
            continue;
        if (!candidate->declaration_identity || !candidate->graph_qualified_name ||
            candidate->type_param_names)
            return c_proof_missing("c_external_definition_identity_missing");
        /* #if variants of one definition share one graph node. */
        if (chosen && !strcmp(chosen->graph_qualified_name, candidate->graph_qualified_name))
            continue;
        definitions++;
        if (declaring && definitions <= 64) {
            const char *file = c_proof_func_file(ctx, candidate, relative);
            if (file && c_proof_file_reaches(ctx, file, declaring)) {
                compiled_against = candidate;
                compiled_count++;
            }
        }
        chosen = candidate;
    }
    /* `int llvm::f(int x) { ... }` is written with its namespace in the declarator. */
    if (!chosen && namespace_len && !c_linkage) {
        const char *qualified = NULL;
        size_t relative_len = strlen(relative);
        for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
             link = link->next) {
            const CBMCDecl *decl = link->decl;
            size_t target_len = decl->target ? strlen(decl->target) : 0;
            if (decl->kind != CBM_C_DECL_METHOD || strcmp(decl->name, name) ||
                target_len <= relative_len || decl->target[target_len - relative_len - 1] != '.' ||
                strcmp(decl->target + target_len - relative_len, relative))
                continue;
            if (arity >= 0 && decl->arity >= 0 && decl->arity != arity)
                continue;
            if (qualified && strcmp(qualified, decl->target))
                return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                              .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                              .candidate_count = 2,
                                              .reason = "c_external_definitions_conflict"};
            qualified = decl->target;
        }
        if (qualified)
            return c_proof_declaration_lookup(ctx, relative, (TSNode){0}, qualified,
                                              "qualified-namespace-definition", false);
    }
    if (definitions > 1) {
        if (compiled_count != 1)
            return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                          .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                          .candidate_count = 2,
                                          .reason = "c_external_definitions_conflict"};
        chosen = compiled_against;
    }
    return chosen ? c_proof_query(ctx, chosen->qualified_name, CBM_RESOLVE_RULE_SCOPE_BINDING)
                  : (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                           .reason = "c_external_definition_missing"};
}

static cbm_resolve_result_t c_proof_visible_prototype(CLSPContext *ctx, const char *local,
                                                      const char *relative, uint32_t at) {
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    int lo = 0, hi = ctx->proof_declaration_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (strcmp(ctx->proof_declarations[mid].qn, local) < 0)
            lo = mid + 1;
        else
            hi = mid;
    }
    const CBMType *signature = NULL;
    bool linkage = false;
    TSNode site = {0};
    for (int i = lo;
         i < ctx->proof_declaration_count && !strcmp(ctx->proof_declarations[i].qn, local); i++) {
        const CProofDeclaration *decl = &ctx->proof_declarations[i];
        if (decl->at >= at || ts_node_is_null(decl->site))
            continue;
        if (!decl->external || decl->no_prototype ||
            (!ts_node_is_null(site) && ((signature && decl->signature &&
                                         !c_proof_signature_equal(signature, decl->signature)) ||
                                        linkage != decl->c_linkage)))
            return c_proof_missing("c_prototype_linkage_unsupported");
        if (decl->signature)
            signature = decl->signature;
        linkage = decl->c_linkage;
        site = decl->site;
    }
    if (ts_node_is_null(site))
        return result;
    result = c_proof_external_definition(ctx, relative, signature, -1, linkage,
                                         ctx->plain_header && !linkage, c_own_file_key(ctx));
    return cbm_resolve_admit(&result)
               ? cbm_lsp_proof_rule(
                     &ctx->field_types, result, site, "c-external-prototype", relative,
                     "visible-source-prototype;compatible-signature;unique-external-definition",
                     NULL)
               : result;
}

static bool c_proof_domain_included(CLSPContext *ctx, const char *relative, uint32_t at,
                                    bool from_includer, cbm_resolve_result_t *merged);

/* A file module is a storage identity, not a C namespace. Includes expose
 * their declarations in the translation unit; collect every mapped module
 * rather than preferring the first header or the current file. */
static cbm_resolve_result_t c_proof_domain(CLSPContext *ctx, const char *relative, uint32_t at) {
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                   .reason = "c_visible_declaration_missing"};
    const char *local = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, relative);
    if (!local)
        return c_proof_missing("c_binding_allocation_failed");
    if (c_proof_declared(ctx, local, at)) {
        const CBMRegisteredFunc *registered = cbm_registry_lookup_func(ctx->proof_registry, local);
        bool prototype = registered && (registered->source_flags & CBM_SOURCE_DECL_TOP_LEVEL) &&
                         !(registered->source_flags & CBM_SOURCE_FUNCTION_DEFINITION);
        cbm_resolve_result_t local_result =
            prototype ? c_proof_visible_prototype(ctx, local, relative, at)
                      : c_proof_query(ctx, local, CBM_RESOLVE_RULE_SCOPE_BINDING);
        if (!prototype && local_result.binding == CBM_RESOLVE_BINDING_UNRESOLVED &&
            local_result.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE)
            local_result = c_proof_visible_prototype(ctx, local, relative, at);
        if (!c_proof_merge(&result, local_result))
            return result;
    } else {
        /* A use before this file's own top-level definition compiled against an
         * earlier declaration, written in some header if not here. File scope has
         * one meaning for an ordinary name, so that declaration is of this function. */
        const CBMRegisteredFunc *registered = cbm_registry_lookup_func(ctx->proof_registry, local);
        uint32_t own = CBM_SOURCE_DECL_TOP_LEVEL | CBM_SOURCE_FUNCTION_DEFINITION;
        if (registered && (registered->source_flags & own) == own &&
            !c_proof_merge(&result, c_proof_query(ctx, local, CBM_RESOLVE_RULE_SCOPE_BINDING)))
            return result;
    }
    for (int i = 0; i < ctx->include_count; i++) {
        const CProofInclude *include = &ctx->proof_includes[i];
        if (include->at >= at)
            continue;
        if ((i && !strcmp(include->path, ctx->proof_includes[i - 1].path) &&
             strcmp(include->module, ctx->proof_includes[i - 1].module)) ||
            (i + 1 < ctx->include_count &&
             !strcmp(include->path, ctx->proof_includes[i + 1].path) &&
             strcmp(include->module, ctx->proof_includes[i + 1].module)))
            return c_proof_missing("c_include_mapping_conflict");
    }
    if (!c_proof_domain_included(ctx, relative, at, false, &result))
        return result;
    if (result.binding == CBM_RESOLVE_BINDING_UNRESOLVED)
        (void)c_proof_domain_included(ctx, relative, at, true, &result);
    return result;
}

/* Returns false when the lookup has settled on an answer that is final, a conflict among them. */
static bool c_proof_domain_included(CLSPContext *ctx, const char *relative, uint32_t at,
                                    bool from_includer, cbm_resolve_result_t *merged) {
    cbm_resolve_result_t result = *merged;
    /* What an included file defines: one candidate per project function of this
     * name, kept when its file is in this translation unit. */
    const char *leaf = strrchr(relative, '.');
    leaf = leaf ? leaf + 1 : relative;
    size_t relative_len = strlen(relative);
    CBMFreeFuncIter iterator;
    cbm_registry_free_funcs_by_short_name_chain(ctx->proof_registry, leaf, &iterator);
    int index;
    while ((index = cbm_free_func_iter_next(&iterator)) >= 0) {
        const CBMRegisteredFunc *candidate = &iterator.reg->funcs[index];
        const char *qn = candidate->qualified_name;
        size_t qn_len = qn ? strlen(qn) : 0;
        if (qn_len <= relative_len + 1 || qn[qn_len - relative_len - 1] != '.' ||
            strcmp(qn + qn_len - relative_len, relative))
            continue;
        const CTUModule *entry =
            c_tu_for_declaration(ctx, candidate->source_file_qn, qn, qn_len - relative_len - 1);
        if (!entry || entry->at >= at || entry->from_includer != from_includer)
            continue;
        if (entry->exports_only && (!(candidate->source_flags & CBM_SOURCE_FUNCTION_DEFINITION) ||
                                    (candidate->source_flags & CBM_SOURCE_DECL_STATIC)))
            continue;
        cbm_resolve_result_t imported = c_proof_query(ctx, qn, CBM_RESOLVE_RULE_IMPORT_BINDING);
        imported = c_proof_import_join(ctx, imported, entry->via, CBM_PROOF_IMPORT_BINDING);
        if (!c_proof_merge(&result, imported)) {
            *merged = result;
            return false;
        }
    }
    /* What an included file only declares. The call is to the external name, so
     * the edge goes to the definition with that name, wherever it is compiled. */
    const CBMCDecl *declared = NULL;
    const CTUModule *declared_via = NULL;
    const CBMType *signature = NULL;
    int arity = -1;
    bool c_linkage = !ctx->cpp_mode;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, relative); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind != CBM_C_DECL_PROTOTYPE || !(decl->flags & CBM_C_DECL_EXTERNAL) ||
            strcmp(decl->name, relative))
            continue;
        const CTUModule *entry = c_tu_find(ctx, decl->file, strlen(decl->file));
        if (!entry || entry->exports_only || entry->at >= at ||
            entry->from_includer != from_includer)
            continue;
        if (signature && decl->signature && !c_proof_signature_equal(signature, decl->signature)) {
            *merged = c_proof_missing("c_prototype_linkage_unsupported");
            return false;
        }
        if (decl->signature)
            signature = decl->signature;
        if (decl->arity >= 0)
            arity = decl->arity;
        if (decl->flags & CBM_C_DECL_C_LINKAGE)
            c_linkage = true;
        declared = decl;
        declared_via = entry;
    }
    if (declared) {
        cbm_resolve_result_t external =
            c_proof_external_definition(ctx, relative, signature, arity, c_linkage,
                                        ctx->plain_header && !c_linkage, declared->file);
        if (cbm_resolve_admit(&external)) {
            external =
                c_proof_import_join(ctx, external, declared_via->via, CBM_PROOF_IMPORT_BINDING);
            if (cbm_resolve_admit(&external)) {
                TSNode none = {0};
                external = cbm_lsp_proof_rule(
                    &ctx->field_types, external, none, "c-header-prototype", relative,
                    "visible-header-prototype;compatible-signature;unique-external-definition",
                    NULL);
            }
        }
        if (!c_proof_merge(&result, external)) {
            *merged = result;
            return false;
        }
    }
    *merged = result;
    return true;
}

/* The definitions of a macro this translation unit can see, in project order. A common name is
 * defined by many files of a project, so the bucket is read once per file and name. */
typedef struct {
    const CBMCDecl **items;
    uint32_t *from; /* first byte of this file where the definition is in force */
    int count;
} CMacroDefs;

/* `#if defined(REDISMODULE_CORE) / #define RedisModuleString robj / #endif`: server.h defines the
 * switch and then includes this header, and a module that includes only this header leaves it off.
 * A switch no file of the project defines may come from the build, and says nothing. */
static bool c_macro_branch_is_off(CLSPContext *ctx, const CBMCDecl *decl) {
    if (!decl->needs_macro)
        return false;
    const char *own = c_own_file_key(ctx);
    bool defined_in_project = false;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, decl->needs_macro); link;
         link = link->next) {
        const CBMCDecl *other = link->decl;
        if (other->kind != CBM_C_DECL_MACRO || strcmp(other->name, decl->needs_macro))
            continue;
        defined_in_project = true;
        if (!strcmp(other->file, own))
            return false;
        const CTUModule *entry = c_tu_find(ctx, other->file, strlen(other->file));
        if (entry && !entry->exports_only)
            return false;
    }
    return defined_in_project;
}

static CMacroDefs *c_macro_defs(CLSPContext *ctx, const char *name) {
    static CMacroDefs none = {0};
    const CBMCDeclLink *first = cbm_c_decl_lookup(ctx->c_decls, name);
    if (!first)
        return &none;
    if (ctx->macro_defs_ready && !ctx->macro_defs)
        ctx->macro_defs = cbm_ht_create(32);
    CMacroDefs *known =
        ctx->macro_defs_ready && ctx->macro_defs ? cbm_ht_get(ctx->macro_defs, name) : NULL;
    if (known)
        return known;
    int count = 0;
    for (const CBMCDeclLink *link = first; link; link = link->next)
        count += link->decl->kind == CBM_C_DECL_MACRO && !strcmp(link->decl->name, name);
    CMacroDefs *defs = cbm_arena_calloc(ctx->arena, sizeof(*defs));
    if (!defs)
        return &none;
    defs->items = count ? cbm_arena_alloc(ctx->arena, (size_t)count * sizeof(*defs->items)) : NULL;
    defs->from = count ? cbm_arena_alloc(ctx->arena, (size_t)count * sizeof(*defs->from)) : NULL;
    if (count && (!defs->items || !defs->from))
        return &none;
    const char *own = c_own_file_key(ctx);
    for (const CBMCDeclLink *link = first; link; link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind != CBM_C_DECL_MACRO || strcmp(decl->name, name) ||
            c_macro_branch_is_off(ctx, decl))
            continue;
        if (!strcmp(decl->file, own)) {
            defs->items[defs->count] = decl;
            defs->from[defs->count++] = 0;
            continue;
        }
        const CTUModule *entry = c_tu_find(ctx, decl->file, strlen(decl->file));
        if (entry && !entry->exports_only) {
            defs->items[defs->count] = decl;
            defs->from[defs->count++] = entry->at;
        }
    }
    if (ctx->macro_defs_ready && ctx->macro_defs) {
        const char *key = cbm_arena_strdup(ctx->arena, name);
        if (key)
            cbm_ht_set(ctx->macro_defs, key, defs);
    }
    return defs;
}

/* A macro of this name may rewrite any use, and which definition wins depends on
 * the configuration. No function of the name is proven at such a use. */
static bool c_proof_macro_visible(CLSPContext *ctx, const char *name, uint32_t at) {
    const char *key = cbm_arena_sprintf(ctx->arena, "$macro:%s", name);
    if (!key || c_proof_declared(ctx, key, at))
        return true;
    const CMacroDefs *defs = c_macro_defs(ctx, name);
    const char *own = c_own_file_key(ctx);
    for (int i = 0; i < defs->count; i++)
        if (defs->from[i] < at && strcmp(defs->items[i]->file, own))
            return true;
    return false;
}

/* `#define CALL() f()`, `#include "gen.h"` with nothing in the index behind it, then the use: that
 * text may `#undef` and redefine CALL, so the definition read here is not shown to be the one in
 * force. An unknown header before an unconditional definition changes nothing, since the later
 * definition wins. Before a conditional one it may have decided the branch. Inside an included file
 * the order is not known here, so an unknown header anywhere under the same include counts. `<...>`
 * is the toolchain, which defines its own names. */
static bool c_macro_definition_unproven(CLSPContext *ctx, const CBMCDecl *decl, uint32_t at) {
    if (!ctx->proof_unknown_include_count)
        return false;
    uint32_t from = 0; /* the first offset where an unknown header could still matter */
    if (!decl->branch_group) {
        if (!strcmp(decl->file, c_own_file_key(ctx))) {
            const char *key = cbm_arena_sprintf(ctx->arena, "$macro:%s", decl->name);
            int low = 0, high = ctx->proof_declaration_count;
            while (key && low < high) {
                int mid = low + (high - low) / 2;
                if (strcmp(ctx->proof_declarations[mid].qn, key) < 0)
                    low = mid + 1;
                else
                    high = mid;
            }
            for (; key && low < ctx->proof_declaration_count &&
                   !strcmp(ctx->proof_declarations[low].qn, key) &&
                   ctx->proof_declarations[low].at < at;
                 low++)
                from = ctx->proof_declarations[low].at + 1;
        } else {
            const CTUModule *entry = c_tu_find(ctx, decl->file, strlen(decl->file));
            from = entry ? entry->at : 0;
        }
    }
    int low = 0, high = ctx->proof_unknown_include_count;
    while (low < high) {
        int mid = low + (high - low) / 2;
        if (ctx->proof_unknown_includes[mid] < from)
            low = mid + 1;
        else
            high = mid;
    }
    return low < ctx->proof_unknown_include_count && ctx->proof_unknown_includes[low] < at;
}

static bool c_proof_using_visible(CLSPContext *ctx, const CProofUsing *using, uint32_t at) {
    if (using->at >= at)
        return false;
    bool scope_visible = false;
    for (const CBMScope *scope = ctx->current_scope; scope; scope = scope->parent)
        if (scope == using->scope)
            scope_visible = true;
    const char *owner = ctx->current_namespace ? ctx->current_namespace : ctx->module_qn;
    size_t len = strlen(using->owner);
    return scope_visible && !strncmp(owner, using->owner, len) &&
           (owner[len] == '\0' || owner[len] == '.');
}

/* A template parameter of this template or of one around it. It stands for whatever the
 * instantiation supplies, so a declaration of that name elsewhere is not what it means. */
static bool c_is_template_parameter(const CLSPContext *ctx, const char *name) {
    for (int i = 0; ctx->in_template && i < ctx->outer_template_param_count; i++)
        if (ctx->outer_template_params[i] && !strcmp(ctx->outer_template_params[i], name))
            return true;
    return false;
}

/* Inside a template a non-dependent name binds where it is written, like any other name: that is
 * the two-phase rule. Only its own parameters, and what an instantiation's argument types add to
 * an unqualified call, are not known here. */
static cbm_resolve_result_t c_proof_name_lookup(CLSPContext *ctx, const char *text, uint32_t at) {
    const char *name = c_proof_name(ctx, text);
    if (!name || ctx->binding_failed || c_is_template_parameter(ctx, name) ||
        ((ctx->enclosing_class_qn || ctx->proof_method_class) && !ctx->proof_class_scope_checked))
        return c_proof_missing("c_name_domain_unsupported");
    if (strchr(name, '.')) {
        if (ctx->current_namespace && !(text[0] == ':' && text[1] == ':'))
            return c_proof_missing("c_relative_namespace_binding_unknown");
        /* Namespace aliases and lexical values can change the first segment. */
        const char *dot = strchr(name, '.');
        char *first = cbm_arena_strndup(ctx->arena, name, (size_t)(dot - name));
        if (!first || cbm_scope_contains(ctx->current_scope, first))
            return c_proof_missing("c_qualified_name_shadowed");
        for (int i = 0; i < ctx->ns_alias_count; i++)
            if (!strcmp(ctx->ns_alias_names[i], first))
                return c_proof_missing("c_namespace_alias_unproven");
        return c_proof_domain(ctx, name, at);
    }
    if (cbm_scope_contains(ctx->current_scope, name))
        return c_proof_missing("c_name_shadowed");
    if (c_proof_macro_visible(ctx, name, at))
        return c_proof_missing("c_macro_may_rewrite_name");
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    const char *relative =
        ctx->current_namespace &&
                !strncmp(ctx->current_namespace, ctx->module_qn, ctx->module_qn_len) &&
                ctx->current_namespace[ctx->module_qn_len] == '.'
            ? ctx->current_namespace + ctx->module_qn_len + 1
            : "";
    for (;;) {
        const char *qualified =
            relative[0] ? cbm_arena_sprintf(ctx->arena, "%s.%s", relative, name) : name;
        result = c_proof_domain(ctx, qualified, at);
        if (result.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
            result.binding != CBM_RESOLVE_BINDING_UNRESOLVED || !relative[0])
            break;
        const char *dot = strrchr(relative, '.');
        relative = dot ? cbm_arena_strndup(ctx->arena, relative, (size_t)(dot - relative)) : "";
        if (!relative)
            return c_proof_missing("c_binding_allocation_failed");
    }
    if (result.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
        result.binding == CBM_RESOLVE_BINDING_AMBIGUOUS)
        return result;
    for (int i = 0; i < ctx->proof_using_count; i++) {
        const CProofUsing *using = &ctx->proof_usings[i];
        if (!c_proof_using_visible(ctx, using, at))
            continue;
        if (!using->supported)
            return c_proof_missing("c_using_binding_unsupported");
        const char *leaf = strrchr(using->target, '.');
        leaf = leaf ? leaf + 1 : using->target;
        if (!using->namespace && strcmp(leaf, name))
            continue;
        const char *qualified = using->namespace
                                    ? cbm_arena_sprintf(ctx->arena, "%s.%s", using->target, name)
                                    : using->target;
        cbm_resolve_result_t used = c_proof_domain(ctx, qualified, at);
        /* `using std::swap;` beside a project swap: overload resolution has a candidate here
         * this index has no declaration of, and it may well be the one chosen. */
        if (!using->namespace && used.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
            used.binding == CBM_RESOLVE_BINDING_UNRESOLVED && cbm_resolve_admit(&result))
            return c_proof_missing("c_using_of_an_unknown_declaration");
        if (!c_proof_merge(&result, used))
            break;
    }
    return result;
}

/* The written name without its template arguments: `cast<X>` is cast, `llvm::cast<X>` is
 * llvm::cast. Every overload of one name in one file is one graph node, so whichever overload
 * deduction would pick, the edge is the same. */
static char *c_template_id_name(CLSPContext *ctx, TSNode name_node) {
    char *text = c_node_text(ctx, name_node);
    TSNode leaf = name_node;
    for (int hop = 0; hop < 16 && !ts_node_is_null(leaf); hop++) {
        const char *kind = ts_node_type(leaf);
        if (!strcmp(kind, "template_function")) {
            TSNode arguments = ts_node_child_by_field_name(leaf, "arguments", 9);
            if (!text || ts_node_is_null(arguments))
                return NULL;
            return cbm_arena_strndup(ctx->arena, text,
                                     ts_node_start_byte(arguments) - ts_node_start_byte(name_node));
        }
        if (strcmp(kind, "qualified_identifier") && strcmp(kind, "scoped_identifier"))
            break;
        leaf = ts_node_child_by_field_name(leaf, "name", 4);
    }
    return text;
}

static cbm_resolve_result_t c_callable_proof(CLSPContext *ctx, TSNode expr) {
    if (!ctx || ts_node_is_null(expr) || ctx->binding_failed || ts_node_has_error(expr) ||
        ts_node_end_byte(expr) > ctx->proof_syntax_limit)
        return c_proof_missing("c_callable_expression_unknown");
    expr = c_fp_unwrap_parentheses(ctx, expr);
    if (ts_node_is_null(expr) || ctx->binding_failed)
        return c_proof_missing("c_callable_parentheses_unproven");
    const char *kind = ts_node_type(expr);
    bool address = !strcmp(kind, "pointer_expression") || !strcmp(kind, "unary_expression");
    TSNode name_node = expr;
    if (address) {
        TSNode operand =
            c_fp_unwrap_parentheses(ctx, ts_node_child_by_field_name(expr, "argument", 8));
        TSNode operator= ts_node_child_by_field_name(expr, "operator", 8);
        const char *op = ts_node_is_null(operator) ? NULL : c_node_text(ctx, operator);
        if (!op || strcmp(op, "&") || ts_node_is_null(operand))
            return c_proof_missing("c_callable_operator_unsupported");
        name_node = operand;
        kind = ts_node_type(operand);
    }
    if (strcmp(kind, "identifier") && strcmp(kind, "qualified_identifier") &&
        strcmp(kind, "scoped_identifier") && strcmp(kind, "template_function"))
        return c_proof_missing("c_callable_expression_unsupported");
    char *name = c_template_id_name(ctx, name_node);
    if (!name)
        return c_proof_missing("c_callable_name_missing");
    int fp = !strcmp(kind, "identifier") ? c_find_fp_binding_index(ctx, name) : -1;
    if (fp >= 0) {
        if (ctx->control_flow_depth || ctx->callable_flow_depth)
            return c_proof_missing("c_callable_flow_join_unknown");
        if (!ctx->fp_binding_scopes[fp] || !ctx->fp_binding_scopes[fp]->parent)
            return c_proof_missing("c_global_callable_mutability_unknown");
        if (ctx->fp_storage_escaped[fp])
            return c_proof_missing("c_callable_storage_escaped");
        if (address || !ctx->fp_target_proven[fp] || !ctx->fp_target_qns[fp])
            return c_proof_missing("c_callable_binding_unknown");
        const cbm_resolve_proof_t *origin = ctx->fp_target_proofs[fp];
        const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(origin);
        if (!view || !view->predecessors_closed || !cbm_resolve_admit(&view->outcome) ||
            strcmp(view->outcome.target, ctx->fp_target_qns[fp]))
            return c_proof_missing("c_callable_binding_origin_missing");
        cbm_resolve_result_t proof = view->outcome;
        proof.proof = origin;
        proof.rule = CBM_RESOLVE_RULE_CALLABLE_BINDING;
        proof.facts |= CBM_RESOLVE_FACT_CALLABLE_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT;
        return cbm_lsp_proof_rule(
            &ctx->field_types, proof, name_node, "c-lexical-function-pointer", name,
            "nearest-local-binding;tracked-exact-target;no-unsupported-flow", NULL);
    }
    return c_proof_name_lookup(ctx, name, ts_node_start_byte(expr));
}

static const cbm_resolve_proof_t *c_exact_function_binding_from_expr(CLSPContext *ctx, TSNode expr,
                                                                     TSNode binding,
                                                                     const char *name) {
    cbm_resolve_result_t proof = c_callable_proof(ctx, expr);
    if (!cbm_resolve_admit(&proof) || !proof.proof)
        return NULL;
    proof =
        cbm_lsp_proof_rule(&ctx->field_types, proof, binding, "c-function-pointer-binding-version",
                           name, "exact-source-callable-assignment", NULL);
    const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(proof.proof);
    return cbm_resolve_admit(&proof) && view && view->predecessors_closed ? proof.proof : NULL;
}

/* Unqualified C++ calls can add ADL candidates. Until associated namespaces
 * are enumerated, only arguments with no associated user-defined type are
 * complete. Qualified calls and lexical function pointers do not use ADL. */
static bool c_proof_builtin_argument(CLSPContext *ctx, TSNode argument);
static const CBMRegisteredType *c_proof_expression_owner(CLSPContext *ctx, TSNode node, int depth,
                                                         const cbm_resolve_proof_t **proof,
                                                         const CBMType **shape);

/* Does this unit declare another function of this written name? Argument-dependent
 * lookup adds only declarations the unit can see, so without one it adds nothing. */
static bool c_proof_other_visible_function(CLSPContext *ctx, const char *name, const char *target,
                                           uint32_t at) {
    size_t name_len = strlen(name);
    CBMFreeFuncIter iterator;
    cbm_registry_free_funcs_by_short_name_chain(ctx->proof_registry, name, &iterator);
    int index;
    while ((index = cbm_free_func_iter_next(&iterator)) >= 0) {
        const CBMRegisteredFunc *candidate = &iterator.reg->funcs[index];
        const char *qn = candidate->qualified_name;
        size_t qn_len = qn ? strlen(qn) : 0;
        if (!(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) || qn_len <= name_len ||
            !strcmp(qn, target))
            continue;
        const CTUModule *entry =
            c_tu_for_declaration(ctx, candidate->source_file_qn, qn, qn_len - name_len - 1);
        if ((entry && entry->at < at) ||
            c_is_own_declaration(ctx, candidate->source_file_qn, qn, qn_len - name_len - 1))
            return true;
    }
    size_t target_len = strlen(target);
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind != CBM_C_DECL_PROTOTYPE)
            continue;
        const char *leaf = strrchr(decl->name, '.');
        if (strcmp(leaf ? leaf + 1 : decl->name, name))
            continue;
        /* The same function, as `<module>.<name>` of its definition. */
        size_t relative_len = strlen(decl->name);
        if (target_len > relative_len && target[target_len - relative_len - 1] == '.' &&
            !strcmp(target + target_len - relative_len, decl->name))
            continue;
        const CTUModule *entry = c_tu_find(ctx, decl->file, strlen(decl->file));
        if (entry && !entry->exports_only && entry->at < at)
            return true;
    }
    return false;
}

static bool c_proof_call_domain(CLSPContext *ctx, TSNode call, TSNode function,
                                const cbm_resolve_result_t *proof) {
    const char *function_kind = ts_node_type(function);
    if (!ctx->cpp_mode || ctx->plain_header || proof->rule == CBM_RESOLVE_RULE_CALLABLE_BINDING ||
        (strcmp(function_kind, "identifier") && strcmp(function_kind, "template_function")))
        return true;
    TSNode args = ts_node_child_by_field_name(call, "arguments", 9);
    uint32_t count = ts_node_named_child_count(args);
    bool class_argument = false;
    for (uint32_t i = 0; i < count; i++) {
        TSNode argument = ts_node_named_child(args, i);
        const char *kind = ts_node_type(argument);
        if (c_proof_builtin_argument(ctx, argument))
            continue;
        if (!strcmp(kind, "comment") || !strcmp(kind, "number_literal") ||
            !strcmp(kind, "char_literal") || !strcmp(kind, "string_literal") ||
            !strcmp(kind, "concatenated_string") || !strcmp(kind, "true") ||
            !strcmp(kind, "false") || !strcmp(kind, "null"))
            continue;
        /* In a template an argument can be dependent, and then an instantiation's namespaces
         * add candidates this file cannot see. */
        if (ctx->in_template)
            return false;
        const cbm_resolve_proof_t *argument_proof = NULL;
        const CBMType *shape = NULL;
        if (!c_proof_expression_owner(ctx, argument, 0, &argument_proof, &shape))
            return false;
        class_argument = true;
    }
    const char *name = c_template_id_name(ctx, function);
    return !class_argument ||
           (name && proof->target &&
            !c_proof_other_visible_function(ctx, name, proof->target, ts_node_start_byte(call)));
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

/* Resolve the class of a qualified member `Scope::member` -- scope_qn is the
 * dotted scope ("SecdManager", "ns.Widget") -- to the QN of ONE registered
 * class, or NULL. The bare and caller-module spellings come first. A class
 * declared in another file is registered under that file's module
 * ("<proj>.app.src.SecdManager.SecdManager"), which neither spelling reaches,
 * so then every registered type whose QN ends in ".<scope_qn>" at a segment
 * boundary is a candidate. More than one distinct candidate (two libraries
 * each declaring `Twin`) names no single class: the call stays unresolved
 * rather than bind one of them by guess. */
static const char *c_resolve_scope_class_qn(CLSPContext *ctx, const char *scope_qn) {
    if (!ctx->registry || !scope_qn || !scope_qn[0]) {
        return NULL;
    }
    if (cbm_registry_lookup_type(ctx->registry, scope_qn)) {
        return scope_qn;
    }
    if (ctx->module_qn) {
        const char *mod_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, scope_qn);
        if (cbm_registry_lookup_type(ctx->registry, mod_qn)) {
            return mod_qn;
        }
    }
    const char *last_dot = strrchr(scope_qn, '.');
    const char *short_name = last_dot ? last_dot + 1 : scope_qn;
    size_t scope_len = strlen(scope_qn);
    const char *found = NULL;
    CBMTypeShortIter it;
    cbm_registry_types_by_short_name_chain(ctx->registry, short_name, &it);
    int i;
    while ((i = cbm_type_short_iter_next(&it)) >= 0) {
        const char *q = it.reg->types[i].qualified_name;
        size_t qlen = q ? strlen(q) : 0;
        if (qlen <= scope_len || q[qlen - scope_len - 1] != '.' ||
            strcmp(q + qlen - scope_len, scope_qn) != 0) {
            continue;
        }
        if (found && strcmp(found, q) != 0) {
            return NULL;
        }
        found = q;
    }
    return found;
}

static const CBMRegisteredFunc *c_lookup_scope_member(CLSPContext *ctx, const char *class_qn,
                                                      const char *member_name);

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
                          (uninitialized tail bound T to stack garbage -> corrupt type
                          graph, bitcoin serialize.h) */
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
            // Try with current namespace prefix (e.g., inside namespace std,
            // "shared_ptr" → "std.shared_ptr")
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
            NULL}; /* zero-fill: cbm_type_substitute requires NULL-terminated args
                      (uninitialized tail bound T to stack garbage -> corrupt type
                      graph, bitcoin serialize.h) */
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

    // struct_specifier / class_specifier / union_specifier (inline type)
    if (strcmp(kind, "struct_specifier") == 0 || strcmp(kind, "class_specifier") == 0 ||
        strcmp(kind, "union_specifier") == 0) {
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

/* Stack guard: the evaluator recurses once per expression NESTING level (a
 * call argument, a lambda body, an operand outside a chain). The links of a
 * left-associative chain are not nesting and do not count against it; see
 * c_eval_chain_operands.
 * There is deliberately NO per-file work budget (#1527): the 10000-step cap
 * that used to sit here (#323) left every later expression in a large file
 * unresolved while the file still reported status `indexed`. What it was
 * containing -- a fluent chain re-evaluating its receiver at every link, 2^N
 * evaluations (the Bitcoin versionbits hang) -- is removed at the root by the
 * per-evaluation memo below. */
#define C_EVAL_DEPTH_LIMIT 256
enum { C_EVAL_MEMO_INITIAL_CAP = 64, C_EVAL_CHAIN_INITIAL_CAP = 64 };

static uint32_t c_eval_memo_hash(const void *id) {
    uintptr_t v = (uintptr_t)id >> 3; /* node structs are >=8-byte aligned */
    return (uint32_t)((v ^ (v >> 29)) * 2654435761u);
}

/* Empty the memo for a new outermost evaluation: O(slots the last one used). */
static void c_eval_memo_reset(CLSPContext *ctx) {
    for (int i = 0; i < ctx->eval_memo_count; i++) {
        ctx->eval_memo[ctx->eval_memo_used[i]].node_id = NULL;
    }
    ctx->eval_memo_count = 0;
}

static const CBMType *c_eval_memo_lookup(const CLSPContext *ctx, const void *id) {
    if (ctx->eval_memo_cap == 0) {
        return NULL;
    }
    uint32_t mask = (uint32_t)ctx->eval_memo_cap - 1;
    for (uint32_t i = c_eval_memo_hash(id) & mask;; i = (i + 1) & mask) {
        const struct CLSPEvalMemoSlot *e = &ctx->eval_memo[i];
        if (!e->node_id) {
            return NULL;
        }
        if (e->node_id == id) {
            return e->result;
        }
    }
}

static void c_eval_memo_place(CLSPContext *ctx, const void *id, const CBMType *result) {
    uint32_t mask = (uint32_t)ctx->eval_memo_cap - 1;
    uint32_t i = c_eval_memo_hash(id) & mask;
    while (ctx->eval_memo[i].node_id) {
        i = (i + 1) & mask;
    }
    ctx->eval_memo[i].node_id = id;
    ctx->eval_memo[i].result = result;
    ctx->eval_memo_used[ctx->eval_memo_count++] = (int)i;
}

static void *c_eval_memo_alloc(CLSPContext *ctx, size_t size) {
#ifdef CBM_ENABLE_TEST_SEAMS
    if (ctx->test_memo_allocations_left == 0)
        return NULL;
    if (ctx->test_memo_allocations_left > 0)
        ctx->test_memo_allocations_left--;
#endif
    return cbm_arena_alloc(ctx->arena, size);
}

/* Memoization is required to bound receiver re-evaluation. An allocation
 * failure stops this file's refinement and is reported to the caller. */
static bool c_eval_memo_insert(CLSPContext *ctx, const void *id, const CBMType *result) {
    if (ctx->eval_memo_count >= ctx->eval_memo_cap / 2) {
        if (ctx->eval_memo_cap > INT_MAX / 2)
            return false;
        int new_cap = ctx->eval_memo_cap ? ctx->eval_memo_cap * 2 : C_EVAL_MEMO_INITIAL_CAP;
        if ((size_t)new_cap > SIZE_MAX / sizeof(struct CLSPEvalMemoSlot) ||
            (size_t)(new_cap / 2) > SIZE_MAX / sizeof(int))
            return false;
        struct CLSPEvalMemoSlot *slots = c_eval_memo_alloc(ctx, (size_t)new_cap * sizeof(*slots));
        if (!slots)
            return false;
        int *used = c_eval_memo_alloc(ctx, (size_t)(new_cap / 2) * sizeof(int));
        if (!used)
            return false;
        memset(slots, 0, (size_t)new_cap * sizeof(*slots));
        struct CLSPEvalMemoSlot *old = ctx->eval_memo;
        const int *old_used = ctx->eval_memo_used;
        int old_count = ctx->eval_memo_count;
        ctx->eval_memo = slots;
        ctx->eval_memo_used = used;
        ctx->eval_memo_cap = new_cap;
        ctx->eval_memo_count = 0;
        for (int k = 0; k < old_count; k++) {
            const struct CLSPEvalMemoSlot *e = &old[old_used[k]];
            c_eval_memo_place(ctx, e->node_id, e->result);
        }
    }
    c_eval_memo_place(ctx, id, result);
    return true;
}

/* --- Left-associative chains are evaluated iteratively ------------------
 * A chain's length is not nesting. `a + b + ... + z`, `x.a().b()...`,
 * `p->f->g`, `m[i][j]`, consequence-nested `c ? (d ? ..) : e`, runs of
 * parentheses and comma lists are left-deep trees in which every link first
 * evaluates ONE operand and derives its own type from that operand's type.
 * Recursing through them spent native stack and a C_EVAL_DEPTH_LIMIT unit per
 * link, so a 300-term sum exceeded the guard and rolled back the whole file.
 *
 * c_eval_expr_type therefore first walks down the chain operands below the
 * node, then evaluates them innermost first, each at the caller's depth. When
 * a link is evaluated, its chain operand is already memoized, so the
 * evaluator's own recursive call into it is a memo hit one frame deep. The
 * types are exactly the recursive ones: every node is still evaluated by
 * c_eval_expr_type_inner, evaluation never mutates scope or context within
 * one outermost call (the memo relies on the same fact), and only an operand
 * the inner evaluator evaluates first and unconditionally is pre-evaluated,
 * so no node is evaluated that the recursion would not have evaluated.
 * Operands in any other position (call arguments, lambda bodies, std::move
 * operands, unary operands, ...) are genuine nesting and still count depth. */

/* A binary operator whose result is bool: c_eval_expr_type_inner returns
 * bool for it WITHOUT evaluating the left operand. Mirrors its operator test
 * (the first anonymous child's source text). */
static bool c_binary_yields_bool(const CLSPContext *ctx, TSNode node) {
    static const char *const bool_ops[] = {"==", "!=", "<", ">", "<=", ">=", "&&", "||"};
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_child(node, i);
        if (ts_node_is_named(child)) {
            continue;
        }
        uint32_t start = ts_node_start_byte(child);
        uint32_t end = ts_node_end_byte(child);
        if (end > (uint32_t)ctx->source_len) {
            return true; /* unreadable operator: do not pre-evaluate */
        }
        size_t len = end > start ? (size_t)(end - start) : 0;
        for (size_t k = 0; k < sizeof(bool_ops) / sizeof(bool_ops[0]); k++) {
            if (strlen(bool_ops[k]) == len && memcmp(ctx->source + start, bool_ops[k], len) == 0) {
                return true;
            }
        }
        return false;
    }
    return false;
}

/* The chain operand of a link: the operand c_eval_expr_type_inner evaluates
 * first and unconditionally, whose type the link's type is derived from. A
 * null node when `node` is not a chain link. */
static TSNode c_chain_operand(const CLSPContext *ctx, TSNode node) {
    const TSNode none = {0};
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "field_expression") == 0) {
        if (ts_node_is_null(ts_node_child_by_field_name(node, "field", 5))) {
            return none;
        }
        return ts_node_child_by_field_name(node, "argument", 8);
    }
    if (strcmp(kind, "call_expression") == 0) {
        TSNode fn = ts_node_child_by_field_name(node, "function", 8);
        if (ts_node_is_null(fn)) {
            return none;
        }
        /* A callee name is a leaf, and std::move/std::forward evaluate their
         * argument instead of the callee: neither continues a chain. */
        const char *fk = ts_node_type(fn);
        if (strcmp(fk, "identifier") == 0 || strcmp(fk, "qualified_identifier") == 0 ||
            strcmp(fk, "scoped_identifier") == 0 || strcmp(fk, "template_function") == 0) {
            return none;
        }
        return fn;
    }
    if (strcmp(kind, "subscript_expression") == 0) {
        return ts_node_child_by_field_name(node, "argument", 8);
    }
    if (strcmp(kind, "binary_expression") == 0) {
        return c_binary_yields_bool(ctx, node) ? none
                                               : ts_node_child_by_field_name(node, "left", 4);
    }
    if (strcmp(kind, "conditional_expression") == 0) {
        return ts_node_child_by_field_name(node, "consequence", 11);
    }
    if (strcmp(kind, "parenthesized_expression") == 0) {
        return ts_node_named_child_count(node) > 0 ? ts_node_named_child(node, 0) : none;
    }
    if (strcmp(kind, "comma_expression") == 0) {
        return ts_node_child_by_field_name(node, "right", 5);
    }
    return none;
}

/* Push a pending link. On allocation failure the caller stops collecting:
 * the rest of the chain is then evaluated recursively, under the depth
 * guard, exactly as before -- slower to fail, never a wrong type. */
static bool c_eval_chain_push(CLSPContext *ctx, TSNode link) {
    if (ctx->eval_chain_len == ctx->eval_chain_cap) {
        if (ctx->eval_chain_cap > INT_MAX / 2) {
            return false;
        }
        int new_cap = ctx->eval_chain_cap ? ctx->eval_chain_cap * 2 : C_EVAL_CHAIN_INITIAL_CAP;
        if ((size_t)new_cap > SIZE_MAX / sizeof(TSNode)) {
            return false;
        }
        TSNode *grown = cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(TSNode));
        if (!grown) {
            return false;
        }
        if (ctx->eval_chain_len > 0) {
            memcpy(grown, ctx->eval_chain, (size_t)ctx->eval_chain_len * sizeof(TSNode));
        }
        ctx->eval_chain = grown;
        ctx->eval_chain_cap = new_cap;
    }
    ctx->eval_chain[ctx->eval_chain_len++] = link;
    return true;
}

/* One real evaluation of a node that has no memo entry. */
static const CBMType *c_eval_expr_type_node(CLSPContext *ctx, TSNode node) {
    CBM_LSP_WORK(1); /* one real evaluation (complexity tests) */
    uint32_t trunc_before = ctx->eval_truncations;
    ctx->eval_depth++;
    const CBMType *result = c_eval_expr_type_inner(ctx, node);
    ctx->eval_depth--;
    if (!result) {
        result = cbm_type_unknown();
    }
    /* Never memoize a result a depth cutoff may have truncated: the incomplete
     * walk must stop before a second receiver branch repeats the truncated work.
     */
    if (!ctx->eval_failure && ctx->eval_truncations == trunc_before &&
        !c_eval_memo_insert(ctx, node.id, result)) {
        ctx->eval_failure = CBM_LSP_MEMO_FAILED;
    }
    return ctx->eval_failure ? cbm_type_unknown() : result;
}

/* Evaluate the not-yet-memoized chain operands below `node`, innermost first.
 * Nested evaluations push above the links still pending, so the stack is
 * shared; a link's index is re-read after each evaluation (it may grow). */
static void c_eval_chain_operands(CLSPContext *ctx, TSNode node) {
    int base = ctx->eval_chain_len;
    for (TSNode op = c_chain_operand(ctx, node); !ts_node_is_null(op);
         op = c_chain_operand(ctx, op)) {
        if (c_eval_memo_lookup(ctx, op.id) || !c_eval_chain_push(ctx, op)) {
            break;
        }
    }
    for (int i = ctx->eval_chain_len - 1; i >= base && !ctx->eval_failure; i--) {
        TSNode link = ctx->eval_chain[i];
        ctx->eval_chain_len = i;
        if (!c_eval_memo_lookup(ctx, link.id)) {
            (void)c_eval_expr_type_node(ctx, link);
        }
    }
    ctx->eval_chain_len = base;
}

const CBMType *c_eval_expr_type(CLSPContext *ctx, TSNode node) {
    if (ctx->eval_failure || ts_node_is_null(node))
        return cbm_type_unknown();
    if (ctx->eval_depth > C_EVAL_DEPTH_LIMIT) {
        ctx->eval_truncations++;
        ctx->eval_failure = CBM_LSP_DEPTH_EXCEEDED;
        return cbm_type_unknown();
    }
    if (ctx->eval_depth == 0) {
        c_eval_memo_reset(ctx); /* scope may have changed since the last evaluation */
    }
    const CBMType *cached = c_eval_memo_lookup(ctx, node.id);
    if (cached) {
        return cached;
    }
    c_eval_chain_operands(ctx, node);
    if (ctx->eval_failure) {
        return cbm_type_unknown();
    }
    return c_eval_expr_type_node(ctx, node);
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
            // Template field substitution: if field type is TYPE_PARAM (or NAMED
            // matching a template param name) and receiver is a TEMPLATE type,
            // substitute using receiver's template args. e.g., pair<int, Foo>.second
            // where second has type V → substitute V=Foo
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

    // --- qualified_identifier / scoped_identifier: ns::Class::method or
    // Enum::Value ---
    if (strcmp(kind, "qualified_identifier") == 0 || strcmp(kind, "scoped_identifier") == 0) {
        char *text = c_node_text(ctx, node);
        if (!text)
            return cbm_type_unknown();

        // Check if name child is template_function: ns::func<T> → strip template
        // args
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

        // Try as function: module-prefixed first (shadows stdlib stubs), then bare
        // QN
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
                    NULL}; /* zero-fill: cbm_type_substitute requires NULL-terminated
                              args (uninitialized tail bound T to stack garbage ->
                              corrupt type graph, bitcoin serialize.h) */
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
                return cbm_type_func_replace_returns(ctx->arena, f->signature, rets);
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

            // Class::member of a class declared in another file: its signature
            // carries the return type a chained or auto receiver dispatches on.
            const char *scope_class =
                c_resolve_scope_class_qn(ctx, cbm_arena_strndup(ctx->arena, qn, prefix_len));
            const CBMRegisteredFunc *sm =
                scope_class ? c_lookup_scope_member(ctx, scope_class, dot + 1) : NULL;
            if (sm && sm->signature) {
                return sm->signature;
            }
        }

        return cbm_type_unknown();
    }

    // --- template_function: make_shared<Widget>, make_unique<Foo> ---
    // Returns a FUNC type whose return type is TEMPLATE(base_return,
    // [explicit_args]).
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
            NULL}; /* zero-fill: cbm_type_substitute requires NULL-terminated args
                      (uninitialized tail bound T to stack garbage -> corrupt type
                      graph, bitcoin serialize.h) */
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
        return cbm_type_func_replace_returns(ctx->arena, f->signature, rets);
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

            // Template param substitution: try receiver's template args (method
            // calls) then try TAD from call-site argument types (free function calls)
            bool needs_subst = (ret->kind == CBM_TYPE_TYPE_PARAM || ret->kind == CBM_TYPE_NAMED ||
                                ret->kind == CBM_TYPE_POINTER || ret->kind == CBM_TYPE_REFERENCE ||
                                ret->kind == CBM_TYPE_RVALUE_REF ||
                                ret->kind == CBM_TYPE_TEMPLATE || ret->kind == CBM_TYPE_SLICE);

            if (needs_subst) {
                bool substituted = false;

                // Strategy 1: method call on templated receiver — use receiver's
                // template args
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

                // Strategy 2: TAD — deduce template params from call-site argument
                // types
                if (!substituted) {
                    // Find the registered function to get its type_param_names and
                    // param_types
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

    // --- pointer_expression: *p or &x (C tree-sitter uses this instead of
    // unary_expression) ---
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

        // Phase A: trailing return type on declarator (e.g., [](int x) -> Widget {
        // ... })
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

        // Phase B: infer from body's immediate return statements (no descent into
        // nested lambdas)
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

    // --- co_await_expression: co_await expr → evaluate operand, return
    // await_resume type ---
    if (strcmp(kind, "co_await_expression") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            TSNode operand = ts_node_named_child(node, 0);
            const CBMType *op_type = c_eval_expr_type(ctx, operand);
            // co_await returns the result of operator co_await or the awaitable's
            // await_resume Heuristic: if operand is a template type (e.g., Task<T>),
            // return first template arg
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
        // Heuristic: return the type of the first named child that isn't an
        // operator.
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

// Build "<module_qn>.<type_qn>" into a caller-provided stack buffer, replacing
// the vsnprintf("%s.%s") path (whose %s does a strlen of both args) that
// otherwise dominates the kernel cross-file resolve miss cascade. Uses the
// cached module_qn_len. Returns buf on success, or NULL if the result would not
// fit (caller falls back to cbm_arena_sprintf). The buffer is only read by the
// immediate registry lookup — a stack lifetime is sufficient (the QN is never
// retained).
static const char *c_build_module_prefixed(const CLSPContext *ctx, const char *type_qn, char *buf,
                                           size_t bufsz) {
    size_t mlen = ctx->module_qn_len;
    size_t tlen = strlen(type_qn);
    if (mlen + 1 + tlen + 1 > bufsz)
        return NULL; // would truncate → fall back to arena sprintf
    memcpy(buf, ctx->module_qn, mlen);
    buf[mlen] = '.';
    memcpy(buf + mlen + 1, type_qn, tlen);
    buf[mlen + 1 + tlen] = '\0';
    return buf;
}

// FNV-1a over (type_qn, 0xff separator, member_name) for the negative-lookup
// memo. 0 is remapped to 1 so it can serve as the empty-slot sentinel.
static uint64_t c_neg_memo_hash(const char *type_qn, const char *member_name) {
    uint64_t h = 1469598103934665603ULL; // FNV-1a offset basis
    for (const unsigned char *p = (const unsigned char *)type_qn; *p; p++) {
        h ^= (uint64_t)*p;
        h *= 1099511628211ULL;
    }
    h ^= (uint64_t)0xffu;
    h *= 1099511628211ULL;
    for (const unsigned char *p = (const unsigned char *)member_name; *p; p++) {
        h ^= (uint64_t)*p;
        h *= 1099511628211ULL;
    }
    return h ? h : 1ULL;
}

// Open-addressing uint64 hash set, 0 = empty slot. Shared by the
// negative-lookup memo and the emitted-field-reference set; both are
// malloc-owned by the context.
static bool c_u64_set_contains(const uint64_t *table, int cap, uint64_t h) {
    if (!table || cap == 0)
        return false;
    uint64_t mask = (uint64_t)cap - 1;
    for (uint64_t i = h & mask;; i = (i + 1) & mask) {
        uint64_t slot = table[i];
        if (slot == 0)
            return false; // empty slot → not present
        if (slot == h)
            return true;
    }
}

// Returns true when h was not in the set before this call. Lazy alloc /
// grow-by-rehash at 70% load. On OOM the set stays as it is and h counts as
// new: every caller is correct without the set, it only saves repeated work.
static bool c_u64_set_insert(uint64_t **table, int *cap, int *count, uint64_t h) {
    if (*table == NULL) {
        uint64_t *nm = (uint64_t *)calloc(1024, sizeof(uint64_t));
        if (!nm)
            return true;
        *table = nm;
        *cap = 1024;
        *count = 0;
    } else if ((*count + 1) * 10 >= *cap * 7) {
        int new_cap = *cap * 2;
        uint64_t *nm = (uint64_t *)calloc((size_t)new_cap, sizeof(uint64_t));
        if (nm) {
            uint64_t nmask = (uint64_t)new_cap - 1;
            for (int j = 0; j < *cap; j++) {
                uint64_t v = (*table)[j];
                if (v == 0)
                    continue;
                uint64_t k = v & nmask;
                while (nm[k] != 0)
                    k = (k + 1) & nmask;
                nm[k] = v;
            }
            free(*table);
            *table = nm;
            *cap = new_cap;
        }
        // if calloc failed: keep the existing table (load may exceed 70%, still
        // correct)
    }
    uint64_t mask = (uint64_t)*cap - 1;
    for (uint64_t i = h & mask;; i = (i + 1) & mask) {
        if ((*table)[i] == 0) {
            (*table)[i] = h;
            (*count)++;
            return true;
        }
        if ((*table)[i] == h)
            return false; // already recorded
    }
}

static bool c_neg_memo_contains(const CLSPContext *ctx, uint64_t h) {
    return c_u64_set_contains(ctx->neg_memo, ctx->neg_memo_cap, h);
}

static void c_neg_memo_insert(CLSPContext *ctx, uint64_t h) {
    // On OOM the memo is silently disabled (correctness is unaffected — the full
    // cascade still runs).
    (void)c_u64_set_insert(&ctx->neg_memo, &ctx->neg_memo_cap, &ctx->neg_memo_count, h);
}

static void c_u64_set_free(uint64_t **table, int *cap, int *count) {
    free(*table);
    *table = NULL;
    *cap = 0;
    *count = 0;
}

static void c_neg_memo_free(CLSPContext *ctx) {
    c_u64_set_free(&ctx->neg_memo, &ctx->neg_memo_cap, &ctx->neg_memo_count);
    c_u64_set_free(&ctx->field_ref_seen, &ctx->field_ref_seen_cap, &ctx->field_ref_seen_count);
}

static const CBMRegisteredFunc *c_lookup_member_depth(CLSPContext *ctx, const char *type_qn,
                                                      const char *member_name, int depth) {
    if (!type_qn || !member_name)
        return NULL;
    if (depth > CBM_LSP_MAX_LOOKUP_DEPTH)
        return NULL;

    // Direct method lookup. Runs FIRST, before consulting the memo, so a real
    // direct-resolvable member (incl. a hash-collision victim, or one registered
    // mid-file) can never be skipped: it is returned here regardless of the memo.
    const CBMRegisteredFunc *f = cbm_registry_lookup_method(ctx->registry, type_qn, member_name);
    if (f)
        return f;

    // Negative-lookup memo (depth==0, shared read-only registry only). A recorded
    // hit means this exact (type_qn, member) already failed the whole cascade.
    // Under the sealed Tier-2 registry the module-prefix (below), base-class and
    // short-name cascades are pure, immutable functions of (type_qn, registry),
    // so they are provably still NULL and can be skipped. Only the SCOPE-ALIAS
    // path is context-dependent, so it is still evaluated below before we trust
    // the memo. The direct lookup above already served as the collision/staleness
    // verification.
    bool neg_memo_hit = false;
    uint64_t neg_h = 0;
    if (depth == 0 && ctx->registry_shared) {
        neg_h = c_neg_memo_hash(type_qn, member_name);
        neg_memo_hit = c_neg_memo_contains(ctx, neg_h);
    }

    // Try module-prefixed QN (e.g., "Container" -> "test.main.Container").
    // Skipped on a memo hit: provably NULL under the read-only shared registry.
    if (!neg_memo_hit && ctx->module_qn) {
        char sbuf[1024];
        const char *prefixed = c_build_module_prefixed(ctx, type_qn, sbuf, sizeof(sbuf));
        if (!prefixed)
            prefixed = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn);
        f = cbm_registry_lookup_method(ctx->registry, prefixed, member_name);
        if (f)
            return f;
    }

    // Scope-based alias: using Vec = std::vector<T>;
    // Vec is in scope as ALIAS → follow to underlying type's QN.
    // ALWAYS evaluated (scope is context-dependent — not covered by the memo).
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

    // Memo hit and the scope-alias path also missed: the remaining base-class and
    // short-name cascades are registry-only and provably NULL → skip them (this
    // is where the O(type_count) short-name scan is avoided). Idempotent
    // re-insert is unnecessary since neg_h is already present.
    if (neg_memo_hit)
        return NULL;

    // Check registered type for alias and base classes
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt && ctx->module_qn) {
        char sbuf[1024];
        const char *prefixed = c_build_module_prefixed(ctx, type_qn, sbuf, sizeof(sbuf));
        if (!prefixed)
            prefixed = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn);
        rt = cbm_registry_lookup_type(ctx->registry, prefixed);
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

    /* Namespaced-type short-name fallback: a type name that resolves nowhere may
     * be a type declared inside a namespace whose registered QN carries the
     * namespace ("<module>.<ns>.Logger"), while the use site only knew the
     * file-scoped "<module>.Logger" or the bare "Logger" (e.g. the return type of
     * a namespace-scoped factory used outside that namespace). Resolve by the
     * SHORT name (last segment) against the registry and retry with the full QN.
     * Reached only after the direct/module/alias/base lookups all miss; prefers
     * an in-module match. Mirrors the C# short-name type fallback. */
    if (depth == 0 && ctx->registry) {
        const char *dot = strrchr(type_qn, '.');
        const char *shortn = dot ? dot + 1 : type_qn;
        size_t slen = strlen(shortn);
        const char *best_qn = NULL;
        CBMTypeShortIter it;
        cbm_registry_types_by_short_name_chain(ctx->registry, shortn, &it);
        int i;
        while ((i = cbm_type_short_iter_next(&it)) >= 0) {
            const char *q = it.reg->types[i].qualified_name;
            if (!q) {
                continue;
            }
            size_t qlen = strlen(q);
            if (qlen <= slen + 1 || q[qlen - slen - 1] != '.' ||
                strcmp(q + qlen - slen, shortn) != 0) {
                continue;
            }
            if (strcmp(q, type_qn) == 0) {
                continue; // already tried as-is above
            }
            best_qn = q;
            if (ctx->module_qn && strncmp(q, ctx->module_qn, strlen(ctx->module_qn)) == 0) {
                break; // prefer a match in the current module
            }
        }
        if (best_qn) {
            f = c_lookup_member_depth(ctx, best_qn, member_name, depth + 1);
            if (f) {
                return f;
            }
        }
    }

    // Whole cascade missed: record the negative result so a repeat of this exact
    // (type_qn, member) can skip the module-prefix/base-class/short-name work.
    if (depth == 0 && ctx->registry_shared)
        c_neg_memo_insert(ctx, neg_h);
    return NULL;
}

const CBMRegisteredFunc *c_lookup_member(CLSPContext *ctx, const char *type_qn,
                                         const char *member_name) {
    return c_lookup_member_depth(ctx, type_qn, member_name, 0);
}

/* A member of a class c_resolve_scope_class_qn already pinned down: direct,
 * then through its aliases and base classes. Entering below depth 0 skips the
 * short-name fallback, which would re-anchor the lookup onto a same-named
 * class elsewhere -- the scope names exactly this class. */
static const CBMRegisteredFunc *c_lookup_scope_member(CLSPContext *ctx, const char *class_qn,
                                                      const char *member_name) {
    return c_lookup_member_depth(ctx, class_qn, member_name, 1);
}

// True if any BASE class of type_qn (not type_qn itself) declares member_name —
// i.e. a method found directly on type_qn is an OVERRIDE of an inherited
// method. This mirrors the existing virtual-dispatch notion (a derived override
// of a base method) for the case where the override is resolved directly on the
// derived type rather than through the base.
static bool c_base_declares_member(CLSPContext *ctx, const char *type_qn, const char *member_name) {
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt && ctx->module_qn) {
        rt = cbm_registry_lookup_type(
            ctx->registry, cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn));
    }
    if (!rt || !rt->embedded_types)
        return false;
    for (int i = 0; rt->embedded_types[i]; i++) {
        if (c_lookup_member(ctx, rt->embedded_types[i], member_name))
            return true;
    }
    return false;
}

/* Type of the idx-th declared field, for structured-binding decomposition.
 * field_types[] is parallel to field_names[] and may hold NULL entries, so
 * only field_names[] bounds it: a binding list longer than the field list
 * (ill-formed, but present in compiler test suites) must yield NULL, never
 * read past the array into the next arena object (#1743). */
static const CBMType *c_struct_field_type_at(const CBMRegisteredType *rt, int idx) {
    if (!rt || !rt->field_names || !rt->field_types || idx < 0) {
        return NULL;
    }
    for (int i = 0; i < idx; i++) {
        if (!rt->field_names[i]) {
            return NULL;
        }
    }
    return rt->field_names[idx] ? rt->field_types[idx] : NULL;
}

// Field type lookup
static const CBMType *c_lookup_field_type(CLSPContext *ctx, const char *type_qn,
                                          const char *field_name, int depth) {
    if (!type_qn || !field_name || depth > 5)
        return NULL;

    /* The same type lookup the field-owner rows use: a use site names the
     * struct under its own module, the registry holds it under the header
     * that declares it. Without it `d->ops->shut` loses its type at the first
     * step whenever `d`'s struct comes from a header. */
    const CBMRegisteredType *rt = c_resolve_field_owner_type(ctx, type_qn);
    if (!rt)
        return NULL;

    if (rt->alias_of)
        return c_lookup_field_type(ctx, rt->alias_of, field_name, depth + 1);

    if (rt->field_names) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], field_name) == 0 && rt->field_types &&
                rt->field_types[i])
                return c_type_is_parameter_of(ctx, rt->short_name, rt->field_types[i])
                           ? NULL
                           : rt->field_types[i];
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

// The registered type a member access is made on: as written, then
// module-qualified, then by short name -- a C use site names `struct Alpha`
// while the struct is registered under the header that declares it.
//
// A short name must belong to this module or an explicitly included header.
// Project-wide uniqueness does not establish visibility. Conflicting included
// declarations remain unresolved.
//
// Only a type that HAS members can own one. `struct Ops *ops;` inside another
// struct, a forward declaration and a parameter type each register a memberless
// `Ops` of their own scope; counted as candidates they made every such name
// ambiguous and left the object untyped. They are considered only when no
// candidate with members exists (the "typed, declares no such field" answer).
static bool c_type_has_members(const CBMRegisteredType *t) {
    return (t->field_names && t->field_names[0]) || (t->embedded_types && t->embedded_types[0]) ||
           t->alias_of;
}

// One short-name scan of c_resolve_field_owner_type. *found says whether the
// scan decided anything, including "ambiguous" (NULL with *found set).
static const CBMRegisteredType *c_field_owner_by_short_name(CLSPContext *ctx, const char *shortn,
                                                            bool members_only, bool *found) {
    const CBMRegisteredType *included = NULL;
    bool ambiguous = false;
    CBMTypeShortIter it;
    cbm_registry_types_by_short_name_chain(ctx->registry, shortn, &it);
    int i;
    *found = false;
    while ((i = cbm_type_short_iter_next(&it)) >= 0) {
        const CBMRegisteredType *candidate = &it.reg->types[i];
        const char *qn = candidate->qualified_name;
        if (!qn || (members_only && !c_type_has_members(candidate)))
            continue;
        size_t qn_len = strlen(qn), short_len = strlen(shortn);
        if (qn_len <= short_len + 1 || qn[qn_len - short_len - 1] != '.' ||
            strcmp(qn + qn_len - short_len, shortn))
            continue;
        const CTUModule *entry =
            c_tu_for_declaration(ctx, candidate->source_file_qn, qn, qn_len - short_len - 1);
        if (!entry || entry->exports_only)
            continue;
        *found = true;
        if (included && strcmp(included->qualified_name, qn))
            ambiguous = true;
        included = candidate;
    }
    return ambiguous ? NULL : included;
}

static const CBMRegisteredType *c_resolve_field_owner_type(CLSPContext *ctx, const char *type_qn) {
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, type_qn);
    if (!rt && ctx->module_qn) {
        char sbuf[1024];
        const char *prefixed = c_build_module_prefixed(ctx, type_qn, sbuf, sizeof(sbuf));
        if (!prefixed)
            prefixed = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn);
        rt = cbm_registry_lookup_type(ctx->registry, prefixed);
    }
    if (!ctx->registry || (rt && c_type_has_members(rt)))
        return rt;

    /* An explicit qualification may never be discarded to rescue a namesake. */
    if (strchr(type_qn, '.'))
        return rt;
    const char *shortn = type_qn;
    bool found = false;
    const CBMRegisteredType *owner = c_field_owner_by_short_name(ctx, shortn, true, &found);
    if (found)
        return owner;
    if (rt)
        return rt;
    return c_field_owner_by_short_name(ctx, shortn, false, &found);
}

/* `typedef struct X X;` in x.h beside `struct X {...}` in x.c: a header and its source share a
 * module, so the two have one QN and denote one type. Neither is an alias of anything else. */
static bool c_type_is_self_alias(const CBMRegisteredType *type) {
    return type->alias_of && type->short_name && !strcmp(type->alias_of, type->short_name);
}

static const CBMRegisteredType *c_self_alias_tag(CLSPContext *ctx, const CBMRegisteredType *type) {
    CBMTypeQNIter iterator;
    cbm_registry_types_with_qn(ctx->registry, type->qualified_name, &iterator);
    const CBMRegisteredType *candidate;
    while ((candidate = cbm_type_qn_next(&iterator, NULL)))
        if (!candidate->alias_of)
            return candidate;
    return type;
}

// The registered type that DECLARES field_name, reached from type_qn through
// aliases and base classes as c_lookup_field_type walks them. A field whose
// type text did not parse still has an owner, so only the name decides.
static const CBMRegisteredType *c_lookup_field_owner(CLSPContext *ctx, const char *type_qn,
                                                     const char *field_name, int depth,
                                                     bool *complete) {
    *complete = false;
    if (!type_qn || !field_name || depth > 5)
        return NULL;
    const CBMRegisteredType *rt = c_resolve_field_owner_type(ctx, type_qn);
    if (!rt)
        return NULL;
    if (c_type_is_self_alias(rt))
        rt = c_self_alias_tag(ctx, rt);
    if (rt->alias_of)
        return c_lookup_field_owner(ctx, rt->alias_of, field_name, depth + 1, complete);
    if (rt->field_names) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (!strcmp(rt->field_names[i], field_name)) {
                *complete = true;
                return rt;
            }
        }
    }
    const CBMRegisteredType *match = NULL;
    bool all_complete = true;
    if (rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            bool base_complete = false;
            const CBMRegisteredType *owner = c_lookup_field_owner(
                ctx, rt->embedded_types[i], field_name, depth + 1, &base_complete);
            if (!base_complete)
                all_complete = false;
            if (owner && match && strcmp(owner->qualified_name, match->qualified_name))
                all_complete = false;
            if (owner)
                match = owner;
        }
    }
    *complete = all_complete;
    return all_complete ? match : NULL;
}

/* Resolve the written annotation independently of the legacy inferred type's
 * spelling. In particular, an unresolved `A` may have received the synthetic
 * current-module prefix; that prefix is not a declaration identity. */
static const CBMRegisteredType *c_proof_direct_class(CLSPContext *ctx, const char *qn, uint32_t at);
static const CBMRegisteredType *c_proof_field_class(CLSPContext *ctx, const char *qn, uint32_t at);

/* Declared inside a class, as `typedef detail::IEEEFloat IEEEFloat;` is in APFloat, a type's
 * plain name means nothing outside that class. Code in a class with bases may inherit it.
 * C has no such scope: a struct tag declared inside another struct is at file scope. */
static bool c_nested_type_in_scope(const CLSPContext *ctx, const char *scope) {
    if (!ctx->cpp_mode || ctx->plain_header)
        return true;
    const CBMRegisteredType *klass = ctx->proof_method_class;
    if (!klass)
        return ctx->enclosing_class_qn != NULL;
    return !strcmp(klass->qualified_name, scope) ||
           (klass->embedded_types && klass->embedded_types[0]);
}

/* A scope named from a module of `module_len` bytes: the top level of that file, a namespace around
 * this code, or one a visible `using namespace` brings here. */
static bool c_scope_reached(CLSPContext *ctx, const char *written, const char *scope,
                            size_t module_len, uint32_t at) {
    if (!scope[module_len])
        return true;
    if (written[0] == ':')
        return false;
    const char *from = scope + module_len + 1;
    size_t from_len = strlen(from);
    const char *around = ctx->current_namespace;
    if (around && ctx->module_qn && !strncmp(around, ctx->module_qn, ctx->module_qn_len) &&
        around[ctx->module_qn_len] == '.') {
        around += ctx->module_qn_len + 1;
        if (!strncmp(around, from, from_len) && (!around[from_len] || around[from_len] == '.'))
            return true;
    }
    for (int i = 0; i < ctx->proof_using_count; i++) {
        const CProofUsing *using = &ctx->proof_usings[i];
        if (using->namespace && using->supported && !strcmp(using->target, from) &&
            c_proof_using_visible(ctx, using, at))
            return true;
    }
    return false;
}

/* `ns::Name`: what the first segment means depends on where lookup starts. The declaration's scope,
 * with the written qualifier taken off, has to be a place that lookup reaches. */
static bool c_qualifier_starts_here(CLSPContext *ctx, const char *written, const char *file_qn,
                                    const char *scope, uint32_t at) {
    /* `p.dir.t.hpp.__file__` is the file whose declarations are named `p.dir.t.Name`. */
    static const char file_tail[] = ".__file__";
    size_t tail_len = sizeof(file_tail) - 1;
    size_t file_len = file_qn ? strlen(file_qn) : 0;
    if (file_len <= tail_len || strcmp(file_qn + file_len - tail_len, file_tail))
        return false;
    file_len -= tail_len;
    while (file_len && file_qn[file_len - 1] != '.')
        file_len--;
    if (!file_len)
        return false;
    file_len--;
    if (strncmp(scope, file_qn, file_len) || (scope[file_len] && scope[file_len] != '.'))
        return false;
    return c_scope_reached(ctx, written, scope, file_len, at);
}

/* Every declaration of this type name the translation unit sees. A name may be
 * declared repeatedly: `typedef struct X X;` beside `struct X {...}`, or the same
 * typedef in several headers. Those denote one type. Anything else is a conflict. */
static const CBMRegisteredType *c_proof_annotation_stage(CLSPContext *ctx, const char *name,
                                                         bool *found, bool from_includer,
                                                         const char *written, uint32_t at) {
    const CBMRegisteredType *chosen = NULL;
    *found = false;
    size_t name_len = strlen(name);
    const char *leaf = strrchr(name, '.');
    CBMTypeShortIter it;
    cbm_registry_types_by_short_name_chain(ctx->proof_registry, leaf ? leaf + 1 : name, &it);
    int index;
    while ((index = cbm_type_short_iter_next(&it)) >= 0) {
        const CBMRegisteredType *candidate = &it.reg->types[index];
        const char *qn = candidate->qualified_name;
        size_t qn_len = qn ? strlen(qn) : 0;
        if (qn_len <= name_len + 1 || qn[qn_len - name_len - 1] != '.' ||
            strcmp(qn + qn_len - name_len, name))
            continue;
        size_t module_len = qn_len - name_len - 1;
        const char *scope = cbm_arena_strndup(ctx->arena, qn, module_len);
        if (scope && cbm_registry_lookup_type(ctx->proof_registry, scope) &&
            !c_nested_type_in_scope(ctx, scope))
            continue;
        if (written && (!scope || !c_qualifier_starts_here(ctx, written, candidate->source_file_qn,
                                                           scope, at)))
            continue;
        bool own = c_is_own_declaration(ctx, candidate->source_file_qn, qn, module_len);
        const CTUModule *entry =
            own ? NULL : c_tu_for_declaration(ctx, candidate->source_file_qn, qn, module_len);
        if (!own && (!entry || entry->exports_only))
            continue;
        if ((entry && entry->from_includer) != from_includer || (own && from_includer))
            continue;
        *found = true;
        if (!chosen || !strcmp(chosen->qualified_name, qn)) {
            /* The tag says what the type is, whichever of the two registered last. */
            if (!chosen || !c_type_is_self_alias(candidate))
                chosen = candidate;
            continue;
        }
        if (chosen->alias_of && candidate->alias_of &&
            !strcmp(chosen->alias_of, candidate->alias_of))
            continue;
        if (chosen->alias_of && !strcmp(chosen->alias_of, name)) {
            chosen = candidate;
            continue;
        }
        if (candidate->alias_of && !strcmp(candidate->alias_of, name))
            continue;
        return NULL;
    }
    return chosen;
}

static const CBMRegisteredType *c_proof_included_annotation(CLSPContext *ctx, const char *name,
                                                            bool *found) {
    const CBMRegisteredType *own = c_proof_annotation_stage(ctx, name, found, false, NULL, 0);
    return *found ? own : c_proof_annotation_stage(ctx, name, found, true, NULL, 0);
}

/* `ns::Name` in a file outside ns. The written qualifier is matched whole against the declaration's
 * name, and a namespace alias can change what its first segment means. */
static const CBMRegisteredType *c_proof_qualified_type(CLSPContext *ctx, const char *written,
                                                       uint32_t at) {
    const char *name = ctx->cpp_mode ? c_proof_name(ctx, written) : NULL;
    const char *dot = name ? strchr(name, '.') : NULL;
    if (!dot)
        return NULL;
    size_t first = (size_t)(dot - name);
    for (int i = 0; i < ctx->ns_alias_count; i++)
        if (!strncmp(ctx->ns_alias_names[i], name, first) && !ctx->ns_alias_names[i][first])
            return NULL;
    bool found = false;
    const CBMRegisteredType *own = c_proof_annotation_stage(ctx, name, &found, false, written, at);
    return found ? own : c_proof_annotation_stage(ctx, name, &found, true, written, at);
}

/* The declaration a type name denotes at this point: this file's own, or the
 * single one its includes make visible. Explicit qualification stays intact. */
static const CBMRegisteredType *c_proof_type_by_name(CLSPContext *ctx, const char *name,
                                                     uint32_t at) {
    const CBMRegisteredType *local = c_resolve_field_owner_type(ctx, c_build_qn(ctx, name));
    /* Keep explicit qualification intact. An unqualified source name also
     * searches every included module, retaining memberless and unsupported
     * declarations as competitors. */
    if (strstr(name, "::") && !local)
        return c_proof_qualified_type(ctx, name, at);
    if (strchr(name, '.') || strstr(name, "::"))
        return local;
    bool found = false;
    const CBMRegisteredType *included = c_proof_included_annotation(ctx, name, &found);
    if (found && (!included || (local && strcmp(local->qualified_name, included->qualified_name))))
        return NULL;
    return included ? included : local;
}

/* Through typedef names to what they alias, in the unit's own view of each name.
 * A name that aliases itself (`typedef struct X X;`) means the tag of that name. */
static const CBMRegisteredType *c_proof_follow_alias(CLSPContext *ctx,
                                                     const CBMRegisteredType *type) {
    for (int hop = 0; type && type->alias_of && hop < 8; hop++) {
        if (type->short_name && !strcmp(type->alias_of, type->short_name)) {
            bool found = false;
            const CBMRegisteredType *tag = c_proof_included_annotation(ctx, type->alias_of, &found);
            if (!tag || tag == type || tag->alias_of)
                return NULL;
            return tag;
        }
        type = c_proof_type_by_name(ctx, type->alias_of, UINT32_MAX);
    }
    return type && !type->alias_of ? type : NULL;
}

static bool c_name_is_declared_as(CLSPContext *ctx, const char *name, CBMCDeclKind kind) {
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link; link = link->next)
        if (link->decl->kind == kind && !strcmp(link->decl->name, name))
            return true;
    return false;
}

static bool c_template_is_specialized(CLSPContext *ctx, const char *name) {
    return c_name_is_declared_as(ctx, name, CBM_C_DECL_SPECIALIZATION);
}

/* Base clauses are recorded without their arguments, so inside a template any class template
 * as a base may depend on the parameters. An unqualified name does not look into such a base. */
static bool c_base_may_depend(CLSPContext *ctx, const char *name) {
    return ctx->in_template && name && c_name_is_declared_as(ctx, name, CBM_C_DECL_CLASS_TEMPLATE);
}

/* `struct Name` is looked up among tags, and where lookup finds none it declares a new one. So it
 * is not a typedef of something else (`typedef struct impl Name;`), not the typedef of an unnamed
 * aggregate (`typedef struct {...} Name;` gave no tag), and in C++ not a namesake in a namespace
 * that lookup does not reach from here. A plain `Name` had to be declared to compile. This need
 * not. */
static bool c_tag_is_other_than(CLSPContext *ctx, const CBMRegisteredType *type, const char *name,
                                bool qualified, uint32_t at) {
    if (type->alias_of && strcmp(type->alias_of, name))
        return true;
    /* This file's own typedefs are known on every pass, an included file's only where facts are. */
    const char *no_tag = cbm_arena_sprintf(ctx->arena, "$no-tag:%s", name);
    if (no_tag && c_proof_declared(ctx, no_tag, at))
        return true;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind == CBM_C_DECL_UNNAMED_TYPEDEF && !strcmp(decl->name, name) &&
            c_tu_find(ctx, decl->file, strlen(decl->file)))
            return true;
    }
    if (!ctx->cpp_mode || ctx->plain_header || qualified)
        return false;
    const char *qn = type->qualified_name;
    const char *dot = qn ? strrchr(qn, '.') : NULL;
    const char *scope = dot ? cbm_arena_strndup(ctx->arena, qn, (size_t)(dot - qn)) : NULL;
    if (!scope)
        return true;
    /* A type nested in a class passed the scope check of that class on its way here. */
    if (cbm_registry_lookup_type(ctx->proof_registry, scope))
        return false;
    if (type->source_file_qn)
        return !c_qualifier_starts_here(ctx, name, type->source_file_qn, scope, at);
    /* The pass over one file alone has no facts, and every project type in it is this file's own.
     */
    size_t module_len = ctx->module_qn_len;
    return strncmp(scope, ctx->module_qn, module_len) ||
           (scope[module_len] && scope[module_len] != '.') ||
           !c_scope_reached(ctx, name, scope, module_len, at);
}

/* `struct S { ... }; struct S x;` in a block: x has the block's S, whatever an outer S declares.
 * Bound under a key of its own, because `struct config config;` is an ordinary variable that hides
 * no tag: C keeps the two names apart. */
static bool c_name_bound_locally(CLSPContext *ctx, const char *name);
static const char *c_local_tag_key(CLSPContext *ctx, const char *name) {
    return cbm_arena_sprintf(ctx->arena, "struct %s", name);
}
static bool c_local_tag_hides(CLSPContext *ctx, const char *name) {
    if (!ctx->local_tag_declared || strstr(name, "::"))
        return false;
    const char *key = c_local_tag_key(ctx, name);
    return key && c_name_bound_locally(ctx, key);
}

static const CBMRegisteredType *c_annotation_owner(CLSPContext *ctx, TSNode site) {
    TSNode annotation = ts_node_child_by_field_name(site, "type", 4);
    if (ts_node_is_null(annotation)) {
        TSNode declaration = ts_node_parent(site);
        if (!ts_node_is_null(declaration))
            annotation = ts_node_child_by_field_name(declaration, "type", 4);
    }
    bool elaborated = false;
    for (int depth = 0; depth < 8 && !ts_node_is_null(annotation); depth++) {
        const char *kind = ts_node_type(annotation);
        if (!strcmp(kind, "type_descriptor")) {
            annotation = ts_node_child_by_field_name(annotation, "type", 4);
            continue;
        }
        if (!strcmp(kind, "struct_specifier") || !strcmp(kind, "union_specifier") ||
            !strcmp(kind, "class_specifier")) {
            elaborated = true;
            annotation = ts_node_child_by_field_name(annotation, "name", 4);
            continue;
        }
        /* `SmallString<128> S`: the template's primary definition, unless one specialization
         * anywhere may replace its members for these arguments. */
        if (!strcmp(kind, "template_type")) {
            TSNode name_node = ts_node_child_by_field_name(annotation, "name", 4);
            const char *name = ts_node_is_null(name_node) ? NULL : c_node_text(ctx, name_node);
            if (!name || strcmp(ts_node_type(name_node), "type_identifier") ||
                c_template_is_specialized(ctx, name))
                return NULL;
            return c_proof_follow_alias(
                ctx, c_proof_type_by_name(ctx, name, ts_node_start_byte(annotation)));
        }
        if (strcmp(kind, "type_identifier") && strcmp(kind, "qualified_identifier") &&
            strcmp(kind, "scoped_type_identifier"))
            return NULL;
        const char *name = c_node_text(ctx, annotation);
        if (!name)
            return NULL;
        /* `llvm::SmallVector<int, 4> V`: the same rule as a template written without its scope. */
        if (!strcmp(kind, "qualified_identifier")) {
            TSNode last = annotation;
            for (int hop = 0; hop < 16 && !ts_node_is_null(last) &&
                              !strcmp(ts_node_type(last), "qualified_identifier");
                 hop++)
                last = ts_node_child_by_field_name(last, "name", 4);
            if (!ts_node_is_null(last) && !strcmp(ts_node_type(last), "template_type")) {
                TSNode template_name = ts_node_child_by_field_name(last, "name", 4);
                TSNode arguments = ts_node_child_by_field_name(last, "arguments", 9);
                const char *short_name =
                    ts_node_is_null(template_name) ? NULL : c_node_text(ctx, template_name);
                if (!short_name || ts_node_is_null(arguments) ||
                    c_template_is_specialized(ctx, short_name))
                    return NULL;
                name = cbm_arena_strndup(ctx->arena, name,
                                         ts_node_start_byte(arguments) -
                                             ts_node_start_byte(annotation));
                if (!name)
                    return NULL;
            }
        }
        if (c_local_tag_hides(ctx, name))
            return NULL;
        uint32_t at = ts_node_start_byte(annotation);
        const CBMRegisteredType *named = c_proof_type_by_name(ctx, name, at);
        /* That tag says nothing about the type its namesake is. */
        if (elaborated && named &&
            c_tag_is_other_than(ctx, named, name, strstr(name, "::") != NULL, at))
            return NULL;
        return c_proof_follow_alias(ctx, named);
    }
    return NULL;
}

/* Preserve the written declaration's indirection while replacing only the
 * inferred name with the exact source-bound owner. Unsupported type forms do
 * not acquire authority through a synthetic current-module prefix. */
static const CBMType *c_proof_annotation_type(CLSPContext *ctx, const CBMType *type,
                                              const char *owner, unsigned depth) {
    if (!type || depth > 8)
        return NULL;
    if (type->kind == CBM_TYPE_NAMED || type->kind == CBM_TYPE_TEMPLATE)
        return cbm_type_named(ctx->arena, owner);
    if (type->kind == CBM_TYPE_POINTER) {
        const CBMType *inner =
            c_proof_annotation_type(ctx, type->data.pointer.elem, owner, depth + 1);
        return inner ? cbm_type_pointer(ctx->arena, inner) : NULL;
    }
    if (type->kind == CBM_TYPE_SLICE)
        return c_proof_annotation_type(ctx, type->data.slice.elem, owner, depth + 1);
    if (type->kind == CBM_TYPE_REFERENCE || type->kind == CBM_TYPE_RVALUE_REF) {
        const CBMType *inner =
            c_proof_annotation_type(ctx, type->data.reference.elem, owner, depth + 1);
        return !inner                             ? NULL
               : type->kind == CBM_TYPE_REFERENCE ? cbm_type_reference(ctx->arena, inner)
                                                  : cbm_type_rvalue_ref(ctx->arena, inner);
    }
    return NULL;
}

static size_t c_proof_builtin_slot(const CBMVarBinding *binding, int capacity) {
    uintptr_t key = (uintptr_t)binding >> 3;
    return (size_t)(key * (uintptr_t)2654435761u) & (size_t)(capacity - 1);
}

static void c_proof_observe_builtin(CLSPContext *ctx, const char *name, const CBMType *type,
                                    TSNode site) {
    if (!type || type->kind != CBM_TYPE_BUILTIN)
        return;
    TSNode annotation = ts_node_child_by_field_name(site, "type", 4);
    if (ts_node_is_null(annotation))
        annotation = ts_node_child_by_field_name(ts_node_parent(site), "type", 4);
    const CBMType *written = cbm_c_builtin_annotation(ctx->arena, ctx->source, annotation);
    const CBMVarBinding *binding = cbm_scope_lookup_binding(ctx->current_scope, name);
    if (!written || !binding || !cbm_type_equal(written, type))
        return;
    if (ctx->proof_builtin_count * 2 >= ctx->proof_builtin_cap) {
        int capacity = ctx->proof_builtin_cap ? ctx->proof_builtin_cap * 2 : 32;
        CProofBuiltinBinding *next = cbm_arena_calloc(ctx->arena, (size_t)capacity * sizeof(*next));
        if (!next) {
            ctx->binding_failed = true;
            return;
        }
        for (int i = 0; i < ctx->proof_builtin_cap; i++) {
            CProofBuiltinBinding old = ctx->proof_builtins[i];
            if (!old.binding)
                continue;
            size_t slot = c_proof_builtin_slot(old.binding, capacity);
            while (next[slot].binding)
                slot = (slot + 1) & (size_t)(capacity - 1);
            next[slot] = old;
        }
        ctx->proof_builtins = next;
        ctx->proof_builtin_cap = capacity;
    }
    size_t slot = c_proof_builtin_slot(binding, ctx->proof_builtin_cap);
    while (ctx->proof_builtins[slot].binding && ctx->proof_builtins[slot].binding != binding)
        slot = (slot + 1) & (size_t)(ctx->proof_builtin_cap - 1);
    if (!ctx->proof_builtins[slot].binding)
        ctx->proof_builtin_count++;
    ctx->proof_builtins[slot] = (CProofBuiltinBinding){binding, written};
}

static bool c_proof_builtin_argument(CLSPContext *ctx, TSNode argument) {
    if (ctx->proof_preprocessor_unknown || !ctx->proof_builtin_cap ||
        strcmp(ts_node_type(argument), "identifier"))
        return false;
    const char *name = c_node_text(ctx, argument);
    const CBMVarBinding *binding = cbm_scope_lookup_binding(ctx->current_scope, name);
    if (!binding)
        return false;
    size_t slot = c_proof_builtin_slot(binding, ctx->proof_builtin_cap);
    while (ctx->proof_builtins[slot].binding) {
        if (ctx->proof_builtins[slot].binding == binding)
            return cbm_type_equal(ctx->proof_builtins[slot].type, binding->type);
        slot = (slot + 1) & (size_t)(ctx->proof_builtin_cap - 1);
    }
    return false;
}

/* Only explicit, resolved declaration types are donated. The type map then
 * keeps lexical identity and detects stale/replaced bindings; an inferred
 * `auto` type never acquires proof merely by naming a registered class. */
static void c_observe_declared_binding(CLSPContext *ctx, const char *name, const CBMType *type,
                                       TSNode site) {
    if (!name || !type || ts_node_is_null(site) || ctx->eval_failure)
        return;
    c_proof_observe_builtin(ctx, name, type, site);
    const CBMRegisteredType *owner = c_annotation_owner(ctx, site);
    if (!owner || !owner->qualified_name)
        return;
    owner = c_proof_field_class(ctx, owner->qualified_name, ts_node_start_byte(site));
    if (!owner)
        return;
    cbm_resolve_result_t proof = cbm_lsp_proof_type_lookup(
        &ctx->field_types, c_node_text(ctx, site), site, "c-written-declaration-type",
        owner->qualified_name, owner->declaration_identity, owner->graph_qualified_name,
        owner->source_flags, "visible-exact-source-type;complete-direct-domain");
    proof = c_proof_type_import(ctx, owner->qualified_name, ts_node_start_byte(site), proof);
    if (!cbm_resolve_admit(&proof))
        return;
    const CBMType *canonical = c_proof_annotation_type(ctx, type, owner->qualified_name, 0);
    /* File scope is walked twice, and `static ConnectionType CT_Socket;` ahead of its definition
     * declares one object at two places. The second walk re-observes that object at the later
     * place. A declaration going back in time would end the whole file's type map. */
    uint32_t at = ts_node_start_byte(site);
    if (!ctx->current_scope->parent) {
        uint32_t later = cbm_resolve_types_valid_from(&ctx->field_types, ctx->current_scope, name);
        at = later > at ? later : at;
    }
    if (canonical)
        (void)cbm_resolve_types_declare(&ctx->field_types, ctx->current_scope, name, at, canonical,
                                        CBM_RESOLVE_TYPE_ANNOTATION, &proof);
}

static const CBMRegisteredType *c_proof_expression_owner(CLSPContext *ctx, TSNode node, int depth,
                                                         const cbm_resolve_proof_t **proof,
                                                         const CBMType **shape);

/* `auto *S = static_cast<Foo *>(P);`, `auto R = makeRef();`, `auto &E = Holder.Field;`: deduction
 * takes the initializer's type, and the initializer's own chain of written declarations says what
 * that is. A template, a dependent or an unknown initializer says nothing. */
static void c_observe_auto_binding(CLSPContext *ctx, const char *name, TSNode value, TSNode site) {
    const cbm_resolve_proof_t *origin = NULL;
    const CBMType *shape = NULL;
    const CBMRegisteredType *owner = c_proof_expression_owner(ctx, value, 0, &origin, &shape);
    if (!owner || !origin || !shape)
        return;
    cbm_resolve_result_t typed = cbm_lsp_proof_type_lookup(
        &ctx->field_types, name, site, "c-deduced-from-the-initializer", owner->qualified_name,
        owner->declaration_identity, owner->graph_qualified_name, owner->source_flags,
        "auto-of-a-written-type");
    typed = c_proof_type_import(ctx, owner->qualified_name, ts_node_start_byte(site), typed);
    if (!cbm_resolve_admit(&typed))
        return;
    const CBMType *canonical = c_proof_annotation_type(ctx, shape, owner->qualified_name, 0);
    if (!canonical)
        return;
    c_scope_bind(ctx, name, canonical);
    (void)cbm_resolve_types_declare(&ctx->field_types, ctx->current_scope, name,
                                    ts_node_start_byte(site), canonical,
                                    CBM_RESOLVE_TYPE_ANNOTATION, &typed);
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

/* C++ declaration/expression ambiguity is resolved by lexical lookup. The
 * grammar can parse `Widget w(n)` as an unnamed parameter whose type is `n`;
 * a visible value binding for n instead makes this object direct-initialization.
 * This recovers only w's written type, not constructor applicability or a call. */
static bool c_bind_cpp_direct_initializer(CLSPContext *ctx, TSNode declarator,
                                          const CBMType *type) {
    if (!ctx->cpp_mode || !ctx->enclosing_func_qn || ts_node_has_error(declarator) ||
        ts_node_named_child_count(declarator) != 2)
        return false;
    TSNode name = ts_node_child_by_field_name(declarator, "declarator", 10);
    TSNode parameters = ts_node_child_by_field_name(declarator, "parameters", 10);
    if (ts_node_is_null(name) || strcmp(ts_node_type(name), "identifier") ||
        ts_node_is_null(parameters) || ts_node_named_child_count(parameters) != 1)
        return false;
    TSNode parameter = ts_node_named_child(parameters, 0);
    TSNode argument = ts_node_child_by_field_name(parameter, "type", 4);
    if (strcmp(ts_node_type(parameter), "parameter_declaration") ||
        ts_node_named_child_count(parameter) != 1 || ts_node_is_null(argument) ||
        strcmp(ts_node_type(argument), "type_identifier"))
        return false;
    const char *argument_name = c_node_text(ctx, argument);
    const CBMType *value =
        argument_name ? cbm_scope_lookup(ctx->current_scope, argument_name) : NULL;
    if (cbm_type_is_unknown(value) || value->kind == CBM_TYPE_ALIAS)
        return false; /* A type alias, unknown name or prototype is not a value argument. */
    const char *variable = c_node_text(ctx, name);
    if (!variable)
        return false;
    c_scope_bind(ctx, variable, type);
    c_observe_declared_binding(ctx, variable, type, declarator);
    return true;
}

void c_process_statement(CLSPContext *ctx, TSNode node) {
    if (ctx->eval_failure)
        return;
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    // declaration: Type var = expr; or Type var1, var2;
    if (strcmp(kind, "declaration") == 0) {
        const CBMType *base_type = c_parse_declaration_type(ctx, node);
        bool has_auto = false;
        bool has_inferred_type = false;

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
                has_inferred_type = true;
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
                                            const CBMType *ft =
                                                c_struct_field_type_at(rt, binding_idx);
                                            if (ft)
                                                elem_type = ft;
                                        }
                                        c_scope_bind(ctx, bname, elem_type);
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
                                    const CBMType *ft = c_struct_field_type_at(rt, binding_idx);
                                    if (ft) {
                                        elem_type = ft;
                                    }
                                }
                                c_scope_bind(ctx, bname, elem_type);
                                binding_idx++;
                            }
                        }
                        continue;
                    }

                    if (var_name && var_name[0] && strcmp(var_name, "_") != 0) {
                        c_scope_bind(ctx, var_name, var_type);
                        if (!has_auto && !has_inferred_type)
                            c_observe_declared_binding(ctx, var_name, var_type, child);
                        else if (has_auto && !ts_node_is_null(value))
                            c_observe_auto_binding(ctx, var_name, value, child);

                        /* Record one lexical fp state, including an explicit
                         * NULL state for callable-looking declarations whose
                         * initializer is dynamic/ambiguous. The NULL entry is
                         * important: it shadows an outer exact fp of the same
                         * name and can later become exact after an unconditional
                         * direct assignment. */
                        bool function_pointer_like =
                            var_type &&
                            (var_type->kind == CBM_TYPE_FUNC || var_type->kind == CBM_TYPE_POINTER);
                        const cbm_resolve_proof_t *fp_origin =
                            ts_node_is_null(value)
                                ? NULL
                                : c_exact_function_binding_from_expr(ctx, value, child, var_name);
                        const char *fp_target = fp_origin ? fp_origin->value.outcome.target : NULL;
                        /* Reference aliases share mutable storage. Until that
                         * alias relation is tracked, neither name proves a
                         * stable callable value. */
                        bool reference_storage = false;
                        (void)c_fp_declarator_name(ctx, decl, &reference_storage);
                        reference_storage |= var_type && var_type->kind == CBM_TYPE_REFERENCE;
                        if (reference_storage) {
                            TSNode storage = c_fp_unwrap_parentheses(ctx, value);
                            if (!ts_node_is_null(storage) &&
                                !strcmp(ts_node_type(storage), "identifier"))
                                c_escape_fp_storage(ctx, c_node_text(ctx, storage));
                            fp_origin = NULL;
                        }
                        const char *declaration_text = c_node_text(ctx, node);
                        if (!declaration_text || strstr(declaration_text, "volatile"))
                            fp_origin = NULL;

                        // DLL/dynamic resolver heuristic: fp =
                        // (FuncType)Resolve("FuncName") If RHS is a call (possibly
                        // cast-wrapped) with a string literal arg, and variable has
                        // function-pointer-like type or RHS has a cast, treat the string as
                        // an external function name.
                        if (!fp_target && !ts_node_is_null(value)) {
                            bool has_cast = false;
                            const char *dll_func =
                                c_extract_dll_resolve_name(ctx, value, &has_cast);
                            if (dll_func) {
                                bool is_fp_type = var_type && (var_type->kind == CBM_TYPE_FUNC ||
                                                               var_type->kind == CBM_TYPE_POINTER);
                                if (is_fp_type || has_cast) {
                                    fp_target =
                                        cbm_arena_sprintf(ctx->arena, "external.%s", dll_func);
                                }
                            }
                        }
                        if (function_pointer_like || fp_target) {
                            c_add_fp_target(ctx, var_name, fp_target, fp_origin);
                            if (reference_storage)
                                c_escape_fp_storage(ctx, var_name);
                        }
                    }
                }
            } else if (strcmp(ck, "identifier") == 0) {
                // Bare declaration without initializer: Type var;
                char *var_name = c_node_text(ctx, child);
                if (var_name && var_name[0]) {
                    c_scope_bind(ctx, var_name, base_type);
                    if (!has_auto && !has_inferred_type)
                        c_observe_declared_binding(ctx, var_name, base_type, child);
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
                /* `T *ptrs[3];` is an array of those pointers. */
                int dimensions = 0;
                while (!ts_node_is_null(inner) &&
                       strcmp(ts_node_type(inner), "array_declarator") == 0) {
                    dimensions++;
                    inner = ts_node_child_by_field_name(inner, "declarator", 10);
                }
                if (!ts_node_is_null(inner) && strcmp(ts_node_type(inner), "identifier") == 0) {
                    char *var_name = c_node_text(ctx, inner);
                    if (var_name) {
                        const CBMType *vt = base_type;
                        for (int d = 0; d < ptr_depth; d++)
                            vt = cbm_type_pointer(ctx->arena, vt);
                        for (int d = 0; d < dimensions; d++)
                            vt = cbm_type_slice(ctx->arena, vt);
                        c_scope_bind(ctx, var_name, vt);
                        if (!has_auto && !has_inferred_type)
                            c_observe_declared_binding(ctx, var_name, vt, child);
                    }
                }
            } else if (strcmp(ck, "function_declarator") == 0) {
                if (!has_auto && !has_inferred_type &&
                    c_bind_cpp_direct_initializer(ctx, child, base_type))
                    continue;
                /* Bare function-pointer declaration: `int (*fp)(void);`.
                 * Ordinary function prototypes have no pointer in the nested
                 * declarator and are intentionally ignored here. */
                TSNode inner = ts_node_child_by_field_name(child, "declarator", 10);
                bool saw_pointer = false;
                while (!ts_node_is_null(inner)) {
                    const char *inner_kind = ts_node_type(inner);
                    if (strcmp(inner_kind, "pointer_declarator") == 0) {
                        saw_pointer = true;
                    }
                    if (strcmp(inner_kind, "identifier") == 0) {
                        if (saw_pointer) {
                            char *var_name = c_node_text(ctx, inner);
                            if (var_name && var_name[0]) {
                                const CBMType *var_type = cbm_type_pointer(ctx->arena, base_type);
                                c_scope_bind(ctx, var_name, var_type);
                                c_add_fp_target(ctx, var_name, NULL, NULL);
                            }
                        }
                        break;
                    }
                    if ((strcmp(inner_kind, "parenthesized_declarator") != 0 &&
                         strcmp(inner_kind, "pointer_declarator") != 0 &&
                         strcmp(inner_kind, "reference_declarator") != 0) ||
                        ts_node_named_child_count(inner) == 0) {
                        break;
                    }
                    inner = ts_node_named_child(inner, ts_node_named_child_count(inner) - 1);
                }
            } else if (strcmp(ck, "array_declarator") == 0) {
                // Type var[N]; and Type var[N][M]; without initializer
                TSNode inner = child;
                const CBMType *vt = base_type;
                while (!ts_node_is_null(inner) &&
                       strcmp(ts_node_type(inner), "array_declarator") == 0) {
                    vt = cbm_type_slice(ctx->arena, vt);
                    inner = ts_node_child_by_field_name(inner, "declarator", 10);
                }
                if (!ts_node_is_null(inner) && strcmp(ts_node_type(inner), "identifier") == 0) {
                    char *var_name = c_node_text(ctx, inner);
                    if (var_name) {
                        c_scope_bind(ctx, var_name, vt);
                        if (!has_auto && !has_inferred_type)
                            c_observe_declared_binding(ctx, var_name, vt, child);
                    }
                }
            }
        }
        return;
    }

    /* Function-pointer reassignment. Only a plain, unconditional assignment to
     * one named function preserves exact identity. Every other assignment to a
     * tracked fp explicitly invalidates the old target. Inside control flow we
     * also invalidate after an exact-looking assignment: without path merging,
     * the pre-branch value remains a possible runtime target. */
    if (strcmp(kind, "assignment_expression") == 0) {
        bool simple_assignment = false;
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                char *op = c_node_text(ctx, child);
                if (op && strcmp(op, "=") == 0) {
                    simple_assignment = true;
                    break;
                }
            }
        }
        TSNode left = c_fp_unwrap_parentheses(ctx, ts_node_child_by_field_name(node, "left", 4));
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(left) && strcmp(ts_node_type(left), "identifier") == 0) {
            char *var_name = c_node_text(ctx, left);
            if (var_name && c_find_fp_binding_index(ctx, var_name) >= 0) {
                if (ctx->callable_flow_depth) {
                    /* Walking a delayed body does not execute its assignment.
                     * A captured write can happen later, so no subsequent
                     * assignment may reestablish this storage's exact value. */
                    c_escape_fp_storage(ctx, var_name);
                } else {
                    const cbm_resolve_proof_t *origin =
                        simple_assignment && !ts_node_is_null(right) && ctx->control_flow_depth == 0
                            ? c_exact_function_binding_from_expr(ctx, right, node, var_name)
                            : NULL;
                    c_update_fp_target(ctx, var_name, origin);
                }
            }
        }
        return;
    }

    // using_declaration: using namespace std; or using std::cout;
    if (strcmp(kind, "using_declaration") == 0) {
        c_proof_record_using(ctx, node);
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
                        // Even if enum type not found, add as using namespace for name
                        // lookup
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
                c_scope_bind(ctx, alias_name, cbm_type_alias(ctx->arena, alias_qn, target));
            }
        }
        return;
    }

    // type_definition: typedef Bar Foo; or typedef struct X Y;
    if (strcmp(kind, "type_definition") == 0) {
        // Get the type being aliased and the alias name(s).
        // Tree-sitter produces: type_definition { type_spec alias_name }
        // For "typedef RealWidget Widget": { type_identifier("RealWidget")
        // type_identifier("Widget") } For "typedef struct Foo Bar": {
        // struct_specifier type_identifier("Bar") } The first type_identifier may
        // be the source type (when no specifier precedes it).
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
                // First type_identifier without a preceding specifier — this IS the
                // source type. Mark it and parse; the NEXT type_identifier will be the
                // alias name.
                first_type_id_idx = (int)i;
                target = c_parse_type_node(ctx, child);
                found_type = true;
            } else if (found_type && strcmp(ck, "type_identifier") == 0) {
                /* `typedef struct { ... } Name;`: the aggregate has no name of
                 * its own, so `Name` is not an alias of anything -- it IS the
                 * type, registered with its members under that name. Aliasing
                 * it to the nameless struct made every `Name *p` untyped. */
                if (target && target->kind == CBM_TYPE_STRUCT)
                    continue;
                char *alias_name = c_node_text(ctx, child);
                if (alias_name) {
                    const char *alias_qn =
                        ctx->current_namespace
                            ? cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->current_namespace,
                                                alias_name)
                            : cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, alias_name);
                    c_scope_bind(ctx, alias_name, cbm_type_alias(ctx->arena, alias_qn, target));
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
                    c_scope_bind(ctx, var_name, elem_type);
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
                            const CBMType *ft = c_struct_field_type_at(rt, binding_idx);
                            if (ft)
                                bt = ft;
                        }
                        c_scope_bind(ctx, bname, bt);
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
                if (name) {
                    c_scope_bind(ctx, name, param_type);
                    c_observe_declared_binding(ctx, name, param_type, node);
                }
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
                        c_scope_bind(ctx, name, vt);
                        c_observe_declared_binding(ctx, name, vt, node);
                    }
                }
            } else if (strcmp(dk, "reference_declarator") == 0) {
                if (ts_node_named_child_count(decl) > 0) {
                    TSNode inner = ts_node_named_child(decl, 0);
                    if (strcmp(ts_node_type(inner), "identifier") == 0) {
                        char *name = c_node_text(ctx, inner);
                        if (name) {
                            const CBMType *vt = cbm_type_reference(ctx->arena, param_type);
                            c_scope_bind(ctx, name, vt);
                            c_observe_declared_binding(ctx, name, vt, node);
                        }
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

static bool c_qn_has_exact_scope(const char *qualified_name, const char *scope) {
    if (!qualified_name || !scope || !scope[0])
        return false;
    const char *dot = strrchr(qualified_name, '.');
    size_t qn_scope_len = dot ? (size_t)(dot - qualified_name) : 0;
    return qn_scope_len == strlen(scope) && strncmp(qualified_name, scope, qn_scope_len) == 0;
}

static int c_operator_param_count(const CBMRegisteredFunc *candidate) {
    if (!candidate || !candidate->signature || candidate->signature->kind != CBM_TYPE_FUNC) {
        return -1;
    }
    int count = 0;
    const CBMType *const *params = candidate->signature->data.func.param_types;
    while (params && params[count]) {
        count++;
    }
    return count;
}

static bool c_operator_signature_matches(CLSPContext *ctx, const CBMRegisteredFunc *candidate,
                                         const CBMType *const *actuals, int actual_count) {
    if (!candidate || candidate->receiver_type || !candidate->signature ||
        candidate->signature->kind != CBM_TYPE_FUNC || actual_count < 0 ||
        c_operator_param_count(candidate) != actual_count) {
        return false;
    }
    const CBMType *const *params = candidate->signature->data.func.param_types;
    for (int i = 0; i < actual_count; i++) {
        const CBMType *expected = c_simplify_type(ctx, params[i], false);
        const CBMType *actual = c_simplify_type(ctx, actuals[i], false);
        const char *expected_qn = type_to_qn(expected);
        const char *actual_qn = type_to_qn(actual);
        if (!expected_qn || !actual_qn || strcmp(expected_qn, actual_qn) != 0) {
            return false;
        }
    }
    return true;
}

/* Resolve a non-member C++ operator only when both operand types exactly match
 * a unique free overload in the current/associated namespace. Ambiguous,
 * templated, or otherwise unprovable cases deliberately stay unresolved. */
static const CBMRegisteredFunc *c_lookup_free_operator(CLSPContext *ctx, const char *operator_name,
                                                       const CBMType *left_type,
                                                       const CBMType *right_type) {
    if (!ctx || !ctx->registry || !operator_name || !left_type || !right_type)
        return NULL;

    const char *left_qn = type_to_qn(c_simplify_type(ctx, left_type, false));
    const char *right_qn = type_to_qn(c_simplify_type(ctx, right_type, false));
    const char *left_ns = extract_namespace_from_qn(ctx->arena, left_qn);
    const char *right_ns = extract_namespace_from_qn(ctx->arena, right_qn);
    const CBMRegisteredFunc *match = NULL;
    CBMFreeFuncIter it;
    cbm_registry_free_funcs_by_short_name_chain(ctx->registry, operator_name, &it);
    for (int index = cbm_free_func_iter_next(&it); index >= 0;
         index = cbm_free_func_iter_next(&it)) {
        if (index >= it.reg->func_count)
            continue;
        const CBMRegisteredFunc *candidate = &it.reg->funcs[index];
        if (candidate->receiver_type || !candidate->short_name ||
            strcmp(candidate->short_name, operator_name) != 0 || !candidate->qualified_name) {
            continue;
        }
        bool associated = c_qn_has_exact_scope(candidate->qualified_name, ctx->current_namespace) ||
                          c_qn_has_exact_scope(candidate->qualified_name, ctx->module_qn) ||
                          c_qn_has_exact_scope(candidate->qualified_name, left_ns) ||
                          c_qn_has_exact_scope(candidate->qualified_name, right_ns);
        const CBMType *actuals[2] = {left_type, right_type};
        bool signature_matches = c_operator_signature_matches(ctx, candidate, actuals, 2);
        if (ctx->debug) {
            const CBMType *const *params =
                candidate->signature && candidate->signature->kind == CBM_TYPE_FUNC
                    ? candidate->signature->data.func.param_types
                    : NULL;
            int param_count = 0;
            while (params && params[param_count]) {
                param_count++;
            }
            const char *p0 =
                param_count > 0 ? type_to_qn(c_simplify_type(ctx, params[0], false)) : NULL;
            const char *p1 =
                param_count > 1 ? type_to_qn(c_simplify_type(ctx, params[1], false)) : NULL;
            fprintf(stderr,
                    "  [clsp] free operator candidate=%s lhs=%s rhs=%s associated=%d "
                    "signature=%d p0=%s p1=%s extra=%d\n",
                    candidate->qualified_name, left_qn ? left_qn : "?", right_qn ? right_qn : "?",
                    associated, signature_matches, p0 ? p0 : "?", p1 ? p1 : "?", param_count > 2);
        }
        if (!associated || !signature_matches) {
            continue;
        }
        if (match && strcmp(match->qualified_name, candidate->qualified_name) != 0) {
            return NULL;
        }
        match = candidate;
    }
    return match;
}

/* Unary ADL is intentionally narrower than ordinary free-function lookup: the
 * operand type must exactly match a single one-parameter overload in the
 * current or associated namespace. Unknown/ambiguous cases stay unresolved. */
static const CBMRegisteredFunc *c_lookup_free_unary_operator(CLSPContext *ctx,
                                                             const char *operator_name,
                                                             const CBMType *operand_type) {
    if (!ctx || !ctx->registry || !operator_name || !operand_type) {
        return NULL;
    }
    const char *operand_qn = type_to_qn(c_simplify_type(ctx, operand_type, false));
    const char *operand_ns = extract_namespace_from_qn(ctx->arena, operand_qn);
    const CBMRegisteredFunc *match = NULL;
    const CBMType *actuals[1] = {operand_type};
    CBMFreeFuncIter it;
    cbm_registry_free_funcs_by_short_name_chain(ctx->registry, operator_name, &it);
    for (int index = cbm_free_func_iter_next(&it); index >= 0;
         index = cbm_free_func_iter_next(&it)) {
        if (index >= it.reg->func_count) {
            continue;
        }
        const CBMRegisteredFunc *candidate = &it.reg->funcs[index];
        if (candidate->receiver_type || !candidate->short_name ||
            strcmp(candidate->short_name, operator_name) != 0 || !candidate->qualified_name) {
            continue;
        }
        bool associated = c_qn_has_exact_scope(candidate->qualified_name, ctx->current_namespace) ||
                          c_qn_has_exact_scope(candidate->qualified_name, ctx->module_qn) ||
                          c_qn_has_exact_scope(candidate->qualified_name, operand_ns);
        if (!associated || !c_operator_signature_matches(ctx, candidate, actuals, 1)) {
            continue;
        }
        if (match && strcmp(match->qualified_name, candidate->qualified_name) != 0) {
            return NULL;
        }
        match = candidate;
    }
    return match;
}

/* Operator syntax has a fixed explicit arity. Refuse the registry API's
 * permissive "any overload" fallback when no exact-arity method exists. */
static const CBMRegisteredFunc *c_lookup_member_operator_by_arity(CLSPContext *ctx,
                                                                  const char *type_qn,
                                                                  const char *operator_name,
                                                                  int explicit_arg_count) {
    if (!ctx || !ctx->registry || !type_qn || !operator_name) {
        return NULL;
    }
    const CBMRegisteredFunc *candidate = cbm_registry_lookup_method_by_args(
        ctx->registry, type_qn, operator_name, explicit_arg_count);
    if (candidate && c_operator_param_count(candidate) == explicit_arg_count) {
        return candidate;
    }
    if (ctx->module_qn) {
        const char *prefixed = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, type_qn);
        candidate = cbm_registry_lookup_method_by_args(ctx->registry, prefixed, operator_name,
                                                       explicit_arg_count);
        if (candidate && c_operator_param_count(candidate) == explicit_arg_count) {
            return candidate;
        }
    }
    candidate = c_lookup_member(ctx, type_qn, operator_name);
    return candidate && c_operator_param_count(candidate) == explicit_arg_count ? candidate : NULL;
}

static bool c_is_compound_assignment_operator(const char *operator_token) {
    return operator_token &&
           (strcmp(operator_token, "+=") == 0 || strcmp(operator_token, "-=") == 0 ||
            strcmp(operator_token, "*=") == 0 || strcmp(operator_token, "/=") == 0 ||
            strcmp(operator_token, "%=") == 0 || strcmp(operator_token, "<<=") == 0 ||
            strcmp(operator_token, ">>=") == 0 || strcmp(operator_token, "&=") == 0 ||
            strcmp(operator_token, "|=") == 0 || strcmp(operator_token, "^=") == 0);
}

/* The additive owner APIs publish rows that outlive their input owner,
 * definition and registry arenas. A registry entry may borrow a caller's QN,
 * so copy the published strings at the common emission boundary. NULL-owner
 * legacy calls retain their existing allocation and borrowing behavior. */
static void c_publish_resolved_call(CLSPContext *ctx, CBMResolvedCall call) {
    if (ctx->test_definition_owners) {
        const char **strings[] = {&call.caller_qn, &call.callee_qn, &call.strategy, &call.reason};
        for (size_t i = 0; i < sizeof(strings) / sizeof(strings[0]); i++) {
            if (!*strings[i])
                continue;
            *strings[i] = cbm_arena_strdup(ctx->arena, *strings[i]);
            if (!*strings[i]) {
                ctx->test_owner_error = true;
                return; /* No row containing a borrowed or failed string escapes. */
            }
        }
    }
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, call);
}

static void c_emit_resolved_call_orig_at(CLSPContext *ctx, const char *callee_qn, const char *orig,
                                         const char *strategy, float confidence, TSNode site) {
    if (!ctx->resolved_calls || !callee_qn || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    // For a data-flow resolution (e.g. a function pointer `fp` resolved to its
    // target), `reason` carries the ORIGINAL textual callee name the LSP
    // resolved FROM, so the pipeline join can match the call site on that name
    // even though it differs from the resolved callee_qn's short name. `reason`
    // is otherwise NULL for resolved calls and is never read for them by the
    // pipeline consumers, so this overload is side-effect-free.
    rc.reason = orig;
    rc.source_origin = ctx->source_origin;
    if (!ts_node_is_null(site)) {
        rc.site_start_byte = ts_node_start_byte(site);
        rc.site_end_byte = ts_node_end_byte(site);
    }
    c_publish_resolved_call(ctx, rc);
}

static void c_emit_resolved_call_orig(CLSPContext *ctx, const char *callee_qn, const char *orig,
                                      const char *strategy, float confidence) {
    c_emit_resolved_call_orig_at(ctx, callee_qn, orig, strategy, confidence, (TSNode){0});
}

static void c_emit_resolved_call(CLSPContext *ctx, const char *callee_qn, const char *strategy,
                                 float confidence) {
    c_emit_resolved_call_orig(ctx, callee_qn, NULL, strategy, confidence);
}

static void c_emit_resolved_call_at(CLSPContext *ctx, const char *callee_qn, const char *strategy,
                                    float confidence, TSNode site) {
    c_emit_resolved_call_orig_at(ctx, callee_qn, NULL, strategy, confidence, site);
}

/* Is this node the object of an assignment or increment? `a->b = 1` writes b, and
 * `a->b.c = 1` writes c while only reading b. */
static bool c_written_lvalue(TSNode site) {
    TSNode node = site;
    TSNode parent = ts_node_parent(node);
    /* The member half of `a->b` stands for the whole access. */
    if (!ts_node_is_null(parent) && !strcmp(ts_node_type(parent), "field_expression")) {
        TSNode field = ts_node_child_by_field_name(parent, "field", 5);
        if (ts_node_is_null(field) || !ts_node_eq(field, node))
            return false;
        node = parent;
        parent = ts_node_parent(node);
    }
    while (!ts_node_is_null(parent) && !strcmp(ts_node_type(parent), "parenthesized_expression")) {
        node = parent;
        parent = ts_node_parent(node);
    }
    if (ts_node_is_null(parent))
        return false;
    const char *kind = ts_node_type(parent);
    if (!strcmp(kind, "update_expression"))
        return true;
    if (strcmp(kind, "assignment_expression"))
        return false;
    TSNode left = ts_node_child_by_field_name(parent, "left", 4);
    return !ts_node_is_null(left) && ts_node_eq(left, node);
}

static void c_emit_proven(CLSPContext *ctx, const cbm_resolve_result_t *proof,
                          const char *source_name, CBMResolvedKind kind, TSNode site) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn || ts_node_is_null(site) ||
        ts_node_has_error(site) || ts_node_end_byte(site) > ctx->proof_syntax_limit ||
        !cbm_resolve_admit(proof))
        return;
    const char *relation = kind == CBM_RESOLVED_INVOCATION ? "CALLS" : "CALL_REFERENCE";
    if (kind == CBM_RESOLVED_TYPE_REFERENCE)
        relation = "USAGE";
    if (kind == CBM_RESOLVED_VALUE_REFERENCE)
        relation = c_written_lvalue(site) ? "WRITES" : "USAGE";
    if (kind == CBM_RESOLVED_MACRO_INVOCATION)
        relation = "CALLS";
    if (kind == CBM_RESOLVED_MACRO_VALUE)
        relation = "USAGE";
    if (kind == CBM_RESOLVED_CALL_REFERENCE) {
        TSNode parent = ts_node_parent(site);
        const char *parent_kind = ts_node_is_null(parent) ? "" : ts_node_type(parent);
        if (!strcmp(parent_kind, "conditional_expression") ||
            !strcmp(parent_kind, "parenthesized_expression") ||
            !strcmp(parent_kind, "return_statement") || !strcmp(parent_kind, "binary_expression"))
            relation = "USAGE";
    }
    if (kind == CBM_RESOLVED_CALL_REFERENCE) {
        const char *node_kind = ts_node_type(site);
        if (!strcmp(node_kind, "qualified_identifier") || !strcmp(node_kind, "scoped_identifier")) {
            TSNode leaf = ts_node_child_by_field_name(site, "name", 4);
            if (!ts_node_is_null(leaf)) {
                site = leaf;
                source_name = c_node_text(ctx, leaf);
            }
        }
    }
    cbm_resolve_result_t occurrence = cbm_lsp_proof_occurrence(
        &ctx->field_types, *proof, site, ctx->enclosing_func_qn, source_name, relation);
    if (!cbm_resolve_admit(&occurrence))
        return;
    proof = &occurrence;
    CBMResolvedCall row = {.caller_qn = ctx->enclosing_func_qn,
                           .callee_qn = proof->target,
                           .strategy = "lsp_proven_callable",
                           .reason = source_name,
                           .confidence = 1.0f,
                           .kind = kind,
                           .site_start_byte = ts_node_start_byte(site),
                           .site_end_byte = ts_node_end_byte(site),
                           .source_origin = ctx->source_origin,
                           .evidence = {.analysis = proof->analysis,
                                        .rule = proof->rule,
                                        .facts = proof->facts,
                                        .proof = proof->proof}};
    c_publish_resolved_call(ctx, row);
}

/* FNV-1a includes the exact source occurrence. Hash collisions may omit a
 * duplicate candidate, but can never make one occurrence borrow another's
 * receiver proof. */
static uint64_t c_field_ref_hash(const char *caller_qn, const char *type_qn, const char *field_name,
                                 TSNode site, CBMSourceOrigin origin) {
    const char *parts[] = {caller_qn, type_qn, field_name};
    uint64_t hash = 1469598103934665603ULL;
    for (int part = 0; part < 3; part++) {
        for (const unsigned char *p = (const unsigned char *)parts[part]; *p; p++) {
            hash ^= (uint64_t)*p;
            hash *= 1099511628211ULL;
        }
        hash ^= 0xffu;
        hash *= 1099511628211ULL;
    }
    uint32_t site_key[] = {ts_node_start_byte(site), ts_node_end_byte(site), (uint32_t)origin};
    for (int i = 0; i < 3; i++) {
        for (int shift = 0; shift < 32; shift += 8) {
            hash ^= (site_key[i] >> shift) & 0xffu;
            hash *= 1099511628211ULL;
        }
    }
    return hash ? hash : 1ULL;
}

/* Lookup checks the nearest lexical binding and its current observed type.
 * A read consumes that existing declaration proof; re-observing the proof as
 * a new donation here would grow a derivation chain on every field read. */
static bool c_field_receiver_proven(CLSPContext *ctx, TSNode expression, const CBMType *observed,
                                    int depth, const char **canonical_type,
                                    const cbm_resolve_proof_t **receiver_proof) {
    if (depth > 16 || ts_node_is_null(expression) || ctx->eval_failure)
        return false;
    const char *kind = ts_node_type(expression);
    if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(expression) == 1)
        return c_field_receiver_proven(ctx, ts_node_named_child(expression, 0), observed, depth + 1,
                                       canonical_type, receiver_proof);
    if (!strcmp(kind, "cast_expression") || !strcmp(kind, "compound_literal_expression")) {
        /* These expressions select their type through explicit syntax. The
         * owner lookup below must still resolve that type exactly. */
        const CBMRegisteredType *owner = c_annotation_owner(ctx, expression);
        if (!owner || !owner->qualified_name)
            return false;
        owner = c_proof_field_class(ctx, owner->qualified_name, ts_node_start_byte(expression));
        if (!owner || cbm_type_is_unknown(observed))
            return false;
        cbm_resolve_result_t proof = cbm_lsp_proof_type_lookup(
            &ctx->field_types, c_node_text(ctx, expression), expression,
            "c-explicit-expression-type", owner->qualified_name, owner->declaration_identity,
            owner->graph_qualified_name, owner->source_flags, "explicit-cast-or-compound-type");
        proof =
            c_proof_type_import(ctx, owner->qualified_name, ts_node_start_byte(expression), proof);
        *canonical_type = owner->qualified_name;
        *receiver_proof = proof.proof;
        return cbm_resolve_admit(&proof);
    }
    if (strcmp(kind, "identifier") && strcmp(kind, "this"))
        return false;
    const char *name = c_node_text(ctx, expression);
    if (!name)
        return false;
    uint32_t at = ts_node_start_byte(expression);
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&ctx->field_types, ctx->current_scope, name, at);
    if (fact.status != CBM_RESOLVE_TYPE_KNOWN || fact.type != observed)
        return false;
    *canonical_type = fact.evidence.target;
    *receiver_proof = fact.evidence.proof;
    return *canonical_type != NULL && *receiver_proof != NULL;
}

static bool c_proof_type_visible(CLSPContext *ctx, const char *qn, uint32_t at) {
    /* A failed prepare stops before proof_includes exists, with include_count
     * already set, and the file's ordinary processing carries on past it. */
    if (ctx->binding_failed)
        return false;
    /* `Object$data` has no name to declare. It is reached through its owner, so it is visible
     * where the owner is. */
    const char *last = strrchr(qn, '.');
    const char *unnamed = strchr(last ? last : qn, '$');
    if (unnamed)
        qn = cbm_arena_strndup(ctx->arena, qn, (size_t)(unnamed - qn));
    const char *key = qn ? cbm_arena_sprintf(ctx->arena, "$type:%s", qn) : NULL;
    if (!key)
        return false;
    if (c_proof_declared(ctx, key, at))
        return true;
    return c_tu_owner(ctx, qn, at) != NULL;
}

static const CBMRegisteredType *c_proof_class_with(CLSPContext *ctx, const char *qn, uint32_t at,
                                                   uint32_t required);

/* The whole class is understood: a member name not listed here does not exist. */
static const CBMRegisteredType *c_proof_direct_class(CLSPContext *ctx, const char *qn,
                                                     uint32_t at) {
    return c_proof_class_with(ctx, qn, at,
                              CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_CLASS_DOMAIN_COMPLETE);
}

/* The class is exact. A field written in its own body is that field whatever else
 * the body holds: a directive, a nested type, a base class hidden by the member. */
static const CBMRegisteredType *c_proof_field_class(CLSPContext *ctx, const char *qn, uint32_t at) {
    return c_proof_class_with(ctx, qn, at, CBM_SOURCE_DECL_KNOWN);
}

static const CBMRegisteredType *c_proof_class_with(CLSPContext *ctx, const char *qn, uint32_t at,
                                                   uint32_t required) {
    if (!qn || !c_proof_type_visible(ctx, qn, at))
        return NULL;
    CBMTypeQNIter iterator;
    cbm_registry_types_with_qn(ctx->proof_registry, qn, &iterator);
    const CBMRegisteredType *chosen = NULL, *candidate;
    while ((candidate = cbm_type_qn_next(&iterator, NULL))) {
        if (c_type_is_self_alias(candidate))
            continue;
        if (!candidate->project_source || !candidate->declaration_identity ||
            !candidate->graph_qualified_name || (candidate->source_flags & required) != required ||
            candidate->alias_of || candidate->type_param_names)
            return NULL;
        if (chosen && (strcmp(chosen->declaration_identity, candidate->declaration_identity) ||
                       strcmp(chosen->graph_qualified_name, candidate->graph_qualified_name)))
            return NULL;
        chosen = candidate;
    }
    return iterator.complete ? chosen : NULL;
}

static cbm_resolve_method_candidate_t c_proof_method_candidate(
    const cbm_resolve_method_query_t *query, const CBMTypeRegistry *owner,
    const CBMRegisteredFunc *candidate, void *context) {
    (void)query;
    (void)owner;
    (void)context;
    cbm_resolve_method_candidate_t proof = {.exposed = CBM_RESOLVE_METHOD_YES};
    uint32_t required =
        CBM_SOURCE_DECL_KNOWN | CBM_SOURCE_DECL_PUBLIC | CBM_SOURCE_DECL_FIXED_ARITY;
    if ((candidate->source_flags & required) != required || candidate->type_param_names ||
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

static cbm_resolve_result_t c_proof_direct_method(CLSPContext *ctx, TSNode member, TSNode call) {
    cbm_resolve_result_t missing = c_proof_missing("cpp_member_domain_unproven");
    if (!ctx->cpp_mode || ctx->binding_failed || ts_node_has_error(call) ||
        ts_node_end_byte(call) > ctx->proof_syntax_limit)
        return missing;
    TSNode arguments = ts_node_child_by_field_name(call, "arguments", 9);
    if (ts_node_is_null(arguments) || ts_node_named_child_count(arguments))
        return missing;
    TSNode receiver = ts_node_child_by_field_name(member, "argument", 8);
    TSNode field = ts_node_child_by_field_name(member, "field", 5);
    if (ts_node_is_null(receiver) || ts_node_is_null(field) ||
        strcmp(ts_node_type(receiver), "identifier") ||
        strcmp(ts_node_type(field), "field_identifier"))
        return missing;
    const CBMType *observed = c_eval_expr_type(ctx, receiver);
    const char *qn = NULL;
    const cbm_resolve_proof_t *receiver_proof = NULL;
    if (!observed || !c_field_receiver_proven(ctx, receiver, observed, 0, &qn, &receiver_proof))
        return missing;
    const CBMRegisteredType *type = c_proof_direct_class(ctx, qn, ts_node_start_byte(member));
    if (!type)
        return missing;
    const char *name = c_node_text(ctx, field);
    if (!name)
        return missing;
    bool arrow = false;
    uint32_t children = ts_node_child_count(member);
    for (uint32_t i = 0; i < children; i++)
        arrow |= !strcmp(ts_node_type(ts_node_child(member, i)), "->");
    if (arrow && observed->kind != CBM_TYPE_POINTER)
        return missing; /* An overloaded operator-> needs its own return-type proof. */
    cbm_resolve_type_fact_t fact = {
        .origin = CBM_RESOLVE_TYPE_OBSERVED_BINDING,
        .proof_pool = &ctx->field_types.proofs,
        .status = CBM_RESOLVE_TYPE_KNOWN,
        .type = cbm_type_named(ctx->arena, type->qualified_name),
        .evidence = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                     .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                     .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                     .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                     .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                              CBM_RESOLVE_FACT_TYPE_BOUND | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
                     .target = type->graph_qualified_name,
                     .candidate_count = 1,
                     .proof = receiver_proof}};
    cbm_resolve_method_query_t query = {.proof_pool = &ctx->field_types.proofs,
                                        .proof_source = &ctx->field_types.proof_source,
                                        .lookup_domain = type->qualified_name,
                                        .registry = ctx->proof_registry,
                                        .receiver = &fact,
                                        .receiver_qn = type->qualified_name,
                                        .method_name = name,
                                        .argument_count = 0,
                                        .adapter_facts = CBM_RESOLVE_METHOD_DIRECT_DOMAIN_COMPLETE |
                                                         CBM_RESOLVE_METHOD_FIXED_ARITY |
                                                         CBM_RESOLVE_METHOD_OCCURRENCE_EXACT,
                                        .classify = c_proof_method_candidate};
    return cbm_resolve_method(&query).result;
}

/* Publish one exact field identifier or invocation occurrence. An unknown
 * owner, unsupported type derivation or incomplete inherited lookup emits no
 * proof; the shared resolver records the extracted occurrence as unresolved. */
static void c_publish_field_owner_as(CLSPContext *ctx, const char *type_qn, const char *field_name,
                                     TSNode site, const cbm_resolve_proof_t *receiver_proof,
                                     CBMResolvedKind kind) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn || !type_qn || !field_name ||
        !field_name[0] || ts_node_is_null(site) || ctx->eval_failure)
        return;
    bool complete = false;
    const CBMRegisteredType *owner = c_lookup_field_owner(ctx, type_qn, field_name, 0, &complete);
    if (!complete || !owner || !owner->qualified_name)
        return;
    if (!c_u64_set_insert(&ctx->field_ref_seen, &ctx->field_ref_seen_cap,
                          &ctx->field_ref_seen_count,
                          c_field_ref_hash(ctx->enclosing_func_qn, type_qn, field_name, site,
                                           ctx->source_origin)))
        return;
    CBMResolvedCall resolved = {0};
    resolved.caller_qn = ctx->enclosing_func_qn;
    resolved.callee_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", owner->qualified_name, field_name);
    resolved.strategy = "lsp_field_access";
    resolved.confidence = 0.95f;
    resolved.kind = kind;
    resolved.reason = field_name;
    resolved.site_start_byte = ts_node_start_byte(site);
    resolved.site_end_byte = ts_node_end_byte(site);
    resolved.source_origin = ctx->source_origin;
    resolved.evidence = (cbm_resolve_evidence_t){
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT};
    if (receiver_proof) {
        const CBMRegisteredType *exact =
            c_proof_field_class(ctx, type_qn, ts_node_start_byte(site));
        unsigned matches = 0, slot = 0;
        if (exact && exact == owner && exact->field_names) {
            unsigned i = 0;
            for (; i < 16384 && exact->field_names[i]; i++)
                if (!strcmp(exact->field_names[i], field_name)) {
                    matches++;
                    slot = i;
                }
            if (i == 16384)
                matches = 0;
        }
        if (matches == 1) {
            const char *detail = cbm_arena_sprintf(
                ctx->arena, "direct-field;slot=%u;source-flags=%u;complete-owner-domain", slot,
                exact->source_flags);
            cbm_resolve_proof_key_t key = {.lookup_domain = type_qn,
                                           .lookup_key = field_name,
                                           .declaration_identity = exact->declaration_identity,
                                           .binding_identity = exact->graph_qualified_name,
                                           .detail = "registered-field-owner"};
            cbm_resolve_result_t declaration = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                                .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                                .target = exact->graph_qualified_name,
                                                .reason = "registered-field-owner"};
            const cbm_resolve_proof_t *owner_proof =
                cbm_resolve_proof_leaf(&ctx->field_types.proofs, CBM_PROOF_REGISTERED_DECLARATION,
                                       NULL, &key, &declaration);
            cbm_resolve_result_t proof = {.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                          .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                          .analysis = resolved.evidence.analysis,
                                          .rule = resolved.evidence.rule,
                                          .facts = resolved.evidence.facts,
                                          .target = resolved.callee_qn,
                                          .candidate_count = 1,
                                          .proof = receiver_proof};
            if (detail)
                proof = cbm_lsp_proof_rule(&ctx->field_types, proof, site, type_qn, field_name,
                                           detail, owner_proof);
            else
                proof.analysis = CBM_RESOLVE_ANALYSIS_FAILED;
            /* This producer establishes a field value occurrence. Whether it is
             * written is read here from the AST, the same way the extractor's
             * assignment record does, and a write also serves as a usage. */
            proof =
                cbm_lsp_proof_occurrence(&ctx->field_types, proof, site, ctx->enclosing_func_qn,
                                         field_name, c_written_lvalue(site) ? "WRITES" : "USAGE");
            resolved.evidence = cbm_lsp_proof_evidence(proof);
        }
    }
    if (resolved.callee_qn)
        c_publish_resolved_call(ctx, resolved);
}

static void c_publish_field_owner(CLSPContext *ctx, const char *type_qn, const char *field_name,
                                  TSNode site, const cbm_resolve_proof_t *receiver_proof) {
    c_publish_field_owner_as(ctx, type_qn, field_name, site, receiver_proof,
                             CBM_RESOLVED_FIELD_REFERENCE);
}

static bool c_name_bound_locally(CLSPContext *ctx, const char *name);

/* The base type name written in a field's declaration: `struct Ops *` is Ops. */
static const char *c_field_type_name(const CBMType *type) {
    for (int hop = 0; type && hop < 8; hop++) {
        if (type->kind == CBM_TYPE_POINTER) {
            type = type->data.pointer.elem;
        } else if (type->kind == CBM_TYPE_SLICE) {
            type = type->data.slice.elem;
        } else if (type->kind == CBM_TYPE_REFERENCE || type->kind == CBM_TYPE_RVALUE_REF) {
            type = type->data.reference.elem;
        } else if (type->kind == CBM_TYPE_NAMED) {
            const char *qn = type->data.named.qualified_name;
            const char *dot = qn ? strrchr(qn, '.') : NULL;
            return dot ? dot + 1 : qn;
        } else if (type->kind == CBM_TYPE_TEMPLATE) {
            /* `Optional<int> Entries`: the template. c_proof_named_class refuses one with a
             * specialization anywhere, which may replace its members for these arguments. */
            const char *qn = type->data.template_type.template_name;
            const char *dot = qn ? strrchr(qn, '.') : NULL;
            return dot ? dot + 1 : qn;
        } else {
            return NULL;
        }
    }
    return NULL;
}

/* `template <class Value> class Pair { Value first; };`: first has whatever type the instantiation
 * supplies, and a project class spelled Value is not it. A field of that kind is still a field, and
 * nothing read through it is known. */
static bool c_type_is_parameter_of(CLSPContext *ctx, const char *klass, const CBMType *written) {
    const char *name = klass ? c_field_type_name(written) : NULL;
    if (!name)
        return false;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind == CBM_C_DECL_TEMPLATE_PARAMETER && !strcmp(decl->name, name) &&
            decl->type_name && !strcmp(decl->type_name, klass))
            return true;
    }
    return false;
}

/* The written type of a file-scope object this unit sees. Every declaration of an
 * object has one type, so a disagreement says this is not one object. */
static const char *c_proof_value_type_name(CLSPContext *ctx, const char *name, uint32_t at) {
    const char *type_name = NULL;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if ((decl->kind != CBM_C_DECL_EXTERN_VALUE && decl->kind != CBM_C_DECL_VALUE) ||
            strcmp(decl->name, name) || !decl->type_name)
            continue;
        if (strcmp(decl->file, c_own_file_key(ctx))) {
            const CTUModule *entry = c_tu_find(ctx, decl->file, strlen(decl->file));
            if (!entry || entry->exports_only || entry->at >= at)
                continue;
        }
        if (type_name && strcmp(type_name, decl->type_name))
            return NULL;
        type_name = decl->type_name;
    }
    return type_name;
}

static bool c_cxx_rules(const CLSPContext *ctx) {
    return ctx->cpp_mode && !ctx->plain_header;
}

static bool c_member_is_arrow(TSNode member) {
    uint32_t count = ts_node_child_count(member);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_child(member, i);
        if (!ts_node_is_named(child) && !strcmp(ts_node_type(child), "->"))
            return true;
    }
    return false;
}

static const CBMType *c_shape_unwrap(const CBMType *shape) {
    while (shape && (shape->kind == CBM_TYPE_REFERENCE || shape->kind == CBM_TYPE_RVALUE_REF))
        shape = shape->data.reference.elem;
    return shape;
}

/* In C++ `->` and `[]` may be overloaded operators, whose result is a different type.
 * Only a written pointer or array makes them the builtin ones. */
static bool c_cxx_access_is_builtin(const CLSPContext *ctx, TSNode member, const CBMType *shape) {
    if (!c_cxx_rules(ctx))
        return true;
    shape = c_shape_unwrap(shape);
    bool pointer = shape && (shape->kind == CBM_TYPE_POINTER || shape->kind == CBM_TYPE_SLICE);
    return c_member_is_arrow(member) ? pointer : (shape && !pointer);
}

static const CBMRegisteredType *c_proof_named_class(CLSPContext *ctx, const char *type_name,
                                                    uint32_t at) {
    if (type_name && c_template_is_specialized(ctx, type_name))
        return NULL;
    const CBMRegisteredType *owner =
        type_name ? c_proof_follow_alias(ctx, c_proof_type_by_name(ctx, type_name, at)) : NULL;
    return owner ? c_proof_field_class(ctx, owner->qualified_name, at) : NULL;
}

/* A base as the extractor wrote it, maybe `Base<T>` or `ns::Base`: its plain name. */
static const char *c_base_class_name(CLSPContext *ctx, const char *base) {
    size_t length = strcspn(base, "<");
    const char *start = base;
    for (size_t i = 0; i < length; i++)
        if (base[i] == '.' || base[i] == ':')
            start = base + i + 1;
    return cbm_arena_strndup(ctx->arena, start, length - (size_t)(start - base));
}

/* The class that declares this field, the class itself first: a member hides what its
 * bases declare, and a valid program cannot name one that two bases both declare. */
static const CBMRegisteredType *c_proof_field_declarer(CLSPContext *ctx,
                                                       const CBMRegisteredType *klass,
                                                       const char *name, uint32_t at, int depth,
                                                       const CBMType **type) {
    if (!klass || depth > 8)
        return NULL;
    for (unsigned i = 0; klass->field_names && i < 16384 && klass->field_names[i]; i++) {
        if (strcmp(klass->field_names[i], name))
            continue;
        if (type) {
            *type = klass->field_types ? klass->field_types[i] : NULL;
            if (c_type_is_parameter_of(ctx, klass->short_name, *type))
                *type = NULL;
        }
        return klass;
    }
    for (int i = 0; klass->embedded_types && klass->embedded_types[i]; i++) {
        const char *base_name = c_base_class_name(ctx, klass->embedded_types[i]);
        if (c_base_may_depend(ctx, base_name))
            continue; /* a dependent base is not searched by an unqualified name */
        const CBMRegisteredType *base = c_proof_named_class(ctx, base_name, at);
        const CBMRegisteredType *found =
            c_proof_field_declarer(ctx, base, name, at, depth + 1, type);
        if (found)
            return found;
    }
    return NULL;
}

/* Written proof that the member function's own class is this class. */
static cbm_resolve_result_t c_proof_method_class_lookup(CLSPContext *ctx, TSNode site) {
    const CBMRegisteredType *klass = ctx->proof_method_class;
    cbm_resolve_result_t proof = cbm_lsp_proof_type_lookup(
        &ctx->field_types, klass->short_name, site, "c-enclosing-class", klass->qualified_name,
        klass->declaration_identity, klass->graph_qualified_name, klass->source_flags,
        "member-function-of-this-class");
    return c_proof_type_import(ctx, klass->qualified_name, ts_node_start_byte(site), proof);
}

typedef struct {
    const char *target; /* the one member found so far */
    bool conflict;      /* several members fit and nothing tells them apart */
    bool open;          /* a base class is not understood, so more may exist */
} CMemberHit;

static void c_proof_class_method(CLSPContext *ctx, const CBMRegisteredType *klass, const char *name,
                                 int argc, uint32_t at, int depth, bool from_own_class,
                                 CMemberHit *hit);
static int c_call_argument_count(TSNode call);

/* The class a selected member function is written to return. */
static const CBMRegisteredType *c_proof_returned_class(CLSPContext *ctx, const char *target,
                                                       const cbm_resolve_proof_t *receiver,
                                                       uint32_t at,
                                                       const cbm_resolve_proof_t **proof,
                                                       const CBMType **shape) {
    size_t base = strcspn(target, "(");
    const char *qn = target[base] ? cbm_arena_strndup(ctx->arena, target, base) : target;
    const CBMRegisteredFunc *function =
        qn ? cbm_registry_lookup_func(ctx->proof_registry, qn) : NULL;
    const CBMType *written = function && function->signature &&
                                     function->signature->kind == CBM_TYPE_FUNC &&
                                     function->signature->data.func.return_types
                                 ? function->signature->data.func.return_types[0]
                                 : NULL;
    /* `Value get();` in a template returns what the instantiation supplies. */
    const char *owner_end = qn ? strrchr(qn, '.') : NULL;
    const char *owner_start = owner_end ? owner_end : NULL;
    while (owner_start && owner_start > qn && owner_start[-1] != '.')
        owner_start--;
    const char *klass =
        owner_end && owner_start < owner_end
            ? cbm_arena_strndup(ctx->arena, owner_start, (size_t)(owner_end - owner_start))
            : NULL;
    if (c_type_is_parameter_of(ctx, klass, written))
        return NULL;
    const CBMRegisteredType *owner = c_proof_named_class(ctx, c_field_type_name(written), at);
    if (!owner)
        return NULL;
    *proof = receiver;
    *shape = written;
    return owner;
}

/* A type written at the cast: its own declarator says how many `*` it has. */
static const CBMRegisteredType *c_proof_written_cast(CLSPContext *ctx, TSNode site,
                                                     const CBMRegisteredType *written,
                                                     TSNode descriptor,
                                                     const cbm_resolve_proof_t **proof,
                                                     const CBMType **shape) {
    uint32_t at = ts_node_start_byte(site);
    const CBMRegisteredType *owner =
        written ? c_proof_field_class(ctx, written->qualified_name, at) : NULL;
    if (!owner)
        return NULL;
    cbm_resolve_result_t typed = cbm_lsp_proof_type_lookup(
        &ctx->field_types, c_node_text(ctx, site), site, "c-explicit-expression-type",
        owner->qualified_name, owner->declaration_identity, owner->graph_qualified_name,
        owner->source_flags, "explicit-cast-type");
    typed = c_proof_type_import(ctx, owner->qualified_name, at, typed);
    if (!cbm_resolve_admit(&typed))
        return NULL;
    const CBMType *type = cbm_type_named(ctx->arena, owner->qualified_name);
    TSNode declarator = ts_node_is_null(descriptor)
                            ? descriptor
                            : ts_node_child_by_field_name(descriptor, "declarator", 10);
    for (int hop = 0; hop < 8 && !ts_node_is_null(declarator); hop++) {
        if (strcmp(ts_node_type(declarator), "abstract_pointer_declarator"))
            break;
        type = cbm_type_pointer(ctx->arena, type);
        declarator = ts_node_child_by_field_name(declarator, "declarator", 10);
    }
    *proof = typed.proof;
    *shape = type;
    return owner;
}

static uint32_t c_macro_use_arguments(TSNode call, TSNode *out, uint32_t cap);

/* `hl_table[i].x` with `#define hl_table ((HlGroup *)(ga.ga_data))`, `SDS_HDR(8, s)->len` with
 * `#define SDS_HDR(T, s) ((struct sdshdr##T *)...)`: whatever the operand is, the replacement
 * writes the type. Every definition in force has to write the same one. */
static const CBMRegisteredType *c_macro_cast_owner(CLSPContext *ctx, const char *macro, TSNode site,
                                                   TSNode call, const cbm_resolve_proof_t **proof,
                                                   const CBMType **shape) {
    uint32_t at = ts_node_start_byte(site);
    if (!ctx->c_decls || !macro || c_name_bound_locally(ctx, macro) ||
        !c_proof_macro_visible(ctx, macro, at))
        return NULL;
    const CMacroDefs *defs = c_macro_defs(ctx, macro);
    const char *own = c_own_file_key(ctx);
    const CBMCMacroShape *agreed = NULL;
    for (int d = 0; d < defs->count; d++) {
        const CBMCDecl *decl = defs->items[d];
        if (defs->from[d] >= at && strcmp(decl->file, own))
            continue;
        const CBMCMacroShape *written = decl->macro;
        if (c_macro_definition_unproven(ctx, decl, at) || !written || !written->cast_type ||
            written->object_like != ts_node_is_null(call))
            return NULL;
        if (agreed && (strcmp(agreed->cast_type, written->cast_type) ||
                       agreed->cast_pointers != written->cast_pointers ||
                       agreed->cast_pastes != written->cast_pastes ||
                       agreed->cast_param != written->cast_param))
            return NULL;
        agreed = written;
    }
    if (!agreed)
        return NULL;
    const char *name = agreed->cast_type;
    if (agreed->cast_pastes) {
        TSNode arguments[32];
        uint32_t count = c_macro_use_arguments(call, arguments, 32);
        const char *argument =
            agreed->cast_param < count ? c_node_text(ctx, arguments[agreed->cast_param]) : NULL;
        for (const char *c = argument; c && *c; c++)
            if (!isalnum((unsigned char)*c) && *c != '_')
                argument = NULL;
        name = argument ? cbm_arena_sprintf(ctx->arena, "%s%s", name, argument) : NULL;
    }
    const CBMRegisteredType *owner = name ? c_proof_named_class(ctx, name, at) : NULL;
    if (!owner)
        return NULL;
    cbm_resolve_result_t typed = cbm_lsp_proof_type_lookup(
        &ctx->field_types, macro, site, "c-macro-replacement-cast", owner->qualified_name,
        owner->declaration_identity, owner->graph_qualified_name, owner->source_flags,
        "cast-written-in-the-replacement");
    typed = c_proof_type_import(ctx, owner->qualified_name, at, typed);
    if (!cbm_resolve_admit(&typed))
        return NULL;
    const CBMType *type = cbm_type_named(ctx->arena, owner->qualified_name);
    for (int i = 0; i < agreed->cast_pointers; i++)
        type = cbm_type_pointer(ctx->arena, type);
    *proof = typed.proof;
    *shape = type;
    return owner;
}

/* The struct an expression denotes, as the written declarations along it establish
 * it: a typed name, an extern object, a member of a known struct, or any of those
 * under `*`, `&`, `[]` and parentheses. No initializer is consulted and nothing
 * about the value flows in, so reassignment cannot make it stale. The shape is the
 * written type of the expression so far, which C++ needs to read `->` and `[]`. */
static const CBMRegisteredType *c_proof_expression_owner(CLSPContext *ctx, TSNode node, int depth,
                                                         const cbm_resolve_proof_t **proof,
                                                         const CBMType **shape) {
    if (depth > 16 || ts_node_is_null(node) || ctx->eval_failure)
        return NULL;
    const char *kind = ts_node_type(node);
    uint32_t at = ts_node_start_byte(node);
    if (!strcmp(kind, "parenthesized_expression") && ts_node_named_child_count(node) == 1)
        return c_proof_expression_owner(ctx, ts_node_named_child(node, 0), depth + 1, proof, shape);
    if (!strcmp(kind, "identifier")) {
        const CBMRegisteredType *cast =
            c_macro_cast_owner(ctx, c_node_text(ctx, node), node, (TSNode){0}, proof, shape);
        if (cast)
            return cast;
    } else if (!strcmp(kind, "call_expression")) {
        TSNode callee = ts_node_child_by_field_name(node, "function", 8);
        const CBMRegisteredType *cast =
            !ts_node_is_null(callee) && !strcmp(ts_node_type(callee), "identifier")
                ? c_macro_cast_owner(ctx, c_node_text(ctx, callee), node, node, proof, shape)
                : NULL;
        if (cast)
            return cast;
    }
    if (!strcmp(kind, "subscript_expression") || !strcmp(kind, "pointer_expression")) {
        const CBMRegisteredType *base = c_proof_expression_owner(
            ctx, ts_node_child_by_field_name(node, "argument", 8), depth + 1, proof, shape);
        if (!base)
            return NULL;
        TSNode op = ts_node_child_by_field_name(node, "operator", 8);
        const char *op_text = ts_node_is_null(op) ? NULL : c_node_text(ctx, op);
        if (op_text && !strcmp(op_text, "&")) {
            *shape = *shape ? cbm_type_pointer(ctx->arena, *shape) : NULL;
            return base;
        }
        const CBMType *written = c_shape_unwrap(*shape);
        bool indexable =
            written && (written->kind == CBM_TYPE_POINTER || written->kind == CBM_TYPE_SLICE);
        if (c_cxx_rules(ctx) && !indexable)
            return NULL;
        *shape = !indexable                          ? NULL
                 : written->kind == CBM_TYPE_POINTER ? written->data.pointer.elem
                                                     : written->data.slice.elem;
        return base;
    }
    if (!strcmp(kind, "field_expression")) {
        const CBMRegisteredType *base = c_proof_expression_owner(
            ctx, ts_node_child_by_field_name(node, "argument", 8), depth + 1, proof, shape);
        TSNode field = ts_node_child_by_field_name(node, "field", 5);
        const char *name = !base || ts_node_is_null(field) ? NULL : c_node_text(ctx, field);
        if (!name || !c_cxx_access_is_builtin(ctx, node, *shape))
            return NULL;
        const CBMType *written = NULL;
        if (!c_proof_field_declarer(ctx, base, name, at, 0, &written))
            return NULL;
        *shape = written;
        return c_proof_named_class(ctx, c_field_type_name(written), at);
    }
    if (!strcmp(kind, "cast_expression")) {
        const CBMRegisteredType *written = c_annotation_owner(ctx, node);
        TSNode descriptor = ts_node_child_by_field_name(node, "type", 4);
        return c_proof_written_cast(ctx, node, written, descriptor, proof, shape);
    }
    if (!strcmp(kind, "call_expression")) {
        /* `static_cast<Foo *>(p)`: the type is written, whatever p was. */
        TSNode cast_callee = ts_node_child_by_field_name(node, "function", 8);
        if (!ts_node_is_null(cast_callee) &&
            !strcmp(ts_node_type(cast_callee), "template_function")) {
            const char *word = c_template_id_name(ctx, cast_callee);
            TSNode arguments = ts_node_child_by_field_name(cast_callee, "arguments", 9);
            if (word && !ts_node_is_null(arguments) && ts_node_named_child_count(arguments) == 1 &&
                (!strcmp(word, "static_cast") || !strcmp(word, "const_cast") ||
                 !strcmp(word, "reinterpret_cast") || !strcmp(word, "dynamic_cast"))) {
                TSNode descriptor = ts_node_named_child(arguments, 0);
                const CBMRegisteredType *written = c_annotation_owner(ctx, descriptor);
                return c_proof_written_cast(ctx, node, written, descriptor, proof, shape);
            }
        }
        /* `V.getOperand(0).getOpcode()`: the member selected in the receiver's class, and what
         * that member is written to return. */
        TSNode member = ts_node_child_by_field_name(node, "function", 8);
        if (c_cxx_rules(ctx) && !ts_node_is_null(member) &&
            !strcmp(ts_node_type(member), "field_expression")) {
            const CBMType *receiver_shape = NULL;
            const cbm_resolve_proof_t *receiver_proof = NULL;
            const CBMRegisteredType *receiver =
                c_proof_expression_owner(ctx, ts_node_child_by_field_name(member, "argument", 8),
                                         depth + 1, &receiver_proof, &receiver_shape);
            TSNode field = ts_node_child_by_field_name(member, "field", 5);
            const char *written = ts_node_is_null(field) ? NULL : c_node_text(ctx, field);
            if (!receiver || !receiver_proof || !written ||
                !c_cxx_access_is_builtin(ctx, member, receiver_shape))
                return NULL;
            CMemberHit hit = {0};
            c_proof_class_method(ctx, receiver, written, c_call_argument_count(node), at, 0, false,
                                 &hit);
            return !hit.target || hit.conflict
                       ? NULL
                       : c_proof_returned_class(ctx, hit.target, receiver_proof, at, proof, shape);
        }
        /* What a call returns is written on the function it selects. */
        ctx->proof_class_scope_checked = false;
        cbm_resolve_result_t callee =
            c_callable_proof(ctx, ts_node_child_by_field_name(node, "function", 8));
        const CBMRegisteredFunc *function =
            cbm_resolve_admit(&callee)
                ? cbm_registry_lookup_func(ctx->proof_registry, callee.target)
                : NULL;
        const CBMType *written = function && function->signature &&
                                         function->signature->kind == CBM_TYPE_FUNC &&
                                         function->signature->data.func.return_types
                                     ? function->signature->data.func.return_types[0]
                                     : NULL;
        const CBMRegisteredType *owner = c_proof_named_class(ctx, c_field_type_name(written), at);
        if (!owner)
            return NULL;
        *proof = callee.proof;
        *shape = written;
        return owner;
    }
    if (!strcmp(kind, "this")) {
        if (!ctx->proof_method_class)
            return NULL;
        cbm_resolve_result_t typed = c_proof_method_class_lookup(ctx, node);
        if (!cbm_resolve_admit(&typed))
            return NULL;
        *proof = typed.proof;
        *shape = cbm_type_pointer(
            ctx->arena, cbm_type_named(ctx->arena, ctx->proof_method_class->qualified_name));
        return ctx->proof_method_class;
    }
    if (strcmp(kind, "identifier"))
        return NULL;
    const char *name = c_node_text(ctx, node);
    if (!name)
        return NULL;
    cbm_resolve_type_fact_t fact =
        cbm_resolve_types_lookup(&ctx->field_types, ctx->current_scope, name, at);
    if (fact.status == CBM_RESOLVE_TYPE_KNOWN && fact.evidence.target && fact.evidence.proof) {
        *proof = fact.evidence.proof;
        *shape = fact.type;
        return c_proof_field_class(ctx, fact.evidence.target, at);
    }
    /* A name bound in this function that the fact above does not type is opaque. */
    if (c_name_bound_locally(ctx, name))
        return NULL;
    /* `Name.size()` inside a member function reads this class's own field. */
    if (ctx->proof_method_class) {
        const CBMType *written = NULL;
        if (!c_proof_field_declarer(ctx, ctx->proof_method_class, name, at, 0, &written))
            return NULL;
        const CBMRegisteredType *owner = c_proof_named_class(ctx, c_field_type_name(written), at);
        cbm_resolve_result_t typed =
            owner ? c_proof_method_class_lookup(ctx, node) : (cbm_resolve_result_t){0};
        if (!owner || !cbm_resolve_admit(&typed))
            return NULL;
        *proof = typed.proof;
        *shape = written;
        return owner;
    }
    const char *type_name = c_proof_value_type_name(ctx, name, at);
    const CBMRegisteredType *owner = c_proof_named_class(ctx, type_name, at);
    if (!owner)
        return NULL;
    cbm_resolve_result_t typed = cbm_lsp_proof_type_lookup(
        &ctx->field_types, name, node, "c-extern-object-type", owner->qualified_name,
        owner->declaration_identity, owner->graph_qualified_name, owner->source_flags,
        "visible-extern-declaration;written-type");
    typed = c_proof_type_import(ctx, owner->qualified_name, at, typed);
    if (!cbm_resolve_admit(&typed))
        return NULL;
    *proof = typed.proof;
    *shape = NULL;
    return owner;
}

/* `wp->w_p_list` with `#define w_p_list w_onebuf_opt.wo_list`, and `buf->b_fnum` with
 * `#define b_fnum handle`: what the compiler reads after the arrow is the replacement, one member
 * after another from the receiver. The field the source spells does not exist. Every definition in
 * force has to spell the same path, and a step the class does not have ends the walk. */
static bool c_field_is_macro_path(CLSPContext *ctx, TSNode argument, TSNode field,
                                  const char *field_name) {
    uint32_t at = ts_node_start_byte(field);
    if (!c_proof_macro_visible(ctx, field_name, at))
        return false;
    const CMacroDefs *defs = c_macro_defs(ctx, field_name);
    const char *own = c_own_file_key(ctx);
    const char *path = NULL;
    for (int d = 0; d < defs->count; d++) {
        const CBMCDecl *decl = defs->items[d];
        if (defs->from[d] >= at && strcmp(decl->file, own))
            continue;
        if (c_macro_definition_unproven(ctx, decl, at))
            return true;
        const char *written = decl->macro ? decl->macro->field_path : NULL;
        if (!written || (path && strcmp(path, written)))
            return false;
        path = written;
    }
    if (!path)
        return false;
    const cbm_resolve_proof_t *proof = NULL;
    const CBMType *shape = NULL;
    const CBMRegisteredType *owner = c_proof_expression_owner(ctx, argument, 0, &proof, &shape);
    const char *rest = path;
    while (owner && proof && *rest) {
        size_t length = strcspn(rest, ".");
        const char *name = cbm_arena_strndup(ctx->arena, rest, length);
        const CBMType *type = NULL;
        const CBMRegisteredType *declarer =
            name ? c_proof_field_declarer(ctx, owner, name, at, 0, &type) : NULL;
        if (!declarer)
            break;
        c_publish_field_owner_as(ctx, declarer->qualified_name, name, field, proof,
                                 CBM_RESOLVED_MACRO_VALUE);
        rest += length + (rest[length] == '.');
        owner = *rest ? c_proof_named_class(ctx, c_field_type_name(type), at) : NULL;
    }
    return true;
}

static void c_emit_field_reference(CLSPContext *ctx, TSNode node) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn)
        return;
    TSNode argument = ts_node_child_by_field_name(node, "argument", 8);
    TSNode field = ts_node_child_by_field_name(node, "field", 5);
    if (ts_node_is_null(argument) || ts_node_is_null(field) ||
        strcmp(ts_node_type(field), "field_identifier"))
        return;
    char *field_name = c_node_text(ctx, field);
    if (!field_name || !field_name[0])
        return;
    if (c_field_is_macro_path(ctx, argument, field, field_name))
        return;
    const cbm_resolve_proof_t *owner_proof = NULL;
    const CBMType *owner_shape = NULL;
    const CBMRegisteredType *owner =
        c_proof_expression_owner(ctx, argument, 0, &owner_proof, &owner_shape);
    if (owner && owner_proof && c_cxx_access_is_builtin(ctx, node, owner_shape)) {
        const CBMRegisteredType *declarer =
            c_proof_field_declarer(ctx, owner, field_name, ts_node_start_byte(node), 0, NULL);
        if (declarer) {
            c_publish_field_owner(ctx, declarer->qualified_name, field_name, field, owner_proof);
            return;
        }
    }
    const CBMType *object_type = c_eval_expr_type(ctx, argument);
    const char *type_qn = NULL;
    const cbm_resolve_proof_t *receiver_proof = NULL;
    if (cbm_type_is_unknown(object_type) || !c_cxx_access_is_builtin(ctx, node, object_type) ||
        !c_field_receiver_proven(ctx, argument, object_type, 0, &type_qn, &receiver_proof))
        return;
    bool is_arrow = false;
    uint32_t count = ts_node_child_count(node);
    for (uint32_t i = 0; i < count && !is_arrow; i++) {
        TSNode child = ts_node_child(node, i);
        is_arrow = !ts_node_is_named(child) && !strcmp(ts_node_type(child), "->");
    }
    const CBMType *base = c_simplify_type(ctx, object_type, is_arrow);
    if (!base || base->kind != CBM_TYPE_NAMED)
        return;
    c_publish_field_owner(ctx, type_qn, field_name, field, receiver_proof);
}

/* ── Designated initializers ─────────────────────────────────────────
 * `.open = my_open` names a member with no object expression: the type being
 * initialized says whose member it is. That type is known where the list
 * starts (a declarator, a compound literal, an enclosing list) and is handed
 * down the walk; nothing here looks upward, so a table of ten thousand entries
 * costs ten thousand steps. */

/* Hand `type` to the walk of `list`, when `list` is an initializer list. */
static void c_hint_initializer_list(CLSPContext *ctx, TSNode list, const CBMType *type) {
    if (ts_node_is_null(list) || !type || cbm_type_is_unknown(type) ||
        strcmp(ts_node_type(list), "initializer_list") != 0)
        return;
    ctx->init_list_type = type;
    ctx->init_list_id = list.id;
}

/* `Type name[N] = {...}` / `Type *name = ...`: the declared type of one
 * init_declarator, its declarator's pointer and array levels applied to the
 * declaration's base type. */
static void c_hint_declared_initializer(CLSPContext *ctx, TSNode init_declarator) {
    TSNode value = ts_node_child_by_field_name(init_declarator, "value", 5);
    if (ts_node_is_null(value) || strcmp(ts_node_type(value), "initializer_list") != 0)
        return;
    TSNode declaration = ts_node_parent(init_declarator);
    if (ts_node_is_null(declaration) || strcmp(ts_node_type(declaration), "declaration") != 0)
        return;
    const CBMType *type = c_parse_declaration_type(ctx, declaration);
    TSNode decl = ts_node_child_by_field_name(init_declarator, "declarator", 10);
    for (int depth = 0; depth < 8 && !ts_node_is_null(decl); depth++) {
        const char *dk = ts_node_type(decl);
        if (strcmp(dk, "identifier") == 0) {
            c_hint_initializer_list(ctx, value, type);
            return;
        }
        if (strcmp(dk, "pointer_declarator") == 0)
            type = cbm_type_pointer(ctx->arena, type);
        else if (strcmp(dk, "array_declarator") == 0)
            type = cbm_type_slice(ctx->arena, type);
        else
            return; /* a parenthesized or function declarator: not followed */
        decl = ts_node_child_by_field_name(decl, "declarator", 10);
    }
}

/* One `.name` step of a designator on a value of type `type`: publish the
 * member's owner and return the member's type (NULL when unknown). */
static const CBMType *c_designate_member(CLSPContext *ctx, const CBMType *type, TSNode name_node) {
    if (!type || ts_node_is_null(name_node))
        return NULL;
    const char *type_qn = type_to_qn(c_simplify_type(ctx, type, false));
    char *name = c_node_text(ctx, name_node);
    if (!type_qn || !name || !name[0])
        return NULL;
    /* The type being initialized is the receiver. It is proven the way a declared variable's type
     * is: the name visible here means this class and no other. The walk's own type only knows the
     * name, qualified with this module for want of better. */
    uint32_t at = ts_node_start_byte(name_node);
    const char *dot = strrchr(type_qn, '.');
    const CBMRegisteredType *owner = c_proof_named_class(ctx, dot ? dot + 1 : type_qn, at);
    const cbm_resolve_proof_t *receiver = NULL;
    if (owner) {
        cbm_resolve_result_t typed = cbm_lsp_proof_type_lookup(
            &ctx->field_types, name, name_node, "c-type-being-initialized", owner->qualified_name,
            owner->declaration_identity, owner->graph_qualified_name, owner->source_flags,
            "designated-member-of-the-written-type");
        typed = c_proof_type_import(ctx, owner->qualified_name, at, typed);
        if (cbm_resolve_admit(&typed)) {
            receiver = typed.proof;
            type_qn = owner->qualified_name;
        }
    }
    c_publish_field_owner(ctx, type_qn, name, name_node, receiver);
    return c_lookup_field_type(ctx, type_qn, name, 0);
}

/* The type of one element of an array being initialized, NULL otherwise. */
static const CBMType *c_initialized_element_type(CLSPContext *ctx, const CBMType *type) {
    if (!type)
        return NULL;
    const CBMType *base = c_simplify_type(ctx, type, false);
    return base && base->kind == CBM_TYPE_SLICE ? base->data.slice.elem : NULL;
}

static void c_walk_initializer_pair(CLSPContext *ctx, TSNode pair, const CBMType *list_type) {
    TSNode value = ts_node_child_by_field_name(pair, "value", 5);
    const CBMType *type = list_type;
    uint32_t nc = ts_node_named_child_count(pair);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode part = ts_node_named_child(pair, i);
        if (!ts_node_is_null(value) && part.id == value.id)
            continue;
        const char *pk = ts_node_type(part);
        if (strcmp(pk, "field_designator") == 0) {
            type = c_designate_member(ctx, type, ts_node_named_child(part, 0));
        } else if (strcmp(pk, "field_identifier") == 0) {
            type = c_designate_member(ctx, type, part); /* GNU `name: value` */
        } else if (strcmp(pk, "subscript_designator") == 0 ||
                   strcmp(pk, "subscript_range_designator") == 0) {
            type = c_initialized_element_type(ctx, type);
            c_resolve_calls_in_node(ctx, part); /* the index is an expression */
        }
    }
    if (!ts_node_is_null(value)) {
        c_hint_initializer_list(ctx, value, type);
        c_resolve_calls_in_node(ctx, value);
    }
}

static void c_walk_initializer_list(CLSPContext *ctx, TSNode list) {
    const CBMType *list_type = ctx->init_list_id == list.id ? ctx->init_list_type : NULL;
    ctx->init_list_type = NULL;
    ctx->init_list_id = NULL;

    const CBMType *elem_type = c_initialized_element_type(ctx, list_type);
    /* A positional nested list initializes the member at its position, as
     * long as no designator has moved the position. */
    const CBMRegisteredType *record = NULL;
    if (list_type && !elem_type) {
        const char *record_qn = type_to_qn(c_simplify_type(ctx, list_type, false));
        record = record_qn ? c_resolve_field_owner_type(ctx, record_qn) : NULL;
        for (int hop = 0; record && record->alias_of && hop < 5; hop++)
            record = c_resolve_field_owner_type(ctx, record->alias_of);
    }
    int position = 0;
    uint32_t nc = ts_node_named_child_count(list);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode child = ts_node_named_child(list, i);
        const char *ck = ts_node_type(child);
        if (strcmp(ck, "comment") == 0)
            continue;
        if (strcmp(ck, "initializer_pair") == 0) {
            record = NULL;
            c_walk_initializer_pair(ctx, child, list_type);
            continue;
        }
        if (strcmp(ck, "initializer_list") == 0) {
            c_hint_initializer_list(
                ctx, child, elem_type ? elem_type : c_struct_field_type_at(record, position));
        }
        position++;
        c_resolve_calls_in_node(ctx, child);
    }
}

/* A binding nearer than file scope: a parameter or a block-scope name. */
static bool c_name_bound_locally(CLSPContext *ctx, const char *name) {
    for (const CBMScope *scope = ctx->current_scope; scope && scope->parent; scope = scope->parent)
        if (cbm_scope_lookup_local(scope, name))
            return true;
    return false;
}

/* The name a declarator introduces, through `*`, `[]` and `(*name)(int)`. */
static TSNode c_declarator_identifier(TSNode declarator) {
    for (int hop = 0; hop < 16 && !ts_node_is_null(declarator); hop++) {
        const char *kind = ts_node_type(declarator);
        if (!strcmp(kind, "identifier"))
            return declarator;
        TSNode inner = ts_node_child_by_field_name(declarator, "declarator", 10);
        if (ts_node_is_null(inner) && !strcmp(kind, "parenthesized_declarator") &&
            ts_node_named_child_count(declarator))
            inner = ts_node_named_child(declarator, 0);
        declarator = inner;
    }
    return (TSNode){0};
}

/* A local name hides a file-scope one from its declaration on, even when nothing
 * else about it is understood: a callback parameter `int (*fn)(int)`, for one. */
static void c_bind_declared_names(CLSPContext *ctx, TSNode node, const char *kind) {
    if (strcmp(kind, "parameter_declaration") && strcmp(kind, "optional_parameter_declaration") &&
        strcmp(kind, "declaration"))
        return;
    TSTreeCursor cursor = ts_tree_cursor_new(node);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            const char *field = ts_tree_cursor_current_field_name(&cursor);
            if (!field || strcmp(field, "declarator"))
                continue;
            TSNode name = c_declarator_identifier(ts_tree_cursor_current_node(&cursor));
            const char *text = ts_node_is_null(name) ? NULL : c_node_text(ctx, name);
            if (text && !cbm_scope_lookup_local(ctx->current_scope, text))
                c_scope_bind(ctx, text, cbm_type_unknown());
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

/* An extern declaration of this object the translation unit sees. */
static const CBMCDecl *c_proof_extern_value(CLSPContext *ctx, const char *name, uint32_t at,
                                            const CTUModule **via) {
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind != CBM_C_DECL_EXTERN_VALUE || strcmp(decl->name, name))
            continue;
        if (!strcmp(decl->file, c_own_file_key(ctx)))
            return decl;
        const CTUModule *entry = c_tu_find(ctx, decl->file, strlen(decl->file));
        if (entry && !entry->exports_only && entry->at < at) {
            *via = entry;
            return decl;
        }
    }
    return NULL;
}

/* An exact graph declaration chosen by the rules around this call. */
static cbm_resolve_result_t c_proof_declaration_lookup(CLSPContext *ctx, const char *spelling,
                                                       TSNode site, const char *target,
                                                       const char *decision, bool imported) {
    bool located = !ts_node_is_null(site);
    cbm_resolve_proof_key_t key = {.lookup_domain = spelling,
                                   .lookup_key = spelling,
                                   .declaration_identity = target,
                                   .start_byte = located ? ts_node_start_byte(site) : 0,
                                   .end_byte = located ? ts_node_end_byte(site) : 0,
                                   .binding_identity = target,
                                   .detail = decision};
    cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE, .reason = decision};
    const cbm_resolve_proof_t *inputs[2];
    inputs[0] = cbm_resolve_proof_leaf(&ctx->field_types.proofs, CBM_PROOF_ADAPTER_DOMAIN,
                                       &ctx->field_types.proof_source, &key, &audit);
    audit.binding = CBM_RESOLVE_BINDING_UNIQUE;
    audit.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    audit.target = target;
    audit.facts = CBM_RESOLVE_FACT_EXACT_TARGET;
    audit.candidate_count = 1;
    key.detail = "selected_declaration";
    inputs[1] = cbm_resolve_proof_leaf(&ctx->field_types.proofs, CBM_PROOF_REGISTERED_DECLARATION,
                                       NULL, &key, &audit);
    cbm_resolve_result_t result = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = imported ? CBM_RESOLVE_RULE_IMPORT_BINDING : CBM_RESOLVE_RULE_SCOPE_BINDING,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_OCCURRENCE_EXACT |
                 (imported ? CBM_RESOLVE_FACT_IMPORT_BOUND : CBM_RESOLVE_FACT_SCOPE_BOUND),
        .target = target,
        .candidate_count = 1,
        .reason = decision};
    result.proof =
        cbm_resolve_proof_derive(&ctx->field_types.proofs, CBM_PROOF_REGISTRY_LOOKUP,
                                 &ctx->field_types.proof_source, &key, &result, inputs, 2, true);
    if (!result.proof || result.proof->value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return c_proof_missing("c_declaration_lookup_failed");
    return result;
}

/* As above, for a member selected through a receiver whose type is proven. */
static cbm_resolve_result_t c_proof_member_lookup(CLSPContext *ctx, const char *spelling,
                                                  TSNode site, const char *target,
                                                  const cbm_resolve_proof_t *receiver) {
    cbm_resolve_proof_key_t key = {.lookup_domain = spelling,
                                   .lookup_key = spelling,
                                   .declaration_identity = target,
                                   .start_byte = ts_node_start_byte(site),
                                   .end_byte = ts_node_end_byte(site),
                                   .binding_identity = target,
                                   .detail = "member-of-the-receiver-class"};
    cbm_resolve_result_t audit = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .reason = "member-of-the-receiver-class"};
    const cbm_resolve_proof_t *inputs[3];
    inputs[0] = cbm_resolve_proof_leaf(&ctx->field_types.proofs, CBM_PROOF_ADAPTER_DOMAIN,
                                       &ctx->field_types.proof_source, &key, &audit);
    audit.binding = CBM_RESOLVE_BINDING_UNIQUE;
    audit.domain = CBM_RESOLVE_DOMAIN_PROJECT;
    audit.target = target;
    audit.facts = CBM_RESOLVE_FACT_EXACT_TARGET;
    audit.candidate_count = 1;
    key.detail = "selected_member_declaration";
    inputs[1] = cbm_resolve_proof_leaf(&ctx->field_types.proofs, CBM_PROOF_REGISTERED_DECLARATION,
                                       NULL, &key, &audit);
    inputs[2] = receiver;
    cbm_resolve_result_t result = {
        .binding = CBM_RESOLVE_BINDING_UNIQUE,
        .domain = CBM_RESOLVE_DOMAIN_PROJECT,
        .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
        .rule = CBM_RESOLVE_RULE_TYPE_MEMBER,
        .facts = CBM_RESOLVE_FACT_EXACT_TARGET | CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                 CBM_RESOLVE_FACT_RECEIVER_TYPED | CBM_RESOLVE_FACT_OCCURRENCE_EXACT,
        .target = target,
        .candidate_count = 1,
        .reason = "member-of-the-receiver-class"};
    result.proof =
        cbm_resolve_proof_derive(&ctx->field_types.proofs, CBM_PROOF_REGISTRY_LOOKUP,
                                 &ctx->field_types.proof_source, &key, &result, inputs, 3, true);
    if (!result.proof || result.proof->value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return c_proof_missing("c_member_lookup_failed");
    return result;
}

/* One member, spelled with its signature on one side at most. */
static bool c_same_member_target(const char *a, const char *b) {
    bool a_signature = strchr(a, '(') != NULL, b_signature = strchr(b, '(') != NULL;
    if (a_signature == b_signature)
        return !strcmp(a, b);
    size_t length = strlen(a_signature ? b : a);
    return !strncmp(a, b, length);
}

static void c_member_hit_add(CMemberHit *hit, const char *target) {
    if (hit->target && !c_same_member_target(hit->target, target))
        hit->conflict = true;
    if (!hit->target || !strchr(hit->target, '('))
        hit->target = target;
}

/* The member function this name selects in this class. A class's own declaration hides
 * its bases. Written out of line it has no node under the class: its definition is the
 * one whose own file includes the class. Overloads are told apart by parameter count,
 * which a default argument makes a maximum, or not at all. */
static void c_proof_class_method(CLSPContext *ctx, const CBMRegisteredType *klass, const char *name,
                                 int argc, uint32_t at, int depth, bool from_own_class,
                                 CMemberHit *hit) {
    if (!klass || depth > 8)
        return;
    bool same_class = ctx->proof_method_class && ctx->proof_method_class == klass;
    /* A member function reaches its bases' protected members, and no further. */
    bool from_derived = !same_class && from_own_class;
    /* What the class body says about this name, which a definition elsewhere does not:
     * that it is declared at all, and with what access. */
    bool declared = false;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind != CBM_C_DECL_MEMBER_NAME || strcmp(decl->name, name) || !decl->type_name ||
            !klass->short_name || strcmp(decl->type_name, klass->short_name) ||
            !klass->source_file_qn || strcmp(decl->file, klass->source_file_qn))
            continue;
        declared = true;
        if (!same_class && (decl->flags & CBM_C_DECL_NOT_PUBLIC) &&
            !(from_derived && (decl->flags & CBM_C_DECL_PROTECTED))) {
            hit->open = true;
            return;
        }
    }
    CMemberHit own = {0};
    CBMMethodIter iterator;
    cbm_registry_methods_chain(ctx->proof_registry, klass->qualified_name, name, &iterator);
    for (int index; (index = cbm_method_iter_next(&iterator)) >= 0;) {
        const CBMRegisteredFunc *candidate = &iterator.reg->funcs[index];
        if (!(candidate->flags & CBM_FUNC_FLAG_PROJECT_SOURCE) || candidate->type_param_names ||
            !(candidate->source_flags & CBM_SOURCE_DECL_KNOWN) ||
            (!same_class && (candidate->source_flags & CBM_SOURCE_DECL_PRIVATE)) ||
            (!same_class && !from_derived &&
             (candidate->source_flags & CBM_SOURCE_DECL_PROTECTED)) ||
            (argc >= 0 && candidate->source_arity && candidate->source_arity != argc + 1))
            continue;
        const char *target =
            candidate->sig && candidate->sig[0]
                ? cbm_arena_sprintf(ctx->arena, "%s%s", candidate->qualified_name, candidate->sig)
                : candidate->qualified_name;
        if (target)
            c_member_hit_add(&own, target);
    }
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind != CBM_C_DECL_METHOD || strcmp(decl->name, name) || !decl->target ||
            !decl->type_name || !klass->short_name || strcmp(decl->type_name, klass->short_name) ||
            !klass->source_file_qn || (decl->arity >= 0 && argc > decl->arity))
            continue;
        if (c_proof_file_reaches(ctx, decl->file, klass->source_file_qn))
            c_member_hit_add(&own, decl->target);
    }
    if (own.target) {
        if (own.conflict)
            hit->conflict = true;
        c_member_hit_add(hit, own.target);
        return;
    }
    /* The class declares this name and nothing here can be bound to it. It hides the
     * bases and the enclosing scopes all the same. */
    if (declared) {
        hit->open = true;
        return;
    }
    for (int i = 0; klass->embedded_types && klass->embedded_types[i]; i++) {
        /* A base written with arguments may depend on the template's parameters, and then an
         * explicit specialization decides what it holds. */
        const char *base_name = c_base_class_name(ctx, klass->embedded_types[i]);
        if (!base_name || c_base_may_depend(ctx, base_name) ||
            c_template_is_specialized(ctx, base_name)) {
            hit->open = true;
            continue;
        }
        const CBMRegisteredType *base = c_proof_named_class(ctx, base_name, at);
        if (!base)
            hit->open = true;
        c_proof_class_method(ctx, base, name, argc, at, depth + 1,
                             from_own_class || ctx->proof_method_class == klass, hit);
    }
    /* A class whose body is not fully understood may hold more. */
    if (!(klass->source_flags & CBM_SOURCE_CLASS_BODY_COMPLETE))
        hit->open = true;
}

static int c_call_argument_count(TSNode call) {
    TSNode arguments = ts_node_child_by_field_name(call, "arguments", 9);
    uint32_t count = ts_node_is_null(arguments) ? 0 : ts_node_named_child_count(arguments);
    int total = 0;
    for (uint32_t i = 0; i < count; i++)
        total += strcmp(ts_node_type(ts_node_named_child(arguments, i)), "comment") != 0;
    return total;
}

/* `Type::member(...)`: the scope names a class the translation unit sees, and the member
 * is selected in it by the same rules as through a receiver. */
static cbm_resolve_result_t c_proof_scoped_member(CLSPContext *ctx, TSNode function, TSNode call) {
    TSNode scope = ts_node_child_by_field_name(function, "scope", 5);
    TSNode name = ts_node_child_by_field_name(function, "name", 4);
    if (ts_node_is_null(scope) || ts_node_is_null(name) ||
        strcmp(ts_node_type(scope), "namespace_identifier") ||
        strcmp(ts_node_type(name), "identifier"))
        return c_proof_missing("c_scope_is_not_a_plain_name");
    const char *owner = c_node_text(ctx, scope);
    const char *written = c_node_text(ctx, name);
    uint32_t at = ts_node_start_byte(call);
    const CBMRegisteredType *klass = c_proof_named_class(ctx, owner, at);
    if (!klass || !written)
        return c_proof_missing("c_scope_class_not_visible");
    CMemberHit hit = {0};
    c_proof_class_method(ctx, klass, written, c_call_argument_count(call), at, 0, false, &hit);
    if (!hit.target || hit.conflict)
        return c_proof_missing("c_scoped_member_not_selected");
    cbm_resolve_result_t typed = cbm_lsp_proof_type_lookup(
        &ctx->field_types, owner, scope, "c-written-class-scope", klass->qualified_name,
        klass->declaration_identity, klass->graph_qualified_name, klass->source_flags,
        "class-named-by-the-call's-scope");
    typed = c_proof_type_import(ctx, klass->qualified_name, at, typed);
    return cbm_resolve_admit(&typed) ? c_proof_member_lookup(ctx, c_node_text(ctx, function),
                                                             function, hit.target, typed.proof)
                                     : typed;
}

/* A file-scope object or enumerator in the translation unit. An extern declaration
 * means the one definition of that external name in the project, as for functions. */
static cbm_resolve_result_t c_proof_value(CLSPContext *ctx, const char *name, TSNode site) {
    uint32_t at = ts_node_start_byte(site);
    cbm_resolve_result_t result = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                   .reason = "c_visible_value_missing"};
    const char *chosen = NULL;
    const CTUModule *chosen_via = NULL;
    const char *external = NULL;
    int external_count = 0;
    /* Twice at most: what this file's own unit shows, then what its one includer showed. */
    for (int from_includer = 0; from_includer < 2 && !chosen; from_includer++) {
        for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
             link = link->next) {
            const CBMCDecl *decl = link->decl;
            if (decl->kind != CBM_C_DECL_VALUE || strcmp(decl->name, name) || !decl->target)
                continue;
            if (!from_includer && (decl->flags & CBM_C_DECL_EXTERNAL) &&
                strcmp(external ? external : "", decl->target)) {
                external = decl->target;
                external_count++;
            }
            const CTUModule *entry = NULL;
            if (strcmp(decl->file, c_own_file_key(ctx))) {
                entry = c_tu_find(ctx, decl->file, strlen(decl->file));
                if (!entry || entry->exports_only || entry->at >= at ||
                    entry->from_includer != (bool)from_includer)
                    continue;
            } else if (from_includer) {
                continue;
            }
            if (chosen && strcmp(chosen, decl->target))
                return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                              .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                              .candidate_count = 2,
                                              .reason = "c_visible_values_conflict"};
            chosen = decl->target;
            chosen_via = entry;
        }
    }
    if (chosen) {
        result = c_proof_declaration_lookup(ctx, name, site, chosen, "visible-file-scope-value",
                                            chosen_via != NULL);
        return chosen_via
                   ? c_proof_import_join(ctx, result, chosen_via->via, CBM_PROOF_IMPORT_BINDING)
                   : result;
    }
    const CTUModule *via = NULL;
    const CBMCDecl *declared = c_proof_extern_value(ctx, name, at, &via);
    if (!declared)
        return result;
    if (external_count > 1)
        return (cbm_resolve_result_t){.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                      .binding = CBM_RESOLVE_BINDING_AMBIGUOUS,
                                      .candidate_count = 2,
                                      .reason = "c_extern_value_definition_not_unique"};
    /* With no definition in the project, the declaration itself is what the name
     * selects, when it has a node of its own. */
    if (!external && declared->target)
        external = declared->target;
    if (!external)
        return result;
    result = c_proof_declaration_lookup(
        ctx, name, site, external, "visible-extern-declaration;unique-external-definition", false);
    return via ? c_proof_import_join(ctx, result, via->via, CBM_PROOF_IMPORT_BINDING) : result;
}

/* A definition of this macro this unit sees: from this file or through its includes. */
static bool c_macro_definition_visible(CLSPContext *ctx, const CBMCDecl *decl, uint32_t at) {
    if (!strcmp(decl->file, c_own_file_key(ctx)))
        return true;
    const CTUModule *entry = c_tu_find(ctx, decl->file, strlen(decl->file));
    return entry && !entry->exports_only && entry->at < at;
}

/* Which parameter of this definition takes argument `position`. */
static int c_macro_parameter(const CBMCMacroShape *shape, int position) {
    if (position < shape->param_count && !(shape->variadic && position >= shape->param_count - 1))
        return position;
    return shape->variadic ? shape->param_count - 1 : -1;
}

/* A macro may hand an argument on several times, to macros that do the same, so following every
 * hand-off doubles with each hop. A walk reads each (macro, position) once. Meeting one again adds
 * nothing: its answer is already part of this one, and a macro is not expanded again inside its
 * own expansion. */
typedef struct {
    const char *macro;
    int position;
} CMacroArgument;

static const char *c_macro_argument_key(CLSPContext *ctx, const char *macro, int position) {
    return cbm_arena_sprintf(ctx->arena, "%d#%s", position, macro);
}

/* Does this written name, as argument `position` of the macro, reach the expansion as itself in
 * every definition the unit sees? The name then means what it means at the use, because
 * substitution puts it there. A definition that pastes it, reads it as a member, declares it or
 * declares a local of that name says otherwise, and so does one this does not understand. One
 * definition's answer does not depend on another's, so the order of the walk cannot change this. */
static bool c_macro_argument_is_plain(CLSPContext *ctx, const char *macro, int position,
                                      const char *written, uint32_t at) {
    CMacroArgument top = {macro, position};
    CMacroArgument *stack = &top;
    int count = 1, cap = 1;
    CBMHashTable *seen = NULL;
    bool plain = true;
    while (plain && count) {
        CMacroArgument use = stack[--count];
        bool defined = false;
        for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, use.macro); link && plain;
             link = link->next) {
            const CBMCDecl *decl = link->decl;
            if (decl->kind != CBM_C_DECL_MACRO || strcmp(decl->name, use.macro) ||
                !c_macro_definition_visible(ctx, decl, at))
                continue;
            CBM_LSP_WORK(1);
            defined = true;
            const CBMCMacroShape *shape = decl->macro;
            int param = shape ? c_macro_parameter(shape, use.position) : -1;
            if (param < 0 || !(shape->plain & (1u << param))) {
                plain = false;
                break;
            }
            for (int i = 0; plain && i < shape->local_count; i++)
                plain = strcmp(shape->locals[i], written) != 0;
            for (int i = 0; plain && i < shape->handoff_count; i++) {
                const CBMCMacroHandoff *handoff = &shape->handoffs[i];
                if (handoff->param != param || !c_proof_macro_visible(ctx, handoff->callee, at))
                    continue;
                /* `__VA_ARGS__` handed on stands for several arguments at several positions. */
                if (shape->variadic && param == shape->param_count - 1) {
                    plain = false;
                    break;
                }
                const char *key = c_macro_argument_key(ctx, handoff->callee, handoff->position);
                if (!seen)
                    seen = cbm_ht_create(8);
                if (!key || !seen) {
                    plain = false;
                    break;
                }
                if (cbm_ht_get(seen, key))
                    continue;
                cbm_ht_set(seen, key, (void *)key);
                if (count == cap) {
                    int grown = cap * 2 + 6;
                    CMacroArgument *next =
                        cbm_arena_alloc(ctx->arena, (size_t)grown * sizeof(*next));
                    if (!next) {
                        plain = false;
                        break;
                    }
                    memcpy(next, stack, (size_t)count * sizeof(*next));
                    stack = next;
                    cap = grown;
                }
                stack[count++] = (CMacroArgument){handoff->callee, handoff->position};
            }
        }
        plain = plain && defined;
    }
    if (seen)
        cbm_ht_free(seen);
    return plain;
}

static void c_macro_args_load(CLSPContext *ctx, TSNode list) {
    ctx->macro_args_list = list.id;
    ctx->macro_args_of_macro = false;
    ctx->macro_plain_count = 0;
    TSNode call = ts_node_parent(list);
    if (ts_node_is_null(call) || strcmp(ts_node_type(call), "call_expression"))
        return;
    TSNode function = ts_node_child_by_field_name(call, "function", 8);
    if (ts_node_is_null(function) || strcmp(ts_node_type(function), "identifier"))
        return;
    const char *name = c_node_text(ctx, function);
    uint32_t at = ts_node_start_byte(call);
    if (!name || !c_proof_macro_visible(ctx, name, at))
        return;
    ctx->macro_args_of_macro = true;
    uint32_t capacity = ts_node_named_child_count(list);
    uint32_t *plain = capacity ? cbm_arena_alloc(ctx->arena, capacity * sizeof(*plain)) : NULL;
    if (!plain)
        return;
    int position = 0, count = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(list);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode argument = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(argument) || !strcmp(ts_node_type(argument), "comment"))
                continue;
            if (!strcmp(ts_node_type(argument), "identifier")) {
                const char *written = c_node_text(ctx, argument);
                if (written && c_macro_argument_is_plain(ctx, name, position, written, at))
                    plain[count++] = ts_node_start_byte(argument);
            }
            position++;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    ctx->macro_plain_args = plain;
    ctx->macro_plain_count = count;
}

/* An unscoped enumerator a class declares is a member of that class. A class in the middle of
 * the search that this cannot see the whole of may hold one too, and then nothing is said. */
static const char *c_proof_class_enumerator(CLSPContext *ctx, const CBMRegisteredType *klass,
                                            const char *name, uint32_t at, int depth) {
    if (!klass || depth > 8 || !klass->short_name || !klass->source_file_qn)
        return NULL;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind == CBM_C_DECL_MEMBER_NAME && !strcmp(decl->name, name) && decl->target &&
            decl->type_name && !strcmp(decl->type_name, klass->short_name) &&
            !strcmp(decl->file, klass->source_file_qn))
            return decl->target;
    }
    for (int i = 0; klass->embedded_types && klass->embedded_types[i]; i++) {
        const char *base_name = c_base_class_name(ctx, klass->embedded_types[i]);
        if (c_base_may_depend(ctx, base_name))
            continue; /* a dependent base is not searched by an unqualified name */
        const CBMRegisteredType *base = c_proof_named_class(ctx, base_name, at);
        const char *found = c_proof_class_enumerator(ctx, base, name, at, depth + 1);
        if (found)
            return found;
    }
    return NULL;
}

/* `Triple::UnknownOS`: the scope names a class the unit sees, and the class declares the name. */
static void c_emit_scoped_enumerator(CLSPContext *ctx, TSNode qualified, TSNode name) {
    TSNode written_name = ts_node_child_by_field_name(qualified, "name", 4);
    TSNode scope = ts_node_child_by_field_name(qualified, "scope", 5);
    if (ts_node_is_null(written_name) || !ts_node_eq(written_name, name) ||
        ts_node_is_null(scope) || strcmp(ts_node_type(scope), "namespace_identifier") ||
        !c_cxx_rules(ctx))
        return;
    const char *owner = c_node_text(ctx, scope);
    const char *text = c_node_text(ctx, name);
    uint32_t at = ts_node_start_byte(name);
    const CBMRegisteredType *klass = owner ? c_proof_named_class(ctx, owner, at) : NULL;
    const char *target = klass && text ? c_proof_class_enumerator(ctx, klass, text, at, 0) : NULL;
    if (!target)
        return;
    cbm_resolve_result_t typed = cbm_lsp_proof_type_lookup(
        &ctx->field_types, owner, scope, "c-written-class-scope", klass->qualified_name,
        klass->declaration_identity, klass->graph_qualified_name, klass->source_flags,
        "class-named-by-the-scope");
    typed = c_proof_type_import(ctx, klass->qualified_name, at, typed);
    if (!cbm_resolve_admit(&typed))
        return;
    cbm_resolve_result_t member = c_proof_member_lookup(ctx, text, name, target, typed.proof);
    if (cbm_resolve_admit(&member))
        c_emit_proven(ctx, &member, text, CBM_RESOLVED_VALUE_REFERENCE, name);
}

/* An argument of a function-like macro is tokens, not an expression: neovim's
 * `HAS_KEY(opts, win_text_height, start_row)` pastes both names into another identifier. So a
 * bare name there says nothing about a declaration that happens to have that name, unless the
 * macro's own replacement shows it is substituted as itself. */
static bool c_bare_argument_of_macro(CLSPContext *ctx, TSNode node) {
    TSNode list = ts_node_parent(node);
    if (ts_node_is_null(list) || strcmp(ts_node_type(list), "argument_list"))
        return false;
    if (ctx->macro_args_list != list.id)
        c_macro_args_load(ctx, list);
    if (!ctx->macro_args_of_macro)
        return false;
    uint32_t start = ts_node_start_byte(node);
    int lo = 0, hi = ctx->macro_plain_count;
    while (lo < hi) {
        int mid = lo + (hi - lo) / 2;
        if (ctx->macro_plain_args[mid] < start)
            lo = mid + 1;
        else
            hi = mid;
    }
    return lo >= ctx->macro_plain_count || ctx->macro_plain_args[lo] != start;
}

/* The base type and pointer depth when this macro declares its argument `position` as a
 * variable in every definition the unit sees, directly or by handing it to a macro that does:
 * `#define FOR_ALL_TABS(tp) for (tabpage_T *(tp) = first_tabpage; ...)`. */
static const char *c_macro_declared_type(CLSPContext *ctx, const char *macro, int position,
                                         uint32_t at, int depth, int *pointers,
                                         CBMHashTable **answers);

typedef struct {
    const char *type;
    int pointers;
} CMacroDeclared;

/* What the macro a hand-off reaches declares, read once per walk. Reached again while being read,
 * in a cycle, the answer is still none: a macro is not expanded inside its own expansion. The depth
 * only keeps a hostile chain off the stack: going to it from 16 changed no edge in redis, neovim,
 * or the LLVM and kernel subtrees measured. */
#define C_MACRO_HANDOFF_MAX_DEPTH 64
static const char *c_macro_handed_type(CLSPContext *ctx, const CBMCMacroHandoff *handoff,
                                       uint32_t at, int depth, int *pointers,
                                       CBMHashTable **answers) {
    if (depth > C_MACRO_HANDOFF_MAX_DEPTH)
        return NULL;
    const char *key = c_macro_argument_key(ctx, handoff->callee, handoff->position);
    if (!*answers)
        *answers = cbm_ht_create(8);
    CMacroDeclared *answer = key && *answers ? cbm_ht_get(*answers, key) : NULL;
    if (answer) {
        *pointers = answer->pointers;
        return answer->type;
    }
    answer = key && *answers ? cbm_arena_alloc(ctx->arena, sizeof(*answer)) : NULL;
    if (!answer)
        return NULL;
    *answer = (CMacroDeclared){0};
    cbm_ht_set(*answers, key, answer);
    const char *type = c_macro_declared_type(ctx, handoff->callee, handoff->position, at, depth,
                                             pointers, answers);
    answer->type = type;
    answer->pointers = *pointers;
    return type;
}

static const char *c_macro_declared_type(CLSPContext *ctx, const char *macro, int position,
                                         uint32_t at, int depth, int *pointers,
                                         CBMHashTable **answers) {
    const char *found = NULL;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, macro); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind != CBM_C_DECL_MACRO || strcmp(decl->name, macro) ||
            !c_macro_definition_visible(ctx, decl, at))
            continue;
        CBM_LSP_WORK(1);
        if (c_macro_definition_unproven(ctx, decl, at))
            return NULL;
        const CBMCMacroShape *shape = decl->macro;
        int param = shape ? c_macro_parameter(shape, position) : -1;
        if (param < 0)
            return NULL;
        const char *type = NULL;
        int depth_found = 0;
        if (shape->declared & (1u << param)) {
            type = shape->declared_types[param];
            depth_found = shape->declared_pointers[param];
        } else {
            for (int i = 0; i < shape->handoff_count && !type; i++) {
                const CBMCMacroHandoff *handoff = &shape->handoffs[i];
                if (handoff->param == param && c_proof_macro_visible(ctx, handoff->callee, at))
                    type = c_macro_handed_type(ctx, handoff, at, depth + 1, &depth_found, answers);
            }
        }
        if (!type || (found && (strcmp(found, type) || *pointers != depth_found)))
            return NULL;
        found = type;
        *pointers = depth_found;
    }
    return found;
}

/* The variable a loop macro declares from one of its arguments. The name is bound whatever its
 * type turns out to be: nothing of that name outside the macro is meant by it from here on. */
static void c_bind_macro_declared(CLSPContext *ctx, const char *macro, int position, TSNode name) {
    uint32_t at = ts_node_start_byte(name);
    const char *written = c_node_text(ctx, name);
    int pointers = 0;
    CBMHashTable *answers = NULL;
    const char *type_name =
        written && ctx->c_decls && c_proof_macro_visible(ctx, macro, at)
            ? c_macro_declared_type(ctx, macro, position, at, 0, &pointers, &answers)
            : NULL;
    if (answers)
        cbm_ht_free(answers);
    if (!type_name)
        return;
    const CBMRegisteredType *owner =
        c_proof_follow_alias(ctx, c_proof_type_by_name(ctx, type_name, at));
    owner = owner ? c_proof_field_class(ctx, owner->qualified_name, at) : NULL;
    cbm_resolve_result_t proof = {0};
    if (owner) {
        proof = cbm_lsp_proof_type_lookup(
            &ctx->field_types, written, name, "c-macro-declared-variable", owner->qualified_name,
            owner->declaration_identity, owner->graph_qualified_name, owner->source_flags,
            "written-type-in-the-macro-that-declares-it");
        proof = c_proof_type_import(ctx, owner->qualified_name, at, proof);
    }
    if (!owner || !cbm_resolve_admit(&proof) || c_name_bound_locally(ctx, written)) {
        c_scope_bind(ctx, written, cbm_type_unknown());
        return;
    }
    const CBMType *type = cbm_type_named(ctx->arena, owner->qualified_name);
    for (int i = 0; i < pointers; i++)
        type = cbm_type_pointer(ctx->arena, type);
    c_scope_bind(ctx, written, type);
    (void)cbm_resolve_types_declare(&ctx->field_types, ctx->current_scope, written, at, type,
                                    CBM_RESOLVE_TYPE_ANNOTATION, &proof);
}

/* ── What a macro's replacement writes ───────────────────────────────
 * `#define quicklistNodeUpdateSz(n) do { (n)->sz = lpBytes((n)->entry); } while (0)`: the function
 * holding the use compiles a call to lpBytes. The name is looked up where the macro is used, as
 * the preprocessor leaves it there. A macro handed on is walked in turn. Every name and macro is
 * taken once per use, so one use costs what the preprocessor pays to expand it, or less. */
enum { C_MACRO_USE_ARGUMENTS = 32 }; /* CSP_MACRO_PARAMS: no shape names a parameter past it */

/* The arguments written at a macro's use, each once: at most what a shape can name by position. */
static uint32_t c_macro_use_arguments(TSNode call, TSNode *out, uint32_t cap) {
    TSNode arguments = ts_node_child_by_field_name(call, "arguments", 9);
    uint32_t count = 0;
    if (ts_node_is_null(arguments))
        return 0;
    TSTreeCursor cursor = ts_tree_cursor_new(arguments);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode part = ts_tree_cursor_current_node(&cursor);
            if (ts_node_is_named(part))
                out[count++] = part;
        } while (count < cap && ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
    return count;
}

typedef struct {
    CBMHashTable *seen;
    const char *top, *top_key;
    const char **pending;
    int pending_count, pending_cap;
    TSNode call;
    bool use_is_call; /* the use has its parentheses, so `#define f g` makes g the callee */
    const CBMCMacroShape *shape; /* the definition being read */
} CMacroWalk;

static bool c_macro_walk_first(CMacroWalk *walk, const char *name) {
    if (!walk->seen) {
        walk->seen = cbm_ht_create(8);
        if (!walk->seen)
            return false;
        cbm_ht_set(walk->seen, walk->top_key, (void *)walk->top_key);
    }
    if (cbm_ht_get(walk->seen, name))
        return false;
    cbm_ht_set(walk->seen, name, (void *)name);
    return true;
}

static void c_macro_walk_push(CLSPContext *ctx, CMacroWalk *walk, const char *macro) {
    const char *key = cbm_arena_sprintf(ctx->arena, "#%s", macro);
    if (!key || !c_macro_walk_first(walk, key))
        return;
    if (walk->pending_count == walk->pending_cap) {
        int next_cap = walk->pending_cap ? walk->pending_cap * 2 : 8;
        const char **next = cbm_arena_alloc(ctx->arena, (size_t)next_cap * sizeof(*next));
        if (!next)
            return;
        if (walk->pending_count)
            memcpy(next, walk->pending, (size_t)walk->pending_count * sizeof(*next));
        walk->pending = next;
        walk->pending_cap = next_cap;
    }
    walk->pending[walk->pending_count++] = macro;
}

static int c_macro_name_compare(const void *key, const void *item) {
    return strcmp((const char *)key, *(const char *const *)item);
}

static void c_macro_body_name(CLSPContext *ctx, CMacroWalk *walk, const char *name, bool called) {
    TSNode site = walk->call;
    uint32_t at = ts_node_start_byte(site);
    /* The class comes first inside a member function, and only `::a::b` gets past it. */
    bool in_class = ctx->enclosing_class_qn || ctx->proof_method_class;
    if (in_class && strncmp(name, "::", 2))
        return;
    if (!c_macro_walk_first(walk, name) || c_name_bound_locally(ctx, name) ||
        c_is_template_parameter(ctx, name))
        return;
    bool qualified = strstr(name, "::") != NULL;
    bool scope_checked = ctx->proof_class_scope_checked;
    ctx->proof_class_scope_checked = scope_checked || in_class;
    cbm_resolve_result_t proof = {.analysis = CBM_RESOLVE_ANALYSIS_COMPLETE};
    if (!called && !qualified) {
        proof = c_proof_value(ctx, name, site);
        if (proof.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE ||
            proof.binding != CBM_RESOLVE_BINDING_UNRESOLVED) {
            if (cbm_resolve_admit(&proof))
                c_emit_proven(ctx, &proof, name, CBM_RESOLVED_MACRO_VALUE, site);
            return;
        }
    }
    proof = c_proof_name_lookup(ctx, name, at);
    ctx->proof_class_scope_checked = scope_checked;
    if (cbm_resolve_admit(&proof)) {
        c_emit_proven(ctx, &proof, name,
                      called ? CBM_RESOLVED_MACRO_INVOCATION : CBM_RESOLVED_MACRO_VALUE, site);
        return;
    }
    /* A cast in the replacement, or `#define sds hisds`: a type this unit sees by that name.
     * `memblock.memory` reads a member, so that word is an object whose definition is not here. */
    if (called || qualified || proof.binding != CBM_RESOLVE_BINDING_UNRESOLVED ||
        (walk->shape && walk->shape->object_count &&
         bsearch(name, walk->shape->objects, (size_t)walk->shape->object_count,
                 sizeof(*walk->shape->objects), c_macro_name_compare)))
        return;
    const CBMRegisteredType *type = c_proof_type_by_name(ctx, name, at);
    if (!type || !type->project_source || !type->graph_qualified_name ||
        !c_proof_type_visible(ctx, type->qualified_name, at))
        return;
    proof = cbm_lsp_proof_type_lookup(&ctx->field_types, name, site, "c-macro-replacement-type",
                                      type->qualified_name, type->declaration_identity,
                                      type->graph_qualified_name, type->source_flags,
                                      "visible-type-declaration");
    proof = c_proof_type_import(ctx, type->qualified_name, at, proof);
    if (cbm_resolve_admit(&proof))
        c_emit_proven(ctx, &proof, name, CBM_RESOLVED_MACRO_VALUE, site);
}

/* Is the name in this sorted list? */
static bool c_macro_lists(const char **names, int count, const char *name) {
    return count && bsearch(name, names, (size_t)count, sizeof(*names), c_macro_name_compare);
}

/* A macro may name a callee or a value only in the configuration one definition belongs to:
 * `#ifdef DEBUG / #define LOG(x) trace(x) / #else / #define LOG(x) / #endif`. What every definition
 * this unit sees agrees on holds whichever one is in force, and that is what is read. */
static bool c_macro_names_in_all(const CMacroDefs *defs, const bool *in_force, const char *name,
                                 bool called) {
    for (int d = 0; d < defs->count; d++) {
        const CBMCMacroShape *shape = defs->items[d]->macro;
        if (in_force[d] && !(called ? c_macro_lists(shape->calls, shape->call_count, name)
                                    : c_macro_lists(shape->values, shape->value_count, name)))
            return false;
    }
    return true;
}

/* `list_for_each_entry(obj, &patch->obj_list, node)`: node is the member the callee reads. A word
 * handed on is a value only when the callee is known to use it as one: a macro that substitutes it,
 * or a function this unit sees. `offsetof(T, x)` and `_Generic(...)` are neither. */
static bool c_macro_word_only_handed_on(CLSPContext *ctx, const CBMCMacroShape *shape,
                                        const char *name, uint32_t at) {
    for (int i = 0; i < shape->name_argument_count; i++) {
        const CBMCMacroNameArgument *argument = &shape->name_arguments[i];
        if (strcmp(argument->name, name))
            continue;
        if (c_proof_macro_visible(ctx, argument->callee, at)) {
            if (!c_macro_argument_is_plain(ctx, argument->callee, argument->position, name, at))
                return true;
            continue;
        }
        cbm_resolve_result_t callee = c_proof_name_lookup(ctx, argument->callee, at);
        if (!cbm_resolve_admit(&callee))
            return true;
    }
    return false;
}

static void c_macro_body_paste(CLSPContext *ctx, CMacroWalk *walk, const CBMCMacroShape *shape) {
    if (!shape->paste_count)
        return;
    TSNode arguments[C_MACRO_USE_ARGUMENTS];
    uint32_t count = c_macro_use_arguments(walk->call, arguments, C_MACRO_USE_ARGUMENTS);
    uint32_t at = ts_node_start_byte(walk->call);
    for (int i = 0; i < shape->paste_count; i++) {
        const CBMCMacroPaste *paste = &shape->pastes[i];
        if (paste->param >= count || strcmp(ts_node_type(arguments[paste->param]), "identifier"))
            continue;
        TSNode written = arguments[paste->param];
        const char *word = c_node_text(ctx, written);
        const char *joined =
            word ? cbm_arena_sprintf(ctx->arena, "%s%s%s", paste->prefix, word, paste->suffix)
                 : NULL;
        if (joined && !c_proof_macro_visible(ctx, joined, at))
            c_macro_body_name(ctx, walk, joined, false);
    }
}

/* `#define listLength(l) ((l)->len)`: the argument's type is known where the macro is used, so the
 * member is too. C only: in C++ `->` may be an operator of the argument's class. */
static void c_macro_body_members(CLSPContext *ctx, CMacroWalk *walk, const CBMCMacroShape *shape) {
    if (!shape->member_count)
        return;
    TSNode arguments[C_MACRO_USE_ARGUMENTS];
    uint32_t count = c_macro_use_arguments(walk->call, arguments, C_MACRO_USE_ARGUMENTS);
    uint32_t at = ts_node_start_byte(walk->call);
    for (int i = 0; i < shape->member_count; i++) {
        const CBMCMacroMember *member = &shape->members[i];
        if (member->param >= count)
            continue;
        TSNode written = arguments[member->param];
        if (ts_node_has_error(written))
            continue;
        const cbm_resolve_proof_t *proof = NULL;
        const CBMType *receiver_shape = NULL;
        const CBMRegisteredType *owner =
            c_proof_expression_owner(ctx, written, 0, &proof, &receiver_shape);
        const char *rest = member->path;
        while (owner && proof && *rest) {
            size_t length = strcspn(rest, ".");
            const char *name = cbm_arena_strndup(ctx->arena, rest, length);
            const CBMType *type = NULL;
            const CBMRegisteredType *declarer =
                name ? c_proof_field_declarer(ctx, owner, name, at, 0, &type) : NULL;
            if (!declarer)
                break;
            c_publish_field_owner_as(ctx, declarer->qualified_name, name, walk->call, proof,
                                     CBM_RESOLVED_MACRO_VALUE);
            rest += length + (rest[length] == '.');
            owner = *rest ? c_proof_named_class(ctx, c_field_type_name(type), at) : NULL;
        }
    }
}

static void c_macro_body_expand(CLSPContext *ctx, CMacroWalk *walk, const char *macro) {
    uint32_t at = ts_node_start_byte(walk->call);
    const CMacroDefs *defs = c_macro_defs(ctx, macro);
    const char *own = c_own_file_key(ctx);
    bool *in_force = defs->count ? cbm_arena_alloc(ctx->arena, (size_t)defs->count) : NULL;
    if (!in_force)
        return;
    const CBMCMacroShape *first = NULL;
    int visible = 0;
    for (int d = 0; d < defs->count; d++) {
        const CBMCDecl *decl = defs->items[d];
        in_force[d] = defs->from[d] < at || !strcmp(decl->file, own);
        if (!in_force[d])
            continue;
        /* A definition this cannot read may say anything, and so may a header it cannot read. */
        if (!decl->macro || c_macro_definition_unproven(ctx, decl, at))
            return;
        first = first ? first : decl->macro;
        visible++;
    }
    if (!first || (!first->object_like && !walk->use_is_call && macro == walk->top))
        return;
    walk->shape = first;
    /* `#define sdsnew hi_sdsnew`: with the parentheses after it, the other name is called. */
    if (first->object_like && walk->use_is_call && first->value_count == 1 && !first->call_count &&
        !first->paste_count) {
        const char *other = first->values[0];
        if (!c_macro_names_in_all(defs, in_force, other, false))
            return;
        if (c_proof_macro_visible(ctx, other, at))
            c_macro_walk_push(ctx, walk, other);
        else
            c_macro_body_name(ctx, walk, other, true);
        return;
    }
    for (int i = 0; i < first->call_count; i++) {
        const char *name = first->calls[i];
        if (!c_macro_names_in_all(defs, in_force, name, true))
            continue;
        if (c_proof_macro_visible(ctx, name, at)) {
            c_macro_walk_push(ctx, walk, name);
        } else {
            c_macro_body_name(ctx, walk, name, true);
        }
    }
    for (int i = 0; i < first->value_count; i++) {
        const char *name = first->values[i];
        bool captured = false;
        for (int l = 0; l < first->local_count && !captured; l++)
            captured = !strcmp(first->locals[l], name);
        if (!captured && c_macro_names_in_all(defs, in_force, name, false) &&
            !c_proof_macro_visible(ctx, name, at) &&
            !c_macro_word_only_handed_on(ctx, first, name, at))
            c_macro_body_name(ctx, walk, name, false);
    }
    /* The arguments at the use are the top macro's. A macro handed on has others. */
    if (macro == walk->top && visible == 1) {
        c_macro_body_paste(ctx, walk, first);
        if (!c_cxx_rules(ctx))
            c_macro_body_members(ctx, walk, first);
    }
}

static void c_macro_body_walk(CLSPContext *ctx, const char *macro, TSNode use, bool use_is_call) {
    /* `#undef assert` with no definition of its own after it: this file left the macro behind. */
    uint32_t at = ts_node_start_byte(use);
    const char *undefined = cbm_arena_sprintf(ctx->arena, "$undef:%s", macro);
    const char *defined = cbm_arena_sprintf(ctx->arena, "$macro:%s", macro);
    if (!undefined || !defined ||
        (c_proof_declared(ctx, undefined, at) && !c_proof_declared(ctx, defined, at)))
        return;
    CMacroWalk walk = {.top = macro,
                       .top_key = cbm_arena_sprintf(ctx->arena, "#%s", macro),
                       .call = use,
                       .use_is_call = use_is_call};
    if (!walk.top_key)
        return;
    c_macro_body_expand(ctx, &walk, macro);
    while (walk.pending_count > 0)
        c_macro_body_expand(ctx, &walk, walk.pending[--walk.pending_count]);
    if (walk.seen)
        cbm_ht_free(walk.seen);
}

/* `FOR_ALL_WINDOWS_IN_TAB(wp, tp) {` reads as a call with its `;` missing and a block after it. */
static void c_bind_macro_declared_arguments(CLSPContext *ctx, TSNode call, const char *macro) {
    TSNode list = ts_node_child_by_field_name(call, "arguments", 9);
    if (ts_node_is_null(list))
        return;
    int position = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(list);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode argument = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(argument) || !strcmp(ts_node_type(argument), "comment"))
                continue;
            if (!strcmp(ts_node_type(argument), "identifier"))
                c_bind_macro_declared(ctx, macro, position, argument);
            position++;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

/* `FOR_ALL_TABS(tp) {` reads as a function definition nested in a function, which C does not
 * have: under the C grammar a function typed FOR_ALL_TABS named (tp), under the C++ grammar a
 * typeless function FOR_ALL_TABS with one parameter of type tp. */
static void c_bind_macro_loop_header(CLSPContext *ctx, TSNode definition) {
    if (!ctx->current_scope || !ctx->current_scope->parent || !ctx->c_decls)
        return;
    TSNode declarator = ts_node_child_by_field_name(definition, "declarator", 10);
    if (ts_node_is_null(declarator))
        return;
    const char *kind = ts_node_type(declarator);
    if (!strcmp(kind, "parenthesized_declarator")) {
        TSNode type = ts_node_child_by_field_name(definition, "type", 4);
        TSNode name = ts_node_named_child_count(declarator) == 1
                          ? ts_node_named_child(declarator, 0)
                          : (TSNode){0};
        const char *macro = ts_node_is_null(type) ? NULL : c_node_text(ctx, type);
        if (macro && !ts_node_is_null(name) && !strcmp(ts_node_type(name), "identifier"))
            c_bind_macro_declared(ctx, macro, 0, name);
        return;
    }
    if (strcmp(kind, "function_declarator"))
        return;
    TSNode callee = ts_node_child_by_field_name(declarator, "declarator", 10);
    TSNode params = ts_node_child_by_field_name(declarator, "parameters", 10);
    const char *macro = !ts_node_is_null(callee) && !strcmp(ts_node_type(callee), "identifier")
                            ? c_node_text(ctx, callee)
                            : NULL;
    if (!macro || ts_node_is_null(params))
        return;
    int position = 0;
    TSTreeCursor cursor = ts_tree_cursor_new(params);
    if (ts_tree_cursor_goto_first_child(&cursor))
        do {
            TSNode param = ts_tree_cursor_current_node(&cursor);
            if (!ts_node_is_named(param) || !strcmp(ts_node_type(param), "comment"))
                continue;
            TSNode name = ts_node_child_by_field_name(param, "type", 4);
            if (!ts_node_is_null(name) && !strcmp(ts_node_type(name), "type_identifier") &&
                ts_node_named_child_count(param) == 1)
                c_bind_macro_declared(ctx, macro, position, name);
            position++;
        } while (ts_tree_cursor_goto_next_sibling(&cursor));
    ts_tree_cursor_delete(&cursor);
}

static void c_emit_type_reference(CLSPContext *ctx, TSNode node);

/* `sizeof(raxNode)`: the grammar knows no types and reads a parenthesized value. */
static bool c_is_sizeof_operand(TSNode node) {
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent) || strcmp(ts_node_type(parent), "parenthesized_expression"))
        return false;
    TSNode operation = ts_node_parent(parent);
    return !ts_node_is_null(operation) && !strcmp(ts_node_type(operation), "sizeof_expression");
}

static void c_emit_value_reference(CLSPContext *ctx, TSNode node) {
    if (!ctx->c_decls || !ctx->enclosing_func_qn)
        return;
    TSNode parent = ts_node_parent(node);
    const char *parent_kind = ts_node_is_null(parent) ? "" : ts_node_type(parent);
    /* `container_of(p, struct X, m)`: no grammar has a type among call arguments, and the keyword
     * lands in an error just before the name. That name is a tag and no value. */
    if (!strcmp(parent_kind, "argument_list") && ts_node_has_error(parent)) {
        TSNode before = ts_node_prev_sibling(node);
        const char *keyword = !ts_node_is_null(before) && !strcmp(ts_node_type(before), "ERROR")
                                  ? c_node_text(ctx, before)
                                  : NULL;
        if (keyword &&
            (!strcmp(keyword, "struct") || !strcmp(keyword, "union") || !strcmp(keyword, "enum"))) {
            c_emit_type_reference(ctx, node);
            return;
        }
    }
    if (c_bare_argument_of_macro(ctx, node))
        return;
    if (!strcmp(parent_kind, "qualified_identifier")) {
        c_emit_scoped_enumerator(ctx, parent, node);
        return;
    }
    /* Only expression positions: a declarator names what it declares, and a callee
     * is the call's own proof. */
    static const char *const not_values[] = {"init_declarator",
                                             "pointer_declarator",
                                             "array_declarator",
                                             "function_declarator",
                                             "parameter_declaration",
                                             "declaration",
                                             "attribute_specifier",
                                             "preproc_defined",
                                             "qualified_identifier",
                                             "field_designator",
                                             "enumerator",
                                             "parenthesized_declarator"};
    for (size_t i = 0; i < sizeof(not_values) / sizeof(not_values[0]); i++) {
        if (strcmp(parent_kind, not_values[i]))
            continue;
        TSNode value = ts_node_child_by_field_name(parent, "value", 5);
        if (ts_node_is_null(value) || !ts_node_eq(value, node))
            return;
    }
    if (!strcmp(parent_kind, "call_expression")) {
        TSNode function = ts_node_child_by_field_name(parent, "function", 8);
        if (!ts_node_is_null(function) && ts_node_eq(function, node))
            return;
    }
    const char *name = c_node_text(ctx, node);
    if (name && c_is_template_parameter(ctx, name))
        return;
    if (!name || c_name_bound_locally(ctx, name))
        return;
    if (c_proof_macro_visible(ctx, name, ts_node_start_byte(node))) {
        c_macro_body_walk(ctx, name, node, false);
        return;
    }
    if (ctx->proof_method_class) {
        /* Inside a member function the class comes first. A field read here has no
         * receiver in the source: the class of the function is the receiver. */
        uint32_t at = ts_node_start_byte(node);
        const CBMRegisteredType *declarer =
            c_proof_field_declarer(ctx, ctx->proof_method_class, name, at, 0, NULL);
        if (declarer) {
            cbm_resolve_result_t typed = c_proof_method_class_lookup(ctx, node);
            const char *field =
                cbm_arena_sprintf(ctx->arena, "%s.%s", declarer->qualified_name, name);
            if (field && cbm_resolve_admit(&typed)) {
                cbm_resolve_result_t member =
                    c_proof_member_lookup(ctx, name, node, field, typed.proof);
                if (cbm_resolve_admit(&member))
                    c_emit_proven(ctx, &member, name, CBM_RESOLVED_VALUE_REFERENCE, node);
            }
            return;
        }
        const char *enumerator =
            c_proof_class_enumerator(ctx, ctx->proof_method_class, name, at, 0);
        if (enumerator) {
            cbm_resolve_result_t typed = c_proof_method_class_lookup(ctx, node);
            if (cbm_resolve_admit(&typed)) {
                cbm_resolve_result_t member =
                    c_proof_member_lookup(ctx, name, node, enumerator, typed.proof);
                if (cbm_resolve_admit(&member))
                    c_emit_proven(ctx, &member, name, CBM_RESOLVED_VALUE_REFERENCE, node);
            }
            return;
        }
        CMemberHit hit = {0};
        c_proof_class_method(ctx, ctx->proof_method_class, name, -1, at, 0, false, &hit);
        if (hit.target || hit.open)
            return;
    }
    cbm_resolve_result_t proof = c_proof_value(ctx, name, node);
    /* No object or enumerator of this name: a function's name is a value too, as in
     * `.call = luaEngineCall`, `hook = intercept;` and a table of `{ 'Z', nv_Zet }`. Ordinary
     * lookup selects it as it selects a callee, and nothing is called, so nothing is added by
     * argument-dependent lookup. */
    if (proof.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
        proof.binding == CBM_RESOLVE_BINDING_UNRESOLVED) {
        ctx->proof_class_scope_checked = ctx->proof_method_class != NULL;
        proof = c_proof_name_lookup(ctx, name, ts_node_start_byte(node));
        ctx->proof_class_scope_checked = false;
    }
    if (cbm_resolve_admit(&proof))
        c_emit_proven(ctx, &proof, name, CBM_RESOLVED_VALUE_REFERENCE, node);
    else if (proof.analysis == CBM_RESOLVE_ANALYSIS_COMPLETE &&
             proof.binding == CBM_RESOLVE_BINDING_UNRESOLVED && c_is_sizeof_operand(node))
        c_emit_type_reference(ctx, node);
}

/* A function can declare its own `enum { LIMIT = 8 }` or `struct node { ... }`. Those
 * names are in block scope and hide any file-scope namesake from here on. A lone `struct node;`
 * does the same: it declares a new tag here, where `struct node *p;` would use the outer one. */
static void c_bind_local_tag_and_enumerators(CLSPContext *ctx, TSNode node, const char *kind) {
    if (strcmp(kind, "struct_specifier") && strcmp(kind, "union_specifier") &&
        strcmp(kind, "enum_specifier") && strcmp(kind, "class_specifier"))
        return;
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (ts_node_is_null(body)) {
        TSNode parent = ts_node_parent(node);
        const char *around = ts_node_is_null(parent) ? "" : ts_node_type(parent);
        if (strcmp(around, "compound_statement") &&
            (strcmp(around, "declaration") ||
             !ts_node_is_null(ts_node_child_by_field_name(parent, "declarator", 10))))
            return;
    }
    TSNode tag = ts_node_child_by_field_name(node, "name", 4);
    const char *tag_name = ts_node_is_null(tag) ? NULL : c_node_text(ctx, tag);
    if (tag_name) {
        c_scope_bind(ctx, tag_name, cbm_type_unknown());
        const char *tag_key = c_local_tag_key(ctx, tag_name);
        if (tag_key) {
            c_scope_bind(ctx, tag_key, cbm_type_unknown());
            ctx->local_tag_declared = true;
        }
    }
    if (strcmp(kind, "enum_specifier"))
        return;
    uint32_t count = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode item = ts_node_named_child(body, i);
        TSNode name = strcmp(ts_node_type(item), "enumerator")
                          ? (TSNode){0}
                          : ts_node_child_by_field_name(item, "name", 4);
        const char *text = ts_node_is_null(name) ? NULL : c_node_text(ctx, name);
        if (text)
            c_scope_bind(ctx, text, cbm_type_unknown());
    }
}

/* Does the class this code is in declare this name, as something other than the type found? */
static bool c_enclosing_class_declares_other(CLSPContext *ctx, const char *name,
                                             const CBMRegisteredType *found) {
    const char *qn =
        ctx->proof_method_class ? ctx->proof_method_class->qualified_name : ctx->enclosing_class_qn;
    const char *dot = qn ? strrchr(qn, '.') : NULL;
    const char *klass = dot ? dot + 1 : qn;
    if (!klass)
        return false;
    /* The class itself, whose constructors share its name, or a type nested in it. */
    size_t length = strlen(qn);
    if (found->qualified_name && !strncmp(found->qualified_name, qn, length) &&
        (!found->qualified_name[length] || found->qualified_name[length] == '.'))
        return false;
    for (const CBMCDeclLink *link = cbm_c_decl_lookup(ctx->c_decls, name); link;
         link = link->next) {
        const CBMCDecl *decl = link->decl;
        if (decl->kind == CBM_C_DECL_MEMBER_NAME && !strcmp(decl->name, name) && decl->type_name &&
            !strcmp(decl->type_name, klass))
            return true;
    }
    return false;
}

/* A written type name. Its declaration in this translation unit is the target,
 * which is a typedef's own node, not what the typedef aliases. */
static void c_emit_type_reference(CLSPContext *ctx, TSNode node) {
    if (!ctx->enclosing_func_qn)
        return;
    TSNode parent = ts_node_parent(node);
    const char *parent_kind = ts_node_is_null(parent) ? "" : ts_node_type(parent);
    /* `Tpl<Name>::x` is not looked up from here. `Tpl<int>` names Tpl, and `ns::Name` names what
     * the whole qualified name does. */
    const char *qualified = NULL;
    if (!strcmp(parent_kind, "qualified_identifier")) {
        TSNode top = parent;
        for (int hop = 0; hop < 16; hop++) {
            TSNode above = ts_node_parent(top);
            if (ts_node_is_null(above) || strcmp(ts_node_type(above), "qualified_identifier"))
                break;
            top = above;
        }
        TSNode named = ts_node_child_by_field_name(parent, "name", 4);
        qualified =
            !ts_node_is_null(named) && ts_node_eq(named, node) ? c_node_text(ctx, top) : NULL;
        if (!qualified)
            return;
    } else if (!strcmp(parent_kind, "template_type")) {
        TSNode named = ts_node_child_by_field_name(parent, "name", 4);
        if (ts_node_is_null(named) || !ts_node_eq(named, node))
            return;
        /* `ns::Box<int>`: the qualified name up to the arguments. */
        TSNode above = ts_node_parent(parent);
        if (!ts_node_is_null(above) && !strcmp(ts_node_type(above), "qualified_identifier")) {
            TSNode last = ts_node_child_by_field_name(above, "name", 4);
            if (ts_node_is_null(last) || !ts_node_eq(last, parent))
                return;
            TSNode top = above;
            for (int hop = 0; hop < 16; hop++) {
                TSNode outer = ts_node_parent(top);
                if (ts_node_is_null(outer) || strcmp(ts_node_type(outer), "qualified_identifier"))
                    break;
                top = outer;
            }
            const char *text = c_node_text(ctx, top);
            qualified = text ? cbm_arena_strndup(ctx->arena, text,
                                                 ts_node_end_byte(node) - ts_node_start_byte(top))
                             : NULL;
            if (!qualified)
                return;
        }
    } else if (!strcmp(parent_kind, "scoped_type_identifier")) {
        return;
    }
    /* `struct node {...}` defines a tag here rather than using one. */
    bool in_specifier =
        !strcmp(parent_kind, "struct_specifier") || !strcmp(parent_kind, "union_specifier") ||
        !strcmp(parent_kind, "class_specifier") || !strcmp(parent_kind, "enum_specifier");
    if (in_specifier && !ts_node_is_null(ts_node_child_by_field_name(parent, "body", 4)))
        return;
    /* `struct X` and `container_of(p, struct X, m)` ask among tags, which a variable named X does
     * not hide. */
    bool is_tag = in_specifier || !strcmp(parent_kind, "argument_list");
    const char *name = c_node_text(ctx, node);
    if (!name || !ctx->module_qn ||
        (!qualified && (c_is_template_parameter(ctx, name) ||
                        (is_tag ? c_local_tag_hides(ctx, name) : c_name_bound_locally(ctx, name)))))
        return;
    /* `#define sds hisds`, `#define RedisModuleString robj`: the type is what the macro names. */
    if (!qualified && c_proof_macro_visible(ctx, name, ts_node_start_byte(node))) {
        c_macro_body_walk(ctx, name, node, false);
        return;
    }
    uint32_t at = ts_node_start_byte(node);
    const CBMRegisteredType *type = c_proof_type_by_name(ctx, qualified ? qualified : name, at);
    /* A tag, which may not be the type found for that spelling. */
    if (type && is_tag && c_tag_is_other_than(ctx, type, name, qualified != NULL, at))
        return;
    /* A member type of the class around this code hides a namesake outside the class. */
    if (type && !qualified && ctx->cpp_mode && c_enclosing_class_declares_other(ctx, name, type))
        return;
    if (!type || !type->project_source || !type->graph_qualified_name ||
        !c_proof_type_visible(ctx, type->qualified_name, at))
        return;
    /* A tag and an alias of the same spelling are one name to a C reader. Several
     * different typedefs of it are not, whichever the lookup above happened to keep. */
    cbm_resolve_result_t proof = cbm_lsp_proof_type_lookup(
        &ctx->field_types, name, node, "c-written-type-name", type->qualified_name,
        type->declaration_identity, type->graph_qualified_name, type->source_flags,
        "visible-type-declaration");
    proof = c_proof_type_import(ctx, type->qualified_name, at, proof);
    if (cbm_resolve_admit(&proof))
        c_emit_proven(ctx, &proof, name, CBM_RESOLVED_TYPE_REFERENCE, node);
}

/* The types in a function's own signature belong to that function's usage. */
static void c_emit_signature_types(CLSPContext *ctx, TSNode node, int depth) {
    if (ts_node_is_null(node) || depth > 32)
        return;
    if (!strcmp(ts_node_type(node), "type_identifier")) {
        c_emit_type_reference(ctx, node);
        return;
    }
    uint32_t count = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < count; i++)
        c_emit_signature_types(ctx, ts_node_named_child(node, i), depth + 1);
}

/* A direct identifier argument denotes a precise callable value only when its
 * nearest lexical binding is a tracked function pointer target, or when no
 * lexical value shadows one exact registered function. Conditional/composite
 * expressions are intentionally not walked here and remain ordinary USAGE. */
static void c_resolve_callable_argument_references(CLSPContext *ctx, TSNode call) {
    TSNode arguments = ts_node_child_by_field_name(call, "arguments", 9);
    if (ts_node_is_null(arguments))
        return;
    uint32_t count = ts_node_named_child_count(arguments);
    for (uint32_t i = 0; i < count; i++) {
        TSNode argument = ts_node_named_child(arguments, i);
        const char *kind = ts_node_type(argument);
        if ((!strcmp(kind, "identifier") || !strcmp(kind, "qualified_identifier") ||
             !strcmp(kind, "scoped_identifier")) &&
            !c_bare_argument_of_macro(ctx, argument)) {
            cbm_resolve_result_t proof = c_callable_proof(ctx, argument);
            c_emit_proven(ctx, &proof, c_node_text(ctx, argument), CBM_RESOLVED_CALL_REFERENCE,
                          argument);
        }
        /* A C++ parameter may bind a reference to the pointer's storage.
         * Parentheses do not change that storage or establish a value-copy
         * parameter contract. The reference occurrence role stays unchanged. */
        TSNode storage = c_fp_unwrap_parentheses(ctx, argument);
        if (ctx->cpp_mode && !ts_node_is_null(storage) &&
            !strcmp(ts_node_type(storage), "identifier"))
            c_escape_fp_storage(ctx, c_node_text(ctx, storage));
    }
}

/* Composite arguments retain ordinary USAGE semantics in the extraction
 * carrier. Their individual function names still have exact declaration
 * identities; proving those leaves must not invent an invocation or claim
 * that the composite expression selects one callable value. */
static void c_resolve_callable_value_reference(CLSPContext *ctx, TSNode node) {
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "identifier") && strcmp(kind, "qualified_identifier") &&
        strcmp(kind, "scoped_identifier"))
        return;
    TSNode parent = ts_node_parent(node);
    if (ts_node_is_null(parent))
        return;
    const char *pk = ts_node_type(parent);
    if (strcmp(pk, "conditional_expression") && strcmp(pk, "parenthesized_expression") &&
        strcmp(pk, "return_statement") && strcmp(pk, "binary_expression"))
        return;
    cbm_resolve_result_t proof = c_callable_proof(ctx, node);
    c_emit_proven(ctx, &proof, c_node_text(ctx, node), CBM_RESOLVED_CALL_REFERENCE, node);
}

static void c_emit_unresolved_call(CLSPContext *ctx, const char *expr_text, const char *reason) {
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn)
        return;
    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_func_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    rc.source_origin = ctx->source_origin;
    c_publish_resolved_call(ctx, rc);
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
    if (ctx->eval_failure)
        return;
    if (ctx->walk_depth >= C_LSP_MAX_WALK_DEPTH)
        return;
    ctx->walk_depth++;
    c_resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}

/* What this one node does, apart from walking its children. Kept out of the recursive
 * function: its locals would otherwise be in every frame of the walk, and
 * C_LSP_MAX_WALK_DEPTH nested frames have to fit the stack. */
__attribute__((noinline)) static void c_resolve_node_operations(CLSPContext *ctx, TSNode node,
                                                                const char *kind) {
    if (!strcmp(kind, "identifier"))
        c_emit_value_reference(ctx, node);
    else if (!strcmp(kind, "type_identifier"))
        c_emit_type_reference(ctx, node);
    else if (ctx->current_scope && ctx->current_scope->parent) {
        c_bind_local_tag_and_enumerators(ctx, node, kind);
        c_bind_declared_names(ctx, node, kind);
    }

    // --- Member access: publish which type owns the field ---
    if (strcmp(kind, "field_expression") == 0) {
        c_emit_field_reference(ctx, node);
    }

    // --- Resolve call expressions ---
    if (strcmp(kind, "call_expression") == 0) {
        TSNode func_node = ts_node_child_by_field_name(node, "function", 8);
        if (ctx->c_decls && !ts_node_is_null(func_node) &&
            !strcmp(ts_node_type(func_node), "identifier")) {
            const char *callee = c_node_text(ctx, func_node);
            if (callee && c_proof_macro_visible(ctx, callee, ts_node_start_byte(node))) {
                c_bind_macro_declared_arguments(ctx, node, callee);
                c_macro_body_walk(ctx, callee, node, true);
            }
        }
        c_resolve_callable_argument_references(ctx, node);
        if (!ts_node_is_null(func_node)) {
            const char *fk = ts_node_type(func_node);
            /* Inside a member function the class's own scope comes first, and an
             * unqualified name it might declare through an unknown base is unknown. */
            ctx->proof_class_scope_checked = false;
            if (ctx->proof_method_class &&
                (!strcmp(fk, "identifier") || !strcmp(fk, "template_function"))) {
                const char *written = c_template_id_name(ctx, func_node);
                uint32_t at = ts_node_start_byte(node);
                if (written && !c_name_bound_locally(ctx, written) &&
                    !c_proof_macro_visible(ctx, written, at)) {
                    CMemberHit hit = {0};
                    c_proof_class_method(ctx, ctx->proof_method_class, written,
                                         c_call_argument_count(node), at, 0, false, &hit);
                    if (hit.target && !hit.conflict) {
                        cbm_resolve_result_t typed = c_proof_method_class_lookup(ctx, node);
                        cbm_resolve_result_t member =
                            cbm_resolve_admit(&typed)
                                ? c_proof_member_lookup(ctx, written, func_node, hit.target,
                                                        typed.proof)
                                : typed;
                        c_emit_proven(ctx, &member, written, CBM_RESOLVED_INVOCATION, node);
                    }
                    if (hit.target || hit.open ||
                        c_proof_field_declarer(ctx, ctx->proof_method_class, written, at, 0, NULL))
                        return;
                    ctx->proof_class_scope_checked = true;
                }
            }
            cbm_resolve_result_t proof = c_callable_proof(ctx, func_node);
            ctx->proof_class_scope_checked = false;
            if (cbm_resolve_admit(&proof) && c_proof_call_domain(ctx, node, func_node, &proof)) {
                /* `usesLayout<int>(1)` calls usesLayout: the arguments are no part of its name. */
                const char *called = c_template_id_name(ctx, func_node);
                proof = cbm_lsp_proof_rule(
                    &ctx->field_types, proof, node, "c-call-lookup-domain", called,
                    "qualified-or-bound-callable-or-no-associated-argument-types", NULL);
                c_emit_proven(ctx, &proof, called, CBM_RESOLVED_INVOCATION, node);
                return;
            }

            if (c_cxx_rules(ctx) && !strcmp(fk, "qualified_identifier")) {
                cbm_resolve_result_t scoped = c_proof_scoped_member(ctx, func_node, node);
                if (cbm_resolve_admit(&scoped)) {
                    c_emit_proven(ctx, &scoped, c_node_text(ctx, func_node),
                                  CBM_RESOLVED_INVOCATION, node);
                    return;
                }
            }

            // field_expression: obj.method() or ptr->method()
            if (strcmp(fk, "field_expression") == 0) {
                cbm_resolve_result_t method = c_proof_direct_method(ctx, func_node, node);
                if (cbm_resolve_admit(&method)) {
                    c_emit_proven(ctx, &method, c_node_text(ctx, func_node),
                                  CBM_RESOLVED_INVOCATION, node);
                    return;
                }
                if (c_cxx_rules(ctx)) {
                    const cbm_resolve_proof_t *receiver_proof = NULL;
                    const CBMType *receiver_shape = NULL;
                    const CBMRegisteredType *receiver = c_proof_expression_owner(
                        ctx, ts_node_child_by_field_name(func_node, "argument", 8), 0,
                        &receiver_proof, &receiver_shape);
                    TSNode member_name = ts_node_child_by_field_name(func_node, "field", 5);
                    const char *written =
                        ts_node_is_null(member_name) ? NULL : c_node_text(ctx, member_name);
                    uint32_t at = ts_node_start_byte(node);
                    /* A field of this name holds a callable, not a member function. */
                    if (receiver && receiver_proof && written &&
                        c_cxx_access_is_builtin(ctx, func_node, receiver_shape) &&
                        !c_proof_field_declarer(ctx, receiver, written, at, 0, NULL)) {
                        CMemberHit hit = {0};
                        c_proof_class_method(ctx, receiver, written, c_call_argument_count(node),
                                             at, 0, false, &hit);
                        if (hit.target && !hit.conflict) {
                            const char *spelling = c_node_text(ctx, func_node);
                            cbm_resolve_result_t member = c_proof_member_lookup(
                                ctx, spelling, func_node, hit.target, receiver_proof);
                            c_emit_proven(ctx, &member, spelling, CBM_RESOLVED_INVOCATION, node);
                            return;
                        }
                    }
                }
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
                                // Check if resolved through base class — prefer derived
                                // override
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
                                } else if (c_base_declares_member(ctx, type_qn, field_name)) {
                                    // Method resolved directly on type_qn but also
                                    // declared in a base → a derived override of an
                                    // inherited (virtual) method → polymorphic dispatch.
                                    strategy = "lsp_virtual_dispatch";
                                }
                                // Check if through smart pointer
                                if (is_arrow && obj_type->kind == CBM_TYPE_TEMPLATE &&
                                    is_smart_ptr(obj_type->data.template_type.template_name))
                                    strategy = "lsp_smart_ptr_dispatch";
                                c_emit_resolved_call_at(ctx, method->qualified_name, strategy,
                                                        0.95f, node);
                                return;
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
                return;
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
                        // Only strip if no '.' follows the '<' (i.e., template is in the
                        // last segment)
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
                        c_emit_resolved_call_at(ctx, f->qualified_name, "lsp_scoped", 0.95f, node);
                        return;
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
                            c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_scoped", 0.95f,
                                                    node);
                            return;
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
                            c_emit_resolved_call_at(ctx, nf->qualified_name, "lsp_scoped", 0.90f,
                                                    node);
                            return;
                        }
                    }
                    // Class::method of a class declared in another file (#1153).
                    if (dot) {
                        const char *scope_class = c_resolve_scope_class_qn(
                            ctx, cbm_arena_strndup(ctx->arena, qn, (size_t)(dot - qn)));
                        const CBMRegisteredFunc *sm = NULL;
                        if (scope_class) {
                            sm = cbm_registry_lookup_method_by_types(ctx->registry, scope_class,
                                                                     dot + 1, arg_types, arg_count);
                            if (!sm) {
                                sm = c_lookup_scope_member(ctx, scope_class, dot + 1);
                            }
                        }
                        if (sm) {
                            c_emit_resolved_call_at(ctx, sm->qualified_name, "lsp_scoped", 0.95f,
                                                    node);
                            return;
                        }
                    }
                    c_emit_unresolved_call(ctx, qn, "scoped_not_in_registry");
                }
                return;
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
                            c_emit_resolved_call_at(ctx, f->qualified_name, "lsp_template", 0.95f,
                                                    node);
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
                return;
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
                        // The textual callee is the pointer variable `name` (e.g.
                        // `fp`), resolved to a differently named target. Pass it
                        // as orig so the join matches the call on the pointer name.
                        c_emit_resolved_call_orig_at(ctx, fp_target, name,
                                                     is_dll ? "lsp_dll_resolve" : "lsp_func_ptr",
                                                     is_dll ? 0.80f : 0.85f, node);
                        return;
                    }

                    // Check if it's a variable with callable type
                    const CBMType *var_type = cbm_scope_lookup(ctx->current_scope, name);
                    if (!cbm_type_is_unknown(var_type)) {
                        if (var_type->kind == CBM_TYPE_FUNC) {
                            // Function type variable without tracked target — can't resolve
                            return;
                        }
                        // Functor call: variable with operator()
                        const CBMType *base = c_simplify_type(ctx, var_type, false);
                        const char *type_qn = type_to_qn(base);
                        if (type_qn) {
                            const CBMRegisteredFunc *op =
                                c_lookup_member(ctx, type_qn, "operator()");
                            if (op) {
                                c_emit_resolved_call_at(ctx, op->qualified_name, "lsp_operator",
                                                        0.90f, node);
                                return;
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
                            c_emit_resolved_call_at(ctx, ctor_qn, "lsp_constructor", 0.95f, node);
                            return;
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
                        c_emit_resolved_call_at(ctx, fqn, strategy, 0.95f, node);
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
                            c_emit_resolved_call_at(ctx, adl_qn, "lsp_adl", 0.90f, node);
                        } else {
                            c_emit_unresolved_call(ctx, name, "function_not_in_registry");
                        }
                    }
                }
                return;
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
                c_emit_resolved_call_at(ctx, ctor_qn, "lsp_constructor", 0.95f, node);
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
                const char *dtor_name = cbm_arena_sprintf(ctx->arena, "~%s", short_name);
                const CBMRegisteredFunc *dtor =
                    c_lookup_member_operator_by_arity(ctx, type_qn, dtor_name, 0);
                if (dtor) {
                    c_emit_resolved_call_at(ctx, dtor->qualified_name, "lsp_destructor", 0.90f,
                                            node);
                }
            }
        }
    }

    // --- Operator calls (C++ only) ---
    if (ctx->cpp_mode && strcmp(kind, "binary_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(left) && !ts_node_is_null(right)) {
            const CBMType *lhs_type = c_eval_expr_type(ctx, left);
            const CBMType *rhs_type = c_eval_expr_type(ctx, right);
            const CBMType *base = c_simplify_type(ctx, lhs_type, false);
            // Extract operator. Prefer a member overload on the lhs type, then
            // perform exact argument-dependent lookup for a free overload.
            for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
                TSNode child = ts_node_child(node, i);
                if (!ts_node_is_named(child)) {
                    char *op = c_node_text(ctx, child);
                    if (op) {
                        const char *op_name = cbm_arena_sprintf(ctx->arena, "operator%s", op);
                        const CBMRegisteredFunc *m = NULL;
                        if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base)) {
                            const char *type_qn = type_to_qn(base);
                            if (type_qn) {
                                m = c_lookup_member(ctx, type_qn, op_name);
                                if (m)
                                    c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_operator",
                                                            0.90f, node);
                            }
                        }
                        if (!m) {
                            m = c_lookup_free_operator(ctx, op_name, lhs_type, rhs_type);
                            if (m) {
                                c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_operator_adl",
                                                        0.90f, node);
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    // --- Compound assignment operator calls (C++ only): a += b, a -= b, etc. ---
    if (ctx->cpp_mode && strcmp(kind, "assignment_expression") == 0) {
        TSNode left = ts_node_child_by_field_name(node, "left", 4);
        TSNode right = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(left) && !ts_node_is_null(right)) {
            const CBMType *lhs_type = c_eval_expr_type(ctx, left);
            const CBMType *rhs_type = c_eval_expr_type(ctx, right);
            const CBMType *base = c_simplify_type(ctx, lhs_type, false);
            // Extract only the compound-assignment operators. Plain `=` is
            // never an overload candidate at this occurrence.
            for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
                TSNode child = ts_node_child(node, i);
                if (!ts_node_is_named(child)) {
                    char *op = c_node_text(ctx, child);
                    if (c_is_compound_assignment_operator(op)) {
                        const char *op_name = cbm_arena_sprintf(ctx->arena, "operator%s", op);
                        const CBMRegisteredFunc *m = NULL;
                        if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base)) {
                            const char *type_qn = type_to_qn(base);
                            if (type_qn) {
                                m = c_lookup_member_operator_by_arity(ctx, type_qn, op_name, 1);
                                if (m) {
                                    c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_operator",
                                                            0.90f, node);
                                }
                            }
                        }
                        if (!m) {
                            m = c_lookup_free_operator(ctx, op_name, lhs_type, rhs_type);
                            if (m) {
                                c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_operator_adl",
                                                        0.90f, node);
                            }
                        }
                    }
                    break;
                }
            }
        }
    }

    // --- field_expression operator-> ---
    if (ctx->cpp_mode && strcmp(kind, "field_expression") == 0) {
        TSNode receiver = ts_node_child_by_field_name(node, "argument", 8);
        TSNode member = ts_node_child_by_field_name(node, "field", 5);
        bool is_arrow = false;
        for (uint32_t i = 0; i < ts_node_child_count(node); i++) {
            TSNode child = ts_node_child(node, i);
            if (!ts_node_is_named(child)) {
                const char *token = c_node_text(ctx, child);
                if (token && strcmp(token, "->") == 0) {
                    is_arrow = true;
                    break;
                }
            }
        }
        if (is_arrow && !ts_node_is_null(receiver) && !ts_node_is_null(member)) {
            const CBMType *receiver_type = c_eval_expr_type(ctx, receiver);
            const CBMType *base = c_simplify_type(ctx, receiver_type, false);
            if (base && base->kind != CBM_TYPE_BUILTIN && base->kind != CBM_TYPE_POINTER &&
                !cbm_type_is_unknown(base)) {
                const char *type_qn = type_to_qn(base);
                const CBMRegisteredFunc *method =
                    type_qn ? c_lookup_member_operator_by_arity(ctx, type_qn, "operator->", 0)
                            : NULL;
                if (method) {
                    c_emit_resolved_call_at(ctx, method->qualified_name, "lsp_operator", 0.90f,
                                            node);
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
                    const CBMRegisteredFunc *m =
                        c_lookup_member_operator_by_arity(ctx, type_qn, "operator[]", 1);
                    if (m) {
                        c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_operator", 0.90f,
                                                node);
                    }
                }
            }
        }
    }

    // --- unary_expression/pointer_expression: operator* / operator++ /
    // operator-- on custom types
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
                    TSNode operator_node = {0};
                    for (uint32_t ui = 0; ui < ts_node_child_count(node); ui++) {
                        TSNode ch = ts_node_child(node, ui);
                        if (!ts_node_is_named(ch)) {
                            char *op = c_node_text(ctx, ch);
                            if (!op)
                                continue;
                            if (strcmp(op, "*") == 0)
                                op_name = "operator*";
                            else if (strcmp(op, "-") == 0)
                                op_name = "operator-";
                            else if (strcmp(op, "++") == 0)
                                op_name = "operator++";
                            else if (strcmp(op, "--") == 0)
                                op_name = "operator--";
                            else if (strcmp(op, "!") == 0)
                                op_name = "operator!";
                            operator_node = ch;
                            break;
                        }
                    }
                    if (op_name) {
                        int explicit_arg_count = 0;
                        if (strcmp(kind, "update_expression") == 0 &&
                            !ts_node_is_null(operator_node) &&
                            ts_node_start_byte(operator_node) >= ts_node_end_byte(operand)) {
                            explicit_arg_count = 1; // postfix ++/-- carries the dummy int
                        }
                        const CBMRegisteredFunc *m = c_lookup_member_operator_by_arity(
                            ctx, type_qn, op_name, explicit_arg_count);
                        if (m) {
                            c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_operator", 0.90f,
                                                    node);
                        } else if (explicit_arg_count == 0) {
                            m = c_lookup_free_unary_operator(ctx, op_name, op_type);
                            if (m) {
                                c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_operator_adl",
                                                        0.90f, node);
                            }
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
                            // If assigning an object of the same type -> copy/move
                            // constructor
                            const char *type_qn = type_to_qn(decl_type);
                            const char *val_qn = type_to_qn(val_base);
                            if (type_qn && val_qn && strcmp(type_qn, val_qn) == 0) {
                                const char *short_name = strrchr(type_qn, '.');
                                short_name = short_name ? short_name + 1 : type_qn;
                                const char *ctor_qn =
                                    cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, short_name);
                                c_emit_resolved_call_at(ctx, ctor_qn, "lsp_copy_constructor", 0.85f,
                                                        node);
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
            // The `condition` field is a `condition_clause` wrapping the `( expr )`;
            // unwrap it to the inner expression so its type evaluates (a clause
            // node has no type, so `if (obj)` would never resolve obj's type).
            if (strcmp(ts_node_type(cond), "condition_clause") == 0 &&
                ts_node_named_child_count(cond) == 1) {
                cond = ts_node_named_child(cond, 0);
            }
            // If condition is a single expression of a custom type with operator bool
            const CBMType *cond_type = c_eval_expr_type(ctx, cond);
            const CBMType *base = c_simplify_type(ctx, cond_type, false);
            if (base && base->kind != CBM_TYPE_BUILTIN && !cbm_type_is_unknown(base) &&
                base->kind != CBM_TYPE_POINTER) {
                const char *type_qn = type_to_qn(base);
                if (type_qn) {
                    const CBMRegisteredFunc *m = c_lookup_member(ctx, type_qn, "operator bool");
                    if (m) {
                        c_emit_resolved_call_at(ctx, m->qualified_name, "lsp_conversion", 0.85f,
                                                node);
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
                // it will be resolved by the normal call_expression handler during
                // recursion. Just let the recursion handle it — no special emission
                // needed.
            }
        }
        // throw; (rethrow) — no calls, just skip
    }

    // --- co_yield_statement: co_yield expr → resolve inner expression calls ---
    if (strcmp(kind, "co_yield_statement") == 0) {
        // co_yield's operand is processed by normal recursion into children
    }

    // --- co_return_statement: co_return expr → resolve inner expression calls
    // ---
    if (strcmp(kind, "co_return_statement") == 0) {
        // co_return's operand is processed by normal recursion into children
    }

    // --- Designated initializer: {.field = value} ---
    if (strcmp(kind, "initializer_pair") == 0 || strcmp(kind, "field_designator") == 0) {
        // These appear inside initializer_list for aggregate init
        // We don't emit CALLS for field access, but we could track which struct is
        // being initialized This is primarily for completeness — no CALLS edge
        // needed for field designators
    }
}

static void c_resolve_calls_in_node_inner(CLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    /* A directive holds names of macros, not uses of declarations. */
    if (!strcmp(kind, "preproc_def") || !strcmp(kind, "preproc_function_def") ||
        !strcmp(kind, "preproc_call") || !strcmp(kind, "preproc_include"))
        return;

    if (ctx->test_definition_owners && ctx->test_definition_owners->has_test_definition_owners &&
        ctx->test_definition_owners->test_owner_language == CBM_LANG_C) {
        if (c_process_configured_test_body(ctx, node))
            return;
        /* Nested native functions are independent owners even when they occur
         * inside the raw body of a configured C definition. */
        if (strcmp(kind, "function_definition") == 0) {
            c_process_function(ctx, node);
            return;
        }
    }

    /* Declarations establish scope before their initializer. Assignments
     * take effect after their operands, never before a nested RHS write. */
    bool assignment = !strcmp(kind, "assignment_expression");
    if (!assignment)
        c_process_statement(ctx, node);
    if (!strcmp(kind, "function_definition"))
        c_bind_macro_loop_header(ctx, node);
    int initializer_fp = -1;
    const cbm_resolve_proof_t *initializer_origin = NULL;
    if (!strcmp(kind, "init_declarator")) {
        TSNode name =
            c_fp_declarator_name(ctx, ts_node_child_by_field_name(node, "declarator", 10), NULL);
        if (!ts_node_is_null(name))
            initializer_fp = c_find_fp_binding_index(ctx, c_node_text(ctx, name));
        if (initializer_fp >= 0)
            initializer_origin = ctx->fp_target_proofs[initializer_fp];
    }

    // --- Initializers: hand the initialized type down to the list ---
    if (strcmp(kind, "initializer_list") == 0) {
        c_walk_initializer_list(ctx, node);
        return;
    }
    if (strcmp(kind, "init_declarator") == 0) {
        c_hint_declared_initializer(ctx, node);
    } else if (strcmp(kind, "compound_literal_expression") == 0) {
        TSNode literal_type = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(literal_type))
            c_hint_initializer_list(ctx, ts_node_child_by_field_name(node, "value", 5),
                                    c_parse_type_node(ctx, literal_type));
    }

    /* Taking a callable variable's address exposes its storage to writes
     * that this local assignment tracker cannot see. */
    if (!strcmp(kind, "pointer_expression") || !strcmp(kind, "unary_expression")) {
        TSNode operand =
            c_fp_unwrap_parentheses(ctx, ts_node_child_by_field_name(node, "argument", 8));
        TSNode operator= ts_node_child_by_field_name(node, "operator", 8);
        const char *op = ts_node_is_null(operator) ? NULL : c_node_text(ctx, operator);
        if (op && !strcmp(op, "&") && !ts_node_is_null(operand) &&
            !strcmp(ts_node_type(operand), "identifier"))
            c_escape_fp_storage(ctx, c_node_text(ctx, operand));
    }

    c_resolve_callable_value_reference(ctx, node);
    c_resolve_node_operations(ctx, node, kind);

    // Push scope for blocks and control structures
    bool push_scope =
        (strcmp(kind, "compound_statement") == 0 || strcmp(kind, "if_statement") == 0 ||
         strcmp(kind, "for_statement") == 0 || strcmp(kind, "for_range_loop") == 0 ||
         strcmp(kind, "while_statement") == 0 || strcmp(kind, "do_statement") == 0 ||
         strcmp(kind, "switch_statement") == 0 || strcmp(kind, "catch_clause") == 0 ||
         strcmp(kind, "lambda_expression") == 0);
    bool control_flow_scope =
        strcmp(kind, "if_statement") == 0 || strcmp(kind, "for_statement") == 0 ||
        strcmp(kind, "for_range_loop") == 0 || strcmp(kind, "while_statement") == 0 ||
        strcmp(kind, "do_statement") == 0 || strcmp(kind, "switch_statement") == 0 ||
        strcmp(kind, "catch_clause") == 0;

    int saved_fp_count = ctx->fp_count;
    if (push_scope) {
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    }
    if (control_flow_scope) {
        ctx->control_flow_depth++;
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

    // Process lambda init-captures: [captured = expr] binds 'captured' in lambda
    // scope
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
                        if (ts_node_is_null(name_node))
                            name_node = ts_node_child_by_field_name(cap, "left", 4);
                        if (ts_node_is_null(val_node))
                            val_node = ts_node_child_by_field_name(cap, "right", 5);
                        // Fallback: positional — first named child is name, second is value
                        if (ts_node_is_null(name_node) && ts_node_named_child_count(cap) >= 2) {
                            name_node = ts_node_named_child(cap, 0);
                            val_node = ts_node_named_child(cap, 1);
                        }
                        if (!ts_node_is_null(name_node) && !ts_node_is_null(val_node)) {
                            bool reference_capture = false;
                            TSTreeCursor capture = ts_tree_cursor_new(cap);
                            if (ts_tree_cursor_goto_first_child(&capture))
                                do {
                                    TSNode part = ts_tree_cursor_current_node(&capture);
                                    reference_capture |= !strcmp(ts_node_type(part), "&");
                                } while (ts_tree_cursor_goto_next_sibling(&capture));
                            ts_tree_cursor_delete(&capture);
                            if (reference_capture) {
                                TSNode storage = c_fp_unwrap_parentheses(ctx, val_node);
                                if (!ts_node_is_null(storage) &&
                                    !strcmp(ts_node_type(storage), "identifier"))
                                    c_escape_fp_storage(ctx, c_node_text(ctx, storage));
                            }
                            char *cap_name = c_node_text(ctx, name_node);
                            const CBMType *cap_type = c_eval_expr_type(ctx, val_node);
                            if (cap_name && !cbm_type_is_unknown(cap_type)) {
                                c_scope_bind(ctx, cap_name, cap_type);
                            }
                        }
                    }
                }
                break; // only one capture specifier per lambda
            }
        }
    }

    bool delayed_callable = !strcmp(kind, "lambda_expression");
    if (delayed_callable)
        ctx->callable_flow_depth++;

    // Recurse into children via a cursor (O(n)); ts_node_child(node,i) is O(i)
    // in tree-sitter → O(n²) on a wide node.
    {
        /* The condition of `#if` and the name of `#ifdef` are evaluated by the preprocessor. */
        bool directive = !strcmp(kind, "preproc_if") || !strcmp(kind, "preproc_elif") ||
                         !strcmp(kind, "preproc_ifdef") || !strcmp(kind, "preproc_elifdef");
        TSTreeCursor cursor = ts_tree_cursor_new(node);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                const char *field = directive ? ts_tree_cursor_current_field_name(&cursor) : NULL;
                if (field && (!strcmp(field, "condition") || !strcmp(field, "name")))
                    continue;
                c_resolve_calls_in_node(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }

    if (assignment)
        c_process_statement(ctx, node);
    if (initializer_fp >= 0 && ctx->fp_target_proofs[initializer_fp] != initializer_origin) {
        /* A nested write inside an unsupported initializer is not its final
         * value. Keep only the initializer's original proof; escaped storage
         * remains unknown when c_update_fp_target observes its persistent bit. */
        c_update_fp_target(ctx, ctx->fp_var_names[initializer_fp], initializer_origin);
    }
    if (delayed_callable)
        ctx->callable_flow_depth--;
    if (push_scope) {
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
        ctx->fp_count = saved_fp_count;
    }
    if (control_flow_scope) {
        ctx->control_flow_depth--;
    }
}

// ============================================================================
// Process function: set enclosing QN, bind params, walk body
// ============================================================================

/* The source digest and RAW origin have already been checked by the entry
 * point, which reparses the source. The shared candidate then proves exact
 * name/body spans against an owner row in that fresh tree. Macro arguments are
 * not native function parameters: enter the proved body directly. */
static bool c_process_configured_test_body(CLSPContext *ctx, TSNode scope) {
    if (!ctx->test_definition_owners ||
        ctx->test_definition_owners->test_owner_language != CBM_LANG_C)
        return false;
    const char *kind = ts_node_type(scope);
    if (strcmp(kind, "function_definition") != 0 && strcmp(kind, "compound_statement") != 0)
        return false;
    const char *qn = cbm_test_definition_owner_qn(ctx->test_definition_owners, scope);
    if (!qn)
        return false;
    TSNode name, body;
    if (!cbm_test_definition_candidate(scope, ctx->test_definition_owners->test_owner_language,
                                       &name, &body) ||
        ts_node_is_null(body)) {
        ctx->test_owner_error = true;
        return true;
    }
    ctx->test_owner_seen_count++;
    if (ctx->arena != &ctx->test_definition_owners->arena) {
        qn = cbm_arena_strdup(ctx->arena, qn);
        if (!qn) {
            ctx->test_owner_error = true;
            return true;
        }
    }
    const char *saved_func = ctx->enclosing_func_qn;
    CBMScope *saved_scope = ctx->current_scope;
    int saved_fp_count = ctx->fp_count;
    ctx->enclosing_func_qn = qn;
    ctx->current_scope = cbm_scope_push(ctx->arena, saved_scope);
    if (!ctx->current_scope) {
        ctx->test_owner_error = true;
    } else {
        /* The compound node itself may be the owner scope. Walk its children
         * once so entering it cannot recursively re-enter the same owner. */
        TSTreeCursor cursor = ts_tree_cursor_new(body);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                c_resolve_calls_in_node(ctx, ts_tree_cursor_current_node(&cursor));
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }
    ctx->fp_count = saved_fp_count;
    ctx->current_scope = saved_scope;
    ctx->enclosing_func_qn = saved_func;
    return true;
}

// #946: a macro-wrapped definition (`Cls::__API_HOOK(Spawn)`, `FNAME(first)`)
// is named by the macro's argument, keeping any class qualifier: replace the
// last
// `::` segment of the walked name (or the whole unqualified name) with it, the
// same name cbm_resolve_c_declarator_name_node gives the def.
static char *c_macro_wrapped_func_name(CLSPContext *ctx, char *func_name, TSNode macro_arg) {
    char *arg = c_node_text(ctx, macro_arg);
    if (!func_name || !arg || !arg[0])
        return func_name;
    const char *last_sep = NULL;
    for (const char *p = strstr(func_name, "::"); p; p = strstr(p + 2, "::"))
        last_sep = p;
    if (!last_sep)
        return arg;
    return cbm_arena_sprintf(ctx->arena, "%.*s::%s", (int)(last_sep - func_name), func_name, arg);
}

/* The class a member function belongs to: where its body is written, or the class
 * named in `Name::f` for one written elsewhere. `ns::f` names no class. */
static const CBMRegisteredType *c_proof_function_class(CLSPContext *ctx, TSNode func_node) {
    if (!c_cxx_rules(ctx))
        return NULL;
    uint32_t at = ts_node_start_byte(func_node);
    /* For `Name::f` this QN is a guess at this module's own class. A class declared in
     * a header under a namespace is found by the name written in the declarator. */
    const CBMRegisteredType *enclosing =
        ctx->enclosing_class_qn ? c_proof_field_class(ctx, ctx->enclosing_class_qn, at) : NULL;
    if (enclosing)
        return enclosing;
    TSNode declarator = ts_node_child_by_field_name(func_node, "declarator", 10);
    for (int hop = 0; hop < 16 && !ts_node_is_null(declarator); hop++) {
        const char *kind = ts_node_type(declarator);
        if (!strcmp(kind, "qualified_identifier")) {
            TSNode scope = ts_node_child_by_field_name(declarator, "scope", 5);
            TSNode name = ts_node_child_by_field_name(declarator, "name", 4);
            const char *inner = ts_node_is_null(name) ? "" : ts_node_type(name);
            if (!strcmp(inner, "qualified_identifier")) {
                declarator = name;
                continue;
            }
            const char *text = ts_node_is_null(scope) ? NULL : c_node_text(ctx, scope);
            return text ? c_proof_named_class(ctx, c_base_class_name(ctx, text), at) : NULL;
        }
        TSNode next = ts_node_child_by_field_name(declarator, "declarator", 10);
        if (ts_node_is_null(next) && !strcmp(kind, "reference_declarator") &&
            ts_node_named_child_count(declarator))
            next = ts_node_named_child(declarator, ts_node_named_child_count(declarator) - 1);
        declarator = next;
    }
    return NULL;
}

static void c_process_function(CLSPContext *ctx, TSNode func_node) {
    if (ctx->eval_failure)
        return;
    if (c_process_configured_test_body(ctx, func_node))
        return;
    TSNode decl = ts_node_child_by_field_name(func_node, "declarator", 10);
    if (ts_node_is_null(decl))
        return;

    /* `void f(int a) ATTR { ... }` under the C++ grammar reads as a function named
     * ATTR with no parameters, its real name and parameters lost in an ERROR. Which
     * function this body belongs to, and what its parameters hide, is not known. */
    if (!strcmp(ts_node_type(decl), "identifier"))
        return;

    // Extract function name from declarator chain
    char *func_name = NULL;
    const char *saved_class_qn = ctx->enclosing_class_qn;
    const char *saved_func_qn = ctx->enclosing_func_qn;
    TSNode params_node = (TSNode){0};
    TSNode macro_arg = (TSNode){0};

    // Navigate declarator to find name and parameters
    TSNode cur = decl;
    // Set after stepping through a recovered `RetT::name` (see
    // cbm_c_qualifier_is_recovered): that path names the function with a
    // type_identifier, and the "scope" is the return type, not a class.
    bool recovered = false;
    for (int depth = 0; depth < 10 && !ts_node_is_null(cur); depth++) {
        const char *dk = ts_node_type(cur);

        if (strcmp(dk, "function_declarator") == 0) {
            TSNode fdecl = ts_node_child_by_field_name(cur, "declarator", 10);
            params_node = ts_node_child_by_field_name(cur, "parameters", 10);
            // `API RetT name(...)`: the real name sits in an ERROR before the
            // parameters; the declarator identifier is the return type.
            TSNode real = cbm_c_recovered_func_name(cur);
            if (!ts_node_is_null(real)) {
                func_name = c_node_text(ctx, real);
                break;
            }
            /* #946: the nested declarator is a macro call. These parameters are
             * the real ones; step past the macro's own argument list to the
             * (possibly class-qualified) macro name, which still scopes it. */
            macro_arg = cbm_c_macro_wrapped_name_node(cur);
            if (!ts_node_is_null(macro_arg))
                fdecl = ts_node_child_by_field_name(fdecl, "declarator", 10);
            cur = fdecl;
            continue;
        }
        /* `TEST_BEGIN(test_zero) {` reads as a function typed TEST_BEGIN named (test_zero). Only a
         * macro makes that a definition, and the def walk names it test_zero as well. */
        if (strcmp(dk, "pointer_declarator") == 0 || strcmp(dk, "reference_declarator") == 0 ||
            strcmp(dk, "parenthesized_declarator") == 0 ||
            (recovered && strcmp(dk, "pointer_type_declarator") == 0)) {
            if (ts_node_named_child_count(cur) > 0)
                cur = ts_node_named_child(cur, ts_node_named_child_count(cur) - 1);
            else
                break;
            continue;
        }
        if (recovered && strcmp(dk, "type_identifier") == 0) {
            func_name = c_node_text(ctx, cur);
            break;
        }
        if ((strcmp(dk, "qualified_identifier") == 0 || strcmp(dk, "scoped_identifier") == 0) &&
            cbm_c_qualifier_is_recovered(cur)) {
            cur = ts_node_child_by_field_name(cur, "name", 4);
            recovered = true;
            continue;
        }
        if (strcmp(dk, "qualified_identifier") == 0 || strcmp(dk, "scoped_identifier") == 0) {
            func_name = c_node_text(ctx, cur);
            /* `ns::Box<T>::run`: the class is the scope beside the name, without its arguments,
             * which is how the def walk names this function. */
            TSNode last = cur;
            for (int hop = 0; hop < 16; hop++) {
                TSNode inner = ts_node_child_by_field_name(last, "name", 4);
                if (ts_node_is_null(inner) ||
                    (strcmp(ts_node_type(inner), "qualified_identifier") &&
                     strcmp(ts_node_type(inner), "scoped_identifier")))
                    break;
                last = inner;
            }
            // Check if this is a method (has Class:: prefix)
            TSNode scope_node = ts_node_child_by_field_name(last, "scope", 5);
            bool templated =
                !ts_node_is_null(scope_node) && !strcmp(ts_node_type(scope_node), "template_type");
            if (templated)
                scope_node = ts_node_child_by_field_name(scope_node, "name", 4);
            TSNode leaf = ts_node_child_by_field_name(last, "name", 4);
            if ((templated || !ts_node_eq(last, cur)) && !ts_node_is_null(scope_node) &&
                !ts_node_is_null(leaf)) {
                const char *written_scope = c_node_text(ctx, scope_node);
                const char *written_name = c_node_text(ctx, leaf);
                if (written_scope && written_name)
                    func_name =
                        cbm_arena_sprintf(ctx->arena, "%s::%s", written_scope, written_name);
            }
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
    if (!ts_node_is_null(macro_arg))
        func_name = c_macro_wrapped_func_name(ctx, func_name, macro_arg);

    // A keyword "name" is an error-recovery artifact (see
    // cbm_c_reserved_func_name): the def extractor mints no node for it, so no
    // caller QN may name it either.
    if (!func_name || !func_name[0] || cbm_c_reserved_func_name(func_name))
        return;

    const char *func_qn = cbm_test_definition_owner_qn(ctx->test_definition_owners, func_node);
    if (func_qn) {
        ctx->test_owner_seen_count++;
        if (ctx->arena != &ctx->test_definition_owners->arena) {
            func_qn = cbm_arena_strdup(ctx->arena, func_qn);
            if (!func_qn) {
                ctx->test_owner_error = true;
                ctx->enclosing_class_qn = saved_class_qn;
                return;
            }
        }
    }
    if (!func_qn) {
        // Build enclosing function QN
        func_qn = c_build_qn(ctx, func_name);
        // For a method defined INLINE inside its class body, func_name is a bare
        // identifier ("compute") and enclosing_class_qn was inherited from
        // c_process_class (saved_class_qn == enclosing_class_qn). The textual
        // extractor and the registry qualify the method as module.Class.method, so
        // building func_qn as module.method here (no class) made the LSP-resolved
        // call's caller_qn disagree with the textual call's enclosing_func_qn and
        // cbm_pipeline_find_lsp_resolution never joined them — every in-method call
        // (e.g. lsp_implicit_this) silently lost its type-aware strategy. Prepend
        // the enclosing class, mirroring the Go receiver-QN fix. Out-of-line
        // definitions (Widget::compute) already carry the class in func_name (a
        // qualified_identifier), so c_build_qn produces module.Class.method and the
        // enclosing_class_qn was set HERE (saved_class_qn != enclosing_class_qn);
        // skip those, and skip names that already contain the class scope.
        if (ctx->enclosing_class_qn && saved_class_qn == ctx->enclosing_class_qn &&
            !strchr(func_qn, '.')) {
            func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->enclosing_class_qn, func_qn);
        } else if (ctx->enclosing_class_qn && saved_class_qn != ctx->enclosing_class_qn &&
                   strchr(func_qn, '.')) {
            /* Out-of-line method `Class::method`: c_build_qn yields the bare
             * "Class.method" (no module) — the class scope was resolved HERE to the
             * full module-qualified class QN (saved_class_qn != enclosing_class_qn).
             * Rebuild as <class QN>.<method short name> so the caller_qn matches the
             * def walk and call-scope QN, which qualify out-of-line methods the same
             * way. Without this the caller_qn stays "Class.method", the
             * exact-equality lsp_resolve join misses, and the LSP rescue is discarded
             * (gap #5a). */
            const char *dot = strrchr(func_qn, '.');
            func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->enclosing_class_qn, dot + 1);
        } else if (strchr(func_qn, '.')) {
            /* `Name::f` whose class is declared in another file, or `ns::f`: the class
             * lookup above found nothing in this module, and the def walk writes this
             * definition directly under the file's module, whatever namespace block it
             * sits in. */
            if (ctx->module_qn)
                func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, func_qn);
        } else if (!strchr(func_qn, '.')) {
            /* A free function in a namespace is qualified by the namespace scope
             * (current_namespace is module_qn.ns), matching the def QN the extractor
             * now produces; outside any namespace this falls back to the file module
             * so non-namespaced free functions are unchanged. */
            const char *scope = ctx->current_namespace ? ctx->current_namespace : ctx->module_qn;
            if (scope) {
                func_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", scope, func_qn);
            }
        }
    }
    ctx->enclosing_func_qn = func_qn;
    const CBMRegisteredType *saved_method_class = ctx->proof_method_class;
    ctx->proof_method_class = c_proof_function_class(ctx, func_node);

    // If inside a template, attach type_param_names to the registered function
    // so pending template calls can be resolved at call sites. NEVER do this
    // against the shared Tier-2 cross registry: resolve workers walk files
    // concurrently, so the write races other readers AND stores a pointer to
    // THIS worker's per-file arena into shared state — once that arena is
    // recycled the registry holds dangling memory (intermittent SIGSEGV
    // indexing bitcoin). Cross-phase template deduction then relies on the
    // positional fallback, which is graceful degradation.
    if (ctx->in_template && ctx->template_param_count > 0 && !ctx->registry_shared) {
        // Set type_param_names on the registered function -- on the head's
        // own copy of it (copy-on-write), never on a shared base entry.
        CBMRegisteredFunc *rf = cbm_registry_func_for_update(ctx->registry_head, func_qn);
        if (rf && !rf->type_param_names) {
            const char **tpn = (const char **)cbm_arena_alloc(
                ctx->arena, (ctx->template_param_count + 1) * sizeof(const char *));
            for (int ti = 0; ti < ctx->template_param_count; ti++)
                tpn[ti] = ctx->template_param_names[ti];
            tpn[ctx->template_param_count] = NULL;
            rf->type_param_names = tpn;
        }
    }

    // Push function scope
    CBMScope *saved_scope = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    uint32_t saved_syntax_limit = ctx->proof_syntax_limit;
    if (!saved_func_qn || saved_func_qn == ctx->module_qn)
        c_proof_enter_function(ctx, func_node);

    // Bind 'this' if in a method
    if (ctx->enclosing_class_qn) {
        c_scope_bind(
            ctx, "this",
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
                c_bind_declared_names(ctx, param, pk);
            }
        }
    }
    // Set min_params on the registered function (for default-arg overload
    // matching)
    if (total_params > 0 && defaulted_params > 0) {
        CBMRegisteredFunc *rf = cbm_registry_func_for_update(ctx->registry_head, func_qn);
        if (rf && rf->min_params < 0) {
            rf->min_params = total_params - defaulted_params;
        }
    }

    c_emit_signature_types(ctx, ts_node_child_by_field_name(func_node, "type", 4), 0);
    c_emit_signature_types(ctx, params_node, 0);

    // Walk function body
    TSNode body = ts_node_child_by_field_name(func_node, "body", 4);
    /* `Name(StringRef S) : Data(toBytes(S)) {`: the initializers run as part of this function. */
    TSTreeCursor parts = ts_tree_cursor_new(func_node);
    if (ts_tree_cursor_goto_first_child(&parts))
        do {
            TSNode part = ts_tree_cursor_current_node(&parts);
            if (!strcmp(ts_node_type(part), "field_initializer_list"))
                c_resolve_calls_in_node(ctx, part);
        } while (ts_tree_cursor_goto_next_sibling(&parts));
    ts_tree_cursor_delete(&parts);
    if (!ts_node_is_null(body)) {
        c_resolve_calls_in_node(ctx, body);
    }

    // Restore
    ctx->current_scope = saved_scope;
    ctx->enclosing_class_qn = saved_class_qn;
    ctx->enclosing_func_qn = saved_func_qn;
    ctx->proof_syntax_limit = saved_syntax_limit;
    ctx->proof_method_class = saved_method_class;
}

// ============================================================================
// Process namespace
// ============================================================================

// Node kinds that only group top-level items: the branches of `#if` / `#ifdef`
// (an include guard is one) and the body of `extern "C" { ... }`.
static bool c_is_body_container(const char *kind) {
    return strcmp(kind, "preproc_if") == 0 || strcmp(kind, "preproc_ifdef") == 0 ||
           strcmp(kind, "preproc_else") == 0 || strcmp(kind, "preproc_elif") == 0 ||
           strcmp(kind, "preproc_elifdef") == 0 || strcmp(kind, "linkage_specification") == 0 ||
           strcmp(kind, "declaration_list") == 0 ||
           /* An #if that splits a brace leaves the rest of the file inside one ERROR,
            * and complete functions sit in it with their own scopes intact. */
           strcmp(kind, "ERROR") == 0;
}

/* A member template's body still has its class template's parameters in scope. */
static void c_widen_template_scope(CLSPContext *ctx, const char **outer, int outer_count) {
    int count = outer_count + ctx->template_param_count;
    const char **all = count ? cbm_arena_alloc(ctx->arena, (size_t)count * sizeof(*all)) : NULL;
    if (!all) {
        ctx->outer_template_param_count = 0;
        return;
    }
    if (outer_count)
        memcpy(all, outer, (size_t)outer_count * sizeof(*all));
    for (int i = 0; i < ctx->template_param_count; i++)
        all[outer_count + i] = ctx->template_param_names[i];
    ctx->outer_template_params = all;
    ctx->outer_template_param_count = count;
}

// Process a top-level or nested declaration within a namespace/class body,
// handling template_declaration wrapping.
static void c_process_body_child(CLSPContext *ctx, TSNode child) {
    if (ctx->eval_failure)
        return;
    if (ts_node_is_null(child))
        return;
    const char *ck = ts_node_type(child);

    if (c_process_configured_test_body(ctx, child))
        return;
    if (strcmp(ck, "function_definition") == 0) {
        c_process_function(ctx, child);
    } else if (strcmp(ck, "namespace_definition") == 0) {
        c_process_namespace(ctx, child);
    } else if (strcmp(ck, "class_specifier") == 0 || strcmp(ck, "struct_specifier") == 0 ||
               strcmp(ck, "union_specifier") == 0) {
        c_process_class(ctx, child);
    } else if (strcmp(ck, "declaration") == 0) {
        // declaration may contain class/struct specifier: class Foo { ... };
        // Extract and process the class before resolving calls.
        bool has_class = false;
        uint32_t dnc = ts_node_named_child_count(child);
        for (uint32_t di = 0; di < dnc; di++) {
            TSNode dch = ts_node_named_child(child, di);
            const char *dk = ts_node_type(dch);
            if (strcmp(dk, "class_specifier") == 0 || strcmp(dk, "struct_specifier") == 0 ||
                strcmp(dk, "union_specifier") == 0) {
                c_process_class(ctx, dch);
                has_class = true;
            }
            // Register bare function declarations: ReturnType func_name(params);
            // Unwrap reference_declarator/pointer_declarator to find
            // function_declarator
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
                                // Only register if not already registered. On the
                                // shared Tier-2 registry, skip entirely: it is
                                // finalized + read-only, the def is already present
                                // from the project-wide build, and the lookup + add
                                // would otherwise hit/grow the post-finalize tail ->
                                // O(files*defs) on large C codebases (the Linux-kernel
                                // full-index hang) plus a cross-worker heap race.
                                if (!ctx->registry_shared &&
                                    !cbm_registry_lookup_func(ctx->registry, func_qn)) {
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
                                    cbm_registry_add_func(ctx->registry_head, rf);
                                }
                            }
                        }
                    }
                }
            }
        }
        uint32_t saved_limit = c_proof_enter_file_item(ctx, child);
        if (!has_class) {
            c_resolve_calls_in_node(ctx, child);
        } else {
            /* The specifier was handled as a class above; what the declaration
             * DECLARES is still code. `static struct Ops table = { .open = f };`
             * names a struct and never opened its initializer. */
            for (uint32_t di = 0; di < dnc; di++) {
                TSNode dch = ts_node_named_child(child, di);
                if (strcmp(ts_node_type(dch), "init_declarator") == 0)
                    c_resolve_calls_in_node(ctx, dch);
            }
        }
        ctx->proof_syntax_limit = saved_limit;
    } else if (strcmp(ck, "concept_definition") == 0) {
        // C++20 concept: concept Sortable = requires(T a) { a.sort(); };
        // No CALLS edges from concept definitions — they are constraints, not
        // runtime calls. Skip processing to avoid false positive CALLS from
        // requires-expressions.
        return;
    } else if (strcmp(ck, "template_declaration") == 0) {
        // Save template state, parse params for defaults, set flag
        bool saved_template = ctx->in_template;
        const char **saved_tpn = ctx->template_param_names;
        const CBMType **saved_tpd = ctx->template_param_defaults;
        int saved_tpc = ctx->template_param_count;

        ctx->in_template = true;
        const char **saved_outer = ctx->outer_template_params;
        int saved_outer_count = ctx->outer_template_param_count;
        c_parse_template_params(ctx, child);
        c_widen_template_scope(ctx, saved_outer, saved_outer_count);

        uint32_t tnc = ts_node_named_child_count(child);
        for (uint32_t ti = 0; ti < tnc; ti++) {
            TSNode inner = ts_node_named_child(child, ti);
            c_process_body_child(ctx, inner);
        }

        ctx->in_template = saved_template;
        ctx->outer_template_params = saved_outer;
        ctx->outer_template_param_count = saved_outer_count;
        ctx->template_param_names = saved_tpn;
        ctx->template_param_defaults = saved_tpd;
        ctx->template_param_count = saved_tpc;
    } else if (c_is_body_container(ck)) {
        /* A preprocessor branch or an `extern "C"` block holds top-level
         * items, not statements: its functions are functions. Walked as an
         * expression they kept the FILE as their enclosing scope, so every
         * typed row of a function behind an include guard or an `#ifdef` was
         * attributed to the module and matched no call or member site.
         * Cursor walk (O(n)): an include guard holds the whole header. */
        TSTreeCursor cursor = ts_tree_cursor_new(child);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode inner = ts_tree_cursor_current_node(&cursor);
                if (ts_node_is_named(inner))
                    c_process_body_child(ctx, inner);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    } else {
        uint32_t saved_limit = c_proof_enter_file_item(ctx, child);
        c_resolve_calls_in_node(ctx, child);
        ctx->proof_syntax_limit = saved_limit;
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
                CBMRegisteredType *rt = cbm_registry_type_for_update(ctx->registry_head, class_qn);
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
        // This allows template return type substitution for methods like T&
        // value();
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
                                    /* Refine the return type on the HEAD copy only
                                     * (copy-on-write). `existing` may live in the
                                     * sealed shared base; writing a scratch-arena
                                     * signature into it was read by other workers
                                     * after this file's arena died (ASan heap-use-
                                     * after-free in c_adl_resolve on dotnet/runtime,
                                     * 2026-09-14). NULL = the head is sealed too:
                                     * skip the refinement. */
                                    CBMRegisteredFunc *mut = cbm_registry_func_for_update(
                                        ctx->registry_head, existing->qualified_name);
                                    if (mut) {
                                        const CBMType **new_rets =
                                            (const CBMType **)cbm_arena_alloc(
                                                ctx->arena, 2 * sizeof(const CBMType *));
                                        new_rets[0] = actual_ret;
                                        new_rets[1] = NULL;
                                        mut->signature = cbm_type_func_replace_returns(
                                            ctx->arena, mut->signature, new_rets);
                                    }
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
                            cbm_registry_add_func(ctx->registry_head, rf);
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

// Need forward declaration since c_process_namespace and c_process_class
// reference each other Already defined above via forward declaration pattern —
// both reference c_resolve_calls_in_node

// ============================================================================
// Process file: top-level walk
// ============================================================================

/* Pass 1 for one file-scope item: bind using declarations, typedefs and global
 * variables before any function body is walked. Preprocessor branches and
 * `extern "C"` blocks are descended: everything in a header sits inside its
 * include guard, and a global behind an `#ifdef` is a global. */
static void c_bind_file_scope_item(CLSPContext *ctx, TSNode item, int depth) {
    const char *ck = ts_node_type(item);
    if (strcmp(ck, "using_declaration") == 0 || strcmp(ck, "alias_declaration") == 0 ||
        strcmp(ck, "type_definition") == 0 || strcmp(ck, "namespace_alias_definition") == 0 ||
        strcmp(ck, "declaration") == 0) {
        c_process_statement(ctx, item);
    } else if (strcmp(ck, "template_declaration") == 0) {
        // template<class T> using Vec = std::vector<T>;
        // Unwrap template_declaration to process inner alias/typedef
        uint32_t tnc = ts_node_named_child_count(item);
        for (uint32_t ti = 0; ti < tnc; ti++) {
            TSNode inner = ts_node_named_child(item, ti);
            const char *ik = ts_node_type(inner);
            if (strcmp(ik, "alias_declaration") == 0 || strcmp(ik, "type_definition") == 0) {
                c_process_statement(ctx, inner);
            }
        }
    } else if (c_is_body_container(ck) && depth < C_LSP_MAX_WALK_DEPTH) {
        // Cursor walk (O(n)): an include guard holds the whole header.
        TSTreeCursor cursor = ts_tree_cursor_new(item);
        if (ts_tree_cursor_goto_first_child(&cursor)) {
            do {
                TSNode inner = ts_tree_cursor_current_node(&cursor);
                if (ts_node_is_named(inner))
                    c_bind_file_scope_item(ctx, inner, depth + 1);
            } while (ts_tree_cursor_goto_next_sibling(&cursor));
        }
        ts_tree_cursor_delete(&cursor);
    }
}

void c_lsp_process_file(CLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;

    int resolved_before = ctx->resolved_calls ? ctx->resolved_calls->count : 0;
    c_proof_prepare(ctx, root);
    ctx->macro_defs_ready = true;

    // Collect top-level children once (O(n)); both passes reuse the array.
    // Indexing ts_node_child(root,i) per iteration is O(i) → O(n²) on a wide
    // root.
    uint32_t kn = 0;
    TSNode *kids = cbm_lsp_collect_children(ctx->arena, root, &kn);
    TSNode child; // Hoisted: prevents ASan stack-use-after-scope between passes

    // Pass 1: process using declarations and global variables
    for (uint32_t i = 0; i < kn && !ctx->eval_failure; i++) {
        child = kids[i];
        c_bind_file_scope_item(ctx, child, 0);
    }

    // Pass 2: process functions, namespaces, classes, templates
    for (uint32_t i = 0; i < kn && !ctx->eval_failure; i++) {
        child = kids[i];
        c_process_body_child(ctx, child);
    }

    // Release the per-file negative-lookup memo (malloc-owned; only allocated in
    // shared-registry mode). No-op when it was never populated.
    c_neg_memo_free(ctx);
    if (ctx->macro_defs) {
        cbm_ht_free(ctx->macro_defs);
        ctx->macro_defs = NULL;
    }
    if (ctx->eval_failure && ctx->resolved_calls)
        ctx->resolved_calls->count = resolved_before;
}

// ============================================================================
// Parse C/C++ return type text into CBMType (for cross-file defs)
// ============================================================================

static const CBMType *c_parse_return_type_text(CBMArena *a, const char *text,
                                               const char *module_qn) {
    if (!text || !text[0])
        return cbm_type_unknown();

    /* An aggregate defined where its member is declared, `union vval_union { ... } vval;`,
     * has its whole body in the type text. The tag before it is the type. */
    const char *body_open = strchr(text, '{');
    const char *body_close = body_open ? strrchr(body_open, '}') : NULL;
    if (body_close) {
        size_t prefix = (size_t)(body_open - text);
        while (prefix > 0 && text[prefix - 1] == ' ')
            prefix--;
        const char *joined = cbm_arena_sprintf(a, "%.*s%s", (int)prefix, text, body_close + 1);
        if (!joined)
            return cbm_type_unknown();
        text = joined;
    }

    // Skip const/volatile qualifiers and the elaborated-type keyword: a member
    // declared `struct Ops *ops;` has the type text `struct Ops`, and the
    // struct is registered as `Ops`.
    static const char *const skipped[] = {"const ", "volatile ", "struct ", "union ", "enum "};
    for (bool again = true; again;) {
        again = false;
        for (size_t k = 0; k < sizeof(skipped) / sizeof(skipped[0]); k++) {
            size_t klen = strlen(skipped[k]);
            if (strncmp(text, skipped[k], klen) == 0) {
                text += klen;
                again = true;
            }
        }
    }

    // Pointer to function, the way the extractor spells the type of an inline
    // pointer-to-function member: `ret (*)(params)`, `ret (*[4])(params)`. A
    // call through it yields `ret`.
    size_t len = strlen(text);
    const char *fp_open = strstr(text, "(*");
    if (fp_open && fp_open > text && len > 0 && text[len - 1] == ')' &&
        strncmp(text, "decltype(", 9) != 0) {
        char *ret_text = cbm_arena_strndup(a, text, (size_t)(fp_open - text));
        size_t rlen = ret_text ? strlen(ret_text) : 0;
        while (rlen > 0 && ret_text[rlen - 1] == ' ')
            ret_text[--rlen] = '\0';
        const CBMType *rets[2] = {
            rlen > 0 ? c_parse_return_type_text(a, ret_text, module_qn) : cbm_type_unknown(), NULL};
        return cbm_type_func(a, NULL, NULL, rets);
    }

    // A qualifier on the outermost pointer level (`char *const`) says nothing
    // about the type's shape.
    static const char *const trailing[] = {"const", "volatile", "restrict", "__restrict"};
    for (size_t k = 0; k < sizeof(trailing) / sizeof(trailing[0]); k++) {
        size_t klen = strlen(trailing[k]);
        if (len > klen && strcmp(text + len - klen, trailing[k]) == 0 &&
            (text[len - klen - 1] == '*' || text[len - klen - 1] == ' ')) {
            char *inner = cbm_arena_strndup(a, text, len - klen);
            size_t ilen = inner ? strlen(inner) : 0;
            while (ilen > 0 && inner[ilen - 1] == ' ')
                inner[--ilen] = '\0';
            return c_parse_return_type_text(a, inner, module_qn);
        }
    }

    // Array member (`int [8]`, `char *[4]`): indexing it yields the element.
    if (len > 0 && text[len - 1] == ']') {
        const char *open = strrchr(text, '[');
        if (open && open > text) {
            char *inner = cbm_arena_strndup(a, text, (size_t)(open - text));
            size_t ilen = inner ? strlen(inner) : 0;
            while (ilen > 0 && inner[ilen - 1] == ' ')
                inner[--ilen] = '\0';
            return cbm_type_slice(a, c_parse_return_type_text(a, inner, module_qn));
        }
    }

    // Pointer
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
        // decltype(expr) — try to interpret as type name (covers decltype(x) where
        // x is a type)
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

/* Materialize the extractor's counted, positional parameter-type carrier.
 * A NULL slot is meaningful (unknown), so never scan for a NULL terminator
 * before `count` and never leave an interior hole in the CBMType array. */
static const CBMType **c_parse_signature_param_types(CBMArena *arena, const char **type_texts,
                                                     int count, const char *module_qn) {
    if (!arena || count <= 0)
        return NULL;
    const CBMType **types =
        (const CBMType **)cbm_arena_alloc(arena, (size_t)(count + 1) * sizeof(const CBMType *));
    if (!types)
        return NULL;
    for (int i = 0; i < count; i++) {
        const char *text = type_texts ? type_texts[i] : NULL;
        const CBMType *type = NULL;
        if (text && text[0] && !(text[0] == '?' && text[1] == '\0')) {
            type = c_parse_return_type_text(arena, text, module_qn);
        }
        types[i] = type ? type : cbm_type_unknown();
    }
    types[count] = NULL;
    return types;
}

// ============================================================================
// Entry point: single-file LSP
// ============================================================================

static void c_rewrite_proved_destructor_candidates(CBMFileResult *result) {
    if (!result) {
        return;
    }
    for (int ci = 0; ci < result->calls.count; ci++) {
        CBMCall *call = &result->calls.items[ci];
        if (!call->requires_lsp_resolution || !call->callee_name ||
            strcmp(call->callee_name, "~") != 0 || call->site_end_byte <= call->site_start_byte) {
            continue;
        }
        for (int ri = 0; ri < result->resolved_calls.count; ri++) {
            const CBMResolvedCall *resolved = &result->resolved_calls.items[ri];
            if (!resolved->strategy || strcmp(resolved->strategy, "lsp_destructor") != 0 ||
                !resolved->callee_qn || !resolved->caller_qn || !call->enclosing_func_qn ||
                strcmp(resolved->caller_qn, call->enclosing_func_qn) != 0 ||
                resolved->site_start_byte != call->site_start_byte ||
                resolved->site_end_byte != call->site_end_byte ||
                resolved->source_origin != call->source_origin) {
                continue;
            }
            const char *leaf = strrchr(resolved->callee_qn, '.');
            leaf = leaf ? leaf + 1 : resolved->callee_qn;
            if (leaf[0] == '~' && leaf[1]) {
                call->callee_name = leaf;
            }
            break;
        }
    }
}

/* Every configured owner must have been visited in this exact raw tree.
 * A stale/mismatched cached tree cannot quietly fall back to bare macro QNs. */
static bool c_test_owners_complete(const CLSPContext *ctx) {
    if (ctx->test_owner_error)
        return false;
    if (!ctx->test_definition_owners || !ctx->test_definition_owners->has_test_definition_owners)
        return true;
    int expected = 0;
    for (int i = 0; i < ctx->test_definition_owners->defs.count; i++)
        if (ctx->test_definition_owners->defs.items[i].test_role != CBM_TEST_ROLE_NONE)
            expected++;
    return expected == ctx->test_owner_seen_count;
}

/* QN -> index of its FIRST registration in reg->types, for the single-file
 * registration loop below (#1527). That loop asked "is this type registered
 * yet, and where?" by scanning every registered type per Field and per
 * Method -- O(defs x types) before the registry was indexed. Tracking the
 * first index as types are added answers the same question in O(1); the
 * types array may be reallocated as it grows, so indices, not pointers. */
typedef struct {
    const char **qn; /* NULL = empty slot */
    int *first;
    int cap; /* power of two, or 0 when unavailable */
    int count;
} CTypeFirstIndex;

static uint32_t c_type_first_hash(const char *s) {
    uint32_t h = 2166136261u;
    for (const unsigned char *p = (const unsigned char *)s; *p; p++) {
        h ^= *p;
        h *= 16777619u;
    }
    return h;
}

/* Slot holding qn, or the empty slot where it belongs. */
static int c_type_first_slot(const CTypeFirstIndex *ix, const char *qn) {
    uint32_t mask = (uint32_t)ix->cap - 1;
    uint32_t i = c_type_first_hash(qn) & mask;
    while (ix->qn[i] && strcmp(ix->qn[i], qn) != 0) {
        CBM_LSP_WORK(1);
        i = (i + 1) & mask;
    }
    return (int)i;
}

static bool c_type_first_alloc(CTypeFirstIndex *ix, CBMArena *arena, int cap) {
    const char **qn = (const char **)cbm_arena_alloc(arena, (size_t)cap * sizeof(*qn));
    int *first = (int *)cbm_arena_alloc(arena, (size_t)cap * sizeof(*first));
    if (!qn || !first) {
        return false;
    }
    memset(qn, 0, (size_t)cap * sizeof(*qn));
    ix->qn = qn;
    ix->first = first;
    ix->cap = cap;
    ix->count = 0;
    return true;
}

/* Record reg->types[idx]; an existing QN keeps its first index. On OOM the
 * index turns itself off (cap 0) and callers fall back to the scan. */
static void c_type_first_note(CTypeFirstIndex *ix, CBMArena *arena, const CBMTypeRegistry *reg,
                              int idx) {
    const char *qn = reg->types[idx].qualified_name;
    if (ix->cap == 0 || !qn) {
        return;
    }
    if ((ix->count + 1) * 2 > ix->cap) {
        CTypeFirstIndex grown = {0};
        if (!c_type_first_alloc(&grown, arena, ix->cap * 2)) {
            ix->cap = 0;
            return;
        }
        for (int i = 0; i < ix->cap; i++) {
            if (ix->qn[i]) {
                int s = c_type_first_slot(&grown, ix->qn[i]);
                grown.qn[s] = ix->qn[i];
                grown.first[s] = ix->first[i];
                grown.count++;
            }
        }
        *ix = grown;
    }
    int s = c_type_first_slot(ix, qn);
    if (!ix->qn[s]) {
        ix->qn[s] = qn;
        ix->first[s] = idx;
        ix->count++;
    }
}

/* Index of the first registered type named qn, or -1. Same answer as the
 * linear scan it replaces (which also serves as the OOM fallback). */
static int c_type_first_find(const CTypeFirstIndex *ix, const CBMTypeRegistry *reg,
                             const char *qn) {
    if (ix->cap > 0) {
        int s = c_type_first_slot(ix, qn);
        return ix->qn[s] ? ix->first[s] : -1;
    }
    for (int ri = 0; ri < reg->type_count; ri++) {
        CBM_LSP_WORK(1);
        if (strcmp(reg->types[ri].qualified_name, qn) == 0) {
            return ri;
        }
    }
    return -1;
}

/* cbm_registry_add_type plus the first-index bookkeeping. */
static void c_add_type_indexed(CBMTypeRegistry *reg, CTypeFirstIndex *ix, CBMArena *arena,
                               CBMRegisteredType rt) {
    int before = reg->type_count;
    cbm_registry_add_type(reg, rt);
    if (reg->type_count > before) {
        c_type_first_note(ix, arena, reg, before);
    }
}

void cbm_run_c_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                   TSNode root, bool cpp_mode, CBMSourceOrigin source_origin) {
    (void)cbm_run_c_lsp_with_test_owners(arena, result, source, source_len, root, cpp_mode,
                                         source_origin, NULL);
}

bool cbm_run_c_lsp_with_test_owners(CBMArena *arena, CBMFileResult *result, const char *source,
                                    int source_len, TSNode root, bool cpp_mode,
                                    CBMSourceOrigin source_origin,
                                    const CBMFileResult *test_owners) {
    if ((test_owners && test_owners->has_test_definition_owners &&
         source_origin != CBM_SOURCE_ORIGIN_RAW) ||
        !cbm_test_definition_owners_match(test_owners, source, source_len, cpp_mode,
                                          result->module_qn))
        return false;

    /* An earlier walk of this file stopped on a memo or depth failure
     * (#1527) and is not repeated. The failure is already reported through
     * lsp_skipped, which the pipeline treats as unverifiable owners for a
     * file with configured owners. */
    if (result->lsp_skipped)
        return true;

    /* TSNode carries no authenticated source-byte provenance. For configured
     * owners, parse the verified buffer here rather than trusting a supplied
     * root whose numeric spans could coincide with a different expression. */
    TSTree *owner_tree = NULL;
    if (test_owners && test_owners->has_test_definition_owners) {
        TSParser *parser = ts_parser_new();
        if (!parser)
            return false;
        const TSLanguage *ts_lang = cpp_mode ? tree_sitter_cpp() : tree_sitter_c();
        if (!ts_parser_set_language(parser, ts_lang)) {
            ts_parser_delete(parser);
            return false;
        }
        owner_tree = cbm_parse_c_source(parser, source, (uint32_t)source_len, (TSParseOptions){0});
        ts_parser_delete(parser);
        if (!owner_tree)
            return false;
        root = ts_tree_root_node(owner_tree);
    }

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    // Register stdlib
    cbm_c_stdlib_register(&reg, arena);
    if (cpp_mode)
        cbm_cpp_stdlib_register(&reg, arena);

    const char *module_qn = result->module_qn;

    /* Scratch for the registration index and the registry's hash index; both
     * die with this call (quadratic-pattern #6: never per-file index memory in
     * a longer-lived arena). */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    CTypeFirstIndex first_ix = {0};
    if (c_type_first_alloc(&first_ix, &idx_arena, 256)) {
        for (int ti = 0; ti < reg.type_count; ti++) {
            c_type_first_note(&first_ix, &idx_arena, &reg, ti);
        }
    }

    CBMLSPDef *source_defs =
        cbm_arena_calloc(arena, (size_t)result->defs.count * sizeof(*source_defs));
    if (source_defs) {
        for (int i = 0; i < result->defs.count; i++) {
            const CBMDefinition *def = &result->defs.items[i];
            source_defs[i] = (CBMLSPDef){.qualified_name = def->qualified_name,
                                         .graph_qualified_name = def->qualified_name,
                                         .short_name = def->name,
                                         .source_start_line = def->start_line,
                                         .signature_param_count = def->signature_param_count};
        }
        (void)cbm_c_collect_source_facts(arena, source, source_len, module_qn, source_defs,
                                         result->defs.count);
    }

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
            rt.project_source = true;
            rt.graph_qualified_name = d->qualified_name;
            rt.declaration_identity = cbm_arena_sprintf(arena, "%s:%d", module_qn, i);
            rt.source_flags = source_defs ? source_defs[i].source_flags : 0;

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
            c_add_type_indexed(&reg, &first_ix, &idx_arena, rt);
        }

        // Field definitions → populate field_names/field_types on parent type
        if (d->label && strcmp(d->label, "Field") == 0 && d->parent_class && d->return_type) {
            // Find or add the parent type in registry
            int parent_idx = c_type_first_find(&first_ix, &reg, d->parent_class);
            CBMRegisteredType *parent_rt = parent_idx >= 0 ? &reg.types[parent_idx] : NULL;
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
            rf.flags |= CBM_FUNC_FLAG_PROJECT_SOURCE;
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->name;
            rf.graph_qualified_name = d->qualified_name;
            rf.declaration_identity = cbm_arena_sprintf(arena, "%s:%d", module_qn, i);
            rf.source_flags = source_defs ? source_defs[i].source_flags : 0;
            if (rf.source_flags & CBM_SOURCE_DECL_FIXED_ARITY)
                rf.min_params = d->signature_param_count;

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

            // Build positional param types from the internal signature carrier.
            const CBMType **param_types_arr = c_parse_signature_param_types(
                arena, d->signature_param_types, d->signature_param_count, module_qn);

            rf.signature = cbm_type_func(arena, NULL, param_types_arr, ret_types);

            // Method receiver
            if (strcmp(d->label, "Method") == 0 && d->parent_class) {
                rf.receiver_type = d->parent_class;
                // Auto-create type if needed
                if (c_type_first_find(&first_ix, &reg, d->parent_class) < 0) {
                    CBMRegisteredType auto_type;
                    memset(&auto_type, 0, sizeof(auto_type));
                    auto_type.qualified_name = d->parent_class;
                    // Extract short name
                    const char *dot = strrchr(d->parent_class, '.');
                    auto_type.short_name = dot ? dot + 1 : d->parent_class;
                    c_add_type_indexed(&reg, &first_ix, &idx_arena, auto_type);
                }
            }

            cbm_registry_add_func(&reg, rf);
        }
    }

    /* Hash-index the registry before the walk (#1527): unindexed, every
     * resolver lookup scanned the stdlib plus all of this file's definitions,
     * O(calls x defs) per file. First-registered chains keep each duplicated
     * name resolving to the entry the scan returned; resolver additions after
     * this point stay visible through the registry's tail scan. */
    reg.index_first_registered = true;
    cbm_registry_finalize_into(&reg, &idx_arena);

    // Initialize context and run
    CLSPContext ctx;
    c_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, cpp_mode, &result->resolved_calls);
    ctx.source_origin = source_origin;
    cbm_resolve_proof_source_t proof_source =
        cbm_result_proof_source(result, source_origin, module_qn);
    (void)cbm_lsp_proof_context(&ctx.field_types, &proof_source, module_qn);
    ctx.test_definition_owners = test_owners;
    c_memo_test_stage(&ctx,
                      source_origin == CBM_SOURCE_ORIGIN_PREPROCESSED ? "preprocessed" : "raw");

    c_lsp_process_file(&ctx, root);
    bool owners_complete;
    if (ctx.eval_failure) {
        /* The walk stopped early and dropped its output (#1527). The failure
         * is reported through lsp_skipped, which the pipeline already treats
         * as unverifiable owners for a file with configured owners; an
         * owner error found before the stop is still returned. */
        cbm_c_lsp_record_failure(result, ctx.eval_failure);
        owners_complete = !ctx.test_owner_error;
    } else {
        c_rewrite_proved_destructor_candidates(result);
        owners_complete = c_test_owners_complete(&ctx);
    }
    cbm_arena_destroy(&idx_arena);
    if (owner_tree)
        ts_tree_delete(owner_tree);
    return owners_complete;
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
                         strcmp(d->label, "Interface") == 0 || strcmp(d->label, "Enum") == 0)) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name; /* borrowed — d outlives this call */
            rt.short_name = d->short_name;
            rt.project_source = true;
            rt.graph_qualified_name = d->graph_qualified_name;
            rt.declaration_identity = d->declaration_identity;
            rt.source_file_qn = d->source_file_qn;
            rt.source_flags = d->source_flags;
            rt.is_interface = d->is_interface;
            /* `typedef struct Tag Name;`, `typedef Other Name;`: the written name.
             * Each use resolves it in its own translation unit. */
            rt.alias_of = d->alias_target;

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

            // Field defs. Sized from the list itself: a fixed cap here would
            // silently drop the later fields of a large struct, and with them
            // every member-access edge onto those fields.
            if (d->field_defs) {
                int fcap = 1;
                for (const char *c = d->field_defs; *c; c++) {
                    if (*c == '|')
                        fcap++;
                }
                const char **fnarr =
                    (const char **)cbm_arena_alloc(arena, (fcap + 1) * sizeof(const char *));
                const CBMType **ftarr =
                    (const CBMType **)cbm_arena_alloc(arena, (fcap + 1) * sizeof(const CBMType *));
                const char *fsrc = d->field_defs;
                int fcount = 0;
                while (fnarr && ftarr && *fsrc && fcount < fcap) {
                    const char *sep = strchr(fsrc, '|');
                    const char *end = sep ? sep : fsrc + strlen(fsrc);
                    char *pair = cbm_arena_strndup(arena, fsrc, end - fsrc);
                    char *colon = pair ? strchr(pair, ':') : NULL;
                    if (colon) {
                        *colon = '\0';
                        fnarr[fcount] = pair;
                        ftarr[fcount] = c_parse_return_type_text(
                            arena, colon + 1, d->def_module_qn ? d->def_module_qn : module_qn);
                        fcount++;
                    }
                    if (!sep)
                        break;
                    fsrc = sep + 1;
                }
                if (fcount > 0) {
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
            rf.flags |= CBM_FUNC_FLAG_PROJECT_SOURCE;
            rf.qualified_name = d->qualified_name; /* borrowed */
            rf.short_name = d->short_name;
            rf.sig = d->sig;
            rf.declaration_identity = d->declaration_identity;
            rf.graph_qualified_name = d->graph_qualified_name;
            rf.source_flags = d->source_flags;
            rf.source_signature = d->source_signature;
            rf.source_arity = d->source_arity;
            rf.source_file_qn = d->source_file_qn;
            rf.source_namespace = d->namespace_name;
            if (rf.source_flags & CBM_SOURCE_DECL_FIXED_ARITY)
                rf.min_params = d->signature_param_count;

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

            const CBMType **param_types_arr = c_parse_signature_param_types(
                arena, d->signature_param_types, d->signature_param_count, def_module);
            const CBMType **return_types_arr =
                rf.signature ? rf.signature->data.func.return_types : NULL;
            rf.signature = cbm_type_func(arena, NULL, param_types_arr, return_types_arr);

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
    cbm_registry_build_type_short_index(reg);
    reg->c_decls = cbm_c_decl_index_build(arena, defs, def_count);
    reg->read_only = true; /* seal: shared Tier-2 registry is read-only during resolve */
    return reg;
}

/* Shared body of the Tier-2 cross entry points. Returns false when the
 * configured owner proof cannot be applied (see c_lsp.h); *status reports a
 * memo or depth failure of the walk (#1527), which dropped the walk's output.
 */
static bool c_lsp_cross_with_registry_run(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *reg, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    CBMSourceOrigin source_origin, const CBMFileResult *test_owners, CBMLSPStatus *status,
    const cbm_resolve_proof_source_t *source_context, const cbm_resolve_import_snapshot_t *imports,
    bool require_import_proofs) {
    *status = CBM_LSP_COMPLETE;
    if ((test_owners && test_owners->has_test_definition_owners &&
         source_origin != CBM_SOURCE_ORIGIN_RAW) ||
        !cbm_test_definition_owners_match(test_owners, source, source_len, cpp_mode, module_qn))
        return false;

    if (!source || source_len == 0 || !out || !reg)
        return false;

    TSParser *parser = NULL;
    /* No source provenance is carried by cached_tree. Preserve the legacy
     * cache fast path only when no configured owner proof is being used. */
    TSTree *tree = test_owners && test_owners->has_test_definition_owners ? NULL : cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return false;
        const TSLanguage *ts_lang = cpp_mode ? tree_sitter_cpp() : tree_sitter_c();
        ts_parser_set_language(parser, ts_lang);
        tree = cbm_parse_c_source(parser, source, (uint32_t)source_len, (TSParseOptions){0});
        ts_parser_delete(parser);
        owns_tree = true;
        if (!tree)
            return false;
    }
    TSNode root = ts_tree_root_node(tree);

    CLSPContext ctx;
    c_lsp_init(&ctx, arena, source, source_len, reg, module_qn, cpp_mode, out);
    ctx.source_origin = source_origin;
    ctx.import_facts = imports;
    ctx.require_import_proofs = require_import_proofs;
    if (source_context || test_owners) {
        cbm_resolve_proof_source_t proof_source =
            source_context ? *source_context
                           : cbm_result_proof_source(test_owners, source_origin, module_qn);
        (void)cbm_lsp_proof_context(&ctx.field_types, &proof_source, module_qn);
    } else if (source_origin != CBM_SOURCE_ORIGIN_RAW) {
        (void)cbm_lsp_proof_standalone_context(&ctx.field_types, source, source_len, module_qn,
                                               source_origin);
    }
    ctx.test_definition_owners = test_owners;
    ctx.registry_shared = true; /* Tier-2 shared registry: read-only, see flag doc */
    for (int i = 0; i < include_count; i++) {
        c_lsp_add_include(&ctx, include_paths[i], include_ns_qns[i]);
    }
    c_memo_test_stage(&ctx, "cross");
    c_lsp_process_file(&ctx, root);

    if (owns_tree) {
        ts_tree_delete(tree);
    }
    *status = ctx.eval_failure;
    return c_test_owners_complete(&ctx);
}

bool cbm_run_c_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                       const char *module_qn, bool cpp_mode, CBMTypeRegistry *reg,
                                       const char **include_paths, const char **include_ns_qns,
                                       int include_count, TSTree *cached_tree,
                                       CBMResolvedCallArray *out) {
    return cbm_run_c_lsp_cross_with_registry_status(
               arena, source, source_len, module_qn, cpp_mode, reg, include_paths, include_ns_qns,
               include_count, cached_tree, out) == CBM_LSP_COMPLETE;
}

CBMLSPStatus cbm_run_c_lsp_cross_with_registry_status(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *reg, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out) {
    return cbm_run_c_lsp_cross_with_registry_status_with_source(
        arena, source, source_len, module_qn, cpp_mode, reg, include_paths, include_ns_qns,
        include_count, cached_tree, out, NULL);
}

CBMLSPStatus cbm_run_c_lsp_cross_with_registry_status_with_source(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *reg, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    const cbm_resolve_proof_source_t *source_context) {
    CBMLSPStatus status;
    (void)c_lsp_cross_with_registry_run(
        arena, source, source_len, module_qn, cpp_mode, reg, include_paths, include_ns_qns,
        include_count, cached_tree, out,
        (source_context ? (CBMSourceOrigin)source_context->source_origin : CBM_SOURCE_ORIGIN_RAW),
        NULL, &status, source_context, NULL, false);
    return status;
}

/* A walk stopped by a memo or depth failure (#1527) cannot have proved every
 * configured owner, so it returns false: the caller discards the output and
 * reports the owners as unverified. */
bool cbm_run_c_lsp_cross_with_registry_with_test_owners(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *reg, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    CBMSourceOrigin source_origin, const CBMFileResult *test_owners) {
    CBMLSPStatus status;
    bool owners_ok = c_lsp_cross_with_registry_run(
        arena, source, source_len, module_qn, cpp_mode, reg, include_paths, include_ns_qns,
        include_count, cached_tree, out, source_origin, test_owners, &status, NULL, NULL, false);
    return owners_ok && status == CBM_LSP_COMPLETE;
}

/* Shared body of the per-file-registry cross entry points; same contract as
 * c_lsp_cross_with_registry_run. */
static bool c_lsp_cross_run(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, bool cpp_mode, CBMLSPDef *defs, int def_count,
                            const char **include_paths, const char **include_ns_qns,
                            int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
                            CBMSourceOrigin source_origin, const CBMFileResult *test_owners,
                            CBMLSPStatus *status, const cbm_resolve_proof_source_t *source_context,
                            const cbm_resolve_import_snapshot_t *imports,
                            bool require_import_proofs) {
    *status = CBM_LSP_COMPLETE;
    if ((test_owners && test_owners->has_test_definition_owners &&
         source_origin != CBM_SOURCE_ORIGIN_RAW) ||
        !cbm_test_definition_owners_match(test_owners, source, source_len, cpp_mode, module_qn))
        return false;

    if (!source || source_len == 0 || !out)
        return false;

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
    /* No source provenance is carried by cached_tree. Preserve the legacy
     * cache fast path only when no configured owner proof is being used. */
    TSTree *tree = test_owners && test_owners->has_test_definition_owners ? NULL : cached_tree;
    bool owns_tree = false;
    if (!tree) {
        parser = ts_parser_new();
        if (!parser)
            return false;
        const TSLanguage *ts_lang = cpp_mode ? tree_sitter_cpp() : tree_sitter_c();
        ts_parser_set_language(parser, ts_lang);
        tree = cbm_parse_c_source(parser, source, (uint32_t)source_len, (TSParseOptions){0});
        ts_parser_delete(parser);
        owns_tree = true;
        if (!tree)
            return false;
    }

    TSNode root = ts_tree_root_node(tree);

    // Finalize registry — O(1) lookups. See go_lsp.c "3c. Finalize"
    // comment for the rationale (linear-scan fallback otherwise).
    cbm_registry_finalize(&reg);
    cbm_registry_build_type_short_index(&reg);
    reg.c_decls = cbm_c_decl_index_build(arena, defs, def_count);

    // Initialize context and run
    CLSPContext ctx;
    c_lsp_init(&ctx, arena, source, source_len, &reg, module_qn, cpp_mode, out);
    ctx.source_origin = source_origin;
    ctx.import_facts = imports;
    ctx.require_import_proofs = require_import_proofs;
    if (source_context || test_owners) {
        cbm_resolve_proof_source_t proof_source =
            source_context ? *source_context
                           : cbm_result_proof_source(test_owners, source_origin, module_qn);
        (void)cbm_lsp_proof_context(&ctx.field_types, &proof_source, module_qn);
    } else if (source_origin != CBM_SOURCE_ORIGIN_RAW) {
        (void)cbm_lsp_proof_standalone_context(&ctx.field_types, source, source_len, module_qn,
                                               source_origin);
    }
    ctx.test_definition_owners = test_owners;

    // Add include mappings
    for (int i = 0; i < include_count; i++) {
        c_lsp_add_include(&ctx, include_paths[i], include_ns_qns[i]);
    }

    c_memo_test_stage(&ctx, "cross");
    c_lsp_process_file(&ctx, root);

    if (owns_tree) {
        ts_tree_delete(tree);
    }
    *status = ctx.eval_failure;
    return c_test_owners_complete(&ctx);
}

bool cbm_run_c_lsp_cross(CBMArena *arena, const char *source, int source_len, const char *module_qn,
                         bool cpp_mode, CBMLSPDef *defs, int def_count, const char **include_paths,
                         const char **include_ns_qns, int include_count, TSTree *cached_tree,
                         CBMResolvedCallArray *out) {
    return cbm_run_c_lsp_cross_status(arena, source, source_len, module_qn, cpp_mode, defs,
                                      def_count, include_paths, include_ns_qns, include_count,
                                      cached_tree, out) == CBM_LSP_COMPLETE;
}

CBMLSPStatus cbm_run_c_lsp_cross_status(CBMArena *arena, const char *source, int source_len,
                                        const char *module_qn, bool cpp_mode, CBMLSPDef *defs,
                                        int def_count, const char **include_paths,
                                        const char **include_ns_qns, int include_count,
                                        TSTree *cached_tree, CBMResolvedCallArray *out) {
    return cbm_run_c_lsp_cross_status_with_source(arena, source, source_len, module_qn, cpp_mode,
                                                  defs, def_count, include_paths, include_ns_qns,
                                                  include_count, cached_tree, out, NULL);
}

CBMLSPStatus cbm_run_c_lsp_cross_status_with_source(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMLSPDef *defs, int def_count, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    const cbm_resolve_proof_source_t *source_context) {
    CBMLSPStatus status;
    (void)c_lsp_cross_run(
        arena, source, source_len, module_qn, cpp_mode, defs, def_count, include_paths,
        include_ns_qns, include_count, cached_tree, out,
        (source_context ? (CBMSourceOrigin)source_context->source_origin : CBM_SOURCE_ORIGIN_RAW),
        NULL, &status, source_context, NULL, false);
    return status;
}

/* Same owner contract as cbm_run_c_lsp_cross_with_registry_with_test_owners. */
bool cbm_run_c_lsp_cross_with_test_owners(CBMArena *arena, const char *source, int source_len,
                                          const char *module_qn, bool cpp_mode, CBMLSPDef *defs,
                                          int def_count, const char **include_paths,
                                          const char **include_ns_qns, int include_count,
                                          TSTree *cached_tree, CBMResolvedCallArray *out,
                                          CBMSourceOrigin source_origin,
                                          const CBMFileResult *test_owners) {
    CBMLSPStatus status;
    bool owners_ok =
        c_lsp_cross_run(arena, source, source_len, module_qn, cpp_mode, defs, def_count,
                        include_paths, include_ns_qns, include_count, cached_tree, out,
                        source_origin, test_owners, &status, NULL, NULL, false);
    return owners_ok && status == CBM_LSP_COMPLETE;
}

bool cbm_run_c_lsp_cross_with_imports(CBMArena *arena, const char *source, int source_len,
                                      const char *module_qn, bool cpp_mode, CBMLSPDef *defs,
                                      int def_count, const char **paths, const char **modules,
                                      int count, TSTree *tree, CBMResolvedCallArray *out,
                                      const CBMFileResult *file,
                                      const cbm_resolve_import_snapshot_t *imports,
                                      CBMLSPStatus *status) {
    cbm_resolve_proof_source_t context =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, module_qn);
    if (!cbm_resolve_import_source_matches(imports, &context))
        imports = NULL;
    bool owners_ok = c_lsp_cross_run(arena, source, source_len, module_qn, cpp_mode, defs,
                                     def_count, paths, modules, count, tree, out,
                                     CBM_SOURCE_ORIGIN_RAW, file, status, &context, imports, true);
    return owners_ok && *status == CBM_LSP_COMPLETE;
}

bool cbm_run_c_lsp_cross_with_registry_with_imports(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *registry, const char **paths, const char **modules, int count, TSTree *tree,
    CBMResolvedCallArray *out, const CBMFileResult *file,
    const cbm_resolve_import_snapshot_t *imports, CBMLSPStatus *status) {
    cbm_resolve_proof_source_t context =
        cbm_result_proof_source(file, CBM_SOURCE_ORIGIN_RAW, module_qn);
    if (!cbm_resolve_import_source_matches(imports, &context))
        imports = NULL;
    bool owners_ok = c_lsp_cross_with_registry_run(
        arena, source, source_len, module_qn, cpp_mode, registry, paths, modules, count, tree, out,
        CBM_SOURCE_ORIGIN_RAW, file, status, &context, imports, true);
    return owners_ok && *status == CBM_LSP_COMPLETE;
}

// --- Batch cross-file LSP ---

bool cbm_batch_c_lsp_cross(CBMArena *arena, CBMBatchCLSPFile *files, int file_count,
                           CBMResolvedCallArray *out) {
    if (!files || file_count <= 0 || !out)
        return true;

    bool complete = true;
    for (int f = 0; f < file_count; f++) {
        CBMBatchCLSPFile *file = &files[f];
        memset(&out[f], 0, sizeof(CBMResolvedCallArray));

        if (!file->source || file->source_len <= 0 || file->def_count <= 0)
            continue;

        // Per-file arena: registry + temp data freed after each file
        CBMArena file_arena;
        cbm_arena_init(&file_arena);

        cbm_resolve_proof_pool_t copied_proofs;
        cbm_resolve_proof_pool_init_project(&copied_proofs, arena);
        CBMResolvedCallArray file_out;
        memset(&file_out, 0, sizeof(file_out));

        // Delegate to existing per-file function
        bool file_complete = cbm_run_c_lsp_cross(
            &file_arena, file->source, file->source_len, file->module_qn, file->cpp_mode,
            file->defs, file->def_count, file->include_paths, file->include_ns_qns,
            file->include_count, file->cached_tree, &file_out);

        complete = complete && file_complete;

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
    return complete;
}
