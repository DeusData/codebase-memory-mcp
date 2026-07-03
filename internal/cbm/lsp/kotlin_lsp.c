/*
 * kotlin_lsp.c — Kotlin Light Semantic Pass implementation.
 *
 * See kotlin_lsp.h for the architectural overview. This file walks the
 * tree-sitter Kotlin AST, populates a per-file CBMTypeRegistry, builds
 * lexical scopes, infers expression types, and emits CBMResolvedCall
 * entries when a call's target FQN can be determined statically.
 *
 * Reverse-engineered from the official Kotlin language specification
 * (kotlinlang.org/spec/) and the reference fwcd/kotlin-language-server
 * implementation. No JVM, no PSI, no BindingContext — pure C, walking
 * tree-sitter syntax with hand-rolled name resolution.
 *
 * Confidence model:
 *   - 0.95 — direct constructor call, top-level fun, exact registry hit
 *   - 0.90 — method dispatch through known receiver type
 *   - 0.85 — extension function dispatch
 *   - 0.80 — companion / object-singleton call
 *   - 0.75 — super-call (with class-hierarchy lookup)
 *   - 0.70 — `this.x()` where `this_type` is known
 *   - 0.65 — partially resolved navigation (last segment)
 *   - 0.60 — minimum floor for the LSP override path; below = drop
 *
 * Strategies emitted:
 *   - "lsp_kt_constructor"       — Foo() / Foo(args)
 *   - "lsp_kt_method"            — receiver.method() with known receiver type
 *   - "lsp_kt_extension"         — extension function dispatch
 *   - "lsp_kt_static"            — Foo.bar() where Foo is object/companion
 *   - "lsp_kt_top_level"         — bare top-level fun call
 *   - "lsp_kt_super"             — super.foo() / super<Type>.foo()
 *   - "lsp_kt_this"              — this.foo() with resolved this-type
 *   - "lsp_kt_safe"              — obj?.foo() with known obj type
 *   - "lsp_kt_lambda_it"         — it.foo() inside scope-function lambda
 *   - "lsp_kt_import"            — direct import target hit (no receiver)
 */

#include "kotlin_lsp.h"
#include "../helpers.h"
#include <ctype.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Minimal Kotlin universal builtins as real graph nodes (kt_builtins_inject_defs).
 * Amalgamation-included (see lsp_all.c); mirror of py_builtins.c. */
#include "kotlin_builtins.c"

#define KT_EVAL_MAX_DEPTH 32
#define KT_IMPORT_INITIAL_CAP 16

#define KT_CONF_CONSTRUCTOR 0.95f
#define KT_CONF_METHOD 0.90f
#define KT_CONF_EXTENSION 0.85f
#define KT_CONF_STATIC 0.80f
#define KT_CONF_SUPER 0.75f
#define KT_CONF_THIS 0.70f
#define KT_CONF_PARTIAL 0.65f
#define KT_CONF_TOP_LEVEL 0.95f
#define KT_CONF_LAMBDA_IT 0.78f
#define KT_CONF_IMPORT 0.92f

/* ── forward declarations ─────────────────────────────────────────── */

static void kt_resolve_calls_in_node_inner(KotlinLSPContext *ctx, TSNode node);

/* Depth-guarded entry for the AST call-resolution walk. The walk recurses once
 * per nesting level; a deeply-nested or cyclic file can overflow the native
 * stack (SIGSEGV) and take down the whole index. Past the cap the subtree is
 * skipped — its calls stay unresolved, which is graceful degradation, not a
 * crash. The cap is CBM_LSP_MAX_WALK_DEPTH, env-overridable via the same name.
 * The walk_depth-- runs after the inner returns, so early returns in the body
 * never leak the counter. */
static void kt_resolve_calls_in_node(KotlinLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= cbm_lsp_max_walk_depth())
        return;
    ctx->walk_depth++;
    kt_resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}
static void kt_process_class_decl(KotlinLSPContext *ctx, TSNode node);
static void kt_process_object_decl(KotlinLSPContext *ctx, TSNode node, bool is_companion,
                                   const char *outer_class_qn);
static void kt_process_function_decl(KotlinLSPContext *ctx, TSNode node);
static void kt_process_property_decl(KotlinLSPContext *ctx, TSNode node);
static void kt_register_class_members(KotlinLSPContext *ctx, const char *class_qn, TSNode body,
                                      bool is_object);
static const CBMType *kt_eval_call_expression_type(KotlinLSPContext *ctx, TSNode node);
static const CBMType *kt_eval_navigation_expression_type(KotlinLSPContext *ctx, TSNode node);
static const CBMType *kt_eval_user_type(KotlinLSPContext *ctx, TSNode node);
static void kt_process_block_stmts(KotlinLSPContext *ctx, TSNode block);
static void kt_bind_function_params(KotlinLSPContext *ctx, TSNode func_node);
static const char *kt_type_qn_of(const CBMType *t);
static char *kt_join_dot(CBMArena *a, const char *prefix, const char *name);
static const char *kt_short(const char *qn);
static char *kt_node_text(KotlinLSPContext *ctx, TSNode node);
static TSNode kt_child_kind(TSNode parent, const char *kind);
static TSNode kt_child_kind_named(TSNode parent, const char *kind);
static TSNode kt_field_named(TSNode parent, const char *field);
static bool kt_node_is(TSNode n, const char *kind);
static bool kt_node_kind_in(TSNode n, const char *const *kinds);
static const char *kt_resolve_in_default_imports(KotlinLSPContext *ctx, const char *name,
                                                 CBMKotlinUseKind kind);
static const CBMType *kt_try_smart_cast(KotlinLSPContext *ctx, TSNode call_or_nav);

/* ── helpers ──────────────────────────────────────────────────────── */

static char *kt_node_text(KotlinLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node)) {
        return NULL;
    }
    return cbm_node_text(ctx->arena, node, ctx->source);
}

/* Cross-grammar name-child resolver. Newer tree-sitter-kotlin dropped the
 * `name` field and names value decls (functions, vars, params) with
 * `simple_identifier` and type decls (classes, objects) with `type_identifier`;
 * older grammars and import/package paths use `identifier`. kt_child_kind_named
 * searches DIRECT named children only, so a class's nested constructor-param
 * simple_identifiers are never mistaken for the class name. */
static TSNode kt_name_child(TSNode node) {
    static const char *const kinds[] = {"simple_identifier", "type_identifier", "identifier"};
    for (int i = 0; i < 3; i++) {
        TSNode n = kt_child_kind_named(node, kinds[i]);
        if (!ts_node_is_null(n)) {
            return n;
        }
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

static bool kt_node_is(TSNode n, const char *kind) {
    if (ts_node_is_null(n)) {
        return false;
    }
    return strcmp(ts_node_type(n), kind) == 0;
}

static bool kt_node_kind_in(TSNode n, const char *const *kinds) {
    if (ts_node_is_null(n)) {
        return false;
    }
    const char *k = ts_node_type(n);
    for (int i = 0; kinds[i]; i++) {
        if (strcmp(k, kinds[i]) == 0) {
            return true;
        }
    }
    return false;
}

static TSNode kt_child_kind(TSNode parent, const char *kind) {
    if (ts_node_is_null(parent)) {
        return parent;
    }
    uint32_t nc = ts_node_child_count(parent);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_child(parent, i);
        if (!ts_node_is_null(c) && strcmp(ts_node_type(c), kind) == 0) {
            return c;
        }
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

static TSNode kt_child_kind_named(TSNode parent, const char *kind) {
    if (ts_node_is_null(parent)) {
        return parent;
    }
    uint32_t nc = ts_node_named_child_count(parent);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(parent, i);
        if (!ts_node_is_null(c) && strcmp(ts_node_type(c), kind) == 0) {
            return c;
        }
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

static TSNode kt_field_named(TSNode parent, const char *field) {
    if (ts_node_is_null(parent)) {
        return parent;
    }
    return ts_node_child_by_field_name(parent, field, (uint32_t)strlen(field));
}

/* Find the first descendant matching kind, depth-first, capped at max_depth. */
static TSNode kt_find_descendant_kind(TSNode node, const char *kind, int max_depth) {
    if (ts_node_is_null(node) || max_depth <= 0) {
        TSNode null_node;
        memset(&null_node, 0, sizeof(null_node));
        return null_node;
    }
    if (strcmp(ts_node_type(node), kind) == 0) {
        return node;
    }
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode found = kt_find_descendant_kind(ts_node_child(node, i), kind, max_depth - 1);
        if (!ts_node_is_null(found)) {
            return found;
        }
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

/* Return last segment after final '.'. */
static const char *kt_short(const char *qn) {
    if (!qn) {
        return NULL;
    }
    const char *last = qn;
    for (const char *p = qn; *p; p++) {
        if (*p == '.') {
            last = p + 1;
        }
    }
    return last;
}

static char *kt_join_dot(CBMArena *a, const char *prefix, const char *name) {
    if (!a || !name) {
        return NULL;
    }
    if (!prefix || !*prefix) {
        return cbm_arena_strdup(a, name);
    }
    size_t pl = strlen(prefix);
    size_t nl = strlen(name);
    char *out = (char *)cbm_arena_alloc(a, pl + 1 + nl + 1);
    if (!out) {
        return NULL;
    }
    memcpy(out, prefix, pl);
    out[pl] = '.';
    memcpy(out + pl + 1, name, nl);
    out[pl + 1 + nl] = '\0';
    return out;
}

static const char *kt_type_qn_of(const CBMType *t) {
    if (!t) {
        return NULL;
    }
    switch (t->kind) {
    case CBM_TYPE_NAMED:
        return t->data.named.qualified_name;
    case CBM_TYPE_BUILTIN:
        return t->data.builtin.name;
    case CBM_TYPE_TEMPLATE:
        return t->data.template_type.template_name;
    case CBM_TYPE_ALIAS:
        if (t->data.alias.underlying) {
            return kt_type_qn_of(t->data.alias.underlying);
        }
        return t->data.alias.alias_qn;
    case CBM_TYPE_POINTER:
        return kt_type_qn_of(t->data.pointer.elem);
    case CBM_TYPE_REFERENCE:
        return kt_type_qn_of(t->data.reference.elem);
    default:
        return NULL;
    }
}

/* Strip a single trailing '?' (nullable type). The tree-sitter grammar
 * exposes `nullable_type`; this utility flattens it. */
static const CBMType *kt_unwrap_nullable(const CBMType *t) {
    /* For our purposes, T? and T are the same — we don't track nullability
     * at the type level. Smart-cast handlers below treat them as the same.
     */
    return t;
}

/* Append a CBMResolvedCall to the output array. */
static void kt_emit_resolved(KotlinLSPContext *ctx, const char *callee_qn, const char *strategy,
                             float confidence) {
    if (ctx->debug) {
        fprintf(stderr, "[kotlin_lsp] EMIT %s -> %s [%s %.2f] (resolved_calls=%p enc=%s)\n",
                ctx->enclosing_func_qn ? ctx->enclosing_func_qn : "(null)",
                callee_qn ? callee_qn : "(null)", strategy ? strategy : "(null)",
                (double)confidence, (void *)ctx->resolved_calls,
                ctx->enclosing_func_qn ? ctx->enclosing_func_qn : "(null)");
    }
    if (!ctx->resolved_calls || !ctx->enclosing_func_qn || !callee_qn) {
        return;
    }
    if (confidence < 0.60f) {
        return;
    }

    CBMResolvedCallArray *arr = ctx->resolved_calls;
    if (arr->count >= arr->cap) {
        int new_cap = arr->cap == 0 ? 16 : arr->cap * 2;
        CBMResolvedCall *new_items = (CBMResolvedCall *)cbm_arena_alloc(
            ctx->arena, (size_t)new_cap * sizeof(CBMResolvedCall));
        if (!new_items) {
            return;
        }
        if (arr->items && arr->count > 0) {
            memcpy(new_items, arr->items, (size_t)arr->count * sizeof(CBMResolvedCall));
        }
        arr->items = new_items;
        arr->cap = new_cap;
    }
    CBMResolvedCall *rc = &arr->items[arr->count];
    memset(rc, 0, sizeof(CBMResolvedCall));
    rc->caller_qn = ctx->enclosing_func_qn;
    rc->callee_qn = cbm_arena_strdup(ctx->arena, callee_qn);
    rc->strategy = strategy;
    rc->confidence = confidence;
    arr->count++;
}

/* Detect a function_declaration's extension receiver type. The
 * tree-sitter Kotlin grammar inlines the rule `_receiver_type` (a hidden
 * rule starting with '_'), so the receiver doesn't appear as a named
 * child of that name — instead, a `user_type` (or `nullable_type` /
 * `parenthesized_type`) named child appears in source order BEFORE the
 * `identifier` (function name).
 *
 * Returns the inner type node and writes its raw text to `recv_text_out`
 * (arena-allocated). Returns null-node if the function is not an
 * extension function. */
static TSNode kt_find_extension_receiver(KotlinLSPContext *ctx, TSNode func_node, TSNode name_node,
                                         char **recv_text_out) {
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    if (recv_text_out) {
        *recv_text_out = NULL;
    }
    if (ts_node_is_null(func_node) || ts_node_is_null(name_node)) {
        return null_node;
    }
    /* Try field first (some grammar variants expose this). */
    TSNode by_field = kt_field_named(func_node, "receiver");
    if (!ts_node_is_null(by_field)) {
        if (recv_text_out) {
            *recv_text_out = kt_node_text(ctx, by_field);
        }
        return by_field;
    }
    /* Walk named children for a type-shaped node before the name. */
    uint32_t name_start = ts_node_start_byte(name_node);
    uint32_t nc = ts_node_named_child_count(func_node);
    static const char *type_kinds[] = {"user_type",          "nullable_type", "non_nullable_type",
                                       "parenthesized_type", "type",          NULL};
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(func_node, i);
        if (ts_node_is_null(c)) {
            continue;
        }
        if (ts_node_start_byte(c) >= name_start) {
            break;
        }
        if (kt_node_kind_in(c, type_kinds)) {
            if (recv_text_out) {
                *recv_text_out = kt_node_text(ctx, c);
            }
            return c;
        }
    }
    return null_node;
}

/* Extract the return type node from a function_declaration. The grammar
 * exposes it via a `type` field or as a `type`/`user_type`/`nullable_type`
 * named child appearing AFTER the `function_value_parameters` and
 * BEFORE the `function_body`. */
static TSNode kt_find_return_type(TSNode func_node) {
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    if (ts_node_is_null(func_node)) {
        return null_node;
    }
    TSNode by_field = kt_field_named(func_node, "type");
    if (!ts_node_is_null(by_field)) {
        return by_field;
    }
    /* Walk children for a type appearing after the parameters. */
    TSNode params = kt_child_kind(func_node, "function_value_parameters");
    uint32_t after_params = ts_node_is_null(params) ? 0 : ts_node_end_byte(params);
    uint32_t nc = ts_node_named_child_count(func_node);
    static const char *type_kinds[] = {"user_type",
                                       "nullable_type",
                                       "non_nullable_type",
                                       "parenthesized_type",
                                       "type",
                                       "function_type",
                                       NULL};
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(func_node, i);
        if (ts_node_is_null(c)) {
            continue;
        }
        if (ts_node_start_byte(c) < after_params) {
            continue;
        }
        const char *k = ts_node_type(c);
        if (strcmp(k, "function_body") == 0 || strcmp(k, "block") == 0) {
            break;
        }
        if (kt_node_kind_in(c, type_kinds)) {
            return c;
        }
    }
    return null_node;
}

/* Build a CBM_TYPE_FUNC signature carrying just the return type, so that
 * cbm_registry_lookup_method's caller can read rf->signature->return_types
 * to determine the chained receiver. param_names/param_types are left
 * empty — we only care about the return type for chain tracking. */
static const CBMType *kt_build_func_sig_with_return(KotlinLSPContext *ctx,
                                                    const CBMType *return_type) {
    if (!return_type || cbm_type_is_unknown(return_type)) {
        return NULL;
    }
    const char **empty_pn = (const char **)cbm_arena_alloc(ctx->arena, sizeof(const char *));
    if (!empty_pn) {
        return NULL;
    }
    empty_pn[0] = NULL;
    const CBMType **empty_pt =
        (const CBMType **)cbm_arena_alloc(ctx->arena, sizeof(const CBMType *));
    if (!empty_pt) {
        return NULL;
    }
    empty_pt[0] = NULL;
    const CBMType **rt = (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(const CBMType *));
    if (!rt) {
        return NULL;
    }
    rt[0] = return_type;
    rt[1] = NULL;
    return cbm_type_func(ctx->arena, empty_pn, empty_pt, rt);
}

/* ── public API ───────────────────────────────────────────────────── */

void kotlin_lsp_init(KotlinLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                     const CBMTypeRegistry *registry, const char *package_qn, const char *module_qn,
                     const char *project_name, const char *rel_path, CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(KotlinLSPContext));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->package_qn = package_qn ? cbm_arena_strdup(arena, package_qn) : "";
    ctx->module_qn = module_qn ? cbm_arena_strdup(arena, module_qn) : "";
    ctx->project_name = project_name ? cbm_arena_strdup(arena, project_name) : "";
    ctx->rel_path = rel_path ? cbm_arena_strdup(arena, rel_path) : "";
    ctx->resolved_calls = out;
    ctx->import_cap = KT_IMPORT_INITIAL_CAP;
    ctx->import_locals =
        (const char **)cbm_arena_alloc(arena, sizeof(const char *) * (size_t)ctx->import_cap);
    ctx->import_targets =
        (const char **)cbm_arena_alloc(arena, sizeof(const char *) * (size_t)ctx->import_cap);
    ctx->import_kinds = (CBMKotlinUseKind *)cbm_arena_alloc(arena, sizeof(CBMKotlinUseKind) *
                                                                       (size_t)ctx->import_cap);
    ctx->import_count = 0;
    ctx->debug = (getenv("CBM_LSP_DEBUG") != NULL);

    /* Compute the JVM file class name. The Kotlin convention is that
     * top-level functions/properties live in a synthetic class named
     * "<FilenameWithoutExt>Kt". For internal references we don't usually
     * need this, but we expose it for completeness. */
    if (rel_path && *rel_path) {
        const char *base = rel_path;
        for (const char *p = rel_path; *p; p++) {
            if (*p == '/' || *p == '\\') {
                base = p + 1;
            }
        }
        const char *dot = strrchr(base, '.');
        size_t name_len = dot ? (size_t)(dot - base) : strlen(base);
        char *fn = (char *)cbm_arena_alloc(arena, name_len + 3);
        if (fn) {
            memcpy(fn, base, name_len);
            /* Capitalize first letter (Kotlin file class convention is
             * to keep filename casing, but for our purposes the literal
             * filename suffices). */
            if (name_len > 0 && fn[0] >= 'a' && fn[0] <= 'z') {
                fn[0] = (char)(fn[0] - 'a' + 'A');
            }
            fn[name_len] = 'K';
            fn[name_len + 1] = 't';
            fn[name_len + 2] = '\0';
            ctx->file_class_qn = kt_join_dot(arena, ctx->package_qn, fn);
        }
    }

    /* Push the file-level scope. */
    ctx->current_scope = cbm_scope_push(arena, NULL);
}

void kotlin_lsp_add_import(KotlinLSPContext *ctx, const char *local_name, const char *target_qn,
                           CBMKotlinUseKind kind) {
    if (!ctx || !target_qn) {
        return;
    }
    if (ctx->import_count >= ctx->import_cap) {
        int new_cap = ctx->import_cap * 2;
        const char **new_locals =
            (const char **)cbm_arena_alloc(ctx->arena, sizeof(const char *) * (size_t)new_cap);
        const char **new_targets =
            (const char **)cbm_arena_alloc(ctx->arena, sizeof(const char *) * (size_t)new_cap);
        CBMKotlinUseKind *new_kinds = (CBMKotlinUseKind *)cbm_arena_alloc(
            ctx->arena, sizeof(CBMKotlinUseKind) * (size_t)new_cap);
        if (!new_locals || !new_targets || !new_kinds) {
            return;
        }
        memcpy(new_locals, ctx->import_locals, sizeof(const char *) * (size_t)ctx->import_count);
        memcpy(new_targets, ctx->import_targets, sizeof(const char *) * (size_t)ctx->import_count);
        memcpy(new_kinds, ctx->import_kinds, sizeof(CBMKotlinUseKind) * (size_t)ctx->import_count);
        ctx->import_locals = new_locals;
        ctx->import_targets = new_targets;
        ctx->import_kinds = new_kinds;
        ctx->import_cap = new_cap;
    }
    ctx->import_locals[ctx->import_count] =
        local_name ? cbm_arena_strdup(ctx->arena, local_name) : NULL;
    ctx->import_targets[ctx->import_count] = cbm_arena_strdup(ctx->arena, target_qn);
    ctx->import_kinds[ctx->import_count] = kind;
    ctx->import_count++;
}

const char *kotlin_resolve_class_name(KotlinLSPContext *ctx, const char *name) {
    if (!ctx || !name || !*name) {
        return NULL;
    }
    /* Already qualified? */
    if (strchr(name, '.')) {
        /* Prefix with project? Best-effort heuristic: leave as-is, the
         * pipeline will retry with project prefix on miss. */
        return cbm_arena_strdup(ctx->arena, name);
    }

    /* Same-package lookup: <package>.<name> */
    if (ctx->package_qn && *ctx->package_qn) {
        char *cand = kt_join_dot(ctx->arena, ctx->package_qn, name);
        if (ctx->registry && cbm_registry_lookup_type(ctx->registry, cand)) {
            return cand;
        }
        /* Project-qualified candidate — keep alongside as fallback */
    }

    /* Bare-name lookup: when the file has no `package` declaration, all
     * user-defined types are registered under their unqualified name.
     * Check this BEFORE default imports so a user-defined `StringBuilder`
     * or `Logger` shadows the stdlib one with the same short name —
     * matching how the official compiler resolves names. */
    if (ctx->registry && cbm_registry_lookup_type(ctx->registry, name)) {
        return cbm_arena_strdup(ctx->arena, name);
    }

    /* Explicit imports */
    for (int i = 0; i < ctx->import_count; i++) {
        const char *local = ctx->import_locals[i];
        if (local && strcmp(local, name) == 0 &&
            (ctx->import_kinds[i] == CBM_KT_USE_TYPE ||
             ctx->import_kinds[i] == CBM_KT_USE_UNKNOWN)) {
            return ctx->import_targets[i];
        }
    }

    /* Wildcard imports + default imports */
    for (int i = 0; i < ctx->import_count; i++) {
        if (ctx->import_kinds[i] == CBM_KT_USE_WILDCARD && ctx->import_targets[i]) {
            char *cand = kt_join_dot(ctx->arena, ctx->import_targets[i], name);
            if (ctx->registry && cbm_registry_lookup_type(ctx->registry, cand)) {
                return cand;
            }
        }
    }

    int n = 0;
    const char *const *defs = cbm_kotlin_default_import_packages(&n);
    for (int i = 0; i < n; i++) {
        char *cand = kt_join_dot(ctx->arena, defs[i], name);
        if (ctx->registry && cbm_registry_lookup_type(ctx->registry, cand)) {
            return cand;
        }
    }

    /* Same-package fallback — even if not in registry, useful for cross-file
     * lookups via the pipeline's <project>.<qn> retry. */
    if (ctx->package_qn && *ctx->package_qn) {
        return kt_join_dot(ctx->arena, ctx->package_qn, name);
    }
    return cbm_arena_strdup(ctx->arena, name);
}

const char *kotlin_resolve_function_name(KotlinLSPContext *ctx, const char *name) {
    if (!ctx || !name || !*name) {
        return NULL;
    }
    if (strchr(name, '.')) {
        return cbm_arena_strdup(ctx->arena, name);
    }

    /* Explicit function import */
    for (int i = 0; i < ctx->import_count; i++) {
        const char *local = ctx->import_locals[i];
        if (local && strcmp(local, name) == 0 &&
            (ctx->import_kinds[i] == CBM_KT_USE_FUNCTION ||
             ctx->import_kinds[i] == CBM_KT_USE_UNKNOWN ||
             ctx->import_kinds[i] == CBM_KT_USE_TYPE)) {
            return ctx->import_targets[i];
        }
    }

    /* Same-package top-level fun */
    if (ctx->package_qn && *ctx->package_qn) {
        char *cand = kt_join_dot(ctx->arena, ctx->package_qn, name);
        if (ctx->registry && cbm_registry_lookup_func(ctx->registry, cand)) {
            return cand;
        }
    }

    /* Bare-name lookup for files without a package declaration. */
    if (ctx->registry && cbm_registry_lookup_func(ctx->registry, name)) {
        return cbm_arena_strdup(ctx->arena, name);
    }

    /* Wildcard imports */
    for (int i = 0; i < ctx->import_count; i++) {
        if (ctx->import_kinds[i] == CBM_KT_USE_WILDCARD && ctx->import_targets[i]) {
            char *cand = kt_join_dot(ctx->arena, ctx->import_targets[i], name);
            if (ctx->registry && cbm_registry_lookup_func(ctx->registry, cand)) {
                return cand;
            }
        }
    }

    /* Default imports */
    const char *via_default = kt_resolve_in_default_imports(ctx, name, CBM_KT_USE_FUNCTION);
    if (via_default) {
        return via_default;
    }

    /* Cross-file sole-definer fallback. In the default package (no `package`
     * declaration) a top-level `double()` in Util.kt is callable bare from
     * Main.kt, but its registered QN embeds the defining file's path
     * ("<project>.Util.double") which the caller can't reconstruct.  When the
     * project-wide registry holds EXACTLY ONE top-level function (receiver_type
     * == NULL) whose short name matches, resolve to it.  Bounded to a single
     * candidate so an ambiguous name (>1 definer) is left unresolved — sound,
     * mirroring the registry's "unique_name" strategy.  Runs only after the
     * package/import/bare/default-import lookups miss, so it never overrides a
     * more specific match; in the per-file pass the registry holds just this
     * file's defs, so the candidate is the file's own sole top-level fun. */
    if (ctx->registry && ctx->registry->funcs) {
        const char *only_qn = NULL;
        int matches = 0;
        for (int i = 0; i < ctx->registry->func_count && matches < 2; i++) {
            const CBMRegisteredFunc *f = &ctx->registry->funcs[i];
            if (!f->qualified_name || !f->short_name) {
                continue;
            }
            if (f->receiver_type) { /* method / extension — not a bare top-level fun */
                continue;
            }
            if (strcmp(f->short_name, name) == 0) {
                only_qn = f->qualified_name;
                matches++;
            }
        }
        if (matches == 1 && only_qn) {
            return cbm_arena_strdup(ctx->arena, only_qn);
        }
    }

    return NULL;
}

static const char *kt_resolve_in_default_imports(KotlinLSPContext *ctx, const char *name,
                                                 CBMKotlinUseKind kind) {
    int n = 0;
    const char *const *defs = cbm_kotlin_default_import_packages(&n);
    for (int i = 0; i < n; i++) {
        char *cand = kt_join_dot(ctx->arena, defs[i], name);
        if (!ctx->registry) {
            continue;
        }
        if (kind == CBM_KT_USE_FUNCTION || kind == CBM_KT_USE_UNKNOWN) {
            if (cbm_registry_lookup_func(ctx->registry, cand)) {
                return cand;
            }
        }
        if (kind == CBM_KT_USE_TYPE || kind == CBM_KT_USE_UNKNOWN) {
            if (cbm_registry_lookup_type(ctx->registry, cand)) {
                return cand;
            }
        }
    }
    return NULL;
}

/* Synthesize a CBMRegisteredFunc on the fly for stdlib types where we
 * only have a method_names array (no full func registration). The
 * lifetime of the synthesized struct is the LSP context's arena. */
static const CBMRegisteredFunc *kt_synth_method(KotlinLSPContext *ctx, const char *class_qn,
                                                const char *method_name) {
    CBMRegisteredFunc *rf =
        (CBMRegisteredFunc *)cbm_arena_alloc(ctx->arena, sizeof(CBMRegisteredFunc));
    if (!rf) {
        return NULL;
    }
    memset(rf, 0, sizeof(CBMRegisteredFunc));
    rf->qualified_name = kt_join_dot(ctx->arena, class_qn, method_name);
    rf->receiver_type = cbm_arena_strdup(ctx->arena, class_qn);
    rf->short_name = cbm_arena_strdup(ctx->arena, method_name);
    rf->min_params = 0;
    return rf;
}

/* Heuristic: stdlib methods that return the same receiver type
 * (fluent style) — used for return-type tracking across chains. */
static bool kt_method_returns_self(const char *class_qn, const char *method) {
    if (!class_qn || !method) {
        return false;
    }
    if (strcmp(class_qn, "kotlin.String") == 0 || strcmp(class_qn, "java.lang.String") == 0 ||
        strcmp(class_qn, "kotlin.CharSequence") == 0 ||
        strcmp(class_qn, "kotlin.text.StringBuilder") == 0) {
        const char *self_returning[] = {
            "trim",         "trimStart",
            "trimEnd",      "trimIndent",
            "trimMargin",   "uppercase",
            "lowercase",    "replace",
            "padStart",     "padEnd",
            "repeat",       "removePrefix",
            "removeSuffix", "removeSurrounding",
            "replaceFirst", "reversed",
            "substring",    "intern",
            "format",       "plus",
            "subSequence",  NULL,
        };
        for (int i = 0; self_returning[i]; i++) {
            if (strcmp(method, self_returning[i]) == 0) {
                return true;
            }
        }
    }
    if (strstr(class_qn, ".List") || strstr(class_qn, ".MutableList") ||
        strstr(class_qn, ".Iterable") || strstr(class_qn, ".Collection")) {
        const char *self_returning[] = {
            "filter",     "filterNot",  "filterNotNull",    "filterIndexed",
            "map",        "mapNotNull", "mapIndexed",       "flatMap",
            "sorted",     "sortedBy",   "sortedDescending", "sortedByDescending",
            "sortedWith", "reversed",   "distinct",         "distinctBy",
            "take",       "takeLast",   "takeWhile",        "takeLastWhile",
            "drop",       "dropLast",   "dropWhile",        "dropLastWhile",
            "shuffled",   "asReversed", "toList",           "toMutableList",
            "ifEmpty",    NULL,
        };
        for (int i = 0; self_returning[i]; i++) {
            if (strcmp(method, self_returning[i]) == 0) {
                return true;
            }
        }
    }
    /* Builder pattern — methods named `add`, `append`, `with` typically
     * return self. Only triggered when no other info is available. */
    if (strcmp(method, "add") == 0 || strcmp(method, "append") == 0 ||
        strcmp(method, "with") == 0 || strcmp(method, "set") == 0) {
        return true;
    }
    return false;
}

/* Returns the return type of a known stdlib method when we can determine
 * it from heuristic rules (self-returning, common String/Int/Boolean
 * results). Best-effort — returns UNKNOWN when we don't know. */
static const CBMType *kt_stdlib_method_return_type(KotlinLSPContext *ctx, const char *class_qn,
                                                   const char *method) {
    if (!class_qn || !method) {
        return cbm_type_unknown();
    }
    if (kt_method_returns_self(class_qn, method)) {
        return cbm_type_named(ctx->arena, class_qn);
    }
    if (strcmp(method, "toString") == 0 || strcmp(method, "toRegex") == 0 ||
        strcmp(method, "joinToString") == 0 || strcmp(method, "toLowerCase") == 0 ||
        strcmp(method, "toUpperCase") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.String");
    }
    if (strcmp(method, "size") == 0 || strcmp(method, "length") == 0 ||
        strcmp(method, "count") == 0 || strcmp(method, "indexOf") == 0 ||
        strcmp(method, "lastIndexOf") == 0 || strcmp(method, "compareTo") == 0 ||
        strcmp(method, "hashCode") == 0 || strcmp(method, "code") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.Int");
    }
    if (strcmp(method, "isEmpty") == 0 || strcmp(method, "isNotEmpty") == 0 ||
        strcmp(method, "isBlank") == 0 || strcmp(method, "isNotBlank") == 0 ||
        strcmp(method, "contains") == 0 || strcmp(method, "containsAll") == 0 ||
        strcmp(method, "containsKey") == 0 || strcmp(method, "containsValue") == 0 ||
        strcmp(method, "startsWith") == 0 || strcmp(method, "endsWith") == 0 ||
        strcmp(method, "equals") == 0 || strcmp(method, "any") == 0 || strcmp(method, "all") == 0 ||
        strcmp(method, "none") == 0 || strcmp(method, "matches") == 0 ||
        strcmp(method, "isDigit") == 0 || strcmp(method, "isLetter") == 0 ||
        strcmp(method, "isWhitespace") == 0 || strcmp(method, "isFile") == 0 ||
        strcmp(method, "isDirectory") == 0 || strcmp(method, "exists") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.Boolean");
    }
    if (strcmp(method, "sum") == 0 || strcmp(method, "sumOf") == 0 ||
        strcmp(method, "sumBy") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.Int");
    }
    return cbm_type_unknown();
}

/* Iterative method lookup with a single depth bound on the combined
 * alias-then-super-chain walk. We never recurse — a class's alias chain
 * is followed first (capped at 16 hops to break self-referential cycles
 * that arise when a typealias resolves to a name that the registry maps
 * back to the same type), then the super chain is walked breadth-first
 * with a small visited set to break diamond inheritance. */
const CBMRegisteredFunc *kotlin_lookup_method(KotlinLSPContext *ctx, const char *class_qn,
                                              const char *method_name) {
    if (!ctx || !class_qn || !method_name || !ctx->registry) {
        return NULL;
    }

    /* Walk the alias chain iteratively, then the super chain, all using
     * a single visited-set so we cannot loop. */
    enum { VISIT_CAP = 32 };
    const char *visited[VISIT_CAP];
    int visited_n = 0;

    const char *queue[VISIT_CAP];
    int qhead = 0;
    int qtail = 0;
    queue[qtail++] = class_qn;

    while (qhead < qtail) {
        const char *cur_qn = queue[qhead++];
        if (!cur_qn) {
            continue;
        }
        bool seen = false;
        for (int v = 0; v < visited_n; v++) {
            if (strcmp(visited[v], cur_qn) == 0) {
                seen = true;
                break;
            }
        }
        if (seen) {
            continue;
        }
        if (visited_n < VISIT_CAP) {
            visited[visited_n++] = cur_qn;
        }

        /* Direct func registry hit (also follows alias chain internally
         * via cbm_registry_lookup_method_aliased). */
        const CBMRegisteredFunc *rf =
            cbm_registry_lookup_method_aliased(ctx->registry, cur_qn, method_name);
        if (rf) {
            return rf;
        }

        /* method_names fallback */
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, cur_qn);
        if (rt && rt->method_names) {
            for (int i = 0; rt->method_names[i]; i++) {
                if (strcmp(rt->method_names[i], method_name) == 0) {
                    return kt_synth_method(ctx, cur_qn, method_name);
                }
            }
        }
        if (!rt) {
            continue;
        }
        /* Enqueue alias target */
        if (rt->alias_of && qtail < VISIT_CAP) {
            queue[qtail++] = rt->alias_of;
        }
        /* Enqueue super-chain */
        if (rt->embedded_types) {
            for (int i = 0; rt->embedded_types[i] && qtail < VISIT_CAP; i++) {
                queue[qtail++] = rt->embedded_types[i];
            }
        }
    }
    return NULL;
}

const CBMType *kotlin_lookup_property_type(KotlinLSPContext *ctx, const char *class_qn,
                                           const char *prop_name) {
    if (!ctx || !class_qn || !prop_name || !ctx->registry) {
        return cbm_type_unknown();
    }
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, class_qn);
    if (!rt) {
        return cbm_type_unknown();
    }
    if (rt->field_names && rt->field_types) {
        for (int i = 0; rt->field_names[i]; i++) {
            if (strcmp(rt->field_names[i], prop_name) == 0) {
                return rt->field_types[i];
            }
        }
    }
    if (rt->embedded_types) {
        for (int i = 0; rt->embedded_types[i]; i++) {
            const CBMType *t = kotlin_lookup_property_type(ctx, rt->embedded_types[i], prop_name);
            if (!cbm_type_is_unknown(t)) {
                return t;
            }
        }
    }
    return cbm_type_unknown();
}

/* ── package and import parsing ───────────────────────────────────── */

static const char *kt_parse_package_header(KotlinLSPContext *ctx, TSNode header) {
    if (ts_node_is_null(header)) {
        return "";
    }
    /* package_header: 'package' identifier ('.' identifier)* ';'
     * The grammar has either `qualified_identifier` or a sequence of
     * identifiers; we extract the textual span between 'package' and
     * the trailing semi/newline. */
    TSNode qid = kt_child_kind_named(header, "qualified_identifier");
    if (ts_node_is_null(qid)) {
        qid = kt_name_child(header);
    }
    if (ts_node_is_null(qid)) {
        return "";
    }
    char *txt = kt_node_text(ctx, qid);
    if (!txt) {
        return "";
    }
    /* Strip whitespace around dots */
    char *out = (char *)cbm_arena_alloc(ctx->arena, strlen(txt) + 1);
    if (!out) {
        return "";
    }
    int oi = 0;
    for (int i = 0; txt[i]; i++) {
        if (txt[i] != ' ' && txt[i] != '\t' && txt[i] != '\n' && txt[i] != '\r') {
            out[oi++] = txt[i];
        }
    }
    out[oi] = '\0';
    return out;
}

static void kt_parse_import_directive(KotlinLSPContext *ctx, TSNode imp) {
    if (ts_node_is_null(imp)) {
        return;
    }
    /* Tree-sitter shape:
     *   import 'import' qualified_identifier ('.*' | 'as' identifier)?
     */
    TSNode qid = kt_child_kind_named(imp, "qualified_identifier");
    if (ts_node_is_null(qid)) {
        qid = kt_name_child(imp);
    }
    if (ts_node_is_null(qid)) {
        return;
    }
    char *qid_text = kt_node_text(ctx, qid);
    if (!qid_text) {
        return;
    }
    /* Compress whitespace */
    char *cleaned = (char *)cbm_arena_alloc(ctx->arena, strlen(qid_text) + 1);
    int oi = 0;
    for (int i = 0; qid_text[i]; i++) {
        if (qid_text[i] != ' ' && qid_text[i] != '\t' && qid_text[i] != '\n' &&
            qid_text[i] != '\r') {
            cleaned[oi++] = qid_text[i];
        }
    }
    cleaned[oi] = '\0';

    /* Detect wildcard suffix `.*` in raw text */
    char *full_text = kt_node_text(ctx, imp);
    bool wildcard = (full_text && strstr(full_text, ".*"));

    /* Detect `as <alias>` */
    const char *alias = NULL;
    if (full_text) {
        const char *as_kw = strstr(full_text, " as ");
        if (as_kw) {
            const char *p = as_kw + 4;
            while (*p == ' ' || *p == '\t') {
                p++;
            }
            char *al = (char *)cbm_arena_alloc(ctx->arena, 128);
            if (al) {
                int ai = 0;
                while (*p && (isalnum((unsigned char)*p) || *p == '_') && ai < 127) {
                    al[ai++] = *p++;
                }
                al[ai] = '\0';
                if (ai > 0) {
                    alias = al;
                }
            }
        }
    }

    if (wildcard) {
        kotlin_lsp_add_import(ctx, NULL, cleaned, CBM_KT_USE_WILDCARD);
        return;
    }

    /* Default: kind = UNKNOWN — we'll match it both ways at lookup time. */
    const char *local = alias ? alias : kt_short(cleaned);
    kotlin_lsp_add_import(ctx, local, cleaned, CBM_KT_USE_UNKNOWN);
}

/* ── definition collection ────────────────────────────────────────── */

/* Walk top-level declarations once to discover classes and top-level fns
 * so that intra-file references can resolve regardless of ordering. */
static void kt_collect_top_level_decls(KotlinLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(root, i);
        const char *kind = ts_node_type(c);
        /* The vendored Kotlin grammar can't parse `interface` at file
         * scope (and a few other constructs), producing an ERROR node
         * that wraps everything that followed. Recurse into ERROR so
         * we can still register the class/function declarations the
         * parser managed to recognize inside it. */
        if (strcmp(kind, "ERROR") == 0) {
            kt_collect_top_level_decls(ctx, c);
            continue;
        }
        if (strcmp(kind, "class_declaration") == 0) {
            kt_process_class_decl(ctx, c);
        } else if (strcmp(kind, "object_declaration") == 0) {
            kt_process_object_decl(ctx, c, false, NULL);
        } else if (strcmp(kind, "function_declaration") == 0) {
            kt_process_function_decl(ctx, c);
        } else if (strcmp(kind, "property_declaration") == 0) {
            kt_process_property_decl(ctx, c);
        } else if (strcmp(kind, "type_alias") == 0) {
            /* type_alias: 'typealias' name '=' type */
            TSNode name = kt_field_named(c, "name");
            if (ts_node_is_null(name)) {
                name = kt_name_child(c);
            }
            char *n = kt_node_text(ctx, name);
            if (!n) {
                continue;
            }
            CBMRegisteredType rt = {0};
            rt.qualified_name = kt_join_dot(ctx->arena, ctx->package_qn, n);
            rt.short_name = n;
            /* Capture the underlying type and stamp alias_of so that
             * cbm_registry_resolve_alias / lookup_method_aliased follow
             * the chain to e.g. kotlin.Int's methods. */
            TSNode rhs = kt_field_named(c, "type");
            if (ts_node_is_null(rhs)) {
                /* Find first type-shaped named child after the name */
                static const char *type_kinds[] = {"user_type",
                                                   "nullable_type",
                                                   "non_nullable_type",
                                                   "parenthesized_type",
                                                   "type",
                                                   "function_type",
                                                   NULL};
                uint32_t name_end = ts_node_end_byte(name);
                uint32_t kc = ts_node_named_child_count(c);
                for (uint32_t j = 0; j < kc; j++) {
                    TSNode tc = ts_node_named_child(c, j);
                    if (ts_node_is_null(tc)) {
                        continue;
                    }
                    if (ts_node_start_byte(tc) < name_end) {
                        continue;
                    }
                    if (kt_node_kind_in(tc, type_kinds)) {
                        rhs = tc;
                        break;
                    }
                }
            }
            if (!ts_node_is_null(rhs)) {
                const CBMType *underlying = kotlin_parse_type_node(ctx, rhs);
                if (underlying && underlying->kind == CBM_TYPE_NAMED &&
                    underlying->data.named.qualified_name) {
                    rt.alias_of = underlying->data.named.qualified_name;
                }
            }
            cbm_registry_add_type((CBMTypeRegistry *)ctx->registry, rt);
        }
    }
}

static const char *kt_qn_for_class_decl(KotlinLSPContext *ctx, TSNode class_node) {
    TSNode name = kt_field_named(class_node, "name");
    if (ts_node_is_null(name)) {
        name = kt_name_child(class_node);
    }
    if (ts_node_is_null(name)) {
        return NULL;
    }
    char *short_name = kt_node_text(ctx, name);
    if (!short_name) {
        return NULL;
    }
    if (ctx->enclosing_class_qn) {
        return kt_join_dot(ctx->arena, ctx->enclosing_class_qn, short_name);
    }
    return kt_join_dot(ctx->arena, ctx->package_qn, short_name);
}

static void kt_process_class_decl(KotlinLSPContext *ctx, TSNode node) {
    const char *class_qn = kt_qn_for_class_decl(ctx, node);
    if (!class_qn) {
        return;
    }
    /* Register the class skeleton */
    CBMRegisteredType rt = {0};
    rt.qualified_name = class_qn;
    rt.short_name = kt_short(class_qn);

    /* Collect inheritance. Older grammars wrap specifiers in a
     * `delegation_specifiers` node; newer tree-sitter-kotlin places
     * `delegation_specifier` directly under the class declaration. */
    TSNode delegation = kt_child_kind(node, "delegation_specifiers");
    {
        TSNode dcontainer = ts_node_is_null(delegation) ? node : delegation;
        const char *parents[16];
        int parent_count = 0;
        uint32_t dnc = ts_node_named_child_count(dcontainer);
        for (uint32_t di = 0; di < dnc && parent_count < 15; di++) {
            TSNode dc = ts_node_named_child(dcontainer, di);
            if (kt_node_is(dc, "delegation_specifier")) {
                /* Find user_type or constructor_invocation */
                TSNode ut = kt_find_descendant_kind(dc, "user_type", 4);
                if (ts_node_is_null(ut)) {
                    ut = kt_find_descendant_kind(dc, "constructor_invocation", 4);
                    if (!ts_node_is_null(ut)) {
                        ut = kt_find_descendant_kind(ut, "user_type", 3);
                    }
                }
                if (!ts_node_is_null(ut)) {
                    char *name_text = kt_node_text(ctx, ut);
                    if (name_text) {
                        /* Strip generics */
                        char *lt = strchr(name_text, '<');
                        if (lt) {
                            *lt = '\0';
                        }
                        const char *resolved = kotlin_resolve_class_name(ctx, name_text);
                        if (resolved) {
                            parents[parent_count++] = resolved;
                        }
                    }
                }
            }
        }
        if (parent_count > 0) {
            const char **embedded = (const char **)cbm_arena_alloc(
                ctx->arena, sizeof(const char *) * (size_t)(parent_count + 1));
            if (embedded) {
                for (int p = 0; p < parent_count; p++) {
                    embedded[p] = parents[p];
                }
                embedded[parent_count] = NULL;
                rt.embedded_types = embedded;
            }
        }
    }

    /* Determine if interface */
    TSNode mods = kt_child_kind(node, "modifiers");
    bool is_interface = false;
    if (!ts_node_is_null(mods)) {
        char *mod_text = kt_node_text(ctx, mods);
        if (mod_text && strstr(mod_text, "interface") != NULL) {
            is_interface = true;
        }
    }
    /* Also possible: the class_declaration's first child is 'class' or
     * 'interface' keyword text. */
    {
        char *full = kt_node_text(ctx, node);
        if (full) {
            /* crude check: starts-with "interface " (after modifiers). */
            const char *p = full;
            while (*p && (*p == ' ' || *p == '\t' || *p == '\n')) {
                p++;
            }
            /* Skip annotations + modifiers tokens */
            while (*p && *p != 'c' && *p != 'i') {
                /* find class/interface keyword */
                const char *cls = strstr(p, "class ");
                const char *iface = strstr(p, "interface ");
                if (cls && (!iface || cls < iface)) {
                    p = cls;
                    break;
                }
                if (iface) {
                    p = iface;
                    break;
                }
                p = "";
            }
            if (p && strncmp(p, "interface", 9) == 0) {
                is_interface = true;
            }
        }
    }
    rt.is_interface = is_interface;

    /* Collect class fields BEFORE registering the type. Source 1: primary
     * constructor val/var parameters. Source 2: class-body val/var
     * `property_declaration` nodes (e.g. `private val data = ...`). Both
     * become accessible via `name`-based scope lookup inside any method. */
    const char *fnames[64];
    const CBMType *ftypes[64];
    int field_count = 0;

    TSNode pc = kt_child_kind(node, "primary_constructor");
    if (!ts_node_is_null(pc)) {
        /* Older grammars wrap params in a `class_parameters` node; newer
         * tree-sitter-kotlin places `class_parameter` directly under the
         * primary_constructor. */
        TSNode params = kt_find_descendant_kind(pc, "class_parameters", 3);
        TSNode pcontainer = ts_node_is_null(params) ? pc : params;
        {
            uint32_t pnc = ts_node_named_child_count(pcontainer);
            for (uint32_t pi = 0; pi < pnc && field_count < 63; pi++) {
                TSNode p = ts_node_named_child(pcontainer, pi);
                if (!kt_node_is(p, "class_parameter")) {
                    continue;
                }
                char *p_text = kt_node_text(ctx, p);
                if (!p_text) {
                    continue;
                }
                bool has_val_var = (strstr(p_text, "val ") || strstr(p_text, "var "));
                if (!has_val_var) {
                    continue;
                }
                TSNode name_node = kt_field_named(p, "name");
                if (ts_node_is_null(name_node)) {
                    name_node = kt_name_child(p);
                }
                if (ts_node_is_null(name_node)) {
                    continue;
                }
                char *fname = kt_node_text(ctx, name_node);
                if (!fname) {
                    continue;
                }
                TSNode type_node = kt_field_named(p, "type");
                if (ts_node_is_null(type_node)) {
                    type_node = kt_child_kind(p, "user_type");
                }
                if (ts_node_is_null(type_node)) {
                    type_node = kt_child_kind(p, "nullable_type");
                }
                const CBMType *ft = ts_node_is_null(type_node)
                                        ? cbm_type_unknown()
                                        : kotlin_parse_type_node(ctx, type_node);
                fnames[field_count] = fname;
                ftypes[field_count] = ft;
                field_count++;
            }
        }
    }

    /* Class-body property declarations. */
    TSNode body_for_props = kt_child_kind(node, "class_body");
    if (!ts_node_is_null(body_for_props)) {
        uint32_t bnc = ts_node_named_child_count(body_for_props);
        for (uint32_t i = 0; i < bnc && field_count < 63; i++) {
            TSNode m = ts_node_named_child(body_for_props, i);
            if (!kt_node_is(m, "property_declaration")) {
                continue;
            }
            TSNode var = kt_child_kind(m, "variable_declaration");
            if (ts_node_is_null(var)) {
                continue;
            }
            TSNode id = kt_name_child(var);
            if (ts_node_is_null(id)) {
                id = kt_child_kind_named(var, "simple_identifier");
            }
            if (ts_node_is_null(id)) {
                continue;
            }
            char *pname = kt_node_text(ctx, id);
            if (!pname) {
                continue;
            }
            /* Determine type from annotation or initializer. */
            TSNode type_node = kt_child_kind(var, "type");
            if (ts_node_is_null(type_node)) {
                type_node = kt_child_kind(var, "user_type");
            }
            if (ts_node_is_null(type_node)) {
                type_node = kt_child_kind(var, "nullable_type");
            }
            const CBMType *pt = cbm_type_unknown();
            if (!ts_node_is_null(type_node)) {
                pt = kotlin_parse_type_node(ctx, type_node);
            } else {
                /* Try inferring from initializer expression. */
                uint32_t mnc = ts_node_named_child_count(m);
                for (uint32_t k = 0; k < mnc; k++) {
                    TSNode mc = ts_node_named_child(m, k);
                    const char *mk = ts_node_type(mc);
                    if (strcmp(mk, "variable_declaration") == 0 || strcmp(mk, "modifiers") == 0) {
                        continue;
                    }
                    pt = kotlin_eval_expr_type(ctx, mc);
                    if (!cbm_type_is_unknown(pt)) {
                        break;
                    }
                }
            }
            fnames[field_count] = pname;
            ftypes[field_count] = pt;
            field_count++;
        }
    }

    if (field_count > 0) {
        const char **fn = (const char **)cbm_arena_alloc(ctx->arena, sizeof(const char *) *
                                                                         (size_t)(field_count + 1));
        const CBMType **ft = (const CBMType **)cbm_arena_alloc(
            ctx->arena, sizeof(const CBMType *) * (size_t)(field_count + 1));
        if (fn && ft) {
            for (int i = 0; i < field_count; i++) {
                fn[i] = fnames[i];
                ft[i] = ftypes[i];
            }
            fn[field_count] = NULL;
            ft[field_count] = NULL;
            rt.field_names = fn;
            rt.field_types = ft;
        }
    }

    cbm_registry_add_type((CBMTypeRegistry *)ctx->registry, rt);

    /* Recurse into body for nested types and members */
    TSNode body = kt_child_kind(node, "class_body");
    if (ts_node_is_null(body)) {
        body = kt_child_kind(node, "enum_class_body");
    }
    if (!ts_node_is_null(body)) {
        const char *prev_class = ctx->enclosing_class_qn;
        ctx->enclosing_class_qn = class_qn;
        kt_register_class_members(ctx, class_qn, body, false);
        ctx->enclosing_class_qn = prev_class;
    }
}

static void kt_process_object_decl(KotlinLSPContext *ctx, TSNode node, bool is_companion,
                                   const char *outer_class_qn) {
    TSNode name = kt_field_named(node, "name");
    if (ts_node_is_null(name)) {
        name = kt_name_child(node);
    }
    char *short_name = ts_node_is_null(name) ? NULL : kt_node_text(ctx, name);
    const char *obj_qn = NULL;
    if (is_companion) {
        /* `companion object [Name]` — if no name given, use "Companion" */
        const char *companion_name = (short_name && *short_name) ? short_name : "Companion";
        obj_qn = kt_join_dot(ctx->arena, outer_class_qn ? outer_class_qn : ctx->enclosing_class_qn,
                             companion_name);
    } else {
        if (!short_name) {
            return;
        }
        if (ctx->enclosing_class_qn) {
            obj_qn = kt_join_dot(ctx->arena, ctx->enclosing_class_qn, short_name);
        } else {
            obj_qn = kt_join_dot(ctx->arena, ctx->package_qn, short_name);
        }
    }

    CBMRegisteredType rt = {0};
    rt.qualified_name = obj_qn;
    rt.short_name = kt_short(obj_qn);

    /* Inheritance for object (delegation_specifiers wrapper in older grammars,
     * else delegation_specifier directly under the object declaration). */
    TSNode delegation = kt_child_kind(node, "delegation_specifiers");
    {
        TSNode dcontainer = ts_node_is_null(delegation) ? node : delegation;
        const char *parents[16];
        int parent_count = 0;
        uint32_t dnc = ts_node_named_child_count(dcontainer);
        for (uint32_t di = 0; di < dnc && parent_count < 15; di++) {
            TSNode dc = ts_node_named_child(dcontainer, di);
            if (!kt_node_is(dc, "delegation_specifier")) {
                continue;
            }
            TSNode ut = kt_find_descendant_kind(dc, "user_type", 4);
            if (!ts_node_is_null(ut)) {
                char *t = kt_node_text(ctx, ut);
                if (t) {
                    char *lt = strchr(t, '<');
                    if (lt) {
                        *lt = '\0';
                    }
                    const char *resolved = kotlin_resolve_class_name(ctx, t);
                    if (resolved) {
                        parents[parent_count++] = resolved;
                    }
                }
            }
        }
        if (parent_count > 0) {
            const char **embedded = (const char **)cbm_arena_alloc(
                ctx->arena, sizeof(const char *) * (size_t)(parent_count + 1));
            if (embedded) {
                for (int p = 0; p < parent_count; p++) {
                    embedded[p] = parents[p];
                }
                embedded[parent_count] = NULL;
                rt.embedded_types = embedded;
            }
        }
    }

    rt.is_object = true; /* object / companion object → static-like member calls */
    cbm_registry_add_type((CBMTypeRegistry *)ctx->registry, rt);

    /* Recurse into body */
    TSNode body = kt_child_kind(node, "class_body");
    if (ts_node_is_null(body)) {
        body = kt_child_kind(node, "enum_class_body");
    }
    if (!ts_node_is_null(body)) {
        const char *prev_class = ctx->enclosing_class_qn;
        const char *prev_companion = ctx->enclosing_companion_qn;
        ctx->enclosing_class_qn = obj_qn;
        if (is_companion) {
            ctx->enclosing_companion_qn = obj_qn;
        }
        kt_register_class_members(ctx, obj_qn, body, true);
        ctx->enclosing_class_qn = prev_class;
        ctx->enclosing_companion_qn = prev_companion;
    }
}

static void kt_register_class_members(KotlinLSPContext *ctx, const char *class_qn, TSNode body,
                                      bool is_object) {
    (void)is_object;
    if (ts_node_is_null(body)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(body, i);
        const char *kind = ts_node_type(c);
        if (strcmp(kind, "function_declaration") == 0) {
            /* Method */
            TSNode name = kt_field_named(c, "name");
            if (ts_node_is_null(name)) {
                name = kt_name_child(c);
            }
            char *fname = kt_node_text(ctx, name);
            if (!fname) {
                continue;
            }
            CBMRegisteredFunc rf = {0};
            rf.qualified_name = kt_join_dot(ctx->arena, class_qn, fname);
            rf.short_name = fname;
            rf.receiver_type = class_qn;
            rf.min_params = 0;
            /* Capture return type for chain tracking. */
            TSNode rt_node = kt_find_return_type(c);
            if (!ts_node_is_null(rt_node)) {
                const CBMType *rt = kotlin_parse_type_node(ctx, rt_node);
                rf.signature = kt_build_func_sig_with_return(ctx, rt);
            }
            /* DSL receiver detection: scan the function's parameters for
             * a function_type node whose first child is a `user_type`
             * (the receiver, as in `Foo.() -> Unit`). Stash the receiver
             * QN in decorator_qns[0] with a `lambda_receiver:` prefix
             * so kt_process_call_with_lambda can pick it up at the call
             * site and propagate it as the lambda's `this`. */
            {
                TSNode params = kt_child_kind(c, "function_value_parameters");
                if (!ts_node_is_null(params)) {
                    uint32_t pnc = ts_node_named_child_count(params);
                    for (uint32_t pi = 0; pi < pnc; pi++) {
                        TSNode p = ts_node_named_child(params, pi);
                        if (!kt_node_is(p, "parameter")) {
                            continue;
                        }
                        TSNode tn = kt_field_named(p, "type");
                        if (ts_node_is_null(tn)) {
                            tn = kt_child_kind(p, "type");
                        }
                        if (ts_node_is_null(tn)) {
                            tn = kt_child_kind(p, "function_type");
                        }
                        TSNode ft = kt_node_is(tn, "function_type")
                                        ? tn
                                        : kt_find_descendant_kind(tn, "function_type", 3);
                        if (ts_node_is_null(ft)) {
                            continue;
                        }
                        TSNode recv = kt_child_kind_named(ft, "user_type");
                        if (ts_node_is_null(recv)) {
                            continue;
                        }
                        char *rt_text = kt_node_text(ctx, recv);
                        if (!rt_text) {
                            continue;
                        }
                        const char *resolved = kotlin_resolve_class_name(ctx, rt_text);
                        if (!resolved) {
                            continue;
                        }
                        const char **dq =
                            (const char **)cbm_arena_alloc(ctx->arena, 2 * sizeof(const char *));
                        if (dq) {
                            char *tag = (char *)cbm_arena_alloc(
                                ctx->arena, strlen("lambda_receiver:") + strlen(resolved) + 1);
                            if (tag) {
                                strcpy(tag, "lambda_receiver:");
                                strcat(tag, resolved);
                                dq[0] = tag;
                                dq[1] = NULL;
                                rf.decorator_qns = dq;
                            }
                        }
                        break;
                    }
                }
            }
            cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, rf);
        } else if (strcmp(kind, "property_declaration") == 0) {
            /* Property — we don't track types deeply here, just register existence */
            (void)c;
        } else if (strcmp(kind, "object_declaration") == 0) {
            /* Could be `companion object` or nested `object` */
            char *otext = kt_node_text(ctx, c);
            bool is_comp = otext && strstr(otext, "companion");
            const char *prev_class = ctx->enclosing_class_qn;
            ctx->enclosing_class_qn = class_qn;
            kt_process_object_decl(ctx, c, is_comp, class_qn);
            ctx->enclosing_class_qn = prev_class;
        } else if (strcmp(kind, "companion_object") == 0) {
            const char *prev_class = ctx->enclosing_class_qn;
            ctx->enclosing_class_qn = class_qn;
            kt_process_object_decl(ctx, c, true, class_qn);
            ctx->enclosing_class_qn = prev_class;
        } else if (strcmp(kind, "class_declaration") == 0) {
            const char *prev_class = ctx->enclosing_class_qn;
            ctx->enclosing_class_qn = class_qn;
            kt_process_class_decl(ctx, c);
            ctx->enclosing_class_qn = prev_class;
        } else if (strcmp(kind, "secondary_constructor") == 0) {
            CBMRegisteredFunc rf = {0};
            rf.qualified_name = kt_join_dot(ctx->arena, class_qn, "<init>");
            rf.short_name = "<init>";
            rf.receiver_type = class_qn;
            rf.min_params = 0;
            cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, rf);
        } else if (strcmp(kind, "enum_entry") == 0) {
            /* Enum entry — register a "field" of the enum class */
        }
    }
}

static void kt_process_function_decl(KotlinLSPContext *ctx, TSNode node) {
    TSNode name = kt_field_named(node, "name");
    if (ts_node_is_null(name)) {
        name = kt_name_child(node);
    }
    if (ts_node_is_null(name)) {
        return;
    }
    char *fname = kt_node_text(ctx, name);
    if (!fname) {
        return;
    }
    /* Detect extension function via the new helper which walks children
     * for a type-shaped node preceding the name. Handles the inlined
     * `_receiver_type` rule that tree-sitter doesn't expose as a named
     * child. */
    char *recv_text = NULL;
    TSNode receiver = kt_find_extension_receiver(ctx, node, name, &recv_text);
    (void)receiver;
    if (recv_text) {
        char *lt = strchr(recv_text, '<');
        if (lt) {
            *lt = '\0';
        }
        size_t rlen = strlen(recv_text);
        while (rlen > 0 && (recv_text[rlen - 1] == '?' || recv_text[rlen - 1] == ' ' ||
                            recv_text[rlen - 1] == '\t' || recv_text[rlen - 1] == '\n')) {
            recv_text[--rlen] = '\0';
        }
    }

    CBMRegisteredFunc rf = {0};
    rf.short_name = fname;
    rf.min_params = 0;

    if (recv_text && *recv_text) {
        const char *recv_qn = kotlin_resolve_class_name(ctx, recv_text);
        rf.receiver_type = recv_qn;
        rf.qualified_name = kt_join_dot(ctx->arena, ctx->package_qn, fname);
    } else if (ctx->enclosing_class_qn) {
        rf.receiver_type = ctx->enclosing_class_qn;
        rf.qualified_name = kt_join_dot(ctx->arena, ctx->enclosing_class_qn, fname);
    } else {
        rf.qualified_name = kt_join_dot(ctx->arena, ctx->package_qn, fname);
    }
    /* Capture return type for chain tracking. */
    TSNode rt_node = kt_find_return_type(node);
    if (!ts_node_is_null(rt_node)) {
        const CBMType *rt = kotlin_parse_type_node(ctx, rt_node);
        rf.signature = kt_build_func_sig_with_return(ctx, rt);
    }
    cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, rf);
}

static void kt_process_property_decl(KotlinLSPContext *ctx, TSNode node) {
    /* property_declaration: ('val'|'var') variable_declaration ('=' expr)? */
    TSNode var = kt_child_kind(node, "variable_declaration");
    if (ts_node_is_null(var)) {
        return;
    }
    /* The variable_declaration's identifier becomes a top-level binding. */
    TSNode id = kt_name_child(var);
    if (ts_node_is_null(id)) {
        id = kt_child_kind_named(var, "simple_identifier");
    }
    if (ts_node_is_null(id)) {
        return;
    }
    char *pname = kt_node_text(ctx, id);
    if (!pname) {
        return;
    }
    /* Type annotation? */
    TSNode type_node = kt_child_kind(var, "type");
    if (ts_node_is_null(type_node)) {
        type_node = kt_child_kind(var, "user_type");
    }
    if (ts_node_is_null(type_node)) {
        type_node = kt_child_kind(var, "nullable_type");
    }
    const CBMType *pt = cbm_type_unknown();
    if (!ts_node_is_null(type_node)) {
        pt = kotlin_parse_type_node(ctx, type_node);
    } else {
        /* Try inferring from the initializer */
        TSNode init = kt_field_named(node, "value");
        if (ts_node_is_null(init)) {
            /* Last named child may be the initializer */
            uint32_t nc = ts_node_named_child_count(node);
            if (nc > 0) {
                TSNode last = ts_node_named_child(node, nc - 1);
                const char *kk = ts_node_type(last);
                if (strstr(kk, "expression") || strstr(kk, "literal") ||
                    strcmp(kk, "call_expression") == 0 ||
                    strcmp(kk, "navigation_expression") == 0 || strcmp(kk, "lambda_literal") == 0) {
                    init = last;
                }
            }
        }
        if (!ts_node_is_null(init)) {
            pt = kotlin_eval_expr_type(ctx, init);
        }
    }
    /* Bind into file scope so top-level-property references resolve. */
    cbm_scope_bind(ctx->current_scope, cbm_arena_strdup(ctx->arena, pname), pt);
}

/* ── type parsing ─────────────────────────────────────────────────── */

const CBMType *kotlin_parse_type_node(KotlinLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node)) {
        return cbm_type_unknown();
    }
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "type") == 0) {
        /* Unwrap to inner type */
        uint32_t nc = ts_node_named_child_count(node);
        if (nc > 0) {
            return kotlin_parse_type_node(ctx, ts_node_named_child(node, 0));
        }
    }
    if (strcmp(kind, "nullable_type") == 0) {
        TSNode inner = ts_node_named_child(node, 0);
        return kotlin_parse_type_node(ctx, inner);
    }
    if (strcmp(kind, "non_nullable_type") == 0) {
        TSNode inner = ts_node_named_child(node, 0);
        return kotlin_parse_type_node(ctx, inner);
    }
    if (strcmp(kind, "parenthesized_type") == 0) {
        TSNode inner = ts_node_named_child(node, 0);
        return kotlin_parse_type_node(ctx, inner);
    }
    if (strcmp(kind, "function_type") == 0) {
        /* (A, B) -> R — represent as CALLABLE for completeness */
        return cbm_type_unknown();
    }
    if (strcmp(kind, "user_type") == 0 || strcmp(kind, "_simple_user_type") == 0) {
        return kt_eval_user_type(ctx, node);
    }
    /* Fallback: textual */
    char *txt = kt_node_text(ctx, node);
    if (!txt) {
        return cbm_type_unknown();
    }
    /* Strip generics and nullability */
    char *lt = strchr(txt, '<');
    if (lt) {
        *lt = '\0';
    }
    size_t tl = strlen(txt);
    while (tl > 0 && txt[tl - 1] == '?') {
        txt[--tl] = '\0';
    }
    /* Trim whitespace */
    while (*txt == ' ' || *txt == '\t') {
        txt++;
    }
    const char *resolved = kotlin_resolve_class_name(ctx, txt);
    if (resolved) {
        return cbm_type_named(ctx->arena, resolved);
    }
    return cbm_type_named(ctx->arena, txt);
}

static const CBMType *kt_eval_user_type(KotlinLSPContext *ctx, TSNode node) {
    /* user_type: simple_user_type ('.' simple_user_type)*
     * simple_user_type: identifier type_arguments? */
    char *txt = kt_node_text(ctx, node);
    if (!txt) {
        return cbm_type_unknown();
    }
    /* Strip generics */
    char *lt = strchr(txt, '<');
    if (lt) {
        *lt = '\0';
    }
    /* Strip whitespace */
    char *clean = (char *)cbm_arena_alloc(ctx->arena, strlen(txt) + 1);
    int oi = 0;
    for (int i = 0; txt[i]; i++) {
        if (txt[i] != ' ' && txt[i] != '\t' && txt[i] != '\n') {
            clean[oi++] = txt[i];
        }
    }
    clean[oi] = '\0';
    if (oi == 0) {
        return cbm_type_unknown();
    }
    const char *resolved = kotlin_resolve_class_name(ctx, clean);
    if (!resolved) {
        return cbm_type_named(ctx->arena, clean);
    }
    return cbm_type_named(ctx->arena, resolved);
}

/* ── expression type inference ────────────────────────────────────── */

static const CBMType *kt_eval_literal_type(KotlinLSPContext *ctx, TSNode node) {
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "string_literal") == 0 || strcmp(kind, "multiline_string_literal") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.String");
    }
    if (strcmp(kind, "character_literal") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.Char");
    }
    if (strcmp(kind, "number_literal") == 0) {
        char *txt = kt_node_text(ctx, node);
        if (!txt) {
            return cbm_type_named(ctx->arena, "kotlin.Int");
        }
        size_t l = strlen(txt);
        if (l == 0) {
            return cbm_type_named(ctx->arena, "kotlin.Int");
        }
        char tail = (char)tolower((unsigned char)txt[l - 1]);
        if (tail == 'l') {
            return cbm_type_named(ctx->arena, "kotlin.Long");
        }
        if (tail == 'f') {
            return cbm_type_named(ctx->arena, "kotlin.Float");
        }
        if (strchr(txt, '.') || strchr(txt, 'e') || strchr(txt, 'E')) {
            return cbm_type_named(ctx->arena, "kotlin.Double");
        }
        return cbm_type_named(ctx->arena, "kotlin.Int");
    }
    if (strcmp(kind, "float_literal") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.Double");
    }
    if (strcmp(kind, "boolean_literal") == 0) {
        return cbm_type_named(ctx->arena, "kotlin.Boolean");
    }
    if (strcmp(kind, "null_literal") == 0 ||
        (kt_node_text(ctx, node) && strcmp(kt_node_text(ctx, node), "null") == 0)) {
        return cbm_type_named(ctx->arena, "kotlin.Nothing");
    }
    return cbm_type_unknown();
}

const CBMType *kotlin_eval_expr_type(KotlinLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node) || !ctx) {
        return cbm_type_unknown();
    }
    if (ctx->eval_depth >= KT_EVAL_MAX_DEPTH) {
        return cbm_type_unknown();
    }
    ctx->eval_depth++;
    const CBMType *result = cbm_type_unknown();
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "expression") == 0 || strcmp(kind, "primary_expression") == 0 ||
        strcmp(kind, "parenthesized_expression") == 0 ||
        strcmp(kind, "annotated_expression") == 0 || strcmp(kind, "labeled_expression") == 0) {
        uint32_t nc = ts_node_named_child_count(node);
        if (nc > 0) {
            result = kotlin_eval_expr_type(ctx, ts_node_named_child(node, 0));
        }
        goto out;
    }
    if (strcmp(kind, "string_literal") == 0 || strcmp(kind, "multiline_string_literal") == 0 ||
        strcmp(kind, "character_literal") == 0 || strcmp(kind, "number_literal") == 0 ||
        strcmp(kind, "float_literal") == 0 || strcmp(kind, "boolean_literal") == 0) {
        result = kt_eval_literal_type(ctx, node);
        goto out;
    }
    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0) {
        char *name = kt_node_text(ctx, node);
        if (!name) {
            goto out;
        }
        if (strcmp(name, "true") == 0 || strcmp(name, "false") == 0) {
            result = cbm_type_named(ctx->arena, "kotlin.Boolean");
            goto out;
        }
        if (strcmp(name, "null") == 0) {
            result = cbm_type_named(ctx->arena, "kotlin.Nothing");
            goto out;
        }
        if (strcmp(name, "it") == 0 && ctx->it_type) {
            result = ctx->it_type;
            goto out;
        }
        /* Scope lookup */
        const CBMType *t = cbm_scope_lookup(ctx->current_scope, name);
        if (!cbm_type_is_unknown(t)) {
            result = t;
            goto out;
        }
        /* Maybe a class name (object reference) */
        const char *cls_qn = kotlin_resolve_class_name(ctx, name);
        if (cls_qn) {
            const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, cls_qn);
            if (rt) {
                result = cbm_type_named(ctx->arena, cls_qn);
                goto out;
            }
        }
        /* Maybe top-level property */
        goto out;
    }
    if (strcmp(kind, "this_expression") == 0) {
        result = ctx->this_type ? ctx->this_type : cbm_type_unknown();
        goto out;
    }
    if (strcmp(kind, "super_expression") == 0) {
        result = ctx->super_type ? ctx->super_type : cbm_type_unknown();
        goto out;
    }
    if (strcmp(kind, "call_expression") == 0) {
        result = kt_eval_call_expression_type(ctx, node);
        goto out;
    }
    if (strcmp(kind, "navigation_expression") == 0) {
        result = kt_eval_navigation_expression_type(ctx, node);
        goto out;
    }
    if (strcmp(kind, "as_expression") == 0) {
        /* obj as Foo / obj as? Foo — result is Foo */
        TSNode rhs = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        result = kotlin_parse_type_node(ctx, rhs);
        goto out;
    }
    if (strcmp(kind, "is_expression") == 0) {
        result = cbm_type_named(ctx->arena, "kotlin.Boolean");
        goto out;
    }
    if (strcmp(kind, "binary_expression") == 0 || strcmp(kind, "additive_expression") == 0 ||
        strcmp(kind, "multiplicative_expression") == 0 ||
        strcmp(kind, "comparison_expression") == 0 || strcmp(kind, "equality_expression") == 0 ||
        strcmp(kind, "range_expression") == 0) {
        /* Operator-convention dispatch: Kotlin desugars `a OP b` to
         * `a.<method>(b)` for a fixed set of operators. We detect the
         * operator token, evaluate `a`'s type, and emit a method-call
         * edge — matching what the official Kotlin compiler frontend
         * does in BindingContext.RESOLVED_CALL. Newer tree-sitter-kotlin
         * splits the old `binary_expression` into precedence-specific nodes. */
        TSNode lhs = kt_field_named(node, "left");
        TSNode rhs = kt_field_named(node, "right");
        if (ts_node_is_null(lhs) && ts_node_named_child_count(node) >= 1) {
            lhs = ts_node_named_child(node, 0);
        }
        if (ts_node_is_null(rhs) && ts_node_named_child_count(node) >= 2) {
            rhs = ts_node_named_child(node, ts_node_named_child_count(node) - 1);
        }
        const CBMType *lhs_t = kotlin_eval_expr_type(ctx, lhs);
        /* The operator token is between named children; extract from
         * source range. */
        char *full = kt_node_text(ctx, node);
        const char *op_method = NULL;
        if (full) {
            /* Common operator → method mapping. We check the FIRST
             * occurrence of each token after the lhs end and before
             * the rhs start. As a simple heuristic we check the raw
             * source text for canonical operator strings; the order
             * matters to disambiguate `==` vs `=` and `..` vs `.`. */
            uint32_t lhs_end = ts_node_is_null(lhs) ? 0 : ts_node_end_byte(lhs);
            uint32_t rhs_start =
                ts_node_is_null(rhs) ? ts_node_end_byte(node) : ts_node_start_byte(rhs);
            uint32_t node_start = ts_node_start_byte(node);
            const char *between = ctx->source + lhs_end;
            int between_len = (int)(rhs_start - lhs_end);
            if (between_len > 0 && lhs_end > node_start) {
                /* Check operators in disambiguation order. */
                if (cbm_memmem(between, (size_t)between_len, "===", 3)) {
                    op_method = NULL; /* identity, no method */
                } else if (cbm_memmem(between, (size_t)between_len, "!==", 3)) {
                    op_method = NULL;
                } else if (cbm_memmem(between, (size_t)between_len, "==", 2)) {
                    op_method = "equals";
                } else if (cbm_memmem(between, (size_t)between_len, "!=", 2)) {
                    op_method = "equals";
                } else if (cbm_memmem(between, (size_t)between_len, "..<", 3)) {
                    op_method = "rangeUntil";
                } else if (cbm_memmem(between, (size_t)between_len, "..", 2)) {
                    op_method = "rangeTo";
                } else if (cbm_memmem(between, (size_t)between_len, "+=", 2)) {
                    op_method = "plusAssign";
                } else if (cbm_memmem(between, (size_t)between_len, "-=", 2)) {
                    op_method = "minusAssign";
                } else if (cbm_memmem(between, (size_t)between_len, "*=", 2)) {
                    op_method = "timesAssign";
                } else if (cbm_memmem(between, (size_t)between_len, "/=", 2)) {
                    op_method = "divAssign";
                } else if (cbm_memmem(between, (size_t)between_len, "%=", 2)) {
                    op_method = "remAssign";
                } else if (cbm_memmem(between, (size_t)between_len, "<=", 2) ||
                           cbm_memmem(between, (size_t)between_len, ">=", 2) ||
                           cbm_memmem(between, (size_t)between_len, "<", 1) ||
                           cbm_memmem(between, (size_t)between_len, ">", 1)) {
                    op_method = "compareTo";
                } else if (cbm_memmem(between, (size_t)between_len, "+", 1)) {
                    op_method = "plus";
                } else if (cbm_memmem(between, (size_t)between_len, "-", 1)) {
                    op_method = "minus";
                } else if (cbm_memmem(between, (size_t)between_len, "*", 1)) {
                    op_method = "times";
                } else if (cbm_memmem(between, (size_t)between_len, "/", 1)) {
                    op_method = "div";
                } else if (cbm_memmem(between, (size_t)between_len, "%", 1)) {
                    op_method = "rem";
                }
            }
            (void)node_start;
        }
        if (op_method && lhs_t && !cbm_type_is_unknown(lhs_t)) {
            const char *lhs_qn = kt_type_qn_of(lhs_t);
            if (lhs_qn) {
                const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, lhs_qn, op_method);
                if (rf && rf->qualified_name) {
                    kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_operator", KT_CONF_METHOD);
                    if (rf->signature && rf->signature->kind == CBM_TYPE_FUNC &&
                        rf->signature->data.func.return_types &&
                        rf->signature->data.func.return_types[0]) {
                        result = rf->signature->data.func.return_types[0];
                        goto out;
                    }
                }
            }
        }
        /* Default: type is LHS type (numeric ops, etc.) */
        result = lhs_t;
        goto out;
    }
    if (strcmp(kind, "unary_expression") == 0 || strcmp(kind, "prefix_expression") == 0 ||
        strcmp(kind, "postfix_expression") == 0) {
        /* Unary operator desugars to method call:
         *   +x → x.unaryPlus(), -x → x.unaryMinus(),
         *   !x → x.not(), ++x → x.inc(), --x → x.dec()
         * Newer tree-sitter-kotlin uses prefix_expression/postfix_expression. */
        uint32_t nc = ts_node_named_child_count(node);
        if (nc > 0) {
            TSNode operand = ts_node_named_child(node, nc - 1);
            const CBMType *t = kotlin_eval_expr_type(ctx, operand);
            char *full = kt_node_text(ctx, node);
            const char *op_method = NULL;
            if (full) {
                /* Look at chars before operand */
                uint32_t op_end = ts_node_start_byte(operand);
                uint32_t node_start = ts_node_start_byte(node);
                if (op_end > node_start) {
                    const char *prefix = ctx->source + node_start;
                    int prefix_len = (int)(op_end - node_start);
                    if (cbm_memmem(prefix, (size_t)prefix_len, "++", 2)) {
                        op_method = "inc";
                    } else if (cbm_memmem(prefix, (size_t)prefix_len, "--", 2)) {
                        op_method = "dec";
                    } else if (cbm_memmem(prefix, (size_t)prefix_len, "!", 1)) {
                        op_method = "not";
                    } else if (cbm_memmem(prefix, (size_t)prefix_len, "-", 1)) {
                        op_method = "unaryMinus";
                    } else if (cbm_memmem(prefix, (size_t)prefix_len, "+", 1)) {
                        op_method = "unaryPlus";
                    }
                }
            }
            if (op_method && t && !cbm_type_is_unknown(t)) {
                const char *qn = kt_type_qn_of(t);
                if (qn) {
                    const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, qn, op_method);
                    if (rf && rf->qualified_name) {
                        kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_operator",
                                         KT_CONF_METHOD);
                    }
                }
            }
            result = t;
        }
        goto out;
    }
    if (strcmp(kind, "in_expression") == 0 || strcmp(kind, "check_expression") == 0) {
        /* `a in b` desugars to `b.contains(a)`; `a is T` is a Boolean type
         * check (no method). Newer tree-sitter-kotlin emits both as
         * `check_expression`; disambiguate via the operator token. */
        char *ntext = kt_node_text(ctx, node);
        bool is_membership =
            ntext && (cbm_memmem(ntext, strlen(ntext), " in ", 4) || strstr(ntext, "!in") != NULL);
        uint32_t nc = ts_node_named_child_count(node);
        if (is_membership && nc >= 2) {
            TSNode container = ts_node_named_child(node, nc - 1);
            const CBMType *ct = kotlin_eval_expr_type(ctx, container);
            const char *cqn = kt_type_qn_of(ct);
            if (cqn) {
                const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, cqn, "contains");
                if (rf && rf->qualified_name) {
                    kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_operator", KT_CONF_METHOD);
                }
            }
        }
        result = cbm_type_named(ctx->arena, "kotlin.Boolean");
        goto out;
    }
    if (strcmp(kind, "index_expression") == 0 || strcmp(kind, "indexing_expression") == 0) {
        /* `a[i]` desugars to `a.get(i)`. */
        uint32_t nc = ts_node_named_child_count(node);
        if (nc >= 1) {
            TSNode recv = ts_node_named_child(node, 0);
            const CBMType *rt = kotlin_eval_expr_type(ctx, recv);
            const char *rqn = kt_type_qn_of(rt);
            if (rqn) {
                const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, rqn, "get");
                if (rf && rf->qualified_name) {
                    kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_operator", KT_CONF_METHOD);
                    if (rf->signature && rf->signature->kind == CBM_TYPE_FUNC &&
                        rf->signature->data.func.return_types &&
                        rf->signature->data.func.return_types[0]) {
                        result = rf->signature->data.func.return_types[0];
                        goto out;
                    }
                }
            }
        }
        result = cbm_type_unknown();
        goto out;
    }
    if (strcmp(kind, "callable_reference") == 0) {
        /* Callable references take three shapes:
         *   `::topLevelFn`    — bound to receiverless top-level function
         *   `Foo::method`     — bound class method or property
         *   `instance::method`— bound to instance method
         *   `Foo::class`      — class literal (KClass<Foo>)
         *   `T::class` (reified) — class literal of reified type param
         *
         * We emit an edge with strategy `lsp_kt_callable_ref` so the call
         * graph captures the reference even when it's not invoked here.
         * The grammar exposes the LHS (optional) and RHS via named
         * children — typically two identifiers separated by `::`. */
        uint32_t nc = ts_node_named_child_count(node);
        if (nc >= 1) {
            TSNode last = ts_node_named_child(node, nc - 1);
            char *member = kt_node_text(ctx, last);
            if (member) {
                if (nc >= 2) {
                    TSNode lhs = ts_node_named_child(node, 0);
                    /* lhs may be a type or expression. Try type-resolve
                     * first (for `Foo::method`), then expression eval
                     * (for `obj::method`). */
                    char *lhs_text = kt_node_text(ctx, lhs);
                    const char *recv_qn = NULL;
                    if (lhs_text) {
                        recv_qn = kotlin_resolve_class_name(ctx, lhs_text);
                    }
                    if (!recv_qn) {
                        const CBMType *t = kotlin_eval_expr_type(ctx, lhs);
                        recv_qn = kt_type_qn_of(t);
                    }
                    if (recv_qn) {
                        if (strcmp(member, "class") == 0) {
                            /* `Foo::class` produces KClass<Foo>. We track
                             * the reference but don't emit an edge — it's
                             * a literal, not a call. */
                            result = cbm_type_named(ctx->arena, "kotlin.reflect.KClass");
                            goto out;
                        }
                        const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, recv_qn, member);
                        if (rf && rf->qualified_name) {
                            kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_callable_ref",
                                             KT_CONF_METHOD);
                        }
                    }
                } else {
                    /* `::name` — bound to top-level fn or local. */
                    const char *fn_qn = kotlin_resolve_function_name(ctx, member);
                    if (fn_qn) {
                        kt_emit_resolved(ctx, fn_qn, "lsp_kt_callable_ref", KT_CONF_TOP_LEVEL);
                    }
                }
            }
        }
        result = cbm_type_unknown();
        goto out;
    }
    if (strcmp(kind, "if_expression") == 0) {
        /* Pick first branch type */
        TSNode then_b = kt_field_named(node, "then");
        if (!ts_node_is_null(then_b)) {
            result = kotlin_eval_expr_type(ctx, then_b);
            goto out;
        }
    }
    if (strcmp(kind, "when_expression") == 0) {
        /* Find first when_entry's body */
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(node, i);
            if (kt_node_is(c, "when_entry")) {
                /* Last named child is the body expression */
                uint32_t en = ts_node_named_child_count(c);
                if (en > 0) {
                    TSNode body = ts_node_named_child(c, en - 1);
                    result = kotlin_eval_expr_type(ctx, body);
                    if (!cbm_type_is_unknown(result)) {
                        goto out;
                    }
                }
            }
        }
    }
    if (strcmp(kind, "lambda_literal") == 0 || strcmp(kind, "annotated_lambda") == 0 ||
        strcmp(kind, "anonymous_function") == 0) {
        /* Lambda — result type is unknown without context. */
        result = cbm_type_unknown();
        goto out;
    }
    if (strcmp(kind, "object_literal") == 0) {
        /* Anonymous object — closest thing is the inferred parent type */
        TSNode delegation = kt_child_kind(node, "delegation_specifiers");
        if (!ts_node_is_null(delegation)) {
            TSNode ut = kt_find_descendant_kind(delegation, "user_type", 5);
            if (!ts_node_is_null(ut)) {
                result = kt_eval_user_type(ctx, ut);
                goto out;
            }
        }
        result = cbm_type_named(ctx->arena, "kotlin.Any");
        goto out;
    }
    if (strcmp(kind, "collection_literal") == 0) {
        /* [a, b, c] — typically a List */
        result = cbm_type_named(ctx->arena, "kotlin.collections.List");
        goto out;
    }
    if (strcmp(kind, "range_expression") == 0) {
        result = cbm_type_named(ctx->arena, "kotlin.ranges.IntRange");
        goto out;
    }
    if (strcmp(kind, "throw_expression") == 0) {
        result = cbm_type_named(ctx->arena, "kotlin.Nothing");
        goto out;
    }
    if (strcmp(kind, "return_expression") == 0) {
        result = cbm_type_named(ctx->arena, "kotlin.Nothing");
        goto out;
    }
    /* Fallthrough: unwrap single-named-child wrappers */
    {
        uint32_t nc = ts_node_named_child_count(node);
        if (nc == 1) {
            result = kotlin_eval_expr_type(ctx, ts_node_named_child(node, 0));
        }
    }
out:
    ctx->eval_depth--;
    return result ? result : cbm_type_unknown();
}

static const CBMType *kt_eval_constructor_or_func_call(KotlinLSPContext *ctx, TSNode call_node,
                                                       const char *callee_text) {
    /* callee_text is a bare identifier — it may be:
     *   1) A class constructor: `Foo(...)` → returns Foo
     *   2) A top-level function: `foo(...)` → returns its declared return
     *   3) A scope binding (function reference held in a val): use type
     *   4) `it(arg)` if `it` is callable
     */
    if (!callee_text) {
        return cbm_type_unknown();
    }
    /* 0. `this`-method dispatch: if we're inside a class method or DSL
     * lambda, bare calls resolve against this_type's members first. This
     * matches Kotlin's resolution order: implicit-receiver before global. */
    if (ctx->this_type && !cbm_type_is_unknown(ctx->this_type)) {
        const char *this_qn = kt_type_qn_of(ctx->this_type);
        if (this_qn) {
            const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, this_qn, callee_text);
            if (rf && rf->qualified_name) {
                kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_this", KT_CONF_THIS);
                if (rf->signature && rf->signature->kind == CBM_TYPE_FUNC &&
                    rf->signature->data.func.return_types &&
                    rf->signature->data.func.return_types[0]) {
                    return rf->signature->data.func.return_types[0];
                }
                return cbm_type_unknown();
            }
        }
    }

    /* 1. Class? */
    const char *cls_qn = kotlin_resolve_class_name(ctx, callee_text);
    if (cls_qn && ctx->registry) {
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, cls_qn);
        if (rt) {
            /* A constructor call `Foo()` resolves to the Foo CLASS node, which the
             * textual extractor stored; there is no separate `Foo.<init>` graph
             * node, and the textual call site's callee is the bare class name
             * `Foo` (not `<init>`). Emitting cls_qn (not cls_qn.<init>) makes the
             * pipeline join's callee bare-segment match AND resolves the target. */
            kt_emit_resolved(ctx, cls_qn, "lsp_kt_constructor", KT_CONF_CONSTRUCTOR);
            return cbm_type_named(ctx->arena, cls_qn);
        }
    }
    /* 2. Top-level fun */
    const char *fn_qn = kotlin_resolve_function_name(ctx, callee_text);
    if (fn_qn) {
        kt_emit_resolved(ctx, fn_qn, "lsp_kt_top_level", KT_CONF_TOP_LEVEL);
        if (ctx->registry) {
            const CBMRegisteredFunc *rf = cbm_registry_lookup_func(ctx->registry, fn_qn);
            if (rf && rf->signature && rf->signature->kind == CBM_TYPE_FUNC) {
                if (rf->signature->data.func.return_types &&
                    rf->signature->data.func.return_types[0]) {
                    return rf->signature->data.func.return_types[0];
                }
            }
        }
        /* Heuristic return types for stdlib top-level builders. The
         * curated stdlib registers these by name only (no signature)
         * to keep the data table compact; we provide return types here
         * so chains like `mutableListOf<X>().filter { … }` propagate. */
        struct {
            const char *qn;
            const char *ret;
        } stdlib_returns[] = {
            {"kotlin.collections.listOf", "kotlin.collections.List"},
            {"kotlin.collections.listOfNotNull", "kotlin.collections.List"},
            {"kotlin.collections.mutableListOf", "kotlin.collections.MutableList"},
            {"kotlin.collections.arrayListOf", "kotlin.collections.ArrayList"},
            {"kotlin.collections.emptyList", "kotlin.collections.List"},
            {"kotlin.collections.setOf", "kotlin.collections.Set"},
            {"kotlin.collections.mutableSetOf", "kotlin.collections.MutableSet"},
            {"kotlin.collections.hashSetOf", "kotlin.collections.HashSet"},
            {"kotlin.collections.linkedSetOf", "kotlin.collections.LinkedHashSet"},
            {"kotlin.collections.emptySet", "kotlin.collections.Set"},
            {"kotlin.collections.mapOf", "kotlin.collections.Map"},
            {"kotlin.collections.mutableMapOf", "kotlin.collections.MutableMap"},
            {"kotlin.collections.hashMapOf", "kotlin.collections.HashMap"},
            {"kotlin.collections.linkedMapOf", "kotlin.collections.LinkedHashMap"},
            {"kotlin.collections.emptyMap", "kotlin.collections.Map"},
            {"kotlin.arrayOf", "kotlin.Array"},
            {"kotlin.arrayOfNulls", "kotlin.Array"},
            {"kotlin.emptyArray", "kotlin.Array"},
            {"kotlin.intArrayOf", "kotlin.IntArray"},
            {"kotlin.longArrayOf", "kotlin.LongArray"},
            {"kotlin.floatArrayOf", "kotlin.FloatArray"},
            {"kotlin.doubleArrayOf", "kotlin.DoubleArray"},
            {"kotlin.byteArrayOf", "kotlin.ByteArray"},
            {"kotlin.booleanArrayOf", "kotlin.BooleanArray"},
            {"kotlin.charArrayOf", "kotlin.CharArray"},
            {"kotlin.sequences.sequenceOf", "kotlin.sequences.Sequence"},
            {"kotlin.sequences.emptySequence", "kotlin.sequences.Sequence"},
            {"kotlin.sequences.generateSequence", "kotlin.sequences.Sequence"},
            {"kotlin.sequences.sequence", "kotlin.sequences.Sequence"},
            {"kotlin.io.readLine", "kotlin.String"},
            {"kotlin.io.readln", "kotlin.String"},
            {"kotlin.text.buildString", "kotlin.String"},
            {"kotlin.lazy", "kotlin.Lazy"},
            {"kotlin.lazyOf", "kotlin.Lazy"},
            {"kotlin.runCatching", "kotlin.Result"},
            {"kotlin.to", "kotlin.Pair"},
            {NULL, NULL},
        };
        for (int i = 0; stdlib_returns[i].qn; i++) {
            if (strcmp(stdlib_returns[i].qn, fn_qn) == 0) {
                return cbm_type_named(ctx->arena, stdlib_returns[i].ret);
            }
        }
    }
    (void)call_node;
    return cbm_type_unknown();
}

static const CBMType *kt_eval_call_expression_type(KotlinLSPContext *ctx, TSNode node) {
    /* call_expression: <expr> value_arguments (annotated_lambda)? */
    /* The first named child is the callee expression. */
    uint32_t nc = ts_node_named_child_count(node);
    if (nc == 0) {
        return cbm_type_unknown();
    }
    TSNode callee = ts_node_named_child(node, 0);
    const char *kind = ts_node_type(callee);

    if (strcmp(kind, "identifier") == 0 || strcmp(kind, "simple_identifier") == 0) {
        char *name = kt_node_text(ctx, callee);
        return kt_eval_constructor_or_func_call(ctx, node, name);
    }
    if (strcmp(kind, "navigation_expression") == 0) {
        /* obj.method(...) — inner type evaluation handles emission */
        const CBMType *t = kt_eval_navigation_expression_type(ctx, callee);
        return t;
    }
    /* Could also be `call_expression` for chained calls like foo()(args) */
    return kotlin_eval_expr_type(ctx, callee);
}

/* Extract the member-name node from a navigation_expression. Newer
 * tree-sitter-kotlin wraps the member in a `navigation_suffix` node
 * (`. simple_identifier`); older grammars placed the simple_identifier
 * directly as the trailing named child. */
static TSNode kt_nav_member_node(TSNode nav) {
    uint32_t nc = ts_node_named_child_count(nav);
    if (nc == 0) {
        TSNode z;
        memset(&z, 0, sizeof(z));
        return z;
    }
    TSNode sel = ts_node_named_child(nav, nc - 1);
    if (kt_node_is(sel, "navigation_suffix")) {
        TSNode inner = kt_child_kind_named(sel, "simple_identifier");
        if (!ts_node_is_null(inner)) {
            return inner;
        }
    }
    return sel;
}

static const CBMType *kt_eval_navigation_expression_type(KotlinLSPContext *ctx, TSNode node) {
    /* navigation_expression: <expr> ('.'|'?.'|'!!.') (simple_identifier | navigation_suffix) */
    uint32_t nc = ts_node_named_child_count(node);
    if (nc < 2) {
        return cbm_type_unknown();
    }
    TSNode receiver_node = ts_node_named_child(node, 0);
    TSNode selector = kt_nav_member_node(node);
    char *member_text = kt_node_text(ctx, selector);
    if (!member_text) {
        return cbm_type_unknown();
    }

    /* Special: super.foo */
    if (kt_node_is(receiver_node, "super_expression")) {
        if (ctx->enclosing_super_qn) {
            const CBMRegisteredFunc *rf =
                kotlin_lookup_method(ctx, ctx->enclosing_super_qn, member_text);
            if (rf && rf->qualified_name) {
                kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_super", KT_CONF_SUPER);
                return cbm_type_unknown();
            }
        }
        return cbm_type_unknown();
    }

    /* Special: this.foo */
    if (kt_node_is(receiver_node, "this_expression")) {
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredFunc *rf =
                kotlin_lookup_method(ctx, ctx->enclosing_class_qn, member_text);
            if (rf && rf->qualified_name) {
                kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_this", KT_CONF_THIS);
                if (rf->signature && rf->signature->kind == CBM_TYPE_FUNC &&
                    rf->signature->data.func.return_types &&
                    rf->signature->data.func.return_types[0]) {
                    return rf->signature->data.func.return_types[0];
                }
                return cbm_type_unknown();
            }
            /* Property access? */
            const CBMType *pt =
                kotlin_lookup_property_type(ctx, ctx->enclosing_class_qn, member_text);
            if (!cbm_type_is_unknown(pt)) {
                return pt;
            }
        }
        return cbm_type_unknown();
    }

    const CBMType *recv_type = kotlin_eval_expr_type(ctx, receiver_node);
    recv_type = kt_unwrap_nullable(recv_type);
    const char *recv_qn = kt_type_qn_of(recv_type);

    /* Receiver might be a class reference itself — `Foo.bar()` where Foo is
     * a singleton object or an enum class with a companion. */
    if (recv_qn && ctx->registry) {
        /* Check object-singleton or companion lookup */
        const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, recv_qn, member_text);
        if (rf && rf->qualified_name) {
            /* Distinguish an extension function from a member method: a member's
             * QN nests under the receiver (`<recv_qn>.<member>`), while an
             * extension `fun Recv.ext()` is a TOP-LEVEL fun whose QN does NOT
             * nest under recv_qn (only its receiver_type points back).
             * kotlin_lookup_method matches both, so pick the strategy by QN shape. */
            size_t recv_len = strlen(recv_qn);
            bool is_member = (strncmp(rf->qualified_name, recv_qn, recv_len) == 0 &&
                              rf->qualified_name[recv_len] == '.');
            const char *strat = "lsp_kt_extension";
            if (is_member) {
                /* A member call on an `object`/`companion object` singleton is a
                 * static dispatch; on a regular class instance it is a method. */
                const CBMRegisteredType *recv_rt =
                    cbm_registry_lookup_type(ctx->registry, recv_qn);
                strat = (recv_rt && recv_rt->is_object) ? "lsp_kt_static" : "lsp_kt_method";
            }
            /* A call through the lambda implicit parameter `it` (e.g. inside
             * `x.let { it.m() }`) is lambda-scoped dispatch, not a plain method. */
            if (kt_node_is(receiver_node, "identifier") ||
                kt_node_is(receiver_node, "simple_identifier")) {
                char *rtext = kt_node_text(ctx, receiver_node);
                if (rtext && strcmp(rtext, "it") == 0) {
                    strat = "lsp_kt_lambda_it";
                }
            }
            kt_emit_resolved(ctx, rf->qualified_name, strat, KT_CONF_METHOD);
            if (rf->signature && rf->signature->kind == CBM_TYPE_FUNC &&
                rf->signature->data.func.return_types && rf->signature->data.func.return_types[0]) {
                return rf->signature->data.func.return_types[0];
            }
            /* Heuristic: stdlib self-returning / known-result methods. */
            const CBMType *guess = kt_stdlib_method_return_type(ctx, recv_qn, member_text);
            if (!cbm_type_is_unknown(guess)) {
                return guess;
            }
            return cbm_type_unknown();
        }

        /* Companion fallback: receiver is class Foo, lookup Foo.Companion.<member> */
        char *companion_qn = kt_join_dot(ctx->arena, recv_qn, "Companion");
        rf = kotlin_lookup_method(ctx, companion_qn, member_text);
        if (rf && rf->qualified_name) {
            kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_static", KT_CONF_STATIC);
            if (rf->signature && rf->signature->kind == CBM_TYPE_FUNC &&
                rf->signature->data.func.return_types && rf->signature->data.func.return_types[0]) {
                return rf->signature->data.func.return_types[0];
            }
            return cbm_type_unknown();
        }

        /* Property */
        const CBMType *pt = kotlin_lookup_property_type(ctx, recv_qn, member_text);
        if (!cbm_type_is_unknown(pt)) {
            return pt;
        }

        /* Extension function: search for any registered func with
         * receiver_type == recv_qn and short_name == member_text. */
        rf = cbm_registry_lookup_method(ctx->registry, recv_qn, member_text);
        if (rf && rf->qualified_name) {
            kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_extension", KT_CONF_EXTENSION);
            return cbm_type_unknown();
        }
    }

    /* Bare receiver via `it.member` when inside scope-function lambda. */
    if (ctx->it_type && kt_node_is(receiver_node, "identifier")) {
        char *recv_text = kt_node_text(ctx, receiver_node);
        if (recv_text && strcmp(recv_text, "it") == 0) {
            const char *it_qn = kt_type_qn_of(ctx->it_type);
            if (it_qn) {
                const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, it_qn, member_text);
                if (rf && rf->qualified_name) {
                    kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_lambda_it",
                                     KT_CONF_LAMBDA_IT);
                    return cbm_type_unknown();
                }
            }
        }
    }

    /* Universal-method fallback: if the receiver type is unknown but the
     * member is a method that EVERY Kotlin reference has via kotlin.Any
     * (toString, equals, hashCode), emit on kotlin.Any. This is what
     * the fwcd LSP also resolves to when no narrower receiver is known —
     * Any is the supertype of every Kotlin reference. We emit at slightly
     * lower confidence (KT_CONF_PARTIAL) since we couldn't pin the exact
     * override target. */
    {
        static const char *kt_any_methods[] = {"toString", "equals", "hashCode", NULL};
        bool is_any = false;
        for (int i = 0; kt_any_methods[i]; i++) {
            if (strcmp(member_text, kt_any_methods[i]) == 0) {
                is_any = true;
                break;
            }
        }
        if (is_any) {
            char *q = kt_join_dot(ctx->arena, "kotlin.Any", member_text);
            kt_emit_resolved(ctx, q, "lsp_kt_any", KT_CONF_PARTIAL);
        }
    }

    return cbm_type_unknown();
}

/* ── statement processing ─────────────────────────────────────────── */

static void kt_process_statement(KotlinLSPContext *ctx, TSNode stmt);

static void kt_process_block_stmts(KotlinLSPContext *ctx, TSNode block) {
    if (ts_node_is_null(block)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(block);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(block, i);
        kt_process_statement(ctx, c);
    }
}

static void kt_bind_property_to_scope(KotlinLSPContext *ctx, TSNode prop) {
    /* property_declaration: ('val'|'var') (variable_declaration | multi_variable_declaration)
     *                       ('=' expr | property_delegate)?
     *
     * Handles three shapes:
     *   1. `val x: T = expr`       — single binding, optional type or initializer
     *   2. `val (a, b) = expr`     — destructuring; emits expr.component1(),
     *                                 expr.component2() per official LSP convention
     *   3. `val x by lazy { ... }` — property delegation; emits delegate.getValue()
     *                                 (and delegate.setValue() for var) per the
     *                                 KProperty contract.
     */
    /* Multi-variable destructuring */
    TSNode multi = kt_child_kind(prop, "multi_variable_declaration");
    if (!ts_node_is_null(multi)) {
        /* Find the initializer expression — the last named child of `prop`
         * that's not the modifiers/multi_variable_declaration. */
        uint32_t nc = ts_node_named_child_count(prop);
        TSNode init;
        memset(&init, 0, sizeof(init));
        for (uint32_t i = nc; i > 0; i--) {
            TSNode c = ts_node_named_child(prop, i - 1);
            const char *k = ts_node_type(c);
            if (strcmp(k, "modifiers") == 0 || strcmp(k, "multi_variable_declaration") == 0) {
                continue;
            }
            init = c;
            break;
        }
        const CBMType *init_t = cbm_type_unknown();
        if (!ts_node_is_null(init)) {
            init_t = kotlin_eval_expr_type(ctx, init);
        }
        /* Emit componentN calls for each variable in the multi-decl. */
        if (init_t && !cbm_type_is_unknown(init_t)) {
            const char *iqn = kt_type_qn_of(init_t);
            uint32_t mnc = ts_node_named_child_count(multi);
            for (uint32_t i = 0; i < mnc; i++) {
                TSNode v = ts_node_named_child(multi, i);
                if (!kt_node_is(v, "variable_declaration")) {
                    continue;
                }
                if (iqn) {
                    char comp[16];
                    snprintf(comp, sizeof(comp), "component%u", i + 1);
                    const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, iqn, comp);
                    if (rf && rf->qualified_name) {
                        kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_destructure",
                                         KT_CONF_METHOD);
                    }
                }
                /* Bind variable to unknown type for now. */
                TSNode vid = kt_name_child(v);
                if (!ts_node_is_null(vid)) {
                    char *vname = kt_node_text(ctx, vid);
                    if (vname) {
                        cbm_scope_bind(ctx->current_scope, cbm_arena_strdup(ctx->arena, vname),
                                       cbm_type_unknown());
                    }
                }
            }
        }
        return;
    }

    TSNode var = kt_child_kind(prop, "variable_declaration");
    if (ts_node_is_null(var)) {
        return;
    }
    TSNode id = kt_name_child(var);
    if (ts_node_is_null(id)) {
        id = kt_child_kind_named(var, "simple_identifier");
    }
    if (ts_node_is_null(id)) {
        return;
    }
    char *name = kt_node_text(ctx, id);
    if (!name) {
        return;
    }
    /* Type annotation? */
    TSNode type_node = kt_child_kind(var, "type");
    if (ts_node_is_null(type_node)) {
        type_node = kt_child_kind(var, "user_type");
    }
    if (ts_node_is_null(type_node)) {
        type_node = kt_child_kind(var, "nullable_type");
    }

    /* Property delegation: `val x by Foo()` — emit getValue (and setValue
     * for var) on the delegate's type. */
    TSNode delegate = kt_child_kind(prop, "property_delegate");
    if (!ts_node_is_null(delegate)) {
        /* Find inner expression of the delegate. */
        uint32_t dnc = ts_node_named_child_count(delegate);
        if (dnc > 0) {
            TSNode inner = ts_node_named_child(delegate, 0);
            const CBMType *dt = kotlin_eval_expr_type(ctx, inner);
            if (dt && !cbm_type_is_unknown(dt)) {
                const char *dqn = kt_type_qn_of(dt);
                if (dqn) {
                    const CBMRegisteredFunc *gv = kotlin_lookup_method(ctx, dqn, "getValue");
                    if (gv && gv->qualified_name) {
                        kt_emit_resolved(ctx, gv->qualified_name, "lsp_kt_delegate",
                                         KT_CONF_METHOD);
                    }
                    /* Detect var (mutable) by looking for `var` keyword in
                     * the property's source range. */
                    char *src_text = kt_node_text(ctx, prop);
                    if (src_text && strstr(src_text, "var ")) {
                        const CBMRegisteredFunc *sv = kotlin_lookup_method(ctx, dqn, "setValue");
                        if (sv && sv->qualified_name) {
                            kt_emit_resolved(ctx, sv->qualified_name, "lsp_kt_delegate",
                                             KT_CONF_METHOD);
                        }
                    }
                }
            }
        }
    }

    const CBMType *t = cbm_type_unknown();
    if (!ts_node_is_null(type_node)) {
        t = kotlin_parse_type_node(ctx, type_node);
    } else {
        /* Scan named children of the property_declaration for an
         * initializer expression after the variable_declaration. */
        uint32_t nc = ts_node_named_child_count(prop);
        for (uint32_t i = 0; i < nc; i++) {
            TSNode c = ts_node_named_child(prop, i);
            const char *k = ts_node_type(c);
            if (strcmp(k, "variable_declaration") == 0 || strcmp(k, "modifiers") == 0 ||
                strcmp(k, "property_delegate") == 0) {
                continue;
            }
            t = kotlin_eval_expr_type(ctx, c);
            if (!cbm_type_is_unknown(t)) {
                break;
            }
        }
    }
    cbm_scope_bind(ctx->current_scope, cbm_arena_strdup(ctx->arena, name), t);
}

/* Smart-cast inside `if (x is Foo)`. Detects pattern in stmt and binds
 * narrowed type into a child scope on the then-branch. */
static void kt_apply_smart_cast(KotlinLSPContext *ctx, TSNode condition_expr, bool then_branch) {
    if (ts_node_is_null(condition_expr) || !then_branch) {
        return;
    }
    /* is_expression (older) or check_expression (newer grammar): <expr> 'is'
     * <type>. Only `is`/`!is` narrows the type — skip `in` membership. */
    bool is_check = kt_node_is(condition_expr, "is_expression");
    if (!is_check && kt_node_is(condition_expr, "check_expression")) {
        char *ctext = kt_node_text(ctx, condition_expr);
        if (ctext &&
            (cbm_memmem(ctext, strlen(ctext), " is ", 4) != NULL || strstr(ctext, "!is") != NULL)) {
            is_check = true;
        }
    }
    if (!is_check) {
        return;
    }
    TSNode lhs = ts_node_named_child(condition_expr, 0);
    TSNode rhs = ts_node_named_child(condition_expr, ts_node_named_child_count(condition_expr) - 1);
    if (ts_node_is_null(lhs) || ts_node_is_null(rhs)) {
        return;
    }
    if (!(kt_node_is(lhs, "identifier") || kt_node_is(lhs, "simple_identifier"))) {
        return;
    }
    char *name = kt_node_text(ctx, lhs);
    if (!name) {
        return;
    }
    const CBMType *t = kotlin_parse_type_node(ctx, rhs);
    if (!cbm_type_is_unknown(t)) {
        cbm_scope_bind(ctx->current_scope, cbm_arena_strdup(ctx->arena, name), t);
    }
}

static void kt_process_if_expression(KotlinLSPContext *ctx, TSNode node) {
    TSNode cond = kt_field_named(node, "condition");
    if (ts_node_is_null(cond)) {
        cond = kt_child_kind(node, "condition");
    }
    if (ts_node_is_null(cond)) {
        /* parenthesized condition fallback */
        cond = ts_node_named_child(node, 0);
    }

    TSNode then_b = kt_field_named(node, "then");
    if (ts_node_is_null(then_b)) {
        /* Find the first non-condition block/expression */
        uint32_t nc = ts_node_named_child_count(node);
        for (uint32_t i = 1; i < nc; i++) {
            TSNode c = ts_node_named_child(node, i);
            const char *k = ts_node_type(c);
            if (strcmp(k, "block") == 0 || strcmp(k, "control_structure_body") == 0 ||
                strstr(k, "expression")) {
                then_b = c;
                break;
            }
        }
    }

    if (!ts_node_is_null(then_b)) {
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
        if (!ts_node_is_null(cond)) {
            kt_apply_smart_cast(ctx, cond, true);
        }
        if (kt_node_is(then_b, "block") || kt_node_is(then_b, "control_structure_body")) {
            kt_process_block_stmts(ctx, then_b);
        } else {
            kt_resolve_calls_in_node(ctx, then_b);
            kotlin_eval_expr_type(ctx, then_b);
        }
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
    }
    /* Resolve calls inside the condition itself */
    if (!ts_node_is_null(cond)) {
        kt_resolve_calls_in_node(ctx, cond);
    }
}

static void kt_process_when_expression(KotlinLSPContext *ctx, TSNode node) {
    /* when (subject) { entries } — bind subject type as `it`-like? Actually
     * Kotlin's `when (x) { is Foo -> ... }` creates smart-cast on x. */
    TSNode subject = kt_child_kind(node, "when_subject");
    char *subject_name = NULL;
    const CBMType *subject_type = NULL;
    if (!ts_node_is_null(subject)) {
        /* when_subject: '(' expression ')' or '(' val name = expr ')' */
        TSNode inner = ts_node_named_child(subject, 0);
        if (!ts_node_is_null(inner)) {
            subject_type = kotlin_eval_expr_type(ctx, inner);
            if (kt_node_is(inner, "identifier") || kt_node_is(inner, "simple_identifier")) {
                subject_name = kt_node_text(ctx, inner);
            }
        }
    }
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(node, i);
        if (!kt_node_is(c, "when_entry")) {
            continue;
        }
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
        /* Look for `is Type` conditions */
        uint32_t en = ts_node_named_child_count(c);
        for (uint32_t e = 0; e < en; e++) {
            TSNode ec = ts_node_named_child(c, e);
            if (kt_node_is(ec, "_when_condition") || kt_node_is(ec, "when_condition") ||
                kt_node_is(ec, "type_test")) {
                /* Find a 'type' node and apply smart-cast on subject_name */
                TSNode tt = kt_find_descendant_kind(ec, "type", 3);
                if (ts_node_is_null(tt)) {
                    tt = kt_find_descendant_kind(ec, "user_type", 3);
                }
                if (!ts_node_is_null(tt) && subject_name) {
                    const CBMType *narrow = kotlin_parse_type_node(ctx, tt);
                    if (!cbm_type_is_unknown(narrow)) {
                        cbm_scope_bind(ctx->current_scope,
                                       cbm_arena_strdup(ctx->arena, subject_name), narrow);
                    }
                }
            }
        }
        /* Process entry body */
        if (en > 0) {
            TSNode body = ts_node_named_child(c, en - 1);
            if (kt_node_is(body, "block")) {
                kt_process_block_stmts(ctx, body);
            } else {
                kt_resolve_calls_in_node(ctx, body);
                kotlin_eval_expr_type(ctx, body);
            }
        }
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
    }
    (void)subject_type;
}

static void kt_process_for_statement(KotlinLSPContext *ctx, TSNode node) {
    /* for (x in iter) body
     *
     * Kotlin desugars this to roughly:
     *   val it = iter.iterator()
     *   while (it.hasNext()) {
     *       val x = it.next()
     *       body
     *   }
     *
     * We emit method calls for `iterator()`, `hasNext()`, and `next()`
     * to match what the official LSP's BindingContext.LOOP_RANGE_*
     * slices contain — these are real graph edges users care about. */
    TSNode loop_var = kt_child_kind_named(node, "variable_declaration");
    TSNode iter = kt_field_named(node, "iterable");
    if (ts_node_is_null(iter)) {
        /* Find 'in' position — iterable is the expr after 'in' */
        uint32_t nc = ts_node_named_child_count(node);
        if (nc >= 2) {
            iter = ts_node_named_child(node, 1);
        }
    }
    const CBMType *iter_t = kotlin_eval_expr_type(ctx, iter);
    if (iter_t && !cbm_type_is_unknown(iter_t)) {
        const char *iqn = kt_type_qn_of(iter_t);
        if (iqn) {
            const char *protocol[] = {"iterator", "hasNext", "next", NULL};
            for (int i = 0; protocol[i]; i++) {
                const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, iqn, protocol[i]);
                if (rf && rf->qualified_name) {
                    kt_emit_resolved(ctx, rf->qualified_name, "lsp_kt_iterator", KT_CONF_METHOD);
                }
            }
        }
    }
    (void)iter_t;

    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    if (!ts_node_is_null(loop_var)) {
        TSNode id = kt_name_child(loop_var);
        if (ts_node_is_null(id)) {
            id = kt_child_kind_named(loop_var, "simple_identifier");
        }
        if (!ts_node_is_null(id)) {
            char *name = kt_node_text(ctx, id);
            if (name) {
                cbm_scope_bind(ctx->current_scope, cbm_arena_strdup(ctx->arena, name),
                               cbm_type_unknown());
            }
        }
    }
    /* Body */
    uint32_t nc = ts_node_named_child_count(node);
    if (nc > 0) {
        TSNode body = ts_node_named_child(node, nc - 1);
        if (kt_node_is(body, "block")) {
            kt_process_block_stmts(ctx, body);
        } else {
            kt_resolve_calls_in_node(ctx, body);
            kotlin_eval_expr_type(ctx, body);
        }
    }
    ctx->current_scope = cbm_scope_pop(ctx->current_scope);
}

static void kt_process_lambda(KotlinLSPContext *ctx, TSNode lambda, const CBMType *receiver_type) {
    /* lambda_literal: '{' lambda_parameters? '->' statements '}'
     * If no parameters, `it` is bound to the receiver. */
    if (ts_node_is_null(lambda)) {
        return;
    }
    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    const CBMType *prev_it = ctx->it_type;
    /* DSL receiver: when the callee's lambda parameter has a function-with-
     * receiver type (`Foo.() -> Unit`), the lambda's `this` is Foo and bare
     * calls inside resolve against Foo's members. The dsl_this_type is
     * passed via receiver_type when the caller can determine it. */
    const CBMType *prev_this = ctx->this_type;
    if (receiver_type && !cbm_type_is_unknown(receiver_type)) {
        ctx->this_type = receiver_type;
    }
    TSNode params = kt_child_kind(lambda, "lambda_parameters");
    if (ts_node_is_null(params)) {
        if (receiver_type && !cbm_type_is_unknown(receiver_type)) {
            ctx->it_type = receiver_type;
        }
    } else {
        uint32_t pnc = ts_node_named_child_count(params);
        for (uint32_t i = 0; i < pnc; i++) {
            TSNode pn = ts_node_named_child(params, i);
            TSNode id = kt_name_child(pn);
            if (ts_node_is_null(id)) {
                id = kt_child_kind_named(pn, "simple_identifier");
            }
            if (!ts_node_is_null(id)) {
                char *name = kt_node_text(ctx, id);
                if (name) {
                    /* Type annotation? */
                    TSNode tn = kt_child_kind(pn, "type");
                    const CBMType *t =
                        ts_node_is_null(tn) ? cbm_type_unknown() : kotlin_parse_type_node(ctx, tn);
                    cbm_scope_bind(ctx->current_scope, cbm_arena_strdup(ctx->arena, name), t);
                }
            }
        }
    }
    /* Walk lambda body */
    uint32_t nc = ts_node_named_child_count(lambda);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(lambda, i);
        const char *k = ts_node_type(c);
        if (strcmp(k, "lambda_parameters") == 0) {
            continue;
        }
        kt_process_statement(ctx, c);
    }
    ctx->it_type = prev_it;
    ctx->this_type = prev_this;
    ctx->current_scope = cbm_scope_pop(ctx->current_scope);
}

static void kt_process_call_with_lambda(KotlinLSPContext *ctx, TSNode call_node) {
    /* If call has trailing lambda, propagate the receiver type as `it`.
     *   xs.forEach { println(it) }   — `it` is element type of xs
     *   "abc".let { … }              — `it` is String
     */
    uint32_t nc = ts_node_named_child_count(call_node);
    /* Resolve callee normally for emission */
    (void)kt_eval_call_expression_type(ctx, call_node);
    /* Find trailing lambda */
    TSNode lambda;
    memset(&lambda, 0, sizeof(lambda));
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(call_node, i);
        /* Newer tree-sitter-kotlin nests the trailing lambda inside a
         * `call_suffix` node; descend into it. */
        if (kt_node_is(c, "call_suffix")) {
            TSNode al = kt_child_kind(c, "annotated_lambda");
            if (ts_node_is_null(al)) {
                al = kt_child_kind(c, "lambda_literal");
            }
            if (ts_node_is_null(al)) {
                continue;
            }
            c = al;
        }
        if (kt_node_is(c, "annotated_lambda") || kt_node_is(c, "lambda_literal")) {
            lambda = c;
            /* Could be wrapped in annotated_lambda */
            if (kt_node_is(c, "annotated_lambda")) {
                TSNode inner = kt_child_kind(c, "lambda_literal");
                if (!ts_node_is_null(inner)) {
                    lambda = inner;
                }
            }
            break;
        }
    }
    if (ts_node_is_null(lambda)) {
        return;
    }
    /* Receiver type for `it` and DSL `this`:
     *   - If callee is `recv.fn` and the resolved fn has a
     *     `lambda_receiver:` decorator hint stamped at registration, use
     *     that as the lambda's `this` (DSL builders).
     *   - Otherwise pass the navigation receiver's type as `it`.
     */
    const CBMType *recv_t = NULL;
    TSNode callee = ts_node_named_child(call_node, 0);
    if (kt_node_is(callee, "navigation_expression")) {
        TSNode rcv_node = ts_node_named_child(callee, 0);
        const CBMType *outer_recv = kotlin_eval_expr_type(ctx, rcv_node);
        const char *outer_qn = kt_type_qn_of(outer_recv);
        if (outer_qn) {
            TSNode sel = kt_nav_member_node(callee);
            char *member = kt_node_text(ctx, sel);
            if (member) {
                const CBMRegisteredFunc *rf = kotlin_lookup_method(ctx, outer_qn, member);
                if (rf && rf->decorator_qns) {
                    for (int i = 0; rf->decorator_qns[i]; i++) {
                        const char *d = rf->decorator_qns[i];
                        if (strncmp(d, "lambda_receiver:", 16) == 0) {
                            recv_t = cbm_type_named(ctx->arena, d + 16);
                            break;
                        }
                    }
                }
            }
        }
        if (!recv_t) {
            recv_t = outer_recv;
        }
    } else if (kt_node_is(callee, "identifier") || kt_node_is(callee, "simple_identifier")) {
        /* Bare-fun call: check the resolved function's lambda receiver. */
        char *fname = kt_node_text(ctx, callee);
        if (fname) {
            const char *fn_qn = kotlin_resolve_function_name(ctx, fname);
            if (fn_qn && ctx->registry) {
                const CBMRegisteredFunc *rf = cbm_registry_lookup_func(ctx->registry, fn_qn);
                if (rf && rf->decorator_qns) {
                    for (int i = 0; rf->decorator_qns[i]; i++) {
                        const char *d = rf->decorator_qns[i];
                        if (strncmp(d, "lambda_receiver:", 16) == 0) {
                            recv_t = cbm_type_named(ctx->arena, d + 16);
                            break;
                        }
                    }
                }
            }
        }
    }
    kt_process_lambda(ctx, lambda, recv_t);
}

static void kt_process_statement(KotlinLSPContext *ctx, TSNode stmt) {
    if (ts_node_is_null(stmt)) {
        return;
    }
    const char *kind = ts_node_type(stmt);
    /* Newer tree-sitter-kotlin wraps a body's statements in a `statements`
     * node (where older grammars used `block`/bare children). Unwrap either so
     * each real statement is bound + resolved in order. */
    if (strcmp(kind, "statements") == 0 || strcmp(kind, "block") == 0) {
        kt_process_block_stmts(ctx, stmt);
        return;
    }
    if (strcmp(kind, "property_declaration") == 0) {
        kt_bind_property_to_scope(ctx, stmt);
        /* Resolve calls within initializer */
        kt_resolve_calls_in_node(ctx, stmt);
        return;
    }
    if (strcmp(kind, "function_declaration") == 0) {
        /* Local function — register and recurse */
        TSNode name = kt_field_named(stmt, "name");
        if (ts_node_is_null(name)) {
            name = kt_child_kind_named(stmt, "simple_identifier");
        }
        if (ts_node_is_null(name)) {
            name = kt_name_child(stmt);
        }
        char *fname = kt_node_text(ctx, name);
        if (!fname) {
            return;
        }
        /* Register the local fn so call sites in the enclosing scope
         * can resolve it via kotlin_resolve_function_name. The QN is
         * "<enclosing_func_qn>.<fname>" so it's distinct from a top-level
         * function with the same short name. */
        const char *prev_func = ctx->enclosing_func_qn;
        const char *new_qn = kt_join_dot(ctx->arena, prev_func ? prev_func : ctx->module_qn, fname);
        {
            CBMRegisteredFunc lrf = {0};
            lrf.qualified_name = new_qn;
            lrf.short_name = fname;
            lrf.min_params = 0;
            cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, lrf);
            /* Also register a same-package alias so bare `inner()` calls
             * succeed via the same-package fallback in
             * kotlin_resolve_function_name. */
            if (ctx->package_qn && *ctx->package_qn) {
                CBMRegisteredFunc alias = lrf;
                alias.qualified_name = kt_join_dot(ctx->arena, ctx->package_qn, fname);
                cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, alias);
            } else {
                /* No package — register at root. */
                CBMRegisteredFunc alias = lrf;
                alias.qualified_name = cbm_arena_strdup(ctx->arena, fname);
                cbm_registry_add_func((CBMTypeRegistry *)ctx->registry, alias);
            }
        }
        ctx->enclosing_func_qn = new_qn;
        TSNode body = kt_field_named(stmt, "body");
        if (ts_node_is_null(body)) {
            body = kt_child_kind(stmt, "function_body");
        }
        if (ts_node_is_null(body)) {
            body = kt_child_kind(stmt, "block");
        }
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
        kt_bind_function_params(ctx, stmt);
        if (!ts_node_is_null(body)) {
            kt_resolve_calls_in_node(ctx, body);
        }
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
        ctx->enclosing_func_qn = prev_func;
        return;
    }
    if (strcmp(kind, "class_declaration") == 0 || strcmp(kind, "object_declaration") == 0) {
        /* Already registered in kt_collect_top_level_decls; recurse for nested */
        return;
    }
    if (strcmp(kind, "if_expression") == 0) {
        kt_process_if_expression(ctx, stmt);
        return;
    }
    if (strcmp(kind, "when_expression") == 0) {
        kt_process_when_expression(ctx, stmt);
        return;
    }
    if (strcmp(kind, "for_statement") == 0) {
        kt_process_for_statement(ctx, stmt);
        return;
    }
    if (strcmp(kind, "while_statement") == 0 || strcmp(kind, "do_while_statement") == 0) {
        kt_resolve_calls_in_node(ctx, stmt);
        return;
    }
    if (strcmp(kind, "try_expression") == 0) {
        kt_resolve_calls_in_node(ctx, stmt);
        return;
    }
    if (strcmp(kind, "call_expression") == 0) {
        kt_process_call_with_lambda(ctx, stmt);
        return;
    }
    if (strcmp(kind, "navigation_expression") == 0) {
        kt_eval_navigation_expression_type(ctx, stmt);
        return;
    }
    if (strcmp(kind, "block") == 0) {
        ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
        kt_process_block_stmts(ctx, stmt);
        ctx->current_scope = cbm_scope_pop(ctx->current_scope);
        return;
    }
    if (strcmp(kind, "assignment") == 0) {
        kt_resolve_calls_in_node(ctx, stmt);
        return;
    }
    /* Fallthrough: treat as expression — eval to populate scope, recurse for calls */
    kotlin_eval_expr_type(ctx, stmt);
    kt_resolve_calls_in_node(ctx, stmt);
}

/* Generic walker that fires call resolution on every call_expression in
 * a subtree, *without* descending into nested function/class bodies (those
 * are processed separately with their own scope). */
static void kt_resolve_calls_in_node_inner(KotlinLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node)) {
        return;
    }
    const char *kind = ts_node_type(node);
    if (strcmp(kind, "function_declaration") == 0 || strcmp(kind, "class_declaration") == 0 ||
        strcmp(kind, "object_declaration") == 0 || strcmp(kind, "lambda_literal") == 0 ||
        strcmp(kind, "anonymous_function") == 0) {
        /* Skip — handled elsewhere */
        return;
    }
    if (strcmp(kind, "call_expression") == 0) {
        kt_process_call_with_lambda(ctx, node);
    } else if (strcmp(kind, "navigation_expression") == 0) {
        kt_eval_navigation_expression_type(ctx, node);
    } else if (strcmp(kind, "if_expression") == 0) {
        kt_process_if_expression(ctx, node);
        return;
    } else if (strcmp(kind, "when_expression") == 0) {
        kt_process_when_expression(ctx, node);
        return;
    } else if (strcmp(kind, "for_statement") == 0) {
        kt_process_for_statement(ctx, node);
        return;
    } else if (strcmp(kind, "property_declaration") == 0) {
        kt_bind_property_to_scope(ctx, node);
    }
    uint32_t nc = ts_node_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        kt_resolve_calls_in_node(ctx, ts_node_child(node, i));
    }
}

static void kt_bind_function_params(KotlinLSPContext *ctx, TSNode func_node) {
    TSNode params = kt_field_named(func_node, "parameters");
    if (ts_node_is_null(params)) {
        params = kt_child_kind(func_node, "function_value_parameters");
    }
    if (ts_node_is_null(params)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(params);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode p = ts_node_named_child(params, i);
        if (!kt_node_is(p, "parameter") && !kt_node_is(p, "_lambda_parameter")) {
            continue;
        }
        TSNode name = kt_field_named(p, "name");
        if (ts_node_is_null(name)) {
            name = kt_name_child(p);
        }
        if (ts_node_is_null(name)) {
            name = kt_child_kind_named(p, "simple_identifier");
        }
        if (ts_node_is_null(name)) {
            continue;
        }
        char *pname = kt_node_text(ctx, name);
        if (!pname) {
            continue;
        }
        TSNode type_node = kt_field_named(p, "type");
        if (ts_node_is_null(type_node)) {
            type_node = kt_child_kind(p, "type");
        }
        if (ts_node_is_null(type_node)) {
            type_node = kt_child_kind(p, "user_type");
        }
        if (ts_node_is_null(type_node)) {
            type_node = kt_child_kind(p, "nullable_type");
        }
        const CBMType *t = ts_node_is_null(type_node) ? cbm_type_unknown()
                                                      : kotlin_parse_type_node(ctx, type_node);
        cbm_scope_bind(ctx->current_scope, cbm_arena_strdup(ctx->arena, pname), t);
    }

    /* For extension fun and method: bind `this` */
    if (ctx->this_type) {
        cbm_scope_bind(ctx->current_scope, "this", ctx->this_type);
    }
}

/* ── top-level walking ────────────────────────────────────────────── */

static void kt_process_function_body(KotlinLSPContext *ctx, TSNode func_node, const char *func_qn,
                                     const char *receiver_class_qn) {
    const char *prev_func = ctx->enclosing_func_qn;
    const char *prev_class = ctx->enclosing_class_qn;
    const CBMType *prev_this = ctx->this_type;
    const CBMType *prev_super = ctx->super_type;
    const char *prev_super_qn = ctx->enclosing_super_qn;

    ctx->enclosing_func_qn = func_qn;
    const CBMRegisteredType *receiver_rt = NULL;
    if (receiver_class_qn) {
        ctx->this_type = cbm_type_named(ctx->arena, receiver_class_qn);
        receiver_rt = cbm_registry_lookup_type(ctx->registry, receiver_class_qn);
        if (receiver_rt && receiver_rt->embedded_types && receiver_rt->embedded_types[0]) {
            ctx->enclosing_super_qn = receiver_rt->embedded_types[0];
            ctx->super_type = cbm_type_named(ctx->arena, receiver_rt->embedded_types[0]);
        }
    }

    ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
    kt_bind_function_params(ctx, func_node);

    /* Bind class fields (primary-constructor val/var properties) into the
     * method scope so bare references like `name.uppercase()` inside a
     * method body resolve to the field's type. */
    if (receiver_rt && receiver_rt->field_names && receiver_rt->field_types) {
        for (int i = 0; receiver_rt->field_names[i]; i++) {
            cbm_scope_bind(ctx->current_scope,
                           cbm_arena_strdup(ctx->arena, receiver_rt->field_names[i]),
                           receiver_rt->field_types[i]);
        }
    }

    TSNode body = kt_field_named(func_node, "body");
    if (ts_node_is_null(body)) {
        body = kt_child_kind(func_node, "function_body");
    }
    if (ts_node_is_null(body)) {
        body = kt_child_kind(func_node, "block");
    }
    /* Fallback: some grammar variants emit the single-expression body
     * directly as a child of function_declaration without wrapping it in
     * `function_body`. Scan the named children for the last node that
     * looks like an expression or statement (i.e. not modifiers, name,
     * type parameters, or value parameters). */
    if (ts_node_is_null(body)) {
        uint32_t nc = ts_node_named_child_count(func_node);
        for (uint32_t i = nc; i > 0; i--) {
            TSNode c = ts_node_named_child(func_node, i - 1);
            if (ts_node_is_null(c)) {
                continue;
            }
            const char *k = ts_node_type(c);
            if (strcmp(k, "modifiers") == 0 || strcmp(k, "identifier") == 0 ||
                strcmp(k, "simple_identifier") == 0 ||
                strcmp(k, "function_value_parameters") == 0 || strcmp(k, "type_parameters") == 0 ||
                strcmp(k, "type_constraints") == 0 || strcmp(k, "annotation") == 0) {
                continue;
            }
            /* Skip the return-type node — the return type appears AFTER
             * the parameters but is itself not the body. We identify it
             * heuristically: type-shaped nodes appearing immediately
             * after parameters when there's also a later body candidate.
             * Since we want the LAST candidate, we accept any non-skip
             * kind here — this is the body. */
            body = c;
            break;
        }
    }
    if (!ts_node_is_null(body)) {
        /* Three body shapes:
         *   1. `block` — { ... }, walk via kt_process_block_stmts so local
         *      function declarations are registered before being called.
         *   2. `function_body` — wrapper around either a block or an expr.
         *   3. Bare expression — single-expression body.
         *
         * For (2) and (3) we walk every named child via
         * kt_resolve_calls_in_node, which already handles call_expression /
         * navigation_expression / nested constructs. We deliberately walk
         * ALL children of function_body (not just the first) because the
         * grammar may emit additional siblings under it (e.g. annotation
         * decorations) and we want to be conservative. We also evaluate
         * the body expression to populate any chained types. */
        if (kt_node_is(body, "block")) {
            kt_process_block_stmts(ctx, body);
        } else if (kt_node_is(body, "function_body")) {
            uint32_t nc = ts_node_named_child_count(body);
            for (uint32_t i = 0; i < nc; i++) {
                TSNode c = ts_node_named_child(body, i);
                if (ts_node_is_null(c)) {
                    continue;
                }
                if (kt_node_is(c, "block") || kt_node_is(c, "statements")) {
                    kt_process_block_stmts(ctx, c);
                } else {
                    kt_resolve_calls_in_node(ctx, c);
                    kotlin_eval_expr_type(ctx, c);
                }
            }
        } else {
            kt_resolve_calls_in_node(ctx, body);
            kotlin_eval_expr_type(ctx, body);
        }
    }

    ctx->current_scope = cbm_scope_pop(ctx->current_scope);
    ctx->enclosing_func_qn = prev_func;
    ctx->enclosing_class_qn = prev_class;
    ctx->this_type = prev_this;
    ctx->super_type = prev_super;
    ctx->enclosing_super_qn = prev_super_qn;
}

static void kt_walk_top_level_for_resolution(KotlinLSPContext *ctx, TSNode root,
                                             const char *enclosing_class_qn) {
    if (ts_node_is_null(root)) {
        return;
    }
    uint32_t nc = ts_node_named_child_count(root);
    if (ctx->debug) {
        fprintf(stderr, "[kotlin_lsp] walk_top_level enclosing=%s root_kind=%s nc=%u\n",
                enclosing_class_qn ? enclosing_class_qn : "(top)", ts_node_type(root), nc);
    }
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(root, i);
        const char *kind = ts_node_type(c);
        if (ctx->debug) {
            uint32_t sb = ts_node_start_byte(c);
            uint32_t eb = ts_node_end_byte(c);
            int len = (int)(eb - sb);
            if (len > 50) {
                len = 50;
            }
            fprintf(stderr, "[kotlin_lsp]   child[%u]: %s '%.*s'\n", i, kind, len,
                    ctx->source + sb);
        }
        /* Recurse into ERROR nodes so partial parses (e.g. when the
         * grammar can't parse `interface` at file scope) still get their
         * recoverable function/class bodies resolved. */
        if (strcmp(kind, "ERROR") == 0) {
            kt_walk_top_level_for_resolution(ctx, c, enclosing_class_qn);
            continue;
        }
        if (strcmp(kind, "function_declaration") == 0) {
            TSNode name = kt_field_named(c, "name");
            if (ts_node_is_null(name)) {
                name = kt_name_child(c);
            }
            char *fname = kt_node_text(ctx, name);
            if (!fname) {
                continue;
            }
            /* Detect extension receiver */
            TSNode receiver = kt_field_named(c, "receiver");
            const char *ext_recv_qn = NULL;
            if (!ts_node_is_null(receiver)) {
                char *rt = kt_node_text(ctx, receiver);
                if (rt) {
                    char *lt = strchr(rt, '<');
                    if (lt) {
                        *lt = '\0';
                    }
                    size_t rl = strlen(rt);
                    while (rl > 0 && rt[rl - 1] == '?') {
                        rt[--rl] = '\0';
                    }
                    ext_recv_qn = kotlin_resolve_class_name(ctx, rt);
                }
            }
            char *func_qn = NULL;
            if (enclosing_class_qn) {
                func_qn = kt_join_dot(ctx->arena, enclosing_class_qn, fname);
            } else {
                func_qn = kt_join_dot(ctx->arena, ctx->package_qn, fname);
            }
            const char *recv_qn = enclosing_class_qn ? enclosing_class_qn : ext_recv_qn;
            kt_process_function_body(ctx, c, func_qn, recv_qn);
        } else if (strcmp(kind, "class_declaration") == 0 ||
                   strcmp(kind, "object_declaration") == 0) {
            const char *cls_qn = kt_qn_for_class_decl(ctx, c);
            if (!cls_qn) {
                continue;
            }
            TSNode body = kt_child_kind(c, "class_body");
            if (ts_node_is_null(body)) {
                body = kt_child_kind(c, "enum_class_body");
            }
            const char *prev_class = ctx->enclosing_class_qn;
            ctx->enclosing_class_qn = cls_qn;
            kt_walk_top_level_for_resolution(ctx, body, cls_qn);
            ctx->enclosing_class_qn = prev_class;
        } else if (strcmp(kind, "companion_object") == 0) {
            const char *companion_qn = kt_join_dot(
                ctx->arena, enclosing_class_qn ? enclosing_class_qn : ctx->package_qn, "Companion");
            TSNode body = kt_child_kind(c, "class_body");
            kt_walk_top_level_for_resolution(ctx, body, companion_qn);
        } else if (strcmp(kind, "property_declaration") == 0) {
            /* Top-level property — bind into file scope and resolve calls
             * in initializer (with caller QN = property's getter QN). */
            const char *prev = ctx->enclosing_func_qn;
            char *prop_name = NULL;
            TSNode var = kt_child_kind(c, "variable_declaration");
            if (!ts_node_is_null(var)) {
                TSNode id = kt_name_child(var);
                if (ts_node_is_null(id)) {
                    id = kt_child_kind_named(var, "simple_identifier");
                }
                if (!ts_node_is_null(id)) {
                    prop_name = kt_node_text(ctx, id);
                }
            }
            if (prop_name) {
                ctx->enclosing_func_qn = kt_join_dot(
                    ctx->arena, enclosing_class_qn ? enclosing_class_qn : ctx->package_qn,
                    prop_name);
            }
            kt_bind_property_to_scope(ctx, c);
            kt_resolve_calls_in_node(ctx, c);
            ctx->enclosing_func_qn = prev;
        } else if (strcmp(kind, "secondary_constructor") == 0) {
            const char *prev_func = ctx->enclosing_func_qn;
            ctx->enclosing_func_qn = kt_join_dot(
                ctx->arena, enclosing_class_qn ? enclosing_class_qn : ctx->package_qn, "<init>");
            ctx->current_scope = cbm_scope_push(ctx->arena, ctx->current_scope);
            kt_bind_function_params(ctx, c);
            TSNode body = kt_child_kind(c, "block");
            if (!ts_node_is_null(body)) {
                kt_process_block_stmts(ctx, body);
            }
            ctx->current_scope = cbm_scope_pop(ctx->current_scope);
            ctx->enclosing_func_qn = prev_func;
        } else if (strcmp(kind, "anonymous_initializer") == 0) {
            const char *prev_func = ctx->enclosing_func_qn;
            ctx->enclosing_func_qn = kt_join_dot(
                ctx->arena, enclosing_class_qn ? enclosing_class_qn : ctx->package_qn, "<init>");
            kt_resolve_calls_in_node(ctx, c);
            ctx->enclosing_func_qn = prev_func;
        }
    }
}

/* Debug AST dumper — only active when CBM_LSP_KOTLIN_AST=1 in env. */
static void kt_debug_dump_ast(TSNode node, const char *src, int depth) {
    if (depth > 8) {
        return;
    }
    const char *kind = ts_node_type(node);
    uint32_t sb = ts_node_start_byte(node);
    uint32_t eb = ts_node_end_byte(node);
    int len = (int)(eb - sb);
    if (len > 40) {
        len = 40;
    }
    fprintf(stderr, "%*s[%s] %.*s%s\n", depth * 2, "", kind, len, src + sb,
            (int)(eb - sb) > 40 ? "…" : "");
    uint32_t nc = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < nc; i++) {
        kt_debug_dump_ast(ts_node_named_child(node, i), src, depth + 1);
    }
}

void kotlin_lsp_process_file(KotlinLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root)) {
        return;
    }
    if (getenv("CBM_LSP_KOTLIN_AST")) {
        fprintf(stderr, "=== AST for %s ===\n", ctx->rel_path ? ctx->rel_path : "<unknown>");
        kt_debug_dump_ast(root, ctx->source, 0);
        fprintf(stderr, "=== END AST ===\n");
    }

    /* 1. Package + imports */
    uint32_t nc = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < nc; i++) {
        TSNode c = ts_node_named_child(root, i);
        const char *kind = ts_node_type(c);
        if (strcmp(kind, "package_header") == 0) {
            const char *pkg = kt_parse_package_header(ctx, c);
            if (pkg && *pkg) {
                ctx->package_qn = pkg;
            }
        } else if (strcmp(kind, "import_list") == 0) {
            /* Newer tree-sitter-kotlin wraps imports in an import_list node. */
            uint32_t inc = ts_node_named_child_count(c);
            for (uint32_t j = 0; j < inc; j++) {
                kt_parse_import_directive(ctx, ts_node_named_child(c, j));
            }
        } else if (strcmp(kind, "import") == 0 || strcmp(kind, "import_directive") == 0 ||
                   strcmp(kind, "import_header") == 0) {
            kt_parse_import_directive(ctx, c);
        }
    }

    /* 2. Collect top-level definitions for intra-file resolution. */
    kt_collect_top_level_decls(ctx, root);

    /* 3. Walk for call resolution. */
    kt_walk_top_level_for_resolution(ctx, root, NULL);
}

static const CBMType *kt_try_smart_cast(KotlinLSPContext *ctx, TSNode call_or_nav) {
    /* Currently a stub — smart-cast is applied during if/when traversal. */
    (void)ctx;
    (void)call_or_nav;
    return NULL;
}

/* ── public entry point used by cbm.c dispatcher ──────────────────── */

/* Tree-sitter handle for re-parsing repaired sources. */
extern const TSLanguage *tree_sitter_kotlin(void);

/* Detect bodyless interface methods (`fun foo()` without `: ReturnType`
 * and without `{ … }`) inside `interface X { … }` bodies — the vendored
 * tree-sitter-kotlin grammar produces an `ERROR` node for these and
 * loses the rest of the file. We patch the source by inserting `: Unit`
 * right after the closing paren of each affected method, then re-parse
 * with tree-sitter.
 *
 * Heuristic and intentionally permissive: walk the source byte-by-byte,
 * track whether we're inside an `interface ... { ... }` block (depth
 * counter on '{' / '}') and, while inside, look for `fun ` followed by
 * an identifier, optional whitespace, '(', balanced parens, then check
 * what follows — if it's whitespace + (',' | ';' | '\n' | '}' ) without
 * an intervening ':' or '=' or '{', insert `: Unit`.
 *
 * Returns NULL when no patches were applied (caller keeps original
 * source). Otherwise the returned arena-allocated buffer is a complete
 * patched source. */
static char *kt_repair_bodyless_interface_methods(CBMArena *arena, const char *src, int src_len,
                                                  int *out_len) {
    if (!src || src_len <= 0) {
        return NULL;
    }
    /* Cheap early-out: skip when neither `interface` nor a function-type
     * with a receiver (`X.() ->`) appears anywhere — those are the two
     * grammar pain points this pass repairs. */
    if (!strstr(src, "interface") && !strstr(src, ".() ->") && !strstr(src, ".()->")) {
        return NULL;
    }

    /* Two-pass: first scan to find insertion points, then build.
     * Each fix has an offset (insert BEFORE this src index) and an
     * insertion string. The insertion strings are static const so they
     * don't need to be arena-copied. */
    enum {
        MAX_FIXES = 64,
    };
    typedef struct {
        int offset;
        const char *text;
        int text_len;
    } kt_fix_t;
    kt_fix_t fixes[MAX_FIXES];
    int fix_count = 0;
    static const char insert_unit[] = ": Unit";
    static const int insert_unit_len = (int)(sizeof(insert_unit) - 1);
    static const char insert_parens[] = "()";
    static const int insert_parens_len = (int)(sizeof(insert_parens) - 1);

    const char *p = src;
    const char *end = src + src_len;
    int iface_depth = 0;    /* nesting of `interface ... { ... }` blocks */
    int brace_depth = 0;    /* total braces (so we can subtract on '}') */
    int iface_brace_at[16]; /* brace_depth at which each interface opened */
    int iface_stack_n = 0;

    while (p < end) {
        char c = *p;
        /* Skip line comments */
        if (c == '/' && p + 1 < end && p[1] == '/') {
            while (p < end && *p != '\n') {
                p++;
            }
            continue;
        }
        /* Skip block comments */
        if (c == '/' && p + 1 < end && p[1] == '*') {
            p += 2;
            while (p + 1 < end && !(*p == '*' && p[1] == '/')) {
                p++;
            }
            if (p + 1 < end) {
                p += 2;
            } else {
                p = end;
            }
            continue;
        }
        /* Skip string literals */
        if (c == '"') {
            p++;
            while (p < end && *p != '"') {
                if (*p == '\\' && p + 1 < end) {
                    p += 2;
                } else {
                    p++;
                }
            }
            if (p < end) {
                p++;
            }
            continue;
        }

        /* Detect `interface ` keyword at word boundary */
        if (c == 'i' && p + 9 < end && strncmp(p, "interface", 9) == 0 &&
            (p == src || !isalnum((unsigned char)p[-1])) &&
            (isspace((unsigned char)p[9]) || p[9] == '\n')) {
            /* Skip ahead to opening brace of interface body */
            const char *q = p + 9;
            while (q < end && *q != '{' && *q != ';' && *q != '\n') {
                q++;
            }
            if (q < end && *q == '{') {
                if (iface_stack_n < 16) {
                    iface_brace_at[iface_stack_n++] = brace_depth;
                }
                iface_depth++;
                brace_depth++;
                p = q + 1;
                continue;
            }
            p = q;
            continue;
        }

        if (c == '{') {
            brace_depth++;
            p++;
            continue;
        }
        if (c == '}') {
            brace_depth--;
            if (iface_stack_n > 0 && iface_brace_at[iface_stack_n - 1] == brace_depth) {
                iface_stack_n--;
                iface_depth--;
            }
            p++;
            continue;
        }

        /* Inside an interface body, detect `fun NAME(...)` followed by
         * end-of-statement without a return type or body. */
        if (iface_depth > 0 && c == 'f' && p + 3 < end && strncmp(p, "fun", 3) == 0 &&
            (p == src || !isalnum((unsigned char)p[-1])) &&
            (isspace((unsigned char)p[3]) || p[3] == '\n')) {
            const char *q = p + 3;
            while (q < end && (isspace((unsigned char)*q) || *q == '\n')) {
                q++;
            }
            /* Identifier */
            while (q < end && (isalnum((unsigned char)*q) || *q == '_' || *q == '`')) {
                q++;
            }
            /* Optional generic type parameters */
            if (q < end && *q == '<') {
                int g = 1;
                q++;
                while (q < end && g > 0) {
                    if (*q == '<') {
                        g++;
                    } else if (*q == '>') {
                        g--;
                    }
                    q++;
                }
            }
            /* Optional whitespace, then '(' */
            while (q < end && (isspace((unsigned char)*q) || *q == '\n')) {
                q++;
            }
            if (q >= end || *q != '(') {
                p++;
                continue;
            }
            int paren = 1;
            q++;
            while (q < end && paren > 0) {
                if (*q == '(') {
                    paren++;
                } else if (*q == ')') {
                    paren--;
                }
                q++;
            }
            /* q is now positioned just past the ')'. */
            int paren_close = (int)(q - src) - 1;
            /* Skip whitespace */
            const char *r = q;
            while (r < end && (*r == ' ' || *r == '\t')) {
                r++;
            }
            /* Check what follows */
            if (r < end && (*r == ':' || *r == '=' || *r == '{')) {
                /* Has return type, body, or expression — fine. */
                p = r;
                continue;
            }
            /* Bodyless / no return type — insert `: Unit` after the ')'. */
            if (fix_count < MAX_FIXES) {
                fixes[fix_count].offset = paren_close + 1;
                fixes[fix_count].text = insert_unit;
                fixes[fix_count].text_len = insert_unit_len;
                fix_count++;
            }
            p = r;
            continue;
        }
        p++;
    }

    /* Second pass: strip the receiver-with-dot from `<Type>.() -> X`
     * function-type patterns. The vendored grammar can't parse the
     * receiver-style function type at file scope, so removing the
     * `<Type>.` prefix preserves parseability while losing only the
     * receiver-binding hint (which we recover separately via
     * decorator_qns at registration time). */
    typedef struct {
        int start; /* inclusive */
        int end;   /* exclusive */
    } cut_range_t;
    enum { MAX_CUTS = 64 };
    cut_range_t cuts[MAX_CUTS];
    int cut_count = 0;
    {
        const char *p2 = src;
        const char *end2 = src + src_len;
        while (p2 < end2 - 4) {
            /* Skip strings/comments to keep things simple */
            if (*p2 == '"') {
                p2++;
                while (p2 < end2 && *p2 != '"') {
                    if (*p2 == '\\' && p2 + 1 < end2) {
                        p2 += 2;
                    } else {
                        p2++;
                    }
                }
                if (p2 < end2) {
                    p2++;
                }
                continue;
            }
            if (*p2 == '/' && p2 + 1 < end2 && p2[1] == '/') {
                while (p2 < end2 && *p2 != '\n') {
                    p2++;
                }
                continue;
            }
            if (*p2 == '/' && p2 + 1 < end2 && p2[1] == '*') {
                p2 += 2;
                while (p2 + 1 < end2 && !(*p2 == '*' && p2[1] == '/')) {
                    p2++;
                }
                p2 += (p2 + 1 < end2 ? 2 : 1);
                continue;
            }
            /* Look for `.()` */
            if (*p2 == '.' && p2 + 2 < end2 && p2[1] == '(' && p2[2] == ')') {
                /* Walk backwards to find the identifier preceding '.' */
                const char *q = p2 - 1;
                while (q > src && (*q == ' ' || *q == '\t')) {
                    q--;
                }
                /* q now points at the last char of the identifier (or
                 * a non-identifier character). */
                const char *id_end = q + 1;
                while (q >= src && (isalnum((unsigned char)*q) || *q == '_' || *q == '`')) {
                    q--;
                }
                const char *id_start = q + 1;
                if (id_start >= id_end) {
                    p2++;
                    continue;
                }
                /* Look ahead past `.()` for ` -> ` or `->` to confirm
                 * this is a function-type receiver and not a method
                 * reference. */
                const char *r = p2 + 3;
                while (r < end2 && (*r == ' ' || *r == '\t')) {
                    r++;
                }
                if (r + 1 >= end2 || r[0] != '-' || r[1] != '>') {
                    p2++;
                    continue;
                }
                /* Cut range: from id_start to (p2+1), removing the
                 * "<Name>." prefix and leaving "()". */
                if (cut_count < MAX_CUTS) {
                    cuts[cut_count].start = (int)(id_start - src);
                    cuts[cut_count].end = (int)(p2 + 1 - src);
                    cut_count++;
                }
                p2 += 3;
                continue;
            }
            p2++;
        }
    }

    /* Third pass: add `()` after `: <Type>` in class delegation when
     * the next token is `{` (interface inheritance). The vendored
     * tree-sitter-kotlin grammar treats `class X : Y { ... }` as a
     * parse error and produces an ERROR node swallowing the rest of
     * the file; rewriting to `class X : Y() { ... }` keeps the parse
     * intact (even though, for interface inheritance, parens are
     * non-idiomatic Kotlin). We only insert when no parens already
     * follow the type. */
    {
        const char *p3 = src;
        const char *end3 = src + src_len;
        while (p3 < end3) {
            if (*p3 == '"') {
                p3++;
                while (p3 < end3 && *p3 != '"') {
                    if (*p3 == '\\' && p3 + 1 < end3) {
                        p3 += 2;
                    } else {
                        p3++;
                    }
                }
                if (p3 < end3) {
                    p3++;
                }
                continue;
            }
            if (*p3 == '/' && p3 + 1 < end3 && p3[1] == '/') {
                while (p3 < end3 && *p3 != '\n') {
                    p3++;
                }
                continue;
            }
            if (*p3 == '/' && p3 + 1 < end3 && p3[1] == '*') {
                p3 += 2;
                while (p3 + 1 < end3 && !(*p3 == '*' && p3[1] == '/')) {
                    p3++;
                }
                p3 += (p3 + 1 < end3 ? 2 : 1);
                continue;
            }
            if (*p3 == ':' && p3 + 1 < end3 && (p3[1] == ' ' || p3[1] == '\t' || p3[1] == '\n')) {
                /* Walk back: ensure preceded by `class IDENT[(params)]?`. */
                /* Walk forward: skip ws, read identifier (possibly dotted),
                 * skip generics, then check for parens or `{`. */
                const char *q = p3 + 1;
                while (q < end3 && (*q == ' ' || *q == '\t' || *q == '\n')) {
                    q++;
                }
                /* dotted identifier */
                while (q < end3 && (isalnum((unsigned char)*q) || *q == '_' || *q == '.')) {
                    q++;
                }
                /* generics? */
                if (q < end3 && *q == '<') {
                    int g = 1;
                    q++;
                    while (q < end3 && g > 0) {
                        if (*q == '<') {
                            g++;
                        } else if (*q == '>') {
                            g--;
                        }
                        q++;
                    }
                }
                /* Skip ws */
                while (q < end3 && (*q == ' ' || *q == '\t' || *q == '\n')) {
                    q++;
                }
                if (q >= end3) {
                    p3++;
                    continue;
                }
                if (*q == '(') {
                    /* Already has parens. */
                    p3 = q;
                    continue;
                }
                if (*q != '{') {
                    p3++;
                    continue;
                }
                /* Walk back to verify this is class inheritance: look for
                 * "class " keyword on the same logical line before `:`. */
                const char *back = p3 - 1;
                while (back > src && *back != '\n' && back > p3 - 200) {
                    back--;
                }
                if (back <= src || back < p3 - 200) {
                    p3++;
                    continue;
                }
                if (!strstr(back, "class ") && !strstr(back, "object ")) {
                    p3++;
                    continue;
                }
                /* Insert `()` right before `{` (which is at position q). */
                if (fix_count < MAX_FIXES) {
                    fixes[fix_count].offset = (int)(q - src);
                    fixes[fix_count].text = insert_parens;
                    fixes[fix_count].text_len = insert_parens_len;
                    fix_count++;
                }
                p3 = q;
                continue;
            }
            p3++;
        }
    }

    if (fix_count == 0 && cut_count == 0) {
        return NULL;
    }

    /* Sort cuts by start offset (small N — bubble sort is fine). */
    for (int i = 0; i < cut_count - 1; i++) {
        for (int j = i + 1; j < cut_count; j++) {
            if (cuts[j].start < cuts[i].start) {
                cut_range_t tmp = cuts[i];
                cuts[i] = cuts[j];
                cuts[j] = tmp;
            }
        }
    }

    /* Sort fixes by offset (different passes may interleave). */
    for (int i = 0; i < fix_count - 1; i++) {
        for (int j = i + 1; j < fix_count; j++) {
            if (fixes[j].offset < fixes[i].offset) {
                kt_fix_t tmp = fixes[i];
                fixes[i] = fixes[j];
                fixes[j] = tmp;
            }
        }
    }

    /* Build patched source. Cuts remove a [start, end) range. */
    int new_len = src_len;
    for (int i = 0; i < fix_count; i++) {
        new_len += fixes[i].text_len;
    }
    for (int i = 0; i < cut_count; i++) {
        new_len -= (cuts[i].end - cuts[i].start);
    }
    char *out = (char *)cbm_arena_alloc(arena, (size_t)new_len + 1);
    if (!out) {
        return NULL;
    }
    int oi = 0;
    int next_fix = 0;
    int next_cut = 0;
    for (int i = 0; i <= src_len; i++) {
        while (next_fix < fix_count && fixes[next_fix].offset == i) {
            memcpy(out + oi, fixes[next_fix].text, (size_t)fixes[next_fix].text_len);
            oi += fixes[next_fix].text_len;
            next_fix++;
        }
        /* Skip range [start, end) when cutting */
        if (next_cut < cut_count && cuts[next_cut].start == i) {
            i = cuts[next_cut].end - 1; /* loop's i++ moves past `end-1` */
            next_cut++;
            continue;
        }
        if (i < src_len) {
            out[oi++] = src[i];
        }
    }
    out[oi] = '\0';
    if (out_len) {
        *out_len = oi;
    }
    return out;
}

void cbm_run_kotlin_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                        TSNode root) {
    if (!arena || !result || !source || ts_node_is_null(root)) {
        return;
    }

    /* Repair pass: if the source uses interface declarations with
     * bodyless `fun X()` methods or function-types with receivers
     * (`Foo.() -> X`), our vendored tree-sitter grammar produces
     * ERROR nodes and loses parts of the file. Patch the source and
     * re-parse. */
    int patched_len = 0;
    char *patched_src =
        kt_repair_bodyless_interface_methods(arena, source, source_len, &patched_len);
    TSTree *patched_tree = NULL;
    TSNode use_root = root;
    const char *use_source = source;
    int use_source_len = source_len;
    bool debug = (getenv("CBM_LSP_DEBUG") != NULL);
    if (debug && patched_src) {
        fprintf(stderr, "[kotlin_lsp] preprocessed %d → %d bytes\n", source_len, patched_len);
        fprintf(stderr, "[kotlin_lsp] patched source:\n%s\n[end patched]\n", patched_src);
    } else if (debug) {
        fprintf(stderr, "[kotlin_lsp] no preprocessing applied (no interface or .() patterns)\n");
    }
    if (patched_src) {
        TSParser *parser = ts_parser_new();
        if (parser) {
            const TSLanguage *lang = tree_sitter_kotlin();
            if (debug) {
                fprintf(stderr, "[kotlin_lsp] tree_sitter_kotlin lang=%p\n", (void *)lang);
            }
            ts_parser_set_language(parser, lang);
            patched_tree = ts_parser_parse_string(parser, NULL, patched_src, (uint32_t)patched_len);
            ts_parser_delete(parser);
            if (debug) {
                fprintf(stderr, "[kotlin_lsp] re-parse result tree=%p\n", (void *)patched_tree);
            }
            if (patched_tree) {
                use_root = ts_tree_root_node(patched_tree);
                use_source = patched_src;
                use_source_len = patched_len;
            }
        }
    }

    /* Build per-file registry. */
    CBMTypeRegistry registry;
    cbm_registry_init(&registry, arena);

    /* Curated stdlib */
    cbm_kotlin_stdlib_register(&registry, arena);

    /* Compute project name + package_qn from result->module_qn (which is
     * "<project>.<rel.path.parts>"). The Kotlin convention places the
     * file class as "<project>.<package>" — or "<project>.<rel-path>" for
     * the module_qn. We honour module_qn as-is and strip the trailing
     * filename to derive the package_qn at the FS-path level — but for
     * cross-file resolution we additionally need the dotted package
     * declared in the source. The kotlin_lsp_process_file pass updates
     * package_qn from the actual `package_header` node when present.
     */
    const char *project_name = "";
    const char *module_qn = result->module_qn ? result->module_qn : "";
    const char *first_dot = strchr(module_qn, '.');
    if (first_dot) {
        size_t pl = (size_t)(first_dot - module_qn);
        char *pn = (char *)cbm_arena_alloc(arena, pl + 1);
        if (pn) {
            memcpy(pn, module_qn, pl);
            pn[pl] = '\0';
            project_name = pn;
        }
    } else {
        project_name = module_qn;
    }

    /* Initial package_qn is the FS-path module_qn ("<project>.<rel.path>"),
     * matching the textual extractor's QN prefix so the LSP's caller_qn equals
     * the call site's enclosing_func_qn (the join keys on an exact caller_qn
     * match). A source `package_header`, when present, overrides this in
     * kotlin_lsp_process_file for cross-file import resolution. */
    KotlinLSPContext ctx;
    kotlin_lsp_init(&ctx, arena, use_source, use_source_len, &registry, module_qn, module_qn,
                    project_name, /*rel_path=*/NULL, &result->resolved_calls);

    kotlin_lsp_process_file(&ctx, use_root);

    /* Inject kotlin.Any universal-method nodes (toString/equals/hashCode) so the
     * lsp_kt_any fallback emitted above has a target node to form a CALLS edge. */
    kt_builtins_inject_defs(result, arena);

    if (patched_tree) {
        ts_tree_delete(patched_tree);
    }
}

/* ── Cross-file LSP ───────────────────────────────────────────────── */

/* Register one cross-file definition into the registry under its graph QN so
 * a call site in another file resolves to the right node. Types and functions
 * keep their full project-qualified QN; functions carry receiver_type so the
 * sole-definer fallback can tell a top-level fun from a method. */
static void kt_register_cross_def(CBMTypeRegistry *reg, CBMArena *arena, const CBMLSPDef *d) {
    if (!d->qualified_name || !d->short_name || !d->label) {
        return;
    }
    if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0 ||
        strcmp(d->label, "Enum") == 0 || strcmp(d->label, "Type") == 0) {
        CBMRegisteredType rt;
        memset(&rt, 0, sizeof(rt));
        rt.qualified_name = d->qualified_name;
        rt.short_name = d->short_name;
        rt.is_interface = (strcmp(d->label, "Interface") == 0) || d->is_interface;
        if (d->embedded_types && d->embedded_types[0]) {
            int n = 1;
            for (const char *p = d->embedded_types; *p; p++) {
                if (*p == '|') {
                    n++;
                }
            }
            const char **emb =
                (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(*emb));
            int idx = 0;
            const char *start = d->embedded_types;
            while (*start) {
                const char *end = start;
                while (*end && *end != '|') {
                    end++;
                }
                if (end > start) {
                    emb[idx++] = cbm_arena_strndup(arena, start, (size_t)(end - start));
                }
                if (!*end) {
                    break;
                }
                start = end + 1;
            }
            emb[idx] = NULL;
            rt.embedded_types = emb;
        }
        cbm_registry_add_type(reg, rt);
    } else if (strcmp(d->label, "Method") == 0 || strcmp(d->label, "Function") == 0 ||
               strcmp(d->label, "Constructor") == 0) {
        CBMRegisteredFunc rf;
        memset(&rf, 0, sizeof(rf));
        rf.qualified_name = d->qualified_name;
        rf.short_name = d->short_name;
        rf.min_params = -1;
        /* receiver_type distinguishes a top-level fun (NULL) from a method
         * (set) — the sole-definer fallback only matches top-level funs. */
        rf.receiver_type = d->receiver_type;
        cbm_registry_add_func(reg, rf);
    }
}

void cbm_run_kotlin_lsp_cross(CBMArena *arena, const char *source, int source_len,
                              const char *module_qn, CBMLSPDef *defs, int def_count,
                              const char **import_names, const char **import_qns, int import_count,
                              TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!arena || !source) {
        return;
    }

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_kotlin_stdlib_register(&reg, arena);

    /* Register project-wide defs (local + cross-file) under their graph QNs. */
    for (int i = 0; i < def_count; i++) {
        kt_register_cross_def(&reg, arena, &defs[i]);
    }

    /* Build the hash indexes: without this every registry lookup in the walk
     * is a LINEAR scan over the whole cross registry — O(lookups x defs) per
     * file (same class as the java_lsp/elasticsearch slowdown). Indexes live
     * in a per-call scratch arena (reg's arena is pipeline-lifetime). */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    cbm_registry_finalize_into(&reg, &idx_arena);

    /* Parse the source if the pipeline didn't hand us a cached tree. */
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        if (!parser) {
            cbm_arena_destroy(&idx_arena);
            return;
        }
        ts_parser_set_language(parser, tree_sitter_kotlin());
        tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)source_len);
        ts_parser_delete(parser);
        owns_tree = true;
    }
    if (!tree) {
        cbm_arena_destroy(&idx_arena);
        return;
    }
    TSNode root = ts_tree_root_node(tree);

    /* project_name prefix (everything before the first dot of module_qn). */
    const char *project_name = "";
    const char *first_dot = module_qn ? strchr(module_qn, '.') : NULL;
    if (first_dot) {
        size_t pl = (size_t)(first_dot - module_qn);
        char *pn = (char *)cbm_arena_alloc(arena, pl + 1);
        if (pn) {
            memcpy(pn, module_qn, pl);
            pn[pl] = '\0';
            project_name = pn;
        }
    } else if (module_qn) {
        project_name = module_qn;
    }

    KotlinLSPContext ctx;
    kotlin_lsp_init(&ctx, arena, source, source_len, &reg, "", module_qn ? module_qn : "",
                    project_name, /*rel_path=*/NULL, out);

    /* Apply caller-supplied imports (resolved IMPORTS edges). */
    for (int i = 0; i < import_count; i++) {
        if (!import_names || !import_qns || !import_names[i] || !import_qns[i]) {
            continue;
        }
        kotlin_lsp_add_import(&ctx, import_names[i], import_qns[i], CBM_KT_USE_UNKNOWN);
    }

    kotlin_lsp_process_file(&ctx, root);
    cbm_arena_destroy(&idx_arena);

    if (owns_tree) {
        ts_tree_delete(tree);
    }
}
