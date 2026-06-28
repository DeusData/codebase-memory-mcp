/*
 * java_lsp.c — Pure-C Java semantic resolver.
 *
 * Reverse-engineered from JLS §6 (Names) / §15 (Expressions) and the
 * resolution shape used by Eclipse JDT-LS + java-language-server (which both
 * lean on javac via com.sun.source). The aim: ≥90% parity with what those
 * servers expose for textDocument/definition + textDocument/references at
 * call sites — *without* JVM/javac dependencies.
 *
 * Big picture:
 *   - Tree-sitter parses Java into an AST.
 *   - We populate a CBMTypeRegistry from this file's CBMDefinitions plus a
 *     hand-curated stdlib (java.lang.*, java.util.*, java.io.*, ...).
 *   - We walk the AST once, building a JLS-style scope chain (fields →
 *     params → locals → blocks). For every method_invocation,
 *     object_creation_expression, and field_access we call java_eval_expr_type
 *     on the receiver and look up the call by name + arity in the registry.
 *   - Best-overload resolution prefers exact arg count; ties broken by type
 *     compatibility (cbm_registry_lookup_method_by_types). Ambiguity ⇒
 *     "lsp_unresolved" diagnostic so downstream knows to drop the edge.
 *   - Inheritance + interface chains are walked exactly the way JLS §8.4.8.4
 *     specifies: most-specific override first, then walk supers.
 *
 * Confidence policy (consumed by lsp_resolve.h with CBM_LSP_CONFIDENCE_FLOOR
 * = 0.6):
 *   - 0.95 — direct method call (callee unambiguous, receiver type known)
 *   - 0.92 — static method via classname / static import
 *   - 0.90 — inherited method (matched up the super-chain)
 *   - 0.85 — interface dispatch (sole impl) / generic type substitution
 *   - 0.80 — receiver type came from a phpdoc-style fallback (not used here)
 *   - 0.0  — diagnostic (won't be promoted to an edge)
 */

#include "java_lsp.h"
#include "../helpers.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define JAVA_LSP_MAX_EVAL_DEPTH 32
#define JAVA_LSP_MAX_STMT_DEPTH 256
#define JAVA_LSP_MAX_WALK_DEPTH 512
#define JAVA_LSP_MAX_INHERIT_HOPS 32
#define JAVA_LSP_MAX_OVERLOADS 64
#define JAVA_LSP_BUF 1024

/* Forward declarations ─────────────────────────────────────────────── */

static void java_resolve_calls_in_node(JavaLSPContext *ctx, TSNode node);
static void java_process_class_decl(JavaLSPContext *ctx, TSNode node);
static void process_method_decl(JavaLSPContext *ctx, TSNode node, const char *class_qn,
                                const char *super_qn);
static void process_constructor_decl(JavaLSPContext *ctx, TSNode node, const char *class_qn,
                                     const char *super_qn);
static void process_field_decl(JavaLSPContext *ctx, TSNode node);
static void process_local_var_decl(JavaLSPContext *ctx, TSNode node);
static void process_enhanced_for(JavaLSPContext *ctx, TSNode node);
static void process_block(JavaLSPContext *ctx, TSNode node);
static void java_emit_resolved(JavaLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence);
static void java_emit_unresolved(JavaLSPContext *ctx, const char *expr_text, const char *reason);
static const CBMType *eval_method_invocation(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_object_creation(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_field_access(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_array_access(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_cast(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_ternary(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_binary(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_unary(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_lambda(JavaLSPContext *ctx, TSNode node);
static const CBMType *eval_method_reference(JavaLSPContext *ctx, TSNode node);
static const CBMType *resolve_identifier_type(JavaLSPContext *ctx, const char *name);
static const CBMType *resolve_member_type(JavaLSPContext *ctx, const CBMType *recv_type,
                                          const char *member_name);
static void resolve_method_call(JavaLSPContext *ctx, TSNode call);
static void register_local_func_or_type_from_file(JavaLSPContext *ctx, CBMTypeRegistry *reg,
                                                  CBMFileResult *result);
static const char *strip_generics(CBMArena *a, const char *type_text);
static const char *unwrap_array_text(CBMArena *a, const char *type_text, int *out_array_dim);
static char *java_node_text(JavaLSPContext *ctx, TSNode node);
static bool is_node_kind(TSNode node, const char *kind);
static TSNode child_by_kind(TSNode parent, const char *kind);
static const CBMType *box_primitive(CBMArena *a, const char *prim);
static int count_call_args(TSNode call_node);
static const CBMType *propagate_template(CBMArena *a, const char *recv_qn, const char *method_name,
                                         const CBMType *const *recv_targs, int recv_targ_count,
                                         const CBMType *return_t);
static bool method_implies_lambda_args(const char *recv_qn, const char *method_name,
                                       const CBMType *const *targs, int targ_count, int *out_arity,
                                       const CBMType **out_param0, const CBMType **out_param1);
static void resolve_method_reference(JavaLSPContext *ctx, TSNode mref,
                                     const CBMRegisteredFunc *outer_resolved, int arg_index,
                                     const CBMType *recv_type);
static bool is_map_like(const char *qn);

/* ── Built-in primitive table ─────────────────────────────────────── */

/* Common Java packages we try as fallback when an unqualified type can't
 * be resolved via java.lang / parent_class / module_qn. Order matters —
 * more-popular packages first to keep the first-match heuristic accurate. */
static const char *JAVA_FALLBACK_PACKAGES[] = {
    "java.util",
    "java.util.function",
    "java.util.stream",
    "java.util.concurrent",
    "java.util.concurrent.atomic",
    "java.util.concurrent.locks",
    "java.io",
    "java.nio.file",
    "java.time",
    "java.util.regex",
    "java.net",
    NULL,
};

static const char *JAVA_PRIMITIVES[] = {"boolean", "byte",  "char",   "short", "int",
                                        "long",    "float", "double", "void",  NULL};

static const char *JAVA_BOXED[] = {"java.lang.Boolean",   "java.lang.Byte",
                                   "java.lang.Character", "java.lang.Short",
                                   "java.lang.Integer",   "java.lang.Long",
                                   "java.lang.Float",     "java.lang.Double",
                                   "java.lang.Void",      NULL};

static bool is_java_primitive(const char *name) {
    if (!name)
        return false;
    for (int i = 0; JAVA_PRIMITIVES[i]; i++) {
        if (strcmp(name, JAVA_PRIMITIVES[i]) == 0)
            return true;
    }
    return false;
}

/* Auto-imported java.lang single-type names. The JLS §7.5.5 says all classes
 * in java.lang are imported on demand into every compilation unit. */
static const char *JAVA_LANG_TYPES[] = {"Object",
                                        "String",
                                        "StringBuilder",
                                        "StringBuffer",
                                        "CharSequence",
                                        "Boolean",
                                        "Byte",
                                        "Character",
                                        "Short",
                                        "Integer",
                                        "Long",
                                        "Float",
                                        "Double",
                                        "Number",
                                        "Math",
                                        "System",
                                        "Thread",
                                        "Runnable",
                                        "Iterable",
                                        "Comparable",
                                        "Cloneable",
                                        "Class",
                                        "ClassLoader",
                                        "Throwable",
                                        "Exception",
                                        "Error",
                                        "RuntimeException",
                                        "NullPointerException",
                                        "IllegalArgumentException",
                                        "IllegalStateException",
                                        "IndexOutOfBoundsException",
                                        "ArrayIndexOutOfBoundsException",
                                        "ArithmeticException",
                                        "ClassCastException",
                                        "ClassNotFoundException",
                                        "NumberFormatException",
                                        "UnsupportedOperationException",
                                        "Enum",
                                        "Record",
                                        "AutoCloseable",
                                        "Process",
                                        "ProcessBuilder",
                                        "StackTraceElement",
                                        "Override",
                                        "Deprecated",
                                        "SuppressWarnings",
                                        "FunctionalInterface",
                                        "Void",
                                        NULL};

/* ── Helpers ──────────────────────────────────────────────────────── */

static char *java_node_text(JavaLSPContext *ctx, TSNode node) {
    return cbm_node_text(ctx->arena, node, ctx->source);
}

static bool is_node_kind(TSNode node, const char *kind) {
    if (ts_node_is_null(node))
        return false;
    return strcmp(ts_node_type(node), kind) == 0;
}

static TSNode child_by_kind(TSNode parent, const char *kind) {
    if (ts_node_is_null(parent))
        return parent;
    uint32_t n = ts_node_named_child_count(parent);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(parent, i);
        if (strcmp(ts_node_type(c), kind) == 0)
            return c;
    }
    TSNode null_node;
    memset(&null_node, 0, sizeof(null_node));
    return null_node;
}

static int count_call_args(TSNode call_node) {
    if (ts_node_is_null(call_node))
        return 0;
    TSNode args = ts_node_child_by_field_name(call_node, "arguments", 9);
    if (ts_node_is_null(args))
        return 0;
    return (int)ts_node_named_child_count(args);
}

/* Strip generic parameters from a type text (List<String> → List). */
static const char *strip_generics(CBMArena *a, const char *type_text) {
    if (!type_text)
        return NULL;
    const char *lt = strchr(type_text, '<');
    if (!lt)
        return type_text;
    return cbm_arena_strndup(a, type_text, (size_t)(lt - type_text));
}

/* Strip array suffix (`int[]` → `int`, `String[][]` → `String`, dim=2). */
static const char *unwrap_array_text(CBMArena *a, const char *type_text, int *out_dim) {
    if (out_dim)
        *out_dim = 0;
    if (!type_text)
        return NULL;
    size_t n = strlen(type_text);
    int dim = 0;
    while (n >= 2 && type_text[n - 1] == ']' && type_text[n - 2] == '[') {
        n -= 2;
        dim++;
    }
    if (out_dim)
        *out_dim = dim;
    if (dim == 0)
        return type_text;
    return cbm_arena_strndup(a, type_text, n);
}

/* Map primitive name → boxed wrapper QN. */
static const CBMType *box_primitive(CBMArena *a, const char *prim) {
    if (!prim)
        return cbm_type_unknown();
    static const char *map[][2] = {
        {"boolean", "java.lang.Boolean"}, {"byte", "java.lang.Byte"},
        {"char", "java.lang.Character"},  {"short", "java.lang.Short"},
        {"int", "java.lang.Integer"},     {"long", "java.lang.Long"},
        {"float", "java.lang.Float"},     {"double", "java.lang.Double"},
        {"void", "java.lang.Void"},
    };
    for (size_t i = 0; i < sizeof(map) / sizeof(map[0]); i++) {
        if (strcmp(prim, map[i][0]) == 0)
            return cbm_type_named(a, map[i][1]);
    }
    return cbm_type_unknown();
}

/* ── Initialization ───────────────────────────────────────────────── */

void java_lsp_init(JavaLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                   const CBMTypeRegistry *registry, const char *package_name, const char *module_qn,
                   CBMResolvedCallArray *out) {
    memset(ctx, 0, sizeof(*ctx));
    ctx->arena = arena;
    ctx->source = source;
    ctx->source_len = source_len;
    ctx->registry = registry;
    ctx->package_name = package_name ? package_name : "";
    ctx->module_qn = module_qn ? module_qn : "";
    ctx->resolved_calls = out;
    ctx->current_scope = cbm_scope_push(arena, NULL);

    const char *dbg = getenv("CBM_LSP_DEBUG");
    ctx->debug = (dbg && dbg[0]);
}

void java_lsp_add_import(JavaLSPContext *ctx, const char *local_name, const char *target_qn,
                         int kind) {
    if (!ctx || !local_name || !target_qn)
        return;
    if (ctx->import_count >= ctx->import_cap) {
        int new_cap = ctx->import_cap == 0 ? 16 : ctx->import_cap * 2;
        const char **new_names =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*new_names));
        const char **new_qns =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*new_qns));
        int *new_kinds = (int *)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*new_kinds));
        if (!new_names || !new_qns || !new_kinds)
            return;
        if (ctx->import_count > 0) {
            memcpy(new_names, ctx->import_local_names,
                   (size_t)ctx->import_count * sizeof(*new_names));
            memcpy(new_qns, ctx->import_target_qns, (size_t)ctx->import_count * sizeof(*new_qns));
            memcpy(new_kinds, ctx->import_kinds, (size_t)ctx->import_count * sizeof(*new_kinds));
        }
        ctx->import_local_names = new_names;
        ctx->import_target_qns = new_qns;
        ctx->import_kinds = new_kinds;
        ctx->import_cap = new_cap;
    }
    ctx->import_local_names[ctx->import_count] = cbm_arena_strdup(ctx->arena, local_name);
    ctx->import_target_qns[ctx->import_count] = cbm_arena_strdup(ctx->arena, target_qn);
    ctx->import_kinds[ctx->import_count] = kind;
    ctx->import_count++;
}

static void push_enclosing_class(JavaLSPContext *ctx, const char *class_qn) {
    if (ctx->enclosing_class_depth >= ctx->enclosing_class_cap) {
        int new_cap = ctx->enclosing_class_cap == 0 ? 8 : ctx->enclosing_class_cap * 2;
        const char **arr =
            (const char **)cbm_arena_alloc(ctx->arena, (size_t)new_cap * sizeof(*arr));
        if (!arr)
            return;
        if (ctx->enclosing_class_depth > 0) {
            memcpy(arr, ctx->enclosing_class_stack,
                   (size_t)ctx->enclosing_class_depth * sizeof(*arr));
        }
        ctx->enclosing_class_stack = arr;
        ctx->enclosing_class_cap = new_cap;
    }
    ctx->enclosing_class_stack[ctx->enclosing_class_depth++] = class_qn;
}

static void pop_enclosing_class(JavaLSPContext *ctx) {
    if (ctx->enclosing_class_depth > 0)
        ctx->enclosing_class_depth--;
}

/* ── Type-AST → CBMType ───────────────────────────────────────────── */

static const CBMType *parse_type_arguments(JavaLSPContext *ctx, TSNode targs_node);

const CBMType *java_parse_type_node(JavaLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();
    const char *kind = ts_node_type(node);

    /* primitives + void */
    if (strcmp(kind, "void_type") == 0)
        return cbm_type_builtin(ctx->arena, "void");
    if (strcmp(kind, "boolean_type") == 0)
        return cbm_type_builtin(ctx->arena, "boolean");
    if (strcmp(kind, "integral_type") == 0 || strcmp(kind, "floating_point_type") == 0) {
        char *txt = java_node_text(ctx, node);
        return cbm_type_builtin(ctx->arena, txt ? txt : "int");
    }

    /* type_identifier — bare name. Look up via resolver, then fallback. */
    if (strcmp(kind, "type_identifier") == 0) {
        char *name = java_node_text(ctx, node);
        if (!name)
            return cbm_type_unknown();
        const char *qn = java_resolve_type_name(ctx, name);
        if (qn)
            return cbm_type_named(ctx->arena, qn);
        /* Could still be a type variable inside a generic method/class. */
        return cbm_type_named(ctx->arena, name);
    }

    /* scoped_type_identifier — Outer.Inner or pkg.Type. */
    if (strcmp(kind, "scoped_type_identifier") == 0) {
        char *full = java_node_text(ctx, node);
        if (!full)
            return cbm_type_unknown();
        /* Try treating the whole text as already-qualified. */
        if (cbm_registry_lookup_type(ctx->registry, full)) {
            return cbm_type_named(ctx->arena, full);
        }
        /* Otherwise resolve the head segment and append the rest. */
        char *dot = strchr(full, '.');
        if (dot) {
            char *head = cbm_arena_strndup(ctx->arena, full, (size_t)(dot - full));
            const char *head_qn = java_resolve_type_name(ctx, head);
            if (head_qn) {
                return cbm_type_named(ctx->arena,
                                      cbm_arena_sprintf(ctx->arena, "%s%s", head_qn, dot));
            }
        }
        return cbm_type_named(ctx->arena, full);
    }

    /* generic_type — List<String>, Map<K, V>, Function<T, R> etc.
     * Capture ALL template args, not just the first — Map.get's return-type
     * substitution depends on having both K and V available. */
    if (strcmp(kind, "generic_type") == 0) {
        TSNode raw = ts_node_named_child(node, 0);
        TSNode targs =
            ts_node_named_child_count(node) > 1 ? ts_node_named_child(node, 1) : (TSNode){0};
        const CBMType *base = java_parse_type_node(ctx, raw);
        const char *base_qn = NULL;
        if (base && base->kind == CBM_TYPE_NAMED)
            base_qn = base->data.named.qualified_name;
        if (!base_qn)
            return base;
        /* Collect every type argument (K, V, R, ...). */
        int arg_count = 0;
        const CBMType *arg_buf[16];
        if (!ts_node_is_null(targs) && strcmp(ts_node_type(targs), "type_arguments") == 0) {
            uint32_t tn = ts_node_named_child_count(targs);
            for (uint32_t ti = 0; ti < tn && arg_count < 16; ti++) {
                arg_buf[arg_count++] = java_parse_type_node(ctx, ts_node_named_child(targs, ti));
            }
        }
        if (arg_count == 0) {
            arg_buf[arg_count++] = cbm_type_unknown();
        }
        const CBMType **args =
            (const CBMType **)cbm_arena_alloc(ctx->arena, (size_t)(arg_count + 1) * sizeof(*args));
        for (int i = 0; i < arg_count; i++)
            args[i] = arg_buf[i];
        args[arg_count] = NULL;
        return cbm_type_template(ctx->arena, base_qn, args, arg_count);
    }

    /* array_type — T[] (modeled as slice for our purposes). */
    if (strcmp(kind, "array_type") == 0) {
        TSNode elem = ts_node_child_by_field_name(node, "element", 7);
        if (ts_node_is_null(elem) && ts_node_named_child_count(node) > 0) {
            elem = ts_node_named_child(node, 0);
        }
        return cbm_type_slice(ctx->arena, java_parse_type_node(ctx, elem));
    }

    /* type_parameter → identifier inside; treat as a TYPE_PARAM. */
    if (strcmp(kind, "type_parameter") == 0) {
        TSNode name_node = ts_node_named_child(node, 0);
        char *name = ts_node_is_null(name_node) ? NULL : java_node_text(ctx, name_node);
        if (name)
            return cbm_type_type_param(ctx->arena, name);
        return cbm_type_unknown();
    }

    /* wildcard `?` and `? extends T` collapse to UNKNOWN/upper bound. */
    if (strcmp(kind, "wildcard") == 0) {
        uint32_t n = ts_node_named_child_count(node);
        if (n > 0)
            return java_parse_type_node(ctx, ts_node_named_child(node, n - 1));
        return cbm_type_unknown();
    }

    /* Last resort: emit the raw text as a NAMED type. */
    char *txt = java_node_text(ctx, node);
    if (!txt)
        return cbm_type_unknown();
    return cbm_type_named(ctx->arena, txt);
}

static const CBMType *parse_type_arguments(JavaLSPContext *ctx, TSNode targs_node) {
    if (ts_node_is_null(targs_node))
        return NULL;
    if (strcmp(ts_node_type(targs_node), "type_arguments") != 0)
        return NULL;
    if (ts_node_named_child_count(targs_node) == 0)
        return NULL;
    return java_parse_type_node(ctx, ts_node_named_child(targs_node, 0));
}

/* ── Type-name resolution (JLS §6.5) ──────────────────────────────── */

const char *java_resolve_type_name(JavaLSPContext *ctx, const char *name) {
    if (!name || !ctx)
        return NULL;

    /* 0. Inside a nested class, prefer enclosing-class qualification:
     * `Outer.Inner` is reachable as `Inner` from inside `Outer`. */
    for (int i = ctx->enclosing_class_depth - 1; i >= 0; i--) {
        const char *outer = ctx->enclosing_class_stack[i];
        if (!outer)
            continue;
        char buf[JAVA_LSP_BUF];
        int n = snprintf(buf, sizeof(buf), "%s.%s", outer, name);
        if (n > 0 && (size_t)n < sizeof(buf)) {
            if (cbm_registry_lookup_type(ctx->registry, buf)) {
                return cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }

    /* 1. Same package — registry types whose QN ends with .name. */
    if (ctx->module_qn && ctx->module_qn[0]) {
        char buf[JAVA_LSP_BUF];
        int n = snprintf(buf, sizeof(buf), "%s.%s", ctx->module_qn, name);
        if (n > 0 && (size_t)n < sizeof(buf)) {
            if (cbm_registry_lookup_type(ctx->registry, buf)) {
                return cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }

    /* 2. Single-type imports. */
    for (int i = 0; i < ctx->import_count; i++) {
        if (ctx->import_kinds[i] != CBM_JAVA_IMPORT_TYPE)
            continue;
        if (strcmp(ctx->import_local_names[i], name) == 0) {
            return ctx->import_target_qns[i];
        }
    }

    /* 3. java.lang auto-import. Try registered first, then the conventional QN. */
    for (int i = 0; JAVA_LANG_TYPES[i]; i++) {
        if (strcmp(JAVA_LANG_TYPES[i], name) == 0) {
            char buf[JAVA_LSP_BUF];
            int n = snprintf(buf, sizeof(buf), "java.lang.%s", name);
            if (n > 0 && (size_t)n < sizeof(buf)) {
                return cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }

    /* 4. On-demand imports. */
    for (int i = 0; i < ctx->import_count; i++) {
        if (ctx->import_kinds[i] != CBM_JAVA_IMPORT_ON_DEMAND)
            continue;
        char buf[JAVA_LSP_BUF];
        int n = snprintf(buf, sizeof(buf), "%s.%s", ctx->import_target_qns[i], name);
        if (n > 0 && (size_t)n < sizeof(buf)) {
            if (cbm_registry_lookup_type(ctx->registry, buf)) {
                return cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }

    /* 5. Java package prefix. */
    if (ctx->package_name && ctx->package_name[0]) {
        char buf[JAVA_LSP_BUF];
        int n = snprintf(buf, sizeof(buf), "%s.%s", ctx->package_name, name);
        if (n > 0 && (size_t)n < sizeof(buf)) {
            if (cbm_registry_lookup_type(ctx->registry, buf)) {
                return cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }

    /* Exact already-qualified name. */
    if (cbm_registry_lookup_type(ctx->registry, name)) {
        return cbm_arena_strdup(ctx->arena, name);
    }

    /* Cross-file sole-definer fallback. A same-package static call
     * `Util.square()` references class `Util` whose graph QN embeds the
     * defining file's path ("<project>.Util.Util"), which the caller's
     * module_qn ("<project>.Main") can't reconstruct — and the fixture has no
     * import to pin it.  When the project-wide registry holds EXACTLY ONE type
     * with this short name, resolve to it.  Bounded to a single candidate so an
     * ambiguous name (>1 type) stays unresolved — sound, mirroring the
     * registry's "unique_name" strategy.  Only fires after all qualified
     * lookups miss, so it never overrides a more specific match. */
    if (ctx->registry && ctx->registry->types) {
        const char *only_qn = NULL;
        int matches = 0;
        for (int i = 0; i < ctx->registry->type_count && matches < 2; i++) {
            const CBMRegisteredType *t = &ctx->registry->types[i];
            if (!t->qualified_name || !t->short_name || t->alias_of) {
                continue;
            }
            if (strcmp(t->short_name, name) == 0) {
                only_qn = t->qualified_name;
                matches++;
            }
        }
        if (matches == 1 && only_qn) {
            return cbm_arena_strdup(ctx->arena, only_qn);
        }
    }

    return NULL;
}

/* ── Expression-type evaluation ───────────────────────────────────── */

const CBMType *java_eval_expr_type(JavaLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return cbm_type_unknown();
    if (ctx->eval_depth >= JAVA_LSP_MAX_EVAL_DEPTH)
        return cbm_type_unknown();
    ctx->eval_depth++;
    const CBMType *result = cbm_type_unknown();
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "parenthesized_expression") == 0) {
        if (ts_node_named_child_count(node) > 0) {
            result = java_eval_expr_type(ctx, ts_node_named_child(node, 0));
        }
        goto out;
    }

    /* Literals */
    if (strcmp(kind, "decimal_integer_literal") == 0 || strcmp(kind, "hex_integer_literal") == 0 ||
        strcmp(kind, "binary_integer_literal") == 0 || strcmp(kind, "octal_integer_literal") == 0) {
        char *txt = java_node_text(ctx, node);
        if (txt) {
            size_t len = strlen(txt);
            if (len > 0 && (txt[len - 1] == 'l' || txt[len - 1] == 'L')) {
                result = cbm_type_builtin(ctx->arena, "long");
                goto out;
            }
        }
        result = cbm_type_builtin(ctx->arena, "int");
        goto out;
    }
    if (strcmp(kind, "decimal_floating_point_literal") == 0 ||
        strcmp(kind, "hex_floating_point_literal") == 0) {
        char *txt = java_node_text(ctx, node);
        if (txt) {
            size_t len = strlen(txt);
            if (len > 0 && (txt[len - 1] == 'f' || txt[len - 1] == 'F')) {
                result = cbm_type_builtin(ctx->arena, "float");
                goto out;
            }
        }
        result = cbm_type_builtin(ctx->arena, "double");
        goto out;
    }
    if (strcmp(kind, "true") == 0 || strcmp(kind, "false") == 0) {
        result = cbm_type_builtin(ctx->arena, "boolean");
        goto out;
    }
    if (strcmp(kind, "character_literal") == 0) {
        result = cbm_type_builtin(ctx->arena, "char");
        goto out;
    }
    if (strcmp(kind, "string_literal") == 0 || strcmp(kind, "string_fragment") == 0) {
        result = cbm_type_named(ctx->arena, "java.lang.String");
        goto out;
    }
    if (strcmp(kind, "null_literal") == 0) {
        /* `null` could be assigned to any reference type — leave UNKNOWN
         * and let context recover. */
        result = cbm_type_unknown();
        goto out;
    }

    /* `this` — current class. */
    if (strcmp(kind, "this") == 0) {
        if (ctx->enclosing_class_qn) {
            result = cbm_type_named(ctx->arena, ctx->enclosing_class_qn);
        }
        goto out;
    }

    /* `super` — superclass. */
    if (strcmp(kind, "super") == 0) {
        if (ctx->enclosing_super_qn) {
            result = cbm_type_named(ctx->arena, ctx->enclosing_super_qn);
        } else {
            result = cbm_type_named(ctx->arena, "java.lang.Object");
        }
        goto out;
    }

    /* Identifier — local var → field → import → type. */
    if (strcmp(kind, "identifier") == 0) {
        char *name = java_node_text(ctx, node);
        if (name)
            result = resolve_identifier_type(ctx, name);
        goto out;
    }

    /* Class literal: `Foo.class` is `Class<Foo>`. */
    if (strcmp(kind, "class_literal") == 0) {
        result = cbm_type_named(ctx->arena, "java.lang.Class");
        goto out;
    }

    /* Field access. */
    if (strcmp(kind, "field_access") == 0) {
        result = eval_field_access(ctx, node);
        goto out;
    }

    /* Method invocation. */
    if (strcmp(kind, "method_invocation") == 0) {
        result = eval_method_invocation(ctx, node);
        goto out;
    }

    /* Object creation: `new Foo()`. */
    if (strcmp(kind, "object_creation_expression") == 0) {
        result = eval_object_creation(ctx, node);
        goto out;
    }

    /* `new int[10]` style. */
    if (strcmp(kind, "array_creation_expression") == 0) {
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        const CBMType *elem = java_parse_type_node(ctx, type_node);
        result = cbm_type_slice(ctx->arena, elem);
        goto out;
    }

    /* Cast — `(T)x`. */
    if (strcmp(kind, "cast_expression") == 0) {
        result = eval_cast(ctx, node);
        goto out;
    }

    /* Ternary — narrow to the common type or LHS. */
    if (strcmp(kind, "ternary_expression") == 0) {
        result = eval_ternary(ctx, node);
        goto out;
    }

    /* Binary ops — JLS §15.18. String concat with + emits java.lang.String;
     * numeric ops emit the wider numeric type. */
    if (strcmp(kind, "binary_expression") == 0) {
        result = eval_binary(ctx, node);
        goto out;
    }

    /* Unary — most preserve operand type; ! → boolean. */
    if (strcmp(kind, "unary_expression") == 0 || strcmp(kind, "update_expression") == 0) {
        result = eval_unary(ctx, node);
        goto out;
    }

    /* Array access — element type. */
    if (strcmp(kind, "array_access") == 0) {
        result = eval_array_access(ctx, node);
        goto out;
    }

    /* instanceof returns boolean. */
    if (strcmp(kind, "instanceof_expression") == 0) {
        result = cbm_type_builtin(ctx->arena, "boolean");
        goto out;
    }

    /* Assignment expression — type of RHS. */
    if (strcmp(kind, "assignment_expression") == 0) {
        TSNode rhs = ts_node_child_by_field_name(node, "right", 5);
        if (!ts_node_is_null(rhs))
            result = java_eval_expr_type(ctx, rhs);
        goto out;
    }

    /* Lambda */
    if (strcmp(kind, "lambda_expression") == 0) {
        result = eval_lambda(ctx, node);
        goto out;
    }

    /* Method reference: ClassName::method or instance::method. */
    if (strcmp(kind, "method_reference") == 0) {
        result = eval_method_reference(ctx, node);
        goto out;
    }

    /* Switch expression — collapse to first arm's type. */
    if (strcmp(kind, "switch_expression") == 0) {
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode c = ts_node_named_child(node, i);
            if (strcmp(ts_node_type(c), "switch_block") == 0) {
                uint32_t bn = ts_node_named_child_count(c);
                for (uint32_t j = 0; j < bn; j++) {
                    TSNode arm = ts_node_named_child(c, j);
                    if (strcmp(ts_node_type(arm), "switch_rule") == 0) {
                        if (ts_node_named_child_count(arm) > 0) {
                            TSNode body =
                                ts_node_named_child(arm, ts_node_named_child_count(arm) - 1);
                            result = java_eval_expr_type(ctx, body);
                            goto out;
                        }
                    }
                }
            }
        }
        goto out;
    }

out:
    ctx->eval_depth--;
    return result ? result : cbm_type_unknown();
}

/* Resolve identifier to a type.
 * Order: local scope → fields of enclosing class (walking outer classes) →
 * import (single static) → type alias (single class import) → on-demand
 * static → fall back to UNKNOWN. */
static const CBMType *resolve_identifier_type(JavaLSPContext *ctx, const char *name) {
    if (!name)
        return cbm_type_unknown();

    /* Scope chain. */
    const CBMType *t = cbm_scope_lookup(ctx->current_scope, name);
    if (t && !cbm_type_is_unknown(t))
        return t;

    /* Enclosing-class fields, walking super chain. */
    if (ctx->enclosing_class_qn) {
        const CBMType *ft = java_lookup_field_type(ctx, ctx->enclosing_class_qn, name);
        if (ft && !cbm_type_is_unknown(ft))
            return ft;
    }
    /* Outer classes. */
    for (int i = ctx->enclosing_class_depth - 2; i >= 0; i--) {
        const char *outer = ctx->enclosing_class_stack[i];
        if (!outer)
            continue;
        const CBMType *ft = java_lookup_field_type(ctx, outer, name);
        if (ft && !cbm_type_is_unknown(ft))
            return ft;
    }

    /* Static imports — could be a static field. */
    for (int i = 0; i < ctx->import_count; i++) {
        if (ctx->import_kinds[i] != CBM_JAVA_IMPORT_STATIC)
            continue;
        if (strcmp(ctx->import_local_names[i], name) == 0) {
            /* The target QN is e.g. `java.lang.System.out` — we don't have
             * a registered type for that; signal via the QN. */
            const char *target = ctx->import_target_qns[i];
            const char *last_dot = strrchr(target, '.');
            if (last_dot) {
                char *cls = cbm_arena_strndup(ctx->arena, target, (size_t)(last_dot - target));
                const CBMType *ft = java_lookup_field_type(ctx, cls, name);
                if (ft && !cbm_type_is_unknown(ft))
                    return ft;
            }
            return cbm_type_unknown();
        }
    }

    /* Type name? Treat the identifier as a class reference. */
    const char *type_qn = java_resolve_type_name(ctx, name);
    if (type_qn)
        return cbm_type_named(ctx->arena, type_qn);

    return cbm_type_unknown();
}

/* Evaluate `obj.field` — dispatch on receiver type. */
static const CBMType *eval_field_access(JavaLSPContext *ctx, TSNode node) {
    TSNode obj = ts_node_child_by_field_name(node, "object", 6);
    TSNode field = ts_node_child_by_field_name(node, "field", 5);
    if (ts_node_is_null(field))
        return cbm_type_unknown();
    char *fname = java_node_text(ctx, field);
    if (!fname)
        return cbm_type_unknown();

    /* Special-case: System.out, System.err — common static fields. */
    if (!ts_node_is_null(obj) && strcmp(ts_node_type(obj), "identifier") == 0) {
        char *oname = java_node_text(ctx, obj);
        if (oname && strcmp(oname, "System") == 0) {
            if (strcmp(fname, "out") == 0 || strcmp(fname, "err") == 0) {
                return cbm_type_named(ctx->arena, "java.io.PrintStream");
            }
            if (strcmp(fname, "in") == 0) {
                return cbm_type_named(ctx->arena, "java.io.InputStream");
            }
        }
    }

    /* Special-case: `length` on array types is `int`. */
    const CBMType *recv = ts_node_is_null(obj) ? cbm_type_unknown() : java_eval_expr_type(ctx, obj);
    if (recv && recv->kind == CBM_TYPE_SLICE && strcmp(fname, "length") == 0) {
        return cbm_type_builtin(ctx->arena, "int");
    }

    /* For `this.field`, try the scope chain first — process_field_decl
     * binds class fields into the enclosing scope, which has accurate type
     * info even when the registered class lacks field metadata. */
    if (!ts_node_is_null(obj) && strcmp(ts_node_type(obj), "this") == 0) {
        const CBMType *scope_t = cbm_scope_lookup(ctx->current_scope, fname);
        if (scope_t && !cbm_type_is_unknown(scope_t))
            return scope_t;
    }

    const CBMType *res = resolve_member_type(ctx, recv, fname);
    if (res && !cbm_type_is_unknown(res))
        return res;

    /* Last resort: if receiver is `this` or an unresolved identifier, fall
     * back to scope chain by name. */
    if (!ts_node_is_null(obj)) {
        const CBMType *scope_t = cbm_scope_lookup(ctx->current_scope, fname);
        if (scope_t && !cbm_type_is_unknown(scope_t))
            return scope_t;
    }
    return cbm_type_unknown();
}

/* Lookup a field's type on a class, walking the parent chain. */
const CBMType *java_lookup_field_type(JavaLSPContext *ctx, const char *class_qn,
                                      const char *field_name) {
    if (!class_qn || !field_name)
        return cbm_type_unknown();
    const char *cur = class_qn;
    for (int hops = 0; hops < JAVA_LSP_MAX_INHERIT_HOPS && cur; hops++) {
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, cur);
        if (!rt)
            break;
        if (rt->field_names && rt->field_types) {
            for (int i = 0; rt->field_names[i]; i++) {
                if (strcmp(rt->field_names[i], field_name) == 0) {
                    return rt->field_types[i];
                }
            }
        }
        if (rt->embedded_types && rt->embedded_types[0]) {
            cur = rt->embedded_types[0];
        } else {
            cur = NULL;
        }
    }
    return cbm_type_unknown();
}

/* Resolve `recv.member` for non-method member access. */
static const CBMType *resolve_member_type(JavaLSPContext *ctx, const CBMType *recv,
                                          const char *member_name) {
    if (!recv || !member_name)
        return cbm_type_unknown();
    const CBMType *base = recv;
    if (base->kind == CBM_TYPE_TEMPLATE)
        base = NULL; /* fall through to QN */
    const char *recv_qn = NULL;
    if (recv->kind == CBM_TYPE_NAMED)
        recv_qn = recv->data.named.qualified_name;
    else if (recv->kind == CBM_TYPE_TEMPLATE)
        recv_qn = recv->data.template_type.template_name;
    if (!recv_qn)
        return cbm_type_unknown();
    return java_lookup_field_type(ctx, recv_qn, member_name);
}

/* ── Method lookup ────────────────────────────────────────────────── */

const CBMRegisteredFunc *java_lookup_method(JavaLSPContext *ctx, const char *class_qn,
                                            const char *method_name, int arg_count) {
    if (!class_qn || !method_name)
        return NULL;
    const char *cur = class_qn;
    const CBMRegisteredFunc *fallback = NULL;
    for (int hops = 0; hops < JAVA_LSP_MAX_INHERIT_HOPS && cur; hops++) {
        /* Try arg-count-aware lookup first. */
        const CBMRegisteredFunc *m =
            cbm_registry_lookup_method_by_args(ctx->registry, cur, method_name, arg_count);
        if (m)
            return m;
        /* Otherwise capture any name match as fallback. */
        if (!fallback) {
            fallback = cbm_registry_lookup_method(ctx->registry, cur, method_name);
        }
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, cur);
        if (!rt)
            break;
        if (rt->embedded_types && rt->embedded_types[0]) {
            cur = rt->embedded_types[0];
        } else {
            cur = NULL;
        }
    }
    return fallback;
}

/* ── Method-invocation evaluation ─────────────────────────────────── */

static const CBMType *java_return_type_of(const CBMRegisteredFunc *f) {
    if (!f || !f->signature)
        return cbm_type_unknown();
    if (f->signature->kind != CBM_TYPE_FUNC)
        return cbm_type_unknown();
    const CBMType *const *rets = f->signature->data.func.return_types;
    if (!rets || !rets[0])
        return cbm_type_unknown();
    return rets[0];
}

/* Well-known generic-type substitution table.
 *
 * For container/wrapper types whose stdlib registration uses
 * java.lang.Object as the return type (because the registry doesn't model
 * Java type variables natively), this helper rewrites the return to the
 * appropriate template argument when the receiver carries one. This is
 * exactly the substitution the JLS §15.12 erasure rules perform.
 *
 * Mapping (one entry per (type, method) pair):
 *   List/Set/Iterator/Optional/Stream/Iterable/Collection/...    .get/.next/... → T0
 *   Map/HashMap/TreeMap/LinkedHashMap/ConcurrentHashMap/...      .get/.put/...  → T1 (V)
 *   Map.Entry                                                    .getValue       → T1
 *   Map.Entry                                                    .getKey         → T0
 *   Function<T,R>.apply / BiFunction<T,U,R>.apply                                → R (last arg)
 *
 * Returns the substituted type, or `fallback` if the receiver isn't a
 * recognized parametric container. */
static bool is_value_typed_container(const char *qn) {
    static const char *known[] = {
        "java.util.List",          "java.util.ArrayList",
        "java.util.LinkedList",    "java.util.Vector",
        "java.util.Stack",         "java.util.Set",
        "java.util.HashSet",       "java.util.TreeSet",
        "java.util.LinkedHashSet", "java.util.Collection",
        "java.lang.Iterable",      "java.util.Iterator",
        "java.util.ListIterator",  "java.util.Optional",
        "java.util.stream.Stream", "java.util.Queue",
        "java.util.Deque",         "java.util.ArrayDeque",
        "java.util.PriorityQueue", NULL,
    };
    for (int i = 0; known[i]; i++) {
        if (strcmp(known[i], qn) == 0)
            return true;
    }
    return false;
}

static bool is_map_like(const char *qn) {
    static const char *known[] = {
        "java.util.Map",
        "java.util.HashMap",
        "java.util.TreeMap",
        "java.util.LinkedHashMap",
        "java.util.concurrent.ConcurrentHashMap",
        "java.util.concurrent.ConcurrentMap",
        NULL,
    };
    for (int i = 0; known[i]; i++) {
        if (strcmp(known[i], qn) == 0)
            return true;
    }
    return false;
}

static const CBMType *substitute_generic_return(JavaLSPContext *ctx, const char *recv_qn,
                                                const char *method_name,
                                                const CBMType *const *arg_arr, int targ_count,
                                                const CBMType *fallback) {
    (void)ctx;
    if (!recv_qn || !method_name || !arg_arr || targ_count <= 0)
        return fallback;
    if (!fallback)
        return fallback;
    /* Only rewrite Object returns. */
    if (fallback->kind != CBM_TYPE_NAMED)
        return fallback;
    if (strcmp(fallback->data.named.qualified_name, "java.lang.Object") != 0)
        return fallback;

    if (is_value_typed_container(recv_qn)) {
        /* For Iterator.next() and List.get/etc. — the element type is T0. */
        if (strcmp(method_name, "get") == 0 || strcmp(method_name, "set") == 0 ||
            strcmp(method_name, "remove") == 0 || strcmp(method_name, "next") == 0 ||
            strcmp(method_name, "peek") == 0 || strcmp(method_name, "poll") == 0 ||
            strcmp(method_name, "element") == 0 || strcmp(method_name, "first") == 0 ||
            strcmp(method_name, "last") == 0 || strcmp(method_name, "getFirst") == 0 ||
            strcmp(method_name, "getLast") == 0 || strcmp(method_name, "removeFirst") == 0 ||
            strcmp(method_name, "removeLast") == 0 || strcmp(method_name, "orElse") == 0 ||
            strcmp(method_name, "orElseGet") == 0 || strcmp(method_name, "orElseThrow") == 0 ||
            strcmp(method_name, "findFirst") == 0 || strcmp(method_name, "findAny") == 0 ||
            strcmp(method_name, "min") == 0 || strcmp(method_name, "max") == 0 ||
            strcmp(method_name, "reduce") == 0) {
            return arg_arr[0] ? arg_arr[0] : fallback;
        }
    }
    if (is_map_like(recv_qn) && targ_count >= 2) {
        if (strcmp(method_name, "get") == 0 || strcmp(method_name, "put") == 0 ||
            strcmp(method_name, "remove") == 0 || strcmp(method_name, "getOrDefault") == 0 ||
            strcmp(method_name, "putIfAbsent") == 0 ||
            strcmp(method_name, "computeIfAbsent") == 0 || strcmp(method_name, "compute") == 0 ||
            strcmp(method_name, "merge") == 0) {
            return arg_arr[1] ? arg_arr[1] : fallback;
        }
    }
    if (strcmp(recv_qn, "java.util.Map.Entry") == 0 && targ_count >= 2) {
        if (strcmp(method_name, "getValue") == 0 || strcmp(method_name, "setValue") == 0) {
            return arg_arr[1] ? arg_arr[1] : fallback;
        }
        if (strcmp(method_name, "getKey") == 0) {
            return arg_arr[0] ? arg_arr[0] : fallback;
        }
    }
    /* Function/BiFunction.apply returns R (last template arg). */
    if ((strcmp(recv_qn, "java.util.function.Function") == 0 ||
         strcmp(recv_qn, "java.util.function.BiFunction") == 0) &&
        strcmp(method_name, "apply") == 0) {
        return arg_arr[targ_count - 1] ? arg_arr[targ_count - 1] : fallback;
    }
    if (strcmp(recv_qn, "java.util.function.Supplier") == 0 && strcmp(method_name, "get") == 0) {
        return arg_arr[0] ? arg_arr[0] : fallback;
    }
    return fallback;
}

static const CBMType *eval_method_invocation(JavaLSPContext *ctx, TSNode node) {
    TSNode obj = ts_node_child_by_field_name(node, "object", 6);
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name_node))
        return cbm_type_unknown();
    char *mname = java_node_text(ctx, name_node);
    if (!mname)
        return cbm_type_unknown();
    int arity = count_call_args(node);

    /* No receiver: `foo()` — method is on enclosing class or static import. */
    if (ts_node_is_null(obj)) {
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredFunc *f =
                java_lookup_method(ctx, ctx->enclosing_class_qn, mname, arity);
            if (f)
                return java_return_type_of(f);
        }
        for (int i = 0; i < ctx->import_count; i++) {
            if (ctx->import_kinds[i] != CBM_JAVA_IMPORT_STATIC)
                continue;
            if (strcmp(ctx->import_local_names[i], mname) != 0)
                continue;
            const char *target = ctx->import_target_qns[i];
            const char *last_dot = strrchr(target, '.');
            if (!last_dot)
                continue;
            char *cls = cbm_arena_strndup(ctx->arena, target, (size_t)(last_dot - target));
            const CBMRegisteredFunc *f = java_lookup_method(ctx, cls, mname, arity);
            if (f)
                return java_return_type_of(f);
        }
        return cbm_type_unknown();
    }

    /* `super.method()` */
    if (strcmp(ts_node_type(obj), "super") == 0) {
        const char *super_qn =
            ctx->enclosing_super_qn ? ctx->enclosing_super_qn : "java.lang.Object";
        const CBMRegisteredFunc *f = java_lookup_method(ctx, super_qn, mname, arity);
        if (f)
            return java_return_type_of(f);
        return cbm_type_unknown();
    }

    /* Static call: `ClassName.method()` where obj is an identifier matching a
     * known type. */
    if (strcmp(ts_node_type(obj), "identifier") == 0) {
        char *oname = java_node_text(ctx, obj);
        if (oname) {
            const char *cls_qn = java_resolve_type_name(ctx, oname);
            if (cls_qn) {
                /* Only treat as static if there's no local var of that name. */
                if (cbm_type_is_unknown(cbm_scope_lookup(ctx->current_scope, oname))) {
                    const CBMRegisteredFunc *f = java_lookup_method(ctx, cls_qn, mname, arity);
                    if (f)
                        return java_return_type_of(f);
                }
            }
        }
    }

    /* Instance call. */
    const CBMType *recv = java_eval_expr_type(ctx, obj);
    const CBMType *base = recv;
    if (base && base->kind == CBM_TYPE_TEMPLATE) {
        const char *base_qn = base->data.template_type.template_name;
        if (base_qn) {
            const CBMRegisteredFunc *f = java_lookup_method(ctx, base_qn, mname, arity);
            if (f) {
                const CBMType *ret = java_return_type_of(f);
                /* Generic substitution + carrier propagation. */
                const CBMType *subst = substitute_generic_return(
                    ctx, base_qn, mname, base->data.template_type.template_args,
                    base->data.template_type.arg_count, ret);
                if (!subst)
                    subst = ret;
                /* Wrap NAMED carriers (Stream, Iterator, Optional, ...) as
                 * TEMPLATE so chained methods retain type-arg context. */
                subst = propagate_template(ctx->arena, base_qn, mname,
                                           base->data.template_type.template_args,
                                           base->data.template_type.arg_count, subst);
                return subst;
            }
        }
    }
    if (base && base->kind == CBM_TYPE_NAMED) {
        const CBMRegisteredFunc *f =
            java_lookup_method(ctx, base->data.named.qualified_name, mname, arity);
        if (f)
            return java_return_type_of(f);
    }
    if (base && base->kind == CBM_TYPE_SLICE) {
        /* No methods on arrays beyond `length` (handled in field_access). */
        return cbm_type_unknown();
    }
    return cbm_type_unknown();
}

/* `new Foo<T>()` */
static const CBMType *eval_object_creation(JavaLSPContext *ctx, TSNode node) {
    TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
    if (ts_node_is_null(type_node)) {
        /* Could be qualified expression form. */
        for (uint32_t i = 0; i < ts_node_named_child_count(node); i++) {
            TSNode c = ts_node_named_child(node, i);
            const char *k = ts_node_type(c);
            if (strcmp(k, "type_identifier") == 0 || strcmp(k, "scoped_type_identifier") == 0 ||
                strcmp(k, "generic_type") == 0) {
                type_node = c;
                break;
            }
        }
    }
    if (ts_node_is_null(type_node))
        return cbm_type_unknown();
    return java_parse_type_node(ctx, type_node);
}

static const CBMType *eval_array_access(JavaLSPContext *ctx, TSNode node) {
    TSNode arr = ts_node_child_by_field_name(node, "array", 5);
    if (ts_node_is_null(arr))
        return cbm_type_unknown();
    const CBMType *t = java_eval_expr_type(ctx, arr);
    if (t && t->kind == CBM_TYPE_SLICE)
        return t->data.slice.elem;
    /* List<T>[i] in Java is illegal — but at runtime we sometimes see
     * Collection.get-style indexing through the array_access node. Stay
     * conservative. */
    return cbm_type_unknown();
}

static const CBMType *eval_cast(JavaLSPContext *ctx, TSNode node) {
    TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
    if (ts_node_is_null(type_node))
        return cbm_type_unknown();
    return java_parse_type_node(ctx, type_node);
}

static const CBMType *eval_ternary(JavaLSPContext *ctx, TSNode node) {
    TSNode then_n = ts_node_child_by_field_name(node, "consequence", 11);
    TSNode else_n = ts_node_child_by_field_name(node, "alternative", 11);
    const CBMType *t = java_eval_expr_type(ctx, then_n);
    if (t && !cbm_type_is_unknown(t))
        return t;
    return java_eval_expr_type(ctx, else_n);
}

static bool is_string_concat(JavaLSPContext *ctx, TSNode node) {
    /* Either operand of type java.lang.String makes the whole expr a String. */
    TSNode lhs = ts_node_child_by_field_name(node, "left", 4);
    TSNode rhs = ts_node_child_by_field_name(node, "right", 5);
    const CBMType *l = java_eval_expr_type(ctx, lhs);
    const CBMType *r = java_eval_expr_type(ctx, rhs);
    if (l && l->kind == CBM_TYPE_NAMED &&
        strcmp(l->data.named.qualified_name, "java.lang.String") == 0)
        return true;
    if (r && r->kind == CBM_TYPE_NAMED &&
        strcmp(r->data.named.qualified_name, "java.lang.String") == 0)
        return true;
    return false;
}

static const CBMType *eval_binary(JavaLSPContext *ctx, TSNode node) {
    /* Determine operator. tree-sitter-java exposes it as a child token. */
    char *op = NULL;
    uint32_t cn = ts_node_child_count(node);
    for (uint32_t i = 0; i < cn; i++) {
        TSNode c = ts_node_child(node, i);
        if (ts_node_is_named(c))
            continue;
        op = java_node_text(ctx, c);
        break;
    }
    /* Fallback when the unnamed-child trick fails (older grammars). */
    if (!op)
        op = "";

    if (strcmp(op, "==") == 0 || strcmp(op, "!=") == 0 || strcmp(op, "<") == 0 ||
        strcmp(op, ">") == 0 || strcmp(op, "<=") == 0 || strcmp(op, ">=") == 0 ||
        strcmp(op, "&&") == 0 || strcmp(op, "||") == 0) {
        return cbm_type_builtin(ctx->arena, "boolean");
    }
    if (strcmp(op, "+") == 0 && is_string_concat(ctx, node)) {
        return cbm_type_named(ctx->arena, "java.lang.String");
    }
    /* Numeric promotion — keep LHS for simplicity. */
    TSNode lhs = ts_node_child_by_field_name(node, "left", 4);
    return java_eval_expr_type(ctx, lhs);
}

static const CBMType *eval_unary(JavaLSPContext *ctx, TSNode node) {
    /* `!x` → boolean; `-x` / `~x` / ++ / -- preserve the operand type. */
    char *op = NULL;
    uint32_t cn = ts_node_child_count(node);
    for (uint32_t i = 0; i < cn; i++) {
        TSNode c = ts_node_child(node, i);
        if (ts_node_is_named(c))
            continue;
        op = java_node_text(ctx, c);
        break;
    }
    if (op && strcmp(op, "!") == 0)
        return cbm_type_builtin(ctx->arena, "boolean");
    TSNode operand = ts_node_child_by_field_name(node, "operand", 7);
    if (ts_node_is_null(operand) && ts_node_named_child_count(node) > 0) {
        operand = ts_node_named_child(node, 0);
    }
    return java_eval_expr_type(ctx, operand);
}

static const CBMType *eval_lambda(JavaLSPContext *ctx, TSNode node) {
    (void)node;
    /* Without a SAM target type, the lambda is functional-interface-typed —
     * we don't attempt to infer the SAM here. Caller drops the edge. */
    return cbm_type_unknown();
}

static const CBMType *eval_method_reference(JavaLSPContext *ctx, TSNode node) {
    /* Same caveat as lambdas; method_reference produces a functional
     * interface. */
    (void)node;
    return cbm_type_unknown();
}

/* ── Statement processing — bind into scope ───────────────────────── */

void java_process_statement(JavaLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node) || ctx->statement_depth >= JAVA_LSP_MAX_STMT_DEPTH)
        return;
    ctx->statement_depth++;
    const char *kind = ts_node_type(node);

    if (strcmp(kind, "local_variable_declaration") == 0) {
        process_local_var_decl(ctx, node);
    } else if (strcmp(kind, "enhanced_for_statement") == 0) {
        process_enhanced_for(ctx, node);
    } else if (strcmp(kind, "block") == 0) {
        process_block(ctx, node);
    } else if (strcmp(kind, "resource") == 0) {
        /* try-with-resources: `try (BufferedReader br = new ...())`.
         * tree-sitter-java models each resource as a `resource` node with
         * type + name + value fields (or an existing-variable reference). */
        TSNode rtype = ts_node_child_by_field_name(node, "type", 4);
        TSNode rname = ts_node_child_by_field_name(node, "name", 4);
        TSNode rvalue = ts_node_child_by_field_name(node, "value", 5);
        char *rn = ts_node_is_null(rname) ? NULL : java_node_text(ctx, rname);
        if (rn) {
            const CBMType *rt = cbm_type_unknown();
            bool is_var = false;
            if (!ts_node_is_null(rtype) && strcmp(ts_node_type(rtype), "type_identifier") == 0) {
                char *tt = java_node_text(ctx, rtype);
                if (tt && strcmp(tt, "var") == 0)
                    is_var = true;
            }
            if (!is_var && !ts_node_is_null(rtype)) {
                rt = java_parse_type_node(ctx, rtype);
            }
            if ((is_var || cbm_type_is_unknown(rt)) && !ts_node_is_null(rvalue)) {
                rt = java_eval_expr_type(ctx, rvalue);
            }
            cbm_scope_bind(ctx->current_scope, rn, rt ? rt : cbm_type_unknown());
        }
    } else if (strcmp(kind, "catch_clause") == 0) {
        /* catch (Type|Type2 var) { body } — bind var into a fresh scope. */
        TSNode formal = ts_node_child_by_field_name(node, "parameter", 9);
        if (ts_node_is_null(formal)) {
            formal = child_by_kind(node, "catch_formal_parameter");
        }
        if (!ts_node_is_null(formal)) {
            TSNode pname = ts_node_child_by_field_name(formal, "name", 4);
            TSNode ptype = ts_node_child_by_field_name(formal, "type", 4);
            if (ts_node_is_null(ptype))
                ptype = child_by_kind(formal, "catch_type");
            char *pn = ts_node_is_null(pname) ? NULL : java_node_text(ctx, pname);
            if (pn) {
                /* catch_type may be a union_type — pick the first union member. */
                const CBMType *pt = cbm_type_unknown();
                if (!ts_node_is_null(ptype)) {
                    if (strcmp(ts_node_type(ptype), "catch_type") == 0 &&
                        ts_node_named_child_count(ptype) > 0) {
                        pt = java_parse_type_node(ctx, ts_node_named_child(ptype, 0));
                    } else {
                        pt = java_parse_type_node(ctx, ptype);
                    }
                }
                cbm_scope_bind(ctx->current_scope, pn, pt);
            }
        }
    }

    ctx->statement_depth--;
}

static void process_local_var_decl(JavaLSPContext *ctx, TSNode node) {
    TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
    const CBMType *static_type =
        ts_node_is_null(type_node) ? cbm_type_unknown() : java_parse_type_node(ctx, type_node);
    bool is_var =
        !ts_node_is_null(type_node) && strcmp(ts_node_type(type_node), "type_identifier") == 0 && ({
            char *txt = java_node_text(ctx, type_node);
            txt &&strcmp(txt, "var") == 0;
        });

    /* Walk variable_declarator children. */
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(c), "variable_declarator") != 0)
            continue;
        TSNode name_node = ts_node_child_by_field_name(c, "name", 4);
        TSNode value_node = ts_node_child_by_field_name(c, "value", 5);
        if (ts_node_is_null(name_node))
            continue;
        char *vname = java_node_text(ctx, name_node);
        if (!vname)
            continue;
        const CBMType *bind_type = static_type;
        if ((is_var || cbm_type_is_unknown(bind_type)) && !ts_node_is_null(value_node)) {
            bind_type = java_eval_expr_type(ctx, value_node);
        }
        cbm_scope_bind(ctx->current_scope, vname, bind_type ? bind_type : cbm_type_unknown());
    }
}

static void process_enhanced_for(JavaLSPContext *ctx, TSNode node) {
    TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    TSNode value_node = ts_node_child_by_field_name(node, "value", 5);
    char *vname = ts_node_is_null(name_node) ? NULL : java_node_text(ctx, name_node);
    if (!vname)
        return;

    /* `var x : xs` — explicit-var inference. tree-sitter exposes "var" as a
     * type_identifier whose text is exactly "var". */
    bool is_var = false;
    if (!ts_node_is_null(type_node) && strcmp(ts_node_type(type_node), "type_identifier") == 0) {
        char *tt = java_node_text(ctx, type_node);
        if (tt && strcmp(tt, "var") == 0)
            is_var = true;
    }

    const CBMType *t = (is_var || ts_node_is_null(type_node))
                           ? cbm_type_unknown()
                           : java_parse_type_node(ctx, type_node);

    /* When type is `var` or unparseable, infer from the iterable. */
    if (cbm_type_is_unknown(t) && !ts_node_is_null(value_node)) {
        const CBMType *iter_t = java_eval_expr_type(ctx, value_node);
        if (iter_t && iter_t->kind == CBM_TYPE_SLICE) {
            t = iter_t->data.slice.elem;
        } else if (iter_t && iter_t->kind == CBM_TYPE_TEMPLATE &&
                   iter_t->data.template_type.arg_count > 0 &&
                   iter_t->data.template_type.template_args[0]) {
            t = iter_t->data.template_type.template_args[0];
        }
    }
    cbm_scope_bind(ctx->current_scope, vname, t ? t : cbm_type_unknown());
}

static void process_block(JavaLSPContext *ctx, TSNode node) {
    CBMScope *saved = ctx->current_scope;
    ctx->current_scope = cbm_scope_push(ctx->arena, saved);
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(node, i);
        java_process_statement(ctx, c);
        java_resolve_calls_in_node(ctx, c);
    }
    ctx->current_scope = saved;
}

/* ── Top-level walk ───────────────────────────────────────────────── */

static void process_field_decl(JavaLSPContext *ctx, TSNode node) {
    /* Handled at class-registration time for inner-class scope. Still bind
     * declared variables with initializers so static-init blocks resolve
     * correctly. */
    TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
    const CBMType *static_type =
        ts_node_is_null(type_node) ? cbm_type_unknown() : java_parse_type_node(ctx, type_node);
    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(node, i);
        if (strcmp(ts_node_type(c), "variable_declarator") != 0)
            continue;
        TSNode name_node = ts_node_child_by_field_name(c, "name", 4);
        if (ts_node_is_null(name_node))
            continue;
        char *fname = java_node_text(ctx, name_node);
        if (!fname)
            continue;
        cbm_scope_bind(ctx->current_scope, fname, static_type);
    }
}

static void process_method_decl(JavaLSPContext *ctx, TSNode node, const char *class_qn,
                                const char *super_qn) {
    /* Compute method QN. */
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *mname = java_node_text(ctx, name_node);
    if (!mname)
        return;
    char *method_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, mname);

    /* Save context. */
    const char *saved_method = ctx->enclosing_method_qn;
    const char *saved_class = ctx->enclosing_class_qn;
    const char *saved_super = ctx->enclosing_super_qn;
    CBMScope *saved_scope = ctx->current_scope;

    ctx->enclosing_method_qn = method_qn;
    ctx->enclosing_class_qn = class_qn;
    ctx->enclosing_super_qn = super_qn;
    ctx->current_scope = cbm_scope_push(ctx->arena, saved_scope);

    /* Bind formal parameters into scope. */
    TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
    if (!ts_node_is_null(params)) {
        uint32_t n = ts_node_named_child_count(params);
        for (uint32_t i = 0; i < n; i++) {
            TSNode p = ts_node_named_child(params, i);
            const char *pk = ts_node_type(p);
            if (strcmp(pk, "formal_parameter") != 0 && strcmp(pk, "spread_parameter") != 0 &&
                strcmp(pk, "receiver_parameter") != 0) {
                continue;
            }
            TSNode pname = ts_node_child_by_field_name(p, "name", 4);
            TSNode ptype = ts_node_child_by_field_name(p, "type", 4);
            if (ts_node_is_null(pname))
                continue;
            char *pn = java_node_text(ctx, pname);
            if (!pn)
                continue;
            const CBMType *pt =
                ts_node_is_null(ptype) ? cbm_type_unknown() : java_parse_type_node(ctx, ptype);
            if (strcmp(pk, "spread_parameter") == 0 && pt) {
                pt = cbm_type_slice(ctx->arena, pt);
            }
            cbm_scope_bind(ctx->current_scope, pn, pt);
        }
    }

    /* Bind `this` (only matters indirectly — `this` is a syntactic node, not
     * an identifier). */

    /* Walk method body. */
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (!ts_node_is_null(body)) {
        process_block(ctx, body);
    }

    /* Restore. */
    ctx->current_scope = saved_scope;
    ctx->enclosing_method_qn = saved_method;
    ctx->enclosing_class_qn = saved_class;
    ctx->enclosing_super_qn = saved_super;
}

static void process_constructor_decl(JavaLSPContext *ctx, TSNode node, const char *class_qn,
                                     const char *super_qn) {
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    char *cname = ts_node_is_null(name_node) ? NULL : java_node_text(ctx, name_node);
    /* Constructor QN convention: Class.<init> or Class.ClassShortName. The
     * extractor uses the short class name; mirror that. */
    char *ctor_qn;
    if (cname) {
        ctor_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, cname);
    } else {
        ctor_qn = cbm_arena_sprintf(ctx->arena, "%s.<init>", class_qn);
    }

    const char *saved_method = ctx->enclosing_method_qn;
    const char *saved_class = ctx->enclosing_class_qn;
    const char *saved_super = ctx->enclosing_super_qn;
    CBMScope *saved_scope = ctx->current_scope;

    ctx->enclosing_method_qn = ctor_qn;
    ctx->enclosing_class_qn = class_qn;
    ctx->enclosing_super_qn = super_qn;
    ctx->current_scope = cbm_scope_push(ctx->arena, saved_scope);

    TSNode params = ts_node_child_by_field_name(node, "parameters", 10);
    if (!ts_node_is_null(params)) {
        uint32_t n = ts_node_named_child_count(params);
        for (uint32_t i = 0; i < n; i++) {
            TSNode p = ts_node_named_child(params, i);
            if (strcmp(ts_node_type(p), "formal_parameter") != 0)
                continue;
            TSNode pname = ts_node_child_by_field_name(p, "name", 4);
            TSNode ptype = ts_node_child_by_field_name(p, "type", 4);
            if (ts_node_is_null(pname))
                continue;
            char *pn = java_node_text(ctx, pname);
            if (!pn)
                continue;
            const CBMType *pt =
                ts_node_is_null(ptype) ? cbm_type_unknown() : java_parse_type_node(ctx, ptype);
            cbm_scope_bind(ctx->current_scope, pn, pt);
        }
    }

    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (!ts_node_is_null(body))
        process_block(ctx, body);

    ctx->current_scope = saved_scope;
    ctx->enclosing_method_qn = saved_method;
    ctx->enclosing_class_qn = saved_class;
    ctx->enclosing_super_qn = saved_super;
}

/* Determine the class's super QN from the AST node. */
static const char *class_super_qn(JavaLSPContext *ctx, TSNode class_node) {
    TSNode super_node = ts_node_child_by_field_name(class_node, "superclass", 10);
    if (ts_node_is_null(super_node))
        return NULL;
    /* superclass node has shape `extends T`; T is the named child. */
    TSNode tnode = (TSNode){0};
    if (ts_node_named_child_count(super_node) > 0) {
        tnode = ts_node_named_child(super_node, 0);
    } else {
        tnode = super_node;
    }
    if (ts_node_is_null(tnode))
        return NULL;
    const CBMType *t = java_parse_type_node(ctx, tnode);
    if (t && t->kind == CBM_TYPE_NAMED)
        return t->data.named.qualified_name;
    if (t && t->kind == CBM_TYPE_TEMPLATE)
        return t->data.template_type.template_name;
    return NULL;
}

static void java_process_class_decl(JavaLSPContext *ctx, TSNode node) {
    TSNode name_node = ts_node_child_by_field_name(node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *cname = java_node_text(ctx, name_node);
    if (!cname)
        return;

    /* Class QN. Outer.Inner naming: walk the enclosing-class stack. */
    char *class_qn;
    if (ctx->enclosing_class_qn) {
        class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->enclosing_class_qn, cname);
    } else if (ctx->module_qn && ctx->module_qn[0]) {
        class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);
    } else {
        class_qn = cbm_arena_strdup(ctx->arena, cname);
    }

    const char *super_qn = class_super_qn(ctx, node);
    if (!super_qn)
        super_qn = "java.lang.Object";

    /* Save context. */
    const char *saved_class = ctx->enclosing_class_qn;
    const char *saved_super = ctx->enclosing_super_qn;
    const char *saved_short = ctx->enclosing_class_short;

    push_enclosing_class(ctx, class_qn);
    ctx->enclosing_class_qn = class_qn;
    ctx->enclosing_super_qn = super_qn;
    ctx->enclosing_class_short = cname;

    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (!ts_node_is_null(body)) {
        CBMScope *saved_scope = ctx->current_scope;
        ctx->current_scope = cbm_scope_push(ctx->arena, saved_scope);

        /* First pass: bind fields so methods see them. */
        uint32_t n = ts_node_named_child_count(body);
        for (uint32_t i = 0; i < n; i++) {
            TSNode c = ts_node_named_child(body, i);
            const char *k = ts_node_type(c);
            if (strcmp(k, "field_declaration") == 0)
                process_field_decl(ctx, c);
        }
        /* Second pass: methods + constructors + nested types. */
        for (uint32_t i = 0; i < n; i++) {
            TSNode c = ts_node_named_child(body, i);
            const char *k = ts_node_type(c);
            if (strcmp(k, "method_declaration") == 0) {
                process_method_decl(ctx, c, class_qn, super_qn);
            } else if (strcmp(k, "constructor_declaration") == 0) {
                process_constructor_decl(ctx, c, class_qn, super_qn);
            } else if (strcmp(k, "class_declaration") == 0 ||
                       strcmp(k, "interface_declaration") == 0 ||
                       strcmp(k, "enum_declaration") == 0 || strcmp(k, "record_declaration") == 0 ||
                       strcmp(k, "annotation_type_declaration") == 0) {
                java_process_class_decl(ctx, c);
            } else if (strcmp(k, "static_initializer") == 0) {
                if (ts_node_named_child_count(c) > 0) {
                    process_block(ctx, ts_node_named_child(c, 0));
                }
            }
        }

        ctx->current_scope = saved_scope;
    }

    pop_enclosing_class(ctx);
    ctx->enclosing_class_qn = saved_class;
    ctx->enclosing_super_qn = saved_super;
    ctx->enclosing_class_short = saved_short;
}

void java_lsp_process_file(JavaLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;
    /* First scan: package_declaration + imports (already pushed via init). */
    uint32_t n = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(root, i);
        const char *k = ts_node_type(c);
        if (strcmp(k, "package_declaration") == 0) {
            uint32_t pn = ts_node_named_child_count(c);
            if (pn > 0) {
                TSNode p = ts_node_named_child(c, 0);
                ctx->package_name = java_node_text(ctx, p);
            }
        } else if (strcmp(k, "import_declaration") == 0) {
            /* Flag `static` and on-demand. */
            bool is_static = false;
            bool is_on_demand = false;
            const char *path = NULL;
            uint32_t cn = ts_node_child_count(c);
            for (uint32_t j = 0; j < cn; j++) {
                TSNode cc = ts_node_child(c, j);
                if (!ts_node_is_named(cc)) {
                    char *t = java_node_text(ctx, cc);
                    if (t && strcmp(t, "static") == 0)
                        is_static = true;
                    if (t && strcmp(t, "*") == 0)
                        is_on_demand = true;
                    if (t && strcmp(t, "asterisk") == 0)
                        is_on_demand = true;
                } else {
                    const char *pk = ts_node_type(cc);
                    if (strcmp(pk, "asterisk") == 0)
                        is_on_demand = true;
                    else if (strcmp(pk, "scoped_identifier") == 0 ||
                             strcmp(pk, "identifier") == 0) {
                        path = java_node_text(ctx, cc);
                    }
                }
            }
            if (path) {
                if (is_on_demand) {
                    int kind = is_static ? CBM_JAVA_IMPORT_STATIC_OD : CBM_JAVA_IMPORT_ON_DEMAND;
                    java_lsp_add_import(ctx, "*", path, kind);
                } else if (is_static) {
                    /* Static import: target is fully-qualified — last segment
                     * is the imported member. */
                    const char *last_dot = strrchr(path, '.');
                    const char *local = last_dot ? last_dot + 1 : path;
                    java_lsp_add_import(ctx, local, path, CBM_JAVA_IMPORT_STATIC);
                } else {
                    const char *last_dot = strrchr(path, '.');
                    const char *local = last_dot ? last_dot + 1 : path;
                    java_lsp_add_import(ctx, local, path, CBM_JAVA_IMPORT_TYPE);
                }
            }
        }
    }

    /* Second scan: top-level type declarations. */
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(root, i);
        const char *k = ts_node_type(c);
        if (strcmp(k, "class_declaration") == 0 || strcmp(k, "interface_declaration") == 0 ||
            strcmp(k, "enum_declaration") == 0 || strcmp(k, "record_declaration") == 0 ||
            strcmp(k, "annotation_type_declaration") == 0) {
            java_process_class_decl(ctx, c);
        }
    }
}

/* ── Call-edge resolution ─────────────────────────────────────────── */

static void java_emit_resolved_orig(JavaLSPContext *ctx, const char *callee_qn, const char *orig,
                                    const char *strategy, float confidence) {
    if (!ctx->resolved_calls || !ctx->enclosing_method_qn || !callee_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_method_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    // For a data-flow resolution (constructor reference `Lhs::new` resolved to
    // the Lhs class), `reason` carries the ORIGINAL textual callee (`new`) so the
    // pipeline join can match the textual call site even though the resolved
    // callee_qn's short name differs. NULL/unread for normal resolved calls.
    rc.reason = orig;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

static void java_emit_resolved(JavaLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence) {
    java_emit_resolved_orig(ctx, callee_qn, NULL, strategy, confidence);
}

static void java_emit_unresolved(JavaLSPContext *ctx, const char *expr_text, const char *reason) {
    if (!ctx->resolved_calls || !ctx->enclosing_method_qn)
        return;
    CBMResolvedCall rc;
    rc.caller_qn = ctx->enclosing_method_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

/* Find a sole concrete in-project implementer of interface `iface_qn` that
 * declares method `mname`. Returns the implementer's QN when exactly ONE
 * exists (else NULL), and sets *out_count to the number found (capped at 2,
 * so 2 means "two or more"). Walks the registered-type parent chain to
 * confirm true subtyping. Mirrors the inline detection that used to live in
 * resolve_method_call so both the f-found and f-absent interface paths share
 * identical semantics. */
static const char *java_find_sole_impl(JavaLSPContext *ctx, const char *iface_qn, const char *mname,
                                       int *out_count) {
    const char *first = NULL; /* first distinct impl QN seen */
    int distinct = 0;         /* distinct impl classes (capped at 2) */
    const char *iface_dot = strrchr(iface_qn, '.');
    const char *iface_bare = iface_dot ? iface_dot + 1 : iface_qn;
    for (int ti = 0; ti < ctx->registry->type_count && distinct < 2; ti++) {
        const CBMRegisteredType *cand = &ctx->registry->types[ti];
        if (cand->is_interface || !cand->qualified_name || cand->alias_of)
            continue;
        /* Does cand declare `mname`? The method-name array is often empty for
         * fixture classes; the method REGISTRY is the authoritative source the
         * dispatch path already uses, so consult it first and fall back to the
         * name array. */
        bool has = cbm_registry_lookup_method(ctx->registry, cand->qualified_name, mname) != NULL;
        if (!has && cand->method_names) {
            for (int mi = 0; cand->method_names[mi]; mi++) {
                if (strcmp(cand->method_names[mi], mname) == 0) {
                    has = true;
                    break;
                }
            }
        }
        if (!has)
            continue;
        /* Subtype check: walk cand's supertype chain, matching iface by FULL
         * QN or BARE name. The registry holds duplicate type entries whose
         * `embedded_types` list a supertype sometimes by short name ("Shape")
         * and sometimes by full QN ("proj.Shape"); a full-QN-only comparison
         * silently misses the short-name form, so compare both. */
        const char *cur = cand->qualified_name;
        bool subtype = false;
        for (int hops = 0; hops < JAVA_LSP_MAX_INHERIT_HOPS && cur && !subtype; hops++) {
            const CBMRegisteredType *ct = cbm_registry_lookup_type(ctx->registry, cur);
            if (!ct || !ct->embedded_types)
                break;
            const char *next = NULL;
            for (int pi = 0; ct->embedded_types[pi]; pi++) {
                const char *e = ct->embedded_types[pi];
                const char *edot = strrchr(e, '.');
                const char *ebare = edot ? edot + 1 : e;
                if (strcmp(e, iface_qn) == 0 || strcmp(ebare, iface_bare) == 0) {
                    subtype = true;
                    break;
                }
                if (!next)
                    next = e; /* first supertype → continue the walk upward */
            }
            cur = next;
        }
        if (!subtype)
            continue;
        /* Count DISTINCT impl classes: the registry duplicates entries per
         * class, so dedup by QN — two entries of one class must not read as
         * two implementers. */
        if (!first) {
            first = cand->qualified_name;
            distinct = 1;
        } else if (strcmp(first, cand->qualified_name) != 0) {
            distinct = 2;
        }
    }
    if (out_count)
        *out_count = distinct;
    return distinct == 1 ? first : NULL;
}

/* Emit the resolution for an interface-typed receiver `iface_qn` calling
 * `mname`: a sole concrete in-project impl → lsp_interface_resolve (resolved
 * to that impl's method, with a synthesized QN when the method isn't in the
 * method registry); two-or-more impls → lsp_interface_dispatch on a synthesized
 * iface-qualified target. Returns true when it emitted (caller should return),
 * false when there is NO in-project implementer (impl_count == 0) so the caller
 * can fall back to dispatching on the interface's own method — this keeps JDK
 * interface calls (List/Stream/Predicate, no in-project impl) resolving via the
 * strict type_dispatch path instead of being downgraded to interface_dispatch. */
static bool java_emit_interface_resolution(JavaLSPContext *ctx, const char *iface_qn,
                                           const char *mname) {
    int impl_count = 0;
    const char *sole_impl = java_find_sole_impl(ctx, iface_qn, mname, &impl_count);
    if (impl_count == 1 && sole_impl) {
        const CBMRegisteredFunc *cf = cbm_registry_lookup_method(ctx->registry, sole_impl, mname);
        const char *target =
            cf ? cf->qualified_name : cbm_arena_sprintf(ctx->arena, "%s.%s", sole_impl, mname);
        java_emit_resolved(ctx, target, "lsp_interface_resolve", 0.85f);
        return true;
    }
    if (impl_count >= 2) {
        java_emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", iface_qn, mname),
                           "lsp_interface_dispatch", 0.80f);
        return true;
    }
    return false; /* impl_count == 0: caller falls back to type_dispatch. */
}

static void resolve_method_call(JavaLSPContext *ctx, TSNode call) {
    TSNode obj = ts_node_child_by_field_name(call, "object", 6);
    TSNode name_node = ts_node_child_by_field_name(call, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *mname = java_node_text(ctx, name_node);
    if (!mname)
        return;
    int arity = count_call_args(call);

    /* Bare call: `foo()`. */
    if (ts_node_is_null(obj)) {
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredFunc *f =
                java_lookup_method(ctx, ctx->enclosing_class_qn, mname, arity);
            if (f) {
                const char *strategy = "lsp_type_dispatch";
                if (f->receiver_type && strcmp(f->receiver_type, ctx->enclosing_class_qn) != 0) {
                    strategy = "lsp_inherited_dispatch";
                }
                java_emit_resolved(ctx, f->qualified_name, strategy, 0.95f);
                return;
            }
        }
        /* JLS §15.12.1: a bare call inside an inner class also looks up the
         * call against each enclosing-class scope. Walk outer classes. */
        for (int i = ctx->enclosing_class_depth - 2; i >= 0; i--) {
            const char *outer = ctx->enclosing_class_stack[i];
            if (!outer)
                continue;
            const CBMRegisteredFunc *f = java_lookup_method(ctx, outer, mname, arity);
            if (f) {
                java_emit_resolved(ctx, f->qualified_name, "lsp_outer_dispatch", 0.92f);
                return;
            }
        }
        /* Static import. */
        for (int i = 0; i < ctx->import_count; i++) {
            if (ctx->import_kinds[i] != CBM_JAVA_IMPORT_STATIC)
                continue;
            if (strcmp(ctx->import_local_names[i], mname) != 0)
                continue;
            const char *target = ctx->import_target_qns[i];
            const char *last_dot = strrchr(target, '.');
            if (!last_dot)
                continue;
            char *cls = cbm_arena_strndup(ctx->arena, target, (size_t)(last_dot - target));
            const CBMRegisteredFunc *f = java_lookup_method(ctx, cls, mname, arity);
            if (!f && ctx->registry) {
                /* The import is written package-qualified ("demo.Util"), but the
                 * class is registered under the project/directory QN
                 * ("<proj>.Util") when the `package` declaration and the file's
                 * directory differ. Resolve the import's class by its short name
                 * against the registry and retry — preferring an in-module match.
                 * Mirrors the C++ short-name type fallback. */
                const char *cls_dot = strrchr(cls, '.');
                const char *cls_short = cls_dot ? cls_dot + 1 : cls;
                size_t sl = strlen(cls_short);
                for (int ti = 0; ti < ctx->registry->type_count && !f; ti++) {
                    const char *q = ctx->registry->types[ti].qualified_name;
                    if (!q) {
                        continue;
                    }
                    size_t ql = strlen(q);
                    if (ql > sl + 1 && q[ql - sl - 1] == '.' &&
                        strcmp(q + ql - sl, cls_short) == 0) {
                        f = java_lookup_method(ctx, q, mname, arity);
                    }
                }
            }
            if (f) {
                java_emit_resolved(ctx, f->qualified_name, "lsp_static_import", 0.92f);
                return;
            }
            /* Couldn't find the method — emit the qualified path anyway so
             * the registry-name resolver can pick it up. */
            java_emit_resolved(ctx, target, "lsp_static_import_text", 0.80f);
            return;
        }
        /* Fall back to default: emit unresolved. */
        java_emit_unresolved(ctx, mname, "no_enclosing_class");
        return;
    }

    /* `super.method()` */
    if (strcmp(ts_node_type(obj), "super") == 0) {
        const char *super_qn =
            ctx->enclosing_super_qn ? ctx->enclosing_super_qn : "java.lang.Object";
        const CBMRegisteredFunc *f = java_lookup_method(ctx, super_qn, mname, arity);
        if (f) {
            java_emit_resolved(ctx, f->qualified_name, "lsp_super_dispatch", 0.95f);
            return;
        }
        java_emit_unresolved(ctx, mname, "super_no_match");
        return;
    }

    /* `this.method()` */
    if (strcmp(ts_node_type(obj), "this") == 0) {
        if (ctx->enclosing_class_qn) {
            const CBMRegisteredFunc *f =
                java_lookup_method(ctx, ctx->enclosing_class_qn, mname, arity);
            if (f) {
                java_emit_resolved(ctx, f->qualified_name, "lsp_this_dispatch", 0.95f);
                return;
            }
        }
        java_emit_unresolved(ctx, mname, "this_no_match");
        return;
    }

    /* Static call via classname. */
    if (strcmp(ts_node_type(obj), "identifier") == 0) {
        char *oname = java_node_text(ctx, obj);
        if (oname && cbm_type_is_unknown(cbm_scope_lookup(ctx->current_scope, oname))) {
            const char *cls_qn = java_resolve_type_name(ctx, oname);
            if (cls_qn) {
                const CBMRegisteredFunc *f = java_lookup_method(ctx, cls_qn, mname, arity);
                if (f) {
                    java_emit_resolved(ctx, f->qualified_name, "lsp_static_call", 0.95f);
                    return;
                }
            }
        }
    }

    /* Instance dispatch. */
    const CBMType *recv = java_eval_expr_type(ctx, obj);
    const CBMType *base = recv;
    const char *recv_qn = NULL;
    if (base && base->kind == CBM_TYPE_NAMED)
        recv_qn = base->data.named.qualified_name;
    else if (base && base->kind == CBM_TYPE_TEMPLATE)
        recv_qn = base->data.template_type.template_name;

    if (recv_qn) {
        const CBMRegisteredFunc *f = java_lookup_method(ctx, recv_qn, mname, arity);
        if (f) {
            /* When the receiver is an interface, java_lookup_method finds the
             * interface's OWN (abstract/default) method. Prefer resolving to a
             * sole concrete in-project implementer first; only fall through to
             * type_dispatch on the interface method when there is NO in-project
             * impl (e.g. JDK List/Stream/Predicate), keeping those strict. */
            const CBMRegisteredType *rt0 = cbm_registry_lookup_type(ctx->registry, recv_qn);
            if (rt0 && rt0->is_interface && java_emit_interface_resolution(ctx, recv_qn, mname)) {
                return;
            }
            const char *strategy = "lsp_type_dispatch";
            if (f->receiver_type && strcmp(f->receiver_type, recv_qn) != 0) {
                strategy = "lsp_inherited_dispatch";
            }
            java_emit_resolved(ctx, f->qualified_name, strategy, 0.95f);
            return;
        }
        /* Interface dispatch with no directly-registered method: resolve to a
         * sole concrete impl, else a synthesized iface-qualified dispatch. */
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, recv_qn);
        if (rt && rt->is_interface) {
            if (java_emit_interface_resolution(ctx, recv_qn, mname)) {
                return;
            }
            java_emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", recv_qn, mname),
                               "lsp_interface_dispatch", 0.80f);
            return;
        }
        java_emit_unresolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", recv_qn, mname),
                             "no_method_match");
        return;
    }

    java_emit_unresolved(ctx, mname, "no_receiver_type");
}

/* ── Functional-interface SAM table + lambda inference ──────────────
 *
 * Java functional interfaces define a Single Abstract Method (SAM); when a
 * lambda or method reference is passed as that arg, the lambda's parameter
 * types come from the SAM's parameter types after generic substitution
 * from the receiver context (JLS §15.27.3). Without this, lambda-body type
 * resolution degrades to UNKNOWN — the single biggest gap vs JDT-LS.
 *
 * Strategy:
 *   - Hand-coded table of common functional interfaces → (sam_name, arity).
 *   - When walking a method_invocation, check each arg position; if the
 *     resolved method's param-type at that position is a known FI, bind
 *     the lambda's formal_parameters to the SAM's parameter types
 *     (substituted from the receiver's template args), then recursively
 *     walk the lambda body in that scope.
 *
 * Coverage: java.util.function.* (21 entries), Runnable, Comparable,
 * Comparator, Iterable.iterator (rarely used as a SAM target but cheap
 * to include), Callable (java.util.concurrent). Adding more is one-line. */

typedef struct {
    const char *qn;  /* fully-qualified functional interface */
    const char *sam; /* the SAM method's short name */
    int arity;       /* number of params on the SAM */
} JavaSAMSpec;

static const JavaSAMSpec JAVA_SAM_TABLE[] = {
    {"java.util.function.Function", "apply", 1},
    {"java.util.function.BiFunction", "apply", 2},
    {"java.util.function.UnaryOperator", "apply", 1},
    {"java.util.function.BinaryOperator", "apply", 2},
    {"java.util.function.Predicate", "test", 1},
    {"java.util.function.BiPredicate", "test", 2},
    {"java.util.function.Consumer", "accept", 1},
    {"java.util.function.BiConsumer", "accept", 2},
    {"java.util.function.Supplier", "get", 0},
    {"java.util.function.IntFunction", "apply", 1},
    {"java.util.function.LongFunction", "apply", 1},
    {"java.util.function.DoubleFunction", "apply", 1},
    {"java.util.function.IntPredicate", "test", 1},
    {"java.util.function.LongPredicate", "test", 1},
    {"java.util.function.DoublePredicate", "test", 1},
    {"java.util.function.IntConsumer", "accept", 1},
    {"java.util.function.LongConsumer", "accept", 1},
    {"java.util.function.DoubleConsumer", "accept", 1},
    {"java.util.function.IntSupplier", "getAsInt", 0},
    {"java.util.function.LongSupplier", "getAsLong", 0},
    {"java.util.function.DoubleSupplier", "getAsDouble", 0},
    {"java.util.function.BooleanSupplier", "getAsBoolean", 0},
    {"java.util.function.ToIntFunction", "applyAsInt", 1},
    {"java.util.function.ToLongFunction", "applyAsLong", 1},
    {"java.util.function.ToDoubleFunction", "applyAsDouble", 1},
    {"java.lang.Runnable", "run", 0},
    {"java.util.Comparator", "compare", 2},
    {"java.lang.Comparable", "compareTo", 1},
    {"java.util.concurrent.Callable", "call", 0},
    {NULL, NULL, 0},
};

static const JavaSAMSpec *find_sam(const char *qn) {
    if (!qn)
        return NULL;
    for (int i = 0; JAVA_SAM_TABLE[i].qn; i++) {
        if (strcmp(JAVA_SAM_TABLE[i].qn, qn) == 0)
            return &JAVA_SAM_TABLE[i];
    }
    return NULL;
}

/* Compute the SAM's `param_idx`-th parameter type for a functional interface
 * `fi_qn` parameterized with `targs`. Returns cbm_type_unknown() when the
 * parametric mapping isn't modeled (e.g. specialized primitive variants). */
static const CBMType *sam_param_type(CBMArena *a, const char *fi_qn, int param_idx,
                                     const CBMType *const *targs, int targ_count) {
    if (!fi_qn)
        return cbm_type_unknown();

    /* Single-template-arg interfaces where SAM uses targ[0]. */
    static const char *single_t[] = {
        "java.util.function.Function",
        "java.util.function.UnaryOperator",
        "java.util.function.Predicate",
        "java.util.function.Consumer",
        "java.util.function.IntFunction",
        "java.util.function.LongFunction",
        "java.util.function.DoubleFunction",
        "java.util.function.ToIntFunction",
        "java.util.function.ToLongFunction",
        "java.util.function.ToDoubleFunction",
        NULL,
    };
    for (int i = 0; single_t[i]; i++) {
        if (strcmp(single_t[i], fi_qn) == 0 && param_idx == 0 && targ_count >= 1) {
            return targs[0] ? targs[0] : cbm_type_unknown();
        }
    }
    /* Two-template-arg, two-param interfaces — params map to targs[0], targs[1]. */
    if (strcmp(fi_qn, "java.util.function.BiFunction") == 0 ||
        strcmp(fi_qn, "java.util.function.BiPredicate") == 0 ||
        strcmp(fi_qn, "java.util.function.BiConsumer") == 0 ||
        strcmp(fi_qn, "java.util.function.BinaryOperator") == 0) {
        if (param_idx <= 1 && targ_count > param_idx) {
            return targs[param_idx] ? targs[param_idx] : cbm_type_unknown();
        }
    }
    /* Comparator<T>.compare(T, T) — both params share targ[0]. */
    if (strcmp(fi_qn, "java.util.Comparator") == 0) {
        if (param_idx <= 1 && targ_count >= 1) {
            return targs[0] ? targs[0] : cbm_type_unknown();
        }
    }
    /* Comparable<T>.compareTo(T) — single param of type targ[0]. */
    if (strcmp(fi_qn, "java.lang.Comparable") == 0) {
        if (param_idx == 0 && targ_count >= 1) {
            return targs[0] ? targs[0] : cbm_type_unknown();
        }
    }
    /* Primitive-specialized interfaces — SAM param is a Java primitive. */
    if (strcmp(fi_qn, "java.util.function.IntPredicate") == 0 ||
        strcmp(fi_qn, "java.util.function.IntConsumer") == 0) {
        return cbm_type_builtin(a, "int");
    }
    if (strcmp(fi_qn, "java.util.function.LongPredicate") == 0 ||
        strcmp(fi_qn, "java.util.function.LongConsumer") == 0) {
        return cbm_type_builtin(a, "long");
    }
    if (strcmp(fi_qn, "java.util.function.DoublePredicate") == 0 ||
        strcmp(fi_qn, "java.util.function.DoubleConsumer") == 0) {
        return cbm_type_builtin(a, "double");
    }
    return cbm_type_unknown();
}

/* Heuristic table mapping (receiver kind, method name) → lambda arg shape.
 *
 * Used when the registry's CBMRegisteredFunc lacks param-type metadata
 * (most stdlib registrations are return-only) but the receiver's template
 * args make the SAM-arg type unambiguous.
 *
 * Returns true and fills out_arity + out_param[0..1] when a heuristic
 * matches. The lambda binder then uses these directly without going
 * through the SAM-spec table. */
static bool method_implies_lambda_args(const char *recv_qn, const char *method_name,
                                       const CBMType *const *targs, int targ_count, int *out_arity,
                                       const CBMType **out_param0, const CBMType **out_param1) {
    *out_arity = 0;
    *out_param0 = NULL;
    *out_param1 = NULL;
    if (!recv_qn || !method_name || !targs || targ_count <= 0)
        return false;

    /* 1-param lambdas over T0 — Predicate / Consumer / Function shapes. */
    static const char *one_arg_methods[] = {
        "forEach",         "filter",   "map",       "flatMap",     "peek",      "removeIf",
        "anyMatch",        "allMatch", "noneMatch", "takeWhile",   "dropWhile", "ifPresent",
        "ifPresentOrElse", "mapToInt", "mapToLong", "mapToDouble", "filter",    NULL,
    };
    for (int i = 0; one_arg_methods[i]; i++) {
        if (strcmp(method_name, one_arg_methods[i]) == 0) {
            *out_arity = 1;
            *out_param0 = targs[0];
            return true;
        }
    }
    /* Map<K,V>.forEach takes BiConsumer<K, V> (2 params). */
    if (is_map_like(recv_qn) && targ_count >= 2) {
        if (strcmp(method_name, "forEach") == 0 || strcmp(method_name, "replaceAll") == 0 ||
            strcmp(method_name, "compute") == 0 || strcmp(method_name, "computeIfPresent") == 0 ||
            strcmp(method_name, "merge") == 0) {
            *out_arity = 2;
            *out_param0 = targs[0];
            *out_param1 = targs[1];
            return true;
        }
        if (strcmp(method_name, "computeIfAbsent") == 0) {
            /* Function<K, V> — 1-arg of K. */
            *out_arity = 1;
            *out_param0 = targs[0];
            return true;
        }
    }
    /* Comparator-typed args — keep simple: Comparator<T0>.compare(T0, T0). */
    if (strcmp(method_name, "sort") == 0 && targ_count >= 1) {
        *out_arity = 2;
        *out_param0 = targs[0];
        *out_param1 = targs[0];
        return true;
    }
    return false;
}

/* When a method on a TEMPLATE receiver returns a NAMED type that is itself
 * a parametric "carrier" (Stream, Iterator, Optional, …), preserve the
 * receiver's element type so chained lambda inference keeps working.
 *
 * This is the fix for `xs.stream().filter(x -> x.foo())` losing track of
 * String inside the filter lambda — without propagation, stream() would
 * return bare NAMED(Stream) with no template args, and the SAM binder
 * couldn't substitute T0. */
static const CBMType *propagate_template(CBMArena *a, const char *recv_qn, const char *method_name,
                                         const CBMType *const *recv_targs, int recv_targ_count,
                                         const CBMType *return_t) {
    if (!return_t || return_t->kind != CBM_TYPE_NAMED)
        return return_t;
    if (recv_targ_count <= 0 || !recv_targs)
        return return_t;
    const char *ret_qn = return_t->data.named.qualified_name;
    if (!ret_qn)
        return return_t;

    /* Carriers that preserve T0. */
    static const char *t0_carriers[] = {
        "java.util.stream.Stream", "java.util.Iterator",   "java.util.ListIterator",
        "java.util.Spliterator",   "java.util.Optional",   "java.util.List",
        "java.util.Set",           "java.util.Collection", "java.lang.Iterable",
        "java.util.Queue",         "java.util.Deque",      NULL,
    };
    bool is_t0 = false;
    for (int i = 0; t0_carriers[i]; i++) {
        if (strcmp(t0_carriers[i], ret_qn) == 0) {
            is_t0 = true;
            break;
        }
    }
    if (is_t0) {
        const CBMType **args = (const CBMType **)cbm_arena_alloc(a, 2 * sizeof(*args));
        if (!args)
            return return_t;
        args[0] = recv_targs[0];
        args[1] = NULL;
        return cbm_type_template(a, ret_qn, args, 1);
    }

    /* Map.keySet → Set<K>, Map.values → Collection<V>, Map.entrySet → Set<Entry<K,V>>. */
    if (is_map_like(recv_qn) && recv_targ_count >= 2) {
        const CBMType **args = (const CBMType **)cbm_arena_alloc(a, 2 * sizeof(*args));
        if (!args)
            return return_t;
        if (strcmp(method_name, "keySet") == 0) {
            args[0] = recv_targs[0];
            args[1] = NULL;
            return cbm_type_template(a, "java.util.Set", args, 1);
        }
        if (strcmp(method_name, "values") == 0) {
            args[0] = recv_targs[1];
            args[1] = NULL;
            return cbm_type_template(a, "java.util.Collection", args, 1);
        }
    }
    return return_t;
}

/* Given a method-invocation node and its resolved CBMRegisteredFunc, walk
 * each lambda argument: bind its formal_parameters to the SAM's parameter
 * types (with generic substitution from the receiver's template args), then
 * resolve calls inside the lambda body against the freshly-bound scope.
 *
 * Returns a bitmask of arg indices that were handled here so the generic
 * walker can skip them. */
static uint32_t bind_lambda_args(JavaLSPContext *ctx, TSNode call_node,
                                 const CBMRegisteredFunc *resolved, const CBMType *recv_type) {
    uint32_t handled_mask = 0;
    TSNode args_node = ts_node_child_by_field_name(call_node, "arguments", 9);
    if (ts_node_is_null(args_node))
        return handled_mask;
    const CBMType *const *param_types = NULL;
    if (resolved && resolved->signature && resolved->signature->kind == CBM_TYPE_FUNC) {
        param_types = resolved->signature->data.func.param_types;
    }
    /* param_types is NULL-terminated with the DECLARED param count — the call
     * site may pass MORE arguments (overload mismatch, varargs). Indexing by
     * the raw argument index read past the terminator and dereferenced
     * whatever followed in the arena (elasticsearch SIGSEGV; same OOB family
     * as #427). Bound every access by the array's own length. */
    int param_type_count = 0;
    if (param_types) {
        while (param_types[param_type_count]) {
            param_type_count++;
        }
    }
    /* Even without registry param_types, the heuristic (recv_qn + method_name
     * → arg shape) often pins the lambda type — that's the path that
     * handles `xs.forEach(x -> ...)` and `xs.stream().filter(x -> ...)`
     * given that stdlib registrations don't model arg types. */

    /* Gather receiver template args for substitution. */
    const CBMType *const *recv_targs = NULL;
    int recv_targ_count = 0;
    const char *recv_qn = NULL;
    if (recv_type && recv_type->kind == CBM_TYPE_TEMPLATE) {
        recv_targs = recv_type->data.template_type.template_args;
        recv_targ_count = recv_type->data.template_type.arg_count;
        recv_qn = recv_type->data.template_type.template_name;
    } else if (recv_type && recv_type->kind == CBM_TYPE_NAMED) {
        recv_qn = recv_type->data.named.qualified_name;
    }

    /* Method name (for heuristic). */
    TSNode mname_node = ts_node_child_by_field_name(call_node, "name", 4);
    char *mname = ts_node_is_null(mname_node) ? NULL : java_node_text(ctx, mname_node);

    uint32_t n = ts_node_named_child_count(args_node);
    for (uint32_t i = 0; i < n && i < 32; i++) {
        TSNode arg = ts_node_named_child(args_node, i);
        const char *kind = ts_node_type(arg);
        if (strcmp(kind, "lambda_expression") != 0)
            continue;

        /* First try registry-driven SAM inference. */
        const CBMType *expected =
            (param_types && i < (uint32_t)param_type_count) ? param_types[i] : NULL;
        if (!expected && recv_qn && mname && recv_targ_count > 0) {
            /* Heuristic path: bind lambda directly using receiver template
             * args + recognized method name, skipping the SAM table. */
            int h_arity = 0;
            const CBMType *h_p0 = NULL;
            const CBMType *h_p1 = NULL;
            if (method_implies_lambda_args(recv_qn, mname, recv_targs, recv_targ_count, &h_arity,
                                           &h_p0, &h_p1)) {
                TSNode params_node = ts_node_child_by_field_name(arg, "parameters", 10);
                TSNode body_node = ts_node_child_by_field_name(arg, "body", 4);
                if (!ts_node_is_null(body_node)) {
                    CBMScope *saved = ctx->current_scope;
                    ctx->current_scope = cbm_scope_push(ctx->arena, saved);

                    if (!ts_node_is_null(params_node)) {
                        const char *pn_kind = ts_node_type(params_node);
                        if (strcmp(pn_kind, "identifier") == 0) {
                            char *pname = java_node_text(ctx, params_node);
                            if (pname)
                                cbm_scope_bind(ctx->current_scope, pname, h_p0);
                        } else if (strcmp(pn_kind, "inferred_parameters") == 0 ||
                                   strcmp(pn_kind, "formal_parameters") == 0) {
                            uint32_t pc = ts_node_named_child_count(params_node);
                            int idx = 0;
                            for (uint32_t pi = 0; pi < pc; pi++) {
                                TSNode p = ts_node_named_child(params_node, pi);
                                const char *pk = ts_node_type(p);
                                char *pname = NULL;
                                const CBMType *pt = (idx == 0) ? h_p0 : h_p1;
                                if (strcmp(pk, "identifier") == 0) {
                                    pname = java_node_text(ctx, p);
                                } else if (strcmp(pk, "formal_parameter") == 0) {
                                    TSNode pnname = ts_node_child_by_field_name(p, "name", 4);
                                    TSNode pntype = ts_node_child_by_field_name(p, "type", 4);
                                    if (!ts_node_is_null(pnname))
                                        pname = java_node_text(ctx, pnname);
                                    if (!ts_node_is_null(pntype)) {
                                        pt = java_parse_type_node(ctx, pntype);
                                    }
                                }
                                if (pname) {
                                    cbm_scope_bind(ctx->current_scope, pname,
                                                   pt ? pt : cbm_type_unknown());
                                    idx++;
                                }
                            }
                        }
                    }
                    java_resolve_calls_in_node(ctx, body_node);
                    ctx->current_scope = saved;
                    handled_mask |= ((uint32_t)1u << i);
                }
                continue;
            }
        }
        if (!expected)
            continue;

        /* Strip TEMPLATE wrapper to find the FI QN + per-call template
         * args. Note: the FI's own template args may be type variables
         * that need substitution from the receiver. */
        const char *fi_qn = NULL;
        const CBMType *const *fi_targs = NULL;
        int fi_targ_count = 0;
        if (expected->kind == CBM_TYPE_NAMED) {
            fi_qn = expected->data.named.qualified_name;
        } else if (expected->kind == CBM_TYPE_TEMPLATE) {
            fi_qn = expected->data.template_type.template_name;
            fi_targs = expected->data.template_type.template_args;
            fi_targ_count = expected->data.template_type.arg_count;
        }
        if (!fi_qn)
            continue;
        const JavaSAMSpec *sam = find_sam(fi_qn);
        if (!sam)
            continue;

        /* If the FI's targs are type-variables (TYPE_PARAM kind) referring to
         * the receiver's parameters, substitute them. We use the shape: when
         * the receiver is e.g. List<String> and the method signature has
         * Predicate<E>, the FI's E maps to receiver's E = String. The
         * registry's stdlib doesn't carry that link explicitly — so we
         * apply a heuristic: if receiver has 1 template arg and the FI has
         * a single targ that's a TYPE_PARAM, substitute receiver's targ[0].
         * For Map<K,V>-like receivers the right substitution depends on
         * the SAM context (forEach uses (K,V)) — we handle the few common
         * cases below. */
        const CBMType *resolved_fi_targs[8] = {0};
        int resolved_count = fi_targ_count;
        for (int j = 0; j < fi_targ_count && j < 8; j++) {
            resolved_fi_targs[j] = fi_targs[j];
        }
        /* When the FI was passed without explicit targs, fall back to the
         * receiver's first template arg as a heuristic single-T substitution. */
        if (fi_targ_count == 0 && recv_targ_count > 0 &&
            (sam->arity == 1 || strcmp(sam->qn, "java.util.function.BiFunction") == 0 ||
             strcmp(sam->qn, "java.util.function.BiConsumer") == 0)) {
            resolved_fi_targs[0] = recv_targs[0];
            resolved_count = 1;
            if (sam->arity == 2 && recv_targ_count >= 2) {
                resolved_fi_targs[1] = recv_targs[1];
                resolved_count = 2;
            }
        }

        /* Find the lambda's parameter list. tree-sitter exposes
         * `parameters` field for inferred-formal_parameters or a single
         * identifier shorthand `x -> body`. */
        TSNode params_node = ts_node_child_by_field_name(arg, "parameters", 10);

        /* Find the body. */
        TSNode body_node = ts_node_child_by_field_name(arg, "body", 4);
        if (ts_node_is_null(body_node))
            continue;

        CBMScope *saved = ctx->current_scope;
        ctx->current_scope = cbm_scope_push(ctx->arena, saved);

        if (!ts_node_is_null(params_node)) {
            const char *pn_kind = ts_node_type(params_node);
            if (strcmp(pn_kind, "identifier") == 0) {
                /* Shorthand `x -> body` */
                char *pname = java_node_text(ctx, params_node);
                if (pname) {
                    const CBMType *pt =
                        sam_param_type(ctx->arena, fi_qn, 0, resolved_fi_targs, resolved_count);
                    cbm_scope_bind(ctx->current_scope, pname, pt);
                }
            } else if (strcmp(pn_kind, "inferred_parameters") == 0 ||
                       strcmp(pn_kind, "formal_parameters") == 0) {
                uint32_t pc = ts_node_named_child_count(params_node);
                int idx = 0;
                for (uint32_t pi = 0; pi < pc; pi++) {
                    TSNode p = ts_node_named_child(params_node, pi);
                    const char *pk = ts_node_type(p);
                    char *pname = NULL;
                    const CBMType *pt = NULL;
                    if (strcmp(pk, "identifier") == 0) {
                        pname = java_node_text(ctx, p);
                        pt = sam_param_type(ctx->arena, fi_qn, idx, resolved_fi_targs,
                                            resolved_count);
                    } else if (strcmp(pk, "formal_parameter") == 0) {
                        TSNode pnname = ts_node_child_by_field_name(p, "name", 4);
                        TSNode pntype = ts_node_child_by_field_name(p, "type", 4);
                        if (!ts_node_is_null(pnname))
                            pname = java_node_text(ctx, pnname);
                        if (!ts_node_is_null(pntype))
                            pt = java_parse_type_node(ctx, pntype);
                        else
                            pt = sam_param_type(ctx->arena, fi_qn, idx, resolved_fi_targs,
                                                resolved_count);
                    }
                    if (pname) {
                        cbm_scope_bind(ctx->current_scope, pname, pt ? pt : cbm_type_unknown());
                        idx++;
                    }
                }
            }
        }

        /* Walk the body in this enriched scope. */
        java_resolve_calls_in_node(ctx, body_node);

        ctx->current_scope = saved;
        handled_mask |= ((uint32_t)1u << i);
    }
    return handled_mask;
}

/* Resolve a method reference (Class::method or instance::method) to a
 * concrete callee, using the surrounding SAM context for arg-count.
 *
 * Emits a CBMResolvedCall edge for the referenced method when we can pin
 * down the receiver type. Otherwise emits an unresolved diagnostic. */
static void resolve_method_reference(JavaLSPContext *ctx, TSNode mref,
                                     const CBMRegisteredFunc *outer_resolved, int arg_index,
                                     const CBMType *recv_type) {
    if (ts_node_is_null(mref))
        return;
    /* method_reference shape: lhs `::` name. tree-sitter-java exposes the
     * LHS as a named child; the method-name token may be a named identifier
     * OR an unnamed `new` keyword (for constructor references like
     * `StringBuilder::new`). Handle both. */
    uint32_t nc_named = ts_node_named_child_count(mref);
    if (nc_named < 1)
        return;
    TSNode lhs = ts_node_named_child(mref, 0);

    /* Try the last named child first; if it's the same as the LHS (only one
     * named child total), we have a constructor ref where `new` is unnamed. */
    char *mname = NULL;
    if (nc_named >= 2) {
        TSNode name_node = ts_node_named_child(mref, nc_named - 1);
        mname = java_node_text(ctx, name_node);
    }
    if (!mname || !mname[0]) {
        /* Fall back to scanning all (named + unnamed) children for the token
         * after `::`. */
        uint32_t total = ts_node_child_count(mref);
        for (uint32_t i = 0; i < total; i++) {
            TSNode c = ts_node_child(mref, i);
            const char *ck = ts_node_type(c);
            if (strcmp(ck, "new") == 0) {
                mname = "new";
                break;
            }
            if (strcmp(ck, "identifier") == 0) {
                /* Skip if it's the LHS. */
                if (ts_node_eq(c, lhs))
                    continue;
                mname = java_node_text(ctx, c);
                if (mname && mname[0])
                    break;
            }
        }
    }
    if (!mname || !mname[0])
        return;

    /* Determine arity: from the SAM of the outer call's expected param. */
    int sam_arity = -1;
    if (outer_resolved && outer_resolved->signature &&
        outer_resolved->signature->kind == CBM_TYPE_FUNC &&
        outer_resolved->signature->data.func.param_types) {
        /* param_types is NULL-terminated with the DECLARED count; arg_index is
         * the CALL-SITE index, which can exceed it (overload mismatch,
         * varargs) — indexing past the terminator dereferences arena garbage
         * (same OOB family as #427 / bind_lambda_args). */
        const CBMType *const *pts = outer_resolved->signature->data.func.param_types;
        int ptc = 0;
        while (pts[ptc]) {
            ptc++;
        }
        const CBMType *expected = (arg_index >= 0 && arg_index < ptc) ? pts[arg_index] : NULL;
        if (expected) {
            const char *fi_qn = NULL;
            if (expected->kind == CBM_TYPE_NAMED)
                fi_qn = expected->data.named.qualified_name;
            else if (expected->kind == CBM_TYPE_TEMPLATE)
                fi_qn = expected->data.template_type.template_name;
            const JavaSAMSpec *sam = find_sam(fi_qn);
            if (sam)
                sam_arity = sam->arity;
        }
    }
    (void)recv_type;

    /* Try to resolve the LHS as a type or expression. */
    const CBMType *lhs_t = NULL;
    const char *lhs_kind = ts_node_type(lhs);
    bool lhs_is_type =
        (strcmp(lhs_kind, "type_identifier") == 0 ||
         strcmp(lhs_kind, "scoped_type_identifier") == 0 || strcmp(lhs_kind, "generic_type") == 0);
    const char *type_qn = NULL;
    if (lhs_is_type) {
        char *txt = java_node_text(ctx, lhs);
        if (txt)
            type_qn = java_resolve_type_name(ctx, strip_generics(ctx->arena, txt));
        if (!type_qn && txt)
            type_qn = txt;
    } else {
        lhs_t = java_eval_expr_type(ctx, lhs);
        if (lhs_t && lhs_t->kind == CBM_TYPE_NAMED)
            type_qn = lhs_t->data.named.qualified_name;
        else if (lhs_t && lhs_t->kind == CBM_TYPE_TEMPLATE)
            type_qn = lhs_t->data.template_type.template_name;
    }
    if (!type_qn) {
        java_emit_unresolved(ctx, mname, "method_reference_unknown_lhs");
        return;
    }

    /* Constructor reference: ClassName::new */
    if (strcmp(mname, "new") == 0) {
        const char *short_name = strrchr(type_qn, '.');
        short_name = short_name ? short_name + 1 : type_qn;
        const CBMRegisteredFunc *cf =
            cbm_registry_lookup_method(ctx->registry, type_qn, short_name);
        // A `ClassName::new` reference constructs ClassName: resolve to the
        // ClassName CLASS node (which the textual extractor stored), not the
        // synthetic constructor QN that has no graph node. orig=mname ("new")
        // lets the join match the textual `new` call site (the constructor
        // reference is extracted as a call to `new`). cf distinguishes an
        // indexed constructor (higher confidence) from a synthesized one.
        java_emit_resolved_orig(ctx, type_qn, mname,
                                cf ? "lsp_method_ref_ctor" : "lsp_method_ref_ctor_synth",
                                cf ? 0.90f : 0.80f);
        return;
    }

    int try_arity = sam_arity >= 0 ? sam_arity : -1;
    /* If LHS is a type and the referenced method is non-static (instance
     * method ref like String::length), the SAM passes the instance as the
     * first arg, so the method-side takes (sam_arity - 1) args. We try both
     * possibilities and accept the first match. */
    const CBMRegisteredFunc *m = NULL;
    if (try_arity >= 0) {
        m = java_lookup_method(ctx, type_qn, mname, try_arity);
        if (!m && try_arity > 0 && lhs_is_type) {
            m = java_lookup_method(ctx, type_qn, mname, try_arity - 1);
        }
    }
    if (!m)
        m = java_lookup_method(ctx, type_qn, mname, 0);
    if (m) {
        java_emit_resolved(ctx, m->qualified_name, "lsp_method_ref", 0.90f);
    } else {
        java_emit_unresolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, mname),
                             "method_reference_no_match");
    }
}

/* Resolve any method-reference args of a method call. */
static uint32_t bind_method_ref_args(JavaLSPContext *ctx, TSNode call_node,
                                     const CBMRegisteredFunc *resolved, const CBMType *recv_type) {
    uint32_t handled = 0;
    if (!resolved)
        return handled;
    TSNode args_node = ts_node_child_by_field_name(call_node, "arguments", 9);
    if (ts_node_is_null(args_node))
        return handled;
    uint32_t n = ts_node_named_child_count(args_node);
    for (uint32_t i = 0; i < n && i < 32; i++) {
        TSNode arg = ts_node_named_child(args_node, i);
        if (strcmp(ts_node_type(arg), "method_reference") != 0)
            continue;
        resolve_method_reference(ctx, arg, resolved, (int)i, recv_type);
        handled |= ((uint32_t)1u << i);
    }
    return handled;
}

/* Lookup the method that a method_invocation node resolves to. Returns
 * NULL if the receiver type is unknown. Used by bind_lambda_args /
 * bind_method_ref_args before re-walking arguments — we need the resolved
 * method to know the SAM-typed parameter slot. */
static const CBMRegisteredFunc *lookup_method_for_call(JavaLSPContext *ctx, TSNode call,
                                                       const CBMType **out_recv_type) {
    if (out_recv_type)
        *out_recv_type = NULL;
    TSNode obj = ts_node_child_by_field_name(call, "object", 6);
    TSNode name_node = ts_node_child_by_field_name(call, "name", 4);
    if (ts_node_is_null(name_node))
        return NULL;
    char *mname = java_node_text(ctx, name_node);
    if (!mname)
        return NULL;
    int arity = count_call_args(call);

    if (ts_node_is_null(obj)) {
        if (ctx->enclosing_class_qn) {
            return java_lookup_method(ctx, ctx->enclosing_class_qn, mname, arity);
        }
        return NULL;
    }
    if (strcmp(ts_node_type(obj), "super") == 0) {
        const char *sq = ctx->enclosing_super_qn ? ctx->enclosing_super_qn : "java.lang.Object";
        return java_lookup_method(ctx, sq, mname, arity);
    }
    if (strcmp(ts_node_type(obj), "this") == 0) {
        if (ctx->enclosing_class_qn) {
            return java_lookup_method(ctx, ctx->enclosing_class_qn, mname, arity);
        }
        return NULL;
    }
    /* Static call via class name. */
    if (strcmp(ts_node_type(obj), "identifier") == 0) {
        char *oname = java_node_text(ctx, obj);
        if (oname && cbm_type_is_unknown(cbm_scope_lookup(ctx->current_scope, oname))) {
            const char *cls_qn = java_resolve_type_name(ctx, oname);
            if (cls_qn) {
                const CBMRegisteredFunc *f = java_lookup_method(ctx, cls_qn, mname, arity);
                if (f)
                    return f;
            }
        }
    }
    /* Instance dispatch. */
    const CBMType *recv = java_eval_expr_type(ctx, obj);
    if (out_recv_type)
        *out_recv_type = recv;
    const char *recv_qn = NULL;
    if (recv && recv->kind == CBM_TYPE_NAMED)
        recv_qn = recv->data.named.qualified_name;
    else if (recv && recv->kind == CBM_TYPE_TEMPLATE)
        recv_qn = recv->data.template_type.template_name;
    if (recv_qn)
        return java_lookup_method(ctx, recv_qn, mname, arity);
    return NULL;
}

/* Walk every node beneath `node`, calling resolve_method_call on each
 * method_invocation / object_creation_expression and recursing into block
 * children with proper scope handling. */
static void java_resolve_calls_in_node_inner(JavaLSPContext *ctx, TSNode node);

/* Depth-guarded entry: the AST walk recurses per nesting level and crashed
 * with a stack overflow on pathologically nested real-world sources
 * (elasticsearch, SIGSEGV in bind_lambda_args under hundreds of recursive
 * java_resolve_calls_in_node frames). Past the cap the subtree is skipped —
 * its calls stay unresolved, which is graceful degradation, not a crash. */
static void java_resolve_calls_in_node(JavaLSPContext *ctx, TSNode node) {
    if (ctx->walk_depth >= JAVA_LSP_MAX_WALK_DEPTH) {
        return;
    }
    ctx->walk_depth++;
    java_resolve_calls_in_node_inner(ctx, node);
    ctx->walk_depth--;
}

static void java_resolve_calls_in_node_inner(JavaLSPContext *ctx, TSNode node) {
    if (ts_node_is_null(node))
        return;
    const char *kind = ts_node_type(node);

    /* Standalone method_reference (e.g. `return StringBuilder::new;` or
     * `Function<X, Y> f = X::method;`): without an enclosing method call
     * the SAM-context-driven path doesn't fire, so resolve the reference
     * directly using its lhs + name. */
    if (strcmp(kind, "method_reference") == 0) {
        resolve_method_reference(ctx, node, NULL, 0, NULL);
        /* Don't recurse — the LHS is already evaluated by resolve_method_reference. */
        return;
    }

    if (strcmp(kind, "method_invocation") == 0) {
        resolve_method_call(ctx, node);

        /* Lambda / method-reference args: re-walk with the SAM-bound scope.
         * This is the central piece that turns chained streams + forEach
         * + map / filter into resolved edges inside the lambda body. */
        const CBMType *recv_type = NULL;
        const CBMRegisteredFunc *outer = lookup_method_for_call(ctx, node, &recv_type);
        if (outer) {
            uint32_t lambda_handled = bind_lambda_args(ctx, node, outer, recv_type);
            uint32_t mref_handled = bind_method_ref_args(ctx, node, outer, recv_type);
            uint32_t handled = lambda_handled | mref_handled;
            if (handled) {
                /* Walk only non-handled children of the method_invocation;
                 * the handled lambda/method-ref args were walked above with
                 * the SAM-bound scope. */
                TSNode args_node = ts_node_child_by_field_name(node, "arguments", 9);
                /* Walk receiver expression. */
                TSNode obj = ts_node_child_by_field_name(node, "object", 6);
                if (!ts_node_is_null(obj))
                    java_resolve_calls_in_node(ctx, obj);
                if (!ts_node_is_null(args_node)) {
                    uint32_t n = ts_node_named_child_count(args_node);
                    for (uint32_t i = 0; i < n; i++) {
                        if (handled & ((uint32_t)1u << i))
                            continue;
                        TSNode c = ts_node_named_child(args_node, i);
                        java_resolve_calls_in_node(ctx, c);
                    }
                }
                return;
            }
        }
    } else if (strcmp(kind, "object_creation_expression") == 0) {
        /* Resolve constructor as a method. */
        TSNode type_node = ts_node_child_by_field_name(node, "type", 4);
        if (!ts_node_is_null(type_node)) {
            const CBMType *t = java_parse_type_node(ctx, type_node);
            const char *qn = NULL;
            if (t && t->kind == CBM_TYPE_NAMED)
                qn = t->data.named.qualified_name;
            else if (t && t->kind == CBM_TYPE_TEMPLATE)
                qn = t->data.template_type.template_name;
            if (qn) {
                int arity = count_call_args(node);
                /* Constructor short name is the class's short name. */
                const char *short_name = strrchr(qn, '.');
                short_name = short_name ? short_name + 1 : qn;
                const CBMRegisteredFunc *cf =
                    cbm_registry_lookup_method_by_args(ctx->registry, qn, short_name, arity);
                if (!cf)
                    cf = cbm_registry_lookup_method(ctx->registry, qn, short_name);
                if (cf) {
                    java_emit_resolved(ctx, cf->qualified_name, "lsp_constructor", 0.95f);
                } else {
                    /* No explicit constructor in the registry, so there is no
                     * `Class.Class` ctor node to point at. Resolve the `new Foo()`
                     * call to the Foo CLASS node (`qn`) instead: its short name
                     * equals the textual callee_name ("Foo"), so the pipeline
                     * join matches, and the class node always exists, so a CALLS
                     * edge forms carrying the strategy. */
                    java_emit_resolved(ctx, qn, "lsp_constructor_synth", 0.85f);
                }
            }
        }
    }

    /* catch_clause: push a fresh scope so the bound exception variable is
     * not visible past the catch body. */
    if (strcmp(kind, "catch_clause") == 0) {
        CBMScope *saved = ctx->current_scope;
        ctx->current_scope = cbm_scope_push(ctx->arena, saved);
        java_process_statement(ctx, node);
        uint32_t n = ts_node_named_child_count(node);
        for (uint32_t i = 0; i < n; i++) {
            TSNode c = ts_node_named_child(node, i);
            java_resolve_calls_in_node(ctx, c);
        }
        ctx->current_scope = saved;
        return;
    }

    /* Mutate scope on local var decl / enhanced for / etc. */
    java_process_statement(ctx, node);

    /* Don't double-walk blocks (process_block handles its own scope). */
    if (strcmp(kind, "block") == 0)
        return;

    uint32_t n = ts_node_named_child_count(node);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(node, i);
        java_resolve_calls_in_node(ctx, c);
    }
}

/* ── File-defs registration ───────────────────────────────────────── */

/* Parse a comma-separated list at the outermost generic level of `text`.
 * `text` is the substring INSIDE the angle brackets, e.g. for
 * "Map<String, List<Integer>>" the caller would pass "String, List<Integer>".
 *
 * Returns a NULL-terminated array of arena-allocated substrings; sets
 * *out_count to the number of args. */
static const char **split_generic_args(CBMArena *a, const char *inside, int *out_count) {
    *out_count = 0;
    if (!inside || !inside[0])
        return NULL;
    const char *args[16];
    int count = 0;
    int depth = 0;
    const char *seg_start = inside;
    for (const char *p = inside; *p; p++) {
        if (*p == '<')
            depth++;
        else if (*p == '>')
            depth--;
        else if (*p == ',' && depth == 0) {
            /* trim leading whitespace from seg_start */
            while (seg_start < p && (*seg_start == ' ' || *seg_start == '\t'))
                seg_start++;
            const char *seg_end = p;
            while (seg_end > seg_start && (seg_end[-1] == ' ' || seg_end[-1] == '\t'))
                seg_end--;
            if (count < 16 && seg_end > seg_start) {
                args[count++] = cbm_arena_strndup(a, seg_start, (size_t)(seg_end - seg_start));
            }
            seg_start = p + 1;
        }
    }
    /* last segment */
    while (*seg_start == ' ' || *seg_start == '\t')
        seg_start++;
    const char *seg_end = inside + strlen(inside);
    while (seg_end > seg_start && (seg_end[-1] == ' ' || seg_end[-1] == '\t'))
        seg_end--;
    if (count < 16 && seg_end > seg_start) {
        args[count++] = cbm_arena_strndup(a, seg_start, (size_t)(seg_end - seg_start));
    }
    if (count == 0)
        return NULL;
    const char **result = (const char **)cbm_arena_alloc(a, (size_t)(count + 1) * sizeof(*result));
    for (int i = 0; i < count; i++)
        result[i] = args[i];
    result[count] = NULL;
    *out_count = count;
    return result;
}

/* Parse a type-text into a CBMType, with full inner-class qualification.
 * `parent_class` is the QN of the enclosing class (NULL at file scope), used
 * to qualify unqualified inner-class references. `reg` is consulted to
 * confirm matches before committing to a candidate QN.
 *
 * Preserves generic template arguments: "Consumer<String>" parses to a
 * TEMPLATE("java.util.function.Consumer", [String]) so the SAM-binder
 * downstream can substitute the lambda's parameter type correctly. */
static const CBMType *parse_param_text_full(CBMArena *a, const char *text, const char *parent_class,
                                            const char *module_qn, const CBMTypeRegistry *reg) {
    if (!text)
        return cbm_type_unknown();
    int dim = 0;
    const char *no_arr = unwrap_array_text(a, text, &dim);
    const char *no_gen = strip_generics(a, no_arr);

    /* Extract the inside-of-angles for generic-arg recursion. */
    const char *gen_args_inner = NULL;
    if (no_arr) {
        const char *lt = strchr(no_arr, '<');
        if (lt) {
            /* find matching '>' at depth 0 */
            int depth = 0;
            const char *gt = NULL;
            for (const char *p = lt; *p; p++) {
                if (*p == '<')
                    depth++;
                else if (*p == '>') {
                    depth--;
                    if (depth == 0) {
                        gt = p;
                        break;
                    }
                }
            }
            if (gt && gt > lt + 1) {
                gen_args_inner = cbm_arena_strndup(a, lt + 1, (size_t)(gt - lt - 1));
            }
        }
    }

    const CBMType *base = NULL;
    if (no_gen && is_java_primitive(no_gen)) {
        base = cbm_type_builtin(a, no_gen);
    } else if (no_gen) {
        if (strchr(no_gen, '.')) {
            base = cbm_type_named(a, no_gen);
        } else {
            /* Try java.lang well-knowns. */
            for (int i = 0; JAVA_LANG_TYPES[i]; i++) {
                if (strcmp(JAVA_LANG_TYPES[i], no_gen) == 0) {
                    base = cbm_type_named(a, cbm_arena_sprintf(a, "java.lang.%s", no_gen));
                    break;
                }
            }
            /* Walk parent_class chain trying parent.X, parent.parent.X, ... */
            if (!base && parent_class && reg) {
                const char *cur = parent_class;
                while (cur) {
                    const char *cand = cbm_arena_sprintf(a, "%s.%s", cur, no_gen);
                    if (cbm_registry_lookup_type(reg, cand)) {
                        base = cbm_type_named(a, cand);
                        break;
                    }
                    const char *last_dot = strrchr(cur, '.');
                    if (!last_dot)
                        break;
                    cur = cbm_arena_strndup(a, cur, (size_t)(last_dot - cur));
                }
            }
            /* Common Java-stdlib package fallback — tries java.util,
             * java.util.function, etc. so user methods declaring
             * `Consumer<String>` resolve to java.util.function.Consumer
             * even if the import wasn't threaded into this codepath. */
            if (!base && reg) {
                for (int i = 0; JAVA_FALLBACK_PACKAGES[i]; i++) {
                    const char *cand =
                        cbm_arena_sprintf(a, "%s.%s", JAVA_FALLBACK_PACKAGES[i], no_gen);
                    if (cbm_registry_lookup_type(reg, cand)) {
                        base = cbm_type_named(a, cand);
                        break;
                    }
                }
            }
            /* Fallback: module_qn.X (same package, top-level class). */
            if (!base) {
                if (module_qn && module_qn[0]) {
                    base = cbm_type_named(a, cbm_arena_sprintf(a, "%s.%s", module_qn, no_gen));
                } else {
                    base = cbm_type_named(a, no_gen);
                }
            }
        }
    }
    if (!base)
        base = cbm_type_unknown();
    /* If we extracted generic args and the base is NAMED, wrap as TEMPLATE
     * with each arg recursively parsed. */
    if (gen_args_inner && base && base->kind == CBM_TYPE_NAMED) {
        int gc = 0;
        const char **arg_strs = split_generic_args(a, gen_args_inner, &gc);
        if (arg_strs && gc > 0) {
            const CBMType **gargs =
                (const CBMType **)cbm_arena_alloc(a, (size_t)(gc + 1) * sizeof(*gargs));
            for (int i = 0; i < gc; i++) {
                gargs[i] = parse_param_text_full(a, arg_strs[i], parent_class, module_qn, reg);
            }
            gargs[gc] = NULL;
            base = cbm_type_template(a, base->data.named.qualified_name, gargs, gc);
        }
    }
    while (dim-- > 0)
        base = cbm_type_slice(a, base);
    return base;
}

/* Backwards-compatible wrapper for callers that don't know the parent_class
 * context. Used by cbm_run_java_lsp_cross. */
static const CBMType *parse_param_text(CBMArena *a, const char *text, const char *module_qn) {
    return parse_param_text_full(a, text, NULL, module_qn, NULL);
}

static void register_local_func_or_type_from_file(JavaLSPContext *ctx, CBMTypeRegistry *reg,
                                                  CBMFileResult *result) {
    CBMArena *a = ctx->arena;
    const char *module_qn = ctx->module_qn;

    /* Pass 1a: register all class/interface types WITHOUT embedded_types so
     * that subsequent base-class qualification (Pass 1b) can look them up
     * regardless of source order. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label)
            continue;
        if (strcmp(d->label, "Class") != 0 && strcmp(d->label, "Interface") != 0 &&
            strcmp(d->label, "Enum") != 0 && strcmp(d->label, "Type") != 0) {
            continue;
        }
        if (cbm_registry_lookup_type(reg, d->qualified_name))
            continue;
        CBMRegisteredType stub;
        memset(&stub, 0, sizeof(stub));
        stub.qualified_name = d->qualified_name;
        stub.short_name = d->name;
        stub.is_interface = (strcmp(d->label, "Interface") == 0);
        cbm_registry_add_type(reg, stub);
    }

    /* Pass 1b: now that every local type is in the registry, walk again and
     * fill in embedded_types using the parent_class chain. We mutate the
     * registry entry in place via the type lookup (CBMRegisteredType *). */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label)
            continue;
        if (strcmp(d->label, "Class") != 0 && strcmp(d->label, "Interface") != 0 &&
            strcmp(d->label, "Enum") != 0 && strcmp(d->label, "Type") != 0) {
            continue;
        }
        const CBMRegisteredType *existing = cbm_registry_lookup_type(reg, d->qualified_name);
        if (!existing)
            continue;
        CBMRegisteredType rt = *existing; /* Will be re-added; lookup_type returns the
                                             registry's stored copy. We instead build a
                                             local copy and let cbm_registry_add_type's
                                             de-dup not apply — but since we already
                                             added the stub in Pass 1a, we must mutate
                                             in-place. The registry exposes its arrays;
                                             do the in-place update directly. */
        (void)rt;
        /* Locate the actual entry in reg->types[] and write into it. */
        CBMRegisteredType *slot = NULL;
        for (int ti = 0; ti < reg->type_count; ti++) {
            if (reg->types[ti].qualified_name == d->qualified_name ||
                (reg->types[ti].qualified_name &&
                 strcmp(reg->types[ti].qualified_name, d->qualified_name) == 0)) {
                slot = &reg->types[ti];
                break;
            }
        }
        if (!slot)
            continue;
        if (d->base_classes) {
            int bc_count = 0;
            while (d->base_classes[bc_count])
                bc_count++;
            if (bc_count > 0) {
                const char **emb =
                    (const char **)cbm_arena_alloc(a, (size_t)(bc_count + 1) * sizeof(*emb));
                for (int j = 0; j < bc_count; j++) {
                    const char *bc = d->base_classes[j];
                    if (!bc || !bc[0]) {
                        emb[j] = "java.lang.Object";
                        continue;
                    }
                    const char *no_gen = strip_generics(a, bc);
                    /* If the base name is unqualified, try in order:
                     *   1. java.lang well-knowns
                     *   2. parent_class.bc (and walking up its dotted parents)
                     *   3. module_qn.bc (same package)
                     * We pick the first match that exists in the registry. */
                    if (!strchr(no_gen, '.')) {
                        const char *resolved = NULL;
                        for (int k = 0; JAVA_LANG_TYPES[k]; k++) {
                            if (strcmp(JAVA_LANG_TYPES[k], no_gen) == 0) {
                                resolved = cbm_arena_sprintf(a, "java.lang.%s", no_gen);
                                break;
                            }
                        }
                        /* extract_defs only sets parent_class on Method defs,
                         * not on Class defs. Derive a parent QN from the
                         * class's own qualified_name by peeling the last
                         * segment ("test.Main.Main.Dog" → "test.Main.Main"). */
                        const char *parent_qn = d->parent_class;
                        if (!parent_qn && d->qualified_name) {
                            const char *last_dot_qn = strrchr(d->qualified_name, '.');
                            if (last_dot_qn) {
                                parent_qn =
                                    cbm_arena_strndup(a, d->qualified_name,
                                                      (size_t)(last_dot_qn - d->qualified_name));
                            }
                        }
                        if (!resolved && parent_qn) {
                            const char *cur = parent_qn;
                            while (cur && !resolved) {
                                const char *cand = cbm_arena_sprintf(a, "%s.%s", cur, no_gen);
                                if (cbm_registry_lookup_type(reg, cand)) {
                                    resolved = cand;
                                    break;
                                }
                                const char *last_dot = strrchr(cur, '.');
                                if (!last_dot)
                                    break;
                                cur = cbm_arena_strndup(a, cur, (size_t)(last_dot - cur));
                            }
                        }
                        if (!resolved && module_qn && module_qn[0]) {
                            resolved = cbm_arena_sprintf(a, "%s.%s", module_qn, no_gen);
                        }
                        emb[j] = resolved ? resolved : cbm_arena_strdup(a, no_gen);
                    } else {
                        emb[j] = cbm_arena_strdup(a, no_gen);
                    }
                }
                emb[bc_count] = NULL;
                slot->embedded_types = emb;
            }
        }
        /* JLS §8.1.4: every class without an explicit `extends` implicitly
         * extends java.lang.Object. Without this, walks like
         * `obj.toString()` on a user class fail because the inheritance
         * walk has no Object link. */
        if (!slot->embedded_types && !slot->is_interface) {
            const char **emb = (const char **)cbm_arena_alloc(a, 2 * sizeof(*emb));
            if (emb) {
                emb[0] = "java.lang.Object";
                emb[1] = NULL;
                slot->embedded_types = emb;
            }
        }
    }

    /* Field metadata is populated by the AST walk inside process_class_decl,
     * since CBMDefinition doesn't carry a per-field type for Java. See
     * register_class_field_in_registry below. */

    /* Register methods. */
    for (int i = 0; i < result->defs.count; i++) {
        CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->name || !d->label)
            continue;
        if (strcmp(d->label, "Method") != 0 && strcmp(d->label, "Function") != 0 &&
            strcmp(d->label, "Constructor") != 0) {
            continue;
        }
        CBMRegisteredFunc rf;
        memset(&rf, 0, sizeof(rf));
        rf.qualified_name = d->qualified_name;
        rf.short_name = d->name;
        rf.min_params = -1;

        /* Build param types — qualify with parent_class chain so inner-class
         * references like `Box` inside `Main` resolve to "test.Main.Main.Box"
         * not "test.Main.Box". */
        const char *parent = d->parent_class;
        const CBMType **ptypes = NULL;
        if (d->param_types) {
            int pc = 0;
            while (d->param_types[pc])
                pc++;
            if (pc > 0) {
                ptypes = (const CBMType **)cbm_arena_alloc(a, (size_t)(pc + 1) * sizeof(*ptypes));
                for (int j = 0; j < pc; j++) {
                    ptypes[j] = parse_param_text_full(a, d->param_types[j], parent, module_qn, reg);
                }
                ptypes[pc] = NULL;
            }
        }
        /* Build return types — single return only in Java. */
        const CBMType **rtypes = NULL;
        if (d->return_type && d->return_type[0]) {
            rtypes = (const CBMType **)cbm_arena_alloc(a, 2 * sizeof(*rtypes));
            rtypes[0] = parse_param_text_full(a, d->return_type, parent, module_qn, reg);
            rtypes[1] = NULL;
        } else if (d->return_types && d->return_types[0]) {
            rtypes = (const CBMType **)cbm_arena_alloc(a, 2 * sizeof(*rtypes));
            rtypes[0] = parse_param_text_full(a, d->return_types[0], parent, module_qn, reg);
            rtypes[1] = NULL;
        }
        rf.signature = cbm_type_func(a, d->param_names, ptypes, rtypes);
        if (d->parent_class) {
            rf.receiver_type = d->parent_class;
        }
        cbm_registry_add_func(reg, rf);
    }
}

/* ── AST-driven param-type patching ──────────────────────────────────
 *
 * extract_defs.c's clean_type_name() strips generics before storing
 * d->param_types — so Consumer<String> arrives as just "Consumer". This
 * is fine for graph-storage but loses the arg-type info the LSP needs
 * for SAM-binder substitution.
 *
 * patch_method_signatures_from_ast walks the file's AST, finds each
 * method/constructor declaration, builds its QN to match what's already
 * in the registry, then re-extracts the formal_parameter type texts
 * directly from the source (with generics preserved) and replaces the
 * registered func's signature with a template-aware version. */

static void patch_method_signatures_from_ast(JavaLSPContext *ctx, CBMTypeRegistry *reg,
                                             TSNode class_node, const char *enclosing_class_qn);

static void patch_one_method(JavaLSPContext *ctx, CBMTypeRegistry *reg, TSNode method_node,
                             const char *class_qn, bool is_constructor) {
    TSNode name_node = ts_node_child_by_field_name(method_node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *mname = java_node_text(ctx, name_node);
    if (!mname)
        return;
    char *method_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, mname);

    /* Find the registered func by exact QN. */
    CBMRegisteredFunc *slot = NULL;
    for (int fi = 0; fi < reg->func_count; fi++) {
        if (reg->funcs[fi].qualified_name &&
            strcmp(reg->funcs[fi].qualified_name, method_qn) == 0) {
            slot = &reg->funcs[fi];
            break;
        }
    }
    if (!slot)
        return;

    /* Re-extract param types from the AST. */
    TSNode params = ts_node_child_by_field_name(method_node, "parameters", 10);
    if (ts_node_is_null(params))
        return;
    uint32_t pn = ts_node_named_child_count(params);
    if (pn == 0)
        return;
    const CBMType **ptypes =
        (const CBMType **)cbm_arena_alloc(ctx->arena, (size_t)(pn + 1) * sizeof(*ptypes));
    if (!ptypes)
        return;
    int idx = 0;
    for (uint32_t i = 0; i < pn; i++) {
        TSNode p = ts_node_named_child(params, i);
        const char *pk = ts_node_type(p);
        if (strcmp(pk, "formal_parameter") != 0 && strcmp(pk, "spread_parameter") != 0) {
            continue;
        }
        TSNode ptype = ts_node_child_by_field_name(p, "type", 4);
        const CBMType *pt = cbm_type_unknown();
        if (!ts_node_is_null(ptype))
            pt = java_parse_type_node(ctx, ptype);
        if (strcmp(pk, "spread_parameter") == 0 && pt) {
            pt = cbm_type_slice(ctx->arena, pt);
        }
        ptypes[idx++] = pt;
    }
    ptypes[idx] = NULL;

    /* Re-extract return type from AST (only for methods, not constructors). */
    const CBMType **rtypes = NULL;
    if (!is_constructor) {
        TSNode rtype = ts_node_child_by_field_name(method_node, "type", 4);
        if (!ts_node_is_null(rtype)) {
            rtypes = (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(*rtypes));
            rtypes[0] = java_parse_type_node(ctx, rtype);
            rtypes[1] = NULL;
        }
    }
    if (is_constructor) {
        rtypes = (const CBMType **)cbm_arena_alloc(ctx->arena, 2 * sizeof(*rtypes));
        rtypes[0] = cbm_type_named(ctx->arena, class_qn);
        rtypes[1] = NULL;
    }

    /* Rebuild the FUNC type with both param-types and return-types. */
    slot->signature = cbm_type_func(ctx->arena, NULL, ptypes, rtypes);
}

static void patch_method_signatures_from_ast(JavaLSPContext *ctx, CBMTypeRegistry *reg,
                                             TSNode class_node, const char *enclosing_class_qn) {
    if (ts_node_is_null(class_node))
        return;
    const char *kind = ts_node_type(class_node);
    if (strcmp(kind, "class_declaration") != 0 && strcmp(kind, "interface_declaration") != 0 &&
        strcmp(kind, "enum_declaration") != 0 && strcmp(kind, "record_declaration") != 0) {
        return;
    }
    TSNode name_node = ts_node_child_by_field_name(class_node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *cname = java_node_text(ctx, name_node);
    if (!cname)
        return;
    char *class_qn;
    if (enclosing_class_qn) {
        class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", enclosing_class_qn, cname);
    } else if (ctx->module_qn && ctx->module_qn[0]) {
        class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);
    } else {
        class_qn = cbm_arena_strdup(ctx->arena, cname);
    }

    TSNode body = ts_node_child_by_field_name(class_node, "body", 4);
    if (ts_node_is_null(body))
        return;

    const char *saved = ctx->enclosing_class_qn;
    ctx->enclosing_class_qn = class_qn;
    push_enclosing_class(ctx, class_qn);

    uint32_t n = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(body, i);
        const char *ck = ts_node_type(c);
        if (strcmp(ck, "method_declaration") == 0) {
            patch_one_method(ctx, reg, c, class_qn, false);
        } else if (strcmp(ck, "constructor_declaration") == 0) {
            patch_one_method(ctx, reg, c, class_qn, true);
        } else if (strcmp(ck, "class_declaration") == 0 ||
                   strcmp(ck, "interface_declaration") == 0 ||
                   strcmp(ck, "enum_declaration") == 0 || strcmp(ck, "record_declaration") == 0) {
            patch_method_signatures_from_ast(ctx, reg, c, class_qn);
        }
    }
    pop_enclosing_class(ctx);
    ctx->enclosing_class_qn = saved;
}

/* ── AST-driven field metadata population ─────────────────────────── */

/* Append (field_name, field_type) to the registry slot for `class_qn`.
 * Used by populate_class_fields_from_ast so eval_field_access can resolve
 * `obj.field.method()` for arbitrary receivers, not just `this`. */
static void append_field_to_class(CBMTypeRegistry *reg, CBMArena *a, const char *class_qn,
                                  const char *field_name, const CBMType *ftype) {
    CBMRegisteredType *slot = NULL;
    for (int ti = 0; ti < reg->type_count; ti++) {
        if (reg->types[ti].qualified_name && strcmp(reg->types[ti].qualified_name, class_qn) == 0) {
            slot = &reg->types[ti];
            break;
        }
    }
    if (!slot)
        return;

    int existing = 0;
    if (slot->field_names) {
        while (slot->field_names[existing])
            existing++;
    }
    const char **new_names =
        (const char **)cbm_arena_alloc(a, (size_t)(existing + 2) * sizeof(*new_names));
    const CBMType **new_types =
        (const CBMType **)cbm_arena_alloc(a, (size_t)(existing + 2) * sizeof(*new_types));
    if (!new_names || !new_types)
        return;
    for (int j = 0; j < existing; j++) {
        new_names[j] = slot->field_names[j];
        new_types[j] = slot->field_types[j];
    }
    new_names[existing] = cbm_arena_strdup(a, field_name);
    new_types[existing] = ftype ? ftype : cbm_type_unknown();
    new_names[existing + 1] = NULL;
    new_types[existing + 1] = NULL;
    slot->field_names = new_names;
    slot->field_types = new_types;
}

/* Walk a class_declaration / interface_declaration / enum_declaration body
 * and append each field_declaration to its containing class's registry
 * field arrays. Recurses into nested type declarations. */
static void populate_class_fields_from_ast(JavaLSPContext *ctx, CBMTypeRegistry *reg,
                                           TSNode class_node, const char *enclosing_class_qn) {
    if (ts_node_is_null(class_node))
        return;
    const char *kind = ts_node_type(class_node);
    if (strcmp(kind, "class_declaration") != 0 && strcmp(kind, "interface_declaration") != 0 &&
        strcmp(kind, "enum_declaration") != 0 && strcmp(kind, "record_declaration") != 0) {
        return;
    }
    TSNode name_node = ts_node_child_by_field_name(class_node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *cname = java_node_text(ctx, name_node);
    if (!cname)
        return;

    char *class_qn;
    if (enclosing_class_qn) {
        class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", enclosing_class_qn, cname);
    } else if (ctx->module_qn && ctx->module_qn[0]) {
        class_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", ctx->module_qn, cname);
    } else {
        class_qn = cbm_arena_strdup(ctx->arena, cname);
    }

    TSNode body = ts_node_child_by_field_name(class_node, "body", 4);
    if (ts_node_is_null(body))
        return;
    /* Snapshot enclosing-class stack so java_parse_type_node sees inner-class
     * scope when parsing field types like `Greeter` inside class `Main`. */
    const char *saved_enc = ctx->enclosing_class_qn;
    ctx->enclosing_class_qn = class_qn;
    push_enclosing_class(ctx, class_qn);

    uint32_t n = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < n; i++) {
        TSNode c = ts_node_named_child(body, i);
        const char *ck = ts_node_type(c);
        if (strcmp(ck, "field_declaration") == 0) {
            TSNode tn = ts_node_child_by_field_name(c, "type", 4);
            const CBMType *ftype =
                ts_node_is_null(tn) ? cbm_type_unknown() : java_parse_type_node(ctx, tn);
            uint32_t fcc = ts_node_named_child_count(c);
            for (uint32_t j = 0; j < fcc; j++) {
                TSNode dec = ts_node_named_child(c, j);
                if (strcmp(ts_node_type(dec), "variable_declarator") != 0)
                    continue;
                TSNode dname = ts_node_child_by_field_name(dec, "name", 4);
                if (ts_node_is_null(dname))
                    continue;
                char *fname = java_node_text(ctx, dname);
                if (fname)
                    append_field_to_class(reg, ctx->arena, class_qn, fname, ftype);
            }
        } else if (strcmp(ck, "class_declaration") == 0 ||
                   strcmp(ck, "interface_declaration") == 0 ||
                   strcmp(ck, "enum_declaration") == 0 || strcmp(ck, "record_declaration") == 0) {
            populate_class_fields_from_ast(ctx, reg, c, class_qn);
        }
    }
    pop_enclosing_class(ctx);
    ctx->enclosing_class_qn = saved_enc;
}

/* ── Top-level entry: cbm_run_java_lsp ────────────────────────────── */

extern const TSLanguage *tree_sitter_java(void);

void cbm_run_java_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root) {
    if (!arena || !result || !source)
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);

    /* Java stdlib — java.lang/util/io/etc. */
    cbm_java_stdlib_register(&reg, arena);

    /* Module QN comes from the file result. */
    const char *module_qn = result->module_qn ? result->module_qn : "";

    JavaLSPContext ctx;
    java_lsp_init(&ctx, arena, source, source_len, &reg, NULL, module_qn, &result->resolved_calls);

    /* Add imports collected by extract_imports.c — they are stored as
     * (local_name, module_path = full Java path). We translate to the LSP
     * import shape here.
     *
     * Note: extract_imports.c can't tell static / on-demand from the AST in
     * a uniform way, so the LSP re-scans the AST for those distinctions in
     * java_lsp_process_file. We still re-emit TYPE imports here so that
     * resolution works even if process_file's import scan misses
     * pre-grammar-version edge cases. */
    for (int i = 0; i < result->imports.count; i++) {
        CBMImport *imp = &result->imports.items[i];
        if (!imp->local_name || !imp->module_path)
            continue;
        java_lsp_add_import(&ctx, imp->local_name, imp->module_path, CBM_JAVA_IMPORT_TYPE);
    }

    /* Register file-local defs into the registry. */
    register_local_func_or_type_from_file(&ctx, &reg, result);

    /* Populate field metadata from the AST so eval_field_access works on
     * receivers other than `this`. Also patch method signatures with
     * generics-preserving types (extract_defs strips generics from
     * d->param_types via clean_type_name). */
    uint32_t rn = ts_node_named_child_count(root);
    for (uint32_t i = 0; i < rn; i++) {
        TSNode c = ts_node_named_child(root, i);
        const char *ck = ts_node_type(c);
        if (strcmp(ck, "class_declaration") == 0 || strcmp(ck, "interface_declaration") == 0 ||
            strcmp(ck, "enum_declaration") == 0 || strcmp(ck, "record_declaration") == 0) {
            populate_class_fields_from_ast(&ctx, &reg, c, NULL);
            patch_method_signatures_from_ast(&ctx, &reg, c, NULL);
        }
    }

    /* Walk the file. */
    java_lsp_process_file(&ctx, root);
}

/* ── Cross-file LSP ───────────────────────────────────────────────── */

void cbm_run_java_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!arena || !source)
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_java_stdlib_register(&reg, arena);

    /* Register cross-file defs. */
    for (int i = 0; i < def_count; i++) {
        CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;
        if (strcmp(d->label, "Class") == 0 || strcmp(d->label, "Interface") == 0 ||
            strcmp(d->label, "Enum") == 0 || strcmp(d->label, "Type") == 0) {
            CBMRegisteredType rt;
            memset(&rt, 0, sizeof(rt));
            rt.qualified_name = d->qualified_name;
            rt.short_name = d->short_name;
            rt.is_interface = (strcmp(d->label, "Interface") == 0) || d->is_interface;
            /* Embedded types from "|"-separated text. */
            if (d->embedded_types && d->embedded_types[0]) {
                int n = 1;
                for (const char *p = d->embedded_types; *p; p++)
                    if (*p == '|')
                        n++;
                const char **emb =
                    (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(*emb));
                int idx = 0;
                const char *start = d->embedded_types;
                while (*start) {
                    const char *end = start;
                    while (*end && *end != '|')
                        end++;
                    if (end > start) {
                        emb[idx++] = cbm_arena_strndup(arena, start, (size_t)(end - start));
                    }
                    if (!*end)
                        break;
                    start = end + 1;
                }
                emb[idx] = NULL;
                rt.embedded_types = emb;
            }
            cbm_registry_add_type(&reg, rt);
        } else if (strcmp(d->label, "Method") == 0 || strcmp(d->label, "Function") == 0 ||
                   strcmp(d->label, "Constructor") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->short_name;
            rf.min_params = -1;
            rf.receiver_type = d->receiver_type;
            /* Build single-return signature from return_types text. */
            const CBMType *ret = NULL;
            if (d->return_types && d->return_types[0]) {
                const char *no_pipe = d->return_types;
                const char *bar = strchr(no_pipe, '|');
                const char *first;
                if (bar) {
                    first = cbm_arena_strndup(arena, no_pipe, (size_t)(bar - no_pipe));
                } else {
                    first = no_pipe;
                }
                ret = parse_param_text(arena, first, d->def_module_qn);
            }
            const CBMType **rtypes = NULL;
            if (ret) {
                rtypes = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rtypes));
                rtypes[0] = ret;
                rtypes[1] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, NULL, rtypes);
            cbm_registry_add_func(&reg, rf);
        }
    }

    /* Build the hash indexes: without this every lookup_type/func/method in
     * the walk below is a LINEAR scan over the whole cross registry —
     * O(lookups x defs) per file. Index allocations go to a per-call scratch
     * arena: reg's arena is the pipeline-lifetime result arena, and per-file
     * bucket allocations there accumulate GBs across a large repo. */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    cbm_registry_finalize_into(&reg, &idx_arena);

    /* Parse if needed. */
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        ts_parser_set_language(parser, tree_sitter_java());
        tree = ts_parser_parse_string(parser, NULL, source, (uint32_t)source_len);
        ts_parser_delete(parser);
        owns_tree = true;
    }
    if (!tree) {
        cbm_arena_destroy(&idx_arena);
        return;
    }
    TSNode root = ts_tree_root_node(tree);

    JavaLSPContext ctx;
    java_lsp_init(&ctx, arena, source, source_len, &reg, NULL, module_qn, out);

    /* Apply caller-supplied imports. */
    for (int i = 0; i < import_count; i++) {
        if (!import_names[i] || !import_qns[i])
            continue;
        java_lsp_add_import(&ctx, import_names[i], import_qns[i], CBM_JAVA_IMPORT_TYPE);
    }

    java_lsp_process_file(&ctx, root);
    cbm_arena_destroy(&idx_arena);

    if (owns_tree)
        ts_tree_delete(tree);
}

void cbm_batch_java_lsp_cross(CBMArena *arena, CBMBatchJavaLSPFile *files, int file_count,
                              CBMResolvedCallArray *out) {
    if (!files || file_count <= 0)
        return;
    for (int i = 0; i < file_count; i++) {
        cbm_run_java_lsp_cross(arena, files[i].source, files[i].source_len, files[i].module_qn,
                               files[i].defs, files[i].def_count, files[i].import_names,
                               files[i].import_qns, files[i].import_count, files[i].cached_tree,
                               &out[i]);
    }
}
