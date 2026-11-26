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
#include "lsp_work.h"
#include "../helpers.h"
#include "../../../src/foundation/hash_table.h"

#include <ctype.h>
#include <stdatomic.h>
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
static bool java_is_project_type(const CBMRegisteredType *rt);

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

bool cbm_java_lang_type(const char *name, size_t len) {
    for (int i = 0; name && JAVA_LANG_TYPES[i]; i++) {
        if (strlen(JAVA_LANG_TYPES[i]) == len && memcmp(JAVA_LANG_TYPES[i], name, len) == 0) {
            return true;
        }
    }
    return false;
}

/* ── Ambiguous type QNs ───────────────────────────────────────────── */

/* The cross registry keys a type by `<package>.<name>`, so a nested type and a
 * same-named type of its package, or one class in two source trees, share one
 * QN: the registry cannot tell them apart, and an identity naming that QN has
 * no answer. Each such QN carries a mark: a registered alias under the
 * reserved key "\x1f<QN>", which no source spelling can reach (a QN never
 * starts with a control byte; alias entries are skipped by every scan). */
#define JAVA_AMBIGUOUS_MARK "\x1f"

static bool java_type_ambiguous(const CBMTypeRegistry *reg, const char *qn) {
    char key[JAVA_LSP_BUF];
    int n = snprintf(key, sizeof(key), JAVA_AMBIGUOUS_MARK "%s", qn ? qn : "");
    /* A QN too long to mark is never marked: treat it as ambiguous. */
    return !qn || n <= 0 || (size_t)n >= sizeof(key) || cbm_registry_lookup_type(reg, key) != NULL;
}

static bool java_type_label(const char *label) {
    return label && (strcmp(label, "Class") == 0 || strcmp(label, "Interface") == 0 ||
                     strcmp(label, "Enum") == 0 || strcmp(label, "Type") == 0);
}

void cbm_java_mark_ambiguous_types(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                   int def_count) {
    if (!arena || !reg || !defs || def_count <= 0) {
        return;
    }
    /* QN -> the first def that registers it, or `seen_twice` once marked. */
    static char seen_twice;
    CBMHashTable *seen = cbm_ht_create((uint32_t)(def_count / 4 + 16));
    if (!seen) {
        return;
    }
    for (int i = 0; i < def_count; i++) {
        const CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !java_type_label(d->label)) {
            continue;
        }
        CBM_LSP_WORK(1);
        void *prev = cbm_ht_get(seen, d->qualified_name);
        if (!prev) {
            cbm_ht_set(seen, d->qualified_name, (void *)d);
            continue;
        }
        if (prev == &seen_twice) {
            continue;
        }
        cbm_ht_set(seen, d->qualified_name, &seen_twice);
        CBMRegisteredType mark;
        memset(&mark, 0, sizeof(mark));
        mark.qualified_name = cbm_arena_sprintf(arena, JAVA_AMBIGUOUS_MARK "%s", d->qualified_name);
        mark.short_name = JAVA_AMBIGUOUS_MARK;
        mark.alias_of = d->qualified_name;
        if (mark.qualified_name) {
            cbm_registry_add_type(reg, mark);
        }
    }
    cbm_ht_free(seen);
}

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

/* The imports with this local name, in table order (lsp_name_rows.h): a scan per
 * use made a file of thousands of imports quadratic. */
static int java_import_first(JavaLSPContext *ctx, const char *name) {
    return cbm_name_rows_first(&ctx->import_rows, ctx->arena, ctx->import_local_names,
                               ctx->import_count, name);
}

static int java_import_next(const JavaLSPContext *ctx, const char *name, int row) {
    return cbm_name_rows_next(&ctx->import_rows, ctx->import_local_names, ctx->import_count, name,
                              row);
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
    ctx->class_epoch++;
}

static void pop_enclosing_class(JavaLSPContext *ctx) {
    if (ctx->enclosing_class_depth > 0)
        ctx->enclosing_class_depth--;
    ctx->class_epoch++;
}

/* ── Type-AST → CBMType ───────────────────────────────────────────── */

static const CBMType *parse_type_arguments(JavaLSPContext *ctx, TSNode targs_node);

/* `Outer.Inner` written with Outer resolved to another file's project type
 * `head_qn`: the cross registry keys a nested type by its package and name
 * (`<package>.Inner`, pxc_jvm_def_qn). That QN when the registry holds it
 * once (never a QN two types share), else NULL. */
static const char *java_flattened_member(JavaLSPContext *ctx, const char *head_qn,
                                         const char *spelled) {
    const CBMRegisteredType *head = cbm_registry_lookup_type(ctx->registry, head_qn);
    const char *leaf = strrchr(spelled, '.');
    if (!head || !java_is_project_type(head) || !leaf || !leaf[1]) {
        return NULL;
    }
    const char *flat = cbm_arena_sprintf(ctx->arena, "%s%s", head->namespace_qn, leaf);
    return flat && strcmp(flat, head_qn) != 0 && cbm_registry_lookup_type(ctx->registry, flat) &&
                   !java_type_ambiguous(ctx->registry, flat)
               ? flat
               : NULL;
}

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
                const char *member = cbm_arena_sprintf(ctx->arena, "%s%s", head_qn, dot);
                const char *flat = cbm_registry_lookup_type(ctx->registry, member)
                                       ? NULL
                                       : java_flattened_member(ctx, head_qn, full);
                return cbm_type_named(ctx->arena, flat ? flat : member);
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

/* ── Member types (JLS 8.5) ── */

/* A registry's member-type index: "\x1e<owner>\x1f<name>" names the member
 * type (alias_of) and "\x1d<member>" its owner, one entry each per nested type
 * definition whose owner the collector named (CBMLSPDef.receiver_type, the
 * namespace form; java_register_types). A member type of the JDK seed or of
 * this file's own types is registered under its owner's QN instead. */
#define JAVA_MEMBER_KEY "\x1e"
#define JAVA_OWNER_KEY "\x1d"
/* "\x1c<package>": a package the shared registrar saw a project type in; the
 * key alone marks a registry that records them (java_register_types). */
#define JAVA_PACKAGE_KEY "\x1c"

/* Whether a supertype the registry does not hold is known to lie outside the
 * project (the possible-against-proven rule): its package, read from the
 * spelling (the segments before the first capitalised one), is no package of
 * the run. The run's package table answers when the run has one, else the
 * packages a shared registrar recorded; a registry with neither (a per-file
 * run) judges nothing, nor does a spelling without a package. Not judged
 * outside = possibly a project type whose members no answer sees. */
typedef struct {
    const CBMTypeRegistry *reg;
    const CBMJavaProjectPackages *packages;
} JavaUnseenJudge;

/* What a supertype the registry does not hold is, as the registry spells it
 * (in the scope of the file that declares the class: an identity, an import's
 * target, a java.lang name, or the spelling as written). */
enum {
    /* Rule (i): a spelling that cannot be read as a type name, or on a
     * closure run a qualified spelling of a type the run's index holds and
     * the closure's registry lacks (JAVA_UNSEEN_CLOSURE as well). No answer
     * rests on it. */
    JAVA_UNSEEN_PROJECT = 1u << 0,
    /* Rule (ii), known outside the project: a qualified spelling in another
     * package (an import's target, a full qualification), or a java.lang name. */
    JAVA_UNSEEN_EXTERNAL = 1u << 1,
    /* Rule (ii), no project type, package unknown: a bare spelling no scope
     * of its file placed (an on-demand import's, or a library's). */
    JAVA_UNSEEN_UNKNOWN = 1u << 2,
    /* With JAVA_UNSEEN_PROJECT: a type the index holds, the closure lacks. */
    JAVA_UNSEEN_CLOSURE = 1u << 3,
    /* Rule (ii), in a project package outside the index: a qualified spelling
     * of a project package whose type the index does not hold (a library type
     * in a split package, a generated or excluded source). A full run's
     * registry holds every indexed type, so there it is every such spelling. */
    JAVA_UNSEEN_PACKAGE = 1u << 4,
};

static bool java_project_package(const JavaUnseenJudge *j, const char *qn, size_t pkg_len) {
    if (j->packages && j->packages->declared) {
        return j->packages->declared(j->packages->ud, qn, pkg_len);
    }
    char key[JAVA_LSP_BUF];
    int n = snprintf(key, sizeof(key), JAVA_PACKAGE_KEY "%.*s", (int)pkg_len, qn);
    return n > 0 && (size_t)n < sizeof(key) && cbm_registry_lookup_type(j->reg, key) != NULL;
}

static unsigned java_unseen_kind(const JavaUnseenJudge *j, const char *qn) {
    size_t len = qn ? strcspn(qn, "<") : 0;
    if (!j || !j->reg || len == 0) {
        return JAVA_UNSEEN_PROJECT;
    }
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)qn[i];
        bool part = isalnum(c) || c == '_' || c == '$' || c >= 0x80;
        if (!(part || (c == '.' && i > 0 && i + 1 < len && qn[i + 1] != '.'))) {
            return JAVA_UNSEEN_PROJECT; /* no type name: "a-b.C", "a..B", ".B" */
        }
    }
    const char *dot = memchr(qn, '.', len);
    if (!dot) {
        return cbm_java_lang_type(qn, len) ? JAVA_UNSEEN_EXTERNAL : JAVA_UNSEEN_UNKNOWN;
    }
    /* The package: the segments before the first one that starts upper-case
     * (`a.b.Outer.Inner` is package a.b). None, or no package before it
     * (`Outer.Inner`): its package is unknown. */
    const char *type = NULL;
    for (size_t i = (size_t)(dot - qn); i + 1 < len && !type; i++) {
        if (qn[i] == '.' && isupper((unsigned char)qn[i + 1])) {
            type = qn + i + 1;
        }
    }
    if (!type || isupper((unsigned char)qn[0])) {
        return JAVA_UNSEEN_UNKNOWN;
    }
    size_t pkg_len = (size_t)(type - 1 - qn);
    if (!java_project_package(j, qn, pkg_len)) {
        return JAVA_UNSEEN_EXTERNAL;
    }
    if (j->packages && j->packages->indexed &&
        j->packages->indexed(j->packages->ud, qn, pkg_len, type, strcspn(type, ".<"))) {
        return JAVA_UNSEEN_PROJECT | JAVA_UNSEEN_CLOSURE;
    }
    return JAVA_UNSEEN_PACKAGE;
}

/* Answers withheld under rule (i), each once (engine step 0 of a cross run,
 * a registrar's declared type). On a full run every project type is
 * registered: each is a defect of ours to look at. */
static _Atomic uint64_t g_java_unseen_project;
static _Atomic uint64_t g_java_unseen_closure; /* of those: a type the closure lacks */

/* The member type `name` that the type `owner` declares, or NULL. *unnamed is
 * set when it declares one whose QN two types share: the name is then that
 * member, and no answer can name it. */
static const char *java_declared_member_type(const CBMTypeRegistry *reg, CBMArena *arena,
                                             const char *owner, const char *name, bool *unnamed) {
    char buf[JAVA_LSP_BUF];
    int n = snprintf(buf, sizeof(buf), "%s.%s", owner, name);
    if (n > 0 && (size_t)n < sizeof(buf) && cbm_registry_lookup_type(reg, buf)) {
        if (java_type_ambiguous(reg, buf)) {
            *unnamed = true;
            return NULL;
        }
        return cbm_arena_strdup(arena, buf);
    }
    n = snprintf(buf, sizeof(buf), JAVA_MEMBER_KEY "%s\x1f%s", owner, name);
    const CBMRegisteredType *e =
        n > 0 && (size_t)n < sizeof(buf) ? cbm_registry_lookup_type(reg, buf) : NULL;
    if (!e || !e->alias_of) {
        return NULL;
    }
    if (java_type_ambiguous(reg, e->alias_of) || !cbm_registry_lookup_type(reg, e->alias_of)) {
        *unnamed = true;
        return NULL;
    }
    return e->alias_of;
}

/* A nested type spelled through its owner (`p.Outer.Inner`, a form javac takes
 * in a supertype list): the registry keys a nested type by its package and
 * simple name and links it to its owner (JAVA_MEMBER_KEY), so the spelling
 * names the member of the longest prefix the registry holds as a type, one
 * segment at a time. NULL when no prefix is a type, a segment is no member,
 * or a member's QN two types share. Work linear in the segments. */
static const char *java_owner_spelled_type(const CBMTypeRegistry *reg, CBMArena *arena,
                                           const char *qn) {
    size_t len = qn ? strlen(qn) : 0;
    if (len == 0 || memchr(qn, '<', len)) {
        return NULL;
    }
    const char *owner = NULL;
    size_t cut = len;
    while (!owner) {
        while (cut > 0 && qn[cut - 1] != '.') {
            cut--;
        }
        if (cut <= 1) {
            return NULL; /* no prefix of the spelling is a type */
        }
        cut--; /* the dot */
        CBM_LSP_WORK(1);
        char *prefix = cbm_arena_strndup(arena, qn, cut);
        if (!prefix) {
            return NULL;
        }
        if (cbm_registry_lookup_type(reg, prefix)) {
            owner = prefix;
        }
    }
    for (const char *rest = qn + cut + 1; owner && *rest;) {
        size_t seg = strcspn(rest, ".");
        char *name = cbm_arena_strndup(arena, rest, seg);
        bool unnamed = false;
        CBM_LSP_WORK(1);
        owner = name ? java_declared_member_type(reg, arena, owner, name, &unnamed) : NULL;
        rest += seg + (rest[seg] == '.');
    }
    return owner;
}

/* The supertypes a walk has queued: each once (a linear check while few, a
 * hash set after), the queue growing with the chain. */
enum { JAVA_TYPE_QUEUE_LOCAL = 16, JAVA_TYPE_QUEUE_LINEAR = 8 };

typedef struct {
    const char **items;
    int count;
    int cap;
    CBMHashTable *seen;
} JavaTypeQueue;

static void java_type_queue_push(CBMArena *arena, JavaTypeQueue *q, const char *qn) {
    if (!qn || !qn[0]) {
        return;
    }
    if (q->seen) {
        if (cbm_ht_get(q->seen, qn)) {
            return;
        }
    } else {
        for (int i = 0; i < q->count; i++) {
            CBM_LSP_WORK(1);
            if (strcmp(q->items[i], qn) == 0) {
                return;
            }
        }
    }
    if (q->count == q->cap) {
        const char **grown =
            (const char **)cbm_arena_alloc(arena, (size_t)q->cap * 2 * sizeof(*grown));
        if (!grown) {
            return;
        }
        memcpy(grown, q->items, (size_t)q->count * sizeof(*grown));
        q->items = grown;
        q->cap *= 2;
    }
    q->items[q->count++] = qn;
    if (q->seen) {
        cbm_ht_set(q->seen, qn, (void *)qn);
    } else if (q->count > JAVA_TYPE_QUEUE_LINEAR) {
        q->seen = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 64);
        for (int i = 0; q->seen && i < q->count; i++) {
            cbm_ht_set(q->seen, q->items[i], (void *)q->items[i]);
        }
    }
}

/* The member type `name` that the type `cls` inherits from a supertype (JLS
 * 8.5): `Entry` in a subclass of HashMap is java.util.Map.Entry, and `X` in a
 * subclass of a project class that declares a member type X is that member.
 * Every supertype the registry records is visited once, so a cyclic or long
 * chain ends in work linear in its length. *unnamed: as
 * java_declared_member_type. *unseen: the kinds (JAVA_UNSEEN_*) of the
 * supertypes met that the registry does not hold; each may declare member
 * types no answer sees. */
static const char *java_inherited_member(const CBMTypeRegistry *reg, CBMArena *arena,
                                         const char *cls, const char *name, bool *unnamed,
                                         const JavaUnseenJudge *judge, unsigned *unseen) {
    const char *local[JAVA_TYPE_QUEUE_LOCAL] = {0};
    JavaTypeQueue q = {.items = local, .cap = JAVA_TYPE_QUEUE_LOCAL};
    const CBMRegisteredType *rt = cls ? cbm_registry_lookup_type(reg, cls) : NULL;
    for (int k = 0; rt && rt->embedded_types && rt->embedded_types[k]; k++) {
        java_type_queue_push(arena, &q, rt->embedded_types[k]);
    }
    const char *found = NULL;
    for (int head = 0; head < q.count && !found && !*unnamed; head++) {
        const char *super = q.items[head];
        CBM_LSP_WORK(1);
        found = java_declared_member_type(reg, arena, super, name, unnamed);
        if (found || *unnamed) {
            break;
        }
        const CBMRegisteredType *st = cbm_registry_lookup_type(reg, super);
        if (!st) {
            /* A project spelling may name a nested type through its owner;
             * the walk then visits the type it names. */
            unsigned kind = java_unseen_kind(judge, super);
            const char *nested = NULL;
            if (kind & (JAVA_UNSEEN_PROJECT | JAVA_UNSEEN_PACKAGE)) {
                nested = java_owner_spelled_type(reg, arena, super);
            }
            if (nested) {
                java_type_queue_push(arena, &q, nested);
            } else {
                *unseen |= kind;
            }
            continue;
        }
        for (int k = 0; st->embedded_types && st->embedded_types[k]; k++) {
            java_type_queue_push(arena, &q, st->embedded_types[k]);
        }
    }
    cbm_ht_free(q.seen); /* NULL-safe */
    return found;
}

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
const char *cbm_java_test_inherited_member(const CBMTypeRegistry *reg, CBMArena *arena,
                                           const char *cls, const char *name) {
    bool unnamed = false;
    unsigned unseen = 0;
    return java_inherited_member(reg, arena, cls, name, &unnamed, NULL, &unseen);
}
#endif

/* Engine answers that rest on a simple type name resolved inside a class
 * whose supertypes (its own or an enclosing class's) include one the
 * registry does not hold (rule ii): counted per answer of a cross run, split
 * by the supertypes' kind (a measurement, no behaviour depends on it). */
static _Atomic uint64_t g_java_unknown_scope_answers;
static _Atomic uint64_t g_java_unseen_external_answers;
static _Atomic uint64_t g_java_unseen_unknown_answers;
static _Atomic uint64_t g_java_unseen_package_answers;

uint64_t cbm_java_take_unknown_scope_answers(void) {
    return atomic_exchange_explicit(&g_java_unknown_scope_answers, 0, memory_order_relaxed);
}

void cbm_java_take_unseen_counts(uint64_t *project, uint64_t *closure, uint64_t *external,
                                 uint64_t *unknown, uint64_t *package) {
    *project = atomic_exchange_explicit(&g_java_unseen_project, 0, memory_order_relaxed);
    *closure = atomic_exchange_explicit(&g_java_unseen_closure, 0, memory_order_relaxed);
    *external = atomic_exchange_explicit(&g_java_unseen_external_answers, 0, memory_order_relaxed);
    *unknown = atomic_exchange_explicit(&g_java_unseen_unknown_answers, 0, memory_order_relaxed);
    *package = atomic_exchange_explicit(&g_java_unseen_package_answers, 0, memory_order_relaxed);
}

/* The values of ctx->unknown_scope_types: the type rests on a supertype of
 * unknown package, else on one in a project package outside the index, else
 * on external ones only. */
static char g_java_rests_unknown;
static char g_java_rests_package;
static char g_java_rests_external;

static void java_note_unknown_scope_type(JavaLSPContext *ctx, const char *qn, unsigned unseen) {
    if (!ctx->cross_run || !qn) {
        return;
    }
    if (!ctx->unknown_scope_types) {
        ctx->unknown_scope_types = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 16);
    }
    void *kind = &g_java_rests_external;
    if (unseen & JAVA_UNSEEN_UNKNOWN) {
        kind = &g_java_rests_unknown;
    } else if (unseen & JAVA_UNSEEN_PACKAGE) {
        kind = &g_java_rests_package;
    }
    void *had = ctx->unknown_scope_types ? cbm_ht_get(ctx->unknown_scope_types, qn) : NULL;
    bool weaker = had == &g_java_rests_external ||
                  (had == &g_java_rests_package && kind == &g_java_rests_unknown);
    if (ctx->unknown_scope_types && (!had || (had != kind && weaker))) {
        const char *key = had ? qn : cbm_arena_strdup(ctx->arena, qn);
        if (key) {
            cbm_ht_set(ctx->unknown_scope_types, key, kind);
        }
    }
}

static void java_count_unknown_scope_answer(const JavaLSPContext *ctx, const char *callee_qn,
                                            const char *strategy) {
    if (!ctx->unknown_scope_types || !callee_qn || !strategy ||
        strcmp(strategy, "lsp_unresolved") == 0) {
        return;
    }
    char owner[JAVA_LSP_BUF];
    const char *dot = strrchr(callee_qn, '.');
    size_t n = dot ? (size_t)(dot - callee_qn) : 0;
    void *hit = cbm_ht_get(ctx->unknown_scope_types, callee_qn);
    if (!hit && n > 0 && n < sizeof(owner)) {
        memcpy(owner, callee_qn, n);
        owner[n] = '\0';
        hit = cbm_ht_get(ctx->unknown_scope_types, owner);
    }
    if (hit) {
        atomic_fetch_add_explicit(&g_java_unknown_scope_answers, 1, memory_order_relaxed);
        _Atomic uint64_t *by_kind = &g_java_unseen_external_answers;
        if (hit == &g_java_rests_unknown) {
            by_kind = &g_java_unseen_unknown_answers;
        } else if (hit == &g_java_rests_package) {
            by_kind = &g_java_unseen_package_answers;
        }
        atomic_fetch_add_explicit(by_kind, 1, memory_order_relaxed);
    }
}

static void java_lsp_ctx_done(JavaLSPContext *ctx) {
    cbm_ht_free(ctx->unknown_scope_types); /* NULL-safe */
    ctx->unknown_scope_types = NULL;
    cbm_ht_free(ctx->step0_memo); /* NULL-safe; its entries live in the arena */
    ctx->step0_memo = NULL;
    cbm_ht_free(ctx->walk_memo); /* NULL-safe; its entries live in the arena */
    ctx->walk_memo = NULL;
    cbm_ht_free(ctx->inherit_memo); /* NULL-safe; its keys live in the arena */
    ctx->inherit_memo = NULL;
}

static const char *java_resolve_type_name_in(JavaLSPContext *ctx, const char *name,
                                             unsigned *unseen);

const char *java_resolve_type_name(JavaLSPContext *ctx, const char *name) {
    unsigned unseen = 0;
    const char *qn = java_resolve_type_name_in(ctx, name, &unseen);
    if (qn && unseen && name && !strchr(name, '.')) {
        java_note_unknown_scope_type(ctx, qn, unseen);
    }
    return qn;
}

/* java_inherited_member per (class, name) in this file, once the registry
 * is complete (step0_ready): the walk is a function of the registry, the
 * class, the name and the judge, so each pair walks once however many
 * epochs (nested classes) the file's class bodies cross. */
typedef struct {
    const char *found;
    unsigned unseen;
    bool unnamed;
} JavaWalkMemo;

static const char *java_inherited_member_ctx(JavaLSPContext *ctx, const char *cls,
                                             const char *name, bool *unnamed,
                                             const JavaUnseenJudge *judge, unsigned *unseen) {
    char key[JAVA_LSP_BUF];
    int n = -1;
    if (ctx->step0_ready && cls) {
        n = snprintf(key, sizeof(key), "%s\x1f%s", cls, name);
    }
    if (n <= 0 || (size_t)n >= sizeof(key)) {
        return java_inherited_member(ctx->registry, ctx->arena, cls, name, unnamed, judge, unseen);
    }
    if (ctx->walk_memo && ctx->walk_memo_types != ctx->registry->type_count) {
        cbm_ht_free(ctx->walk_memo);
        ctx->walk_memo = NULL;
    }
    if (!ctx->walk_memo) {
        ctx->walk_memo = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 64);
        ctx->walk_memo_types = ctx->registry->type_count;
    }
    CBM_LSP_WORK(1);
    const JavaWalkMemo *hit =
        ctx->walk_memo ? (const JavaWalkMemo *)cbm_ht_get(ctx->walk_memo, key) : NULL;
    if (hit) {
        *unnamed = hit->unnamed;
        *unseen |= hit->unseen;
        return hit->found;
    }
    JavaWalkMemo r = {0};
    r.found =
        java_inherited_member(ctx->registry, ctx->arena, cls, name, &r.unnamed, judge, &r.unseen);
    JavaWalkMemo *slot = (JavaWalkMemo *)cbm_arena_alloc(ctx->arena, sizeof(*slot));
    const char *k = cbm_arena_strdup(ctx->arena, key);
    if (slot && k && ctx->walk_memo && ctx->walk_memo_types == ctx->registry->type_count) {
        *slot = r;
        cbm_ht_set(ctx->walk_memo, k, slot);
    }
    *unnamed = r.unnamed;
    *unseen |= r.unseen;
    return r.found;
}

/* Step 0 of java_resolve_type_name, the enclosing classes innermost first:
 * decided when it answers (a member type) or gives no answer (a member two
 * types share, or rule i); else the name goes on to the imports. unseen: the
 * kinds of the supertypes met that the registry does not hold. */
typedef struct {
    const char *qn;
    unsigned unseen;
    bool decided;
    bool withheld; /* rule (i) */
} JavaStep0;

static JavaStep0 java_step0_compute(JavaLSPContext *ctx, const char *name) {
    JavaStep0 r = {0};
    /* A member type each enclosing class declares (`Outer.Inner` is `Inner`
     * inside `Outer`), then one it inherits from a supertype (JLS 8.5). Both
     * are in scope in the class body and shadow the imports and the package
     * (JLS 6.4.1); a member whose QN two types share is that member, which no
     * answer can name. */
    for (int i = ctx->enclosing_class_depth - 1; i >= 0; i--) {
        const char *outer = ctx->enclosing_class_stack[i];
        if (!outer)
            continue;
        char buf[JAVA_LSP_BUF];
        int n = snprintf(buf, sizeof(buf), "%s.%s", outer, name);
        if (n > 0 && (size_t)n < sizeof(buf)) {
            if (cbm_registry_lookup_type(ctx->registry, buf)) {
                r.qn = cbm_arena_strdup(ctx->arena, buf);
                r.decided = true;
                return r;
            }
        }
        bool unnamed = false;
        unsigned met = 0;
        JavaUnseenJudge judge = {.reg = ctx->registry, .packages = ctx->project_packages};
        const char *member = java_inherited_member_ctx(ctx, outer, name, &unnamed, &judge, &met);
        r.unseen |= met;
        if (member || unnamed) {
            r.qn = member;
            r.decided = true;
            return r;
        }
        if ((met & JAVA_UNSEEN_PROJECT) && ctx->cross_run) {
            /* Rule (i): a spelling no type has, or a type the run's index
             * holds and this closure lacks, may declare the name; no proven
             * type here, and neither the outer classes, the imports nor the
             * package answer for it. A cross run judges this (its registry
             * holds the project or a closure of it); the per-file run's
             * registry holds one file and the JDK seed and judges nothing: it
             * answers as main does, and a cross answer replaces its answer at
             * that call. A supertype known outside the project, or of no
             * known package (rule ii), leaves the answer standing. */
            r.decided = true;
            r.withheld = true;
            return r;
        }
    }
    return r;
}

/* Step 0 once per (enclosing classes, simple name) in this file: the stack's
 * epoch and the name key a memo made once the registry is complete and
 * dropped when its type count changes, so each pair walks once (step 0 is a
 * function of the registry, the classes, the name and the judge). */
static JavaStep0 java_step0(JavaLSPContext *ctx, const char *name) {
    char key[JAVA_LSP_BUF];
    int n = -1;
    if (ctx->step0_ready) {
        n = snprintf(key, sizeof(key), "%u\x1f%s", ctx->class_epoch, name);
    }
    if (n <= 0 || (size_t)n >= sizeof(key)) {
        return java_step0_compute(ctx, name);
    }
    if (ctx->step0_memo && ctx->step0_memo_types != ctx->registry->type_count) {
        cbm_ht_free(ctx->step0_memo);
        ctx->step0_memo = NULL;
    }
    if (!ctx->step0_memo) {
        ctx->step0_memo = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 64);
        ctx->step0_memo_types = ctx->registry->type_count;
    }
    CBM_LSP_WORK(1);
    const JavaStep0 *hit =
        ctx->step0_memo ? (const JavaStep0 *)cbm_ht_get(ctx->step0_memo, key) : NULL;
    if (hit) {
        return *hit;
    }
    JavaStep0 r = java_step0_compute(ctx, name);
    JavaStep0 *slot = (JavaStep0 *)cbm_arena_alloc(ctx->arena, sizeof(*slot));
    const char *k = cbm_arena_strdup(ctx->arena, key);
    if (slot && k && ctx->step0_memo && ctx->step0_memo_types == ctx->registry->type_count) {
        *slot = r;
        cbm_ht_set(ctx->step0_memo, k, slot);
    }
    return r;
}

static const char *java_resolve_type_name_in(JavaLSPContext *ctx, const char *name,
                                             unsigned *unseen) {
    if (!name || !ctx)
        return NULL;

    /* 0. The enclosing classes (java_step0). */
    JavaStep0 s0 = java_step0(ctx, name);
    *unseen |= s0.unseen;
    if (s0.withheld) {
        atomic_fetch_add_explicit(&g_java_unseen_project, 1, memory_order_relaxed);
        if (s0.unseen & JAVA_UNSEEN_CLOSURE) {
            atomic_fetch_add_explicit(&g_java_unseen_closure, 1, memory_order_relaxed);
        }
    }
    if (s0.decided) {
        return s0.qn;
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

    /* 2. Single-type imports: an import binds the name, so nothing below
     * answers for it. The calling file's verified identity answers (the
     * declared-type step's, one type of the project registered once here).
     * Without one, the imported name answers only when it is no project
     * type: a JDK type of the seed, or a name the registry does not hold
     * (a library; the dispatch judges its package as before). A project type
     * the step could not verify gets no answer. */
    const char *imported = NULL;
    for (int i = java_import_first(ctx, name); i >= 0; i = java_import_next(ctx, name, i)) {
        const char *target = ctx->import_target_qns[i];
        if (ctx->import_kinds[i] == CBM_JAVA_IMPORT_IDENTITY && target &&
            !java_type_ambiguous(ctx->registry, target) &&
            cbm_registry_lookup_type(ctx->registry, target)) {
            return target;
        }
        if (ctx->import_kinds[i] == CBM_JAVA_IMPORT_TYPE && !imported) {
            imported = target;
        }
    }
    if (imported) {
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, imported);
        return rt && java_is_project_type(rt) ? NULL : imported;
    }

    /* 3. Java package prefix. A type of the own package shadows every
     * on-demand import, java.lang's included (JLS 6.4.1). */
    if (ctx->package_name && ctx->package_name[0]) {
        char buf[JAVA_LSP_BUF];
        int n = snprintf(buf, sizeof(buf), "%s.%s", ctx->package_name, name);
        if (n > 0 && (size_t)n < sizeof(buf)) {
            if (cbm_registry_lookup_type(ctx->registry, buf)) {
                return cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }

    /* 4. java.lang auto-import. Try registered first, then the conventional QN. */
    for (int i = 0; JAVA_LANG_TYPES[i]; i++) {
        if (strcmp(JAVA_LANG_TYPES[i], name) == 0) {
            char buf[JAVA_LSP_BUF];
            int n = snprintf(buf, sizeof(buf), "java.lang.%s", name);
            if (n > 0 && (size_t)n < sizeof(buf)) {
                return cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }

    /* 5. On-demand imports: the one type of them that has the name. Two
     * different ones make the name ambiguous (javac rejects the use): no
     * answer, never the first import's. */
    const char *on_demand = NULL;
    for (int i = 0; i < ctx->import_count; i++) {
        if (ctx->import_kinds[i] != CBM_JAVA_IMPORT_ON_DEMAND)
            continue;
        char buf[JAVA_LSP_BUF];
        int n = snprintf(buf, sizeof(buf), "%s.%s", ctx->import_target_qns[i], name);
        if (n > 0 && (size_t)n < sizeof(buf)) {
            if (cbm_registry_lookup_type(ctx->registry, buf)) {
                if (on_demand && strcmp(on_demand, buf) != 0) {
                    return NULL;
                }
                on_demand = on_demand ? on_demand : cbm_arena_strdup(ctx->arena, buf);
            }
        }
    }
    if (on_demand) {
        return on_demand;
    }

    /* Exact already-qualified name. */
    if (cbm_registry_lookup_type(ctx->registry, name)) {
        return cbm_arena_strdup(ctx->arena, name);
    }
    /* No scope of the file binds the name: no answer (a type of another
     * package is only visible through an import, which step 2 answers). */
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
    for (int i = java_import_first(ctx, name); i >= 0; i = java_import_next(ctx, name, i)) {
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

/* A type declared in the project: the cross registrar and the own-file pass
 * stamp namespace_qn on every project type; the JDK seed leaves it NULL. */
static bool java_is_project_type(const CBMRegisteredType *rt) {
    return rt && rt->namespace_qn && rt->namespace_qn[0];
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
        for (int i = java_import_first(ctx, mname); i >= 0; i = java_import_next(ctx, mname, i)) {
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

/* Java enum constants are direct enum_body children, while declarations after
 * the semicolon live in enum_body_declarations.  LSP class processing only
 * wants declarations, so expose the nested container for enums and the normal
 * body for every other type declaration. */
static TSNode java_type_declaration_body(TSNode node) {
    TSNode body = ts_node_child_by_field_name(node, "body", 4);
    if (ts_node_is_null(body) || strcmp(ts_node_type(body), "enum_body") != 0)
        return body;

    uint32_t count = ts_node_named_child_count(body);
    for (uint32_t i = 0; i < count; i++) {
        TSNode child = ts_node_named_child(body, i);
        if (strcmp(ts_node_type(child), "enum_body_declarations") == 0)
            return child;
    }
    return body;
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

    TSNode body = java_type_declaration_body(node);
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

/* The file's package declaration and import declarations, once. */
static void java_lsp_scan_imports(JavaLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root) || ctx->imports_scanned)
        return;
    ctx->imports_scanned = true;
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
}

void java_lsp_process_file(JavaLSPContext *ctx, TSNode root) {
    if (ts_node_is_null(root))
        return;
    /* First scan: package_declaration + imports (already pushed via init). */
    java_lsp_scan_imports(ctx, root);
    uint32_t n = ts_node_named_child_count(root);

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

/* True when `callee_qn` names a member (or a constructor) of a registered type
 * the project does not declare — a JDK seed type. Such a target has no graph
 * node, and the pipeline must not hand it to a same-named project method. */
static bool java_callee_is_external(const JavaLSPContext *ctx, const char *callee_qn) {
    if (!ctx->registry || !callee_qn)
        return false;
    const CBMRegisteredType *self = cbm_registry_lookup_type(ctx->registry, callee_qn);
    if (self)
        return !java_is_project_type(self);
    const char *dot = strrchr(callee_qn, '.');
    if (!dot || dot == callee_qn)
        return false;
    char owner[JAVA_LSP_BUF];
    size_t n = (size_t)(dot - callee_qn);
    if (n >= sizeof(owner))
        return false;
    memcpy(owner, callee_qn, n);
    owner[n] = '\0';
    const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, owner);
    return rt && !java_is_project_type(rt);
}

/* `T`, `ID`, `SELF`: a type-variable spelling — one or two characters, or
 * capitals, digits and '_' only — that the file imports neither by name nor
 * through its package (`import org.jooq.impl.*;` makes `DSL` a type). A type
 * variable the resolver failed to bind can reach a receiver qualified by some
 * package; it is unknown, not a library type. */
static bool java_is_type_var_spelling(const JavaLSPContext *ctx, const char *pkg, size_t pkg_len,
                                      const char *simple) {
    size_t n = strlen(simple);
    bool caps = n > 0;
    for (size_t i = 0; i < n && caps; i++) {
        caps = isupper((unsigned char)simple[i]) || isdigit((unsigned char)simple[i]) ||
               simple[i] == '_';
    }
    if (!caps && n > 2) {
        return false;
    }
    for (int i = 0; i < ctx->import_count; i++) {
        const char *target = ctx->import_target_qns[i];
        if (ctx->import_kinds[i] == CBM_JAVA_IMPORT_TYPE && ctx->import_local_names[i] &&
            strcmp(ctx->import_local_names[i], simple) == 0) {
            return false;
        }
        if (ctx->import_kinds[i] == CBM_JAVA_IMPORT_ON_DEMAND && target &&
            strlen(target) == pkg_len && strncmp(target, pkg, pkg_len) == 0) {
            return false;
        }
    }
    return true;
}

/* Whether the shared registry holds no project type under `prefix`
 * ("org.jooq."), memoised per package. */
static bool java_package_is_external(JavaLSPContext *ctx, const CBMTypeRegistry *base,
                                     const char *prefix) {
    for (int i = 0; i < JAVA_EXT_MEMO; i++) {
        if (ctx->ext_memo_pkg[i] && strcmp(ctx->ext_memo_pkg[i], prefix) == 0) {
            return ctx->ext_memo_val[i];
        }
    }
    CBMArena scratch;
    cbm_arena_init(&scratch);
    CBMQNPrefixIter it;
    bool project = false;
    /* The prefix search reads one layer, and the layer above the shared
     * registry is a per-file overlay that is often empty: walk the chain. */
    for (const CBMTypeRegistry *reg = base; reg && !project; reg = reg->fallback) {
        if (!cbm_registry_types_with_qn_prefix(reg, prefix, true, &scratch, &it))
            continue;
        for (int ti = cbm_qn_prefix_iter_next(&it); ti >= 0 && !project;
             ti = cbm_qn_prefix_iter_next(&it)) {
            project = java_is_project_type(&reg->types[ti]);
        }
    }
    cbm_arena_destroy(&scratch);
    int slot = ctx->ext_memo_next++ % JAVA_EXT_MEMO;
    ctx->ext_memo_pkg[slot] = prefix;
    ctx->ext_memo_val[slot] = !project;
    return !project;
}

/* A receiver type the registry does not know, spelled with a package the
 * project declares no type in (`org.jooq.DSLContext` from an import): a
 * library type. The package is the segments before the first capitalised
 * one, so a nested type (`com.acme.Order.OrderBuilder`, registered flattened
 * as `com.acme.OrderBuilder`) is judged by `com.acme`. Cross-file runs only —
 * the shared registry is the one that knows every project package, and its
 * type order makes the probe a binary search. A path-derived QN
 * (project-prefixed) is an unresolved project spelling, never a library. */
static bool java_receiver_is_external(JavaLSPContext *ctx, const char *recv_qn) {
    const CBMTypeRegistry *base = ctx->registry ? ctx->registry->fallback : NULL;
    const CBMJavaProjectPackages *packages = ctx->project_packages;
    if ((!base && !(packages && packages->declared)) || !recv_qn ||
        !islower((unsigned char)recv_qn[0]))
        return false;
    if (packages && packages->declared) {
        base = NULL; /* the run's package table judges (a closure's registry is partial) */
    }
    const char *type = NULL;
    for (const char *p = recv_qn; (p = strchr(p, '.')) != NULL; p++) {
        if (isupper((unsigned char)p[1])) {
            type = p + 1;
            break;
        }
    }
    if (!type)
        return false;
    size_t pkg_len = (size_t)(type - 1 - recv_qn);
    const char *last = strrchr(recv_qn, '.');
    if (java_is_type_var_spelling(ctx, recv_qn, pkg_len, last + 1))
        return false;
    /* The project segment; a file at the repository root has a module QN
     * that is the project name alone. */
    const char *mod_dot = ctx->module_qn ? strchr(ctx->module_qn, '.') : NULL;
    size_t mod_len = mod_dot ? (size_t)(mod_dot - ctx->module_qn)
                             : (ctx->module_qn ? strlen(ctx->module_qn) : 0);
    if (mod_len && strncmp(recv_qn, ctx->module_qn, mod_len) == 0 && recv_qn[mod_len] == '.')
        return false;
    if (ctx->package_name && strlen(ctx->package_name) == pkg_len &&
        strncmp(ctx->package_name, recv_qn, pkg_len) == 0)
        return false;
    if (!base) {
        /* No shared registry (a per-file cross run): the run's package table
         * tells whether the project declares the package or one below it. */
        return !packages->declared(packages->ud, recv_qn, pkg_len);
    }
    /* "org.jooq." */
    return java_package_is_external(ctx, base, cbm_arena_strndup(ctx->arena, recv_qn, pkg_len + 1));
}

static void java_emit_resolved_orig_kind(JavaLSPContext *ctx, const char *callee_qn,
                                         const char *orig, const char *strategy, float confidence,
                                         CBMResolvedKind kind) {
    if (!ctx->resolved_calls || !ctx->enclosing_method_qn || !callee_qn)
        return;
    if (kind == CBM_RESOLVED_INVOCATION && java_callee_is_external(ctx, callee_qn)) {
        strategy = "lsp_external";
    }
    java_count_unknown_scope_answer(ctx, callee_qn, strategy);
    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_method_qn;
    rc.callee_qn = callee_qn;
    rc.strategy = strategy;
    rc.confidence = confidence;
    rc.kind = kind;
    // For data-flow resolutions, `reason` may carry the original textual
    // callee when it differs from the resolved target. NULL for normal calls.
    rc.reason = orig;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

static void java_emit_resolved_orig(JavaLSPContext *ctx, const char *callee_qn, const char *orig,
                                    const char *strategy, float confidence) {
    java_emit_resolved_orig_kind(ctx, callee_qn, orig, strategy, confidence,
                                 CBM_RESOLVED_INVOCATION);
}

static void java_emit_resolved(JavaLSPContext *ctx, const char *callee_qn, const char *strategy,
                               float confidence) {
    java_emit_resolved_orig(ctx, callee_qn, NULL, strategy, confidence);
}

static void java_emit_resolved_reference(JavaLSPContext *ctx, const char *callee_qn,
                                         const char *strategy, float confidence) {
    java_emit_resolved_orig_kind(ctx, callee_qn, NULL, strategy, confidence,
                                 CBM_RESOLVED_CALL_REFERENCE);
}

static void java_stamp_resolved_site(JavaLSPContext *ctx, int first, TSNode site) {
    if (!ctx->resolved_calls || ts_node_is_null(site) || first < 0) {
        return;
    }
    for (int i = first; i < ctx->resolved_calls->count; i++) {
        ctx->resolved_calls->items[i].site_start_byte = ts_node_start_byte(site);
        ctx->resolved_calls->items[i].site_end_byte = ts_node_end_byte(site);
    }
}

static void java_emit_resolved_reference_at(JavaLSPContext *ctx, const char *callee_qn,
                                            const char *strategy, float confidence, TSNode site) {
    int first = ctx->resolved_calls ? ctx->resolved_calls->count : -1;
    java_emit_resolved_reference(ctx, callee_qn, strategy, confidence);
    java_stamp_resolved_site(ctx, first, site);
}

static void java_emit_unresolved_kind(JavaLSPContext *ctx, const char *expr_text,
                                      const char *reason, CBMResolvedKind kind) {
    if (!ctx->resolved_calls || !ctx->enclosing_method_qn)
        return;
    CBMResolvedCall rc = {0};
    rc.caller_qn = ctx->enclosing_method_qn;
    rc.callee_qn = expr_text ? expr_text : "?";
    rc.strategy = "lsp_unresolved";
    rc.confidence = 0.0f;
    rc.reason = reason;
    rc.kind = kind;
    cbm_resolvedcall_push(ctx->resolved_calls, ctx->arena, rc);
}

static void java_emit_unresolved(JavaLSPContext *ctx, const char *expr_text, const char *reason) {
    java_emit_unresolved_kind(ctx, expr_text, reason, CBM_RESOLVED_INVOCATION);
}

static void java_emit_unresolved_reference(JavaLSPContext *ctx, const char *expr_text,
                                           const char *reason) {
    java_emit_unresolved_kind(ctx, expr_text, reason, CBM_RESOLVED_CALL_REFERENCE);
}

static void java_emit_unresolved_reference_at(JavaLSPContext *ctx, const char *expr_text,
                                              const char *reason, TSNode site) {
    int first = ctx->resolved_calls ? ctx->resolved_calls->count : -1;
    java_emit_unresolved_reference(ctx, expr_text, reason);
    java_stamp_resolved_site(ctx, first, site);
}

/* Whether `f` takes `arity` arguments: its parameter count, or the range a
 * registered minimum opens; a last parameter that is an array (`T...` or
 * `T[]`) takes any count from one less on, as varargs do. A signature the
 * registry does not model takes any count. */
static bool java_func_takes(const CBMRegisteredFunc *f, int arity) {
    const CBMType *sig = f ? f->signature : NULL;
    if (!sig || sig->kind != CBM_TYPE_FUNC) {
        return true;
    }
    int pc = 0;
    while (sig->data.func.param_types && sig->data.func.param_types[pc]) {
        pc++;
    }
    const CBMType *last = pc > 0 ? sig->data.func.param_types[pc - 1] : NULL;
    const char *last_name =
        last && last->kind == CBM_TYPE_NAMED ? last->data.named.qualified_name : NULL;
    if (last && (last->kind == CBM_TYPE_SLICE || (last_name && strstr(last_name, "...")) ||
                 (last_name && strstr(last_name, "[]")))) {
        return arity >= pc - 1;
    }
    int min = f->min_params >= 0 ? f->min_params : pc;
    return arity >= min && arity <= pc;
}

/* A parameter type's erased name, when the position can prove two methods
 * distinct: a type the registry resolves, a primitive, or a written qualified
 * name (*resolved tells which); NULL for a type variable, an unknown type or a
 * simple name the registry does not hold (it may be a type variable). *dims
 * counts the array dimensions. */
static const char *java_param_name(JavaLSPContext *ctx, const CBMType *t, int *dims,
                                   bool *resolved) {
    *dims = 0;
    *resolved = false;
    while (t && t->kind == CBM_TYPE_SLICE) {
        (*dims)++;
        t = t->data.slice.elem;
    }
    if (!t) {
        return NULL;
    }
    if (t->kind == CBM_TYPE_BUILTIN) {
        *resolved = true;
        return t->data.builtin.name;
    }
    const char *name = t->kind == CBM_TYPE_NAMED      ? t->data.named.qualified_name
                       : t->kind == CBM_TYPE_TEMPLATE ? t->data.template_type.template_name
                                                      : NULL;
    if (!name || !name[0]) {
        return NULL;
    }
    *resolved = cbm_registry_lookup_type(ctx->registry, name) != NULL;
    return *resolved || strchr(name, '.') ? name : NULL;
}

/* Two methods of one name are provably distinct (overloads, not one method
 * and its override, nor one method registered twice) when at some position
 * both declared parameter types are concrete and differ: both resolved by the
 * registry, or both written qualified names, and not equal. A type variable,
 * an unknown type, or a position where only one side resolves proves
 * nothing. No overload is ever chosen by an argument's type. */
static bool java_funcs_distinct(JavaLSPContext *ctx, const CBMRegisteredFunc *a,
                                const CBMRegisteredFunc *b) {
    const CBMType *sa = a->signature;
    const CBMType *sb = b->signature;
    if (a == b || !sa || !sb || sa->kind != CBM_TYPE_FUNC || sb->kind != CBM_TYPE_FUNC ||
        !sa->data.func.param_types || !sb->data.func.param_types) {
        return false;
    }
    const CBMType **pa = sa->data.func.param_types;
    const CBMType **pb = sb->data.func.param_types;
    for (int i = 0; pa[i] && pb[i]; i++) {
        int da = 0;
        int db = 0;
        bool ra = false;
        bool rb = false;
        const char *na = java_param_name(ctx, pa[i], &da, &ra);
        const char *nb = java_param_name(ctx, pb[i], &db, &rb);
        if (!na || !nb || ra != rb) {
            continue; /* this position proves nothing */
        }
        if (da != db || strcmp(na, nb) != 0) {
            return true;
        }
    }
    return false;
}

/* The methods named `mname` that the class `cls` declares and that take
 * `arity` arguments, each once; *one is set to one of them. Only methods
 * provably distinct from `like` (and from each other) are counted: one that
 * overrides `like` is `like`. */
static int java_level_taking(JavaLSPContext *ctx, const char *cls, const char *mname, int arity,
                             const CBMRegisteredFunc *like, const CBMRegisteredFunc **one) {
    const CBMRegisteredFunc *seen[2] = {0};
    int count = 0;
    CBMMethodIter it;
    cbm_registry_methods_chain(ctx->registry, cls, mname, &it);
    for (int index; (index = cbm_method_iter_next(&it)) >= 0;) {
        CBM_LSP_WORK(1);
        const CBMRegisteredFunc *g = &it.reg->funcs[index];
        if (!java_func_takes(g, arity) || (like && !java_funcs_distinct(ctx, g, like))) {
            continue;
        }
        bool again = false;
        for (int k = 0; k < count && k < 2; k++) {
            again = again || !java_funcs_distinct(ctx, g, seen[k]);
        }
        if (again) {
            continue;
        }
        if (count < 2) {
            seen[count] = g;
        }
        count++;
        if (count > 1) {
            break; /* two: no single one */
        }
    }
    if (one) {
        *one = count == 1 ? seen[0] : NULL;
    }
    return count;
}

/* The inherited rule (lsp_inherited_dispatch) names the method `f` of the
 * nearest supertype that declares `mname`. javac binds the most specific
 * method applicable to the arguments' types, which can be another overload,
 * nearer or farther; the engine chooses by name and argument count only.
 * So the rule answers only where that count leaves one method: the nearest
 * declaring class has exactly one method of the name that takes the call's
 * argument count, and no farther supertype declares another one that does
 * and is provably distinct from it (java_funcs_distinct: an override is the
 * same method). Returns that method, or NULL for no answer. A JDK seed class
 * keeps its answer: its parameters are not modelled. */
static const CBMRegisteredFunc *java_inherited_unique_compute(JavaLSPContext *ctx,
                                                              const CBMRegisteredFunc *f,
                                                              const char *mname, int arity) {
    const CBMRegisteredType *lt =
        f->receiver_type ? cbm_registry_lookup_type(ctx->registry, f->receiver_type) : NULL;
    if (!java_is_project_type(lt)) {
        return f;
    }
    const CBMRegisteredFunc *m = NULL;
    if (java_level_taking(ctx, f->receiver_type, mname, arity, NULL, &m) != 1 || !m) {
        return NULL;
    }
    const char *cur = lt->embedded_types ? lt->embedded_types[0] : NULL;
    for (int hops = 0; hops < JAVA_LSP_MAX_INHERIT_HOPS && cur; hops++) {
        CBM_LSP_WORK(1);
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, cur);
        if (!rt) {
            break;
        }
        if (java_level_taking(ctx, cur, mname, arity, m, NULL) > 0) {
            return NULL; /* a farther overload: the argument types choose */
        }
        cur = rt->embedded_types ? rt->embedded_types[0] : NULL;
    }
    return m;
}

/* java_inherited_unique_compute once per (declaring class, method name,
 * argument count) in a file, made once the registry is complete and dropped
 * when it grows (the answer is a function of the registry and the key). */
static const CBMRegisteredFunc *java_inherited_unique(JavaLSPContext *ctx,
                                                      const CBMRegisteredFunc *f,
                                                      const char *mname, int arity) {
    static const char no_answer; /* the memo's value for "no answer" */
    char key[JAVA_LSP_BUF];
    int n = -1;
    if (ctx->step0_ready && f->receiver_type) {
        n = snprintf(key, sizeof(key), "%s\x1f%s\x1f%d", f->receiver_type, mname, arity);
    }
    if (n <= 0 || (size_t)n >= sizeof(key)) {
        return java_inherited_unique_compute(ctx, f, mname, arity);
    }
    int size = ctx->registry->type_count + ctx->registry->func_count;
    if (ctx->inherit_memo && ctx->inherit_memo_types != size) {
        cbm_ht_free(ctx->inherit_memo);
        ctx->inherit_memo = NULL;
    }
    if (!ctx->inherit_memo) {
        ctx->inherit_memo = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 64);
        ctx->inherit_memo_types = size;
    }
    CBM_LSP_WORK(1);
    const void *hit = ctx->inherit_memo ? cbm_ht_get(ctx->inherit_memo, key) : NULL;
    if (hit) {
        return hit == (const void *)&no_answer ? NULL : (const CBMRegisteredFunc *)hit;
    }
    const CBMRegisteredFunc *m = java_inherited_unique_compute(ctx, f, mname, arity);
    const char *k = cbm_arena_strdup(ctx->arena, key);
    if (k && ctx->inherit_memo) {
        cbm_ht_set(ctx->inherit_memo, k, m ? (void *)m : (void *)&no_answer);
    }
    return m;
}

/* The answer for a method `f` found on the class `cls` (the receiver's or
 * the enclosing class): type dispatch on its own method, the inherited rule
 * on a supertype's, or no answer where the inherited rule leaves the choice
 * to the argument types. */
static void java_emit_found_method(JavaLSPContext *ctx, const char *cls,
                                   const CBMRegisteredFunc *f, const char *mname, int arity) {
    if (!f->receiver_type || strcmp(f->receiver_type, cls) == 0) {
        java_emit_resolved(ctx, f->qualified_name, "lsp_type_dispatch", 0.95f);
        return;
    }
    const CBMRegisteredFunc *m = java_inherited_unique(ctx, f, mname, arity);
    if (!m) {
        java_emit_unresolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", cls, mname),
                             "inherited_overload_by_types");
        return;
    }
    java_emit_resolved(ctx, m->qualified_name, "lsp_inherited_dispatch", 0.95f);
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
                java_emit_found_method(ctx, ctx->enclosing_class_qn, f, mname, arity);
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
        for (int i = java_import_first(ctx, mname); i >= 0; i = java_import_next(ctx, mname, i)) {
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
            /* An interface receiver binds the interface's own (abstract or
             * default) method, as javac's invokeinterface names it, whatever
             * implements it: never the one implementer a file declares. A
             * library interface's method has no node, so no edge follows. */
            java_emit_found_method(ctx, recv_qn, f, mname, arity);
            return;
        }
        /* An interface whose registered types do not declare the method
         * (one of a library super-interface): the compiler binds a method
         * with no node here, and no implementer is the target. A library
         * interface's call (a seeded JDK interface, `IntPredicate.test`) is
         * a library call: its exact QN, which no project method is, as for a
         * library receiver the registry does not hold. */
        const CBMRegisteredType *rt = cbm_registry_lookup_type(ctx->registry, recv_qn);
        if (rt && rt->is_interface) {
            const char *target = cbm_arena_sprintf(ctx->arena, "%s.%s", recv_qn, mname);
            if (!java_is_project_type(rt) || java_receiver_is_external(ctx, recv_qn)) {
                java_emit_resolved(ctx, target, "lsp_external", 0.90f);
            } else {
                java_emit_unresolved(ctx, target, "interface_method_unregistered");
            }
            return;
        }
        /* A library type the registry does not hold: no method of it can be
         * looked up, but no project method is its target either. A seeded
         * JDK type with no such method stays a diagnostic below, as Rust's
         * gate (#2053) needs a positively resolved std method. */
        if (!rt && java_receiver_is_external(ctx, recv_qn)) {
            java_emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", recv_qn, mname),
                               "lsp_external", 0.90f);
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
    TSNode name_node = {0};
    if (nc_named >= 2) {
        name_node = ts_node_named_child(mref, nc_named - 1);
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
                /* Usage extraction represents `Type::new` by its type leaf,
                 * because `new` is an unnamed grammar token. Keep the LSP
                 * occurrence on that same leaf so the two passes join. */
                name_node = lhs;
                break;
            }
            if (strcmp(ck, "identifier") == 0) {
                /* Skip if it's the LHS. */
                if (ts_node_eq(c, lhs))
                    continue;
                mname = java_node_text(ctx, c);
                if (mname && mname[0]) {
                    name_node = c;
                    break;
                }
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
        java_emit_unresolved_reference_at(ctx, mname, "method_reference_unknown_lhs", name_node);
        return;
    }

    /* Constructor reference: ClassName::new */
    if (strcmp(mname, "new") == 0) {
        const char *short_name = strrchr(type_qn, '.');
        short_name = short_name ? short_name + 1 : type_qn;
        const CBMRegisteredFunc *cf =
            cbm_registry_lookup_method(ctx->registry, type_qn, short_name);
        /* A constructor reference is precise only when a concrete constructor
         * callable exists in the registry. Never relabel the class node itself
         * as a callable target; if the constructor is absent/unmaterialized,
         * the usage pass will retain the type occurrence as ordinary USAGE. */
        if (cf && cf->qualified_name) {
            java_emit_resolved_reference_at(ctx, cf->qualified_name, "lsp_method_ref_ctor", 0.90f,
                                            name_node);
        } else {
            java_emit_unresolved_reference_at(
                ctx, type_qn, "method_reference_constructor_not_materialized", name_node);
        }
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
        java_emit_resolved_reference_at(ctx, m->qualified_name, "lsp_method_ref", 0.90f, name_node);
    } else {
        java_emit_unresolved_reference_at(ctx,
                                          cbm_arena_sprintf(ctx->arena, "%s.%s", type_qn, mname),
                                          "method_reference_no_match", name_node);
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
        int first_resolution = ctx->resolved_calls ? ctx->resolved_calls->count : -1;
        resolve_method_call(ctx, node);
        /* Raw CALL extraction records the full method_invocation span. Stamp
         * the semantic record with that identical occurrence so two calls to
         * the same method name in one caller retain receiver identity at the
         * pipeline join (for example alpha.handle(); beta.handle();). */
        java_stamp_resolved_site(ctx, first_resolution, node);

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
        int first_resolution = ctx->resolved_calls ? ctx->resolved_calls->count : -1;
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
                const CBMRegisteredType *ct =
                    cf ? NULL : cbm_registry_lookup_type(ctx->registry, qn);
                if (cf) {
                    java_emit_resolved(ctx, cf->qualified_name, "lsp_constructor", 0.95f);
                } else if ((ct && !java_is_project_type(ct)) ||
                           (!ct && java_receiver_is_external(ctx, qn))) {
                    /* A library class's constructor (a JDK seed class, or
                     * one outside the project's packages): its exact QN,
                     * which no project method is. The class's own QN has no
                     * node either, and the call join would bind it to any
                     * project constructor of that name. */
                    java_emit_resolved(ctx, cbm_arena_sprintf(ctx->arena, "%s.%s", qn, short_name),
                                       "lsp_external", 0.90f);
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
        java_stamp_resolved_site(ctx, first_resolution, node);
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

/* The text inside the outermost angle brackets of a type text ("String,
 * List<Integer>" for "Map<String, List<Integer>>"), or NULL. */
static const char *java_generic_args_inner(CBMArena *a, const char *no_arr) {
    const char *lt = no_arr ? strchr(no_arr, '<') : NULL;
    if (!lt) {
        return NULL;
    }
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
    return gt && gt > lt + 1 ? cbm_arena_strndup(a, lt + 1, (size_t)(gt - lt - 1)) : NULL;
}

/* Parse a type-text of this file's own definitions into a CBMType, with
 * inner-class qualification. `parent_class` is the QN of the enclosing class
 * (NULL at file scope), used to qualify unqualified inner-class references
 * through the enclosing classes of this file down to its module `module_qn`.
 * `reg` is consulted to confirm matches before committing to a candidate QN.
 * Another file's declared types are resolved by java_decl_type.
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
    const char *gen_args_inner = java_generic_args_inner(a, no_arr);

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
            /* Walk the enclosing classes, parent.X, parent.parent.X, ...,
             * down to the file's own module (the walk stops there). */
            size_t module_len = module_qn ? strlen(module_qn) : 0;
            if (!base && parent_class && reg) {
                const char *cur = parent_class;
                while (cur && strlen(cur) >= module_len) {
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
            /* No scope of the file declares it: a name the file can only see
             * through an import, or a type variable. No answer. */
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

/* ── Another file's declared types (the cross registrar) ──────────────
 *
 * A declared type text of another file's definition (a return, parameter or
 * field type, a supertype) is read through the identities the declared-type
 * step gave its spellings in the scope of the file that wrote it
 * (CBMLSPDef.type_scope, in this registry's QN form). A spelling with one
 * names that project type, unless two types share its QN here (no answer).
 * A spelling without one names no project type: a primitive, a spelling
 * written fully qualified, a java.lang type, for a parameter (as before) a
 * type of the JDK packages the seed registers, and otherwise nothing. Before
 * all of these, a simple name written in a class body is a member type that
 * class or an enclosing class declares or inherits (JLS 6.4.1, 8.5), which
 * shadows the imports and the package the identity came from. */
typedef struct {
    const cbm_type_scope_index_t *ix; /* the def's identities, or NULL */
    const CBMTypeRegistry *reg;
    bool library_packages; /* the JDK package fallback (parameters) */
    const char *owner;     /* the class whose body holds the text, or NULL */
} JavaDeclScope;

/* The member type `name` that the class `owner` or one of its enclosing
 * classes (the member index's owner links) declares or inherits, innermost
 * first, or NULL; *unnamed as java_declared_member_type. Each class once. */
/* The run's package table while a closure registrar runs
 * (cbm_run_java_lsp_cross_packages); NULL for the shared build. */
static _Thread_local const CBMJavaProjectPackages *g_java_reg_packages;

/* java_decl_member per (owner, name) while one registrar reads the declared
 * types of its defs (java_decl_memo_begin to java_decl_memo_end: the fields,
 * then the signatures): the walk is a function of the registry, the owner and
 * the name, and no type changes meanwhile. withheld: the JAVA_UNSEEN_* bits
 * of a rule-(i) answer, counted at every lookup. */
typedef struct {
    const char *found;
    unsigned withheld;
    bool unnamed;
} JavaDeclMemo;

static _Thread_local CBMHashTable *g_java_decl_memo;

static void java_decl_memo_begin(void) {
    cbm_ht_free(g_java_decl_memo); /* NULL-safe */
    g_java_decl_memo = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 256);
}

static void java_decl_memo_end(void) {
    cbm_ht_free(g_java_decl_memo); /* NULL-safe; its entries live in the registrar's arena */
    g_java_decl_memo = NULL;
}

static const char *java_decl_member_walk(const JavaDeclScope *s, CBMArena *a, const char *name,
                                         bool *unnamed, unsigned *withheld) {
    const char *local[JAVA_TYPE_QUEUE_LOCAL] = {0};
    JavaUnseenJudge judge = {.reg = s->reg, .packages = g_java_reg_packages};
    JavaTypeQueue chain = {.items = local, .cap = JAVA_TYPE_QUEUE_LOCAL};
    const char *found = NULL;
    for (const char *o = s->owner; o && !found && !*unnamed;) {
        int before = chain.count;
        java_type_queue_push(a, &chain, o);
        if (chain.count == before) {
            break; /* met again: the owner links loop */
        }
        found = java_declared_member_type(s->reg, a, o, name, unnamed);
        if (found || *unnamed) {
            break;
        }
        unsigned unseen = 0;
        found = java_inherited_member(s->reg, a, o, name, unnamed, &judge, &unseen);
        if (found || *unnamed) {
            break;
        }
        if (unseen & JAVA_UNSEEN_PROJECT) {
            *unnamed = true; /* rule (i): no proven type for the name */
            *withheld = unseen & (JAVA_UNSEEN_PROJECT | JAVA_UNSEEN_CLOSURE);
            break;
        }
        if (java_type_ambiguous(s->reg, o)) {
            break; /* two types share the QN: which one encloses is unknown */
        }
        char key[JAVA_LSP_BUF];
        int n = snprintf(key, sizeof(key), JAVA_OWNER_KEY "%s", o);
        const CBMRegisteredType *e =
            n > 0 && (size_t)n < sizeof(key) ? cbm_registry_lookup_type(s->reg, key) : NULL;
        o = e ? e->alias_of : NULL;
    }
    cbm_ht_free(chain.seen); /* NULL-safe */
    return found;
}

static const char *java_decl_member(const JavaDeclScope *s, CBMArena *a, const char *name,
                                    bool *unnamed) {
    char key[JAVA_LSP_BUF];
    int n = -1;
    if (g_java_decl_memo && s->owner) {
        n = snprintf(key, sizeof(key), "%s\x1f%s", s->owner, name);
    }
    JavaDeclMemo r = {0};
    const JavaDeclMemo *hit = NULL;
    if (n > 0 && (size_t)n < sizeof(key)) {
        CBM_LSP_WORK(1);
        hit = (const JavaDeclMemo *)cbm_ht_get(g_java_decl_memo, key);
    }
    if (hit) {
        r = *hit;
    } else {
        r.found = java_decl_member_walk(s, a, name, &r.unnamed, &r.withheld);
        JavaDeclMemo *slot = n > 0 && (size_t)n < sizeof(key)
                                 ? (JavaDeclMemo *)cbm_arena_alloc(a, sizeof(*slot))
                                 : NULL;
        const char *k = slot ? cbm_arena_strdup(a, key) : NULL;
        if (slot && k) {
            *slot = r;
            cbm_ht_set(g_java_decl_memo, k, slot);
        }
    }
    if (r.withheld & JAVA_UNSEEN_PROJECT) {
        atomic_fetch_add_explicit(&g_java_unseen_project, 1, memory_order_relaxed);
        if (r.withheld & JAVA_UNSEEN_CLOSURE) {
            atomic_fetch_add_explicit(&g_java_unseen_closure, 1, memory_order_relaxed);
        }
    }
    *unnamed = r.unnamed;
    return r.found;
}

static const CBMType *java_decl_type(CBMArena *a, const char *text, const JavaDeclScope *s) {
    if (!text || !s)
        return cbm_type_unknown();
    int dim = 0;
    const char *no_arr = unwrap_array_text(a, text, &dim);
    const char *no_gen = strip_generics(a, no_arr);
    const char *gen_args_inner = java_generic_args_inner(a, no_arr);
    if (!no_gen || !no_gen[0])
        return cbm_type_unknown();
    const CBMType *base = NULL;
    const char *identity = cbm_type_scope_find(s->ix, no_gen, strlen(no_gen));
    bool unnamed = false;
    const char *member = s->owner && !strchr(no_gen, '.') && !is_java_primitive(no_gen)
                             ? java_decl_member(s, a, no_gen, &unnamed)
                             : NULL;
    if (is_java_primitive(no_gen)) {
        base = cbm_type_builtin(a, no_gen);
    } else if (member) {
        base = cbm_type_named(a, member);
    } else if (unnamed) {
        base = NULL; /* that member, which no QN names here */
    } else if (identity) {
        /* A project type: the identity, or nothing when the registry holds
         * it as two types (or not at all). */
        if (s->reg && !java_type_ambiguous(s->reg, identity) &&
            cbm_registry_lookup_type(s->reg, identity)) {
            base = cbm_type_named(a, identity);
        }
    } else if (strchr(no_gen, '.')) {
        base = cbm_type_named(a, no_gen);
    } else if (cbm_java_lang_type(no_gen, strlen(no_gen))) {
        base = cbm_type_named(a, cbm_arena_sprintf(a, "java.lang.%s", no_gen));
    } else if (s->library_packages && s->reg) {
        for (int i = 0; JAVA_FALLBACK_PACKAGES[i] && !base; i++) {
            const char *cand = cbm_arena_sprintf(a, "%s.%s", JAVA_FALLBACK_PACKAGES[i], no_gen);
            if (cbm_registry_lookup_type(s->reg, cand)) {
                base = cbm_type_named(a, cand);
            }
        }
    }
    if (!base)
        return cbm_type_unknown();
    if (gen_args_inner && base->kind == CBM_TYPE_NAMED) {
        int gc = 0;
        const char **arg_strs = split_generic_args(a, gen_args_inner, &gc);
        if (arg_strs && gc > 0) {
            const CBMType **gargs =
                (const CBMType **)cbm_arena_alloc(a, (size_t)(gc + 1) * sizeof(*gargs));
            if (!gargs)
                return base;
            for (int i = 0; i < gc; i++) {
                gargs[i] = java_decl_type(a, arg_strs[i], s);
            }
            gargs[gc] = NULL;
            base = cbm_type_template(a, base->data.named.qualified_name, gargs, gc);
        }
    }
    while (dim-- > 0)
        base = cbm_type_slice(a, base);
    return base;
}

static const CBMType *java_decl_parse_type(CBMArena *arena, const char *text, void *parser_ctx) {
    return java_decl_type(arena, text, (const JavaDeclScope *)parser_ctx);
}

typedef struct {
    const char *parent_class;
    const char *module_qn;
    const CBMTypeRegistry *registry;
} JavaSignatureParseContext;

static const CBMType *java_signature_parse_type(CBMArena *arena, const char *text,
                                                void *parser_ctx) {
    const JavaSignatureParseContext *ctx = (const JavaSignatureParseContext *)parser_ctx;
    return parse_param_text_full(arena, text, ctx ? ctx->parent_class : NULL,
                                 ctx ? ctx->module_qn : NULL, ctx ? ctx->registry : NULL);
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
        stub.namespace_qn = (ctx->package_name && ctx->package_name[0]) ? ctx->package_name
                            : (module_qn && module_qn[0])               ? module_qn
                                                                        : d->qualified_name;
        cbm_registry_add_type(reg, stub);
    }
    /* Every local type is in: from here on a type is found by name, not by a scan of
     * every registered type per definition (first registrations, as the scan). */
    if (reg->tail_arena)
        cbm_registry_index_types_into(reg, reg->tail_arena);

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
        CBMRegisteredType *slot = cbm_registry_first_own_type(reg, d->qualified_name);
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
        const char **param_names = NULL;
        if (d->signature_param_types || d->signature_param_count > 0) {
            JavaSignatureParseContext parse_ctx = {
                .parent_class = parent,
                .module_qn = module_qn,
                .registry = reg,
            };
            ptypes = cbm_type_materialize_signature_params(a, d->signature_param_types,
                                                           d->signature_param_count,
                                                           java_signature_parse_type, &parse_ctx);
        } else if (!d->signature_param_types && d->signature_param_count == 0) {
            /* Compatibility for callers that still construct CBMDefinition by
             * hand.  The legacy projection is deduplicated/non-positional, so
             * it is consulted only when the counted carrier is wholly absent. */
            if (d->param_types) {
                int pc = 0;
                while (d->param_types[pc])
                    pc++;
                if (pc > 0) {
                    ptypes =
                        (const CBMType **)cbm_arena_alloc(a, (size_t)(pc + 1) * sizeof(*ptypes));
                    for (int j = 0; j < pc; j++) {
                        ptypes[j] =
                            parse_param_text_full(a, d->param_types[j], parent, module_qn, reg);
                    }
                    ptypes[pc] = NULL;
                }
            }
            param_names = d->param_names;
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
        rf.signature = cbm_type_func(a, param_names, ptypes, rtypes);
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

/* The next registered function of this QN after `row` (-1: the first), in
 * registration order. By name when the caller listed the functions in
 * ctx->func_names and none was added since; otherwise by a scan. */
static int java_func_row_next(JavaLSPContext *ctx, const CBMTypeRegistry *reg, const char *qn,
                              int row) {
    if (ctx->func_names && ctx->func_name_count == reg->func_count) {
        return row < 0 ? cbm_name_rows_first(&ctx->func_rows, ctx->func_rows_arena, ctx->func_names,
                                             ctx->func_name_count, qn)
                       : cbm_name_rows_next(&ctx->func_rows, ctx->func_names, ctx->func_name_count,
                                            qn, row);
    }
    for (int fi = row + 1; fi < reg->func_count; fi++) {
        CBM_LSP_WORK(1);
        if (reg->funcs[fi].qualified_name && strcmp(reg->funcs[fi].qualified_name, qn) == 0)
            return fi;
    }
    return -1;
}

static void patch_one_method(JavaLSPContext *ctx, CBMTypeRegistry *reg, TSNode method_node,
                             const char *class_qn, bool is_constructor) {
    TSNode name_node = ts_node_child_by_field_name(method_node, "name", 4);
    if (ts_node_is_null(name_node))
        return;
    char *mname = java_node_text(ctx, name_node);
    if (!mname)
        return;
    char *method_qn = cbm_arena_sprintf(ctx->arena, "%s.%s", class_qn, mname);

    /* Re-extract param types from the AST. */
    TSNode params = ts_node_child_by_field_name(method_node, "parameters", 10);
    if (ts_node_is_null(params))
        return;
    uint32_t pn = ts_node_named_child_count(params);
    const CBMType **ptypes = NULL;
    if (pn > 0) {
        ptypes = (const CBMType **)cbm_arena_alloc(ctx->arena, (size_t)(pn + 1) * sizeof(*ptypes));
        if (!ptypes)
            return;
    }
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
    if (ptypes)
        ptypes[idx] = NULL;

    /* Java overloads share a graph QN. Select the registry slot whose counted
     * signature has the same arity as this AST declaration; falling back to the
     * first QN match keeps the legacy no-signature path best-effort. */
    CBMRegisteredFunc *slot = NULL;
    CBMRegisteredFunc *first_qn_match = NULL;
    for (int fi = java_func_row_next(ctx, reg, method_qn, -1); fi >= 0;
         fi = java_func_row_next(ctx, reg, method_qn, fi)) {
        CBMRegisteredFunc *candidate = &reg->funcs[fi];
        if (!candidate->qualified_name || strcmp(candidate->qualified_name, method_qn) != 0)
            continue;
        if (!first_qn_match)
            first_qn_match = candidate;
        if (!candidate->signature || candidate->signature->kind != CBM_TYPE_FUNC)
            continue;
        int candidate_arity = 0;
        const CBMType *const *candidate_params = candidate->signature->data.func.param_types;
        if (candidate_params) {
            while (candidate_params[candidate_arity])
                candidate_arity++;
        }
        if (candidate_arity == idx) {
            slot = candidate;
            break;
        }
    }
    if (!slot)
        slot = first_qn_match;
    if (!slot)
        return;

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

    TSNode body = java_type_declaration_body(class_node);
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
    CBMRegisteredType *slot = cbm_registry_first_own_type(reg, class_qn);
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

    TSNode body = java_type_declaration_body(class_node);
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

enum { JAVA_SCRATCH_ARENA_BLOCK = 4096 };

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
        /* An on-demand import ("*") binds no name; the AST scan records it. */
        if (!imp->local_name || !imp->module_path || strcmp(imp->local_name, "*") == 0)
            continue;
        java_lsp_add_import(&ctx, imp->local_name, imp->module_path, CBM_JAVA_IMPORT_TYPE);
    }

    /* Register file-local defs into the registry. Its lookups run while it
     * fills, so it indexes its types as they come, in a scratch arena. */
    CBMArena scratch;
    cbm_arena_init_lazy(&scratch, JAVA_SCRATCH_ARENA_BLOCK);
    reg.tail_arena = &scratch;
    register_local_func_or_type_from_file(&ctx, &reg, result);

    /* Overloads share a QN, so patching a method asks for all of its registrations. */
    ctx.func_rows_arena = &scratch;
    ctx.func_names =
        reg.func_count > 0
            ? (const char **)cbm_arena_alloc(&scratch, (size_t)reg.func_count * sizeof(char *))
            : NULL;
    ctx.func_name_count = ctx.func_names ? reg.func_count : 0;
    for (int fi = 0; fi < ctx.func_name_count; fi++)
        ctx.func_names[fi] = reg.funcs[fi].qualified_name;

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
    ctx.func_names = NULL;
    ctx.func_name_count = 0;

    /* Walk the file. */
    ctx.step0_ready = true; /* the registry is complete: step 0 may be memoized */
    java_lsp_process_file(&ctx, root);
    java_lsp_ctx_done(&ctx);
    cbm_arena_destroy(&scratch);
}

/* ── Cross-file LSP ───────────────────────────────────────────────── */

/* The '|'-separated pieces of `text` as a NULL-terminated arena array, or
 * NULL for none. */
static const char **java_split_pipe(CBMArena *arena, const char *text) {
    if (!text || !text[0])
        return NULL;
    int n = 1;
    for (const char *p = text; *p; p++)
        if (*p == '|')
            n++;
    const char **out = (const char **)cbm_arena_alloc(arena, (size_t)(n + 1) * sizeof(*out));
    if (!out)
        return NULL;
    int idx = 0;
    const char *start = text;
    while (*start) {
        const char *end = start;
        while (*end && *end != '|')
            end++;
        if (end > start) {
            out[idx++] = cbm_arena_strndup(arena, start, (size_t)(end - start));
        }
        if (!*end)
            break;
        start = end + 1;
    }
    out[idx] = NULL;
    return out;
}

/* Register the type defs: QN, short name, the interface flag, the package that
 * marks a project type, and the supertypes as written (their identities come
 * with java_register_type_details). */
/* JAVA_PACKAGE_KEY entries: each package a project type of `defs` is in,
 * once, and the bare key that says the registry records them. */
static void java_register_packages(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                   int def_count) {
    CBMHashTable *seen = cbm_ht_create_in(CBM_MEM_CLASS_EXTRACT, 64);
    if (!seen) {
        return;
    }
    for (int i = 0; i < def_count; i++) {
        const CBMLSPDef *d = &defs[i];
        const char *dot =
            d->qualified_name && java_type_label(d->label) ? strrchr(d->qualified_name, '.') : NULL;
        if (!dot) {
            continue;
        }
        char *key = cbm_arena_sprintf(arena, JAVA_PACKAGE_KEY "%.*s",
                                      (int)(dot - d->qualified_name), d->qualified_name);
        if (!key || cbm_ht_get(seen, key)) {
            continue;
        }
        cbm_ht_set(seen, key, key);
        CBMRegisteredType mark;
        memset(&mark, 0, sizeof(mark));
        mark.qualified_name = key;
        mark.short_name = JAVA_PACKAGE_KEY;
        mark.alias_of = key; /* no type: every walk over types skips it */
        cbm_registry_add_type(reg, mark);
    }
    if (cbm_ht_count(seen) > 0) {
        CBMRegisteredType mark;
        memset(&mark, 0, sizeof(mark));
        mark.qualified_name = JAVA_PACKAGE_KEY;
        mark.short_name = JAVA_PACKAGE_KEY;
        mark.alias_of = JAVA_PACKAGE_KEY;
        cbm_registry_add_type(reg, mark);
    }
    cbm_ht_free(seen);
}

static void java_register_types(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                int def_count) {
    java_register_packages(arena, reg, defs, def_count);
    for (int i = 0; i < def_count; i++) {
        const CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !java_type_label(d->label))
            continue;
        CBMRegisteredType rt;
        memset(&rt, 0, sizeof(rt));
        rt.qualified_name = d->qualified_name;
        rt.short_name = d->short_name;
        rt.is_interface = (strcmp(d->label, "Interface") == 0) || d->is_interface;
        /* Project marker (java_is_project_type) and the package that
         * qualifies the type's own member signatures. */
        rt.namespace_qn = (d->namespace_name && d->namespace_name[0]) ? d->namespace_name
                          : (d->def_module_qn && d->def_module_qn[0]) ? d->def_module_qn
                                                                      : d->qualified_name;
        /* Embedded types from "|"-separated text. */
        rt.embedded_types = java_split_pipe(arena, d->embedded_types);
        cbm_registry_add_type(reg, rt);
        /* The member-type index (JAVA_MEMBER_KEY, JAVA_OWNER_KEY) for a nested
         * type whose owner the collector named. */
        if (d->receiver_type && d->receiver_type[0]) {
            CBMRegisteredType link;
            memset(&link, 0, sizeof(link));
            link.qualified_name = cbm_arena_sprintf(arena, JAVA_MEMBER_KEY "%s\x1f%s",
                                                    d->receiver_type, d->short_name);
            link.short_name = JAVA_MEMBER_KEY;
            link.alias_of = d->qualified_name;
            if (link.qualified_name) {
                cbm_registry_add_type(reg, link);
            }
            memset(&link, 0, sizeof(link));
            link.qualified_name = cbm_arena_sprintf(arena, JAVA_OWNER_KEY "%s", d->qualified_name);
            link.short_name = JAVA_OWNER_KEY;
            link.alias_of = d->receiver_type;
            if (link.qualified_name) {
                cbm_registry_add_type(reg, link);
            }
        }
    }
}

static void java_register_type_detail(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *d,
                                      int pass);
static void java_register_type_fields(CBMArena *arena, const CBMLSPDef *d, CBMRegisteredType *slot,
                                      JavaDeclScope *scope);

/* A type def's supertypes and fields through its identities: each supertype
 * spelling with one becomes that registered type (the member lookup follows
 * it), and each field "name:type" of field_defs is registered with its type
 * (no JDK package fallback: a field's type is either known or nothing). Runs
 * after every type is registered and the ambiguous QNs are marked; a QN two
 * defs register keeps its raw supertypes and no fields. */
static void java_register_type_details(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                       int def_count) {
    /* Every type's supertypes before any field's type: a field's simple name
     * may be a member type inherited along them. */
    for (int pass = 0; pass < 2; pass++) {
        if (pass == 1) {
            java_decl_memo_begin(); /* the supertypes are set: member walks are stable */
        }
        for (int i = 0; i < def_count; i++) {
            java_register_type_detail(arena, reg, &defs[i], pass);
        }
    }
}

static void java_register_type_detail(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *d,
                                      int pass) {
    {
        if (!d->qualified_name || !java_type_label(d->label) || !d->type_scope ||
            java_type_ambiguous(reg, d->qualified_name)) {
            return;
        }
        CBMRegisteredType *slot = cbm_registry_first_own_type(reg, d->qualified_name);
        if (!slot)
            return;
        JavaDeclScope scope = {.ix = cbm_type_scope_index(arena, d->type_scope),
                               .reg = reg,
                               .library_packages = false};
        if (pass == 1) {
            java_register_type_fields(arena, d, slot, &scope);
            return;
        }
        int emb_count = 0;
        while (slot->embedded_types && slot->embedded_types[emb_count])
            emb_count++;
        if (emb_count > 0) {
            const char **emb =
                (const char **)cbm_arena_alloc(arena, (size_t)(emb_count + 1) * sizeof(*emb));
            for (int j = 0; emb && j < emb_count; j++) {
                const CBMType *t = java_decl_type(arena, slot->embedded_types[j], &scope);
                const char *qn = t->kind == CBM_TYPE_NAMED ? t->data.named.qualified_name
                                 : t->kind == CBM_TYPE_TEMPLATE
                                     ? t->data.template_type.template_name
                                     : NULL;
                /* Only an identity replaces the spelling as written. */
                const char *no_gen = strip_generics(arena, slot->embedded_types[j]);
                bool proven = qn && no_gen && cbm_type_scope_find(scope.ix, no_gen, strlen(no_gen));
                emb[j] = proven ? qn : slot->embedded_types[j];
            }
            if (emb) {
                emb[emb_count] = NULL;
                slot->embedded_types = emb;
            }
        }
    }
}

/* A type def's fields "name:type" (field_defs), each registered with its
 * type, read in the type's own body (scope->owner). */
static void java_register_type_fields(CBMArena *arena, const CBMLSPDef *d, CBMRegisteredType *slot,
                                      JavaDeclScope *scope) {
    const char **fields = java_split_pipe(arena, d->field_defs);
    int field_count = 0;
    while (fields && fields[field_count])
        field_count++;
    if (field_count == 0)
        return;
    const char **names =
        (const char **)cbm_arena_alloc(arena, (size_t)(field_count + 1) * sizeof(*names));
    const CBMType **types =
        (const CBMType **)cbm_arena_alloc(arena, (size_t)(field_count + 1) * sizeof(*types));
    if (!names || !types)
        return;
    scope->owner = d->qualified_name;
    int kept = 0;
    for (int j = 0; j < field_count; j++) {
        const char *colon = strchr(fields[j], ':');
        if (!colon || colon == fields[j])
            continue;
        names[kept] = cbm_arena_strndup(arena, fields[j], (size_t)(colon - fields[j]));
        types[kept] = java_decl_type(arena, colon + 1, scope);
        kept++;
    }
    names[kept] = NULL;
    types[kept] = NULL;
    slot->field_names = names;
    slot->field_types = types;
}

/* Register the method defs: parameter and return types through the def's
 * identities (parameters keep the JDK package fallback they had). */
static void java_register_funcs(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                int def_count) {
    for (int i = 0; i < def_count; i++) {
        const CBMLSPDef *d = &defs[i];
        if (!d->qualified_name || !d->short_name || !d->label)
            continue;
        if (strcmp(d->label, "Method") == 0 || strcmp(d->label, "Function") == 0 ||
            strcmp(d->label, "Constructor") == 0) {
            CBMRegisteredFunc rf;
            memset(&rf, 0, sizeof(rf));
            rf.qualified_name = d->qualified_name;
            rf.short_name = d->short_name;
            rf.min_params = -1;
            rf.receiver_type = d->receiver_type;
            const cbm_type_scope_index_t *ix = cbm_type_scope_index(arena, d->type_scope);
            JavaDeclScope params = {
                .ix = ix, .reg = reg, .library_packages = true, .owner = d->receiver_type};
            JavaDeclScope result = {
                .ix = ix, .reg = reg, .library_packages = false, .owner = d->receiver_type};
            const CBMType **ptypes = cbm_type_materialize_signature_params(
                arena, d->signature_param_types, d->signature_param_count, java_decl_parse_type,
                &params);
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
                ret = java_decl_type(arena, first, &result);
            }
            const CBMType **rtypes = NULL;
            if (ret) {
                rtypes = (const CBMType **)cbm_arena_alloc(arena, 2 * sizeof(*rtypes));
                rtypes[0] = ret;
                rtypes[1] = NULL;
            }
            rf.signature = cbm_type_func(arena, NULL, ptypes, rtypes);
            cbm_registry_add_func(reg, rf);
        }
    }
}

void cbm_java_register_lsp_defs(CBMArena *arena, CBMTypeRegistry *reg, const CBMLSPDef *defs,
                                int def_count) {
    java_register_types(arena, reg, defs, def_count);
    cbm_java_mark_ambiguous_types(arena, reg, defs, def_count);
    java_register_type_details(arena, reg, defs, def_count);
    java_register_funcs(arena, reg, defs, def_count);
    java_decl_memo_end();
}

/* ── Tier 2: shared cross-registry + per-file overlay (#1669) ──────────
 *
 * Without this, every Java file rebuilt a whole registry from its filtered def
 * set. That set is reduced by a constant FACTOR, not to a constant SIZE, so it
 * grew with the corpus (measured 1,292 -> 5,031 defs/file as the tree went
 * 179k -> 689k defs) and cross-file LSP became O(files x corpus_defs): 3.7
 * ms/file on v0.9.0 versus 208.9 ms/file on v0.10.5.
 *
 * Build the JVM def universe ONCE, seal it read-only, and share it across
 * workers. Kotlin defs are included deliberately: a mixed source root resolves
 * Java->Kotlin calls, and dropping them here would silently lose those edges. */
/* Declared in src/pipeline/pass_lsp_cross.h; extern'd here to keep the
 * internal/cbm layer free of src/pipeline includes. */
extern void cbm_pxc_count_perfile_defs(uint64_t defs);

CBMTypeRegistry *cbm_java_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count) {
    if (!arena) {
        return NULL;
    }
    CBMTypeRegistry *reg = (CBMTypeRegistry *)cbm_arena_alloc(arena, sizeof(*reg));
    if (!reg) {
        return NULL;
    }
    cbm_registry_init(reg, arena);
    cbm_java_stdlib_register(reg, arena);
    /* Two-phase registration, same idiom as the per-file Pass 1a/1b split.
     *
     * Func registration parses signatures, and parse_param_text_full qualifies
     * bare type names via cbm_registry_lookup_type. Before finalize that lookup
     * is a LINEAR scan over every type registered so far, so one mixed pass is
     * O(defs x types) — measured 0.44 ms/def at 689k defs, i.e. a ~300 s
     * sequential build that erased the win of sharing the registry at all.
     *
     * Register all TYPES first (their registration does no lookups), finalize
     * once so the type-QN buckets exist, then register FUNCS with O(1) type
     * lookups, and finalize again to index the funcs. parse_param_text_full
     * takes the registry const — no stub types appear post-finalize, so the
     * post-finalize tail stays empty. Bonus over the old one-pass order: a
     * signature can now resolve a type that appears LATER in defs[], the same
     * source-order independence the per-file path already guarantees. */
    /* def_count == 0 is a valid corpus (no Java files): arena_alloc(0) returns
     * NULL, which must not be mistaken for OOM — the empty registry is still
     * built, finalized, and shared. */
    CBMLSPDef *jvm = NULL;
    if (def_count > 0) {
        jvm = (CBMLSPDef *)cbm_arena_alloc(arena, (size_t)def_count * sizeof(*jvm));
        if (!jvm) {
            return NULL;
        }
    }
    /* Stable two-pass partition: types in defs[] order, then funcs in defs[]
     * order. Func registration order is load-bearing — overload ties resolve
     * to the FIRST registered QN match — so no swap-based partition. */
    int total = 0;
    for (int i = 0; i < def_count; i++) {
        const char *lb = defs[i].label;
        if ((defs[i].lang == CBM_LANG_JAVA || defs[i].lang == CBM_LANG_KOTLIN) && lb &&
            (strcmp(lb, "Class") == 0 || strcmp(lb, "Interface") == 0 || strcmp(lb, "Enum") == 0 ||
             strcmp(lb, "Type") == 0)) {
            jvm[total++] = defs[i];
        }
    }
    int type_count = total;
    for (int i = 0; i < def_count; i++) {
        const char *lb = defs[i].label;
        if ((defs[i].lang == CBM_LANG_JAVA || defs[i].lang == CBM_LANG_KOTLIN) &&
            !(lb && (strcmp(lb, "Class") == 0 || strcmp(lb, "Interface") == 0 ||
                     strcmp(lb, "Enum") == 0 || strcmp(lb, "Type") == 0))) {
            jvm[total++] = defs[i];
        }
    }
    /* Types, the marks of the QNs two of them share, then (indexed) their
     * supertypes and fields through their identities, then the methods. */
    java_register_types(arena, reg, jvm, type_count);
    cbm_java_mark_ambiguous_types(arena, reg, jvm, type_count);
    cbm_registry_finalize(reg);
    java_register_type_details(arena, reg, jvm, type_count);
    if (total > type_count) { /* jvm is NULL for an empty corpus: no offset */
        java_register_funcs(arena, reg, jvm + type_count, total - type_count);
    }
    java_decl_memo_end();
    cbm_registry_finalize(reg);
    /* java_receiver_is_external asks "does the project declare a type in
     * this package?" per unresolved receiver: a binary search on this order. */
    cbm_registry_build_type_qn_order(reg, arena);
    reg->read_only = true; /* seal: shared Tier-2 registry is read-only during resolve */
    return reg;
}

/* The cross walk's own types: a supertype the file's own types do not declare
 * was left as written by register_local_func_or_type_from_file (it runs before
 * the imports are known). Once they are, each such spelling is resolved in
 * the file's scope (java_resolve_type_name: its verified imports, its
 * package, its on-demand imports) and replaced when the registry holds the
 * answer. */
static void java_own_supertypes(JavaLSPContext *ctx, CBMTypeRegistry *overlay,
                                const CBMFileResult *result) {
    for (int i = 0; i < result->defs.count; i++) {
        const CBMDefinition *d = &result->defs.items[i];
        if (!d->qualified_name || !d->base_classes || !java_type_label(d->label))
            continue;
        CBMRegisteredType *slot = cbm_registry_first_own_type(overlay, d->qualified_name);
        for (int j = 0;
             slot && slot->embedded_types && slot->embedded_types[j] && d->base_classes[j]; j++) {
            CBM_LSP_WORK(1);
            const char *spelled = strip_generics(ctx->arena, d->base_classes[j]);
            if (!spelled || !spelled[0] || strchr(spelled, '.') ||
                cbm_registry_lookup_type(ctx->registry, slot->embedded_types[j])) {
                continue;
            }
            const char *qn = java_resolve_type_name(ctx, spelled);
            if (qn && cbm_registry_lookup_type(ctx->registry, qn) &&
                !java_type_ambiguous(ctx->registry, qn)) {
                slot->embedded_types[j] = qn;
            } else {
                /* No registered type: what the file's scope names outside the
                 * project (an import's target, a java.lang name) or the
                 * spelling as the shared registrar keeps it, never main's
                 * same-package guess `module_qn.Name`, which names no type. */
                bool outside =
                    qn && strchr(qn, '.') && !cbm_registry_lookup_type(ctx->registry, qn);
                slot->embedded_types[j] = outside ? qn : spelled;
            }
        }
    }
}

/* Per-file resolve against the shared base. The overlay holds exactly THIS
 * FILE's own definitions (from the extract result), because patch_one_method()
 * refines signatures from the AST and those writes must land on a private
 * copy: its types live in the per-file arena, and the shared base is sealed
 * read-only. Everything else — same-package siblings included — resolves
 * through overlay.fallback into the shared registry.
 *
 * Scope matters for complexity, not just safety: a MODULE- or NAMESPACE-scoped
 * overlay pays package-size per file, which is quadratic on repos that
 * concentrate files in large packages (the #1669 growth pattern; pinned by
 * test_complexity.c's shared-package gate). Own-file scope is O(file). */
void cbm_run_java_lsp_cross_with_registry(CBMArena *arena, CBMFileResult *result,
                                          const char *source, int source_len, const char *module_qn,
                                          CBMTypeRegistry *reg, const char **import_names,
                                          const char **import_qns, int import_count,
                                          TSTree *cached_tree, CBMResolvedCallArray *out) {
    if (!arena || !result || !source || !reg || !out) {
        return;
    }

    CBMTypeRegistry overlay;
    cbm_registry_init(&overlay, arena);
    overlay.fallback = reg;

    JavaLSPContext ctx;
    java_lsp_init(&ctx, arena, source, source_len, &overlay, NULL, module_qn, out);

    /* Index the overlay only — the base is already finalized. Scratch arena so
     * per-file bucket allocations do not accumulate in the pipeline-lifetime
     * arena across a large repo. Registration already finds its own types there. */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    overlay.tail_arena = &idx_arena;
    register_local_func_or_type_from_file(&ctx, &overlay, result);
    cbm_pxc_count_perfile_defs((uint64_t)result->defs.count);
    cbm_registry_finalize_into(&overlay, &idx_arena);

    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        if (!parser) {
            cbm_arena_destroy(&idx_arena);
            return;
        }
        ts_parser_set_language(parser, tree_sitter_java());
        tree = cbm_parse_source(parser, source, (uint32_t)source_len, (TSParseOptions){0});
        ts_parser_delete(parser);
        owns_tree = true;
    }
    if (!tree) {
        cbm_arena_destroy(&idx_arena);
        return;
    }
    TSNode root = ts_tree_root_node(tree);

    for (int i = 0; i < import_count; i++) {
        if (!import_names[i] || !import_qns[i]) {
            continue;
        }
        java_lsp_add_import(&ctx, import_names[i], import_qns[i], CBM_JAVA_IMPORT_IDENTITY);
    }
    /* The file's own scope is complete once its import declarations are read
     * (the AST scan of java_lsp_process_file records them and the package):
     * read them first, then resolve the own types' supertypes in it. */
    java_lsp_scan_imports(&ctx, root);
    java_own_supertypes(&ctx, &overlay, result);

    ctx.cross_run = true;
    ctx.step0_ready = true; /* the registry is complete: step 0 may be memoized */
    java_lsp_process_file(&ctx, root);
    java_lsp_ctx_done(&ctx);
    cbm_arena_destroy(&idx_arena);

    if (owns_tree) {
        ts_tree_delete(tree);
    }
}

/* The per-file fallback when no shared cross registry was built. Its
 * registry has no fallback layer: java_receiver_is_external judges a
 * receiver's package by the run's project packages when they are handed in,
 * and not at all without them (library calls then keep the textual registry
 * fallback, as before). */
void cbm_run_java_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out) {
    cbm_run_java_lsp_cross_packages(arena, source, source_len, module_qn, defs, def_count,
                                    import_names, import_qns, import_count, cached_tree, NULL, NULL,
                                    out);
}

void cbm_run_java_lsp_cross_packages(CBMArena *arena, const char *source, int source_len,
                                     const char *module_qn, CBMLSPDef *defs, int def_count,
                                     const char **import_names, const char **import_qns,
                                     int import_count, TSTree *cached_tree,
                                     const CBMJavaProjectPackages *packages, CBMFileResult *result,
                                     CBMResolvedCallArray *out) {
    if (!arena || !source)
        return;

    CBMTypeRegistry reg;
    cbm_registry_init(&reg, arena);
    cbm_java_stdlib_register(&reg, arena);

    /* Register cross-file defs as the shared build does: types, the marks of
     * shared QNs, then (indexed) supertypes and fields, then the methods.
     * Without the hash indexes every lookup_type/func/method in the walk
     * below is a LINEAR scan over the whole cross registry -- O(lookups x
     * defs) per file. Index allocations go to a per-call scratch arena: reg's
     * arena is the pipeline-lifetime result arena, and per-file bucket
     * allocations there accumulate GBs across a large repo. */
    CBMArena idx_arena;
    cbm_arena_init(&idx_arena);
    java_register_types(arena, &reg, defs, def_count);
    cbm_java_mark_ambiguous_types(arena, &reg, defs, def_count);
    cbm_registry_finalize_into(&reg, &idx_arena);
    g_java_reg_packages = packages;
    java_register_type_details(arena, &reg, defs, def_count);
    java_register_funcs(arena, &reg, defs, def_count);
    java_decl_memo_end();
    g_java_reg_packages = NULL;
    cbm_registry_finalize_into(&reg, &idx_arena);

    /* The calling file's own types and their supertypes in an overlay, as the
     * shared-registry run registers them (its enclosing classes carry the
     * graph QNs the walk starts from). */
    CBMTypeRegistry overlay;
    cbm_registry_init(&overlay, arena);
    overlay.fallback = &reg;

    /* Parse if needed. */
    TSTree *tree = cached_tree;
    bool owns_tree = false;
    if (!tree) {
        TSParser *parser = ts_parser_new();
        ts_parser_set_language(parser, tree_sitter_java());
        tree = cbm_parse_source(parser, source, (uint32_t)source_len, (TSParseOptions){0});
        ts_parser_delete(parser);
        owns_tree = true;
    }
    if (!tree) {
        cbm_arena_destroy(&idx_arena);
        return;
    }
    TSNode root = ts_tree_root_node(tree);

    JavaLSPContext ctx;
    java_lsp_init(&ctx, arena, source, source_len, result ? &overlay : &reg, NULL, module_qn, out);
    ctx.project_packages = packages;
    if (result) {
        overlay.tail_arena = &idx_arena;
        register_local_func_or_type_from_file(&ctx, &overlay, result);
        cbm_registry_finalize_into(&overlay, &idx_arena);
    }

    /* The calling file's verified import identities. */
    for (int i = 0; i < import_count; i++) {
        if (!import_names[i] || !import_qns[i])
            continue;
        java_lsp_add_import(&ctx, import_names[i], import_qns[i], CBM_JAVA_IMPORT_IDENTITY);
    }
    if (result) {
        java_lsp_scan_imports(&ctx, root);
        java_own_supertypes(&ctx, &overlay, result);
    }

    ctx.cross_run = true;
    ctx.step0_ready = true; /* the registry is complete: step 0 may be memoized */
    java_lsp_process_file(&ctx, root);
    java_lsp_ctx_done(&ctx);
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
