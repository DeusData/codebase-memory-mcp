/*
 * kotlin_lsp.h — Kotlin Light Semantic Pass.
 *
 * In-process type-aware call resolver for Kotlin. Mirrors go_lsp / php_lsp
 * shape: a per-file pass that walks the tree-sitter Kotlin AST, tracks
 * scope, infers expression types, and emits CBMResolvedCall entries for
 * call_expression / navigation_expression nodes whose target FQN can be
 * determined statically.
 *
 * Reverse-engineered from the fwcd kotlin-language-server reference
 * implementation (Kotlin compiler frontend) plus the official Kotlin
 * language specification — distilled into a pure-C resolver with no JVM
 * dependency. The goal is ≥ 90% quality parity with the LSP for the
 * call-edge and method-dispatch attribution problems CBM cares about.
 *
 * The Kotlin features handled:
 *   - Package declaration (`package a.b.c`)
 *   - Default Kotlin imports (kotlin.*, kotlin.collections.*, etc.)
 *   - Explicit imports with optional `as` aliases
 *   - Wildcard imports (`import a.b.*`)
 *   - Top-level functions and properties
 *   - Class / interface / object / data class / enum / sealed class
 *   - Companion objects (named and anonymous) with `Foo.bar()` static-style dispatch
 *   - Primary and secondary constructors (incl. `class Foo(val x: Int)`)
 *   - Inheritance: delegation_specifier list, super-method lookup
 *   - Extension functions and properties (`fun String.uppercaseFirst()`)
 *   - Infix and operator functions (`a foo b`, `a + b`)
 *   - Smart casts after `is` / `as` / null-checks
 *   - Generics (basic substitution; we track but don't fully unify)
 *   - Nullable types (T?), safe calls (?.) and not-null assertions (!!)
 *   - `lateinit var` and `by` delegation (best-effort)
 *   - Lambdas with implicit `it` parameter
 *   - `with`, `let`, `also`, `apply`, `run`, `takeIf` scope functions
 *   - `when` expressions with subject smart-cast
 *   - `object` declarations as singletons
 *
 * Out of scope (low value or impractical without the Kotlin compiler):
 *   - Reified generics across call boundaries
 *   - Full type unification with constraints
 *   - Decompilation of compiled Kotlin/Java bytecode
 *   - DSL-style type-safe builders beyond direct lambda receivers
 *   - Inline class boxing/unboxing
 *
 * The unresolved fallthrough goes to the registry's name-based matcher,
 * just like Go/Python/PHP. We never produce *worse* attribution than the
 * pre-LSP baseline; if the LSP can't decide, it emits nothing.
 */
#ifndef CBM_LSP_KOTLIN_LSP_H
#define CBM_LSP_KOTLIN_LSP_H

#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"
#include "go_lsp.h" /* CBMLSPDef, CBMResolvedCallArray reused across languages */

/* Use-kind for `import a.b.c.foo` — tracks whether the import refers to a
 * type, a function/extension, a property, or an object. Determines how
 * `foo` is resolved when used as a bare identifier. */
typedef enum {
    CBM_KT_USE_UNKNOWN = 0,
    CBM_KT_USE_TYPE,     /* class / interface / object / typealias */
    CBM_KT_USE_FUNCTION, /* top-level fun, extension fun */
    CBM_KT_USE_PROPERTY, /* top-level val/var */
    CBM_KT_USE_WILDCARD, /* import a.b.* — local_name is a.b prefix */
} CBMKotlinUseKind;

/* KotlinLSPContext — per-file state for Kotlin call resolution. */
typedef struct KotlinLSPContext {
    CBMArena *arena;
    const char *source;
    int source_len;
    const CBMTypeRegistry *registry;
    CBMScope *current_scope;

    /* Package context. Empty string for default package. */
    const char *package_qn;    /* dotted form, e.g. "com.example.foo" */
    const char *module_qn;     /* file-level QN, e.g. "<project>.com.example.foo.<File>" */
    const char *project_name;  /* project prefix (without trailing dot) */
    const char *file_class_qn; /* JVM file-class QN, "<package>.<File>Kt" */
    const char *rel_path;      /* for diagnostics */

    /* Import map (parallel arrays). Kotlin imports are flat — no grouping
     * — but each may have an `as` alias. Wildcard imports record the
     * package prefix in `import_targets[i]` with `import_kinds[i] = WILDCARD`
     * and `import_locals[i] = NULL`. */
    const char **import_locals;  /* alias name (or short name) used in code */
    const char **import_targets; /* full FQN being imported */
    CBMKotlinUseKind *import_kinds;
    int import_count;
    int import_cap;

    /* Current declaration context. */
    const char *enclosing_func_qn;      /* function we are resolving inside */
    const char *enclosing_class_qn;     /* innermost class/interface/object QN, or NULL */
    const char *enclosing_companion_qn; /* if inside a companion object body */
    const char *enclosing_super_qn;     /* primary super-class QN of current class, or NULL */

    /* Current `this` and `super` types. */
    const CBMType *this_type;
    const CBMType *super_type;

    /* `it` lambda parameter type, when inside a single-arg lambda.
     * Saved/restored across nested lambdas. */
    const CBMType *it_type;

    /* Output: resolved calls accumulate here. */
    CBMResolvedCallArray *resolved_calls;

    /* Recursion guard for kotlin_eval_expr_type. */
    int eval_depth;

    /* AST-walk recursion depth for kt_resolve_calls_in_node (guards stack
     * overflow on deeply-nested/cyclic files; see cbm_lsp_max_walk_depth).
     * Zero via memset. */
    int walk_depth;

    /* Debug mode (CBM_LSP_DEBUG env). */
    bool debug;
} KotlinLSPContext;

/* Initialize a KotlinLSPContext for processing one file. */
void kotlin_lsp_init(KotlinLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                     const CBMTypeRegistry *registry, const char *package_qn, const char *module_qn,
                     const char *project_name, const char *rel_path, CBMResolvedCallArray *out);

/* Add an import mapping. local_name is the name used in code (alias or
 * short name); target_qn is the full dotted FQN. For wildcard imports,
 * pass the package prefix as target_qn and CBM_KT_USE_WILDCARD as kind. */
void kotlin_lsp_add_import(KotlinLSPContext *ctx, const char *local_name, const char *target_qn,
                           CBMKotlinUseKind kind);

/* Walk a file's AST: top-level decls, then function/method bodies. */
void kotlin_lsp_process_file(KotlinLSPContext *ctx, TSNode root);

/* Evaluate a Kotlin expression's type. Returns cbm_type_unknown() on
 * failure — never NULL. */
const CBMType *kotlin_eval_expr_type(KotlinLSPContext *ctx, TSNode node);

/* Parse a Kotlin type-AST node (user_type, nullable_type, function_type, …)
 * to CBMType. */
const CBMType *kotlin_parse_type_node(KotlinLSPContext *ctx, TSNode node);

/* Resolve a bare class name (possibly qualified like "Foo" or "a.Foo")
 * to its full QN using current package + import map. NULL if unresolved. */
const char *kotlin_resolve_class_name(KotlinLSPContext *ctx, const char *name);

/* Resolve a bare top-level function name to its target QN. */
const char *kotlin_resolve_function_name(KotlinLSPContext *ctx, const char *name);

/* Look up a method on a class, walking the super-chain (registry-based). */
const CBMRegisteredFunc *kotlin_lookup_method(KotlinLSPContext *ctx, const char *class_qn,
                                              const char *method_name);

/* Look up a property/field on a class, walking super-chain. */
const CBMType *kotlin_lookup_property_type(KotlinLSPContext *ctx, const char *class_qn,
                                           const char *prop_name);

/* Entry point: build registry from file defs + stdlib, then run resolution.
 * Called from cbm_extract_file() after definitions and imports have been
 * extracted. */
void cbm_run_kotlin_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                        TSNode root);

/* Cross-file LSP: build a registry from project-wide defs (local + cross-file)
 * + stdlib, re-parse the source if no cached tree, walk and resolve calls.
 * Mirrors cbm_run_java_lsp_cross. `defs` carries the graph QNs of every
 * project definition so a bare top-level call in file B resolves to the
 * definition node living in file A. Output is appended to `out`. */
void cbm_run_kotlin_lsp_cross(CBMArena *arena, const char *source, int source_len,
                              const char *module_qn, CBMLSPDef *defs, int def_count,
                              const char **import_names, const char **import_qns, int import_count,
                              TSTree *cached_tree, CBMResolvedCallArray *out);

/* Register the curated Kotlin stdlib (kotlin.*, kotlin.collections.*, …)
 * into a registry. Implemented in lsp/generated/kotlin_stdlib_data.c. */
void cbm_kotlin_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena);

/* Register Kotlin default-import targets — the prefixes auto-imported
 * by every Kotlin file. Used by the LSP context init. */
const char *const *cbm_kotlin_default_import_packages(int *count_out);

#endif /* CBM_LSP_KOTLIN_LSP_H */
