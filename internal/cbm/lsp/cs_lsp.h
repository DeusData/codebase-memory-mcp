#ifndef CBM_LSP_CS_LSP_H
#define CBM_LSP_CS_LSP_H

#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"
#include "go_lsp.h" /* CBMLSPDef, CBMResolvedCall, CBMResolvedCallArray are reused */

/*
 * cs_lsp — C# Light Semantic Pass.
 *
 * Reverse-engineered from Roslyn's Binder pipeline (src/Compilers/CSharp/
 * Portable/Binder/Binder_*.cs). Mirrors the structure of go_lsp / c_lsp /
 * php_lsp / py_lsp so the shared pipeline (lsp_resolve.h) treats every
 * language identically.
 *
 * Coverage targets (≥90% parity vs Roslyn for typical user code):
 *   - using / using static / using alias / global using
 *   - file-scoped + block namespaces; nested namespaces
 *   - classes, structs, records, interfaces, enums
 *   - inheritance + interface implementation; partial classes
 *   - methods (instance, static, generic, async, extension `this`)
 *   - properties (auto, expression-bodied, full); indexers
 *   - constructors + primary constructors (records / C# 12 classes)
 *   - object creation `new T(...)` / target-typed `new(...)`
 *   - var + explicit local types; foreach element inference
 *   - tuples (literal + parameter)
 *   - lambdas + delegate calls
 *   - await: Task<T> → T, ValueTask<T> → T
 *   - this / base / `base.Method()` calls
 *   - cast `(T)x`, `x as T`, pattern `x is T y`
 *   - null-conditional `?.`, null-coalescing `??`
 *   - generic instantiation + type-parameter substitution
 *   - extension method dispatch (`obj.Foo()` -> static Foo(this T self, ...))
 *
 * Out-of-scope (intentional):
 *   - flow analysis-driven nullable narrowing
 *   - LINQ query syntax (handled as method-syntax via `Select`/`Where` lookup)
 *   - dynamic / reflection
 *   - source generators / Roslyn analyzers
 */

/* CSAlias / CSUsing — per-file using state.
 *
 * `using Foo.Bar;`            -> namespace import (kind = NAMESPACE)
 * `using static Foo.Bar;`     -> static-member import (kind = STATIC)
 * `using F = Foo.Bar;`        -> alias (kind = ALIAS, local_name = "F")
 * `global using ...;`         -> kept identical, marked is_global (rare in
 *                                a single file but handled for ASP.NET-style
 *                                Program.cs files).
 */
typedef enum {
    CBM_CS_USING_NAMESPACE = 0,
    CBM_CS_USING_STATIC,
    CBM_CS_USING_ALIAS,
} CBMCSUsingKind;

typedef struct {
    CBMCSUsingKind kind;
    const char *local_name; /* alias name; "" for non-alias */
    const char *target_qn;  /* dotted target QN */
    bool is_global;         /* `global using` */
} CBMCSUsing;

/* CSLSPContext — per-file type-evaluation state. */
typedef struct {
    CBMArena *arena;
    const char *source;
    int source_len;
    const CBMTypeRegistry *registry;
    CBMScope *current_scope;

    /* Namespace stack (innermost first). C# allows nested + file-scoped. */
    const char **namespace_stack;
    int namespace_count;
    int namespace_cap;

    /* Active using directives in the current file. C# resolves bare names
     * by walking the namespace stack outward, then searching using
     * directives. */
    CBMCSUsing *usings;
    int using_count;
    int using_cap;

    /* Enclosing class / struct / record / interface — the "type" body
     * we're currently inside. NULL outside type body. */
    const char *enclosing_class_qn;
    const char *enclosing_base_qn;       /* base class QN; NULL if none */
    const char **enclosing_iface_qns;    /* NULL-terminated; NULL if none */

    /* Enclosing function/method/lambda. */
    const char *enclosing_func_qn;

    /* Module QN for this file (matches what the unified extractor records). */
    const char *module_qn;

    /* Output: resolved calls accumulate here. */
    CBMResolvedCallArray *resolved_calls;

    /* Active type-parameter substitution map (for generic methods/types).
     * Parallel arrays. NULL-terminated. */
    const char **type_param_names;
    const CBMType **type_param_args;
    int type_param_count;

    /* Recursion guard for cs_eval_expr_type. */
    int eval_depth;

    /* Debug mode (CBM_LSP_DEBUG env). */
    bool debug;
} CSLSPContext;

/* Initialize a CSLSPContext for processing one file. */
void cs_lsp_init(CSLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                 const CBMTypeRegistry *registry, const char *module_qn,
                 CBMResolvedCallArray *out);

/* Append a using directive. local_name may be NULL/empty for non-alias kinds. */
void cs_lsp_add_using(CSLSPContext *ctx, CBMCSUsingKind kind, const char *local_name,
                      const char *target_qn, bool is_global);

/* Process a file's AST. */
void cs_lsp_process_file(CSLSPContext *ctx, TSNode root);

/* Evaluate the type of an expression. Never returns NULL — falls back to
 * cbm_type_unknown(). */
const CBMType *cs_eval_expr_type(CSLSPContext *ctx, TSNode node);

/* Convert a C# type-AST node to a CBMType. */
const CBMType *cs_parse_type_node(CSLSPContext *ctx, TSNode node);

/* Resolve a bare or dotted type name against namespace stack + using map.
 * Returns dotted QN or NULL. */
const char *cs_resolve_type_name(CSLSPContext *ctx, const char *name);

/* Look up a method on a type, walking base + interface chains. */
const CBMRegisteredFunc *cs_lookup_method(CSLSPContext *ctx, const char *type_qn,
                                           const char *method_name);

/* Single-file entry: build registry from file defs + stdlib, run resolution. */
void cbm_run_cs_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                    TSNode root);

/* Cross-file entry. Like Go/C: caller supplies pre-resolved defs from siblings. */
void cbm_run_cs_lsp_cross(CBMArena *arena, const char *source, int source_len,
                           const char *module_qn, CBMLSPDef *defs, int def_count,
                           const char **using_targets, int using_count,
                           TSTree *cached_tree, CBMResolvedCallArray *out);

/* Tier 2: build a project-wide C# registry ONCE from all defs (filters
 * by lang), shared READ-ONLY across resolve workers. Def-driven. */
CBMTypeRegistry *cbm_cs_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count);

/* Cross-file resolve using a pre-built shared registry (Tier 2). */
void cbm_run_cs_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                        const char *module_qn, CBMTypeRegistry *reg,
                                        const char **using_targets, int using_count,
                                        TSTree *cached_tree, CBMResolvedCallArray *out);

/* Batch cross-file entry for one CGo call from the parallel pipeline. */
typedef struct {
    const char *source;
    int source_len;
    const char *module_qn;
    TSTree *cached_tree;
    CBMLSPDef *defs;
    int def_count;
    const char **using_targets;
    int using_count;
} CBMBatchCSLSPFile;

void cbm_batch_cs_lsp_cross(CBMArena *arena, CBMBatchCSLSPFile *files, int file_count,
                             CBMResolvedCallArray *out);

/* Register .NET BCL stdlib types and functions. Generated. */
void cbm_csharp_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena);

#endif /* CBM_LSP_CS_LSP_H */
