/* rust_lsp.h — Type-aware call resolution for Rust source files.
 *
 * Architecture mirrors go_lsp / py_lsp: build a `CBMTypeRegistry` from the
 * file's own definitions plus a small hand-rolled stdlib seed, then walk
 * each function body with scope-tracked variable bindings, evaluating the
 * type of each receiver expression and dispatching method calls through
 * inherent impls, trait impls, deref/autoref, generics, and `Self`.
 *
 * The implementation is a structural mirror of the algorithm in
 * `rust-analyzer` (`hir-def/resolver.rs` + `hir-ty/method_resolution.rs`)
 * reverse-engineered into pure C with no new runtime dependencies. The
 * goal is per-file (and cross-file) call attribution at >=90% parity with
 * what rust-analyzer would produce for the same source — without the cost
 * of a full IDE process.
 *
 * Public entry points:
 *   - `cbm_run_rust_lsp` — single-file resolution invoked from
 *     `cbm_extract_file()`.
 *   - `cbm_run_rust_lsp_cross` — cross-file resolution given a list of
 *     `CBMLSPDef`s gathered by the pipeline.
 *   - `cbm_batch_rust_lsp_cross` — batch wrapper that processes several
 *     files in one CGo call (per-file arenas + result copy).
 */
#ifndef CBM_LSP_RUST_LSP_H
#define CBM_LSP_RUST_LSP_H

#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"

/* Forward declaration — defined in rust_cargo.h. We keep it forward
 * here to avoid pulling rust_cargo.h into every consumer that only
 * needs the LSP API. */
struct CBMCargoManifest;

/* Global confidence assigned to LSP-resolved call edges. The pipeline's
 * shared override resolver (`pipeline/lsp_resolve.h`) only admits entries
 * scoring >= CBM_LSP_CONFIDENCE_FLOOR (0.6). Numbers here mirror the Go
 * LSP so the call-edge mix from a polyglot project stays comparable. */
#define CBM_RUST_CONF_DIRECT 0.95f      /* path::to::func or alias hit  */
#define CBM_RUST_CONF_METHOD 0.95f      /* inherent method dispatch     */
#define CBM_RUST_CONF_TRAIT_SOLE 0.92f  /* trait method, single impl    */
#define CBM_RUST_CONF_TRAIT_AMB 0.85f   /* trait method, many impls     */
#define CBM_RUST_CONF_UFCS 0.93f        /* T::method() / Self::new()    */
#define CBM_RUST_CONF_PROMOTED 0.90f    /* through Deref / blanket impl */
#define CBM_RUST_CONF_MACRO_KNOWN 0.85f /* known std macro mapped to fn */
#define CBM_RUST_CONF_OPERATOR 0.88f    /* a+b → T::add (operator trait) */

/* Rust-flavoured LSP context: one per file, lifetime tied to a single
 * `cbm_extract_file()` invocation (or the cross-file caller's arena). */
typedef struct {
    CBMArena *arena;
    const char *source;
    int source_len;

    const CBMTypeRegistry *registry;
    CBMScope *current_scope;

    /* `use` map: parallel arrays mapping a local-name (the last segment, or
     * the `as` alias) to its full module path (`std::collections::HashMap`,
     * `crate::foo::Bar`). Glob imports go in `glob_module_qns` instead. */
    const char **use_local_names;
    const char **use_module_paths;
    int use_count;

    const char **glob_module_qns;
    int glob_count;

    /* Module-qualified name for this file (e.g. "<project>.<crate>.foo"). */
    const char *module_qn;

    /* Enclosing function context. `enclosing_func_qn` is the QN we attach
     * to every emitted CBMResolvedCall as `caller_qn`. */
    const char *enclosing_func_qn;

    /* `Self` resolution: when inside `impl T { ... }` or
     * `impl Trait for T { ... }`, `self_type_qn` is `T`'s QN. NULL outside
     * an impl. `self_trait_qn` is the trait QN (only set for trait impls)
     * — used when emitting trait method calls so we can prefer concrete
     * implementations over the trait method when only one impl exists. */
    const char *self_type_qn;
    const char *self_trait_qn;

    /* Closure parameter inference: when the call resolver descends into a
     * call's argument list and the callee is a method like
     * `.map(|x| ...)` / `.filter(|x| ...)`, it stashes the inferred type
     * of the closure's first param here. The closure_expression handler
     * consumes it (and clears it) when binding params, so a chain like
     * `vec.iter().map(|x| x.method())` resolves `x.method()` correctly. */
    const CBMType *pending_closure_param_type;

    /* User-defined `macro_rules!` definitions collected during the
     * pre-walk phase. Each rule stores its pattern + transcriber text;
     * `macro_invocation` resolution attempts to match the invocation
     * against each rule and then substitutes/re-walks the expansion.
     *
     * The arrays are doubling-grown out of `arena`. */
    struct RustMacroRule **macro_rules_arr;
    int macro_rules_count;

    /* Recursion guard for macro expansion. Real macro_rules! can be
     * recursive; we cap at 8 nested expansions to keep the walker
     * bounded. */
    int macro_expand_depth;

    /* Pathological-input guard: counts the number of call-resolution
     * + type-evaluation steps spent on the current file. When the
     * counter exceeds the cap we stop attributing further calls so a
     * malicious or hand-crafted file cannot wedge the resolver.
     * Mirrors the cap added to `c_lsp.c` since the worktree branched
     * (RUST_LSP_FOLLOWUP §B.2). */
    int eval_step_count;

    /* Cargo.toml manifest, when the caller has parsed one and routed
     * it through. The resolver consults `dep_count`/`member_count` so
     * paths beginning with a workspace member or declared dependency
     * route to a sensible canonical form (`<crate>.<tail>`) instead of
     * falling through to module-prefix fallback. NULL when no manifest
     * is available — the resolver still works, just without workspace
     * awareness. Owned by the caller; the RustLSPContext only borrows. */
    const struct CBMCargoManifest *cargo_manifest;

    /* Chalk-lite trait-bound environment for the *currently-active*
     * function or impl. Populated at function/impl entry from the
     * `<T: Bound + …>` parameter list and `where` clause. Consulted
     * by trait method dispatch when the receiver is typed as a type
     * parameter — we look up the param's bounds and try resolving
     * the method on each trait. Also stores associated-type bindings
     * (`T: Iterator<Item = U>` → `U` aliases the Item of `T`).
     *
     * Arrays are arena-allocated and reset on every function entry.
     */
    struct {
        const char *param_name; /* "T", "U", "Item" */
        const char *trait_qn;   /* "core.clone.Clone" */
    } *type_param_bounds;
    int type_param_bound_count;

    struct {
        const char *alias_name; /* "U" */
        const char *aliased_to; /* "T.Item" (representational) */
    } *type_param_aliases;
    int type_param_alias_count;

    /* Output: resolved (and unresolved-with-reason) calls accumulate here. */
    CBMResolvedCallArray *resolved_calls;

    /* Syntactic-call list (result->calls), borrowed from the per-file
     * extraction result. The downstream pipeline only turns a resolved_call
     * into a CALLS edge when a *syntactic* CBMCall with the same
     * (enclosing_func_qn, callee short-name) exists here. Some calls the Rust
     * resolver recovers — operator-trait desugaring (`a + b`) and method
     * calls hidden inside macro token-trees (`format!("{}", d.label())`) —
     * never appear in result->calls because the syntactic extractor cannot
     * see them. When this is non-NULL the resolver injects matching synthetic
     * CBMCall entries so those recovered calls become real edges. NULL in the
     * cross-file path (no result available). */
    CBMCallArray *syn_calls;

    /* While >0, rust_emit_resolved_call also injects a matching synthetic
     * CBMCall into `syn_calls` so the recovered call becomes an edge. Set
     * around the macro-argument re-parse where the syntactic extractor never
     * produced a call node. */
    int inject_syn_calls;

    /* CBM_LSP_DEBUG=1 in env enables verbose stderr trace. */
    bool debug;
} RustLSPContext;

/* Initialise an empty context for processing one file. */
void rust_lsp_init(RustLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                   const CBMTypeRegistry *registry, const char *module_qn,
                   CBMResolvedCallArray *out);

/* Register a `use` alias. `local_name` may be the last segment of the path
 * (`HashMap` for `use std::collections::HashMap`) or an `as` alias. The
 * full `module_path` is stored verbatim (e.g. `std::collections::HashMap`).
 * Glob imports (`use foo::*`) go through `rust_lsp_add_glob` instead. */
void rust_lsp_add_use(RustLSPContext *ctx, const char *local_name, const char *module_path);
void rust_lsp_add_glob(RustLSPContext *ctx, const char *module_qn);

/* Process every function/method in the file, walking statements and
 * evaluating expression types as we go. */
void rust_lsp_process_file(RustLSPContext *ctx, TSNode root);

/* Evaluate the static type of a Rust expression node. Returns
 * `cbm_type_unknown()` for anything we cannot type. */
const CBMType *rust_eval_expr_type(RustLSPContext *ctx, TSNode node);

/* Bidirectional variant: evaluate an expression with an `expected`
 * hint available. The hint is used to:
 *   - disambiguate `Vec::new()` / `HashMap::default()` when the LHS
 *     of a let or function arg constrains the result type;
 *   - infer turbofish-free method generics (`s.parse()` typed as
 *     `Result<i32, _>`);
 *   - thread context into nested expressions (struct field init,
 *     match arm, if-else branches).
 *
 * `expected` may be NULL (no hint). If the synthesised type matches
 * the expected, returns it; if a hint resolves an ambiguity that the
 * synthesis path cannot, returns the hint-substituted form. */
const CBMType *rust_eval_expr_typed(RustLSPContext *ctx, TSNode node, const CBMType *expected);

/* Convert a `*type*` AST node (type_identifier, scoped_type_identifier,
 * reference_type, primitive_type, …) into a CBMType. */
const CBMType *rust_parse_type_node(RustLSPContext *ctx, TSNode node);

/* Bind the bindings introduced by a let/for/match-pattern statement into
 * the current scope. */
void rust_process_statement(RustLSPContext *ctx, TSNode node);

/* Look up an inherent method or field promoted through Deref / embedded
 * trait blanket impls. Returns NULL if nothing matches. */
const CBMRegisteredFunc *rust_lookup_method(RustLSPContext *ctx, const char *type_qn,
                                            const char *member_name);

/* Entry point — called from `cbm_extract_file()` after the unified
 * extractor has filled `result->defs`, `result->imports`, and
 * `result->impl_traits`. Builds a per-file registry, parses the `use`
 * graph, and walks every function body emitting CBMResolvedCall entries. */
void cbm_run_rust_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                      TSNode root);

/* Synthesize call edges for curated attribute proc-macros.
 *
 * For each `CBMDefinition` whose decorators include a known
 * attribute proc-macro (`#[tokio::main]`, `#[tracing::instrument]`,
 * `#[async_trait]`, …), emit synthetic edges from that definition
 * to the helper functions the macro would have injected (e.g.
 * `tokio::runtime::Runtime::new` + `block_on`). The edges carry
 * strategy `lsp_proc_macro` so consumers can distinguish them from
 * direct call attributions.
 *
 * Called automatically from `cbm_run_rust_lsp_with_manifest`. */
void cbm_rust_synth_proc_macro_edges(CBMArena *arena, CBMFileResult *result);

/* Same as `cbm_run_rust_lsp`, but accepts an optional Cargo manifest
 * so the resolver can route paths whose head is a workspace member /
 * declared dependency. Pass NULL to fall back to the manifest-free
 * behaviour. */
void cbm_run_rust_lsp_with_manifest(CBMArena *arena, CBMFileResult *result, const char *source,
                                    int source_len, TSNode root,
                                    const struct CBMCargoManifest *manifest);

/* Register a curated subset of the Rust core/alloc/std prelude into the
 * given registry. The seed is intentionally compact (~150 types, ~600
 * methods) and covers Option/Result/Vec/String/HashMap/BTreeMap/Iterator
 * plus the prelude trait method names (`clone`, `to_string`, `into`, …).
 * Generated from `scripts/gen-rust-stdlib.py` if present, otherwise the
 * hand-written `rust_stdlib_data.c` module is used. */
void cbm_rust_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena);

/* Register seeds for commonly-used external crates (serde, tokio,
 * anyhow, clap, regex, log, futures, parking_lot, once_cell, chrono,
 * uuid, reqwest, rayon). Best-effort curated entries — RUST_LSP_FOLLOWUP
 * §A3. Calls into crates not in this seed remain `unresolved`. */
void cbm_rust_crates_register(CBMTypeRegistry *reg, CBMArena *arena);

/* --- Cross-file LSP resolution --- */

/* Cross-file definition record. The Rust LSP reuses the same shape as
 * `CBMLSPDef` in `go_lsp.h` for compatibility with the pipeline plumbing
 * — see that header for field semantics. We declare a separate typedef
 * to allow Rust-specific fields without touching the Go ABI. */
typedef struct {
    const char *qualified_name;
    const char *short_name;
    const char *label;            /* "Function", "Method", "Type", "Trait" */
    const char *receiver_type;    /* for methods: receiver type QN (NULL for free fns) */
    const char *def_module_qn;    /* module QN where this def lives */
    const char *return_types;     /* "|"-separated return type texts          */
    const char *embedded_types;   /* "|"-separated embedded type QNs          */
    const char *field_defs;       /* "|"-separated "name:type" pairs          */
    const char *method_names_str; /* "|"-separated method names for traits   */
    const char *trait_qn;         /* impl Trait for Type → set on Method defs */
    bool is_interface;            /* true for traits                          */
} CBMRustLSPDef;

/* Run cross-file resolution on a single file. */
void cbm_run_rust_lsp_cross(CBMArena *arena, const char *source, int source_len,
                            const char *module_qn, CBMRustLSPDef *defs, int def_count,
                            const char **import_names, const char **import_qns, int import_count,
                            TSTree *cached_tree, CBMResolvedCallArray *out);

/* Same as `cbm_run_rust_lsp_cross`, plus an optional parsed Cargo manifest
 * (NULL = manifest-free behaviour). The manifest lets call paths whose head
 * is a workspace member / declared dependency route across the crate
 * boundary (`crate_a::foo` → the def inside crate_a). Wired from the
 * cross-file LSP pass (pass_lsp_cross.c) which builds the manifest once from
 * the project root Cargo.toml. */
void cbm_run_rust_lsp_cross_with_manifest(CBMArena *arena, const char *source, int source_len,
                                          const char *module_qn, CBMRustLSPDef *defs, int def_count,
                                          const char **import_names, const char **import_qns,
                                          int import_count, TSTree *cached_tree,
                                          const struct CBMCargoManifest *manifest,
                                          CBMResolvedCallArray *out);

/* Per-file input for batch cross-file Rust LSP processing. */
typedef struct {
    const char *source;
    int source_len;
    const char *module_qn;
    TSTree *cached_tree;
    CBMRustLSPDef *defs;
    int def_count;
    const char **import_names;
    const char **import_qns;
    int import_count;
} CBMBatchRustLSPFile;

/* Process several files in one CGo call (per-file arenas, result copy). */
void cbm_batch_rust_lsp_cross(CBMArena *arena, CBMBatchRustLSPFile *files, int file_count,
                              CBMResolvedCallArray *out);

#endif /* CBM_LSP_RUST_LSP_H */
