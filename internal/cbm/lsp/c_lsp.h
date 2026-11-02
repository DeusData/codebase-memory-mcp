#ifndef CBM_LSP_C_LSP_H
#define CBM_LSP_C_LSP_H

#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"
#include "../resolve/types.h"
#include "../resolve/imports.h"
#include "go_lsp.h" // for CBMLSPDef, CBMResolvedCallArray

// CLSPContext holds state for C/C++ expression type evaluation within a file.
typedef struct {
    CBMArena *arena;
    const char *source;
    int source_len;
    const CBMTypeRegistry *registry;
    /* The writable head of the registry chain: the per-file overlay the
     * dispatcher hands in, or the per-file registry on the non-cross path.
     * Every refinement (lazy add, min_params, template params) goes here
     * via cbm_registry_*_for_update; nothing behind ->fallback is written. */
    CBMTypeRegistry *registry_head;
    const CBMTypeRegistry *proof_registry; /* immutable declaration boundary */
    struct CProofDeclaration *proof_declarations;
    int proof_declaration_count, proof_declaration_cap;
    struct CProofInclude *proof_includes;
    /* Where this file includes a quoted header the index holds nothing for, in file order. */
    uint32_t *proof_unknown_includes;
    int proof_unknown_include_count, proof_unknown_include_cap;
    /* The translation unit: every file this one includes, through other files
     * too, with the point in this file where it became visible. */
    struct CTUModule *tu_modules;
    uint32_t tu_cap, tu_count;
    bool tu_adding_includer_context;
    bool local_tag_declared; /* a block of this file declared a tag of its own */
    const struct CBMCDeclIndex *c_decls;
    const char *own_file_qn; /* NULL when only the module identifies this file */
    /* C++: the class whose member function this is, inline or out of line. Its members
     * are found before anything at namespace scope. */
    const CBMRegisteredType *proof_method_class;
    bool proof_class_scope_checked; /* the current name was looked for in that class */
    bool plain_header;              /* this TU is a .h, so its own language is its includer's */
    /* The argument list last asked about: is its callee a visible macro, and which of its bare
     * names reach the expansion as themselves. Start bytes, ascending. */
    const char **outer_template_params; /* every enclosing template's parameter names */
    int outer_template_param_count;
    const void *macro_args_list;
    const uint32_t *macro_plain_args;
    int macro_plain_count;
    bool macro_args_of_macro;
    /* Name to CMacroDefs: the definitions in this translation unit, each name scanned once. */
    struct CBMHashTable *macro_defs;
    bool macro_defs_ready;
    const cbm_resolve_import_snapshot_t *import_facts;
    bool require_import_proofs;
    struct CProofBuiltinBinding *proof_builtins;
    int proof_builtin_count, proof_builtin_cap;
    struct CProofUsing *proof_usings;
    int proof_using_count, proof_using_cap;
    bool binding_failed;
    bool proof_preprocessor_unknown;
    uint32_t proof_syntax_limit; /* first unaudited syntax error; earlier complete sites survive */
    int callable_flow_depth;     /* delayed bodies cannot borrow mutable outer callable state */
    CBMScope *current_scope;
    CBMResolveTypeMap field_types; /* explicit declaration evidence, keyed by lexical binding */

    // Include map: header_path -> namespace QN prefix
    const char **include_paths;
    const char **include_ns_qns;
    int include_count;

    // Namespace state
    const char *current_namespace; // current namespace QN (e.g., "proj.ns1.ns2")

    // Using namespace directives
    const char **using_namespaces;
    int using_ns_count;
    int using_ns_cap;

    // Using declarations: specific names imported into scope
    const char **using_decl_names;
    const char **using_decl_qns;
    int using_decl_count;
    int using_decl_cap;

    // Namespace aliases: short -> full QN
    const char **ns_alias_names;
    const char **ns_alias_qns;
    int ns_alias_count;
    int ns_alias_cap;

    // Current context
    const char *enclosing_func_qn;
    const char *enclosing_class_qn; // for implicit `this` resolution
    const char *module_qn;
    size_t module_qn_len; // cached strlen(module_qn); for stack-buffer QN building

    // Negative-lookup memo for c_lookup_member_depth (depth==0 misses only).
    // Open-addressing uint64 hash SET; 0 is the empty-slot sentinel. Populated
    // ONLY when the Tier-2 registry is shared+read-only (registry_shared), where
    // the module-prefix/base-class/short-name cascades are pure, stable functions
    // of (type_qn, registry) — so a recorded miss can never turn into a hit. Lets
    // the hot resolve path skip the sprintf("%s.%s") strlen storm + the O(type_count)
    // short-name scan on repeated misses of the same (type_qn, member). malloc-owned;
    // freed at end of c_lsp_process_file.
    uint64_t *neg_memo;
    int neg_memo_cap;   // power-of-two; 0 until first insert
    int neg_memo_count; // live entries (grow by rehash at 70% load)

    // Field references already emitted for this file, keyed by occurrence,
    // enclosing function, owner type and field. Same hash-set ownership as
    // neg_memo; freed at end of c_lsp_process_file.
    uint64_t *field_ref_seen;
    int field_ref_seen_cap;
    int field_ref_seen_count;

    // The type an initializer_list is about to initialize, handed from the
    // declarator / compound literal / enclosing list that knows it to the walk
    // of that list. Bound to the list's node id: a hint nothing consumed must
    // never type a later, unrelated list.
    const CBMType *init_list_type;
    const void *init_list_id;

    // Output
    CBMResolvedCallArray *resolved_calls;
    CBMSourceOrigin source_origin; // source buffer represented by emitted occurrence spans
    const CBMFileResult *test_definition_owners; /* borrowed, prevalidated raw source identity */
    int test_owner_seen_count;
    bool test_owner_error;

    // Function pointer targets: lexical binding -> exact target function QN.
    // A NULL target is an explicitly unknown/ambiguous binding and must shadow
    // any outer exact target of the same name.
    const char **fp_var_names;
    const char **fp_target_qns;
    const CBMScope **fp_binding_scopes;
    bool *fp_target_proven;
    // Storage escape persists for this lexical binding, across assignments.
    bool *fp_storage_escaped;
    // Immutable assignment/copy origin, owned by this context's arena.
    const cbm_resolve_proof_t **fp_target_proofs;
    int fp_count;
    int fp_cap;

    // Template parameter defaults for current template scope
    const char **template_param_names;       // e.g., ["T", "U"]
    const CBMType **template_param_defaults; // e.g., [int_type, NULL]
    int template_param_count;

    // Pending template calls: member calls on TYPE_PARAM inside template functions.
    // At call sites with known arg types, these are resolved retroactively.
    struct {
        const char *func_qn;     // enclosing template function
        const char *type_param;  // e.g., "T"
        const char *method_name; // e.g., "draw"
        int arg_count;
    } *pending_template_calls;
    int pending_tc_count;
    int pending_tc_cap;

    // Flags
    bool cpp_mode;        // C++ features enabled
    bool in_template;     // currently inside template declaration
    bool registry_shared; // ctx->registry is the Tier-2 cross registry, shared
                          // READ-ONLY across resolve workers — never mutate it
                          // (and never store per-worker arena pointers into it)
    bool debug;
    int eval_depth;            // recursion depth for c_eval_expr_type (crash guard)
    uint32_t eval_truncations; // depth-cap cutoffs; a truncated result is never memoized
    // Per-evaluation memo (#1527): results of the sub-expressions evaluated
    // during ONE outermost c_eval_expr_type call, keyed by node identity. The
    // evaluator re-evaluates a call's receiver (member lookup, then template
    // substitution), which made an N-link fluent chain cost 2^N; the per-file
    // step cap that used to hide that is gone. Evaluation never mutates scope
    // or context, so within one outermost call a node's type cannot change;
    // the memo is emptied when the next outermost call starts, because the
    // resolver walk mutates scope between calls.
    struct CLSPEvalMemoSlot *eval_memo; // open addressing, arena-owned
    int *eval_memo_used;                // slots filled by the current evaluation
    int eval_memo_cap;                  // power of two, or 0
    int eval_memo_count;
    // Pending links of the left-associative chains being evaluated
    // iteratively (operator, receiver, subscript, conditional, parenthesis and
    // comma chains): one stack shared by nested evaluations, arena-owned.
    TSNode *eval_chain;
    int eval_chain_len;
    int eval_chain_cap;
    CBMLSPStatus eval_failure; // sticky: stop refinement rather than repeat uncached work
#ifdef CBM_ENABLE_TEST_SEAMS
    int test_memo_allocations_left; // -1 = normal; 0 = fail the next memo allocation
#endif
    int walk_depth;         // c_resolve_calls_in_node self-recursion (AST nesting)
    int control_flow_depth; // if/loop/switch/catch nesting; assignments merge fail-closed
} CLSPContext;

struct CLSPEvalMemoSlot {
    const void *node_id; // TSNode.id; NULL = empty
    const CBMType *result;
};

/* Conservative syntax audit shared with the source-fact collector. Only direct,
 * fixed-arity free signatures with explicitly written builtin types qualify. */
bool cbm_c_preprocessor_domain_unsupported(TSNode root, const char *source);
const CBMType *cbm_c_builtin_annotation(CBMArena *arena, const char *source, TSNode type);
const CBMType *cbm_c_free_signature(CBMArena *arena, const char *source, TSNode declaration,
                                    bool cpp_mode);

// --- API ---

void c_lsp_init(CLSPContext *ctx, CBMArena *arena, const char *source, int source_len,
                const CBMTypeRegistry *registry, const char *module_qn, bool cpp_mode,
                CBMResolvedCallArray *out);

void c_lsp_add_include(CLSPContext *ctx, const char *header_path, const char *ns_qn);

void c_lsp_process_file(CLSPContext *ctx, TSNode root);

const CBMType *c_eval_expr_type(CLSPContext *ctx, TSNode node);
const CBMType *c_parse_type_node(CLSPContext *ctx, TSNode node);
void c_process_statement(CLSPContext *ctx, TSNode node);

// Look up a member (method/field) on a type, traversing base classes.
const CBMRegisteredFunc *c_lookup_member(CLSPContext *ctx, const char *type_qn,
                                         const char *member_name);

// Type simplification: unwrap refs, aliases, pointers (like clangd simplifyType).
const CBMType *c_simplify_type(CLSPContext *ctx, const CBMType *t, bool unwrap_pointer);

// --- Entry points ---

// Single-file LSP: build registry from file defs + stdlib, run resolution.
void cbm_run_c_lsp(CBMArena *arena, CBMFileResult *result, const char *source, int source_len,
                   TSNode root, bool cpp_mode, CBMSourceOrigin source_origin);

// A memo allocation/capacity failure is an explicit incomplete-refinement error.
#define CBM_C_LSP_DEPTH_ERROR "C/C++ expression recursion depth limit exceeded"
#define CBM_C_LSP_MEMO_ERROR "C/C++ expression memo allocation or capacity failure"
void cbm_c_lsp_record_failure(CBMFileResult *result, CBMLSPStatus status);
#ifdef CBM_ENABLE_TEST_SEAMS
// Thread-local, scoped to memo allocations only; restore with -1 after a test.
void cbm_c_lsp_test_memo_fail_after(int successful_allocations);
void cbm_c_lsp_test_depth_fail(bool enabled);
uint32_t cbm_c_lsp_test_walks_take(void);
#endif

// Cross-file LSP: build registry from defs + stdlib, re-parse and resolve.
// Status variants distinguish memo/depth failure. Bool wrappers report success.
// Completed output before a failed walk is kept.
CBMLSPStatus cbm_run_c_lsp_cross_status(CBMArena *arena, const char *source, int source_len,
                                        const char *module_qn, bool cpp_mode, CBMLSPDef *defs,
                                        int def_count, const char **include_paths,
                                        const char **include_ns_qns, int include_count,
                                        TSTree *cached_tree, // NULL = parse internally
                                        CBMResolvedCallArray *out);

bool cbm_run_c_lsp_cross(CBMArena *arena, const char *source, int source_len, const char *module_qn,
                         bool cpp_mode, CBMLSPDef *defs, int def_count, const char **include_paths,
                         const char **include_ns_qns, int include_count,
                         TSTree *cached_tree, // NULL = parse internally
                         CBMResolvedCallArray *out);

// Tier 2: build a project-wide C/C++/CUDA registry ONCE from all defs
// (filters by lang). Shared READ-ONLY across resolve workers. Def-driven
// (no AST field collection) → identical entries to the per-file build.
CBMTypeRegistry *cbm_c_build_cross_registry(CBMArena *arena, CBMLSPDef *defs, int def_count);

// Cross-file LSP using a pre-built shared registry (Tier 2). Skips the
// per-file registry build; just parse + resolve.
CBMLSPStatus cbm_run_c_lsp_cross_with_registry_status(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *reg, // pre-built, finalized, READ-ONLY
    const char **include_paths, const char **include_ns_qns, int include_count,
    TSTree *cached_tree, // NULL = parse internally
    CBMResolvedCallArray *out);

bool cbm_run_c_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                       const char *module_qn, bool cpp_mode,
                                       CBMTypeRegistry *reg, // pre-built, finalized, READ-ONLY
                                       const char **include_paths, const char **include_ns_qns,
                                       int include_count,
                                       TSTree *cached_tree, // NULL = parse internally
                                       CBMResolvedCallArray *out);

/* Additive configured-owner entrypoints. Owners must outlive the call. With
 * owners, RAW origin, exact source identity and module identity are required.
 * Configured owners fresh-parse verified source: supplied roots/cached trees
 * have no source provenance and are not trusted. NULL owners retain caching.
 * False leaves output untouched when identity validation fails. A later
 * mapping/OOM failure may leave partial output that callers must discard. QNs
 * in successful output belong to the supplied output arena. Callers must
 * abort publication on false. Legacy wrappers pass NULL and retain previous behavior. */
bool cbm_run_c_lsp_with_test_owners(CBMArena *arena, CBMFileResult *result, const char *source,
                                    int source_len, TSNode root, bool cpp_mode,
                                    CBMSourceOrigin source_origin,
                                    const CBMFileResult *test_owners);
bool cbm_run_c_lsp_cross_with_test_owners(CBMArena *arena, const char *source, int source_len,
                                          const char *module_qn, bool cpp_mode, CBMLSPDef *defs,
                                          int def_count, const char **include_paths,
                                          const char **include_ns_qns, int include_count,
                                          TSTree *cached_tree, CBMResolvedCallArray *out,
                                          CBMSourceOrigin source_origin,
                                          const CBMFileResult *test_owners);
bool cbm_run_c_lsp_cross_with_registry_with_test_owners(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *reg, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    CBMSourceOrigin source_origin, const CBMFileResult *test_owners);

// Register C stdlib types and functions into a registry.
void cbm_c_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena);

// Register C++ stdlib types and functions into a registry.
void cbm_cpp_stdlib_register(CBMTypeRegistry *reg, CBMArena *arena);

// --- Batch cross-file LSP ---

// Per-file input for batch C/C++ LSP processing.
typedef struct {
    const char *source;
    int source_len;
    const char *module_qn;
    bool cpp_mode;
    TSTree *cached_tree; // from TSTree caching (NULL = parse internally)
    CBMLSPDef *defs;     // combined file-local + cross-file defs
    int def_count;
    const char **include_paths; // parallel arrays, include_count long
    const char **include_ns_qns;
    int include_count;
} CBMBatchCLSPFile;

// Process multiple C/C++ files' cross-file LSP in one CGo call.
// out must point to file_count pre-zeroed CBMResolvedCallArray structs.
bool cbm_batch_c_lsp_cross(CBMArena *arena, CBMBatchCLSPFile *files, int file_count,
                           CBMResolvedCallArray *out);

CBMLSPStatus cbm_run_c_lsp_cross_with_registry_status_with_source(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMTypeRegistry *reg, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    const cbm_resolve_proof_source_t *source_context);

CBMLSPStatus cbm_run_c_lsp_cross_status_with_source(
    CBMArena *arena, const char *source, int source_len, const char *module_qn, bool cpp_mode,
    CBMLSPDef *defs, int def_count, const char **include_paths, const char **include_ns_qns,
    int include_count, TSTree *cached_tree, CBMResolvedCallArray *out,
    const cbm_resolve_proof_source_t *source_context);

/* Production entry points retain normalized dependency predecessors. The bool
 * result is the configured test-owner validation; status reports walk failure. */
bool cbm_run_c_lsp_cross_with_imports(CBMArena *, const char *, int, const char *, bool,
                                      CBMLSPDef *, int, const char **, const char **, int, TSTree *,
                                      CBMResolvedCallArray *, const CBMFileResult *,
                                      const cbm_resolve_import_snapshot_t *, CBMLSPStatus *);
bool cbm_run_c_lsp_cross_with_registry_with_imports(CBMArena *, const char *, int, const char *,
                                                    bool, CBMTypeRegistry *, const char **,
                                                    const char **, int, TSTree *,
                                                    CBMResolvedCallArray *, const CBMFileResult *,
                                                    const cbm_resolve_import_snapshot_t *,
                                                    CBMLSPStatus *);

#endif // CBM_LSP_C_LSP_H
