#ifndef CBM_LSP_C_LSP_H
#define CBM_LSP_C_LSP_H

#include "type_rep.h"
#include "scope.h"
#include "type_registry.h"
#include "../cbm.h"
#include "go_lsp.h" // for CBMLSPDef, CBMResolvedCallArray

// CLSPContext holds state for C/C++ expression type evaluation within a file.
typedef struct {
    CBMArena *arena;
    const char *source;
    int source_len;
    const CBMTypeRegistry *registry;
    CBMScope *current_scope;

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

    // Output
    CBMResolvedCallArray *resolved_calls;

    // Function pointer targets: var_name -> target function QN
    const char **fp_var_names;
    const char **fp_target_qns;
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
    int eval_depth; // recursion depth for c_eval_expr_type (crash guard)
    int eval_steps; // total expression eval calls for current file (hang guard)
    int walk_depth; // c_resolve_calls_in_node self-recursion (AST nesting)
} CLSPContext;

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
                   TSNode root, bool cpp_mode);

// Cross-file LSP: build registry from defs + stdlib, re-parse and resolve.
void cbm_run_c_lsp_cross(CBMArena *arena, const char *source, int source_len, const char *module_qn,
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
void cbm_run_c_lsp_cross_with_registry(CBMArena *arena, const char *source, int source_len,
                                       const char *module_qn, bool cpp_mode,
                                       CBMTypeRegistry *reg, // pre-built, finalized, READ-ONLY
                                       const char **include_paths, const char **include_ns_qns,
                                       int include_count,
                                       TSTree *cached_tree, // NULL = parse internally
                                       CBMResolvedCallArray *out);

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
void cbm_batch_c_lsp_cross(CBMArena *arena, CBMBatchCLSPFile *files, int file_count,
                           CBMResolvedCallArray *out);

#endif // CBM_LSP_C_LSP_H
