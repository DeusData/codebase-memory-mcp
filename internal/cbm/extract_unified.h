#ifndef CBM_EXTRACT_UNIFIED_H
#define CBM_EXTRACT_UNIFIED_H

#include "cbm.h"
#include "lang_specs.h"

// Scope kinds for the walk state stack.
#define SCOPE_FUNC 1
#define SCOPE_CLASS 2
#define SCOPE_CALL 3
#define SCOPE_IMPORT 4
#define SCOPE_LOOP 5
#define SCOPE_BRANCH 6

#define MAX_SCOPES 64

// WalkState tracks scope context during the unified cursor walk.
// Replaces parent-chain walks for enclosing_func_qn, inside_call, etc.
typedef struct {
    const char *enclosing_func_qn;  // current function QN (module_qn at top level)
    const char *enclosing_class_qn; // current class QN (NULL outside class)
    bool inside_call;               // within a call_node_types subtree
    bool inside_import;             // within an import_node_types subtree
    int loop_depth;                 // count of enclosing loop scopes (for bottleneck metrics)
    int branch_depth;               // count of enclosing branch scopes

    struct {
        const char *qn;
        uint32_t depth;
        uint8_t kind;
    } scopes[MAX_SCOPES];
    int scope_top;
} WalkState;

// Per-node handler prototypes. Each is called once per node during the
// unified cursor walk, replacing the old recursive walk_* functions.
void handle_calls(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state);
void handle_usages(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state);
void handle_throws(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state);
void handle_readwrites(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state);
void handle_type_refs(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state);
void handle_env_accesses(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                         WalkState *state);
void handle_type_assigns(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                         WalkState *state);

// Single-pass extraction using TSTreeCursor. Visits every node once,
// dispatching to all handlers per node. Replaces the 7 separate walk_*
// functions for calls/usages/throws/readwrites/type_refs/env_accesses/type_assigns.
// Definitions and imports stay as separate passes (different recursion patterns).
void cbm_extract_unified(CBMExtractCtx *ctx);

#endif // CBM_EXTRACT_UNIFIED_H
