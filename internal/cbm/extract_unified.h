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
#define SCOPE_LEXICAL 7
#define SCOPE_NAMESPACE 8

#define MAX_SCOPES 64
#define INLINE_LEXICAL_SCOPES 64
#define INLINE_LEXICAL_BINDINGS 64
#define INLINE_PYTHON_DIRECTIVES 16

// ObjectScript type map: variable name → class name (for instance_method_call
// resolution). Stack-allocated, per-method scope. Overflow is silent (no crash).
#define OS_TYPE_MAP_CAP 64
typedef struct {
    const char *var_name;
    const char *class_name;
} os_type_entry_t;

typedef struct {
    os_type_entry_t entries[OS_TYPE_MAP_CAP];
    int count;
    int class_base_count; // entries [0,class_base_count) survive method-scope resets
} os_type_map_t;

// A call consumes only the exact AST occurrence that denotes its callee.  The
// rest of the call subtree (receiver, computed key, arguments, callback body)
// remains ordinary expression input and is eligible for USAGE extraction.
typedef enum {
    CBM_INVOCATION_NONE = 0,
    CBM_INVOCATION_PRIMARY,
    CBM_INVOCATION_CALLABLE_REFERENCE,
} CBMInvocationKind;

typedef struct {
    CBMInvocationKind kind;
    TSNode site;
    TSNode callee_expr;
    TSNode callee_leaf;
    const char *callee_name;
    bool raw_call_emitted;
} CBMInvocationDescriptor;

typedef struct {
    const char *qn;
    uint32_t depth;
    uint32_t lexical_scope_id;
    uint8_t kind;
    CBMInvocationKind invocation_kind;
    TSNode callee_expr;
    TSNode callee_leaf;
} CBMWalkScope;

typedef enum {
    CBM_LEXICAL_SCOPE_MODULE = 0,
    CBM_LEXICAL_SCOPE_CLASS,
    CBM_LEXICAL_SCOPE_FUNCTION,
    CBM_LEXICAL_SCOPE_BLOCK,
    CBM_LEXICAL_SCOPE_COMPREHENSION,
} CBMLexicalScopeKind;

/* Concrete AST scope identity. QNs remain graph-attribution metadata only;
 * overloads, lambdas and sibling blocks therefore never share binding facts. */
typedef struct {
    uint32_t id;
    uint32_t parent_id;
    uint32_t lookup_parent_id;
    uint32_t start_byte;
    uint32_t end_byte;
    uint8_t kind;
} CBMLexicalScope;

/* Deferred binding event. Applying these after the walk represents hoisted
 * and whole-scope rules without depending on traversal order. */
typedef struct {
    uint32_t scope_id;
    uint32_t active_start;
    uint32_t active_end;
    const char *name;
} CBMLexicalBinding;

typedef enum {
    CBM_PYTHON_DIRECTIVE_GLOBAL = 1,
    CBM_PYTHON_DIRECTIVE_NONLOCAL,
} CBMPythonDirectiveKind;

typedef struct {
    uint32_t function_scope_id;
    const char *name;
    uint8_t kind;
} CBMPythonDirective;

// WalkState tracks scope context during the unified cursor walk.
// Replaces parent-chain walks for enclosing_func_qn, import context, etc.
typedef struct {
    const char *enclosing_func_qn;      // current function QN (module_qn at top level)
    const char *enclosing_class_qn;     // current class QN (NULL outside class)
    const TSTreeCursor *current_cursor; // unified walk cursor at the current node
    TSTreeCursor *occurrence_cursor;    // reusable parent-preserving classifier cursor
    CBMInvocationKind invocation_kind;  // exact active invocation/reference role
    TSNode callee_expr;                 // exact active callee expression, if any
    TSNode callee_leaf;                 // exact active terminal callee, if any
    bool inside_import;                 // within an import_node_types subtree
    int loop_depth;                     // count of enclosing loop scopes (for bottleneck metrics)
    int branch_depth;                   // count of enclosing branch scopes

    CBMArena *arena;
    CBMWalkScope *scopes;
    CBMWalkScope inline_scopes[MAX_SCOPES];
    int scope_capacity;
    int scope_top;

    CBMLexicalScope *lexical_scopes;
    CBMLexicalScope inline_lexical_scopes[INLINE_LEXICAL_SCOPES];
    int lexical_scope_capacity;
    int lexical_scope_count;
    uint32_t root_lexical_scope_id;
    uint32_t split_function_scope_id;
    uint32_t split_signature_start_byte;
    uint32_t split_signature_end_byte;
    const char *split_function_qn;
    uint32_t flat_function_scope_id;
    uint32_t flat_anchor_start_byte;
    uint32_t flat_anchor_end_byte;
    const char *flat_function_qn;

    CBMLexicalBinding *lexical_bindings;
    CBMLexicalBinding inline_lexical_bindings[INLINE_LEXICAL_BINDINGS];
    int lexical_binding_capacity;
    int lexical_binding_count;
    int usage_start_index;
    bool lexical_binding_tracking_failed;
    CBMPythonDirective *python_directives;
    CBMPythonDirective inline_python_directives[INLINE_PYTHON_DIRECTIVES];
    int python_directive_capacity;
    int python_directive_count;
    CBMLanguage language;

    os_type_map_t os_type_map; // ObjectScript variable → type mapping
} WalkState;

// Per-node handler prototypes. Each is called once per node during the
// unified cursor walk, replacing the old recursive walk_* functions.
CBMInvocationDescriptor handle_calls(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec,
                                     WalkState *state);
void handle_usages(CBMExtractCtx *ctx, TSNode node, const CBMLangSpec *spec, WalkState *state);
void cbm_finalize_lexical_usages(CBMExtractCtx *ctx, WalkState *state);
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
