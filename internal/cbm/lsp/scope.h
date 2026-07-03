#ifndef CBM_LSP_SCOPE_H
#define CBM_LSP_SCOPE_H

#include "type_rep.h"
#include "../arena.h"
#include <stdlib.h> /* getenv, atoi (cbm_lsp_max_walk_depth) */

typedef struct {
    const char* name;
    const CBMType* type;
} CBMVarBinding;

#define CBM_SCOPE_CHUNK_BINDINGS 16

typedef struct CBMScopeChunk {
    CBMVarBinding bindings[CBM_SCOPE_CHUNK_BINDINGS];
    int used;
    struct CBMScopeChunk* next;
} CBMScopeChunk;

typedef struct CBMScope {
    struct CBMScope* parent;
    CBMScopeChunk* chunks;
    CBMArena* arena;        // owning arena, propagated to children at push time
} CBMScope;

// Bail-to-UNKNOWN depth for type-lookup chains: alias resolution, MRO walks,
// embedded-field/struct-traversal. Exceeding this collapses to cbm_type_unknown
// rather than recursing — guards against pathological hierarchies.
#define CBM_LSP_MAX_LOOKUP_DEPTH 16

// Recursion cap for the per-language "resolve calls in AST node" walkers. These
// recurse once per AST nesting level; a deeply-nested or cyclic file can drive
// them into a native stack overflow (SIGSEGV) that takes down the whole index.
// Past this cap the wrapper skips the subtree — those calls stay unresolved,
// which is graceful degradation, not a crash. 512 is far deeper than any
// hand-written source nests; override for pathological/generated repos via the
// CBM_LSP_MAX_WALK_DEPTH env var (positive integer).
#define CBM_LSP_MAX_WALK_DEPTH 512

// Resolved walk-depth cap: env override (CBM_LSP_MAX_WALK_DEPTH, if a positive
// integer) else CBM_LSP_MAX_WALK_DEPTH. Read once and cached — the walkers call
// this per node, so it must not hit getenv on the hot path. The cache is a
// benign idempotent race under multi-threaded indexing (every thread computes
// the same value).
static inline int cbm_lsp_max_walk_depth(void) {
    static int cached = -1;
    if (cached < 0) {
        const char* e = getenv("CBM_LSP_MAX_WALK_DEPTH");
        int v = (e && *e) ? atoi(e) : 0;
        cached = (v > 0) ? v : CBM_LSP_MAX_WALK_DEPTH;
    }
    return cached;
}

CBMScope* cbm_scope_push(CBMArena* a, CBMScope* current);
CBMScope* cbm_scope_pop(CBMScope* scope);
void cbm_scope_bind(CBMScope* scope, const char* name, const CBMType* type);
const CBMType* cbm_scope_lookup(const CBMScope* scope, const char* name);

#endif // CBM_LSP_SCOPE_H
