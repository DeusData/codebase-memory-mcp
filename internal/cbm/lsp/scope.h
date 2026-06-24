#ifndef CBM_LSP_SCOPE_H
#define CBM_LSP_SCOPE_H

#include "type_rep.h"
#include "../arena.h"

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

CBMScope* cbm_scope_push(CBMArena* a, CBMScope* current);
CBMScope* cbm_scope_pop(CBMScope* scope);
void cbm_scope_bind(CBMScope* scope, const char* name, const CBMType* type);
const CBMType* cbm_scope_lookup(const CBMScope* scope, const char* name);

#endif // CBM_LSP_SCOPE_H
