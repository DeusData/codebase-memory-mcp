#include "scope.h"
#include <string.h>

CBMScope* cbm_scope_push(CBMArena* a, CBMScope* current) {
    CBMScope* scope = (CBMScope*)cbm_arena_alloc(a, sizeof(CBMScope));
    if (!scope) {
        return current;
    }
    memset(scope, 0, sizeof(CBMScope));
    scope->parent = current;
    scope->arena = a;
    return scope;
}

CBMScope* cbm_scope_pop(CBMScope* scope) {
    if (!scope) {
        return NULL;
    }
    return scope->parent;
}

static CBMScopeChunk* alloc_chunk(CBMScope* scope) {
    if (!scope->arena) {
        return NULL;
    }
    CBMScopeChunk* c = (CBMScopeChunk*)cbm_arena_alloc(scope->arena, sizeof(CBMScopeChunk));
    if (!c) {
        return NULL;
    }
    memset(c, 0, sizeof(CBMScopeChunk));
    c->next = scope->chunks;
    scope->chunks = c;
    return c;
}

static void cbm_scope_bind_value(CBMScope *scope, const char *name, const CBMType *type,
                                 const char *callable_qn) {
    if (!scope || !name) {
        return;
    }
    for (CBMScopeChunk* c = scope->chunks; c != NULL; c = c->next) {
        for (int i = 0; i < c->used; i++) {
            if (c->bindings[i].name && strcmp(c->bindings[i].name, name) == 0) {
                c->bindings[i].type = type;
                c->bindings[i].callable_qn = callable_qn;
                return;
            }
        }
    }
    CBMScopeChunk* head = scope->chunks;
    if (!head || head->used >= CBM_SCOPE_CHUNK_BINDINGS) {
        head = alloc_chunk(scope);
        if (!head) {
            return;
        }
    }
    head->bindings[head->used].name = name;
    head->bindings[head->used].type = type;
    head->bindings[head->used].callable_qn = callable_qn;
    head->used++;
}

void cbm_scope_bind(CBMScope *scope, const char *name, const CBMType *type) {
    cbm_scope_bind_value(scope, name, type, NULL);
}

void cbm_scope_bind_callable(CBMScope *scope, const char *name, const CBMType *type,
                             const char *callable_qn) {
    cbm_scope_bind_value(scope, name, type, callable_qn);
}

const CBMType* cbm_scope_lookup(const CBMScope* scope, const char* name) {
    if (!name) {
        return cbm_type_unknown();
    }
    for (const CBMScope* s = scope; s != NULL; s = s->parent) {
        for (CBMScopeChunk* c = s->chunks; c != NULL; c = c->next) {
            for (int i = 0; i < c->used; i++) {
                if (c->bindings[i].name && strcmp(c->bindings[i].name, name) == 0) {
                    return c->bindings[i].type;
                }
            }
        }
    }
    return cbm_type_unknown();
}

bool cbm_scope_contains(const CBMScope *scope, const char *name) {
    if (!name) {
        return false;
    }
    for (const CBMScope *s = scope; s != NULL; s = s->parent) {
        for (const CBMScopeChunk *c = s->chunks; c != NULL; c = c->next) {
            for (int i = 0; i < c->used; i++) {
                if (c->bindings[i].name && strcmp(c->bindings[i].name, name) == 0) {
                    return true;
                }
            }
        }
    }
    return false;
}

const char *cbm_scope_lookup_callable(const CBMScope *scope, const char *name) {
    if (!name) {
        return NULL;
    }
    for (const CBMScope *s = scope; s != NULL; s = s->parent) {
        for (const CBMScopeChunk *c = s->chunks; c != NULL; c = c->next) {
            for (int i = 0; i < c->used; i++) {
                if (c->bindings[i].name && strcmp(c->bindings[i].name, name) == 0) {
                    return c->bindings[i].callable_qn;
                }
            }
        }
    }
    return NULL;
}

bool cbm_scope_update_callable(CBMScope *scope, const char *name, const char *callable_qn) {
    if (!name) {
        return false;
    }
    for (CBMScope *s = scope; s != NULL; s = s->parent) {
        for (CBMScopeChunk *c = s->chunks; c != NULL; c = c->next) {
            for (int i = 0; i < c->used; i++) {
                if (c->bindings[i].name && strcmp(c->bindings[i].name, name) == 0) {
                    c->bindings[i].callable_qn = callable_qn;
                    return true;
                }
            }
        }
    }
    return false;
}
