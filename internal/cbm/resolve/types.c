#include "types.h"
#include "policy.h"
#include <limits.h>
#include <string.h>

struct cbm_resolve_type_slot {
    cbm_resolve_type_fact_t fact;
    const CBMType *observed_type; // source snapshot, including rejected richer types
};

enum { TYPE_MAP_INITIAL_CAPACITY = 32, TYPE_MAP_DEFAULT_STEPS = 65536 };

static cbm_resolve_type_fact_t unknown_fact(cbm_resolve_type_status_t status) {
    cbm_resolve_type_fact_t fact = {0};
    fact.type = cbm_type_unknown();
    fact.status = status;
    return fact;
}

static bool type_step(CBMResolveTypeMap *map) {
    if (!map || map->failure != CBM_RESOLVE_TYPE_UNKNOWN) {
        return false;
    }
    if (map->steps >= map->step_limit) {
        map->failure = CBM_RESOLVE_TYPE_LIMITED;
        return false;
    }
    map->steps++;
    return true;
}

void cbm_resolve_types_init(CBMResolveTypeMap *map, CBMArena *arena, uint64_t step_limit) {
    if (!map) {
        return;
    }
    *map = (CBMResolveTypeMap){.arena = arena,
                               .step_limit = step_limit ? step_limit : TYPE_MAP_DEFAULT_STEPS};
}

static size_t binding_hash(const CBMVarBinding *binding) {
    uintptr_t value = (uintptr_t)binding >> 3;
    value ^= value >> 17;
    value *= (uintptr_t)0xed5ad4bbu;
    return (size_t)(value ^ (value >> 11));
}

static cbm_resolve_type_slot_t *find_slot(CBMResolveTypeMap *map, const CBMVarBinding *binding) {
    if (!map->capacity) {
        return NULL;
    }
    size_t mask = map->capacity - 1;
    size_t slot = binding_hash(binding) & mask;
    while (map->slots[slot].fact.binding) {
        /* Probe placement depends on arena addresses. Do not charge it to the
         * semantic work budget: ASLR must not change an analysis outcome. The
         * table's load factor and capacity bound this accelerator walk. */
        if (map->slots[slot].fact.binding == binding) {
            return &map->slots[slot];
        }
        slot = (slot + 1) & mask;
    }
    return &map->slots[slot];
}

static bool grow_slots(CBMResolveTypeMap *map) {
    if (map->capacity && map->count < map->capacity / 2) {
        return true;
    }
    size_t capacity = map->capacity ? map->capacity * 2 : TYPE_MAP_INITIAL_CAPACITY;
    if (capacity < map->capacity || capacity > SIZE_MAX / sizeof(*map->slots)) {
        map->failure = CBM_RESOLVE_TYPE_FAILED;
        return false;
    }
    cbm_resolve_type_slot_t *slots =
        map->arena ? cbm_arena_calloc(map->arena, capacity * sizeof(*slots)) : NULL;
    if (!slots) {
        map->failure = CBM_RESOLVE_TYPE_FAILED;
        return false;
    }
    cbm_resolve_type_slot_t *old = map->slots;
    size_t old_capacity = map->capacity;
    map->slots = slots;
    map->capacity = capacity;
    for (size_t i = 0; i < old_capacity; i++) {
        if (!type_step(map)) {
            return false;
        }
        if (old[i].fact.binding) {
            cbm_resolve_type_slot_t *slot = find_slot(map, old[i].fact.binding);
            if (!slot) {
                return false;
            }
            *slot = old[i];
        }
    }
    return true;
}

/* Deliberately narrow: richer CBMType variants remain available to language
 * engines, but this shared proof map does not assert conditional/structural/
 * generic semantics. Unknown or multi-arm unions can never produce a unique
 * receiver. Wrapper cycles and deep aliases fail closed. */
static cbm_resolve_type_status_t validate_type(CBMResolveTypeMap *map, const CBMType *type) {
    for (int depth = 0; depth < CBM_LSP_MAX_LOOKUP_DEPTH; depth++) {
        if (!type_step(map)) {
            return map ? map->failure : CBM_RESOLVE_TYPE_FAILED;
        }
        if (!type || type->kind == CBM_TYPE_UNKNOWN) {
            return CBM_RESOLVE_TYPE_UNKNOWN;
        }
        switch (type->kind) {
        case CBM_TYPE_NAMED:
            return type->data.named.qualified_name && *type->data.named.qualified_name
                       ? CBM_RESOLVE_TYPE_KNOWN
                       : CBM_RESOLVE_TYPE_UNKNOWN;
        case CBM_TYPE_BUILTIN:
            return type->data.builtin.name && *type->data.builtin.name ? CBM_RESOLVE_TYPE_KNOWN
                                                                       : CBM_RESOLVE_TYPE_UNKNOWN;
        case CBM_TYPE_POINTER:
            type = type->data.pointer.elem;
            break;
        case CBM_TYPE_REFERENCE:
        case CBM_TYPE_RVALUE_REF:
            type = type->data.reference.elem;
            break;
        case CBM_TYPE_ALIAS:
            type = type->data.alias.underlying;
            break;
        case CBM_TYPE_UNION:
            if (type->data.union_type.count != 1 || !type->data.union_type.members) {
                return CBM_RESOLVE_TYPE_AMBIGUOUS;
            }
            type = type->data.union_type.members[0];
            break;
        default:
            return CBM_RESOLVE_TYPE_UNSUPPORTED;
        }
    }
    return CBM_RESOLVE_TYPE_LIMITED;
}

static CBMScope *binding_scope(CBMResolveTypeMap *map, CBMScope *scope, const char *name) {
    for (; scope; scope = scope->parent) {
        if (!type_step(map)) {
            return NULL;
        }
        if (cbm_scope_lookup_local(scope, name)) {
            return scope;
        }
    }
    return NULL;
}

static bool record_fact(CBMResolveTypeMap *map, CBMScope *scope, const char *name, uint32_t at,
                        const CBMType *type, cbm_resolve_type_origin_t origin,
                        const cbm_resolve_result_t *evidence, cbm_resolve_type_status_t status,
                        bool write_binding) {
    if (!type_step(map) || !scope || !name || !*name || !grow_slots(map)) {
        return false;
    }
    if (write_binding &&
        !cbm_scope_bind_checked(scope, name,
                                status == CBM_RESOLVE_TYPE_KNOWN ? type : cbm_type_unknown())) {
        map->failure = CBM_RESOLVE_TYPE_FAILED;
        return false;
    }
    const CBMVarBinding *binding = cbm_scope_lookup_local(scope, name);
    if (!binding) {
        return false;
    }
    cbm_resolve_type_slot_t *slot = find_slot(map, binding);
    if (!slot) {
        return false;
    }
    uint32_t version = 1;
    uint32_t declared_at = at;
    if (slot->fact.binding) {
        if (at < slot->fact.valid_from || slot->fact.version == UINT32_MAX) {
            map->failure = CBM_RESOLVE_TYPE_STALE;
            return false;
        }
        version = slot->fact.version + 1;
        declared_at = slot->fact.declared_at;
    } else {
        map->count++;
    }
    slot->fact = (cbm_resolve_type_fact_t){
        .type = status == CBM_RESOLVE_TYPE_KNOWN ? type : cbm_type_unknown(),
        .binding = binding,
        .scope = scope,
        .version = version,
        .declared_at = declared_at,
        .valid_from = at,
        .status = status,
        .origin = origin};
    slot->observed_type = binding->type;
    if (evidence && status == CBM_RESOLVE_TYPE_KNOWN) {
        slot->fact.evidence = *evidence;
    }
    return true;
}

static cbm_resolve_type_status_t fact_status(CBMResolveTypeMap *map, const CBMType *type,
                                             cbm_resolve_type_origin_t origin,
                                             const cbm_resolve_result_t *evidence) {
    if (origin <= CBM_RESOLVE_TYPE_NO_EVIDENCE || origin > CBM_RESOLVE_TYPE_OBSERVED_BINDING ||
        !cbm_resolve_admit(evidence) || !(evidence->facts & CBM_RESOLVE_FACT_TYPE_BOUND)) {
        return CBM_RESOLVE_TYPE_UNKNOWN;
    }
    return validate_type(map, type);
}

bool cbm_resolve_types_declare(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                               uint32_t at, const CBMType *type, cbm_resolve_type_origin_t origin,
                               const cbm_resolve_result_t *evidence) {
    cbm_resolve_type_status_t status = fact_status(map, type, origin, evidence);
    return record_fact(map, scope, name, at, type, origin, evidence, status, true);
}

bool cbm_resolve_types_assign(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                              uint32_t at, const CBMType *type, cbm_resolve_type_origin_t origin,
                              const cbm_resolve_result_t *evidence) {
    CBMScope *owner = name ? binding_scope(map, scope, name) : NULL;
    return owner && cbm_resolve_types_declare(map, owner, name, at, type, origin, evidence);
}

cbm_resolve_type_fact_t cbm_resolve_types_lookup(CBMResolveTypeMap *map, const CBMScope *scope,
                                                 const char *name, uint32_t at) {
    if (!type_step(map)) {
        return unknown_fact(map ? map->failure : CBM_RESOLVE_TYPE_FAILED);
    }
    CBMScope *owner = name ? binding_scope(map, (CBMScope *)scope, name) : NULL;
    if (!owner) {
        return unknown_fact(map->failure);
    }
    const CBMVarBinding *binding = cbm_scope_lookup_local(owner, name);
    cbm_resolve_type_slot_t *slot = find_slot(map, binding);
    if (!slot || !slot->fact.binding) {
        return unknown_fact(map->failure);
    }
    if (binding->type != slot->observed_type || at < slot->fact.valid_from) {
        return unknown_fact(CBM_RESOLVE_TYPE_STALE);
    }
    return slot->fact;
}

bool cbm_resolve_types_copy(CBMResolveTypeMap *map, CBMScope *destination_scope,
                            const char *destination, const CBMScope *source_scope,
                            const char *source, uint32_t at, bool declaration) {
    cbm_resolve_type_fact_t fact = cbm_resolve_types_lookup(map, source_scope, source, at);
    CBMScope *owner =
        declaration ? destination_scope : binding_scope(map, destination_scope, destination);
    return owner && record_fact(map, owner, destination, at, fact.type, CBM_RESOLVE_TYPE_ASSIGNMENT,
                                &fact.evidence, fact.status, true);
}

cbm_resolve_type_fact_t cbm_resolve_types_join(CBMResolveTypeMap *map,
                                               const cbm_resolve_type_fact_t *arms, size_t count,
                                               uint32_t at) {
    if (!type_step(map)) {
        return unknown_fact(map ? map->failure : CBM_RESOLVE_TYPE_FAILED);
    }
    if (!arms || !count) {
        return unknown_fact(CBM_RESOLVE_TYPE_UNKNOWN);
    }
    for (size_t i = 0; i < count; i++) {
        if (!type_step(map)) {
            return unknown_fact(map->failure);
        }
        if (arms[i].status != CBM_RESOLVE_TYPE_KNOWN || arms[i].valid_from > at ||
            fact_status(map, arms[i].type, arms[i].origin, &arms[i].evidence) !=
                CBM_RESOLVE_TYPE_KNOWN) {
            return unknown_fact(
                map->failure != CBM_RESOLVE_TYPE_UNKNOWN ? map->failure : CBM_RESOLVE_TYPE_UNKNOWN);
        }
        if (arms[i].binding) {
            cbm_resolve_type_slot_t *slot = find_slot(map, arms[i].binding);
            if (!slot || slot->fact.version != arms[i].version ||
                arms[i].binding->type != arms[i].type) {
                return unknown_fact(CBM_RESOLVE_TYPE_STALE);
            }
        }
        if (!cbm_type_equal(arms[0].type, arms[i].type)) {
            return unknown_fact(CBM_RESOLVE_TYPE_AMBIGUOUS);
        }
    }
    cbm_resolve_type_fact_t result = arms[0];
    result.binding = NULL;
    result.scope = NULL;
    result.version = 0;
    result.valid_from = at;
    result.origin = CBM_RESOLVE_TYPE_JOIN;
    return result;
}

bool cbm_resolve_types_merge(CBMResolveTypeMap *map, CBMScope *scope, const char *name, uint32_t at,
                             const cbm_resolve_type_fact_t *arms, size_t count, bool declaration) {
    cbm_resolve_type_fact_t fact = cbm_resolve_types_join(map, arms, count, at);
    CBMScope *owner = declaration ? scope : binding_scope(map, scope, name);
    return owner && record_fact(map, owner, name, at, fact.type, CBM_RESOLVE_TYPE_JOIN,
                                &fact.evidence, fact.status, true);
}

bool cbm_resolve_types_invalidate(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                                  uint32_t at, cbm_resolve_type_status_t reason) {
    CBMScope *owner = name ? binding_scope(map, scope, name) : NULL;
    if (reason == CBM_RESOLVE_TYPE_KNOWN) {
        reason = CBM_RESOLVE_TYPE_UNKNOWN;
    }
    return owner && record_fact(map, owner, name, at, cbm_type_unknown(),
                                CBM_RESOLVE_TYPE_NO_EVIDENCE, NULL, reason, true);
}

void cbm_resolve_types_invalidate_visible(CBMResolveTypeMap *map, CBMScope *scope, uint32_t at,
                                          cbm_resolve_type_status_t reason) {
    /* Iterate the existing lexical bindings, including ones not yet donated to
     * the map. This prevents unsupported flow from resurrecting an outer fact. */
    for (CBMScope *frame = scope; frame; frame = frame->parent) {
        if (!type_step(map)) {
            return;
        }
        for (CBMScopeChunk *chunk = frame->chunks; chunk; chunk = chunk->next) {
            for (int i = 0; i < chunk->used; i++) {
                if (!cbm_resolve_types_invalidate(map, frame, chunk->bindings[i].name, at,
                                                  reason)) {
                    return;
                }
            }
        }
    }
}

bool cbm_resolve_types_observe(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                               uint32_t at, const CBMType *observed,
                               cbm_resolve_type_origin_t origin,
                               const cbm_resolve_result_t *evidence) {
    CBMScope *owner = name ? binding_scope(map, scope, name) : NULL;
    const CBMVarBinding *binding = owner ? cbm_scope_lookup_local(owner, name) : NULL;
    if (!binding || binding->type != observed) {
        return false;
    }
    cbm_resolve_type_status_t status = fact_status(map, observed, origin, evidence);
    return record_fact(map, owner, name, at, observed, origin, evidence, status, false);
}
