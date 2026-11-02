#include "resolve/stats.h"
#include "types.h"
#include "policy.h"
#include <limits.h>
#include <string.h>

struct cbm_resolve_type_witness {
    const CBMResolveTypeMap *map;
    const void *generation;
    const CBMVarBinding *binding;
    const CBMType *observed_type;
    uint32_t version;
    cbm_resolve_type_status_t status;
};

struct cbm_resolve_type_slot {
    cbm_resolve_type_fact_t fact;
    const CBMType *observed_type; // source snapshot, including rejected richer types
    cbm_resolve_type_witness_t *witness;
    size_t binding_serial;
};

enum { TYPE_MAP_INITIAL_CAPACITY = 32, TYPE_MAP_DEFAULT_STEPS = 65536 };

static cbm_resolve_type_fact_t unknown_fact(cbm_resolve_type_status_t status) {
    cbm_resolve_type_fact_t fact = {0};
    fact.type = cbm_type_unknown();
    fact.status = status;
    return fact;
}

static cbm_resolve_type_status_t proof_failure(cbm_resolve_analysis_t state) {
    return state == CBM_RESOLVE_ANALYSIS_LIMITED    ? CBM_RESOLVE_TYPE_LIMITED
           : state == CBM_RESOLVE_ANALYSIS_COMPLETE ? CBM_RESOLVE_TYPE_KNOWN
                                                    : CBM_RESOLVE_TYPE_FAILED;
}

static bool type_step(CBMResolveTypeMap *map) {
    if (map && map->proofs.failure != CBM_RESOLVE_ANALYSIS_COMPLETE)
        map->failure = proof_failure(map->proofs.failure);
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
    cbm_resolve_proof_pool_init(&map->proofs, arena, 0, 0, 0);
    /* A fresh arena token avoids reading uninitialized map storage on first
     * init and distinguishes later reinitialization even if versions repeat. */
    map->generation = arena ? cbm_arena_alloc(arena, 1) : NULL;
    if (!map->generation)
        map->failure = CBM_RESOLVE_TYPE_FAILED;
}

bool cbm_resolve_types_set_context(CBMResolveTypeMap *map,
                                   const cbm_resolve_proof_source_t *source) {
    if (!map || !source || !map->arena || map->count || map->proofs.count)
        return false;
    cbm_resolve_proof_source_t owned = *source;
    const char **fields[] = {&owned.project,          &owned.file,
                             &owned.module,           &owned.scope,
                             &owned.content_identity, &owned.configuration_identity};
    for (size_t i = 0; i < sizeof(fields) / sizeof(fields[0]); i++) {
        if (*fields[i]) {
            *fields[i] = cbm_arena_strdup(map->arena, *fields[i]);
            if (!*fields[i]) {
                map->failure = CBM_RESOLVE_TYPE_FAILED;
                return false;
            }
        }
    }
    map->proof_source = owned;
    return true;
}

static cbm_resolve_type_witness_t *new_witness(CBMResolveTypeMap *map) {
    cbm_resolve_type_witness_t *witness = cbm_arena_calloc(map->arena, sizeof(*witness));
    if (!witness) {
        map->failure = CBM_RESOLVE_TYPE_FAILED;
        return NULL;
    }
    witness->map = map;
    witness->generation = map->generation;
    return witness;
}

cbm_resolve_type_status_t cbm_resolve_types_fact_status(const cbm_resolve_type_fact_t *fact) {
    if (!fact)
        return CBM_RESOLVE_TYPE_UNKNOWN;
    if (fact->status != CBM_RESOLVE_TYPE_KNOWN)
        return fact->status;
    const cbm_resolve_type_witness_t *witness = fact->witness;
    if (!witness)
        return fact->binding ? CBM_RESOLVE_TYPE_STALE : CBM_RESOLVE_TYPE_KNOWN;
    const CBMResolveTypeMap *map = witness->map;
    if (!map || !witness->generation || map->generation != witness->generation)
        return CBM_RESOLVE_TYPE_STALE;
    if (map->failure != CBM_RESOLVE_TYPE_UNKNOWN)
        return map->failure;
    if (map->proofs.failure != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return proof_failure(map->proofs.failure);
    if (witness->version != fact->version || witness->status != CBM_RESOLVE_TYPE_KNOWN ||
        (witness->binding && witness->binding->type != witness->observed_type) ||
        (fact->binding && fact->binding->type != fact->type))
        return CBM_RESOLVE_TYPE_STALE;
    return CBM_RESOLVE_TYPE_KNOWN;
}

static size_t binding_hash(const CBMVarBinding *binding) {
    uintptr_t value = (uintptr_t)binding >> 3;
    value ^= value >> 17;
    value *= (uintptr_t)0xed5ad4bbu;
    return (size_t)(value ^ (value >> 11));
}

static cbm_resolve_type_slot_t *find_slot(CBMResolveTypeMap *map, const CBMVarBinding *binding) {
    cbm_resolve_stats_add(CBM_RESOLVE_STAT_LOOKUP_PROBES, 1);
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

static const char *type_identity(CBMResolveTypeMap *map, const CBMType *type, unsigned depth) {
    if (!type || depth >= CBM_LSP_MAX_LOOKUP_DEPTH)
        return NULL;
    const char *name = NULL, *inner = NULL;
    switch (type->kind) {
    case CBM_TYPE_NAMED:
        name = type->data.named.qualified_name;
        break;
    case CBM_TYPE_BUILTIN:
        name = type->data.builtin.name;
        break;
    case CBM_TYPE_POINTER:
        inner = type_identity(map, type->data.pointer.elem, depth + 1);
        break;
    case CBM_TYPE_REFERENCE:
    case CBM_TYPE_RVALUE_REF:
        inner = type_identity(map, type->data.reference.elem, depth + 1);
        break;
    case CBM_TYPE_ALIAS:
        name = type->data.alias.alias_qn;
        inner = type_identity(map, type->data.alias.underlying, depth + 1);
        if (!inner)
            return NULL;
        break;
    case CBM_TYPE_UNION:
        if (type->data.union_type.count != 1 || !type->data.union_type.members)
            return NULL;
        inner = type_identity(map, type->data.union_type.members[0], depth + 1);
        break;
    default:
        return NULL;
    }
    if (!name && !inner)
        return NULL;
    return cbm_arena_sprintf(map->arena, "%u:%zu:%s:%zu:%s", (unsigned)type->kind,
                             name ? strlen(name) : 0, name ? name : "", inner ? strlen(inner) : 0,
                             inner ? inner : "");
}
static const char *type_target(const CBMType *type) {
    for (unsigned depth = 0; type && depth < CBM_LSP_MAX_LOOKUP_DEPTH; depth++) {
        switch (type->kind) {
        case CBM_TYPE_NAMED:
            return type->data.named.qualified_name;
        case CBM_TYPE_BUILTIN:
            return type->data.builtin.name;
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
            type = type->data.union_type.count == 1 && type->data.union_type.members
                       ? type->data.union_type.members[0]
                       : NULL;
            break;
        default:
            return NULL;
        }
    }
    return NULL;
}
static cbm_resolve_result_t type_transfer_result(const CBMType *type, const char *reason) {
    return (cbm_resolve_result_t){.binding = CBM_RESOLVE_BINDING_UNIQUE,
                                  .domain = CBM_RESOLVE_DOMAIN_PROJECT,
                                  .analysis = CBM_RESOLVE_ANALYSIS_COMPLETE,
                                  .rule = CBM_RESOLVE_RULE_DECLARED_TYPE,
                                  .facts = CBM_RESOLVE_FACT_EXACT_TARGET |
                                           CBM_RESOLVE_FACT_LOOKUP_COMPLETE |
                                           CBM_RESOLVE_FACT_TYPE_BOUND,
                                  .target = type_target(type),
                                  .candidate_count = 1,
                                  .reason = reason};
}
static const cbm_resolve_proof_t *type_record_proof(CBMResolveTypeMap *map, const char *name,
                                                    size_t binding_serial, uint32_t declared_at,
                                                    uint32_t at, const CBMType *type,
                                                    cbm_resolve_type_origin_t origin,
                                                    const cbm_resolve_result_t *evidence,
                                                    cbm_resolve_proof_kind_t operation) {
    const char *identity = type_identity(map, type, 0);
    const char *binding = cbm_arena_sprintf(map->arena, "local:%zu:%zu:%s:%u:%u", binding_serial,
                                            strlen(name), name, declared_at, at);
    if (!identity || !binding || !evidence) {
        map->failure = CBM_RESOLVE_TYPE_FAILED;
        return NULL;
    }
    cbm_resolve_proof_key_t key = {.lookup_domain = "lexical-type-binding",
                                   .lookup_key = name,
                                   .binding_identity = binding,
                                   .type_identity = identity,
                                   .start_byte = at,
                                   .end_byte = at,
                                   .origin = (uint32_t)origin};
    const cbm_resolve_proof_t *inputs[2];
    size_t count = 0;
    if (evidence->proof)
        inputs[count++] = evidence->proof;
    /* COPY/JOIN consume an already settled type value. Initial annotations,
     * constructors, returns and hybrid observations are explicit adapter
     * donations of this resolved type; an existing symbol proof alone must
     * not pretend to contain the adapter's type transformation. */
    if (operation != CBM_PROOF_TYPE_COPY && origin != CBM_RESOLVE_TYPE_JOIN) {
        cbm_resolve_result_t donation = *evidence;
        donation.proof = NULL;
        inputs[count++] = cbm_resolve_proof_leaf(&map->proofs, CBM_PROOF_ADAPTER_TYPE,
                                                 &map->proof_source, &key, &donation);
    }
    if (!count)
        return NULL;
    for (size_t i = 0; i < count; i++) {
        const cbm_resolve_proof_view_t *view = cbm_resolve_proof_view(inputs[i]);
        if (!view || view->outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
            return NULL;
    }
    cbm_resolve_result_t outcome = type_transfer_result(type, "closed_type_binding");
    return cbm_resolve_proof_derive(&map->proofs, operation, &map->proof_source, &key, &outcome,
                                    inputs, count, true);
}

static bool record_fact(CBMResolveTypeMap *map, CBMScope *scope, const char *name, uint32_t at,
                        const CBMType *type, cbm_resolve_type_origin_t origin,
                        const cbm_resolve_result_t *evidence, cbm_resolve_type_status_t status,
                        bool write_binding, cbm_resolve_proof_kind_t operation) {
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
        slot->witness = new_witness(map);
        if (!slot->witness)
            return false;
        slot->binding_serial = ++map->count;
    }
    const cbm_resolve_proof_t *proof = NULL;
    if (status == CBM_RESOLVE_TYPE_KNOWN) {
        proof = type_record_proof(map, name, slot->binding_serial, declared_at, at, type, origin,
                                  evidence, operation);
        if (!proof) {
            map->failure = map->proofs.failure == CBM_RESOLVE_ANALYSIS_COMPLETE
                               ? CBM_RESOLVE_TYPE_FAILED
                               : proof_failure(map->proofs.failure);
            return false;
        }
    }
    if (slot->fact.binding)
        cbm_resolve_stats_add(CBM_RESOLVE_STAT_INVALIDATIONS, 1);
    slot->witness->binding = binding;
    slot->witness->observed_type = binding->type;
    slot->witness->version = version;
    slot->witness->status = status;
    slot->fact = (cbm_resolve_type_fact_t){
        .type = status == CBM_RESOLVE_TYPE_KNOWN ? type : cbm_type_unknown(),
        .binding = binding,
        .scope = scope,
        .version = version,
        .declared_at = declared_at,
        .valid_from = at,
        .status = status,
        .origin = origin,
        .witness = slot->witness,
        .proof_pool = &map->proofs};
    slot->observed_type = binding->type;
    if (evidence && status == CBM_RESOLVE_TYPE_KNOWN) {
        slot->fact.evidence = type_transfer_result(type, "closed_type_binding");
        slot->fact.evidence.proof = proof;
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
    if (evidence->proof && evidence->proof->value.outcome.analysis != CBM_RESOLVE_ANALYSIS_COMPLETE)
        return CBM_RESOLVE_TYPE_UNKNOWN;
    return validate_type(map, type);
}

bool cbm_resolve_types_declare(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                               uint32_t at, const CBMType *type, cbm_resolve_type_origin_t origin,
                               const cbm_resolve_result_t *evidence) {
    cbm_resolve_type_status_t status = fact_status(map, type, origin, evidence);
    return record_fact(map, scope, name, at, type, origin, evidence, status, true,
                       CBM_PROOF_TYPE_BIND);
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
                                &fact.evidence, fact.status, true, CBM_PROOF_TYPE_COPY);
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
        if (arms[i].status == CBM_RESOLVE_TYPE_KNOWN) {
            cbm_resolve_type_status_t current = cbm_resolve_types_fact_status(&arms[i]);
            if (current != CBM_RESOLVE_TYPE_KNOWN)
                return unknown_fact(current);
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
    if (count > map->proofs.premise_limit ||
        count > SIZE_MAX / sizeof(const cbm_resolve_proof_t *)) {
        map->failure = CBM_RESOLVE_TYPE_LIMITED;
        return unknown_fact(map->failure);
    }
    const cbm_resolve_proof_t **inputs = cbm_arena_alloc(map->arena, count * sizeof(*inputs));
    const char *identity = type_identity(map, arms[0].type, 0);
    if (!inputs || !identity) {
        map->failure = CBM_RESOLVE_TYPE_FAILED;
        return unknown_fact(map->failure);
    }
    for (size_t i = 0; i < count; i++) {
        inputs[i] = arms[i].evidence.proof;
        if (!inputs[i])
            return unknown_fact(CBM_RESOLVE_TYPE_UNKNOWN);
    }
    cbm_resolve_type_fact_t result = {.type = arms[0].type,
                                      .status = CBM_RESOLVE_TYPE_KNOWN,
                                      .declared_at = at,
                                      .valid_from = at,
                                      .origin = CBM_RESOLVE_TYPE_JOIN,
                                      .proof_pool = &map->proofs};
    result.evidence = type_transfer_result(result.type, "closed_type_join");
    cbm_resolve_proof_key_t key = {.lookup_domain = "closed-type-join",
                                   .lookup_key = identity,
                                   .type_identity = identity,
                                   .start_byte = at,
                                   .end_byte = at,
                                   .origin = CBM_RESOLVE_TYPE_JOIN};
    result.evidence.proof =
        cbm_resolve_proof_derive(&map->proofs, CBM_PROOF_TYPE_JOIN, &map->proof_source, &key,
                                 &result.evidence, inputs, count, true);
    if (!result.evidence.proof) {
        map->failure = proof_failure(map->proofs.failure);
        return unknown_fact(map->failure);
    }
    cbm_resolve_type_witness_t *witness = new_witness(map);
    if (!witness)
        return unknown_fact(map->failure);
    witness->status = CBM_RESOLVE_TYPE_KNOWN;
    result.witness = witness;
    return result;
}

bool cbm_resolve_types_merge(CBMResolveTypeMap *map, CBMScope *scope, const char *name, uint32_t at,
                             const cbm_resolve_type_fact_t *arms, size_t count, bool declaration) {
    cbm_resolve_type_fact_t fact = cbm_resolve_types_join(map, arms, count, at);
    CBMScope *owner = declaration ? scope : binding_scope(map, scope, name);
    return owner && record_fact(map, owner, name, at, fact.type, CBM_RESOLVE_TYPE_JOIN,
                                &fact.evidence, fact.status, true, CBM_PROOF_TYPE_BIND);
}

bool cbm_resolve_types_invalidate(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                                  uint32_t at, cbm_resolve_type_status_t reason) {
    CBMScope *owner = name ? binding_scope(map, scope, name) : NULL;
    if (reason == CBM_RESOLVE_TYPE_KNOWN) {
        reason = CBM_RESOLVE_TYPE_UNKNOWN;
    }
    return owner &&
           record_fact(map, owner, name, at, cbm_type_unknown(), CBM_RESOLVE_TYPE_NO_EVIDENCE, NULL,
                       reason, true, CBM_PROOF_TYPE_BIND);
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
    return record_fact(map, owner, name, at, observed, origin, evidence, status, false,
                       CBM_PROOF_TYPE_BIND);
}
