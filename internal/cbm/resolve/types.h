#ifndef CBM_RESOLVE_TYPES_H
#define CBM_RESOLVE_TYPES_H

#include "model.h"
#include "proof.h"
#include "../lsp/scope.h"

/* Evidence on existing LSP types and lexical bindings. This is not a second
 * type representation or registry. Adapters retain ownership of language
 * lookup, declaration/assignment rules and control-flow traversal. All writes
 * to a participating binding must go through this map. */
typedef enum {
    CBM_RESOLVE_TYPE_UNKNOWN = 0,
    CBM_RESOLVE_TYPE_KNOWN,
    CBM_RESOLVE_TYPE_AMBIGUOUS,
    CBM_RESOLVE_TYPE_UNSUPPORTED,
    CBM_RESOLVE_TYPE_LIMITED,
    CBM_RESOLVE_TYPE_FAILED,
    CBM_RESOLVE_TYPE_STALE
} cbm_resolve_type_status_t;

typedef enum {
    CBM_RESOLVE_TYPE_NO_EVIDENCE = 0,
    CBM_RESOLVE_TYPE_ANNOTATION,
    CBM_RESOLVE_TYPE_CONSTRUCTOR,
    CBM_RESOLVE_TYPE_RETURN,
    CBM_RESOLVE_TYPE_ASSIGNMENT,
    CBM_RESOLVE_TYPE_JOIN,
    CBM_RESOLVE_TYPE_OBSERVED_BINDING
} cbm_resolve_type_origin_t;

typedef struct cbm_resolve_type_witness cbm_resolve_type_witness_t;

typedef struct {
    const CBMType *type;
    const CBMVarBinding *binding;
    const CBMScope *scope;
    uint32_t version;
    uint32_t declared_at;
    uint32_t valid_from;
    cbm_resolve_type_status_t status;
    cbm_resolve_type_origin_t origin;
    cbm_resolve_result_t evidence;
    /* Opaque revocation state survives checked receiver adaptations that
     * clear binding or replace type with its language-proved named owner. */
    const cbm_resolve_type_witness_t *witness;
    /* Live construction context only; never persisted with a proof node. */
    cbm_resolve_proof_pool_t *proof_pool;
} cbm_resolve_type_fact_t;

typedef struct cbm_resolve_type_slot cbm_resolve_type_slot_t;
typedef struct {
    CBMArena *arena;
    cbm_resolve_type_slot_t *slots;
    size_t capacity;
    size_t count;
    uint64_t steps;
    uint64_t step_limit;
    cbm_resolve_type_status_t failure;
    const void *generation;
    cbm_resolve_proof_pool_t proofs;
    cbm_resolve_proof_source_t proof_source;
} CBMResolveTypeMap;

/* Zero selects a deterministic default, not an unbounded walk. All borrowed
 * scopes, types and evidence strings must outlive the map. Per-file maps may
 * consult immutable shared registries without copying them. Facts borrow both
 * map storage and its arena: neither may be destroyed, and map storage may not
 * move, while facts are used. Reinitializing live map storage revokes every
 * fact from the old generation. */
void cbm_resolve_types_init(CBMResolveTypeMap *map, CBMArena *arena, uint64_t step_limit);

/* Supply identities captured with the extraction snapshot before the first
 * fact. Strings are copied into map->arena. Missing context stays explicitly
 * absent on ADAPTER_TYPE leaves; it never becomes a parsed/source-sealed fact.
 * Changing context after any fact requires reinitializing the map. */
bool cbm_resolve_types_set_context(CBMResolveTypeMap *map,
                                   const cbm_resolve_proof_source_t *source);

/* A known fact requires accepted binding evidence AND TYPE_BOUND: the adapter
 * must resolve the type as well as any constructor/function used to obtain it.
 * A spelling, capitalized call, unresolved annotation or naming convention is
 * not evidence. NULL/unknown evidence still records the shadow/reassignment.
 * The input must retain all uncertainty: never donate a type after a legacy
 * normalization that removed UNKNOWN branches. Use merge on the original
 * branch facts before calling any such normalizer. The CBMType alone cannot
 * recover provenance that its producer already erased. */
bool cbm_resolve_types_declare(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                               uint32_t at, const CBMType *type, cbm_resolve_type_origin_t origin,
                               const cbm_resolve_result_t *evidence);
bool cbm_resolve_types_assign(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                              uint32_t at, const CBMType *type, cbm_resolve_type_origin_t origin,
                              const cbm_resolve_result_t *evidence);
/* Where the current fact of this exact binding in this exact scope starts, 0 with none. */
uint32_t cbm_resolve_types_valid_from(CBMResolveTypeMap *map, const CBMScope *scope,
                                      const char *name);
cbm_resolve_type_fact_t cbm_resolve_types_lookup(CBMResolveTypeMap *map, const CBMScope *scope,
                                                 const char *name, uint32_t at);

/* O(1), read-only currentness check; does not establish binding/type evidence.
 * Map-backed facts retain their witness across adaptations and are stale after
 * any replacement/revocation of that binding. Map failures also revoke them.
 * Unbound immutable declaration/construction facts may have no witness; a
 * lexical binding without a witness cannot claim freshness. A join produces
 * a value snapshot, independent of subsequent writes to its input variables;
 * its witness still observes map failure/reinitialization. */
cbm_resolve_type_status_t cbm_resolve_types_fact_status(const cbm_resolve_type_fact_t *fact);

/* Straight-line propagation only. A control-flow adapter must merge all arms
 * with join, including unknown/unreachable-as-yet arms. No guessed arm wins. */
bool cbm_resolve_types_copy(CBMResolveTypeMap *map, CBMScope *destination_scope,
                            const char *destination, const CBMScope *source_scope,
                            const char *source, uint32_t at, bool declaration);
cbm_resolve_type_fact_t cbm_resolve_types_join(CBMResolveTypeMap *map,
                                               const cbm_resolve_type_fact_t *arms, size_t count,
                                               uint32_t at);
/* Merge the original branch facts and record the destination in one operation.
 * UNKNOWN arms therefore cannot disappear through an intermediate legacy
 * cbm_type_union call. Unsupported/ambiguous joins invalidate the destination. */
bool cbm_resolve_types_merge(CBMResolveTypeMap *map, CBMScope *scope, const char *name, uint32_t at,
                             const cbm_resolve_type_fact_t *arms, size_t count, bool declaration);

/* Call for an unknown write, alias write, captured mutation or unsupported
 * flow. invalidate_visible is the conservative barrier when the affected
 * names cannot be established. Both clear callable identity in CBMScope too. */
bool cbm_resolve_types_invalidate(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                                  uint32_t at, cbm_resolve_type_status_t reason);
void cbm_resolve_types_invalidate_visible(CBMResolveTypeMap *map, CBMScope *scope, uint32_t at,
                                          cbm_resolve_type_status_t reason);

/* Bridge for existing hybrid scope clients. It verifies the observed type is
 * still the nearest binding, then records its proof without inventing a type.
 * Never changes the existing CBMScope binding, including on rejected/unknown
 * donations. The adapter must subsequently route writes/barriers through this
 * map, or immediately re-observe the current binding before each consumption.
 * OBSERVED_BINDING describes a hybrid engine's exact current type without
 * asserting whether its original source was an annotation or constructor. */
bool cbm_resolve_types_observe(CBMResolveTypeMap *map, CBMScope *scope, const char *name,
                               uint32_t at, const CBMType *observed,
                               cbm_resolve_type_origin_t origin,
                               const cbm_resolve_result_t *evidence);

#endif
