#ifndef CBM_RESOLVE_SOLVER_H
#define CBM_RESOLVE_SOLVER_H

#include "proof.h"
#include "../lsp/type_rep.h"

typedef enum {
    CBM_SOLVER_PENDING = 0,
    CBM_SOLVER_KNOWN,
    CBM_SOLVER_UNKNOWN,
    CBM_SOLVER_AMBIGUOUS,
    CBM_SOLVER_UNSUPPORTED,
    CBM_SOLVER_LIMITED,
    CBM_SOLVER_FAILED
} cbm_resolve_solver_status_t;

typedef enum {
    CBM_SOLVER_FACT_BINDING = 0,
    CBM_SOLVER_FACT_TYPE,
    CBM_SOLVER_FACT_RETURN,
    CBM_SOLVER_FACT_MEMBER
} cbm_resolve_solver_fact_kind_t;

typedef struct {
    const char *target;
    uint32_t kind;
    bool module;
    cbm_resolve_solver_fact_kind_t fact_kind;
    const CBMType *type;
    /* Output: canonical identity is derived from type, never accepted as input. */
    const char *type_identity;
    const char *declaration_identity;
    uint32_t binding_version;
} cbm_resolve_solver_value_t;

typedef struct {
    size_t nodes;
    size_t edges;
    uint64_t steps;
} cbm_resolve_solver_limits_t;

typedef struct {
    const char *key;
    cbm_resolve_solver_status_t status;
    cbm_resolve_solver_value_t value;
    const cbm_resolve_proof_t *proof;
    const char *const *dependencies;
    size_t dependency_count;
    bool predecessors_closed;
} cbm_resolve_solver_result_t;

typedef struct cbm_resolve_solver cbm_resolve_solver_t;

/* A finite, flat information lattice: PENDING is the only unsettled state.
 * Each immutable rule transitions once to KNOWN(value) or a terminal explicit
 * uncertainty. A copy waits for its predecessor; a join waits for ALL of its
 * declared predecessors and is known only when every value agrees, including
 * kind and module identity. UNKNOWN/UNSUPPORTED arms never disappear. Cycles
 * without an independently proved leaf remain UNKNOWN with an open proof;
 * even a known external join arm cannot discharge a cyclic predecessor.
 *
 * Registration then solve is single-threaded. Published results/proofs are
 * immutable. Keys, values and dependencies are copied into arena; audit roots
 * are cloned into pool. Both must outlive the solver. Zero limits use finite
 * defaults. Duplicate keys fail the whole solve closed, never last-wins.
 * Node/edge/step exhaustion makes EVERY result LIMITED, independent of input
 * order. Steps count node settlement and dependency indexing/inspection/
 * notification; stable-key sorting and indexed lookup do not spend semantic
 * budget. There are no corpus-wide repeated convergence passes. */
cbm_resolve_solver_t *cbm_resolve_solver_new(CBMArena *arena, cbm_resolve_proof_pool_t *pool,
                                             cbm_resolve_solver_limits_t limits);
bool cbm_resolve_solver_leaf(cbm_resolve_solver_t *solver, const char *key,
                             cbm_resolve_solver_status_t status, cbm_resolve_solver_value_t value,
                             const cbm_resolve_proof_t *audit);
bool cbm_resolve_solver_copy(cbm_resolve_solver_t *solver, const char *key, const char *dependency,
                             const cbm_resolve_proof_t *audit);
bool cbm_resolve_solver_join(cbm_resolve_solver_t *solver, const char *key,
                             const char *const *dependencies, size_t count,
                             const cbm_resolve_proof_t *audit);
/* SELECT waits for selector, then subscribes to prefix + its exact target + suffix.
 * This one additional dependency is reserved at registration. It is never read
 * while pending. Missing selected keys, cycles and terminal uncertainty abstain.
 * A type-member adapter must first establish selector's structured type. */
bool cbm_resolve_solver_select(cbm_resolve_solver_t *solver, const char *key, const char *selector,
                               const char *prefix, const char *suffix,
                               const cbm_resolve_proof_t *audit);
/* The same dependency selection keyed by the complete structured type identity,
 * rather than only its named declaration. Adapters supply exact applicability
 * domains (for example addressable vs temporary receivers). */
bool cbm_resolve_solver_select_type(cbm_resolve_solver_t *solver, const char *key,
                                    const char *selector, const char *prefix, const char *suffix,
                                    const cbm_resolve_proof_t *audit);
const char *cbm_resolve_solver_type_identity(CBMArena *, const CBMType *);
/* A finite transfer over a proved type. UNKNOWN is an explicit unsupported
 * operation; no arbitrary inference callback can smuggle unsettled facts in.
 * NAMED copies an already resolved type (including its wrappers), POINTER and
 * SLICE add one wrapper. Immutable lexical writes carry distinct keys/versions. */
bool cbm_resolve_solver_type(cbm_resolve_solver_t *solver, const char *key, const char *dependency,
                             CBMTypeKind wrapper, uint32_t version,
                             const cbm_resolve_proof_t *audit);
bool cbm_resolve_solver_solve(cbm_resolve_solver_t *solver);

/* No named fact is implicitly known. Before solve all lookups are PENDING.
 * After solve an absent key is UNKNOWN; resource/failure states override every
 * lookup. Summary rows are sorted by full key and expose actual terminal
 * binding/type/module/completeness, not a hash of occurrence counts. A global
 * failure publishes no partial summary rows; get still returns its failure
 * state for every key. complete reports evaluation completion; individual
 * cyclic rows retain predecessors_closed=false. */
void cbm_resolve_solver_get(const cbm_resolve_solver_t *solver, const char *key,
                            cbm_resolve_solver_result_t *out);
size_t cbm_resolve_solver_count(const cbm_resolve_solver_t *solver);
const cbm_resolve_solver_result_t *cbm_resolve_solver_at(const cbm_resolve_solver_t *solver,
                                                         size_t index);
uint64_t cbm_resolve_solver_steps(const cbm_resolve_solver_t *solver);
bool cbm_resolve_solver_complete(const cbm_resolve_solver_t *solver);

#endif
