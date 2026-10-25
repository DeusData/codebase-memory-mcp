#ifndef CBM_RESOLVE_SOLVER_H
#define CBM_RESOLVE_SOLVER_H

#include "proof.h"

typedef enum {
    CBM_SOLVER_PENDING = 0,
    CBM_SOLVER_KNOWN,
    CBM_SOLVER_UNKNOWN,
    CBM_SOLVER_AMBIGUOUS,
    CBM_SOLVER_UNSUPPORTED,
    CBM_SOLVER_LIMITED,
    CBM_SOLVER_FAILED
} cbm_resolve_solver_status_t;

typedef struct {
    const char *target;
    uint32_t kind;
    bool module;
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
