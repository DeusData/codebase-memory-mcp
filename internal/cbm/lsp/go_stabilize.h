#ifndef CBM_LSP_GO_STABILIZE_H
#define CBM_LSP_GO_STABILIZE_H

#include "../cbm.h"
#include "../resolve/solver.h"
#include "../resolve/imports.h"

typedef struct cbm_go_stabilization cbm_go_stabilization_t;
/* Source normalization only: no graph edges/IDs, SQLite, inferred legacy types
 * or per-file borrowed storage enter the shared worklist. The original source
 * snapshot and exact configured import bindings are required. */
cbm_go_stabilization_t *cbm_go_stabilization_new(CBMArena *, cbm_resolve_proof_pool_t *,
                                                 cbm_resolve_solver_t *);
void cbm_go_stabilization_dispose(cbm_go_stabilization_t *);
bool cbm_go_stabilization_collect(cbm_go_stabilization_t *, const CBMFileResult *, const char *,
                                  size_t, const cbm_resolve_proof_source_t *, const char *,
                                  const cbm_resolve_import_snapshot_t *);
bool cbm_go_stabilization_register(cbm_go_stabilization_t *);
bool cbm_go_stabilization_seal(cbm_go_stabilization_t *);
/* true means this exact supported occurrence is controlled by the shared
 * fixed point, including uncertainty; callers must not fall back on failure.
 * Published results/proofs are immutable and safe for concurrent workers. */
bool cbm_go_stabilization_call(const cbm_go_stabilization_t *, const char *, const char *, uint32_t,
                               uint32_t, cbm_resolve_result_t *);
#endif
