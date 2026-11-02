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
/* Before any collect: one file's declared package name, keyed by its directory module. An
 * unaliased import binds this name, which need not be the import path's last segment
 * (k8s.io/mount-utils declares `package mount`). */
bool cbm_go_stabilization_note_package(cbm_go_stabilization_t *, const char *module,
                                       const char *source, size_t length);
bool cbm_go_stabilization_collect(cbm_go_stabilization_t *, const CBMFileResult *, const char *,
                                  size_t, const cbm_resolve_proof_source_t *, const char *,
                                  const cbm_resolve_import_snapshot_t *);
bool cbm_go_stabilization_register(cbm_go_stabilization_t *);
bool cbm_go_stabilization_seal(cbm_go_stabilization_t *);
/* true means this exact call occurrence was analysed by the shared fixed point. An unknown
 * result is missing knowledge: a caller may keep a proof it holds independently, but never
 * replace an ambiguous result. Published results/proofs are immutable and safe for
 * concurrent workers. */
/* The reason of a call site that is a conversion, compared by identity. */
extern const char cbm_go_type_conversion[];
bool cbm_go_stabilization_call(const cbm_go_stabilization_t *, const char *, const char *, uint32_t,
                               uint32_t, cbm_resolve_result_t *);
/* A non-call reference token: a package-scope name, a qualified name or a selected member.
 * True only when Go's rules select a declaration or find two; the proof attests the
 * declaration, so the caller adds the site and role it read this token for. */
bool cbm_go_stabilization_reference(const cbm_go_stabilization_t *, const char *file,
                                    uint32_t start, uint32_t end, cbm_resolve_result_t *);
/* The project-level names worth keeping as derived bindings. Member, element and per-site
 * facts are reached from the occurrences they justify, and would otherwise be kept once per
 * name for every type in the project. */
bool cbm_go_stabilization_summary_key(const char *key);
#endif
