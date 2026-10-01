#ifndef CBM_ARCHITECTURE_PROJECTION_H
#define CBM_ARCHITECTURE_PROJECTION_H

#include "store/store.h"

typedef struct {
    /* Zero selects the bounded default. Larger requests are clamped. */
    int max_nodes;                  /* 100000; ceiling 250000 */
    int max_edges;                  /* 500000; ceiling 1000000 */
    int max_components;             /* 256; ceiling 512 */
    int max_dependencies;           /* 2048; ceiling 4096 */
    int max_paths;                  /* 12; ceiling 32 */
    int max_depth;                  /* 10; ceiling 16 */
    int max_targets;                /* 256; ceiling 1024 */
    int max_corridor_nodes;         /* 1024; ceiling 4096 */
    int max_corridor_edges;         /* 4096; ceiling 16384 */
    int64_t entry_node_id;          /* zero chooses indexed entry points */
    int64_t target_node_id;         /* optional bounded call corridor; requires entry */
    bool include_behavior_evidence; /* opt-in indexed declarations and bounded call arguments */
    uint64_t deadline_ms;           /* absolute cbm_now_ms(); zero = now + 5000 */
    cbm_store_cancel_fn cancel;
    void *cancel_context;
} cbm_architecture_projection_options_t;

/* On-demand, bounded, read-only projection of one consistent indexed graph.
 * Requires exclusive use of the store connection (prefer a dedicated worker
 * connection). Installs and removes its SQLite progress handler. No parsing,
 * indexing, database writes, or runtime execution observations are performed.
 * Returns malloc-owned JSON on OK, including explicit status="limited" when
 * graph/resource limits prevent a complete projection. Cancel returns
 * CBM_STORE_CANCELLED, unknown projects return CBM_STORE_NOT_FOUND.
 * Component membership is inferred; every dependency witness is an indexed
 * edge. CALL_REFERENCE is never treated as execution or traversed in paths.
 */
int cbm_store_architecture_projection(cbm_store_t *store, const char *project,
                                      const cbm_architecture_projection_options_t *options,
                                      char **out_json);

#endif
