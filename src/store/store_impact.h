/*
 * store_impact.h — fixpoint impact walk over the stored graph.
 *
 * "What depends on these nodes?" Starting from a set of seeds, the walk follows
 * the requested edge types BACKWARDS (`caller --CALLS--> callee`: a change to
 * the callee reaches the caller) until nothing new is reached.
 *
 * It exists next to cbm_store_bfs_multi because test selection needs three
 * things the depth-bounded recursive CTE cannot give:
 *   - a true fixpoint. Every node is expanded once, so a cycle terminates and
 *     the cost is O(nodes reached + edges probed); no depth or row cap decides
 *     the result;
 *   - a result that is a pure function of the graph and the request. Hits come
 *     back ordered by (hop, qualified name), and the parent recorded for a hit
 *     is, among the parents one hop closer, the one with the smallest (rank of
 *     the edge type in the request, qualified name). Node ids, row order and
 *     seed order never take part;
 *   - walk state that is sized when the walk opens. A refused allocation is an
 *     error of the request, never a shorter answer.
 */
#ifndef CBM_STORE_IMPACT_H
#define CBM_STORE_IMPACT_H

#include "store/store.h"

#include <stdbool.h>
#include <stdint.h>

typedef struct cbm_impact_walk cbm_impact_walk_t;

typedef struct {
    const char *project;
    /* Edge types to walk, each from its target to its sources. The order is
     * the rank used to pick the recorded parent. At least one. */
    const char *const *edge_types;
    int edge_type_count;
    /* Also cross HTTP/async/gRPC routes: a handler reaches the Route it
     * HANDLES, and a Route reaches whoever calls it over HTTP_CALLS,
     * ASYNC_CALLS or GRPC_CALLS. These rank after every requested type. */
    bool follow_routes;
    /* 0 walks to the fixpoint. A positive value stops expanding nodes that are
     * that many hops from the seeds of their run: the bounded neighbourhood,
     * with the same ordering and parents. */
    int max_hops;
} cbm_impact_policy_t;

typedef struct {
    int64_t id;
    int hop;                    /* 0 for a seed of the first run */
    int64_t via_id;             /* the parent this node was reached from; 0 for a seed */
    const char *via_edge;       /* edge type from the parent; NULL for a seed */
    const char *qualified_name; /* owned by the walk */
    const char *label;          /* owned by the walk */
} cbm_impact_hit_t;

/* Open a walk. The policy is copied. CBM_STORE_ERR on a missing store, project
 * or edge type, and when the walk state cannot be allocated. */
int cbm_impact_walk_open(cbm_store_t *s, const cbm_impact_policy_t *policy,
                         cbm_impact_walk_t **out);

/* Same walk within a pinned read scope, borrowed until close. All work,
 * including ordering, checks cancellation. The first walk error invalidates
 * this walk and its scope. Close the walk before closing the scope. */
int cbm_impact_walk_open_scoped(cbm_store_read_scope_t *scope, const cbm_impact_policy_t *policy,
                                cbm_impact_walk_t **out);

/* Nodes that are reached but never expanded: a test case that reaches the
 * change is selected, and what calls the test case is not impacted by it. May
 * be called before any run and between runs. An id the store does not hold is
 * CBM_STORE_ERR. */
int cbm_impact_walk_add_sinks(cbm_impact_walk_t *w, const int64_t *ids, int count);

/* Add seeds and walk to the fixpoint. A later call adds seeds to the state the
 * earlier ones left (a "lift"): they enter one hop beyond everything reached
 * so far, nodes already reached keep their hop and parent, and the walk goes
 * on to the new fixpoint. A seed the store does not hold is CBM_STORE_ERR: an
 * empty answer would read as "nothing depends on this". After an error the
 * walk can only be closed. */
int cbm_impact_walk_run(cbm_impact_walk_t *w, const int64_t *seeds, int count);

/* Everything reached, seeds included, ordered by (hop, qualified name). The
 * array is valid until the next operation or close. After any error or scope
 * failure all accessors hide every hit, including prior lifts; previously
 * borrowed hit pointers must not be used. Only close remains valid. */
int cbm_impact_walk_count(const cbm_impact_walk_t *w);
const cbm_impact_hit_t *cbm_impact_walk_hits(const cbm_impact_walk_t *w);
bool cbm_impact_walk_reached(const cbm_impact_walk_t *w, int64_t id);
/* The hit of one node, or NULL when the walk did not reach it. */
const cbm_impact_hit_t *cbm_impact_walk_hit(const cbm_impact_walk_t *w, int64_t id);

void cbm_impact_walk_close(cbm_impact_walk_t *w);

#endif /* CBM_STORE_IMPACT_H */
