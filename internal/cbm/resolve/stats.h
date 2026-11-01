#ifndef CBM_RESOLVE_STATS_H
#define CBM_RESOLVE_STATS_H

#include <stdatomic.h>
#include <stdint.h>

/* Semantic work, never elapsed time or address-dependent hash placement.
 * LOOKUP_PROBES counts keyed probes/comparisons; CANDIDATE_VISITS counts
 * declarations actually examined; PROPAGATED_FACTS counts visits to facts
 * consumed by derivation, cloning and persistence (not unique facts);
 * INVALIDATIONS counts binding versions replaced;
 * STORED_RECORDS counts successful resolver metadata row writes;
 * SYNTAX_CHILD_STEPS counts the sibling steps taken to enumerate a node's named
 * children, the cost that was quadratic when each child was fetched by index. */
typedef enum {
    CBM_RESOLVE_STAT_LOOKUP_PROBES,
    CBM_RESOLVE_STAT_CANDIDATE_VISITS,
    CBM_RESOLVE_STAT_PROPAGATED_FACTS,
    CBM_RESOLVE_STAT_INVALIDATIONS,
    CBM_RESOLVE_STAT_STORED_RECORDS,
    CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS,
    CBM_RESOLVE_STAT_COUNT
} cbm_resolve_stat_t;

typedef struct {
    _Atomic uint64_t values[CBM_RESOLVE_STAT_COUNT];
} cbm_resolve_stats_t;
typedef struct {
    uint64_t values[CBM_RESOLVE_STAT_COUNT];
} cbm_resolve_stats_snapshot_t;

/* Owned by one indexing run. Reset only before attaching workers; snapshot
 * after they join. Concurrent indices have distinct sinks. No allocation or
 * process-global counter is involved. Bind returns the previous thread-local
 * sink, which every caller must restore before leaving its scope. */
void cbm_resolve_stats_reset(cbm_resolve_stats_t *stats);
cbm_resolve_stats_t *cbm_resolve_stats_bind(cbm_resolve_stats_t *stats);
void cbm_resolve_stats_add(cbm_resolve_stat_t event, uint64_t amount);
cbm_resolve_stats_snapshot_t cbm_resolve_stats_snapshot(const cbm_resolve_stats_t *stats);
const char *cbm_resolve_stat_name(cbm_resolve_stat_t event);

#endif
