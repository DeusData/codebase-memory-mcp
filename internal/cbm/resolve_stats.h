/*
 * resolve_stats.h — deterministic work counters for growth tests.
 *
 * Semantic work, never elapsed time or address-dependent hash placement, so a
 * test can double its input and assert the work about doubles (O9).
 * LOOKUP_PROBES counts keyed probes and comparisons; SYNTAX_CHILD_STEPS counts
 * the sibling steps taken to enumerate a node's children.
 *
 * Seam builds only (CBM_ENABLE_TEST_SEAMS), like lsp/lsp_work.h: nothing in a
 * product build reads these, so there cbm_resolve_stats_add compiles to nothing.
 */
#ifndef CBM_RESOLVE_STATS_H
#define CBM_RESOLVE_STATS_H

#include <stdint.h>

typedef enum {
    CBM_RESOLVE_STAT_LOOKUP_PROBES,
    CBM_RESOLVE_STAT_SYNTAX_CHILD_STEPS,
    CBM_RESOLVE_STAT_COUNT
} cbm_resolve_stat_t;

#ifdef CBM_ENABLE_TEST_SEAMS
#include <stdatomic.h>

typedef struct {
    _Atomic uint64_t values[CBM_RESOLVE_STAT_COUNT];
} cbm_resolve_stats_t;
typedef struct {
    uint64_t values[CBM_RESOLVE_STAT_COUNT];
} cbm_resolve_stats_snapshot_t;

/* A sink is owned by its caller. Reset it before binding; snapshot it after
 * every thread bound to it has unbound. Bind returns the thread's previous
 * sink, which the caller must restore before leaving its scope. Work done on a
 * thread with no sink bound is not counted. No allocation is involved. */
void cbm_resolve_stats_reset(cbm_resolve_stats_t *stats);
cbm_resolve_stats_t *cbm_resolve_stats_bind(cbm_resolve_stats_t *stats);
void cbm_resolve_stats_add(cbm_resolve_stat_t event, uint64_t amount);
cbm_resolve_stats_snapshot_t cbm_resolve_stats_snapshot(const cbm_resolve_stats_t *stats);
const char *cbm_resolve_stat_name(cbm_resolve_stat_t event);
#else
static inline void cbm_resolve_stats_add(cbm_resolve_stat_t event, uint64_t amount) {
    (void)event;
    (void)amount;
}
#endif

#endif /* CBM_RESOLVE_STATS_H */
