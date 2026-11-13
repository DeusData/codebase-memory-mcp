/*
 * resolve_stats.c — deterministic work counters for growth tests (see the header).
 */
#include "resolve_stats.h"

#ifdef CBM_ENABLE_TEST_SEAMS
#include <stddef.h>

static _Thread_local cbm_resolve_stats_t *current_stats;

void cbm_resolve_stats_reset(cbm_resolve_stats_t *stats) {
    if (stats) {
        for (unsigned i = 0; i < CBM_RESOLVE_STAT_COUNT; i++) {
            atomic_init(&stats->values[i], 0);
        }
    }
}

cbm_resolve_stats_t *cbm_resolve_stats_bind(cbm_resolve_stats_t *stats) {
    cbm_resolve_stats_t *previous = current_stats;
    current_stats = stats;
    return previous;
}

void cbm_resolve_stats_add(cbm_resolve_stat_t event, uint64_t amount) {
    if (current_stats && (unsigned)event < CBM_RESOLVE_STAT_COUNT) {
        atomic_fetch_add_explicit(&current_stats->values[event], amount, memory_order_relaxed);
    }
}

cbm_resolve_stats_snapshot_t cbm_resolve_stats_snapshot(const cbm_resolve_stats_t *stats) {
    cbm_resolve_stats_snapshot_t result = {0};
    if (stats) {
        for (unsigned i = 0; i < CBM_RESOLVE_STAT_COUNT; i++) {
            result.values[i] = atomic_load_explicit(&stats->values[i], memory_order_relaxed);
        }
    }
    return result;
}

const char *cbm_resolve_stat_name(cbm_resolve_stat_t event) {
    static const char *const names[] = {"lookup_probes", "syntax_child_steps"};
    _Static_assert(sizeof(names) / sizeof(names[0]) == CBM_RESOLVE_STAT_COUNT,
                   "one name per counter");
    return (unsigned)event < CBM_RESOLVE_STAT_COUNT ? names[event] : "unknown";
}
#endif
