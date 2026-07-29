/*
 * mem_profile.h — Which allocation sites retain memory?
 *
 * The census answers "which POOL holds the memory"; it cannot answer "which
 * allocation put it there". Without that, every hypothesis about #581 needed a
 * code change plus a soak to test, so the investigation could only ask one
 * yes/no question per eight minutes — and two mechanisms that looked confirmed
 * were later falsified. This records the missing half.
 *
 * Only allocations at or above a size threshold are tracked (default 32 KiB).
 * That is deliberate: arena fragmentation is driven by large transient runs,
 * and tracking every small allocation would cost more than the signal is worth.
 * The threshold is reported alongside the data so the blind spot is explicit.
 *
 * Off unless CBM_MEM_PROFILE=1. All tables are fixed-capacity and allocated
 * once; when a table fills, the overflow is COUNTED and reported rather than
 * silently dropped, because a profiler that quietly loses records produces
 * exactly the false confidence this is meant to end.
 */
#ifndef CBM_MEM_PROFILE_H
#define CBM_MEM_PROFILE_H

#include <stdbool.h>
#include <stddef.h>

/* Gate: CBM_MEM_PROFILE=1. Read once, cached. */
bool cbm_mem_profile_enabled(void);

/* Minimum allocation size recorded, from CBM_MEM_PROFILE_MIN (bytes). */
size_t cbm_mem_profile_threshold(void);

/* Record an allocation / release. Safe to call unconditionally: both return
 * immediately when profiling is off or the size is below the threshold. */
void cbm_mem_profile_alloc(void *block, size_t size);

/* Same, but with the caller's return address supplied by the hook itself.
 * Required on Windows ARM64, where CaptureStackBackTrace returns nothing: a
 * return address captured inside the capture helper names the helper, so every
 * allocation collapses onto a handful of hook addresses and the attribution
 * column becomes useless. Each hook must evaluate __builtin_return_address(0)
 * in its OWN frame and pass it here. */
void cbm_mem_profile_alloc_at(void *block, size_t size, void *caller);
void cbm_mem_profile_free(void *block);

typedef struct {
    size_t sites;      /* distinct call sites seen */
    size_t live_bytes; /* currently retained, attributed */
    size_t live_blocks;
    size_t total_bytes;        /* cumulative allocated at/above threshold */
    size_t untracked_frees;    /* frees whose pointer was not in the table */
    size_t site_table_full;    /* records lost: site table exhausted */
    size_t pointer_table_full; /* records lost: pointer table exhausted */
    /* Stack capture returned nothing. On Windows ARM64 CaptureStackBackTrace
     * can legitimately fail, and dropping those samples silently makes a
     * working profiler look like an empty one. */
    size_t capture_failed;
} cbm_mem_profile_totals_t;

void cbm_mem_profile_totals(cbm_mem_profile_totals_t *out);

/* Append one JSON object per site to `path`, biggest live first, with the raw
 * return addresses of each site's stack. Symbolisation is deliberately left to
 * the offline analyser so the hot path never loads a symbol handler. */
bool cbm_mem_profile_dump(const char *path, const char *label);

#endif /* CBM_MEM_PROFILE_H */
