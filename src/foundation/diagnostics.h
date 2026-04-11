/*
 * diagnostics.h — Periodic diagnostics file writer.
 *
 * When CBM_DIAGNOSTICS=1, writes /tmp/cbm-diagnostics-<pid>.json every 5s.
 * Soak tests read this file to track memory, FDs, query stats over time.
 */
#ifndef CBM_DIAGNOSTICS_H
#define CBM_DIAGNOSTICS_H

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>

/* Global query stats — updated by the MCP server on each tool call. */
typedef struct {
    atomic_int count;     /* total tool calls */
    atomic_int errors;    /* tool calls that returned isError=true */
    atomic_llong time_us; /* cumulative wall-clock time (microseconds) */
    atomic_llong max_us;  /* max single call time (microseconds) */
} cbm_query_stats_t;

/* Singleton query stats — MCP server increments these. */
extern cbm_query_stats_t g_query_stats;

/* Record a completed tool call. */
void cbm_diag_record_query(long long duration_us, bool is_error);

/* Start the diagnostics writer thread (if CBM_DIAGNOSTICS env is set).
 * Call once from main(). Returns true if started. */
bool cbm_diag_start(void);

/* Stop the writer thread and delete the diagnostics file. */
void cbm_diag_stop(void);

#endif /* CBM_DIAGNOSTICS_H */
