/*
 * diagnostics.h — Periodic diagnostics file writer.
 *
 * When CBM_DIAGNOSTICS=1, writes a snapshot and retained trajectory below a
 * fresh owner-private temporary directory every 5s. The start diagnostic
 * reports both randomized paths for soak-test and support tooling.
 */
#ifndef CBM_DIAGNOSTICS_H
#define CBM_DIAGNOSTICS_H

#include <stdbool.h>
#include <stddef.h>
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

/* Stop the writer within a bounded deadline and delete the live snapshot.
 * The trajectory remains for post-mortem support. */
void cbm_diag_stop(void);

#ifdef CBM_DIAGNOSTICS_ENABLE_TEST_API
/* Focused lifecycle/security seams; absent from production builds. */
bool cbm_diag_test_copy_paths(char *directory, size_t directory_size, char *snapshot,
                              size_t snapshot_size, char *trajectory, size_t trajectory_size);
void cbm_diag_test_hold_writer(bool hold);
bool cbm_diag_test_writer_reached(void);
bool cbm_diag_test_abandoned(void);
bool cbm_diag_test_reset_abandoned(void);
#endif

#endif /* CBM_DIAGNOSTICS_H */
