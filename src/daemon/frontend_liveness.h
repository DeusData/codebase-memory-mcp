/*
 * frontend_liveness.h — no-progress policy shared by the frontend's waiters.
 *
 * Two threads wait for the worker with different ordinary windows: the stdin
 * reader's EOF drain (15 s) and the join watchdog (5 s). Both must treat a
 * replacement-daemon attempt as forward progress, because a cold daemon start
 * can outlast either window. Keeping that arithmetic in each loop is how the
 * two policies drifted once (#2796): the drain granted the spawn its grace
 * while the watchdog killed the same worker five seconds in. Each waiter
 * supplies its own ordinary window; the reconnect grace is shared here and
 * anchored to the attempt's real start, so two waiters observing the same
 * attempt spend one 120 s budget rather than two.
 */
#ifndef CBM_DAEMON_FRONTEND_LIVENESS_H
#define CBM_DAEMON_FRONTEND_LIVENESS_H

#include <stdbool.h>
#include <stdint.h>

/* One replacement spawn may take as long as a cold daemon start (connect,
 * bootstrap, and possibly an upgrade drain). This bounds a single attempt, not
 * the run: the reconnect budget bounds the run. */
#define CBM_DAEMON_FRONTEND_RECONNECT_GRACE_MS 120000U

typedef struct {
    uint64_t last_progress_ms;
    uint64_t reconnect_since_ms;
    unsigned int seen_attempts;
    bool was_reconnecting;
} cbm_daemon_frontend_liveness_t;

/* Start a window at `now`; `attempts` is the reconnect counter's current
 * value (a nonzero value when a waiter starts mid-attempt). */
void cbm_daemon_frontend_liveness_init(cbm_daemon_frontend_liveness_t *liveness, uint64_t now,
                                       unsigned int attempts);

/* Observe one poll and report whether the caller may keep waiting.
 * `base_window_ms` is the ordinary no-progress window. A reconnect in progress
 * extends the deadline to that attempt's grace. `attempt_started_ms` is the
 * attempt's real start (0 when the caller has no timestamp): anchoring to it
 * lets the drain and the watchdog share one budget for the same attempt.
 * A changed attempt counter (an attempt that started and ended between polls)
 * restarts the window, so a fast failure is not charged to the previous
 * attempt. */
bool cbm_daemon_frontend_liveness_wait(cbm_daemon_frontend_liveness_t *liveness, uint64_t now,
                                       unsigned int attempts, bool reconnecting,
                                       uint64_t attempt_started_ms, uint64_t base_window_ms);

/* A unit of work completed: restart the ordinary window. The per-attempt
 * reconnect grace is not extended. */
void cbm_daemon_frontend_liveness_progress(cbm_daemon_frontend_liveness_t *liveness, uint64_t now);

/* The deadline expired, but a re-sample showed a reconnect that started after
 * the poll: grant that attempt its grace instead of stopping underneath it.
 * Returns true when a grace was granted; repeated calls for the same attempt
 * are no-ops and return false, so the caller cannot extend a hung attempt by
 * re-sampling. */
bool cbm_daemon_frontend_liveness_reconnect_late(cbm_daemon_frontend_liveness_t *liveness,
                                                 uint64_t now, uint64_t attempt_started_ms);

#endif /* CBM_DAEMON_FRONTEND_LIVENESS_H */
