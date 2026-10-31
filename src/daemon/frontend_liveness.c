/*
 * frontend_liveness.c — deterministic deadline policy for the frontend
 * waiters. No clocks, no atomics, no frontend state: callers pass a sampled
 * `now`, the reconnect counter and attempt start, and whether a replacement is
 * running.
 */
#include "daemon/frontend_liveness.h"

static uint64_t frontend_liveness_deadline(uint64_t start, uint64_t window_ms) {
    return start > UINT64_MAX - window_ms ? UINT64_MAX : start + window_ms;
}

static uint64_t frontend_liveness_anchor(uint64_t now, uint64_t attempt_started_ms) {
    /* A start after `now` would mean the sampler saw the attempt before its
     * timestamp; prefer the local observation so the deadline cannot move
     * backwards. */
    return attempt_started_ms != 0 && attempt_started_ms <= now ? attempt_started_ms : now;
}

void cbm_daemon_frontend_liveness_init(cbm_daemon_frontend_liveness_t *liveness, uint64_t now,
                                       unsigned int attempts) {
    liveness->last_progress_ms = now;
    liveness->reconnect_since_ms = 0;
    liveness->seen_attempts = attempts;
    liveness->was_reconnecting = false;
}

bool cbm_daemon_frontend_liveness_wait(cbm_daemon_frontend_liveness_t *liveness, uint64_t now,
                                       unsigned int attempts, bool reconnecting,
                                       uint64_t attempt_started_ms, uint64_t base_window_ms) {
    if (attempts != liveness->seen_attempts) {
        liveness->seen_attempts = attempts;
        liveness->last_progress_ms = now;
        liveness->reconnect_since_ms = 0;
        liveness->was_reconnecting = false;
    }
    if (reconnecting) {
        if (!liveness->was_reconnecting) {
            liveness->reconnect_since_ms = frontend_liveness_anchor(now, attempt_started_ms);
        }
    } else if (liveness->was_reconnecting) {
        /* The attempt ended; the retried exchange gets its own stuck window
         * instead of inheriting the spawn's remainder. */
        liveness->last_progress_ms = now;
        liveness->reconnect_since_ms = 0;
    }
    liveness->was_reconnecting = reconnecting;

    uint64_t deadline = frontend_liveness_deadline(liveness->last_progress_ms, base_window_ms);
    if (reconnecting) {
        uint64_t reconnect_deadline = frontend_liveness_deadline(
            liveness->reconnect_since_ms, CBM_DAEMON_FRONTEND_RECONNECT_GRACE_MS);
        if (reconnect_deadline > deadline) {
            deadline = reconnect_deadline;
        }
    }
    return now < deadline;
}

void cbm_daemon_frontend_liveness_progress(cbm_daemon_frontend_liveness_t *liveness, uint64_t now) {
    liveness->last_progress_ms = now;
}

bool cbm_daemon_frontend_liveness_reconnect_late(cbm_daemon_frontend_liveness_t *liveness,
                                                 uint64_t now, uint64_t attempt_started_ms) {
    if (liveness->was_reconnecting) {
        /* This attempt already has its grace; re-anchoring here would let a
         * repeated re-sample extend a hung attempt without bound. */
        return false;
    }
    liveness->reconnect_since_ms = frontend_liveness_anchor(now, attempt_started_ms);
    liveness->was_reconnecting = true;
    return true;
}
