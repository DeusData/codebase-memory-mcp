/*
 * frontend.c — Stateless stdio bridge for a daemon-backed MCP session.
 */
#include "daemon/frontend.h"

#include "daemon/application.h"
#include "daemon/frontend_liveness.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"
#include "foundation/subprocess.h"
#include "mcp/mcp.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    FRONTEND_QUEUE_CAPACITY = 8,
    FRONTEND_QUEUE_BYTES_MAX = 12 * 1024 * 1024,
    FRONTEND_REQUEST_TIMEOUT_MS = 24 * 60 * 60 * 1000,
    FRONTEND_CLOSE_TIMEOUT_MS = 5000,
    FRONTEND_JOIN_WATCHDOG_MS = FRONTEND_CLOSE_TIMEOUT_MS,
    /* A regular-file/buffered client can write several complete requests and
     * close stdin before the worker is scheduled. Give already-accepted input
     * a short FIFO drain window so EOF cannot silently discard it. A genuinely
     * active long operation is still cancelled at this deadline. */
    FRONTEND_WAIT_US = 1000,
    /* An idle thin frontend owns no work and only needs to notice the next
     * queue item promptly. Ten milliseconds avoids a 1 kHz wake-up loop per
     * connected coding-agent session without perceptible request latency. */
    FRONTEND_IDLE_WAIT_US = 10 * 1000,
    /* EOF-drain progress window. Sized to cover a cold daemon spawn plus one
     * request round-trip on the slowest platform (several seconds on Windows
     * under CI load): the drain aborts only after this long with NO item
     * completing, so batch clients that close stdin after writing keep
     * receiving every response while a genuinely stuck request still gets
     * cancelled and the frontend exits cleanly. */
    FRONTEND_EOF_DRAIN_MS = 15000,
    /* #2796: replacement-daemon attempts per frontend run once the transport
     * fails mid-session. Bounded so a daemon that cannot come back still
     * terminates the process exactly like the pre-reconnect behavior. A
     * replacement spawn may legitimately outlast one drain window; the shared
     * per-attempt grace lives in frontend_liveness.h. */
    FRONTEND_DAEMON_RECONNECT_MAX = 3,
    FRONTEND_MAINTENANCE_POLL_MS = 10,
    /* Cadence for the quiet no-maintenance steady state. Every presence probe
     * try-acquires the maintenance marker lock, and on Windows that
     * revalidates the whole ancestor directory chain (a CreateFileW +
     * identity check per path component). At the old 10 ms cadence this
     * burned ~20% of a core for the life of every MCP session — 7762
     * CPU-seconds on one 9.5 h session in the 2026-08-29 incident. The
     * budget: an activation drains participants within
     * CLI_ACTIVATION_DRAIN_TIMEOUT_MS (15 s) and the monitor's own exit grace
     * is FRONTEND_MAINTENANCE_GRACE_MS (3 s), so a 250 ms detection latency
     * spends under 2% of that window while cutting the probe cost 25-fold.
     * The 10 ms cadence still paces the monitor's post-detection grace loop,
     * which does no lock probing. */
    FRONTEND_MAINTENANCE_IDLE_POLL_MS = 250,
    /* The owner thread may be draining a supervised process tree. Preserve the
     * supervisor's complete graceful + forced-settle window before the monitor
     * fail-stops the process, plus scheduling/teardown margin. */
    FRONTEND_MAINTENANCE_GRACE_MS =
        CBM_SUBPROCESS_MAX_CANCEL_GRACE_MS + CBM_SUBPROCESS_FORCE_SETTLE_MS + 1000,
    FRONTEND_PARTICIPANT_NAME_CAP = 64,
};

/* A grace shorter than either ordinary window would cut a legitimate spawn
 * off at the old deadline, silently restoring the pre-#2796 fatal path. */
_Static_assert(CBM_DAEMON_FRONTEND_RECONNECT_GRACE_MS >= FRONTEND_EOF_DRAIN_MS,
               "reconnect grace must cover the EOF drain window");
_Static_assert(CBM_DAEMON_FRONTEND_RECONNECT_GRACE_MS >= FRONTEND_JOIN_WATCHDOG_MS,
               "reconnect grace must cover the join watchdog window");

typedef struct {
    char *message;
    size_t length;
    bool content_length_framed;
    bool has_id;
    int64_t id;
    char *id_str;
    cbm_daemon_runtime_application_token_t request_token;
    bool cancelled;
} frontend_item_t;

typedef struct {
    cbm_mutex_t mutex;
    cbm_daemon_runtime_client_t *client;
    cbm_version_cohort_manager_t *cohort_manager;
    FILE *out;
    frontend_item_t queue[FRONTEND_QUEUE_CAPACITY];
    size_t head;
    size_t count;
    size_t queued_bytes;
    bool input_closed;
    bool stopping;
    bool worker_done;
    bool in_request;
    bool active_has_id;
    int64_t active_id;
    const char *active_id_str;
    cbm_daemon_runtime_application_token_t active_request_token;
    bool failed;
    /* #2796: replacement supplier for a dead daemon transport, plus the retry
     * budget already spent in this run. daemon_lost==NULL keeps the legacy
     * fatal behavior. */
    cbm_daemon_frontend_daemon_lost_fn daemon_lost;
    void *daemon_lost_context;
    /* Read by the worker and the EOF drain (grace reset per attempt). */
    atomic_uint daemon_reconnects;
    /* A cancellation for the active request arrived while its token was
     * invalid during a reconnect; deliver it with the re-minted token. */
    bool cancel_pending;
    /* Clients replaced by a reconnect. close_begin has run, but the allocation
     * stays alive until the run tail: the stdin reader and the maintenance
     * monitor may still be using a pointer they read before the swap, and
     * close_finish frees. Bounded by FRONTEND_DAEMON_RECONNECT_MAX. */
    cbm_daemon_runtime_client_t *retired[FRONTEND_DAEMON_RECONNECT_MAX];
    size_t retired_count;
    /* True while the daemon_lost callback runs. A cold replacement spawn can
     * outlast FRONTEND_EOF_DRAIN_MS, and the drain must treat that time as
     * forward progress rather than a stuck request. */
    atomic_bool reconnect_active;
    /* cbm_now_ms() when the in-flight attempt started (0 when none has run).
     * Both waiters anchor the per-attempt grace here, so the drain and the
     * join watchdog spend one 120 s budget for the same attempt. */
    atomic_uint_fast64_t reconnect_started_ms;
    /* Monotonic count of fully processed queue items (responses written or
     * cancellations acknowledged). The EOF drain below watches it to tell a
     * batch that is still making progress from a genuinely stuck request. */
    atomic_uint_fast64_t completed_items;
} frontend_state_t;

typedef struct {
    frontend_state_t *state;
    atomic_bool complete;
} frontend_join_watchdog_t;

struct cbm_daemon_maintenance_monitor {
    cbm_thread_t thread;
    cbm_version_cohort_manager_t *manager;
    cbm_daemon_maintenance_cancel_fn cancel;
    void *cancel_context;
    atomic_bool stopping;
    bool exit_when_cancel_not_needed;
    int exit_code;
    char participant[FRONTEND_PARTICIPANT_NAME_CAP];
};

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
static atomic_bool frontend_test_hold_monitor = ATOMIC_VAR_INIT(false);
static atomic_bool frontend_test_monitor_is_waiting = ATOMIC_VAR_INIT(false);
static atomic_uint_fast64_t frontend_test_monitor_observation_count = ATOMIC_VAR_INIT(0);
static atomic_uint_fast64_t frontend_test_worker_observation_count = ATOMIC_VAR_INIT(0);
static atomic_uint_fast64_t frontend_test_worker_idle_count = ATOMIC_VAR_INIT(0);

void cbm_daemon_frontend_test_observer_reset(bool hold_monitor) {
    atomic_store_explicit(&frontend_test_monitor_observation_count, 0, memory_order_release);
    atomic_store_explicit(&frontend_test_worker_observation_count, 0, memory_order_release);
    atomic_store_explicit(&frontend_test_worker_idle_count, 0, memory_order_release);
    atomic_store_explicit(&frontend_test_monitor_is_waiting, false, memory_order_release);
    atomic_store_explicit(&frontend_test_hold_monitor, hold_monitor, memory_order_release);
}

void cbm_daemon_frontend_test_observer_release(void) {
    atomic_store_explicit(&frontend_test_hold_monitor, false, memory_order_release);
}

bool cbm_daemon_frontend_test_monitor_waiting(void) {
    return atomic_load_explicit(&frontend_test_monitor_is_waiting, memory_order_acquire);
}

uint64_t cbm_daemon_frontend_test_monitor_observations(void) {
    return atomic_load_explicit(&frontend_test_monitor_observation_count, memory_order_acquire);
}

uint64_t cbm_daemon_frontend_test_worker_observations(void) {
    return atomic_load_explicit(&frontend_test_worker_observation_count, memory_order_acquire);
}

uint64_t cbm_daemon_frontend_test_worker_idle_cycles(void) {
    return atomic_load_explicit(&frontend_test_worker_idle_count, memory_order_acquire);
}
#endif

static cbm_version_cohort_maintenance_presence_t frontend_observe_maintenance(
    cbm_version_cohort_manager_t *manager, bool independent_monitor) {
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
    if (independent_monitor) {
        atomic_fetch_add_explicit(&frontend_test_monitor_observation_count, 1,
                                  memory_order_release);
        if (atomic_load_explicit(&frontend_test_hold_monitor, memory_order_acquire)) {
            atomic_store_explicit(&frontend_test_monitor_is_waiting, true, memory_order_release);
            while (atomic_load_explicit(&frontend_test_hold_monitor, memory_order_acquire)) {
                cbm_usleep(FRONTEND_WAIT_US);
            }
            atomic_store_explicit(&frontend_test_monitor_is_waiting, false, memory_order_release);
        }
    } else {
        atomic_fetch_add_explicit(&frontend_test_worker_observation_count, 1, memory_order_release);
    }
#else
    (void)independent_monitor;
#endif
    return cbm_version_cohort_maintenance_presence_terminal(manager);
}

static void *frontend_maintenance_monitor_worker(void *opaque) {
    cbm_daemon_maintenance_monitor_t *monitor = opaque;
    while (!atomic_load_explicit(&monitor->stopping, memory_order_acquire)) {
        cbm_version_cohort_maintenance_presence_t presence =
            frontend_observe_maintenance(monitor->manager, true);
        if (presence == CBM_VERSION_COHORT_MAINTENANCE_ABSENT) {
            cbm_usleep(FRONTEND_MAINTENANCE_IDLE_POLL_MS * 1000U);
            continue;
        }

        if (presence == CBM_VERSION_COHORT_MAINTENANCE_REQUESTED) {
            bool cancel_requested = false;
            if (monitor->cancel) {
                cancel_requested = monitor->cancel(monitor->cancel_context);
            }
            /* Never log, write to, or flush agent stdio from this monitor.
             * Structured logging itself writes to stderr, and this thread's
             * reason for existing is to remain runnable when another frontend
             * thread is blocked on a full stdout/stderr pipe. The activation
             * owner records the maintenance event durably. */

            if (monitor->exit_when_cancel_not_needed && !cancel_requested) {
                _Exit(monitor->exit_code);
            }
            uint64_t now = cbm_now_ms();
            uint64_t deadline = now > UINT64_MAX - FRONTEND_MAINTENANCE_GRACE_MS
                                    ? UINT64_MAX
                                    : now + FRONTEND_MAINTENANCE_GRACE_MS;
            while (!atomic_load_explicit(&monitor->stopping, memory_order_acquire) &&
                   cbm_now_ms() < deadline) {
                cbm_usleep(FRONTEND_MAINTENANCE_POLL_MS * 1000U);
            }
            if (atomic_load_explicit(&monitor->stopping, memory_order_acquire)) {
                return NULL;
            }
            _Exit(monitor->exit_code);
        }

        /* An observer that cannot prove absence must not let local work
         * survive into a binary mutation window. Fail closed and let native
         * process teardown release SQLite and cohort ownership. */
        _Exit(EXIT_FAILURE);
    }
    return NULL;
}

static cbm_daemon_maintenance_monitor_t *frontend_maintenance_monitor_start(
    cbm_version_cohort_manager_t *manager, cbm_daemon_maintenance_cancel_fn cancel,
    void *cancel_context, int exit_code, const char *participant,
    bool exit_when_cancel_not_needed) {
    if (!manager || exit_code < 0 || !participant || !participant[0]) {
        return NULL;
    }
    cbm_daemon_maintenance_monitor_t *monitor = calloc(1, sizeof(*monitor));
    if (!monitor) {
        return NULL;
    }
    monitor->manager = manager;
    monitor->cancel = cancel;
    monitor->cancel_context = cancel_context;
    monitor->exit_when_cancel_not_needed = exit_when_cancel_not_needed;
    monitor->exit_code = exit_code;
    atomic_init(&monitor->stopping, false);
    int written = snprintf(monitor->participant, sizeof(monitor->participant), "%s", participant);
    if (written <= 0 || written >= (int)sizeof(monitor->participant) ||
        cbm_thread_create(&monitor->thread, 0, frontend_maintenance_monitor_worker, monitor) != 0) {
        free(monitor);
        return NULL;
    }
    return monitor;
}

cbm_daemon_maintenance_monitor_t *cbm_daemon_maintenance_monitor_start(
    cbm_version_cohort_manager_t *manager, cbm_daemon_maintenance_cancel_fn cancel,
    void *cancel_context, int exit_code, const char *participant) {
    return frontend_maintenance_monitor_start(manager, cancel, cancel_context, exit_code,
                                              participant, false);
}

bool cbm_daemon_maintenance_monitor_stop(cbm_daemon_maintenance_monitor_t **monitor_io) {
    if (!monitor_io || !*monitor_io) {
        return false;
    }
    cbm_daemon_maintenance_monitor_t *monitor = *monitor_io;
    atomic_store_explicit(&monitor->stopping, true, memory_order_release);
    if (cbm_thread_join(&monitor->thread) != 0) {
        return false;
    }
    free(monitor);
    *monitor_io = NULL;
    return true;
}

static void frontend_item_free(frontend_item_t *item) {
    if (item) {
        free(item->message);
        free(item->id_str);
        memset(item, 0, sizeof(*item));
    }
}

static bool frontend_should_stop(frontend_state_t *state) {
    cbm_mutex_lock(&state->mutex);
    /* The input-closed leg must also require that no item is IN FLIGHT: the
     * worker consults this while deciding whether a request failure was an
     * expected shutdown, and the final queued item after EOF has count == 0
     * precisely while its own forward is outstanding. Without the in_request
     * term, any daemon-side failure of that last item was classified as an
     * expected stop and its response silently dropped with a success exit —
     * an unanswerable failure mode for the client. */
    bool stopping =
        state->stopping || (state->input_closed && state->count == 0 && !state->in_request);
    cbm_mutex_unlock(&state->mutex);
    return stopping;
}

static void frontend_worker_mark_done(frontend_state_t *state) {
    cbm_mutex_lock(&state->mutex);
    state->worker_done = true;
    cbm_mutex_unlock(&state->mutex);
}

static bool frontend_worker_is_done(frontend_state_t *state) {
    cbm_mutex_lock(&state->mutex);
    bool done = state->worker_done;
    cbm_mutex_unlock(&state->mutex);
    return done;
}

static void frontend_input_closed(frontend_state_t *state) {
    cbm_mutex_lock(&state->mutex);
    state->input_closed = true;
    cbm_mutex_unlock(&state->mutex);
}

static void *frontend_join_watchdog(void *opaque) {
    frontend_join_watchdog_t *watchdog = opaque;
    frontend_state_t *state = watchdog->state;
    cbm_daemon_frontend_liveness_t liveness;
    cbm_daemon_frontend_liveness_init(
        &liveness, cbm_now_ms(),
        atomic_load_explicit(&state->daemon_reconnects, memory_order_acquire));
    while (!atomic_load_explicit(&watchdog->complete, memory_order_acquire)) {
        uint64_t now = cbm_now_ms();
        unsigned int attempts =
            atomic_load_explicit(&state->daemon_reconnects, memory_order_acquire);
        bool reconnecting = atomic_load_explicit(&state->reconnect_active, memory_order_acquire);
        uint64_t attempt_started =
            atomic_load_explicit(&state->reconnect_started_ms, memory_order_acquire);
        if (!cbm_daemon_frontend_liveness_wait(&liveness, now, attempts, reconnecting,
                                               attempt_started, FRONTEND_JOIN_WATCHDOG_MS)) {
            /* The worker may have completed between the poll and the
             * deadline; do not fail-stop a join that is already finishing.
             * `complete` is published only after the main thread's join
             * returns, so also query the worker state to cover the
             * done-but-not-yet-joined window. */
            if (atomic_load_explicit(&watchdog->complete, memory_order_acquire) ||
                frontend_worker_is_done(state)) {
                break;
            }
            /* A thin frontend owns no durable state. If stdout is
             * backpressured, fclose/IPC cancellation cannot portably wake its
             * worker on every platform. Fail-stop releases the authenticated
             * connection and lets the daemon cancel this exact session instead
             * of hanging forever. A replacement spawn in progress gets the
             * same bounded grace as in the EOF drain. */
            _Exit(EXIT_FAILURE);
        }
        cbm_usleep(FRONTEND_WAIT_US);
    }
    return NULL;
}

/* Pop and publish the active request identity under one mutex acquisition, so
 * a cancellation reader can never observe the item as neither queued nor
 * active. */
static bool frontend_pop_begin(frontend_state_t *state, frontend_item_t *item) {
    bool popped = false;
    cbm_mutex_lock(&state->mutex);
    if (!state->stopping && state->count > 0) {
        frontend_item_t *queued = &state->queue[state->head];
        cbm_daemon_runtime_application_token_t request_token =
            CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
        bool token_ready = queued->cancelled || cbm_daemon_runtime_client_application_token_reserve(
                                                    state->client, &request_token);
        if (!token_ready) {
            state->failed = true;
            state->stopping = true;
            cbm_mutex_unlock(&state->mutex);
            return false;
        }
        *item = state->queue[state->head];
        memset(&state->queue[state->head], 0, sizeof(state->queue[state->head]));
        state->head = (state->head + 1U) % FRONTEND_QUEUE_CAPACITY;
        state->count--;
        state->queued_bytes -= item->length;
        state->in_request = true;
        state->cancel_pending = false;
        state->active_has_id = item->has_id;
        state->active_id = item->id;
        state->active_id_str = item->id_str;
        item->request_token = request_token;
        state->active_request_token = request_token;
        popped = true;
    }
    cbm_mutex_unlock(&state->mutex);
    return popped;
}

static void frontend_end_request(frontend_state_t *state, bool failed) {
    cbm_mutex_lock(&state->mutex);
    state->in_request = false;
    state->active_has_id = false;
    state->active_id = 0;
    state->active_id_str = NULL;
    state->active_request_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    state->cancel_pending = false;
    state->failed = state->failed || failed;
    cbm_mutex_unlock(&state->mutex);
}

static bool frontend_write_response(FILE *out, const uint8_t *response, uint32_t response_length,
                                    bool content_length_framed) {
    bool written = false;
    if (content_length_framed) {
        written = fprintf(out, "Content-Length: %u\r\n\r\n", response_length) >= 0 &&
                  fwrite(response, 1, response_length, out) == response_length;
    } else {
        written =
            fwrite(response, 1, response_length, out) == response_length && fputc('\n', out) != EOF;
    }
    return fflush(out) == 0 && written;
}

const char *cbm_daemon_frontend_cancelled_error_message(const char *request_message) {
    static const char plain[] = "Request cancelled";
    static const char index_call[] = "Request cancelled. " CBM_MCP_INDEX_ASYNC_HINT;
    cbm_jsonrpc_request_t request = {0};
    if (!request_message || cbm_jsonrpc_parse(request_message, &request) != 0) {
        return plain;
    }
    char *tool = request.method && strcmp(request.method, "tools/call") == 0 && request.params_raw
                     ? cbm_mcp_get_string_arg(request.params_raw, "name")
                     : NULL;
    bool index_repository = tool && strcmp(tool, "index_repository") == 0;
    safe_free(tool);
    cbm_jsonrpc_request_free(&request);
    return index_repository ? index_call : plain;
}

static bool frontend_write_cancelled_response(FILE *out, const frontend_item_t *item) {
    char cancelled_error[512];
    (void)snprintf(cancelled_error, sizeof(cancelled_error), "{\"code\":-32800,\"message\":\"%s\"}",
                   cbm_daemon_frontend_cancelled_error_message(item->message));
    cbm_jsonrpc_response_t response = {
        .id = item->id,
        .id_str = item->id_str,
        .error_json = cancelled_error,
        .error_code = -32800,
    };
    char *encoded = cbm_jsonrpc_format_response(&response);
    bool written =
        encoded && frontend_write_response(out, (const uint8_t *)encoded, (uint32_t)strlen(encoded),
                                           item->content_length_framed);
    free(encoded);
    return written;
}

static bool frontend_parse_cancellation(const char *message, cbm_jsonrpc_request_t *request_out) {
    if (!message || !request_out) {
        return false;
    }
    memset(request_out, 0, sizeof(*request_out));
    if (cbm_jsonrpc_parse(message, request_out) != 0) {
        return false;
    }
    bool cancellation = !request_out->has_id && request_out->method &&
                        strcmp(request_out->method, "notifications/cancelled") == 0;
    if (!cancellation) {
        cbm_jsonrpc_request_free(request_out);
    }
    return cancellation;
}

bool cbm_daemon_frontend_is_cancellation_notification(const char *message) {
    cbm_jsonrpc_request_t request = {0};
    bool cancellation = frontend_parse_cancellation(message, &request);
    if (cancellation) {
        cbm_jsonrpc_request_free(&request);
    }
    return cancellation;
}

bool cbm_daemon_frontend_cancellation_matches_request(const char *message, int64_t active_id,
                                                      const char *active_id_str) {
    cbm_jsonrpc_request_t request = {0};
    bool cancellation = frontend_parse_cancellation(message, &request);
    bool matches = cancellation &&
                   cbm_mcp_cancel_request_matches(request.params_raw, active_id, active_id_str);
    if (cancellation) {
        cbm_jsonrpc_request_free(&request);
    }
    return matches;
}

typedef enum {
    FRONTEND_CANCELLATION_NONE = 0,
    FRONTEND_CANCELLATION_STALE = 1,
    FRONTEND_CANCELLATION_QUEUED = 2,
    FRONTEND_CANCELLATION_ACTIVE = 3,
    /* Matched the active request while its token was invalid during a
     * reconnect; delivered when the retry publishes the new token. */
    FRONTEND_CANCELLATION_DEFERRED = 4,
} frontend_cancellation_route_t;

/* Cancellation is an out-of-band frontend control message. A matching active
 * request routes its exact runtime token without closing the authenticated
 * session. A queued request is marked and receives a cancellation error
 * without ever reaching the daemon. Stale/invalid targets are ignored. */
static frontend_cancellation_route_t frontend_route_cancellation(
    frontend_state_t *state, const char *message,
    cbm_daemon_runtime_application_token_t *request_token_out,
    cbm_daemon_runtime_client_t **request_client_out) {
    if (request_token_out) {
        *request_token_out = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    }
    if (request_client_out) {
        *request_client_out = NULL;
    }
    cbm_jsonrpc_request_t request = {0};
    if (!frontend_parse_cancellation(message, &request)) {
        return FRONTEND_CANCELLATION_NONE;
    }

    frontend_cancellation_route_t route = FRONTEND_CANCELLATION_STALE;
    cbm_mutex_lock(&state->mutex);
    if (state->in_request && state->active_has_id &&
        cbm_mcp_cancel_request_matches(request.params_raw, state->active_id,
                                       state->active_id_str)) {
        /* Remember the cancel even while the token is invalid during a
         * reconnect: the retry delivers it with the freshly minted token. */
        state->cancel_pending = true;
        if (state->active_request_token != CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID) {
            if (request_token_out) {
                *request_token_out = state->active_request_token;
            }
            /* Snapshot the owner under the same lock as the token: the worker
             * may already have swapped clients, and the token is only
             * meaningful on the client that minted it. */
            if (request_client_out) {
                *request_client_out = state->client;
            }
            route = FRONTEND_CANCELLATION_ACTIVE;
        } else {
            route = FRONTEND_CANCELLATION_DEFERRED;
        }
    } else {
        for (size_t offset = 0; offset < state->count; offset++) {
            size_t index = (state->head + offset) % FRONTEND_QUEUE_CAPACITY;
            frontend_item_t *item = &state->queue[index];
            if (item->has_id &&
                cbm_mcp_cancel_request_matches(request.params_raw, item->id, item->id_str)) {
                item->cancelled = true;
                route = FRONTEND_CANCELLATION_QUEUED;
                break;
            }
        }
    }
    cbm_mutex_unlock(&state->mutex);
    cbm_jsonrpc_request_free(&request);
    return route;
}

/* #2796: swap in a replacement daemon client after a transport failure. The
 * request token minted for the interrupted exchange was scoped to the dead
 * client, so the caller must re-reserve one before re-issuing the request.
 * The callback runs unmutexed (a cold daemon spawn takes seconds); the swap
 * itself refuses once teardown has begun so no replacement outlives the run.
 * The dead client stays allocated until the run tail because the stdin reader
 * and the maintenance monitor can still hold a pointer read before the swap;
 * close_begin marks it closed without freeing. */
static bool frontend_daemon_reconnect(frontend_state_t *state) {
    unsigned int attempts = atomic_load_explicit(&state->daemon_reconnects, memory_order_relaxed);
    if (!state->daemon_lost || attempts >= FRONTEND_DAEMON_RECONNECT_MAX) {
        return false;
    }
    cbm_mutex_lock(&state->mutex);
    bool stopping = state->stopping;
    cbm_daemon_runtime_client_t *dead = state->client;
    if (!stopping) {
        /* The published token belongs to the dead client. Invalidate it for
         * the duration of the callback (a cold spawn takes seconds); a
         * cancellation that arrives meanwhile is remembered in cancel_pending
         * and delivered with the re-minted token. */
        state->active_request_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    }
    cbm_mutex_unlock(&state->mutex);
    if (stopping) {
        return false;
    }
    /* Publish the attempt's timestamp and active flag BEFORE incrementing the
     * counter. A waiter that observes the new counter (acquire) must then see
     * this attempt's timestamp too; incrementing first would let it pair a new
     * counter with the previous attempt's timestamp and compress the new
     * attempt's grace. */
    atomic_store_explicit(&state->reconnect_started_ms, cbm_now_ms(), memory_order_release);
    atomic_store_explicit(&state->reconnect_active, true, memory_order_release);
    atomic_fetch_add_explicit(&state->daemon_reconnects, 1, memory_order_release);
    cbm_daemon_runtime_client_t *replacement = state->daemon_lost(state->daemon_lost_context, dead);
    atomic_store_explicit(&state->reconnect_active, false, memory_order_release);
    if (!replacement || replacement == dead) {
        return false;
    }
    /* A supplier that hands back a client retired earlier in this run would be
     * appended twice and freed twice at the run tail. */
    for (size_t i = 0; i < state->retired_count; i++) {
        if (state->retired[i] == replacement) {
            return false;
        }
    }
    cbm_mutex_lock(&state->mutex);
    if (state->stopping || state->client != dead) {
        cbm_mutex_unlock(&state->mutex);
        (void)cbm_daemon_runtime_client_close(replacement, FRONTEND_CLOSE_TIMEOUT_MS);
        return false;
    }
    state->client = replacement;
    cbm_mutex_unlock(&state->mutex);
    (void)cbm_daemon_runtime_client_close_begin(dead);
    /* daemon_reconnects bounds this to the array size; guard locally so a
     * future caller cannot corrupt the run. */
    if (state->retired_count < FRONTEND_DAEMON_RECONNECT_MAX) {
        state->retired[state->retired_count++] = dead;
    }
    return true;
}

/* #2796: one request exchange with replacement-daemon retries. Returns the
 * final status; response and response_length are set only on OK. */
static cbm_daemon_runtime_application_status_t frontend_exchange_with_reconnect(
    frontend_state_t *state, frontend_item_t *item, uint8_t **response, uint32_t *response_length) {
    cbm_daemon_runtime_application_status_t status;
    for (;;) {
        status = cbm_daemon_application_client_mcp_tagged(state->client, item->request_token,
                                                          item->message, response, response_length,
                                                          FRONTEND_REQUEST_TIMEOUT_MS);
        if (status == CBM_DAEMON_RUNTIME_APPLICATION_OK) {
            return status;
        }
        /* Only a transport failure triggers recovery. BUSY, UNAVAILABLE,
         * REJECTED, HANDLER_ERROR and CANCELLED are daemon answers and must
         * keep their existing meaning; CANCELLED in particular must never be
         * re-issued as a fresh request. A dead transport is not this session's
         * death: swap in a replacement client, re-mint the request token
         * (tokens are scoped to the client that reserved them), and re-issue
         * the interrupted request.
         *
         * TRANSPORT_ERROR also covers local allocation failures in the
         * application layer while the connection is still healthy; replaying
         * such a request is harmless (it never left the client) but costs a
         * reconnect. It also covers a reply the client could not copy after
         * the daemon executed the request (runtime response-copy failure).
         * Such a request is replayed even though it may have run; the
         * alternative is losing both the response and the session, and the
         * transport failure carries no evidence either way. */
        if (status != CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR ||
            !frontend_daemon_reconnect(state)) {
            return status;
        }
        /* Reserve and publish under one lock, like frontend_pop_begin. A
         * cancellation that arrived while the reconnect held the token
         * invalid is remembered in cancel_pending and delivered with this
         * fresh token. */
        cbm_mutex_lock(&state->mutex);
        bool minted = cbm_daemon_runtime_client_application_token_reserve(state->client,
                                                                          &item->request_token);
        bool deliver_cancel = minted && state->cancel_pending;
        if (minted) {
            state->active_request_token = item->request_token;
            state->cancel_pending = false;
        }
        cbm_mutex_unlock(&state->mutex);
        if (!minted) {
            /* The replacement is installed but cannot mint a token (it is
             * already closing or unusable); the run teardown closes it. */
            return status;
        }
        if (deliver_cancel) {
            (void)cbm_daemon_runtime_client_application_cancel(state->client, item->request_token);
        }
    }
}

static void *frontend_worker(void *opaque) {
    frontend_state_t *state = opaque;
    for (;;) {
        frontend_item_t item = {0};
        if (!frontend_pop_begin(state, &item)) {
            if (frontend_should_stop(state)) {
                break;
            }
#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
            atomic_fetch_add_explicit(&frontend_test_worker_idle_count, 1, memory_order_release);
#endif
            cbm_usleep(FRONTEND_IDLE_WAIT_US);
            continue;
        }
        uint8_t *response = NULL;
        uint32_t response_length = 0;
        bool failed = false;
        if (item.cancelled) {
            failed = !frontend_write_cancelled_response(state->out, &item);
        } else {
            cbm_daemon_runtime_application_status_t status =
                frontend_exchange_with_reconnect(state, &item, &response, &response_length);
            if (status == CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED) {
                failed = !frontend_write_cancelled_response(state->out, &item);
            } else {
                failed = status != CBM_DAEMON_RUNTIME_APPLICATION_OK;
            }
            if (status == CBM_DAEMON_RUNTIME_APPLICATION_OK && !failed && response &&
                response_length > 0) {
                failed = !frontend_write_response(state->out, response, response_length,
                                                  item.content_length_framed);
            }
        }
        free(response);
        bool expected_stop = failed && frontend_should_stop(state);
        frontend_end_request(state, failed && !expected_stop);
        frontend_item_free(&item);
        atomic_fetch_add_explicit(&state->completed_items, 1, memory_order_release);
        if (failed) {
            if (!expected_stop) {
                /* #2796: reached only without a daemon_lost callback or after
                 * its retry budget is spent. A failed daemon transport cannot
                 * wake a thread blocked in stdio portably. This frontend owns
                 * no state: immediate process exit closes the kernel IPC
                 * handle, which cancels daemon session ownership without a
                 * detached reader or an unsafe cross-thread fclose. Logging or
                 * flushing here could itself block on agent-owned
                 * stdout/stderr. */
                _Exit(EXIT_FAILURE);
            }
            break;
        }
    }
    frontend_worker_mark_done(state);
    return NULL;
}

static bool frontend_enqueue(frontend_state_t *state, char *message, bool content_length_framed) {
    size_t length = strlen(message);
    bool has_id = false;
    int64_t id = 0;
    char *id_str = NULL;
    cbm_jsonrpc_request_t request = {0};
    if (cbm_jsonrpc_parse(message, &request) == 0) {
        has_id = request.has_id;
        id = request.id;
        if (request.id_str) {
            id_str = cbm_strdup(request.id_str);
        }
        bool identity_copied = !request.id_str || id_str;
        cbm_jsonrpc_request_free(&request);
        if (!identity_copied) {
            return false;
        }
    }
    /* A frame that exceeds the whole byte budget can never be admitted no
     * matter how long we wait; only that case is a hard failure. */
    if (length > FRONTEND_QUEUE_BYTES_MAX) {
        cbm_mutex_lock(&state->mutex);
        if (!state->stopping && !state->failed) {
            state->failed = true;
            state->stopping = true;
        }
        cbm_mutex_unlock(&state->mutex);
        free(id_str);
        return false;
    }
    /* A full queue is BACKPRESSURE, not failure. This runs on the stdin reader
     * thread, so blocking here stops further reads and lets the kernel pipe
     * absorb the client's burst — a pipelining client is a client going fast,
     * not a client going wrong. The old code failed the whole session at frame
     * capacity+1, killing it with every buffered response unwritten: any 7+
     * pipelined requests (e.g. an agent issuing parallel tool calls) died with
     * rc=1 and zero output (#1522 follow-up). Waits are bounded only by the
     * stop/fail flags, which every teardown path already sets — the same
     * condition the polling worker and watchdogs key on. */
    for (;;) {
        cbm_mutex_lock(&state->mutex);
        bool stopped = state->stopping || state->failed;
        if (stopped) {
            cbm_mutex_unlock(&state->mutex);
            free(id_str);
            return false;
        }
        bool capacity = state->count < FRONTEND_QUEUE_CAPACITY &&
                        state->queued_bytes <= FRONTEND_QUEUE_BYTES_MAX &&
                        length <= FRONTEND_QUEUE_BYTES_MAX - state->queued_bytes;
        if (capacity) {
            size_t tail = (state->head + state->count) % FRONTEND_QUEUE_CAPACITY;
            state->queue[tail] = (frontend_item_t){
                .message = message,
                .length = length,
                .content_length_framed = content_length_framed,
                .has_id = has_id,
                .id = id,
                .id_str = id_str,
            };
            state->count++;
            state->queued_bytes += length;
            cbm_mutex_unlock(&state->mutex);
            return true;
        }
        cbm_mutex_unlock(&state->mutex);
        cbm_usleep(FRONTEND_WAIT_US);
    }
}

static bool frontend_stop_begin(frontend_state_t *state) {
    cbm_mutex_lock(&state->mutex);
    state->stopping = true;
    cbm_daemon_runtime_client_t *client = state->client;
    cbm_mutex_unlock(&state->mutex);
    /* Retain the client allocation until the worker is joined. This covers the
     * boundary where the worker has claimed an item but has not yet entered the
     * runtime exchange: a late call observes closing instead of freed memory.
     * The worker cannot swap after stopping is published, so this snapshot is
     * the client this run owns. */
    return cbm_daemon_runtime_client_close_begin(client);
}

static bool frontend_cancel_for_maintenance(void *opaque) {
    frontend_state_t *state = opaque;
    cbm_daemon_runtime_application_token_t request_token =
        CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    cbm_daemon_runtime_client_t *client = NULL;
    bool in_request;
    cbm_mutex_lock(&state->mutex);
    state->stopping = true;
    in_request = state->in_request;
    if (in_request) {
        request_token = state->active_request_token;
        client = state->client;
    }
    cbm_mutex_unlock(&state->mutex);
    if (!in_request) {
        return false;
    }
    if (request_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID || !client) {
        /* A reconnect holds the token invalid while a request is still in
         * flight: report the cancel as requested so the monitor takes its
         * bounded grace instead of exiting immediately. */
        return true;
    }
    return cbm_daemon_runtime_client_application_cancel(client, request_token) ==
           CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED;
}

int cbm_daemon_frontend_mcp_run(cbm_daemon_runtime_client_t *client,
                                cbm_version_cohort_manager_t *cohort_manager, FILE *in, FILE *out,
                                cbm_daemon_frontend_daemon_lost_fn daemon_lost,
                                void *daemon_lost_context) {
    if (!client || !cohort_manager || !in || !out) {
        return -1;
    }
    frontend_state_t state = {
        .client = client,
        .cohort_manager = cohort_manager,
        .out = out,
        .daemon_lost = daemon_lost,
        .daemon_lost_context = daemon_lost_context,
    };
    cbm_mutex_init(&state.mutex);
    atomic_init(&state.completed_items, 0);
    atomic_init(&state.daemon_reconnects, 0);
    atomic_init(&state.reconnect_active, false);
    atomic_init(&state.reconnect_started_ms, 0);
    cbm_daemon_maintenance_monitor_t *maintenance_monitor =
        frontend_maintenance_monitor_start(cohort_manager, frontend_cancel_for_maintenance, &state,
                                           EXIT_SUCCESS, "MCP frontend", true);
    if (!maintenance_monitor) {
        cbm_mutex_destroy(&state.mutex);
        (void)cbm_daemon_runtime_client_close(client, FRONTEND_CLOSE_TIMEOUT_MS);
        return -1;
    }
    cbm_thread_t worker;
    if (cbm_thread_create(&worker, 0, frontend_worker, &state) != 0) {
        if (!cbm_daemon_maintenance_monitor_stop(&maintenance_monitor)) {
            _Exit(EXIT_FAILURE);
        }
        cbm_mutex_destroy(&state.mutex);
        (void)cbm_daemon_runtime_client_close(client, FRONTEND_CLOSE_TIMEOUT_MS);
        return -1;
    }

    int result = 0;
    bool close_begun = false;
    bool clean_eof = false;
    for (;;) {
        char *message = NULL;
        bool content_length_framed = false;
        int read_status = cbm_mcp_read_message(in, &message, &content_length_framed);
        if (read_status <= 0) {
            result = read_status < 0 ? -1 : 0;
            clean_eof = read_status == 0;
            free(message);
            break;
        }
        cbm_daemon_runtime_application_token_t cancel_token =
            CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
        cbm_daemon_runtime_client_t *cancel_client = NULL;
        frontend_cancellation_route_t cancellation =
            frontend_route_cancellation(&state, message, &cancel_token, &cancel_client);
        if (cancellation != FRONTEND_CANCELLATION_NONE) {
            free(message);
            if (cancellation == FRONTEND_CANCELLATION_ACTIVE) {
                /* Cancellation is best-effort: ERROR means the client is
                 * already unusable or closing. The in-flight exchange observes
                 * the same failure (or stop), and the worker owns the recovery
                 * decision instead of the reader tearing the session down. */
                (void)cbm_daemon_runtime_client_application_cancel(cancel_client, cancel_token);
            }
            /* A queued cancellation is represented on that queue item; stale
             * and malformed targets are intentionally side-effect free. */
            continue;
        }
        if (!frontend_enqueue(&state, message, content_length_framed)) {
            free(message);
            result = -1;
            break;
        }
    }

    if (clean_eof && !close_begun) {
        frontend_input_closed(&state);
        /* EOF ends INPUT, not accepted work: as long as queued items keep
         * completing, every already-enqueued request still receives its
         * response, exactly like the pre-daemon stdio server, which
         * processed the whole stream before exiting. The window resets on
         * each completed item, so it bounds STUCKNESS, not batch length: a
         * request making no progress for the full window is cancelled by the
         * stop path below and the frontend still exits cleanly. The previous
         * fixed 1s total window abandoned queued requests whenever the
         * daemon cold spawn plus the first request outlasted it (several
         * seconds on Windows), so batch clients that close stdin right after
         * writing — file redirects, piped harnesses, hook one-shots —
         * intermittently lost their tail responses. */
        uint64_t drained = atomic_load_explicit(&state.completed_items, memory_order_acquire);
        cbm_daemon_frontend_liveness_t liveness;
        cbm_daemon_frontend_liveness_init(
            &liveness, cbm_now_ms(),
            atomic_load_explicit(&state.daemon_reconnects, memory_order_acquire));
        for (;;) {
            if (frontend_worker_is_done(&state)) {
                break;
            }
            uint64_t now = cbm_now_ms();
            unsigned int attempts =
                atomic_load_explicit(&state.daemon_reconnects, memory_order_acquire);
            bool reconnecting = atomic_load_explicit(&state.reconnect_active, memory_order_acquire);
            uint64_t attempt_started =
                atomic_load_explicit(&state.reconnect_started_ms, memory_order_acquire);
            if (!cbm_daemon_frontend_liveness_wait(&liveness, now, attempts, reconnecting,
                                                   attempt_started, FRONTEND_EOF_DRAIN_MS)) {
                if (frontend_worker_is_done(&state)) {
                    break;
                }
                /* Re-sample once: an attempt that became visible after the
                 * poll above is granted its grace instead of being cut off by
                 * stop_begin. */
                bool late_reconnect =
                    atomic_load_explicit(&state.reconnect_active, memory_order_acquire);
                uint64_t late_started =
                    atomic_load_explicit(&state.reconnect_started_ms, memory_order_acquire);
                if (late_reconnect &&
                    cbm_daemon_frontend_liveness_reconnect_late(&liveness, now, late_started)) {
                    continue;
                }
                break;
            }
            cbm_usleep(FRONTEND_WAIT_US);
            uint64_t now_drained =
                atomic_load_explicit(&state.completed_items, memory_order_acquire);
            if (now_drained != drained) {
                drained = now_drained;
                cbm_daemon_frontend_liveness_progress(&liveness, cbm_now_ms());
            }
        }
    }
    if (!close_begun) {
        close_begun = frontend_stop_begin(&state);
    }
    frontend_join_watchdog_t watchdog;
    cbm_thread_t watchdog_thread;
    bool watchdog_started = false;
    if (!frontend_worker_is_done(&state)) {
        watchdog.state = &state;
        atomic_init(&watchdog.complete, false);
        if (cbm_thread_create(&watchdog_thread, 0, frontend_join_watchdog, &watchdog) != 0) {
            _Exit(EXIT_FAILURE);
        }
        watchdog_started = true;
    }
    if (cbm_thread_join(&worker) != 0) {
        /* Preserve every object the worker may still reference. */
        _Exit(EXIT_FAILURE);
    }
    if (watchdog_started) {
        atomic_store_explicit(&watchdog.complete, true, memory_order_release);
        if (cbm_thread_join(&watchdog_thread) != 0) {
            _Exit(EXIT_FAILURE);
        }
    }
    if (!cbm_daemon_maintenance_monitor_stop(&maintenance_monitor)) {
        _Exit(EXIT_FAILURE);
    }
    if (close_begun) {
        (void)cbm_daemon_runtime_client_close_finish(state.client, FRONTEND_CLOSE_TIMEOUT_MS);
    } else {
        result = -1;
    }
    /* The worker is joined and the maintenance monitor is stopped, so no other
     * thread can still reference a client retired by a reconnect. */
    for (size_t i = 0; i < state.retired_count; i++) {
        (void)cbm_daemon_runtime_client_close_finish(state.retired[i], FRONTEND_CLOSE_TIMEOUT_MS);
    }
    for (size_t i = 0; i < FRONTEND_QUEUE_CAPACITY; i++) {
        frontend_item_free(&state.queue[i]);
    }
    if (state.failed) {
        result = -1;
    }
    cbm_mutex_destroy(&state.mutex);
    return result;
}
