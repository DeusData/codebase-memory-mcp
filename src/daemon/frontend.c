/*
 * frontend.c — Stateless stdio bridge for a daemon-backed MCP session.
 */
#include "daemon/frontend.h"

#include "daemon/application.h"
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
    FRONTEND_MAINTENANCE_POLL_MS = 10,
    /* The owner thread may be draining a supervised process tree. Preserve the
     * supervisor's complete graceful + forced-settle window before the monitor
     * fail-stops the process, plus scheduling/teardown margin. */
    FRONTEND_MAINTENANCE_GRACE_MS =
        CBM_SUBPROCESS_MAX_CANCEL_GRACE_MS + CBM_SUBPROCESS_FORCE_SETTLE_MS + 1000,
    FRONTEND_PARTICIPANT_NAME_CAP = 64,
};

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
    /* Monotonic count of fully processed queue items (responses written or
     * cancellations acknowledged). The EOF drain below watches it to tell a
     * batch that is still making progress from a genuinely stuck request. */
    atomic_uint_fast64_t completed_items;
} frontend_state_t;

typedef struct {
    atomic_bool complete;
} frontend_join_watchdog_t;

struct cbm_daemon_maintenance_monitor {
    cbm_thread_t thread;
    cbm_version_cohort_manager_t *manager;
    cbm_daemon_maintenance_cancel_fn cancel;
    void *cancel_context;
    atomic_bool stopping;
    int exit_code;
    char participant[FRONTEND_PARTICIPANT_NAME_CAP];
};

static void *frontend_maintenance_monitor_worker(void *opaque) {
    cbm_daemon_maintenance_monitor_t *monitor = opaque;
    while (!atomic_load_explicit(&monitor->stopping, memory_order_acquire)) {
        cbm_version_cohort_maintenance_presence_t presence =
            cbm_version_cohort_maintenance_presence_terminal(monitor->manager);
        if (presence == CBM_VERSION_COHORT_MAINTENANCE_ABSENT) {
            cbm_usleep(FRONTEND_MAINTENANCE_POLL_MS * 1000U);
            continue;
        }

        if (presence == CBM_VERSION_COHORT_MAINTENANCE_REQUESTED) {
            if (monitor->cancel) {
                (void)monitor->cancel(monitor->cancel_context);
            }
            /* Never log, write to, or flush agent stdio from this monitor.
             * Structured logging itself writes to stderr, and this thread's
             * reason for existing is to remain runnable when another frontend
             * thread is blocked on a full stdout/stderr pipe. The activation
             * owner records the maintenance event durably. */

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

cbm_daemon_maintenance_monitor_t *cbm_daemon_maintenance_monitor_start(
    cbm_version_cohort_manager_t *manager, cbm_daemon_maintenance_cancel_fn cancel,
    void *cancel_context, int exit_code, const char *participant) {
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

static void frontend_exit_for_maintenance(frontend_state_t *state) {
    cbm_version_cohort_maintenance_presence_t presence =
        cbm_version_cohort_maintenance_presence_terminal(state->cohort_manager);
    if (presence == CBM_VERSION_COHORT_MAINTENANCE_ABSENT) {
        return;
    }
    /* Do not fclose stdin across threads. Process exit closes the authenticated
     * kernel IPC handle, and daemon ownership then cancels only this session.
     * Agent stdout/stderr may both be backpressured, so terminal paths must not
     * log, write, or flush before fail-stop. */
    _Exit(presence == CBM_VERSION_COHORT_MAINTENANCE_REQUESTED ? EXIT_SUCCESS : EXIT_FAILURE);
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
    uint64_t now = cbm_now_ms();
    uint64_t deadline =
        now > UINT64_MAX - FRONTEND_JOIN_WATCHDOG_MS ? UINT64_MAX : now + FRONTEND_JOIN_WATCHDOG_MS;
    while (!atomic_load_explicit(&watchdog->complete, memory_order_acquire) &&
           cbm_now_ms() < deadline) {
        cbm_usleep(FRONTEND_WAIT_US);
    }
    if (!atomic_load_explicit(&watchdog->complete, memory_order_acquire)) {
        /* A thin frontend owns no durable state. If stdout is backpressured,
         * fclose/IPC cancellation cannot portably wake its worker on every
         * platform. Fail-stop releases the authenticated connection and lets
         * the daemon cancel this exact session instead of hanging forever. */
        _Exit(EXIT_FAILURE);
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

static bool frontend_write_cancelled_response(FILE *out, const frontend_item_t *item) {
    static const char cancelled_error[] = "{\"code\":-32800,\"message\":\"Request cancelled\"}";
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
} frontend_cancellation_route_t;

/* Cancellation is an out-of-band frontend control message. A matching active
 * request routes its exact runtime token without closing the authenticated
 * session. A queued request is marked and receives a cancellation error
 * without ever reaching the daemon. Stale/invalid targets are ignored. */
static frontend_cancellation_route_t frontend_route_cancellation(
    frontend_state_t *state, const char *message,
    cbm_daemon_runtime_application_token_t *request_token_out) {
    if (request_token_out) {
        *request_token_out = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    }
    cbm_jsonrpc_request_t request = {0};
    if (!frontend_parse_cancellation(message, &request)) {
        return FRONTEND_CANCELLATION_NONE;
    }

    frontend_cancellation_route_t route = FRONTEND_CANCELLATION_STALE;
    cbm_mutex_lock(&state->mutex);
    if (state->in_request && state->active_has_id &&
        state->active_request_token != CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID &&
        cbm_mcp_cancel_request_matches(request.params_raw, state->active_id,
                                       state->active_id_str)) {
        if (request_token_out) {
            *request_token_out = state->active_request_token;
        }
        route = FRONTEND_CANCELLATION_ACTIVE;
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

static void *frontend_worker(void *opaque) {
    frontend_state_t *state = opaque;
    uint64_t next_maintenance_check_ms = 0;
    for (;;) {
        uint64_t now_ms = cbm_now_ms();
        if (now_ms >= next_maintenance_check_ms) {
            frontend_exit_for_maintenance(state);
            next_maintenance_check_ms = now_ms > UINT64_MAX - FRONTEND_MAINTENANCE_POLL_MS
                                            ? UINT64_MAX
                                            : now_ms + FRONTEND_MAINTENANCE_POLL_MS;
        }
        frontend_item_t item = {0};
        if (!frontend_pop_begin(state, &item)) {
            if (frontend_should_stop(state)) {
                break;
            }
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
                cbm_daemon_application_client_mcp_tagged(state->client, item.request_token,
                                                         item.message, &response, &response_length,
                                                         FRONTEND_REQUEST_TIMEOUT_MS);
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
                /* A failed daemon transport cannot wake a thread blocked in
                 * stdio portably. This frontend owns no state: immediate
                 * process exit closes the kernel IPC handle, which cancels
                 * daemon session ownership without a detached reader or an
                 * unsafe cross-thread fclose. Logging or flushing here could
                 * itself block on agent-owned stdout/stderr. */
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
    cbm_mutex_lock(&state->mutex);
    bool stopped = state->stopping || state->failed;
    bool capacity = state->count < FRONTEND_QUEUE_CAPACITY &&
                    state->queued_bytes <= FRONTEND_QUEUE_BYTES_MAX &&
                    length <= FRONTEND_QUEUE_BYTES_MAX - state->queued_bytes;
    if (!stopped && capacity) {
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
    if (!stopped) {
        state->failed = true;
        state->stopping = true;
    }
    cbm_mutex_unlock(&state->mutex);
    free(id_str);
    return false;
}

static bool frontend_stop_begin(frontend_state_t *state) {
    cbm_mutex_lock(&state->mutex);
    state->stopping = true;
    cbm_mutex_unlock(&state->mutex);
    /* Retain the client allocation until the worker is joined. This covers the
     * boundary where the worker has claimed an item but has not yet entered the
     * runtime exchange: a late call observes closing instead of freed memory. */
    return cbm_daemon_runtime_client_close_begin(state->client);
}

static bool frontend_cancel_for_maintenance(void *opaque) {
    frontend_state_t *state = opaque;
    cbm_daemon_runtime_application_token_t request_token =
        CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    cbm_mutex_lock(&state->mutex);
    state->stopping = true;
    if (state->in_request) {
        request_token = state->active_request_token;
    }
    cbm_mutex_unlock(&state->mutex);
    if (request_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID) {
        return false;
    }
    return cbm_daemon_runtime_client_application_cancel(state->client, request_token) ==
           CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED;
}

int cbm_daemon_frontend_mcp_run(cbm_daemon_runtime_client_t *client,
                                cbm_version_cohort_manager_t *cohort_manager, FILE *in, FILE *out) {
    if (!client || !cohort_manager || !in || !out) {
        return -1;
    }
    frontend_state_t state = {
        .client = client,
        .cohort_manager = cohort_manager,
        .out = out,
    };
    cbm_mutex_init(&state.mutex);
    atomic_init(&state.completed_items, 0);
    cbm_daemon_maintenance_monitor_t *maintenance_monitor = cbm_daemon_maintenance_monitor_start(
        cohort_manager, frontend_cancel_for_maintenance, &state, EXIT_SUCCESS, "MCP frontend");
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
        frontend_cancellation_route_t cancellation =
            frontend_route_cancellation(&state, message, &cancel_token);
        if (cancellation != FRONTEND_CANCELLATION_NONE) {
            free(message);
            if (cancellation == FRONTEND_CANCELLATION_ACTIVE) {
                cbm_daemon_runtime_cancel_result_t cancelled =
                    cbm_daemon_runtime_client_application_cancel(state.client, cancel_token);
                if (cancelled == CBM_DAEMON_RUNTIME_CANCEL_ERROR) {
                    result = -1;
                    close_begun = frontend_stop_begin(&state);
                    break;
                }
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
        uint64_t drain_deadline = cbm_now_ms() + FRONTEND_EOF_DRAIN_MS;
        while (!frontend_worker_is_done(&state) && cbm_now_ms() < drain_deadline) {
            cbm_usleep(FRONTEND_WAIT_US);
            uint64_t now_drained =
                atomic_load_explicit(&state.completed_items, memory_order_acquire);
            if (now_drained != drained) {
                drained = now_drained;
                drain_deadline = cbm_now_ms() + FRONTEND_EOF_DRAIN_MS;
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
    for (size_t i = 0; i < FRONTEND_QUEUE_CAPACITY; i++) {
        frontend_item_free(&state.queue[i]);
    }
    if (state.failed) {
        result = -1;
    }
    cbm_mutex_destroy(&state.mutex);
    return result;
}
