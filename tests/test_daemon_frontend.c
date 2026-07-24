/*
 * test_daemon_frontend.c — Exact JSON-RPC cancellation and maintenance.
 */
#include "test_framework.h"
#include "test_helpers.h"

#include "daemon/frontend.h"
#include "daemon/ipc.h"
#include "daemon/service.h"
#include "daemon/version_cohort.h"
#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"

#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <string.h>

#ifndef _WIN32
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

enum { FRONTEND_TEST_PATH_CAP = 1024 };

typedef struct {
    char parent[FRONTEND_TEST_PATH_CAP];
    cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_version_cohort_manager_t *manager;
} frontend_maintenance_fixture_t;

static bool frontend_maintenance_fixture_start(frontend_maintenance_fixture_t *fixture,
                                               const char *tag) {
    memset(fixture, 0, sizeof(*fixture));
    /* Endpoint parents need production-shaped ancestry (LocalAppData on
     * Windows); temp roots are refused by the runtime ancestry validation. */
    if (!th_secure_runtime_parent_new(fixture->parent, sizeof(fixture->parent), tag)) {
        return false;
    }
    fixture->endpoint = cbm_daemon_ipc_endpoint_new("0123456789abcdef", fixture->parent);
    fixture->manager = fixture->endpoint ? cbm_version_cohort_manager_new(fixture->endpoint) : NULL;
    return fixture->endpoint && fixture->manager;
}

static void frontend_maintenance_fixture_finish(frontend_maintenance_fixture_t *fixture) {
    while (fixture->manager &&
           cbm_version_cohort_manager_free(&fixture->manager) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
    cbm_daemon_ipc_endpoint_free(fixture->endpoint);
    if (fixture->parent[0]) {
        (void)th_rmtree(fixture->parent);
    }
    memset(fixture, 0, sizeof(*fixture));
}

#ifndef _WIN32
static const char FRONTEND_TEST_BUILD[] =
    "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
static const char FRONTEND_TEST_CACHE[] =
    "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

enum {
    /* Hang guards, not benchmarks: starved 3-4-core CI runners need several
     * seconds for frontend/daemon startup paths that finish instantly on a
     * dev machine (see the index-supervisor calibration). */
    FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS = 30000,
    FRONTEND_EOF_TEST_CATASTROPHIC_TIMEOUT_S = 90,
    FRONTEND_BACKPRESSURE_MESSAGE_BYTES = 2 * 1024 * 1024,
    FRONTEND_BACKPRESSURE_FRONTEND_TIMEOUT_S = 90,
    FRONTEND_BACKPRESSURE_DAEMON_TIMEOUT_S = 90,
    FRONTEND_BACKPRESSURE_RUNTIME_TIMEOUT_MS = 90000,
    FRONTEND_BACKPRESSURE_CLEANUP_TIMEOUT_MS = 30000,
    /* The production queue is deliberately bounded below this count. Keep the
     * regression black-box: it must remain valid if the exact capacity changes
     * while still proving that overload cannot hide an already-pending EOF. */
    FRONTEND_EOF_TEST_OVERFLOW_MESSAGES = 32,
};

typedef struct {
    atomic_int requests;
    atomic_int session_cancels;
    atomic_int sessions_opened;
    atomic_int first_session_cancels;
    atomic_int second_session_cancels;
    atomic_bool block_first_request;
    atomic_bool first_request_started;
    int request_observed_fd;
    int session_cancel_fd;
} frontend_eof_application_context_t;

typedef struct {
    frontend_eof_application_context_t *context;
    atomic_bool cancelled;
    int session_index;
} frontend_eof_application_session_t;

typedef struct {
    int fd;
    bool overflow;
    frontend_eof_application_context_t *application;
    atomic_bool finished;
    atomic_bool succeeded;
} frontend_eof_writer_t;

typedef struct {
    char conflict_log[FRONTEND_TEST_PATH_CAP];
    cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_version_cohort_manager_t *manager;
    cbm_daemon_runtime_service_t *service;
    cbm_daemon_runtime_client_t *client;
    frontend_eof_application_context_t application;
} frontend_eof_fixture_t;

static cbm_daemon_build_identity_t frontend_test_identity(void) {
    cbm_daemon_build_identity_t identity = {
        .semantic_version = "2.4.0",
        .build_fingerprint = FRONTEND_TEST_BUILD,
        .cache_fingerprint = FRONTEND_TEST_CACHE,
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    return identity;
}

static cbm_daemon_runtime_application_session_t *frontend_eof_application_session_open(
    void *opaque, cbm_daemon_client_id_t client_id, uint64_t authenticated_process_id) {
    frontend_eof_application_context_t *context = opaque;
    frontend_eof_application_session_t *session = calloc(1, sizeof(*session));
    if (!context || !session || client_id == CBM_DAEMON_CLIENT_ID_INVALID ||
        authenticated_process_id == 0) {
        free(session);
        return NULL;
    }
    session->context = context;
    session->session_index =
        atomic_fetch_add_explicit(&context->sessions_opened, 1, memory_order_relaxed);
    atomic_init(&session->cancelled, false);
    return (cbm_daemon_runtime_application_session_t *)session;
}

static cbm_daemon_runtime_application_status_t frontend_eof_application_request(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session,
    cbm_daemon_runtime_application_token_t request_token, const uint8_t *request,
    uint32_t request_length, uint8_t **response_out, uint32_t *response_length_out) {
    (void)request_token;
    frontend_eof_application_context_t *context = opaque;
    frontend_eof_application_session_t *session =
        (frontend_eof_application_session_t *)opaque_session;
    if (!context || !session || session->context != context || !response_out ||
        !response_length_out || (request_length > 0 && !request)) {
        return CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR;
    }
    *response_out = NULL;
    *response_length_out = 0;
    int request_index = atomic_fetch_add_explicit(&context->requests, 1, memory_order_relaxed);
    if (request_index == 0 &&
        atomic_load_explicit(&context->block_first_request, memory_order_acquire)) {
        atomic_store_explicit(&context->first_request_started, true, memory_order_release);
        while (!atomic_load_explicit(&session->cancelled, memory_order_acquire)) {
            cbm_usleep(1000);
        }
        return CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED;
    }
    if (request_length == 0) {
        return CBM_DAEMON_RUNTIME_APPLICATION_OK;
    }
    uint8_t *response = malloc(request_length);
    if (!response) {
        return CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR;
    }
    memcpy(response, request, request_length);
    *response_out = response;
    *response_length_out = request_length;
    if (context->request_observed_fd >= 0) {
        const char marker = 'Q';
        (void)write(context->request_observed_fd, &marker, 1);
    }
    return CBM_DAEMON_RUNTIME_APPLICATION_OK;
}

static void frontend_eof_application_request_cancel(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session,
    cbm_daemon_runtime_application_token_t request_token) {
    (void)request_token;
    frontend_eof_application_context_t *context = opaque;
    frontend_eof_application_session_t *session =
        (frontend_eof_application_session_t *)opaque_session;
    if (context && session && session->context == context) {
        atomic_store_explicit(&session->cancelled, true, memory_order_release);
    }
}

static void frontend_eof_application_session_cancel(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session) {
    frontend_eof_application_context_t *context = opaque;
    frontend_eof_application_session_t *session =
        (frontend_eof_application_session_t *)opaque_session;
    if (context && session && session->context == context) {
        atomic_store_explicit(&session->cancelled, true, memory_order_release);
        if (session->session_index == 0) {
            (void)atomic_fetch_add_explicit(&context->first_session_cancels, 1,
                                            memory_order_relaxed);
        } else if (session->session_index == 1) {
            (void)atomic_fetch_add_explicit(&context->second_session_cancels, 1,
                                            memory_order_relaxed);
        }
        /* Publish the total only after the exact-session counter. The daemon
         * fixture acquires this value before inspecting attribution. */
        (void)atomic_fetch_add_explicit(&context->session_cancels, 1, memory_order_release);
        if (context->session_cancel_fd >= 0) {
            const char marker = 'C';
            (void)write(context->session_cancel_fd, &marker, 1);
        }
    }
}

static void frontend_eof_application_session_close(
    void *opaque, cbm_daemon_runtime_application_session_t *opaque_session) {
    frontend_eof_application_context_t *context = opaque;
    frontend_eof_application_session_t *session =
        (frontend_eof_application_session_t *)opaque_session;
    if (context && session && session->context == context) {
        free(session);
    }
}

static bool frontend_eof_write_all(int fd, const char *bytes, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t written = write(fd, bytes + offset, length - offset);
        if (written > 0) {
            offset += (size_t)written;
        } else if (written < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

static void *frontend_eof_writer(void *opaque) {
    frontend_eof_writer_t *writer = opaque;
    static const char first[] =
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"test/block\",\"params\":{}}\n";
    bool ok = frontend_eof_write_all(writer->fd, first, sizeof(first) - 1);
    uint64_t deadline = cbm_now_ms() + FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS;
    while (
        ok &&
        !atomic_load_explicit(&writer->application->first_request_started, memory_order_acquire) &&
        cbm_now_ms() < deadline) {
        cbm_usleep(1000);
    }
    ok = ok &&
         atomic_load_explicit(&writer->application->first_request_started, memory_order_acquire);
    for (int index = 0; ok && writer->overflow && index < FRONTEND_EOF_TEST_OVERFLOW_MESSAGES;
         index++) {
        char message[160];
        int written =
            snprintf(message, sizeof(message),
                     "{\"jsonrpc\":\"2.0\",\"id\":%d,\"method\":\"test/queued\",\"params\":{}}\n",
                     index + 2);
        ok = written > 0 && written < (int)sizeof(message) &&
             frontend_eof_write_all(writer->fd, message, (size_t)written);
    }
    ok = close(writer->fd) == 0 && ok;
    writer->fd = -1;
    atomic_store_explicit(&writer->succeeded, ok, memory_order_release);
    atomic_store_explicit(&writer->finished, true, memory_order_release);
    return NULL;
}

static bool frontend_eof_fixture_start(frontend_eof_fixture_t *fixture, const char *parent) {
    memset(fixture, 0, sizeof(*fixture));
    atomic_init(&fixture->application.requests, 0);
    atomic_init(&fixture->application.session_cancels, 0);
    atomic_init(&fixture->application.sessions_opened, 0);
    atomic_init(&fixture->application.first_session_cancels, 0);
    atomic_init(&fixture->application.second_session_cancels, 0);
    atomic_init(&fixture->application.block_first_request, true);
    atomic_init(&fixture->application.first_request_started, false);
    fixture->application.request_observed_fd = -1;
    fixture->application.session_cancel_fd = -1;
    char key[CBM_DAEMON_KEY_SIZE];
    char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    int log_written = snprintf(fixture->conflict_log, sizeof(fixture->conflict_log),
                               "%s/conflicts.ndjson", parent);
    if (log_written <= 0 || log_written >= (int)sizeof(fixture->conflict_log) ||
        !cbm_daemon_rendezvous_key(key) ||
        !cbm_daemon_runtime_process_build_fingerprint((uint64_t)getpid(), build)) {
        return false;
    }
    fixture->endpoint = cbm_daemon_ipc_endpoint_new(key, parent);
    fixture->manager = fixture->endpoint ? cbm_version_cohort_manager_new(fixture->endpoint) : NULL;
    cbm_daemon_build_identity_t identity = {
        .semantic_version = "2.4.0",
        .build_fingerprint = build,
        .cache_fingerprint = FRONTEND_TEST_CACHE,
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    cbm_daemon_runtime_application_callbacks_t callbacks = {
        .context = &fixture->application,
        .session_open = frontend_eof_application_session_open,
        .request = frontend_eof_application_request,
        .request_cancel = frontend_eof_application_request_cancel,
        .session_cancel = frontend_eof_application_session_cancel,
        .session_close = frontend_eof_application_session_close,
    };
    cbm_daemon_runtime_service_config_t config = {
        .endpoint = fixture->endpoint,
        .identity = identity,
        .conflict_log_path = fixture->conflict_log,
        .conflict_log_cap_bytes = 64U * 1024U,
        .max_clients = 4,
        .lease_timeout_ms = 5000,
        .request_timeout_ms = FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS,
        .shutdown_timeout_ms = FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS,
        .application = callbacks,
    };
    fixture->service = fixture->manager ? cbm_daemon_runtime_service_start(&config) : NULL;
    cbm_daemon_runtime_connect_result_t connect_result = {0};
    fixture->client = fixture->service
                          ? cbm_daemon_runtime_client_connect(fixture->endpoint, &identity,
                                                              FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS,
                                                              &connect_result)
                          : NULL;
    return fixture->client && connect_result.status == CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED;
}

static bool frontend_eof_fixture_finish(frontend_eof_fixture_t *fixture) {
    bool ok = true;
    if (fixture->client) {
        ok = cbm_daemon_runtime_client_close(fixture->client,
                                             FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS) &&
             ok;
        fixture->client = NULL;
    }
    if (fixture->service) {
        if (cbm_daemon_runtime_service_state(fixture->service) !=
            CBM_DAEMON_RUNTIME_SERVICE_EXITED) {
            ok = cbm_daemon_runtime_service_stop(fixture->service,
                                                 FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS) &&
                 ok;
        }
        ok = cbm_daemon_runtime_service_free(fixture->service) && ok;
        fixture->service = NULL;
    }
    if (fixture->manager) {
        ok = cbm_version_cohort_manager_free(&fixture->manager) == CBM_PRIVATE_FILE_LOCK_OK && ok;
    }
    cbm_daemon_ipc_endpoint_free(fixture->endpoint);
    fixture->endpoint = NULL;
    return ok;
}

static int frontend_eof_child_run(const char *parent, bool overflow) {
    frontend_eof_fixture_t fixture;
    if (!frontend_eof_fixture_start(&fixture, parent)) {
        (void)frontend_eof_fixture_finish(&fixture);
        return 70;
    }
    int input_pipe[2] = {-1, -1};
    if (pipe(input_pipe) != 0) {
        (void)frontend_eof_fixture_finish(&fixture);
        return 71;
    }
    FILE *input = fdopen(input_pipe[0], "rb");
    FILE *output = tmpfile();
    frontend_eof_writer_t writer = {
        .fd = input_pipe[1],
        .overflow = overflow,
        .application = &fixture.application,
    };
    atomic_init(&writer.finished, false);
    atomic_init(&writer.succeeded, false);
    cbm_thread_t writer_thread;
    bool writer_started =
        input && output && cbm_thread_create(&writer_thread, 0, frontend_eof_writer, &writer) == 0;
    if (!writer_started) {
        if (input) {
            (void)fclose(input);
        } else {
            (void)close(input_pipe[0]);
        }
        (void)close(input_pipe[1]);
        if (output) {
            (void)fclose(output);
        }
        (void)frontend_eof_fixture_finish(&fixture);
        return 72;
    }

    cbm_daemon_runtime_client_t *frontend_client = fixture.client;
    fixture.client = NULL; /* cbm_daemon_frontend_mcp_run consumes it. */
    int result = cbm_daemon_frontend_mcp_run(frontend_client, fixture.manager, input, output);
    bool joined = cbm_thread_join(&writer_thread) == 0;
    bool writer_ok = atomic_load_explicit(&writer.finished, memory_order_acquire) &&
                     atomic_load_explicit(&writer.succeeded, memory_order_acquire);
    bool request_started =
        atomic_load_explicit(&fixture.application.first_request_started, memory_order_acquire);
    bool session_cancelled =
        atomic_load_explicit(&fixture.application.session_cancels, memory_order_acquire) > 0;
    bool input_closed = fclose(input) == 0;
    bool output_closed = fclose(output) == 0;
    bool streams_closed = input_closed && output_closed;
    bool fixture_closed = frontend_eof_fixture_finish(&fixture);
    bool result_matches_contract = overflow ? result < 0 : result == 0;
    return result_matches_contract && joined && writer_ok && request_started && session_cancelled &&
                   streams_closed && fixture_closed
               ? 0
               : 73;
}

static bool frontend_eof_run_isolated(const char *tag, bool overflow) {
    char parent[FRONTEND_TEST_PATH_CAP];
    int written =
        snprintf(parent, sizeof(parent), "%s/cbm-frontend-eof-%s-XXXXXX", cbm_tmpdir(), tag);
    if (written <= 0 || written >= (int)sizeof(parent) || !cbm_mkdtemp(parent)) {
        return false;
    }
    pid_t child = fork();
    if (child == 0) {
        (void)signal(SIGALRM, SIG_DFL);
        /* This alarm bounds a broken isolated harness, not product behavior.
         * Valid startup, drain/watchdog, close, and fixture teardown can exceed
         * ten seconds under sanitizers while remaining within their own bounds. */
        (void)alarm(FRONTEND_EOF_TEST_CATASTROPHIC_TIMEOUT_S);
        _exit(frontend_eof_child_run(parent, overflow));
    }
    int status = 0;
    pid_t waited;
    do {
        waited = child > 0 ? waitpid(child, &status, 0) : -1;
    } while (waited < 0 && errno == EINTR);
    bool cleaned = th_rmtree(parent) == 0;
    bool child_ok = child > 0 && waited == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    if (!child_ok) {
        if (child <= 0) {
            (void)fprintf(stderr, "frontend EOF fixture %s: fork failed\n", tag);
        } else if (waited != child) {
            (void)fprintf(stderr, "frontend EOF fixture %s: waitpid failed errno=%d\n", tag, errno);
        } else if (WIFSIGNALED(status)) {
            (void)fprintf(stderr, "frontend EOF fixture %s: child signal=%d\n", tag,
                          WTERMSIG(status));
        } else if (WIFEXITED(status)) {
            (void)fprintf(stderr, "frontend EOF fixture %s: child exit=%d\n", tag,
                          WEXITSTATUS(status));
        } else {
            (void)fprintf(stderr, "frontend EOF fixture %s: unexpected child status\n", tag);
        }
    }
    if (!cleaned) {
        (void)fprintf(stderr, "frontend EOF fixture %s: cleanup failed\n", tag);
    }
    return child_ok && cleaned;
}

static void frontend_test_release_lease(cbm_version_cohort_lease_t **lease) {
    while (lease && *lease && cbm_version_cohort_lease_release(lease) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
}

static bool frontend_test_release_lease_until(cbm_version_cohort_lease_t **lease,
                                              uint64_t deadline_ms) {
    while (lease && *lease) {
        if (cbm_version_cohort_lease_release(lease) == CBM_PRIVATE_FILE_LOCK_OK) {
            continue;
        }
        if (cbm_now_ms() >= deadline_ms) {
            return false;
        }
        cbm_usleep(1000);
    }
    return !lease || !*lease;
}

static bool frontend_test_read_byte(int fd, char *observed, uint64_t deadline_ms) {
    if (!observed) {
        return false;
    }
    *observed = '\0';
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        return false;
    }
    for (;;) {
        char value = '\0';
        ssize_t count = read(fd, &value, 1);
        if (count == 1) {
            *observed = value;
            return true;
        }
        if (count == 0 || (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK) ||
            cbm_now_ms() >= deadline_ms) {
            return false;
        }
        cbm_usleep(1000);
    }
}

static bool frontend_test_wait_byte(int fd, char expected, uint64_t deadline_ms) {
    char observed = '\0';
    return frontend_test_read_byte(fd, &observed, deadline_ms) && observed == expected;
}

typedef struct {
    int fd;
    atomic_bool finished;
    atomic_bool succeeded;
} frontend_backpressure_writer_t;

static cbm_version_cohort_quiesce_result_t frontend_test_quiesce_requested(void *context);

static bool frontend_backpressure_identity(cbm_daemon_build_identity_t *identity,
                                           const char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!identity || !build || strlen(build) != CBM_DAEMON_BUILD_FINGERPRINT_SIZE - 1) {
        return false;
    }
    *identity = (cbm_daemon_build_identity_t){
        .semantic_version = "2.4.0",
        .build_fingerprint = build,
        .cache_fingerprint = FRONTEND_TEST_CACHE,
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    return true;
}

static void *frontend_backpressure_writer(void *opaque) {
    frontend_backpressure_writer_t *writer = opaque;
    static const char prefix[] = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"test/large\","
                                 "\"params\":{\"blob\":\"";
    static const char suffix[] = "\"}}\n";
    char *message = malloc(FRONTEND_BACKPRESSURE_MESSAGE_BYTES);
    size_t prefix_length = sizeof(prefix) - 1;
    size_t suffix_length = sizeof(suffix) - 1;
    bool valid = message && prefix_length + suffix_length < FRONTEND_BACKPRESSURE_MESSAGE_BYTES;
    if (valid) {
        memcpy(message, prefix, prefix_length);
        memset(message + prefix_length, 'x',
               FRONTEND_BACKPRESSURE_MESSAGE_BYTES - prefix_length - suffix_length);
        memcpy(message + FRONTEND_BACKPRESSURE_MESSAGE_BYTES - suffix_length, suffix,
               suffix_length);
        valid = frontend_eof_write_all(writer->fd, message, FRONTEND_BACKPRESSURE_MESSAGE_BYTES);
    }
    free(message);
    valid = close(writer->fd) == 0 && valid;
    writer->fd = -1;
    atomic_store_explicit(&writer->succeeded, valid, memory_order_release);
    atomic_store_explicit(&writer->finished, true, memory_order_release);
    return NULL;
}

static int frontend_backpressure_daemon_run(const char *parent, int ready_fd, int cancel_fd,
                                            int control_fd,
                                            const char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    (void)alarm(FRONTEND_BACKPRESSURE_DAEMON_TIMEOUT_S);
    char key[CBM_DAEMON_KEY_SIZE];
    char conflict_log[FRONTEND_TEST_PATH_CAP];
    cbm_daemon_build_identity_t identity = {0};
    int log_written = snprintf(conflict_log, sizeof(conflict_log), "%s/conflicts.ndjson", parent);
    cbm_daemon_ipc_endpoint_t *endpoint =
        log_written > 0 && log_written < (int)sizeof(conflict_log) &&
                cbm_daemon_rendezvous_key(key) && frontend_backpressure_identity(&identity, build)
            ? cbm_daemon_ipc_endpoint_new(key, parent)
            : NULL;
    frontend_eof_application_context_t application;
    memset(&application, 0, sizeof(application));
    atomic_init(&application.requests, 0);
    atomic_init(&application.session_cancels, 0);
    atomic_init(&application.sessions_opened, 0);
    atomic_init(&application.first_session_cancels, 0);
    atomic_init(&application.second_session_cancels, 0);
    atomic_init(&application.block_first_request, false);
    atomic_init(&application.first_request_started, false);
    application.request_observed_fd = ready_fd;
    application.session_cancel_fd = cancel_fd;
    cbm_daemon_runtime_application_callbacks_t callbacks = {
        .context = &application,
        .session_open = frontend_eof_application_session_open,
        .request = frontend_eof_application_request,
        .request_cancel = frontend_eof_application_request_cancel,
        .session_cancel = frontend_eof_application_session_cancel,
        .session_close = frontend_eof_application_session_close,
    };
    cbm_daemon_runtime_service_config_t config = {
        .endpoint = endpoint,
        .identity = identity,
        .conflict_log_path = conflict_log,
        .conflict_log_cap_bytes = 64U * 1024U,
        .max_clients = 4,
        .lease_timeout_ms = 30000,
        .request_timeout_ms = FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS,
        .shutdown_timeout_ms = FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS,
        .application = callbacks,
    };
    cbm_daemon_runtime_service_t *service =
        endpoint ? cbm_daemon_runtime_service_start(&config) : NULL;
    const char ready = 'R';
    bool announced = service && write(ready_fd, &ready, 1) == 1;
    uint64_t cancellation_deadline = cbm_now_ms() + FRONTEND_BACKPRESSURE_RUNTIME_TIMEOUT_MS;
    while (announced &&
           atomic_load_explicit(&application.session_cancels, memory_order_acquire) == 0 &&
           cbm_now_ms() < cancellation_deadline) {
        cbm_usleep(1000);
    }
    int target_cancellations =
        atomic_load_explicit(&application.session_cancels, memory_order_acquire);
    int sentinel_cancellations_before_close =
        atomic_load_explicit(&application.first_session_cancels, memory_order_acquire);
    int target_session_cancellations =
        atomic_load_explicit(&application.second_session_cancels, memory_order_acquire);
    bool target_cancelled_exactly_once = announced && target_cancellations == 1 &&
                                         sentinel_cancellations_before_close == 0 &&
                                         target_session_cancellations == 1;
    const char checkpoint = target_cancelled_exactly_once ? 'T' : 'E';
    bool checkpoint_written = write(ready_fd, &checkpoint, 1) == 1;
    char control = '\0';
    bool shutdown_released =
        checkpoint_written &&
        frontend_test_read_byte(control_fd, &control,
                                cbm_now_ms() + FRONTEND_BACKPRESSURE_RUNTIME_TIMEOUT_MS) &&
        control == 'S';
    int final_cancellations =
        atomic_load_explicit(&application.session_cancels, memory_order_acquire);
    int sentinel_session_cancellations =
        atomic_load_explicit(&application.first_session_cancels, memory_order_acquire);
    int final_target_session_cancellations =
        atomic_load_explicit(&application.second_session_cancels, memory_order_acquire);
    bool sentinel_closed_exactly_once = final_cancellations == 2 &&
                                        sentinel_session_cancellations == 1 &&
                                        final_target_session_cancellations == 1;
    bool stopped =
        service && cbm_daemon_runtime_service_stop(service, FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS);
    bool exited = stopped && cbm_daemon_runtime_service_wait_exited(service, 0);
    bool freed = exited && cbm_daemon_runtime_service_free(service);
    cbm_daemon_ipc_endpoint_free(endpoint);
    const char done = target_cancelled_exactly_once && shutdown_released &&
                              sentinel_closed_exactly_once && stopped && exited && freed
                          ? 'D'
                          : 'E';
    bool finished = write(ready_fd, &done, 1) == 1;
    (void)close(ready_fd);
    (void)close(cancel_fd);
    (void)close(control_fd);
    return announced && target_cancelled_exactly_once && checkpoint_written && shutdown_released &&
                   sentinel_closed_exactly_once && stopped && exited && freed && finished
               ? 0
               : 80;
}

static int frontend_backpressure_frontend_run(const char *parent, int input_fd, int input_write_fd,
                                              int output_fd,
                                              const char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE],
                                              bool keep_input_open) {
    (void)alarm(FRONTEND_BACKPRESSURE_FRONTEND_TIMEOUT_S);
    char key[CBM_DAEMON_KEY_SIZE];
    cbm_daemon_build_identity_t identity = {0};
    cbm_daemon_ipc_endpoint_t *endpoint =
        cbm_daemon_rendezvous_key(key) && frontend_backpressure_identity(&identity, build)
            ? cbm_daemon_ipc_endpoint_new(key, parent)
            : NULL;
    cbm_version_cohort_manager_t *manager =
        endpoint ? cbm_version_cohort_manager_new(endpoint) : NULL;
    cbm_version_cohort_lease_t *participant = NULL;
    cbm_daemon_conflict_t conflict;
    cbm_version_cohort_status_t admitted =
        manager ? cbm_version_cohort_acquire(manager, &identity, cbm_now_ms() + 2000U, &participant,
                                             &conflict)
                : CBM_VERSION_COHORT_IO;
    cbm_daemon_runtime_connect_result_t connect_result = {0};
    cbm_daemon_runtime_client_t *client =
        admitted == CBM_VERSION_COHORT_OK
            ? cbm_daemon_runtime_client_connect(
                  endpoint, &identity, FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS, &connect_result)
            : NULL;
    FILE *input = client ? fdopen(input_fd, "rb") : NULL;
    FILE *output = input ? fdopen(output_fd, "wb") : NULL;
    int input_hold_fd = keep_input_open && output ? dup(input_write_fd) : -1;
    frontend_backpressure_writer_t writer = {
        .fd = input_write_fd,
    };
    atomic_init(&writer.finished, false);
    atomic_init(&writer.succeeded, false);
    cbm_thread_t writer_thread;
    bool writer_started =
        output && (!keep_input_open || input_hold_fd >= 0) &&
        cbm_thread_create(&writer_thread, 0, frontend_backpressure_writer, &writer) == 0;
    if (!writer_started) {
        if (client) {
            (void)cbm_daemon_runtime_client_close(client, FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS);
        }
        if (input) {
            (void)fclose(input);
        } else {
            (void)close(input_fd);
        }
        if (output) {
            (void)fclose(output);
        } else {
            (void)close(output_fd);
        }
        (void)close(input_write_fd);
        if (input_hold_fd >= 0) {
            (void)close(input_hold_fd);
        }
        frontend_test_release_lease(&participant);
        if (manager) {
            (void)cbm_version_cohort_manager_free(&manager);
        }
        cbm_daemon_ipc_endpoint_free(endpoint);
        return 81;
    }

    int result = cbm_daemon_frontend_mcp_run(client, manager, input, output);
    bool joined = cbm_thread_join(&writer_thread) == 0;
    bool writer_ok = atomic_load_explicit(&writer.finished, memory_order_acquire) &&
                     atomic_load_explicit(&writer.succeeded, memory_order_acquire);
    bool input_closed = fclose(input) == 0;
    bool output_closed = fclose(output) == 0;
    bool input_hold_closed = input_hold_fd < 0 || close(input_hold_fd) == 0;
    frontend_test_release_lease(&participant);
    bool manager_closed = cbm_version_cohort_manager_free(&manager) == CBM_PRIVATE_FILE_LOCK_OK;
    cbm_daemon_ipc_endpoint_free(endpoint);
    return result < 0 && joined && writer_ok && input_closed && output_closed &&
                   input_hold_closed && manager_closed
               ? 47
               : 82;
}

static bool frontend_backpressure_run_isolated(bool maintenance) {
    char parent[FRONTEND_TEST_PATH_CAP];
    char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    int path_written = snprintf(parent, sizeof(parent),
                                maintenance ? "%s/cbm-frontend-maintenance-backpressure-XXXXXX"
                                            : "%s/cbm-frontend-backpressure-XXXXXX",
                                cbm_tmpdir());
    int ready_pipe[2] = {-1, -1};
    int cancel_pipe[2] = {-1, -1};
    int control_pipe[2] = {-1, -1};
    bool directory_ready =
        path_written > 0 && path_written < (int)sizeof(parent) && cbm_mkdtemp(parent);
    bool identity_ready =
        directory_ready && cbm_daemon_runtime_process_build_fingerprint((uint64_t)getpid(), build);
    bool prepared = identity_ready && pipe(ready_pipe) == 0 && pipe(cancel_pipe) == 0 &&
                    pipe(control_pipe) == 0;
    if (!prepared) {
        for (size_t index = 0; index < 2; index++) {
            if (ready_pipe[index] >= 0) {
                (void)close(ready_pipe[index]);
            }
            if (cancel_pipe[index] >= 0) {
                (void)close(cancel_pipe[index]);
            }
            if (control_pipe[index] >= 0) {
                (void)close(control_pipe[index]);
            }
        }
        if (directory_ready) {
            (void)th_rmtree(parent);
        }
        return false;
    }
    pid_t daemon = prepared ? fork() : -1;
    if (daemon == 0) {
        (void)signal(SIGALRM, SIG_DFL);
        (void)close(ready_pipe[0]);
        (void)close(cancel_pipe[0]);
        (void)close(control_pipe[1]);
        _exit(frontend_backpressure_daemon_run(parent, ready_pipe[1], cancel_pipe[1],
                                               control_pipe[0], build));
    }
    if (prepared) {
        (void)close(ready_pipe[1]);
        (void)close(cancel_pipe[1]);
        (void)close(control_pipe[0]);
    }
    char announced_marker = '\0';
    /* The daemon child runs cbm_daemon_runtime_service_start before it can
     * announce, and that includes the cohort-claim path whose own internal
     * budget is HOST_DAEMON_CLAIM_TIMEOUT_MS (12s). An announce deadline
     * below that budget SIGKILLs a slow-but-legitimate startup — on
     * oversubscribed CI runners this fired every round (announced=0,
     * daemon_signal=9) while fast local machines never saw it. Keep the
     * deadline comfortably above the daemon's own worst-case budget. */
    bool announced_read = daemon > 0 && frontend_test_read_byte(ready_pipe[0], &announced_marker,
                                                                cbm_now_ms() + 30000U);
    bool announced = announced_read && announced_marker == 'R';

    /* This raw runtime client intentionally owns no cohort lease. It is a
     * sentinel for session isolation: maintenance must remove the target
     * frontend without touching this independently authenticated session. Its
     * completed admission precedes the target fork, so the daemon fixture can
     * attribute session-cancel callbacks to sentinel index 0 and target index
     * 1 rather than relying on a global callback count. */
    char sentinel_key[CBM_DAEMON_KEY_SIZE];
    cbm_daemon_build_identity_t sentinel_identity = {0};
    cbm_daemon_ipc_endpoint_t *sentinel_endpoint =
        announced && cbm_daemon_rendezvous_key(sentinel_key) &&
                frontend_backpressure_identity(&sentinel_identity, build)
            ? cbm_daemon_ipc_endpoint_new(sentinel_key, parent)
            : NULL;
    cbm_daemon_runtime_connect_result_t sentinel_connect = {0};
    cbm_daemon_runtime_client_t *sentinel =
        sentinel_endpoint ? cbm_daemon_runtime_client_connect(sentinel_endpoint, &sentinel_identity,
                                                              FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS,
                                                              &sentinel_connect)
                          : NULL;
    bool sentinel_ready =
        sentinel && sentinel_connect.status == CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED;
    if (!sentinel_ready && daemon > 0) {
        (void)kill(daemon, SIGKILL);
    }

    int input_pipe[2] = {-1, -1};
    int output_pipe[2] = {-1, -1};
    bool input_ready = sentinel_ready && pipe(input_pipe) == 0;
    bool frontend_pipes = input_ready && pipe(output_pipe) == 0;
    if (input_ready && !frontend_pipes) {
        (void)close(input_pipe[0]);
        (void)close(input_pipe[1]);
        input_pipe[0] = -1;
        input_pipe[1] = -1;
    }
    pid_t frontend = frontend_pipes ? fork() : -1;
    if (frontend == 0) {
        (void)signal(SIGALRM, SIG_DFL);
        (void)close(output_pipe[0]);
        (void)close(ready_pipe[0]);
        (void)close(cancel_pipe[0]);
        (void)close(control_pipe[1]);
        _exit(frontend_backpressure_frontend_run(parent, input_pipe[0], input_pipe[1],
                                                 output_pipe[1], build, maintenance));
    }
    if (frontend_pipes) {
        (void)close(input_pipe[0]);
        (void)close(input_pipe[1]);
        (void)close(output_pipe[1]);
    }

    char request_marker = '\0';
    bool request_read = frontend > 0 && frontend_test_read_byte(ready_pipe[0], &request_marker,
                                                                cbm_now_ms() + 4000U);
    bool request_observed = request_read && request_marker == 'Q';
    char output_marker = '\0';
    bool output_observed =
        !maintenance || (request_observed && frontend_test_read_byte(output_pipe[0], &output_marker,
                                                                     cbm_now_ms() + 4000U));

    cbm_daemon_ipc_endpoint_t *mutation_endpoint = NULL;
    cbm_version_cohort_manager_t *mutation_manager = NULL;
    cbm_version_cohort_lease_t *mutation = NULL;
    cbm_version_cohort_quiesce_result_t quiesce = CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED;
    cbm_version_cohort_status_t mutation_status = CBM_VERSION_COHORT_OK;
    if (maintenance) {
        char mutation_key[CBM_DAEMON_KEY_SIZE];
        mutation_endpoint = output_observed && cbm_daemon_rendezvous_key(mutation_key)
                                ? cbm_daemon_ipc_endpoint_new(mutation_key, parent)
                                : NULL;
        mutation_manager =
            mutation_endpoint ? cbm_version_cohort_manager_new(mutation_endpoint) : NULL;
        mutation_status = mutation_manager
                              ? cbm_version_cohort_reserve_for_mutation(
                                    mutation_manager, cbm_now_ms() + 8000U,
                                    frontend_test_quiesce_requested, NULL, &quiesce, &mutation)
                              : CBM_VERSION_COHORT_IO;
        if (mutation_status != CBM_VERSION_COHORT_OK && frontend > 0) {
            (void)kill(frontend, SIGKILL);
        }
    }

    int frontend_status = 0;
    pid_t frontend_waited;
    if (frontend > 0) {
        do {
            frontend_waited = waitpid(frontend, &frontend_status, 0);
        } while (frontend_waited < 0 && errno == EINTR);
    } else {
        frontend_waited = -1;
    }
    char cancel_marker = '\0';
    bool cancel_read = request_observed && frontend_test_read_byte(cancel_pipe[0], &cancel_marker,
                                                                   cbm_now_ms() + 4000U);
    bool cancelled = cancel_read && cancel_marker == 'C';
    char checkpoint_marker = '\0';
    bool checkpoint_read =
        frontend_test_read_byte(ready_pipe[0], &checkpoint_marker, cbm_now_ms() + 4000U);
    bool target_cancelled_exactly_once = checkpoint_read && checkpoint_marker == 'T';
    bool sentinel_usable =
        cancelled && target_cancelled_exactly_once && sentinel_ready &&
        cbm_daemon_runtime_client_heartbeat(sentinel, FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS);
    bool sentinel_closed =
        sentinel && cbm_daemon_runtime_client_close(sentinel, FRONTEND_EOF_TEST_REQUEST_TIMEOUT_MS);
    sentinel = NULL;
    cbm_daemon_ipc_endpoint_free(sentinel_endpoint);
    sentinel_endpoint = NULL;
    const char shutdown = 'S';
    bool shutdown_sent = checkpoint_read && write(control_pipe[1], &shutdown, 1) == 1;
    if (control_pipe[1] >= 0) {
        (void)close(control_pipe[1]);
        control_pipe[1] = -1;
    }
    char done_marker = '\0';
    bool done_read =
        shutdown_sent && frontend_test_read_byte(ready_pipe[0], &done_marker, cbm_now_ms() + 4000U);
    bool daemon_done = done_read && done_marker == 'D';
    if (!daemon_done && daemon > 0) {
        (void)kill(daemon, SIGKILL);
    }
    int daemon_status = 0;
    pid_t daemon_waited;
    do {
        daemon_waited = daemon > 0 ? waitpid(daemon, &daemon_status, 0) : -1;
    } while (daemon_waited < 0 && errno == EINTR);

    uint64_t cleanup_now = cbm_now_ms();
    uint64_t cleanup_deadline = cleanup_now > UINT64_MAX - FRONTEND_BACKPRESSURE_CLEANUP_TIMEOUT_MS
                                    ? UINT64_MAX
                                    : cleanup_now + FRONTEND_BACKPRESSURE_CLEANUP_TIMEOUT_MS;
    bool mutation_released = frontend_test_release_lease_until(&mutation, cleanup_deadline);
    while (mutation_released && mutation_manager && cbm_now_ms() < cleanup_deadline &&
           cbm_version_cohort_manager_free(&mutation_manager) != CBM_PRIVATE_FILE_LOCK_OK) {
        cbm_usleep(1000);
    }
    bool mutation_manager_closed = mutation_manager == NULL;
    cbm_daemon_ipc_endpoint_free(mutation_endpoint);

    if (output_pipe[0] >= 0) {
        (void)close(output_pipe[0]);
    }
    if (ready_pipe[0] >= 0) {
        (void)close(ready_pipe[0]);
    }
    if (cancel_pipe[0] >= 0) {
        (void)close(cancel_pipe[0]);
    }
    if (control_pipe[1] >= 0) {
        (void)close(control_pipe[1]);
    }
    bool cleaned = prepared && th_rmtree(parent) == 0;
    bool frontend_exited_boundedly = frontend_waited == frontend && WIFEXITED(frontend_status) &&
                                     (maintenance ? WEXITSTATUS(frontend_status) == EXIT_SUCCESS
                                                  : (WEXITSTATUS(frontend_status) == EXIT_FAILURE ||
                                                     WEXITSTATUS(frontend_status) == 47));
    bool daemon_clean =
        daemon_waited == daemon && WIFEXITED(daemon_status) && WEXITSTATUS(daemon_status) == 0;
    bool maintenance_completed =
        !maintenance || (output_observed && mutation_status == CBM_VERSION_COHORT_OK &&
                         quiesce == CBM_VERSION_COHORT_QUIESCE_REQUESTED);
    bool passed = prepared && announced && sentinel_ready && frontend_pipes && frontend > 0 &&
                  request_observed && maintenance_completed && frontend_exited_boundedly &&
                  cancelled && target_cancelled_exactly_once && sentinel_usable &&
                  sentinel_closed && shutdown_sent && daemon_done && daemon_clean &&
                  mutation_released && mutation_manager_closed && cleaned;
    if (!passed) {
        bool frontend_reaped = frontend > 0 && frontend_waited == frontend;
        bool daemon_reaped = daemon > 0 && daemon_waited == daemon;
        int frontend_exit =
            frontend_reaped && WIFEXITED(frontend_status) ? WEXITSTATUS(frontend_status) : -1;
        int frontend_signal =
            frontend_reaped && WIFSIGNALED(frontend_status) ? WTERMSIG(frontend_status) : 0;
        int daemon_exit =
            daemon_reaped && WIFEXITED(daemon_status) ? WEXITSTATUS(daemon_status) : -1;
        int daemon_signal =
            daemon_reaped && WIFSIGNALED(daemon_status) ? WTERMSIG(daemon_status) : 0;
        fprintf(stderr,
                "frontend backpressure fixture failed: maintenance=%d prepared=%d daemon_pid=%ld "
                "announced=%d announced_marker=0x%02x sentinel_ready=%d frontend_pipes=%d "
                "frontend_pid=%ld frontend_reaped=%d frontend_status=0x%x "
                "frontend_exit=%d frontend_signal=%d request_read=%d "
                "request_marker=0x%02x output_observed=%d output_marker=0x%02x "
                "mutation_status=%d quiesce=%d cancel_read=%d cancel_marker=0x%02x "
                "checkpoint_read=%d checkpoint_marker=0x%02x sentinel_usable=%d "
                "sentinel_closed=%d shutdown_sent=%d "
                "done_read=%d done_marker=0x%02x daemon_reaped=%d "
                "daemon_status=0x%x daemon_exit=%d daemon_signal=%d "
                "mutation_released=%d mutation_manager_closed=%d cleaned=%d\n",
                maintenance, prepared, (long)daemon, announced,
                (unsigned int)(unsigned char)announced_marker, sentinel_ready, frontend_pipes,
                (long)frontend, frontend_reaped, frontend_status, frontend_exit, frontend_signal,
                request_read, (unsigned int)(unsigned char)request_marker, output_observed,
                (unsigned int)(unsigned char)output_marker, mutation_status, quiesce, cancel_read,
                (unsigned int)(unsigned char)cancel_marker, checkpoint_read,
                (unsigned int)(unsigned char)checkpoint_marker, sentinel_usable, sentinel_closed,
                shutdown_sent, done_read, (unsigned int)(unsigned char)done_marker, daemon_reaped,
                daemon_status, daemon_exit, daemon_signal, mutation_released,
                mutation_manager_closed, cleaned);
    }
    return passed;
}

static cbm_version_cohort_quiesce_result_t frontend_test_quiesce_requested(void *context) {
    (void)context;
    return CBM_VERSION_COHORT_QUIESCE_REQUESTED;
}

static bool frontend_test_cancel_active(void *context) {
    int fd = *(int *)context;
    const char marker = 'C';
    return write(fd, &marker, 1) == 1;
}

static bool frontend_test_cancel_and_mark(void *context) {
    atomic_bool *cancelled = context;
    atomic_store_explicit(cancelled, true, memory_order_release);
    return true;
}
#endif

TEST(daemon_frontend_recognizes_exact_cancellation_notification) {
    ASSERT_TRUE(cbm_daemon_frontend_is_cancellation_notification(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\","
        "\"params\":{\"requestId\":7}}"));
    ASSERT_TRUE(cbm_daemon_frontend_is_cancellation_notification(
        "{ \"params\": {}, \"method\": \"notifications/cancelled\", "
        "\"jsonrpc\": \"2.0\" }"));
    PASS();
}

/* RED on the whole-session cancellation shortcut: merely recognizing the
 * notification method is not authority to close a session.  The requestId
 * must match the exact numeric/string identity currently being executed. */
TEST(daemon_frontend_correlates_cancellation_to_exact_request) {
    ASSERT_TRUE(cbm_daemon_frontend_cancellation_matches_request(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\","
        "\"params\":{\"requestId\":7}}",
        7, NULL));
    ASSERT_FALSE(cbm_daemon_frontend_cancellation_matches_request(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\","
        "\"params\":{\"requestId\":8}}",
        7, NULL));
    ASSERT_TRUE(cbm_daemon_frontend_cancellation_matches_request(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\","
        "\"params\":{\"requestId\":\"request-7\"}}",
        -1, "request-7"));
    ASSERT_FALSE(cbm_daemon_frontend_cancellation_matches_request(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\","
        "\"params\":{\"requestId\":7}}",
        -1, "7"));
    ASSERT_FALSE(cbm_daemon_frontend_cancellation_matches_request(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled\","
        "\"params\":{}}",
        7, NULL));
    ASSERT_FALSE(
        cbm_daemon_frontend_cancellation_matches_request("{\"jsonrpc\":\"2.0\",\"id\":9,"
                                                         "\"method\":\"notifications/cancelled\","
                                                         "\"params\":{\"requestId\":7}}",
                                                         7, NULL));
    PASS();
}

TEST(daemon_frontend_ignores_cancellation_text_in_string_content) {
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification(
        "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"tools/call\","
        "\"params\":{\"name\":\"search_graph\",\"arguments\":{\"query\":"
        "\"notifications/cancelled\"}}}"));
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification(
        "{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\","
        "\"params\":{\"method\":\"notifications/cancelled\"}}"));
    PASS();
}

TEST(daemon_frontend_rejects_non_notification_cancellation_shapes) {
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification(
        "{\"jsonrpc\":\"2.0\",\"id\":3,"
        "\"method\":\"notifications/cancelled\"}"));
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification(
        "{\"jsonrpc\":\"2.0\",\"method\":\"notifications/cancelled-extra\"}"));
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification(
        "{\"jsonrpc\":\"2.0\",\"method\":\"prefix/notifications/cancelled\"}"));
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification(
        "{\"jsonrpc\":\"2.0\",\"params\":{\"method\":"
        "\"notifications/cancelled\"}}"));
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification("not-json"));
    ASSERT_FALSE(cbm_daemon_frontend_is_cancellation_notification(NULL));
    PASS();
}

#ifndef _WIN32
/* RED: an MCP frontend's main thread can remain blocked forever in stdio while
 * install/update/uninstall owns maintenance intent. The already-present
 * frontend worker must observe that native intent and terminate this stateless
 * child, releasing its cohort lease and kernel IPC ownership. A deliberately
 * invalid client pointer is safe only if the maintenance path exits before
 * ordinary EOF/session-close handling, which also pins that exact path. */
TEST(daemon_frontend_maintenance_exits_while_stdio_reader_is_blocked) {
    frontend_maintenance_fixture_t fixture;
    ASSERT_TRUE(frontend_maintenance_fixture_start(&fixture, "blocked-stdio"));
    int input_pipe[2] = {-1, -1};
    int ready_pipe[2] = {-1, -1};
    bool pipes_ready = pipe(input_pipe) == 0 && pipe(ready_pipe) == 0;
    pid_t child = pipes_ready ? fork() : -1;
    if (child == 0) {
        (void)close(input_pipe[1]);
        (void)close(ready_pipe[0]);
        cbm_daemon_ipc_endpoint_t *endpoint =
            cbm_daemon_ipc_endpoint_new("0123456789abcdef", fixture.parent);
        cbm_version_cohort_manager_t *manager =
            endpoint ? cbm_version_cohort_manager_new(endpoint) : NULL;
        cbm_version_cohort_lease_t *participant = NULL;
        cbm_daemon_conflict_t conflict;
        cbm_daemon_build_identity_t identity = frontend_test_identity();
        cbm_version_cohort_status_t admitted =
            manager ? cbm_version_cohort_acquire(manager, &identity, cbm_now_ms() + 2000U,
                                                 &participant, &conflict)
                    : CBM_VERSION_COHORT_IO;
        FILE *input = admitted == CBM_VERSION_COHORT_OK ? fdopen(input_pipe[0], "rb") : NULL;
        FILE *output = input ? tmpfile() : NULL;
        const char ready = 'R';
        bool announced = output && write(ready_pipe[1], &ready, 1) == 1;
        (void)close(ready_pipe[1]);
        if (!announced) {
            _exit(70);
        }
        int result = cbm_daemon_frontend_mcp_run((cbm_daemon_runtime_client_t *)(uintptr_t)1,
                                                 manager, input, output);
        (void)result;
        _exit(71);
    }

    if (pipes_ready) {
        (void)close(input_pipe[0]);
        (void)close(ready_pipe[1]);
    }
    bool announced = child > 0 && frontend_test_wait_byte(ready_pipe[0], 'R', cbm_now_ms() + 2000U);
    cbm_version_cohort_lease_t *mutation = NULL;
    cbm_version_cohort_quiesce_result_t quiesce = CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED;
    cbm_version_cohort_status_t status =
        announced ? cbm_version_cohort_reserve_for_mutation(fixture.manager, cbm_now_ms() + 3000U,
                                                            frontend_test_quiesce_requested, NULL,
                                                            &quiesce, &mutation)
                  : CBM_VERSION_COHORT_IO;
    if (status != CBM_VERSION_COHORT_OK && child > 0) {
        (void)kill(child, SIGKILL);
    }
    int child_status = 0;
    bool waited = child > 0 && waitpid(child, &child_status, 0) == child;
    if (pipes_ready) {
        (void)close(input_pipe[1]);
        (void)close(ready_pipe[0]);
    }
    frontend_test_release_lease(&mutation);
    frontend_maintenance_fixture_finish(&fixture);

    ASSERT_TRUE(pipes_ready);
    ASSERT_TRUE(child > 0);
    ASSERT_TRUE(announced);
    ASSERT_EQ(status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(quiesce, CBM_VERSION_COHORT_QUIESCE_REQUESTED);
    ASSERT_TRUE(waited);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    PASS();
}

/* RED: local CLI/worker work has no standing daemon session to interrupt. Its
 * command-lifetime monitor must request cooperative cancellation once, allow a
 * bounded grace, then hard-exit the isolated process so SQLite/native locks
 * roll back and the mutation barrier can prove that every lifetime participant
 * is gone. */
TEST(daemon_local_participant_monitor_cancels_then_bounds_active_operation) {
    frontend_maintenance_fixture_t fixture;
    ASSERT_TRUE(frontend_maintenance_fixture_start(&fixture, "local-monitor"));
    int ready_pipe[2] = {-1, -1};
    int cancel_pipe[2] = {-1, -1};
    bool pipes_ready = pipe(ready_pipe) == 0 && pipe(cancel_pipe) == 0;
    pid_t child = pipes_ready ? fork() : -1;
    if (child == 0) {
        (void)close(ready_pipe[0]);
        (void)close(cancel_pipe[0]);
        cbm_daemon_ipc_endpoint_t *endpoint =
            cbm_daemon_ipc_endpoint_new("0123456789abcdef", fixture.parent);
        cbm_version_cohort_manager_t *manager =
            endpoint ? cbm_version_cohort_manager_new(endpoint) : NULL;
        cbm_version_cohort_lease_t *participant = NULL;
        cbm_daemon_conflict_t conflict;
        cbm_daemon_build_identity_t identity = frontend_test_identity();
        cbm_version_cohort_status_t admitted =
            manager ? cbm_version_cohort_acquire(manager, &identity, cbm_now_ms() + 2000U,
                                                 &participant, &conflict)
                    : CBM_VERSION_COHORT_IO;
        cbm_daemon_maintenance_monitor_t *monitor =
            admitted == CBM_VERSION_COHORT_OK
                ? cbm_daemon_maintenance_monitor_start(manager, frontend_test_cancel_active,
                                                       &cancel_pipe[1], 37, "test-local-operation")
                : NULL;
        const char ready = 'R';
        bool announced = monitor && write(ready_pipe[1], &ready, 1) == 1;
        (void)close(ready_pipe[1]);
        if (!announced) {
            _exit(72);
        }
        for (;;) {
            cbm_usleep(100000);
        }
    }

    if (pipes_ready) {
        (void)close(ready_pipe[1]);
        (void)close(cancel_pipe[1]);
    }
    bool announced = child > 0 && frontend_test_wait_byte(ready_pipe[0], 'R', cbm_now_ms() + 2000U);
    cbm_version_cohort_lease_t *mutation = NULL;
    cbm_version_cohort_quiesce_result_t quiesce = CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED;
    cbm_version_cohort_status_t status =
        announced ? cbm_version_cohort_reserve_for_mutation(fixture.manager, cbm_now_ms() + 5000U,
                                                            frontend_test_quiesce_requested, NULL,
                                                            &quiesce, &mutation)
                  : CBM_VERSION_COHORT_IO;
    bool cancelled = status == CBM_VERSION_COHORT_OK &&
                     frontend_test_wait_byte(cancel_pipe[0], 'C', cbm_now_ms() + 1000U);
    if (status != CBM_VERSION_COHORT_OK && child > 0) {
        (void)kill(child, SIGKILL);
    }
    int child_status = 0;
    bool waited = child > 0 && waitpid(child, &child_status, 0) == child;
    if (pipes_ready) {
        (void)close(ready_pipe[0]);
        (void)close(cancel_pipe[0]);
    }
    frontend_test_release_lease(&mutation);
    frontend_maintenance_fixture_finish(&fixture);

    ASSERT_TRUE(pipes_ready);
    ASSERT_TRUE(child > 0);
    ASSERT_TRUE(announced);
    ASSERT_EQ(status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(quiesce, CBM_VERSION_COHORT_QUIESCE_REQUESTED);
    ASSERT_TRUE(cancelled);
    ASSERT_TRUE(waited);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 37);
    PASS();
}

/* RED: supervised process-tree cancellation permits one second of graceful
 * shutdown followed by one second of forced containment. The maintenance
 * observer must not _Exit the owning process before that bounded supervisor
 * window can finish and join the observer normally. */
TEST(daemon_local_participant_monitor_allows_supervisor_containment_window) {
    frontend_maintenance_fixture_t fixture;
    ASSERT_TRUE(frontend_maintenance_fixture_start(&fixture, "monitor-window"));
    int ready_pipe[2] = {-1, -1};
    bool pipe_ready = pipe(ready_pipe) == 0;
    pid_t child = pipe_ready ? fork() : -1;
    if (child == 0) {
        (void)close(ready_pipe[0]);
        cbm_daemon_ipc_endpoint_t *endpoint =
            cbm_daemon_ipc_endpoint_new("0123456789abcdef", fixture.parent);
        cbm_version_cohort_manager_t *manager =
            endpoint ? cbm_version_cohort_manager_new(endpoint) : NULL;
        cbm_version_cohort_lease_t *participant = NULL;
        cbm_daemon_conflict_t conflict;
        cbm_daemon_build_identity_t identity = frontend_test_identity();
        cbm_version_cohort_status_t admitted =
            manager ? cbm_version_cohort_acquire(manager, &identity, cbm_now_ms() + 2000U,
                                                 &participant, &conflict)
                    : CBM_VERSION_COHORT_IO;
        atomic_bool cancelled;
        atomic_init(&cancelled, false);
        cbm_daemon_maintenance_monitor_t *monitor =
            admitted == CBM_VERSION_COHORT_OK
                ? cbm_daemon_maintenance_monitor_start(manager, frontend_test_cancel_and_mark,
                                                       &cancelled, 38, "test-supervisor-window")
                : NULL;
        const char ready = 'R';
        bool announced = monitor && write(ready_pipe[1], &ready, 1) == 1;
        (void)close(ready_pipe[1]);
        if (!announced) {
            _exit(72);
        }
        while (!atomic_load_explicit(&cancelled, memory_order_acquire)) {
            cbm_usleep(1000);
        }

        /* Model the maximum graceful + forced-settle supervisor bounds. */
        cbm_usleep(2100U * 1000U);
        bool stopped = cbm_daemon_maintenance_monitor_stop(&monitor);
        frontend_test_release_lease(&participant);
        while (manager && cbm_version_cohort_manager_free(&manager) != CBM_PRIVATE_FILE_LOCK_OK) {
            cbm_usleep(1000);
        }
        cbm_daemon_ipc_endpoint_free(endpoint);
        _exit(stopped ? 0 : 73);
    }

    if (pipe_ready) {
        (void)close(ready_pipe[1]);
    }
    bool announced = child > 0 && frontend_test_wait_byte(ready_pipe[0], 'R', cbm_now_ms() + 2000U);
    cbm_version_cohort_lease_t *mutation = NULL;
    cbm_version_cohort_quiesce_result_t quiesce = CBM_VERSION_COHORT_QUIESCE_NOT_NEEDED;
    cbm_version_cohort_status_t status =
        announced ? cbm_version_cohort_reserve_for_mutation(fixture.manager, cbm_now_ms() + 5000U,
                                                            frontend_test_quiesce_requested, NULL,
                                                            &quiesce, &mutation)
                  : CBM_VERSION_COHORT_IO;
    if (status != CBM_VERSION_COHORT_OK && child > 0) {
        (void)kill(child, SIGKILL);
    }
    int child_status = 0;
    bool waited = child > 0 && waitpid(child, &child_status, 0) == child;
    if (pipe_ready) {
        (void)close(ready_pipe[0]);
    }
    frontend_test_release_lease(&mutation);
    frontend_maintenance_fixture_finish(&fixture);

    ASSERT_TRUE(pipe_ready);
    ASSERT_TRUE(child > 0);
    ASSERT_TRUE(announced);
    ASSERT_EQ(status, CBM_VERSION_COHORT_OK);
    ASSERT_EQ(quiesce, CBM_VERSION_COHORT_QUIESCE_REQUESTED);
    ASSERT_TRUE(waited);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    PASS();
}

/* RED: once a long request is active, a buffered writer can fill the bounded
 * frontend queue and close its pipe. Waiting indefinitely for queue capacity
 * prevents the sole reader from observing that already-pending EOF, so neither
 * the request nor its daemon session is cancelled. Overload must instead fail
 * the frontend promptly and close/cancel the authenticated session. */
TEST(daemon_frontend_over_capacity_input_cannot_hide_eof_behind_active_request) {
    ASSERT_TRUE(frontend_eof_run_isolated("overflow", true));
    PASS();
}

/* Clean EOF is the normal MCP-session ownership boundary. Accepted work gets a
 * bounded drain opportunity; work still active at the deadline is cancelled
 * with that exact session, while the frontend itself reports a clean close. */
TEST(daemon_frontend_eof_drain_timeout_cancels_and_returns_success) {
    ASSERT_TRUE(frontend_eof_run_isolated("timeout", false));
    PASS();
}

/* A daemon response can finish its IPC exchange and then block forever while
 * writing to an agent that stopped reading stdout. EOF must still bound the
 * thin frontend process. Keep the daemon in a separate child so the frontend's
 * terminal watchdog closes a real kernel connection; the daemon must observe
 * that close, cancel the exact session, and shut down normally. */
TEST(daemon_frontend_stdout_backpressure_eof_fail_stops_and_cancels_session) {
    ASSERT_TRUE(frontend_backpressure_run_isolated(false));
    PASS();
}

/* Maintenance must not depend on either MCP stdio thread making progress.
 * Keep stdin open, fill stdout with a completed daemon response, then publish
 * activation intent. The independent monitor must release the cohort lease and
 * kernel session within its bound; the daemon must cancel that exact session. */
TEST(daemon_frontend_stdout_backpressure_maintenance_stops_and_cancels_session) {
    ASSERT_TRUE(frontend_backpressure_run_isolated(true));
    PASS();
}
#endif

TEST(daemon_local_participant_monitor_joins_before_manager_teardown) {
    frontend_maintenance_fixture_t fixture;
    ASSERT_TRUE(frontend_maintenance_fixture_start(&fixture, "monitor-join"));
    cbm_daemon_maintenance_monitor_t *monitor = cbm_daemon_maintenance_monitor_start(
        fixture.manager, NULL, NULL, EXIT_FAILURE, "test-idle-command");
    bool started = monitor != NULL;
    bool stopped = started && cbm_daemon_maintenance_monitor_stop(&monitor);
    bool consumed = monitor == NULL;
    frontend_maintenance_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(stopped);
    ASSERT_TRUE(consumed);
    PASS();
}

SUITE(daemon_frontend) {
    RUN_TEST(daemon_frontend_recognizes_exact_cancellation_notification);
    RUN_TEST(daemon_frontend_correlates_cancellation_to_exact_request);
    RUN_TEST(daemon_frontend_ignores_cancellation_text_in_string_content);
    RUN_TEST(daemon_frontend_rejects_non_notification_cancellation_shapes);
#ifndef _WIN32
    RUN_TEST(daemon_frontend_maintenance_exits_while_stdio_reader_is_blocked);
    RUN_TEST(daemon_local_participant_monitor_cancels_then_bounds_active_operation);
    RUN_TEST(daemon_local_participant_monitor_allows_supervisor_containment_window);
    RUN_TEST(daemon_frontend_over_capacity_input_cannot_hide_eof_behind_active_request);
    RUN_TEST(daemon_frontend_eof_drain_timeout_cancels_and_returns_success);
    RUN_TEST(daemon_frontend_stdout_backpressure_eof_fail_stops_and_cancels_session);
    RUN_TEST(daemon_frontend_stdout_backpressure_maintenance_stops_and_cancels_session);
#endif
    RUN_TEST(daemon_local_participant_monitor_joins_before_manager_teardown);
}
