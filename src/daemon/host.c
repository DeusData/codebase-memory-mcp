/*
 * host.c — Lifecycle owner for the mandatory per-account CBM daemon.
 */
#include "daemon/host.h"
#include "daemon/host_internal.h"

#include "daemon/application.h"
#include "daemon/runtime.h"
#include "daemon/project_lock.h"
#include "daemon/version_cohort.h"
#include "cli/cli.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/diagnostics.h"
#include "foundation/log.h"
#include "foundation/mem.h"
#include "foundation/platform.h"
#include "mcp/index_supervisor.h"
#include "store/store.h"
#include "ui/config.h"
#include "ui/embedded_assets.h"
#include "ui/http_server.h"
#include "watcher/watcher.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    HOST_MAX_CLIENTS = 64,
    HOST_LEASE_TIMEOUT_MS = 30000,
    HOST_REQUEST_TIMEOUT_MS = 5000,
    HOST_RUNTIME_SHUTDOWN_MS = 10000,
    HOST_APPLICATION_SHUTDOWN_MS = 10000,
    HOST_COORDINATION_CLEANUP_MS = 500,
    HOST_INITIAL_CLIENT_TIMEOUT_MS = 10000,
    /* A just-superseded daemon generation can still hold the cohort claim for
     * the brief window between going client-terminal (which lets the next
     * generation start) and releasing its cohort locks. Wait out that handoff
     * rather than failing startup on a transient BUSY. Bounded well under the
     * client's 30 s daemon-start budget. */
    HOST_DAEMON_CLAIM_TIMEOUT_MS = 12000,
    HOST_WAIT_TICK_MS = 100,
    HOST_HTTP_CONFIG_POLL_MS = 1000,
    HOST_HTTP_RETRY_INITIAL_MS = 1000,
    HOST_HTTP_RETRY_MAX_MS = 30000,
    HOST_WATCH_INTERVAL_MS = 5000,
    HOST_CONFLICT_LOG_CAP = 1024 * 1024,
    HOST_OPERATION_LOG_CAP = 5 * 1024 * 1024,
    HOST_PATH_CAP = 4096,
#ifdef _WIN32
    /* LockFileEx has no non-owning F_GETLK equivalent. A lifetime probe must
     * momentarily take and release the private file range. The child can also
     * briefly lose the legacy-mutex handoff to another waiting starter, so it
     * retries both bounded observation/compatibility windows. */
    HOST_WINDOWS_LIFETIME_ACQUIRE_MS = 250,
#endif
};

typedef struct host_state host_state_t;

typedef struct {
    void *context;
    void (*config_load)(void *context, cbm_ui_config_t *config_out);
    cbm_http_server_t *(*server_new)(void *context, int port);
    void (*server_configure)(void *context, cbm_http_server_t *server, host_state_t *host);
    void (*server_stop)(void *context, cbm_http_server_t *server);
    bool (*server_free)(void *context, cbm_http_server_t *server);
    int (*thread_start)(void *context, cbm_thread_t *thread, void *(*entry)(void *),
                        void *entry_context);
    int (*thread_join)(void *context, cbm_thread_t *thread);
} host_http_ops_t;

struct host_state {
    /* Mirrors cbm_daemon_host_config_t.permanent for prepare-time consumers. */
    bool permanent;
    cbm_daemon_application_t *application;
    cbm_watcher_t *watcher;
    cbm_store_t *watch_store;
    cbm_config_t *runtime_config;
    cbm_project_lock_manager_t *project_locks;
    cbm_http_server_t *http;
    cbm_thread_t watcher_thread;
    cbm_thread_t http_thread;
    bool watcher_started;
    bool http_started;
    bool http_retiring;
    bool http_config_loaded;
    bool http_config_enabled;
    bool http_assets_available;
    int http_config_port;
    uint64_t http_next_config_load_ms;
    uint64_t http_retry_at_ms;
    uint32_t http_retry_delay_ms;
    uint32_t http_largest_scheduled_retry_ms;
    const host_http_ops_t *http_ops;
};

static FILE *g_host_log_file = NULL;
static cbm_mutex_t g_host_log_mutex;
static bool g_host_log_mutex_initialized = false;

static _Noreturn void host_force_terminate(const char *component);

static void host_log_sink(const char *line) {
    cbm_ui_log_append(line);
    if (!g_host_log_file || !g_host_log_mutex_initialized) {
        return;
    }
    cbm_mutex_lock(&g_host_log_mutex);
    (void)fprintf(g_host_log_file, "%s\n", line ? line : "");
    (void)fflush(g_host_log_file);
    cbm_mutex_unlock(&g_host_log_mutex);
}

static bool host_log_open(char conflict_log_out[HOST_PATH_CAP]) {
    const char *cache = cbm_resolve_cache_dir();
    if (!cache || !cache[0] || !conflict_log_out) {
        return false;
    }
    char logs[HOST_PATH_CAP];
    int written = snprintf(logs, sizeof(logs), "%s/logs", cache);
    if (written <= 0 || written >= (int)sizeof(logs)) {
        return false;
    }
    written = snprintf(conflict_log_out, HOST_PATH_CAP, "%s/daemon-conflicts.ndjson", logs);
    if (written <= 0 || written >= HOST_PATH_CAP) {
        return false;
    }
    g_host_log_file =
        cbm_daemon_ipc_private_log_open(logs, "cbm-daemon.log", HOST_OPERATION_LOG_CAP);
    if (!g_host_log_file) {
        conflict_log_out[0] = '\0';
        return false;
    }
    cbm_mutex_init(&g_host_log_mutex);
    g_host_log_mutex_initialized = true;
    cbm_log_set_sink(host_log_sink);
    return true;
}

static void host_log_close(void) {
    cbm_log_set_sink(NULL);
    if (g_host_log_mutex_initialized) {
        cbm_mutex_lock(&g_host_log_mutex);
    }
    FILE *file = g_host_log_file;
    g_host_log_file = NULL;
    if (file) {
        (void)fclose(file);
    }
    if (g_host_log_mutex_initialized) {
        cbm_mutex_unlock(&g_host_log_mutex);
        cbm_mutex_destroy(&g_host_log_mutex);
        g_host_log_mutex_initialized = false;
    }
}

static uint64_t host_deadline_after(uint32_t timeout_ms) {
    uint64_t now = cbm_now_ms();
    return now > UINT64_MAX - timeout_ms ? UINT64_MAX : now + timeout_ms;
}

static uint64_t host_current_process_id(void) {
#ifdef _WIN32
    return (uint64_t)GetCurrentProcessId();
#else
    return (uint64_t)getpid();
#endif
}

static _Noreturn void host_cleanup_force_terminate(const char *component) {
    /* Early startup failures can precede the ordinary operation-log setup.
     * Make one best-effort attempt to establish that same owner-only durable
     * sink before the process-level escape releases every native claim. */
    if (!g_host_log_file) {
        char conflict_log[HOST_PATH_CAP];
        cbm_ui_log_init();
        (void)host_log_open(conflict_log);
    }
    host_force_terminate(component);
}

static void host_cleanup_release_until_complete(cbm_daemon_host_cleanup_release_for_test_fn release,
                                                void *context, const char *component) {
    if (!release) {
        return;
    }
    uint64_t deadline = host_deadline_after(HOST_COORDINATION_CLEANUP_MS);
    while (!release(context)) {
        if (cbm_now_ms() >= deadline) {
            host_cleanup_force_terminate(component);
        }
        cbm_usleep(1000);
    }
}

void cbm_daemon_host_cleanup_release_until_complete_for_test(
    cbm_daemon_host_cleanup_release_for_test_fn release, void *context) {
    host_cleanup_release_until_complete(release, context, "coordination_cleanup");
}

static bool host_cohort_lease_release_once(void *context) {
    cbm_version_cohort_lease_t **lease = context;
    if (!lease || !*lease) {
        return true;
    }
    (void)cbm_version_cohort_lease_release(lease);
    return *lease == NULL;
}

static bool host_cohort_manager_free_once(void *context) {
    cbm_version_cohort_manager_t **manager = context;
    if (!manager || !*manager) {
        return true;
    }
    (void)cbm_version_cohort_manager_free(manager);
    return *manager == NULL;
}

static void host_cohort_close(cbm_version_cohort_lease_t **lease,
                              cbm_version_cohort_manager_t **manager) {
    host_cleanup_release_until_complete(host_cohort_lease_release_once, lease,
                                        "cohort_lease_cleanup");
    host_cleanup_release_until_complete(host_cohort_manager_free_once, manager,
                                        "cohort_manager_cleanup");
}

static bool host_daemon_claim_release_once(void *context) {
    cbm_version_cohort_daemon_claim_t **claim = context;
    if (!claim || !*claim) {
        return true;
    }
    (void)cbm_version_cohort_daemon_claim_release(claim);
    return *claim == NULL;
}

static void host_daemon_claim_close(cbm_version_cohort_daemon_claim_t **claim) {
    host_cleanup_release_until_complete(host_daemon_claim_release_once, claim,
                                        "daemon_claim_cleanup");
}

static bool host_participant_guard_release_once(void *context) {
    cbm_daemon_ipc_participant_guard_t **guard = context;
    if (!guard || !*guard) {
        return true;
    }
    (void)cbm_daemon_ipc_participant_guard_release(guard);
    return *guard == NULL;
}

static void host_participant_guard_close(cbm_daemon_ipc_participant_guard_t **guard) {
    host_cleanup_release_until_complete(host_participant_guard_release_once, guard,
                                        "participant_guard_cleanup");
}

static int host_lifetime_reservation_acquire(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    cbm_daemon_ipc_lifetime_reservation_t **reservation_out) {
#ifdef _WIN32
    uint64_t now = cbm_now_ms();
    uint64_t deadline = now > UINT64_MAX - HOST_WINDOWS_LIFETIME_ACQUIRE_MS
                            ? UINT64_MAX
                            : now + HOST_WINDOWS_LIFETIME_ACQUIRE_MS;
    do {
        int status = cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, reservation_out);
        if (status != 0) {
            return status;
        }
        cbm_usleep(1000);
    } while (cbm_now_ms() < deadline);
    return 0;
#else
    return cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, reservation_out);
#endif
}

static void *host_watcher_thread(void *opaque) {
    cbm_watcher_run(opaque, HOST_WATCH_INTERVAL_MS);
    return NULL;
}

static void *host_http_thread(void *opaque) {
    cbm_http_server_run(opaque);
    return NULL;
}

static int host_watcher_index(const char *project_name, const char *root_path, void *opaque) {
    host_state_t *host = opaque;
    return host && host->application
               ? cbm_daemon_application_watcher_index(project_name, root_path, host->application)
               : -1;
}

static int host_ui_index(void *opaque, const char *root_path, const char *project_name) {
    host_state_t *host = opaque;
    return host && host->application
               ? cbm_daemon_application_index(host->application, project_name ? project_name : "",
                                              root_path)
               : -1;
}

static bool host_ui_mutation_begin(void *opaque, const char *project) {
    host_state_t *host = opaque;
    return host && host->application &&
           cbm_daemon_application_project_mutation_try_begin(host->application, project);
}

static void host_ui_mutation_end(void *opaque, const char *project) {
    host_state_t *host = opaque;
    if (host && host->application) {
        cbm_daemon_application_project_mutation_end(host->application, project);
    }
}

static void host_http_config_load_default(void *context, cbm_ui_config_t *config_out) {
    (void)context;
    cbm_ui_config_load(config_out);
}

static cbm_http_server_t *host_http_server_new_default(void *context, int port) {
    (void)context;
    return cbm_http_server_new(port);
}

static void host_http_server_configure_default(void *context, cbm_http_server_t *server,
                                               host_state_t *host) {
    (void)context;
    cbm_http_server_set_watcher(server, host->watcher);
    cbm_http_server_set_index_executor(server, host_ui_index, host);
    cbm_http_server_set_project_mutation_guard(server, host_ui_mutation_begin, host_ui_mutation_end,
                                               host);
}

static void host_http_server_stop_default(void *context, cbm_http_server_t *server) {
    (void)context;
    cbm_http_server_stop(server);
}

static bool host_http_server_free_default(void *context, cbm_http_server_t *server) {
    (void)context;
    return cbm_http_server_free(server);
}

typedef int (*host_http_thread_create_fn)(void *context, cbm_thread_t *thread,
                                          void *(*entry)(void *), void *entry_context);

static int host_http_thread_create_default(void *context, cbm_thread_t *thread,
                                           void *(*entry)(void *), void *entry_context) {
    (void)context;
    return cbm_thread_create(thread, 0, entry, entry_context);
}

static int host_http_thread_schedule_create(cbm_thread_t *thread, void *(*entry)(void *),
                                            cbm_http_server_t *server,
                                            host_http_thread_create_fn create,
                                            void *create_context) {
    if (!server || !create || !cbm_http_server_schedule_run(server))
        return -1;
    int rc = create(create_context, thread, entry, server);
    if (rc != 0 && !cbm_http_server_cancel_scheduled_run(server))
        return -1;
    return rc;
}

static int host_http_thread_start_default(void *context, cbm_thread_t *thread,
                                          void *(*entry)(void *), void *entry_context) {
    (void)context;
    return host_http_thread_schedule_create(thread, entry, entry_context,
                                            host_http_thread_create_default, NULL);
}

static int host_http_thread_join_default(void *context, cbm_thread_t *thread) {
    (void)context;
    return cbm_thread_join(thread);
}

static const host_http_ops_t g_host_http_default_ops = {
    .context = NULL,
    .config_load = host_http_config_load_default,
    .server_new = host_http_server_new_default,
    .server_configure = host_http_server_configure_default,
    .server_stop = host_http_server_stop_default,
    .server_free = host_http_server_free_default,
    .thread_start = host_http_thread_start_default,
    .thread_join = host_http_thread_join_default,
};

static bool host_http_stop_join_free(host_state_t *host) {
    const host_http_ops_t *ops = host->http_ops;
    if (host->http && host->http_started) {
        ops->server_stop(ops->context, host->http);
    }
    if (host->http_started) {
        if (ops->thread_join(ops->context, &host->http_thread) != 0) {
            return false;
        }
        host->http_started = false;
    }
    if (!ops->server_free(ops->context, host->http)) {
        return false;
    }
    host->http = NULL;
    return true;
}

static uint64_t host_deadline_from(uint64_t now_ms, uint32_t delay_ms) {
    return now_ms > UINT64_MAX - delay_ms ? UINT64_MAX : now_ms + delay_ms;
}

static void host_http_schedule_retry(host_state_t *host, uint64_t now_ms, const char *reason) {
    uint32_t delay =
        host->http_retry_delay_ms ? host->http_retry_delay_ms : HOST_HTTP_RETRY_INITIAL_MS;
    host->http_retry_at_ms = host_deadline_from(now_ms, delay);
    if (delay > host->http_largest_scheduled_retry_ms) {
        host->http_largest_scheduled_retry_ms = delay;
    }
    host->http_retry_delay_ms =
        delay >= HOST_HTTP_RETRY_MAX_MS / 2U ? HOST_HTTP_RETRY_MAX_MS : delay * 2U;
    char delay_text[32];
    (void)snprintf(delay_text, sizeof(delay_text), "%u", delay);
    cbm_log_warn("ui.retry_scheduled", "reason", reason, "delay_ms", delay_text);
}

static void host_http_reconcile_at(host_state_t *host, uint64_t now_ms, bool force_config_load) {
    if (!host || !host->http_ops) {
        return;
    }
    if (!force_config_load && host->http_config_loaded && now_ms < host->http_next_config_load_ms) {
        return;
    }
    host->http_next_config_load_ms = host_deadline_from(now_ms, HOST_HTTP_CONFIG_POLL_MS);

    cbm_ui_config_t desired;
    host->http_ops->config_load(host->http_ops->context, &desired);
    bool config_changed = !host->http_config_loaded ||
                          desired.ui_enabled != host->http_config_enabled ||
                          desired.ui_port != host->http_config_port;
    if (config_changed && !host->http_retiring) {
        host->http_retiring = true;
        host->http_retry_at_ms = now_ms;
        host->http_retry_delay_ms = HOST_HTTP_RETRY_INITIAL_MS;
    }
    if (host->http_retiring) {
        if (now_ms < host->http_retry_at_ms) {
            return;
        }
        if (!host_http_stop_join_free(host)) {
            host_http_schedule_retry(host, now_ms, "server_retire");
            return;
        }
        host->http_retiring = false;
    }
    if (config_changed) {
        host->http_config_loaded = true;
        host->http_config_enabled = desired.ui_enabled;
        host->http_config_port = desired.ui_port;
        host->http_retry_at_ms = now_ms;
        host->http_retry_delay_ms = HOST_HTTP_RETRY_INITIAL_MS;
    }
    if (!desired.ui_enabled) {
        return;
    }
    if (host->http_started || host->http) {
        return;
    }
    if (!host->http_assets_available) {
        if (config_changed) {
            cbm_log_warn("ui.no_assets", "hint", "rebuild with: make -f Makefile.cbm cbm-with-ui");
        }
        host->http_retry_at_ms = UINT64_MAX;
        return;
    }
    if (now_ms < host->http_retry_at_ms) {
        return;
    }

    const host_http_ops_t *ops = host->http_ops;
    host->http = ops->server_new(ops->context, desired.ui_port);
    if (!host->http) {
        host_http_schedule_retry(host, now_ms, "server_create");
        return;
    }
    ops->server_configure(ops->context, host->http, host);
    if (ops->thread_start(ops->context, &host->http_thread, host_http_thread, host->http) == 0) {
        host->http_started = true;
        host->http_retry_at_ms = 0;
        host->http_retry_delay_ms = HOST_HTTP_RETRY_INITIAL_MS;
    } else {
        host->http_retiring = true;
        if (host_http_stop_join_free(host)) {
            host->http_retiring = false;
        }
        host_http_schedule_retry(host, now_ms, "thread_start");
    }
}

static void host_background_stop(host_state_t *host) {
    if (host->http && host->http_started) {
        host->http_ops->server_stop(host->http_ops->context, host->http);
    }
    if (host->watcher) {
        cbm_watcher_stop(host->watcher);
    }
}

static bool host_background_join(host_state_t *host) {
    if (host->http_started) {
        if (host->http_ops->thread_join(host->http_ops->context, &host->http_thread) != 0) {
            return false;
        }
        host->http_started = false;
    }
    if (host->watcher_started) {
        if (cbm_thread_join(&host->watcher_thread) != 0) {
            return false;
        }
        host->watcher_started = false;
    }
    return true;
}

static bool host_project_lock_manager_free_once(void *context) {
    cbm_project_lock_manager_t **manager = context;
    if (!manager || !*manager) {
        return true;
    }
    (void)cbm_project_lock_manager_free(manager);
    return *manager == NULL;
}

static void host_state_free(host_state_t *host) {
    if (!host_http_stop_join_free(host)) {
        /* The retained server or one of its callbacks can still borrow every
         * dependency below. Process containment is the only safe teardown. */
        host_force_terminate("http_cleanup");
    }
    if (host->application) {
        if (!cbm_daemon_application_free(host->application)) {
            /* The application still owns work or a borrowed callback context.
             * Freeing any dependency below would turn the retained application
             * into UAF. The daemon is the ultimate containment boundary. */
            host_cleanup_force_terminate("application_cleanup");
        }
        host->application = NULL;
    }
    cbm_watcher_free(host->watcher);
    host->watcher = NULL;
    cbm_store_close(host->watch_store);
    host->watch_store = NULL;
    cbm_config_close(host->runtime_config);
    host->runtime_config = NULL;
    if (host->project_locks) {
        host_cleanup_release_until_complete(host_project_lock_manager_free_once,
                                            &host->project_locks, "project_lock_manager_cleanup");
    }
}

static bool host_state_prepare(host_state_t *host, const cbm_daemon_ipc_endpoint_t *endpoint) {
    host->http_ops = &g_host_http_default_ops;
    host->http_assets_available = CBM_EMBEDDED_FILE_COUNT > 0;
    size_t aggregate_memory_budget_bytes = cbm_mem_budget();
    const char *cache = cbm_resolve_cache_dir();
    if (!cache || !cache[0]) {
        cbm_log_error("daemon.runtime_config_open_failed", "reason", "cache_dir_unavailable");
        return false;
    }
    host->runtime_config = cbm_config_open(cache);
    if (!host->runtime_config) {
        cbm_log_error("daemon.runtime_config_open_failed", "reason", "config_db_unavailable");
        return false;
    }
    host->watch_store = cbm_store_open_memory();
    host->project_locks = cbm_project_lock_manager_new(endpoint);
    host->watcher = cbm_watcher_new(host->watch_store, host_watcher_index, host);
    cbm_daemon_application_config_t application_config = {
        .watcher = host->watcher,
        .config = host->runtime_config,
        .aggregate_memory_budget_bytes = aggregate_memory_budget_bytes,
        .project_locks = host->project_locks,
    };
    host->application = cbm_daemon_application_new(&application_config);
    if (host->application && host->permanent) {
        cbm_daemon_application_set_permanent(host->application, true);
    }
    if (!host->watch_store || !host->watcher || !host->project_locks || !host->application) {
        return false;
    }
    return true;
}

bool cbm_daemon_host_state_prepare_for_test(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint) {
        return false;
    }
    host_state_t host = {0};
    bool prepared = host_state_prepare(&host, endpoint);
    host_state_free(&host);
    return prepared;
}

typedef struct {
    size_t create_failures;
    size_t thread_start_failures;
    size_t server_free_failures;
    size_t config_change_after_load;
    size_t config_loads;
    size_t server_create_attempts;
    size_t thread_start_attempts;
    size_t server_stops;
    size_t server_frees;
    size_t thread_joins;
} host_http_reconcile_test_context_t;

static void host_http_test_config_load(void *opaque, cbm_ui_config_t *config_out) {
    host_http_reconcile_test_context_t *context = opaque;
    context->config_loads++;
    config_out->ui_enabled = true;
    bool change_port = context->config_change_after_load > 0 &&
                       context->config_loads == context->config_change_after_load;
    config_out->ui_port = change_port ? CBM_UI_DEFAULT_PORT + 1 : CBM_UI_DEFAULT_PORT;
}

static cbm_http_server_t *host_http_test_server_new(void *opaque, int port) {
    (void)port;
    host_http_reconcile_test_context_t *context = opaque;
    context->server_create_attempts++;
    return context->server_create_attempts <= context->create_failures
               ? NULL
               : (cbm_http_server_t *)context;
}

static void host_http_test_server_configure(void *opaque, cbm_http_server_t *server,
                                            host_state_t *host) {
    (void)opaque;
    (void)server;
    (void)host;
}

/* WHY: host_http_ops_t is also implemented by production callbacks that mutate
 * the server; this test double must retain that callback ABI even though it
 * only observes whether the opaque fake server is present. */
// cppcheck-suppress constParameterCallback
static void host_http_test_server_stop(void *opaque, cbm_http_server_t *server) {
    if (server) {
        ((host_http_reconcile_test_context_t *)opaque)->server_stops++;
    }
}

/* WHY: See host_http_test_server_stop; changing only this implementation to a
 * pointer-to-const parameter would make it incompatible with host_http_ops_t. */
// cppcheck-suppress constParameterCallback
static bool host_http_test_server_free(void *opaque, cbm_http_server_t *server) {
    host_http_reconcile_test_context_t *context = opaque;
    if (server) {
        context->server_frees++;
    }
    return !server || context->server_frees > context->server_free_failures;
}

static int host_http_test_thread_start(void *opaque, cbm_thread_t *thread, void *(*entry)(void *),
                                       void *entry_context) {
    (void)thread;
    (void)entry;
    (void)entry_context;
    host_http_reconcile_test_context_t *context = opaque;
    context->thread_start_attempts++;
    return context->thread_start_attempts <= context->thread_start_failures ? -1 : 0;
}

static int host_http_test_thread_join(void *opaque, cbm_thread_t *thread) {
    (void)thread;
    host_http_reconcile_test_context_t *context = opaque;
    context->thread_joins++;
    return 0;
}

bool cbm_daemon_host_http_reconcile_sequence_for_test(
    const uint64_t *timestamps_ms, size_t timestamp_count, size_t create_failures,
    size_t thread_start_failures, cbm_daemon_host_http_reconcile_test_result_t *result_out) {
    if (!timestamps_ms || timestamp_count == 0 || !result_out) {
        return false;
    }
    for (size_t i = 1; i < timestamp_count; i++) {
        if (timestamps_ms[i] < timestamps_ms[i - 1]) {
            return false;
        }
    }

    host_http_reconcile_test_context_t context = {
        .create_failures = create_failures,
        .thread_start_failures = thread_start_failures,
    };
    const host_http_ops_t ops = {
        .context = &context,
        .config_load = host_http_test_config_load,
        .server_new = host_http_test_server_new,
        .server_configure = host_http_test_server_configure,
        .server_stop = host_http_test_server_stop,
        .server_free = host_http_test_server_free,
        .thread_start = host_http_test_thread_start,
        .thread_join = host_http_test_thread_join,
    };
    host_state_t host = {
        .http_assets_available = true,
        .http_ops = &ops,
    };
    for (size_t i = 0; i < timestamp_count; i++) {
        host_http_reconcile_at(&host, timestamps_ms[i], i == 0);
    }
    bool active = host.http_started;
    uint32_t largest_retry = host.http_largest_scheduled_retry_ms;
    uint64_t next_retry = host.http_retry_at_ms;
    if (!host_http_stop_join_free(&host)) {
        return false;
    }

    *result_out = (cbm_daemon_host_http_reconcile_test_result_t){
        .config_loads = context.config_loads,
        .server_create_attempts = context.server_create_attempts,
        .thread_start_attempts = context.thread_start_attempts,
        .server_stops = context.server_stops,
        .server_frees = context.server_frees,
        .thread_joins = context.thread_joins,
        .largest_scheduled_retry_ms = largest_retry,
        .next_retry_ms = next_retry,
        .active_after_sequence = active,
    };
    return true;
}

bool cbm_daemon_host_http_reconcile_free_refusal_for_test(
    cbm_daemon_host_http_free_refusal_test_result_t *result_out) {
    if (!result_out) {
        return false;
    }
    host_http_reconcile_test_context_t context = {
        .server_free_failures = 1,
        .config_change_after_load = 2,
    };
    const host_http_ops_t ops = {
        .context = &context,
        .config_load = host_http_test_config_load,
        .server_new = host_http_test_server_new,
        .server_configure = host_http_test_server_configure,
        .server_stop = host_http_test_server_stop,
        .server_free = host_http_test_server_free,
        .thread_start = host_http_test_thread_start,
        .thread_join = host_http_test_thread_join,
    };
    host_state_t host = {
        .http_assets_available = true,
        .http_ops = &ops,
    };

    host_http_reconcile_at(&host, 0, true);
    host_http_reconcile_at(&host, HOST_HTTP_CONFIG_POLL_MS, false);
    bool retained_after_refusal =
        host.http != NULL && !host.http_started && context.server_create_attempts == 1;
    size_t creates_after_refusal = context.server_create_attempts;
    host_http_reconcile_at(&host, HOST_HTTP_CONFIG_POLL_MS * 2U, false);
    bool replacement_active = host.http != NULL && host.http_started;

    *result_out = (cbm_daemon_host_http_free_refusal_test_result_t){
        .server_create_attempts_after_refusal = creates_after_refusal,
        .server_create_attempts = context.server_create_attempts,
        .thread_start_attempts = context.thread_start_attempts,
        .server_stops = context.server_stops,
        .server_free_attempts = context.server_frees,
        .thread_joins = context.thread_joins,
        .retained_after_refusal = retained_after_refusal,
        .replacement_active_after_retry = replacement_active,
    };
    return host_http_stop_join_free(&host);
}

typedef struct {
    cbm_http_server_t *server;
    bool free_refused_while_scheduled;
} host_http_thread_create_failure_test_t;

/* WHY: the signature is fixed by the host_http_ops_t thread_start hook type;
 * a const pointee would make the function-pointer types incompatible. */
static int host_http_thread_create_failure_for_test(
    void *opaque, cbm_thread_t *thread, void *(*entry)(void *),
    void *entry_context) { // cppcheck-suppress constParameterCallback
    (void)thread;
    (void)entry;
    host_http_thread_create_failure_test_t *test = opaque;
    if (!test || entry_context != test->server)
        return -1;
    test->free_refused_while_scheduled = !cbm_http_server_free(test->server);
    return -1;
}

bool cbm_daemon_host_http_thread_create_failure_lifecycle_for_test(void) {
    cbm_http_server_t *server = cbm_http_server_new(0);
    if (!server)
        return false;
    host_http_thread_create_failure_test_t test = {
        .server = server,
    };
    cbm_thread_t unused_thread = {0};
    int rc = host_http_thread_schedule_create(&unused_thread, host_http_thread, server,
                                              host_http_thread_create_failure_for_test, &test);
    bool free_refused = test.free_refused_while_scheduled;
    bool final_free_succeeded = free_refused && cbm_http_server_free(server);
    return rc != 0 && free_refused && final_free_succeeded;
}

static bool host_background_start(host_state_t *host) {
    if (cbm_thread_create(&host->watcher_thread, 0, host_watcher_thread, host->watcher) != 0) {
        return false;
    }
    host->watcher_started = true;

    host_http_reconcile_at(host, cbm_now_ms(), true);
    return true;
}

static bool host_wait_for_lifetime(cbm_daemon_runtime_service_t *service,
                                   atomic_int *stop_requested, host_state_t *host, bool permanent) {
    uint64_t initial_deadline = cbm_now_ms() + HOST_INITIAL_CLIENT_TIMEOUT_MS;
    uint64_t stopping_deadline = 0;
    for (;;) {
        cbm_daemon_runtime_service_state_t state = cbm_daemon_runtime_service_state(service);
        if (state == CBM_DAEMON_RUNTIME_SERVICE_EXITED) {
            cbm_log_info("daemon.lifetime_end", "reason", "runtime_exited");
            return true;
        }
        if (state == CBM_DAEMON_RUNTIME_SERVICE_STOPPING) {
            if (stopping_deadline == 0) {
                stopping_deadline = cbm_now_ms() + HOST_RUNTIME_SHUTDOWN_MS;
            }
            if (cbm_now_ms() >= stopping_deadline) {
                return false;
            }
            (void)cbm_daemon_runtime_service_wait_exited(service, HOST_WAIT_TICK_MS);
            continue;
        }
        /* The nobody-ever-connected window latches on the monotonic admission
         * total, not the live client count: a short first session can begin
         * and end entirely between two polls of this loop, and a sampled
         * count then reads zero forever — stopping a daemon whose client is
         * mid-conversation between per-request connections. */
        bool admitted = cbm_daemon_runtime_service_clients_admitted_total(service) > 0;
        bool stop = stop_requested && atomic_load(stop_requested);
        /* A permanent generation (`daemon start`) skips the nobody-ever-
         * connected window — it exists precisely to idle ahead of clients.
         * Explicit stop requests (signals) are still honored. */
        if (stop || (!permanent && !admitted && cbm_now_ms() >= initial_deadline)) {
            cbm_log_info("daemon.lifetime_end", "reason",
                         stop ? "stop_requested" : "initial_window_expired");
            return cbm_daemon_runtime_service_stop(service, HOST_RUNTIME_SHUTDOWN_MS);
        }
        host_http_reconcile_at(host, cbm_now_ms(), false);
        (void)cbm_daemon_runtime_service_wait_exited(service, HOST_WAIT_TICK_MS);
    }
}

static bool host_application_shutdown(host_state_t *host) {
    if (cbm_daemon_application_shutdown(host->application, HOST_APPLICATION_SHUTDOWN_MS)) {
        return true;
    }
    cbm_log_error("daemon.shutdown_timeout", "component", "operations");
    return false;
}

static bool host_runtime_stop_free(cbm_daemon_runtime_service_t *service) {
    if (cbm_daemon_runtime_service_state(service) != CBM_DAEMON_RUNTIME_SERVICE_EXITED) {
        if (!cbm_daemon_runtime_service_stop(service, HOST_RUNTIME_SHUTDOWN_MS)) {
            cbm_log_error("daemon.shutdown_timeout", "component", "runtime");
            return false;
        }
    }
    if (!cbm_daemon_runtime_service_free(service)) {
        cbm_log_error("daemon.cleanup_timeout", "component", "runtime");
        return false;
    }
    return true;
}

static _Noreturn void host_force_terminate(const char *component) {
    /* The operation-log sink flushes every record before returning. Do not run
     * ordinary teardown after the cooperative deadline: a callback thread may
     * still own application storage. Process termination releases native
     * locks, and supervised worker groups observe their daemon parent's death. */
    cbm_log_error("daemon.forced_shutdown", "component", component);
#ifdef _WIN32
    (void)TerminateProcess(GetCurrentProcess(), EXIT_FAILURE);
    abort();
#else
    _exit(EXIT_FAILURE);
#endif
}

_Noreturn void cbm_daemon_host_force_terminate_for_test(const char *component) {
    char conflict_log[HOST_PATH_CAP];
    cbm_ui_log_init();
    if (host_log_open(conflict_log)) {
        host_force_terminate(component ? component : "test");
    }
#ifdef _WIN32
    (void)TerminateProcess(GetCurrentProcess(), EXIT_FAILURE);
    abort();
#else
    _exit(EXIT_FAILURE);
#endif
}

int cbm_daemon_host_run(const cbm_daemon_host_config_t *config) {
    if (!config || !config->endpoint || !config->executable_path ||
        !config->identity.semantic_version || !config->identity.build_fingerprint) {
        return -1;
    }
    cbm_ui_log_init();
    char conflict_log[HOST_PATH_CAP];
    if (!host_log_open(conflict_log)) {
        (void)fprintf(stderr, "codebase-memory: daemon log path is not private or safe\n");
        return -1;
    }
    cbm_version_cohort_manager_t *cohort_manager = cbm_version_cohort_manager_new(config->endpoint);
    cbm_version_cohort_lease_t *cohort_lease = NULL;
    cbm_daemon_conflict_t cohort_conflict;
    uint64_t now_ms = cbm_now_ms();
    uint64_t cohort_deadline = now_ms > UINT64_MAX - HOST_INITIAL_CLIENT_TIMEOUT_MS
                                   ? UINT64_MAX
                                   : now_ms + HOST_INITIAL_CLIENT_TIMEOUT_MS;
    cbm_version_cohort_status_t cohort_status =
        cohort_manager
            ? cbm_version_cohort_acquire(cohort_manager, &config->identity, cohort_deadline,
                                         &cohort_lease, &cohort_conflict)
            : CBM_VERSION_COHORT_IO;
    if (cohort_status != CBM_VERSION_COHORT_OK) {
        cbm_log_error("daemon.start_failed", "component", "cohort");
        char message[CBM_DAEMON_CONFLICT_MESSAGE_SIZE];
        bool formatted = cohort_status == CBM_VERSION_COHORT_CONFLICT &&
                         cbm_daemon_conflict_format(&cohort_conflict, message, sizeof(message));
        if (cohort_status == CBM_VERSION_COHORT_CONFLICT) {
            (void)cbm_version_cohort_log_conflict(&cohort_conflict);
        }
        (void)fprintf(stderr, "codebase-memory: %s\n",
                      formatted ? message : "daemon exact-build admission failed");
        host_cohort_close(&cohort_lease, &cohort_manager);
        host_log_close();
        return -1;
    }
    cbm_daemon_ipc_participant_guard_t *participant_guard = NULL;
    if (cbm_daemon_ipc_participant_guard_try_join(config->endpoint, &participant_guard) != 1) {
        cbm_log_error("daemon.start_failed", "component", "participant");
        (void)fprintf(stderr, "codebase-memory: daemon participant admission failed\n");
        host_participant_guard_close(&participant_guard);
        host_cohort_close(&cohort_lease, &cohort_manager);
        host_log_close();
        return -1;
    }
    cbm_version_cohort_daemon_claim_t *daemon_claim = NULL;
    uint64_t claim_deadline = host_deadline_after(HOST_DAEMON_CLAIM_TIMEOUT_MS);
    cbm_version_cohort_status_t claim_status =
        cbm_version_cohort_daemon_claim_acquire(cohort_manager, &daemon_claim);
    while (claim_status == CBM_VERSION_COHORT_BUSY && cbm_now_ms() < claim_deadline) {
        /* The previous generation is releasing; retry until the claim is free
         * or the handoff window elapses (a persistent BUSY = a real conflict). */
        cbm_usleep(HOST_WAIT_TICK_MS * 1000);
        claim_status = cbm_version_cohort_daemon_claim_acquire(cohort_manager, &daemon_claim);
    }
    if (claim_status != CBM_VERSION_COHORT_OK) {
        cbm_log_error("daemon.start_failed", "component", "claim");
        host_participant_guard_close(&participant_guard);
        host_daemon_claim_close(&daemon_claim);
        host_cohort_close(&cohort_lease, &cohort_manager);
        host_log_close();
        return -1;
    }
    cbm_daemon_ipc_lifetime_reservation_t *lifetime_reservation = NULL;
    if (host_lifetime_reservation_acquire(config->endpoint, &lifetime_reservation) != 1) {
        cbm_log_error("daemon.start_failed", "component", "lifetime");
        cbm_daemon_ipc_lifetime_reservation_release(lifetime_reservation);
        host_participant_guard_close(&participant_guard);
        host_daemon_claim_close(&daemon_claim);
        host_cohort_close(&cohort_lease, &cohort_manager);
        host_log_close();
        return -1;
    }
    cbm_mem_init(cbm_mem_ram_fraction_for_total(cbm_system_info().total_ram));
    cbm_http_server_set_binary_path(config->executable_path);
    cbm_index_supervisor_mark_host();

    host_state_t host = {0};
    host.permanent = config->permanent;
    if (!host_state_prepare(&host, config->endpoint)) {
        cbm_log_error("daemon.start_failed", "component", "application");
        host_state_free(&host);
        host_log_close();
        cbm_daemon_ipc_lifetime_reservation_release(lifetime_reservation);
        host_participant_guard_close(&participant_guard);
        host_daemon_claim_close(&daemon_claim);
        host_cohort_close(&cohort_lease, &cohort_manager);
        return -1;
    }
    cbm_daemon_runtime_application_callbacks_t callbacks =
        cbm_daemon_application_runtime_callbacks(host.application);
    cbm_daemon_runtime_service_config_t runtime_config = {
        .endpoint = config->endpoint,
        .identity = config->identity,
        .conflict_log_path = conflict_log,
        .conflict_log_cap_bytes = HOST_CONFLICT_LOG_CAP,
        .max_clients = HOST_MAX_CLIENTS,
        .lease_timeout_ms = HOST_LEASE_TIMEOUT_MS,
        .request_timeout_ms = HOST_REQUEST_TIMEOUT_MS,
        .shutdown_timeout_ms = HOST_RUNTIME_SHUTDOWN_MS,
        .application = callbacks,
        .permanent = config->permanent,
    };
    cbm_daemon_runtime_service_t *service =
        cbm_daemon_runtime_service_start_reserved(&runtime_config, &lifetime_reservation);
    cbm_daemon_ipc_lifetime_reservation_release(lifetime_reservation);
    lifetime_reservation = NULL;
    if (!service) {
        cbm_log_error("daemon.start_failed", "component", "runtime");
        host_state_free(&host);
        host_log_close();
        host_participant_guard_close(&participant_guard);
        host_daemon_claim_close(&daemon_claim);
        host_cohort_close(&cohort_lease, &cohort_manager);
        return -1;
    }
    if (!host_background_start(&host)) {
        cbm_log_error("daemon.start_failed", "component", "background");
        if (!host_application_shutdown(&host)) {
            host_force_terminate("operations");
        }
        host_background_stop(&host);
        if (!host_background_join(&host)) {
            host_force_terminate("background");
        }
        if (!host_runtime_stop_free(service)) {
            host_force_terminate("runtime");
        }
        host_state_free(&host);
        host_log_close();
        host_participant_guard_close(&participant_guard);
        host_daemon_claim_close(&daemon_claim);
        host_cohort_close(&cohort_lease, &cohort_manager);
        return -1;
    }
    cbm_diag_start();

    char process_id[32];
    char memory_budget[32];
    char physical_job_limit[32];
    char worker_memory_budget[32];
    (void)snprintf(process_id, sizeof(process_id), "%llu",
                   (unsigned long long)host_current_process_id());
    (void)snprintf(memory_budget, sizeof(memory_budget), "%zu", cbm_mem_budget());
    (void)snprintf(physical_job_limit, sizeof(physical_job_limit), "%zu",
                   cbm_daemon_application_physical_job_limit(host.application));
    (void)snprintf(worker_memory_budget, sizeof(worker_memory_budget), "%zu",
                   cbm_daemon_application_worker_memory_budget_bytes(host.application));
    cbm_log_info("daemon.start", "version", config->identity.semantic_version, "pid", process_id,
                 "cache_fingerprint",
                 config->identity.cache_fingerprint ? config->identity.cache_fingerprint
                                                    : "unavailable",
                 "memory_budget_bytes", memory_budget, "physical_job_limit", physical_job_limit,
                 "worker_memory_budget_bytes", worker_memory_budget);
    if (!host_wait_for_lifetime(service, config->stop_requested, &host, config->permanent)) {
        host_force_terminate("runtime");
    }

    /* Prevent any new UI/watcher operation, then cancel/reap every physical
     * job before those background loops and the daemon process disappear. */
    if (!host_application_shutdown(&host)) {
        host_force_terminate("operations");
    }
    host_background_stop(&host);
    if (!host_background_join(&host)) {
        host_force_terminate("background");
    }
    if (!host_runtime_stop_free(service)) {
        host_force_terminate("runtime");
    }
    host_state_free(&host);
    cbm_diag_stop();
    cbm_log_info("daemon.stop");
    host_log_close();
    /* Keep the exact-build lifetime lease through listener/lifetime teardown,
     * application destruction, diagnostics shutdown, the durable stop record,
     * participant release, and daemon-claim release. An activation transaction
     * may treat exclusive acquisition as proof that coordinated cleanup for
     * this generation has completed. */
    host_participant_guard_close(&participant_guard);
    host_daemon_claim_close(&daemon_claim);
    host_cohort_close(&cohort_lease, &cohort_manager);
    return 0;
}
