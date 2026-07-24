/*
 * host_internal.h — Private daemon-host test seams.
 */
#ifndef CBM_DAEMON_HOST_INTERNAL_H
#define CBM_DAEMON_HOST_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

struct cbm_daemon_ipc_endpoint;

typedef bool (*cbm_daemon_host_cleanup_release_for_test_fn)(void *context);

/* Runs the same finite retry/fail-stop driver used by daemon-claim shutdown
 * around an injected release operation. The hook is process-local and inert
 * unless a focused subprocess test calls this seam. */
void cbm_daemon_host_cleanup_release_until_complete_for_test(
    cbm_daemon_host_cleanup_release_for_test_fn release, void *context);

/* Opens the ordinary private daemon operation log, records the same forced
 * shutdown event used by production, flushes it, and terminates the process.
 * This exists only so an isolated child can verify the hard escape path. */
_Noreturn void cbm_daemon_host_force_terminate_for_test(const char *component);

/* Focused host-state probe. It performs no listener publication and starts no
 * background thread; the production preparation and cleanup paths are used
 * verbatim so tests can make the runtime config database unavailable. */
bool cbm_daemon_host_state_prepare_for_test(const struct cbm_daemon_ipc_endpoint *endpoint);

typedef struct {
    size_t config_loads;
    size_t server_create_attempts;
    size_t thread_start_attempts;
    size_t server_stops;
    size_t server_frees;
    size_t thread_joins;
    uint32_t largest_scheduled_retry_ms;
    uint64_t next_retry_ms;
    bool active_after_sequence;
} cbm_daemon_host_http_reconcile_test_result_t;

typedef struct {
    size_t server_create_attempts_after_refusal;
    size_t server_create_attempts;
    size_t thread_start_attempts;
    size_t server_stops;
    size_t server_free_attempts;
    size_t thread_joins;
    bool retained_after_refusal;
    bool replacement_active_after_retry;
} cbm_daemon_host_http_free_refusal_test_result_t;

/* Drive the production HTTP reconciliation state machine at explicit
 * monotonic timestamps with deterministic transient failures. */
bool cbm_daemon_host_http_reconcile_sequence_for_test(
    const uint64_t *timestamps_ms, size_t timestamp_count, size_t create_failures,
    size_t thread_start_failures, cbm_daemon_host_http_reconcile_test_result_t *result_out);

/* Change the configured UI port for one poll under an active fake server,
 * inject one server-free refusal, revert the desired port, then drive the
 * next retry. */
bool cbm_daemon_host_http_reconcile_free_refusal_for_test(
    cbm_daemon_host_http_free_refusal_test_result_t *result_out);

/* Exercise the production HTTP schedule/create/cancel adapter with an injected
 * create failure. The callback verifies free is refused while SCHEDULED; after
 * the failure, the adapter must cancel so the final free succeeds. */
bool cbm_daemon_host_http_thread_create_failure_lifecycle_for_test(void);

#endif /* CBM_DAEMON_HOST_INTERNAL_H */
