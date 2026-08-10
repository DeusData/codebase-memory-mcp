/*
 * application_internal.h — Private daemon application test seams.
 */
#ifndef CBM_DAEMON_APPLICATION_INTERNAL_H
#define CBM_DAEMON_APPLICATION_INTERNAL_H

#include "daemon/application.h"
#include "mcp/mcp.h" /* cbm_jsonrpc_request_t */

#include <stdbool.h>
#include <stddef.h>

/* Read-only test diagnostic for a live session. The caller must serialize
 * this check with request, cancellation, and close callbacks. */
bool cbm_daemon_application_session_retains_store_for_test(
    const cbm_daemon_runtime_application_session_t *session);

/* Fail exactly one physical index supervisor-thread admission before any
 * worker starts. This keeps the otherwise OS-dependent thread-create failure
 * path deterministic and verifies that its linked job reservation is rolled
 * back rather than retained as terminal background state. */
void cbm_daemon_application_fail_next_job_thread_start_for_test(void);

/* Park every new job thread before its first pre-start cancel check while
 * held. This makes the cancel-wins-before-worker-start interleaving — which
 * otherwise needs a descheduled thread on a loaded runner — reproducible by
 * construction: hold, subscribe, cancel, observe CANCELLED, release, then
 * assert the worker was never started and its ops were never invoked. */
void cbm_daemon_application_hold_job_before_start_for_test(bool hold);

/* Completed background-initialize passes (auto-index admission tail).
 * Waiting for a delta here replaces sleep-then-assert negatives. */
int cbm_daemon_application_background_initializes_for_test(void);

/* Iterations of the explicit-index busy-queue wait (request parked behind
 * the physical job limit). Waiting for a delta here is the positive signal
 * that a request QUEUED rather than erroring or starting. */
int cbm_daemon_application_busy_queue_waits_for_test(void);

/* Build the JSON-RPC error substituted for a reply too large to frame (#1375).
 * Exposed because the alternative — driving a real >10 MiB reply — needs a
 * ~20k-node index, a fixture cost the unit suite should not carry. The
 * end-to-end behaviour is covered by the repro in the issue. Caller frees.
 * `request` may be NULL (unparseable message → no id echoed). */
char *cbm_daemon_application_framable_response_for_test(char *response,
                                                        const cbm_jsonrpc_request_t *request);

#endif /* CBM_DAEMON_APPLICATION_INTERNAL_H */
