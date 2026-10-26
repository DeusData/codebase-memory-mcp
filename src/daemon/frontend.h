/*
 * frontend.h — Stateless stdio bridge for a daemon-backed MCP session.
 */
#ifndef CBM_DAEMON_FRONTEND_H
#define CBM_DAEMON_FRONTEND_H

#include "daemon/runtime.h"
#include "daemon/version_cohort.h"

#include <stdbool.h>
#include <stdio.h>

typedef struct cbm_daemon_maintenance_monitor cbm_daemon_maintenance_monitor_t;

/* Called once when install/update/uninstall requests an active local command
 * to stop cooperatively. Returning false does not authorize the command to
 * outlive the bounded grace period. */
typedef bool (*cbm_daemon_maintenance_cancel_fn)(void *context);

/* #2796: invoked on the frontend worker thread when a request exchange fails
 * with a transport error while the MCP client's session is still live.
 * dead_client remains owned by the frontend, which closes it after the swap
 * and frees it only once every frontend thread has stopped; the callback must
 * not use or close it. The callback establishes a replacement (connect-or-spawn
 * plus session context) and returns it — NULL leaves the transport failure
 * fatal. The returned client must be fresh, open, and distinct from every
 * client returned earlier in this run (dead_client included); the frontend
 * takes ownership: it closes the client if the swap is refused, and
 * closes/frees it at the end of a successful run. The callback runs at most
 * three times per run. It may take as long as a cold daemon spawn; the EOF
 * drain and the join watchdog grant at most a hard 120 s grace per attempt.
 * A maintenance activation still fail-stops the process on the monitor's own
 * shorter grace, because a binary update must not wait for a reconnect. It
 * must not read or write the frontend's stdin/stdout; stderr diagnostics are
 * allowed, but the callback must not assume that stream is being drained. */
typedef cbm_daemon_runtime_client_t *(*cbm_daemon_frontend_daemon_lost_fn)(
    void *context, cbm_daemon_runtime_client_t *dead_client);

/* Parse one JSON-RPC message and recognize only the exact cancellation
 * notification method. String contents, nested fields, method prefixes, and
 * requests carrying an id are not cancellation notifications. */
bool cbm_daemon_frontend_is_cancellation_notification(const char *message);

/* Return true only when message is an exact cancellation notification whose
 * params.requestId has the same JSON type and value as the active request.
 * Empty, stale, and numeric-vs-string targets never authorize request
 * cancellation. */
bool cbm_daemon_frontend_cancellation_matches_request(const char *message, int64_t active_id,
                                                      const char *active_id_str);

/* The JSON-RPC error message answered for a cancelled request (#2144). A
 * cancelled index_repository call is usually a client deadline on a long
 * index, so its reply names the async alternative; every other request keeps
 * the plain message. Returns a static string free of JSON metacharacters. */
const char *cbm_daemon_frontend_cancelled_error_message(const char *request_message);

/* Start a temporary observer for a one-shot local CLI command or physical
 * supervised worker. manager and cancel_context are borrowed until stop. On
 * maintenance intent the observer invokes cancel once, permits a fixed bounded
 * grace large enough for supervised worker-tree containment, then
 * _Exit(exit_code) so OS/SQLite cleanup releases the participant's locks. The
 * caller must stop and join before freeing either borrowed object; a false stop
 * result means process-level exit is the only safe alternative to freeing
 * memory still visible to the observer. */
cbm_daemon_maintenance_monitor_t *cbm_daemon_maintenance_monitor_start(
    cbm_version_cohort_manager_t *manager, cbm_daemon_maintenance_cancel_fn cancel,
    void *cancel_context, int exit_code, const char *participant);
bool cbm_daemon_maintenance_monitor_stop(cbm_daemon_maintenance_monitor_t **monitor_io);

#if defined(CBM_ENABLE_TEST_SEAMS) && CBM_ENABLE_TEST_SEAMS
/* Deterministic observer accounting for the idle-frontend regression test.
 * The hold is inert until reset(true), and production builds contain neither
 * these controls nor their counters. */
void cbm_daemon_frontend_test_observer_reset(bool hold_monitor);
void cbm_daemon_frontend_test_observer_release(void);
bool cbm_daemon_frontend_test_monitor_waiting(void);
uint64_t cbm_daemon_frontend_test_monitor_observations(void);
uint64_t cbm_daemon_frontend_test_worker_observations(void);
uint64_t cbm_daemon_frontend_test_worker_idle_cycles(void);
#endif

/* Takes ownership of client and borrows cohort_manager for the complete call.
 * A dedicated reader keeps observing stdin while one joinable worker performs
 * daemon requests. An independent maintenance monitor remains runnable when
 * either thread is blocked in stdio, requests cooperative cancellation for the
 * exact active request, and then bounds process exit. Kernel IPC close cancels
 * only this session's daemon work. EOF/parse failure closes the authenticated
 * session. An unexpected daemon transport failure is retried against a
 * replacement daemon through daemon_lost (bounded, #2796); daemon answers such
 * as busy, rejected, handler error or cancellation are not retried. A replayed
 * request may already have executed in the dead daemon: a transport failure
 * cannot distinguish "never sent" from "reply lost", so the interrupted call
 * has at-least-once semantics. Without the callback — or once the retry budget
 * is spent — the worker terminates the process immediately so an agent waiting
 * with stdin still open observes server EOF. */
int cbm_daemon_frontend_mcp_run(cbm_daemon_runtime_client_t *client,
                                cbm_version_cohort_manager_t *cohort_manager, FILE *in, FILE *out,
                                cbm_daemon_frontend_daemon_lost_fn daemon_lost,
                                void *daemon_lost_context);

#endif /* CBM_DAEMON_FRONTEND_H */
