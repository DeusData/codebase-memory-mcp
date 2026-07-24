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

/* Takes ownership of client and borrows cohort_manager for the complete call.
 * A dedicated reader keeps observing stdin while one joinable worker performs
 * daemon requests. An independent maintenance monitor remains runnable when
 * either thread is blocked in stdio, requests cooperative cancellation for the
 * exact active request, and then bounds process exit. Kernel IPC close cancels
 * only this session's daemon work. EOF/parse failure closes the authenticated
 * session. An unexpected daemon transport failure likewise terminates the
 * process so an agent waiting with stdin still open observes server EOF. */
int cbm_daemon_frontend_mcp_run(cbm_daemon_runtime_client_t *client,
                                cbm_version_cohort_manager_t *cohort_manager, FILE *in, FILE *out);

#endif /* CBM_DAEMON_FRONTEND_H */
