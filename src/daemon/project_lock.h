/* project_lock.h — Shared daemon/local-CLI project mutation leases. */
#ifndef CBM_DAEMON_PROJECT_LOCK_H
#define CBM_DAEMON_PROJECT_LOCK_H

#include "daemon/ipc.h"
#include "foundation/lock_registry.h"

#include <stdint.h>

typedef struct cbm_project_lock_manager cbm_project_lock_manager_t;
typedef struct cbm_project_lock_lease cbm_project_lock_lease_t;

/* Each manager is an independent process-local registry over the endpoint's
 * owner-only runtime directory. Separate CBM processes therefore coordinate
 * through the same native lock files without sharing memory. */
cbm_project_lock_manager_t *cbm_project_lock_manager_new(const cbm_daemon_ipc_endpoint_t *endpoint);

/* Normal projects hold SH(project-set) + EX(project). "*" holds
 * EX(project-set), blocking every named project. Project lock keys are ASCII
 * case-folded to cover filename aliases on case-insensitive filesystems. */
cbm_private_file_lock_status_t cbm_project_lock_acquire(cbm_project_lock_manager_t *manager,
                                                        const char *project, uint64_t deadline_ms,
                                                        const cbm_lock_cancel_token_t *cancel_token,
                                                        cbm_project_lock_lease_t **lease_out);

/* One fair, nonblocking attempt for UI/watcher paths. */
cbm_private_file_lock_status_t cbm_project_lock_try_acquire(cbm_project_lock_manager_t *manager,
                                                            const char *project,
                                                            cbm_project_lock_lease_t **lease_out);

cbm_private_file_lock_status_t cbm_project_lock_lease_release(cbm_project_lock_lease_t **lease_io);

cbm_private_file_lock_status_t cbm_project_lock_request_cancel(cbm_project_lock_manager_t *manager,
                                                               cbm_lock_cancel_token_t *token);

/* Refuses teardown while any lease/cleanup state remains. */
cbm_private_file_lock_status_t cbm_project_lock_manager_free(
    cbm_project_lock_manager_t **manager_io);

#endif /* CBM_DAEMON_PROJECT_LOCK_H */
