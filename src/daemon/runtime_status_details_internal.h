#ifndef CBM_DAEMON_RUNTIME_STATUS_DETAILS_INTERNAL_H
#define CBM_DAEMON_RUNTIME_STATUS_DETAILS_INTERNAL_H

#include "daemon/ipc.h"

/* Dependencies of the new STATUS_DETAILS exchange only. Legacy STATUS/STOP
 * continue to use runtime_control_request_send unchanged. */
typedef struct {
    uint64_t (*now_ms)(void);
    cbm_daemon_ipc_connection_t *(*connect)(const cbm_daemon_ipc_endpoint_t *, uint32_t);
    bool (*send)(cbm_daemon_ipc_connection_t *, cbm_daemon_frame_type_t, uint16_t, const void *,
                 uint32_t);
    int (*receive)(cbm_daemon_ipc_connection_t *, uint32_t, uint32_t, cbm_daemon_frame_t *,
                   uint8_t **);
    void (*close)(cbm_daemon_ipc_connection_t *);
} cbm_daemon_status_details_transport_t;

#ifdef CBM_ENABLE_TEST_SEAMS
/* Single-threaded test scope; NULL restores real IPC and the monotonic clock. */
void cbm_daemon_runtime_set_status_details_transport_for_testing(
    const cbm_daemon_status_details_transport_t *transport);
#endif

#endif
