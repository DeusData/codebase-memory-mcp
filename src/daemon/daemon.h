/*
 * daemon.h — Process-local coordination and wire framing for the CBM daemon.
 *
 * Transport and worker supervision live outside this module. The coordinator
 * binds clients and resource subscriptions to transport connections, coalesces
 * shared work, and defines the daemon's terminal shutdown transition.
 */
#ifndef CBM_DAEMON_H
#define CBM_DAEMON_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Permanent framing version for the account-wide rendezvous endpoint. Never
 * bump this for detailed runtime payload changes: incompatible executable
 * generations must still exchange the stable HELLO conflict envelope. */
#define CBM_DAEMON_RENDEZVOUS_FRAME_VERSION 1U
#define CBM_DAEMON_FRAME_HEADER_SIZE 12U
#define CBM_DAEMON_MAX_FRAME_SIZE (10U * 1024U * 1024U)
#define CBM_DAEMON_KEY_SIZE 17U

typedef enum {
    CBM_DAEMON_FRAME_REQUEST = 1,
    CBM_DAEMON_FRAME_RESPONSE = 2,
} cbm_daemon_frame_type_t;

typedef struct {
    cbm_daemon_frame_type_t type;
    uint16_t flags;
    uint32_t length;
} cbm_daemon_frame_t;

typedef struct cbm_daemon_coordinator cbm_daemon_coordinator_t;

typedef uint64_t cbm_daemon_client_id_t;
typedef uint64_t cbm_daemon_subscription_id_t;

#define CBM_DAEMON_CLIENT_ID_INVALID ((cbm_daemon_client_id_t)0)
#define CBM_DAEMON_SUBSCRIPTION_ID_INVALID ((cbm_daemon_subscription_id_t)0)

typedef enum {
    CBM_DAEMON_COORDINATOR_RUNNING = 1,
    CBM_DAEMON_COORDINATOR_STOPPING = 2,
} cbm_daemon_coordinator_state_t;

typedef enum {
    CBM_DAEMON_SUBSCRIPTION_REJECTED = 0,
    CBM_DAEMON_SUBSCRIPTION_STARTED = 1,
    CBM_DAEMON_SUBSCRIPTION_JOINED = 2,
} cbm_daemon_subscription_result_t;

typedef enum {
    CBM_DAEMON_JOB_NONE = 0,
    CBM_DAEMON_JOB_RUNNING = 1,
    CBM_DAEMON_JOB_CANCEL_REQUESTED = 2,
    CBM_DAEMON_JOB_REAPING = 3,
} cbm_daemon_job_state_t;

typedef void (*cbm_daemon_job_cancel_fn)(const char *project_key, void *context);
typedef void (*cbm_daemon_watch_release_fn)(const char *project_key, void *context);

typedef struct {
    cbm_daemon_job_cancel_fn cancel_job;
    cbm_daemon_watch_release_fn release_watch;
    void *context;
} cbm_daemon_coordinator_hooks_t;

/* lease_timeout_ms is fixed for the coordinator lifetime. All timestamps must
 * come from the same monotonic clock domain. */
cbm_daemon_coordinator_t *cbm_daemon_coordinator_new(uint64_t lease_timeout_ms);

/* A PERMANENT coordinator (backing a `daemon start` generation) never
 * self-transitions to STOPPING when its client count reaches zero; only the
 * explicit stop/drain paths end it. */
void cbm_daemon_coordinator_set_permanent(cbm_daemon_coordinator_t *coordinator, bool permanent);
/* The caller must first quiesce coordinator calls and hook invocations. */
void cbm_daemon_coordinator_free(cbm_daemon_coordinator_t *coordinator);

/* Hooks are copied. Their context must remain valid until the coordinator is
 * quiescent. Hooks are always invoked after releasing the coordinator mutex. */
bool cbm_daemon_coordinator_set_hooks(cbm_daemon_coordinator_t *coordinator,
                                      const cbm_daemon_coordinator_hooks_t *hooks);
cbm_daemon_coordinator_state_t cbm_daemon_coordinator_state(cbm_daemon_coordinator_t *coordinator);

/* Client IDs are daemon-issued, nonzero, monotonic, and never recycled. */
cbm_daemon_client_id_t cbm_daemon_client_connected(cbm_daemon_coordinator_t *coordinator,
                                                   uint64_t now_ms);
bool cbm_daemon_client_disconnected(cbm_daemon_coordinator_t *coordinator,
                                    cbm_daemon_client_id_t client_id, uint64_t now_ms);
bool cbm_daemon_client_heartbeat(cbm_daemon_coordinator_t *coordinator,
                                 cbm_daemon_client_id_t client_id, uint64_t now_ms);
size_t cbm_daemon_expire_leases(cbm_daemon_coordinator_t *coordinator, uint64_t now_ms);
size_t cbm_daemon_active_clients(cbm_daemon_coordinator_t *coordinator);

/* Every accepted subscription receives a unique daemon-issued handle. The
 * first subscriber starts the physical resource; later subscribers join it. */
cbm_daemon_subscription_result_t cbm_daemon_job_subscribe(
    cbm_daemon_coordinator_t *coordinator, cbm_daemon_client_id_t client_id,
    const char *project_key, cbm_daemon_subscription_id_t *subscription_id);
cbm_daemon_subscription_result_t cbm_daemon_watch_subscribe(
    cbm_daemon_coordinator_t *coordinator, cbm_daemon_client_id_t client_id,
    const char *project_key, cbm_daemon_subscription_id_t *subscription_id);
bool cbm_daemon_job_unsubscribe(cbm_daemon_coordinator_t *coordinator,
                                cbm_daemon_client_id_t client_id,
                                cbm_daemon_subscription_id_t subscription_id);
bool cbm_daemon_watch_unsubscribe(cbm_daemon_coordinator_t *coordinator,
                                  cbm_daemon_client_id_t client_id,
                                  cbm_daemon_subscription_id_t subscription_id);

size_t cbm_daemon_job_subscribers(cbm_daemon_coordinator_t *coordinator, const char *project_key);
size_t cbm_daemon_watch_subscribers(cbm_daemon_coordinator_t *coordinator, const char *project_key);
size_t cbm_daemon_active_jobs(cbm_daemon_coordinator_t *coordinator);
size_t cbm_daemon_active_watches(cbm_daemon_coordinator_t *coordinator);
cbm_daemon_job_state_t cbm_daemon_job_state(cbm_daemon_coordinator_t *coordinator,
                                            const char *project_key);

/* Cancellation is two phase. Losing the final subscriber requests cancel;
 * the job remains active until its supervisor reports completion/reaping. */
bool cbm_daemon_job_reaping(cbm_daemon_coordinator_t *coordinator, const char *project_key);
bool cbm_daemon_job_reaped(cbm_daemon_coordinator_t *coordinator, const char *project_key,
                           uint64_t now_ms);
bool cbm_daemon_job_completed(cbm_daemon_coordinator_t *coordinator, const char *project_key,
                              uint64_t now_ms);

/* STOPPING is terminal. Exit is ready only after every job/watch is gone. */
bool cbm_daemon_should_exit(cbm_daemon_coordinator_t *coordinator, uint64_t now_ms);

/* Encode/decode the permanently stable 12-byte "CBMD" rendezvous frame header
 * in network byte order. Detailed operation ABIs live above this framing. */
bool cbm_daemon_frame_header_encode(uint8_t header[CBM_DAEMON_FRAME_HEADER_SIZE],
                                    cbm_daemon_frame_type_t type, uint16_t flags, uint32_t length);
bool cbm_daemon_frame_header_decode(const uint8_t header[CBM_DAEMON_FRAME_HEADER_SIZE],
                                    cbm_daemon_frame_t *frame);

#endif /* CBM_DAEMON_H */
