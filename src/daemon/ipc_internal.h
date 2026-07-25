/*
 * ipc_internal.h — Private, platform-neutral seams used by daemon IPC tests.
 */
#ifndef CBM_DAEMON_IPC_INTERNAL_H
#define CBM_DAEMON_IPC_INTERNAL_H

#include "daemon/ipc.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Windows daemon rendezvous addresses are generation-specific and
 * unguessable. These platform-neutral seams keep the SID/nonce derivation and
 * fixed record parser covered on every CI host; the Windows endpoint
 * constructor supplies the current token's binary SID and the startup winner
 * supplies a BCryptGenRandom nonce. */
#define CBM_DAEMON_IPC_WINDOWS_NAME_CAP 256U
#define CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE 32U
#define CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE \
    (8U + CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE + CBM_DAEMON_IPC_WINDOWS_NAME_CAP)
#define CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE "cbm-rendezvous.lock"

bool cbm_daemon_ipc_windows_generation_address(
    const uint8_t *sid, size_t sid_length, const char *instance_key,
    const uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE],
    char address_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP]);

/* Exact pre-cohort Windows namespace retained only for migration safety. New
 * generations never publish the deterministic pipe and never treat either
 * object as current authority; startup/lifetime may hold the old startup mutex
 * solely as a compatibility guard against an overlapping pre-cohort process. */
bool cbm_daemon_ipc_windows_legacy_names(const char *canonical_runtime_parent,
                                         const char *instance_key,
                                         char pipe_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP],
                                         char startup_mutex_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP]);

bool cbm_daemon_ipc_windows_rendezvous_record_encode(
    const uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE], const char *address,
    uint8_t record_out[CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE]);
bool cbm_daemon_ipc_windows_rendezvous_record_decode(
    const uint8_t *record, size_t record_length,
    uint8_t nonce_out[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE],
    char address_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP]);

typedef enum {
    CBM_IPC_PENDING_WAIT_FAILED = -1,
    CBM_IPC_PENDING_WAIT_TIMEOUT = 0,
    CBM_IPC_PENDING_WAIT_SIGNALED = 1,
} cbm_ipc_pending_wait_status_t;

typedef enum {
    CBM_IPC_PENDING_FINISH_FAILED = -1,
    CBM_IPC_PENDING_FINISH_CANCELLED = 0,
    CBM_IPC_PENDING_FINISH_COMPLETED = 1,
} cbm_ipc_pending_finish_status_t;

typedef struct {
    void *context;
    cbm_ipc_pending_wait_status_t (*wait)(void *context, uint32_t timeout_ms);
    void (*cancel)(void *context);
    cbm_ipc_pending_finish_status_t (*finish)(void *context, bool blocking,
                                              uint32_t *transferred_out);
} cbm_ipc_pending_ops_t;

/* Returns 1 for a completed transfer, 0 for a cancelled timeout, and -1 for
 * an error. Every pending operation is terminal before this function returns. */
int cbm_daemon_ipc_wait_pending(const cbm_ipc_pending_ops_t *ops, uint32_t timeout_ms,
                                uint32_t *transferred_out);

/* Internal receive path for fixed-size unauthenticated protocol envelopes.
 * Payloads above max_payload_length poison the stream. The implementation must
 * reject from the decoded header before allocating or reading that payload. */
int cbm_daemon_ipc_receive_frame_bounded(cbm_daemon_ipc_connection_t *connection,
                                         uint32_t timeout_ms, uint32_t max_payload_length,
                                         cbm_daemon_frame_t *frame_out, uint8_t **payload_out);

/* Narrow crash/fault seams for publication-state and retry-state tests. They
 * are inert unless a test installs a hook/failure count in its own process. */
typedef enum {
    CBM_DAEMON_IPC_POSIX_PUBLICATION_ANCHOR_DURABLE = 1,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_DURABLE = 2,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_STABLE_DURABLE = 3,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_DURABLE = 4,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_REMOVED = 5,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_TEMP_SYNCED = 6,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_RECORD_LINKED = 7,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_TEMP_SYNCED = 8,
    CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_RECORD_LINKED = 9,
} cbm_daemon_ipc_posix_publication_stage_t;

typedef void (*cbm_daemon_ipc_posix_publication_hook_fn)(
    cbm_daemon_ipc_posix_publication_stage_t stage, void *context);

void cbm_daemon_ipc_posix_publication_hook_set_for_test(
    cbm_daemon_ipc_posix_publication_hook_fn hook, void *context);
void cbm_daemon_ipc_windows_legacy_guard_release_failures_set_for_test(unsigned int count);

#endif /* CBM_DAEMON_IPC_INTERNAL_H */
