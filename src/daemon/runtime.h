/*
 * runtime.h — Mandatory per-account CBM daemon runtime.
 *
 * This layer combines the authenticated local IPC transport, exact-build
 * HELLO policy, and connection-owned coordinator leases.  It deliberately
 * does not spawn the daemon process; executable bootstrap is a caller concern.
 */
#ifndef CBM_DAEMON_RUNTIME_H
#define CBM_DAEMON_RUNTIME_H

#include "daemon/daemon.h"
#include "daemon/ipc.h"
#include "daemon/service.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Detailed post-admission operation ABI. It is deliberately absent from the
 * stable endpoint HELLO: an exact executable fingerprint already selects this
 * layout, while conflicting generations must remain able to diagnose each
 * other even when this value and every detailed payload have changed. */
#define CBM_DAEMON_RUNTIME_WIRE_ABI 1U

/* Permanent account-wide rendezvous envelope, generation zero. These numeric
 * capacities and byte sizes are frozen independently of service/runtime data
 * structures. A future generation must preserve both request and response
 * layouts exactly at the stable endpoint.
 *
 * Request:  u32 ABI @0, version[64] @4, build[65] @68.
 * Response: u32 connect @0, u32 hello @4, u64 client @8, u64 PID @16,
 *           u32 conflict @24, active version[64] @28, active build[65] @92,
 *           requested version[64] @157, requested build[65] @221,
 *           message[512] @286. All integers use network byte order. */
#define CBM_DAEMON_RENDEZVOUS_ABI 1U
#define CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP 64U
#define CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP 65U
#define CBM_DAEMON_RENDEZVOUS_MESSAGE_CAP 512U
#define CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE 133U
#define CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE 798U
/* Cross-version activation shutdown is a separate first-frame protocol which
 * remains parseable when normal HELLO would report a version/build conflict.
 * Request: u32 action @0 followed by the unchanged 133-byte rendezvous
 * identity @4. Response: u32 rendezvous ABI @0, u32 accepted @4, u64 active
 * client snapshot @8, u64 drained-connection snapshot @16 (the activation
 * requester excludes itself). */
#define CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE 137U
#define CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE 24U

/* STATUS/STOP wire sizes. Requests carry only the requester's claimed build
 * fingerprint (kernel-peer-verified, cross-build by design). Responses are
 * fixed-size with a capped committed-client pid table. */
#define CBM_DAEMON_CONTROL_REQUEST_SIZE 65U
#define CBM_DAEMON_CONTROL_CLIENT_CAP 8U
#define CBM_DAEMON_STATUS_RESPONSE_SIZE 121U
#define CBM_DAEMON_STOP_RESPONSE_SIZE 40U
#define CBM_DAEMON_RUNTIME_PROJECT_KEY_MAX 4096U
#define CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX (CBM_DAEMON_MAX_FRAME_SIZE - 16U)

/* Frame flags are operation codes. Every operation has one exact payload
 * length (or an explicitly length-prefixed payload); trailing bytes are a
 * protocol violation and close the connection without registering a client.
 * Multi-byte integers are encoded in network byte order. */
typedef enum {
    CBM_DAEMON_RUNTIME_OP_HELLO = 1,
    CBM_DAEMON_RUNTIME_OP_HEARTBEAT = 2,
    CBM_DAEMON_RUNTIME_OP_JOB_SUBSCRIBE = 3,
    CBM_DAEMON_RUNTIME_OP_JOB_UNSUBSCRIBE = 4,
    CBM_DAEMON_RUNTIME_OP_DISCONNECT = 5,
    CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST = 6,
    CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL = 7,
    CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN = 8,
    /* Fire-and-forget departure notice sent by close_begin when it interrupts
     * an in-flight exchange. POSIX peers additionally signal departure via
     * shutdown()/EOF; Windows named pipes have no client half-close, so this
     * frame is what releases admission before the handle closes. No response
     * is sent: the client's receive path is already interrupted. */
    CBM_DAEMON_RUNTIME_OP_CLOSE_INTENT = 9,
    /* `daemon status` observability probe. Like ACTIVATION_SHUTDOWN this is a
     * one-shot authenticated first frame — no cohort, no HELLO, no admission —
     * so it works across build skew, which is exactly when status matters. */
    CBM_DAEMON_RUNTIME_OP_STATUS = 10,
    /* `daemon stop`. Same authentication shape as STATUS. Refuses while
     * committed clients exist (their pids come back in the response) and
     * otherwise begins the normal stopping sequence — including for a
     * PERMANENT generation, which this op and process kill alone may end. */
    CBM_DAEMON_RUNTIME_OP_STOP = 11,
} cbm_daemon_runtime_operation_t;

typedef enum {
    CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL = 1,
    CBM_DAEMON_RUNTIME_ACTIVATION_UPDATE = 2,
    CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL = 3,
} cbm_daemon_runtime_activation_action_t;

typedef struct {
    bool accepted;
    uint64_t active_clients;
    uint64_t active_connections;
} cbm_daemon_runtime_activation_result_t;

typedef struct cbm_daemon_runtime_service cbm_daemon_runtime_service_t;
typedef struct cbm_daemon_runtime_client cbm_daemon_runtime_client_t;

typedef enum {
    CBM_DAEMON_RUNTIME_SERVICE_STARTING = 1,
    CBM_DAEMON_RUNTIME_SERVICE_RUNNING = 2,
    CBM_DAEMON_RUNTIME_SERVICE_STOPPING = 3,
    CBM_DAEMON_RUNTIME_SERVICE_EXITED = 4,
} cbm_daemon_runtime_service_state_t;

/* Application requests are binary-safe and bounded. TRANSPORT_ERROR is local
 * to the client API and is never emitted as a valid wire response. BUSY is
 * generated by the runtime when a connection already has an in-flight request;
 * CANCELLED is the correlated, request-scoped cancellation result. */
typedef enum {
    CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR = 0,
    CBM_DAEMON_RUNTIME_APPLICATION_OK = 1,
    CBM_DAEMON_RUNTIME_APPLICATION_BUSY = 2,
    CBM_DAEMON_RUNTIME_APPLICATION_UNAVAILABLE = 3,
    CBM_DAEMON_RUNTIME_APPLICATION_REJECTED = 4,
    CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR = 5,
    CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED = 6,
} cbm_daemon_runtime_application_status_t;

typedef uint64_t cbm_daemon_runtime_application_token_t;
#define CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID UINT64_C(0)

typedef enum {
    CBM_DAEMON_RUNTIME_CANCEL_ERROR = -1,
    CBM_DAEMON_RUNTIME_CANCEL_STALE = 0,
    CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED = 1,
} cbm_daemon_runtime_cancel_result_t;

typedef void cbm_daemon_runtime_application_session_t;

typedef cbm_daemon_runtime_application_session_t *(*cbm_daemon_runtime_application_session_open_fn)(
    void *context, cbm_daemon_client_id_t client_id, uint64_t authenticated_process_id);

/* For OK, response_out may receive a malloc-owned binary buffer which the
 * runtime frees after sending; NULL is valid only for a zero-length response.
 * Non-OK results must leave an empty response. The request buffer is an owned
 * runtime copy and remains valid only for the duration of this callback. */
typedef cbm_daemon_runtime_application_status_t (*cbm_daemon_runtime_application_request_fn)(
    void *context, cbm_daemon_runtime_application_session_t *session,
    cbm_daemon_runtime_application_token_t request_token, const uint8_t *request,
    uint32_t request_length, uint8_t **response_out, uint32_t *response_length_out);

/* Request cancellation is non-terminal and may arrive before request() enters.
 * The exact token must therefore remain sticky until that request observes it.
 * The callback must be nonblocking and safe while request() is running. */
typedef void (*cbm_daemon_runtime_application_request_cancel_fn)(
    void *context, cbm_daemon_runtime_application_session_t *session,
    cbm_daemon_runtime_application_token_t request_token);

/* cancel is terminal for the session and is invoked after coordinator
 * ownership is released. It may run before a newly-created request thread
 * enters request(), so the cancellation state must remain sticky. It must be
 * nonblocking, safe to invoke while request is running, and arrange for that
 * request to return promptly. open and close are synchronous and must also
 * return promptly; close is invoked exactly once after any request thread has
 * joined. Runtime teardown is necessarily cooperative at this callback
 * boundary; it never detaches or frees a still-running callback. */
typedef void (*cbm_daemon_runtime_application_session_cancel_fn)(
    void *context, cbm_daemon_runtime_application_session_t *session);
typedef void (*cbm_daemon_runtime_application_session_close_fn)(
    void *context, cbm_daemon_runtime_application_session_t *session);

typedef struct {
    void *context;
    cbm_daemon_runtime_application_session_open_fn session_open;
    cbm_daemon_runtime_application_request_fn request;
    cbm_daemon_runtime_application_request_cancel_fn request_cancel;
    cbm_daemon_runtime_application_session_cancel_fn session_cancel;
    cbm_daemon_runtime_application_session_close_fn session_close;
} cbm_daemon_runtime_application_callbacks_t;

typedef struct {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_daemon_build_identity_t identity;
    const char *conflict_log_path;
    size_t conflict_log_cap_bytes;
    /* Hard cap on accepted connection threads, including sockets that have not
     * completed HELLO. request_timeout_ms bounds every unauthenticated slot
     * and therefore must be finite, not CBM_DAEMON_IPC_WAIT_FOREVER. */
    uint32_t max_clients;
    uint64_t lease_timeout_ms;
    uint32_t request_timeout_ms;
    /* Bounds cooperative stop and retained participant-guard cleanup. Must be
     * finite, not CBM_DAEMON_IPC_WAIT_FOREVER. */
    uint32_t shutdown_timeout_ms;
    /* Disabled when all handlers are NULL. Otherwise all five handlers are
     * required. Function pointers and context are copied; the context's owner
     * must keep it alive until service_free returns true. */
    cbm_daemon_runtime_application_callbacks_t application;
    /* Born via `daemon start`: the service does not begin stopping when its
     * last committed client disconnects; only the stop/drain ops or an
     * explicit process kill end it. */
    bool permanent;
} cbm_daemon_runtime_service_config_t;

typedef enum {
    CBM_DAEMON_RUNTIME_CONNECT_ERROR = 0,
    CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED = 1,
    CBM_DAEMON_RUNTIME_CONNECT_CONFLICT = 2,
    CBM_DAEMON_RUNTIME_CONNECT_REJECTED = 3,
} cbm_daemon_runtime_connect_status_t;

typedef struct {
    cbm_daemon_runtime_connect_status_t status;
    cbm_daemon_hello_status_t hello_status;
    cbm_daemon_client_id_t client_id;
    /* Kernel-authenticated PID of this client as observed by the daemon. */
    uint64_t authenticated_process_id;
    cbm_daemon_conflict_t conflict;
    char message[CBM_DAEMON_CONFLICT_MESSAGE_SIZE];
} cbm_daemon_runtime_connect_result_t;

/* The fixed HELLO request contains only rendezvous ABI, semantic version, and
 * exact executable SHA-256. It has no detailed ABI, client-ID, PID, user, or
 * endpoint fields. Strings are NUL-terminated and zero padded. Encoding and
 * validation depend only on these stable fields; detailed identity fields may
 * be zero or unknown without changing the bytes. */
bool cbm_daemon_runtime_hello_request_encode(uint8_t out[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE],
                                             const cbm_daemon_build_identity_t *identity);

/* Resolve process_id through the OS kernel's process metadata and hash the
 * executable image rather than reopening an unbound pathname. Linux hashes an
 * open /proc/<pid>/exe handle anchored to one process instance; macOS accepts
 * an opened path only while its vnode is present in that process's executable
 * mappings before and after hashing. Windows holds one process instance and a
 * stable, non-share-write image-file handle across the hash, with path and
 * creation-time snapshots around it (the Windows loader's image-section
 * replacement restriction supplies the final path-to-mapping binding).
 * Unsupported, inaccessible, replaced, or otherwise raced images fail closed.
 * The daemon hashes and retains its stable native image object at startup;
 * HELLO first compares a bracketed peer's mapped-image file identity with that
 * retained object. A provable identity match avoids re-hashing. Different or
 * unverifiable identities fall back to this full process-image fingerprint so
 * byte-identical copies remain compatible while changed copies are rejected.
 * Stateful HELLO admission requires the proven or computed fingerprint to
 * match both the claimed and active build fingerprints. */
bool cbm_daemon_runtime_process_build_fingerprint(uint64_t process_id,
                                                  char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]);

/* Ask any current daemon generation to drain before install/update/uninstall.
 * This is not a normal HELLO and never creates an application session. The
 * kernel-authenticated peer image must match identity->build_fingerprint, but
 * that build is deliberately allowed to differ from the active daemon. A true
 * return means a fixed response was received; inspect result_out->accepted.
 * timeout_ms must be finite. */
bool cbm_daemon_runtime_request_activation_shutdown(
    const cbm_daemon_ipc_endpoint_t *endpoint, const cbm_daemon_build_identity_t *identity,
    cbm_daemon_runtime_activation_action_t action, uint32_t timeout_ms,
    cbm_daemon_runtime_activation_result_t *result_out);

typedef struct {
    bool permanent;
    bool stopping;
    uint16_t committed_clients;
    uint32_t daemon_pid;
    uint8_t client_count; /* entries in client_pids, capped at the wire limit */
    uint32_t client_pids[CBM_DAEMON_CONTROL_CLIENT_CAP];
    char build_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    char semantic_version[12];
} cbm_daemon_runtime_status_t;

typedef struct {
    bool accepted; /* stopping was initiated */
    bool busy;     /* refused: committed clients hold the daemon */
    uint16_t committed_clients;
    uint8_t client_count;
    uint32_t client_pids[CBM_DAEMON_CONTROL_CLIENT_CAP];
} cbm_daemon_runtime_stop_result_t;

/* One-shot authenticated status probe (no cohort, no HELLO, no admission).
 * Works across build skew. A true return means a valid response arrived. */
bool cbm_daemon_runtime_request_status(const cbm_daemon_ipc_endpoint_t *endpoint,
                                       const cbm_daemon_build_identity_t *identity,
                                       uint32_t timeout_ms,
                                       cbm_daemon_runtime_status_t *status_out);

/* One-shot authenticated stop request. Refuses while committed clients exist
 * (result_out lists their pids); otherwise begins the stopping sequence, for
 * permanent generations too. */
bool cbm_daemon_runtime_request_stop(const cbm_daemon_ipc_endpoint_t *endpoint,
                                     const cbm_daemon_build_identity_t *identity,
                                     uint32_t timeout_ms,
                                     cbm_daemon_runtime_stop_result_t *result_out);

/* Performs the complete guarded first-participant handoff, starts listening
 * synchronously, then owns both that participant claim and its
 * accept/connection threads. All config scalar/text data is copied. endpoint
 * is borrowed only for this synchronous call and may be freed after a
 * successful return. Only one service may listen at an endpoint. */
cbm_daemon_runtime_service_t *cbm_daemon_runtime_service_start(
    const cbm_daemon_runtime_service_config_t *config);

/* Host-start variant for preparing inert application resources before the
 * listener. The caller must acquire the endpoint lifetime reservation first;
 * this lets the host defer watcher/UI/diagnostic threads until the call has
 * succeeded. A successful start transfers the reservation into the service
 * and clears *reservation_io. On failure it remains owned by the caller unless
 * the service had already adopted it and released it during rollback. */
cbm_daemon_runtime_service_t *cbm_daemon_runtime_service_start_reserved(
    const cbm_daemon_runtime_service_config_t *config,
    cbm_daemon_ipc_lifetime_reservation_t **reservation_io);

cbm_daemon_runtime_service_state_t cbm_daemon_runtime_service_state(
    cbm_daemon_runtime_service_t *service);
size_t cbm_daemon_runtime_service_active_clients(cbm_daemon_runtime_service_t *service);
/* Monotonic count of every admission since service start; never decremented.
 * Use for "has any client ever connected" decisions — the live count above
 * can read zero between two short-lived sessions. */
uint64_t cbm_daemon_runtime_service_clients_admitted_total(cbm_daemon_runtime_service_t *service);
/* Includes accepted connections still waiting for HELLO. Never exceeds the
 * configured max_clients; over-cap peers receive REJECTED before close. */
size_t cbm_daemon_runtime_service_active_connections(cbm_daemon_runtime_service_t *service);
size_t cbm_daemon_runtime_service_job_subscribers(cbm_daemon_runtime_service_t *service,
                                                  const char *project_key);
uint64_t cbm_daemon_runtime_service_client_process_id(cbm_daemon_runtime_service_t *service,
                                                      cbm_daemon_client_id_t client_id);

/* Wait functions use a monotonic deadline and never sleep past timeout_ms. */
bool cbm_daemon_runtime_service_wait_for_clients(cbm_daemon_runtime_service_t *service,
                                                 size_t expected, uint32_t timeout_ms);
bool cbm_daemon_runtime_service_wait_for_connections(cbm_daemon_runtime_service_t *service,
                                                     size_t expected, uint32_t timeout_ms);
bool cbm_daemon_runtime_service_wait_exited(cbm_daemon_runtime_service_t *service,
                                            uint32_t timeout_ms);

/* Worker supervisors report the end of two-phase cancellation through this
 * API. The service cannot exit while a cancelled job remains unreaped. */
bool cbm_daemon_runtime_service_job_reaped(cbm_daemon_runtime_service_t *service,
                                           const char *project_key);

/* Emergency/test teardown only. Normal lifetime is connection-owned: the
 * final disconnect makes STOPPING terminal, drains/reaps within the configured
 * bound, and exits automatically. stop is itself bounded by timeout_ms. */
bool cbm_daemon_runtime_service_stop(cbm_daemon_runtime_service_t *service, uint32_t timeout_ms);
/* Before wait_exited or a successful stop this returns false without teardown.
 * Otherwise it returns true only after every thread is joined, the owned
 * participant guard is released, and the service allocation is destroyed. A
 * false return retains the service and all retry authority; the caller must
 * retry or fail-stop the owning process. */
bool cbm_daemon_runtime_service_free(cbm_daemon_runtime_service_t *service);

/* A successful return is connection-bound. The daemon delivers an explicit
 * mismatch result (and persists it) before closing a rejected connection.
 * Client exchange timeouts must be finite, not CBM_DAEMON_IPC_WAIT_FOREVER. */
cbm_daemon_runtime_client_t *cbm_daemon_runtime_client_connect(
    const cbm_daemon_ipc_endpoint_t *endpoint, const cbm_daemon_build_identity_t *identity,
    uint32_t timeout_ms, cbm_daemon_runtime_connect_result_t *result_out);

cbm_daemon_client_id_t cbm_daemon_runtime_client_id(const cbm_daemon_runtime_client_t *client);
uint64_t cbm_daemon_runtime_client_process_id(const cbm_daemon_runtime_client_t *client);

cbm_daemon_subscription_result_t cbm_daemon_runtime_client_job_subscribe(
    cbm_daemon_runtime_client_t *client, const char *project_key,
    cbm_daemon_subscription_id_t *subscription_id_out, uint32_t timeout_ms);
bool cbm_daemon_runtime_client_job_unsubscribe(cbm_daemon_runtime_client_t *client,
                                               cbm_daemon_subscription_id_t subscription_id,
                                               uint32_t timeout_ms);
bool cbm_daemon_runtime_client_heartbeat(cbm_daemon_runtime_client_t *client, uint32_t timeout_ms);

/* Reserve a monotonically increasing token for the next application request.
 * Only one unstarted reservation may exist per client. It must be consumed by
 * request_tagged() (or the client must be closed) before another reservation.
 * Tokens are never reused within a client connection. */
bool cbm_daemon_runtime_client_application_token_reserve(
    cbm_daemon_runtime_client_t *client, cbm_daemon_runtime_application_token_t *token_out);

/* Send a one-way cancellation control for an exact reserved/active request.
 * ACCEPTED means the control was queued or written; callback completion is the
 * final cancellation linearization point. Stale/wrong tokens are harmless. */
cbm_daemon_runtime_cancel_result_t cbm_daemon_runtime_client_application_cancel(
    cbm_daemon_runtime_client_t *client, cbm_daemon_runtime_application_token_t request_token);

/* Executes one binary application request. Calls on one client are serialized.
 * response_out is malloc-owned on OK and must be freed by the caller; an empty
 * OK response is represented by NULL/0. Invalid/oversized requests are rejected
 * locally without poisoning an otherwise usable client. */
cbm_daemon_runtime_application_status_t cbm_daemon_runtime_client_application_request(
    cbm_daemon_runtime_client_t *client, const void *request, uint32_t request_length,
    uint8_t **response_out, uint32_t *response_length_out, uint32_t timeout_ms);

/* Execute using the sole outstanding token returned by token_reserve(). This
 * is the cancellable frontend path; the legacy helper above reserves
 * internally. */
cbm_daemon_runtime_application_status_t cbm_daemon_runtime_client_application_request_tagged(
    cbm_daemon_runtime_client_t *client, cbm_daemon_runtime_application_token_t request_token,
    const void *request, uint32_t request_length, uint8_t **response_out,
    uint32_t *response_length_out, uint32_t timeout_ms);

/* Begin a two-phase close without freeing the client. This atomically rejects
 * future exchanges and interrupts an exchange already in flight. Exactly one
 * caller may begin close. After all threads that could have entered a client
 * API are joined, close_finish closes/frees the retained handle. */
bool cbm_daemon_runtime_client_close_begin(cbm_daemon_runtime_client_t *client);

/* Finish a close begun by close_begin. Sends DISCONNECT when the transport was
 * not interrupted, always closes/frees the local handle, and returns whether
 * the daemon acknowledged DISCONNECT. The client is invalid after this call. */
bool cbm_daemon_runtime_client_close_finish(cbm_daemon_runtime_client_t *client,
                                            uint32_t timeout_ms);

/* One-call convenience wrapper around close_begin + close_finish. Ownership on
 * the server is tied to EOF as well as the explicit operation. This remains
 * safe with one exchange that was already in flight, but callers with a worker
 * that may be immediately about to enter an exchange must use the two-phase API
 * and join that worker before close_finish. */
bool cbm_daemon_runtime_client_close(cbm_daemon_runtime_client_t *client, uint32_t timeout_ms);

#endif /* CBM_DAEMON_RUNTIME_H */
