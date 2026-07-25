/*
 * runtime.c — Mandatory per-account daemon service over authenticated IPC.
 */
#include "daemon/runtime.h"

#include "daemon/ipc_internal.h"
#include "daemon/service_internal.h"

#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/platform.h"

#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#elif defined(__APPLE__)
#include <fcntl.h>
#include <libproc.h>
#include <mach/vm_prot.h>
#include <sys/proc_info.h>
#include <sys/stat.h>
#include <unistd.h>
#elif defined(__linux__)
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#endif

enum {
    RUNTIME_MAX_CLIENTS_HARD = 64,
    RUNTIME_ACCEPT_POLL_MS = 20,
    RUNTIME_WAIT_POLL_NS = 1000000,
    RUNTIME_WORKER_STACK_SIZE = 256 * 1024,
    /* Bounds the drain-before-close wait for a peer consuming its final
     * response; interrupted or already-poisoned connections skip it. */
    RUNTIME_WORKER_DRAIN_TIMEOUT_MS = 2000,
    /* Inline rejections run on the accept thread, so their drain stays short:
     * a local cooperative peer is already blocked in read and consumes the
     * rejection within milliseconds. */
    RUNTIME_REJECT_DRAIN_TIMEOUT_MS = 250,
    RUNTIME_PATH_CAP = 4096,

    RENDEZVOUS_REQUEST_ABI_OFFSET = 0,
    RENDEZVOUS_REQUEST_VERSION_OFFSET = 4,
    RENDEZVOUS_REQUEST_BUILD_OFFSET =
        RENDEZVOUS_REQUEST_VERSION_OFFSET + CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,

    RENDEZVOUS_RESPONSE_CONNECT_STATUS_OFFSET = 0,
    RENDEZVOUS_RESPONSE_HELLO_STATUS_OFFSET = 4,
    RENDEZVOUS_RESPONSE_CLIENT_ID_OFFSET = 8,
    RENDEZVOUS_RESPONSE_PROCESS_ID_OFFSET = 16,
    RENDEZVOUS_RESPONSE_CONFLICT_STATUS_OFFSET = 24,
    RENDEZVOUS_RESPONSE_ACTIVE_VERSION_OFFSET = 28,
    RENDEZVOUS_RESPONSE_ACTIVE_BUILD_OFFSET =
        RENDEZVOUS_RESPONSE_ACTIVE_VERSION_OFFSET + CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,
    RENDEZVOUS_RESPONSE_REQUESTED_VERSION_OFFSET =
        RENDEZVOUS_RESPONSE_ACTIVE_BUILD_OFFSET + CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP,
    RENDEZVOUS_RESPONSE_REQUESTED_BUILD_OFFSET =
        RENDEZVOUS_RESPONSE_REQUESTED_VERSION_OFFSET + CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,
    RENDEZVOUS_RESPONSE_MESSAGE_OFFSET =
        RENDEZVOUS_RESPONSE_REQUESTED_BUILD_OFFSET + CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP,

    ACTIVATION_REQUEST_ACTION_OFFSET = 0,
    ACTIVATION_REQUEST_IDENTITY_OFFSET = 4,
    ACTIVATION_RESPONSE_ABI_OFFSET = 0,
    ACTIVATION_RESPONSE_ACCEPTED_OFFSET = 4,
    ACTIVATION_RESPONSE_CLIENTS_OFFSET = 8,
    ACTIVATION_RESPONSE_CONNECTIONS_OFFSET = 16,

    STATUS_RESPONSE_SIZE = 4,
    SUBSCRIBE_REQUEST_PREFIX_SIZE = 4,
    SUBSCRIBE_RESPONSE_SIZE = 12,
    UNSUBSCRIBE_REQUEST_SIZE = 8,
    APPLICATION_CANCEL_REQUEST_SIZE = 8,
    APPLICATION_REQUEST_PREFIX_SIZE = 12,
    APPLICATION_RESPONSE_PREFIX_SIZE = 16,
};

_Static_assert(RENDEZVOUS_REQUEST_BUILD_OFFSET + CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP ==
                   CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE,
               "rendezvous request layout changed");
_Static_assert(RENDEZVOUS_RESPONSE_MESSAGE_OFFSET + CBM_DAEMON_RENDEZVOUS_MESSAGE_CAP ==
                   CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE,
               "rendezvous response layout changed");
_Static_assert(ACTIVATION_REQUEST_IDENTITY_OFFSET + CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE ==
                       CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE &&
                   ACTIVATION_RESPONSE_CONNECTIONS_OFFSET + 8 ==
                       CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE,
               "activation shutdown layout changed");
_Static_assert(CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP == CBM_DAEMON_VERSION_TEXT_SIZE,
               "service version capacity requires a new rendezvous strategy");
_Static_assert(CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP == CBM_DAEMON_BUILD_FINGERPRINT_SIZE,
               "service fingerprint capacity requires a new rendezvous strategy");
_Static_assert(CBM_DAEMON_RENDEZVOUS_MESSAGE_CAP == CBM_DAEMON_CONFLICT_MESSAGE_SIZE,
               "service message capacity requires a new rendezvous strategy");
_Static_assert(CBM_DAEMON_RUNTIME_CONNECT_ERROR == 0 && CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED == 1 &&
                   CBM_DAEMON_RUNTIME_CONNECT_CONFLICT == 2 &&
                   CBM_DAEMON_RUNTIME_CONNECT_REJECTED == 3,
               "rendezvous connect status values changed");
_Static_assert(CBM_DAEMON_HELLO_INVALID == 0 && CBM_DAEMON_HELLO_COMPATIBLE == 1 &&
                   CBM_DAEMON_HELLO_VERSION_CONFLICT == 2 && CBM_DAEMON_HELLO_BUILD_CONFLICT == 3 &&
                   CBM_DAEMON_HELLO_PROTOCOL_ABI_CONFLICT == 4 &&
                   CBM_DAEMON_HELLO_STORE_ABI_CONFLICT == 5 &&
                   CBM_DAEMON_HELLO_FEATURE_ABI_CONFLICT == 6,
               "rendezvous hello status values changed");
_Static_assert(CBM_DAEMON_RENDEZVOUS_FRAME_VERSION == 1 && CBM_DAEMON_RENDEZVOUS_ABI == 1 &&
                   CBM_DAEMON_FRAME_REQUEST == 1 && CBM_DAEMON_FRAME_RESPONSE == 2 &&
                   CBM_DAEMON_RUNTIME_OP_HELLO == 1 &&
                   CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN == 8 &&
                   CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL == 1 &&
                   CBM_DAEMON_RUNTIME_ACTIVATION_UPDATE == 2 &&
                   CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL == 3,
               "rendezvous framing values changed");

typedef struct cbm_daemon_runtime_worker cbm_daemon_runtime_worker_t;

/* The service keeps the exact native image object whose bytes were hashed at
 * startup.  HELLO can then prove that a peer maps this same, still-stable file
 * object without reading the executable again.  A different file identity is
 * not a rejection by itself: identical copies remain supported through the
 * full fingerprint fallback. */
typedef struct {
    bool held;
#ifdef _WIN32
    HANDLE file;
    BY_HANDLE_FILE_INFORMATION information;
    LARGE_INTEGER size;
#elif defined(__APPLE__) || defined(__linux__)
    int fd;
    struct stat status;
#endif
} runtime_process_image_reference_t;

struct cbm_daemon_runtime_service {
    cbm_mutex_t mutex;
    cbm_daemon_ipc_listener_t *listener;
    cbm_daemon_coordinator_t *coordinator;
    cbm_daemon_runtime_worker_t *workers;
    size_t worker_capacity;
    size_t worker_mutexes_initialized;
    size_t active_connections;
    size_t committed_clients;
    /* Monotonic count of every admission since service start. The host's
     * nobody-ever-connected window latches on this instead of sampling the
     * live client count: a short-lived first session can begin and end
     * entirely between two host polls, and a sampled count then reads zero
     * forever, stopping a daemon whose client is mid-conversation. */
    uint64_t admitted_total;
    /* Born via `daemon start`: last-client-disconnect does not begin stopping.
     * The stop/drain ops and process kill remain the only exits. */
    bool permanent;

    cbm_thread_t accept_thread;
    bool accept_thread_started;
    bool accept_thread_joined;
    atomic_bool accept_thread_done;

    cbm_daemon_runtime_service_state_t state;
    uint64_t stop_deadline_ms;
    bool emergency_stop;
    bool activation_shutdown_requested;
    bool activation_response_inflight;

    char semantic_version[CBM_DAEMON_VERSION_TEXT_SIZE];
    char build_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    cbm_daemon_build_identity_t identity;
    runtime_process_image_reference_t active_image;
    char *conflict_log_path;
    size_t conflict_log_cap_bytes;
    uint64_t lease_timeout_ms;
    uint32_t request_timeout_ms;
    uint32_t shutdown_timeout_ms;
    cbm_daemon_runtime_application_callbacks_t application;
    /* Owned only by the convenience start() path. start_reserved() callers
     * retain their externally managed participant guard. */
    cbm_daemon_ipc_participant_guard_t *owned_participant_guard;
};

struct cbm_daemon_runtime_worker {
    cbm_daemon_runtime_service_t *service;
    cbm_mutex_t send_mutex;
    cbm_thread_t thread;
    bool thread_started;
    bool in_use;
    bool joining;
    atomic_bool done;
    cbm_daemon_ipc_connection_t *connection;
    cbm_daemon_client_id_t client_id;
    uint64_t peer_process_id;
    bool admitted;
    bool admission_committed;
    bool final_response_inflight;

    cbm_daemon_runtime_application_session_t *application_session;
    bool application_session_opened;
    bool application_cancelled;
    atomic_bool disconnecting;

    cbm_thread_t application_thread;
    bool application_thread_started;
    atomic_bool application_thread_done;
    cbm_daemon_runtime_application_token_t application_request_token;
    cbm_daemon_runtime_application_token_t last_application_request_token;
    uint8_t *application_request;
    uint32_t application_request_length;
};

struct cbm_daemon_runtime_client {
    cbm_mutex_t exchange_mutex;
    cbm_mutex_t send_mutex;
    cbm_mutex_t state_mutex;
    cbm_daemon_ipc_connection_t *connection;
    cbm_daemon_client_id_t client_id;
    uint64_t authenticated_process_id;
    bool usable;
    bool exchange_active;
    bool closing;
    bool close_interrupted_exchange;
    cbm_daemon_runtime_application_token_t next_application_token;
    cbm_daemon_runtime_application_token_t last_started_application_token;
    cbm_daemon_runtime_application_token_t active_application_token;
    cbm_daemon_runtime_application_token_t pending_cancel_token;
    bool application_request_sent;
    bool application_cancel_sent;
};

static uint64_t runtime_deadline_after(uint32_t timeout_ms) {
    uint64_t now_ms = cbm_now_ms();
    if (UINT64_MAX - now_ms < (uint64_t)timeout_ms) {
        return UINT64_MAX;
    }
    return now_ms + (uint64_t)timeout_ms;
}

static void runtime_wait_tick(uint64_t deadline_ms) {
    uint64_t now_ms = cbm_now_ms();
    if (now_ms >= deadline_ms) {
        return;
    }
    uint64_t remaining_ms = deadline_ms - now_ms;
    struct timespec pause = {
        .tv_sec = 0,
        .tv_nsec = remaining_ms > 1 ? RUNTIME_WAIT_POLL_NS : (long)(remaining_ms * 1000000ULL),
    };
    (void)cbm_nanosleep(&pause, NULL);
}

static void runtime_put_u32(uint8_t *out, uint32_t value) {
    out[0] = (uint8_t)(value >> 24);
    out[1] = (uint8_t)(value >> 16);
    out[2] = (uint8_t)(value >> 8);
    out[3] = (uint8_t)value;
}

static uint32_t runtime_get_u32(const uint8_t *in) {
    return ((uint32_t)in[0] << 24) | ((uint32_t)in[1] << 16) | ((uint32_t)in[2] << 8) |
           (uint32_t)in[3];
}

static void runtime_put_u64(uint8_t *out, uint64_t value) {
    for (size_t i = 0; i < 8; i++) {
        out[i] = (uint8_t)(value >> (56 - i * 8));
    }
}

static uint64_t runtime_get_u64(const uint8_t *in) {
    uint64_t value = 0;
    for (size_t i = 0; i < 8; i++) {
        value = (value << 8) | in[i];
    }
    return value;
}

static bool runtime_bounded_length(const char *value, size_t capacity, size_t *length_out) {
    if (!value || capacity == 0) {
        return false;
    }
    for (size_t i = 0; i < capacity; i++) {
        if (value[i] == '\0') {
            if (length_out) {
                *length_out = i;
            }
            return true;
        }
    }
    return false;
}

static bool runtime_wire_string_encode(uint8_t *out, size_t capacity, const char *value,
                                       bool allow_empty) {
    size_t length = 0;
    if (!out || !runtime_bounded_length(value, capacity, &length) ||
        (!allow_empty && length == 0)) {
        return false;
    }
    memset(out, 0, capacity);
    memcpy(out, value, length);
    return true;
}

static bool runtime_wire_string_decode(const uint8_t *in, size_t capacity, char *out,
                                       bool allow_empty) {
    if (!in || !out || capacity == 0) {
        return false;
    }
    size_t length = capacity;
    for (size_t i = 0; i < capacity; i++) {
        if (in[i] == 0) {
            length = i;
            break;
        }
    }
    if (length == capacity || (!allow_empty && length == 0)) {
        return false;
    }
    for (size_t i = length + 1; i < capacity; i++) {
        if (in[i] != 0) {
            return false;
        }
    }
    memcpy(out, in, length);
    out[length] = '\0';
    return true;
}

/* Reuse the service layer's canonical printable-version and lowercase-SHA256
 * validation without importing detailed ABI values into rendezvous. Fixed
 * nonzero sentinels on both sides make only version/build ordering observable. */
static cbm_daemon_hello_status_t runtime_rendezvous_compare(const char *active_version,
                                                            const char *active_build,
                                                            const char *requested_version,
                                                            const char *requested_build,
                                                            cbm_daemon_conflict_t *conflict_out) {
    cbm_daemon_build_identity_t active = {
        .semantic_version = active_version,
        .build_fingerprint = active_build,
        .protocol_abi = 1,
        .store_abi = 1,
        .feature_abi = 1,
    };
    cbm_daemon_build_identity_t requested = {
        .semantic_version = requested_version,
        .build_fingerprint = requested_build,
        .protocol_abi = 1,
        .store_abi = 1,
        .feature_abi = 1,
    };
    cbm_daemon_hello_status_t status = cbm_daemon_hello_compare(&active, &requested, conflict_out);
    if (status != CBM_DAEMON_HELLO_INVALID && status != CBM_DAEMON_HELLO_COMPATIBLE &&
        status != CBM_DAEMON_HELLO_VERSION_CONFLICT && status != CBM_DAEMON_HELLO_BUILD_CONFLICT) {
        if (conflict_out) {
            memset(conflict_out, 0, sizeof(*conflict_out));
            conflict_out->status = CBM_DAEMON_HELLO_INVALID;
        }
        return CBM_DAEMON_HELLO_INVALID;
    }
    return status;
}

bool cbm_daemon_runtime_hello_request_encode(uint8_t out[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE],
                                             const cbm_daemon_build_identity_t *identity) {
    if (!out || !identity) {
        return false;
    }
    cbm_daemon_conflict_t validation;
    if (runtime_rendezvous_compare(identity->semantic_version, identity->build_fingerprint,
                                   identity->semantic_version, identity->build_fingerprint,
                                   &validation) != CBM_DAEMON_HELLO_COMPATIBLE) {
        return false;
    }
    memset(out, 0, CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE);
    runtime_put_u32(out + RENDEZVOUS_REQUEST_ABI_OFFSET, CBM_DAEMON_RENDEZVOUS_ABI);
    return runtime_wire_string_encode(out + RENDEZVOUS_REQUEST_VERSION_OFFSET,
                                      CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,
                                      identity->semantic_version, false) &&
           runtime_wire_string_encode(out + RENDEZVOUS_REQUEST_BUILD_OFFSET,
                                      CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP,
                                      identity->build_fingerprint, false);
}

static bool runtime_rendezvous_request_decode(
    const uint8_t payload[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE],
    char version[CBM_DAEMON_VERSION_TEXT_SIZE], char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!payload || !version || !build ||
        runtime_get_u32(payload + RENDEZVOUS_REQUEST_ABI_OFFSET) != CBM_DAEMON_RENDEZVOUS_ABI ||
        !runtime_wire_string_decode(payload + RENDEZVOUS_REQUEST_VERSION_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP, version, false) ||
        !runtime_wire_string_decode(payload + RENDEZVOUS_REQUEST_BUILD_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP, build, false)) {
        return false;
    }
    cbm_daemon_conflict_t validation;
    return runtime_rendezvous_compare(version, build, version, build, &validation) ==
           CBM_DAEMON_HELLO_COMPATIBLE;
}

static bool runtime_activation_action_valid(cbm_daemon_runtime_activation_action_t action) {
    return action == CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL ||
           action == CBM_DAEMON_RUNTIME_ACTIVATION_UPDATE ||
           action == CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL;
}

static const char *runtime_activation_action_text(cbm_daemon_runtime_activation_action_t action) {
    switch (action) {
    case CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL:
        return "install";
    case CBM_DAEMON_RUNTIME_ACTIVATION_UPDATE:
        return "update";
    case CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL:
        return "uninstall";
    default:
        return "invalid";
    }
}

static bool runtime_activation_request_encode(
    uint8_t out[CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE],
    cbm_daemon_runtime_activation_action_t action, const cbm_daemon_build_identity_t *identity) {
    if (!out || !identity || !runtime_activation_action_valid(action)) {
        return false;
    }
    memset(out, 0, CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE);
    runtime_put_u32(out + ACTIVATION_REQUEST_ACTION_OFFSET, (uint32_t)action);
    return cbm_daemon_runtime_hello_request_encode(out + ACTIVATION_REQUEST_IDENTITY_OFFSET,
                                                   identity);
}

static bool runtime_activation_request_decode(
    const uint8_t payload[CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE],
    cbm_daemon_runtime_activation_action_t *action_out, char version[CBM_DAEMON_VERSION_TEXT_SIZE],
    char build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!payload || !action_out || !version || !build) {
        return false;
    }
    uint32_t raw_action = runtime_get_u32(payload + ACTIVATION_REQUEST_ACTION_OFFSET);
    if (raw_action < (uint32_t)CBM_DAEMON_RUNTIME_ACTIVATION_INSTALL ||
        raw_action > (uint32_t)CBM_DAEMON_RUNTIME_ACTIVATION_UNINSTALL ||
        !runtime_rendezvous_request_decode(payload + ACTIVATION_REQUEST_IDENTITY_OFFSET, version,
                                           build)) {
        return false;
    }
    cbm_daemon_runtime_activation_action_t action =
        (cbm_daemon_runtime_activation_action_t)raw_action;
    *action_out = action;
    return true;
}

static bool runtime_activation_response_encode(
    uint8_t out[CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE],
    const cbm_daemon_runtime_activation_result_t *result) {
    if (!out || !result) {
        return false;
    }
    memset(out, 0, CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE);
    runtime_put_u32(out + ACTIVATION_RESPONSE_ABI_OFFSET, CBM_DAEMON_RENDEZVOUS_ABI);
    runtime_put_u32(out + ACTIVATION_RESPONSE_ACCEPTED_OFFSET, result->accepted ? 1U : 0U);
    runtime_put_u64(out + ACTIVATION_RESPONSE_CLIENTS_OFFSET, result->active_clients);
    runtime_put_u64(out + ACTIVATION_RESPONSE_CONNECTIONS_OFFSET, result->active_connections);
    return true;
}

static bool runtime_activation_response_decode(
    const uint8_t payload[CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE],
    cbm_daemon_runtime_activation_result_t *result_out) {
    if (!payload || !result_out ||
        runtime_get_u32(payload + ACTIVATION_RESPONSE_ABI_OFFSET) != CBM_DAEMON_RENDEZVOUS_ABI) {
        return false;
    }
    uint32_t accepted = runtime_get_u32(payload + ACTIVATION_RESPONSE_ACCEPTED_OFFSET);
    if (accepted > 1U) {
        return false;
    }
    result_out->accepted = accepted == 1U;
    result_out->active_clients = runtime_get_u64(payload + ACTIVATION_RESPONSE_CLIENTS_OFFSET);
    result_out->active_connections =
        runtime_get_u64(payload + ACTIVATION_RESPONSE_CONNECTIONS_OFFSET);
    return true;
}

static uint64_t runtime_current_process_id(void) {
#ifdef _WIN32
    return (uint64_t)GetCurrentProcessId();
#elif defined(__APPLE__) || defined(__linux__)
    return (uint64_t)getpid();
#else
    return 0;
#endif
}

static void runtime_process_image_reference_init(runtime_process_image_reference_t *reference) {
    if (!reference) {
        return;
    }
    memset(reference, 0, sizeof(*reference));
#ifdef _WIN32
    reference->file = INVALID_HANDLE_VALUE;
#elif defined(__APPLE__) || defined(__linux__)
    reference->fd = -1;
#endif
}

static bool runtime_process_image_reference_release(runtime_process_image_reference_t *reference) {
    if (!reference) {
        return false;
    }
    bool ok = true;
#ifdef _WIN32
    if (reference->file != INVALID_HANDLE_VALUE && !CloseHandle(reference->file)) {
        ok = false;
    }
#elif defined(__APPLE__) || defined(__linux__)
    if (reference->fd >= 0 && close(reference->fd) != 0) {
        ok = false;
    }
#endif
    runtime_process_image_reference_init(reference);
    return ok;
}

#ifdef _WIN32

typedef struct {
    FILETIME creation_time;
    wchar_t path[32768];
} runtime_windows_process_image_snapshot_t;

static bool runtime_windows_process_image_snapshot(
    HANDLE process, runtime_windows_process_image_snapshot_t *snapshot) {
    if (!process || !snapshot) {
        return false;
    }
    memset(snapshot, 0, sizeof(*snapshot));
    FILETIME exit_time;
    FILETIME kernel_time;
    FILETIME user_time;
    DWORD exit_code = 0;
    DWORD path_length = (DWORD)(sizeof(snapshot->path) / sizeof(snapshot->path[0]));
    bool ok = GetProcessTimes(process, &snapshot->creation_time, &exit_time, &kernel_time,
                              &user_time) != 0 &&
              GetExitCodeProcess(process, &exit_code) != 0 && exit_code == STILL_ACTIVE &&
              QueryFullProcessImageNameW(process, 0, snapshot->path, &path_length) != 0 &&
              path_length > 0 &&
              path_length < (DWORD)(sizeof(snapshot->path) / sizeof(snapshot->path[0]));
    if (ok) {
        snapshot->path[path_length] = L'\0';
    }
    return ok;
}

static bool runtime_windows_process_image_snapshot_same(
    const runtime_windows_process_image_snapshot_t *first,
    const runtime_windows_process_image_snapshot_t *second) {
    return first && second && CompareFileTime(&first->creation_time, &second->creation_time) == 0 &&
           CompareStringOrdinal(first->path, -1, second->path, -1, TRUE) == CSTR_EQUAL;
}

static bool runtime_windows_file_snapshot(HANDLE file, BY_HANDLE_FILE_INFORMATION *information,
                                          LARGE_INTEGER *size) {
    if (!information || !size) {
        return false;
    }
    memset(information, 0, sizeof(*information));
    size->QuadPart = -1;
    return file != INVALID_HANDLE_VALUE && GetFileType(file) == FILE_TYPE_DISK &&
           GetFileInformationByHandle(file, information) != 0 &&
           (information->dwFileAttributes &
            (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
           GetFileSizeEx(file, size) != 0 && size->QuadPart >= 0;
}

static bool runtime_windows_file_snapshot_same(const BY_HANDLE_FILE_INFORMATION *first_information,
                                               const LARGE_INTEGER *first_size,
                                               const BY_HANDLE_FILE_INFORMATION *second_information,
                                               const LARGE_INTEGER *second_size) {
    return first_information && first_size && second_information && second_size &&
           first_information->dwVolumeSerialNumber == second_information->dwVolumeSerialNumber &&
           first_information->nFileIndexHigh == second_information->nFileIndexHigh &&
           first_information->nFileIndexLow == second_information->nFileIndexLow &&
           first_size->QuadPart == second_size->QuadPart &&
           CompareFileTime(&first_information->ftLastWriteTime,
                           &second_information->ftLastWriteTime) == 0;
}

#elif defined(__APPLE__)

static bool runtime_mac_process_instance(int process_id, struct proc_bsdinfo *info) {
    if (!info) {
        return false;
    }
    memset(info, 0, sizeof(*info));
    return proc_pidinfo(process_id, PROC_PIDTBSDINFO, 0, info, (int)sizeof(*info)) ==
               (int)sizeof(*info) &&
           info->pbi_pid == (uint32_t)process_id;
}

static bool runtime_mac_process_instance_same(const struct proc_bsdinfo *first,
                                              const struct proc_bsdinfo *second) {
    return first && second && first->pbi_pid == second->pbi_pid &&
           first->pbi_start_tvsec == second->pbi_start_tvsec &&
           first->pbi_start_tvusec == second->pbi_start_tvusec;
}

static bool runtime_mac_stat_matches_vnode(const struct stat *status,
                                           const struct vinfo_stat *vnode) {
    return status && vnode && S_ISREG(vnode->vst_mode) &&
           vnode->vst_dev == (uint32_t)status->st_dev &&
           vnode->vst_ino == (uint64_t)status->st_ino && vnode->vst_size == status->st_size &&
           vnode->vst_mtime == status->st_mtimespec.tv_sec &&
           vnode->vst_mtimensec == status->st_mtimespec.tv_nsec &&
           vnode->vst_ctime == status->st_ctimespec.tv_sec &&
           vnode->vst_ctimensec == status->st_ctimespec.tv_nsec;
}

static bool runtime_mac_stat_same(const struct stat *first, const struct stat *second) {
    return first && second && first->st_dev == second->st_dev && first->st_ino == second->st_ino &&
           first->st_size == second->st_size &&
           first->st_mtimespec.tv_sec == second->st_mtimespec.tv_sec &&
           first->st_mtimespec.tv_nsec == second->st_mtimespec.tv_nsec &&
           first->st_ctimespec.tv_sec == second->st_ctimespec.tv_sec &&
           first->st_ctimespec.tv_nsec == second->st_ctimespec.tv_nsec;
}

static bool runtime_mac_process_maps_file_executable(int process_id, const struct stat *status) {
    uint64_t address = 0;
    for (size_t region_count = 0; region_count < 65536; region_count++) {
        struct proc_regionwithpathinfo region;
        memset(&region, 0, sizeof(region));
        int received =
            proc_pidinfo(process_id, PROC_PIDREGIONPATHINFO, address, &region, (int)sizeof(region));
        if (received != (int)sizeof(region)) {
            return false;
        }
        if ((region.prp_prinfo.pri_protection & VM_PROT_EXECUTE) != 0 &&
            runtime_mac_stat_matches_vnode(status, &region.prp_vip.vip_vi.vi_stat)) {
            return true;
        }
        uint64_t region_address = region.prp_prinfo.pri_address;
        uint64_t region_size = region.prp_prinfo.pri_size;
        if (region_size == 0 || region_address > UINT64_MAX - region_size) {
            return false;
        }
        uint64_t next = region_address + region_size;
        if (next <= address) {
            return false;
        }
        address = next;
    }
    return false;
}

#elif defined(__linux__)

static bool runtime_linux_stat_same_image(const struct stat *first, const struct stat *second) {
    return first && second && S_ISREG(first->st_mode) && S_ISREG(second->st_mode) &&
           first->st_dev == second->st_dev && first->st_ino == second->st_ino &&
           first->st_size == second->st_size && first->st_mtim.tv_sec == second->st_mtim.tv_sec &&
           first->st_mtim.tv_nsec == second->st_mtim.tv_nsec &&
           first->st_ctim.tv_sec == second->st_ctim.tv_sec &&
           first->st_ctim.tv_nsec == second->st_ctim.tv_nsec;
}

#endif

/* Acquire one process instance's mapped image as a native object. Supplying a
 * fingerprint hashes that same held object; NULL performs metadata-only
 * acquisition for the HELLO fast path. Every platform brackets the process
 * identity/image lookup before returning the retained reference. */
static bool runtime_process_image_reference_acquire(
    uint64_t process_id, runtime_process_image_reference_t *reference,
    char fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!reference || process_id == 0) {
        return false;
    }
    runtime_process_image_reference_init(reference);
    if (fingerprint) {
        fingerprint[0] = '\0';
    }
#ifdef _WIN32
    if (process_id > MAXDWORD) {
        return false;
    }
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, (DWORD)process_id);
    if (!process) {
        return false;
    }
    runtime_windows_process_image_snapshot_t process_before;
    runtime_windows_process_image_snapshot_t process_after;
    BY_HANDLE_FILE_INFORMATION file_before;
    BY_HANDLE_FILE_INFORMATION file_after;
    LARGE_INTEGER size_before;
    LARGE_INTEGER size_after;
    bool ok = runtime_windows_process_image_snapshot(process, &process_before);
    HANDLE file = ok ? CreateFileW(process_before.path, GENERIC_READ,
                                   FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                                   FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, NULL)
                     : INVALID_HANDLE_VALUE;
    ok = ok && runtime_windows_file_snapshot(file, &file_before, &size_before) &&
         (!fingerprint || cbm_daemon_build_fingerprint_native_file((uintptr_t)file, fingerprint)) &&
         runtime_windows_file_snapshot(file, &file_after, &size_after) &&
         runtime_windows_file_snapshot_same(&file_before, &size_before, &file_after, &size_after) &&
         runtime_windows_process_image_snapshot(process, &process_after) &&
         runtime_windows_process_image_snapshot_same(&process_before, &process_after);
    if (!CloseHandle(process)) {
        ok = false;
    }
    if (ok) {
        reference->held = true;
        reference->file = file;
        reference->information = file_after;
        reference->size = size_after;
    } else if (file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(file);
    }
#elif defined(__APPLE__)
    if (process_id > INT_MAX) {
        return false;
    }
    int pid = (int)process_id;
    struct proc_bsdinfo process_before;
    struct proc_bsdinfo process_after;
    char path[PROC_PIDPATHINFO_MAXSIZE];
    int path_length = 0;
    bool ok = runtime_mac_process_instance(pid, &process_before) &&
              (path_length = proc_pidpath(pid, path, sizeof(path))) > 0 &&
              path_length < (int)sizeof(path);
    if (ok) {
        path[path_length] = '\0';
    }
    int fd = ok ? open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK) : -1;
    struct stat file_before;
    struct stat file_after;
    ok = ok && fd >= 0 && fstat(fd, &file_before) == 0 && S_ISREG(file_before.st_mode) &&
         runtime_mac_process_maps_file_executable(pid, &file_before) &&
         (!fingerprint || cbm_daemon_build_fingerprint_native_file((uintptr_t)fd, fingerprint)) &&
         fstat(fd, &file_after) == 0 && runtime_mac_stat_same(&file_before, &file_after) &&
         runtime_mac_process_maps_file_executable(pid, &file_after) &&
         runtime_mac_process_instance(pid, &process_after) &&
         runtime_mac_process_instance_same(&process_before, &process_after);
    if (ok) {
        reference->held = true;
        reference->fd = fd;
        reference->status = file_after;
    } else if (fd >= 0) {
        (void)close(fd);
    }
#elif defined(__linux__)
    char proc_path[64];
    int written =
        snprintf(proc_path, sizeof(proc_path), "/proc/%llu", (unsigned long long)process_id);
    if (written <= 0 || written >= (int)sizeof(proc_path)) {
        return false;
    }
    int process_fd = open(proc_path, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    int image_fd = process_fd >= 0 ? openat(process_fd, "exe", O_RDONLY | O_CLOEXEC) : -1;
    struct stat image_before;
    struct stat image_after;
    bool ok = image_fd >= 0 && fstat(image_fd, &image_before) == 0 &&
              S_ISREG(image_before.st_mode) &&
              (!fingerprint ||
               cbm_daemon_build_fingerprint_native_file((uintptr_t)image_fd, fingerprint)) &&
              fstat(image_fd, &image_after) == 0 &&
              runtime_linux_stat_same_image(&image_before, &image_after);
    int verify_fd = ok ? openat(process_fd, "exe", O_RDONLY | O_CLOEXEC) : -1;
    struct stat verify_status;
    ok = ok && verify_fd >= 0 && fstat(verify_fd, &verify_status) == 0 &&
         runtime_linux_stat_same_image(&image_after, &verify_status);
    if (verify_fd >= 0 && close(verify_fd) != 0) {
        ok = false;
    }
    if (process_fd >= 0 && close(process_fd) != 0) {
        ok = false;
    }
    if (ok) {
        reference->held = true;
        reference->fd = image_fd;
        reference->status = image_after;
    } else if (image_fd >= 0) {
        (void)close(image_fd);
    }
#else
    (void)process_id;
    bool ok = false;
#endif
    if (!ok && fingerprint) {
        fingerprint[0] = '\0';
    }
    return ok;
}

/* A false result means only "not proven identical". The caller must use the
 * full fingerprint fallback before rejecting the peer. */
static bool runtime_process_image_reference_matches_process(
    const runtime_process_image_reference_t *active, uint64_t process_id) {
    if (!active || !active->held || process_id == 0) {
        return false;
    }
#ifdef _WIN32
    runtime_process_image_reference_t peer;
    runtime_process_image_reference_init(&peer);
    bool same = runtime_process_image_reference_acquire(process_id, &peer, NULL);
    BY_HANDLE_FILE_INFORMATION active_now;
    LARGE_INTEGER active_size_now;
    same = same && runtime_windows_file_snapshot(active->file, &active_now, &active_size_now) &&
           runtime_windows_file_snapshot_same(&active->information, &active->size, &active_now,
                                              &active_size_now) &&
           runtime_windows_file_snapshot_same(&active->information, &active->size,
                                              &peer.information, &peer.size);
    bool released = runtime_process_image_reference_release(&peer);
    return same && released;
#elif defined(__APPLE__)
    runtime_process_image_reference_t peer;
    runtime_process_image_reference_init(&peer);
    bool same = runtime_process_image_reference_acquire(process_id, &peer, NULL);
    struct stat active_now;
    same = same && fstat(active->fd, &active_now) == 0 &&
           runtime_mac_stat_same(&active->status, &active_now) &&
           runtime_mac_stat_same(&active->status, &peer.status);
    bool released = runtime_process_image_reference_release(&peer);
    return same && released;
#elif defined(__linux__)
    runtime_process_image_reference_t peer;
    runtime_process_image_reference_init(&peer);
    bool same = runtime_process_image_reference_acquire(process_id, &peer, NULL);
    struct stat active_now;
    same = same && fstat(active->fd, &active_now) == 0 &&
           runtime_linux_stat_same_image(&active->status, &active_now) &&
           runtime_linux_stat_same_image(&active->status, &peer.status);
    bool released = runtime_process_image_reference_release(&peer);
    return same && released;
#else
    (void)process_id;
    return false;
#endif
}

bool cbm_daemon_runtime_process_build_fingerprint(uint64_t process_id,
                                                  char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    runtime_process_image_reference_t reference;
    runtime_process_image_reference_init(&reference);
    bool ok = runtime_process_image_reference_acquire(process_id, &reference, out);
    if (!runtime_process_image_reference_release(&reference)) {
        ok = false;
    }
    if (!ok) {
        out[0] = '\0';
    }
    return ok;
}

static bool runtime_hello_response_encode(uint8_t out[CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE],
                                          const cbm_daemon_runtime_connect_result_t *result) {
    if (!out || !result) {
        return false;
    }
    memset(out, 0, CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE);
    runtime_put_u32(out + RENDEZVOUS_RESPONSE_CONNECT_STATUS_OFFSET, (uint32_t)result->status);
    runtime_put_u32(out + RENDEZVOUS_RESPONSE_HELLO_STATUS_OFFSET, (uint32_t)result->hello_status);
    runtime_put_u64(out + RENDEZVOUS_RESPONSE_CLIENT_ID_OFFSET, result->client_id);
    runtime_put_u64(out + RENDEZVOUS_RESPONSE_PROCESS_ID_OFFSET, result->authenticated_process_id);
    runtime_put_u32(out + RENDEZVOUS_RESPONSE_CONFLICT_STATUS_OFFSET,
                    (uint32_t)result->conflict.status);

    if (!runtime_wire_string_encode(out + RENDEZVOUS_RESPONSE_ACTIVE_VERSION_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,
                                    result->conflict.active_version, true) ||
        !runtime_wire_string_encode(out + RENDEZVOUS_RESPONSE_ACTIVE_BUILD_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP,
                                    result->conflict.active_build_fingerprint, true) ||
        !runtime_wire_string_encode(out + RENDEZVOUS_RESPONSE_REQUESTED_VERSION_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,
                                    result->conflict.requested_version, true) ||
        !runtime_wire_string_encode(out + RENDEZVOUS_RESPONSE_REQUESTED_BUILD_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP,
                                    result->conflict.requested_build_fingerprint, true) ||
        !runtime_wire_string_encode(out + RENDEZVOUS_RESPONSE_MESSAGE_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_MESSAGE_CAP, result->message, true)) {
        return false;
    }
    return true;
}

static bool runtime_hello_response_decode(
    const uint8_t payload[CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE],
    cbm_daemon_runtime_connect_result_t *result) {
    if (!payload || !result) {
        return false;
    }
    memset(result, 0, sizeof(*result));
    result->status = (cbm_daemon_runtime_connect_status_t)runtime_get_u32(
        payload + RENDEZVOUS_RESPONSE_CONNECT_STATUS_OFFSET);
    result->hello_status = (cbm_daemon_hello_status_t)runtime_get_u32(
        payload + RENDEZVOUS_RESPONSE_HELLO_STATUS_OFFSET);
    result->client_id = runtime_get_u64(payload + RENDEZVOUS_RESPONSE_CLIENT_ID_OFFSET);
    result->authenticated_process_id =
        runtime_get_u64(payload + RENDEZVOUS_RESPONSE_PROCESS_ID_OFFSET);
    result->conflict.status = (cbm_daemon_hello_status_t)runtime_get_u32(
        payload + RENDEZVOUS_RESPONSE_CONFLICT_STATUS_OFFSET);
    if (!runtime_wire_string_decode(payload + RENDEZVOUS_RESPONSE_ACTIVE_VERSION_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,
                                    result->conflict.active_version, true) ||
        !runtime_wire_string_decode(payload + RENDEZVOUS_RESPONSE_ACTIVE_BUILD_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP,
                                    result->conflict.active_build_fingerprint, true) ||
        !runtime_wire_string_decode(payload + RENDEZVOUS_RESPONSE_REQUESTED_VERSION_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_VERSION_TEXT_CAP,
                                    result->conflict.requested_version, true) ||
        !runtime_wire_string_decode(payload + RENDEZVOUS_RESPONSE_REQUESTED_BUILD_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_BUILD_FINGERPRINT_CAP,
                                    result->conflict.requested_build_fingerprint, true) ||
        !runtime_wire_string_decode(payload + RENDEZVOUS_RESPONSE_MESSAGE_OFFSET,
                                    CBM_DAEMON_RENDEZVOUS_MESSAGE_CAP, result->message, true)) {
        return false;
    }
    if (result->status == CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED) {
        return result->hello_status == CBM_DAEMON_HELLO_COMPATIBLE &&
               result->conflict.status == CBM_DAEMON_HELLO_COMPATIBLE &&
               result->client_id != CBM_DAEMON_CLIENT_ID_INVALID &&
               result->authenticated_process_id != 0;
    }
    if (result->status == CBM_DAEMON_RUNTIME_CONNECT_CONFLICT) {
        char expected[CBM_DAEMON_RENDEZVOUS_MESSAGE_CAP];
        return result->hello_status >= CBM_DAEMON_HELLO_VERSION_CONFLICT &&
               result->hello_status <= CBM_DAEMON_HELLO_FEATURE_ABI_CONFLICT &&
               result->conflict.status == result->hello_status &&
               result->client_id == CBM_DAEMON_CLIENT_ID_INVALID &&
               result->authenticated_process_id == 0 &&
               cbm_daemon_conflict_format(&result->conflict, expected, sizeof(expected)) &&
               strcmp(result->message, expected) == 0;
    }
    return result->status == CBM_DAEMON_RUNTIME_CONNECT_REJECTED &&
           result->hello_status == CBM_DAEMON_HELLO_INVALID &&
           result->conflict.status == CBM_DAEMON_HELLO_INVALID &&
           result->client_id == CBM_DAEMON_CLIENT_ID_INVALID &&
           result->authenticated_process_id == 0;
}

static bool runtime_send_hello_response(cbm_daemon_ipc_connection_t *connection,
                                        const cbm_daemon_runtime_connect_result_t *result) {
    uint8_t payload[CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE];
    return runtime_hello_response_encode(payload, result) &&
           cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_RESPONSE,
                                     CBM_DAEMON_RUNTIME_OP_HELLO, payload,
                                     (uint32_t)sizeof(payload));
}

static bool runtime_worker_send_frame(cbm_daemon_runtime_worker_t *worker,
                                      cbm_daemon_frame_type_t type,
                                      cbm_daemon_runtime_operation_t operation, const void *payload,
                                      uint32_t length) {
    if (!worker) {
        return false;
    }
    cbm_mutex_lock(&worker->send_mutex);
    bool sent =
        worker->connection &&
        cbm_daemon_ipc_send_frame(worker->connection, type, (uint16_t)operation, payload, length);
    cbm_mutex_unlock(&worker->send_mutex);
    return sent;
}

static bool runtime_worker_send_status(cbm_daemon_runtime_worker_t *worker,
                                       cbm_daemon_runtime_operation_t operation, bool success) {
    uint8_t payload[STATUS_RESPONSE_SIZE];
    runtime_put_u32(payload, success ? 1U : 0U);
    return runtime_worker_send_frame(worker, CBM_DAEMON_FRAME_RESPONSE, operation, payload,
                                     (uint32_t)sizeof(payload));
}

static bool runtime_application_status_is_callback_result(
    cbm_daemon_runtime_application_status_t status) {
    return status == CBM_DAEMON_RUNTIME_APPLICATION_OK ||
           status == CBM_DAEMON_RUNTIME_APPLICATION_REJECTED ||
           status == CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR ||
           status == CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED;
}

static bool runtime_worker_send_application_response(
    cbm_daemon_runtime_worker_t *worker, cbm_daemon_runtime_application_token_t request_token,
    cbm_daemon_runtime_application_status_t status, const uint8_t *response,
    uint32_t response_length, bool suppress_when_disconnecting) {
    if (!worker || request_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID ||
        status <= CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR ||
        status > CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED ||
        response_length > CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX ||
        (response_length > 0 && !response)) {
        return false;
    }
    uint64_t wire_length = APPLICATION_RESPONSE_PREFIX_SIZE + (uint64_t)response_length;
    if (wire_length > CBM_DAEMON_MAX_FRAME_SIZE || wire_length > UINT32_MAX) {
        return false;
    }
    uint8_t *wire = malloc((size_t)wire_length);
    if (!wire) {
        return false;
    }
    runtime_put_u64(wire, request_token);
    runtime_put_u32(wire + 8, (uint32_t)status);
    runtime_put_u32(wire + 12, response_length);
    if (response_length > 0) {
        memcpy(wire + APPLICATION_RESPONSE_PREFIX_SIZE, response, response_length);
    }

    cbm_mutex_lock(&worker->send_mutex);
    bool disconnected = suppress_when_disconnecting &&
                        atomic_load_explicit(&worker->disconnecting, memory_order_acquire);
    bool sent = !disconnected && worker->connection &&
                cbm_daemon_ipc_send_frame(worker->connection, CBM_DAEMON_FRAME_RESPONSE,
                                          CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST, wire,
                                          (uint32_t)wire_length);
    cbm_mutex_unlock(&worker->send_mutex);
    free(wire);
    return sent;
}

static void runtime_result_rejected(cbm_daemon_runtime_connect_result_t *result,
                                    const char *message) {
    memset(result, 0, sizeof(*result));
    result->status = CBM_DAEMON_RUNTIME_CONNECT_REJECTED;
    result->hello_status = CBM_DAEMON_HELLO_INVALID;
    result->conflict.status = CBM_DAEMON_HELLO_INVALID;
    (void)snprintf(result->message, sizeof(result->message), "%s", message);
}

static void runtime_service_begin_stopping_locked(cbm_daemon_runtime_service_t *service,
                                                  uint64_t deadline, bool emergency,
                                                  const char *reason) {
    if (service->state == CBM_DAEMON_RUNTIME_SERVICE_RUNNING) {
        service->state = CBM_DAEMON_RUNTIME_SERVICE_STOPPING;
        service->stop_deadline_ms = deadline;
        /* The generation's fate is decided here by one of several owners
         * (last-client exit, coordinator stop, activation drain, external
         * stop). Sessions dropped by the losing side of a race are
         * undiagnosable without naming which owner pulled the trigger. */
        cbm_log_info("daemon.runtime_stopping", "reason", reason ? reason : "unspecified");
    } else if (service->state == CBM_DAEMON_RUNTIME_SERVICE_STOPPING &&
               (service->stop_deadline_ms == 0 || deadline < service->stop_deadline_ms)) {
        service->stop_deadline_ms = deadline;
    }
    if (emergency) {
        service->emergency_stop = true;
    }
}

static void runtime_service_begin_stopping(cbm_daemon_runtime_service_t *service,
                                           uint32_t timeout_ms, bool emergency,
                                           const char *reason) {
    uint64_t deadline = runtime_deadline_after(timeout_ms);
    cbm_mutex_lock(&service->mutex);
    runtime_service_begin_stopping_locked(service, deadline, emergency, reason);
    cbm_mutex_unlock(&service->mutex);
}

static void runtime_service_interrupt_connections_except(cbm_daemon_runtime_service_t *service,
                                                         cbm_daemon_runtime_worker_t *except_worker,
                                                         bool activation_owner) {
    uint64_t now_ms = cbm_now_ms();
    cbm_mutex_lock(&service->mutex);
    if (service->activation_response_inflight && !activation_owner) {
        cbm_mutex_unlock(&service->mutex);
        return;
    }
    bool force = service->emergency_stop ||
                 (service->stop_deadline_ms != 0 && now_ms >= service->stop_deadline_ms);
    for (size_t i = 0; i < service->worker_capacity; i++) {
        cbm_daemon_runtime_worker_t *worker = &service->workers[i];
        if (worker != except_worker && worker->in_use && worker->connection &&
            (activation_owner || force || !worker->final_response_inflight)) {
            cbm_daemon_ipc_connection_interrupt(worker->connection);
        }
    }
    cbm_mutex_unlock(&service->mutex);
}

static void runtime_service_interrupt_connections(cbm_daemon_runtime_service_t *service) {
    runtime_service_interrupt_connections_except(service, NULL, false);
}

static void runtime_worker_disconnect(cbm_daemon_runtime_worker_t *worker) {
    cbm_daemon_runtime_service_t *service = worker->service;
    cbm_daemon_client_id_t client_id = CBM_DAEMON_CLIENT_ID_INVALID;
    uint64_t shutdown_deadline = runtime_deadline_after(service->shutdown_timeout_ms);
    atomic_store_explicit(&worker->disconnecting, true, memory_order_release);
    cbm_mutex_lock(&service->mutex);
    if (worker->admitted) {
        client_id = worker->client_id;
        worker->admitted = false;
    }
    if (worker->admission_committed) {
        worker->admission_committed = false;
        if (service->committed_clients > 0) {
            service->committed_clients--;
        }
        if (service->committed_clients == 0 && !service->permanent) {
            /* A HELLO whose application session is still opening is only a
             * provisional coordinator client. It cannot keep the generation
             * alive after the final fully committed frontend disconnects.
             * A permanent generation (`daemon start`) deliberately survives
             * this: only the stop/drain ops or a process kill end it. */
            runtime_service_begin_stopping_locked(service, shutdown_deadline, false,
                                                  "last_committed_client_disconnected");
        }
    }
    cbm_mutex_unlock(&service->mutex);
    if (client_id == CBM_DAEMON_CLIENT_ID_INVALID) {
        return;
    }
    (void)cbm_daemon_client_disconnected(service->coordinator, client_id, cbm_now_ms());
    if (worker->application_session_opened && !worker->application_cancelled) {
        worker->application_cancelled = true;
        service->application.session_cancel(service->application.context,
                                            worker->application_session);
    }
    if (cbm_daemon_coordinator_state(service->coordinator) == CBM_DAEMON_COORDINATOR_STOPPING) {
        runtime_service_begin_stopping(service, service->shutdown_timeout_ms, false,
                                       "coordinator_stopping");
    }
}

static bool runtime_worker_service_running(cbm_daemon_runtime_worker_t *worker) {
    cbm_daemon_runtime_service_t *service = worker->service;
    cbm_mutex_lock(&service->mutex);
    bool running = service->state == CBM_DAEMON_RUNTIME_SERVICE_RUNNING;
    cbm_mutex_unlock(&service->mutex);
    return running;
}

static bool runtime_worker_admit(cbm_daemon_runtime_worker_t *worker,
                                 cbm_daemon_client_id_t *client_id_out) {
    cbm_daemon_runtime_service_t *service = worker->service;
    cbm_daemon_client_id_t client_id = CBM_DAEMON_CLIENT_ID_INVALID;
    cbm_mutex_lock(&service->mutex);
    if (service->state == CBM_DAEMON_RUNTIME_SERVICE_RUNNING) {
        client_id = cbm_daemon_client_connected(service->coordinator, cbm_now_ms());
        if (client_id != CBM_DAEMON_CLIENT_ID_INVALID) {
            worker->client_id = client_id;
            worker->admitted = true;
            worker->admission_committed = false;
            service->admitted_total++;
        }
    }
    cbm_mutex_unlock(&service->mutex);
    *client_id_out = client_id;
    return client_id != CBM_DAEMON_CLIENT_ID_INVALID;
}

static bool runtime_worker_commit_admission(cbm_daemon_runtime_worker_t *worker) {
    cbm_daemon_runtime_service_t *service = worker->service;
    cbm_mutex_lock(&service->mutex);
    bool committed = service->state == CBM_DAEMON_RUNTIME_SERVICE_RUNNING && worker->admitted &&
                     !worker->admission_committed;
    if (committed) {
        worker->admission_committed = true;
        service->committed_clients++;
    }
    cbm_mutex_unlock(&service->mutex);
    return committed;
}

static bool runtime_worker_handle_subscribe(cbm_daemon_runtime_worker_t *worker,
                                            const uint8_t *payload, uint32_t length) {
    if (!runtime_worker_service_running(worker) || !payload ||
        length < SUBSCRIBE_REQUEST_PREFIX_SIZE) {
        return false;
    }
    uint32_t key_length = runtime_get_u32(payload);
    if (key_length == 0 || key_length > CBM_DAEMON_RUNTIME_PROJECT_KEY_MAX ||
        length != SUBSCRIBE_REQUEST_PREFIX_SIZE + key_length) {
        return false;
    }
    char project_key[CBM_DAEMON_RUNTIME_PROJECT_KEY_MAX + 1];
    memcpy(project_key, payload + SUBSCRIBE_REQUEST_PREFIX_SIZE, key_length);
    project_key[key_length] = '\0';
    if (memchr(project_key, '\0', key_length) != NULL) {
        return false;
    }
    cbm_daemon_subscription_id_t subscription_id = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    cbm_daemon_subscription_result_t result = cbm_daemon_job_subscribe(
        worker->service->coordinator, worker->client_id, project_key, &subscription_id);
    uint8_t response[SUBSCRIBE_RESPONSE_SIZE];
    runtime_put_u32(response, (uint32_t)result);
    runtime_put_u64(response + 4, subscription_id);
    return runtime_worker_send_frame(worker, CBM_DAEMON_FRAME_RESPONSE,
                                     CBM_DAEMON_RUNTIME_OP_JOB_SUBSCRIBE, response,
                                     (uint32_t)sizeof(response));
}

static bool runtime_worker_handle_unsubscribe(cbm_daemon_runtime_worker_t *worker,
                                              const uint8_t *payload, uint32_t length) {
    if (!runtime_worker_service_running(worker) || !payload || length != UNSUBSCRIBE_REQUEST_SIZE) {
        return false;
    }
    cbm_daemon_subscription_id_t subscription_id = runtime_get_u64(payload);
    bool removed = cbm_daemon_job_unsubscribe(worker->service->coordinator, worker->client_id,
                                              subscription_id);
    return runtime_worker_send_status(worker, CBM_DAEMON_RUNTIME_OP_JOB_UNSUBSCRIBE, removed);
}

static void *runtime_application_worker(void *opaque) {
    cbm_daemon_runtime_worker_t *worker = opaque;
    cbm_daemon_runtime_service_t *service = worker->service;
    uint8_t *response = NULL;
    uint32_t response_length = 0;
    cbm_daemon_runtime_application_status_t status = service->application.request(
        service->application.context, worker->application_session,
        worker->application_request_token, worker->application_request,
        worker->application_request_length, &response, &response_length);

    bool valid_status = runtime_application_status_is_callback_result(status);
    bool valid_response =
        response_length <= CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX &&
        (response_length == 0 || response != NULL) &&
        (status == CBM_DAEMON_RUNTIME_APPLICATION_OK || (response == NULL && response_length == 0));
    if (!valid_status || !valid_response) {
        free(response);
        response = NULL;
        response_length = 0;
        status = CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR;
    }

    /* Completion must be one atomic transition from the client's point of
     * view: publish the done flag BEFORE the response bytes leave, so by the
     * time any client can react to the response, admission already observes
     * this slot as reusable. With the store after the send, a client that
     * pipelines its next request the moment it sees a response raced this
     * thread's final instructions and was rejected BUSY — on loaded Windows
     * hosts roughly half of all back-to-back requests on one connection,
     * surfacing as the wandering Phase 5 session drops. The admission side
     * joins this thread after observing the flag, which safely absorbs the
     * tail of the send. */
    atomic_store_explicit(&worker->application_thread_done, true, memory_order_release);
    bool sent = runtime_worker_send_application_response(worker, worker->application_request_token,
                                                         status, response, response_length, true);
    free(response);
    if (!sent && !atomic_load_explicit(&worker->disconnecting, memory_order_acquire)) {
        cbm_daemon_ipc_connection_interrupt(worker->connection);
    }
    return NULL;
}

static bool runtime_worker_reap_application(cbm_daemon_runtime_worker_t *worker, bool wait) {
    if (!worker->application_thread_started) {
        return true;
    }
    if (!wait && !atomic_load_explicit(&worker->application_thread_done, memory_order_acquire)) {
        return false;
    }
    if (cbm_thread_join(&worker->application_thread) != 0) {
        return false;
    }
    worker->application_thread_started = false;
    free(worker->application_request);
    worker->application_request = NULL;
    worker->application_request_length = 0;
    worker->application_request_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    atomic_store_explicit(&worker->application_thread_done, false, memory_order_release);
    return true;
}

static bool runtime_worker_handle_application(cbm_daemon_runtime_worker_t *worker,
                                              const uint8_t *payload, uint32_t length) {
    if (!payload || length < APPLICATION_REQUEST_PREFIX_SIZE) {
        return false;
    }
    cbm_daemon_runtime_application_token_t request_token = runtime_get_u64(payload);
    uint32_t request_length = runtime_get_u32(payload + 8);
    if (request_length > CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX ||
        length != APPLICATION_REQUEST_PREFIX_SIZE + request_length ||
        request_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID ||
        request_token <= worker->last_application_request_token) {
        return false;
    }
    /* A structurally valid token is consumed at admission, including BUSY,
     * UNAVAILABLE, and local allocation/thread failures. Reusing a token after
     * any terminal response would make late cancellation controls ambiguous. */
    worker->last_application_request_token = request_token;
    cbm_daemon_runtime_service_t *service = worker->service;
    if (!service->application.request) {
        return runtime_worker_send_application_response(
            worker, request_token, CBM_DAEMON_RUNTIME_APPLICATION_UNAVAILABLE, NULL, 0, false);
    }
    (void)runtime_worker_reap_application(worker, false);
    if (worker->application_thread_started) {
        return runtime_worker_send_application_response(
            worker, request_token, CBM_DAEMON_RUNTIME_APPLICATION_BUSY, NULL, 0, false);
    }

    uint8_t *request_copy = NULL;
    if (request_length > 0) {
        request_copy = malloc(request_length);
        if (!request_copy) {
            return runtime_worker_send_application_response(
                worker, request_token, CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR, NULL, 0,
                false);
        }
        memcpy(request_copy, payload + APPLICATION_REQUEST_PREFIX_SIZE, request_length);
    }
    worker->application_request_token = request_token;
    worker->application_request = request_copy;
    worker->application_request_length = request_length;
    atomic_store_explicit(&worker->application_thread_done, false, memory_order_release);
    if (cbm_thread_create(&worker->application_thread, RUNTIME_WORKER_STACK_SIZE,
                          runtime_application_worker, worker) != 0) {
        free(worker->application_request);
        worker->application_request = NULL;
        worker->application_request_length = 0;
        return runtime_worker_send_application_response(
            worker, request_token, CBM_DAEMON_RUNTIME_APPLICATION_HANDLER_ERROR, NULL, 0, false);
    }
    worker->application_thread_started = true;
    return true;
}

static bool runtime_worker_handle_application_cancel(cbm_daemon_runtime_worker_t *worker,
                                                     const uint8_t *payload, uint32_t length) {
    if (!worker || !payload || length != APPLICATION_CANCEL_REQUEST_SIZE) {
        return false;
    }
    cbm_daemon_runtime_application_token_t request_token = runtime_get_u64(payload);
    if (request_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID) {
        return false;
    }
    if (worker->application_thread_started && worker->application_request_token == request_token &&
        !atomic_load_explicit(&worker->application_thread_done, memory_order_acquire)) {
        worker->service->application.request_cancel(worker->service->application.context,
                                                    worker->application_session, request_token);
    }
    /* Cancellation is deliberately one-way. A stale exact-sized token is a
     * harmless no-op and never changes stream response ordering. */
    return true;
}

static bool runtime_worker_handle_close_intent(cbm_daemon_runtime_worker_t *worker,
                                               uint32_t length) {
    if (length != 0) {
        return false;
    }
    /* Terminal for the session (admission, coordinator registration, session
     * cancel), but not for the connection: the worker keeps reading so the
     * in-flight response tail and the eventual EOF unwind normally. */
    runtime_worker_disconnect(worker);
    return true;
}

static bool runtime_worker_handle_disconnect(cbm_daemon_runtime_worker_t *worker, uint32_t length) {
    if (length != 0) {
        return false;
    }
    cbm_daemon_runtime_service_t *service = worker->service;
    cbm_mutex_lock(&service->mutex);
    worker->final_response_inflight = true;
    cbm_mutex_unlock(&service->mutex);
    runtime_worker_disconnect(worker);
    bool sent = runtime_worker_send_status(worker, CBM_DAEMON_RUNTIME_OP_DISCONNECT, true);
    cbm_mutex_lock(&service->mutex);
    worker->final_response_inflight = false;
    cbm_mutex_unlock(&service->mutex);
    return sent;
}

static void runtime_worker_finish(cbm_daemon_runtime_worker_t *worker) {
    cbm_daemon_runtime_service_t *service = worker->service;
    runtime_worker_disconnect(worker);
    if (!runtime_worker_reap_application(worker, true)) {
        /* Fail closed on an impossible/invalid join rather than closing the
         * session or freeing storage a callback could still reference. The
         * service intentionally remains non-terminal for diagnosis. */
        return;
    }
    if (worker->application_session_opened) {
        service->application.session_close(service->application.context,
                                           worker->application_session);
        worker->application_session = NULL;
        worker->application_session_opened = false;
    }
    if (worker->connection) {
        /* Off the service mutex: a peer that just received its final response
         * (hello rejection, disconnect acknowledgement) must be able to read
         * it before the handle closes; see cbm_daemon_ipc_connection_drain. */
        cbm_daemon_ipc_connection_drain(worker->connection, RUNTIME_WORKER_DRAIN_TIMEOUT_MS);
    }
    cbm_mutex_lock(&service->mutex);
    if (worker->connection) {
        cbm_daemon_ipc_connection_close(worker->connection);
        worker->connection = NULL;
    }
    if (service->active_connections > 0) {
        service->active_connections--;
    }
    atomic_store_explicit(&worker->done, true, memory_order_release);
    cbm_mutex_unlock(&service->mutex);
}

static bool runtime_activation_peer_matches_claim(cbm_daemon_runtime_service_t *service,
                                                  uint64_t process_id, const char *claimed_build) {
    if (!service || process_id == 0 || !claimed_build) {
        return false;
    }
    bool active_image =
        runtime_process_image_reference_matches_process(&service->active_image, process_id);
    if (active_image) {
        return strcmp(claimed_build, service->identity.build_fingerprint) == 0;
    }
    char peer_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    return cbm_daemon_runtime_process_build_fingerprint(process_id, peer_fingerprint) &&
           strcmp(peer_fingerprint, claimed_build) == 0;
}

static bool runtime_control_request_decode(const uint8_t *payload, uint32_t length,
                                           char build_out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!payload || length != CBM_DAEMON_CONTROL_REQUEST_SIZE ||
        payload[CBM_DAEMON_CONTROL_REQUEST_SIZE - 1U] != 0U) {
        return false;
    }
    memcpy(build_out, payload, CBM_DAEMON_BUILD_FINGERPRINT_SIZE);
    return strlen(build_out) == CBM_DAEMON_BUILD_FINGERPRINT_SIZE - 1U;
}

static void runtime_control_collect_clients_locked(cbm_daemon_runtime_service_t *service,
                                                   cbm_daemon_runtime_worker_t *except,
                                                   uint8_t *count_out,
                                                   uint32_t pids[CBM_DAEMON_CONTROL_CLIENT_CAP]) {
    uint8_t count = 0U;
    for (size_t index = 0; index < service->worker_capacity; index++) {
        cbm_daemon_runtime_worker_t *candidate = &service->workers[index];
        if (!candidate->in_use || !candidate->admission_committed || candidate == except) {
            continue;
        }
        if (count < CBM_DAEMON_CONTROL_CLIENT_CAP) {
            pids[count] = (uint32_t)candidate->peer_process_id;
        }
        count++;
    }
    *count_out =
        count > CBM_DAEMON_CONTROL_CLIENT_CAP ? (uint8_t)CBM_DAEMON_CONTROL_CLIENT_CAP : count;
}

static void runtime_worker_handle_status(cbm_daemon_runtime_worker_t *worker,
                                         const uint8_t *payload, uint32_t length) {
    cbm_daemon_runtime_service_t *service = worker->service;
    char requested_build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    bool peer_verified =
        runtime_control_request_decode(payload, length, requested_build) &&
        runtime_activation_peer_matches_claim(service, worker->peer_process_id, requested_build);
    uint8_t response[CBM_DAEMON_STATUS_RESPONSE_SIZE];
    memset(response, 0, sizeof(response));
    if (peer_verified) {
        uint8_t count = 0U;
        uint32_t pids[CBM_DAEMON_CONTROL_CLIENT_CAP] = {0};
        cbm_mutex_lock(&service->mutex);
        response[0] = 1U;
        response[1] =
            (uint8_t)((service->permanent ? 0x01U : 0U) |
                      (service->state != CBM_DAEMON_RUNTIME_SERVICE_RUNNING ? 0x02U : 0U));
        uint16_t committed = service->committed_clients > UINT16_MAX
                                 ? UINT16_MAX
                                 : (uint16_t)service->committed_clients;
        response[2] = (uint8_t)(committed >> 8);
        response[3] = (uint8_t)(committed & 0xFFU);
        uint32_t daemon_pid = (uint32_t)runtime_current_process_id();
        response[4] = (uint8_t)(daemon_pid >> 24);
        response[5] = (uint8_t)(daemon_pid >> 16);
        response[6] = (uint8_t)(daemon_pid >> 8);
        response[7] = (uint8_t)(daemon_pid & 0xFFU);
        runtime_control_collect_clients_locked(service, worker, &count, pids);
        response[8] = count;
        for (size_t index = 0; index < CBM_DAEMON_CONTROL_CLIENT_CAP; index++) {
            size_t offset = 12U + index * 4U;
            response[offset] = (uint8_t)(pids[index] >> 24);
            response[offset + 1U] = (uint8_t)(pids[index] >> 16);
            response[offset + 2U] = (uint8_t)(pids[index] >> 8);
            response[offset + 3U] = (uint8_t)(pids[index] & 0xFFU);
        }
        (void)snprintf((char *)response + 44U, CBM_DAEMON_BUILD_FINGERPRINT_SIZE, "%s",
                       service->identity.build_fingerprint);
        (void)snprintf((char *)response + 109U, 12U, "%s",
                       service->identity.semantic_version ? service->identity.semantic_version
                                                          : "");
        cbm_mutex_unlock(&service->mutex);
    }
    (void)runtime_worker_send_frame(worker, CBM_DAEMON_FRAME_RESPONSE, CBM_DAEMON_RUNTIME_OP_STATUS,
                                    response, (uint32_t)sizeof(response));
    runtime_worker_finish(worker);
}

static void runtime_worker_handle_stop(cbm_daemon_runtime_worker_t *worker, const uint8_t *payload,
                                       uint32_t length) {
    cbm_daemon_runtime_service_t *service = worker->service;
    char requested_build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    bool peer_verified =
        runtime_control_request_decode(payload, length, requested_build) &&
        runtime_activation_peer_matches_claim(service, worker->peer_process_id, requested_build);
    uint8_t response[CBM_DAEMON_STOP_RESPONSE_SIZE];
    memset(response, 0, sizeof(response));
    bool accepted = false;
    if (peer_verified) {
        uint8_t count = 0U;
        uint32_t pids[CBM_DAEMON_CONTROL_CLIENT_CAP] = {0};
        uint64_t deadline = runtime_deadline_after(service->shutdown_timeout_ms);
        cbm_mutex_lock(&service->mutex);
        response[0] = 1U;
        uint16_t committed = service->committed_clients > UINT16_MAX
                                 ? UINT16_MAX
                                 : (uint16_t)service->committed_clients;
        runtime_control_collect_clients_locked(service, worker, &count, pids);
        if (service->committed_clients > 0) {
            /* Refuse-if-busy: the caller reports these pids to the user as
             * the processes that must exit before a graceful stop. */
            response[1] = 0x02U;
        } else if (service->state == CBM_DAEMON_RUNTIME_SERVICE_RUNNING) {
            /* Explicit stop is the sanctioned exit for PERMANENT generations
             * as well — begin_stopping here overrides the permanence gate.
             * Mirror the activation-shutdown ACK protocol: the stopping
             * teardown must not consume this requester's response slot before
             * the acceptance byte leaves. */
            runtime_service_begin_stopping_locked(service, deadline, false,
                                                  "daemon_stop_requested");
            /* Same exit criterion as an activation drain: a permanent
             * coordinator never reports client-terminal, so resource
             * emptiness must be allowed to complete this stop promptly. */
            service->activation_shutdown_requested = true;
            service->activation_response_inflight = true;
            worker->final_response_inflight = true;
            response[1] = 0x01U;
            accepted = true;
        }
        response[2] = (uint8_t)(committed >> 8);
        response[3] = (uint8_t)(committed & 0xFFU);
        response[4] = count;
        for (size_t index = 0; index < CBM_DAEMON_CONTROL_CLIENT_CAP; index++) {
            size_t offset = 8U + index * 4U;
            response[offset] = (uint8_t)(pids[index] >> 24);
            response[offset + 1U] = (uint8_t)(pids[index] >> 16);
            response[offset + 2U] = (uint8_t)(pids[index] >> 8);
            response[offset + 3U] = (uint8_t)(pids[index] & 0xFFU);
        }
        cbm_mutex_unlock(&service->mutex);
    }
    if (accepted) {
        char requester_pid[32];
        (void)snprintf(requester_pid, sizeof(requester_pid), "%llu",
                       (unsigned long long)worker->peer_process_id);
        cbm_log_info("daemon.stop_requested", "requester_pid", requester_pid, "requester_build",
                     requested_build);
        runtime_service_interrupt_connections_except(worker->service, worker, true);
    }
    (void)runtime_worker_send_frame(worker, CBM_DAEMON_FRAME_RESPONSE, CBM_DAEMON_RUNTIME_OP_STOP,
                                    response, (uint32_t)sizeof(response));
    if (accepted) {
        cbm_daemon_runtime_service_t *stop_service = worker->service;
        runtime_worker_finish(worker);
        cbm_mutex_lock(&stop_service->mutex);
        worker->final_response_inflight = false;
        stop_service->activation_response_inflight = false;
        cbm_mutex_unlock(&stop_service->mutex);
        runtime_service_interrupt_connections(stop_service);
        return;
    }
    runtime_worker_finish(worker);
}

static void runtime_worker_handle_activation_shutdown(cbm_daemon_runtime_worker_t *worker,
                                                      const uint8_t *payload, uint32_t length) {
    cbm_daemon_runtime_service_t *service = worker->service;
    cbm_daemon_runtime_activation_result_t result = {0};
    cbm_daemon_runtime_activation_action_t action = 0;
    char requested_version[CBM_DAEMON_VERSION_TEXT_SIZE] = {0};
    char requested_build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE] = {0};
    bool request_valid =
        length == CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE && payload &&
        runtime_activation_request_decode(payload, &action, requested_version, requested_build);
    bool peer_verified = request_valid && runtime_activation_peer_matches_claim(
                                              service, worker->peer_process_id, requested_build);
    if (peer_verified) {
        uint64_t deadline = runtime_deadline_after(service->shutdown_timeout_ms);
        cbm_mutex_lock(&service->mutex);
        result.active_clients = (uint64_t)service->committed_clients;
        /* Report only connections this request will drain. The authenticated
         * one-shot requester is never a session and excludes itself. */
        result.active_connections =
            service->active_connections > 0 ? (uint64_t)(service->active_connections - 1U) : 0;
        if (service->state == CBM_DAEMON_RUNTIME_SERVICE_RUNNING) {
            runtime_service_begin_stopping_locked(service, deadline, false, "activation_shutdown");
            service->activation_shutdown_requested = true;
            service->activation_response_inflight = true;
            worker->final_response_inflight = true;
            result.accepted = true;
        }
        cbm_mutex_unlock(&service->mutex);
    }

    uint8_t response[CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE];
    bool encoded = runtime_activation_response_encode(response, &result);
    if (result.accepted) {
        char requester_pid[32];
        char active_clients[32];
        char active_connections[32];
        (void)snprintf(requester_pid, sizeof(requester_pid), "%llu",
                       (unsigned long long)worker->peer_process_id);
        (void)snprintf(active_clients, sizeof(active_clients), "%llu",
                       (unsigned long long)result.active_clients);
        (void)snprintf(active_connections, sizeof(active_connections), "%llu",
                       (unsigned long long)result.active_connections);
        cbm_log_info("daemon.activation_shutdown", "requester_pid", requester_pid,
                     "requester_build", requested_build, "action",
                     runtime_activation_action_text(action), "active_clients", active_clients,
                     "active_connections", active_connections);
        /* Interrupt every other connection BEFORE the ACK leaves: the ACK is
         * the requester's license to act on "snapshotted and draining", so
         * every drain must already be initiated when it arrives. Acking first
         * left a window where a session could still get one request serviced
         * after the requester observed the ACK. The requester itself is
         * excepted and its response path stays protected. */
        runtime_service_interrupt_connections_except(service, worker, true);
    }
    bool response_sent =
        encoded && runtime_worker_send_frame(worker, CBM_DAEMON_FRAME_RESPONSE,
                                             CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN, response,
                                             (uint32_t)sizeof(response));
    if (result.accepted && !response_sent) {
        cbm_log_error("daemon.activation_shutdown_ack_failed", "shutdown", "accepted",
                      "requester_build", requested_build, "action",
                      runtime_activation_action_text(action));
    }

    if (result.accepted) {
        /* Close this requester normally, then release the global ACK gate so
         * the accept loop can finish any stragglers. */
        runtime_worker_finish(worker);
        cbm_mutex_lock(&service->mutex);
        worker->final_response_inflight = false;
        service->activation_response_inflight = false;
        cbm_mutex_unlock(&service->mutex);
        runtime_service_interrupt_connections(service);
        return;
    }
    runtime_worker_finish(worker);
}

static void *runtime_connection_worker(void *opaque) {
    cbm_daemon_runtime_worker_t *worker = opaque;
    cbm_daemon_runtime_service_t *service = worker->service;
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    char requested_version[CBM_DAEMON_VERSION_TEXT_SIZE];
    char requested_build[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];

    int received = cbm_daemon_ipc_receive_frame_bounded(
        worker->connection, service->request_timeout_ms,
        CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE, &frame, &payload);
    if (received != 1 || frame.type != CBM_DAEMON_FRAME_REQUEST) {
        free(payload);
        runtime_worker_finish(worker);
        return NULL;
    }
    if (frame.flags == CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN) {
        runtime_worker_handle_activation_shutdown(worker, payload, frame.length);
        free(payload);
        return NULL;
    }
    if (frame.flags == CBM_DAEMON_RUNTIME_OP_STATUS) {
        runtime_worker_handle_status(worker, payload, frame.length);
        free(payload);
        return NULL;
    }
    if (frame.flags == CBM_DAEMON_RUNTIME_OP_STOP) {
        runtime_worker_handle_stop(worker, payload, frame.length);
        free(payload);
        return NULL;
    }
    if (frame.flags != CBM_DAEMON_RUNTIME_OP_HELLO ||
        frame.length != CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE ||
        !runtime_rendezvous_request_decode(payload, requested_version, requested_build)) {
        free(payload);
        runtime_worker_finish(worker);
        return NULL;
    }
    free(payload);
    payload = NULL;

    cbm_daemon_runtime_connect_result_t hello_result;
    memset(&hello_result, 0, sizeof(hello_result));
    cbm_daemon_hello_status_t hello_status = runtime_rendezvous_compare(
        service->identity.semantic_version, service->identity.build_fingerprint, requested_version,
        requested_build, &hello_result.conflict);
    if (hello_status != CBM_DAEMON_HELLO_COMPATIBLE) {
        if (hello_status == CBM_DAEMON_HELLO_INVALID) {
            runtime_worker_finish(worker);
            return NULL;
        }
        hello_result.status = CBM_DAEMON_RUNTIME_CONNECT_CONFLICT;
        hello_result.hello_status = hello_status;
        (void)cbm_daemon_conflict_format(&hello_result.conflict, hello_result.message,
                                         sizeof(hello_result.message));
        if (!cbm_daemon_conflict_log_append(service->conflict_log_path, &hello_result.conflict,
                                            service->conflict_log_cap_bytes)) {
            /* This goes through the daemon operation-log sink, never back
             * through the failed conflict-log path. Do not silently lose the
             * durable startup-conflict diagnostic. */
            cbm_log_error("daemon.conflict_log_append_failed", "path", service->conflict_log_path);
        }
        (void)runtime_send_hello_response(worker->connection, &hello_result);
        runtime_worker_finish(worker);
        return NULL;
    }

    bool peer_image_verified = runtime_process_image_reference_matches_process(
        &service->active_image, worker->peer_process_id);
    bool peer_image_fingerprinted = false;
    if (!peer_image_verified) {
        char peer_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
        peer_image_fingerprinted =
            cbm_daemon_runtime_process_build_fingerprint(worker->peer_process_id, peer_fingerprint);
        peer_image_verified = peer_image_fingerprinted &&
                              strcmp(peer_fingerprint, requested_build) == 0 &&
                              strcmp(peer_fingerprint, service->identity.build_fingerprint) == 0;
    }
    if (!peer_image_verified) {
        cbm_log_error("daemon.client_image_rejected", "reason",
                      peer_image_fingerprinted ? "fingerprint_mismatch" : "image_unverifiable");
        runtime_worker_finish(worker);
        return NULL;
    }

    cbm_daemon_client_id_t client_id = CBM_DAEMON_CLIENT_ID_INVALID;
    if (!runtime_worker_admit(worker, &client_id)) {
        runtime_result_rejected(&hello_result, "CBM daemon is stopping");
        (void)runtime_send_hello_response(worker->connection, &hello_result);
        runtime_worker_finish(worker);
        return NULL;
    }
    if (service->application.request) {
        worker->application_session = service->application.session_open(
            service->application.context, client_id, worker->peer_process_id);
        if (!worker->application_session) {
            runtime_result_rejected(&hello_result, "CBM daemon session initialization failed");
            cbm_mutex_lock(&service->mutex);
            worker->final_response_inflight = true;
            cbm_mutex_unlock(&service->mutex);
            runtime_worker_disconnect(worker);
            (void)runtime_send_hello_response(worker->connection, &hello_result);
            cbm_mutex_lock(&service->mutex);
            worker->final_response_inflight = false;
            cbm_mutex_unlock(&service->mutex);
            runtime_worker_finish(worker);
            return NULL;
        }
        worker->application_session_opened = true;
    }
    if (!runtime_worker_commit_admission(worker)) {
        /* session_open() returns a provisional allocation. STOPPING and this
         * commit linearize on service->mutex; a losing provisional session is
         * cancelled and closed exactly like any other connection-owned state,
         * but is never exposed to the frontend as admitted. */
        runtime_result_rejected(&hello_result, "CBM daemon is stopping");
        cbm_mutex_lock(&service->mutex);
        worker->final_response_inflight = true;
        cbm_mutex_unlock(&service->mutex);
        runtime_worker_disconnect(worker);
        (void)runtime_send_hello_response(worker->connection, &hello_result);
        cbm_mutex_lock(&service->mutex);
        worker->final_response_inflight = false;
        cbm_mutex_unlock(&service->mutex);
        runtime_worker_finish(worker);
        return NULL;
    }
    hello_result.status = CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED;
    hello_result.hello_status = CBM_DAEMON_HELLO_COMPATIBLE;
    hello_result.conflict.status = CBM_DAEMON_HELLO_COMPATIBLE;
    hello_result.client_id = client_id;
    hello_result.authenticated_process_id = worker->peer_process_id;
    if (!runtime_send_hello_response(worker->connection, &hello_result)) {
        runtime_worker_finish(worker);
        return NULL;
    }

    bool keep_running = true;
    while (keep_running && runtime_worker_service_running(worker)) {
        memset(&frame, 0, sizeof(frame));
        payload = NULL;
        received = cbm_daemon_ipc_receive_frame(worker->connection, CBM_DAEMON_IPC_WAIT_FOREVER,
                                                &frame, &payload);
        if (received != 1 || frame.type != CBM_DAEMON_FRAME_REQUEST) {
            /* An admitted frontend disconnecting decides the daemon's fate
             * (last committed client ends the generation), so the transport
             * cause must be visible: 0 = orderly EOF, negative = transport
             * error. A silent break here previously made session-drop races
             * undiagnosable across three platforms. */
            char received_text[16];
            (void)snprintf(received_text, sizeof(received_text), "%d", received);
            cbm_log_info("daemon.connection_end", "received", received_text, "frame_type",
                         received == 1 ? "non_request" : "none");
            break;
        }
        switch (frame.flags) {
        case CBM_DAEMON_RUNTIME_OP_HEARTBEAT: {
            bool valid = frame.length == 0 &&
                         cbm_daemon_client_heartbeat(service->coordinator, client_id, cbm_now_ms());
            keep_running =
                valid && runtime_worker_send_status(worker, CBM_DAEMON_RUNTIME_OP_HEARTBEAT, true);
            break;
        }
        case CBM_DAEMON_RUNTIME_OP_JOB_SUBSCRIBE:
            keep_running = runtime_worker_handle_subscribe(worker, payload, frame.length);
            break;
        case CBM_DAEMON_RUNTIME_OP_JOB_UNSUBSCRIBE:
            keep_running = runtime_worker_handle_unsubscribe(worker, payload, frame.length);
            break;
        case CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST:
            keep_running = runtime_worker_handle_application(worker, payload, frame.length);
            break;
        case CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL:
            keep_running = runtime_worker_handle_application_cancel(worker, payload, frame.length);
            break;
        case CBM_DAEMON_RUNTIME_OP_CLOSE_INTENT:
            keep_running = runtime_worker_handle_close_intent(worker, frame.length);
            break;
        case CBM_DAEMON_RUNTIME_OP_DISCONNECT:
            keep_running = false;
            (void)runtime_worker_handle_disconnect(worker, frame.length);
            break;
        default:
            keep_running = false;
            break;
        }
        free(payload);
        payload = NULL;
    }
    free(payload);
    runtime_worker_finish(worker);
    return NULL;
}

static void runtime_worker_reset_after_join(cbm_daemon_runtime_worker_t *worker) {
    free(worker->application_request);
    worker->thread_started = false;
    worker->in_use = false;
    worker->joining = false;
    worker->connection = NULL;
    worker->client_id = CBM_DAEMON_CLIENT_ID_INVALID;
    worker->peer_process_id = 0;
    worker->admitted = false;
    worker->admission_committed = false;
    worker->final_response_inflight = false;
    worker->application_session = NULL;
    worker->application_session_opened = false;
    worker->application_cancelled = false;
    worker->application_thread_started = false;
    worker->application_request_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    worker->last_application_request_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    worker->application_request = NULL;
    worker->application_request_length = 0;
    atomic_store_explicit(&worker->done, false, memory_order_release);
    atomic_store_explicit(&worker->disconnecting, false, memory_order_release);
    atomic_store_explicit(&worker->application_thread_done, false, memory_order_release);
}

static void runtime_reap_completed_workers(cbm_daemon_runtime_service_t *service) {
    for (size_t i = 0; i < service->worker_capacity; i++) {
        cbm_daemon_runtime_worker_t *worker = &service->workers[i];
        cbm_mutex_lock(&service->mutex);
        bool reap = worker->in_use && worker->thread_started && !worker->joining &&
                    atomic_load_explicit(&worker->done, memory_order_acquire);
        if (reap) {
            worker->joining = true;
        }
        cbm_mutex_unlock(&service->mutex);
        if (!reap) {
            continue;
        }
        int joined = cbm_thread_join(&worker->thread);
        cbm_mutex_lock(&service->mutex);
        if (joined == 0) {
            runtime_worker_reset_after_join(worker);
        } else {
            worker->joining = false;
        }
        cbm_mutex_unlock(&service->mutex);
    }
}

static cbm_daemon_runtime_worker_t *runtime_find_free_worker_locked(
    cbm_daemon_runtime_service_t *service) {
    for (size_t i = 0; i < service->worker_capacity; i++) {
        if (!service->workers[i].in_use) {
            return &service->workers[i];
        }
    }
    return NULL;
}

static void runtime_reject_inline(cbm_daemon_ipc_connection_t *connection, const char *message) {
    cbm_daemon_runtime_connect_result_t result;
    runtime_result_rejected(&result, message);
    (void)runtime_send_hello_response(connection, &result);
    /* Same Windows named-pipe discard hazard as runtime_worker_finish: the
     * peer must get to read the rejection before the handle closes. */
    cbm_daemon_ipc_connection_drain(connection, RUNTIME_REJECT_DRAIN_TIMEOUT_MS);
    cbm_daemon_ipc_connection_close(connection);
}

static void runtime_accept_connection(cbm_daemon_runtime_service_t *service,
                                      cbm_daemon_ipc_connection_t *connection) {
    uint64_t peer_pid = cbm_daemon_ipc_connection_peer_pid(connection);
    if (peer_pid == 0) {
        cbm_daemon_ipc_connection_close(connection);
        return;
    }

    cbm_mutex_lock(&service->mutex);
    bool running = service->state == CBM_DAEMON_RUNTIME_SERVICE_RUNNING;
    bool capacity = service->active_connections >= service->worker_capacity;
    cbm_daemon_runtime_worker_t *worker =
        running && !capacity ? runtime_find_free_worker_locked(service) : NULL;
    if (worker) {
        worker->connection = connection;
        worker->peer_process_id = peer_pid;
        worker->client_id = CBM_DAEMON_CLIENT_ID_INVALID;
        worker->admitted = false;
        worker->admission_committed = false;
        worker->final_response_inflight = false;
        worker->application_session = NULL;
        worker->application_session_opened = false;
        worker->application_cancelled = false;
        worker->application_thread_started = false;
        free(worker->application_request);
        worker->application_request = NULL;
        worker->application_request_length = 0;
        worker->joining = false;
        worker->in_use = true;
        atomic_store_explicit(&worker->done, false, memory_order_release);
        atomic_store_explicit(&worker->disconnecting, false, memory_order_release);
        atomic_store_explicit(&worker->application_thread_done, false, memory_order_release);
        service->active_connections++;
        int created = cbm_thread_create(&worker->thread, RUNTIME_WORKER_STACK_SIZE,
                                        runtime_connection_worker, worker);
        if (created == 0) {
            worker->thread_started = true;
            cbm_mutex_unlock(&service->mutex);
            return;
        }
        service->active_connections--;
        runtime_worker_reset_after_join(worker);
    }
    cbm_mutex_unlock(&service->mutex);

    if (!running) {
        runtime_reject_inline(connection, "CBM daemon is stopping");
    } else {
        runtime_reject_inline(connection, "CBM daemon connection capacity reached");
    }
}

static bool runtime_service_can_exit(cbm_daemon_runtime_service_t *service, uint64_t now_ms) {
    bool can_exit = false;
    cbm_mutex_lock(&service->mutex);
    if (service->state == CBM_DAEMON_RUNTIME_SERVICE_STOPPING && service->active_connections == 0) {
        bool drained = cbm_daemon_should_exit(service->coordinator, now_ms);
        if (!drained && service->activation_shutdown_requested) {
            /* Activation can arrive while the daemon has no committed
             * coordinator client, so no final disconnect exists to flip the
             * coordinator's terminal bit. Resource emptiness is equivalent
             * once the runtime has atomically stopped all new admission. */
            drained = cbm_daemon_active_clients(service->coordinator) == 0 &&
                      cbm_daemon_active_jobs(service->coordinator) == 0 &&
                      cbm_daemon_active_watches(service->coordinator) == 0;
        }
        bool deadline_reached =
            service->stop_deadline_ms != 0 && now_ms >= service->stop_deadline_ms;
        can_exit = drained || deadline_reached || service->emergency_stop;
        if (can_exit) {
            service->state = CBM_DAEMON_RUNTIME_SERVICE_EXITED;
        }
    }
    cbm_mutex_unlock(&service->mutex);
    return can_exit;
}

static void *runtime_accept_loop(void *opaque) {
    cbm_daemon_runtime_service_t *service = opaque;
    for (;;) {
        runtime_reap_completed_workers(service);
        cbm_mutex_lock(&service->mutex);
        cbm_daemon_runtime_service_state_t state = service->state;
        cbm_mutex_unlock(&service->mutex);
        if (state == CBM_DAEMON_RUNTIME_SERVICE_EXITED) {
            break;
        }
        if (state == CBM_DAEMON_RUNTIME_SERVICE_STOPPING) {
            runtime_service_interrupt_connections(service);
            if (runtime_service_can_exit(service, cbm_now_ms())) {
                break;
            }
            uint64_t tick_deadline = runtime_deadline_after(1);
            runtime_wait_tick(tick_deadline);
            continue;
        }

        cbm_daemon_ipc_connection_t *connection = NULL;
        int accepted =
            cbm_daemon_ipc_accept(service->listener, RUNTIME_ACCEPT_POLL_MS, &connection);
        if (accepted == 1 && connection) {
            runtime_accept_connection(service, connection);
        } else if (accepted < 0) {
            /* IPC intentionally combines invalid-peer and listener errors.
             * Retry while RUNNING so one hostile peer cannot stop the daemon. */
            uint64_t tick_deadline = runtime_deadline_after(1);
            runtime_wait_tick(tick_deadline);
        }
    }
    runtime_reap_completed_workers(service);
    atomic_store_explicit(&service->accept_thread_done, true, memory_order_release);
    return NULL;
}

static char *runtime_string_copy_bounded(const char *value, size_t capacity) {
    size_t length = 0;
    if (!runtime_bounded_length(value, capacity, &length)) {
        return NULL;
    }
    char *copy = malloc(length + 1);
    if (copy) {
        memcpy(copy, value, length + 1);
    }
    return copy;
}

static bool runtime_application_callbacks_valid(
    const cbm_daemon_runtime_application_callbacks_t *application) {
    bool any = application->session_open || application->request || application->request_cancel ||
               application->session_cancel || application->session_close;
    bool all = application->session_open && application->request && application->request_cancel &&
               application->session_cancel && application->session_close;
    return !any || all;
}

static bool runtime_participant_guard_release_complete(
    cbm_daemon_ipc_participant_guard_t **guard_io, uint32_t timeout_ms) {
    uint64_t deadline = runtime_deadline_after(timeout_ms);
    while (guard_io && *guard_io) {
        (void)cbm_daemon_ipc_participant_guard_release(guard_io);
        if (!*guard_io) {
            return true;
        }
        if (cbm_now_ms() >= deadline) {
            return false;
        }
        cbm_usleep(1000);
    }
    return guard_io != NULL;
}

static _Noreturn void runtime_cleanup_fail_stop(const char *component) {
    /* Convenience start() has no outward cleanup handle on failure. Losing a
     * live participant guard would permit later code to misreport teardown,
     * so terminate the process and let the kernel release native ownership. */
    cbm_log_error("daemon.forced_shutdown", "component", component);
    (void)fflush(stdout);
    (void)fflush(stderr);
#ifdef _WIN32
    (void)TerminateProcess(GetCurrentProcess(), EXIT_FAILURE);
    abort();
#else
    _Exit(EXIT_FAILURE);
#endif
}

static void runtime_startup_lock_release_complete(cbm_daemon_ipc_startup_lock_t **lock_io,
                                                  uint32_t timeout_ms) {
    uint64_t deadline = runtime_deadline_after(timeout_ms);
    while (lock_io && *lock_io) {
        (void)cbm_daemon_ipc_startup_lock_release(lock_io);
        if (!*lock_io) {
            return;
        }
        if (cbm_now_ms() >= deadline) {
            runtime_cleanup_fail_stop("startup_lock_cleanup");
        }
        cbm_usleep(1000);
    }
}

static void runtime_service_destroy_unstarted(cbm_daemon_runtime_service_t *service) {
    if (!service) {
        return;
    }
    cbm_daemon_ipc_listener_close(service->listener);
    cbm_daemon_coordinator_free(service->coordinator);
    (void)runtime_process_image_reference_release(&service->active_image);
    free(service->conflict_log_path);
    for (size_t i = 0; i < service->worker_mutexes_initialized; i++) {
        cbm_mutex_destroy(&service->workers[i].send_mutex);
    }
    free(service->workers);
    cbm_mutex_destroy(&service->mutex);
    free(service);
}

cbm_daemon_runtime_service_t *cbm_daemon_runtime_service_start_reserved(
    const cbm_daemon_runtime_service_config_t *config,
    cbm_daemon_ipc_lifetime_reservation_t **reservation_io) {
    uint8_t validation[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE];
    char active_process_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    runtime_process_image_reference_t active_image;
    runtime_process_image_reference_init(&active_image);
    if (!reservation_io || !*reservation_io || !config || !config->endpoint ||
        !config->conflict_log_path || config->conflict_log_cap_bytes == 0 ||
        config->max_clients == 0 || config->max_clients > RUNTIME_MAX_CLIENTS_HARD ||
        config->lease_timeout_ms == 0 || config->request_timeout_ms == 0 ||
        config->request_timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER ||
        config->shutdown_timeout_ms == 0 ||
        config->shutdown_timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER ||
        !runtime_application_callbacks_valid(&config->application) ||
        !cbm_daemon_runtime_hello_request_encode(validation, &config->identity)) {
        cbm_log_error("daemon.runtime.start_failed", "stage", "config_validation");
        return NULL;
    }
    /* WHY: cppcheck evaluates the unsupported-platform fail-closed branch
     * because its invocation defines no host OS macro. Production compilers
     * select one of the Windows/macOS/Linux native-image implementations. */
    // cppcheck-suppress knownConditionTrueFalse
    if (!runtime_process_image_reference_acquire(runtime_current_process_id(), &active_image,
                                                 active_process_fingerprint) ||
        strcmp(active_process_fingerprint, config->identity.build_fingerprint) != 0) {
        cbm_log_error("daemon.runtime.start_failed", "stage", "active_image_identity");
        (void)runtime_process_image_reference_release(&active_image);
        return NULL;
    }

    cbm_daemon_runtime_service_t *service = calloc(1, sizeof(*service));
    if (!service) {
        cbm_log_error("daemon.runtime.start_failed", "stage", "service_allocation");
        (void)runtime_process_image_reference_release(&active_image);
        return NULL;
    }
    service->active_image = active_image;
    runtime_process_image_reference_init(&active_image);
    cbm_mutex_init(&service->mutex);
    atomic_init(&service->accept_thread_done, false);
    service->worker_capacity = config->max_clients;
    service->workers = calloc(service->worker_capacity, sizeof(*service->workers));
    service->coordinator = cbm_daemon_coordinator_new(config->lease_timeout_ms);
    if (service->coordinator && config->permanent) {
        cbm_daemon_coordinator_set_permanent(service->coordinator, true);
    }
    service->conflict_log_path =
        runtime_string_copy_bounded(config->conflict_log_path, RUNTIME_PATH_CAP);
    size_t version_length = 0;
    size_t build_length = 0;
    bool copied_identity =
        runtime_bounded_length(config->identity.semantic_version, sizeof(service->semantic_version),
                               &version_length) &&
        runtime_bounded_length(config->identity.build_fingerprint,
                               sizeof(service->build_fingerprint), &build_length);
    if (copied_identity) {
        memcpy(service->semantic_version, config->identity.semantic_version, version_length + 1);
        memcpy(service->build_fingerprint, config->identity.build_fingerprint, build_length + 1);
    }
    service->identity.semantic_version = service->semantic_version;
    service->identity.build_fingerprint = service->build_fingerprint;
    service->identity.protocol_abi = config->identity.protocol_abi;
    service->identity.store_abi = config->identity.store_abi;
    service->identity.feature_abi = config->identity.feature_abi;
    service->conflict_log_cap_bytes = config->conflict_log_cap_bytes;
    service->lease_timeout_ms = config->lease_timeout_ms;
    service->request_timeout_ms = config->request_timeout_ms;
    service->shutdown_timeout_ms = config->shutdown_timeout_ms;
    service->application = config->application;
    service->permanent = config->permanent;
    service->state = CBM_DAEMON_RUNTIME_SERVICE_STARTING;

    if (!service->workers || !service->coordinator || !service->conflict_log_path ||
        !copied_identity) {
        cbm_log_error("daemon.runtime.start_failed", "stage", "service_initialization");
        runtime_service_destroy_unstarted(service);
        return NULL;
    }
    for (size_t i = 0; i < service->worker_capacity; i++) {
        service->workers[i].service = service;
        cbm_mutex_init(&service->workers[i].send_mutex);
        service->worker_mutexes_initialized++;
        atomic_init(&service->workers[i].done, false);
        atomic_init(&service->workers[i].disconnecting, false);
        atomic_init(&service->workers[i].application_thread_done, false);
    }
    service->listener = cbm_daemon_ipc_listen_reserved(config->endpoint, reservation_io);
    if (!service->listener) {
        cbm_log_error("daemon.runtime.start_failed", "stage", "listener_handoff");
        runtime_service_destroy_unstarted(service);
        return NULL;
    }
    service->state = CBM_DAEMON_RUNTIME_SERVICE_RUNNING;
    if (cbm_thread_create(&service->accept_thread, RUNTIME_WORKER_STACK_SIZE, runtime_accept_loop,
                          service) != 0) {
        cbm_log_error("daemon.runtime.start_failed", "stage", "accept_thread");
        service->state = CBM_DAEMON_RUNTIME_SERVICE_EXITED;
        runtime_service_destroy_unstarted(service);
        return NULL;
    }
    service->accept_thread_started = true;
    return service;
}

cbm_daemon_runtime_service_t *cbm_daemon_runtime_service_start(
    const cbm_daemon_runtime_service_config_t *config) {
    if (!config || !config->endpoint) {
        return NULL;
    }
    cbm_daemon_ipc_startup_lock_t *startup_lock = NULL;
    if (cbm_daemon_ipc_startup_lock_try_acquire(config->endpoint, &startup_lock) != 1) {
        return NULL;
    }
    int generation =
        cbm_daemon_ipc_generation_probe_under_startup_lock(config->endpoint, startup_lock);
    if (generation != 0 || !cbm_daemon_ipc_startup_lock_prepare_handoff(startup_lock)) {
        runtime_startup_lock_release_complete(&startup_lock, config->shutdown_timeout_ms);
        return NULL;
    }
    cbm_daemon_ipc_participant_guard_t *participant_guard = NULL;
    if (cbm_daemon_ipc_participant_guard_try_join(config->endpoint, &participant_guard) != 1) {
        runtime_startup_lock_release_complete(&startup_lock, config->shutdown_timeout_ms);
        return NULL;
    }
    cbm_daemon_ipc_lifetime_reservation_t *reservation = NULL;
    if (cbm_daemon_ipc_lifetime_reservation_try_acquire(config->endpoint, &reservation) != 1) {
        if (!runtime_participant_guard_release_complete(&participant_guard,
                                                        config->shutdown_timeout_ms)) {
            runtime_cleanup_fail_stop("participant_guard_cleanup");
        }
        runtime_startup_lock_release_complete(&startup_lock, config->shutdown_timeout_ms);
        return NULL;
    }
    cbm_daemon_runtime_service_t *service =
        cbm_daemon_runtime_service_start_reserved(config, &reservation);
    cbm_daemon_ipc_lifetime_reservation_release(reservation);
    if (!service) {
        if (!runtime_participant_guard_release_complete(&participant_guard,
                                                        config->shutdown_timeout_ms)) {
            runtime_cleanup_fail_stop("participant_guard_cleanup");
        }
        runtime_startup_lock_release_complete(&startup_lock, config->shutdown_timeout_ms);
        return NULL;
    }
    service->owned_participant_guard = participant_guard;
    runtime_startup_lock_release_complete(&startup_lock, config->shutdown_timeout_ms);
    return service;
}

cbm_daemon_runtime_service_state_t cbm_daemon_runtime_service_state(
    cbm_daemon_runtime_service_t *service) {
    if (!service) {
        return CBM_DAEMON_RUNTIME_SERVICE_EXITED;
    }
    cbm_mutex_lock(&service->mutex);
    cbm_daemon_runtime_service_state_t state = service->state;
    cbm_mutex_unlock(&service->mutex);
    return state;
}

uint64_t cbm_daemon_runtime_service_clients_admitted_total(cbm_daemon_runtime_service_t *service) {
    if (!service) {
        return 0;
    }
    cbm_mutex_lock(&service->mutex);
    uint64_t total = service->admitted_total;
    cbm_mutex_unlock(&service->mutex);
    return total;
}

size_t cbm_daemon_runtime_service_active_clients(cbm_daemon_runtime_service_t *service) {
    if (!service) {
        return 0;
    }
    cbm_mutex_lock(&service->mutex);
    size_t count = service->committed_clients;
    cbm_mutex_unlock(&service->mutex);
    return count;
}

size_t cbm_daemon_runtime_service_active_connections(cbm_daemon_runtime_service_t *service) {
    if (!service) {
        return 0;
    }
    cbm_mutex_lock(&service->mutex);
    size_t count = service->active_connections;
    cbm_mutex_unlock(&service->mutex);
    return count;
}

size_t cbm_daemon_runtime_service_job_subscribers(cbm_daemon_runtime_service_t *service,
                                                  const char *project_key) {
    return service ? cbm_daemon_job_subscribers(service->coordinator, project_key) : 0;
}

uint64_t cbm_daemon_runtime_service_client_process_id(cbm_daemon_runtime_service_t *service,
                                                      cbm_daemon_client_id_t client_id) {
    if (!service || client_id == CBM_DAEMON_CLIENT_ID_INVALID) {
        return 0;
    }
    uint64_t process_id = 0;
    cbm_mutex_lock(&service->mutex);
    for (size_t i = 0; i < service->worker_capacity; i++) {
        cbm_daemon_runtime_worker_t *worker = &service->workers[i];
        if (worker->in_use && worker->admitted && worker->admission_committed &&
            worker->client_id == client_id) {
            process_id = worker->peer_process_id;
            break;
        }
    }
    cbm_mutex_unlock(&service->mutex);
    return process_id;
}

static bool runtime_wait_for_count(cbm_daemon_runtime_service_t *service, size_t expected,
                                   uint32_t timeout_ms, bool connections) {
    if (!service) {
        return false;
    }
    uint64_t deadline = runtime_deadline_after(timeout_ms);
    for (;;) {
        size_t actual = connections ? cbm_daemon_runtime_service_active_connections(service)
                                    : cbm_daemon_runtime_service_active_clients(service);
        if (actual == expected) {
            return true;
        }
        if (cbm_now_ms() >= deadline) {
            return false;
        }
        runtime_wait_tick(deadline);
    }
}

bool cbm_daemon_runtime_service_wait_for_clients(cbm_daemon_runtime_service_t *service,
                                                 size_t expected, uint32_t timeout_ms) {
    return runtime_wait_for_count(service, expected, timeout_ms, false);
}

bool cbm_daemon_runtime_service_wait_for_connections(cbm_daemon_runtime_service_t *service,
                                                     size_t expected, uint32_t timeout_ms) {
    return runtime_wait_for_count(service, expected, timeout_ms, true);
}

bool cbm_daemon_runtime_service_wait_exited(cbm_daemon_runtime_service_t *service,
                                            uint32_t timeout_ms) {
    if (!service) {
        return false;
    }
    uint64_t deadline = runtime_deadline_after(timeout_ms);
    for (;;) {
        if (cbm_daemon_runtime_service_state(service) == CBM_DAEMON_RUNTIME_SERVICE_EXITED &&
            atomic_load_explicit(&service->accept_thread_done, memory_order_acquire)) {
            return true;
        }
        if (cbm_now_ms() >= deadline) {
            return false;
        }
        runtime_wait_tick(deadline);
    }
}

bool cbm_daemon_runtime_service_job_reaped(cbm_daemon_runtime_service_t *service,
                                           const char *project_key) {
    return service && cbm_daemon_job_reaped(service->coordinator, project_key, cbm_now_ms());
}

bool cbm_daemon_runtime_service_stop(cbm_daemon_runtime_service_t *service, uint32_t timeout_ms) {
    if (!service) {
        return false;
    }
    if (cbm_daemon_runtime_service_state(service) == CBM_DAEMON_RUNTIME_SERVICE_EXITED) {
        return cbm_daemon_runtime_service_wait_exited(service, timeout_ms);
    }
    runtime_service_begin_stopping(service, timeout_ms, true, "service_stop");
    runtime_service_interrupt_connections(service);
    return cbm_daemon_runtime_service_wait_exited(service, timeout_ms);
}

bool cbm_daemon_runtime_service_free(cbm_daemon_runtime_service_t *service) {
    if (!service ||
        cbm_daemon_runtime_service_state(service) != CBM_DAEMON_RUNTIME_SERVICE_EXITED) {
        return false;
    }
    if (service->accept_thread_started && !service->accept_thread_joined) {
        if (cbm_thread_join(&service->accept_thread) != 0) {
            return false;
        }
        service->accept_thread_joined = true;
    }
    for (size_t i = 0; i < service->worker_capacity; i++) {
        cbm_daemon_runtime_worker_t *worker = &service->workers[i];
        if (worker->thread_started) {
            if (!atomic_load_explicit(&worker->done, memory_order_acquire) ||
                cbm_thread_join(&worker->thread) != 0) {
                return false;
            }
            runtime_worker_reset_after_join(worker);
        }
    }
    cbm_daemon_ipc_listener_close(service->listener);
    service->listener = NULL;
    if (!runtime_participant_guard_release_complete(&service->owned_participant_guard,
                                                    service->shutdown_timeout_ms)) {
        /* Preserve the service allocation so the caller can retry or force
         * process exit without dropping the only live cleanup handle. */
        return false;
    }
    cbm_daemon_coordinator_free(service->coordinator);
    (void)runtime_process_image_reference_release(&service->active_image);
    free(service->conflict_log_path);
    for (size_t i = 0; i < service->worker_mutexes_initialized; i++) {
        cbm_mutex_destroy(&service->workers[i].send_mutex);
    }
    free(service->workers);
    cbm_mutex_destroy(&service->mutex);
    free(service);
    return true;
}

bool cbm_daemon_runtime_request_activation_shutdown(
    const cbm_daemon_ipc_endpoint_t *endpoint, const cbm_daemon_build_identity_t *identity,
    cbm_daemon_runtime_activation_action_t action, uint32_t timeout_ms,
    cbm_daemon_runtime_activation_result_t *result_out) {
    if (result_out) {
        memset(result_out, 0, sizeof(*result_out));
    }
    uint8_t request[CBM_DAEMON_ACTIVATION_SHUTDOWN_REQUEST_SIZE];
    if (!endpoint || !identity || !result_out || timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER ||
        !runtime_activation_request_encode(request, action, identity)) {
        return false;
    }
    cbm_daemon_ipc_connection_t *connection = cbm_daemon_ipc_connect(endpoint, timeout_ms);
    bool sent = connection && cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                                        CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN,
                                                        request, (uint32_t)sizeof(request));
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received = sent ? cbm_daemon_ipc_receive_frame_bounded(
                              connection, timeout_ms, CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE,
                              &frame, &payload)
                        : 0;
    bool valid = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                 frame.flags == CBM_DAEMON_RUNTIME_OP_ACTIVATION_SHUTDOWN &&
                 frame.length == CBM_DAEMON_ACTIVATION_SHUTDOWN_RESPONSE_SIZE &&
                 runtime_activation_response_decode(payload, result_out);
    free(payload);
    cbm_daemon_ipc_connection_close(connection);
    if (!valid) {
        memset(result_out, 0, sizeof(*result_out));
    }
    return valid;
}

static bool runtime_control_request_send(const cbm_daemon_ipc_endpoint_t *endpoint,
                                         const cbm_daemon_build_identity_t *identity,
                                         cbm_daemon_runtime_operation_t operation,
                                         uint32_t timeout_ms, uint32_t response_size,
                                         uint8_t **payload_out) {
    *payload_out = NULL;
    if (!endpoint || !identity || !identity->build_fingerprint ||
        timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER) {
        return false;
    }
    uint8_t request[CBM_DAEMON_CONTROL_REQUEST_SIZE];
    memset(request, 0, sizeof(request));
    int written = snprintf((char *)request, sizeof(request), "%s", identity->build_fingerprint);
    if (written <= 0 || (size_t)written != CBM_DAEMON_BUILD_FINGERPRINT_SIZE - 1U) {
        return false;
    }
    cbm_daemon_ipc_connection_t *connection = cbm_daemon_ipc_connect(endpoint, timeout_ms);
    bool sent =
        connection && cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST, operation,
                                                request, (uint32_t)sizeof(request));
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received = sent ? cbm_daemon_ipc_receive_frame_bounded(connection, timeout_ms,
                                                               response_size, &frame, &payload)
                        : 0;
    bool valid = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                 frame.flags == operation && frame.length == response_size && payload &&
                 payload[0] == 1U;
    cbm_daemon_ipc_connection_close(connection);
    if (!valid) {
        free(payload);
        return false;
    }
    *payload_out = payload;
    return true;
}

static uint32_t runtime_control_read_u32(const uint8_t *bytes) {
    return ((uint32_t)bytes[0] << 24) | ((uint32_t)bytes[1] << 16) | ((uint32_t)bytes[2] << 8) |
           (uint32_t)bytes[3];
}

bool cbm_daemon_runtime_request_status(const cbm_daemon_ipc_endpoint_t *endpoint,
                                       const cbm_daemon_build_identity_t *identity,
                                       uint32_t timeout_ms,
                                       cbm_daemon_runtime_status_t *status_out) {
    if (!status_out) {
        return false;
    }
    memset(status_out, 0, sizeof(*status_out));
    uint8_t *payload = NULL;
    if (!runtime_control_request_send(endpoint, identity, CBM_DAEMON_RUNTIME_OP_STATUS, timeout_ms,
                                      CBM_DAEMON_STATUS_RESPONSE_SIZE, &payload)) {
        return false;
    }
    status_out->permanent = (payload[1] & 0x01U) != 0U;
    status_out->stopping = (payload[1] & 0x02U) != 0U;
    status_out->committed_clients = (uint16_t)(((uint16_t)payload[2] << 8) | payload[3]);
    status_out->daemon_pid = runtime_control_read_u32(payload + 4);
    status_out->client_count = payload[8] > CBM_DAEMON_CONTROL_CLIENT_CAP
                                   ? (uint8_t)CBM_DAEMON_CONTROL_CLIENT_CAP
                                   : payload[8];
    for (size_t index = 0; index < CBM_DAEMON_CONTROL_CLIENT_CAP; index++) {
        status_out->client_pids[index] = runtime_control_read_u32(payload + 12U + index * 4U);
    }
    memcpy(status_out->build_fingerprint, payload + 44U, CBM_DAEMON_BUILD_FINGERPRINT_SIZE);
    status_out->build_fingerprint[CBM_DAEMON_BUILD_FINGERPRINT_SIZE - 1U] = '\0';
    memcpy(status_out->semantic_version, payload + 109U, sizeof(status_out->semantic_version));
    status_out->semantic_version[sizeof(status_out->semantic_version) - 1U] = '\0';
    free(payload);
    return true;
}

bool cbm_daemon_runtime_request_stop(const cbm_daemon_ipc_endpoint_t *endpoint,
                                     const cbm_daemon_build_identity_t *identity,
                                     uint32_t timeout_ms,
                                     cbm_daemon_runtime_stop_result_t *result_out) {
    if (!result_out) {
        return false;
    }
    memset(result_out, 0, sizeof(*result_out));
    uint8_t *payload = NULL;
    if (!runtime_control_request_send(endpoint, identity, CBM_DAEMON_RUNTIME_OP_STOP, timeout_ms,
                                      CBM_DAEMON_STOP_RESPONSE_SIZE, &payload)) {
        return false;
    }
    result_out->accepted = (payload[1] & 0x01U) != 0U;
    result_out->busy = (payload[1] & 0x02U) != 0U;
    result_out->committed_clients = (uint16_t)(((uint16_t)payload[2] << 8) | payload[3]);
    result_out->client_count = payload[4] > CBM_DAEMON_CONTROL_CLIENT_CAP
                                   ? (uint8_t)CBM_DAEMON_CONTROL_CLIENT_CAP
                                   : payload[4];
    for (size_t index = 0; index < CBM_DAEMON_CONTROL_CLIENT_CAP; index++) {
        result_out->client_pids[index] = runtime_control_read_u32(payload + 8U + index * 4U);
    }
    free(payload);
    return true;
}

cbm_daemon_runtime_client_t *cbm_daemon_runtime_client_connect(
    const cbm_daemon_ipc_endpoint_t *endpoint, const cbm_daemon_build_identity_t *identity,
    uint32_t timeout_ms, cbm_daemon_runtime_connect_result_t *result_out) {
    if (result_out) {
        memset(result_out, 0, sizeof(*result_out));
    }
    uint8_t request[CBM_DAEMON_RENDEZVOUS_REQUEST_SIZE];
    if (!endpoint || !identity || !result_out || timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER ||
        !cbm_daemon_runtime_hello_request_encode(request, identity)) {
        return NULL;
    }
    cbm_daemon_ipc_connection_t *connection = cbm_daemon_ipc_connect(endpoint, timeout_ms);
    if (!connection || !cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                                  CBM_DAEMON_RUNTIME_OP_HELLO, request,
                                                  (uint32_t)sizeof(request))) {
        cbm_daemon_ipc_connection_close(connection);
        return NULL;
    }
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received = cbm_daemon_ipc_receive_frame_bounded(
        connection, timeout_ms, CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE, &frame, &payload);
    bool valid = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                 frame.flags == CBM_DAEMON_RUNTIME_OP_HELLO &&
                 frame.length == CBM_DAEMON_RENDEZVOUS_RESPONSE_SIZE &&
                 runtime_hello_response_decode(payload, result_out);
    free(payload);
    if (!valid || result_out->status != CBM_DAEMON_RUNTIME_CONNECT_ACCEPTED) {
        cbm_daemon_ipc_connection_close(connection);
        return NULL;
    }

    cbm_daemon_runtime_client_t *client = calloc(1, sizeof(*client));
    if (!client) {
        cbm_daemon_ipc_connection_close(connection);
        memset(result_out, 0, sizeof(*result_out));
        return NULL;
    }
    cbm_mutex_init(&client->exchange_mutex);
    cbm_mutex_init(&client->send_mutex);
    cbm_mutex_init(&client->state_mutex);
    client->connection = connection;
    client->client_id = result_out->client_id;
    client->authenticated_process_id = result_out->authenticated_process_id;
    client->usable = true;
    return client;
}

cbm_daemon_client_id_t cbm_daemon_runtime_client_id(const cbm_daemon_runtime_client_t *client) {
    return client ? client->client_id : CBM_DAEMON_CLIENT_ID_INVALID;
}

uint64_t cbm_daemon_runtime_client_process_id(const cbm_daemon_runtime_client_t *client) {
    return client ? client->authenticated_process_id : 0;
}

static bool runtime_client_exchange(cbm_daemon_runtime_client_t *client,
                                    cbm_daemon_runtime_operation_t operation, const void *request,
                                    uint32_t request_length, uint32_t timeout_ms,
                                    uint8_t **response_out, uint32_t expected_response_length) {
    *response_out = NULL;
    if (timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER) {
        return false;
    }
    cbm_mutex_lock(&client->exchange_mutex);
    cbm_mutex_lock(&client->state_mutex);
    if (!client->usable || client->closing || !client->connection) {
        cbm_mutex_unlock(&client->state_mutex);
        cbm_mutex_unlock(&client->exchange_mutex);
        return false;
    }
    cbm_daemon_ipc_connection_t *connection = client->connection;
    client->exchange_active = true;
    cbm_mutex_unlock(&client->state_mutex);

    cbm_mutex_lock(&client->send_mutex);
    bool sent = cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST, (uint16_t)operation,
                                          request, request_length);
    cbm_mutex_unlock(&client->send_mutex);
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received =
        sent ? cbm_daemon_ipc_receive_frame(connection, timeout_ms, &frame, &payload) : -1;
    bool valid = sent && received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                 frame.flags == (uint16_t)operation && frame.length == expected_response_length;
    cbm_mutex_lock(&client->state_mutex);
    client->exchange_active = false;
    if (!valid) {
        client->usable = false;
    }
    cbm_mutex_unlock(&client->state_mutex);
    if (!valid) {
        free(payload);
    } else {
        *response_out = payload;
    }
    cbm_mutex_unlock(&client->exchange_mutex);
    return valid;
}

cbm_daemon_subscription_result_t cbm_daemon_runtime_client_job_subscribe(
    cbm_daemon_runtime_client_t *client, const char *project_key,
    cbm_daemon_subscription_id_t *subscription_id_out, uint32_t timeout_ms) {
    if (subscription_id_out) {
        *subscription_id_out = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    }
    size_t key_length = 0;
    if (!client || !subscription_id_out ||
        !runtime_bounded_length(project_key, CBM_DAEMON_RUNTIME_PROJECT_KEY_MAX + 1, &key_length) ||
        key_length == 0 || key_length > CBM_DAEMON_RUNTIME_PROJECT_KEY_MAX) {
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }
    uint8_t request[SUBSCRIBE_REQUEST_PREFIX_SIZE + CBM_DAEMON_RUNTIME_PROJECT_KEY_MAX];
    runtime_put_u32(request, (uint32_t)key_length);
    memcpy(request + SUBSCRIBE_REQUEST_PREFIX_SIZE, project_key, key_length);
    uint8_t *response = NULL;
    if (!runtime_client_exchange(client, CBM_DAEMON_RUNTIME_OP_JOB_SUBSCRIBE, request,
                                 (uint32_t)(SUBSCRIBE_REQUEST_PREFIX_SIZE + key_length), timeout_ms,
                                 &response, SUBSCRIBE_RESPONSE_SIZE)) {
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }
    cbm_daemon_subscription_result_t result =
        (cbm_daemon_subscription_result_t)runtime_get_u32(response);
    cbm_daemon_subscription_id_t subscription_id = runtime_get_u64(response + 4);
    free(response);
    if ((result != CBM_DAEMON_SUBSCRIPTION_STARTED && result != CBM_DAEMON_SUBSCRIPTION_JOINED) ||
        subscription_id == CBM_DAEMON_SUBSCRIPTION_ID_INVALID) {
        return CBM_DAEMON_SUBSCRIPTION_REJECTED;
    }
    *subscription_id_out = subscription_id;
    return result;
}

bool cbm_daemon_runtime_client_job_unsubscribe(cbm_daemon_runtime_client_t *client,
                                               cbm_daemon_subscription_id_t subscription_id,
                                               uint32_t timeout_ms) {
    if (!client || subscription_id == CBM_DAEMON_SUBSCRIPTION_ID_INVALID) {
        return false;
    }
    uint8_t request[UNSUBSCRIBE_REQUEST_SIZE];
    runtime_put_u64(request, subscription_id);
    uint8_t *response = NULL;
    if (!runtime_client_exchange(client, CBM_DAEMON_RUNTIME_OP_JOB_UNSUBSCRIBE, request,
                                 (uint32_t)sizeof(request), timeout_ms, &response,
                                 STATUS_RESPONSE_SIZE)) {
        return false;
    }
    bool removed = runtime_get_u32(response) == 1;
    free(response);
    return removed;
}

bool cbm_daemon_runtime_client_heartbeat(cbm_daemon_runtime_client_t *client, uint32_t timeout_ms) {
    if (!client) {
        return false;
    }
    uint8_t *response = NULL;
    if (!runtime_client_exchange(client, CBM_DAEMON_RUNTIME_OP_HEARTBEAT, NULL, 0, timeout_ms,
                                 &response, STATUS_RESPONSE_SIZE)) {
        return false;
    }
    bool valid = runtime_get_u32(response) == 1;
    free(response);
    return valid;
}

bool cbm_daemon_runtime_client_application_token_reserve(
    cbm_daemon_runtime_client_t *client, cbm_daemon_runtime_application_token_t *token_out) {
    if (token_out) {
        *token_out = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    }
    if (!client || !token_out) {
        return false;
    }
    cbm_mutex_lock(&client->state_mutex);
    bool valid = client->usable && !client->closing && client->connection &&
                 client->active_application_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID &&
                 client->next_application_token == client->last_started_application_token &&
                 client->next_application_token != UINT64_MAX;
    if (valid) {
        client->next_application_token++;
        *token_out = client->next_application_token;
    }
    cbm_mutex_unlock(&client->state_mutex);
    return valid;
}

cbm_daemon_runtime_cancel_result_t cbm_daemon_runtime_client_application_cancel(
    cbm_daemon_runtime_client_t *client, cbm_daemon_runtime_application_token_t request_token) {
    if (!client || request_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID) {
        return CBM_DAEMON_RUNTIME_CANCEL_ERROR;
    }

    bool dispatch = false;
    cbm_mutex_lock(&client->state_mutex);
    if (!client->usable || client->closing || !client->connection) {
        cbm_mutex_unlock(&client->state_mutex);
        return CBM_DAEMON_RUNTIME_CANCEL_ERROR;
    }
    if (client->active_application_token == request_token) {
        client->pending_cancel_token = request_token;
        dispatch = client->application_request_sent && !client->application_cancel_sent;
    } else if (client->active_application_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID &&
               request_token == client->next_application_token &&
               request_token > client->last_started_application_token) {
        /* The frontend publishes its active identity immediately after token
         * reservation. Preserve this one pre-send cancellation until the
         * tagged exchange emits REQUEST then CANCEL under send_mutex. */
        client->pending_cancel_token = request_token;
    } else {
        cbm_mutex_unlock(&client->state_mutex);
        return CBM_DAEMON_RUNTIME_CANCEL_STALE;
    }
    cbm_mutex_unlock(&client->state_mutex);
    if (!dispatch) {
        return CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED;
    }

    uint8_t wire[APPLICATION_CANCEL_REQUEST_SIZE];
    runtime_put_u64(wire, request_token);
    cbm_daemon_ipc_connection_t *connection = NULL;
    bool already_sent = false;
    bool unavailable = false;
    cbm_mutex_lock(&client->send_mutex);
    cbm_mutex_lock(&client->state_mutex);
    dispatch = client->usable && !client->closing && client->connection &&
               client->active_application_token == request_token &&
               client->application_request_sent && !client->application_cancel_sent;
    if (dispatch) {
        client->application_cancel_sent = true;
        connection = client->connection;
    } else {
        unavailable = !client->usable || client->closing || !client->connection;
        already_sent = client->usable && !client->closing && client->connection &&
                       client->active_application_token == request_token &&
                       client->application_cancel_sent;
    }
    cbm_mutex_unlock(&client->state_mutex);
    bool sent = !dispatch || cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                                       CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL,
                                                       wire, (uint32_t)sizeof(wire));
    cbm_mutex_unlock(&client->send_mutex);
    if (!dispatch) {
        /* The request path may have observed pending_cancel_token and emitted
         * the control while this caller waited for send_mutex. That is still
         * an accepted cancellation, not a stale target. */
        return already_sent ? CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED
                            : (unavailable ? CBM_DAEMON_RUNTIME_CANCEL_ERROR
                                           : CBM_DAEMON_RUNTIME_CANCEL_STALE);
    }
    if (!sent) {
        cbm_mutex_lock(&client->state_mutex);
        client->usable = false;
        cbm_mutex_unlock(&client->state_mutex);
        cbm_daemon_ipc_connection_interrupt(connection);
        return CBM_DAEMON_RUNTIME_CANCEL_ERROR;
    }
    return CBM_DAEMON_RUNTIME_CANCEL_ACCEPTED;
}

cbm_daemon_runtime_application_status_t cbm_daemon_runtime_client_application_request_tagged(
    cbm_daemon_runtime_client_t *client, cbm_daemon_runtime_application_token_t request_token,
    const void *request, uint32_t request_length, uint8_t **response_out,
    uint32_t *response_length_out, uint32_t timeout_ms) {
    if (response_out) {
        *response_out = NULL;
    }
    if (response_length_out) {
        *response_length_out = 0;
    }
    if (!client || !response_out || !response_length_out ||
        request_token == CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID ||
        timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER ||
        request_length > CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX ||
        (request_length > 0 && !request)) {
        return CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    }
    uint64_t wire_length = APPLICATION_REQUEST_PREFIX_SIZE + (uint64_t)request_length;
    if (wire_length > CBM_DAEMON_MAX_FRAME_SIZE || wire_length > UINT32_MAX) {
        return CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    }
    uint8_t *wire = malloc((size_t)wire_length);
    if (!wire) {
        return CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    }
    runtime_put_u64(wire, request_token);
    runtime_put_u32(wire + 8, request_length);
    if (request_length > 0) {
        memcpy(wire + APPLICATION_REQUEST_PREFIX_SIZE, request, request_length);
    }

    cbm_mutex_lock(&client->exchange_mutex);
    cbm_mutex_lock(&client->state_mutex);
    if (!client->usable || client->closing || !client->connection ||
        request_token != client->next_application_token ||
        request_token <= client->last_started_application_token ||
        client->active_application_token != CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID) {
        cbm_mutex_unlock(&client->state_mutex);
        cbm_mutex_unlock(&client->exchange_mutex);
        free(wire);
        return CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    }
    cbm_daemon_ipc_connection_t *connection = client->connection;
    client->exchange_active = true;
    client->last_started_application_token = request_token;
    client->active_application_token = request_token;
    client->application_request_sent = false;
    client->application_cancel_sent = false;
    if (client->pending_cancel_token != request_token) {
        client->pending_cancel_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    }
    cbm_mutex_unlock(&client->state_mutex);

    uint8_t cancel_wire[APPLICATION_CANCEL_REQUEST_SIZE];
    runtime_put_u64(cancel_wire, request_token);
    cbm_mutex_lock(&client->send_mutex);
    cbm_mutex_lock(&client->state_mutex);
    bool request_can_send = client->usable && !client->closing &&
                            client->connection == connection &&
                            client->active_application_token == request_token;
    cbm_mutex_unlock(&client->state_mutex);
    bool sent =
        request_can_send && cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                                      CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST,
                                                      wire, (uint32_t)wire_length);
    bool send_cancel = false;
    cbm_mutex_lock(&client->state_mutex);
    client->application_request_sent = sent;
    send_cancel = sent && !client->closing && client->pending_cancel_token == request_token &&
                  !client->application_cancel_sent;
    if (send_cancel) {
        client->application_cancel_sent = true;
    }
    cbm_mutex_unlock(&client->state_mutex);
    if (send_cancel) {
        sent = cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                         CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL, cancel_wire,
                                         (uint32_t)sizeof(cancel_wire));
        if (!sent) {
            cbm_mutex_lock(&client->state_mutex);
            client->usable = false;
            cbm_mutex_unlock(&client->state_mutex);
        }
    }
    cbm_mutex_unlock(&client->send_mutex);
    free(wire);
    cbm_daemon_frame_t frame = {0};
    uint8_t *payload = NULL;
    int received =
        sent ? cbm_daemon_ipc_receive_frame(connection, timeout_ms, &frame, &payload) : -1;
    bool protocol_valid = sent && received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                          frame.flags == CBM_DAEMON_RUNTIME_OP_APPLICATION_REQUEST &&
                          frame.length >= APPLICATION_RESPONSE_PREFIX_SIZE && payload;
    cbm_daemon_runtime_application_status_t status = CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    uint32_t response_length = 0;
    if (protocol_valid) {
        cbm_daemon_runtime_application_token_t response_token = runtime_get_u64(payload);
        status = (cbm_daemon_runtime_application_status_t)runtime_get_u32(payload + 8);
        response_length = runtime_get_u32(payload + 12);
        protocol_valid = response_token == request_token &&
                         status >= CBM_DAEMON_RUNTIME_APPLICATION_OK &&
                         status <= CBM_DAEMON_RUNTIME_APPLICATION_CANCELLED &&
                         response_length <= CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX &&
                         frame.length == APPLICATION_RESPONSE_PREFIX_SIZE + response_length &&
                         (status == CBM_DAEMON_RUNTIME_APPLICATION_OK || response_length == 0);
    }

    uint8_t *response_copy = NULL;
    bool copied = true;
    if (protocol_valid && response_length > 0) {
        response_copy = malloc(response_length);
        copied = response_copy != NULL;
        if (copied) {
            memcpy(response_copy, payload + APPLICATION_RESPONSE_PREFIX_SIZE, response_length);
        }
    }
    free(payload);

    cbm_mutex_lock(&client->state_mutex);
    client->exchange_active = false;
    if (client->active_application_token == request_token) {
        client->active_application_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
        client->application_request_sent = false;
        client->application_cancel_sent = false;
    }
    if (client->pending_cancel_token == request_token) {
        client->pending_cancel_token = CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    }
    if (!protocol_valid) {
        client->usable = false;
    }
    cbm_mutex_unlock(&client->state_mutex);
    cbm_mutex_unlock(&client->exchange_mutex);

    if (!protocol_valid || !copied) {
        free(response_copy);
        return CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    }
    *response_out = response_copy;
    *response_length_out = response_length;
    return status;
}

cbm_daemon_runtime_application_status_t cbm_daemon_runtime_client_application_request(
    cbm_daemon_runtime_client_t *client, const void *request, uint32_t request_length,
    uint8_t **response_out, uint32_t *response_length_out, uint32_t timeout_ms) {
    if (response_out) {
        *response_out = NULL;
    }
    if (response_length_out) {
        *response_length_out = 0;
    }
    if (!client || !response_out || !response_length_out ||
        timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER ||
        request_length > CBM_DAEMON_RUNTIME_APPLICATION_PAYLOAD_MAX ||
        (request_length > 0 && !request)) {
        return CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    }
    cbm_daemon_runtime_application_token_t request_token =
        CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    if (!cbm_daemon_runtime_client_application_token_reserve(client, &request_token)) {
        return CBM_DAEMON_RUNTIME_APPLICATION_TRANSPORT_ERROR;
    }
    return cbm_daemon_runtime_client_application_request_tagged(client, request_token, request,
                                                                request_length, response_out,
                                                                response_length_out, timeout_ms);
}

bool cbm_daemon_runtime_client_close_begin(cbm_daemon_runtime_client_t *client) {
    if (!client) {
        return false;
    }
    /* Serialize with request publication. If the request won send_mutex, its
     * exact token is visible here and cancellation is ordered after REQUEST;
     * if close won, the request path's closing recheck refuses the late send. */
    cbm_mutex_lock(&client->send_mutex);
    cbm_mutex_lock(&client->state_mutex);
    if (client->closing) {
        cbm_mutex_unlock(&client->state_mutex);
        cbm_mutex_unlock(&client->send_mutex);
        return false;
    }
    client->closing = true;
    client->close_interrupted_exchange = client->exchange_active;
    cbm_daemon_ipc_connection_t *connection = client->connection;
    cbm_daemon_runtime_application_token_t cancel_token = client->active_application_token;
    bool send_cancel = client->close_interrupted_exchange && connection &&
                       client->application_request_sent && !client->application_cancel_sent &&
                       cancel_token != CBM_DAEMON_RUNTIME_APPLICATION_TOKEN_INVALID;
    if (send_cancel) {
        client->application_cancel_sent = true;
    }
    cbm_mutex_unlock(&client->state_mutex);

    if (send_cancel) {
        uint8_t wire[APPLICATION_CANCEL_REQUEST_SIZE];
        runtime_put_u64(wire, cancel_token);
        (void)cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                        CBM_DAEMON_RUNTIME_OP_APPLICATION_CANCEL, wire,
                                        (uint32_t)sizeof(wire));
    }
    if (client->close_interrupted_exchange && connection) {
        /* The interrupted-exchange close cannot run the DISCONNECT handshake,
         * so announce the departure explicitly (ordered after the cancel);
         * the server releases this client's admission on receipt instead of
         * waiting for the handle to close. Mirrors POSIX shutdown() timing. */
        (void)cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                        CBM_DAEMON_RUNTIME_OP_CLOSE_INTENT, NULL, 0);
        cbm_daemon_ipc_connection_interrupt(connection);
    }
    cbm_mutex_unlock(&client->send_mutex);
    return true;
}

bool cbm_daemon_runtime_client_close_finish(cbm_daemon_runtime_client_t *client,
                                            uint32_t timeout_ms) {
    if (!client) {
        return false;
    }
    cbm_mutex_lock(&client->state_mutex);
    bool closing = client->closing;
    cbm_mutex_unlock(&client->state_mutex);
    if (!closing) {
        return false;
    }

    cbm_mutex_lock(&client->exchange_mutex);
    bool acknowledged = false;
    cbm_mutex_lock(&client->state_mutex);
    bool can_disconnect = timeout_ms != CBM_DAEMON_IPC_WAIT_FOREVER &&
                          !client->close_interrupted_exchange && client->usable &&
                          client->connection != NULL;
    cbm_daemon_ipc_connection_t *connection = client->connection;
    cbm_mutex_unlock(&client->state_mutex);
    bool disconnect_sent = false;
    if (can_disconnect) {
        cbm_mutex_lock(&client->send_mutex);
        disconnect_sent = cbm_daemon_ipc_send_frame(connection, CBM_DAEMON_FRAME_REQUEST,
                                                    CBM_DAEMON_RUNTIME_OP_DISCONNECT, NULL, 0);
        cbm_mutex_unlock(&client->send_mutex);
    }
    if (disconnect_sent) {
        cbm_daemon_frame_t frame = {0};
        uint8_t *response = NULL;
        int received = cbm_daemon_ipc_receive_frame(connection, timeout_ms, &frame, &response);
        acknowledged = received == 1 && frame.type == CBM_DAEMON_FRAME_RESPONSE &&
                       frame.flags == CBM_DAEMON_RUNTIME_OP_DISCONNECT &&
                       frame.length == STATUS_RESPONSE_SIZE && response &&
                       runtime_get_u32(response) == 1;
        free(response);
    }

    cbm_mutex_lock(&client->state_mutex);
    connection = client->connection;
    client->connection = NULL;
    client->usable = false;
    client->exchange_active = false;
    cbm_mutex_unlock(&client->state_mutex);
    cbm_daemon_ipc_connection_close(connection);
    cbm_mutex_unlock(&client->exchange_mutex);
    cbm_mutex_destroy(&client->state_mutex);
    cbm_mutex_destroy(&client->send_mutex);
    cbm_mutex_destroy(&client->exchange_mutex);
    free(client);
    return acknowledged;
}

bool cbm_daemon_runtime_client_close(cbm_daemon_runtime_client_t *client, uint32_t timeout_ms) {
    if (!cbm_daemon_runtime_client_close_begin(client)) {
        return false;
    }
    return cbm_daemon_runtime_client_close_finish(client, timeout_ms);
}
