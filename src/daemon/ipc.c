/*
 * ipc.c — Authenticated, owner-scoped local transport for the CBM daemon.
 */
#include "daemon/ipc.h"
#include "daemon/ipc_internal.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/macos_acl.h"
#include "foundation/private_file_lock_internal.h"
#include "foundation/sha256.h"

#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    CBM_DAEMON_IPC_SEND_TIMEOUT_MS = 5000,
    CBM_DAEMON_IPC_PATH_CAP = 4096,
    CBM_DAEMON_IPC_RETRY_INTERVAL_MS = 10,
    CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS = 500,
    CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RETRY_MS = 250,
};

/* Last private-namespace validation refusal, set at the exact failing check.
 * A bare "cache-private" status is undiagnosable in the field; the refusal
 * names the object and rule instead. Last-writer-wins, diagnostic only. */
static char ipc_validation_detail_buffer[384];

static void ipc_validation_detail_set(const char *format, ...) {
    va_list arguments;
    va_start(arguments, format);
    (void)vsnprintf(ipc_validation_detail_buffer, sizeof(ipc_validation_detail_buffer), format,
                    arguments);
    va_end(arguments);
}

const char *cbm_daemon_ipc_validation_detail(void) {
    return ipc_validation_detail_buffer;
}

static bool instance_key_valid(const char *key) {
    if (!key) {
        return false;
    }
    for (size_t i = 0; i < 16; i++) {
        char ch = key[i];
        bool decimal = ch >= '0' && ch <= '9';
        bool lower = ch >= 'a' && ch <= 'f';
        if (!decimal && !lower) {
            return false;
        }
    }
    return key[16] == '\0';
}

static char *string_copy(const char *value) {
    if (!value) {
        return NULL;
    }
    size_t length = strlen(value);
    char *copy = malloc(length + 1);
    if (copy) {
        memcpy(copy, value, length + 1);
    }
    return copy;
}

static char *string_format(const char *format, ...) {
    va_list args;
    va_start(args, format);
    va_list measure;
    va_copy(measure, args);
    int needed = vsnprintf(NULL, 0, format, measure);
    va_end(measure);
    if (needed < 0) {
        va_end(args);
        return NULL;
    }
    char *result = malloc((size_t)needed + 1);
    if (!result || vsnprintf(result, (size_t)needed + 1, format, args) != needed) {
        free(result);
        result = NULL;
    }
    va_end(args);
    return result;
}

static cbm_daemon_ipc_posix_publication_hook_fn g_posix_publication_hook_for_test;
static void *g_posix_publication_hook_context_for_test;
static atomic_uint g_windows_legacy_guard_release_failures_for_test;

void cbm_daemon_ipc_posix_publication_hook_set_for_test(
    cbm_daemon_ipc_posix_publication_hook_fn hook, void *context) {
    g_posix_publication_hook_context_for_test = context;
    g_posix_publication_hook_for_test = hook;
}

void cbm_daemon_ipc_windows_legacy_guard_release_failures_set_for_test(unsigned int count) {
    atomic_store_explicit(&g_windows_legacy_guard_release_failures_for_test, count,
                          memory_order_release);
}

bool cbm_daemon_ipc_windows_legacy_names(const char *canonical_runtime_parent,
                                         const char *instance_key,
                                         char pipe_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP],
                                         char startup_mutex_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP]) {
    if (!canonical_runtime_parent || !canonical_runtime_parent[0] ||
        !instance_key_valid(instance_key) || !pipe_out || !startup_mutex_out) {
        return false;
    }
    uint64_t hash = UINT64_C(14695981039346656037);
    const unsigned char *cursor = (const unsigned char *)canonical_runtime_parent;
    while (*cursor) {
        hash ^= *cursor++;
        hash *= UINT64_C(1099511628211);
    }
    int pipe_length =
        snprintf(pipe_out, CBM_DAEMON_IPC_WINDOWS_NAME_CAP, "\\\\.\\pipe\\cbm-daemon-%016llx-%s",
                 (unsigned long long)hash, instance_key);
    int mutex_length =
        snprintf(startup_mutex_out, CBM_DAEMON_IPC_WINDOWS_NAME_CAP,
                 "Local\\cbm-daemon-%016llx-%s-startup", (unsigned long long)hash, instance_key);
    return pipe_length > 0 && (size_t)pipe_length < CBM_DAEMON_IPC_WINDOWS_NAME_CAP &&
           mutex_length > 0 && (size_t)mutex_length < CBM_DAEMON_IPC_WINDOWS_NAME_CAP;
}

static bool windows_sid_valid(const uint8_t *sid, size_t sid_length) {
    enum {
        WINDOWS_SID_HEADER_SIZE = 8,
        WINDOWS_SID_SUBAUTHORITY_SIZE = 4,
        WINDOWS_SID_SUBAUTHORITY_MAX = 15,
    };
    return sid && sid_length >= WINDOWS_SID_HEADER_SIZE && sid[0] == 1 &&
           sid[1] <= WINDOWS_SID_SUBAUTHORITY_MAX &&
           sid_length == WINDOWS_SID_HEADER_SIZE + (size_t)sid[1] * WINDOWS_SID_SUBAUTHORITY_SIZE;
}

static bool windows_pipe_address_valid(const char *address) {
    static const char prefix[] = "\\\\.\\pipe\\cbm-daemon-";
    if (!address || strncmp(address, prefix, sizeof(prefix) - 1U) != 0) {
        return false;
    }
    const char *digest = address + sizeof(prefix) - 1U;
    for (size_t index = 0; index < CBM_SHA256_HEX_LEN; index++) {
        char character = digest[index];
        if (!((character >= '0' && character <= '9') || (character >= 'a' && character <= 'f'))) {
            return false;
        }
    }
    return digest[CBM_SHA256_HEX_LEN] == '\0';
}

bool cbm_daemon_ipc_windows_generation_address(
    const uint8_t *sid, size_t sid_length, const char *instance_key,
    const uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE],
    char address_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP]) {
    static const uint8_t domain[] = "cbm-daemon-win-pipe-v1";
    if (!windows_sid_valid(sid, sid_length) || !instance_key_valid(instance_key) || !nonce ||
        !address_out) {
        return false;
    }
    cbm_sha256_ctx context;
    cbm_sha256_init(&context);
    cbm_sha256_update(&context, domain, sizeof(domain) - 1U);
    cbm_sha256_update(&context, sid, sid_length);
    cbm_sha256_update(&context, instance_key, 16U);
    cbm_sha256_update(&context, nonce, CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE);
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&context, digest);

    char digest_hex[CBM_SHA256_HEX_LEN + 1U];
    static const char hex[] = "0123456789abcdef";
    for (size_t index = 0; index < sizeof(digest); index++) {
        digest_hex[index * 2U] = hex[digest[index] >> 4U];
        digest_hex[index * 2U + 1U] = hex[digest[index] & 0x0fU];
    }
    digest_hex[CBM_SHA256_HEX_LEN] = '\0';
    int written = snprintf(address_out, CBM_DAEMON_IPC_WINDOWS_NAME_CAP,
                           "\\\\.\\pipe\\cbm-daemon-%s", digest_hex);
    return written > 0 && (size_t)written < CBM_DAEMON_IPC_WINDOWS_NAME_CAP;
}

bool cbm_daemon_ipc_windows_rendezvous_record_encode(
    const uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE], const char *address,
    uint8_t record_out[CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE]) {
    static const uint8_t magic[8] = {'C', 'B', 'M', 'R', 'D', 'V', '1', 0};
    if (!nonce || !windows_pipe_address_valid(address) || !record_out) {
        return false;
    }
    memset(record_out, 0, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE);
    memcpy(record_out, magic, sizeof(magic));
    memcpy(record_out + sizeof(magic), nonce, CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE);
    size_t address_length = strlen(address);
    memcpy(record_out + sizeof(magic) + CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE, address,
           address_length + 1U);
    return true;
}

bool cbm_daemon_ipc_windows_rendezvous_record_decode(
    const uint8_t *record, size_t record_length,
    uint8_t nonce_out[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE],
    char address_out[CBM_DAEMON_IPC_WINDOWS_NAME_CAP]) {
    static const uint8_t magic[8] = {'C', 'B', 'M', 'R', 'D', 'V', '1', 0};
    if (!record || record_length != CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE || !nonce_out ||
        !address_out || memcmp(record, magic, sizeof(magic)) != 0) {
        return false;
    }
    const uint8_t *encoded_address = record + sizeof(magic) + CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE;
    const uint8_t *terminator = memchr(encoded_address, 0, CBM_DAEMON_IPC_WINDOWS_NAME_CAP);
    if (!terminator) {
        return false;
    }
    size_t address_length = (size_t)(terminator - encoded_address);
    if (address_length + 1U > CBM_DAEMON_IPC_WINDOWS_NAME_CAP) {
        return false;
    }
    for (size_t index = address_length + 1U; index < CBM_DAEMON_IPC_WINDOWS_NAME_CAP; index++) {
        if (encoded_address[index] != 0) {
            return false;
        }
    }
    memcpy(address_out, encoded_address, address_length + 1U);
    if (!windows_pipe_address_valid(address_out)) {
        memset(address_out, 0, CBM_DAEMON_IPC_WINDOWS_NAME_CAP);
        return false;
    }
    memcpy(nonce_out, record + sizeof(magic), CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE);
    return true;
}

int cbm_daemon_ipc_wait_pending(const cbm_ipc_pending_ops_t *ops, uint32_t timeout_ms,
                                uint32_t *transferred_out) {
    if (!ops || !ops->wait || !ops->cancel || !ops->finish || !transferred_out) {
        return -1;
    }
    cbm_ipc_pending_wait_status_t wait_status = ops->wait(ops->context, timeout_ms);
    if (wait_status == CBM_IPC_PENDING_WAIT_SIGNALED) {
        return ops->finish(ops->context, false, transferred_out) == CBM_IPC_PENDING_FINISH_COMPLETED
                   ? 1
                   : -1;
    }

    /* Cancellation is asynchronous.  Keep the platform operation alive and
     * drain it to a terminal state before returning, even when waiting itself
     * failed. */
    ops->cancel(ops->context);
    uint32_t transferred = 0;
    cbm_ipc_pending_finish_status_t finish_status = ops->finish(ops->context, true, &transferred);
    if (wait_status != CBM_IPC_PENDING_WAIT_TIMEOUT) {
        return -1;
    }
    if (finish_status == CBM_IPC_PENDING_FINISH_COMPLETED) {
        *transferred_out = transferred;
        return 1;
    }
    return finish_status == CBM_IPC_PENDING_FINISH_CANCELLED ? 0 : -1;
}

#ifndef _WIN32

#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stddef.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <time.h>
#include <unistd.h>

#ifndef O_CLOEXEC
#define O_CLOEXEC 0
#endif
#ifndef O_DIRECTORY
#define O_DIRECTORY 0
#endif
#ifndef O_NOFOLLOW
#define O_NOFOLLOW 0
#endif
#ifndef AT_SYMLINK_NOFOLLOW
#define AT_SYMLINK_NOFOLLOW 0
#endif

enum {
    POSIX_SOCKET_RECORD_MAGIC_SIZE = 8,
    POSIX_SOCKET_IDENTITY_DEVICE_OFFSET = POSIX_SOCKET_RECORD_MAGIC_SIZE,
    POSIX_SOCKET_IDENTITY_INODE_OFFSET = POSIX_SOCKET_IDENTITY_DEVICE_OFFSET + 8,
    POSIX_SOCKET_IDENTITY_CTIME_SECONDS_OFFSET = POSIX_SOCKET_IDENTITY_INODE_OFFSET + 8,
    POSIX_SOCKET_IDENTITY_CTIME_NANOSECONDS_OFFSET = POSIX_SOCKET_IDENTITY_CTIME_SECONDS_OFFSET + 8,
    POSIX_SOCKET_IDENTITY_RESERVED_OFFSET = POSIX_SOCKET_IDENTITY_CTIME_NANOSECONDS_OFFSET + 4,
    POSIX_SOCKET_ANCHOR_NAME_OFFSET = POSIX_SOCKET_IDENTITY_RESERVED_OFFSET + 4,
    POSIX_SOCKET_ANCHOR_NAME_CAP = 64,
    POSIX_SOCKET_RECORD_SIZE = POSIX_SOCKET_ANCHOR_NAME_OFFSET + POSIX_SOCKET_ANCHOR_NAME_CAP,
};

static const uint8_t POSIX_SOCKET_MARKER_MAGIC[POSIX_SOCKET_RECORD_MAGIC_SIZE] = {
    'C', 'B', 'M', 'S', 'O', 'C', 2, 0,
};

static const uint8_t POSIX_SOCKET_PENDING_MAGIC[POSIX_SOCKET_RECORD_MAGIC_SIZE] = {
    'C', 'B', 'M', 'P', 'E', 'N', 1, 0,
};

typedef struct {
    dev_t device;
    ino_t inode;
    int64_t ctime_seconds;
    uint32_t ctime_nanoseconds;
} posix_socket_identity_t;

typedef struct {
    posix_socket_identity_t identity;
    char anchor_name[POSIX_SOCKET_ANCHOR_NAME_CAP];
} posix_socket_record_t;

static void posix_publication_stage_reached(cbm_daemon_ipc_posix_publication_stage_t stage) {
    cbm_daemon_ipc_posix_publication_hook_fn hook = g_posix_publication_hook_for_test;
    if (hook) {
        hook(stage, g_posix_publication_hook_context_for_test);
    }
}

static void posix_record_publication_stage_reached(
    const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE], bool linked) {
    cbm_daemon_ipc_posix_publication_stage_t stage;
    if (memcmp(magic, POSIX_SOCKET_PENDING_MAGIC, POSIX_SOCKET_RECORD_MAGIC_SIZE) == 0) {
        stage = linked ? CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_RECORD_LINKED
                       : CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_TEMP_SYNCED;
    } else if (memcmp(magic, POSIX_SOCKET_MARKER_MAGIC, POSIX_SOCKET_RECORD_MAGIC_SIZE) == 0) {
        stage = linked ? CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_RECORD_LINKED
                       : CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_TEMP_SYNCED;
    } else {
        return;
    }
    posix_publication_stage_reached(stage);
}

struct cbm_daemon_ipc_endpoint {
    char *runtime_dir;
    char *address;
    char *socket_name;
    char *socket_anchor_address;
    char *socket_anchor_name;
    char *socket_identity_name;
    char *socket_pending_name;
    char *lock_name;
    char *startup_v2_lock_name;
    char *lifetime_lock_name;
    int dir_fd;
    dev_t dir_device;
    ino_t dir_inode;
};

struct cbm_daemon_ipc_lifetime_reservation {
    int fd;
    struct process_lock_entry *process_entry;
    pid_t owner_pid;
};

struct cbm_daemon_ipc_listener {
    int fd;
    int dir_fd;
    char *runtime_dir;
    dev_t dir_device;
    ino_t dir_inode;
    char *address;
    char *socket_name;
    char *socket_anchor_name;
    char *socket_identity_name;
    char *socket_pending_name;
    posix_socket_identity_t socket_identity;
    dev_t identity_device;
    ino_t identity_inode;
    pid_t owner_pid;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime_reservation;
    cbm_daemon_ipc_participant_guard_t *participant_guard;
};

struct cbm_daemon_ipc_connection {
    int fd;
    atomic_bool poisoned;
};

typedef struct process_lock_entry {
    dev_t directory_device;
    ino_t directory_inode;
    char *lock_name;
    bool shared;
    struct process_lock_entry *next;
} process_lock_entry_t;

struct cbm_daemon_ipc_startup_lock {
    int startup_v2_fd;
    process_lock_entry_t *startup_v2_process_entry;
    int legacy_fd;
    process_lock_entry_t *legacy_process_entry;
    cbm_daemon_ipc_endpoint_t *endpoint_snapshot;
    pid_t owner_pid;
    bool prepared;
};

struct cbm_daemon_ipc_local_transition {
    int startup_v2_fd;
    process_lock_entry_t *startup_v2_process_entry;
    int legacy_fd;
    process_lock_entry_t *legacy_process_entry;
    const cbm_daemon_ipc_endpoint_t *endpoint;
    pid_t owner_pid;
    bool sealed;
    bool work_begun;
};

struct cbm_daemon_ipc_participant_guard {
    int legacy_fd;
    process_lock_entry_t *legacy_process_entry;
    pid_t owner_pid;
};

static pthread_mutex_t process_locks_mutex = PTHREAD_MUTEX_INITIALIZER;
static process_lock_entry_t *process_locks;

static bool posix_socket_status_secure_transport(const struct stat *status);

static int process_lock_claim_mode(const cbm_daemon_ipc_endpoint_t *endpoint, const char *lock_name,
                                   bool shared, process_lock_entry_t **entry_out) {
    *entry_out = NULL;
    if (!endpoint || !lock_name || pthread_mutex_lock(&process_locks_mutex) != 0) {
        return -1;
    }
    for (process_lock_entry_t *entry = process_locks; entry; entry = entry->next) {
        if (entry->directory_device == endpoint->dir_device &&
            entry->directory_inode == endpoint->dir_inode &&
            strcmp(entry->lock_name, lock_name) == 0 && (!shared || !entry->shared)) {
            (void)pthread_mutex_unlock(&process_locks_mutex);
            return 0;
        }
    }
    process_lock_entry_t *entry = calloc(1, sizeof(*entry));
    if (entry) {
        entry->lock_name = string_copy(lock_name);
    }
    if (!entry || !entry->lock_name) {
        free(entry);
        (void)pthread_mutex_unlock(&process_locks_mutex);
        return -1;
    }
    entry->directory_device = endpoint->dir_device;
    entry->directory_inode = endpoint->dir_inode;
    entry->shared = shared;
    entry->next = process_locks;
    process_locks = entry;
    *entry_out = entry;
    (void)pthread_mutex_unlock(&process_locks_mutex);
    return 1;
}

static int process_lock_claim(const cbm_daemon_ipc_endpoint_t *endpoint, const char *lock_name,
                              process_lock_entry_t **entry_out) {
    return process_lock_claim_mode(endpoint, lock_name, false, entry_out);
}

static void process_lock_unclaim(process_lock_entry_t *claimed) {
    if (!claimed || pthread_mutex_lock(&process_locks_mutex) != 0) {
        return;
    }
    process_lock_entry_t **cursor = &process_locks;
    while (*cursor && *cursor != claimed) {
        cursor = &(*cursor)->next;
    }
    if (*cursor) {
        *cursor = claimed->next;
        free(claimed->lock_name);
        free(claimed);
    }
    (void)pthread_mutex_unlock(&process_locks_mutex);
}

static int process_lock_is_claimed(const cbm_daemon_ipc_endpoint_t *endpoint,
                                   const char *lock_name) {
    if (!endpoint || !lock_name || pthread_mutex_lock(&process_locks_mutex) != 0) {
        return -1;
    }
    int claimed = 0;
    for (process_lock_entry_t *entry = process_locks; entry; entry = entry->next) {
        if (entry->directory_device == endpoint->dir_device &&
            entry->directory_inode == endpoint->dir_inode &&
            strcmp(entry->lock_name, lock_name) == 0) {
            claimed = 1;
            break;
        }
    }
    (void)pthread_mutex_unlock(&process_locks_mutex);
    return claimed;
}

static uint64_t ipc_now_ms(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000U + (uint64_t)now.tv_nsec / 1000000U;
}

static uint64_t ipc_deadline_after(uint32_t timeout_ms) {
    if (timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER) {
        return UINT64_MAX;
    }
    return ipc_now_ms() + (uint64_t)timeout_ms;
}

static _Noreturn void ipc_coordination_cleanup_fail_stop(const char *component) {
    cbm_log_error("daemon.forced_shutdown", "component", component, "action",
                  "coordination_cleanup");
    (void)fflush(stdout);
    (void)fflush(stderr);
    _exit(EXIT_FAILURE);
}

static void ipc_startup_lock_release_complete(cbm_daemon_ipc_startup_lock_t **lock_io) {
    uint64_t deadline = ipc_deadline_after(CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS);
    while (lock_io && *lock_io) {
        (void)cbm_daemon_ipc_startup_lock_release(lock_io);
        if (!*lock_io) {
            return;
        }
        if (ipc_now_ms() >= deadline) {
            ipc_coordination_cleanup_fail_stop("startup_lock_cleanup");
        }
        cbm_usleep(1000);
    }
}

static void ipc_participant_guard_release_complete(cbm_daemon_ipc_participant_guard_t **guard_io) {
    uint64_t deadline = ipc_deadline_after(CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS);
    while (guard_io && *guard_io) {
        (void)cbm_daemon_ipc_participant_guard_release(guard_io);
        if (!*guard_io) {
            return;
        }
        if (ipc_now_ms() >= deadline) {
            ipc_coordination_cleanup_fail_stop("participant_guard_cleanup");
        }
        cbm_usleep(1000);
    }
}

static int deadline_remaining_ms(uint64_t deadline_ms) {
    uint64_t now_ms = ipc_now_ms();
    if (now_ms >= deadline_ms) {
        return 0;
    }
    uint64_t remaining = deadline_ms - now_ms;
    return remaining > (uint64_t)INT_MAX ? INT_MAX : (int)remaining;
}

static bool ipc_retry_pause(uint64_t deadline_ms) {
    int remaining_ms = deadline_remaining_ms(deadline_ms);
    if (remaining_ms <= 0) {
        return false;
    }
    int pause_ms = remaining_ms < CBM_DAEMON_IPC_RETRY_INTERVAL_MS
                       ? remaining_ms
                       : CBM_DAEMON_IPC_RETRY_INTERVAL_MS;
    struct timespec pause = {
        .tv_sec = 0,
        .tv_nsec = (long)pause_ms * 1000000L,
    };
    while (nanosleep(&pause, &pause) != 0 && errno == EINTR) {}
    return true;
}

static bool fd_set_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD);
    return flags >= 0 && fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
}

static bool fd_set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL);
    return flags >= 0 && fcntl(fd, F_SETFL, flags | O_NONBLOCK) == 0;
}

static bool socket_disable_sigpipe(int fd) {
#ifdef SO_NOSIGPIPE
    int enabled = 1;
    return setsockopt(fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled)) == 0;
#else
    (void)fd;
    return true;
#endif
}

static int local_socket_new(void) {
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    if (!fd_set_cloexec(fd) || !fd_set_nonblocking(fd) || !socket_disable_sigpipe(fd)) {
        (void)close(fd);
        return -1;
    }
    return fd;
}

/* Keep the only raw connect call at a sockaddr_un-typed boundary so the
 * security audit can count and review the complete local IPC surface. */
static int local_socket_connect(int fd, const struct sockaddr_un *address,
                                socklen_t address_length) {
    return connect(fd, (const struct sockaddr *)address, address_length);
}

static bool unix_address_set(struct sockaddr_un *address, const char *path, socklen_t *length_out) {
    if (!address || !path || !length_out) {
        return false;
    }
    size_t path_length = strlen(path);
    if (path_length == 0 || path_length >= sizeof(address->sun_path)) {
        return false;
    }
    memset(address, 0, sizeof(*address));
    address->sun_family = AF_UNIX;
    memcpy(address->sun_path, path, path_length + 1);
    socklen_t address_length =
        (socklen_t)(offsetof(struct sockaddr_un, sun_path) + path_length + 1);
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    address->sun_len = (uint8_t)address_length;
#endif
    *length_out = address_length;
    return true;
}

static bool runtime_parent_valid(const char *path) {
    struct stat status;
    return path && path[0] != '\0' && lstat(path, &status) == 0 && S_ISDIR(status.st_mode);
}

static bool private_runtime_snapshot(int fd, const char *path, bool require_empty_acl,
                                     struct stat *status_out) {
    struct stat by_handle;
    struct stat by_path;
    bool valid = fd >= 0 && path && path[0] != '\0' && fstat(fd, &by_handle) == 0 &&
                 lstat(path, &by_path) == 0 && S_ISDIR(by_handle.st_mode) &&
                 S_ISDIR(by_path.st_mode) && by_handle.st_uid == geteuid() &&
                 by_path.st_uid == geteuid() && (by_handle.st_mode & 07777) == 0700 &&
                 (by_path.st_mode & 07777) == 0700 && by_handle.st_dev == by_path.st_dev &&
                 by_handle.st_ino == by_path.st_ino &&
                 (!require_empty_acl || cbm_macos_extended_acl_fd_is_empty(fd));
    if (valid && status_out) {
        *status_out = by_handle;
    }
    return valid;
}

static int private_directory_tree_open(const char *directory_path);

static bool private_runtime_open(const char *path, int *fd_out, dev_t *device_out,
                                 ino_t *inode_out) {
    if (!path || !fd_out || !device_out || !inode_out) {
        return false;
    }

    int fd = private_directory_tree_open(path);
    if (fd < 0) {
        return false;
    }

    struct stat before;
    bool secured = private_runtime_snapshot(fd, path, true, &before);
    if (!secured) {
        (void)close(fd);
        return false;
    }
    *fd_out = fd;
    *device_out = before.st_dev;
    *inode_out = before.st_ino;
    return true;
}

static bool endpoint_runtime_still_valid(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint || endpoint->dir_fd < 0) {
        return false;
    }
    struct stat current;
    return private_runtime_snapshot(endpoint->dir_fd, endpoint->runtime_dir, true, &current) &&
           current.st_dev == endpoint->dir_device && current.st_ino == endpoint->dir_inode;
}

static bool private_regular_file_snapshot(int directory_fd, const char *base_name, int fd,
                                          nlink_t expected_links, struct stat *status_out) {
    struct stat by_handle;
    struct stat by_path;
    bool valid = directory_fd >= 0 && base_name && base_name[0] != '\0' && fd >= 0 &&
                 fstat(fd, &by_handle) == 0 &&
                 fstatat(directory_fd, base_name, &by_path, AT_SYMLINK_NOFOLLOW) == 0 &&
                 S_ISREG(by_handle.st_mode) && S_ISREG(by_path.st_mode) &&
                 by_handle.st_uid == geteuid() && by_path.st_uid == geteuid() &&
                 by_handle.st_nlink == expected_links && by_path.st_nlink == expected_links &&
                 (by_handle.st_mode & 07777) == 0600 && (by_path.st_mode & 07777) == 0600 &&
                 by_handle.st_dev == by_path.st_dev && by_handle.st_ino == by_path.st_ino &&
                 cbm_macos_extended_acl_fd_is_empty(directory_fd) &&
                 cbm_macos_extended_acl_fd_is_empty(fd);
    if (valid && status_out) {
        *status_out = by_handle;
    }
    return valid;
}

static bool private_regular_file_at_is_safe(int directory_fd, const char *base_name,
                                            nlink_t expected_links) {
    int fd = openat(directory_fd, base_name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    bool safe = fd >= 0 && fd_set_cloexec(fd) &&
                private_regular_file_snapshot(directory_fd, base_name, fd, expected_links, NULL);
    if (fd >= 0 && close(fd) != 0) {
        safe = false;
    }
    return safe;
}

static int poll_until(int fd, short events, uint64_t deadline_ms) {
    for (;;) {
        struct pollfd descriptor = {.fd = fd, .events = events, .revents = 0};
        int timeout_ms = deadline_ms == UINT64_MAX ? -1 : deadline_remaining_ms(deadline_ms);
        int result = poll(&descriptor, 1, timeout_ms);
        if (result > 0) {
            if ((descriptor.revents & events) != 0) {
                return 1;
            }
            return -1;
        }
        if (result == 0) {
            return 0;
        }
        if (errno != EINTR) {
            return -1;
        }
    }
}

static bool socket_peer_is_current_user(int fd) {
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    uid_t peer_uid = (uid_t)-1;
    gid_t peer_gid = (gid_t)-1;
    return getpeereid(fd, &peer_uid, &peer_gid) == 0 && peer_uid == geteuid();
#elif defined(__linux__)
    struct ucred credentials;
    socklen_t length = sizeof(credentials);
    return getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &credentials, &length) == 0 &&
           length == sizeof(credentials) && credentials.uid == geteuid();
#else
    (void)fd;
    return false;
#endif
}

static int connection_read_full(cbm_daemon_ipc_connection_t *connection, void *buffer,
                                size_t length, uint64_t deadline_ms) {
    if (!connection || atomic_load_explicit(&connection->poisoned, memory_order_acquire)) {
        return -1;
    }
    size_t offset = 0;
    while (offset < length) {
        int ready = poll_until(connection->fd, POLLIN, deadline_ms);
        if (ready != 1) {
            /* Framing is fail-stop across platforms.  Even at offset zero a
             * deadline may race transport progress (notably OVERLAPPED I/O on
             * Windows), so callers must reconnect instead of reusing a stream
             * whose next byte boundary is uncertain. */
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return ready;
        }
        ssize_t received = recv(connection->fd, (uint8_t *)buffer + offset, length - offset, 0);
        if (received > 0) {
            offset += (size_t)received;
            continue;
        }
        if (received == 0) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return -1;
        }
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return -1;
        }
    }
    return 1;
}

static int connection_write_full(cbm_daemon_ipc_connection_t *connection, const void *buffer,
                                 size_t length, uint64_t deadline_ms) {
    if (!connection || atomic_load_explicit(&connection->poisoned, memory_order_acquire)) {
        return -1;
    }
    size_t offset = 0;
    while (offset < length) {
        int ready = poll_until(connection->fd, POLLOUT, deadline_ms);
        if (ready != 1) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return ready;
        }
#ifdef MSG_NOSIGNAL
        ssize_t sent =
            send(connection->fd, (const uint8_t *)buffer + offset, length - offset, MSG_NOSIGNAL);
#else
        ssize_t sent = send(connection->fd, (const uint8_t *)buffer + offset, length - offset, 0);
#endif
        if (sent > 0) {
            offset += (size_t)sent;
            continue;
        }
        if (sent == 0) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return -1;
        }
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return -1;
        }
    }
    return 1;
}

cbm_daemon_ipc_endpoint_t *cbm_daemon_ipc_endpoint_new(const char *instance_key,
                                                       const char *runtime_parent) {
    if (!instance_key_valid(instance_key)) {
        return NULL;
    }
#ifdef __APPLE__
    const char *default_parent = "/private/tmp"; /* /tmp itself is a symlink on macOS. */
#else
    const char *default_parent = "/tmp";
#endif
    const char *requested_parent = runtime_parent ? runtime_parent : default_parent;
    char canonical_parent[CBM_DAEMON_IPC_PATH_CAP];
    if (!cbm_canonical_path(requested_parent, canonical_parent, sizeof(canonical_parent))) {
        return NULL;
    }
    const char *parent = canonical_parent;
    if (!runtime_parent_valid(parent)) {
        return NULL;
    }

    cbm_daemon_ipc_endpoint_t *endpoint = calloc(1, sizeof(*endpoint));
    if (!endpoint) {
        return NULL;
    }
    endpoint->dir_fd = -1;
    endpoint->runtime_dir =
        string_format("%s%s%s%lu", parent, parent[strlen(parent) - 1] == '/' ? "" : "/",
                      "cbm-daemon-", (unsigned long)geteuid());
    endpoint->socket_name = string_format("cbm-%s.sock", instance_key);
    endpoint->socket_anchor_name = string_format("cbm-%s.anc", instance_key);
    endpoint->socket_identity_name = string_format("cbm-%s.sock.identity", instance_key);
    endpoint->socket_pending_name = string_format("cbm-%s.sock.pending", instance_key);
    endpoint->lock_name = string_format("cbm-%s.lock", instance_key);
    endpoint->startup_v2_lock_name = string_format("cbm-%s.startup-v2.lock", instance_key);
    endpoint->lifetime_lock_name = string_format("cbm-%s.lifetime.lock", instance_key);
    if (!endpoint->runtime_dir || !endpoint->socket_name || !endpoint->socket_anchor_name ||
        !endpoint->socket_identity_name || !endpoint->socket_pending_name || !endpoint->lock_name ||
        !endpoint->startup_v2_lock_name || !endpoint->lifetime_lock_name ||
        !private_runtime_open(endpoint->runtime_dir, &endpoint->dir_fd, &endpoint->dir_device,
                              &endpoint->dir_inode)) {
        cbm_daemon_ipc_endpoint_free(endpoint);
        return NULL;
    }
    endpoint->address = string_format("%s/%s", endpoint->runtime_dir, endpoint->socket_name);
    endpoint->socket_anchor_address =
        string_format("%s/%s", endpoint->runtime_dir, endpoint->socket_anchor_name);
    struct sockaddr_un address;
    socklen_t address_length;
    if (!endpoint->address || !endpoint->socket_anchor_address ||
        strlen(endpoint->socket_anchor_name) >= POSIX_SOCKET_ANCHOR_NAME_CAP ||
        !unix_address_set(&address, endpoint->address, &address_length) ||
        !unix_address_set(&address, endpoint->socket_anchor_address, &address_length)) {
        cbm_daemon_ipc_endpoint_free(endpoint);
        return NULL;
    }
    return endpoint;
}

void cbm_daemon_ipc_endpoint_free(cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint) {
        return;
    }
    if (endpoint->dir_fd >= 0) {
        (void)close(endpoint->dir_fd);
    }
    free(endpoint->runtime_dir);
    free(endpoint->address);
    free(endpoint->socket_name);
    free(endpoint->socket_anchor_address);
    free(endpoint->socket_anchor_name);
    free(endpoint->socket_identity_name);
    free(endpoint->socket_pending_name);
    free(endpoint->lock_name);
    free(endpoint->startup_v2_lock_name);
    free(endpoint->lifetime_lock_name);
    free(endpoint);
}

static cbm_daemon_ipc_endpoint_t *endpoint_snapshot_new(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint_runtime_still_valid(endpoint)) {
        return NULL;
    }
    cbm_daemon_ipc_endpoint_t *snapshot = calloc(1, sizeof(*snapshot));
    if (!snapshot) {
        return NULL;
    }
    snapshot->dir_fd = -1;
    snapshot->runtime_dir = string_copy(endpoint->runtime_dir);
    snapshot->address = string_copy(endpoint->address);
    snapshot->socket_name = string_copy(endpoint->socket_name);
    snapshot->socket_anchor_address = string_copy(endpoint->socket_anchor_address);
    snapshot->socket_anchor_name = string_copy(endpoint->socket_anchor_name);
    snapshot->socket_identity_name = string_copy(endpoint->socket_identity_name);
    snapshot->socket_pending_name = string_copy(endpoint->socket_pending_name);
    snapshot->lock_name = string_copy(endpoint->lock_name);
    snapshot->startup_v2_lock_name = string_copy(endpoint->startup_v2_lock_name);
    snapshot->lifetime_lock_name = string_copy(endpoint->lifetime_lock_name);
    snapshot->dir_fd = dup(endpoint->dir_fd);
    snapshot->dir_device = endpoint->dir_device;
    snapshot->dir_inode = endpoint->dir_inode;
    if (!snapshot->runtime_dir || !snapshot->address || !snapshot->socket_name ||
        !snapshot->socket_anchor_address || !snapshot->socket_anchor_name ||
        !snapshot->socket_identity_name || !snapshot->socket_pending_name || !snapshot->lock_name ||
        !snapshot->startup_v2_lock_name || !snapshot->lifetime_lock_name || snapshot->dir_fd < 0 ||
        !fd_set_cloexec(snapshot->dir_fd) || !endpoint_runtime_still_valid(snapshot)) {
        cbm_daemon_ipc_endpoint_free(snapshot);
        return NULL;
    }
    return snapshot;
}

const char *cbm_daemon_ipc_endpoint_address(const cbm_daemon_ipc_endpoint_t *endpoint) {
    return endpoint ? endpoint->address : NULL;
}

const char *cbm_daemon_ipc_endpoint_runtime_dir(const cbm_daemon_ipc_endpoint_t *endpoint) {
    return endpoint ? endpoint->runtime_dir : NULL;
}

cbm_private_file_lock_status_t cbm_daemon_ipc_private_lock_directory_new(
    const cbm_daemon_ipc_endpoint_t *endpoint, cbm_private_lock_directory_t **directory_out) {
    if (directory_out) {
        *directory_out = NULL;
    }
    if (!directory_out || !endpoint_runtime_still_valid(endpoint)) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
#ifdef F_DUPFD_CLOEXEC
    int duplicate = fcntl(endpoint->dir_fd, F_DUPFD_CLOEXEC, 0);
#else
    int duplicate = fcntl(endpoint->dir_fd, F_DUPFD, 0);
    if (duplicate >= 0 && !fd_set_cloexec(duplicate)) {
        (void)close(duplicate);
        duplicate = -1;
    }
#endif
    if (duplicate < 0) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    cbm_private_file_lock_status_t status =
        cbm_private_lock_directory_adopt_posix(duplicate, endpoint->runtime_dir, directory_out);
    if (status != CBM_PRIVATE_FILE_LOCK_OK) {
        (void)close(duplicate);
    }
    return status;
}

static void posix_named_lock_release(int fd, process_lock_entry_t *process_entry) {
    if (fd >= 0) {
        (void)flock(fd, LOCK_UN);
        (void)close(fd);
    }
    process_lock_unclaim(process_entry);
}

static int posix_named_lock_try_acquire_mode(const cbm_daemon_ipc_endpoint_t *endpoint,
                                             const char *lock_name, bool shared, int *fd_out,
                                             process_lock_entry_t **process_entry_out) {
    if (fd_out) {
        *fd_out = -1;
    }
    if (process_entry_out) {
        *process_entry_out = NULL;
    }
    if (!fd_out || !process_entry_out || !lock_name || !endpoint_runtime_still_valid(endpoint)) {
        return -1;
    }

    process_lock_entry_t *process_entry = NULL;
    int process_result = process_lock_claim_mode(endpoint, lock_name, shared, &process_entry);
    if (process_result != 1) {
        return process_result == 0 &&
                       private_regular_file_at_is_safe(endpoint->dir_fd, lock_name, 1) &&
                       endpoint_runtime_still_valid(endpoint)
                   ? 0
                   : -1;
    }
    int fd = openat(endpoint->dir_fd, lock_name, O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0 || !fd_set_cloexec(fd)) {
        if (fd >= 0) {
            (void)close(fd);
        }
        process_lock_unclaim(process_entry);
        return -1;
    }
    if (fchmod(fd, 0600) != 0 ||
        !private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) ||
        !endpoint_runtime_still_valid(endpoint)) {
        (void)close(fd);
        process_lock_unclaim(process_entry);
        return -1;
    }
    if (flock(fd, (shared ? LOCK_SH : LOCK_EX) | LOCK_NB) != 0) {
        int lock_error = errno;
        bool still_private =
            private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) &&
            endpoint_runtime_still_valid(endpoint);
        (void)close(fd);
        process_lock_unclaim(process_entry);
        return still_private && (lock_error == EWOULDBLOCK || lock_error == EAGAIN) ? 0 : -1;
    }
    if (!private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) ||
        !endpoint_runtime_still_valid(endpoint)) {
        posix_named_lock_release(fd, process_entry);
        return -1;
    }
    *fd_out = fd;
    *process_entry_out = process_entry;
    return 1;
}

static int posix_named_lock_try_acquire(const cbm_daemon_ipc_endpoint_t *endpoint,
                                        const char *lock_name, int *fd_out,
                                        process_lock_entry_t **process_entry_out) {
    return posix_named_lock_try_acquire_mode(endpoint, lock_name, false, fd_out, process_entry_out);
}

static int posix_named_shared_lock_try_acquire(const cbm_daemon_ipc_endpoint_t *endpoint,
                                               const char *lock_name, int *fd_out,
                                               process_lock_entry_t **process_entry_out) {
    return posix_named_lock_try_acquire_mode(endpoint, lock_name, true, fd_out, process_entry_out);
}

static int posix_record_lock_set(int fd, short lock_type) {
    struct flock record_lock = {
        .l_type = lock_type,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
    };
    int result;
    do {
        result = fcntl(fd, F_SETLK, &record_lock);
    } while (result != 0 && errno == EINTR);
    return result;
}

static int posix_lifetime_lock_probe(const cbm_daemon_ipc_endpoint_t *endpoint,
                                     const char *lock_name) {
    if (!lock_name || !endpoint_runtime_still_valid(endpoint)) {
        return -1;
    }
    int process_claimed = process_lock_is_claimed(endpoint, lock_name);
    if (process_claimed < 0) {
        return -1;
    }

    int fd = openat(endpoint->dir_fd, lock_name, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return errno == ENOENT && process_claimed == 0 ? 0 : -1;
    }
    if (!fd_set_cloexec(fd) ||
        !private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) ||
        !endpoint_runtime_still_valid(endpoint)) {
        (void)close(fd);
        return -1;
    }
    if (process_claimed == 1) {
        bool still_private = endpoint_runtime_still_valid(endpoint);
        return close(fd) == 0 && still_private ? 1 : -1;
    }
    struct flock record_lock = {
        .l_type = F_WRLCK,
        .l_whence = SEEK_SET,
        .l_start = 0,
        .l_len = 0,
    };
    int query_result;
    do {
        query_result = fcntl(fd, F_GETLK, &record_lock);
    } while (query_result != 0 && errno == EINTR);
    bool still_private = private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) &&
                         endpoint_runtime_still_valid(endpoint);
    (void)close(fd);
    if (query_result != 0 || !still_private) {
        return -1;
    }
    if (record_lock.l_type == F_UNLCK) {
        return 0;
    }
    return record_lock.l_type == F_RDLCK || record_lock.l_type == F_WRLCK ? 1 : -1;
}

static int posix_lifetime_lock_try_acquire(const cbm_daemon_ipc_endpoint_t *endpoint,
                                           const char *lock_name, int *fd_out,
                                           process_lock_entry_t **process_entry_out) {
    if (fd_out) {
        *fd_out = -1;
    }
    if (process_entry_out) {
        *process_entry_out = NULL;
    }
    if (!fd_out || !process_entry_out || !lock_name || !endpoint_runtime_still_valid(endpoint)) {
        return -1;
    }
    process_lock_entry_t *process_entry = NULL;
    int process_result = process_lock_claim(endpoint, lock_name, &process_entry);
    if (process_result != 1) {
        return process_result == 0 &&
                       private_regular_file_at_is_safe(endpoint->dir_fd, lock_name, 1) &&
                       endpoint_runtime_still_valid(endpoint)
                   ? 0
                   : -1;
    }
    int fd = openat(endpoint->dir_fd, lock_name, O_RDWR | O_CREAT | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (fd < 0 || !fd_set_cloexec(fd)) {
        if (fd >= 0) {
            (void)close(fd);
        }
        process_lock_unclaim(process_entry);
        return -1;
    }
    if (fchmod(fd, 0600) != 0 ||
        !private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) ||
        !endpoint_runtime_still_valid(endpoint)) {
        (void)close(fd);
        process_lock_unclaim(process_entry);
        return -1;
    }
    if (posix_record_lock_set(fd, F_WRLCK) != 0) {
        int lock_error = errno;
        bool still_private =
            private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) &&
            endpoint_runtime_still_valid(endpoint);
        (void)close(fd);
        process_lock_unclaim(process_entry);
        return still_private && (lock_error == EACCES || lock_error == EAGAIN) ? 0 : -1;
    }
    if (!private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL) ||
        !endpoint_runtime_still_valid(endpoint)) {
        (void)posix_record_lock_set(fd, F_UNLCK);
        (void)close(fd);
        process_lock_unclaim(process_entry);
        return -1;
    }
    *fd_out = fd;
    *process_entry_out = process_entry;
    return 1;
}

static void posix_lifetime_lock_release(int fd, process_lock_entry_t *process_entry) {
    if (fd >= 0) {
        (void)posix_record_lock_set(fd, F_UNLCK);
        (void)close(fd);
    }
    process_lock_unclaim(process_entry);
}

int cbm_daemon_ipc_lifetime_reservation_try_acquire(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    cbm_daemon_ipc_lifetime_reservation_t **reservation_out) {
    if (reservation_out) {
        *reservation_out = NULL;
    }
    if (!endpoint || !reservation_out) {
        return -1;
    }
    int fd = -1;
    process_lock_entry_t *process_entry = NULL;
    int result = posix_lifetime_lock_try_acquire(endpoint, endpoint->lifetime_lock_name, &fd,
                                                 &process_entry);
    if (result != 1) {
        return result;
    }
    cbm_daemon_ipc_lifetime_reservation_t *reservation = malloc(sizeof(*reservation));
    if (!reservation) {
        posix_lifetime_lock_release(fd, process_entry);
        return -1;
    }
    reservation->fd = fd;
    reservation->process_entry = process_entry;
    reservation->owner_pid = getpid();
    *reservation_out = reservation;
    return 1;
}

void cbm_daemon_ipc_lifetime_reservation_release(
    cbm_daemon_ipc_lifetime_reservation_t *reservation) {
    if (!reservation) {
        return;
    }
    posix_lifetime_lock_release(reservation->fd, reservation->process_entry);
    free(reservation);
}

static bool lifetime_reservation_matches_endpoint(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_ipc_lifetime_reservation_t *reservation) {
    const process_lock_entry_t *entry = reservation ? reservation->process_entry : NULL;
    if (!endpoint_runtime_still_valid(endpoint) || !reservation || reservation->fd < 0 ||
        reservation->owner_pid != getpid() || !entry || !endpoint->lifetime_lock_name ||
        entry->directory_device != endpoint->dir_device ||
        entry->directory_inode != endpoint->dir_inode || !entry->lock_name ||
        strcmp(entry->lock_name, endpoint->lifetime_lock_name) != 0) {
        return false;
    }
    if (!private_regular_file_snapshot(endpoint->dir_fd, endpoint->lifetime_lock_name,
                                       reservation->fd, 1, NULL) ||
        !endpoint_runtime_still_valid(endpoint)) {
        return false;
    }
    /* Reasserting the same process-owned record lock is a no-op for a valid
     * reservation and fails if another process has replaced ownership. */
    return posix_record_lock_set(reservation->fd, F_WRLCK) == 0;
}

int cbm_daemon_ipc_lifetime_reservation_probe(const cbm_daemon_ipc_endpoint_t *endpoint) {
    return endpoint ? posix_lifetime_lock_probe(endpoint, endpoint->lifetime_lock_name) : -1;
}

static int posix_startup_lock_probe(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint_runtime_still_valid(endpoint)) {
        return -1;
    }
    int process_claimed = process_lock_is_claimed(endpoint, endpoint->lock_name);
    if (process_claimed < 0) {
        return -1;
    }
    int fd = openat(endpoint->dir_fd, endpoint->lock_name, O_RDWR | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) {
        return errno == ENOENT && process_claimed == 0 ? 0 : -1;
    }
    if (!fd_set_cloexec(fd) ||
        !private_regular_file_snapshot(endpoint->dir_fd, endpoint->lock_name, fd, 1, NULL)) {
        (void)close(fd);
        return -1;
    }
    if (process_claimed == 1) {
        bool still_private = endpoint_runtime_still_valid(endpoint);
        return close(fd) == 0 && still_private ? 1 : -1;
    }
    int lock_result;
    do {
        lock_result = flock(fd, LOCK_SH | LOCK_NB);
    } while (lock_result != 0 && errno == EINTR);
    if (lock_result != 0) {
        int lock_error = errno;
        bool still_private =
            private_regular_file_snapshot(endpoint->dir_fd, endpoint->lock_name, fd, 1, NULL) &&
            endpoint_runtime_still_valid(endpoint);
        (void)close(fd);
        return still_private && (lock_error == EWOULDBLOCK || lock_error == EAGAIN) ? 1 : -1;
    }
    int unlock_result;
    do {
        unlock_result = flock(fd, LOCK_UN);
    } while (unlock_result != 0 && errno == EINTR);
    bool still_private =
        private_regular_file_snapshot(endpoint->dir_fd, endpoint->lock_name, fd, 1, NULL) &&
        endpoint_runtime_still_valid(endpoint);
    int close_result = close(fd);
    return unlock_result == 0 && still_private && close_result == 0 ? 0 : -1;
}

int cbm_daemon_ipc_legacy_generation_probe(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint_runtime_still_valid(endpoint)) {
        return -1;
    }
    struct stat status;
    if (fstatat(endpoint->dir_fd, endpoint->socket_name, &status, AT_SYMLINK_NOFOLLOW) == 0) {
        return posix_socket_status_secure_transport(&status) ? 1 : -1;
    }
    if (errno != ENOENT) {
        return -1;
    }
    return posix_startup_lock_probe(endpoint);
}

static bool private_log_base_name_valid(const char *base_name) {
    if (!base_name || !base_name[0] || strcmp(base_name, ".") == 0 ||
        strcmp(base_name, "..") == 0 || strchr(base_name, '/') || strchr(base_name, '\\')) {
        return false;
    }
    size_t length = strlen(base_name);
    return length <= NAME_MAX - 2;
}

static char *private_log_directory_path_copy(const char *directory_path) {
#ifdef __APPLE__
    /* Darwin exposes the trusted top-level aliases /tmp -> /private/tmp and
     * /var -> /private/var.  Resolve only those root-owned aliases before the
     * component-wise O_NOFOLLOW walk.  Canonicalizing the complete caller path
     * would follow an attacker-controlled cache/log symlink and is forbidden. */
    static const char *const aliases[] = {"/tmp", "/var"};
    for (size_t index = 0; index < sizeof(aliases) / sizeof(aliases[0]); index++) {
        const char *alias = aliases[index];
        size_t alias_length = strlen(alias);
        if (strncmp(directory_path, alias, alias_length) != 0 ||
            (directory_path[alias_length] != '\0' && directory_path[alias_length] != '/')) {
            continue;
        }
        struct stat alias_status;
        if (lstat(alias, &alias_status) != 0 || !S_ISLNK(alias_status.st_mode)) {
            break;
        }
        struct stat root_status;
        char resolved[CBM_DAEMON_IPC_PATH_CAP];
        if (alias_status.st_uid != 0 || lstat("/", &root_status) != 0 ||
            !S_ISDIR(root_status.st_mode) || root_status.st_uid != 0 ||
            (root_status.st_mode & 0022) != 0 || !realpath(alias, resolved)) {
            return NULL;
        }
        struct stat resolved_status;
        if (lstat(resolved, &resolved_status) != 0 || !S_ISDIR(resolved_status.st_mode) ||
            resolved_status.st_uid != 0) {
            return NULL;
        }
        return string_format("%s%s", resolved, directory_path + alias_length);
    }
#endif
    return string_copy(directory_path);
}

static bool posix_directory_owner_trusted(uid_t owner) {
    return owner == (uid_t)0 || owner == geteuid();
}

static bool posix_directory_parent_secure(int directory_fd) {
    struct stat status;
    if (directory_fd < 0 || fstat(directory_fd, &status) != 0 || !S_ISDIR(status.st_mode) ||
        !posix_directory_owner_trusted(status.st_uid) ||
        !cbm_macos_extended_acl_fd_is_deny_only(directory_fd)) {
        return false;
    }
    return (status.st_mode & 0022) == 0 ||
           (status.st_uid == (uid_t)0 && (status.st_mode & S_ISVTX) != 0);
}

/* Validate a path transition only through the two already-open directory
 * handles.  A group/other-writable parent is unsafe unless it is the standard
 * root-owned sticky-directory pattern (for example /tmp) and the selected
 * child is itself root/current-user owned.  Existing ancestors are observed,
 * never chmod'd or ACL-rewritten. */
static bool posix_directory_transition_secure(int parent_fd, int child_fd) {
    struct stat parent;
    struct stat child;
    if (parent_fd < 0 || child_fd < 0 || !posix_directory_parent_secure(parent_fd) ||
        fstat(parent_fd, &parent) != 0 || fstat(child_fd, &child) != 0 ||
        !S_ISDIR(parent.st_mode) || !S_ISDIR(child.st_mode) ||
        !posix_directory_owner_trusted(parent.st_uid) ||
        !posix_directory_owner_trusted(child.st_uid)) {
        return false;
    }
    return true;
}

static int private_directory_tree_open(const char *directory_path) {
    if (!directory_path || !directory_path[0] || O_DIRECTORY == 0 || O_NOFOLLOW == 0) {
        return -1;
    }
    char *path = private_log_directory_path_copy(directory_path);
    if (!path) {
        return -1;
    }
    bool absolute = path[0] == '/';
    int current_fd = open(absolute ? "/" : ".", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    bool ok = current_fd >= 0 && fd_set_cloexec(current_fd);
    char *cursor = path;
    while (ok && *cursor == '/') {
        cursor++;
    }
    bool visited = false;
    while (ok && *cursor) {
        char *component = cursor;
        while (*cursor && *cursor != '/') {
            cursor++;
        }
        char saved = *cursor;
        *cursor = '\0';
        if (strcmp(component, ".") == 0) {
            /* Relative paths may contain a harmless explicit current-dir
             * component. Parent traversal is never valid for private logs. */
        } else if (strcmp(component, "..") == 0 || !component[0]) {
            ok = false;
        } else {
            ok = posix_directory_parent_secure(current_fd);
            bool created = ok && mkdirat(current_fd, component, 0700) == 0;
            if (!created && errno != EEXIST) {
                ok = false;
            }
            int next_fd =
                ok ? openat(current_fd, component, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW)
                   : -1;
            struct stat status;
            ok = next_fd >= 0 && fd_set_cloexec(next_fd) && fstat(next_fd, &status) == 0 &&
                 S_ISDIR(status.st_mode) && posix_directory_transition_secure(current_fd, next_fd);
            if (ok && created) {
                ok = status.st_uid == geteuid() && fchmod(next_fd, 0700) == 0 &&
                     cbm_macos_extended_acl_fd_clear(next_fd) &&
                     cbm_macos_extended_acl_fd_is_empty(next_fd);
            }
            if (ok) {
                (void)close(current_fd);
                current_fd = next_fd;
                visited = true;
            } else if (next_fd >= 0) {
                (void)close(next_fd);
            }
        }
        *cursor = saved;
        while (*cursor == '/') {
            cursor++;
        }
    }
    struct stat final_status;
    if (ok && !visited) {
        ipc_validation_detail_set("%s: no path components resolved", directory_path);
        ok = false;
    }
    if (ok && (fstat(current_fd, &final_status) != 0 || !S_ISDIR(final_status.st_mode))) {
        ipc_validation_detail_set("%s: final stat failed or not a directory (errno %d)",
                                  directory_path, errno);
        ok = false;
    }
    if (ok && final_status.st_uid != geteuid()) {
        ipc_validation_detail_set("%s: owner uid %ld, expected euid %ld", directory_path,
                                  (long)final_status.st_uid, (long)geteuid());
        ok = false;
    }
    if (ok && fchmod(current_fd, 0700) != 0) {
        ipc_validation_detail_set("%s: chmod 0700 failed (errno %d)", directory_path, errno);
        ok = false;
    }
    if (ok && !cbm_macos_extended_acl_fd_clear(current_fd)) {
        ipc_validation_detail_set("%s: extended ACL not clearable", directory_path);
        ok = false;
    }
    if (ok && (fstat(current_fd, &final_status) != 0 || (final_status.st_mode & 07777) != 0700)) {
        ipc_validation_detail_set("%s: mode 0%o survived chmod, expected 0700", directory_path,
                                  (unsigned)(final_status.st_mode & 07777));
        ok = false;
    }
    if (ok && !cbm_macos_extended_acl_fd_is_empty(current_fd)) {
        ipc_validation_detail_set("%s: extended ACL still present after clear", directory_path);
        ok = false;
    }
    free(path);
    if (!ok) {
        if (current_fd >= 0) {
            (void)close(current_fd);
        }
        return -1;
    }
    return current_fd;
}

bool cbm_daemon_ipc_private_directory_secure(const char *directory_path) {
    ipc_validation_detail_set("%s", "");
    int directory_fd = private_directory_tree_open(directory_path);
    if (directory_fd < 0) {
        if (!ipc_validation_detail_buffer[0]) {
            ipc_validation_detail_set("%s: ancestry component validation failed (errno %d)",
                                      directory_path, errno);
        }
        return false;
    }
    return close(directory_fd) == 0;
}

static int private_log_file_open(int directory_fd, const char *base_name, struct stat *status_out) {
    int flags = O_WRONLY | O_APPEND | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    int fd = openat(directory_fd, base_name, flags | O_CREAT | O_EXCL, 0600);
    if (fd < 0 && errno == EEXIST) {
        fd = openat(directory_fd, base_name, flags);
    }
    struct stat status;
    if (fd < 0 || !fd_set_cloexec(fd) || fchmod(fd, 0600) != 0 ||
        !private_regular_file_snapshot(directory_fd, base_name, fd, 1, &status) ||
        status.st_size < 0) {
        if (fd >= 0) {
            (void)close(fd);
        }
        return -1;
    }
    *status_out = status;
    return fd;
}

FILE *cbm_daemon_ipc_private_log_open(const char *directory_path, const char *base_name,
                                      size_t rotate_cap_bytes) {
    if (!private_log_base_name_valid(base_name) || rotate_cap_bytes == 0) {
        return NULL;
    }
    int directory_fd = private_directory_tree_open(directory_path);
    if (directory_fd < 0) {
        return NULL;
    }
    struct stat status;
    int fd = private_log_file_open(directory_fd, base_name, &status);
    bool ok = fd >= 0;
    if (ok && (uintmax_t)status.st_size > (uintmax_t)rotate_cap_bytes) {
        char rotated[NAME_MAX + 3];
        int written = snprintf(rotated, sizeof(rotated), "%s.1", base_name);
        struct stat destination;
        bool destination_ok = false;
        if (written > 0 && written < (int)sizeof(rotated)) {
            if (fstatat(directory_fd, rotated, &destination, AT_SYMLINK_NOFOLLOW) == 0) {
                destination_ok = S_ISREG(destination.st_mode) && destination.st_uid == geteuid() &&
                                 destination.st_nlink == 1;
            } else {
                destination_ok = errno == ENOENT;
            }
        }
        struct stat current;
        bool current_ok = fstatat(directory_fd, base_name, &current, AT_SYMLINK_NOFOLLOW) == 0 &&
                          S_ISREG(current.st_mode) && current.st_uid == geteuid() &&
                          current.st_nlink == 1 && current.st_dev == status.st_dev &&
                          current.st_ino == status.st_ino;
        ok = destination_ok && current_ok && close(fd) == 0;
        fd = -1;
        if (ok) {
            ok = renameat(directory_fd, base_name, directory_fd, rotated) == 0;
        }
        if (ok) {
            fd = private_log_file_open(directory_fd, base_name, &status);
            ok = fd >= 0;
        }
    }
    FILE *stream = NULL;
    if (ok) {
        stream = fdopen(fd, "ab");
        if (stream) {
            fd = -1;
        }
    }
    if (fd >= 0) {
        (void)close(fd);
    }
    (void)fsync(directory_fd);
    (void)close(directory_fd);
    return stream;
}

static void posix_u32_encode(uint8_t out[4], uint32_t value) {
    out[0] = (uint8_t)(value & 0xffU);
    out[1] = (uint8_t)((value >> 8U) & 0xffU);
    out[2] = (uint8_t)((value >> 16U) & 0xffU);
    out[3] = (uint8_t)((value >> 24U) & 0xffU);
}

static uint32_t posix_u32_decode(const uint8_t in[4]) {
    return (uint32_t)in[0] | ((uint32_t)in[1] << 8U) | ((uint32_t)in[2] << 16U) |
           ((uint32_t)in[3] << 24U);
}

static void posix_u64_encode(uint8_t out[8], uint64_t value) {
    for (size_t index = 0; index < 8; index++) {
        out[index] = (uint8_t)(value >> (index * 8U));
    }
}

static uint64_t posix_u64_decode(const uint8_t in[8]) {
    uint64_t value = 0;
    for (size_t index = 0; index < 8; index++) {
        value |= (uint64_t)in[index] << (index * 8U);
    }
    return value;
}

static bool posix_stat_ctime(const struct stat *status, int64_t *seconds_out,
                             uint32_t *nanoseconds_out) {
    if (!status || !seconds_out || !nanoseconds_out) {
        return false;
    }
#if defined(__APPLE__) || defined(__FreeBSD__) || defined(__OpenBSD__) || defined(__NetBSD__)
    time_t raw_seconds = status->st_ctimespec.tv_sec;
    long raw_nanoseconds = status->st_ctimespec.tv_nsec;
#else
    time_t raw_seconds = status->st_ctim.tv_sec;
    long raw_nanoseconds = status->st_ctim.tv_nsec;
#endif
    int64_t seconds = (int64_t)raw_seconds;
    if ((time_t)seconds != raw_seconds || seconds < 0 || raw_nanoseconds < 0 ||
        raw_nanoseconds >= 1000000000L) {
        return false;
    }
    *seconds_out = seconds;
    *nanoseconds_out = (uint32_t)raw_nanoseconds;
    return true;
}

static bool posix_socket_identity_from_stat(const struct stat *status,
                                            posix_socket_identity_t *identity_out) {
    if (!status || !identity_out || !S_ISSOCK(status->st_mode)) {
        return false;
    }
    uint64_t device = (uint64_t)status->st_dev;
    uint64_t inode = (uint64_t)status->st_ino;
    int64_t ctime_seconds = 0;
    uint32_t ctime_nanoseconds = 0;
    if ((dev_t)device != status->st_dev || (ino_t)inode != status->st_ino ||
        !posix_stat_ctime(status, &ctime_seconds, &ctime_nanoseconds)) {
        return false;
    }
    identity_out->device = status->st_dev;
    identity_out->inode = status->st_ino;
    identity_out->ctime_seconds = ctime_seconds;
    identity_out->ctime_nanoseconds = ctime_nanoseconds;
    return true;
}

static bool posix_socket_identity_equal(const posix_socket_identity_t *left,
                                        const posix_socket_identity_t *right) {
    return left && right && left->device == right->device && left->inode == right->inode &&
           left->ctime_seconds == right->ctime_seconds &&
           left->ctime_nanoseconds == right->ctime_nanoseconds;
}

static bool posix_socket_inode_equal(const posix_socket_identity_t *left,
                                     const posix_socket_identity_t *right) {
    return left && right && left->device == right->device && left->inode == right->inode;
}

static bool posix_socket_status_secure_links(const struct stat *status, nlink_t links) {
    return status && S_ISSOCK(status->st_mode) && status->st_uid == geteuid() &&
           status->st_nlink == links && (status->st_mode & 0777) == 0600;
}

static bool posix_socket_status_secure_transport(const struct stat *status) {
    return status && S_ISSOCK(status->st_mode) && status->st_uid == geteuid() &&
           (status->st_nlink == 1 || status->st_nlink == 2) && (status->st_mode & 0777) == 0600;
}

static bool posix_socket_record_encode(const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE],
                                       const posix_socket_record_t *source,
                                       uint8_t record[POSIX_SOCKET_RECORD_SIZE]) {
    if (!magic || !source || !record || source->identity.ctime_seconds < 0 ||
        source->identity.ctime_nanoseconds >= 1000000000U) {
        return false;
    }
    size_t anchor_length = strnlen(source->anchor_name, POSIX_SOCKET_ANCHOR_NAME_CAP);
    uint64_t device = (uint64_t)source->identity.device;
    uint64_t inode = (uint64_t)source->identity.inode;
    if ((dev_t)device != source->identity.device || (ino_t)inode != source->identity.inode ||
        anchor_length == 0 || anchor_length >= POSIX_SOCKET_ANCHOR_NAME_CAP ||
        strchr(source->anchor_name, '/')) {
        return false;
    }
    memset(record, 0, POSIX_SOCKET_RECORD_SIZE);
    memcpy(record, magic, POSIX_SOCKET_RECORD_MAGIC_SIZE);
    posix_u64_encode(record + POSIX_SOCKET_IDENTITY_DEVICE_OFFSET, device);
    posix_u64_encode(record + POSIX_SOCKET_IDENTITY_INODE_OFFSET, inode);
    posix_u64_encode(record + POSIX_SOCKET_IDENTITY_CTIME_SECONDS_OFFSET,
                     (uint64_t)source->identity.ctime_seconds);
    posix_u32_encode(record + POSIX_SOCKET_IDENTITY_CTIME_NANOSECONDS_OFFSET,
                     source->identity.ctime_nanoseconds);
    memcpy(record + POSIX_SOCKET_ANCHOR_NAME_OFFSET, source->anchor_name, anchor_length + 1U);
    return true;
}

static bool posix_socket_record_decode(const uint8_t record[POSIX_SOCKET_RECORD_SIZE],
                                       const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE],
                                       posix_socket_record_t *record_out) {
    if (!record || !magic || !record_out ||
        memcmp(record, magic, POSIX_SOCKET_RECORD_MAGIC_SIZE) != 0 ||
        posix_u32_decode(record + POSIX_SOCKET_IDENTITY_RESERVED_OFFSET) != 0) {
        return false;
    }
    uint64_t device = posix_u64_decode(record + POSIX_SOCKET_IDENTITY_DEVICE_OFFSET);
    uint64_t inode = posix_u64_decode(record + POSIX_SOCKET_IDENTITY_INODE_OFFSET);
    uint64_t seconds = posix_u64_decode(record + POSIX_SOCKET_IDENTITY_CTIME_SECONDS_OFFSET);
    uint32_t nanoseconds =
        posix_u32_decode(record + POSIX_SOCKET_IDENTITY_CTIME_NANOSECONDS_OFFSET);
    if ((dev_t)device != device || (ino_t)inode != inode || seconds > (uint64_t)INT64_MAX ||
        nanoseconds >= 1000000000U) {
        return false;
    }
    const uint8_t *anchor = record + POSIX_SOCKET_ANCHOR_NAME_OFFSET;
    const uint8_t *terminator = memchr(anchor, 0, POSIX_SOCKET_ANCHOR_NAME_CAP);
    if (!terminator || terminator == anchor || memchr(anchor, '/', (size_t)(terminator - anchor))) {
        return false;
    }
    size_t anchor_length = (size_t)(terminator - anchor);
    for (size_t index = anchor_length + 1U; index < POSIX_SOCKET_ANCHOR_NAME_CAP; index++) {
        if (anchor[index] != 0) {
            return false;
        }
    }
    memset(record_out, 0, sizeof(*record_out));
    record_out->identity.device = (dev_t)device;
    record_out->identity.inode = (ino_t)inode;
    record_out->identity.ctime_seconds = (int64_t)seconds;
    record_out->identity.ctime_nanoseconds = nanoseconds;
    memcpy(record_out->anchor_name, anchor, anchor_length + 1U);
    return true;
}

static bool posix_fd_write_all(int fd, const uint8_t *buffer, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t written = write(fd, buffer + offset, length - offset);
        if (written > 0) {
            offset += (size_t)written;
        } else if (written < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

static bool posix_fd_pread_all(int fd, uint8_t *buffer, size_t length) {
    size_t offset = 0;
    while (offset < length) {
        ssize_t count = pread(fd, buffer + offset, length - offset, (off_t)offset);
        if (count > 0) {
            offset += (size_t)count;
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            return false;
        }
    }
    return true;
}

typedef enum {
    POSIX_RECORD_UNSAFE = -1,
    POSIX_RECORD_ABSENT = 0,
    POSIX_RECORD_VALID = 1,
    POSIX_RECORD_UNKNOWN = 2,
} posix_record_state_t;

static posix_record_state_t posix_socket_record_read_links(
    const cbm_daemon_ipc_endpoint_t *endpoint, const char *record_name,
    const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE], nlink_t expected_links,
    posix_socket_record_t *record_out, struct stat *record_status_out) {
    if (!endpoint || !record_name || !magic || !record_out || !record_status_out ||
        !endpoint->socket_anchor_name || (expected_links != 1 && expected_links != 2) ||
        !endpoint_runtime_still_valid(endpoint)) {
        return POSIX_RECORD_UNSAFE;
    }
    int fd = openat(endpoint->dir_fd, record_name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    if (fd < 0) {
        return errno == ENOENT ? POSIX_RECORD_ABSENT : POSIX_RECORD_UNSAFE;
    }
    struct stat before;
    struct stat after;
    struct stat by_path;
    int64_t before_ctime_seconds = 0;
    uint32_t before_ctime_nanoseconds = 0;
    int64_t after_ctime_seconds = 0;
    uint32_t after_ctime_nanoseconds = 0;
    bool metadata_safe =
        fd_set_cloexec(fd) && fstat(fd, &before) == 0 && S_ISREG(before.st_mode) &&
        before.st_uid == geteuid() && before.st_nlink == expected_links &&
        (before.st_mode & 07777) == 0600 && cbm_macos_extended_acl_fd_is_empty(fd) &&
        posix_stat_ctime(&before, &before_ctime_seconds, &before_ctime_nanoseconds);
    if (!metadata_safe) {
        (void)close(fd);
        return POSIX_RECORD_UNSAFE;
    }
    if (before.st_size != (off_t)POSIX_SOCKET_RECORD_SIZE) {
        bool still_private = private_regular_file_snapshot(endpoint->dir_fd, record_name, fd,
                                                           expected_links, NULL) &&
                             endpoint_runtime_still_valid(endpoint);
        int close_result = close(fd);
        return still_private && close_result == 0 ? POSIX_RECORD_UNKNOWN : POSIX_RECORD_UNSAFE;
    }
    uint8_t record[POSIX_SOCKET_RECORD_SIZE];
    bool read_ok = posix_fd_pread_all(fd, record, sizeof(record));
    bool stable = read_ok && fstat(fd, &after) == 0 &&
                  posix_stat_ctime(&after, &after_ctime_seconds, &after_ctime_nanoseconds) &&
                  fstatat(endpoint->dir_fd, record_name, &by_path, AT_SYMLINK_NOFOLLOW) == 0 &&
                  S_ISREG(after.st_mode) && after.st_uid == geteuid() &&
                  after.st_nlink == expected_links && (after.st_mode & 07777) == 0600 &&
                  cbm_macos_extended_acl_fd_is_empty(fd) && after.st_size == before.st_size &&
                  after.st_dev == before.st_dev && after.st_ino == before.st_ino &&
                  after_ctime_seconds == before_ctime_seconds &&
                  after_ctime_nanoseconds == before_ctime_nanoseconds && S_ISREG(by_path.st_mode) &&
                  by_path.st_uid == geteuid() && by_path.st_nlink == expected_links &&
                  (by_path.st_mode & 07777) == 0600 && by_path.st_dev == before.st_dev &&
                  by_path.st_ino == before.st_ino && endpoint_runtime_still_valid(endpoint);
    int close_result = close(fd);
    if (!stable || close_result != 0) {
        return POSIX_RECORD_UNSAFE;
    }
    if (!posix_socket_record_decode(record, magic, record_out) ||
        strcmp(record_out->anchor_name, endpoint->socket_anchor_name) != 0) {
        return POSIX_RECORD_UNKNOWN;
    }
    *record_status_out = before;
    return POSIX_RECORD_VALID;
}

static posix_record_state_t posix_socket_record_read(
    const cbm_daemon_ipc_endpoint_t *endpoint, const char *record_name,
    const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE], posix_socket_record_t *record_out,
    struct stat *record_status_out) {
    return posix_socket_record_read_links(endpoint, record_name, magic, 1, record_out,
                                          record_status_out);
}

static int posix_socket_path_identity_read(const cbm_daemon_ipc_endpoint_t *endpoint,
                                           const char *socket_name,
                                           posix_socket_identity_t *identity_out,
                                           struct stat *status_out) {
    struct stat status;
    if (!endpoint || !socket_name || !identity_out) {
        return -1;
    }
    if (fstatat(endpoint->dir_fd, socket_name, &status, AT_SYMLINK_NOFOLLOW) != 0) {
        return errno == ENOENT ? 0 : -1;
    }
    if (!posix_socket_status_secure_transport(&status) ||
        !posix_socket_identity_from_stat(&status, identity_out)) {
        return -1;
    }
    if (status_out) {
        *status_out = status;
    }
    return 1;
}

static bool posix_path_unlink_regular_if_matches(int dir_fd, const char *name, dev_t device,
                                                 ino_t inode, nlink_t links) {
    if (dir_fd < 0 || !name) {
        return false;
    }
    int fd = openat(dir_fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    struct stat status;
    bool matches = fd >= 0 && fd_set_cloexec(fd) &&
                   private_regular_file_snapshot(dir_fd, name, fd, links, &status) &&
                   status.st_dev == device && status.st_ino == inode;
    if (fd >= 0 && close(fd) != 0) {
        matches = false;
    }
    return matches && unlinkat(dir_fd, name, 0) == 0;
}

static bool posix_socket_path_unlink_inode_if_matches(int dir_fd, const char *socket_name,
                                                      const posix_socket_identity_t *identity,
                                                      nlink_t links) {
    struct stat status;
    posix_socket_identity_t observed;
    return dir_fd >= 0 && socket_name && identity &&
           fstatat(dir_fd, socket_name, &status, AT_SYMLINK_NOFOLLOW) == 0 &&
           posix_socket_status_secure_links(&status, links) &&
           posix_socket_identity_from_stat(&status, &observed) &&
           posix_socket_inode_equal(&observed, identity) && unlinkat(dir_fd, socket_name, 0) == 0;
}

static bool posix_directory_sync(int dir_fd) {
    if (dir_fd < 0) {
        return false;
    }
    int result;
    do {
        result = fsync(dir_fd);
    } while (result != 0 && errno == EINTR);
    if (result == 0) {
        return true;
    }

    int sync_error = errno;
    bool unsupported = sync_error == EINVAL;
#ifdef ENOTSUP
    unsupported = unsupported || sync_error == ENOTSUP;
#endif
#ifdef EOPNOTSUPP
    unsupported = unsupported || sync_error == EOPNOTSUPP;
#endif
    /* Some POSIX filesystems, including Darwin filesystems, do not expose a
     * directory-sync operation. Record contents are still fsynced before
     * publication, and the recovery state machine accepts either atomic
     * namespace outcome after a crash. */
    return unsupported;
}

static bool posix_socket_record_temp_name(const char *record_name, char temp_name[NAME_MAX + 1]) {
    if (!record_name || !temp_name) {
        return false;
    }
    int written = snprintf(temp_name, NAME_MAX + 1, "%s.tmp", record_name);
    return written > 0 && written <= NAME_MAX;
}

static int posix_record_artifact_status(const cbm_daemon_ipc_endpoint_t *endpoint, const char *name,
                                        struct stat *status_out) {
    if (!endpoint || !name || !status_out || !endpoint_runtime_still_valid(endpoint)) {
        return -1;
    }
    struct stat status;
    if (fstatat(endpoint->dir_fd, name, &status, AT_SYMLINK_NOFOLLOW) != 0) {
        return errno == ENOENT ? 0 : -1;
    }
    if (!S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
        (status.st_mode & 07777) != 0600 || (status.st_nlink != 1 && status.st_nlink != 2)) {
        return -1;
    }
    int fd = openat(endpoint->dir_fd, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    struct stat by_handle;
    bool safe =
        fd >= 0 && fd_set_cloexec(fd) &&
        private_regular_file_snapshot(endpoint->dir_fd, name, fd, status.st_nlink, &by_handle) &&
        by_handle.st_dev == status.st_dev && by_handle.st_ino == status.st_ino;
    if (fd >= 0 && close(fd) != 0) {
        safe = false;
    }
    if (!safe) {
        return -1;
    }
    *status_out = status;
    return 1;
}

static int posix_socket_record_identity_corroborated(
    const cbm_daemon_ipc_endpoint_t *endpoint, const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE],
    const posix_socket_record_t *record) {
    if (!endpoint || !magic || !record) {
        return -1;
    }
    posix_socket_identity_t stable = {0};
    posix_socket_identity_t anchor = {0};
    struct stat stable_status = {0};
    struct stat anchor_status = {0};
    int stable_state =
        posix_socket_path_identity_read(endpoint, endpoint->socket_name, &stable, &stable_status);
    int anchor_state = posix_socket_path_identity_read(endpoint, endpoint->socket_anchor_name,
                                                       &anchor, &anchor_status);
    if (stable_state < 0 || anchor_state < 0) {
        return -1;
    }

    if (memcmp(magic, POSIX_SOCKET_PENDING_MAGIC, POSIX_SOCKET_RECORD_MAGIC_SIZE) == 0) {
        return stable_state == 0 && anchor_state == 1 && anchor_status.st_nlink == 1 &&
                       posix_socket_identity_equal(&record->identity, &anchor)
                   ? 1
                   : 0;
    }
    if (memcmp(magic, POSIX_SOCKET_MARKER_MAGIC, POSIX_SOCKET_RECORD_MAGIC_SIZE) == 0) {
        return stable_state == 1 && anchor_state == 1 && stable_status.st_nlink == 2 &&
                       anchor_status.st_nlink == 2 &&
                       posix_socket_identity_equal(&record->identity, &stable) &&
                       posix_socket_identity_equal(&record->identity, &anchor)
                   ? 1
                   : 0;
    }
    return -1;
}

/* Normalize only publication artifacts whose deterministic temp name,
 * owner/mode/link shape, encoded record, and socket phase all agree. A
 * mismatched artifact is preserved and blocks startup rather than becoming
 * deletion authority. */
static int posix_socket_record_publication_recover(
    const cbm_daemon_ipc_endpoint_t *endpoint, const char *record_name,
    const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE]) {
    char temp_name[NAME_MAX + 1];
    if (!posix_socket_record_temp_name(record_name, temp_name)) {
        return -1;
    }
    struct stat record_status = {0};
    struct stat temp_status = {0};
    int record_state = posix_record_artifact_status(endpoint, record_name, &record_status);
    int temp_state = posix_record_artifact_status(endpoint, temp_name, &temp_status);
    if (record_state < 0 || temp_state < 0) {
        return -1;
    }
    if (temp_state == 0) {
        return record_state == 0 || record_status.st_nlink == 1 ? 1 : 0;
    }

    posix_socket_record_t recovered = {0};
    struct stat recovered_status = {0};
    if (record_state == 0) {
        if (temp_status.st_nlink != 1) {
            return 0;
        }
        posix_record_state_t read_state = posix_socket_record_read_links(
            endpoint, temp_name, magic, 1, &recovered, &recovered_status);
        if (read_state != POSIX_RECORD_VALID) {
            return read_state == POSIX_RECORD_UNSAFE ? -1 : 0;
        }
    } else {
        if (record_status.st_nlink != 2 || temp_status.st_nlink != 2 ||
            record_status.st_dev != temp_status.st_dev ||
            record_status.st_ino != temp_status.st_ino) {
            return 0;
        }
        posix_record_state_t read_state = posix_socket_record_read_links(
            endpoint, record_name, magic, 2, &recovered, &recovered_status);
        if (read_state != POSIX_RECORD_VALID) {
            return read_state == POSIX_RECORD_UNSAFE ? -1 : 0;
        }
        if (recovered_status.st_dev != temp_status.st_dev ||
            recovered_status.st_ino != temp_status.st_ino) {
            return 0;
        }
    }

    int corroborated = posix_socket_record_identity_corroborated(endpoint, magic, &recovered);
    if (corroborated != 1) {
        return corroborated;
    }
    if (!posix_path_unlink_regular_if_matches(endpoint->dir_fd, temp_name, recovered_status.st_dev,
                                              recovered_status.st_ino, record_state == 1 ? 2 : 1) ||
        !posix_directory_sync(endpoint->dir_fd)) {
        return -1;
    }
    return 1;
}

static int posix_linkat_no_follow(int source_dir_fd, const char *source_name,
                                  int destination_dir_fd, const char *destination_name) {
    /* linkat without AT_SYMLINK_FOLLOW links the source entry itself, which is
     * the required no-follow behavior.  Do not pass Darwin's broader
     * AT_SYMLINK_NOFOLLOW_ANY constant: macOS 14 headers define it, but that
     * kernel rejects it for linkat with EINVAL.  The retained and validated
     * source state plus post-link inode/link-count checks retain the
     * fail-closed publication contract. */
    return linkat(source_dir_fd, source_name, destination_dir_fd, destination_name, 0);
}

static void posix_bound_socket_unlink_if_matches(int dir_fd, const char *socket_name, dev_t device,
                                                 ino_t inode) {
    struct stat status;
    if (dir_fd >= 0 && socket_name &&
        fstatat(dir_fd, socket_name, &status, AT_SYMLINK_NOFOLLOW) == 0 &&
        S_ISSOCK(status.st_mode) && status.st_uid == geteuid() && status.st_dev == device &&
        status.st_ino == inode) {
        (void)unlinkat(dir_fd, socket_name, 0);
    }
}

static bool posix_socket_record_publish(const cbm_daemon_ipc_endpoint_t *endpoint,
                                        const char *record_name,
                                        const uint8_t magic[POSIX_SOCKET_RECORD_MAGIC_SIZE],
                                        const posix_socket_record_t *source, dev_t *device_out,
                                        ino_t *inode_out) {
    if (!endpoint || !record_name || !magic || !source || !device_out || !inode_out ||
        !endpoint_runtime_still_valid(endpoint)) {
        return false;
    }
    struct stat existing;
    if (fstatat(endpoint->dir_fd, record_name, &existing, AT_SYMLINK_NOFOLLOW) == 0 ||
        errno != ENOENT) {
        return false;
    }
    uint8_t record[POSIX_SOCKET_RECORD_SIZE];
    if (!posix_socket_record_encode(magic, source, record)) {
        return false;
    }

    char temp_name[NAME_MAX + 1];
    if (!posix_socket_record_temp_name(record_name, temp_name)) {
        return false;
    }
    int fd = openat(endpoint->dir_fd, temp_name,
                    O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK, 0600);
    if (fd < 0) {
        return false;
    }

    struct stat created = {0};
    bool temp_exists = true;
    bool stable_linked = false;
    bool ok = fd_set_cloexec(fd) && fchmod(fd, 0600) == 0 &&
              private_regular_file_snapshot(endpoint->dir_fd, temp_name, fd, 1, &created) &&
              posix_fd_write_all(fd, record, sizeof(record)) && fsync(fd) == 0 &&
              private_regular_file_snapshot(endpoint->dir_fd, temp_name, fd, 1, &created) &&
              created.st_size == (off_t)POSIX_SOCKET_RECORD_SIZE &&
              endpoint_runtime_still_valid(endpoint);
    if (ok) {
        posix_record_publication_stage_reached(magic, false);
        ok =
            posix_linkat_no_follow(endpoint->dir_fd, temp_name, endpoint->dir_fd, record_name) == 0;
        stable_linked = ok;
    }
    if (ok) {
        posix_record_publication_stage_reached(magic, true);
        ok = posix_path_unlink_regular_if_matches(endpoint->dir_fd, temp_name, created.st_dev,
                                                  created.st_ino, 2);
        temp_exists = !ok;
    }
    if (ok) {
        ok = posix_directory_sync(endpoint->dir_fd);
    }
    if (close(fd) != 0) {
        ok = false;
    }

    posix_socket_record_t published_record;
    struct stat published_status;
    if (ok) {
        ok = posix_socket_record_read(endpoint, record_name, magic, &published_record,
                                      &published_status) == POSIX_RECORD_VALID &&
             posix_socket_identity_equal(&published_record.identity, &source->identity) &&
             strcmp(published_record.anchor_name, source->anchor_name) == 0 &&
             published_status.st_dev == created.st_dev && published_status.st_ino == created.st_ino;
    }
    if (!ok) {
        if (stable_linked) {
            (void)posix_path_unlink_regular_if_matches(
                endpoint->dir_fd, record_name, created.st_dev, created.st_ino, temp_exists ? 2 : 1);
        }
        if (temp_exists) {
            (void)posix_path_unlink_regular_if_matches(endpoint->dir_fd, temp_name, created.st_dev,
                                                       created.st_ino, 1);
        }
        (void)posix_directory_sync(endpoint->dir_fd);
        return false;
    }
    *device_out = published_status.st_dev;
    *inode_out = published_status.st_ino;
    return true;
}

static bool posix_endpoint_namespace_equal(const cbm_daemon_ipc_endpoint_t *left,
                                           const cbm_daemon_ipc_endpoint_t *right) {
    return left && right && left->dir_device == right->dir_device &&
           left->dir_inode == right->dir_inode && left->socket_name && right->socket_name &&
           strcmp(left->socket_name, right->socket_name) == 0 && left->socket_anchor_name &&
           right->socket_anchor_name &&
           strcmp(left->socket_anchor_name, right->socket_anchor_name) == 0 &&
           left->socket_anchor_address && right->socket_anchor_address &&
           strcmp(left->socket_anchor_address, right->socket_anchor_address) == 0 &&
           left->socket_identity_name && right->socket_identity_name &&
           strcmp(left->socket_identity_name, right->socket_identity_name) == 0 &&
           left->socket_pending_name && right->socket_pending_name &&
           strcmp(left->socket_pending_name, right->socket_pending_name) == 0 && left->lock_name &&
           right->lock_name && strcmp(left->lock_name, right->lock_name) == 0 &&
           left->startup_v2_lock_name && right->startup_v2_lock_name &&
           strcmp(left->startup_v2_lock_name, right->startup_v2_lock_name) == 0 &&
           left->lifetime_lock_name && right->lifetime_lock_name &&
           strcmp(left->lifetime_lock_name, right->lifetime_lock_name) == 0;
}

static bool posix_named_lock_is_retained(const cbm_daemon_ipc_endpoint_t *endpoint,
                                         const char *lock_name, bool shared, int fd,
                                         const process_lock_entry_t *entry, pid_t owner_pid) {
    if (!endpoint_runtime_still_valid(endpoint) || !lock_name || fd < 0 || !entry ||
        entry->shared != shared || owner_pid != getpid()) {
        return false;
    }
    if (entry->directory_device != endpoint->dir_device ||
        entry->directory_inode != endpoint->dir_inode || !entry->lock_name ||
        strcmp(entry->lock_name, lock_name) != 0 ||
        process_lock_is_claimed(endpoint, lock_name) != 1) {
        return false;
    }

    if (!private_regular_file_snapshot(endpoint->dir_fd, lock_name, fd, 1, NULL)) {
        return false;
    }
    int lock_result;
    do {
        lock_result = flock(fd, (shared ? LOCK_SH : LOCK_EX) | LOCK_NB);
    } while (lock_result != 0 && errno == EINTR);
    return lock_result == 0;
}

static bool posix_startup_lock_matches_endpoint(const cbm_daemon_ipc_endpoint_t *endpoint,
                                                const cbm_daemon_ipc_startup_lock_t *startup_lock) {
    return startup_lock && endpoint_runtime_still_valid(startup_lock->endpoint_snapshot) &&
           posix_endpoint_namespace_equal(endpoint, startup_lock->endpoint_snapshot) &&
           posix_named_lock_is_retained(
               endpoint, endpoint->startup_v2_lock_name, false, startup_lock->startup_v2_fd,
               startup_lock->startup_v2_process_entry, startup_lock->owner_pid) &&
           posix_named_lock_is_retained(endpoint, endpoint->lock_name, true,
                                        startup_lock->legacy_fd, startup_lock->legacy_process_entry,
                                        startup_lock->owner_pid);
}

static int posix_stale_generation_cleanup_locked(const cbm_daemon_ipc_endpoint_t *endpoint) {
    /* Holding the startup lock excludes compliant starters. Reserving the
     * endpoint lifetime as well closes the check/unlink race against a caller
     * that already owns a transferred reservation. */
    cbm_daemon_ipc_lifetime_reservation_t *cleanup_reservation = NULL;
    int reservation_result =
        cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &cleanup_reservation);
    if (reservation_result != 1) {
        return reservation_result == 0 ? 0 : -1;
    }

    int result = -1;
    int pending_publication = posix_socket_record_publication_recover(
        endpoint, endpoint->socket_pending_name, POSIX_SOCKET_PENDING_MAGIC);
    int marker_publication = posix_socket_record_publication_recover(
        endpoint, endpoint->socket_identity_name, POSIX_SOCKET_MARKER_MAGIC);
    if (pending_publication != 1 || marker_publication != 1) {
        result = pending_publication < 0 || marker_publication < 0 ? -1 : 0;
        cbm_daemon_ipc_lifetime_reservation_release(cleanup_reservation);
        return result;
    }

    posix_socket_record_t marker = {0};
    posix_socket_record_t pending = {0};
    struct stat marker_status = {0};
    struct stat pending_status = {0};
    posix_record_state_t marker_state =
        posix_socket_record_read(endpoint, endpoint->socket_identity_name,
                                 POSIX_SOCKET_MARKER_MAGIC, &marker, &marker_status);
    posix_record_state_t pending_state =
        posix_socket_record_read(endpoint, endpoint->socket_pending_name,
                                 POSIX_SOCKET_PENDING_MAGIC, &pending, &pending_status);
    posix_socket_identity_t stable_identity = {0};
    posix_socket_identity_t anchor_identity = {0};
    struct stat stable_status = {0};
    struct stat anchor_status = {0};
    int stable_state = posix_socket_path_identity_read(endpoint, endpoint->socket_name,
                                                       &stable_identity, &stable_status);
    int anchor_state = posix_socket_path_identity_read(endpoint, endpoint->socket_anchor_name,
                                                       &anchor_identity, &anchor_status);

    if (marker_state == POSIX_RECORD_UNSAFE || pending_state == POSIX_RECORD_UNSAFE ||
        stable_state < 0 || anchor_state < 0) {
        result = -1;
        goto cleanup_done;
    }
    if (marker_state == POSIX_RECORD_UNKNOWN || pending_state == POSIX_RECORD_UNKNOWN ||
        (marker_state == POSIX_RECORD_VALID && pending_state == POSIX_RECORD_VALID &&
         !posix_socket_inode_equal(&marker.identity, &pending.identity))) {
        result = 0;
        goto cleanup_done;
    }

    /* A durable pending record may complete the commit only when both names
     * still corroborate its exact anchor inode. It never authorizes deleting
     * the stable name by itself. linkat changes ctime, so pending's pre-link
     * identity is compared by dev+ino and the final identity is captured now. */
    if (marker_state == POSIX_RECORD_ABSENT && pending_state == POSIX_RECORD_VALID &&
        stable_state == 1 && anchor_state == 1 && stable_status.st_nlink == 2 &&
        anchor_status.st_nlink == 2 &&
        posix_socket_inode_equal(&stable_identity, &anchor_identity) &&
        posix_socket_inode_equal(&pending.identity, &anchor_identity)) {
        posix_socket_record_t completed = {
            .identity = anchor_identity,
        };
        (void)snprintf(completed.anchor_name, sizeof(completed.anchor_name), "%s",
                       endpoint->socket_anchor_name);
        if (!posix_socket_record_publish(endpoint, endpoint->socket_identity_name,
                                         POSIX_SOCKET_MARKER_MAGIC, &completed,
                                         &marker_status.st_dev, &marker_status.st_ino)) {
            result = -1;
            goto cleanup_done;
        }
        marker = completed;
        marker_state = POSIX_RECORD_VALID;
    }

    if (marker_state == POSIX_RECORD_VALID) {
        bool anchor_owned =
            anchor_state == 1 && posix_socket_inode_equal(&marker.identity, &anchor_identity);
        if (stable_state == 1 && anchor_owned &&
            posix_socket_inode_equal(&stable_identity, &anchor_identity)) {
            if (stable_status.st_nlink != 2 || anchor_status.st_nlink != 2 ||
                !posix_socket_identity_equal(&marker.identity, &stable_identity) ||
                !posix_socket_identity_equal(&marker.identity, &anchor_identity)) {
                result = -1;
                goto cleanup_done;
            }
            posix_socket_identity_t confirmed_stable = {0};
            posix_socket_identity_t confirmed_anchor = {0};
            struct stat confirmed_stable_status = {0};
            struct stat confirmed_anchor_status = {0};
            bool confirmed =
                posix_socket_path_identity_read(endpoint, endpoint->socket_name, &confirmed_stable,
                                                &confirmed_stable_status) == 1 &&
                posix_socket_path_identity_read(endpoint, endpoint->socket_anchor_name,
                                                &confirmed_anchor, &confirmed_anchor_status) == 1 &&
                confirmed_stable_status.st_nlink == 2 && confirmed_anchor_status.st_nlink == 2 &&
                posix_socket_identity_equal(&confirmed_stable, &marker.identity) &&
                posix_socket_identity_equal(&confirmed_anchor, &marker.identity);
            if (!confirmed ||
                !posix_socket_path_unlink_inode_if_matches(endpoint->dir_fd, endpoint->socket_name,
                                                           &marker.identity, 2) ||
                !posix_socket_path_unlink_inode_if_matches(
                    endpoint->dir_fd, endpoint->socket_anchor_name, &marker.identity, 1)) {
                result = -1;
                goto cleanup_done;
            }
            stable_state = 0;
            anchor_state = 0;
        } else {
            /* A legacy/unknown process may have replaced stable. The retained
             * anchor proves which inode is ours; preserve every differing
             * stable socket and collect only our private publication state. */
            if (anchor_state == 1) {
                if (!anchor_owned || anchor_status.st_nlink != 1 ||
                    !posix_socket_path_unlink_inode_if_matches(
                        endpoint->dir_fd, endpoint->socket_anchor_name, &marker.identity, 1)) {
                    result = -1;
                    goto cleanup_done;
                }
                anchor_state = 0;
            }
        }

        if (!posix_path_unlink_regular_if_matches(endpoint->dir_fd, endpoint->socket_identity_name,
                                                  marker_status.st_dev, marker_status.st_ino, 1) ||
            (pending_state == POSIX_RECORD_VALID &&
             !posix_path_unlink_regular_if_matches(endpoint->dir_fd, endpoint->socket_pending_name,
                                                   pending_status.st_dev, pending_status.st_ino,
                                                   1)) ||
            !posix_directory_sync(endpoint->dir_fd)) {
            result = -1;
            goto cleanup_done;
        }
        result = stable_state == 0 ? 1 : 0;
        goto cleanup_done;
    }

    if (pending_state == POSIX_RECORD_VALID) {
        bool anchor_owned =
            anchor_state == 1 && posix_socket_inode_equal(&pending.identity, &anchor_identity);
        if (anchor_state == 1 && (!anchor_owned || anchor_status.st_nlink != 1)) {
            result = -1;
            goto cleanup_done;
        }
        if (anchor_owned && !posix_socket_path_unlink_inode_if_matches(endpoint->dir_fd,
                                                                       endpoint->socket_anchor_name,
                                                                       &pending.identity, 1)) {
            result = -1;
            goto cleanup_done;
        }
        if (!posix_path_unlink_regular_if_matches(endpoint->dir_fd, endpoint->socket_pending_name,
                                                  pending_status.st_dev, pending_status.st_ino,
                                                  1) ||
            !posix_directory_sync(endpoint->dir_fd)) {
            result = -1;
            goto cleanup_done;
        }
        result = stable_state == 0 ? 1 : 0;
        goto cleanup_done;
    }

    /* The deterministic generation-local anchor lets us collect the sole
     * otherwise-untrackable crash boundary: bind/listen completed but the
     * pending record was not yet durable. It never grants authority over the
     * public stable path. */
    if (anchor_state == 1) {
        if (anchor_status.st_nlink != 1 ||
            !posix_socket_path_unlink_inode_if_matches(
                endpoint->dir_fd, endpoint->socket_anchor_name, &anchor_identity, 1) ||
            !posix_directory_sync(endpoint->dir_fd)) {
            result = -1;
            goto cleanup_done;
        }
    }
    result = stable_state == 0 ? 1 : 0;

cleanup_done:
    cbm_daemon_ipc_lifetime_reservation_release(cleanup_reservation);
    return result;
}

int cbm_daemon_ipc_stale_generation_cleanup(const cbm_daemon_ipc_endpoint_t *endpoint,
                                            const cbm_daemon_ipc_startup_lock_t *startup_lock) {
    return posix_startup_lock_matches_endpoint(endpoint, startup_lock)
               ? posix_stale_generation_cleanup_locked(endpoint)
               : -1;
}

static void posix_publication_abort(const cbm_daemon_ipc_endpoint_t *endpoint,
                                    const posix_socket_identity_t *anchor_identity,
                                    bool pending_published, const struct stat *pending_status,
                                    bool marker_published, const struct stat *marker_status) {
    if (!endpoint || !anchor_identity) {
        return;
    }
    posix_socket_identity_t stable = {0};
    posix_socket_identity_t anchor = {0};
    struct stat stable_status = {0};
    struct stat anchor_status = {0};
    int stable_state =
        posix_socket_path_identity_read(endpoint, endpoint->socket_name, &stable, &stable_status);
    int anchor_state = posix_socket_path_identity_read(endpoint, endpoint->socket_anchor_name,
                                                       &anchor, &anchor_status);
    if (stable_state == 1 && anchor_state == 1 && stable_status.st_nlink == 2 &&
        anchor_status.st_nlink == 2 && posix_socket_inode_equal(&stable, anchor_identity) &&
        posix_socket_inode_equal(&anchor, anchor_identity) &&
        posix_socket_path_unlink_inode_if_matches(endpoint->dir_fd, endpoint->socket_name,
                                                  anchor_identity, 2)) {
        anchor_status.st_nlink = 1;
    }
    if (anchor_state == 1 && anchor_status.st_nlink == 1 &&
        posix_socket_inode_equal(&anchor, anchor_identity)) {
        (void)posix_socket_path_unlink_inode_if_matches(
            endpoint->dir_fd, endpoint->socket_anchor_name, anchor_identity, 1);
    }
    if (marker_published && marker_status) {
        (void)posix_path_unlink_regular_if_matches(endpoint->dir_fd, endpoint->socket_identity_name,
                                                   marker_status->st_dev, marker_status->st_ino, 1);
    }
    if (pending_published && pending_status) {
        (void)posix_path_unlink_regular_if_matches(endpoint->dir_fd, endpoint->socket_pending_name,
                                                   pending_status->st_dev, pending_status->st_ino,
                                                   1);
    }
    (void)posix_directory_sync(endpoint->dir_fd);
}

cbm_daemon_ipc_listener_t *cbm_daemon_ipc_listen_reserved(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    cbm_daemon_ipc_lifetime_reservation_t **reservation_io) {
    cbm_daemon_ipc_lifetime_reservation_t *lifetime_reservation =
        reservation_io ? *reservation_io : NULL;
    if (!lifetime_reservation_matches_endpoint(endpoint, lifetime_reservation)) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "reservation_validation");
        return NULL;
    }
    /* Stale removal happens only under the startup lock, before a daemon host
     * transfers this lifetime reservation. The listener path itself never
     * connects to, classifies, or removes pre-existing artifacts. */
    char identity_temp_name[NAME_MAX + 1];
    char pending_temp_name[NAME_MAX + 1];
    if (!posix_socket_record_temp_name(endpoint->socket_identity_name, identity_temp_name) ||
        !posix_socket_record_temp_name(endpoint->socket_pending_name, pending_temp_name)) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "temp_names");
        return NULL;
    }
    struct stat existing;
    const char *required_absent[] = {
        endpoint->socket_name,         endpoint->socket_anchor_name, endpoint->socket_identity_name,
        endpoint->socket_pending_name, identity_temp_name,           pending_temp_name,
    };
    bool namespace_absent = true;
    for (size_t index = 0; index < sizeof(required_absent) / sizeof(required_absent[0]); index++) {
        if (fstatat(endpoint->dir_fd, required_absent[index], &existing, AT_SYMLINK_NOFOLLOW) ==
                0 ||
            errno != ENOENT) {
            namespace_absent = false;
            break;
        }
    }
    if (!endpoint_runtime_still_valid(endpoint) || !namespace_absent) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "namespace_validation");
        return NULL;
    }

    int fd = local_socket_new();
    if (fd < 0) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "socket_creation");
        return NULL;
    }
    cbm_daemon_ipc_listener_t *listener = calloc(1, sizeof(*listener));
    if (!listener) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "listener_allocation");
        (void)close(fd);
        return NULL;
    }
    listener->fd = fd;
    listener->dir_fd = dup(endpoint->dir_fd);
    listener->runtime_dir = string_copy(endpoint->runtime_dir);
    listener->dir_device = endpoint->dir_device;
    listener->dir_inode = endpoint->dir_inode;
    listener->address = string_copy(endpoint->address);
    listener->socket_name = string_copy(endpoint->socket_name);
    listener->socket_anchor_name = string_copy(endpoint->socket_anchor_name);
    listener->socket_identity_name = string_copy(endpoint->socket_identity_name);
    listener->socket_pending_name = string_copy(endpoint->socket_pending_name);
    listener->owner_pid = getpid();
    if (listener->dir_fd < 0 || !fd_set_cloexec(listener->dir_fd) || !listener->runtime_dir ||
        !listener->address || !listener->socket_name || !listener->socket_anchor_name ||
        !listener->socket_identity_name || !listener->socket_pending_name) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "listener_initialization");
        if (listener->dir_fd >= 0) {
            (void)close(listener->dir_fd);
        }
        (void)close(fd);
        free(listener->runtime_dir);
        free(listener->address);
        free(listener->socket_name);
        free(listener->socket_anchor_name);
        free(listener->socket_identity_name);
        free(listener->socket_pending_name);
        free(listener);
        return NULL;
    }

    struct sockaddr_un address;
    socklen_t address_length;
    if (!unix_address_set(&address, endpoint->socket_anchor_address, &address_length)) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "socket_address");
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    if (bind(fd, (const struct sockaddr *)&address, address_length) != 0) {
        int bind_error = errno;
        char error_text[32];
        (void)snprintf(error_text, sizeof(error_text), "%d", bind_error);
        cbm_log_error("daemon.ipc.listen_failed", "bind_errno", error_text);
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }

    struct stat bound_status;
    bool bound_path_ok = fstatat(endpoint->dir_fd, endpoint->socket_anchor_name, &bound_status,
                                 AT_SYMLINK_NOFOLLOW) == 0 &&
                         S_ISSOCK(bound_status.st_mode) && bound_status.st_uid == geteuid();
    struct stat anchor_status = {0};
    bool secured =
        bound_path_ok && fchmodat(endpoint->dir_fd, endpoint->socket_anchor_name, 0600, 0) == 0 &&
        fstatat(endpoint->dir_fd, endpoint->socket_anchor_name, &anchor_status,
                AT_SYMLINK_NOFOLLOW) == 0 &&
        posix_socket_status_secure_links(&anchor_status, 1) &&
        anchor_status.st_dev == bound_status.st_dev && anchor_status.st_ino == bound_status.st_ino;
    posix_socket_identity_t anchor_identity = {0};
    if (!secured || listen(fd, 32) != 0 ||
        fstatat(endpoint->dir_fd, endpoint->socket_anchor_name, &anchor_status,
                AT_SYMLINK_NOFOLLOW) != 0 ||
        !posix_socket_status_secure_links(&anchor_status, 1) ||
        anchor_status.st_dev != bound_status.st_dev ||
        anchor_status.st_ino != bound_status.st_ino ||
        !posix_socket_identity_from_stat(&anchor_status, &anchor_identity) ||
        !posix_directory_sync(endpoint->dir_fd)) {
        if (bound_path_ok) {
            posix_bound_socket_unlink_if_matches(endpoint->dir_fd, endpoint->socket_anchor_name,
                                                 bound_status.st_dev, bound_status.st_ino);
        }
        cbm_log_error("daemon.ipc.listen_failed", "stage", "socket_security");
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    listener->socket_identity = anchor_identity;
    posix_publication_stage_reached(CBM_DAEMON_IPC_POSIX_PUBLICATION_ANCHOR_DURABLE);

    posix_socket_record_t pending = {
        .identity = anchor_identity,
    };
    (void)snprintf(pending.anchor_name, sizeof(pending.anchor_name), "%s",
                   endpoint->socket_anchor_name);
    struct stat pending_status = {0};
    bool pending_published = posix_socket_record_publish(
        endpoint, endpoint->socket_pending_name, POSIX_SOCKET_PENDING_MAGIC, &pending,
        &pending_status.st_dev, &pending_status.st_ino);
    if (!pending_published) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "pending_publication");
        posix_publication_abort(endpoint, &anchor_identity, false, NULL, false, NULL);
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    posix_publication_stage_reached(CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_DURABLE);

    bool stable_linked = posix_linkat_no_follow(endpoint->dir_fd, endpoint->socket_anchor_name,
                                                endpoint->dir_fd, endpoint->socket_name) == 0 &&
                         posix_directory_sync(endpoint->dir_fd);
    posix_socket_identity_t stable_identity = {0};
    posix_socket_identity_t committed_identity = {0};
    struct stat stable_status = {0};
    struct stat committed_anchor_status = {0};
    bool stable_valid =
        stable_linked &&
        posix_socket_path_identity_read(endpoint, endpoint->socket_name, &stable_identity,
                                        &stable_status) == 1 &&
        posix_socket_path_identity_read(endpoint, endpoint->socket_anchor_name, &committed_identity,
                                        &committed_anchor_status) == 1 &&
        stable_status.st_nlink == 2 && committed_anchor_status.st_nlink == 2 &&
        posix_socket_identity_equal(&stable_identity, &committed_identity) &&
        posix_socket_inode_equal(&anchor_identity, &committed_identity);
    if (!stable_valid) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "stable_publication");
        posix_publication_abort(endpoint, &anchor_identity, pending_published, &pending_status,
                                false, NULL);
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    listener->socket_identity = committed_identity;
    posix_publication_stage_reached(CBM_DAEMON_IPC_POSIX_PUBLICATION_STABLE_DURABLE);

    posix_socket_record_t marker = {
        .identity = committed_identity,
    };
    (void)snprintf(marker.anchor_name, sizeof(marker.anchor_name), "%s",
                   endpoint->socket_anchor_name);
    struct stat marker_status = {0};
    bool marker_published = posix_socket_record_publish(
        endpoint, endpoint->socket_identity_name, POSIX_SOCKET_MARKER_MAGIC, &marker,
        &marker_status.st_dev, &marker_status.st_ino);
    if (!marker_published) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "marker_publication");
        posix_publication_abort(endpoint, &committed_identity, pending_published, &pending_status,
                                false, NULL);
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    listener->identity_device = marker_status.st_dev;
    listener->identity_inode = marker_status.st_ino;
    posix_publication_stage_reached(CBM_DAEMON_IPC_POSIX_PUBLICATION_MARKER_DURABLE);

    if (!posix_path_unlink_regular_if_matches(endpoint->dir_fd, endpoint->socket_pending_name,
                                              pending_status.st_dev, pending_status.st_ino, 1) ||
        !posix_directory_sync(endpoint->dir_fd)) {
        cbm_log_error("daemon.ipc.listen_failed", "stage", "pending_removal");
        posix_publication_abort(endpoint, &committed_identity, true, &pending_status,
                                marker_published, &marker_status);
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    posix_publication_stage_reached(CBM_DAEMON_IPC_POSIX_PUBLICATION_PENDING_REMOVED);
    listener->lifetime_reservation = lifetime_reservation;
    *reservation_io = NULL;
    return listener;
}

cbm_daemon_ipc_listener_t *cbm_daemon_ipc_listen(const cbm_daemon_ipc_endpoint_t *endpoint) {
    cbm_daemon_ipc_startup_lock_t *startup_lock = NULL;
    if (cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup_lock) != 1) {
        return NULL;
    }
    cbm_daemon_ipc_lifetime_reservation_t *reservation = NULL;
    cbm_daemon_ipc_participant_guard_t *participant_guard = NULL;
    if (cbm_daemon_ipc_stale_generation_cleanup(endpoint, startup_lock) != 1 ||
        cbm_daemon_ipc_participant_guard_try_join(endpoint, &participant_guard) != 1 ||
        cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &reservation) != 1) {
        ipc_participant_guard_release_complete(&participant_guard);
        ipc_startup_lock_release_complete(&startup_lock);
        return NULL;
    }
    cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen_reserved(endpoint, &reservation);
    if (listener) {
        listener->participant_guard = participant_guard;
        participant_guard = NULL;
    }
    cbm_daemon_ipc_lifetime_reservation_release(reservation);
    ipc_participant_guard_release_complete(&participant_guard);
    ipc_startup_lock_release_complete(&startup_lock);
    return listener;
}

static void posix_listener_artifacts_remove_if_matches(cbm_daemon_ipc_listener_t *listener) {
    if (!listener || listener->dir_fd < 0 || !listener->socket_name ||
        !listener->socket_anchor_name || !listener->socket_identity_name ||
        !listener->socket_pending_name || listener->owner_pid != getpid()) {
        return;
    }
    cbm_daemon_ipc_endpoint_t view = {
        .runtime_dir = listener->runtime_dir,
        .dir_device = listener->dir_device,
        .dir_inode = listener->dir_inode,
        .socket_name = listener->socket_name,
        .socket_anchor_name = listener->socket_anchor_name,
        .socket_identity_name = listener->socket_identity_name,
        .socket_pending_name = listener->socket_pending_name,
        .dir_fd = listener->dir_fd,
    };
    posix_socket_record_t marker = {0};
    posix_socket_record_t pending = {0};
    struct stat marker_status = {0};
    struct stat pending_status = {0};
    posix_record_state_t marker_state = posix_socket_record_read(
        &view, listener->socket_identity_name, POSIX_SOCKET_MARKER_MAGIC, &marker, &marker_status);
    posix_record_state_t pending_state =
        posix_socket_record_read(&view, listener->socket_pending_name, POSIX_SOCKET_PENDING_MAGIC,
                                 &pending, &pending_status);
    bool marker_matches = marker_state == POSIX_RECORD_VALID &&
                          marker_status.st_dev == listener->identity_device &&
                          marker_status.st_ino == listener->identity_inode &&
                          posix_socket_identity_equal(&marker.identity, &listener->socket_identity);
    bool pending_matches =
        pending_state == POSIX_RECORD_ABSENT ||
        (pending_state == POSIX_RECORD_VALID &&
         posix_socket_inode_equal(&pending.identity, &listener->socket_identity));
    if (!marker_matches || !pending_matches) {
        return;
    }

    posix_socket_identity_t stable = {0};
    posix_socket_identity_t anchor = {0};
    struct stat stable_status = {0};
    struct stat anchor_status = {0};
    int stable_state =
        posix_socket_path_identity_read(&view, listener->socket_name, &stable, &stable_status);
    int anchor_state = posix_socket_path_identity_read(&view, listener->socket_anchor_name, &anchor,
                                                       &anchor_status);
    if (stable_state < 0 || anchor_state < 0) {
        return;
    }
    bool anchor_owned =
        anchor_state == 1 && posix_socket_inode_equal(&anchor, &listener->socket_identity);
    if (stable_state == 1 && anchor_owned && posix_socket_inode_equal(&stable, &anchor)) {
        if (stable_status.st_nlink != 2 || anchor_status.st_nlink != 2 ||
            !posix_socket_identity_equal(&stable, &listener->socket_identity) ||
            !posix_socket_identity_equal(&anchor, &listener->socket_identity) ||
            !posix_socket_path_unlink_inode_if_matches(listener->dir_fd, listener->socket_name,
                                                       &listener->socket_identity, 2) ||
            !posix_socket_path_unlink_inode_if_matches(
                listener->dir_fd, listener->socket_anchor_name, &listener->socket_identity, 1)) {
            return;
        }
    } else if (anchor_state == 1) {
        if (!anchor_owned || anchor_status.st_nlink != 1 ||
            !posix_socket_path_unlink_inode_if_matches(
                listener->dir_fd, listener->socket_anchor_name, &listener->socket_identity, 1)) {
            return;
        }
    }

    if (!posix_path_unlink_regular_if_matches(listener->dir_fd, listener->socket_identity_name,
                                              listener->identity_device, listener->identity_inode,
                                              1)) {
        return;
    }
    if (pending_state == POSIX_RECORD_VALID &&
        !posix_path_unlink_regular_if_matches(listener->dir_fd, listener->socket_pending_name,
                                              pending_status.st_dev, pending_status.st_ino, 1)) {
        return;
    }
    (void)posix_directory_sync(listener->dir_fd);
}

void cbm_daemon_ipc_listener_close(cbm_daemon_ipc_listener_t *listener) {
    if (!listener) {
        return;
    }
    if (listener->fd >= 0) {
        (void)close(listener->fd);
        listener->fd = -1;
    }
    if (listener->dir_fd >= 0) {
        posix_listener_artifacts_remove_if_matches(listener);
        (void)close(listener->dir_fd);
    }
    cbm_daemon_ipc_lifetime_reservation_release(listener->lifetime_reservation);
    ipc_participant_guard_release_complete(&listener->participant_guard);
    free(listener->runtime_dir);
    free(listener->address);
    free(listener->socket_name);
    free(listener->socket_anchor_name);
    free(listener->socket_identity_name);
    free(listener->socket_pending_name);
    free(listener);
}

int cbm_daemon_ipc_accept(cbm_daemon_ipc_listener_t *listener, uint32_t timeout_ms,
                          cbm_daemon_ipc_connection_t **connection_out) {
    if (connection_out) {
        *connection_out = NULL;
    }
    if (!listener || listener->fd < 0 || listener->owner_pid != getpid() || !connection_out) {
        return -1;
    }
    uint64_t deadline_ms = ipc_deadline_after(timeout_ms);
    for (;;) {
        int ready = poll_until(listener->fd, POLLIN, deadline_ms);
        if (ready != 1) {
            return ready;
        }
        int fd = accept(listener->fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            return -1;
        }
        if (!fd_set_cloexec(fd) || !fd_set_nonblocking(fd) || !socket_disable_sigpipe(fd) ||
            !socket_peer_is_current_user(fd)) {
            (void)close(fd);
            return -1;
        }
        cbm_daemon_ipc_connection_t *connection = malloc(sizeof(*connection));
        if (!connection) {
            (void)close(fd);
            return -1;
        }
        connection->fd = fd;
        atomic_init(&connection->poisoned, false);
        *connection_out = connection;
        return 1;
    }
}

int cbm_daemon_ipc_endpoint_probe(const cbm_daemon_ipc_endpoint_t *endpoint, uint32_t timeout_ms) {
    if (!endpoint_runtime_still_valid(endpoint)) {
        return -1;
    }
    struct stat status;
    if (fstatat(endpoint->dir_fd, endpoint->socket_name, &status, AT_SYMLINK_NOFOLLOW) != 0) {
        return errno == ENOENT ? 0 : -1;
    }
    if (!posix_socket_status_secure_transport(&status)) {
        return -1;
    }

    int fd = local_socket_new();
    if (fd < 0) {
        return -1;
    }
    struct sockaddr_un address;
    socklen_t address_length;
    if (!unix_address_set(&address, endpoint->address, &address_length)) {
        (void)close(fd);
        return -1;
    }

    int connected = local_socket_connect(fd, &address, address_length);
    int connect_error = connected == 0 ? 0 : errno;
    if (connected == 0) {
        bool authenticated = socket_peer_is_current_user(fd);
        (void)close(fd);
        return authenticated ? 1 : -1;
    }
    if (connect_error == ENOENT) {
        (void)close(fd);
        return 0;
    }
    if (connect_error == ECONNREFUSED) {
        /* BSD-derived kernels may report ECONNREFUSED when the listen queue
         * is full. A validated owner-only socket path therefore fails closed
         * as active; treating it as stale could permit an in-place update of
         * a running daemon. */
        (void)close(fd);
        return 1;
    }
    if (connect_error == EAGAIN || connect_error == EWOULDBLOCK) {
        /* A secure listening socket whose accept queue is full is active. */
        (void)close(fd);
        return 1;
    }
    if (connect_error != EINPROGRESS) {
        (void)close(fd);
        return -1;
    }

    int ready = poll_until(fd, POLLOUT, ipc_deadline_after(timeout_ms));
    int socket_error = 0;
    socklen_t error_length = sizeof(socket_error);
    if (ready == 0) {
        /* Still pending against a validated local socket: conservatively
         * active, most commonly a saturated accept queue. */
        (void)close(fd);
        return 1;
    }
    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_length) != 0) {
        (void)close(fd);
        return -1;
    }
    if (socket_error == 0) {
        bool authenticated = socket_peer_is_current_user(fd);
        (void)close(fd);
        return authenticated ? 1 : -1;
    }
    (void)close(fd);
    if (socket_error == ENOENT) {
        return 0;
    }
    if (socket_error == ECONNREFUSED) {
        return 1;
    }
    if (socket_error == EAGAIN || socket_error == EWOULDBLOCK || socket_error == EINPROGRESS) {
        return 1;
    }
    return -1;
}

cbm_daemon_ipc_connection_t *cbm_daemon_ipc_connect(const cbm_daemon_ipc_endpoint_t *endpoint,
                                                    uint32_t timeout_ms) {
    if (!endpoint_runtime_still_valid(endpoint)) {
        return NULL;
    }
    uint64_t deadline_ms = ipc_deadline_after(timeout_ms);
    for (;;) {
        struct stat status;
        if (fstatat(endpoint->dir_fd, endpoint->socket_name, &status, AT_SYMLINK_NOFOLLOW) != 0) {
            int path_error = errno;
            if (path_error == ENOENT && ipc_retry_pause(deadline_ms)) {
                continue;
            }
            return NULL;
        }
        if (!posix_socket_status_secure_transport(&status)) {
            return NULL;
        }

        int fd = local_socket_new();
        if (fd < 0) {
            return NULL;
        }
        struct sockaddr_un address;
        socklen_t address_length;
        if (!unix_address_set(&address, endpoint->address, &address_length)) {
            (void)close(fd);
            return NULL;
        }

        int result = local_socket_connect(fd, &address, address_length);
        int connect_error = result == 0 ? 0 : errno;
        if (result != 0 && connect_error != EINPROGRESS && connect_error != EAGAIN &&
            connect_error != EWOULDBLOCK) {
            (void)close(fd);
            if ((connect_error == ENOENT || connect_error == ECONNREFUSED) &&
                ipc_retry_pause(deadline_ms)) {
                continue;
            }
            return NULL;
        }
        if (result != 0) {
            int ready = poll_until(fd, POLLOUT, deadline_ms);
            int socket_error = 0;
            socklen_t error_length = sizeof(socket_error);
            if (ready != 1 ||
                getsockopt(fd, SOL_SOCKET, SO_ERROR, &socket_error, &error_length) != 0) {
                (void)close(fd);
                return NULL;
            }
            if (socket_error != 0) {
                (void)close(fd);
                if ((socket_error == ENOENT || socket_error == ECONNREFUSED) &&
                    ipc_retry_pause(deadline_ms)) {
                    continue;
                }
                return NULL;
            }
        }
        if (!socket_peer_is_current_user(fd)) {
            (void)close(fd);
            return NULL;
        }

        cbm_daemon_ipc_connection_t *connection = malloc(sizeof(*connection));
        if (!connection) {
            (void)close(fd);
            return NULL;
        }
        connection->fd = fd;
        atomic_init(&connection->poisoned, false);
        return connection;
    }
}

void cbm_daemon_ipc_connection_close(cbm_daemon_ipc_connection_t *connection) {
    if (!connection) {
        return;
    }
    if (connection->fd >= 0) {
        (void)close(connection->fd);
    }
    free(connection);
}

void cbm_daemon_ipc_connection_drain(cbm_daemon_ipc_connection_t *connection, uint32_t timeout_ms) {
    /* POSIX stream sockets deliver buffered data after close; the final
     * response cannot be lost to an immediate close, so no wait is needed. */
    (void)connection;
    (void)timeout_ms;
}

void cbm_daemon_ipc_connection_interrupt(cbm_daemon_ipc_connection_t *connection) {
    if (connection && connection->fd >= 0) {
        (void)shutdown(connection->fd, SHUT_RDWR);
    }
}

uint64_t cbm_daemon_ipc_connection_peer_pid(const cbm_daemon_ipc_connection_t *connection) {
    if (!connection || connection->fd < 0 || !socket_peer_is_current_user(connection->fd)) {
        return 0;
    }
#if defined(__linux__)
    struct ucred credentials;
    socklen_t length = sizeof(credentials);
    if (getsockopt(connection->fd, SOL_SOCKET, SO_PEERCRED, &credentials, &length) != 0 ||
        length != sizeof(credentials) || credentials.uid != geteuid() || credentials.pid <= 0) {
        return 0;
    }
    return (uint64_t)credentials.pid;
#elif defined(SOL_LOCAL) && defined(LOCAL_PEERPID)
    pid_t peer_pid = 0;
    socklen_t length = sizeof(peer_pid);
    if (getsockopt(connection->fd, SOL_LOCAL, LOCAL_PEERPID, &peer_pid, &length) != 0 ||
        length != sizeof(peer_pid) || peer_pid <= 0) {
        return 0;
    }
    return (uint64_t)peer_pid;
#else
    return 0;
#endif
}

int cbm_daemon_ipc_startup_lock_try_acquire(const cbm_daemon_ipc_endpoint_t *endpoint,
                                            cbm_daemon_ipc_startup_lock_t **lock_out) {
    if (lock_out) {
        *lock_out = NULL;
    }
    if (!lock_out || !endpoint) {
        return -1;
    }
    int startup_v2_fd = -1;
    process_lock_entry_t *startup_v2_process_entry = NULL;
    int startup_v2_result = posix_named_lock_try_acquire(endpoint, endpoint->startup_v2_lock_name,
                                                         &startup_v2_fd, &startup_v2_process_entry);
    if (startup_v2_result != 1) {
        return startup_v2_result;
    }
    int legacy_fd = -1;
    process_lock_entry_t *legacy_process_entry = NULL;
    int legacy_result = posix_named_shared_lock_try_acquire(endpoint, endpoint->lock_name,
                                                            &legacy_fd, &legacy_process_entry);
    if (legacy_result != 1) {
        posix_named_lock_release(startup_v2_fd, startup_v2_process_entry);
        return legacy_result;
    }
    cbm_daemon_ipc_startup_lock_t *lock = calloc(1, sizeof(*lock));
    if (lock) {
        lock->endpoint_snapshot = endpoint_snapshot_new(endpoint);
    }
    if (!lock || !lock->endpoint_snapshot) {
        if (lock) {
            cbm_daemon_ipc_endpoint_free(lock->endpoint_snapshot);
        }
        free(lock);
        posix_named_lock_release(legacy_fd, legacy_process_entry);
        posix_named_lock_release(startup_v2_fd, startup_v2_process_entry);
        return -1;
    }
    lock->startup_v2_fd = startup_v2_fd;
    lock->startup_v2_process_entry = startup_v2_process_entry;
    lock->legacy_fd = legacy_fd;
    lock->legacy_process_entry = legacy_process_entry;
    lock->owner_pid = getpid();
    *lock_out = lock;
    return 1;
}

int cbm_daemon_ipc_generation_probe_under_startup_lock(
    const cbm_daemon_ipc_endpoint_t *endpoint, const cbm_daemon_ipc_startup_lock_t *startup_lock) {
    if (!posix_startup_lock_matches_endpoint(endpoint, startup_lock) || startup_lock->prepared) {
        return -1;
    }
    int lifetime = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
    if (lifetime != 0) {
        return lifetime;
    }
    return cbm_daemon_ipc_endpoint_probe(endpoint, 0);
}

bool cbm_daemon_ipc_startup_lock_prepare_handoff(cbm_daemon_ipc_startup_lock_t *lock) {
    if (!lock || !lock->endpoint_snapshot) {
        return false;
    }
    if (lock->prepared) {
        return true;
    }
    lock->prepared = cbm_daemon_ipc_stale_generation_cleanup(lock->endpoint_snapshot, lock) == 1;
    return lock->prepared;
}

bool cbm_daemon_ipc_startup_lock_release(cbm_daemon_ipc_startup_lock_t **lock_io) {
    if (!lock_io) {
        return false;
    }
    cbm_daemon_ipc_startup_lock_t *lock = *lock_io;
    if (!lock) {
        return true;
    }
    if (lock->owner_pid != getpid()) {
        return false;
    }
    posix_named_lock_release(lock->legacy_fd, lock->legacy_process_entry);
    posix_named_lock_release(lock->startup_v2_fd, lock->startup_v2_process_entry);
    cbm_daemon_ipc_endpoint_free(lock->endpoint_snapshot);
    free(lock);
    *lock_io = NULL;
    return true;
}

int cbm_daemon_ipc_participant_guard_try_join(const cbm_daemon_ipc_endpoint_t *endpoint,
                                              cbm_daemon_ipc_participant_guard_t **guard_out) {
    if (guard_out) {
        *guard_out = NULL;
    }
    if (!endpoint || !guard_out) {
        return -1;
    }
    int legacy_fd = -1;
    process_lock_entry_t *legacy_process_entry = NULL;
    int result = posix_named_shared_lock_try_acquire(endpoint, endpoint->lock_name, &legacy_fd,
                                                     &legacy_process_entry);
    if (result != 1) {
        return result;
    }
    cbm_daemon_ipc_participant_guard_t *guard = calloc(1, sizeof(*guard));
    if (!guard) {
        posix_named_lock_release(legacy_fd, legacy_process_entry);
        return -1;
    }
    guard->legacy_fd = legacy_fd;
    guard->legacy_process_entry = legacy_process_entry;
    guard->owner_pid = getpid();
    *guard_out = guard;
    return 1;
}

bool cbm_daemon_ipc_participant_guard_release(cbm_daemon_ipc_participant_guard_t **guard_io) {
    if (!guard_io) {
        return false;
    }
    cbm_daemon_ipc_participant_guard_t *guard = *guard_io;
    if (!guard) {
        return true;
    }
    if (guard->legacy_fd < 0 || !guard->legacy_process_entry || guard->owner_pid != getpid()) {
        return false;
    }
    posix_named_lock_release(guard->legacy_fd, guard->legacy_process_entry);
    free(guard);
    *guard_io = NULL;
    return true;
}

int cbm_daemon_ipc_local_transition_try_acquire(
    const cbm_daemon_ipc_endpoint_t *endpoint, cbm_daemon_ipc_local_transition_t **transition_out) {
    if (transition_out) {
        *transition_out = NULL;
    }
    if (!endpoint || !transition_out) {
        return -1;
    }
    int startup_v2_fd = -1;
    process_lock_entry_t *startup_v2_process_entry = NULL;
    int startup_v2_result = posix_named_lock_try_acquire(endpoint, endpoint->startup_v2_lock_name,
                                                         &startup_v2_fd, &startup_v2_process_entry);
    if (startup_v2_result != 1) {
        return startup_v2_result;
    }
    int legacy_fd = -1;
    process_lock_entry_t *legacy_process_entry = NULL;
    int legacy_result = posix_named_shared_lock_try_acquire(endpoint, endpoint->lock_name,
                                                            &legacy_fd, &legacy_process_entry);
    if (legacy_result != 1) {
        posix_named_lock_release(startup_v2_fd, startup_v2_process_entry);
        return legacy_result;
    }
    cbm_daemon_ipc_local_transition_t *transition = calloc(1, sizeof(*transition));
    if (!transition) {
        posix_named_lock_release(legacy_fd, legacy_process_entry);
        posix_named_lock_release(startup_v2_fd, startup_v2_process_entry);
        return -1;
    }
    transition->startup_v2_fd = startup_v2_fd;
    transition->startup_v2_process_entry = startup_v2_process_entry;
    transition->legacy_fd = legacy_fd;
    transition->legacy_process_entry = legacy_process_entry;
    transition->endpoint = endpoint;
    transition->owner_pid = getpid();
    *transition_out = transition;
    return 1;
}

int cbm_daemon_ipc_local_transition_seal_legacy(cbm_daemon_ipc_local_transition_t *transition) {
    if (!transition || !transition->endpoint ||
        !posix_named_lock_is_retained(
            transition->endpoint, transition->endpoint->startup_v2_lock_name, false,
            transition->startup_v2_fd, transition->startup_v2_process_entry,
            transition->owner_pid) ||
        !posix_named_lock_is_retained(transition->endpoint, transition->endpoint->lock_name, true,
                                      transition->legacy_fd, transition->legacy_process_entry,
                                      transition->owner_pid) ||
        transition->work_begun) {
        return -1;
    }
    if (transition->sealed) {
        return 1;
    }
    int lifetime = cbm_daemon_ipc_lifetime_reservation_probe(transition->endpoint);
    int result = lifetime == 1   ? 1
                 : lifetime == 0 ? posix_stale_generation_cleanup_locked(transition->endpoint)
                                 : -1;
    if (result == 1) {
        transition->sealed = true;
    }
    return result;
}

int cbm_daemon_ipc_local_transition_lifetime_probe(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_ipc_local_transition_t *transition) {
    if (!transition || !transition->sealed || !transition->endpoint || transition->work_begun ||
        !posix_endpoint_namespace_equal(endpoint, transition->endpoint) ||
        !posix_named_lock_is_retained(
            endpoint, endpoint->startup_v2_lock_name, false, transition->startup_v2_fd,
            transition->startup_v2_process_entry, transition->owner_pid) ||
        !posix_named_lock_is_retained(endpoint, endpoint->lock_name, true, transition->legacy_fd,
                                      transition->legacy_process_entry, transition->owner_pid)) {
        return -1;
    }
    return cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
}

bool cbm_daemon_ipc_local_transition_begin_work(cbm_daemon_ipc_local_transition_t *transition) {
    if (!transition || !transition->sealed || transition->work_begun || !transition->endpoint ||
        !posix_named_lock_is_retained(
            transition->endpoint, transition->endpoint->startup_v2_lock_name, false,
            transition->startup_v2_fd, transition->startup_v2_process_entry,
            transition->owner_pid) ||
        !posix_named_lock_is_retained(transition->endpoint, transition->endpoint->lock_name, true,
                                      transition->legacy_fd, transition->legacy_process_entry,
                                      transition->owner_pid)) {
        return false;
    }
    posix_named_lock_release(transition->startup_v2_fd, transition->startup_v2_process_entry);
    transition->startup_v2_fd = -1;
    transition->startup_v2_process_entry = NULL;
    transition->work_begun = true;
    return true;
}

bool cbm_daemon_ipc_local_transition_release(cbm_daemon_ipc_local_transition_t **transition_io) {
    if (!transition_io) {
        return false;
    }
    cbm_daemon_ipc_local_transition_t *transition = *transition_io;
    if (!transition) {
        return true;
    }
    if (transition->legacy_fd < 0 || !transition->legacy_process_entry ||
        transition->owner_pid != getpid() ||
        (!transition->work_begun &&
         (transition->startup_v2_fd < 0 || !transition->startup_v2_process_entry))) {
        return false;
    }
    posix_named_lock_release(transition->legacy_fd, transition->legacy_process_entry);
    if (!transition->work_begun) {
        posix_named_lock_release(transition->startup_v2_fd, transition->startup_v2_process_entry);
    }
    free(transition);
    *transition_io = NULL;
    return true;
}

#else /* _WIN32 */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <aclapi.h>
#include <sddl.h>
#include <fcntl.h>
#include <io.h>
#include <shlobj.h>
#include <wchar.h>

#ifndef PIPE_REJECT_REMOTE_CLIENTS
#define PIPE_REJECT_REMOTE_CLIENTS 0x00000008
#endif
#ifndef PROCESS_QUERY_LIMITED_INFORMATION
#define PROCESS_QUERY_LIMITED_INFORMATION 0x1000
#endif

typedef BOOL(WINAPI *open_process_token_fn)(HANDLE, DWORD, PHANDLE);
typedef BOOL(WINAPI *open_thread_token_fn)(HANDLE, DWORD, BOOL, PHANDLE);
typedef BOOL(WINAPI *get_token_information_fn)(HANDLE, TOKEN_INFORMATION_CLASS, LPVOID, DWORD,
                                               PDWORD);
typedef DWORD(WINAPI *get_length_sid_fn)(PSID);
typedef BOOL(WINAPI *copy_sid_fn)(DWORD, PSID, PSID);
typedef BOOL(WINAPI *equal_sid_fn)(PSID, PSID);
typedef BOOL(WINAPI *is_valid_sid_fn)(PSID);
typedef BOOL(WINAPI *is_well_known_sid_fn)(PSID, WELL_KNOWN_SID_TYPE);
typedef BOOL(WINAPI *is_valid_acl_fn)(PACL);
typedef BOOL(WINAPI *initialize_acl_fn)(PACL, DWORD, DWORD);
typedef BOOL(WINAPI *add_access_allowed_ace_fn)(PACL, DWORD, DWORD, PSID);
typedef BOOL(WINAPI *initialize_security_descriptor_fn)(PSECURITY_DESCRIPTOR, DWORD);
typedef BOOL(WINAPI *set_security_descriptor_dacl_fn)(PSECURITY_DESCRIPTOR, BOOL, PACL, BOOL);
typedef BOOL(WINAPI *set_security_descriptor_owner_fn)(PSECURITY_DESCRIPTOR, PSID, BOOL);
typedef BOOL(WINAPI *get_acl_information_fn)(PACL, LPVOID, DWORD, ACL_INFORMATION_CLASS);
typedef BOOL(WINAPI *get_ace_fn)(PACL, DWORD, LPVOID *);
typedef DWORD(WINAPI *get_security_info_fn)(HANDLE, SE_OBJECT_TYPE, SECURITY_INFORMATION, PSID *,
                                            PSID *, PACL *, PACL *, PSECURITY_DESCRIPTOR *);
typedef DWORD(WINAPI *set_security_info_fn)(HANDLE, SE_OBJECT_TYPE, SECURITY_INFORMATION, PSID,
                                            PSID, PACL, PACL);
typedef BOOL(WINAPI *impersonate_named_pipe_client_fn)(HANDLE);
typedef BOOL(WINAPI *revert_to_self_fn)(void);
typedef BOOL(WINAPI *get_named_pipe_client_process_id_fn)(HANDLE, PULONG);
typedef BOOL(WINAPI *get_named_pipe_server_process_id_fn)(HANDLE, PULONG);

typedef struct {
    HMODULE advapi;
    open_process_token_fn open_process_token;
    open_thread_token_fn open_thread_token;
    get_token_information_fn get_token_information;
    get_length_sid_fn get_length_sid;
    copy_sid_fn copy_sid;
    equal_sid_fn equal_sid;
    is_valid_sid_fn is_valid_sid;
    is_well_known_sid_fn is_well_known_sid;
    is_valid_acl_fn is_valid_acl;
    initialize_acl_fn initialize_acl;
    add_access_allowed_ace_fn add_access_allowed_ace;
    initialize_security_descriptor_fn initialize_security_descriptor;
    set_security_descriptor_dacl_fn set_security_descriptor_dacl;
    set_security_descriptor_owner_fn set_security_descriptor_owner;
    get_acl_information_fn get_acl_information;
    get_ace_fn get_ace;
    get_security_info_fn get_security_info;
    set_security_info_fn set_security_info;
    impersonate_named_pipe_client_fn impersonate_named_pipe_client;
    revert_to_self_fn revert_to_self;
    PSID user_sid;
    PACL acl;
    PSECURITY_DESCRIPTOR descriptor;
    SECURITY_ATTRIBUTES attributes;
} win_security_t;

typedef struct win_generation_address {
    char address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP];
    wchar_t *pipe_name;
    struct win_generation_address *next;
} win_generation_address_t;

typedef struct win_legacy_mutex_guard win_legacy_mutex_guard_t;

struct cbm_daemon_ipc_lifetime_reservation {
    const struct cbm_daemon_ipc_endpoint *endpoint;
    cbm_private_lock_directory_t *directory;
    cbm_private_file_lock_t *lock;
};

struct cbm_daemon_ipc_endpoint {
    char *runtime_dir;
    char instance_key[17];
    uint8_t *user_sid;
    size_t user_sid_length;
    wchar_t *legacy_pipe_name;
    wchar_t *legacy_startup_mutex_name;
    cbm_mutex_t generations_lock;
    _Atomic(win_generation_address_t *) current_generation;
    win_generation_address_t *generations;
};

struct cbm_daemon_ipc_listener {
    wchar_t *pipe_name;
    HANDLE pipe;
    bool first_instance;
    /* The pending overlapped ConnectNamedPipe persists across accept-poll
     * timeouts. Destroying a listening instance on every poll timeout races
     * clients attaching in the teardown window: they end up severed or,
     * worse, attached to an orphaned pipe object no server will ever read,
     * silently absorbing their HELLO until their own timeout expires. */
    HANDLE connect_event;
    OVERLAPPED connect_overlapped;
    bool connect_pending;
    cbm_daemon_ipc_lifetime_reservation_t *lifetime_reservation;
    cbm_daemon_ipc_participant_guard_t *participant_guard;
};

typedef enum {
    CBM_DAEMON_IPC_PIPE_ROLE_ACCEPTED_SERVER = 1,
    CBM_DAEMON_IPC_PIPE_ROLE_CONNECTED_CLIENT = 2,
} cbm_daemon_ipc_pipe_role_t;

struct cbm_daemon_ipc_connection {
    HANDLE handle;
    atomic_bool poisoned;
    cbm_daemon_ipc_pipe_role_t role;
};

struct cbm_daemon_ipc_startup_lock {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_private_lock_directory_t *directory;
    cbm_private_file_lock_t *startup_v2_lock;
    win_legacy_mutex_guard_t *legacy_guard;
    cbm_private_file_lock_t *group_lock;
    HANDLE legacy_sentinel;
    bool prepared;
};

struct cbm_daemon_ipc_local_transition {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_private_lock_directory_t *directory;
    cbm_private_file_lock_t *startup_v2_lock;
    cbm_private_file_lock_t *group_lock;
    HANDLE legacy_sentinel;
    win_legacy_mutex_guard_t *teardown_legacy_guard;
    bool sealed;
    bool work_begun;
};

struct cbm_daemon_ipc_participant_guard {
    const cbm_daemon_ipc_endpoint_t *endpoint;
    cbm_private_lock_directory_t *directory;
    cbm_private_file_lock_t *group_lock;
    HANDLE legacy_sentinel;
    cbm_private_file_lock_t *teardown_startup_v2_lock;
    win_legacy_mutex_guard_t *teardown_legacy_guard;
};

static uint64_t ipc_now_ms(void) {
    return (uint64_t)GetTickCount64();
}

static uint64_t ipc_deadline_after(uint32_t timeout_ms) {
    if (timeout_ms == CBM_DAEMON_IPC_WAIT_FOREVER) {
        return UINT64_MAX;
    }
    return ipc_now_ms() + (uint64_t)timeout_ms;
}

static _Noreturn void ipc_coordination_cleanup_fail_stop(const char *component) {
    cbm_log_error("daemon.forced_shutdown", "component", component, "action",
                  "coordination_cleanup");
    (void)fflush(stdout);
    (void)fflush(stderr);
    (void)TerminateProcess(GetCurrentProcess(), EXIT_FAILURE);
    abort();
}

static void ipc_startup_lock_release_complete(cbm_daemon_ipc_startup_lock_t **lock_io) {
    uint64_t deadline = ipc_deadline_after(CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS);
    while (lock_io && *lock_io) {
        (void)cbm_daemon_ipc_startup_lock_release(lock_io);
        if (!*lock_io) {
            return;
        }
        if (ipc_now_ms() >= deadline) {
            ipc_coordination_cleanup_fail_stop("startup_lock_cleanup");
        }
        Sleep(1);
    }
}

static void ipc_participant_guard_release_complete(cbm_daemon_ipc_participant_guard_t **guard_io) {
    uint64_t deadline = ipc_deadline_after(CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS);
    while (guard_io && *guard_io) {
        (void)cbm_daemon_ipc_participant_guard_release(guard_io);
        if (!*guard_io) {
            return;
        }
        if (ipc_now_ms() >= deadline) {
            ipc_coordination_cleanup_fail_stop("participant_guard_cleanup");
        }
        Sleep(1);
    }
}

static DWORD win_deadline_remaining(uint64_t deadline_ms) {
    if (deadline_ms == UINT64_MAX) {
        return INFINITE;
    }
    uint64_t now_ms = ipc_now_ms();
    if (now_ms >= deadline_ms) {
        return 0;
    }
    uint64_t remaining = deadline_ms - now_ms;
    return remaining > (uint64_t)(INFINITE - 1) ? INFINITE - 1 : (DWORD)remaining;
}

static bool win_retry_pause(uint64_t deadline_ms) {
    DWORD remaining_ms = win_deadline_remaining(deadline_ms);
    if (remaining_ms == 0) {
        return false;
    }
    DWORD pause_ms = remaining_ms < CBM_DAEMON_IPC_RETRY_INTERVAL_MS
                         ? remaining_ms
                         : CBM_DAEMON_IPC_RETRY_INTERVAL_MS;
    Sleep(pause_ms);
    return true;
}

static wchar_t *utf8_to_wide(const char *value) {
    if (!value) {
        return NULL;
    }
    int needed = MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, NULL, 0);
    if (needed <= 0) {
        return NULL;
    }
    wchar_t *wide = malloc((size_t)needed * sizeof(*wide));
    if (!wide || MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, value, -1, wide, needed) <= 0) {
        free(wide);
        return NULL;
    }
    return wide;
}

static char *wide_to_utf8(const wchar_t *value) {
    if (!value) {
        return NULL;
    }
    int needed = WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, value, -1, NULL, 0, NULL, NULL);
    if (needed <= 0) {
        return NULL;
    }
    char *utf8 = malloc((size_t)needed);
    if (!utf8 || WideCharToMultiByte(CP_UTF8, WC_ERR_INVALID_CHARS, value, -1, utf8, needed, NULL,
                                     NULL) <= 0) {
        free(utf8);
        return NULL;
    }
    return utf8;
}

static wchar_t *wide_copy(const wchar_t *value) {
    if (!value) {
        return NULL;
    }
    size_t length = wcslen(value);
    wchar_t *copy = malloc((length + 1) * sizeof(*copy));
    if (copy) {
        memcpy(copy, value, (length + 1) * sizeof(*copy));
    }
    return copy;
}

static void win_security_destroy(win_security_t *security) {
    if (!security) {
        return;
    }
    free(security->descriptor);
    free(security->acl);
    free(security->user_sid);
    if (security->advapi) {
        (void)FreeLibrary(security->advapi);
    }
    memset(security, 0, sizeof(*security));
}

static void *win_token_user_query(win_security_t *security, HANDLE token, PSID *sid_out) {
    DWORD needed = 0;
    (void)security->get_token_information(token, TokenUser, NULL, 0, &needed);
    if (needed == 0 || GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        return NULL;
    }
    /* The dynamically-resolved GetTokenInformation call initializes this
     * buffer on success.  Zero-initialize it as defense in depth and so
     * static analysis does not have to infer writes through a function
     * pointer before TOKEN_USER is inspected. */
    void *buffer = calloc(1, needed);
    if (!buffer || !security->get_token_information(token, TokenUser, buffer, needed, &needed)) {
        free(buffer);
        return NULL;
    }
    *sid_out = ((TOKEN_USER *)buffer)->User.Sid;
    return buffer;
}

#define RESOLVE_ADVAPI_MEMBER(context, member, type, symbol)                   \
    do {                                                                       \
        (context)->member = (type)GetProcAddress((context)->advapi, (symbol)); \
        if (!(context)->member) {                                              \
            win_security_destroy((context));                                   \
            return false;                                                      \
        }                                                                      \
    } while (0)

static bool win_security_init(win_security_t *security) {
    memset(security, 0, sizeof(*security));
    security->advapi = LoadLibraryW(L"advapi32.dll");
    if (!security->advapi) {
        return false;
    }
    RESOLVE_ADVAPI_MEMBER(security, open_process_token, open_process_token_fn, "OpenProcessToken");
    RESOLVE_ADVAPI_MEMBER(security, open_thread_token, open_thread_token_fn, "OpenThreadToken");
    RESOLVE_ADVAPI_MEMBER(security, get_token_information, get_token_information_fn,
                          "GetTokenInformation");
    RESOLVE_ADVAPI_MEMBER(security, get_length_sid, get_length_sid_fn, "GetLengthSid");
    RESOLVE_ADVAPI_MEMBER(security, copy_sid, copy_sid_fn, "CopySid");
    RESOLVE_ADVAPI_MEMBER(security, equal_sid, equal_sid_fn, "EqualSid");
    RESOLVE_ADVAPI_MEMBER(security, is_valid_sid, is_valid_sid_fn, "IsValidSid");
    RESOLVE_ADVAPI_MEMBER(security, is_well_known_sid, is_well_known_sid_fn, "IsWellKnownSid");
    RESOLVE_ADVAPI_MEMBER(security, is_valid_acl, is_valid_acl_fn, "IsValidAcl");
    RESOLVE_ADVAPI_MEMBER(security, initialize_acl, initialize_acl_fn, "InitializeAcl");
    RESOLVE_ADVAPI_MEMBER(security, add_access_allowed_ace, add_access_allowed_ace_fn,
                          "AddAccessAllowedAce");
    RESOLVE_ADVAPI_MEMBER(security, initialize_security_descriptor,
                          initialize_security_descriptor_fn, "InitializeSecurityDescriptor");
    RESOLVE_ADVAPI_MEMBER(security, set_security_descriptor_dacl, set_security_descriptor_dacl_fn,
                          "SetSecurityDescriptorDacl");
    RESOLVE_ADVAPI_MEMBER(security, set_security_descriptor_owner, set_security_descriptor_owner_fn,
                          "SetSecurityDescriptorOwner");
    RESOLVE_ADVAPI_MEMBER(security, get_acl_information, get_acl_information_fn,
                          "GetAclInformation");
    RESOLVE_ADVAPI_MEMBER(security, get_ace, get_ace_fn, "GetAce");
    RESOLVE_ADVAPI_MEMBER(security, get_security_info, get_security_info_fn, "GetSecurityInfo");
    RESOLVE_ADVAPI_MEMBER(security, set_security_info, set_security_info_fn, "SetSecurityInfo");
    RESOLVE_ADVAPI_MEMBER(security, impersonate_named_pipe_client, impersonate_named_pipe_client_fn,
                          "ImpersonateNamedPipeClient");
    RESOLVE_ADVAPI_MEMBER(security, revert_to_self, revert_to_self_fn, "RevertToSelf");

    HANDLE token = NULL;
    if (!security->open_process_token(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        win_security_destroy(security);
        return false;
    }
    PSID token_sid = NULL;
    void *token_user = win_token_user_query(security, token, &token_sid);
    (void)CloseHandle(token);
    if (!token_user || !token_sid) {
        free(token_user);
        win_security_destroy(security);
        return false;
    }
    DWORD sid_length = security->get_length_sid(token_sid);
    security->user_sid = malloc(sid_length);
    if (sid_length == 0 || !security->user_sid ||
        !security->copy_sid(sid_length, security->user_sid, token_sid)) {
        free(token_user);
        win_security_destroy(security);
        return false;
    }
    free(token_user);

    size_t acl_size = sizeof(ACL) + sizeof(ACCESS_ALLOWED_ACE) - sizeof(DWORD) + sid_length;
    if (acl_size > MAXDWORD) {
        win_security_destroy(security);
        return false;
    }
    security->acl = malloc(acl_size);
    security->descriptor = malloc(SECURITY_DESCRIPTOR_MIN_LENGTH);
    if (!security->acl || !security->descriptor ||
        !security->initialize_acl(security->acl, (DWORD)acl_size, ACL_REVISION) ||
        !security->add_access_allowed_ace(security->acl, ACL_REVISION, GENERIC_ALL,
                                          security->user_sid) ||
        !security->initialize_security_descriptor(security->descriptor,
                                                  SECURITY_DESCRIPTOR_REVISION) ||
        !security->set_security_descriptor_dacl(security->descriptor, TRUE, security->acl, FALSE) ||
        /* The owner must be stamped explicitly at creation: members of the
         * Administrators group can carry a default-owner policy of BUILTIN\
         * Administrators (standard on Windows Server, including CI runners),
         * and every private-namespace validation demands the exact token-user
         * SID as owner. Relying on the token default makes the daemon reject
         * objects it created itself. */
        !security->set_security_descriptor_owner(security->descriptor, security->user_sid, FALSE)) {
        win_security_destroy(security);
        return false;
    }
    security->attributes.nLength = sizeof(security->attributes);
    security->attributes.lpSecurityDescriptor = security->descriptor;
    security->attributes.bInheritHandle = FALSE;
    return true;
}

#undef RESOLVE_ADVAPI_MEMBER

static bool win_kernel_mutex_current_user_only(win_security_t *security, HANDLE mutex) {
    if (!security || !mutex || mutex == INVALID_HANDLE_VALUE) {
        return false;
    }
    PSID owner = NULL;
    PACL dacl = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD status = security->get_security_info(
        mutex, SE_KERNEL_OBJECT, OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &owner,
        NULL, &dacl, NULL, &descriptor);
    ACL_SIZE_INFORMATION information;
    memset(&information, 0, sizeof(information));
    void *raw_ace = NULL;
    bool valid = status == ERROR_SUCCESS && owner &&
                 security->equal_sid(owner, security->user_sid) && dacl &&
                 security->get_acl_information(dacl, &information, sizeof(information),
                                               AclSizeInformation) &&
                 information.AceCount == 1 && security->get_ace(dacl, 0, &raw_ace) && raw_ace;
    if (valid) {
        const ACCESS_ALLOWED_ACE *ace = raw_ace;
        const uint8_t *ace_sid = (const uint8_t *)&ace->SidStart;
        size_t sid_offset = offsetof(ACCESS_ALLOWED_ACE, SidStart);
        size_t sid_capacity =
            ace->Header.AceSize >= sid_offset ? ace->Header.AceSize - sid_offset : 0;
        size_t sid_length = sid_capacity >= 8U ? 8U + (size_t)ace_sid[1] * 4U : 0;
        DWORD required = SYNCHRONIZE | MUTEX_MODIFY_STATE | READ_CONTROL;
        bool access_ok = (ace->Mask & GENERIC_ALL) != 0 || (ace->Mask & required) == required;
        valid = ace->Header.AceType == ACCESS_ALLOWED_ACE_TYPE && ace->Header.AceFlags == 0 &&
                access_ok && sid_length <= sid_capacity && windows_sid_valid(ace_sid, sid_length) &&
                security->equal_sid((PSID)ace_sid, security->user_sid);
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    return valid;
}

struct win_legacy_mutex_guard {
    HANDLE mutex;
    HANDLE ready_event;
    HANDLE stop_event;
    cbm_thread_t owner_thread;
    bool owner_started;
    atomic_bool release_ok;
    atomic_int acquire_result;
};

static void *win_legacy_mutex_owner(void *opaque) {
    win_legacy_mutex_guard_t *guard = opaque;
    DWORD wait_result = WaitForSingleObject(guard->mutex, 0);
    int acquire_result;
    if (wait_result == WAIT_OBJECT_0 || wait_result == WAIT_ABANDONED) {
        acquire_result = 1;
    } else {
        acquire_result = wait_result == WAIT_TIMEOUT ? 0 : -1;
    }
    atomic_store_explicit(&guard->acquire_result, acquire_result, memory_order_release);
    bool ready = SetEvent(guard->ready_event) != 0;
    bool release_ok = acquire_result != 1;
    if (acquire_result == 1) {
        DWORD stopped = WaitForSingleObject(guard->stop_event, INFINITE);
        release_ok = stopped == WAIT_OBJECT_0 && ReleaseMutex(guard->mutex) != 0;
    }
    if (!ready) {
        release_ok = false;
    }
    atomic_store_explicit(&guard->release_ok, release_ok, memory_order_release);
    return NULL;
}

static bool win_legacy_mutex_guard_release(win_legacy_mutex_guard_t **guard_io) {
    if (!guard_io || !*guard_io) {
        return true;
    }
    win_legacy_mutex_guard_t *guard = *guard_io;
    unsigned int failures = atomic_load_explicit(&g_windows_legacy_guard_release_failures_for_test,
                                                 memory_order_acquire);
    while (failures > 0) {
        if (atomic_compare_exchange_weak_explicit(&g_windows_legacy_guard_release_failures_for_test,
                                                  &failures, failures - 1U, memory_order_acq_rel,
                                                  memory_order_acquire)) {
            return false;
        }
    }
    bool stopped = !guard->owner_started || SetEvent(guard->stop_event) != 0;
    bool joined = !guard->owner_started || cbm_thread_join(&guard->owner_thread) == 0;
    bool released =
        stopped && joined && atomic_load_explicit(&guard->release_ok, memory_order_acquire);
    if (!joined) {
        return false;
    }
    if (guard->ready_event) {
        (void)CloseHandle(guard->ready_event);
    }
    if (guard->stop_event) {
        (void)CloseHandle(guard->stop_event);
    }
    if (guard->mutex) {
        (void)CloseHandle(guard->mutex);
    }
    free(guard);
    *guard_io = NULL;
    return released;
}

static void win_legacy_mutex_guard_release_complete(win_legacy_mutex_guard_t **guard_io) {
    uint64_t deadline = ipc_deadline_after(CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS);
    while (guard_io && *guard_io) {
        (void)win_legacy_mutex_guard_release(guard_io);
        if (!*guard_io) {
            return;
        }
        if (ipc_now_ms() >= deadline) {
            ipc_coordination_cleanup_fail_stop("legacy_mutex_cleanup");
        }
        Sleep(1);
    }
}

static int win_legacy_mutex_guard_try_acquire(const cbm_daemon_ipc_endpoint_t *endpoint,
                                              win_legacy_mutex_guard_t **guard_out) {
    if (guard_out) {
        *guard_out = NULL;
    }
    if (!endpoint || !endpoint->legacy_startup_mutex_name || !guard_out) {
        return -1;
    }
    win_security_t security;
    if (!win_security_init(&security)) {
        return -1;
    }
    SetLastError(ERROR_SUCCESS);
    HANDLE mutex = CreateMutexW(&security.attributes, FALSE, endpoint->legacy_startup_mutex_name);
    DWORD create_error = mutex ? GetLastError() : ERROR_GEN_FAILURE;
    bool secured = mutex && mutex != INVALID_HANDLE_VALUE;
    if (secured && create_error != ERROR_ALREADY_EXISTS) {
        secured = security.set_security_info(mutex, SE_KERNEL_OBJECT,
                                             OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
                                             security.user_sid, NULL, security.acl,
                                             NULL) == ERROR_SUCCESS;
    }
    secured = secured && SetHandleInformation(mutex, HANDLE_FLAG_INHERIT, 0) != 0 &&
              win_kernel_mutex_current_user_only(&security, mutex);
    win_security_destroy(&security);
    if (!secured) {
        if (mutex && mutex != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(mutex);
        }
        return -1;
    }

    win_legacy_mutex_guard_t *guard = calloc(1, sizeof(*guard));
    if (guard) {
        guard->mutex = mutex;
        guard->ready_event = CreateEventW(NULL, TRUE, FALSE, NULL);
        guard->stop_event = CreateEventW(NULL, TRUE, FALSE, NULL);
        atomic_init(&guard->release_ok, false);
        atomic_init(&guard->acquire_result, -1);
    }
    if (!guard || !guard->ready_event || !guard->stop_event ||
        cbm_thread_create(&guard->owner_thread, 0, win_legacy_mutex_owner, guard) != 0) {
        if (guard) {
            win_legacy_mutex_guard_release_complete(&guard);
        } else {
            (void)CloseHandle(mutex);
        }
        return -1;
    }
    guard->owner_started = true;
    DWORD ready = WaitForSingleObject(guard->ready_event, 5000);
    int result = ready == WAIT_OBJECT_0
                     ? atomic_load_explicit(&guard->acquire_result, memory_order_acquire)
                     : -1;
    if (result != 1) {
        win_legacy_mutex_guard_release_complete(&guard);
        return result;
    }
    *guard_out = guard;
    return 1;
}

typedef enum {
    WIN_RENDEZVOUS_ERROR = -1,
    WIN_RENDEZVOUS_ABSENT = 0,
    WIN_RENDEZVOUS_VALID = 1,
    WIN_RENDEZVOUS_BUSY = 2,
    WIN_RENDEZVOUS_CORRUPT = 3,
} win_rendezvous_status_t;

static win_rendezvous_status_t win_endpoint_refresh_rendezvous(
    const cbm_daemon_ipc_endpoint_t *endpoint);
static win_generation_address_t *win_endpoint_generation_snapshot(
    const cbm_daemon_ipc_endpoint_t *endpoint);
static int win_legacy_pipe_probe(const cbm_daemon_ipc_endpoint_t *endpoint);

static uint32_t win_sid_read_u32_le(const uint8_t *bytes) {
    return (uint32_t)bytes[0] | ((uint32_t)bytes[1] << 8U) | ((uint32_t)bytes[2] << 16U) |
           ((uint32_t)bytes[3] << 24U);
}

static bool win_sid_is_trusted_installer(const uint8_t *sid, size_t sid_length) {
    static const uint32_t subauthorities[] = {
        80U, 956008885U, 3418522649U, 1831038044U, 1853292631U, 2271478464U,
    };
    if (!windows_sid_valid(sid, sid_length) || sid[1] != 6U || sid[2] != 0U || sid[3] != 0U ||
        sid[4] != 0U || sid[5] != 0U || sid[6] != 0U || sid[7] != 5U) {
        return false;
    }
    for (size_t index = 0; index < sizeof(subauthorities) / sizeof(subauthorities[0]); index++) {
        if (win_sid_read_u32_le(sid + 8U + index * 4U) != subauthorities[index]) {
            return false;
        }
    }
    return true;
}

static bool win_sid_trusted(win_security_t *security, PSID sid) {
    if (!security || !sid || !security->is_valid_sid(sid)) {
        return false;
    }
    DWORD sid_length = security->get_length_sid(sid);
    return (sid_length > 0U && security->equal_sid(sid, security->user_sid)) ||
           security->is_well_known_sid(sid, WinLocalSystemSid) ||
           security->is_well_known_sid(sid, WinBuiltinAdministratorsSid) ||
           win_sid_is_trusted_installer((const uint8_t *)sid, (size_t)sid_length);
}

static bool win_bounded_sid_trusted(win_security_t *security, const uint8_t *sid,
                                    size_t sid_capacity, bool creator_owner_inherit_only) {
    if (!security || !sid || sid_capacity < 8U || sid[1] > 15U) {
        return false;
    }
    size_t sid_length = 8U + (size_t)sid[1] * 4U;
    return sid_length <= sid_capacity && windows_sid_valid(sid, sid_length) &&
           security->is_valid_sid((PSID)sid) &&
           security->get_length_sid((PSID)sid) == (DWORD)sid_length &&
           (win_sid_trusted(security, (PSID)sid) ||
            (creator_owner_inherit_only &&
             security->is_well_known_sid((PSID)sid, WinCreatorOwnerSid)) ||
            /* OWNER RIGHTS (S-1-3-4) modulates the rights of whoever OWNS the
             * object; the owner is separately validated as the exact current
             * user, so such an ACE only ever grants to us. Default Windows
             * profile/temp ACLs (and GitHub runner profiles) carry it, and
             * rejecting it locked real current-user directories out. */
            security->is_well_known_sid((PSID)sid, WinCreatorOwnerRightsSid));
}

static bool win_file_owner_secure(win_security_t *security, HANDLE file,
                                  bool require_current_user) {
    PSID owner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD status = security->get_security_info(file, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION,
                                               &owner, NULL, NULL, NULL, &descriptor);
    bool queried = status == ERROR_SUCCESS && owner && security->is_valid_sid(owner);
    bool secure =
        queried && (require_current_user ? security->equal_sid(owner, security->user_sid) != 0
                                         : win_sid_trusted(security, owner));
    if (!queried) {
        ipc_validation_detail_set("owner query failed (status %lu)", (unsigned long)status);
    } else if (!secure) {
        const char *owner_class = security->is_well_known_sid(owner, WinLocalSystemSid) ? "SYSTEM"
                                  : security->is_well_known_sid(owner, WinBuiltinAdministratorsSid)
                                      ? "Administrators"
                                      : "another account";
        ipc_validation_detail_set("owner is %s, %s required", owner_class,
                                  require_current_user ? "the exact user" : "a trusted identity");
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    return secure;
}

static DWORD win_private_mutation_rights(void) {
    return GENERIC_ALL | GENERIC_WRITE | FILE_WRITE_DATA | FILE_APPEND_DATA | FILE_ADD_FILE |
           FILE_ADD_SUBDIRECTORY | FILE_DELETE_CHILD | FILE_WRITE_EA | FILE_WRITE_ATTRIBUTES |
           DELETE | WRITE_DAC | WRITE_OWNER | ACCESS_SYSTEM_SECURITY;
}

static bool win_file_acl_secure(win_security_t *security, HANDLE file, DWORD mutation) {
    PACL dacl = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD status = security->get_security_info(file, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION,
                                               NULL, NULL, &dacl, NULL, &descriptor);
    ACL_SIZE_INFORMATION information;
    memset(&information, 0, sizeof(information));
    bool secure =
        status == ERROR_SUCCESS && descriptor && dacl && security->is_valid_acl(dacl) &&
        security->get_acl_information(dacl, &information, sizeof(information), AclSizeInformation);
    enum {
        WIN_FILE_ACE_ALLOW = 0x00,
        WIN_FILE_ACE_DENY = 0x01,
        WIN_FILE_ACE_DENY_OBJECT = 0x06,
        WIN_FILE_ACE_DENY_CALLBACK = 0x0a,
        WIN_FILE_ACE_DENY_CALLBACK_OBJECT = 0x0c,
    };
    for (DWORD index = 0U; secure && index < information.AceCount; index++) {
        void *opaque = NULL;
        if (!security->get_ace(dacl, index, &opaque) || !opaque) {
            secure = false;
            break;
        }
        ACE_HEADER *header = opaque;
        if (header->AceType == WIN_FILE_ACE_DENY || header->AceType == WIN_FILE_ACE_DENY_OBJECT ||
            header->AceType == WIN_FILE_ACE_DENY_CALLBACK ||
            header->AceType == WIN_FILE_ACE_DENY_CALLBACK_OBJECT) {
            continue;
        }
        size_t sid_offset = offsetof(ACCESS_ALLOWED_ACE, SidStart);
        if (header->AceType != WIN_FILE_ACE_ALLOW || (size_t)header->AceSize < sid_offset + 8U) {
            secure = false;
            break;
        }
        const ACCESS_ALLOWED_ACE *ace = opaque;
        if ((ace->Mask & mutation) == 0U) {
            continue;
        }
        const uint8_t *sid = (const uint8_t *)&ace->SidStart;
        size_t sid_capacity = (size_t)header->AceSize - sid_offset;
        bool creator_owner_inherit_only = (header->AceFlags & INHERIT_ONLY_ACE) != 0U;
        if (!win_bounded_sid_trusted(security, sid, sid_capacity, creator_owner_inherit_only)) {
            /* Name the untrusted identity class so a harness/profile ACL leak
             * (an inherited Users / Authenticated Users / Everyone ACE) is
             * distinguishable from a genuinely hostile grant. */
            const char *sid_class =
                security->is_well_known_sid((PSID)sid, WinBuiltinUsersSid) ? "BUILTIN\\Users"
                : security->is_well_known_sid((PSID)sid, WinAuthenticatedUserSid)
                    ? "Authenticated Users"
                : security->is_well_known_sid((PSID)sid, WinWorldSid)       ? "Everyone"
                : security->is_well_known_sid((PSID)sid, WinInteractiveSid) ? "INTERACTIVE"
                                                                            : "other";
            /* Print the raw SID too: an "other" class is a specific account,
             * and only its string form identifies the harness/profile leak. */
            wchar_t *sid_text = NULL;
            char sid_utf8[96] = "<unprintable>";
            if (ConvertSidToStringSidW((PSID)sid, &sid_text) && sid_text) {
                (void)WideCharToMultiByte(CP_UTF8, 0, sid_text, -1, sid_utf8, (int)sizeof(sid_utf8),
                                          NULL, NULL);
                LocalFree(sid_text);
            }
            ipc_validation_detail_set(
                "DACL entry %lu grants mutation rights 0x%08lx to untrusted identity (%s %s)",
                (unsigned long)index, (unsigned long)(ace->Mask & mutation), sid_class, sid_utf8);
            secure = false;
        }
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    return secure;
}

static bool win_file_security_secure(win_security_t *security, HANDLE file,
                                     bool require_current_user, DWORD mutation) {
    return win_file_owner_secure(security, file, require_current_user) &&
           win_file_acl_secure(security, file, mutation);
}

static bool win_runtime_directory_secure(const wchar_t *runtime_dir) {
    win_security_t security;
    if (!win_security_init(&security)) {
        return false;
    }
    bool created = CreateDirectoryW(runtime_dir, &security.attributes) != 0;
    if (!created && GetLastError() != ERROR_ALREADY_EXISTS) {
        win_security_destroy(&security);
        return false;
    }
    DWORD attributes = GetFileAttributesW(runtime_dir);
    if (attributes == INVALID_FILE_ATTRIBUTES || (attributes & FILE_ATTRIBUTE_DIRECTORY) == 0 ||
        (attributes & FILE_ATTRIBUTE_REPARSE_POINT) != 0) {
        win_security_destroy(&security);
        return false;
    }
    HANDLE directory =
        CreateFileW(runtime_dir, READ_CONTROL | WRITE_DAC | WRITE_OWNER,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    bool can_write_owner = directory != INVALID_HANDLE_VALUE;
    if (!can_write_owner) {
        directory =
            CreateFileW(runtime_dir, READ_CONTROL | WRITE_DAC,
                        FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                        FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    }
    if (directory == INVALID_HANDLE_VALUE) {
        win_security_destroy(&security);
        return false;
    }
    BY_HANDLE_FILE_INFORMATION file_info;
    bool valid_handle = GetFileInformationByHandle(directory, &file_info) != 0 &&
                        (file_info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                        (file_info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
    bool owner_exact = valid_handle && win_file_owner_secure(&security, directory, true);
    /* One-time normalization of the admin-group default-owner artifact: a
     * directory created by plain mkdir under an Administrators-default-owner
     * token (standard policy on Windows Server) is born owned by BUILTIN\
     * Administrators even though it is this account's own private dir. A
     * TRUSTED owner (the launcher's directory policy: SYSTEM, Administrators,
     * TrustedInstaller) is re-stamped to the exact token user inside the same
     * repair that already re-protects the DACL; any other owner remains
     * refused, and the final validation below still demands the exact user. */
    bool owner_ok = owner_exact || (valid_handle && can_write_owner &&
                                    win_file_owner_secure(&security, directory, false));
    DWORD secure_result = ERROR_ACCESS_DENIED;
    if (valid_handle && owner_ok) {
        secure_result = security.set_security_info(
            directory, SE_FILE_OBJECT,
            (owner_exact ? 0U : (DWORD)OWNER_SECURITY_INFORMATION) | DACL_SECURITY_INFORMATION |
                PROTECTED_DACL_SECURITY_INFORMATION,
            owner_exact ? NULL : security.user_sid, NULL, security.acl, NULL);
    }
    if (valid_handle && owner_ok && secure_result != ERROR_SUCCESS) {
        ipc_validation_detail_set("owner/DACL repair failed (status %lu%s)",
                                  (unsigned long)secure_result,
                                  can_write_owner ? "" : ", WRITE_OWNER unavailable");
    }
    bool final_private =
        secure_result == ERROR_SUCCESS &&
        win_file_security_secure(&security, directory, true, win_private_mutation_rights());
    (void)CloseHandle(directory);
    win_security_destroy(&security);
    return valid_handle && owner_ok && final_private;
}

static bool win_directory_component_secure(win_security_t *security, const wchar_t *path) {
    HANDLE directory =
        CreateFileW(path, FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (directory == INVALID_HANDLE_VALUE) {
        return false;
    }
    BY_HANDLE_FILE_INFORMATION info;
    /* Default Windows profile ancestors grant cross-account add-subdirectory.
     * That permits siblings but cannot replace the existing next path
     * component. Keep every other mutation right forbidden; the final runtime
     * directory is separately owner-validated and given a protected DACL. */
    DWORD mutation = win_private_mutation_rights() & ~((DWORD)FILE_ADD_SUBDIRECTORY);
    bool valid = GetFileInformationByHandle(directory, &info) != 0 &&
                 (info.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                 (info.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
                 win_file_security_secure(security, directory, false, mutation);
    (void)CloseHandle(directory);
    return valid;
}

static bool win_private_directory_tree_secure(const wchar_t *directory_path) {
    if (!directory_path) {
        return false;
    }
    size_t length = wcslen(directory_path);
    bool drive_absolute = length >= 4 &&
                          ((directory_path[0] >= L'A' && directory_path[0] <= L'Z') ||
                           (directory_path[0] >= L'a' && directory_path[0] <= L'z')) &&
                          directory_path[1] == L':' &&
                          (directory_path[2] == L'\\' || directory_path[2] == L'/');
    if (!drive_absolute) {
        /* Local current-user ACLs do not provide the intended guarantee for
         * UNC/device namespaces. Daemon cache logs must stay on a local
         * absolute drive path. */
        return false;
    }
    wchar_t *path = wide_copy(directory_path);
    if (!path) {
        return false;
    }
    for (size_t i = 0; i < length; i++) {
        if (path[i] == L'/') {
            path[i] = L'\\';
        }
    }
    win_security_t security;
    if (!win_security_init(&security)) {
        free(path);
        return false;
    }
    bool ok = true;
    size_t component_start = 3;
    for (size_t i = component_start; ok && i <= length; i++) {
        if (i < length && path[i] != L'\\') {
            continue;
        }
        if (i == component_start) {
            component_start = i + 1;
            continue;
        }
        wchar_t saved = path[i];
        path[i] = L'\0';
        const wchar_t *component = path + component_start;
        if (wcscmp(component, L".") == 0 || wcscmp(component, L"..") == 0) {
            ok = false;
        } else {
            DWORD attributes = GetFileAttributesW(path);
            if (attributes == INVALID_FILE_ATTRIBUTES) {
                DWORD error = GetLastError();
                ok = (error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND) &&
                     CreateDirectoryW(path, &security.attributes) != 0;
            }
            /* Ancestors are observe-only and must already be secure.  The
             * final current-user directory is intentionally handled below by
             * win_runtime_directory_secure(), which may replace its DACL. */
            if (ok && i < length) {
                ok = win_directory_component_secure(&security, path);
                if (!ok) {
                    /* Name the ancestry component while it is NUL-terminated
                     * at this walk position; the helper set the inner rule. */
                    char inner[384];
                    (void)snprintf(inner, sizeof(inner), "%s", ipc_validation_detail_buffer);
                    char *component_utf8 = wide_to_utf8(path);
                    ipc_validation_detail_set(
                        "%s: %s", component_utf8 ? component_utf8 : "<component>", inner);
                    free(component_utf8);
                }
            }
        }
        path[i] = saved;
        component_start = i + 1;
    }
    win_security_destroy(&security);
    if (ok) {
        ok = win_runtime_directory_secure(path);
    }
    free(path);
    return ok;
}

bool cbm_daemon_ipc_private_directory_secure(const char *directory_path) {
    ipc_validation_detail_set("%s", "");
    if (!directory_path || !directory_path[0]) {
        return false;
    }
    wchar_t *wide_directory = utf8_to_wide(directory_path);
    bool secure = wide_directory && win_private_directory_tree_secure(wide_directory);
    free(wide_directory);
    return secure;
}

static bool private_log_base_name_valid(const char *base_name) {
    if (!base_name || !base_name[0] || strcmp(base_name, ".") == 0 ||
        strcmp(base_name, "..") == 0 || strchr(base_name, '/') || strchr(base_name, '\\')) {
        return false;
    }
    return strlen(base_name) <= 253;
}

static HANDLE win_private_log_file_open(const wchar_t *path, DWORD creation_disposition,
                                        win_security_t *security, LARGE_INTEGER *size_out) {
    HANDLE file = CreateFileW(path, GENERIC_READ | GENERIC_WRITE | READ_CONTROL | WRITE_DAC,
                              FILE_SHARE_READ, &security->attributes, creation_disposition,
                              FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (file == INVALID_HANDLE_VALUE) {
        return INVALID_HANDLE_VALUE;
    }
    (void)SetHandleInformation(file, HANDLE_FLAG_INHERIT, 0);
    BY_HANDLE_FILE_INFORMATION info;
    PSID owner = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    bool valid =
        GetFileType(file) == FILE_TYPE_DISK && GetFileInformationByHandle(file, &info) != 0 &&
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
        info.nNumberOfLinks == 1 && GetFileSizeEx(file, size_out) != 0 && size_out->QuadPart >= 0;
    DWORD owner_result = security->get_security_info(
        file, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION, &owner, NULL, NULL, NULL, &descriptor);
    bool owner_ok =
        owner_result == ERROR_SUCCESS && owner && security->equal_sid(owner, security->user_sid);
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    DWORD secure_result = ERROR_ACCESS_DENIED;
    if (valid && owner_ok) {
        secure_result = security->set_security_info(
            file, SE_FILE_OBJECT, DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
            NULL, NULL, security->acl, NULL);
    }
    if (!valid || !owner_ok || secure_result != ERROR_SUCCESS) {
        (void)CloseHandle(file);
        return INVALID_HANDLE_VALUE;
    }
    return file;
}

FILE *cbm_daemon_ipc_private_log_open(const char *directory_path, const char *base_name,
                                      size_t rotate_cap_bytes) {
    if (!directory_path || !directory_path[0] || !private_log_base_name_valid(base_name) ||
        rotate_cap_bytes == 0) {
        return NULL;
    }
    wchar_t *wide_directory = utf8_to_wide(directory_path);
    char *path = string_format("%s/%s", directory_path, base_name);
    char *rotated_path = path ? string_format("%s.1", path) : NULL;
    wchar_t *wide_path = path ? utf8_to_wide(path) : NULL;
    wchar_t *wide_rotated = rotated_path ? utf8_to_wide(rotated_path) : NULL;
    if (!wide_directory || !path || !rotated_path || !wide_path || !wide_rotated ||
        !win_private_directory_tree_secure(wide_directory)) {
        free(wide_directory);
        free(path);
        free(rotated_path);
        free(wide_path);
        free(wide_rotated);
        return NULL;
    }
    free(wide_directory);

    win_security_t security;
    if (!win_security_init(&security)) {
        free(path);
        free(rotated_path);
        free(wide_path);
        free(wide_rotated);
        return NULL;
    }
    LARGE_INTEGER size;
    HANDLE file = win_private_log_file_open(wide_path, OPEN_ALWAYS, &security, &size);
    bool ok = file != INVALID_HANDLE_VALUE;
    if (ok && (uint64_t)size.QuadPart > (uint64_t)rotate_cap_bytes) {
        (void)CloseHandle(file);
        file = INVALID_HANDLE_VALUE;
        DWORD attributes = GetFileAttributesW(wide_rotated);
        DWORD attributes_error =
            attributes == INVALID_FILE_ATTRIBUTES ? GetLastError() : ERROR_SUCCESS;
        bool destination_ok =
            attributes == INVALID_FILE_ATTRIBUTES &&
            (attributes_error == ERROR_FILE_NOT_FOUND || attributes_error == ERROR_PATH_NOT_FOUND);
        if (attributes != INVALID_FILE_ATTRIBUTES) {
            LARGE_INTEGER destination_size;
            HANDLE destination = win_private_log_file_open(wide_rotated, OPEN_EXISTING, &security,
                                                           &destination_size);
            destination_ok = destination != INVALID_HANDLE_VALUE;
            if (destination != INVALID_HANDLE_VALUE) {
                (void)CloseHandle(destination);
            }
        }
        ok = destination_ok && MoveFileExW(wide_path, wide_rotated,
                                           MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) != 0;
        if (ok) {
            file = win_private_log_file_open(wide_path, OPEN_ALWAYS, &security, &size);
            ok = file != INVALID_HANDLE_VALUE;
        }
    }
    FILE *stream = NULL;
    if (ok) {
        LARGE_INTEGER end = {.QuadPart = 0};
        ok = SetFilePointerEx(file, end, NULL, FILE_END) != 0;
    }
    if (ok) {
        int fd = _open_osfhandle((intptr_t)file, _O_WRONLY | _O_APPEND | _O_BINARY);
        if (fd >= 0) {
            file = INVALID_HANDLE_VALUE;
            stream = _fdopen(fd, "ab");
            if (!stream) {
                (void)_close(fd);
            }
        }
    }
    if (file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(file);
    }
    win_security_destroy(&security);
    free(path);
    free(rotated_path);
    free(wide_path);
    free(wide_rotated);
    return stream;
}

static bool win_parent_valid(const wchar_t *parent) {
    DWORD attributes = GetFileAttributesW(parent);
    return attributes != INVALID_FILE_ATTRIBUTES && (attributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
           (attributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
}

static wchar_t *win_default_runtime_parent(void) {
    wchar_t path[MAX_PATH];
    HRESULT result = SHGetFolderPathW(NULL, CSIDL_LOCAL_APPDATA | CSIDL_FLAG_CREATE, NULL,
                                      SHGFP_TYPE_CURRENT, path);
    return SUCCEEDED(result) ? wide_copy(path) : NULL;
}

cbm_daemon_ipc_endpoint_t *cbm_daemon_ipc_endpoint_new(const char *instance_key,
                                                       const char *runtime_parent) {
    if (!instance_key_valid(instance_key)) {
        return NULL;
    }
    char *parent_utf8 = NULL;
    wchar_t *parent_wide = NULL;
    if (runtime_parent) {
        parent_utf8 = string_copy(runtime_parent);
        parent_wide = utf8_to_wide(runtime_parent);
    } else {
        parent_wide = win_default_runtime_parent();
        parent_utf8 = wide_to_utf8(parent_wide);
    }
    if (!parent_utf8 || !parent_wide) {
        free(parent_utf8);
        free(parent_wide);
        return NULL;
    }
    free(parent_wide);

    char canonical_parent[CBM_DAEMON_IPC_PATH_CAP];
    if (!cbm_canonical_path(parent_utf8, canonical_parent, sizeof(canonical_parent))) {
        free(parent_utf8);
        return NULL;
    }
    free(parent_utf8);
    parent_utf8 = string_copy(canonical_parent);
    parent_wide = utf8_to_wide(parent_utf8);
    if (!parent_utf8 || !parent_wide || !win_parent_valid(parent_wide)) {
        free(parent_utf8);
        free(parent_wide);
        return NULL;
    }
    free(parent_wide);

    size_t parent_length = strlen(parent_utf8);
    bool has_separator = parent_length > 0 && (parent_utf8[parent_length - 1] == '/' ||
                                               parent_utf8[parent_length - 1] == '\\');
    cbm_daemon_ipc_endpoint_t *endpoint = calloc(1, sizeof(*endpoint));
    if (!endpoint) {
        free(parent_utf8);
        return NULL;
    }
    win_security_t identity_security;
    if (!win_security_init(&identity_security)) {
        free(parent_utf8);
        free(endpoint);
        return NULL;
    }
    DWORD sid_length = identity_security.get_length_sid(identity_security.user_sid);
    bool identity_ok = sid_length > 0 &&
                       windows_sid_valid((const uint8_t *)identity_security.user_sid, sid_length);
    if (identity_ok) {
        endpoint->user_sid = identity_security.user_sid;
        endpoint->user_sid_length = sid_length;
        identity_security.user_sid = NULL;
    }
    win_security_destroy(&identity_security);
    if (!identity_ok) {
        free(parent_utf8);
        free(endpoint);
        return NULL;
    }
    char legacy_pipe[CBM_DAEMON_IPC_WINDOWS_NAME_CAP];
    char legacy_startup[CBM_DAEMON_IPC_WINDOWS_NAME_CAP];
    bool legacy_names_ok =
        cbm_daemon_ipc_windows_legacy_names(parent_utf8, instance_key, legacy_pipe, legacy_startup);
    endpoint->runtime_dir =
        string_format("%s%scbm-daemon-%s", parent_utf8, has_separator ? "" : "/", instance_key);
    endpoint->legacy_pipe_name = legacy_names_ok ? utf8_to_wide(legacy_pipe) : NULL;
    endpoint->legacy_startup_mutex_name = legacy_names_ok ? utf8_to_wide(legacy_startup) : NULL;
    (void)memcpy(endpoint->instance_key, instance_key, sizeof(endpoint->instance_key));
    cbm_mutex_init(&endpoint->generations_lock);
    atomic_init(&endpoint->current_generation, NULL);
    free(parent_utf8);
    wchar_t *runtime_wide = utf8_to_wide(endpoint->runtime_dir);
    if (!endpoint->runtime_dir || !runtime_wide || !legacy_names_ok ||
        !endpoint->legacy_pipe_name || !endpoint->legacy_startup_mutex_name ||
        !win_private_directory_tree_secure(runtime_wide)) {
        free(runtime_wide);
        cbm_daemon_ipc_endpoint_free(endpoint);
        return NULL;
    }
    free(runtime_wide);
    /* Construction never publishes or repairs rendezvous payload. Absence or
     * a partial/corrupt record leaves the endpoint safely unaddressed; only a
     * later startup-lock winner is permitted to publish a replacement. */
    (void)win_endpoint_refresh_rendezvous(endpoint);
    return endpoint;
}

void cbm_daemon_ipc_endpoint_free(cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint) {
        return;
    }
    win_generation_address_t *generation = endpoint->generations;
    while (generation) {
        win_generation_address_t *next = generation->next;
        free(generation->pipe_name);
        free(generation);
        generation = next;
    }
    cbm_mutex_destroy(&endpoint->generations_lock);
    free(endpoint->runtime_dir);
    free(endpoint->legacy_pipe_name);
    free(endpoint->legacy_startup_mutex_name);
    free(endpoint->user_sid);
    free(endpoint);
}

const char *cbm_daemon_ipc_endpoint_address(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint || win_endpoint_refresh_rendezvous(endpoint) != WIN_RENDEZVOUS_VALID) {
        return NULL;
    }
    win_generation_address_t *generation = win_endpoint_generation_snapshot(endpoint);
    return generation ? generation->address : NULL;
}

const char *cbm_daemon_ipc_endpoint_runtime_dir(const cbm_daemon_ipc_endpoint_t *endpoint) {
    return endpoint ? endpoint->runtime_dir : NULL;
}

cbm_private_file_lock_status_t cbm_daemon_ipc_private_lock_directory_new(
    const cbm_daemon_ipc_endpoint_t *endpoint, cbm_private_lock_directory_t **directory_out) {
    if (directory_out) {
        *directory_out = NULL;
    }
    if (!directory_out || !endpoint || !endpoint->runtime_dir) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    wchar_t *runtime_wide = utf8_to_wide(endpoint->runtime_dir);
    if (!runtime_wide) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    HANDLE directory =
        CreateFileW(runtime_wide, FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    free(runtime_wide);
    if (directory == INVALID_HANDLE_VALUE) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    (void)SetHandleInformation(directory, HANDLE_FLAG_INHERIT, 0);
    cbm_private_file_lock_status_t status =
        cbm_private_lock_directory_adopt_windows(directory, endpoint->runtime_dir, directory_out);
    if (status != CBM_PRIVATE_FILE_LOCK_OK) {
        (void)CloseHandle(directory);
    }
    return status;
}

static const char WIN_STARTUP_V2_LOCK_NAME[] = "cbm-startup-v2.lock";
static const char WIN_PARTICIPANT_GROUP_LOCK_NAME[] = "cbm-participant-group-v1.lock";
static const char WIN_LIFETIME_LOCK_NAME[] = "cbm-lifetime.lock";

static void win_private_lock_release_complete(cbm_private_file_lock_t **lock_io) {
    uint64_t deadline = ipc_deadline_after(CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS);
    while (lock_io && *lock_io) {
        (void)cbm_private_file_lock_release(lock_io);
        if (!*lock_io) {
            return;
        }
        if (ipc_now_ms() >= deadline) {
            ipc_coordination_cleanup_fail_stop("private_lock_cleanup");
        }
        Sleep(1);
    }
}

static win_generation_address_t *win_endpoint_generation_snapshot(
    const cbm_daemon_ipc_endpoint_t *endpoint) {
    return endpoint ? atomic_load_explicit(&endpoint->current_generation, memory_order_acquire)
                    : NULL;
}

static void win_endpoint_generation_invalidate(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (endpoint) {
        atomic_store_explicit(&((cbm_daemon_ipc_endpoint_t *)endpoint)->current_generation, NULL,
                              memory_order_release);
    }
}

static bool win_endpoint_generation_install(const cbm_daemon_ipc_endpoint_t *endpoint,
                                            const char *address) {
    if (!endpoint || !windows_pipe_address_valid(address)) {
        return false;
    }
    win_generation_address_t *snapshot = win_endpoint_generation_snapshot(endpoint);
    if (snapshot && strcmp(snapshot->address, address) == 0) {
        return true;
    }
    win_generation_address_t *candidate = calloc(1, sizeof(*candidate));
    if (candidate) {
        (void)memcpy(candidate->address, address, strlen(address) + 1U);
        candidate->pipe_name = utf8_to_wide(address);
    }
    if (!candidate || !candidate->pipe_name) {
        free(candidate ? candidate->pipe_name : NULL);
        free(candidate);
        return false;
    }

    cbm_daemon_ipc_endpoint_t *mutable_endpoint = (cbm_daemon_ipc_endpoint_t *)endpoint;
    cbm_mutex_lock(&mutable_endpoint->generations_lock);
    win_generation_address_t *current =
        atomic_load_explicit(&mutable_endpoint->current_generation, memory_order_acquire);
    if (current && strcmp(current->address, address) == 0) {
        cbm_mutex_unlock(&mutable_endpoint->generations_lock);
        free(candidate->pipe_name);
        free(candidate);
        return true;
    }
    candidate->next = mutable_endpoint->generations;
    mutable_endpoint->generations = candidate;
    atomic_store_explicit(&mutable_endpoint->current_generation, candidate, memory_order_release);
    cbm_mutex_unlock(&mutable_endpoint->generations_lock);
    return true;
}

static win_rendezvous_status_t win_endpoint_refresh_rendezvous(
    const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint || !endpoint->user_sid || endpoint->user_sid_length == 0) {
        return WIN_RENDEZVOUS_ERROR;
    }
    cbm_private_lock_directory_t *directory = NULL;
    cbm_private_file_lock_t *record_lock = NULL;
    cbm_private_file_lock_status_t directory_status =
        cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory);
    if (directory_status != CBM_PRIVATE_FILE_LOCK_OK) {
        win_endpoint_generation_invalidate(endpoint);
        return WIN_RENDEZVOUS_ERROR;
    }
    cbm_private_file_lock_status_t lock_status = cbm_private_file_lock_try_acquire(
        directory, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE, CBM_PRIVATE_FILE_LOCK_SH, &record_lock);
    if (lock_status != CBM_PRIVATE_FILE_LOCK_OK) {
        win_private_lock_release_complete(&record_lock);
        cbm_private_lock_directory_close(directory);
        win_endpoint_generation_invalidate(endpoint);
        return lock_status == CBM_PRIVATE_FILE_LOCK_BUSY ? WIN_RENDEZVOUS_BUSY
                                                         : WIN_RENDEZVOUS_ERROR;
    }

    uint8_t record[CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE];
    size_t record_length = 0;
    cbm_private_file_lock_status_t read_status =
        cbm_private_file_lock_payload_read(record_lock, record, sizeof(record), &record_length);
    uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE];
    char address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP];
    bool decoded =
        read_status == CBM_PRIVATE_FILE_LOCK_OK &&
        cbm_daemon_ipc_windows_rendezvous_record_decode(record, record_length, nonce, address);
    char expected[CBM_DAEMON_IPC_WINDOWS_NAME_CAP];
    bool bound =
        decoded &&
        cbm_daemon_ipc_windows_generation_address(endpoint->user_sid, endpoint->user_sid_length,
                                                  endpoint->instance_key, nonce, expected) &&
        strcmp(expected, address) == 0;
    win_rendezvous_status_t result;
    if (read_status != CBM_PRIVATE_FILE_LOCK_OK) {
        win_endpoint_generation_invalidate(endpoint);
        result = WIN_RENDEZVOUS_ERROR;
    } else if (record_length == 0) {
        win_endpoint_generation_invalidate(endpoint);
        result = WIN_RENDEZVOUS_ABSENT;
    } else if (!bound) {
        win_endpoint_generation_invalidate(endpoint);
        result = WIN_RENDEZVOUS_CORRUPT;
    } else if (!win_endpoint_generation_install(endpoint, address)) {
        win_endpoint_generation_invalidate(endpoint);
        result = WIN_RENDEZVOUS_ERROR;
    } else {
        result = WIN_RENDEZVOUS_VALID;
    }
    /* Keep the record lock through the in-memory snapshot update. Otherwise
     * a reader of generation N could install/invalidate after a startup owner
     * has published generation N+1, regressing this endpoint to stale state. */
    win_private_lock_release_complete(&record_lock);
    cbm_private_lock_directory_close(directory);
    return result;
}

typedef LONG(WINAPI *bcrypt_gen_random_fn)(void *, unsigned char *, ULONG, ULONG);

static bool win_generation_nonce(uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE]) {
    enum { WIN_BCRYPT_USE_SYSTEM_PREFERRED_RNG = 0x00000002 };
    HMODULE bcrypt = LoadLibraryW(L"bcrypt.dll");
    bcrypt_gen_random_fn generate =
        bcrypt ? (bcrypt_gen_random_fn)GetProcAddress(bcrypt, "BCryptGenRandom") : NULL;
    LONG status = generate ? generate(NULL, nonce, CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE,
                                      WIN_BCRYPT_USE_SYSTEM_PREFERRED_RNG)
                           : (LONG)-1;
    if (bcrypt) {
        (void)FreeLibrary(bcrypt);
    }
    return status >= 0;
}

static int win_private_lock_probe(cbm_private_lock_directory_t *directory, const char *base_name) {
    cbm_private_file_lock_t *probe = NULL;
    cbm_private_file_lock_status_t status =
        cbm_private_file_lock_try_acquire(directory, base_name, CBM_PRIVATE_FILE_LOCK_SH, &probe);
    if (status == CBM_PRIVATE_FILE_LOCK_BUSY) {
        return 1;
    }
    if (status != CBM_PRIVATE_FILE_LOCK_OK) {
        win_private_lock_release_complete(&probe);
        return -1;
    }
    win_private_lock_release_complete(&probe);
    return 0;
}

static bool win_startup_publish_generation_locked(const cbm_daemon_ipc_endpoint_t *endpoint,
                                                  cbm_private_file_lock_t *record_lock) {
    uint8_t nonce[CBM_DAEMON_IPC_WINDOWS_NONCE_SIZE];
    char address[CBM_DAEMON_IPC_WINDOWS_NAME_CAP];
    uint8_t record[CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RECORD_SIZE];
    if (!record_lock || !win_generation_nonce(nonce) ||
        !cbm_daemon_ipc_windows_generation_address(endpoint->user_sid, endpoint->user_sid_length,
                                                   endpoint->instance_key, nonce, address) ||
        !cbm_daemon_ipc_windows_rendezvous_record_encode(nonce, address, record)) {
        return false;
    }
    bool written = cbm_private_file_lock_payload_write(record_lock, record, sizeof(record)) ==
                   CBM_PRIVATE_FILE_LOCK_OK;
    return written && win_endpoint_generation_install(endpoint, address);
}

/* Startup ownership serializes publishers, while the rendezvous EX lock is
 * the bridge to lifetime ownership: a lifetime acquirer must finish its final
 * rendezvous SH refresh before this probe can declare lifetime free. If it
 * acquires lifetime after the probe, its final refresh waits for this publish
 * and therefore binds the newly advertised generation. */
static int win_startup_prepare_generation(const cbm_daemon_ipc_endpoint_t *endpoint,
                                          cbm_private_lock_directory_t *directory) {
    uint64_t now = ipc_now_ms();
    uint64_t deadline = now > UINT64_MAX - CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RETRY_MS
                            ? UINT64_MAX
                            : now + CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_RETRY_MS;
    for (;;) {
        cbm_private_file_lock_t *record_lock = NULL;
        cbm_private_file_lock_status_t status =
            cbm_private_file_lock_try_acquire(directory, CBM_DAEMON_IPC_WINDOWS_RENDEZVOUS_FILE,
                                              CBM_PRIVATE_FILE_LOCK_EX, &record_lock);
        if (status == CBM_PRIVATE_FILE_LOCK_OK) {
            int lifetime = win_private_lock_probe(directory, WIN_LIFETIME_LOCK_NAME);
            bool published =
                lifetime == 0 && win_startup_publish_generation_locked(endpoint, record_lock);
            win_private_lock_release_complete(&record_lock);
            if (lifetime != 0) {
                return lifetime == 1 ? 0 : -1;
            }
            return published ? 1 : -1;
        }
        win_private_lock_release_complete(&record_lock);
        if (status != CBM_PRIVATE_FILE_LOCK_BUSY) {
            return -1;
        }
        if (ipc_now_ms() >= deadline) {
            return 0;
        }
        Sleep(1);
    }
}

int cbm_daemon_ipc_lifetime_reservation_try_acquire(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    cbm_daemon_ipc_lifetime_reservation_t **reservation_out) {
    if (reservation_out) {
        *reservation_out = NULL;
    }
    if (!endpoint || !reservation_out) {
        return -1;
    }

    win_rendezvous_status_t rendezvous = win_endpoint_refresh_rendezvous(endpoint);
    if (rendezvous == WIN_RENDEZVOUS_ABSENT || rendezvous == WIN_RENDEZVOUS_CORRUPT) {
        cbm_daemon_ipc_startup_lock_t *startup = NULL;
        int startup_status = cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup);
        bool prepared = startup_status == 1 && cbm_daemon_ipc_startup_lock_prepare_handoff(startup);
        ipc_startup_lock_release_complete(&startup);
        if (!prepared) {
            return startup_status == 0 ? 0 : -1;
        }
        rendezvous = win_endpoint_refresh_rendezvous(endpoint);
    }
    if (rendezvous != WIN_RENDEZVOUS_VALID) {
        return rendezvous == WIN_RENDEZVOUS_ABSENT || rendezvous == WIN_RENDEZVOUS_BUSY ? 0 : -1;
    }

    cbm_private_lock_directory_t *directory = NULL;
    cbm_private_file_lock_t *lock = NULL;
    if (cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory) !=
        CBM_PRIVATE_FILE_LOCK_OK) {
        return -1;
    }
    cbm_private_file_lock_status_t status = cbm_private_file_lock_try_acquire(
        directory, WIN_LIFETIME_LOCK_NAME, CBM_PRIVATE_FILE_LOCK_EX, &lock);
    if (status != CBM_PRIVATE_FILE_LOCK_OK) {
        win_private_lock_release_complete(&lock);
        cbm_private_lock_directory_close(directory);
        return status == CBM_PRIVATE_FILE_LOCK_BUSY ? 0 : -1;
    }

    /* A starter may have won immediately before this lifetime lock. Its
     * rendezvous publication must become visible before this owner listens;
     * otherwise release rather than binding a stale generation address. */
    if (win_endpoint_refresh_rendezvous(endpoint) != WIN_RENDEZVOUS_VALID) {
        win_private_lock_release_complete(&lock);
        cbm_private_lock_directory_close(directory);
        return 0;
    }
    cbm_daemon_ipc_lifetime_reservation_t *reservation = calloc(1, sizeof(*reservation));
    if (!reservation) {
        win_private_lock_release_complete(&lock);
        cbm_private_lock_directory_close(directory);
        return -1;
    }
    reservation->endpoint = endpoint;
    reservation->directory = directory;
    reservation->lock = lock;
    *reservation_out = reservation;
    return 1;
}

void cbm_daemon_ipc_lifetime_reservation_release(
    cbm_daemon_ipc_lifetime_reservation_t *reservation) {
    if (!reservation) {
        return;
    }
    win_private_lock_release_complete(&reservation->lock);
    cbm_private_lock_directory_close(reservation->directory);
    free(reservation);
}

static bool lifetime_reservation_matches_endpoint(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_ipc_lifetime_reservation_t *reservation) {
    return endpoint && reservation && reservation->endpoint == endpoint && reservation->lock;
}

int cbm_daemon_ipc_lifetime_reservation_probe(const cbm_daemon_ipc_endpoint_t *endpoint) {
    cbm_private_lock_directory_t *directory = NULL;
    if (!endpoint || cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory) !=
                         CBM_PRIVATE_FILE_LOCK_OK) {
        return -1;
    }
    int result = win_private_lock_probe(directory, WIN_LIFETIME_LOCK_NAME);
    cbm_private_lock_directory_close(directory);
    return result;
}

static int win_legacy_pipe_probe(const cbm_daemon_ipc_endpoint_t *endpoint) {
    if (!endpoint || !endpoint->legacy_pipe_name || !endpoint->legacy_startup_mutex_name) {
        return -1;
    }

    if (WaitNamedPipeW(endpoint->legacy_pipe_name, 0)) {
        return 1;
    }
    DWORD pipe_error = GetLastError();
    if (pipe_error == ERROR_SEM_TIMEOUT || pipe_error == ERROR_PIPE_BUSY) {
        return 1;
    }
    if (pipe_error != ERROR_FILE_NOT_FOUND && pipe_error != ERROR_PATH_NOT_FOUND) {
        return -1;
    }
    return 0;
}

int cbm_daemon_ipc_legacy_generation_probe(const cbm_daemon_ipc_endpoint_t *endpoint) {
    int pipe = win_legacy_pipe_probe(endpoint);
    if (pipe != 0) {
        return pipe;
    }

    /* Open-only observation is sufficient: the legacy startup owner retains
     * the sole named-object handle until it releases startup. Never create or
     * wait on the mutex, so this compatibility path cannot become authority. */
    HANDLE startup =
        OpenMutexW(SYNCHRONIZE | READ_CONTROL, FALSE, endpoint->legacy_startup_mutex_name);
    if (startup) {
        win_security_t security;
        bool safe =
            win_security_init(&security) && win_kernel_mutex_current_user_only(&security, startup);
        win_security_destroy(&security);
        (void)SetHandleInformation(startup, HANDLE_FLAG_INHERIT, 0);
        (void)CloseHandle(startup);
        return safe ? 1 : -1;
    }
    DWORD startup_error = GetLastError();
    return startup_error == ERROR_FILE_NOT_FOUND ? 0 : -1;
}

static HANDLE win_pipe_instance_new(const wchar_t *pipe_name, bool first_instance) {
    win_security_t security;
    if (!win_security_init(&security)) {
        return INVALID_HANDLE_VALUE;
    }
    DWORD open_mode = PIPE_ACCESS_DUPLEX | FILE_FLAG_OVERLAPPED;
    if (first_instance) {
        open_mode |= FILE_FLAG_FIRST_PIPE_INSTANCE;
    }
    HANDLE pipe = CreateNamedPipeW(
        pipe_name, open_mode,
        PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT | PIPE_REJECT_REMOTE_CLIENTS,
        PIPE_UNLIMITED_INSTANCES, 64U * 1024U, 64U * 1024U, 0, &security.attributes);
    win_security_destroy(&security);
    if (pipe != INVALID_HANDLE_VALUE) {
        (void)SetHandleInformation(pipe, HANDLE_FLAG_INHERIT, 0);
    }
    return pipe;
}

/* This is a namespace reservation, never a protocol endpoint. Every current
 * participant owns one idle instance. The first uses
 * FILE_FLAG_FIRST_PIPE_INSTANCE; later participants use the same exact pipe
 * parameters without that flag. Never calling ConnectNamedPipe means no
 * sentinel thread or cancellable I/O survives a participant. */
static HANDLE win_legacy_sentinel_new(const cbm_daemon_ipc_endpoint_t *endpoint, bool first,
                                      DWORD *error_out) {
    if (error_out) {
        *error_out = ERROR_INVALID_PARAMETER;
    }
    if (!endpoint || !endpoint->legacy_pipe_name || !error_out) {
        return INVALID_HANDLE_VALUE;
    }
    win_security_t security;
    if (!win_security_init(&security)) {
        *error_out = ERROR_GEN_FAILURE;
        return INVALID_HANDLE_VALUE;
    }
    SetLastError(ERROR_SUCCESS);
    DWORD open_mode = PIPE_ACCESS_DUPLEX;
    if (first) {
        open_mode |= FILE_FLAG_FIRST_PIPE_INSTANCE;
    }
    HANDLE sentinel = CreateNamedPipeW(endpoint->legacy_pipe_name, open_mode,
                                       PIPE_TYPE_BYTE | PIPE_READMODE_BYTE | PIPE_WAIT |
                                           PIPE_REJECT_REMOTE_CLIENTS,
                                       PIPE_UNLIMITED_INSTANCES, 1, 1, 0, &security.attributes);
    DWORD create_error = sentinel == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
    win_security_destroy(&security);
    if (sentinel != INVALID_HANDLE_VALUE &&
        SetHandleInformation(sentinel, HANDLE_FLAG_INHERIT, 0) == 0) {
        create_error = GetLastError();
        (void)CloseHandle(sentinel);
        sentinel = INVALID_HANDLE_VALUE;
    }
    *error_out = create_error;
    return sentinel;
}

static int win_startup_v2_try_acquire(cbm_private_lock_directory_t *directory,
                                      cbm_private_file_lock_t **lock_out) {
    if (lock_out) {
        *lock_out = NULL;
    }
    if (!directory || !lock_out) {
        return -1;
    }
    cbm_private_file_lock_status_t status = cbm_private_file_lock_try_acquire(
        directory, WIN_STARTUP_V2_LOCK_NAME, CBM_PRIVATE_FILE_LOCK_EX, lock_out);
    if (status == CBM_PRIVATE_FILE_LOCK_OK) {
        return 1;
    }
    win_private_lock_release_complete(lock_out);
    return status == CBM_PRIVATE_FILE_LOCK_BUSY ? 0 : -1;
}

static bool win_legacy_sentinel_conflict(DWORD error) {
    return error == ERROR_ACCESS_DENIED || error == ERROR_PIPE_BUSY ||
           error == ERROR_ALREADY_EXISTS;
}

/* The validated legacy mutex serializes the group-file and deterministic-pipe
 * observations. startup-v2 is retained by this process, or by the bootstrap
 * parent when a child joins. The SH lock is acquired before the mutex is
 * released, so group existence and at least one sentinel have no gap. */
static int win_participant_group_join_locked(const cbm_daemon_ipc_endpoint_t *endpoint,
                                             cbm_private_lock_directory_t *directory,
                                             bool allow_first, cbm_private_file_lock_t **group_out,
                                             HANDLE *sentinel_out) {
    if (group_out) {
        *group_out = NULL;
    }
    if (sentinel_out) {
        *sentinel_out = INVALID_HANDLE_VALUE;
    }
    if (!endpoint || !directory || !group_out || !sentinel_out) {
        return -1;
    }

    cbm_private_file_lock_t *exclusive_probe = NULL;
    cbm_private_file_lock_status_t group_status = cbm_private_file_lock_try_acquire(
        directory, WIN_PARTICIPANT_GROUP_LOCK_NAME, CBM_PRIVATE_FILE_LOCK_EX, &exclusive_probe);
    bool first = group_status == CBM_PRIVATE_FILE_LOCK_OK;
    if (!first && group_status != CBM_PRIVATE_FILE_LOCK_BUSY) {
        win_private_lock_release_complete(&exclusive_probe);
        return -1;
    }
    if (first && !allow_first) {
        win_private_lock_release_complete(&exclusive_probe);
        return 0;
    }

    int pipe = win_legacy_pipe_probe(endpoint);
    if ((first && pipe != 0) || (!first && pipe != 1)) {
        win_private_lock_release_complete(&exclusive_probe);
        return pipe < 0 ? -1 : 0;
    }
    DWORD create_error = ERROR_SUCCESS;
    HANDLE sentinel = win_legacy_sentinel_new(endpoint, first, &create_error);
    if (sentinel == INVALID_HANDLE_VALUE) {
        win_private_lock_release_complete(&exclusive_probe);
        return win_legacy_sentinel_conflict(create_error) ? 0 : -1;
    }
    if (exclusive_probe) {
        win_private_lock_release_complete(&exclusive_probe);
        if (exclusive_probe) {
            (void)CloseHandle(sentinel);
            return -1;
        }
    }

    cbm_private_file_lock_t *group = NULL;
    group_status = cbm_private_file_lock_try_acquire(directory, WIN_PARTICIPANT_GROUP_LOCK_NAME,
                                                     CBM_PRIVATE_FILE_LOCK_SH, &group);
    if (group_status != CBM_PRIVATE_FILE_LOCK_OK) {
        win_private_lock_release_complete(&group);
        (void)CloseHandle(sentinel);
        return group_status == CBM_PRIVATE_FILE_LOCK_BUSY ? 0 : -1;
    }
    *group_out = group;
    *sentinel_out = sentinel;
    return 1;
}

/* Called only while startup-v2 and the validated legacy mutex are retained.
 * Group SH disappears before this participant's sentinel; both gates exclude
 * joiners from observing that required teardown intermediate state. */
static bool win_participant_group_release_locked(cbm_private_file_lock_t **group_io,
                                                 HANDLE *sentinel_io) {
    if (!group_io || !sentinel_io) {
        return false;
    }
    if (*group_io && cbm_private_file_lock_release(group_io) != CBM_PRIVATE_FILE_LOCK_OK) {
        return false;
    }
    if (*sentinel_io != INVALID_HANDLE_VALUE) {
        if (CloseHandle(*sentinel_io) == 0) {
            return false;
        }
        *sentinel_io = INVALID_HANDLE_VALUE;
    }
    return true;
}

static void win_participant_group_release_complete(cbm_private_file_lock_t **group_io,
                                                   HANDLE *sentinel_io) {
    uint64_t deadline = ipc_deadline_after(CBM_DAEMON_IPC_COORDINATION_CLEANUP_MS);
    while ((group_io && *group_io) || (sentinel_io && *sentinel_io != INVALID_HANDLE_VALUE)) {
        (void)win_participant_group_release_locked(group_io, sentinel_io);
        if ((!group_io || !*group_io) && (!sentinel_io || *sentinel_io == INVALID_HANDLE_VALUE)) {
            return;
        }
        if (ipc_now_ms() >= deadline) {
            ipc_coordination_cleanup_fail_stop("participant_group_cleanup");
        }
        Sleep(1);
    }
}

static bool win_participant_state_release(const cbm_daemon_ipc_endpoint_t *endpoint,
                                          cbm_private_lock_directory_t *directory,
                                          cbm_private_file_lock_t **startup_v2_io,
                                          win_legacy_mutex_guard_t **legacy_guard_io,
                                          cbm_private_file_lock_t **group_io, HANDLE *sentinel_io) {
    if (!endpoint || !directory || !startup_v2_io || !legacy_guard_io || !group_io ||
        !sentinel_io) {
        return false;
    }
    if (!*startup_v2_io) {
        int startup = win_startup_v2_try_acquire(directory, startup_v2_io);
        if (startup != 1) {
            return false;
        }
    }
    if (!*legacy_guard_io) {
        int legacy = win_legacy_mutex_guard_try_acquire(endpoint, legacy_guard_io);
        if (legacy != 1) {
            (void)cbm_private_file_lock_release(startup_v2_io);
            return false;
        }
    }
    if (!win_participant_group_release_locked(group_io, sentinel_io)) {
        return false;
    }
    if (!win_legacy_mutex_guard_release(legacy_guard_io)) {
        return false;
    }
    return cbm_private_file_lock_release(startup_v2_io) == CBM_PRIVATE_FILE_LOCK_OK;
}

cbm_daemon_ipc_listener_t *cbm_daemon_ipc_listen_reserved(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    cbm_daemon_ipc_lifetime_reservation_t **reservation_io) {
    cbm_daemon_ipc_lifetime_reservation_t *lifetime_reservation =
        reservation_io ? *reservation_io : NULL;
    if (!lifetime_reservation_matches_endpoint(endpoint, lifetime_reservation) ||
        win_endpoint_refresh_rendezvous(endpoint) != WIN_RENDEZVOUS_VALID) {
        return NULL;
    }
    win_generation_address_t *generation = win_endpoint_generation_snapshot(endpoint);
    if (!generation || !generation->pipe_name) {
        return NULL;
    }
    cbm_daemon_ipc_listener_t *listener = calloc(1, sizeof(*listener));
    if (!listener) {
        return NULL;
    }
    listener->pipe_name = wide_copy(generation->pipe_name);
    listener->pipe = INVALID_HANDLE_VALUE;
    listener->first_instance = true;
    listener->lifetime_reservation = lifetime_reservation;
    if (!listener->pipe_name) {
        listener->lifetime_reservation = NULL;
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    listener->pipe = win_pipe_instance_new(listener->pipe_name, true);
    if (listener->pipe == INVALID_HANDLE_VALUE) {
        listener->lifetime_reservation = NULL;
        cbm_daemon_ipc_listener_close(listener);
        return NULL;
    }
    listener->first_instance = false;
    *reservation_io = NULL;
    return listener;
}

cbm_daemon_ipc_listener_t *cbm_daemon_ipc_listen(const cbm_daemon_ipc_endpoint_t *endpoint) {
    cbm_daemon_ipc_startup_lock_t *startup = NULL;
    cbm_daemon_ipc_participant_guard_t *participant_guard = NULL;
    cbm_daemon_ipc_lifetime_reservation_t *reservation = NULL;
    if (cbm_daemon_ipc_startup_lock_try_acquire(endpoint, &startup) != 1 ||
        !cbm_daemon_ipc_startup_lock_prepare_handoff(startup) ||
        cbm_daemon_ipc_participant_guard_try_join(endpoint, &participant_guard) != 1 ||
        cbm_daemon_ipc_lifetime_reservation_try_acquire(endpoint, &reservation) != 1) {
        cbm_daemon_ipc_lifetime_reservation_release(reservation);
        ipc_participant_guard_release_complete(&participant_guard);
        ipc_startup_lock_release_complete(&startup);
        return NULL;
    }
    cbm_daemon_ipc_listener_t *listener = cbm_daemon_ipc_listen_reserved(endpoint, &reservation);
    if (listener) {
        listener->participant_guard = participant_guard;
        participant_guard = NULL;
    }
    cbm_daemon_ipc_lifetime_reservation_release(reservation);
    ipc_participant_guard_release_complete(&participant_guard);
    ipc_startup_lock_release_complete(&startup);
    return listener;
}

void cbm_daemon_ipc_listener_close(cbm_daemon_ipc_listener_t *listener) {
    if (!listener) {
        return;
    }
    if (listener->pipe != INVALID_HANDLE_VALUE) {
        (void)CancelIoEx(listener->pipe, NULL);
        (void)DisconnectNamedPipe(listener->pipe);
        (void)CloseHandle(listener->pipe);
    }
    if (listener->connect_event) {
        (void)CloseHandle(listener->connect_event);
    }
    cbm_daemon_ipc_lifetime_reservation_release(listener->lifetime_reservation);
    ipc_participant_guard_release_complete(&listener->participant_guard);
    free(listener->pipe_name);
    free(listener);
}

typedef struct {
    HANDLE handle;
    OVERLAPPED *overlapped;
} win_pending_io_t;

static cbm_ipc_pending_wait_status_t win_pending_wait(void *opaque, uint32_t timeout_ms) {
    win_pending_io_t *pending = (win_pending_io_t *)opaque;
    DWORD result = WaitForSingleObject(pending->overlapped->hEvent, timeout_ms);
    if (result == WAIT_OBJECT_0) {
        return CBM_IPC_PENDING_WAIT_SIGNALED;
    }
    return result == WAIT_TIMEOUT ? CBM_IPC_PENDING_WAIT_TIMEOUT : CBM_IPC_PENDING_WAIT_FAILED;
}

static void win_pending_cancel(void *opaque) {
    win_pending_io_t *pending = (win_pending_io_t *)opaque;
    (void)CancelIoEx(pending->handle, pending->overlapped);
}

static cbm_ipc_pending_finish_status_t win_pending_finish(void *opaque, bool blocking,
                                                          uint32_t *transferred_out) {
    win_pending_io_t *pending = (win_pending_io_t *)opaque;
    DWORD transferred = 0;
    if (GetOverlappedResult(pending->handle, pending->overlapped, &transferred,
                            blocking ? TRUE : FALSE)) {
        *transferred_out = transferred;
        return CBM_IPC_PENDING_FINISH_COMPLETED;
    }
    return GetLastError() == ERROR_OPERATION_ABORTED ? CBM_IPC_PENDING_FINISH_CANCELLED
                                                     : CBM_IPC_PENDING_FINISH_FAILED;
}

static int win_overlapped_wait(HANDLE handle, OVERLAPPED *overlapped, DWORD timeout_ms,
                               DWORD *transferred_out) {
    win_pending_io_t pending = {.handle = handle, .overlapped = overlapped};
    cbm_ipc_pending_ops_t ops = {
        .context = &pending,
        .wait = win_pending_wait,
        .cancel = win_pending_cancel,
        .finish = win_pending_finish,
    };
    uint32_t transferred = 0;
    int result = cbm_daemon_ipc_wait_pending(&ops, timeout_ms, &transferred);
    if (result == 1) {
        *transferred_out = (DWORD)transferred;
    }
    return result;
}

static bool win_pipe_client_is_current_user(HANDLE pipe) {
    /* Validate the connected client by its process token, resolved from the
     * pipe's CLIENT process id - the mirror of the client-side server check
     * (win_pipe_server_is_current_user via GetNamedPipeServerProcessId). This
     * needs neither impersonation nor a prior read on the pipe, so it works at
     * accept time without changing accept's contract. ImpersonateNamedPipeClient
     * was unusable here: it fails with ERROR_CANNOT_IMPERSONATE until the server
     * has completed a read, which accept must not require. */
    HMODULE kernel = GetModuleHandleW(L"kernel32.dll");
    get_named_pipe_server_process_id_fn get_client_pid =
        kernel ? (get_named_pipe_server_process_id_fn)GetProcAddress(kernel,
                                                                     "GetNamedPipeClientProcessId")
               : NULL;
    if (!get_client_pid) {
        cbm_log_warn("daemon.accept.client_identity", "step", "pid_fn_missing");
        return false;
    }
    ULONG process_id = 0;
    if (!get_client_pid(pipe, &process_id) || process_id == 0) {
        char error_text[16];
        (void)snprintf(error_text, sizeof(error_text), "%lu", (unsigned long)GetLastError());
        cbm_log_warn("daemon.accept.client_identity", "step", "client_pid_query", "error",
                     error_text);
        return false;
    }
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, process_id);
    if (!process) {
        process = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, process_id);
    }
    if (!process) {
        char pid_text[16];
        char error_text[16];
        (void)snprintf(pid_text, sizeof(pid_text), "%lu", (unsigned long)process_id);
        (void)snprintf(error_text, sizeof(error_text), "%lu", (unsigned long)GetLastError());
        cbm_log_warn("daemon.accept.client_identity", "step", "process_open", "pid", pid_text,
                     "error", error_text);
        return false;
    }
    win_security_t security;
    bool initialized = win_security_init(&security);
    HANDLE token = NULL;
    bool opened = initialized && security.open_process_token(process, TOKEN_QUERY, &token) != 0;
    PSID token_sid = NULL;
    void *token_user = opened ? win_token_user_query(&security, token, &token_sid) : NULL;
    bool same_user =
        token_user && token_sid && security.equal_sid(token_sid, security.user_sid) != 0;
    if (!same_user) {
        const char *step = !initialized ? "security_init"
                           : !opened    ? "token_open"
                           : !token_sid ? "token_query"
                                        : "sid_mismatch";
        cbm_log_warn("daemon.accept.client_identity", "step", step);
    }
    free(token_user);
    if (token) {
        (void)CloseHandle(token);
    }
    if (initialized) {
        win_security_destroy(&security);
    }
    (void)CloseHandle(process);
    return same_user;
}

int cbm_daemon_ipc_accept(cbm_daemon_ipc_listener_t *listener, uint32_t timeout_ms,
                          cbm_daemon_ipc_connection_t **connection_out) {
    if (connection_out) {
        *connection_out = NULL;
    }
    if (!listener || !connection_out) {
        return -1;
    }
    if (listener->pipe == INVALID_HANDLE_VALUE) {
        listener->pipe = win_pipe_instance_new(listener->pipe_name, listener->first_instance);
        listener->first_instance = false;
        listener->connect_pending = false;
        if (listener->pipe == INVALID_HANDLE_VALUE) {
            return -1;
        }
    }

    if (!listener->connect_pending) {
        if (!listener->connect_event) {
            listener->connect_event = CreateEventW(NULL, TRUE, FALSE, NULL);
            if (!listener->connect_event) {
                return -1;
            }
        }
        if (!ResetEvent(listener->connect_event)) {
            return -1;
        }
        memset(&listener->connect_overlapped, 0, sizeof(listener->connect_overlapped));
        listener->connect_overlapped.hEvent = listener->connect_event;
        BOOL connected = ConnectNamedPipe(listener->pipe, &listener->connect_overlapped);
        DWORD connect_error = connected ? ERROR_PIPE_CONNECTED : GetLastError();
        if (connect_error == ERROR_IO_PENDING) {
            listener->connect_pending = true;
        } else if (connect_error != ERROR_PIPE_CONNECTED) {
            (void)DisconnectNamedPipe(listener->pipe);
            (void)CloseHandle(listener->pipe);
            listener->pipe = INVALID_HANDLE_VALUE;
            return -1;
        }
    }

    if (listener->connect_pending) {
        DWORD wait = WaitForSingleObject(listener->connect_event, timeout_ms);
        if (wait == WAIT_TIMEOUT) {
            /* Keep the instance and its pending connect armed: a listening
             * pipe must stay available for clients between polls. */
            return 0;
        }
        DWORD transferred = 0;
        if (wait != WAIT_OBJECT_0 ||
            !GetOverlappedResult(listener->pipe, &listener->connect_overlapped, &transferred,
                                 FALSE)) {
            DWORD connect_error = GetLastError();
            listener->connect_pending = false;
            if (connect_error != ERROR_PIPE_CONNECTED) {
                (void)DisconnectNamedPipe(listener->pipe);
                (void)CloseHandle(listener->pipe);
                listener->pipe = INVALID_HANDLE_VALUE;
                return -1;
            }
        }
        listener->connect_pending = false;
    }
    cbm_daemon_ipc_connection_t *connection = malloc(sizeof(*connection));
    if (!connection) {
        (void)DisconnectNamedPipe(listener->pipe);
        (void)CloseHandle(listener->pipe);
        listener->pipe = INVALID_HANDLE_VALUE;
        return -1;
    }
    connection->handle = listener->pipe;
    atomic_init(&connection->poisoned, false);
    connection->role = CBM_DAEMON_IPC_PIPE_ROLE_ACCEPTED_SERVER;
    /* Validate the client's identity by its process token (no impersonation,
     * no read) so accept keeps its immediate-return contract. */
    if (!win_pipe_client_is_current_user(listener->pipe)) {
        cbm_log_warn("daemon.accept.client_rejected", "stage", "client_identity");
        free(connection);
        (void)DisconnectNamedPipe(listener->pipe);
        (void)CloseHandle(listener->pipe);
        listener->pipe = INVALID_HANDLE_VALUE;
        return -1;
    }
    listener->pipe = INVALID_HANDLE_VALUE;
    *connection_out = connection;
    return 1;
}

static bool win_pipe_server_is_current_user(HANDLE pipe) {
    HMODULE kernel = GetModuleHandleW(L"kernel32.dll");
    get_named_pipe_server_process_id_fn get_server_pid =
        kernel ? (get_named_pipe_server_process_id_fn)GetProcAddress(kernel,
                                                                     "GetNamedPipeServerProcessId")
               : NULL;
    ULONG process_id = 0;
    if (!get_server_pid) {
        cbm_log_warn("daemon.client.server_identity", "step", "pid_fn_missing");
        return false;
    }
    if (!get_server_pid(pipe, &process_id) || process_id == 0) {
        char error_text[16];
        (void)snprintf(error_text, sizeof(error_text), "%lu", (unsigned long)GetLastError());
        char pid_text[16];
        (void)snprintf(pid_text, sizeof(pid_text), "%lu", (unsigned long)process_id);
        cbm_log_warn("daemon.client.server_identity", "step", "server_pid_query", "error",
                     error_text, "pid", pid_text);
        return false;
    }
    HANDLE process = OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, FALSE, process_id);
    if (!process) {
        process = OpenProcess(PROCESS_QUERY_INFORMATION, FALSE, process_id);
    }
    if (!process) {
        /* A pipe whose server process cannot be opened is usually a dead
         * server whose PID Windows already reaped or reused. */
        char pid_text[16];
        (void)snprintf(pid_text, sizeof(pid_text), "%lu", (unsigned long)process_id);
        char error_text[16];
        (void)snprintf(error_text, sizeof(error_text), "%lu", (unsigned long)GetLastError());
        cbm_log_warn("daemon.client.server_identity", "step", "process_open", "pid", pid_text,
                     "error", error_text);
        return false;
    }
    win_security_t security;
    bool initialized = win_security_init(&security);
    HANDLE token = NULL;
    bool opened = initialized && security.open_process_token(process, TOKEN_QUERY, &token) != 0;
    PSID token_sid = NULL;
    void *token_user = opened ? win_token_user_query(&security, token, &token_sid) : NULL;
    bool same_user =
        token_user && token_sid && security.equal_sid(token_sid, security.user_sid) != 0;
    if (!same_user) {
        /* Name the failing step and the peer: a rejected server can be a dead
         * PID that Windows already reused (often for a SYSTEM service), which
         * looks identical to a hostile pipe without this classification. */
        const char *step = !initialized ? "security_init"
                           : !opened    ? "token_open"
                           : !token_sid ? "token_query"
                           : security.is_well_known_sid(token_sid, WinLocalSystemSid)
                               ? "server_is_system"
                           : security.is_well_known_sid(token_sid, WinBuiltinAdministratorsSid)
                               ? "server_is_admins"
                               : "server_other_account";
        char pid_text[16];
        (void)snprintf(pid_text, sizeof(pid_text), "%lu", (unsigned long)process_id);
        cbm_log_warn("daemon.client.server_identity", "step", step, "pid", pid_text);
    }
    free(token_user);
    if (token) {
        (void)CloseHandle(token);
    }
    if (initialized) {
        win_security_destroy(&security);
    }
    (void)CloseHandle(process);
    return same_user;
}

static int win_current_generation_transport_probe(const cbm_daemon_ipc_endpoint_t *endpoint) {
    win_rendezvous_status_t rendezvous = win_endpoint_refresh_rendezvous(endpoint);
    if (rendezvous == WIN_RENDEZVOUS_ABSENT) {
        return 0;
    }
    if (rendezvous != WIN_RENDEZVOUS_VALID) {
        return -1;
    }
    win_generation_address_t *generation = win_endpoint_generation_snapshot(endpoint);
    if (!generation || !generation->pipe_name) {
        return -1;
    }
    HANDLE pipe = CreateFileW(generation->pipe_name, GENERIC_READ | GENERIC_WRITE, 0, NULL,
                              OPEN_EXISTING, FILE_FLAG_OVERLAPPED, NULL);
    if (pipe != INVALID_HANDLE_VALUE) {
        (void)SetHandleInformation(pipe, HANDLE_FLAG_INHERIT, 0);
        DWORD mode = PIPE_READMODE_BYTE;
        bool authenticated = SetNamedPipeHandleState(pipe, &mode, NULL, NULL) != 0 &&
                             win_pipe_server_is_current_user(pipe);
        (void)CloseHandle(pipe);
        return authenticated ? 1 : -1;
    }
    DWORD error = GetLastError();
    if (error == ERROR_PIPE_BUSY) {
        return 1;
    }
    return error == ERROR_FILE_NOT_FOUND || error == ERROR_PATH_NOT_FOUND ? 0 : -1;
}

int cbm_daemon_ipc_endpoint_probe(const cbm_daemon_ipc_endpoint_t *endpoint, uint32_t timeout_ms) {
    (void)timeout_ms;
    if (!endpoint) {
        return -1;
    }
    win_rendezvous_status_t rendezvous = win_endpoint_refresh_rendezvous(endpoint);
    if (rendezvous != WIN_RENDEZVOUS_VALID) {
        if (rendezvous == WIN_RENDEZVOUS_CORRUPT || rendezvous == WIN_RENDEZVOUS_ERROR) {
            return -1;
        }
        cbm_private_lock_directory_t *directory = NULL;
        if (cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory) !=
            CBM_PRIVATE_FILE_LOCK_OK) {
            return -1;
        }
        int startup = win_private_lock_probe(directory, WIN_STARTUP_V2_LOCK_NAME);
        cbm_private_lock_directory_close(directory);
        return startup;
    }
    win_generation_address_t *generation = win_endpoint_generation_snapshot(endpoint);
    if (!generation) {
        return -1;
    }
    HANDLE pipe = CreateFileW(generation->pipe_name, GENERIC_READ | GENERIC_WRITE, 0, NULL,
                              OPEN_EXISTING, FILE_FLAG_OVERLAPPED, NULL);
    if (pipe != INVALID_HANDLE_VALUE) {
        (void)SetHandleInformation(pipe, HANDLE_FLAG_INHERIT, 0);
        DWORD mode = PIPE_READMODE_BYTE;
        bool authenticated = SetNamedPipeHandleState(pipe, &mode, NULL, NULL) != 0 &&
                             win_pipe_server_is_current_user(pipe);
        (void)CloseHandle(pipe);
        return authenticated ? 1 : -1;
    }
    DWORD open_error = GetLastError();
    if (open_error == ERROR_PIPE_BUSY) {
        /* Every server instance is occupied, which is itself positive
         * liveness evidence. Treat capacity saturation as active. */
        return 1;
    }
    if (open_error == ERROR_FILE_NOT_FOUND || open_error == ERROR_PATH_NOT_FOUND) {
        int lifetime = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
        if (lifetime != 0) {
            return lifetime;
        }
        cbm_private_lock_directory_t *directory = NULL;
        if (cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory) !=
            CBM_PRIVATE_FILE_LOCK_OK) {
            return -1;
        }
        int startup = win_private_lock_probe(directory, WIN_STARTUP_V2_LOCK_NAME);
        cbm_private_lock_directory_close(directory);
        return startup;
    }
    return -1;
}

cbm_daemon_ipc_connection_t *cbm_daemon_ipc_connect(const cbm_daemon_ipc_endpoint_t *endpoint,
                                                    uint32_t timeout_ms) {
    if (!endpoint) {
        return NULL;
    }
    uint64_t deadline_ms = ipc_deadline_after(timeout_ms);
    HANDLE pipe = INVALID_HANDLE_VALUE;
    bool wait_logged = false;
    for (;;) {
        win_rendezvous_status_t rendezvous = win_endpoint_refresh_rendezvous(endpoint);
        if (rendezvous == WIN_RENDEZVOUS_CORRUPT || rendezvous == WIN_RENDEZVOUS_ERROR) {
            /* Terminal refusal: without a reason here the caller can only
             * report a generic connect timeout, which made owner-policy
             * failures on the rendezvous namespace undiagnosable. */
            cbm_log_warn("daemon.client.rendezvous_unreadable", "status",
                         rendezvous == WIN_RENDEZVOUS_CORRUPT ? "corrupt" : "unsafe_or_io");
            return NULL;
        }
        win_generation_address_t *generation =
            rendezvous == WIN_RENDEZVOUS_VALID ? win_endpoint_generation_snapshot(endpoint) : NULL;
        if (!generation) {
            if (win_retry_pause(deadline_ms)) {
                if (!wait_logged) {
                    wait_logged = true;
                    cbm_log_warn("daemon.client.rendezvous_wait", "status",
                                 rendezvous == WIN_RENDEZVOUS_ABSENT  ? "absent"
                                 : rendezvous == WIN_RENDEZVOUS_BUSY  ? "busy"
                                 : rendezvous == WIN_RENDEZVOUS_VALID ? "no_generation"
                                                                      : "unknown");
                }
                continue;
            }
            cbm_log_warn("daemon.client.connect_deadline", "status",
                         rendezvous == WIN_RENDEZVOUS_ABSENT  ? "absent"
                         : rendezvous == WIN_RENDEZVOUS_BUSY  ? "busy"
                         : rendezvous == WIN_RENDEZVOUS_VALID ? "no_generation"
                                                              : "unknown");
            return NULL;
        }
        pipe = CreateFileW(generation->pipe_name, GENERIC_READ | GENERIC_WRITE, 0, NULL,
                           OPEN_EXISTING, FILE_FLAG_OVERLAPPED, NULL);
        if (pipe != INVALID_HANDLE_VALUE) {
            break;
        }
        DWORD open_error = GetLastError();
        if ((open_error == ERROR_FILE_NOT_FOUND || open_error == ERROR_PATH_NOT_FOUND) &&
            win_retry_pause(deadline_ms)) {
            continue;
        }
        if (open_error != ERROR_PIPE_BUSY) {
            return NULL;
        }
        DWORD remaining = win_deadline_remaining(deadline_ms);
        if (remaining == 0) {
            return NULL;
        }
        if (!WaitNamedPipeW(generation->pipe_name, remaining)) {
            DWORD wait_error = GetLastError();
            if ((wait_error == ERROR_FILE_NOT_FOUND || wait_error == ERROR_PATH_NOT_FOUND) &&
                win_retry_pause(deadline_ms)) {
                continue;
            }
            return NULL;
        }
    }
    (void)SetHandleInformation(pipe, HANDLE_FLAG_INHERIT, 0);
    DWORD mode = PIPE_READMODE_BYTE;
    if (!SetNamedPipeHandleState(pipe, &mode, NULL, NULL) ||
        !win_pipe_server_is_current_user(pipe)) {
        cbm_log_warn("daemon.client.pipe_rejected", "stage", "server_identity");
        (void)CloseHandle(pipe);
        return NULL;
    }
    cbm_daemon_ipc_connection_t *connection = malloc(sizeof(*connection));
    if (!connection) {
        (void)CloseHandle(pipe);
        return NULL;
    }
    connection->handle = pipe;
    atomic_init(&connection->poisoned, false);
    connection->role = CBM_DAEMON_IPC_PIPE_ROLE_CONNECTED_CLIENT;
    return connection;
}

void cbm_daemon_ipc_connection_close(cbm_daemon_ipc_connection_t *connection) {
    if (!connection) {
        return;
    }
    if (connection->handle != INVALID_HANDLE_VALUE) {
        (void)CancelIoEx(connection->handle, NULL);
        (void)CloseHandle(connection->handle);
    }
    free(connection);
}

static int win_io_once(cbm_daemon_ipc_connection_t *connection, void *buffer, DWORD length,
                       bool writing, uint64_t deadline_ms, DWORD *transferred_out);

void cbm_daemon_ipc_connection_drain(cbm_daemon_ipc_connection_t *connection, uint32_t timeout_ms) {
    if (!connection || connection->handle == INVALID_HANDLE_VALUE ||
        connection->role != CBM_DAEMON_IPC_PIPE_ROLE_ACCEPTED_SERVER ||
        atomic_load_explicit(&connection->poisoned, memory_order_acquire)) {
        return;
    }
    /* Closing the server end of a named pipe discards data the client has not
     * read yet, so a final response followed by an immediate close races the
     * client's decode. Reading until peer EOF proves consumption: clients
     * close only after draining their pending response. A poisoned or
     * interrupted connection returns immediately above/below, so ordinary
     * EOF-triggered teardown pays nothing here. */
    uint64_t deadline_ms = ipc_deadline_after(timeout_ms);
    for (;;) {
        uint8_t discard[256];
        DWORD transferred = 0;
        int result =
            win_io_once(connection, discard, sizeof(discard), false, deadline_ms, &transferred);
        if (result != 1 || transferred == 0) {
            return;
        }
    }
}

void cbm_daemon_ipc_connection_interrupt(cbm_daemon_ipc_connection_t *connection) {
    if (!connection || connection->handle == INVALID_HANDLE_VALUE) {
        return;
    }
    (void)CancelIoEx(connection->handle, NULL);
    if (connection->role == CBM_DAEMON_IPC_PIPE_ROLE_ACCEPTED_SERVER) {
        (void)DisconnectNamedPipe(connection->handle);
    }
}

uint64_t cbm_daemon_ipc_connection_peer_pid(const cbm_daemon_ipc_connection_t *connection) {
    if (!connection || !connection->handle || connection->handle == INVALID_HANDLE_VALUE) {
        return 0;
    }

    HMODULE kernel = GetModuleHandleW(L"kernel32.dll");
    ULONG process_id = 0;
    if (connection->role == CBM_DAEMON_IPC_PIPE_ROLE_ACCEPTED_SERVER) {
        get_named_pipe_client_process_id_fn get_client_pid =
            kernel ? (get_named_pipe_client_process_id_fn)GetProcAddress(
                         kernel, "GetNamedPipeClientProcessId")
                   : NULL;
        if (!get_client_pid || !get_client_pid(connection->handle, &process_id)) {
            return 0;
        }
    } else if (connection->role == CBM_DAEMON_IPC_PIPE_ROLE_CONNECTED_CLIENT) {
        get_named_pipe_server_process_id_fn get_server_pid =
            kernel ? (get_named_pipe_server_process_id_fn)GetProcAddress(
                         kernel, "GetNamedPipeServerProcessId")
                   : NULL;
        if (!get_server_pid || !get_server_pid(connection->handle, &process_id)) {
            return 0;
        }
    } else {
        return 0;
    }
    return process_id == 0 ? 0 : (uint64_t)process_id;
}

static int win_io_once(cbm_daemon_ipc_connection_t *connection, void *buffer, DWORD length,
                       bool writing, uint64_t deadline_ms, DWORD *transferred_out) {
    OVERLAPPED overlapped;
    memset(&overlapped, 0, sizeof(overlapped));
    overlapped.hEvent = CreateEventW(NULL, TRUE, FALSE, NULL);
    if (!overlapped.hEvent) {
        return -1;
    }
    DWORD transferred = 0;
    BOOL completed = writing
                         ? WriteFile(connection->handle, buffer, length, &transferred, &overlapped)
                         : ReadFile(connection->handle, buffer, length, &transferred, &overlapped);
    int result = 1;
    if (!completed) {
        if (GetLastError() != ERROR_IO_PENDING) {
            result = -1;
        } else {
            result = win_overlapped_wait(connection->handle, &overlapped,
                                         win_deadline_remaining(deadline_ms), &transferred);
        }
    }
    (void)CloseHandle(overlapped.hEvent);
    if (result == 1) {
        *transferred_out = transferred;
    }
    return result;
}

static int connection_read_full(cbm_daemon_ipc_connection_t *connection, void *buffer,
                                size_t length, uint64_t deadline_ms) {
    if (!connection || atomic_load_explicit(&connection->poisoned, memory_order_acquire)) {
        return -1;
    }
    size_t offset = 0;
    while (offset < length) {
        DWORD chunk = length - offset > MAXDWORD ? MAXDWORD : (DWORD)(length - offset);
        DWORD transferred = 0;
        int result = win_io_once(connection, (uint8_t *)buffer + offset, chunk, false, deadline_ms,
                                 &transferred);
        if (result != 1) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return result;
        }
        if (transferred == 0) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return -1;
        }
        offset += transferred;
    }
    return 1;
}

static int connection_write_full(cbm_daemon_ipc_connection_t *connection, const void *buffer,
                                 size_t length, uint64_t deadline_ms) {
    if (!connection || atomic_load_explicit(&connection->poisoned, memory_order_acquire)) {
        return -1;
    }
    size_t offset = 0;
    while (offset < length) {
        DWORD chunk = length - offset > MAXDWORD ? MAXDWORD : (DWORD)(length - offset);
        DWORD transferred = 0;
        int result = win_io_once(connection, (void *)((const uint8_t *)buffer + offset), chunk,
                                 true, deadline_ms, &transferred);
        if (result != 1) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return result;
        }
        if (transferred == 0) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return -1;
        }
        offset += transferred;
    }
    return 1;
}

int cbm_daemon_ipc_startup_lock_try_acquire(const cbm_daemon_ipc_endpoint_t *endpoint,
                                            cbm_daemon_ipc_startup_lock_t **lock_out) {
    if (lock_out) {
        *lock_out = NULL;
    }
    if (!endpoint || !lock_out) {
        return -1;
    }
    cbm_private_lock_directory_t *directory = NULL;
    if (cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory) !=
        CBM_PRIVATE_FILE_LOCK_OK) {
        return -1;
    }
    cbm_private_file_lock_t *startup_v2 = NULL;
    int startup_status = win_startup_v2_try_acquire(directory, &startup_v2);
    if (startup_status != 1) {
        cbm_private_lock_directory_close(directory);
        return startup_status;
    }
    win_legacy_mutex_guard_t *legacy_guard = NULL;
    int legacy_status = win_legacy_mutex_guard_try_acquire(endpoint, &legacy_guard);
    if (legacy_status != 1) {
        win_private_lock_release_complete(&startup_v2);
        cbm_private_lock_directory_close(directory);
        return legacy_status;
    }

    cbm_daemon_ipc_startup_lock_t *lock = calloc(1, sizeof(*lock));
    if (!lock) {
        win_legacy_mutex_guard_release_complete(&legacy_guard);
        win_private_lock_release_complete(&startup_v2);
        cbm_private_lock_directory_close(directory);
        return -1;
    }
    lock->endpoint = endpoint;
    lock->directory = directory;
    lock->startup_v2_lock = startup_v2;
    lock->legacy_guard = legacy_guard;
    lock->legacy_sentinel = INVALID_HANDLE_VALUE;
    *lock_out = lock;
    return 1;
}

int cbm_daemon_ipc_generation_probe_under_startup_lock(
    const cbm_daemon_ipc_endpoint_t *endpoint, const cbm_daemon_ipc_startup_lock_t *startup_lock) {
    if (!endpoint || !startup_lock || startup_lock->endpoint != endpoint ||
        !startup_lock->directory || !startup_lock->startup_v2_lock || !startup_lock->legacy_guard ||
        startup_lock->prepared || startup_lock->group_lock ||
        startup_lock->legacy_sentinel != INVALID_HANDLE_VALUE) {
        return -1;
    }
    int lifetime = cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
    if (lifetime != 0) {
        return lifetime;
    }
    int deterministic_pipe = win_legacy_pipe_probe(endpoint);
    if (deterministic_pipe != 0) {
        return deterministic_pipe;
    }
    return win_current_generation_transport_probe(endpoint);
}

bool cbm_daemon_ipc_startup_lock_prepare_handoff(cbm_daemon_ipc_startup_lock_t *lock) {
    if (!lock || !lock->endpoint || !lock->directory || !lock->startup_v2_lock) {
        return false;
    }
    if (lock->prepared) {
        return lock->group_lock && lock->legacy_sentinel != INVALID_HANDLE_VALUE &&
               !lock->legacy_guard;
    }
    if (!lock->legacy_guard || lock->group_lock || lock->legacy_sentinel != INVALID_HANDLE_VALUE) {
        return false;
    }
    int joined = win_participant_group_join_locked(lock->endpoint, lock->directory, true,
                                                   &lock->group_lock, &lock->legacy_sentinel);
    if (joined != 1) {
        return false;
    }
    int generation = win_startup_prepare_generation(lock->endpoint, lock->directory);
    if (generation != 1) {
        (void)win_participant_group_release_locked(&lock->group_lock, &lock->legacy_sentinel);
        return false;
    }
    if (!win_legacy_mutex_guard_release(&lock->legacy_guard)) {
        return false;
    }
    lock->prepared = true;
    return true;
}

bool cbm_daemon_ipc_startup_lock_release(cbm_daemon_ipc_startup_lock_t **lock_io) {
    if (!lock_io) {
        return false;
    }
    cbm_daemon_ipc_startup_lock_t *lock = *lock_io;
    if (!lock) {
        return true;
    }
    if (!win_participant_state_release(lock->endpoint, lock->directory, &lock->startup_v2_lock,
                                       &lock->legacy_guard, &lock->group_lock,
                                       &lock->legacy_sentinel)) {
        return false;
    }
    cbm_private_lock_directory_close(lock->directory);
    free(lock);
    *lock_io = NULL;
    return true;
}

int cbm_daemon_ipc_participant_guard_try_join(const cbm_daemon_ipc_endpoint_t *endpoint,
                                              cbm_daemon_ipc_participant_guard_t **guard_out) {
    if (guard_out) {
        *guard_out = NULL;
    }
    if (!endpoint || !guard_out) {
        return -1;
    }
    cbm_daemon_ipc_participant_guard_t *guard = calloc(1, sizeof(*guard));
    if (!guard) {
        return -1;
    }
    cbm_private_lock_directory_t *directory = NULL;
    if (cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory) !=
        CBM_PRIVATE_FILE_LOCK_OK) {
        free(guard);
        return -1;
    }
    win_legacy_mutex_guard_t *legacy_guard = NULL;
    int legacy = win_legacy_mutex_guard_try_acquire(endpoint, &legacy_guard);
    if (legacy != 1) {
        cbm_private_lock_directory_close(directory);
        free(guard);
        return legacy;
    }
    cbm_private_file_lock_t *group = NULL;
    HANDLE sentinel = INVALID_HANDLE_VALUE;
    int joined = win_participant_group_join_locked(endpoint, directory, false, &group, &sentinel);
    bool mutex_released = win_legacy_mutex_guard_release(&legacy_guard);
    if (joined != 1 || !mutex_released) {
        if (group || sentinel != INVALID_HANDLE_VALUE) {
            win_participant_group_release_complete(&group, &sentinel);
        }
        win_legacy_mutex_guard_release_complete(&legacy_guard);
        cbm_private_lock_directory_close(directory);
        free(guard);
        return joined == 0 && mutex_released ? 0 : -1;
    }
    guard->endpoint = endpoint;
    guard->directory = directory;
    guard->group_lock = group;
    guard->legacy_sentinel = sentinel;
    *guard_out = guard;
    return 1;
}

bool cbm_daemon_ipc_participant_guard_release(cbm_daemon_ipc_participant_guard_t **guard_io) {
    if (!guard_io) {
        return false;
    }
    cbm_daemon_ipc_participant_guard_t *guard = *guard_io;
    if (!guard) {
        return true;
    }
    if (!win_participant_state_release(
            guard->endpoint, guard->directory, &guard->teardown_startup_v2_lock,
            &guard->teardown_legacy_guard, &guard->group_lock, &guard->legacy_sentinel)) {
        return false;
    }
    cbm_private_lock_directory_close(guard->directory);
    free(guard);
    *guard_io = NULL;
    return true;
}

int cbm_daemon_ipc_local_transition_try_acquire(
    const cbm_daemon_ipc_endpoint_t *endpoint, cbm_daemon_ipc_local_transition_t **transition_out) {
    if (transition_out) {
        *transition_out = NULL;
    }
    if (!endpoint || !transition_out) {
        return -1;
    }
    cbm_private_lock_directory_t *directory = NULL;
    if (cbm_daemon_ipc_private_lock_directory_new(endpoint, &directory) !=
        CBM_PRIVATE_FILE_LOCK_OK) {
        return -1;
    }
    cbm_private_file_lock_t *startup_v2 = NULL;
    int startup = win_startup_v2_try_acquire(directory, &startup_v2);
    if (startup != 1) {
        cbm_private_lock_directory_close(directory);
        return startup;
    }
    cbm_daemon_ipc_local_transition_t *transition = calloc(1, sizeof(*transition));
    if (!transition) {
        win_private_lock_release_complete(&startup_v2);
        cbm_private_lock_directory_close(directory);
        return -1;
    }
    transition->endpoint = endpoint;
    transition->directory = directory;
    transition->startup_v2_lock = startup_v2;
    transition->legacy_sentinel = INVALID_HANDLE_VALUE;
    *transition_out = transition;
    return 1;
}

int cbm_daemon_ipc_local_transition_seal_legacy(cbm_daemon_ipc_local_transition_t *transition) {
    if (!transition || !transition->endpoint || !transition->directory ||
        !transition->startup_v2_lock || transition->work_begun) {
        return -1;
    }
    if (transition->sealed) {
        return transition->group_lock && transition->legacy_sentinel != INVALID_HANDLE_VALUE ? 1
                                                                                             : -1;
    }
    win_legacy_mutex_guard_t *legacy_guard = NULL;
    int legacy = win_legacy_mutex_guard_try_acquire(transition->endpoint, &legacy_guard);
    if (legacy != 1) {
        return legacy;
    }
    int joined =
        win_participant_group_join_locked(transition->endpoint, transition->directory, true,
                                          &transition->group_lock, &transition->legacy_sentinel);
    bool mutex_released = win_legacy_mutex_guard_release(&legacy_guard);
    if (joined != 1 || !mutex_released) {
        if (legacy_guard || transition->group_lock ||
            transition->legacy_sentinel != INVALID_HANDLE_VALUE) {
            transition->teardown_legacy_guard = legacy_guard;
        }
        return joined == 0 && mutex_released ? 0 : -1;
    }
    transition->sealed = true;
    return 1;
}

int cbm_daemon_ipc_local_transition_lifetime_probe(
    const cbm_daemon_ipc_endpoint_t *endpoint,
    const cbm_daemon_ipc_local_transition_t *transition) {
    if (!endpoint || !transition || transition->endpoint != endpoint || !transition->directory ||
        !transition->startup_v2_lock || !transition->sealed || transition->work_begun ||
        !transition->group_lock || transition->legacy_sentinel == INVALID_HANDLE_VALUE) {
        return -1;
    }
    return cbm_daemon_ipc_lifetime_reservation_probe(endpoint);
}

bool cbm_daemon_ipc_local_transition_begin_work(cbm_daemon_ipc_local_transition_t *transition) {
    if (!transition || !transition->endpoint || !transition->directory ||
        !transition->startup_v2_lock || !transition->sealed || transition->work_begun ||
        !transition->group_lock || transition->legacy_sentinel == INVALID_HANDLE_VALUE) {
        return false;
    }
    if (cbm_private_file_lock_release(&transition->startup_v2_lock) != CBM_PRIVATE_FILE_LOCK_OK) {
        return false;
    }
    transition->work_begun = true;
    return true;
}

bool cbm_daemon_ipc_local_transition_release(cbm_daemon_ipc_local_transition_t **transition_io) {
    if (!transition_io) {
        return false;
    }
    cbm_daemon_ipc_local_transition_t *transition = *transition_io;
    if (!transition) {
        return true;
    }
    if (!transition->endpoint || !transition->directory) {
        return false;
    }
    if (transition->group_lock || transition->legacy_sentinel != INVALID_HANDLE_VALUE ||
        transition->teardown_legacy_guard) {
        if (!win_participant_state_release(transition->endpoint, transition->directory,
                                           &transition->startup_v2_lock,
                                           &transition->teardown_legacy_guard,
                                           &transition->group_lock, &transition->legacy_sentinel)) {
            return false;
        }
    } else if (transition->startup_v2_lock &&
               cbm_private_file_lock_release(&transition->startup_v2_lock) !=
                   CBM_PRIVATE_FILE_LOCK_OK) {
        return false;
    }
    cbm_private_lock_directory_close(transition->directory);
    transition->directory = NULL;
    free(transition);
    *transition_io = NULL;
    return true;
}

#endif /* _WIN32 */

bool cbm_daemon_ipc_send_frame(cbm_daemon_ipc_connection_t *connection,
                               cbm_daemon_frame_type_t type, uint16_t flags, const void *payload,
                               uint32_t length) {
    if (!connection || atomic_load_explicit(&connection->poisoned, memory_order_acquire) ||
        (length > 0 && !payload)) {
        return false;
    }
    uint8_t header[CBM_DAEMON_FRAME_HEADER_SIZE];
    if (!cbm_daemon_frame_header_encode(header, type, flags, length)) {
        return false;
    }
    uint64_t deadline_ms = ipc_deadline_after(CBM_DAEMON_IPC_SEND_TIMEOUT_MS);
    if (connection_write_full(connection, header, sizeof(header), deadline_ms) != 1) {
        return false;
    }
    if (length > 0 && connection_write_full(connection, payload, length, deadline_ms) != 1) {
        /* The peer has already received a complete header and will interpret
         * subsequent bytes as this payload, so this stream cannot be reused. */
        atomic_store_explicit(&connection->poisoned, true, memory_order_release);
        return false;
    }
    return true;
}

int cbm_daemon_ipc_receive_frame_bounded(cbm_daemon_ipc_connection_t *connection,
                                         uint32_t timeout_ms, uint32_t max_payload_length,
                                         cbm_daemon_frame_t *frame_out, uint8_t **payload_out) {
    if (payload_out) {
        *payload_out = NULL;
    }
    if (!connection || atomic_load_explicit(&connection->poisoned, memory_order_acquire) ||
        !frame_out || !payload_out) {
        return -1;
    }
    memset(frame_out, 0, sizeof(*frame_out));
    uint64_t deadline_ms = ipc_deadline_after(timeout_ms);
    uint8_t header[CBM_DAEMON_FRAME_HEADER_SIZE];
    int result = connection_read_full(connection, header, sizeof(header), deadline_ms);
    if (result != 1) {
        return result;
    }
    cbm_daemon_frame_t frame;
    if (!cbm_daemon_frame_header_decode(header, &frame)) {
        atomic_store_explicit(&connection->poisoned, true, memory_order_release);
        return -1;
    }
    if (frame.length > max_payload_length) {
        /* The fixed unauthenticated envelope limit is enforced from the header
         * before allocating or reading attacker-controlled payload bytes. The
         * unread stream is necessarily unusable and is closed by its owner. */
        atomic_store_explicit(&connection->poisoned, true, memory_order_release);
        return -1;
    }
    uint8_t *payload = NULL;
    if (frame.length > 0) {
        payload = malloc(frame.length);
        if (!payload) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            return -1;
        }
        result = connection_read_full(connection, payload, frame.length, deadline_ms);
        if (result != 1) {
            atomic_store_explicit(&connection->poisoned, true, memory_order_release);
            free(payload);
            return result;
        }
    }
    *frame_out = frame;
    *payload_out = payload;
    return 1;
}

int cbm_daemon_ipc_receive_frame(cbm_daemon_ipc_connection_t *connection, uint32_t timeout_ms,
                                 cbm_daemon_frame_t *frame_out, uint8_t **payload_out) {
    return cbm_daemon_ipc_receive_frame_bounded(connection, timeout_ms, CBM_DAEMON_MAX_FRAME_SIZE,
                                                frame_out, payload_out);
}
