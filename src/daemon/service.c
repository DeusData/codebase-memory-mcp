/*
 * service.c — Stable daemon rendezvous, exact-build policy, and conflict log.
 */
#include "daemon/service.h"

#include "daemon/service_internal.h"
#ifdef _WIN32
#include "daemon/ipc.h"
#endif

#include "foundation/compat_fs.h"
#include "foundation/compat.h"
#include "foundation/constants.h"
#include "foundation/sha256.h"

#include <errno.h>
#include <inttypes.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

enum {
    DAEMON_SERVICE_PATH_CAP = 4096,
    DAEMON_SERVICE_IO_CAP = 64 * 1024,
    DAEMON_SERVICE_FINGERPRINT_RECORD_CAP = CBM_SZ_512,
    /* A fixed 2 KiB file retains the ordinary installed client, managed-daemon
     * copy, supervised worker, and adjacent-generation snapshots with room for
     * another image. Lookup and storage therefore remain O(1); an arbitrary
     * number of copied images cannot grow persistent state without bound. */
    DAEMON_SERVICE_FINGERPRINT_CACHE_CAP = CBM_SZ_2K,
    DAEMON_SERVICE_ESCAPED_VERSION_CAP = (CBM_DAEMON_VERSION_TEXT_SIZE - 1) * 6 + 1,
    DAEMON_SERVICE_LOG_RECORD_CAP = 1536,
};

typedef struct {
    uint64_t field[CBM_SZ_7];
} daemon_fingerprint_snapshot_t;

enum {
    DAEMON_FINGERPRINT_SNAPSHOT_FIELDS = sizeof(((daemon_fingerprint_snapshot_t *)0)->field) /
                                         sizeof(((daemon_fingerprint_snapshot_t *)0)->field[0]),
    DAEMON_FINGERPRINT_FIELD_HEX_CHARS = sizeof(uint64_t) * CBM_SZ_2,
    DAEMON_FINGERPRINT_POSIX_WRITE_SEC_FIELD = 3,
    DAEMON_FINGERPRINT_POSIX_WRITE_NSEC_FIELD = 4,
    DAEMON_FINGERPRINT_WINDOWS_WRITE_TIME_FIELD = 4,
};

static cbm_daemon_conflict_log_test_hook_fn g_conflict_log_test_hook;
static void *g_conflict_log_test_context;

void cbm_daemon_conflict_log_set_test_hook(cbm_daemon_conflict_log_test_hook_fn hook,
                                           void *context) {
    g_conflict_log_test_context = context;
    g_conflict_log_test_hook = hook;
}

static void conflict_log_test_hook(cbm_daemon_conflict_log_test_stage_t stage) {
    if (g_conflict_log_test_hook) {
        g_conflict_log_test_hook(g_conflict_log_test_context, stage);
    }
}

static bool bounded_length(const char *value, size_t cap, size_t *length_out) {
    if (!value || cap == 0) {
        return false;
    }
    for (size_t i = 0; i < cap; i++) {
        if (value[i] == '\0') {
            if (length_out) {
                *length_out = i;
            }
            return true;
        }
    }
    return false;
}

static bool version_valid(const char *version) {
    size_t length = 0;
    if (!bounded_length(version, CBM_DAEMON_VERSION_TEXT_SIZE, &length) || length == 0) {
        return false;
    }
    /* Version text reaches stderr and a persistent JSON log. Keep it printable
     * ASCII so an untrusted HELLO cannot inject terminal control sequences or
     * malformed UTF-8 into either diagnostic channel. */
    for (size_t i = 0; i < length; i++) {
        unsigned char ch = (unsigned char)version[i];
        if (ch < 0x20 || ch > 0x7e) {
            return false;
        }
    }
    return true;
}

static bool fingerprint_valid(const char *fingerprint) {
    size_t length = 0;
    if (!bounded_length(fingerprint, CBM_DAEMON_BUILD_FINGERPRINT_SIZE, &length) ||
        length != CBM_SHA256_HEX_LEN) {
        return false;
    }
    for (size_t i = 0; i < length; i++) {
        char ch = fingerprint[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
            return false;
        }
    }
    return true;
}

static bool optional_fingerprint_valid(const char *fingerprint) {
    return !fingerprint || !fingerprint[0] || fingerprint_valid(fingerprint);
}

static bool identity_valid(const cbm_daemon_build_identity_t *identity) {
    return identity && version_valid(identity->semantic_version) &&
           fingerprint_valid(identity->build_fingerprint) &&
           optional_fingerprint_valid(identity->cache_fingerprint) && identity->protocol_abi != 0 &&
           identity->store_abi != 0 && identity->feature_abi != 0;
}

static bool conflict_status_valid(cbm_daemon_hello_status_t status) {
    return status >= CBM_DAEMON_HELLO_VERSION_CONFLICT && status <= CBM_DAEMON_HELLO_CACHE_CONFLICT;
}

static bool conflict_valid(const cbm_daemon_conflict_t *conflict) {
    return conflict && conflict_status_valid(conflict->status) &&
           version_valid(conflict->active_version) &&
           fingerprint_valid(conflict->active_build_fingerprint) &&
           version_valid(conflict->requested_version) &&
           fingerprint_valid(conflict->requested_build_fingerprint) &&
           (conflict->status != CBM_DAEMON_HELLO_CACHE_CONFLICT ||
            (fingerprint_valid(conflict->active_cache_fingerprint) &&
             fingerprint_valid(conflict->requested_cache_fingerprint)));
}

static const char *conflict_reason(cbm_daemon_hello_status_t status) {
    switch (status) {
    case CBM_DAEMON_HELLO_VERSION_CONFLICT:
        return "version";
    case CBM_DAEMON_HELLO_BUILD_CONFLICT:
        return "build";
    case CBM_DAEMON_HELLO_PROTOCOL_ABI_CONFLICT:
        return "protocol_abi";
    case CBM_DAEMON_HELLO_STORE_ABI_CONFLICT:
        return "store_abi";
    case CBM_DAEMON_HELLO_FEATURE_ABI_CONFLICT:
        return "feature_abi";
    case CBM_DAEMON_HELLO_CACHE_CONFLICT:
        return "cache_root";
    default:
        return NULL;
    }
}

static void digest_to_hex(const uint8_t digest[CBM_SHA256_DIGEST_LEN],
                          char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    static const char hex[] = "0123456789abcdef";
    for (size_t i = 0; i < CBM_SHA256_DIGEST_LEN; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0x0f];
    }
    out[CBM_SHA256_HEX_LEN] = '\0';
}

bool cbm_daemon_rendezvous_key(char out[CBM_DAEMON_KEY_SIZE]) {
    if (!out) {
        return false;
    }
    /* This product-domain string is intentionally the only key input. Account
     * isolation comes from the authenticated IPC runtime, not spoofable text. */
    static const unsigned char domain[] = "codebase-memory-mcp:coordination-daemon";
    uint64_t hash = 14695981039346656037ULL;
    for (size_t i = 0; i < sizeof(domain) - 1; i++) {
        hash ^= domain[i];
        hash *= 1099511628211ULL;
    }
    int written = snprintf(out, CBM_DAEMON_KEY_SIZE, "%016llx", (unsigned long long)hash);
    if (written != (int)CBM_DAEMON_KEY_SIZE - 1) {
        out[0] = '\0';
        return false;
    }
    return true;
}

cbm_daemon_hello_status_t cbm_daemon_hello_compare(const cbm_daemon_build_identity_t *active,
                                                   const cbm_daemon_build_identity_t *requested,
                                                   cbm_daemon_conflict_t *conflict_out) {
    if (conflict_out) {
        memset(conflict_out, 0, sizeof(*conflict_out));
        conflict_out->status = CBM_DAEMON_HELLO_INVALID;
    }
    if (!conflict_out || !identity_valid(active) || !identity_valid(requested)) {
        return CBM_DAEMON_HELLO_INVALID;
    }

    (void)snprintf(conflict_out->active_version, sizeof(conflict_out->active_version), "%s",
                   active->semantic_version);
    (void)snprintf(conflict_out->active_build_fingerprint,
                   sizeof(conflict_out->active_build_fingerprint), "%s", active->build_fingerprint);
    (void)snprintf(conflict_out->requested_version, sizeof(conflict_out->requested_version), "%s",
                   requested->semantic_version);
    (void)snprintf(conflict_out->requested_build_fingerprint,
                   sizeof(conflict_out->requested_build_fingerprint), "%s",
                   requested->build_fingerprint);
    if (active->cache_fingerprint) {
        (void)snprintf(conflict_out->active_cache_fingerprint,
                       sizeof(conflict_out->active_cache_fingerprint), "%s",
                       active->cache_fingerprint);
    }
    if (requested->cache_fingerprint) {
        (void)snprintf(conflict_out->requested_cache_fingerprint,
                       sizeof(conflict_out->requested_cache_fingerprint), "%s",
                       requested->cache_fingerprint);
    }

    cbm_daemon_hello_status_t status = CBM_DAEMON_HELLO_COMPATIBLE;
    if (strcmp(active->semantic_version, requested->semantic_version) != 0) {
        status = CBM_DAEMON_HELLO_VERSION_CONFLICT;
    } else if (strcmp(active->build_fingerprint, requested->build_fingerprint) != 0) {
        status = CBM_DAEMON_HELLO_BUILD_CONFLICT;
    } else if (active->protocol_abi != requested->protocol_abi) {
        status = CBM_DAEMON_HELLO_PROTOCOL_ABI_CONFLICT;
    } else if (active->store_abi != requested->store_abi) {
        status = CBM_DAEMON_HELLO_STORE_ABI_CONFLICT;
    } else if (active->feature_abi != requested->feature_abi) {
        status = CBM_DAEMON_HELLO_FEATURE_ABI_CONFLICT;
    }
    conflict_out->status = status;
    return status;
}

bool cbm_daemon_conflict_format(const cbm_daemon_conflict_t *conflict, char *out, size_t out_size) {
    if (!out || out_size == 0) {
        return false;
    }
    out[0] = '\0';
    const char *reason = conflict ? conflict_reason(conflict->status) : NULL;
    if (!reason || !conflict_valid(conflict)) {
        return false;
    }
    int written;
    if (conflict->status == CBM_DAEMON_HELLO_CACHE_CONFLICT) {
        written =
            snprintf(out, out_size,
                     "CBM could not start because the active account daemon uses a "
                     "different cache directory (active cache %s; requested cache %s). "
                     "Close all CBM sessions and commands, then retry with one "
                     "consistent CBM_CACHE_DIR.",
                     conflict->active_cache_fingerprint, conflict->requested_cache_fingerprint);
    } else {
        written = snprintf(out, out_size,
                           "CBM could not start because a conflicting CBM process is active "
                           "(%s; active version %s, build %s; requested version %s, build %s). "
                           "Close all CBM sessions and commands, then retry.",
                           reason, conflict->active_version, conflict->active_build_fingerprint,
                           conflict->requested_version, conflict->requested_build_fingerprint);
    }
    if (written < 0 || (size_t)written >= out_size) {
        out[0] = '\0';
        return false;
    }
    return true;
}

static bool json_escape_version(const char *value, char out[DAEMON_SERVICE_ESCAPED_VERSION_CAP]) {
    static const char hex[] = "0123456789abcdef";
    size_t length = 0;
    if (!version_valid(value) || !bounded_length(value, CBM_DAEMON_VERSION_TEXT_SIZE, &length)) {
        return false;
    }
    size_t used = 0;
    for (size_t i = 0; i < length; i++) {
        unsigned char ch = (unsigned char)value[i];
        if (ch == '"' || ch == '\\') {
            if (used + 2 >= DAEMON_SERVICE_ESCAPED_VERSION_CAP) {
                return false;
            }
            out[used++] = '\\';
            out[used++] = (char)ch;
        } else if (ch < 0x20) {
            if (used + 6 >= DAEMON_SERVICE_ESCAPED_VERSION_CAP) {
                return false;
            }
            out[used++] = '\\';
            out[used++] = 'u';
            out[used++] = '0';
            out[used++] = '0';
            out[used++] = hex[ch >> 4];
            out[used++] = hex[ch & 0x0f];
        } else {
            if (used + 1 >= DAEMON_SERVICE_ESCAPED_VERSION_CAP) {
                return false;
            }
            out[used++] = (char)ch;
        }
    }
    out[used] = '\0';
    return true;
}

static bool conflict_log_record(const cbm_daemon_conflict_t *conflict,
                                char out[DAEMON_SERVICE_LOG_RECORD_CAP], size_t *length_out) {
    char active[DAEMON_SERVICE_ESCAPED_VERSION_CAP];
    char requested[DAEMON_SERVICE_ESCAPED_VERSION_CAP];
    const char *reason = conflict ? conflict_reason(conflict->status) : NULL;
    if (!length_out || !reason || !conflict_valid(conflict) ||
        !json_escape_version(conflict->active_version, active) ||
        !json_escape_version(conflict->requested_version, requested)) {
        return false;
    }
    time_t timestamp = time(NULL);
    if (timestamp == (time_t)-1) {
        return false;
    }
    int written;
    if (conflict->status == CBM_DAEMON_HELLO_CACHE_CONFLICT) {
        written = snprintf(out, DAEMON_SERVICE_LOG_RECORD_CAP,
                           "{\"event\":\"daemon.version_conflict\",\"timestamp_unix_s\":%lld,"
                           "\"reason\":\"%s\",\"active_cache\":\"%s\","
                           "\"requested_cache\":\"%s\",\"active_version\":\"%s\","
                           "\"active_build\":\"%s\",\"requested_version\":\"%s\","
                           "\"requested_build\":\"%s\"}\n",
                           (long long)timestamp, reason, conflict->active_cache_fingerprint,
                           conflict->requested_cache_fingerprint, active,
                           conflict->active_build_fingerprint, requested,
                           conflict->requested_build_fingerprint);
    } else {
        written = snprintf(out, DAEMON_SERVICE_LOG_RECORD_CAP,
                           "{\"event\":\"daemon.version_conflict\",\"timestamp_unix_s\":%lld,"
                           "\"reason\":\"%s\","
                           "\"active_version\":\"%s\",\"active_build\":\"%s\","
                           "\"requested_version\":\"%s\",\"requested_build\":\"%s\"}\n",
                           (long long)timestamp, reason, active, conflict->active_build_fingerprint,
                           requested, conflict->requested_build_fingerprint);
    }
    if (written < 0 || written >= DAEMON_SERVICE_LOG_RECORD_CAP) {
        return false;
    }
    *length_out = (size_t)written;
    return true;
}

#ifndef _WIN32

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

static bool fd_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD);
    return flags >= 0 && (flags & FD_CLOEXEC || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0);
}

static bool fd_regular_current_user(int fd, struct stat *status_out) {
    struct stat status;
    if (fstat(fd, &status) != 0 || !S_ISREG(status.st_mode) || status.st_uid != geteuid() ||
        status.st_nlink != 1) {
        return false;
    }
    if (status_out) {
        *status_out = status;
    }
    return true;
}

static bool fd_regular(int fd, struct stat *status_out) {
    struct stat status;
    if (fstat(fd, &status) != 0 || !S_ISREG(status.st_mode)) {
        return false;
    }
    if (status_out) {
        *status_out = status;
    }
    return true;
}

static bool daemon_fingerprint_native_snapshot(uintptr_t native_file,
                                               daemon_fingerprint_snapshot_t *snapshot) {
    if (!snapshot || native_file > INT_MAX) {
        return false;
    }
    struct stat status;
    if (!fd_regular((int)native_file, &status) || status.st_size < 0) {
        return false;
    }
#if defined(__APPLE__)
    const struct timespec modified = status.st_mtimespec;
    const struct timespec changed = status.st_ctimespec;
#else
    const struct timespec modified = status.st_mtim;
    const struct timespec changed = status.st_ctim;
#endif
    *snapshot = (daemon_fingerprint_snapshot_t){
        .field = {(uint64_t)status.st_dev, (uint64_t)status.st_ino, (uint64_t)status.st_size,
                  (uint64_t)modified.tv_sec, (uint64_t)modified.tv_nsec, (uint64_t)changed.tv_sec,
                  (uint64_t)changed.tv_nsec},
    };
    return true;
}

bool cbm_daemon_build_fingerprint_native_file(uintptr_t native_file,
                                              char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    if (native_file > INT_MAX) {
        return false;
    }
    int fd = (int)native_file;
    struct stat before;
    if (fd < 0 || !fd_regular(fd, &before) || before.st_size < 0) {
        return false;
    }

    cbm_sha256_ctx context;
    cbm_sha256_init(&context);
    unsigned char buffer[DAEMON_SERVICE_IO_CAP];
    off_t offset = 0;
    bool ok = true;
    while (offset < before.st_size) {
        off_t remaining = before.st_size - offset;
        size_t request = remaining < (off_t)sizeof(buffer) ? (size_t)remaining : sizeof(buffer);
        ssize_t count = pread(fd, buffer, request, offset);
        if (count > 0) {
            cbm_sha256_update(&context, buffer, (size_t)count);
            offset += (off_t)count;
        } else if (count == 0) {
            ok = false;
            break;
        } else if (errno != EINTR) {
            ok = false;
            break;
        }
    }
    struct stat after;
    ok = ok && fstat(fd, &after) == 0 && before.st_dev == after.st_dev &&
         before.st_ino == after.st_ino && before.st_size == after.st_size &&
         offset == after.st_size;
    if (!ok) {
        return false;
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&context, digest);
    digest_to_hex(digest, out);
    return true;
}

bool cbm_daemon_build_fingerprint_file(const char *path,
                                       char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    size_t path_length = 0;
    if (!bounded_length(path, DAEMON_SERVICE_PATH_CAP, &path_length) || path_length == 0) {
        return false;
    }
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    bool ok =
        fd >= 0 && fd_cloexec(fd) && cbm_daemon_build_fingerprint_native_file((uintptr_t)fd, out);
    if (fd >= 0 && close(fd) != 0) {
        ok = false;
    }
    if (!ok) {
        out[0] = '\0';
    }
    return ok;
}

typedef struct {
    int dir_fd;
    char storage[DAEMON_SERVICE_PATH_CAP];
    char *base;
    char rotated[NAME_MAX + 3];
    char lock[NAME_MAX + 1];
} posix_log_path_t;

static void posix_log_path_close(posix_log_path_t *path) {
    if (path && path->dir_fd >= 0) {
        (void)close(path->dir_fd);
        path->dir_fd = -1;
    }
}

static bool posix_log_path_open(const char *log_path, posix_log_path_t *path) {
    size_t length = 0;
    if (!path || !bounded_length(log_path, sizeof(path->storage), &length) || length == 0) {
        return false;
    }
    memset(path, 0, sizeof(*path));
    path->dir_fd = -1;
    memcpy(path->storage, log_path, length + 1);
    char *slash = strrchr(path->storage, '/');
    const char *parent = ".";
    if (slash) {
        path->base = slash + 1;
        if (slash == path->storage) {
            parent = "/";
        } else {
            *slash = '\0';
            parent = path->storage;
        }
    } else {
        path->base = path->storage;
    }
    size_t base_length = strlen(path->base);
    if (base_length == 0 || base_length > NAME_MAX - 5 || strcmp(path->base, ".") == 0 ||
        strcmp(path->base, "..") == 0) {
        return false;
    }
    int rotated_written = snprintf(path->rotated, sizeof(path->rotated), "%s.1", path->base);
    int lock_written = snprintf(path->lock, sizeof(path->lock), "%s.lock", path->base);
    if (rotated_written < 0 || (size_t)rotated_written >= sizeof(path->rotated) ||
        lock_written < 0 || (size_t)lock_written >= sizeof(path->lock)) {
        return false;
    }
    path->dir_fd = open(parent, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    struct stat parent_status;
    if (path->dir_fd < 0 || !fd_cloexec(path->dir_fd) || fstat(path->dir_fd, &parent_status) != 0 ||
        !S_ISDIR(parent_status.st_mode) || parent_status.st_uid != geteuid() ||
        (parent_status.st_mode & 0022) != 0) {
        posix_log_path_close(path);
        return false;
    }
    return true;
}

static int posix_lock_file_open(const posix_log_path_t *path, bool *created_out) {
    int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    int fd = openat(path->dir_fd, path->lock, flags | O_CREAT | O_EXCL, 0600);
    bool created = fd >= 0;
    if (fd < 0 && errno == EEXIST) {
        fd = openat(path->dir_fd, path->lock, flags);
    }
    struct stat status;
    struct stat by_path;
    if (fd < 0 || !fd_cloexec(fd) || !fd_regular_current_user(fd, &status) ||
        fchmod(fd, 0600) != 0 ||
        fstatat(path->dir_fd, path->lock, &by_path, AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(by_path.st_mode) || by_path.st_uid != geteuid() || by_path.st_nlink != 1 ||
        status.st_dev != by_path.st_dev || status.st_ino != by_path.st_ino) {
        if (fd >= 0) {
            (void)close(fd);
        }
        return -1;
    }
    *created_out = created;
    return fd;
}

static bool posix_lock_exclusive(int fd) {
    while (flock(fd, LOCK_EX) != 0) {
        if (errno != EINTR) {
            return false;
        }
    }
    return true;
}

static int posix_log_file_open(const posix_log_path_t *path, struct stat *status_out,
                               bool *created_out) {
    int flags = O_RDWR | O_APPEND | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    int fd = openat(path->dir_fd, path->base, flags | O_CREAT | O_EXCL, 0600);
    bool created = fd >= 0;
    if (fd < 0 && errno == EEXIST) {
        fd = openat(path->dir_fd, path->base, flags);
    }
    struct stat status;
    struct stat by_path;
    if (fd < 0 || !fd_cloexec(fd) || !fd_regular_current_user(fd, &status) ||
        fchmod(fd, 0600) != 0 ||
        fstatat(path->dir_fd, path->base, &by_path, AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(by_path.st_mode) || status.st_dev != by_path.st_dev ||
        status.st_ino != by_path.st_ino) {
        if (fd >= 0) {
            (void)close(fd);
        }
        return -1;
    }
    *status_out = status;
    *created_out = created;
    return fd;
}

static bool posix_complete_write(int fd, const char *record, size_t record_length,
                                 off_t original_size) {
    size_t written = 0;
    while (written < record_length) {
        ssize_t count = write(fd, record + written, record_length - written);
        if (count > 0) {
            written += (size_t)count;
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            (void)ftruncate(fd, original_size);
            return false;
        }
    }
    while (fsync(fd) != 0) {
        if (errno != EINTR) {
            (void)ftruncate(fd, original_size);
            return false;
        }
    }
    return true;
}

static bool posix_sync(int fd) {
    for (;;) {
        if (fsync(fd) == 0) {
            return true;
        }
        if (errno != EINTR) {
            return false;
        }
    }
}

static bool posix_log_refresh_locked(const posix_log_path_t *path, int fd,
                                     struct stat *status_out) {
    struct stat current;
    struct stat by_path;
    if (!fd_regular_current_user(fd, &current) || current.st_size < 0 ||
        fstatat(path->dir_fd, path->base, &by_path, AT_SYMLINK_NOFOLLOW) != 0 ||
        !S_ISREG(by_path.st_mode) || current.st_dev != by_path.st_dev ||
        current.st_ino != by_path.st_ino) {
        return false;
    }
    *status_out = current;
    return true;
}

static bool posix_log_append(const char *log_path, const char *record, size_t record_length,
                             size_t cap_bytes) {
    posix_log_path_t path;
    if (!posix_log_path_open(log_path, &path)) {
        return false;
    }
    bool lock_created = false;
    int lock_fd = posix_lock_file_open(&path, &lock_created);
    if (lock_fd >= 0) {
        conflict_log_test_hook(CBM_DAEMON_CONFLICT_LOG_BEFORE_SERIALIZATION_LOCK);
    }
    bool ok = lock_fd >= 0 && posix_lock_exclusive(lock_fd);
    if (ok) {
        conflict_log_test_hook(CBM_DAEMON_CONFLICT_LOG_AFTER_SERIALIZATION_LOCK);
    }
    struct stat status;
    bool created = false;
    int fd = ok ? posix_log_file_open(&path, &status, &created) : -1;
    ok = ok && fd >= 0 && posix_log_refresh_locked(&path, fd, &status);
    bool directory_changed = lock_created || created;
    bool rotate = false;
    if (ok && status.st_size > 0) {
        uintmax_t size = (uintmax_t)status.st_size;
        rotate = size > cap_bytes || record_length > cap_bytes - (size_t)size;
    }
    if (ok && rotate) {
        ok = renameat(path.dir_fd, path.base, path.dir_fd, path.rotated) == 0;
        ok = close(fd) == 0 && ok;
        fd = -1;
        directory_changed = directory_changed || ok;
        if (ok) {
            fd = posix_log_file_open(&path, &status, &created);
            ok = fd >= 0 && posix_log_refresh_locked(&path, fd, &status);
            directory_changed = directory_changed || created;
        }
    }
    if (ok) {
        char last = '\n';
        if (status.st_size > 0) {
            ssize_t count;
            do {
                count = pread(fd, &last, 1, status.st_size - 1);
            } while (count < 0 && errno == EINTR);
            ok = count == 1 && last == '\n';
        }
        if (ok) {
            ok = posix_complete_write(fd, record, record_length, status.st_size);
        }
    }
    if (fd >= 0) {
        if (close(fd) != 0) {
            ok = false;
        }
    }
    if (lock_fd >= 0) {
        (void)flock(lock_fd, LOCK_UN);
        if (close(lock_fd) != 0) {
            ok = false;
        }
    }
    if (ok && directory_changed) {
        ok = posix_sync(path.dir_fd);
    }
    posix_log_path_close(&path);
    return ok;
}

#else /* _WIN32 */

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <wchar.h>

#include "foundation/win_utf8.h"

static bool windows_same_file_identity(const BY_HANDLE_FILE_INFORMATION *first,
                                       const BY_HANDLE_FILE_INFORMATION *second) {
    return first && second && first->dwVolumeSerialNumber == second->dwVolumeSerialNumber &&
           first->nFileIndexHigh == second->nFileIndexHigh &&
           first->nFileIndexLow == second->nFileIndexLow;
}

static uint64_t windows_file_time(FILETIME value) {
    return ((uint64_t)value.dwHighDateTime << 32U) | (uint64_t)value.dwLowDateTime;
}

static bool daemon_fingerprint_native_snapshot(uintptr_t native_file,
                                               daemon_fingerprint_snapshot_t *snapshot) {
    HANDLE file = (HANDLE)native_file;
    BY_HANDLE_FILE_INFORMATION info;
    LARGE_INTEGER size;
    if (!snapshot || !file || file == INVALID_HANDLE_VALUE || GetFileType(file) != FILE_TYPE_DISK ||
        GetFileInformationByHandle(file, &info) == 0 ||
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0 ||
        GetFileSizeEx(file, &size) == 0 || size.QuadPart < 0) {
        return false;
    }
    *snapshot = (daemon_fingerprint_snapshot_t){
        .field = {(uint64_t)info.dwVolumeSerialNumber,
                  ((uint64_t)info.nFileIndexHigh << 32U) | (uint64_t)info.nFileIndexLow,
                  (uint64_t)size.QuadPart, windows_file_time(info.ftCreationTime),
                  windows_file_time(info.ftLastWriteTime), (uint64_t)info.dwFileAttributes,
                  (uint64_t)info.nNumberOfLinks},
    };
    return true;
}

bool cbm_daemon_build_fingerprint_native_file(uintptr_t native_file,
                                              char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    HANDLE original = (HANDLE)native_file;
    if (!original || original == INVALID_HANDLE_VALUE) {
        return false;
    }
    BY_HANDLE_FILE_INFORMATION original_info;
    HANDLE file = ReOpenFile(original, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE,
                             FILE_FLAG_SEQUENTIAL_SCAN);
    BY_HANDLE_FILE_INFORMATION info;
    LARGE_INTEGER before;
    bool ok =
        GetFileType(original) == FILE_TYPE_DISK &&
        GetFileInformationByHandle(original, &original_info) != 0 &&
        (original_info.dwFileAttributes &
         (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
        file != INVALID_HANDLE_VALUE && GetFileType(file) == FILE_TYPE_DISK &&
        GetFileInformationByHandle(file, &info) != 0 &&
        (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
        windows_same_file_identity(&original_info, &info) && GetFileSizeEx(file, &before) != 0 &&
        before.QuadPart >= 0;
    cbm_sha256_ctx context;
    cbm_sha256_init(&context);
    unsigned char buffer[DAEMON_SERVICE_IO_CAP];
    uint64_t total = 0;
    while (ok) {
        DWORD count = 0;
        if (!ReadFile(file, buffer, sizeof(buffer), &count, NULL)) {
            ok = false;
        } else if (count == 0) {
            break;
        } else {
            cbm_sha256_update(&context, buffer, count);
            total += count;
        }
    }
    LARGE_INTEGER after;
    BY_HANDLE_FILE_INFORMATION after_info;
    ok = ok && GetFileSizeEx(file, &after) != 0 &&
         GetFileInformationByHandle(file, &after_info) != 0 &&
         windows_same_file_identity(&info, &after_info) && before.QuadPart == after.QuadPart &&
         total == (uint64_t)after.QuadPart;
    if (file != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(file);
    }
    if (!ok) {
        return false;
    }
    uint8_t digest[CBM_SHA256_DIGEST_LEN];
    cbm_sha256_final(&context, digest);
    digest_to_hex(digest, out);
    return true;
}

bool cbm_daemon_build_fingerprint_file(const char *path,
                                       char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    size_t path_length = 0;
    if (!bounded_length(path, DAEMON_SERVICE_PATH_CAP, &path_length) || path_length == 0) {
        return false;
    }
    wchar_t *wide = cbm_path_to_wide(path);
    if (!wide) {
        return false;
    }
    HANDLE file =
        CreateFileW(wide, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    free(wide);
    bool ok = file != INVALID_HANDLE_VALUE &&
              cbm_daemon_build_fingerprint_native_file((uintptr_t)file, out);
    if (file != INVALID_HANDLE_VALUE && !CloseHandle(file)) {
        ok = false;
    }
    if (!ok) {
        out[0] = '\0';
    }
    return ok;
}

static bool windows_log_handle_valid(HANDLE file, LARGE_INTEGER *size_out) {
    BY_HANDLE_FILE_INFORMATION info;
    return file != INVALID_HANDLE_VALUE && GetFileType(file) == FILE_TYPE_DISK &&
           GetFileInformationByHandle(file, &info) != 0 &&
           (info.dwFileAttributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) ==
               0 &&
           info.nNumberOfLinks == 1 && GetFileSizeEx(file, size_out) != 0 &&
           size_out->QuadPart >= 0;
}

static HANDLE windows_log_open(const wchar_t *path, LARGE_INTEGER *size_out) {
    /* windows_private_file_prepare creates or validates the file with IPC's
     * explicit protected current-user DACL before this handle is opened. */
    HANDLE file =
        CreateFileW(path, GENERIC_READ | GENERIC_WRITE, FILE_SHARE_READ, NULL, OPEN_EXISTING,
                    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (!windows_log_handle_valid(file, size_out)) {
        if (file != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(file);
        }
        return INVALID_HANDLE_VALUE;
    }
    return file;
}

typedef struct {
    char storage[DAEMON_SERVICE_PATH_CAP];
    char *base;
    char lock_base[DAEMON_SERVICE_PATH_CAP];
    wchar_t *wide;
    wchar_t *rotated;
    wchar_t *lock;
} windows_log_path_t;

static void windows_log_path_close(windows_log_path_t *path) {
    if (!path) {
        return;
    }
    free(path->wide);
    free(path->rotated);
    free(path->lock);
    memset(path, 0, sizeof(*path));
}

static bool windows_log_path_open(const char *log_path, windows_log_path_t *path) {
    size_t length = 0;
    if (!path || !bounded_length(log_path, sizeof(path->storage), &length) || length == 0 ||
        length + 5 >= sizeof(path->storage)) {
        return false;
    }
    memset(path, 0, sizeof(*path));
    memcpy(path->storage, log_path, length + 1);
    char *forward = strrchr(path->storage, '/');
    char *backward = strrchr(path->storage, '\\');
    char *slash = forward;
    if (!slash || (backward && backward > slash)) {
        slash = backward;
    }
    if (!slash || slash == path->storage || slash[1] == '\0') {
        return false;
    }
    path->base = slash + 1;
    *slash = '\0';
    size_t base_length = strlen(path->base);
    int lock_written = snprintf(path->lock_base, sizeof(path->lock_base), "%s.lock", path->base);
    if (base_length == 0 || base_length > 248 || lock_written <= 0 ||
        (size_t)lock_written >= sizeof(path->lock_base)) {
        return false;
    }

    path->wide = cbm_path_to_wide(log_path);
    if (!path->wide) {
        return false;
    }
    size_t wide_length = wcslen(path->wide);
    path->rotated = malloc((wide_length + 3) * sizeof(*path->rotated));
    path->lock = malloc((wide_length + 6) * sizeof(*path->lock));
    if (!path->rotated || !path->lock) {
        windows_log_path_close(path);
        return false;
    }
    memcpy(path->rotated, path->wide, wide_length * sizeof(*path->rotated));
    memcpy(path->rotated + wide_length, L".1", 3 * sizeof(*path->rotated));
    memcpy(path->lock, path->wide, wide_length * sizeof(*path->lock));
    memcpy(path->lock + wide_length, L".lock", 6 * sizeof(*path->lock));
    return true;
}

static bool windows_private_file_prepare(const char *directory, const char *base) {
    /* cbm_daemon_ipc_private_log_open supplies the same explicit protected
     * current-SID DACL and reparse/owner checks as the daemon's operation log.
     * Its brief FILE_SHARE_READ handle can collide with a concurrent opener,
     * so retry that transient window rather than dropping a conflict event.
     * The retry budget is a short deadline, not a count: Sleep(2) rounds up
     * to the ~16 ms timer granularity, and a permanently obstructed path
     * (e.g. a directory squatting on it) must fail fast — a rejected client
     * is waiting behind this on the hello path with its own timeout. */
    ULONGLONG obstruction_deadline = GetTickCount64() + 250;
    /* Genuine share collisions get a wider budget than obstruction: with
     * many concurrent appenders on a loaded, sanitized CI runner the
     * last-in-line writer can legitimately spend more than 250 ms behind
     * its peers' brief exclusive windows, and the no-drop contract for
     * conflict events outranks latency there — while a permanently
     * obstructed path (a directory squatting on the name) still fails
     * inside 250 ms, because a rejected client is waiting behind this on
     * the hello path with its own timeout. */
    ULONGLONG contention_deadline = GetTickCount64() + 2000;
    for (;;) {
        SetLastError(ERROR_SUCCESS);
        FILE *file = cbm_daemon_ipc_private_log_open(directory, base, SIZE_MAX);
        if (file) {
            return fclose(file) == 0;
        }
        DWORD open_error = GetLastError();
        bool transient_collision =
            open_error == ERROR_SHARING_VIOLATION || open_error == ERROR_LOCK_VIOLATION;
        ULONGLONG deadline = transient_collision ? contention_deadline : obstruction_deadline;
        if (GetTickCount64() >= deadline) {
            return false;
        }
        Sleep(2);
    }
}

static HANDLE windows_lock_open(const wchar_t *path) {
    for (unsigned int attempt = 0; attempt < 100; attempt++) {
        HANDLE file = CreateFileW(
            /* Read access is sufficient for LockFileEx. Keeping this handle
             * read-only makes it share-compatible with the brief DACL repair
             * handle, whose FILE_SHARE_READ still admits every live locker. */
            path, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
        if (file != INVALID_HANDLE_VALUE) {
            LARGE_INTEGER size;
            if (windows_log_handle_valid(file, &size)) {
                (void)SetHandleInformation(file, HANDLE_FLAG_INHERIT, 0);
                return file;
            }
            (void)CloseHandle(file);
            return INVALID_HANDLE_VALUE;
        }
        DWORD error = GetLastError();
        if (error != ERROR_SHARING_VIOLATION && error != ERROR_LOCK_VIOLATION) {
            return INVALID_HANDLE_VALUE;
        }
        Sleep(2);
    }
    return INVALID_HANDLE_VALUE;
}

static bool windows_lock_exclusive(HANDLE file, OVERLAPPED *range) {
    memset(range, 0, sizeof(*range));
    return LockFileEx(file, LOCKFILE_EXCLUSIVE_LOCK, 0, 1, 0, range) != 0;
}

static bool windows_complete_write(HANDLE file, const char *record, size_t length,
                                   LARGE_INTEGER original_size) {
    LARGE_INTEGER end = {.QuadPart = 0};
    if (!SetFilePointerEx(file, end, NULL, FILE_END)) {
        return false;
    }
    size_t written = 0;
    while (written < length) {
        DWORD chunk = length - written > MAXDWORD ? MAXDWORD : (DWORD)(length - written);
        DWORD count = 0;
        if (!WriteFile(file, record + written, chunk, &count, NULL) || count == 0) {
            LARGE_INTEGER rollback = original_size;
            (void)SetFilePointerEx(file, rollback, NULL, FILE_BEGIN);
            (void)SetEndOfFile(file);
            return false;
        }
        written += count;
    }
    return FlushFileBuffers(file) != 0;
}

static bool windows_log_append(const char *log_path, const char *record, size_t record_length,
                               size_t cap_bytes) {
    windows_log_path_t path;
    if (!windows_log_path_open(log_path, &path)) {
        return false;
    }
    bool prepared = windows_private_file_prepare(path.storage, path.lock_base);
    HANDLE lock = prepared ? windows_lock_open(path.lock) : INVALID_HANDLE_VALUE;
    if (lock != INVALID_HANDLE_VALUE) {
        conflict_log_test_hook(CBM_DAEMON_CONFLICT_LOG_BEFORE_SERIALIZATION_LOCK);
    }
    OVERLAPPED lock_range;
    bool lock_acquired = lock != INVALID_HANDLE_VALUE && windows_lock_exclusive(lock, &lock_range);
    if (lock_acquired) {
        conflict_log_test_hook(CBM_DAEMON_CONFLICT_LOG_AFTER_SERIALIZATION_LOCK);
    }

    LARGE_INTEGER size;
    bool ok = lock_acquired && windows_private_file_prepare(path.storage, path.base);
    HANDLE file = ok ? windows_log_open(path.wide, &size) : INVALID_HANDLE_VALUE;
    ok = ok && file != INVALID_HANDLE_VALUE;
    bool rotate = false;
    if (ok && size.QuadPart > 0) {
        uint64_t current = (uint64_t)size.QuadPart;
        rotate = current > cap_bytes || record_length > cap_bytes - (size_t)current;
    }
    if (ok && rotate) {
        (void)CloseHandle(file);
        file = INVALID_HANDLE_VALUE;
        DWORD rotated_attributes = GetFileAttributesW(path.rotated);
        if (rotated_attributes != INVALID_FILE_ATTRIBUTES &&
            (rotated_attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) != 0) {
            ok = false;
        }
        if (ok) {
            ok = MoveFileExW(path.wide, path.rotated,
                             MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH) != 0;
        }
        if (ok) {
            ok = windows_private_file_prepare(path.storage, path.base);
        }
        if (ok) {
            file = windows_log_open(path.wide, &size);
            ok = file != INVALID_HANDLE_VALUE;
        }
    }
    if (ok && size.QuadPart > 0) {
        LARGE_INTEGER offset = {.QuadPart = size.QuadPart - 1};
        char last = '\0';
        DWORD count = 0;
        ok = SetFilePointerEx(file, offset, NULL, FILE_BEGIN) != 0 &&
             ReadFile(file, &last, 1, &count, NULL) != 0 && count == 1 && last == '\n';
    }
    if (ok) {
        ok = windows_complete_write(file, record, record_length, size);
    }
    if (file != INVALID_HANDLE_VALUE) {
        if (!CloseHandle(file)) {
            ok = false;
        }
    }
    if (lock_acquired && !UnlockFileEx(lock, 0, 1, 0, &lock_range)) {
        ok = false;
    }
    if (lock != INVALID_HANDLE_VALUE && !CloseHandle(lock)) {
        ok = false;
    }
    windows_log_path_close(&path);
    return ok;
}

#endif /* _WIN32 */

static bool daemon_fingerprint_snapshot_equal(const daemon_fingerprint_snapshot_t *first,
                                              const daemon_fingerprint_snapshot_t *second) {
    if (!first || !second) {
        return false;
    }
    for (size_t index = 0; index < sizeof(first->field) / sizeof(first->field[0]); index++) {
        if (first->field[index] != second->field[index]) {
            return false;
        }
    }
    return true;
}

static bool daemon_fingerprint_cache_file_snapshot(FILE *file,
                                                   daemon_fingerprint_snapshot_t *snapshot) {
    if (!file || !snapshot) {
        return false;
    }
#ifdef _WIN32
    intptr_t native_handle = _get_osfhandle(cbm_fileno(file));
    return native_handle != -1 &&
           daemon_fingerprint_native_snapshot((uintptr_t)native_handle, snapshot);
#else
    int descriptor = cbm_fileno(file);
    return descriptor >= 0 && fd_regular_current_user(descriptor, NULL) &&
           daemon_fingerprint_native_snapshot((uintptr_t)descriptor, snapshot);
#endif
}

/* A cached digest is authoritative only when its cache file was written after
 * the image and is not future-dated. Both timestamps use the OS wall clock, so
 * cache and image may live on different native volumes (the normal shape for
 * container bind mounts and runtime directories). Equal/coarse timestamps,
 * clock rollback, and a volume whose clock is ahead remain ambiguous and fall
 * back to exact O(image bytes) hashing. The record itself binds the digest to
 * the image's native volume, file identity, size, and timestamps; descriptor
 * snapshots below reject cache or image replacement during admission. A
 * settled installed image retains O(1) cache admission time and memory. */
static bool daemon_fingerprint_cache_newer_than_image(const daemon_fingerprint_snapshot_t *cache,
                                                      const daemon_fingerprint_snapshot_t *image) {
    if (!cache || !image) {
        return false;
    }
#ifdef _WIN32
    FILETIME now_file_time;
    GetSystemTimeAsFileTime(&now_file_time);
    uint64_t now = windows_file_time(now_file_time);
    uint64_t cache_time = cache->field[DAEMON_FINGERPRINT_WINDOWS_WRITE_TIME_FIELD];
    uint64_t image_time = image->field[DAEMON_FINGERPRINT_WINDOWS_WRITE_TIME_FIELD];
    return image_time < cache_time && cache_time <= now;
#else
    struct timespec now;
    if (clock_gettime(CLOCK_REALTIME, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0) {
        return false;
    }
    uint64_t cache_sec = cache->field[DAEMON_FINGERPRINT_POSIX_WRITE_SEC_FIELD];
    uint64_t cache_nsec = cache->field[DAEMON_FINGERPRINT_POSIX_WRITE_NSEC_FIELD];
    uint64_t image_sec = image->field[DAEMON_FINGERPRINT_POSIX_WRITE_SEC_FIELD];
    uint64_t image_nsec = image->field[DAEMON_FINGERPRINT_POSIX_WRITE_NSEC_FIELD];
    uint64_t now_sec = (uint64_t)now.tv_sec;
    uint64_t now_nsec = (uint64_t)now.tv_nsec;
    bool image_before_cache =
        image_sec < cache_sec || (image_sec == cache_sec && image_nsec < cache_nsec);
    bool cache_not_future = cache_sec < now_sec || (cache_sec == now_sec && cache_nsec <= now_nsec);
    return image_before_cache && cache_not_future;
#endif
}

static bool daemon_fingerprint_cache_prefix(const daemon_fingerprint_snapshot_t *snapshot,
                                            char out[DAEMON_SERVICE_FINGERPRINT_RECORD_CAP],
                                            size_t *length_out) {
    if (!snapshot || !out || !length_out) {
        return false;
    }
    int written =
        snprintf(out, DAEMON_SERVICE_FINGERPRINT_RECORD_CAP,
                 "cbm-build-fingerprint-v1\n%016" PRIx64 ":%016" PRIx64 ":%016" PRIx64
                 ":%016" PRIx64 ":%016" PRIx64 ":%016" PRIx64 ":%016" PRIx64 "\n",
                 snapshot->field[0], snapshot->field[1], snapshot->field[2], snapshot->field[3],
                 snapshot->field[4], snapshot->field[5], snapshot->field[6]);
    if (written <= 0 || written >= DAEMON_SERVICE_FINGERPRINT_RECORD_CAP) {
        return false;
    }
    *length_out = (size_t)written;
    return true;
}

static bool daemon_fingerprint_cache_load(const char *cache_path,
                                          const daemon_fingerprint_snapshot_t *image_snapshot,
                                          char out[DAEMON_SERVICE_FINGERPRINT_CACHE_CAP],
                                          size_t *length_out) {
    if (!cache_path || !cache_path[0] || !out || !length_out) {
        return false;
    }
    *length_out = 0;
    FILE *file = cbm_fopen(cache_path, "rb");
    if (!file) {
        return false;
    }
    daemon_fingerprint_snapshot_t before;
    daemon_fingerprint_snapshot_t after;
    bool before_ok = daemon_fingerprint_cache_file_snapshot(file, &before);
    size_t length = fread(out, 1, DAEMON_SERVICE_FINGERPRINT_CACHE_CAP, file);
    int extra = length == DAEMON_SERVICE_FINGERPRINT_CACHE_CAP ? fgetc(file) : EOF;
    bool read_ok =
        before_ok && !ferror(file) && extra == EOF &&
        daemon_fingerprint_cache_file_snapshot(file, &after) &&
        daemon_fingerprint_snapshot_equal(&before, &after) &&
        (!image_snapshot || daemon_fingerprint_cache_newer_than_image(&after, image_snapshot));
    bool close_ok = fclose(file) == 0;
    if (!read_ok || !close_ok) {
        return false;
    }
    *length_out = length;
    return true;
}

static bool daemon_fingerprint_cache_record_valid(
    const char *record, size_t record_length, size_t prefix_length,
    char digest_out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    static const char header[] = "cbm-build-fingerprint-v1\n";
    size_t header_length = sizeof(header) - 1U;
    size_t expected_prefix_length =
        header_length + DAEMON_FINGERPRINT_SNAPSHOT_FIELDS * DAEMON_FINGERPRINT_FIELD_HEX_CHARS +
        (DAEMON_FINGERPRINT_SNAPSHOT_FIELDS - 1U) + 1U;
    if (!record || !digest_out ||
        record_length != prefix_length + CBM_SHA256_HEX_LEN + 1U + CBM_SHA256_HEX_LEN + 1U ||
        prefix_length != expected_prefix_length || memcmp(record, header, header_length) != 0 ||
        record[prefix_length + CBM_SHA256_HEX_LEN] != '\n' || record[record_length - 1U] != '\n') {
        return false;
    }
    size_t position = header_length;
    for (size_t field = 0; field < DAEMON_FINGERPRINT_SNAPSHOT_FIELDS; field++) {
        for (size_t digit = 0; digit < DAEMON_FINGERPRINT_FIELD_HEX_CHARS; digit++) {
            char ch = record[position++];
            if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
                return false;
            }
        }
        char separator = field + 1U == DAEMON_FINGERPRINT_SNAPSHOT_FIELDS ? '\n' : ':';
        if (record[position++] != separator) {
            return false;
        }
    }
    if (position != prefix_length) {
        return false;
    }
    memcpy(digest_out, record + prefix_length, CBM_SHA256_HEX_LEN);
    digest_out[CBM_SHA256_HEX_LEN] = '\0';
    if (!fingerprint_valid(digest_out)) {
        return false;
    }
    char expected_checksum[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    size_t checksum_input_length = prefix_length + CBM_SHA256_HEX_LEN + 1U;
    cbm_sha256_hex(record, checksum_input_length, expected_checksum);
    return memcmp(record + checksum_input_length, expected_checksum, CBM_SHA256_HEX_LEN) == 0;
}

static bool daemon_fingerprint_cache_read(const char *cache_path,
                                          const daemon_fingerprint_snapshot_t *snapshot,
                                          char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE]) {
    if (!cache_path || !cache_path[0] || !snapshot || !out) {
        return false;
    }
    char cache[DAEMON_SERVICE_FINGERPRINT_CACHE_CAP];
    size_t cache_length = 0;
    char prefix[DAEMON_SERVICE_FINGERPRINT_RECORD_CAP];
    size_t prefix_length = 0;
    if (!daemon_fingerprint_cache_load(cache_path, snapshot, cache, &cache_length) ||
        !daemon_fingerprint_cache_prefix(snapshot, prefix, &prefix_length)) {
        return false;
    }
    size_t record_length = prefix_length + CBM_SHA256_HEX_LEN + 1U + CBM_SHA256_HEX_LEN + 1U;
    if (record_length > DAEMON_SERVICE_FINGERPRINT_RECORD_CAP ||
        cache_length % record_length != 0) {
        return false;
    }
    for (size_t offset = 0; offset < cache_length; offset += record_length) {
        char digest[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
        if (memcmp(cache + offset, prefix, prefix_length) == 0 &&
            daemon_fingerprint_cache_record_valid(cache + offset, record_length, prefix_length,
                                                  digest)) {
            memcpy(out, digest, sizeof(digest));
            return true;
        }
    }
    return false;
}

static void daemon_fingerprint_cache_write(const char *cache_path,
                                           const daemon_fingerprint_snapshot_t *snapshot,
                                           const char *digest) {
    if (!cache_path || !cache_path[0] || !snapshot || !fingerprint_valid(digest)) {
        return;
    }
    char record[DAEMON_SERVICE_FINGERPRINT_RECORD_CAP];
    size_t prefix_length = 0;
    if (!daemon_fingerprint_cache_prefix(snapshot, record, &prefix_length)) {
        return;
    }
    memcpy(record + prefix_length, digest, CBM_SHA256_HEX_LEN);
    size_t checksum_input_length = prefix_length + CBM_SHA256_HEX_LEN + 1U;
    record[prefix_length + CBM_SHA256_HEX_LEN] = '\n';
    char checksum[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    cbm_sha256_hex(record, checksum_input_length, checksum);
    memcpy(record + checksum_input_length, checksum, CBM_SHA256_HEX_LEN);
    size_t record_length = checksum_input_length + CBM_SHA256_HEX_LEN;
    record[record_length++] = '\n';
    if (record_length > sizeof(record)) {
        return;
    }

    char previous[DAEMON_SERVICE_FINGERPRINT_CACHE_CAP];
    size_t previous_length = 0;
    bool previous_valid =
        daemon_fingerprint_cache_load(cache_path, NULL, previous, &previous_length) &&
        previous_length % record_length == 0;
    char cache[DAEMON_SERVICE_FINGERPRINT_CACHE_CAP];
    size_t cache_length = 0;
    memcpy(cache, record, record_length);
    cache_length += record_length;
    if (previous_valid) {
        for (size_t offset = 0;
             offset < previous_length && cache_length <= sizeof(cache) - record_length;
             offset += record_length) {
            char previous_digest[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
            const char *previous_record = previous + offset;
            if (memcmp(previous_record, record, prefix_length) != 0 &&
                daemon_fingerprint_cache_record_valid(previous_record, record_length, prefix_length,
                                                      previous_digest)) {
                memcpy(cache + cache_length, previous_record, record_length);
                cache_length += record_length;
            }
        }
    }
    (void)cbm_write_file_atomic(cache_path, cache, cache_length, NULL);
}

bool cbm_daemon_build_fingerprint_native_file_cached(uintptr_t native_file, const char *cache_path,
                                                     bool allow_cache,
                                                     char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE],
                                                     bool *cache_hit_out) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    if (cache_hit_out) {
        *cache_hit_out = false;
    }
    daemon_fingerprint_snapshot_t before;
    daemon_fingerprint_snapshot_t after;
    if (!daemon_fingerprint_native_snapshot(native_file, &before)) {
        return false;
    }
    if (allow_cache && daemon_fingerprint_cache_read(cache_path, &before, out) &&
        daemon_fingerprint_native_snapshot(native_file, &after) &&
        daemon_fingerprint_snapshot_equal(&before, &after)) {
        if (cache_hit_out) {
            *cache_hit_out = true;
        }
        return true;
    }
    out[0] = '\0';
    if (!cbm_daemon_build_fingerprint_native_file(native_file, out) ||
        !daemon_fingerprint_native_snapshot(native_file, &after) ||
        !daemon_fingerprint_snapshot_equal(&before, &after)) {
        out[0] = '\0';
        return false;
    }
    if (allow_cache) {
        daemon_fingerprint_cache_write(cache_path, &after, out);
    }
    return true;
}

#if defined(CBM_CLI_ENABLE_TEST_API)
bool cbm_daemon_build_fingerprint_file_cached_for_testing(
    const char *path, const char *cache_path, bool allow_cache,
    char out[CBM_DAEMON_BUILD_FINGERPRINT_SIZE], bool *cache_hit_out) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
#ifdef _WIN32
    wchar_t *wide = cbm_path_to_wide(path);
    if (!wide) {
        return false;
    }
    HANDLE file =
        CreateFileW(wide, GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_OPEN_REPARSE_POINT | FILE_FLAG_SEQUENTIAL_SCAN, NULL);
    free(wide);
    bool ok = file != INVALID_HANDLE_VALUE &&
              cbm_daemon_build_fingerprint_native_file_cached((uintptr_t)file, cache_path,
                                                              allow_cache, out, cache_hit_out);
    if (file != INVALID_HANDLE_VALUE && !CloseHandle(file)) {
        ok = false;
    }
#else
    int fd = open(path, O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
    bool ok = fd >= 0 && fd_cloexec(fd) &&
              cbm_daemon_build_fingerprint_native_file_cached((uintptr_t)fd, cache_path,
                                                              allow_cache, out, cache_hit_out);
    if (fd >= 0 && close(fd) != 0) {
        ok = false;
    }
#endif
    if (!ok) {
        out[0] = '\0';
    }
    return ok;
}
#endif

bool cbm_daemon_conflict_log_append(const char *log_path, const cbm_daemon_conflict_t *conflict,
                                    size_t cap_bytes) {
    char record[DAEMON_SERVICE_LOG_RECORD_CAP];
    size_t record_length = 0;
    if (cap_bytes == 0 || !conflict_log_record(conflict, record, &record_length)) {
        return false;
    }
#ifdef _WIN32
    return windows_log_append(log_path, record, record_length, cap_bytes);
#else
    return posix_log_append(log_path, record, record_length, cap_bytes);
#endif
}
