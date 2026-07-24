/* private_file_lock.c — Handle-anchored private-file locking. */
#include "foundation/private_file_lock.h"

#include "foundation/macos_acl.h"
#include "foundation/private_file_lock_internal.h"
#include "foundation/platform.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

enum { PRIVATE_FILE_LOCK_PAYLOAD_CAP = 4096 };

#ifndef _WIN32

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

struct cbm_private_lock_directory {
    int fd;
    char *path;
    dev_t device;
    ino_t inode;
    pid_t owner_pid;
    bool test_fail_post_acquire_once;
    bool test_fail_post_acquire_unlock;
    bool test_fail_post_acquire_close;
};

struct cbm_private_file_lock {
    int fd;
    pid_t owner_pid;
    cbm_private_file_lock_mode_t mode;
    struct cbm_private_file_lock *next_tracked;
    bool unlocked;
    bool test_fail_unlock_once;
    bool test_fail_close_once;
    bool test_fail_close_after_consuming_once;
    unsigned int test_unlock_attempts;
    unsigned int test_close_attempts;
};

struct cbm_private_fork_condition {
    pthread_cond_t value;
};

static pthread_once_t private_atfork_once = PTHREAD_ONCE_INIT;
static pthread_mutex_t private_fork_mutex = PTHREAD_MUTEX_INITIALIZER;
static cbm_private_file_lock_t *private_tracked_locks;
static int private_atfork_status = -1;

static void private_atfork_prepare(void) {
    (void)pthread_mutex_lock(&private_fork_mutex);
}

static void private_atfork_parent(void) {
    (void)pthread_mutex_unlock(&private_fork_mutex);
}

static void private_atfork_child(void) {
    cbm_private_file_lock_t *lock = private_tracked_locks;
    while (lock) {
        if (lock->fd >= 0) {
            /* Never LOCK_UN in the child: its descriptor references the same
             * flock description as the parent's still-live descriptor. */
            (void)close(lock->fd);
            lock->fd = -1;
        }
        lock = lock->next_tracked;
    }
    private_tracked_locks = NULL;
    (void)pthread_mutex_unlock(&private_fork_mutex);
}

static void private_atfork_install(void) {
    private_atfork_status =
        pthread_atfork(private_atfork_prepare, private_atfork_parent, private_atfork_child);
}

static bool private_atfork_ensure(void) {
    return pthread_once(&private_atfork_once, private_atfork_install) == 0 &&
           private_atfork_status == 0;
}

bool cbm_private_file_lock_fork_guard_enter(void) {
    return private_atfork_ensure() && pthread_mutex_lock(&private_fork_mutex) == 0;
}

void cbm_private_file_lock_fork_guard_leave(void) {
    (void)pthread_mutex_unlock(&private_fork_mutex);
}

cbm_private_fork_condition_t *cbm_private_fork_condition_new(void) {
    cbm_private_fork_condition_t *condition = calloc(1, sizeof(*condition));
    pthread_condattr_t attributes;
    if (!condition || pthread_condattr_init(&attributes) != 0) {
        free(condition);
        return NULL;
    }
#ifndef __APPLE__
    if (pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC) != 0) {
        (void)pthread_condattr_destroy(&attributes);
        free(condition);
        return NULL;
    }
#endif
    int status = pthread_cond_init(&condition->value, &attributes);
    (void)pthread_condattr_destroy(&attributes);
    if (status != 0) {
        free(condition);
        return NULL;
    }
    return condition;
}

void cbm_private_fork_condition_free(cbm_private_fork_condition_t *condition) {
    if (!condition) {
        return;
    }
    (void)pthread_cond_destroy(&condition->value);
    free(condition);
}

void cbm_private_fork_condition_broadcast_while_guarded(cbm_private_fork_condition_t *condition) {
    if (condition) {
        (void)pthread_cond_broadcast(&condition->value);
    }
}

cbm_private_fork_wait_status_t cbm_private_fork_condition_wait_until_while_guarded(
    cbm_private_fork_condition_t *condition, uint64_t deadline_ms) {
    if (!condition) {
        return CBM_PRIVATE_FORK_WAIT_ERROR;
    }
    int status;
    if (deadline_ms == UINT64_MAX) {
        status = pthread_cond_wait(&condition->value, &private_fork_mutex);
    } else {
        struct timespec timeout;
#ifdef __APPLE__
        uint64_t now_ms = cbm_now_ms();
        uint64_t remaining_ms = deadline_ms > now_ms ? deadline_ms - now_ms : 0;
        timeout.tv_sec = (time_t)(remaining_ms / 1000U);
        timeout.tv_nsec = (long)((remaining_ms % 1000U) * 1000000U);
        status =
            pthread_cond_timedwait_relative_np(&condition->value, &private_fork_mutex, &timeout);
#else
        timeout.tv_sec = (time_t)(deadline_ms / 1000U);
        timeout.tv_nsec = (long)((deadline_ms % 1000U) * 1000000U);
        status = pthread_cond_timedwait(&condition->value, &private_fork_mutex, &timeout);
#endif
    }
    if (status == 0) {
        return CBM_PRIVATE_FORK_WAIT_SIGNALED;
    }
    return status == ETIMEDOUT ? CBM_PRIVATE_FORK_WAIT_TIMEOUT : CBM_PRIVATE_FORK_WAIT_ERROR;
}

static bool private_fd_set_cloexec(int fd) {
    int flags = fcntl(fd, F_GETFD);
    return flags >= 0 && fcntl(fd, F_SETFD, flags | FD_CLOEXEC) == 0;
}

static bool private_directory_revalidate(const cbm_private_lock_directory_t *directory) {
    struct stat by_handle;
    struct stat by_path;
    return directory && directory->owner_pid == getpid() && directory->fd >= 0 &&
           fstat(directory->fd, &by_handle) == 0 && lstat(directory->path, &by_path) == 0 &&
           S_ISDIR(by_handle.st_mode) && S_ISDIR(by_path.st_mode) &&
           by_handle.st_dev == directory->device && by_handle.st_ino == directory->inode &&
           by_path.st_dev == directory->device && by_path.st_ino == directory->inode &&
           by_handle.st_uid == geteuid() && by_path.st_uid == geteuid() &&
           (by_handle.st_mode & 07777) == 0700 && (by_path.st_mode & 07777) == 0700 &&
           cbm_macos_extended_acl_fd_is_empty(directory->fd);
}

static bool private_base_name_valid(const char *base_name) {
    if (!base_name || !base_name[0] || strcmp(base_name, ".") == 0 ||
        strcmp(base_name, "..") == 0) {
        return false;
    }
    size_t length = strlen(base_name);
    if (length > NAME_MAX) {
        return false;
    }
    for (size_t index = 0; index < length; index++) {
        char ch = base_name[index];
        if (!((ch >= 'a' && ch <= 'z') || (ch >= '0' && ch <= '9') || ch == '-' || ch == '_' ||
              ch == '.')) {
            return false;
        }
    }
    return true;
}

static bool private_file_revalidate(const cbm_private_lock_directory_t *directory,
                                    const char *base_name, int fd, const struct stat *expected) {
    struct stat by_handle;
    struct stat by_path;
    return private_directory_revalidate(directory) && fstat(fd, &by_handle) == 0 &&
           S_ISREG(by_handle.st_mode) && by_handle.st_uid == geteuid() && by_handle.st_nlink == 1 &&
           (by_handle.st_mode & 07777) == 0600 &&
           fstatat(directory->fd, base_name, &by_path, AT_SYMLINK_NOFOLLOW) == 0 &&
           S_ISREG(by_path.st_mode) && by_path.st_uid == geteuid() && by_path.st_nlink == 1 &&
           (by_path.st_mode & 07777) == 0600 && by_path.st_dev == by_handle.st_dev &&
           by_path.st_ino == by_handle.st_ino && cbm_macos_extended_acl_fd_is_empty(fd) &&
           (!expected ||
            (expected->st_dev == by_handle.st_dev && expected->st_ino == by_handle.st_ino));
}

static cbm_private_file_lock_status_t private_open_failure_status(
    const cbm_private_lock_directory_t *directory, const char *base_name) {
    struct stat status;
    if (fstatat(directory->fd, base_name, &status, AT_SYMLINK_NOFOLLOW) == 0) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    return errno == ENOENT ? CBM_PRIVATE_FILE_LOCK_IO : CBM_PRIVATE_FILE_LOCK_UNSAFE;
}

static int private_flock_set(int fd, int operation) {
    int result;
    do {
        result = flock(fd, operation);
    } while (result != 0 && errno == EINTR);
    return result;
}

static int private_release_unlock(cbm_private_file_lock_t *lock) {
    lock->test_unlock_attempts++;
    if (lock->test_fail_unlock_once) {
        lock->test_fail_unlock_once = false;
        errno = EIO;
        return -1;
    }
    return private_flock_set(lock->fd, LOCK_UN);
}

static int private_release_close(cbm_private_file_lock_t *lock, bool *attempted_out) {
    *attempted_out = false;
    lock->test_close_attempts++;
    if (lock->test_fail_close_once) {
        lock->test_fail_close_once = false;
        errno = EIO;
        return -1;
    }
    *attempted_out = true;
    if (lock->test_fail_close_after_consuming_once) {
        lock->test_fail_close_after_consuming_once = false;
        (void)close(lock->fd);
        errno = EIO;
        return -1;
    }
    return close(lock->fd);
}

cbm_private_file_lock_status_t cbm_private_lock_directory_adopt_posix(
    int directory_fd, const char *stable_path, cbm_private_lock_directory_t **directory_out) {
    if (directory_out) {
        *directory_out = NULL;
    }
    if (directory_fd < 0 || !stable_path || !stable_path[0] || !directory_out ||
        !private_fd_set_cloexec(directory_fd)) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    struct stat status;
    struct stat by_path;
    if (fstat(directory_fd, &status) != 0 || lstat(stable_path, &by_path) != 0 ||
        !S_ISDIR(status.st_mode) || !S_ISDIR(by_path.st_mode) || status.st_uid != geteuid() ||
        by_path.st_uid != geteuid() || (status.st_mode & 07777) != 0700 ||
        (by_path.st_mode & 07777) != 0700 || status.st_dev != by_path.st_dev ||
        status.st_ino != by_path.st_ino || !cbm_macos_extended_acl_fd_is_empty(directory_fd)) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    cbm_private_lock_directory_t *directory = calloc(1, sizeof(*directory));
    if (directory) {
        directory->path = strdup(stable_path);
    }
    if (!directory || !directory->path) {
        free(directory);
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    directory->fd = directory_fd;
    directory->device = status.st_dev;
    directory->inode = status.st_ino;
    directory->owner_pid = getpid();
    *directory_out = directory;
    return CBM_PRIVATE_FILE_LOCK_OK;
}

cbm_private_file_lock_status_t cbm_private_file_lock_try_acquire(
    cbm_private_lock_directory_t *directory, const char *base_name,
    cbm_private_file_lock_mode_t mode, cbm_private_file_lock_t **lock_out) {
    if (lock_out) {
        *lock_out = NULL;
    }
    if (!directory || !lock_out || !private_base_name_valid(base_name) ||
        (mode != CBM_PRIVATE_FILE_LOCK_SH && mode != CBM_PRIVATE_FILE_LOCK_EX)) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!private_directory_revalidate(directory)) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!cbm_private_file_lock_fork_guard_enter()) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }

    int flags = O_RDWR | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    bool created = false;
    int fd = openat(directory->fd, base_name, flags | O_CREAT | O_EXCL, 0600);
    if (fd >= 0) {
        created = true;
    } else if (errno == EEXIST) {
        fd = openat(directory->fd, base_name, flags);
    }
    if (fd < 0) {
        cbm_private_file_lock_status_t status = private_open_failure_status(directory, base_name);
        cbm_private_file_lock_fork_guard_leave();
        return status;
    }

    struct stat initial;
    bool initial_ok = private_fd_set_cloexec(fd) && fstat(fd, &initial) == 0;
    if (initial_ok && created) {
        initial_ok = fchmod(fd, 0600) == 0 && fstat(fd, &initial) == 0;
    }
    if (!initial_ok || !private_file_revalidate(directory, base_name, fd, &initial)) {
        (void)close(fd);
        cbm_private_file_lock_fork_guard_leave();
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }

    cbm_private_file_lock_t *lock = calloc(1, sizeof(*lock));
    if (!lock) {
        (void)close(fd);
        cbm_private_file_lock_fork_guard_leave();
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    lock->fd = fd;
    lock->owner_pid = getpid();
    lock->mode = mode;

    int operation = mode == CBM_PRIVATE_FILE_LOCK_SH ? LOCK_SH : LOCK_EX;
    if (private_flock_set(fd, operation | LOCK_NB) != 0) {
        int lock_error = errno;
        (void)close(fd);
        free(lock);
        cbm_private_file_lock_fork_guard_leave();
        return lock_error == EWOULDBLOCK || lock_error == EAGAIN ? CBM_PRIVATE_FILE_LOCK_BUSY
                                                                 : CBM_PRIVATE_FILE_LOCK_IO;
    }
    lock->next_tracked = private_tracked_locks;
    private_tracked_locks = lock;

    bool forced_cleanup = directory->test_fail_post_acquire_once;
    bool fail_cleanup_unlock = directory->test_fail_post_acquire_unlock;
    bool fail_cleanup_close = directory->test_fail_post_acquire_close;
    directory->test_fail_post_acquire_once = false;
    directory->test_fail_post_acquire_unlock = false;
    directory->test_fail_post_acquire_close = false;
    if (forced_cleanup || !private_file_revalidate(directory, base_name, fd, &initial)) {
        cbm_private_file_lock_status_t failure_status =
            forced_cleanup ? CBM_PRIVATE_FILE_LOCK_IO : CBM_PRIVATE_FILE_LOCK_UNSAFE;
        lock->test_fail_unlock_once = fail_cleanup_unlock;
        lock->test_fail_close_once = fail_cleanup_close;
        *lock_out = lock;
        cbm_private_file_lock_fork_guard_leave();
        cbm_private_file_lock_status_t cleanup_status = cbm_private_file_lock_release(lock_out);
        return cleanup_status == CBM_PRIVATE_FILE_LOCK_OK ? failure_status
                                                          : CBM_PRIVATE_FILE_LOCK_IO;
    }
    *lock_out = lock;
    cbm_private_file_lock_fork_guard_leave();
    return CBM_PRIVATE_FILE_LOCK_OK;
}

static bool private_lock_is_tracked(const cbm_private_file_lock_t *lock) {
    for (const cbm_private_file_lock_t *cursor = private_tracked_locks; cursor;
         cursor = cursor->next_tracked) {
        if (cursor == lock) {
            return true;
        }
    }
    return false;
}

static bool private_payload_fd_valid(const cbm_private_file_lock_t *lock, struct stat *status_out) {
    struct stat status;
    bool valid = lock && lock->fd >= 0 && !lock->unlocked && lock->owner_pid == getpid() &&
                 private_lock_is_tracked(lock) && fstat(lock->fd, &status) == 0 &&
                 S_ISREG(status.st_mode) && status.st_uid == geteuid() && status.st_nlink == 1 &&
                 (status.st_mode & 07777) == 0600 && status.st_size >= 0 &&
                 cbm_macos_extended_acl_fd_is_empty(lock->fd);
    if (valid && status_out) {
        *status_out = status;
    }
    return valid;
}

cbm_private_file_lock_status_t cbm_private_file_lock_payload_read(cbm_private_file_lock_t *lock,
                                                                  void *buffer, size_t capacity,
                                                                  size_t *length_out) {
    if (length_out) {
        *length_out = 0;
    }
    if (!lock || !buffer || capacity == 0 || !length_out) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!cbm_private_file_lock_fork_guard_enter()) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    struct stat status;
    bool metadata_safe = private_payload_fd_valid(lock, &status);
    bool valid = metadata_safe && (uintmax_t)status.st_size <= PRIVATE_FILE_LOCK_PAYLOAD_CAP &&
                 (uintmax_t)status.st_size <= capacity;
    size_t length = valid ? (size_t)status.st_size : 0;
    size_t offset = 0;
    while (valid && offset < length) {
        ssize_t count =
            pread(lock->fd, (unsigned char *)buffer + offset, length - offset, (off_t)offset);
        if (count > 0) {
            offset += (size_t)count;
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            valid = false;
        }
    }
    cbm_private_file_lock_fork_guard_leave();
    if (!metadata_safe) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!valid) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    *length_out = length;
    return CBM_PRIVATE_FILE_LOCK_OK;
}

cbm_private_file_lock_status_t cbm_private_file_lock_payload_write(cbm_private_file_lock_t *lock,
                                                                   const void *buffer,
                                                                   size_t length) {
    if (!lock || !buffer || length == 0 || length > PRIVATE_FILE_LOCK_PAYLOAD_CAP ||
        lock->mode != CBM_PRIVATE_FILE_LOCK_EX) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!cbm_private_file_lock_fork_guard_enter()) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    bool metadata_safe = private_payload_fd_valid(lock, NULL);
    bool valid = metadata_safe && ftruncate(lock->fd, 0) == 0;
    size_t offset = 0;
    while (valid && offset < length) {
        ssize_t count = pwrite(lock->fd, (const unsigned char *)buffer + offset, length - offset,
                               (off_t)offset);
        if (count > 0) {
            offset += (size_t)count;
        } else if (count < 0 && errno == EINTR) {
            continue;
        } else {
            valid = false;
        }
    }
    valid = valid && ftruncate(lock->fd, (off_t)length) == 0;
    if (valid) {
        int sync_status;
        do {
            sync_status = fsync(lock->fd);
        } while (sync_status != 0 && errno == EINTR);
        valid = sync_status == 0;
    }
    cbm_private_file_lock_fork_guard_leave();
    if (!metadata_safe) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    return valid ? CBM_PRIVATE_FILE_LOCK_OK : CBM_PRIVATE_FILE_LOCK_IO;
}

cbm_private_file_lock_status_t cbm_private_file_lock_release(cbm_private_file_lock_t **lock_io) {
    if (!lock_io || !*lock_io) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    cbm_private_file_lock_t *lock = *lock_io;
    if (lock->owner_pid != getpid() || lock->fd < 0) {
        *lock_io = NULL;
        free(lock);
        return CBM_PRIVATE_FILE_LOCK_OK;
    }
    if (!cbm_private_file_lock_fork_guard_enter()) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    cbm_private_file_lock_t **cursor = &private_tracked_locks;
    while (*cursor && *cursor != lock) {
        cursor = &(*cursor)->next_tracked;
    }
    if (*cursor != lock) {
        cbm_private_file_lock_fork_guard_leave();
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    if (!lock->unlocked) {
        if (private_release_unlock(lock) != 0) {
            cbm_private_file_lock_fork_guard_leave();
            return CBM_PRIVATE_FILE_LOCK_IO;
        }
        lock->unlocked = true;
    }
    bool close_attempted = false;
    int close_status = private_release_close(lock, &close_attempted);
    if (close_status != 0 && !close_attempted) {
        cbm_private_file_lock_fork_guard_leave();
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    lock->fd = -1;
    *cursor = lock->next_tracked;
    cbm_private_file_lock_fork_guard_leave();
    *lock_io = NULL;
    free(lock);
    return close_status == 0 ? CBM_PRIVATE_FILE_LOCK_OK : CBM_PRIVATE_FILE_LOCK_IO;
}

void cbm_private_lock_directory_close(cbm_private_lock_directory_t *directory) {
    if (!directory) {
        return;
    }
    if (directory->fd >= 0) {
        (void)close(directory->fd);
    }
    free(directory->path);
    free(directory);
}

const char *cbm_private_lock_directory_path(const cbm_private_lock_directory_t *directory) {
    return directory ? directory->path : NULL;
}

bool cbm_private_file_lock_is_cloexec_for_test(const cbm_private_file_lock_t *lock) {
    if (!lock || lock->fd < 0) {
        return false;
    }
    int flags = fcntl(lock->fd, F_GETFD);
    return flags >= 0 && (flags & FD_CLOEXEC) != 0;
}

bool cbm_private_file_lock_unlock_complete(const cbm_private_file_lock_t *lock) {
    return lock && lock->unlocked;
}

bool cbm_private_lock_directory_fail_post_acquire_cleanup_for_test(
    cbm_private_lock_directory_t *directory, bool fail_unlock, bool fail_close) {
    if (!directory || (!fail_unlock && !fail_close)) {
        return false;
    }
    directory->test_fail_post_acquire_once = true;
    directory->test_fail_post_acquire_unlock = fail_unlock;
    directory->test_fail_post_acquire_close = fail_close;
    return true;
}

bool cbm_private_file_lock_fail_next_release_step_for_test(
    cbm_private_file_lock_t *lock, cbm_private_file_lock_release_step_t step) {
    if (!lock) {
        return false;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK) {
        lock->test_fail_unlock_once = true;
        return true;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE) {
        lock->test_fail_close_once = true;
        return true;
    }
    return false;
}

unsigned int cbm_private_file_lock_release_step_attempts_for_test(
    const cbm_private_file_lock_t *lock, cbm_private_file_lock_release_step_t step) {
    if (!lock) {
        return 0;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK) {
        return lock->test_unlock_attempts;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE) {
        return lock->test_close_attempts;
    }
    return 0;
}

bool cbm_private_file_lock_fail_close_after_consuming_for_test(cbm_private_file_lock_t *lock) {
    if (!lock) {
        return false;
    }
    lock->test_fail_close_after_consuming_once = true;
    return true;
}

int cbm_private_file_lock_native_fd_for_test(const cbm_private_file_lock_t *lock) {
    return lock ? lock->fd : -1;
}

#else

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#include <aclapi.h>
#include <wchar.h>

typedef BOOL(WINAPI *private_open_process_token_fn)(HANDLE, DWORD, PHANDLE);
typedef BOOL(WINAPI *private_get_token_information_fn)(HANDLE, TOKEN_INFORMATION_CLASS, LPVOID,
                                                       DWORD, PDWORD);
typedef DWORD(WINAPI *private_get_length_sid_fn)(PSID);
typedef BOOL(WINAPI *private_copy_sid_fn)(DWORD, PSID, PSID);
typedef BOOL(WINAPI *private_equal_sid_fn)(PSID, PSID);
typedef BOOL(WINAPI *private_is_valid_sid_fn)(PSID);
typedef BOOL(WINAPI *private_initialize_acl_fn)(PACL, DWORD, DWORD);
typedef BOOL(WINAPI *private_add_access_allowed_ace_fn)(PACL, DWORD, DWORD, PSID);
typedef BOOL(WINAPI *private_initialize_security_descriptor_fn)(PSECURITY_DESCRIPTOR, DWORD);
typedef BOOL(WINAPI *private_set_security_descriptor_owner_fn)(PSECURITY_DESCRIPTOR, PSID, BOOL);
typedef BOOL(WINAPI *private_set_security_descriptor_dacl_fn)(PSECURITY_DESCRIPTOR, BOOL, PACL,
                                                              BOOL);
typedef BOOL(WINAPI *private_set_security_descriptor_control_fn)(PSECURITY_DESCRIPTOR,
                                                                 SECURITY_DESCRIPTOR_CONTROL,
                                                                 SECURITY_DESCRIPTOR_CONTROL);
typedef BOOL(WINAPI *private_get_security_descriptor_control_fn)(PSECURITY_DESCRIPTOR,
                                                                 PSECURITY_DESCRIPTOR_CONTROL,
                                                                 LPDWORD);
typedef BOOL(WINAPI *private_get_acl_information_fn)(PACL, LPVOID, DWORD, ACL_INFORMATION_CLASS);
typedef BOOL(WINAPI *private_get_ace_fn)(PACL, DWORD, LPVOID *);
typedef DWORD(WINAPI *private_get_security_info_fn)(HANDLE, SE_OBJECT_TYPE, SECURITY_INFORMATION,
                                                    PSID *, PSID *, PACL *, PACL *,
                                                    PSECURITY_DESCRIPTOR *);

typedef struct {
    HMODULE advapi;
    private_open_process_token_fn open_process_token;
    private_get_token_information_fn get_token_information;
    private_get_length_sid_fn get_length_sid;
    private_copy_sid_fn copy_sid;
    private_equal_sid_fn equal_sid;
    private_is_valid_sid_fn is_valid_sid;
    private_initialize_acl_fn initialize_acl;
    private_add_access_allowed_ace_fn add_access_allowed_ace;
    private_initialize_security_descriptor_fn initialize_security_descriptor;
    private_set_security_descriptor_dacl_fn set_security_descriptor_dacl;
    private_set_security_descriptor_owner_fn set_security_descriptor_owner;
    private_set_security_descriptor_control_fn set_security_descriptor_control;
    private_get_security_descriptor_control_fn get_security_descriptor_control;
    private_get_acl_information_fn get_acl_information;
    private_get_ace_fn get_ace;
    private_get_security_info_fn get_security_info;
    PSID user_sid;
    PACL acl;
    PSECURITY_DESCRIPTOR descriptor;
    SECURITY_ATTRIBUTES attributes;
} private_win_security_t;

typedef struct {
    DWORD volume_serial;
    DWORD index_high;
    DWORD index_low;
} private_win_identity_t;

struct cbm_private_lock_directory {
    HANDLE handle;
    char *path;
    wchar_t *wide_path;
    private_win_identity_t identity;
    private_win_security_t security;
    bool test_fail_post_acquire_once;
    bool test_fail_post_acquire_unlock;
    bool test_fail_post_acquire_close;
    bool test_fail_lock_attempt_once;
    bool test_fail_lock_attempt_close;
};

struct cbm_private_file_lock {
    HANDLE handle;
    OVERLAPPED range;
    cbm_private_file_lock_mode_t mode;
    bool unlocked;
    bool test_fail_unlock_once;
    bool test_fail_close_once;
    unsigned int test_unlock_attempts;
    unsigned int test_close_attempts;
};

struct cbm_private_fork_condition {
    CONDITION_VARIABLE value;
};

static INIT_ONCE private_win_gate_once = INIT_ONCE_STATIC_INIT;
static CRITICAL_SECTION private_win_gate;

static BOOL CALLBACK private_win_gate_initialize(PINIT_ONCE once, PVOID parameter, PVOID *context) {
    (void)once;
    (void)parameter;
    (void)context;
    return InitializeCriticalSectionAndSpinCount(&private_win_gate, 4000);
}

static void private_win_security_destroy(private_win_security_t *security) {
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

static void *private_win_token_user_query(private_win_security_t *security, HANDLE token,
                                          PSID *sid_out) {
    DWORD needed = 0;
    (void)security->get_token_information(token, TokenUser, NULL, 0, &needed);
    if (needed == 0 || GetLastError() != ERROR_INSUFFICIENT_BUFFER) {
        return NULL;
    }
    void *buffer = calloc(1, needed);
    if (!buffer || !security->get_token_information(token, TokenUser, buffer, needed, &needed)) {
        free(buffer);
        return NULL;
    }
    *sid_out = ((TOKEN_USER *)buffer)->User.Sid;
    return buffer;
}

#define PRIVATE_RESOLVE_ADVAPI(context, member, type, symbol)                  \
    do {                                                                       \
        (context)->member = (type)GetProcAddress((context)->advapi, (symbol)); \
        if (!(context)->member) {                                              \
            private_win_security_destroy((context));                           \
            return false;                                                      \
        }                                                                      \
    } while (0)

static bool private_win_security_init(private_win_security_t *security) {
    memset(security, 0, sizeof(*security));
    security->advapi = LoadLibraryW(L"advapi32.dll");
    if (!security->advapi) {
        return false;
    }
    PRIVATE_RESOLVE_ADVAPI(security, open_process_token, private_open_process_token_fn,
                           "OpenProcessToken");
    PRIVATE_RESOLVE_ADVAPI(security, get_token_information, private_get_token_information_fn,
                           "GetTokenInformation");
    PRIVATE_RESOLVE_ADVAPI(security, get_length_sid, private_get_length_sid_fn, "GetLengthSid");
    PRIVATE_RESOLVE_ADVAPI(security, copy_sid, private_copy_sid_fn, "CopySid");
    PRIVATE_RESOLVE_ADVAPI(security, equal_sid, private_equal_sid_fn, "EqualSid");
    PRIVATE_RESOLVE_ADVAPI(security, is_valid_sid, private_is_valid_sid_fn, "IsValidSid");
    PRIVATE_RESOLVE_ADVAPI(security, initialize_acl, private_initialize_acl_fn, "InitializeAcl");
    PRIVATE_RESOLVE_ADVAPI(security, add_access_allowed_ace, private_add_access_allowed_ace_fn,
                           "AddAccessAllowedAce");
    PRIVATE_RESOLVE_ADVAPI(security, initialize_security_descriptor,
                           private_initialize_security_descriptor_fn,
                           "InitializeSecurityDescriptor");
    PRIVATE_RESOLVE_ADVAPI(security, set_security_descriptor_dacl,
                           private_set_security_descriptor_dacl_fn, "SetSecurityDescriptorDacl");
    PRIVATE_RESOLVE_ADVAPI(security, set_security_descriptor_owner,
                           private_set_security_descriptor_owner_fn, "SetSecurityDescriptorOwner");
    PRIVATE_RESOLVE_ADVAPI(security, set_security_descriptor_control,
                           private_set_security_descriptor_control_fn,
                           "SetSecurityDescriptorControl");
    PRIVATE_RESOLVE_ADVAPI(security, get_security_descriptor_control,
                           private_get_security_descriptor_control_fn,
                           "GetSecurityDescriptorControl");
    PRIVATE_RESOLVE_ADVAPI(security, get_acl_information, private_get_acl_information_fn,
                           "GetAclInformation");
    PRIVATE_RESOLVE_ADVAPI(security, get_ace, private_get_ace_fn, "GetAce");
    PRIVATE_RESOLVE_ADVAPI(security, get_security_info, private_get_security_info_fn,
                           "GetSecurityInfo");

    HANDLE token = NULL;
    if (!security->open_process_token(GetCurrentProcess(), TOKEN_QUERY, &token)) {
        private_win_security_destroy(security);
        return false;
    }
    PSID token_sid = NULL;
    void *token_user = private_win_token_user_query(security, token, &token_sid);
    (void)CloseHandle(token);
    if (!token_user || !token_sid || !security->is_valid_sid(token_sid)) {
        free(token_user);
        private_win_security_destroy(security);
        return false;
    }
    DWORD sid_length = security->get_length_sid(token_sid);
    security->user_sid = malloc(sid_length);
    if (sid_length == 0 || !security->user_sid ||
        !security->copy_sid(sid_length, security->user_sid, token_sid)) {
        free(token_user);
        private_win_security_destroy(security);
        return false;
    }
    free(token_user);

    DWORD acl_size = (DWORD)(sizeof(ACL) + sizeof(ACCESS_ALLOWED_ACE) - sizeof(DWORD));
    if (sid_length > MAXDWORD - acl_size) {
        private_win_security_destroy(security);
        return false;
    }
    acl_size += sid_length;
    security->acl = malloc(acl_size);
    security->descriptor = malloc(SECURITY_DESCRIPTOR_MIN_LENGTH);
    if (!security->acl || !security->descriptor ||
        !security->initialize_acl(security->acl, acl_size, ACL_REVISION) ||
        !security->add_access_allowed_ace(security->acl, ACL_REVISION, FILE_ALL_ACCESS,
                                          security->user_sid) ||
        !security->initialize_security_descriptor(security->descriptor,
                                                  SECURITY_DESCRIPTOR_REVISION) ||
        !security->set_security_descriptor_dacl(security->descriptor, TRUE, security->acl, FALSE) ||
        /* Stamp the exact token-user SID as owner at creation: admin-group
         * tokens can default new objects to BUILTIN\Administrators (standard
         * on Windows Server), and private_win_owner_only_dacl demands the
         * exact user SID — without this every lock file the process creates
         * fails its own validation. */
        !security->set_security_descriptor_owner(security->descriptor, security->user_sid, FALSE) ||
        !security->set_security_descriptor_control(security->descriptor, SE_DACL_PROTECTED,
                                                   SE_DACL_PROTECTED)) {
        private_win_security_destroy(security);
        return false;
    }
    security->attributes.nLength = sizeof(security->attributes);
    security->attributes.lpSecurityDescriptor = security->descriptor;
    security->attributes.bInheritHandle = FALSE;
    return true;
}

#undef PRIVATE_RESOLVE_ADVAPI

static bool private_win_handle_is_noninheritable(HANDLE handle) {
    DWORD flags = 0;
    return handle && handle != INVALID_HANDLE_VALUE && GetHandleInformation(handle, &flags) != 0 &&
           (flags & HANDLE_FLAG_INHERIT) == 0;
}

static bool private_win_make_noninheritable(HANDLE handle) {
    return handle && handle != INVALID_HANDLE_VALUE &&
           SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) != 0 &&
           private_win_handle_is_noninheritable(handle);
}

static private_win_identity_t private_win_identity(const BY_HANDLE_FILE_INFORMATION *information) {
    private_win_identity_t identity;
    identity.volume_serial = information->dwVolumeSerialNumber;
    identity.index_high = information->nFileIndexHigh;
    identity.index_low = information->nFileIndexLow;
    return identity;
}

static bool private_win_identity_equal(const private_win_identity_t *left,
                                       const private_win_identity_t *right) {
    return left->volume_serial == right->volume_serial && left->index_high == right->index_high &&
           left->index_low == right->index_low;
}

static bool private_win_handle_has_local_dos_path(HANDLE handle, wchar_t expected_drive) {
    DWORD capacity =
        GetFinalPathNameByHandleW(handle, NULL, 0, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
    if (capacity < 8 || capacity > 32768) {
        return false;
    }
    wchar_t *path = malloc((size_t)capacity * sizeof(*path));
    if (!path) {
        return false;
    }
    DWORD length =
        GetFinalPathNameByHandleW(handle, path, capacity, FILE_NAME_NORMALIZED | VOLUME_NAME_DOS);
    bool valid = length >= 7 && length < capacity && path[0] == L'\\' && path[1] == L'\\' &&
                 path[2] == L'?' && path[3] == L'\\' && path[5] == L':' && path[6] == L'\\';
    wchar_t actual_drive = valid ? path[4] : L'\0';
    if (actual_drive >= L'a' && actual_drive <= L'z') {
        actual_drive -= L'a' - L'A';
    }
    if (expected_drive >= L'a' && expected_drive <= L'z') {
        expected_drive -= L'a' - L'A';
    }
    valid = valid && actual_drive == expected_drive;
    free(path);
    return valid;
}

static bool private_win_owner_only_dacl(private_win_security_t *security, HANDLE handle) {
    PSID owner = NULL;
    PACL dacl = NULL;
    PSECURITY_DESCRIPTOR descriptor = NULL;
    DWORD result = security->get_security_info(
        handle, SE_FILE_OBJECT, OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION, &owner,
        NULL, &dacl, NULL, &descriptor);
    SECURITY_DESCRIPTOR_CONTROL control = 0;
    DWORD revision = 0;
    ACL_SIZE_INFORMATION acl_information;
    memset(&acl_information, 0, sizeof(acl_information));
    LPVOID opaque_ace = NULL;
    bool valid = result == ERROR_SUCCESS && descriptor && owner && dacl &&
                 security->is_valid_sid(owner) && security->equal_sid(owner, security->user_sid) &&
                 security->get_security_descriptor_control(descriptor, &control, &revision) &&
                 (control & SE_DACL_PRESENT) != 0 && (control & SE_DACL_PROTECTED) != 0 &&
                 security->get_acl_information(dacl, &acl_information, sizeof(acl_information),
                                               AclSizeInformation) &&
                 acl_information.AceCount == 1 && security->get_ace(dacl, 0, &opaque_ace) &&
                 opaque_ace;
    if (valid) {
        ACCESS_ALLOWED_ACE *ace = (ACCESS_ALLOWED_ACE *)opaque_ace;
        PSID ace_sid = (PSID)&ace->SidStart;
        valid = ace->Header.AceType == ACCESS_ALLOWED_ACE_TYPE &&
                ace->Header.AceSize >= sizeof(ACCESS_ALLOWED_ACE) &&
                (ace->Header.AceFlags & (INHERITED_ACE | INHERIT_ONLY_ACE)) == 0 &&
                security->is_valid_sid(ace_sid) &&
                security->equal_sid(ace_sid, security->user_sid) &&
                (ace->Mask == FILE_ALL_ACCESS || ace->Mask == GENERIC_ALL);
    }
    if (descriptor) {
        (void)LocalFree(descriptor);
    }
    return valid;
}

static bool private_win_path_character_forbidden(wchar_t character) {
    return character == L':' || character == L'*' || character == L'?' || character == L'"' ||
           character == L'<' || character == L'>' || character == L'|';
}

static bool private_win_path_syntax_valid(const wchar_t *path) {
    size_t length = path ? wcslen(path) : 0;
    bool drive_absolute =
        length >= 3 &&
        ((path[0] >= L'A' && path[0] <= L'Z') || (path[0] >= L'a' && path[0] <= L'z')) &&
        path[1] == L':' && path[2] == L'\\';
    if (!drive_absolute) {
        /* Reject UNC, device, NT-object and relative namespaces. */
        return false;
    }
    size_t component_start = 3;
    for (size_t index = component_start; index <= length; index++) {
        if (index < length && path[index] != L'\\') {
            if (private_win_path_character_forbidden(path[index])) {
                return false;
            }
            continue;
        }
        if (index == component_start) {
            return length == 3 && index == length;
        }
        size_t component_length = index - component_start;
        const wchar_t *component = path + component_start;
        if ((component_length == 1 && component[0] == L'.') ||
            (component_length == 2 && component[0] == L'.' && component[1] == L'.') ||
            component[component_length - 1] == L'.' || component[component_length - 1] == L' ') {
            return false;
        }
        component_start = index + 1;
    }
    return true;
}

static cbm_private_file_lock_status_t private_win_path_from_utf8(const char *path,
                                                                 wchar_t **wide_out) {
    *wide_out = NULL;
    int needed = path ? MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, NULL, 0) : 0;
    if (needed <= 0 || needed > MAX_PATH) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    wchar_t *wide = malloc((size_t)needed * sizeof(*wide));
    if (!wide) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    if (MultiByteToWideChar(CP_UTF8, MB_ERR_INVALID_CHARS, path, -1, wide, needed) <= 0) {
        free(wide);
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    for (int index = 0; index < needed - 1; index++) {
        if (wide[index] == L'/') {
            wide[index] = L'\\';
        }
    }
    size_t length = wcslen(wide);
    while (length > 3 && wide[length - 1] == L'\\') {
        wide[--length] = L'\0';
    }
    if (!private_win_path_syntax_valid(wide)) {
        free(wide);
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    *wide_out = wide;
    return CBM_PRIVATE_FILE_LOCK_OK;
}

static bool private_win_path_tree_is_plain_local(const wchar_t *path) {
    if (!private_win_path_syntax_valid(path)) {
        return false;
    }
    wchar_t volume_root[4] = {path[0], L':', L'\\', L'\0'};
    UINT drive_type = GetDriveTypeW(volume_root);
    if (drive_type != DRIVE_FIXED && drive_type != DRIVE_REMOVABLE && drive_type != DRIVE_RAMDISK) {
        return false;
    }
    DWORD filesystem_flags = 0;
    if (!GetVolumeInformationW(volume_root, NULL, 0, NULL, NULL, &filesystem_flags, NULL, 0) ||
        (filesystem_flags & FILE_PERSISTENT_ACLS) == 0) {
        return false;
    }

    size_t length = wcslen(path);
    wchar_t *partial = malloc((length + 1) * sizeof(*partial));
    if (!partial) {
        return false;
    }
    memcpy(partial, path, (length + 1) * sizeof(*partial));
    bool valid = true;
    for (size_t index = 3; valid && index <= length; index++) {
        if (index < length && partial[index] != L'\\') {
            continue;
        }
        wchar_t saved = partial[index];
        partial[index] = L'\0';
        HANDLE component = CreateFileW(
            partial, FILE_READ_ATTRIBUTES, FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            NULL, OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
        BY_HANDLE_FILE_INFORMATION information;
        valid = component != INVALID_HANDLE_VALUE && GetFileType(component) == FILE_TYPE_DISK &&
                GetFileInformationByHandle(component, &information) != 0 &&
                (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0;
        if (component != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(component);
        }
        partial[index] = saved;
    }
    free(partial);
    return valid;
}

static bool private_win_directory_handle_valid(cbm_private_lock_directory_t *directory,
                                               BY_HANDLE_FILE_INFORMATION *information_out) {
    BY_HANDLE_FILE_INFORMATION information;
    bool valid =
        directory && directory->handle != INVALID_HANDLE_VALUE &&
        GetFileType(directory->handle) == FILE_TYPE_DISK &&
        GetFileInformationByHandle(directory->handle, &information) != 0 &&
        (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
        (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
        private_win_handle_has_local_dos_path(directory->handle, directory->wide_path[0]) &&
        private_win_handle_is_noninheritable(directory->handle) &&
        private_win_owner_only_dacl(&directory->security, directory->handle);
    if (valid && information_out) {
        *information_out = information;
    }
    return valid;
}

static bool private_win_directory_path_matches(cbm_private_lock_directory_t *directory) {
    HANDLE probe =
        CreateFileW(directory->wide_path, FILE_READ_ATTRIBUTES | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL, OPEN_EXISTING,
                    FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    BY_HANDLE_FILE_INFORMATION information;
    bool valid = probe != INVALID_HANDLE_VALUE && GetFileType(probe) == FILE_TYPE_DISK &&
                 GetFileInformationByHandle(probe, &information) != 0 &&
                 (information.dwFileAttributes & FILE_ATTRIBUTE_DIRECTORY) != 0 &&
                 (information.dwFileAttributes & FILE_ATTRIBUTE_REPARSE_POINT) == 0 &&
                 private_win_handle_has_local_dos_path(probe, directory->wide_path[0]) &&
                 private_win_owner_only_dacl(&directory->security, probe);
    if (valid) {
        private_win_identity_t identity = private_win_identity(&information);
        valid = private_win_identity_equal(&identity, &directory->identity);
    }
    if (probe != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(probe);
    }
    return valid;
}

static bool private_win_directory_revalidate(cbm_private_lock_directory_t *directory) {
    BY_HANDLE_FILE_INFORMATION information;
    if (!private_win_path_tree_is_plain_local(directory->wide_path) ||
        !private_win_directory_handle_valid(directory, &information)) {
        return false;
    }
    private_win_identity_t identity = private_win_identity(&information);
    return private_win_identity_equal(&identity, &directory->identity) &&
           private_win_directory_path_matches(directory);
}

static bool private_win_base_name_valid(const char *base_name) {
    if (!base_name || !base_name[0] || strcmp(base_name, ".") == 0 ||
        strcmp(base_name, "..") == 0) {
        return false;
    }
    size_t length = strlen(base_name);
    if (length > 253 || base_name[length - 1] == '.') {
        return false;
    }
    for (size_t index = 0; index < length; index++) {
        char character = base_name[index];
        if (!((character >= 'a' && character <= 'z') || (character >= '0' && character <= '9') ||
              character == '-' || character == '_' || character == '.')) {
            return false;
        }
    }
    size_t stem_length = strcspn(base_name, ".");
    if ((stem_length == 3 &&
         (strncmp(base_name, "con", 3) == 0 || strncmp(base_name, "prn", 3) == 0 ||
          strncmp(base_name, "aux", 3) == 0 || strncmp(base_name, "nul", 3) == 0)) ||
        (stem_length == 4 &&
         ((strncmp(base_name, "com", 3) == 0 || strncmp(base_name, "lpt", 3) == 0) &&
          base_name[3] >= '1' && base_name[3] <= '9'))) {
        /* Win32 resolves these names as devices even below a drive path and
         * even when they carry an extension. */
        return false;
    }
    return true;
}

static wchar_t *private_win_file_path(const cbm_private_lock_directory_t *directory,
                                      const char *base_name) {
    size_t directory_length = wcslen(directory->wide_path);
    size_t base_length = strlen(base_name);
    bool separator = directory_length > 0 && directory->wide_path[directory_length - 1] != L'\\';
    size_t total = directory_length + (separator ? 1 : 0) + base_length + 1;
    if (total > MAX_PATH) {
        return NULL;
    }
    wchar_t *path = malloc(total * sizeof(*path));
    if (!path) {
        return NULL;
    }
    memcpy(path, directory->wide_path, directory_length * sizeof(*path));
    size_t offset = directory_length;
    if (separator) {
        path[offset++] = L'\\';
    }
    for (size_t index = 0; index < base_length; index++) {
        path[offset++] = (unsigned char)base_name[index];
    }
    path[offset] = L'\0';
    return path;
}

static bool private_win_file_handle_valid(cbm_private_lock_directory_t *directory, HANDLE handle,
                                          BY_HANDLE_FILE_INFORMATION *information_out) {
    BY_HANDLE_FILE_INFORMATION information;
    bool valid = handle != INVALID_HANDLE_VALUE && GetFileType(handle) == FILE_TYPE_DISK &&
                 GetFileInformationByHandle(handle, &information) != 0 &&
                 (information.dwFileAttributes &
                  (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
                 information.nNumberOfLinks == 1 &&
                 private_win_handle_has_local_dos_path(handle, directory->wide_path[0]) &&
                 private_win_handle_is_noninheritable(handle) &&
                 private_win_owner_only_dacl(&directory->security, handle);
    if (valid && information_out) {
        *information_out = information;
    }
    return valid;
}

static bool private_win_file_revalidate(cbm_private_lock_directory_t *directory,
                                        const wchar_t *path, HANDLE handle,
                                        const private_win_identity_t *expected) {
    BY_HANDLE_FILE_INFORMATION information;
    if (!private_win_directory_revalidate(directory) ||
        !private_win_file_handle_valid(directory, handle, &information)) {
        return false;
    }
    private_win_identity_t identity = private_win_identity(&information);
    if (expected && !private_win_identity_equal(&identity, expected)) {
        return false;
    }

    HANDLE probe = CreateFileW(path, FILE_READ_ATTRIBUTES | READ_CONTROL,
                               FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_EXISTING,
                               FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    BY_HANDLE_FILE_INFORMATION probe_information;
    bool valid = probe != INVALID_HANDLE_VALUE && GetFileType(probe) == FILE_TYPE_DISK &&
                 GetFileInformationByHandle(probe, &probe_information) != 0 &&
                 (probe_information.dwFileAttributes &
                  (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
                 probe_information.nNumberOfLinks == 1;
    if (valid) {
        private_win_identity_t probe_identity = private_win_identity(&probe_information);
        valid = private_win_identity_equal(&identity, &probe_identity);
    }
    if (probe != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(probe);
    }
    return valid;
}

static cbm_private_file_lock_status_t private_win_open_failure_status(const wchar_t *path) {
    DWORD attributes = GetFileAttributesW(path);
    return attributes == INVALID_FILE_ATTRIBUTES ? CBM_PRIVATE_FILE_LOCK_IO
                                                 : CBM_PRIVATE_FILE_LOCK_UNSAFE;
}

cbm_private_file_lock_status_t cbm_private_lock_directory_adopt_windows(
    void *directory_handle, const char *stable_path, cbm_private_lock_directory_t **directory_out) {
    if (directory_out) {
        *directory_out = NULL;
    }
    HANDLE handle = (HANDLE)directory_handle;
    if (!directory_out || !stable_path || !stable_path[0] || !handle ||
        handle == INVALID_HANDLE_VALUE) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    wchar_t *wide_path = NULL;
    cbm_private_file_lock_status_t path_status =
        private_win_path_from_utf8(stable_path, &wide_path);
    if (path_status != CBM_PRIVATE_FILE_LOCK_OK) {
        return path_status;
    }
    private_win_security_t security;
    if (!private_win_security_init(&security)) {
        free(wide_path);
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    if (!private_win_make_noninheritable(handle)) {
        private_win_security_destroy(&security);
        free(wide_path);
        return CBM_PRIVATE_FILE_LOCK_IO;
    }

    cbm_private_lock_directory_t candidate;
    memset(&candidate, 0, sizeof(candidate));
    candidate.handle = handle;
    candidate.wide_path = wide_path;
    candidate.security = security;
    BY_HANDLE_FILE_INFORMATION information;
    bool safe = private_win_path_tree_is_plain_local(wide_path) &&
                private_win_directory_handle_valid(&candidate, &information);
    if (safe) {
        candidate.identity = private_win_identity(&information);
        safe = private_win_directory_path_matches(&candidate);
    }
    if (!safe) {
        private_win_security_destroy(&candidate.security);
        free(wide_path);
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }

    size_t path_length = strlen(stable_path);
    cbm_private_lock_directory_t *directory = calloc(1, sizeof(*directory));
    char *path_copy = malloc(path_length + 1);
    if (!directory || !path_copy) {
        free(directory);
        free(path_copy);
        private_win_security_destroy(&candidate.security);
        free(wide_path);
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    memcpy(path_copy, stable_path, path_length + 1);
    *directory = candidate;
    directory->path = path_copy;
    *directory_out = directory;
    return CBM_PRIVATE_FILE_LOCK_OK;
}

cbm_private_file_lock_status_t cbm_private_file_lock_try_acquire(
    cbm_private_lock_directory_t *directory, const char *base_name,
    cbm_private_file_lock_mode_t mode, cbm_private_file_lock_t **lock_out) {
    if (lock_out) {
        *lock_out = NULL;
    }
    if (!directory || !lock_out || !private_win_base_name_valid(base_name) ||
        (mode != CBM_PRIVATE_FILE_LOCK_SH && mode != CBM_PRIVATE_FILE_LOCK_EX)) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!private_win_directory_revalidate(directory)) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    wchar_t *path = private_win_file_path(directory, base_name);
    if (!path) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    if (!cbm_private_file_lock_fork_guard_enter()) {
        free(path);
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    HANDLE handle =
        CreateFileW(path, GENERIC_READ | GENERIC_WRITE | READ_CONTROL,
                    FILE_SHARE_READ | FILE_SHARE_WRITE, &directory->security.attributes,
                    OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    if (handle == INVALID_HANDLE_VALUE) {
        cbm_private_file_lock_status_t status = private_win_open_failure_status(path);
        cbm_private_file_lock_fork_guard_leave();
        free(path);
        return status;
    }
    if (!private_win_make_noninheritable(handle)) {
        (void)CloseHandle(handle);
        cbm_private_file_lock_fork_guard_leave();
        free(path);
        return CBM_PRIVATE_FILE_LOCK_IO;
    }

    BY_HANDLE_FILE_INFORMATION information;
    bool initially_valid = private_win_file_handle_valid(directory, handle, &information);
    private_win_identity_t identity;
    if (initially_valid) {
        identity = private_win_identity(&information);
        initially_valid = private_win_file_revalidate(directory, path, handle, &identity);
    }
    if (!initially_valid) {
        (void)CloseHandle(handle);
        cbm_private_file_lock_fork_guard_leave();
        free(path);
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }

    cbm_private_file_lock_t *lock = calloc(1, sizeof(*lock));
    if (!lock) {
        (void)CloseHandle(handle);
        cbm_private_file_lock_fork_guard_leave();
        free(path);
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    lock->handle = handle;
    lock->mode = mode;

    DWORD flags = LOCKFILE_FAIL_IMMEDIATELY;
    if (mode == CBM_PRIVATE_FILE_LOCK_EX) {
        flags |= LOCKFILE_EXCLUSIVE_LOCK;
    }
    bool forced_lock_failure = directory->test_fail_lock_attempt_once;
    bool fail_lock_cleanup_close = directory->test_fail_lock_attempt_close;
    directory->test_fail_lock_attempt_once = false;
    directory->test_fail_lock_attempt_close = false;
    BOOL native_locked =
        forced_lock_failure ? FALSE : LockFileEx(handle, flags, 0, 1, 0, &lock->range);
    if (!native_locked) {
        DWORD lock_error = forced_lock_failure ? ERROR_LOCK_VIOLATION : GetLastError();
        lock->unlocked = true;
        lock->test_fail_close_once = fail_lock_cleanup_close;
        *lock_out = lock;
        cbm_private_file_lock_fork_guard_leave();
        free(path);
        cbm_private_file_lock_status_t failure_status = lock_error == ERROR_LOCK_VIOLATION
                                                            ? CBM_PRIVATE_FILE_LOCK_BUSY
                                                            : CBM_PRIVATE_FILE_LOCK_IO;
        cbm_private_file_lock_status_t cleanup_status = cbm_private_file_lock_release(lock_out);
        return cleanup_status == CBM_PRIVATE_FILE_LOCK_OK ? failure_status
                                                          : CBM_PRIVATE_FILE_LOCK_IO;
    }
    bool forced_cleanup = directory->test_fail_post_acquire_once;
    bool fail_cleanup_unlock = directory->test_fail_post_acquire_unlock;
    bool fail_cleanup_close = directory->test_fail_post_acquire_close;
    directory->test_fail_post_acquire_once = false;
    directory->test_fail_post_acquire_unlock = false;
    directory->test_fail_post_acquire_close = false;
    if (forced_cleanup || !private_win_file_revalidate(directory, path, handle, &identity)) {
        cbm_private_file_lock_status_t failure_status =
            forced_cleanup ? CBM_PRIVATE_FILE_LOCK_IO : CBM_PRIVATE_FILE_LOCK_UNSAFE;
        lock->test_fail_unlock_once = fail_cleanup_unlock;
        lock->test_fail_close_once = fail_cleanup_close;
        *lock_out = lock;
        cbm_private_file_lock_fork_guard_leave();
        free(path);
        cbm_private_file_lock_status_t cleanup_status = cbm_private_file_lock_release(lock_out);
        return cleanup_status == CBM_PRIVATE_FILE_LOCK_OK ? failure_status
                                                          : CBM_PRIVATE_FILE_LOCK_IO;
    }
    free(path);
    *lock_out = lock;
    cbm_private_file_lock_fork_guard_leave();
    return CBM_PRIVATE_FILE_LOCK_OK;
}

static bool private_win_payload_handle_valid(const cbm_private_file_lock_t *lock) {
    BY_HANDLE_FILE_INFORMATION information;
    return lock && lock->handle != INVALID_HANDLE_VALUE && !lock->unlocked &&
           GetFileType(lock->handle) == FILE_TYPE_DISK &&
           GetFileInformationByHandle(lock->handle, &information) != 0 &&
           (information.dwFileAttributes &
            (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
           information.nNumberOfLinks == 1 && private_win_handle_is_noninheritable(lock->handle);
}

cbm_private_file_lock_status_t cbm_private_file_lock_payload_read(cbm_private_file_lock_t *lock,
                                                                  void *buffer, size_t capacity,
                                                                  size_t *length_out) {
    if (length_out) {
        *length_out = 0;
    }
    if (!lock || !buffer || capacity == 0 || !length_out) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!cbm_private_file_lock_fork_guard_enter()) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    LARGE_INTEGER size;
    LARGE_INTEGER zero;
    zero.QuadPart = 0;
    bool valid = private_win_payload_handle_valid(lock) &&
                 GetFileSizeEx(lock->handle, &size) != 0 && size.QuadPart >= 0 &&
                 (uint64_t)size.QuadPart <= PRIVATE_FILE_LOCK_PAYLOAD_CAP &&
                 (uint64_t)size.QuadPart <= capacity &&
                 SetFilePointerEx(lock->handle, zero, NULL, FILE_BEGIN) != 0;
    size_t length = valid ? (size_t)size.QuadPart : 0;
    size_t offset = 0;
    while (valid && offset < length) {
        DWORD chunk = (DWORD)(length - offset);
        DWORD count = 0;
        valid =
            ReadFile(lock->handle, (unsigned char *)buffer + offset, chunk, &count, NULL) != 0 &&
            count > 0;
        offset += valid ? (size_t)count : 0;
    }
    cbm_private_file_lock_fork_guard_leave();
    if (!valid) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    *length_out = length;
    return CBM_PRIVATE_FILE_LOCK_OK;
}

cbm_private_file_lock_status_t cbm_private_file_lock_payload_write(cbm_private_file_lock_t *lock,
                                                                   const void *buffer,
                                                                   size_t length) {
    if (!lock || !buffer || length == 0 || length > PRIVATE_FILE_LOCK_PAYLOAD_CAP ||
        lock->mode != CBM_PRIVATE_FILE_LOCK_EX) {
        return CBM_PRIVATE_FILE_LOCK_UNSAFE;
    }
    if (!cbm_private_file_lock_fork_guard_enter()) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    LARGE_INTEGER zero;
    zero.QuadPart = 0;
    bool valid = private_win_payload_handle_valid(lock) &&
                 SetFilePointerEx(lock->handle, zero, NULL, FILE_BEGIN) != 0 &&
                 SetEndOfFile(lock->handle) != 0;
    size_t offset = 0;
    while (valid && offset < length) {
        DWORD chunk = (DWORD)(length - offset);
        DWORD count = 0;
        valid = WriteFile(lock->handle, (const unsigned char *)buffer + offset, chunk, &count,
                          NULL) != 0 &&
                count > 0;
        offset += valid ? (size_t)count : 0;
    }
    valid = valid && FlushFileBuffers(lock->handle) != 0;
    cbm_private_file_lock_fork_guard_leave();
    return valid ? CBM_PRIVATE_FILE_LOCK_OK : CBM_PRIVATE_FILE_LOCK_IO;
}

static bool private_win_release_unlock(cbm_private_file_lock_t *lock) {
    lock->test_unlock_attempts++;
    if (lock->test_fail_unlock_once) {
        lock->test_fail_unlock_once = false;
        SetLastError(ERROR_GEN_FAILURE);
        return false;
    }
    return UnlockFileEx(lock->handle, 0, 1, 0, &lock->range) != 0;
}

static bool private_win_release_close(cbm_private_file_lock_t *lock) {
    lock->test_close_attempts++;
    if (lock->test_fail_close_once) {
        lock->test_fail_close_once = false;
        SetLastError(ERROR_GEN_FAILURE);
        return false;
    }
    return CloseHandle(lock->handle) != 0;
}

cbm_private_file_lock_status_t cbm_private_file_lock_release(cbm_private_file_lock_t **lock_io) {
    if (!lock_io || !*lock_io) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    cbm_private_file_lock_t *lock = *lock_io;
    if (!cbm_private_file_lock_fork_guard_enter()) {
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    if (lock->handle == INVALID_HANDLE_VALUE) {
        cbm_private_file_lock_fork_guard_leave();
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    if (!lock->unlocked) {
        if (!private_win_release_unlock(lock)) {
            cbm_private_file_lock_fork_guard_leave();
            return CBM_PRIVATE_FILE_LOCK_IO;
        }
        lock->unlocked = true;
    }
    if (!private_win_release_close(lock)) {
        cbm_private_file_lock_fork_guard_leave();
        return CBM_PRIVATE_FILE_LOCK_IO;
    }
    lock->handle = INVALID_HANDLE_VALUE;
    cbm_private_file_lock_fork_guard_leave();
    *lock_io = NULL;
    free(lock);
    return CBM_PRIVATE_FILE_LOCK_OK;
}

void cbm_private_lock_directory_close(cbm_private_lock_directory_t *directory) {
    if (!directory) {
        return;
    }
    if (directory->handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(directory->handle);
    }
    private_win_security_destroy(&directory->security);
    free(directory->wide_path);
    free(directory->path);
    free(directory);
}

const char *cbm_private_lock_directory_path(const cbm_private_lock_directory_t *directory) {
    return directory ? directory->path : NULL;
}

bool cbm_private_file_lock_is_cloexec_for_test(const cbm_private_file_lock_t *lock) {
    return lock && private_win_handle_is_noninheritable(lock->handle);
}

bool cbm_private_file_lock_unlock_complete(const cbm_private_file_lock_t *lock) {
    return lock && lock->unlocked;
}

bool cbm_private_lock_directory_fail_post_acquire_cleanup_for_test(
    cbm_private_lock_directory_t *directory, bool fail_unlock, bool fail_close) {
    if (!directory || (!fail_unlock && !fail_close)) {
        return false;
    }
    directory->test_fail_post_acquire_once = true;
    directory->test_fail_post_acquire_unlock = fail_unlock;
    directory->test_fail_post_acquire_close = fail_close;
    return true;
}

bool cbm_private_lock_directory_fail_lock_attempt_cleanup_for_test(
    cbm_private_lock_directory_t *directory) {
    if (!directory) {
        return false;
    }
    directory->test_fail_lock_attempt_once = true;
    directory->test_fail_lock_attempt_close = true;
    return true;
}

bool cbm_private_file_lock_fail_next_release_step_for_test(
    cbm_private_file_lock_t *lock, cbm_private_file_lock_release_step_t step) {
    if (!lock) {
        return false;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK) {
        lock->test_fail_unlock_once = true;
        return true;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE) {
        lock->test_fail_close_once = true;
        return true;
    }
    return false;
}

unsigned int cbm_private_file_lock_release_step_attempts_for_test(
    const cbm_private_file_lock_t *lock, cbm_private_file_lock_release_step_t step) {
    if (!lock) {
        return 0;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK) {
        return lock->test_unlock_attempts;
    }
    if (step == CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE) {
        return lock->test_close_attempts;
    }
    return 0;
}

bool cbm_private_file_lock_fork_guard_enter(void) {
    if (!InitOnceExecuteOnce(&private_win_gate_once, private_win_gate_initialize, NULL, NULL)) {
        return false;
    }
    EnterCriticalSection(&private_win_gate);
    return true;
}

void cbm_private_file_lock_fork_guard_leave(void) {
    LeaveCriticalSection(&private_win_gate);
}

cbm_private_fork_condition_t *cbm_private_fork_condition_new(void) {
    cbm_private_fork_condition_t *condition = calloc(1, sizeof(*condition));
    if (condition) {
        InitializeConditionVariable(&condition->value);
    }
    return condition;
}

void cbm_private_fork_condition_free(cbm_private_fork_condition_t *condition) {
    free(condition);
}

void cbm_private_fork_condition_broadcast_while_guarded(cbm_private_fork_condition_t *condition) {
    if (condition) {
        WakeAllConditionVariable(&condition->value);
    }
}

cbm_private_fork_wait_status_t cbm_private_fork_condition_wait_until_while_guarded(
    cbm_private_fork_condition_t *condition, uint64_t deadline_ms) {
    if (!condition) {
        return CBM_PRIVATE_FORK_WAIT_ERROR;
    }
    DWORD timeout = INFINITE;
    if (deadline_ms != UINT64_MAX) {
        uint64_t now_ms = cbm_now_ms();
        uint64_t remaining_ms = deadline_ms > now_ms ? deadline_ms - now_ms : 0;
        timeout = remaining_ms >= (uint64_t)INFINITE ? INFINITE - 1U : (DWORD)remaining_ms;
    }
    if (SleepConditionVariableCS(&condition->value, &private_win_gate, timeout)) {
        return CBM_PRIVATE_FORK_WAIT_SIGNALED;
    }
    return GetLastError() == ERROR_TIMEOUT ? CBM_PRIVATE_FORK_WAIT_TIMEOUT
                                           : CBM_PRIVATE_FORK_WAIT_ERROR;
}

#endif
