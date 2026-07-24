/* RED contract for handle-anchored owner-only SH/EX lock files. */
#include "test_framework.h"

#include "foundation/compat.h"
#include "foundation/private_file_lock.h"
#include "foundation/private_file_lock_internal.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include "daemon/ipc.h"
#include "foundation/win_utf8.h"

#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#else
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#ifdef __APPLE__
#include <membership.h>
#include <sys/acl.h>
#endif
#endif

enum { PRIVATE_LOCK_TEST_PATH_CAP = 1024 };

#ifdef __APPLE__
/* Return 1 when installed, 0 only when the filesystem has no extended ACL
 * support, and -1 for every other fixture failure. */
static int private_lock_macos_set_mutating_acl(const char *path) {
    errno = 0;
    acl_t acl = acl_init(1);
    acl_entry_t entry = NULL;
    acl_permset_t permissions = NULL;
    gid_t foreign_group = getegid() == (gid_t)0 ? (gid_t)1 : (gid_t)0;
    uuid_t foreign_group_uuid = {0};
    bool valid = acl && acl_create_entry(&acl, &entry) == 0 && entry &&
                 acl_set_tag_type(entry, ACL_EXTENDED_ALLOW) == 0 &&
                 mbr_gid_to_uuid(foreign_group, foreign_group_uuid) == 0 &&
                 acl_set_qualifier(entry, foreign_group_uuid) == 0 &&
                 acl_get_permset(entry, &permissions) == 0 && permissions &&
                 acl_clear_perms(permissions) == 0 &&
                 acl_add_perm(permissions, ACL_WRITE_DATA) == 0 &&
                 acl_add_perm(permissions, ACL_WRITE_ATTRIBUTES) == 0 &&
                 acl_add_perm(permissions, ACL_DELETE) == 0 &&
                 acl_set_permset(entry, permissions) == 0 && acl_valid(acl) == 0;
    errno = valid ? 0 : errno;
    int result = valid ? acl_set_file(path, ACL_TYPE_EXTENDED, acl) : -1;
    int saved_error = errno;
    if (acl && acl_free(acl) != 0 && result == 0) {
        result = -1;
        saved_error = errno;
    }
    if (result == 0) {
        return 1;
    }
    return saved_error == ENOTSUP || saved_error == EOPNOTSUPP ? 0 : -1;
}
#endif

typedef struct {
    char parent[PRIVATE_LOCK_TEST_PATH_CAP];
    char root[PRIVATE_LOCK_TEST_PATH_CAP];
    cbm_private_lock_directory_t *directory;
} private_lock_fixture_t;

static bool private_lock_fixture_start(private_lock_fixture_t *fixture) {
    memset(fixture, 0, sizeof(*fixture));
#ifdef _WIN32
    int written = snprintf(fixture->parent, sizeof(fixture->parent), "%s/cbm-private-lock-XXXXXX",
                           cbm_tmpdir());
    if (written <= 0 || written >= (int)sizeof(fixture->parent) || !cbm_mkdtemp(fixture->parent)) {
        return false;
    }
    written = snprintf(fixture->root, sizeof(fixture->root), "%s/root", fixture->parent);
    if (written <= 0 || written >= (int)sizeof(fixture->root) || cbm_mkdir(fixture->root) != 0) {
        return false;
    }
    FILE *seed = cbm_daemon_ipc_private_log_open(fixture->root, "seed", 64);
    bool seeded = seed && fclose(seed) == 0;
    char seed_path[PRIVATE_LOCK_TEST_PATH_CAP];
    written = snprintf(seed_path, sizeof(seed_path), "%s/seed", fixture->root);
    wchar_t *wide_seed =
        written > 0 && written < (int)sizeof(seed_path) ? cbm_utf8_to_wide(seed_path) : NULL;
    if (wide_seed) {
        (void)DeleteFileW(wide_seed);
    }
    free(wide_seed);
    wchar_t *wide_root = seeded ? cbm_utf8_to_wide(fixture->root) : NULL;
    HANDLE handle =
        wide_root ? CreateFileW(wide_root, FILE_READ_ATTRIBUTES | READ_CONTROL,
                                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                                OPEN_EXISTING,
                                FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT, NULL)
                  : INVALID_HANDLE_VALUE;
    free(wide_root);
    cbm_private_file_lock_status_t status =
        handle != INVALID_HANDLE_VALUE
            ? cbm_private_lock_directory_adopt_windows(handle, fixture->root, &fixture->directory)
            : CBM_PRIVATE_FILE_LOCK_IO;
    if (status != CBM_PRIVATE_FILE_LOCK_OK && handle != INVALID_HANDLE_VALUE) {
        (void)CloseHandle(handle);
    }
    return status == CBM_PRIVATE_FILE_LOCK_OK;
#else
    int written = snprintf(fixture->parent, sizeof(fixture->parent), "%s/cbm-private-lock-XXXXXX",
                           cbm_tmpdir());
    if (written <= 0 || written >= (int)sizeof(fixture->parent) || !cbm_mkdtemp(fixture->parent)) {
        return false;
    }
    written = snprintf(fixture->root, sizeof(fixture->root), "%s/root", fixture->parent);
    if (written <= 0 || written >= (int)sizeof(fixture->root) || mkdir(fixture->root, 0700) != 0) {
        return false;
    }
    int fd = open(fixture->root, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    cbm_private_file_lock_status_t status =
        cbm_private_lock_directory_adopt_posix(fd, fixture->root, &fixture->directory);
    if (status != CBM_PRIVATE_FILE_LOCK_OK) {
        if (fd >= 0) {
            (void)close(fd);
        }
        return false;
    }
    return true;
#endif
}

static bool private_lock_path(char out[PRIVATE_LOCK_TEST_PATH_CAP],
                              const private_lock_fixture_t *fixture, const char *base_name) {
    int written = snprintf(out, PRIVATE_LOCK_TEST_PATH_CAP, "%s/%s", fixture->root, base_name);
    return written > 0 && written < PRIVATE_LOCK_TEST_PATH_CAP;
}

#ifdef _WIN32
static void private_lock_windows_remove(const char *path, bool directory) {
    wchar_t *wide = cbm_utf8_to_wide(path);
    if (wide) {
        if (directory) {
            (void)RemoveDirectoryW(wide);
        } else {
            (void)DeleteFileW(wide);
        }
    }
    free(wide);
}
#endif

static void private_lock_fixture_finish(private_lock_fixture_t *fixture) {
    cbm_private_lock_directory_close(fixture->directory);
    char path[PRIVATE_LOCK_TEST_PATH_CAP];
    static const char *const files[] = {"matrix.lock",
                                        "payload.lock",
                                        "release-unlock.lock",
                                        "release-close.lock",
                                        "consumed-close.lock",
                                        "post-acquire-cleanup.lock",
                                        "lock-attempt-cleanup.lock",
                                        "acl-directory.lock",
                                        "acl-file.lock",
                                        "fork.lock"};
    if (fixture->root[0]) {
        for (size_t index = 0; index < sizeof(files) / sizeof(files[0]); index++) {
            if (!private_lock_path(path, fixture, files[index])) {
                continue;
            }
#ifdef _WIN32
            private_lock_windows_remove(path, false);
#else
            (void)unlink(path);
#endif
        }
    }
#ifdef _WIN32
    if (fixture->root[0]) {
        private_lock_windows_remove(fixture->root, true);
    }
    if (fixture->parent[0]) {
        private_lock_windows_remove(fixture->parent, true);
    }
#else
    if (fixture->root[0]) {
        (void)rmdir(fixture->root);
    }
    if (fixture->parent[0]) {
        (void)rmdir(fixture->parent);
    }
#endif
    memset(fixture, 0, sizeof(*fixture));
}

TEST(private_file_lock_payload_requires_exclusive_write_and_survives_reopen) {
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    static const unsigned char payload[] = {
        'C', 'B', 'M', 0, 1, 2, 3, 0xff,
    };
    unsigned char readback[sizeof(payload)] = {0};
    size_t readback_length = 0;
    cbm_private_file_lock_t *writer = NULL;
    cbm_private_file_lock_t *reader = NULL;
    cbm_private_file_lock_status_t writer_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t write_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t reader_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t read_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t shared_write_status = CBM_PRIVATE_FILE_LOCK_IO;
    if (started) {
        writer_status = cbm_private_file_lock_try_acquire(fixture.directory, "payload.lock",
                                                          CBM_PRIVATE_FILE_LOCK_EX, &writer);
    }
    if (writer_status == CBM_PRIVATE_FILE_LOCK_OK) {
        write_status = cbm_private_file_lock_payload_write(writer, payload, sizeof(payload));
        (void)cbm_private_file_lock_release(&writer);
        reader_status = cbm_private_file_lock_try_acquire(fixture.directory, "payload.lock",
                                                          CBM_PRIVATE_FILE_LOCK_SH, &reader);
    }
    if (reader_status == CBM_PRIVATE_FILE_LOCK_OK) {
        read_status = cbm_private_file_lock_payload_read(reader, readback, sizeof(readback),
                                                         &readback_length);
        shared_write_status = cbm_private_file_lock_payload_write(reader, payload, sizeof(payload));
    }
    if (reader) {
        (void)cbm_private_file_lock_release(&reader);
    }
    if (writer) {
        (void)cbm_private_file_lock_release(&writer);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(writer_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(write_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(reader_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(read_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(readback_length, sizeof(payload));
    ASSERT_TRUE(memcmp(readback, payload, sizeof(payload)) == 0);
    ASSERT_EQ(shared_write_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    PASS();
}

#ifndef _WIN32
static bool private_lock_fd_write_byte(int fd, char value) {
    ssize_t written;
    do {
        written = write(fd, &value, 1);
    } while (written < 0 && errno == EINTR);
    return written == 1;
}

static bool private_lock_fd_read_byte(int fd, char *value_out) {
    ssize_t received;
    do {
        received = read(fd, value_out, 1);
    } while (received < 0 && errno == EINTR);
    return received == 1;
}
#endif

TEST(private_file_lock_shared_and_exclusive_matrix) {
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    cbm_private_file_lock_t *first_reader = NULL;
    cbm_private_file_lock_t *second_reader = NULL;
    cbm_private_file_lock_t *writer = NULL;
    cbm_private_file_lock_status_t first_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t second_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t blocked_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t one_reader_blocked_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t writer_status = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t reader_blocked_status = CBM_PRIVATE_FILE_LOCK_IO;
    bool stable_file = false;
#ifdef _WIN32
    bool noninheritable = false;
    cbm_private_file_lock_status_t device_name_status = CBM_PRIVATE_FILE_LOCK_IO;
#endif
    if (started) {
        first_status = cbm_private_file_lock_try_acquire(fixture.directory, "matrix.lock",
                                                         CBM_PRIVATE_FILE_LOCK_SH, &first_reader);
#ifdef _WIN32
        noninheritable = cbm_private_file_lock_is_cloexec_for_test(first_reader);
        cbm_private_file_lock_t *device_lock = NULL;
        device_name_status = cbm_private_file_lock_try_acquire(
            fixture.directory, "nul.lock", CBM_PRIVATE_FILE_LOCK_EX, &device_lock);
        if (device_lock) {
            (void)cbm_private_file_lock_release(&device_lock);
        }
#endif
        second_status = cbm_private_file_lock_try_acquire(fixture.directory, "matrix.lock",
                                                          CBM_PRIVATE_FILE_LOCK_SH, &second_reader);
        blocked_status = cbm_private_file_lock_try_acquire(fixture.directory, "matrix.lock",
                                                           CBM_PRIVATE_FILE_LOCK_EX, &writer);
    }
    if (second_reader) {
        (void)cbm_private_file_lock_release(&second_reader);
    }
    if (started) {
        one_reader_blocked_status = cbm_private_file_lock_try_acquire(
            fixture.directory, "matrix.lock", CBM_PRIVATE_FILE_LOCK_EX, &writer);
    }
    if (first_reader) {
        (void)cbm_private_file_lock_release(&first_reader);
    }
    if (started) {
        writer_status = cbm_private_file_lock_try_acquire(fixture.directory, "matrix.lock",
                                                          CBM_PRIVATE_FILE_LOCK_EX, &writer);
        reader_blocked_status = cbm_private_file_lock_try_acquire(
            fixture.directory, "matrix.lock", CBM_PRIVATE_FILE_LOCK_SH, &first_reader);
    }
    if (first_reader) {
        (void)cbm_private_file_lock_release(&first_reader);
    }
    if (writer) {
        (void)cbm_private_file_lock_release(&writer);
    }
    char matrix_path[PRIVATE_LOCK_TEST_PATH_CAP];
#ifdef _WIN32
    bool matrix_path_ok = private_lock_path(matrix_path, &fixture, "matrix.lock");
    wchar_t *wide_matrix = matrix_path_ok ? cbm_utf8_to_wide(matrix_path) : NULL;
    DWORD matrix_attributes =
        wide_matrix ? GetFileAttributesW(wide_matrix) : INVALID_FILE_ATTRIBUTES;
    stable_file =
        matrix_attributes != INVALID_FILE_ATTRIBUTES &&
        (matrix_attributes & (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0;
    free(wide_matrix);
#else
    struct stat matrix_status;
    stable_file = private_lock_path(matrix_path, &fixture, "matrix.lock") &&
                  lstat(matrix_path, &matrix_status) == 0 && S_ISREG(matrix_status.st_mode);
#endif
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(first_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(second_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(blocked_status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(one_reader_blocked_status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(writer_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(reader_blocked_status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_TRUE(stable_file);
#ifdef _WIN32
    ASSERT_TRUE(noninheritable);
    ASSERT_EQ(device_name_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
#endif
    PASS();
}

TEST(private_file_lock_unlock_failure_retains_retryable_lock) {
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    cbm_private_file_lock_t *lock = NULL;
    cbm_private_file_lock_status_t acquired =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "release-unlock.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &lock)
                : CBM_PRIVATE_FILE_LOCK_IO;
    bool fault_set = cbm_private_file_lock_fail_next_release_step_for_test(
        lock, CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK);
    cbm_private_file_lock_status_t first_release =
        fault_set ? cbm_private_file_lock_release(&lock) : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained = lock != NULL;

    cbm_private_file_lock_t *contender = NULL;
    cbm_private_file_lock_status_t while_failed =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "release-unlock.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &contender)
                : CBM_PRIVATE_FILE_LOCK_IO;
    if (contender) {
        (void)cbm_private_file_lock_release(&contender);
    }
    cbm_private_file_lock_status_t retry =
        lock ? cbm_private_file_lock_release(&lock) : CBM_PRIVATE_FILE_LOCK_IO;
    if (lock) {
        (void)cbm_private_file_lock_release(&lock);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(fault_set);
    ASSERT_EQ(first_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained);
    ASSERT_EQ(while_failed, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(retry, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(lock);
    PASS();
}

TEST(private_file_lock_close_failure_retries_without_duplicate_unlock) {
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    cbm_private_file_lock_t *lock = NULL;
    cbm_private_file_lock_status_t acquired =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "release-close.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &lock)
                : CBM_PRIVATE_FILE_LOCK_IO;
    bool close_fault_set = cbm_private_file_lock_fail_next_release_step_for_test(
        lock, CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE);
    cbm_private_file_lock_status_t first_release =
        close_fault_set ? cbm_private_file_lock_release(&lock) : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained = lock != NULL;
    unsigned int unlock_attempts = cbm_private_file_lock_release_step_attempts_for_test(
        lock, CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK);
    unsigned int close_attempts = cbm_private_file_lock_release_step_attempts_for_test(
        lock, CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE);

    cbm_private_file_lock_t *contender = NULL;
    cbm_private_file_lock_status_t after_unlock =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "release-close.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &contender)
                : CBM_PRIVATE_FILE_LOCK_IO;
    if (contender) {
        (void)cbm_private_file_lock_release(&contender);
    }
    bool duplicate_unlock_fault_set = cbm_private_file_lock_fail_next_release_step_for_test(
        lock, CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK);
    cbm_private_file_lock_status_t retry =
        lock ? cbm_private_file_lock_release(&lock) : CBM_PRIVATE_FILE_LOCK_IO;
    if (lock) {
        (void)cbm_private_file_lock_release(&lock);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(close_fault_set);
    ASSERT_EQ(first_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained);
    ASSERT_EQ(unlock_attempts, 1);
    ASSERT_EQ(close_attempts, 1);
    ASSERT_EQ(after_unlock, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(duplicate_unlock_fault_set);
    ASSERT_EQ(retry, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(lock);
    PASS();
}

TEST(private_file_lock_consumed_close_error_never_retries_recycled_fd) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX close descriptor-consumption semantics run on POSIX");
#else
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    cbm_private_file_lock_t *lock = NULL;
    cbm_private_file_lock_status_t acquired =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "consumed-close.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &lock)
                : CBM_PRIVATE_FILE_LOCK_IO;
    int consumed_fd = cbm_private_file_lock_native_fd_for_test(lock);
    bool fault_set = cbm_private_file_lock_fail_close_after_consuming_for_test(lock);
    cbm_private_file_lock_status_t first_release =
        fault_set ? cbm_private_file_lock_release(&lock) : CBM_PRIVATE_FILE_LOCK_IO;
    bool terminally_cleared = lock == NULL;

    int replacement_fd = open("/dev/null", O_RDONLY | O_CLOEXEC);
    bool descriptor_reused = replacement_fd >= 0 && replacement_fd == consumed_fd;
    if (lock) {
        (void)cbm_private_file_lock_release(&lock);
    }
    bool replacement_alive = replacement_fd >= 0 && fcntl(replacement_fd, F_GETFD) >= 0;
    if (replacement_alive) {
        (void)close(replacement_fd);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(fault_set);
    ASSERT_EQ(first_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(terminally_cleared);
    ASSERT_TRUE(descriptor_reused);
    ASSERT_TRUE(replacement_alive);
    PASS();
#endif
}

TEST(private_file_lock_post_acquire_failure_returns_cleanup_owner) {
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    bool fault_set = started && cbm_private_lock_directory_fail_post_acquire_cleanup_for_test(
                                    fixture.directory, true, true);
    cbm_private_file_lock_t *cleanup = NULL;
    cbm_private_file_lock_status_t acquire_status =
        fault_set
            ? cbm_private_file_lock_try_acquire(fixture.directory, "post-acquire-cleanup.lock",
                                                CBM_PRIVATE_FILE_LOCK_EX, &cleanup)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool cleanup_retained = cleanup != NULL;

    cbm_private_file_lock_t *contender = NULL;
    cbm_private_file_lock_status_t while_unlock_pending =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "post-acquire-cleanup.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &contender)
                : CBM_PRIVATE_FILE_LOCK_IO;
    if (contender) {
        (void)cbm_private_file_lock_release(&contender);
    }
    cbm_private_file_lock_status_t first_cleanup =
        cleanup ? cbm_private_file_lock_release(&cleanup) : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained_close_pending = cleanup != NULL;
    cbm_private_file_lock_status_t second_cleanup =
        cleanup ? cbm_private_file_lock_release(&cleanup) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_t *after = NULL;
    cbm_private_file_lock_status_t after_status =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "post-acquire-cleanup.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &after)
                : CBM_PRIVATE_FILE_LOCK_IO;
    if (after) {
        (void)cbm_private_file_lock_release(&after);
    }
    if (cleanup) {
        (void)cbm_private_file_lock_release(&cleanup);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(fault_set);
    ASSERT_EQ(acquire_status, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(cleanup_retained);
    ASSERT_EQ(while_unlock_pending, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(first_cleanup, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained_close_pending);
    ASSERT_EQ(second_cleanup, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(cleanup);
    ASSERT_EQ(after_status, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
}

TEST(private_file_lock_windows_lock_attempt_failure_returns_cleanup_owner) {
#ifndef _WIN32
    SKIP_PLATFORM("LockFileEx cleanup ownership runs on Windows");
#else
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    bool fault_set =
        started && cbm_private_lock_directory_fail_lock_attempt_cleanup_for_test(fixture.directory);
    cbm_private_file_lock_t *cleanup = NULL;
    cbm_private_file_lock_status_t acquire_status =
        fault_set
            ? cbm_private_file_lock_try_acquire(fixture.directory, "lock-attempt-cleanup.lock",
                                                CBM_PRIVATE_FILE_LOCK_EX, &cleanup)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained = cleanup != NULL;
    cbm_private_file_lock_status_t cleanup_release =
        cleanup ? cbm_private_file_lock_release(&cleanup) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_t *after = NULL;
    cbm_private_file_lock_status_t after_status =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "lock-attempt-cleanup.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &after)
                : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t after_release =
        after ? cbm_private_file_lock_release(&after) : CBM_PRIVATE_FILE_LOCK_IO;
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(fault_set);
    ASSERT_EQ(acquire_status, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained);
    ASSERT_EQ(cleanup_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(cleanup);
    ASSERT_EQ(after_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(after_release, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(private_file_lock_rejects_unsafe_entries_and_replaced_root) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX unsafe-entry fixtures run on POSIX");
#else
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    char target[PRIVATE_LOCK_TEST_PATH_CAP];
    char symlink_path[PRIVATE_LOCK_TEST_PATH_CAP];
    char hardlink_path[PRIVATE_LOCK_TEST_PATH_CAP];
    char mode_path[PRIVATE_LOCK_TEST_PATH_CAP];
    char special_mode_path[PRIVATE_LOCK_TEST_PATH_CAP];
    char fifo_path[PRIVATE_LOCK_TEST_PATH_CAP];
    bool paths_ok = started && private_lock_path(target, &fixture, "target") &&
                    private_lock_path(symlink_path, &fixture, "symlink.lock") &&
                    private_lock_path(hardlink_path, &fixture, "hardlink.lock") &&
                    private_lock_path(mode_path, &fixture, "mode.lock") &&
                    private_lock_path(special_mode_path, &fixture, "special-mode.lock") &&
                    private_lock_path(fifo_path, &fixture, "fifo.lock");
    int target_fd = paths_ok ? open(target, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600) : -1;
    bool target_ok = target_fd >= 0 && close(target_fd) == 0;
    bool fixtures_ok =
        target_ok && symlink("target", symlink_path) == 0 && link(target, hardlink_path) == 0;
    int mode_fd = fixtures_ok ? open(mode_path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600) : -1;
    fixtures_ok = fixtures_ok && mode_fd >= 0 && fchmod(mode_fd, 0644) == 0 && close(mode_fd) == 0;
    int special_mode_fd =
        fixtures_ok ? open(special_mode_path, O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600) : -1;
    fixtures_ok = fixtures_ok && special_mode_fd >= 0 && fchmod(special_mode_fd, 01600) == 0 &&
                  close(special_mode_fd) == 0 && mkfifo(fifo_path, 0600) == 0;

    cbm_private_file_lock_t *lock = NULL;
    cbm_private_file_lock_status_t symlink_status =
        fixtures_ok ? cbm_private_file_lock_try_acquire(fixture.directory, "symlink.lock",
                                                        CBM_PRIVATE_FILE_LOCK_EX, &lock)
                    : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t hardlink_status =
        fixtures_ok ? cbm_private_file_lock_try_acquire(fixture.directory, "hardlink.lock",
                                                        CBM_PRIVATE_FILE_LOCK_EX, &lock)
                    : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t mode_status =
        fixtures_ok ? cbm_private_file_lock_try_acquire(fixture.directory, "mode.lock",
                                                        CBM_PRIVATE_FILE_LOCK_EX, &lock)
                    : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t special_mode_status =
        fixtures_ok ? cbm_private_file_lock_try_acquire(fixture.directory, "special-mode.lock",
                                                        CBM_PRIVATE_FILE_LOCK_EX, &lock)
                    : CBM_PRIVATE_FILE_LOCK_IO;
    if (lock) {
        (void)cbm_private_file_lock_release(&lock);
    }
    cbm_private_file_lock_status_t fifo_status =
        fixtures_ok ? cbm_private_file_lock_try_acquire(fixture.directory, "fifo.lock",
                                                        CBM_PRIVATE_FILE_LOCK_EX, &lock)
                    : CBM_PRIVATE_FILE_LOCK_IO;

    bool special_root_set = fixtures_ok && chmod(fixture.root, 01700) == 0;
    cbm_private_file_lock_status_t special_root_status =
        special_root_set ? cbm_private_file_lock_try_acquire(fixture.directory, "special-root.lock",
                                                             CBM_PRIVATE_FILE_LOCK_EX, &lock)
                         : CBM_PRIVATE_FILE_LOCK_IO;
    if (lock) {
        (void)cbm_private_file_lock_release(&lock);
    }
    bool special_root_restored = special_root_set && chmod(fixture.root, 0700) == 0;

    (void)unlink(symlink_path);
    (void)unlink(hardlink_path);
    (void)unlink(mode_path);
    (void)unlink(special_mode_path);
    (void)unlink(fifo_path);
    char special_root_path[PRIVATE_LOCK_TEST_PATH_CAP];
    if (private_lock_path(special_root_path, &fixture, "special-root.lock")) {
        (void)unlink(special_root_path);
    }
    (void)unlink(target);

    char moved_root[PRIVATE_LOCK_TEST_PATH_CAP];
    int moved_written = snprintf(moved_root, sizeof(moved_root), "%s-old", fixture.root);
    bool replaced = fixtures_ok && moved_written > 0 && moved_written < (int)sizeof(moved_root) &&
                    rename(fixture.root, moved_root) == 0 && mkdir(fixture.root, 0700) == 0;
    cbm_private_file_lock_status_t replaced_status =
        replaced ? cbm_private_file_lock_try_acquire(fixture.directory, "replaced.lock",
                                                     CBM_PRIVATE_FILE_LOCK_EX, &lock)
                 : CBM_PRIVATE_FILE_LOCK_IO;

    cbm_private_lock_directory_close(fixture.directory);
    fixture.directory = NULL;
    if (replaced) {
        (void)rmdir(fixture.root);
        (void)rmdir(moved_root);
        (void)rmdir(fixture.parent);
    } else {
        private_lock_fixture_finish(&fixture);
    }

    ASSERT_TRUE(started);
    ASSERT_TRUE(fixtures_ok);
    ASSERT_EQ(symlink_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_EQ(hardlink_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_EQ(mode_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_EQ(special_mode_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_EQ(fifo_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_TRUE(special_root_set);
    ASSERT_EQ(special_root_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_TRUE(special_root_restored);
    ASSERT_TRUE(replaced);
    ASSERT_EQ(replaced_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    PASS();
#endif
}

#ifdef __APPLE__
TEST(private_file_lock_macos_rejects_directory_acl_added_after_adoption) {
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    char lock_path[PRIVATE_LOCK_TEST_PATH_CAP] = {0};
    bool path_ok = started && private_lock_path(lock_path, &fixture, "acl-directory.lock");
    int acl_fixture = path_ok ? private_lock_macos_set_mutating_acl(fixture.root) : -1;
    if (acl_fixture == 0) {
        private_lock_fixture_finish(&fixture);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    struct stat root_status = {0};
    bool mode_stayed_private = acl_fixture == 1 && lstat(fixture.root, &root_status) == 0 &&
                               S_ISDIR(root_status.st_mode) &&
                               (root_status.st_mode & 07777) == 0700;

    cbm_private_file_lock_t *lock = NULL;
    cbm_private_file_lock_status_t acquire_status =
        mode_stayed_private
            ? cbm_private_file_lock_try_acquire(fixture.directory, "acl-directory.lock",
                                                CBM_PRIVATE_FILE_LOCK_EX, &lock)
            : CBM_PRIVATE_FILE_LOCK_IO;
    struct stat unexpected = {0};
    errno = 0;
    bool file_was_not_created = lstat(lock_path, &unexpected) != 0 && errno == ENOENT;
    if (lock) {
        (void)cbm_private_file_lock_release(&lock);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(path_ok);
    ASSERT_EQ(acl_fixture, 1);
    ASSERT_TRUE(mode_stayed_private);
    ASSERT_EQ(acquire_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_NULL(lock);
    ASSERT_TRUE(file_was_not_created);
    PASS();
}

TEST(private_file_lock_macos_rejects_file_acl_added_after_acquisition) {
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    char lock_path[PRIVATE_LOCK_TEST_PATH_CAP] = {0};
    bool path_ok = started && private_lock_path(lock_path, &fixture, "acl-file.lock");
    cbm_private_file_lock_t *lock = NULL;
    cbm_private_file_lock_status_t acquire_status =
        path_ok ? cbm_private_file_lock_try_acquire(fixture.directory, "acl-file.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &lock)
                : CBM_PRIVATE_FILE_LOCK_IO;
    int acl_fixture = acquire_status == CBM_PRIVATE_FILE_LOCK_OK
                          ? private_lock_macos_set_mutating_acl(lock_path)
                          : -1;
    if (acl_fixture == 0) {
        if (lock) {
            (void)cbm_private_file_lock_release(&lock);
        }
        private_lock_fixture_finish(&fixture);
        SKIP_PLATFORM("macOS fixture filesystem has no extended ACL support");
    }
    struct stat file_status = {0};
    bool mode_stayed_private = acl_fixture == 1 && lstat(lock_path, &file_status) == 0 &&
                               S_ISREG(file_status.st_mode) &&
                               (file_status.st_mode & 07777) == 0600;
    static const unsigned char payload[] = {'a', 'c', 'l'};
    cbm_private_file_lock_status_t payload_status =
        mode_stayed_private ? cbm_private_file_lock_payload_write(lock, payload, sizeof(payload))
                            : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t release_status =
        lock ? cbm_private_file_lock_release(&lock) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_t *reopened = NULL;
    cbm_private_file_lock_status_t reopen_status = cbm_private_file_lock_try_acquire(
        fixture.directory, "acl-file.lock", CBM_PRIVATE_FILE_LOCK_EX, &reopened);
    if (reopened) {
        (void)cbm_private_file_lock_release(&reopened);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(path_ok);
    ASSERT_EQ(acquire_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(acl_fixture, 1);
    ASSERT_TRUE(mode_stayed_private);
    ASSERT_EQ(payload_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_EQ(release_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(lock);
    ASSERT_EQ(reopen_status, CBM_PRIVATE_FILE_LOCK_UNSAFE);
    ASSERT_NULL(reopened);
    PASS();
}
#endif

TEST(private_file_lock_fork_child_cannot_unlock_or_retain_parent_lock) {
#ifdef _WIN32
    SKIP_PLATFORM("fork inheritance applies only to POSIX");
#else
    private_lock_fixture_t fixture;
    bool started = private_lock_fixture_start(&fixture);
    cbm_private_file_lock_t *parent_lock = NULL;
    cbm_private_file_lock_status_t acquired =
        started ? cbm_private_file_lock_try_acquire(fixture.directory, "fork.lock",
                                                    CBM_PRIVATE_FILE_LOCK_EX, &parent_lock)
                : CBM_PRIVATE_FILE_LOCK_IO;
    bool cloexec = cbm_private_file_lock_is_cloexec_for_test(parent_lock);
    int command_pipe[2] = {-1, -1};
    int result_pipe[2] = {-1, -1};
    bool pipes_ok =
        acquired == CBM_PRIVATE_FILE_LOCK_OK && pipe(command_pipe) == 0 && pipe(result_pipe) == 0;
    pid_t child = pipes_ok ? fork() : -1;
    if (child == 0) {
        (void)close(command_pipe[1]);
        (void)close(result_pipe[0]);
        cbm_private_file_lock_status_t child_release = cbm_private_file_lock_release(&parent_lock);
        char report = child_release == CBM_PRIVATE_FILE_LOCK_OK ? 'R' : 'E';
        bool reported = private_lock_fd_write_byte(result_pipe[1], report);
        char command = 0;
        bool commanded = private_lock_fd_read_byte(command_pipe[0], &command);
        (void)close(command_pipe[0]);
        (void)close(result_pipe[1]);
        _exit(reported && commanded && command == 'X' ? 0 : 1);
    }

    char child_report = 0;
    bool child_released = false;
    cbm_private_file_lock_t *contender = NULL;
    cbm_private_file_lock_status_t while_parent = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t after_parent = CBM_PRIVATE_FILE_LOCK_IO;
    bool parent_released = false;
    bool child_commanded = false;
    int child_status = -1;
    if (child > 0) {
        (void)close(command_pipe[0]);
        (void)close(result_pipe[1]);
        child_released =
            private_lock_fd_read_byte(result_pipe[0], &child_report) && child_report == 'R';
        while_parent = cbm_private_file_lock_try_acquire(fixture.directory, "fork.lock",
                                                         CBM_PRIVATE_FILE_LOCK_EX, &contender);
        parent_released = cbm_private_file_lock_release(&parent_lock) == CBM_PRIVATE_FILE_LOCK_OK;
        after_parent = cbm_private_file_lock_try_acquire(fixture.directory, "fork.lock",
                                                         CBM_PRIVATE_FILE_LOCK_EX, &contender);
        if (contender) {
            (void)cbm_private_file_lock_release(&contender);
        }
        child_commanded = private_lock_fd_write_byte(command_pipe[1], 'X');
        (void)close(command_pipe[1]);
        (void)close(result_pipe[0]);
        (void)waitpid(child, &child_status, 0);
    }
    if (parent_lock) {
        (void)cbm_private_file_lock_release(&parent_lock);
    }
    if (command_pipe[0] >= 0 && child <= 0) {
        (void)close(command_pipe[0]);
        (void)close(command_pipe[1]);
    }
    if (result_pipe[0] >= 0 && child <= 0) {
        (void)close(result_pipe[0]);
        (void)close(result_pipe[1]);
    }
    private_lock_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(cloexec);
    ASSERT_TRUE(pipes_ok);
    ASSERT_GT(child, 0);
    ASSERT_TRUE(child_released);
    ASSERT_EQ(while_parent, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_TRUE(parent_released);
    ASSERT_EQ(after_parent, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(child_commanded);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    PASS();
#endif
}

SUITE(private_file_lock) {
    RUN_TEST(private_file_lock_shared_and_exclusive_matrix);
    RUN_TEST(private_file_lock_payload_requires_exclusive_write_and_survives_reopen);
    RUN_TEST(private_file_lock_unlock_failure_retains_retryable_lock);
    RUN_TEST(private_file_lock_close_failure_retries_without_duplicate_unlock);
    RUN_TEST(private_file_lock_consumed_close_error_never_retries_recycled_fd);
    RUN_TEST(private_file_lock_post_acquire_failure_returns_cleanup_owner);
    RUN_TEST(private_file_lock_windows_lock_attempt_failure_returns_cleanup_owner);
    RUN_TEST(private_file_lock_rejects_unsafe_entries_and_replaced_root);
#ifdef __APPLE__
    RUN_TEST(private_file_lock_macos_rejects_directory_acl_added_after_adoption);
    RUN_TEST(private_file_lock_macos_rejects_file_acl_added_after_acquisition);
#endif
    RUN_TEST(private_file_lock_fork_child_cannot_unlock_or_retain_parent_lock);
}
