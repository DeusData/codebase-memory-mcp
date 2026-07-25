/*
 * test_daemon_version.c — RED contract for one-version daemon rendezvous.
 *
 * These tests intentionally target the service boundary that will be exposed
 * by daemon/service.h.  They remain a separate suite so implementation can be
 * developed without mixing version policy into transport or job-lifecycle
 * tests.
 */
#include "test_framework.h"

#include "daemon/daemon.h"
#include "daemon/service.h"
#include "daemon/service_internal.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"

#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef _WIN32
#include <sys/stat.h>
#include <unistd.h>
#endif

enum {
    VERSION_TEST_PATH_CAP = 1024,
    VERSION_TEST_FILE_CAP = 8192,
};

static const char BUILD_A[] = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
static const char BUILD_B[] = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
static const char BUILD_C[] = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

static cbm_daemon_build_identity_t version_test_identity(const char *version, const char *build) {
    cbm_daemon_build_identity_t identity = {
        .semantic_version = version,
        .build_fingerprint = build,
        .protocol_abi = 3,
        .store_abi = 11,
        .feature_abi = 7,
    };
    return identity;
}

static bool version_test_temp_dir(char out[VERSION_TEST_PATH_CAP], const char *tag) {
    int written =
        snprintf(out, VERSION_TEST_PATH_CAP, "%s/cbm-daemon-%s-XXXXXX", cbm_tmpdir(), tag);
    return written > 0 && written < VERSION_TEST_PATH_CAP && cbm_mkdtemp(out) != NULL;
}

static bool version_test_child_path(char out[VERSION_TEST_PATH_CAP], const char *dir,
                                    const char *name) {
    int written = snprintf(out, VERSION_TEST_PATH_CAP, "%s/%s", dir, name);
    return written > 0 && written < VERSION_TEST_PATH_CAP;
}

static long version_test_file_size(const char *path) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return -1;
    }
    if (fseek(file, 0, SEEK_END) != 0) {
        (void)fclose(file);
        return -1;
    }
    long size = ftell(file);
    (void)fclose(file);
    return size;
}

static bool version_test_read_file(const char *path, char out[VERSION_TEST_FILE_CAP]) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file) {
        return false;
    }
    size_t used = fread(out, 1, VERSION_TEST_FILE_CAP - 1, file);
    bool complete = !ferror(file) && feof(file);
    out[used] = '\0';
    (void)fclose(file);
    return complete;
}

static bool version_test_write_file(const char *path, const char *contents) {
    FILE *file = cbm_fopen(path, "wb");
    if (!file) {
        return false;
    }
    size_t length = strlen(contents);
    bool wrote = fwrite(contents, 1, length, file) == length;
    bool closed = fclose(file) == 0;
    return wrote && closed;
}

static bool version_test_is_sha256(const char *value) {
    if (!value || strlen(value) != CBM_DAEMON_BUILD_FINGERPRINT_SIZE - 1) {
        return false;
    }
    for (size_t i = 0; i < CBM_DAEMON_BUILD_FINGERPRINT_SIZE - 1; i++) {
        char ch = value[i];
        if (!((ch >= '0' && ch <= '9') || (ch >= 'a' && ch <= 'f'))) {
            return false;
        }
    }
    return true;
}

static void version_test_cleanup(const char *dir, const char *path, const char *rotated,
                                 const char *victim) {
    char lock_path[VERSION_TEST_PATH_CAP];
    if (path) {
        int written = snprintf(lock_path, sizeof(lock_path), "%s.lock", path);
        if (written > 0 && written < (int)sizeof(lock_path)) {
            (void)cbm_unlink(lock_path);
        }
    }
    if (path) {
        (void)cbm_unlink(path);
    }
    if (rotated) {
        (void)cbm_unlink(rotated);
    }
    if (victim) {
        (void)cbm_unlink(victim);
    }
    if (dir) {
        (void)cbm_rmdir(dir);
    }
}

#ifndef _WIN32
typedef struct {
    atomic_int lock_attempts;
    atomic_bool first_lock_acquired;
} version_test_rotation_race_t;

typedef struct {
    const char *path;
    const cbm_daemon_conflict_t *conflict;
    size_t cap_bytes;
    bool result;
} version_test_append_call_t;

static bool version_test_wait_atomic_bool(atomic_bool *value) {
    struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    for (int i = 0; i < 2000; i++) {
        if (atomic_load_explicit(value, memory_order_acquire)) {
            return true;
        }
        (void)cbm_nanosleep(&pause, NULL);
    }
    return atomic_load_explicit(value, memory_order_acquire);
}

static void version_test_rotation_race_hook(void *opaque,
                                            cbm_daemon_conflict_log_test_stage_t stage) {
    version_test_rotation_race_t *race = opaque;
    if (stage == CBM_DAEMON_CONFLICT_LOG_BEFORE_SERIALIZATION_LOCK) {
        (void)atomic_fetch_add_explicit(&race->lock_attempts, 1, memory_order_acq_rel);
        return;
    }
    if (stage != CBM_DAEMON_CONFLICT_LOG_AFTER_SERIALIZATION_LOCK ||
        atomic_exchange_explicit(&race->first_lock_acquired, true, memory_order_acq_rel)) {
        return;
    }
    struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    for (int i = 0;
         i < 2000 && atomic_load_explicit(&race->lock_attempts, memory_order_acquire) < 2; i++) {
        (void)cbm_nanosleep(&pause, NULL);
    }
}

static void *version_test_append_thread(void *opaque) {
    version_test_append_call_t *call = opaque;
    call->result = cbm_daemon_conflict_log_append(call->path, call->conflict, call->cap_bytes);
    return NULL;
}
#endif

static cbm_daemon_hello_status_t version_test_compare(const cbm_daemon_build_identity_t *active,
                                                      const cbm_daemon_build_identity_t *requested,
                                                      cbm_daemon_conflict_t *conflict) {
    memset(conflict, 0, sizeof(*conflict));
    return cbm_daemon_hello_compare(active, requested, conflict);
}

static bool version_test_conflict_identity_matches(const cbm_daemon_conflict_t *conflict,
                                                   const char *active_version,
                                                   const char *active_build,
                                                   const char *requested_version,
                                                   const char *requested_build) {
    return conflict && strcmp(conflict->active_version, active_version) == 0 &&
           strcmp(conflict->active_build_fingerprint, active_build) == 0 &&
           strcmp(conflict->requested_version, requested_version) == 0 &&
           strcmp(conflict->requested_build_fingerprint, requested_build) == 0;
}

TEST(daemon_rendezvous_key_is_stable_and_version_independent) {
    char first[CBM_DAEMON_KEY_SIZE];
    char after_upgrade[CBM_DAEMON_KEY_SIZE];

    /* Version, executable path, build fingerprint, cache directory, and ABI
     * values are deliberately absent from this API. Every build and cache
     * domain must rendezvous at the same product endpoint before HELLO decides
     * whether it may proceed. The IPC layer supplies the trusted OS-account
     * scope through its owner-only runtime directory / current-user ACL. */
    ASSERT_TRUE(cbm_daemon_rendezvous_key(first));
    ASSERT_TRUE(cbm_daemon_rendezvous_key(after_upgrade));

    ASSERT_STR_EQ(first, after_upgrade);
    PASS();
}

TEST(daemon_build_fingerprint_hashes_exact_executable_bytes) {
    char dir[VERSION_TEST_PATH_CAP] = {0};
    char first_path[VERSION_TEST_PATH_CAP] = {0};
    char second_path[VERSION_TEST_PATH_CAP] = {0};
    char first[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    char repeated[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    char second[CBM_DAEMON_BUILD_FINGERPRINT_SIZE];
    bool setup_ok = version_test_temp_dir(dir, "fingerprint") &&
                    version_test_child_path(first_path, dir, "build-a.bin") &&
                    version_test_child_path(second_path, dir, "build-b.bin") &&
                    version_test_write_file(first_path, "same-version-build-a") &&
                    version_test_write_file(second_path, "same-version-build-b");
    if (!setup_ok) {
        version_test_cleanup(dir, first_path, second_path, NULL);
        FAIL("could not create fingerprint fixtures");
    }

    ASSERT_TRUE(cbm_daemon_build_fingerprint_file(first_path, first));
    ASSERT_TRUE(cbm_daemon_build_fingerprint_file(first_path, repeated));
    ASSERT_TRUE(cbm_daemon_build_fingerprint_file(second_path, second));
    ASSERT_TRUE(version_test_is_sha256(first));
    ASSERT_TRUE(version_test_is_sha256(second));
    ASSERT_STR_EQ(first, repeated);
    ASSERT_STR_NEQ(first, second);

    version_test_cleanup(dir, first_path, second_path, NULL);
    PASS();
}

TEST(daemon_hello_accepts_only_the_exact_active_build_identity) {
    cbm_daemon_build_identity_t active = version_test_identity("2.4.0", BUILD_A);
    cbm_daemon_build_identity_t exact = version_test_identity("2.4.0", BUILD_A);
    cbm_daemon_conflict_t conflict;

    ASSERT_EQ(version_test_compare(&active, &exact, &conflict), CBM_DAEMON_HELLO_COMPATIBLE);

    cbm_daemon_build_identity_t rebuilt = version_test_identity("2.4.0", BUILD_B);
    ASSERT_EQ(version_test_compare(&active, &rebuilt, &conflict), CBM_DAEMON_HELLO_BUILD_CONFLICT);
    ASSERT_TRUE(
        version_test_conflict_identity_matches(&conflict, "2.4.0", BUILD_A, "2.4.0", BUILD_B));
    PASS();
}

TEST(daemon_hello_version_conflict_exposes_active_and_requested_builds) {
    cbm_daemon_build_identity_t active = version_test_identity("2.3.1", BUILD_A);
    cbm_daemon_build_identity_t requested = version_test_identity("2.4.0", BUILD_B);
    cbm_daemon_conflict_t conflict;
    char visible[CBM_DAEMON_CONFLICT_MESSAGE_SIZE];

    ASSERT_EQ(version_test_compare(&active, &requested, &conflict),
              CBM_DAEMON_HELLO_VERSION_CONFLICT);
    ASSERT_TRUE(
        version_test_conflict_identity_matches(&conflict, "2.3.1", BUILD_A, "2.4.0", BUILD_B));
    ASSERT_TRUE(cbm_daemon_conflict_format(&conflict, visible, sizeof(visible)));
    ASSERT_NOT_NULL(strstr(visible, "could not start"));
    ASSERT_NOT_NULL(strstr(visible, "conflicting"));
    ASSERT_NOT_NULL(strstr(visible, "2.3.1"));
    ASSERT_NOT_NULL(strstr(visible, BUILD_A));
    ASSERT_NOT_NULL(strstr(visible, "2.4.0"));
    ASSERT_NOT_NULL(strstr(visible, BUILD_B));
    PASS();
}

TEST(daemon_hello_rejects_each_abi_mismatch) {
    cbm_daemon_build_identity_t active = version_test_identity("2.4.0", BUILD_A);
    cbm_daemon_build_identity_t requested = active;
    cbm_daemon_conflict_t conflict;

    requested.protocol_abi++;
    ASSERT_EQ(version_test_compare(&active, &requested, &conflict),
              CBM_DAEMON_HELLO_PROTOCOL_ABI_CONFLICT);
    ASSERT_TRUE(
        version_test_conflict_identity_matches(&conflict, "2.4.0", BUILD_A, "2.4.0", BUILD_A));

    requested = active;
    requested.store_abi++;
    ASSERT_EQ(version_test_compare(&active, &requested, &conflict),
              CBM_DAEMON_HELLO_STORE_ABI_CONFLICT);
    ASSERT_TRUE(
        version_test_conflict_identity_matches(&conflict, "2.4.0", BUILD_A, "2.4.0", BUILD_A));

    requested = active;
    requested.feature_abi++;
    ASSERT_EQ(version_test_compare(&active, &requested, &conflict),
              CBM_DAEMON_HELLO_FEATURE_ABI_CONFLICT);
    ASSERT_TRUE(
        version_test_conflict_identity_matches(&conflict, "2.4.0", BUILD_A, "2.4.0", BUILD_A));
    PASS();
}

TEST(daemon_hello_fails_closed_without_an_exact_build_fingerprint) {
    cbm_daemon_build_identity_t active = version_test_identity("2.4.0", BUILD_A);
    cbm_daemon_build_identity_t missing = version_test_identity("2.4.0", NULL);
    cbm_daemon_build_identity_t malformed = version_test_identity("2.4.0", "not-a-sha256");
    cbm_daemon_conflict_t conflict;

    ASSERT_EQ(version_test_compare(&active, &missing, &conflict), CBM_DAEMON_HELLO_INVALID);
    ASSERT_EQ(version_test_compare(&active, &malformed, &conflict), CBM_DAEMON_HELLO_INVALID);
    PASS();
}

TEST(daemon_conflict_log_is_durable_private_and_rotates) {
    char dir[VERSION_TEST_PATH_CAP] = {0};
    char path[VERSION_TEST_PATH_CAP] = {0};
    char rotated[VERSION_TEST_PATH_CAP] = {0};
    char lock_path[VERSION_TEST_PATH_CAP] = {0};
    char current_data[VERSION_TEST_FILE_CAP];
    char rotated_data[VERSION_TEST_FILE_CAP];
    cbm_daemon_build_identity_t active = version_test_identity("2.3.1", BUILD_A);
    cbm_daemon_build_identity_t first_requested = version_test_identity("2.4.0", BUILD_B);
    cbm_daemon_build_identity_t second_requested = version_test_identity("2.5.0", BUILD_C);
    cbm_daemon_conflict_t first_conflict;
    cbm_daemon_conflict_t second_conflict;
    bool setup_ok = version_test_temp_dir(dir, "log") &&
                    version_test_child_path(path, dir, "daemon.log") &&
                    version_test_child_path(rotated, dir, "daemon.log.1") &&
                    version_test_child_path(lock_path, dir, "daemon.log.lock");
    if (!setup_ok) {
        version_test_cleanup(dir, path, rotated, NULL);
        FAIL("could not create isolated daemon-log directory");
    }

    ASSERT_EQ(version_test_compare(&active, &first_requested, &first_conflict),
              CBM_DAEMON_HELLO_VERSION_CONFLICT);
    ASSERT_TRUE(cbm_daemon_conflict_log_append(path, &first_conflict, 4096));
    long first_size = version_test_file_size(path);
    ASSERT_GT(first_size, 0);

#ifndef _WIN32
    struct stat status;
    ASSERT_EQ(lstat(path, &status), 0);
    ASSERT_TRUE(S_ISREG(status.st_mode));
    ASSERT_EQ(status.st_mode & 0777, 0600);
    ASSERT_EQ(lstat(lock_path, &status), 0);
    ASSERT_TRUE(S_ISREG(status.st_mode));
    ASSERT_EQ(status.st_uid, geteuid());
    ASSERT_EQ(status.st_mode & 0777, 0600);
#endif

    /* The second complete record would cross this cap, so the old complete
     * generation moves to .1 and the new record becomes the active log. */
    ASSERT_EQ(version_test_compare(&active, &second_requested, &second_conflict),
              CBM_DAEMON_HELLO_VERSION_CONFLICT);
    ASSERT_TRUE(cbm_daemon_conflict_log_append(path, &second_conflict, (size_t)first_size + 1));
    ASSERT_TRUE(version_test_read_file(path, current_data));
    ASSERT_TRUE(version_test_read_file(rotated, rotated_data));

    ASSERT_NOT_NULL(strstr(rotated_data, "\"event\":\"daemon.version_conflict\""));
    ASSERT_NOT_NULL(strstr(rotated_data, "\"active_version\":\"2.3.1\""));
    ASSERT_NOT_NULL(strstr(rotated_data, "\"requested_version\":\"2.4.0\""));
    ASSERT_NOT_NULL(strstr(rotated_data, BUILD_A));
    ASSERT_NOT_NULL(strstr(rotated_data, BUILD_B));
    ASSERT_NOT_NULL(strstr(current_data, "\"requested_version\":\"2.5.0\""));
    ASSERT_NOT_NULL(strstr(current_data, BUILD_C));

    version_test_cleanup(dir, path, rotated, NULL);
    PASS();
}

#ifndef _WIN32
TEST(daemon_conflict_log_rotation_serializes_on_stable_sidecar) {
    enum { ROTATION_CAP = 4096, PADDED_LOG_SIZE = 4000 };
    char dir[VERSION_TEST_PATH_CAP] = {0};
    char path[VERSION_TEST_PATH_CAP] = {0};
    char rotated[VERSION_TEST_PATH_CAP] = {0};
    char padded[PADDED_LOG_SIZE + 1];
    char current_data[VERSION_TEST_FILE_CAP] = {0};
    cbm_daemon_build_identity_t active = version_test_identity("2.3.1", BUILD_A);
    cbm_daemon_build_identity_t seed_requested = version_test_identity("2.4.0", BUILD_B);
    cbm_daemon_build_identity_t first_requested = version_test_identity("2.5.0", BUILD_B);
    cbm_daemon_build_identity_t second_requested = version_test_identity("2.6.0", BUILD_C);
    cbm_daemon_conflict_t seed_conflict;
    cbm_daemon_conflict_t first_conflict;
    cbm_daemon_conflict_t second_conflict;
    version_test_rotation_race_t race = {0};
    version_test_append_call_t first_call = {0};
    version_test_append_call_t second_call = {0};
    cbm_thread_t first_thread;
    cbm_thread_t second_thread;
    bool first_started = false;
    bool second_started = false;

    bool setup_ok = version_test_temp_dir(dir, "log-rotation-race") &&
                    version_test_child_path(path, dir, "daemon.log") &&
                    version_test_child_path(rotated, dir, "daemon.log.1") &&
                    version_test_compare(&active, &seed_requested, &seed_conflict) ==
                        CBM_DAEMON_HELLO_VERSION_CONFLICT &&
                    version_test_compare(&active, &first_requested, &first_conflict) ==
                        CBM_DAEMON_HELLO_VERSION_CONFLICT &&
                    version_test_compare(&active, &second_requested, &second_conflict) ==
                        CBM_DAEMON_HELLO_VERSION_CONFLICT &&
                    cbm_daemon_conflict_log_append(path, &seed_conflict, 8192);
    memset(padded, 'x', PADDED_LOG_SIZE);
    padded[PADDED_LOG_SIZE - 1] = '\n';
    padded[PADDED_LOG_SIZE] = '\0';
    setup_ok = setup_ok && version_test_write_file(path, padded);
    if (!setup_ok) {
        version_test_cleanup(dir, path, rotated, NULL);
        FAIL("could not create rotation-race fixture");
    }

    first_call.path = path;
    first_call.conflict = &first_conflict;
    first_call.cap_bytes = ROTATION_CAP;
    second_call.path = path;
    second_call.conflict = &second_conflict;
    second_call.cap_bytes = ROTATION_CAP;

    cbm_daemon_conflict_log_set_test_hook(version_test_rotation_race_hook, &race);
    first_started =
        cbm_thread_create(&first_thread, 0, version_test_append_thread, &first_call) == 0;
    bool first_locked = first_started && version_test_wait_atomic_bool(&race.first_lock_acquired);
    if (first_locked) {
        second_started =
            cbm_thread_create(&second_thread, 0, version_test_append_thread, &second_call) == 0;
    }
    if (first_started) {
        (void)cbm_thread_join(&first_thread);
    }
    if (second_started) {
        (void)cbm_thread_join(&second_thread);
    }
    cbm_daemon_conflict_log_set_test_hook(NULL, NULL);

    bool complete = version_test_read_file(path, current_data);
    bool first_present = strstr(current_data, "\"requested_version\":\"2.5.0\"") != NULL;
    bool second_present = strstr(current_data, "\"requested_version\":\"2.6.0\"") != NULL;
    version_test_cleanup(dir, path, rotated, NULL);

    ASSERT_TRUE(first_started);
    ASSERT_TRUE(first_locked);
    ASSERT_TRUE(second_started);
    ASSERT_TRUE(first_call.result);
    ASSERT_TRUE(second_call.result);
    ASSERT_TRUE(complete);
    ASSERT_TRUE(first_present);
    ASSERT_TRUE(second_present);
    PASS();
}

TEST(daemon_conflict_log_rejects_symlink_destination) {
    char dir[VERSION_TEST_PATH_CAP] = {0};
    char path[VERSION_TEST_PATH_CAP] = {0};
    char victim[VERSION_TEST_PATH_CAP] = {0};
    char victim_data[VERSION_TEST_FILE_CAP];
    cbm_daemon_build_identity_t active = version_test_identity("2.3.1", BUILD_A);
    cbm_daemon_build_identity_t requested = version_test_identity("2.4.0", BUILD_B);
    cbm_daemon_conflict_t conflict;
    bool setup_ok = version_test_temp_dir(dir, "log-link") &&
                    version_test_child_path(path, dir, "daemon.log") &&
                    version_test_child_path(victim, dir, "victim.txt");
    if (!setup_ok) {
        version_test_cleanup(dir, path, NULL, victim);
        FAIL("could not create isolated symlink-test directory");
    }

    FILE *file = cbm_fopen(victim, "wb");
    bool victim_written = false;
    if (file) {
        victim_written = fputs("must-remain-unchanged", file) >= 0 && fclose(file) == 0;
        file = NULL;
    }
    if (!victim_written) {
        if (file) {
            (void)fclose(file);
        }
        version_test_cleanup(dir, path, NULL, victim);
        FAIL("could not create symlink victim");
    }
    if (symlink(victim, path) != 0) {
        version_test_cleanup(dir, path, NULL, victim);
        FAIL("could not create daemon-log symlink");
    }

    ASSERT_EQ(version_test_compare(&active, &requested, &conflict),
              CBM_DAEMON_HELLO_VERSION_CONFLICT);
    ASSERT_FALSE(cbm_daemon_conflict_log_append(path, &conflict, 4096));
    ASSERT_TRUE(version_test_read_file(victim, victim_data));
    ASSERT_STR_EQ(victim_data, "must-remain-unchanged");

    version_test_cleanup(dir, path, NULL, victim);
    PASS();
}

TEST(daemon_conflict_log_rejects_symlink_sidecar) {
    char dir[VERSION_TEST_PATH_CAP] = {0};
    char path[VERSION_TEST_PATH_CAP] = {0};
    char lock_path[VERSION_TEST_PATH_CAP] = {0};
    char victim[VERSION_TEST_PATH_CAP] = {0};
    char victim_data[VERSION_TEST_FILE_CAP];
    cbm_daemon_build_identity_t active = version_test_identity("2.3.1", BUILD_A);
    cbm_daemon_build_identity_t requested = version_test_identity("2.4.0", BUILD_B);
    cbm_daemon_conflict_t conflict;
    bool setup_ok = version_test_temp_dir(dir, "log-lock-link") &&
                    version_test_child_path(path, dir, "daemon.log") &&
                    version_test_child_path(lock_path, dir, "daemon.log.lock") &&
                    version_test_child_path(victim, dir, "victim.txt") &&
                    version_test_write_file(victim, "must-remain-unchanged") &&
                    symlink(victim, lock_path) == 0;
    if (!setup_ok) {
        version_test_cleanup(dir, path, NULL, victim);
        FAIL("could not create daemon-log lock symlink");
    }

    ASSERT_EQ(version_test_compare(&active, &requested, &conflict),
              CBM_DAEMON_HELLO_VERSION_CONFLICT);
    ASSERT_FALSE(cbm_daemon_conflict_log_append(path, &conflict, 4096));
    ASSERT_TRUE(version_test_read_file(victim, victim_data));
    ASSERT_STR_EQ(victim_data, "must-remain-unchanged");

    version_test_cleanup(dir, path, NULL, victim);
    PASS();
}
#endif

#ifdef _WIN32
typedef struct {
    const char *path;
    const cbm_daemon_conflict_t *conflict;
    atomic_bool *start;
    bool result;
} version_test_windows_append_t;

static void *version_test_windows_append_thread(void *opaque) {
    version_test_windows_append_t *call = opaque;
    struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    while (!atomic_load_explicit(call->start, memory_order_acquire)) {
        (void)cbm_nanosleep(&pause, NULL);
    }
    call->result = cbm_daemon_conflict_log_append(call->path, call->conflict, 64U * 1024U);
    return NULL;
}

TEST(daemon_conflict_log_windows_concurrent_appends_are_not_dropped) {
    enum { APPEND_COUNT = 8 };
    char dir[VERSION_TEST_PATH_CAP] = {0};
    char path[VERSION_TEST_PATH_CAP] = {0};
    char rotated[VERSION_TEST_PATH_CAP] = {0};
    char current_data[VERSION_TEST_FILE_CAP] = {0};
    cbm_daemon_build_identity_t active = version_test_identity("2.3.1", BUILD_A);
    cbm_daemon_build_identity_t requested = version_test_identity("2.4.0", BUILD_B);
    cbm_daemon_conflict_t conflict;
    cbm_thread_t threads[APPEND_COUNT];
    version_test_windows_append_t calls[APPEND_COUNT];
    atomic_bool start = false;
    size_t started = 0;

    bool setup_ok =
        version_test_temp_dir(dir, "log-windows-concurrent") &&
        version_test_child_path(path, dir, "daemon.log") &&
        version_test_child_path(rotated, dir, "daemon.log.1") &&
        version_test_compare(&active, &requested, &conflict) == CBM_DAEMON_HELLO_VERSION_CONFLICT;
    if (!setup_ok) {
        version_test_cleanup(dir, path, rotated, NULL);
        FAIL("could not create Windows concurrent-log fixture");
    }

    memset(calls, 0, sizeof(calls));
    for (; started < APPEND_COUNT; started++) {
        calls[started].path = path;
        calls[started].conflict = &conflict;
        calls[started].start = &start;
        if (cbm_thread_create(&threads[started], 0, version_test_windows_append_thread,
                              &calls[started]) != 0) {
            break;
        }
    }
    atomic_store_explicit(&start, true, memory_order_release);
    for (size_t i = 0; i < started; i++) {
        (void)cbm_thread_join(&threads[i]);
    }

    bool complete = version_test_read_file(path, current_data);
    size_t records = 0;
    const char *cursor = current_data;
    static const char event[] = "\"event\":\"daemon.version_conflict\"";
    while ((cursor = strstr(cursor, event)) != NULL) {
        records++;
        cursor += sizeof(event) - 1;
    }
    bool all_succeeded = true;
    for (size_t i = 0; i < started; i++) {
        all_succeeded = all_succeeded && calls[i].result;
    }
    version_test_cleanup(dir, path, rotated, NULL);

    ASSERT_EQ(started, APPEND_COUNT);
    ASSERT_TRUE(all_succeeded);
    ASSERT_TRUE(complete);
    ASSERT_EQ(records, APPEND_COUNT);
    PASS();
}
#endif

SUITE(daemon_version) {
    RUN_TEST(daemon_rendezvous_key_is_stable_and_version_independent);
    RUN_TEST(daemon_build_fingerprint_hashes_exact_executable_bytes);
    RUN_TEST(daemon_hello_accepts_only_the_exact_active_build_identity);
    RUN_TEST(daemon_hello_version_conflict_exposes_active_and_requested_builds);
    RUN_TEST(daemon_hello_rejects_each_abi_mismatch);
    RUN_TEST(daemon_hello_fails_closed_without_an_exact_build_fingerprint);
    RUN_TEST(daemon_conflict_log_is_durable_private_and_rotates);
#ifndef _WIN32
    RUN_TEST(daemon_conflict_log_rotation_serializes_on_stable_sidecar);
    RUN_TEST(daemon_conflict_log_rejects_symlink_destination);
    RUN_TEST(daemon_conflict_log_rejects_symlink_sidecar);
#endif
#ifdef _WIN32
    RUN_TEST(daemon_conflict_log_windows_concurrent_appends_are_not_dropped);
#endif
}
