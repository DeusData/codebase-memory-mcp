/* RED contract for the generic writer-preference lock registry. */
#include "test_framework.h"

#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/lock_registry.h"
#include "foundation/lock_registry_internal.h"
#include "foundation/platform.h"
#include "foundation/private_file_lock_internal.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#ifndef _WIN32
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <sched.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#endif

enum {
    LOCK_REGISTRY_TEST_PATH_CAP = 1024,
    LOCK_REGISTRY_TEST_TIMEOUT_MS = 5000,
    LOCK_REGISTRY_STRESS_THREADS = 8,
    LOCK_REGISTRY_STRESS_ITERATIONS = 160,
    LOCK_REGISTRY_PARKING_WAITERS = 64,
};

typedef struct {
    char parent[LOCK_REGISTRY_TEST_PATH_CAP];
    char root[LOCK_REGISTRY_TEST_PATH_CAP];
    cbm_private_lock_directory_t *directory;
    cbm_lock_registry_t *registry;
} lock_registry_fixture_t;

/* The stress fixtures that use this helper are POSIX-only. */
#ifndef _WIN32
static void lock_registry_test_yield(void) {
    (void)sched_yield();
}
#endif

#ifndef _WIN32
static cbm_private_lock_directory_t *lock_registry_test_directory_open(const char *root) {
    int fd = open(root, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    cbm_private_lock_directory_t *directory = NULL;
    if (fd < 0 ||
        cbm_private_lock_directory_adopt_posix(fd, root, &directory) != CBM_PRIVATE_FILE_LOCK_OK) {
        if (fd >= 0) {
            (void)close(fd);
        }
        return NULL;
    }
    return directory;
}
#endif

#ifndef _WIN32
static bool lock_registry_fixture_start(lock_registry_fixture_t *fixture) {
    memset(fixture, 0, sizeof(*fixture));
#ifdef _WIN32
    return false;
#else
    int written = snprintf(fixture->parent, sizeof(fixture->parent), "%s/cbm-lock-registry-XXXXXX",
                           cbm_tmpdir());
    if (written <= 0 || written >= (int)sizeof(fixture->parent) || !cbm_mkdtemp(fixture->parent)) {
        return false;
    }
    written = snprintf(fixture->root, sizeof(fixture->root), "%s/root", fixture->parent);
    if (written <= 0 || written >= (int)sizeof(fixture->root) || mkdir(fixture->root, 0700) != 0) {
        return false;
    }
    fixture->directory = lock_registry_test_directory_open(fixture->root);
    fixture->registry = cbm_lock_registry_new(fixture->directory);
    return fixture->directory != NULL && fixture->registry != NULL;
#endif
}

static void lock_registry_fixture_finish(lock_registry_fixture_t *fixture) {
    (void)cbm_lock_registry_free(&fixture->registry);
    cbm_private_lock_directory_close(fixture->directory);
#ifndef _WIN32
    DIR *directory = opendir(fixture->root);
    if (directory) {
        struct dirent *entry;
        while ((entry = readdir(directory)) != NULL) {
            if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
                continue;
            }
            char path[LOCK_REGISTRY_TEST_PATH_CAP];
            int written = snprintf(path, sizeof(path), "%s/%s", fixture->root, entry->d_name);
            if (written > 0 && written < (int)sizeof(path)) {
                (void)unlink(path);
            }
        }
        (void)closedir(directory);
    }
    (void)rmdir(fixture->root);
    (void)rmdir(fixture->parent);
#endif
    memset(fixture, 0, sizeof(*fixture));
}
#endif

typedef struct {
    cbm_lock_registry_t *registry;
    const char *resource_key;
    cbm_private_file_lock_mode_t mode;
    cbm_lock_cancel_token_t cancel_token;
    atomic_bool finished;
    cbm_private_file_lock_status_t status;
    cbm_lock_lease_t *lease;
} lock_registry_waiter_t;

#ifndef _WIN32
static void *lock_registry_waiter_run(void *opaque) {
    lock_registry_waiter_t *waiter = opaque;
    waiter->status = cbm_lock_registry_acquire(waiter->registry, waiter->resource_key, waiter->mode,
                                               cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS,
                                               &waiter->cancel_token, &waiter->lease);
    atomic_store_explicit(&waiter->finished, true, memory_order_release);
    return NULL;
}

typedef struct {
    cbm_lock_cancel_token_t cancel_token;
    atomic_bool native_ready;
} lock_registry_rollback_fault_t;

static void lock_registry_cancel_at_native_ready(void *opaque, cbm_private_file_lock_mode_t mode,
                                                 cbm_lock_registry_stage_t stage) {
    lock_registry_rollback_fault_t *fault = opaque;
    if (mode == CBM_PRIVATE_FILE_LOCK_EX && stage == CBM_LOCK_REGISTRY_STAGE_NATIVE_READY) {
        atomic_store_explicit(&fault->native_ready, true, memory_order_release);
        atomic_store_explicit(&fault->cancel_token, true, memory_order_release);
    }
}
#endif

TEST(lock_registry_cancelled_wait_rolls_back_and_does_not_barge) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry RED runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *holder = NULL;
    cbm_private_file_lock_status_t holder_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "cancelled-waiter",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &holder)
            : CBM_PRIVATE_FILE_LOCK_IO;

    lock_registry_waiter_t waiter = {.registry = fixture.registry,
                                     .resource_key = "cancelled-waiter",
                                     .mode = CBM_PRIVATE_FILE_LOCK_EX,
                                     .status = CBM_PRIVATE_FILE_LOCK_IO};
    atomic_init(&waiter.cancel_token, false);
    atomic_init(&waiter.finished, false);
    cbm_thread_t thread;
    bool thread_started = holder_status == CBM_PRIVATE_FILE_LOCK_OK &&
                          cbm_thread_create(&thread, 0, lock_registry_waiter_run, &waiter) == 0;
    bool queued = false;
    bool writer_has_turn = false;
    char turn_name[CBM_LOCK_REGISTRY_NAME_CAP];
    char rw_name[CBM_LOCK_REGISTRY_NAME_CAP];
    bool names_ok = cbm_lock_registry_resource_names("cancelled-waiter", turn_name, rw_name);
    uint64_t observe_deadline = cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS;
    while (thread_started && cbm_now_ms() < observe_deadline) {
        if (cbm_lock_registry_waiter_count(fixture.registry) == 1) {
            queued = true;
            cbm_private_file_lock_t *probe = NULL;
            cbm_private_file_lock_status_t probe_status =
                names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, turn_name,
                                                             CBM_PRIVATE_FILE_LOCK_EX, &probe)
                         : CBM_PRIVATE_FILE_LOCK_IO;
            if (probe_status == CBM_PRIVATE_FILE_LOCK_BUSY) {
                writer_has_turn = true;
                break;
            }
            if (probe) {
                (void)cbm_private_file_lock_release(&probe);
            }
        }
        if (atomic_load_explicit(&waiter.finished, memory_order_acquire)) {
            break;
        }
        lock_registry_test_yield();
    }
    if (thread_started) {
        (void)cbm_lock_registry_request_cancel(fixture.registry, &waiter.cancel_token);
        (void)cbm_thread_join(&thread);
    }
    cbm_private_file_lock_t *after_turn = NULL;
    cbm_private_file_lock_status_t after_turn_status =
        names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, turn_name,
                                                     CBM_PRIVATE_FILE_LOCK_EX, &after_turn)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    if (after_turn) {
        (void)cbm_private_file_lock_release(&after_turn);
    }
    cbm_private_file_lock_status_t holder_release =
        holder ? cbm_lock_lease_release(&holder) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_lock_lease_t *after = NULL;
    cbm_private_file_lock_status_t after_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "cancelled-waiter",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &after)
            : CBM_PRIVATE_FILE_LOCK_IO;
    if (after) {
        (void)cbm_lock_lease_release(&after);
    }
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(holder_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(thread_started);
    ASSERT_TRUE(queued);
    ASSERT_TRUE(names_ok);
    ASSERT_TRUE(writer_has_turn);
    ASSERT_EQ(waiter.status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_NULL(waiter.lease);
    ASSERT_EQ(after_turn_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(holder_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(after_status, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(lock_registry_failed_rollback_returns_cleanup_only_lease) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry rollback runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    lock_registry_rollback_fault_t fault;
    atomic_init(&fault.cancel_token, false);
    atomic_init(&fault.native_ready, false);
    bool hook_set = started && cbm_lock_registry_set_stage_hook_for_test(
                                   fixture.registry, lock_registry_cancel_at_native_ready, &fault);
    bool fault_set = hook_set && cbm_lock_registry_fail_next_native_release_step_for_test(
                                     fixture.registry, CBM_LOCK_REGISTRY_RELEASE_RW,
                                     CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK);
    cbm_lock_lease_t *cleanup = NULL;
    cbm_private_file_lock_status_t status =
        fault_set ? cbm_lock_registry_acquire(
                        fixture.registry, "rollback-cleanup", CBM_PRIVATE_FILE_LOCK_EX,
                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, &fault.cancel_token, &cleanup)
                  : CBM_PRIVATE_FILE_LOCK_IO;
    bool reached_native_ready = atomic_load_explicit(&fault.native_ready, memory_order_acquire);
    bool cleanup_retained = cleanup != NULL;

    char turn_name[CBM_LOCK_REGISTRY_NAME_CAP];
    char rw_name[CBM_LOCK_REGISTRY_NAME_CAP];
    bool names_ok = cbm_lock_registry_resource_names("rollback-cleanup", turn_name, rw_name);
    cbm_private_file_lock_t *probe = NULL;
    cbm_private_file_lock_status_t while_cleanup_pending =
        names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, rw_name,
                                                     CBM_PRIVATE_FILE_LOCK_EX, &probe)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    if (probe) {
        (void)cbm_private_file_lock_release(&probe);
    }
    cbm_private_file_lock_status_t free_while_pending =
        started ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    bool registry_preserved = fixture.registry != NULL;
    cbm_private_file_lock_status_t cleanup_release =
        cleanup ? cbm_lock_lease_release(&cleanup) : CBM_PRIVATE_FILE_LOCK_IO;
    if (cleanup) {
        (void)cbm_lock_lease_release(&cleanup);
    }
    cbm_private_file_lock_status_t final_free =
        registry_preserved ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(hook_set);
    ASSERT_TRUE(fault_set);
    ASSERT_TRUE(reached_native_ready);
    ASSERT_EQ(status, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(cleanup_retained);
    ASSERT_TRUE(names_ok);
    ASSERT_EQ(while_cleanup_pending, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(free_while_pending, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_TRUE(registry_preserved);
    ASSERT_EQ(cleanup_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(cleanup);
    ASSERT_EQ(final_free, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

#ifndef _WIN32
static int lock_registry_abort_bookkeeping_failure_retains_cleanup(
    cbm_lock_registry_abort_failure_t failure, const char *resource_key) {
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    lock_registry_rollback_fault_t fault;
    atomic_init(&fault.cancel_token, false);
    atomic_init(&fault.native_ready, false);
    bool hook_set = started && cbm_lock_registry_set_stage_hook_for_test(
                                   fixture.registry, lock_registry_cancel_at_native_ready, &fault);
    bool abort_fault_set = hook_set && cbm_lock_registry_fail_next_abort_bookkeeping_for_test(
                                           fixture.registry, failure);
    bool release_fault_set =
        abort_fault_set &&
        cbm_lock_registry_fail_next_native_release_step_for_test(
            fixture.registry, CBM_LOCK_REGISTRY_RELEASE_RW, CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK);

    cbm_lock_lease_t *cleanup = NULL;
    cbm_private_file_lock_status_t status =
        release_fault_set
            ? cbm_lock_registry_acquire(fixture.registry, resource_key, CBM_PRIVATE_FILE_LOCK_EX,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS,
                                        &fault.cancel_token, &cleanup)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool reached_native_ready = atomic_load_explicit(&fault.native_ready, memory_order_acquire);
    bool cleanup_retained = cleanup != NULL;
    bool owns_rw =
        cbm_lock_lease_has_release_handle_for_test(cleanup, CBM_LOCK_REGISTRY_RELEASE_RW);
    bool owns_turn =
        cbm_lock_lease_has_release_handle_for_test(cleanup, CBM_LOCK_REGISTRY_RELEASE_TURN);
    bool used_exact_lock_failure_path =
        failure != CBM_LOCK_REGISTRY_ABORT_FAIL_LOCK ||
        cbm_lock_lease_used_abort_lock_failure_path_for_test(cleanup);
    size_t waiter_before_release = cbm_lock_registry_waiter_count(fixture.registry);
    size_t pending_before_release =
        cbm_lock_registry_pending_cleanup_count_for_test(fixture.registry);

    char turn_name[CBM_LOCK_REGISTRY_NAME_CAP];
    char rw_name[CBM_LOCK_REGISTRY_NAME_CAP];
    bool names_ok = cbm_lock_registry_resource_names(resource_key, turn_name, rw_name);
    cbm_private_file_lock_t *probe = NULL;
    cbm_private_file_lock_status_t native_before_release =
        names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, rw_name,
                                                     CBM_PRIVATE_FILE_LOCK_EX, &probe)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    if (probe) {
        (void)cbm_private_file_lock_release(&probe);
    }
    cbm_private_file_lock_status_t free_while_waiter =
        started ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    bool registry_preserved = fixture.registry != NULL;

    cbm_private_file_lock_status_t first_cleanup_release =
        cleanup ? cbm_lock_lease_release(&cleanup) : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained_after_native_failure = cleanup != NULL;
    size_t waiter_after_detach = cbm_lock_registry_waiter_count(fixture.registry);
    size_t pending_after_detach =
        cbm_lock_registry_pending_cleanup_count_for_test(fixture.registry);
    probe = NULL;
    cbm_private_file_lock_status_t native_while_pending =
        names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, rw_name,
                                                     CBM_PRIVATE_FILE_LOCK_EX, &probe)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    if (probe) {
        (void)cbm_private_file_lock_release(&probe);
    }
    cbm_private_file_lock_status_t free_while_pending =
        registry_preserved ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;

    cbm_private_file_lock_status_t second_cleanup_release =
        cleanup ? cbm_lock_lease_release(&cleanup) : CBM_PRIVATE_FILE_LOCK_IO;
    size_t waiter_after_cleanup = cbm_lock_registry_waiter_count(fixture.registry);
    size_t pending_after_cleanup =
        cbm_lock_registry_pending_cleanup_count_for_test(fixture.registry);
    cbm_lock_lease_t *after = NULL;
    cbm_private_file_lock_status_t after_status =
        cleanup_retained && fixture.registry
            ? cbm_lock_registry_acquire(fixture.registry, resource_key, CBM_PRIVATE_FILE_LOCK_EX,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &after)
            : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t after_release =
        after ? cbm_lock_lease_release(&after) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t final_free = cleanup_retained && fixture.registry
                                                    ? cbm_lock_registry_free(&fixture.registry)
                                                    : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_TRUE(hook_set);
    ASSERT_TRUE(abort_fault_set);
    ASSERT_TRUE(release_fault_set);
    ASSERT_TRUE(reached_native_ready);
    ASSERT_EQ(status, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(cleanup_retained);
    ASSERT_TRUE(owns_rw);
    ASSERT_TRUE(owns_turn);
    ASSERT_TRUE(used_exact_lock_failure_path);
    ASSERT_EQ(waiter_before_release, 1);
    ASSERT_EQ(pending_before_release, 0);
    ASSERT_TRUE(names_ok);
    ASSERT_EQ(native_before_release, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(free_while_waiter, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_TRUE(registry_preserved);
    ASSERT_EQ(first_cleanup_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained_after_native_failure);
    ASSERT_EQ(waiter_after_detach, 0);
    ASSERT_EQ(pending_after_detach, 1);
    ASSERT_EQ(native_while_pending, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(free_while_pending, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(second_cleanup_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(cleanup);
    ASSERT_EQ(waiter_after_cleanup, 0);
    ASSERT_EQ(pending_after_cleanup, 0);
    ASSERT_EQ(after_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(after_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(final_free, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
}
#endif

TEST(lock_registry_terminal_close_error_finishes_pending_accounting) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX consumed-close registry accounting runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *reader = NULL;
    cbm_private_file_lock_status_t acquired =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "terminal-close-accounting",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &reader)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool retryable_close_set = cbm_lock_lease_fail_next_release_step_for_test(
        reader, CBM_LOCK_REGISTRY_RELEASE_RW, CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE);
    cbm_private_file_lock_status_t first_release =
        retryable_close_set ? cbm_lock_lease_release(&reader) : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained_pending = reader != NULL;
    size_t active_pending = cbm_lock_registry_active_lease_count_for_test(fixture.registry);
    size_t cleanup_pending = cbm_lock_registry_pending_cleanup_count_for_test(fixture.registry);

    bool terminal_close_set =
        cbm_lock_lease_fail_close_after_consuming_for_test(reader, CBM_LOCK_REGISTRY_RELEASE_RW);
    cbm_private_file_lock_status_t terminal_release =
        terminal_close_set ? cbm_lock_lease_release(&reader) : CBM_PRIVATE_FILE_LOCK_IO;
    size_t cleanup_finished = cbm_lock_registry_pending_cleanup_count_for_test(fixture.registry);
    cbm_lock_lease_t *after = NULL;
    cbm_private_file_lock_status_t after_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "terminal-close-accounting",
                                        CBM_PRIVATE_FILE_LOCK_EX,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &after)
            : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t after_release =
        after ? cbm_lock_lease_release(&after) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t final_free =
        started ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(retryable_close_set);
    ASSERT_EQ(first_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained_pending);
    ASSERT_EQ(active_pending, 0);
    ASSERT_EQ(cleanup_pending, 1);
    ASSERT_TRUE(terminal_close_set);
    ASSERT_EQ(terminal_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_NULL(reader);
    ASSERT_EQ(cleanup_finished, 0);
    ASSERT_EQ(after_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(after_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(final_free, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(lock_registry_abort_lock_failure_returns_waiter_cleanup_lease) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry abort cleanup runs on POSIX");
#else
    return lock_registry_abort_bookkeeping_failure_retains_cleanup(
        CBM_LOCK_REGISTRY_ABORT_FAIL_LOCK, "abort-lock-cleanup");
#endif
}

TEST(lock_registry_abort_remove_failure_returns_waiter_cleanup_lease) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry abort cleanup runs on POSIX");
#else
    return lock_registry_abort_bookkeeping_failure_retains_cleanup(
        CBM_LOCK_REGISTRY_ABORT_FAIL_REMOVE, "abort-remove-cleanup");
#endif
}

TEST(lock_registry_never_upgrades_shared_lease_in_place) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry RED runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *reader = NULL;
    cbm_private_file_lock_status_t reader_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "no-upgrade", CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &reader)
            : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_waiter_t writer = {.registry = fixture.registry,
                                     .resource_key = "no-upgrade",
                                     .mode = CBM_PRIVATE_FILE_LOCK_EX,
                                     .status = CBM_PRIVATE_FILE_LOCK_IO};
    atomic_init(&writer.cancel_token, false);
    atomic_init(&writer.finished, false);
    cbm_thread_t writer_thread;
    bool writer_started =
        reader_status == CBM_PRIVATE_FILE_LOCK_OK &&
        cbm_thread_create(&writer_thread, 0, lock_registry_waiter_run, &writer) == 0;
    char turn_name[CBM_LOCK_REGISTRY_NAME_CAP];
    char rw_name[CBM_LOCK_REGISTRY_NAME_CAP];
    bool names_ok = cbm_lock_registry_resource_names("no-upgrade", turn_name, rw_name);
    bool writer_has_turn = false;
    uint64_t deadline = cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS;
    while (writer_started && names_ok && cbm_now_ms() < deadline) {
        cbm_private_file_lock_t *probe = NULL;
        cbm_private_file_lock_status_t probe_status = cbm_private_file_lock_try_acquire(
            fixture.directory, turn_name, CBM_PRIVATE_FILE_LOCK_EX, &probe);
        if (probe_status == CBM_PRIVATE_FILE_LOCK_BUSY) {
            writer_has_turn = true;
            break;
        }
        if (probe) {
            (void)cbm_private_file_lock_release(&probe);
        }
        if (atomic_load_explicit(&writer.finished, memory_order_acquire)) {
            break;
        }
        lock_registry_test_yield();
    }
    bool finished_before_reader_release =
        atomic_load_explicit(&writer.finished, memory_order_acquire);
    cbm_private_file_lock_status_t reader_release =
        reader ? cbm_lock_lease_release(&reader) : CBM_PRIVATE_FILE_LOCK_IO;
    if (writer_started) {
        (void)cbm_thread_join(&writer_thread);
    }
    cbm_private_file_lock_status_t writer_release =
        writer.lease ? cbm_lock_lease_release(&writer.lease) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(reader_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(writer_started);
    ASSERT_TRUE(names_ok);
    ASSERT_TRUE(writer_has_turn);
    ASSERT_FALSE(finished_before_reader_release);
    ASSERT_EQ(reader_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(writer.status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(writer_release, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(lock_registry_reader_close_failure_retains_lease_and_accounting) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry lifecycle runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *reader = NULL;
    cbm_private_file_lock_status_t acquired =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "retry-reader-close",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &reader)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool fault_set = cbm_lock_lease_fail_next_release_step_for_test(
        reader, CBM_LOCK_REGISTRY_RELEASE_RW, CBM_PRIVATE_FILE_LOCK_RELEASE_CLOSE);
    cbm_private_file_lock_status_t first_release =
        fault_set ? cbm_lock_lease_release(&reader) : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained = reader != NULL;
    size_t active_after_failure = cbm_lock_registry_active_lease_count_for_test(fixture.registry);
    size_t pending_after_failure =
        cbm_lock_registry_pending_cleanup_count_for_test(fixture.registry);

    cbm_lock_lease_t *blocked_writer = NULL;
    cbm_private_file_lock_status_t while_close_pending =
        started ? cbm_lock_registry_acquire(fixture.registry, "retry-reader-close",
                                            CBM_PRIVATE_FILE_LOCK_EX, cbm_now_ms() + 50, NULL,
                                            &blocked_writer)
                : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t blocked_writer_release =
        blocked_writer ? cbm_lock_lease_release(&blocked_writer) : CBM_PRIVATE_FILE_LOCK_IO;

    bool duplicate_unlock_fault_set = cbm_lock_lease_fail_next_release_step_for_test(
        reader, CBM_LOCK_REGISTRY_RELEASE_RW, CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK);
    cbm_private_file_lock_status_t retry =
        reader ? cbm_lock_lease_release(&reader) : CBM_PRIVATE_FILE_LOCK_IO;
    if (reader) {
        (void)cbm_lock_lease_release(&reader);
    }
    size_t active_after_retry = cbm_lock_registry_active_lease_count_for_test(fixture.registry);
    cbm_lock_lease_t *after = NULL;
    cbm_private_file_lock_status_t after_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "retry-reader-close",
                                        CBM_PRIVATE_FILE_LOCK_EX,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &after)
            : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t after_release =
        after ? cbm_lock_lease_release(&after) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(fault_set);
    ASSERT_EQ(first_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained);
    ASSERT_EQ(active_after_failure, 0);
    ASSERT_EQ(pending_after_failure, 1);
    ASSERT_EQ(while_close_pending, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(blocked_writer);
    ASSERT_EQ(blocked_writer_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(duplicate_unlock_fault_set);
    ASSERT_EQ(retry, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(reader);
    ASSERT_EQ(active_after_retry, 0);
    ASSERT_EQ(after_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(after_release, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(lock_registry_writer_partial_release_retries_rw_then_turn) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry lifecycle runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *writer = NULL;
    cbm_private_file_lock_status_t acquired =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "retry-writer-turn",
                                        CBM_PRIVATE_FILE_LOCK_EX,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &writer)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool fault_set = cbm_lock_lease_fail_next_release_step_for_test(
        writer, CBM_LOCK_REGISTRY_RELEASE_TURN, CBM_PRIVATE_FILE_LOCK_RELEASE_UNLOCK);
    cbm_private_file_lock_status_t first_release =
        fault_set ? cbm_lock_lease_release(&writer) : CBM_PRIVATE_FILE_LOCK_IO;
    bool retained = writer != NULL;
    bool rw_closed =
        !cbm_lock_lease_has_release_handle_for_test(writer, CBM_LOCK_REGISTRY_RELEASE_RW);
    bool turn_retained =
        cbm_lock_lease_has_release_handle_for_test(writer, CBM_LOCK_REGISTRY_RELEASE_TURN);
    size_t active_after_failure = cbm_lock_registry_active_lease_count_for_test(fixture.registry);
    size_t pending_after_failure =
        cbm_lock_registry_pending_cleanup_count_for_test(fixture.registry);

    char turn_name[CBM_LOCK_REGISTRY_NAME_CAP];
    char rw_name[CBM_LOCK_REGISTRY_NAME_CAP];
    bool names_ok = cbm_lock_registry_resource_names("retry-writer-turn", turn_name, rw_name);
    cbm_private_file_lock_t *rw_probe = NULL;
    cbm_private_file_lock_status_t rw_status =
        names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, rw_name,
                                                     CBM_PRIVATE_FILE_LOCK_EX, &rw_probe)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    if (rw_probe) {
        (void)cbm_private_file_lock_release(&rw_probe);
    }
    cbm_private_file_lock_t *turn_probe = NULL;
    cbm_private_file_lock_status_t turn_status =
        names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, turn_name,
                                                     CBM_PRIVATE_FILE_LOCK_EX, &turn_probe)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    if (turn_probe) {
        (void)cbm_private_file_lock_release(&turn_probe);
    }

    cbm_lock_lease_t *next_writer = NULL;
    cbm_private_file_lock_status_t next_writer_status =
        started ? cbm_lock_registry_acquire(fixture.registry, "retry-writer-turn",
                                            CBM_PRIVATE_FILE_LOCK_EX, cbm_now_ms() + 50, NULL,
                                            &next_writer)
                : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t next_writer_release =
        next_writer ? cbm_lock_lease_release(&next_writer) : CBM_PRIVATE_FILE_LOCK_IO;

    cbm_private_file_lock_status_t retry =
        writer ? cbm_lock_lease_release(&writer) : CBM_PRIVATE_FILE_LOCK_IO;
    if (writer) {
        (void)cbm_lock_lease_release(&writer);
    }
    size_t active_after_retry = cbm_lock_registry_active_lease_count_for_test(fixture.registry);
    cbm_lock_lease_t *after = NULL;
    cbm_private_file_lock_status_t after_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "retry-writer-turn",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &after)
            : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t after_release =
        after ? cbm_lock_lease_release(&after) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(fault_set);
    ASSERT_EQ(first_release, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(retained);
    ASSERT_TRUE(rw_closed);
    ASSERT_TRUE(turn_retained);
    ASSERT_EQ(active_after_failure, 0);
    ASSERT_EQ(pending_after_failure, 1);
    ASSERT_TRUE(names_ok);
    ASSERT_EQ(rw_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(turn_status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(next_writer_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(next_writer_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(retry, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_NULL(writer);
    ASSERT_EQ(active_after_retry, 0);
    ASSERT_EQ(after_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(after_release, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(lock_registry_free_refuses_active_lease) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry lifecycle runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *lease = NULL;
    cbm_private_file_lock_status_t acquired =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "free-active", CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &lease)
            : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t busy =
        started ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    bool registry_preserved = fixture.registry != NULL;
    cbm_private_file_lock_status_t released =
        lease ? cbm_lock_lease_release(&lease) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t freed =
        registry_preserved ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    bool registry_cleared = fixture.registry == NULL;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(acquired, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(busy, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_TRUE(registry_preserved);
    ASSERT_EQ(released, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(freed, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(registry_cleared);
    PASS();
#endif
}

TEST(lock_registry_free_retires_identity_and_rejects_stale_pointer) {
#ifdef _WIN32
    SKIP_PLATFORM("native registry fixture retirement runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_registry_t *stale = fixture.registry;
    cbm_private_file_lock_status_t first_free =
        started ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    bool caller_cleared = fixture.registry == NULL;
    bool retired = cbm_lock_registry_is_retired_for_test(stale);

    cbm_lock_registry_t *fresh = started ? cbm_lock_registry_new(fixture.directory) : NULL;
    bool identity_not_reused = fresh && fresh != stale;
    cbm_lock_lease_t *stale_lease = NULL;
    cbm_private_file_lock_status_t stale_acquire = CBM_PRIVATE_FILE_LOCK_IO;
    cbm_lock_cancel_token_t stale_cancel_token;
    atomic_init(&stale_cancel_token, false);
    cbm_private_file_lock_status_t stale_cancel = CBM_PRIVATE_FILE_LOCK_OK;
    cbm_private_file_lock_status_t stale_free = CBM_PRIVATE_FILE_LOCK_IO;
    bool stale_free_preserved = true;
    if (retired && identity_not_reused) {
        stale_acquire = cbm_lock_registry_acquire(
            stale, "retired-registry", CBM_PRIVATE_FILE_LOCK_SH,
            cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &stale_lease);
        stale_cancel = cbm_lock_registry_request_cancel(stale, &stale_cancel_token);
        cbm_lock_registry_t *stale_copy = stale;
        stale_free = cbm_lock_registry_free(&stale_copy);
        stale_free_preserved = stale_copy == stale;
    }

    cbm_lock_lease_t *fresh_lease = NULL;
    cbm_private_file_lock_status_t fresh_acquire =
        fresh ? cbm_lock_registry_acquire(fresh, "retired-registry", CBM_PRIVATE_FILE_LOCK_SH,
                                          cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL,
                                          &fresh_lease)
              : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t fresh_release =
        fresh_lease ? cbm_lock_lease_release(&fresh_lease) : CBM_PRIVATE_FILE_LOCK_IO;
    fixture.registry = fresh;
    cbm_private_file_lock_status_t fresh_free =
        fresh ? cbm_lock_registry_free(&fixture.registry) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(first_free, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(caller_cleared);
    ASSERT_TRUE(retired);
    ASSERT_TRUE(identity_not_reused);
    ASSERT_EQ(stale_acquire, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_NULL(stale_lease);
    ASSERT_EQ(stale_cancel, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(atomic_load_explicit(&stale_cancel_token, memory_order_acquire));
    ASSERT_EQ(stale_free, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(stale_free_preserved);
    ASSERT_EQ(fresh_acquire, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(fresh_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(fresh_free, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(lock_registry_large_queue_parks_non_head_waiters) {
#ifdef _WIN32
    SKIP_PLATFORM("native registry parking fixture runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *holder = NULL;
    cbm_private_file_lock_status_t holder_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "large-queue-parking",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &holder)
            : CBM_PRIVATE_FILE_LOCK_IO;

    lock_registry_waiter_t waiters[LOCK_REGISTRY_PARKING_WAITERS];
    cbm_thread_t threads[LOCK_REGISTRY_PARKING_WAITERS];
    size_t thread_count = 0;
    memset(waiters, 0, sizeof(waiters));
    for (;
         holder_status == CBM_PRIVATE_FILE_LOCK_OK && thread_count < LOCK_REGISTRY_PARKING_WAITERS;
         thread_count++) {
        waiters[thread_count] = (lock_registry_waiter_t){
            .registry = fixture.registry,
            .resource_key = "large-queue-parking",
            .mode = CBM_PRIVATE_FILE_LOCK_EX,
            .status = CBM_PRIVATE_FILE_LOCK_IO,
        };
        atomic_init(&waiters[thread_count].cancel_token, false);
        atomic_init(&waiters[thread_count].finished, false);
        if (cbm_thread_create(&threads[thread_count], 0, lock_registry_waiter_run,
                              &waiters[thread_count]) != 0) {
            break;
        }
    }

    bool all_queued = false;
    bool tails_parked = false;
    uint64_t observe_deadline = cbm_now_ms() + 500;
    while (thread_count == LOCK_REGISTRY_PARKING_WAITERS && cbm_now_ms() < observe_deadline) {
        all_queued =
            cbm_lock_registry_waiter_count(fixture.registry) == LOCK_REGISTRY_PARKING_WAITERS;
        tails_parked = cbm_lock_registry_condition_waiter_count_for_test(fixture.registry) >=
                       LOCK_REGISTRY_PARKING_WAITERS - 1;
        if (all_queued && tails_parked) {
            break;
        }
        lock_registry_test_yield();
    }
    size_t attempting = cbm_lock_registry_attempting_waiter_count_for_test(fixture.registry);
    uint64_t waits_before = cbm_lock_registry_condition_wait_call_count_for_test(fixture.registry);
    cbm_usleep(25000);
    uint64_t waits_after = cbm_lock_registry_condition_wait_call_count_for_test(fixture.registry);

    for (size_t index = 0; index < thread_count; index++) {
        (void)cbm_lock_registry_request_cancel(fixture.registry, &waiters[index].cancel_token);
    }
    bool all_cancelled = true;
    for (size_t index = 0; index < thread_count; index++) {
        all_cancelled = cbm_thread_join(&threads[index]) == 0 &&
                        waiters[index].status == CBM_PRIVATE_FILE_LOCK_BUSY && all_cancelled;
        if (waiters[index].lease) {
            (void)cbm_lock_lease_release(&waiters[index].lease);
        }
    }
    cbm_private_file_lock_status_t holder_release =
        holder ? cbm_lock_lease_release(&holder) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(holder_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(thread_count, LOCK_REGISTRY_PARKING_WAITERS);
    ASSERT_TRUE(all_queued);
    ASSERT_TRUE(tails_parked);
    ASSERT_EQ(attempting, 1);
    ASSERT_GTE(waits_before, LOCK_REGISTRY_PARKING_WAITERS - 1);
    ASSERT_TRUE(waits_after - waits_before < 128);
    ASSERT_TRUE(all_cancelled);
    ASSERT_EQ(holder_release, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

TEST(lock_registry_cancel_request_wakes_parked_tail) {
#ifdef _WIN32
    SKIP_PLATFORM("native registry cancellation fixture runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *holder = NULL;
    cbm_private_file_lock_status_t holder_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "cancel-parked-tail",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &holder)
            : CBM_PRIVATE_FILE_LOCK_IO;

    lock_registry_waiter_t head = {.registry = fixture.registry,
                                   .resource_key = "cancel-parked-tail",
                                   .mode = CBM_PRIVATE_FILE_LOCK_EX,
                                   .status = CBM_PRIVATE_FILE_LOCK_IO};
    atomic_init(&head.cancel_token, false);
    atomic_init(&head.finished, false);
    cbm_thread_t head_thread;
    bool head_started = holder_status == CBM_PRIVATE_FILE_LOCK_OK &&
                        cbm_thread_create(&head_thread, 0, lock_registry_waiter_run, &head) == 0;
    bool head_attempting = false;
    uint64_t head_deadline = cbm_now_ms() + 500;
    while (head_started && cbm_now_ms() < head_deadline) {
        head_attempting = cbm_lock_registry_waiter_count(fixture.registry) == 1 &&
                          cbm_lock_registry_attempting_waiter_count_for_test(fixture.registry) == 1;
        if (head_attempting) {
            break;
        }
        lock_registry_test_yield();
    }

    lock_registry_waiter_t tail = {.registry = fixture.registry,
                                   .resource_key = "cancel-parked-tail",
                                   .mode = CBM_PRIVATE_FILE_LOCK_EX,
                                   .status = CBM_PRIVATE_FILE_LOCK_IO};
    atomic_init(&tail.cancel_token, false);
    atomic_init(&tail.finished, false);
    cbm_thread_t tail_thread;
    bool tail_started =
        head_attempting && cbm_thread_create(&tail_thread, 0, lock_registry_waiter_run, &tail) == 0;
    bool tail_parked = false;
    uint64_t tail_deadline = cbm_now_ms() + 500;
    while (tail_started && cbm_now_ms() < tail_deadline) {
        tail_parked = cbm_lock_registry_waiter_count(fixture.registry) == 2 &&
                      cbm_lock_registry_attempting_waiter_count_for_test(fixture.registry) == 1 &&
                      cbm_lock_registry_condition_waiter_count_for_test(fixture.registry) >= 1;
        if (tail_parked) {
            break;
        }
        lock_registry_test_yield();
    }

    cbm_private_file_lock_status_t cancel_status =
        tail_parked ? cbm_lock_registry_request_cancel(fixture.registry, &tail.cancel_token)
                    : CBM_PRIVATE_FILE_LOCK_IO;
    bool tail_woke = false;
    uint64_t wake_deadline = cbm_now_ms() + 500;
    while (tail_started && cbm_now_ms() < wake_deadline) {
        tail_woke = atomic_load_explicit(&tail.finished, memory_order_acquire);
        if (tail_woke) {
            break;
        }
        lock_registry_test_yield();
    }
    bool head_still_waiting =
        head_started && !atomic_load_explicit(&head.finished, memory_order_acquire);

    if (head_started) {
        (void)cbm_lock_registry_request_cancel(fixture.registry, &head.cancel_token);
    }
    cbm_private_file_lock_status_t holder_release =
        holder ? cbm_lock_lease_release(&holder) : CBM_PRIVATE_FILE_LOCK_IO;
    if (tail_started) {
        (void)cbm_thread_join(&tail_thread);
    }
    if (head_started) {
        (void)cbm_thread_join(&head_thread);
    }
    size_t remaining_waiters =
        started ? cbm_lock_registry_waiter_count(fixture.registry) : (size_t)-1;
    if (tail.lease) {
        (void)cbm_lock_lease_release(&tail.lease);
    }
    if (head.lease) {
        (void)cbm_lock_lease_release(&head.lease);
    }
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(holder_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(head_started);
    ASSERT_TRUE(head_attempting);
    ASSERT_TRUE(tail_started);
    ASSERT_TRUE(tail_parked);
    ASSERT_EQ(cancel_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(tail_woke);
    ASSERT_TRUE(head_still_waiting);
    ASSERT_EQ(tail.status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_NULL(tail.lease);
    ASSERT_EQ(head.status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_NULL(head.lease);
    ASSERT_EQ(holder_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(remaining_waiters, 0);
    PASS();
#endif
}

typedef struct {
    cbm_lock_registry_t *registry;
    cbm_lock_cancel_token_t cancel_token;
    atomic_bool ready;
    atomic_bool go;
    atomic_bool finished;
    uint64_t deadline_ms;
    uint64_t returned_ms;
    cbm_private_file_lock_status_t status;
    cbm_lock_lease_t *lease;
} lock_registry_deadline_waiter_t;

#ifndef _WIN32
static void *lock_registry_deadline_waiter_run(void *opaque) {
    lock_registry_deadline_waiter_t *waiter = opaque;
    atomic_store_explicit(&waiter->ready, true, memory_order_release);
    while (!atomic_load_explicit(&waiter->go, memory_order_acquire)) {
        lock_registry_test_yield();
    }
    waiter->status =
        cbm_lock_registry_acquire(waiter->registry, "absolute-deadline", CBM_PRIVATE_FILE_LOCK_EX,
                                  waiter->deadline_ms, &waiter->cancel_token, &waiter->lease);
    waiter->returned_ms = cbm_now_ms();
    atomic_store_explicit(&waiter->finished, true, memory_order_release);
    return NULL;
}
#endif

TEST(lock_registry_absolute_deadline_survives_repeated_wakes) {
#ifdef _WIN32
    SKIP_PLATFORM("native registry deadline fixture runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *holder = NULL;
    cbm_private_file_lock_status_t holder_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "absolute-deadline",
                                        CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &holder)
            : CBM_PRIVATE_FILE_LOCK_IO;

    lock_registry_waiter_t head = {.registry = fixture.registry,
                                   .resource_key = "absolute-deadline",
                                   .mode = CBM_PRIVATE_FILE_LOCK_EX,
                                   .status = CBM_PRIVATE_FILE_LOCK_IO};
    atomic_init(&head.cancel_token, false);
    atomic_init(&head.finished, false);
    cbm_thread_t head_thread;
    bool head_started = holder_status == CBM_PRIVATE_FILE_LOCK_OK &&
                        cbm_thread_create(&head_thread, 0, lock_registry_waiter_run, &head) == 0;
    bool head_attempting = false;
    uint64_t head_deadline = cbm_now_ms() + 500;
    while (head_started && cbm_now_ms() < head_deadline) {
        head_attempting = cbm_lock_registry_waiter_count(fixture.registry) == 1 &&
                          cbm_lock_registry_attempting_waiter_count_for_test(fixture.registry) == 1;
        if (head_attempting) {
            break;
        }
        lock_registry_test_yield();
    }

    lock_registry_deadline_waiter_t tail = {.registry = fixture.registry,
                                            .status = CBM_PRIVATE_FILE_LOCK_IO};
    atomic_init(&tail.cancel_token, false);
    atomic_init(&tail.ready, false);
    atomic_init(&tail.go, false);
    atomic_init(&tail.finished, false);
    cbm_thread_t tail_thread;
    bool tail_started =
        head_attempting &&
        cbm_thread_create(&tail_thread, 0, lock_registry_deadline_waiter_run, &tail) == 0;
    uint64_t ready_deadline = cbm_now_ms() + 500;
    while (tail_started && !atomic_load_explicit(&tail.ready, memory_order_acquire) &&
           cbm_now_ms() < ready_deadline) {
        lock_registry_test_yield();
    }
    bool tail_ready = tail_started && atomic_load_explicit(&tail.ready, memory_order_acquire);
    uint64_t deadline_start = cbm_now_ms();
    tail.deadline_ms = deadline_start + 200;
    atomic_store_explicit(&tail.go, true, memory_order_release);

    bool tail_queued = false;
    uint64_t queue_deadline = deadline_start + 100;
    while (tail_ready && cbm_now_ms() < queue_deadline) {
        tail_queued = cbm_lock_registry_waiter_count(fixture.registry) == 2 &&
                      cbm_lock_registry_attempting_waiter_count_for_test(fixture.registry) == 1;
        if (tail_queued) {
            break;
        }
        lock_registry_test_yield();
    }

    cbm_lock_cancel_token_t unrelated_token;
    atomic_init(&unrelated_token, false);
    bool broadcasts_ok = true;
    uint64_t observe_deadline = deadline_start + 600;
    while (tail_ready && !atomic_load_explicit(&tail.finished, memory_order_acquire) &&
           cbm_now_ms() < observe_deadline) {
        broadcasts_ok = cbm_lock_registry_request_cancel(fixture.registry, &unrelated_token) ==
                            CBM_PRIVATE_FILE_LOCK_OK &&
                        broadcasts_ok;
        cbm_usleep(5000);
    }
    bool returned_at_deadline =
        tail_ready && atomic_load_explicit(&tail.finished, memory_order_acquire);
    uint64_t elapsed_ms = returned_at_deadline ? tail.returned_ms - deadline_start : UINT64_MAX;

    if (!returned_at_deadline && tail_started) {
        (void)cbm_lock_registry_request_cancel(fixture.registry, &tail.cancel_token);
    }
    if (head_started) {
        (void)cbm_lock_registry_request_cancel(fixture.registry, &head.cancel_token);
    }
    cbm_private_file_lock_status_t holder_release =
        holder ? cbm_lock_lease_release(&holder) : CBM_PRIVATE_FILE_LOCK_IO;
    if (tail_started) {
        (void)cbm_thread_join(&tail_thread);
    }
    if (head_started) {
        (void)cbm_thread_join(&head_thread);
    }
    if (tail.lease) {
        (void)cbm_lock_lease_release(&tail.lease);
    }
    if (head.lease) {
        (void)cbm_lock_lease_release(&head.lease);
    }
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(holder_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(head_started);
    ASSERT_TRUE(head_attempting);
    ASSERT_TRUE(tail_started);
    ASSERT_TRUE(tail_ready);
    ASSERT_TRUE(tail_queued);
    ASSERT_TRUE(broadcasts_ok);
    ASSERT_TRUE(returned_at_deadline);
    ASSERT_GTE(elapsed_ms, 150);
    ASSERT_TRUE(elapsed_ms < 350);
    ASSERT_EQ(tail.status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_NULL(tail.lease);
    ASSERT_EQ(head.status, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_NULL(head.lease);
    ASSERT_EQ(holder_release, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

typedef struct {
    cbm_lock_registry_t *registry;
    unsigned int id;
    atomic_int *ready;
    atomic_bool *go;
    atomic_int *readers;
    atomic_int *writers;
    atomic_int *violations;
} lock_registry_stress_worker_t;

#ifndef _WIN32
static void *lock_registry_stress_run(void *opaque) {
    lock_registry_stress_worker_t *worker = opaque;
    (void)atomic_fetch_add_explicit(worker->ready, 1, memory_order_acq_rel);
    while (!atomic_load_explicit(worker->go, memory_order_acquire)) {
        lock_registry_test_yield();
    }
    for (unsigned int iteration = 0; iteration < LOCK_REGISTRY_STRESS_ITERATIONS; iteration++) {
        cbm_private_file_lock_mode_t mode = (iteration + worker->id) % 5U == 0U
                                                ? CBM_PRIVATE_FILE_LOCK_EX
                                                : CBM_PRIVATE_FILE_LOCK_SH;
        cbm_lock_lease_t *lease = NULL;
        cbm_private_file_lock_status_t status =
            cbm_lock_registry_acquire(worker->registry, "stress", mode,
                                      cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &lease);
        if (status != CBM_PRIVATE_FILE_LOCK_OK || !lease) {
            (void)atomic_fetch_add_explicit(worker->violations, 1, memory_order_relaxed);
            return NULL;
        }
        if (mode == CBM_PRIVATE_FILE_LOCK_EX) {
            if (atomic_fetch_add_explicit(worker->writers, 1, memory_order_acq_rel) != 0 ||
                atomic_load_explicit(worker->readers, memory_order_acquire) != 0) {
                (void)atomic_fetch_add_explicit(worker->violations, 1, memory_order_relaxed);
            }
            lock_registry_test_yield();
            if (atomic_load_explicit(worker->readers, memory_order_acquire) != 0) {
                (void)atomic_fetch_add_explicit(worker->violations, 1, memory_order_relaxed);
            }
            (void)atomic_fetch_sub_explicit(worker->writers, 1, memory_order_acq_rel);
        } else {
            if (atomic_load_explicit(worker->writers, memory_order_acquire) != 0) {
                (void)atomic_fetch_add_explicit(worker->violations, 1, memory_order_relaxed);
            }
            (void)atomic_fetch_add_explicit(worker->readers, 1, memory_order_acq_rel);
            if (atomic_load_explicit(worker->writers, memory_order_acquire) != 0) {
                (void)atomic_fetch_add_explicit(worker->violations, 1, memory_order_relaxed);
            }
            lock_registry_test_yield();
            (void)atomic_fetch_sub_explicit(worker->readers, 1, memory_order_acq_rel);
        }
        if (cbm_lock_lease_release(&lease) != CBM_PRIVATE_FILE_LOCK_OK) {
            (void)atomic_fetch_add_explicit(worker->violations, 1, memory_order_relaxed);
            return NULL;
        }
    }
    return NULL;
}
#endif

TEST(lock_registry_concurrent_shared_exclusive_stress) {
#ifdef _WIN32
    SKIP_PLATFORM("POSIX native-directory registry RED runs on POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_thread_t threads[LOCK_REGISTRY_STRESS_THREADS];
    lock_registry_stress_worker_t workers[LOCK_REGISTRY_STRESS_THREADS];
    atomic_int ready;
    atomic_bool go;
    atomic_int readers;
    atomic_int writers;
    atomic_int violations;
    atomic_init(&ready, 0);
    atomic_init(&go, false);
    atomic_init(&readers, 0);
    atomic_init(&writers, 0);
    atomic_init(&violations, 0);
    size_t created = 0;
    for (; started && created < LOCK_REGISTRY_STRESS_THREADS; created++) {
        workers[created] = (lock_registry_stress_worker_t){
            .registry = fixture.registry,
            .id = (unsigned int)created,
            .ready = &ready,
            .go = &go,
            .readers = &readers,
            .writers = &writers,
            .violations = &violations,
        };
        if (cbm_thread_create(&threads[created], 0, lock_registry_stress_run, &workers[created]) !=
            0) {
            break;
        }
    }
    uint64_t ready_deadline = cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS;
    while (created == LOCK_REGISTRY_STRESS_THREADS &&
           atomic_load_explicit(&ready, memory_order_acquire) != LOCK_REGISTRY_STRESS_THREADS &&
           cbm_now_ms() < ready_deadline) {
        lock_registry_test_yield();
    }
    bool all_ready =
        atomic_load_explicit(&ready, memory_order_acquire) == LOCK_REGISTRY_STRESS_THREADS;
    atomic_store_explicit(&go, true, memory_order_release);
    for (size_t index = 0; index < created; index++) {
        (void)cbm_thread_join(&threads[index]);
    }
    int violation_count = atomic_load_explicit(&violations, memory_order_acquire);
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(created, LOCK_REGISTRY_STRESS_THREADS);
    ASSERT_TRUE(all_ready);
    ASSERT_EQ(violation_count, 0);
    PASS();
#endif
}

#ifndef _WIN32
static bool lock_registry_pipe_write(int fd, char value) {
    ssize_t written;
    do {
        written = write(fd, &value, 1);
    } while (written < 0 && errno == EINTR);
    return written == 1;
}

static bool lock_registry_pipe_read(int fd, uint32_t timeout_ms, char *value_out) {
    struct pollfd descriptor = {.fd = fd, .events = POLLIN, .revents = 0};
    int ready;
    do {
        ready = poll(&descriptor, 1, (int)timeout_ms);
    } while (ready < 0 && errno == EINTR);
    if (ready != 1 || (descriptor.revents & POLLIN) == 0) {
        return false;
    }
    ssize_t received;
    do {
        received = read(fd, value_out, 1);
    } while (received < 0 && errno == EINTR);
    return received == 1;
}

static bool lock_registry_child_wait(pid_t child, int *status_out) {
    uint64_t deadline = cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS;
    while (cbm_now_ms() < deadline) {
        pid_t result = waitpid(child, status_out, WNOHANG);
        if (result == child) {
            return true;
        }
        if (result < 0 && errno != EINTR) {
            return false;
        }
        lock_registry_test_yield();
    }
    (void)kill(child, SIGKILL);
    return waitpid(child, status_out, 0) == child && false;
}

static int lock_registry_writer_child(const char *root, int started_fd, int acquired_fd,
                                      int release_fd) {
    cbm_private_lock_directory_t *directory = lock_registry_test_directory_open(root);
    cbm_lock_registry_t *registry = cbm_lock_registry_new(directory);
    bool started = registry && lock_registry_pipe_write(started_fd, 'S');
    cbm_lock_lease_t *lease = NULL;
    cbm_private_file_lock_status_t status =
        started
            ? cbm_lock_registry_acquire(registry, "writer-preference", CBM_PRIVATE_FILE_LOCK_EX,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &lease)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool reported =
        lock_registry_pipe_write(acquired_fd, status == CBM_PRIVATE_FILE_LOCK_OK ? 'W' : 'E');
    char command = 0;
    bool released = status == CBM_PRIVATE_FILE_LOCK_OK &&
                    lock_registry_pipe_read(release_fd, LOCK_REGISTRY_TEST_TIMEOUT_MS, &command) &&
                    command == 'X' && cbm_lock_lease_release(&lease) == CBM_PRIVATE_FILE_LOCK_OK;
    cbm_private_file_lock_status_t freed = cbm_lock_registry_free(&registry);
    cbm_private_lock_directory_close(directory);
    return started && reported && released && freed == CBM_PRIVATE_FILE_LOCK_OK ? 0 : 1;
}

typedef struct {
    int fd;
    bool reported;
} lock_registry_stage_pipe_t;

static void lock_registry_stage_pipe_report(void *opaque, cbm_private_file_lock_mode_t mode,
                                            cbm_lock_registry_stage_t stage) {
    lock_registry_stage_pipe_t *stage_pipe = opaque;
    if (!stage_pipe->reported && mode == CBM_PRIVATE_FILE_LOCK_SH &&
        stage == CBM_LOCK_REGISTRY_STAGE_TURN_BUSY) {
        stage_pipe->reported = lock_registry_pipe_write(stage_pipe->fd, 'T');
    }
}

static int lock_registry_reader_child(const char *root, int acquired_fd, int attempted_fd) {
    cbm_private_lock_directory_t *directory = lock_registry_test_directory_open(root);
    cbm_lock_registry_t *registry = cbm_lock_registry_new(directory);
    lock_registry_stage_pipe_t stage_pipe = {.fd = attempted_fd};
    bool hook_set = registry && cbm_lock_registry_set_stage_hook_for_test(
                                    registry, lock_registry_stage_pipe_report, &stage_pipe);
    cbm_lock_lease_t *lease = NULL;
    cbm_private_file_lock_status_t status =
        hook_set
            ? cbm_lock_registry_acquire(registry, "writer-preference", CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &lease)
            : CBM_PRIVATE_FILE_LOCK_IO;
    bool reported =
        lock_registry_pipe_write(acquired_fd, status == CBM_PRIVATE_FILE_LOCK_OK ? 'R' : 'E');
    bool released = status == CBM_PRIVATE_FILE_LOCK_OK &&
                    cbm_lock_lease_release(&lease) == CBM_PRIVATE_FILE_LOCK_OK;
    cbm_private_file_lock_status_t freed = cbm_lock_registry_free(&registry);
    cbm_private_lock_directory_close(directory);
    return hook_set && stage_pipe.reported && reported && released &&
                   freed == CBM_PRIVATE_FILE_LOCK_OK
               ? 0
               : 1;
}

static int lock_registry_inherited_child(cbm_lock_registry_t *registry,
                                         cbm_lock_lease_t *inherited_lease, int report_fd) {
    cbm_lock_lease_t *new_lease = NULL;
    cbm_private_file_lock_status_t acquire_status =
        cbm_lock_registry_acquire(registry, "fork-registry", CBM_PRIVATE_FILE_LOCK_EX,
                                  cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &new_lease);
    cbm_private_file_lock_status_t release_status = cbm_lock_lease_release(&inherited_lease);
    cbm_private_file_lock_status_t free_status = cbm_lock_registry_free(&registry);
    bool reported = lock_registry_pipe_write(report_fd, (char)acquire_status) &&
                    lock_registry_pipe_write(report_fd, (char)release_status) &&
                    lock_registry_pipe_write(report_fd, (char)free_status);
    if (new_lease) {
        (void)cbm_lock_lease_release(&new_lease);
    }
    return reported ? 0 : 1;
}
#endif

TEST(lock_registry_cross_process_writer_beats_late_reader) {
#ifdef _WIN32
    SKIP_PLATFORM("fork/pipe writer-preference proof applies only to POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *initial_reader = NULL;
    cbm_private_file_lock_status_t initial_status =
        started ? cbm_lock_registry_acquire(
                      fixture.registry, "writer-preference", CBM_PRIVATE_FILE_LOCK_SH,
                      cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &initial_reader)
                : CBM_PRIVATE_FILE_LOCK_IO;
    int writer_started[2] = {-1, -1};
    int writer_acquired[2] = {-1, -1};
    int writer_release[2] = {-1, -1};
    bool writer_pipes = initial_status == CBM_PRIVATE_FILE_LOCK_OK && pipe(writer_started) == 0 &&
                        pipe(writer_acquired) == 0 && pipe(writer_release) == 0;
    pid_t writer = writer_pipes ? fork() : -1;
    if (writer == 0) {
        (void)close(writer_started[0]);
        (void)close(writer_acquired[0]);
        (void)close(writer_release[1]);
        int child_result = lock_registry_writer_child(fixture.root, writer_started[1],
                                                      writer_acquired[1], writer_release[0]);
        _exit(child_result);
    }

    char report = 0;
    bool writer_announced = false;
    bool writer_has_turn = false;
    char turn_name[CBM_LOCK_REGISTRY_NAME_CAP];
    char rw_name[CBM_LOCK_REGISTRY_NAME_CAP];
    bool names_ok = cbm_lock_registry_resource_names("writer-preference", turn_name, rw_name);
    if (writer > 0) {
        (void)close(writer_started[1]);
        (void)close(writer_acquired[1]);
        (void)close(writer_release[0]);
        writer_announced =
            lock_registry_pipe_read(writer_started[0], LOCK_REGISTRY_TEST_TIMEOUT_MS, &report) &&
            report == 'S';
        uint64_t deadline = cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS;
        while (writer_announced && names_ok && cbm_now_ms() < deadline) {
            cbm_private_file_lock_t *probe = NULL;
            cbm_private_file_lock_status_t probe_status = cbm_private_file_lock_try_acquire(
                fixture.directory, turn_name, CBM_PRIVATE_FILE_LOCK_EX, &probe);
            if (probe_status == CBM_PRIVATE_FILE_LOCK_BUSY) {
                writer_has_turn = true;
                break;
            }
            if (probe) {
                (void)cbm_private_file_lock_release(&probe);
            }
            if (probe_status != CBM_PRIVATE_FILE_LOCK_OK) {
                break;
            }
            lock_registry_test_yield();
        }
    }

    int late_attempted[2] = {-1, -1};
    int late_acquired[2] = {-1, -1};
    bool late_pipe = writer_has_turn && pipe(late_attempted) == 0 && pipe(late_acquired) == 0;
    pid_t late_reader = late_pipe ? fork() : -1;
    if (late_reader == 0) {
        (void)close(late_attempted[0]);
        (void)close(late_acquired[0]);
        (void)close(writer_started[0]);
        (void)close(writer_acquired[0]);
        (void)close(writer_release[1]);
        _exit(lock_registry_reader_child(fixture.root, late_acquired[1], late_attempted[1]));
    }
    if (late_reader > 0) {
        (void)close(late_attempted[1]);
        (void)close(late_acquired[1]);
    }

    char attempted_report = 0;
    bool late_reached_turn =
        late_reader > 0 &&
        lock_registry_pipe_read(late_attempted[0], LOCK_REGISTRY_TEST_TIMEOUT_MS,
                                &attempted_report) &&
        attempted_report == 'T';

    /* All forks are complete before introducing a second parent thread. */
    lock_registry_waiter_t local_reader = {
        .registry = fixture.registry,
        .resource_key = "writer-preference",
        .mode = CBM_PRIVATE_FILE_LOCK_SH,
        .status = CBM_PRIVATE_FILE_LOCK_IO,
    };
    atomic_init(&local_reader.cancel_token, false);
    atomic_init(&local_reader.finished, false);
    cbm_thread_t local_reader_thread;
    bool local_reader_started =
        late_reached_turn &&
        cbm_thread_create(&local_reader_thread, 0, lock_registry_waiter_run, &local_reader) == 0;
    bool local_reader_queued = false;
    uint64_t local_deadline = cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS;
    while (local_reader_started && cbm_now_ms() < local_deadline) {
        if (cbm_lock_registry_waiter_count(fixture.registry) == 1) {
            local_reader_queued = true;
            break;
        }
        if (atomic_load_explicit(&local_reader.finished, memory_order_acquire)) {
            break;
        }
        lock_registry_test_yield();
    }

    cbm_private_file_lock_status_t initial_release =
        initial_reader ? cbm_lock_lease_release(&initial_reader) : CBM_PRIVATE_FILE_LOCK_IO;
    char writer_report = 0;
    bool writer_won = writer > 0 &&
                      lock_registry_pipe_read(writer_acquired[0], LOCK_REGISTRY_TEST_TIMEOUT_MS,
                                              &writer_report) &&
                      writer_report == 'W';
    struct pollfd late_probe = {
        .fd = late_reader > 0 ? late_acquired[0] : -1,
        .events = POLLIN,
        .revents = 0,
    };
    int late_ready_while_writer = late_reader > 0 ? poll(&late_probe, 1, 0) : -1;
    bool local_ready_while_writer =
        atomic_load_explicit(&local_reader.finished, memory_order_acquire);
    bool writer_commanded = writer_won && lock_registry_pipe_write(writer_release[1], 'X');
    int writer_status = -1;
    bool writer_exited = writer > 0 && lock_registry_child_wait(writer, &writer_status);
    if (local_reader_started) {
        (void)cbm_thread_join(&local_reader_thread);
    }
    cbm_private_file_lock_status_t local_reader_release =
        local_reader.lease ? cbm_lock_lease_release(&local_reader.lease) : CBM_PRIVATE_FILE_LOCK_IO;
    char late_report = 0;
    bool late_followed =
        late_reader > 0 &&
        lock_registry_pipe_read(late_acquired[0], LOCK_REGISTRY_TEST_TIMEOUT_MS, &late_report) &&
        late_report == 'R';
    int late_status = -1;
    bool late_exited = late_reader > 0 && lock_registry_child_wait(late_reader, &late_status);
    if (writer > 0) {
        (void)close(writer_started[0]);
        (void)close(writer_acquired[0]);
        (void)close(writer_release[1]);
    }
    if (late_reader > 0) {
        (void)close(late_attempted[0]);
        (void)close(late_acquired[0]);
    }
    if (initial_reader) {
        (void)cbm_lock_lease_release(&initial_reader);
    }
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(initial_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(writer_pipes);
    ASSERT_GT(writer, 0);
    ASSERT_TRUE(writer_announced);
    ASSERT_TRUE(names_ok);
    ASSERT_TRUE(writer_has_turn);
    ASSERT_TRUE(local_reader_started);
    ASSERT_TRUE(local_reader_queued);
    ASSERT_TRUE(late_pipe);
    ASSERT_GT(late_reader, 0);
    ASSERT_TRUE(late_reached_turn);
    ASSERT_EQ(initial_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(writer_won);
    ASSERT_EQ(late_ready_while_writer, 0);
    ASSERT_FALSE(local_ready_while_writer);
    ASSERT_TRUE(writer_commanded);
    ASSERT_TRUE(writer_exited);
    ASSERT_TRUE(WIFEXITED(writer_status));
    ASSERT_EQ(WEXITSTATUS(writer_status), 0);
    ASSERT_EQ(local_reader.status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(local_reader_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(late_followed);
    ASSERT_TRUE(late_exited);
    ASSERT_TRUE(WIFEXITED(late_status));
    ASSERT_EQ(WEXITSTATUS(late_status), 0);
    PASS();
#endif
}

TEST(lock_registry_fork_child_rejects_inherited_registry) {
#ifdef _WIN32
    SKIP_PLATFORM("fork inheritance applies only to POSIX");
#else
    lock_registry_fixture_t fixture;
    bool started = lock_registry_fixture_start(&fixture);
    cbm_lock_lease_t *reader = NULL;
    cbm_private_file_lock_status_t reader_status =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "fork-registry", CBM_PRIVATE_FILE_LOCK_SH,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &reader)
            : CBM_PRIVATE_FILE_LOCK_IO;
    int reports[2] = {-1, -1};
    bool pipe_ok = reader_status == CBM_PRIVATE_FILE_LOCK_OK && pipe(reports) == 0;
    pid_t child = pipe_ok ? fork() : -1;
    if (child == 0) {
        (void)close(reports[0]);
        int result = lock_registry_inherited_child(fixture.registry, reader, reports[1]);
        _exit(result);
    }
    if (child > 0) {
        (void)close(reports[1]);
    }

    char acquire_report = 0;
    char release_report = 0;
    char free_report = 0;
    bool reports_ok =
        child > 0 &&
        lock_registry_pipe_read(reports[0], LOCK_REGISTRY_TEST_TIMEOUT_MS, &acquire_report) &&
        lock_registry_pipe_read(reports[0], LOCK_REGISTRY_TEST_TIMEOUT_MS, &release_report) &&
        lock_registry_pipe_read(reports[0], LOCK_REGISTRY_TEST_TIMEOUT_MS, &free_report);
    int child_status = -1;
    bool child_exited = child > 0 && lock_registry_child_wait(child, &child_status);
    if (child > 0) {
        (void)close(reports[0]);
    }

    char turn_name[CBM_LOCK_REGISTRY_NAME_CAP];
    char rw_name[CBM_LOCK_REGISTRY_NAME_CAP];
    bool names_ok = cbm_lock_registry_resource_names("fork-registry", turn_name, rw_name);
    cbm_private_file_lock_t *probe = NULL;
    cbm_private_file_lock_status_t parent_still_locked =
        names_ok ? cbm_private_file_lock_try_acquire(fixture.directory, rw_name,
                                                     CBM_PRIVATE_FILE_LOCK_EX, &probe)
                 : CBM_PRIVATE_FILE_LOCK_IO;
    if (probe) {
        (void)cbm_private_file_lock_release(&probe);
    }
    cbm_private_file_lock_status_t reader_release =
        reader ? cbm_lock_lease_release(&reader) : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_lock_lease_t *writer = NULL;
    cbm_private_file_lock_status_t writer_after =
        started
            ? cbm_lock_registry_acquire(fixture.registry, "fork-registry", CBM_PRIVATE_FILE_LOCK_EX,
                                        cbm_now_ms() + LOCK_REGISTRY_TEST_TIMEOUT_MS, NULL, &writer)
            : CBM_PRIVATE_FILE_LOCK_IO;
    cbm_private_file_lock_status_t writer_release =
        writer ? cbm_lock_lease_release(&writer) : CBM_PRIVATE_FILE_LOCK_IO;
    lock_registry_fixture_finish(&fixture);

    ASSERT_TRUE(started);
    ASSERT_EQ(reader_status, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_TRUE(pipe_ok);
    ASSERT_GT(child, 0);
    ASSERT_TRUE(reports_ok);
    ASSERT_EQ((unsigned char)acquire_report, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_EQ((unsigned char)release_report, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ((unsigned char)free_report, CBM_PRIVATE_FILE_LOCK_IO);
    ASSERT_TRUE(child_exited);
    ASSERT_TRUE(WIFEXITED(child_status));
    ASSERT_EQ(WEXITSTATUS(child_status), 0);
    ASSERT_TRUE(names_ok);
    ASSERT_EQ(parent_still_locked, CBM_PRIVATE_FILE_LOCK_BUSY);
    ASSERT_EQ(reader_release, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(writer_after, CBM_PRIVATE_FILE_LOCK_OK);
    ASSERT_EQ(writer_release, CBM_PRIVATE_FILE_LOCK_OK);
    PASS();
#endif
}

SUITE(lock_registry) {
    RUN_TEST(lock_registry_cancelled_wait_rolls_back_and_does_not_barge);
    RUN_TEST(lock_registry_failed_rollback_returns_cleanup_only_lease);
    RUN_TEST(lock_registry_abort_lock_failure_returns_waiter_cleanup_lease);
    RUN_TEST(lock_registry_abort_remove_failure_returns_waiter_cleanup_lease);
    RUN_TEST(lock_registry_never_upgrades_shared_lease_in_place);
    RUN_TEST(lock_registry_reader_close_failure_retains_lease_and_accounting);
    RUN_TEST(lock_registry_writer_partial_release_retries_rw_then_turn);
    RUN_TEST(lock_registry_terminal_close_error_finishes_pending_accounting);
    RUN_TEST(lock_registry_free_refuses_active_lease);
    RUN_TEST(lock_registry_free_retires_identity_and_rejects_stale_pointer);
    RUN_TEST(lock_registry_large_queue_parks_non_head_waiters);
    RUN_TEST(lock_registry_cancel_request_wakes_parked_tail);
    RUN_TEST(lock_registry_absolute_deadline_survives_repeated_wakes);
    RUN_TEST(lock_registry_concurrent_shared_exclusive_stress);
    RUN_TEST(lock_registry_cross_process_writer_beats_late_reader);
    RUN_TEST(lock_registry_fork_child_rejects_inherited_registry);
}
