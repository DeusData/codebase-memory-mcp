/*
 * test_platform.c — RED phase tests for foundation/platform.
 */
#include "test_framework.h"
#include "../src/foundation/compat.h" /* cbm_setenv / cbm_unsetenv (Windows-portable) */
#include "../src/foundation/compat_fs.h"
#include "../src/foundation/constants.h"
#include "../src/foundation/compat_thread.h"
#include "../src/foundation/platform.h"
#include "../src/foundation/platform_internal.h"
#include "../src/foundation/system_info_internal.h"
#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <unistd.h>

#ifdef __linux__
/* Linux-only cgroup tests need stdio for FILE*, stdlib for mkdtemp,
 * string for strncpy/strchr, sys/stat for mkdir, dirent for the
 * shell-free recursive teardown. */
#include <dirent.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#endif

enum { PLATFORM_TIME_THREADS = 8 };
enum { PLATFORM_MKDTEMP_THREADS = 8, PLATFORM_MKDTEMP_ITERATIONS = 32 };

#include <stdio.h>
#include <string.h>

/* Worker staging files land under CBM_CACHE_DIR, which users may place at
 * non-ASCII paths. On Windows the templates must round-trip through the wide
 * APIs; the ANSI CRT (_mktemp/_open) mangles UTF-8 bytes and fails. */
/* Store DB paths embed full repo-path-derived project names; deep repo or
 * runner paths overflow the legacy 260-char limit. The compat file layer must
 * carry absolute paths of any length (extended-length \\?\ form on
 * Windows). */
TEST(platform_file_apis_survive_max_path_overflow) {
    char base[CBM_SZ_512];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-longpath-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));

    enum { LONG_SEGMENTS = 5 };
    static const char segment[] =
        "segment-abcdefghijklmnopqrstuvwxyz0123456789-abcdefghijklmnop";
    char deep[CBM_SZ_1K];
    written = snprintf(deep, sizeof(deep), "%s", base);
    ASSERT_TRUE(written > 0 && written < (int)sizeof(deep));
    for (int index = 0; index < LONG_SEGMENTS; index++) {
        written = snprintf(deep + strlen(deep), sizeof(deep) - strlen(deep), "/%s", segment);
        ASSERT_TRUE(written > 0);
    }
    ASSERT_TRUE(strlen(deep) > 300);
    ASSERT_TRUE(cbm_mkdir_p(deep, 0700));

    char file_path[CBM_SZ_1K];
    written = snprintf(file_path, sizeof(file_path), "%s/store.db", deep);
    ASSERT_TRUE(written > 0 && written < (int)sizeof(file_path));
    FILE *file = cbm_fopen(file_path, "wb");
    ASSERT_NOT_NULL(file);
    ASSERT_GTE(fputs("deep\n", file), 0);
    ASSERT_EQ(fclose(file), 0);
    ASSERT_TRUE(cbm_file_exists(file_path));
    ASSERT_TRUE(cbm_is_dir(deep));
    ASSERT_EQ(cbm_unlink(file_path), 0);
    char cursor[CBM_SZ_1K];
    (void)snprintf(cursor, sizeof(cursor), "%s", deep);
    for (int index = 0; index < LONG_SEGMENTS; index++) {
        ASSERT_EQ(cbm_rmdir(cursor), 0);
        char *slash = strrchr(cursor, '/');
        ASSERT_NOT_NULL(slash);
        *slash = '\0';
    }
    (void)cbm_rmdir(base);
    PASS();
}

TEST(platform_mkstemp_and_mkdtemp_survive_non_ascii_directory) {
    char base[CBM_SZ_256];
    int written = snprintf(base, sizeof(base), "/tmp/cbm-utf8-Ã©Ã¨-XXXXXX");
    ASSERT_TRUE(written > 0 && written < (int)sizeof(base));
    ASSERT_NOT_NULL(cbm_mkdtemp(base));

    char file_template[CBM_SZ_512];
    written = snprintf(file_template, sizeof(file_template), "%s/.probe-XXXXXX", base);
    ASSERT_TRUE(written > 0 && written < (int)sizeof(file_template));
    int descriptor = cbm_mkstemp(file_template);
    bool created = descriptor >= 0;
    if (created) {
#ifdef _WIN32
        _close(descriptor);
#else
        close(descriptor);
#endif
    }

    static const char probe[] = "probe";
    FILE *probe_file = created ? cbm_fopen(file_template, "wb") : NULL;
    bool probe_written = false;
    if (probe_file) {
        bool write_ok =
            fwrite(probe, sizeof(probe) - SKIP_ONE, SKIP_ONE, probe_file) == SKIP_ONE;
        bool close_ok = fclose(probe_file) == 0;
        probe_written = write_ok && close_ok;
    }
    struct stat directory_state = {0};
    struct stat file_state = {0};
    int directory_stat = cbm_stat(base, &directory_state);
    int file_stat = cbm_stat(file_template, &file_state);
    cbm_file_identity_t directory_identity = {0};
    cbm_file_identity_t file_identity = {0};
    cbm_file_identity_t repeated_file_identity = {0};
    bool directory_identity_read = cbm_file_identity_read(base, &directory_identity);
    bool file_identity_read = cbm_file_identity_read(file_template, &file_identity);
    bool repeated_file_identity_read =
        cbm_file_identity_read(file_template, &repeated_file_identity);
    if (created) {
        (void)cbm_unlink(file_template);
    }
    errno = 0;
    int missing_stat = cbm_stat(file_template, &file_state);
    int missing_error = errno;
    (void)cbm_rmdir(base);
    ASSERT_TRUE(created);
    ASSERT_TRUE(probe_written);
    ASSERT_EQ(directory_stat, 0);
    ASSERT_TRUE(S_ISDIR(directory_state.st_mode));
    ASSERT_EQ(file_stat, 0);
    ASSERT_TRUE(S_ISREG(file_state.st_mode));
    ASSERT_EQ(file_state.st_size, (off_t)(sizeof(probe) - SKIP_ONE));
    ASSERT_TRUE(directory_identity_read);
    ASSERT_TRUE(file_identity_read);
    ASSERT_TRUE(repeated_file_identity_read);
    ASSERT_TRUE(cbm_file_identity_equal(&file_identity, &repeated_file_identity));
    ASSERT_EQ(missing_stat, -1);
    ASSERT_EQ(missing_error, ENOENT);
    /* The returned path must keep the caller's UTF-8 directory intact. */
    ASSERT_NOT_NULL(strstr(file_template, "Ã©Ã¨"));
    PASS();
}

typedef struct {
    atomic_int *ready;
    atomic_bool *go;
    int worker_index;
    int created;
    bool ok;
    char paths[PLATFORM_MKDTEMP_ITERATIONS][CBM_SZ_512];
} platform_mkdtemp_worker_t;

static void *platform_mkdtemp_concurrent_worker(void *opaque) {
    platform_mkdtemp_worker_t *worker = opaque;
    (void)atomic_fetch_add_explicit(worker->ready, 1, memory_order_acq_rel);
    while (!atomic_load_explicit(worker->go, memory_order_acquire)) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    worker->ok = true;
    for (int index = 0; index < PLATFORM_MKDTEMP_ITERATIONS; index++) {
        int written =
            snprintf(worker->paths[index], sizeof(worker->paths[index]),
                     "/tmp/cbm-mkdtemp-concurrent-%d-%d-XXXXXX", worker->worker_index, index);
        if (written <= 0 || written >= (int)sizeof(worker->paths[index]) ||
            !cbm_mkdtemp(worker->paths[index])) {
            worker->ok = false;
            break;
        }
        worker->created++;
    }
    return NULL;
}

/* MCP daemon sessions can enter cbm_mkdtemp simultaneously. Every request must
 * retain its own expanded path and create a distinct directory. */
TEST(platform_mkdtemp_is_thread_safe) {
    cbm_thread_t threads[PLATFORM_MKDTEMP_THREADS];
    platform_mkdtemp_worker_t workers[PLATFORM_MKDTEMP_THREADS] = {0};
    atomic_int ready;
    atomic_bool go;
    atomic_init(&ready, 0);
    atomic_init(&go, false);

    int started = 0;
    for (; started < PLATFORM_MKDTEMP_THREADS; started++) {
        workers[started].ready = &ready;
        workers[started].go = &go;
        workers[started].worker_index = started;
        if (cbm_thread_create(&threads[started], 0, platform_mkdtemp_concurrent_worker,
                              &workers[started]) != 0) {
            break;
        }
    }
    while (started == PLATFORM_MKDTEMP_THREADS &&
           atomic_load_explicit(&ready, memory_order_acquire) != PLATFORM_MKDTEMP_THREADS) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    atomic_store_explicit(&go, true, memory_order_release);
    for (int index = 0; index < started; index++) {
        (void)cbm_thread_join(&threads[index]);
    }

    bool all_ok = started == PLATFORM_MKDTEMP_THREADS;
    for (int index = 0; index < started; index++) {
        all_ok =
            all_ok && workers[index].ok && workers[index].created == PLATFORM_MKDTEMP_ITERATIONS;
        for (int path_index = 0; path_index < workers[index].created; path_index++) {
            all_ok = all_ok && cbm_is_dir(workers[index].paths[path_index]);
            (void)cbm_rmdir(workers[index].paths[path_index]);
        }
    }
    ASSERT_TRUE(all_ok);
    PASS();
}

TEST(platform_counter_scaling_avoids_intermediate_overflow) {
    const uint64_t frequency = UINT64_C(10000000);
    const uint64_t counter = UINT64_C(20000000000);

    ASSERT(cbm_platform_scale_counter_ns(counter, frequency) == UINT64_C(2000000000000));
    ASSERT(cbm_platform_scale_counter_ns(UINT64_MAX, UINT64_MAX) == UINT64_C(1000000000));
    ASSERT(cbm_platform_scale_counter_ns(UINT64_MAX, 1) == UINT64_MAX);
    PASS();
}

TEST(platform_counter_scaling_preserves_monotonic_deadlines) {
    const uint64_t frequency = UINT64_C(10000000);
    const uint64_t counter = UINT64_MAX / UINT64_C(1000000000);
    const uint64_t now_ns = cbm_platform_scale_counter_ns(counter, frequency);
    const uint64_t next_ns = cbm_platform_scale_counter_ns(counter + 1, frequency);
    const uint64_t deadline_ns = cbm_platform_scale_counter_ns(counter + 5 * frequency, frequency);

    ASSERT(next_ns >= now_ns);
    ASSERT(deadline_ns > now_ns);
    ASSERT(deadline_ns - now_ns == UINT64_C(5000000000));
    PASS();
}

TEST(platform_proc_stat_group_parser_handles_parentheses_and_states) {
    int64_t process_group = 0;
    bool execution_quiescent = false;
    ASSERT_TRUE(cbm_platform_parse_proc_stat_group(
        "123 (ordinary worker) R 7 41 41 0 -1 0", &process_group, &execution_quiescent));
    ASSERT_EQ(process_group, 41);
    ASSERT_FALSE(execution_quiescent);

    ASSERT_TRUE(cbm_platform_parse_proc_stat_group(
        "456 (worker ) name with spaces) Z 8 99 99 0 -1 0", &process_group,
        &execution_quiescent));
    ASSERT_EQ(process_group, 99);
    ASSERT_TRUE(execution_quiescent);

    ASSERT_TRUE(cbm_platform_parse_proc_stat_group(
        "789 (dead worker) X 9 101 101 0 -1 0", &process_group, &execution_quiescent));
    ASSERT_EQ(process_group, 101);
    ASSERT_TRUE(execution_quiescent);

    ASSERT_FALSE(cbm_platform_parse_proc_stat_group(
        "123 missing-close R 7 41", &process_group, &execution_quiescent));
    ASSERT_FALSE(cbm_platform_parse_proc_stat_group(
        "123 (missing fields) R 7", &process_group, &execution_quiescent));
    ASSERT_FALSE(cbm_platform_parse_proc_stat_group(NULL, &process_group, &execution_quiescent));
    PASS();
}

TEST(platform_proc_entry_disappearance_is_narrow) {
    ASSERT_TRUE(cbm_platform_proc_entry_vanished(ENOENT));
    ASSERT_TRUE(cbm_platform_proc_entry_vanished(ESRCH));
    ASSERT_FALSE(cbm_platform_proc_entry_vanished(0));
    ASSERT_FALSE(cbm_platform_proc_entry_vanished(EACCES));
    ASSERT_FALSE(cbm_platform_proc_entry_vanished(EIO));
    PASS();
}

typedef struct {
    atomic_int *ready;
    atomic_bool *go;
    uint64_t value;
} platform_time_worker_t;

static void *platform_time_first_call(void *opaque) {
    platform_time_worker_t *worker = opaque;
    (void)atomic_fetch_add_explicit(worker->ready, 1, memory_order_acq_rel);
    while (!atomic_load_explicit(worker->go, memory_order_acquire)) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    worker->value = cbm_now_ns();
    return NULL;
}

TEST(platform_now_ns_concurrent_first_call) {
    cbm_thread_t threads[PLATFORM_TIME_THREADS];
    platform_time_worker_t workers[PLATFORM_TIME_THREADS];
    atomic_int ready;
    atomic_bool go;
    atomic_init(&ready, 0);
    atomic_init(&go, false);
    size_t created = 0;
    for (; created < PLATFORM_TIME_THREADS; created++) {
        workers[created] = (platform_time_worker_t){
            .ready = &ready,
            .go = &go,
        };
        if (cbm_thread_create(&threads[created], 0, platform_time_first_call, &workers[created]) !=
            0) {
            break;
        }
    }
    while (created == PLATFORM_TIME_THREADS &&
           atomic_load_explicit(&ready, memory_order_acquire) != PLATFORM_TIME_THREADS) {
        atomic_signal_fence(memory_order_seq_cst);
    }
    atomic_store_explicit(&go, true, memory_order_release);
    for (size_t index = 0; index < created; index++) {
        (void)cbm_thread_join(&threads[index]);
    }

    ASSERT_EQ(created, PLATFORM_TIME_THREADS);
    for (size_t index = 0; index < created; index++) {
        ASSERT_GT(workers[index].value, 0);
    }
    PASS();
}

TEST(platform_now_ns) {
    uint64_t t1 = cbm_now_ns();
    ASSERT_GT(t1, 0);
    /* Busy-wait a tiny bit */
    for (volatile int i = 0; i < 100000; i++) {}
    uint64_t t2 = cbm_now_ns();
    ASSERT_GT(t2, t1);
    PASS();
}

TEST(platform_now_ms) {
    uint64_t t1 = cbm_now_ms();
    ASSERT_GT(t1, 0);
    PASS();
}

TEST(platform_thread_condition_uses_monotonic_deadline) {
    enum {
        CONDITION_TEST_TIMEOUT_MS = 20,
        /* A deadline can be observed late under load, but an incorrect clock
         * domain must not park the suite indefinitely. */
        CONDITION_TEST_HANG_BOUND_MS = 5000,
    };
    cbm_mutex_t mutex;
    cbm_thread_condition_t condition;
    cbm_mutex_init(&mutex);
    int init_status = cbm_thread_condition_init(&condition);
    cbm_mutex_lock(&mutex);
    uint64_t started_ms = cbm_now_ms();
    cbm_thread_condition_wait_status_t wait_status =
        init_status == 0 ? cbm_thread_condition_wait_until(&condition, &mutex,
                                                           started_ms + CONDITION_TEST_TIMEOUT_MS)
                         : CBM_THREAD_CONDITION_WAIT_ERROR;
    uint64_t elapsed_ms = cbm_now_ms() - started_ms;
    cbm_mutex_unlock(&mutex);
    if (init_status == 0) {
        cbm_thread_condition_destroy(&condition);
    }
    cbm_mutex_destroy(&mutex);

    ASSERT_EQ(init_status, 0);
    ASSERT_EQ(wait_status, CBM_THREAD_CONDITION_WAIT_TIMEOUT);
    ASSERT_TRUE(elapsed_ms >= CONDITION_TEST_TIMEOUT_MS);
    ASSERT_TRUE(elapsed_ms <= CONDITION_TEST_HANG_BOUND_MS);
    PASS();
}

TEST(platform_nprocs) {
    int n = cbm_nprocs();
    ASSERT_GT(n, 0);
    ASSERT_LT(n, 10000); /* sanity */
    PASS();
}

TEST(platform_file_exists) {
    /* This test file should exist */
    ASSERT_TRUE(cbm_file_exists("tests/test_platform.c"));
    ASSERT_FALSE(cbm_file_exists("nonexistent_file_xyz.txt"));
    PASS();
}

TEST(platform_is_dir) {
    ASSERT_TRUE(cbm_is_dir("tests"));
    ASSERT_FALSE(cbm_is_dir("tests/test_platform.c"));
    ASSERT_FALSE(cbm_is_dir("nonexistent_dir"));
    PASS();
}

TEST(platform_file_size) {
    int64_t sz = cbm_file_size("tests/test_platform.c");
    ASSERT_GT(sz, 0);
    ASSERT_EQ(cbm_file_size("nonexistent_file_xyz.txt"), -1);
    PASS();
}

TEST(platform_mmap) {
    /* mmap this test file and verify first bytes */
    size_t sz = 0;
    void *data = cbm_mmap_read("tests/test_platform.c", &sz);
    ASSERT_NOT_NULL(data);
    ASSERT_GT(sz, 0);
    /* First line should be the comment */
    ASSERT(memcmp(data, "/*", 2) == 0);
    cbm_munmap(data, sz);
    PASS();
}

TEST(platform_mmap_nonexistent) {
    size_t sz = 0;
    void *data = cbm_mmap_read("nonexistent_xyz.txt", &sz);
    ASSERT_NULL(data);
    PASS();
}

typedef struct {
    const char *home;
    const char *cache;
    atomic_int *ready;
    atomic_int *release;
} platform_path_thread_result_t;

static void *platform_capture_path_buffers(void *opaque) {
    platform_path_thread_result_t *result = opaque;
    result->home = cbm_get_home_dir();
    result->cache = cbm_resolve_cache_dir();
    atomic_fetch_add_explicit(result->ready, 1, memory_order_release);
    while (atomic_load_explicit(result->release, memory_order_acquire) == 0) {
        cbm_usleep(1000);
    }
    return NULL;
}

/* The daemon dispatches different sessions concurrently. Path helpers may
 * retain their historical return-pointer API, but each live thread needs
 * separate storage; process-global mutable buffers are a C data race. */
TEST(platform_path_helpers_use_per_thread_storage) {
    const char *saved_home = getenv("HOME");
    const char *saved_cache = getenv("CBM_CACHE_DIR");
    char *saved_home_copy = saved_home ? strdup(saved_home) : NULL;
    char *saved_cache_copy = saved_cache ? strdup(saved_cache) : NULL;
    ASSERT_EQ(cbm_setenv("HOME", "/tmp/cbm-platform-thread-home", 1), 0);
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", "/tmp/cbm-platform-thread-cache", 1), 0);

    atomic_int ready;
    atomic_int release;
    atomic_init(&ready, 0);
    atomic_init(&release, 0);
    platform_path_thread_result_t results[2] = {
        {.ready = &ready, .release = &release},
        {.ready = &ready, .release = &release},
    };
    cbm_thread_t threads[2];
    bool started0 =
        cbm_thread_create(&threads[0], 0, platform_capture_path_buffers, &results[0]) == 0;
    bool started1 =
        cbm_thread_create(&threads[1], 0, platform_capture_path_buffers, &results[1]) == 0;
    for (int spins = 0; started0 && started1 && spins < 5000 &&
                        atomic_load_explicit(&ready, memory_order_acquire) < 2;
         spins++) {
        cbm_usleep(1000);
    }
    bool both_ready = atomic_load_explicit(&ready, memory_order_acquire) == 2;
    bool separate_home =
        both_ready && results[0].home && results[1].home && results[0].home != results[1].home;
    bool separate_cache =
        both_ready && results[0].cache && results[1].cache && results[0].cache != results[1].cache;
    atomic_store_explicit(&release, 1, memory_order_release);
    if (started0) {
        (void)cbm_thread_join(&threads[0]);
    }
    if (started1) {
        (void)cbm_thread_join(&threads[1]);
    }

    if (saved_home_copy) {
        (void)cbm_setenv("HOME", saved_home_copy, 1);
    } else {
        (void)cbm_unsetenv("HOME");
    }
    if (saved_cache_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_cache_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_home_copy);
    free(saved_cache_copy);

    ASSERT_TRUE(started0);
    ASSERT_TRUE(started1);
    ASSERT_TRUE(both_ready);
    ASSERT_TRUE(separate_home);
    ASSERT_TRUE(separate_cache);
    PASS();
}

/* A configured cache root is an identity boundary. Truncating an oversized
 * value and silently using the prefix would admit/log one root while placing
 * data somewhere the user never selected. Reject it instead of falling back. */
TEST(platform_cache_dir_rejects_truncated_override) {
    const char *saved = getenv("CBM_CACHE_DIR");
    char *saved_copy = saved ? strdup(saved) : NULL;
    size_t length = 5000U;
    char *value = malloc(length + 1U);
    ASSERT_NOT_NULL(value);
    memcpy(value, "/tmp/", 5U);
    memset(value + 5U, 'a', length - 5U);
    value[length] = '\0';
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", value, 1), 0);

    const char *resolved = cbm_resolve_cache_dir();

    if (saved_copy) {
        (void)cbm_setenv("CBM_CACHE_DIR", saved_copy, 1);
    } else {
        (void)cbm_unsetenv("CBM_CACHE_DIR");
    }
    free(saved_copy);
    free(value);
    ASSERT_NULL(resolved);
    PASS();
}

#ifdef _WIN32
/* cbm_safe_getenv reads Windows' wide environment as UTF-8. Its matching
 * setter must update that same wide environment; _putenv_s alone interprets
 * UTF-8 path bytes through the active ANSI code page. */
TEST(platform_setenv_preserves_utf8_in_wide_environment) {
    static const char utf8[] = "C:/cbm-cache-\xce\x94-\xe6\x97\xa5\xe6\x9c\xac";
    static const wchar_t wide[] = L"C:/cbm-cache-\u0394-\u65e5\u672c";
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", utf8, 1), 0);
    wchar_t observed_wide[128];
    ASSERT_EQ(GetEnvironmentVariableW(L"CBM_CACHE_DIR", observed_wide, 128), (DWORD)(wcslen(wide)));
    ASSERT_EQ(wcscmp(observed_wide, wide), 0);
    char observed_utf8[128];
    ASSERT_NOT_NULL(cbm_safe_getenv("CBM_CACHE_DIR", observed_utf8, sizeof(observed_utf8), NULL));
    ASSERT_STR_EQ(observed_utf8, utf8);
    (void)cbm_unsetenv("CBM_CACHE_DIR");
    PASS();
}

/* cbm_getenv_fits must share cbm_safe_getenv's UTF-16 environment reader.
 * SetEnvironmentVariableW deliberately bypasses the CRT's narrow _environ
 * snapshot so this test fails if the two public APIs drift apart again. */
TEST(platform_getenv_fits_reads_windows_wide_environment) {
    static const wchar_t name[] = L"CBM_TEST_GETENV_FITS_WIDE";
    static const wchar_t wide[] = L"C:/cbm-config-\u0394-\u65e5\u672c";
    static const char utf8[] = "C:/cbm-config-\xce\x94-\xe6\x97\xa5\xe6\x9c\xac";
    ASSERT_TRUE(SetEnvironmentVariableW(name, wide) != 0);

    char observed[128];
    bool present = false;
    bool fits =
        cbm_getenv_fits("CBM_TEST_GETENV_FITS_WIDE", observed, sizeof(observed), &present);
    bool full_value_present = present;

    char too_small[8] = "stale";
    present = false;
    bool too_long =
        !cbm_getenv_fits("CBM_TEST_GETENV_FITS_WIDE", too_small, sizeof(too_small), &present);
    bool long_value_present = present;

    ASSERT_TRUE(SetEnvironmentVariableW(name, NULL) != 0);
    ASSERT_TRUE(fits);
    ASSERT_TRUE(full_value_present);
    ASSERT_STR_EQ(observed, utf8);
    ASSERT_TRUE(too_long);
    ASSERT_TRUE(long_value_present);
    ASSERT_STR_EQ(too_small, "");
    PASS();
}

/* Empty and absent variables have different fallback semantics. In
 * particular, an explicitly empty CBM_CACHE_DIR means "use the default"; it
 * must not be misreported as a failed wide-environment read. Unset is also
 * required to stay idempotent because test and process cleanup call it after
 * partially initialized paths. */
TEST(platform_windows_empty_environment_is_read_and_unset_idempotently) {
    ASSERT_EQ(cbm_setenv("CBM_CACHE_DIR", "", 1), 0);
    char observed[9] = "sentinel";
    ASSERT_NOT_NULL(cbm_safe_getenv("CBM_CACHE_DIR", observed, sizeof(observed), "fallback"));
    ASSERT_STR_EQ(observed, "");
    ASSERT_EQ(cbm_unsetenv("CBM_CACHE_DIR"), 0);
    ASSERT_EQ(cbm_unsetenv("CBM_CACHE_DIR"), 0);
    PASS();
}
#endif

/*
 * CBM_WORKERS env override for cbm_default_worker_count.
 *
 * Containers running cbm on a host with more CPUs than the cgroup's
 * effective quota currently see ~host_cpu workers spawned because
 * sysconf(_SC_NPROCESSORS_ONLN) is not cgroup-aware (see GitHub
 * issue for the cgroup-detection ask). CBM_WORKERS is the smaller,
 * explicit-override path that ships independently.
 */
TEST(platform_default_workers_env_override) {
    cbm_setenv("CBM_WORKERS", "4", 1);
    int n = cbm_default_worker_count(true);
    ASSERT_EQ(n, 4);
    /* initial=false should also honor the explicit override. */
    int m = cbm_default_worker_count(false);
    ASSERT_EQ(m, 4);
    cbm_unsetenv("CBM_WORKERS");
    PASS();
}

TEST(platform_default_workers_env_invalid) {
    /* Out-of-range values (< 1 or > 256) and non-numeric strings
     * fall back to the sysconf-derived default. */
    int baseline = cbm_default_worker_count(true);
    ASSERT_GT(baseline, 0);

    cbm_setenv("CBM_WORKERS", "0", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "-1", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "9999", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_setenv("CBM_WORKERS", "not-a-number", 1);
    ASSERT_EQ(cbm_default_worker_count(true), baseline);

    cbm_unsetenv("CBM_WORKERS");
    PASS();
}

TEST(platform_default_workers_env_unset) {
    /* When CBM_WORKERS is unset the result matches today's behaviour
     * (info.total_cores for initial=true, perf_cores-1 for false). */
    cbm_unsetenv("CBM_WORKERS");
    cbm_system_info_t info = cbm_system_info();
    ASSERT_EQ(cbm_default_worker_count(true), info.total_cores);
    PASS();
}

TEST(platform_getenv_fits) {
    const char *name = "CBM_TEST_GETENV_FITS";
    char buf[8];
    bool present = true;

    cbm_unsetenv(name);
    ASSERT_FALSE(cbm_getenv_fits(name, buf, sizeof(buf), &present));
    ASSERT_FALSE(present);
    ASSERT_STR_EQ(buf, "");

    cbm_setenv(name, "", 1);
    present = true;
    ASSERT_FALSE(cbm_getenv_fits(name, buf, sizeof(buf), &present));
    ASSERT_FALSE(present);
    ASSERT_STR_EQ(buf, "");

    cbm_setenv(name, "fits", 1);
    ASSERT_TRUE(cbm_getenv_fits(name, buf, sizeof(buf), &present));
    ASSERT_TRUE(present);
    ASSERT_STR_EQ(buf, "fits");

    cbm_setenv(name, "too-long-for-buffer", 1);
    present = false;
    ASSERT_FALSE(cbm_getenv_fits(name, buf, sizeof(buf), &present));
    ASSERT_TRUE(present);
    ASSERT_STR_EQ(buf, "");

    cbm_unsetenv(name);
    PASS();
}

TEST(platform_env_flag_enabled) {
    const char *name = "CBM_TEST_ENV_FLAG";

    cbm_unsetenv(name);
    ASSERT_FALSE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "", 1);
    ASSERT_FALSE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "0", 1);
    ASSERT_FALSE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "false", 1);
    ASSERT_FALSE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "OFF", 1);
    ASSERT_FALSE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "No", 1);
    ASSERT_FALSE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "1", 1);
    ASSERT_TRUE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "true", 1);
    ASSERT_TRUE(cbm_env_flag_enabled(name));

    cbm_setenv(name, "debug", 1);
    ASSERT_TRUE(cbm_env_flag_enabled(name));

    cbm_unsetenv(name);
    PASS();
}

TEST(platform_dirent_name_fits_boundary) {
    char fits[CBM_DIRENT_NAME_MAX];
    char too_long[CBM_DIRENT_NAME_MAX + SKIP_ONE];

    memset(fits, 'a', sizeof(fits) - SKIP_ONE);
    fits[sizeof(fits) - SKIP_ONE] = '\0';
    ASSERT_TRUE(cbm_dirent_name_fits(fits));

    memset(too_long, 'b', sizeof(too_long) - SKIP_ONE);
    too_long[sizeof(too_long) - SKIP_ONE] = '\0';
    ASSERT_FALSE(cbm_dirent_name_fits(too_long));
    ASSERT_FALSE(cbm_dirent_name_fits(NULL));
    PASS();
}

/* ── cgroup-aware detection (Linux only) ─────────────────────────── */

#ifdef __linux__

/* Create a unique tmp directory the caller will own; returns 0 on success. */
static int cgroup_test_setup(char *root, size_t root_sz) {
    strncpy(root, "/tmp/cbm_cgroup_test_XXXXXX", root_sz);
    return mkdtemp(root) != NULL ? 0 : -1;
}

/* Write `content` to "<root>/<relpath>". Creates parent subdir if needed.
 * Returns 0 on success, -1 on any failure. */
static int cgroup_test_write(const char *root, const char *relpath, const char *content) {
    char path[1024];
    const char *slash = strchr(relpath, '/');
    if (slash != NULL) {
        char subdir[1024];
        size_t n = (size_t)(slash - relpath);
        if (n >= sizeof(subdir)) {
            return -1;
        }
        memcpy(subdir, relpath, n);
        subdir[n] = '\0';
        snprintf(path, sizeof(path), "%s/%s", root, subdir);
        (void)mkdir(path, S_IRWXU);
    }
    snprintf(path, sizeof(path), "%s/%s", root, relpath);
    FILE *fp = fopen(path, "we");
    if (fp == NULL) {
        return -1;
    }
    size_t n = strlen(content);
    int rc = (fwrite(content, 1, n, fp) == n) ? 0 : -1;
    fclose(fp);
    return rc;
}

/* Recursively remove a tmp dir created by cgroup_test_setup. Best-effort.
 * Uses opendir/unlink/rmdir rather than system("rm -rf ...") to avoid
 * spawning a shell from the test binary. */
static void cgroup_test_teardown(const char *root) {
    DIR *d = opendir(root);
    if (d != NULL) {
        struct dirent *ent;
        while ((ent = readdir(d)) != NULL) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
                continue;
            }
            char child[1024];
            snprintf(child, sizeof(child), "%s/%s", root, ent->d_name);
            struct stat st;
            if (stat(child, &st) == 0 && S_ISDIR(st.st_mode)) {
                cgroup_test_teardown(child); /* recurse into subdir */
            } else {
                (void)unlink(child);
            }
        }
        closedir(d);
    }
    (void)rmdir(root);
}

TEST(cgroup_v2_cpu_quota) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 200ms quota in a 100ms period → 2 effective CPUs. */
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "200000 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_cpu_quota_rounds_up) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 150ms quota / 100ms period = 1.5 → ceil = 2. */
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "150000 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_cpu_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu.max", "max 100000\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_cpu_quota) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_quota_us", "200000"), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_period_us", "100000"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), 2);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_cpu_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* quota=-1 is the cgroup-v1 sentinel for "no quota". */
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_quota_us", "-1"), 0);
    ASSERT_EQ(cgroup_test_write(root, "cpu/cpu.cfs_period_us", "100000"), 0);
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_no_cpu_files) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* Empty tmp dir: no v2 file, no v1 file → fall through to sysconf. */
    ASSERT_EQ(cbm_detect_cgroup_cpus(root), -1);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_mem) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 2 GiB. */
    ASSERT_EQ(cgroup_test_write(root, "memory.max", "2147483648\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)2147483648UL);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v2_mem_unlimited) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cgroup_test_write(root, "memory.max", "max\n"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_mem) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* 1 GiB. */
    ASSERT_EQ(cgroup_test_write(root, "memory/memory.limit_in_bytes", "1073741824"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)1073741824UL);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_v1_mem_unlimited_sentinel) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    /* cgroup v1 reports a huge near-ULLONG_MAX value when unlimited
     * (PAGE_COUNTER_MAX). Our parser treats anything >= ULLONG_MAX/2
     * as effectively unlimited. */
    ASSERT_EQ(cgroup_test_write(root, "memory/memory.limit_in_bytes", "9223372036854775807"), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

TEST(cgroup_no_mem_files) {
    char root[64];
    ASSERT_EQ(cgroup_test_setup(root, sizeof(root)), 0);
    ASSERT_EQ(cbm_detect_cgroup_mem(root), (size_t)0);
    cgroup_test_teardown(root);
    PASS();
}

#endif /* __linux__ */

/* Restored from the merge base: cbm_system_info() is still live
 * (src/foundation/platform.h:107) and load-bearing -- src/daemon/host.c:998 sizes
 * the memory pool from total_ram, and src/foundation/mem.c:221 reads it too. It
 * lost its only direct test when this file auto-merged, with no conflict raised.
 * Strengthened past the base form, which checked only total_cores and total_ram:
 * perf_cores is also consumed by callers and must be sane and bounded by
 * total_cores. */
TEST(platform_system_info) {
    cbm_system_info_t info = cbm_system_info();
    ASSERT_GT(info.total_cores, 0);
    ASSERT_GT(info.total_ram, 0);
    ASSERT_GT(info.perf_cores, 0);
    ASSERT_TRUE(info.perf_cores <= info.total_cores);
    PASS();
}

SUITE(platform) {
    RUN_TEST(platform_file_apis_survive_max_path_overflow);
    RUN_TEST(platform_mkstemp_and_mkdtemp_survive_non_ascii_directory);
    RUN_TEST(platform_mkdtemp_is_thread_safe);
    RUN_TEST(platform_counter_scaling_avoids_intermediate_overflow);
    RUN_TEST(platform_counter_scaling_preserves_monotonic_deadlines);
    RUN_TEST(platform_proc_stat_group_parser_handles_parentheses_and_states);
    RUN_TEST(platform_proc_entry_disappearance_is_narrow);
    RUN_TEST(platform_now_ns_concurrent_first_call);
    RUN_TEST(platform_now_ns);
    RUN_TEST(platform_now_ms);
    RUN_TEST(platform_thread_condition_uses_monotonic_deadline);
    RUN_TEST(platform_nprocs);
    RUN_TEST(platform_system_info); /* restored from merge base */
    RUN_TEST(platform_file_exists);
    RUN_TEST(platform_is_dir);
    RUN_TEST(platform_file_size);
    RUN_TEST(platform_mmap);
    RUN_TEST(platform_mmap_nonexistent);
    RUN_TEST(platform_path_helpers_use_per_thread_storage);
    RUN_TEST(platform_cache_dir_rejects_truncated_override);
#ifdef _WIN32
    RUN_TEST(platform_setenv_preserves_utf8_in_wide_environment);
    RUN_TEST(platform_getenv_fits_reads_windows_wide_environment);
    RUN_TEST(platform_windows_empty_environment_is_read_and_unset_idempotently);
#endif
    RUN_TEST(platform_default_workers_env_override);
    RUN_TEST(platform_default_workers_env_invalid);
    RUN_TEST(platform_default_workers_env_unset);
    RUN_TEST(platform_getenv_fits);
    RUN_TEST(platform_env_flag_enabled);
    RUN_TEST(platform_dirent_name_fits_boundary);
#ifdef __linux__
    RUN_TEST(cgroup_v2_cpu_quota);
    RUN_TEST(cgroup_v2_cpu_quota_rounds_up);
    RUN_TEST(cgroup_v2_cpu_unlimited);
    RUN_TEST(cgroup_v1_cpu_quota);
    RUN_TEST(cgroup_v1_cpu_unlimited);
    RUN_TEST(cgroup_no_cpu_files);
    RUN_TEST(cgroup_v2_mem);
    RUN_TEST(cgroup_v2_mem_unlimited);
    RUN_TEST(cgroup_v1_mem);
    RUN_TEST(cgroup_v1_mem_unlimited_sentinel);
    RUN_TEST(cgroup_no_mem_files);
#endif
}
