/*
 * diagnostics.c — Periodic diagnostics file writer.
 *
 * Every enabled process writes below a fresh owner-private directory. Files
 * are created exclusively without following links, and POSIX I/O remains
 * anchored to the validated directory descriptor. Diagnostics are strictly
 * best-effort: shutdown is interruptible and bounded, so a stuck filesystem
 * can never retain the daemon.
 */
#include "foundation/diagnostics.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/macos_acl.h"
#include "foundation/mem.h"
#include "foundation/platform.h"
#include "foundation/private_file_lock.h"
#include "foundation/private_file_lock_internal.h"

#include <mimalloc.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <process.h>
#include <windows.h>
#define getpid _getpid
#else
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#endif

enum {
    DIAG_INTERVAL_MS = 5000,
    DIAG_WAIT_SLICE_MS = 10,
    DIAG_STOP_TIMEOUT_MS = 500,
};

#define DIAG_NDJSON_CAP_BYTES (8u * 1024u * 1024u)
#define DIAG_SNAPSHOT_NAME "snapshot.json"
#define DIAG_SNAPSHOT_TMP_NAME "snapshot.json.tmp"
#define DIAG_TRAJECTORY_NAME "trajectory.ndjson"
#define DIAG_TRAJECTORY_ROTATED_NAME "trajectory.ndjson.1"

/* ── Globals ────────────────────────────────────────────────────────────── */

cbm_query_stats_t g_query_stats = {0};
static atomic_bool g_diag_stop = false;
static atomic_bool g_diag_done = false;
static cbm_thread_t g_diag_thread;
static bool g_diag_started = false;
static bool g_diag_abandoned = false;
static time_t g_start_time = 0;
static cbm_private_lock_directory_t *g_diag_directory = NULL;
#ifndef _WIN32
static int g_diag_directory_fd = -1;
#endif
static char g_diag_directory_path[CBM_PATH_MAX] = "";
static char g_diag_path[CBM_PATH_MAX] = "";
static char g_diag_ndjson_path[CBM_PATH_MAX] = "";
static size_t g_diag_ndjson_size = 0;

#ifdef CBM_DIAGNOSTICS_ENABLE_TEST_API
static atomic_bool g_diag_test_hold_writer = false;
static atomic_bool g_diag_test_writer_reached = false;
#endif

/* ── Query stats ────────────────────────────────────────────────────────── */

void cbm_diag_record_query(long long duration_us, bool is_error) {
    atomic_fetch_add(&g_query_stats.count, 1);
    atomic_fetch_add(&g_query_stats.time_us, duration_us);
    if (is_error) {
        atomic_fetch_add(&g_query_stats.errors, 1);
    }
    long long old_max = atomic_load(&g_query_stats.max_us);
    while (duration_us > old_max) {
        if (atomic_compare_exchange_weak(&g_query_stats.max_us, &old_max, duration_us)) {
            break;
        }
    }
}

/* ── FD count (platform-specific) ──────────────────────────────────────── */

static int count_open_fds(void) {
#ifdef __linux__
    struct dirent **entries = NULL;
    int n = scandir("/proc/self/fd", &entries, NULL, NULL);
    if (n < 0) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < n; i++) {
        free(entries[i]);
    }
    free(entries);
    return n - PAIR_LEN;
#elif defined(__APPLE__)
    struct dirent **entries = NULL;
    int n = scandir("/dev/fd", &entries, NULL, NULL);
    if (n < 0) {
        return CBM_NOT_FOUND;
    }
    for (int i = 0; i < n; i++) {
        free(entries[i]);
    }
    free(entries);
    return n - PAIR_LEN;
#else
    return CBM_NOT_FOUND;
#endif
}

/* ── Private output directory ────────────────────────────────────────────── */

static bool diag_set_output_paths(void) {
    int snapshot = snprintf(g_diag_path, sizeof(g_diag_path), "%s/%s", g_diag_directory_path,
                            DIAG_SNAPSHOT_NAME);
    int trajectory = snprintf(g_diag_ndjson_path, sizeof(g_diag_ndjson_path), "%s/%s",
                              g_diag_directory_path, DIAG_TRAJECTORY_NAME);
    return snapshot > 0 && (size_t)snapshot < sizeof(g_diag_path) && trajectory > 0 &&
           (size_t)trajectory < sizeof(g_diag_ndjson_path);
}

static void diag_directory_close(bool remove_directory) {
#ifndef _WIN32
    if (g_diag_directory_fd >= 0) {
        (void)close(g_diag_directory_fd);
        g_diag_directory_fd = -1;
    }
#endif
    cbm_private_lock_directory_close(g_diag_directory);
    g_diag_directory = NULL;
    if (remove_directory && g_diag_directory_path[0] != '\0') {
        (void)cbm_rmdir(g_diag_directory_path);
    }
}

/* Diagnostics placement honors the documented contract ($TMPDIR, /tmp
 * fallback) without touching cbm_tmpdir(), whose many other callers keep
 * their established behavior. Windows already resolves the real user temp. */
static const char *diag_tmp_base(char *buffer, size_t size) {
#ifdef _WIN32
    (void)buffer;
    (void)size;
    return cbm_tmpdir();
#else
    cbm_safe_getenv("TMPDIR", buffer, size, NULL);
    if (buffer[0] != '\0') {
        return buffer;
    }
    return "/tmp";
#endif
}

static bool diag_directory_prepare(void) {
    char tmp_base[CBM_PATH_MAX];
    char directory_template[CBM_PATH_MAX];
    int written =
        snprintf(directory_template, sizeof(directory_template), "%s/cbm-diagnostics-%d-XXXXXX",
                 diag_tmp_base(tmp_base, sizeof(tmp_base)), (int)getpid());
    if (written <= 0 || (size_t)written >= sizeof(directory_template) ||
        !cbm_mkdtemp(directory_template)) {
        return false;
    }
    written =
        snprintf(g_diag_directory_path, sizeof(g_diag_directory_path), "%s", directory_template);
    if (written <= 0 || (size_t)written >= sizeof(g_diag_directory_path)) {
        (void)cbm_rmdir(directory_template);
        return false;
    }

#ifdef _WIN32
    wchar_t *wide_directory = cbm_path_to_wide(g_diag_directory_path);
    HANDLE handle =
        wide_directory
            ? CreateFileW(wide_directory, FILE_READ_ATTRIBUTES | READ_CONTROL,
                          FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE, NULL,
                          OPEN_EXISTING, FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT,
                          NULL)
            : INVALID_HANDLE_VALUE;
    free(wide_directory);
    cbm_private_file_lock_status_t status = CBM_PRIVATE_FILE_LOCK_IO;
    if (handle != INVALID_HANDLE_VALUE) {
        status = cbm_private_lock_directory_adopt_windows(handle, g_diag_directory_path,
                                                          &g_diag_directory);
        if (status != CBM_PRIVATE_FILE_LOCK_OK) {
            (void)CloseHandle(handle);
        }
    }
#else
    int flags = O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW;
    int validation_fd = open(g_diag_directory_path, flags);
    int output_fd = open(g_diag_directory_path, flags);
    struct stat validation_state;
    struct stat output_state;
    bool same_directory =
        validation_fd >= 0 && output_fd >= 0 && fstat(validation_fd, &validation_state) == 0 &&
        fstat(output_fd, &output_state) == 0 && validation_state.st_dev == output_state.st_dev &&
        validation_state.st_ino == output_state.st_ino;
    cbm_private_file_lock_status_t status = CBM_PRIVATE_FILE_LOCK_IO;
    if (same_directory) {
        status = cbm_private_lock_directory_adopt_posix(validation_fd, g_diag_directory_path,
                                                        &g_diag_directory);
        if (status == CBM_PRIVATE_FILE_LOCK_OK) {
            validation_fd = -1;
            g_diag_directory_fd = output_fd;
            output_fd = -1;
        }
    }
    if (validation_fd >= 0) {
        (void)close(validation_fd);
    }
    if (output_fd >= 0) {
        (void)close(output_fd);
    }
#endif

    if (status != CBM_PRIVATE_FILE_LOCK_OK || !diag_set_output_paths()) {
        diag_directory_close(true);
        return false;
    }
    return true;
}

#ifdef _WIN32

static bool diag_full_path(const char *base_name, char *path, size_t path_size) {
    int written = snprintf(path, path_size, "%s/%s", g_diag_directory_path, base_name);
    return written > 0 && (size_t)written < path_size;
}

static bool diag_win_handle_valid(HANDLE handle) {
    BY_HANDLE_FILE_INFORMATION information;
    return handle != INVALID_HANDLE_VALUE && GetFileType(handle) == FILE_TYPE_DISK &&
           GetFileInformationByHandle(handle, &information) != 0 &&
           (information.dwFileAttributes &
            (FILE_ATTRIBUTE_DIRECTORY | FILE_ATTRIBUTE_REPARSE_POINT)) == 0 &&
           information.nNumberOfLinks == 1 &&
           SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) != 0;
}

static bool diag_write_file(const char *base_name, const char *data, size_t length, bool append) {
    char path[CBM_PATH_MAX];
    if (!diag_full_path(base_name, path, sizeof(path))) {
        return false;
    }
    wchar_t *wide_path = cbm_path_to_wide(path);
    DWORD access = append ? FILE_APPEND_DATA : GENERIC_WRITE;
    HANDLE handle = wide_path
                        ? CreateFileW(wide_path, access, FILE_SHARE_READ, NULL, CREATE_NEW,
                                      FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL)
                        : INVALID_HANDLE_VALUE;
    DWORD creation_error = handle == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
    if (handle == INVALID_HANDLE_VALUE && wide_path &&
        (creation_error == ERROR_FILE_EXISTS || creation_error == ERROR_ALREADY_EXISTS)) {
        handle = CreateFileW(wide_path, access, FILE_SHARE_READ, NULL,
                             append ? OPEN_EXISTING : TRUNCATE_EXISTING,
                             FILE_ATTRIBUTE_NORMAL | FILE_FLAG_OPEN_REPARSE_POINT, NULL);
    }
    free(wide_path);
    if (!diag_win_handle_valid(handle)) {
        if (handle != INVALID_HANDLE_VALUE) {
            (void)CloseHandle(handle);
        }
        return false;
    }
    bool ok = true;
    size_t offset = 0;
    while (offset < length) {
        DWORD chunk = length - offset > MAXDWORD ? MAXDWORD : (DWORD)(length - offset);
        DWORD bytes_written = 0;
        if (!WriteFile(handle, data + offset, chunk, &bytes_written, NULL) || bytes_written == 0) {
            ok = false;
            break;
        }
        offset += bytes_written;
    }
    return CloseHandle(handle) != 0 && ok;
}

static bool diag_native_rename(const char *source_name, const char *destination_name) {
    char source[CBM_PATH_MAX];
    char destination[CBM_PATH_MAX];
    return diag_full_path(source_name, source, sizeof(source)) &&
           diag_full_path(destination_name, destination, sizeof(destination)) &&
           cbm_rename_replace(source, destination) == 0;
}

static void diag_native_unlink(const char *base_name) {
    char path[CBM_PATH_MAX];
    if (diag_full_path(base_name, path, sizeof(path))) {
        (void)cbm_unlink(path);
    }
}

#else

static bool diag_posix_file_valid(int descriptor) {
    struct stat state;
    return descriptor >= 0 && fstat(descriptor, &state) == 0 && S_ISREG(state.st_mode) &&
           state.st_uid == geteuid() && state.st_nlink == 1 && (state.st_mode & 07777) == 0600 &&
           cbm_macos_extended_acl_fd_is_empty(descriptor);
}

static bool diag_write_file(const char *base_name, const char *data, size_t length, bool append) {
    int flags = O_WRONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK;
    flags |= append ? O_APPEND : O_TRUNC;
    int descriptor = openat(g_diag_directory_fd, base_name, flags | O_CREAT | O_EXCL, 0600);
    bool created = descriptor >= 0;
    if (descriptor < 0 && errno == EEXIST) {
        descriptor = openat(g_diag_directory_fd, base_name, flags);
    }
    if (created && fchmod(descriptor, 0600) != 0) {
        (void)close(descriptor);
        return false;
    }
    if (!diag_posix_file_valid(descriptor)) {
        if (descriptor >= 0) {
            (void)close(descriptor);
        }
        return false;
    }
    bool ok = true;
    size_t offset = 0;
    while (offset < length) {
        ssize_t bytes_written = write(descriptor, data + offset, length - offset);
        if (bytes_written > 0) {
            offset += (size_t)bytes_written;
        } else if (bytes_written < 0 && errno == EINTR) {
            continue;
        } else {
            ok = false;
            break;
        }
    }
    return close(descriptor) == 0 && ok;
}

static bool diag_native_rename(const char *source_name, const char *destination_name) {
    return renameat(g_diag_directory_fd, source_name, g_diag_directory_fd, destination_name) == 0;
}

static void diag_native_unlink(const char *base_name) {
    (void)unlinkat(g_diag_directory_fd, base_name, 0);
}

#endif

/* ── Writer ─────────────────────────────────────────────────────────────────────── */

static void diag_sleep_ms(unsigned int milliseconds) {
    struct timespec delay = {
        .tv_sec = (time_t)(milliseconds / 1000U),
        .tv_nsec = (long)((milliseconds % 1000U) * 1000000U),
    };
    (void)cbm_nanosleep(&delay, NULL);
}

static void diag_wait_for_interval(void) {
    uint64_t deadline = cbm_now_ms() + DIAG_INTERVAL_MS;
    while (!atomic_load_explicit(&g_diag_stop, memory_order_acquire)) {
        uint64_t now = cbm_now_ms();
        if (now >= deadline) {
            return;
        }
        uint64_t remaining = deadline - now;
        unsigned int slice =
            remaining < DIAG_WAIT_SLICE_MS ? (unsigned int)remaining : DIAG_WAIT_SLICE_MS;
        diag_sleep_ms(slice > 0 ? slice : 1);
    }
}

#ifdef CBM_DIAGNOSTICS_ENABLE_TEST_API
static void diag_test_pause_writer(void) {
    if (!atomic_load_explicit(&g_diag_test_hold_writer, memory_order_acquire)) {
        return;
    }
    atomic_store_explicit(&g_diag_test_writer_reached, true, memory_order_release);
    while (atomic_load_explicit(&g_diag_test_hold_writer, memory_order_acquire)) {
        diag_sleep_ms(DIAG_WAIT_SLICE_MS);
    }
}
#endif

static void append_trajectory(long uptime, size_t rss, size_t peak_rss, size_t commit,
                              size_t peak_commit, size_t page_faults, int fds, int qcount) {
    if (g_diag_ndjson_size > DIAG_NDJSON_CAP_BYTES) {
        if (!diag_native_rename(DIAG_TRAJECTORY_NAME, DIAG_TRAJECTORY_ROTATED_NAME)) {
            return;
        }
        g_diag_ndjson_size = 0;
    }
    char line[CBM_SZ_512];
    int length = snprintf(line, sizeof(line),
                          "{\"uptime_s\":%ld,\"rss\":%zu,\"peak_rss\":%zu,\"committed\":%zu,"
                          "\"peak_committed\":%zu,\"page_faults\":%zu,\"fd\":%d,\"queries\":%d}\n",
                          uptime, rss, peak_rss, commit, peak_commit, page_faults, fds, qcount);
    if (length <= 0 || (size_t)length >= sizeof(line) ||
        !diag_write_file(DIAG_TRAJECTORY_NAME, line, (size_t)length, true)) {
        return;
    }
    g_diag_ndjson_size += (size_t)length;
}

static void write_diagnostics(void) {
#ifdef CBM_DIAGNOSTICS_ENABLE_TEST_API
    diag_test_pause_writer();
#endif
    if (atomic_load_explicit(&g_diag_stop, memory_order_acquire)) {
        return;
    }

    size_t elapsed_ms = 0;
    size_t user_ms = 0;
    size_t sys_ms = 0;
    size_t current_rss = 0;
    size_t peak_rss = 0;
    size_t current_commit = 0;
    size_t peak_commit = 0;
    size_t page_faults = 0;
    mi_process_info(&elapsed_ms, &user_ms, &sys_ms, &current_rss, &peak_rss, &current_commit,
                    &peak_commit, &page_faults);
    if (current_rss == 0) {
        current_rss = cbm_mem_rss();
    }

    int fds = count_open_fds();
    time_t now = time(NULL);
    long uptime = (long)(now - g_start_time);
    int qcount = atomic_load(&g_query_stats.count);
    int qerrors = atomic_load(&g_query_stats.errors);
    long long qtime = atomic_load(&g_query_stats.time_us);
    long long qmax = atomic_load(&g_query_stats.max_us);
    long long qavg = qcount > 0 ? qtime / qcount : 0;

    char snapshot[CBM_SZ_1K];
    int length = snprintf(snapshot, sizeof(snapshot),
                          "{\n"
                          "  \"uptime_s\": %ld,\n"
                          "  \"rss_bytes\": %zu,\n"
                          "  \"peak_rss_bytes\": %zu,\n"
                          "  \"heap_committed_bytes\": %zu,\n"
                          "  \"peak_committed_bytes\": %zu,\n"
                          "  \"page_faults\": %zu,\n"
                          "  \"fd_count\": %d,\n"
                          "  \"query_count\": %d,\n"
                          "  \"query_errors\": %d,\n"
                          "  \"query_total_us\": %lld,\n"
                          "  \"query_avg_us\": %lld,\n"
                          "  \"query_max_us\": %lld,\n"
                          "  \"pid\": %d\n"
                          "}\n",
                          uptime, current_rss, peak_rss, current_commit, peak_commit, page_faults,
                          fds, qcount, qerrors, qtime, qavg, qmax, (int)getpid());
    if (length <= 0 || (size_t)length >= sizeof(snapshot) ||
        !diag_write_file(DIAG_SNAPSHOT_TMP_NAME, snapshot, (size_t)length, false) ||
        !diag_native_rename(DIAG_SNAPSHOT_TMP_NAME, DIAG_SNAPSHOT_NAME)) {
        diag_native_unlink(DIAG_SNAPSHOT_TMP_NAME);
        return;
    }
    if (!atomic_load_explicit(&g_diag_stop, memory_order_acquire)) {
        append_trajectory(uptime, current_rss, peak_rss, current_commit, peak_commit, page_faults,
                          fds, qcount);
    }
}

static void *diag_thread_fn(void *arg) {
    (void)arg;
    while (!atomic_load_explicit(&g_diag_stop, memory_order_acquire)) {
        write_diagnostics();
        if (!atomic_load_explicit(&g_diag_stop, memory_order_acquire)) {
            diag_wait_for_interval();
        }
    }
    atomic_store_explicit(&g_diag_done, true, memory_order_release);
    return NULL;
}

static void diag_cleanup_live_files(void) {
    diag_native_unlink(DIAG_SNAPSHOT_NAME);
    diag_native_unlink(DIAG_SNAPSHOT_TMP_NAME);
}

/* ── Public API ──────────────────────────────────────────────────────── */

bool cbm_diag_start(void) {
    char env_buf[CBM_SZ_32] = "";
    cbm_safe_getenv("CBM_DIAGNOSTICS", env_buf, sizeof(env_buf), NULL);
    if (env_buf[0] == '\0' || (strcmp(env_buf, "1") != 0 && strcmp(env_buf, "true") != 0) ||
        g_diag_started || g_diag_abandoned) {
        return false;
    }

    g_start_time = time(NULL);
    g_diag_ndjson_size = 0;
    atomic_store_explicit(&g_diag_stop, false, memory_order_release);
    atomic_store_explicit(&g_diag_done, false, memory_order_release);
#ifdef CBM_DIAGNOSTICS_ENABLE_TEST_API
    atomic_store_explicit(&g_diag_test_writer_reached, false, memory_order_release);
#endif
    if (!diag_directory_prepare()) {
        return false;
    }
    if (cbm_thread_create(&g_diag_thread, 0, diag_thread_fn, NULL) != 0) {
        diag_directory_close(true);
        return false;
    }

    g_diag_started = true;
    char interval[CBM_SZ_32];
    (void)snprintf(interval, sizeof(interval), "%d", DIAG_INTERVAL_MS / 1000);
    /* Discovery must survive CBM_LOG_LEVEL suppression and paths containing
     * spaces: a control record is the only announced copy of these paths. */
    cbm_log_control("diagnostics.start", "snapshot", g_diag_path, "trajectory", g_diag_ndjson_path,
                    "interval_s", interval);
    return true;
}

void cbm_diag_stop(void) {
    if (!g_diag_started) {
        return;
    }
    atomic_store_explicit(&g_diag_stop, true, memory_order_release);

    uint64_t deadline = cbm_now_ms() + DIAG_STOP_TIMEOUT_MS;
    while (!atomic_load_explicit(&g_diag_done, memory_order_acquire) && cbm_now_ms() < deadline) {
        diag_sleep_ms(DIAG_WAIT_SLICE_MS);
    }
    bool completed = atomic_load_explicit(&g_diag_done, memory_order_acquire);
    if (completed) {
        /* The completion publication is the final access to diagnostics
         * state. Detaching avoids turning an already-complete best-effort
         * writer into an unbounded native join during daemon teardown. */
        (void)cbm_thread_detach(&g_diag_thread);
        diag_cleanup_live_files();
        diag_directory_close(false);
    } else {
        /* The thread may still hold a native file or directory descriptor.
         * Detach it and intentionally retain all static state until process
         * exit; closing or reusing those handles would create an fd/handle
         * reuse race if the stalled operation later resumes. */
        (void)cbm_thread_detach(&g_diag_thread);
        g_diag_abandoned = true;
    }
    g_diag_started = false;
}

#ifdef CBM_DIAGNOSTICS_ENABLE_TEST_API

static bool diag_test_copy(char *destination, size_t destination_size, const char *source) {
    if (!destination || destination_size == 0) {
        return false;
    }
    int written = snprintf(destination, destination_size, "%s", source);
    return written >= 0 && (size_t)written < destination_size;
}

bool cbm_diag_test_copy_paths(char *directory, size_t directory_size, char *snapshot,
                              size_t snapshot_size, char *trajectory, size_t trajectory_size) {
    return diag_test_copy(directory, directory_size, g_diag_directory_path) &&
           diag_test_copy(snapshot, snapshot_size, g_diag_path) &&
           diag_test_copy(trajectory, trajectory_size, g_diag_ndjson_path);
}

void cbm_diag_test_hold_writer(bool hold) {
    atomic_store_explicit(&g_diag_test_hold_writer, hold, memory_order_release);
}

bool cbm_diag_test_writer_reached(void) {
    return atomic_load_explicit(&g_diag_test_writer_reached, memory_order_acquire);
}

bool cbm_diag_test_abandoned(void) {
    return g_diag_abandoned;
}

bool cbm_diag_test_reset_abandoned(void) {
    if (!g_diag_abandoned || g_diag_started ||
        !atomic_load_explicit(&g_diag_done, memory_order_acquire)) {
        return false;
    }
    diag_cleanup_live_files();
    diag_directory_close(false);
    g_diag_abandoned = false;
    return true;
}

#endif
